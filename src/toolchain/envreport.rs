//! Measure this host's environment for `workers diff`: the same function runs
//! on the parent and, through `worker-service env`, on a worker. Every
//! measurement uses the worker's persisted profile (its PATH), never the PATH
//! of the ssh session that asked.
use super::capability::{ToolObservation, TOOLS};
use super::contract::canonical_tool;
use super::envdiff::{Docker, EnvReport, Managed, Observed, ToolFact, SCHEMA};
use super::profile::Profile;
use super::registry::Registry;
use std::collections::BTreeMap;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const DOCKER_TIMEOUT: Duration = Duration::from_secs(5);
const OUTPUT_LIMIT: usize = 8192;
/// Variables the docker CLI needs to find its daemon, taken from the
/// worker's own environment when it has them.
const DOCKER_ENV: &[&str] = &["DOCKER_HOST", "DOCKER_CONTEXT"];

#[derive(Debug, PartialEq, Eq)]
pub enum Run {
    Output(String),
    Absent,
    Failed,
    TimedOut,
}

/// Read what is there without waiting; keep the first `OUTPUT_LIMIT` bytes
/// and throw the rest away, so a chatty child never blocks on a full pipe.
/// False when the stream broke.
fn drain(out: &mut impl Read, kept: &mut Vec<u8>) -> bool {
    let mut chunk = [0u8; 4096];
    loop {
        match out.read(&mut chunk) {
            Ok(0) => return true,
            Ok(n) => {
                let room = OUTPUT_LIMIT.saturating_sub(kept.len()).min(n);
                kept.extend_from_slice(&chunk[..room]);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return true,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return false,
        }
    }
}

/// One fixed call of an executable found in `dirs`, with a cleared
/// environment of that PATH and `extra`. The child leads its own process
/// group, which is killed (with every descendant) when the call ends, and the
/// child is reaped by its handle.
pub fn run_fixed(
    dirs: &[PathBuf],
    program: &str,
    args: &[&str],
    extra: &BTreeMap<String, String>,
    timeout: Duration,
) -> Run {
    let Some(path) = dirs.iter().map(|d| d.join(program)).find(|p| {
        std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }) else {
        return Run::Absent;
    };
    let search = std::env::join_paths(dirs).unwrap_or_default();
    let Ok(mut child) = Command::new(path)
        .args(args)
        .env_clear()
        .envs(extra)
        .env("PATH", search)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
    else {
        return Run::Failed;
    };
    let pid = child.id();
    let mut text = Vec::new();
    let finished = wait_draining(&mut child, pid, timeout, &mut text);
    // Our unreaped Child owns pid, so this group number cannot be recycled.
    if let Ok(group) = i32::try_from(pid) {
        crate::platform::signal_process_group(group, libc::SIGKILL);
    }
    let status = child.wait();
    match (finished, status) {
        (Some(true), Ok(status)) if status.success() => {
            Run::Output(String::from_utf8_lossy(&text).into_owned())
        }
        (Some(false), _) => Run::TimedOut,
        _ => Run::Failed,
    }
}

/// Read the child's output until it exits. `Some(true)`: it exited in time;
/// `Some(false)`: it did not; `None`: the pipe or the wait failed.
fn wait_draining(
    child: &mut std::process::Child,
    pid: u32,
    timeout: Duration,
    kept: &mut Vec<u8>,
) -> Option<bool> {
    let mut out = child.stdout.take()?;
    let fd = out.as_raw_fd();
    // SAFETY: plain fcntl calls on a descriptor this function owns.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return None;
    }
    let started = Instant::now();
    loop {
        if !drain(&mut out, kept) {
            return None;
        }
        // SAFETY: waitid on our own child; WNOWAIT leaves it unreaped.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let observed = unsafe {
            libc::waitid(
                libc::P_PID,
                pid,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if observed != 0 {
            return None;
        }
        // SAFETY: reads the field waitid filled in.
        if unsafe { info.si_pid() } == pid as i32 {
            return drain(&mut out, kept).then_some(true);
        }
        if started.elapsed() >= timeout {
            return Some(false);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn observed(o: &ToolObservation) -> (Observed, Option<String>) {
    match o {
        // `v20.1.0` and `20.1.0` are one version, as the manager inventory writes it.
        ToolObservation::Available { version, .. } => (
            Observed::Available,
            Some(version.strip_prefix('v').unwrap_or(version).to_string()),
        ),
        ToolObservation::Unavailable => (Observed::Unavailable, None),
        ToolObservation::Unknown => (Observed::Unknown, None),
    }
}

fn free_disk(state: &Path) -> Option<u64> {
    rustix::fs::statvfs(state)
        .ok()
        .map(|s| s.f_bavail.saturating_mul(s.f_frsize))
}

/// What `docker info` says, run the way the worker's own jobs would: with the
/// profile's HOME and the worker's docker variables. An error says only that
/// it could not be measured; just a missing docker is "not runnable".
fn docker_state(
    dirs: &[PathBuf],
    profile: &Profile,
    env: &BTreeMap<String, String>,
    timeout: Duration,
) -> Docker {
    let mut extra: BTreeMap<String, String> = env
        .iter()
        .filter(|(k, _)| DOCKER_ENV.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if let Some(home) = profile.environment().get("HOME") {
        extra.insert("HOME".into(), home.clone());
    }
    match run_fixed(
        dirs,
        "docker",
        &["info", "--format", "{{.ServerVersion}}"],
        &extra,
        timeout,
    ) {
        Run::Output(text) if !text.trim().is_empty() => Docker::Runnable,
        Run::Absent => Docker::NotRunnable,
        _ => Docker::Unknown,
    }
}

pub fn collect(state: &Path, env: &BTreeMap<String, String>) -> Result<EnvReport, String> {
    collect_with(state, env, DOCKER_TIMEOUT)
}

fn collect_with(
    state: &Path,
    env: &BTreeMap<String, String>,
    docker_timeout: Duration,
) -> Result<EnvReport, String> {
    let profile = Profile::load(state, env)?;
    Ok(measure(state, &profile, env, docker_timeout))
}

pub(crate) fn collect_profile(
    state: &Path,
    profile: &Profile,
    env: &BTreeMap<String, String>,
) -> EnvReport {
    measure(state, profile, env, DOCKER_TIMEOUT)
}

fn measure(
    state: &Path,
    profile: &Profile,
    env: &BTreeMap<String, String>,
    docker_timeout: Duration,
) -> EnvReport {
    let report = profile.startup_report();
    // No private tool root: no adapters, which is a fact and not an error.
    let managed = if state.join("toolchains").is_dir() {
        Registry::load(state, profile, report.profile_fingerprint.clone())
            .map(|r| r.installed_inventory())
            .unwrap_or_default()
    } else {
        BTreeMap::new()
    };
    let tools = TOOLS
        .iter()
        .map(|name| {
            let (state, version) = report
                .tools
                .get(*name)
                .map_or((Observed::Unknown, None), observed);
            let managed = managed.get(canonical_tool(name)).map(|m| Managed {
                inventory: m.inventory.clone(),
                installed: m.installed.clone(),
            });
            (
                name.to_string(),
                ToolFact {
                    observed: state,
                    version,
                    managed,
                },
            )
        })
        .collect();
    // task-traffic-controller and local-ci are built into this binary (spec
    // 22.4): its build identity stands for them, and no external `--version`
    // is run.
    let helpers = BTreeMap::from([(
        "remote-task-runner".to_string(),
        Some(env!("CARGO_PKG_VERSION").to_string()),
    )]);
    let docker = docker_state(profile.path_dirs(), profile, env, docker_timeout);
    EnvReport {
        schema_version: SCHEMA,
        os: report.os,
        arch: report.arch,
        tools,
        helpers,
        docker,
        disk_free_bytes: free_disk(state),
        profile: profile.name().to_string(),
        required_tools: profile.required_tools().iter().cloned().collect(),
        profile_digest: Some(report.profile_digest.to_string()),
        policy_digest: policy_digest(env),
    }
}

/// The digest of the task-traffic-controller policy in effect for `env`: the
/// scope that environment resolves to, and the effective configuration read
/// from it (a missing file is the default policy). The same formula is the
/// one `doctor` gives a scope. `None` when the scope cannot be resolved or
/// the configuration cannot be read, which is "not measured", never a digest
/// of nothing.
fn policy_digest(env: &BTreeMap<String, String>) -> Option<String> {
    let scope = crate::config::traffic::resolve(env).ok()?;
    let config = task_traffic_controller::config::Config::load(&scope.config_file).ok()?;
    crate::config::traffic::policy_digest(&config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn shim(dir: &Path, name: &str, body: &str) {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    /// A private state directory whose active profile runs only `bin`.
    fn state() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = crate::test_tmp::tempdir().unwrap();
        let root = std::fs::canonicalize(temp.path()).unwrap();
        let bin = root.join("bin");
        std::fs::create_dir(&bin).unwrap();
        let state = root.join("state");
        std::fs::create_dir(&state).unwrap();
        let text = format!(
            "schema_version=1\nactive='t'\n[profiles.t]\npath=[{:?}]\nshell='/bin/sh'\ncache_namespace='t'\n",
            bin.to_str().unwrap()
        );
        std::fs::write(state.join("profiles.toml"), text).unwrap();
        std::fs::set_permissions(
            state.join("profiles.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        (temp, state, bin)
    }
    const SHORT: Duration = Duration::from_millis(300);
    fn none() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    #[test]
    fn a_fixed_call_says_what_happened_and_never_waits_past_its_time() {
        let (_t, _state, bin) = state();
        let dirs = vec![bin.clone()];
        shim(&bin, "ok", "echo hello");
        shim(&bin, "bad", "exit 3");
        shim(&bin, "slow", "/bin/sleep 5");
        shim(&bin, "env", "echo \"[$PATH][$HOME]\"");
        std::fs::write(bin.join("plain"), "not executable").unwrap();
        assert_eq!(
            run_fixed(&dirs, "ok", &[], &none(), SHORT * 10),
            Run::Output("hello\n".into())
        );
        assert_eq!(
            run_fixed(&dirs, "bad", &[], &none(), SHORT * 10),
            Run::Failed
        );
        assert_eq!(
            run_fixed(&dirs, "missing", &[], &none(), SHORT),
            Run::Absent
        );
        assert_eq!(run_fixed(&dirs, "plain", &[], &none(), SHORT), Run::Absent);
        let started = Instant::now();
        assert_eq!(
            run_fixed(&dirs, "slow", &[], &none(), Duration::from_millis(100)),
            Run::TimedOut
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        // The environment is cleared: only the profile PATH is passed.
        assert_eq!(
            run_fixed(&dirs, "env", &[], &none(), SHORT * 10),
            Run::Output(format!("[{}][]\n", bin.display()))
        );
        // A program that cannot be started.
        std::fs::write(bin.join("broken"), "\u{1}").unwrap();
        std::fs::set_permissions(bin.join("broken"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        assert_eq!(run_fixed(&dirs, "broken", &[], &none(), SHORT), Run::Failed);
    }

    fn policy_env(home: &Path, config: Option<&str>) -> BTreeMap<String, String> {
        let dir = home.join(".config").join("task-traffic-controller");
        std::fs::create_dir_all(&dir).unwrap();
        if let Some(text) = config {
            std::fs::write(dir.join("config.toml"), text).unwrap();
        }
        BTreeMap::from([("HOME".to_string(), home.to_str().unwrap().to_string())])
    }

    #[test]
    fn the_policy_digest_follows_the_effective_configuration_of_the_scope() {
        let temp = crate::test_tmp::tempdir().unwrap();
        let home = |n: &str| {
            let h = std::fs::canonicalize(temp.path()).unwrap().join(n);
            std::fs::create_dir_all(&h).unwrap();
            h
        };
        let digest = |n: &str, config: Option<&str>| policy_digest(&policy_env(&home(n), config));
        let default = digest("a", None);
        assert!(default.as_deref().unwrap().starts_with("sha256:"));
        // A missing file is the default policy, the same as an explicit one.
        assert_eq!(digest("b", None), default);
        let four = digest("d", Some("budget = 4\n"));
        assert_eq!(digest("e", Some("budget = 4\n")), four);
        let default_budget = task_traffic_controller::config::Config::default().budget;
        assert_eq!(
            digest("i", Some(&format!("budget = {default_budget}\n"))),
            default
        );
        assert_ne!(digest("f", Some("budget = 5\n")), four);
        // A configuration that cannot be read is not measured.
        assert_eq!(digest("g", Some("budget = 0\n")), None);
        assert_eq!(digest("h", Some("not toml [")), None);
        // So is an environment that resolves to no scope.
        assert_eq!(policy_digest(&BTreeMap::new()), None);
        let relative = BTreeMap::from([("HOME".to_string(), "relative".to_string())]);
        assert_eq!(policy_digest(&relative), None);
    }

    #[test]
    fn collect_reports_the_policy_digest_of_the_workers_scope() {
        let (t, state, _bin) = state();
        let home = std::fs::canonicalize(t.path()).unwrap().join("home");
        std::fs::create_dir(&home).unwrap();
        let env = policy_env(&home, Some("budget = 3\n"));
        let report = collect_with(&state, &env, SHORT).unwrap();
        assert_eq!(report.policy_digest, policy_digest(&env));
        assert!(report.policy_digest.is_some());
    }

    #[test]
    fn collect_measures_with_the_profile_path() {
        let (_t, state, bin) = state();
        shim(&bin, "node", "echo v20.1.0");
        // Built-in tools are never asked: this shim would be seen if they were.
        shim(&bin, "local-ci", "echo local-ci 9.9.9");
        shim(&bin, "docker", "echo 27.0.1");
        let report = collect_with(&state, &BTreeMap::new(), SHORT * 10).unwrap();
        assert_eq!(report.schema_version, SCHEMA);
        assert_eq!(report.os, std::env::consts::OS);
        assert_eq!(report.tools["node"].observed, Observed::Available);
        assert_eq!(report.tools["node"].version.as_deref(), Some("20.1.0"));
        assert_eq!(report.tools["cargo"].observed, Observed::Unavailable);
        assert_eq!(report.tools["node"].managed, None);
        assert_eq!(
            report.helpers.keys().collect::<Vec<_>>(),
            ["remote-task-runner"]
        );
        assert_eq!(
            report.helpers["remote-task-runner"].as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(report.docker, Docker::Runnable);
        assert!(report.disk_free_bytes.is_some_and(|b| b > 0));
        assert_eq!(report.profile, "t");
        assert!(report.required_tools.is_empty());
        assert!(report.profile_digest.unwrap().starts_with("sha256:"));
        // Nothing to resolve the scope from: not measured.
        assert_eq!(report.policy_digest, None);
    }

    #[test]
    fn docker_runnability_follows_the_daemon_answer() {
        let (_t, state, bin) = state();
        let docker = |state: &Path| {
            collect_with(state, &BTreeMap::new(), Duration::from_millis(400))
                .unwrap()
                .docker
        };
        // No docker program at all: not runnable.
        assert_eq!(docker(&state), Docker::NotRunnable);
        // An error or an empty answer says only that it could not be measured.
        shim(&bin, "docker", "exit 1");
        assert_eq!(docker(&state), Docker::Unknown);
        shim(&bin, "docker", "true");
        assert_eq!(docker(&state), Docker::Unknown);
        shim(&bin, "docker", "/bin/sleep 3");
        assert_eq!(docker(&state), Docker::Unknown);
        shim(&bin, "docker", "echo 27.0.1");
        assert_eq!(docker(&state), Docker::Runnable);
    }

    #[test]
    fn docker_gets_the_profile_home_and_the_workers_docker_variables() {
        let (_t, state, bin) = state();
        // Succeeds only with the daemon location the worker configured.
        shim(
            &bin,
            "docker",
            "[ \"$DOCKER_HOST\" = unix:///colima.sock ] && [ \"$DOCKER_CONTEXT\" = colima ] && [ \"$HOME\" = /worker/home ] && [ -z \"$OTHER\" ] && echo 27",
        );
        let env = BTreeMap::from([
            ("HOME".to_string(), "/worker/home".to_string()),
            ("DOCKER_HOST".to_string(), "unix:///colima.sock".to_string()),
            ("DOCKER_CONTEXT".to_string(), "colima".to_string()),
            ("OTHER".to_string(), "x".to_string()),
        ]);
        let got = collect_with(&state, &env, SHORT * 10).unwrap();
        assert_eq!(got.docker, Docker::Runnable);
        let got = collect_with(&state, &BTreeMap::new(), SHORT * 10).unwrap();
        assert_eq!(got.docker, Docker::Unknown);
    }

    #[test]
    fn output_beyond_the_limit_is_discarded_and_never_blocks_the_child() {
        let (_t, _state, bin) = state();
        let dirs = vec![bin.clone()];
        // Far more than a pipe holds, then a clean exit.
        shim(
            &bin,
            "chatty",
            "/usr/bin/yes abcdefgh | /usr/bin/head -c 400000",
        );
        let started = Instant::now();
        let got = run_fixed(&dirs, "chatty", &[], &none(), Duration::from_secs(20));
        let Run::Output(text) = got else {
            panic!("{got:?}");
        };
        assert_eq!(text.len(), OUTPUT_LIMIT);
        assert!(text.starts_with("abcdefgh\n"));
        assert!(started.elapsed() < Duration::from_secs(10));
        // Exactly the limit and one byte more.
        shim(
            &bin,
            "exact",
            &format!("/usr/bin/head -c {OUTPUT_LIMIT} /dev/zero | /usr/bin/tr '\\0' x"),
        );
        assert_eq!(
            run_fixed(&dirs, "exact", &[], &none(), SHORT * 10),
            Run::Output("x".repeat(OUTPUT_LIMIT))
        );
        shim(
            &bin,
            "over",
            &format!(
                "/usr/bin/head -c {} /dev/zero | /usr/bin/tr '\\0' x",
                OUTPUT_LIMIT + 1
            ),
        );
        assert_eq!(
            run_fixed(&dirs, "over", &[], &none(), SHORT * 10),
            Run::Output("x".repeat(OUTPUT_LIMIT))
        );
    }

    #[test]
    fn a_timeout_stops_the_whole_process_group_of_the_child() {
        let (_t, state, bin) = state();
        let dirs = vec![bin.clone()];
        let pidfile = state.join("grandchild.pid");
        shim(
            &bin,
            "spawner",
            &format!(
                "/bin/sleep 30 &\necho $! > {}\n/bin/sleep 30",
                pidfile.display()
            ),
        );
        assert_eq!(
            run_fixed(&dirs, "spawner", &[], &none(), Duration::from_millis(500)),
            Run::TimedOut
        );
        let grandchild: u32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(grandchild > 1);
        let gone = (0..100).any(|_| {
            let gone = crate::platform::process_start_identity(grandchild)
                == crate::platform::StartIdentity::NoProcess;
            if !gone {
                std::thread::sleep(Duration::from_millis(50));
            }
            gone
        });
        assert!(gone, "the descendant outlived the timeout");
    }

    #[test]
    fn a_missing_or_unusable_profile_is_an_error_and_adapters_come_from_the_registry() {
        let temp = crate::test_tmp::tempdir().unwrap();
        let bad = temp.path().join("nothing");
        std::fs::create_dir(&bad).unwrap();
        std::fs::write(bad.join("profiles.toml"), "not toml [").unwrap();
        std::fs::set_permissions(
            bad.join("profiles.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(collect(&bad, &BTreeMap::new()).is_err());
        // A private tool root without a configuration: no adapters, no error.
        let (_t, state, _bin) = state();
        std::fs::create_dir(state.join("toolchains")).unwrap();
        std::fs::set_permissions(
            state.join("toolchains"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let report = collect_with(&state, &BTreeMap::new(), SHORT).unwrap();
        assert!(report.tools.values().all(|t| t.managed.is_none()));
        // A configuration that does not load is no adapters either.
        std::fs::write(state.join("toolchains.toml"), "garbage [").unwrap();
        std::fs::set_permissions(
            state.join("toolchains.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let report = collect_with(&state, &BTreeMap::new(), SHORT).unwrap();
        assert!(report.tools.values().all(|t| t.managed.is_none()));
    }

    #[test]
    fn managed_versions_are_attached_by_canonical_tool_name() {
        let (_t, state, bin) = state();
        for name in ["manager", "node", "node-build", "rustup"] {
            shim(&bin, name, "exit 1");
        }
        std::fs::create_dir(state.join("toolchains")).unwrap();
        std::fs::set_permissions(
            state.join("toolchains"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let definition = Profile::load(&state, &BTreeMap::new())
            .unwrap()
            .definition_digest()
            .to_string();
        let b = bin.to_str().unwrap();
        let text = format!(
            "schema_version = 1\nactive_profile = 't'\nprofile_definition = '{definition}'\n\
             [[recipes]]\ntool = 'node'\nkind = 'Nodenv'\nmanager = '{b}/manager'\n\
             dependencies = ['{b}/node', '{b}/node-build']\nsearch_path = ['{b}']\n\
             inventory = ['20.1.0']\n\
             [[recipes]]\ntool = 'rust'\nkind = 'Rustup'\nmanager = '{b}/rustup'\n\
             search_path = ['{b}']\nrust_host = 'fixture-host'\ninventory = ['1.80.0']\n"
        );
        std::fs::write(state.join("toolchains.toml"), text).unwrap();
        std::fs::set_permissions(
            state.join("toolchains.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let report = collect_with(&state, &BTreeMap::new(), SHORT).unwrap();
        let managed = |tool: &str| report.tools[tool].managed.clone();
        let node = Managed {
            inventory: vec!["20.1.0".into()],
            installed: vec![],
        };
        let rust = Managed {
            inventory: vec!["1.80.0".into()],
            installed: vec![],
        };
        assert_eq!(managed("node"), Some(node));
        assert_eq!(managed("rustc"), Some(rust.clone()));
        assert_eq!(managed("cargo"), Some(rust));
        assert_eq!(managed("pnpm"), None);
        assert_eq!(managed("git"), None);
    }
}
