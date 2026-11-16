//! Measure this host's environment for `workers diff`: the same function runs
//! on the parent and, through `worker-service env`, on a worker. Every
//! measurement uses the worker's persisted profile (its PATH), never the PATH
//! of the ssh session that asked.
use super::capability::{valid_version, ToolObservation, TOOLS};
use super::contract::canonical_tool;
use super::envdiff::{Docker, EnvReport, Managed, Observed, ToolFact, SCHEMA};
use super::profile::Profile;
use super::registry::Registry;
use crate::ids::Digest;
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const HELPER_TIMEOUT: Duration = Duration::from_secs(2);
const DOCKER_TIMEOUT: Duration = Duration::from_secs(5);
const OUTPUT_LIMIT: u64 = 8192;
/// Helpers probed by a fixed `--version` call on the profile PATH.
const HELPERS: &[&str] = &["task-traffic-controller", "local-ci"];

#[derive(Debug, PartialEq, Eq)]
pub enum Run {
    Output(String),
    Absent,
    Failed,
    TimedOut,
}

/// One fixed call of an executable found in `dirs`, with a cleared
/// environment whose only variable is that PATH. The child is this function's
/// own handle: killed and reaped by handle on a timeout.
pub fn run_fixed(dirs: &[PathBuf], program: &str, args: &[&str], timeout: Duration) -> Run {
    let Some(path) = dirs.iter().map(|d| d.join(program)).find(|p| {
        std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }) else {
        return Run::Absent;
    };
    let search = std::env::join_paths(dirs).unwrap_or_default();
    let Ok(mut child) = Command::new(path)
        .args(args)
        .env_clear()
        .env("PATH", search)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return Run::Failed;
    };
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Run::TimedOut;
            }
        }
    };
    let mut text = String::new();
    if let Some(out) = child.stdout.take() {
        let _ = out.take(OUTPUT_LIMIT).read_to_string(&mut text);
    }
    if status.success() {
        Run::Output(text)
    } else {
        Run::Failed
    }
}

/// The first token of the output that starts with a digit (after an optional
/// `v`), when it is a plausible version.
pub fn parse_version(output: &str) -> Option<String> {
    output
        .lines()
        .next()?
        .split_whitespace()
        .map(|t| t.strip_prefix('v').unwrap_or(t))
        .find(|t| t.starts_with(|c: char| c.is_ascii_digit()) && valid_version(t))
        .map(str::to_string)
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

pub fn collect(state: &Path, env: &BTreeMap<String, String>) -> Result<EnvReport, String> {
    collect_with(state, env, HELPER_TIMEOUT, DOCKER_TIMEOUT)
}

fn collect_with(
    state: &Path,
    env: &BTreeMap<String, String>,
    helper_timeout: Duration,
    docker_timeout: Duration,
) -> Result<EnvReport, String> {
    let profile = Profile::load(state, env)?;
    let report = profile.startup_report();
    // No private tool root: no adapters, which is a fact and not an error.
    let managed = if state.join("toolchains").is_dir() {
        Registry::load(state, &profile, report.profile_fingerprint.clone())
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
    let dirs = profile.path_dirs();
    let mut helpers: BTreeMap<String, Option<String>> = HELPERS
        .iter()
        .map(|h| {
            let version = match run_fixed(dirs, h, &["--version"], helper_timeout) {
                Run::Output(text) => parse_version(&text),
                _ => None,
            };
            (h.to_string(), version)
        })
        .collect();
    helpers.insert(
        "remote-task-runner".into(),
        Some(env!("CARGO_PKG_VERSION").into()),
    );
    let docker = match run_fixed(
        dirs,
        "docker",
        &["info", "--format", "{{.ServerVersion}}"],
        docker_timeout,
    ) {
        Run::Output(text) if !text.trim().is_empty() => Docker::Runnable,
        Run::TimedOut => Docker::Unknown,
        _ => Docker::NotRunnable,
    };
    Ok(EnvReport {
        schema_version: SCHEMA,
        os: report.os,
        arch: report.arch,
        tools,
        helpers,
        docker,
        disk_free_bytes: free_disk(state),
        profile_digest: Some(report.profile_digest.to_string()),
        // The agent reports the digest of its configured policy bytes; the
        // service configures none, so it is the digest of nothing.
        policy_digest: Some(Digest::of_bytes(&[]).to_string()),
    })
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

    #[test]
    fn versions_are_the_first_numeric_token_of_the_first_line() {
        assert_eq!(parse_version("local-ci 0.13.0\n"), Some("0.13.0".into()));
        assert_eq!(parse_version("v20.1.0"), Some("20.1.0".into()));
        assert_eq!(
            parse_version("task-traffic-controller v0.8.2 (abc)"),
            Some("0.8.2".into())
        );
        assert_eq!(parse_version("no version here"), None);
        assert_eq!(parse_version(""), None);
        // Only the first line counts.
        assert_eq!(parse_version("banner\n1.2.3"), None);
        // Not a plausible version.
        assert_eq!(parse_version("1.2.3;rm"), None);
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
            run_fixed(&dirs, "ok", &[], SHORT * 10),
            Run::Output("hello\n".into())
        );
        assert_eq!(run_fixed(&dirs, "bad", &[], SHORT * 10), Run::Failed);
        assert_eq!(run_fixed(&dirs, "missing", &[], SHORT), Run::Absent);
        assert_eq!(run_fixed(&dirs, "plain", &[], SHORT), Run::Absent);
        let started = Instant::now();
        assert_eq!(
            run_fixed(&dirs, "slow", &[], Duration::from_millis(100)),
            Run::TimedOut
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        // The environment is cleared: only the profile PATH is passed.
        assert_eq!(
            run_fixed(&dirs, "env", &[], SHORT * 10),
            Run::Output(format!("[{}][]\n", bin.display()))
        );
        // A program that cannot be started.
        std::fs::write(bin.join("broken"), "\u{1}").unwrap();
        std::fs::set_permissions(bin.join("broken"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        assert_eq!(run_fixed(&dirs, "broken", &[], SHORT), Run::Failed);
    }

    #[test]
    fn collect_measures_with_the_profile_path() {
        let (_t, state, bin) = state();
        shim(&bin, "node", "echo v20.1.0");
        shim(&bin, "local-ci", "echo local-ci 0.13.0");
        shim(&bin, "docker", "echo 27.0.1");
        let report = collect_with(&state, &BTreeMap::new(), SHORT * 10, SHORT * 10).unwrap();
        assert_eq!(report.schema_version, SCHEMA);
        assert_eq!(report.os, std::env::consts::OS);
        assert_eq!(report.tools["node"].observed, Observed::Available);
        assert_eq!(report.tools["node"].version.as_deref(), Some("20.1.0"));
        assert_eq!(report.tools["cargo"].observed, Observed::Unavailable);
        assert_eq!(report.tools["node"].managed, None);
        assert_eq!(report.helpers["local-ci"].as_deref(), Some("0.13.0"));
        assert_eq!(report.helpers["task-traffic-controller"], None);
        assert_eq!(
            report.helpers["remote-task-runner"].as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(report.docker, Docker::Runnable);
        assert!(report.disk_free_bytes.is_some_and(|b| b > 0));
        assert!(report.profile_digest.unwrap().starts_with("sha256:"));
        assert_eq!(
            report.policy_digest.unwrap(),
            Digest::of_bytes(&[]).to_string()
        );
    }

    #[test]
    fn docker_runnability_follows_the_daemon_answer() {
        let (_t, state, bin) = state();
        let docker = |state: &Path| {
            collect_with(
                state,
                &BTreeMap::new(),
                SHORT * 10,
                Duration::from_millis(400),
            )
            .unwrap()
            .docker
        };
        assert_eq!(docker(&state), Docker::NotRunnable);
        shim(&bin, "docker", "exit 1");
        assert_eq!(docker(&state), Docker::NotRunnable);
        shim(&bin, "docker", "true");
        assert_eq!(docker(&state), Docker::NotRunnable);
        shim(&bin, "docker", "/bin/sleep 3");
        assert_eq!(docker(&state), Docker::Unknown);
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
        let report = collect_with(&state, &BTreeMap::new(), SHORT, SHORT).unwrap();
        assert!(report.tools.values().all(|t| t.managed.is_none()));
        // A configuration that does not load is no adapters either.
        std::fs::write(state.join("toolchains.toml"), "garbage [").unwrap();
        std::fs::set_permissions(
            state.join("toolchains.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let report = collect_with(&state, &BTreeMap::new(), SHORT, SHORT).unwrap();
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
        let report = collect_with(&state, &BTreeMap::new(), SHORT, SHORT).unwrap();
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
