//! `doctor`: what is installed on the parent and on every registered worker,
//! looked at the way the service sees it, and whether it fits together.
//!
//! Each host gathers plain facts in its *service* environment (the retained
//! environment the LaunchAgent runs with, not the shell that typed the
//! command): which `task-traffic-controller` binary that environment
//! resolves and whether it understands managed ownership, the resource scope
//! the worker uses and the scope that CLI uses, both policy digests, the
//! Docker engine the environment reaches, and whether the worker profile
//! loads there. The parent turns the facts into checks in one place
//! ([`evaluate`], [`shared_engines`]), so a host never grades itself.
//!
//! A check is `ok`, `notice`, `fail` or `not_checked`. Something that could
//! not be looked at is never `ok`: an unreachable worker fails, and a check
//! that is not implemented yet is listed as `not_checked`. What a person has
//! to fix fails; what an exec or `workers sync` installs by itself, or what
//! neither side measures yet, is a `notice` and does not change the exit.
use super::bootstrap::Spec;
use super::macos::policy::Backend;
use super::worker_service::Status;
use super::workers::{self, Remote, RemoteOutput};
use crate::{
    error::{ApiError, ErrorCode},
    ids::{ClusterId, WorkerId},
    protocol::ssh::{quote, SshTarget},
    toolchain::envdiff::{self, Class, EnvReport, Item},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use task_traffic_controller::{config::Config, ledger::Ledger, lock, Context, Paths};

const TRAFFIC_CLI: &str = "task-traffic-controller";
/// Where `loginctl` is when the service environment's PATH does not have it.
const LOGINCTL: &str = "/usr/bin/loginctl";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const OUTPUT_LIMIT: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Ok,
    Notice,
    Fail,
    NotChecked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    /// `parent` or the worker's name.
    pub host: String,
    pub id: String,
    pub level: Level,
    pub detail: String,
}
impl Check {
    fn new(host: &str, id: &str, level: Level, detail: impl Into<String>) -> Self {
        Check {
            host: host.into(),
            id: id.into(),
            level,
            detail: detail.into(),
        }
    }
}

/// One task-traffic-controller resource scope as it is on disk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub state_dir: String,
    pub config_file: String,
    /// Something exists at the state directory or the configuration file.
    pub in_use: bool,
    /// `None`: the configuration could not be read.
    pub policy_digest: Option<String>,
    /// Entries in the ledger. `None`: the ledger could not be read.
    pub ledger_entries: Option<usize>,
}

/// The jobs of the `task-traffic-controller` command in its scope: the
/// ledger entries that have no managed sidecar (belt's own entries always
/// have one), told apart by their lease, which the owning process holds for
/// as long as it lives.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliJobs {
    /// Their owner holds the lease: the job is running or queued now.
    pub live: usize,
    /// Nobody holds the lease: the owner is gone and the entry is left over.
    pub stale: usize,
}

/// The `task-traffic-controller` binary the service environment resolves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficCli {
    pub path: String,
    /// `status --json` ran and printed a JSON object.
    pub status_readable: bool,
    /// That object has `ledger_format`: the binary knows managed entries.
    pub knows_managed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Docker {
    /// The `docker` binary the service environment resolves.
    pub cli: Option<String>,
    /// Where that client connects (`DOCKER_HOST`, else the current context).
    pub endpoint: Option<String>,
    /// The engine's own id. `None`: no engine answered.
    pub engine_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileFacts {
    pub name: Option<String>,
    pub fingerprint: Option<String>,
    pub error: Option<String>,
}

/// What one host found. Facts only; the parent decides what they mean.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostFacts {
    pub schema: u32,
    pub worker_scope: Scope,
    pub cli_scope: Scope,
    pub cli: Option<TrafficCli>,
    /// `None`: the command's ledger could not be read, or the host that
    /// answered predates the check.
    #[serde(default)]
    pub cli_jobs: Option<CliJobs>,
    /// Why `cli_jobs` is `None` when this host tried and failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_jobs_error: Option<String>,
    pub docker: Docker,
    pub profile: ProfileFacts,
    /// Lingering of the service user. Only a host whose service is a systemd
    /// user service reports it; a helper that predates the check does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub linger: Option<Linger>,
    /// What released import tokens still hold in the worker's source store
    /// (spec 25.8): `None` when there is no store, it could not be read, or
    /// the host predates the check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abandoned_imports: Option<AbandonedImports>,
}

/// Abandoned token roots, their memberships, and the bytes of the files
/// only they hold (no paths).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbandonedImports {
    pub roots: u64,
    pub members: u64,
    pub bytes: u64,
    /// More files than `doctor` stats: `bytes` is a lower bound.
    #[serde(default)]
    pub bytes_truncated: bool,
    /// Staging directories under `incoming/` whose import token record
    /// cannot be read: what their token holds is not released until a
    /// person looks (the record is kept, not guessed at).
    #[serde(default)]
    pub unreadable_tokens: u64,
}

fn abandoned_imports(worker_state: &Path) -> Option<AbandonedImports> {
    // read only, never makes a store
    let (roots, members, bytes, bytes_truncated) =
        crate::source::retention::abandoned_imports_read_only(worker_state).ok()??;
    Some(AbandonedImports {
        roots,
        members,
        bytes,
        bytes_truncated,
        unreadable_tokens: crate::worker::blobs::unreadable_import_tokens(worker_state),
    })
}

/// Runs one bounded command in a given environment. `None`: it could not be
/// run or did not finish. A test double stands in for the real programs.
pub trait Runner {
    fn run(
        &mut self,
        program: &Path,
        arguments: &[&str],
        env: &BTreeMap<String, String>,
    ) -> Option<(i32, String)>;
}

pub struct SystemRunner {
    /// How long one command may take before it is stopped and counts as unanswered.
    pub timeout: Duration,
}
impl Default for SystemRunner {
    fn default() -> Self {
        SystemRunner {
            timeout: COMMAND_TIMEOUT,
        }
    }
}
impl Runner for SystemRunner {
    fn run(
        &mut self,
        program: &Path,
        arguments: &[&str],
        env: &BTreeMap<String, String>,
    ) -> Option<(i32, String)> {
        let mut child = Command::new(program)
            .args(arguments)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let pipe = child.stdout.take()?;
        let reader = std::thread::spawn(move || {
            let mut text = String::new();
            let _ = pipe.take(OUTPUT_LIMIT).read_to_string(&mut text);
            text
        });
        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                _ => {
                    // This Child is the process spawned above, never an inferred pid.
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        };
        Some((status.code()?, reader.join().ok()?))
    }
}

/// The first executable regular file called `name` on the environment's PATH.
pub fn find_on_path(env: &BTreeMap<String, String>, name: &str) -> Option<PathBuf> {
    env.get("PATH")?
        .split(':')
        .filter(|directory| directory.starts_with('/'))
        .map(|directory| Path::new(directory).join(name))
        .find(|candidate| {
            std::fs::metadata(candidate)
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
}

/// Where the `task-traffic-controller` command keeps its state and reads its
/// configuration in this environment (its own rule: XDG, else under HOME).
pub fn cli_scope_paths(env: &BTreeMap<String, String>) -> (PathBuf, PathBuf) {
    let home = PathBuf::from(env.get("HOME").map_or("/", String::as_str));
    let base = |variable: &str, fallback: &str| {
        env.get(variable)
            .filter(|value| !value.is_empty())
            .map_or_else(|| home.join(fallback), PathBuf::from)
    };
    (
        base("XDG_STATE_HOME", ".local/state").join(TRAFFIC_CLI),
        base("XDG_CONFIG_HOME", ".config")
            .join(TRAFFIC_CLI)
            .join("config.toml"),
    )
}

/// The scope the worker uses: the task-traffic-controller rule applied to
/// the environment recorded at install, which is where the worker reads it
/// from (not from its process environment). An environment that resolves to
/// no scope is looked at by the command's own rule.
pub fn worker_scope_paths(recorded: &BTreeMap<String, String>) -> (PathBuf, PathBuf) {
    match crate::config::traffic::resolve(recorded) {
        Ok(scope) => (scope.state_dir, scope.config_file),
        Err(_) => cli_scope_paths(recorded),
    }
}

/// The digest task-traffic-controller itself gives an effective configuration.
fn policy_digest(config: &Config) -> Option<String> {
    let bytes = serde_json::to_vec(config).ok()?;
    Some(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn scope(state_dir: &Path, config_file: &Path) -> Scope {
    let exists = |path: &Path| std::fs::symlink_metadata(path).is_ok();
    let ledger = Paths::new(state_dir, config_file).ledger_file();
    Scope {
        state_dir: state_dir.to_string_lossy().into_owned(),
        config_file: config_file.to_string_lossy().into_owned(),
        in_use: exists(state_dir) || exists(config_file),
        policy_digest: Config::load(config_file)
            .ok()
            .as_ref()
            .and_then(policy_digest),
        ledger_entries: if exists(&ledger) {
            Ledger::load(&ledger).ok().map(|l| l.entries.len())
        } else {
            Some(0)
        },
    }
}

/// Who holds the entries of one scope, read without writing anything. The
/// lease is the library's own proof that an owner lives (a PID alone is
/// reused), so no process is looked up or signalled here.
fn cli_jobs(state_dir: &Path, config_file: &Path) -> Result<CliJobs, String> {
    let paths = Paths::new(state_dir, config_file);
    if std::fs::symlink_metadata(paths.ledger_file()).is_err() {
        return Ok(CliJobs::default());
    }
    let context = Context::new(paths);
    let snapshot = lock::snapshot_read_only(&context).map_err(|e| e.to_string())?;
    let entries = &snapshot.ledger.entries;
    let managed = |id: &str| context.paths.managed_file(id).exists();
    // A nested `exec` inside a belt attempt has no sidecar of its own but
    // belongs to the managed entry its parent chain leads to.
    let belts = |entry: &task_traffic_controller::ledger::Entry| {
        let mut at = entry;
        for _ in 0..entries.len() {
            if managed(&at.id) {
                return true;
            }
            match at
                .parent
                .as_ref()
                .and_then(|p| entries.iter().find(|e| &e.id == p))
            {
                Some(up) => at = up,
                None => return false,
            }
        }
        false
    };
    let mut jobs = CliJobs::default();
    for entry in entries.iter().filter(|e| !belts(e)) {
        // A lease file the doctor user cannot open counts as stale.
        if snapshot.suspect_dead.contains(&entry.id) {
            jobs.stale += 1;
        } else {
            jobs.live += 1;
        }
    }
    Ok(jobs)
}

fn traffic_cli(env: &BTreeMap<String, String>, runner: &mut dyn Runner) -> Option<TrafficCli> {
    let path = find_on_path(env, TRAFFIC_CLI)?;
    let answer = runner
        .run(&path, &["status", "--json"], env)
        .filter(|(code, _)| *code == 0)
        .and_then(|(_, text)| serde_json::from_str::<serde_json::Value>(&text).ok())
        .filter(serde_json::Value::is_object);
    Some(TrafficCli {
        path: path.to_string_lossy().into_owned(),
        status_readable: answer.is_some(),
        knows_managed: answer.is_some_and(|value| value.get("ledger_format").is_some()),
    })
}

fn docker(env: &BTreeMap<String, String>, runner: &mut dyn Runner) -> Docker {
    let Some(path) = find_on_path(env, "docker") else {
        return Docker::default();
    };
    let mut line = |arguments: &[&str]| {
        runner
            .run(&path, arguments, env)
            .filter(|(code, _)| *code == 0)
            .map(|(_, text)| text.trim().to_string())
            .filter(|text| !text.is_empty() && !text.contains('\n'))
    };
    let endpoint = env
        .get("DOCKER_HOST")
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| {
            line(&[
                "context",
                "inspect",
                "--format",
                "{{.Endpoints.docker.Host}}",
            ])
        });
    Docker {
        engine_id: line(&["info", "--format", "{{.ID}}"]),
        endpoint,
        cli: Some(path.to_string_lossy().into_owned()),
    }
}

fn profile(worker_state: &Path, env: &BTreeMap<String, String>) -> ProfileFacts {
    match crate::toolchain::profile::Profile::load(worker_state, env) {
        Ok(profile) => ProfileFacts {
            name: Some(profile.name().to_string()),
            fingerprint: Some(profile.startup_report().profile_fingerprint.to_string()),
            error: None,
        },
        Err(error) => ProfileFacts {
            name: None,
            fingerprint: None,
            error: Some(error),
        },
    }
}

/// Look at this host: `worker_state` is the worker's state directory, `env`
/// the environment its service runs with and `recorded` the environment
/// recorded at install, which places the task-traffic-controller scope.
pub fn gather(
    worker_state: &Path,
    env: &BTreeMap<String, String>,
    recorded: &BTreeMap<String, String>,
    runner: &mut dyn Runner,
) -> HostFacts {
    let (worker_dir, worker_config) = worker_scope_paths(recorded);
    // The command the worker's steps run sees the recorded environment.
    let (cli_dir, cli_config) = cli_scope_paths(recorded);
    let (cli_jobs, cli_jobs_error) = match cli_jobs(&cli_dir, &cli_config) {
        Ok(jobs) => (Some(jobs), None),
        Err(why) => (None, Some(why)),
    };
    HostFacts {
        schema: 1,
        worker_scope: scope(&worker_dir, &worker_config),
        cli_scope: scope(&cli_dir, &cli_config),
        cli: traffic_cli(env, runner),
        cli_jobs,
        cli_jobs_error,
        docker: docker(env, runner),
        profile: profile(worker_state, env),
        linger: None,
        abandoned_imports: abandoned_imports(worker_state),
    }
}

/// Whether lingering is enabled for the user `uid`, asked of `loginctl` in
/// the service environment. `None` for a service manager that has no such
/// thing (launchd). A `loginctl` that is missing or does not answer is
/// reported as no answer, never as enabled.
pub fn linger_fact(
    backend: Backend,
    uid: u32,
    env: &BTreeMap<String, String>,
    runner: &mut dyn Runner,
) -> Option<Linger> {
    if backend != Backend::Systemd {
        return None;
    }
    let program = find_on_path(env, "loginctl").unwrap_or_else(|| PathBuf::from(LOGINCTL));
    let arguments = linger::arguments(uid);
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    let enabled = runner
        .run(&program, &arguments, env)
        .and_then(|(code, stdout)| linger::parse(code, &stdout));
    Some(Linger { uid, enabled })
}

/// An endpoint on this host: a Unix socket or a pipe, not a network address.
fn local_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with("unix://") || endpoint.starts_with("npipe://")
}

/// The checks one host's facts give.
pub fn evaluate(host: &str, facts: &HostFacts) -> Vec<Check> {
    let check = |id: &str, level, detail: String| Check::new(host, id, level, detail);
    let mut out = Vec::new();
    // The separate CLI matters when it is installed or has left state behind.
    let cli_present = facts.cli.is_some() || facts.cli_scope.in_use;
    out.push(match &facts.cli {
        None => check(
            "traffic_runtime",
            Level::Ok,
            "no separate task-traffic-controller command in the service environment".into(),
        ),
        Some(cli) if !cli.status_readable => check(
            "traffic_runtime",
            Level::Fail,
            format!(
                "{} did not answer `status --json`; its compatibility is unknown",
                cli.path
            ),
        ),
        Some(cli) if !cli.knows_managed => check(
            "traffic_runtime",
            Level::Fail,
            format!(
                "{} does not know managed entries (no `ledger_format` in `status --json`); drain its jobs ({}) and update it before it shares a scope with belt",
                cli.path,
                match facts.cli_scope.ledger_entries {
                    Some(n) => format!("{n} in its ledger"),
                    None => "its ledger is unreadable".into(),
                }
            ),
        ),
        Some(cli) => check(
            "traffic_runtime",
            Level::Ok,
            format!("{} knows managed entries", cli.path),
        ),
    });
    // A job started by an older command keeps running that older code
    // whatever the PATH resolves now. Such a job is an entry of the
    // command's scope that is not belt's (no sidecar) and whose owner still
    // holds its lease. Without a command and without state there is none.
    out.push(if !cli_present {
        check(
            "traffic_processes",
            Level::Ok,
            "no separate task-traffic-controller command or state, so none of its jobs can be running".into(),
        )
    } else {
        match &facts.cli_jobs {
            None => check(
                "traffic_processes",
                Level::NotChecked,
                match (&facts.cli_jobs_error, facts.cli_scope.ledger_entries) {
                    (Some(why), _) => format!("the command's jobs could not be read ({why})"),
                    (None, None) => "the command's ledger is unreadable, so its running jobs cannot be told".into(),
                    (None, Some(_)) => "this host did not report the command's jobs (its helper predates the check); update it".into(),
                },
            ),
            Some(jobs) if jobs.live > 0 => {
                let old = facts
                    .cli
                    .as_ref()
                    .is_some_and(|cli| !cli.status_readable || !cli.knows_managed);
                let text = format!(
                    "{} job(s) of the command are running in its scope ({} left-over entries)",
                    jobs.live, jobs.stale
                );
                if old {
                    check(
                        "traffic_processes",
                        Level::Fail,
                        format!("{text} under a command that does not know managed entries: let them finish and update the command before it shares a scope with belt"),
                    )
                } else {
                    check(
                        "traffic_processes",
                        Level::Notice,
                        format!("{text}; they are not belt's, and a job started by an older build keeps its code: let them finish before updating the command"),
                    )
                }
            }
            Some(jobs) if jobs.stale > 0 => check(
                "traffic_processes",
                Level::Notice,
                format!(
                    "{} entries of the command's ledger have no live owner; the next task-traffic-controller run releases them",
                    jobs.stale
                ),
            ),
            Some(_) => check(
                "traffic_processes",
                Level::Ok,
                "no job of the command is running".into(),
            ),
        }
    });
    let same_scope = facts.worker_scope.state_dir == facts.cli_scope.state_dir
        && facts.worker_scope.config_file == facts.cli_scope.config_file;
    out.push(if same_scope {
        check(
            "traffic_state_path",
            Level::Ok,
            format!("one scope: {}", facts.worker_scope.state_dir),
        )
    } else {
        check(
            "traffic_state_path",
            if cli_present {
                Level::Fail
            } else {
                Level::Notice
            },
            format!(
                "the worker uses {} and the task-traffic-controller command uses {}: two ledgers, so two budgets{}",
                facts.worker_scope.state_dir,
                facts.cli_scope.state_dir,
                if cli_present {
                    ""
                } else {
                    " (the command is not installed and has no state here)"
                }
            ),
        )
    });
    out.push(
        match (
            &facts.worker_scope.policy_digest,
            &facts.cli_scope.policy_digest,
        ) {
            (Some(worker), Some(cli)) if worker == cli => check(
                "traffic_policy",
                Level::Ok,
                format!("one effective policy: {worker}"),
            ),
            (Some(worker), Some(cli)) => check(
                "traffic_policy",
                if cli_present {
                    Level::Fail
                } else {
                    Level::Notice
                },
                format!("the worker's policy is {worker}, the command's is {cli}"),
            ),
            (worker, _) => check(
                "traffic_policy",
                Level::Fail,
                format!(
                    "the {} configuration could not be read",
                    if worker.is_none() {
                        "worker scope's"
                    } else {
                        "command's"
                    }
                ),
            ),
        },
    );
    out.push(match (&facts.docker.cli, &facts.docker.engine_id) {
        (None, _) => check(
            "docker_resource_domain",
            Level::Ok,
            "no docker client in the service environment".into(),
        ),
        (Some(_), _)
            if facts
                .docker
                .endpoint
                .as_deref()
                .is_some_and(|e| !local_endpoint(e)) =>
        {
            check(
                "docker_resource_domain",
                Level::Fail,
                format!(
                    "the docker client connects to {}: an engine on another host is a resource domain this version does not manage",
                    facts.docker.endpoint.as_deref().unwrap_or_default()
                ),
            )
        }
        (Some(cli), None) => check(
            "docker_resource_domain",
            Level::Notice,
            format!("{cli} is installed but no engine answered in the service environment"),
        ),
        (Some(_), Some(engine)) if facts.docker.endpoint.is_none() => check(
            "docker_resource_domain",
            Level::NotChecked,
            format!(
                "engine {engine} answered, but where the docker client connects could not be determined (no DOCKER_HOST and no single endpoint from `docker context inspect`); it is not confirmed to be on this host"
            ),
        ),
        (Some(_), Some(engine)) => check(
            "docker_resource_domain",
            Level::Ok,
            format!("engine {engine} on this host"),
        ),
    });
    out.push(match &facts.profile {
        ProfileFacts {
            error: Some(error), ..
        } => check(
            "service_profile",
            Level::Fail,
            format!("the worker profile does not load in the service environment: {error}"),
        ),
        ProfileFacts {
            name, fingerprint, ..
        } => check(
            "service_profile",
            Level::Ok,
            format!(
                "profile {} ({})",
                name.as_deref().unwrap_or("?"),
                fingerprint.as_deref().unwrap_or("?")
            ),
        ),
    });
    if let Some(linger) = &facts.linger {
        out.push(linger::check(host, linger));
    }
    if let Some(a) = facts
        .abandoned_imports
        .filter(|a| a.roots > 0 || a.unreadable_tokens > 0)
    {
        let at_least = if a.bytes_truncated { "at least " } else { "" };
        let mut text = format!(
            "{} released import token(s) still hold {} membership(s), {at_least}{} byte(s) only they hold; they go once the store's retention gates open",
            a.roots, a.members, a.bytes
        );
        if a.unreadable_tokens > 0 {
            text.push_str(&format!(
                "; {} incoming source(s) have an import token record that cannot be read and keep what they hold",
                a.unreadable_tokens
            ));
        }
        out.push(check("abandoned_imports", Level::Notice, text));
    }
    out
}

/// Hosts that reach the same Docker engine: its capacity would be counted
/// once per host. `hosts` is `(name, engine id)`.
pub fn shared_engines(hosts: &[(String, Option<String>)]) -> Vec<Check> {
    let mut by_engine: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (host, engine) in hosts {
        if let Some(engine) = engine {
            by_engine.entry(engine).or_default().push(host);
        }
    }
    by_engine
        .into_iter()
        .filter(|(_, hosts)| hosts.len() > 1)
        .flat_map(|(engine, hosts)| {
            let all = hosts.join(", ");
            hosts.into_iter().map(move |host| {
                Check::new(
                    host,
                    "docker_shared_engine",
                    Level::Fail,
                    format!("engine {engine} is also used by: {all}; one engine must not be counted as separate resources"),
                )
            })
        })
        .collect()
}

/// The checks a worker's service status gives. `parent_version` is the
/// digest of the binary that runs `doctor`.
pub fn service_checks(host: &str, status: &Status, parent_version: &str) -> Vec<Check> {
    let short = |digest: &str| digest.get(..12).unwrap_or(digest).to_string();
    let service = if status.switch_running {
        Check::new(
            host,
            "service",
            Level::NotChecked,
            "a version switch is running on the host; run doctor again when it has finished",
        )
    } else if status.switching_from.is_some() {
        Check::new(
            host,
            "service",
            Level::Fail,
            "a version switch is unfinished or awaits its commit; run workers upgrade",
        )
    } else {
        match (status.loaded, status.ready) {
            (Some(true), true) if status.draining => {
                Check::new(host, "service", Level::Notice, "running, draining")
            }
            (Some(true), true) => Check::new(host, "service", Level::Ok, "running"),
            (Some(true), false) => Check::new(
                host,
                "service",
                Level::Fail,
                "loaded, but the agent does not accept connections",
            ),
            (Some(false), _) => Check::new(host, "service", Level::Fail, "not loaded"),
            (None, _) => Check::new(
                host,
                "service",
                Level::Fail,
                "the loaded service could not be identified",
            ),
        }
    };
    let version = if status.binary_sha256 == parent_version {
        Check::new(
            host,
            "version",
            Level::Ok,
            format!("same build as the parent ({})", short(parent_version)),
        )
    } else {
        Check::new(
            host,
            "version",
            Level::Notice,
            format!(
                "runs {}, the parent is {}; run workers upgrade (a worker whose OS or architecture differs from the parent's runs a build of its own and always shows another digest: upgrade it with --binary when the parent's version changes)",
                short(&status.binary_sha256),
                short(parent_version)
            ),
        )
    };
    vec![service, version]
}

/// What a worker host answers to `worker-service doctor`.
#[derive(Debug, Serialize, Deserialize)]
pub struct HostAnswer {
    pub status: Status,
    pub facts: HostFacts,
}

/// The hidden `worker-service doctor`: facts about this host on stdout.
pub fn host_main() -> i32 {
    let env: BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let launcher = super::worker_service::HostLauncher::native();
    let backend = super::macos::Launcher::backend(&launcher);
    let answer = super::worker_service::service_view(&env, launcher).map(
        |(status, state, service_env, recorded)| {
            let mut runner = SystemRunner::default();
            let mut facts = gather(&state, &service_env, &recorded, &mut runner);
            if !status.system_user {
                facts.linger = linger_fact(
                    backend,
                    rustix::process::geteuid().as_raw(),
                    &service_env,
                    &mut runner,
                );
            }
            HostAnswer { status, facts }
        },
    );
    match answer {
        Ok(answer) => {
            println!("{}", serde_json::json!({"ok": true, "doctor": answer}));
            0
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({"ok": false, "error": format!("{error:#}")})
            );
            1
        }
    }
}

/// Read a worker's answer. Every way of not getting one is a reason, never
/// a pass.
fn parse_answer(output: &RemoteOutput) -> Result<HostAnswer, String> {
    if output.code == Some(255) {
        return Err("the worker could not be reached over ssh".into());
    }
    let line = output
        .stdout
        .split(|b| *b == b'\n')
        .rfind(|l| !l.is_empty())
        .ok_or_else(|| {
            format!(
                "the worker helper gave no answer (exit {:?}); a helper that predates doctor answers like this, run workers upgrade",
                output.code
            )
        })?;
    let value: serde_json::Value = serde_json::from_slice(line)
        .map_err(|_| "the worker helper answer was unreadable".to_string())?;
    if value["ok"] == true && output.code == Some(0) {
        return serde_json::from_value(value["doctor"].clone())
            .map_err(|_| "the worker facts were unreadable; run workers upgrade".to_string());
    }
    Err(format!(
        "the worker helper refused: {}",
        value["error"].as_str().unwrap_or("unspecified")
    ))
}

#[derive(Debug, Serialize)]
pub struct Report {
    /// No check failed and none was left out. A check that was not made is
    /// not a pass: with any `not_checked` entry the report is not healthy.
    pub healthy: bool,
    pub failed: usize,
    pub not_checked: usize,
    pub checks: Vec<Check>,
}
impl Report {
    fn of(checks: Vec<Check>) -> Self {
        let count = |level| checks.iter().filter(|c| c.level == level).count();
        let failed = count(Level::Fail);
        Report {
            healthy: failed == 0 && count(Level::NotChecked) == 0,
            failed,
            not_checked: count(Level::NotChecked),
            checks,
        }
    }
    /// Why the command does not exit 0, if it does not. A failed check is
    /// `BLOCKED`. A report with nothing failed but something left out exits
    /// differently (1), so a caller can tell "wrong" from "not known".
    pub fn problem(&self) -> Option<(ErrorCode, String)> {
        if self.failed > 0 {
            return Some((
                ErrorCode::Blocked,
                format!("{} check(s) failed", self.failed),
            ));
        }
        (self.not_checked > 0).then(|| {
            (
                ErrorCode::CommandFailed,
                format!(
                    "{} check(s) were not made; nothing failed, but the report is incomplete",
                    self.not_checked
                ),
            )
        })
    }
}

/// Unsupported legacy keys of the project the command was run in. Only keys
/// that carry a value are said; a project whose configuration could not be
/// resolved is listed as not looked at.
pub fn project_checks(notices: Result<&[crate::config::resolve::Notice], String>) -> Vec<Check> {
    match notices {
        Ok(notices) => notices
            .iter()
            .map(|notice| {
                Check::new(
                    "project",
                    "legacy_config",
                    Level::Notice,
                    format!("`{}` in {}: {}", notice.key, notice.file, notice.reason),
                )
            })
            .collect(),
        Err(why) => vec![Check::new(
            "project",
            "legacy_config",
            Level::NotChecked,
            format!("the project configuration here could not be resolved ({why}); unsupported legacy keys were not looked for"),
        )],
    }
}

/// Where the checks of `workers diff` (§22.2) join the report: asked once
/// for every worker that answered. `None` leaves the comparison out, and the
/// report says so per worker. It is called from the thread that looks at that
/// worker, with the handle to ask over, so workers are looked at together.
pub type DiffSource<'a> = Option<&'a (dyn Fn(&mut dyn Remote, &str) -> Vec<Check> + Sync)>;

/// The difference `workers diff` found for one worker, as checks: one per
/// item. `same` is ok. Only what a person has to fix fails (`differs`,
/// `missing`, `manual_required`). `auto_installable` is a notice: an exec's
/// warm-up and `workers sync` install it. An item neither side measured is a
/// notice too. A difference that could not be taken at all is a check that
/// was not made: never a pass, never an empty list.
pub fn diff_checks(host: &str, measured: Result<&[Item], &str>) -> Vec<Check> {
    let left_out = |why: &str| {
        vec![Check::new(
            host,
            "environment_diff",
            Level::NotChecked,
            workers::strip_control(&format!("the environments were not compared: {why}")),
        )]
    };
    let items = match measured {
        Ok([]) => return left_out("the comparison listed nothing"),
        Ok(items) => items,
        Err(why) => return left_out(why),
    };
    items
        .iter()
        .map(|item| {
            let level = match item.class {
                Class::Same => Level::Ok,
                Class::AutoInstallable | Class::NotMeasured => Level::Notice,
                Class::Differs | Class::Missing | Class::ManualRequired => Level::Fail,
            };
            let class = serde_json::to_value(item.class)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            let mut detail = format!(
                "{}: {class} parent={} worker={}",
                item.subject,
                item.parent.as_deref().unwrap_or("-"),
                item.worker.as_deref().unwrap_or("-")
            );
            if let Some(steps) = &item.steps {
                detail.push_str(&format!(" -> {steps}"));
            }
            match item.class {
                Class::AutoInstallable => detail.push_str(&format!(
                    " (an exec installs it; `belt workers sync {host}` installs it now)"
                )),
                Class::NotMeasured => detail.push_str(" (not measured yet)"),
                _ => {}
            }
            // Values come from the worker's report.
            Check::new(
                host,
                "environment_diff",
                level,
                workers::strip_control(&detail),
            )
        })
        .collect()
}

/// The parent's own measurement, in the retained service environment and
/// never in the invoking shell's: the rule `workers diff` follows.
fn measure_parent(
    spec: &Spec,
    collect: impl FnOnce(&Path, &BTreeMap<String, String>) -> Result<EnvReport, String>,
) -> Result<EnvReport, String> {
    collect(&spec.worker_state_dir, &spec.env)
}

/// `workers diff` for one worker, as doctor's checks. `parent` is the
/// parent's own measurement, taken once for all workers; when it failed no
/// worker is asked.
pub fn worker_diff(
    remote: &mut (impl Remote + ?Sized),
    env: &BTreeMap<String, String>,
    exe: &Path,
    parent: Result<&EnvReport, &str>,
    host: &str,
) -> Vec<Check> {
    let items = parent
        .map_err(|why| format!("the parent's environment could not be measured ({why})"))
        .and_then(|parent| {
            workers::fetch_env(remote, env, exe, host)
                .map(|worker| envdiff::diff(parent, &worker))
                .map_err(|error| error.message)
        });
    diff_checks(host, items.as_deref().map_err(String::as_str))
}

/// What `belt exec` uses when no resident runs (direct execution): the
/// scope of this shell's environment, read by the embedded core. It shares
/// one budget with the resident only when the two scopes are the same.
pub fn direct_checks(env: &BTreeMap<String, String>, resident: &Scope) -> Vec<Check> {
    let check = |id: &str, level, detail: String| Check::new("parent", id, level, detail);
    let scope = match crate::config::traffic::resolve(env) {
        Ok(scope) => scope,
        Err(e) => {
            return vec![check(
                "direct_exec_scope",
                Level::Notice,
                format!("`belt exec` without a resident cannot run in this shell: {e}"),
            )]
        }
    };
    let state = scope.state_dir.to_string_lossy();
    let config = scope.config_file.to_string_lossy();
    let mut out = vec![
        if state == resident.state_dir && config == resident.config_file {
            check(
                "direct_exec_scope",
                Level::Ok,
                format!("`belt exec` without a resident shares the resident's scope: {state}"),
            )
        } else {
            check(
            "direct_exec_scope",
            Level::Notice,
            format!(
                "`belt exec` without a resident uses {state} (this shell's environment) and the resident uses {}: two ledgers, so two budgets",
                resident.state_dir
            ),
        )
        },
    ];
    let ledger = Paths::new(&scope.state_dir, &scope.config_file).ledger_file();
    out.push(match Ledger::load(&ledger) {
        Ok(_) => check(
            "direct_exec_ledger",
            Level::Ok,
            format!("the embedded task-traffic-controller reads {}", ledger.display()),
        ),
        Err(e) => check(
            "direct_exec_ledger",
            Level::Fail,
            format!(
                "the embedded task-traffic-controller cannot read {} ({e}), so `belt exec` without a resident refuses to run and leaves it unchanged; a newer task-traffic-controller may have written it: update belt, or let that command's jobs finish",
                ledger.display()
            ),
        ),
    });
    out
}

/// The captures nothing can decide about (spec 25.4 (b), 25.11): owner rows
/// whose owner was recorded where this host cannot see it, and run records
/// and in-place bindings that neither a run nor an owner row claims (a
/// binary from before the owner row left them, or wrote them a moment ago).
/// They are never deleted automatically; this only counts them. Read-only:
/// nothing is created, locked or changed.
pub fn capture_checks(spec: &Spec) -> Vec<Check> {
    let check = |level, detail: String| Check::new("parent", "unsubmitted_captures", level, detail);
    // A host without a source index has no capture to count.
    if !spec
        .state_dir
        .join("sources")
        .join("retention.db")
        .is_file()
    {
        return Vec::new();
    }
    let Ok(store) = super::bootstrap::read_only_store(spec) else {
        return vec![check(
            Level::NotChecked,
            "the coordinator database cannot be read".into(),
        )];
    };
    let counted = unjudged_captures(&spec.state_dir, &|run| store.has_run(run).unwrap_or(true));
    match counted {
        Ok(None) => Vec::new(),
        Err(_) => vec![check(
            Level::NotChecked,
            "the index of captured inputs cannot be read".into(),
        )],
        Ok(Some(counts)) => {
            let (level, detail) = capture_detail(&counts);
            vec![check(level, detail)]
        }
    }
}

/// The level and the text of the check for what [`unjudged_captures`] counted.
fn capture_detail(counts: &UnjudgedCaptures) -> (Level, String) {
    if *counts == UnjudgedCaptures::default() {
        return (
            Level::Ok,
            "every capture that was not submitted has an owner this host can judge".into(),
        );
    }
    let mut detail = format!(
        "kept because nothing proves they are abandoned: {} capture(s) whose owner was recorded on another machine or PID namespace, {} run record(s) and {} in-place binding(s) that no run and no capture owner claims, {} entr(ies) with a name that is not a run's; they are never deleted automatically",
        counts.elsewhere, counts.records, counts.bindings, counts.unnamed
    );
    if counts.corrupt_held > 0 {
        detail.push_str(&format!(
            "; {} file(s) of a stopped capture were kept because their content is wrong and another root holds them (belt cannot remove them while they are held)",
            counts.corrupt_held
        ));
    }
    if counts.retry_failing != (0, 0, 0) {
        detail.push_str(&format!(
            "; {} manifest(s) of the legacy registration, {} received source(s) of the clone ref fill and {} object(s) of the size fill failed to be read 8 times or more (belt keeps retrying them)",
            counts.retry_failing.0, counts.retry_failing.1, counts.retry_failing.2
        ));
    }
    (Level::Notice, detail)
}

/// The runs whose pruning began and has not reached its end (spec 25.5):
/// usually none outside a pass. A run stays here while a reader holds its
/// results, while a file of it cannot be proven ours, or while the source
/// store is busy; nothing of it that cannot be proven is deleted, and every
/// pass tries again. Read-only.
pub fn pruning_checks(spec: &Spec) -> Vec<Check> {
    if !spec.state_dir.join("coordinator.db").is_file() {
        return Vec::new();
    }
    let check = |level, detail: String| Check::new("parent", "run_pruning", level, detail);
    let Ok(store) = super::bootstrap::read_only_store(spec) else {
        return Vec::new();
    };
    let gate = crate::source::retention::Gate::closed_read_only(&spec.state_dir)
        .ok()
        .flatten();
    pruning_report(store.marked_run_count().ok(), gate)
        .map(|(level, detail)| vec![check(level, detail)])
        .unwrap_or_default()
}

/// What [`pruning_checks`] says for a count of marked runs (`None`: the
/// count could not be read). Nothing for no run.
/// `gate` is the gate of the source store that holds the deletion of their
/// inputs, if one does.
fn pruning_report(
    runs: Option<u64>,
    gate: Option<crate::source::retention::Gate>,
) -> Option<(Level, String)> {
    match runs {
        Some(0) => None,
        Some(runs) => Some((
            Level::Notice,
            format!(
                "{runs} run(s) past their retention are being deleted; they are no longer shown, and what is left of them (kept while a reader holds it or while it cannot be proven to be belt's) is tried again every minute{}",
                match gate {
                    Some(gate) => format!("; their inputs wait for the source store (gate={})", gate.as_str()),
                    None => String::new(),
                }
            ),
        )),
        None => Some((
            Level::NotChecked,
            "the runs being deleted cannot be counted".into(),
        )),
    }
}

/// What [`capture_checks`] counts.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct UnjudgedCaptures {
    /// Owner rows whose owner cannot be judged from this host.
    pub(crate) elsewhere: usize,
    /// `sources/runs/<run>` of no run and no owner row.
    pub(crate) records: usize,
    /// `in-place/<run>.json` of no run and no owner row.
    pub(crate) bindings: usize,
    /// Entries of those two directories whose name is not a run's.
    pub(crate) unnamed: usize,
    /// Files with wrong content that a deletion kept because another root
    /// holds them.
    pub(crate) corrupt_held: usize,
    /// Legacy manifests, `clone_ref` fill roots and size fill objects that
    /// failed 8 times or more (they are still retried).
    pub(crate) retry_failing: (usize, usize, usize),
}

/// One `readdir` of `sources/runs/` and of `in-place/`, and one lookup by
/// primary key for each entry. `None` when this host holds no source index.
pub(crate) fn unjudged_captures(
    state: &Path,
    has_run: &dyn Fn(&crate::ids::RunId) -> bool,
) -> anyhow::Result<Option<UnjudgedCaptures>> {
    let Some(owners) = crate::source::retention::inspect_owners(state)? else {
        return Ok(None);
    };
    let mut counts = UnjudgedCaptures {
        elsewhere: owners
            .iter()
            .filter(|row| row.verdict() == crate::source::retention::CaptureVerdict::Elsewhere)
            .count(),
        ..Default::default()
    };
    let owned: std::collections::BTreeSet<&str> =
        owners.iter().map(|row| row.run.as_str()).collect();
    let mut unclaimed = |dir: PathBuf, suffix: &str| -> usize {
        let mut found = 0;
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let name = entry.file_name();
            let run = name.to_str().and_then(|name| {
                if name.ends_with(".lock") {
                    return Some(None);
                }
                let id = name.strip_suffix(suffix)?;
                crate::ids::RunId::parse(id).ok().map(Some)
            });
            match run {
                // The lock of a run is judged with its record.
                Some(None) => {}
                Some(Some(run)) => {
                    if !owned.contains(run.as_str()) && !has_run(&run) {
                        found += 1;
                    }
                }
                None => counts.unnamed += 1,
            }
        }
        found
    };
    let records = unclaimed(state.join("sources").join("runs"), "");
    let bindings = unclaimed(state.join("in-place"), ".json");
    counts.records = records;
    counts.bindings = bindings;
    counts.corrupt_held = usize::try_from(crate::source::retention::inspect_corrupt_held(state)?)
        .unwrap_or(usize::MAX);
    let (legacy, refs, sizes) = crate::source::retention::inspect_retry_failing(state)?;
    counts.retry_failing = (
        usize::try_from(legacy).unwrap_or(usize::MAX),
        usize::try_from(refs).unwrap_or(usize::MAX),
        usize::try_from(sizes).unwrap_or(usize::MAX),
    );
    Ok(Some(counts))
}

/// Everything `doctor` looks at. `env` is the invoking environment (it
/// locates the user configuration); `spec` is the initialized coordinator.
pub fn run(
    remote: &mut impl Remote,
    runner: &mut dyn Runner,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    parent_version: &str,
    parent_backend: Backend,
    diff: DiffSource,
) -> Result<Report, ApiError> {
    let cluster = spec.expected_cluster.clone().ok_or_else(|| {
        ApiError::new(
            ErrorCode::InvalidInput,
            "the coordinator has no cluster identity; run init",
        )
    })?;
    let mut checks = Vec::new();
    for warning in super::group::removal_warnings(spec, remote)? {
        checks.push(Check::new(
            &warning.name,
            warning.code,
            Level::Notice,
            warning.message,
        ));
    }

    let mut engines = Vec::new();
    // The parent's own worker, in the environment its service was given.
    let retained = spec.env.contains_key("HOME");
    checks.push(if retained {
        Check::new(
            "parent",
            "service_environment",
            Level::Ok,
            "checked in the environment retained for the service",
        )
    } else {
        Check::new(
            "parent",
            "service_environment",
            Level::Notice,
            "no service environment is retained; checked in this shell's environment, which the service may not share",
        )
    });
    let mut local = gather(
        &spec.worker_state_dir,
        if retained { &spec.env } else { env },
        if retained { &spec.env } else { env },
        runner,
    );
    // The parent's own worker runs inside the coordinator's service: only a
    // parent that `init` registered has a service whose user can linger.
    if spec.managed_service.is_some() {
        local.linger = linger_fact(
            parent_backend,
            rustix::process::geteuid().as_raw(),
            if retained { &spec.env } else { env },
            runner,
        );
    }
    checks.extend(evaluate("parent", &local));
    checks.extend(direct_checks(env, &local.worker_scope));
    checks.extend(capture_checks(spec));
    checks.extend(pruning_checks(spec));
    let leftover = super::leftover_temporaries(env);
    if leftover > 0 {
        checks.push(Check::new(
            "parent",
            "leftover_temporaries",
            Level::Notice,
            format!(
                "{leftover} temporary file(s) named .belt-tmp.* are left in the configuration, state or service directories: their owner is not proven gone (another host, a running process, or a name belt cannot read); belt removes only the ones it can prove abandoned"
            ),
        ));
    }
    let kept = super::old_binaries::kept_versions(env);
    if kept > 0 {
        checks.push(Check::new(
            "parent",
            "kept_versions",
            Level::Notice,
            format!(
                "{kept} earlier version(s) of belt are kept in the configuration directory's bin/: something still names them (a service definition, a journal, a trial) or they could not be proven unused (a setup builder or an attempt was running); belt removes them at the next commit or start that can prove it"
            ),
        ));
    }
    if let Some(why) = super::old_binaries::unreadable_reason(env) {
        checks.push(Check::new(
            "parent",
            "versions_unexamined",
            Level::Notice,
            format!("no earlier version of belt can be removed while this stays: {why}"),
        ));
    }
    for notice in crate::setup::builder::toolchain_doctor_notices(
        &spec.worker_state_dir,
        &spec.worker_id,
        crate::platform::wall_ms(),
    ) {
        checks.push(Check::new(
            "parent",
            "toolchain_roots",
            Level::Notice,
            notice,
        ));
    }
    engines.push(("parent".to_string(), local.docker.engine_id.clone()));
    // Every worker is looked at at the same time (up to a bound), each on its
    // own ssh processes; what is printed keeps the order of the registrations.
    let registered = workers::registrations(env)?;
    let looked = workers::fan_out(remote, registered.iter().collect(), &|handle, worker| {
        look_at(handle, worker, &cluster, parent_version, diff)
    });
    for one in looked {
        checks.extend(one.checks);
        engines.extend(one.engine);
    }
    checks.extend(shared_engines(&engines));
    Ok(Report::of(checks))
}

/// An ephemeral worker has no service and no helper installed by the
/// parent: the only thing to look at is whether its bridge answers `hello`
/// as this worker. One that does not answer is a notice, not a failure: it
/// exists only while its user runs it.
fn look_at_ephemeral(
    remote: &mut dyn Remote,
    worker: &workers::Registered,
    cluster: &ClusterId,
    checks: &mut Vec<Check>,
) {
    let host = worker.id.as_str();
    checks.push(Check::new(
        host,
        "service",
        Level::Notice,
        "ephemeral worker: it has no service; it is started on its own host with worker --ephemeral and is not installed, upgraded or controlled from here; its environment is not compared with the parent's",
    ));
    let hello = SshTarget::parse(&worker.ssh)
        .map_err(|e| e.to_string())
        .and_then(|target| {
            remote.bridge_hello(&target, worker.binary.as_deref().unwrap_or("belt"), cluster)
        });
    let (level, detail) = match hello {
        Ok((answered_cluster, answered_worker))
            if answered_worker.as_str() == worker.id && &answered_cluster == cluster =>
        {
            (
                Level::Ok,
                "hello completed with every required feature".to_string(),
            )
        }
        Ok(_) => (
            Level::Fail,
            "the worker bridge answered for a different worker or cluster".to_string(),
        ),
        Err(why) => (
            Level::Notice,
            format!("the worker bridge did not complete hello ({why}); an ephemeral worker answers only while it is started on its host. If it is gone for good, workers forget {host} ends what it held"),
        ),
    };
    checks.push(Check::new(host, "bridge", level, detail));
}

/// What one worker contributes to the report.
struct Looked {
    checks: Vec<Check>,
    /// The worker's Docker engine, for the check that workers do not share one.
    engine: Option<(String, Option<String>)>,
}

/// One worker, with every question it is asked. The doctor helper answers
/// first; the bridge `hello` and the environment comparison then depend only
/// on that answer, so they are asked at the same time.
fn look_at(
    remote: &mut dyn Remote,
    worker: &workers::Registered,
    cluster: &ClusterId,
    parent_version: &str,
    diff: DiffSource,
) -> Looked {
    let host = worker.id.as_str();
    let mut checks = Vec::new();
    let mut engine = None;
    if worker.ephemeral {
        look_at_ephemeral(remote, worker, cluster, &mut checks);
        return Looked { checks, engine };
    }
    match ask_worker(remote, worker, cluster) {
        // Its own check says why; a second ssh would only wait again.
        Err(reason) => {
            checks.push(Check::new(host, "reachable", Level::Fail, reason));
            checks.push(Check::new(
                host,
                "environment_diff",
                Level::NotChecked,
                "the worker did not answer; its environment was not compared with the parent's",
            ));
        }
        Ok(answer) => {
            let (bridge, compared) = workers::both(
                remote,
                |handle| probe_bridge(handle, worker, cluster),
                |handle| diff.map(|source| source(handle, host)),
            );
            checks.push(Check::new(
                host,
                "reachable",
                Level::Ok,
                "answered over ssh",
            ));
            checks.extend(service_checks(host, &answer.status, parent_version));
            checks.push(match bridge {
                Ok(()) => Check::new(
                    host,
                    "bridge",
                    Level::Ok,
                    "hello completed with every required feature",
                ),
                Err(reason) => Check::new(host, "bridge", Level::Fail, reason),
            });
            checks.extend(evaluate(host, &answer.facts));
            engine = Some((host.to_string(), answer.facts.docker.engine_id.clone()));
            checks.extend(compared.unwrap_or_else(|| {
                vec![Check::new(
                    host,
                    "environment_diff",
                    Level::NotChecked,
                    "the comparison of workers diff was left out; toolchain and tool differences were not compared",
                )]
            }));
        }
    }
    Looked { checks, engine }
}

/// The worker's own doctor helper, over ssh: its facts and status, for the
/// worker the registration names.
fn ask_worker(
    remote: &mut dyn Remote,
    worker: &workers::Registered,
    cluster: &ClusterId,
) -> Result<HostAnswer, String> {
    let target = SshTarget::parse(&worker.ssh).map_err(|e| e.to_string())?;
    let binary = worker
        .binary
        .as_deref()
        .ok_or("the registration has no binary path; run workers add --install for this worker")?;
    let shell = format!(
        "exec {} {} {}",
        quote(binary),
        quote("worker-service"),
        quote("doctor")
    );
    let output = remote.run(&target, &shell, &[], Duration::from_secs(120))?;
    let answer = parse_answer(&output)?;
    if answer.status.worker_id.as_str() != worker.id || &answer.status.cluster != cluster {
        return Err("the host answered for a different worker identity or cluster".into());
    }
    if worker.system_user != answer.status.system_user {
        return Err("the installed service mode differs from the worker registration".into());
    }
    Ok(answer)
}

/// Whether the worker's bridge completes `hello`, as the resident runs it.
fn probe_bridge(
    remote: &mut dyn Remote,
    worker: &workers::Registered,
    cluster: &ClusterId,
) -> Result<(), String> {
    let target = SshTarget::parse(&worker.ssh).map_err(|e| e.to_string())?;
    let binary = worker
        .binary
        .as_deref()
        .ok_or("the registration has no binary path")?;
    remote
        .bridge_hello(&target, binary, cluster)
        .map_err(|m| format!("the worker bridge did not complete hello: {m}"))
        .and_then(
            |(answered_cluster, answered_worker): (ClusterId, WorkerId)| {
                if answered_worker.as_str() == worker.id && &answered_cluster == cluster {
                    Ok(())
                } else {
                    Err("the worker bridge answered for a different worker or cluster".to_string())
                }
            },
        )
}

fn text(report: &Report) -> String {
    let mut out = String::new();
    for check in &report.checks {
        let mark = match check.level {
            Level::Ok => "ok",
            Level::Notice => "notice",
            Level::Fail => "FAIL",
            Level::NotChecked => "not checked",
        };
        out.push_str(&format!(
            "[{mark}] {} {}: {}\n",
            check.host, check.id, check.detail
        ));
    }
    out.push_str(&format!(
        "{} failed, {} not checked, {} checks",
        report.failed,
        report.not_checked,
        report.checks.len()
    ));
    out
}

/// The `doctor` command. Exit 0 only when no check failed and none was left
/// out; a failed check and a check that was not made exit differently.
pub fn command(json: bool) -> i32 {
    let env: BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let result = std::env::current_exe()
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
        .and_then(|exe| {
            let version = super::worker_service::sha256_file(&exe)
                .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
            let spec = super::group::load_spec(&env)?;
            let mut group_checks = Vec::new();
            for member in workers::registrations(&env)? {
                let name = WorkerId::parse(&member.id)
                    .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
                match super::group::observed_refusal(&spec.state_dir, &name) {
                    Ok(Some(reason)) => {
                        group_checks.push(Check::new(&member.id, "group", Level::Fail, reason))
                    }
                    Ok(None) => {}
                    Err(_) => group_checks.push(Check::new(
                        &member.id,
                        "group",
                        Level::NotChecked,
                        "group connection diagnostic unreadable",
                    )),
                }
            }
            // Measured as `workers diff` measures it, once, and only when a
            // worker answers.
            let parent: std::sync::OnceLock<Result<EnvReport, String>> = std::sync::OnceLock::new();
            let source = |remote: &mut dyn Remote, host: &str| {
                let parent = parent
                    .get_or_init(|| measure_parent(&spec, crate::toolchain::envreport::collect));
                worker_diff(
                    remote,
                    &env,
                    &exe,
                    parent.as_ref().map_err(String::as_str),
                    host,
                )
            };
            let report = run(
                &mut workers::SshRemote::new(&spec.cluster_key_path),
                &mut SystemRunner::default(),
                &env,
                &spec,
                &version,
                super::macos::Launcher::backend(&super::worker_service::HostLauncher::native()),
                Some(&source),
            )?;
            let project = std::env::current_dir()
                .map_err(|e| e.to_string())
                .and_then(|cwd| {
                    super::context::Context::load(env.clone(), cwd, spec).map_err(|e| e.message)
                });
            let mut checks = report.checks;
            checks.extend(group_checks);
            checks.extend(project_checks(
                project
                    .as_ref()
                    .map(|context| context.config.notices.as_slice())
                    .map_err(Clone::clone),
            ));
            Ok(Report::of(checks))
        });
    match result.map(|report| (report.problem(), report)) {
        Ok((None, report)) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":true,"result":report})
                );
            } else {
                println!("{}", text(&report));
            }
            crate::cli::exit::SUCCESS
        }
        Ok((Some((code, message)), report)) => {
            if json {
                let mut error = serde_json::json!({"code": code, "message": message});
                if code == ErrorCode::Blocked {
                    error["blocked_reason"] =
                        serde_json::json!(crate::error::BlockedReason::EnvironmentMismatch);
                }
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":error,"result":report})
                );
            } else {
                println!("{}", text(&report));
                eprintln!("error: {message}");
            }
            code.exit_code()
        }
        Err(error) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":{"code":error.code,"message":error.message}})
                );
            } else {
                eprintln!("error: {}", error.message);
            }
            error.code.exit_code()
        }
    }
}

pub mod linger;
pub use linger::Linger;

#[cfg(test)]
mod tests;
