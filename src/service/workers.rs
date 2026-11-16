//! Parent-side `workers add|start|stop|drain|resume`. Every remote step is one
//! fixed `ssh` command built from constants and validated, quoted values; the
//! worker name, paths and the cluster key never enter a shell string (the key
//! travels on ssh stdin). An unreachable or silent worker is an error, never
//! "stopped". No ssh agent forwarding, no copy of any ssh private key.
use super::bootstrap::Spec;
use super::worker_service::{self, InstallRequest, Status};
use crate::{
    cli::WorkersSub,
    config::paths::user_config_path,
    error::{ApiError, ErrorCode},
    ids::WorkerId,
    protocol::ssh::{quote, ssh_exec_argv, SshTarget},
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

const OUTPUT_LIMIT: usize = 1 << 20;
/// How long `stop` waits for accepted work before giving up (the worker stays drained).
pub const STOP_WAIT_SECS: u64 = 600;
const STAGING: &str = "$HOME/.remote-task-runner-bootstrap";

#[derive(Debug)]
pub struct RemoteOutput {
    pub code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// One remote shell command with stdin. A test double stands in for `ssh`.
pub trait Remote {
    fn run(
        &mut self,
        target: &SshTarget,
        shell: &str,
        stdin: &[u8],
        timeout: Duration,
    ) -> Result<RemoteOutput, String>;
}

pub struct SshRemote;
impl Remote for SshRemote {
    fn run(
        &mut self,
        target: &SshTarget,
        shell: &str,
        stdin: &[u8],
        timeout: Duration,
    ) -> Result<RemoteOutput, String> {
        let argv = ssh_exec_argv(target, shell);
        let mut child = Command::new(&argv[0])
            .args(&argv[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| "ssh could not be started".to_string())?;
        let mut input = child.stdin.take().ok_or("ssh stdin unavailable")?;
        let payload = stdin.to_vec();
        let writer = std::thread::spawn(move || {
            let _ = input.write_all(&payload);
        });
        let read = |mut pipe: Box<dyn Read + Send>| {
            std::thread::spawn(move || {
                let mut out = Vec::new();
                let mut buffer = [0u8; 8192];
                while let Ok(n) = pipe.read(&mut buffer) {
                    if n == 0 {
                        break;
                    }
                    if out.len() + n <= OUTPUT_LIMIT {
                        out.extend_from_slice(&buffer[..n]);
                    }
                }
                out
            })
        };
        let stdout = read(Box::new(
            child.stdout.take().ok_or("ssh stdout unavailable")?,
        ));
        let stderr = read(Box::new(
            child.stderr.take().ok_or("ssh stderr unavailable")?,
        ));
        let deadline = Instant::now() + timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Ok(None) => {
                    // This Child is the process we spawned, never an inferred pid.
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("ssh timed out; the remote outcome is unknown".into());
                }
                Err(_) => return Err("ssh wait failed".into()),
            }
        };
        let _ = writer.join();
        Ok(RemoteOutput {
            code: status.code(),
            stdout: stdout.join().unwrap_or_default(),
            stderr: stderr.join().unwrap_or_default(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    Start,
    Stop { cancel_running: bool },
    Drain,
    Resume,
}
impl Lifecycle {
    fn action(self) -> &'static str {
        match self {
            Lifecycle::Start => "start",
            Lifecycle::Stop { .. } => "stop",
            Lifecycle::Drain => "drain",
            Lifecycle::Resume => "resume",
        }
    }
}

fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, message)
}

/// The shell string for a helper action. The staged binary of this exact
/// build is preferred; otherwise whatever `remote-task-runner` the login PATH
/// provides. Only constants, a hex digest and quoted words appear.
pub fn helper_command(digest: &str, lifecycle: Lifecycle, wait_secs: u64) -> String {
    let mut words = vec![
        quote("worker-service"),
        quote(lifecycle.action()),
        quote("--wait-secs"),
        quote(&wait_secs.to_string()),
    ];
    if matches!(
        lifecycle,
        Lifecycle::Stop {
            cancel_running: true
        }
    ) {
        words.push(quote("--cancel-running"));
    }
    let words = words.join(" ");
    format!(
        "b=\"{STAGING}/rtr-{digest}\"; if [ -x \"$b\" ]; then exec \"$b\" {words}; else exec {} {words}; fi",
        quote("remote-task-runner")
    )
}

fn stage_command(digest: &str) -> String {
    format!(
        "set -eu; umask 077; d=\"{STAGING}\"; mkdir -p \"$d\"; cat > \"$d/rtr-{digest}.part\"; chmod 700 \"$d/rtr-{digest}.part\"; mv \"$d/rtr-{digest}.part\" \"$d/rtr-{digest}\""
    )
}

fn probe_command(digest: &str) -> String {
    format!(
        "uname -s; uname -m; if [ -x \"{STAGING}/rtr-{digest}\" ]; then echo staged; else echo absent; fi"
    )
}

fn install_command(digest: &str) -> String {
    format!("exec \"{STAGING}/rtr-{digest}\" worker-service install")
}

fn local_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        other => other,
    }
}

/// Parse the helper's single JSON line. A remote that printed nothing usable
/// is an error even when ssh reported success.
fn parse_reply(output: &RemoteOutput) -> Result<Status, ApiError> {
    if output.code == Some(255) {
        return Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the worker could not be reached over ssh; nothing was confirmed",
        ));
    }
    let line = output
        .stdout
        .split(|b| *b == b'\n')
        .rfind(|l| !l.is_empty())
        .ok_or_else(|| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!(
                    "the worker helper gave no answer (exit {:?}); the outcome is unknown",
                    output.code
                ),
            )
        })?;
    let value: serde_json::Value = serde_json::from_slice(line).map_err(|_| {
        ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the worker helper answer was unreadable; the outcome is unknown",
        )
    })?;
    if value["ok"] == true && output.code == Some(0) {
        return serde_json::from_value(value["status"].clone()).map_err(|_| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                "the worker status was unreadable; the outcome is unknown",
            )
        });
    }
    Err(ApiError::new(
        ErrorCode::Conflict,
        format!(
            "worker helper refused: {}",
            value["error"].as_str().unwrap_or("unspecified")
        ),
    ))
}

struct Registration {
    ssh: String,
}

fn read_user_config(
    env: &BTreeMap<String, String>,
) -> Result<(std::path::PathBuf, String), ApiError> {
    let path = user_config_path(env).map_err(|e| invalid(e.to_string()))?;
    match std::fs::read_to_string(&path) {
        Ok(text) => Ok((path, text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok((path, "schema_version = 1\n".to_string()))
        }
        Err(e) => Err(invalid(format!("user configuration unreadable: {e}"))),
    }
}

fn registered(
    env: &BTreeMap<String, String>,
    name: &str,
) -> Result<Option<Registration>, ApiError> {
    let (path, text) = read_user_config(env)?;
    let user = crate::config::parse::parse_user(&path.to_string_lossy(), &text)
        .map_err(|e| invalid(e.to_string()))?;
    Ok(user
        .workers
        .into_iter()
        .find(|w| w.id == name)
        .map(|w| Registration { ssh: w.ssh }))
}

/// Append one `[[workers]]` table, atomically, keeping every other byte.
fn register(env: &BTreeMap<String, String>, name: &str, ssh: &str) -> Result<(), ApiError> {
    let (path, mut text) = read_user_config(env)?;
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&format!(
        "\n[[workers]]\nid = {}\nssh = {}\n",
        toml::Value::String(name.into()),
        toml::Value::String(ssh.into())
    ));
    // The result must still be a valid configuration with exactly this worker once.
    let user = crate::config::parse::parse_user(&path.to_string_lossy(), &text)
        .map_err(|e| invalid(e.to_string()))?;
    let snapshot = crate::config::resolve::Loaded {
        path: path.to_string_lossy().into_owned(),
        value: user,
    };
    crate::config::resolve::resolve(&crate::config::resolve::Inputs {
        overlay: &crate::config::Overlay::default(),
        project: None,
        legacy: None,
        user: Some(&snapshot),
    })
    .map_err(|e| invalid(e.to_string()))?;
    let io = |e: std::io::Error| ApiError::new(ErrorCode::Internal, e.to_string());
    let parent = path
        .parent()
        .ok_or_else(|| invalid("configuration parent"))?;
    std::fs::create_dir_all(parent).map_err(io)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
    temporary.write_all(text.as_bytes()).map_err(io)?;
    temporary.as_file().sync_all().map_err(io)?;
    temporary.persist(&path).map_err(|e| io(e.error))?;
    std::fs::File::open(parent)
        .and_then(|d| d.sync_all())
        .map_err(io)?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct AddResult {
    pub id: String,
    pub ssh: String,
    /// The user configuration gained this entry now (false: it was already there).
    pub registered: bool,
    pub installed: bool,
    pub status: Option<Status>,
}

fn target_of(ssh: &str) -> Result<SshTarget, ApiError> {
    SshTarget::parse(ssh).map_err(|e| invalid(e.to_string()))
}

fn check_name(name: &str) -> Result<(), ApiError> {
    WorkerId::parse(name).map_err(|e| invalid(format!("worker name: {e}")))?;
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(invalid(
            "worker name: use letters, digits, '-', '_' and '.'",
        ));
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// `workers add`. Without `--install` only the registration is written.
/// With it, the parent stages its own binary, runs the helper's install on
/// the worker host (resumable), and registers the worker last, so a failed
/// install leaves nothing registered. Re-running continues where it stopped.
pub fn add(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    exe: &Path,
    name: &str,
    ssh: Option<&str>,
    install: bool,
) -> Result<AddResult, ApiError> {
    check_name(name)?;
    let ssh = ssh.ok_or_else(|| invalid("--ssh is required"))?;
    let target = target_of(ssh)?;
    let existing = registered(env, name)?;
    if let Some(existing) = &existing {
        if existing.ssh != ssh {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "the worker is already registered with a different ssh destination",
            ));
        }
    }
    let status = if install {
        Some(install_remote(remote, spec, exe, name, &target)?)
    } else {
        None
    };
    let newly = existing.is_none();
    if newly {
        if spec.worker_id.as_str() == name {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "the name belongs to the local worker",
            ));
        }
        register(env, name, ssh)?;
    }
    Ok(AddResult {
        id: name.into(),
        ssh: ssh.into(),
        registered: newly,
        installed: install,
        status,
    })
}

fn install_remote(
    remote: &mut impl Remote,
    spec: &Spec,
    exe: &Path,
    name: &str,
    target: &SshTarget,
) -> Result<Status, ApiError> {
    let internal = |e: &dyn std::fmt::Display| ApiError::new(ErrorCode::Internal, e.to_string());
    if !cfg!(target_os = "macos") {
        return Err(invalid(
            "installing a worker is implemented for macOS parents and workers only",
        ));
    }
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or_else(|| invalid("the coordinator has no cluster identity; run init"))?;
    let key = crate::secrets::key::load(&spec.cluster_key_path).map_err(|e| internal(&e))?;
    let digest = worker_service::sha256_file(exe).map_err(|e| internal(&e))?;
    let probe = remote
        .run(
            target,
            &probe_command(&digest),
            &[],
            Duration::from_secs(60),
        )
        .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
    if probe.code != Some(0) {
        return Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the worker could not be probed over ssh",
        ));
    }
    let probe_text = String::from_utf8_lossy(&probe.stdout).into_owned();
    let mut lines = probe_text.lines();
    let (os, arch, staged) = (
        lines.next().unwrap_or(""),
        lines.next().unwrap_or(""),
        lines.next().unwrap_or(""),
    );
    if os != "Darwin" || arch != local_arch() {
        return Err(invalid(format!(
            "the worker is {os}/{arch}; this parent installs only a matching macOS build ({}/{})",
            "Darwin",
            local_arch()
        )));
    }
    if staged != "staged" {
        let binary = std::fs::read(exe).map_err(|e| internal(&e))?;
        let out = remote
            .run(
                target,
                &stage_command(&digest),
                &binary,
                Duration::from_secs(600),
            )
            .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
        if out.code != Some(0) {
            return Err(ApiError::new(
                ErrorCode::CommunicationUnknown,
                "the binary could not be staged on the worker",
            ));
        }
    }
    let request = InstallRequest {
        schema: 1,
        cluster,
        worker_id: WorkerId::parse(name).map_err(|e| invalid(e.to_string()))?,
        key_file_text: format!("{}\n", hex(key.as_bytes())),
        binary_sha256: digest.clone(),
    };
    let body = serde_json::to_vec(&request).map_err(|e| internal(&e))?;
    let out = remote
        .run(
            target,
            &install_command(&digest),
            &body,
            Duration::from_secs(300),
        )
        .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
    let status = parse_reply(&out)?;
    if !status.ready || status.loaded != Some(true) {
        return Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the service was installed but the worker agent did not become ready; run workers start",
        ));
    }
    Ok(status)
}

/// `workers start|stop|drain|resume` for a registered worker.
pub fn lifecycle(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    exe: &Path,
    name: &str,
    action: Lifecycle,
) -> Result<Status, ApiError> {
    check_name(name)?;
    let registration = registered(env, name)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no such worker is registered"))?;
    let target = target_of(&registration.ssh)?;
    let digest = worker_service::sha256_file(exe)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let wait = STOP_WAIT_SECS;
    let timeout = match action {
        Lifecycle::Stop { .. } => Duration::from_secs(wait + 120),
        _ => Duration::from_secs(120),
    };
    let out = remote
        .run(
            &target,
            &helper_command(&digest, action, wait),
            &[],
            timeout,
        )
        .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
    let status = parse_reply(&out)?;
    // Do not accept an answer about some other worker.
    if status.worker_id.as_str() != name {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            "the host answered for a different worker identity",
        ));
    }
    Ok(status)
}

fn emit(
    json: bool,
    result: Result<serde_json::Value, ApiError>,
    text: impl FnOnce() -> String,
) -> i32 {
    match result {
        Ok(value) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":true,"result":value})
                );
            } else {
                println!("{}", text());
            }
            crate::cli::exit::SUCCESS
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

/// Entry from the CLI for the wired subcommands. `None`: not wired here.
pub fn command(sub: &WorkersSub, json: bool) -> Option<i32> {
    let env: BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let exe = std::env::current_exe();
    let lifecycle_of = |name: &str, action: Lifecycle| {
        let exe = exe
            .as_ref()
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        lifecycle(&mut SshRemote, &env, exe, name, action)
    };
    let (name, result): (&str, Result<Status, ApiError>) = match sub {
        WorkersSub::Add { name, ssh, install } => {
            let result = exe
                .as_ref()
                .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
                .and_then(|exe| {
                    let spec = super::context::runtime_spec(&env)?;
                    add(
                        &mut SshRemote,
                        &env,
                        &spec,
                        exe,
                        name,
                        ssh.as_deref(),
                        *install,
                    )
                })
                .map(|r| serde_json::to_value(r).expect("serializes"));
            return Some(emit(json, result, || format!("added worker {name}")));
        }
        WorkersSub::Start { name } => (name, lifecycle_of(name, Lifecycle::Start)),
        WorkersSub::Drain { name } => (name, lifecycle_of(name, Lifecycle::Drain)),
        WorkersSub::Resume { name } => (name, lifecycle_of(name, Lifecycle::Resume)),
        WorkersSub::Stop {
            name,
            cancel_running,
        } => (
            name,
            lifecycle_of(
                name,
                Lifecycle::Stop {
                    cancel_running: *cancel_running,
                },
            ),
        ),
        _ => return None,
    };
    let verb = match sub {
        WorkersSub::Start { .. } => "started",
        WorkersSub::Drain { .. } => "draining",
        WorkersSub::Resume { .. } => "resumed",
        _ => "stopped",
    };
    Some(emit(
        json,
        result.map(|s| serde_json::to_value(s).expect("serializes")),
        || format!("worker {name} {verb}"),
    ))
}

#[cfg(test)]
mod tests;
