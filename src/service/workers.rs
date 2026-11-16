//! Parent-side `workers add|start|stop|drain|resume`. Every remote step is one
//! fixed `ssh` command built from constants and validated, quoted values; the
//! worker name, paths and the cluster key never enter a shell string (the key
//! travels on ssh stdin). An unreachable or silent worker is an error, never
//! "stopped". No ssh agent forwarding, no copy of any ssh private key.
use super::bootstrap::Spec;
use super::worker_service::{self, InstallRequest, Status};
use crate::toolchain::envdiff::{self, EnvReport, Item};
use crate::{
    cli::WorkersSub,
    config::paths::user_config_path,
    error::{ApiError, ErrorCode},
    ids::{ClusterId, CoordinatorEpoch, CoordinatorId, WorkerId},
    protocol::ssh::{quote, ssh_argv, ssh_exec_argv, RemoteCommand, SshTarget},
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

    /// The parent's operating system, as `std::env::consts::OS` names it. A
    /// test double can answer for another one so a test never depends on the
    /// machine it runs on.
    fn parent_os(&self) -> &str {
        std::env::consts::OS
    }

    /// Run the worker bridge exactly as the resident will (`binary worker
    /// --stdio` over ssh), complete one `hello`, and report who answered.
    /// A probe hello carries a throwaway coordinator identity and persists
    /// nothing on the worker.
    fn bridge_hello(
        &mut self,
        target: &SshTarget,
        binary: &str,
        cluster: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String>;
}

const HELLO_TIMEOUT: Duration = Duration::from_secs(30);

/// One `hello` over an already open transport.
async fn hello_on(
    io: crate::protocol::Io,
    cluster: &ClusterId,
) -> Result<(ClusterId, WorkerId), String> {
    let config = crate::protocol::ClientConfig::new(
        cluster.clone(),
        CoordinatorId::generate(),
        CoordinatorEpoch(1),
    );
    let connected = tokio::time::timeout(HELLO_TIMEOUT, crate::protocol::connect(io, config))
        .await
        .map_err(|_| "the worker bridge did not answer hello in time".to_string())?
        .map_err(|e| format!("the worker bridge refused hello: {e}"))?;
    Ok((connected.worker.cluster_id, connected.worker.worker_id))
}

/// `hello` through a child process. The child is this function's own `Child`:
/// it is killed and reaped by handle, never by an inferred pid.
async fn hello_through(
    argv: &[String],
    cluster: &ClusterId,
) -> Result<(ClusterId, WorkerId), String> {
    let transport = crate::protocol::transport::spawn_child_piped_stderr(argv)
        .map_err(|_| "ssh could not be started".to_string())?;
    let mut child = transport.child;
    drop(child.stderr.take());
    let answer = hello_on(transport.io, cluster).await;
    let _ = child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    answer
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

    fn bridge_hello(
        &mut self,
        target: &SshTarget,
        binary: &str,
        cluster: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        let command = RemoteCommand::worker_stdio(binary).map_err(|e| e.to_string())?;
        let argv = ssh_argv(target, &command);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(hello_through(&argv, cluster))
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

/// The shell string for a helper action. With the registered absolute binary
/// (the worker's own installed copy, independent of ssh's PATH and of this
/// parent's version) it is that binary. A registration without one keeps the
/// older lookup: the staged binary of this exact build, else whatever
/// `remote-task-runner` the login PATH provides. Only constants, a hex digest
/// and quoted words appear.
pub fn helper_command(
    binary: Option<&str>,
    digest: &str,
    lifecycle: Lifecycle,
    wait_secs: u64,
) -> String {
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
    launch(binary, digest, &words.join(" "))
}

/// `exec` of the worker's own binary (or the staged one) with fixed `words`.
fn launch(binary: Option<&str>, digest: &str, words: &str) -> String {
    if let Some(binary) = binary {
        return format!("exec {} {words}", quote(binary));
    }
    format!(
        "b=\"{STAGING}/rtr-{digest}\"; if [ -x \"$b\" ]; then exec \"$b\" {words}; else exec {} {words}; fi",
        quote("remote-task-runner")
    )
}

/// The shell string asking a worker's binary for its environment report.
pub fn env_command(binary: Option<&str>, digest: &str) -> String {
    launch(
        binary,
        digest,
        &format!("{} {}", quote("worker-service"), quote("env")),
    )
}

/// Receive the binary into a `.part` file and publish it only when its
/// SHA-256 is the expected digest; a short or damaged transfer is removed.
fn stage_command(digest: &str) -> String {
    format!(
        "set -eu; umask 077; d=\"{STAGING}\"; mkdir -p \"$d\"; p=\"$d/rtr-{digest}.part\"; cat > \"$p\"; chmod 700 \"$p\"; got=$(shasum -a 256 \"$p\" | cut -d' ' -f1); if [ \"$got\" != \"{digest}\" ]; then rm -f \"$p\"; echo 'staged binary digest mismatch' >&2; exit 3; fi; mv \"$p\" \"$d/rtr-{digest}\""
    )
}

/// `staged` only for an executable whose content is the expected digest.
fn probe_command(digest: &str) -> String {
    format!(
        "uname -s; uname -m; f=\"{STAGING}/rtr-{digest}\"; if [ -x \"$f\" ] && [ \"$(shasum -a 256 \"$f\" | cut -d' ' -f1)\" = \"{digest}\" ]; then echo staged; else echo absent; fi"
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
    binary: Option<String>,
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
        .map(|w| Registration {
            ssh: w.ssh,
            binary: w.binary,
        }))
}

/// Append one `[[workers]]` table, atomically, keeping every other byte.
fn register(
    env: &BTreeMap<String, String>,
    name: &str,
    ssh: &str,
    binary: Option<&str>,
) -> Result<(), ApiError> {
    let (path, mut text) = read_user_config(env)?;
    if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&format!(
        "\n[[workers]]\nid = {}\nssh = {}\n",
        toml::Value::String(name.into()),
        toml::Value::String(ssh.into())
    ));
    if let Some(binary) = binary {
        text.push_str(&format!(
            "binary = {}\n",
            toml::Value::String(binary.into())
        ));
    }
    write_config(&path, &text)
}

/// Add `binary` to the existing `[[workers]]` table of `name`, keeping every
/// other byte. An entry that was written by hand in a shape this cannot find
/// is reported, never guessed at.
fn set_binary(env: &BTreeMap<String, String>, name: &str, binary: &str) -> Result<(), ApiError> {
    let (path, text) = read_user_config(env)?;
    let mut out = String::new();
    let mut done = false;
    let mut block = String::new();
    let mut in_workers = false;
    let flush = |out: &mut String, block: &mut String, in_workers: bool, done: &mut bool| {
        if in_workers && !*done {
            if let Ok(table) = block.parse::<toml::Table>() {
                let is_ours = table
                    .get("workers")
                    .and_then(|w| w.as_array())
                    .and_then(|a| a.first())
                    .and_then(|t| t.get("id"))
                    .and_then(|id| id.as_str())
                    == Some(name);
                if is_ours {
                    if !block.ends_with('\n') {
                        block.push('\n');
                    }
                    block.push_str(&format!(
                        "binary = {}\n",
                        toml::Value::String(binary.into())
                    ));
                    *done = true;
                }
            }
        }
        out.push_str(block);
        block.clear();
    };
    for line in text.split_inclusive('\n') {
        if line.trim_start().starts_with('[') {
            flush(&mut out, &mut block, in_workers, &mut done);
            in_workers = line.trim() == "[[workers]]";
        }
        block.push_str(line);
    }
    flush(&mut out, &mut block, in_workers, &mut done);
    if !done {
        return Err(invalid(format!(
            "could not record the binary path for worker {name}; add `binary = \"{binary}\"` to its [[workers]] table"
        )));
    }
    write_config(&path, &out)
}

/// Validate the whole result as a configuration, then replace the file atomically.
fn write_config(path: &Path, text: &str) -> Result<(), ApiError> {
    let user = crate::config::parse::parse_user(&path.to_string_lossy(), text)
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
    temporary.persist(path).map_err(|e| io(e.error))?;
    std::fs::File::open(parent)
        .and_then(|d| d.sync_all())
        .map_err(io)?;
    Ok(())
}

/// What happened to the running resident when the registration changed.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ResidentReload {
    /// No resident is running; the next one reads the registration.
    NotRunning,
    /// A running resident now dials these workers; no restart is needed.
    Reloaded { connecting: Vec<String> },
    /// A resident is running but did not take the change.
    RestartRequired { reason: String },
}

#[derive(Debug, Serialize)]
pub struct AddResult {
    pub id: String,
    pub ssh: String,
    /// The user configuration gained this entry now (false: it was already there).
    pub registered: bool,
    pub installed: bool,
    /// The absolute binary path recorded for this worker.
    pub binary: Option<String>,
    pub status: Option<Status>,
    /// What the running resident did with the new registration.
    pub resident: ResidentReload,
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
/// the worker host (resumable), proves the exact bridge the resident will use
/// (absolute binary, `worker --stdio` over ssh, `hello` from the expected
/// worker in the expected cluster), and registers the worker last, so a
/// failed install leaves nothing registered. Re-running continues where it
/// stopped. Finally a running resident is asked to dial the worker now.
#[allow(clippy::too_many_arguments)]
pub fn add(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    exe: &Path,
    name: &str,
    ssh: Option<&str>,
    install: bool,
    reload: impl FnOnce(&Spec) -> ResidentReload,
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
    // Before any remote step: a name that cannot be registered must not cost an install.
    if existing.is_none() && spec.worker_id.as_str() == name {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            "the name belongs to the local worker",
        ));
    }
    let (status, binary) = if install {
        let status = install_remote(remote, spec, exe, name, &target)?;
        let binary = status.bridge_binary.clone();
        (Some(status), binary)
    } else {
        (None, existing.as_ref().and_then(|e| e.binary.clone()))
    };
    match &existing {
        None => register(env, name, ssh, binary.as_deref())?,
        Some(old) if old.binary != binary => {
            if let (Some(_), Some(new)) = (&old.binary, &binary) {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!("the worker is registered with another binary path; expected {new}"),
                ));
            }
            if let Some(new) = &binary {
                set_binary(env, name, new)?;
            }
        }
        Some(_) => {}
    }
    let newly = existing.is_none();
    let resident = reload(spec);
    Ok(AddResult {
        id: name.into(),
        ssh: ssh.into(),
        registered: newly,
        installed: install,
        binary,
        status,
        resident,
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
    if remote.parent_os() != "macos" {
        return Err(invalid(
            "installing a worker is implemented for macOS parents and workers only",
        ));
    }
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or_else(|| invalid("the coordinator has no cluster identity; run init"))?;
    let cluster_expected = cluster.clone();
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
            "the service was installed but the worker agent did not become ready; run workers add --install again to continue",
        ));
    }
    let binary = status.bridge_binary.clone().ok_or_else(|| {
        ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the worker did not report its installed binary path; nothing was registered",
        )
    })?;
    // The capability check: the path and the transport the resident will use.
    let (answered_cluster, answered_worker) = remote
        .bridge_hello(target, &binary, &status.cluster)
        .map_err(|m| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!(
                    "the worker bridge could not be started over ssh: {m}; nothing was registered"
                ),
            )
        })?;
    if answered_worker.as_str() != name
        || answered_cluster != status.cluster
        || status.cluster != cluster_expected
    {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            "the worker bridge answered for a different worker or cluster; nothing was registered",
        ));
    }
    Ok(status)
}

/// Whether `status` shows what `action` asked for. Anything else is an
/// error: a command never reports a state the host did not show.
fn check_outcome(action: Lifecycle, status: &Status) -> Result<(), ApiError> {
    let unmet = |what: &str| {
        Err(ApiError::new(
            ErrorCode::Conflict,
            format!("the worker did not reach the requested state: {what}"),
        ))
    };
    match action {
        Lifecycle::Start if status.loaded != Some(true) => unmet("the service is not loaded"),
        Lifecycle::Stop { .. } if status.loaded != Some(false) => {
            unmet("the service is not confirmed stopped")
        }
        Lifecycle::Drain if !status.draining => unmet("the worker is not draining"),
        Lifecycle::Resume if status.draining => unmet("the worker is still draining"),
        _ => Ok(()),
    }
}

/// The sentence for a successful `action`. A started service whose agent does
/// not accept connections yet is said to be exactly that.
pub fn describe(name: &str, action: Lifecycle, status: &Status) -> String {
    match action {
        Lifecycle::Start if !status.ready => {
            format!("worker {name} started; its agent is not accepting connections yet")
        }
        Lifecycle::Start => format!("worker {name} started"),
        Lifecycle::Stop { .. } => format!("worker {name} stopped"),
        Lifecycle::Drain => format!("worker {name} draining"),
        Lifecycle::Resume => format!("worker {name} resumed"),
    }
}

/// `workers start|stop|drain|resume` for a registered worker.
pub fn lifecycle(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    exe: &Path,
    name: &str,
    action: Lifecycle,
) -> Result<Status, ApiError> {
    check_name(name)?;
    let registration = registered(env, name)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no such worker is registered"))?;
    let cluster = spec
        .expected_cluster
        .as_ref()
        .ok_or_else(|| invalid("the coordinator has no cluster identity; run init"))?;
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
            &helper_command(registration.binary.as_deref(), &digest, action, wait),
            &[],
            timeout,
        )
        .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
    let status = parse_reply(&out)?;
    // Do not accept an answer about some other worker or another cluster.
    if status.worker_id.as_str() != name || &status.cluster != cluster {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            "the host answered for a different worker identity or cluster",
        ));
    }
    check_outcome(action, &status)?;
    Ok(status)
}

/// Ask a running resident to dial workers added since it started. No
/// resident is not an error; any other failure is said, not hidden.
pub fn reload_resident(spec: &Spec) -> ResidentReload {
    let Some(cluster) = spec.expected_cluster.clone() else {
        return ResidentReload::NotRunning;
    };
    let Ok(client) = super::resident::Client::load(&spec.state_dir, cluster) else {
        return ResidentReload::NotRunning;
    };
    let restart = |reason: String| ResidentReload::RestartRequired {
        reason: format!("{reason}; the worker stays unconnected until the resident restarts"),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => return restart(e.to_string()),
    };
    match runtime.block_on(client.request(super::resident::Operation::ReloadWorkers)) {
        Ok(super::resident::Reply::WorkersReloaded { connecting }) => {
            ResidentReload::Reloaded { connecting }
        }
        Ok(super::resident::Reply::ControlError { error }) => restart(error.message),
        Ok(_) => restart("the resident did not understand the request".into()),
        Err(e) => restart(format!("the resident did not answer: {e}")),
    }
}

/// What a worker answered to `worker-service env`. An older binary does not
/// know the action: that is said, never turned into an empty difference.
fn parse_env_reply(out: &RemoteOutput) -> Result<EnvReport, ApiError> {
    let unreadable = || {
        ApiError::new(
            ErrorCode::Conflict,
            "the worker's binary does not support environment reports; reinstall it with workers add --install",
        )
    };
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).map_err(|_| unreadable())?;
    if value["ok"] == true {
        return serde_json::from_value(value["env"].clone()).map_err(|_| unreadable());
    }
    Err(ApiError::new(
        ErrorCode::Conflict,
        format!(
            "worker environment report refused: {}",
            value["error"].as_str().unwrap_or("unspecified")
        ),
    ))
}

/// The environment of a registered worker, measured by its own binary.
pub fn fetch_env(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    exe: &Path,
    name: &str,
) -> Result<EnvReport, ApiError> {
    check_name(name)?;
    let registration = registered(env, name)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no such worker is registered"))?;
    let target = target_of(&registration.ssh)?;
    let digest = worker_service::sha256_file(exe)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    let out = remote
        .run(
            &target,
            &env_command(registration.binary.as_deref(), &digest),
            &[],
            Duration::from_secs(60),
        )
        .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
    parse_env_reply(&out)
}

#[derive(Debug, Serialize)]
pub struct WorkerDiff {
    pub worker: String,
    pub all_same: bool,
    pub items: Vec<Item>,
}

/// `workers diff [<worker>]`: the parent's environment against each named
/// (or every registered) worker. `local` measures the parent.
pub fn diff_workers(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    exe: &Path,
    name: Option<&str>,
    local: impl FnOnce() -> Result<EnvReport, String>,
) -> Result<Vec<WorkerDiff>, ApiError> {
    let names = match name {
        Some(name) => vec![name.to_string()],
        None => registered_names(env)?,
    };
    let parent = local().map_err(|e| ApiError::new(ErrorCode::Internal, e))?;
    names
        .into_iter()
        .map(|worker| {
            let report = fetch_env(remote, env, exe, &worker)?;
            let items = envdiff::diff(&parent, &report);
            Ok(WorkerDiff {
                all_same: envdiff::all_same(&items),
                worker,
                items,
            })
        })
        .collect()
}

fn registered_names(env: &BTreeMap<String, String>) -> Result<Vec<String>, ApiError> {
    let (path, text) = read_user_config(env)?;
    let user = crate::config::parse::parse_user(&path.to_string_lossy(), &text)
        .map_err(|e| invalid(e.to_string()))?;
    Ok(user.workers.into_iter().map(|w| w.id).collect())
}

/// Plain text for `workers diff`: one line per item.
pub fn render_diff(diffs: &[WorkerDiff]) -> String {
    let mut lines = Vec::new();
    for d in diffs {
        lines.push(format!("worker {}", d.worker));
        for i in &d.items {
            let class = serde_json::to_value(i.class)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default();
            let mut line = format!(
                "  {:<24} {:<16} parent={} worker={}",
                i.subject,
                class,
                i.parent.as_deref().unwrap_or("-"),
                i.worker.as_deref().unwrap_or("-")
            );
            if let Some(steps) = &i.steps {
                line.push_str(&format!("  -> {steps}"));
            }
            lines.push(line);
        }
    }
    lines.join("\n")
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
        let spec = super::context::runtime_spec(&env)?;
        lifecycle(&mut SshRemote, &env, &spec, exe, name, action)
    };
    let (name, action, result): (&str, Lifecycle, Result<Status, ApiError>) = match sub {
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
                        reload_resident,
                    )
                })
                .map(|r| {
                    let text = match &r.resident {
                        ResidentReload::RestartRequired { reason } => {
                            format!("added worker {name}; {reason}")
                        }
                        ResidentReload::Reloaded { .. } => {
                            format!("added worker {name}; the running resident is connecting")
                        }
                        ResidentReload::NotRunning => format!("added worker {name}"),
                    };
                    (serde_json::to_value(r).expect("serializes"), text)
                });
            let text = result.as_ref().map(|(_, t)| t.clone()).unwrap_or_default();
            return Some(emit(json, result.map(|(v, _)| v), || text));
        }
        WorkersSub::Start { name } => {
            (name, Lifecycle::Start, lifecycle_of(name, Lifecycle::Start))
        }
        WorkersSub::Drain { name } => {
            (name, Lifecycle::Drain, lifecycle_of(name, Lifecycle::Drain))
        }
        WorkersSub::Resume { name } => (
            name,
            Lifecycle::Resume,
            lifecycle_of(name, Lifecycle::Resume),
        ),
        WorkersSub::Stop {
            name,
            cancel_running,
        } => {
            let action = Lifecycle::Stop {
                cancel_running: *cancel_running,
            };
            (name, action, lifecycle_of(name, action))
        }
        WorkersSub::Diff { name } => {
            let result = (|| {
                let exe = exe
                    .as_ref()
                    .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
                let spec = super::context::runtime_spec(&env)?;
                diff_workers(&mut SshRemote, &env, exe, name.as_deref(), || {
                    crate::toolchain::envreport::collect(&spec.worker_state_dir, &spec.env)
                })
            })()
            .map(|diffs| {
                let text = render_diff(&diffs);
                (serde_json::json!({ "workers": diffs }), text)
            });
            let text = result.as_ref().map(|(_, t)| t.clone()).unwrap_or_default();
            return Some(emit(json, result.map(|(v, _)| v), || text));
        }
        _ => return None,
    };
    let text = result
        .as_ref()
        .map(|status| describe(name, action, status))
        .unwrap_or_default();
    Some(emit(
        json,
        result.map(|s| serde_json::to_value(s).expect("serializes")),
        || text,
    ))
}

#[cfg(test)]
mod tests;
