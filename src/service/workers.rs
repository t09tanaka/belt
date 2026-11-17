//! Parent-side `workers add|start|stop|drain|resume`. Every remote step is one
//! fixed `ssh` command built from constants and validated, quoted values; the
//! worker name, paths and the cluster key never enter a shell string (the key
//! travels on ssh stdin). An unreachable or silent worker is an error, never
//! "stopped". No ssh agent forwarding, no copy of any ssh private key.
use super::bootstrap::Spec;
use super::worker_service::{self, InstallRequest, Status};
use crate::toolchain::envdiff::{self, EnvReport, Item};
use crate::toolchain::envsync::{self, Blocked, InstallStep};
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

    /// The parent's architecture, as `std::env::consts::ARCH` names it.
    fn parent_arch(&self) -> &str {
        std::env::consts::ARCH
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

    /// The same probe, requiring exactly `required` instead of what this
    /// build requires, and reporting the features that were agreed. A double
    /// that does not model features answers as `bridge_hello` does.
    fn bridge_hello_with(
        &mut self,
        target: &SshTarget,
        binary: &str,
        cluster: &ClusterId,
        required: &[String],
    ) -> Result<BridgeHello, String> {
        let _ = required;
        let (cluster, worker) = self.bridge_hello(target, binary, cluster)?;
        Ok(BridgeHello {
            cluster,
            worker,
            features: Vec::new(),
        })
    }
}

/// Who answered a probe `hello`, and the features agreed with them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeHello {
    pub cluster: ClusterId,
    pub worker: WorkerId,
    pub features: Vec<String>,
}

/// What `workers upgrade` learns about the coordinator resident before it
/// changes anything.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResidentBuild {
    /// No resident runs; the next one starts from this binary.
    NotRunning,
    Running {
        binary_sha256: String,
        features: Vec<String>,
        required_features: Vec<String>,
    },
    /// A resident runs but did not say which build it is (one older than the
    /// question, or one that failed to answer).
    Unknown { reason: String },
}

const HELLO_TIMEOUT: Duration = Duration::from_secs(30);

/// One `hello` over an already open transport. `required`: the features the
/// worker must agree to; `None` requires what this build requires.
async fn hello_on(
    io: crate::protocol::Io,
    cluster: &ClusterId,
    required: Option<&[String]>,
) -> Result<BridgeHello, String> {
    let mut config = crate::protocol::ClientConfig::new(
        cluster.clone(),
        CoordinatorId::generate(),
        CoordinatorEpoch(1),
    );
    let connected = tokio::time::timeout(HELLO_TIMEOUT, async {
        match required {
            None => crate::protocol::connect(io, config).await,
            Some(required) => {
                config.required_features = required.to_vec();
                crate::protocol::connect_requiring(io, config).await
            }
        }
    })
    .await
    .map_err(|_| "the worker bridge did not answer hello in time".to_string())?
    .map_err(|e| format!("the worker bridge refused hello: {e}"))?;
    Ok(BridgeHello {
        cluster: connected.worker.cluster_id,
        worker: connected.worker.worker_id,
        features: connected.negotiated.features,
    })
}

/// `hello` through a child process. The child is this function's own `Child`:
/// it is killed and reaped by handle, never by an inferred pid.
async fn hello_through(
    argv: &[String],
    cluster: &ClusterId,
    required: Option<&[String]>,
) -> Result<BridgeHello, String> {
    let transport = crate::protocol::transport::spawn_child_piped_stderr(argv)
        .map_err(|_| "ssh could not be started".to_string())?;
    let mut child = transport.child;
    drop(child.stderr.take());
    let answer = hello_on(transport.io, cluster, required).await;
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
        ssh_hello(target, binary, cluster, None).map(|hello| (hello.cluster, hello.worker))
    }

    fn bridge_hello_with(
        &mut self,
        target: &SshTarget,
        binary: &str,
        cluster: &ClusterId,
        required: &[String],
    ) -> Result<BridgeHello, String> {
        ssh_hello(target, binary, cluster, Some(required))
    }
}

fn ssh_hello(
    target: &SshTarget,
    binary: &str,
    cluster: &ClusterId,
    required: Option<&[String]>,
) -> Result<BridgeHello, String> {
    let command = RemoteCommand::worker_stdio(binary).map_err(|e| e.to_string())?;
    let argv = ssh_argv(target, &command);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?
        .block_on(hello_through(&argv, cluster, required))
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
        "set -eu; umask 077; {SHA256}; d=\"{STAGING}\"; mkdir -p \"$d\"; p=\"$d/rtr-{digest}.part\"; cat > \"$p\"; chmod 700 \"$p\"; got=$(sha \"$p\"); if [ \"$got\" != \"{digest}\" ]; then rm -f \"$p\"; echo 'staged binary digest mismatch' >&2; exit 3; fi; mv \"$p\" \"$d/rtr-{digest}\""
    )
}

/// The SHA-256 of a file with whichever tool the host has: `sha256sum` on
/// Linux, `shasum` on macOS.
const SHA256: &str = "sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$1\"; else shasum -a 256 \"$1\"; fi | cut -d' ' -f1; }";

/// What the staged binary says its version is. Nothing on the host changes.
fn version_command(digest: &str) -> String {
    format!("exec \"{STAGING}/rtr-{digest}\" --version")
}

/// Remove a staged binary that must not be used.
fn unstage_command(digest: &str) -> String {
    format!("rm -f \"{STAGING}/rtr-{digest}\"")
}

/// What this parent prints for `--version`: what a binary sent to a worker
/// must print too.
fn own_version() -> String {
    <crate::cli::Cli as clap::CommandFactory>::command().render_version()
}

const EPHEMERAL_NO_SERVICE: &str = "an ephemeral worker has no service: it is started on its own host with `worker --ephemeral` and is never installed, upgraded, started, stopped, drained or resumed from the parent";

/// `staged` only for an executable whose content is the expected digest.
fn probe_command(digest: &str) -> String {
    format!(
        "uname -s; uname -m; {SHA256}; f=\"{STAGING}/rtr-{digest}\"; if [ -x \"$f\" ] && [ \"$(sha \"$f\")\" = \"{digest}\" ]; then echo staged; else echo absent; fi"
    )
}

fn install_command(digest: &str) -> String {
    format!("exec \"{STAGING}/rtr-{digest}\" worker-service install")
}

#[cfg(test)]
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
    ephemeral: bool,
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
            ephemeral: w.ephemeral.unwrap_or(false),
        }))
}

/// One `[[workers]]` entry of the user configuration.
pub struct Registered {
    pub id: String,
    pub ssh: String,
    pub binary: Option<String>,
    /// Started on its own host with `worker --ephemeral`; it has no service.
    pub ephemeral: bool,
}

/// Every registered worker, in configuration order.
pub fn registrations(env: &BTreeMap<String, String>) -> Result<Vec<Registered>, ApiError> {
    let (path, text) = read_user_config(env)?;
    let user = crate::config::parse::parse_user(&path.to_string_lossy(), &text)
        .map_err(|e| invalid(e.to_string()))?;
    Ok(user
        .workers
        .into_iter()
        .map(|w| Registered {
            id: w.id,
            ssh: w.ssh,
            binary: w.binary,
            ephemeral: w.ephemeral.unwrap_or(false),
        })
        .collect())
}

/// Append one `[[workers]]` table, atomically, keeping every other byte.
fn register(
    env: &BTreeMap<String, String>,
    name: &str,
    ssh: &str,
    binary: Option<&str>,
    ephemeral: bool,
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
    if ephemeral {
        text.push_str("ephemeral = true\n");
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
    /// Registered as an ephemeral worker: nothing is installed on it.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub ephemeral: bool,
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

/// What `workers add` was asked to do beyond registering the name.
#[derive(Clone, Copy, Debug, Default)]
pub struct AddOptions<'a> {
    /// Install the worker over SSH.
    pub install: bool,
    /// Register an ephemeral worker (`ephemeral = true`).
    pub ephemeral: bool,
    /// The build to send instead of the parent's own binary (`--binary`).
    pub binary: Option<&'a Path>,
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
    options: AddOptions<'_>,
    reload: impl FnOnce(&Spec) -> ResidentReload,
) -> Result<AddResult, ApiError> {
    check_name(name)?;
    let install = options.install;
    if options.ephemeral && (install || options.binary.is_some()) {
        return Err(invalid(EPHEMERAL_NO_SERVICE));
    }
    if options.binary.is_some() && !install {
        return Err(invalid(
            "--binary names the build that --install sends; use it together with --install",
        ));
    }
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
    let ephemeral = match &existing {
        Some(existing) if existing.ephemeral && install => {
            return Err(invalid(EPHEMERAL_NO_SERVICE))
        }
        Some(existing) if !existing.ephemeral && options.ephemeral => {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "the worker is already registered as a persistent worker",
            ))
        }
        Some(existing) => existing.ephemeral,
        None => options.ephemeral,
    };
    // Before any remote step: a name that cannot be registered must not cost an install.
    if existing.is_none() && spec.worker_id.as_str() == name {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            "the name belongs to the local worker",
        ));
    }
    let (status, binary) = if install {
        let status = install_remote(remote, spec, exe, options.binary, name, &target)?;
        let binary = status.bridge_binary.clone();
        (Some(status), binary)
    } else {
        (None, existing.as_ref().and_then(|e| e.binary.clone()))
    };
    match &existing {
        None => register(env, name, ssh, binary.as_deref(), ephemeral)?,
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
        ephemeral,
        binary,
        status,
        resident,
    })
}

/// Make sure the binary `ship` (whose SHA-256 is `digest`) is staged on the
/// worker host and reports this parent's version. `explicit`: it was named
/// with `--binary`; without that the parent's own binary goes only to a
/// host of the parent's own OS and architecture. An already staged copy is
/// not sent again. A copy that does not report the parent's version is
/// removed again; nothing else on the host was touched by then.
fn ensure_staged(
    remote: &mut impl Remote,
    target: &SshTarget,
    ship: &Path,
    digest: &str,
    explicit: bool,
) -> Result<(), ApiError> {
    let internal = |e: &dyn std::fmt::Display| ApiError::new(ErrorCode::Internal, e.to_string());
    let unknown = |m: String| ApiError::new(ErrorCode::CommunicationUnknown, m);
    let probe = remote
        .run(target, &probe_command(digest), &[], Duration::from_secs(60))
        .map_err(unknown)?;
    if probe.code != Some(0) {
        return Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the worker could not be probed over ssh",
        ));
    }
    let probe_text = String::from_utf8_lossy(&probe.stdout).into_owned();
    let probe = target::parse_probe(&probe_text);
    let parent = target::Platform::from_rust(remote.parent_os(), remote.parent_arch());
    target::decide(parent, probe.system, probe.machine, explicit).map_err(invalid)?;
    if !probe.staged {
        let binary = std::fs::read(ship).map_err(|e| internal(&e))?;
        let out = remote
            .run(
                target,
                &stage_command(digest),
                &binary,
                Duration::from_secs(600),
            )
            .map_err(unknown)?;
        if out.code != Some(0) {
            return Err(ApiError::new(
                ErrorCode::CommunicationUnknown,
                "the binary could not be staged on the worker",
            ));
        }
    }
    // Before anything is installed or switched: the copy runs there and is
    // this parent's version.
    let expected = own_version();
    let reported = remote
        .run(
            target,
            &version_command(digest),
            &[],
            Duration::from_secs(60),
        )
        .map_err(unknown)?;
    if reported.code == Some(255) {
        return Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the worker could not be asked for the version of the staged binary",
        ));
    }
    if reported.code == Some(0) && target::version_matches(&expected, &reported.stdout) {
        return Ok(());
    }
    let removed = remote
        .run(
            target,
            &unstage_command(digest),
            &[],
            Duration::from_secs(60),
        )
        .is_ok_and(|out| out.code == Some(0));
    Err(invalid(format!(
        "the binary sent to the worker does not run there as this parent's version: it reported {} (exit {}), this parent is {}; a worker must run the parent's version, so nothing was installed or switched{}",
        target::shown_version(&reported.stdout),
        reported.code.map_or("unknown".to_string(), |code| code.to_string()),
        expected.trim(),
        if removed {
            " and the staged copy was removed"
        } else {
            "; the staged copy could not be removed"
        }
    )))
}

fn install_remote(
    remote: &mut impl Remote,
    spec: &Spec,
    exe: &Path,
    binary: Option<&Path>,
    name: &str,
    target: &SshTarget,
) -> Result<Status, ApiError> {
    let internal = |e: &dyn std::fmt::Display| ApiError::new(ErrorCode::Internal, e.to_string());
    // What is sent: the build named with `--binary`, else this binary.
    let ship = binary.unwrap_or(exe);
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or_else(|| invalid("the coordinator has no cluster identity; run init"))?;
    let cluster_expected = cluster.clone();
    let key = crate::secrets::key::load(&spec.cluster_key_path).map_err(|e| internal(&e))?;
    let digest = worker_service::sha256_file(ship).map_err(|e| internal(&e))?;
    ensure_staged(remote, target, ship, &digest, binary.is_some())?;
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
    if registration.ephemeral {
        return Err(invalid(EPHEMERAL_NO_SERVICE));
    }
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

/// What `workers forget` did.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ForgetResult {
    pub id: String,
    /// An identity was recorded for the worker and is retired now: a worker
    /// that comes back with it is refused.
    pub identity_retired: bool,
    /// The attempts that were ended as `interrupted` (`cancelled` when
    /// their cancel had been asked for), oldest first.
    pub ended: Vec<crate::ids::AttemptId>,
}

/// `workers forget <name>`: declare an ephemeral worker gone. The running
/// resident decides and does it (`ask`); this side only reads its answer and
/// never reports a forget the resident did not confirm.
pub fn forget(
    name: &str,
    ask: impl FnOnce(WorkerId) -> Result<super::resident::Reply, String>,
) -> Result<ForgetResult, ApiError> {
    use super::resident::Reply;
    check_name(name)?;
    let worker = WorkerId::parse(name).map_err(|e| invalid(e.to_string()))?;
    match ask(worker) {
        Ok(Reply::WorkerForgotten {
            identity_retired,
            ended,
        }) => Ok(ForgetResult {
            id: name.into(),
            identity_retired,
            ended,
        }),
        Ok(Reply::ControlError { error }) => Err(error),
        Ok(Reply::Stopping) => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the coordinator resident is stopping; nothing was forgotten, run workers forget again when it runs",
        )),
        Ok(_) => Err(ApiError::new(
            ErrorCode::Conflict,
            "the coordinator resident did not understand the request; restart it so that it runs this binary, then run workers forget again (nothing was changed)",
        )),
        Err(why) => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            format!("the coordinator resident did not answer ({why}); the resident carries out a forget, so whether anything changed is not known: run workers forget again while it runs"),
        )),
    }
}

/// Ask the running resident. Nothing is started for it.
pub fn ask_resident_to_forget(
    spec: &Spec,
    worker: WorkerId,
) -> Result<super::resident::Reply, String> {
    resident_request(spec, super::resident::Operation::ForgetWorker { worker })
}

fn resident_request(
    spec: &Spec,
    operation: super::resident::Operation,
) -> Result<super::resident::Reply, String> {
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or("the coordinator has no cluster identity; run init")?;
    let client = super::resident::Client::load(&spec.state_dir, cluster)
        .map_err(|_| "it is not running".to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime
        .block_on(client.request(operation))
        .map_err(|e| e.to_string())
}

pub fn describe_forget(result: &ForgetResult) -> String {
    let identity = if result.identity_retired {
        "its identity is retired and is refused if it comes back"
    } else {
        "no identity was recorded for it"
    };
    match result.ended.len() {
        0 => format!(
            "forgot worker {}: {identity}; it held no attempt",
            result.id
        ),
        1 => format!("forgot worker {}: {identity}; 1 attempt ended", result.id),
        n => format!(
            "forgot worker {}: {identity}; {n} attempts ended",
            result.id
        ),
    }
}

/// A worker-supplied text without control characters (escape sequences,
/// newlines), so it cannot rewrite the parent's terminal.
pub(super) fn strip_control(text: &str) -> String {
    text.chars().filter(|c| !c.is_control()).collect()
}

/// [`strip_control`], cut to a length a terminal line can hold.
fn plain(text: &str) -> String {
    strip_control(text).chars().take(200).collect()
}

/// What a worker answered to `worker-service env`. A binary that does not
/// know the action, a newer or damaged report and a login banner each get
/// their own message: none is turned into an empty difference.
fn parse_env_reply(out: &RemoteOutput, name: &str) -> Result<EnvReport, ApiError> {
    let conflict = |message: String| ApiError::new(ErrorCode::Conflict, message);
    let upgrade = format!("run workers upgrade {name}");
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&out.stdout) else {
        let text = String::from_utf8_lossy(&out.stdout);
        let banner = text
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .any(|v| v.get("ok").is_some());
        return Err(conflict(if banner {
            "the worker printed other output (a login banner?) before its environment report; \
             silence it for non-interactive ssh sessions"
                .to_string()
        } else if text.trim().is_empty() {
            format!("the worker's binary gave no environment report; it may be too old: {upgrade}")
        } else {
            format!(
                "the worker's answer is not an environment report; its binary may be too old: {upgrade}"
            )
        }));
    };
    if value["ok"] == true {
        let version = value["env"]["schema_version"].as_u64();
        return match version {
            Some(v) if v > u64::from(envdiff::SCHEMA) => Err(conflict(format!(
                "the worker reports environment schema {v}, newer than this machine understands ({}); upgrade remote-task-runner here",
                envdiff::SCHEMA
            ))),
            Some(v) if v < u64::from(envdiff::SCHEMA) => Err(conflict(format!(
                "the worker reports environment schema {v}, older than this machine's {}: {upgrade}",
                envdiff::SCHEMA
            ))),
            _ => serde_json::from_value(value["env"].clone()).map_err(|_| {
                conflict(format!(
                    "the worker's environment report is damaged; if it persists: {upgrade}"
                ))
            }),
        };
    }
    Err(conflict(format!(
        "worker environment report refused: {}",
        value["error"]
            .as_str()
            .map_or_else(|| "unspecified".to_string(), plain)
    )))
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
    if registration.ephemeral {
        let spec = super::context::runtime_spec(env)?;
        let worker = WorkerId::parse(name)
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        return match resident_request(&spec, super::resident::Operation::EnvReport { worker }) {
            Ok(super::resident::Reply::EnvReport { environment }) => Ok(environment),
            Ok(super::resident::Reply::ControlError { error }) => Err(error),
            Ok(_) => Err(ApiError::new(
                ErrorCode::ProtocolIncompatible,
                "resident did not return the worker environment",
            )),
            Err(message) => Err(ApiError::new(ErrorCode::CommunicationUnknown, message)),
        };
    }
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
    parse_env_reply(&out, name)
}

#[derive(Debug, Serialize)]
pub struct WorkerDiff {
    pub worker: String,
    pub all_same: bool,
    pub items: Vec<Item>,
    /// Why this worker could not be measured; `items` is empty then.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `workers diff [<worker>]`: the parent's environment against each named
/// (or every registered) worker. `local` measures the parent. Without a name
/// one worker that cannot be measured does not stop the others: it gets its
/// own entry with the reason. A named worker that fails is an error.
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
    if names.is_empty() {
        return Err(ApiError::new(
            ErrorCode::NotFound,
            "no workers are registered; add one with workers add",
        ));
    }
    let parent = local().map_err(|e| ApiError::new(ErrorCode::Internal, e))?;
    let mut out = Vec::new();
    for worker in names {
        match fetch_env(remote, env, exe, &worker) {
            Ok(report) => {
                let items = envdiff::diff(&parent, &report);
                out.push(WorkerDiff {
                    all_same: envdiff::all_same(&items),
                    worker,
                    items,
                    error: None,
                });
            }
            Err(e) if name.is_some() => return Err(e),
            Err(e) => out.push(WorkerDiff {
                worker,
                all_same: false,
                items: Vec::new(),
                error: Some(plain(&e.message)),
            }),
        }
    }
    Ok(out)
}

/// Why a version was not installed.
#[derive(Debug)]
pub enum InstallError {
    /// This version could not be installed; the other steps go on, and the
    /// version is reported as blocked.
    Failed(String),
    /// Nothing can be installed on this worker now (no resident, a worker
    /// that must be upgraded first, no answer): the sync ends here.
    Stop(ApiError),
}

/// Installs one version on a worker through its version manager, by the same
/// admitted setup path a placed attempt uses. The production one
/// ([`warmup::ResidentWarmup`]) asks the running resident, which asks the
/// worker over the link it already holds.
pub trait Installer {
    fn install(&mut self, worker: &str, step: &InstallStep) -> Result<Installed, InstallError>;
}

/// What an install that did not fail did on the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Installed {
    /// It installed the version.
    Now,
    /// The worker had the version already and did nothing.
    Already,
}

#[derive(Debug, Serialize)]
pub struct SyncResult {
    pub worker: String,
    pub installed: Vec<InstallStep>,
    /// The worker now matches the parent in everything `workers diff` shows.
    /// False only with `--project`: the project's requirements are met while
    /// a tool the project declares still differs from the parent.
    pub in_sync: bool,
}

/// The project's version requirements per tool, read from the version files
/// directly under `dir` (never followed through a link, bounded in size).
pub fn read_project_requirements(
    dir: &Path,
) -> Result<Vec<crate::toolchain::versions::EffectiveRequirement>, ApiError> {
    use crate::toolchain::{declarations, versions};
    let bad = |what: &str| invalid(format!("project version files: {what}"));
    // Resolved once, so that nothing below follows a link the project holds.
    let root = std::fs::canonicalize(dir).map_err(|_| bad("the project is not a directory"))?;
    if !root.is_dir() {
        return Err(bad("the project is not a directory"));
    }
    let mut documents = BTreeMap::new();
    for name in declarations::VERSION_FILES {
        // Every directory of the name (`.fvm`) and the file are opened one by
        // one without following a link.
        let file = match crate::platform::open_beneath(&root, Path::new(name)) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(bad(&format!("{name} is not a small regular file"))),
        };
        let meta = file
            .metadata()
            .map_err(|_| bad(&format!("{name} is unreadable")))?;
        if !meta.is_file() || meta.len() > declarations::MAX_DOCUMENT_BYTES as u64 {
            return Err(bad(&format!("{name} is not a small regular file")));
        }
        let mut bytes = Vec::new();
        file.take(declarations::MAX_DOCUMENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| bad(&format!("{name} is unreadable")))?;
        let doc = declarations::Document::new(name, &bytes)
            .map_err(|d| bad(&format!("{name}: {}", d.code)))?;
        documents.insert(name.to_string(), doc);
    }
    if documents.is_empty() {
        return Err(bad(&format!("no version files found in {}", dir.display())));
    }
    let found = declarations::read_project("", &documents, &[]).map_err(|d| bad(&d.code))?;
    versions::effective(&found, &versions::Scope::project("")).map_err(|d| bad(&d.code))
}

fn blocked_error(name: &str, installed: &[InstallStep], blocked: &[Blocked]) -> ApiError {
    ApiError::blocked(
        crate::error::BlockedReason::EnvironmentMismatch,
        envsync::render_blocked(name, installed, blocked),
    )
}

/// `workers sync <worker> [--project <dir>]`. Installs what the worker's
/// version manager may, then reports every difference that still needs a
/// person. A failed install is never success, and no other version stands in.
pub fn sync_worker(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    exe: &Path,
    name: &str,
    local: impl FnOnce() -> Result<EnvReport, String>,
    project: Option<&[crate::toolchain::versions::EffectiveRequirement]>,
    installer: &mut dyn Installer,
) -> Result<SyncResult, ApiError> {
    let parent = local().map_err(|e| ApiError::new(ErrorCode::Internal, e))?;
    sync_measured(
        name,
        &parent,
        || fetch_env(remote, env, exe, name),
        project,
        installer,
    )
}

/// `error`, with what was installed before it said first.
fn after_installing(installed: &[InstallStep], what: &str, error: ApiError) -> ApiError {
    if installed.is_empty() {
        return error;
    }
    let list: Vec<String> = installed
        .iter()
        .map(|s| format!("{} {}", s.tool, s.version))
        .collect();
    ApiError {
        message: format!(
            "installed {}, but {what}: {}",
            list.join(", "),
            error.message
        ),
        ..error
    }
}

/// [`sync_worker`] with the worker's measurement given: `measure` is asked
/// once before the installs and once after them.
pub fn sync_measured(
    name: &str,
    parent: &EnvReport,
    mut measure: impl FnMut() -> Result<EnvReport, ApiError>,
    project: Option<&[crate::toolchain::versions::EffectiveRequirement]>,
    installer: &mut dyn Installer,
) -> Result<SyncResult, ApiError> {
    let worker = measure()?;
    let first = envsync::plan(parent, &worker, project);
    // What this sync installed, and every step the worker now says it has
    // (a version it had already is there, but was not installed here).
    let mut installed = Vec::new();
    let mut settled = Vec::new();
    let mut blocked = Vec::new();
    for step in &first.install {
        match installer.install(name, step) {
            Ok(how) => {
                if how == Installed::Now {
                    installed.push(step.clone());
                }
                settled.push(step.clone());
            }
            Err(InstallError::Failed(reason)) => blocked.push(Blocked {
                subject: step.tool.clone(),
                missing: format!("{} {} was not installed", step.tool, step.version),
                steps: reason,
            }),
            Err(InstallError::Stop(error)) => {
                return Err(after_installing(
                    &installed,
                    &format!("{} {} was not installed", step.tool, step.version),
                    error,
                ))
            }
        }
    }
    let last = if settled.is_empty() {
        blocked.extend(first.blocked);
        worker
    } else {
        // Look again: what the worker says it has must now be there, whether
        // it was installed here or not. If the worker cannot be measured,
        // what was installed so far is still said.
        let after = measure().map_err(|e| {
            after_installing(&installed, "the worker could not be measured afterwards", e)
        })?;
        blocked.extend(envsync::verify(
            &settled,
            envsync::plan(parent, &after, project),
        ));
        after
    };
    if blocked.is_empty() {
        Ok(SyncResult {
            worker: name.into(),
            installed,
            in_sync: envdiff::all_same(&envdiff::diff(parent, &last)),
        })
    } else {
        Err(blocked_error(name, &installed, &blocked))
    }
}

/// One line for a sync that ended without anything blocked. It says "in
/// sync" exactly when `in_sync` is true; otherwise it leads with what is
/// still different, and says the project's requirements are met only when a
/// project was given.
pub fn render_sync(done: &SyncResult, project: bool) -> String {
    let name = &done.worker;
    let state = match (done.in_sync, project) {
        (true, _) => format!("worker {name} is in sync"),
        (false, true) => format!(
            "worker {name} is not in sync with the parent: it meets the project's requirements, and still differs from the parent where the project declares a tool (see workers diff {name})"
        ),
        (false, false) => format!(
            "worker {name} is not in sync with the parent: nothing is left for sync to install (see workers diff {name})"
        ),
    };
    if done.installed.is_empty() {
        return state;
    }
    let list: Vec<String> = done
        .installed
        .iter()
        .map(|s| format!("{} {}", s.tool, s.version))
        .collect();
    format!("{state}; installed {}", list.join(", "))
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
        if let Some(error) = &d.error {
            lines.push(format!("  could not be measured: {error}"));
        }
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
            // Values come from the worker's report.
            lines.push(strip_control(&line));
        }
    }
    lines.join("\n")
}

/// Ask a running resident which build it is. Missing credentials or a socket
/// nobody listens on mean no resident runs; a resident that is there and
/// does not answer the question is said to be unknown, never absent.
pub fn resident_build(spec: &Spec) -> ResidentBuild {
    let Some(cluster) = spec.expected_cluster.clone() else {
        return ResidentBuild::NotRunning;
    };
    let Ok(client) = super::resident::Client::load(&spec.state_dir, cluster) else {
        return ResidentBuild::NotRunning;
    };
    let unknown = |reason: String| ResidentBuild::Unknown { reason };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => return unknown(e.to_string()),
    };
    resident_answer(runtime.block_on(client.request(super::resident::Operation::Build)))
}

fn resident_answer(answer: anyhow::Result<super::resident::Reply>) -> ResidentBuild {
    use std::io::ErrorKind::{ConnectionRefused, NotFound};
    match answer {
        Ok(super::resident::Reply::Build {
            binary_sha256,
            features,
            required_features,
        }) => ResidentBuild::Running {
            binary_sha256,
            features,
            required_features,
        },
        Ok(super::resident::Reply::Error { message }) => ResidentBuild::Unknown { reason: message },
        Ok(_) => ResidentBuild::Unknown {
            reason: "it did not understand the question".into(),
        },
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| matches!(e.kind(), ConnectionRefused | NotFound)) =>
        {
            ResidentBuild::NotRunning
        }
        Err(error) => ResidentBuild::Unknown {
            reason: format!("it did not answer: {error}"),
        },
    }
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
                    serde_json::json!({"schema_version":1,"ok":false,"error":error})
                );
            } else {
                eprintln!("error: {}", error.message);
            }
            error.code.exit_code()
        }
    }
}

/// List registrations even when no resident is running. Only a resident
/// observation can establish that a worker is connected or disconnected.
pub fn list_command(json: bool) -> i32 {
    let env = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let result = registrations(&env).and_then(|entries| {
        let ids = entries
            .iter()
            .map(|entry| WorkerId::parse(&entry.id).map_err(|e| invalid(e.to_string())))
            .collect::<Result<Vec<_>, _>>()?;
        let observations = if ids.is_empty() {
            None
        } else {
            match super::context::runtime_spec(&env) {
                Ok(spec) => match resident_request(
                    &spec,
                    super::resident::Operation::ListWorkers { names: ids },
                ) {
                    Ok(super::resident::Reply::WorkersListed { observations }) => {
                        Some(observations)
                    }
                    Ok(super::resident::Reply::ControlError { error }) => return Err(error),
                    _ => None,
                },
                Err(e) if e.code == ErrorCode::NotFound => None,
                Err(e) => return Err(e),
            }
        };
        let mut text = Vec::new();
        let mut rows = Vec::new();
        for entry in entries {
            let key = WorkerId::parse(&entry.id).map_err(|e| invalid(e.to_string()))?;
            let observed = observations.as_ref().and_then(|m| m.get(&key));
            let connection = match observed {
                Some((true, _)) => "connected",
                Some((false, _)) => "disconnected",
                None => "unknown",
            };
            let identity = observed.and_then(|(_, id)| id.as_deref());
            let kind = if entry.ephemeral {
                "ephemeral"
            } else {
                "persistent"
            };
            text.push(format!(
                "{}  {}  {}  {}  {}",
                entry.id,
                entry.ssh,
                kind,
                connection,
                identity.unwrap_or("-")
            ));
            rows.push(
                serde_json::json!({"id":entry.id, "ssh":entry.ssh, "ephemeral":entry.ephemeral,
                "connection":connection, "identity":identity}),
            );
        }
        if text.is_empty() {
            text.push("no registered workers".into());
        }
        Ok((serde_json::json!({"workers":rows}), text.join("\n")))
    });
    match result {
        Ok((value, text)) => emit(json, Ok(value), || text),
        Err(error) => emit(json, Err(error), String::new),
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
        WorkersSub::Forget { name } => {
            let result = super::context::runtime_spec(&env)
                .and_then(|spec| forget(name, |worker| ask_resident_to_forget(&spec, worker)));
            let text = result.as_ref().map(describe_forget).unwrap_or_default();
            return Some(emit(
                json,
                result.map(|r| serde_json::to_value(r).expect("serializes")),
                || text,
            ));
        }
        WorkersSub::Add {
            name,
            ssh,
            install,
            binary,
            ephemeral,
        } => {
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
                        AddOptions {
                            install: *install,
                            ephemeral: *ephemeral,
                            binary: binary.as_deref(),
                        },
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
            let unmeasured = result.as_ref().is_ok_and(|(v, _)| {
                v["workers"]
                    .as_array()
                    .is_some_and(|w| w.iter().any(|d| d.get("error").is_some()))
            });
            let code = emit(json, result.map(|(v, _)| v), || text);
            // The others were still reported; the exit status says one was not.
            return Some(if unmeasured && code == crate::cli::exit::SUCCESS {
                ErrorCode::CommunicationUnknown.exit_code()
            } else {
                code
            });
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
            let unmeasured = result.as_ref().is_ok_and(|(v, _)| {
                v["workers"]
                    .as_array()
                    .is_some_and(|w| w.iter().any(|d| d.get("error").is_some()))
            });
            let code = emit(json, result.map(|(v, _)| v), || text);
            // The others were still reported; the exit status says one was not.
            return Some(if unmeasured && code == crate::cli::exit::SUCCESS {
                ErrorCode::CommunicationUnknown.exit_code()
            } else {
                code
            });
        }
        WorkersSub::Sync { name, project } => {
            let result = (|| {
                let exe = exe
                    .as_ref()
                    .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
                let spec = super::context::runtime_spec(&env)?;
                let requirements = project
                    .as_deref()
                    .map(|dir| read_project_requirements(Path::new(dir)))
                    .transpose()?;
                sync_worker(
                    &mut SshRemote,
                    &env,
                    exe,
                    name,
                    || crate::toolchain::envreport::collect(&spec.worker_state_dir, &spec.env),
                    requirements.as_deref(),
                    &mut warmup::ResidentWarmup::new(spec.clone()),
                )
            })()
            .map(|done| {
                let text = render_sync(&done, project.is_some());
                (serde_json::to_value(&done).expect("serializes"), text)
            });
            let text = result.as_ref().map(|(_, t)| t.clone()).unwrap_or_default();
            return Some(emit(json, result.map(|(v, _)| v), || text));
        }
        WorkersSub::Upgrade { name, binary } => {
            return Some(upgrade::command(name, binary.as_deref(), json, &env, exe))
        }
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

pub mod target;
pub mod upgrade;
pub mod warmup;

#[cfg(test)]
mod tests;
