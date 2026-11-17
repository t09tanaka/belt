//! Worker-host service helper (macOS). It runs on the machine that hosts the
//! worker, either directly or through a fixed `ssh` command from the parent
//! (`workers add --install`, `workers start|stop|drain|resume`).
//!
//! The helper is generic over [`Launcher`] so tests never register a real
//! LaunchAgent. Nothing here signals a process: the service is stopped only
//! through an acknowledged worker request followed by an ownership-checked
//! `bootout`, and a drain or stop never reports success it did not observe.
//! Cooperative lifecycle locks coordinate this helper's own invocations; they
//! do not defend against another same-UID actor replacing files.
use super::macos::{
    self, policy, DirectoryFiles, Launcher, LoadedState, ManagedFiles, Manager, WorkerControl,
    WorkerIdentity, WorkerRequest,
};
use super::resident;
use crate::{
    ids::{ClusterId, WorkerId},
    worker::control::{self, Control},
};
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const LIMIT: u64 = 262_144;
const ENV: &[&str] = &[
    "HOME",
    "PATH",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "XDG_CONFIG_HOME",
    // The scope of the task-traffic-controller CLI follows these two, so the
    // worker takes the values of the user's shell at install or init.
    "XDG_STATE_HOME",
];
const BINARY: &str = "remote-task-runner";
/// How long `install` and `start` wait for the agent socket to accept.
pub const READY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Action {
    Install,
    Start,
    Stop,
    Drain,
    Resume,
    Status,
}

/// What the parent sends on stdin for `install`. The key travels only here.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallRequest {
    pub schema: u32,
    pub cluster: ClusterId,
    pub worker_id: WorkerId,
    /// The cluster key file's text (lower-case hex).
    pub key_file_text: String,
    /// SHA-256 of the binary that runs `install`; it must be its own digest.
    pub binary_sha256: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Status {
    pub worker_id: WorkerId,
    pub cluster: ClusterId,
    pub binary_sha256: String,
    /// `None` when launchd's answer could not be matched to this service.
    pub loaded: Option<bool>,
    /// The agent socket accepts connections.
    pub ready: bool,
    pub draining: bool,
    pub cancel_running: bool,
    /// `None` when the worker database could not be read.
    pub live_attempts: Option<u32>,
    /// The version-independent absolute path of the installed binary, once
    /// `install` has created it. The parent stores this path and uses it for
    /// the ssh bridge and the lifecycle helper, so neither depends on the
    /// non-login ssh PATH.
    #[serde(default)]
    pub bridge_binary: Option<String>,
}

pub struct Paths {
    pub home: PathBuf,
    pub config: PathBuf,
    pub key: PathBuf,
    pub input: PathBuf,
    pub journal: PathBuf,
    pub state: PathBuf,
    pub services: PathBuf,
}
impl Paths {
    pub fn resolve(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let home = PathBuf::from(env.get("HOME").context("HOME is required")?);
        resident::validate_components(&home)?;
        if let Some(x) = env.get("XDG_CONFIG_HOME") {
            resident::validate_components(Path::new(x))?;
        }
        // The worker will share the scope of the user's CLI: refuse here, at
        // install, an environment it would refuse at start.
        crate::config::traffic::resolve(env).map_err(anyhow::Error::msg)?;
        let config = crate::config::paths::user_config_path(env)?
            .parent()
            .context("configuration parent")?
            .to_owned();
        Ok(Self {
            key: config.join("worker-cluster.key"),
            input: config.join("worker.json"),
            journal: config.join("worker-service.json"),
            state: home.join(".remote-task-runner"),
            services: home.join("Library/LaunchAgents"),
            config,
            home,
        })
    }
    pub fn executable(&self, digest: &str) -> PathBuf {
        self.config.join("bin").join(digest).join(BINARY)
    }
    /// A symlink to the installed version's directory.
    pub fn current_link(&self) -> PathBuf {
        self.config.join("bin").join("current")
    }
    /// The fixed absolute path the parent runs, whatever version is installed.
    pub fn bridge_binary(&self) -> PathBuf {
        self.current_link().join(BINARY)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    uid: u32,
    cluster: ClusterId,
    worker_id: WorkerId,
    binary_sha256: String,
    env: BTreeMap<String, String>,
    receipt: Option<macos::Receipt>,
}

fn euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

fn read_journal(paths: &Paths) -> anyhow::Result<Option<Journal>> {
    match fs::symlink_metadata(&paths.journal) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let mut bytes = Vec::new();
    resident::secure_file(&paths.journal, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= LIMIT,
        "worker service journal too large"
    );
    let journal: Journal = serde_json::from_slice(&bytes)?;
    ensure!(
        journal.schema_version == 1 && journal.uid == euid(),
        "foreign worker service journal"
    );
    Ok(Some(journal))
}

fn save_journal(paths: &Paths, journal: &Journal) -> anyhow::Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(&paths.config)?;
    temporary.write_all(&serde_json::to_vec(journal)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(&paths.journal).map_err(|e| e.error)?;
    File::open(&paths.config)?.sync_all()?;
    Ok(())
}

fn require_journal(paths: &Paths) -> anyhow::Result<Journal> {
    read_journal(paths)?.context("no worker is installed here; run workers add --install")
}

/// Create missing directories below `home` as 0700; refuse symlinks. The
/// leaf must end up private to this user.
fn private_dir(home: &Path, leaf: &Path) -> anyhow::Result<()> {
    let relative = leaf
        .strip_prefix(home)
        .context("directory outside the home directory")?;
    let mut prefix = home.to_owned();
    for component in relative.components() {
        prefix.push(component);
        match fs::symlink_metadata(&prefix) {
            Ok(m) => ensure!(
                m.is_dir() && !m.file_type().is_symlink(),
                "unsafe directory ancestor"
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match fs::DirBuilder::new().mode(0o700).create(&prefix) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(e.into()),
                }
                File::open(prefix.parent().context("parent")?)?.sync_all()?;
            }
            Err(e) => return Err(e.into()),
        }
    }
    resident::check_private_dir(leaf)?;
    Ok(())
}

/// Publish `bytes` at `path` unless identical bytes are already there. Any
/// other existing content is refused: replacing enrollment is not an install.
/// With `replace`, an earlier install that never reached a service
/// registration (no receipt) may be overwritten, so a re-run can finish.
fn publish_exact(path: &Path, bytes: &[u8], mode: u32, replace: bool) -> anyhow::Result<()> {
    let parent = path.parent().context("file parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(mode))?;
    temporary.as_file().sync_all()?;
    if replace {
        temporary.persist(path).map_err(|e| e.error)?;
        File::open(parent)?.sync_all()?;
        return Ok(());
    }
    match temporary.persist_noclobber(path) {
        Ok(_) => {
            File::open(parent)?.sync_all()?;
            Ok(())
        }
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut existing = Vec::new();
            resident::secure_file(path, false)?
                .take(bytes.len() as u64 + 1)
                .read_to_end(&mut existing)?;
            ensure!(
                existing == bytes,
                "existing file differs; refusing to replace {}",
                path.display()
            );
            Ok(())
        }
        Err(e) => Err(e.error.into()),
    }
}

pub fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn place_binary(paths: &Paths, digest: &str, current: &Path) -> anyhow::Result<PathBuf> {
    ensure!(
        digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "binary digest is not lower-case hex"
    );
    let target = paths.executable(digest);
    let directory = target.parent().context("version directory")?;
    private_dir(&paths.home, directory)?;
    match fs::symlink_metadata(&target) {
        Ok(m) => {
            ensure!(m.is_file(), "version binary is not a regular file");
            ensure!(
                sha256_file(&target)? == digest,
                "versioned binary differs from its name; refusing to overwrite"
            );
            return Ok(target);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let temporary = tempfile::Builder::new()
        .prefix(".part")
        .tempfile_in(directory)?;
    std::io::copy(&mut File::open(current)?, &mut temporary.as_file())?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o755))?;
    temporary.as_file().sync_all()?;
    ensure!(
        sha256_file(temporary.path())? == digest,
        "copied binary does not match the digest"
    );
    temporary.persist_noclobber(&target).map_err(|e| e.error)?;
    File::open(directory)?.sync_all()?;
    Ok(target)
}

/// `bin/current` -> `<digest>`: the version-independent path. Created once; a
/// link that already names this version is left alone, any other is refused
/// (moving it is an upgrade, which is a separate operation).
fn link_current(paths: &Paths, digest: &str) -> anyhow::Result<()> {
    let link = paths.current_link();
    match fs::read_link(&link) {
        Ok(target) => {
            ensure!(
                target == Path::new(digest),
                "bin/current names another version; refusing to move it"
            );
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match std::os::unix::fs::symlink(digest, &link) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return link_current(paths, digest)
                }
                Err(e) => return Err(e.into()),
            }
            File::open(link.parent().context("link parent")?)?.sync_all()?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn worker_env(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    env.iter()
        .filter(|(k, _)| ENV.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn plan(paths: &Paths, journal: &Journal) -> anyhow::Result<policy::Plan> {
    let label = format!(
        "io.github.t09tanaka.rtr.worker.{}.{}",
        journal.uid,
        &format!(
            "{:x}",
            Sha256::digest(paths.config.as_os_str().as_encoded_bytes())
        )[..12]
    );
    // The service environment never carries a configuration or state
    // redirect: the worker reads both from the journal, not from its process.
    let environment = journal
        .env
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "XDG_CONFIG_HOME" | "XDG_STATE_HOME"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    policy::render(&policy::Definition {
        label,
        mode: policy::Mode::User { uid: journal.uid },
        executable: paths.executable(&journal.binary_sha256),
        arguments: vec![
            "worker".into(),
            "--input".into(),
            paths
                .input
                .to_str()
                .context("input path must be UTF-8")?
                .into(),
        ],
        environment,
        working_directory: paths.config.clone(),
    })
    .map_err(anyhow::Error::msg)
}

fn agent_reachable(state: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(state.join("agent.sock")).is_ok()
}

/// Live attempts according to the worker database; none if it was never created.
pub fn live_attempts(state: &Path) -> anyhow::Result<u32> {
    match fs::symlink_metadata(state.join("worker.db")) {
        Ok(_) => {}
        // Only a database that was never created counts as no attempts; an
        // unreadable state directory is an error, never idle.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    }
    let store = crate::storage::WorkerStore::open_read_only(state)?;
    Ok(u32::try_from(store.list_nonterminal()?.len()).unwrap_or(u32::MAX))
}

/// Poll `count` until it reports zero. A failed read is an error, never idle.
pub fn wait_until_idle(
    mut count: impl FnMut() -> anyhow::Result<u32>,
    timeout: Duration,
    interval: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut settling = false;
    loop {
        let live = count()?;
        if live == 0 {
            // An acceptance that began before the drain marker may still be
            // committing: only a second zero one interval later is idle.
            if settling {
                return Ok(());
            }
            settling = true;
            std::thread::sleep(interval);
            continue;
        }
        settling = false;
        if Instant::now() >= deadline {
            anyhow::bail!(
                "{live} attempt(s) are still live; the worker stays drained, run stop again to keep waiting"
            );
        }
        std::thread::sleep(interval);
    }
}

/// Acknowledges requests by durable marker plus, for stop, observed idleness.
/// The binding file proves the state directory belongs to the expected identity.
pub struct MarkerControl {
    pub state: PathBuf,
    pub wait: Duration,
    pub interval: Duration,
}
impl MarkerControl {
    fn verify_binding(&self, identity: &WorkerIdentity) -> anyhow::Result<()> {
        let path = self.state.join("worker-binding.json");
        let mut bytes = Vec::new();
        resident::secure_file(&path, false)?
            .take(LIMIT + 1)
            .read_to_end(&mut bytes)?;
        let (schema, cluster, worker, _key_digest): (u32, ClusterId, WorkerId, String) =
            serde_json::from_slice(&bytes).context("worker binding unreadable")?;
        ensure!(
            schema == 1 && cluster == identity.cluster_id && worker == identity.worker_id,
            "worker state is bound to a different identity"
        );
        Ok(())
    }
}
impl WorkerControl for MarkerControl {
    fn request(&mut self, identity: &WorkerIdentity, request: WorkerRequest) -> anyhow::Result<()> {
        self.verify_binding(identity)?;
        match request {
            WorkerRequest::Drain => control::write(&self.state, Control::draining())?,
            WorkerRequest::Resume => control::write(&self.state, Control::default())?,
            WorkerRequest::Stop { cancel_running } => {
                control::write(
                    &self.state,
                    if cancel_running {
                        Control::cancelling()
                    } else {
                        Control::draining()
                    },
                )?;
                let state = self.state.clone();
                wait_until_idle(|| live_attempts(&state), self.wait, self.interval)?;
            }
        }
        Ok(())
    }
}

fn identity(journal: &Journal) -> WorkerIdentity {
    WorkerIdentity {
        cluster_id: journal.cluster.clone(),
        worker_id: journal.worker_id.clone(),
    }
}

fn marker(paths: &Paths, wait: Duration) -> MarkerControl {
    MarkerControl {
        state: paths.state.clone(),
        wait,
        interval: Duration::from_millis(250),
    }
}

fn wait_ready(state: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if agent_reachable(state) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn ensure_started<L: Launcher>(
    files: &mut DirectoryFiles,
    launcher: &mut L,
    plan: &policy::Plan,
    receipt: &macos::Receipt,
) -> anyhow::Result<()> {
    let _lease = files.lifecycle_lock()?;
    files.verify(receipt)?;
    match launcher.loaded_state(plan, receipt) {
        LoadedState::Match => Ok(()),
        LoadedState::Absent => launcher
            .launchctl(
                &plan
                    .start_arguments(receipt.path())
                    .map_err(anyhow::Error::msg)?,
            )
            .map_err(|e| anyhow::anyhow!("{e}; managed file retained, run start again")),
        LoadedState::Unknown => anyhow::bail!(
            "loaded service identity unknown; reconciliation required (nothing started or stopped)"
        ),
    }
}

fn services(paths: &Paths) -> anyhow::Result<DirectoryFiles> {
    if fs::symlink_metadata(&paths.services).is_err() {
        // Created user-private-ish (0755) like the system's own directory.
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o755)
            .create(&paths.services)?;
    }
    DirectoryFiles::open(&paths.services)
}

/// Resumable: every step either completes or verifies what an earlier run left.
pub fn install<L: Launcher>(
    env: &BTreeMap<String, String>,
    request: &InstallRequest,
    current_exe: &Path,
    mut launcher: L,
    ready_timeout: Duration,
) -> anyhow::Result<Status> {
    ensure!(request.schema == 1, "unsupported install request schema");
    let paths = Paths::resolve(env)?;
    let digest = sha256_file(current_exe)?;
    ensure!(
        digest == request.binary_sha256,
        "the installing binary is not the one the parent sent"
    );
    // Validate before any file is published: a rejected key must leave nothing behind.
    let key_bytes = hex_decode(request.key_file_text.trim_end_matches('\n'))
        .context("cluster key is not lower-case hex")?;
    crate::secrets::key::ClusterKey::from_bytes(key_bytes).context("cluster key rejected")?;
    private_dir(&paths.home, &paths.config)?;
    private_dir(&paths.home, &paths.state)?;
    let existing = read_journal(&paths)?;
    if let Some(existing) = &existing {
        ensure!(
            existing.cluster == request.cluster
                && existing.worker_id == request.worker_id
                && existing.binary_sha256 == digest,
            "a different worker or binary is already installed; upgrade or removal is a separate operation"
        );
    }
    // Once a service file was published the installed files are fixed (the
    // journal's environment included, so a re-run from another shell cannot
    // trip the comparison). Before that nothing runs yet and a re-run replaces.
    let registered = existing.as_ref().is_some_and(|j| j.receipt.is_some());
    let env = match &existing {
        Some(journal) if registered => journal.env.clone(),
        _ => worker_env(env),
    };
    let executable = place_binary(&paths, &digest, current_exe)?;
    link_current(&paths, &digest)?;
    publish_exact(
        &paths.key,
        request.key_file_text.as_bytes(),
        0o600,
        !registered,
    )?;
    crate::secrets::key::load(&paths.key).context("cluster key rejected")?;
    let spec = super::worker::WorkerSpec {
        schema: 1,
        expected_cluster: request.cluster.clone(),
        worker_id: request.worker_id.clone(),
        state_dir: paths.state.clone(),
        cluster_key_path: paths.key.clone(),
        executable,
        env,
        config: "schema_version = 1\n".into(),
    };
    publish_exact(
        &paths.input,
        &serde_json::to_vec(&spec)?,
        0o600,
        !registered,
    )?;
    let mut journal = match existing {
        Some(mut journal) => {
            if journal.env != spec.env {
                journal.env = spec.env.clone();
                save_journal(&paths, &journal)?;
            }
            journal
        }
        None => {
            let fresh = Journal {
                schema_version: 1,
                uid: euid(),
                cluster: request.cluster.clone(),
                worker_id: request.worker_id.clone(),
                binary_sha256: digest.clone(),
                env: spec.env.clone(),
                receipt: None,
            };
            save_journal(&paths, &fresh)?;
            fresh
        }
    };
    let plan = plan(&paths, &journal)?;
    let mut files = services(&paths)?;
    if journal.receipt.is_none() {
        let _lease = files.lifecycle_lock()?;
        let receipt = files
            .install_journaled(&plan, |receipt, _stage| {
                journal.receipt = Some(receipt.clone());
                save_journal(&paths, &journal)
            })
            .map_err(|e| {
                anyhow::anyhow!(
                    "service file publication failed ({e}); reconciliation may be required"
                )
            })?;
        ensure!(
            journal.receipt.as_ref() == Some(&receipt),
            "service receipt differs from the journal"
        );
    }
    let receipt = journal.receipt.clone().context("service receipt missing")?;
    // Starting never resumes a previous stop's cancel request. It is cleared
    // before the agent can run, so the new process never sees the old one.
    clear_cancel(&paths.state)?;
    ensure_started(&mut files, &mut launcher, &plan, &receipt)?;
    let ready = wait_ready(&paths.state, ready_timeout);
    status_with(&paths, &journal, &plan, &receipt, &mut launcher, ready)
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if text.is_empty() || !text.len().is_multiple_of(2) {
        return None;
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            let digit = |b: u8| match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                _ => None,
            };
            Some(digit(pair[0])? << 4 | digit(pair[1])?)
        })
        .collect()
}

fn clear_cancel(state: &Path) -> anyhow::Result<()> {
    let current = control::read(state).unwrap_or_else(|_| Control::draining());
    if current.cancel_running {
        control::write(state, Control::draining())?;
    }
    Ok(())
}

pub fn start<L: Launcher>(
    env: &BTreeMap<String, String>,
    mut launcher: L,
    ready_timeout: Duration,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve(env)?;
    let journal = require_journal(&paths)?;
    let plan = plan(&paths, &journal)?;
    let receipt = journal
        .receipt
        .clone()
        .context("service was never installed")?;
    let mut files = services(&paths)?;
    // Only a bounded, identity-checked marker may be cleared here.
    marker(&paths, Duration::ZERO).verify_binding(&identity(&journal))?;
    clear_cancel(&paths.state)?;
    ensure_started(&mut files, &mut launcher, &plan, &receipt)?;
    let ready = wait_ready(&paths.state, ready_timeout);
    status_with(&paths, &journal, &plan, &receipt, &mut launcher, ready)
}

/// Drain, wait for the accepted work to finish (or cancel it), then boot the
/// service out. An already unloaded service is reported stopped without a
/// launchctl change; an unreadable or uncertain state is an error.
pub fn stop<L: Launcher>(
    env: &BTreeMap<String, String>,
    mut launcher: L,
    cancel_running: bool,
    wait: Duration,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve(env)?;
    let journal = require_journal(&paths)?;
    let plan = plan(&paths, &journal)?;
    let receipt = journal
        .receipt
        .clone()
        .context("service was never installed")?;
    let files = services(&paths)?;
    match launcher.loaded_state(&plan, &receipt) {
        LoadedState::Absent => {}
        LoadedState::Unknown => anyhow::bail!(
            "loaded service identity unknown; reconciliation required (nothing stopped)"
        ),
        LoadedState::Match => {
            let mut manager = Manager {
                identity: identity(&journal),
                files,
                launcher,
                worker: marker(&paths, wait),
            };
            manager.stop(&plan, &receipt, cancel_running)?;
            control::write(&paths.state, Control::default())?;
            let mut launcher = manager.launcher;
            return status_with(&paths, &journal, &plan, &receipt, &mut launcher, false);
        }
    }
    status_with(&paths, &journal, &plan, &receipt, &mut launcher, false)
}

pub fn drain<L: Launcher>(
    env: &BTreeMap<String, String>,
    mut launcher: L,
) -> anyhow::Result<Status> {
    lifecycle_marker(env, &mut launcher, WorkerRequest::Drain)
}

pub fn resume<L: Launcher>(
    env: &BTreeMap<String, String>,
    mut launcher: L,
) -> anyhow::Result<Status> {
    lifecycle_marker(env, &mut launcher, WorkerRequest::Resume)
}

fn lifecycle_marker<L: Launcher>(
    env: &BTreeMap<String, String>,
    launcher: &mut L,
    request: WorkerRequest,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve(env)?;
    let journal = require_journal(&paths)?;
    let plan = plan(&paths, &journal)?;
    let receipt = journal
        .receipt
        .clone()
        .context("service was never installed")?;
    marker(&paths, Duration::ZERO).request(&identity(&journal), request)?;
    status_with(
        &paths,
        &journal,
        &plan,
        &receipt,
        launcher,
        agent_reachable(&paths.state),
    )
}

pub fn status<L: Launcher>(
    env: &BTreeMap<String, String>,
    mut launcher: L,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve(env)?;
    let journal = require_journal(&paths)?;
    let plan = plan(&paths, &journal)?;
    let receipt = journal
        .receipt
        .clone()
        .context("service was never installed")?;
    let ready = agent_reachable(&paths.state);
    status_with(&paths, &journal, &plan, &receipt, &mut launcher, ready)
}

fn status_with<L: Launcher>(
    paths: &Paths,
    journal: &Journal,
    plan: &policy::Plan,
    receipt: &macos::Receipt,
    launcher: &mut L,
    ready: bool,
) -> anyhow::Result<Status> {
    let loaded = match launcher.loaded_state(plan, receipt) {
        LoadedState::Match => Some(true),
        LoadedState::Absent => Some(false),
        LoadedState::Unknown => None,
    };
    let current = Control::effective(&paths.state);
    Ok(Status {
        worker_id: journal.worker_id.clone(),
        cluster: journal.cluster.clone(),
        binary_sha256: journal.binary_sha256.clone(),
        loaded,
        ready: ready && loaded != Some(false),
        draining: current.drain,
        cancel_running: current.cancel_running,
        live_attempts: live_attempts(&paths.state).ok(),
        bridge_binary: fs::symlink_metadata(paths.current_link())
            .ok()
            .and_then(|_| paths.bridge_binary().to_str().map(str::to_owned)),
    })
}

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub cancel_running: bool,
    /// How long stop waits for accepted work to finish.
    pub wait: Duration,
    pub ready_timeout: Duration,
}

/// One action on this host. The caller supplies the launcher.
pub fn handle<L: Launcher>(
    action: Action,
    env: &BTreeMap<String, String>,
    stdin: &[u8],
    current_exe: &Path,
    launcher: L,
    options: Options,
) -> anyhow::Result<Status> {
    match action {
        Action::Install => {
            let request: InstallRequest =
                serde_json::from_slice(stdin).context("install request unreadable")?;
            install(env, &request, current_exe, launcher, options.ready_timeout)
        }
        Action::Start => start(env, launcher, options.ready_timeout),
        Action::Stop => stop(env, launcher, options.cancel_running, options.wait),
        Action::Drain => drain(env, launcher),
        Action::Resume => resume(env, launcher),
        Action::Status => status(env, launcher),
    }
}

/// The hidden `worker-service` command. Drain, resume and status never touch
/// launchctl; the others use the native launcher.
pub fn main(action: Action, cancel_running: bool, wait_secs: u64) -> i32 {
    let env: BTreeMap<String, String> = ENV
        .iter()
        .filter_map(|k| Some((k.to_string(), std::env::var_os(k)?.into_string().ok()?)))
        .collect();
    let mut stdin = Vec::new();
    if action == Action::Install
        && std::io::stdin()
            .take(LIMIT + 1)
            .read_to_end(&mut stdin)
            .map_or(true, |_| stdin.len() as u64 > LIMIT)
    {
        println!(
            "{}",
            serde_json::json!({"ok": false, "error": "install request unreadable"})
        );
        return 2;
    }
    let result = std::env::current_exe()
        .map_err(anyhow::Error::from)
        .and_then(|exe| {
            handle(
                action,
                &env,
                &stdin,
                &exe,
                macos::NativeLauncher::default(),
                Options {
                    cancel_running,
                    wait: Duration::from_secs(wait_secs),
                    ready_timeout: READY_TIMEOUT,
                },
            )
        });
    match result {
        Ok(status) => {
            println!("{}", serde_json::json!({"ok": true, "status": status}));
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

#[cfg(test)]
pub(crate) mod tests;
