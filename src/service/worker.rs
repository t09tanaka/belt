//! Foreground persistent worker. SSH owns only a bridge, never this runtime.
use super::resident;
use crate::config::{self, runtime::WorkerInputs};
use crate::ids::{ClusterId, WorkerBootId, WorkerId};
use crate::worker::{agent::Agent, serve};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

pub mod ephemeral;

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

/// Provisioned out of band; never generated from an unverified peer hello.
/// The config snapshot is user TOML, resolved without reading mutable config files.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSpec {
    pub schema: u32,
    pub expected_cluster: ClusterId,
    pub worker_id: WorkerId,
    pub state_dir: PathBuf,
    pub cluster_key_path: PathBuf,
    pub executable: PathBuf,
    pub env: BTreeMap<String, String>,
    pub config: String,
}

impl WorkerSpec {
    fn validate(&self, home: &Path) -> anyhow::Result<config::ResolvedConfig> {
        anyhow::ensure!(self.schema == 1, "unsupported worker input schema");
        anyhow::ensure!(
            self.env.keys().all(|k| ENV.contains(&k.as_str())),
            "worker environment is not allowlisted"
        );
        anyhow::ensure!(
            self.env.get("HOME").map(Path::new) == Some(home),
            "worker HOME differs from bridge HOME"
        );
        anyhow::ensure!(
            self.state_dir == home.join(".remote-task-runner"),
            "worker state must match default bridge socket directory"
        );
        resident::check_private_dir(&self.state_dir)?;
        resident::reject_symlinks(&self.state_dir)?;
        resident::validate_components(&self.executable)?;
        let meta = std::fs::metadata(&self.executable)?;
        anyhow::ensure!(
            meta.is_file() && meta.permissions().mode() & 0o111 != 0,
            "invalid pinned worker executable"
        );
        anyhow::ensure!(
            std::fs::canonicalize(&self.executable)? == std::env::current_exe()?,
            "worker executable differs from pinned input"
        );
        let mut key = resident::secure_file(&self.cluster_key_path, false)?;
        crate::secrets::key::load_opened(&self.cluster_key_path, &mut key)?;
        let snapshot = config::resolve::Loaded {
            path: "retained worker configuration".into(),
            value: config::parse::parse_user("retained worker configuration", &self.config)?,
        };
        let config = config::resolve(&config::Inputs {
            overlay: &config::Overlay::default(),
            project: None,
            legacy: None,
            user: Some(&snapshot),
        })?;
        anyhow::ensure!(
            config.workers.is_empty(),
            "standalone worker input cannot enroll workers"
        );
        config.runtime_settings()?;
        Ok(config)
    }
}

/// Bounded, nofollow, owner-only input. Reading creates no worker identity/state.
pub fn read_input(path: &Path) -> anyhow::Result<WorkerSpec> {
    resident::check_private_dir(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("worker input parent missing"))?,
    )?;
    let mut bytes = Vec::new();
    resident::secure_file(path, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= LIMIT, "worker input too large");
    Ok(serde_json::from_slice(&bytes)?)
}

fn private_lock(state: &Path, name: &str) -> anyhow::Result<File> {
    let path = state.join(name);
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(file) => drop(file),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let file = resident::secure_file(&path, true)?;
    file.try_lock()
        .map_err(|e| anyhow::anyhow!("worker owner already held or unsafe: {e}"))?;
    Ok(file)
}

/// Durable cluster binding is separate from the coordinator database. Existing
/// bytes must match exactly: changing cluster, worker identity or key is enrollment.
fn bind(spec: &WorkerSpec) -> anyhow::Result<()> {
    use sha2::{Digest, Sha256};
    let mut key = Vec::new();
    resident::secure_file(&spec.cluster_key_path, false)?
        .take(4097)
        .read_to_end(&mut key)?;
    let bytes = serde_json::to_vec(&(
        1,
        &spec.expected_cluster,
        &spec.worker_id,
        format!("{:x}", Sha256::digest(&key)),
    ))?;
    let path = spec.state_dir.join("worker-binding.json");
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            let mut existing = Vec::new();
            resident::secure_file(&path, false)?
                .take(LIMIT + 1)
                .read_to_end(&mut existing)?;
            anyhow::ensure!(existing == bytes, "foreign worker enrollment binding");
            return Ok(());
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    // Missing enrollment is not evidence of a fresh worker. Never infer a
    // cluster/key for an existing DB, boot identity, attempt or listener history.
    // This check and initial publication run under both owner and agent locks,
    // before any writable WorkerStore is opened or agent_epoch is incremented.
    for entry in std::fs::read_dir(&spec.state_dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        anyhow::ensure!(
            !name.starts_with("worker.db")
                && ![
                    "boot_id",
                    "attempts",
                    "agent.sock",
                    "worker-socket.json",
                    "traffic"
                ]
                .contains(&name.as_ref()),
            "worker enrollment binding missing for existing state; explicit migration required"
        );
    }
    let mut temporary = tempfile::NamedTempFile::new_in(&spec.state_dir)?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(&path) {
        Ok(_) => File::open(&spec.state_dir)?.sync_all()?,
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut existing = Vec::new();
            resident::secure_file(&path, false)?
                .take(LIMIT + 1)
                .read_to_end(&mut existing)?;
            anyhow::ensure!(existing == bytes, "foreign worker enrollment binding");
        }
        Err(e) => return Err(e.error.into()),
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SocketReceipt {
    cluster: ClusterId,
    worker: WorkerId,
    dev: u64,
    ino: u64,
}

fn reconcile_socket(spec: &WorkerSpec, socket: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let meta = match std::fs::symlink_metadata(socket) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    resident::secure_file(&spec.state_dir.join("worker-socket.json"), false)?
        .take(4097)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= 4096, "socket receipt too large");
    let receipt: SocketReceipt = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        meta.file_type().is_socket()
            && meta.uid() == rustix::process::geteuid().as_raw()
            && meta.mode() & 0o077 == 0
            && meta.dev() == receipt.dev
            && meta.ino() == receipt.ino
            && receipt.cluster == spec.expected_cluster
            && receipt.worker == spec.worker_id,
        "foreign worker socket ownership"
    );
    match std::os::unix::net::UnixStream::connect(socket) {
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {}
        _ => anyhow::bail!("worker socket is live or its ownership is uncertain"),
    }
    // Recheck inode immediately before removing only our recorded dead listener.
    let current = std::fs::symlink_metadata(socket)?;
    anyhow::ensure!(
        current.dev() == receipt.dev && current.ino() == receipt.ino,
        "worker socket changed during reconciliation"
    );
    std::fs::remove_file(socket)?;
    File::open(&spec.state_dir)?.sync_all()?;
    Ok(())
}

fn publish_socket(
    spec: &WorkerSpec,
    socket: &Path,
) -> anyhow::Result<(tokio::net::UnixListener, OwnedSocket)> {
    // A crash before the durable receipt never leaves an unreceipted canonical
    // socket. Abandoned unpublished staging directories are left for reconciliation.
    let stage = tempfile::Builder::new()
        .prefix(".s")
        .tempdir_in(&spec.state_dir)?;
    std::fs::set_permissions(stage.path(), std::fs::Permissions::from_mode(0o700))?;
    // .sXXXXXX/s is no longer than agent.sock on Unix socket paths.
    let staged = stage.path().join("s");
    let listener = crate::protocol::transport::listen_unix(&staged)?;
    let meta = std::fs::symlink_metadata(&staged)?;
    let receipt = SocketReceipt {
        cluster: spec.expected_cluster.clone(),
        worker: spec.worker_id.clone(),
        dev: meta.dev(),
        ino: meta.ino(),
    };
    let receipt_path = spec.state_dir.join("worker-socket.json");
    if receipt_path.try_exists()? {
        resident::secure_file(&receipt_path, false)?;
    }
    let mut temporary = tempfile::NamedTempFile::new_in(&spec.state_dir)?;
    temporary.write_all(&serde_json::to_vec(&receipt)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(&receipt_path)?;
    File::open(&spec.state_dir)?.sync_all()?;
    anyhow::ensure!(
        std::fs::symlink_metadata(socket).is_err(),
        "worker socket appeared during publication"
    );
    std::fs::rename(&staged, socket)?;
    let owned = OwnedSocket {
        path: socket.to_owned(),
        dev: meta.dev(),
        ino: meta.ino(),
    };
    File::open(&spec.state_dir)?.sync_all()?;
    Ok((listener, owned))
}

struct OwnedSocket {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if let Ok(meta) = std::fs::symlink_metadata(&self.path) {
            if meta.dev() == self.dev && meta.ino() == self.ino {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

struct Worker {
    agent: Arc<Agent>,
    _owner: File,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.agent.shutdown();
    }
}

/// `boot_id` replaces the OS boot identity: a worker that has no boot of its
/// own to be told apart by (an ephemeral one) names its start instead.
async fn run(
    spec: WorkerSpec,
    config: config::ResolvedConfig,
    boot_id: Option<WorkerBootId>,
) -> anyhow::Result<()> {
    let owner = private_lock(&spec.state_dir, "worker-owner.lock")?;
    // Serialize enrollment against every Agent, including the combined resident.
    // Transfer the same descriptor into Agent; enrollment through DB open has no unlock gap.
    let agent_gate = private_lock(&spec.state_dir, "agent.lock")?;
    let socket = spec.state_dir.join("agent.sock");
    // Enrollment is checked before any recorded old listener can be removed.
    bind(&spec)?;
    reconcile_socket(&spec, &socket)?;
    let boot = match boot_id {
        Some(boot) => boot,
        None => {
            let os = crate::platform::boot_id()
                .ok_or_else(|| anyhow::anyhow!("OS boot identity unavailable"))?;
            WorkerBootId::parse(&os)?
        }
    };
    let settings = config.runtime_settings()?;
    // Commands of this worker that still run in another scope: wait for
    // them, not serving, instead of failing into a restart loop.
    if !super::traffic_hold::wait_until_clear(&spec.state_dir, &spec.env).await? {
        return Ok(());
    }
    let runtime = config.worker_runtime(WorkerInputs {
        state_dir: spec.state_dir.clone(),
        worker_id: spec.worker_id.clone(),
        boot_id: boot,
        supervisor_exe: spec.executable.clone(),
        cluster_key_path: spec.cluster_key_path.clone(),
        env: spec.env.clone(),
    })?;
    let agent = tokio::task::spawn_blocking(move || runtime.start_owned(agent_gate))
        .await?
        .map_err(|e| anyhow::anyhow!(e.message))?;
    let worker = Worker {
        agent,
        _owner: owner,
    };
    // Install handlers before publication/background tasks so readiness also
    // implies the foreground role can receive its service stop signal.
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let (listener, _socket) = publish_socket(&spec, &socket)?;
    let permits = Arc::new(Semaphore::new(serve::MAX_CONCURRENT_REQUESTS));
    let mut connections = tokio::task::JoinSet::new();
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let agent = worker.agent.clone();
    let mut supervisor = tokio::spawn(async move {
        let mut cadence = tokio::time::interval(Duration::from_millis(settings.heartbeat_ms));
        loop {
            tokio::select! {
                _ = stopped.changed() => return Ok::<_, anyhow::Error>(()),
                _ = cadence.tick() => {
                    let agent = agent.clone();
                    tokio::task::spawn_blocking(move || agent.supervise()).await?;
                }
            }
        }
    });
    let mut housekeeping = tokio::task::JoinSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut supervisor_done = false;
    let result: anyhow::Result<()> = loop {
        tokio::select! {
            r = tokio::signal::ctrl_c() => break r.map_err(Into::into),
            done = &mut supervisor => {
                supervisor_done = true;
                break match done { Ok(result) => result, Err(e) => Err(e.into()) };
            },
            _ = terminate.recv() => break Ok(()),
            _ = ticker.tick(), if housekeeping.is_empty() => {
                let agent = worker.agent.clone();
                housekeeping.spawn_blocking(move || agent.tick());
            }
            Some(done) = housekeeping.join_next(), if !housekeeping.is_empty() => {
                if let Err(e) = done { break Err(e.into()); }
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            incoming = listener.accept() => match incoming {
                Ok((stream, _)) => {
                    let agent = worker.agent.clone(); let cluster = spec.expected_cluster.clone(); let permits = permits.clone();
                    connections.spawn(async move { serve::serve_connection(agent, crate::protocol::Io::from_stream(stream), cluster, permits).await });
                },
                Err(e) => break Err(e.into()),
            }
        }
    };
    drop(listener);
    let _ = stop.send(true);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    // Accepted blocking requests finish before releasing the agent owner lock.
    let _drained = permits
        .acquire_many(serve::MAX_CONCURRENT_REQUESTS as u32)
        .await?;
    let supervised = if supervisor_done {
        Ok(())
    } else {
        supervisor
            .await
            .map_err(anyhow::Error::from)
            .and_then(|done| done)
    };
    let mut ticking_error = None;
    while let Some(done) = housekeeping.join_next().await {
        if let Err(e) = done {
            ticking_error.get_or_insert(e);
        }
    }
    supervised?;
    if let Some(e) = ticking_error {
        return Err(e.into());
    }
    result?;
    Ok(())
}

/// Foreground public role. Bridge EOF is deliberately absent from this lifetime.
pub fn main(input: &Path) -> i32 {
    let result = (|| -> anyhow::Result<()> {
        let spec = read_input(input)?;
        let home =
            std::env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("worker HOME missing"))?;
        let config = spec.validate(Path::new(&home))?;
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(run(spec, config, None))
    })();
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: worker failed: {e}");
            1
        }
    }
}
