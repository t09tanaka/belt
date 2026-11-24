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
pub(super) mod rotation_binding;

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
            self.state_dir == home.join(".belt"),
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

fn private_lock(state: &Path, name: &str) -> anyhow::Result<crate::platform::flock::HeldLock> {
    let file = open_private_lock(state, name)?;
    crate::platform::flock::HeldLock::try_lock(file)
        .map_err(|e| anyhow::anyhow!("worker owner already held or unsafe: {e}"))
}

/// The private lock file `name` of a worker state, created when missing and
/// not yet locked.
fn open_private_lock(state: &Path, name: &str) -> std::io::Result<File> {
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
        Err(e) => return Err(e),
    }
    resident::secure_file(&path, true)
}

/// Durable cluster binding is separate from the coordinator database. Existing
/// bytes must match exactly: changing cluster, worker identity or key is enrollment.
fn bind(spec: &WorkerSpec) -> anyhow::Result<()> {
    let bytes = rotation_binding::expected(
        &spec.cluster_key_path,
        &spec.expected_cluster,
        &spec.worker_id,
    )?;
    let path = spec.state_dir.join("worker-binding.json");
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            let mut existing = Vec::new();
            resident::secure_file(&path, false)?
                .take(LIMIT + 1)
                .read_to_end(&mut existing)?;
            rotation_binding::reconcile(&spec.state_dir, &existing, &bytes)?;
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
    let mut temporary = crate::platform::owned_temp::file_in(&spec.state_dir)?;
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

fn reconcile_socket(
    state: &Path,
    cluster: &ClusterId,
    worker: &WorkerId,
    socket: &Path,
) -> anyhow::Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let meta = match std::fs::symlink_metadata(socket) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    resident::secure_file(&state.join("worker-socket.json"), false)?
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
            && receipt.cluster == *cluster
            && receipt.worker == *worker,
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
    File::open(state)?.sync_all()?;
    Ok(())
}

fn publish_socket(
    state: &Path,
    cluster: &ClusterId,
    worker: &WorkerId,
    socket: &Path,
) -> anyhow::Result<(tokio::net::UnixListener, OwnedSocket)> {
    // A crash before the durable receipt never leaves an unreceipted canonical
    // socket. Abandoned unpublished staging directories are left for reconciliation.
    // A marker names the owner, so a later start removes what a crash left.
    crate::platform::owned_temp::sweep(state, ".s");
    let stage = crate::platform::owned_temp::OwnedTemp::new_in(state, ".s")?;
    std::fs::set_permissions(stage.outer(), std::fs::Permissions::from_mode(0o700))?;
    // .sXXXXXX/s (beside the marker) is no longer than agent.sock on Unix socket paths.
    let staged = stage.outer().join("s");
    let listener = crate::protocol::transport::listen_unix(&staged)?;
    let meta = std::fs::symlink_metadata(&staged)?;
    let receipt = SocketReceipt {
        cluster: cluster.clone(),
        worker: worker.clone(),
        dev: meta.dev(),
        ino: meta.ino(),
    };
    let receipt_path = state.join("worker-socket.json");
    if receipt_path.try_exists()? {
        resident::secure_file(&receipt_path, false)?;
    }
    let mut temporary = crate::platform::owned_temp::file_in(state)?;
    temporary.write_all(&serde_json::to_vec(&receipt)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(&receipt_path)?;
    File::open(state)?.sync_all()?;
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
    File::open(state)?.sync_all()?;
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

/// Shared by worker-only services and combined residents. The caller holds the
/// agent ownership lock before reconciling or publishing the socket.
pub(crate) struct AgentListener {
    listener: tokio::net::UnixListener,
    _socket: OwnedSocket,
}
impl AgentListener {
    pub(crate) fn bind(
        state: &Path,
        cluster: &ClusterId,
        worker: &WorkerId,
    ) -> anyhow::Result<Self> {
        let socket = state.join("agent.sock");
        reconcile_socket(state, cluster, worker, &socket)?;
        let (listener, owned) = publish_socket(state, cluster, worker, &socket)?;
        Ok(Self {
            listener,
            _socket: owned,
        })
    }
    pub(crate) async fn accept(
        &self,
        agent: Arc<Agent>,
        cluster: ClusterId,
        permits: Arc<Semaphore>,
    ) -> std::io::Result<impl std::future::Future<Output = anyhow::Result<()>>> {
        let (stream, _) = self.listener.accept().await?;
        Ok(async move {
            serve::serve_connection(
                agent,
                crate::protocol::Io::from_stream(stream),
                cluster,
                permits,
            )
            .await
            .map_err(Into::into)
        })
    }
}

struct Worker {
    agent: Arc<Agent>,
    _owner: crate::platform::flock::HeldLock,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.agent.shutdown();
    }
}

/// Wait for `f`; after a give-up only for a few seconds (`None` then).
async fn bounded<F: std::future::Future>(give_up: bool, f: F) -> Option<F::Output> {
    if give_up {
        tokio::time::timeout(Duration::from_secs(5), f).await.ok()
    } else {
        Some(f.await)
    }
}

/// An ephemeral worker told to end stops what runs on it the way an operator's
/// cancel does: no new placement, every live attempt cancelled on the agent's
/// ticks, the supervisors ending their commands (TERM, grace, KILL). A second
/// signal changes nothing.
fn begin_stop(
    state: &Path,
    ending: &mut Option<tokio::time::Instant>,
    bound: Duration,
) -> anyhow::Result<()> {
    if ending.is_some() {
        return Ok(());
    }
    crate::worker::control::write(state, crate::worker::control::Control::cancelling())?;
    eprintln!("ending: stopping the attempts that still run");
    let now = tokio::time::Instant::now();
    *ending = Some(
        now.checked_add(bound)
            .unwrap_or_else(|| now + Duration::from_secs(365 * 86_400)),
    );
    Ok(())
}

/// An ephemeral worker advertises its per-start identity to the coordinator.
/// Process recovery always compares against the actual OS boot.
async fn run(
    spec: WorkerSpec,
    config: config::ResolvedConfig,
    boot_id: Option<WorkerBootId>,
) -> anyhow::Result<()> {
    let ephemeral = boot_id.is_some();
    // Before anything is created or locked in it.
    if !ephemeral {
        ephemeral::refuse_marked(&spec.state_dir)?;
    }
    let owner = private_lock(&spec.state_dir, "worker-owner.lock")?;
    // Serialize enrollment against every Agent, including the combined resident.
    // Transfer the same descriptor into Agent; enrollment through DB open has no unlock gap.
    let agent_gate = private_lock(&spec.state_dir, "agent.lock")?.into_file();
    let socket = spec.state_dir.join("agent.sock");
    // Spec 25.4 (a): under `agent.lock`, what a killed process left beside the state files.
    crate::platform::owned_temp::sweep_files(&spec.state_dir);
    // Spec 25.7 (a): the versions a commit could not remove.
    super::old_binaries::sweep_at_start(&spec.env);
    // Enrollment is checked before any recorded old listener can be removed.
    bind(&spec)?;
    reconcile_socket(
        &spec.state_dir,
        &spec.expected_cluster,
        &spec.worker_id,
        &socket,
    )?;
    let os_boot = crate::platform::boot_id()
        .ok_or_else(|| anyhow::anyhow!("OS boot identity unavailable"))?;
    let settings = config.runtime_settings()?;
    // Commands of this worker that still run in another scope: wait for
    // them, not serving, instead of failing into a restart loop.
    if !super::traffic_hold::wait_until_clear(&spec.state_dir, &spec.env).await? {
        return Ok(());
    }
    let runtime = config
        .worker_runtime(WorkerInputs {
            state_dir: spec.state_dir.clone(),
            worker_id: spec.worker_id.clone(),
            boot_id: WorkerBootId::parse(&os_boot)?,
            supervisor_exe: spec.executable.clone(),
            cluster_key_path: spec.cluster_key_path.clone(),
            env: spec.env.clone(),
        })?
        .with_instance(boot_id);
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
    // An ephemeral worker also ends gracefully when its terminal goes away.
    let mut hangup = if ephemeral {
        Some(tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::hangup(),
        )?)
    } else {
        None
    };
    let stop_bound = Duration::from_millis(ephemeral::plan::stop_bound_ms(settings.term_grace_ms));
    // Set when an ephemeral worker was told to end: when its wait for the
    // supervisors stops.
    let mut ending: Option<tokio::time::Instant> = None;
    // The previous look after the stop began found nothing running.
    let mut quiet_before = false;
    let listener = AgentListener::bind(&spec.state_dir, &spec.expected_cluster, &spec.worker_id)?;
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
    let giving_up = || {
        anyhow::anyhow!(
            "the attempts did not stop within {} s",
            stop_bound.as_secs()
        )
    };
    let result: anyhow::Result<()> = loop {
        let deadline = ending;
        tokio::select! {
            _ = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            } => break Err(giving_up()),
            r = tokio::signal::ctrl_c() => {
                if !ephemeral { break r.map_err(Into::into); }
                if let Err(e) = r { break Err(e.into()); }
                if let Err(e) = begin_stop(&spec.state_dir, &mut ending, stop_bound) { break Err(e); }
            },
            _ = async {
                match hangup.as_mut() {
                    Some(h) => h.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Err(e) = begin_stop(&spec.state_dir, &mut ending, stop_bound) { break Err(e); }
            },
            done = &mut supervisor => {
                supervisor_done = true;
                break match done { Ok(result) => result, Err(e) => Err(e.into()) };
            },
            _ = terminate.recv() => {
                if !ephemeral { break Ok(()); }
                if let Err(e) = begin_stop(&spec.state_dir, &mut ending, stop_bound) { break Err(e); }
            },
            _ = ticker.tick(), if housekeeping.is_empty() => {
                let agent = worker.agent.clone();
                // A look started before the stop began says nothing about it.
                let after_begin = ending.is_some();
                housekeeping.spawn_blocking(move || {
                    agent.tick();
                    // An ephemeral worker whose identity a member retired
                    // ends itself once nothing runs on it and no other
                    // member has a result to collect (spec 24.3).
                    if ephemeral {
                        let (told, live, uncollected) = agent.retirement();
                        let (live, running) = if after_begin { agent.settling() } else { (live, None) };
                        (ephemeral::plan::finished(told, live, uncollected), after_begin, ephemeral::plan::quiet(live, running))
                    } else {
                        (false, false, false)
                    }
                });
            }
            Some(done) = housekeeping.join_next(), if !housekeeping.is_empty() => {
                match done {
                    Ok((true, _, _)) => {
                        eprintln!("this worker's identity was retired, nothing runs on it and nothing is left to collect: ending");
                        break Ok(());
                    }
                    Ok((false, after_begin, quiet)) => {
                        let Some(deadline) = ending else { continue };
                        let (step, q) = ephemeral::plan::stop_step(
                            after_begin,
                            quiet,
                            quiet_before,
                            tokio::time::Instant::now() >= deadline,
                        );
                        quiet_before = q;
                        match step {
                            ephemeral::plan::StopStep::Wait => {}
                            ephemeral::plan::StopStep::Settled => break Ok(()),
                            ephemeral::plan::StopStep::GiveUp => break Err(giving_up()),
                        }
                    }
                    Err(e) => break Err(e.into()),
                }
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            incoming = listener.accept(worker.agent.clone(), spec.expected_cluster.clone(), permits.clone()) => {
                match incoming { Ok(connection) => { connections.spawn(connection); }, Err(e) => break Err(e.into()) }
            }
        }
    };
    // After a give-up, a pass blocked in a tick must not hold the exit: the
    // state is kept and the process ends.
    let give_up = ending.is_some() && result.is_err();
    drop(listener);
    let _ = stop.send(true);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    // Accepted blocking requests finish before releasing the agent owner lock.
    let _drained = bounded(
        give_up,
        permits.acquire_many(serve::MAX_CONCURRENT_REQUESTS as u32),
    )
    .await
    .transpose()?;
    let supervised = if supervisor_done {
        Ok(())
    } else {
        bounded(give_up, &mut supervisor)
            .await
            .map_or(Ok(()), |done| {
                done.map_err(anyhow::Error::from).and_then(|done| done)
            })
    };
    let mut ticking_error = None;
    loop {
        let Some(done) = bounded(give_up, housekeeping.join_next()).await.flatten() else {
            break;
        };
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
