//! The worker agent: every worker-side operation of spec chapter 11 over the
//! worker database, and the life of an accepted attempt up to its supervisor.
//!
//! The agent never runs a user command. It persists the acceptance, prepares
//! the workspace, writes the supervisor's spec and starts `supervise` as a
//! detached process; from then on the supervisor owns the attempt and the
//! agent only watches. One thread per attempt (`drive`) does this, and the
//! same loop is what a restarted agent runs for the attempts it finds: a
//! living supervisor is left alone (adopted), a dead one is replaced by a
//! `supervise` that recovers and never spawns the command again.
//!
//! Everything here blocks (SQLite, files, child processes): [`super::serve`]
//! calls it through `spawn_blocking`.

use super::blob_policy::BlobLimits;
use super::blobs;
use super::host::Host;
use super::policy::{self, AttemptFacts, Recovery, Settle, Step, SupervisorExit};
use crate::error::{ApiError, ConflictReason, ErrorCode, OverloadedReason};
use crate::ids::*;
use crate::platform;
use crate::setup::builder::SetupPoll;
use crate::state::{
    AttemptState, Outcome, Receipt, Termination, TimeoutInfo, TimeoutKind, TimeoutPhase,
};
use crate::storage::{
    AcceptedAttempt, AcceptedLease, CancelOutcome, GcReport, GrantStamp, LeaseOutcome,
    NewAcceptance, ProcessLock, StorageError, SubmitOutcome, WorkerStore,
};
use crate::supervisor::lease::LeaseSeed;
use crate::supervisor::lock::SupervisorLock;
use crate::supervisor::outcome::drain_pending_deletes;
use crate::supervisor::process::spawn_detached;
use crate::supervisor::spec::{
    ArtifactFile, ArtifactsSpec, MaskValues, NodeSpec, SuperviseSpec, DEFAULT_ARTIFACT_BYTES,
    DEFAULT_LOG_LIMIT_BYTES,
};
use crate::supervisor::spool::{SpoolError, SpoolReader};
use crate::supervisor::spool_plan::result_not_stored;
use crate::supervisor::termination::{termination, StopReason};
use crate::wire::*;
use crate::workspace::plan::check_cwd_relative;
use crate::workspace::Workspace;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::Child;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

mod environment;
mod warmup;

/// What an agent is started with. Nothing is read from the process
/// environment: the boot ID and the supervisor binary are inputs.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Present only in a combined resident; read without a self-session.
    pub coordinator_state: Option<PathBuf>,
    /// `worker.db`, the attempts and the locks live here (mode 0700).
    pub state_dir: PathBuf,
    pub worker_id: WorkerId,
    /// This boot of the machine (`platform::boot_id()` in production).
    pub boot_id: WorkerBootId,
    /// Per-start identity advertised by an ephemeral worker.
    pub instance_id: Option<WorkerBootId>,
    /// The binary whose `supervise --spec <file>` runs an attempt.
    pub supervisor_exe: PathBuf,
    pub traffic_state_dir: PathBuf,
    pub traffic_config: Option<PathBuf>,
    pub capability_profile: Option<Arc<crate::toolchain::profile::Profile>>,
    pub profile: String,
    pub profile_fingerprint: Digest,
    pub max_active_task: u32,
    pub prefetch_per_worker: u32,
    /// How long a forecast answer stays usable (`scheduler.forecast_ttl_ms`,
    /// spec default 2000). It is the `ttl_ms` the answer carries.
    pub forecast_ttl_ms: u64,
    pub term_grace_ms: u64,
    /// Maximum bytes retained per supervisor log spool.
    pub log_limit_bytes: u64,
    pub kill_wait_ms: u64,
    /// How often the supervisors and the agent's own loops look again.
    pub poll_ms: u64,
    pub inhibit_sleep: bool,
    /// How long `retire_attempt` waits for the stop before it answers
    /// `unconfirmed` (the coordinator asks again).
    pub retire_wait_ms: u64,
    /// How long a prepared attempt waits for its secret bundle.
    pub secret_wait_ms: u64,
    /// Pause before a supervisor that could not settle is started again.
    pub retry_ms: u64,
    /// How long a start waits for the agent lock. A process that was forked
    /// by the previous agent holds the lock's descriptor until its `exec`,
    /// so the lock of an agent that just went away can look held for a
    /// moment; a living agent holds it for good and the start is refused.
    pub lock_wait_ms: u64,
    /// Files of ended attempts are kept this long (spec 13: seven days).
    pub retention_ms: u64,
    /// Without `ack_collected`, the protection ends this long after the end.
    pub ack_wait_ms: u64,
    /// Attempts that are not terminal, at most. A new attempt beyond that is
    /// refused with `OVERLOADED`; nothing is stored for it.
    pub max_live_attempts: u32,
    /// Workspaces prepared at once. The other attempts wait their turn.
    pub max_concurrent_prepares: u32,
    /// How long the preparation of one workspace may take before the attempt
    /// fails (the default of the workspace tool: 15 minutes).
    pub prepare_timeout_ms: u64,
    /// How long a warm-up (`env_sync`) nobody asks about keeps its reference
    /// to an install.
    pub warmup_idle_ms: u64,
    /// Supervisor starts that may fail in a row before the agent gives up.
    pub max_spawn_failures: u32,
    /// Attempts in one `list_attempts` answer; more is an error.
    pub max_list_attempts: u32,
    /// Journal entries of one attempt a `watch_events` reads; more is an
    /// error.
    pub max_event_scan: u32,
    /// What a coordinator may send with `blob_put`.
    pub blobs: BlobLimits,
    /// The group key: the file of the current key. The next key of a
    /// rotation lies beside it ([`crate::group::next_key_path`]).
    pub group_key_path: PathBuf,
}

impl AgentConfig {
    pub(crate) fn advertised_boot(&self) -> &WorkerBootId {
        self.instance_id.as_ref().unwrap_or(&self.boot_id)
    }

    pub fn new(
        state_dir: PathBuf,
        worker_id: WorkerId,
        boot_id: WorkerBootId,
        supervisor_exe: PathBuf,
    ) -> Self {
        AgentConfig {
            coordinator_state: None,
            group_key_path: state_dir.join("cluster.key"),
            traffic_state_dir: state_dir.join("traffic"),
            state_dir,
            worker_id,
            boot_id,
            instance_id: None,
            supervisor_exe,
            traffic_config: None,
            capability_profile: None,
            profile: "default".into(),
            profile_fingerprint: Digest::of_bytes(b""),
            max_active_task: 1,
            prefetch_per_worker: 1,
            forecast_ttl_ms: 2000,
            log_limit_bytes: DEFAULT_LOG_LIMIT_BYTES,
            term_grace_ms: crate::supervisor::spec::DEFAULT_TERM_GRACE_MS,
            kill_wait_ms: 5_000,
            poll_ms: 200,
            inhibit_sleep: true,
            retire_wait_ms: 2_000,
            secret_wait_ms: 60_000,
            retry_ms: 5_000,
            lock_wait_ms: 2_000,
            retention_ms: crate::storage::DEFAULT_ACK_WAIT_MS,
            ack_wait_ms: crate::storage::DEFAULT_ACK_WAIT_MS,
            max_live_attempts: policy::DEFAULT_MAX_LIVE_ATTEMPTS,
            max_concurrent_prepares: policy::DEFAULT_MAX_PREPARES,
            prepare_timeout_ms: crate::workspace::DEFAULT_PREPARE_TIMEOUT.as_millis() as u64,
            warmup_idle_ms: 60_000,
            max_spawn_failures: policy::DEFAULT_MAX_SPAWN_FAILURES,
            max_list_attempts: policy::MAX_LIST_ATTEMPTS,
            max_event_scan: policy::MAX_EVENT_SCAN,
            blobs: BlobLimits::default(),
        }
    }

    /// Where each attempt keeps its files: `<root>/<attempt>/` holds the
    /// supervisor spec, the workspace record, `logs/` and `artifacts/`.
    pub fn attempts_root(&self) -> PathBuf {
        self.state_dir.join("attempts")
    }

    /// The agent's Unix socket.
    pub fn socket_path(&self) -> PathBuf {
        self.state_dir.join("agent.sock")
    }
}

/// Where a request came from, for the rules of stage 3 (spec 25.6): whether
/// its connection agreed `attempt_release_v1`, and where it was taken from
/// the connection's queue. The default is a call that is not over a
/// connection and did not agree the feature (today's behaviour).
#[derive(Debug, Clone, Copy, Default)]
pub struct Link {
    pub release: bool,
    /// The connection agreed `input_missing_v1` (spec 25.8 5b (b)).
    pub input_missing: bool,
    pub taken: Option<super::release::Taken>,
}

/// An `INPUT_MISSING` answer for a peer that did not agree
/// `input_missing_v1` (it cannot read the code) becomes `INTERNAL`, which it
/// takes for a fault of this host: it delivers again, and nothing it sent was
/// thrown away (spec 25.8 5b (b), old peers).
fn unless_agreed(e: ApiError, agreed: bool) -> ApiError {
    if agreed || e.code != ErrorCode::InputMissing {
        return e;
    }
    ApiError::new(ErrorCode::Internal, e.message)
}

/// What the connection's `hello` and group proof established: the member
/// at the other end. Every request of the connection is that member's, and
/// reaches that member's attempts only (spec 24.4).
#[derive(Debug, Clone)]
pub struct Session {
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
}

/// What a startup found and did, for the log and the tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupReport {
    pub steps: Vec<Step>,
    pub boot_changed: bool,
    /// Attempts fixed as interrupted or cancelled without a supervisor.
    pub settled: Vec<AttemptId>,
    /// Attempts handed to a watcher thread (adopt, recover or resume).
    pub watched: Vec<AttemptId>,
    /// Attempts that may have run and cannot be recovered.
    pub unresolved: Vec<AttemptId>,
}

pub struct Agent {
    coordinator: Mutex<std::sync::Weak<crate::coordinator::Coordinator>>,
    coordinator_reader: Mutex<Option<crate::storage::CoordinatorStore>>,
    identity: Mutex<WorkerBootId>,
    rekey_gate: std::sync::RwLock<()>,
    /// The fingerprint of the group key this worker signs with, as last
    /// read from its file. A connection watches it: one whose peer proved
    /// with a key this worker no longer accepts is closed.
    group_key: tokio::sync::watch::Sender<String>,
    toolchains: Option<crate::toolchain::registry::Registry>,
    cfg: AgentConfig,
    store: WorkerStore,
    host: Arc<dyn Host>,
    epoch: AgentEpoch,
    lock: Mutex<Option<ProcessLock>>,
    driving: Mutex<BTreeSet<String>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    stopping: AtomicBool,
    report: StartupReport,
    /// Workspaces being prepared.
    prepares: Arc<Permits>,
    /// One lock per attempt (by hash): the cleanup of an attempt and the
    /// placement of its secrets never run at once, from whichever thread.
    shards: Arc<Vec<Mutex<()>>>,
    /// When this process accepted an attempt and last extended its lease.
    clocks: Mutex<BTreeMap<String, Clock>>,
    /// A renewal (its commit and the new grant time) and the measuring of a
    /// lease before a start (the applied key, the grant time, the expiry it
    /// may write) never interleave: whoever measures sees a key together
    /// with the time of that key's grant, and no renewal lands between the
    /// finding that a lease ran out and the record of it. Not the attempt's
    /// shard: that one is held across a cleanup, which may take long.
    lease_step: Mutex<()>,
    /// See [`Agent::set_probe`].
    probe: Mutex<Option<Probe>>,
    /// Supervisors this agent started that had not exited when their watcher
    /// let go of them: waited for later, so none stays a zombie.
    children: Mutex<Vec<Child>>,
    /// Attempts the agent stopped trying to start or recover.
    gave_up: Mutex<BTreeSet<String>>,
    /// The last diagnostic written per subject, so a repeating failure is
    /// one event.
    noted: Mutex<BTreeMap<String, String>>,
    /// One new acceptance at a time: the live cap and the insert are one step.
    submitting: Mutex<()>,
    /// The released dedup records and the submits that may be behind them
    /// (spec 25.6 (c)).
    releases: super::release::Releases,
    /// Released records a submit's handling let go of where no database
    /// write may run (a drop on the async runtime): forgotten by the tick.
    ripe_later: Mutex<super::release::Ripe>,
    /// Where the retry of unfinished releases goes on: entries that keep
    /// failing never stop the ones after them (it wraps at the end).
    dropped_cursor: Mutex<String>,
    /// Entries of `dropped_attempt` that keep failing: the pass after which
    /// each is tried again and its current delay in passes (doubling, up to
    /// [`DROPPED_BACKOFF_MAX`]). In memory only: a start tries all again.
    dropped_backoff: Mutex<std::collections::HashMap<String, (u64, u64)>>,
    passes: std::sync::atomic::AtomicU64,
    /// The stored uptime sample and the newest event mark, once read.
    uptime: Mutex<Option<(Option<crate::storage::Uptime>, Option<u64>)>>,
    /// When this agent started (wall clock): the rows acknowledged before it
    /// are an older version's (spec 25.6 (c2)).
    started_ms: u64,
    /// This agent accepted an acknowledgement of a parent without
    /// `attempt_release_v1`: the tick no longer continues (c2).
    old_acks: AtomicBool,
    ticking: AtomicBool,
    /// A deleting pass of the received sources runs on its own thread
    /// (spec 25.8 5b (h)6): at most one, apart from `ticking` (which the
    /// tick drops when it returns).
    deleting: AtomicBool,
    /// When the last round of those passes started: at most one a minute.
    deleted_at: Mutex<Option<Instant>>,
    /// Warm-ups in progress or just answered, by `sync_id`.
    warmups: Mutex<BTreeMap<String, warmup::Warmup>>,
    /// Directory enumeration continues across bounded terminal cleanup passes.
    cleanup_scan: Mutex<Option<fs::ReadDir>>,
}

/// A point in the agent's work a test may stop at. See [`Agent::set_probe`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbePoint {
    /// A start is about to measure the lease of the attempt.
    LeaseMeasure,
    /// A renewal is committed; its grant time is not recorded yet.
    LeaseRenewed,
}

#[doc(hidden)]
pub type Probe = Arc<dyn Fn(ProbePoint, &AttemptId) + Send + Sync>;

/// Monotonic times (suspend included) of this process.
#[derive(Debug, Clone, Copy)]
struct Clock {
    accepted_ms: u64,
    granted_ms: u64,
}

struct Permits {
    used: Mutex<u32>,
    max: u32,
}

struct Permit(Arc<Permits>);

impl Permits {
    fn try_acquire(p: &Arc<Permits>) -> Option<Permit> {
        let mut used = lock(&p.used);
        if *used >= p.max {
            return None;
        }
        *used += 1;
        Some(Permit(p.clone()))
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut used = lock(&self.0.used);
        *used = used.saturating_sub(1);
    }
}

/// What a preparation running on its own thread left for its waiter.
enum Slot {
    Waiting,
    Done(Result<Workspace, ApiError>),
    /// The waiter gave up: a workspace that still appears is removed.
    Abandoned,
}

/// Takes the attempt out of `driving` when its watcher ends, however it ends.
struct Driving {
    agent: Arc<Agent>,
    attempt: AttemptId,
}

impl Drop for Driving {
    fn drop(&mut self) {
        lock(&self.agent.driving).remove(self.attempt.as_str());
    }
}

/// At most one round of deleting passes of the received sources a minute
/// (spec 25.8 5b (d)), and the pause between the passes of a round.
const RECEIVED_PASS_EVERY: Duration = Duration::from_secs(60);
const RECEIVED_PASS_PAUSE: Duration = Duration::from_millis(100);
/// How often a wait for the capability measurement looks at `stopping`.
const STOP_LOOK: Duration = Duration::from_millis(10);
/// The measurement was not waited for: the agent stops.
const AGENT_STOPPING: &str = "the agent is stopping";

struct Ticking<'a>(&'a AtomicBool);

impl Drop for Ticking<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

const SHARDS: usize = 64;

/// A panic elsewhere must not take the agent's bookkeeping with it: the data
/// behind these locks stays consistent under every partial update.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn shard<'a>(shards: &'a [Mutex<()>], attempt: &AttemptId) -> &'a Mutex<()> {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    attempt.as_str().hash(&mut h);
    &shards[(h.finish() % shards.len() as u64) as usize]
}

fn now() -> u64 {
    platform::wall_ms()
}

/// `text` on one line, cut at [`crate::wire::FAILURE_REASON_MAX`] characters.
pub(crate) fn bounded_reason(text: &str) -> String {
    let one: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let max = crate::wire::FAILURE_REASON_MAX;
    if one.chars().count() <= max {
        return one;
    }
    let cut: String = one.chars().take(max - 3).collect();
    format!("{cut}...")
}

fn failed() -> Termination {
    Termination::new(Outcome::Failed, None, None).expect("failed without an exit is valid")
}

fn api(e: StorageError) -> ApiError {
    // The one conflict a sender must not answer by sending again.
    if matches!(e, StorageError::PayloadConflict(_)) {
        return ApiError::conflict(ConflictReason::PayloadMismatch, e.to_string());
    }
    // Nothing was stored, and sending the same attempt again does not help:
    // the reason tells the member to use a new attempt id.
    if matches!(e, StorageError::AttemptIdInUse(_)) {
        return ApiError::overloaded(OverloadedReason::AttemptIdInUse, e.to_string());
    }
    // Nothing was stored, no tombstone either: the window of this worker is
    // full with the attempts of every member.
    if matches!(e, StorageError::Capacity { .. }) {
        return ApiError::overloaded(OverloadedReason::Capacity, e.to_string());
    }
    // Spec 25.6 (c): the member holds its bound of dedup records here.
    if matches!(e, StorageError::AttemptRecords(_)) {
        return ApiError::overloaded(OverloadedReason::AttemptRecords, e.to_string());
    }
    // A busy database committed nothing: the same request may be sent again.
    let code = if e.is_retryable() {
        ErrorCode::Overloaded
    } else {
        e.code()
    };
    ApiError::new(code, e.to_string())
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

/// The frozen selections of one bundle, split by how this worker serves them.
#[derive(Default)]
struct ProvidedToolchains {
    requests: Vec<crate::toolchain::preparation::PreparationRequest>,
    native: Vec<crate::toolchain::contract::NativeTool>,
}
/// The refusal of an activation: its code, or, when a managed install would
/// run in place of a native tool, the two tools by name.
fn activation_refused(
    code: &'static str,
    native: &[crate::toolchain::contract::NativeTool],
    plans: &[crate::toolchain::adapters::InstallPlan],
) -> ApiError {
    let shadowed = (code == "activation_native_mismatch")
        .then(|| {
            crate::toolchain::adapters::shadowed_native(native, &plans.iter().collect::<Vec<_>>())
        })
        .flatten();
    ApiError::blocked(
        crate::error::BlockedReason::EnvironmentMismatch,
        match shadowed {
            Some(shadowed) => format!("{code}: {shadowed}"),
            None => code.into(),
        },
    )
}
/// The identity of the profile shell's real file. A profile may name its shell
/// through a symlink (`/bin/sh` is one on many Linux hosts) and already
/// measures the resolved file for its fingerprint, so a retargeted link is
/// profile drift. The identity is taken from that resolved path, without
/// following a link again.
fn shell_identity(
    profile: &crate::toolchain::profile::Profile,
) -> Result<crate::toolchain::adapters::FileIdentity, ApiError> {
    let real = fs::canonicalize(profile.shell()).map_err(|_| invalid("identity_open"))?;
    crate::toolchain::adapters::FileIdentity::capture(&real).map_err(invalid)
}

/// Refuse a payload that declares one of the attempt's secret files (the
/// declared ones and what `.worktreeinclude` matched) as an artifact.
fn check_artifacts_not_secret(
    payload: &AttemptPayload,
    secrets: &[String],
) -> Result<(), ApiError> {
    if policy::artifact_is_secret(&payload.artifacts, secrets) {
        return Err(invalid(
            "a declared artifact is one of the attempt's secret files",
        ));
    }
    Ok(())
}

/// The file is this user's alone (0600): the supervisor refuses a spec that
/// anybody else could read or that somebody else wrote.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().expect("a file has a parent");
    let mut tmp = crate::platform::owned_temp::file_in(dir)?;
    tmp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// `<dir>` exists, is a directory of this user's, and nobody else can enter.
fn private_dir(dir: &Path) -> std::io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
}

/// What an ended attempt answers with: its termination, the stored and the
/// unstored artifacts, the digest of the result, and whether a result is
/// stored (otherwise the digest is of the termination alone).
type Ended = (
    Termination,
    Vec<ArtifactRecord>,
    Vec<ArtifactNotStored>,
    Digest,
    bool,
);

/// A validated agent.lock descriptor bound to the configuration it protects.
/// Fields are private: callers cannot substitute another path/configuration at start.
pub(crate) struct OwnedAgentLock {
    config: AgentConfig,
    lock: ProcessLock,
}
impl OwnedAgentLock {
    pub(crate) fn new(config: AgentConfig, file: std::fs::File) -> Result<Self, ApiError> {
        let lock =
            ProcessLock::adopt_private(file, config.state_dir.join("agent.lock")).map_err(api)?;
        Ok(Self { config, lock })
    }
}

impl Agent {
    pub(crate) fn set_coordinator(&self, coordinator: &Arc<crate::coordinator::Coordinator>) {
        if let Ok(mut slot) = self.coordinator.lock() {
            *slot = Arc::downgrade(coordinator);
        }
    }

    fn own_runs_active(&self) -> Option<bool> {
        if let Some(coordinator) = self.coordinator.lock().ok()?.upgrade() {
            return coordinator.store().has_nonterminal_runs().ok();
        }
        let Some(state) = &self.cfg.coordinator_state else {
            return Some(false);
        };
        let mut reader = self.coordinator_reader.lock().ok()?;
        if reader.is_none() {
            *reader = Some(crate::storage::CoordinatorStore::open_read_only(state).ok()?);
        }
        reader.as_ref()?.has_nonterminal_runs().ok()
    }

    /// Run the startup steps in [`policy::STARTUP_ORDER`] and return the
    /// running agent. Any failing step refuses the start.
    pub fn start(cfg: AgentConfig, host: Arc<dyn Host>) -> Result<Arc<Agent>, ApiError> {
        Self::start_inner(cfg, host, None)
    }

    pub(crate) fn start_owned(
        owner: OwnedAgentLock,
        host: Arc<dyn Host>,
    ) -> Result<Arc<Agent>, ApiError> {
        Self::start_inner(owner.config, host, Some(owner.lock))
    }

    fn start_inner(
        cfg: AgentConfig,
        host: Arc<dyn Host>,
        mut lock: Option<ProcessLock>,
    ) -> Result<Arc<Agent>, ApiError> {
        let mut opened: Option<(WorkerStore, AgentEpoch)> = None;
        let mut report = StartupReport::default();
        let mut todo: Vec<AttemptId> = Vec::new();
        let boot_file = cfg.state_dir.join("boot_id");
        for step in policy::STARTUP_ORDER {
            let store = opened.as_ref().map(|o| &o.0);
            match step {
                Step::StateDir => {
                    private_dir(&cfg.state_dir).map_err(|e| internal("state directory", e))?;
                    private_dir(&cfg.attempts_root())
                        .map_err(|e| internal("attempts directory", e))?;
                }
                Step::CoreDumps => host.disable_core_dumps()?,
                Step::Lock => {
                    if lock.is_none() {
                        lock = Some(acquire_lock(&cfg)?);
                    }
                }
                Step::Store => {
                    let s = WorkerStore::open(&cfg.state_dir, &cfg.worker_id).map_err(api)?;
                    let e = s.start_agent(now()).map_err(api)?;
                    let recorded = match fs::read_to_string(&boot_file) {
                        Ok(t) => Some(t),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                        Err(e) => return Err(internal("boot id record", e)),
                    };
                    let has_history = s.count_nonterminal().map_err(api)? > 0
                        || s.count_launches().map_err(api)? > 0;
                    report.boot_changed = policy::boot_changed(
                        recorded.as_deref(),
                        cfg.boot_id.as_str(),
                        has_history,
                    );
                    opened = Some((s, e));
                }
                Step::StoreSecretCopies => host.sweep_store_copies()?,
                Step::IncomingImports => {
                    // Not while a version is on trial (spec 25.1, 8). A
                    // failure keeps the rows and does not stop the start
                    // (25.11): the next start tries again.
                    if let Some(blobs) = host
                        .blobs()
                        .filter(|_| !super::control::in_trial(&cfg.state_dir))
                    {
                        if let Err(e) = blobs.forget_unowned() {
                            let s = store.expect("the store is open");
                            let payload = json!({ "code": e.code, "message": e.message });
                            let _ = s.append_event(
                                None,
                                None,
                                "agent_incoming_tokens_kept",
                                &payload,
                                now(),
                            );
                        }
                    }
                }
                Step::Vault => host.sweep_vault()?,
                Step::PlacedSecrets => {
                    let s = store.expect("the store is open");
                    // Unknown or unreadable counts as ended: fail closed.
                    host.reconcile_secrets(
                        &|a| !matches!(s.get(a), Ok(Some(r)) if r.state != AttemptState::Terminal),
                    )?
                }
                Step::Workspaces => {
                    let s = store.expect("the store is open");
                    // An unreadable record keeps the workspace.
                    host.sweep_workspaces(&|a| {
                        !matches!(s.get(a), Ok(None))
                            && !matches!(s.get(a), Ok(Some(r)) if r.state == AttemptState::Terminal)
                    })?;
                    // After the workspaces: what the rebuild of the database
                    // dropped has no row that would clean its directory up.
                    let dropped = remove_dropped_attempts(&cfg.attempts_root(), s).map_err(api)?;
                    // Then the lock files no attempt owns any more (spec
                    // 25.6 (b), 25.11): an older `hold` made one again.
                    if let Err(e) = sweep_supervisor_locks(&cfg.state_dir, s) {
                        let payload = json!({ "error": e.to_string() });
                        let _ = s.append_event(
                            None,
                            None,
                            "agent_supervisor_locks_kept",
                            &payload,
                            now(),
                        );
                    }
                    for (attempt, error) in &dropped.failed {
                        let payload = json!({ "attempt": attempt, "error": error });
                        let _ = s.append_event(
                            None,
                            None,
                            "agent_dropped_attempt_kept",
                            &payload,
                            now(),
                        );
                    }
                }
                Step::Attempts => {
                    let s = store.expect("the store is open");
                    for rec in s.list_nonterminal().map_err(api)? {
                        let a = rec.attempt_id.clone();
                        match recovery_of(&cfg, s, &rec, report.boot_changed)? {
                            Recovery::Settle(how) => {
                                if settle(&cfg, s, &a, how_termination(how), how, false)? {
                                    report.settled.push(a.clone());
                                }
                                // Settled or held by a living supervisor:
                                // the watcher cleans up or adopts.
                                todo.push(a);
                            }
                            Recovery::Unresolved => {
                                note(s, &rec, "agent_unresolved", json!({"why": "no spec"}));
                                if report.boot_changed {
                                    // Kept per attempt: the next start no
                                    // longer sees the change.
                                    note(s, &rec, "agent_boot_changed", json!({}));
                                }
                                report.unresolved.push(a);
                            }
                            _ => todo.push(a),
                        }
                    }
                }
                Step::BootRecord => write_atomic(&boot_file, cfg.boot_id.as_str().as_bytes())
                    .map_err(|e| internal("boot id record", e))?,
            }
            report.steps.push(step);
        }
        let (store, epoch) = opened.expect("the store step ran");
        report.watched = todo.clone();
        // Spec 25.6 (c), (c2): before any connection is accepted. No submit
        // is in flight, so every released record may go; the rows an older
        // version acknowledged go as an acknowledgement goes now. Not on
        // trial (a rollback restores the database, not the directories).
        let started_ms = now();
        if !super::control::in_trial(&cfg.state_dir) {
            while store
                .purge_released_records(crate::storage::RELEASE_BATCH)
                .map_err(api)?
                == crate::storage::RELEASE_BATCH as usize
            {}
            for _ in 0..STARTUP_ACKED_BATCHES {
                let dropped = store
                    .drop_acked_before(started_ms, crate::storage::RELEASE_BATCH)
                    .map_err(api)?;
                if dropped.len() < crate::storage::RELEASE_BATCH as usize {
                    break;
                }
            }
            let dropped = remove_dropped_attempts(&cfg.attempts_root(), &store).map_err(api)?;
            for (attempt, error) in &dropped.failed {
                let payload = json!({ "attempt": attempt, "error": error });
                let _ =
                    store.append_event(None, None, "agent_dropped_attempt_kept", &payload, now());
            }
        }
        let max_prepares = cfg.max_concurrent_prepares.max(1);
        let toolchains = cfg
            .capability_profile
            .as_ref()
            .map(|profile| host.toolchain_registry(profile, cfg.profile_fingerprint.clone()))
            .transpose()?
            .flatten();
        let agent = Arc::new(Agent {
            coordinator: Mutex::default(),
            coordinator_reader: Mutex::default(),
            identity: Mutex::new(if cfg.instance_id.is_some() {
                cfg.advertised_boot().clone()
            } else {
                super::rekey::identity(&cfg.state_dir, &cfg.boot_id)
                    .map_err(|e| invalid(e.to_string()))?
            }),
            rekey_gate: std::sync::RwLock::new(()),
            group_key: tokio::sync::watch::channel(String::new()).0,
            toolchains,
            cfg,
            store,
            host,
            epoch,
            lock: Mutex::new(lock),
            driving: Mutex::default(),
            threads: Mutex::default(),
            stopping: AtomicBool::new(false),
            report,
            prepares: Arc::new(Permits {
                used: Mutex::new(0),
                max: max_prepares,
            }),
            shards: Arc::new((0..SHARDS).map(|_| Mutex::new(())).collect()),
            clocks: Mutex::default(),
            lease_step: Mutex::new(()),
            probe: Mutex::new(None),
            children: Mutex::default(),
            gave_up: Mutex::default(),
            noted: Mutex::default(),
            submitting: Mutex::default(),
            releases: super::release::Releases::default(),
            ripe_later: Mutex::default(),
            dropped_cursor: Mutex::default(),
            dropped_backoff: Mutex::default(),
            passes: std::sync::atomic::AtomicU64::new(0),
            uptime: Mutex::default(),
            started_ms,
            old_acks: AtomicBool::new(false),
            ticking: AtomicBool::new(false),
            deleting: AtomicBool::new(false),
            deleted_at: Mutex::default(),
            warmups: Mutex::default(),
            cleanup_scan: Mutex::new(None),
        });
        // The key this worker holds is recorded as a generation (the single
        // key of before the group is generation 1).
        agent.refresh_group_key();
        agent.apply_rekey();
        for a in todo {
            agent.kick(&a);
        }
        // A crash during send-key must finish the handoff before hello can
        // expose either identity. The same normal supervisor path stops work.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while super::rekey::pending(&agent.cfg.state_dir) {
            agent.apply_rekey();
            if std::time::Instant::now() >= deadline {
                agent.shutdown();
                return Err(invalid(
                    "pending send-key has not finished stopping old attempts",
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        // Attempts that ended while no agent ran still hold a workspace.
        agent.cleanup_terminal(32);
        Ok(agent)
    }

    pub fn config(&self) -> &AgentConfig {
        &self.cfg
    }

    pub fn store(&self) -> &WorkerStore {
        &self.store
    }

    pub fn agent_epoch(&self) -> AgentEpoch {
        self.epoch
    }

    pub fn startup_report(&self) -> &StartupReport {
        &self.report
    }

    /// Stop watching and release the agent lock. Supervisors keep running:
    /// they do not depend on the agent (spec chapter 12).
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let threads = std::mem::take(&mut *lock(&self.threads));
        for t in threads {
            let _ = t.join();
        }
        // The uptime since the last stored sample is not lost at a stop.
        self.uptime_ms(true);
        lock(&self.lock).take();
    }

    /// For the tests: `probe` is called at each [`ProbePoint`] the agent
    /// passes, on the thread that passes it, and that thread goes on when it
    /// returns. This is how a test puts a renewal and a start in an order.
    #[doc(hidden)]
    pub fn set_probe(&self, probe: Probe) {
        *lock(&self.probe) = Some(probe);
    }

    fn at(&self, point: ProbePoint, attempt: &AttemptId) {
        let probe = lock(&self.probe).clone();
        if let Some(probe) = probe {
            probe(point, attempt);
        }
    }

    pub(crate) fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn pause(&self, ms: u64) {
        let step = self.cfg.poll_ms.clamp(1, 100);
        let mut left = ms;
        while left > 0 && !self.stopping() {
            let d = left.min(step);
            std::thread::sleep(Duration::from_millis(d));
            left -= d;
        }
    }

    /// [`Self::pause`], ended early when the attempt is cancelled or retired
    /// meanwhile (or is gone): that is acted on at the next look, not after
    /// the wait.
    fn pause_unless_news(&self, rec: &AcceptedAttempt, ms: u64) {
        let step = self.cfg.poll_ms.max(1);
        let mut left = ms;
        while left > 0 && !self.stopping() {
            let d = left.min(step);
            self.pause(d);
            left -= d;
            let news = match self.store.get(&rec.attempt_id) {
                Ok(Some(now)) => policy::wakes(
                    rec.cancel_requested_at_ms.is_some(),
                    now.cancel_requested_at_ms.is_some(),
                    rec.retired,
                    now.retired,
                ),
                Ok(None) => true,
                Err(_) => false,
            };
            if news {
                break;
            }
        }
    }

    // ------------------------------------------------------------ attempts

    /// Make sure a thread watches `attempt`. Idempotent.
    pub fn kick(self: &Arc<Self>, attempt: &AttemptId) {
        self.reap();
        if self.stopping() || lock(&self.gave_up).contains(attempt.as_str()) {
            return;
        }
        if !lock(&self.driving).insert(attempt.to_string()) {
            return;
        }
        // Made before the thread exists: a thread that cannot be started
        // drops the guard with its closure, a panicking one while unwinding.
        let guard = Driving {
            agent: self.clone(),
            attempt: attempt.clone(),
        };
        let spawned = std::thread::Builder::new()
            .name("attempt".into())
            .spawn(move || {
                let (me, a) = (&guard.agent, &guard.attempt);
                if catch_unwind(AssertUnwindSafe(|| me.drive(a))).is_err() {
                    me.drive_panicked(a);
                }
            });
        match spawned {
            Ok(t) => {
                let mut threads = lock(&self.threads);
                threads.retain(|t| !t.is_finished());
                threads.push(t);
            }
            // The next tick tries again.
            Err(e) => self.note_once(
                attempt,
                "agent_thread_failed",
                json!({ "error": e.to_string() }),
            ),
        }
    }

    /// The watcher of `attempt` panicked. Nothing may be left half-watched
    /// for ever: the attempt is failed when no command was started, and
    /// reported as unknown otherwise.
    fn drive_panicked(&self, attempt: &AttemptId) {
        let Ok(Some(rec)) = self.store.get(attempt) else {
            return;
        };
        note(&self.store, &rec, "agent_drive_panicked", json!({}));
        if rec.state != AttemptState::Terminal {
            self.give_up(&rec, "the watcher panicked", failed());
        }
    }

    /// Stop trying to start or recover `rec`. With no launch intent the
    /// result is fixed here as `t`; an attempt whose command may have run is
    /// left as it is, no longer watched, and reported as unknown (a retire
    /// settles it once the machine has rebooted).
    fn give_up(&self, rec: &AcceptedAttempt, why: &str, t: Termination) {
        let a = &rec.attempt_id;
        let settled = settle(&self.cfg, &self.store, a, t, Settle::Interrupted, false);
        let settled = matches!(settled, Ok(true));
        note(
            &self.store,
            rec,
            "agent_gave_up",
            json!({ "why": why, "settled": settled }),
        );
        if settled {
            self.clean(a);
        } else {
            lock(&self.gave_up).insert(a.to_string());
        }
    }

    fn drive(&self, attempt: &AttemptId) {
        let mut child: Option<Child> = None;
        let mut noted_exit = false;
        // Supervisor starts and supervisors that failed, in a row.
        let mut failures = 0u32;
        // Supervisors, in a row, that could not tell whose a process is.
        let mut unknowns = 0u32;
        let mut settle_errors = 0u32;
        while !self.stopping() {
            let rec = match self.store.get(attempt) {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
            };
            if rec.state == AttemptState::Terminal {
                // The supervisor is about to exit; it is waited for later.
                self.park(child.take());
                self.clean(attempt);
                return;
            }
            // A supervisor we started and that still runs owns the attempt.
            if let Some(c) = child.as_mut() {
                match c.try_wait() {
                    Ok(None) => {
                        self.pause(self.cfg.poll_ms);
                        continue;
                    }
                    Ok(Some(status)) => {
                        let code = status.code();
                        child = None;
                        let exit = policy::supervisor_exit(code);
                        if exit == SupervisorExit::IdentityUnknown {
                            // Nothing was signalled and nothing is known. Not
                            // a failure: the attempt is never given up for
                            // it. It is looked at again, later each time.
                            let wait = policy::backoff_ms(self.cfg.retry_ms, unknowns);
                            if unknowns == 0 || policy::backoff_capped(unknowns) {
                                note(
                                    &self.store,
                                    &rec,
                                    "agent_identity_unknown",
                                    json!({
                                        "code": code,
                                        "retry_ms": wait,
                                        "longest": policy::backoff_capped(unknowns),
                                    }),
                                );
                            }
                            unknowns = unknowns.saturating_add(1);
                            self.pause_unless_news(&rec, wait);
                        } else if exit == SupervisorExit::Failed {
                            if !noted_exit {
                                noted_exit = true;
                                note(
                                    &self.store,
                                    &rec,
                                    "agent_supervisor_exit",
                                    json!({ "code": code }),
                                );
                            }
                            failures += 1;
                            if policy::gives_up(failures, self.cfg.max_spawn_failures) {
                                self.give_up(
                                    &rec,
                                    "the supervisor kept failing",
                                    termination(None, None),
                                );
                                return;
                            }
                            self.pause(self.cfg.retry_ms);
                        }
                        continue;
                    }
                    Err(_) => {
                        child = None;
                    }
                }
            }
            // The lock tells whether any supervisor (ours or an adopted one)
            // lives. Only this thread starts supervisors for the attempt, so
            // taking it for a moment cannot make a new one give up. (The
            // acknowledgement of an attempt that ended without a result
            // takes it too, for an attempt that is terminal already.)
            match SupervisorLock::try_acquire_existing(&self.cfg.state_dir, attempt) {
                Ok(Some(lock)) => drop(lock),
                Ok(None) | Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
            }
            let started = match recovery_of(&self.cfg, &self.store, &rec, false) {
                Ok(Recovery::Nothing) | Err(_) => {
                    self.pause(self.cfg.poll_ms);
                    continue;
                }
                Ok(Recovery::Unresolved) => break,
                Ok(Recovery::Settle(how)) => {
                    let done = settle(
                        &self.cfg,
                        &self.store,
                        attempt,
                        how_termination(how),
                        how,
                        false,
                    );
                    self.after_settle(attempt, done, &mut settle_errors);
                    continue;
                }
                Ok(Recovery::Supervise) => self.spawn(&rec).map(Some),
                Ok(Recovery::Resume) => {
                    if rec.state == AttemptState::Ready && self.spec_path(attempt).is_file() {
                        self.launch(&rec)
                    } else {
                        // An error met while the agent stops (a probe it
                        // left) is not the attempt's: the next agent resumes it.
                        if let Err(e) = self.prepare(&rec).or_else(|e| {
                            if self.stopping() {
                                Ok(())
                            } else {
                                Err(e)
                            }
                        }) {
                            note(
                                &self.store,
                                &rec,
                                "agent_prepare_failed",
                                json!({ "code": e.code, "message": e.message }),
                            );
                            let done = settle(
                                &self.cfg,
                                &self.store,
                                attempt,
                                failed(),
                                Settle::Interrupted,
                                false,
                            );
                            self.after_settle(attempt, done, &mut settle_errors);
                        }
                        continue;
                    }
                }
            };
            match started {
                Ok(Some(c)) => child = Some(c),
                // Settled instead of started: the next look sees the end.
                Ok(None) => {}
                Err(()) => {
                    failures += 1;
                    if policy::gives_up(failures, self.cfg.max_spawn_failures) {
                        self.give_up(
                            &rec,
                            "the supervisor could not be started",
                            termination(None, None),
                        );
                        return;
                    }
                    self.pause(self.cfg.retry_ms);
                }
            }
        }
        self.park(child);
    }

    /// A settle that failed is tried again, later each time; one that found
    /// the attempt owned by somebody else waits for the next look.
    fn after_settle(&self, attempt: &AttemptId, done: Result<bool, ApiError>, errors: &mut u32) {
        match done {
            Ok(true) => *errors = 0,
            Ok(false) => self.pause(self.cfg.poll_ms),
            Err(e) => {
                self.note_once(
                    attempt,
                    "agent_settle_failed",
                    json!({ "code": e.code, "message": e.message }),
                );
                self.pause(policy::backoff_ms(self.cfg.retry_ms, *errors));
                *errors = errors.saturating_add(1);
            }
        }
    }

    fn spawn(&self, rec: &AcceptedAttempt) -> Result<Child, ()> {
        spawn_detached(&self.cfg.supervisor_exe, &self.spec_path(&rec.attempt_id)).map_err(|e| {
            note(
                &self.store,
                rec,
                "agent_spawn_failed",
                json!({ "error": e.to_string() }),
            );
        })
    }

    /// Start the supervisor that will start the commands of a prepared
    /// attempt. The lease and the deadline have been running since the
    /// acceptance: with the lease run out nothing is started (spec chapter
    /// 12), and the supervisor is given what is left of both: the deadline,
    /// and the lease as a seed for its own lease clock, so that it stops the
    /// commands when the grant made before its start runs out.
    /// `Ok(None)`: the attempt was settled instead (or will be, next look).
    fn launch(&self, rec: &AcceptedAttempt) -> Result<Option<Child>, ()> {
        let a = &rec.attempt_id;
        let payload: AttemptPayload = serde_json::from_value(rec.spec.clone()).map_err(|e| {
            self.note_once(
                a,
                "agent_spawn_failed",
                json!({ "error": format!("stored payload: {e}") }),
            );
        })?;
        if let Err(error) = self
            .check_capabilities(&payload)
            .and_then(|_| self.check_profile_identity(rec))
            .and_then(|_| self.check_execution_binding(rec))
        {
            // The agent stops: the check may have been left unfinished, and
            // the next agent looks again.
            if self.stopping() {
                return Ok(None);
            }
            self.note_once(a, "agent_environment_changed", json!({ "code": error.code, "blocked_reason": error.blocked_reason, "message": error.message }));
            settle(
                &self.cfg,
                &self.store,
                a,
                failed(),
                Settle::Interrupted,
                false,
            )
            .map_err(|_| ())?;
            return Ok(None);
        }
        self.at(ProbePoint::LeaseMeasure, a);
        // The key and the time of its grant, with no renewal in between
        // (`lease_step`): a key read with the time of an earlier grant would
        // end a lease that was just renewed, here or in the supervisor. A
        // renewal that arrives after this makes the seed void (the supervisor
        // then counts that grant in full), never too short.
        let measuring = lock(&self.lease_step);
        let measured_at = platform::monotonic_ms();
        let measured = self
            .store
            .lease(a)
            .and_then(|lease| Ok((lease, self.elapsed(rec, measured_at)?)));
        let (lease, (since_accept, since_grant)) = match measured {
            Ok(m) => m,
            Err(e) => {
                // Without the key the supervisor would take the seed for
                // void and count a full period from its start; without the
                // grant time the lease cannot be measured at all. Nothing is
                // started and nothing is given up: the next look reads again.
                drop(measuring);
                self.note_once(
                    a,
                    "agent_lease_unreadable",
                    json!({ "error": e.to_string() }),
                );
                self.pause(self.cfg.retry_ms);
                return Ok(None);
            }
        };
        // A lease that was ended before its time ran out (the member said
        // it retired this identity) is over as well.
        let ended = lease.as_ref().is_some_and(|(_, expired)| *expired);
        let key = lease.map(|(k, _)| k);
        let left = policy::remaining_ms(payload.lease_ms, since_grant).filter(|_| !ended);
        let Some(lease_left) = left else {
            // The journal gets `lease_expired` with it. Still under the
            // lock: a renewal either came before the measuring or is refused.
            let _ = self.store.expire_lease(a, now());
            drop(measuring);
            let t = termination(Some(StopReason::LeaseExpired), None);
            self.settle_unstarted(a, t);
            return Ok(None);
        };
        drop(measuring);
        let mut deadline_left = None;
        if let Some(total) = payload.deadline_ms {
            let Some(left) = policy::remaining_ms(total, since_accept) else {
                self.note_once(a, "agent_deadline_before_start", json!({}));
                let info = TimeoutInfo {
                    kind: TimeoutKind::Deadline,
                    phase: TimeoutPhase::Preparing,
                };
                let t = termination(Some(StopReason::Timeout(info)), None);
                self.settle_unstarted(a, t);
                return Ok(None);
            };
            deadline_left = Some(left);
        }
        let seed = LeaseSeed {
            ends_mono_ms: measured_at.saturating_add(lease_left),
            key,
        };
        // Written at every start: an earlier start's values are stale.
        self.rewrite_start(a, deadline_left, seed).map_err(|e| {
            self.note_once(
                a,
                "agent_spawn_failed",
                json!({ "error": format!("supervisor spec: {e}") }),
            );
        })?;
        self.spawn(rec).map(Some)
    }

    fn settle_unstarted(&self, attempt: &AttemptId, t: Termination) {
        if settle(
            &self.cfg,
            &self.store,
            attempt,
            t,
            Settle::Interrupted,
            false,
        )
        .is_err()
        {
            self.pause(self.cfg.retry_ms);
        }
    }

    /// The supervisor counts its deadline from its own start, and its lease
    /// from the seed.
    fn rewrite_start(
        &self,
        attempt: &AttemptId,
        deadline_left_ms: Option<u64>,
        seed: LeaseSeed,
    ) -> Result<(), String> {
        let path = self.spec_path(attempt);
        let text = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let mut spec: SuperviseSpec = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        spec.deadline_ms = deadline_left_ms;
        spec.lease_seed = Some(seed);
        // Stored when the attempt was prepared, maybe under another scope.
        spec.rescope(
            &self.cfg.traffic_state_dir,
            self.cfg.traffic_config.as_deref(),
            self.cfg
                .capability_profile
                .as_ref()
                .map(|profile| profile.scope_environment()),
        );
        let bytes = serde_json::to_vec(&spec).map_err(|e| e.to_string())?;
        write_atomic(&path, &bytes).map_err(|e| e.to_string())
    }

    /// How long ago the attempt was accepted and its lease last granted, in
    /// milliseconds: on this process's monotonic clock when it saw both, else
    /// (the agent restarted since) from the wall-clock times this worker
    /// recorded. No other host's time is involved. A renewal time that
    /// cannot be read is an error, not "never renewed": that would end a
    /// renewed lease early.
    fn elapsed(&self, rec: &AcceptedAttempt, mono: u64) -> Result<(u64, u64), StorageError> {
        if let Some(c) = lock(&self.clocks).get(rec.attempt_id.as_str()) {
            return Ok((
                mono.saturating_sub(c.accepted_ms),
                mono.saturating_sub(c.granted_ms),
            ));
        }
        let wall = now();
        let granted = self
            .store
            .lease_renewed_at(&rec.attempt_id)?
            .unwrap_or(rec.accepted_at_ms)
            .max(rec.accepted_at_ms);
        Ok((
            wall.saturating_sub(rec.accepted_at_ms),
            wall.saturating_sub(granted),
        ))
    }

    /// Wait (without blocking) for the supervisors that have exited.
    fn reap(&self) {
        lock(&self.children).retain_mut(|c| matches!(c.try_wait(), Ok(None)));
    }

    fn park(&self, child: Option<Child>) {
        if let Some(mut c) = child {
            if matches!(c.try_wait(), Ok(None)) {
                lock(&self.children).push(c);
            }
        }
    }

    fn attempt_dir(&self, attempt: &AttemptId) -> PathBuf {
        self.cfg.attempts_root().join(attempt.as_str())
    }

    fn spec_path(&self, attempt: &AttemptId) -> PathBuf {
        spec_path(&self.cfg, attempt)
    }

    fn workspace_path(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("workspace.json")
    }

    fn workspace_of(&self, attempt: &AttemptId) -> Option<Workspace> {
        let text = fs::read_to_string(self.workspace_path(attempt)).ok()?;
        serde_json::from_str(&text).ok()
    }

    /// `host.prepare` under the limits: at most `max_concurrent_prepares` at
    /// once, at most `prepare_timeout_ms` long. It runs on its own thread,
    /// which keeps its permit until the host returns, however late; a
    /// workspace nobody waits for any more is removed. `Ok(None)`: the agent
    /// is stopping or the attempt is being stopped.
    fn host_prepare(
        &self,
        rec: &AcceptedAttempt,
        payload: &AttemptPayload,
        toolchain: Option<super::host::PrepareToolchain>,
    ) -> Result<Option<Workspace>, ApiError> {
        let a = &rec.attempt_id;
        let permit = loop {
            if self.stopping() || self.stop_requested(a) {
                return Ok(None);
            }
            if let Some(p) = Permits::try_acquire(&self.prepares) {
                break p;
            }
            self.pause(self.cfg.poll_ms);
        };
        let slot = Arc::new(Mutex::new(Slot::Waiting));
        let (host, shards, theirs) = (self.host.clone(), self.shards.clone(), slot.clone());
        let (rec2, payload2) = (rec.clone(), payload.clone());
        std::thread::Builder::new()
            .name("prepare".into())
            .spawn(move || {
                let _permit = permit;
                let made = catch_unwind(AssertUnwindSafe(|| {
                    host.prepare(&rec2, &payload2, toolchain.as_ref())
                }))
                .unwrap_or_else(|_| Err(internal("workspace", "the preparation panicked")));
                let mut slot = lock(&theirs);
                if matches!(*slot, Slot::Abandoned) {
                    drop(slot);
                    if let Ok(ws) = made {
                        let _one = lock(shard(&shards, &rec2.attempt_id));
                        let _ = host.cleanup(&rec2.attempt_id, Some(&ws));
                    }
                } else {
                    *slot = Slot::Done(made);
                }
            })
            .map_err(|e| internal("prepare thread", e))?;
        let end = platform::monotonic_ms().saturating_add(self.cfg.prepare_timeout_ms);
        loop {
            {
                let mut slot = lock(&slot);
                match std::mem::replace(&mut *slot, Slot::Abandoned) {
                    Slot::Done(made) => return made.map(Some),
                    _ if platform::monotonic_ms() >= end => {
                        return Err(internal(
                            "workspace",
                            format!(
                                "the preparation took longer than {} ms",
                                self.cfg.prepare_timeout_ms
                            ),
                        ))
                    }
                    _ if self.stopping() => return Ok(None),
                    waiting => *slot = waiting,
                }
            }
            std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.clamp(1, 100)));
        }
    }

    /// `Accepted | Preparing -> Ready`: the workspace, the secret files, the
    /// supervisor's spec. No command is started here.
    fn prepare(&self, rec: &AcceptedAttempt) -> Result<(), ApiError> {
        let attempt = &rec.attempt_id;
        let payload: AttemptPayload =
            serde_json::from_value(rec.spec.clone()).map_err(|e| internal("stored payload", e))?;
        self.check_capabilities(&payload)?;
        self.check_profile_identity(rec)?;
        if rec.state == AttemptState::Accepted {
            self.store
                .transition(attempt, AttemptState::Preparing, now())
                .map_err(api)?;
        }
        let dir = self.attempt_dir(attempt);
        private_dir(&dir).map_err(|e| internal("attempt directory", e))?;
        // The toolchains first: the workspace's preparation runs with them
        // activated (spec 22.1), as setup-node runs before npm ci. Nothing
        // is installed for a bundle the source does not read back to.
        self.host.check_toolchain_source(rec, &payload)?;
        let Some(binding) = self.prepare_toolchains(rec, &payload)? else {
            return Ok(());
        };
        let binding = binding.as_ref();
        // An agent that died after making the workspace left its record.
        let ws = match self.workspace_of(attempt) {
            Some(ws) if ws.root.is_dir() => ws,
            _ => {
                let bin = self
                    .cfg
                    .capability_profile
                    .as_ref()
                    .and_then(|p| p.embedded_bin());
                let toolchain = binding
                    .map(|b| super::host::PrepareToolchain::of(bin, b))
                    .transpose()
                    .map_err(invalid)?;
                match self.host_prepare(rec, &payload, toolchain)? {
                    Some(ws) => ws,
                    None => return Ok(()),
                }
            }
        };
        let ws_json = serde_json::to_vec(&ws).map_err(|e| internal("workspace record", e))?;
        write_atomic(&self.workspace_path(attempt), &ws_json)
            .map_err(|e| internal("workspace record", e))?;

        // The prepared setup steps are copied in (or waited for); the
        // commands that are such a step are left out.
        let Some(skip) = self.setup_wait(rec, &payload, &ws, binding)? else {
            return Ok(());
        };
        let commands = crate::setup::policy::prune_commands(&payload.commands, &skip);
        // Every command was a prepared setup step (an install-only task with
        // its template there): the supervisor settles it as succeeded
        // without a command. The journal says which steps were not run.
        // One task of a frozen CI plan has no command either: the plan has
        // its steps, and the supervisor runs them as the plan's core asks.
        let ci = match &payload.ci_task {
            Some(task) => {
                let env = super::ci_task::base_env(self.cfg.capability_profile.as_deref(), binding)
                    .map_err(invalid)?;
                Some(super::ci_task::prepare(
                    &self.cfg.state_dir,
                    &dir,
                    &payload,
                    task,
                    &ws,
                    env,
                )?)
            }
            None => None,
        };
        let prepared_only = commands.is_empty() && ci.is_none();
        if prepared_only {
            self.note_once(
                attempt,
                "agent_setup_satisfied",
                json!({ "steps": skip.iter().collect::<Vec<_>>() }),
            );
        }

        // The secret bundle arrives by its own request once the workspace is
        // there; the commands do not start before the set is complete.
        //
        // From the look that finds the set complete until the attempt is
        // `Ready`, the attempt's shard is held, the lock a
        // `secret_bundle_put` places its files under: a put is either done
        // before that look, and the mask below is made of all it placed, or
        // it finds the attempt `Ready` and places nothing. No file is read
        // for the mask while it is being written, and none arrives after.
        let mut waited = 0;
        let _placed = loop {
            let one = lock(shard(&self.shards, attempt));
            if self.host.secrets_ready(rec)? {
                break one;
            }
            drop(one);
            if self.stopping() || self.stop_requested(attempt) {
                return Ok(());
            }
            if waited >= self.cfg.secret_wait_ms {
                return Err(ApiError::blocked(
                    crate::error::BlockedReason::SecretUnavailable,
                    "the secret bundle did not arrive",
                ));
            }
            self.pause(self.cfg.poll_ms);
            waited += self.cfg.poll_ms;
        };

        // The values of the secret files in place, read now that the set is
        // complete: everything the commands print goes through a masker made
        // of them before it is stored. They are read from the files, so an
        // agent that restarted since the bundle arrived masks the same.
        let placed = self.host.placed_secrets(rec)?;
        let (secret, secret_files) = (placed.values, placed.files);
        if !secret.skipped_keys.is_empty() {
            // Key names are not secret, but the count says enough.
            self.note_once(
                attempt,
                "agent_mask_skipped",
                json!({ "too_short_to_mask": secret.skipped_keys.len() }),
            );
        }
        let mask = policy::mask_strings(secret.values).map_err(|e| {
            ApiError::blocked(
                crate::error::BlockedReason::SecretUnavailable,
                format!("{e}: its output could not be masked, nothing is started"),
            )
        })?;

        // The declared artifacts, checked again against the secret files of
        // the source (which is here now: the workspace was made from it). An
        // acceptance that came before the source could not look.
        let artifacts = if payload.artifacts.is_empty()
            && payload.outputs.is_empty()
            && ci.is_none()
        {
            // (The result of a CI task leaves as an artifact.)
            None
        } else {
            let secrets = self.host.secret_paths(&payload, true, None)?.ok_or_else(|| {
                ApiError::new(
                    ErrorCode::NotFound,
                    format!(
                        "source {} is not in this worker's store: its secret files cannot be told from the declared artifacts",
                        payload.source_id
                    ),
                )
            })?;
            check_artifacts_not_secret(&payload, &secrets)?;
            Some(ArtifactsSpec {
                outputs: payload.outputs.clone(),
                secret_paths: secrets,
                // Declared paths are relative to the workspace root, like
                // the paths of the secret files they are compared with.
                workspace: ws.root.clone(),
                source_id: payload.source_id.clone(),
                plan_digest: payload.plan_digest.clone(),
                profile_fingerprint: binding
                    .map(|b| b.fingerprint().clone())
                    .unwrap_or_else(|| self.cfg.profile_fingerprint.clone()),
                max_bytes: DEFAULT_ARTIFACT_BYTES,
                files: payload
                    .artifacts
                    .iter()
                    .map(|d| ArtifactFile {
                        name: d.name.clone(),
                        source: PathBuf::from(&d.path),
                        if_missing: d.if_missing,
                    })
                    .collect(),
                // What the names above may not lead to, by any spelling or
                // link: the files the mask was read from.
                secret_files,
            })
        };

        // In the order they run: after what they depend on.
        let order = policy::execution_order(&commands).map_err(invalid)?;
        let mut nodes = Vec::with_capacity(commands.len());
        for c in order.into_iter().map(|i| &commands[i]) {
            let rel = Path::new(&c.cwd);
            check_cwd_relative(rel)?;
            let cwd = if ws.kind == crate::workspace::WorkspaceKind::InPlace {
                // Recheck every command, including recovered preparation and
                // explicit retry: the original tree is mutable between attempts.
                let resolved = fs::canonicalize(ws.cwd.join(rel))
                    .map_err(|_| invalid("in-place command cwd is unavailable"))?;
                if !resolved.is_dir() || !resolved.starts_with(&ws.root) {
                    return Err(invalid(
                        "in-place command cwd leaves the original workspace",
                    ));
                }
                resolved
            } else {
                ws.root.join(rel)
            };
            let env = match &self.cfg.capability_profile {
                Some(profile) => profile
                    .command_environment(binding, &c.env)
                    .map_err(invalid)?,
                None => c.env.clone(),
            };
            nodes.push(NodeSpec {
                node_id: c.node_id.clone(),
                argv: c.argv.clone(),
                cwd,
                env,
                weight: None,
                timeout_ms: c.timeout_ms,
            });
        }
        let terms = self.store.lease_terms(attempt).map_err(api)?;
        let spec = SuperviseSpec {
            preparation: None,
            profile_fingerprint: binding.map(|b| b.fingerprint().clone()).or_else(|| {
                self.cfg
                    .capability_profile
                    .as_ref()
                    .map(|_| self.cfg.profile_fingerprint.clone())
            }),
            state_dir: self.cfg.state_dir.clone(),
            worker_id: self.cfg.worker_id.clone(),
            attempt_id: attempt.clone(),
            traffic_state_dir: self.cfg.traffic_state_dir.clone(),
            traffic_config: self.cfg.traffic_config.clone(),
            nodes,
            lease_ms: terms.map_or(payload.lease_ms, |t| t.0),
            term_grace_ms: self.cfg.term_grace_ms,
            kill_wait_ms: self.cfg.kill_wait_ms,
            // The window persisted at acceptance, the one the renewal check
            // counts (spec 12), not this process's poll.
            poll_ms: terms.map_or(self.cfg.poll_ms, |t| t.1),
            command_timeout_ms: payload.command_timeout_ms,
            resource_wait_timeout_ms: payload.resource_wait_timeout_ms,
            // What is left of both is written when the supervisor is started.
            deadline_ms: payload.deadline_ms,
            lease_seed: None,
            inhibit_sleep: self.cfg.inhibit_sleep,
            // The spool and this file share `<state_dir>/attempts/<attempt>`.
            spool_root: Some(self.cfg.attempts_root()),
            log_limit_bytes: self.cfg.log_limit_bytes,
            workspace_warnings: ws.warnings.clone(),
            // This file therefore holds secrets: it is written 0600 and
            // removed with the workspace when the attempt has ended.
            mask: MaskValues(mask),
            artifacts,
            prepared_only,
            ci,
        };
        self.check_capabilities(&payload)?;
        spec.validate().map_err(|e| invalid(e.to_string()))?;
        let bytes = serde_json::to_vec(&spec).map_err(|e| internal("supervisor spec", e))?;
        write_atomic(&self.spec_path(attempt), &bytes)
            .map_err(|e| internal("supervisor spec", e))?;
        if self.stop_requested(attempt) {
            // The loop settles it as cancelled; nothing is started.
            return Ok(());
        }
        self.store
            .transition(attempt, AttemptState::Ready, now())
            .map_err(api)?;
        Ok(())
    }

    fn resolve_toolchains(
        &self,
        mut bundle: crate::toolchain::contract::FrozenBundle,
        requirements: &crate::toolchain::capability::ExecutionRequirements,
    ) -> Result<crate::toolchain::contract::FrozenBundle, ApiError> {
        bundle
            .validate_partial(&bundle.frozen.source_manifest)
            .map_err(invalid)?;
        requirements.validate().map_err(invalid)?;
        // Answered from the measured profile, not by probing every tool per
        // question: a probe that runs late on a loaded host is no answer,
        // and the parent waits only `forecast_wait_ms` for this one.
        let started = Instant::now();
        let (report, held) = self
            .cfg
            .capability_profile
            .as_ref()
            .map(|profile| profile.measured_profile())
            .filter(|(report, _)| report.profile_fingerprint == self.cfg.profile_fingerprint)
            .ok_or_else(|| invalid("proposal profile unavailable"))?;
        self.note_slow_resolution(started.elapsed(), &held);
        let observed = crate::toolchain::contract::observations(&report);
        for baseline in &bundle.frozen.baseline {
            if bundle.frozen.policy.compare(
                &baseline.version,
                observed
                    .get(&baseline.tool)
                    .unwrap_or(&crate::toolchain::versions::VersionObservation::Unknown),
            ) != crate::toolchain::versions::Match::Match
            {
                return Err(ApiError::blocked(
                    crate::error::BlockedReason::EnvironmentMismatch,
                    "proposal baseline mismatch",
                ));
            }
        }
        for requirement in bundle.frozen.requirements.clone() {
            if bundle.evidence.iter().any(|e| e.tool == requirement.tool) {
                continue;
            }
            // A recipe of the registry wins. Where none serves the version,
            // a toolchain the user's rustup already has may (spec 22.1).
            let refused = match self.toolchains.as_ref() {
                Some(registry) => match registry.request(&bundle.frozen, &requirement.tool, None) {
                    Ok(request) => {
                        bundle.frozen = request.frozen;
                        bundle.evidence.push(request.evidence);
                        continue;
                    }
                    Err(e) => Some(e),
                },
                None => None,
            };
            let detected = refused
                .as_ref()
                .is_none_or(|e| e.allows_native())
                .then(|| {
                    crate::toolchain::contract::detected_rustup_evidence(
                        &bundle.frozen,
                        &requirement,
                        &report,
                    )
                })
                .flatten();
            let Some(evidence) = detected else {
                return Err(match refused {
                    Some(e) => ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        format!(
                            "toolchain resolve required: {e:?} ({})",
                            requirement.describe()
                        ),
                    ),
                    None => invalid("proposal registry unavailable"),
                });
            };
            bundle.frozen =
                crate::toolchain::adapters::resolve(&bundle.frozen, &evidence).map_err(invalid)?;
            bundle.evidence.push(evidence);
        }
        bundle
            .validate(&bundle.frozen.source_manifest)
            .map_err(invalid)?;
        self.toolchain_requests(&bundle, &|| Some(report.clone()))?;
        if !crate::toolchain::capability::missing_with_installable(
            requirements,
            Some(&report),
            Some(&bundle),
        )
        .is_empty()
        {
            return Err(ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                "proposal worker requirements mismatch",
            ));
        }

        Ok(bundle)
    }
    /// Read-only preparation identity; never readiness or forecast evidence.
    pub(crate) fn probe_preparation(
        &self,
        input: crate::setup::preparation_probe::PreparationProbeInput<'_>,
    ) -> crate::setup::preparation_probe::PreparationProbe {
        use crate::setup::preparation_probe::{PreparationProbe, ProbeUnknown};
        let Some(owner) = self.host.setup_owner() else {
            return PreparationProbe::Unknown(ProbeUnknown::MissingContext);
        };
        owner.probe_preparation(
            input,
            self.cfg.capability_profile.as_deref(),
            Some(&self.cfg.profile_fingerprint),
            self.toolchains.as_ref(),
        )
    }

    /// How each frozen selection is served here: by a managed install the
    /// registry plans, or, when no managed install can serve it, by the tool
    /// the profile itself has at exactly the selected version. `report` is
    /// asked at most once, and only for a selection the registry cannot serve.
    fn toolchain_requests(
        &self,
        bundle: &crate::toolchain::contract::FrozenBundle,
        report: &dyn Fn() -> Option<crate::toolchain::capability::WorkerCapabilities>,
    ) -> Result<ProvidedToolchains, ApiError> {
        use crate::toolchain::registry::Unavailable;
        let mut provided = ProvidedToolchains::default();
        let mut measured = None;
        for evidence in &bundle.evidence {
            let refused = match self
                .toolchains
                .as_ref()
                .map_or(Err(Unavailable::Unconfigured), |registry| {
                    registry.request(&bundle.frozen, &evidence.tool, Some(evidence))
                }) {
                Ok(request) => {
                    provided.requests.push(request);
                    continue;
                }
                Err(refused) => refused,
            };
            let native = refused
                .allows_native()
                .then(|| measured.get_or_insert_with(report).as_ref())
                .flatten()
                .and_then(|report| {
                    crate::toolchain::contract::native_tool(&bundle.frozen, &evidence.tool, report)
                });
            // Neither a recipe nor the worker's own tool serves it: the
            // user's mise or rustup may install that exact version (spec 22.1).
            let installed = match (&native, &self.toolchains) {
                (None, Some(registry)) if refused.allows_native() => registry
                    .install_request(&bundle.frozen, &evidence.tool, evidence)
                    .ok(),
                _ => None,
            };
            if let Some(request) = installed {
                provided.requests.push(request);
                continue;
            }
            match native {
                Some(native) => provided.native.push(native),
                None => {
                    return Err(ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        match &self.toolchains {
                            Some(registry)
                                if refused.allows_native()
                                    && crate::toolchain::registry::MISE_TOOLS
                                        .contains(&evidence.tool.as_str())
                                    && !registry.can_install(evidence.tool.as_str()) =>
                            {
                                format!(
                                    "toolchain unavailable: {} {} is not installed and this worker has no mise; {}",
                                    evidence.tool.as_str(),
                                    evidence.concrete,
                                    crate::toolchain::registry::MISE_INSTALL_HINT
                                )
                            }
                            Some(_) => format!("toolchain unavailable: {refused:?}"),
                            None => "toolchain registry unavailable".into(),
                        },
                    ))
                }
            }
        }
        Ok(provided)
    }
    fn execution_binding_path(&self, attempt: &AttemptId) -> PathBuf {
        self.attempt_dir(attempt).join("execution-binding.json")
    }
    fn check_execution_binding(&self, rec: &AcceptedAttempt) -> Result<(), ApiError> {
        let payload: AttemptPayload =
            serde_json::from_value(rec.spec.clone()).map_err(|e| internal("stored payload", e))?;
        let Some(bundle) = payload.toolchains else {
            return Ok(());
        };
        let binding: crate::toolchain::contract::ExecutionBinding = serde_json::from_slice(
            &fs::read(self.execution_binding_path(&rec.attempt_id))
                .map_err(|e| internal("execution binding", e))?,
        )
        .map_err(|e| internal("execution binding", e))?;
        use crate::toolchain::{adapters, contract::observations};
        // One report decides what is native and is what the activation is
        // checked against.
        let measured = std::cell::OnceCell::new();
        let measure = || measured.get_or_init(|| self.measured()).clone();
        let ProvidedToolchains { requests, native } = self.toolchain_requests(&bundle, &measure)?;
        let mut receipts = Vec::with_capacity(requests.len());
        for request in &requests {
            match self.toolchains.as_ref().map(|r| r.detect(request)) {
                Some(Ok(adapters::Availability::Installed(receipt))) => receipts.push(receipt),
                _ => {
                    return Err(ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        "activated tool identity changed",
                    ))
                }
            }
        }
        let profile = self
            .cfg
            .capability_profile
            .as_ref()
            .ok_or_else(|| invalid("toolchain profile unavailable"))?;
        let report = measure().ok_or_else(|| {
            ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                "accepted base profile changed",
            )
        })?;
        let plans = requests
            .iter()
            .map(|r| r.plan(&r.private_root).map_err(invalid))
            .collect::<Result<Vec<_>, _>>()?;
        let installed = plans
            .iter()
            .zip(receipts.iter().map(|r| r.as_ref()))
            .collect::<Vec<_>>();
        let shell = shell_identity(profile)?;
        let setup_policy = Digest::of_bytes(
            &serde_json::to_vec(&payload.setup).map_err(|e| internal("setup policy", e))?,
        );
        // A valid same-version replacement receipt is a different execution
        // identity. Reconstruct the complete activation read-only; never repair
        // or rewrite the accepted binding during launch/recovery checks.
        let current = adapters::activate_with_native(
            &bundle.frozen,
            &installed,
            &native,
            self.cfg.profile_fingerprint.clone(),
            &observations(&report),
            profile.environment(),
            setup_policy,
            &shell,
        )
        .map_err(|e| activation_refused(e, &native, &plans))?;
        let setup = self.setup_tool_requests(&bundle, &payload.requirements, &plans, &report)?;
        let mut setup_receipts = Vec::with_capacity(setup.len());
        for request in &setup {
            match self.toolchains.as_ref().map(|r| r.detect(request)) {
                Some(Ok(adapters::Availability::Installed(receipt))) => {
                    setup_receipts.push(receipt)
                }
                _ => {
                    return Err(ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        "activated tool identity changed",
                    ))
                }
            }
        }
        let current = self.with_setup_tools(current, &setup, &setup_receipts)?;
        if current != binding.activation {
            return Err(ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                "accepted execution binding changed",
            ));
        }
        Ok(())
    }
    /// Wait for one install of the supervisor. `None`: the agent is
    /// stopping or the attempt is being stopped.
    fn poll_tool_install(
        &self,
        rec: &AcceptedAttempt,
        request: &crate::toolchain::preparation::PreparationRequest,
        begun: u64,
    ) -> Result<Option<Box<crate::toolchain::adapters::InstallationReceipt>>, ApiError> {
        use crate::toolchain::preparation::ToolPreparationPoll;
        loop {
            match self.host.toolchain_poll(&rec.attempt_id, request)? {
                ToolPreparationPoll::Installed(receipt) => return Ok(Some(receipt)),
                ToolPreparationPoll::Failed(reason) => {
                    return Err(ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        format!("toolchain preparation failed: {reason:?}"),
                    ))
                }
                ToolPreparationPoll::Pending => {
                    if self.stopping() || self.stop_requested(&rec.attempt_id) {
                        self.host.setup_release(&rec.attempt_id);
                        return Ok(None);
                    }
                    if platform::monotonic_ms().saturating_sub(begun) >= self.cfg.prepare_timeout_ms
                    {
                        return Err(ApiError::blocked(
                            crate::error::BlockedReason::EnvironmentMismatch,
                            "toolchain preparation timed out",
                        ));
                    }
                    self.pause(self.cfg.poll_ms);
                }
            }
        }
    }
    /// The installs of the setup tools of the job that the worker does not
    /// have on its PATH (spec 22.1): each `cargo-*` one, with the cargo of
    /// the rust the attempt activates. A tool that cannot be installed so is
    /// a refusal with the reason, never a silent omission.
    fn setup_tool_requests(
        &self,
        bundle: &crate::toolchain::contract::FrozenBundle,
        requirements: &crate::toolchain::capability::ExecutionRequirements,
        plans: &[crate::toolchain::adapters::InstallPlan],
        report: &crate::toolchain::capability::WorkerCapabilities,
    ) -> Result<Vec<crate::toolchain::preparation::PreparationRequest>, ApiError> {
        use crate::toolchain::capability::Requirement;
        let wanted: Vec<&String> = requirements
            .requires
            .iter()
            .filter_map(|r| match r {
                Requirement::Setup(name)
                    if crate::toolchain::adapters::cargo_installable(name)
                        && !report.path_tools.contains(name) =>
                {
                    Some(name)
                }
                _ => None,
            })
            .collect();
        if wanted.is_empty() {
            return Ok(Vec::new());
        }
        let refuse =
            |why: String| ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, why);
        let names = || {
            wanted
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let rust_bin = crate::toolchain::contract::setup_cargo_bin(
            &bundle.frozen,
            report,
            plans
                .iter()
                .find(|p| p.tool().as_str() == "rust")
                .map(|p| p.activation_bin()),
        );
        let evidence = bundle.evidence.iter().find(|e| e.tool.as_str() == "rust");
        let (Some(rust_bin), Some(evidence), Some(registry)) =
            (rust_bin, evidence, self.toolchains.as_ref())
        else {
            return Err(refuse(format!(
                "{} unavailable: belt installs a setup tool with the cargo of the rust toolchain the job sets up (one belt installs or a rustup toolchain of that version), and this attempt has none",
                names()
            )));
        };
        wanted
            .iter()
            .map(|name| {
                let tool = crate::toolchain::versions::ToolId::parse(name)
                    .map_err(|e| refuse(format!("{name} unavailable: {e}")))?;
                registry
                    .cargo_request(&bundle.frozen, &tool, evidence, &rust_bin)
                    .map_err(|e| refuse(format!("{name} unavailable: {e:?}")))
            })
            .collect()
    }
    /// [`crate::toolchain::adapters::with_setup_tools`] over the plans of
    /// `requests`.
    fn with_setup_tools(
        &self,
        activation: crate::toolchain::adapters::ActivationReceipt,
        requests: &[crate::toolchain::preparation::PreparationRequest],
        receipts: &[Box<crate::toolchain::adapters::InstallationReceipt>],
    ) -> Result<crate::toolchain::adapters::ActivationReceipt, ApiError> {
        let plans = requests
            .iter()
            .map(|r| r.plan(&r.private_root).map_err(invalid))
            .collect::<Result<Vec<_>, _>>()?;
        let tools: Vec<_> = plans
            .iter()
            .zip(receipts.iter().map(|r| r.as_ref()))
            .collect();
        crate::toolchain::adapters::with_setup_tools(activation, &tools).map_err(|e| {
            ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                format!("setup tool activation refused: {e}"),
            )
        })
    }
    fn prepare_toolchains(
        &self,
        rec: &AcceptedAttempt,
        payload: &AttemptPayload,
    ) -> Result<Option<Option<crate::toolchain::contract::ExecutionBinding>>, ApiError> {
        use crate::toolchain::{
            adapters,
            contract::{observations, ExecutionBinding},
        };
        let Some(bundle) = &payload.toolchains else {
            return Ok(Some(None));
        };
        let accepted_path = self.execution_binding_path(&rec.attempt_id);
        if accepted_path.exists() {
            // Recovery inspects accepted identities; it must not repair drift by reinstalling.
            self.check_execution_binding(rec)?;
            let binding = serde_json::from_slice(
                &fs::read(accepted_path).map_err(|e| internal("execution binding", e))?,
            )
            .map_err(|e| internal("execution binding", e))?;
            return Ok(Some(Some(binding)));
        }
        // The report that decides what is native is the one the activation is
        // made from: two probes could differ and refuse a tool that did not
        // change. With no native tool nothing is measured here.
        let measured = std::cell::OnceCell::new();
        let ProvidedToolchains { requests, native } =
            self.toolchain_requests(bundle, &|| measured.get_or_init(|| self.measured()).clone())?;
        let profile = self
            .cfg
            .capability_profile
            .as_ref()
            .ok_or_else(|| invalid("toolchain profile unavailable"))?;
        let begun = platform::monotonic_ms();
        let mut receipts = Vec::new();
        for request in &requests {
            match self.poll_tool_install(rec, request, begun)? {
                Some(receipt) => receipts.push(receipt),
                None => return Ok(None),
            }
        }
        let plans = requests
            .iter()
            .map(|r| r.plan(&r.private_root).map_err(invalid))
            .collect::<Result<Vec<_>, _>>()?;
        let installed = plans
            .iter()
            .zip(receipts.iter().map(|r| r.as_ref()))
            .collect::<Vec<_>>();
        let shell = shell_identity(profile)?;
        let setup_policy = Digest::of_bytes(
            &serde_json::to_vec(&payload.setup).map_err(|e| internal("setup policy", e))?,
        );
        let (native, report) = if native.is_empty() {
            (native, self.measured().unwrap_or_else(|| profile.report()))
        } else if requests.is_empty() {
            // Nothing was waited for: the report that named the native tools.
            let report = measured.into_inner().flatten();
            (
                native,
                report
                    .or_else(|| self.measured())
                    .unwrap_or_else(|| profile.report()),
            )
        } else {
            // An install may have taken long. What is native is decided again
            // from the report of this moment, and the activation uses that
            // same report: a tool that changed meanwhile is refused here.
            let report = self.measured().ok_or_else(|| {
                ApiError::blocked(
                    crate::error::BlockedReason::EnvironmentMismatch,
                    "accepted base profile changed",
                )
            })?;
            let again = self.toolchain_requests(bundle, &|| Some(report.clone()))?;
            (again.native, report)
        };
        let activation = adapters::activate_with_native(
            &bundle.frozen,
            &installed,
            &native,
            self.cfg.profile_fingerprint.clone(),
            &observations(&report),
            profile.environment(),
            setup_policy,
            &shell,
        )
        .map_err(|e| activation_refused(e, &native, &plans))?;
        // The setup tools of the job belt installs with that rust (spec
        // 22.1), after the toolchains: their roots are named by its cargo.
        let setup = self.setup_tool_requests(bundle, &payload.requirements, &plans, &report)?;
        let mut setup_receipts = Vec::with_capacity(setup.len());
        for request in &setup {
            match self.poll_tool_install(rec, request, begun)? {
                Some(receipt) => setup_receipts.push(receipt),
                None => return Ok(None),
            }
        }
        let activation = self.with_setup_tools(activation, &setup, &setup_receipts)?;
        let binding = ExecutionBinding { activation };
        let path = self.execution_binding_path(&rec.attempt_id);
        if path.exists() {
            let previous: ExecutionBinding = serde_json::from_slice(
                &fs::read(&path).map_err(|e| internal("execution binding", e))?,
            )
            .map_err(|e| internal("execution binding", e))?;
            if previous != binding {
                return Err(ApiError::blocked(
                    crate::error::BlockedReason::EnvironmentMismatch,
                    "accepted execution binding changed",
                ));
            }
        } else {
            write_atomic(
                &path,
                &serde_json::to_vec(&binding).map_err(|e| internal("execution binding", e))?,
            )
            .map_err(|e| internal("execution binding", e))?;
        }
        Ok(Some(Some(binding)))
    }

    /// Wait for the setup of the attempt (`Host::setup_poll`), as long as
    /// the preparation may take. `None`: the agent is stopping or the
    /// attempt is being stopped. An attempt that waited too long, or whose
    /// template could not be had, runs every step itself.
    fn setup_wait(
        &self,
        rec: &AcceptedAttempt,
        payload: &AttemptPayload,
        ws: &Workspace,
        binding: Option<&crate::toolchain::contract::ExecutionBinding>,
    ) -> Result<Option<std::collections::BTreeSet<String>>, ApiError> {
        let attempt = &rec.attempt_id;
        let begun = platform::monotonic_ms();
        loop {
            match self.host.setup_poll_bound(rec, payload, ws, binding)? {
                SetupPoll::Done { skip, note } => {
                    if let Some(why) = note {
                        self.note_once(
                            attempt,
                            "agent_setup_unavailable",
                            json!({ "reason": why }),
                        );
                    }
                    return Ok(Some(skip));
                }
                SetupPoll::Pending => {
                    if self.stopping() || self.stop_requested(attempt) {
                        self.host.setup_release(attempt);
                        return Ok(None);
                    }
                    if platform::monotonic_ms().saturating_sub(begun) >= self.cfg.prepare_timeout_ms
                    {
                        self.host.setup_release(attempt);
                        let why = format!(
                            "the setup was not ready within {} ms",
                            self.cfg.prepare_timeout_ms
                        );
                        self.note_once(
                            attempt,
                            "agent_setup_unavailable",
                            json!({ "reason": why }),
                        );
                        return Ok(Some(Default::default()));
                    }
                    self.pause(self.cfg.poll_ms);
                }
            }
        }
    }

    fn stop_requested(&self, attempt: &AttemptId) -> bool {
        match self.store.get(attempt) {
            Ok(Some(r)) => r.cancel_requested_at_ms.is_some() || r.retired,
            // Unknown is not a licence to start.
            _ => true,
        }
    }

    /// After the end: secret files and the workspace go at once (spec 22.3
    /// does not wait for the logs to be collected); logs, artifacts and the
    /// result stay until GC. One cleanup of an attempt at a time: the tick
    /// and the attempt's watcher both come here.
    fn clean(&self, attempt: &AttemptId) {
        let _one = lock(shard(&self.shards, attempt));
        // The source the attempt used gets the time it ended (spec 25.8 5b
        // (c)). Every terminal attempt comes here (its watcher, the tick, the
        // sweep after a restart), so this is the one place that records it;
        // a cleanup tried again records again, which only makes the time
        // newer.
        if let Ok(Some(rec)) = self.store.get(attempt) {
            if rec.state == AttemptState::Terminal {
                if let Some(source) = rec.spec["source_id"]
                    .as_str()
                    .and_then(|s| SourceId::parse(s).ok())
                {
                    self.host.source_ended(&source);
                }
            }
        }
        let pending = self.attempt_dir(attempt).join("cleanup-pending");
        // Persist retry evidence before removing the last secret-bearing spec,
        // including when there is no workspace record. A restart sees it too.
        if self.attempt_dir(attempt).exists() && !pending.exists() {
            if let Err(e) = write_atomic(&pending, b"") {
                self.note_once(
                    attempt,
                    "agent_cleanup_failed",
                    json!({ "message": e.to_string() }),
                );
                return;
            }
        }
        // First, and whatever becomes of the rest: the supervisor's spec
        // carries the secret values to mask. The attempt is terminal, so no
        // supervisor is started from it again (one that still runs has read
        // it already).
        let _ = fs::remove_file(self.spec_path(attempt));
        // And what a write of it that was cut short left next to it.
        if let Ok(rd) = fs::read_dir(self.attempt_dir(attempt)) {
            for ent in rd.flatten() {
                let is_file = ent.file_type().is_ok_and(|t| t.is_file());
                if is_file
                    && crate::supervisor::spool_plan::is_tmp_name(
                        &ent.file_name().to_string_lossy(),
                    )
                {
                    let _ = fs::remove_file(ent.path());
                }
            }
        }
        let ws = self.workspace_of(attempt);
        let marker = self.workspace_path(attempt);
        if ws.is_none() && marker.exists() {
            // An unreadable record: keep it, a person has to look.
            return;
        }
        match self.host.cleanup(attempt, ws.as_ref()) {
            Ok(()) => {
                let _ = fs::remove_file(pending);
                let _ = fs::remove_file(marker);
                let _ = fs::remove_dir(self.attempt_dir(attempt));
                lock(&self.clocks).remove(attempt.as_str());
                let mine = format!("{attempt}/");
                lock(&self.noted).retain(|k, _| !k.starts_with(&mine));
            }
            // Tried again at every tick; said once per distinct failure.
            Err(e) => {
                self.note_once(
                    attempt,
                    "agent_cleanup_failed",
                    json!({ "code": e.code, "message": e.message }),
                );
            }
        }
    }

    /// Examine at most `limit` directory entries for terminal cleanup, without
    /// deleting retained results, logs or artifacts. Enumeration resumes on the
    /// next call; after EOF a later call starts a new scan, retrying failures.
    /// Every entry counts, including invalid names and nonterminal attempts.
    pub fn cleanup_terminal(&self, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let mut scan = lock(&self.cleanup_scan);
        if scan.is_none() {
            *scan = fs::read_dir(self.cfg.attempts_root()).ok();
        }
        let mut examined = 0;
        while examined < limit {
            let Some(entry) = scan.as_mut().and_then(Iterator::next) else {
                *scan = None;
                break;
            };
            examined += 1;
            let Ok(entry) = entry else { continue };
            let Ok(a) = AttemptId::parse(&entry.file_name().to_string_lossy()) else {
                continue;
            };
            if !self.workspace_path(&a).exists()
                && !self.spec_path(&a).exists()
                && !self.attempt_dir(&a).join("cleanup-pending").exists()
            {
                continue;
            }
            if matches!(self.store.get(&a), Ok(Some(r)) if r.state == AttemptState::Terminal) {
                self.clean(&a);
            }
        }
        examined
    }

    /// Clean every ended attempt that still has cleanup files.
    pub fn clean_ended(&self) {
        let Ok(rd) = fs::read_dir(self.cfg.attempts_root()) else {
            return;
        };
        for ent in rd.flatten() {
            let Ok(a) = AttemptId::parse(&ent.file_name().to_string_lossy()) else {
                continue;
            };
            if !self.workspace_path(&a).exists()
                && !self.spec_path(&a).exists()
                && !self.attempt_dir(&a).join("cleanup-pending").exists()
            {
                continue;
            }
            if matches!(self.store.get(&a), Ok(Some(r)) if r.state == AttemptState::Terminal) {
                self.clean(&a);
            }
        }
    }

    /// One GC pass: drop what the store says may go (never an attempt that
    /// runs or waits for `ack_collected`), then delete the files. What could
    /// not be deleted, and queued names that are not this store's, are
    /// written to the journal (once each while they stay the same).
    pub fn gc(&self) -> Result<GcReport, ApiError> {
        // Not while a version is on trial (spec 25.1 rule 8).
        if super::control::in_trial(&self.cfg.state_dir) {
            return Ok(GcReport::default());
        }
        // The ack wait is never judged here by the wall clock: a row whose
        // acknowledgement never came goes by uptime (`retire_rows`), with
        // its directory, results and logs.
        let report = self
            .store
            .gc(now(), self.cfg.retention_ms, u64::MAX)
            .map_err(api)?;
        let drained = drain_pending_deletes(&self.store, &self.cfg.attempts_root())
            .map_err(|e| internal("gc", e))?;
        for (locator, error) in &drained.failed {
            self.note_global(
                "agent_gc_delete_failed",
                locator,
                json!({ "locator": locator, "error": error }),
            );
        }
        for locator in &drained.foreign {
            self.note_global(
                "agent_gc_foreign_locator",
                locator,
                json!({ "locator": locator }),
            );
        }
        // The frozen plans no unfinished attempt names go with the attempts.
        super::ci_task::gc_plans(&self.cfg.state_dir, &self.store, self.cfg.retention_ms);
        Ok(report)
    }

    /// Supervise durable attempts and retry bounded terminal cleanup.
    /// Retention GC is deliberately separate.
    pub fn supervise(self: &Arc<Self>) {
        if self.ticking.swap(true, Ordering::SeqCst) {
            return;
        }
        let _pass = Ticking(&self.ticking);
        // The init resident supervises its agent only through this pass, so
        // the key is refreshed here too: links proven with a discarded key
        // must close on an init host as well. Its own in-process link is
        // served by `serve_local_connection` and stays open.
        self.forget_ripe_later();
        self.apply_rekey();
        self.refresh_group_key();
        // The init resident runs this pass and not `tick`: a warm-up nobody
        // asks about must be let go here too, or its install is never stopped.
        self.reap_warmups();
        // An operator's stop with `--cancel-running` reaches the attempts of
        // an init host here, as `tick` does on a worker-only host.
        self.apply_control();
        self.supervise_attempts();
        self.host.toolchain_tick();
        self.cleanup_terminal(32);
        // The deletions time drives (the ack wait, rows an older version
        // acknowledged, retried teardowns, events of no attempt) run on an
        // init host only through this pass; each is bounded.
        self.retire_rows();
        self.drop_stale_incoming();
        self.schedule_received_passes();
    }

    /// Sources nobody added to for 24 hours go, every pass and not only
    /// when the 16 staging places are taken (spec 25.8, 25.10). Not while a
    /// version is on trial (25.1, 8).
    fn drop_stale_incoming(&self) {
        if super::control::in_trial(&self.cfg.state_dir) {
            return;
        }
        if let Some(blobs) = self.host.blobs() {
            blobs.drop_stale(&self.cfg.blobs);
        }
    }

    /// An operator's stop request: cancel whatever is live. Cancellation is
    /// persisted per attempt (idempotent) and the supervisors act on it.
    fn apply_control(self: &Arc<Self>) {
        // Only an explicit, readable request cancels; a broken marker only drains.
        if !super::control::read(&self.cfg.state_dir).is_ok_and(|c| c.cancel_running) {
            return;
        }
        let Ok(list) = self.store.list_nonterminal() else {
            return;
        };
        for rec in list {
            // Under the epoch of the member the attempt belongs to: one
            // member's epoch says nothing about another's.
            let store = self.store.for_member(&rec.coordinator_id);
            let Ok(epoch) = store.max_coordinator_epoch() else {
                continue;
            };
            if rec.state != AttemptState::Terminal
                && matches!(
                    store.cancel(&rec.attempt_id, epoch, now()),
                    Ok(CancelOutcome::Requested(_) | CancelOutcome::AlreadyRequested(_))
                )
            {
                self.kick(&rec.attempt_id);
            }
        }
    }

    fn supervise_attempts(self: &Arc<Self>) {
        self.reap();
        if let Ok(list) = self.store.list_nonterminal() {
            for rec in list {
                self.kick(&rec.attempt_id);
            }
        }
    }

    /// Periodic work: watch attempts nobody watches, clean, collect garbage.
    /// One pass at a time; a call during a pass returns at once.
    pub fn tick(self: &Arc<Self>) {
        if self.ticking.swap(true, Ordering::SeqCst) {
            return;
        }
        let _pass = Ticking(&self.ticking);
        self.forget_ripe_later();
        self.apply_rekey();
        self.refresh_group_key();
        self.apply_control();
        self.reap_warmups();
        self.supervise_attempts();
        self.host.toolchain_tick();
        self.clean_ended();
        self.retire_rows();
        self.drop_stale_incoming();
        self.schedule_received_passes();
        self.collect_garbage();
    }

    /// Starts a round of deleting passes of the received sources on a
    /// thread of its own (spec 25.8 5b (d), (h)6): not on this tick's
    /// thread, at most one at a time, at most once a minute, not while a
    /// version is on trial (25.1, 8).
    fn schedule_received_passes(self: &Arc<Self>) {
        if self.stopping() || super::control::in_trial(&self.cfg.state_dir) {
            return;
        }
        if self
            .deleted_at
            .lock()
            .unwrap()
            .is_some_and(|at| at.elapsed() < RECEIVED_PASS_EVERY)
        {
            return;
        }
        if self.deleting.swap(true, Ordering::SeqCst) {
            return;
        }
        // (in `threads` like the attempts' threads, so `shutdown` joins it
        // before the agent lock goes: a pass that outlived it could detach
        // a source another process's agent pins in the meantime. The check
        // of `stopping` is under the list's lock: `shutdown` sets it before
        // it takes the list.)
        let mut threads = lock(&self.threads);
        if self.stopping() {
            self.deleting.store(false, Ordering::SeqCst);
            return;
        }
        let agent = self.clone();
        let spawned = std::thread::Builder::new()
            .name("belt-received-pass".into())
            .spawn(move || {
                let _running = Ticking(&agent.deleting);
                agent.run_received_passes();
            });
        match spawned {
            Ok(t) => {
                threads.retain(|t| !t.is_finished());
                threads.push(t);
            }
            Err(_) => self.deleting.store(false, Ordering::SeqCst),
        }
    }

    /// One round: passes back to back while each made progress (spec
    /// 25.8 5b (d): while step 0 has work or the store is over its cap, a
    /// pass that ends is followed by the next, 100 ms apart); a pass that
    /// changed nothing, a busy store, a closed gate, a parent's store or a
    /// claim that did not drain ends the round. Returns the passes run.
    ///
    /// For the tests and [`Self::schedule_received_passes`]: it takes the
    /// same flag as the scheduled round, so two rounds never run at once
    /// (`0`: one is running).
    pub fn received_passes(&self) -> usize {
        if self.deleting.swap(true, Ordering::SeqCst) {
            return 0;
        }
        let _running = Ticking(&self.deleting);
        self.run_received_passes()
    }

    /// Whether a round of received passes (scheduled or not) holds the
    /// flag now. For the tests.
    #[doc(hidden)]
    pub fn received_round_running(&self) -> bool {
        self.deleting.load(Ordering::SeqCst)
    }

    /// The round, with `deleting` held by the caller.
    fn run_received_passes(&self) -> usize {
        *self.deleted_at.lock().unwrap() = Some(Instant::now());
        // (stop at the next section: the agent is stopping, or a version
        // is on trial, spec 25.1 8)
        let stop = || self.stopping() || super::control::in_trial(&self.cfg.state_dir);
        let attempts = |source: &crate::ids::SourceId| -> anyhow::Result<(bool, Option<i64>)> {
            Ok(self.store.source_use(source)?)
        };
        let mut passes = 0;
        loop {
            if stop() {
                return passes;
            }
            let Some(result) = self.host.received_pass(&attempts, &stop) else {
                return passes;
            };
            passes += 1;
            let report = match result {
                Ok(report) => report,
                Err(e) => {
                    self.note_global(
                        "received_pass_failed",
                        "",
                        json!({ "message": format!("{e:#}") }),
                    );
                    return passes;
                }
            };
            // (25.8 5b: a store that holds a parent's roots is left alone,
            // and said so; the journal keeps one such event while nothing
            // changes)
            // (review round 1, L6: a clone gc that failed, or that
            // `extensions.preciousObjects` keeps from running)
            if let Some(f) = &report.gc_failed {
                self.note_global(
                    "clone_gc_failed",
                    &f.clone,
                    json!({ "message": f.message, "failures": f.failures }),
                );
            }
            if let Some(clone) = &report.gc_ran {
                // (a gc that ran again ends the failure: the next one is
                // news, even with the same message)
                lock(&self.noted).remove(&format!("/clone_gc_failed/{clone}"));
            }
            if report.parent {
                self.note_global("source_store_shared", "", json!({}));
            } else if !report.busy {
                // (a later share is news again)
                lock(&self.noted).remove("/source_store_shared/");
            }
            let progress = report.detached + report.unlinked + report.forgotten + report.refs > 0;
            if !progress || report.busy || report.parent || report.stuck || report.gate.is_some() {
                return passes;
            }
            std::thread::sleep(RECEIVED_PASS_PAUSE);
        }
    }

    /// One retention GC pass ([`Self::gc`]); a failure goes to the journal.
    pub fn collect_garbage(&self) {
        if let Err(e) = self.gc() {
            self.note_global(
                "agent_gc_failed",
                "",
                json!({ "code": e.code, "message": e.message }),
            );
        }
    }

    /// One bounded batch of each deletion of stage 3 that time drives (spec
    /// 25.6 (b), (c2), (e)). Not on trial.
    fn retire_rows(&self) {
        if super::control::in_trial(&self.cfg.state_dir) {
            return;
        }
        let batch = crate::storage::RELEASE_BATCH;
        let at = now();
        let mut ids = Vec::new();
        // The ack wait counts uptime only (spec 25.6 (b)): rows are stamped
        // with the uptime at which the agent first sees them ended, and go
        // once `ack_wait` more of it has passed. No wall clock is read.
        let uptime = self.uptime_ms(false);
        let expired = match uptime {
            Some(u) => self.store.stamp_terminal_uptime(u, batch).and_then(|_| {
                self.store
                    .expire_unreleased(u, at, self.cfg.ack_wait_ms, batch)
            }),
            None => Ok(Vec::new()),
        };
        match expired {
            Ok(v) => ids.extend(v),
            Err(e) => self.note_global(
                "agent_release_failed",
                "ack_wait",
                json!({ "error": e.to_string() }),
            ),
        }
        // Not after this agent accepted an acknowledgement of a parent
        // without the feature: its row stays until the next start, whatever
        // the wall clock does (spec 25.6 (c2)).
        let acked = if self.old_acks.load(Ordering::SeqCst) {
            Ok(Vec::new())
        } else {
            self.store.drop_acked_before(self.started_ms, batch)
        };
        match acked {
            Ok(v) => ids.extend(v),
            Err(e) => self.note_global(
                "agent_release_failed",
                "acked",
                json!({ "error": e.to_string() }),
            ),
        }
        // What a release could not finish (a lock a supervisor still held
        // while it exited) is tried again, a few at a time.
        {
            let mut cursor = lock(&self.dropped_cursor);
            if let Ok(left) = self.store.dropped_attempts_after(&cursor, DROPPED_PER_TICK) {
                *cursor = if left.len() < DROPPED_PER_TICK as usize {
                    String::new()
                } else {
                    left.last().cloned().unwrap_or_default()
                };
                let pass = self.passes.fetch_add(1, Ordering::SeqCst);
                let backoff = lock(&self.dropped_backoff);
                ids.extend(
                    left.into_iter()
                        .filter(|id| backoff.get(id).is_none_or(|(next, _)| pass >= *next)),
                );
            }
        }
        ids.sort();
        ids.dedup();
        // The pass this one took above: a first failure is tried again on
        // the next pass, then after 2, 4, ... passes.
        let pass = self.passes.load(Ordering::SeqCst).saturating_sub(1);
        for id in ids {
            let done = self.remove_dropped(&id);
            let mut backoff = lock(&self.dropped_backoff);
            if done {
                backoff.remove(&id);
            } else {
                let delay = backoff
                    .get(&id)
                    .map_or(1, |(_, d)| (d * 2).min(DROPPED_BACKOFF_MAX));
                backoff.insert(id, (pass + delay, delay));
            }
        }
        // An entry another path took out of the list (a release, a forget)
        // leaves the map too, so it holds only what is still waiting.
        {
            let mut backoff = lock(&self.dropped_backoff);
            if !backoff.is_empty() {
                if let Ok(all) = self.store.dropped_attempts_after("", u32::MAX) {
                    let listed: std::collections::HashSet<String> = all.into_iter().collect();
                    backoff.retain(|id, _| listed.contains(id));
                }
            }
        }
        // Events of no attempt go by uptime too (spec 25.6 (e)): those older
        // than the newest hourly mark `retention` of uptime old.
        let pruned = match uptime {
            Some(u) => self
                .store
                .prune_unowned_events(u, self.cfg.retention_ms, batch),
            None => Ok(0),
        };
        if let Err(e) = pruned {
            self.note_global(
                "agent_release_failed",
                "events",
                json!({ "error": e.to_string() }),
            );
        }
    }

    /// The directory and the lock of an attempt whose row just went, at
    /// once; what fails stays in `dropped_attempt` for the next start.
    fn remove_dropped(&self, id: &str) -> bool {
        // The teardown of a terminal attempt (workspace, placed secrets,
        // scratch) may not have run yet when its member released it: it
        // runs first, under the attempt's shard, as `clean_ended` would.
        if let Ok(a) = AttemptId::parse(id) {
            if a.as_str() == id {
                self.clean(&a);
                // A teardown that failed (a busy workspace, a secret that
                // could not be erased) left its evidence in the directory:
                // keep the directory and the `dropped_attempt` entry, so a
                // later tick (`DROPPED_PER_TICK`) tries the teardown again.
                if self.teardown_left(&a) {
                    self.note_global(
                        "agent_dropped_attempt_kept",
                        id,
                        json!({ "attempt": id, "error": "its cleanup is not finished" }),
                    );
                    return false;
                }
            }
        }
        let mut report = DroppedReport::default();
        let done = remove_dropped_attempt(&self.cfg.attempts_root(), &self.store, id, &mut report);
        for (attempt, error) in &report.failed {
            self.note_global(
                "agent_dropped_attempt_kept",
                attempt,
                json!({ "attempt": attempt, "error": error }),
            );
        }
        if let Err(e) = &done {
            self.note_global(
                "agent_dropped_attempt_kept",
                id,
                json!({ "attempt": id, "error": e.to_string() }),
            );
        }
        done.is_ok() && report.failed.is_empty()
    }

    /// The worker's accumulated uptime now ([`crate::storage::uptime_step`]),
    /// stored at most once a minute (always with `flush`) and marked against
    /// the event journal once an hour. `None` while it cannot be read.
    pub fn uptime_ms(&self, flush: bool) -> Option<u64> {
        let mut g = lock(&self.uptime);
        if g.is_none() {
            let stored = self.store.stored_uptime().ok()?;
            let mark = self.store.last_uptime_mark().ok()?;
            *g = Some((stored, mark));
        }
        let (stored, mark) = g.as_mut()?;
        let (u, write) = crate::storage::uptime_step(
            stored.as_ref(),
            self.cfg.boot_id.as_str(),
            platform::monotonic_ms(),
        )?;
        if write || flush {
            let marked = mark.is_none_or(|m| u.uptime_ms >= m.saturating_add(UPTIME_MARK_MS));
            if self.store.store_uptime(&u, marked).is_ok() {
                *stored = Some(u.clone());
                if marked {
                    *mark = Some(u.uptime_ms);
                }
            }
        }
        Some(u.uptime_ms)
    }

    /// Whether the teardown of `attempt` left something to do again: its
    /// `cleanup-pending` mark or its workspace record.
    fn teardown_left(&self, attempt: &AttemptId) -> bool {
        self.attempt_dir(attempt).join("cleanup-pending").exists()
            || self.workspace_path(attempt).exists()
    }

    /// A release of `attempt` by `member` was committed: its directory and
    /// lock go now, and its record once no submit can be behind it.
    fn released(
        &self,
        member: &CoordinatorId,
        attempt: &AttemptId,
        at: Option<super::release::Taken>,
    ) {
        self.remove_dropped(attempt.as_str());
        let ripe = self.releases.released(member, attempt, at);
        self.forget_ripe(ripe);
    }

    fn forget_ripe(&self, ripe: super::release::Ripe) {
        if let Err(e) = self.store.forget_released(ripe) {
            // Kept: the next start deletes every released record.
            self.note_global(
                "agent_release_failed",
                "records",
                json!({ "error": e.to_string() }),
            );
        }
    }

    /// A connection of `member` was proven (spec 25.6 (c)).
    pub(crate) fn connection_opened(&self, member: &CoordinatorId) -> u64 {
        self.releases.open(member)
    }

    /// A request was taken from connection `conn`.
    pub(crate) fn request_taken(&self, conn: u64, submit: bool) -> super::release::Taken {
        self.releases.take(conn, submit)
    }

    /// A submit taken from a connection was handled.
    pub(crate) fn submit_handled(&self, taken: super::release::Taken) {
        let ripe = self.releases.handled(taken);
        self.forget_ripe(ripe);
    }

    fn forget_ripe_later(&self) {
        let later = std::mem::take(&mut *lock(&self.ripe_later));
        if !later.is_empty() {
            self.forget_ripe(later);
        }
    }

    /// [`Self::submit_handled`] without the database write: the records it
    /// lets go of are forgotten by the next tick.
    pub(crate) fn submit_handled_later(&self, taken: super::release::Taken) {
        let ripe = self.releases.handled(taken);
        if !ripe.is_empty() {
            lock(&self.ripe_later).extend(ripe);
        }
    }

    /// Connection `conn` takes nothing more.
    pub(crate) fn connection_closed(&self, conn: u64) {
        let ripe = self.releases.close(conn);
        self.forget_ripe(ripe);
    }

    /// Released records that still wait for a submit (tests).
    pub fn releases_waiting(&self) -> usize {
        self.releases.waiting()
    }

    fn apply_rekey(self: &Arc<Self>) {
        if super::rekey::pending(&self.cfg.state_dir) {
            let _gate = self.rekey_gate.write().unwrap_or_else(|e| e.into_inner());
            let result = (|| -> Result<(), ApiError> {
                for rec in self.store.list_nonterminal().map_err(api)? {
                    self.store
                        .expire_lease(&rec.attempt_id, now())
                        .map_err(api)?;
                    self.kick(&rec.attempt_id);
                }
                if self.store.list_nonterminal().map_err(api)?.is_empty() {
                    // Advertise the new identity before finish removes the
                    // marker: a helper that sees the marker gone must never
                    // find a connection still offered under the old identity.
                    if let Some(next) =
                        super::rekey::pending_identity(&self.cfg.state_dir, &self.cfg.boot_id)
                            .map_err(|e| invalid(e.to_string()))?
                    {
                        *lock(&self.identity) = next;
                    }
                    super::rekey::finish(
                        &self.cfg.state_dir,
                        &self.cfg.group_key_path,
                        &self.store,
                    )
                    .map_err(|e| invalid(e.to_string()))?;
                    *lock(&self.identity) =
                        super::rekey::identity(&self.cfg.state_dir, &self.cfg.boot_id)
                            .map_err(|e| invalid(e.to_string()))?;
                    self.refresh_group_key();
                    self.group_key.send_modify(|_| {});
                }
                Ok(())
            })();
            if let Err(e) = result {
                self.note_global("send_key_failed", "", json!({"code":e.code}));
            }
        }
    }

    /// Read the group key again. The database records which generation it
    /// is (the single key of before the group is generation 1), and a key
    /// that differs from the one read last closes the connections that were
    /// proven with a key this worker no longer accepts: after a switch only
    /// the new key's proof is taken (spec 24.2). Called at the start and on
    /// every tick, so a switch made by any process of this member is seen.
    /// Returns the current fingerprint, or `None` while the key cannot be
    /// read (no connection is accepted then either).
    pub fn refresh_group_key(&self) -> Option<String> {
        let keys = crate::group::load_keys(&self.cfg.group_key_path).ok()?;
        let current = crate::group::fingerprint(&keys.current);
        if *self.group_key.borrow() != current {
            if let Err(e) = self.store.observe_group_key(&current) {
                self.note_global(
                    "agent_group_key_unrecorded",
                    "",
                    json!({ "error": e.to_string() }),
                );
                return None;
            }
            self.group_key.send_replace(current.clone());
        }
        if let Some(next) = &keys.next {
            // The number of a next key that arrived without one (the
            // distribution of a rotation records its own) follows the
            // current one. Looked at only while a next key lies there.
            if let Ok((Some(have), None)) = self.store.group_keys() {
                let _ = self
                    .store
                    .set_next_group_key(&crate::storage::KeyGeneration {
                        generation: have.generation + 1,
                        fingerprint: crate::group::fingerprint(next),
                    });
            }
        }
        Some(current)
    }

    /// Whether a member told this worker that the identity it answers with
    /// is retired, how many attempts (of any member) are still live, and how
    /// many results the members that still use the identity have not
    /// collected (`None`: not readable now). An ephemeral worker ends itself
    /// on these.
    pub fn retirement(&self) -> (bool, Option<u64>, Option<u64>) {
        let boot = &self.advertised_boot();
        let told = self.store.identity_retired_by_any(boot).unwrap_or(false);
        (
            told,
            self.store.count_nonterminal().ok(),
            self.store.count_uncollected_of_serving_members(boot).ok(),
        )
    }

    /// For an ephemeral worker's graceful end: how many attempts are live,
    /// and how many processes of this worker still run (supervisors, which
    /// release their lock only as they end, and setup builders). `None`: not
    /// readable now, never taken as idle.
    pub fn settling(&self) -> (Option<u64>, Option<u64>) {
        self.reap();
        let spawned = lock(&self.children).len() as u64;
        let builds = self.host.setup_owner().map_or(0, |s| s.running() as u64);
        // The supervisors that still run: the locks of the attempts the
        // database has as not terminal, by their own path (spec 25.6 (b)),
        // never a scan of `supervisors/`. A supervisor only runs for such an
        // attempt; a terminal attempt's supervisor has stored its end.
        let held = (|| {
            let mut held = 0u64;
            for rec in self.store.list_nonterminal().ok()? {
                // Taken for a moment and dropped: only a live supervisor holds it.
                match SupervisorLock::try_acquire_existing(&self.cfg.state_dir, &rec.attempt_id) {
                    Ok(Some(_)) => {}
                    Ok(None) => held += 1,
                    Err(_) => return None,
                }
            }
            Some(held)
        })();
        (
            self.store.count_nonterminal().ok(),
            held.map(|h| h + spawned + builds),
        )
    }

    /// The identity currently published by this worker.
    pub(crate) fn advertised_boot(&self) -> WorkerBootId {
        lock(&self.identity).clone()
    }

    pub(crate) fn check_rekey(&self) -> Result<(), ApiError> {
        if super::rekey::pending(&self.cfg.state_dir) {
            Err(ApiError::new(
                ErrorCode::Conflict,
                "send-key is stopping the old worker identity",
            ))
        } else {
            Ok(())
        }
    }

    /// Key switches and identity resets wake every member connection.
    pub fn group_key_changes(&self) -> tokio::sync::watch::Receiver<String> {
        self.group_key.subscribe()
    }

    /// A member tells this worker that it retired the identity this worker
    /// still answers with (spec 24.3). Only for this worker's own name and
    /// its present identity; anything else is refused and changes nothing.
    /// The member's live attempts are stopped now instead of when their
    /// lease runs out, by the same path (the lease is marked expired, the
    /// supervisor stops the command, the attempt ends `interrupted`), and
    /// the retirement is recorded: this member's connections are refused
    /// for as long as the identity lasts. Other members' attempts,
    /// connections and leases are not touched. The same notice again gives
    /// the same answer.
    fn identity_retired(
        self: &Arc<Self>,
        store: &WorkerStore,
        r: crate::wire::IdentityRetiredRequest,
    ) -> Result<crate::wire::IdentityRetiredResponse, ApiError> {
        if r.worker_id != self.cfg.worker_id
            || r.worker_boot_id != self.advertised_boot()
            || Some(&r.coordinator_id) != store.member().as_ref()
        {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                "the notice does not name this worker's present identity and the member that sends it",
            ));
        }
        let at = now();
        store
            .record_identity_retired(&r.worker_boot_id, at)
            .map_err(api)?;
        for rec in &store.list_nonterminal_of_member().map_err(api)? {
            // Idempotent: the first call writes `lease_expired`, a repeat
            // changes nothing. A renewal is refused from here on.
            store.expire_lease(&rec.attempt_id, at).map_err(api)?;
            self.kick(&rec.attempt_id);
        }
        Ok(crate::wire::IdentityRetiredResponse {
            schema_version: SchemaVersion::CURRENT,
            worker_id: r.worker_id,
            worker_boot_id: r.worker_boot_id,
        })
    }

    /// Whether a diagnostic about `subject` is new (not the one written last).
    fn is_news(&self, subject: String, payload: &Value) -> bool {
        let text = payload.to_string();
        let mut noted = lock(&self.noted);
        if noted.get(&subject) == Some(&text) {
            return false;
        }
        if noted.len() >= 4096 {
            noted.clear();
        }
        noted.insert(subject, text);
        true
    }

    /// A diagnostic event of an attempt, written once while it repeats.
    fn note_once(&self, attempt: &AttemptId, kind: &str, payload: Value) {
        if !self.is_news(format!("{attempt}/{kind}"), &payload) {
            return;
        }
        if let Ok(Some(rec)) = self.store.get(attempt) {
            note(&self.store, &rec, kind, payload);
        }
    }

    /// A diagnostic event of no attempt, written once while it repeats.
    /// A toolchain question that took long, or tools that kept an earlier
    /// answer because their last probe gave none, go to the journal. The time
    /// is a bucket, so one event stands for a stretch of the same slowness.
    fn note_slow_resolution(&self, took: Duration, held: &[String]) {
        if !held.is_empty() {
            self.note_global("agent_toolchain_probe_held", "", json!({ "tools": held }));
        }
        let Some(at_least_ms) = [5_000u64, 1_000, 250]
            .into_iter()
            .find(|ms| took >= Duration::from_millis(*ms))
        else {
            return;
        };
        self.note_global(
            "agent_toolchain_resolution_slow",
            "",
            json!({ "at_least_ms": at_least_ms }),
        );
    }
    fn note_global(&self, kind: &str, subject: &str, payload: Value) {
        if self.is_news(format!("/{kind}/{subject}"), &payload) {
            let _ = self.store.append_event(None, None, kind, &payload, now());
        }
    }

    /// A retire of an attempt that may have run and that nobody can watch or
    /// recover: once every launch of it is known to be from another boot of
    /// the machine, nothing of it runs any more and it is fixed as
    /// interrupted. On the same boot it stays unknown.
    fn settle_lost(&self, attempt: &AttemptId) -> Result<(), ApiError> {
        let Some(rec) = self.store.get(attempt).map_err(api)? else {
            return Ok(());
        };
        if rec.state == AttemptState::Terminal {
            return Ok(());
        }
        let lost = lock(&self.gave_up).contains(attempt.as_str())
            || recovery_of(&self.cfg, &self.store, &rec, false)? == Recovery::Unresolved;
        if !lost {
            return Ok(());
        }
        let launches = self.store.launches(attempt).map_err(api)?;
        let boots: Vec<Option<&str>> = launches
            .iter()
            .map(|(_, _, ident)| ident.as_ref().map(|i| i.boot_id.as_str()))
            .collect();
        let rebooted = policy::launched_on_another_boot(&boots, self.cfg.boot_id.as_str())
            || !self
                .store
                .events_of(attempt, "agent_boot_changed")
                .map_err(api)?
                .is_empty();
        if !rebooted {
            return Ok(());
        }
        // A look by a watcher holds the supervisor lock for an instant; a
        // living supervisor holds it for good.
        for _ in 0..3 {
            let t = termination(None, None);
            if settle(
                &self.cfg,
                &self.store,
                attempt,
                t,
                Settle::Interrupted,
                true,
            )? {
                note(&self.store, &rec, "agent_settled_lost", json!({}));
                lock(&self.gave_up).remove(attempt.as_str());
                self.clean(attempt);
                break;
            }
            self.pause(self.cfg.poll_ms);
        }
        Ok(())
    }

    // ---------------------------------------------------------- operations

    /// Answer one request. Mutations are committed before this returns.
    pub fn handle(
        self: &Arc<Self>,
        session: Session,
        request: Request,
    ) -> Result<Response, ApiError> {
        self.handle_identity(session, request, None)
    }

    pub(crate) fn handle_identity(
        self: &Arc<Self>,
        session: Session,
        request: Request,
        expected: Option<&WorkerBootId>,
    ) -> Result<Response, ApiError> {
        self.handle_linked(session, request, expected, Link::default())
    }

    /// [`Self::handle`] for a request taken from a connection: whether the
    /// connection agreed `attempt_release_v1`, and where the request was
    /// taken (spec 25.6).
    pub fn handle_linked(
        self: &Arc<Self>,
        session: Session,
        request: Request,
        expected: Option<&WorkerBootId>,
        link: Link,
    ) -> Result<Response, ApiError> {
        let _gate = self.rekey_gate.read().unwrap_or_else(|e| e.into_inner());
        self.check_rekey()?;
        if expected.is_some_and(|boot| boot != &self.advertised_boot()) {
            return Err(invalid("connection belongs to the old worker identity"));
        }
        request
            .ensure_current_schema()
            .map_err(|e| ApiError::new(ErrorCode::ProtocolIncompatible, e.to_string()))?;
        // A version on trial promises nothing: whoever asks for a change
        // sends it again after the commit or the rollback (§24.8).
        if super::control::in_trial(&self.cfg.state_dir) && changes_state(&request) {
            return Err(ApiError::new(
                ErrorCode::Overloaded,
                "this worker is trying a new version; send the request again later",
            ));
        }
        // The member of this connection: what it names by attempt id is its
        // own attempt, and another member's is not found.
        let store = &self.store.for_member(&session.coordinator_id);
        match request {
            Request::Hello(_) => Err(invalid("hello is the first request of a connection")),
            Request::GroupProof(_) => Err(invalid(
                "the group proof follows hello, once per connection",
            )),
            Request::IdentityRetired(r) => self
                .identity_retired(store, r)
                .map(Response::IdentityRetired),
            Request::Inspect(r) => self.inspect(store, r).map(Response::Inspect),
            Request::SubmitAttempt(r) => self
                .submit(store, r, link.input_missing)
                .map(Response::SubmitAttempt),
            Request::GetAttempt(r) => match self.known(store, &r.attempt_id) {
                Ok(rec) => self.status(&rec).map(Response::GetAttempt),
                Err(e) if e.code == ErrorCode::NotFound => {
                    // Spec 25.6 (c): of an attempt that went, the record
                    // says how it ended, and nothing else.
                    match store.attempt_record(&r.attempt_id).map_err(api)? {
                        Some(rec) => Ok(Response::GetAttempt(self.recorded_status(rec)?)),
                        None => Err(e),
                    }
                }
                Err(e) => Err(e),
            },
            Request::ReleaseAttempt(r) => {
                if !link.release {
                    return Err(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "attempt_release_v1 was not negotiated",
                    ));
                }
                self.release(store, &session, r, link.taken)
                    .map(Response::ReleaseAttempt)
            }
            Request::ListUnreleased(r) => {
                if !link.release {
                    return Err(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "attempt_release_v1 was not negotiated",
                    ));
                }
                let (attempts, more) = store
                    .list_unreleased(r.after.as_ref(), r.limit)
                    .map_err(api)?;
                Ok(Response::ListUnreleased(
                    crate::wire::ListUnreleasedResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempts,
                        more,
                    },
                ))
            }
            Request::ListAttempts(r) => self.list(store, r).map(Response::ListAttempts),
            Request::WorkflowArtifact(r) => {
                let rec = self.known(store, &r.attempt_id)?;
                Self::generation(&rec, r.generation)?;
                if r.coordinator_epoch != session.coordinator_epoch
                    || r.coordinator_epoch < store.max_coordinator_epoch().map_err(api)?
                {
                    return Err(ApiError::new(
                        ErrorCode::StaleEpoch,
                        "artifact exchange from a stale coordinator",
                    ));
                }
                if rec.retired
                    || rec.cancel_requested_at_ms.is_some()
                    || rec.state == AttemptState::Terminal
                {
                    return Err(ApiError::new(
                        ErrorCode::Conflict,
                        "artifact attempt is no longer live",
                    ));
                }
                let root = self
                    .cfg
                    .attempts_root()
                    .join(r.attempt_id.as_str())
                    .join("ci")
                    .join("exchange");
                crate::ci::artifact_exchange::respond(&root, r.command)
                    .map(|result| {
                        Response::WorkflowArtifact(crate::wire::WorkflowArtifactResponse {
                            schema_version: SchemaVersion::CURRENT,
                            result,
                        })
                    })
                    .map_err(|e| invalid(e.to_string()))
            }
            Request::RenewLease(r) => self.renew(store, r).map(Response::RenewLease),
            Request::CancelAttempt(r) => self.cancel(store, r).map(Response::CancelAttempt),
            Request::RetireAttempt(r) => self.retire(store, r).map(Response::RetireAttempt),
            Request::WatchEvents(r) => self.watch(store, r).map(Response::WatchEvents),
            Request::ReadLog(r) => self.read_log(store, r).map(Response::ReadLog),
            Request::GetResult(r) => self.result(store, r).map(Response::GetResult),
            Request::AckCollected(r) => self
                .ack(store, &session, r, link)
                .map(Response::AckCollected),
            Request::SecretBundlePut(r) => self.secrets(store, r).map(Response::SecretBundlePut),
            Request::BlobPut(r) => {
                let (host, limits) = (self.host.as_ref(), &self.cfg.blobs);
                blobs::put(store, host, limits, &self.cfg.state_dir, &r)
                    .map(Response::BlobPut)
                    .map_err(|e| unless_agreed(e, link.input_missing))
            }
            Request::BlobVerify(r) => {
                blobs::verify(self.host.as_ref(), &self.cfg.blobs, &r).map(Response::BlobVerify)
            }
            Request::BlobGet(r) => {
                self.known(store, &r.attempt_id)?;
                blobs::get(store, &self.cfg.attempts_root(), &r).map(Response::BlobGet)
            }
            Request::Forecast(r) => self.forecast(store, r).map(Response::Forecast),
            Request::ForecastCommand(r) => {
                let attempts = self.store.list_nonterminal().map_err(api)?;
                let max_epoch = store.max_coordinator_epoch().map_err(api)?;
                let ident = crate::forecast::plan::Ident {
                    worker_id: self.cfg.worker_id.clone(),
                    boot_id: self.advertised_boot(),
                    agent_epoch: self.epoch,
                    forecast_ttl_ms: self.cfg.forecast_ttl_ms,
                };
                crate::forecast::answer_worker_command(
                    &self.cfg.traffic_state_dir,
                    self.cfg.traffic_config.as_deref(),
                    &self.cfg.state_dir,
                    self.cfg
                        .capability_profile
                        .as_ref()
                        .map(|_| &self.cfg.profile_fingerprint),
                    &ident,
                    crate::forecast::AcceptedWork {
                        attempts: &attempts,
                        installs: &|payload| self.pending_tool_installs(payload),
                    },
                    max_epoch,
                    &r,
                )
                .map(Response::ForecastCommand)
            }
            Request::PreparationProbe(r) => {
                use crate::setup::preparation_probe::{
                    PreparationProbe, PreparationProbeInput, ProbeUnknown,
                };
                let probe = if r.expected_profile_fingerprint != self.cfg.profile_fingerprint {
                    PreparationProbe::Unknown(ProbeUnknown::InvalidBinding)
                } else {
                    let input = PreparationProbeInput {
                        manifest_digest: &r.manifest_digest,
                        source_id: &r.source_id,
                        run_id: &r.run_id,
                        setup: &r.setup,
                        frozen: &r.frozen,
                        in_place: r.in_place,
                    };
                    match crate::setup::preparation_probe::input_budget(&input) {
                        Ok(()) => self.probe_preparation(input),
                        Err(reason) => PreparationProbe::Unknown(reason),
                    }
                };
                Ok(Response::PreparationProbe(
                    crate::wire::PreparationProbeResponse {
                        schema_version: SchemaVersion::CURRENT,
                        worker_id: self.cfg.worker_id.clone(),
                        worker_boot_id: self.advertised_boot(),
                        agent_epoch: self.epoch,
                        probe,
                    },
                ))
            }
            Request::ResolveToolchains(r) => {
                let bundle = self.resolve_toolchains(r.bundle, &r.requirements)?;
                Ok(Response::ResolveToolchains(
                    crate::wire::ResolveToolchainsResponse {
                        schema_version: SchemaVersion::CURRENT,
                        bundle,
                    },
                ))
            }
            Request::EnvSync(r) => self.env_sync(store, r).map(Response::EnvSync),
            Request::EnvReport(r) => self.env_report(r).map(Response::EnvReport),
        }
    }

    /// The record, or why there is none: `RETIRED` for a tombstone without an
    /// attempt, else `NOT_FOUND`.
    fn known(&self, store: &WorkerStore, attempt: &AttemptId) -> Result<AcceptedAttempt, ApiError> {
        if let Some(rec) = store.get(attempt).map_err(api)? {
            return Ok(rec);
        }
        if store.tombstone(attempt).map_err(api)?.is_some() {
            return Err(ApiError::new(
                ErrorCode::Retired,
                format!("attempt {attempt} has a tombstone and was never accepted"),
            ));
        }
        Err(ApiError::new(
            ErrorCode::NotFound,
            format!("attempt {attempt} is unknown"),
        ))
    }

    fn generation(rec: &AcceptedAttempt, got: Generation) -> Result<(), ApiError> {
        if rec.generation == got {
            Ok(())
        } else {
            Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "attempt {} is generation {}, not {}",
                    rec.attempt_id, rec.generation.0, got.0
                ),
            ))
        }
    }

    fn submit(
        self: &Arc<Self>,
        store: &WorkerStore,
        r: SubmitAttemptRequest,
        input_missing: bool,
    ) -> Result<SubmitAttemptResponse, ApiError> {
        // The claimed hash is the sender's word: recompute it.
        if !r.hash_matches().map_err(|e| invalid(e.to_string()))? {
            return Err(invalid("payload_hash does not match the payload"));
        }
        // One acceptance at a time, so the cap and the insert are one step.
        let one = lock(&self.submitting);
        // A resend of an accepted attempt is answered with what is there
        // (spec 11, idempotency): its payload was checked when it was
        // accepted, and it adds no load. The store still compares the hash.
        // A late copy of a deleted attempt is answered by its dedup record
        // (RETIRED, in the store's step below), never by the checks of a
        // new acceptance (spec 25.6 (c)).
        let fresh = store.get(&r.attempt_id).map_err(api)?.is_none()
            && store.tombstone(&r.attempt_id).map_err(api)?.is_none()
            && store.attempt_record(&r.attempt_id).map_err(api)?.is_none();
        let mut held = None;
        if fresh {
            policy::check_payload(&r.payload, self.cfg.poll_ms).map_err(invalid)?;
            super::ci_task::check_accepted(&self.cfg.state_dir, &r.payload)?;
            self.check_capabilities(&r.payload)?;
            self.host.admit(&r.payload)?;
            for c in &r.payload.commands {
                check_cwd_relative(Path::new(&c.cwd))?;
            }
            // A setup the builder would refuse is refused here, as bad
            // input, not later as a preparation that failed.
            if let Some(setup) = &r.payload.setup {
                crate::setup::policy::validate(setup).map_err(invalid)?;
            }
            // The source store stays as it is from here to the commit: no
            // deleting step runs while the shared lock is held, and a busy
            // store is answered at once (spec 25.8 5b (a)).
            held = self.host.hold_source()?;
            // A source that is not here refuses the attempt, nothing of it
            // stored (spec 25.8 5b (b)), whatever the payload declares: the
            // parent delivers it and sends the same attempt again. A parent
            // that did not agree `input_missing_v1` cannot read the code: its
            // attempt is taken as before and ends at the preparation.
            if input_missing {
                if let Some(guard) = &held {
                    if !self.host.source_present(guard, &r.payload)? {
                        return Err(ApiError::input_missing(
                            r.payload.source_id.to_string(),
                            Vec::new(),
                            format!(
                                "source {} is not in this worker's store; deliver it and send again",
                                r.payload.source_id
                            ),
                        ));
                    }
                }
            }
            // Against the secret files of its source, when that source is
            // already here. When it is not, the same check runs before the
            // supervisor's spec is written ([`Self::prepare`]): a
            // declaration that names a secret file never starts a command.
            if !r.payload.artifacts.is_empty() {
                // Only a source that is not here yet (`None`) waits for the
                // later check. One that is here with another manifest than
                // the payload names, or a store that cannot be read, is the
                // answer: nothing is accepted.
                if let Some(secrets) = self.host.secret_paths(&r.payload, false, held.as_ref())? {
                    check_artifacts_not_secret(&r.payload, &secrets)?;
                }
            }
            let live = store.count_nonterminal().map_err(api)?;
            if policy::at_live_cap(live, self.cfg.max_live_attempts) {
                return Err(ApiError::new(
                    ErrorCode::Overloaded,
                    format!("{live} attempts are not terminal on this worker; ask again later"),
                ));
            }
        }
        // A draining worker takes no new attempt, whatever the placement said
        // (stop drains, then counts what is live, then boots the service out).
        // Checked under the acceptance lock, just before the insert; a resend
        // of an accepted attempt was answered above and is unaffected.
        if fresh
            && (super::control::Control::effective(&self.cfg.state_dir).drain
                || super::control::held(&self.cfg.state_dir))
        {
            return Err(ApiError::new(
                ErrorCode::Overloaded,
                "this worker is draining; no new attempt is accepted",
            ));
        }
        let mut spec = serde_json::to_value(&r.payload).map_err(|e| internal("payload", e))?;
        if self.cfg.capability_profile.is_some() {
            spec.as_object_mut().expect("payload object").insert(
                "_worker_profile_fingerprint".into(),
                serde_json::to_value(&self.cfg.profile_fingerprint)
                    .map_err(|e| internal("profile identity", e))?,
            );
        }
        // Taken before the commit: the first window never counts from later
        // than the acceptance.
        let accepted = GrantStamp::now();
        let outcome = store
            .submit_within(
                NewAcceptance {
                    attempt_id: r.attempt_id.clone(),
                    run_id: r.payload.run_id.clone(),
                    task_id: r.payload.task_id.clone(),
                    generation: r.payload.generation,
                    payload_hash: r.payload_hash,
                    coordinator_epoch: r.coordinator_epoch,
                    spec,
                    lease: Some(AcceptedLease {
                        lease_ms: r.payload.lease_ms,
                        poll_ms: self.cfg.poll_ms,
                        at: accepted.clone(),
                    }),
                },
                Some(policy::window(
                    self.cfg.max_active_task,
                    self.cfg.prefetch_per_worker,
                )),
                now(),
            )
            .map_err(api)?;
        // The acceptance is committed; only now may anything start. The
        // source was used: its time, under the same shared lock.
        if let (Some(held), SubmitOutcome::Accepted(_)) = (&held, &outcome) {
            self.host.source_used(held, &r.payload);
        }
        drop(held);
        drop(one);
        let (rec, duplicate) = match outcome {
            SubmitOutcome::Accepted(rec) => {
                // The persisted acceptance stamp: the renewal check counts
                // the first window from it, and the seed from this.
                let at = accepted.mono_ms;
                lock(&self.clocks).insert(
                    rec.attempt_id.to_string(),
                    Clock {
                        accepted_ms: at,
                        granted_ms: at,
                    },
                );
                (rec, false)
            }
            SubmitOutcome::Existing(rec) => (rec, true),
        };
        if rec.state != AttemptState::Terminal {
            self.kick(&rec.attempt_id);
        }
        Ok(SubmitAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: rec.attempt_id,
            generation: rec.generation,
            state: rec.state,
            receipts: rec.receipts,
            duplicate,
        })
    }

    /// Why a terminal attempt whose command never started ended: what the
    /// preparation said, bounded.
    fn failure_reason(&self, rec: &AcceptedAttempt) -> Option<String> {
        if rec.state != AttemptState::Terminal || rec.receipts.at_ms(Receipt::Running).is_some() {
            return None;
        }
        let events = self
            .store
            .events_of(&rec.attempt_id, "agent_prepare_failed")
            .ok()?;
        let payload = &events.last()?.payload;
        let field = |k: &str| payload.get(k).and_then(Value::as_str).unwrap_or("");
        Some(bounded_reason(&format!(
            "agent_prepare_failed: {} {}",
            field("code"),
            field("message")
        )))
    }

    fn status(&self, rec: &AcceptedAttempt) -> Result<AttemptStatus, ApiError> {
        let a = &rec.attempt_id;
        let out = self.store.log_position(a, "stdout").map_err(api)?;
        let err = self.store.log_position(a, "stderr").map_err(api)?;
        let dropped = out.dropped_bytes.saturating_add(err.dropped_bytes);
        let gave_up = lock(&self.gave_up).contains(a.as_str());
        let recovery = recovery_of(&self.cfg, &self.store, rec, false)?;
        Ok(AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: rec.run_id.clone(),
            task_id: rec.task_id.clone(),
            generation: rec.generation,
            worker_boot_id: self.advertised_boot(),
            agent_epoch: self.epoch,
            state: rec.state,
            receipts: rec.receipts,
            spawned_at_ms: rec.receipts.at_ms(Receipt::Running),
            observation: policy::observation(rec.state, gave_up, recovery),
            termination: rec.termination,
            nodes: Vec::new(),
            steps: Vec::new(),
            retired: rec.retired,
            log_truncated: dropped > 0,
            log_dropped_bytes: dropped,
            failure_reason: self.failure_reason(rec),
        })
    }

    fn list(
        &self,
        store: &WorkerStore,
        r: ListAttemptsRequest,
    ) -> Result<ListAttemptsResponse, ApiError> {
        // One more than fits: an answer that would be cut is an error, the
        // caller narrows `states` instead of trusting a part of the list.
        let cap = self.cfg.max_list_attempts;
        let recs = store
            .list_attempts(
                r.states.as_deref(),
                r.include_terminal,
                cap.saturating_add(1),
            )
            .map_err(api)?;
        if !policy::fits(recs.len(), cap) {
            return Err(invalid(format!(
                "more than {cap} attempts match; narrow `states`"
            )));
        }
        let mut attempts = Vec::with_capacity(recs.len());
        for rec in recs {
            attempts.push(self.status(&rec)?);
        }
        Ok(ListAttemptsResponse {
            schema_version: SchemaVersion::CURRENT,
            attempts,
        })
    }

    fn renew(
        &self,
        store: &WorkerStore,
        r: RenewLeaseRequest,
    ) -> Result<RenewLeaseResponse, ApiError> {
        let rec = self.known(store, &r.attempt_id)?;
        Self::generation(&rec, r.generation)?;
        // The new key and the time of its grant change as one step for a
        // start that measures the lease (`lease_step`).
        let renewing = lock(&self.lease_step);
        // The store applies it only in time after the grant before it, as
        // persisted (spec 12): a supervisor counts the lease from the last
        // grant however late it looks, so a late grant would revive it.
        let stamp = GrantStamp::now();
        let mono = stamp.mono_ms;
        let outcome = store
            .renew_lease(&r.attempt_id, r.key(), now(), stamp)
            .map_err(api)?;
        let extended = outcome == LeaseOutcome::Extended;
        if extended {
            self.at(ProbePoint::LeaseRenewed, &r.attempt_id);
            if let Some(c) = lock(&self.clocks).get_mut(r.attempt_id.as_str()) {
                c.granted_ms = mono;
            }
        }
        drop(renewing);
        // Only what the response reports; the check above does not use it.
        let lease_ms = serde_json::from_value::<AttemptPayload>(rec.spec)
            .map(|p| p.lease_ms)
            .unwrap_or(0);
        Ok(RenewLeaseResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            lease_seq: r.lease_seq,
            extended,
            // The supervisor counts the lease on its own monotonic clock; a
            // renewal that extended starts a full period from its grant.
            lease_remaining_ms: if extended { lease_ms } else { 0 },
            state: rec.state,
        })
    }

    fn cancel(
        self: &Arc<Self>,
        store: &WorkerStore,
        r: CancelAttemptRequest,
    ) -> Result<CancelAttemptResponse, ApiError> {
        if let Some(rec) = store.get(&r.attempt_id).map_err(api)? {
            Self::generation(&rec, r.generation)?;
        }
        // Persisted first; the supervisor (or the watcher, before a
        // supervisor exists) acts on it.
        let outcome = store
            .cancel(&r.attempt_id, r.coordinator_epoch, now())
            .map_err(api)?;
        let (state, tombstone) = match outcome {
            CancelOutcome::Requested(rec) | CancelOutcome::AlreadyRequested(rec) => {
                if rec.state != AttemptState::Terminal {
                    self.kick(&rec.attempt_id);
                }
                (Some(rec.state), false)
            }
            CancelOutcome::Tombstoned => (None, true),
        };
        Ok(CancelAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            phase: policy::cancel_phase(state),
            state,
            tombstone,
        })
    }

    fn retire(
        self: &Arc<Self>,
        store: &WorkerStore,
        r: RetireAttemptRequest,
    ) -> Result<RetireAttemptResponse, ApiError> {
        let a = &r.attempt_id;
        let before = store.get(a).map_err(api)?;
        if let Some(rec) = &before {
            Self::generation(rec, r.generation)?;
        }
        let was_terminal = before
            .as_ref()
            .is_some_and(|b| b.state == AttemptState::Terminal);
        // The tombstone is durable before anything else: a submit that
        // arrives late is refused from here on.
        let known = store
            .retire(a, r.coordinator_epoch, now())
            .map_err(api)?
            .is_some();
        let mut now_terminal = was_terminal;
        if known && !was_terminal {
            // Before the watcher is woken: it takes the supervisor lock for
            // a moment at each look.
            self.settle_lost(a)?;
            self.kick(a);
            let mut waited = 0;
            loop {
                now_terminal = matches!(
                    store.get(a).map_err(api)?,
                    Some(rec) if rec.state == AttemptState::Terminal
                );
                if now_terminal || waited >= self.cfg.retire_wait_ms || self.stopping() {
                    break;
                }
                let step = self.cfg.poll_ms.clamp(1, 50);
                std::thread::sleep(Duration::from_millis(step));
                waited += step;
            }
        }
        let had_launches = known && !store.launches(a).map_err(api)?.is_empty();
        Ok(RetireAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            tombstone: true,
            status: policy::retire_status(known, was_terminal, now_terminal, had_launches),
        })
    }

    fn watch(
        &self,
        store: &WorkerStore,
        r: WatchEventsRequest,
    ) -> Result<WatchEventsResponse, ApiError> {
        // The wire `seq` is per attempt, so one cursor follows one attempt.
        let Some(a) = r.attempt_id else {
            return Err(invalid("watch_events needs an attempt_id"));
        };
        self.known(store, &a)?;
        let scan = self.cfg.max_event_scan;
        let journal = store
            .events_of_attempt(&a, scan.saturating_add(1))
            .map_err(api)?;
        if !policy::fits(journal.len(), scan) {
            // Numbering what was read would renumber the events behind it.
            return Err(internal(
                "watch_events",
                format!("the journal of attempt {a} has more than {scan} entries"),
            ));
        }
        let wire: Vec<_> = journal
            .iter()
            .map(|e| policy::wire_event(&e.kind, &e.payload))
            .collect();
        let is_wire: Vec<bool> = wire.iter().map(Option::is_some).collect();
        let mut next_seq = r.after_seq;
        let mut events = Vec::new();
        for (seq, i) in policy::select_events(&is_wire, r.after_seq, r.limit) {
            let w = wire[i].expect("selected entries are wire events");
            next_seq = seq;
            events.push(AttemptEvent {
                seq,
                attempt_id: a.clone(),
                kind: w.kind,
                state: w.state,
                receipt: w.receipt,
                node_id: None,
                step: None,
            });
        }
        Ok(WatchEventsResponse {
            schema_version: SchemaVersion::CURRENT,
            events,
            next_seq,
        })
    }

    fn read_log(
        &self,
        store: &WorkerStore,
        r: ReadLogRequest,
    ) -> Result<ReadLogResponse, ApiError> {
        let rec = self.known(store, &r.attempt_id)?;
        let reader = match SpoolReader::open(&self.cfg.attempts_root(), &r.attempt_id) {
            Ok(reader) => reader,
            // Nothing was written yet. Only an ended attempt has no more to
            // come.
            Err(SpoolError::NotFound(_)) => {
                return Ok(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: r.offset,
                    data: Bytes(Vec::new()),
                    next_offset: r.offset,
                    eof: rec.state == AttemptState::Terminal,
                    log_truncated: false,
                    dropped_bytes: 0,
                })
            }
            Err(e) => return Err(internal("log spool", e)),
        };
        let chunk = reader
            .read(r.stream, r.offset, r.max_bytes)
            .map_err(|e| internal("log spool", e))?;
        Ok(ReadLogResponse {
            schema_version: SchemaVersion::CURRENT,
            offset: chunk.offset,
            data: Bytes(chunk.data),
            next_offset: chunk.next_offset,
            eof: chunk.eof,
            log_truncated: chunk.log_truncated,
            dropped_bytes: chunk.dropped_bytes,
        })
    }

    /// The ended attempt's result, its digest, and whether a result is stored.
    /// The stored result (written
    /// by the supervisor) is the truth when there is one; an attempt that
    /// ended without one (settled by the agent or by recovery) has its
    /// termination and nothing else.
    fn ended(
        &self,
        store: &WorkerStore,
        attempt: &AttemptId,
        generation: Generation,
    ) -> Result<Ended, ApiError> {
        let rec = self.known(store, attempt)?;
        Self::generation(&rec, generation)?;
        let Some(t) = rec.termination else {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("attempt {attempt} has not ended"),
            ));
        };
        let stored = store.get_result(attempt).map_err(api)?;
        let artifacts: Vec<ArtifactRecord> = match &stored {
            Some(v) => serde_json::from_value(v["artifacts"].clone())
                .map_err(|e| internal("stored artifacts", e))?,
            None => Vec::new(),
        };
        // Unreadable is never "all were stored".
        let not_stored = match &stored {
            Some(v) => result_not_stored(v).ok_or_else(|| {
                internal("stored result", "its unstored artifacts cannot be read")
            })?,
            None => Vec::new(),
        };
        let has_result = stored.is_some();
        let value = stored.unwrap_or_else(|| json!({ "termination": t, "artifacts": [] }));
        let digest = payload_hash(&value).map_err(|e| internal("result digest", e))?;
        Ok((t, artifacts, not_stored, digest, has_result))
    }

    fn result(
        &self,
        store: &WorkerStore,
        r: GetResultRequest,
    ) -> Result<GetResultResponse, ApiError> {
        let (termination, artifacts, artifacts_not_stored, result_digest, _) =
            self.ended(store, &r.attempt_id, r.generation)?;
        Ok(GetResultResponse {
            outputs_evaluated: store
                .get_result(&r.attempt_id)
                .map_err(api)?
                .is_some_and(|v| v["outputs_evaluated"] == true),
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            generation: r.generation,
            termination,
            nodes: Vec::new(),
            artifacts,
            artifacts_not_stored,
            result_digest,
        })
    }

    fn ack(
        &self,
        store: &WorkerStore,
        session: &Session,
        r: AckCollectedRequest,
        link: Link,
    ) -> Result<AckCollectedResponse, ApiError> {
        let (_, _, _, digest, has_result) = self.ended(store, &r.attempt_id, r.generation)?;
        if digest != r.result_digest {
            // The parent stored something else than what is here: the
            // protection stays.
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "result_digest is not the digest of this attempt's result",
            ));
        }
        // An attempt without a result can still get one while a supervisor
        // of it lives, and a result that arrives after the acknowledgement
        // is not protected. The supervisor holds its lock for as long as it
        // may store one: the acknowledgement is recorded under that lock,
        // or not now.
        let _no_supervisor = if has_result {
            None
        } else {
            match SupervisorLock::try_acquire_existing(&self.cfg.state_dir, &r.attempt_id) {
                Ok(Some(lock)) => Some(lock),
                Ok(None) => {
                    return Err(ApiError::new(
                        ErrorCode::Overloaded,
                        "a supervisor of the attempt may still store its result; acknowledge again later",
                    ))
                }
                Err(e) => return Err(internal("supervisor lock", e)),
            }
        };
        // A result that is stored between that read and this write is
        // another result than the acknowledged one: refused like a digest
        // that does not match.
        if link.release {
            // Spec 25.6 (b): the member reads no more of it. The attempt
            // goes in the same transaction, a released record stays.
            store
                .ack_and_release(&r.attempt_id, session.coordinator_epoch, now(), has_result)
                .map_err(api)?;
            drop(_no_supervisor);
            self.released(&session.coordinator_id, &r.attempt_id, link.taken);
        } else {
            self.old_acks.store(true, Ordering::SeqCst);
            store
                .ack_collected_as_read(&r.attempt_id, session.coordinator_epoch, now(), has_result)
                .map_err(api)?;
        }
        Ok(AckCollectedResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            released: true,
        })
    }

    /// `release_attempt` (spec 25.6 (a)).
    fn release(
        &self,
        store: &WorkerStore,
        session: &Session,
        r: crate::wire::ReleaseAttemptRequest,
        taken: Option<super::release::Taken>,
    ) -> Result<crate::wire::ReleaseAttemptResponse, ApiError> {
        // A terminal attempt without a result can still get one while a
        // supervisor of it lives: released under its lock, as an
        // acknowledgement is, or not now.
        let rec = store.get(&r.attempt_id).map_err(api)?;
        let needs_lock = rec.as_ref().is_some_and(|rec| {
            rec.state == AttemptState::Terminal
                && !matches!(store.get_result(&r.attempt_id), Ok(Some(_)))
        });
        let _no_supervisor = if needs_lock {
            match SupervisorLock::try_acquire_existing(&self.cfg.state_dir, &r.attempt_id) {
                Ok(Some(lock)) => Some(lock),
                Ok(None) => {
                    return Err(ApiError::new(
                        ErrorCode::Overloaded,
                        "a supervisor of the attempt may still store its result; release it again later",
                    ))
                }
                Err(e) => return Err(internal("supervisor lock", e)),
            }
        } else {
            None
        };
        let done = store
            .release_attempt(&r.attempt_id, session.coordinator_epoch, now())
            .map_err(api)?;
        drop(_no_supervisor);
        let _ = done;
        self.released(&session.coordinator_id, &r.attempt_id, taken);
        Ok(crate::wire::ReleaseAttemptResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
        })
    }

    /// What `get_attempt` answers for an attempt that left only a record.
    fn recorded_status(&self, rec: crate::storage::DedupRecord) -> Result<AttemptStatus, ApiError> {
        let corrupt = |what: &str, e: String| internal(what, e);
        Ok(AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: rec.attempt_id,
            run_id: RunId::parse(&rec.run_id)
                .map_err(|e| corrupt("record run id", e.to_string()))?,
            task_id: TaskId::parse(&rec.task_id)
                .map_err(|e| corrupt("record task id", e.to_string()))?,
            generation: Generation(rec.generation),
            worker_boot_id: self.advertised_boot(),
            agent_epoch: self.epoch,
            state: AttemptState::Terminal,
            receipts: crate::state::Receipts::default(),
            spawned_at_ms: None,
            observation: policy::observation(AttemptState::Terminal, false, Recovery::Nothing),
            termination: Some(rec.termination.unwrap_or_else(failed)),
            nodes: Vec::new(),
            steps: Vec::new(),
            retired: false,
            log_truncated: false,
            log_dropped_bytes: 0,
            failure_reason: None,
        })
    }

    fn secrets(
        &self,
        store: &WorkerStore,
        r: SecretBundlePutRequest,
    ) -> Result<SecretBundlePutResponse, ApiError> {
        // The cleanup of an ended attempt takes the same lock and runs only
        // after the end is in the database: read under the lock, the state
        // either still allows the placement (the cleanup then erases what is
        // placed) or it shows the end and nothing is placed.
        let _one = lock(shard(&self.shards, &r.attempt_id));
        let rec = self.known(store, &r.attempt_id)?;
        Self::generation(&rec, r.generation)?;
        let ws = self.workspace_of(&r.attempt_id);
        let stop = rec.retired || rec.cancel_requested_at_ms.is_some();
        let ws = match (policy::secret_put(rec.state, stop, ws.is_some()), ws) {
            (policy::SecretPut::Place, Some(ws)) => ws,
            (policy::SecretPut::Stopping, _) => {
                return Err(ApiError::new(
                    ErrorCode::Retired,
                    "the attempt is being stopped",
                ))
            }
            (policy::SecretPut::TooLate, _) => {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!(
                        "attempt {} is {:?}: secrets go in before the start",
                        rec.attempt_id, rec.state
                    ),
                ))
            }
            // Not `OVERLOADED` (the worker is not refusing load) and not
            // `CONFLICT` (that one is final): what the bundle goes into does
            // not exist yet. The sender sends it again.
            (policy::SecretPut::NotReady, _) | (policy::SecretPut::Place, None) => {
                return Err(ApiError::new(
                    ErrorCode::NotFound,
                    "the workspace is not prepared yet; send the bundle again",
                ))
            }
        };
        let placed = self.host.place_secrets(&rec, &ws, &r.files)?;
        Ok(SecretBundlePutResponse {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: r.attempt_id,
            verified: true,
            placed,
        })
    }

    /// Read-only: nothing is written, whatever the request.
    ///
    /// Every member's accepted attempts are demand. The answer names the
    /// asking member's only; of the others it carries a count (spec 24.5).
    fn forecast(
        &self,
        store: &WorkerStore,
        r: ForecastRequest,
    ) -> Result<ForecastResponse, ApiError> {
        let attempts = store.list_nonterminal().map_err(api)?;
        let asking = store.member();
        let foreign: std::collections::BTreeSet<AttemptId> = attempts
            .iter()
            .filter(|a| Some(&a.coordinator_id) != asking.as_ref())
            .map(|a| a.attempt_id.clone())
            .collect();
        let max_epoch = store.max_coordinator_epoch().map_err(api)?;
        let ident = crate::forecast::plan::Ident {
            worker_id: self.cfg.worker_id.clone(),
            boot_id: self.advertised_boot(),
            agent_epoch: self.epoch,
            forecast_ttl_ms: self.cfg.forecast_ttl_ms,
        };
        crate::forecast::answer_worker(
            &self.cfg.traffic_state_dir,
            self.cfg.traffic_config.as_deref(),
            &self.cfg.state_dir,
            Some(&self.cfg.profile_fingerprint),
            &ident,
            crate::forecast::AcceptedWork {
                attempts: &attempts,
                installs: &|payload| self.pending_tool_installs(payload),
            },
            max_epoch,
            &r,
        )
        .map(|answer| crate::forecast::plan::for_member(answer, &foreign))
    }

    /// How many of an accepted attempt's tools are not installed yet (read
    /// only: the same detect admission uses). A tool whose state cannot be
    /// read counts as waiting; an attempt that needs no tool, or a worker with
    /// no registry, counts as none (admission blocks the latter).
    fn pending_tool_installs(&self, payload: &AttemptPayload) -> usize {
        let (Some(bundle), Some(registry)) = (&payload.toolchains, &self.toolchains) else {
            return 0;
        };
        // A selection the registry does not plan is served natively or not at
        // all (admission decides); neither is an install to wait for, and
        // this forecast path must not probe the profile. A selection only the
        // user's mise or rustup can serve is an install unless the last
        // completed report already has that version (spec 22.1): so a
        // worker that has it forecasts sooner than one that must install it.
        let last = self
            .cfg
            .capability_profile
            .as_ref()
            .and_then(|p| p.last_report(&self.cfg.profile_fingerprint));
        bundle
            .evidence
            .iter()
            .filter_map(
                |e| match registry.request(&bundle.frozen, &e.tool, Some(e)) {
                    Ok(request) => Some(request),
                    Err(refused) if refused.allows_native() => {
                        install_is_pending(last.as_ref(), &bundle.frozen, &e.tool)
                            .then(|| registry.install_request(&bundle.frozen, &e.tool, e).ok())
                            .flatten()
                    }
                    Err(_) => None,
                },
            )
            .filter(|r| {
                !matches!(
                    registry.detect(r),
                    Ok(crate::toolchain::adapters::Availability::Installed(_))
                )
            })
            .count()
    }

    fn capabilities_with_diagnostic(
        &self,
    ) -> Result<Option<crate::toolchain::capability::WorkerCapabilities>, &'static str> {
        let Some(profile) = &self.cfg.capability_profile else {
            return Ok(None);
        };
        let report = profile.report();
        if report.profile_fingerprint == self.cfg.profile_fingerprint {
            Ok(Some(report))
        } else {
            Err(profile.identity_failure_message(&report))
        }
    }
    pub(crate) fn capabilities(&self) -> Option<crate::toolchain::capability::WorkerCapabilities> {
        self.capabilities_with_diagnostic().ok().flatten()
    }
    /// The report admission and toolchain activation check a task against
    /// ([`crate::toolchain::profile::Profile::accepted_profile`]): the
    /// measured profile, measured again only on a drift signal or once it is
    /// old. A probe that runs late keeps the version measured before; it is
    /// never taken for a mismatch. A real version change still is one.
    fn measured_with_diagnostic(
        &self,
    ) -> Result<Option<crate::toolchain::capability::WorkerCapabilities>, &'static str> {
        let Some(profile) = &self.cfg.capability_profile else {
            return Ok(None);
        };
        let (report, held) = self.checked_report_unless_stopping(profile)?;
        if !held.is_empty() {
            self.note_global("agent_toolchain_probe_held", "", json!({ "tools": held }));
        }
        if report.profile_fingerprint == self.cfg.profile_fingerprint {
            Ok(Some(report))
        } else {
            Err(profile.identity_failure_message(&report))
        }
    }
    /// The profile's measurement, on a thread of its own: it can take
    /// seconds (each tool probe has its own deadline), and a stopping agent
    /// does not wait for it. A measurement left behind only updates the
    /// profile's own record and ends at its deadlines.
    fn checked_report_unless_stopping(
        &self,
        profile: &Arc<crate::toolchain::profile::Profile>,
    ) -> Result<
        (
            crate::toolchain::capability::WorkerCapabilities,
            Vec<String>,
        ),
        &'static str,
    > {
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let theirs = profile.clone();
        let spawned = std::thread::Builder::new()
            .name("profile-probe".into())
            .spawn(move || {
                let _ = send.send(theirs.accepted_profile());
            });
        if spawned.is_err() {
            return Ok(profile.accepted_profile());
        }
        loop {
            match receive.recv_timeout(STOP_LOOK) {
                Ok(checked) => return Ok(checked),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) if self.stopping() => {
                    return Err(AGENT_STOPPING)
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("the capability measurement panicked")
                }
            }
        }
    }
    fn measured(&self) -> Option<crate::toolchain::capability::WorkerCapabilities> {
        self.measured_with_diagnostic().ok().flatten()
    }
    fn check_profile_identity(&self, rec: &AcceptedAttempt) -> Result<(), ApiError> {
        if let Some(expected) = rec.spec.get("_worker_profile_fingerprint") {
            if expected
                != &serde_json::to_value(&self.cfg.profile_fingerprint)
                    .map_err(|e| internal("profile identity", e))?
            {
                return Err(ApiError::blocked(
                    crate::error::BlockedReason::EnvironmentMismatch,
                    "accepted worker profile changed",
                ));
            }
        }
        Ok(())
    }
    fn check_capabilities(&self, payload: &AttemptPayload) -> Result<(), ApiError> {
        payload.requirements.validate().map_err(invalid)?;
        if let Some(bundle) = &payload.toolchains {
            bundle.validate(&payload.manifest_digest).map_err(invalid)?;
            self.toolchain_requests(bundle, &|| self.measured())?;
        }
        if let Some(profile) = &self.cfg.capability_profile {
            for command in &payload.commands {
                profile
                    .execution_environment(&command.env)
                    .map_err(invalid)?;
            }
            if let Some(setup) = &payload.setup {
                for step in &setup.steps {
                    profile.execution_environment(&step.env).map_err(invalid)?;
                }
            }
        }
        let report = self.measured_with_diagnostic().map_err(|message| {
            ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, message)
        })?;
        if let Some(bundle) = &payload.toolchains {
            crate::toolchain::contract::check_baselines(&bundle.frozen, report.as_ref()).map_err(
                |e| ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, e),
            )?;
        }
        let mut missing = crate::toolchain::capability::missing_with_installable(
            &payload.requirements,
            report.as_ref(),
            payload.toolchains.as_deref(),
        );
        if let Some(profile) = &self.cfg.capability_profile {
            for tool in profile.required_tools() {
                if !report.as_ref().is_some_and(|r| {
                    matches!(
                        r.tools.get(tool),
                        Some(crate::toolchain::capability::ToolObservation::Available { .. })
                    )
                }) {
                    missing.push(format!("profile tool {tool}"));
                }
            }
        }
        if !missing.is_empty() || (self.cfg.capability_profile.is_some() && report.is_none()) {
            return Err(ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                format!(
                    "worker capabilities changed or unmet: {}",
                    missing.join(", ")
                ),
            ));
        }
        Ok(())
    }
    /// `store` is the asking member's: the window is counted over every
    /// member, and split into its attempts and the others' (counts only).
    fn inspect(
        &self,
        store: &WorkerStore,
        request: InspectRequest,
    ) -> Result<InspectResponse, ApiError> {
        let live = self.store.list_nonterminal().map_err(api)?;
        let (active, pending) = policy::load_counts(live.iter().map(|r| r.state));
        let asking = store.member();
        let own = live
            .iter()
            .filter(|r| Some(&r.coordinator_id) == asking.as_ref())
            .count();
        let admission = policy::admission(
            policy::window(self.cfg.max_active_task, self.cfg.prefetch_per_worker),
            live.len(),
            own,
        );
        let policy_bytes = self
            .cfg
            .traffic_config
            .as_ref()
            .and_then(|p| fs::read(p).ok())
            .unwrap_or_default();
        let disk = rustix::fs::statvfs(&self.cfg.state_dir)
            .map(|s| s.f_bavail.saturating_mul(s.f_frsize))
            .unwrap_or(0);
        Ok(InspectResponse {
            own_runs_active: self.own_runs_active(),
            preparation_observations: request
                .preparation_observations
                .as_ref()
                .and_then(|request| self.host.preparation_observation_page(request)),
            capabilities: self.capabilities(),
            schema_version: SchemaVersion::CURRENT,
            worker_id: self.cfg.worker_id.clone(),
            worker_boot_id: self.advertised_boot(),
            agent_epoch: self.epoch,
            snapshot_digest: Digest::of_bytes(
                format!("{}/{active}/{pending}", self.epoch.0).as_bytes(),
            ),
            policy_digest: Digest::of_bytes(&policy_bytes),
            active_task: active,
            prefetch_pending: pending,
            max_active_task: self.cfg.max_active_task,
            prefetch_per_worker: self.cfg.prefetch_per_worker,
            disk_free_bytes: disk,
            draining: super::control::Control::effective(&self.cfg.state_dir).drain
                || super::control::held(&self.cfg.state_dir),
            admission: Some(admission),
        })
    }
}

/// The most dropped attempts one start clears; more wait for the next start.
const DROPPED_PER_START: u32 = 10_000;

/// Leftover dropped attempts one tick tries again.
const DROPPED_PER_TICK: u32 = 32;
/// The longest wait, in passes, before a failing dropped entry is retried.
const DROPPED_BACKOFF_MAX: u64 = 64;
/// How often (in uptime) the event journal is marked for (e).
const UPTIME_MARK_MS: u64 = 3_600_000;

/// Batches of rows an older version acknowledged that the start deletes
/// before it accepts a connection; the tick goes on with the rest.
const STARTUP_ACKED_BATCHES: usize = 10;

/// What [`remove_dropped_attempts`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DroppedReport {
    /// Directories (or a link standing in their place) that were removed.
    pub removed: u32,
    /// Entries that were let go without touching anything: no directory, a
    /// name that is not an attempt id, or an id a member holds again.
    pub skipped: u32,
    /// What could not be removed (`attempt`, the I/O error): still listed,
    /// tried again at the next start.
    pub failed: Vec<(String, String)>,
}

/// Remove the directories of the attempts the worker database dropped: the
/// rebuild of migration step 4, and from stage 3 every deletion of an
/// attempt (spec 25.6 (b)). Only an entry directly below `attempts_root`
/// whose name is one of the dropped ids is touched. A symbolic link in its
/// place is unlinked, never followed, and links inside the directory are
/// removed as links. After the directory, the supervisor lock of the
/// attempt in the state directory above `attempts_root` is unlinked (taken
/// without creating it); a lock a live supervisor holds keeps the entry for
/// the next start. An id that a member holds again is left alone. Each
/// entry is forgotten once its directory and lock are gone, so a pass that
/// was cut short goes on at the next start.
pub fn remove_dropped_attempts(
    attempts_root: &Path,
    store: &WorkerStore,
) -> Result<DroppedReport, StorageError> {
    let mut report = DroppedReport::default();
    for id in store.dropped_attempts(DROPPED_PER_START)? {
        remove_dropped_attempt(attempts_root, store, &id, &mut report)?;
    }
    Ok(report)
}

/// The most entries of `supervisors/` one start looks at (spec 25.11: one
/// `readdir`, each entry one primary-key lookup); more wait for the next.
const SUPERVISOR_LOCKS_PER_START: usize = 10_000;

/// Remove the lock files in `supervisors/` whose attempt has no row and no
/// directory (spec 25.6 (b)): what a version whose `hold` still created the
/// file left after the attempt went. Each is taken without creating it and
/// unlinked while held; one a live supervisor holds, a name that is not a
/// canonical `<attempt>.lock`, anything but a plain file with one link, or
/// an attempt still listed in `dropped_attempt` (its own removal goes on)
/// is kept. Returns how many files went.
pub(crate) fn sweep_supervisor_locks(
    state_dir: &Path,
    store: &WorkerStore,
) -> Result<u32, StorageError> {
    use std::os::unix::fs::MetadataExt;
    let rd = match fs::read_dir(state_dir.join("supervisors")) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(StorageError::Io(e)),
    };
    let dropped: std::collections::BTreeSet<String> = store
        .dropped_attempts(DROPPED_PER_START)?
        .into_iter()
        .collect();
    let mut removed = 0;
    for ent in rd.take(SUPERVISOR_LOCKS_PER_START).flatten() {
        let name = ent.file_name();
        let Some(id) = name.to_str().and_then(|n| n.strip_suffix(".lock")) else {
            continue;
        };
        let Some(attempt) = AttemptId::parse(id).ok().filter(|a| a.as_str() == id) else {
            continue;
        };
        let plain = fs::symlink_metadata(ent.path()).is_ok_and(|m| m.is_file() && m.nlink() == 1);
        if !plain
            || dropped.contains(id)
            || store.attempt_id_held(id)?
            || fs::symlink_metadata(state_dir.join("attempts").join(id)).is_ok()
        {
            continue;
        }
        if let Ok(true) = SupervisorLock::remove(state_dir, &attempt) {
            removed += 1;
        }
    }
    Ok(removed)
}

/// [`remove_dropped_attempts`] for one entry, right after the transaction
/// that deleted the attempt (it does not wait for the next start).
pub(crate) fn remove_dropped_attempt(
    attempts_root: &Path,
    store: &WorkerStore,
    id: &str,
    report: &mut DroppedReport,
) -> Result<(), StorageError> {
    // `parse` re-encodes the id, so only the canonical spelling passes:
    // nothing that could name another place.
    let parsed = AttemptId::parse(id).ok().filter(|a| a.as_str() == id);
    let Some(attempt) = parsed else {
        store.forget_dropped_attempt(id)?;
        report.skipped += 1;
        return Ok(());
    };
    if store.attempt_id_held(id)? {
        store.forget_dropped_attempt(id)?;
        report.skipped += 1;
        return Ok(());
    }
    let path = attempts_root.join(id);
    let removed = match fs::symlink_metadata(&path) {
        Ok(m) if m.is_dir() => fs::remove_dir_all(&path),
        Ok(_) => fs::remove_file(&path),
        Err(e) => Err(e),
    };
    match removed {
        Ok(()) => report.removed += 1,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => report.skipped += 1,
        Err(e) => {
            report.failed.push((id.to_string(), e.to_string()));
            return Ok(());
        }
    }
    // The directory first, the lock after it: a check that read the
    // attempt's `supervise.json` did so before this point.
    if let Some(state_dir) = attempts_root.parent() {
        match SupervisorLock::remove(state_dir, &attempt) {
            Ok(true) => {}
            Ok(false) => {
                report
                    .failed
                    .push((id.to_string(), "a supervisor holds its lock".into()));
                return Ok(());
            }
            Err(e) => {
                report.failed.push((id.to_string(), e.to_string()));
                return Ok(());
            }
        }
    }
    store.forget_dropped_attempt(id)?;
    Ok(())
}

/// The SSH helper holds agent.lock while the resident is absent. Stop through
/// the same lease/supervisor recovery path; never infer that a PID is ours.
pub(crate) fn interrupt_offline(
    state: &Path,
    worker: &WorkerId,
    store: &WorkerStore,
    deadline: std::time::Instant,
) -> Result<(), ApiError> {
    for rec in store.list_nonterminal().map_err(api)? {
        if std::time::Instant::now() >= deadline {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "send-key is still stopping old attempts; rerun send-key to continue",
            ));
        }
        store.expire_lease(&rec.attempt_id, now()).map_err(api)?;
        // A living supervisor observes the lease. Otherwise hold its lock
        // while settling an unstarted attempt or recovering a recorded launch.
        let Unended::Held(_owner) = hold_unended(state, store, &rec.attempt_id)? else {
            continue;
        };
        if store.launches(&rec.attempt_id).map_err(api)?.is_empty() {
            crate::supervisor::run::finalize(store, &rec.attempt_id, termination(None, None))
                .map_err(|e| internal("settling old identity", e))?;
            continue;
        }
        let path = state
            .join("attempts")
            .join(rec.attempt_id.as_str())
            .join("supervise.json");
        let file = crate::service::resident::secure_file(&path, false)
            .map_err(|e| internal("supervisor spec", e))?;
        let spec: crate::supervisor::spec::SuperviseSpec =
            serde_json::from_reader(file).map_err(|e| internal("supervisor spec", e))?;
        if spec.state_dir != state || &spec.worker_id != worker || spec.attempt_id != rec.attempt_id
        {
            return Err(invalid("supervisor spec does not name this attempt"));
        }
        // Recovery verifies recorded process identities before signalling.
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("send-key-recovery".into())
            .spawn(move || {
                // Keep ownership until recovery finishes, including after the helper's
                // deadline. A later invocation resumes through the same durable state.
                let _owner = _owner;
                let _ = send.send(crate::supervisor::run::run(&spec));
            })
            .map_err(|e| internal("starting old identity recovery", e))?;
        wait_recovery(receive, deadline)?.map_err(|e| internal("stopping old identity", e))?;
    }
    Ok(())
}

fn wait_recovery<T>(
    receive: std::sync::mpsc::Receiver<T>,
    deadline: std::time::Instant,
) -> Result<T, ApiError> {
    receive
        .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        .map_err(|_| {
            ApiError::new(
                ErrorCode::Conflict,
                "send-key is still stopping old attempts; rerun send-key to continue",
            )
        })
}

fn acquire_lock(cfg: &AgentConfig) -> Result<ProcessLock, ApiError> {
    let mut waited = 0;
    loop {
        match ProcessLock::acquire(&cfg.state_dir, "agent.lock") {
            Err(StorageError::Locked(_)) if waited < cfg.lock_wait_ms => {
                std::thread::sleep(Duration::from_millis(50));
                waited += 50;
            }
            other => return other.map_err(api),
        }
    }
}

fn spec_path(cfg: &AgentConfig, attempt: &AttemptId) -> PathBuf {
    cfg.attempts_root()
        .join(attempt.as_str())
        .join("supervise.json")
}

fn recovery_of(
    cfg: &AgentConfig,
    store: &WorkerStore,
    rec: &AcceptedAttempt,
    boot_changed: bool,
) -> Result<Recovery, ApiError> {
    let facts = AttemptFacts {
        state: rec.state,
        has_launches: !store.launches(&rec.attempt_id).map_err(api)?.is_empty(),
        stop_requested: rec.cancel_requested_at_ms.is_some() || rec.retired,
        has_spec: spec_path(cfg, &rec.attempt_id).is_file(),
    };
    Ok(policy::decide_recovery(boot_changed, facts))
}

fn how_termination(how: Settle) -> Termination {
    match how {
        Settle::Cancelled => termination(Some(StopReason::Cancel), None),
        Settle::Interrupted => termination(None, None),
    }
}

/// What [`hold_unended`] found.
#[derive(Debug)]
enum Unended {
    /// A live supervisor holds the lock: it owns the attempt.
    Alive,
    /// The row is gone or terminal: nothing to settle (the lock file, which
    /// this call may have created, was unlinked while held).
    Ended,
    /// The lock, held until dropped, of an attempt that is not terminal.
    Held(SupervisorLock),
}

/// Take the supervisor lock of an attempt the caller saw as not terminal,
/// for a recovery that settles it (spec 25.6 (b)). The lock is taken
/// *creating* its file, as the supervisor's start does: a supervisor that
/// was spawned but has not taken its lock yet then finds it held and gives
/// up, instead of starting the command under a result being fixed. A row
/// that is not terminal is never deleted, so this cannot make a lock file
/// again after its deletion; if the row turns out gone or terminal once the
/// lock is held, the file is unlinked while held.
fn hold_unended(
    state: &Path,
    store: &WorkerStore,
    attempt: &AttemptId,
) -> Result<Unended, ApiError> {
    let Some(held) =
        SupervisorLock::try_acquire(state, attempt).map_err(|e| internal("supervisor lock", e))?
    else {
        return Ok(Unended::Alive);
    };
    let row = store.get(attempt).map_err(api)?;
    let Some(rec) = row else {
        // No row: the file this look made (or found) belongs to no attempt.
        match fs::remove_file(crate::supervisor::lock::lock_path(state, attempt)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(internal("supervisor lock", e)),
        }
        drop(held);
        return Ok(Unended::Ended);
    };
    if rec.termination.is_none() {
        return Ok(Unended::Held(held));
    }
    // A terminal row keeps its file: a supervisor that opened it and has
    // not locked it yet would otherwise lock an orphan inode and go on, and
    // an acknowledgement or a release would read "no file, no supervisor".
    // The file goes with the release (`SupervisorLock::remove`).
    drop(held);
    Ok(Unended::Ended)
}

/// Fix the result of an attempt whose command was never started, holding the
/// supervisor lock so no supervisor can be starting it meanwhile. `false`
/// when a supervisor lives (it owns the attempt) or a launch appeared.
/// `lost` skips the look at the launches: the caller has shown that whatever
/// was launched cannot run any more (another boot).
fn settle(
    cfg: &AgentConfig,
    store: &WorkerStore,
    attempt: &AttemptId,
    t: Termination,
    how: Settle,
    lost: bool,
) -> Result<bool, ApiError> {
    let Unended::Held(_lock) = hold_unended(&cfg.state_dir, store, attempt)? else {
        return Ok(false);
    };
    if !lost && !store.launches(attempt).map_err(api)?.is_empty() {
        return Ok(false);
    }
    if how == Settle::Cancelled {
        // Already there, or past it: both are fine.
        let _ = store.transition(attempt, AttemptState::Cancelling, now());
    }
    crate::supervisor::run::finalize(store, attempt, t)
        .map_err(|e| internal("settling the attempt", e))?;
    Ok(true)
}

fn note(store: &WorkerStore, rec: &AcceptedAttempt, kind: &str, payload: Value) {
    let _ = store.append_event(
        Some(&rec.run_id),
        Some(&rec.attempt_id),
        kind,
        &payload,
        now(),
    );
}

/// The requests a version on trial refuses: everything that would change or
/// save something on this worker. Reads are answered.
fn changes_state(request: &Request) -> bool {
    matches!(
        request,
        Request::IdentityRetired(_)
            | Request::SubmitAttempt(_)
            | Request::RenewLease(_)
            | Request::CancelAttempt(_)
            | Request::RetireAttempt(_)
            | Request::AckCollected(_)
            | Request::ReleaseAttempt(_)
            | Request::SecretBundlePut(_)
            | Request::BlobPut(_)
            | Request::EnvSync(_)
    )
}

/// Whether a forecast counts the user's mise or rustup installing `tool`:
/// unless the last completed profile report already has the version. With
/// no completed report yet, the install is counted (spec 22.1).
fn install_is_pending(
    last: Option<&crate::toolchain::capability::WorkerCapabilities>,
    frozen: &crate::toolchain::versions::FrozenToolchains,
    tool: &crate::toolchain::versions::ToolId,
) -> bool {
    !last.is_some_and(|report| {
        crate::toolchain::contract::native_tool(frozen, tool, report).is_some()
    })
}

#[cfg(test)]
mod install_pending_tests {
    use crate::ids::Digest;
    use crate::toolchain::versions::*;

    /// A worker that has not finished a profile report yet cannot be shown
    /// to hold the version: its forecast counts the install (spec 22.1).
    #[test]
    fn a_forecast_without_a_completed_report_counts_the_install() {
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(b"source"),
            Digest::of_bytes(b"parent"),
            scope.clone(),
            &[Declaration {
                tool: ToolId::parse("node").unwrap(),
                constraint: Constraint::parse("22.1.0").unwrap(),
                scope,
                location: Location {
                    path: ".node-version".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &Default::default(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let node = ToolId::parse("node").unwrap();
        assert!(super::install_is_pending(None, &frozen, &node));
    }
}

#[cfg(test)]
mod rekey_deadline_tests {
    #[test]
    fn unfinished_recovery_returns_at_deadline_without_success() {
        let (_send, receive) = std::sync::mpsc::channel::<()>();
        let start = std::time::Instant::now();
        let error = super::wait_recovery(receive, start + std::time::Duration::from_millis(10))
            .unwrap_err();
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        assert!(error.message.contains("still stopping old attempts"));
        assert!(error.message.contains("rerun send-key"));
    }
}

#[cfg(test)]
mod settle_lock_tests {
    use super::*;
    use crate::storage::NewAcceptance;
    use crate::supervisor::lock::lock_path;

    fn accepted(store: &WorkerStore) -> AttemptId {
        accepted_by(&store.for_member(&CoordinatorId::generate()))
    }

    fn accepted_by(store: &WorkerStore) -> AttemptId {
        let a = AttemptId::generate();
        let hash = format!("sha256:{}", "a".repeat(64));
        store
            .submit(
                NewAcceptance {
                    lease: None,
                    attempt_id: a.clone(),
                    run_id: RunId::generate(),
                    task_id: TaskId::generate(),
                    generation: Generation(1),
                    payload_hash: PayloadHash::parse(&hash).unwrap(),
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: json!({"cmd": "true"}),
                },
                1,
            )
            .unwrap();
        a
    }

    /// M1: a supervisor spawned before an agent restart, that has not taken
    /// its lock yet (no lock file), is kept out while recovery settles the
    /// attempt: the recovery's lock creates the file, so the supervisor's
    /// own `try_acquire` finds it held.
    #[test]
    fn a_settle_keeps_out_a_supervisor_that_has_not_locked_yet() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap();
        let a = accepted(&store);
        assert!(!lock_path(dir.path(), &a).exists());
        let Unended::Held(held) = hold_unended(dir.path(), &store, &a).unwrap() else {
            panic!("a free lock of a live attempt is held");
        };
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_none());
        drop(held);
        // A supervisor that holds it keeps the recovery out.
        let sup = SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .unwrap();
        assert!(matches!(
            hold_unended(dir.path(), &store, &a).unwrap(),
            Unended::Alive
        ));
        drop(sup);
    }

    /// L1: the start removes a lock file no attempt owns; it keeps one of
    /// an attempt with a row, one whose directory is still there, one a
    /// supervisor holds and a name that is not a lock of an attempt.
    #[test]
    fn the_start_removes_only_the_lock_files_of_attempts_that_went() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap();
        let gone = AttemptId::generate();
        let live = accepted(&store);
        let with_dir = AttemptId::generate();
        let held_id = AttemptId::generate();
        for a in [&gone, &live, &with_dir, &held_id] {
            drop(SupervisorLock::try_acquire(dir.path(), a).unwrap().unwrap());
        }
        fs::create_dir_all(dir.path().join("attempts").join(with_dir.as_str())).unwrap();
        let other = dir.path().join("supervisors").join("notes.txt");
        fs::write(&other, b"").unwrap();
        let held = SupervisorLock::try_acquire_existing(dir.path(), &held_id)
            .unwrap()
            .unwrap();
        assert_eq!(sweep_supervisor_locks(dir.path(), &store).unwrap(), 1);
        assert!(!lock_path(dir.path(), &gone).exists());
        for a in [&live, &with_dir, &held_id] {
            assert!(lock_path(dir.path(), a).exists(), "{a}");
        }
        assert!(other.exists());
        drop(held);
    }

    /// L6: a terminal row keeps its lock file (a supervisor may have opened
    /// it and not locked it yet); the release removes it.
    #[test]
    fn a_settle_of_a_terminal_attempt_keeps_its_lock_file() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap();
        let m = store.for_member(&CoordinatorId::generate());
        let a = accepted_by(&m);
        for state in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            m.transition(&a, state, 1).unwrap();
        }
        let t = Termination::new(
            crate::state::Outcome::Succeeded,
            Some(crate::state::Exit::Code(0)),
            None,
        )
        .unwrap();
        m.finalize(&a, t, 1).unwrap();
        assert!(matches!(
            hold_unended(dir.path(), &store, &a).unwrap(),
            Unended::Ended
        ));
        assert!(lock_path(dir.path(), &a).exists());
    }

    /// No row: nothing to settle, and no lock file is left behind by the
    /// look.
    #[test]
    fn a_settle_of_a_gone_attempt_leaves_no_lock_file() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap();
        let gone = AttemptId::generate();
        assert!(matches!(
            hold_unended(dir.path(), &store, &gone).unwrap(),
            Unended::Ended
        ));
        assert!(!lock_path(dir.path(), &gone).exists());
    }
}
