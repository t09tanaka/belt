//! The worker's setup builders: one per template key at a time (singleflight),
//! each a supervised process of its own (spec chapters 8 and 12).
//!
//! A consumer (an attempt whose payload carries a [`SetupSpec`]) asks with
//! [`Setups::poll`], which never blocks: the template is there, or a builder
//! for it is working and the consumer holds a reference to it, or the builder
//! could not make it and the consumer is told why (and runs the steps
//! itself). The builder is an internal attempt in a database of its own
//! (`<state>/setup/builders/<builder_id>/worker.db`), run by the same
//! `supervise` process as any attempt: its steps are admitted by
//! task-traffic-controller, its processes are tracked and stopped by the
//! supervisor, and its lease is renewed here only while at least one
//! consumer still needs the result. When the last reference goes the lease is
//! not renewed and the builder is cancelled; the supervisor stops it.
//!
//! The builder works in a workspace of its own, made from the same source as
//! the consumers'. The whole workspace is read before the steps and after
//! them (every file by its digest); what they added, changed and removed,
//! wherever they wrote, becomes the template, published by rename. Nothing
//! of the builder is ever visible under a template key before it is
//! complete. What cannot be kept soundly (a link out of the workspace, a
//! change of Git's own state, a directory that became a file) is not kept
//! at all: the consumers are told why and run the steps themselves.
//!
//! The references of a builder live in this process only. A builder does
//! not outlive its agent: at the next start it is stopped and removed, and
//! the first attempt that needs the template starts a new one.

use super::observation::{self, PreparationBinding, PreparationKind, PreparationObservation};
use super::policy::{self, BuildEnd, KeyContext, Leave, Limits, LinkPlan, Sig, Unkept};
use super::store::{digest_of, remove_tree, StoreError, Template, TemplateStore};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{
    AttemptId, BuilderId, CoordinatorEpoch, Digest, Generation, NodeId, RunId, TaskId,
};
use crate::platform;
use crate::state::AttemptState;
use crate::storage::{NewAcceptance, WorkerStore};
use crate::supervisor::lock::SupervisorLock;
use crate::supervisor::process::{code, spawn_detached, supervise};
use crate::supervisor::spec::{MaskValues, NodeSpec, SuperviseSpec};
use crate::toolchain::adapters::{self, Availability, InstallationReceipt};
use crate::toolchain::contract::ExecutionBinding;
use crate::toolchain::preparation::{
    self, PreparationFailure, PreparationRequest, ToolPreparationPoll,
};
use crate::wire::{
    AttemptPayload, LeaseKey, PreparedArtifact, PreparedChange, PreparedRemoval, ReuseScope,
    SetupSpec,
};
use crate::workspace::{Workspace, WorkspaceKind};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// How often, and how far apart, a builder that could not be settled is
/// looked at again.
const REAP_TRIES: u32 = 60;
const REAP_PAUSE_MS: u64 = 1_000;
/// How many reasons not to keep a template are remembered.
const MAX_UNKEPT: usize = 1024;

/// How the builders get their workspaces: the same way the consumers do.
pub trait BuilderWorkspaces: Send + Sync {
    fn create(&self, attempt: &AttemptId, payload: &AttemptPayload) -> Result<Workspace, ApiError>;
    fn remove(&self, ws: &Workspace) -> Result<(), ApiError>;
}

#[derive(Debug, Clone)]
pub struct SetupConfig {
    /// The worker's state directory; everything of the setup is under
    /// `<state_dir>/setup`.
    pub state_dir: PathBuf,
    pub worker_id: crate::ids::WorkerId,
    /// The binary whose `supervise --spec <file>` runs a builder.
    pub supervisor_exe: PathBuf,
    pub traffic_state_dir: PathBuf,
    pub traffic_config: Option<PathBuf>,
    /// Part of every template key: what the tools of this worker are.
    pub profile: String,
    pub profile_fingerprint: Option<crate::ids::Digest>,
    /// Operator profile definition, distinct from its observed execution identity.
    pub profile_definition: Option<crate::ids::Digest>,
    /// The environment the steps start from (`PATH`, `HOME`, ...). Nothing
    /// of this process's own environment is inherited.
    pub env: BTreeMap<String, String>,
    pub poll_ms: u64,
    pub lease_ms: u64,
    pub term_grace_ms: u64,
    /// Maximum bytes retained per setup supervisor log spool.
    pub log_limit_bytes: u64,
    pub kill_wait_ms: u64,
    /// The whole builder, from its start to its end.
    pub deadline_ms: u64,
    pub inhibit_sleep: bool,
    /// All templates together; over it, the least recently used go.
    pub template_max_bytes: u64,
    /// A template nobody used for this long goes.
    pub template_max_age_ms: u64,
    /// A run-scope template whose run has no attempt here any more goes
    /// after this long without use.
    pub run_template_idle_ms: u64,
    /// How long a tool version whose install failed by itself is not
    /// installed again for a new consumer.
    pub tool_failure_hold_ms: u64,
}

impl SetupConfig {
    pub fn new(
        state_dir: PathBuf,
        worker_id: crate::ids::WorkerId,
        supervisor_exe: PathBuf,
    ) -> Self {
        SetupConfig {
            traffic_state_dir: state_dir.join("traffic"),
            state_dir,
            worker_id,
            supervisor_exe,
            traffic_config: None,
            profile: "default".into(),
            profile_fingerprint: None,
            profile_definition: None,
            log_limit_bytes: crate::supervisor::spec::DEFAULT_LOG_LIMIT_BYTES,
            env: BTreeMap::new(),
            poll_ms: 200,
            lease_ms: crate::supervisor::spec::DEFAULT_LEASE_MS,
            term_grace_ms: crate::supervisor::spec::DEFAULT_TERM_GRACE_MS,
            kill_wait_ms: 10_000,
            deadline_ms: 30 * 60 * 1000,
            inhibit_sleep: false,
            // 20 GiB, 14 days, 1 hour
            template_max_bytes: 21_474_836_480,
            template_max_age_ms: 1_209_600_000,
            run_template_idle_ms: 3_600_000,
            // 10 minutes
            tool_failure_hold_ms: 600_000,
        }
    }

    fn root(&self) -> PathBuf {
        self.state_dir.join("setup")
    }

    fn builders(&self) -> PathBuf {
        self.root().join("builders")
    }
}

/// What a consumer is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupPoll {
    /// A builder is working (or is about to start). Ask again.
    Pending,
    /// Over: `skip` are the ids of the setup steps whose result is in the
    /// consumer's workspace now. `note` says why there is nothing to skip,
    /// when a template was wanted and could not be had.
    Done {
        skip: BTreeSet<String>,
        note: Option<String>,
    },
}

impl SetupPoll {
    fn nothing() -> Self {
        SetupPoll::Done {
            skip: BTreeSet::new(),
            note: None,
        }
    }
}

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum BuilderKind {
    #[default]
    WorkspaceSetup,
    ToolInstall,
}

enum BuilderJob {
    WorkspaceSetup {
        source: String,
        payload: AttemptPayload,
        binding: Option<Box<ExecutionBinding>>,
    },
    ToolInstall {
        request: PreparationRequest,
    },
}

#[derive(Clone)]
enum BuilderResult {
    Workspace(BuildEnd),
    Tool(Result<Box<InstallationReceipt>, PreparationFailure>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    #[serde(default)]
    kind: BuilderKind,
    builder_id: BuilderId,
    attempt_id: AttemptId,
    key: String,
}

struct BuilderLease {
    last: u64,
    seq: u64,
    cancelled: bool,
}

struct Flight {
    key: String,
    attempt_id: AttemptId,
    /// The attempts that hold a reference.
    refs: BTreeSet<String>,
    /// Set once, when the builder has ended.
    end: Option<BuilderResult>,
    kind: BuilderKind,
    /// The last reference went: no new consumer joins, and the builder is
    /// cancelled.
    stop: bool,
}

#[derive(Clone)]
enum Resolved {
    Skip(BTreeSet<String>),
    Unavailable(String),
}

#[derive(Default)]
struct Consumer {
    /// Its run: a run-scope template is kept while a consumer of its run
    /// is known here.
    run: String,
    builder: Option<BuilderId>,
    resolved: Option<Resolved>,
    binding: Option<Digest>,
    binding_generation: Option<BuilderId>,
}

#[derive(Default)]
struct Inner {
    observations: super::observation_transport::ObservationLog,
    probe_versions: HashMap<String, std::sync::Weak<AtomicU64>>,
    /// The builder working for a key (none once it ended or was stopped).
    by_key: HashMap<String, BuilderId>,
    /// Private artifacts remain owned until every old writer is settled.
    barriers: HashMap<String, AttemptId>,
    unknown_builder: bool,
    /// Detect snapshots are invalidated even when a concurrent writer already settled.
    tool_versions: HashMap<String, BuilderId>,
    flights: HashMap<BuilderId, Flight>,
    consumers: HashMap<String, Consumer>,
    /// Keys whose steps passed but made something that cannot be kept, with
    /// the reason: building again would end the same way, so the next
    /// consumer is told at once. Forgotten when the agent restarts.
    unkept: HashMap<String, String>,
    /// Tool keys whose install failed by itself: for `tool_failure_hold_ms`
    /// a new consumer is told the failure and no installer is started.
    /// Forgotten when the host tools change, when a warm-up asks for the
    /// version, and when the agent restarts.
    failed_tools: HashMap<String, FailedTool>,
}

/// The last failed install of a tool key.
struct FailedTool {
    why: String,
    at_ms: u64,
    /// The host tools the install ran with.
    host: Digest,
}

fn host_digest(request: &PreparationRequest) -> Digest {
    Digest::of_bytes(&serde_json::to_vec(&request.host).expect("trusted request serializes"))
}

pub struct Setups {
    probe_gate: Mutex<()>,
    owner_generation: BuilderId,
    cfg: SetupConfig,
    store: TemplateStore,
    ws: Arc<dyn BuilderWorkspaces>,
    inner: Mutex<Inner>,
    started: AtomicU64,
    /// Builders left from before this start that could not be settled.
    leftover: Mutex<BTreeSet<AttemptId>>,
    /// Where the builders found unsettled at the start are.
    leftover_dirs: Mutex<HashMap<AttemptId, PathBuf>>,
}

impl Setups {
    /// Open the setup area of the worker and clean up after a crash: builders
    /// of an earlier run are stopped (through the supervisor's own recovery
    /// when one had started a command) and removed, scratch directories and
    /// unfinished templates are removed. Runs before the worker's workspace
    /// sweep and before any consumer asks.
    pub fn open(cfg: SetupConfig, ws: Arc<dyn BuilderWorkspaces>) -> Result<Arc<Setups>, ApiError> {
        private_dir(&cfg.root()).map_err(|e| internal("setup directory", e))?;
        private_dir(&cfg.builders()).map_err(|e| internal("setup directory", e))?;
        let store = TemplateStore::open(cfg.root().join("templates"))
            .map_err(|e| internal("template store", e))?;
        store.sweep().map_err(|e| internal("template store", e))?;
        let s = Setups {
            probe_gate: Mutex::new(()),
            owner_generation: BuilderId::generate(),
            cfg,
            store,
            ws,
            inner: Mutex::new(Inner::default()),
            started: AtomicU64::new(0),
            leftover: Mutex::new(BTreeSet::new()),
            leftover_dirs: Mutex::new(HashMap::new()),
        };
        s.recover();
        // What a clear renamed aside and did not get to remove. An installer
        // of an earlier run that still works holds the lock of its root, and
        // its trash is left alone.
        adapters::sweep_trash(&s.cfg.state_dir.join("toolchains"));
        let s = Arc::new(s);
        // A builder that could not be settled now (its supervisor is still
        // alive and was asked to stop) is looked at again until it is.
        for attempt in s.unsettled() {
            let me = s.clone();
            let dir = lock(&s.leftover_dirs).get(&attempt).cloned();
            if let Some(dir) = dir {
                let _ = std::thread::Builder::new()
                    .name("setup-reap".into())
                    .spawn(move || me.reap(&dir, &attempt));
            }
        }
        s.evict();
        Ok(s)
    }

    /// Take out the templates that are too old, of runs that are gone, or
    /// over the size of the store.
    pub fn evict(&self) {
        let live: BTreeSet<String> = lock(&self.inner)
            .consumers
            .values()
            .map(|c| c.run.clone())
            .collect();
        let limits = Limits {
            max_bytes: self.cfg.template_max_bytes,
            max_age_ms: self.cfg.template_max_age_ms,
            run_idle_ms: self.cfg.run_template_idle_ms,
        };
        self.store
            .evict(platform::wall_ms(), &limits, &|run| !live.contains(run));
    }

    /// How many builders were started by this process.
    pub fn builds_started(&self) -> u64 {
        self.started.load(Ordering::SeqCst)
    }

    /// The workspaces that belong to a builder (a running one, or one that
    /// could not be settled at the start): the worker's sweep of workspaces
    /// no attempt owns must leave them.
    pub fn owns_workspace(&self, attempt: &AttemptId) -> bool {
        let leftover = { lock(&self.leftover).contains(attempt) };
        leftover
            || lock(&self.inner)
                .flights
                .values()
                .any(|f| &f.attempt_id == attempt)
    }

    /// Builders found at the start that are still not settled (the processes
    /// could not be confirmed stopped). They are tried again at the next
    /// start.
    pub fn unsettled(&self) -> Vec<AttemptId> {
        lock(&self.leftover).iter().cloned().collect()
    }

    fn recover(&self) {
        let Ok(rd) = std::fs::read_dir(self.cfg.builders()) else {
            return;
        };
        for ent in rd.flatten() {
            let dir = ent.path();
            let meta = std::fs::read(dir.join("meta.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<Meta>(&b).ok());
            let Some(meta) = meta else {
                // made but not described: nothing ran
                if !dir.join("worker.db").exists() && !dir.join("spec.json").exists() {
                    let _ = remove_tree(&dir);
                } else {
                    lock(&self.inner).unknown_builder = true;
                }
                continue;
            };
            if meta.key.starts_with("tool:") && meta.kind != BuilderKind::ToolInstall {
                lock(&self.inner).unknown_builder = true;
                continue;
            }
            if meta.kind == BuilderKind::ToolInstall {
                lock(&self.inner)
                    .barriers
                    .insert(meta.key.clone(), meta.attempt_id.clone());
            }
            if !self.settle_old(&dir, &meta) {
                lock(&self.leftover_dirs).insert(meta.attempt_id.clone(), dir);
                lock(&self.leftover).insert(meta.attempt_id);
            } else {
                self.clear_barrier(&meta.attempt_id);
            }
        }
    }

    /// Settle one builder of an earlier run. `true` when nothing of it is
    /// left (directory and workspace removed).
    fn settle_old(&self, dir: &Path, meta: &Meta) -> bool {
        let attempt = &meta.attempt_id;
        let held = match SupervisorLock::try_acquire(dir, attempt) {
            Ok(Some(lock)) => lock,
            // a supervisor is still alive: it is asked to stop (nobody
            // renews its lease, so it would stop by itself anyway)
            Ok(None) => {
                if let Ok(store) = WorkerStore::open(dir, &self.cfg.worker_id) {
                    let _ = store.cancel(attempt, CoordinatorEpoch(1), platform::wall_ms());
                }
                return false;
            }
            Err(_) => return false,
        };
        // nothing was accepted, or nothing was started: nothing runs
        let launched =
            WorkerStore::open(dir, &self.cfg.worker_id).and_then(|s| match s.get(attempt)? {
                None => Ok(false),
                Some(_) => {
                    if meta.kind == BuilderKind::ToolInstall {
                        // A supervisor may have loaded its spec before recovery took the lock.
                        // Fence that late starter before no-launch scratch can be removed.
                        s.cancel(attempt, CoordinatorEpoch(1), platform::wall_ms())?;
                    }
                    Ok(!s.launches(attempt)?.is_empty())
                }
            });
        let launched = match launched {
            Ok(l) => l,
            Err(_) => return false,
        };
        drop(held);
        if launched {
            // something may still run: the supervisor's recovery stops what
            // it recorded, through the guarded path, and settles the attempt
            if !self.settle_in_process(dir, attempt) {
                return false;
            }
        }
        if meta.kind == BuilderKind::WorkspaceSetup {
            if let Some(ws) = std::fs::read(dir.join("workspace.json"))
                .ok()
                .and_then(|b| serde_json::from_slice::<Workspace>(&b).ok())
            {
                if self.ws.remove(&ws).is_err() {
                    return false;
                }
            }
        }
        remove_tree(dir).is_ok()
    }

    /// Allocate the worker-owned private tool area, never a project path.
    pub fn private_tool_root(&self) -> Result<PathBuf, ApiError> {
        let state =
            std::fs::canonicalize(&self.cfg.state_dir).map_err(|e| internal("worker state", e))?;
        let root = state.join("toolchains");
        match std::fs::symlink_metadata(&root) {
            Ok(m)
                if m.is_dir()
                    && m.uid() == rustix::process::geteuid().as_raw()
                    && m.mode() & 0o077 == 0 => {}
            Ok(_) => return Err(invalid("unsafe private tool root")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::DirBuilder::new()
                    .mode(0o700)
                    .create(&root)
                    .map_err(|e| internal("private tool directory", e))?;
            }
            Err(e) => return Err(internal("private tool directory", e)),
        }
        if std::fs::canonicalize(&root).map_err(|e| internal("private tool directory", e))? != root
        {
            return Err(invalid("private tool root is not canonical"));
        }
        Ok(root)
    }

    /// Poll one trusted concrete tool plan. Failed preparation never becomes setup fallback.
    pub fn poll_tool(
        self: &Arc<Self>,
        attempt: &AttemptId,
        request: &PreparationRequest,
    ) -> Result<ToolPreparationPoll, ApiError> {
        if self.cfg.profile_definition.as_ref() != Some(&request.profile_definition)
            || self.cfg.profile_fingerprint.as_ref() != Some(&request.base_profile)
        {
            return Err(invalid("preparation profile identity mismatch"));
        }
        let root = self.private_tool_root()?;
        let plan = request.plan(&root).map_err(invalid)?;
        self.poll_tool_checked(attempt, request, &plan, |receipt| match receipt {
            Some(receipt) => match receipt.validate(&plan) {
                Ok(()) => ToolPreparationPoll::Installed(receipt),
                Err(e) => ToolPreparationPoll::Failed(PreparationFailure(e.into())),
            },
            None => match adapters::detect(&plan) {
                Availability::Installed(receipt) => ToolPreparationPoll::Installed(receipt),
                // An installer failed or was stopped in this root and none
                // of this worker works there now (a live or unsettled one
                // answers before this look): a new installer is started, and
                // it clears the root under the install lock.
                Availability::Unknown(adapters::UNRECEIPTED) | Availability::Missing => {
                    ToolPreparationPoll::Pending
                }
                Availability::Unknown(e) | Availability::ManualRequired(e) => {
                    ToolPreparationPoll::Failed(PreparationFailure(e.into()))
                }
            },
        })
    }

    // The private validation seam also lets tests hold actual receipt/detect I/O at
    // a causal gate while unrelated builders renew and cancel. No filesystem work
    // belongs under `inner`, including verification of a previously finished flight.
    fn poll_tool_checked(
        self: &Arc<Self>,
        attempt: &AttemptId,
        request: &PreparationRequest,
        plan: &adapters::InstallPlan,
        validate: impl FnOnce(Option<Box<InstallationReceipt>>) -> ToolPreparationPoll,
    ) -> Result<ToolPreparationPoll, ApiError> {
        let key = format!("tool:{}", plan.key());
        let consumer = format!("{attempt}/tool:{}", request.tool.as_str());
        let binding = request.digest();
        let mut inner = lock(&self.inner);
        if inner.unknown_builder {
            return Ok(ToolPreparationPoll::Failed(PreparationFailure(
                "unresolved builder metadata".into(),
            )));
        }
        let c = inner.consumers.entry(consumer.clone()).or_default();
        if c.binding.as_ref().is_some_and(|old| *old != binding) {
            return Err(invalid("preparation consumer binding changed"));
        }
        if c.binding.is_none() {
            c.binding_generation = Some(BuilderId::generate());
        }
        c.binding = Some(binding.clone());
        let generation = c.binding_generation.clone();
        let joined = c.builder.clone();
        let receipt = if let Some(id) = &joined {
            match inner.flights.get(id).map(|f| f.end.clone()) {
                Some(None) => return Ok(ToolPreparationPoll::Pending),
                Some(Some(BuilderResult::Tool(Err(failure)))) => {
                    return Ok(ToolPreparationPoll::Failed(failure))
                }
                Some(Some(BuilderResult::Tool(Ok(receipt)))) => Some(receipt),
                _ => {
                    return Ok(ToolPreparationPoll::Failed(PreparationFailure(
                        "tool builder lost".into(),
                    )))
                }
            }
        } else {
            if Self::join_tool(&mut inner, &key, &consumer) {
                return Ok(ToolPreparationPoll::Pending);
            }
            if inner.barriers.contains_key(&key) {
                return Ok(ToolPreparationPoll::Pending);
            }
            None
        };
        let version = inner.tool_versions.get(&key).cloned();
        drop(inner);
        let checked = validate(receipt);
        let mut inner = lock(&self.inner);
        // A release, new binding, joined flight, or writer may have changed while
        // files were read. Never resurrect a released consumer or trust old I/O.
        if inner.unknown_builder {
            return Ok(ToolPreparationPoll::Failed(PreparationFailure(
                "unresolved builder metadata".into(),
            )));
        }
        let Some(c) = inner.consumers.get(&consumer) else {
            return Ok(ToolPreparationPoll::Failed(PreparationFailure(
                "preparation consumer released during validation".into(),
            )));
        };
        if c.binding.as_ref() != Some(&binding) || c.binding_generation != generation {
            return Err(invalid(
                "preparation consumer binding changed during validation",
            ));
        }
        if c.builder != joined {
            return Ok(ToolPreparationPoll::Pending);
        }
        if joined.is_none() && Self::join_tool(&mut inner, &key, &consumer) {
            return Ok(ToolPreparationPoll::Pending);
        }
        if inner.barriers.contains_key(&key) || inner.tool_versions.get(&key).cloned() != version {
            return Ok(ToolPreparationPoll::Pending);
        }
        if let Some(id) = joined {
            if !inner
                .flights
                .get(&id)
                .is_some_and(|f| matches!(&f.end, Some(BuilderResult::Tool(Ok(_)))))
            {
                return Ok(ToolPreparationPoll::Failed(PreparationFailure(
                    "tool builder lost during validation".into(),
                )));
            }
            return Ok(checked);
        }
        // Pending from detection means a missing artifact, not a cache success.
        if checked != ToolPreparationPoll::Pending {
            return Ok(checked);
        }
        // An install of this version that failed by itself a short while ago
        // is not repeated for every consumer that asks.
        if let Some(failed) = inner.failed_tools.get(&key) {
            let held = policy::failure_held(
                failed.host == host_digest(request),
                platform::monotonic_ms(),
                failed.at_ms,
                self.cfg.tool_failure_hold_ms,
            );
            match held {
                Some(left) => {
                    return Ok(ToolPreparationPoll::Failed(PreparationFailure(
                        policy::failure_held_message(&failed.why, left),
                    )))
                }
                None => {
                    inner.failed_tools.remove(&key);
                }
            }
        }
        let id = BuilderId::generate();
        let builder_attempt = AttemptId::generate();
        inner.tool_versions.insert(key.clone(), id.clone());
        Self::invalidate_probe(&mut inner, &key);
        inner.barriers.insert(key.clone(), builder_attempt.clone());
        inner.by_key.insert(key.clone(), id.clone());
        inner.flights.insert(
            id.clone(),
            Flight {
                key: key.clone(),
                attempt_id: builder_attempt.clone(),
                refs: BTreeSet::from([consumer.clone()]),
                end: None,
                stop: false,
                kind: BuilderKind::ToolInstall,
            },
        );
        inner.consumers.get_mut(&consumer).unwrap().builder = Some(id.clone());
        drop(inner);
        self.spawn_builder(
            id,
            builder_attempt,
            key,
            BuilderJob::ToolInstall {
                request: request.clone(),
            },
        );
        Ok(ToolPreparationPoll::Pending)
    }

    /// Join only a live writer; a stopped writer still blocks until settlement.
    fn join_tool(inner: &mut Inner, key: &str, consumer: &str) -> bool {
        let Some(id) = inner.by_key.get(key).cloned() else {
            return false;
        };
        let f = inner.flights.get_mut(&id).expect("listed builder exists");
        if !f.stop {
            policy::join(&mut f.refs, consumer);
            inner
                .consumers
                .get_mut(consumer)
                .expect("bound consumer")
                .builder = Some(id);
        }
        true
    }

    /// Forget that the install of this version failed: the next consumer
    /// starts an installer at once. For an explicit request (a warm-up).
    pub fn retry_tool(&self, request: &PreparationRequest) {
        lock(&self.inner)
            .failed_tools
            .remove(&format!("tool:{}", request.expected_plan));
    }

    fn clear_barrier(&self, attempt: &AttemptId) {
        lock(&self.inner).barriers.retain(|_, old| old != attempt);
    }

    fn run_tool_builder(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        key: &str,
        request: &PreparationRequest,
    ) -> Result<Box<InstallationReceipt>, PreparationFailure> {
        let dir = std::fs::canonicalize(self.cfg.builders())
            .map_err(|e| PreparationFailure(e.to_string()))?
            .join(id.as_str());
        // Whether a failure is one the installer made by itself.
        let mut by_itself = false;
        let mut work = || -> Result<InstallationReceipt, String> {
            private_dir(&dir).map_err(|e| e.to_string())?;
            let meta = Meta {
                kind: BuilderKind::ToolInstall,
                builder_id: id.clone(),
                attempt_id: attempt.clone(),
                key: key.into(),
            };
            write_private(
                &dir.join("meta.json"),
                &serde_json::to_vec(&meta).map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
            write_private(
                &dir.join(preparation::REQUEST_FILE),
                &serde_json::to_vec(request).map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
            if self.wanted(id).0 == 0 {
                return Err("tool preparation cancelled".into());
            }
            let exe = std::fs::canonicalize(&self.cfg.supervisor_exe).map_err(|e| e.to_string())?;
            let node = preparation::helper_node(&exe, &dir, request)?;
            self.write_nodes(&dir, attempt, vec![node], Vec::new())?;
            let store = self.accept(&dir, attempt)?;
            if self.wanted(id).0 == 0 {
                return Err("tool preparation cancelled".into());
            }
            let end = self.supervised(id, attempt, &dir, &store);
            by_itself = policy::install_itself_failed(&end);
            if !end.is_built() {
                return Err(end.describe());
            }
            // The installer said it was done, and what it left is not an
            // installation.
            by_itself = true;
            let plan = request.plan(&self.private_tool_root().map_err(|e| e.message)?)?;
            match adapters::detect(&plan) {
                Availability::Installed(receipt) => Ok(*receipt),
                Availability::Unknown(e) | Availability::ManualRequired(e) => Err(e.into()),
                Availability::Missing => Err("installer ended without receipt".into()),
            }
        };
        let result = work().map(Box::new).map_err(PreparationFailure);
        match &result {
            Ok(_) => {
                lock(&self.inner).failed_tools.remove(key);
                self.handoff_observation(id, attempt, key, BuilderKind::ToolInstall, &dir);
            }
            Err(failure) if by_itself => {
                let mut inner = lock(&self.inner);
                if inner.failed_tools.len() >= MAX_UNKEPT {
                    inner.failed_tools.clear();
                }
                inner.failed_tools.insert(
                    key.into(),
                    FailedTool {
                        why: failure.0.clone(),
                        at_ms: platform::monotonic_ms(),
                        host: host_digest(request),
                    },
                );
            }
            Err(_) => {}
        }
        if !lock(&self.leftover).contains(attempt) {
            let _ = remove_tree(&dir);
        }
        result
    }

    /// Where a consumer stands. Never blocks for a build: a template that is
    /// there is copied into `ws` (which can take a while), a builder that is
    /// working makes the answer `Pending`.
    pub fn poll(
        self: &Arc<Self>,
        attempt: &AttemptId,
        payload: &AttemptPayload,
        ws: &Workspace,
    ) -> Result<SetupPoll, ApiError> {
        self.poll_bound(attempt, payload, ws, None)
    }

    pub fn poll_bound(
        self: &Arc<Self>,
        attempt: &AttemptId,
        payload: &AttemptPayload,
        ws: &Workspace,
        binding: Option<&ExecutionBinding>,
    ) -> Result<SetupPoll, ApiError> {
        let Some(spec) = &payload.setup else {
            return Ok(SetupPoll::nothing());
        };
        policy::validate(spec).map_err(invalid)?;
        if policy::hoisted(spec).is_empty() {
            return Ok(SetupPoll::nothing());
        }
        if ws.kind == WorkspaceKind::InPlace {
            // the user's own worktree: nothing is copied into it, and no
            // builder would have another one to work in
            return Ok(SetupPoll::Done {
                skip: BTreeSet::new(),
                note: Some("an in-place workspace runs its setup steps itself".into()),
            });
        }
        let a = attempt.to_string();
        let resolved = lock(&self.inner)
            .consumers
            .get(&a)
            .and_then(|c| c.resolved.clone());
        match resolved {
            Some(Resolved::Skip(skip)) => return Ok(SetupPoll::Done { skip, note: None }),
            Some(Resolved::Unavailable(why)) => {
                return Ok(SetupPoll::Done {
                    skip: BTreeSet::new(),
                    note: Some(why),
                })
            }
            None => {}
        }
        let bound_env = binding
            .map(|b| b.activation.environment.clone())
            .unwrap_or_else(|| self.cfg.env.clone());
        let bound_profile = binding
            .map(|b| b.fingerprint().to_string())
            .unwrap_or_else(|| self.cfg.profile.clone());
        let ctx = workspace_key_context(
            spec,
            &payload.manifest_digest,
            &payload.run_id,
            bound_profile,
            bound_env,
        );
        let key = policy::template_key(spec, &ctx);
        let now = platform::wall_ms();
        let mut inner = lock(&self.inner);
        inner.consumers.entry(a.clone()).or_default().run = ctx.run_id.clone();
        let joined = inner.consumers.get(&a).and_then(|c| c.builder.clone());
        let ready: Option<Template> = match joined {
            Some(id) => match inner.flights.get(&id).map(|f| {
                f.end.as_ref().map(|end| match end {
                    BuilderResult::Workspace(end) => end.clone(),
                    BuilderResult::Tool(_) => BuildEnd::Crashed("wrong builder kind".into()),
                })
            }) {
                Some(None) => return Ok(SetupPoll::Pending),
                None => {
                    drop(inner);
                    let why = "the builder it waited for is not known any more".to_string();
                    return Ok(self.resolve(&a, Resolved::Unavailable(why)));
                }
                Some(Some(end)) if !end.is_built() => {
                    let why = end.describe();
                    drop(inner);
                    return Ok(self.resolve(&a, Resolved::Unavailable(why)));
                }
                Some(Some(_)) => {
                    drop(inner);
                    self.store.get(&key, &ctx.source_digest, spec, now)
                }
            },
            None => match inner.by_key.get(&key).cloned() {
                Some(id) => {
                    let f = inner.flights.get_mut(&id).expect("a flight is listed");
                    policy::join(&mut f.refs, &a);
                    inner.consumers.entry(a.clone()).or_default().builder = Some(id);
                    return Ok(SetupPoll::Pending);
                }
                None => {
                    // no builder: a template that is there already, or a new
                    // builder; the store is looked at under the lock so that
                    // a builder that just published is not built again
                    let found = self.store.get(&key, &ctx.source_digest, spec, now);
                    if let (None, Some(why)) = (&found, inner.unkept.get(&key).cloned()) {
                        drop(inner);
                        let why = BuildEnd::NotKept(why).describe();
                        return Ok(self.resolve(&a, Resolved::Unavailable(why)));
                    }
                    if found.is_none() {
                        let id = BuilderId::generate();
                        let attempt_id = AttemptId::generate();
                        let mut refs = BTreeSet::new();
                        policy::join(&mut refs, &a);
                        inner.flights.insert(
                            id.clone(),
                            Flight {
                                key: key.clone(),
                                attempt_id: attempt_id.clone(),
                                refs,
                                end: None,
                                kind: BuilderKind::WorkspaceSetup,
                                stop: false,
                            },
                        );
                        inner.by_key.insert(key.clone(), id.clone());
                        Self::invalidate_probe(&mut inner, &key);
                        inner.consumers.entry(a.clone()).or_default().builder = Some(id.clone());
                        drop(inner);
                        self.spawn_builder(
                            id,
                            attempt_id,
                            key,
                            BuilderJob::WorkspaceSetup {
                                source: ctx.source_digest,
                                payload: payload.clone(),
                                binding: binding.cloned().map(Box::new),
                            },
                        );
                        return Ok(SetupPoll::Pending);
                    }
                    drop(inner);
                    found
                }
            },
        };
        let Some(template) = ready else {
            // it was built but is gone or unusable now
            return Ok(self.resolve(
                &a,
                Resolved::Unavailable("the template made for it is not usable".into()),
            ));
        };
        let resolved = match self.store.materialize(&template, &ws.root) {
            Ok(()) => Resolved::Skip(policy::skip_set(&template.receipt)),
            Err(StoreError::Corrupt(why)) => {
                // not what its receipt says: this one is never used again
                // (one a builder has put in its place since is another)
                self.store.discard(&key, &template.receipt.digest);
                Resolved::Unavailable(format!("the template did not match its receipt: {why}"))
            }
            Err(StoreError::Conflict(path)) => Resolved::Unavailable(format!(
                "the workspace holds something else at `{path}` than the template was made over"
            )),
            // it went while it was read (discarded by another consumer,
            // or the disk failed): the workspace is as it was
            Err(StoreError::Io(why)) => {
                Resolved::Unavailable(format!("the template could not be copied: {why}"))
            }
            // only here is the workspace in a state nobody knows
            Err(e @ StoreError::Dirty(_)) => return Err(internal("template", e)),
        };
        drop(template);
        self.evict();
        Ok(self.resolve(&a, resolved))
    }

    /// Record how a consumer ended and give up its reference.
    fn resolve(&self, consumer: &str, r: Resolved) -> SetupPoll {
        let mut inner = lock(&self.inner);
        let c = inner.consumers.entry(consumer.to_string()).or_default();
        c.resolved = Some(r.clone());
        let builder = c.builder.clone();
        if let Some(id) = builder {
            Self::drop_ref(&mut inner, &id, consumer);
        }
        match r {
            Resolved::Skip(skip) => SetupPoll::Done { skip, note: None },
            Resolved::Unavailable(why) => SetupPoll::Done {
                skip: BTreeSet::new(),
                note: Some(why),
            },
        }
    }

    /// The consumer is gone (cancelled, ended): its reference goes. The last
    /// reference of a builder that is still working stops the builder.
    pub fn release(&self, attempt: &AttemptId) {
        let a = attempt.to_string();
        let mut inner = lock(&self.inner);
        let prefix = format!("{a}/tool:");
        let keys: Vec<_> = inner
            .consumers
            .keys()
            .filter(|k| **k == a || k.starts_with(&prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(c) = inner.consumers.remove(&key) {
                if let Some(id) = c.builder {
                    Self::drop_ref(&mut inner, &id, &key);
                }
            }
        }
    }

    fn drop_ref(inner: &mut Inner, id: &BuilderId, consumer: &str) {
        let Some(f) = inner.flights.get_mut(id) else {
            return;
        };
        let building = f.end.is_none() && !f.stop;
        if policy::leave(&mut f.refs, consumer, building) == Leave::StopBuilder {
            f.stop = true;
            let key = f.key.clone();
            let workspace = f.kind == BuilderKind::WorkspaceSetup;
            Self::invalidate_probe(inner, &key);
            if workspace && inner.by_key.get(&key) == Some(id) {
                inner.by_key.remove(&key);
            }
        }
        if let Some(f) = inner.flights.get(id) {
            if f.end.is_some() && f.refs.is_empty() {
                inner.flights.remove(id);
            }
        }
    }

    fn spawn_builder(
        self: &Arc<Self>,
        id: BuilderId,
        attempt: AttemptId,
        key: String,
        job: BuilderJob,
    ) {
        self.started.fetch_add(1, Ordering::SeqCst);
        let me = self.clone();
        let spawned = std::thread::Builder::new()
            .name("setup-builder".into())
            .spawn({
                let (id, attempt, key) = (id.clone(), attempt.clone(), key.clone());
                move || {
                    let end = catch_unwind(AssertUnwindSafe(|| match &job {
                        BuilderJob::WorkspaceSetup {
                            source,
                            payload,
                            binding,
                        } => BuilderResult::Workspace(me.run_builder(
                            &id,
                            &attempt,
                            &key,
                            source,
                            payload,
                            binding.as_deref(),
                        )),
                        BuilderJob::ToolInstall { request } => {
                            BuilderResult::Tool(me.run_tool_builder(&id, &attempt, &key, request))
                        }
                    }))
                    .unwrap_or_else(|_| match job {
                        BuilderJob::WorkspaceSetup { .. } => BuilderResult::Workspace(
                            BuildEnd::Crashed("the builder thread panicked".into()),
                        ),
                        BuilderJob::ToolInstall { .. } => {
                            let dir = me.cfg.builders().join(id.as_str());
                            if dir.join("worker.db").exists()
                                && !me.settle_in_process(&dir, &attempt)
                            {
                                lock(&me.leftover).insert(attempt.clone());
                            }
                            BuilderResult::Tool(Err(PreparationFailure(
                                "the builder thread panicked".into(),
                            )))
                        }
                    });
                    // the consumers are told first; then a builder whose
                    // processes could not be confirmed stopped is looked at
                    // again, as the agent does for an attempt
                    let workspace = matches!(&end, BuilderResult::Workspace(_));
                    me.finish(&id, end);
                    if workspace {
                        me.evict();
                    }
                    me.reap(&me.cfg.builders().join(id.as_str()), &attempt);
                }
            });
        if spawned.is_err() {
            self.finish(
                &id,
                BuilderResult::Workspace(BuildEnd::Crashed("no thread for the builder".into())),
            );
        }
    }

    /// Settle a builder that was left unsettled (its supervisor died and
    /// what it started could not be told from what is not its own, or the
    /// supervisor itself could not confirm the stop): the supervisor's
    /// recovery is run again, with a pause, as the agent does for an attempt,
    /// until it settles, up to a limit. What is still unsettled then is
    /// settled at the next start.
    fn reap(&self, dir: &Path, attempt: &AttemptId) {
        for _ in 0..REAP_TRIES {
            if !lock(&self.leftover).contains(attempt) {
                return;
            }
            std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.max(REAP_PAUSE_MS)));
            if !dir.exists() {
                // somebody else cleared it up
                lock(&self.leftover).remove(attempt);
                self.clear_barrier(attempt);
                return;
            }
            // a supervisor that is still alive holds the lock: it is left
            // to stop (it was asked to), and nothing is removed under it
            match SupervisorLock::try_acquire(dir, attempt) {
                Ok(Some(held)) => drop(held),
                _ => continue,
            }
            if !self.settle_in_process(dir, attempt) {
                continue;
            }
            let tool = lock(&self.inner).barriers.values().any(|a| a == attempt);
            if !tool {
                if let Some(ws) = std::fs::read(dir.join("workspace.json"))
                    .ok()
                    .and_then(|b| serde_json::from_slice::<Workspace>(&b).ok())
                {
                    if self.ws.remove(&ws).is_err() {
                        continue;
                    }
                }
            }
            if remove_tree(dir).is_ok() {
                self.clear_barrier(attempt);
                lock(&self.leftover).remove(attempt);
                return;
            }
        }
    }

    /// Settle a builder whose supervisor is gone, in this process, through
    /// the supervisor's own recovery. The builder is cancelled first: a
    /// supervisor that died before it recorded a launch left the attempt
    /// `Ready` with nothing started, and `supervise` would then *run* the
    /// steps here, in the agent, with nobody renewing the lease. With the
    /// cancel recorded it starts nothing and only stops what was recorded.
    /// `true` when the attempt is settled and nothing of it can still run.
    fn settle_in_process(&self, dir: &Path, attempt: &AttemptId) -> bool {
        let Ok(store) = WorkerStore::open(dir, &self.cfg.worker_id) else {
            return false;
        };
        match store.get(attempt) {
            Ok(None) => return true,
            Ok(Some(_)) => {}
            Err(_) => return false,
        }
        if store
            .cancel(attempt, CoordinatorEpoch(1), platform::wall_ms())
            .is_err()
        {
            return false;
        }
        drop(store);
        SuperviseSpec::from_file(&dir.join("spec.json"))
            .ok()
            .and_then(|s| supervise(&s).ok())
            .is_some_and(|r| r.unresolved.is_none())
    }

    /// Bounded immutable process-local producer snapshot. No filesystem or database I/O.
    /// A reopened owner has no observations, even if installed artifacts remain valid.
    pub fn preparation_observations(&self, limit: usize) -> Vec<PreparationObservation> {
        lock(&self.inner).observations.snapshot(limit)
    }

    /// Read native clock before acquiring the ownership mutex; no I/O under it.
    pub fn preparation_observation_page(
        &self,
        request: &super::observation_transport::ObservationPageRequest,
    ) -> super::observation_transport::ObservationPage {
        let boot = platform::boot_id();
        let now = platform::monotonic_ms();
        lock(&self.inner)
            .observations
            .page(&self.owner_generation, request, boot.as_deref(), now)
    }

    fn handoff_observation(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        key: &str,
        kind: BuilderKind,
        dir: &Path,
    ) {
        // Read outside the ownership mutex; recheck its current generation afterwards.
        let Some(spec): Option<SuperviseSpec> = observation::read_private(&dir.join("spec.json"))
        else {
            return;
        };
        if spec.validate().is_err() {
            return;
        }
        let Some(value) = observation::aggregate(&spec) else {
            return;
        };
        let expected_kind = match kind {
            BuilderKind::WorkspaceSetup => PreparationKind::WorkspaceSetup,
            BuilderKind::ToolInstall => PreparationKind::ToolInstall,
        };
        if value.binding.builder_id != *id
            || value.binding.attempt_id != *attempt
            || value.binding.owner_generation != self.owner_generation
            || value.binding.kind != expected_kind
            || value.binding.key != crate::ids::Digest::of_bytes(key.as_bytes())
        {
            return;
        }
        if lock(&self.leftover).contains(attempt) {
            return;
        }
        let mut inner = lock(&self.inner);
        if inner.unknown_builder
            || !inner.flights.get(id).is_some_and(|f| {
                f.attempt_id == *attempt
                    && f.key == key
                    && f.kind == kind
                    && !f.stop
                    && !f.refs.is_empty()
                    && f.end.is_none()
            })
            || inner.by_key.get(key) != Some(id)
            || (kind == BuilderKind::ToolInstall
                && (inner.barriers.get(key) != Some(attempt)
                    || inner.tool_versions.get(key) != Some(id)))
        {
            return;
        }
        inner.observations.insert(value);
    }

    fn finish(&self, id: &BuilderId, end: BuilderResult) {
        // Never hold both mutexes: ownership readers and finishers can run together.
        let attempt = {
            lock(&self.inner)
                .flights
                .get(id)
                .map(|f| f.attempt_id.clone())
        };
        let unsettled = attempt
            .as_ref()
            .is_some_and(|attempt| lock(&self.leftover).contains(attempt));
        let mut inner = lock(&self.inner);
        let Some(f) = inner.flights.get_mut(id) else {
            return;
        };
        let unkept = match &end {
            BuilderResult::Workspace(BuildEnd::NotKept(why)) => Some(why.clone()),
            _ => None,
        };
        f.end = Some(end);
        let (key, empty) = (f.key.clone(), f.refs.is_empty());
        Self::invalidate_probe(&mut inner, &key);
        if !unsettled {
            inner.barriers.remove(&key);
        }
        if let Some(why) = unkept {
            if inner.unkept.len() >= MAX_UNKEPT {
                inner.unkept.clear();
            }
            inner.unkept.insert(key.clone(), why);
        }
        if inner.by_key.get(&key) == Some(id) {
            inner.by_key.remove(&key);
        }
        if empty {
            inner.flights.remove(id);
        }
    }

    fn wanted(&self, id: &BuilderId) -> (usize, bool) {
        lock(&self.inner)
            .flights
            .get(id)
            .map_or((0, true), |f| (f.refs.len(), f.stop))
    }

    /// The builder: its workspace, its supervisor, its lease, its template.
    /// Everything it made is gone when it returns, except the template.
    fn run_builder(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        key: &str,
        source: &str,
        payload: &AttemptPayload,
        binding: Option<&ExecutionBinding>,
    ) -> BuildEnd {
        let spec = payload.setup.as_ref().expect("a builder is for a setup");
        let dir = self.cfg.builders().join(id.as_str());
        let mut made: Option<Workspace> = None;
        let end = self.build(
            id, attempt, key, source, payload, spec, &dir, &mut made, binding,
        );
        // the supervisor has ended or never started: nothing runs in them
        if let Some(ws) = &made {
            let _ = self.ws.remove(ws);
        }
        // unless its processes could not be confirmed stopped: then the
        // next start's recovery needs what it recorded
        if !lock(&self.leftover).contains(attempt) {
            let _ = remove_tree(&dir);
        }
        end
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        key: &str,
        source: &str,
        payload: &AttemptPayload,
        spec: &SetupSpec,
        dir: &Path,
        made: &mut Option<Workspace>,
        binding: Option<&ExecutionBinding>,
    ) -> BuildEnd {
        let crashed =
            |what: &str, e: &dyn std::fmt::Display| BuildEnd::Crashed(format!("{what}: {e}"));
        if let Err(e) = private_dir(dir) {
            return crashed("builder directory", &e);
        }
        let meta = Meta {
            kind: BuilderKind::WorkspaceSetup,
            builder_id: id.clone(),
            attempt_id: attempt.clone(),
            key: key.to_string(),
        };
        if let Err(e) = write_private(&dir.join("meta.json"), &serde_json::to_vec(&meta).unwrap()) {
            return crashed("builder record", &e);
        }
        if self.wanted(id).0 == 0 {
            return BuildEnd::Cancelled;
        }
        let ws = match self.ws.create(attempt, payload) {
            Ok(ws) => ws,
            Err(e) => return crashed("builder workspace", &e.message),
        };
        *made = Some(ws.clone());
        let record = serde_json::to_vec(&ws).expect("a workspace is plain data");
        if let Err(e) = write_private(&dir.join("workspace.json"), &record) {
            return crashed("builder record", &e);
        }
        // the whole workspace as the steps find it
        let before = match snapshot(&ws.root, None) {
            Ok(b) => b,
            Err(why) => return BuildEnd::NotKept(why.to_string()),
        };
        let store = match self.accept(dir, attempt) {
            Ok(s) => s,
            Err(e) => return crashed("builder state", &e),
        };
        if let Err(e) = self.write_spec(dir, attempt, spec, &ws, binding) {
            return crashed("builder spec", &e);
        }
        if self.wanted(id).0 == 0 {
            return BuildEnd::Cancelled;
        }
        let end = self.supervised(id, attempt, dir, &store);
        if lock(&self.leftover).contains(attempt) {
            *made = None;
        }
        if !end.is_built() {
            return end;
        }
        let (artifacts, modified, removed) = match capture(&ws.root, &before) {
            Ok(c) => c,
            Err(why) => return BuildEnd::NotKept(why.to_string()),
        };
        let now = platform::wall_ms();
        let receipt =
            policy::seal_receipt(key, source, id, spec, artifacts, modified, removed, now);
        let run =
            (policy::effective_scope(spec) == ReuseScope::Run).then(|| payload.run_id.to_string());
        match self
            .store
            .publish(key, &receipt, &ws.root, run.as_deref(), now)
        {
            Ok(template) => {
                // Publication can legitimately retain an older pinned template.
                // Its success is not proof of this builder's completed artifact.
                if template.receipt.builder_id == *id
                    && template.receipt.template_key == key
                    && template.receipt.source_digest == source
                {
                    self.handoff_observation(id, attempt, key, BuilderKind::WorkspaceSetup, dir);
                }
                BuildEnd::Built
            }
            Err(e) => crashed("the template", &e),
        }
    }

    /// One iteration of the existing supervisor lease loop. Clock inputs permit
    /// causal tests without waiting for a real lease interval to elapse.
    fn lease_tick(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        store: &WorkerStore,
        lease: &mut BuilderLease,
        clock: impl FnOnce() -> (u64, u64),
    ) {
        let (refs, stop) = self.wanted(id);
        let (now, wall) = clock();
        let every = (self.cfg.lease_ms / 3).max(self.cfg.poll_ms);
        if policy::renew_lease(refs, stop) {
            if now.saturating_sub(lease.last) >= every {
                lease.seq += 1;
                let key = LeaseKey {
                    coordinator_epoch: CoordinatorEpoch(1),
                    lease_seq: lease.seq,
                };
                let _ = store.renew_lease(attempt, key, wall);
                lease.last = now;
            }
        } else if !lease.cancelled {
            lease.cancelled = true;
            let _ = store.cancel(attempt, CoordinatorEpoch(1), wall);
        }
    }

    /// Shared supervised execution, renewal, cancellation and settlement for both job kinds.
    fn supervised(
        &self,
        id: &BuilderId,
        attempt: &AttemptId,
        dir: &Path,
        store: &WorkerStore,
    ) -> BuildEnd {
        let crashed =
            |what: &str, e: &dyn std::fmt::Display| BuildEnd::Crashed(format!("{what}: {e}"));
        let mut child = match spawn_detached(&self.cfg.supervisor_exe, &dir.join("spec.json")) {
            Ok(c) => c,
            Err(e) => return crashed("supervisor", &e),
        };
        // The same lease tick is used by both builder kinds.
        let mut lease = BuilderLease {
            last: platform::monotonic_ms(),
            seq: 0,
            cancelled: false,
        };
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) => {}
                Err(e) => {
                    // whether it still runs is not known: it is asked to
                    // stop, and its workspace and directory are kept until
                    // it is seen settled (`reap`), not removed under it
                    let _ = store.cancel(attempt, CoordinatorEpoch(1), platform::wall_ms());
                    lock(&self.leftover).insert(attempt.clone());
                    return crashed("supervisor", &e);
                }
            }
            self.lease_tick(id, attempt, store, &mut lease, || {
                (platform::monotonic_ms(), platform::wall_ms())
            });
            std::thread::sleep(Duration::from_millis(self.cfg.poll_ms.clamp(1, 100)));
        };
        match status.code() {
            Some(code::SETTLED) => {}
            Some(code::UNRESOLVED) | Some(code::IDENTITY_UNKNOWN) => {
                // the processes could not be confirmed stopped: the builder
                // and its workspace are kept for the next start's recovery
                lock(&self.leftover).insert(attempt.clone());
                return BuildEnd::Crashed("its processes could not be confirmed stopped".into());
            }
            other => {
                // The supervisor died or failed: what it started is stopped
                // by the supervisor's own recovery, which settles the
                // attempt. Only a builder that could not be settled is kept.
                if !self.settle_in_process(dir, attempt) {
                    lock(&self.leftover).insert(attempt.clone());
                }
                return BuildEnd::Crashed(format!("the supervisor ended with {other:?}"));
            }
        }
        policy::end_of(
            store
                .get(attempt)
                .ok()
                .flatten()
                .and_then(|r| r.termination),
        )
    }

    /// The builder's own database: one attempt, `Ready`.
    fn accept(&self, dir: &Path, attempt: &AttemptId) -> Result<WorkerStore, String> {
        let store = WorkerStore::open(dir, &self.cfg.worker_id).map_err(|e| e.to_string())?;
        let now = platform::wall_ms();
        store
            .submit(
                NewAcceptance {
                    attempt_id: attempt.clone(),
                    run_id: RunId::generate(),
                    task_id: TaskId::generate(),
                    generation: Generation(1),
                    payload_hash: Digest::of_bytes(attempt.as_str().as_bytes()),
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: serde_json::json!({ "setup_builder": true }),
                },
                now,
            )
            .map_err(|e| e.to_string())?;
        for to in [AttemptState::Preparing, AttemptState::Ready] {
            store
                .transition(attempt, to, now)
                .map_err(|e| e.to_string())?;
        }
        Ok(store)
    }

    fn write_spec(
        &self,
        dir: &Path,
        attempt: &AttemptId,
        spec: &SetupSpec,
        ws: &Workspace,
        binding: Option<&ExecutionBinding>,
    ) -> Result<(), String> {
        let base_env = binding
            .map(|b| &b.activation.environment)
            .unwrap_or(&self.cfg.env);
        let mut nodes = Vec::new();
        for (i, s) in policy::hoisted(spec).iter().enumerate() {
            let mut argv = policy::shell_argv(s.shell.as_deref(), &s.run)
                .ok_or_else(|| format!("step {}: unsupported shell", s.id))?;
            if s.shell.is_none() {
                if let Some(shell) = base_env.get("SHELL") {
                    match Path::new(shell).file_name().and_then(|v| v.to_str()) {
                        Some("bash") => argv[0] = shell.clone(),
                        Some("sh") => {
                            argv = vec![shell.clone(), "-e".into(), "-c".into(), s.run.clone()]
                        }
                        Some("zsh") => {
                            argv = vec![
                                shell.clone(),
                                "-f".into(),
                                "-e".into(),
                                "-c".into(),
                                s.run.clone(),
                            ]
                        }
                        _ => return Err("unsupported worker profile shell".into()),
                    }
                }
            }
            let env = match binding {
                Some(b) => b.environment(&s.env).map_err(str::to_string)?,
                None => {
                    let mut env = base_env.clone();
                    env.extend(s.env.clone());
                    env
                }
            };
            nodes.push(NodeSpec {
                node_id: NodeId::parse(&format!("setup-{i}")).map_err(|e| e.to_string())?,
                argv,
                cwd: ws.root.join(&s.dir),
                env,
                weight: None,
                timeout_ms: spec.step_timeout_ms,
            });
        }
        self.write_nodes_bound(
            dir,
            attempt,
            nodes,
            ws.warnings.clone(),
            binding.map(|b| b.fingerprint().clone()),
        )
    }

    fn write_nodes(
        &self,
        dir: &Path,
        attempt: &AttemptId,
        nodes: Vec<NodeSpec>,
        workspace_warnings: Vec<String>,
    ) -> Result<(), String> {
        self.write_nodes_bound(
            dir,
            attempt,
            nodes,
            workspace_warnings,
            self.cfg.profile_fingerprint.clone(),
        )
    }

    fn write_nodes_bound(
        &self,
        dir: &Path,
        attempt: &AttemptId,
        nodes: Vec<NodeSpec>,
        workspace_warnings: Vec<String>,
        fingerprint: Option<crate::ids::Digest>,
    ) -> Result<(), String> {
        let meta: Option<Meta> = observation::read_private(&dir.join("meta.json"));
        let preparation = meta
            .filter(|m| &m.attempt_id == attempt)
            .map(|m| PreparationBinding {
                builder_id: m.builder_id,
                attempt_id: m.attempt_id,
                owner_generation: self.owner_generation.clone(),
                kind: match m.kind {
                    BuilderKind::WorkspaceSetup => PreparationKind::WorkspaceSetup,
                    BuilderKind::ToolInstall => PreparationKind::ToolInstall,
                },
                key: crate::ids::Digest::of_bytes(m.key.as_bytes()),
            });
        let sup = SuperviseSpec {
            preparation,
            profile_fingerprint: fingerprint,
            state_dir: dir.to_path_buf(),
            worker_id: self.cfg.worker_id.clone(),
            attempt_id: attempt.clone(),
            traffic_state_dir: self.cfg.traffic_state_dir.clone(),
            traffic_config: self.cfg.traffic_config.clone(),
            nodes,
            lease_ms: self.cfg.lease_ms,
            term_grace_ms: self.cfg.term_grace_ms,
            kill_wait_ms: self.cfg.kill_wait_ms,
            poll_ms: self.cfg.poll_ms,
            command_timeout_ms: None,
            resource_wait_timeout_ms: None,
            deadline_ms: Some(self.cfg.deadline_ms),
            lease_seed: None,
            inhibit_sleep: self.cfg.inhibit_sleep,
            spool_root: None,
            log_limit_bytes: self.cfg.log_limit_bytes,
            workspace_warnings,
            // no secret reaches a builder; an explicit empty list says so
            mask: MaskValues(Vec::new()),
            artifacts: None,
            prepared_only: false,
        };
        sup.validate().map_err(|e| e.to_string())?;
        let bytes = serde_json::to_vec(&sup).map_err(|e| e.to_string())?;
        write_private(&dir.join("spec.json"), &bytes).map_err(|e| e.to_string())
    }
}

fn private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().expect("a file has a parent");
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

fn unreadable(e: impl std::fmt::Display) -> Unkept {
    Unkept::Unreadable(e.to_string())
}

fn name_of(ent: &std::fs::DirEntry) -> Result<String, Unkept> {
    ent.file_name()
        .into_string()
        .map_err(|n| unreadable(format!("the name {n:?} is not UTF-8")))
}

/// Every entry of the workspace at `root`, by its path relative to it: the
/// directories, and each file and link by the digest of what it holds (the
/// files are read; no time or size is trusted to say that one is unchanged).
/// With `known` (the workspace as it was), a directory that was not there is
/// listed without what is in it: it is new as a whole.
///
/// A `.git` *directory* at the root is listed but not looked into: one that
/// a step made (`git init`) is new, and is a reason not to keep a template;
/// one that was there is a user's own worktree, where no builder works. In
/// a workspace the runner made, `.git` is a file that names a repository
/// elsewhere; it is an entry like any other, and what a step does to the
/// repository itself is outside the workspace, like everything else it
/// does outside.
fn snapshot(
    root: &Path,
    known: Option<&BTreeMap<String, Sig>>,
) -> Result<BTreeMap<String, Sig>, Unkept> {
    let mut out = BTreeMap::new();
    let mut todo = vec![(String::new(), 0usize)];
    while let Some((dir, depth)) = todo.pop() {
        if depth > policy::MAX_DEPTH {
            return Err(unreadable(format!(
                "`{dir}` is deeper than {} directories",
                policy::MAX_DEPTH
            )));
        }
        for ent in std::fs::read_dir(root.join(&dir)).map_err(unreadable)? {
            let ent = ent.map_err(unreadable)?;
            let name = name_of(&ent)?;
            let rel = if dir.is_empty() {
                name
            } else {
                format!("{dir}/{name}")
            };
            let ty = ent.file_type().map_err(unreadable)?;
            let sig = if ty.is_dir() {
                if rel != ".git" && known.is_none_or(|k| k.get(&rel) == Some(&Sig::Dir)) {
                    todo.push((rel.clone(), depth + 1));
                }
                Sig::Dir
            } else if ty.is_symlink() {
                Sig::Link(digest_of(&root.join(&rel)).map_err(unreadable)?)
            } else if ty.is_file() {
                Sig::File(digest_of(&root.join(&rel)).map_err(unreadable)?)
            } else {
                return Err(unreadable(format!(
                    "`{rel}` is neither a file, a directory nor a link"
                )));
            };
            out.insert(rel, sig);
        }
    }
    Ok(out)
}

/// Look at what the steps left at `rel` and under it, before it is kept:
/// a link with an absolute target inside the builder's workspace is made
/// relative (it would dangle once that workspace is removed), any other
/// link that leads out of the workspace, and a `.git` file (a submodule's
/// link to a repository that is not in the template), is a reason not to
/// keep anything. `roots` are the spellings of the workspace root.
fn fix_links(root: &Path, roots: &[String], rel: &str) -> Result<(), Unkept> {
    let mut todo = vec![(rel.to_string(), 0usize)];
    while let Some((rel, depth)) = todo.pop() {
        let path = root.join(&rel);
        let ty = std::fs::symlink_metadata(&path)
            .map_err(unreadable)?
            .file_type();
        if ty.is_dir() {
            if depth >= policy::MAX_DEPTH {
                return Err(unreadable(format!(
                    "`{rel}` is deeper than {} directories",
                    policy::MAX_DEPTH
                )));
            }
            for ent in std::fs::read_dir(&path).map_err(unreadable)? {
                let name = name_of(&ent.map_err(unreadable)?)?;
                todo.push((format!("{rel}/{name}"), depth + 1));
            }
            continue;
        }
        if depth > 0 && rel.rsplit('/').next() == Some(".git") {
            return Err(Unkept::Git(rel));
        }
        if ty.is_symlink() {
            let target = std::fs::read_link(&path).map_err(unreadable)?;
            let Some(target) = target.to_str() else {
                return Err(unreadable(format!("the target of `{rel}` is not UTF-8")));
            };
            match policy::link_plan(&rel, target, roots) {
                LinkPlan::Keep => {}
                LinkPlan::Rewrite(to) => {
                    std::fs::remove_file(&path).map_err(unreadable)?;
                    std::os::unix::fs::symlink(&to, &path).map_err(unreadable)?;
                }
                LinkPlan::Refuse => {
                    return Err(Unkept::Link {
                        path: rel,
                        target: target.to_string(),
                    })
                }
            }
        } else if !ty.is_file() {
            return Err(unreadable(format!(
                "`{rel}` is neither a file, a directory nor a link"
            )));
        }
    }
    Ok(())
}

type Captured = (
    Vec<PreparedArtifact>,
    Vec<PreparedChange>,
    Vec<PreparedRemoval>,
);

/// What the steps did to the workspace at `root`, which was `before`.
fn capture(root: &Path, before: &BTreeMap<String, Sig>) -> Result<Captured, Unkept> {
    let after = snapshot(root, Some(before))?;
    let changes = policy::change_set(before, &after)?;
    let mut roots = vec![root.display().to_string()];
    if let Ok(real) = std::fs::canonicalize(root) {
        roots.push(real.display().to_string());
    }
    let mut artifacts = Vec::new();
    for path in changes.added {
        fix_links(root, &roots, &path)?;
        let digest = digest_of(&root.join(&path)).map_err(unreadable)?;
        artifacts.push(PreparedArtifact { path, digest });
    }
    let mut modified = Vec::new();
    for (path, base) in changes.modified {
        fix_links(root, &roots, &path)?;
        let digest = digest_of(&root.join(&path)).map_err(unreadable)?;
        modified.push(PreparedChange { path, base, digest });
    }
    Ok((artifacts, modified, changes.removed))
}

/// One key context shared by the actual producer and read-only probe.
fn workspace_key_context(
    spec: &SetupSpec,
    manifest: &Digest,
    run: &RunId,
    profile: String,
    env: BTreeMap<String, String>,
) -> KeyContext {
    KeyContext {
        source_digest: manifest.to_string(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        profile,
        run_id: run.to_string(),
        tools: tool_marks(spec, &env),
        env,
    }
}

/// What the tools the spec declares are on this worker: for each name, the
/// file `PATH` (of the steps' environment) resolves it to, with its size
/// and time of change, or that there is none. Part of the template key, so
/// that a tool that was replaced makes another template.
///
/// S20 hook: this is where the measured versions of the worker's tools
/// belong. Until then a tool reached through a shim that does not change
/// when the tool does (a version manager's), and every tool the spec does
/// not declare, is not seen here: a persistent template can outlive such an
/// upgrade unless the spec sets a freshness.
fn tool_marks(spec: &SetupSpec, env: &BTreeMap<String, String>) -> Vec<(String, String)> {
    let path = env.get("PATH").map(String::as_str).unwrap_or("");
    let mark = |name: &str| -> Option<String> {
        if name.is_empty() || name.contains('/') {
            return None;
        }
        let found = path
            .split(':')
            .filter(|d| d.starts_with('/'))
            .map(|d| Path::new(d).join(name))
            .find(|p| p.is_file())?;
        let real = std::fs::canonicalize(&found).ok()?;
        let meta = std::fs::metadata(&real).ok()?;
        let changed = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos());
        Some(format!("{} {} {changed}", real.display(), meta.len()))
    };
    spec.toolchain
        .iter()
        .map(|t| {
            (
                t.name.clone(),
                mark(&t.name).unwrap_or_else(|| "absent".into()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tool_validation_tests {
    use super::*;
    use crate::toolchain::adapters::{
        AdapterKind, AdmittedExecutor, CommandOutput, CommandSpec, FileIdentity, HostTools,
        ResolutionEvidence,
    };
    use crate::toolchain::versions::{
        Constraint, Declaration, FrozenToolchains, Location, MatchPolicy, Scope, ToolId,
    };
    use std::sync::mpsc;

    struct NoWorkspace;
    impl BuilderWorkspaces for NoWorkspace {
        fn create(&self, _: &AttemptId, _: &AttemptPayload) -> Result<Workspace, ApiError> {
            panic!("unexpected workspace")
        }
        fn remove(&self, _: &Workspace) -> Result<(), ApiError> {
            panic!("unexpected workspace cleanup")
        }
    }
    fn fixture() -> (
        tempfile::TempDir,
        Arc<Setups>,
        PreparationRequest,
        adapters::InstallPlan,
    ) {
        let temp = crate::test_tmp::tempdir().unwrap();
        let base = std::fs::canonicalize(temp.path()).unwrap();
        let mut cfg = SetupConfig::new(
            base.join("state"),
            crate::ids::WorkerId::parse("w-validation").unwrap(),
            "/bin/sh".into(),
        );
        cfg.profile_definition = Some(Digest::of_bytes(b"definition"));
        cfg.profile_fingerprint = Some(Digest::of_bytes(b"execution"));
        let setups = Setups::open(cfg, Arc::new(NoWorkspace)).unwrap();
        let root = setups.private_tool_root().unwrap();
        let manager = base.join("node-build");
        std::fs::write(&manager, b"private fake manager; never executed").unwrap();
        std::fs::set_permissions(&manager, std::fs::Permissions::from_mode(0o700)).unwrap();
        let identity = FileIdentity::capture(&manager).unwrap();
        let host = HostTools {
            manager: identity.clone(),
            dependencies: vec![identity],
            search_path: vec![],
            rust_host: String::new(),
        };
        let tool = ToolId::parse("node").unwrap();
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(b"source"),
            Digest::of_bytes(b"definition"),
            scope.clone(),
            &[Declaration {
                tool: tool.clone(),
                constraint: Constraint::parse("1.2.3").unwrap(),
                scope,
                location: Location {
                    path: "captured".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let evidence = ResolutionEvidence {
            contract: frozen.digest(),
            tool: tool.clone(),
            concrete: node_semver::Version::parse("1.2.3").unwrap(),
            catalog: Digest::of_bytes(b"fixture authority"),
            resolved_inputs: BTreeMap::new(),
        };
        let frozen = adapters::resolve(&frozen, &evidence).unwrap();
        let plan =
            adapters::plan(AdapterKind::Nodenv, &frozen, &tool, &evidence, &root, &host).unwrap();
        let request = PreparationRequest {
            kind: AdapterKind::Nodenv,
            frozen,
            tool,
            evidence,
            private_root: root,
            host,
            expected_plan: plan.key(),
            profile_definition: Digest::of_bytes(b"definition"),
            base_profile: Digest::of_bytes(b"execution"),
        };
        (temp, setups, request, plan)
    }
    #[test]
    fn a_remembered_install_failure_is_held_for_the_same_host_tools_and_can_be_lifted() {
        let (_temp, setups, request, _plan) = fixture();
        let key = format!("tool:{}", request.expected_plan);
        let remember = |host: Digest| {
            lock(&setups.inner).failed_tools.insert(
                key.clone(),
                FailedTool {
                    why: "exit 19".into(),
                    at_ms: platform::monotonic_ms(),
                    host,
                },
            );
        };
        // The same host tools: a new consumer is told, nothing is started.
        remember(host_digest(&request));
        let a = AttemptId::generate();
        let ToolPreparationPoll::Failed(held) = setups.poll_tool(&a, &request).unwrap() else {
            panic!("an installer was started")
        };
        assert!(
            held.0.starts_with("exit 19 (this version failed"),
            "{held:?}"
        );
        assert_eq!(setups.builds_started(), 0);
        assert!(lock(&setups.inner).failed_tools.contains_key(&key));
        // Lifting another version leaves it; lifting this one removes it.
        let mut other = request.clone();
        other.expected_plan = Digest::of_bytes(b"another plan");
        setups.retry_tool(&other);
        assert!(lock(&setups.inner).failed_tools.contains_key(&key));
        setups.retry_tool(&request);
        assert!(lock(&setups.inner).failed_tools.is_empty());
        // Recorded with other host tools (the manager was replaced since):
        // forgotten at the first look, and an installer is started.
        remember(Digest::of_bytes(b"other host tools"));
        assert_ne!(host_digest(&request), Digest::of_bytes(b"other host tools"));
        let b = AttemptId::generate();
        assert_eq!(
            setups.poll_tool(&b, &request).unwrap(),
            ToolPreparationPoll::Pending
        );
        assert_eq!(setups.builds_started(), 1);
        setups.release(&a);
        setups.release(&b);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while !lock(&setups.inner).flights.is_empty() || !lock(&setups.inner).by_key.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the builder did not end"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // That installer was lost before it ran (no supervisor here): not a
        // failure of the install itself, so nothing is remembered.
        assert!(lock(&setups.inner).failed_tools.is_empty());
    }
    struct Populate(adapters::InstallPlan);
    impl AdmittedExecutor for Populate {
        fn execute(&mut self, _: &CommandSpec) -> Result<CommandOutput, &'static str> {
            for (_, probe) in self.0.probes() {
                std::fs::create_dir_all(probe.program.parent().unwrap()).unwrap();
                std::fs::write(&probe.program, b"private fake executable").unwrap();
                std::fs::set_permissions(&probe.program, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
            }
            Ok(CommandOutput {
                success: true,
                stdout: b"1.2.3".to_vec(),
            })
        }
    }
    fn seed(
        setups: &Setups,
        consumer: &AttemptId,
        kind: BuilderKind,
    ) -> (BuilderId, AttemptId, WorkerStore) {
        let id = BuilderId::generate();
        let attempt = AttemptId::generate();
        let dir = setups.cfg.builders().join(id.as_str());
        private_dir(&dir).unwrap();
        let store = setups.accept(&dir, &attempt).unwrap();
        let consumer_key = consumer.to_string();
        let key = format!("fixture:{id}");
        let mut inner = lock(&setups.inner);
        inner.consumers.insert(
            consumer_key.clone(),
            Consumer {
                builder: Some(id.clone()),
                ..Consumer::default()
            },
        );
        inner.flights.insert(
            id.clone(),
            Flight {
                key,
                attempt_id: attempt.clone(),
                refs: BTreeSet::from([consumer_key]),
                end: None,
                stop: false,
                kind,
            },
        );
        (id, attempt, store)
    }

    #[test]
    fn blocked_detect_and_receipt_verification_allow_durable_renew_cancel_finish_and_ownership_reads(
    ) {
        for receipt_path in [false, true] {
            let (_temp, setups, request, plan) = fixture();
            let checked_consumer = AttemptId::generate();
            if receipt_path {
                let receipt = adapters::install_admitted(
                    &plan,
                    &request.private_root,
                    &mut Populate(plan.clone()),
                )
                .unwrap();
                let builder = BuilderId::generate();
                let consumer_key = format!("{checked_consumer}/tool:node");
                let mut inner = lock(&setups.inner);
                inner.consumers.insert(
                    consumer_key.clone(),
                    Consumer {
                        builder: Some(builder.clone()),
                        binding: Some(request.digest()),
                        binding_generation: Some(BuilderId::generate()),
                        ..Consumer::default()
                    },
                );
                inner.flights.insert(
                    builder,
                    Flight {
                        key: format!("tool:{}", plan.key()),
                        attempt_id: AttemptId::generate(),
                        refs: BTreeSet::from([consumer_key]),
                        end: Some(BuilderResult::Tool(Ok(Box::new(receipt)))),
                        stop: false,
                        kind: BuilderKind::ToolInstall,
                    },
                );
            }
            let workspace_consumer = AttemptId::generate();
            let tool_consumer = AttemptId::generate();
            let workspace = seed(&setups, &workspace_consumer, BuilderKind::WorkspaceSetup);
            let tool = seed(&setups, &tool_consumer, BuilderKind::ToolInstall);
            let (entered_tx, entered_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let (progress_tx, progress_rx) = mpsc::channel();
            std::thread::scope(|scope| {
                let s = setups.clone();
                let req = request.clone();
                let p = plan.clone();
                let consumer = checked_consumer.clone();
                let validation = scope.spawn(move || {
                    s.poll_tool_checked(&consumer, &req, &p, |receipt| {
                        assert_eq!(receipt.is_some(), receipt_path);
                        entered_tx.send(()).unwrap();
                        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                        match receipt {
                            Some(receipt) => {
                                receipt.validate(&p).unwrap();
                                ToolPreparationPoll::Installed(receipt)
                            }
                            None => {
                                assert_eq!(adapters::detect(&p), Availability::Missing);
                                ToolPreparationPoll::Pending
                            }
                        }
                    })
                });
                entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                let progress = scope.spawn(|| {
                    for (consumer, (id, attempt, store), kind) in [
                        (&workspace_consumer, &workspace, BuilderKind::WorkspaceSetup),
                        (&tool_consumer, &tool, BuilderKind::ToolInstall),
                    ] {
                        let (refs, stop) = setups.wanted(id);
                        assert!(policy::renew_lease(refs, stop));
                        let key = LeaseKey {
                            coordinator_epoch: CoordinatorEpoch(1),
                            lease_seq: 1,
                        };
                        let mut lease = BuilderLease {
                            last: 0,
                            seq: 0,
                            cancelled: false,
                        };
                        setups.lease_tick(id, attempt, store, &mut lease, || {
                            (setups.cfg.lease_ms, 42_000)
                        });
                        assert_eq!(store.lease(attempt).unwrap(), Some((key, false)));
                        assert_eq!(store.lease_renewed_at(attempt).unwrap(), Some(42_000));
                        assert!(setups.owns_workspace(attempt));
                        setups.release(consumer);
                        let (refs, stop) = setups.wanted(id);
                        assert_eq!((refs, stop), (0, true));
                        assert!(!policy::renew_lease(refs, stop));
                        setups.lease_tick(id, attempt, store, &mut lease, || {
                            (setups.cfg.lease_ms + 1, 42_001)
                        });
                        assert_eq!(
                            store.get(attempt).unwrap().unwrap().cancel_requested_at_ms,
                            Some(42_001)
                        );
                        setups.finish(
                            id,
                            match kind {
                                BuilderKind::WorkspaceSetup => {
                                    BuilderResult::Workspace(BuildEnd::Cancelled)
                                }
                                BuilderKind::ToolInstall => {
                                    BuilderResult::Tool(Err(PreparationFailure("cancelled".into())))
                                }
                            },
                        );
                        assert!(!setups.owns_workspace(attempt));
                    }
                    setups.release(&checked_consumer);
                    progress_tx.send(()).unwrap();
                });
                // This acknowledgment must arrive while validation is still gated;
                // it observes real durable renewals and cancellation, not a timing benchmark.
                let progressed = progress_rx.recv_timeout(Duration::from_secs(5));
                resume_tx.send(()).unwrap();
                assert!(
                    progressed.is_ok(),
                    "validation held the shared lifecycle lock"
                );
                progress.join().unwrap();
                assert!(matches!(
                    validation.join().unwrap().unwrap(),
                    ToolPreparationPoll::Failed(_)
                ));
            });
            assert_eq!(
                setups.builds_started(),
                0,
                "released validator must not resurrect a builder"
            );
        }
    }

    #[test]
    fn released_and_rebound_identical_consumer_cannot_accept_old_validation() {
        let (_temp, setups, request, plan) = fixture();
        let consumer = AttemptId::generate();
        let polled = setups.poll_tool_checked(&consumer, &request, &plan, |_| {
            setups.release(&consumer);
            lock(&setups.inner).consumers.insert(
                format!("{consumer}/tool:node"),
                Consumer {
                    binding: Some(request.digest()),
                    binding_generation: Some(BuilderId::generate()),
                    ..Consumer::default()
                },
            );
            ToolPreparationPoll::Pending
        });
        assert!(polled.is_err());
        assert_eq!(setups.builds_started(), 0);
    }

    #[test]
    fn validation_rechecks_settlement_barrier_and_completed_concurrent_writer_generation() {
        let (_temp, setups, request, plan) = fixture();
        let consumer = AttemptId::generate();
        let key = format!("tool:{}", plan.key());
        let polled = setups
            .poll_tool_checked(&consumer, &request, &plan, |_| {
                let mut inner = lock(&setups.inner);
                // Another writer started and settled while detection was reading.
                inner
                    .tool_versions
                    .insert(key.clone(), BuilderId::generate());
                ToolPreparationPoll::Pending
            })
            .unwrap();
        assert_eq!(polled, ToolPreparationPoll::Pending);
        assert_eq!(setups.builds_started(), 0);
        let polled = setups
            .poll_tool_checked(&consumer, &request, &plan, |_| {
                lock(&setups.inner)
                    .barriers
                    .insert(key.clone(), AttemptId::generate());
                ToolPreparationPoll::Failed(PreparationFailure("stale read must be ignored".into()))
            })
            .unwrap();
        assert_eq!(polled, ToolPreparationPoll::Pending);
        assert_eq!(setups.builds_started(), 0);
    }
    #[test]
    fn observation_handoff_checks_generation_cancel_barriers_and_bounded_cache() {
        let (_temp, setups, _, _) = fixture();
        let consumer = AttemptId::generate();
        let (id, attempt, _store) = seed(&setups, &consumer, BuilderKind::ToolInstall);
        let key = format!("fixture:{id}");
        let dir = setups.cfg.builders().join(id.as_str());
        write_private(
            &dir.join("meta.json"),
            &serde_json::to_vec(&Meta {
                kind: BuilderKind::ToolInstall,
                builder_id: id.clone(),
                attempt_id: attempt.clone(),
                key: key.clone(),
            })
            .unwrap(),
        )
        .unwrap();
        setups
            .write_nodes(
                &dir,
                &attempt,
                vec![NodeSpec {
                    node_id: NodeId::parse("tool-install").unwrap(),
                    argv: vec!["/usr/bin/true".into()],
                    cwd: dir.clone(),
                    env: BTreeMap::new(),
                    weight: None,
                    timeout_ms: None,
                }],
                vec![],
            )
            .unwrap();
        let spec = SuperviseSpec::from_file(&dir.join("spec.json")).unwrap();
        observation::record_node(&spec, 0, 0.01).unwrap();
        let value = observation::aggregate(&spec).unwrap();
        {
            let mut inner = lock(&setups.inner);
            inner.by_key.insert(key.clone(), id.clone());
            inner.barriers.insert(key.clone(), attempt.clone());
            inner.tool_versions.insert(key.clone(), id.clone());
        }
        for variant in 0..4 {
            {
                let mut inner = lock(&setups.inner);
                match variant {
                    0 => inner.flights.get_mut(&id).unwrap().stop = true,
                    1 => {
                        inner.barriers.remove(&key);
                    }
                    2 => {
                        inner
                            .tool_versions
                            .insert(key.clone(), BuilderId::generate());
                    }
                    _ => inner.flights.get_mut(&id).unwrap().attempt_id = AttemptId::generate(),
                }
            }
            setups.handoff_observation(&id, &attempt, &key, BuilderKind::ToolInstall, &dir);
            assert!(setups.preparation_observations(64).is_empty());
            let mut inner = lock(&setups.inner);
            inner.flights.get_mut(&id).unwrap().stop = false;
            inner.flights.get_mut(&id).unwrap().attempt_id = attempt.clone();
            inner.barriers.insert(key.clone(), attempt.clone());
            inner.tool_versions.insert(key.clone(), id.clone());
        }
        let mut foreign = spec.clone();
        foreign.preparation.as_mut().unwrap().owner_generation = BuilderId::generate();
        write_private(
            &dir.join("spec.json"),
            &serde_json::to_vec(&foreign).unwrap(),
        )
        .unwrap();
        setups.handoff_observation(&id, &attempt, &key, BuilderKind::ToolInstall, &dir);
        assert!(setups.preparation_observations(64).is_empty());
        write_private(&dir.join("spec.json"), &serde_json::to_vec(&spec).unwrap()).unwrap();
        {
            let mut inner = lock(&setups.inner);
            for _ in 0..4096 {
                let mut other = value.clone();
                other.binding.builder_id = BuilderId::generate();
                inner.observations.insert(other);
            }
        }
        setups.handoff_observation(&id, &attempt, &key, BuilderKind::ToolInstall, &dir);
        assert_eq!(lock(&setups.inner).observations.len(), 4096);
        assert!(!lock(&setups.inner).observations.contains_key(&id));
        assert_eq!(setups.preparation_observations(usize::MAX).len(), 64);
        lock(&setups.inner).observations = Default::default();
        setups.handoff_observation(&id, &attempt, &key, BuilderKind::ToolInstall, &dir);
        assert_eq!(setups.preparation_observations(64), vec![value.clone()]);
        setups.handoff_observation(&id, &attempt, &key, BuilderKind::ToolInstall, &dir);
        assert_eq!(setups.preparation_observations(64), vec![value]);
    }
}

mod probe;
