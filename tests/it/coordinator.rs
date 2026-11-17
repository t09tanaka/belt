//! The coordinator against workers that keep the worker side of the
//! contract (spec chapter 11) in memory: A19 (lost ACK, retire before a new
//! placement), A21 (a link that is gone), A22 (the parent stopped for long),
//! A23 (a restarted parent adopts), with a crash of the parent at each
//! boundary of a placement: recorded and not sent, sent and the ACK lost,
//! retire sent and not confirmed.
//!
//! No process is started here. A "crash" is the coordinator dropped where it
//! stands and its database opened again.

use crate::common::{alive, read_pid};
use crate::worker_agent;
use remote_task_runner::coordinator::policy::Limits;
use remote_task_runner::coordinator::ClientLink;
use remote_task_runner::coordinator::{
    Coordinator, CoordinatorConfig, CoordinatorError, LinkError, TaskSpec, WorkerLink,
};
use remote_task_runner::error::{ApiError, BlockedReason, ConflictReason, ErrorCode};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{connect, ClientConfig};
use remote_task_runner::results::{CollectConfig, CollectReport, ResultStore};
use remote_task_runner::scheduler::{CostModel, Scheduler, SchedulerConfig, UnknownCosts};
use remote_task_runner::state::{
    AttemptState, Observation, Outcome, Receipt, Receipts, Termination,
};
use remote_task_runner::storage::{AttemptRecord, CoordinatorStore, StorageError};
use remote_task_runner::wire::*;
use remote_task_runner::worker::policy::{load_counts, retire_status};
use remote_task_runner::worker::serve::serve_connection;
use remote_task_runner::worker::Agent;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Semaphore;

// ------------------------------------------------------------ the workers

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Inspect,
    Forecast,
    Get,
    Submit,
    Cancel,
    Retire,
    Renew,
}

/// What a fake worker says when it is asked for a forecast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Says {
    /// A finish time (`measured`), or a ranking (`fallback`).
    Number(RuntimeSource),
    /// No number: a command of its own ran past its history.
    Nothing,
    /// An error.
    Error,
    /// It never answers.
    Silence,
}

/// How a fake worker forecasts: every task it holds, and every one the
/// question says is on its way, takes `run_ms`, one after the other; the
/// asked task is ready after the delay the question names, which overlaps
/// that queue.
#[derive(Debug, Clone)]
pub(crate) struct Forecasting {
    pub(crate) run_ms: u64,
    pub(crate) says: Says,
    pub(crate) boot: &'static str,
    pub(crate) epoch: u64,
    pub(crate) ttl_ms: u64,
    /// The answer takes this long.
    pub(crate) delay_ms: u64,
}

/// Where a request is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lost {
    /// It never arrives.
    Request,
    /// It is carried out and its answer never arrives.
    Answer,
}

#[derive(Debug, Clone)]
pub(crate) struct Held {
    pub(crate) payload: AttemptPayload,
    pub(crate) hash: PayloadHash,
    pub(crate) state: AttemptState,
    pub(crate) termination: Option<Termination>,
    pub(crate) receipts: Receipts,
    pub(crate) retired: bool,
    pub(crate) launches: u32,
    pub(crate) lease: Option<LeaseKey>,
    pub(crate) renewals: u32,
}

/// Which worker runs which task now, over all workers of a test: a task
/// that runs in two places at once is the failure everything here prevents.
#[derive(Default)]
pub(crate) struct Cluster {
    running: Mutex<BTreeMap<TaskId, String>>,
    starts: Mutex<Vec<(TaskId, AttemptId, String)>>,
}

impl Cluster {
    pub(crate) fn starts_of(&self, task: &TaskId) -> Vec<String> {
        let starts = self.starts.lock().unwrap();
        starts
            .iter()
            .filter(|(t, _, _)| t == task)
            .map(|(_, _, w)| w.clone())
            .collect()
    }
}

pub(crate) struct State {
    capabilities: Option<remote_task_runner::toolchain::capability::WorkerCapabilities>,
    pub(crate) max_epoch: u64,
    pub(crate) held: BTreeMap<AttemptId, Held>,
    pub(crate) tombstones: BTreeSet<AttemptId>,
    pub(crate) offline: bool,
    pub(crate) lose: Vec<(Op, Lost)>,
    pub(crate) overloaded: bool,
    pub(crate) refuse: bool,
    /// What a new submit is answered with instead of being taken.
    pub(crate) fault: Option<ApiError>,
    pub(crate) slow_stop: bool,
    /// How a command that is being retired ends, when not by the stop.
    pub(crate) ends_in_retire: Option<Outcome>,
    /// The `get_attempt` that follows a retire gets no answer.
    pub(crate) lose_get_after_retire: bool,
    /// After `inspect`, nothing is answered and nothing returns.
    pub(crate) stall: bool,
    pub(crate) draining: bool,
    pub(crate) limits: (u32, u32),
    /// The requests that were carried out, in order.
    pub(crate) log: Vec<(Op, AttemptId)>,
    /// What `inspect` says its id is.
    pub(crate) says: WorkerId,
    pub(crate) forecasting: Forecasting,
    /// Every forecast question that arrived, in order.
    pub(crate) asked: Vec<ForecastRequest>,
    /// Every request that arrived, answered or not, in order.
    pub(crate) received: Vec<Op>,
}

pub(crate) struct FakeWorker {
    pub(crate) id: WorkerId,
    cluster: Arc<Cluster>,
    pub(crate) state: Mutex<State>,
}

fn err(code: ErrorCode) -> ApiError {
    ApiError::new(code, format!("{code}"))
}

fn ended(outcome: Outcome) -> Termination {
    Termination::new(outcome, None, None).unwrap()
}

impl FakeWorker {
    fn new(name: &str, cluster: &Arc<Cluster>, limits: (u32, u32)) -> Arc<FakeWorker> {
        let id = WorkerId::parse(name).unwrap();
        Arc::new(FakeWorker {
            id: id.clone(),
            cluster: Arc::clone(cluster),
            state: Mutex::new(State {
                capabilities: None,
                max_epoch: 0,
                held: BTreeMap::new(),
                tombstones: BTreeSet::new(),
                offline: false,
                lose: Vec::new(),
                overloaded: false,
                refuse: false,
                fault: None,
                slow_stop: false,
                ends_in_retire: None,
                lose_get_after_retire: false,
                stall: false,
                draining: false,
                limits,
                log: Vec::new(),
                says: id,
                forecasting: Forecasting {
                    run_ms: 10_000,
                    says: Says::Number(RuntimeSource::Measured),
                    boot: "boot-1",
                    epoch: 1,
                    // Long: a test that is about something else must not
                    // turn on how fast the machine is.
                    ttl_ms: 600_000,
                    delay_ms: 0,
                },
                asked: Vec::new(),
                received: Vec::new(),
            }),
        })
    }

    pub(crate) fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    pub(crate) fn held(&self, a: &AttemptId) -> Option<Held> {
        self.state.lock().unwrap().held.get(a).cloned()
    }

    pub(crate) fn holds(&self) -> Vec<AttemptId> {
        self.state.lock().unwrap().held.keys().cloned().collect()
    }

    pub(crate) fn count(&self, op: Op) -> usize {
        let s = self.state.lock().unwrap();
        s.log.iter().filter(|(o, _)| *o == op).count()
    }

    /// How many requests of a kind arrived, answered or not.
    pub(crate) fn received(&self, op: Op) -> usize {
        let s = self.state.lock().unwrap();
        s.received.iter().filter(|o| **o == op).count()
    }

    /// The worker starts the command of an accepted attempt.
    pub(crate) fn start(&self, a: &AttemptId) {
        let mut s = self.state.lock().unwrap();
        let h = s.held.get_mut(a).expect("the worker holds the attempt");
        assert_eq!(
            h.state,
            AttemptState::Accepted,
            "started once, from accepted"
        );
        assert!(!h.retired);
        h.state = AttemptState::Active;
        h.launches += 1;
        h.receipts.mark(Receipt::CommandRegistered, 1);
        h.receipts.mark(Receipt::Running, 1);
        let task = h.payload.task_id.clone();
        let name = self.id.to_string();
        let other = self
            .cluster
            .running
            .lock()
            .unwrap()
            .insert(task.clone(), name.clone());
        assert_eq!(other, None, "task {task} already runs on another worker");
        self.cluster
            .starts
            .lock()
            .unwrap()
            .push((task, a.clone(), name));
    }

    fn stop(&self, h: &mut Held, outcome: Outcome) {
        if h.state == AttemptState::Active {
            self.cluster
                .running
                .lock()
                .unwrap()
                .remove(&h.payload.task_id);
        }
        h.state = AttemptState::Terminal;
        h.termination = Some(ended(outcome));
    }

    /// The command ends, or the worker stops it by itself (lease expiry).
    pub(crate) fn finish(&self, a: &AttemptId, outcome: Outcome) {
        let mut s = self.state.lock().unwrap();
        let mut h = s.held.remove(a).expect("the worker holds the attempt");
        self.stop(&mut h, outcome);
        s.held.insert(a.clone(), h);
    }

    fn status(&self, a: &AttemptId, h: &Held) -> AttemptStatus {
        AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: h.payload.run_id.clone(),
            task_id: h.payload.task_id.clone(),
            generation: h.payload.generation,
            worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
            agent_epoch: AgentEpoch(1),
            state: h.state,
            receipts: h.receipts,
            spawned_at_ms: None,
            observation: Observation::Known,
            termination: h.termination,
            nodes: vec![],
            steps: vec![],
            retired: h.retired,
            log_truncated: false,
            log_dropped_bytes: 0,
        }
    }

    fn epoch(s: &mut State, epoch: CoordinatorEpoch) -> Result<(), ApiError> {
        if epoch.0 < s.max_epoch {
            return Err(err(ErrorCode::StaleEpoch));
        }
        s.max_epoch = epoch.0;
        Ok(())
    }

    /// Carry one request out as a worker would.
    fn handle(&self, request: Request) -> Result<Response, ApiError> {
        let mut s = self.state.lock().unwrap();
        match request {
            Request::Inspect(_) => {
                let (active, pending) = load_counts(s.held.values().map(|h| h.state));
                Ok(Response::Inspect(InspectResponse {
                    preparation_observations: None,
                    capabilities: s.capabilities.clone(),
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: s.says.clone(),
                    worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
                    agent_epoch: AgentEpoch(1),
                    snapshot_digest: Digest::of_bytes(b"s"),
                    policy_digest: Digest::of_bytes(b"p"),
                    active_task: active,
                    prefetch_pending: pending,
                    max_active_task: s.limits.0,
                    prefetch_per_worker: s.limits.1,
                    disk_free_bytes: 1,
                    draining: s.draining,
                }))
            }
            Request::GetAttempt(r) => match s.held.get(&r.attempt_id) {
                Some(h) => Ok(Response::GetAttempt(self.status(&r.attempt_id, h))),
                None if s.tombstones.contains(&r.attempt_id) => Err(err(ErrorCode::Retired)),
                None => Err(err(ErrorCode::NotFound)),
            },
            Request::SubmitAttempt(r) => {
                if !r.hash_matches().unwrap() {
                    return Err(err(ErrorCode::InvalidInput));
                }
                Self::epoch(&mut s, r.coordinator_epoch)?;
                let retired = s.held.get(&r.attempt_id).is_some_and(|h| h.retired);
                if retired || s.tombstones.contains(&r.attempt_id) {
                    return Err(err(ErrorCode::Retired));
                }
                if let Some(h) = s.held.get(&r.attempt_id) {
                    if h.hash != r.payload_hash {
                        return Err(ApiError::conflict(
                            ConflictReason::PayloadMismatch,
                            "another payload",
                        ));
                    }
                    return Ok(Response::SubmitAttempt(SubmitAttemptResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        generation: h.payload.generation,
                        state: h.state,
                        receipts: h.receipts,
                        duplicate: true,
                    }));
                }
                if s.refuse {
                    return Err(err(ErrorCode::InvalidInput));
                }
                if let Some(fault) = s.fault.clone() {
                    return Err(fault);
                }
                if s.overloaded {
                    return Err(err(ErrorCode::Overloaded));
                }
                let mut receipts = Receipts::default();
                receipts.mark(Receipt::WorkerAccepted, 1);
                let generation = r.payload.generation;
                s.log.push((Op::Submit, r.attempt_id.clone()));
                s.held.insert(
                    r.attempt_id.clone(),
                    Held {
                        payload: r.payload,
                        hash: r.payload_hash,
                        state: AttemptState::Accepted,
                        termination: None,
                        receipts,
                        retired: false,
                        launches: 0,
                        lease: None,
                        renewals: 0,
                    },
                );
                Ok(Response::SubmitAttempt(SubmitAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    generation,
                    state: AttemptState::Accepted,
                    receipts,
                    duplicate: false,
                }))
            }
            Request::CancelAttempt(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                s.log.push((Op::Cancel, r.attempt_id.clone()));
                let Some(mut h) = s.held.remove(&r.attempt_id) else {
                    s.tombstones.insert(r.attempt_id.clone());
                    return Ok(Response::CancelAttempt(CancelAttemptResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        phase: CancelPhase::Accepted,
                        state: None,
                        tombstone: true,
                    }));
                };
                if h.state != AttemptState::Terminal {
                    self.stop(&mut h, Outcome::Cancelled);
                }
                s.held.insert(r.attempt_id.clone(), h);
                Ok(Response::CancelAttempt(CancelAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    phase: CancelPhase::Completed,
                    state: Some(AttemptState::Terminal),
                    tombstone: false,
                }))
            }
            Request::RetireAttempt(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                s.log.push((Op::Retire, r.attempt_id.clone()));
                s.tombstones.insert(r.attempt_id.clone());
                let slow = s.slow_stop;
                let stopped_as = s.ends_in_retire.unwrap_or(Outcome::Cancelled);
                let (known, was, now, launched) = match s.held.remove(&r.attempt_id) {
                    None => (false, false, false, false),
                    Some(mut h) => {
                        let was = h.state == AttemptState::Terminal;
                        h.retired = true;
                        if !was && !slow {
                            self.stop(&mut h, stopped_as);
                        }
                        let out = (true, was, h.state == AttemptState::Terminal, h.launches > 0);
                        s.held.insert(r.attempt_id.clone(), h);
                        out
                    }
                };
                Ok(Response::RetireAttempt(RetireAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    tombstone: true,
                    status: retire_status(known, was, now, launched),
                }))
            }
            Request::RenewLease(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                let key = r.key();
                let Some(h) = s.held.get_mut(&r.attempt_id) else {
                    return Err(err(ErrorCode::NotFound));
                };
                if h.retired || h.state == AttemptState::Terminal {
                    return Err(err(ErrorCode::LeaseNotRenewable));
                }
                let extended = key.extends(h.lease);
                if extended {
                    h.lease = Some(key);
                    h.renewals += 1;
                }
                let state = h.state;
                s.log.push((Op::Renew, r.attempt_id.clone()));
                Ok(Response::RenewLease(RenewLeaseResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    lease_seq: r.lease_seq,
                    extended,
                    lease_remaining_ms: r.lease_ms,
                    state,
                }))
            }
            Request::Forecast(r) => {
                Self::epoch(&mut s, r.coordinator_epoch)?;
                s.asked.push(r.clone());
                s.log.push((Op::Forecast, r.workload.attempt_id.clone()));
                let f = s.forecasting.clone();
                if f.says == Says::Error {
                    return Err(err(ErrorCode::Internal));
                }
                let held = s
                    .held
                    .iter()
                    .filter(|(_, h)| h.state != AttemptState::Terminal)
                    .count();
                let coming = r
                    .pending
                    .iter()
                    .filter(|p| !s.held.contains_key(&p.attempt_id))
                    .count();
                let queue = (held + coming) as u64 * f.run_ms;
                let nodes = r.workload.nodes.iter();
                let ready = nodes.filter_map(|n| n.external_delay_ms).max().unwrap_or(0);
                let finish = ready.max(queue) + f.run_ms;
                let wait = queue.saturating_sub(ready);
                let mut null_reasons = BTreeMap::new();
                let (finish_in_ms, execution_ms, ranking, runtime_source) = match f.says {
                    Says::Number(RuntimeSource::Measured) => {
                        (Some(finish), Some(f.run_ms), None, RuntimeSource::Measured)
                    }
                    Says::Number(source) => {
                        null_reasons.insert(ForecastField::ExecutionMs, Uncertainty::NoHistory);
                        null_reasons.insert(ForecastField::FinishInMs, Uncertainty::NoHistory);
                        let ranking = ForecastRanking {
                            start_in_ms: Some(ready.max(queue)),
                            finish_in_ms: Some(finish),
                        };
                        (None, None, Some(ranking), source)
                    }
                    _ => {
                        null_reasons.insert(ForecastField::FinishInMs, Uncertainty::Overrun);
                        (None, Some(f.run_ms), None, RuntimeSource::Measured)
                    }
                };
                let known = f.says != Says::Nothing;
                Ok(Response::Forecast(ForecastResponse {
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: s.says.clone(),
                    worker_boot_id: WorkerBootId::parse(f.boot).unwrap(),
                    agent_epoch: AgentEpoch(f.epoch),
                    snapshot_digest: Digest::of_bytes(b"s"),
                    policy_digest: Digest::of_bytes(b"p"),
                    ttl_ms: f.ttl_ms,
                    coverage: vec![],
                    target: ForecastTarget {
                        plan_digest: r.workload.plan_digest.clone(),
                        ready_in_ms: Some(ready),
                        queue_wait_after_ready_ms: known.then_some(wait),
                        execution_ms,
                        finish_in_ms,
                        confidence: Confidence::Medium,
                        runtime_source,
                        history_count: 0,
                        conditional: false,
                        overrun: !known,
                        wait_reason: None,
                        assumptions: vec![],
                        null_reasons,
                        ranking,
                    },
                }))
            }
            other => panic!("the coordinator core does not send {other:?}"),
        }
    }
}

fn op_of(request: &Request) -> Op {
    match request {
        Request::Inspect(_) => Op::Inspect,
        Request::Forecast(_) => Op::Forecast,
        Request::GetAttempt(_) => Op::Get,
        Request::SubmitAttempt(_) => Op::Submit,
        Request::CancelAttempt(_) => Op::Cancel,
        Request::RetireAttempt(_) => Op::Retire,
        Request::RenewLease(_) => Op::Renew,
        other => panic!("{other:?}"),
    }
}

impl WorkerLink for FakeWorker {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let op = op_of(&request);
            self.state.lock().unwrap().received.push(op);
            let stall = self.state.lock().unwrap().stall;
            if stall && op != Op::Inspect {
                // Never answered; only the caller's own deadline ends it.
                std::future::pending::<()>().await;
            }
            if op == Op::Forecast {
                let f = self.state.lock().unwrap().forecasting.clone();
                if f.says == Says::Silence {
                    std::future::pending::<()>().await;
                }
                if f.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(f.delay_ms)).await;
                }
            }
            let lost = {
                let mut s = self.state.lock().unwrap();
                let after_retire = matches!(s.log.last(), Some((Op::Retire, _)));
                if s.offline || (s.lose_get_after_retire && op == Op::Get && after_retire) {
                    Some(Lost::Request)
                } else {
                    let at = s.lose.iter().position(|(o, _)| *o == op);
                    at.map(|i| s.lose.remove(i).1)
                }
            };
            match lost {
                Some(Lost::Request) => Err(LinkError::Unconfirmed),
                Some(Lost::Answer) => {
                    let _ = self.handle(request);
                    Err(LinkError::Unconfirmed)
                }
                None => self.handle(request).map_err(LinkError::Remote),
            }
        })
    }
}

// ------------------------------------------------------------ the fixture

pub(crate) struct Fx {
    dir: TempDir,
    pub(crate) cluster: Arc<Cluster>,
    pub(crate) cfg: CoordinatorConfig,
    pub(crate) sched: SchedulerConfig,
    pub(crate) costs: Arc<dyn CostModel>,
}

pub(crate) fn fx() -> Fx {
    Fx {
        dir: crate::test_tmp::tempdir().unwrap(),
        cluster: Arc::default(),
        cfg: CoordinatorConfig {
            limits: Limits {
                max_active_task: 2,
                prefetch_per_worker: 1,
            },
            lease_ms: 60_000,
            worker_budget_ms: 10_000,
        },
        sched: {
            let mut sched = SchedulerConfig::default();
            // See the fake's `ttl_ms`.
            sched.rules.forecast_ttl_ms = 600_000;
            sched
        },
        costs: Arc::new(UnknownCosts),
    }
}

impl Fx {
    /// Start a coordinator on the state directory: the first, or the next
    /// after the one before was dropped.
    pub(crate) fn open(&self, workers: &[&Arc<FakeWorker>]) -> Coordinator {
        // The lock of the coordinator before is released when it is dropped,
        // but a child another test forks at that moment holds a copy of the
        // lock file until it execs: wait that moment out.
        let started = std::time::Instant::now();
        let store = loop {
            match CoordinatorStore::open(self.dir.path(), 1) {
                Err(StorageError::Locked(_)) if started.elapsed() < Duration::from_secs(5) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                other => break other.unwrap(),
            }
        };
        let sched = Scheduler::new(self.sched, Arc::clone(&self.costs)).unwrap();
        let c = Coordinator::start_with(store, self.cfg, sched).unwrap();
        for w in workers {
            c.try_attach(w.id.clone(), Arc::clone(w) as Arc<dyn WorkerLink>)
                .unwrap();
        }
        c
    }

    pub(crate) fn worker(&self, name: &str, limits: (u32, u32)) -> Arc<FakeWorker> {
        FakeWorker::new(name, &self.cluster, limits)
    }
}

pub(crate) fn spec() -> TaskSpec {
    TaskSpec {
        toolchains: None,
        outputs: Vec::new(),
        requirements: Default::default(),
        in_place: false,
        setup: None,
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::of_bytes(b"manifest"),
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![],
    }
}

/// A source the parent holds (a run refers to one).
pub(crate) fn source(c: &Coordinator) -> SourceId {
    let id = SourceId::generate();
    c.store()
        .put_source_ref(&id, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    id
}

pub(crate) async fn enqueue(c: &Coordinator, n: usize) -> Vec<TaskId> {
    let tasks: Vec<TaskId> = (0..n).map(|_| TaskId::generate()).collect();
    let specs = tasks.iter().map(|t| (t.clone(), spec())).collect();
    c.enqueue(RunId::generate(), source(c), specs)
        .await
        .unwrap();
    tasks
}

pub(crate) fn attempts(c: &Coordinator, task: &TaskId) -> Vec<AttemptRecord> {
    c.store().attempts_of_task(task).unwrap()
}

/// The one attempt of a task.
pub(crate) fn only(c: &Coordinator, task: &TaskId) -> AttemptRecord {
    let mut all = attempts(c, task);
    assert_eq!(all.len(), 1, "{all:?}");
    all.remove(0)
}

pub(crate) fn queued(c: &Coordinator) -> Vec<TaskId> {
    let q = c.store().queued_tasks(100).unwrap();
    q.into_iter().map(|t| t.task_id).collect()
}

// --------------------------------------------------------------- dispatch

#[tokio::test]
async fn dispatch_fills_the_free_active_slots_and_the_prefetch_and_no_more() {
    let f = fx();
    // The worker's own limits (1 active, 1 prefetch) are stricter than the
    // coordinator's (2, 1).
    let a = f.worker("w-a", (1, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 5).await;

    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2, "one to run and one prefetched");
    assert_eq!(a.holds().len(), 2);
    let first = only(&c, &tasks[0]);
    let second = only(&c, &tasks[1]);
    assert_eq!(
        report.placed,
        vec![first.attempt_id.clone(), second.attempt_id.clone()]
    );
    assert_eq!(queued(&c), tasks[2..].to_vec(), "oldest first");
    assert!(first.receipts.reached(Receipt::WorkerAccepted));
    assert!(c.store().unacked_demand().unwrap().is_empty());

    // Nothing more while both slots are held, started or not.
    assert!(c.tick().await.unwrap().placed.is_empty());
    a.start(&first.attempt_id);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(only(&c, &tasks[0]).state, AttemptState::Active);
    assert!(only(&c, &tasks[0]).receipts.reached(Receipt::Running));

    // The end of one frees one.
    a.finish(&first.attempt_id, Outcome::Succeeded);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    assert_eq!(report.placed.len(), 1);
    assert_eq!(only(&c, &tasks[2]).worker_id, a.id);
    assert_eq!(
        only(&c, &tasks[0]).termination.unwrap().outcome(),
        Outcome::Succeeded
    );
    assert_eq!(queued(&c), tasks[3..].to_vec());

    // A second worker takes what the first has no room for; a draining one
    // takes nothing.
    let b = f.worker("w-b", (9, 9));
    let d = f.worker("w-d", (9, 9));
    d.set(|s| s.draining = true);
    c.attach(b.id.clone(), b.clone()).await;
    c.attach(d.id.clone(), d.clone()).await;
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2);
    assert_eq!(only(&c, &tasks[3]).worker_id, b.id);
    assert_eq!(only(&c, &tasks[4]).worker_id, b.id);
    assert!(d.holds().is_empty());
    assert!(queued(&c).is_empty());
}

#[tokio::test]
async fn the_coordinators_own_limits_hold_when_the_worker_allows_more() {
    let f = fx();
    let a = f.worker("w-a", (50, 50));
    let c = f.open(&[&a]);
    enqueue(&c, 6).await;
    assert_eq!(
        c.tick().await.unwrap().placed.len(),
        3,
        "2 active + 1 prefetch"
    );
    assert!(c.tick().await.unwrap().placed.is_empty());
    for id in a.holds() {
        a.start(&id);
    }
    // Started or not, three is all it ever holds.
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(a.holds().len(), 3);
}

#[tokio::test]
async fn a_worker_that_did_not_take_a_submit_gets_no_more_in_that_tick() {
    for fault in ["silent", "overloaded"] {
        let f = fx();
        let a = f.worker("w-a", (2, 1));
        let b = f.worker("w-b", (2, 1));
        let c = f.open(&[&a, &b]);
        let tasks = enqueue(&c, 4).await;
        match fault {
            "silent" => a.set(|s| s.lose.push((Op::Submit, Lost::Request))),
            _ => a.set(|s| s.overloaded = true),
        }
        let report = c.tick().await.unwrap();
        // One attempt is pinned to the worker that did not answer for it,
        // not the three it has room for; the rest go to the other worker.
        assert_eq!(only(&c, &tasks[0]).worker_id, a.id, "{fault}");
        assert_eq!(c.store().worker_load(&a.id).unwrap().pending, 1, "{fault}");
        assert_eq!(report.placed.len(), 4, "{fault}");
        for t in &tasks[1..] {
            assert_eq!(only(&c, t).worker_id, b.id, "{fault}");
        }
        assert_eq!(b.holds().len(), 3);
        assert!(a.holds().is_empty());
        assert_eq!(c.store().unacked_demand().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn a_worker_that_is_not_who_it_should_be_gets_nothing() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    a.set(|s| s.says = WorkerId::parse("w-other").unwrap());
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(a.holds().is_empty());
    assert_eq!(queued(&c), tasks);
}

// ------------------------------------ recorded, and the parent dies unsent

#[tokio::test]
async fn a_placement_that_was_recorded_and_never_sent_is_sent_by_the_next_parent() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 2).await;
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));

    let report = c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(report.placed, vec![first.attempt_id.clone()]);
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(a.holds().is_empty(), "the submit never arrived");
    // The decision and the demand are durable together, before any send.
    assert_eq!(first.observation, Observation::Unknown);
    assert_eq!(first.state, AttemptState::Accepted);
    let demand = c.store().unacked_demand().unwrap();
    assert_eq!(demand.len(), 1);
    assert_eq!(demand[0].attempt_id, Some(first.attempt_id.clone()));
    assert_eq!(demand[0].worker_id, a.id);
    let recorded: AttemptPayload =
        serde_json::from_value(demand[0].demand["payload"].clone()).unwrap();
    assert_eq!(payload_hash(&recorded).unwrap(), first.payload_hash);
    assert_eq!(recorded.task_id, tasks[0]);
    assert_eq!(recorded.generation, Generation(1));
    assert_eq!(recorded.lease_ms, 60_000);

    // The parent dies here. The next one finds the demand, counts it against
    // the worker (the second task does not go there) and sends the same
    // attempt to the same worker, not to the idle one.
    drop(c);
    let c = f.open(&[&a, &b]);
    assert_eq!(c.epoch(), CoordinatorEpoch(2));
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Reconciling);
    let report = c.tick().await.unwrap();
    assert_eq!(a.holds(), vec![first.attempt_id.clone()]);
    let held = a.held(&first.attempt_id).unwrap();
    assert_eq!(held.hash, first.payload_hash);
    assert_eq!(held.payload, recorded);
    let now = only(&c, &tasks[0]);
    assert_eq!(now.attempt_id, first.attempt_id);
    assert_eq!(now.observation, Observation::Known);
    assert_eq!(
        now.coordinator_epoch,
        CoordinatorEpoch(1),
        "the same attempt"
    );
    assert!(c.store().unacked_demand().unwrap().is_empty());
    // The second task went to the other worker, and only that.
    assert_eq!(report.placed.len(), 1);
    assert_eq!(only(&c, &tasks[1]).worker_id, b.id);
    assert_eq!(b.holds().len(), 1);
}

// --------------------------------------------------- A19: the ACK is lost

#[tokio::test]
async fn a19_a_lost_ack_is_asked_on_the_same_worker_and_the_task_runs_nowhere_else() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.lose.push((Op::Submit, Lost::Answer)));

    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    assert_eq!(first.observation, Observation::Unknown);
    assert!(!first.receipts.reached(Receipt::WorkerAccepted));
    assert_eq!(c.store().unacked_demand().unwrap().len(), 1);
    // The worker took it and starts it; the parent does not know.
    a.start(&first.attempt_id);
    a.set(|s| s.offline = true);

    // However long the worker is silent, with an idle worker next to it:
    // not placed again, not failed, not forgotten.
    for _ in 0..20 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty());
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    }
    assert!(b.holds().is_empty());
    assert!(queued(&c).is_empty());
    let kept = only(&c, &tasks[0]);
    assert_eq!(kept.state, AttemptState::Accepted);
    assert_eq!(kept.observation, Observation::Unknown);

    // A parent restart in the middle changes nothing about that.
    drop(c);
    let c = f.open(&[&a, &b]);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(b.holds().is_empty());

    // The worker is back: the same attempt is found there, running.
    a.set(|s| s.offline = false);
    let report = c.tick().await.unwrap();
    assert!(report.placed.is_empty() && report.unknown.is_empty());
    let found = only(&c, &tasks[0]);
    assert_eq!(found.attempt_id, first.attempt_id);
    assert_eq!(found.state, AttemptState::Active);
    assert_eq!(found.observation, Observation::Known);
    assert!(found.receipts.reached(Receipt::WorkerAccepted));
    assert!(c.store().unacked_demand().unwrap().is_empty());
    assert_eq!(a.count(Op::Submit), 1, "accepted once, never sent anew");
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a"]);
}

#[tokio::test]
async fn a19_the_task_moves_only_after_the_retire_and_the_late_submit_is_refused() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    // The submit is on its way for a long time: it has not arrived.
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    let demand = c.store().unacked_demand().unwrap().remove(0);
    let late = SubmitAttemptRequest::new(
        RequestId::generate(),
        CoordinatorEpoch(1),
        first.attempt_id.clone(),
        serde_json::from_value(demand.demand["payload"].clone()).unwrap(),
    )
    .unwrap();

    // Someone wants the task elsewhere. While the worker cannot confirm the
    // retire, the task stays where it is.
    a.set(|s| s.offline = true);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    for _ in 0..5 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.retired.is_empty());
    }
    assert!(b.holds().is_empty());
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Unknown);
    assert!(!only(&c, &tasks[0]).retired);

    // The worker answers: tombstone written, never started. Only now is the
    // next generation placed (the first worker is draining by then).
    a.set(|s| {
        s.offline = false;
        s.draining = true;
    });
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    let all = attempts(&c, &tasks[0]);
    assert_eq!(all.len(), 2);
    assert!(all[0].retired);
    assert_eq!(all[1].generation, Generation(2));
    assert_eq!(all[1].worker_id, b.id);
    assert_eq!(report.placed, vec![all[1].attempt_id.clone()]);
    let (_, confirmed) = c
        .store()
        .retire_tombstone(&first.attempt_id)
        .unwrap()
        .unwrap();
    assert!(confirmed.is_some());
    assert!(c.store().unacked_demand().unwrap().is_empty());

    // The old submit arrives at last, and again: refused, never started.
    for _ in 0..3 {
        let e = a.handle(Request::SubmitAttempt(late.clone())).unwrap_err();
        assert_eq!(e.code, ErrorCode::Retired);
    }
    assert!(a.holds().is_empty());
    b.start(&all[1].attempt_id);
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-b"]);
    // Nothing more is placed for the task.
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(attempts(&c, &tasks[0]).len(), 2);
}

// ----------------------------------- retire sent, and its answer is lost

#[tokio::test]
async fn a_retire_whose_answer_was_lost_frees_the_task_exactly_once_after_a_restart() {
    for lose_answer in [false, true] {
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let c = f.open(&[&a]);
        let tasks = enqueue(&c, 1).await;
        c.tick().await.unwrap();
        let first = only(&c, &tasks[0]);
        a.start(&first.attempt_id);
        c.tick().await.unwrap();

        c.request_retire(&first.attempt_id, "move").await.unwrap();
        let c = if lose_answer {
            // The worker stops the command and writes the tombstone; the
            // parent never hears it, and dies.
            a.set(|s| s.lose.push((Op::Retire, Lost::Answer)));
            let report = c.tick().await.unwrap();
            assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
            assert!(report.retired.is_empty() && report.placed.is_empty());
            let held = a.held(&first.attempt_id).unwrap();
            assert!(held.retired);
            assert_eq!(held.termination, Some(ended(Outcome::Cancelled)));
            assert!(queued(&c).is_empty(), "not confirmed: the task stays");
            drop(c);
            f.open(&[&a])
        } else {
            c
        };
        a.set(|s| s.draining = true);
        c.attach(b.id.clone(), b.clone()).await;

        // The retire is sent (again). What the worker did is the same in
        // both runs, and so is what the parent concludes.
        let report = c.tick().await.unwrap();
        assert_eq!(
            report.retired,
            vec![first.attempt_id.clone()],
            "{lose_answer}"
        );
        assert!(report.settled.is_empty());
        let all = attempts(&c, &tasks[0]);
        assert_eq!(all.len(), 2, "{lose_answer}");
        assert!(all[0].retired && all[0].termination.is_none());
        assert_eq!(all[1].generation, Generation(2));
        assert_eq!(all[1].worker_id, b.id);
        b.start(&all[1].attempt_id);
        for _ in 0..3 {
            assert!(c.tick().await.unwrap().placed.is_empty());
        }
        assert_eq!(
            attempts(&c, &tasks[0]).len(),
            2,
            "one new generation, not more"
        );
        assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a", "w-b"]);
        assert_eq!(a.count(Op::Retire), if lose_answer { 2 } else { 1 });
    }
}

#[tokio::test]
async fn an_attempt_that_had_ended_by_itself_keeps_its_result_and_is_not_placed_again() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();
    // It succeeds, and before the parent sees that, a retire is asked for.
    a.finish(&first.attempt_id, Outcome::Succeeded);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    let report = c.tick().await.unwrap();
    assert!(report.retired.is_empty() && report.placed.is_empty());
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Succeeded);
    assert!(!rec.retired);
    assert!(queued(&c).is_empty());
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(b.holds().is_empty());
}

#[tokio::test]
async fn a_retire_the_worker_cannot_confirm_yet_keeps_the_task_and_its_slot() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 2).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    assert_eq!(only(&c, &tasks[1]).worker_id, b.id);
    a.start(&first.attempt_id);
    a.set(|s| s.slow_stop = true);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    for _ in 0..3 {
        let report = c.tick().await.unwrap();
        assert!(report.retired.is_empty() && report.placed.is_empty());
    }
    assert_eq!(attempts(&c, &tasks[0]).len(), 1);
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Known);
    // It stops at last.
    a.set(|s| s.slow_stop = false);
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    // The freed task goes back to the first worker with room: the same one.
    assert_eq!(attempts(&c, &tasks[0])[1].worker_id, a.id);
    assert_eq!(a.holds().len(), 2);
}

// ------------------------------------------- A21: the link is gone for long

#[tokio::test]
async fn a21_an_attempt_whose_link_is_gone_is_kept_and_followed_again_on_return() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    let renewed = a.held(&first.attempt_id).unwrap();
    assert_eq!(renewed.renewals, 2, "each look renews the lease");
    assert_eq!(
        renewed.lease.unwrap().coordinator_epoch,
        CoordinatorEpoch(1)
    );

    // The connection is gone. The worker keeps running within its lease.
    c.detach(&a.id).await.unwrap();
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Unknown);
    for _ in 0..30 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.settled.is_empty());
    }
    let kept = only(&c, &tasks[0]);
    assert_eq!(kept.state, AttemptState::Active, "a cut link ends nothing");
    assert_eq!(kept.observation, Observation::Unknown);
    assert_eq!(kept.termination, None);
    assert!(b.holds().is_empty());
    assert_eq!(a.held(&first.attempt_id).unwrap().renewals, 2);

    // Reconnected: the same attempt, known again, its lease renewed with a
    // higher key.
    c.attach(a.id.clone(), a.clone()).await;
    let report = c.tick().await.unwrap();
    assert!(report.unknown.is_empty());
    let back = only(&c, &tasks[0]);
    assert_eq!(back.attempt_id, first.attempt_id);
    assert_eq!(back.observation, Observation::Known);
    let held = a.held(&first.attempt_id).unwrap();
    assert_eq!(held.renewals, 3);
    assert!(held.lease.unwrap() > renewed.lease.unwrap());
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a"]);
    assert_eq!(a.count(Op::Submit), 1);
}

// ------------------------------------ A22: the parent is stopped for long

#[tokio::test]
async fn a22_after_a_long_stop_the_parent_keeps_unknown_until_the_worker_says_it_stopped() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();

    // The parent is gone for longer than the lease. The worker stops the
    // command by itself.
    drop(c);
    a.finish(&first.attempt_id, Outcome::Interrupted);
    a.set(|s| s.offline = true);

    // The parent returns and cannot reach the worker. The lease has surely
    // run out, and an idle worker is there: still nothing is placed and
    // nothing is concluded.
    let c = f.open(&[&b]);
    for _ in 0..5 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(only(&c, &tasks[0]).observation, Observation::Reconciling);
    c.attach(a.id.clone(), a.clone()).await;
    for _ in 0..5 {
        let report = c.tick().await.unwrap();
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
        assert!(report.placed.is_empty() && report.settled.is_empty());
    }
    let kept = only(&c, &tasks[0]);
    assert_eq!(kept.state, AttemptState::Active);
    assert_eq!(kept.observation, Observation::Unknown);
    assert!(b.holds().is_empty() && queued(&c).is_empty());

    // The worker's word that it stopped is what ends the attempt.
    a.set(|s| s.offline = false);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Interrupted);
    assert_eq!(rec.observation, Observation::Known);
    // Interrupted is a result; a retry is the user's to ask for.
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(b.holds().is_empty());
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a"]);
}

// ------------------------------------------- A23: the parent is restarted

#[tokio::test]
async fn a23_a_restarted_parent_adopts_what_runs_and_starts_nothing_again() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    let before: Vec<AttemptRecord> = tasks.iter().map(|t| only(&c, t)).collect();
    a.start(&before[0].attempt_id);
    a.start(&before[1].attempt_id);
    c.tick().await.unwrap();
    assert_eq!(a.count(Op::Submit), 3);

    drop(c);
    let c = f.open(&[&a]);
    assert_eq!(c.epoch(), CoordinatorEpoch(2));
    for t in &tasks {
        assert_eq!(only(&c, t).observation, Observation::Reconciling);
    }
    let report = c.tick().await.unwrap();
    assert!(report.placed.is_empty() && report.unknown.is_empty());
    for (t, was) in tasks.iter().zip(&before) {
        let now = only(&c, t);
        assert_eq!(now.attempt_id, was.attempt_id, "the same attempt, adopted");
        assert_eq!(now.generation, Generation(1));
        assert_eq!(now.observation, Observation::Known);
        assert_eq!(now.coordinator_epoch, CoordinatorEpoch(1));
    }
    assert_eq!(only(&c, &tasks[0]).state, AttemptState::Active);
    assert_eq!(only(&c, &tasks[2]).state, AttemptState::Accepted);
    assert_eq!(a.count(Op::Submit), 3, "nothing was submitted again");
    assert_eq!(a.holds().len(), 3);
    // The new epoch controls the old attempts.
    let lease = a.held(&before[0].attempt_id).unwrap().lease.unwrap();
    assert_eq!(lease.coordinator_epoch, CoordinatorEpoch(2));
    assert_eq!(a.state.lock().unwrap().max_epoch, 2);

    // A result of an attempt made under the old epoch is taken.
    a.finish(&before[0].attempt_id, Outcome::Succeeded);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![before[0].attempt_id.clone()]);
    assert_eq!(
        only(&c, &tasks[0]).termination.unwrap().outcome(),
        Outcome::Succeeded
    );
    for t in &tasks {
        assert_eq!(f.cluster.starts_of(t).len(), usize::from(t != &tasks[2]));
    }
}

#[tokio::test]
async fn a_parent_that_a_newer_one_replaced_stops_for_good() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 2).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    // The worker has heard from a newer coordinator.
    a.set(|s| s.max_epoch = 7);
    let e = c.tick().await.unwrap_err();
    assert!(
        matches!(e, CoordinatorError::Fenced(CoordinatorEpoch(1))),
        "{e:?}"
    );
    assert!(c.is_fenced());
    let renewals = a.held(&first.attempt_id).unwrap().renewals;
    let calls = a.count(Op::Renew) + a.count(Op::Submit) + a.count(Op::Cancel);
    assert!(matches!(c.tick().await, Err(CoordinatorError::Fenced(_))));
    let more = c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(TaskId::generate(), spec())],
    );
    assert!(matches!(more.await, Err(CoordinatorError::Fenced(_))));
    assert_eq!(a.held(&first.attempt_id).unwrap().renewals, renewals);
    assert_eq!(
        a.count(Op::Renew) + a.count(Op::Submit) + a.count(Op::Cancel),
        calls
    );
    // What it knew stays as it was: nothing is failed by the fencing.
    assert_eq!(only(&c, &tasks[0]).termination, None);
}

// ----------------------------------------------------------------- cancel

#[tokio::test]
async fn a_cancel_is_durable_first_and_reaches_the_worker_also_after_a_restart() {
    let f = fx();
    let a = f.worker("w-a", (1, 1));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    let running = only(&c, &tasks[0]);
    a.start(&running.attempt_id);
    c.tick().await.unwrap();

    // A queued task: never placed. A running one: the worker is offline, so
    // the request waits in the database, across a restart.
    assert_eq!(c.cancel_task(&tasks[2]).await.unwrap(), None);
    a.set(|s| s.offline = true);
    let hit = c.cancel_task(&tasks[0]).await.unwrap().unwrap();
    assert_eq!(hit.attempt_id, running.attempt_id);
    c.tick().await.unwrap();
    assert_eq!(a.count(Op::Cancel), 0);
    drop(c);
    let c = f.open(&[&a]);
    a.set(|s| s.offline = false);

    let report = c.tick().await.unwrap();
    assert_eq!(a.count(Op::Cancel), 1);
    assert_eq!(
        a.held(&running.attempt_id).unwrap().termination,
        Some(ended(Outcome::Cancelled))
    );
    assert!(report.settled.is_empty(), "accepted is not completed");
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![running.attempt_id.clone()]);
    assert_eq!(
        only(&c, &tasks[0]).termination.unwrap().outcome(),
        Outcome::Cancelled
    );
    assert_eq!(
        a.count(Op::Cancel),
        1,
        "a settled attempt is not cancelled again"
    );
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    assert!(
        attempts(&c, &tasks[2]).is_empty(),
        "the cancelled task was never placed"
    );
    assert_eq!(attempts(&c, &tasks[0]).len(), 1);
}

#[tokio::test]
async fn a_cancel_of_an_attempt_that_never_arrived_leaves_a_tombstone_and_is_settled_here() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    let demand = c.store().unacked_demand().unwrap().remove(0);
    c.cancel_task(&tasks[0]).await.unwrap();

    // The cancel goes instead of a resend, and the worker remembers it.
    let report = c.tick().await.unwrap();
    assert!(report.settled.is_empty());
    assert_eq!(a.count(Op::Cancel), 1);
    assert_eq!(a.count(Op::Submit), 0);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(c.store().unacked_demand().unwrap().is_empty());
    // The submit that was on its way cannot start anything.
    let late = SubmitAttemptRequest::new(
        RequestId::generate(),
        CoordinatorEpoch(1),
        first.attempt_id.clone(),
        serde_json::from_value(demand.demand["payload"].clone()).unwrap(),
    )
    .unwrap();
    let e = a.handle(Request::SubmitAttempt(late)).unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
    assert!(f.cluster.starts_of(&tasks[0]).is_empty());
    assert!(c.tick().await.unwrap().placed.is_empty());
}

// ----------------------------------------------------------- refused submits

#[tokio::test]
async fn an_overloaded_worker_gets_the_same_attempt_again_later() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.overloaded = true);
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    assert_eq!(first.observation, Observation::Known);
    assert!(a.holds().is_empty() && b.holds().is_empty());
    assert_eq!(c.store().unacked_demand().unwrap().len(), 1);
    a.set(|s| s.overloaded = false);
    c.tick().await.unwrap();
    assert_eq!(a.holds(), vec![first.attempt_id.clone()]);
    assert!(c.store().unacked_demand().unwrap().is_empty());
}

#[tokio::test]
async fn an_attempt_a_worker_will_never_take_is_retired_there_and_failed_here() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.refuse = true);
    let report = c.tick().await.unwrap();
    assert!(report.settled.is_empty());
    let first = only(&c, &tasks[0]);
    assert_eq!(
        c.store()
            .events_of(&first.attempt_id, "submit_refused")
            .unwrap()
            .len(),
        1
    );
    // The tombstone goes to the worker first; then the parent fixes failed.
    let report = c.tick().await.unwrap();
    assert_eq!(a.count(Op::Retire), 1);
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    assert!(report.retired.is_empty());
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Failed);
    assert!(!rec.retired, "its result is the task's last word");
    let tombstone = c.store().retire_tombstone(&first.attempt_id).unwrap();
    assert!(
        matches!(tombstone, Some((_, Some(_)))),
        "the confirmed retire is on record here too: {tombstone:?}"
    );
    assert!(c.store().unacked_demand().unwrap().is_empty());
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert!(
        b.holds().is_empty(),
        "a refusal is not a reason to try elsewhere"
    );
    assert_eq!(attempts(&c, &tasks[0]).len(), 1);
}

#[tokio::test]
async fn the_same_attempt_with_another_payload_is_refused_for_good() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| {
        s.fault = Some(ApiError::conflict(
            ConflictReason::PayloadMismatch,
            "another payload",
        ))
    });
    c.tick().await.unwrap();
    let report = c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    assert_eq!(first.termination.unwrap().outcome(), Outcome::Failed);
}

#[tokio::test]
async fn a_fault_of_the_worker_is_not_a_verdict_on_the_attempt() {
    // Every answer that says nothing about the attempt itself, a conflict
    // without the reason that names the payload included, and `BLOCKED`
    // without a reason or for a secret that is missing (which is missing on
    // every worker alike). (A worker that keeps answering `BLOCKED` for a
    // reason of its own, or `PROTOCOL_INCOMPATIBLE`, is left for another
    // one: see the scheduler's tests.)
    let secret = ApiError::blocked(BlockedReason::SecretUnavailable, "no bundle");
    for fault in [
        err(ErrorCode::Internal),
        err(ErrorCode::CommunicationUnknown),
        err(ErrorCode::NotImplemented),
        err(ErrorCode::NotFound),
        err(ErrorCode::Conflict),
        err(ErrorCode::Blocked),
        secret,
    ] {
        let code = (fault.code, fault.blocked_reason);
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let c = f.open(&[&a, &b]);
        let tasks = enqueue(&c, 1).await;
        a.set(|s| s.fault = Some(fault));
        // However often: the same attempt is sent to the same worker again.
        for sent in 1..=6 {
            let report = c.tick().await.unwrap();
            assert!(report.settled.is_empty() && report.retired.is_empty());
            assert_eq!(a.received(Op::Submit), sent, "{code:?}");
        }
        assert_eq!(a.received(Op::Retire), 0, "{code:?}");
        assert!(b.holds().is_empty() && b.received(Op::Submit) == 0);
        let first = only(&c, &tasks[0]);
        assert_eq!(c.store().retire_tombstone(&first.attempt_id).unwrap(), None);
        assert_eq!(first.worker_id, a.id, "{code:?}");
        assert_eq!(first.state, AttemptState::Accepted, "{code:?}");
        assert_eq!(first.termination, None, "{code:?}: the task did not fail");
        assert_eq!(first.observation, Observation::Known);
        assert_eq!(c.store().retire_tombstone(&first.attempt_id).unwrap(), None);
        assert_eq!(a.count(Op::Retire), 0);
        assert!(b.holds().is_empty(), "and it is not tried elsewhere");
        // Why it waits is on record, once however often it is asked.
        let why = c
            .store()
            .events_of(&first.attempt_id, "submit_deferred")
            .unwrap();
        assert_eq!(why.len(), 1, "{code:?}");
        assert_eq!(why[0].payload["code"], code.0.to_string());
        // The fault passes: the same attempt goes to the same worker.
        a.set(|s| s.fault = None);
        c.tick().await.unwrap();
        assert_eq!(a.holds(), vec![first.attempt_id.clone()], "{code:?}");
        assert!(c.store().unacked_demand().unwrap().is_empty());
    }
}

#[tokio::test]
async fn an_attempt_that_waits_on_its_worker_is_moved_by_a_retire() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    a.set(|s| s.fault = Some(err(ErrorCode::Blocked)));
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    assert_eq!(first.worker_id, a.id);
    // Whoever sees that it will not run there asks for the retire; the
    // worker never had it, and the task is free for another one (this
    // worker takes nothing new).
    a.set(|s| s.draining = true);
    c.request_retire(&first.attempt_id, "blocked")
        .await
        .unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    let second = attempts(&c, &tasks[0]).remove(1);
    assert_eq!(second.worker_id, b.id);
    assert_eq!(b.holds(), vec![second.attempt_id.clone()]);
    assert!(a.holds().is_empty());
}

#[tokio::test]
async fn a_command_that_ends_by_itself_while_it_is_retired_keeps_its_result() {
    // `lost`: the first answer to the retire does not arrive, so the worker
    // answers `already_terminal` to the second. Both ways end the same.
    for lost in [false, true] {
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let c = f.open(&[&a, &b]);
        let tasks = enqueue(&c, 1).await;
        c.tick().await.unwrap();
        let first = only(&c, &tasks[0]);
        a.start(&first.attempt_id);
        c.tick().await.unwrap();
        // The command succeeds between the worker's look and its stop: the
        // retire is answered `stopped`.
        a.set(|s| s.ends_in_retire = Some(Outcome::Succeeded));
        c.request_retire(&first.attempt_id, "move").await.unwrap();
        if lost {
            a.set(|s| s.lose.push((Op::Retire, Lost::Answer)));
            let report = c.tick().await.unwrap();
            assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
        }
        let report = c.tick().await.unwrap();
        assert!(
            report.retired.is_empty() && report.placed.is_empty(),
            "lost={lost}: {report:?}"
        );
        assert_eq!(report.settled, vec![first.attempt_id.clone()]);
        let rec = only(&c, &tasks[0]);
        assert_eq!(rec.termination.unwrap().outcome(), Outcome::Succeeded);
        assert!(!rec.retired);
        assert!(queued(&c).is_empty());
        for _ in 0..2 {
            assert!(c.tick().await.unwrap().placed.is_empty());
        }
        assert!(b.holds().is_empty(), "lost={lost}: it ran once");
        assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a".to_string()]);
    }
}

#[tokio::test]
async fn a_stop_whose_ending_is_not_known_frees_nothing() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.start(&first.attempt_id);
    c.tick().await.unwrap();
    // The retire is answered `stopped`; how it ended is not answered.
    a.set(|s| s.lose_get_after_retire = true);
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    for _ in 0..3 {
        let report = c.tick().await.unwrap();
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
        assert!(report.retired.is_empty() && report.placed.is_empty());
    }
    let rec = only(&c, &tasks[0]);
    assert_eq!(rec.observation, Observation::Unknown);
    assert!(!rec.retired);
    assert!(queued(&c).is_empty() && b.holds().is_empty());
    // It answers: stopped without completing, so the task is free.
    a.set(|s| s.lose_get_after_retire = false);
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    assert_eq!(attempts(&c, &tasks[0]).len(), 2);
}

#[tokio::test]
async fn a_cancel_while_a_retire_is_pending_settles_the_attempt_cancelled() {
    // The cancel before or after the retire request: the same end.
    for cancel_first in [false, true] {
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let c = f.open(&[&a, &b]);
        let tasks = enqueue(&c, 1).await;
        c.tick().await.unwrap();
        let first = only(&c, &tasks[0]);
        a.start(&first.attempt_id);
        c.tick().await.unwrap();
        if cancel_first {
            c.cancel_task(&tasks[0]).await.unwrap();
            c.request_retire(&first.attempt_id, "move").await.unwrap();
        } else {
            c.request_retire(&first.attempt_id, "move").await.unwrap();
            c.cancel_task(&tasks[0]).await.unwrap();
        }
        let report = c.tick().await.unwrap();
        assert_eq!(report.settled, vec![first.attempt_id.clone()]);
        assert!(report.retired.is_empty() && report.placed.is_empty());
        let rec = only(&c, &tasks[0]);
        assert_eq!(rec.state, AttemptState::Terminal);
        assert_eq!(rec.termination.unwrap().outcome(), Outcome::Cancelled);
        assert!(!rec.retired);
        let tombstone = c.store().retire_tombstone(&first.attempt_id).unwrap();
        assert!(matches!(tombstone, Some((_, Some(_)))), "{tombstone:?}");
        assert!(c.store().live_attempts(None).unwrap().is_empty());
        assert!(c.store().unacked_demand().unwrap().is_empty());
        assert!(queued(&c).is_empty());
        assert!(c.tick().await.unwrap().placed.is_empty());
        assert!(b.holds().is_empty());
    }
}

#[tokio::test]
async fn a_worker_that_stalls_does_not_keep_the_leases_of_the_others_from_renewal() {
    let mut f = fx();
    f.cfg.worker_budget_ms = 1_500;
    // It is asked first (by id), and holds two attempts: the first task
    // goes to it, the second to the other (which is free), and the third
    // to it again (both are as busy, and its id is the smaller).
    let stalled = f.worker("w-a", (2, 0));
    let healthy = f.worker("w-b", (2, 0));
    let c = f.open(&[&stalled, &healthy]);
    let mut tasks = enqueue(&c, 3).await;
    c.tick().await.unwrap();
    tasks.swap(1, 2);
    for t in &tasks[..2] {
        assert_eq!(only(&c, t).worker_id, stalled.id);
        stalled.start(&only(&c, t).attempt_id);
    }
    let on_healthy = only(&c, &tasks[2]);
    assert_eq!(on_healthy.worker_id, healthy.id);
    healthy.start(&on_healthy.attempt_id);
    c.tick().await.unwrap();
    let before = healthy.held(&on_healthy.attempt_id).unwrap().renewals;

    stalled.set(|s| s.stall = true);
    let more = enqueue(&c, 1).await;
    let started = std::time::Instant::now();
    let report = tokio::time::timeout(Duration::from_secs(20), c.tick())
        .await
        .expect("the tick ends although a worker never answers")
        .unwrap();
    // Far below the lease (60 s), and not one wait per attempt.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let after = healthy.held(&on_healthy.attempt_id).unwrap().renewals;
    assert_eq!(after, before + 1, "the healthy worker's lease was renewed");
    let mut unknown = report.unknown.clone();
    unknown.sort();
    let mut want: Vec<AttemptId> = tasks[..2].iter().map(|t| only(&c, t).attempt_id).collect();
    want.sort();
    assert_eq!(unknown, want, "what did not answer is unknown, and kept");
    // Nothing new goes to the worker that used up its time.
    assert_eq!(only(&c, &more[0]).worker_id, healthy.id);
    assert_eq!(f.cluster.starts_of(&tasks[0]), vec!["w-a".to_string()]);

    // It answers again: its attempts are followed as before.
    stalled.set(|s| s.stall = false);
    let report = c.tick().await.unwrap();
    assert!(report.unknown.is_empty());
    for t in &tasks[..2] {
        assert_eq!(only(&c, t).observation, Observation::Known);
    }
}

#[tokio::test]
async fn a_task_no_payload_can_be_made_for_is_passed_over_and_the_rest_is_placed() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    // Queued by something else than this coordinator: not a task spec.
    let (run, broken) = (RunId::generate(), TaskId::generate());
    let src = source(&c);
    c.store()
        .enqueue(
            &run,
            Some(&src),
            &serde_json::json!({}),
            &[(broken.clone(), serde_json::json!({"nope": 1}))],
            1,
        )
        .unwrap();
    let tasks = enqueue(&c, 2).await;
    for _ in 0..2 {
        c.tick().await.expect("one task does not fail the tick");
    }
    assert!(attempts(&c, &broken).is_empty());
    assert_eq!(queued(&c), vec![broken.clone()], "it stays queued");
    assert_eq!(a.holds().len(), 2);
    for t in &tasks {
        assert_eq!(only(&c, t).worker_id, a.id);
    }
    let events = c.store().events_after(0, 1_000).unwrap();
    let noted: Vec<_> = events
        .iter()
        .filter(|e| e.kind == "task_unplaceable")
        .collect();
    assert_eq!(noted.len(), 1, "recorded once");
    assert_eq!(noted[0].payload["task"], broken.to_string());
}

#[tokio::test]
async fn a_status_about_another_generation_is_no_answer() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    a.set(|s| {
        let h = s.held.get_mut(&first.attempt_id).unwrap();
        h.payload.generation = Generation(9);
        h.state = AttemptState::Terminal;
        h.termination = Some(ended(Outcome::Succeeded));
    });
    let report = c.tick().await.unwrap();
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(report.settled.is_empty());
    assert_eq!(only(&c, &tasks[0]).termination, None);
}

#[tokio::test]
async fn a_run_without_a_command_is_refused_whole() {
    let f = fx();
    let c = f.open(&[]);
    let mut empty = spec();
    empty.commands.clear();
    let tasks = vec![(TaskId::generate(), spec()), (TaskId::generate(), empty)];
    let e = c.enqueue(RunId::generate(), source(&c), tasks).await;
    assert!(matches!(e, Err(CoordinatorError::Invalid(_))), "{e:?}");
    assert!(queued(&c).is_empty());
}

// ------------------------------------------------- against a real worker

/// A link that carries one retire out and loses its answer.
struct LoseRetireAnswer {
    inner: ClientLink,
    armed: AtomicBool,
}

impl WorkerLink for LoseRetireAnswer {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let lose = matches!(request, Request::RetireAttempt(_))
                && self.armed.swap(false, Ordering::SeqCst);
            let answer = self.inner.call(request).await;
            if lose {
                Err(LinkError::Unconfirmed)
            } else {
                answer
            }
        })
    }
}

/// A connection of this coordinator (its cluster, id and epoch) to the agent.
async fn link_to(c: &Coordinator, agent: &Arc<Agent>, lose_retire: bool) -> Arc<dyn WorkerLink> {
    let (near, far) = duplex_pair(64 * 1024);
    let cluster = c.store().cluster_id().clone();
    let permits = Arc::new(Semaphore::new(64));
    tokio::spawn(serve_connection(
        agent.clone(),
        far,
        cluster.clone(),
        permits,
    ));
    let cfg = ClientConfig::new(cluster, c.store().coordinator_id().clone(), c.epoch());
    let client = connect(near, cfg).await.unwrap().client;
    Arc::new(LoseRetireAnswer {
        inner: ClientLink::new(client, Duration::from_secs(30)),
        armed: AtomicBool::new(lose_retire),
    })
}

fn count_lines(p: &std::path::Path) -> usize {
    std::fs::read_to_string(p).map_or(0, |s| s.lines().count())
}

/// The attempt of a task that is not retired.
fn only_live(c: &Coordinator, task: &TaskId) -> AttemptRecord {
    let mut live: Vec<AttemptRecord> = attempts(c, task)
        .into_iter()
        .filter(|a| !a.retired)
        .collect();
    assert_eq!(live.len(), 1);
    live.remove(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a19_a_real_worker_stops_the_retired_run_and_the_resent_retire_frees_the_task_once() {
    let mut w = worker_agent::fx();
    let dir = crate::test_tmp::tempdir().unwrap();
    let open = || {
        // The agent of this test lives in this process and forks its
        // supervisors: a child between fork and exec still holds a copy of
        // the lock of the coordinator that was just dropped. Wait for it.
        let end = std::time::Instant::now() + Duration::from_secs(10);
        let store = loop {
            match CoordinatorStore::open(dir.path(), 1) {
                Ok(store) => break store,
                Err(StorageError::Locked(_)) if std::time::Instant::now() < end => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("{e:?}"),
            }
        };
        Coordinator::start(store, CoordinatorConfig::default()).unwrap()
    };
    let c = open();
    w.cluster = c.store().cluster_id().clone();
    let agent = w.start("boot-a");
    let id = WorkerId::parse("w-test").unwrap();
    c.attach(id.clone(), link_to(&c, &agent, true).await).await;

    let (script, marker, pidfile) = worker_agent::stay(&w);
    let mut task_spec = spec();
    task_spec.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script];
    task_spec.commands[0]
        .env
        .insert("PATH".into(), "/usr/bin:/bin".into());
    let task = TaskId::generate();
    c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(task.clone(), task_spec)],
    )
    .await
    .unwrap();
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    let first = only(&c, &task);
    // The real worker answered the scheduler's forecast question with a
    // number to compare by (a ranking: nothing has run here before).
    let decision = remote_task_runner::scheduler::reason_of(c.store(), &first.attempt_id)
        .unwrap()
        .unwrap();
    let outlook = decision.compared[0].outlook;
    assert_eq!(outlook.unusable, None, "{decision:?}");
    assert!(
        outlook.finish_ms.or(outlook.ranking_ms).is_some(),
        "{decision:?}"
    );
    worker_agent::wait_for("the command", || read_pid(&pidfile).is_some());
    let pid = read_pid(&pidfile).unwrap();
    c.tick().await.unwrap();
    assert!(only(&c, &task).receipts.reached(Receipt::WorkerAccepted));

    // The retire reaches the worker, which stops the command; the answer
    // is lost and the parent dies not knowing.
    c.request_retire(&first.attempt_id, "move").await.unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
    assert!(report.retired.is_empty() && report.placed.is_empty());
    assert!(!alive(pid), "the worker stopped the command");
    assert!(queued(&c).is_empty());
    drop(c);

    // The next parent, in a new epoch, sends the retire again. The worker
    // says the attempt had ended already; that it was stopped, not finished,
    // is what frees the task. It is placed once more, and only once.
    let c = open();
    assert_eq!(c.epoch(), CoordinatorEpoch(2));
    c.attach(id.clone(), link_to(&c, &agent, false).await).await;
    let report = c.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    assert_eq!(report.placed.len(), 1);
    let all = attempts(&c, &task);
    assert_eq!(all.len(), 2);
    assert!(all[0].retired);
    assert_eq!(all[1].generation, Generation(2));
    worker_agent::wait_for("the second run", || count_lines(&marker) == 2);
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(attempts(&c, &task).len(), 2);
    // The marker is written a moment before the worker reports `active`.
    let state = only_live(&c, &task).state;
    assert!(
        matches!(state, AttemptState::Starting | AttemptState::Active),
        "{state:?}"
    );

    // Cancel what still runs, and see it end.
    c.cancel_task(&task).await.unwrap();
    let second = all[1].attempt_id.clone();
    let mut ended = None;
    for _ in 0..200 {
        c.tick().await.unwrap();
        ended = c.store().get_attempt(&second).unwrap().unwrap().termination;
        if ended.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ended.map(|t| t.outcome()), Some(Outcome::Cancelled));
    assert_eq!(count_lines(&marker), 2, "one start per generation");
}

/// A30 against a real worker: what a command left is fetched through the
/// worker's own `get_result`, `read_log`, `blob_get` and `ack_collected`,
/// over a connection that is replaced in the middle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a30_a_real_workers_outputs_are_collected_over_two_connections_and_the_worker_is_told() {
    let mut w = worker_agent::fx();
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let c = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
    w.cluster = c.store().cluster_id().clone();
    let agent = w.start("boot-a");
    let id = WorkerId::parse("w-test").unwrap();
    c.attach(id.clone(), link_to(&c, &agent, false).await).await;
    let files = Arc::new(ResultStore::open(&dir.path().join("results")).unwrap());

    let mut task_spec = spec();
    let script = "printf 0123456789 > out.txt; echo hello";
    task_spec.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script.into()];
    task_spec.commands[0]
        .env
        .insert("PATH".into(), "/usr/bin:/bin".into());
    task_spec.artifacts = vec![
        ArtifactDecl {
            name: "report/out.txt".into(),
            path: "out.txt".into(),
            if_missing: MissingPolicy::Required,
        },
        ArtifactDecl {
            name: "maybe".into(),
            path: "maybe.txt".into(),
            if_missing: MissingPolicy::Optional,
        },
    ];
    let task = TaskId::generate();
    c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(task.clone(), task_spec)],
    )
    .await
    .unwrap();
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    let a = only(&c, &task).attempt_id;
    let mut ended = None;
    for _ in 0..400 {
        c.tick().await.unwrap();
        ended = c.store().get_attempt(&a).unwrap().unwrap().termination;
        if ended.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(ended.map(|t| t.outcome()), Some(Outcome::Succeeded));

    // A pass whose time is up before it starts does nothing and gives nothing up.
    let slow = CollectConfig {
        chunk_bytes: 4,
        budget_ms: 0,
        ..CollectConfig::default()
    };
    let report = c.collect(&files, slow).await.unwrap();
    assert_eq!(report.pending, vec![a.clone()]);
    assert!(agent
        .store()
        .events_of(&a, "ack_collected")
        .unwrap()
        .is_empty());

    // A new connection; the collection goes on, in small chunks.
    c.attach(id, link_to(&c, &agent, false).await).await;
    let cfg = CollectConfig {
        chunk_bytes: 4,
        ..CollectConfig::default()
    };
    let report = c.collect(&files, cfg).await.unwrap();
    assert_eq!(report.collected, vec![a.clone()]);
    let out = files.artifact_path(&a, &Digest::of_bytes(b"0123456789"));
    assert_eq!(std::fs::read(out).unwrap(), b"0123456789");
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), b"hello\n");
    let row = c.store().collection(&a).unwrap().unwrap();
    let logs = row.logs.unwrap();
    assert_eq!(logs["stdout"]["complete"], true);
    assert_eq!(logs["stdout"]["bytes"], 6);
    assert!(only(&c, &task).receipts.reached(Receipt::Collected));
    // The worker recorded the acknowledgement, once.
    let acked = agent.store().events_of(&a, "ack_collected").unwrap();
    assert_eq!(acked.len(), 1);
    assert_eq!(
        c.collect(&files, cfg).await.unwrap(),
        CollectReport::default()
    );
}

// Readiness constrains only new placements; control traffic still reaches the peer.
struct InputGatedWorker {
    worker: Arc<FakeWorker>,
    gate: Arc<remote_task_runner::coordinator::InputGate>,
    invalidate_forecast: AtomicBool,
    attestor: Option<Arc<ToolchainAttestor>>,
}
impl WorkerLink for InputGatedWorker {
    fn read_only_call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        match &self.attestor {
            Some(attestor) => attestor.read_only_call(request),
            None => self.call(request),
        }
    }

    fn cost_transport(&self) -> Option<remote_task_runner::scheduler::measured::Transport> {
        self.gate.cost_transport()
    }
    fn cold_eligible(&self, task: &TaskId) -> bool {
        self.gate.cold_eligible(task)
    }
    fn prefer(
        &self,
        task: &remote_task_runner::storage::QueuedTask,
        expected: u64,
        ttl: u64,
    ) -> bool {
        self.gate.prefer(task, expected, ttl)
    }
    fn clear_preference(&self, task: Option<&TaskId>, run: Option<&RunId>) {
        self.gate.clear_preference(task, run);
    }

    fn placement_fence(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
        Some(self.gate.placement_fence())
    }
    fn inputs_evaluated(&self, generation: u64, run: &RunId) {
        self.gate.evaluated(generation, run);
    }
    fn placement_inputs(&self) -> Option<remote_task_runner::coordinator::PlacementInputs> {
        Some(self.gate.placement_inputs())
    }
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let forecast = matches!(request, Request::Forecast(_));
            let answer = self.worker.call(request).await;
            if forecast && self.invalidate_forecast.swap(false, Ordering::SeqCst) {
                let previous = self.gate.snapshot();
                self.gate
                    .observe(WorkerBootId::parse("changed-boot").unwrap(), AgentEpoch(5))
                    .await;
                for input in previous {
                    self.gate.publish(self.gate.generation(), input);
                }
            }
            answer
        })
    }
}
fn gated_worker(worker: &Arc<FakeWorker>) -> Arc<InputGatedWorker> {
    Arc::new(InputGatedWorker {
        worker: worker.clone(),
        gate: Arc::default(),
        invalidate_forecast: AtomicBool::new(false),
        attestor: None,
    })
}
fn ready_input(
    task: &remote_task_runner::storage::QueuedTask,
) -> remote_task_runner::coordinator::ReadyInput {
    remote_task_runner::coordinator::ReadyInput {
        run_id: task.run_id.clone(),
        source_id: task.source_id.clone().unwrap(),
        manifest_digest: spec().manifest_digest,
    }
}
#[tokio::test]
async fn source_gate_requires_exact_input_and_preserves_existing_attempt_control() {
    let f = fx();
    let worker = f.worker("w-a", (2, 1));
    let c = f.open(&[]);
    let gated = gated_worker(&worker);
    c.attach(worker.id.clone(), gated.clone()).await;
    let tasks = enqueue(&c, 1).await;
    let input = ready_input(&c.store().queued_tasks(1).unwrap()[0]);
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert!(attempts(&c, &tasks[0]).is_empty());
    assert!(worker.state.lock().unwrap().asked.is_empty());
    assert!(c.scheduler().waiting_reason(&tasks[0]).is_none());
    let mut wrong = input.clone();
    wrong.run_id = RunId::generate();
    gated.gate.publish(gated.gate.generation(), wrong);
    let mut wrong = input.clone();
    wrong.manifest_digest = Digest::of_bytes(b"wrong");
    gated.gate.publish(gated.gate.generation(), wrong);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(worker.state.lock().unwrap().asked.is_empty());
    gated.gate.publish(gated.gate.generation(), input);
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    let attempt = only(&c, &tasks[0]);
    gated.gate.retain(|_| false);
    c.tick().await.unwrap();
    assert_eq!(only(&c, &tasks[0]).attempt_id, attempt.attempt_id);
    assert!(worker.state.lock().unwrap().received.contains(&Op::Renew));
}
#[tokio::test]
async fn source_gate_rechecks_after_forecast_and_replacement_starts_empty() {
    let f = fx();
    let worker = f.worker("w-a", (2, 1));
    let c = f.open(&[]);
    let gated = gated_worker(&worker);
    c.attach(worker.id.clone(), gated.clone()).await;
    let tasks = enqueue(&c, 1).await;
    let input = ready_input(&c.store().queued_tasks(1).unwrap()[0]);
    gated.gate.publish(gated.gate.generation(), input.clone());
    let initial = gated.gate.generation();
    gated.invalidate_forecast.store(true, Ordering::SeqCst);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(attempts(&c, &tasks[0]).is_empty());
    gated.gate.publish(initial, input.clone());
    assert_eq!(gated.gate.snapshot().len(), 1);
    assert!(
        !gated.gate.evict_evaluated(),
        "old dispatch cannot evaluate a reset generation"
    );
    let replacement = gated_worker(&worker);
    c.attach(worker.id.clone(), replacement.clone()).await;
    assert!(c.tick().await.unwrap().placed.is_empty());
    replacement
        .gate
        .publish(replacement.gate.generation(), input);
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
}
#[tokio::test]
async fn source_gate_places_later_ready_run_beyond_dispatch_scan_and_keeps_wait_state() {
    let f = fx();
    let worker = f.worker("w-a", (1, 0));
    let c = f.open(&[&worker]);
    let initial = enqueue(&c, 2).await;
    c.tick().await.unwrap();
    c.tick().await.unwrap();
    let waiting = c.scheduler().waiting_reason(&initial[1]).unwrap();
    let gated = gated_worker(&worker);
    c.attach(worker.id.clone(), gated.clone()).await;
    let _blocked = enqueue(&c, 300).await;
    let ready_tasks = enqueue(&c, 1).await;
    let last = c.store().queued_tasks(400).unwrap().last().unwrap().clone();
    gated
        .gate
        .publish(gated.gate.generation(), ready_input(&last));
    // The occupied worker prevents placement but filtered discovery must not
    // erase the unrelated wait recorded by the earlier complete scan.
    c.tick().await.unwrap();
    assert_eq!(c.scheduler().waiting_reason(&initial[1]), Some(waiting));
    worker.set(|s| s.limits = (2, 1));
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    assert_eq!(only(&c, &ready_tasks[0]).task_id, ready_tasks[0]);
    let mut cursor = None;
    let mut count = 0;
    loop {
        let page = c.store().input_page(cursor, 1000, None).unwrap();
        assert!(page.len() <= 64);
        if page.is_empty() {
            break;
        }
        count += page.len();
        cursor = page.last().map(|(cursor, _)| *cursor);
    }
    assert_eq!(count, 301);
}

fn source_gate_exclude_task(c: &Coordinator, task: &TaskId, worker: &WorkerId) {
    let attempt = AttemptId::generate();
    let placed = c
        .store()
        .place(
            task,
            worker,
            &attempt,
            |_| true,
            |_| Ok((Digest::of_bytes(b"demand"), serde_json::json!({}))),
            1,
        )
        .unwrap();
    assert!(matches!(
        placed,
        remote_task_runner::storage::PlaceOutcome::Placed(_)
    ));
    c.store()
        .begin_retire(
            &attempt,
            remote_task_runner::scheduler::REASON_WORKER_CANNOT_RUN,
            2,
        )
        .unwrap();
    c.store().confirm_retire(&attempt, 3).unwrap();
}
#[tokio::test]
async fn source_gate_full_cache_evicts_only_evaluated_inputs_for_later_compatible_run() {
    let f = fx();
    let worker = f.worker("w-a", (2, 1));
    let c = f.open(&[]);
    let gated = gated_worker(&worker);
    c.attach(worker.id.clone(), gated.clone()).await;
    for _ in 0..64 {
        enqueue(&c, 1).await;
    }
    for task in c.store().queued_tasks(64).unwrap() {
        source_gate_exclude_task(&c, &task.task_id, &worker.id);
        gated
            .gate
            .publish(gated.gate.generation(), ready_input(&task));
    }
    assert_eq!(gated.gate.snapshot().len(), 64);
    assert!(!gated.gate.evict_evaluated());
    let run = RunId::generate();
    let task = TaskId::generate();
    let mut compatible = spec();
    compatible.plan_digest = Digest::of_bytes(b"compatible");
    let source_id = source(&c);
    c.enqueue(
        run.clone(),
        source_id.clone(),
        vec![(task.clone(), compatible)],
    )
    .await
    .unwrap();
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(gated.gate.evict_evaluated());
    let input = remote_task_runner::coordinator::ReadyInput {
        run_id: run,
        source_id,
        manifest_digest: spec().manifest_digest,
    };
    gated.gate.publish(gated.gate.generation(), input);
    assert_eq!(gated.gate.snapshot().len(), 64);
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    assert_eq!(attempts(&c, &task).len(), 1);
}
#[tokio::test]
async fn source_gate_advances_within_one_ready_run_beyond_dispatch_scan() {
    let f = fx();
    let worker = f.worker("w-a", (2, 1));
    let c = f.open(&[]);
    let gated = gated_worker(&worker);
    c.attach(worker.id.clone(), gated.clone()).await;
    let run = RunId::generate();
    let source = source(&c);
    let mut tasks: Vec<_> = (0..300).map(|_| (TaskId::generate(), spec())).collect();
    tasks.last_mut().unwrap().1.plan_digest = Digest::of_bytes(b"compatible");
    let compatible = tasks.last().unwrap().0.clone();
    let incompatible: Vec<_> = tasks[..299].iter().map(|(id, _)| id.clone()).collect();
    c.enqueue(run.clone(), source.clone(), tasks).await.unwrap();
    for task in incompatible {
        source_gate_exclude_task(&c, &task, &worker.id);
    }
    gated.gate.publish(
        gated.gate.generation(),
        remote_task_runner::coordinator::ReadyInput {
            run_id: run,
            source_id: source,
            manifest_digest: spec().manifest_digest,
        },
    );
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    assert_eq!(attempts(&c, &compatible).len(), 1);
}

#[tokio::test]
async fn source_gate_keeps_unrestricted_draining_and_unreached_wait_reasons() {
    let f = fx();
    let worker = f.worker("w-a", (2, 1));
    let c = f.open(&[&worker]);
    let tasks = enqueue(&c, 1).await;
    worker.set(|s| s.draining = true);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(c.scheduler().waiting_reason(&tasks[0]).is_some());
    worker.set(|s| {
        s.draining = false;
        s.offline = true;
    });
    c.tick().await.unwrap();
    assert!(c.scheduler().waiting_reason(&tasks[0]).is_some());
    assert!(attempts(&c, &tasks[0]).is_empty());
}

/// The SQLite writer is taken only after forecasting, so all preflight reads
/// finish and the placement itself has to wait at BEGIN IMMEDIATE.
struct DbCommitWorker {
    inner: Arc<InputGatedWorker>,
    database: std::path::PathBuf,
    blocker: Mutex<Option<rusqlite::Connection>>,
    lock_forecast: AtomicBool,
    commit_entered: tokio::sync::Notify,
}
impl WorkerLink for DbCommitWorker {
    fn placement_inputs(&self) -> Option<remote_task_runner::coordinator::PlacementInputs> {
        self.inner.placement_inputs()
    }
    fn placement_fence(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
        self.commit_entered.notify_one();
        self.inner.placement_fence()
    }
    fn inputs_evaluated(&self, generation: u64, run: &RunId) {
        self.inner.inputs_evaluated(generation, run);
    }
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let forecast = matches!(request, Request::Forecast(_));
            let response = self.inner.call(request).await;
            if forecast && self.lock_forecast.swap(false, Ordering::SeqCst) {
                let connection = rusqlite::Connection::open(&self.database).unwrap();
                connection.execute_batch("BEGIN IMMEDIATE").unwrap();
                *self.blocker.lock().unwrap() = Some(connection);
            }
            response
        })
    }
}
impl DbCommitWorker {
    fn unlock(&self) {
        self.blocker
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .execute_batch("ROLLBACK")
            .unwrap();
    }
}
async fn committing_fixture(
    f: &Fx,
) -> (
    Arc<Coordinator>,
    Arc<FakeWorker>,
    Arc<DbCommitWorker>,
    TaskId,
) {
    let worker = f.worker("w-a", (2, 1));
    let c = Arc::new(f.open(&[]));
    let tasks = enqueue(&c, 1).await;
    let inner = gated_worker(&worker);
    inner.gate.publish(
        inner.gate.generation(),
        ready_input(&c.store().queued_tasks(1).unwrap()[0]),
    );
    let link = Arc::new(DbCommitWorker {
        inner,
        database: f.dir.path().join("coordinator.db"),
        blocker: Mutex::new(None),
        lock_forecast: AtomicBool::new(true),
        commit_entered: tokio::sync::Notify::new(),
    });
    c.attach(worker.id.clone(), link.clone()).await;
    (c, worker, link, tasks[0].clone())
}
async fn assert_pending_once<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_gate_sqlite_commit_linearizes_before_same_worker_replacement() {
    let f = fx();
    let (c, worker, link, task) = committing_fixture(&f).await;
    let tick_owner = c.clone();
    let tick = tokio::spawn(async move { tick_owner.tick().await });
    tokio::time::timeout(Duration::from_secs(2), link.commit_entered.notified())
        .await
        .unwrap();
    let replacement = gated_worker(&worker);
    let mut attaching = Box::pin(c.attach(worker.id.clone(), replacement.clone()));
    assert_pending_once(attaching.as_mut()).await;
    assert!(
        c.try_attach(worker.id.clone(), replacement.clone())
            .is_err(),
        "busy attachment must not silently succeed"
    );
    link.unlock();
    assert_eq!(tick.await.unwrap().unwrap().placed.len(), 1);
    attaching.await;
    assert!(replacement.gate.snapshot().is_empty());
    let before = only(&c, &task);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(only(&c, &task).attempt_id, before.attempt_id);
    assert_eq!(before.generation, Generation(1));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_gate_sqlite_commit_linearizes_before_detach() {
    let f = fx();
    let (c, worker, link, task) = committing_fixture(&f).await;
    let tick_owner = c.clone();
    let tick = tokio::spawn(async move { tick_owner.tick().await });
    tokio::time::timeout(Duration::from_secs(2), link.commit_entered.notified())
        .await
        .unwrap();
    let mut detaching = Box::pin(c.detach(&worker.id));
    assert_pending_once(detaching.as_mut()).await;
    link.unlock();
    assert_eq!(tick.await.unwrap().unwrap().placed.len(), 1);
    detaching.await.unwrap();
    let before = only(&c, &task);
    assert_eq!(before.observation, Observation::Unknown);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(only(&c, &task).attempt_id, before.attempt_id);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_gate_dropped_tick_keeps_registry_and_generation_fences_through_db_commit() {
    let f = fx();
    let (c, worker, link, task) = committing_fixture(&f).await;
    let tick_owner = c.clone();
    let tick = tokio::spawn(async move { tick_owner.tick().await });
    tokio::time::timeout(Duration::from_secs(2), link.commit_entered.notified())
        .await
        .unwrap();
    tick.abort();
    assert!(tick.await.unwrap_err().is_cancelled());
    let generation = link.inner.gate.generation();
    let mut resetting = Box::pin(link.inner.gate.observe(
        WorkerBootId::parse("replacement-boot").unwrap(),
        AgentEpoch(9),
    ));
    assert_pending_once(resetting.as_mut()).await;
    let replacement = gated_worker(&worker);
    let mut attaching = Box::pin(c.attach(worker.id.clone(), replacement.clone()));
    assert_pending_once(attaching.as_mut()).await;
    assert!(c.try_attach(worker.id.clone(), replacement).is_err());
    assert_eq!(link.inner.gate.generation(), generation);
    link.unlock();
    resetting.await;
    attaching.await;
    assert_ne!(link.inner.gate.generation(), generation);
    let attempt = only(&c, &task);
    assert_eq!(attempt.generation, Generation(1));
    assert!(!attempt.receipts.reached(Receipt::WorkerAccepted));
}
struct ReplacesDuringForecast {
    inner: Arc<InputGatedWorker>,
    owner: std::sync::Weak<Coordinator>,
    replacement: Arc<InputGatedWorker>,
    replace: AtomicBool,
    unrestricted: bool,
}
impl WorkerLink for ReplacesDuringForecast {
    fn placement_inputs(&self) -> Option<remote_task_runner::coordinator::PlacementInputs> {
        if self.unrestricted {
            None
        } else {
            self.inner.placement_inputs()
        }
    }
    fn placement_fence(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
        if self.unrestricted {
            None
        } else {
            self.inner.placement_fence()
        }
    }
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let forecast = matches!(request, Request::Forecast(_));
            let answer = self.inner.call(request).await;
            if forecast && self.replace.swap(false, Ordering::SeqCst) {
                self.owner
                    .upgrade()
                    .unwrap()
                    .attach(self.inner.worker.id.clone(), self.replacement.clone())
                    .await;
            }
            answer
        })
    }
}
#[tokio::test]
async fn source_gate_replacement_during_forecast_finishes_and_rejects_old_decision() {
    let f = fx();
    let c = Arc::new(f.open(&[]));
    let worker = f.worker("w-a", (2, 1));
    let tasks = enqueue(&c, 1).await;
    let inner = gated_worker(&worker);
    inner.gate.publish(
        inner.gate.generation(),
        ready_input(&c.store().queued_tasks(1).unwrap()[0]),
    );
    let replacement = gated_worker(&worker);
    let link = Arc::new(ReplacesDuringForecast {
        inner,
        owner: Arc::downgrade(&c),
        replacement: replacement.clone(),
        replace: AtomicBool::new(true),
        unrestricted: false,
    });
    c.attach(worker.id.clone(), link).await;
    assert!(tokio::time::timeout(Duration::from_secs(2), c.tick())
        .await
        .unwrap()
        .unwrap()
        .placed
        .is_empty());
    assert!(attempts(&c, &tasks[0]).is_empty());
    assert!(replacement.gate.snapshot().is_empty());
}

#[tokio::test]
async fn source_gate_unrestricted_forecast_cannot_commit_into_a_gated_replacement() {
    let f = fx();
    let c = Arc::new(f.open(&[]));
    let worker = f.worker("w-a", (2, 1));
    let tasks = enqueue(&c, 1).await;
    let replacement = gated_worker(&worker);
    let link = Arc::new(ReplacesDuringForecast {
        inner: gated_worker(&worker),
        owner: Arc::downgrade(&c),
        replacement: replacement.clone(),
        replace: AtomicBool::new(true),
        unrestricted: true,
    });
    c.attach(worker.id.clone(), link).await;
    assert!(tokio::time::timeout(Duration::from_secs(2), c.tick())
        .await
        .unwrap()
        .unwrap()
        .placed
        .is_empty());
    assert!(attempts(&c, &tasks[0]).is_empty());
    assert!(replacement.gate.snapshot().is_empty());
}

#[tokio::test]
async fn in_place_scheduler_uses_only_explicit_parent_local_and_never_automatic_retry() {
    let f = fx();
    let remote = f.worker("a-remote", (1, 0));
    let local = f.worker("z-local", (1, 0));
    let c = f.open(&[&remote, &local]);
    let task = TaskId::generate();
    let mut input = spec();
    input.in_place = true;
    c.enqueue(RunId::generate(), source(&c), vec![(task.clone(), input)])
        .await
        .unwrap();
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(remote.holds().is_empty());
    c.set_parent_local(local.id.clone());
    c.tick().await.unwrap();
    let attempt = only(&c, &task);
    assert_eq!(attempt.worker_id, local.id);
    assert!(remote.holds().is_empty());
    c.request_retire(&attempt.attempt_id, "move").await.unwrap();
    c.tick().await.unwrap();
    assert_eq!(
        only(&c, &task).termination.unwrap().outcome(),
        Outcome::Failed
    );
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(attempts(&c, &task).len(), 1);
    assert!(remote.holds().is_empty());
}

#[tokio::test]
async fn capability_filter_runs_before_forecast_and_records_unmet_requirements() {
    use remote_task_runner::toolchain::capability::*;
    let f = fx();
    let old = f.worker("old", (1, 1));
    let capable = f.worker("capable", (1, 1));
    capable.set(|s| {
        s.capabilities = Some(WorkerCapabilities {
            schema_version: 1,
            os: "linux".into(),
            arch: "x86_64".into(),
            profile: "node22".into(),
            profile_digest: Digest::of_bytes(b"definition"),
            execution_uid: 1,
            protocol_versions: vec![1],
            profile_fingerprint: Digest::of_bytes(b"p"),
            tools: [(
                "node".into(),
                ToolObservation::Available {
                    version: "v22.0.0".into(),
                    identity: Digest::of_bytes(b"node"),
                },
            )]
            .into(),
            features: [FEATURE.into()].into(),
            disk_free_bytes: None,
            resource_budget: None,
        })
    });
    let c = f.open(&[&old, &capable]);
    let task = TaskId::generate();
    let run = RunId::generate();
    let mut task_spec = spec();
    task_spec.requirements =
        ExecutionRequirements::parse(Some("node22".into()), &["node".into(), "linux".into()])
            .unwrap();
    c.enqueue(
        run.clone(),
        source(&c),
        vec![(task.clone(), task_spec.clone())],
    )
    .await
    .unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    assert_eq!(old.received(Op::Forecast), 0);
    assert_eq!(capable.received(Op::Forecast), 1);
    assert_eq!(only(&c, &task).worker_id, capable.id);
    assert_eq!(
        capable
            .held(&only(&c, &task).attempt_id)
            .unwrap()
            .payload
            .requirements,
        task_spec.requirements
    );
    let mismatch_task = TaskId::generate();
    task_spec.requirements.profile = Some("unknown".into());
    c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(mismatch_task.clone(), task_spec)],
    )
    .await
    .unwrap();
    c.tick().await.unwrap();
    assert!(attempts(&c, &mismatch_task).is_empty());
    assert_eq!(old.received(Op::Forecast), 0);
    let event = c
        .store()
        .events_after(0, 100)
        .unwrap()
        .into_iter()
        .find(|e| {
            e.kind == remote_task_runner::scheduler::EVENT_TASK_WAITING
                && e.payload["task"] == mismatch_task.to_string()
        })
        .unwrap();
    assert_eq!(event.payload["blocked_reason"], "no_compatible_worker");
    let public = c
        .store()
        .query_task(mismatch_task)
        .unwrap()
        .unwrap()
        .placement
        .unwrap();
    assert_eq!(
        public.blocked_reason,
        Some(BlockedReason::NoCompatibleWorker)
    );
    assert!(!public.truncated);
    assert!(public.excluded.iter().all(|w| !w.unmet.is_empty()));

    assert!(event.payload["decision"]["excluded"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["why"] == "capability_mismatch" && !e["unmet"].as_array().unwrap().is_empty()));
}

struct ToolchainAttestor {
    worker: Arc<FakeWorker>,
    stall_resolution: bool,
}
impl WorkerLink for ToolchainAttestor {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        self.worker.call(request)
    }
    fn read_only_call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            match request {
                Request::ResolveToolchains(r) if !self.stall_resolution => {
                    Ok(Response::ResolveToolchains(ResolveToolchainsResponse {
                        schema_version: SchemaVersion::CURRENT,
                        bundle: r.bundle,
                    }))
                }
                Request::ResolveToolchains(_) => std::future::pending().await,
                other => self.worker.call(other).await,
            }
        })
    }
}
#[tokio::test]
async fn slow_toolchain_attestation_respects_round_budget_and_other_lease_progress() {
    use remote_task_runner::toolchain::{contract::FrozenBundle, versions::*};
    let mut f = fx();
    f.cfg.worker_budget_ms = 1500;
    f.sched.forecast_wait_ms = 50;
    let healthy = f.worker("w-b", (2, 0));
    let c = f.open(&[&healthy]);
    let first = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let active = only(&c, &first[0]);
    healthy.start(&active.attempt_id);
    c.tick().await.unwrap();
    let before = healthy.held(&active.attempt_id).unwrap().renewals;
    let stalled = f.worker("w-a", (2, 0));
    c.attach(
        stalled.id.clone(),
        Arc::new(ToolchainAttestor {
            worker: stalled,
            stall_resolution: true,
        }),
    )
    .await;
    c.attach(
        healthy.id.clone(),
        Arc::new(ToolchainAttestor {
            worker: healthy.clone(),
            stall_resolution: false,
        }),
    )
    .await;
    let mut task_spec = spec();
    let scope = Scope::project("");
    task_spec.toolchains = Some(
        FrozenBundle::seal_exact(
            FrozenToolchains::freeze(
                task_spec.manifest_digest.clone(),
                Digest::of_bytes(b"parent"),
                scope.clone(),
                &[Declaration {
                    tool: ToolId::parse("node").unwrap(),
                    constraint: Constraint::parse("20.0.0").unwrap(),
                    scope,
                    location: Location {
                        path: ".node-version".into(),
                        first_line: 1,
                        last_line: 1,
                    },
                }],
                &[],
                &BTreeMap::new(),
                MatchPolicy::Exact,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let task = TaskId::generate();
    c.enqueue(
        RunId::generate(),
        source(&c),
        vec![(task.clone(), task_spec)],
    )
    .await
    .unwrap();
    let begun = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(3), c.tick())
        .await
        .unwrap()
        .unwrap();
    assert!(
        begun.elapsed() < Duration::from_secs(1),
        "a complete attestation cannot spend the full peer budget"
    );
    assert_eq!(
        healthy.held(&active.attempt_id).unwrap().renewals,
        before + 1
    );
    assert_eq!(only(&c, &task).worker_id, healthy.id);
    healthy.finish(&active.attempt_id, Outcome::Succeeded);
    c.tick().await.unwrap();
    assert_eq!(only(&c, &first[0]).state, AttemptState::Terminal);
}

#[tokio::test]
async fn measured_cold_choice_waits_without_attempt_then_failed_prefetch_places_healthy_warm() {
    measured_cold_fixture(true, false, false).await;
}
#[tokio::test]
async fn unknown_cold_candidate_keeps_existing_ready_gate_eligibility() {
    measured_cold_fixture(false, false, false).await;
}
#[tokio::test]
async fn measured_cold_candidate_cannot_bypass_frozen_toolchain_eligibility() {
    measured_cold_fixture(true, true, false).await;
}
#[tokio::test]
async fn measured_cold_frozen_attested_candidate_prefetches_before_attempt() {
    measured_cold_fixture(true, true, true).await;
}
async fn measured_cold_fixture(known: bool, frozen: bool, cold_attests: bool) {
    use remote_task_runner::scheduler::measured::{CostQuery, MeasuredCosts, Phase, Sample};
    let mut f = fx();
    let stats = crate::test_tmp::tempdir().unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(stats.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let model = Arc::new(
        MeasuredCosts::open(
            &std::fs::canonicalize(stats.path()).unwrap(),
            Default::default(),
        )
        .unwrap(),
    );
    f.costs = model.clone();
    let warm = f.worker("warm", (2, 1));
    let cold = f.worker("cold", (2, 1));
    warm.set(|s| s.forecasting.run_ms = 100_000);
    cold.set(|s| s.forecasting.run_ms = 1000);
    let c = f.open(&[]);
    let warm_link = Arc::new(InputGatedWorker {
        worker: warm.clone(),
        gate: Arc::new(
            remote_task_runner::coordinator::InputGate::with_transport_policy(
                "fixture-rpc-v1".into(),
            ),
        ),
        invalidate_forecast: AtomicBool::new(false),
        attestor: frozen.then(|| {
            Arc::new(ToolchainAttestor {
                worker: warm.clone(),
                stall_resolution: false,
            })
        }),
    });
    let cold_link = Arc::new(InputGatedWorker {
        worker: cold.clone(),
        gate: Arc::new(
            remote_task_runner::coordinator::InputGate::with_transport_policy(
                "fixture-rpc-v1".into(),
            ),
        ),
        invalidate_forecast: AtomicBool::new(false),
        attestor: frozen.then(|| {
            Arc::new(ToolchainAttestor {
                worker: cold.clone(),
                stall_resolution: !cold_attests,
            })
        }),
    });
    for link in [&warm_link, &cold_link] {
        link.gate
            .observe(WorkerBootId::parse("boot-1").unwrap(), AgentEpoch(1))
            .await;
    }
    c.attach(warm.id.clone(), warm_link.clone()).await;
    c.attach(cold.id.clone(), cold_link.clone()).await;
    let tasks = if frozen {
        use remote_task_runner::toolchain::{contract::FrozenBundle, versions::*};
        let mut task_spec = spec();
        let scope = Scope::project("");
        task_spec.toolchains = Some(
            FrozenBundle::seal_exact(
                FrozenToolchains::freeze(
                    task_spec.manifest_digest.clone(),
                    Digest::of_bytes(b"parent"),
                    scope.clone(),
                    &[Declaration {
                        tool: ToolId::parse("node").unwrap(),
                        constraint: Constraint::parse("20.0.0").unwrap(),
                        scope,
                        location: Location {
                            path: ".node-version".into(),
                            first_line: 1,
                            last_line: 1,
                        },
                    }],
                    &[],
                    &BTreeMap::new(),
                    MatchPolicy::Exact,
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let id = TaskId::generate();
        c.enqueue(RunId::generate(), source(&c), vec![(id.clone(), task_spec)])
            .await
            .unwrap();
        vec![id]
    } else {
        enqueue(&c, 1).await
    };
    let task = c.store().queued_tasks(1).unwrap().remove(0);
    warm_link
        .gate
        .publish(warm_link.gate.generation(), ready_input(&task));
    if known {
        let query = CostQuery::task(
            &task.spec,
            task.source_id.as_ref().map(|s| s.as_str()),
            cold_link.cost_transport(),
        );
        let now = remote_task_runner::platform::monotonic_ms();
        model.record(
            Sample::verified(
                &cold.id,
                &query,
                Phase::Transfer,
                "verified-fixture-exchange".into(),
                now.saturating_sub(10),
                10,
            )
            .unwrap(),
        );
    }
    model.drain();
    let report = c.tick().await.unwrap();
    if known && (!frozen || cold_attests) {
        assert!(report.placed.is_empty());
        assert!(attempts(&c, &tasks[0]).is_empty());
        assert_eq!(cold_link.gate.preferred().unwrap().task_id, task.task_id);
        assert!(cold.count(Op::Forecast) > 0);
        // A real delivery error suppresses the cold choice; the next normal
        // scheduler comparison places the healthy ready worker.
        cold_link.gate.fail_preference(&task.run_id);
        assert_eq!(c.tick().await.unwrap().placed.len(), 1);
    } else {
        assert_eq!(report.placed.len(), 1);
        assert_eq!(cold.count(Op::Forecast), 0);
    }
    assert_eq!(only(&c, &tasks[0]).worker_id, warm.id);
    let evidence =
        remote_task_runner::scheduler::cost_evidence_of(c.store(), &only(&c, &tasks[0]).attempt_id)
            .unwrap()
            .unwrap();
    let warm_evidence = &evidence
        .iter()
        .find(|e| e.worker_id == warm.id)
        .unwrap()
        .costs;
    assert_eq!(
        warm_evidence.transfer.kind,
        remote_task_runner::scheduler::measured::EvidenceKind::VerifiedReady
    );
    assert_eq!(warm_evidence.transfer.sample_count, Some(0));
    assert_eq!(warm_evidence.setup.ms, None);
    assert!(cold_link.gate.preferred().is_none());
    assert_eq!(warm.count(Op::Submit), 1);
    assert_eq!(cold.count(Op::Submit), 0);
    if frozen && !cold_attests {
        assert_eq!(warm.count(Op::Forecast), 1);
        assert_eq!(cold.count(Op::Forecast), 0);
    }
}

// ------------------------------------------- more live attempts than a page

/// The live attempts of a worker are walked a page at a time (64). With more
/// than that, every one is still followed, and one that ends while the walk
/// is under way neither hides another nor is settled twice.
#[tokio::test]
async fn a_worker_with_more_live_attempts_than_a_page_is_followed_completely() {
    const MANY: usize = 150;
    let mut f = fx();
    f.cfg.limits = Limits {
        max_active_task: MANY as u32,
        prefetch_per_worker: 0,
    };
    let a = f.worker("w-a", (MANY as u32, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, MANY).await;
    let placed = c.tick().await.unwrap().placed;
    assert_eq!(placed.len(), MANY);
    assert_eq!(a.holds().len(), MANY);

    // Each of them is seen running, the ones of the later pages too.
    for id in &placed {
        a.start(id);
    }
    assert!(c.tick().await.unwrap().settled.is_empty());
    for task in &tasks {
        assert_eq!(only(&c, task).state, AttemptState::Active);
    }

    // All end at once: each page read finds the earlier ones gone already.
    for id in &placed {
        a.finish(id, Outcome::Succeeded);
    }
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, placed);
    assert!(report.unknown.is_empty());
    assert!(c.tick().await.unwrap().settled.is_empty());
    for task in &tasks {
        assert_eq!(
            only(&c, task).termination.unwrap().outcome(),
            Outcome::Succeeded
        );
    }
}
