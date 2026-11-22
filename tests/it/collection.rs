//! Result collection (spec chapter 13) against a worker that keeps the
//! worker side of the contract in memory: A30 (a connection lost while
//! collecting: `collecting` is kept and only the collection is continued),
//! A31 (a missing or empty result is not a success), a crash of the parent
//! at each boundary of a collection, corrupted bytes, and the order of the
//! acknowledgement.
//!
//! No process is started here. A "crash" is the coordinator dropped where
//! it stands and its database and result store opened again.

use belt::ci::status::Computed;
use belt::coordinator::policy::Limits;
use belt::coordinator::{Coordinator, CoordinatorConfig, LinkError, TaskSpec, WorkerLink};
use belt::error::{ApiError, ErrorCode};
use belt::ids::*;
use belt::platform::wall_ms;
use belt::results::plan::{RunVerdict, TaskStanding, Verdict};
// Deterministic clock samples for retention integration; production uses
// suspend-aware platform samples and a persisted quarantine baseline.
#[derive(Default)]
struct Sweep {
    deleted: Vec<String>,
}
fn sweep(
    store: &belt::storage::CoordinatorStore,
    files: &ResultStore,
    now: u64,
    days: u32,
) -> Result<Sweep, belt::results::CollectError> {
    let retention = belt::results::plan::retention_ms(days);
    let mut report = Sweep::default();
    for run in store.query_runs(64, None)?.items {
        let guard = crate::test_tmp::gc_guard(store, &run.run_id);
        // Give the fixture a full, known elapsed retention interval.
        store.claim_result_page(
            &guard,
            &run.run_id,
            "",
            64,
            1,
            1,
            Some("integration".into()),
            retention,
        )?;
        let mut after = String::new();
        loop {
            let page = store.claim_result_page(
                &guard,
                &run.run_id,
                &after,
                64,
                now,
                now,
                Some("integration".into()),
                retention,
            )?;
            for claim in store.pending_result_page(&guard, &run.run_id, 64)? {
                files.remove(&claim.locator)?;
                store.finish_result_delete(&guard, &claim)?;
                report.deleted.push(claim.locator);
            }
            match page.next {
                Some(next) => after = next,
                None => break,
            }
        }
    }
    Ok(report)
}
use belt::results::{CollectConfig, CollectReport, ResultStore};
use belt::state::{
    AttemptState, Collection, Observation, Outcome, Receipt, Receipts, RunDisplay, Termination,
};
use belt::storage::{CoordinatorStore, Seen, StorageError};
use belt::wire::*;
use serde_json::json;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

const DAY: u64 = 86_400_000;

// ------------------------------------------------------------ the worker

/// Where a request is lost. The worker is gone from then on, until it is
/// brought back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lost {
    /// It never arrives.
    Request,
    /// It is carried out and its answer never arrives.
    Answer,
}

/// What an ended attempt left on the worker.
#[derive(Debug, Clone, Default)]
struct Out {
    outputs_evaluated: Option<bool>,
    /// A result is stored (otherwise the attempt has its termination only).
    has_result: bool,
    /// The records of the result, each with the bytes the worker serves.
    artifacts: Vec<(ArtifactRecord, Vec<u8>)>,
    not_stored: Vec<ArtifactNotStored>,
    log: Vec<u8>,
    stderr: Vec<u8>,
    /// The spool is closed: the log has its end mark.
    log_closed: bool,
    dropped: u64,
}

struct Job {
    payload: AttemptPayload,
    state: AttemptState,
    termination: Option<Termination>,
    out: Out,
    /// The digests that were acknowledged, in order.
    acks: Vec<Digest>,
}

#[derive(Default)]
struct State {
    jobs: BTreeMap<AttemptId, Job>,
    one_slot: bool,
    pending_artifacts: BTreeMap<AttemptId, ArtifactAction>,
    artifact_answers: BTreeMap<Digest, ArtifactAnswer>,
    artifact_data: BTreeMap<Digest, Vec<u8>>,
    offline: bool,
    /// Lose the n-th request (from 1) of a kind, and be gone after it.
    trip: Option<(&'static str, usize, Lost)>,
    /// How many of the next chunks arrive with a flipped byte.
    corrupt_chunks: u32,
    /// The next chunk says it is from one byte further.
    shift_chunk: bool,
    shift_log: bool,
    /// What was left behind for an attempt that has no result yet is stored
    /// as its result the moment an acknowledgement arrives.
    result_appears_on_ack: bool,
    /// Answer the next requests of a kind with an error: the kind, the
    /// code, and how many times.
    fail: Option<(&'static str, ErrorCode, u32)>,
    /// Every request that arrived: its kind and (for bytes) its offset.
    ops: Vec<(&'static str, u64)>,
    /// The connection did not negotiate `ci_task_v1`.
    no_ci: bool,
    no_discovery: bool,
    /// The worker did not take the frozen plan of any run, for this reason:
    /// no input is ready there.
    refused_plan: Option<String>,
    refused_runs: Vec<RunId>,
    /// Attempts that were cancelled before they arrived: they never start.
    tombstones: Vec<AttemptId>,
}

struct Worker {
    id: WorkerId,
    state: Mutex<State>,
}

fn err(code: ErrorCode) -> ApiError {
    ApiError::new(code, format!("{code}"))
}

fn ended(outcome: Outcome) -> Termination {
    Termination::new(
        outcome,
        None,
        (outcome == Outcome::TimedOut).then_some(belt::state::TimeoutInfo {
            kind: belt::state::TimeoutKind::ResourceWait,
            phase: belt::state::TimeoutPhase::Waiting,
        }),
    )
    .unwrap()
}

fn digest_of(job: &Job) -> Digest {
    let t = job.termination.unwrap();
    if job.out.has_result {
        let records: Vec<&ArtifactRecord> = job.out.artifacts.iter().map(|(r, _)| r).collect();
        payload_hash(&json!({
            "termination": t,
            "artifacts": records,
            "artifacts_not_stored": job.out.not_stored,
            "logs": job.out.log.len(),
        }))
        .unwrap()
    } else {
        payload_hash(&json!({ "termination": t, "artifacts": [] })).unwrap()
    }
}

impl Worker {
    fn new(name: &str) -> Arc<Worker> {
        Arc::new(Worker {
            id: WorkerId::parse(name).unwrap(),
            state: Mutex::default(),
        })
    }

    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut self.state.lock().unwrap());
    }

    fn count(&self, kind: &str) -> usize {
        let s = self.state.lock().unwrap();
        s.ops.iter().filter(|(k, _)| *k == kind).count()
    }

    fn offsets(&self, kind: &str) -> Vec<u64> {
        let s = self.state.lock().unwrap();
        let of_kind = s.ops.iter().filter(|(k, _)| *k == kind);
        of_kind.map(|(_, o)| *o).collect()
    }

    fn acks(&self, a: &AttemptId) -> Vec<Digest> {
        self.state.lock().unwrap().jobs[a].acks.clone()
    }

    fn digest(&self, a: &AttemptId) -> Digest {
        digest_of(&self.state.lock().unwrap().jobs[a])
    }

    fn payload(&self, a: &AttemptId) -> AttemptPayload {
        self.state.lock().unwrap().jobs[a].payload.clone()
    }

    /// The attempt ends and leaves `out`.
    fn finish(&self, a: &AttemptId, outcome: Outcome, out: Out) {
        let mut s = self.state.lock().unwrap();
        let job = s.jobs.get_mut(a).unwrap();
        job.state = AttemptState::Terminal;
        job.termination = Some(ended(outcome));
        job.out = out;
    }

    fn status(a: &AttemptId, job: &Job) -> AttemptStatus {
        let mut receipts = Receipts::default();
        if job.out.outputs_evaluated == Some(false) {
            receipts.mark(Receipt::CommandRegistered, 1);
        }
        AttemptStatus {
            schema_version: SchemaVersion::CURRENT,
            attempt_id: a.clone(),
            run_id: job.payload.run_id.clone(),
            task_id: job.payload.task_id.clone(),
            generation: job.payload.generation,
            worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
            agent_epoch: AgentEpoch(1),
            state: job.state,
            receipts,
            spawned_at_ms: None,
            observation: Observation::Known,
            termination: job.termination,
            nodes: vec![],
            steps: vec![],
            retired: false,
            log_truncated: false,
            log_dropped_bytes: 0,
        }
    }

    fn handle(&self, request: Request) -> Result<Response, ApiError> {
        let mut s = self.state.lock().unwrap();
        let job_of = |s: &mut State, a: &AttemptId| -> Result<(), ApiError> {
            match s.jobs.get(a) {
                Some(j) if j.state == AttemptState::Terminal => Ok(()),
                Some(_) => Err(err(ErrorCode::Conflict)),
                None => Err(err(ErrorCode::NotFound)),
            }
        };
        match request {
            Request::WorkflowArtifact(r) => {
                let result = match r.command {
                    WorkflowArtifactCommand::Poll => ArtifactExchangeResult::Pending {
                        action: s.pending_artifacts.get(&r.attempt_id).cloned(),
                    },
                    WorkflowArtifactCommand::Answer { key, answer } => {
                        assert_eq!(&s.pending_artifacts[&r.attempt_id].key, &key);
                        s.artifact_answers.insert(key, answer);
                        ArtifactExchangeResult::Accepted
                    }
                    WorkflowArtifactCommand::Read {
                        key,
                        digest,
                        offset,
                    } => {
                        assert_eq!(&s.pending_artifacts[&r.attempt_id].key, &key);
                        let blob = &s.artifact_data[&digest];
                        let start = offset as usize;
                        ArtifactExchangeResult::Chunk {
                            data: Bytes(blob[start..blob.len().min(start + 128 * 1024)].to_vec()),
                        }
                    }
                };
                Ok(Response::WorkflowArtifact(WorkflowArtifactResponse {
                    schema_version: SchemaVersion::CURRENT,
                    result,
                }))
            }
            Request::Inspect(_) => Ok(Response::Inspect(InspectResponse {
                own_runs_active: None,
                admission: None,
                preparation_observations: None,
                capabilities: None,
                schema_version: SchemaVersion::CURRENT,
                worker_id: self.id.clone(),
                worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
                agent_epoch: AgentEpoch(1),
                snapshot_digest: Digest::of_bytes(b"s"),
                policy_digest: Digest::of_bytes(b"p"),
                active_task: 0,
                prefetch_pending: 0,
                max_active_task: if s.one_slot { 1 } else { 8 },
                prefetch_per_worker: if s.one_slot { 0 } else { 8 },
                disk_free_bytes: 1,
                draining: false,
            })),
            Request::GetAttempt(r) => match s.jobs.get(&r.attempt_id) {
                Some(job) => Ok(Response::GetAttempt(Self::status(&r.attempt_id, job))),
                None if s.tombstones.contains(&r.attempt_id) => Err(err(ErrorCode::Retired)),
                None => Err(err(ErrorCode::NotFound)),
            },
            Request::SubmitAttempt(r) => {
                if s.tombstones.contains(&r.attempt_id) {
                    return Err(err(ErrorCode::Retired));
                }
                let generation = r.payload.generation;
                let job = s.jobs.entry(r.attempt_id.clone()).or_insert(Job {
                    payload: r.payload,
                    state: AttemptState::Accepted,
                    termination: None,
                    out: Out::default(),
                    acks: Vec::new(),
                });
                Ok(Response::SubmitAttempt(SubmitAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    generation,
                    state: job.state,
                    receipts: Receipts::default(),
                    duplicate: false,
                }))
            }
            Request::RenewLease(r) => Ok(Response::RenewLease(RenewLeaseResponse {
                schema_version: SchemaVersion::CURRENT,
                attempt_id: r.attempt_id.clone(),
                lease_seq: r.lease_seq,
                extended: true,
                lease_remaining_ms: r.lease_ms,
                state: s.jobs[&r.attempt_id].state,
            })),
            Request::GetResult(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let job = &s.jobs[&r.attempt_id];
                let stored = job.out.has_result;
                Ok(Response::GetResult(GetResultResponse {
                    outputs_evaluated: job
                        .out
                        .outputs_evaluated
                        .unwrap_or(stored && !job.payload.outputs.is_empty()),
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id.clone(),
                    generation: r.generation,
                    termination: job.termination.unwrap(),
                    nodes: vec![],
                    artifacts: if stored {
                        job.out.artifacts.iter().map(|(r, _)| r.clone()).collect()
                    } else {
                        vec![]
                    },
                    artifacts_not_stored: if stored {
                        job.out.not_stored.clone()
                    } else {
                        vec![]
                    },
                    result_digest: digest_of(job),
                }))
            }
            Request::ReadLog(r) => {
                if !s.jobs.contains_key(&r.attempt_id) {
                    return Err(err(ErrorCode::NotFound));
                }
                let shift = std::mem::take(&mut s.shift_log);
                let out = &s.jobs[&r.attempt_id].out;
                let log: &[u8] = match r.stream {
                    LogStream::Stdout => &out.log,
                    LogStream::Stderr => &out.stderr,
                };
                let from = (r.offset as usize).min(log.len());
                let to = (from + r.max_bytes.max(1) as usize).min(log.len());
                Ok(Response::ReadLog(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: r.offset + u64::from(shift),
                    data: Bytes(log[from..to].to_vec()),
                    next_offset: to as u64,
                    eof: out.log_closed && to == log.len(),
                    log_truncated: out.log_closed && out.dropped > 0,
                    dropped_bytes: if out.log_closed { out.dropped } else { 0 },
                }))
            }
            Request::BlobGet(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let flip = s.corrupt_chunks > 0;
                if flip && s.corrupt_chunks != u32::MAX {
                    s.corrupt_chunks -= 1;
                }
                let shift = std::mem::take(&mut s.shift_chunk);
                let out = &s.jobs[&r.attempt_id].out;
                let found = out.artifacts.iter().find(|(a, _)| a.digest == r.digest);
                let Some((record, bytes)) = found.filter(|_| out.has_result) else {
                    return Err(err(ErrorCode::NotFound));
                };
                if r.offset > record.size {
                    return Err(err(ErrorCode::InvalidInput));
                }
                let from = r.offset as usize;
                let to = (from + r.max_bytes.max(1) as usize).min(bytes.len());
                let mut data = bytes[from..to].to_vec();
                if flip && !data.is_empty() {
                    data[0] ^= 0xff;
                }
                Ok(Response::BlobGet(BlobGetResponse {
                    schema_version: SchemaVersion::CURRENT,
                    digest: r.digest,
                    offset: r.offset + u64::from(shift),
                    total_size: record.size,
                    data: Bytes(data),
                    eof: to == bytes.len(),
                }))
            }
            Request::AckCollected(r) => {
                job_of(&mut s, &r.attempt_id)?;
                let appears = s.result_appears_on_ack;
                let job = s.jobs.get_mut(&r.attempt_id).unwrap();
                if appears && !job.out.has_result {
                    job.out.has_result = true;
                    job.out.log_closed = true;
                }
                if digest_of(job) != r.result_digest {
                    return Err(err(ErrorCode::Conflict));
                }
                job.acks.push(r.result_digest);
                Ok(Response::AckCollected(AckCollectedResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    released: true,
                }))
            }
            // It has no estimate to give: the scheduler then places by what
            // the parent itself holds.
            Request::Forecast(_) => Err(err(ErrorCode::Internal)),
            // A cancel stops the attempt at once; it leaves no result.
            Request::CancelAttempt(r) => {
                let Some(job) = s.jobs.get_mut(&r.attempt_id) else {
                    // It never arrived: the worker remembers the cancel.
                    s.tombstones.push(r.attempt_id.clone());
                    return Ok(Response::CancelAttempt(CancelAttemptResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        phase: CancelPhase::Accepted,
                        state: None,
                        tombstone: true,
                    }));
                };
                if job.state != AttemptState::Terminal {
                    job.state = AttemptState::Terminal;
                    job.termination = Some(ended(Outcome::Cancelled));
                }
                Ok(Response::CancelAttempt(CancelAttemptResponse {
                    schema_version: SchemaVersion::CURRENT,
                    attempt_id: r.attempt_id,
                    phase: CancelPhase::Completed,
                    state: Some(AttemptState::Terminal),
                    tombstone: false,
                }))
            }
            other => panic!("not sent to this worker: {other:?}"),
        }
    }
}

fn kind_of(request: &Request) -> (&'static str, u64) {
    match request {
        Request::WorkflowArtifact(r) => match &r.command {
            WorkflowArtifactCommand::Read { offset, .. } => ("workflow_read", *offset),
            WorkflowArtifactCommand::Answer { .. } => ("workflow_ack", 0),
            WorkflowArtifactCommand::Poll => ("workflow_poll", 0),
        },
        Request::Inspect(_) => ("inspect", 0),
        Request::GetAttempt(_) => ("get", 0),
        Request::SubmitAttempt(_) => ("submit", 0),
        Request::RenewLease(_) => ("renew", 0),
        Request::GetResult(_) => ("result", 0),
        Request::ReadLog(r) => ("log", r.offset),
        Request::BlobGet(r) => ("blob", r.offset),
        Request::AckCollected(_) => ("ack", 0),
        Request::Forecast(_) => ("forecast", 0),
        Request::CancelAttempt(_) => ("cancel", 0),
        other => panic!("{other:?}"),
    }
}

impl WorkerLink for Worker {
    fn workflow_artifacts_negotiated(&self) -> bool {
        self.ci_task_negotiated()
    }

    fn matrix_dispatch_negotiated(&self) -> bool {
        self.ci_task_negotiated()
    }
    fn ci_discovery_negotiated(&self) -> bool {
        let worker = self.state.lock().unwrap();
        !worker.no_ci && !worker.no_discovery
    }
    fn ci_task_negotiated(&self) -> bool {
        !self.state.lock().unwrap().no_ci
    }
    fn placement_inputs(&self) -> Option<belt::coordinator::PlacementInputs> {
        let refused = self.state.lock().unwrap().refused_plan.is_some();
        refused.then(|| belt::coordinator::PlacementInputs {
            generation: 1,
            ready: Vec::new(),
            identity: None,
        })
    }
    fn input_refusals(&self) -> Vec<(RunId, String)> {
        let state = self.state.lock().unwrap();
        let why = state.refused_plan.iter();
        why.flat_map(|why| {
            state
                .refused_runs
                .iter()
                .map(|run| (run.clone(), why.clone()))
        })
        .collect()
    }
    fn cost_transport(&self) -> Option<belt::scheduler::measured::Transport> {
        Some(belt::scheduler::measured::Transport {
            worker_boot: WorkerBootId::parse("boot-1").unwrap(),
            agent_epoch: AgentEpoch(1),
            policy: "verified-memory-fixture-v1".into(),
        })
    }

    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let op = kind_of(&request);
            let lost = {
                let mut s = self.state.lock().unwrap();
                if s.offline {
                    Some(Lost::Request)
                } else {
                    s.ops.push(op);
                    let seen = s.ops.iter().filter(|(k, _)| *k == op.0).count();
                    match s.trip {
                        Some((kind, nth, lost)) if kind == op.0 && nth == seen => {
                            s.trip = None;
                            s.offline = true;
                            Some(lost)
                        }
                        _ => None,
                    }
                }
            };
            match lost {
                Some(Lost::Request) => Err(LinkError::Unconfirmed),
                Some(Lost::Answer) => {
                    let _ = self.handle(request);
                    Err(LinkError::Unconfirmed)
                }
                None => {
                    let refused = {
                        let mut s = self.state.lock().unwrap();
                        match s.fail {
                            Some((kind, code, left)) if kind == op.0 && left > 0 => {
                                s.fail = Some((kind, code, left - 1));
                                Some(code)
                            }
                            _ => None,
                        }
                    };
                    match refused {
                        Some(code) => Err(LinkError::Remote(err(code))),
                        None => self.handle(request).map_err(LinkError::Remote),
                    }
                }
            }
        })
    }
}

// ------------------------------------------------------------ the fixture

struct Fx {
    dir: TempDir,
    w: Arc<Worker>,
}

fn fx() -> Fx {
    Fx {
        dir: crate::test_tmp::tempdir().unwrap(),
        w: Worker::new("w-a"),
    }
}

/// Small chunks, so that every artifact and log takes several requests.
fn cfg() -> CollectConfig {
    CollectConfig {
        chunk_bytes: 4,
        budget_ms: 10_000,
        // The passes of a test follow each other at once.
        backoff_ms: 0,
        ..CollectConfig::default()
    }
}

impl Fx {
    /// Start a coordinator and open the result store on the state directory:
    /// the first, or the next after the one before was dropped.
    fn open(&self) -> (Coordinator, Arc<ResultStore>) {
        self.open_with(Arc::new(belt::scheduler::UnknownCosts))
    }
    fn open_with(
        &self,
        costs: Arc<dyn belt::scheduler::CostModel>,
    ) -> (Coordinator, Arc<ResultStore>) {
        let started = std::time::Instant::now();
        let store = loop {
            match CoordinatorStore::open(self.dir.path(), 1) {
                Err(StorageError::Locked(_)) if started.elapsed() < Duration::from_secs(5) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                other => break other.unwrap(),
            }
        };
        let cfg = CoordinatorConfig {
            limits: Limits {
                max_active_task: 8,
                prefetch_per_worker: 8,
            },
            lease_ms: 60_000,
            worker_budget_ms: 10_000,
        };
        let scheduler = belt::scheduler::Scheduler::new(Default::default(), costs).unwrap();
        let c = Coordinator::start_with(store, cfg, scheduler).unwrap();
        c.try_attach(
            self.w.id.clone(),
            Arc::clone(&self.w) as Arc<dyn WorkerLink>,
        )
        .unwrap();
        let files = ResultStore::open(&self.dir.path().join("results")).unwrap();
        (c, Arc::new(files))
    }
}

fn decl(name: &str, if_missing: MissingPolicy) -> ArtifactDecl {
    ArtifactDecl {
        name: name.into(),
        path: format!("out/{name}"),
        if_missing,
    }
}

fn spec(artifacts: Vec<ArtifactDecl>) -> TaskSpec {
    TaskSpec {
        ci: None,
        toolchains: None,
        outputs: Vec::new(),
        requirements: Default::default(),
        in_place: false,
        local: false,
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
        artifacts,
    }
}

/// A stored artifact of an attempt, as the worker records it.
fn stored(
    a: &AttemptId,
    p: &AttemptPayload,
    name: &str,
    bytes: &[u8],
) -> (ArtifactRecord, Vec<u8>) {
    let record = ArtifactRecord {
        encoding: belt::wire::ArtifactEncoding::Raw,
        name: name.into(),
        path: name.into(),
        size: bytes.len() as u64,
        digest: Digest::of_bytes(bytes),
        source_id: p.source_id.clone(),
        plan_digest: p.plan_digest.clone(),
        attempt_id: a.clone(),
        profile_fingerprint: Digest::of_bytes(b"profile"),
    };
    (record, bytes.to_vec())
}

fn not_stored(d: &ArtifactDecl, reason: NotStoredReason) -> ArtifactNotStored {
    ArtifactNotStored {
        name: d.name.clone(),
        path: d.path.clone(),
        if_missing: d.if_missing,
        reason,
    }
}

/// Queue one task, let the worker take it, and return its attempt (which
/// still runs).
async fn placed(
    fx: &Fx,
    c: &Coordinator,
    artifacts: Vec<ArtifactDecl>,
) -> (RunId, TaskId, AttemptId) {
    let (run, task) = (RunId::generate(), TaskId::generate());
    let source = SourceId::generate();
    c.store()
        .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    c.enqueue(run.clone(), source, vec![(task.clone(), spec(artifacts))])
        .await
        .unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    assert!(fx.w.count("submit") >= 1);
    (run, task, report.placed[0].clone())
}

/// The attempt ends on the worker with `out` and the parent sees it.
async fn end(fx: &Fx, c: &Coordinator, a: &AttemptId, outcome: Outcome, out: Out) {
    fx.w.finish(a, outcome, out);
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![a.clone()]);
}

const ALPHA: &[u8] = b"0123456789";
const BETA: &[u8] = b"abcdef";
const LOG: &[u8] = b"hello log\n";

fn two() -> Vec<ArtifactDecl> {
    vec![
        decl("alpha.txt", MissingPolicy::Required),
        decl("reports/beta.bin", MissingPolicy::Required),
    ]
}

/// A result with both artifacts of [`two`] and a closed log.
fn full(fx: &Fx, a: &AttemptId) -> Out {
    let p = fx.w.payload(a);
    Out {
        outputs_evaluated: None,
        has_result: true,
        artifacts: vec![
            stored(a, &p, "alpha.txt", ALPHA),
            stored(a, &p, "reports/beta.bin", BETA),
        ],
        not_stored: vec![],
        log: LOG.to_vec(),
        stderr: vec![],
        log_closed: true,
        dropped: 0,
    }
}

async fn collect(c: &Coordinator, files: &Arc<ResultStore>) -> CollectReport {
    c.collect(files, cfg()).await.unwrap()
}

fn is_collected(c: &Coordinator, a: &AttemptId) -> bool {
    let rec = c.store().get_attempt(a).unwrap().unwrap();
    rec.receipts.reached(Receipt::Collected)
}

/// The published bytes of the artifact `name`: the result that was collected
/// says which digest the name has, and the file is stored under the digest.
fn published(files: &ResultStore, a: &AttemptId, name: &str) -> Option<Vec<u8>> {
    let result: GetResultResponse =
        serde_json::from_slice(&files.read_result(a).unwrap()?).unwrap();
    let record = result.artifacts.iter().find(|r| r.name == name)?;
    std::fs::read(files.artifact_path(a, &record.digest)).ok()
}

/// The names of the artifacts the parent recorded as verified and published.
fn persisted(c: &Coordinator, a: &AttemptId) -> Vec<String> {
    let all = c.store().persisted_artifacts(a).unwrap();
    all.into_iter().map(|(name, _)| name).collect()
}

fn hex(bytes: &[u8]) -> String {
    let digest = Digest::of_bytes(bytes);
    digest
        .as_str()
        .trim_start_matches(Digest::PREFIX)
        .to_string()
}

fn strikes(c: &Coordinator, a: &AttemptId) -> u32 {
    c.store().collection(a).unwrap().map_or(0, |r| r.strikes)
}

/// Change the parent's database behind its back.
fn sql(fx: &Fx, statement: &str) {
    let conn = rusqlite::Connection::open(fx.dir.path().join("coordinator.db")).unwrap();
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(conn.execute(statement, []).unwrap(), 1, "{statement}");
}

/// Everything of [`full`] is in the parent, verified, and the worker was
/// told so for the result it holds.
fn assert_all_here(fx: &Fx, c: &Coordinator, files: &ResultStore, a: &AttemptId) {
    assert_eq!(published(files, a, "alpha.txt").as_deref(), Some(ALPHA));
    assert_eq!(
        published(files, a, "reports/beta.bin").as_deref(),
        Some(BETA)
    );
    assert_eq!(files.read_log(a, LogStream::Stdout).unwrap(), LOG);
    assert!(files.read_result(a).unwrap().is_some());
    assert_eq!(
        persisted(c, a),
        vec!["alpha.txt".to_string(), "reports/beta.bin".to_string()]
    );
    // Durable as recorded: the logs with their length and digest.
    let logs = c.store().collection(a).unwrap().unwrap().logs.unwrap();
    assert_eq!(logs["stdout"]["sha256"], Digest::of_bytes(LOG).as_str());
    assert_eq!(logs["stderr"]["sha256"], Digest::of_bytes(b"").as_str());
    let dir = files.root().join(a.as_str()).join("artifacts");
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), 2);
    let row = c.store().collection(a).unwrap().unwrap();
    assert_eq!(row.verdict, Some(Verdict::Complete));
    assert!(row.acked_at_ms.is_some() && row.closed_at_ms.is_some());
    assert_eq!(row.result_digest, Some(fx.w.digest(a)));
    assert!(is_collected(c, a));
    assert!(fx.w.acks(a).iter().all(|d| d == &fx.w.digest(a)));
    assert!(!fx.w.acks(a).is_empty());
    // Nothing of an unfinished transfer is left behind.
    let tmp = files.root().join(a.as_str()).join("tmp");
    let left = std::fs::read_dir(tmp).map_or(0, |d| d.count());
    assert_eq!(left, 0);
}

// ------------------------------------------------------------ the path

#[tokio::test]
async fn the_outputs_are_fetched_verified_published_and_acknowledged_last() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, task, a) = placed(&fx, &c, two()).await;
    // While it runs there is nothing to collect.
    assert_eq!(collect(&c, &files).await, CollectReport::default());
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;

    // Ended on the worker and not collected: the run shows `collecting`.
    let rec = c.store().get_attempt(&a).unwrap().unwrap();
    let display = RunDisplay::derive(
        rec.state,
        rec.termination.as_ref(),
        rec.observation,
        Collection::Pending,
    );
    assert_eq!(display, RunDisplay::Collecting);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Collecting);
    assert_eq!(standing.tasks, vec![(task.clone(), TaskStanding::Pending)]);

    let report = collect(&c, &files).await;
    assert_eq!(report.collected, vec![a.clone()]);
    assert!(report.pending.is_empty() && report.gave_up.is_empty());
    assert_all_here(&fx, &c, &files, &a);

    // One read of the result; the bytes in chunks from where they were; the
    // acknowledgement once, after everything else.
    let ops: Vec<&str> = {
        let s = fx.w.state.lock().unwrap();
        let after = s.ops.iter().skip_while(|(k, _)| *k != "result");
        after.map(|(k, _)| *k).collect()
    };
    assert_eq!(ops.first(), Some(&"result"));
    assert_eq!(ops.last(), Some(&"ack"));
    assert_eq!(fx.w.count("result"), 1);
    assert_eq!(fx.w.count("ack"), 1);
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4]);
    assert_eq!(fx.w.offsets("log"), vec![0, 0, 0, 4, 8, 0]);
    let row = c.store().collection(&a).unwrap().unwrap();
    let logs = row.logs.unwrap();
    assert_eq!(logs["stdout"]["bytes"], 10);
    assert_eq!(logs["stdout"]["complete"], true);
    assert_eq!(logs["stdout"]["log_truncated"], false);
    assert_eq!(logs["stderr"]["bytes"], 0);

    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Passed);
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Succeeded)]);

    // A closed collection asks the worker for nothing more.
    let before = fx.w.state.lock().unwrap().ops.len();
    assert_eq!(collect(&c, &files).await, CollectReport::default());
    assert_eq!(fx.w.state.lock().unwrap().ops.len(), before);
    assert_eq!(fx.w.count("submit"), 1);
}

#[tokio::test]
async fn a30_a_worker_that_disappears_while_collecting_is_continued_where_it_stopped() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    // The second chunk of the first artifact never arrives.
    fx.w.set(|s| s.trip = Some(("blob", 2, Lost::Request)));

    for _ in 0..3 {
        let report = collect(&c, &files).await;
        assert_eq!(report.pending, vec![a.clone()]);
        assert!(report.collected.is_empty() && report.gave_up.is_empty());
    }
    // Collecting is kept; nothing partial is visible; nothing was given up.
    assert!(!is_collected(&c, &a));
    assert_eq!(published(&files, &a, "alpha.txt"), None);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.strikes, row.closed_at_ms), (0, None));
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(
        c.store().run_standing(&run, None).unwrap().verdict,
        RunVerdict::Collecting
    );
    // A tick in between changes nothing about the ended attempt.
    assert_eq!(c.tick().await.unwrap().placed, vec![]);

    fx.w.set(|s| s.offline = false);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    // The first artifact continued at byte 4; the result was read once and
    // the command ran once.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 4, 8, 0, 4]);
    assert_eq!(fx.w.count("result"), 1);
    assert_eq!(fx.w.count("submit"), 1);
    assert_eq!(fx.w.acks(&a).len(), 1);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundary {
    ResultFetched,
    SomeArtifactsPublished,
    AllPublishedNotAcked,
    AckedNotRecorded,
}

#[tokio::test]
async fn a_parent_that_crashes_at_any_boundary_continues_the_collection_and_runs_nothing() {
    use Boundary::*;
    for boundary in [
        ResultFetched,
        SomeArtifactsPublished,
        AllPublishedNotAcked,
        AckedNotRecorded,
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        let (_, _, a) = placed(&fx, &c, two()).await;
        end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
        let trip = match boundary {
            ResultFetched => ("log", 1, Lost::Request),
            // The first artifact takes three chunks.
            SomeArtifactsPublished => ("blob", 4, Lost::Request),
            AllPublishedNotAcked => ("ack", 1, Lost::Request),
            AckedNotRecorded => ("ack", 1, Lost::Answer),
        };
        fx.w.set(|s| s.trip = Some(trip));
        let report = collect(&c, &files).await;
        assert_eq!(report.pending, vec![a.clone()], "{boundary:?}");

        // What is durable at the boundary.
        let row = c.store().collection(&a).unwrap().unwrap();
        assert_eq!(row.result_digest, Some(fx.w.digest(&a)), "{boundary:?}");
        assert_eq!(row.closed_at_ms, None, "{boundary:?}");
        assert!(!is_collected(&c, &a), "{boundary:?}");
        let names = persisted(&c, &a);
        let acks = fx.w.acks(&a).len();
        match boundary {
            ResultFetched => {
                assert!(files.read_result(&a).unwrap().is_some());
                assert_eq!((names.len(), acks), (0, 0));
            }
            SomeArtifactsPublished => {
                assert_eq!(names, vec!["alpha.txt".to_string()]);
                assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
                assert_eq!(published(&files, &a, "reports/beta.bin"), None);
                assert_eq!(acks, 0);
            }
            AllPublishedNotAcked => assert_eq!((names.len(), acks), (2, 0)),
            AckedNotRecorded => assert_eq!((names.len(), acks), (2, 1)),
        }

        // The parent is gone; the next one finds the worker again.
        drop(c);
        drop(files);
        fx.w.set(|s| s.offline = false);
        let (c, files) = fx.open();
        assert_eq!(
            collect(&c, &files).await.collected,
            vec![a.clone()],
            "{boundary:?}"
        );
        assert_all_here(&fx, &c, &files, &a);
        // Only the collection was continued: the result is not read again,
        // no byte is fetched twice, and no command ran again.
        assert_eq!(fx.w.count("result"), 1, "{boundary:?}");
        // (The request the worker was lost on is the one that is repeated.)
        let blobs = match boundary {
            SomeArtifactsPublished => vec![0, 4, 8, 0, 0, 4],
            _ => vec![0, 4, 8, 0, 4],
        };
        assert_eq!(fx.w.offsets("blob"), blobs, "{boundary:?}");
        assert_eq!(fx.w.count("submit"), 1, "{boundary:?}");
        // An acknowledgement whose answer was lost is sent again, and the
        // worker takes the duplicate.
        let twice = boundary == AckedNotRecorded;
        assert_eq!(
            fx.w.acks(&a).len(),
            if twice { 2 } else { 1 },
            "{boundary:?}"
        );
        // And once more changes nothing.
        assert_eq!(collect(&c, &files).await, CollectReport::default());
    }
}

#[tokio::test]
async fn a_corrupted_chunk_is_never_published_and_the_artifact_is_fetched_again() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.corrupt_chunks = 1);

    let report = collect(&c, &files).await;
    assert_eq!(report.pending, vec![a.clone()]);
    // The bytes did not have the recorded digest: nothing is visible, the
    // worker was not told anything, and the fault is counted.
    assert_eq!(published(&files, &a, "alpha.txt"), None);
    assert!(persisted(&c, &a).is_empty());
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(c.store().collection(&a).unwrap().unwrap().strikes, 1);

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    // The whole artifact again, from its first byte.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4, 8, 0, 4]);
}

#[tokio::test]
async fn a_chunk_from_another_offset_is_not_written() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| {
        s.trip = Some(("blob", 2, Lost::Request));
    });
    collect(&c, &files).await;
    // Four bytes are here. The next chunk claims to be from byte 5.
    fx.w.set(|s| {
        s.offline = false;
        s.shift_chunk = true;
    });
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(c.store().collection(&a).unwrap().unwrap().strikes, 1);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    // Byte 4 was asked for again: what the bad chunk carried was dropped.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 4, 4, 8, 0, 4]);
}

#[tokio::test]
async fn bytes_that_never_have_their_digest_end_the_collection_and_run_nothing_again() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, task, a) = placed(&fx, &c, two()).await;
    let mut out = full(&fx, &a);
    // The worker serves other bytes than it recorded.
    out.artifacts[1].1 = b"ABCDEF".to_vec();
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    let report = collect(&c, &files).await;
    assert_eq!(report.gave_up, vec![a.clone()]);

    // The artifact that verified is kept; the other never appeared.
    assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
    assert_eq!(published(&files, &a, "reports/beta.bin"), None);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.verdict, row.strikes), (Some(Verdict::GaveUp), 3));
    assert_eq!(row.acked_at_ms, None);
    // The worker keeps what it has: it was not acknowledged.
    assert!(fx.w.acks(&a).is_empty());
    let rec = c.store().get_attempt(&a).unwrap().unwrap();
    assert!(rec.collection_failed_at_ms.is_some());
    assert!(!rec.receipts.reached(Receipt::Collected));
    // Not a success, and not a reason to run the command again.
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Failed);
    assert_eq!(
        standing.tasks,
        vec![(task.clone(), TaskStanding::CollectionFailed)]
    );
    assert_eq!(c.tick().await.unwrap().placed, vec![]);
    assert_eq!(c.store().attempts_of_task(&task).unwrap().len(), 1);
    assert_eq!(fx.w.count("submit"), 1);
    assert_eq!(collect(&c, &files).await, CollectReport::default());
}

// ------------------------------------------------------------ the order

#[tokio::test]
async fn a_result_that_is_stored_after_it_was_read_is_collected_before_it_is_acknowledged() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    // The attempt is terminal and has no result yet: its termination only,
    // and a spool that is not closed.
    let mut out = full(&fx, &a);
    out.has_result = false;
    out.log_closed = false;
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    let bare = fx.w.digest(&a);
    // The result is stored the moment the acknowledgement arrives.
    fx.w.set(|s| s.result_appears_on_ack = true);

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    // The result that was read first was never acknowledged; the one the
    // worker holds was collected whole and acknowledged.
    let real = fx.w.digest(&a);
    assert_ne!(bare, real);
    assert_eq!(fx.w.acks(&a), vec![real]);
    assert_eq!(fx.w.count("ack"), 2);
    assert_eq!(fx.w.count("result"), 2);
    assert_all_here(&fx, &c, &files, &a);
    let logs = c.store().collection(&a).unwrap().unwrap().logs.unwrap();
    assert_eq!(logs["stdout"]["complete"], true);
}

#[tokio::test]
async fn an_attempt_without_a_result_is_closed_with_what_there_is_and_is_no_success() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, task, a) = placed(&fx, &c, two()).await;
    let out = Out {
        log: LOG.to_vec(),
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Interrupted, out).await;

    assert_eq!(collect(&c, &files).await.required_missing, vec![a.clone()]);
    // The log is taken as far as it goes and marked incomplete.
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), LOG);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(row.logs.unwrap()["stdout"]["complete"], false);
    assert_eq!(row.verdict, Some(Verdict::RequiredMissing));
    assert_eq!(fx.w.acks(&a), vec![fx.w.digest(&a)]);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Failed);
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Failed)]);
}

#[tokio::test]
async fn the_logs_are_complete_only_at_their_end_mark_which_carries_the_truncation() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    // Terminal, with a result, and the end of the log is not there yet.
    let out = Out {
        has_result: true,
        log: LOG.to_vec(),
        log_closed: false,
        dropped: 7,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    for _ in 0..4 {
        assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    }
    // Every byte is here, and still nothing is called complete: no
    // acknowledgement, no strike, no closed collection.
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), LOG);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.logs, row.strikes, row.closed_at_ms), (None, 0, None));
    assert!(fx.w.acks(&a).is_empty());

    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().out.log_closed = true);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    let logs = c.store().collection(&a).unwrap().unwrap().logs.unwrap();
    assert_eq!(logs["stdout"]["complete"], true);
    assert_eq!(logs["stdout"]["log_truncated"], true);
    assert_eq!(logs["stdout"]["dropped_bytes"], 7);
    assert_eq!(logs["stdout"]["bytes"], 10);
    assert_eq!(fx.w.acks(&a).len(), 1);
}

// ------------------------------------------------------- required, optional

#[tokio::test]
async fn an_optional_artifact_that_is_missing_or_refused_changes_nothing_and_is_not_asked_for() {
    let fx = fx();
    let (c, files) = fx.open();
    let decls = vec![
        decl("alpha.txt", MissingPolicy::Required),
        decl("gone.txt", MissingPolicy::Optional),
        decl("secret.txt", MissingPolicy::Optional),
    ];
    let (run, task, a) = placed(&fx, &c, decls.clone()).await;
    let p = fx.w.payload(&a);
    let out = Out {
        has_result: true,
        artifacts: vec![stored(&a, &p, "alpha.txt", ALPHA)],
        not_stored: vec![
            not_stored(&decls[1], NotStoredReason::Missing),
            not_stored(&decls[2], NotStoredReason::Refused),
        ],
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
    assert_eq!(c.store().collection(&a).unwrap().unwrap().strikes, 0);
    // Only the stored artifact was fetched: three chunks, nothing else.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8]);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::Passed);
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Succeeded)]);
}

#[tokio::test]
async fn a_required_artifact_that_is_missing_or_refused_fails_the_task_once_and_for_all() {
    for reason in [NotStoredReason::Missing, NotStoredReason::Refused] {
        let fx = fx();
        let (c, files) = fx.open();
        let (run, task, a) = placed(&fx, &c, two()).await;
        let p = fx.w.payload(&a);
        let out = Out {
            has_result: true,
            artifacts: vec![stored(&a, &p, "alpha.txt", ALPHA)],
            not_stored: vec![not_stored(&two()[1], reason)],
            log_closed: true,
            ..Out::default()
        };
        end(&fx, &c, &a, Outcome::Succeeded, out).await;

        let report = collect(&c, &files).await;
        assert_eq!(report.required_missing, vec![a.clone()], "{reason:?}");
        assert!(report.collected.is_empty() && report.gave_up.is_empty());
        // What exists was collected and the worker was told so: there is
        // nothing more to wait for, and nothing is asked for again.
        assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
        assert_eq!(fx.w.acks(&a).len(), 1, "{reason:?}");
        let row = c.store().collection(&a).unwrap().unwrap();
        assert_eq!(row.verdict, Some(Verdict::RequiredMissing));
        assert_eq!(row.strikes, 0, "{reason:?}");
        // The command succeeded; the task did not.
        let rec = c.store().get_attempt(&a).unwrap().unwrap();
        assert!(!rec.receipts.reached(Receipt::Collected));
        assert!(rec.collection_failed_at_ms.is_some());
        let standing = c.store().run_standing(&run, None).unwrap();
        assert_eq!(standing.verdict, RunVerdict::Failed);
        assert_eq!(
            standing.tasks,
            vec![(task.clone(), TaskStanding::RequiredArtifactMissing)]
        );
        let ops = fx.w.state.lock().unwrap().ops.len();
        assert_eq!(collect(&c, &files).await, CollectReport::default());
        assert_eq!(fx.w.state.lock().unwrap().ops.len(), ops, "{reason:?}");
        assert_eq!(c.store().attempts_of_task(&task).unwrap().len(), 1);
    }
}

// ------------------------------------------------------- what is not trusted

#[tokio::test]
async fn a_result_that_names_a_path_outside_the_store_is_refused_whole() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    let p = fx.w.payload(&a);
    let mut out = full(&fx, &a);
    let (mut record, bytes) = stored(&a, &p, "alpha.txt", b"x");
    record.name = "../../escaped".into();
    record.path = record.name.clone();
    out.artifacts.push((record, bytes));
    end(&fx, &c, &a, Outcome::Succeeded, out).await;

    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    // Not a byte was asked for or written.
    assert_eq!(fx.w.count("blob"), 0);
    assert_eq!(fx.w.count("log"), 0);
    assert!(!fx.dir.path().join("escaped").exists());
    assert!(!files.root().join(a.as_str()).exists());
    assert!(fx.w.acks(&a).is_empty());
}

#[tokio::test]
async fn an_artifact_the_parent_did_not_declare_is_not_fetched() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![decl("alpha.txt", MissingPolicy::Required)]).await;
    let p = fx.w.payload(&a);
    let out = Out {
        has_result: true,
        artifacts: vec![
            stored(&a, &p, "alpha.txt", ALPHA),
            stored(&a, &p, "extra.bin", BETA),
        ],
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(published(&files, &a, "extra.bin"), None);
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8]);
}

#[tokio::test]
async fn a_result_of_another_termination_than_the_attempts_is_not_recorded() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    let out = Out {
        has_result: true,
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    // The worker now says the attempt failed.
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().termination = Some(ended(Outcome::Failed)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.result_digest, row.strikes), (None, 1));
    assert!(fx.w.acks(&a).is_empty());
}

// ------------------------------------------------------- without a worker

#[tokio::test]
async fn an_attempt_the_parent_settled_itself_is_closed_without_asking_a_worker() {
    let fx = fx();
    let (c, files) = fx.open();
    // The submit never arrives, and the worker is gone.
    fx.w.set(|s| s.trip = Some(("submit", 1, Lost::Request)));
    let (run, task) = (RunId::generate(), TaskId::generate());
    let source = SourceId::generate();
    c.store()
        .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    let optional = vec![decl("alpha.txt", MissingPolicy::Optional)];
    c.enqueue(run.clone(), source, vec![(task.clone(), spec(optional))])
        .await
        .unwrap();
    c.tick().await.unwrap();
    let a = c.store().attempts_of_task(&task).unwrap()[0]
        .attempt_id
        .clone();
    let seen = Seen {
        state: AttemptState::Terminal,
        termination: Some(ended(Outcome::Cancelled)),
        receipts: Receipts::default(),
    };
    c.store()
        .record_seen(&a, false, Some(seen), Observation::Known, wall_ms())
        .unwrap();

    let asked = fx.w.state.lock().unwrap().ops.len();
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(fx.w.state.lock().unwrap().ops.len(), asked);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(row.verdict, Some(Verdict::Complete));
    assert_eq!(row.acked_at_ms, None);
    let standing = c.store().run_standing(&run, None).unwrap();
    assert_eq!(standing.tasks, vec![(task, TaskStanding::Cancelled)]);
    assert_eq!(standing.verdict, RunVerdict::Failed);
}

// ------------------------------------------------------- A31

#[tokio::test]
async fn a31_an_empty_or_short_set_of_results_is_told_apart_and_never_a_success() {
    let fx = fx();
    let (c, files) = fx.open();
    // A run whose filter left nothing to do.
    let empty = RunId::generate();
    c.store()
        .create_run(&empty, None, None, &json!({}), 1)
        .unwrap();
    let standing = c.store().run_standing(&empty, None).unwrap();
    assert_eq!(standing.verdict, RunVerdict::NoWork);
    assert!(standing.tasks.is_empty());
    // The same run when a task was expected: its result is missing.
    assert_eq!(
        c.store().run_standing(&empty, Some(1)).unwrap().verdict,
        RunVerdict::Missing
    );

    let (run, _, a) = placed(&fx, &c, vec![]).await;
    let out = Out {
        has_result: true,
        log_closed: true,
        ..Out::default()
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a]);
    let s = c.store();
    assert_eq!(
        s.run_standing(&run, None).unwrap().verdict,
        RunVerdict::Passed
    );
    assert_eq!(
        s.run_standing(&run, Some(1)).unwrap().verdict,
        RunVerdict::Passed
    );
    // Two were expected and one is there: missing, whatever the one says.
    assert_eq!(
        s.run_standing(&run, Some(2)).unwrap().verdict,
        RunVerdict::Missing
    );
    assert!(matches!(
        s.run_standing(&RunId::generate(), None),
        Err(StorageError::NotFound { .. })
    ));
}

// ------------------------------------------------------- retention

#[tokio::test]
async fn collected_results_are_kept_for_the_retention_and_then_deleted() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    // A second attempt whose collection is cut off after its result.
    let (_, _, b) = placed(&fx, &c, two()).await;
    // Collected a moment after it ended.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    end(&fx, &c, &b, Outcome::Succeeded, full(&fx, &b)).await;
    let next_log = fx.w.count("log") + 1;
    fx.w.set(|s| s.trip = Some(("log", next_log, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![b.clone()]);

    // The retention runs from the later of the end and the collection.
    let terminal = c.store().get_attempt(&a).unwrap().unwrap();
    let terminal = terminal.terminal_at_ms.unwrap();
    let ended = c.store().collection(&a).unwrap().unwrap();
    let ended = ended.closed_at_ms.unwrap();
    assert!(ended > terminal);
    let report = sweep(c.store(), &files, ended + 7 * DAY - 1, 7).unwrap();
    assert!(report.deleted.is_empty());
    assert_all_here(&fx, &c, &files, &a);

    let report = sweep(c.store(), &files, ended + 7 * DAY, 7).unwrap();
    let mut deleted = report.deleted.clone();
    deleted.sort();
    assert_eq!(
        deleted,
        vec![
            format!("results/{a}/artifacts/{}", hex(ALPHA)),
            format!("results/{a}/artifacts/{}", hex(BETA)),
            format!("results/{a}/logs"),
            format!("results/{a}/result.json"),
        ]
    );
    assert!(!files.root().join(a.as_str()).exists());
    assert!(c.store().pending_deletes().unwrap().is_empty());
    // What still waits for its collection is not touched, however old.
    assert!(files.read_result(&b).unwrap().is_some());
    // The verdict outlives the files.
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(row.verdict, Some(Verdict::Complete));
    // A sweep with nothing to do does nothing.
    let again = sweep(c.store(), &files, ended + 31 * DAY, 7).unwrap();
    assert!(again.deleted.is_empty());

    fx.w.set(|s| s.offline = false);
    assert_eq!(collect(&c, &files).await.collected, vec![b.clone()]);
    assert_all_here(&fx, &c, &files, &b);
}

// ------------------------------------------- nothing is acknowledged early

#[tokio::test]
async fn a_parent_that_dies_between_a_file_operation_and_its_record_continues_from_the_files() {
    use belt::results::store::Crash;
    for point in [
        Crash::ResultWritten,
        Crash::LogAppended,
        Crash::ChunkAppended,
        Crash::ArtifactPublished,
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        let (_, _, a) = placed(&fx, &c, two()).await;
        end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
        files.crash_once_at(point);
        let report = collect(&c, &files).await;
        assert_eq!(report.pending, vec![a.clone()], "{point:?}");
        assert_eq!(report.faults.len(), 1, "{point:?}");
        assert_eq!(report.faults[0].0, a, "{point:?}");
        // The file operation happened, its record did not, and the worker
        // was told nothing.
        assert!(fx.w.acks(&a).is_empty(), "{point:?}");
        assert!(!is_collected(&c, &a), "{point:?}");
        assert_eq!(strikes(&c, &a), 0, "{point:?}");
        match point {
            Crash::ResultWritten => {
                assert!(files.read_result(&a).unwrap().is_some());
                assert_eq!(c.store().collection(&a).unwrap(), None);
            }
            Crash::ArtifactPublished => {
                assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
                assert!(persisted(&c, &a).is_empty());
            }
            _ => assert!(persisted(&c, &a).is_empty()),
        }

        drop(c);
        drop(files);
        let (c, files) = fx.open();
        assert_eq!(
            collect(&c, &files).await.collected,
            vec![a.clone()],
            "{point:?}"
        );
        assert_all_here(&fx, &c, &files, &a);
        assert_eq!(fx.w.acks(&a).len(), 1, "{point:?}");
        assert_eq!(fx.w.count("submit"), 1, "{point:?}");
        if point == Crash::ArtifactPublished {
            // The file that was published is verified again, not fetched.
            assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4], "{point:?}");
        }
    }
}

#[tokio::test]
async fn missing_result_and_artifacts_are_collected_again_before_the_acknowledgement() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(persisted(&c, &a).len(), 2);

    // Everything is recorded as collected; then the disk loses the result
    // and one artifact, and damages the other artifact.
    let dir = files.root().join(a.as_str());
    std::fs::remove_file(dir.join("result.json")).unwrap();
    std::fs::remove_file(dir.join("artifacts").join(hex(ALPHA))).unwrap();
    std::fs::write(dir.join("artifacts").join(hex(BETA)), b"abcdeX").unwrap();
    fx.w.set(|s| s.offline = false);
    let (blobs, logs) = (fx.w.count("blob"), fx.w.count("log"));

    // The acknowledgement is not sent for what is not there: it is fetched
    // again first.
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    assert_eq!(fx.w.count("blob"), blobs + 5);
    assert_eq!(fx.w.count("log"), logs);
    assert_eq!(fx.w.count("result"), 1);
    assert_eq!(fx.w.acks(&a).len(), 1);
    assert_eq!(strikes(&c, &a), 0);
    let events = c.store().events_of(&a, "collection_recollect").unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["logs"], false);
    assert_eq!(
        events[0].payload["artifacts"],
        json!(["alpha.txt", "reports/beta.bin"])
    );
}

#[tokio::test]
async fn an_attempt_whose_payload_cannot_be_read_is_never_acknowledged_and_keeps_nothing_else_back()
{
    for broken in [
        "json_remove(demand_json, '$.payload')",
        "json_set(demand_json, '$.payload', 'junk')",
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        // The older attempt is the broken one: it comes first in every pass.
        let (_, _, a) = placed(&fx, &c, two()).await;
        end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
        let (_, _, b) = placed(&fx, &c, two()).await;
        end(&fx, &c, &b, Outcome::Succeeded, full(&fx, &b)).await;
        sql(
            &fx,
            &format!("UPDATE placement SET demand_json = {broken} WHERE attempt_id = '{a}'"),
        );
        for pass in 0..4 {
            let report = collect(&c, &files).await;
            assert_eq!(report.pending, vec![a.clone()], "{broken}");
            assert_eq!(report.faults.len(), 1, "{broken}");
            assert!(report.faults[0].1.contains("payload"), "{report:?}");
            let others = if pass == 0 { vec![b.clone()] } else { vec![] };
            assert_eq!(report.collected, others, "{broken}");
        }
        assert_all_here(&fx, &c, &files, &b);
        // "Nothing was declared" would have released the worker's
        // artifacts unfetched: nothing was asked, nothing was told.
        assert!(fx.w.acks(&a).is_empty(), "{broken}");
        assert_eq!(c.store().collection(&a).unwrap(), None, "{broken}");
        assert!(!files.root().join(a.as_str()).exists(), "{broken}");
        assert!(!is_collected(&c, &a), "{broken}");
    }
}

#[tokio::test]
async fn an_error_inside_the_worker_is_waited_out_however_often_it_comes() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    for (kind, code) in [
        ("result", ErrorCode::Internal),
        ("log", ErrorCode::Internal),
        ("blob", ErrorCode::Internal),
        ("blob", ErrorCode::InvalidInput),
        ("blob", ErrorCode::Conflict),
        // A supervisor of a result-less attempt may still store its result.
        ("ack", ErrorCode::Overloaded),
        ("ack", ErrorCode::Internal),
    ] {
        fx.w.set(|s| s.fail = Some((kind, code, 4)));
        for _ in 0..4 {
            let report = collect(&c, &files).await;
            assert_eq!(report.pending, vec![a.clone()], "{kind} {code:?}");
            assert!(report.faults.is_empty(), "{kind} {code:?}");
        }
        assert_eq!(strikes(&c, &a), 0, "{kind} {code:?}");
        assert!(fx.w.acks(&a).is_empty(), "{kind} {code:?}");
        let row = c.store().collection(&a).unwrap();
        assert!(row.is_none_or(|r| r.closed_at_ms.is_none()));
    }
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
}

#[tokio::test]
async fn a_worker_that_answered_with_an_error_is_given_time_and_one_that_was_away_is_not() {
    let fx = fx();
    let (c, files) = fx.open();
    let slow = CollectConfig {
        backoff_ms: 60_000,
        ..cfg()
    };
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    let ops = |fx: &Fx| fx.w.state.lock().unwrap().ops.len();
    // Away: asked again by the very next pass.
    fx.w.set(|s| s.trip = Some(("blob", 2, Lost::Request)));
    assert_eq!(
        c.collect(&files, slow).await.unwrap().pending,
        vec![a.clone()]
    );
    fx.w.set(|s| {
        s.offline = false;
        s.fail = Some(("blob", ErrorCode::Internal, 1));
    });
    let before = ops(&fx);
    assert_eq!(
        c.collect(&files, slow).await.unwrap().pending,
        vec![a.clone()]
    );
    assert!(ops(&fx) > before);
    // It answered with an error: the next pass leaves it alone.
    let before = ops(&fx);
    assert_eq!(
        c.collect(&files, slow).await.unwrap().pending,
        vec![a.clone()]
    );
    assert_eq!(ops(&fx), before);
    // The wait is not durable: a parent that starts again asks at once.
    drop(c);
    drop(files);
    let (c, files) = fx.open();
    assert_eq!(
        c.collect(&files, slow).await.unwrap().collected,
        vec![a.clone()]
    );
    assert_all_here(&fx, &c, &files, &a);
}

#[tokio::test]
async fn strikes_start_over_with_verified_progress_and_a_worker_that_forgot_closes_the_collection()
{
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.shift_chunk = true);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(strikes(&c, &a), 1);
    // An artifact is verified and published: what went wrong before it is
    // not held against the rest.
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Answer)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(strikes(&c, &a), 0);
    assert_eq!(fx.w.acks(&a).len(), 1);

    // The worker took the acknowledgement, released, and has deleted the
    // attempt by the time the acknowledgement is sent again.
    for code in [ErrorCode::NotFound, ErrorCode::Retired] {
        let (c, files, a) = if code == ErrorCode::NotFound {
            (&c, &files, a.clone())
        } else {
            let (_, _, b) = placed(&fx, &c, two()).await;
            end(&fx, &c, &b, Outcome::Succeeded, full(&fx, &b)).await;
            (&c, &files, b)
        };
        fx.w.set(|s| {
            s.offline = false;
            s.fail = Some(("ack", code, 1));
        });
        assert_eq!(collect(c, files).await.collected, vec![a.clone()]);
        let row = c.store().collection(&a).unwrap().unwrap();
        assert_eq!(row.verdict, Some(Verdict::Complete), "{code:?}");
        assert!(row.closed_at_ms.is_some() && row.acked_at_ms.is_none());
        assert_eq!(row.strikes, 0, "{code:?}");
        assert!(is_collected(c, &a), "{code:?}");
        assert_eq!(published(files, &a, "alpha.txt").as_deref(), Some(ALPHA));
        assert_eq!(collect(c, files).await, CollectReport::default());
    }
}

#[tokio::test]
async fn a_part_a_dead_parent_left_torn_is_fetched_again_and_is_no_strike() {
    use belt::results::store::Crash;
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    files.crash_once_at(Crash::ChunkAppended);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    let part = files.root().join(a.as_str()).join("tmp");
    let part = part.join(format!("{}.part", hex(ALPHA)));
    assert_eq!(std::fs::read(&part).unwrap(), &ALPHA[..4]);
    drop(c);
    drop(files);
    // The power went with the parent: the tail of the file is not what was
    // written.
    std::fs::write(&part, b"01\0\0").unwrap();

    let (c, files) = fx.open();
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    assert_eq!(strikes(&c, &a), 0);
    assert!(c
        .store()
        .events_of(&a, "collection_fault")
        .unwrap()
        .is_empty());
    // Continued at its length, found wrong when whole, fetched once more.
    assert_eq!(fx.w.offsets("blob"), vec![0, 4, 8, 0, 4, 8, 0, 4]);
}

#[tokio::test]
async fn a_collection_that_was_given_up_is_opened_again_while_the_worker_has_the_result() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, a) = placed(&fx, &c, two()).await;
    let mut out = full(&fx, &a);
    out.artifacts[1].1 = b"ABCDEF".to_vec();
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    // An open collection is not one to reopen.
    assert!(!c.store().reopen_collection(&a, 1).unwrap());
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert!(!c.store().reopen_collection(&a, 1).unwrap());
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    // Nothing of the transfers that never finished is left.
    assert!(!files.root().join(a.as_str()).join("tmp").exists());
    assert!(!c
        .store()
        .reopen_collection(&AttemptId::generate(), 1)
        .unwrap());

    // The worker was not acknowledged, so it still has the result (for its
    // `ack_wait_ms`); now it serves the bytes it recorded.
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().out.artifacts[1].1 = BETA.to_vec());
    assert!(c.store().reopen_collection(&a, wall_ms()).unwrap());
    assert!(!c.store().reopen_collection(&a, wall_ms()).unwrap());
    let rec = c.store().get_attempt(&a).unwrap().unwrap();
    assert_eq!(rec.collection_failed_at_ms, None);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!(
        (
            row.closed_at_ms,
            row.verdict,
            row.strikes,
            row.result_digest
        ),
        (None, None, 0, None)
    );
    assert!(persisted(&c, &a).is_empty());
    assert_eq!(
        c.store().run_standing(&run, None).unwrap().verdict,
        RunVerdict::Collecting
    );
    // The retention of what is here does not run while it is open.
    let report = sweep(c.store(), &files, wall_ms() + 30 * DAY, 7).unwrap();
    assert!(report.deleted.is_empty());

    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_all_here(&fx, &c, &files, &a);
    assert_eq!(fx.w.count("result"), 2);
    assert_eq!(
        c.store().run_standing(&run, None).unwrap().verdict,
        RunVerdict::Passed
    );
    // What the worker acknowledged is closed for good.
    assert!(!c.store().reopen_collection(&a, wall_ms()).unwrap());
    assert_eq!(
        c.store()
            .events_of(&a, "collection_reopened")
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn names_that_are_one_path_on_the_parent_are_each_collected() {
    let fx = fx();
    let (c, files) = fx.open();
    // The same name but for its case, a name that is a directory of
    // another, and two names with the same bytes: fine on the worker.
    let all: [(&str, &[u8]); 6] = [
        ("Report.txt", b"upper"),
        ("report.txt", b"lower"),
        ("a", b"a file"),
        ("a/b", b"below a"),
        ("same-1", b"same"),
        ("same-2", b"same"),
    ];
    let decls = all.iter().map(|(n, _)| decl(n, MissingPolicy::Required));
    let (_, _, a) = placed(&fx, &c, decls.collect()).await;
    let p = fx.w.payload(&a);
    let out = Out {
        outputs_evaluated: None,
        has_result: true,
        artifacts: all.iter().map(|(n, b)| stored(&a, &p, n, b)).collect(),
        not_stored: vec![],
        log: LOG.to_vec(),
        stderr: vec![],
        log_closed: true,
        dropped: 0,
    };
    end(&fx, &c, &a, Outcome::Succeeded, out).await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    for (name, bytes) in all {
        assert_eq!(
            published(&files, &a, name).as_deref(),
            Some(bytes),
            "{name}"
        );
    }
    assert_eq!(persisted(&c, &a).len(), 6);
    assert!(is_collected(&c, &a));
    // One file per content, none under a name of the worker.
    let dir = files.root().join(a.as_str()).join("artifacts");
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut want: Vec<String> = ["upper", "lower", "a file", "below a", "same"]
        .iter()
        .map(|b| hex(b.as_bytes()))
        .collect();
    want.sort();
    assert_eq!(names, want);
    // And all of it goes with the retention.
    let closed = c.store().collection(&a).unwrap().unwrap();
    let closed = closed.closed_at_ms.unwrap();
    let report = sweep(c.store(), &files, closed + 7 * DAY, 7).unwrap();
    assert_eq!(report.deleted.len(), 7);
    assert!(!files.root().join(a.as_str()).exists());
    assert!(c.store().pending_deletes().unwrap().is_empty());
}

#[tokio::test]
async fn a_worker_that_no_longer_has_the_blob_ends_the_collection_after_three_answers() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    // Asking again does not bring back what the worker deleted.
    fx.w.set(|s| s.fail = Some(("blob", ErrorCode::NotFound, 3)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    let row = c.store().collection(&a).unwrap().unwrap();
    assert_eq!((row.verdict, row.strikes), (Some(Verdict::GaveUp), 3));
    assert!(fx.w.acks(&a).is_empty());
    assert!(persisted(&c, &a).is_empty());
    assert_eq!(fx.w.count("blob"), 3);
}

#[tokio::test]
async fn a_name_that_is_published_is_not_fetched_over_with_other_bytes() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, two()).await;
    end(&fx, &c, &a, Outcome::Succeeded, full(&fx, &a)).await;
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    // The receipt says `alpha.txt` was published with another digest than
    // the result records: an artifact never changes, so this is no reason
    // to fetch, and no state to acknowledge.
    let other = Digest::of_bytes(b"other");
    sql(
        &fx,
        &format!(
            "UPDATE artifact_receipt SET digest = '{}' WHERE attempt_id = '{a}' AND name = 'alpha.txt'",
            other.as_str()
        ),
    );
    fx.w.set(|s| s.offline = false);
    let blobs = fx.w.count("blob");
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(strikes(&c, &a), 1);
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    assert_eq!(collect(&c, &files).await.gave_up, vec![a.clone()]);
    assert_eq!(fx.w.count("blob"), blobs);
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(published(&files, &a, "alpha.txt").as_deref(), Some(ALPHA));
    assert!(!files.artifact_path(&a, &other).exists());
}

// ------------------------------------------------ durable active prefixes

#[tokio::test]
async fn gc_pin_contention_skips_live_poll_without_stopping_coordinator() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, a) = placed(&fx, &c, vec![]).await;
    fx.w.set(|s| {
        let job = s.jobs.get_mut(&a).unwrap();
        job.state = AttemptState::Active;
        job.out.log = b"live".to_vec();
    });
    c.tick().await.unwrap();
    let guard = crate::test_tmp::gc_guard(c.store(), &run);
    let report = c.collect(&files, cfg()).await.unwrap();
    assert!(report.pending.contains(&a));
    assert!(report.faults.is_empty());
    // A fresh coordinator tick and metadata request remain usable under GC.
    c.tick().await.unwrap();
    assert_eq!(
        c.store().get_attempt(&a).unwrap().unwrap().state,
        AttemptState::Active
    );
    assert!(c
        .store()
        .log_progress(&a, LogStream::Stdout)
        .unwrap()
        .is_none());
    drop(guard);
    let report = c.collect(&files, cfg()).await.unwrap();
    assert!(report.faults.is_empty());
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), b"live");
}

#[tokio::test]
async fn gc_pin_contention_leaves_terminal_collection_pending_and_corruption_errors() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, a) = placed(&fx, &c, vec![]).await;
    end(
        &fx,
        &c,
        &a,
        Outcome::Succeeded,
        Out {
            has_result: true,
            log_closed: true,
            ..Out::default()
        },
    )
    .await;
    let guard = crate::test_tmp::gc_guard(c.store(), &run);
    let report = c.collect(&files, cfg()).await.unwrap();
    assert!(report.pending.contains(&a));
    assert!(report.faults.is_empty());
    drop(guard);
    assert_eq!(collect(&c, &files).await.collected, vec![a]);

    // A malformed pin is an I/O failure, not ordinary GC contention.
    let (run, _, a) = placed(&fx, &c, vec![]).await;
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().state = AttemptState::Active);
    c.tick().await.unwrap();
    drop(crate::test_tmp::gc_guard(c.store(), &run));
    let pin = fx.dir.path().join(format!("reader-pins/{run}.lock"));
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&pin, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(c.collect(&files, cfg()).await.is_err());
    std::fs::set_permissions(&pin, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[tokio::test]
async fn active_raw_logs_are_durable_before_terminal_and_reconnect_at_committed_offset() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    let raw = b"a\xff\x00bcdef";
    fx.w.set(|s| {
        let job = s.jobs.get_mut(&a).unwrap();
        job.state = AttemptState::Active;
        job.out.log = raw.to_vec();
        job.out.stderr = b"\xfe\xff".to_vec();
    });
    c.tick().await.unwrap();
    assert_eq!(
        c.store().get_attempt(&a).unwrap().unwrap().state,
        AttemptState::Active
    );
    collect(&c, &files).await;
    assert_eq!(files.read_log(&a, LogStream::Stderr).unwrap(), b"\xfe\xff");
    let p = c
        .store()
        .log_progress(&a, LogStream::Stdout)
        .unwrap()
        .unwrap();
    assert_eq!(p.committed_bytes, 4);
    assert!(!p.eof);
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), &raw[..4]);
    assert!(c.store().collection(&a).unwrap().is_none());
    assert!(!is_collected(&c, &a));
    assert!(fx.w.acks(&a).is_empty());
    // The same public observer used by CLI follow sees active committed bytes
    // before termination, and keeps waiting rather than treating file EOF as done.
    let task = c.store().get_attempt(&a).unwrap().unwrap().task_id;
    let mut observer = belt::service::logs::Observer::new(
        belt::service::query::Reader::Offline(Arc::new(
            CoordinatorStore::open_read_only(fx.dir.path()).unwrap(),
        )),
        fx.dir.path().join("results"),
        belt::service::completion::Target::Task(task),
        None,
    )
    .unwrap();
    let (mut visible, mut diagnostics) = (Vec::new(), Vec::new());
    assert!(tokio::time::timeout(
        Duration::from_millis(300),
        observer.observe(true, false, &mut visible, &mut diagnostics)
    )
    .await
    .is_err());
    assert_eq!(visible, &raw[..4]);
    // Reopening uses the DB prefix, not the physical length, including a
    // durable append that never reached its paired DB commit.
    files
        .append_log(&a, LogStream::Stdout, 4, b"uncommitted")
        .unwrap();
    drop(c);
    let (c, files) = fx.open();
    fx.w.set(|s| {
        s.offline = true;
        s.ops.clear();
    });
    collect(&c, &files).await;
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), &raw[..4]);
    fx.w.set(|s| s.offline = false);
    collect(&c, &files).await;
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), raw);
    assert!(fx.w.state.lock().unwrap().ops.contains(&("log", 4)));
    // Empty without an end mark remains active, including repeated polls.
    for _ in 0..3 {
        collect(&c, &files).await;
    }
    assert!(
        !c.store()
            .log_progress(&a, LogStream::Stdout)
            .unwrap()
            .unwrap()
            .eof
    );
    end(
        &fx,
        &c,
        &a,
        Outcome::Succeeded,
        Out {
            has_result: true,
            log: raw.to_vec(),
            stderr: b"\xfe\xff".to_vec(),
            log_closed: true,
            ..Out::default()
        },
    )
    .await;
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), raw);
    assert_eq!(fx.w.acks(&a).len(), 1);
    let summary = tokio::time::timeout(
        Duration::from_secs(5),
        observer.observe(true, false, &mut visible, &mut diagnostics),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(visible, raw);
    assert_eq!(summary.pending_tasks, 0);
    assert_eq!(summary.failed_tasks, 0);
}

#[tokio::test]
async fn a_short_published_active_prefix_is_an_error_and_is_never_reset() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().out.log = b"abcdef".to_vec());
    collect(&c, &files).await;
    std::fs::write(files.log_path(&a, LogStream::Stdout), b"a").unwrap();
    fx.w.set(|s| s.ops.clear());
    let report = collect(&c, &files).await;
    assert!(report.faults.iter().any(|(id, _)| id == &a));
    assert_eq!(
        c.store()
            .log_progress(&a, LogStream::Stdout)
            .unwrap()
            .unwrap()
            .committed_bytes,
        4
    );
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), b"a");
    assert!(!fx.w.state.lock().unwrap().ops.contains(&("log", 4)));
}

#[tokio::test]
async fn active_log_response_with_malformed_offset_publishes_no_bytes() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    fx.w.set(|s| {
        s.jobs.get_mut(&a).unwrap().out.log = b"abcdef".to_vec();
        s.shift_log = true;
    });
    let report = collect(&c, &files).await;
    assert!(report
        .faults
        .iter()
        .any(|(id, why)| id == &a && why.contains("offset")));
    assert_eq!(
        c.store()
            .log_progress(&a, LogStream::Stdout)
            .unwrap()
            .unwrap()
            .committed_bytes,
        0
    );
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), b"");
    collect(&c, &files).await;
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), b"abcd");
}

#[tokio::test]
async fn published_log_digest_corruption_fails_closed_before_ack() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    end(
        &fx,
        &c,
        &a,
        Outcome::Succeeded,
        Out {
            has_result: true,
            log: LOG.to_vec(),
            log_closed: true,
            ..Out::default()
        },
    )
    .await;
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    std::fs::write(files.log_path(&a, LogStream::Stdout), b"HELLO log\n").unwrap();
    fx.w.set(|s| s.offline = false);
    let report = collect(&c, &files).await;
    assert!(report
        .faults
        .iter()
        .any(|(id, why)| id == &a && why.contains("published log prefix")));
    assert!(fx.w.acks(&a).is_empty());
    assert_eq!(
        files.read_log(&a, LogStream::Stdout).unwrap(),
        b"HELLO log\n"
    );
    assert_eq!(
        c.store()
            .log_progress(&a, LogStream::Stdout)
            .unwrap()
            .unwrap()
            .committed_bytes,
        LOG.len() as u64
    );
}

#[tokio::test]
async fn active_pages_above_64_and_hot_logs_do_not_starve_terminal_artifacts() {
    let fx = fx();
    let (c, files) = fx.open();
    let (run, _, first) = placed(&fx, &c, two()).await;
    let base = fx.w.payload(&first);
    let mut attempts = vec![first];
    for _ in 1..70 {
        let task = TaskId::generate();
        let a = AttemptId::generate();
        let mut payload = base.clone();
        payload.task_id = task.clone();
        let db = c.store();
        db.create_task(&task, &run, &json!({}), 1).unwrap();
        db.create_attempt(
            belt::storage::NewAttempt {
                attempt_id: a.clone(),
                task_id: task.clone(),
                worker_id: fx.w.id.clone(),
                payload_hash: payload_hash(&payload).unwrap(),
                permit_id: None,
            },
            1,
        )
        .unwrap();
        let placement = db
            .record_placement(&task, &fx.w.id, Some(&a), &json!({"payload": payload}), 1)
            .unwrap();
        db.ack_placement(placement, 1).unwrap();
        let mut receipts = Receipts::default();
        receipts.mark(Receipt::WorkerAccepted, 1);
        db.record_seen(
            &a,
            true,
            Some(Seen {
                state: AttemptState::Active,
                termination: None,
                receipts,
            }),
            Observation::Known,
            1,
        )
        .unwrap();
        fx.w.set(|s| {
            s.jobs.insert(
                a.clone(),
                Job {
                    payload,
                    state: AttemptState::Active,
                    termination: None,
                    out: Out::default(),
                    acks: vec![],
                },
            );
        });
        attempts.push(a);
    }
    fx.w.set(|s| {
        for job in s.jobs.values_mut() {
            job.out.log = vec![0xff; 140_000];
        }
    });
    let wide = CollectConfig {
        chunk_bytes: u32::MAX,
        scan: u32::MAX,
        ..cfg()
    };
    let progress = || {
        attempts
            .iter()
            .map(|a| {
                c.store()
                    .log_progress(a, LogStream::Stdout)
                    .unwrap()
                    .map_or(0, |p| p.committed_bytes)
            })
            .collect::<Vec<_>>()
    };
    // The live half of the wall budget can stop a page before its 64th
    // target, especially when durable writes run under instrumentation.
    // Follow the cursor across bounded passes instead of requiring a full page.
    let mut before = progress();
    for pass in 0..attempts.len() * 2 {
        let report = c.collect(&files, wide).await.unwrap();
        assert!(report.faults.is_empty(), "{:?}", report.faults);
        let after = progress();
        let changed = before.iter().zip(&after).filter(|(a, b)| a != b).count();
        assert!(changed > 0, "live collection stalled on pass {pass}");
        assert!(changed <= 64, "one pass advanced {changed} targets");
        for (old, new) in before.iter().zip(&after) {
            assert!(*new >= *old && *new - *old <= 65_536);
        }
        if pass == 0 {
            assert!(after.iter().filter(|bytes| **bytes > 0).count() <= 64);
            assert!(after.contains(&0));
        }
        before = after;
        if before.iter().all(|bytes| *bytes > 0) {
            break;
        }
    }
    assert!(before.iter().all(|bytes| *bytes > 0), "{before:?}");
    for (a, bytes) in attempts.iter().zip(&before) {
        let stored = c
            .store()
            .log_progress(a, LogStream::Stdout)
            .unwrap()
            .unwrap();
        assert!(!stored.eof);
        assert_eq!(
            files.read_log(a, LogStream::Stdout).unwrap(),
            vec![0xff; *bytes as usize]
        );
    }
    // One target turns terminal while the others remain hot. The independent
    // final quota still fetches its artifact and ACKs in the same pass.
    let terminal = attempts[0].clone();
    let mut out = full(&fx, &terminal);
    out.log = vec![0xff; 140_000];
    end(&fx, &c, &terminal, Outcome::Succeeded, out).await;
    let report = c.collect(&files, wide).await.unwrap();
    assert_eq!(report.collected, vec![terminal.clone()]);
    assert_eq!(fx.w.acks(&terminal).len(), 1);
    assert_eq!(
        published(&files, &terminal, "alpha.txt").as_deref(),
        Some(ALPHA)
    );
    assert_eq!(
        published(&files, &terminal, "reports/beta.bin").as_deref(),
        Some(BETA)
    );
    assert!(attempts.iter().zip(&before).any(|(a, old)| a != &terminal
        && c.store()
            .log_progress(a, LogStream::Stdout)
            .unwrap()
            .unwrap()
            .committed_bytes
            > *old));
}

#[tokio::test]
async fn reaching_the_parent_log_cap_does_not_fabricate_an_end_mark() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    fx.w.set(|s| s.jobs.get_mut(&a).unwrap().out.log = b"abcdef".to_vec());
    let cap = CollectConfig {
        max_log_bytes: 4,
        ..cfg()
    };
    c.collect(&files, cap).await.unwrap();
    c.collect(&files, cap).await.unwrap();
    let progress = c
        .store()
        .log_progress(&a, LogStream::Stdout)
        .unwrap()
        .unwrap();
    assert_eq!(progress.committed_bytes, 4);
    assert!(!progress.eof);
    assert!(c.store().collection(&a).unwrap().is_none());
    assert!(fx.w.acks(&a).is_empty());
}

#[tokio::test]
async fn unpublished_legacy_final_log_corruption_can_be_refetched_before_ack() {
    let fx = fx();
    let (c, files) = fx.open();
    let (_, _, a) = placed(&fx, &c, vec![]).await;
    end(
        &fx,
        &c,
        &a,
        Outcome::Succeeded,
        Out {
            has_result: true,
            log: LOG.to_vec(),
            log_closed: true,
            ..Out::default()
        },
    )
    .await;
    fx.w.set(|s| s.trip = Some(("ack", 1, Lost::Request)));
    assert_eq!(collect(&c, &files).await.pending, vec![a.clone()]);
    // Simulate a pre-migration final receipt: no prefix was ever published.
    sql(
        &fx,
        &format!(
            "DELETE FROM collection_log WHERE attempt_id='{}' AND stream='stdout'",
            a
        ),
    );
    sql(
        &fx,
        &format!(
            "DELETE FROM collection_log WHERE attempt_id='{}' AND stream='stderr'",
            a
        ),
    );
    std::fs::write(files.log_path(&a, LogStream::Stdout), b"HELLO log\n").unwrap();
    fx.w.set(|s| s.offline = false);
    assert_eq!(collect(&c, &files).await.collected, vec![a.clone()]);
    assert_eq!(files.read_log(&a, LogStream::Stdout).unwrap(), LOG);
    assert_eq!(fx.w.acks(&a).len(), 1);
    assert_eq!(
        c.store()
            .log_progress(&a, LogStream::Stdout)
            .unwrap()
            .unwrap()
            .committed_bytes,
        LOG.len() as u64
    );
}

struct DelayedLog {
    inner: Arc<Worker>,
    old: AttemptId,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
impl WorkerLink for DelayedLog {
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            if matches!(&request, Request::ReadLog(r) if r.attempt_id == self.old && r.stream == LogStream::Stdout && r.offset == 0)
            {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            self.inner.call(request).await
        })
    }
}

#[tokio::test]
async fn retirement_and_replacement_during_log_rpc_never_mix_attempt_bytes() {
    let fx = fx();
    let (coordinator, files) = fx.open();
    let c = Arc::new(coordinator);
    let (_, task, old) = placed(&fx, &c, vec![]).await;
    fx.w.set(|s| s.jobs.get_mut(&old).unwrap().out.log = b"old!".to_vec());
    let delayed = Arc::new(DelayedLog {
        inner: fx.w.clone(),
        old: old.clone(),
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    c.attach(fx.w.id.clone(), delayed.clone()).await;
    let (collect_c, collect_files) = (c.clone(), files.clone());
    let pass = tokio::spawn(async move { collect(&collect_c, &collect_files).await });
    tokio::time::timeout(Duration::from_secs(2), delayed.entered.notified())
        .await
        .unwrap();
    let db = c.store();
    db.begin_retire(&old, "replace", 2).unwrap();
    db.confirm_retire(&old, 3).unwrap();
    let new = AttemptId::generate();
    let record = db
        .create_attempt(
            belt::storage::NewAttempt {
                attempt_id: new.clone(),
                task_id: task.clone(),
                worker_id: fx.w.id.clone(),
                payload_hash: Digest::of_bytes(b"replacement"),
                permit_id: None,
            },
            4,
        )
        .unwrap();
    let mut payload = fx.w.payload(&old);
    payload.generation = record.generation;
    let placement = db
        .record_placement(&task, &fx.w.id, Some(&new), &json!({"payload":payload}), 4)
        .unwrap();
    db.ack_placement(placement, 4).unwrap();
    let mut receipts = Receipts::default();
    receipts.mark(Receipt::WorkerAccepted, 4);
    db.record_seen(
        &new,
        true,
        Some(Seen {
            state: AttemptState::Active,
            termination: None,
            receipts,
        }),
        Observation::Known,
        4,
    )
    .unwrap();
    fx.w.set(|s| {
        s.jobs.insert(
            new.clone(),
            Job {
                payload,
                state: AttemptState::Active,
                termination: None,
                out: Out {
                    log: b"NEW!".to_vec(),
                    ..Out::default()
                },
                acks: vec![],
            },
        );
    });
    delayed.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(3), pass)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(files.read_log(&old, LogStream::Stdout).unwrap(), b"old!");
    assert!(db.log_progress(&old, LogStream::Stderr).unwrap().is_none());
    assert!(db.log_progress(&new, LogStream::Stdout).unwrap().is_none());
    collect(&c, &files).await;
    assert_eq!(files.read_log(&new, LogStream::Stdout).unwrap(), b"NEW!");
    assert_eq!(files.read_log(&old, LogStream::Stdout).unwrap(), b"old!");
}

#[tokio::test]
async fn generic_bundle_encoding_conditions_and_exact_receipt_are_enforced() {
    use belt::supervisor::bundles::store_bundle;
    use belt::supervisor::outcome::Forbidden;
    use belt::supervisor::spec::ArtifactsSpec;
    for (when, outcome, required, include, wrong_encoding, drop_marker) in [
        (
            OutputWhen::Success,
            Outcome::Succeeded,
            true,
            true,
            false,
            false,
        ),
        (
            OutputWhen::Failure,
            Outcome::Failed,
            true,
            true,
            false,
            false,
        ),
        (
            OutputWhen::Failure,
            Outcome::Succeeded,
            true,
            false,
            false,
            false,
        ),
        (
            OutputWhen::Always,
            Outcome::Succeeded,
            false,
            false,
            false,
            false,
        ),
        (
            OutputWhen::Always,
            Outcome::Succeeded,
            true,
            false,
            false,
            false,
        ),
        (
            OutputWhen::Always,
            Outcome::Succeeded,
            true,
            true,
            true,
            false,
        ),
        (
            OutputWhen::Always,
            Outcome::Succeeded,
            true,
            false,
            false,
            true,
        ),
        (
            OutputWhen::Always,
            Outcome::Cancelled,
            true,
            false,
            false,
            true,
        ),
        (
            OutputWhen::Always,
            Outcome::TimedOut,
            true,
            false,
            false,
            true,
        ),
    ] {
        let fx = fx();
        let (c, files) = fx.open();
        let (run, task) = (RunId::generate(), TaskId::generate());
        let source = SourceId::generate();
        c.store()
            .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
            .unwrap();
        let d = OutputDecl {
            name: "bundle".into(),
            paths: vec!["reports/**".into(), "coverage/**".into(), "reports".into()],
            if_missing: if required {
                MissingPolicy::Required
            } else {
                MissingPolicy::Optional
            },
            when,
        };
        let mut s = spec(vec![]);
        s.outputs = vec![d.clone()];
        c.enqueue(run, source, vec![(task, s)]).await.unwrap();
        let a = c.tick().await.unwrap().placed[0].clone();
        let p = fx.w.payload(&a);
        let ws = fx.dir.path().join("ws");
        std::fs::create_dir_all(ws.join("reports")).unwrap();
        std::fs::create_dir_all(ws.join("coverage")).unwrap();
        std::fs::write(ws.join("reports/a"), ALPHA).unwrap();
        std::fs::write(ws.join("coverage/b"), BETA).unwrap();
        let plan = ArtifactsSpec {
            workspace: ws,
            source_id: p.source_id.clone(),
            plan_digest: p.plan_digest.clone(),
            profile_fingerprint: Digest::of_bytes(b"profile"),
            max_bytes: 1 << 20,
            files: vec![],
            outputs: vec![d.clone()],
            secret_files: vec![],
            secret_paths: vec![],
        };
        let mut records = vec![];
        let mut bytes = vec![];
        if include {
            let mut remaining = 16384;
            let mut record = store_bundle(
                &fx.dir.path().join("spool"),
                &a,
                &plan,
                &d,
                &Forbidden::nothing(),
                &mut remaining,
            )
            .unwrap();
            bytes = std::fs::read(
                fx.dir
                    .path()
                    .join("spool")
                    .join(a.as_str())
                    .join("artifacts/bundle"),
            )
            .unwrap();
            if wrong_encoding || (drop_marker && outcome == Outcome::Succeeded) {
                record.encoding = ArtifactEncoding::Raw;
            }
            records.push((record, bytes.clone()));
        }
        end(
            &fx,
            &c,
            &a,
            outcome,
            Out {
                outputs_evaluated: if drop_marker { Some(false) } else { None },
                has_result: true,
                artifacts: records,
                log_closed: true,
                ..Out::default()
            },
        )
        .await;
        let mut config = cfg();
        config.chunk_bytes = 64 << 10;
        let report = c.collect(&files, config).await.unwrap();
        if wrong_encoding || (drop_marker && outcome == Outcome::Succeeded) {
            assert!(report.collected.is_empty());
            assert!(persisted(&c, &a).is_empty());
            assert!(fx.w.acks(&a).is_empty());
            continue;
        }
        if !include && required && belt::outputs::eligible(when, outcome, !drop_marker) {
            assert_eq!(report.required_missing, vec![a.clone()]);
        } else {
            assert_eq!(report.collected, vec![a.clone()]);
        }
        let collection = c.store().collection(&a).unwrap().unwrap();
        if include {
            assert_eq!(published(&files, &a, "bundle"), Some(bytes));
            assert_eq!(persisted(&c, &a), vec!["bundle"]);
            let result: serde_json::Value =
                serde_json::from_slice(&files.read_result(&a).unwrap().unwrap()).unwrap();
            assert_eq!(result["artifacts"][0]["encoding"], "tar_v1");
        } else if required && belt::outputs::eligible(when, outcome, !drop_marker) {
            assert_eq!(collection.verdict, Some(Verdict::RequiredMissing));
        }
        if drop_marker {
            assert!(c
                .store()
                .get_attempt(&a)
                .unwrap()
                .unwrap()
                .receipts
                .reached(Receipt::CommandRegistered));
        }
        assert!(!fx.w.acks(&a).is_empty());
    }
}

#[tokio::test]
async fn measured_terminal_tail_records_real_chunked_complete_collection_after_live_progress() {
    use belt::scheduler::{
        measured::{collection_policy, CostQuery, MeasuredCosts},
        CostModel,
    };
    let f = fx();
    let model = Arc::new(MeasuredCosts::open(&f.dir.path().join("stats"), cfg()).unwrap());
    let (c, files) = f.open_with(model.clone());
    let (_, _, a) = placed(&f, &c, two()).await;
    f.w.set(|s| s.jobs.get_mut(&a).unwrap().out.log = b"hello ".to_vec());
    collect(&c, &files).await;
    assert!(c
        .store()
        .log_progress(&a, LogStream::Stdout)
        .unwrap()
        .is_some_and(|p| p.committed_bytes > 0));
    end(&f, &c, &a, Outcome::Succeeded, full(&f, &a)).await;
    for _ in 0..20 {
        if is_collected(&c, &a) {
            break;
        }
        collect(&c, &files).await;
    }
    assert!(is_collected(&c, &a));
    assert_eq!(published(&files, &a, "alpha.txt"), Some(ALPHA.to_vec()));
    model.drain();
    let payload = f.w.payload(&a);
    let mut query = CostQuery::task(
        &serde_json::to_value(&payload).unwrap(),
        Some(payload.source_id.as_str()),
        f.w.cost_transport(),
    );
    query.collection_policy = Some(collection_policy(cfg()));
    assert!(model.query(&f.w.id, &query).ms[COLLECTION_PART].is_some());
    assert_eq!(model.query(&f.w.id, &query).ms[1], None);
    // A closed receipt is never timed again, nor does a lookup refresh it.
    let db = rusqlite::Connection::open(f.dir.path().join("stats/costs-v1.sqlite")).unwrap();
    let before: (i64, i64) = db
        .query_row("SELECT COUNT(*),SUM(observed) FROM costs", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    for _ in 0..100 {
        let _ = model.query(&f.w.id, &query);
    }
    collect(&c, &files).await;
    let after: (i64, i64) = db
        .query_row("SELECT COUNT(*),SUM(observed) FROM costs", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(before, after);
    drop(c);
    let restarted = Arc::new(MeasuredCosts::open(&f.dir.path().join("stats"), cfg()).unwrap());
    let (c, files) = f.open_with(restarted.clone());
    for _ in 0..3 {
        collect(&c, &files).await;
    }
    assert!(is_collected(&c, &a));
    restarted.drain();
    let after_restart: (i64, i64) = db
        .query_row("SELECT COUNT(*),SUM(observed) FROM costs", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(before, after_restart);
}
const COLLECTION_PART: usize = 2;

#[tokio::test]
async fn measured_terminal_tail_fault_then_recovery_does_not_publish_partial_duration() {
    use belt::scheduler::measured::MeasuredCosts;
    let f = fx();
    let model = Arc::new(MeasuredCosts::open(&f.dir.path().join("stats"), cfg()).unwrap());
    let (c, files) = f.open_with(model.clone());
    let (_, _, a) = placed(&f, &c, two()).await;
    end(&f, &c, &a, Outcome::Succeeded, full(&f, &a)).await;
    f.w.set(|s| s.fail = Some(("result", ErrorCode::NotFound, 1)));
    let failed = collect(&c, &files).await;
    assert!(failed.pending.contains(&a));
    assert!(!is_collected(&c, &a));
    assert_eq!(
        f.w.state.lock().unwrap().fail,
        Some(("result", ErrorCode::NotFound, 0))
    );
    for _ in 0..20 {
        if is_collected(&c, &a) {
            break;
        }
        collect(&c, &files).await;
    }
    assert!(is_collected(&c, &a));
    model.drain();
    let db = rusqlite::Connection::open(f.dir.path().join("stats/costs-v1.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM costs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn measured_terminal_tail_durable_close_failure_keeps_sample_unknown_after_recovery() {
    use belt::scheduler::measured::MeasuredCosts;
    let f = fx();
    let model = Arc::new(MeasuredCosts::open(&f.dir.path().join("stats"), cfg()).unwrap());
    let (c, files) = f.open_with(model.clone());
    let (_, _, a) = placed(&f, &c, two()).await;
    end(&f, &c, &a, Outcome::Succeeded, full(&f, &a)).await;
    // Reject only the final durable closure, after all actual result IO and
    // worker acknowledgement. Recovery must not invent a whole-tail sample.
    let db = rusqlite::Connection::open(f.dir.path().join("coordinator.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fixture_deny_close BEFORE UPDATE ON collection WHEN NEW.closed_at_ms IS NOT NULL BEGIN SELECT RAISE(ABORT,'fixture durable close failure'); END;").unwrap();
    collect(&c, &files).await;
    assert!(!is_collected(&c, &a));
    db.execute_batch("DROP TRIGGER fixture_deny_close;")
        .unwrap();
    for _ in 0..20 {
        if is_collected(&c, &a) {
            break;
        }
        collect(&c, &files).await;
    }
    assert!(is_collected(&c, &a));
    model.drain();
    let db = rusqlite::Connection::open(f.dir.path().join("stats/costs-v1.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM costs", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

// ------------------------------------------------------------- CI tasks

use belt::supervisor::ci_task::RESULT_ARTIFACT;

/// The frozen plan of a workflow, made from a capture of its own (in a
/// directory of its own: the parent of these tests holds no source).
fn ci_plan(workflow: &str) -> local_ci::FrozenPlan {
    let dir = crate::test_tmp::tempdir().unwrap();
    ci_plan_at(workflow, dir.path(), &RunId::generate())
}

fn ci_plan_at(workflow: &str, state_dir: &std::path::Path, run: &RunId) -> local_ci::FrozenPlan {
    use belt::source::{capture_for_run, CaptureParams, SourceRequest, SourceStore};
    use local_ci::frozen::Filters;
    let dir = crate::test_tmp::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(project.join(".github/workflows")).unwrap();
    std::fs::write(project.join(".github/workflows/ci.yml"), workflow).unwrap();
    let env: BTreeMap<String, String> = [
        ("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"),
        ("HOME", home.to_str().unwrap()),
        ("GIT_CONFIG_GLOBAL", "/dev/null"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let store = SourceStore::open(state_dir).unwrap();
    let captured = capture_for_run(
        &env,
        &store,
        run,
        &SourceRequest::Directory { dir: project },
        &CaptureParams::default(),
    )
    .unwrap();
    let manifest = store
        .manifest(&captured.manifest.ci_source_id)
        .unwrap()
        .unwrap();
    let options = local_ci::PinnedPlanOptions {
        workflow_dirs_override: None,
        filters: Filters {
            all: true,
            include_disabled: false,
            workflows_dirs: vec![],
            workflows: vec![],
            jobs: vec![],
        },
        disabled: vec![],
        base_branch: None,
        changed_files: None,
    };
    belt::ci::execute::freeze_plan(&store, &manifest, &options).unwrap()
}

const CI_CHAIN: &str = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    outputs:\n      version: ${{ steps.v.outputs.version }}\n    steps:\n      - id: v\n        run: echo \"version=1.2.3\" >> \"$GITHUB_OUTPUT\"\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo one\n      - run: echo two\n";

/// The id of the task of `plan` that runs `job`.
fn ci_task_of(plan: &local_ci::FrozenPlan, job: &str) -> String {
    let task = plan.tasks.iter().find(|t| t.jobs.iter().any(|j| j == job));
    task.unwrap().id.clone()
}

/// The result of a task of one job, as its worker keeps it.
fn ci_result(task: &str, job: &str, status: &str, outputs: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1, "task": task, "jobs": [], "receipt_ignored": null, "artifacts": [],
        "outcomes": [{
            "job": job, "variant": null, "status": status, "outputs": outputs,
            "skipped": null, "ancestor_failed": false,
        }],
    }))
    .unwrap()
}

/// Queue the tasks of `plan` as one run, as `ci` does. The task ids, by the
/// job each runs.
async fn ci_run(
    fx: &Fx,
    c: &Coordinator,
    plan: &local_ci::FrozenPlan,
) -> (RunId, BTreeMap<String, TaskId>) {
    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), plan).unwrap();
    let manifest = Digest::of_bytes(b"manifest");
    let none = Default::default();
    let specs = belt::service::ci_run::task_specs(plan, &digest, &manifest, &none, None).unwrap();
    let (run, source) = (RunId::generate(), SourceId::generate());
    c.store().put_source_ref(&source, &manifest, 1).unwrap();
    let mut ids = BTreeMap::new();
    let mut tasks = Vec::new();
    for (plan_task, spec) in specs {
        let id = TaskId::generate();
        let task = plan.tasks.iter().find(|t| t.id == plan_task).unwrap();
        ids.insert(task.jobs[0].clone(), id.clone());
        tasks.push((id, spec));
    }
    c.enqueue(run.clone(), source, tasks).await.unwrap();
    (run, ids)
}

fn ci_advance(fx: &Fx, c: &Coordinator) -> belt::ci::advance::Pass {
    let results = fx.dir.path().join("results");
    belt::ci::advance::advance_runs(c.store(), fx.dir.path(), &results, wall_ms()).unwrap()
}

/// Collect until the collection of `a` is closed (a pass has a budget, and
/// the result of a task is many chunks here). What the passes reported.
async fn ci_collect(c: &Coordinator, files: &Arc<ResultStore>, a: &AttemptId) -> CollectReport {
    let mut all = CollectReport::default();
    for _ in 0..50 {
        let report = collect(c, files).await;
        all.collected.extend(report.collected);
        all.required_missing.extend(report.required_missing);
        all.gave_up.extend(report.gave_up);
        let row = c.store().collection(a).unwrap();
        if row.is_some_and(|row| row.verdict.is_some()) {
            break;
        }
    }
    all
}

fn ci_out(a: &AttemptId, p: &AttemptPayload, result: &[u8]) -> Out {
    Out {
        has_result: true,
        artifacts: vec![stored(a, p, RESULT_ARTIFACT, result)],
        log_closed: true,
        ..Out::default()
    }
}

/// A38, with the workers in memory: the job that needs another is not
/// placed before the other's result is on the parent, it then starts on
/// another worker with the outputs, and neither task is ever run twice. A
/// worker that did not negotiate `ci_task_v1` is given no CI task, and the
/// placement says why it was left out. A10: the forecast is asked for every
/// step of the task.
#[tokio::test]
async fn a38_a_job_that_needs_another_starts_elsewhere_once_the_result_is_here_and_runs_once() {
    let fx = fx();
    let (c, files) = fx.open();
    let other = Worker::new("w-b");
    other.set(|s| s.no_ci = true);
    c.try_attach(other.id.clone(), Arc::clone(&other) as Arc<dyn WorkerLink>)
        .unwrap();
    let plan = ci_plan(CI_CHAIN);
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    let (build, test) = (ids["build"].clone(), ids["test"].clone());
    let attempts = |task: &TaskId| c.store().attempts_of_task(task).unwrap();

    // Only the task that needs nothing is placed, and only where CI tasks
    // are taken.
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    let a = report.placed[0].clone();
    assert_eq!(attempts(&build)[0].attempt_id, a);
    assert_eq!(other.count("submit"), 0);
    let decision = belt::scheduler::reason_of(c.store(), &a).unwrap().unwrap();
    let left_out = decision.excluded.iter().find(|e| e.worker_id == other.id);
    assert_eq!(left_out.unwrap().unmet, ["protocol feature ci_task_v1"]);
    let demand = c.store().placement_demand(&a).unwrap().unwrap();
    assert_eq!(demand["ci_steps"], 1);
    // While build runs nothing is released, and test is not placed.
    assert!(ci_advance(&fx, &c).released.is_empty());
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert!(attempts(&test).is_empty());

    // build ends. Its result is on the parent only after the collection.
    let p = fx.w.payload(&a);
    assert_eq!(
        p.ci_task.as_ref().unwrap().task_id,
        ci_task_of(&plan, "build")
    );
    assert!(p.commands.is_empty());
    let outputs = serde_json::json!({"version": "1.2.3"});
    let result = ci_result(&ci_task_of(&plan, "build"), "build", "success", outputs);
    end(&fx, &c, &a, Outcome::Succeeded, ci_out(&a, &p, &result)).await;
    assert!(
        ci_advance(&fx, &c).released.is_empty(),
        "not before it is collected"
    );
    assert!(c.tick().await.unwrap().placed.is_empty());
    let report = ci_collect(&c, &files, &a).await;
    assert_eq!(report.collected, vec![a.clone()], "{report:?}");
    let pass = ci_advance(&fx, &c);
    assert_eq!(pass.released, vec![test.clone()]);
    assert_eq!(pass.plans.len(), 1);
    assert!(pass.faults.is_empty() && pass.unrunnable.is_empty());
    assert!(ci_advance(&fx, &c).released.is_empty(), "released once");

    // The worker that ran build is away; the other one takes CI tasks now.
    fx.w.set(|s| s.offline = true);
    other.set(|s| s.no_ci = false);
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    let b = report.placed[0].clone();
    assert_eq!(attempts(&test)[0].attempt_id, b);
    let sent = other.payload(&b);
    let ci = sent.ci_task.clone().unwrap();
    assert_eq!(ci.task_id, ci_task_of(&plan, "test"));
    assert_eq!(ci.inputs["needs"]["build"]["status"], "success");
    assert_eq!(ci.inputs["needs"]["build"]["outputs"]["version"], "1.2.3");
    let demand = c.store().placement_demand(&b).unwrap().unwrap();
    assert_eq!(demand["ci_steps"], 2, "both steps of test are asked for");

    // Whatever is looked at again, neither task gets a second attempt.
    for _ in 0..3 {
        assert!(ci_advance(&fx, &c).released.is_empty());
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert_eq!(attempts(&build).len(), 1);
    assert_eq!(attempts(&test).len(), 1);

    // test ends on the other worker and the run is settled as a success.
    let result = ci_result(
        &ci_task_of(&plan, "test"),
        "test",
        "success",
        serde_json::json!({}),
    );
    other.finish(&b, Outcome::Succeeded, ci_out(&b, &sent, &result));
    assert_eq!(c.tick().await.unwrap().settled, vec![b.clone()]);
    assert_eq!(ci_collect(&c, &files, &b).await.collected, vec![b]);
    let target = belt::service::completion::Target::Run(run.clone());
    assert_eq!(c.store().completion(target).unwrap().exit_code(), Some(0));
    let lines =
        belt::service::ci_run::job_lines(c.store(), fx.dir.path(), files.root(), &run).unwrap();
    let seen: Vec<_> = lines
        .iter()
        .map(|l| (l.job.as_deref(), l.status.as_str()))
        .collect();
    assert_eq!(
        seen,
        [(Some("build"), "success"), (Some("test"), "success")]
    );
}

/// The result of a CI task is required by the parent although no payload
/// can declare it. A worker that leaves none, or one that cannot be read
/// as the result of that task, does not make the task a success; the job
/// that needs it is given a failure. A worker that returns only this
/// artifact is collected.
#[tokio::test]
async fn a_ci_task_without_a_usable_result_is_no_success_and_the_next_job_sees_a_failure() {
    for unreadable in [false, true] {
        let fx = fx();
        let (c, files) = fx.open();
        let plan = ci_plan(CI_CHAIN);
        let (run, ids) = ci_run(&fx, &c, &plan).await;
        let a = c.tick().await.unwrap().placed[0].clone();
        let p = fx.w.payload(&a);
        // The command "succeeded" on the worker.
        let out = if unreadable {
            // The result of another task under the reserved name.
            let other = ci_result("another task", "build", "success", serde_json::json!({}));
            ci_out(&a, &p, &other)
        } else {
            Out {
                has_result: true,
                log_closed: true,
                ..Out::default()
            }
        };
        end(&fx, &c, &a, Outcome::Succeeded, out).await;
        let report = ci_collect(&c, &files, &a).await;
        assert_eq!(report.required_missing, vec![a.clone()], "{unreadable}");
        assert!(report.collected.is_empty());
        let rec = c.store().get_attempt(&a).unwrap().unwrap();
        assert!(rec.collection_failed_at_ms.is_some(), "{unreadable}");
        assert!(!rec.receipts.reached(Receipt::Collected));

        // The job that needs it is told that it failed; whether it runs is
        // for the core on the worker to say.
        assert_eq!(ci_advance(&fx, &c).released, vec![ids["test"].clone()]);
        let b = c.tick().await.unwrap().placed[0].clone();
        let sent = fx.w.payload(&b);
        let inputs = &sent.ci_task.as_ref().unwrap().inputs;
        assert_eq!(
            inputs["needs"]["build"]["status"], "failure",
            "{unreadable}"
        );
        assert_eq!(inputs["needs"]["build"]["outputs"], serde_json::json!({}));

        // It comes back skipped, with nothing but its result: collected.
        let mut skipped: serde_json::Value = serde_json::from_slice(&ci_result(
            &ci_task_of(&plan, "test"),
            "test",
            "skipped",
            serde_json::json!({}),
        ))
        .unwrap();
        skipped["outcomes"][0]["skipped"] = "needs failed".into();
        let result = serde_json::to_vec(&skipped).unwrap();
        end(&fx, &c, &b, Outcome::Succeeded, ci_out(&b, &sent, &result)).await;
        assert_eq!(ci_collect(&c, &files, &b).await.collected, vec![b]);
        let target = belt::service::completion::Target::Run(run.clone());
        let exit = c.store().completion(target).unwrap().exit_code();
        assert!(exit.is_some_and(|code| code != 0), "{exit:?}");
        // No job failed: a result that is lost is an error of the run.
        let status = ci_computed(&fx, &c, &files, &run);
        assert_eq!(status, Computed::Error, "{unreadable}");
        let lines =
            belt::service::ci_run::job_lines(c.store(), fx.dir.path(), files.root(), &run).unwrap();
        let test = lines
            .iter()
            .find(|l| l.job.as_deref() == Some("test"))
            .unwrap();
        assert_eq!(test.status, "skipped");
        assert_eq!(test.skipped.as_deref(), Some("needs failed"));
        if !unreadable {
            assert!(lines
                .iter()
                .any(|l| l.job.is_none() && l.status == "no_result"));
        }
    }
}

/// A cancel of the run reaches the tasks that wait: they are never
/// released, and the run settles as cancelled.
#[tokio::test]
async fn a_cancelled_ci_run_releases_nothing_more_and_settles() {
    let fx = fx();
    let (c, files) = fx.open();
    let plan = ci_plan(CI_CHAIN);
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    let a = c.tick().await.unwrap().placed[0].clone();
    c.cancel_run(run.clone()).await.unwrap();
    let p = fx.w.payload(&a);
    let result = ci_result(
        &ci_task_of(&plan, "build"),
        "build",
        "success",
        serde_json::json!({}),
    );
    fx.w.finish(&a, Outcome::Cancelled, ci_out(&a, &p, &result));
    for _ in 0..3 {
        c.tick().await.unwrap();
        collect(&c, &files).await;
        assert!(ci_advance(&fx, &c).released.is_empty());
    }
    assert!(c.store().attempts_of_task(&ids["test"]).unwrap().is_empty());
    assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());
    let target = belt::service::completion::Target::Run(run.clone());
    let snapshot = c.store().completion(target).unwrap();
    assert!(snapshot.settled());
    assert!(snapshot.exit_code().is_some_and(|code| code != 0));
    assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Cancelled);
    // A released spec that still waits is never written.
    let waiting = serde_json::json!({"ci": {"hold": true}});
    assert!(c
        .store()
        .release_ci_task(&ids["test"], &waiting, 1)
        .is_err());
    assert!(!c
        .store()
        .release_ci_task(&ids["test"], &serde_json::json!({}), 1)
        .unwrap());
}

/// A run whose frozen plan the parent does not have any more does not wait
/// for it: the job that was not placed yet is cancelled with the reason,
/// the job that runs ends, and the run settles as no success.
#[tokio::test]
async fn a_ci_run_whose_plan_is_gone_cancels_what_waits_and_says_why() {
    let fx = fx();
    let (c, files) = fx.open();
    let plan = ci_plan(CI_CHAIN);
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    let (build, test) = (ids["build"].clone(), ids["test"].clone());
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    let a = report.placed[0].clone();
    assert!(ci_advance(&fx, &c).failed.is_empty());

    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), &plan).unwrap();
    std::fs::remove_file(belt::worker::ci_task::plan_path(fx.dir.path(), &digest)).unwrap();
    let pass = ci_advance(&fx, &c);
    assert_eq!(pass.failed.len(), 1, "{pass:?}");
    assert_eq!(pass.failed[0].0, run);
    let why = pass.failed[0].1.clone();
    assert_eq!(pass.unrunnable, vec![(test.clone(), why.clone())]);
    assert!(pass.faults.is_empty() && pass.released.is_empty());
    let events: Vec<_> = c
        .store()
        .events_after(0, 1000)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "ci_task_unrunnable")
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].run_id.as_deref(), Some(run.as_str()));
    assert_eq!(events[0].payload["task"], test.as_str());
    assert_eq!(events[0].payload["reason"], why.as_str());
    let shown = c.store().query_task(test.clone()).unwrap().unwrap();
    assert_eq!(shown.unrunnable.as_deref(), Some(why.as_str()));
    assert!(shown.cancel_requested_at_ms.is_some());

    // The job that runs is not touched, and ends as it would have.
    assert!(c
        .store()
        .query_task(build.clone())
        .unwrap()
        .unwrap()
        .cancel_requested_at_ms
        .is_none());
    let p = fx.w.payload(&a);
    let result = ci_result(
        &ci_task_of(&plan, "build"),
        "build",
        "success",
        serde_json::json!({"version": "1"}),
    );
    end(&fx, &c, &a, Outcome::Succeeded, ci_out(&a, &p, &result)).await;
    assert_eq!(ci_collect(&c, &files, &a).await.collected, vec![a]);
    // Nothing is looked at, released or placed any more.
    for _ in 0..2 {
        assert_eq!(ci_advance(&fx, &c), Default::default());
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    assert!(c.store().attempts_of_task(&test).unwrap().is_empty());
    let target = belt::service::completion::Target::Run(run.clone());
    let snapshot = c.store().completion(target).unwrap();
    assert!(snapshot.settled());
    let lines =
        belt::service::ci_run::job_lines(c.store(), fx.dir.path(), files.root(), &run).unwrap();
    let seen: Vec<_> = lines
        .iter()
        .map(|l| (l.job.as_deref(), l.status.as_str(), l.reason.as_deref()))
        .collect();
    assert_eq!(
        seen,
        [
            (Some("build"), "success", None),
            (None, "no_result", Some(why.as_str()))
        ]
    );
}

/// A worker that did not take the frozen plan of a run is left out of the
/// placement of its tasks by name: the wait has a reason where the task is
/// shown, another worker takes the task, and the exclusion is over when the
/// worker takes the plan.
#[tokio::test]
async fn a_worker_that_did_not_take_the_plan_is_left_out_with_the_reason_until_it_does() {
    use belt::scheduler::decide::Exclusion;
    const WHY: &str = "frozen plan not taken: the worker holds 256 frozen plans (OVERLOADED)";
    let fx = fx();
    let (c, _files) = fx.open();
    fx.w.set(|s| s.refused_plan = Some(WHY.into()));
    let plan = ci_plan(CI_CHAIN);
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    // Before the worker was asked to take the plan nothing is known, and
    // nothing is said.
    assert!(c.tick().await.unwrap().placed.is_empty());
    fx.w.set(|s| s.refused_runs.push(run.clone()));
    let build = ids["build"].clone();
    let waits = || -> Vec<belt::storage::Event> {
        let events = c.store().events_after(0, 1000).unwrap();
        events
            .into_iter()
            .filter(|e| e.kind == belt::scheduler::EVENT_TASK_WAITING)
            .collect()
    };

    // The only worker did not take the plan: nothing is placed, and the run
    // says why, once.
    for _ in 0..3 {
        assert!(c.tick().await.unwrap().placed.is_empty());
    }
    let recorded = waits();
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].run_id.as_deref(), Some(run.as_str()));
    assert_eq!(recorded[0].payload["task"], build.as_str());
    assert!(recorded[0].payload["blocked_reason"].is_null());
    let excluded = &recorded[0].payload["decision"]["excluded"];
    assert_eq!(excluded.as_array().unwrap().len(), 1);
    assert_eq!(excluded[0]["worker_id"], "w-a");
    assert_eq!(excluded[0]["why"], "input_refused");
    assert_eq!(excluded[0]["unmet"], serde_json::json!([WHY]));
    let shown = c.store().query_task(build.clone()).unwrap().unwrap();
    let placement = shown.placement.unwrap();
    assert_eq!(placement.blocked_reason, None);
    assert_eq!(placement.excluded.len(), 1);
    assert_eq!(placement.excluded[0].why, Exclusion::InputRefused);
    assert_eq!(placement.excluded[0].unmet, [WHY]);

    // Another worker takes it; the placement keeps who was left out and why.
    let other = Worker::new("w-b");
    c.try_attach(other.id.clone(), Arc::clone(&other) as Arc<dyn WorkerLink>)
        .unwrap();
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    let a = report.placed[0].clone();
    assert_eq!(other.count("submit"), 1);
    assert_eq!(fx.w.count("submit"), 0);
    let decision = belt::scheduler::reason_of(c.store(), &a).unwrap().unwrap();
    let left_out: Vec<_> = decision
        .excluded
        .iter()
        .filter(|e| e.worker_id == fx.w.id)
        .collect();
    assert_eq!(left_out.len(), 1);
    assert_eq!(left_out[0].why, Exclusion::InputRefused);
    assert_eq!(left_out[0].unmet, [WHY]);

    // The other worker is away. The next run waits again, until the first
    // worker has taken the plan: then it is a candidate like any other.
    other.set(|s| s.offline = true);
    let (run_2, ids_2) = ci_run(&fx, &c, &plan).await;
    fx.w.set(|s| s.refused_runs.push(run_2.clone()));
    assert!(c.tick().await.unwrap().placed.is_empty());
    fx.w.set(|s| s.refused_plan = None);
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    let b = report.placed[0].clone();
    let attempts = c.store().attempts_of_task(&ids_2["build"]).unwrap();
    assert_eq!(attempts[0].attempt_id, b);
    assert_eq!(fx.w.count("submit"), 1);
    let decision = belt::scheduler::reason_of(c.store(), &b).unwrap().unwrap();
    assert!(decision
        .excluded
        .iter()
        .all(|e| e.why != Exclusion::InputRefused));
}

// ---- S18: the shards of a split job (A39 to A41) ----

use local_ci::dag::{NotSplit, ShardGroup, ShardRequest};
use local_ci::shard::adapters::{CargoTest, Jest, Safety, Vitest};
use local_ci::shard::{
    partition_tests, Basis, Discovered, History, PartitionOpts, RunnerAdapter, ShardPlan,
};

fn split_workflow(run: &str) -> String {
    format!(
        "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: {run}\n  report:\n    needs: test\n    if: always()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo report\n"
    )
}

fn listed(tests: &[&str]) -> Discovered {
    Discovered {
        tests: tests.iter().map(|t| t.to_string()).collect(),
        complete: true,
        blocked: vec![],
    }
}

/// A request to split job `test` of `plan` as `shards` divides what was
/// listed.
fn split_request(
    plan: &local_ci::FrozenPlan,
    shards: ShardPlan,
    discovered: &Discovered,
    fail_fast: bool,
) -> ShardRequest {
    let task = plan.tasks.iter().find(|t| t.jobs == ["test"]).unwrap();
    ShardRequest {
        workflow: task.workflow.clone(),
        job: "test".into(),
        plan: shards,
        step: 0,
        dir: String::new(),
        discovered: discovered.clone(),
        fail_fast,
    }
}

/// The one split job of `plan`. Fails the test when the request was
/// refused: a test of shards must not pass on a job that was not split.
fn split_group(plan: &local_ci::FrozenPlan) -> ShardGroup {
    let split: Vec<&ShardGroup> = plan.shard_groups.iter().filter(|g| g.split).collect();
    let reasons: Vec<_> = plan.shard_groups.iter().map(|g| &g.not_split).collect();
    assert_eq!(split.len(), 1, "not split: {reasons:?}");
    split[0].clone()
}

/// The result of shard `at` of `group`: its job with `status`, and its
/// tests (those it was given, without `leave_out`) with `tests`.
fn shard_result(
    group: &ShardGroup,
    at: usize,
    status: &str,
    tests: &str,
    leave_out: Option<&str>,
) -> Vec<u8> {
    let task = &group.shards[at];
    let mut result: serde_json::Value =
        serde_json::from_slice(&ci_result(task, "test", status, json!({}))).unwrap();
    let reported: Vec<serde_json::Value> = group.assignments[at]
        .iter()
        .filter(|t| Some(t.as_str()) != leave_out)
        .map(|t| json!({"identity": t, "status": tests}))
        .collect();
    result["shard"] = json!({
        "job_id": "test", "shard_id": task, "tests": reported,
        "exit_ok": tests != "failed", "parsed": true,
    });
    serde_json::to_vec(&result).unwrap()
}

/// [`ci_run`], with the task ids by the id of the task in the plan.
async fn ci_run_by_task(
    fx: &Fx,
    c: &Coordinator,
    plan: &local_ci::FrozenPlan,
) -> (RunId, BTreeMap<String, TaskId>) {
    ci_run_by_id(fx, c, plan, RunId::generate()).await
}

async fn ci_run_by_id(
    fx: &Fx,
    c: &Coordinator,
    plan: &local_ci::FrozenPlan,
    run: RunId,
) -> (RunId, BTreeMap<String, TaskId>) {
    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), plan).unwrap();
    let manifest = Digest::of_bytes(b"manifest");
    let none = Default::default();
    let specs = belt::service::ci_run::task_specs(plan, &digest, &manifest, &none, None).unwrap();
    let source = SourceId::generate();
    c.store().put_source_ref(&source, &manifest, 1).unwrap();
    let mut ids = BTreeMap::new();
    let mut tasks = Vec::new();
    for (plan_task, spec) in specs {
        let id = TaskId::generate();
        ids.insert(plan_task, id.clone());
        tasks.push((id, spec));
    }
    c.enqueue(run.clone(), source, tasks).await.unwrap();
    (run, ids)
}

/// The attempts a tick placed, by the task of the plan each runs, and the
/// worker that has it.
fn placed_by_task(
    workers: &[&Arc<Worker>],
    placed: &[AttemptId],
) -> BTreeMap<String, (AttemptId, Arc<Worker>)> {
    let mut out = BTreeMap::new();
    for a in placed {
        let worker = workers
            .iter()
            .find(|w| w.state.lock().unwrap().jobs.contains_key(a))
            .unwrap();
        let task = worker.payload(a).ci_task.unwrap().task_id;
        out.insert(task, (a.clone(), Arc::clone(worker)));
    }
    out
}

/// End the attempt of a shard on its worker with `result`, and collect it.
async fn end_shard(
    c: &Coordinator,
    files: &Arc<ResultStore>,
    placed: &(AttemptId, Arc<Worker>),
    outcome: Outcome,
    result: &[u8],
) {
    let (a, worker) = placed;
    let p = worker.payload(a);
    worker.finish(a, outcome, ci_out(a, &p, result));
    assert_eq!(c.tick().await.unwrap().settled, vec![a.clone()]);
    ci_collect(c, files, a).await;
}

fn ci_lines(
    fx: &Fx,
    c: &Coordinator,
    files: &Arc<ResultStore>,
    run: &RunId,
) -> Vec<belt::service::ci_run::JobLine> {
    belt::service::ci_run::job_lines(c.store(), fx.dir.path(), files.root(), run).unwrap()
}

/// What the status of the run says it computed (S19): from the completion of
/// the run, as `ci` reads it after its wait, and the lines of the run.
fn ci_computed(
    fx: &Fx,
    c: &Coordinator,
    files: &Arc<ResultStore>,
    run: &RunId,
) -> belt::ci::status::Computed {
    use belt::service::{ci_run, completion::Target, control::Report};
    let target = Target::Run(run.clone());
    let snapshot = c.store().completion(target.clone()).unwrap();
    let report = Report::settled(target, &snapshot).expect("the run settled");
    let lines = ci_lines(fx, c, files, run);
    let expected: Vec<String> = lines
        .iter()
        .filter(|l| l.part != Some(ci_run::Part::Job))
        .map(|l| l.plan_task.clone())
        .collect();
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    ci_run::Ended::of(&report).computed(&expected, &lines)
}

/// A39: a job of Vitest (and of Jest) files is split by file. Every listed
/// file is in exactly one shard and each shard is told its own files; the
/// shards are placed side by side as tasks of their own; the job has one
/// outcome only when all of them ended, made of every file exactly once;
/// what needs the job starts then, once. A file that no shard reported
/// makes the job a failure although every shard passed.
#[tokio::test]
async fn a39_file_shards_cover_every_test_once_and_add_up_to_one_job() {
    use belt::service::ci_run::Part;
    let files_listed = [
        "src/a.test.ts",
        "src/b.test.ts",
        "src/c.test.ts",
        "src/d.test.ts",
        "src/e.test.ts",
    ];
    let discovered = listed(&files_listed);
    let runners: [(&str, Box<dyn RunnerAdapter>); 2] = [
        (
            "npx vitest run",
            Box::new(Vitest::new("3.2.4", "/project", &[], Safety::default())),
        ),
        (
            "npx jest",
            Box::new(Jest::new("29.7.0", "/project", &[], Safety::default())),
        ),
    ];
    for (omit, (command, adapter)) in [false, true].into_iter().zip(runners) {
        let fx = fx();
        let (c, files) = fx.open();
        let other = Worker::new("w-b");
        c.try_attach(other.id.clone(), Arc::clone(&other) as Arc<dyn WorkerLink>)
            .unwrap();
        let whole = ci_plan(&split_workflow(command));
        let opts = PartitionOpts::default();
        let shards = partition_tests(adapter.as_ref(), &discovered, &History::default(), 2, &opts);
        assert!(shards.verify(&discovered).is_ok());
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, false)])
            .unwrap();
        let group = split_group(&plan);
        assert_eq!(group.shards.len(), 2, "{command}");
        // The file the second shard does not report, in the second round.
        let left_out = omit.then(|| group.assignments[1][0].clone());
        let leave_out = left_out.as_deref();

        // The discovery and what each shard is told to run are the same
        // files: every one in exactly one shard, and the task of a shard
        // carries exactly its own.
        let mut all: Vec<&str> = group
            .assignments
            .iter()
            .flatten()
            .map(String::as_str)
            .collect();
        all.sort();
        assert_eq!(all, files_listed);
        for (id, given) in group.shards.iter().zip(&group.assignments) {
            let task = plan.task(id).unwrap();
            assert_eq!(&task.shard.as_ref().unwrap().tests, given);
            let demand = local_ci::plan_task_demand(&plan, id).unwrap();
            let tests: Vec<usize> = demand.nodes.iter().filter_map(|n| n.weight.tests).collect();
            assert_eq!(tests, [given.len()], "{id}");
            // The command the shard's worker builds from the plan names
            // exactly those files.
            let shard = task.shard.as_ref().unwrap();
            let runner = local_ci::shard::adapters::from_info(&shard.runner, "/project").unwrap();
            let listed = local_ci::shard::Shard {
                index: shard.index,
                tests: shard.tests.clone(),
                load: 0,
            };
            let argv = runner.invoke(&listed, shard.count, shard.mode).argv;
            let named: Vec<&str> = files_listed
                .iter()
                .copied()
                .filter(|file| argv.iter().any(|arg| arg.ends_with(file)))
                .collect();
            let mut own: Vec<&str> = given.iter().map(String::as_str).collect();
            own.sort();
            assert_eq!(named, own, "{argv:?}");
        }

        // Both shards are placed at once; what needs the job waits.
        let (run, ids) = ci_run_by_task(&fx, &c, &plan).await;
        let report_task = ids[&ci_task_of(&plan, "report")].clone();
        let report = c.tick().await.unwrap();
        let placed = placed_by_task(&[&fx.w, &other], &report.placed);
        assert_eq!(
            placed.keys().collect::<Vec<_>>(),
            group.shards.iter().collect::<Vec<_>>()
        );
        for id in &group.shards {
            let sent = placed[id].1.payload(&placed[id].0).ci_task.unwrap();
            assert_eq!(&sent.task_id, id);
        }

        // One shard ended: the job has no outcome yet and nothing starts.
        let first = shard_result(&group, 0, "success", "passed", None);
        end_shard(
            &c,
            &files,
            &placed[&group.shards[0]],
            Outcome::Succeeded,
            &first,
        )
        .await;
        assert!(ci_advance(&fx, &c).released.is_empty());
        let lines = ci_lines(&fx, &c, &files, &run);
        assert!(lines.iter().all(|l| l.part != Some(Part::Job)), "{lines:?}");

        // The other ended: the job is made of both, and report starts once.
        let second = shard_result(&group, 1, "success", "passed", leave_out);
        end_shard(
            &c,
            &files,
            &placed[&group.shards[1]],
            Outcome::Succeeded,
            &second,
        )
        .await;
        let pass = ci_advance(&fx, &c);
        assert_eq!(pass.released, vec![report_task.clone()]);
        assert!(pass.stopped.is_empty() && pass.unrunnable.is_empty());
        assert!(ci_advance(&fx, &c).released.is_empty());
        let next = c.tick().await.unwrap();
        assert_eq!(next.placed.len(), 1);
        let placed = placed_by_task(&[&fx.w, &other], &next.placed);
        let (b, worker) = &placed[&ci_task_of(&plan, "report")];
        let inputs = worker.payload(b).ci_task.unwrap().inputs;
        let expected = if leave_out.is_some() {
            "failure"
        } else {
            "success"
        };
        assert_eq!(inputs["needs"]["test"]["status"], expected, "{command}");
        assert_eq!(inputs["needs"].as_object().unwrap().len(), 1);

        // The report: a line per shard, and one for the job they are.
        let lines = ci_lines(&fx, &c, &files, &run);
        let shard_lines = lines.iter().filter(|l| l.part == Some(Part::Shard)).count();
        assert_eq!(shard_lines, 2);
        let job: Vec<_> = lines.iter().filter(|l| l.part == Some(Part::Job)).collect();
        assert_eq!(job.len(), 1);
        assert_eq!(job[0].status, expected);
        assert_eq!(job[0].plan_task, group.id);
        assert_eq!(job[0].job.as_deref(), Some("test"));
        // The run itself says so, wherever its result is read: report ends
        // as a success, and the run is a failure only when the job is.
        let sent = worker.payload(b);
        let done = ci_result(&ci_task_of(&plan, "report"), "report", "success", json!({}));
        worker.finish(b, Outcome::Succeeded, ci_out(b, &sent, &done));
        assert_eq!(c.tick().await.unwrap().settled, vec![b.clone()]);
        ci_collect(&c, &files, b).await;
        let target = belt::service::completion::Target::Run(run.clone());
        let snapshot = c.store().completion(target).unwrap();
        assert!(snapshot.settled());
        let code = snapshot.exit_code().unwrap();
        match leave_out {
            None => {
                assert_eq!((&job[0].skipped, &job[0].reason), (&None, &None));
                assert_eq!(code, 0);
            }
            Some(file) => {
                let why = job[0].reason.clone().unwrap();
                assert!(why.contains(file) && why.contains("not reported"), "{why}");
                assert_eq!(code, 1, "every shard passed, the job did not");
            }
        }
        assert_eq!(c.store().attempts_of_task(&report_task).unwrap().len(), 1);
    }
}

/// A40: with the times of earlier runs a slow test gets a shard of its own
/// and the rest share the other, so the longest shard (what the job takes
/// when the shards run side by side) is shorter than with an even count.
/// The uneven shards are placed in one tick, each as its own task. Without
/// any history the split is still made, the same way every time, and runs.
#[tokio::test]
async fn a40_history_shortens_the_longest_shard_and_no_history_is_a_stable_split_that_runs() {
    let names = ["slow", "t1", "t2", "t3", "t4", "t5"];
    let discovered = listed(&names);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let mut history = History::default();
    history.millis.insert("slow".into(), 50_000);
    for name in &names[1..] {
        history.millis.insert(name.to_string(), 10_000);
    }
    let weighed = partition_tests(&adapter, &discovered, &history, 2, &opts);
    assert_eq!(weighed.basis, Basis::History);
    let loads: Vec<u64> = weighed.shards.iter().map(|s| s.load).collect();
    assert_eq!(loads.iter().sum::<u64>(), 100_000);
    assert_eq!(loads.iter().max(), Some(&50_000), "{loads:?}");
    // Three tests a shard, as a count would divide them, takes 70 seconds.
    let slow_shard = weighed
        .shards
        .iter()
        .find(|s| s.tests.contains(&"slow".to_string()));
    assert_eq!(slow_shard.unwrap().tests, ["slow"]);

    let fx = fx();
    let (c, files) = fx.open();
    let whole = ci_plan(&split_workflow("cargo test"));
    let plan = whole
        .with_shards(&[split_request(&whole, weighed, &discovered, false)])
        .unwrap();
    let group = split_group(&plan);
    let sizes: Vec<usize> = group.assignments.iter().map(Vec::len).collect();
    assert!(sizes.contains(&1) && sizes.contains(&5), "{sizes:?}");
    let (run, _ids) = ci_run_by_task(&fx, &c, &plan).await;
    let report = c.tick().await.unwrap();
    assert_eq!(
        report.placed.len(),
        2,
        "side by side, not one after the other"
    );
    let placed = placed_by_task(&[&fx.w], &report.placed);
    for (at, id) in group.shards.iter().enumerate() {
        let result = shard_result(&group, at, "success", "passed", None);
        end_shard(&c, &files, &placed[id], Outcome::Succeeded, &result).await;
    }
    assert_eq!(ci_advance(&fx, &c).released.len(), 1);
    let lines = ci_lines(&fx, &c, &files, &run);
    let job = lines.iter().find(|l| l.plan_task == group.id).unwrap();
    assert_eq!(job.status, "success");

    // No history: every test weighs the same, and the same input gives the
    // same shards, the same plan and the same tasks.
    let fallback = || {
        let shards = partition_tests(&adapter, &discovered, &History::default(), 2, &opts);
        assert_eq!(shards.basis, Basis::Fallback);
        whole
            .with_shards(&[split_request(&whole, shards, &discovered, false)])
            .unwrap()
    };
    let (one, two) = (fallback(), fallback());
    assert_eq!(one.digest, two.digest);
    let group = split_group(&one);
    assert_eq!(group, split_group(&two));
    let sizes: Vec<usize> = group.assignments.iter().map(Vec::len).collect();
    assert_eq!(sizes, [3, 3]);
    let fx = crate::collection::fx();
    let (c, files) = fx.open();
    let (run, _ids) = ci_run_by_task(&fx, &c, &one).await;
    let report = c.tick().await.unwrap();
    let placed = placed_by_task(&[&fx.w], &report.placed);
    assert_eq!(placed.len(), 2);
    for (at, id) in group.shards.iter().enumerate() {
        let result = shard_result(&group, at, "success", "passed", None);
        end_shard(&c, &files, &placed[id], Outcome::Succeeded, &result).await;
    }
    assert_eq!(ci_advance(&fx, &c).released.len(), 1);
    let lines = ci_lines(&fx, &c, &files, &run);
    let job = lines.iter().find(|l| l.plan_task == group.id).unwrap();
    assert_eq!(job.status, "success");
}

/// A41, a job that must not be split: the request is refused, the plan
/// says why, and the job is one task that runs like any other. A job whose
/// tests use a port of their own (a server the shards would share) and one
/// with a name filter are examples; a job with service containers is
/// explained the same way.
#[tokio::test]
async fn a41_a_job_that_cannot_be_split_says_why_and_runs_as_one_job() {
    let discovered = listed(&["a", "b", "c", "d"]);
    let opts = PartitionOpts::default();
    let two = |adapter: &dyn RunnerAdapter| {
        partition_tests(adapter, &discovered, &History::default(), 2, &opts)
    };
    let plain = CargoTest::new("1.91.0", &[], Safety::default());
    // The job starts a server on a port: its shards would share it.
    let with_port = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    env:\n      API_PORT: 4000\n    steps:\n      - run: cargo test\n";
    // The tests update state they share (declared by the caller).
    let shared = CargoTest::new(
        "1.91.0",
        &[],
        Safety {
            shared_state: true,
            isolated_fixtures: false,
        },
    );
    let with_services = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    services:\n      db:\n        image: postgres:16\n    steps:\n      - run: cargo test\n";
    let plain_workflow = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n";
    let cases: [(&str, ShardPlan, &[NotSplit], &str); 3] = [
        (
            with_port,
            two(&plain),
            &[NotSplit::NoIsolation],
            "no_isolation",
        ),
        (
            plain_workflow,
            two(&shared),
            &[NotSplit::SharedState, NotSplit::NoIsolation],
            "shared_state",
        ),
        (
            with_services,
            two(&plain),
            &[NotSplit::HasServices],
            "has_services",
        ),
    ];
    for (workflow, shards, reasons, shown) in cases {
        let whole = ci_plan(workflow);
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, true)])
            .unwrap();
        // It fell back to the one task of the job, and says why.
        assert_eq!(plan.tasks.len(), 1, "{shown}");
        assert!(plan.tasks[0].shard.is_none());
        let group = &plan.shard_groups[0];
        assert!(!group.split);
        for reason in reasons {
            assert!(group.not_split.contains(reason), "{:?}", group.not_split);
        }
        let digest = plan.digest.clone();
        let ci = belt::ci::CiPlan::from_frozen(plan).unwrap();
        let text = belt::ci::summary::render_text(ci.summary());
        assert!(
            text.contains("is not split") && text.contains(shown),
            "{text}"
        );
        let plan = ci.into_frozen();
        assert_eq!(plan.digest, digest);
        if !plan.tasks[0].services.is_empty() {
            // Service containers are not run yet at all (not a matter of
            // shards): the plan explains, the run is refused.
            continue;
        }

        // The single job runs and ends like any job: no shard, no stop.
        let fx = fx();
        let (c, files) = fx.open();
        let (run, ids) = ci_run(&fx, &c, &plan).await;
        let spec = c.store().ci_run_tasks(&run).unwrap()[0].spec.clone();
        assert!(spec["ci"].get("shard_group").is_none());
        assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());
        let a = c.tick().await.unwrap().placed[0].clone();
        let p = fx.w.payload(&a);
        let result = ci_result(&ci_task_of(&plan, "test"), "test", "success", json!({}));
        end(&fx, &c, &a, Outcome::Succeeded, ci_out(&a, &p, &result)).await;
        ci_collect(&c, &files, &a).await;
        assert_eq!(ci_advance(&fx, &c), Default::default());
        let lines = ci_lines(&fx, &c, &files, &run);
        assert_eq!(lines.len(), 1);
        assert_eq!((lines[0].status.as_str(), lines[0].part), ("success", None));
        assert_eq!(lines[0].task_id, ids["test"]);
        let target = belt::service::completion::Target::Run(run);
        assert_eq!(c.store().completion(target).unwrap().exit_code(), Some(0));
    }
}

/// A41, fail-fast: a shard fails while the others run. The parent asks for
/// their cancel once, the coordinator delivers it, the stop is confirmed
/// and what the failed shard left is kept; the job is a failure, what needs
/// it is told so, and the report names each shard and the job. Without
/// fail-fast the other shards go on to their end.
#[tokio::test]
async fn a41_fail_fast_stops_the_running_shards_once_and_the_job_fails_with_a_report() {
    use belt::service::ci_run::Part;
    let names = ["a", "b", "c", "d", "e", "f"];
    let discovered = listed(&names);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    for fail_fast in [true, false] {
        let fx = fx();
        let (c, files) = fx.open();
        let whole = ci_plan(&split_workflow("cargo test"));
        let shards = partition_tests(&adapter, &discovered, &History::default(), 3, &opts);
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, fail_fast)])
            .unwrap();
        let group = split_group(&plan);
        assert_eq!((group.shards.len(), group.fail_fast), (3, fail_fast));
        let (run, ids) = ci_run_by_task(&fx, &c, &plan).await;
        let report = c.tick().await.unwrap();
        let placed = placed_by_task(&[&fx.w], &report.placed);
        assert_eq!(placed.len(), 3);
        // Nothing failed: a pass stops nothing.
        assert!(ci_advance(&fx, &c).stopped.is_empty());

        let failed = shard_result(&group, 0, "failure", "failed", None);
        end_shard(
            &c,
            &files,
            &placed[&group.shards[0]],
            Outcome::Failed,
            &failed,
        )
        .await;
        let pass = ci_advance(&fx, &c);
        let others = [ids[&group.shards[1]].clone(), ids[&group.shards[2]].clone()];
        if !fail_fast {
            assert!(pass.stopped.is_empty());
            c.tick().await.unwrap();
            assert_eq!(fx.w.count("cancel"), 0);
            for task in &others {
                assert_eq!(c.store().task_cancel_requested(task).unwrap(), None);
            }
            // The others end on their own, and the job is still a failure.
            for at in [1, 2] {
                let result = shard_result(&group, at, "success", "passed", None);
                end_shard(
                    &c,
                    &files,
                    &placed[&group.shards[at]],
                    Outcome::Succeeded,
                    &result,
                )
                .await;
            }
            assert_eq!(ci_advance(&fx, &c).released.len(), 1);
            let lines = ci_lines(&fx, &c, &files, &run);
            let job = lines.iter().find(|l| l.part == Some(Part::Job)).unwrap();
            assert_eq!(job.status, "failure");
            continue;
        }
        assert_eq!(pass.stopped, others);
        assert!(pass.released.is_empty(), "the job has not ended yet");
        for task in &others {
            assert!(c.store().task_cancel_requested(task).unwrap().is_some());
        }
        // Asked once: a pass that sees the same again writes nothing.
        let again = ci_advance(&fx, &c);
        assert!(again.stopped.is_empty() && again.released.is_empty());
        let stops = c
            .store()
            .events_after(0, 1000)
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == "ci_fail_fast_requested")
            .collect::<Vec<_>>();
        assert_eq!(stops.len(), 2);
        for id in &group.shards[1..] {
            let event = stops
                .iter()
                .find(|e| e.attempt_id.as_deref() == Some(placed[id].0.as_str()))
                .unwrap();
            assert_eq!(event.payload["task"], ids[id].as_str());
            assert_eq!(event.payload["reason"], belt::ci::shards::FAIL_FAST_REASON);
        }
        // The coordinator delivers the cancels; the stop is confirmed.
        for _ in 0..4 {
            c.tick().await.unwrap();
            collect(&c, &files).await;
        }
        assert_eq!(fx.w.count("cancel"), 2);
        for id in &group.shards[1..] {
            let rec = c.store().get_attempt(&placed[id].0).unwrap().unwrap();
            let ended = rec.termination.map(|t| t.outcome());
            assert_eq!(ended, Some(Outcome::Cancelled));
        }
        // What the failed shard left is still there.
        let kept = c
            .store()
            .persisted_artifacts(&placed[&group.shards[0]].0)
            .unwrap();
        assert!(kept.iter().any(|(name, _)| name == RESULT_ARTIFACT));

        // The job ended as a failure; report (`if: always()`) is told so.
        let pass = ci_advance(&fx, &c);
        assert_eq!(
            pass.released,
            vec![ids[&ci_task_of(&plan, "report")].clone()]
        );
        assert!(pass.stopped.is_empty());
        let next = c.tick().await.unwrap();
        let b = next.placed[0].clone();
        let inputs = fx.w.payload(&b).ci_task.unwrap().inputs;
        assert_eq!(inputs["needs"]["test"]["status"], "failure");
        assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());

        let lines = ci_lines(&fx, &c, &files, &run);
        let of = |part: Part| -> Vec<(&str, &str)> {
            lines
                .iter()
                .filter(|l| l.part == Some(part))
                .map(|l| (l.plan_task.as_str(), l.status.as_str()))
                .collect()
        };
        assert_eq!(
            of(Part::Shard),
            [
                (group.shards[0].as_str(), "failure"),
                (group.shards[1].as_str(), "no_result"),
                (group.shards[2].as_str(), "no_result"),
            ]
        );
        assert_eq!(of(Part::Job), [(group.id.as_str(), "failure")]);
        let text = belt::service::ci_run::render_jobs(&lines);
        assert!(
            text.contains(&format!("{}: failure\n", group.shards[0])),
            "{text}"
        );
        let said = lines.iter().find(|l| l.part == Some(Part::Job)).unwrap();
        let why = said.reason.clone().unwrap();
        let first_failed = &group.assignments[0][0];
        assert!(why.contains(&format!("failed: {first_failed}")), "{why}");
        assert!(
            why.contains(&format!("missing: shard {}", group.shards[1])),
            "{why}"
        );
        assert!(text.contains(&format!("test: failure ({why})\n")), "{text}");

        // Once report ended too, the status of the run (S19) is its
        // failure: the stop of the other shards makes it no cancelled run.
        let p = fx.w.payload(&b);
        let task = ci_task_of(&plan, "report");
        let result = ci_result(&task, "report", "success", serde_json::json!({}));
        fx.w.finish(&b, Outcome::Succeeded, ci_out(&b, &p, &result));
        for _ in 0..3 {
            c.tick().await.unwrap();
            collect(&c, &files).await;
        }
        let target = belt::service::completion::Target::Run(run.clone());
        assert!(c.store().completion(target).unwrap().cancel_requested);
        assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Failure);
    }
}

/// A41, retry: after a shard failed and fail-fast stopped another, a retry
/// of the run starts a new attempt of those two only, on the same frozen
/// plan. The shard that passed is not run again and its checked result is
/// kept: the job becomes a success from the kept result and the new ones.
#[tokio::test]
async fn a41_a_retry_runs_only_the_shards_without_a_passing_result_and_keeps_the_rest() {
    retry_shards_with_report(true).await;
}

#[tokio::test]
async fn a41_retry_rebinds_failed_downstream_needs_after_the_shards_rejoin() {
    retry_shards_with_report(false).await;
}

async fn retry_shards_with_report(report_passes: bool) {
    use belt::service::ci_run::Part;
    use belt::service::completion::Target;
    use belt::storage::RetryPreparation;
    let discovered = listed(&["a", "b", "c", "d", "e", "f"]);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let fx = fx();
    let (c, files) = fx.open();
    let whole = ci_plan(&split_workflow("cargo test"));
    let shards = partition_tests(&adapter, &discovered, &History::default(), 3, &opts);
    let plan = whole
        .with_shards(&[split_request(&whole, shards, &discovered, true)])
        .unwrap();
    let group = split_group(&plan);
    let (run, ids) = ci_run_by_task(&fx, &c, &plan).await;
    let task = |at: usize| ids[&group.shards[at]].clone();
    let attempts = |at: usize| c.store().attempts_of_task(&task(at)).unwrap();
    let first = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);

    // Shard 0 passes, shard 1 fails, shard 2 is stopped while it runs.
    let passed = shard_result(&group, 0, "success", "passed", None);
    end_shard(
        &c,
        &files,
        &first[&group.shards[0]],
        Outcome::Succeeded,
        &passed,
    )
    .await;
    let failed = shard_result(&group, 1, "failure", "failed", None);
    end_shard(
        &c,
        &files,
        &first[&group.shards[1]],
        Outcome::Failed,
        &failed,
    )
    .await;
    assert_eq!(ci_advance(&fx, &c).stopped, vec![task(2)]);
    for _ in 0..4 {
        c.tick().await.unwrap();
        collect(&c, &files).await;
    }
    assert_eq!(ci_advance(&fx, &c).released.len(), 1, "report goes on");
    let lines = ci_lines(&fx, &c, &files, &run);
    let job = lines.iter().find(|l| l.part == Some(Part::Job)).unwrap();
    assert_eq!(job.status, "failure");
    let report = ci_task_of(&plan, "report");
    let b = c.tick().await.unwrap().placed[0].clone();
    let sent = fx.w.payload(&b);
    assert_eq!(
        sent.ci_task.as_ref().unwrap().inputs["needs"]["test"]["status"],
        "failure"
    );
    let status = if report_passes { "success" } else { "failure" };
    let outcome = if report_passes {
        Outcome::Succeeded
    } else {
        Outcome::Failed
    };
    let done = ci_result(&report, "report", status, json!({}));
    end(&fx, &c, &b, outcome, ci_out(&b, &sent, &done)).await;
    ci_collect(&c, &files, &b).await;

    let target = Target::Run(run.clone());
    let revision = c
        .store()
        .completion(target.clone())
        .unwrap()
        .control_revision;
    let prepared = match c.store().prepare_retry(target, revision, "again".into()) {
        Ok(RetryPreparation::Prepared(prepared)) => prepared,
        Ok(RetryPreparation::Replay(_)) => panic!("a first request is no replay"),
        Err(e) => panic!("{e}"),
    };
    let receipt = c.store().commit_retry(prepared, false, wall_ms()).unwrap();
    assert_eq!(receipt.task_count, if report_passes { 2 } else { 3 });
    let rows = c.store().ci_run_tasks(&run).unwrap();
    let row = |at: usize| rows.iter().find(|r| r.task_id == task(at)).unwrap();
    let kept = row(0).attempt.clone().unwrap();
    assert_eq!(kept.attempt_id, first[&group.shards[0]].0);
    assert!(kept.termination.is_some() && kept.collection.is_some());
    for at in [1, 2] {
        assert!(
            row(at).attempt.is_none() && !row(at).cancelled,
            "shard {at}"
        );
    }
    // The job is open again: the run is looked at, and nothing is stopped
    // for the failure that was retried.
    assert_eq!(c.store().ci_waiting_runs(8).unwrap(), vec![run.clone()]);
    let pass = ci_advance(&fx, &c);
    assert!(
        pass.stopped.is_empty() && pass.released.is_empty(),
        "{pass:?}"
    );
    let lines = ci_lines(&fx, &c, &files, &run);
    assert!(lines.iter().all(|l| l.part != Some(Part::Job)), "{lines:?}");

    // The two get a new attempt each; the shard that passed gets none.
    let again = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
    let retried: Vec<&String> = again.keys().collect();
    assert_eq!(retried, [&group.shards[1], &group.shards[2]]);
    for at in [1, 2] {
        let sent = again[&group.shards[at]]
            .1
            .payload(&again[&group.shards[at]].0);
        let before = first[&group.shards[at]]
            .1
            .payload(&first[&group.shards[at]].0);
        // The same task of the same frozen plan.
        assert_eq!(sent.ci_task, before.ci_task);
        let result = shard_result(&group, at, "success", "passed", None);
        end_shard(
            &c,
            &files,
            &again[&group.shards[at]],
            Outcome::Succeeded,
            &result,
        )
        .await;
    }
    assert_eq!(
        (attempts(0).len(), attempts(1).len(), attempts(2).len()),
        (1, 2, 2)
    );
    let joined = ci_advance(&fx, &c);
    if !report_passes {
        assert_eq!(joined.released, vec![ids[&report].clone()]);
        let placements = c.tick().await.unwrap().placed;
        assert_eq!(placements.len(), 1);
        let attempt = &placements[0];
        let sent = fx.w.payload(attempt);
        assert_eq!(
            sent.ci_task.as_ref().unwrap().inputs["needs"]["test"]["status"],
            "success"
        );
        let done = ci_result(&report, "report", "success", json!({}));
        end(
            &fx,
            &c,
            attempt,
            Outcome::Succeeded,
            ci_out(attempt, &sent, &done),
        )
        .await;
        ci_collect(&c, &files, attempt).await;
    }
    assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());
    let lines = ci_lines(&fx, &c, &files, &run);
    let job = lines.iter().find(|l| l.part == Some(Part::Job)).unwrap();
    assert_eq!((job.status.as_str(), &job.reason), ("success", &None));
    let shards: Vec<&str> = lines
        .iter()
        .filter(|l| l.part == Some(Part::Shard))
        .map(|l| l.status.as_str())
        .collect();
    assert_eq!(shards, ["success", "success", "success"]);
}

/// A41, fail-fast for a job nothing waits for, with more shards than are
/// placed at once: the run is looked at although no task of it waits, the
/// shards that run are cancelled and the one that was never placed is
/// cancelled with the reason and never starts. Such a shard has no attempt,
/// so the existing rule refuses a retry of the run.
#[tokio::test]
async fn a41_fail_fast_reaches_a_shard_that_was_never_placed_in_a_job_nothing_waits_for() {
    use belt::service::ci_run::Part;
    use belt::service::completion::Target;
    let names: Vec<String> = (0..36).map(|i| format!("t{i:02}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let discovered = listed(&names);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let fx = fx();
    let (c, files) = fx.open();
    let only = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n";
    let whole = ci_plan(only);
    let shards = partition_tests(&adapter, &discovered, &History::default(), 18, &opts);
    let plan = whole
        .with_shards(&[split_request(&whole, shards, &discovered, true)])
        .unwrap();
    let group = split_group(&plan);
    assert_eq!((group.shards.len(), plan.tasks.len()), (18, 18));
    let (run, ids) = ci_run_by_task(&fx, &c, &plan).await;
    // No task waits for a job, yet the run is one a pass looks at.
    assert_eq!(c.store().ci_waiting_runs(8).unwrap(), vec![run.clone()]);
    let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
    assert_eq!(placed.len(), 16, "what one worker holds at once");
    let queued: Vec<&String> = group
        .shards
        .iter()
        .filter(|s| !placed.contains_key(*s))
        .collect();
    assert_eq!(queued.len(), 2);

    let failing = placed.keys().next().unwrap().clone();
    let at = group.shards.iter().position(|s| s == &failing).unwrap();
    let failed = shard_result(&group, at, "failure", "failed", None);
    end_shard(&c, &files, &placed[&failing], Outcome::Failed, &failed).await;
    // The slot that came free took one of the two; the other still waits.
    let unplaced = |s: &&&String| c.store().attempts_of_task(&ids[**s]).unwrap().is_empty();
    let still: Vec<&&String> = queued.iter().filter(unplaced).collect();
    assert_eq!(still.len(), 1);
    let waiting = ids[*still[0]].clone();
    let pass = ci_advance(&fx, &c);
    assert_eq!(
        pass.stopped.len(),
        17,
        "those that run and the one that waits"
    );
    assert!(pass.stopped.contains(&waiting));
    let rows = c.store().ci_run_tasks(&run).unwrap();
    let row = rows.iter().find(|r| r.task_id == waiting).unwrap();
    assert!(row.cancelled && row.attempt.is_none());
    assert_eq!(
        row.spec["ci"]["unrunnable"],
        belt::ci::shards::FAIL_FAST_REASON
    );
    for _ in 0..4 {
        c.tick().await.unwrap();
        collect(&c, &files).await;
        assert!(ci_advance(&fx, &c).stopped.is_empty(), "asked once");
    }
    assert_eq!(fx.w.count("cancel"), 16);
    assert!(c.store().attempts_of_task(&waiting).unwrap().is_empty());
    assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());

    let lines = ci_lines(&fx, &c, &files, &run);
    let job = lines.iter().find(|l| l.part == Some(Part::Job)).unwrap();
    assert_eq!(job.status, "failure");
    let never = lines.iter().find(|l| l.task_id == waiting).unwrap();
    assert_eq!(never.status, "no_result");
    assert_eq!(
        never.reason.as_deref(),
        Some(belt::ci::shards::FAIL_FAST_REASON)
    );
    let target = Target::Run(run.clone());
    let snapshot = c.store().completion(target.clone()).unwrap();
    assert!(snapshot.settled());
    assert!(snapshot.exit_code().is_some_and(|code| code != 0));
    // A retry of that shard alone: it has no attempt, so the retry names no
    // generation, and the result of the run can still be read. While the
    // failure stands, fail-fast stops it again (still never placed).
    let alone = Target::Task(waiting.clone());
    let prepared = match c
        .store()
        .prepare_retry(alone, snapshot.control_revision, "alone".into())
    {
        Ok(belt::storage::RetryPreparation::Prepared(prepared)) => prepared,
        Ok(_) => panic!("a first request is no replay"),
        Err(e) => panic!("{e}"),
    };
    let receipt = c.store().commit_retry(prepared, false, wall_ms()).unwrap();
    assert_eq!((receipt.task_count, receipt.min_generation), (1, None));
    let open = c.store().completion(target.clone()).unwrap();
    assert!(!open.settled() && open.exit_code().is_none());
    let rows = c.store().ci_run_tasks(&run).unwrap();
    let row = rows.iter().find(|r| r.task_id == waiting).unwrap();
    assert!(!row.cancelled && row.attempt.is_none());
    let pass = ci_advance(&fx, &c);
    assert_eq!(pass.stopped, vec![waiting.clone()]);
    // The pass after sees it ended, and the job has its outcome again.
    assert_eq!(ci_advance(&fx, &c).joined, vec![group.id.clone()]);
    let snapshot = c.store().completion(target.clone()).unwrap();
    assert!(snapshot.settled());

    // A retry of the run takes the shard that never ran back into the
    // queue, with the failed one and the ones that were stopped.
    let prepared =
        match c
            .store()
            .prepare_retry(target.clone(), snapshot.control_revision, "again".into())
        {
            Ok(belt::storage::RetryPreparation::Prepared(prepared)) => prepared,
            Ok(_) => panic!("a first request is no replay"),
            Err(e) => panic!("{e}"),
        };
    let receipt = c.store().commit_retry(prepared, false, wall_ms()).unwrap();
    assert_eq!(receipt.task_count, 18);
    let rows = c.store().ci_run_tasks(&run).unwrap();
    for row in &rows {
        assert!(!row.cancelled && row.attempt.is_none(), "{}", row.task_id);
        assert!(row.spec["ci"].get("unrunnable").is_none());
        assert!(row.spec["ci"].get("joined").is_none());
        assert!(row.spec["ci"].get("join_failure").is_none());
    }
    assert!(!c.store().completion(target).unwrap().settled());
    assert_eq!(c.store().ci_waiting_runs(8).unwrap(), vec![run.clone()]);
    let again = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
    assert_eq!(again.len(), 16);
    let first_time: Vec<&String> = again.keys().filter(|id| ids[*id] == waiting).collect();
    assert!(first_time.len() <= 1);
    assert!(ci_advance(&fx, &c).stopped.is_empty(), "nothing failed now");
}

/// A run whose split job is a failure only as a whole (every shard passed,
/// one test was reported by none) is a failed run wherever its result is
/// read. It is not settled before the job is joined, so no reader sees a
/// success first. A run that can never go on does not wait for the job.
#[tokio::test]
async fn a41_a_run_is_settled_only_when_its_split_job_is_joined_and_fails_with_it() {
    use belt::service::completion::Target;
    let discovered = listed(&["a", "b", "c", "d"]);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let only = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n";
    for omit in [false, true] {
        let fx = fx();
        let (c, files) = fx.open();
        let whole = ci_plan(only);
        let shards = partition_tests(&adapter, &discovered, &History::default(), 2, &opts);
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, false)])
            .unwrap();
        let group = split_group(&plan);
        let (run, ids) = ci_run_by_task(&fx, &c, &plan).await;
        let target = Target::Run(run.clone());
        let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
        let left_out = omit.then(|| group.assignments[1][0].clone());
        for at in [0, 1] {
            let leave = left_out.as_deref().filter(|_| at == 1);
            let result = shard_result(&group, at, "success", "passed", leave);
            end_shard(
                &c,
                &files,
                &placed[&group.shards[at]],
                Outcome::Succeeded,
                &result,
            )
            .await;
        }
        // Every attempt ended and is collected, but the job has no outcome
        // yet: the run is not settled, as a whole. A shard alone is.
        let before = c.store().completion(target.clone()).unwrap();
        assert!(before.stop_confirmed() && !before.settled());
        assert_eq!(before.exit_code(), None);
        let shard = Target::Task(ids[&group.shards[0]].clone());
        assert_eq!(
            c.store().completion(shard.clone()).unwrap().exit_code(),
            Some(0)
        );
        assert_eq!(c.store().ci_waiting_runs(8).unwrap(), vec![run.clone()]);

        let pass = ci_advance(&fx, &c);
        assert_eq!(pass.joined, vec![group.id.clone()]);
        assert!(ci_advance(&fx, &c).joined.is_empty(), "joined once");
        assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());
        let after = c.store().completion(target.clone()).unwrap();
        assert!(after.settled());
        let rows = c.store().ci_run_tasks(&run).unwrap();
        assert!(rows.iter().all(|row| row.spec["ci"]["joined"] == true));
        let joined: Vec<_> = c
            .store()
            .events_after(0, 1000)
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == "ci_job_joined")
            .collect();
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].payload["job"], group.id.as_str());
        if omit {
            assert_eq!(after.exit_code(), Some(1));
            assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Failure);
            let failure = after.first_compute_failure.clone().unwrap();
            assert_eq!(failure.task_id, *ids.values().min().unwrap());
            let why = rows[0].spec["ci"]["join_failure"].as_str().unwrap();
            assert!(why.contains("not reported"), "{why}");
            assert_eq!(joined[0].payload["failure"], why);
            // The shard itself passed.
            assert_eq!(c.store().completion(shard).unwrap().exit_code(), Some(0));

            // A retry of the run: every attempt succeeded, yet the shard
            // that did not report all of its tests runs again. The other,
            // which passed on its own, is kept.
            let of = |at: usize| {
                let rows = c.store().ci_run_tasks(&run).unwrap();
                let row = rows
                    .into_iter()
                    .find(|r| r.task_id == ids[&group.shards[at]]);
                row.unwrap()
            };
            assert!(of(0).spec["ci"].get("rerun").is_none());
            assert_eq!(of(1).spec["ci"]["rerun"], true);
            let prepared = match c.store().prepare_retry(
                target.clone(),
                after.control_revision,
                "again".into(),
            ) {
                Ok(belt::storage::RetryPreparation::Prepared(p)) => p,
                Ok(_) => panic!("a first request is no replay"),
                Err(e) => panic!("{e}"),
            };
            let receipt = c.store().commit_retry(prepared, false, wall_ms()).unwrap();
            assert_eq!(receipt.task_count, 1);
            assert!(of(0).attempt.is_some() && of(1).attempt.is_none());
            for at in [0, 1] {
                let ci = &of(at).spec["ci"];
                assert!(ci.get("joined").is_none() && ci.get("join_failure").is_none());
                assert!(ci.get("rerun").is_none());
            }
            assert!(!c.store().completion(target.clone()).unwrap().settled());
            // An outcome that was read before the retry is not written over
            // the shard that runs again.
            let stale = c
                .store()
                .join_ci_shards(&run, &group.id, Some("stale"), &[], wall_ms())
                .unwrap();
            assert!(!stale);
            assert!(of(0).spec["ci"].get("joined").is_none());
            assert!(ci_advance(&fx, &c).joined.is_empty());
            let again = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
            assert_eq!(again.keys().collect::<Vec<_>>(), [&group.shards[1]]);
            let result = shard_result(&group, 1, "success", "passed", None);
            end_shard(
                &c,
                &files,
                &again[&group.shards[1]],
                Outcome::Succeeded,
                &result,
            )
            .await;
            assert_eq!(ci_advance(&fx, &c).joined, vec![group.id.clone()]);
            let done = c.store().completion(target.clone()).unwrap();
            assert_eq!(done.exit_code(), Some(0));
            assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Success);
            assert!(of(1).spec["ci"].get("rerun").is_none());
            continue;
        } else {
            assert_eq!(after.exit_code(), Some(0));
            assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Success);
            assert!(rows
                .iter()
                .all(|row| row.spec["ci"].get("join_failure").is_none()));
            assert!(joined[0].payload["failure"].is_null());
        }
    }

    // A run that can never go on does not wait for its split job either.
    let fx = fx();
    let (c, files) = fx.open();
    let whole = ci_plan(only);
    let shards = partition_tests(&adapter, &discovered, &History::default(), 2, &opts);
    let plan = whole
        .with_shards(&[split_request(&whole, shards, &discovered, false)])
        .unwrap();
    let group = split_group(&plan);
    let (run, _ids) = ci_run_by_task(&fx, &c, &plan).await;
    let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
    for at in [0, 1] {
        let result = shard_result(&group, at, "success", "passed", None);
        end_shard(
            &c,
            &files,
            &placed[&group.shards[at]],
            Outcome::Succeeded,
            &result,
        )
        .await;
    }
    let rows = c.store().ci_run_tasks(&run).unwrap();
    let digest = Digest::parse(rows[0].spec["plan_digest"].as_str().unwrap()).unwrap();
    std::fs::remove_file(belt::worker::ci_task::plan_path(fx.dir.path(), &digest)).unwrap();
    let pass = ci_advance(&fx, &c);
    assert_eq!(pass.failed.len(), 1, "{pass:?}");
    let after = c.store().completion(Target::Run(run.clone())).unwrap();
    assert!(after.settled());
    assert_eq!(after.exit_code(), Some(1));
    // The plan is gone, so no line of the job says it: the run does.
    assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Failure);
    assert!(c.store().ci_waiting_runs(8).unwrap().is_empty());
}

/// A41: what a run failed of is the shard that failed, not a shard that
/// fail-fast only stopped, wherever their ids sort. When the failed shard's
/// attempt succeeded and only its tests failed, the run is a failure of the
/// job, and a retry of the run runs that shard again with the stopped ones
/// (it would otherwise stop them again for ever).
#[tokio::test]
async fn a41_a_run_fails_of_the_failed_shard_and_a_retry_runs_a_shard_whose_tests_failed_again() {
    use belt::service::completion::Target;
    let discovered = listed(&["a", "b", "c", "d", "e", "f"]);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let only = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n";
    for tests_only in [false, true] {
        let fx = fx();
        let (c, files) = fx.open();
        let whole = ci_plan(only);
        let shards = partition_tests(&adapter, &discovered, &History::default(), 3, &opts);
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, true)])
            .unwrap();
        let group = split_group(&plan);
        let (run, ids) = ci_run_by_task(&fx, &c, &plan).await;
        let target = Target::Run(run.clone());
        let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
        // The shard that fails is the one whose task id sorts last: the
        // stopped ones are read first.
        let at = (0..3)
            .max_by_key(|at| ids[&group.shards[*at]].clone())
            .unwrap();
        let failing = ids[&group.shards[at]].clone();
        let (outcome, job) = if tests_only {
            (Outcome::Succeeded, "success")
        } else {
            (Outcome::Failed, "failure")
        };
        let failed = shard_result(&group, at, job, "failed", None);
        end_shard(&c, &files, &placed[&group.shards[at]], outcome, &failed).await;
        assert_eq!(ci_advance(&fx, &c).stopped.len(), 2);
        let mut joined = false;
        for _ in 0..4 {
            c.tick().await.unwrap();
            collect(&c, &files).await;
            joined |= !ci_advance(&fx, &c).joined.is_empty();
        }
        assert!(joined);
        let snapshot = c.store().completion(target.clone()).unwrap();
        assert!(snapshot.settled());
        let own = c.store().completion(Target::Task(failing.clone())).unwrap();
        if !tests_only {
            // The exit code of the attempt that failed.
            let failure = snapshot.first_compute_failure.clone().unwrap();
            assert_eq!(failure.task_id, failing);
            assert_eq!(snapshot.exit_code(), own.exit_code());
            assert!(own.exit_code().is_some_and(|code| code != 0));
            continue;
        }
        assert_eq!(own.exit_code(), Some(0));
        assert_eq!(snapshot.exit_code(), Some(1));

        let prepared =
            match c
                .store()
                .prepare_retry(target.clone(), snapshot.control_revision, "again".into())
            {
                Ok(belt::storage::RetryPreparation::Prepared(p)) => p,
                Ok(_) => panic!("a first request is no replay"),
                Err(e) => panic!("{e}"),
            };
        let receipt = c.store().commit_retry(prepared, false, wall_ms()).unwrap();
        assert_eq!(receipt.task_count, 3, "the failed shard too");
        let pass = ci_advance(&fx, &c);
        assert!(
            pass.stopped.is_empty(),
            "the failure does not stand any more"
        );
        let again = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
        assert_eq!(again.len(), 3);
        for (n, id) in group.shards.iter().enumerate() {
            let result = shard_result(&group, n, "success", "passed", None);
            end_shard(&c, &files, &again[id], Outcome::Succeeded, &result).await;
        }
        let pass = ci_advance(&fx, &c);
        assert_eq!(pass.joined, vec![group.id.clone()]);
        assert!(pass.stopped.is_empty());
        let done = c.store().completion(target).unwrap();
        assert_eq!(done.exit_code(), Some(0));
    }
}

/// [`ci_run_by_task`], with the order of the task ids chosen: a task of the
/// plan with a larger `rank` has a larger task id (those of one rank follow
/// the order of the plan).
async fn ci_run_ranked(
    fx: &Fx,
    c: &Coordinator,
    plan: &local_ci::FrozenPlan,
    rank: impl Fn(&str) -> usize,
) -> (RunId, BTreeMap<String, TaskId>) {
    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), plan).unwrap();
    let manifest = Digest::of_bytes(b"manifest");
    let none = Default::default();
    let specs = belt::service::ci_run::task_specs(plan, &digest, &manifest, &none, None).unwrap();
    let (run, source) = (RunId::generate(), SourceId::generate());
    c.store().put_source_ref(&source, &manifest, 1).unwrap();
    let mut sorted: Vec<TaskId> = specs.iter().map(|_| TaskId::generate()).collect();
    sorted.sort();
    let mut order: Vec<usize> = (0..specs.len()).collect();
    order.sort_by_key(|at| rank(&specs[*at].0));
    let mut of = BTreeMap::new();
    for (id, at) in sorted.into_iter().zip(order) {
        of.insert(at, id);
    }
    let mut ids = BTreeMap::new();
    let mut tasks = Vec::new();
    for (at, (plan_task, spec)) in specs.into_iter().enumerate() {
        ids.insert(plan_task, of[&at].clone());
        tasks.push((of[&at].clone(), spec));
    }
    c.enqueue(run.clone(), source, tasks).await.unwrap();
    (run, ids)
}

/// Tick, collect and advance until the split job of the run is joined.
async fn until_joined(fx: &Fx, c: &Coordinator, files: &Arc<ResultStore>) {
    for _ in 0..40 {
        c.tick().await.unwrap();
        collect(c, files).await;
        if !ci_advance(fx, c).joined.is_empty() {
            return;
        }
    }
    panic!("the split job was never joined");
}

/// A41, retry of a run with more tasks than one page of the scan of a
/// retry: the shard that did not pass on its own (its attempt succeeded,
/// a test of it failed) has the largest task id, behind a full page of
/// shards that are retried before it is read. It runs again with them.
#[tokio::test]
async fn a41_a_retry_runs_a_shard_that_did_not_pass_again_behind_a_page_of_other_shards() {
    use belt::service::completion::Target;
    let names: Vec<String> = (0..70).map(|n| format!("t{n:02}")).collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let discovered = listed(&names);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let only = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n";
    let fx = fx();
    let (c, files) = fx.open();
    let whole = ci_plan(only);
    let shards = partition_tests(&adapter, &discovered, &History::default(), 70, &opts);
    let plan = whole
        .with_shards(&[split_request(&whole, shards, &discovered, true)])
        .unwrap();
    let group = split_group(&plan);
    assert_eq!(group.shards.len(), 70, "more than one page of the scan");
    // The first shard of the plan is placed first, and its id sorts last.
    let last = group.shards[0].clone();
    let (run, ids) = ci_run_ranked(&fx, &c, &plan, |task| usize::from(task == last)).await;
    assert_eq!(ids.values().max(), Some(&ids[&last]));
    let target = Target::Run(run.clone());
    let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
    let failed = shard_result(&group, 0, "success", "failed", None);
    end_shard(&c, &files, &placed[&last], Outcome::Succeeded, &failed).await;
    assert_eq!(ci_advance(&fx, &c).stopped.len(), 69);
    until_joined(&fx, &c, &files).await;
    let own = c
        .store()
        .completion(Target::Task(ids[&last].clone()))
        .unwrap();
    assert_eq!(own.exit_code(), Some(0), "its attempt succeeded");
    let before = c.store().attempts_of_task(&ids[&last]).unwrap().len();
    let snapshot = c.store().completion(target.clone()).unwrap();
    assert_eq!(snapshot.exit_code(), Some(1));

    let prepared =
        match c
            .store()
            .prepare_retry(target.clone(), snapshot.control_revision, "again".into())
        {
            Ok(belt::storage::RetryPreparation::Prepared(p)) => p,
            Ok(_) => panic!("a first request is no replay"),
            Err(e) => panic!("{e}"),
        };
    let receipt = c.store().commit_retry(prepared, false, wall_ms()).unwrap();
    assert_eq!(receipt.task_count, 70, "the shard on the later page too");
    // It is queued first again, and gets a new attempt.
    let again = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
    assert!(again.contains_key(&last));
    let after = c.store().attempts_of_task(&ids[&last]).unwrap().len();
    assert_eq!(after, before + 1);
}

/// A41: the run fails of the shard that failed, and reports its exit code,
/// although a shard that fail-fast stopped and a task of another job that
/// was cancelled on its own both have smaller task ids, in either order.
#[tokio::test]
async fn a41_a_run_fails_of_the_failed_shard_and_not_of_a_cancelled_task_of_another_job() {
    use belt::service::completion::Target;
    let discovered = listed(&["a", "b", "c", "d", "e", "f"]);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let workflow = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n  other:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo other\n";
    for other_first in [false, true] {
        let fx = fx();
        let (c, files) = fx.open();
        let whole = ci_plan(workflow);
        let shards = partition_tests(&adapter, &discovered, &History::default(), 3, &opts);
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, true)])
            .unwrap();
        let group = split_group(&plan);
        let other = ci_task_of(&plan, "other");
        let failing_task = group.shards[2].clone();
        let rank = |task: &str| {
            if task == failing_task {
                3
            } else if task == other {
                if other_first {
                    0
                } else {
                    2
                }
            } else {
                1
            }
        };
        let (run, ids) = ci_run_ranked(&fx, &c, &plan, rank).await;
        let failing = ids[&failing_task].clone();
        let mut sorted: Vec<&TaskId> = ids.values().collect();
        sorted.sort();
        assert_eq!(sorted[3], &failing);
        assert_eq!(sorted[if other_first { 0 } else { 2 }], &ids[&other]);
        let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
        assert_eq!(placed.len(), 4);
        // The task of the other job is cancelled on its own, and stops.
        c.cancel_task(&ids[&other]).await.unwrap();
        for _ in 0..4 {
            c.tick().await.unwrap();
            collect(&c, &files).await;
        }
        let rec = c.store().get_attempt(&placed[&other].0).unwrap().unwrap();
        let ended = rec.termination.map(|t| t.outcome());
        assert_eq!(ended, Some(Outcome::Cancelled));
        let failed = shard_result(&group, 2, "failure", "failed", None);
        let at = &placed[&failing_task];
        end_shard(&c, &files, at, Outcome::Failed, &failed).await;
        assert_eq!(ci_advance(&fx, &c).stopped.len(), 2);
        until_joined(&fx, &c, &files).await;
        let snapshot = c.store().completion(Target::Run(run)).unwrap();
        assert!(snapshot.settled());
        let own = c.store().completion(Target::Task(failing.clone())).unwrap();
        let failure = snapshot.first_compute_failure.clone().unwrap();
        assert_eq!(failure.task_id, failing, "other first: {other_first}");
        assert!(own.exit_code().is_some_and(|code| code != 0));
        assert_ne!(own.exit_code(), Some(belt::error::exit::CANCELLED));
        assert_eq!(snapshot.exit_code(), own.exit_code());
    }
}

/// A41: a run that was cancelled as a whole while a shard of it still ran
/// is cancelled, whatever a shard had found before: a shard with a failed
/// test (its attempt failed of it, or succeeded) makes its job a failure,
/// and the run still ends as cancelled (its exit code and the failure it
/// names say the same), wherever that shard sorts. A cancel that comes when
/// fail-fast had stopped the rest and the run had failed changes nothing.
#[tokio::test]
async fn a41_a_cancelled_run_is_cancelled_although_a_test_of_a_shard_had_failed() {
    use belt::service::ci_run::Part;
    use belt::service::completion::Target;
    let cancelled = belt::error::exit::CANCELLED;
    let discovered = listed(&["a", "b", "c", "d", "e", "f"]);
    let adapter = CargoTest::new("1.91.0", &[], Safety::default());
    let opts = PartitionOpts::default();
    let only = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: cargo test\n";
    for case in 0..8 {
        let (failed_last, attempt_failed, late) = (case & 1 != 0, case & 2 != 0, case & 4 != 0);
        let said = format!("last {failed_last}, attempt {attempt_failed}, late {late}");
        let fx = fx();
        let (c, files) = fx.open();
        let whole = ci_plan(only);
        let shards = partition_tests(&adapter, &discovered, &History::default(), 3, &opts);
        // Only fail-fast ends the run before the cancel comes late.
        let plan = whole
            .with_shards(&[split_request(&whole, shards, &discovered, late)])
            .unwrap();
        let group = split_group(&plan);
        let failed_task = group.shards[1].clone();
        let rank = |task: &str| match (task == failed_task, failed_last) {
            (true, true) => 2,
            (true, false) => 0,
            _ => 1,
        };
        let (run, ids) = ci_run_ranked(&fx, &c, &plan, rank).await;
        let edge = if failed_last {
            ids.values().max()
        } else {
            ids.values().min()
        };
        assert_eq!(edge, Some(&ids[&failed_task]));
        let target = Target::Run(run.clone());
        let placed = placed_by_task(&[&fx.w], &c.tick().await.unwrap().placed);
        let (outcome, job) = if attempt_failed {
            (Outcome::Failed, "failure")
        } else {
            (Outcome::Succeeded, "success")
        };
        let failed = shard_result(&group, 1, job, "failed", None);
        let at = &placed[&failed_task];
        end_shard(&c, &files, at, outcome, &failed).await;
        let stopped = ci_advance(&fx, &c).stopped.len();
        assert_eq!(stopped, if late { 2 } else { 0 }, "{said}");
        if late {
            until_joined(&fx, &c, &files).await;
            let before = c.store().completion(target.clone()).unwrap();
            assert!(before.settled(), "{said}");
            let own = c
                .store()
                .completion(Target::Task(ids[&failed_task].clone()))
                .unwrap();
            let code = if attempt_failed {
                own.exit_code()
            } else {
                Some(1)
            };
            assert_eq!(before.exit_code(), code, "{said}");
            // The cancel is of a later millisecond than the fail-fast stop.
            tokio::time::sleep(Duration::from_millis(3)).await;
            c.cancel_run(run.clone()).await.unwrap();
            let after = c.store().completion(target).unwrap();
            assert_eq!(after.exit_code(), code, "{said}");
            // A cancel that stopped nothing: the status is the failure.
            let status = ci_computed(&fx, &c, &files, &run);
            assert_eq!(status, Computed::Failure, "{said}");
            continue;
        }
        c.cancel_run(run.clone()).await.unwrap();
        until_joined(&fx, &c, &files).await;
        let snapshot = c.store().completion(target).unwrap();
        assert!(snapshot.settled() && snapshot.cancel_requested, "{said}");
        assert_eq!(snapshot.exit_code(), Some(cancelled), "{said}");
        let failure = snapshot.first_compute_failure.clone().unwrap();
        assert_eq!(failure.exit_code, cancelled, "{said}");
        assert_ne!(failure.task_id, ids[&failed_task], "{said}");
        // The job is still a failure of its own, and says so.
        let lines = ci_lines(&fx, &c, &files, &run);
        let job = lines.iter().find(|l| l.part == Some(Part::Job)).unwrap();
        assert_eq!(job.status, "failure", "{said}");
        // Yet the run is cancelled: its status is no failure and no success.
        let status = ci_computed(&fx, &c, &files, &run);
        assert_eq!(status, Computed::Cancelled, "{said}");
    }
}

/// A27. Drive the durable queue, worker receipts and local-ci matrix driver,
/// including replay between settlements. The job's outputs follow variant
/// order even when collection completes in the opposite order.
#[tokio::test]
async fn a27_matrix_permits_join_and_fail_fast_across_workers() {
    let workflow = r#"on: push
jobs:
  leg:
    runs-on: ubuntu-latest
    strategy:
      max-parallel: 2
      fail-fast: true
      matrix:
        n: [0, 1, 2]
    outputs:
      value: ${{ steps.out.outputs.value }}
    steps:
      - id: out
        run: echo "value=${{ matrix.n }}" >> "$GITHUB_OUTPUT"
  report:
    if: always()
    needs: leg
    runs-on: ubuntu-latest
    steps:
      - run: echo "${{ needs.leg.outputs.value }}"
"#;
    for fail in [false, true] {
        let fx = fx();
        let (c, files) = fx.open();
        let other = Worker::new("w-b");
        c.try_attach(other.id.clone(), Arc::clone(&other) as Arc<dyn WorkerLink>)
            .unwrap();
        let plan = ci_plan(workflow);
        let variants: Vec<_> = plan.tasks.iter().filter(|t| t.variant.is_some()).collect();
        assert_eq!(variants.len(), 3);
        let (_, ids) = ci_run_by_task(&fx, &c, &plan).await;
        let pass = ci_advance(&fx, &c);
        assert_eq!(pass.released.len(), 2, "{pass:?}");
        let placements = placed_by_task(&[&fx.w, &other], &c.tick().await.unwrap().placed);
        assert_eq!(placements.len(), 2);
        let group = &variants[0].variant.as_ref().unwrap().group;
        for (a, w) in placements.values() {
            assert!(w.payload(a).permit_id.is_some());
        }
        let result = |t: &local_ci::dag::ExecutionTask, success: bool| {
            let index = t.variant.as_ref().unwrap().index;
            serde_json::to_vec(&json!({"schema_version":1,"task":t.id,"jobs":[{
                "workflow":t.workflow,"job":"leg","display":"leg","result":if success{"pass"}else{"fail"},"secs":0,"steps":[]
            }],"outcomes":[{"job":"leg","variant":index,"status":if success{"success"}else{"failure"},
                "outputs":{"value":index.to_string()},"skipped":null,"ancestor_failed":false}],
                "receipt_ignored":null,"artifacts":[]})).unwrap()
        };
        assert!(ci_advance(&fx, &c).released.is_empty());
        end_shard(
            &c,
            &files,
            &placements[&variants[1].id],
            if fail {
                Outcome::Failed
            } else {
                Outcome::Succeeded
            },
            &result(variants[1], !fail),
        )
        .await;
        let pass = ci_advance(&fx, &c);
        assert!(pass.faults.is_empty() && pass.failed.is_empty(), "{pass:?}");
        if fail {
            assert!(pass.released.is_empty());
            assert!(pass.stopped.contains(&ids[&variants[0].id]), "{pass:?}");
            assert!(pass.stopped.contains(&ids[&variants[2].id]), "{pass:?}");
            assert!(c
                .store()
                .attempts_of_task(&ids[&variants[2].id])
                .unwrap()
                .is_empty());
            continue;
        }
        assert_eq!(pass.released, [ids[&variants[2].id].clone()]);
        let more = placed_by_task(&[&fx.w, &other], &c.tick().await.unwrap().placed);
        end_shard(
            &c,
            &files,
            &more[&variants[2].id],
            Outcome::Succeeded,
            &result(variants[2], true),
        )
        .await;
        assert!(ci_advance(&fx, &c).released.is_empty());
        end_shard(
            &c,
            &files,
            &placements[&variants[0].id],
            Outcome::Succeeded,
            &result(variants[0], true),
        )
        .await;
        let pass = ci_advance(&fx, &c);
        assert_eq!(pass.joined.as_slice(), std::slice::from_ref(group));
        let next = ci_task_of(&plan, "report");
        assert_eq!(pass.released, [ids[&next].clone()]);
        let last = placed_by_task(&[&fx.w, &other], &c.tick().await.unwrap().placed);
        let (a, w) = &last[&next];
        assert_eq!(
            w.payload(a).ci_task.unwrap().inputs["needs"]["leg"]["outputs"]["value"],
            "2"
        );
        assert!(ci_advance(&fx, &c).released.is_empty());
    }
}

#[tokio::test]
async fn a27_dynamic_members_are_persisted_once_and_join_after_verified_receipts() {
    let workflow = r#"on: push
jobs:
  leg:
    runs-on: ubuntu-latest
    strategy:
      max-parallel: 1
      matrix: ${{ fromJSON('{"include":[{"n":0},{"n":1}]}') }}
    steps:
      - run: echo ${{ matrix.n }}
"#;
    let fx = fx();
    let (c, files) = fx.open();
    let run = RunId::generate();
    let plan = ci_plan_at(workflow, fx.dir.path(), &run);
    assert!(plan.tasks.iter().any(|t| t.dynamic_matrix));
    let (_, ids) = ci_run_by_id(&fx, &c, &plan, run.clone()).await;
    let origin = ci_task_of(&plan, "leg");
    let pass = ci_advance(&fx, &c);
    assert!(pass.failed.is_empty() && pass.faults.is_empty(), "{pass:?}");
    assert_eq!(c.store().ci_run_tasks(&run).unwrap().len(), 3);
    for index in 0..2 {
        let pass = ci_advance(&fx, &c);
        assert_eq!(pass.released.len(), 1, "{pass:?}");
        let report = c.tick().await.unwrap();
        assert_eq!(report.placed.len(), 1, "{report:?}");
        let attempt = &report.placed[0];
        let sent = fx.w.payload(attempt);
        let ci = sent.ci_task.as_ref().unwrap();
        let dispatch = ci.dispatch.as_ref().unwrap();
        assert_eq!(dispatch["origin"], origin);
        assert_eq!(dispatch["task"], ci.task_id);
        assert_eq!(dispatch["index"], index);
        assert!(sent.permit_id.is_some());
        let receipt=serde_json::to_vec(&json!({"schema_version":1,"task":ci.task_id,"jobs":[{
            "workflow":plan.tasks[0].workflow,"job":"leg","display":"leg","result":"pass","secs":0,"steps":[]
        }],"outcomes":[{"job":"leg","variant":index,"status":"success","outputs":{},"skipped":null,"ancestor_failed":false}],
        "receipt_ignored":null,"artifacts":[]})).unwrap();
        assert!(ci_advance(&fx, &c).released.is_empty());
        end(
            &fx,
            &c,
            attempt,
            Outcome::Succeeded,
            ci_out(attempt, &sent, &receipt),
        )
        .await;
        ci_collect(&c, &files, attempt).await;
    }
    let pass = ci_advance(&fx, &c);
    assert!(pass.joined.contains(&origin), "{pass:?}");
    assert_eq!(c.store().ci_run_tasks(&run).unwrap().len(), 3);
    assert!(c
        .store()
        .attempts_of_task(&ids[&origin])
        .unwrap()
        .is_empty());
    assert_eq!(
        c.store()
            .completion(belt::service::completion::Target::Run(run))
            .unwrap()
            .exit_code(),
        Some(0)
    );
}

/// The parent starts from the unpartitioned job. A separate discovery
/// attempt yields a collected listing and only then do the two shards
/// become eligible. Repeated progression cannot create a second copy.
#[tokio::test]
async fn a39_discovery_collection_expands_the_run_once_before_any_test_is_placed() {
    use belt::ci::discovery::{candidates, stage, WorkerListing};
    let fx = fx();
    let (c, files) = fx.open();
    let whole =
        ci_plan(&CI_CHAIN.replace("- run: echo one\n      - run: echo two", "- run: npx jest"));
    let build = ci_task_of(&whole, "build");
    let test = ci_task_of(&whole, "test");
    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), &whole).unwrap();
    let manifest = Digest::of_bytes(b"manifest");
    let mut specs =
        belt::service::ci_run::task_specs(&whole, &digest, &manifest, &Default::default(), None)
            .unwrap();
    stage(&whole, &mut specs, 2, 2, "example/project", &[]);
    let run = RunId::generate();
    let source = SourceId::generate();
    c.store().put_source_ref(&source, &manifest, 1).unwrap();
    let steps = candidates(&whole);
    let discovery = steps[0].task_id();
    let probe = specs
        .iter()
        .find(|(id, _)| id == &discovery)
        .and_then(|(_, spec)| spec.ci.as_ref())
        .unwrap();
    assert_eq!(probe.steps, 2, "runner version and test listing");
    assert_eq!(probe.history_keys.len(), 2);
    assert!(probe.history_keys.iter().all(Option::is_some));
    let job = specs
        .iter()
        .find(|(id, _)| id == &test)
        .and_then(|(_, spec)| spec.ci.as_ref())
        .unwrap();
    for key in &probe.history_keys {
        assert!(!job.history_keys.contains(key));
    }
    let task_id = TaskId::generate();
    c.enqueue(
        run.clone(),
        source,
        specs
            .into_iter()
            .map(|(name, spec)| {
                (
                    if name == test {
                        task_id.clone()
                    } else {
                        TaskId::generate()
                    },
                    spec,
                )
            })
            .collect(),
    )
    .await
    .unwrap();
    let placed = c.tick().await.unwrap().placed;
    assert_eq!(placed.len(), 1);
    assert_eq!(fx.w.payload(&placed[0]).ci_task.unwrap().task_id, build);
    let result = ci_result(&build, "build", "success", json!({"version":"1.2.3"}));
    end_shard(
        &c,
        &files,
        &(placed[0].clone(), fx.w.clone()),
        Outcome::Succeeded,
        &result,
    )
    .await;
    let pass = ci_advance(&fx, &c);
    assert!(pass.faults.is_empty() && pass.failed.is_empty(), "{pass:?}");
    let placed = c.tick().await.unwrap().placed;
    assert_eq!(placed.len(), 1);
    assert_eq!(fx.w.payload(&placed[0]).ci_task.unwrap().task_id, discovery);
    assert_eq!(
        fx.w.payload(&placed[0]).ci_task.unwrap().inputs["needs"]["build"]["outputs"]["version"],
        "1.2.3"
    );
    assert!(c.store().attempts_of_task(&task_id).unwrap().is_empty());
    let listing = WorkerListing {
        setup_ms: None,
        step: steps[0].clone(),
        runner_version: "30.2".into(),
        discovered: listed(&["a.js", "b.js", "c.js", "d.js"]),
    };
    end_shard(
        &c,
        &files,
        &(placed[0].clone(), fx.w.clone()),
        Outcome::Succeeded,
        &serde_json::to_vec(&listing).unwrap(),
    )
    .await;
    for _ in 0..3 {
        let pass = ci_advance(&fx, &c);
        assert!(pass.faults.is_empty() && pass.failed.is_empty(), "{pass:?}");
    }
    let rows = c.store().ci_run_tasks(&run).unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter()
            .filter(|r| r.spec["ci"]["shard_group"].is_string())
            .count(),
        2
    );
    let placed = c.tick().await.unwrap().placed;
    assert_eq!(placed.len(), 2);
    assert_eq!(c.store().attempts_of_task(&task_id).unwrap().len(), 1);
    for attempt in &placed {
        assert!(fx.w.payload(attempt).ci_task.unwrap().verify_shard_version);
        assert_eq!(
            fx.w.payload(attempt).ci_task.unwrap().inputs["needs"]["build"]["outputs"]["version"],
            "1.2.3"
        );
    }
    let changed = fx.w.payload(&placed[0]).ci_task.unwrap().plan_digest;
    assert_ne!(changed, digest);
    let bytes = belt::ci::advance::read_plan(fx.dir.path(), &changed).unwrap();
    let split: local_ci::FrozenPlan = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(split_group(&split).manifest, listing.discovered.tests);
}

#[tokio::test]
async fn a38_live_artifact_transfer_resumes_before_either_worker_finishes() {
    let fx = fx();
    let (mut c, files) = fx.open();
    let other = Worker::new("w-b");
    fx.w.set(|s| s.one_slot = true);
    other.set(|s| s.one_slot = true);
    c.try_attach(other.id.clone(), Arc::clone(&other) as Arc<dyn WorkerLink>)
        .unwrap();
    let workflow = r#"on: push
jobs:
  produce:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
        with:
          name: package
          path: result.dat
          if-no-files-found: error
      - run: echo still-running
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: package
"#;
    let plan = ci_plan(workflow);
    // Separate admission keeps each live job on a known worker without
    // introducing a dependency on the producer's terminal result.
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    let first = &ids["produce"];
    let second = &ids["consume"];
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2);
    let producer = c.store().attempts_of_task(first).unwrap()[0]
        .attempt_id
        .clone();
    let consumer = c.store().attempts_of_task(second).unwrap()[0]
        .attempt_id
        .clone();
    let worker_of = |id: &AttemptId| {
        if fx.w.state.lock().unwrap().jobs.contains_key(id) {
            fx.w.clone()
        } else {
            other.clone()
        }
    };
    let up = worker_of(&producer);
    let down = worker_of(&consumer);
    assert_ne!(up.id, down.id);
    let contents = vec![b'c'; 128 * 1024 + 17];
    let digest = Digest::of_bytes(&contents);
    let logical = &plan
        .tasks
        .iter()
        .find(|t| t.jobs.iter().any(|j| j == "produce"))
        .unwrap()
        .workflow;
    let upload = ArtifactAction {
        key: Digest::of_bytes(b"upload"),
        intent: ArtifactIntent::Upload {
            overwrite: false,
            receipt: json!({
                "name":"package","version":1,"files":[{"path":"result.dat","size":contents.len(),"sha256":digest.as_str().strip_prefix("sha256:").unwrap()}],
                "plan_digest":plan.digest,"attempt_id":"run","job":format!("{logical}::produce"),"display":"produce","task":null,"retention_days":null,
            }),
        },
    };
    up.set(|s| {
        s.artifact_data.insert(digest.clone(), contents.clone());
        s.pending_artifacts.insert(producer.clone(), upload.clone());
    });
    c.tick().await.unwrap();
    assert!(c
        .store()
        .workflow_artifacts(&run, logical)
        .unwrap()
        .is_empty());
    assert_eq!(up.offsets("workflow_read"), [0]);
    let catalog = ArtifactAction {
        key: Digest::of_bytes(b"catalog"),
        intent: ArtifactIntent::Catalog {
            name: Some("package".into()),
            pattern: None,
        },
    };
    down.set(|s| {
        s.pending_artifacts
            .insert(consumer.clone(), catalog.clone());
    });
    drop(c);
    drop(files);
    (c, _) = fx.open();
    c.try_attach(other.id.clone(), Arc::clone(&other) as Arc<dyn WorkerLink>)
        .unwrap();
    c.tick().await.unwrap();
    assert!(
        !down
            .state
            .lock()
            .unwrap()
            .artifact_answers
            .contains_key(&catalog.key),
        "the upload's last chunk has arrived but the parent has not published the receipt"
    );
    c.tick().await.unwrap();
    assert_eq!(up.offsets("workflow_read"), [0, 128 * 1024]);
    assert_eq!(
        up.state.lock().unwrap().artifact_answers[&upload.key],
        ArtifactAnswer::Uploaded { version: 1 }
    );
    c.tick().await.unwrap();
    assert_eq!(
        c.store().workflow_artifacts(&run, logical).unwrap().len(),
        1
    );
    up.set(|s| {
        s.pending_artifacts.remove(&producer);
    });
    c.tick().await.unwrap();
    assert!(
        matches!(&down.state.lock().unwrap().artifact_answers[&catalog.key],ArtifactAnswer::Catalog{artifacts} if artifacts.len()==1)
    );
    let fetch = ArtifactAction {
        key: Digest::of_bytes(b"download"),
        intent: ArtifactIntent::Fetch {
            producer: producer.clone(),
            name: "package".into(),
            version: 1,
            digest,
            offset: 128 * 1024,
        },
    };
    down.set(|s| {
        s.pending_artifacts.insert(consumer, fetch.clone());
    });
    c.tick().await.unwrap();
    assert_eq!(
        down.state.lock().unwrap().artifact_answers[&fetch.key],
        ArtifactAnswer::Chunk {
            data: Bytes(vec![b'c'; 17])
        }
    );
}

#[tokio::test]
async fn a38_a_failed_artifact_action_fails_its_step_and_the_others_are_still_driven() {
    let fx = fx();
    let (c, files) = fx.open();
    let workflow = r#"on: push
jobs:
  produce:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
        with:
          name: package
          path: result.dat
      - run: echo still-running
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: package
"#;
    let plan = ci_plan(workflow);
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    // Both jobs are live on the one worker: one walk drives them in turn.
    assert_eq!(c.tick().await.unwrap().placed.len(), 2);
    let attempt_of = |job: &str| {
        c.store().attempts_of_task(&ids[job]).unwrap()[0]
            .attempt_id
            .clone()
    };
    let (producer, consumer) = (attempt_of("produce"), attempt_of("consume"));
    let contents = b"seventeen bytes!!".to_vec();
    let digest = Digest::of_bytes(&contents);
    let logical = &plan
        .tasks
        .iter()
        .find(|t| t.jobs.iter().any(|j| j == "produce"))
        .unwrap()
        .workflow;
    let upload_of = |key: &[u8], size: usize, overwrite: bool| ArtifactAction {
        key: Digest::of_bytes(key),
        intent: ArtifactIntent::Upload {
            overwrite,
            receipt: json!({
                "name":"package","version":1,"files":[{"path":"result.dat","size":size,"sha256":digest.as_str().strip_prefix("sha256:").unwrap()}],
                "plan_digest":plan.digest,"attempt_id":"run","job":format!("{logical}::produce"),"display":"produce","task":null,"retention_days":null,
            }),
        },
    };
    let answer_of = |key: &Digest| {
        fx.w.state
            .lock()
            .unwrap()
            .artifact_answers
            .get(key)
            .cloned()
    };
    let upload = upload_of(b"upload", contents.len(), false);
    fx.w.set(|s| {
        s.artifact_data.insert(digest.clone(), contents.clone());
        s.pending_artifacts.insert(producer.clone(), upload.clone());
    });
    for _ in 0..4 {
        c.tick().await.unwrap();
    }
    assert_eq!(
        answer_of(&upload.key),
        Some(ArtifactAnswer::Uploaded { version: 1 })
    );
    assert_eq!(
        c.store().workflow_artifacts(&run, logical).unwrap().len(),
        1
    );
    let published = files.artifact_path(&producer, &digest);
    assert_eq!(std::fs::read(&published).unwrap(), contents);

    // A later declaration of the published digest with another size is
    // refused, and the published file stays as it is.
    let resized = upload_of(b"resized", contents.len() + 1, true);
    fx.w.set(|s| {
        s.pending_artifacts
            .insert(producer.clone(), resized.clone());
    });
    c.tick().await.unwrap();
    assert!(
        matches!(answer_of(&resized.key), Some(ArtifactAnswer::Failed { reason }) if reason.contains("size conflicts")),
        "{:?}",
        answer_of(&resized.key)
    );
    assert_eq!(std::fs::read(&published).unwrap(), contents);
    // So is one declaration that gives a digest two sizes at once.
    let mut twice = upload_of(b"twice", contents.len(), true);
    let ArtifactIntent::Upload { receipt, .. } = &mut twice.intent else {
        unreachable!()
    };
    let mut second = receipt["files"][0].clone();
    second["path"] = json!("copy.dat");
    second["size"] = json!(contents.len() + 1);
    receipt["files"].as_array_mut().unwrap().push(second);
    fx.w.set(|s| {
        s.pending_artifacts.insert(producer.clone(), twice.clone());
    });
    c.tick().await.unwrap();
    assert!(
        matches!(answer_of(&twice.key), Some(ArtifactAnswer::Failed { reason }) if reason.contains("two sizes")),
        "{:?}",
        answer_of(&twice.key)
    );
    assert_eq!(std::fs::read(&published).unwrap(), contents);
    assert_eq!(fx.w.offsets("workflow_read"), [0]);

    // The file is gone from the result store. Whichever of the two attempts
    // the walk reaches first, the fetch of one fails its own step only and
    // the action of the other is answered in the same walk.
    std::fs::remove_file(&published).unwrap();
    for (n, (broken, other)) in [(&producer, &consumer), (&consumer, &producer)]
        .into_iter()
        .enumerate()
    {
        let fetch = ArtifactAction {
            key: Digest::of_bytes(format!("fetch-{n}").as_bytes()),
            intent: ArtifactIntent::Fetch {
                producer: producer.clone(),
                name: "package".into(),
                version: 1,
                digest: digest.clone(),
                offset: 0,
            },
        };
        let catalog = ArtifactAction {
            key: Digest::of_bytes(format!("catalog-{n}").as_bytes()),
            intent: ArtifactIntent::Catalog {
                name: Some("package".into()),
                pattern: None,
            },
        };
        fx.w.set(|s| {
            s.pending_artifacts.insert(broken.clone(), fetch.clone());
            s.pending_artifacts.insert(other.clone(), catalog.clone());
        });
        let report = c.tick().await.unwrap();
        assert!(report.unknown.is_empty(), "{report:?}");
        assert!(
            matches!(answer_of(&fetch.key), Some(ArtifactAnswer::Failed { .. })),
            "{:?}",
            answer_of(&fetch.key)
        );
        assert!(
            matches!(answer_of(&catalog.key), Some(ArtifactAnswer::Catalog { artifacts }) if artifacts.len() == 1),
            "{:?}",
            answer_of(&catalog.key)
        );
    }
}

/// One artifact of one file that the producer of a two-job run has
/// uploaded, with both jobs still live on the one worker.
struct Package {
    c: Coordinator,
    files: Arc<ResultStore>,
    producer: AttemptId,
    consumer: AttemptId,
    contents: Vec<u8>,
    digest: Digest,
    /// The receipt of the upload, to declare again.
    receipt: serde_json::Value,
}

impl Package {
    async fn published(fx: &Fx) -> Package {
        let (c, files) = fx.open();
        let plan = ci_plan(
            r#"on: push
jobs:
  produce:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
        with:
          name: package
          path: result.dat
      - run: echo still-running
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: package
"#,
        );
        let (_, ids) = ci_run(fx, &c, &plan).await;
        assert_eq!(c.tick().await.unwrap().placed.len(), 2);
        let attempt_of = |job: &str| {
            c.store().attempts_of_task(&ids[job]).unwrap()[0]
                .attempt_id
                .clone()
        };
        let (producer, consumer) = (attempt_of("produce"), attempt_of("consume"));
        let contents = b"seventeen bytes!!".to_vec();
        let digest = Digest::of_bytes(&contents);
        let logical = &plan
            .tasks
            .iter()
            .find(|t| t.jobs.iter().any(|j| j == "produce"))
            .unwrap()
            .workflow;
        let receipt = json!({
            "name":"package","version":1,"files":[{"path":"result.dat","size":contents.len(),"sha256":digest.as_str().strip_prefix("sha256:").unwrap()}],
            "plan_digest":plan.digest,"attempt_id":"run","job":format!("{logical}::produce"),"display":"produce","task":null,"retention_days":null,
        });
        let p = Package {
            c,
            files,
            producer,
            consumer,
            contents,
            digest,
            receipt,
        };
        let upload = p.upload(b"upload", p.contents.len());
        fx.w.set(|s| {
            s.artifact_data.insert(p.digest.clone(), p.contents.clone());
            s.pending_artifacts
                .insert(p.producer.clone(), upload.clone());
        });
        for _ in 0..4 {
            p.c.tick().await.unwrap();
        }
        assert_eq!(
            answer_of(fx, &upload),
            Some(ArtifactAnswer::Uploaded { version: 1 })
        );
        assert_eq!(std::fs::read(p.path()).unwrap(), p.contents);
        p
    }

    fn path(&self) -> std::path::PathBuf {
        self.files.artifact_path(&self.producer, &self.digest)
    }

    /// The producer declares the file again as `size` bytes.
    fn upload(&self, key: &[u8], size: usize) -> ArtifactAction {
        let mut receipt = self.receipt.clone();
        receipt["files"][0]["size"] = json!(size);
        ArtifactAction {
            key: Digest::of_bytes(key),
            intent: ArtifactIntent::Upload {
                overwrite: true,
                receipt,
            },
        }
    }

    fn fetch(&self, key: &[u8]) -> ArtifactAction {
        ArtifactAction {
            key: Digest::of_bytes(key),
            intent: ArtifactIntent::Fetch {
                producer: self.producer.clone(),
                name: "package".into(),
                version: 1,
                digest: self.digest.clone(),
                offset: 0,
            },
        }
    }
}

fn answer_of(fx: &Fx, action: &ArtifactAction) -> Option<ArtifactAnswer> {
    let s = fx.w.state.lock().unwrap();
    s.artifact_answers.get(&action.key).cloned()
}

#[tokio::test]
async fn a38_an_artifact_action_that_met_an_io_fault_is_not_answered_and_is_done_by_a_later_poll() {
    let fx = fx();
    let p = Package::published(&fx).await;
    // The directory of the published files cannot be opened: a file is in
    // its place. That says nothing about the artifact.
    let dir = p.path().parent().unwrap().to_owned();
    let aside = dir.with_extension("aside");
    std::fs::rename(&dir, &aside).unwrap();
    std::fs::write(&dir, b"").unwrap();
    let fetch = p.fetch(b"fetch");
    fx.w.set(|s| {
        s.pending_artifacts
            .insert(p.consumer.clone(), fetch.clone());
    });
    for _ in 0..2 {
        let report = p.c.tick().await.unwrap();
        assert!(report.unknown.is_empty(), "{report:?}");
        assert_eq!(answer_of(&fx, &fetch), None);
    }
    std::fs::remove_file(&dir).unwrap();
    std::fs::rename(&aside, &dir).unwrap();
    p.c.tick().await.unwrap();
    assert_eq!(
        answer_of(&fx, &fetch),
        Some(ArtifactAnswer::Chunk {
            data: Bytes(p.contents.clone())
        })
    );
}

#[tokio::test]
async fn a38_a_fetch_of_a_published_file_that_is_gone_or_another_fails_its_step() {
    let fx = fx();
    let p = Package::published(&fx).await;
    let reason_of = |action: &ArtifactAction| match answer_of(&fx, action) {
        Some(ArtifactAnswer::Failed { reason }) => reason,
        other => panic!("{other:?}"),
    };
    // Not the bytes of its receipt: one byte more. The producer asks for
    // nothing any more, so no upload of its puts the file back.
    std::fs::write(p.path(), [&p.contents[..], b"!"].concat()).unwrap();
    let longer = p.fetch(b"longer");
    fx.w.set(|s| {
        s.pending_artifacts.remove(&p.producer);
        s.pending_artifacts
            .insert(p.consumer.clone(), longer.clone());
    });
    p.c.tick().await.unwrap();
    assert!(
        reason_of(&longer).contains("corrupt"),
        "{}",
        reason_of(&longer)
    );
    std::fs::remove_file(p.path()).unwrap();
    let gone = p.fetch(b"gone");
    fx.w.set(|s| {
        s.pending_artifacts.insert(p.consumer.clone(), gone.clone());
    });
    p.c.tick().await.unwrap();
    assert!(
        reason_of(&gone).contains("not found"),
        "{}",
        reason_of(&gone)
    );
}

#[tokio::test]
async fn a38_a_damaged_published_file_is_uploaded_again_and_a_whole_one_is_never_replaced() {
    let fx = fx();
    let p = Package::published(&fx).await;
    assert_eq!(fx.w.offsets("workflow_read"), [0]);
    // The file is whole: a declaration of its digest with another length is
    // refused, and the file is neither removed nor read from the worker.
    let resized = p.upload(b"resized", p.contents.len() + 1);
    fx.w.set(|s| {
        s.pending_artifacts
            .insert(p.producer.clone(), resized.clone());
    });
    p.c.tick().await.unwrap();
    assert!(
        matches!(answer_of(&fx, &resized), Some(ArtifactAnswer::Failed { reason }) if reason.contains("size conflicts")),
        "{:?}",
        answer_of(&fx, &resized)
    );
    assert_eq!(std::fs::read(p.path()).unwrap(), p.contents);
    assert_eq!(fx.w.offsets("workflow_read"), [0]);

    // The file was damaged where it lay and its length changed. It does not
    // stand in the way of the same producer's upload, which replaces it.
    std::fs::write(p.path(), [&p.contents[..], b"!"].concat()).unwrap();
    let again = p.upload(b"again", p.contents.len());
    fx.w.set(|s| {
        s.pending_artifacts
            .insert(p.producer.clone(), again.clone());
    });
    for _ in 0..4 {
        p.c.tick().await.unwrap();
    }
    assert!(
        matches!(
            answer_of(&fx, &again),
            Some(ArtifactAnswer::Uploaded { .. })
        ),
        "{:?}",
        answer_of(&fx, &again)
    );
    assert_eq!(std::fs::read(p.path()).unwrap(), p.contents);
    assert_eq!(fx.w.offsets("workflow_read"), [0, 0]);
}

#[test]
fn a38_refuses_cross_run_artifact_options_before_any_job_can_be_placed() {
    let plan = ci_plan(
        r#"on: push
jobs:
  consume:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          run-id: '123'
"#,
    );
    let err = belt::service::ci_run::task_specs(
        &plan,
        &Digest::of_bytes(b"plan"),
        &Digest::of_bytes(b"manifest"),
        &Default::default(),
        None,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("run-id"), "{}", err.message);
}

#[tokio::test]
async fn discovery_precedes_the_matrix_journal_and_its_shard_timings_survive_replay() {
    use belt::ci::discovery::{candidates, stage, WorkerListing};
    let fx = fx();
    let (c, files) = fx.open();
    let plan = ci_plan(
        r#"on: push
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: npx jest
  variants:
    needs: test
    runs-on: ubuntu-latest
    strategy:
      matrix:
        n: [1, 2]
    steps:
      - run: echo "${{ matrix.n }}"
"#,
    );
    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), &plan).unwrap();
    let manifest = Digest::of_bytes(b"manifest");
    let mut specs =
        belt::service::ci_run::task_specs(&plan, &digest, &manifest, &Default::default(), None)
            .unwrap();
    stage(&plan, &mut specs, 2, 2, "example/project", &[]);
    let run = RunId::generate();
    let source = SourceId::generate();
    c.store().put_source_ref(&source, &manifest, 1).unwrap();
    c.enqueue(
        run.clone(),
        source,
        specs
            .into_iter()
            .map(|(_, s)| (TaskId::generate(), s))
            .collect(),
    )
    .await
    .unwrap();
    let listing = WorkerListing {
        step: candidates(&plan).pop().unwrap(),
        runner_version: "30.2".into(),
        setup_ms: None,
        discovered: listed(&["a.js", "b.js"]),
    };
    assert!(ci_advance(&fx, &c).released.is_empty());
    let placements = c.tick().await.unwrap().placed;
    assert_eq!(placements.len(), 1);
    assert_eq!(
        fx.w.payload(&placements[0]).ci_task.unwrap().task_id,
        listing.step.task_id()
    );
    end_shard(
        &c,
        &files,
        &(placements[0].clone(), fx.w.clone()),
        Outcome::Succeeded,
        &serde_json::to_vec(&listing).unwrap(),
    )
    .await;
    for _ in 0..3 {
        let pass = ci_advance(&fx, &c);
        assert!(pass.faults.is_empty() && pass.failed.is_empty(), "{pass:?}");
    }
    let placements = c.tick().await.unwrap().placed;
    assert_eq!(placements.len(), 2);
    let dag = fx.w.payload(&placements[0]).ci_task.unwrap().plan_digest;
    let split = serde_json::from_slice(&belt::ci::advance::read_plan(fx.dir.path(), &dag).unwrap())
        .unwrap();
    let group = split_group(&split);
    for attempt in placements {
        let id = fx.w.payload(&attempt).ci_task.unwrap().task_id;
        let index = group.shards.iter().position(|s| s == &id).unwrap();
        let mut report: serde_json::Value =
            serde_json::from_slice(&shard_result(&group, index, "success", "passed", None))
                .unwrap();
        report["jobs"] = json!([{
            "workflow": listing.step.workflow, "job": "test", "display": "test",
            "result": "pass", "secs": 0.035, "steps": []
        }]);
        for test in report["shard"]["tests"].as_array_mut().unwrap() {
            test["millis"] = json!(35);
        }
        end_shard(
            &c,
            &files,
            &(attempt, fx.w.clone()),
            Outcome::Succeeded,
            &serde_json::to_vec(&report).unwrap(),
        )
        .await;
    }
    let joined = ci_advance(&fx, &c);
    assert!(joined.joined.contains(&group.id), "{joined:?}");
    assert_eq!(joined.released.len(), 2);
    let timings = c
        .store()
        .ci_test_timings(
            listing.history_key(&plan, "example/project").unwrap(),
            listing.discovered.tests,
        )
        .unwrap();
    assert_eq!(
        timings
            .values()
            .flat_map(|h| h.millis.values())
            .copied()
            .collect::<Vec<_>>(),
        [35, 35]
    );
    let replay = ci_advance(&fx, &c);
    assert!(
        replay.joined.is_empty()
            && replay.released.is_empty()
            && replay.failed.is_empty()
            && replay.faults.is_empty(),
        "{replay:?}"
    );
}

#[tokio::test]
async fn a_discovery_incompatible_worker_runs_the_original_job_after_the_probe_is_rejected() {
    let fx = fx();
    let (c, files) = fx.open();
    fx.w.set(|s| s.no_discovery = true);
    let plan = ci_plan(
        "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: npx jest\n",
    );
    let (digest, _) = belt::ci::advance::keep_plan(fx.dir.path(), &plan).unwrap();
    let manifest = Digest::of_bytes(b"manifest");
    let mut specs =
        belt::service::ci_run::task_specs(&plan, &digest, &manifest, &Default::default(), None)
            .unwrap();
    belt::ci::discovery::stage(&plan, &mut specs, 2, 2, "project", &[]);
    let run = RunId::generate();
    let source = SourceId::generate();
    c.store().put_source_ref(&source, &manifest, 1).unwrap();
    c.enqueue(
        run.clone(),
        source,
        specs
            .into_iter()
            .map(|(_, s)| (TaskId::generate(), s))
            .collect(),
    )
    .await
    .unwrap();
    assert!(c.tick().await.unwrap().placed.is_empty());
    for _ in 0..2 {
        let pass = ci_advance(&fx, &c);
        assert!(pass.failed.is_empty() && pass.faults.is_empty(), "{pass:?}");
    }
    let placed = c.tick().await.unwrap().placed;
    assert_eq!(placed.len(), 1);
    let task = fx.w.payload(&placed[0]).ci_task.unwrap();
    assert_eq!(task.task_id, plan.tasks[0].id);
    assert!(!task.verify_shard_version);
    end_shard(
        &c,
        &files,
        &(placed[0].clone(), fx.w.clone()),
        Outcome::Succeeded,
        &ci_result(&task.task_id, "test", "success", json!({})),
    )
    .await;
    assert_eq!(
        c.store()
            .completion(belt::service::completion::Target::Run(run))
            .unwrap()
            .exit_code(),
        Some(0)
    );
}

/// The worker of a test behind a link that holds its first artifact poll
/// until released.
struct HeldPoll {
    worker: Arc<Worker>,
    hold: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl WorkerLink for HeldPoll {
    fn workflow_artifacts_negotiated(&self) -> bool {
        self.worker.workflow_artifacts_negotiated()
    }
    fn matrix_dispatch_negotiated(&self) -> bool {
        self.worker.matrix_dispatch_negotiated()
    }
    fn ci_discovery_negotiated(&self) -> bool {
        self.worker.ci_discovery_negotiated()
    }
    fn ci_task_negotiated(&self) -> bool {
        self.worker.ci_task_negotiated()
    }
    fn placement_inputs(&self) -> Option<belt::coordinator::PlacementInputs> {
        self.worker.placement_inputs()
    }
    fn input_refusals(&self) -> Vec<(RunId, String)> {
        self.worker.input_refusals()
    }
    fn cost_transport(&self) -> Option<belt::scheduler::measured::Transport> {
        self.worker.cost_transport()
    }
    fn call(
        &self,
        request: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            let poll = matches!(
                &request,
                Request::WorkflowArtifact(r) if matches!(r.command, WorkflowArtifactCommand::Poll)
            );
            if poll && self.hold.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            self.worker.call(request).await
        })
    }
}

/// The uploads the parent reserved files for.
fn reserved_uploads(fx: &Fx) -> i64 {
    let db = rusqlite::Connection::open_with_flags(
        fx.dir.path().join("coordinator.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    db.query_row("SELECT count(*) FROM workflow_upload_inflight", [], |r| {
        r.get(0)
    })
    .unwrap()
}

#[tokio::test]
async fn a56_an_upload_asked_for_on_a_connection_that_went_away_meanwhile_reserves_nothing() {
    let workflow = r#"on: push
jobs:
  produce:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
        with:
          name: package
          path: result.dat
          if-no-files-found: error
      - run: echo still-running
"#;
    // Without the detach, the same exchange reserves the upload's files.
    for detached in [false, true] {
        let fx = fx();
        let (c, _files) = fx.open();
        let plan = ci_plan(workflow);
        let (_, ids) = ci_run(&fx, &c, &plan).await;
        assert_eq!(c.tick().await.unwrap().placed.len(), 1);
        let producer = c.store().attempts_of_task(&ids["produce"]).unwrap()[0]
            .attempt_id
            .clone();
        let contents = vec![b'c'; 17];
        let digest = Digest::of_bytes(&contents);
        let logical = &plan.tasks[0].workflow;
        let upload = ArtifactAction {
            key: Digest::of_bytes(b"upload"),
            intent: ArtifactIntent::Upload {
                overwrite: false,
                receipt: json!({
                    "name":"package","version":1,"files":[{"path":"result.dat","size":contents.len(),"sha256":digest.as_str().strip_prefix("sha256:").unwrap()}],
                    "plan_digest":plan.digest,"attempt_id":"run","job":format!("{logical}::produce"),"display":"produce","task":null,"retention_days":null,
                }),
            },
        };
        fx.w.set(|s| {
            s.artifact_data.insert(digest.clone(), contents.clone());
            s.pending_artifacts.insert(producer.clone(), upload.clone());
        });
        let held = Arc::new(HeldPoll {
            worker: Arc::clone(&fx.w),
            hold: std::sync::atomic::AtomicBool::new(true),
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let link: Arc<dyn WorkerLink> = held.clone();
        c.attach(fx.w.id.clone(), Arc::clone(&link)).await;
        let (ticked, ()) = tokio::join!(c.tick(), async {
            // The worker asks for the upload; while that answer is on its
            // way, the connection goes away.
            held.entered.acquire().await.unwrap().forget();
            if detached {
                assert!(c.detach_link(&fx.w.id, &link).await.unwrap());
            }
            held.release.add_permits(1);
        });
        ticked.unwrap();
        assert!(
            !held.hold.load(std::sync::atomic::Ordering::SeqCst),
            "the poll was held"
        );
        if detached {
            assert_eq!(reserved_uploads(&fx), 0);
            assert!(fx.w.offsets("workflow_read").is_empty());
        } else {
            assert_eq!(reserved_uploads(&fx), 1);
            assert_eq!(fx.w.offsets("workflow_read"), [0]);
        }
    }
}

/// A cancel of the run recorded after a CI task was placed and before its
/// worker took it (the submit did not arrive): the task is never submitted
/// again, so no step starts, it ends cancelled, nothing that waits is
/// released, and the run settles as cancelled.
#[tokio::test]
async fn a_run_cancel_recorded_before_a_ci_task_is_accepted_starts_nothing_and_settles_cancelled() {
    let fx = fx();
    let (c, files) = fx.open();
    let plan = ci_plan(CI_CHAIN);
    let (run, ids) = ci_run(&fx, &c, &plan).await;
    fx.w.set(|s| s.trip = Some(("submit", 1, Lost::Request)));
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 1);
    let attempt = report.placed[0].clone();
    assert!(
        fx.w.state.lock().unwrap().jobs.is_empty(),
        "the worker never took it"
    );

    c.cancel_run(run.clone()).await.unwrap();
    fx.w.set(|s| s.offline = false);
    for _ in 0..4 {
        c.tick().await.unwrap();
        collect(&c, &files).await;
        assert!(ci_advance(&fx, &c).released.is_empty());
    }
    assert_eq!(fx.w.count("submit"), 1, "not sent again");
    assert!(
        !fx.w
            .state
            .lock()
            .unwrap()
            .jobs
            .values()
            .any(|job| job.state != AttemptState::Terminal),
        "no step runs"
    );
    let record = c.store().get_attempt(&attempt).unwrap().unwrap();
    assert_eq!(record.termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(c.store().attempts_of_task(&ids["test"]).unwrap().is_empty());
    let target = belt::service::completion::Target::Run(run.clone());
    let snapshot = c.store().completion(target).unwrap();
    assert!(snapshot.settled());
    assert_eq!(ci_computed(&fx, &c, &files, &run), Computed::Cancelled);
}
