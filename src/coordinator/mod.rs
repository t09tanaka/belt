//! Parent process: acceptance, placement and state.
//!
//! The [`Coordinator`] owns the durable queue (runs, tasks, attempts) in the
//! parent database and talks to workers through [`WorkerLink`]s. One
//! [`Coordinator::tick`] at a time is the single actor that commits
//! placements (spec chapter 9). A tick first looks at every attempt that
//! still needs attention on each attached worker, then places queued tasks.
//!
//! The workers are looked at side by side, each within the same time budget
//! ([`CoordinatorConfig::worker_budget_ms`]): a worker that stalls costs the
//! tick that budget once, not one timeout per attempt, so the leases on the
//! other workers are renewed in time. Placing stays one step after the
//! other, by one tick at a time.
//!
//! No worker is ever asked anything under the actor: a tick applies each
//! answer in a short critical section, and drops an answer whose connection
//! was detached or replaced meanwhile. A request the parent answers from its
//! own database (`workers`, `forget`) never waits for a worker (A56).
//!
//! Every step is decided from what is durable in the parent database and
//! what the worker answers now ([`policy`]), so a tick can be repeated after
//! a crash at any point: a placement that was committed and never sent, a
//! submit whose acknowledgement was lost and a retire whose answer was lost
//! are all picked up by the next tick, in this process or the next.
//!
//! Which worker gets a task is the simplest deterministic choice here (the
//! first attached worker, by id, that has room); choosing by predicted
//! completion belongs to the scheduler.

mod collect;
mod env_sync;
mod estimate;
mod forget;
pub mod identity;
mod input_gate;
mod link;
pub(crate) mod member_contact;
pub mod policy;
mod prefetch;
mod preparation;
mod preparation_identity;
pub(crate) mod release;
mod workflow_artifact;

pub use input_gate::{PlacementInputs, ReadyInput};
pub use link::{ClientLink, InputGate, LinkError, WorkerLink};

use crate::error::{ApiError, ConflictReason, ErrorCode, OverloadedReason};
use crate::ids::{
    AgentEpoch, AttemptId, CoordinatorEpoch, Generation, ManifestDigest, PlanDigest, RequestId,
    RunId, SourceId, TaskId, WorkerBootId, WorkerId,
};
use crate::platform::wall_ms;
use crate::scheduler::decide::{self, Asked, Candidate, Decision, Excluded, Exclusion, Outlook};
use crate::scheduler::{self, Scheduler};
use crate::state::{AttemptState, Observation, Outcome, Termination};
use crate::storage::{
    AttemptRecord, CoordinatorStore, InputCursor, LiveAttempt, LiveAttemptPage, PlaceInput,
    PlaceOutcome, QueuedTask, RetireEnd, Seen, StorageError, WorkerLoad, PAGE_MAX,
};
use crate::wire::{
    payload_hash, ArtifactDecl, AttemptPayload, AttemptStatus, CancelAttemptRequest, CommandSpec,
    ForecastRequest, ForecastResponse, GetAttemptRequest, InspectRequest, InspectResponse,
    RenewLeaseRequest, Request, Response, RetireAttemptRequest, RetireStatus, SchemaVersion,
    SubmitAttemptRequest, WorkloadPlan,
};
use link::Budgeted;
use policy::{Answer, Ended, Facts, Limits, Refusal, RetireVerdict, Step, Unstored};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

/// The reason recorded for a retire the coordinator starts by itself after a
/// worker refused the attempt for good. Such an attempt is failed, not
/// placed again, once the retire is confirmed.
pub const REASON_SUBMIT_REFUSED: &str = "submit_refused";

/// The reason recorded for an attempt its worker refused because the
/// attempt id is held there by an attempt of another member (spec 24.4).
/// The attempt is closed without a retire when the answered send was the
/// only one, and retired on the worker first otherwise. The task is then
/// free again and is placed under a new attempt id; no worker is left out
/// for it and the task does not fail.
pub const REASON_ATTEMPT_ID_IN_USE: &str = "attempt_id_in_use";

/// The `reason` of the [`scheduler::EVENT_WORKER_EXCLUDED`] event on an
/// attempt taken off its worker for its input (spec 25.8 5b (b)): refused
/// with `INPUT_MISSING` three times in a row after its input was delivered
/// there again, its input not delivered again within [`INPUT_WAIT`], or a
/// payload that names no source or run. The tombstone's reason is
/// [`scheduler::REASON_WORKER_CANNOT_RUN`], so the task is not placed on
/// that worker again.
pub const REASON_INPUT_MISSING: &str = "input_missing";

/// The event on a run whose attempt its worker refused with
/// `INPUT_MISSING`: the attempt waits for its input to be delivered there
/// again. Payload: `{"worker", "source", "strikes"}`.
pub const EVENT_INPUT_WAIT: &str = "input_wait";

/// How long a marked attempt waits for its input to be back in its worker's
/// `ready` before it is taken off that worker (spec 25.8 5b (b)).
const INPUT_WAIT: Duration = Duration::from_secs(30 * 60);

/// How many `INPUT_MISSING` marks the coordinator keeps (one per attempt).
/// One more drops another: that attempt is then as after a restart of this
/// process (its next refusal marks it again).
const INPUT_MISSING_MARKS: usize = 4096;

/// An attempt its worker refused with `INPUT_MISSING` (spec 25.8 5b (b)): it
/// is not sent again until the worker's `ready` holds its run's source
/// again. `strikes` counts the refusals that came after `ready` returned.
#[derive(Debug, Clone)]
struct MissingMark {
    run: crate::ids::RunId,
    source: crate::ids::SourceId,
    strikes: u8,
    /// When the last refusal marked it (this process's clock).
    marked: Instant,
    /// Insertion order: on overflow the oldest mark goes.
    order: u64,
}

/// What [`Coordinator::input_wait`] found for an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputWait {
    Send,
    Waits,
    Expired,
}

/// The event on an attempt whose id its worker could not take. Payload:
/// `{"worker", "code", "reason", "retire"}` (`retire`: whether a retire was
/// asked for); written with the tombstone, in one transaction.
pub const EVENT_SUBMIT_REFUSED_ATTEMPT_ID: &str = "submit_refused_attempt_id";

/// The reason recorded for an attempt its worker refused because its
/// admission window was full with the attempts of every member (spec 24.4).
/// Kept in this database only. The attempt is closed without a retire when
/// the answered send was the only one, and retired on the worker first
/// otherwise; either way the task does not fail and goes on as its next
/// generation, and the worker is left out only until it answers room.
pub const REASON_CAPACITY_REFUSED: &str = "capacity_refused";

/// The event on an attempt its worker refused for capacity. Payload:
/// `{"worker", "code", "reason", "retire"}` (`retire`: whether a retire was
/// asked for); written with the tombstone, in one transaction.
pub const EVENT_SUBMIT_REFUSED_CAPACITY: &str = "submit_refused_capacity";

/// The most queued tasks one tick looks at.
pub const DISPATCH_SCAN: u32 = 256;

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A worker has seen a newer coordinator epoch: this process is not the
    /// coordinator any more. It places and controls nothing from here on.
    #[error("a newer coordinator took over; this one (epoch {0}) stopped")]
    Fenced(CoordinatorEpoch),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("a storage call was aborted")]
    Aborted,
    /// The parent's result store could not be read or written.
    #[error("result store: {0}")]
    Results(String),
}

type Result<T, E = CoordinatorError> = std::result::Result<T, E>;

impl TaskSpec {
    /// Only the submitting member's own local worker may take it: an
    /// in-place task (which includes `--local`) or an `exec --local` one.
    pub fn pinned_local(&self) -> bool {
        self.in_place || self.local
    }
}

/// What a task runs, as it is queued. The payload of each attempt is built
/// from this by [`build_payload`] and nowhere else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    /// The task of a frozen CI plan this is. It has no `commands` then: its
    /// steps are in the plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci: Option<CiSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchains: Option<crate::toolchain::contract::FrozenBundle>,
    #[serde(
        default,
        skip_serializing_if = "crate::toolchain::capability::ExecutionRequirements::is_empty"
    )]
    pub requirements: crate::toolchain::capability::ExecutionRequirements,
    /// Mutable local workspace; source is a comparison baseline only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub in_place: bool,
    /// Placed only on the submitting member's own local worker (`exec --local`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub local: bool,
    #[serde(default)]
    pub setup: Option<crate::wire::SetupSpec>,
    pub plan_digest: PlanDigest,
    pub manifest_digest: ManifestDigest,
    pub commands: Vec<CommandSpec>,
    #[serde(default)]
    pub command_timeout_ms: Option<u64>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub resource_wait_timeout_ms: Option<u64>,
    #[serde(default)]
    pub has_secret_bundle: bool,
    /// The files to keep as artifacts; none when the task declares none (and
    /// then not written, like in the payload).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactDecl>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<crate::wire::OutputDecl>,
}

/// The shared permit pool of a matrix job within this run. An unknown attempt keeps its permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixAdmission {
    pub group: String,
    pub max_parallel: std::num::NonZeroU32,
}

/// A task of a frozen CI plan, as it is queued.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CiSpec {
    /// Planned SDK step identities, in demand order. Old tasks have no samples.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history_keys: Vec<Option<crate::ids::Digest>>,
    /// The job of the plan in its project (`ci::timing::task_scope`): the
    /// key earlier runs of the same job are found by, to place the longest
    /// first (spec 配置の手順 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_key: Option<crate::ids::Digest>,
    /// A dynamic origin waits on derived variants and never executes itself.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dynamic_origin: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix: Option<MatrixAdmission>,
    /// A rerun could repeat external effects. A captured job policy must
    /// explicitly allow retry for a non-idempotent runner.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub shard_retry_forbidden: bool,
    /// Final tie-break for a manifest balanced with this worker's measured speed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_preferred_worker: Option<WorkerId>,

    /// What the worker is sent: the plan by its digest, the task, and the
    /// results of the jobs it needs.
    pub task: crate::wire::CiTask,
    /// The jobs the task needs have not all ended: it has no inputs yet, and
    /// it is not placed. The progression of its run takes this away when it
    /// writes the inputs (`CoordinatorStore::release_ci_task`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hold: bool,
    /// How many commands the plan says the task starts
    /// (`local_ci::plan_task_demand`): the demand a forecast is asked for.
    #[serde(default)]
    pub steps: u32,
    /// Why the task cannot be run. Written together with the cancel of a
    /// task that was never placed (`CoordinatorStore::fail_ci_tasks`), and
    /// shown where the task is shown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unrunnable: Option<String>,
    /// No worker resolves the toolchains this task requires (spec 22.1):
    /// the refusal, with the requirement. Such a task is never placed (it
    /// has no bundle, and running it on whatever a worker has would not be
    /// what it requires): the owner fails it in the transaction that queues
    /// the run, and it counts as a failed job. The other tasks run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unplaceable: Option<String>,
    /// The task is a shard of this split job of the plan. The progression
    /// looks at the run until the job is joined
    /// (`CoordinatorStore::ci_waiting_runs`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_group: Option<String>,
    /// The split job this shard belongs to has its one outcome: every shard
    /// of it ended and the progression put them together. Until then the run
    /// is not settled (`CoordinatorStore::completion`). A retry of a shard of
    /// the job takes it away again.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub joined: bool,
    /// The split job is a failure, with what is wrong: the run is no success
    /// even when the attempt of every shard is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_failure: Option<String>,
    /// The split job is a failure and this shard is one a retry of the run
    /// runs again although its attempt succeeded: its tests failed, it left
    /// no receipt, or it did not report exactly the tests it was given
    /// (`ci::shards::rerun`). A retry takes it away.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rerun: bool,
}

/// The one place an [`AttemptPayload`] is made.
pub fn build_payload(
    input: &PlaceInput,
    spec: &TaskSpec,
    lease_ms: u64,
) -> Result<AttemptPayload, StorageError> {
    let source_id = input
        .source_id
        .clone()
        .ok_or_else(|| StorageError::Invalid(format!("run {} has no source", input.run_id)))?;
    Ok(AttemptPayload {
        ci_task: spec.ci.as_ref().map(|ci| ci.task.clone()),
        toolchains: spec.toolchains.clone().map(Box::new),
        requirements: spec.requirements.clone(),
        in_place: spec.in_place,
        setup: spec.setup.clone(),
        run_id: input.run_id.clone(),
        task_id: input.task_id.clone(),
        generation: input.generation,
        source_id,
        plan_digest: spec.plan_digest.clone(),
        manifest_digest: spec.manifest_digest.clone(),
        permit_id: input.permit_id.clone(),
        commands: spec.commands.clone(),
        lease_ms,
        command_timeout_ms: spec.command_timeout_ms,
        deadline_ms: spec.deadline_ms,
        resource_wait_timeout_ms: spec.resource_wait_timeout_ms,
        has_secret_bundle: spec.has_secret_bundle,
        artifacts: spec.artifacts.clone(),
        outputs: spec.outputs.clone(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorConfig {
    /// The coordinator's limits per worker; a worker's own stricter limits
    /// (from `inspect`) win.
    pub limits: Limits,
    /// The execution lease granted with each submit and renewal.
    pub lease_ms: u64,
    /// How long one tick waits for one worker, over all its attempts; then
    /// again for the submits of that tick, over all workers. What was not
    /// answered by then is `unknown` and is asked again by the next tick.
    /// Keep twice this well below `lease_ms`: a tick takes no longer, and
    /// every tick renews the leases.
    pub worker_budget_ms: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        CoordinatorConfig {
            limits: Limits {
                max_active_task: 2,
                prefetch_per_worker: 1,
            },
            lease_ms: 60_000,
            worker_budget_ms: 15_000,
        }
    }
}

/// What one tick did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Attempts placed (and sent) by this tick.
    pub placed: Vec<AttemptId>,
    /// Attempts left `unknown` because their worker gave no answer.
    pub unknown: Vec<AttemptId>,
    /// Attempts whose retire was confirmed, or that were closed without one
    /// because their worker stored nothing of them: their tasks are queued
    /// again.
    pub retired: Vec<AttemptId>,
    /// Attempts that became terminal on the parent.
    pub settled: Vec<AttemptId>,
}

impl TickReport {
    fn absorb(&mut self, part: TickReport) {
        self.placed.extend(part.placed);
        self.unknown.extend(part.unknown);
        self.retired.extend(part.retired);
        self.settled.extend(part.settled);
    }
}

/// What a worker said about its host: the cores and memory of `workers` and
/// `top`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct WorkerResources {
    pub cpu_cores: Option<u32>,
    pub memory_bytes: Option<u64>,
}

/// A worker that answered `inspect` in this tick.
struct Reached {
    capabilities: Option<crate::toolchain::capability::WorkerCapabilities>,
    worker_id: WorkerId,
    link: Arc<dyn WorkerLink>,
    attachment: Arc<dyn WorkerLink>,
    limits: Limits,
    draining: bool,
    /// Its admission window as it answered it: one budget over the attempts
    /// of every member.
    admission: Option<crate::wire::AdmissionWindow>,
    /// Who answered: a forecast from another boot or agent is not used.
    boot_id: WorkerBootId,
    agent_epoch: AgentEpoch,
}

/// The workers of one tick's placements: those a task may still go to, and
/// those that are out for the rest of the tick, with the reason.
struct Round {
    /// Every worker this tick looked at.
    known: BTreeSet<WorkerId>,
    open: Vec<Reached>,
    out: BTreeMap<WorkerId, Exclusion>,
    deadline: Instant,
    /// Whether a worker can provide a frozen toolchain, by the question
    /// (bundle digest, requirements) and the worker; answered once per tick.
    toolchain_eligible: BTreeMap<((crate::ids::Digest, String), WorkerId), bool>,
}

impl Round {
    /// The workers that answered, among those the tick looked at; the rest
    /// are out as not reached.
    fn new(known: BTreeSet<WorkerId>, reached: Vec<Reached>, deadline: Instant) -> Round {
        let mut round = Round {
            out: BTreeMap::new(),
            open: Vec::new(),
            known,
            deadline,
            toolchain_eligible: BTreeMap::new(),
        };
        let answered: BTreeSet<WorkerId> = reached.iter().map(|w| w.worker_id.clone()).collect();
        for missing in round.known.difference(&answered) {
            round.out.insert(missing.clone(), Exclusion::Unreached);
        }
        for worker in reached {
            round.admit(worker);
        }
        round
    }

    /// Place once more, by the same deadline, now that some of the workers
    /// that were not reached answered late. What closed a worker before
    /// stays, and the placing of the tick still takes one budget at most.
    fn again(&mut self, late: Vec<Reached>) {
        for mut worker in late {
            self.out.remove(&worker.worker_id);
            worker.link = Arc::clone(&worker.attachment);
            self.admit(worker);
        }
    }

    fn admit(&mut self, mut worker: Reached) {
        if worker.draining {
            self.out.insert(worker.worker_id, Exclusion::Draining);
            return;
        }
        worker.link = Arc::new(Budgeted {
            inner: worker.link,
            deadline: self.deadline,
        });
        self.open.push(worker);
        self.open.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
    }

    fn close(&mut self, worker_id: &WorkerId, why: Exclusion) {
        self.open.retain(|w| &w.worker_id != worker_id);
        self.out.insert(worker_id.clone(), why);
    }
}

/// What came of asking one worker for a forecast.
enum Forecast {
    /// The answer, and when the question was sent.
    Answer(Instant, Box<ForecastResponse>),
    /// The worker answered with an error: it says nothing about its queue.
    Refused,
    /// No answer in time.
    Silent,
    /// A newer coordinator took over.
    Fenced,
}

/// What became of an attempt that was just placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    /// The worker holds it.
    Taken,
    /// The worker did not take it (now).
    NotTaken,
    /// The worker did not take it because its admission window is full.
    WindowFull,
    /// It was not sent: a cancel or a retire for it was recorded first.
    Withheld,
}

/// A submit the worker stored nothing of was not taken; a full window also
/// says that nothing else goes there for now.
fn not_taken(why: Unstored) -> Sent {
    match why {
        Unstored::Capacity => Sent::WindowFull,
        Unstored::AttemptIdInUse => Sent::NotTaken,
    }
}

type Followed = Result<(Option<Reached>, TickReport)>;

/// An attached worker and its connection.
type Link = (WorkerId, Arc<dyn WorkerLink>);

/// The connection a worker was asked on: the very link the calls went to,
/// never a later read of the registry.
type Attached = Arc<dyn WorkerLink>;

/// The workers that answered, with what was done on each in `report`; the
/// first failure, if any, after all of them were looked at.
fn absorb_all(report: &mut TickReport, all: Vec<Followed>) -> Result<Vec<Reached>> {
    let mut reached = Vec::new();
    let mut failed = None;
    for followed in all {
        match followed {
            Ok((worker, part)) => {
                report.absorb(part);
                reached.extend(worker);
            }
            Err(e) => failed = failed.or(Some(e)),
        }
    }
    match failed {
        Some(e) => Err(e),
        None => Ok(reached),
    }
}

/// Drive every future to its end, side by side on this task.
async fn join_all<'a, T>(mut all: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>) -> Vec<T> {
    let mut done: Vec<Option<T>> = all.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, slot) in all.iter_mut().zip(done.iter_mut()) {
            if slot.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(out) => *slot = Some(out),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    done.into_iter().flatten().collect()
}

pub struct Coordinator {
    /// Who this coordinator is in the group: the name of its member and
    /// where the member's group key is. Set once at startup.
    group: std::sync::OnceLock<(WorkerId, std::path::PathBuf)>,
    /// The marker of a version trial; see [`Coordinator::set_trial_marker`].
    trial: std::sync::OnceLock<std::path::PathBuf>,
    store: Arc<CoordinatorStore>,
    cfg: CoordinatorConfig,
    workers: Mutex<BTreeMap<WorkerId, Arc<dyn WorkerLink>>>,
    /// Cores and memory each worker last reported at `inspect`.
    resources: Mutex<BTreeMap<WorkerId, WorkerResources>>,
    /// The submits on their way: no acknowledgement or release of them is
    /// sent until they are answered (spec 25.6 (c)).
    submits: Arc<release::InFlight>,
    /// The workers whose `list_unreleased` reconciliation is due.
    unreleased_due: Mutex<BTreeSet<WorkerId>>,
    /// Where the reconciliation of each worker goes on (the last attempt
    /// it read), so a pass does not read the same first pages again. In
    /// memory: a lost cursor only means the next pass starts at the front.
    unreleased_after: Mutex<BTreeMap<WorkerId, AttemptId>>,
    /// Workers whose reconciliation had nothing left to send again but did
    /// not read the whole list in one pass: due again at this instant, not
    /// at once (a worker with many unreleasable attempts is not read on
    /// every pass).
    unreleased_resume: Mutex<BTreeMap<WorkerId, tokio::time::Instant>>,
    parent_local: Mutex<Option<WorkerId>>,
    preparation_identities: Mutex<BTreeMap<WorkerId, preparation_identity::IdentityContext>>,
    preparation_query: tokio::sync::Mutex<()>,
    preparation_cursors: Mutex<BTreeMap<WorkerId, preparation::CursorState>>,
    /// Short critical sections over the state of the workers' attempts:
    /// what a worker answered is applied under it, and `forget`, `detach`
    /// and the admission of an identity hold it. It is never held while a
    /// worker is asked anything (spec A56), so a request the parent answers
    /// from its own database does not wait for a worker.
    actor: tokio::sync::Mutex<()>,
    /// One tick at a time: the single placer. Nothing but a tick takes it.
    ticking: tokio::sync::Mutex<()>,
    /// The workers whose last look got no answer in time. A tick places on
    /// the others once they answered, without waiting for these first.
    /// Kept in this process only.
    silent: Mutex<BTreeSet<WorkerId>>,
    /// One pass of result collection at a time.
    collecting: Arc<tokio::sync::Mutex<()>>,
    /// The collections that wait before they are tried again.
    collect_waits: Arc<crate::results::collect::Waits>,
    cost_tails: Arc<scheduler::measured::Tails>,
    live_log_waits: Arc<crate::results::collect::Waits>,
    live_worker_cursor: std::sync::Mutex<Option<WorkerId>>,
    live_log_cursors: std::sync::Mutex<std::collections::BTreeMap<WorkerId, crate::ids::AttemptId>>,
    lease_seq: AtomicU64,
    input_cursor: Mutex<Option<crate::storage::InputCursor>>,
    placement_fence: Arc<tokio::sync::Mutex<()>>,
    fenced: AtomicBool,
    /// Set when the resident stops: a tick still running finishes the step
    /// it is in, and starts no submit after it.
    stopping: AtomicBool,
    /// The queued tasks no payload could be made for, as this process
    /// already recorded.
    unplaceable: Mutex<BTreeSet<TaskId>>,
    /// The workers that refused a submit for capacity and whose `inspect`
    /// has not answered room since (spec 24.4). Nothing new is placed on
    /// them. Kept in this process only: after a restart the `inspect` of
    /// each worker decides anew.
    capacity_refused: Mutex<BTreeSet<WorkerId>>,
    /// The attempts refused with `INPUT_MISSING`, in this process's memory
    /// (spec 25.8 5b (b)). A mark goes when its attempt is taken, found
    /// terminal or retired, and at most [`INPUT_MISSING_MARKS`] are kept.
    input_missing: Mutex<std::collections::HashMap<AttemptId, MissingMark>>,
    sched: Scheduler,
}

fn cancelled() -> Termination {
    Termination::new(Outcome::Cancelled, None, None).expect("cancelled without an exit is valid")
}

fn seen_of(status: &AttemptStatus) -> Seen {
    Seen {
        state: status.state,
        termination: status.termination,
        receipts: status.receipts,
    }
}

impl Coordinator {
    /// Take over the state in `store` (opening it started a new coordinator
    /// epoch). Nothing is known first-hand after a start: every attempt that
    /// still needs attention is `reconciling` until its worker was asked.
    pub fn start(store: CoordinatorStore, cfg: CoordinatorConfig) -> Result<Coordinator> {
        Coordinator::start_with(store, cfg, Scheduler::default())
    }

    /// [`Coordinator::start`] with the scheduler's settings and estimates.
    pub fn start_with(
        store: CoordinatorStore,
        cfg: CoordinatorConfig,
        sched: Scheduler,
    ) -> Result<Coordinator> {
        store.mark_reconciling()?;
        Ok(Coordinator {
            group: std::sync::OnceLock::new(),
            trial: std::sync::OnceLock::new(),
            store: Arc::new(store),
            cfg,
            workers: Mutex::default(),
            resources: Mutex::default(),
            submits: Arc::default(),
            unreleased_due: Mutex::default(),
            unreleased_after: Mutex::default(),
            unreleased_resume: Mutex::default(),
            parent_local: Mutex::default(),
            preparation_identities: Mutex::default(),
            preparation_query: tokio::sync::Mutex::new(()),
            preparation_cursors: Mutex::default(),
            actor: tokio::sync::Mutex::new(()),
            ticking: tokio::sync::Mutex::new(()),
            silent: Mutex::default(),
            collecting: Arc::new(tokio::sync::Mutex::new(())),
            collect_waits: Arc::default(),
            cost_tails: Arc::default(),
            live_log_waits: Arc::default(),
            live_worker_cursor: std::sync::Mutex::default(),
            live_log_cursors: std::sync::Mutex::default(),
            lease_seq: AtomicU64::new(0),
            input_cursor: Mutex::default(),
            placement_fence: Arc::default(),
            fenced: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            unplaceable: Mutex::default(),
            capacity_refused: Mutex::default(),
            input_missing: Mutex::default(),
            sched,
        })
    }

    /// Name the marker of a version trial (`belt upgrade`, §24.8). While it
    /// is there this coordinator only adopts attempts and renews their
    /// leases: it places nothing, makes no generation, takes no submission,
    /// sends no retire or cancel and settles no result. The first call holds.
    pub fn set_trial_marker(&self, marker: std::path::PathBuf) {
        let _ = self.trial.set(marker);
    }

    pub fn in_trial(&self) -> bool {
        self.trial
            .get()
            .is_some_and(|marker| crate::worker::control::present(marker))
    }

    /// On trial, leave the proof of what this version did with an attempt
    /// it adopted: its lease was renewed, or a worker answered and it has no
    /// lease to renew. The upgrade checks it before its commit (§24.8). A
    /// proof that cannot be written is missing, and the upgrade then rolls
    /// back.
    fn note_trial(&self, attempt: &AttemptId, renewed: bool) {
        if let Some(marker) = self.trial.get() {
            if crate::worker::control::present(marker) {
                let _ = if renewed {
                    crate::worker::control::note_renewal(marker, attempt.as_str())
                } else {
                    crate::worker::control::note_unrenewed(marker, attempt.as_str())
                };
            }
        }
    }

    /// A change asked of a member on trial is answered "send again later"
    /// (`OVERLOADED`) with nothing saved: a rollback would lose it.
    pub(crate) fn check_trial(&self) -> Result<()> {
        if self.in_trial() {
            return Err(CoordinatorError::Storage(StorageError::Overloaded(
                "this member is trying a new version; send again after the upgrade".into(),
            )));
        }
        Ok(())
    }

    /// The scheduler: priorities, and why a task waits.
    pub fn scheduler(&self) -> &Scheduler {
        &self.sched
    }

    pub fn store(&self) -> &CoordinatorStore {
        &self.store
    }

    /// Say who this coordinator is in the group: the name of the member it
    /// runs on and the file of that member's group key. The first call
    /// holds.
    pub fn set_group_identity(&self, member: WorkerId, key_path: std::path::PathBuf) {
        let _ = self.group.set((member, key_path));
    }

    /// The file of this member's group key, once the identity is set.
    pub fn group_key_path(&self) -> Option<&std::path::Path> {
        self.group.get().map(|(_, path)| path.as_path())
    }

    /// What this coordinator proves to a worker (spec 24.2): its member's
    /// name and the group key, read now, so a key that was switched since
    /// the last connection is the one used.
    pub fn membership(&self) -> std::result::Result<crate::protocol::Membership, ApiError> {
        let (member, key_path) = self.group.get().ok_or_else(|| {
            ApiError::new(
                ErrorCode::Internal,
                "this coordinator was not told which member of the group it is",
            )
        })?;
        crate::protocol::Membership::load(member.clone(), key_path)
            .map_err(|e| ApiError::new(ErrorCode::Internal, format!("the group key: {e}")))
    }

    pub fn epoch(&self) -> CoordinatorEpoch {
        self.store.epoch()
    }

    /// The resident stops: no submit starts after this, neither a new
    /// placement nor a resend. What is not sent is sent by the next
    /// coordinator, which follows the attempt again.
    pub fn stop_submitting(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    async fn db<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&CoordinatorStore) -> crate::storage::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || f(&store)).await {
            Ok(out) => Ok(out?),
            Err(_) => Err(CoordinatorError::Aborted),
        }
    }

    fn fence(&self) -> CoordinatorError {
        self.fenced.store(true, Ordering::SeqCst);
        CoordinatorError::Fenced(self.epoch())
    }

    fn check_fenced(&self) -> Result<()> {
        if self.is_fenced() {
            Err(CoordinatorError::Fenced(self.epoch()))
        } else {
            Ok(())
        }
    }

    /// Bounded, authenticated metadata calls outside actor/admission/SQLite ownership.
    pub async fn resolve_toolchains(
        &self,
        bundle: crate::toolchain::contract::FrozenBundle,
        requirements: crate::toolchain::capability::ExecutionRequirements,
    ) -> std::result::Result<crate::toolchain::contract::FrozenBundle, crate::error::ApiError> {
        let mut workers: Vec<_> = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, link)| (id.clone(), link.clone()))
            .collect();
        workers.sort_by(|a, b| {
            self.sched
                .priority_of(&b.0)
                .cmp(&self.sched.priority_of(&a.0))
                .then(a.0.cmp(&b.0))
        });
        let budget = std::time::Duration::from_millis(self.cfg.worker_budget_ms);
        let epoch = self.epoch();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
        let source = bundle.frozen.source_manifest.clone();
        for (_, link) in workers.into_iter().take(32) {
            if self.is_fenced() {
                break;
            }
            let request =
                crate::wire::Request::ResolveToolchains(crate::wire::ResolveToolchainsRequest {
                    schema_version: crate::wire::SchemaVersion::CURRENT,
                    request_id: crate::ids::RequestId::generate(),
                    bundle: bundle.clone(),
                    requirements: requirements.clone(),
                });
            if let Ok(Ok(crate::wire::Response::ResolveToolchains(response))) =
                tokio::time::timeout(
                    budget.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                    link.read_only_call(request),
                )
                .await
            {
                if self.is_fenced() || self.epoch() != epoch {
                    break;
                }
                let proposed = response.bundle;
                let mut original = proposed.frozen.clone();
                original.selections = bundle.frozen.selections.clone();
                if original != bundle.frozen
                    || bundle
                        .evidence
                        .iter()
                        .any(|e| !proposed.evidence.contains(e))
                    || proposed.validate(&source).is_err()
                {
                    continue;
                }
                return Ok(proposed);
            }
        }
        Err(crate::error::ApiError::blocked(
            crate::error::BlockedReason::EnvironmentMismatch,
            format!(
                "toolchain resolve required: no coherent worker proposal ({})",
                bundle.frozen.describe_requirements()
            ),
        ))
    }

    /// Whether some attached worker reports every tool the workspace's
    /// preparation runs (the [`Requirement::Prepare`] of `requirements`,
    /// spec 22.1) on its bare PATH. The submit leaves out the tools the
    /// resolved toolchains of every task serve. The refusal names what each asked worker lacks, as
    /// placement would. Read-only: the same worker cap, deadline and fence
    /// checks as [`Self::resolve_toolchains`].
    ///
    /// [`Requirement::Prepare`]: crate::toolchain::capability::Requirement::Prepare
    pub async fn check_preparation(
        &self,
        requirements: crate::toolchain::capability::ExecutionRequirements,
    ) -> std::result::Result<(), crate::error::ApiError> {
        use crate::toolchain::capability::{missing, ExecutionRequirements, Requirement};
        let prepare = ExecutionRequirements {
            profile: None,
            requires: requirements
                .requires
                .iter()
                .filter(|r| {
                    matches!(
                        r,
                        Requirement::Prepare(_) | Requirement::Setup(_) | Requirement::Component(_)
                    )
                })
                .cloned()
                .collect(),
        };
        if prepare.is_empty() {
            return Ok(());
        }
        let mut workers: Vec<_> = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, link)| (id.clone(), link.clone()))
            .collect();
        workers.sort_by(|a, b| a.0.cmp(&b.0));
        let budget = std::time::Duration::from_millis(self.cfg.worker_budget_ms);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(4);
        let mut lacking = Vec::new();
        for (worker_id, link) in workers.into_iter().take(32) {
            if self.is_fenced() {
                break;
            }
            let request = Request::Inspect(InspectRequest {
                preparation_observations: None,
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
            });
            let response = tokio::time::timeout(
                budget.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                link.read_only_call(request),
            )
            .await;
            let capabilities = match response {
                Ok(Ok(Response::Inspect(r))) if r.worker_id == worker_id => r.capabilities,
                _ => continue,
            };
            let unmet = crate::toolchain::envdiff::with_steps(
                missing(&prepare, capabilities.as_ref()),
                &prepare.requires,
                capabilities.as_ref(),
            );
            if unmet.is_empty() {
                return Ok(());
            }
            if lacking.len() < 4 {
                lacking.push(format!("{worker_id}: {}", unmet.join(", ")));
            }
        }
        let named = |kind: fn(&Requirement) -> Option<&String>| -> Vec<String> {
            prepare.requires.iter().filter_map(kind).cloned().collect()
        };
        let tools = named(|r| match r {
            Requirement::Prepare(name) => Some(name),
            _ => None,
        });
        let setup = named(|r| match r {
            Requirement::Setup(name) => Some(name),
            _ => None,
        });
        let components = named(|r| match r {
            Requirement::Component(name) => Some(name),
            _ => None,
        });
        let mut parts = Vec::new();
        if !tools.is_empty() {
            parts.push(format!("workspace preparation runs {}", tools.join(", ")));
        }
        if !setup.is_empty() {
            parts.push(format!(
                "the job's setup steps install {}",
                setup.join(", ")
            ));
        }
        if !components.is_empty() {
            parts.push(format!(
                "the job's setup steps add the rust components {}",
                components.join(", ")
            ));
        }
        let mut why = format!("{}: no worker reports them", parts.join("; "));
        if !lacking.is_empty() {
            why = format!("{why} ({})", lacking.join("; "));
        }
        Err(crate::error::ApiError::blocked(
            crate::error::BlockedReason::EnvironmentMismatch,
            why,
        ))
    }

    pub fn set_parent_local(&self, worker: WorkerId) {
        *self.parent_local.lock().unwrap_or_else(|e| e.into_inner()) = Some(worker);
    }

    /// Install a verified connection. Replacement is linearized with placement
    /// commits, without holding up forecasts or blocking the async executor.
    pub async fn attach(&self, worker_id: WorkerId, link: Arc<dyn WorkerLink>) {
        let _commit = self.placement_fence.lock().await;
        self.reconcile_due(&worker_id);
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(worker_id, link);
    }

    /// Nonblocking attachment for synchronous construction and cost callbacks.
    /// Busy means no connection was installed; callers must handle the result.
    pub fn try_attach(
        &self,
        worker_id: WorkerId,
        link: Arc<dyn WorkerLink>,
    ) -> std::result::Result<(), tokio::sync::TryLockError> {
        let _commit = self.placement_fence.try_lock()?;
        self.reconcile_due(&worker_id);
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(worker_id, link);
        Ok(())
    }

    /// The link is gone. The worker's attempts are `unknown` from here on;
    /// they are kept, and followed again when it is attached again.
    pub async fn detach(&self, worker_id: &WorkerId) -> Result<()> {
        {
            let _commit = self.placement_fence.lock().await;
            let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            workers.remove(worker_id);
        }
        let _actor = self.actor.lock().await;
        self.unknown_all(worker_id).await?;
        Ok(())
    }

    /// Every live attempt of a detached worker is `unknown`. The caller
    /// removed the link under the placement fence first, so nothing is placed
    /// there any more, and holds the actor, so an answer of the old link that
    /// arrives now is dropped rather than applied. The walk holds one page of
    /// the attempts at a time.
    async fn unknown_all(&self, worker_id: &WorkerId) -> Result<()> {
        let mut after = None;
        loop {
            let page = self.live_page(worker_id, after).await?;
            let Some((last, _)) = page.last() else {
                return Ok(());
            };
            after = Some(*last);
            let last_page = page.len() < PAGE_MAX as usize;
            for (_, l) in page {
                self.observe(&l.record.attempt_id, Observation::Unknown)
                    .await?;
            }
            if last_page {
                return Ok(());
            }
        }
    }

    /// The next page of a worker's live attempts (at most
    /// [`PAGE_MAX`](crate::storage::PAGE_MAX)): what a walk over them holds
    /// in memory, however many the worker has.
    async fn live_page(
        &self,
        worker_id: &WorkerId,
        after: Option<InputCursor>,
    ) -> Result<LiveAttemptPage> {
        let w = worker_id.clone();
        self.db(move |s| s.live_attempt_page(&w, after, PAGE_MAX))
            .await
    }

    /// Disconnect only the installed session. Late cleanup must never remove
    /// a replacement connection or mark its observations unknown.
    pub async fn detach_link(
        &self,
        worker_id: &WorkerId,
        expected: &Arc<dyn WorkerLink>,
    ) -> Result<bool> {
        let _actor = self.actor.lock().await;
        let _commit = self.placement_fence.lock().await;
        {
            let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            if !workers
                .get(worker_id)
                .is_some_and(|link| Arc::ptr_eq(link, expected))
            {
                return Ok(false);
            }
            workers.remove(worker_id);
        }
        self.unknown_all(worker_id).await?;
        Ok(true)
    }

    /// Accept a run and its tasks durably (`coordinator accepted`).
    pub async fn enqueue(
        &self,
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
    ) -> Result<()> {
        let owner = self.store.clone();
        let source = tokio::task::spawn_blocking(move || owner.source_mutation_guard_foreground())
            .await
            .map_err(|e| CoordinatorError::Invalid(e.to_string()))??;
        self.enqueue_guarded(run_id, source_id, tasks, source).await
    }
    pub(crate) async fn enqueue_guarded(
        &self,
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
        source: crate::source::retention::Guard,
    ) -> Result<()> {
        self.check_fenced()?;
        // Every submission path comes through here, the resident's included.
        if self.in_trial() {
            return Err(CoordinatorError::Storage(StorageError::Overloaded(
                "this member is trying a new version; submit again after the upgrade".into(),
            )));
        }
        let mut specs = Vec::with_capacity(tasks.len());
        for (task, spec) in tasks {
            if let Some(bundle) = &spec.toolchains {
                bundle
                    .validate(&spec.manifest_digest)
                    .map_err(|e| CoordinatorError::Invalid(e.into()))?;
            }
            if spec.ci.is_none() && spec.commands.is_empty() {
                return Err(CoordinatorError::Invalid(format!(
                    "task {task} has no command"
                )));
            }
            let value = serde_json::to_value(&spec)
                .map_err(|e| CoordinatorError::Invalid(e.to_string()))?;
            check_spec(&run_id, &source_id, &task, &value, self.cfg.lease_ms)?;
            specs.push((task, value));
        }
        self.db(move |s| {
            let mut specs = specs;
            let keys = specs
                .iter()
                .filter_map(|(_, spec)| policy::duration_key(spec).map(str::to_string))
                .collect();
            let since = wall_ms().saturating_sub(crate::storage::DURATION_MAX_AGE_MS);
            policy::longest_first(&mut specs, &s.ci_job_durations(keys, since)?);
            s.enqueue_guarded(
                &run_id,
                Some(&source_id),
                &json!({}),
                &specs,
                wall_ms(),
                source,
            )
        })
        .await
    }

    pub(crate) fn clear_prefetch(&self, target: &crate::service::completion::Target) {
        let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        for link in workers.values() {
            match target {
                crate::service::completion::Target::Task(task) => {
                    link.clear_preference(Some(task), None)
                }
                crate::service::completion::Target::Run(run) => {
                    link.clear_preference(None, Some(run))
                }
            }
        }
    }
    /// Cancel a task: durable before anything is sent. A queued task is
    /// never placed; a placed one gets the cancel on the next tick, and on
    /// every later one until its worker answered.
    pub async fn cancel_task(&self, task_id: &TaskId) -> Result<Option<AttemptRecord>> {
        self.check_fenced()?;
        self.check_trial()?;
        self.clear_prefetch(&crate::service::completion::Target::Task(task_id.clone()));
        let task = task_id.clone();
        self.db(move |s| s.cancel_task(&task, wall_ms())).await
    }

    pub async fn cancel_run(&self, run_id: RunId) -> Result<()> {
        self.check_fenced()?;
        self.check_trial()?;
        self.clear_prefetch(&crate::service::completion::Target::Run(run_id.clone()));
        self.db(move |s| s.cancel_run(&run_id, wall_ms())).await
    }

    /// Ask for an attempt to be taken off its worker so that its task can be
    /// placed again. The tombstone is durable here first; the task is free
    /// only after the worker confirmed that the attempt never started or has
    /// stopped.
    pub async fn request_retire(&self, attempt_id: &AttemptId, reason: &str) -> Result<()> {
        self.check_trial()?;
        self.retire_durably(attempt_id, reason).await
    }

    /// The tombstone of [`Coordinator::request_retire`], for a tick that
    /// already knows it is not on trial (a tick on trial places nothing).
    async fn retire_durably(&self, attempt_id: &AttemptId, reason: &str) -> Result<()> {
        let (a, reason) = (attempt_id.clone(), reason.to_string());
        self.db(move |s| s.begin_retire(&a, &reason, wall_ms()))
            .await
    }

    /// One pass: follow every attempt on every attached worker, then place
    /// queued tasks. Fails only for a storage fault or when this coordinator
    /// was fenced; a worker that does not answer is not an error.
    ///
    /// Only one tick runs at a time, but a tick never holds the actor while
    /// it waits for a worker: each answer is applied in a short critical
    /// section of its own, and only if the worker's connection is still the
    /// one that was asked. A worker that did not answer the last look is not
    /// waited for before the others are placed on; it is still followed
    /// side by side, within the same budget, and placed on once it answers.
    pub async fn tick(&self) -> Result<TickReport> {
        let ticking = self.ticking.lock().await;
        self.check_fenced()?;
        let telemetry = Mutex::new(Vec::new());
        let mut report = TickReport::default();
        let attached: Vec<(WorkerId, Arc<dyn WorkerLink>)> = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            workers
                .iter()
                .map(|(w, l)| (w.clone(), Arc::clone(l)))
                .collect()
        };
        let known: BTreeSet<WorkerId> = attached.iter().map(|(w, _)| w.clone()).collect();
        let deadline = self.deadline();
        let (late, prompt): (Vec<_>, Vec<_>) = {
            let silent = self.silent.lock().unwrap_or_else(|e| e.into_inner());
            attached.into_iter().partition(|(w, _)| silent.contains(w))
        };
        let result = self
            .follow_and_place((prompt, late), known, deadline, &mut report, &telemetry)
            .await?;
        drop(ticking);
        // Conversion and the best-effort writer queue never run under the actor.
        self.ingest_preparation(telemetry.into_inner().unwrap_or_else(|e| e.into_inner()));
        result?;
        Ok(report)
    }

    /// Follow the `prompt` workers and the `late` ones side by side, and
    /// place on the prompt ones as soon as they all answered. The outer error
    /// is a failure while following, before anything was placed.
    ///
    /// Following takes one budget at most (`deadline`), and placing another,
    /// shared by both rounds of placements, so the tick stays within twice
    /// the budget.
    async fn follow_and_place(
        &self,
        (prompt, late): (Vec<Link>, Vec<Link>),
        known: BTreeSet<WorkerId>,
        deadline: Instant,
        report: &mut TickReport,
        telemetry: &Mutex<Vec<preparation::RawPage>>,
    ) -> Result<Result<()>> {
        let mut prompt = std::pin::pin!(join_all(self.follows(prompt, deadline, telemetry)));
        let mut late = std::pin::pin!(join_all(self.follows(late, deadline, telemetry)));
        let mut late_done = None;
        let prompt_done = loop {
            tokio::select! {
                biased;
                done = &mut late, if late_done.is_none() => late_done = Some(done),
                done = &mut prompt => break done,
            }
        };
        // In the order of the worker ids, whichever answered first.
        let mut reached = match absorb_all(report, prompt_done) {
            Ok(reached) => reached,
            Err(e) => {
                // The late ones may be applying an answer: let them finish,
                // so that no write of theirs lands after this tick ended.
                if late_done.is_none() {
                    (&mut late).await;
                }
                return Err(e);
            }
        };
        if late_done.is_none() && reached.is_empty() {
            // Nobody else to place on: wait for those as before.
            late_done = Some((&mut late).await);
        }
        Ok(match late_done {
            Some(done) => {
                reached.extend(absorb_all(report, done)?);
                let mut round = Round::new(known, reached, self.deadline());
                self.dispatch(&mut round, report, telemetry).await
            }
            None => {
                let mut round = Round::new(known, reached, self.deadline());
                let (placed, done) =
                    tokio::join!(self.dispatch(&mut round, report, telemetry), &mut late);
                let mut part = TickReport::default();
                let answered = absorb_all(&mut part, done);
                report.absorb(part);
                match (placed, answered) {
                    (Err(e), _) | (Ok(()), Err(e)) => Err(e),
                    (Ok(()), Ok(answered)) if answered.is_empty() => Ok(()),
                    (Ok(()), Ok(answered)) => {
                        // Those that answered late are placed on now.
                        round.again(answered);
                        self.dispatch(&mut round, report, telemetry).await
                    }
                }
            }
        })
    }

    fn follows<'a>(
        &'a self,
        workers: Vec<(WorkerId, Arc<dyn WorkerLink>)>,
        deadline: Instant,
        telemetry: &'a Mutex<Vec<preparation::RawPage>>,
    ) -> Vec<Pin<Box<dyn Future<Output = Followed> + Send + 'a>>> {
        workers
            .into_iter()
            .map(|(worker_id, link)| {
                Box::pin(self.follow(worker_id, link, deadline, telemetry))
                    as Pin<Box<dyn Future<Output = Followed> + Send + 'a>>
            })
            .collect()
    }

    /// What is attached for a worker now, compared with the connection that
    /// was asked before its answer is applied ([`Coordinator::answered`]).
    fn attached_now(&self, worker_id: &WorkerId) -> Option<Attached> {
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(worker_id)
            .cloned()
    }

    /// Take the actor to apply what a worker answered (or that it did not).
    /// `None` when the answer is stale: the connection that was asked was
    /// detached or replaced meanwhile (a `forget`, a `detach` or a new
    /// identity ran), so nothing of it may change the record any more. A
    /// worker with nothing attached now never matches: what was asked was a
    /// real connection, and it is gone.
    async fn answered(
        &self,
        worker_id: &WorkerId,
        asked: &Attached,
    ) -> Result<Option<tokio::sync::MutexGuard<'_, ()>>> {
        let actor = self.actor.lock().await;
        self.check_fenced()?;
        let same = self
            .attached_now(worker_id)
            .is_some_and(|now| Arc::ptr_eq(asked, &now));
        Ok(same.then_some(actor))
    }

    fn mark_silent(&self, worker_id: &WorkerId, silent: bool) {
        let mut marked = self.silent.lock().unwrap_or_else(|e| e.into_inner());
        if silent {
            marked.insert(worker_id.clone());
        } else {
            marked.remove(worker_id);
        }
    }

    fn deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.cfg.worker_budget_ms)
    }

    /// Look at one worker and at every attempt on it that needs attention.
    /// Nothing waits for the worker past `deadline`.
    async fn follow(
        &self,
        worker_id: WorkerId,
        link: Arc<dyn WorkerLink>,
        deadline: Instant,
        telemetry: &Mutex<Vec<preparation::RawPage>>,
    ) -> Followed {
        let budgeted = Budgeted {
            inner: Arc::clone(&link),
            deadline,
        };
        let mut report = TickReport::default();
        // The link the calls below go to, whatever is attached by now.
        let asked: Attached = Arc::clone(&link);
        let inputs = link.placement_inputs();
        let requested = self.preparation_request(&worker_id, &link, inputs.as_ref());
        let sent_boot = crate::platform::boot_id();
        let sent = crate::platform::monotonic_ms();
        let mut inspected = self.inspect(&worker_id, &budgeted, requested.clone()).await;
        {
            let Some(_actor) = self.answered(&worker_id, &asked).await? else {
                // Detached or replaced meanwhile: what this connection says
                // is no longer about the worker.
                return Ok((None, report));
            };
            if inspected
                .as_ref()
                .is_some_and(|answer| policy::answers_room(answer.admission.as_ref()))
            {
                // Only this inspect can clear an earlier refusal. A refusal
                // while driving below must wait for the next inspect.
                self.capacity_refused
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&worker_id);
            }
        }
        let received = crate::platform::monotonic_ms();
        let received_boot = crate::platform::boot_id();
        if let (Some(response), Some(inputs), Some(requested), Some(transport), Some(sent_boot)) = (
            inspected.as_mut(),
            inputs,
            requested,
            link.cost_transport(),
            sent_boot,
        ) {
            if received_boot.as_deref() == Some(sent_boot.as_str())
                && response.schema_version == SchemaVersion::CURRENT
            {
                if let Some(page) = response.preparation_observations.take() {
                    let mut pending = telemetry.lock().unwrap_or_else(|e| e.into_inner());
                    if pending.len() < 32 {
                        pending.push(preparation::RawPage {
                            worker: worker_id.clone(),
                            link: link.clone(),
                            inputs,
                            transport,
                            request: requested,
                            response_boot: response.worker_boot_id.clone(),
                            response_epoch: response.agent_epoch,
                            page,
                            parent_boot: sent_boot,
                            sent,
                            received,
                        });
                    }
                }
            }
        }
        // The worker's live attempts, one page in memory at a time. The tick
        // is the only placer, and it places on this worker only after this
        // walk.
        let mut after = None;
        loop {
            let page = self.live_page(&worker_id, after).await?;
            let Some((last, _)) = page.last() else { break };
            after = Some(*last);
            // A short page is the end: the cursor is stable, so a row that
            // would arrive behind it is not part of this walk either way.
            let last_page = page.len() < PAGE_MAX as usize;
            if inspected.is_none() {
                let Some(_actor) = self.answered(&worker_id, &asked).await? else {
                    return Ok((None, report));
                };
                for (_, l) in page {
                    self.unknown(&l.record.attempt_id, &mut report).await?;
                }
            } else {
                for (_, l) in page {
                    // Another worker may have said that this coordinator is replaced.
                    self.check_fenced()?;
                    self.drive(&budgeted, l, &mut report, &asked).await?;
                }
            }
            if last_page {
                break;
            }
        }
        let Some(inspected) = inspected else {
            self.mark_silent(&worker_id, true);
            return Ok((None, report));
        };
        if Instant::now() >= deadline {
            // It used up its time: nothing new goes there in this tick.
            self.mark_silent(&worker_id, true);
            return Ok((None, report));
        }
        self.mark_silent(&worker_id, false);
        if let Some(c) = &inspected.capabilities {
            self.resources
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    worker_id.clone(),
                    WorkerResources {
                        cpu_cores: c.cpu_cores,
                        memory_bytes: c.memory_bytes,
                    },
                );
        }
        let reached = Reached {
            capabilities: inspected.capabilities,
            worker_id,
            attachment: Arc::clone(&link),
            link,
            limits: self.cfg.limits.stricter(Limits {
                max_active_task: inspected.max_active_task,
                prefetch_per_worker: inspected.prefetch_per_worker,
            }),
            draining: inspected.draining,
            admission: inspected.admission,
            boot_id: inspected.worker_boot_id,
            agent_epoch: inspected.agent_epoch,
        };
        Ok((Some(reached), report))
    }

    async fn inspect(
        &self,
        worker_id: &WorkerId,
        link: &dyn WorkerLink,
        preparation_observations: Option<
            crate::setup::observation_transport::ObservationPageRequest,
        >,
    ) -> Option<InspectResponse> {
        let request = Request::Inspect(InspectRequest {
            preparation_observations,
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
        });
        match link.call(request).await {
            // Another identity behind the link is not this worker.
            Ok(Response::Inspect(r)) if &r.worker_id == worker_id => Some(r),
            _ => None,
        }
    }

    async fn observe(&self, attempt_id: &AttemptId, o: Observation) -> Result<AttemptRecord> {
        let a = attempt_id.clone();
        self.db(move |s| s.record_seen(&a, false, None, o, wall_ms()))
            .await
    }

    async fn note(&self, attempt_id: &AttemptId, kind: &'static str, payload: Value) -> Result<()> {
        let a = attempt_id.clone();
        self.db(move |s| {
            // Once per attempt: a stuck attempt is looked at on every tick.
            if s.events_of(&a, kind)?.is_empty() {
                s.append_event(None, Some(&a), kind, &payload, wall_ms())?;
            }
            Ok(())
        })
        .await
    }

    /// Record why a worker did not take an attempt, once per answer (the
    /// error code and, for `BLOCKED`, its reason), so that an attempt which
    /// waits on its worker shows what it waits for. Returns whether the
    /// worker gave this answer for the attempt before.
    async fn note_refusal(
        &self,
        attempt_id: &AttemptId,
        kind: &'static str,
        e: &ApiError,
    ) -> Result<bool> {
        let a = attempt_id.clone();
        let answer = decide::refusal_key(e.code, e.blocked_reason);
        let payload = json!({
            "code": e.code.to_string(),
            "reason": e.blocked_reason,
            "answer": answer,
            "message": e.message,
        });
        let appended = self
            .db(move |s| {
                s.append_events_unseen(None, &a, kind, "answer", &[(answer, payload)], wall_ms())
            })
            .await?;
        Ok(appended == 0)
    }

    /// Ask the worker for the attempt. A status that is not about this
    /// attempt and generation, or contradicts itself, is no answer.
    async fn ask(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
    ) -> (Answer, Option<AttemptStatus>) {
        let request = Request::GetAttempt(GetAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.attempt_id.clone(),
        });
        match link.call(request).await {
            Ok(Response::GetAttempt(s))
                if s.attempt_id == a.attempt_id
                    && s.generation == a.generation
                    && s.task_id == a.task_id
                    && s.check().is_ok() =>
            {
                (Answer::Status(s.state), Some(s))
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::NotFound => (Answer::NotFound, None),
            Err(LinkError::Remote(e)) if e.code == ErrorCode::Retired => (Answer::Tombstone, None),
            _ => (Answer::Unreachable, None),
        }
    }

    /// The worker holds the attempt: acknowledge its demand and mirror what
    /// it reported.
    async fn mirror(&self, status: &AttemptStatus, report: &mut TickReport) -> Result<()> {
        let (a, seen) = (status.attempt_id.clone(), seen_of(status));
        let rec = self
            .db(move |s| s.record_seen(&a, true, Some(seen), Observation::Known, wall_ms()))
            .await?;
        if let Some(why) = &status.failure_reason {
            // The reason the worker gives stays here, where `ci` reads it.
            let why = crate::worker::agent::bounded_reason(why);
            self.note(
                &rec.attempt_id,
                crate::ci::FAILURE_REASON_EVENT,
                json!({ "reason": why }),
            )
            .await?;
        }
        if rec.state == AttemptState::Terminal {
            report.settled.push(rec.attempt_id);
        }
        Ok(())
    }

    /// The parent fixes the result of an attempt no worker will ever report.
    async fn settle_here(
        &self,
        attempt_id: &AttemptId,
        termination: Termination,
        report: &mut TickReport,
    ) -> Result<()> {
        let a = attempt_id.clone();
        let seen = Seen {
            state: AttemptState::Terminal,
            termination: Some(termination),
            receipts: Default::default(),
        };
        self.db(move |s| s.record_seen(&a, false, Some(seen), Observation::Known, wall_ms()))
            .await?;
        report.settled.push(attempt_id.clone());
        Ok(())
    }

    async fn unknown(&self, attempt_id: &AttemptId, report: &mut TickReport) -> Result<()> {
        self.observe(attempt_id, Observation::Unknown).await?;
        report.unknown.push(attempt_id.clone());
        Ok(())
    }

    /// One look at one attempt, and the one step that follows from it.
    async fn drive(
        &self,
        link: &dyn WorkerLink,
        live: LiveAttempt,
        report: &mut TickReport,
        asked: &Attached,
    ) -> Result<()> {
        let a = &live.record;
        let facts = Facts {
            acked: live
                .placement
                .as_ref()
                .is_none_or(|p| p.acked_at_ms.is_some()),
            cancel_requested: a.cancel_requested_at_ms.is_some(),
            retire_requested: live.retire_reason.is_some(),
        };
        let (answer, status) = self.ask(link, a).await;
        let step = policy::next_step(facts, answer);
        let renewing = step == Step::Follow
            && !matches!(
                answer,
                Answer::Status(AttemptState::Cancelling | AttemptState::Finalizing)
            );
        if answer != Answer::Unreachable && !renewing {
            // A worker answered, and this attempt has no lease to renew.
            self.note_trial(&a.attempt_id, false);
        }
        if self.in_trial() && !matches!(step, Step::Follow | Step::KeepUnknown) {
            // Settled, retired, cancelled or sent again after the trial.
            return Ok(());
        }
        match step {
            // These ask the worker first, and apply what it says themselves.
            Step::Retire => return self.retire(link, &live, report, asked).await,
            Step::Cancel => return self.cancel(link, a, status, report, asked).await,
            // Not sent while stopping: the next coordinator sends it.
            Step::Submit if self.stopping() => return Ok(()),
            Step::Submit => return self.submit(link, &live, report, asked).await.map(|_| ()),
            _ => {}
        }
        let Some(actor) = self.answered(&a.worker_id, asked).await? else {
            return Ok(());
        };
        match (step, status) {
            (Step::KeepUnknown, _) => self.unknown(&a.attempt_id, report).await,
            (Step::SettleCancelled, _) => {
                self.settle_here(&a.attempt_id, cancelled(), report).await
            }
            (Step::Settle, Some(status)) => self.mirror(&status, report).await,
            (Step::Follow, Some(status)) => {
                self.mirror(&status, report).await?;
                drop(actor);
                self.renew(link, a, status.state, report, asked).await?;
                if !self.in_trial()
                    && !matches!(
                        status.state,
                        AttemptState::Cancelling | AttemptState::Finalizing
                    )
                {
                    self.follow_artifacts(link, &live, asked).await?;
                }
                Ok(())
            }
            (Step::Retire | Step::Cancel | Step::Submit, _) => Ok(()),
            // A step that needs a status always comes with one; without it
            // nothing is known.
            (Step::Settle | Step::Follow, None) => self.unknown(&a.attempt_id, report).await,
        }
    }

    /// Send the attempt as it was recorded when it was placed: the stored
    /// payload, which must still hash to the stored hash. Returns whether
    /// the worker holds the attempt now.
    async fn submit(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
        report: &mut TickReport,
        asked: &Attached,
    ) -> Result<Sent> {
        let a = &live.record;
        // Marked at the decision, before anything is awaited (spec 25.6
        // (xvi)): from here no acknowledgement or release of the attempt is
        // sent. A collection that made it terminal before the mark is seen
        // by the look at the record below, and sends nothing.
        let sending = self.submits.sending(&a.attempt_id);
        let id = a.attempt_id.clone();
        let ended = self
            .db(move |s| {
                Ok(s.get_attempt(&id)?
                    .is_none_or(|r| r.state == AttemptState::Terminal))
            })
            .await?;
        if ended {
            self.forget_missing(&a.attempt_id);
            return Ok(Sent::NotTaken);
        }
        // Refused for its input: not sent again, on either path, until the
        // worker's `ready` holds the input again (spec 25.8 5b (b)).
        match self.input_wait(link, &a.attempt_id) {
            InputWait::Send => {}
            InputWait::Waits => return Ok(Sent::Withheld),
            InputWait::Expired => {
                self.forget_missing(&a.attempt_id);
                self.exclude_for_input(a, "not_delivered_again").await?;
                self.observe(&a.attempt_id, Observation::Known).await?;
                return Ok(Sent::NotTaken);
            }
        }
        let payload: Option<AttemptPayload> = live
            .placement
            .as_ref()
            .and_then(|p| serde_json::from_value(p.demand["payload"].clone()).ok());
        let request = payload.and_then(|p| {
            SubmitAttemptRequest::new(RequestId::generate(), self.epoch(), a.attempt_id.clone(), p)
                .ok()
        });
        let Some(actor) = self.answered(&a.worker_id, asked).await? else {
            return Ok(Sent::NotTaken);
        };
        let Some(request) = request.filter(|r| r.payload_hash == a.payload_hash) else {
            // Fail closed: what would be sent is not what was decided.
            self.note(&a.attempt_id, "submit_payload_mismatch", Value::Null)
                .await?;
            self.unknown(&a.attempt_id, report).await?;
            return Ok(Sent::NotTaken);
        };
        // An in-place task is never placed again by itself (it works on the
        // user's own directory), so for it an attempt id in use or a full
        // window is waited out like any other `OVERLOADED`: the same attempt
        // is sent again.
        let replaceable = !request.payload.in_place;
        // Counted before the send, durably: a copy that leaves and is never
        // answered, or whose answer this process does not live to see, is a
        // send all the same.
        let id = a.attempt_id.clone();
        let sends_before = self.db(move |s| s.count_submit_send(&id)).await?;
        drop(actor);
        let answer = link.call(Request::SubmitAttempt(request)).await;
        drop(sending);
        let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
            return Ok(Sent::NotTaken);
        };
        match answer {
            Ok(Response::SubmitAttempt(r))
                if r.attempt_id == a.attempt_id && r.generation == a.generation =>
            {
                // A resend may find the attempt terminal already; its result
                // comes with the next look, not with this answer.
                let seen = (r.state != AttemptState::Terminal).then_some(Seen {
                    state: r.state,
                    termination: None,
                    receipts: r.receipts,
                });
                let id = a.attempt_id.clone();
                self.db(move |s| s.record_seen(&id, true, seen, Observation::Known, wall_ms()))
                    .await?;
                self.forget_missing(&a.attempt_id);
                Ok(Sent::Taken)
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::InputMissing => {
                // Not taken, nothing stored: not a retry of the task. The
                // input leaves the worker's `ready` and the attempt waits for
                // it there. Only refusals after `ready` came back count; the
                // third retires the attempt.
                // The source the parent sent, not the one the worker named.
                let field = |k: &str| {
                    live.placement
                        .as_ref()
                        .and_then(|p| p.demand["payload"][k].as_str().map(str::to_string))
                };
                let source = field("source_id").and_then(|s| crate::ids::SourceId::parse(&s).ok());
                let run = field("run_id").and_then(|s| crate::ids::RunId::parse(&s).ok());
                let (Some(source), Some(run)) = (source, run) else {
                    // Nothing to wait for: sending it again would be refused
                    // the same way on every tick.
                    self.exclude_for_input(a, "no_source_in_payload").await?;
                    self.observe(&a.attempt_id, Observation::Known).await?;
                    return Ok(Sent::NotTaken);
                };
                link.withdraw_input(&source);
                match self.refused_for_input(&a.attempt_id, run.clone(), source.clone()) {
                    None => {
                        self.forget_missing(&a.attempt_id);
                        self.note_refusal(&a.attempt_id, "submit_input_missing", &e)
                            .await?;
                        self.exclude_for_input(a, "refused_three_times").await?;
                    }
                    Some(strikes) => {
                        let (aid, payload) = (
                            a.attempt_id.clone(),
                            json!({
                                "worker": a.worker_id.to_string(),
                                "source": source.to_string(),
                                "strikes": strikes,
                            }),
                        );
                        self.db(move |s| {
                            s.append_event(
                                Some(&run),
                                Some(&aid),
                                EVENT_INPUT_WAIT,
                                &payload,
                                wall_ms(),
                            )
                            .map(|_| ())
                        })
                        .await?;
                    }
                }
                self.observe(&a.attempt_id, Observation::Known).await?;
                Ok(Sent::NotTaken)
            }
            Err(LinkError::Remote(e)) => {
                let mismatch = e.conflict_reason == Some(ConflictReason::PayloadMismatch);
                if e.overloaded_reason == Some(OverloadedReason::AttemptRecords) {
                    // Spec 25.6 (c): the worker holds its bound of records of
                    // this parent; what it can let go of is released.
                    self.reconcile_due(&a.worker_id);
                }
                let unstored = match e.overloaded_reason {
                    Some(OverloadedReason::Capacity) if replaceable => Some(Unstored::Capacity),
                    Some(OverloadedReason::AttemptIdInUse) if replaceable => {
                        Some(Unstored::AttemptIdInUse)
                    }
                    _ => None,
                };
                let once = policy::sent_once(sends_before);
                if unstored == Some(Unstored::Capacity) {
                    // Until its `inspect` answers room, nothing new goes
                    // there, whatever this parent counts.
                    self.capacity_refused
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(a.worker_id.clone());
                }
                match policy::submit_refusal(e.code, mismatch, unstored, once) {
                    Refusal::Fenced => Err(self.fence()),
                    Refusal::Close(why) => {
                        // The worker stored nothing of this attempt and the
                        // one copy of its submit that ever left was answered:
                        // none exists anywhere. No retire is sent; the task
                        // is free with this commit.
                        if self.move_on(a, &e, why, false).await? {
                            report.retired.push(a.attempt_id.clone());
                        } else {
                            self.observe(&a.attempt_id, Observation::Known).await?;
                        }
                        Ok(not_taken(why))
                    }
                    Refusal::Replace(why) => {
                        // The worker stored nothing of this attempt, but a
                        // copy of the submit may still be on its way and
                        // could be taken later, once there is room or the
                        // other member's attempt is gone. So the attempt is
                        // retired there first (the tombstone is this
                        // member's own and refuses that copy); the task is
                        // free once the worker confirmed.
                        self.move_on(a, &e, why, true).await?;
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(not_taken(why))
                    }
                    Refusal::Tombstoned => {
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(Sent::NotTaken)
                    }
                    Refusal::Later => {
                        let before = self
                            .note_refusal(&a.attempt_id, "submit_deferred", &e)
                            .await?;
                        if decide::moves_elsewhere(e.code, e.blocked_reason, before) {
                            // The worker keeps refusing for a reason of its
                            // own: take the attempt off it. The task is free
                            // once the worker confirmed, and is then placed
                            // on another worker. The tombstone's reason is
                            // what leaves this worker out; why is written
                            // with it, in the same transaction.
                            let excluded = json!({
                                "worker": a.worker_id.to_string(),
                                "code": e.code.to_string(),
                                "reason": e.blocked_reason,
                            });
                            let id = a.attempt_id.clone();
                            self.db(move |s| {
                                s.begin_retire_noting(
                                    &id,
                                    scheduler::REASON_WORKER_CANNOT_RUN,
                                    Some((scheduler::EVENT_WORKER_EXCLUDED, &excluded)),
                                    wall_ms(),
                                )
                            })
                            .await?;
                        }
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(Sent::NotTaken)
                    }
                    Refusal::Never => {
                        self.note_refusal(&a.attempt_id, "submit_refused", &e)
                            .await?;
                        self.retire_durably(&a.attempt_id, REASON_SUBMIT_REFUSED)
                            .await?;
                        self.observe(&a.attempt_id, Observation::Known).await?;
                        Ok(Sent::NotTaken)
                    }
                }
            }
            // Sent or not, accepted or not: nothing is known.
            _ => {
                self.unknown(&a.attempt_id, report).await?;
                Ok(Sent::NotTaken)
            }
        }
    }

    /// Whether `attempt` was refused for its input and the worker's
    /// `ready` does not hold that input again yet ([`InputWait::Waits`]),
    /// or has not for [`INPUT_WAIT`] since the mark ([`InputWait::Expired`]).
    /// A link that keeps no `ready` never withholds.
    fn input_wait(&self, link: &dyn WorkerLink, attempt: &AttemptId) -> InputWait {
        let marks = self.input_missing.lock().unwrap_or_else(|e| e.into_inner());
        let Some(mark) = marks.get(attempt) else {
            return InputWait::Send;
        };
        let waits = link.placement_inputs().is_some_and(|inputs| {
            !inputs
                .ready
                .iter()
                .any(|i| i.run_id == mark.run && i.source_id == mark.source)
        });
        if !waits {
            InputWait::Send
        } else if mark.marked.elapsed() >= INPUT_WAIT {
            InputWait::Expired
        } else {
            InputWait::Waits
        }
    }

    /// Mark `attempt` refused for its input. Returns the strikes counted
    /// so far, or `None` for the third refusal in a row after its input was
    /// ready again: then it is taken off its worker.
    fn refused_for_input(
        &self,
        attempt: &AttemptId,
        run: crate::ids::RunId,
        source: crate::ids::SourceId,
    ) -> Option<u8> {
        let mut marks = self.input_missing.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mark) = marks.get_mut(attempt) {
            // (sent while marked: `ready` had come back)
            mark.strikes = mark.strikes.saturating_add(1);
            mark.marked = Instant::now();
            return (mark.strikes < 3).then_some(mark.strikes);
        }
        if marks.len() >= INPUT_MISSING_MARKS {
            let oldest = marks
                .iter()
                .min_by_key(|(_, m)| m.order)
                .map(|(a, _)| a.clone());
            if let Some(oldest) = oldest {
                marks.remove(&oldest);
            }
        }
        let order = marks.values().map(|m| m.order + 1).max().unwrap_or(0);
        marks.insert(
            attempt.clone(),
            MissingMark {
                run,
                source,
                strikes: 0,
                marked: Instant::now(),
                order,
            },
        );
        Some(0)
    }

    /// Take `a` off its worker for its input, and leave that worker out for
    /// its task, as for a worker that keeps refusing it.
    async fn exclude_for_input(&self, a: &AttemptRecord, why: &str) -> Result<()> {
        let excluded = json!({
            "worker": a.worker_id.to_string(),
            "code": ErrorCode::InputMissing.to_string(),
            "reason": REASON_INPUT_MISSING,
            "why": why,
        });
        let id = a.attempt_id.clone();
        self.db(move |s| {
            s.begin_retire_noting(
                &id,
                scheduler::REASON_WORKER_CANNOT_RUN,
                Some((scheduler::EVENT_WORKER_EXCLUDED, &excluded)),
                wall_ms(),
            )
            .map(|_| ())
        })
        .await
    }

    /// Drop the marks of attempts that are no longer live (gone, terminal,
    /// retired or being retired, cancelled): they are never sent again.
    async fn prune_missing(&self) -> Result<()> {
        let marked: Vec<AttemptId> = {
            let marks = self.input_missing.lock().unwrap_or_else(|e| e.into_inner());
            if marks.is_empty() {
                return Ok(());
            }
            marks.keys().cloned().collect()
        };
        let gone = self
            .db(move |s| {
                let mut gone = Vec::new();
                for id in marked {
                    let live = match s.get_attempt(&id)? {
                        Some(r) => {
                            r.state != AttemptState::Terminal
                                && !r.retired
                                && r.cancel_requested_at_ms.is_none()
                                && s.retire_tombstone(&id)?.is_none()
                        }
                        None => false,
                    };
                    if !live {
                        gone.push(id);
                    }
                }
                Ok(gone)
            })
            .await?;
        let mut marks = self.input_missing.lock().unwrap_or_else(|e| e.into_inner());
        for id in gone {
            marks.remove(&id);
        }
        Ok(())
    }

    fn forget_missing(&self, attempt: &AttemptId) {
        self.input_missing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(attempt);
    }

    /// The worker stored nothing of an attempt and will not take it as it
    /// is: let its task go on as the next generation. With `retire` the
    /// attempt is retired on the worker first (the task is free once that is
    /// confirmed); without, it is closed here and now. Why is written with
    /// the tombstone, in one transaction. Returns whether the attempt was
    /// closed by this call.
    ///
    /// An attempt that cannot be closed as it is recorded now (a cancel or
    /// a retire was asked for meanwhile) takes the retire path instead.
    async fn move_on(
        &self,
        a: &AttemptRecord,
        e: &ApiError,
        why: Unstored,
        retire: bool,
    ) -> Result<bool> {
        let (reason, kind, word) = match why {
            Unstored::Capacity => (
                REASON_CAPACITY_REFUSED,
                EVENT_SUBMIT_REFUSED_CAPACITY,
                "capacity",
            ),
            Unstored::AttemptIdInUse => (
                REASON_ATTEMPT_ID_IN_USE,
                EVENT_SUBMIT_REFUSED_ATTEMPT_ID,
                REASON_ATTEMPT_ID_IN_USE,
            ),
        };
        let id = a.attempt_id.clone();
        let (worker, code) = (a.worker_id.to_string(), e.code.to_string());
        self.db(move |s| {
            let note = |retire: bool| {
                json!({"worker": worker, "code": code, "reason": word, "retire": retire})
            };
            if !retire && s.close_unstored(&id, reason, (kind, &note(false)), wall_ms())? {
                return Ok(true);
            }
            s.begin_retire_noting(&id, reason, Some((kind, &note(true))), wall_ms())?;
            Ok(false)
        })
        .await
    }

    async fn cancel(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        status: Option<AttemptStatus>,
        report: &mut TickReport,
        asked: &Attached,
    ) -> Result<()> {
        if let Some(status) = &status {
            let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
                return Ok(());
            };
            self.mirror(status, report).await?;
        }
        let request = Request::CancelAttempt(CancelAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
            reason: None,
        });
        let answer = link.call(request).await;
        if matches!(&answer, Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch) {
            return Err(self.fence());
        }
        let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
            return Ok(());
        };
        match answer {
            Ok(Response::CancelAttempt(_)) => {
                self.observe(&a.attempt_id, Observation::Known).await?;
                Ok(())
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => Err(self.fence()),
            Err(LinkError::Remote(e)) => {
                let payload = json!({"code": e.code.to_string()});
                self.note(&a.attempt_id, "cancel_refused", payload).await
            }
            _ => self.unknown(&a.attempt_id, report).await,
        }
    }

    async fn retire(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
        report: &mut TickReport,
        asked: &Attached,
    ) -> Result<()> {
        let a = &live.record;
        let request = Request::RetireAttempt(RetireAttemptRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
        });
        let status = match link.call(request).await {
            Ok(Response::RetireAttempt(r)) if r.attempt_id == a.attempt_id && r.tombstone => {
                r.status
            }
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => {
                return Err(self.fence())
            }
            Err(LinkError::Remote(e)) => {
                let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
                    return Ok(());
                };
                let payload = json!({"code": e.code.to_string()});
                return self.note(&a.attempt_id, "retire_refused", payload).await;
            }
            _ => {
                let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
                    return Ok(());
                };
                return self.unknown(&a.attempt_id, report).await;
            }
        };
        // A worker that says the attempt is over is asked how it ended: the
        // word of the answer alone never frees the task.
        let (ended, seen) = if status == RetireStatus::Unconfirmed {
            (Ended::Unknown, None)
        } else {
            self.ended(link, a).await
        };
        let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
            return Ok(());
        };
        match (policy::retire_verdict(status, ended), seen) {
            (RetireVerdict::Confirmed, _) => {
                // What the confirmation means (free the task, or settle the
                // attempt because the task was cancelled or refused) is read
                // in the commit: a cancel may have come since `live` was read.
                let id = a.attempt_id.clone();
                let end = self
                    .db(move |s| s.finish_retire(&id, REASON_SUBMIT_REFUSED, wall_ms()))
                    .await?;
                match end {
                    RetireEnd::Freed => report.retired.push(a.attempt_id.clone()),
                    RetireEnd::Settled(_) => report.settled.push(a.attempt_id.clone()),
                }
                Ok(())
            }
            (RetireVerdict::Adopt, Some(status)) => self.mirror(&status, report).await,
            (RetireVerdict::Wait, _) => {
                self.observe(&a.attempt_id, Observation::Known).await?;
                Ok(())
            }
            (RetireVerdict::Unknown, _) | (RetireVerdict::Adopt, None) => {
                self.unknown(&a.attempt_id, report).await
            }
        }
    }

    /// How a retired attempt stands on its worker now.
    async fn ended(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
    ) -> (Ended, Option<AttemptStatus>) {
        match self.ask(link, a).await {
            (Answer::Tombstone, _) => (Ended::Never, None),
            (Answer::Status(AttemptState::Terminal), Some(status)) if status.retired => {
                match status.termination {
                    Some(t) => (Ended::With(t.outcome()), Some(status)),
                    None => (Ended::Unknown, None),
                }
            }
            _ => (Ended::Unknown, None),
        }
    }

    async fn renew(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        state: AttemptState,
        report: &mut TickReport,
        asked: &Attached,
    ) -> Result<()> {
        if matches!(state, AttemptState::Cancelling | AttemptState::Finalizing) {
            return Ok(());
        }
        let request = Request::RenewLease(RenewLeaseRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
            lease_seq: self.lease_seq.fetch_add(1, Ordering::SeqCst) + 1,
            lease_ms: self.cfg.lease_ms,
        });
        match link.call(request).await {
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => Err(self.fence()),
            Ok(Response::RenewLease(r)) if r.extended && r.attempt_id == a.attempt_id => {
                self.note_trial(&a.attempt_id, true);
                Ok(())
            }
            // Renewed, or not renewable any more: the next look says why.
            Ok(_) | Err(LinkError::Remote(_)) => Ok(()),
            Err(LinkError::Unconfirmed) => {
                let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
                    return Ok(());
                };
                self.unknown(&a.attempt_id, report).await
            }
        }
    }

    /// Place queued tasks, oldest first. For each task the workers that
    /// answered in this tick, are not draining and have room are asked for
    /// a forecast side by side, and the scheduler's rules choose among them
    /// (`scheduler::decide`). The commit counts the winner's load again and
    /// records the decision with the placement; the submit follows the
    /// commit, and a worker that does not take it is left alone for the
    /// rest of the tick.
    async fn dispatch(
        &self,
        round: &mut Round,
        report: &mut TickReport,
        telemetry: &Mutex<Vec<preparation::RawPage>>,
    ) -> Result<()> {
        if self.in_trial() || self.stopping() {
            return Ok(());
        }
        self.prune_missing().await?;
        // Tasks every attached worker is excluded for come last: however
        // many of them wait, they do not fill the scan in front of a task
        // that can be placed. A worker that is attached later is not
        // excluded for them, which puts them back in their place by age.
        let attached: Vec<WorkerId> = round.known.iter().cloned().collect();
        let eligible = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            if workers.is_empty() {
                None
            } else {
                workers
                    .values()
                    .try_fold(BTreeSet::new(), |mut ids, link| {
                        let inputs = link.placement_inputs()?;
                        ids.extend(inputs.ready.into_iter().map(|i| i.run_id));
                        // A run whose input a worker refused is looked at
                        // too: its wait gets a reason.
                        ids.extend(link.input_refusals().into_iter().map(|(run, _)| run));
                        Some(ids)
                    })
                    .map(|ids| ids.into_iter().collect::<Vec<_>>())
            }
        };
        let filtered = eligible.is_some();
        let cursor = *self.input_cursor.lock().unwrap_or_else(|e| e.into_inner());
        let queued = self
            .db(move |s| {
                if let Some(eligible) = eligible {
                    s.queued_tasks_ready_page(DISPATCH_SCAN, &eligible, cursor)
                } else {
                    s.queued_tasks_eligible(
                        DISPATCH_SCAN,
                        &attached,
                        scheduler::REASON_WORKER_CANNOT_RUN,
                        None,
                    )
                }
            })
            .await?;
        if !filtered && queued.len() < DISPATCH_SCAN as usize {
            // The whole queue was read: a task that is not in it waits no
            // more.
            let ids = queued.iter().map(|t| t.task_id.clone()).collect();
            self.sched.keep_waiting(&ids);
        }
        let page_complete = queued.len() < DISPATCH_SCAN as usize;
        let mut next_cursor = None;
        let page_len = queued.len();
        let mut processed = 0;
        for task in queued {
            if self.stopping() {
                break;
            }
            self.join_newcomers(round, report, telemetry).await?;
            let evaluated: Vec<_> = round
                .open
                .iter()
                .filter_map(|w| {
                    let snapshot = w.link.placement_inputs()?;
                    snapshot
                        .ready
                        .iter()
                        .any(|i| {
                            i.run_id == task.run_id
                                && Some(&i.source_id) == task.source_id.as_ref()
                                && task.spec["manifest_digest"].as_str()
                                    == Some(i.manifest_digest.as_str())
                        })
                        .then(|| (Arc::clone(&w.link), snapshot.generation))
                })
                .collect();
            self.place_one(&task, round, report).await?;
            for (link, generation) in evaluated {
                link.inputs_evaluated(generation, &task.run_id);
            }
            processed += 1;
            next_cursor = Some((task.created_at_ms, task.queue_rowid));
            if round.open.is_empty() || Instant::now() >= round.deadline {
                break;
            }
        }
        if filtered {
            *self.input_cursor.lock().unwrap_or_else(|e| e.into_inner()) =
                if page_complete && processed == page_len {
                    None
                } else {
                    next_cursor
                };
        }
        Ok(())
    }

    /// Workers that were attached since this tick began are looked at like
    /// the others (their attempts are followed first), so that the next
    /// decision counts them in.
    async fn join_newcomers(
        &self,
        round: &mut Round,
        report: &mut TickReport,
        telemetry: &Mutex<Vec<preparation::RawPage>>,
    ) -> Result<()> {
        let fresh: Vec<(WorkerId, Arc<dyn WorkerLink>)> = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            workers
                .iter()
                .filter(|(w, _)| !round.known.contains(*w))
                .map(|(w, l)| (w.clone(), Arc::clone(l)))
                .collect()
        };
        for (worker_id, link) in fresh {
            round.known.insert(worker_id.clone());
            let (reached, part) = self
                .follow(worker_id.clone(), link, round.deadline, telemetry)
                .await?;
            report.absorb(part);
            match reached {
                Some(worker) => round.admit(worker),
                None => {
                    round.out.insert(worker_id, Exclusion::Unreached);
                }
            }
        }
        Ok(())
    }

    /// The workers that refused a submit for capacity and have not answered
    /// room since. Read only: an estimate looks at it and changes nothing.
    pub(crate) fn refused_for_capacity(&self) -> BTreeSet<WorkerId> {
        self.capacity_refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The workers a task is not placed on again: those an attempt of it was
    /// taken off because they kept refusing it.
    async fn refused_by(&self, task_id: &TaskId) -> Result<BTreeSet<WorkerId>> {
        let task = task_id.clone();
        self.db(move |s| s.excluded_workers(&task, scheduler::REASON_WORKER_CANNOT_RUN))
            .await
    }

    /// Ask one worker what the task would take there, on top of what the
    /// parent placed there and the worker has not acknowledged.
    async fn forecast(
        &self,
        worker: &Reached,
        attempt_id: &AttemptId,
        spec: &TaskSpec,
        costs: &decide::Costs,
        pending: Vec<WorkloadPlan>,
    ) -> Forecast {
        let request = Request::Forecast(ForecastRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            workload: policy::workload_of_spec(attempt_id, spec, decide::ready_delay_ms(costs)),
            pending,
        });
        let wait = Duration::from_millis(self.sched.cfg.forecast_wait_ms);
        // The age of the answer counts from before the question left.
        let sent = Instant::now();
        match tokio::time::timeout(wait, worker.link.call(request)).await {
            Ok(Ok(Response::Forecast(answer))) => Forecast::Answer(sent, Box::new(answer)),
            Ok(Err(LinkError::Remote(e))) if e.code == ErrorCode::StaleEpoch => Forecast::Fenced,
            Ok(Err(LinkError::Remote(_))) => Forecast::Refused,
            _ => Forecast::Silent,
        }
    }

    /// Decide where one task goes, commit that, and send it.
    async fn place_one(
        &self,
        task: &QueuedTask,
        round: &mut Round,
        report: &mut TickReport,
    ) -> Result<()> {
        let spec: TaskSpec = match serde_json::from_value(task.spec.clone()) {
            Ok(spec) => spec,
            // This task only; the ones behind it are placed.
            Err(e) => return self.note_unplaceable(task, format!("task spec: {e}")).await,
        };
        if spec.ci.as_ref().is_some_and(|ci| ci.hold) {
            // It waits for the jobs it needs; the queue does not list it.
            return Ok(());
        }
        let unmet_ci = |w: &Reached| policy::unmet_ci(&spec, w.link.as_ref());
        let parent_local = self
            .parent_local
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let readiness = |w: &Reached| {
            if spec.pinned_local() && parent_local.as_ref() != Some(&w.worker_id) {
                return None;
            }
            match w.link.placement_inputs() {
                None => Some(None),
                Some(snapshot) => snapshot
                    .ready
                    .iter()
                    .any(|i| {
                        i.run_id == task.run_id
                            && Some(&i.source_id) == task.source_id.as_ref()
                            && i.manifest_digest == spec.manifest_digest
                    })
                    .then_some(Some(snapshot.generation)),
            }
        };
        let ready = |w: &Reached| readiness(w).is_some();
        // A worker that did not take the input of the run (the frozen plan
        // of a CI run it has no room for) is left out by name, so the wait
        // has a reason where the task is shown.
        let refusal =
            |w: &Reached| policy::input_refusal(ready(w), w.link.input_refusals(), &task.run_id);
        let measured_cold = |w: &Reached| {
            if spec.in_place || !w.link.cold_eligible(&task.task_id) {
                return false;
            }
            let query = scheduler::measured::CostQuery::task(
                &task.spec,
                task.source_id.as_ref().map(|s| s.as_str()),
                w.link.cost_transport(),
            );
            let costs = self.sched.query(&w.worker_id, &query);
            costs.ms[decide::TRANSFER].is_some() && !costs.assumed[decide::TRANSFER]
        };
        if !round.open.is_empty() && !round.open.iter().any(|w| ready(w) || refusal(w).is_some()) {
            return Ok(());
        }
        let mut toolchain_eligible = BTreeSet::new();
        // Asked and silent within the wait: no answer, not a refusal.
        let mut toolchain_silent = BTreeSet::new();
        if let Some(bundle) = &spec.toolchains {
            // The same question of the same worker is asked once a tick: the
            // tasks of a run share one bundle.
            let asked = (
                bundle.digest(),
                serde_json::to_string(&spec.requirements).unwrap_or_default(),
            );
            let workers: Vec<(WorkerId, Arc<dyn WorkerLink>)> = round
                .open
                .iter()
                .filter(|w| ready(w))
                .chain(round.open.iter().filter(|w| !ready(w) && measured_cold(w)))
                .map(|w| (w.worker_id.clone(), Arc::clone(&w.link)))
                .collect();
            for (worker_id, link) in workers {
                let key = (asked.clone(), worker_id.clone());
                if let Some(eligible) = round.toolchain_eligible.get(&key) {
                    if *eligible {
                        toolchain_eligible.insert(worker_id);
                    }
                    continue;
                }
                let remaining = round.deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let wait = Duration::from_millis(self.sched.cfg.forecast_wait_ms).min(remaining);
                let request = Request::ResolveToolchains(crate::wire::ResolveToolchainsRequest {
                    schema_version: crate::wire::SchemaVersion::CURRENT,
                    request_id: RequestId::generate(),
                    bundle: bundle.clone(),
                    requirements: spec.requirements.clone(),
                });
                // Only an answer is kept; a silent worker is asked again.
                match tokio::time::timeout(wait, link.read_only_call(request)).await {
                    Ok(Ok(Response::ResolveToolchains(response))) => {
                        let eligible = response.bundle == *bundle;
                        round.toolchain_eligible.insert(key, eligible);
                        if eligible {
                            toolchain_eligible.insert(worker_id);
                        }
                    }
                    Err(_) => {
                        toolchain_silent.insert(worker_id);
                    }
                    Ok(_) => {}
                }
            }
        }
        let refused = self.refused_by(&task.task_id).await?;
        // Holders of the setup tools that gave no usable forecast for this
        // task: left out of its later passes only (spec 22.1).
        let mut holders_out: BTreeSet<WorkerId> = BTreeSet::new();
        loop {
            // Who has room, counted from the parent's own record.
            let ids: Vec<WorkerId> = round.open.iter().map(|w| w.worker_id.clone()).collect();
            // Of the unacknowledged placements only the plans a question may
            // carry are kept, per worker; their demand stays in the store.
            let (loads, pending) = self
                .db(move |s| {
                    let mut loads: BTreeMap<WorkerId, WorkerLoad> = BTreeMap::new();
                    let mut pending: BTreeMap<WorkerId, Vec<WorkloadPlan>> = BTreeMap::new();
                    for w in ids {
                        let load = s.worker_load(&w)?;
                        pending.insert(w.clone(), scheduler::pending_plans(s, &w)?);
                        loads.insert(w, load);
                    }
                    Ok((loads, pending))
                })
                .await?;
            let full: Vec<WorkerId> = round
                .open
                .iter()
                .filter(|w| !policy::has_room(loads[&w.worker_id], w.limits))
                .map(|w| w.worker_id.clone())
                .collect();
            for worker_id in &full {
                round.close(worker_id, Exclusion::Full);
            }
            // A worker whose window is full with the work of every member
            // takes nothing either, however little of it is this parent's.
            let capacity_refused = self.refused_for_capacity();
            let closed: Vec<WorkerId> = round
                .open
                .iter()
                .filter(|w| {
                    policy::window_closed(
                        capacity_refused.contains(&w.worker_id),
                        w.admission.as_ref(),
                        loads[&w.worker_id],
                    )
                })
                .map(|w| w.worker_id.clone())
                .collect();
            for worker_id in &closed {
                round.close(worker_id, Exclusion::Capacity);
            }
            let mut excluded: Vec<Excluded> = round
                .out
                .iter()
                .map(|(w, why)| Excluded {
                    unmet: Vec::new(),
                    worker_id: w.clone(),
                    why: *why,
                })
                .collect();
            excluded.extend(round.open.iter().filter_map(|w| {
                refusal(w).map(|why| Excluded {
                    unmet: vec![why],
                    worker_id: w.worker_id.clone(),
                    why: Exclusion::InputRefused,
                })
            }));
            let (compatible, mismatched): (Vec<&Reached>, Vec<&Reached>) = round
                .open
                .iter()
                .filter(|w| refusal(w).is_none() && (ready(w) || measured_cold(w)))
                .partition(|w| {
                    unmet_ci(w).is_none()
                        && (spec.toolchains.is_none() || toolchain_eligible.contains(&w.worker_id))
                        && crate::toolchain::capability::missing_with_installable(
                            &spec.requirements,
                            w.capabilities.as_ref(),
                            spec.toolchains.as_ref(),
                        )
                        .is_empty()
                });
            excluded.extend(mismatched.into_iter().map(|w| Excluded {
                unmet: if let Some(why) = unmet_ci(w) {
                    vec![why.to_string()]
                } else {
                    let mut unmet = crate::toolchain::envdiff::with_steps(
                        crate::toolchain::capability::missing(
                            &spec.requirements,
                            w.capabilities.as_ref(),
                        ),
                        &spec.requirements.requires,
                        w.capabilities.as_ref(),
                    );
                    // Excluded by the frozen toolchain alone: name what it
                    // asks for.
                    if unmet.is_empty() && !toolchain_eligible.contains(&w.worker_id) {
                        if let Some(bundle) = &spec.toolchains {
                            unmet.extend(bundle.frozen.requirements.iter().map(|r| {
                                match toolchain_silent.contains(&w.worker_id) {
                                    true => {
                                        format!("toolchain {} not answered in time", r.describe())
                                    }
                                    false => {
                                        format!("toolchain {} unavailable", r.describe())
                                    }
                                }
                            }));
                        }
                    }
                    unmet
                },
                worker_id: w.worker_id.clone(),
                why: Exclusion::CapabilityMismatch,
            }));
            let (asked, left_out): (Vec<&Reached>, Vec<&Reached>) =
                compatible.into_iter().partition(|w| {
                    !refused.contains(&w.worker_id) && !holders_out.contains(&w.worker_id)
                });
            excluded.extend(left_out.into_iter().map(|w| Excluded {
                unmet: Vec::new(),
                worker_id: w.worker_id.clone(),
                why: Exclusion::RefusedTask,
            }));
            // A worker that has the job's setup tools is preferred to one
            // that would install them (spec 22.1): once the workers that refused
            // the task are left out, while one is asked the others are not.
            // If none of the holders gives a usable forecast, they are closed
            // below and the installers are asked in the next pass.
            let setup_tools: Vec<&String> = spec
                .requirements
                .requires
                .iter()
                .filter_map(|r| match r {
                    crate::toolchain::capability::Requirement::Setup(name) => Some(name),
                    _ => None,
                })
                .collect();
            let has_setup_tools = |w: &Reached| {
                w.capabilities
                    .as_ref()
                    .is_some_and(|c| setup_tools.iter().all(|t| c.path_tools.contains(*t)))
            };
            let mut holders: Vec<WorkerId> = Vec::new();
            let asked = if !setup_tools.is_empty() && asked.iter().any(|w| has_setup_tools(w)) {
                let (has, installs): (Vec<&Reached>, Vec<&Reached>) =
                    asked.into_iter().partition(|w| has_setup_tools(w));
                holders = has.iter().map(|w| w.worker_id.clone()).collect();
                excluded.extend(installs.into_iter().map(|w| Excluded {
                    unmet: vec![format!(
                        "another worker has {} on its PATH; belt installs setup tools only where none has them",
                        setup_tools
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )],
                    worker_id: w.worker_id.clone(),
                    why: Exclusion::CapabilityMismatch,
                }));
                has
            } else {
                asked
            };
            let sessions: BTreeMap<_, _> = asked
                .iter()
                .filter_map(|w| readiness(w).map(|generation| (w.worker_id.clone(), generation)))
                .collect();

            let mut costs: Vec<decide::Costs> = asked
                .iter()
                .map(|w| {
                    let mut query = scheduler::measured::CostQuery::task(
                        &task.spec,
                        task.source_id.as_ref().map(|s| s.as_str()),
                        w.link.cost_transport(),
                    );
                    query.ready = readiness(w).is_some_and(|generation| generation.is_some());
                    // Preserve external injected models through the existing
                    // task adapter; a preview assembles the same costs.
                    scheduler::preview::merge_costs(
                        self.sched.query(&w.worker_id, &query),
                        self.sched.costs(&w.worker_id, task),
                    )
                })
                .collect();
            decide::fill_costs(&mut costs);
            let attempt_id = AttemptId::generate();
            let questions: Vec<Pin<Box<dyn Future<Output = Forecast> + Send + '_>>> = asked
                .iter()
                .zip(&costs)
                .map(|(w, c)| {
                    let pending = pending.get(&w.worker_id).cloned().unwrap_or_default();
                    Box::pin(self.forecast(w, &attempt_id, &spec, c, pending))
                        as Pin<Box<dyn Future<Output = Forecast> + Send + '_>>
                })
                .collect();
            let answers = join_all(questions).await;
            let now = Instant::now();
            let ttl = self.sched.cfg.rules.forecast_ttl_ms;
            let mut candidates = Vec::new();
            let mut silent = Vec::new();
            let mut forecast_ttl = BTreeMap::new();
            for ((w, c), answer) in asked.iter().zip(&costs).zip(answers) {
                let outlook = match answer {
                    Forecast::Fenced => return Err(self.fence()),
                    Forecast::Silent => {
                        silent.push(w.worker_id.clone());
                        continue;
                    }
                    Forecast::Refused => Outlook::unusable(decide::Unusable::Refused),
                    Forecast::Answer(sent, answer) => {
                        let age = now.saturating_duration_since(sent).as_millis();
                        forecast_ttl.insert(
                            w.worker_id.clone(),
                            ttl.min(answer.ttl_ms)
                                .saturating_sub(u64::try_from(age).unwrap_or(u64::MAX)),
                        );
                        let asked = Asked {
                            worker_id: &w.worker_id,
                            plan_digest: &spec.plan_digest,
                            boot_id: &w.boot_id,
                            agent_epoch: w.agent_epoch,
                        };
                        decide::outlook_of(
                            &asked,
                            &answer,
                            u64::try_from(age).unwrap_or(u64::MAX),
                            ttl,
                        )
                    }
                };
                // An unusable/expired cold forecast cannot authorize a wait
                // hint; ready workers retain the existing unknown-outlook rule.
                if !scheduler::preview::forecast_counts(
                    ready(w),
                    outlook.unusable.is_some(),
                    forecast_ttl.get(&w.worker_id).copied().unwrap_or(0),
                ) {
                    excluded.push(Excluded {
                        worker_id: w.worker_id.clone(),
                        why: Exclusion::NoForecast,
                        unmet: vec![],
                    });
                    continue;
                }
                let load = loads[&w.worker_id];
                candidates.push(Candidate {
                    worker_id: w.worker_id.clone(),
                    priority: self.sched.priority_of(&w.worker_id),
                    load: load.active.saturating_add(load.pending),
                    costs: *c,
                    outlook,
                });
            }
            for worker_id in &silent {
                // It gets no more questions in this tick: the others do not
                // wait for it again.
                round.close(worker_id, Exclusion::NoForecast);
                excluded.push(Excluded {
                    unmet: Vec::new(),
                    worker_id: worker_id.clone(),
                    why: Exclusion::NoForecast,
                });
            }
            // The holders were silent, refused in their forecast or gave none
            // that counts: they are out of this task's next pass, so the
            // workers that install the tools are asked.
            if !holders.is_empty()
                && !candidates.iter().any(|c| {
                    holders.contains(&c.worker_id)
                        && c.outlook.unusable != Some(decide::Unusable::Refused)
                })
            {
                holders_out.extend(holders);
                continue;
            }
            let decision = decide::decide_for_shard(
                candidates,
                excluded,
                self.sched.cfg.rules,
                spec.ci
                    .as_ref()
                    .and_then(|c| c.shard_preferred_worker.as_ref()),
            );
            let Some(winner) = decision.winner.clone() else {
                return self.note_waiting(task, &decision).await;
            };
            if let Some(cold) = round
                .open
                .iter()
                .find(|w| w.worker_id == winner && !ready(w))
            {
                let query = scheduler::measured::CostQuery::task(
                    &task.spec,
                    task.source_id.as_ref().map(|s| s.as_str()),
                    cold.link.cost_transport(),
                );
                if let Some(expected) = self.sched.query(&winner, &query).ms[decide::TRANSFER] {
                    if cold.link.prefer(
                        task,
                        expected,
                        forecast_ttl.get(&winner).copied().unwrap_or(0),
                    ) {
                        return self.note_waiting(task, &decision).await;
                    }
                }
                // First wait expired while forecasts were in flight. Re-run
                // the same comparison with that cold candidate suppressed.
                continue;
            }
            let Some(limits) = round
                .open
                .iter()
                .find(|w| w.worker_id == winner)
                .map(|w| w.limits)
            else {
                return Ok(());
            };
            // Lock order is registry then source generation. Owned guards move
            // into the blocking DB closure, so dropping this future cannot let a
            // connection exchange/reset overtake an already-started commit.
            let registry_guard = self.placement_fence.clone().lock_owned().await;
            let source_fence = round
                .open
                .iter()
                .find(|w| w.worker_id == winner)
                .and_then(|w| w.link.placement_fence());
            let source_guard = match source_fence {
                Some(fence) => Some(fence.lock_owned().await),
                None => None,
            };
            if !round
                .open
                .iter()
                .any(|w| w.worker_id == winner && readiness(w) == sessions.get(&winner).copied())
            {
                return Ok(());
            }
            let same_attachment = {
                let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
                workers.get(&winner).is_some_and(|current| {
                    round
                        .open
                        .iter()
                        .any(|w| w.worker_id == winner && Arc::ptr_eq(current, &w.attachment))
                })
            };
            if !same_attachment {
                return Ok(());
            }
            if let Some(Some(generation)) = sessions.get(&winner) {
                let current = self
                    .workers
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&winner)
                    .and_then(|link| link.placement_inputs())
                    .map(|inputs| inputs.generation);
                if current != Some(*generation) {
                    return Ok(());
                }
            }
            // A worker followed side by side with this placing may have said
            // that this coordinator is replaced: then nothing more is placed.
            self.check_fenced()?;
            let (t, w, a) = (task.task_id.clone(), winner.clone(), attempt_id.clone());
            let lease_ms = self.cfg.lease_ms;
            let cost_evidence: Vec<_> = decision
                .compared
                .iter()
                .filter_map(|compared| {
                    let worker = round
                        .open
                        .iter()
                        .find(|w| w.worker_id == compared.worker_id)?;
                    let mut query = scheduler::measured::CostQuery::task(
                        &task.spec,
                        task.source_id.as_ref().map(|s| s.as_str()),
                        worker.link.cost_transport(),
                    );
                    query.ready = readiness(worker).is_some_and(|generation| generation.is_some());
                    let mut costs = self.sched.evidence(&compared.worker_id, &query)?;
                    costs.fallback(&compared.costs);
                    Some(scheduler::measured::WorkerCostEvidence {
                        worker_id: compared.worker_id.clone(),
                        costs,
                    })
                })
                .collect();
            let record = json!({
                "cost_evidence":cost_evidence,
                "decision": serde_json::to_value(&decision)
                    .map_err(|e| CoordinatorError::Invalid(e.to_string()))?,
                "decision_seq": self.sched.next_seq(),
            });
            let outcome = self
                .db(move |s| {
                    let _registry_guard = registry_guard;
                    let _source_guard = source_guard;
                    s.place(
                        &t,
                        &w,
                        &a,
                        move |load| policy::has_room(load, limits),
                        move |input| {
                            let (hash, mut demand) = demand_of(input, lease_ms)?;
                            demand["decision"] = record["decision"].clone();
                            demand["decision_seq"] = record["decision_seq"].clone();
                            if record["cost_evidence"]
                                .as_array()
                                .is_some_and(|e| !e.is_empty())
                            {
                                demand["cost_evidence"] = record["cost_evidence"].clone();
                            }
                            Ok((hash, demand))
                        },
                        wall_ms(),
                    )
                })
                .await?;
            match outcome {
                // Counted again in the commit, it has no room after all:
                // decide once more without it.
                PlaceOutcome::WorkerFull(_) => round.close(&winner, Exclusion::Full),
                PlaceOutcome::NotQueued | PlaceOutcome::MatrixFull => return Ok(()),
                PlaceOutcome::Unbuildable(why) => {
                    // This task only; the ones behind it are placed.
                    return self.note_unplaceable(task, why).await;
                }
                PlaceOutcome::Placed(_) => {
                    self.clear_prefetch(&crate::service::completion::Target::Task(
                        task.task_id.clone(),
                    ));
                    self.sched.clear_waiting(&task.task_id);
                    report.placed.push(attempt_id.clone());
                    let link = round
                        .open
                        .iter()
                        .find(|w| w.worker_id == winner)
                        .map(|w| (Arc::clone(&w.link), Arc::clone(&w.attachment)));
                    if let Some((link, attachment)) = link {
                        let sent = self
                            .send_placed(&winner, &*link, &attachment, &attempt_id, report)
                            .await?;
                        match sent {
                            // A worker that did not take this one gets no
                            // more in this tick: what is placed there cannot
                            // move until it answers.
                            Sent::NotTaken => round.close(&winner, Exclusion::NotTaking),
                            // Nor does one whose window is full, until its
                            // `inspect` answers room: every tick asks.
                            Sent::WindowFull => round.close(&winner, Exclusion::Capacity),
                            Sent::Taken | Sent::Withheld => {}
                        }
                    }
                    return Ok(());
                }
            }
        }
    }

    /// No worker could be chosen for a task: keep why, and record it on the
    /// run when it is news.
    async fn note_waiting(&self, task: &QueuedTask, decision: &Decision) -> Result<()> {
        // Old CI workers can still run the original job. An explicit fleet-wide
        // discovery feature refusal settles its optional probe; a transient
        // outage, full worker or missing forecast leaves it waiting.
        if !decision.excluded.is_empty()
            && decision.excluded.iter().all(|worker| {
                worker.why == Exclusion::CapabilityMismatch
                    && worker
                        .unmet
                        .iter()
                        .any(|why| why == policy::DISCOVERY_UNMET)
            })
            && task
                .spec
                .pointer("/ci/task")
                .and_then(|t| serde_json::from_value::<crate::wire::CiTask>(t.clone()).ok())
                .is_some_and(|task| crate::wire::is_discovery_task(&task))
        {
            let (run, id) = (task.run_id.clone(), task.task_id.clone());
            self.db(move |s| {
                s.fail_ci_tasks(
                    &run,
                    Some(&id),
                    "workers do not support runner discovery",
                    wall_ms(),
                )
            })
            .await?;
        }
        if !self.sched.note_waiting(&task.task_id, decision) {
            return Ok(());
        }
        let run = task.run_id.clone();
        let payload = json!({
            "task": task.task_id.to_string(),
            "blocked_reason": decide::blocked_reason(decision),
            "decision": serde_json::to_value(decision)
                .map_err(|e| CoordinatorError::Invalid(e.to_string()))?,
        });
        self.db(move |s| {
            s.append_event(
                Some(&run),
                None,
                scheduler::EVENT_TASK_WAITING,
                &payload,
                wall_ms(),
            )
        })
        .await?;
        Ok(())
    }

    /// Read back what the placement committed, and send that. A cancel or a
    /// retire that was recorded since goes first: then nothing is sent, and
    /// the next look at the attempt delivers it.
    async fn send_placed(
        &self,
        worker_id: &WorkerId,
        link: &dyn WorkerLink,
        asked: &Attached,
        attempt_id: &AttemptId,
        report: &mut TickReport,
    ) -> Result<Sent> {
        let (w, a) = (worker_id.clone(), attempt_id.clone());
        let Some(placed) = self.db(move |s| s.live_attempt(&w, &a)).await? else {
            return Ok(Sent::NotTaken);
        };
        let facts = Facts {
            acked: false,
            cancel_requested: placed.record.cancel_requested_at_ms.is_some(),
            retire_requested: placed.retire_reason.is_some(),
        };
        // A placement committed as the resident stops is sent by the next
        // coordinator, like any placement that was never sent.
        if !policy::may_send(facts) || self.stopping() {
            return Ok(Sent::Withheld);
        }
        self.submit(link, &placed, report, asked).await
    }

    /// A queued task no payload can be made for stays queued and is passed
    /// over; why is recorded once (per process) on its run.
    async fn note_unplaceable(&self, task: &QueuedTask, why: String) -> Result<()> {
        let fresh = {
            let mut seen = self.unplaceable.lock().unwrap_or_else(|e| e.into_inner());
            seen.insert(task.task_id.clone())
        };
        if !fresh {
            return Ok(());
        }
        let run = task.run_id.clone();
        let payload = json!({"task": task.task_id.to_string(), "error": why});
        self.db(move |s| s.append_event(Some(&run), None, "task_unplaceable", &payload, wall_ms()))
            .await?;
        Ok(())
    }
}

/// Whether a payload can be made for a task as it is about to be queued: the
/// construction every placement of it will use.
fn check_spec(
    run_id: &RunId,
    source_id: &SourceId,
    task_id: &TaskId,
    spec: &Value,
    lease_ms: u64,
) -> Result<()> {
    let input = PlaceInput {
        permit_id: None,
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        generation: Generation(1),
        source_id: Some(source_id.clone()),
        spec: spec.clone(),
    };
    let invalid = |e: String| CoordinatorError::Invalid(format!("task {task_id}: {e}"));
    demand_of(&input, lease_ms).map_err(|e| invalid(e.to_string()))?;
    let parsed: TaskSpec =
        serde_json::from_value(spec.clone()).map_err(|e| invalid(e.to_string()))?;
    if parsed.ci.is_some() {
        // What every worker would refuse is refused here, before the run exists.
        let payload =
            build_payload(&input, &parsed, lease_ms).map_err(|e| invalid(e.to_string()))?;
        crate::worker::ci_task::check_payload(&payload).map_err(|why| invalid(why.into()))?;
        if !policy::ci_declarations_fit(payload.artifacts.len(), payload.outputs.len()) {
            return Err(invalid(
                "a CI task declares too many artifacts and outputs: its result is one more".into(),
            ));
        }
    }
    Ok(())
}

/// The payload hash and the demand recorded with a placement: the payload
/// itself, so that exactly what was decided can be sent, again if needed.
fn demand_of(
    input: &PlaceInput,
    lease_ms: u64,
) -> crate::storage::Result<(crate::ids::PayloadHash, Value)> {
    let spec: TaskSpec = serde_json::from_value(input.spec.clone())
        .map_err(|e| StorageError::Invalid(format!("task spec: {e}")))?;
    let payload = build_payload(input, &spec, lease_ms)?;
    let hash = payload_hash(&payload).map_err(|e| StorageError::Invalid(e.to_string()))?;
    let stored =
        serde_json::to_value(&payload).map_err(|e| StorageError::Invalid(e.to_string()))?;
    // What is recorded is what will be read back and sent.
    let back: AttemptPayload = serde_json::from_value(stored.clone())
        .map_err(|e| StorageError::Invalid(format!("payload: {e}")))?;
    if back != payload {
        return Err(StorageError::Invalid(
            "the payload does not read back as it was written".into(),
        ));
    }
    let mut demand = json!({ "payload": stored });
    if let Some(ci) = &spec.ci {
        // The payload does not say how many commands the task starts.
        demand["ci_steps"] = json!(ci.steps);
    }
    Ok((hash, demand))
}

#[cfg(test)]
mod paging_tests;
#[cfg(test)]
mod release_tests;
#[cfg(test)]
mod tests;
