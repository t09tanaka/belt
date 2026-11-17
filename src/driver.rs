//! Transactional, host-neutral progression for frozen tasks.
//!
//! Dynamic members are derived from verified source metadata and portable facts.
//! Frozen shards join in core; worker authentication and retries remain host work.
//! Hosts durably commit a proposed transition before applying it and sending
//! its outbox. A claim acknowledges Ready. Before sending execution work or
//! starting task execution, hosts MUST durably commit/apply MarkDispatched. Its marker
//! means may-have-started, not authenticated delivery. Hosts serialize dispatch
//! with cancellation and reconcile crash/replay without sending twice.
//! Admission releases on a real terminal Wire receipt, or a typed host abort
//! asserting execution was never dispatched. The latter trusts the host:
//! drain preparation and queued senders first. Cancel/ACK never proves stopping.
//! After dispatch, errors (including run_task errors after partial execution) need
//! a real receipt or a separately designed authenticated fenced-stop path.
//! Worker ownership/source/receipt authentication remains the host's duty.

#![doc = include_str!("driver/RESULT_STORAGE.md")]

use crate::dag::{ExecutionTask, JobNode, NodeKind};
pub(crate) mod dispatch;
use crate::frozen::FrozenPlan;
use crate::result_wire::{WireTaskResult, MAX_WIRE_BYTES};
use crate::source_execute::VerifiedEvaluationSource;
use crate::task::{JobOutcome, JobStatus, TaskInputs};
use crate::wire::Digested;
use anyhow::{bail, ensure, Context, Result};
use dispatch::{Derivation, EvaluationContext, Expansion};
pub use dispatch::{DerivedDispatch, PortableFacts, RunnerRequirement, DISPATCH_VERSION};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const DRIVER_VERSION: u32 = 7;
mod shards;
pub use shards::{FrozenShardUnsupported, ShardJoin};
mod atomic;
pub use atomic::{AtomicGroupUnsupported, GroupPlacement, TaskExecutionMode};
const MAX_TASKS: usize = 4096;
const MAX_TEXT: usize = 16384;
// The independently bounded Wire has a bounded ownership/replay envelope.
const MAX_TRANSITION_BYTES: usize = MAX_WIRE_BYTES + 16 * MAX_TEXT + 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverConfig {
    pub schema_version: u32,
    /// Concurrent claimed task lifetimes, independent of host CPU weights.
    pub max_parallel: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    Advance,
    /// Durably request cancellation of this entire driver/run. Unclaimed
    /// work becomes a core cancellation; owned work retains its permit until a
    /// real settlement or an exact never-dispatched host abort.
    Cancel,
    Claim {
        task: String,
        token: String,
    },
    /// Commit/apply before sending execution work or starting task execution.
    /// This is a conservative may-have-started marker, not a delivery receipt.
    /// Repeating the same owner's mark is idempotent, never permission to resend.
    MarkDispatched {
        task: String,
        token: String,
    },
    /// Trusted host assertion that preparation and queued senders are drained,
    /// and this exact claim never dispatched execution. Arbitrary executor errors
    /// after entry/partial execution cannot use this input. After cancellation,
    /// this releases never-dispatched ownership as UnstartedCancelled.
    AbortBeforeExecution {
        task: String,
        token: String,
        reason: HostFailureReason,
    },
    /// A real worker result. Host must fence/verify attempt ownership first.
    /// Accepted even without a dispatch marker: the real terminal is stronger.
    /// A fully validated result that exceeds bounded State storage settles its
    /// owner with genuine digest/status metadata and halts run progression with
    /// a typed infrastructure failure. The full Wire remains in the replay record.
    Settle {
        task: String,
        token: String,
        result_digest: String,
        result: Box<WireTaskResult>,
    },
    Ack {
        action: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActionKind {
    Ready {
        task: String,
        inputs: TaskInputs,
    },
    DerivedReady {
        dispatch: Box<DerivedDispatch>,
        inputs: TaskInputs,
    },
    Cancel {
        task: String,
        token: String,
    },
    Finished,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub id: String,
    pub kind: ActionKind,
}

/// Replay record. Fields are private; decoding is always revalidated by apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transition {
    schema_version: u32,
    plan_digest: String,
    config: DriverConfig,
    evaluation_digest: Option<String>,
    revision: u64,
    input: Input,
    state_digest: String,
}
impl Transition {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn from_json(text: &str) -> Result<Self> {
        ensure!(
            text.len() <= MAX_TRANSITION_BYTES,
            "transition exceeds byte bound"
        );
        Ok(serde_json::from_str(text)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreDecision {
    UnstartedSkipped,
    UnstartedCancelled,
    EarlyFailure,
    MatrixJoined,
    HostFailure,
    InfrastructureBlocked,
}

/// Run infrastructure failure, distinct from the genuine worker's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultStorageFailure {
    DriverCapacity,
}

/// Genuine statuses in immutable task job order; outputs are unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerStatus {
    pub status: JobStatus,
    pub ancestor_failed: bool,
}

#[derive(Debug)]
pub struct UnavailableWorkerReceipt<'a> {
    pub result_digest: &'a str,
    pub statuses: &'a [WorkerStatus],
    pub reason: ResultStorageFailure,
}

#[derive(Debug)]
pub enum ShardJoinAvailability {
    Available(Box<ShardJoin>),
    Pending,
    Unavailable(ResultStorageFailure),
}

/// The proposed state cannot durably retain its real receipts and obligations.
/// A rejected proposal leaves the committed owner and permit unchanged.
#[derive(Debug)]
pub struct DriverStorageError {
    pub reason: String,
}
impl std::fmt::Display for DriverStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "driver storage capacity: {}", self.reason)
    }
}
impl std::error::Error for DriverStorageError {}
fn storage_error(error: anyhow::Error) -> anyhow::Error {
    DriverStorageError {
        reason: format!("{error:#}"),
    }
    .into()
}

/// Bounded host preparation categories; detailed errors remain in host storage.
/// None of these categories attests that an already-dispatched worker stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostFailureReason {
    WorkspaceCopy,
    Lock,
    Setup,
    Provenance,
    ExecutorPreparation,
}
impl HostFailureReason {
    fn diagnostic(self) -> &'static str {
        match self {
            Self::WorkspaceCopy => "host workspace preparation failed",
            Self::Lock => "host lock preparation failed",
            Self::Setup => "host setup preparation failed",
            Self::Provenance => "host provenance preparation failed",
            Self::ExecutorPreparation => "host executor preparation failed",
        }
    }
}

/// A never-started decision is deliberately distinct from a worker receipt.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Terminal {
    WorkerStorageUnavailable {
        token: String,
        result_digest: String,
        statuses: Vec<WorkerStatus>,
    },
    Worker {
        token: String,
        result_digest: String,
        outcomes: Vec<JobOutcome>,
        shard: Option<crate::shard::ShardOutcome>,
    },
    HostAbort {
        token: String,
        reason: HostFailureReason,
        decision: CoreDecision,
        outcomes: Vec<JobOutcome>,
    },
    Core {
        decision: CoreDecision,
        diagnostic: Option<String>,
        outcomes: Vec<JobOutcome>,
    },
}
impl Terminal {
    fn outcomes(&self) -> &[JobOutcome] {
        match self {
            Self::WorkerStorageUnavailable { .. } => &[],
            Self::Worker { outcomes, .. }
            | Self::Core { outcomes, .. }
            | Self::HostAbort { outcomes, .. } => outcomes,
        }
    }
}
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Phase {
    Pending,
    Expanded,
    Ready {
        inputs: TaskInputs,
    },
    Claimed {
        token: String,
        inputs: TaskInputs,
        cancel_requested: bool,
        dispatched: bool,
    },
    Terminal(Terminal),
}
impl Phase {
    fn terminal_outcomes(&self) -> Option<&[JobOutcome]> {
        match self {
            Self::Terminal(terminal) => Some(terminal.outcomes()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct State {
    revision: u64,
    cancelled: bool,
    infrastructure_failure: Option<ResultStorageFailure>,
    phases: BTreeMap<String, Phase>,
    expansions: BTreeMap<String, Expansion>,
    shard_joins: BTreeMap<String, shards::JoinSummary>,
    // Tuple keys are not JSON object keys; nested maps preserve workflow scope.
    outcomes: BTreeMap<String, BTreeMap<String, JobOutcome>>,
    outbox: Vec<Action>,
    finished: bool,
}

#[derive(Debug, Clone)]
pub struct Driver {
    plan: FrozenPlan,
    config: DriverConfig,
    evaluation: Option<EvaluationContext>,
    state_byte_limit: usize,
    state: State,
}
impl Driver {
    pub fn new(plan: &FrozenPlan, config: DriverConfig) -> Result<Self> {
        Self::construct(plan, config, None)
    }
    pub fn with_evaluation(
        plan: &FrozenPlan,
        config: DriverConfig,
        source: &VerifiedEvaluationSource,
        facts: PortableFacts,
    ) -> Result<Self> {
        let evaluation = EvaluationContext::new(plan, source, facts)?;
        Self::construct(plan, config, Some(evaluation))
    }
    fn construct(
        plan: &FrozenPlan,
        config: DriverConfig,
        evaluation: Option<EvaluationContext>,
    ) -> Result<Self> {
        plan.verify()?;
        ensure!(
            serde_json::to_vec(plan)?.len() <= MAX_WIRE_BYTES,
            "driver plan exceeds byte bound"
        );
        ensure!(
            config.schema_version == DRIVER_VERSION,
            "unsupported driver version"
        );
        ensure!(
            (1..=MAX_TASKS).contains(&config.max_parallel),
            "invalid admission limit"
        );
        ensure!(plan.tasks.len() <= MAX_TASKS, "too many driver tasks");
        shards::validate(plan)?;
        let mut ids = BTreeSet::new();
        let mut producers = BTreeMap::new();
        for t in &plan.tasks {
            ensure!(
                !t.dynamic_matrix || evaluation.is_some(),
                "driver needs evaluation context for dynamic tasks"
            );
            ensure!(
                !t.jobs.is_empty() && ids.insert(t.id.clone()),
                "empty/duplicate task"
            );
            atomic::validate_task(plan, t)?;
            if t.jobs.len() > 1
                && t.jobs.iter().any(|j| {
                    plan.workflows
                        .iter()
                        .find(|w| w.file == t.workflow)
                        .and_then(|w| w.job(j))
                        .is_some_and(|j| j.matrix.is_some())
                })
            {
                ensure!(
                    evaluation.is_some(),
                    "atomic matrix group needs verified evaluation source"
                );
            }
            if t.dynamic_matrix && t.jobs.len() == 1 {
                ensure!(
                    t.jobs.len() == 1
                        && t.nodes.len() == 1
                        && t.variant.is_none()
                        && t.grouped_reason.is_none(),
                    "grouped dynamic tasks are unsupported"
                );
                ensure!(
                    plan.graph.nodes.iter().any(|g| g.id == t.nodes[0]
                        && g.workflow == t.workflow
                        && g.job == t.jobs[0]
                        && matches!(g.kind, NodeKind::DynamicMatrix { .. })),
                    "dynamic origin graph mismatch"
                );
            }
            crate::wire::check_version("execution task", t.schema_version)?;
            for dep in &t.depends_on {
                ensure!(
                    ids.contains(dep) && dep != &t.id,
                    "task dependency is not earlier in frozen order"
                );
            }
            if let Some(v) = &t.variant {
                ensure!(t.jobs.len() == 1, "matrix task must hold one job");
                let (g, variants, max_parallel) = static_matrix_group(plan, &v.group)?;
                ensure!(
                    g.workflow == t.workflow && g.job == t.jobs[0],
                    "matrix workflow/job mismatch"
                );
                ensure!(max_parallel != Some(0), "zero matrix admission limit");
                ensure!(
                    variants
                        .get(v.index)
                        .is_some_and(|id| t.nodes == [id.clone()]),
                    "matrix index/node mismatch"
                );
                ensure!(
                    producers
                        .insert(
                            (t.workflow.clone(), t.jobs[0].clone(), Some(v.index)),
                            vec![t.id.clone()]
                        )
                        .is_none(),
                    "duplicate matrix member"
                );
            } else {
                for j in &t.jobs {
                    let owners = producers
                        .entry((t.workflow.clone(), j.clone(), None))
                        .or_insert_with(Vec::new);
                    if !owners.is_empty() {
                        let previous = plan.task(&owners[0]).context("missing producer")?;
                        ensure!(
                            t.shard
                                .as_ref()
                                .zip(previous.shard.as_ref())
                                .is_some_and(|(a, b)| a.group == b.group),
                            "duplicate job producer"
                        );
                    }
                    owners.push(t.id.clone());
                }
            }
            let wf = plan
                .workflows
                .iter()
                .find(|w| w.file == t.workflow)
                .context("missing task workflow")?;
            for j in &t.jobs {
                ensure!(wf.job(j).is_some(), "missing task job");
            }
        }
        for g in &plan.graph.nodes {
            if matches!(g.kind, NodeKind::MatrixGroup { .. }) {
                let (_, variants, _) = static_matrix_group(plan, &g.id)?;
                match producers.get(&(g.workflow.clone(), g.job.clone(), None)) {
                    Some(owner) => {
                        ensure!(owner.len() == 1, "mixed shard/matrix producers");
                        let task = plan.task(&owner[0]).context("missing atomic producer")?;
                        ensure!(task.jobs.len() > 1, "mixed matrix/job producers");
                        ensure!(
                            !(0..variants.len()).any(|i| producers.contains_key(&(
                                g.workflow.clone(),
                                g.job.clone(),
                                Some(i)
                            ))),
                            "mixed atomic/matrix producers"
                        );
                    }
                    None => {
                        for i in 0..variants.len() {
                            ensure!(
                                producers.contains_key(&(
                                    g.workflow.clone(),
                                    g.job.clone(),
                                    Some(i)
                                )),
                                "incomplete matrix task membership"
                            );
                        }
                    }
                }
            }
        }
        atomic::validate_readiness(plan, &producers)?;
        let phases = plan
            .tasks
            .iter()
            .map(|t| (t.id.clone(), Phase::Pending))
            .collect();
        let driver = Self {
            plan: plan.clone(),
            config,
            evaluation,
            state_byte_limit: MAX_WIRE_BYTES,
            state: State {
                revision: 0,
                cancelled: false,
                infrastructure_failure: None,
                phases,
                expansions: BTreeMap::new(),
                shard_joins: BTreeMap::new(),
                outcomes: BTreeMap::new(),
                outbox: vec![],
                finished: false,
            },
        };
        driver.bound(&driver.state)?;
        Ok(driver)
    }
    /// A frozen shard join exists only after every member is terminal.
    /// Coverage is computed from genuine worker receipts; core skips stay absent.
    /// None also means unavailable after storage halt; hosts must inspect
    /// shard_join_availability and infrastructure_failure before reporting success.
    pub fn shard_join(&self, group: &str) -> Option<ShardJoin> {
        if self.state.infrastructure_failure.is_some() {
            return None;
        }
        let summary = self.state.shard_joins.get(group)?;
        let group = self.plan.shard_groups.iter().find(|g| g.id == group)?;
        Some(shards::view(group, &self.state.phases, summary))
    }
    /// Unlike shard_join, distinguishes storage loss from unfinished coverage.
    pub fn shard_join_availability(&self, group: &str) -> ShardJoinAvailability {
        if !self
            .plan
            .shard_groups
            .iter()
            .any(|g| g.id == group && g.split)
        {
            return ShardJoinAvailability::Pending;
        }
        if let Some(reason) = self.infrastructure_failure() {
            return ShardJoinAvailability::Unavailable(reason);
        }
        match self.shard_join(group) {
            Some(join) => ShardJoinAvailability::Available(Box::new(join)),
            None => ShardJoinAvailability::Pending,
        }
    }
    /// Finished means drained orchestration, not success, when this is present.
    pub fn infrastructure_failure(&self) -> Option<ResultStorageFailure> {
        self.state.infrastructure_failure
    }
    /// Full genuine Wire is retained in the committed Settle replay record.
    /// Missing outputs must never be read as an empty successful result.
    pub fn unavailable_worker_receipt(&self, task: &str) -> Option<UnavailableWorkerReceipt<'_>> {
        match self.state.phases.get(task)? {
            Phase::Terminal(Terminal::WorkerStorageUnavailable {
                result_digest,
                statuses,
                ..
            }) => Some(UnavailableWorkerReceipt {
                result_digest,
                statuses,
                reason: ResultStorageFailure::DriverCapacity,
            }),
            _ => None,
        }
    }
    /// Whether a cancellation request was committed before run completion.
    pub fn cancelled(&self) -> bool {
        self.state.cancelled
    }
    pub fn core_decision(&self, task: &str) -> Option<(CoreDecision, Option<&str>)> {
        match self.state.phases.get(task)? {
            Phase::Terminal(Terminal::Core {
                decision,
                diagnostic,
                ..
            }) => Some((*decision, diagnostic.as_deref())),
            Phase::Terminal(Terminal::HostAbort {
                decision, reason, ..
            }) => Some((
                *decision,
                Some(if *decision == CoreDecision::UnstartedCancelled {
                    "run cancelled before dispatch"
                } else {
                    reason.diagnostic()
                }),
            )),
            _ => None,
        }
    }
    /// Original bounded host abort reason, including aborts after cancellation.
    /// It is not a worker receipt or authenticated stop proof.
    pub fn host_failure_reason(&self, task: &str) -> Option<HostFailureReason> {
        match self.state.phases.get(task)? {
            Phase::Terminal(Terminal::HostAbort { reason, .. }) => Some(*reason),
            _ => None,
        }
    }
    /// Deterministic execution contract of the original immutable task. A group
    /// owns one permit/workspace; internal matrices never become derived claims.
    pub fn execution_mode(&self, task: &str) -> Result<TaskExecutionMode> {
        let task = self.plan.task(task).context("unknown original task")?;
        Ok(atomic::mode(task))
    }
    /// Placement descriptors from immutable source facts, including static jobs.
    /// Atomic groups return original descriptors with evaluated=None: internal
    /// needs/matrix labels and heterogeneous co-placement remain unknown.
    pub fn requirements(&self, task: &str) -> Result<Vec<RunnerRequirement>> {
        let evaluation = self
            .evaluation
            .as_ref()
            .context("evaluation source unavailable")?;
        if let Some(selection) = self
            .state
            .expansions
            .values()
            .flat_map(|e| &e.selections)
            .find(|s| s.task.id == task)
        {
            return Ok(vec![selection.requirement.clone()]);
        }
        let task = self.plan.task(task).context("unknown task")?;
        if task.jobs.len() > 1 {
            return atomic::requirements(evaluation, task);
        }
        let mut contexts = evaluation.contexts(&self.plan, task)?;
        let inputs = match &self.state.phases[&task.id] {
            Phase::Ready { inputs } | Phase::Claimed { inputs, .. } => inputs,
            _ => anyhow::bail!("task inputs unavailable before readiness"),
        };
        contexts.insert("needs".into(), serde_json::Value::Object(inputs.context()));
        if let Some(v) = &task.variant {
            contexts.insert("matrix".into(), serde_json::Value::Object(v.values.clone()));
            let (_, variants, _) = static_matrix_group(&self.plan, &v.group)?;
            contexts.insert(
                "strategy".into(),
                serde_json::json!({"job-index":v.index,"job-total":variants.len()}),
            );
        }
        task.jobs
            .iter()
            .map(|job| evaluation.requirement(task, job, &contexts))
            .collect()
    }
    pub fn revision(&self) -> u64 {
        self.state.revision
    }
    pub fn pending_actions(&self) -> &[Action] {
        &self.state.outbox
    }
    pub fn outcome(&self, workflow: &str, job: &str) -> Option<&JobOutcome> {
        self.state.outcomes.get(workflow)?.get(job)
    }
    pub fn admitted(&self) -> usize {
        self.state
            .phases
            .values()
            .filter(|p| matches!(p, Phase::Claimed { .. }))
            .count()
    }
    pub fn propose(&self, input: Input) -> Result<Transition> {
        let next = self.compute(&input)?;
        Ok(Transition {
            schema_version: DRIVER_VERSION,
            plan_digest: self.plan.digest.clone(),
            config: self.config.clone(),
            evaluation_digest: self.evaluation.as_ref().map(digest).transpose()?,
            revision: next.revision,
            input,
            state_digest: digest(&next)?,
        })
    }
    /// Only apply after the host commits this record. Recomputing rejects
    /// altered records and stale/out-of-order replay without partial mutation.
    pub fn apply(&mut self, committed: &Transition) -> Result<()> {
        ensure!(
            committed.schema_version == DRIVER_VERSION
                && committed.plan_digest == self.plan.digest
                && committed.config == self.config
                && committed.evaluation_digest
                    == self.evaluation.as_ref().map(digest).transpose()?,
            "transition binding mismatch"
        );
        ensure!(
            self.state.revision.checked_add(1) == Some(committed.revision),
            "stale/out-of-order transition"
        );
        let next = self.compute(&committed.input)?;
        ensure!(
            digest(&next)? == committed.state_digest,
            "transition state digest mismatch"
        );
        self.state = next;
        Ok(())
    }
    fn compute(&self, input: &Input) -> Result<State> {
        input_bound(input, MAX_WIRE_BYTES)?;
        let mut s = self.state.clone();
        s.revision = s
            .revision
            .checked_add(1)
            .context("driver revision exhausted")?;
        match input {
            Input::Advance => {}
            Input::Cancel => {
                // A late request cannot rewrite an already completed run.
                if !s.finished && !s.cancelled {
                    s.cancelled = true;
                    self.cancel_unclaimed(&mut s)?;
                }
            }
            Input::Ack { action } => {
                ensure!(action.len() <= MAX_TEXT, "action id too long");
                // Unknown ACKs are harmless and idempotent. A Ready ACK does
                // not claim work; it remains visible until claim/termination.
                s.outbox.retain(|a| {
                    &a.id != action
                        || matches!(
                            a.kind,
                            ActionKind::Ready { .. } | ActionKind::DerivedReady { .. }
                        )
                });
            }
            Input::Claim { task, token } => {
                ensure!(!s.cancelled, "run is cancelled");
                ensure!(s.infrastructure_failure.is_none(), "run storage halted");
                ensure!(
                    !token.is_empty() && token.len() <= MAX_TEXT,
                    "invalid ownership token"
                );
                let t = self.task(&s, task).context("unknown task")?;
                let Phase::Ready { inputs } = s.phases.get(task).context("unknown task")? else {
                    bail!("task is not ready")
                };
                let inputs = inputs.clone();
                ensure!(
                    s.phases
                        .values()
                        .filter(|p| matches!(p, Phase::Claimed { .. }))
                        .count()
                        < self.config.max_parallel,
                    "global admission exhausted"
                );
                if let Some(v) = &t.variant {
                    let g = self.group(&s, &v.group).context("missing group")?;
                    if let NodeKind::MatrixGroup {
                        max_parallel: Some(limit),
                        ..
                    } = g.kind
                    {
                        let active = self
                            .tasks(&s)
                            .iter()
                            .filter(|m| {
                                m.variant.as_ref().is_some_and(|m| m.group == v.group)
                                    && matches!(s.phases[&m.id], Phase::Claimed { .. })
                            })
                            .count();
                        ensure!(active < limit, "matrix admission exhausted");
                    }
                }
                s.phases.insert(
                    task.clone(),
                    Phase::Claimed {
                        token: token.clone(),
                        inputs,
                        cancel_requested: false,
                        dispatched: false,
                    },
                );
                s.outbox
                    .retain(|a| ready_task(&a.kind) != Some(task.as_str()));
                self.ensure_cancel_capacity(&s)
                    .context("claim exceeds durable cancellation capacity")?;
            }
            Input::MarkDispatched { task, token } => {
                let Phase::Claimed {
                    token: owner,
                    dispatched,
                    cancel_requested,
                    ..
                } = s.phases.get_mut(task).context("unknown task")?
                else {
                    bail!("dispatch has no admitted owner")
                };
                ensure!(owner == token, "dispatch owner mismatch");
                if !*dispatched {
                    ensure!(!s.cancelled && !*cancel_requested, "dispatch is cancelled");
                    *dispatched = true;
                }
            }
            Input::AbortBeforeExecution {
                task,
                token,
                reason,
            } => {
                let t = self.task(&s, task).context("unknown task")?;
                if let Phase::Terminal(Terminal::HostAbort {
                    token: old,
                    reason: old_reason,
                    ..
                }) = &s.phases[task]
                {
                    ensure!(
                        old == token && old_reason == reason,
                        "conflicting duplicate host abort"
                    );
                    return self.checked(s);
                }
                let Phase::Claimed {
                    token: owner,
                    inputs,
                    dispatched,
                    cancel_requested,
                } = &s.phases[task]
                else {
                    bail!("host abort has no admitted owner")
                };
                ensure!(owner == token, "host abort owner mismatch");
                ensure!(
                    !dispatched,
                    "host abort cannot release dispatched execution"
                );
                let cancelled = s.cancelled || *cancel_requested;
                let decision = if cancelled {
                    CoreDecision::UnstartedCancelled
                } else {
                    CoreDecision::HostFailure
                };
                let wf = self
                    .plan
                    .workflows
                    .iter()
                    .find(|w| w.file == t.workflow)
                    .context("missing workflow")?;
                let mut outcomes: Vec<JobOutcome> = Vec::new();
                for job in &t.jobs {
                    let ancestor_failed =
                        wf.job(job)
                            .context("missing job")?
                            .needs
                            .iter()
                            .any(|need| {
                                outcomes
                                    .iter()
                                    .find(|o| &o.job == need)
                                    .or_else(|| inputs.needs.get(need))
                                    .is_some_and(JobOutcome::failed_or_ancestor_failed)
                            });
                    outcomes.push(JobOutcome {
                        job: job.clone(),
                        variant: t.variant.as_ref().map(|v| v.index),
                        status: if cancelled {
                            JobStatus::Cancelled
                        } else {
                            JobStatus::Failure
                        },
                        outputs: BTreeMap::new(),
                        skipped: cancelled.then(|| "run cancelled before dispatch".into()),
                        ancestor_failed,
                    });
                }
                s.phases.insert(
                    task.clone(),
                    Phase::Terminal(Terminal::HostAbort {
                        token: token.clone(),
                        reason: *reason,
                        decision,
                        outcomes,
                    }),
                );
                s.outbox.retain(
                    |a| !matches!(&a.kind, ActionKind::Cancel { task: id, .. } if id == task),
                );
                if !cancelled {
                    self.fail_fast(&mut s, &t)?;
                }
            }
            Input::Settle {
                task,
                token,
                result_digest,
                result,
            } => {
                let t = self.task(&s, task).context("unknown task")?;
                if let Phase::Terminal(
                    Terminal::Worker {
                        token: old,
                        result_digest: old_digest,
                        ..
                    }
                    | Terminal::WorkerStorageUnavailable {
                        token: old,
                        result_digest: old_digest,
                        ..
                    },
                ) = &s.phases[task]
                {
                    ensure!(
                        old == token && old_digest == result_digest,
                        "conflicting duplicate settlement"
                    );
                    result.verify_digest(result_digest)?;
                    return self.checked(s);
                }
                let Phase::Claimed {
                    token: owner,
                    inputs,
                    ..
                } = &s.phases[task]
                else {
                    bail!("result has no admitted owner")
                };
                ensure!(owner == token, "result owner mismatch");
                result.verify_digest(result_digest)?;
                // Relative wire logs are validated by the shared codec. No
                // files are read and no host paths enter committed state.
                let r = result.as_ref().clone().into_task_result(Path::new("."))?;
                let problems = crate::complete::verify_task_result(&t, &r);
                ensure!(
                    problems.is_empty(),
                    "invalid result: {}",
                    problems.join("; ")
                );
                shards::verify_receipt(&t, r.shard.as_ref())?;
                for j in &t.jobs {
                    let parts: Vec<_> = r.outcomes.iter().filter(|o| &o.job == j).collect();
                    ensure!(
                        parts.len() == 1 && parts[0].variant == t.variant.as_ref().map(|v| v.index),
                        "result job/variant mismatch"
                    );
                    let wf = self
                        .plan
                        .workflows
                        .iter()
                        .find(|w| w.file == t.workflow)
                        .context("missing workflow")?;
                    let expected_ancestor =
                        wf.job(j).context("missing job")?.needs.iter().any(|n| {
                            r.outcomes
                                .iter()
                                .find(|o| &o.job == n)
                                .or_else(|| inputs.needs.get(n))
                                .is_some_and(JobOutcome::failed_or_ancestor_failed)
                        });
                    ensure!(
                        parts[0].ancestor_failed == expected_ancestor,
                        "result ancestor failure mismatch"
                    );
                }
                if t.jobs.len() > 1 {
                    atomic::verify_result(&self.plan, &t, inputs, &r)?;
                } else {
                    ensure!(
                        r.jobs.len() == t.jobs.len(),
                        "missing/duplicate job reports"
                    );
                    for o in &r.outcomes {
                        let reports: Vec<_> = r.jobs.iter().filter(|j| j.job == o.job).collect();
                        ensure!(
                            reports.len() == 1 && JobStatus::of(reports[0]) == o.status,
                            "job report/status mismatch"
                        );
                    }
                }
                for j in &r.jobs {
                    ensure!(
                        j.workflow == t.workflow && t.jobs.contains(&j.job),
                        "result report workflow/job mismatch"
                    );
                }
                return self.settle_validated(s, &t, token, result_digest, r);
            }
        }
        self.progress(&mut s)?;
        self.checked(s)
    }
    /// Entered only after the complete, single genuine Settle validation path.
    fn settle_validated(
        &self,
        mut s: State,
        task: &ExecutionTask,
        token: &str,
        result_digest: &str,
        result: crate::task::TaskResult,
    ) -> Result<State> {
        let unavailable = Terminal::WorkerStorageUnavailable {
            token: token.into(),
            result_digest: result_digest.into(),
            statuses: task
                .jobs
                .iter()
                .map(|job| {
                    let o = result
                        .outcomes
                        .iter()
                        .find(|o| &o.job == job)
                        .expect("validated job");
                    WorkerStatus {
                        status: o.status,
                        ancestor_failed: o.ancestor_failed,
                    }
                })
                .collect(),
        };
        if s.infrastructure_failure.is_none() {
            let mut normal = s.clone();
            let failed = result
                .outcomes
                .iter()
                .any(|o| o.status == JobStatus::Failure)
                || result.shard.as_ref().is_some_and(shards::failed);
            normal.phases.insert(
                task.id.clone(),
                Phase::Terminal(Terminal::Worker {
                    token: token.into(),
                    result_digest: result_digest.into(),
                    outcomes: result.outcomes,
                    shard: result.shard,
                }),
            );
            normal.outbox.retain(|a| {
                !matches!(&a.kind,
                ActionKind::Cancel { task: id, .. } if id == &task.id)
            });
            let candidate = (|| {
                if failed {
                    self.fail_fast(&mut normal, task)?;
                }
                self.ensure_cancel_capacity(&normal)?;
                self.progress(&mut normal)?;
                self.checked(normal)
            })();
            if let Some(normal) = classify_settlement_candidate(candidate)? {
                return Ok(normal);
            }
        }
        s.phases
            .insert(task.id.clone(), Phase::Terminal(unavailable));
        s.outbox.retain(|a| {
            !matches!(&a.kind,
            ActionKind::Cancel { task: id, .. } if id == &task.id)
        });
        s.infrastructure_failure = Some(ResultStorageFailure::DriverCapacity);
        self.halt_progress(&mut s);
        self.checked(s)
    }
    /// Do not publish incomplete outputs or derive work after storage loss.
    fn halt_progress(&self, s: &mut State) {
        for task in self.tasks(s) {
            match &s.phases[&task.id] {
                Phase::Pending | Phase::Ready { .. } | Phase::Expanded => {
                    s.phases.insert(
                        task.id.clone(),
                        Phase::Terminal(Terminal::Core {
                            decision: CoreDecision::InfrastructureBlocked,
                            diagnostic: None,
                            outcomes: vec![],
                        }),
                    );
                }
                Phase::Claimed { .. } => self.request_cancel(s, &task.id),
                Phase::Terminal(_) => {}
            }
        }
        s.outbox.retain(|a| ready_task(&a.kind).is_none());
        self.finish_if_drained(s);
    }
    fn finish_if_drained(&self, s: &mut State) {
        if !s.finished && s.phases.values().all(|p| matches!(p, Phase::Terminal(_))) {
            s.finished = true;
            self.push(s, "finished", ActionKind::Finished);
        }
    }
    fn checked(&self, s: State) -> Result<State> {
        self.bound(&s).map_err(storage_error)?;
        Ok(s)
    }
    fn bound(&self, s: &State) -> Result<()> {
        ensure!(
            s.phases.len() <= MAX_TASKS,
            "driver membership exceeds bound"
        );
        ensure!(s.outbox.len() <= MAX_TASKS * 2 + 1, "outbox exceeds bound");
        crate::expr::serialized_size(s, self.state_byte_limit)
            .context("driver state exceeds byte bound")?;
        self.ensure_cancel_capacity(s)?;
        Ok(())
    }
    fn push(&self, s: &mut State, key: &str, kind: ActionKind) {
        let id = format!("{}:{key}", self.plan.digest);
        if !s.outbox.iter().any(|a| a.id == id) {
            s.outbox.push(Action { id, kind });
        }
    }
    fn fail_fast(&self, s: &mut State, task: &ExecutionTask) -> Result<()> {
        let shard_scope = task.shard.as_ref().map(|v| &v.group);
        let matrix_scope = task.variant.as_ref().map(|v| &v.group);
        let enabled = if let Some(id) = shard_scope {
            self.plan
                .shard_groups
                .iter()
                .any(|g| &g.id == id && g.split && g.fail_fast)
        } else if let Some(id) = matrix_scope {
            matches!(
                self.group(s, id).map(|g| &g.kind),
                Some(NodeKind::MatrixGroup {
                    fail_fast: true,
                    ..
                })
            )
        } else {
            false
        };
        if !enabled {
            return Ok(());
        }
        let ancestor_failed = s.phases[&task.id]
            .terminal_outcomes()
            .into_iter()
            .flatten()
            .any(|o| o.ancestor_failed);
        for member in self.tasks(s) {
            let same = if let Some(id) = shard_scope {
                member.shard.as_ref().is_some_and(|m| &m.group == id)
            } else {
                member
                    .variant
                    .as_ref()
                    .is_some_and(|m| Some(&m.group) == matrix_scope)
            };
            if !same {
                continue;
            }
            match s.phases[&member.id].clone() {
                Phase::Pending | Phase::Ready { .. } => {
                    let outcomes = member
                        .jobs
                        .iter()
                        .map(|j| JobOutcome {
                            job: j.clone(),
                            variant: member.variant.as_ref().map(|v| v.index),
                            status: JobStatus::Skipped,
                            outputs: BTreeMap::new(),
                            skipped: Some("fail-fast".into()),
                            ancestor_failed,
                        })
                        .collect();
                    s.phases.insert(
                        member.id.clone(),
                        Phase::Terminal(Terminal::Core {
                            decision: CoreDecision::UnstartedSkipped,
                            diagnostic: Some("fail-fast".into()),
                            outcomes,
                        }),
                    );
                    s.outbox
                        .retain(|a| ready_task(&a.kind) != Some(member.id.as_str()));
                }
                Phase::Claimed { .. } => self.request_cancel(s, &member.id),
                Phase::Terminal(_) | Phase::Expanded => {}
            }
        }
        Ok(())
    }
    fn unstarted_cancel_reserve(task: &ExecutionTask) -> Result<usize> {
        // Core terminal + logical outcome/map identity; cancelled outcomes have
        // no user outputs and a fixed short diagnostic. Count escaped JSON IDs.
        Ok(1024usize
            .saturating_mul(task.jobs.len().max(1))
            .saturating_add(
                crate::expr::serialized_size(&(&task.jobs, &task.workflow), MAX_WIRE_BYTES)?
                    .saturating_mul(4),
            ))
    }
    fn cancel_reserve(&self, s: &State) -> Result<usize> {
        if s.finished {
            return Ok(0);
        }
        let tasks = self.plan.tasks.iter().chain(
            s.expansions
                .values()
                .flat_map(|e| e.selections.iter().map(|selection| &selection.task)),
        );
        // One bounded summary + join envelope per unfinished frozen group.
        // Fixed run failure marker and Finished envelope. Each unfinished phase
        // separately reserves its own worst halt replacement below.
        let mut reserve = 1024usize;
        for group in self.plan.shard_groups.iter().filter(|group| {
            s.infrastructure_failure.is_none()
                && group.split
                && !s.shard_joins.contains_key(&group.id)
        }) {
            reserve = reserve
                .saturating_add(8192)
                .saturating_add(crate::expr::serialized_size(
                    &(&group.id, &group.workflow, &group.source_jobs),
                    MAX_WIRE_BYTES,
                )?);
        }
        for task in tasks {
            let addition = match &s.phases[&task.id] {
                Phase::Pending | Phase::Ready { .. } => Self::unstarted_cancel_reserve(task)?,
                Phase::Expanded => 512,
                Phase::Claimed {
                    token,
                    cancel_requested,
                    dispatched,
                    ..
                } => {
                    let cancel = if !*cancel_requested {
                        let id = format!("{}:cancel:{}", self.plan.digest, task.id);
                        crate::expr::serialized_size(&(&id, &task.id, token), MAX_WIRE_BYTES)?
                            .saturating_add(512)
                    } else {
                        0
                    };
                    // Keep never-dispatched abort terminal/publication/join capacity
                    // through Cancel and ACK. The abort retains its original token.
                    let abort = if !*dispatched {
                        Self::unstarted_cancel_reserve(task)?
                            .saturating_add(crate::expr::serialized_size(token, MAX_WIRE_BYTES)?)
                            .saturating_add(1024)
                    } else {
                        0
                    };
                    // Keep this reserve after dispatch and through cancellation
                    // ACKs. All jobs are known at claim time; Cancelled/false are
                    // the longest serialized genuine status/ancestor variants.
                    let terminal = Terminal::WorkerStorageUnavailable {
                        token: token.clone(),
                        result_digest: format!("sha256:{}", "f".repeat(64)),
                        statuses: task
                            .jobs
                            .iter()
                            .map(|_| WorkerStatus {
                                status: JobStatus::Cancelled,
                                ancestor_failed: false,
                            })
                            .collect(),
                    };
                    let storage = crate::expr::serialized_size(&terminal, MAX_WIRE_BYTES)?
                        .saturating_add(512);
                    cancel.saturating_add(abort).saturating_add(storage)
                }
                Phase::Terminal(terminal)
                    if s.infrastructure_failure.is_none()
                        && task.variant.as_ref().is_some_and(|_| {
                            !s.outcomes
                                .get(&task.workflow)
                                .is_some_and(|m| m.contains_key(&task.jobs[0]))
                        }) =>
                {
                    // Cancellation may complete an otherwise-unfinished matrix:
                    // reserve aggregate/Core/map copies of already-real outputs.
                    crate::expr::serialized_size(&terminal.outcomes(), MAX_WIRE_BYTES)?
                        .saturating_mul(2)
                        .saturating_add(1024)
                }
                Phase::Terminal(terminal)
                    if s.infrastructure_failure.is_none()
                        && task.variant.is_none()
                        && !s
                            .outcomes
                            .get(&task.workflow)
                            .is_some_and(|m| task.jobs.iter().all(|job| m.contains_key(job))) =>
                {
                    crate::expr::serialized_size(&terminal.outcomes(), MAX_WIRE_BYTES)?
                        .saturating_add(1024)
                }
                _ => 0,
            };
            reserve = reserve.saturating_add(addition);
        }
        Ok(reserve)
    }
    fn ensure_cancel_capacity(&self, s: &State) -> Result<()> {
        (|| {
            let bytes = crate::expr::serialized_size(s, self.state_byte_limit)?;
            ensure!(
                bytes
                    .checked_add(self.cancel_reserve(s)?)
                    .is_some_and(|n| n <= self.state_byte_limit),
                "state lacks durable cancellation capacity"
            );
            Ok(())
        })()
        .map_err(storage_error)
    }
    fn request_cancel(&self, s: &mut State, task: &str) {
        let Some(Phase::Claimed {
            token,
            cancel_requested,
            ..
        }) = s.phases.get_mut(task)
        else {
            return;
        };
        // Persist the request bit independently of its outbox ACK; a repeated
        // run cancel or another fail-fast must not recreate an ACKed action.
        if *cancel_requested {
            return;
        }
        *cancel_requested = true;
        let token = token.clone();
        self.push(
            s,
            &format!("cancel:{task}"),
            ActionKind::Cancel {
                task: task.into(),
                token,
            },
        );
    }
    fn cancel_unclaimed(&self, s: &mut State) -> Result<()> {
        for task in self.tasks(s) {
            match &s.phases[&task.id] {
                Phase::Pending | Phase::Ready { .. } => {
                    let wf = self
                        .plan
                        .workflows
                        .iter()
                        .find(|w| w.file == task.workflow)
                        .context("missing cancellation workflow")?;
                    let outcomes = task
                        .jobs
                        .iter()
                        .map(|job| {
                            let ancestor_failed = wf.job(job).is_some_and(|j| {
                                j.needs.iter().any(|need| {
                                    s.outcomes
                                        .get(&task.workflow)
                                        .and_then(|m| m.get(need))
                                        .is_some_and(JobOutcome::failed_or_ancestor_failed)
                                })
                            });
                            JobOutcome {
                                job: job.clone(),
                                variant: task.variant.as_ref().map(|v| v.index),
                                status: JobStatus::Cancelled,
                                outputs: BTreeMap::new(),
                                skipped: Some("run cancelled".into()),
                                ancestor_failed,
                            }
                        })
                        .collect();
                    s.phases.insert(
                        task.id.clone(),
                        Phase::Terminal(Terminal::Core {
                            decision: CoreDecision::UnstartedCancelled,
                            diagnostic: Some("run cancelled".into()),
                            outcomes,
                        }),
                    );
                    s.outbox
                        .retain(|action| ready_task(&action.kind) != Some(task.id.as_str()));
                }
                Phase::Claimed { .. } => self.request_cancel(s, &task.id),
                Phase::Expanded | Phase::Terminal(_) => {}
            }
        }
        Ok(())
    }
    fn task(&self, s: &State, id: &str) -> Option<ExecutionTask> {
        self.plan.task(id).cloned().or_else(|| {
            s.expansions
                .values()
                .flat_map(|e| &e.selections)
                .find(|selection| selection.task.id == id)
                .map(|selection| selection.task.clone())
        })
    }
    fn tasks(&self, s: &State) -> Vec<ExecutionTask> {
        self.plan
            .tasks
            .iter()
            .cloned()
            .chain(
                s.expansions
                    .values()
                    .flat_map(|e| e.selections.iter().map(|s| s.task.clone())),
            )
            .collect()
    }
    fn group<'a>(&'a self, s: &'a State, id: &str) -> Option<&'a JobNode> {
        s.expansions
            .values()
            .map(|e| &e.group)
            .chain(self.plan.graph.nodes.iter())
            .find(|g| g.id == id)
    }
    fn expansion_state_bytes(
        &self,
        origin: &str,
        expansion: &Expansion,
        limit: usize,
    ) -> Result<usize> {
        // Count actual JSON data (including escapes), without cloning inputs or
        // selections. Fixed allowances exceed the enum/map wrapper punctuation.
        let mut bytes = crate::expr::serialized_size(&(origin, expansion), limit)?
            .checked_add(512)
            .context("expansion state byte budget exceeded")?;
        for selection in &expansion.selections {
            bytes = bytes.saturating_add(Self::unstarted_cancel_reserve(&selection.task)?);
        }
        for dispatch in &expansion.dispatches {
            let phase =
                crate::expr::serialized_size(&(dispatch.task_id(), &expansion.inputs), limit)?;
            let id = format!("{}:ready:{}", self.plan.digest, dispatch.task_id());
            let action = crate::expr::serialized_size(&(&id, dispatch, &expansion.inputs), limit)?;
            bytes = bytes
                .checked_add(phase)
                .and_then(|n| n.checked_add(action))
                .and_then(|n| n.checked_add(512))
                .context("expansion state byte budget exceeded")?;
            ensure!(bytes <= limit, "expansion state byte budget exceeded");
        }
        ensure!(bytes <= limit, "expansion state byte budget exceeded");
        Ok(bytes)
    }
    fn fallback_reserve(task: &ExecutionTask) -> Result<usize> {
        Ok(8192usize.saturating_add(
            crate::expr::serialized_size(&(&task.jobs, &task.workflow), MAX_WIRE_BYTES)?
                .saturating_mul(4),
        ))
    }
    fn pending_fallback_reserve(&self, s: &State) -> Result<usize> {
        self.plan
            .tasks
            .iter()
            .filter(|task| task.dynamic_matrix && matches!(s.phases[&task.id], Phase::Pending))
            .try_fold(0usize, |bytes, task| {
                Ok(bytes.saturating_add(Self::fallback_reserve(task)?))
            })
    }
    fn check_progress_storage(&self, s: &State) -> Result<()> {
        crate::expr::serialized_size(s, self.state_byte_limit)
            .context("progress exceeds state byte bound")
            .map_err(storage_error)?;
        Ok(())
    }
    fn progress(&self, s: &mut State) -> Result<()> {
        if s.infrastructure_failure.is_some() {
            self.halt_progress(s);
            return Ok(());
        }
        loop {
            let mut changed = false;
            for t in &self.plan.tasks {
                if t.variant.is_some() || t.shard.is_some() {
                    continue;
                }
                if let Phase::Terminal(terminal) = &s.phases[&t.id] {
                    for o in terminal.outcomes() {
                        s.outcomes
                            .entry(t.workflow.clone())
                            .or_default()
                            .insert(o.job.clone(), o.clone());
                    }
                }
            }
            // Publication/join batches copy at most the bounded resident
            // receipts. Check before the next stage can amplify those values.
            self.check_progress_storage(s)?;
            for group in self.plan.shard_groups.iter().filter(|g| g.split) {
                if s.shard_joins.contains_key(&group.id) {
                    continue;
                }
                if let Some(join) = shards::join(group, &s.phases)? {
                    s.outcomes
                        .entry(group.workflow.clone())
                        .or_default()
                        .insert(group.source_jobs[0].clone(), join.outcome);
                    s.shard_joins.insert(group.id.clone(), join.summary);
                    changed = true;
                }
            }
            self.check_progress_storage(s)?;
            let groups: Vec<_> = self
                .plan
                .graph
                .nodes
                .iter()
                .filter(|g| matches!(g.kind, NodeKind::MatrixGroup { .. }))
                .cloned()
                .chain(s.expansions.values().map(|e| e.group.clone()))
                .collect();
            let tasks = self.tasks(s);
            for g in groups {
                let members: Vec<_> = tasks
                    .iter()
                    .filter(|t| t.variant.as_ref().is_some_and(|v| v.group == g.id))
                    .collect();
                let parts = members
                    .iter()
                    .map(|t| s.phases[&t.id].terminal_outcomes())
                    .collect::<Option<Vec<_>>>();
                if let Some(parts) = parts.filter(|_| !members.is_empty()) {
                    let parts: Vec<_> = parts.into_iter().flatten().cloned().collect();
                    let joined = crate::task::aggregate_matrix(&g, &parts)?;
                    s.outcomes
                        .entry(g.workflow.clone())
                        .or_default()
                        .insert(g.job.clone(), joined.clone());
                    if let Some(origin) = s
                        .expansions
                        .iter()
                        .find(|(_, e)| e.group.id == g.id)
                        .map(|(id, _)| id.clone())
                    {
                        if matches!(s.phases[&origin], Phase::Expanded) {
                            s.phases.insert(
                                origin,
                                Phase::Terminal(Terminal::Core {
                                    decision: CoreDecision::MatrixJoined,
                                    diagnostic: None,
                                    outcomes: vec![joined],
                                }),
                            );
                            changed = true;
                        }
                    }
                }
            }
            self.check_progress_storage(s)?;
            // Keep room for every pending origin's core terminal and logical
            // outcome, including siblings not yet reached in frozen order.
            let mut fallback_reserve = self.pending_fallback_reserve(s)?;
            for t in &self.plan.tasks {
                if !matches!(s.phases[&t.id], Phase::Pending) {
                    continue;
                }
                let wf = self
                    .plan
                    .workflows
                    .iter()
                    .find(|w| w.file == t.workflow)
                    .context("missing workflow")?;
                let needs: BTreeSet<_> = t
                    .jobs
                    .iter()
                    .flat_map(|j| wf.job(j).into_iter().flat_map(|j| &j.needs))
                    .filter(|n| !t.jobs.contains(n))
                    .collect();
                let finished = s.outcomes.get(&t.workflow);
                if !t
                    .depends_on
                    .iter()
                    .all(|d| matches!(s.phases[d], Phase::Terminal(_)))
                    || !needs
                        .iter()
                        .all(|n| finished.is_some_and(|m| m.contains_key(*n)))
                {
                    continue;
                }
                let inputs = TaskInputs::new(
                    needs
                        .iter()
                        .filter_map(|n| finished.and_then(|m| m.get(*n))),
                    finished.into_iter().flat_map(|m| m.values()),
                    s.cancelled,
                );
                changed = true;
                if t.dynamic_matrix && t.jobs.len() == 1 {
                    let evaluation = self
                        .evaluation
                        .as_ref()
                        .context("missing evaluation context")?;
                    // User expression failures are core outcomes; never discard the
                    // already-validated producer settlement and permit release.
                    // Expansion cannot consume any pending origin's fallback.
                    let baseline_bytes = crate::expr::serialized_size(s, MAX_WIRE_BYTES)?;
                    let remaining = self
                        .state_byte_limit
                        .saturating_sub(baseline_bytes)
                        .saturating_sub(fallback_reserve)
                        .saturating_sub(self.cancel_reserve(s)?);
                    let mut derivation = dispatch::derive(
                        &self.plan,
                        evaluation,
                        t,
                        &inputs,
                        MAX_TASKS.saturating_sub(s.phases.len()),
                        remaining,
                    )
                    .unwrap_or_else(|e| Derivation::Failed(format!("expansion: {e:#}")));
                    if let Derivation::Expanded(expansion) = &derivation {
                        if self
                            .expansion_state_bytes(&t.id, expansion, remaining)
                            .is_err()
                        {
                            derivation = Derivation::Failed(
                                "expansion exceeds remaining state byte budget".into(),
                            );
                        }
                    }
                    let terminal = match derivation {
                        Derivation::Expanded(expansion) => {
                            verify_derived_membership_available(s, &expansion)?;
                            for dispatch in &expansion.dispatches {
                                s.phases.insert(
                                    dispatch.task_id().into(),
                                    Phase::Ready {
                                        inputs: inputs.clone(),
                                    },
                                );
                                self.push(
                                    s,
                                    &format!("ready:{}", dispatch.task_id()),
                                    ActionKind::DerivedReady {
                                        dispatch: Box::new(dispatch.clone()),
                                        inputs: inputs.clone(),
                                    },
                                );
                            }
                            s.phases.insert(t.id.clone(), Phase::Expanded);
                            s.expansions.insert(t.id.clone(), *expansion);
                            None
                        }
                        Derivation::Skipped(reason) => {
                            Some((CoreDecision::UnstartedSkipped, JobStatus::Skipped, reason))
                        }
                        Derivation::Failed(reason) => {
                            Some((CoreDecision::EarlyFailure, JobStatus::Failure, reason))
                        }
                    };
                    if let Some((decision, status, reason)) = terminal {
                        let diagnostic: String = reason.chars().take(1024).collect();
                        let ancestor_failed = inputs
                            .needs
                            .values()
                            .any(JobOutcome::failed_or_ancestor_failed);
                        let outcome = JobOutcome {
                            job: t.jobs[0].clone(),
                            variant: None,
                            status,
                            outputs: BTreeMap::new(),
                            skipped: (status == JobStatus::Skipped).then(|| diagnostic.clone()),
                            ancestor_failed,
                        };
                        // Publish the small logical outcome now, so later
                        // origins count its bytes before releasing its reserve.
                        s.outcomes
                            .entry(t.workflow.clone())
                            .or_default()
                            .insert(outcome.job.clone(), outcome.clone());
                        s.phases.insert(
                            t.id.clone(),
                            Phase::Terminal(Terminal::Core {
                                decision,
                                diagnostic: Some(diagnostic),
                                outcomes: vec![outcome],
                            }),
                        );
                    }
                    fallback_reserve = fallback_reserve.saturating_sub(Self::fallback_reserve(t)?);
                } else {
                    s.phases.insert(
                        t.id.clone(),
                        Phase::Ready {
                            inputs: inputs.clone(),
                        },
                    );
                    self.push(
                        s,
                        &format!("ready:{}", t.id),
                        ActionKind::Ready {
                            task: t.id.clone(),
                            inputs,
                        },
                    );
                }
                // Each new Ready copies bounded needs into its phase/outbox.
                // Never materialize the entire fanout before detecting overflow.
                self.check_progress_storage(s)?;
            }
            if !changed {
                break;
            }
        }
        self.finish_if_drained(s);
        Ok(())
    }
}
/// Check the entire proposed membership before adding any Ready phase/outbox.
fn verify_derived_membership_available(state: &State, expansion: &Expansion) -> Result<()> {
    for selection in &expansion.selections {
        ensure!(
            !state.phases.contains_key(&selection.task.id),
            "derived membership collision"
        );
    }
    Ok(())
}

/// Only an explicitly typed capacity refusal permits genuine-receipt fallback.
/// Every other validation/infrastructure error retains its original type/source.
fn classify_settlement_candidate(candidate: Result<State>) -> Result<Option<State>> {
    match candidate {
        Ok(normal) => Ok(Some(normal)),
        Err(error) if error.downcast_ref::<DriverStorageError>().is_some() => Ok(None),
        Err(error) => Err(error),
    }
}

/// Resolve the immutable static matrix contract once for admission and labels.
fn static_matrix_group<'a>(
    plan: &'a FrozenPlan,
    id: &str,
) -> Result<(&'a JobNode, &'a [String], Option<usize>)> {
    let group = plan
        .graph
        .nodes
        .iter()
        .find(|g| g.id == id)
        .context("missing matrix group")?;
    let NodeKind::MatrixGroup {
        variants,
        max_parallel,
        ..
    } = &group.kind
    else {
        bail!("not a static matrix group")
    };
    ensure!(
        !variants.is_empty(),
        "empty matrix unsupported in this slice"
    );
    Ok((group, variants, *max_parallel))
}

fn ready_task(action: &ActionKind) -> Option<&str> {
    match action {
        ActionKind::Ready { task, .. } => Some(task),
        ActionKind::DerivedReady { dispatch, .. } => Some(dispatch.task_id()),
        _ => None,
    }
}
fn input_bound(input: &Input, wire_limit: usize) -> Result<()> {
    // A Settle contains one independently bounded Wire plus task/token/digest.
    // At most 6 bytes per escaped ownership byte, 4KiB Wire task identity,
    // fixed digests, config/revision and JSON punctuation fit this allowance.
    crate::expr::serialized_size(input, wire_limit + MAX_TRANSITION_BYTES - MAX_WIRE_BYTES)
        .context("input exceeds byte bound")?;
    if let Input::Settle { result, .. } = input {
        crate::expr::serialized_size(result, wire_limit)
            .context("worker result exceeds wire byte bound")?;
    } else {
        crate::expr::serialized_size(input, wire_limit).context("input exceeds byte bound")?;
    }
    Ok(())
}
fn digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(crate::frozen::sha256_of(&crate::frozen::canonical_json(
        &serde_json::to_value(value)?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::Outcome;
    use crate::result_wire::WireJobResult;
    use crate::source::{CaptureOptions, ObjectStore};
    use crate::{ExecContext, PinnedPlanOptions};

    fn plan(matrix: bool, limit: usize) -> FrozenPlan {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("input");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        let strategy = if matrix {
            format!("    strategy:\n      fail-fast: true\n      max-parallel: {limit}\n      matrix:\n        n: [0, 1, 2]\n")
        } else {
            String::new()
        };
        let workflow = format!("name: test\non: push\njobs:\n  first:\n    runs-on: ubuntu-latest\n{strategy}    steps:\n      - run: echo first\n  next:\n    needs: first\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo next\n");
        for file in ["a.yml", "b.yml"] {
            std::fs::write(root.join(".github/workflows").join(file), &workflow).unwrap();
        }
        let store = ObjectStore::open(tmp.path().join("objects")).unwrap();
        let ctx = ExecContext::new(BTreeMap::new());
        let manifest = crate::source::capture_directory(
            &ctx,
            &root,
            &CaptureOptions {
                store: Some(store.root().into()),
                ..Default::default()
            },
        )
        .unwrap()
        .manifest;
        crate::build_frozen_plan_from_source(
            &store,
            &manifest,
            &PinnedPlanOptions {
                workflow_dirs_override: None,
                filters: crate::frozen::Filters {
                    all: false,
                    include_disabled: true,
                    workflows_dirs: vec![],
                    workflows: vec![],
                    jobs: vec![],
                },
                disabled: vec![],
                base_branch: None,
                changed_files: None,
            },
        )
        .unwrap()
    }
    fn driver(matrix: bool, limit: usize) -> Driver {
        Driver::new(
            &plan(matrix, limit),
            DriverConfig {
                schema_version: DRIVER_VERSION,
                max_parallel: 8,
            },
        )
        .unwrap()
    }
    fn commit(d: &mut Driver, input: Input) -> Transition {
        let t = d.propose(input).unwrap();
        d.apply(&t).unwrap();
        t
    }
    fn ready(d: &Driver, workflow: &str) -> Vec<String> {
        d.pending_actions()
            .iter()
            .filter_map(|a| match &a.kind {
                ActionKind::Ready { task, .. }
                    if d.plan.task(task).unwrap().workflow == workflow =>
                {
                    Some(task.clone())
                }
                _ => None,
            })
            .collect()
    }
    fn result(d: &Driver, id: &str, status: JobStatus, value: &str) -> WireTaskResult {
        let t = d.plan.task(id).unwrap();
        let outcomes = t
            .jobs
            .iter()
            .map(|j| JobOutcome {
                job: j.clone(),
                variant: t.variant.as_ref().map(|v| v.index),
                status,
                outputs: [("value".into(), value.into())].into(),
                skipped: None,
                ancestor_failed: false,
            })
            .collect();
        WireTaskResult {
            schema_version: crate::wire::wire_version(),
            task: id.into(),
            jobs: t
                .jobs
                .iter()
                .map(|j| WireJobResult {
                    workflow: t.workflow.clone(),
                    job: j.clone(),
                    display: j.clone(),
                    outcome: if status == JobStatus::Failure {
                        Outcome::Fail
                    } else {
                        Outcome::Pass
                    },
                    secs: 0.0,
                    steps: vec![],
                    cancelled: false,
                    time: None,
                    build_cache: None,
                    os_note: None,
                })
                .collect(),
            outcomes,
            receipt_ignored: None,
            artifacts: vec![],
            shard: None,
        }
    }
    fn settle(d: &mut Driver, id: &str, status: JobStatus, value: &str) -> Transition {
        let r = result(d, id, status, value);
        commit(
            d,
            Input::Settle {
                task: id.into(),
                token: id.into(),
                result_digest: r.digest().unwrap(),
                result: Box::new(r),
            },
        )
    }
    #[test]
    fn transactions_replay_outbox_and_workflow_scoped_needs() {
        let mut d = driver(false, 1);
        let original = d.clone();
        let proposed = d.propose(Input::Advance).unwrap();
        assert!(d.pending_actions().is_empty());
        d.apply(&proposed).unwrap();
        let first = ready(&d, ".github/workflows/a.yml")[0].clone();
        let action = d.pending_actions()[0].id.clone();
        commit(&mut d, Input::Ack { action });
        assert!(ready(&d, ".github/workflows/a.yml").contains(&first));
        let claim = commit(
            &mut d,
            Input::Claim {
                task: first.clone(),
                token: first.clone(),
            },
        );
        let done = settle(&mut d, &first, JobStatus::Success, "from-a");
        assert!(d.outcome(".github/workflows/b.yml", "first").is_none());
        let next = ready(&d, ".github/workflows/a.yml");
        assert_eq!(next.len(), 1);
        let inputs = d
            .pending_actions()
            .iter()
            .find_map(|a| match &a.kind {
                ActionKind::Ready { task, inputs } if task == &next[0] => Some(inputs),
                _ => None,
            })
            .unwrap();
        assert_eq!(inputs.needs["first"].outputs["value"], "from-a");
        let mut replay = original;
        // Include the Ready ACK revision in exact event order.
        replay.apply(&proposed).unwrap();
        let ack = replay
            .propose(Input::Ack {
                action: format!("{}:ready:{first}", replay.plan.digest),
            })
            .unwrap();
        replay.apply(&ack).unwrap();
        replay.apply(&claim).unwrap();
        replay.apply(&done).unwrap();
        assert_eq!(digest(&replay.state).unwrap(), digest(&d.state).unwrap());
        assert!(d.apply(&done).is_err());
        let dup = d.propose(done.input.clone()).unwrap();
        d.apply(&dup).unwrap();
        assert_eq!(d.admitted(), 0);
        let mut corrupt = d.propose(Input::Advance).unwrap();
        corrupt.state_digest = "forged".into();
        let revision = d.revision();
        assert!(d.apply(&corrupt).is_err());
        assert_eq!(revision, d.revision());
        let json = serde_json::to_string(&dup).unwrap();
        assert!(Transition::from_json(&json).is_ok());
    }
    #[test]
    fn matrix_join_is_exact_ordered_and_admission_is_global() {
        let mut d = driver(true, 1);
        commit(&mut d, Input::Advance);
        let ids = ready(&d, ".github/workflows/a.yml");
        assert_eq!(ids.len(), 3);
        commit(
            &mut d,
            Input::Claim {
                task: ids[2].clone(),
                token: ids[2].clone(),
            },
        );
        assert!(d
            .propose(Input::Claim {
                task: ids[1].clone(),
                token: ids[1].clone()
            })
            .is_err());
        settle(&mut d, &ids[2], JobStatus::Success, "last-index");
        assert!(d.outcome(".github/workflows/a.yml", "first").is_none());
        for id in [&ids[1], &ids[0]] {
            commit(
                &mut d,
                Input::Claim {
                    task: id.clone(),
                    token: id.clone(),
                },
            );
            settle(&mut d, id, JobStatus::Success, "earlier-index");
        }
        assert_eq!(
            d.outcome(".github/workflows/a.yml", "first")
                .unwrap()
                .outputs["value"],
            "last-index"
        );
        assert_eq!(ready(&d, ".github/workflows/a.yml").len(), 1);
    }
    #[test]
    fn fail_fast_keeps_owned_permit_and_distinguishes_unstarted() {
        let mut d = driver(true, 2);
        commit(&mut d, Input::Advance);
        let ids = ready(&d, ".github/workflows/a.yml");
        for id in &ids[..2] {
            commit(
                &mut d,
                Input::Claim {
                    task: id.clone(),
                    token: id.clone(),
                },
            );
        }
        settle(&mut d, &ids[0], JobStatus::Failure, "failed");
        assert_eq!(d.admitted(), 1);
        assert!(d.outcome(".github/workflows/a.yml", "first").is_none());
        assert!(matches!(
            d.state.phases[&ids[2]],
            Phase::Terminal(Terminal::Core {
                decision: CoreDecision::UnstartedSkipped,
                ..
            })
        ));
        let cancel = d
            .pending_actions()
            .iter()
            .find(|a| matches!(a.kind, ActionKind::Cancel { .. }))
            .unwrap()
            .id
            .clone();
        commit(
            &mut d,
            Input::Ack {
                action: cancel.clone(),
            },
        );
        commit(&mut d, Input::Ack { action: cancel });
        assert_eq!(d.admitted(), 1);
        settle(&mut d, &ids[1], JobStatus::Success, "passed");
        assert_eq!(d.admitted(), 0);
        assert_eq!(
            d.outcome(".github/workflows/a.yml", "first")
                .unwrap()
                .status,
            JobStatus::Failure
        );
    }
    #[test]
    fn global_permit_is_held_until_terminal_and_not_released_twice() {
        let mut d = driver(false, 1);
        d.config.max_parallel = 1;
        commit(&mut d, Input::Advance);
        let a = ready(&d, ".github/workflows/a.yml")[0].clone();
        let b = ready(&d, ".github/workflows/b.yml")[0].clone();
        commit(
            &mut d,
            Input::Claim {
                task: a.clone(),
                token: a.clone(),
            },
        );
        assert!(d
            .propose(Input::Claim {
                task: b.clone(),
                token: b.clone()
            })
            .is_err());
        let done = settle(&mut d, &a, JobStatus::Success, "a");
        commit(
            &mut d,
            Input::Claim {
                task: b.clone(),
                token: b.clone(),
            },
        );
        commit(&mut d, done.input);
        assert_eq!(d.admitted(), 1);
        assert!(d
            .propose(Input::Claim {
                task: b.clone(),
                token: b
            })
            .is_err());
    }
    #[test]
    fn refuses_modified_frozen_plan_and_config_binding() {
        let mut fp = plan(true, 1);
        fp.tasks[0].jobs.push("forged".into());
        assert!(Driver::new(
            &fp,
            DriverConfig {
                schema_version: DRIVER_VERSION,
                max_parallel: 1
            }
        )
        .is_err());
        let mut d = driver(false, 1);
        let mut transition = d.propose(Input::Advance).unwrap();
        transition.config.max_parallel += 1;
        assert!(d.apply(&transition).is_err());
        assert_eq!(d.revision(), 0);
        let record = serde_json::to_string(&d.propose(Input::Advance).unwrap()).unwrap();
        let mut restored = Driver::new(&d.plan, d.config.clone()).unwrap();
        restored
            .apply(&Transition::from_json(&record).unwrap())
            .unwrap();
        assert_eq!(restored.pending_actions().len(), 2);
        assert_eq!(
            restored.pending_actions(),
            d.compute(&Input::Advance).unwrap().outbox
        );
    }
    #[test]
    fn rejects_forged_outcomes_owner_and_revision_overflow() {
        let mut d = driver(true, 2);
        commit(&mut d, Input::Advance);
        assert!(d
            .propose(Input::Claim {
                task: "unknown".into(),
                token: "owner".into()
            })
            .is_err());
        let id = ready(&d, ".github/workflows/a.yml")[0].clone();
        commit(
            &mut d,
            Input::Claim {
                task: id.clone(),
                token: id.clone(),
            },
        );
        for bad in 0..6 {
            let mut r = result(&d, &id, JobStatus::Success, "ok");
            match bad {
                0 => r.outcomes.clear(),
                1 => r.outcomes.push(r.outcomes[0].clone()),
                2 => r.outcomes[0].variant = Some(99),
                3 => r.jobs[0].workflow = ".github/workflows/b.yml".into(),
                4 => r.jobs.clear(),
                _ => r.outcomes[0].job = "foreign-job".into(),
            }
            assert!(d
                .propose(Input::Settle {
                    task: id.clone(),
                    token: id.clone(),
                    result_digest: r.digest().unwrap(),
                    result: Box::new(r)
                })
                .is_err());
        }
        let r = result(&d, &id, JobStatus::Success, "ok");
        assert!(d
            .propose(Input::Settle {
                task: id.clone(),
                token: "stale".into(),
                result_digest: r.digest().unwrap(),
                result: Box::new(r)
            })
            .is_err());
        d.state.revision = u64::MAX;
        assert!(d.propose(Input::Advance).is_err());
        assert!(d
            .propose(Input::Ack {
                action: "x".repeat(MAX_WIRE_BYTES + 1)
            })
            .is_err());
    }
    #[test]
    fn run_cancel_joins_static_matrices_without_cross_workflow_results() {
        let mut d = driver(true, 2);
        commit(&mut d, Input::Advance);
        let a = ready(&d, ".github/workflows/a.yml")[0].clone();
        let b = ready(&d, ".github/workflows/b.yml")[0].clone();
        commit(
            &mut d,
            Input::Claim {
                task: a.clone(),
                token: a.clone(),
            },
        );
        commit(
            &mut d,
            Input::Claim {
                task: b.clone(),
                token: b.clone(),
            },
        );
        commit(&mut d, Input::Cancel);
        assert_eq!(d.admitted(), 2);
        assert_eq!(
            d.pending_actions()
                .iter()
                .filter(|a| matches!(a.kind, ActionKind::Cancel { .. }))
                .count(),
            2
        );
        settle(&mut d, &a, JobStatus::Success, "A");
        assert_eq!(d.admitted(), 1);
        assert_eq!(
            d.outcome(".github/workflows/a.yml", "first")
                .unwrap()
                .outputs["value"],
            "A"
        );
        assert!(d.outcome(".github/workflows/b.yml", "first").is_none());
        settle(&mut d, &b, JobStatus::Success, "B");
        assert_eq!(d.admitted(), 0);
        assert_eq!(
            d.outcome(".github/workflows/b.yml", "first")
                .unwrap()
                .outputs["value"],
            "B"
        );
        assert!(d
            .pending_actions()
            .iter()
            .any(|a| a.kind == ActionKind::Finished));
    }
}

#[cfg(test)]
mod runtime_tests;
