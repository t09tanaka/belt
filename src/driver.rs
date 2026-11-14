//! Transactional, host-neutral progression for frozen tasks.
//!
//! Dynamic members are derived from verified source metadata and portable facts.
//! Shards, worker authentication and retries remain host/following-slice work.
//! Hosts durably commit a proposed transition before applying it and sending
//! its outbox. A claim acknowledges Ready; only settlement releases admission.
//! Worker ownership/source/receipt authentication remains the host's duty.

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

pub const DRIVER_VERSION: u32 = 2;
const MAX_TASKS: usize = 4096;
const MAX_TEXT: usize = 16384;

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
    Claim {
        task: String,
        token: String,
    },
    /// A real worker result. Host must fence/verify attempt ownership first.
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
            text.len() <= MAX_WIRE_BYTES,
            "transition exceeds byte bound"
        );
        Ok(serde_json::from_str(text)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreDecision {
    UnstartedSkipped,
    EarlyFailure,
    MatrixJoined,
}

/// A never-started decision is deliberately distinct from a worker receipt.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Terminal {
    Worker {
        token: String,
        result_digest: String,
        outcomes: Vec<JobOutcome>,
    },
    Unstarted {
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
            Self::Worker { outcomes, .. }
            | Self::Unstarted { outcomes }
            | Self::Core { outcomes, .. } => outcomes,
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
    },
    Terminal(Terminal),
}
#[derive(Debug, Clone, Serialize)]
struct State {
    revision: u64,
    phases: BTreeMap<String, Phase>,
    expansions: BTreeMap<String, Expansion>,
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
        ensure!(
            plan.shard_groups.iter().all(|g| !g.split),
            "driver slice does not support shards"
        );
        let mut ids = BTreeSet::new();
        let mut producers = BTreeMap::new();
        for t in &plan.tasks {
            ensure!(
                (!t.dynamic_matrix || evaluation.is_some()) && t.shard.is_none(),
                "driver needs evaluation context for dynamic unsharded tasks"
            );
            ensure!(
                !t.jobs.is_empty() && ids.insert(t.id.clone()),
                "empty/duplicate task"
            );
            if t.dynamic_matrix {
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
                let g = plan
                    .graph
                    .nodes
                    .iter()
                    .find(|g| g.id == v.group)
                    .context("missing matrix group")?;
                let NodeKind::MatrixGroup {
                    variants,
                    max_parallel,
                    ..
                } = &g.kind
                else {
                    bail!("not a static matrix group")
                };
                ensure!(
                    g.workflow == t.workflow && g.job == t.jobs[0],
                    "matrix workflow/job mismatch"
                );
                ensure!(max_parallel != &Some(0), "zero matrix admission limit");
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
                            t.id.clone()
                        )
                        .is_none(),
                    "duplicate matrix member"
                );
            } else {
                for j in &t.jobs {
                    ensure!(
                        producers
                            .insert((t.workflow.clone(), j.clone(), None), t.id.clone())
                            .is_none(),
                        "duplicate job producer"
                    );
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
            if let NodeKind::MatrixGroup { variants, .. } = &g.kind {
                ensure!(
                    !variants.is_empty(),
                    "empty matrix unsupported in this slice"
                );
                for i in 0..variants.len() {
                    ensure!(
                        producers.contains_key(&(g.workflow.clone(), g.job.clone(), Some(i))),
                        "incomplete matrix task membership"
                    );
                }
                ensure!(
                    !producers.contains_key(&(g.workflow.clone(), g.job.clone(), None)),
                    "mixed matrix/job producers"
                );
            }
        }
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
                phases,
                expansions: BTreeMap::new(),
                outcomes: BTreeMap::new(),
                outbox: vec![],
                finished: false,
            },
        };
        driver.bound(&driver.state)?;
        Ok(driver)
    }
    pub fn core_decision(&self, task: &str) -> Option<(CoreDecision, Option<&str>)> {
        match self.state.phases.get(task)? {
            Phase::Terminal(Terminal::Core {
                decision,
                diagnostic,
                ..
            }) => Some((*decision, diagnostic.as_deref())),
            _ => None,
        }
    }
    /// Placement descriptors from immutable source facts, including static jobs.
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
        let mut contexts = evaluation.contexts(&self.plan, task)?;
        let inputs = match &self.state.phases[&task.id] {
            Phase::Ready { inputs } | Phase::Claimed { inputs, .. } => inputs,
            _ => anyhow::bail!("task inputs unavailable before readiness"),
        };
        contexts.insert("needs".into(), serde_json::Value::Object(inputs.context()));
        if let Some(v) = &task.variant {
            contexts.insert("matrix".into(), serde_json::Value::Object(v.values.clone()));
            let g = self.group(&self.state, &v.group).context("missing group")?;
            let NodeKind::MatrixGroup { variants, .. } = &g.kind else {
                anyhow::bail!("not matrix group")
            };
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
        ensure!(
            serde_json::to_vec(input)?.len() <= MAX_WIRE_BYTES,
            "input exceeds byte bound"
        );
        let mut s = self.state.clone();
        s.revision = s
            .revision
            .checked_add(1)
            .context("driver revision exhausted")?;
        match input {
            Input::Advance => {}
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
                    },
                );
                s.outbox
                    .retain(|a| ready_task(&a.kind) != Some(task.as_str()));
            }
            Input::Settle {
                task,
                token,
                result_digest,
                result,
            } => {
                let t = self.task(&s, task).context("unknown task")?;
                if let Phase::Terminal(Terminal::Worker {
                    token: old,
                    result_digest: old_digest,
                    ..
                }) = &s.phases[task]
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
                ensure!(r.shard.is_none(), "unexpected shard result");
                ensure!(
                    r.outcomes.len() == t.jobs.len(),
                    "missing/duplicate job outcomes"
                );
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
                for j in &r.jobs {
                    ensure!(
                        j.workflow == t.workflow && t.jobs.contains(&j.job),
                        "result report workflow/job mismatch"
                    );
                }
                for j in &t.jobs {
                    ensure!(
                        r.jobs.iter().any(|r| &r.job == j),
                        "worker result lacks job report"
                    );
                }
                let failed = r.outcomes.iter().any(|o| o.status == JobStatus::Failure);
                s.phases.insert(
                    task.clone(),
                    Phase::Terminal(Terminal::Worker {
                        token: token.clone(),
                        result_digest: result_digest.clone(),
                        outcomes: r.outcomes,
                    }),
                );
                s.outbox.retain(
                    |a| !matches!(&a.kind, ActionKind::Cancel { task: id, .. } if id == task),
                );
                if failed {
                    self.fail_fast(&mut s, &t)?;
                }
            }
        }
        self.progress(&mut s)?;
        self.checked(s)
    }
    fn checked(&self, s: State) -> Result<State> {
        self.bound(&s)?;
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
        Ok(())
    }
    fn push(&self, s: &mut State, key: &str, kind: ActionKind) {
        let id = format!("{}:{key}", self.plan.digest);
        if !s.outbox.iter().any(|a| a.id == id) {
            s.outbox.push(Action { id, kind });
        }
    }
    fn fail_fast(&self, s: &mut State, task: &ExecutionTask) -> Result<()> {
        let Some(v) = &task.variant else {
            return Ok(());
        };
        let g = self.group(s, &v.group).context("missing group")?;
        if !matches!(
            g.kind,
            NodeKind::MatrixGroup {
                fail_fast: true,
                ..
            }
        ) {
            return Ok(());
        }
        let ancestor_failed = match &s.phases[&task.id] {
            Phase::Terminal(t) => t.outcomes().iter().any(|o| o.ancestor_failed),
            _ => false,
        };
        for member in self.tasks(s) {
            if !member.variant.as_ref().is_some_and(|m| m.group == v.group) {
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
                        Phase::Terminal(Terminal::Unstarted { outcomes }),
                    );
                    s.outbox
                        .retain(|a| ready_task(&a.kind) != Some(member.id.as_str()));
                }
                Phase::Claimed { token, inputs, .. } => {
                    s.phases.insert(
                        member.id.clone(),
                        Phase::Claimed {
                            token: token.clone(),
                            inputs,
                            cancel_requested: true,
                        },
                    );
                    self.push(
                        s,
                        &format!("cancel:{}", member.id),
                        ActionKind::Cancel {
                            task: member.id.clone(),
                            token,
                        },
                    );
                }
                Phase::Terminal(_) | Phase::Expanded => {}
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
    fn progress(&self, s: &mut State) -> Result<()> {
        loop {
            let mut changed = false;
            for t in &self.plan.tasks {
                if t.variant.is_some() {
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
                if !members.is_empty()
                    && members
                        .iter()
                        .all(|t| matches!(s.phases[&t.id], Phase::Terminal(_)))
                {
                    let parts: Vec<_> = members
                        .iter()
                        .flat_map(|t| match &s.phases[&t.id] {
                            Phase::Terminal(x) => x.outcomes().to_vec(),
                            _ => vec![],
                        })
                        .collect();
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
            // Keep room for every pending origin's core terminal and logical
            // outcome, including siblings not yet reached in frozen order.
            let mut fallback_reserve = self.pending_fallback_reserve(s)?;
            for t in &self.plan.tasks {
                if !matches!(s.phases[&t.id], Phase::Pending)
                    || !t
                        .depends_on
                        .iter()
                        .all(|d| matches!(s.phases[d], Phase::Terminal(_)))
                {
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
                if !needs
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
                    false,
                );
                changed = true;
                if t.dynamic_matrix {
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
                        .saturating_sub(fallback_reserve);
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
                    match derivation {
                        Derivation::Expanded(expansion) => {
                            for selection in &expansion.selections {
                                ensure!(
                                    !s.phases.contains_key(&selection.task.id),
                                    "derived membership collision"
                                );
                            }
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
                        }
                        terminal => {
                            let (decision, status, reason) = match terminal {
                                Derivation::Skipped(reason) => {
                                    (CoreDecision::UnstartedSkipped, JobStatus::Skipped, reason)
                                }
                                Derivation::Failed(reason) => {
                                    (CoreDecision::EarlyFailure, JobStatus::Failure, reason)
                                }
                                _ => unreachable!(),
                            };
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
            }
            if !changed {
                break;
            }
        }
        if !s.finished && s.phases.values().all(|p| matches!(p, Phase::Terminal(_))) {
            s.finished = true;
            self.push(s, "finished", ActionKind::Finished);
        }
        Ok(())
    }
}
fn ready_task(action: &ActionKind) -> Option<&str> {
    match action {
        ActionKind::Ready { task, .. } => Some(task),
        ActionKind::DerivedReady { dispatch, .. } => Some(dispatch.task_id()),
        _ => None,
    }
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
        let ActionKind::Ready { inputs, .. } = &d
            .pending_actions()
            .iter()
            .find(|a| matches!(&a.kind, ActionKind::Ready { task, .. } if task == &next[0]))
            .unwrap()
            .kind
        else {
            panic!()
        };
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
            Phase::Terminal(Terminal::Unstarted { .. })
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
}

#[cfg(test)]
mod runtime_tests;
