//! Placement decisions.
//!
//! [`decide`] holds the judgement (pure). This module holds what the
//! coordinator's actor needs around it: the settings, the estimates the
//! parent itself brings to a comparison, the forecast question for a task,
//! and the record of each decision.
//!
//! One actor decides: the coordinator calls into here only while it holds
//! its actor lock, so two decisions never see the same free slot. The
//! forecast questions of one decision go out side by side; the commit that
//! follows is one transaction, which counts the worker's load again.

pub mod decide;
pub mod measured;
pub mod preview;

use crate::forecast::plan::MAX_PENDING_PLANS;
use crate::ids::{AttemptId, NodeId, PlanDigest, TaskId, WorkerId};
use crate::storage::{CoordinatorStore, Placement, QueuedTask, PAGE_MAX};
use crate::wire::{
    AttemptPayload, CommandSpec, ConcurrencyLimits, Correlation, WorkloadNode, WorkloadPlan,
};
use decide::{Costs, Decision, Rules};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The reason recorded for a retire the coordinator starts because the
/// worker keeps refusing the attempt for a reason of its own. Once it is
/// confirmed the task is free again, and that worker is left out for it.
pub const REASON_WORKER_CANNOT_RUN: &str = "worker_cannot_run";

/// The event on an attempt that says why its worker is one the task is not
/// placed on again. Payload: `{"worker", "code", "reason"}`. It is written
/// with the tombstone, in one transaction; what excludes the worker is the
/// tombstone's reason ([`REASON_WORKER_CANNOT_RUN`]), not this event.
pub const EVENT_WORKER_EXCLUDED: &str = "worker_excluded";

/// The event on a run for a task no worker could be chosen for. Payload:
/// `{"task", "decision"}`; written again only when the outcome or a lasting
/// exclusion changes ([`decide::same_wait`]).
pub const EVENT_TASK_WAITING: &str = "task_waiting";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub rules: Rules,
    /// How long one decision waits for a worker's forecast. A worker that
    /// takes longer is left out for the rest of the tick. It must be below
    /// `rules.forecast_ttl_ms` ([`SchedulerConfig::check`]): the answers of
    /// the quick workers must still be fresh when the slowest one is given
    /// up. Not a key of the configuration file: the default is used.
    pub forecast_wait_ms: u64,
}

impl SchedulerConfig {
    /// Settings no decision could be made with are refused: a wait as long
    /// as an answer lives would make every answer too old to use.
    pub fn check(&self) -> Result<(), String> {
        if self.forecast_wait_ms < self.rules.forecast_ttl_ms {
            Ok(())
        } else {
            Err(format!(
                "forecast_wait_ms ({}) must be below forecast_ttl_ms ({})",
                self.forecast_wait_ms, self.rules.forecast_ttl_ms
            ))
        }
    }
}

/// The range of a worker's configured priority (spec chapter 9).
pub const PRIORITY_MIN: i64 = -100;
pub const PRIORITY_MAX: i64 = 100;

impl Default for SchedulerConfig {
    fn default() -> Self {
        SchedulerConfig {
            rules: Rules {
                priority_step_ms: 60_000,
                tie_window_ms: 1000,
                forecast_ttl_ms: 2000,
            },
            forecast_wait_ms: 1000,
        }
    }
}

/// What the parent itself estimates for a task on a worker: transfer,
/// setup, collection, cache state, affinity. The worker's forecast covers
/// the queue and the run.
pub trait CostModel: Send + Sync {
    fn costs(&self, worker_id: &WorkerId, task: &QueuedTask) -> Costs;
    fn query(&self, _worker: &WorkerId, _query: &measured::CostQuery) -> Costs {
        Costs::default()
    }
    fn evidence(
        &self,
        _worker: &WorkerId,
        _query: &measured::CostQuery,
    ) -> Option<measured::CostEvidence> {
        None
    }
    fn record(&self, _sample: measured::Sample) {}
    fn record_preparation(&self, _sample: measured::PreparationSample) {}
    fn drain(&self) {}
    fn collection_policy(&self) -> Option<String> {
        None
    }
}

/// Nothing is known: every part is unknown for every worker, which leaves
/// the comparison to the workers' forecasts.
#[derive(Debug, Clone, Copy, Default)]
pub struct UnknownCosts;

impl CostModel for UnknownCosts {
    fn evidence(
        &self,
        _worker: &WorkerId,
        _query: &measured::CostQuery,
    ) -> Option<measured::CostEvidence> {
        Some(measured::CostEvidence::unknown(
            "parent cost measurements unavailable",
        ))
    }
    fn costs(&self, _worker_id: &WorkerId, _task: &QueuedTask) -> Costs {
        Costs::default()
    }
}

/// The scheduler's state inside a coordinator.
pub struct Scheduler {
    pub cfg: SchedulerConfig,
    costs: Arc<dyn CostModel>,
    priorities: Mutex<BTreeMap<WorkerId, i64>>,
    seq: AtomicU64,
    /// A longer wait for a forecast that a test sets (0: none). A test that
    /// asks many workers' inspections at once on a loaded machine cannot
    /// rely on the production wait.
    #[cfg(test)]
    forecast_wait_override_ms: AtomicU64,
    /// The last decision for each task that waits, so that a task that
    /// waits for the same reason is recorded once.
    waiting: Mutex<BTreeMap<TaskId, Decision>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Scheduler::unchecked(SchedulerConfig::default(), Arc::new(UnknownCosts))
    }
}

impl Scheduler {
    /// Fails for settings [`SchedulerConfig::check`] refuses.
    pub fn new(cfg: SchedulerConfig, costs: Arc<dyn CostModel>) -> Result<Scheduler, String> {
        cfg.check()?;
        Ok(Scheduler::unchecked(cfg, costs))
    }

    fn unchecked(cfg: SchedulerConfig, costs: Arc<dyn CostModel>) -> Scheduler {
        Scheduler {
            cfg,
            costs,
            priorities: Mutex::default(),
            seq: AtomicU64::new(0),
            #[cfg(test)]
            forecast_wait_override_ms: AtomicU64::new(0),
            waiting: Mutex::default(),
        }
    }

    /// How long one question to a worker is waited for
    /// ([`SchedulerConfig::forecast_wait_ms`]).
    pub fn forecast_wait(&self) -> std::time::Duration {
        #[cfg(test)]
        {
            let over = self.forecast_wait_override_ms.load(Ordering::Relaxed);
            if over != 0 {
                return std::time::Duration::from_millis(over);
            }
        }
        std::time::Duration::from_millis(self.cfg.forecast_wait_ms)
    }

    /// Test only: wait this long instead of the production default.
    #[cfg(test)]
    pub(crate) fn set_forecast_wait_for_test(&self, wait: std::time::Duration) {
        self.forecast_wait_override_ms
            .store(wait.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn costs(&self, worker_id: &WorkerId, task: &QueuedTask) -> Costs {
        self.costs.costs(worker_id, task)
    }

    pub fn query(&self, worker: &WorkerId, query: &measured::CostQuery) -> Costs {
        let mut query = query.clone();
        query.collection_policy = self.costs.collection_policy();
        self.costs.query(worker, &query)
    }
    pub fn evidence(
        &self,
        worker: &WorkerId,
        query: &measured::CostQuery,
    ) -> Option<measured::CostEvidence> {
        let mut query = query.clone();
        query.collection_policy = self.costs.collection_policy();
        self.costs.evidence(worker, &query)
    }
    pub(crate) fn model(&self) -> Arc<dyn CostModel> {
        self.costs.clone()
    }

    /// A priority outside the range is taken as the nearest end of it.
    pub fn set_priority(&self, worker_id: &WorkerId, priority: i64) {
        let mut all = self.priorities.lock().unwrap_or_else(|e| e.into_inner());
        all.insert(
            worker_id.clone(),
            priority.clamp(PRIORITY_MIN, PRIORITY_MAX),
        );
    }

    /// The configured priority; 0 for a worker that was given none.
    pub fn priority_of(&self, worker_id: &WorkerId) -> i64 {
        let all = self.priorities.lock().unwrap_or_else(|e| e.into_inner());
        all.get(worker_id).copied().unwrap_or(0)
    }

    /// The number of the next decision of this process, counting from 1. It
    /// is local to the process: a restarted coordinator counts from 1 again,
    /// and a decision whose commit placed nothing uses one up. It orders the
    /// decisions of one process in a record; it identifies nothing in the
    /// database (the placement and the attempt have their own ids).
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Remember why a task waits. Returns whether this is news: the first
    /// reason, or another one than before. What differs only in what one
    /// tick saw (a worker that was full or silent) is kept, and is not news.
    pub fn note_waiting(&self, task_id: &TaskId, decision: &Decision) -> bool {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        let news = all
            .get(task_id)
            .is_none_or(|before| !decide::same_wait(before, decision));
        all.insert(task_id.clone(), decision.clone());
        news
    }

    /// Forget every waiting task that is not among `queued`: it was
    /// cancelled, its run was closed, or it went another way.
    pub fn keep_waiting(&self, queued: &BTreeSet<TaskId>) {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        all.retain(|task_id, _| queued.contains(task_id));
    }

    /// The task was placed (or is gone): it waits no more.
    pub fn clear_waiting(&self, task_id: &TaskId) {
        let mut all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        all.remove(task_id);
    }

    /// Why the task was not placed when it was last looked at, if it was
    /// looked at by this process and not placed since.
    pub fn waiting_reason(&self, task_id: &TaskId) -> Option<Decision> {
        let all = self.waiting.lock().unwrap_or_else(|e| e.into_inner());
        all.get(task_id).cloned()
    }
}

/// The plan a worker is asked to forecast for an attempt: its commands, with
/// what the parent knows about the time before it is ready (transfer and
/// setup) on the commands that wait for no other. The parent has no runtime
/// history of its own, so the runtimes are left to the worker.
pub fn workload_of(
    attempt_id: &AttemptId,
    plan_digest: &PlanDigest,
    commands: &[CommandSpec],
    ready_delay_ms: Option<u64>,
) -> WorkloadPlan {
    let nodes = commands
        .iter()
        .map(|c| (c.node_id.clone(), c.depends_on.clone()));
    workload_of_nodes(attempt_id, plan_digest, nodes, ready_delay_ms)
}

/// The id of the node step `index` of a CI task runs as, on the worker too
/// (`forecast::plan::accepted_nodes`, the supervisor's launches).
pub fn ci_step_node(index: u32) -> NodeId {
    NodeId::parse(&format!("step-{index}")).expect("a step id is a node id")
}

/// The work of a CI task of `steps` commands: one node per step, each after
/// the one before it, so the forecast is of the whole task and not of its
/// first step.
pub fn ci_workload_of(
    attempt_id: &AttemptId,
    plan_digest: &PlanDigest,
    steps: u32,
    ready_delay_ms: Option<u64>,
) -> WorkloadPlan {
    let nodes = (0..steps).map(|i| {
        let before = i.checked_sub(1).map(ci_step_node);
        (ci_step_node(i), before.into_iter().collect())
    });
    workload_of_nodes(attempt_id, plan_digest, nodes, ready_delay_ms)
}

fn workload_of_nodes(
    attempt_id: &AttemptId,
    plan_digest: &PlanDigest,
    nodes: impl Iterator<Item = (NodeId, Vec<NodeId>)>,
    ready_delay_ms: Option<u64>,
) -> WorkloadPlan {
    let nodes = nodes
        .map(|(node_id, depends_on)| WorkloadNode {
            external_delay_ms: ready_delay_ms.filter(|_| depends_on.is_empty()),
            depends_on,
            priority: 0,
            weight: None,
            predicted_rss_bytes: None,
            predicted_runtime_ms: None,
            ready_in_ms: None,
            concurrency: ConcurrencyLimits::default(),
            correlation: Correlation {
                attempt_id: attempt_id.clone(),
                node_id: node_id.clone(),
                ledger_entry: None,
            },
            node_id,
            shared_builder: None,
        })
        .collect();
    WorkloadPlan {
        attempt_id: attempt_id.clone(),
        plan_digest: plan_digest.clone(),
        expansion_seq: 0,
        nodes,
    }
}

/// The work the parent placed on a worker that the worker has not
/// acknowledged: it is not in the worker's own state yet, so the forecast
/// question carries it (spec chapter 9, step 6). The oldest first, and no
/// more than a worker accepts in one question.
///
/// The placements are read a page at a time and only their plans are kept,
/// so what this holds is bounded by one page and [`MAX_PENDING_PLANS`]
/// plans, however many placements wait.
pub fn pending_plans(
    store: &CoordinatorStore,
    worker_id: &WorkerId,
) -> crate::storage::Result<Vec<WorkloadPlan>> {
    let mut plans = Vec::new();
    let mut after = 0;
    while plans.len() < MAX_PENDING_PLANS {
        let page = store.unacked_demand_page(worker_id, after, PAGE_MAX)?;
        let Some(last) = page.last() else { break };
        after = last.placement_id;
        let room = MAX_PENDING_PLANS - plans.len();
        plans.extend(page.iter().filter_map(plan_of).take(room));
        if page.len() < PAGE_MAX as usize {
            break;
        }
    }
    Ok(plans)
}

/// The plan of one placement; `None` for one without an attempt or with a
/// payload that does not read.
fn plan_of(p: &Placement) -> Option<WorkloadPlan> {
    let attempt_id = p.attempt_id.as_ref()?;
    let payload: AttemptPayload = serde_json::from_value(p.demand.get("payload")?.clone()).ok()?;
    if payload.ci_task.is_some() {
        // The steps of a CI task are in its plan; the placement kept their number.
        let steps = p.demand.get("ci_steps").and_then(|n| n.as_u64());
        let steps = u32::try_from(steps.unwrap_or(0)).unwrap_or(u32::MAX);
        return Some(ci_workload_of(
            attempt_id,
            &payload.plan_digest,
            steps,
            None,
        ));
    }
    Some(workload_of(
        attempt_id,
        &payload.plan_digest,
        &payload.commands,
        None,
    ))
}

/// The decision that placed an attempt, as it was committed with the
/// placement. `None` for an attempt that was placed before decisions were
/// recorded, or whose record is gone.
pub fn reason_of(
    store: &CoordinatorStore,
    attempt_id: &AttemptId,
) -> crate::storage::Result<Option<Decision>> {
    let demand = store.placement_demand(attempt_id)?;
    Ok(demand
        .and_then(|d| d.get("decision").cloned())
        .and_then(|d| serde_json::from_value(d).ok()))
}

/// The number of the decision that placed an attempt, within the process
/// that made it ([`Scheduler::next_seq`]): not unique across restarts.
pub fn decision_seq_of(
    store: &CoordinatorStore,
    attempt_id: &AttemptId,
) -> crate::storage::Result<Option<u64>> {
    let demand = store.placement_demand(attempt_id)?;
    Ok(demand.and_then(|d| d.get("decision_seq").and_then(|s| s.as_u64())))
}

/// Parent component evidence committed with a real placement (not worker wire).
pub fn cost_evidence_of(
    store: &CoordinatorStore,
    attempt_id: &AttemptId,
) -> crate::storage::Result<Option<Vec<measured::WorkerCostEvidence>>> {
    Ok(store
        .placement_demand(attempt_id)?
        .and_then(|d| d.get("cost_evidence").cloned())
        .and_then(|value| serde_json::from_value(value).ok()))
}
