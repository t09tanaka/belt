//! Frozen shard membership and genuine-receipt joins; no executor or scheduler.
use super::*;
use crate::complete::{aggregate_shard_group, ShardAggregate};
use crate::dag::ShardGroup;
use crate::shard::ShardOutcome;

/// The current planner only splits independent, nonmatrix single jobs.
#[derive(Debug)]
pub struct FrozenShardUnsupported {
    pub reason: String,
}
impl std::fmt::Display for FrozenShardUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported frozen shards: {}", self.reason)
    }
}
impl std::error::Error for FrozenShardUnsupported {}
fn unsupported(reason: &str) -> anyhow::Error {
    FrozenShardUnsupported {
        reason: reason.into(),
    }
    .into()
}

pub(super) fn validate(plan: &FrozenPlan) -> Result<()> {
    let split = plan.shard_groups.iter().filter(|g| g.split).count();
    if split == 0 && plan.tasks.iter().all(|t| t.shard.is_none()) {
        return Ok(());
    }
    for task in plan.tasks.iter().filter(|t| t.shard.is_some()) {
        if task.jobs.len() != 1
            || task.variant.is_some()
            || task.dynamic_matrix
            || task.grouped_reason.is_some()
            || task.workspace.shared_by_jobs
        {
            return Err(unsupported(
                "split matrix/dynamic/grouped tasks are not planner-producible",
            ));
        }
    }
    // derive_tasks preserves the request's indices: require the planner's real
    // partition index/order convention, which shard arguments rely upon.
    let mut seen = BTreeSet::new();
    for group in plan.shard_groups.iter().filter(|g| g.split) {
        if !seen.insert(&group.id) || group.source_jobs.len() != 1 || group.shards.len() < 2 {
            return Err(unsupported("duplicate/empty/grouped shard group"));
        }
        for (index, id) in group.shards.iter().enumerate() {
            let task = plan
                .task(id)
                .ok_or_else(|| unsupported("missing frozen shard"))?;
            let shard = task
                .shard
                .as_ref()
                .ok_or_else(|| unsupported("nonshard member"))?;
            if shard.index != index
                || shard.count != group.shards.len()
                || task.id
                    != crate::dag::shard_task_id(&group.workflow, &group.source_jobs[0], index)
            {
                return Err(unsupported("shard identity/index/count mismatch"));
            }
        }
    }
    // Use the original planner's request/manifest/structural checks, rather than
    // inventing a second test inventory or weakening native partial manifests.
    let requests = plan.shard_requests();
    let (tasks, groups) =
        crate::dag::derive_tasks(&plan.to_plan(), &plan.workflows, &plan.units, &requests);
    if tasks != plan.tasks || groups != plan.shard_groups {
        return Err(unsupported(
            "tasks/groups do not equal the canonical frozen shard partition",
        ));
    }
    Ok(())
}
pub(super) fn verify_receipt(task: &ExecutionTask, receipt: Option<&ShardOutcome>) -> Result<()> {
    match (&task.shard, receipt) {
        (None, None) => Ok(()),
        (Some(_), Some(r)) => {
            ensure!(
                r.shard_id == task.id && r.job_id == task.jobs[0],
                "shard receipt identity mismatch"
            );
            Ok(())
        }
        _ => bail!("missing or unexpected shard receipt"),
    }
}
pub(super) fn failed(receipt: &ShardOutcome) -> bool {
    !receipt.exit_ok
        || receipt
            .tests
            .iter()
            .any(|t| t.status == crate::shard::TestStatus::Failed)
}

/// Only bounded core diagnostics are persisted. Full exact coverage can always
/// be recomputed from immutable frozen inventory and genuine receipt projections.
#[derive(Debug, Clone, Serialize)]
pub(super) struct JoinSummary {
    pub diagnostic: Option<String>,
}
/// A readonly join view. A core skip/abort is never a fabricated shard receipt.
#[derive(Debug)]
pub struct ShardJoin {
    pub aggregate: ShardAggregate,
    pub outcome: JobOutcome,
    pub diagnostic: Option<String>,
    pub core_members: Vec<String>,
}
pub(super) struct Joined {
    pub outcome: JobOutcome,
    pub summary: JoinSummary,
}
fn receipts(group: &ShardGroup, phases: &BTreeMap<String, Phase>) -> Vec<ShardOutcome> {
    group
        .shards
        .iter()
        .filter_map(|id| match &phases[id] {
            Phase::Terminal(Terminal::Worker { shard, .. }) => shard.clone(),
            _ => None,
        })
        .collect()
}
fn outcomes(group: &ShardGroup, phases: &BTreeMap<String, Phase>) -> Vec<JobOutcome> {
    group
        .shards
        .iter()
        .filter_map(|id| phases[id].terminal_outcomes())
        .flatten()
        .cloned()
        .collect()
}
fn logical(
    group: &ShardGroup,
    phases: &BTreeMap<String, Phase>,
) -> (ShardAggregate, JobOutcome, Option<String>) {
    let aggregate = aggregate_shard_group(group, &receipts(group, phases));
    let parts = outcomes(group, phases);
    let mut problems = aggregate.problems();
    problems.extend(crate::task::conflicting_outputs(&parts));
    let mut outcome = crate::task::combine(&group.source_jobs[0], parts);
    // Bounded summary avoids derived diagnostic amplification in durable state.
    // Do not concatenate all problem strings before truncating.
    let diagnostic = (!problems.is_empty()).then(|| {
        problems
            .iter()
            .flat_map(|p| p.chars().chain("; ".chars()))
            .take(512)
            .collect::<String>()
    });
    if outcome.status == JobStatus::Success {
        if let Some(why) = &diagnostic {
            outcome.status = JobStatus::Failure;
            outcome.skipped = Some(why.clone());
        }
    }
    (aggregate, outcome, diagnostic)
}
pub(super) fn join(group: &ShardGroup, phases: &BTreeMap<String, Phase>) -> Result<Option<Joined>> {
    if !group
        .shards
        .iter()
        .all(|id| matches!(phases[id], Phase::Terminal(_)))
    {
        return Ok(None);
    }
    let (_, outcome, diagnostic) = logical(group, phases);
    Ok(Some(Joined {
        outcome,
        summary: JoinSummary { diagnostic },
    }))
}
pub(super) fn view(
    group: &ShardGroup,
    phases: &BTreeMap<String, Phase>,
    summary: &JoinSummary,
) -> ShardJoin {
    let (aggregate, outcome, _) = logical(group, phases);
    ShardJoin {
        aggregate,
        outcome,
        diagnostic: summary.diagnostic.clone(),
        core_members: group
            .shards
            .iter()
            .filter(|id| !matches!(phases[*id], Phase::Terminal(Terminal::Worker { .. })))
            .cloned()
            .collect(),
    }
}
