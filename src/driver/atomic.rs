//! One immutable ordered worker task, including its internal matrices.
use super::*;
use crate::expr::PortableStage;
use crate::task::TaskResult;
use serde_json::{json, Map, Value};

/// Derived solely from frozen membership, bound by plan and Driver version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionMode {
    Independent,
    /// One claim, permit and workspace until the real aggregate settles.
    /// Internal matrix entries are worker-local, never independent dispatches.
    AtomicSharedWorkspace {
        placement: GroupPlacement,
    },
}
/// The core preserves each original runs-on descriptor. It cannot certify
/// that heterogeneous jobs or labels depending on internal outcomes co-place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupPlacement {
    Unknown,
}

/// Worker runtime contexts cannot yet be bound to atomic report membership.
#[derive(Debug)]
pub struct AtomicGroupUnsupported {
    pub task: String,
    pub reason: String,
}
impl std::fmt::Display for AtomicGroupUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported atomic group {}: {}", self.task, self.reason)
    }
}
impl std::error::Error for AtomicGroupUnsupported {}

pub(super) fn mode(task: &ExecutionTask) -> TaskExecutionMode {
    if task.jobs.len() > 1 {
        TaskExecutionMode::AtomicSharedWorkspace {
            placement: GroupPlacement::Unknown,
        }
    } else {
        TaskExecutionMode::Independent
    }
}
fn contexts(plan: &FrozenPlan, task: &ExecutionTask, job: &str) -> Result<Map<String, Value>> {
    let wf = plan
        .workflows
        .iter()
        .find(|w| w.file == task.workflow)
        .context("missing workflow")?;
    // Existing ordinary Runner has no portable vars/inputs injection. Never
    // invent host runtime facts to reproduce matrix membership.
    Ok([(
        "github".into(),
        json!({"job":job,"workflow":wf.name,"event_name":wf.event}),
    )]
    .into_iter()
    .collect())
}
pub(super) fn validate_task(plan: &FrozenPlan, task: &ExecutionTask) -> Result<()> {
    if task.jobs.len() <= 1 {
        return Ok(());
    }
    ensure!(
        task.variant.is_none()
            && task.shard.is_none()
            && task.workspace.own_checkout
            && task.workspace.shared_by_jobs,
        "invalid atomic shared-workspace membership"
    );
    ensure!(
        task.nodes
            == task
                .jobs
                .iter()
                .map(|j| crate::dag::node_id(&task.workflow, j))
                .collect::<Vec<_>>(),
        "atomic group node/order mismatch"
    );
    ensure!(
        task.grouped_reason.is_some()
            && task.id == format!("{}::{}", task.workflow, task.jobs.join("+")),
        "atomic group identity mismatch"
    );
    let unit = plan.units.get(task.unit).context("missing atomic unit")?;
    ensure!(
        plan.workflows
            .get(unit.workflow)
            .is_some_and(|w| w.file == task.workflow),
        "atomic group unit/workflow mismatch"
    );
    let positions: Vec<_> = task
        .jobs
        .iter()
        .map(|job| {
            unit.jobs
                .iter()
                .position(|j| j == job)
                .context("atomic job outside unit")
        })
        .collect::<Result<_>>()?;
    ensure!(
        positions.windows(2).all(|p| p[0] < p[1]),
        "atomic group unit order mismatch"
    );
    let wf = plan
        .workflows
        .iter()
        .find(|w| w.file == task.workflow)
        .context("missing workflow")?;
    let mut earlier = BTreeSet::new();
    for id in &task.jobs {
        ensure!(earlier.insert(id), "duplicate atomic group job");
        let spec = wf.job(id).context("missing group job")?;
        ensure!(
            spec.needs
                .iter()
                .filter(|n| task.jobs.contains(n))
                .all(|n| n != id && earlier.contains(n)),
            "atomic group internal needs must precede consumer"
        );
        ensure!(
            plan.graph
                .nodes
                .iter()
                .any(|n| n.id == crate::dag::node_id(&task.workflow, id)
                    && n.workflow == task.workflow
                    && n.job == *id),
            "atomic group graph membership mismatch"
        );
        if let Some(matrix) = &spec.matrix {
            let check = || -> Result<()> {
                let contexts = contexts(plan, task, id)?;
                let value = crate::matrix::yaml_to_json(&matrix.to_yaml());
                // Portable membership syntax/contexts must be replayable;
                // malformed evaluated matrix data remains a real worker error.
                validate_membership_contexts(&value, &contexts, PortableStage::Matrix)?;
                if let Some(name) = &spec.name {
                    validate_membership_contexts(
                        &Value::String(name.clone()),
                        &contexts,
                        PortableStage::Name,
                    )?;
                }
                Ok(())
            };
            if let Err(error) = check() {
                return Err(AtomicGroupUnsupported {
                    task: task.id.clone(),
                    reason: format!("matrix membership context: {error:#}"),
                }
                .into());
            }
        }
    }
    Ok(())
}
fn validate_membership_contexts(
    value: &Value,
    contexts: &Map<String, Value>,
    stage: PortableStage,
) -> Result<()> {
    crate::job_evaluate::validate_strings(value, contexts, stage)
}

pub(super) fn validate_readiness(
    plan: &FrozenPlan,
    producers: &BTreeMap<(String, String, Option<usize>), Vec<String>>,
) -> Result<()> {
    for (index, task) in plan.tasks.iter().enumerate() {
        let wf = plan
            .workflows
            .iter()
            .find(|w| w.file == task.workflow)
            .context("missing workflow")?;
        for job in &task.jobs {
            for need in &wf.job(job).context("missing job")?.needs {
                if task.jobs.contains(need) {
                    continue;
                }
                let owners: BTreeSet<_> = producers
                    .iter()
                    .filter(|((w, j, _), _)| w == &task.workflow && j == need)
                    .flat_map(|(_, ids)| ids)
                    .collect();
                ensure!(!owners.is_empty(), "missing external logical producer");
                for owner in owners {
                    // A dependency from an early internal job through an
                    // external task back to a late internal job cannot be
                    // satisfied atomically. Reject; never forge graph edges.
                    ensure!(
                        plan.tasks[..index].iter().any(|t| &t.id == owner)
                            && task.depends_on.contains(owner),
                        "unsafe atomic/external readiness dependency"
                    );
                }
            }
        }
    }
    Ok(())
}
pub(super) fn requirements(
    evaluation: &EvaluationContext,
    task: &ExecutionTask,
) -> Result<Vec<RunnerRequirement>> {
    let wf = evaluation
        .source
        .workflows()
        .iter()
        .find(|w| w.workflow() == task.workflow)
        .context("missing scheduling workflow")?;
    task.jobs
        .iter()
        .map(|job| {
            let source = wf
                .jobs()
                .iter()
                .find(|j| j.job() == job)
                .context("missing scheduling job")?;
            Ok(RunnerRequirement {
                job: job.clone(),
                original: source.runs_on().cloned(),
                evaluated: None,
            })
        })
        .collect()
}

pub(super) fn verify_result(
    plan: &FrozenPlan,
    task: &ExecutionTask,
    inputs: &TaskInputs,
    result: &TaskResult,
) -> Result<()> {
    ensure!(
        result.outcomes.iter().map(|o| &o.job).eq(task.jobs.iter()),
        "atomic outcome order/membership mismatch"
    );
    let wf = plan
        .workflows
        .iter()
        .find(|w| w.file == task.workflow)
        .context("missing workflow")?;
    let mut needs = inputs.context();
    let mut offset = 0;
    for (id, outcome) in task.jobs.iter().zip(&result.outcomes) {
        let spec = wf.job(id).context("missing group job")?;
        let ancestor = spec.needs.iter().any(|n| {
            result
                .outcomes
                .iter()
                .find(|o| &o.job == n)
                .or_else(|| inputs.needs.get(n))
                .is_some_and(JobOutcome::failed_or_ancestor_failed)
        });
        let evaluated = if spec.matrix.is_some() {
            Some(
                crate::job_evaluate::evaluate_atomic(
                    &spec.to_job(),
                    contexts(plan, task, id)?["github"].clone(),
                    &needs,
                    ancestor,
                    inputs.run_cancelled,
                )
                .map_err(|e| AtomicGroupUnsupported {
                    task: task.id.clone(),
                    reason: format!("{e:#}"),
                })?,
            )
        } else {
            None
        };
        let count = evaluated.as_ref().map_or(1, |e| e.entries.len());
        ensure!(count > 0, "empty atomic report membership");
        let reports = result
            .jobs
            .get(offset..offset + count)
            .context("missing atomic matrix reports")?;
        ensure!(
            reports
                .iter()
                .all(|r| r.workflow == task.workflow && r.job == *id),
            "atomic report order/membership mismatch"
        );
        if let Some(evaluated) = evaluated {
            ensure!(
                reports
                    .iter()
                    .zip(&evaluated.entries)
                    .all(|(r, e)| r.display == e.display),
                "atomic matrix report display/order mismatch"
            );
        }
        let statuses: Vec<_> = reports.iter().map(JobStatus::of).collect();
        let status = if statuses.contains(&JobStatus::Failure) {
            JobStatus::Failure
        } else if statuses.contains(&JobStatus::Cancelled) {
            JobStatus::Cancelled
        } else if statuses.iter().all(|s| *s == JobStatus::Skipped) {
            JobStatus::Skipped
        } else {
            JobStatus::Success
        };
        ensure!(
            outcome.status == status,
            "atomic aggregate report/status mismatch"
        );
        needs.insert(id.clone(), crate::task::ctx_of(outcome));
        offset += count;
    }
    ensure!(
        offset == result.jobs.len(),
        "unexpected atomic matrix reports"
    );
    Ok(())
}
