//! Recomputable dynamic dispatch. Digests bind data; hosts authenticate transport.
use super::{digest, MAX_TASKS};
use crate::dag::{ExecutionTask, JobNode, NodeKind, OutputAggregation, TaskVariant};
use crate::expr::{Ctx, PortableStage, Status};
use crate::frozen::FrozenPlan;
use crate::job_evaluate::{self, Decision};
use crate::source_execute::{EvaluationSource, VerifiedEvaluationSource};
use crate::task::TaskInputs;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub const DISPATCH_VERSION: u32 = 1;
const MAX_FACT_BYTES: usize = 1024 * 1024;
const MAX_DERIVED_BYTES: usize = 8 * 1024 * 1024;

/// Explicit host-authorized portable facts, never inferred from a worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableFacts {
    pub schema_version: u32,
    pub github: Map<String, Value>,
    pub vars: Map<String, Value>,
    pub inputs: Map<String, Value>,
}
impl PortableFacts {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == DISPATCH_VERSION,
            "unsupported portable facts version"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= MAX_FACT_BYTES,
            "portable facts exceed byte bound"
        );
        for key in self.github.keys() {
            ensure!(
                key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid github fact key"
            );
            crate::expr::validate_portable(&format!("github.{key}"), true, PortableStage::Matrix)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerRequirement {
    pub job: String,
    pub original: Option<Value>,
    pub evaluated: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedDispatch {
    schema_version: u32,
    plan_digest: String,
    origin: String,
    task: String,
    index: usize,
    inputs_digest: String,
    evaluation_digest: String,
    expansion_digest: String,
}
impl DerivedDispatch {
    pub fn task_id(&self) -> &str {
        &self.task
    }
    pub fn origin(&self) -> &str {
        &self.origin
    }
    pub fn index(&self) -> usize {
        self.index
    }
    pub fn from_json(text: &str) -> Result<Self> {
        ensure!(text.len() <= MAX_FACT_BYTES, "dispatch exceeds byte bound");
        Ok(serde_json::from_str(text)?)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EvaluationContext {
    pub source: EvaluationSource,
    pub facts: PortableFacts,
}
impl EvaluationContext {
    pub fn new(
        plan: &FrozenPlan,
        source: &VerifiedEvaluationSource,
        facts: PortableFacts,
    ) -> Result<Self> {
        facts.validate()?;
        ensure!(
            source.record().plan_digest() == plan.digest,
            "evaluation source plan mismatch"
        );
        Ok(Self {
            source: source.record().clone(),
            facts,
        })
    }
    pub fn contexts(&self, plan: &FrozenPlan, task: &ExecutionTask) -> Result<Map<String, Value>> {
        let wf = plan
            .workflows
            .iter()
            .find(|w| w.file == task.workflow)
            .context("missing evaluation workflow")?;
        let mut github = self.facts.github.clone();
        // These facts are actually present in the selected source plan.
        github.insert("workflow".into(), Value::String(wf.name.clone()));
        github.insert("event_name".into(), Value::String(wf.event.clone()));
        if task.jobs.len() == 1 {
            github.insert("job".into(), Value::String(task.jobs[0].clone()));
        }
        Ok([
            ("github".into(), Value::Object(github)),
            ("vars".into(), Value::Object(self.facts.vars.clone())),
            ("inputs".into(), Value::Object(self.facts.inputs.clone())),
        ]
        .into_iter()
        .collect())
    }
    pub fn requirement(
        &self,
        task: &ExecutionTask,
        job: &str,
        contexts: &Map<String, Value>,
    ) -> Result<RunnerRequirement> {
        let wf = self
            .source
            .workflows()
            .iter()
            .find(|w| w.workflow() == task.workflow)
            .context("missing source workflow")?;
        let source = wf
            .jobs()
            .iter()
            .find(|j| j.job() == job)
            .context("missing source job")?;
        let original = source.runs_on().cloned();
        let mut contexts = contexts.clone();
        if let Some(github) = contexts.get_mut("github").and_then(Value::as_object_mut) {
            github.insert("job".into(), Value::String(job.into()));
        }
        let evaluated = original
            .as_ref()
            .map(|value| {
                job_evaluate::validate_strings(value, &contexts, PortableStage::RunsOn)?;
                job_evaluate::eval_strings(value, &Ctx::new(&contexts, Status::Success))
            })
            .transpose()?;
        Ok(RunnerRequirement {
            job: job.into(),
            original,
            evaluated,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Selection {
    pub task: ExecutionTask,
    pub display: String,
    pub matrix: Value,
    pub strategy: Value,
    pub requirement: RunnerRequirement,
}
impl Selection {
    pub fn worker_evaluation(
        &self,
        github: Value,
        runner: Value,
        needs: &Map<String, Value>,
        facts: &PortableFacts,
    ) -> job_evaluate::JobEvaluation {
        let mut github = github;
        if let Some(object) = github.as_object_mut() {
            object.extend(
                facts
                    .github
                    .iter()
                    .filter(|(key, _)| !matches!(key.as_str(), "workflow" | "job" | "event_name"))
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        let contexts = [
            ("github".into(), github),
            ("runner".into(), runner),
            ("needs".into(), Value::Object(needs.clone())),
            ("matrix".into(), self.matrix.clone()),
            ("strategy".into(), self.strategy.clone()),
            ("vars".into(), Value::Object(facts.vars.clone())),
            ("inputs".into(), Value::Object(facts.inputs.clone())),
            ("env".into(), json!({})),
        ]
        .into_iter()
        .collect();
        job_evaluate::JobEvaluation {
            entries: vec![job_evaluate::EvaluatedEntry {
                display: self.display.clone(),
                decision: Decision::Run(contexts),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Expansion {
    pub group: JobNode,
    pub selections: Vec<Selection>,
    pub dispatches: Vec<DerivedDispatch>,
    pub inputs: TaskInputs,
}
pub(crate) enum Derivation {
    Expanded(Box<Expansion>),
    Skipped(String),
    Failed(String),
}

pub(crate) fn derive(
    plan: &FrozenPlan,
    evaluation: &EvaluationContext,
    origin: &ExecutionTask,
    inputs: &TaskInputs,
    limit: usize,
) -> Result<Derivation> {
    ensure!(
        origin.dynamic_matrix
            && origin.jobs.len() == 1
            && origin.variant.is_none()
            && origin.shard.is_none(),
        "not a supported dynamic origin"
    );
    ensure!(
        inputs.schema_version == crate::wire::WIRE_SCHEMA_VERSION,
        "invalid task inputs version"
    );
    let wf = plan
        .workflows
        .iter()
        .find(|w| w.file == origin.workflow)
        .context("missing workflow")?;
    let spec = wf.job(&origin.jobs[0]).context("missing dynamic job")?;
    ensure!(
        spec.needs.iter().all(|n| inputs.needs.contains_key(n)),
        "missing dynamic needs"
    );
    let source_job = evaluation
        .source
        .workflows()
        .iter()
        .find(|w| w.workflow() == origin.workflow)
        .and_then(|w| w.jobs().iter().find(|j| j.job() == origin.jobs[0]))
        .context("missing source scheduling job")?;
    // Bound copied data before Cartesian construction, including duplicated
    // needs/requirements, rather than relying only on a member-count bound.
    let per_member = serde_json::to_vec(&(origin, inputs, &evaluation.facts, source_job))?
        .len()
        .max(1);
    let limit = limit.min(MAX_DERIVED_BYTES / per_member);
    let contexts = evaluation.contexts(plan, origin)?;
    let ancestor_failed = spec
        .needs
        .iter()
        .any(|n| inputs.needs[n].failed_or_ancestor_failed());
    let evaluated = job_evaluate::evaluate_portable(
        &spec.to_job(),
        &contexts,
        &inputs.context(),
        ancestor_failed,
        inputs.run_cancelled,
        limit.min(MAX_TASKS),
    );
    let mut selections = Vec::new();
    for (index, entry) in evaluated.entries.into_iter().enumerate() {
        let cm = match entry.decision {
            Decision::Run(cm) => cm,
            Decision::Skip(reason) => return Ok(Derivation::Skipped(reason)),
            Decision::Error(reason) => return Ok(Derivation::Failed(reason)),
        };
        let mut task = origin.clone();
        task.id = format!("{}#{index}", origin.id);
        ensure!(
            !plan.tasks.iter().any(|t| t.id == task.id),
            "derived member collides with frozen task"
        );
        task.nodes = vec![task.id.clone()];
        task.dynamic_matrix = false;
        task.variant = Some(TaskVariant {
            group: origin.nodes[0].clone(),
            index,
            values: cm["matrix"]
                .as_object()
                .context("missing derived matrix")?
                .clone(),
            label: entry.display.clone(),
        });
        let requirement = match evaluation.requirement(&task, &task.jobs[0], &cm) {
            Ok(requirement) => requirement,
            Err(error) => return Ok(Derivation::Failed(format!("runs-on: {error:#}"))),
        };
        selections.push(Selection {
            task,
            display: entry.display,
            matrix: cm["matrix"].clone(),
            strategy: cm["strategy"].clone(),
            requirement,
        });
    }
    ensure!(!selections.is_empty(), "empty derived evaluation");
    let group = JobNode {
        id: origin.nodes[0].clone(),
        workflow: origin.workflow.clone(),
        job: origin.jobs[0].clone(),
        depends_on: vec![],
        kind: NodeKind::MatrixGroup {
            variants: selections.iter().map(|s| s.task.nodes[0].clone()).collect(),
            fail_fast: spec.fail_fast,
            max_parallel: spec.max_parallel,
            aggregation: OutputAggregation::MatrixOrder,
        },
    };
    let inputs_digest = digest(inputs)?;
    let evaluation_digest = digest(evaluation)?;
    let expansion_digest = digest(&(&group, &selections, &inputs_digest, &evaluation_digest))?;
    let dispatches = selections
        .iter()
        .enumerate()
        .map(|(index, selection)| DerivedDispatch {
            schema_version: DISPATCH_VERSION,
            plan_digest: plan.digest.clone(),
            origin: origin.id.clone(),
            task: selection.task.id.clone(),
            index,
            inputs_digest: inputs_digest.clone(),
            evaluation_digest: evaluation_digest.clone(),
            expansion_digest: expansion_digest.clone(),
        })
        .collect();
    Ok(Derivation::Expanded(Box::new(Expansion {
        group,
        selections,
        dispatches,
        inputs: inputs.clone(),
    })))
}

pub(crate) fn validate(
    plan: &FrozenPlan,
    source: &VerifiedEvaluationSource,
    facts: &PortableFacts,
    dispatch: &DerivedDispatch,
    inputs: &TaskInputs,
) -> Result<Selection> {
    let evaluation = EvaluationContext::new(plan, source, facts.clone())?;
    let origin = plan
        .task(&dispatch.origin)
        .context("unknown dispatch origin")?;
    let Derivation::Expanded(expansion) = derive(plan, &evaluation, origin, inputs, MAX_TASKS)?
    else {
        anyhow::bail!("dispatch origin does not expand");
    };
    let expected = expansion
        .dispatches
        .get(dispatch.index)
        .context("dispatch is not an expansion member")?;
    ensure!(expected == dispatch, "derived dispatch proof mismatch");
    Ok(expansion.selections[dispatch.index].clone())
}
