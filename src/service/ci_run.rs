//! `ci`: capture the input once, freeze its plan once, and submit every task
//! of the plan as one run. The tasks that need no other job are queued; the
//! others wait in the run until the resident's progression gives them the
//! results of the jobs they need ([`crate::ci::advance`]).
use super::{
    completion::Target,
    context::Context,
    control::{self, Report},
    resident::{Operation, Reply},
};
use crate::{
    coordinator::{CiSpec, TaskSpec},
    error::{ApiError, ErrorCode},
    ids::*,
    wire::CiTask,
};
use serde::Serialize;
use std::time::Duration;
use tokio::sync::mpsc;

/// One task of the plan as it was submitted.
#[derive(Debug, Clone, Serialize)]
pub struct SubmittedTask {
    pub task_id: TaskId,
    /// The id of the task in the frozen plan.
    pub plan_task: String,
    /// It waits for the jobs it needs.
    pub waits: bool,
}

#[derive(Debug, Serialize)]
pub struct Receipt {
    pub schema_version: u32,
    pub run_id: RunId,
    pub source_id: SourceId,
    pub plan_digest: Digest,
    pub tasks: Vec<SubmittedTask>,
    #[serde(skip)]
    pub warnings: Vec<String>,
}

/// How one job of the run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobLine {
    pub task_id: TaskId,
    pub plan_task: String,
    /// `None` when the task left no result: the line is of the task.
    pub job: Option<String>,
    /// `success`, `failure`, `skipped`, `cancelled`, or `no_result`.
    pub status: String,
    /// Why the job did not run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    /// Why the task could not be run at all (it was cancelled before it was
    /// placed, with this reason).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CiReport {
    #[serde(flatten)]
    pub report: Report,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<Digest>,
    pub jobs: Vec<JobLine>,
}

fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, message)
}

/// The tasks of `plan` as they are queued: one spec per task, in plan order.
/// A task that depends on another waits (`hold`) and has no inputs yet.
pub fn task_specs(
    plan: &local_ci::FrozenPlan,
    plan_digest: &Digest,
    manifest_digest: &Digest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
    command_timeout_ms: Option<u64>,
) -> Result<Vec<(String, TaskSpec)>, ApiError> {
    if let Some(why) = crate::ci::progress::unsupported(plan) {
        return Err(invalid(format!("this plan cannot be run yet: {why}")));
    }
    if plan.tasks.is_empty() {
        return Err(invalid(
            "the plan has no task: no workflow has a job to run",
        ));
    }
    let no_inputs = serde_json::to_value(local_ci::TaskInputs::default())
        .map_err(|e| invalid(e.to_string()))?;
    plan.tasks
        .iter()
        .map(|task| {
            let demand = local_ci::plan_task_demand(plan, &task.id)
                .map_err(|e| invalid(format!("task `{}`: {e:#}", task.id)))?;
            let spec = TaskSpec {
                ci: Some(CiSpec {
                    task: CiTask {
                        plan_digest: plan_digest.clone(),
                        task_id: task.id.clone(),
                        inputs: no_inputs.clone(),
                    },
                    hold: !task.depends_on.is_empty(),
                    steps: u32::try_from(demand.nodes.len()).unwrap_or(u32::MAX),
                    unrunnable: None,
                }),
                toolchains: None,
                requirements: requirements.clone(),
                in_place: false,
                setup: None,
                plan_digest: plan_digest.clone(),
                manifest_digest: manifest_digest.clone(),
                commands: Vec::new(),
                command_timeout_ms,
                deadline_ms: None,
                resource_wait_timeout_ms: None,
                has_secret_bundle: false,
                artifacts: Vec::new(),
                outputs: Vec::new(),
            };
            Ok((task.id.clone(), spec))
        })
        .collect()
}

pub async fn submit(
    context: Context,
    revision: Option<String>,
    run_id: RunId,
) -> Result<Receipt, ApiError> {
    let requirements = crate::toolchain::capability::ExecutionRequirements::parse(
        context.config.profile.value.clone(),
        &context.config.requires.value,
    )
    .map_err(invalid)?;
    let timeout = context
        .config
        .timeout_seconds
        .value
        .map(|s| {
            s.checked_mul(1000)
                .ok_or_else(|| invalid("command timeout overflow"))
        })
        .transpose()?;
    let owner = context
        .owner()
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?;
    let (frozen_context, frozen_run) = (context.clone(), run_id.clone());
    // Blocking source IO keeps its own ownership when this future is dropped.
    let frozen = tokio::task::spawn_blocking(move || -> Result<_, ApiError> {
        let (context, run_id) = (frozen_context, frozen_run);
        let captured = super::input::capture(&context, &run_id, revision.as_deref())?;
        if !captured.manifest.secret_files.is_empty() {
            return Err(invalid(
                "a CI run of a source with secret files cannot be run yet: the workspace of a \
                 CI task must be the captured source alone",
            ));
        }
        let manifest = super::input::source_manifest(&context, &run_id)?;
        let (store, _guard) = crate::source::SourceStore::open_foreground(context.state_dir())
            .map_err(|error| {
                if error.is::<crate::source::retention::ForegroundBusy>() {
                    crate::source::classify::api_error_of(&error)
                } else {
                    invalid(error.to_string())
                }
            })?;
        let plan = super::planning::freeze(&store, &manifest)?;
        let (plan_digest, _) = crate::ci::advance::keep_plan(context.state_dir(), &plan)
            .map_err(|e| invalid(format!("cannot keep the frozen plan: {e}")))?;
        Ok((captured, plan, plan_digest))
    })
    .await
    .map_err(|e| invalid(e.to_string()))?;
    let (captured, plan, plan_digest) = frozen?;
    let source_id = captured.manifest.source_id.clone();
    let manifest_digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .map_err(|e| invalid(e.to_string()))?;
    let specs = task_specs(
        &plan,
        &plan_digest,
        &manifest_digest,
        &requirements,
        timeout,
    )?;
    let mut submitted = Vec::with_capacity(specs.len());
    let mut tasks = Vec::with_capacity(specs.len());
    for (plan_task, spec) in specs {
        let task_id = TaskId::generate();
        submitted.push(SubmittedTask {
            task_id: task_id.clone(),
            plan_task,
            waits: spec.ci.as_ref().is_some_and(|ci| ci.hold),
        });
        tasks.push((task_id, spec));
    }
    let reply = owner
        .request(Operation::SubmitCaptured {
            run_id: run_id.clone(),
            source_id: source_id.clone(),
            tasks,
        })
        .await
        .map_err(|e| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!("submit unconfirmed for run {run_id}: {e}"),
            )
        })?;
    match reply {
        Reply::Accepted { run_id: accepted } if accepted == run_id => Ok(Receipt {
            schema_version: 1,
            run_id,
            source_id,
            plan_digest,
            tasks: submitted,
            warnings: captured.warnings,
        }),
        Reply::ControlError { error } => Err(error),
        Reply::Error { message } => Err(invalid(message)),
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "unexpected submit receipt",
        )),
    }
}

/// Submit the run and wait for it, like `exec`: an interrupt cancels the
/// run (every task of it, placed or waiting) and waits for the stop.
pub async fn run(
    context: Context,
    revision: Option<String>,
    signals: &mut mpsc::UnboundedReceiver<()>,
) -> Result<CiReport, ApiError> {
    let run_id = RunId::generate();
    let target = Target::Run(run_id.clone());
    let budget = Duration::from_secs(30);
    let submitted = submit(context.clone(), revision, run_id.clone());
    let (report, plan_digest) = tokio::select! {biased;
        _ = control::interrupted(signals) => {
            (control::cancel_and_stop(&context.bootstrap, target.clone(), signals, budget).await, None)
        }
        receipt = submitted => {
            let receipt = receipt?;
            for warning in &receipt.warnings {
                eprintln!("warning: {warning}");
            }
            let observed = control::wait(&context.bootstrap, target.clone(), signals).await?;
            let report = if observed.state == "wait_interrupted" {
                control::cancel_and_stop(&context.bootstrap, target, signals, budget).await
            } else {
                observed
            };
            (report, Some(receipt.plan_digest))
        }
    };
    let spec = context.bootstrap.clone();
    let jobs = tokio::task::spawn_blocking(move || jobs(&spec, &run_id))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    Ok(CiReport {
        report,
        plan_digest,
        jobs,
    })
}

/// How the jobs of a run ended, read from what the parent collected. Reads
/// only; a task that left no result gets one line of its own.
pub fn jobs(spec: &super::bootstrap::Spec, run: &RunId) -> Result<Vec<JobLine>, String> {
    let store = super::bootstrap::read_only_store(spec).map_err(|e| e.to_string())?;
    job_lines(&store, &spec.state_dir.join("results"), run)
}

pub fn job_lines(
    store: &crate::storage::CoordinatorStore,
    results: &std::path::Path,
    run: &RunId,
) -> Result<Vec<JobLine>, String> {
    let mut lines = Vec::new();
    for row in store.ci_run_tasks(run).map_err(|e| e.to_string())? {
        let Some(ci) = serde_json::from_value::<TaskSpec>(row.spec.clone())
            .ok()
            .and_then(|spec| spec.ci)
        else {
            continue;
        };
        let result = row.attempt.as_ref().and_then(|attempt| {
            let kept = store.persisted_artifacts(&attempt.attempt_id).ok()?;
            let (_, digest) = kept
                .iter()
                .find(|(name, _)| name == crate::supervisor::ci_task::RESULT_ARTIFACT)?;
            crate::ci::advance::read_result(results, &attempt.attempt_id, digest).ok()
        });
        let line = |job: Option<String>, status: &str, skipped: Option<String>| JobLine {
            task_id: row.task_id.clone(),
            plan_task: ci.task.task_id.clone(),
            job,
            status: status.to_string(),
            skipped,
            reason: ci.unrunnable.clone(),
        };
        match result {
            Some(result) => lines.extend(
                result
                    .outcomes
                    .into_iter()
                    .map(|o| line(Some(o.job), o.status.as_str(), o.skipped)),
            ),
            None => lines.push(line(None, "no_result", None)),
        }
    }
    Ok(lines)
}

/// One line per job, as `ci` prints it.
pub fn render_jobs(jobs: &[JobLine]) -> String {
    jobs.iter()
        .map(|line| {
            let name = line.job.as_deref().unwrap_or(&line.plan_task);
            match line.skipped.as_ref().or(line.reason.as_ref()) {
                Some(why) => format!("{name}: {} ({why})\n", line.status),
                None => format!("{name}: {}\n", line.status),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::execute::tests::{fixture, task_of};

    const WORKFLOW: &str = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo one\n      - run: echo two\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n";

    #[test]
    fn every_task_of_the_plan_is_a_spec_and_only_the_ones_that_need_a_job_wait() {
        let f = fixture(WORKFLOW);
        let digest = Digest::of_bytes(b"plan bytes");
        let manifest = Digest::of_bytes(b"manifest");
        let specs = task_specs(&f.plan, &digest, &manifest, &Default::default(), Some(7)).unwrap();
        assert_eq!(specs.len(), 2);
        let of = |job: &str| {
            let id = task_of(&f.plan, job).to_string();
            specs
                .iter()
                .find(|(task, _)| task == &id)
                .unwrap()
                .1
                .clone()
        };
        let build = of("build");
        let ci = build.ci.as_ref().unwrap();
        assert!(!ci.hold);
        assert_eq!(ci.steps, 2, "the commands the plan says the task starts");
        assert_eq!(ci.task.plan_digest, digest);
        assert!(ci.task.inputs.is_object());
        assert!(build.commands.is_empty());
        assert_eq!(build.plan_digest, digest);
        assert_eq!(build.manifest_digest, manifest);
        assert_eq!(build.command_timeout_ms, Some(7));
        assert!(!build.has_secret_bundle && build.setup.is_none() && !build.in_place);
        let test = of("test");
        let ci = test.ci.as_ref().unwrap();
        assert!(ci.hold);
        assert_eq!(ci.steps, 1);
    }

    #[test]
    fn a_plan_that_cannot_be_run_is_refused_before_anything_is_submitted() {
        let f = fixture(WORKFLOW);
        let d = Digest::of_bytes(b"d");
        let mut matrix = f.plan.clone();
        matrix.tasks[0].dynamic_matrix = true;
        let e = task_specs(&matrix, &d, &d, &Default::default(), None).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("cannot be run yet"), "{}", e.message);
        let mut empty = f.plan.clone();
        empty.tasks.clear();
        let e = task_specs(&empty, &d, &d, &Default::default(), None).unwrap_err();
        assert!(e.message.contains("no task"), "{}", e.message);
    }

    #[test]
    fn a_job_is_printed_with_its_status_and_why_it_did_not_run() {
        let line = |job: Option<&str>, status: &str, skipped: Option<&str>| JobLine {
            task_id: TaskId::generate(),
            plan_task: "ci.yml/deploy".into(),
            job: job.map(str::to_string),
            status: status.into(),
            skipped: skipped.map(str::to_string),
            reason: None,
        };
        let unrunnable = JobLine {
            reason: Some("the frozen plan is gone".into()),
            ..line(None, "no_result", None)
        };
        let text = render_jobs(&[
            line(Some("build"), "failure", None),
            line(Some("test"), "skipped", Some("needs failed")),
            line(None, "no_result", None),
            unrunnable,
        ]);
        assert_eq!(
            text,
            "build: failure\ntest: skipped (needs failed)\nci.yml/deploy: no_result\nci.yml/deploy: no_result (the frozen plan is gone)\n"
        );
        assert_eq!(render_jobs(&[]), "");
    }
}
