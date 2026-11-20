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
    /// Discovery is listed in the submit receipt but produces no public job line.
    #[serde(skip)]
    discovery: bool,
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
    /// The line is one shard of a split job, or the split job as its shards
    /// together (`plan_task` is then the job's id in the plan, and `task_id`
    /// its first shard).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part: Option<Part>,
    /// Why the job did not run, or what is wrong with its shards together.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    /// Why the task could not be run at all (it was cancelled before it was
    /// placed, with this reason). For a split job: what is wrong with its
    /// shards together (a failed or missing test, a shard without a result).
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
    /// The GitHub status of the run: what was computed and what became of
    /// the post, kept apart. A post that failed changes nothing else here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_status: Option<crate::ci::status::Record>,
}

fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, message)
}

/// What a line of a split job is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Part {
    Shard,
    Job,
}

/// A matrix variant reserves one slot shared by every worker running this job.
fn matrix_admission(
    plan: &local_ci::FrozenPlan,
    task: &local_ci::dag::ExecutionTask,
) -> Result<Option<crate::coordinator::MatrixAdmission>, ApiError> {
    use local_ci::dag::NodeKind;
    let Some(variant) = &task.variant else {
        return Ok(None);
    };
    let group = plan
        .graph
        .nodes
        .iter()
        .find(|node| node.id == variant.group)
        .ok_or_else(|| invalid(format!("missing matrix group for task {}", task.id)))?;
    let NodeKind::MatrixGroup {
        variants,
        max_parallel,
        ..
    } = &group.kind
    else {
        return Err(invalid(format!(
            "invalid matrix group for task {}",
            task.id
        )));
    };
    let limit = max_parallel.unwrap_or(variants.len());
    let max_parallel = u32::try_from(limit)
        .ok()
        .and_then(std::num::NonZeroU32::new)
        .ok_or_else(|| invalid(format!("invalid matrix parallelism for task {}", task.id)))?;
    Ok(Some(crate::coordinator::MatrixAdmission {
        group: group.id.clone(),
        max_parallel,
    }))
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
    if let Some(why) = crate::ci::advance::unsupported(plan) {
        return Err(invalid(format!("this plan cannot be run yet: {why}")));
    }
    if plan.tasks.is_empty() {
        return Err(invalid(
            "the plan has no task: no workflow has a job to run",
        ));
    }
    let local_plan = plan.to_plan();
    for workflow in 0..plan.workflows.len() {
        if let Some(reason) = local_ci::artifact_io::workflow_refusal(&local_plan, workflow) {
            return Err(invalid(reason));
        }
    }
    let distributed_matrix = plan
        .tasks
        .iter()
        .any(|t| t.variant.is_some() || t.dynamic_matrix);
    let no_inputs = serde_json::to_value(local_ci::TaskInputs::default())
        .map_err(|e| invalid(e.to_string()))?;
    plan.tasks
        .iter()
        .map(|task| {
            let demand = local_ci::plan_task_demand(plan, &task.id)
                .map_err(|e| invalid(format!("task `{}`: {e:#}", task.id)))?;
            let spec = TaskSpec {
                ci: Some(CiSpec {
                    history_keys: crate::ci::timing::keys(
                        &demand,
                        &crate::ci::timing::task_scope(plan, &task.id),
                    ),
                    dynamic_origin: task.dynamic_matrix,
                    core_outcome: None,
                    matrix: matrix_admission(plan, task)?,
                    shard_retry_forbidden: false,
                    shard_preferred_worker: None,
                    task: CiTask {
                        verify_shard_version: false,
                        artifact_workflow: plan
                            .workflows
                            .iter()
                            .find(|w| w.file == task.workflow)
                            .filter(|w| {
                                w.jobs
                                    .iter()
                                    .filter(|j| task.jobs.contains(&j.id))
                                    .flat_map(|j| &j.steps)
                                    .filter_map(|s| s.uses.as_deref())
                                    .any(|u| local_ci::artifact::parse_uses(u).is_some())
                            })
                            .map(|w| w.file.clone()),
                        dispatch: None,
                        plan_digest: plan_digest.clone(),
                        task_id: task.id.clone(),
                        inputs: no_inputs.clone(),
                    },
                    hold: distributed_matrix || !task.depends_on.is_empty(),
                    steps: u32::try_from(demand.nodes.len()).unwrap_or(u32::MAX),
                    unrunnable: None,
                    shard_group: crate::ci::shards::group_of(plan, task)
                        .map(|group| group.id.clone()),
                    joined: false,
                    join_failure: None,
                    rerun: false,
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
        let (plan, policies) = super::planning::freeze_with_policies(&store, &manifest)?;
        let (plan_digest, _) = crate::ci::advance::keep_plan(context.state_dir(), &plan)
            .map_err(|e| invalid(format!("cannot keep the frozen plan: {e}")))?;
        Ok((captured, plan, plan_digest, policies))
    })
    .await
    .map_err(|e| invalid(e.to_string()))?;
    let (captured, plan, plan_digest, policies) = frozen?;
    let source_id = captured.manifest.source_id.clone();
    let manifest_digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .map_err(|e| invalid(e.to_string()))?;
    let mut specs = task_specs(
        &plan,
        &plan_digest,
        &manifest_digest,
        &requirements,
        timeout,
    )?;
    let capacity = context.config.workers.len() + usize::from(context.config.local_worker.value);
    let capacity = capacity.saturating_mul(context.config.max_active_task.value as usize);
    if capacity > 1 {
        use std::os::unix::ffi::OsStrExt;
        let root = context.repo_root.as_deref().unwrap_or(&context.cwd);
        let project = captured
            .manifest
            .repository_id
            .clone()
            .unwrap_or_else(|| Digest::of_bytes(root.as_os_str().as_bytes()).to_string());
        let project = crate::ci::discovery::history_project(
            &project,
            &plan,
            &captured.manifest.object_manifest.entries,
        );
        crate::ci::discovery::stage(
            &plan,
            &mut specs,
            capacity,
            context.config.max_active_task.value,
            &project,
            &policies,
        );
    }
    let mut submitted = Vec::with_capacity(specs.len());
    let mut tasks = Vec::with_capacity(specs.len());
    for (plan_task, spec) in specs {
        let task_id = TaskId::generate();
        submitted.push(SubmittedTask {
            task_id: task_id.clone(),
            plan_task,
            waits: spec.ci.as_ref().is_some_and(|ci| ci.hold),
            discovery: spec
                .ci
                .as_ref()
                .is_some_and(|ci| crate::wire::is_discovery_task(&ci.task)),
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
    status: super::github_status::Options,
    poster: std::sync::Arc<dyn super::github_status::Poster>,
) -> Result<CiReport, ApiError> {
    let run_id = RunId::generate();
    let target = Target::Run(run_id.clone());
    let budget = Duration::from_secs(30);
    let submitted = submit(context.clone(), revision, run_id.clone());
    let (report, receipt) = tokio::select! {biased;
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
            (report, Some(receipt))
        }
    };
    let (spec, settled) = (context.bootstrap.clone(), context.clone());
    let end = Ended::of(&report);
    let expected = receipt.as_ref().map(|receipt| {
        let tasks = receipt.tasks.iter().filter(|task| !task.discovery);
        tasks.map(|task| task.plan_task.clone()).collect::<Vec<_>>()
    });
    // Decided only here, after the wait: on the parent, from the result of
    // the run and what it collected.
    let (executed_digest, jobs, github_status) = tokio::task::spawn_blocking(move || {
        let (executed_digest, jobs) = super::bootstrap::read_only_store(&spec)
            .ok()
            .and_then(|store| {
                job_report(
                    &store,
                    &spec.state_dir,
                    &spec.state_dir.join("results"),
                    &run_id,
                )
                .ok()
            })
            .unwrap_or_default();
        let record = expected.map(|expected| {
            let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
            let lines: Vec<_> = jobs.iter().map(JobLine::status_line).collect();
            let end = end.with(&expected, &lines);
            super::github_status::settle(&settled, &run_id, &end, &status, poster.as_ref())
        });
        (executed_digest, jobs, record.map(|record| (run_id, record)))
    })
    .await
    .unwrap_or_default();
    let github_status = match github_status {
        Some((run_id, record)) => {
            if let Err(why) = keep_status(&context, run_id, record.clone()).await {
                eprintln!("warning: the status of the run was not recorded: {why}");
            }
            Some(record)
        }
        None => None,
    };
    Ok(CiReport {
        report,
        plan_digest: executed_digest.or_else(|| receipt.map(|receipt| receipt.plan_digest)),
        jobs,
        github_status,
    })
}

/// Keep the status with the run, so that `show` says it later. The resident
/// owns the journal; an older one does not know the request and says so.
async fn keep_status(
    context: &Context,
    run_id: RunId,
    record: crate::ci::status::Record,
) -> Result<(), String> {
    let owner = context.owner().await.map_err(|e| e.to_string())?;
    let request = Operation::RecordStatus { run_id, record };
    match owner.request(request).await.map_err(|e| e.to_string())? {
        Reply::Accepted { .. } => Ok(()),
        Reply::Error { message } => Err(message),
        Reply::ControlError { error } => Err(error.message),
        _ => Err("unexpected reply".into()),
    }
}

/// How the run ended, as the status of the run reads it: the result of the
/// run that its completion made (`CoordinatorStore::completion`), which the
/// exit code of `ci` and `show` read too. Nothing is judged again here.
#[derive(Debug, Clone, Copy)]
pub struct Ended {
    settled: bool,
    cancel_requested: bool,
    collection_pending: Option<bool>,
    exit_code: i32,
    failed: Option<crate::ci::status::Cause>,
}

/// The failure of a run in the plain values the status decides from.
fn fault(failure: &super::completion::Failure) -> crate::ci::status::Fault {
    use crate::state::{Exit, Outcome};
    let end = failure.termination;
    crate::ci::status::Fault {
        exit_code: failure.exit_code,
        own_exit: end
            .and_then(super::completion::compute_failure)
            .map(|(_, exit)| exit),
        exited: matches!(end.and_then(|t| t.exit()), Some(Exit::Code(code)) if code != 0),
        cancelled: end.is_none_or(|t| t.outcome() == Outcome::Cancelled),
    }
}

impl Ended {
    pub fn of(report: &Report) -> Self {
        let snapshot = report.snapshot.as_ref();
        Self {
            settled: report.state == "settled",
            cancel_requested: report.cancel_requested
                || snapshot.is_some_and(|s| s.cancel_requested),
            collection_pending: report.collection_pending,
            exit_code: report.cli_exit_code,
            failed: snapshot
                .and_then(|s| s.first_compute_failure.as_ref())
                .map(|failure| crate::ci::status::cause(&fault(failure))),
        }
    }

    fn with<'a>(
        &self,
        expected: &'a [&'a str],
        jobs: &'a [crate::ci::status::Job<'a>],
    ) -> crate::ci::status::RunEnd<'a> {
        crate::ci::status::RunEnd {
            settled: self.settled,
            cancel_requested: self.cancel_requested,
            collection_pending: self.collection_pending,
            exit_code: self.exit_code,
            failed: self.failed,
            expected,
            jobs,
        }
    }

    /// What the run computed, with the tasks that were submitted and the
    /// lines of the run.
    pub fn computed(&self, expected: &[&str], jobs: &[JobLine]) -> crate::ci::status::Computed {
        let lines: Vec<_> = jobs.iter().map(JobLine::status_line).collect();
        crate::ci::status::computed(&self.with(expected, &lines))
    }
}

impl JobLine {
    /// The line as the status decision reads it.
    fn status_line(&self) -> crate::ci::status::Job<'_> {
        crate::ci::status::Job {
            plan_task: &self.plan_task,
            status: &self.status,
        }
    }
}

/// How the jobs of a run ended, read from what the parent collected. Reads
/// only; a task that left no result gets one line of its own.
pub fn jobs(spec: &super::bootstrap::Spec, run: &RunId) -> Result<Vec<JobLine>, String> {
    let store = super::bootstrap::read_only_store(spec).map_err(|e| e.to_string())?;
    job_lines(
        &store,
        &spec.state_dir,
        &spec.state_dir.join("results"),
        run,
    )
}

/// The lines of the split jobs of `plan`: each shard's lines are marked,
/// and a job whose shards all ended gets one line more, after them, with
/// the one outcome of the job (as the progression of the run makes it).
fn mark_split_jobs(
    lines: &mut Vec<JobLine>,
    plan: &local_ci::FrozenPlan,
    progress: &crate::ci::progress::NeedsProgression,
) {
    use crate::ci::progress::Progression;
    for group in plan.shard_groups.iter().filter(|g| g.split) {
        let mut last = None;
        for (at, line) in lines.iter_mut().enumerate() {
            if group.shards.contains(&line.plan_task) {
                line.part = Some(Part::Shard);
                last = Some(at);
            }
        }
        let job = group.source_jobs.join("+");
        let (Some(last), Some(outcome)) = (last, progress.outcome(&group.workflow, &job)) else {
            continue;
        };
        let first = lines.iter().find(|line| line.plan_task == group.shards[0]);
        let line = JobLine {
            task_id: first.unwrap_or(&lines[last]).task_id.clone(),
            plan_task: group.id.clone(),
            job: Some(job),
            status: outcome.status.as_str().to_string(),
            part: Some(Part::Job),
            // A job that failed says what is wrong with its shards.
            skipped: outcome
                .skipped
                .clone()
                .filter(|_| outcome.status.as_str() != "failure"),
            reason: progress.split_job_problems(&group.id).map(str::to_string),
        };
        lines.insert(last + 1, line);
    }
}

/// `state_dir` holds the frozen plan of the run. Without it (it is kept as
/// long as the results, a day at least) the lines of a split job are those
/// of its shards alone.
pub fn job_lines(
    store: &crate::storage::CoordinatorStore,
    state_dir: &std::path::Path,
    results: &std::path::Path,
    run: &RunId,
) -> Result<Vec<JobLine>, String> {
    job_report(store, state_dir, results, run).map(|(_, lines)| lines)
}

fn job_report(
    store: &crate::storage::CoordinatorStore,
    state_dir: &std::path::Path,
    results: &std::path::Path,
    run: &RunId,
) -> Result<(Option<Digest>, Vec<JobLine>), String> {
    let mut lines = Vec::new();
    let rows = store.ci_run_tasks(run).map_err(|e| e.to_string())?;
    let mut by_plan_task = std::collections::BTreeMap::new();
    let mut digest = crate::ci::discovery::active_plan(&rows)
        .map(Digest::parse)
        .transpose()
        .map_err(|e| e.to_string())?;
    for row in &rows {
        let Some(ci) = serde_json::from_value::<TaskSpec>(row.spec.clone())
            .ok()
            .and_then(|spec| spec.ci)
        else {
            continue;
        };
        if ci.task.inputs.get("rtr_discovery").is_some() {
            continue;
        }
        by_plan_task.insert(ci.task.task_id.clone(), row);
        digest.get_or_insert_with(|| ci.task.plan_digest.clone());
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
            part: None,
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
            None => lines.push(line(
                None,
                ci.core_outcome.as_deref().unwrap_or("no_result"),
                None,
            )),
        }
    }
    let plan = digest
        .as_ref()
        .and_then(|digest| crate::ci::advance::read_plan(state_dir, digest).ok())
        .and_then(|bytes| serde_json::from_slice::<local_ci::FrozenPlan>(&bytes).ok())
        .filter(|plan| plan.shard_groups.iter().any(|group| group.split));
    if let Some(plan) = plan {
        let row_of = |task: &str| by_plan_task.get(task).copied();
        if let Ok(progress) = crate::ci::advance::replay(store, results, &plan, row_of) {
            mark_split_jobs(&mut lines, &plan, &progress);
        }
    }
    Ok((digest, lines))
}

/// One line per job, as `ci` prints it.
pub fn render_jobs(jobs: &[JobLine]) -> String {
    jobs.iter()
        .map(|line| {
            // A shard is named as the task it is: its job is on a line
            // of its own.
            let job = line
                .job
                .as_deref()
                .filter(|_| line.part != Some(Part::Shard));
            let name = job.unwrap_or(&line.plan_task);
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
            part: None,
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
        // A shard is named as its task; the split job has its own line.
        let part = |part: Part, status: &str, why: Option<&str>| JobLine {
            part: Some(part),
            ..line(Some("test"), status, why)
        };
        let text = render_jobs(&[
            part(Part::Shard, "success", None),
            part(Part::Job, "failure", Some("missing: shard x")),
        ]);
        assert_eq!(
            text,
            "ci.yml/deploy: success\ntest: failure (missing: shard x)\n"
        );

        let shard = part(Part::Shard, "failure", None);
        let json = serde_json::to_value(&shard).unwrap();
        assert_eq!(json["part"], "shard");
        let json = serde_json::to_value(line(None, "no_result", None)).unwrap();
        assert!(json.get("part").is_none());
    }

    #[test]
    fn a_shard_is_queued_at_once_and_says_which_split_job_it_is_of() {
        let d = Digest::of_bytes(b"x");
        for fail_fast in [true, false] {
            let plan = crate::ci::shards::tests::split_plan(fail_fast);
            let group = crate::ci::shards::tests::group(&plan);
            let specs = task_specs(&plan, &d, &d, &Default::default(), None).unwrap();
            assert_eq!(specs.len(), 3);
            for (task, spec) in &specs {
                let ci = spec.ci.as_ref().unwrap();
                let shard = group.shards.contains(task);
                assert_eq!(ci.hold, !shard, "{task}");
                let of = shard.then(|| group.id.clone());
                assert_eq!(ci.shard_group, of, "{task} {fail_fast}");
                assert!(!ci.joined && ci.join_failure.is_none());
            }
        }
    }
}
