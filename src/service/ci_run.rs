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
    /// A shard whose split job has a line of its own: the status counts
    /// the job, not its shards too.
    #[serde(skip)]
    pub superseded: bool,
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

mod lines;
pub use lines::render_jobs;

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
            // What the setup steps of the task's jobs install is required of
            // its worker (spec 22.1).
            let mut requirements = requirements.clone();
            let setup = crate::ci::setup_tools::of_task(plan, task);
            requirements.requires.extend(setup.requires);
            requirements.requires.sort();
            requirements.requires.dedup();
            requirements
                .validate()
                .map_err(|e| invalid(format!("task `{}`: {e}", task.id)))?;
            let spec = TaskSpec {
                ci: Some(CiSpec {
                    history_keys: crate::ci::timing::keys(
                        &demand,
                        &crate::ci::timing::task_scope(plan, &task.id),
                    ),
                    duration_key: Some(crate::ci::timing::task_scope(plan, &task.id)),
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
                    unplaceable: None,
                    shard_group: crate::ci::shards::group_of(plan, task)
                        .map(|group| group.id.clone()),
                    joined: false,
                    join_failure: None,
                    rerun: false,
                }),
                toolchains: None,
                requirements,
                in_place: false,
                local: false,
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

/// The requirements of every task of a run: the configured ones, with the
/// tools the workspace's preparation runs (spec 22.1).
pub(crate) fn run_requirements(
    configured: crate::toolchain::capability::ExecutionRequirements,
    prepare: &crate::workspace::prepare_tools::PrepareTools,
) -> Result<crate::toolchain::capability::ExecutionRequirements, ApiError> {
    configured.with_prepare(&prepare.tools).map_err(invalid)
}

/// The notices of the setup tools of every task of `plan`, each once.
pub(crate) fn setup_notices(plan: &local_ci::FrozenPlan) -> Vec<String> {
    let mut all: Vec<String> = Vec::new();
    for task in &plan.tasks {
        for notice in crate::ci::setup_tools::of_task(plan, task).notices {
            if !all.contains(&notice) {
                all.push(notice);
            }
        }
    }
    all
}

/// The setup tool requirements of `requirements`: what a setup step of the
/// task's jobs installs or adds.
pub(crate) fn setup_requirements(
    requirements: &crate::toolchain::capability::ExecutionRequirements,
) -> crate::toolchain::capability::ExecutionRequirements {
    use crate::toolchain::capability::Requirement;
    crate::toolchain::capability::ExecutionRequirements {
        profile: None,
        requires: requirements
            .requires
            .iter()
            .filter(|r| matches!(r, Requirement::Setup(_) | Requirement::Component(_)))
            .cloned()
            .collect(),
    }
}

/// Asks the owner, once for each set of setup tools, whether some worker
/// has them; a task no worker serves is marked unplaceable with the reason.
async fn check_setup_tools(
    owner: &super::resident::Client,
    specs: &mut [(String, TaskSpec)],
) -> Result<(), ApiError> {
    let mut answers: Vec<(
        crate::toolchain::capability::ExecutionRequirements,
        Option<String>,
    )> = Vec::new();
    for (_, spec) in specs.iter_mut() {
        let setup = setup_requirements(&spec.requirements);
        if setup.is_empty() {
            continue;
        }
        let refused = match answers.iter().find(|(asked, _)| *asked == setup) {
            Some((_, refused)) => refused.clone(),
            None => {
                let refused = super::submit::ask_preparation(owner, &setup).await?;
                answers.push((setup, refused.clone()));
                refused
            }
        };
        if let (Some(why), Some(ci)) = (refused, spec.ci.as_mut()) {
            if ci.unplaceable.is_none() {
                ci.unplaceable = Some(why);
            }
        }
    }
    Ok(())
}

/// Every task of the run carries its own frozen toolchain selection:
/// `toolchains` is in the order of the plan's tasks, as `specs` is.
pub(crate) fn with_toolchains(
    specs: &mut [(String, TaskSpec)],
    toolchains: Vec<super::submit::Selection>,
) {
    for ((_, spec), selection) in specs.iter_mut().zip(toolchains) {
        match selection {
            Ok(bundle) => spec.toolchains = bundle,
            // No requirement is not what the task requires: it keeps none,
            // carries the refusal, and is never placed (spec 22.1).
            Err(refused) => {
                spec.toolchains = None;
                if let Some(ci) = spec.ci.as_mut() {
                    ci.unplaceable = Some(refused.message);
                }
            }
        }
    }
}

pub async fn submit(
    context: Context,
    revision: Option<String>,
    run_id: RunId,
) -> Result<Receipt, ApiError> {
    let (state, run) = (context.state_dir().to_owned(), run_id.clone());
    let result = submit_run(context, revision, run_id).await;
    if let Err(error) = &result {
        // A submission that failed or was refused will not be sent again.
        super::input::give_up_after(state, run, error).await;
    }
    result
}

async fn submit_run(
    context: Context,
    revision: Option<String>,
    run_id: RunId,
) -> Result<Receipt, ApiError> {
    let mut requirements = crate::toolchain::capability::ExecutionRequirements::parse(
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
    super::input::capture_begins(&run_id);
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
        let (store, guard) = crate::source::SourceStore::open_foreground(context.state_dir())
            .map_err(|error| {
                if error.is::<crate::source::retention::ForegroundBusy>() {
                    crate::source::classify::api_error_of(&error)
                } else {
                    invalid(error.to_string())
                }
            })?;
        let prepare = crate::toolchain::snapshot::read_captured_prepare_tools(&store, &manifest)
            .map_err(|e| invalid(format!("cannot read how the workspace is prepared: {e}")))?;
        let (plan, policies) = super::planning::freeze_with_policies(&store, &manifest)?;
        let (plan_digest, _) = crate::ci::advance::keep_plan(context.state_dir(), &plan)
            .map_err(|e| invalid(format!("cannot keep the frozen plan: {e}")))?;
        Ok((captured, plan, plan_digest, policies, guard, prepare))
    })
    .await
    .map_err(|e| invalid(e.to_string()))?;
    let (captured, plan, plan_digest, policies, guard, prepare) = frozen?;
    // Every task's workspace is prepared alike: the tools it runs are
    // required of the worker of every task (spec 22.1).
    requirements = run_requirements(requirements, &prepare)?;
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
    // Frozen once for the run: the declarations a plain command of this
    // source would freeze, and for each task the `setup-*` inputs of its own
    // jobs. Every task carries its bundle, and the worker activates it
    // before the job's steps.
    let (toolchains, notices) = super::submit::freeze_resolved_tasks(
        &context,
        &owner,
        &captured.manifest,
        &manifest_digest,
        &requirements,
        &plan,
        guard,
    )
    .await?;
    with_toolchains(&mut specs, toolchains);
    // A task whose setup steps install what no worker has is never placed,
    // with the reason (spec 22.1); the other tasks of the run still run.
    check_setup_tools(&owner, &mut specs).await?;
    let notices: Vec<String> = notices.into_iter().chain(setup_notices(&plan)).collect();
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
            // What the capture warned of, then what the freeze left out.
            warnings: captured.warnings.into_iter().chain(notices).collect(),
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
            (control::stop_submission(&context, &run_id, signals, budget).await, None)
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
        let (executed_digest, jobs, _) = super::bootstrap::read_only_store(&spec)
            .ok()
            .and_then(|store| {
                job_report(
                    &store,
                    &spec.state_dir,
                    &spec.state_dir.join("results"),
                    &run_id,
                    false,
                )
                .ok()
            })
            .unwrap_or_default();
        let record = expected.map(|expected| {
            let verdict = verdict_of(&settled, &run_id, end, &expected, &jobs, &status);
            let posting =
                crate::ci::status::outcome(verdict.decision.clone(), |post| poster.post(post));
            verdict.record(posting)
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
pub(super) async fn keep_status(
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

    /// How a run that has not been waited for stands, from what the parent
    /// has recorded of it. `settled` is whether its completion has an exit
    /// code, as `wait` reads it.
    pub fn of_snapshot(snapshot: &super::completion::Snapshot) -> Self {
        Self {
            settled: snapshot.exit_code().is_some(),
            cancel_requested: snapshot.cancel_requested,
            collection_pending: Some(snapshot.collection_pending_tasks > 0),
            exit_code: snapshot.exit_code().unwrap_or(0),
            failed: snapshot
                .first_compute_failure
                .as_ref()
                .map(|failure| crate::ci::status::cause(&fault(failure))),
        }
    }

    pub(super) fn with<'a>(
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

/// What the status of the run is and whether it may be posted: the one
/// decision the automatic post after `ci` and a post again share.
pub(super) fn verdict_of(
    context: &Context,
    run: &RunId,
    end: Ended,
    expected: &[String],
    jobs: &[JobLine],
    options: &super::github_status::Options,
) -> super::github_status::Verdict {
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    let lines: Vec<_> = jobs.iter().map(JobLine::status_line).collect();
    let end = end.with(&expected, &lines);
    super::github_status::verdict(context, run, &end, options)
}

/// The tasks of the run now that are not discovery tasks.
fn row_tasks(store: &crate::storage::CoordinatorStore, run: &RunId) -> Result<Vec<String>, String> {
    let rows = store.ci_run_tasks(run).map_err(|e| e.to_string())?;
    let specs = rows.iter().filter_map(|row| {
        serde_json::from_value::<TaskSpec>(row.spec.clone())
            .ok()?
            .ci
    });
    Ok(specs
        .filter(|ci| !crate::wire::is_discovery_task(&ci.task))
        .map(|ci| ci.task.task_id)
        .collect())
}

impl JobLine {
    #[cfg(test)]
    fn clone_line(&self) -> JobLine {
        JobLine {
            task_id: self.task_id.clone(),
            plan_task: self.plan_task.clone(),
            job: self.job.clone(),
            status: self.status.clone(),
            part: self.part,
            skipped: self.skipped.clone(),
            reason: self.reason.clone(),
            superseded: self.superseded,
        }
    }

    /// The line as the status decision reads it.
    pub(super) fn status_line(&self) -> crate::ci::status::Job<'_> {
        crate::ci::status::Job {
            plan_task: &self.plan_task,
            status: &self.status,
            counted: !self.superseded,
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

/// What a post again decides from: the tasks the submit receipt named and
/// the lines of the run. A result or the plan that the run recorded and that
/// cannot be read, a join of a split job that cannot be replayed, and a split
/// job without a joined result are errors: posting `error` over the success
/// GitHub holds because a file is gone, or `success` when the join is missing,
/// would not be what the automatic post said.
pub fn strict_report(
    store: &crate::storage::CoordinatorStore,
    state_dir: &std::path::Path,
    results: &std::path::Path,
    run: &RunId,
) -> Result<(Vec<String>, Vec<JobLine>), String> {
    let (_, lines, split) = job_report(store, state_dir, results, run, true)?;
    Ok((expected_of(row_tasks(store, run)?, &split), lines))
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
    job_report(store, state_dir, results, run, false).map(|(_, lines, _)| lines)
}

/// The result a finished attempt kept, if it kept one. `Err` when it did and
/// the file cannot be read.
fn row_result(
    store: &crate::storage::CoordinatorStore,
    results: &std::path::Path,
    attempt: &crate::ids::AttemptId,
) -> Result<Option<local_ci::WireTaskResult>, String> {
    let kept = store
        .persisted_artifacts(attempt)
        .map_err(|e| e.to_string())?;
    let Some((_, digest)) = kept
        .iter()
        .find(|(name, _)| name == crate::supervisor::ci_task::RESULT_ARTIFACT)
    else {
        return Ok(None);
    };
    crate::ci::advance::read_result(results, attempt, digest).map(Some)
}

/// `strict`: a result or the plan that the run recorded but that cannot be
/// read now (kept "a day at least") is an error, not a task without a result.
fn job_report(
    store: &crate::storage::CoordinatorStore,
    state_dir: &std::path::Path,
    results: &std::path::Path,
    run: &RunId,
    strict: bool,
) -> Result<JobReport, String> {
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
        if !lines::has_lines(&ci.task.inputs) {
            continue;
        }
        by_plan_task.insert(ci.task.task_id.clone(), row);
        digest.get_or_insert_with(|| ci.task.plan_digest.clone());
        let result = match row.attempt.as_ref() {
            None => None,
            Some(attempt) => match row_result(store, results, &attempt.attempt_id) {
                Ok(result) => result,
                Err(why) if strict => {
                    return Err(format!(
                        "the result of task {} cannot be read: {why}",
                        row.task_id
                    ))
                }
                Err(_) => None,
            },
        };
        // A task that left no result says why its attempt ended, when its
        // worker told (the preparation failed).
        let unrunnable = ci
            .unrunnable
            .clone()
            .or_else(|| match (&result, &row.attempt) {
                (None, Some(attempt)) => store
                    .events_of(&attempt.attempt_id, crate::ci::FAILURE_REASON_EVENT)
                    .ok()
                    .and_then(|events| {
                        events
                            .last()
                            .and_then(|e| e.payload.get("reason")?.as_str().map(str::to_string))
                    }),
                _ => None,
            })
            // A task whose collection closed as `required_missing` says
            // which required output is missing, and why.
            .or_else(|| {
                let attempt = row.attempt.as_ref()?;
                store
                    .events_of(&attempt.attempt_id, crate::ci::COLLECTION_MISSING_EVENT)
                    .ok()?
                    .last()?
                    .payload
                    .get("reason")?
                    .as_str()
                    .map(str::to_string)
            });
        lines.extend(lines::row_lines(
            &row.task_id,
            &ci.task.task_id,
            unrunnable.as_deref(),
            // A task no worker could be placed for failed (spec 22.1).
            ci.core_outcome
                .as_deref()
                .or(ci.unplaceable.as_ref().map(|_| "failure")),
            result,
        ));
    }
    let read = digest.as_ref().map(|digest| {
        let bytes = crate::ci::advance::plan_bytes(state_dir, digest)?;
        serde_json::from_slice::<local_ci::FrozenPlan>(&bytes).map_err(|e| e.to_string())
    });
    let plan = match read {
        Some(Err(why)) if strict => {
            return Err(format!("the frozen plan of the run cannot be read: {why}"))
        }
        read => read.and_then(Result::ok),
    }
    .filter(lines::has_split_job);
    let mut split = Vec::new();
    if let Some(plan) = plan {
        let row_of = |task: &str| by_plan_task.get(task).copied();
        match crate::ci::advance::replay(store, results, &plan, row_of) {
            Ok(progress) => lines::mark_split_jobs(&mut lines, &plan, &progress),
            Err(why) if strict => {
                return Err(format!(
                    "the join of the split jobs cannot be replayed: {}",
                    why.reason()
                ))
            }
            Err(_) => {}
        }
        split = plan
            .shard_groups
            .iter()
            .filter(|group| group.split)
            .map(|group| (group.id.clone(), group.shards.clone()))
            .collect();
        if let Some(job) = strict.then(|| unjoined(&lines, &split)).flatten() {
            return Err(format!("the split job {job} has no joined result"));
        }
    }
    Ok((digest, lines, split))
}

/// A split job: its id (the task of the plan it was submitted as) and its
/// shards.
type SplitGroup = (String, Vec<String>);

/// The digest of the plan the lines are of, the lines, and the split jobs.
type JobReport = (Option<Digest>, Vec<JobLine>, Vec<SplitGroup>);

/// The first split job whose join left no line of its own. The automatic
/// post needs that line (the job was submitted as one task); without it the
/// status cannot be told from what the shards say.
fn unjoined(lines: &[JobLine], split: &[SplitGroup]) -> Option<String> {
    split
        .iter()
        .find(|(id, _)| {
            !lines
                .iter()
                .any(|line| &line.plan_task == id && line.part == Some(Part::Job))
        })
        .map(|(id, _)| id.clone())
}

/// The tasks the submit receipt named, from the tasks of the run now: a
/// shard that discovery made of a task stands for the task it was made of,
/// which is the id of its split job.
fn expected_of(tasks: Vec<String>, split: &[SplitGroup]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for task in tasks {
        let task = split
            .iter()
            .find(|(_, shards)| shards.contains(&task))
            .map_or(task, |(id, _)| id.clone());
        if !out.contains(&task) {
            out.push(task);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::execute::tests::{fixture, task_of};

    /// run5: the coverage job's setup steps are its requirements (spec 22.1).
    #[test]
    fn the_coverage_job_requires_its_setup_tools_and_the_other_job_does_not() {
        use crate::toolchain::capability::*;
        const COVERAGE: &str = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n  coverage:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: dtolnay/rust-toolchain@stable\n        with:\n          components: llvm-tools-preview\n      - uses: taiki-e/install-action@cargo-llvm-cov\n      - run: echo cover\n      - uses: taiki-e/install-action@v2\n        with:\n          tool: ${{ env.MORE }}\n";
        let f = fixture(COVERAGE);
        let digest = Digest::of_bytes(b"plan bytes");
        let specs = task_specs(&f.plan, &digest, &digest, &Default::default(), None).unwrap();
        let of = |job: &str| {
            let (_, spec) = specs.iter().find(|(id, _)| id.contains(job)).unwrap();
            spec.requirements.clone()
        };
        let coverage = of("coverage");
        assert_eq!(
            coverage.requires,
            vec![
                Requirement::Setup("cargo-llvm-cov".into()),
                Requirement::Component("llvm-tools".into()),
            ]
        );
        assert!(of("test").requires.is_empty());
        assert_eq!(setup_requirements(&coverage), coverage);
        let notices = setup_notices(&f.plan);
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains("${{ env.MORE }}"), "{notices:?}");
        // A worker without the tool (mini, run5) is no candidate.
        let worker = |tools: &[&str], components: &[&str]| WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            cpu_cores: None,
            memory_bytes: None,
            schema_version: 1,
            os: "macos".into(),
            arch: "aarch64".into(),
            profile: "default".into(),
            profile_digest: Digest::of_bytes(b"definition"),
            execution_uid: 1,
            protocol_versions: vec![1],
            profile_fingerprint: Digest::of_bytes(b"p"),
            tools: Default::default(),
            features: [FEATURE.into(), SETUP_TOOLS_FEATURE.into()].into(),
            rustup: vec![DetectedRustup {
                name: "stable-aarch64-apple-darwin".into(),
                version: "1.100.0".into(),
                bin: "/r/bin".into(),
                rustc: Digest::of_bytes(b"r"),
                cargo: Digest::of_bytes(b"c"),
                components: components.iter().map(|c| c.to_string()).collect(),
            }],
            installs: Vec::new(),
            path_tools: tools.iter().map(|t| t.to_string()).collect(),
        };
        let mini = worker(&["cargo-schemata"], &["llvm-tools"]);
        let unmet = missing_with_installable(&coverage, Some(&mini), None);
        assert_eq!(unmet.len(), 1, "{unmet:?}");
        assert!(
            unmet[0].starts_with("cargo-llvm-cov unavailable"),
            "{unmet:?}"
        );
        let studio = worker(&["cargo-llvm-cov"], &["llvm-tools"]);
        assert!(missing_with_installable(&coverage, Some(&studio), None).is_empty());
        assert!(missing_with_installable(&of("test"), Some(&mini), None).is_empty());
    }

    /// A Rust-only job of a source whose tracked npm example the workspace
    /// preparation installs runs `node -v`: without a node selection a
    /// worker without node is no candidate (run4 of the acceptance), and
    /// with one the activated node serves preparation too (spec 22.1).
    #[test]
    fn a_worker_without_node_is_a_candidate_only_when_the_task_selects_node() {
        use crate::toolchain::capability::{missing_with_installable, ToolObservation};
        use crate::toolchain::versions::*;
        let f = fixture(WORKFLOW);
        let paths = ["Cargo.lock", "examples/sharding/node/package-lock.json"];
        let prepare = crate::workspace::prepare_tools::prepare_tools(paths, None).unwrap();
        let requirements = Default::default();
        let requirements = run_requirements(requirements, &prepare).unwrap();
        let digest = Digest::of_bytes(b"plan bytes");
        let specs = task_specs(&f.plan, &digest, &digest, &requirements, None).unwrap();
        let scope = Scope::project("");
        let mut node = crate::toolchain::contract::FrozenBundle::seal_exact(
            FrozenToolchains::freeze(
                Digest::of_bytes(b"source"),
                Digest::of_bytes(b"parent"),
                scope.clone(),
                &[Declaration {
                    tool: ToolId::parse("node").unwrap(),
                    constraint: Constraint::parse("20.0.0").unwrap(),
                    scope,
                    location: Location {
                        path: ".node-version".into(),
                        first_line: 1,
                        last_line: 1,
                    },
                }],
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            )
            .unwrap(),
        )
        .unwrap();
        // The bundle provides node for the job's steps.
        node.frozen.selections.push(ResolvedSelection {
            tool: ToolId::parse("node").unwrap(),
            concrete: "20.0.0".parse().unwrap(),
            requirement_digest: Digest::of_bytes(b"requirement"),
            authority: Digest::of_bytes(b"authority"),
        });
        let worker = |tools: &[&str]| crate::toolchain::capability::WorkerCapabilities {
            disk_free_bytes: None,
            cpu_cores: None,
            memory_bytes: None,
            resource_budget: None,
            schema_version: 1,
            os: "macos".into(),
            arch: "aarch64".into(),
            profile: "default".into(),
            profile_digest: Digest::of_bytes(b"definition"),
            execution_uid: 1,
            protocol_versions: vec![1],
            profile_fingerprint: Digest::of_bytes(b"p"),
            tools: tools
                .iter()
                .map(|t| {
                    let seen = ToolObservation::Available {
                        version: "1.0.0".into(),
                        identity: Digest::of_bytes(t.as_bytes()),
                    };
                    (t.to_string(), seen)
                })
                .collect(),
            features: [crate::toolchain::capability::FEATURE.into()].into(),
            rustup: Vec::new(),
            installs: Vec::new(),
            path_tools: Vec::new(),
        };
        let (mini, studio) = (
            worker(&["cargo", "rustc"]),
            worker(&["cargo", "node", "npm"]),
        );
        for (_, spec) in &specs {
            let unmet = missing_with_installable(&spec.requirements, Some(&mini), Some(&node));
            assert!(unmet.is_empty(), "the selected node prepares: {unmet:?}");
            for bundle in [None, Some(&node)] {
                if bundle.is_none() {
                    let unmet = missing_with_installable(&spec.requirements, Some(&mini), None);
                    let node = unmet.iter().any(|u| u.contains("\"node\""));
                    assert!(node, "mini cannot run `node -v`: {unmet:?}");
                }
                assert!(
                    missing_with_installable(&spec.requirements, Some(&studio), bundle).is_empty()
                );
            }
        }
    }

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
    fn every_task_of_a_run_carries_the_one_frozen_toolchain_or_none() {
        use crate::toolchain::{
            contract::FrozenBundle,
            versions::{
                Constraint, Declaration, FrozenToolchains, Location, MatchPolicy, Scope, ToolId,
            },
        };
        let f = fixture(WORKFLOW);
        let digest = Digest::of_bytes(b"plan bytes");
        let manifest = Digest::of_bytes(b"manifest");
        let mut specs = task_specs(&f.plan, &digest, &manifest, &Default::default(), None).unwrap();
        assert!(specs.iter().all(|(_, spec)| spec.toolchains.is_none()));
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            manifest.clone(),
            Digest::of_bytes(b"capture"),
            scope.clone(),
            &[Declaration {
                tool: ToolId::parse("node").unwrap(),
                constraint: Constraint::parse("20.0.0").unwrap(),
                scope,
                location: Location {
                    path: ".node-version".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &Default::default(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let bundle = FrozenBundle::seal_exact(frozen).unwrap();
        // Each task takes the bundle frozen for it, in plan order.
        with_toolchains(&mut specs, vec![Ok(None), Ok(Some(bundle.clone()))]);
        assert_eq!(specs.len(), 2);
        assert!(specs[0].1.toolchains.is_none());
        assert_eq!(specs[1].1.toolchains.as_ref(), Some(&bundle));
        // The bundle is bound to the same source the task is.
        bundle
            .validate_partial(&specs[1].1.manifest_digest)
            .unwrap();
        with_toolchains(&mut specs, vec![Ok(Some(bundle.clone())), Ok(None)]);
        assert_eq!(specs[0].1.toolchains.as_ref(), Some(&bundle));
        assert!(specs[1].1.toolchains.is_none());
        assert!(specs[1].1.ci.as_ref().unwrap().unplaceable.is_none());
        // A task no worker resolves keeps no bundle and carries the refusal
        // (spec 22.1); the other task keeps its bundle.
        let refused = ApiError::blocked(
            crate::error::BlockedReason::EnvironmentMismatch,
            "toolchain resolve required: no coherent worker proposal (rust=1.89)",
        );
        with_toolchains(&mut specs, vec![Ok(Some(bundle.clone())), Err(refused)]);
        assert_eq!(specs[0].1.toolchains.as_ref(), Some(&bundle));
        assert!(specs[0].1.ci.as_ref().unwrap().unplaceable.is_none());
        assert!(specs[1].1.toolchains.is_none());
        assert_eq!(
            specs[1].1.ci.as_ref().unwrap().unplaceable.as_deref(),
            Some("toolchain resolve required: no coherent worker proposal (rust=1.89)")
        );
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
    fn the_status_counts_a_split_job_once() {
        use crate::ci::progress::{Input, NeedsProgression, Progression};
        let plan = crate::ci::shards::tests::split_plan(false);
        let group = crate::ci::shards::tests::group(&plan).clone();
        let mut progress = NeedsProgression::new(&plan).unwrap();
        let mut lines = Vec::new();
        for shard in &group.shards {
            progress
                .step(Input::Fence {
                    task: shard.clone(),
                    status: local_ci::task::JobStatus::Failure,
                    reason: "failed".into(),
                })
                .unwrap();
            lines.push(JobLine {
                task_id: TaskId::generate(),
                plan_task: shard.clone(),
                job: Some("test".into()),
                status: "failure".into(),
                part: None,
                skipped: None,
                reason: None,
                superseded: false,
            });
        }
        lines::mark_split_jobs(&mut lines, &plan, &progress);
        assert_eq!(lines.len(), group.shards.len() + 1);
        let status: Vec<_> = lines.iter().map(JobLine::status_line).collect();
        // Every task of the plan is still covered by a line, and the one
        // split job is one failure in the description.
        assert_eq!(
            crate::ci::status::description(crate::ci::status::Computed::Failure, &status),
            "failure: pass 0 / fail 1 / error 0 / skip 0"
        );
    }

    /// A split job whose shards all succeeded, with or without the join the
    /// automatic post sees: the post again decides the same, or refuses.
    #[test]
    fn a_split_job_is_expected_as_the_task_it_was_submitted_as_and_needs_its_join() {
        use crate::ci::progress::{Input, NeedsProgression, Progression};
        use crate::ci::status::{computed, Computed, RunEnd};
        let plan = crate::ci::shards::tests::split_plan(false);
        let group = crate::ci::shards::tests::group(&plan).clone();
        let split = vec![(group.id.clone(), group.shards.clone())];
        // The shards are the tasks of the run now; the receipt named the job.
        let mut tasks = group.shards.clone();
        tasks.insert(1, "other".into());
        tasks.push(group.shards[0].clone());
        let expected = expected_of(tasks, &split);
        assert_eq!(expected, [group.id.clone(), "other".to_string()]);
        let mut progress = NeedsProgression::new(&plan).unwrap();
        let mut lines = Vec::new();
        for shard in &group.shards {
            progress
                .step(Input::Fence {
                    task: shard.clone(),
                    status: local_ci::task::JobStatus::Success,
                    reason: "ok".into(),
                })
                .unwrap();
            lines.push(JobLine {
                task_id: TaskId::generate(),
                plan_task: shard.clone(),
                job: Some("test".into()),
                status: "success".into(),
                part: None,
                skipped: None,
                reason: None,
                superseded: false,
            });
        }
        lines.push(JobLine {
            plan_task: "other".into(),
            ..lines[0].clone_line()
        });
        let end = |lines: &[JobLine]| {
            let status: Vec<_> = lines.iter().map(JobLine::status_line).collect();
            let names: Vec<&str> = expected.iter().map(String::as_str).collect();
            computed(&RunEnd {
                settled: true,
                cancel_requested: false,
                collection_pending: Some(false),
                exit_code: 0,
                failed: None,
                expected: &names,
                jobs: &status,
            })
        };
        // The join is missing: the automatic post says error, and the post
        // again refuses instead of saying success from the shards.
        assert_eq!(unjoined(&lines, &split), Some(group.id.clone()));
        assert_eq!(end(&lines), Computed::Error);
        lines::mark_split_jobs(&mut lines, &plan, &progress);
        assert_eq!(unjoined(&lines, &split), None);
        // The join of these shards (no test results) is a failure: that line
        // of the job, not the successes of the shards, is what both say.
        let job = lines.iter().find(|line| line.part == Some(Part::Job));
        assert_eq!(job.map(|line| line.status.as_str()), Some("failure"));
        assert_eq!(end(&lines), Computed::Failure);
        assert_eq!(unjoined(&[], &[]), None);
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
