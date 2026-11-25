//! An attempt that is one task of a frozen CI plan (spec chapter 7).
//!
//! The core of the plan decides which steps run and in which order; every
//! step it wants started comes here as a request and is run as a node of the
//! attempt, by the same code that runs a command of `exec`: registered and
//! admitted by task-traffic-controller, claimed in the database, its output
//! masked into the spool, stopped and confirmed stopped by the supervisor.
//! The core starts no process of a step and signals none: its cancel handle
//! is never given a process group, so cancelling it only makes the core
//! stop (no further step, no further helper process of its own).
//!
//! While the core works outside a step (verifying the workspace, between
//! two steps) the supervisor keeps looking, every `poll_ms`, at the cancel
//! request, the lease and the deadline of the attempt, exactly as while a
//! command runs: the lease is renewed all along, and a reason to stop
//! cancels the core at once instead of waiting for its next step.
//!
//! What ends the attempt is a cancel, an expired lease, the deadline of the
//! attempt, a timeout while waiting for resources, a spool that failed and
//! a stop that could not be confirmed. After any of them no further step is
//! started, whatever the plan would still run (`if: always()` included).
//! A step that ran into its own time limit (`timeout-minutes` of the step
//! or of its job, never more than the `command_timeout_ms` of the attempt)
//! is stopped and confirmed stopped like any command, and is then only a
//! result of that step: the core decides what follows (`continue-on-error`,
//! `if: failure()`, `if: always()`).

use super::error::{Result, SupervisorError};
use super::run::{Control, NodeEnd, NodeTimes};
use super::spec::NodeSpec;
use super::termination::StopReason;
use crate::ci::execute::{execute_selected, SelectedTask, TaskPaths};
use crate::ids::{Digest, NodeId};
use crate::source::SourceStore;
use crate::state::{Exit, TimeoutKind};
use local_ci::exec::Exit as StepEnd;
use local_ci::{
    CancelHandle, FrozenPlan, JobStatus, StepExecutor, StepExit, StepRequest, TaskInputs,
    WireTaskResult,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The name the task's result is kept under, as an artifact of the attempt:
/// the parent reads the outcomes and outputs of the task's jobs from it.
pub const RESULT_ARTIFACT: &str = "belt-ci-task-result.json";
/// The frozen plan is JSON of at most this many bytes.
pub const MAX_PLAN_BYTES: u64 = 64 << 20;
/// The inputs of a task (the outcomes of the jobs it needs) are JSON of at
/// most this many bytes. A task whose own outcomes are larger than that is
/// failed where it ran: no task that needs it could be given them.
pub const MAX_INPUT_BYTES: usize = 4 << 20;
const RESULT_FILE: &str = "result.json";

/// What the agent hands the supervisor for a CI task. `Debug` shows the
/// names of `env` only.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CiTaskSpec {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub verify_shard_version: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_workflow: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_scope: Option<Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<serde_json::Value>,
    /// The bytes of the frozen plan. Used only when they hash to
    /// `plan_digest`.
    pub plan_path: PathBuf,
    pub plan_digest: Digest,
    pub task_id: String,
    /// The core's `TaskInputs`, as the parent sent them.
    pub inputs: serde_json::Value,
    /// The source of the attempt, as the worker's source store names it.
    pub ci_source_id: String,
    /// The attempt's workspace root: the captured source, plus only what
    /// preparation wrote at `prepared`.
    pub workspace: PathBuf,
    /// What `fast-worktree add` wrote into the workspace (relative paths,
    /// see `Workspace::prepared`): left out of the check against the
    /// captured source, and nothing else is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prepared: Vec<String>,
    /// The attempt's own directory for step logs, scripts and the result.
    pub scratch: PathBuf,
    /// The environment every step starts from.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl std::fmt::Debug for CiTaskSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CiTaskSpec")
            .field("plan_path", &self.plan_path)
            .field("plan_digest", &self.plan_digest)
            .field("task_id", &self.task_id)
            .field("ci_source_id", &self.ci_source_id)
            .field("workspace", &self.workspace)
            .field("scratch", &self.scratch)
            .field("env", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CiTaskSpec {
    pub fn validate(&self) -> std::result::Result<(), &'static str> {
        if self.task_id.is_empty() {
            return Err("a CI task without a task id");
        }
        if [&self.plan_path, &self.workspace, &self.scratch]
            .iter()
            .any(|p| !p.is_absolute())
        {
            return Err("a CI task path is not absolute");
        }
        Ok(())
    }

    /// Where the task's result is written before it is kept as
    /// [`RESULT_ARTIFACT`].
    pub fn result_path(&self) -> PathBuf {
        self.scratch.join(RESULT_FILE)
    }

    /// The result file, relative to [`CiTaskSpec::scratch`].
    pub fn result_source(&self) -> &'static Path {
        Path::new(RESULT_FILE)
    }

    fn paths(&self, cancel: CancelHandle) -> TaskPaths {
        TaskPaths {
            root: self.workspace.clone(),
            prepared: self.prepared.clone(),
            run_dir: self.scratch.join("run"),
            tools: self.scratch.join("tools"),
            lock_file: self.scratch.join("lock"),
            trash: self.scratch.join("trash"),
            env: self.env.clone(),
            cancel,
        }
    }
}

/// How the task ended, for the attempt's termination.
pub(super) struct CiEnd {
    /// The stop reason and confirmation of the step that ended the attempt,
    /// or, without one, the task's own result as an exit: 0 when no job of
    /// it failed.
    pub end: NodeEnd,
    pub last_node: String,
    /// The task could not be run to a result (and nothing stopped it): the
    /// attempt failed without an exit status.
    pub failure: Option<String>,
}

/// The exit a finished task stands for: jobs that were skipped by their own
/// condition do not fail it.
pub(crate) fn exit_of_outcomes(statuses: impl IntoIterator<Item = JobStatus>) -> Exit {
    let failed = statuses
        .into_iter()
        .any(|s| matches!(s, JobStatus::Failure | JobStatus::Cancelled));
    Exit::Code(i32::from(failed))
}

/// The node a step is run as: the steps of a task are numbered in the order
/// the core asks for them, and that number is the node's index (its command
/// id for task-traffic-controller is made of it).
pub(crate) fn step_node_name(index: usize) -> String {
    format!("step-{index}")
}

/// The index of a node made by [`step_node_name`]; `None` for any other
/// name. Recovery finds the command of a launched step by it.
pub(crate) fn step_index(node: &str) -> Option<usize> {
    let index: usize = node.strip_prefix("step-")?.parse().ok()?;
    (step_node_name(index) == node).then_some(index)
}

/// What the core is told about a step the supervisor ran or refused.
pub(crate) fn step_reply(end: &NodeEnd) -> std::result::Result<StepEnd, String> {
    match (end.reason, end.exit) {
        (Some(StopReason::Timeout(_)), _) => Ok(StepEnd::Timeout),
        (Some(_), _) => Ok(StepEnd::Cancelled),
        (None, _) if end.unconfirmed => Ok(StepEnd::Cancelled),
        (None, Some(Exit::Code(code))) => Ok(StepEnd::Code(code)),
        (None, Some(Exit::Signal(signal))) => Ok(StepEnd::Code(128 + signal)),
        (None, None) => Err("the command of the step could not be started".into()),
    }
}

/// Does this end of a step end the attempt? Every stop does, and a stop
/// that could not be confirmed, but one: the step ran into its own time
/// limit. That is a result of the step, and the core decides what follows.
pub(crate) fn ends_attempt(end: &NodeEnd) -> bool {
    let own_limit = matches!(
        end.reason,
        Some(StopReason::Timeout(t)) if t.kind == TimeoutKind::Command
    );
    end.unconfirmed || (end.reason.is_some() && !own_limit)
}

/// The run-time limit of a step: what the plan gives it, never more than
/// the limit the attempt has for a command.
pub(crate) fn step_timeout_ms(step: Option<Duration>, cap_ms: Option<u64>) -> Option<u64> {
    let own = step.map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX).max(1));
    match (own, cap_ms) {
        (Some(own), Some(cap)) => Some(own.min(cap)),
        (own, cap) => own.or(cap),
    }
}

/// What the core is told about the time of a step: the time its command
/// ran, and apart from it the time it waited to be admitted (which the core
/// does not count against `timeout-minutes`).
pub(crate) fn step_exit(exit: StepEnd, times: NodeTimes) -> StepExit {
    let secs = |ms: u64| Duration::from_millis(ms).as_secs_f64();
    StepExit {
        exit,
        run_secs: secs(times.ran_ms),
        queue_wait_secs: Some(secs(times.waited_ms)),
    }
}

/// The outcomes of a task are what the tasks that need it get as inputs:
/// larger than an attempt accepts as inputs, they are refused here.
pub(crate) fn outcomes_fit(json_len: usize, max: usize) -> std::result::Result<(), String> {
    if json_len > max {
        return Err(format!(
            "the outputs of the task are too large to be given to the tasks that need them \
             ({json_len} bytes, at most {max})"
        ));
    }
    Ok(())
}

/// The message a step whose script comes through a pipe is refused with.
pub(crate) const SCRIPT_PIPE_REFUSED: &str =
    "a step whose script is read from a pipe (it holds the GitHub token) cannot be run by a worker";

/// What runs the task and asks `executor` for its steps: the core of the
/// plan ([`run_task`]). The handle stops it. Returns the statuses of the
/// jobs of the task.
pub(crate) type Core<'a> = &'a (dyn Fn(
    &Path,
    &CiTaskSpec,
    Arc<dyn StepExecutor>,
    CancelHandle,
) -> std::result::Result<Vec<JobStatus>, String>
         + Sync);

struct StepJob {
    sdk_key: Option<String>,
    argv: Vec<String>,
    cwd: PathBuf,
    env: Vec<(String, String)>,
    log: PathBuf,
    timeout: Option<Duration>,
    script_pipe: bool,
    reply: SyncSender<std::result::Result<StepExit, String>>,
}

/// The executor the core is given: it only queues the step for the thread
/// that owns the supervisor's state and waits for the answer.
struct Queue {
    verify_shard_version: bool,
    sender: Mutex<Sender<StepJob>>,
    artifacts: crate::ci::artifact_exchange::worker::Host,
}

impl StepExecutor for Queue {
    fn verify_shard_runner(&self) -> bool {
        self.verify_shard_version
    }

    fn artifact(
        &self,
        req: &local_ci::step_executor::ArtifactRequest<'_>,
    ) -> std::result::Result<String, String> {
        self.artifacts.run(req).map_err(|e| format!("{e:#}"))
    }

    fn run(&self, req: &StepRequest) -> anyhow::Result<StepExit> {
        let (reply, answer) = sync_channel(1);
        let job = StepJob {
            sdk_key: req.key.clone(),
            argv: req.argv.clone(),
            cwd: req.cwd.clone(),
            env: req.env.clone(),
            log: req.log.clone(),
            timeout: req.timeout,
            script_pipe: req.script_pipe.is_some(),
            reply,
        };
        self.sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(job)
            .map_err(|_| anyhow::anyhow!("the supervisor no longer takes steps"))?;
        answer
            .recv()
            .map_err(|_| anyhow::anyhow!("the supervisor did not answer for the step"))?
            .map_err(anyhow::Error::msg)
    }
}

fn read_plan(ci: &CiTaskSpec) -> std::result::Result<FrozenPlan, String> {
    let meta = std::fs::metadata(&ci.plan_path).map_err(|e| format!("the frozen plan: {e}"))?;
    if meta.len() > MAX_PLAN_BYTES {
        return Err("the frozen plan is too large".into());
    }
    let bytes = std::fs::read(&ci.plan_path).map_err(|e| format!("the frozen plan: {e}"))?;
    if Digest::of_bytes(&bytes) != ci.plan_digest {
        return Err("the frozen plan does not hash to the digest of the attempt".into());
    }
    let plan: FrozenPlan =
        serde_json::from_slice(&bytes).map_err(|_| "the frozen plan is not a plan".to_string())?;
    plan.verify()
        .map_err(|e| format!("the frozen plan is not sealed: {e}"))?;
    Ok(plan)
}

/// Run the task through the core and write its result file. Returns the
/// statuses of its jobs.
pub(crate) fn run_task(
    state_dir: &Path,
    ci: &CiTaskSpec,
    executor: Arc<dyn StepExecutor>,
    cancel: CancelHandle,
) -> std::result::Result<Vec<JobStatus>, String> {
    let plan = read_plan(ci)?;
    let inputs: TaskInputs = serde_json::from_value(ci.inputs.clone())
        .map_err(|_| "the inputs of the task cannot be read".to_string())?;
    let store = SourceStore::open(state_dir).map_err(|e| format!("the source store: {e}"))?;
    let manifest = {
        // (waits for a deleting pass's step, spec 25.8 5b (a); held for
        // this read only, never for the task)
        let _held = store
            .mutation_guard_waiting()
            .map_err(|e| format!("the source store: {e}"))?;
        store
            .manifest(&ci.ci_source_id)
            .map_err(|e| format!("the source of the attempt: {e}"))?
            .ok_or_else(|| "the source of the attempt is not in this worker's store".to_string())?
    };
    let paths = ci.paths(cancel);
    let run_dir = paths.run_dir.clone();
    if let Some(step) = crate::ci::discovery::DiscoveryStep::from_task(&plan, &ci.task_id) {
        let probe = step.execution_plan(&plan).map_err(|e| e.to_string())?;
        let task = probe
            .tasks
            .iter()
            .find(|t| {
                t.workflow == step.workflow && t.jobs.as_slice() == std::slice::from_ref(&step.job)
            })
            .ok_or("missing runner discovery task")?;
        let result = execute_selected(
            &store,
            &manifest,
            &probe,
            SelectedTask::Plan(&task.id),
            &inputs,
            paths,
            executor,
        )
        .map_err(|e| e.to_string())?;
        let listing = step
            .result(&plan, &ci.workspace, &run_dir, &result)
            .map_err(|e| e.to_string())?;
        let mut json = serde_json::to_vec(&listing).map_err(|e| e.to_string())?;
        if json.len() as u64 > crate::ci::advance::MAX_RESULT_BYTES {
            json = serde_json::to_vec(&listing.step.failed()).map_err(|e| e.to_string())?;
        }
        std::fs::write(ci.result_path(), json).map_err(|e| e.to_string())?;
        return Ok(vec![JobStatus::Success]);
    }
    let dispatch = ci
        .dispatch
        .as_ref()
        .map(|value| crate::ci::execute::decode_dispatch(value, &ci.task_id))
        .transpose()?;
    let selected = dispatch
        .as_ref()
        .map_or(SelectedTask::Plan(&ci.task_id), SelectedTask::Derived);
    let result = execute_selected(&store, &manifest, &plan, selected, &inputs, paths, executor)
        .map_err(|e| e.to_string())?;
    let outcomes =
        serde_json::to_vec(&result.outcomes).map_err(|e| format!("the result of the task: {e}"))?;
    outcomes_fit(outcomes.len(), MAX_INPUT_BYTES)?;
    let wire = WireTaskResult::from_task_result(&result, &run_dir)
        .map_err(|e| format!("the result of the task: {e:#}"))?;
    let json = serde_json::to_vec(&wire).map_err(|e| format!("the result of the task: {e}"))?;
    std::fs::write(ci.result_path(), json).map_err(|e| format!("the result of the task: {e}"))?;
    Ok(result.outcomes.iter().map(|o| o.status).collect())
}

fn node_of(index: usize, job: &StepJob, cap_ms: Option<u64>) -> Result<NodeSpec> {
    Ok(NodeSpec {
        node_id: NodeId::parse(&step_node_name(index))
            .map_err(|e| SupervisorError::Spec(e.to_string()))?,
        argv: job.argv.clone(),
        cwd: job.cwd.clone(),
        env: job.env.iter().cloned().collect(),
        weight: None,
        timeout_ms: step_timeout_ms(job.timeout, cap_ms),
    })
}

/// A step that is not started: the attempt is over.
fn not_started() -> StepExit {
    step_exit(StepEnd::Cancelled, NodeTimes::default())
}

/// The end of an attempt that was stopped while no step ran.
fn stopped_between(reason: StopReason) -> NodeEnd {
    NodeEnd {
        reason: Some(reason),
        exit: None,
        unconfirmed: false,
        identity_unknown: false,
        times: NodeTimes::default(),
    }
}

/// Serve the steps the core asks for until it has finished the task. While
/// it asks for none, look at the cancel request, the lease and the deadline
/// every `poll_ms`; a reason to stop cancels the core through `cancel`.
fn serve(
    ctl: &mut Control<'_>,
    steps: Receiver<StepJob>,
    ci: &CiTaskSpec,
    cancel: &CancelHandle,
) -> (Option<NodeEnd>, String, Option<SupervisorError>) {
    let mut over: Option<NodeEnd> = None;
    let mut fatal: Option<SupervisorError> = None;
    let mut last_node = String::new();
    let mut index = 0usize;
    let poll = Duration::from_millis(ctl.spec.poll_ms.max(1));
    let cap_ms = ctl.spec.command_timeout_ms;
    loop {
        let job = match steps.recv_timeout(poll) {
            Ok(job) => job,
            // The core returned (or panicked): the task is over.
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                if over.is_none() && fatal.is_none() {
                    // The core works outside a step. The look renews the
                    // lease; a reason to stop is acted on now.
                    match ctl.look_between_nodes() {
                        Ok(None) => {}
                        Ok(Some(reason)) => {
                            // First the core: it starts nothing from here.
                            cancel.cancel();
                            match ctl.begin_stop(reason) {
                                Ok(()) => over = Some(stopped_between(reason)),
                                Err(e) => fatal = Some(e),
                            }
                        }
                        Err(e) => {
                            cancel.cancel();
                            fatal = Some(e);
                        }
                    }
                }
                continue;
            }
        };
        if over.is_some() || fatal.is_some() {
            // The attempt is over: nothing is started any more.
            let _ = job.reply.send(Ok(not_started()));
            continue;
        }
        if job.script_pipe {
            let _ = job.reply.send(Err(SCRIPT_PIPE_REFUSED.into()));
            continue;
        }
        let ran = node_of(index, &job, cap_ms).and_then(|node| {
            last_node = node.node_id.to_string();
            // What the step prints is kept beside the core's own files too,
            // masked like the spool.
            ctl.tee = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&job.log)
                .ok();
            let timing = ci
                .history_scope
                .as_ref()
                .zip(job.sdk_key.as_deref())
                .and_then(|(scope, key)| {
                    let cwd = job.cwd.strip_prefix(&ci.workspace).ok()?.to_str()?;
                    Some(crate::ci::timing::step(scope, key, cwd))
                });
            let end = ctl.run_node_with_ci_history(index, &node, timing.as_ref());
            ctl.tee = None;
            end
        });
        index += 1;
        let answer = match ran {
            Ok(end) => {
                if ends_attempt(&end) {
                    // Only a flag: the handle holds no process group.
                    cancel.cancel();
                    over = Some(end);
                }
                step_reply(&end).map(|exit| step_exit(exit, end.times))
            }
            Err(e) => {
                cancel.cancel();
                let why = e.to_string();
                fatal = Some(e);
                Err(why)
            }
        };
        let _ = job.reply.send(answer);
    }
    (over, last_node, fatal)
}

/// Run the CI task of the attempt: the core on a thread of its own, its
/// steps on this one.
pub(super) fn drive(ctl: &mut Control<'_>, ci: &CiTaskSpec, core: Core<'_>) -> Result<CiEnd> {
    let state_dir = ctl.spec.state_dir.clone();
    std::fs::create_dir_all(&ci.scratch)
        .map_err(|e| SupervisorError::State(format!("the CI scratch directory: {e}")))?;
    let (send, steps) = channel();
    let executor: Arc<dyn StepExecutor> = Arc::new(Queue {
        verify_shard_version: ci.verify_shard_version,
        sender: Mutex::new(send),
        artifacts: crate::ci::artifact_exchange::worker::Host {
            root: ci.scratch.join("exchange"),
            workflow: ci.artifact_workflow.clone(),
            secret_files: ctl
                .spec
                .artifacts
                .as_ref()
                .map(|s| s.secret_files.clone())
                .unwrap_or_default(),
            secret_paths: ctl
                .spec
                .artifacts
                .as_ref()
                .map(|s| s.secret_paths.clone())
                .unwrap_or_default(),
            masker: ctl.spec.mask.masker(),
            serial: Mutex::new(()),
        },
    });
    // The one handle of the attempt: the core watches it, this side cancels.
    let cancel = CancelHandle::new();
    let (served, ran) = std::thread::scope(|s| {
        // The executor goes with the core: when the core returns (or
        // panics) the queue closes and the loop below ends.
        let handle = cancel.clone();
        let running = s.spawn(move || core(&state_dir, ci, executor, handle));
        let served = serve(ctl, steps, ci, &cancel);
        (served, running.join())
    });
    let (over, last_node, fatal) = served;
    if let Some(e) = fatal {
        return Err(e);
    }
    let ran = ran.unwrap_or_else(|_| Err("the CI core panicked".into()));
    let ended = |exit| NodeEnd {
        reason: None,
        exit,
        unconfirmed: false,
        identity_unknown: false,
        times: NodeTimes::default(),
    };
    Ok(match (over, ran) {
        (Some(end), _) => CiEnd {
            end,
            last_node,
            failure: None,
        },
        (None, Ok(statuses)) => CiEnd {
            end: ended(Some(exit_of_outcomes(statuses))),
            last_node,
            failure: None,
        },
        (None, Err(why)) => CiEnd {
            end: ended(None),
            last_node,
            failure: Some(why),
        },
    })
}

#[cfg(test)]
mod run_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{TimeoutInfo, TimeoutKind, TimeoutPhase};

    fn end(reason: Option<StopReason>, exit: Option<Exit>, unconfirmed: bool) -> NodeEnd {
        NodeEnd {
            reason,
            exit,
            unconfirmed,
            identity_unknown: false,
            times: NodeTimes::default(),
        }
    }

    #[test]
    fn a_task_fails_by_a_failed_or_cancelled_job_and_not_by_a_skipped_one() {
        use JobStatus::*;
        assert_eq!(exit_of_outcomes([]), Exit::Code(0));
        assert_eq!(exit_of_outcomes([Success, Skipped]), Exit::Code(0));
        assert_eq!(exit_of_outcomes([Success, Failure]), Exit::Code(1));
        assert_eq!(exit_of_outcomes([Cancelled]), Exit::Code(1));
    }

    #[test]
    fn the_core_is_told_how_a_step_ended() {
        let timeout = StopReason::Timeout(TimeoutInfo {
            kind: TimeoutKind::Command,
            phase: TimeoutPhase::Running,
        });
        assert_eq!(
            step_reply(&end(None, Some(Exit::Code(3)), false)),
            Ok(StepEnd::Code(3))
        );
        assert_eq!(
            step_reply(&end(None, Some(Exit::Signal(9)), false)),
            Ok(StepEnd::Code(137))
        );
        assert_eq!(
            step_reply(&end(Some(timeout), Some(Exit::Signal(15)), false)),
            Ok(StepEnd::Timeout)
        );
        assert_eq!(
            step_reply(&end(Some(StopReason::Cancel), None, false)),
            Ok(StepEnd::Cancelled)
        );
        assert_eq!(
            step_reply(&end(
                Some(StopReason::LeaseExpired),
                Some(Exit::Code(0)),
                false
            )),
            Ok(StepEnd::Cancelled)
        );
        assert_eq!(
            step_reply(&end(None, Some(Exit::Code(0)), true)),
            Ok(StepEnd::Cancelled)
        );
        assert!(step_reply(&end(None, None, false)).is_err());
    }

    #[test]
    fn a_step_node_is_found_again_by_its_number_and_nothing_else_is() {
        for index in [0, 1, 9, 10, 1234] {
            assert_eq!(step_index(&step_node_name(index)), Some(index));
        }
        assert_eq!(step_node_name(7), "step-7");
        for other in [
            "step-", "step-01", "step-+1", "step--1", "step-1x", "main", "7", "",
        ] {
            assert_eq!(step_index(other), None, "{other:?}");
        }
    }

    fn timed_out(kind: TimeoutKind, phase: TimeoutPhase) -> Option<StopReason> {
        Some(StopReason::Timeout(TimeoutInfo { kind, phase }))
    }

    #[test]
    fn a_step_over_its_own_limit_is_a_result_and_every_other_stop_ends_the_attempt() {
        assert!(!ends_attempt(&end(None, Some(Exit::Code(1)), false)));
        assert!(!ends_attempt(&end(None, None, false)));
        // `timeout-minutes` (or the cap of the attempt on a command): the
        // core decides what follows.
        let own = timed_out(TimeoutKind::Command, TimeoutPhase::Running);
        assert!(!ends_attempt(&end(own, Some(Exit::Signal(15)), false)));
        // ... unless the stop could not be confirmed.
        assert!(ends_attempt(&end(own, None, true)));
        for reason in [
            Some(StopReason::Cancel),
            Some(StopReason::LeaseExpired),
            Some(StopReason::SpoolFailed),
            timed_out(TimeoutKind::Deadline, TimeoutPhase::Running),
            timed_out(TimeoutKind::Deadline, TimeoutPhase::Waiting),
            timed_out(TimeoutKind::ResourceWait, TimeoutPhase::Waiting),
        ] {
            assert!(ends_attempt(&end(reason, None, false)), "{reason:?}");
        }
        assert!(ends_attempt(&end(None, Some(Exit::Code(0)), true)));
    }

    #[test]
    fn a_step_never_gets_more_time_than_the_attempt_allows_a_command() {
        let ms = |n| Some(Duration::from_millis(n));
        assert_eq!(step_timeout_ms(None, None), None);
        assert_eq!(step_timeout_ms(ms(700), None), Some(700));
        assert_eq!(step_timeout_ms(None, Some(500)), Some(500));
        assert_eq!(step_timeout_ms(ms(700), Some(500)), Some(500));
        assert_eq!(step_timeout_ms(ms(300), Some(500)), Some(300));
        assert_eq!(step_timeout_ms(ms(500), Some(500)), Some(500));
        // Never 0 (which would be no time at all), never an overflow.
        assert_eq!(
            step_timeout_ms(Some(Duration::from_nanos(1)), None),
            Some(1)
        );
        assert_eq!(step_timeout_ms(Some(Duration::MAX), None), Some(u64::MAX));
        assert_eq!(step_timeout_ms(Some(Duration::MAX), Some(9)), Some(9));
    }

    #[test]
    fn the_core_is_told_the_running_time_and_the_waiting_time_apart() {
        let told = step_exit(
            StepEnd::Code(0),
            NodeTimes {
                waited_ms: 1_500,
                ran_ms: 250,
            },
        );
        assert_eq!(told.exit, StepEnd::Code(0));
        assert_eq!(told.run_secs, 0.25);
        assert_eq!(told.queue_wait_secs, Some(1.5));
        let none = not_started();
        assert_eq!(none.exit, StepEnd::Cancelled);
        assert_eq!((none.run_secs, none.queue_wait_secs), (0.0, Some(0.0)));
    }

    #[test]
    fn outcomes_larger_than_an_attempt_takes_as_inputs_are_refused() {
        assert_eq!(outcomes_fit(0, 10), Ok(()));
        assert_eq!(outcomes_fit(10, 10), Ok(()));
        let why = outcomes_fit(11, 10).unwrap_err();
        assert!(why.contains("too large") && why.contains("11") && why.contains("10"));
        // What a task writes and what the next one may be given are one limit.
        assert_eq!(MAX_INPUT_BYTES, 4 << 20);
    }

    #[test]
    fn a_stop_between_steps_has_no_exit_and_is_confirmed() {
        let end = stopped_between(StopReason::LeaseExpired);
        assert_eq!(end.reason, Some(StopReason::LeaseExpired));
        assert_eq!(end.exit, None);
        assert!(!end.unconfirmed && !end.identity_unknown);
        assert!(ends_attempt(&end));
    }

    #[test]
    fn the_spec_shows_no_value_of_its_environment_and_needs_absolute_paths() {
        let mut ci = CiTaskSpec {
            verify_shard_version: false,
            artifact_workflow: None,
            history_scope: None,
            dispatch: None,
            plan_path: "/a/plan.json".into(),
            plan_digest: Digest::of_bytes(b"plan"),
            task_id: "t".into(),
            inputs: serde_json::json!({}),
            ci_source_id: "src-0".into(),
            workspace: "/a/ws".into(),
            prepared: Vec::new(),
            scratch: "/a/ci".into(),
            env: [("TOKEN".to_string(), "hunter2-secret".to_string())].into(),
        };
        let shown = format!("{ci:?}");
        assert!(shown.contains("TOKEN") && !shown.contains("hunter2"));
        assert_eq!(ci.validate(), Ok(()));
        assert_eq!(ci.result_path(), PathBuf::from("/a/ci/result.json"));
        ci.scratch = "ci".into();
        assert!(ci.validate().is_err());
        ci.scratch = "/a/ci".into();
        ci.task_id.clear();
        assert!(ci.validate().is_err());
    }
}
