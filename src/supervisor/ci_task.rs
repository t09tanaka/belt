//! An attempt that is one task of a frozen CI plan (spec chapter 7).
//!
//! The core of the plan decides which steps run and in which order; every
//! step it wants started comes here as a request and is run as a node of the
//! attempt, by the same code that runs a command of `exec`: registered and
//! admitted by task-traffic-controller, claimed in the database, its output
//! masked into the spool, stopped and confirmed stopped by the supervisor.
//! The core starts no process of a step and signals none: its cancel handle
//! is never given a process group, so all it does is stop asking for steps.
//!
//! Once the supervisor has a reason to stop (a cancel, an expired lease, a
//! timeout) or could not confirm a stop, no further step is started,
//! whatever the plan would still run (`if: always()` included): the attempt
//! is over.

use super::error::{Result, SupervisorError};
use super::run::{Control, NodeEnd};
use super::spec::NodeSpec;
use super::termination::StopReason;
use crate::ci::execute::{execute_task, TaskPaths};
use crate::ids::{Digest, NodeId};
use crate::source::SourceStore;
use crate::state::Exit;
use local_ci::exec::Exit as StepEnd;
use local_ci::{
    CancelHandle, FrozenPlan, JobStatus, StepExecutor, StepExit, StepRequest, TaskInputs,
    WireTaskResult,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The name the task's result is kept under, as an artifact of the attempt:
/// the parent reads the outcomes and outputs of the task's jobs from it.
pub const RESULT_ARTIFACT: &str = "rtr-ci-task-result.json";
/// The frozen plan is JSON of at most this many bytes.
pub const MAX_PLAN_BYTES: u64 = 64 << 20;
const RESULT_FILE: &str = "result.json";

/// What the agent hands the supervisor for a CI task. `Debug` shows the
/// names of `env` only.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CiTaskSpec {
    /// The bytes of the frozen plan. Used only when they hash to
    /// `plan_digest`.
    pub plan_path: PathBuf,
    pub plan_digest: Digest,
    pub task_id: String,
    /// The core's `TaskInputs`, as the parent sent them.
    pub inputs: serde_json::Value,
    /// The source of the attempt, as the worker's source store names it.
    pub ci_source_id: String,
    /// The attempt's workspace root: the captured source, nothing added.
    pub workspace: PathBuf,
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

    fn paths(&self) -> TaskPaths {
        TaskPaths {
            root: self.workspace.clone(),
            run_dir: self.scratch.join("run"),
            tools: self.scratch.join("tools"),
            lock_file: self.scratch.join("lock"),
            trash: self.scratch.join("trash"),
            env: self.env.clone(),
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

/// Does this end of a step end the attempt?
pub(crate) fn ends_attempt(end: &NodeEnd) -> bool {
    end.reason.is_some() || end.unconfirmed
}

struct StepJob {
    argv: Vec<String>,
    cwd: PathBuf,
    env: Vec<(String, String)>,
    log: PathBuf,
    timeout: Option<Duration>,
    script_pipe: bool,
    cancel: CancelHandle,
    reply: SyncSender<std::result::Result<StepExit, String>>,
}

/// The executor the core is given: it only queues the step for the thread
/// that owns the supervisor's state and waits for the answer.
struct Queue(Mutex<Sender<StepJob>>);

impl StepExecutor for Queue {
    fn run(&self, req: &StepRequest) -> anyhow::Result<StepExit> {
        let (reply, answer) = sync_channel(1);
        let job = StepJob {
            argv: req.argv.clone(),
            cwd: req.cwd.clone(),
            env: req.env.clone(),
            log: req.log.clone(),
            timeout: req.timeout,
            script_pipe: req.script_pipe.is_some(),
            cancel: req.cancel.clone(),
            reply,
        };
        self.0
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
fn run_task(
    state_dir: &Path,
    ci: &CiTaskSpec,
    executor: Arc<dyn StepExecutor>,
) -> std::result::Result<Vec<JobStatus>, String> {
    let plan = read_plan(ci)?;
    let inputs: TaskInputs = serde_json::from_value(ci.inputs.clone())
        .map_err(|_| "the inputs of the task cannot be read".to_string())?;
    let store = SourceStore::open(state_dir).map_err(|e| format!("the source store: {e}"))?;
    let manifest = store
        .manifest(&ci.ci_source_id)
        .map_err(|e| format!("the source of the attempt: {e}"))?
        .ok_or_else(|| "the source of the attempt is not in this worker's store".to_string())?;
    let paths = ci.paths();
    let run_dir = paths.run_dir.clone();
    let result = execute_task(
        &store,
        &manifest,
        &plan,
        &ci.task_id,
        &inputs,
        paths,
        executor,
    )
    .map_err(|e| e.to_string())?;
    let wire = WireTaskResult::from_task_result(&result, &run_dir)
        .map_err(|e| format!("the result of the task: {e:#}"))?;
    let json = serde_json::to_vec(&wire).map_err(|e| format!("the result of the task: {e}"))?;
    std::fs::write(ci.result_path(), json).map_err(|e| format!("the result of the task: {e}"))?;
    Ok(result.outcomes.iter().map(|o| o.status).collect())
}

fn node_of(index: usize, job: &StepJob) -> Result<NodeSpec> {
    Ok(NodeSpec {
        node_id: NodeId::parse(&step_node_name(index))
            .map_err(|e| SupervisorError::Spec(e.to_string()))?,
        argv: job.argv.clone(),
        cwd: job.cwd.clone(),
        env: job.env.iter().cloned().collect(),
        weight: None,
        timeout_ms: job
            .timeout
            .map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX).max(1)),
    })
}

/// Serve the steps the core asks for until it has finished the task.
fn serve(
    ctl: &mut Control<'_>,
    steps: Receiver<StepJob>,
) -> (Option<NodeEnd>, String, Option<SupervisorError>) {
    let mut over: Option<NodeEnd> = None;
    let mut fatal: Option<SupervisorError> = None;
    let mut last_node = String::new();
    let mut index = 0usize;
    for job in steps {
        if over.is_some() || fatal.is_some() {
            // The attempt is over: nothing is started any more.
            let _ = job.reply.send(Ok(StepExit {
                exit: StepEnd::Cancelled,
                run_secs: 0.0,
                queue_wait_secs: None,
            }));
            continue;
        }
        if job.script_pipe {
            let _ = job.reply.send(Err(
                "a step whose script is read from a pipe (it holds the GitHub token) cannot \
                 be run by a worker"
                    .into(),
            ));
            continue;
        }
        let since = Instant::now();
        let ran = node_of(index, &job).and_then(|node| {
            last_node = node.node_id.to_string();
            // What the step prints is kept beside the core's own files too,
            // masked like the spool.
            ctl.tee = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&job.log)
                .ok();
            let end = ctl.run_node(index, &node);
            ctl.tee = None;
            end
        });
        index += 1;
        let answer = match ran {
            Ok(end) => {
                if ends_attempt(&end) {
                    // Only a flag: the handle holds no process group.
                    job.cancel.cancel();
                    over = Some(end);
                }
                step_reply(&end).map(|exit| StepExit {
                    exit,
                    run_secs: since.elapsed().as_secs_f64(),
                    queue_wait_secs: None,
                })
            }
            Err(e) => {
                job.cancel.cancel();
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
pub(super) fn drive(ctl: &mut Control<'_>, ci: &CiTaskSpec) -> Result<CiEnd> {
    let state_dir = ctl.spec.state_dir.clone();
    std::fs::create_dir_all(&ci.scratch)
        .map_err(|e| SupervisorError::State(format!("the CI scratch directory: {e}")))?;
    let (send, steps) = channel();
    let executor: Arc<dyn StepExecutor> = Arc::new(Queue(Mutex::new(send)));
    let (served, ran) = std::thread::scope(|s| {
        // The executor goes with the core: when the core returns (or
        // panics) the queue closes and the loop below ends.
        let core = s.spawn(move || run_task(&state_dir, ci, executor));
        let served = serve(ctl, steps);
        (served, core.join())
    });
    let (over, last_node, fatal) = served;
    if let Some(e) = fatal {
        return Err(e);
    }
    let ran = ran.unwrap_or_else(|_| Err("the CI core panicked".into()));
    Ok(match (over, ran) {
        (Some(end), _) => CiEnd {
            end,
            last_node,
            failure: None,
        },
        (None, Ok(statuses)) => CiEnd {
            end: NodeEnd {
                reason: None,
                exit: Some(exit_of_outcomes(statuses)),
                unconfirmed: false,
                identity_unknown: false,
            },
            last_node,
            failure: None,
        },
        (None, Err(why)) => CiEnd {
            end: NodeEnd {
                reason: None,
                exit: None,
                unconfirmed: false,
                identity_unknown: false,
            },
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

    #[test]
    fn only_a_stop_or_an_unconfirmed_end_ends_the_attempt() {
        assert!(!ends_attempt(&end(None, Some(Exit::Code(1)), false)));
        assert!(!ends_attempt(&end(None, None, false)));
        assert!(ends_attempt(&end(Some(StopReason::Cancel), None, false)));
        assert!(ends_attempt(&end(None, Some(Exit::Code(0)), true)));
    }

    #[test]
    fn the_spec_shows_no_value_of_its_environment_and_needs_absolute_paths() {
        let mut ci = CiTaskSpec {
            plan_path: "/a/plan.json".into(),
            plan_digest: Digest::of_bytes(b"plan"),
            task_id: "t".into(),
            inputs: serde_json::json!({}),
            ci_source_id: "src-0".into(),
            workspace: "/a/ws".into(),
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
