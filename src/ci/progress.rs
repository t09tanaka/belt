//! How a CI run goes on from one task to the next: which tasks may start
//! once the jobs they need have ended, and with which inputs.
//!
//! [`Progression`] has the shape of the shared driver of the core (inputs
//! that are proposed and applied, pending actions that are acknowledged), so
//! that the driver can take the place of [`NeedsProgression`] when it is
//! published. Nothing here does IO: the host hands in what ended, and carries
//! out the actions.
//!
//! [`NeedsProgression`] is the least that a run of plain jobs needs: the
//! `needs` of the plan as a DAG. Whether a job runs is not decided here. A
//! task is made ready as soon as every task it depends on has ended, however
//! they ended, and the core of the plan evaluates the job's `if:` on the
//! worker from the inputs (`success()` by default, `failure()`, `always()`):
//! a job that is not to run comes back as `skipped` without a step started.
//! Matrix variants and dynamic matrices are the driver's.
//!
//! The shards of a split job are tasks like the others, placed side by side.
//! The job they are has one outcome, made when the last of them ended
//! ([`super::shards::join`]): only then do the jobs that need it get its
//! result. With fail-fast, a shard that failed makes the host stop the
//! shards that have not ended ([`ActionKind::Cancel`]).

use super::shards;
use local_ci::dag::{ExecutionTask, ShardGroup};
use local_ci::shard::ShardOutcome;
use local_ci::task::{JobOutcome, JobStatus, TaskInputs};
use local_ci::{FrozenPlan, WireTaskResult};
use std::collections::{BTreeMap, BTreeSet};

/// What the host tells the progression.
#[derive(Debug, Clone)]
pub enum Input {
    /// Look for what may go on.
    Advance,
    /// The run was cancelled: no task that is not ready yet becomes ready.
    Cancel,
    /// A task ended with a result.
    Settle {
        task: String,
        result: Box<WireTaskResult>,
    },
    /// The host asserts that the task ended and will deliver no result (it
    /// was cancelled before it ran, its attempt was lost, its result is
    /// missing or cannot be read): its jobs count as `status`.
    Fence {
        task: String,
        status: JobStatus,
        reason: String,
    },
    /// The host carried out an action.
    Ack { action: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ActionKind {
    /// The task may be placed, with these inputs.
    Ready { task: String, inputs: TaskInputs },
    /// The task is to be stopped (cancelled where it runs, never placed
    /// where it does not yet): a shard of its job failed.
    Cancel { task: String, reason: String },
    /// Every task of the run ended.
    Finished,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    pub id: String,
    pub kind: ActionKind,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProgressError {
    #[error("the plan cannot be run yet: {0}")]
    Unsupported(String),
    #[error("the plan has no task `{0}`")]
    UnknownTask(String),
    #[error("task `{0}` ended already")]
    Settled(String),
    #[error("the result of task `{task}` is not acceptable: {why}")]
    Result { task: String, why: String },
    #[error("there is no pending action `{0}`")]
    UnknownAction(String),
}

/// The progression of one run, behind the shape of the core's driver.
pub trait Progression {
    /// Take one input. An input that is refused changes nothing.
    fn step(&mut self, input: Input) -> Result<(), ProgressError>;
    /// What the host has to carry out and acknowledge, oldest first.
    fn pending_actions(&self) -> &[Action];
    /// How a job of a workflow ended, once its task has.
    fn outcome(&self, workflow: &str, job: &str) -> Option<&JobOutcome>;
}

/// Why a plan cannot be run by [`NeedsProgression`] (and by the workers of
/// this build), or `None`.
pub fn unsupported(plan: &FrozenPlan) -> Option<String> {
    let task = plan.tasks.iter().find_map(|t| {
        let what = if t.variant.is_some() {
            "is a variant of a matrix"
        } else if t.dynamic_matrix {
            "has a matrix that is an expression"
        } else if !t.services.is_empty() {
            "has service containers"
        } else {
            return None;
        };
        Some(format!("task `{}` {what}", t.id))
    });
    task.or_else(|| shards::shape_fault(plan))
}

/// The `needs` of the plan as a DAG over its tasks.
pub struct NeedsProgression {
    tasks: Vec<ExecutionTask>,
    /// The outcomes of the jobs of each task that ended.
    settled: BTreeMap<String, Vec<JobOutcome>>,
    /// The tasks a `Ready` action was made for.
    readied: BTreeSet<String>,
    /// The split jobs of the plan.
    groups: Vec<ShardGroup>,
    /// What the tests of each shard that ended with a result did.
    receipts: BTreeMap<String, ShardOutcome>,
    /// The one outcome of each split job whose shards all ended.
    joins: BTreeMap<String, JobOutcome>,
    /// What is wrong with the shards of such a job taken together.
    diagnostics: BTreeMap<String, String>,
    /// The shards a `Cancel` action was made for.
    stopped: BTreeSet<String>,
    cancelled: bool,
    finished: bool,
    actions: Vec<Action>,
}

impl NeedsProgression {
    pub fn new(plan: &FrozenPlan) -> Result<Self, ProgressError> {
        if let Some(why) = unsupported(plan) {
            return Err(ProgressError::Unsupported(why));
        }
        let ids: BTreeSet<&str> = plan.tasks.iter().map(|t| t.id.as_str()).collect();
        for task in &plan.tasks {
            if let Some(missing) = task.depends_on.iter().find(|d| !ids.contains(d.as_str())) {
                return Err(ProgressError::UnknownTask(missing.clone()));
            }
        }
        Ok(NeedsProgression {
            tasks: plan.tasks.clone(),
            settled: BTreeMap::new(),
            readied: BTreeSet::new(),
            groups: plan
                .shard_groups
                .iter()
                .filter(|g| g.split)
                .cloned()
                .collect(),
            receipts: BTreeMap::new(),
            joins: BTreeMap::new(),
            diagnostics: BTreeMap::new(),
            stopped: BTreeSet::new(),
            cancelled: false,
            finished: false,
            actions: Vec::new(),
        })
    }

    fn task(&self, id: &str) -> Result<&ExecutionTask, ProgressError> {
        self.tasks
            .iter()
            .find(|t| t.id == id)
            .ok_or_else(|| ProgressError::UnknownTask(id.to_string()))
    }

    /// Has every task `task` depends on ended?
    pub fn dependencies_settled(&self, task: &str) -> bool {
        self.task(task)
            .is_ok_and(|t| t.depends_on.iter().all(|d| self.settled.contains_key(d)))
    }

    /// What is wrong with the shards of the split job `group` together
    /// (a failed test, a test that is missing or there twice, a shard
    /// without a result), once all of them ended. `None` when nothing is.
    pub fn split_job_problems(&self, group: &str) -> Option<&str> {
        self.diagnostics.get(group).map(String::as_str)
    }

    pub fn is_settled(&self, task: &str) -> bool {
        self.settled.contains_key(task)
    }

    fn group_of(&self, task: &str) -> Option<&ShardGroup> {
        self.groups
            .iter()
            .find(|g| g.shards.iter().any(|s| s == task))
    }

    /// The outcomes of the jobs the tasks `ids` are: those of a task that
    /// ended, and for the shards of a split job the one outcome of the job,
    /// once, when all of them ended.
    fn jobs_of<'a, 'b>(&'a self, ids: impl Iterator<Item = &'b String>) -> Vec<&'a JobOutcome> {
        let mut joined = BTreeSet::new();
        let mut out = Vec::new();
        for id in ids {
            match self.group_of(id) {
                Some(group) if joined.insert(&group.id) => out.extend(self.joins.get(&group.id)),
                Some(_) => {}
                None => out.extend(self.settled.get(id).into_iter().flatten()),
            }
        }
        out
    }

    /// The outcomes of the jobs of the tasks `task` depends on.
    fn needs_of(&self, task: &ExecutionTask) -> Vec<&JobOutcome> {
        self.jobs_of(task.depends_on.iter())
    }

    /// Give every split job whose shards all ended its one outcome, and
    /// stop the shards a failed one stops.
    fn settle_groups(&mut self) {
        for group in &self.groups {
            let all_ended = group.shards.iter().all(|s| self.settled.contains_key(s));
            if all_ended && !self.joins.contains_key(&group.id) {
                let parts: Vec<JobOutcome> = group
                    .shards
                    .iter()
                    .filter_map(|s| self.settled.get(s))
                    .flatten()
                    .cloned()
                    .collect();
                let receipts: Vec<ShardOutcome> = group
                    .shards
                    .iter()
                    .filter_map(|s| self.receipts.get(s))
                    .cloned()
                    .collect();
                let job = shards::join(group, &parts, &receipts);
                self.joins.insert(group.id.clone(), job);
                let problems = shards::problems(group, &parts, &receipts);
                if let Some(why) = shards::diagnostic(&problems) {
                    self.diagnostics.insert(group.id.clone(), why);
                }
            }
            let failed: BTreeMap<String, bool> = group
                .shards
                .iter()
                .filter_map(|s| Some((s, self.settled.get(s)?)))
                .map(|(s, outcomes)| {
                    let failed = shards::shard_failed(outcomes, self.receipts.get(s));
                    (s.clone(), failed)
                })
                .collect();
            for task in shards::to_cancel(group, &failed) {
                if self.stopped.insert(task.clone()) {
                    self.actions.push(Action {
                        id: format!("cancel:{task}"),
                        kind: ActionKind::Cancel {
                            task,
                            reason: shards::FAIL_FAST_REASON.into(),
                        },
                    });
                }
            }
        }
    }

    fn unsettled(&self, task: &str) -> Result<&ExecutionTask, ProgressError> {
        let found = self.task(task)?;
        if self.settled.contains_key(task) {
            return Err(ProgressError::Settled(task.to_string()));
        }
        Ok(found)
    }

    fn advance(&mut self) {
        self.settle_groups();
        let ready: Vec<(String, TaskInputs)> = self
            .tasks
            .iter()
            .filter(|_| !self.cancelled)
            .filter(|t| !self.readied.contains(&t.id) && !self.settled.contains_key(&t.id))
            .filter(|t| t.depends_on.iter().all(|d| self.settled.contains_key(d)))
            .map(|t| {
                let finished = self.jobs_of(self.tasks.iter().map(|t| &t.id));
                (
                    t.id.clone(),
                    TaskInputs::new(self.needs_of(t), finished, false),
                )
            })
            .collect();
        for (task, inputs) in ready {
            self.readied.insert(task.clone());
            self.actions.push(Action {
                id: format!("ready:{task}"),
                kind: ActionKind::Ready { task, inputs },
            });
        }
        if !self.finished && self.tasks.iter().all(|t| self.settled.contains_key(&t.id)) {
            self.finished = true;
            self.actions.push(Action {
                id: "finished".into(),
                kind: ActionKind::Finished,
            });
        }
    }
}

/// What is wrong with a result as the result of `task`, if anything: it must
/// be of that task and hold one outcome of each of its jobs, and of no other.
pub fn result_fault(task: &ExecutionTask, result: &WireTaskResult) -> Option<String> {
    if result.task != task.id {
        return Some(format!("it is the result of task `{}`", result.task));
    }
    if let Some(o) = result.outcomes.iter().find(|o| !task.jobs.contains(&o.job)) {
        return Some(format!("it has an outcome of job `{}`", o.job));
    }
    task.jobs
        .iter()
        .find(|job| result.outcomes.iter().filter(|o| &o.job == *job).count() != 1)
        .map(|job| format!("it does not have one outcome of job `{job}`"))
}

/// The outcomes of the jobs of a task that ended without a result: each as
/// `status`, with no outputs. `ancestor_failed` says whether a job the task
/// needed had failed (or one further up), as the core would have set it.
pub fn outcomes_without_result(
    task: &ExecutionTask,
    status: JobStatus,
    reason: &str,
    needs: &[&JobOutcome],
) -> Vec<JobOutcome> {
    let ancestor_failed = needs.iter().any(|o| o.failed_or_ancestor_failed());
    task.jobs
        .iter()
        .map(|job| JobOutcome {
            job: job.clone(),
            variant: None,
            status,
            outputs: BTreeMap::new(),
            skipped: (status != JobStatus::Failure).then(|| reason.to_string()),
            ancestor_failed,
        })
        .collect()
}

impl Progression for NeedsProgression {
    fn step(&mut self, input: Input) -> Result<(), ProgressError> {
        match input {
            Input::Advance => {}
            Input::Cancel => self.cancelled = true,
            Input::Settle { task, result } => {
                let found = self.unsettled(&task)?;
                if let Some(why) = result_fault(found, &result) {
                    return Err(ProgressError::Result { task, why });
                }
                if let Some(receipt) = shards::receipt(found, &result) {
                    self.receipts.insert(task.clone(), receipt.clone());
                }
                self.settled.insert(task, result.outcomes);
            }
            Input::Fence {
                task,
                status,
                reason,
            } => {
                let found = self.unsettled(&task)?;
                let outcomes =
                    outcomes_without_result(found, status, &reason, &self.needs_of(found));
                self.settled.insert(task, outcomes);
            }
            Input::Ack { action } => {
                let at = self.actions.iter().position(|a| a.id == action);
                let at = at.ok_or(ProgressError::UnknownAction(action))?;
                self.actions.remove(at);
                return Ok(());
            }
        }
        self.advance();
        Ok(())
    }

    fn pending_actions(&self) -> &[Action] {
        &self.actions
    }

    fn outcome(&self, workflow: &str, job: &str) -> Option<&JobOutcome> {
        let of = self.tasks.iter().filter(|t| t.workflow == workflow);
        self.jobs_of(of.map(|t| &t.id))
            .into_iter()
            .find(|o| o.job == job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::execute::tests::{fixture, task_of};

    const CHAIN: &str = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    outputs:\n      v: ${{ steps.v.outputs.v }}\n    steps:\n      - id: v\n        run: echo v=1 >> \"$GITHUB_OUTPUT\"\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n  lint:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo lint\n  report:\n    needs: [test, lint]\n    if: always()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo report\n";

    fn outcome(job: &str, status: JobStatus) -> JobOutcome {
        JobOutcome {
            job: job.into(),
            variant: None,
            status,
            outputs: [("v".to_string(), "1".to_string())].into(),
            skipped: None,
            ancestor_failed: false,
        }
    }

    fn result(task: &str, outcomes: Vec<JobOutcome>) -> Box<WireTaskResult> {
        Box::new(WireTaskResult {
            schema_version: local_ci::wire::WIRE_SCHEMA_VERSION,
            task: task.into(),
            jobs: vec![],
            outcomes,
            receipt_ignored: None,
            artifacts: vec![],
            shard: None,
        })
    }

    fn ready(p: &NeedsProgression) -> Vec<String> {
        p.pending_actions()
            .iter()
            .filter_map(|a| match &a.kind {
                ActionKind::Ready { task, .. } => Some(task.clone()),
                ActionKind::Cancel { .. } | ActionKind::Finished => None,
            })
            .collect()
    }

    fn ack_all(p: &mut NeedsProgression) {
        let ids: Vec<String> = p.pending_actions().iter().map(|a| a.id.clone()).collect();
        for id in ids {
            p.step(Input::Ack { action: id }).unwrap();
        }
    }

    #[test]
    fn a_task_is_ready_only_when_every_task_it_needs_ended_and_gets_their_outcomes() {
        let f = fixture(CHAIN);
        let id = |job: &str| task_of(&f.plan, job).to_string();
        let mut p = NeedsProgression::new(&f.plan).unwrap();
        assert!(
            p.pending_actions().is_empty(),
            "nothing before the first look"
        );
        p.step(Input::Advance).unwrap();
        let mut first = ready(&p);
        first.sort();
        let mut roots = vec![id("build"), id("lint")];
        roots.sort();
        assert_eq!(first, roots, "the tasks that need nothing");
        // Asked again, nothing is made ready twice.
        p.step(Input::Advance).unwrap();
        assert_eq!(ready(&p).len(), 2);
        ack_all(&mut p);
        assert!(!p.dependencies_settled(&id("test")));

        let built = result(&id("build"), vec![outcome("build", JobStatus::Success)]);
        p.step(Input::Settle {
            task: id("build"),
            result: built,
        })
        .unwrap();
        assert!(p.is_settled(&id("build")));
        assert!(p.dependencies_settled(&id("test")));
        let actions = p.pending_actions().to_vec();
        assert_eq!(actions.len(), 1, "report still waits for test and lint");
        let ActionKind::Ready { task, inputs } = &actions[0].kind else {
            panic!("{actions:?}")
        };
        assert_eq!(task, &id("test"));
        assert_eq!(inputs.needs["build"].outputs["v"], "1");
        assert_eq!(inputs.needs["build"].status, JobStatus::Success);
        assert!(!inputs.run_cancelled);
        assert_eq!(
            p.outcome(&f.plan.tasks[0].workflow, "build").unwrap().job,
            "build"
        );
        assert!(p.outcome("another.yml", "build").is_none());
        assert!(p.outcome(&f.plan.tasks[0].workflow, "test").is_none());
    }

    #[test]
    fn a_failed_task_still_makes_the_next_ready_and_the_run_finishes_once() {
        let f = fixture(CHAIN);
        let id = |job: &str| task_of(&f.plan, job).to_string();
        let mut p = NeedsProgression::new(&f.plan).unwrap();
        p.step(Input::Advance).unwrap();
        ack_all(&mut p);
        // build has no result: its attempt was lost.
        p.step(Input::Fence {
            task: id("build"),
            status: JobStatus::Failure,
            reason: "no result".into(),
        })
        .unwrap();
        let ActionKind::Ready { task, inputs } = &p.pending_actions()[0].kind else {
            panic!()
        };
        assert_eq!(task, &id("test"), "the core decides whether it runs");
        let need = &inputs.needs["build"];
        assert_eq!(need.status, JobStatus::Failure);
        assert!(need.outputs.is_empty());
        assert_eq!(need.skipped, None, "a failure is not a skip");
        assert!(!need.ancestor_failed);
        ack_all(&mut p);

        // test comes back skipped by the core; lint succeeded.
        let mut skipped = outcome("test", JobStatus::Skipped);
        skipped.ancestor_failed = true;
        p.step(Input::Settle {
            task: id("test"),
            result: result(&id("test"), vec![skipped]),
        })
        .unwrap();
        assert!(ready(&p).is_empty());
        p.step(Input::Settle {
            task: id("lint"),
            result: result(&id("lint"), vec![outcome("lint", JobStatus::Success)]),
        })
        .unwrap();
        assert_eq!(ready(&p), [id("report")]);
        let last = p.pending_actions().last().unwrap();
        let ActionKind::Ready { inputs, .. } = &last.kind else {
            panic!()
        };
        assert_eq!(inputs.needs.len(), 2, "only the jobs it needs directly");
        assert!(inputs.needs["test"].ancestor_failed);
        ack_all(&mut p);
        // Cancelled before it ran: its jobs count as cancelled, with the
        // failure further up carried on.
        p.step(Input::Fence {
            task: id("report"),
            status: JobStatus::Cancelled,
            reason: "cancelled".into(),
        })
        .unwrap();
        let wf = &f.plan.tasks[0].workflow;
        let report = p.outcome(wf, "report").unwrap();
        assert_eq!(report.status, JobStatus::Cancelled);
        assert_eq!(report.skipped.as_deref(), Some("cancelled"));
        assert!(report.ancestor_failed);
        assert_eq!(
            p.pending_actions(),
            [Action {
                id: "finished".into(),
                kind: ActionKind::Finished
            }]
        );
        p.step(Input::Advance).unwrap();
        assert_eq!(p.pending_actions().len(), 1, "finished once");
        ack_all(&mut p);
        p.step(Input::Advance).unwrap();
        assert!(p.pending_actions().is_empty());
    }

    #[test]
    fn after_a_cancel_nothing_more_becomes_ready() {
        let f = fixture(CHAIN);
        let id = |job: &str| task_of(&f.plan, job).to_string();
        let mut p = NeedsProgression::new(&f.plan).unwrap();
        p.step(Input::Advance).unwrap();
        ack_all(&mut p);
        p.step(Input::Cancel).unwrap();
        p.step(Input::Settle {
            task: id("build"),
            result: result(&id("build"), vec![outcome("build", JobStatus::Success)]),
        })
        .unwrap();
        assert!(ready(&p).is_empty());
        for job in ["test", "lint", "report"] {
            p.step(Input::Fence {
                task: id(job),
                status: JobStatus::Cancelled,
                reason: "cancelled".into(),
            })
            .unwrap();
        }
        assert_eq!(p.pending_actions().len(), 1);
        assert_eq!(p.pending_actions()[0].kind, ActionKind::Finished);
    }

    #[test]
    fn what_is_not_a_result_of_the_task_is_refused_and_changes_nothing() {
        let f = fixture(CHAIN);
        let id = |job: &str| task_of(&f.plan, job).to_string();
        let mut p = NeedsProgression::new(&f.plan).unwrap();
        let settle = |p: &mut NeedsProgression, task: &str, r: Box<WireTaskResult>| {
            p.step(Input::Settle {
                task: task.into(),
                result: r,
            })
        };
        let ok = || outcome("build", JobStatus::Success);
        let other_task = settle(&mut p, &id("build"), result(&id("test"), vec![ok()]));
        assert!(
            matches!(other_task, Err(ProgressError::Result { .. })),
            "{other_task:?}"
        );
        let foreign = result(
            &id("build"),
            vec![ok(), outcome("test", JobStatus::Success)],
        );
        assert!(matches!(
            settle(&mut p, &id("build"), foreign),
            Err(ProgressError::Result { .. })
        ));
        let none = settle(&mut p, &id("build"), result(&id("build"), vec![]));
        assert!(matches!(none, Err(ProgressError::Result { .. })));
        let twice = settle(&mut p, &id("build"), result(&id("build"), vec![ok(), ok()]));
        assert!(matches!(twice, Err(ProgressError::Result { .. })));
        assert!(!p.is_settled(&id("build")));
        assert_eq!(
            settle(&mut p, "no such task", result("no such task", vec![])),
            Err(ProgressError::UnknownTask("no such task".into()))
        );
        settle(&mut p, &id("build"), result(&id("build"), vec![ok()])).unwrap();
        assert_eq!(
            settle(&mut p, &id("build"), result(&id("build"), vec![ok()])),
            Err(ProgressError::Settled(id("build")))
        );
        let fence = p.step(Input::Fence {
            task: id("build"),
            status: JobStatus::Failure,
            reason: String::new(),
        });
        assert_eq!(fence, Err(ProgressError::Settled(id("build"))));
        assert_eq!(
            p.step(Input::Ack {
                action: "ready:nothing".into()
            }),
            Err(ProgressError::UnknownAction("ready:nothing".into()))
        );
    }

    #[test]
    fn a_plan_the_progression_cannot_run_is_refused_with_the_task_and_the_reason() {
        let f = fixture(CHAIN);
        assert_eq!(unsupported(&f.plan), None);
        let id = task_of(&f.plan, "build").to_string();
        let at = f.plan.tasks.iter().position(|t| t.id == id).unwrap();
        let broken = |change: &dyn Fn(&mut ExecutionTask)| {
            let mut plan = f.plan.clone();
            change(&mut plan.tasks[at]);
            unsupported(&plan).unwrap()
        };
        assert!(broken(&|t| t.dynamic_matrix = true).contains("expression"));
        let variant = broken(&|t| {
            t.variant = Some(local_ci::dag::TaskVariant {
                group: "g".into(),
                index: 0,
                values: Default::default(),
                label: "l".into(),
            })
        });
        assert!(
            variant.contains("variant") && variant.contains(&id),
            "{variant}"
        );
        let services = broken(&|t| {
            t.services = vec![local_ci::dag::TaskService {
                job: "build".into(),
                id: "db".into(),
                image: "postgres".into(),
            }]
        });
        assert!(services.contains("service"), "{services}");
        let shard = broken(&|t| {
            let mut shard: local_ci::dag::TaskShard = serde_json::from_value(serde_json::json!({
                "group": "g", "index": 0, "count": 2, "mode": "single", "tests": [],
            }))
            .unwrap_or_else(|e| panic!("the shape of a shard changed: {e}"));
            shard.index = 1;
            t.shard = Some(shard);
        });
        assert!(shard.contains("shard"), "{shard}");
        let mut plan = f.plan.clone();
        plan.tasks[at].dynamic_matrix = true;
        assert!(matches!(
            NeedsProgression::new(&plan).err(),
            Some(ProgressError::Unsupported(_))
        ));
        let mut plan = f.plan.clone();
        plan.tasks[at].depends_on = vec!["gone".into()];
        assert_eq!(
            NeedsProgression::new(&plan).err(),
            Some(ProgressError::UnknownTask("gone".into()))
        );
        // A shard that is not what its split job names.
        let mut plan = crate::ci::shards::tests::split_plan(false);
        assert_eq!(unsupported(&plan), None);
        let at = plan.tasks.iter().position(|t| t.shard.is_some()).unwrap();
        plan.tasks[at].shard.as_mut().unwrap().count = 9;
        let why = unsupported(&plan).unwrap();
        assert!(why.contains("is not shard"), "{why}");
    }

    use crate::ci::shards::tests::{group, receipt_of, split_plan};
    use local_ci::shard::TestStatus;

    fn cancels(p: &NeedsProgression) -> Vec<(String, String)> {
        p.pending_actions()
            .iter()
            .filter_map(|a| match &a.kind {
                ActionKind::Cancel { task, reason } => Some((task.clone(), reason.clone())),
                _ => None,
            })
            .collect()
    }

    fn shard_result(plan: &FrozenPlan, at: usize, status: JobStatus, tests: TestStatus) -> Input {
        let g = group(plan);
        let mut result = result(&g.shards[at], vec![outcome("test", status)]);
        result.shard = Some(receipt_of(g, at, tests));
        Input::Settle {
            task: g.shards[at].clone(),
            result,
        }
    }

    #[test]
    fn the_shards_of_a_job_start_together_and_the_job_has_its_outcome_when_all_ended() {
        let plan = split_plan(false);
        let g = group(&plan).clone();
        let report = task_of(&plan, "report").to_string();
        let mut p = NeedsProgression::new(&plan).unwrap();
        p.step(Input::Advance).unwrap();
        // Both shards may be placed at once; what needs the job waits.
        assert_eq!(ready(&p), g.shards);
        ack_all(&mut p);

        p.step(shard_result(
            &plan,
            0,
            JobStatus::Success,
            TestStatus::Passed,
        ))
        .unwrap();
        // One shard is not the job: nothing is published, nothing is ready.
        assert!(p.pending_actions().is_empty());
        assert_eq!(p.outcome(&g.workflow, "test"), None);
        assert!(!p.dependencies_settled(&report));

        p.step(shard_result(
            &plan,
            1,
            JobStatus::Success,
            TestStatus::Passed,
        ))
        .unwrap();
        let job = p.outcome(&g.workflow, "test").unwrap().clone();
        assert_eq!(job.status, JobStatus::Success);
        assert_eq!(p.split_job_problems(&g.id), None);
        assert_eq!(job.outputs["v"], "1");
        let actions = p.pending_actions().to_vec();
        assert_eq!(actions.len(), 1);
        let ActionKind::Ready { task, inputs } = &actions[0].kind else {
            panic!("{actions:?}")
        };
        assert_eq!(task, &report);
        // The job is there once, as the one job it is.
        let expected = TaskInputs::new([&job], [&job], false);
        assert_eq!(inputs, &expected);
        assert!(cancels(&p).is_empty());
    }

    #[test]
    fn shards_that_passed_are_still_a_failed_job_when_a_test_is_missing() {
        let plan = split_plan(false);
        let g = group(&plan).clone();
        let mut p = NeedsProgression::new(&plan).unwrap();
        p.step(shard_result(
            &plan,
            0,
            JobStatus::Success,
            TestStatus::Passed,
        ))
        .unwrap();
        // The second shard says it passed, but leaves no receipt of its
        // tests (or one of another shard, which does not count).
        let mut forged = result(&g.shards[1], vec![outcome("test", JobStatus::Success)]);
        forged.shard = Some(receipt_of(&g, 0, TestStatus::Passed));
        p.step(Input::Settle {
            task: g.shards[1].clone(),
            result: forged,
        })
        .unwrap();
        let job = p.outcome(&g.workflow, "test").unwrap();
        assert_eq!(job.status, JobStatus::Failure);
        let why = job.skipped.clone().unwrap();
        assert!(why.contains("missing: shard"), "{why}");
        assert_eq!(p.split_job_problems(&g.id), Some(why.as_str()));
        assert_eq!(p.split_job_problems("another"), None);
        // What needs the job is given the failure.
        let last = p.pending_actions().last().unwrap();
        let ActionKind::Ready { inputs, .. } = &last.kind else {
            panic!()
        };
        let expected = TaskInputs::new([job], [job], false);
        assert_eq!(inputs, &expected);
    }

    #[test]
    fn with_fail_fast_a_failed_shard_stops_the_ones_that_have_not_ended_once() {
        let plan = split_plan(true);
        let g = group(&plan).clone();
        let mut p = NeedsProgression::new(&plan).unwrap();
        p.step(Input::Advance).unwrap();
        ack_all(&mut p);
        p.step(shard_result(
            &plan,
            0,
            JobStatus::Failure,
            TestStatus::Failed,
        ))
        .unwrap();
        assert_eq!(
            cancels(&p),
            [(
                g.shards[1].clone(),
                crate::ci::shards::FAIL_FAST_REASON.to_string()
            )]
        );
        assert_eq!(p.pending_actions()[0].id, format!("cancel:{}", g.shards[1]));
        // Asked once, however often the run is looked at.
        ack_all(&mut p);
        p.step(Input::Advance).unwrap();
        assert!(p.pending_actions().is_empty());
        // The stopped shard ends without a result: the job is a failure,
        // and what needs it goes on.
        p.step(Input::Fence {
            task: g.shards[1].clone(),
            status: JobStatus::Cancelled,
            reason: "cancelled".into(),
        })
        .unwrap();
        let job = p.outcome(&g.workflow, "test").unwrap();
        assert_eq!(job.status, JobStatus::Failure);
        assert_eq!(ready(&p), [task_of(&plan, "report").to_string()]);

        // A shard that fails only by its tests (the job step did not fail)
        // stops the others too; one that passed stops nothing.
        let mut p = NeedsProgression::new(&plan).unwrap();
        p.step(shard_result(
            &plan,
            1,
            JobStatus::Success,
            TestStatus::Failed,
        ))
        .unwrap();
        assert_eq!(cancels(&p).len(), 1);
        assert_eq!(cancels(&p)[0].0, g.shards[0]);
        let mut p = NeedsProgression::new(&plan).unwrap();
        p.step(shard_result(
            &plan,
            1,
            JobStatus::Success,
            TestStatus::Passed,
        ))
        .unwrap();
        assert!(cancels(&p).is_empty());
        // Without fail-fast the other shard goes on.
        let plain = split_plan(false);
        let mut p = NeedsProgression::new(&plain).unwrap();
        p.step(shard_result(
            &plain,
            0,
            JobStatus::Failure,
            TestStatus::Failed,
        ))
        .unwrap();
        assert!(cancels(&p).is_empty());
    }
}
