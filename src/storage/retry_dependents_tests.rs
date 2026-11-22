//! R19: a retry of one job of a CI run, and the jobs that need it.

use super::*;
use crate::ci::execute::tests::fixture;
use crate::ids::*;
use crate::service::completion::Target;
use crate::state::{Exit, Outcome, Termination};
use serde_json::{json, Value};

const CHAIN: &str = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo a\n  b:\n    needs: a\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo b\n  c:\n    needs: b\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo c\n";

struct Chain {
    _dir: tempfile::TempDir,
    store: CoordinatorStore,
    run: RunId,
    a: TaskId,
    b: TaskId,
    c: TaskId,
}

fn digest() -> crate::ids::Digest {
    crate::ids::Digest::parse(&format!("sha256:{}", "a".repeat(64))).unwrap()
}

/// A run of the chain a, b, c: every task is a row of the run, held while it
/// waits for the job it needs.
fn chain() -> Chain {
    let f = fixture(CHAIN);
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let (plan, _) = crate::ci::advance::keep_plan(dir.path(), &f.plan).unwrap();
    let run = RunId::generate();
    let source = SourceId::generate();
    store.create_run(&run, None, None, &json!({}), 1).unwrap();
    store.put_source_ref(&source, &digest(), 1).unwrap();
    let (r, s) = (run.to_string(), source.to_string());
    store
        .db
        .write(move |tx| {
            tx.execute(
                "UPDATE run SET source_id=?1 WHERE run_id=?2",
                rusqlite::params![s, r],
            )?;
            Ok(())
        })
        .unwrap();
    let specs =
        crate::service::ci_run::task_specs(&f.plan, &plan, &digest(), &Default::default(), None)
            .unwrap();
    let mut ids = std::collections::BTreeMap::new();
    for (name, spec) in specs {
        let id = TaskId::generate();
        let mut spec = serde_json::to_value(&spec).unwrap();
        spec["manifest_digest"] = json!(digest().to_string());
        spec["has_secret_bundle"] = json!(false);
        store.create_task(&id, &run, &spec, 2).unwrap();
        ids.insert(name.rsplit("::").next().unwrap().to_string(), id);
    }
    Chain {
        _dir: dir,
        store,
        run,
        a: ids["a"].clone(),
        b: ids["b"].clone(),
        c: ids["c"].clone(),
    }
}

impl Chain {
    fn attempt(&self, task: &TaskId) -> AttemptRecord {
        self.store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: task.clone(),
                    worker_id: WorkerId::parse("mini-a").unwrap(),
                    payload_hash: PayloadHash::parse(&format!("sha256:{}", "b".repeat(64)))
                        .unwrap(),
                    permit_id: None,
                },
                10,
            )
            .unwrap()
    }

    /// The task ran and ended.
    fn end(&self, task: &TaskId, outcome: Outcome) {
        use crate::state::AttemptState::*;
        let a = self.attempt(task);
        for s in [Preparing, Ready, Starting, Active, Finalizing] {
            self.store.transition_attempt(&a.attempt_id, s, 20).unwrap();
        }
        let code = if outcome == Outcome::Succeeded { 0 } else { 1 };
        let termination = Termination::new(outcome, Some(Exit::Code(code)), None).unwrap();
        self.store
            .finalize_attempt(&a.attempt_id, termination, 30)
            .unwrap();
    }

    /// The task ended without a step: the result of its attempt says every job
    /// of it was skipped.
    fn end_skipped(&self, task: &TaskId) {
        use local_ci::task::{JobOutcome, JobStatus};
        self.end(task, Outcome::Succeeded);
        let attempt = self.store.attempts_of_task(task).unwrap().pop().unwrap();
        let result = local_ci::WireTaskResult {
            schema_version: local_ci::wire::WIRE_SCHEMA_VERSION,
            task: "t".into(),
            jobs: vec![],
            outcomes: vec![JobOutcome {
                job: "t".into(),
                variant: None,
                status: JobStatus::Skipped,
                outputs: Default::default(),
                skipped: Some("needs failed".into()),
                ancestor_failed: true,
            }],
            receipt_ignored: None,
            artifacts: vec![],
            shard: None,
        };
        let bytes = serde_json::to_vec(&result).unwrap();
        let digest = crate::ids::Digest::of_bytes(&bytes);
        let text = digest.to_string();
        let dir = self
            ._dir
            .path()
            .join("results")
            .join(attempt.attempt_id.as_str())
            .join("artifacts");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(text.strip_prefix("sha256:").unwrap()), &bytes).unwrap();
        let id = attempt.attempt_id.to_string();
        self.store
            .db
            .write(move |tx| {
                tx.execute(
                    "INSERT INTO artifact_receipt(attempt_id,name,required,digest,persisted_at_ms) VALUES(?1,?2,0,?3,1)",
                    rusqlite::params![id, crate::supervisor::ci_task::RESULT_ARTIFACT, text],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn row(&self, task: &TaskId) -> crate::storage::CiTaskRow {
        let rows = self.store.ci_run_tasks(&self.run).unwrap();
        rows.into_iter().find(|r| &r.task_id == task).unwrap()
    }

    /// The progression let the task in with the results of what it needs.
    fn release(&self, task: &TaskId) {
        let mut spec = self.spec(task);
        spec["ci"]["hold"] = json!(false);
        spec["ci"]["task"]["inputs"]["needs"] = json!({"a": {"job": "a"}});
        assert!(self.store.release_ci_task(task, &spec, 40).unwrap());
    }

    fn spec(&self, task: &TaskId) -> Value {
        let rows = self.store.ci_run_tasks(&self.run).unwrap();
        rows.into_iter().find(|r| &r.task_id == task).unwrap().spec
    }

    fn held(&self, task: &TaskId) -> bool {
        self.spec(task)["ci"]["hold"] == json!(true)
    }

    fn retry(
        &self,
        target: Target,
        expected: u64,
    ) -> Result<crate::service::completion::RetryReceipt> {
        self.store
            .retry(target, expected, format!("r{expected}"), 50, |_, _, _| {
                Ok(false)
            })
    }

    /// A ended; b is let in; the earlier results are used or not by `b`.
    fn a_ended_and_b_released(&self) {
        self.end(&self.a, Outcome::Succeeded);
        self.release(&self.b);
    }
}

fn refusal(error: StorageError, b: &TaskId, a: &TaskId) {
    let StorageError::Immutable(text) = error else {
        panic!("{error:?}")
    };
    assert!(
        text.contains(&format!("retry of {a} would leave {b}")),
        "{text}"
    );
    assert!(
        text.contains(&format!("retry {b} as well or retry the run")),
        "{text}"
    );
}

#[test]
fn a_released_job_that_did_not_start_waits_again_for_the_retried_job() {
    let f = chain();
    f.a_ended_and_b_released();
    assert!(!f.held(&f.b));
    let receipt = f.retry(Target::Task(f.a.clone()), 0).unwrap();
    assert_eq!(receipt.task_count, 1);
    assert!(f.held(&f.b), "b waits for the new result of a");
    assert!(f.held(&f.c), "c still waits");
    assert!(f
        .store
        .queued_tasks(10)
        .unwrap()
        .iter()
        .all(|t| t.task_id != f.b));
}

#[test]
fn a_job_that_runs_with_the_earlier_results_refuses_the_retry_and_changes_nothing() {
    let f = chain();
    f.a_ended_and_b_released();
    f.attempt(&f.b);
    refusal(
        f.retry(Target::Task(f.a.clone()), 0).unwrap_err(),
        &f.b,
        &f.a,
    );
    assert!(!f.held(&f.b));
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .control_revision,
        0
    );
}

#[test]
fn a_job_that_ended_with_the_earlier_results_refuses_the_retry_of_the_job_it_needs() {
    let f = chain();
    f.a_ended_and_b_released();
    f.end(&f.b, Outcome::Succeeded);
    refusal(
        f.retry(Target::Task(f.a.clone()), 0).unwrap_err(),
        &f.b,
        &f.a,
    );
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .control_revision,
        0
    );
    // The same request covers both when the run is retried: both run again.
    let f = chain();
    f.end(&f.a, Outcome::Failed);
    f.release(&f.b);
    f.end(&f.b, Outcome::Failed);
    f.end(&f.c, Outcome::Failed);
    let receipt = f.retry(Target::Run(f.run.clone()), 0).unwrap();
    assert_eq!(receipt.task_count, 3);
    assert!(f.held(&f.b), "b waits for the new result of a");
}

#[test]
fn a_retry_of_the_dependent_alone_is_not_refused() {
    let f = chain();
    f.a_ended_and_b_released();
    f.end(&f.b, Outcome::Failed);
    let receipt = f.retry(Target::Task(f.b.clone()), 0).unwrap();
    assert_eq!(receipt.task_count, 1);
}

/// a failed, so b and c ended without a step (skipped).
fn skipped_chain() -> Chain {
    let f = chain();
    f.end(&f.a, Outcome::Failed);
    f.release(&f.b);
    f.end_skipped(&f.b);
    f.release(&f.c);
    f.end_skipped(&f.c);
    f
}

#[test]
fn jobs_that_were_skipped_for_the_failed_job_are_opened_again_with_its_retry() {
    let f = skipped_chain();
    let receipt = f.retry(Target::Task(f.a.clone()), 0).unwrap();
    assert_eq!(receipt.task_count, 1);
    for task in [&f.b, &f.c] {
        let row = f.row(task);
        assert!(
            row.attempt.is_none(),
            "the skipped attempt no longer counts"
        );
        assert!(f.held(task), "waits for the new result of a");
    }
    assert!(f
        .store
        .queued_tasks(10)
        .unwrap()
        .iter()
        .all(|t| t.task_id != f.b && t.task_id != f.c));
}

#[test]
fn a_skipped_job_behind_a_job_that_ran_keeps_the_refusal() {
    let f = chain();
    f.end(&f.a, Outcome::Failed);
    f.release(&f.b);
    f.end(&f.b, Outcome::Succeeded);
    refusal(
        f.retry(Target::Task(f.a.clone()), 0).unwrap_err(),
        &f.b,
        &f.a,
    );
}

#[test]
fn a_retry_of_the_run_opens_the_skipped_jobs_again_and_keeps_the_one_that_ran() {
    let f = skipped_chain();
    let receipt = f.retry(Target::Run(f.run.clone()), 0).unwrap();
    assert_eq!(receipt.task_count, 3, "a, and b and c opened again");
    assert!(f.row(&f.b).attempt.is_none() && f.row(&f.c).attempt.is_none());
    // A job that ran with the failed result (`if: always()`) is kept.
    let f = chain();
    f.end(&f.a, Outcome::Failed);
    f.release(&f.b);
    f.end(&f.b, Outcome::Succeeded);
    f.release(&f.c);
    f.end(&f.c, Outcome::Succeeded);
    let receipt = f.retry(Target::Run(f.run.clone()), 0).unwrap();
    assert_eq!(receipt.task_count, 1);
    assert!(f.row(&f.b).attempt.is_some());
}
