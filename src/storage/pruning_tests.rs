//! Spec 25.3 (b): the mark of a run that is being pruned
//! (`run.pruning_at_ms`). Nothing writes the mark yet, so every test here
//! writes it directly.

use super::*;
use crate::error::ErrorCode;
use crate::ids::*;
use crate::results::pin::RunPin;
use crate::service::completion::Target;
use crate::service::query::{Query, Response};
use crate::state::{AttemptState, Outcome, Termination};
use serde_json::json;
use std::fmt::Debug;

struct Fx {
    dir: tempfile::TempDir,
    store: CoordinatorStore,
    run: RunId,
    /// Has one attempt, which ended (failed).
    task: TaskId,
    attempt: AttemptId,
    /// Has no attempt: it waits to be placed.
    waiting: TaskId,
}

fn attempt_of(store: &CoordinatorStore, task: &TaskId, at: u64) -> Result<AttemptRecord> {
    store.create_attempt(
        NewAttempt {
            attempt_id: AttemptId::generate(),
            task_id: task.clone(),
            worker_id: WorkerId::parse("mini-a").unwrap(),
            payload_hash: Digest::of_bytes(b"payload"),
            permit_id: None,
        },
        at,
    )
}

/// A run bound to a source, so that it can be retried, with a task that
/// ended and a task that was not placed yet.
fn run_in(store: &CoordinatorStore) -> (RunId, TaskId, AttemptId, TaskId) {
    let (run, task, waiting) = (RunId::generate(), TaskId::generate(), TaskId::generate());
    let digest = format!("sha256:{}", "a".repeat(64));
    let source = SourceId::generate();
    store
        .put_source_ref(&source, &ManifestDigest::parse(&digest).unwrap(), 1)
        .unwrap();
    store
        .create_run(&run, Some(&source), None, &json!({}), 1)
        .unwrap();
    let spec = json!({"manifest_digest": digest, "has_secret_bundle": false});
    store.create_task(&task, &run, &spec, 1).unwrap();
    store.create_task(&waiting, &run, &spec, 2).unwrap();
    let attempt = attempt_of(store, &task, 3).unwrap().attempt_id;
    for state in [
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
        AttemptState::Finalizing,
    ] {
        store.transition_attempt(&attempt, state, 4).unwrap();
    }
    let failed =
        Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(1)), None).unwrap();
    store.finalize_attempt(&attempt, failed, 5).unwrap();
    (run, task, attempt, waiting)
}

fn fx() -> Fx {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let (run, task, attempt, waiting) = run_in(&store);
    Fx {
        dir,
        store,
        run,
        task,
        attempt,
        waiting,
    }
}

impl CoordinatorStore {
    /// What the pruning of a run will do first: write the mark.
    pub(crate) fn mark_pruning_for_test(&self, run: &RunId) {
        let run = run.to_string();
        let marked = self
            .db
            .write(move |tx| {
                Ok(tx.execute(
                    "UPDATE run SET pruning_at_ms=99 WHERE run_id=?1 AND pruning_at_ms IS NULL",
                    [run],
                )?)
            })
            .unwrap();
        assert_eq!(marked, 1);
    }
    /// What the pruning does later, a part of it: a task that has no
    /// attempt loses its row while the rest of its run still has its rows.
    pub(crate) fn delete_task_for_test(&self, task: &TaskId) {
        let task = task.to_string();
        let gone = self
            .db
            .write(move |tx| Ok(tx.execute("DELETE FROM task WHERE task_id=?1", [task])?))
            .unwrap();
        assert_eq!(gone, 1);
    }
}

fn mark(store: &CoordinatorStore, run: &RunId) {
    store.mark_pruning_for_test(run);
}

fn delete_half(f: &Fx) {
    f.store.delete_task_for_test(&f.waiting);
}

/// The answer for a run that carries the mark: `NOT_FOUND`, reason `run_pruned`.
#[track_caller]
fn refused<T: Debug>(answer: Result<T>) {
    let error = answer.expect_err("a run that is being pruned must be refused");
    assert_eq!(error.code(), ErrorCode::NotFound, "{error}");
    assert!(error.to_string().contains(RUN_PRUNED), "{error}");
}

fn pin_path(f: &Fx) -> std::path::PathBuf {
    f.dir.path().join(format!("reader-pins/{}.lock", f.run))
}

/// (i) Every write to a marked run is refused with `run_pruned`, and no row
/// is made or changed by the refused call.
#[test]
fn nothing_is_written_to_a_run_that_is_being_pruned() {
    let f = fx();
    mark(&f.store, &f.run);
    let before = f.store.snapshot_rows_for_test();
    let run = Target::Run(f.run.clone());
    let task = Target::Task(f.task.clone());

    refused(
        f.store
            .retry(run.clone(), 0, "retry-run".into(), 20, |_, _, _| Ok(false)),
    );
    refused(
        f.store
            .retry(task.clone(), 0, "retry-task".into(), 20, |_, _, _| {
                Ok(false)
            }),
    );
    refused(f.store.enqueue(
        &f.run,
        None,
        &json!({}),
        &[(TaskId::generate(), json!({}))],
        20,
    ));
    refused(
        f.store
            .create_task(&TaskId::generate(), &f.run, &json!({}), 20),
    );
    refused(attempt_of(&f.store, &f.waiting, 20));
    refused(
        f.store
            .cancel_control(run.clone(), 0, "cancel-run".into(), 20),
    );
    refused(f.store.cancel_control(task, 0, "cancel-task".into(), 20));
    refused(f.store.cancel_run(&f.run, 20));
    refused(f.store.cancel_task(&f.task, 20));
    // The writes of a CI run that make tasks, and the record of a status.
    refused(f.store.expand_ci_matrix(
        &f.run,
        &f.waiting,
        0,
        vec![(TaskId::generate(), json!({}))],
        20,
    ));
    let digest = Digest::of_bytes(b"plan");
    refused(
        f.store
            .apply_ci_discovery(&f.run, &digest, &digest, vec![], vec![], 20),
    );
    refused(f.store.record_github_status(&f.run, &json!({}), 20));
    // The task that waits is not one a placement may take.
    assert!(f.store.queued_tasks(10).unwrap().is_empty());
    assert!(!f.store.estimate_queued(&f.waiting).unwrap());

    assert_eq!(f.store.snapshot_rows_for_test(), before, "no row changed");
}

/// Every read of a run, as the commands make them (`show`, `ls`, `logs`,
/// `outputs`, `top`, the status of a CI run, the scans of the resident).
fn assert_not_found_by_any_reader(f: &Fx, reader: &CoordinatorStore) {
    let run = Target::Run(f.run.clone());
    let task = Target::Task(f.task.clone());
    // `show` of the run and of a task, `ls`.
    assert!(reader.query_run(f.run.clone()).unwrap().is_none());
    assert!(matches!(
        reader
            .query(Query::Run {
                run_id: f.run.clone()
            })
            .unwrap(),
        Response::Run { run: None }
    ));
    refused(reader.query_tasks(f.run.clone(), 10, None));
    assert!(reader.query_task(f.task.clone()).unwrap().is_none());
    refused(reader.query_attempts(f.task.clone(), 10, None));
    assert!(reader.query_runs(10, None).unwrap().items.is_empty());
    // What `logs` and `outputs` wait on and read.
    refused(reader.completion(run.clone()));
    refused(reader.completion(task.clone()));
    refused(reader.query(Query::OutputMetadata {
        task_id: f.task.clone(),
        attempt_id: f.attempt.clone(),
        after: None,
        limit: 10,
    }));
    let logs = reader.query(Query::LogMetadata {
        task_id: f.task.clone(),
        attempt_id: f.attempt.clone(),
    });
    assert_eq!(logs.unwrap_err().code(), ErrorCode::NotFound);
    // Their pin, for the run, the task and the attempt.
    refused(reader.result_read_pin(&run, None));
    refused(reader.result_read_pin(&task, None));
    refused(reader.result_read_pin(&task, Some(&f.attempt)));
    // `top`, the status of a CI run, and whether a run carries the mark.
    assert!(reader.monitor_tasks().unwrap().is_empty());
    refused(reader.ci_run_tasks(&f.run));
    assert!(reader.run_is_pruned(&f.run).unwrap());
    // The scans of the resident: what may be placed, what a CI pass advances.
    assert!(f.store.queued_tasks(10).unwrap().is_empty());
    assert!(f.store.ci_waiting_runs(10).unwrap().is_empty());
}

/// (ii) No reader finds a marked run. (iii) With half of its rows deleted
/// the answer is the same "not found", and never a verdict on what is left
/// (an empty page, a task that is "missing").
#[test]
fn no_reader_finds_a_run_that_is_being_pruned_whatever_is_left_of_its_rows() {
    let f = fx();
    mark(&f.store, &f.run);
    let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
    for store in [&f.store, &reader] {
        assert_not_found_by_any_reader(&f, store);
    }
    delete_half(&f);
    for store in [&f.store, &reader] {
        assert_not_found_by_any_reader(&f, store);
    }
}

/// (ii), for the two functions that make the pin file of a run (spec 25.5):
/// neither makes the pin of a marked run again.
#[test]
fn the_pin_of_a_run_that_is_being_pruned_is_not_made_again() {
    let f = fx();
    drop(f.store.result_writer_pin(&f.attempt).unwrap());
    drop(crate::test_tmp::gc_guard(&f.store, &f.run));
    mark(&f.store, &f.run);
    std::fs::remove_file(pin_path(&f)).unwrap();

    refused(f.store.result_writer_pin(&f.attempt));
    refused(f.store.result_gc_guard(&f.run));
    assert!(!pin_path(&f).exists());
    // A run without a row has no pin made for it either.
    let unknown = RunId::generate();
    let error = f.store.result_gc_guard(&unknown).unwrap_err();
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(!f
        .dir
        .path()
        .join(format!("reader-pins/{unknown}.lock"))
        .exists());
}

/// (iv) A run without the mark is read and written as before, also next to
/// a run that carries it.
#[test]
fn a_run_without_the_mark_is_read_and_written_as_before() {
    let f = fx();
    let (other, _, _, _) = run_in(&f.store);
    mark(&f.store, &other);
    let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
    let run = Target::Run(f.run.clone());

    let listed = reader.query_runs(10, None).unwrap().items;
    assert_eq!(listed.len(), 1, "the marked run is not listed");
    assert_eq!(listed[0].run_id, f.run);
    assert_eq!(
        reader.query_run(f.run.clone()).unwrap().unwrap().task_count,
        2
    );
    assert_eq!(
        reader
            .query_tasks(f.run.clone(), 10, None)
            .unwrap()
            .items
            .len(),
        2
    );
    assert!(reader.query_task(f.task.clone()).unwrap().is_some());
    assert_eq!(
        reader
            .query_attempts(f.task.clone(), 10, None)
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(reader.completion(run.clone()).unwrap().task_count, 2);
    assert_eq!(reader.ci_run_tasks(&f.run).unwrap().len(), 2);
    assert_eq!(
        reader.monitor_tasks().unwrap().len(),
        1,
        "the task that waits"
    );
    assert!(!reader.run_is_pruned(&f.run).unwrap());
    drop(reader.result_read_pin(&run, None).unwrap());
    drop(
        reader
            .result_read_pin(&Target::Task(f.task.clone()), Some(&f.attempt))
            .unwrap(),
    );
    drop(f.store.result_writer_pin(&f.attempt).unwrap());
    drop(crate::test_tmp::gc_guard(&f.store, &f.run));
    let queued = f.store.queued_tasks(10).unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].task_id, f.waiting);

    f.store
        .create_task(&TaskId::generate(), &f.run, &json!({}), 20)
        .unwrap();
    attempt_of(&f.store, &f.waiting, 21).unwrap();
    let retried = f
        .store
        .retry(
            Target::Task(f.task.clone()),
            0,
            "retry-one".into(),
            22,
            |_, _, _| Ok(false),
        )
        .unwrap();
    assert_eq!(retried.control_revision, 1);
    let cancelled = f
        .store
        .cancel_control(run, 1, "cancel-one".into(), 23)
        .unwrap();
    assert!(cancelled.cancel_requested);
    let fresh = RunId::generate();
    f.store
        .enqueue(
            &fresh,
            None,
            &json!({}),
            &[(TaskId::generate(), json!({}))],
            24,
        )
        .unwrap();
}

/// (v) A cancel and a retry were accepted and left their receipts. Once the
/// run carries the mark, the same requests sent again are answered
/// `run_pruned`, not from the receipts, which are still there.
#[test]
fn a_request_sent_again_to_a_run_that_is_being_pruned_is_not_answered_from_its_receipt() {
    let f = fx();
    let run = Target::Run(f.run.clone());
    let task = Target::Task(f.task.clone());
    let retry = |at| {
        f.store
            .retry(task.clone(), 0, "retry-one".into(), at, |_, _, _| Ok(false))
    };
    let cancel = |at| {
        f.store
            .cancel_control(run.clone(), 1, "cancel-one".into(), at)
    };
    let retried = retry(20).unwrap();
    let cancelled = cancel(21).unwrap();
    // Without the mark, a request sent again gets the stored answer.
    assert_eq!(json!(retry(22).unwrap()), json!(retried));
    assert_eq!(json!(cancel(23).unwrap()), json!(cancelled));

    mark(&f.store, &f.run);
    let before = f.store.snapshot_rows_for_test();
    refused(retry(24));
    refused(cancel(25));
    let receipts = before
        .iter()
        .find(|(table, _)| table == "control_request")
        .map(|(_, rows)| rows.len());
    assert_eq!(receipts, Some(2), "both receipts are still stored");
    assert_eq!(f.store.snapshot_rows_for_test(), before);
}

/// (vi) A reader of the files of a run (`logs`, `outputs`) passed its first
/// check. Before it takes the shared pin, the pruning writes the mark, takes
/// the pin exclusively, deletes the files and lets go of the pin. The reader
/// then gets the shared pin, and is refused by the check it makes with the
/// pin held: it is handed no pin, so it opens none of the deleted files.
#[test]
fn a_reader_overtaken_by_the_pruning_before_its_pin_is_refused_and_opens_no_file() {
    for target in [true, false] {
        let f = fx();
        let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
        let (target, attempt) = if target {
            (Target::Run(f.run.clone()), None)
        } else {
            (Target::Task(f.task.clone()), Some(&f.attempt))
        };
        let file = f.dir.path().join("result-of-the-run");
        std::fs::write(&file, b"result").unwrap();
        let mut overtaken = false;
        let answer = reader.result_read_pin_after(&target, attempt, || {
            mark(&f.store, &f.run);
            let exclusive = RunPin::exclusive(f.dir.path(), &f.run).unwrap();
            std::fs::remove_file(&file).unwrap();
            drop(exclusive);
            overtaken = true;
        });
        assert!(overtaken, "the first check passed: the run had no mark");
        refused(answer);
        // The refused reader let go of its pin: the pruning takes it again.
        drop(RunPin::exclusive(f.dir.path(), &f.run).unwrap());
    }
}

/// The other order: a reader that holds its pin keeps the pruning from the
/// files, whatever is marked meanwhile.
#[test]
fn a_reader_that_holds_its_pin_keeps_the_pruning_from_the_files() {
    let f = fx();
    let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
    let pin = reader
        .result_read_pin(&Target::Run(f.run.clone()), None)
        .unwrap();
    mark(&f.store, &f.run);
    let error = RunPin::exclusive(f.dir.path(), &f.run).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    drop(pin);
    drop(RunPin::exclusive(f.dir.path(), &f.run).unwrap());
}

/// A file from before the mark, read as it is by a newer command while its
/// owner was not migrated yet: no run of it is marked, and every reader
/// answers without the column.
#[test]
fn a_file_from_before_the_mark_is_read_as_it_is() {
    let f = fx();
    let steps = schema::steps(DbKind::Coordinator);
    assert!(
        steps.last().unwrap().contains("pruning_at_ms"),
        "the mark is the last step: rewind to the step this test means"
    );
    let previous = (steps.len() - 1).to_string();
    f.store
        .db
        .write(move |tx| {
            tx.execute_batch("ALTER TABLE run DROP COLUMN pruning_at_ms;")?;
            tx.execute(
                "UPDATE schema_meta SET value=?1 WHERE key='schema_version'",
                [previous],
            )?;
            Ok(())
        })
        .unwrap();
    let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
    let run = Target::Run(f.run.clone());
    assert_eq!(reader.query_runs(10, None).unwrap().items.len(), 1);
    assert!(reader.query_run(f.run.clone()).unwrap().is_some());
    assert_eq!(
        reader
            .query_tasks(f.run.clone(), 10, None)
            .unwrap()
            .items
            .len(),
        2
    );
    assert!(reader.query_task(f.task.clone()).unwrap().is_some());
    assert_eq!(
        reader
            .query_attempts(f.task.clone(), 10, None)
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(reader.completion(run.clone()).unwrap().task_count, 2);
    assert_eq!(reader.ci_run_tasks(&f.run).unwrap().len(), 2);
    assert_eq!(reader.monitor_tasks().unwrap().len(), 1);
    assert!(!reader.run_is_pruned(&f.run).unwrap());
    assert!(reader.estimate_queued(&f.waiting).unwrap());
    assert!(reader
        .query(Query::OutputMetadata {
            task_id: f.task.clone(),
            attempt_id: f.attempt.clone(),
            after: None,
            limit: 10,
        })
        .is_ok());
    drop(reader.result_read_pin(&run, None).unwrap());
}

/// The step that adds the mark keeps the runs of an older file, and none of
/// them carries it.
#[test]
fn the_migration_that_adds_the_mark_leaves_every_run_without_it() {
    use super::policy::Access;
    let root = crate::test_tmp::tempdir().unwrap();
    let path = root.path().join("coordinator.db");
    let steps = schema::steps(DbKind::Coordinator);
    assert_eq!(
        steps.last().unwrap(),
        "ALTER TABLE run ADD COLUMN pruning_at_ms INTEGER;"
    );
    assert_eq!(steps.len(), 23);
    let previous = steps.len() - 1;
    let run = RunId::generate();
    let old = db::open_with(
        &path,
        DbKind::Coordinator.as_str(),
        &steps[..previous],
        schema::compat(previous),
        Access::ReadWrite,
    )
    .unwrap();
    let id = run.to_string();
    old.write(move |tx| {
        tx.execute(
            "INSERT INTO run (run_id, spec_json, created_at_ms) VALUES (?1,'{}',1)",
            [id],
        )?;
        Ok(())
    })
    .unwrap();
    drop(old);
    let store = CoordinatorStore::open(root.path(), 50).unwrap();
    assert!(!store.run_is_pruned(&run).unwrap());
    assert!(store.query_run(run.clone()).unwrap().is_some());
    mark(&store, &run);
    assert!(store.run_is_pruned(&run).unwrap());
    assert!(store.query_run(run).unwrap().is_none());
}

/// What a reader that reads several times asks after its last read: the run
/// (and the task it names) has its row and no mark. A run whose pruning ran
/// to its end has no row and no mark to see, and is not whole either.
#[test]
fn a_run_is_whole_only_with_its_row_and_without_the_mark() {
    let f = fx();
    let (other, other_task, _, _) = run_in(&f.store);
    assert!(f.store.run_is_whole(&f.run).unwrap());
    assert!(f.store.task_is_whole(&f.task).unwrap());
    assert!(f.store.task_is_whole(&f.waiting).unwrap());
    mark(&f.store, &f.run);
    assert!(!f.store.run_is_whole(&f.run).unwrap());
    assert!(!f.store.task_is_whole(&f.task).unwrap());
    // Rows that are gone, with or without a mark left to see.
    delete_half(&f);
    assert!(!f.store.task_is_whole(&f.waiting).unwrap());
    assert!(!f.store.run_is_whole(&RunId::generate()).unwrap());
    assert!(!f.store.task_is_whole(&TaskId::generate()).unwrap());
    assert!(f.store.run_is_whole(&other).unwrap());
    assert!(f.store.task_is_whole(&other_task).unwrap());
}
