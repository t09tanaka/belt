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

impl CoordinatorStore {
    /// What the pruning does last: the attempts, the tasks and the row of
    /// the run are gone, and no mark is left to see.
    pub(crate) fn delete_run_for_test(&self, run: &RunId) {
        let run = run.to_string();
        let gone = self
            .db
            .write(move |tx| {
                tx.execute_batch("PRAGMA defer_foreign_keys=ON")?;
                tx.execute(
                    "DELETE FROM attempt WHERE task_id IN (SELECT task_id FROM task WHERE run_id=?1)",
                    [&run],
                )?;
                tx.execute("DELETE FROM task WHERE run_id=?1", [&run])?;
                Ok(tx.execute("DELETE FROM run WHERE run_id=?1", [&run])?)
            })
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
    // A task of a CI run that waits for the jobs it needs: without the mark
    // the scan of the resident finds its run.
    f.store
        .create_task(
            &TaskId::generate(),
            &f.run,
            &json!({"ci": {"hold": true}}),
            6,
        )
        .unwrap();
    assert_eq!(f.store.ci_waiting_runs(10).unwrap(), vec![f.run.clone()]);
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

/// Spec 25.5, the after-pin check of the writer of results. The writer
/// passed its first check. Before it makes the pin file, the pruning writes
/// the mark, takes the pin exclusively, deletes what it deletes, unlinks the
/// pin file and lets go. The writer makes the file again and takes it, and
/// is refused by the check it makes with the pin held: it is handed no pin
/// (so it writes no result), and the file it made is gone again.
#[test]
fn a_writer_of_results_overtaken_by_the_pruning_is_refused_and_leaves_no_pin() {
    for rows_gone in [false, true] {
        let f = fx();
        drop(f.store.result_writer_pin(&f.attempt).unwrap());
        let mut overtaken = false;
        let answer = f.store.result_writer_pin_after(&f.attempt, || {
            mark(&f.store, &f.run);
            let exclusive = RunPin::exclusive(f.dir.path(), &f.run).unwrap();
            if rows_gone {
                f.store.delete_run_for_test(&f.run);
            }
            std::fs::remove_file(pin_path(&f)).unwrap();
            drop(exclusive);
            overtaken = true;
        });
        assert!(overtaken, "the first check passed: the run had no mark");
        let error = answer.expect_err("a writer overtaken by the pruning must be refused");
        assert_eq!(error.code(), ErrorCode::NotFound, "{error}");
        assert_eq!(
            error.to_string().contains(RUN_PRUNED),
            !rows_gone,
            "{error}"
        );
        assert!(!pin_path(&f).exists(), "rows gone: {rows_gone}");
    }
    // The mark alone, with the pin file where it was: the refused writer
    // cannot tell that file from one it made, and unlinks it as well.
    let f = fx();
    drop(f.store.result_writer_pin(&f.attempt).unwrap());
    refused(
        f.store
            .result_writer_pin_after(&f.attempt, || mark(&f.store, &f.run)),
    );
    assert!(!pin_path(&f).exists());
}

/// The writer that is refused with the pin held unlinks the file only when
/// it holds it alone: a file somebody else holds is left where it is.
#[test]
fn a_refused_writer_of_results_leaves_a_pin_that_somebody_holds() {
    let f = fx();
    drop(f.store.result_writer_pin(&f.attempt).unwrap());
    let mut held = None;
    let answer = f.store.result_writer_pin_after(&f.attempt, || {
        mark(&f.store, &f.run);
        held = Some(RunPin::shared(f.dir.path(), &f.run).unwrap());
    });
    refused(answer);
    assert!(pin_path(&f).exists());
    // The refused writer let go of its own pin.
    drop(held);
    drop(RunPin::exclusive(f.dir.path(), &f.run).unwrap());
}

/// Every writer of a row that belongs to a run, given the run, a task, an
/// attempt or a matrix group of it, refuses a marked run in its own
/// transaction, and writes nothing. An event that names only an attempt is
/// one of them.
#[test]
fn no_row_of_a_run_that_is_being_pruned_is_written_by_any_writer() {
    use crate::results::plan::Verdict;
    use crate::wire::LogStream;
    use local_ci::artifact_io::{FileEntry, Receipt as Manifest};
    for marked in [false, true] {
        let f = fx();
        f.store.create_matrix_group("held", &f.run, 2, 6).unwrap();
        f.store
            .prepare_log_progress(&f.attempt, LogStream::Stdout, 6)
            .unwrap();
        // The bytes of the workflow artifact are in the result store, so
        // that its publication reaches its transaction.
        let digest = Digest::of_bytes(b"result");
        let files = crate::results::ResultStore::open(&f.dir.path().join("results")).unwrap();
        files
            .append_part(&f.attempt, &digest, 0, b"result")
            .unwrap();
        files.publish_artifact(&f.attempt, &digest, 6).unwrap();
        if marked {
            mark(&f.store, &f.run);
        }
        let before = f.store.snapshot_rows_for_test();
        let worker = WorkerId::parse("mini-a").unwrap();
        let file = ResultRef {
            ref_id: "ref-of-a-file".into(),
            kind: BlobKind::Result,
            locator: "results/file".into(),
        };
        let receipt = ArtifactReceipt {
            attempt_id: f.attempt.clone(),
            name: "out".into(),
            rel_path: "out".into(),
            size: 1,
            digest: digest.clone(),
            source_id: SourceId::generate(),
            plan_digest: digest.clone(),
            profile_fingerprint: "profile".into(),
        };
        let upload = || WorkflowUpload {
            run: &f.run,
            workflow: "ci",
            producer: &f.attempt,
            key: &digest,
            overwrite: false,
            receipt: Manifest {
                name: "build".into(),
                version: 1,
                files: vec![FileEntry {
                    path: "out/app".into(),
                    size: 6,
                    sha256: digest.as_str().strip_prefix("sha256:").unwrap().into(),
                }],
                plan_digest: "plan".into(),
                attempt_id: "run".into(),
                job: "ci::build".into(),
                display: "build".into(),
                task: None,
                retention_days: None,
                compression_level: None,
                restored_from: None,
            },
        };
        let s = &f.store;
        let null = serde_json::Value::Null;
        let answers: Vec<(&str, Result<()>)> = vec![
            (
                "append_ci_driver_record",
                s.append_ci_driver_record(&f.run, 0, 1, "record".into())
                    .map(drop),
            ),
            (
                "complete_ci_origin",
                s.complete_ci_origin(&f.run, &f.waiting, 0, "success", 20)
                    .map(drop),
            ),
            (
                "append_event of the run",
                s.append_event(Some(&f.run), None, "note", &null, 20)
                    .map(drop),
            ),
            (
                "append_event of an attempt alone",
                s.append_event(None, Some(&f.attempt), "submit_deferred", &null, 20)
                    .map(drop),
            ),
            (
                "record_placement",
                s.record_placement(&f.waiting, &worker, None, &json!({}), 20)
                    .map(drop),
            ),
            (
                "create_matrix_group",
                s.create_matrix_group("fresh", &f.run, 1, 20),
            ),
            (
                "acquire_permit",
                s.acquire_permit("held", &PermitId::generate(), 20)
                    .map(drop),
            ),
            (
                "begin_collection",
                s.begin_collection(&f.attempt, &digest, &json!({}), &[], &[], 20)
                    .map(drop),
            ),
            (
                "set_collection_logs",
                s.set_collection_logs(&f.attempt, &digest, &json!({}), 20)
                    .map(drop),
            ),
            (
                "record_collected_artifact",
                s.record_collected_artifact(&receipt, &file, 20),
            ),
            ("record_artifact", s.record_artifact(&receipt, 20)),
            (
                "recollect",
                s.recollect(&f.attempt, true, &[], 20).map(drop),
            ),
            (
                "strike_collection",
                s.strike_collection(&f.attempt, "why", 20).map(drop),
            ),
            (
                "close_collection",
                s.close_collection(&f.attempt, None, Verdict::GaveUp, false, 20)
                    .map(drop),
            ),
            (
                "reopen_collection",
                s.reopen_collection(&f.attempt, 20).map(drop),
            ),
            (
                "prepare_log_progress",
                s.prepare_log_progress(&f.attempt, LogStream::Stderr, 20)
                    .map(drop),
            ),
            (
                "commit_log_progress",
                s.commit_log_progress(&f.attempt, LogStream::Stdout, 0, 1, false, false, 0, 20)
                    .map(drop),
            ),
            (
                "reserve_workflow_files",
                s.reserve_workflow_files(upload(), 20),
            ),
            (
                "publish_workflow_artifact",
                s.publish_workflow_artifact(upload(), 20).map(drop),
            ),
            (
                "workflow_download",
                s.workflow_download(&f.run, "ci", &f.attempt, &digest, None, None)
                    .map(drop),
            ),
            (
                "add_blob_ref",
                s.add_blob_ref(
                    &NewBlobRef {
                        ref_id: "another-ref".into(),
                        attempt_id: f.attempt.to_string(),
                        kind: BlobKind::Result,
                        locator: "results/another".into(),
                    },
                    20,
                ),
            ),
            (
                "append_events_unseen of an attempt alone",
                s.append_events_unseen(
                    None,
                    &f.attempt,
                    "submit_deferred",
                    "answer",
                    &[("busy".into(), json!({"answer": "busy"}))],
                    20,
                )
                .map(drop),
            ),
            (
                "begin_retire_noting",
                s.begin_retire_noting(
                    &f.attempt,
                    "cannot",
                    Some(("worker_excluded", &json!({}))),
                    20,
                )
                .map(drop),
            ),
        ];
        let pruned = |answer: &Result<()>| {
            answer.as_ref().is_err_and(|error| {
                error.code() == ErrorCode::NotFound && error.to_string().contains(RUN_PRUNED)
            })
        };
        if marked {
            let written: Vec<_> = answers
                .iter()
                .filter(|(_, answer)| !pruned(answer))
                .collect();
            assert!(written.is_empty(), "not refused: {written:?}");
            assert_eq!(f.store.snapshot_rows_for_test(), before, "no row changed");
        } else {
            let refused: Vec<_> = answers
                .iter()
                .filter(|(_, answer)| pruned(answer))
                .collect();
            assert!(refused.is_empty(), "refused without the mark: {refused:?}");
            // The writers reach their rows: without the mark they write.
            let failed: Vec<_> = answers
                .iter()
                .filter(|(name, answer)| {
                    answer.is_err()
                        && !["publish_workflow_artifact", "workflow_download"].contains(name)
                })
                .collect();
            assert!(failed.is_empty(), "{failed:?}");
            assert_ne!(f.store.snapshot_rows_for_test(), before);
        }
    }
}

/// The check a retry makes after it read the files (the second of its
/// preparation) and the check in the transaction that authorizes it: each
/// refuses a run that was marked after the check before it passed.
#[test]
fn a_retry_overtaken_by_the_mark_is_refused_at_each_later_check() {
    // Marked while the files are read: the preparation is refused.
    let f = fx();
    let before = f.store.snapshot_rows_for_test();
    let mut overtaken = false;
    let prepared =
        f.store
            .prepare_retry_after(Target::Task(f.task.clone()), 0, "retry-one".into(), || {
                mark(&f.store, &f.run);
                overtaken = true;
            });
    assert!(overtaken);
    let error = prepared
        .map(drop)
        .expect_err("the second check of the preparation refuses");
    assert_eq!(error.code(), ErrorCode::NotFound, "{error}");
    assert!(error.to_string().contains(RUN_PRUNED), "{error}");
    let mut after = f.store.snapshot_rows_for_test();
    let unmark = |rows: &mut Vec<(String, Vec<Vec<rusqlite::types::Value>>)>| {
        rows.retain(|(table, _)| table != "run");
    };
    let mut before = before;
    unmark(&mut before);
    unmark(&mut after);
    assert_eq!(after, before, "no row but the mark changed");

    // Marked after the preparation: the commit is refused.
    let f = fx();
    let mut overtaken = false;
    let answer = f.store.retry(
        Target::Task(f.task.clone()),
        0,
        "retry-one".into(),
        20,
        |_, _, _| {
            mark(&f.store, &f.run);
            overtaken = true;
            Ok(false)
        },
    );
    assert!(overtaken, "the preparation passed: the run had no mark");
    let marked = f.store.snapshot_rows_for_test();
    refused(answer);
    assert_eq!(f.store.snapshot_rows_for_test(), marked, "no row changed");
}

/// An enqueue passed the check it makes before the pin file. The mark is
/// written before its transaction, which refuses it: `run_pruned`, and not
/// the answer of a run that exists already.
#[test]
fn an_enqueue_overtaken_by_the_mark_is_refused_in_its_transaction() {
    let f = fx();
    let source = f.store.source_mutation_guard().unwrap();
    let mut overtaken = false;
    let answer = f.store.enqueue_guarded_after(
        &f.run,
        None,
        &json!({}),
        &[(TaskId::generate(), json!({}))],
        20,
        source,
        || {
            mark(&f.store, &f.run);
            overtaken = true;
        },
    );
    assert!(overtaken, "the first check passed: the run had no mark");
    refused(answer);
}

/// A query that reads a task several times is overtaken by the pruning
/// after its reads: by the mark, or by the whole of the pruning, which
/// leaves no row and no mark. Both are "not found", whatever was read.
#[test]
fn a_query_overtaken_by_the_pruning_after_its_reads_answers_not_found() {
    let queries = |f: &Fx| {
        vec![
            Query::Attempts {
                task_id: f.task.clone(),
                limit: 10,
                after: None,
            },
            Query::LogMetadata {
                task_id: f.task.clone(),
                attempt_id: f.attempt.clone(),
            },
            Query::OutputMetadata {
                task_id: f.task.clone(),
                attempt_id: f.attempt.clone(),
                after: None,
                limit: 10,
            },
        ]
    };
    for index in 0..3 {
        for rows_gone in [false, true] {
            let f = fx();
            let reader = CoordinatorStore::open_read_only(f.dir.path()).unwrap();
            let query = queries(&f).remove(index);
            let mut overtaken = false;
            let answer = reader.query_after(query, || {
                if rows_gone {
                    f.store.delete_run_for_test(&f.run);
                } else {
                    mark(&f.store, &f.run);
                }
                overtaken = true;
            });
            assert!(overtaken);
            let error = answer.expect_err("a run pruned meanwhile is not found");
            assert_eq!(error.code(), ErrorCode::NotFound, "{index} {error}");
            assert_eq!(
                error.to_string().contains(RUN_PRUNED),
                !rows_gone,
                "{index} {error}"
            );
        }
    }
    // Without the pruning the same queries answer.
    let f = fx();
    for query in queries(&f) {
        f.store.query_after(query, || ()).unwrap();
    }
}

/// A refusal that was read of a task whose row is gone says something of
/// rows that were being deleted: it is "not found" too, and never "the
/// attempt does not belong to the task".
#[test]
fn a_refusal_read_of_a_task_that_is_gone_is_not_found() {
    let f = fx();
    let query = || Query::OutputMetadata {
        task_id: f.waiting.clone(),
        attempt_id: f.attempt.clone(),
        after: None,
        limit: 10,
    };
    // With its row, the task is told that the attempt is not its own.
    let error = f.store.query(query()).unwrap_err();
    assert!(error.to_string().contains("does not belong"), "{error}");
    delete_half(&f);
    let error = f.store.query(query()).unwrap_err();
    assert_eq!(error.code(), ErrorCode::NotFound, "{error}");
}

/// Spec 25.3 (a), rule 2, as the end of a run reads it for the files a
/// later job may still download: a task without an attempt is settled by
/// the close of its run, as it is for the retention of the inputs.
#[test]
fn a_closed_run_ends_for_its_workflow_files_with_a_task_that_was_never_placed() {
    let f = fx();
    assert_eq!(f.store.workflow_run_end_for_test(&f.run).unwrap(), None);
    f.store.close_run(&f.run, 50).unwrap();
    assert_eq!(f.store.workflow_run_end_for_test(&f.run).unwrap(), Some(50));
}
