//! Spec 25.5: the parent rows of the pruning of a run. The whole pruning,
//! with the files, is tested with its driver
//! (`service::resident::run_pruning`).

use super::*;
use crate::ids::*;
use crate::results::pin::RunPin;
use crate::service::completion::Target;
use crate::state::{AttemptState, Outcome, Termination};
use serde_json::json;

const WORKER: &str = "mini-a";

fn store() -> (tempfile::TempDir, CoordinatorStore) {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    (dir, store)
}

/// A task of `run` whose only attempt ended at about `at` and whose
/// collection was closed.
fn ended_task(store: &CoordinatorStore, run: &RunId, at: u64) -> (TaskId, AttemptId) {
    let (task, attempt) = (TaskId::generate(), AttemptId::generate());
    store.create_task(&task, run, &json!({}), at).unwrap();
    store
        .create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task.clone(),
                worker_id: WorkerId::parse(WORKER).unwrap(),
                payload_hash: Digest::of_bytes(b"payload"),
                permit_id: None,
            },
            at,
        )
        .unwrap();
    for state in [
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
        AttemptState::Finalizing,
    ] {
        store.transition_attempt(&attempt, state, at).unwrap();
    }
    let failed =
        Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(1)), None).unwrap();
    store.finalize_attempt(&attempt, failed, at + 1).unwrap();
    store
        .close_collection(
            &attempt,
            None,
            crate::results::plan::Verdict::GaveUp,
            false,
            at + 2,
        )
        .unwrap();
    (task, attempt)
}

/// A run that ended at about `at`.
fn ended_run(store: &CoordinatorStore, at: u64) -> (RunId, TaskId, AttemptId) {
    let run = RunId::generate();
    store.create_run(&run, None, None, &json!({}), at).unwrap();
    let (task, attempt) = ended_task(store, &run, at);
    (run, task, attempt)
}

fn sql(store: &CoordinatorStore, batch: String) {
    store
        .db
        .write(move |tx| Ok(tx.execute_batch(&batch)?))
        .unwrap();
}

fn count(store: &CoordinatorStore, query: &'static str) -> i64 {
    store
        .db
        .call(move |c| Ok(c.query_row(query, [], |r| r.get(0))?))
        .unwrap()
}

fn mentions(store: &CoordinatorStore, needles: &[String]) -> Vec<String> {
    store.mentions_for_test(needles)
}

impl CoordinatorStore {
    /// Every text cell of every table that contains one of `needles`.
    pub(crate) fn mentions_for_test(&self, needles: &[String]) -> Vec<String> {
        let needles = needles.to_vec();
        self.db
            .call(move |c| {
            let tables: Vec<String> = {
                let mut s = c.prepare(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                )?;
                let rows = s.query_map([], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let mut found = Vec::new();
            for table in tables {
                let mut s = c.prepare(&format!("SELECT * FROM {table}"))?;
                let columns = s.column_count();
                let mut rows = s.query([])?;
                while let Some(row) = rows.next()? {
                    for i in 0..columns {
                        if let rusqlite::types::Value::Text(text) = row.get(i)? {
                            if needles.iter().any(|needle| text.contains(needle)) {
                                found.push(format!("{table}: {text}"));
                            }
                        }
                    }
                }
            }
            Ok(found)
        })
        .unwrap()
    }
}

/// The whole table list, with how many rows each has.
fn sizes(store: &CoordinatorStore) -> Vec<(String, i64)> {
    store
        .db
        .call(|c| {
            let tables: Vec<String> = {
                let mut s = c.prepare(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                )?;
                let rows = s.query_map([], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let mut out = Vec::new();
            for table in tables {
                let n = c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
                out.push((table, n));
            }
            Ok(out)
        })
        .unwrap()
}

fn foreign_keys_hold(store: &CoordinatorStore) {
    let broken: i64 = count(store, "SELECT count(*) FROM pragma_foreign_key_check");
    assert_eq!(broken, 0, "a transaction left a row without its parent");
    assert_eq!(
        count(store, "SELECT foreign_keys FROM pragma_foreign_keys"),
        1
    );
}

/// The source lock held exclusively, as the driver takes it for the last
/// transaction of a run.
fn exclusive(store: &CoordinatorStore) -> crate::source::retention::ExclusiveGuard {
    crate::source::SourceStore::open(&store.state_dir)
        .unwrap()
        .prune_rows_guard()
        .unwrap()
        .expect("the source lock is free")
}

/// Every transaction of the rows of a marked run, to the end.
fn prune_all_rows(store: &CoordinatorStore, run: &RunId) -> (u32, Vec<(WorkerId, AttemptId)>) {
    let (mut transactions, mut attempts) = (0, Vec::new());
    loop {
        let page = store
            .prune_rows(run, ROWS_PER_TRANSACTION, Some(&exclusive(store)))
            .unwrap();
        transactions += 1;
        assert!(page.rows <= ROWS_PER_TRANSACTION, "{} rows", page.rows);
        foreign_keys_hold(store);
        attempts.extend(page.attempts);
        match page.state {
            RowsState::More => assert_eq!(page.rows, ROWS_PER_TRANSACTION),
            RowsState::Results => panic!("a result reference was left"),
            RowsState::Source => panic!("the source lock was held"),
            RowsState::Done => return (transactions, attempts),
        }
    }
}

const R: u64 = 1000;
fn mark(store: &CoordinatorStore, wall: u64, mono: u64) -> MarkPage {
    store
        .mark_ended_runs(None, 32, wall, mono, Some("boot".into()), R)
        .unwrap()
}

/// Step 1 and the condition of spec 25.5: only a run that has ended, whose
/// retention has passed on a trusted clock, gets the mark.
#[test]
fn a_run_is_marked_only_when_it_ended_its_retention_passed_and_the_clock_is_trusted() {
    let (_dir, store) = store();
    let (due, _, _) = ended_run(&store, 5);
    let (recent, _, _) = ended_run(&store, 11_500);
    // A task that was not placed yet: the run has not ended.
    let (open, _, _) = ended_run(&store, 5);
    store
        .create_task(&TaskId::generate(), &open, &json!({}), 6)
        .unwrap();
    // No task and not closed: its first task may still be made.
    let empty = RunId::generate();
    store.create_run(&empty, None, None, &json!({}), 5).unwrap();
    // No task, closed long ago.
    let closed = RunId::generate();
    store
        .create_run(&closed, None, None, &json!({}), 5)
        .unwrap();
    store.close_run(&closed, 6).unwrap();
    // An attempt whose observation is unknown (25.3 (a), rule 3).
    let (unknown, _, lost) = ended_run(&store, 5);
    sql(
        &store,
        format!("UPDATE attempt SET observation='unknown' WHERE attempt_id='{lost}'"),
    );
    // A termination this version cannot read.
    let (unread, _, odd) = ended_run(&store, 5);
    sql(
        &store,
        format!("UPDATE attempt SET termination_json='{{\"outcome\":\"from_the_future\"}}' WHERE attempt_id='{odd}'"),
    );

    // The first sample only starts the quarantine of the clock.
    let first = mark(&store, 10_000, 1_000);
    assert!(first.clock_held && first.marked.is_empty() && first.now.is_none());
    assert_eq!(store.marked_run_count().unwrap(), 0);
    // One full retention later the clock is trusted.
    let second = mark(&store, 12_000, 3_000);
    assert!(!second.clock_held);
    assert_eq!(second.now, Some(12_000));
    let mut marked = second.marked.clone();
    marked.sort();
    let mut expected = vec![due.clone(), closed.clone()];
    expected.sort();
    assert_eq!(marked, expected);
    for run in [&recent, &open, &empty, &unknown, &unread] {
        assert!(!store.run_is_pruned(run).unwrap(), "{run} was marked");
    }
    assert_eq!(store.marked_run_count().unwrap(), 2);
    assert_eq!(store.marked_runs(None, 32).unwrap(), expected);
    // The mark is found by its index, and a marked run is not judged again.
    assert!(mark(&store, 12_001, 3_001).marked.is_empty());
    // Once its retention has passed, the recent run is due too.
    assert_eq!(mark(&store, 12_600, 3_600).marked, vec![recent]);
}

/// The page of step 1 is bounded and continues where it stopped.
#[test]
fn the_page_of_runs_to_judge_is_bounded_and_continues() {
    let (_dir, store) = store();
    let mut runs: Vec<RunId> = (0..5).map(|_| ended_run(&store, 5).0).collect();
    runs.sort();
    mark(&store, 10_000, 1_000);
    let page = store
        .mark_ended_runs(None, 2, 12_000, 3_000, Some("boot".into()), R)
        .unwrap();
    assert_eq!((page.examined, page.marked.len()), (2, 2));
    assert_eq!(page.next, Some(runs[1].clone()));
    let page = store
        .mark_ended_runs(page.next, 32, 12_001, 3_001, Some("boot".into()), R)
        .unwrap();
    assert_eq!((page.examined, page.marked.len()), (3, 3));
    assert_eq!(page.next, None);
}

/// Acceptance (iii), (iv) and (x), and the order of the rows: a run with
/// more than 3,000 rows, a row in every table of the list, a retire
/// tombstone and events that name only an attempt goes in transactions of
/// 1,000 rows with the foreign keys on, and leaves no row.
#[test]
fn the_rows_of_a_run_go_in_bounded_transactions_children_first_and_none_is_left() {
    let (_dir, store) = store();
    let digest = format!("sha256:{}", "a".repeat(64));
    let (only, shared) = (SourceId::generate(), SourceId::generate());
    for source in [&only, &shared] {
        store
            .put_source_ref(source, &ManifestDigest::parse(&digest).unwrap(), 1)
            .unwrap();
    }
    sql(
        &store,
        "INSERT INTO secret_bundle_ref VALUES('bundle-only','d',1),('bundle-shared','d',1)".into(),
    );
    let run = RunId::generate();
    store
        .create_run(&run, Some(&only), Some("bundle-only"), &json!({}), 1)
        .unwrap();
    let (task, attempt) = ended_task(&store, &run, 5);
    let (task2, attempt2) = ended_task(&store, &run, 5);
    // Another run keeps what it shares, and all of its own rows.
    let other = RunId::generate();
    store
        .create_run(&other, Some(&shared), Some("bundle-shared"), &json!({}), 1)
        .unwrap();
    let (_, other_attempt) = ended_task(&store, &other, 5);
    let kept = RunId::generate();
    store
        .create_run(&kept, Some(&shared), Some("bundle-shared"), &json!({}), 1)
        .unwrap();
    // Events that name an attempt and no run (`Coordinator::note`).
    for a in [&attempt, &attempt2, &other_attempt] {
        store
            .append_event(None, Some(a), "worker_excluded", &json!({}), 6)
            .unwrap();
    }
    store
        .cancel_control(Target::Run(run.clone()), 0, "cancel-1".into(), 7)
        .unwrap();
    store.create_matrix_group("group-1", &run, 2, 7).unwrap();
    sql(
        &store,
        format!(
            "INSERT INTO event(run_id,attempt_id,attempt_seq,kind,payload_json,at_ms)
               WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<3200)
               SELECT '{run}',NULL,NULL,'filler','null',8 FROM n;
             INSERT INTO placement(task_id,worker_id,attempt_id,demand_json,created_at_ms) VALUES('{task}','{WORKER}','{attempt}','{{}}',8),('{task2}','{WORKER}',NULL,'{{}}',8);
             INSERT INTO retire_tombstone(attempt_id,worker_id,reason,requested_at_ms,confirmed_at_ms) VALUES('{attempt2}','{WORKER}','lost',8,9);
             INSERT INTO matrix_permit(permit_id,group_id,attempt_id,held,created_at_ms,released_at_ms) VALUES('permit-1','group-1','{attempt}',0,8,9),('permit-2','group-1',NULL,0,8,9);
             INSERT INTO artifact_receipt(attempt_id,name,required) VALUES('{attempt}','out',1);
             INSERT INTO blob_ref(ref_id,attempt_id,kind,locator,created_at_ms) VALUES('ref-input','{attempt}','input','elsewhere/input',8),('ref-log','{attempt}','log','results/{attempt}/logs',8);
             INSERT INTO collection_log(attempt_id,stream,updated_at_ms) VALUES('{attempt}','stdout',8);
             INSERT INTO ci_driver_transition(run_id,authorization,revision,record) VALUES('{run}',0,1,'r');
             INSERT INTO workflow_artifact(run_id,workflow,name,version,producer,action_key,fingerprint,receipt_json) VALUES('{run}','w','n',1,'{attempt}','k','f','{{}}');
             INSERT INTO workflow_download(consumer,action_key,fingerprint,selection_json) VALUES('{attempt2}','k','f','{{}}');
             INSERT INTO workflow_upload_inflight(producer,action_key,workflow,name,fingerprint) VALUES('{attempt2}','k','w','n','f');"
        ),
    );
    let ids = [
        run.to_string(),
        task.to_string(),
        task2.to_string(),
        attempt.to_string(),
        attempt2.to_string(),
    ];
    assert!(mentions(&store, &ids).len() > 3200);
    let others_before = mentions(&store, &[other.to_string(), other_attempt.to_string()]);

    // A run without the mark loses nothing.
    let before = sizes(&store);
    assert!(matches!(
        store.prune_rows(&run, ROWS_PER_TRANSACTION, None),
        Err(StorageError::Invalid(_))
    ));
    assert_eq!(sizes(&store), before);
    store.mark_pruning_for_test(&run);
    // 2 of the order: while a result reference or a claim of the run is
    // left, no row goes; step 3 comes first.
    let page = store.prune_rows(&run, ROWS_PER_TRANSACTION, None).unwrap();
    assert_eq!((page.rows, page.state), (0, RowsState::Results));
    assert_eq!(sizes(&store), before);
    assert_eq!(store.prune_claim_results(&run, 32, 10).unwrap(), (1, 0));
    let page = store.prune_rows(&run, ROWS_PER_TRANSACTION, None).unwrap();
    assert_eq!((page.rows, page.state), (0, RowsState::Results));
    let claims = store.prune_pending_results(&run, 32).unwrap();
    assert_eq!(claims.len(), 1);
    store.prune_finish_result(&claims[0]).unwrap();
    assert_eq!(store.prune_claim_results(&run, 32, 10).unwrap(), (0, 0));

    let (transactions, attempts) = prune_all_rows(&store, &run);
    assert!(transactions >= 4, "{transactions} transactions");
    let mut attempts: Vec<String> = attempts
        .into_iter()
        .map(|(worker, attempt)| format!("{worker}/{attempt}"))
        .collect();
    attempts.sort();
    let mut expected = vec![
        format!("{WORKER}/{attempt}"),
        format!("{WORKER}/{attempt2}"),
    ];
    expected.sort();
    assert_eq!(attempts, expected, "the attempts are read as their rows go");
    assert_eq!(mentions(&store, &ids), Vec::<String>::new());
    // What only this run named is gone with it; what another run names stays.
    assert_eq!(
        count(&store, "SELECT count(*) FROM source_ref"),
        1,
        "the source of the pruned run alone"
    );
    assert_eq!(count(&store, "SELECT count(*) FROM secret_bundle_ref"), 1);
    assert_eq!(
        mentions(&store, &[other.to_string(), other_attempt.to_string()]),
        others_before
    );
    assert!(store.has_run(&other).unwrap() && store.has_run(&kept).unwrap());
    // Asking again finds nothing to do.
    let again = store.prune_rows(&run, ROWS_PER_TRANSACTION, None).unwrap();
    assert_eq!((again.rows, again.state), (0, RowsState::Done));
}

/// The widened trigger of the journal: only the events of a marked run, of
/// its attempts, of a run without a row and of no run at all can go.
#[test]
fn the_journal_keeps_the_events_of_a_run_that_carries_no_mark() {
    let (_dir, store) = store();
    let (run, _, attempt) = ended_run(&store, 5);
    store
        .append_event(None, Some(&attempt), "worker_excluded", &json!({}), 6)
        .unwrap();
    store
        .append_event(None, None, "started", &json!({}), 6)
        .unwrap();
    let delete = |what: String| -> Result<usize> {
        store
            .db
            .write(move |tx| Ok(tx.execute(&format!("DELETE FROM event WHERE {what}"), [])?))
    };
    assert!(delete(format!("run_id='{run}'")).is_err());
    assert!(delete(format!("run_id IS NULL AND attempt_id='{attempt}'")).is_err());
    assert!(delete("run_id IS NULL AND attempt_id IS NULL".into()).unwrap() >= 1);
    store.mark_pruning_for_test(&run);
    assert_eq!(
        delete(format!("run_id IS NULL AND attempt_id='{attempt}'")).unwrap(),
        1
    );
    assert!(delete(format!("run_id='{run}'")).unwrap() > 0);
}

/// Spec 25.5, "runに属さない記録": an event of no run and no attempt goes
/// one retention after it was written, the oldest first and at most the
/// bound; a test timing goes 14 days after it was last written.
#[test]
fn records_of_no_run_go_by_their_age_and_within_the_bound() {
    let (_dir, store) = store();
    let (run, _, attempt) = ended_run(&store, 5);
    store
        .append_event(None, Some(&attempt), "worker_excluded", &json!({}), 6)
        .unwrap();
    for at in [10, 11, 12, 13, 14, 90_000] {
        store
            .append_event(None, None, "started", &json!({}), at)
            .unwrap();
    }
    let day = 24 * 60 * 60 * 1000u64;
    let now = 20 * day;
    sql(
        &store,
        format!(
            "INSERT INTO ci_test_timing(scope,worker,identity,millis,written_at_ms) VALUES('s','w','old',1,{}),('s','w','edge',1,{}),('s','w','new',1,{})",
            now - 14 * day - 1,
            now - 14 * day + 1,
            now
        ),
    );
    let unowned = "SELECT count(*) FROM event WHERE run_id IS NULL AND attempt_id IS NULL";
    let owned = count(&store, "SELECT count(*) FROM event") - count(&store, unowned);
    // Nothing is old enough yet (the store wrote events of its own start).
    assert_eq!(store.prune_unowned(1_000, 1_000, 2).unwrap(), (0, 0));
    // The bound: two of the old events, the oldest first.
    let old = count(&store, unowned) - 1;
    assert!(old >= 5);
    let third = count(
        &store,
        "SELECT seq FROM event WHERE run_id IS NULL AND attempt_id IS NULL ORDER BY seq LIMIT 1 OFFSET 2",
    );
    assert_eq!(store.prune_unowned(50_000, 1_000, 2).unwrap(), (2, 0));
    assert_eq!(
        count(
            &store,
            "SELECT min(seq) FROM event WHERE run_id IS NULL AND attempt_id IS NULL"
        ),
        third
    );
    assert_eq!(
        store.prune_unowned(50_000, 1_000, 1000).unwrap(),
        (old as u32 - 2, 0)
    );
    assert_eq!(count(&store, unowned), 1, "the recent event stays");
    assert_eq!(
        count(&store, "SELECT count(*) FROM event") - count(&store, unowned),
        owned,
        "the events of a run and of an attempt are not judged by age"
    );
    assert_eq!(store.prune_unowned(now, 1_000, 1000).unwrap(), (1, 1));
    assert_eq!(
        count(
            &store,
            "SELECT count(*) FROM ci_test_timing WHERE identity IN ('edge','new')"
        ),
        2
    );
    assert!(store.has_run(&run).unwrap());
}

/// Acceptance (xiv), and `retry`: a cancel of a pruned run, and the same
/// request sent again, are `NOT_FOUND`, say why a run can be gone, and add
/// no row. The receipt went with the run, so no stored answer comes back.
#[test]
fn a_cancel_and_a_retry_of_a_pruned_run_are_not_found_and_write_nothing() {
    let (_dir, store) = store();
    let (run, task, _) = ended_run(&store, 5);
    store
        .cancel_control(Target::Run(run.clone()), 0, "cancel-1".into(), 7)
        .unwrap();
    store.mark_pruning_for_test(&run);
    prune_all_rows(&store, &run);
    let before = sizes(&store);
    for (target, request) in [
        (Target::Run(run.clone()), "cancel-1"),
        (Target::Run(run.clone()), "cancel-2"),
        (Target::Task(task.clone()), "cancel-3"),
    ] {
        let refused = store
            .cancel_control(target, 0, request.into(), 9)
            .unwrap_err();
        assert!(
            matches!(refused, StorageError::NotFound { .. }),
            "{refused}"
        );
    }
    let refused = store
        .cancel_control(Target::Run(run.clone()), 0, "cancel-1".into(), 9)
        .unwrap_err();
    assert!(refused.to_string().contains(RUN_RETENTION), "{refused}");
    for target in [Target::Run(run.clone()), Target::Task(task)] {
        let refused = store
            .prepare_retry(target, 0, "retry-1".into())
            .map(|_| ())
            .unwrap_err();
        assert!(
            matches!(refused, StorageError::NotFound { .. }),
            "{refused}"
        );
    }
    assert_eq!(sizes(&store), before);
    assert!(store.query_run(run).unwrap().is_none());
}

/// Step 3 and step 8, and acceptance (xiii): the pin of a marked run is
/// taken without waiting, a missing pin file is "no reader", the pin file
/// goes with the run, and nothing makes it again for an attempt of the run.
#[test]
fn the_pin_of_a_pruned_run_is_unlinked_and_never_made_again() {
    let (dir, store) = store();
    let (run, _, attempt) = ended_run(&store, 5);
    let path = dir.path().join(format!("reader-pins/{run}.lock"));
    assert!(path.exists());
    // Only a marked run is pruned.
    assert!(matches!(
        store.prune_pin(&run),
        Err(StorageError::Invalid(_))
    ));
    let reader = RunPin::shared(dir.path(), &run).unwrap();
    store.mark_pruning_for_test(&run);
    // A reader that began before the mark holds the run.
    match store.prune_pin(&run) {
        Err(StorageError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock),
        other => panic!("{other:?}"),
    }
    drop(reader);
    let pin = store
        .prune_pin(&run)
        .unwrap()
        .expect("the pin file is there");
    prune_all_rows(&store, &run);
    // A process that opened the file before the unlink holds an inode the
    // path no longer names: its comparison fails (25.1 (B)).
    let stale = std::fs::File::open(&path).unwrap();
    store.prune_unpin(pin).unwrap();
    assert!(!path.exists());
    drop(stale);
    assert!(RunPin::shared(dir.path(), &run).is_err());
    assert!(!path.exists(), "a reader makes no pin");
    let refused = store.result_writer_pin(&attempt).unwrap_err();
    assert!(
        matches!(refused, StorageError::NotFound { .. }),
        "{refused}"
    );
    assert!(matches!(
        store.result_gc_guard(&run),
        Err(StorageError::NotFound { .. })
    ));
    assert!(!path.exists(), "no pin is made for a pruned run");

    // A marked run whose pin file is gone has no reader.
    let (marked, _, _) = ended_run(&store, 5);
    store.mark_pruning_for_test(&marked);
    std::fs::remove_file(dir.path().join(format!("reader-pins/{marked}.lock"))).unwrap();
    assert!(store.prune_pin(&marked).unwrap().is_none());
    assert!(!dir
        .path()
        .join(format!("reader-pins/{marked}.lock"))
        .exists());
}

/// Only the exclusive holder unlinks a pin, and only its own inode.
#[test]
fn a_shared_holder_does_not_unlink_a_pin() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let run = RunId::generate();
    RunPin::ensure(dir.path(), &run).unwrap();
    let path = dir.path().join(format!("reader-pins/{run}.lock"));
    assert!(RunPin::shared(dir.path(), &run)
        .unwrap()
        .unlink(dir.path())
        .is_err());
    assert!(path.exists());
    // The path names another inode than the one that was locked.
    let pin = RunPin::exclusive(dir.path(), &run).unwrap();
    std::fs::remove_file(&path).unwrap();
    RunPin::ensure(dir.path(), &run).unwrap();
    assert!(pin.unlink(dir.path()).is_err());
    assert!(path.exists(), "the file of another holder stays");
}

/// The scan at start (spec 25.11): what a stop between step 6 and step 8
/// left is removed; the pin of a run, a held pin and a name that is not a
/// run's stay.
#[test]
fn the_scan_at_start_removes_only_the_pin_of_a_run_without_a_row() {
    let (dir, store) = store();
    let (run, _, _) = ended_run(&store, 5);
    let (orphan, held) = (RunId::generate(), RunId::generate());
    RunPin::ensure(dir.path(), &orphan).unwrap();
    RunPin::ensure(dir.path(), &held).unwrap();
    let pins = dir.path().join("reader-pins");
    std::fs::write(pins.join("notes.txt"), b"not a pin").unwrap();
    let reader = RunPin::shared(dir.path(), &held).unwrap();
    assert_eq!(store.sweep_orphan_pins(), (1, 2));
    assert!(pins.join(format!("{run}.lock")).exists());
    assert!(!pins.join(format!("{orphan}.lock")).exists());
    assert!(pins.join(format!("{held}.lock")).exists());
    assert!(pins.join("notes.txt").exists());
    drop(reader);
    assert_eq!(store.sweep_orphan_pins(), (1, 1));
    assert!(!pins.join(format!("{held}.lock")).exists());
}

/// Acceptance (xv), and the ground of 25.1 (B) for step 8: this step raises
/// the schema version, and a binary that knows only an earlier version does
/// not open the file, so it reaches no pin and makes none.
#[test]
fn a_binary_from_before_this_step_does_not_open_the_file_and_reaches_no_pin() {
    let steps = schema::steps(DbKind::Coordinator);
    assert!(
        steps.last().unwrap().contains("run_pruning"),
        "the step of the pruning is the last one: move this test with it"
    );
    let (dir, store) = store();
    let (run, _, _) = ended_run(&store, 5);
    // The file as a later binary leaves it: one version above what this
    // binary knows, exactly what the file of this step is to the binary
    // before it.
    let later = (steps.len() + 1).to_string();
    store
        .db
        .write(move |tx| {
            Ok(tx.execute(
                "UPDATE schema_meta SET value=?1 WHERE key='schema_version'",
                [later],
            )?)
        })
        .unwrap();
    drop(store);
    let pins = || {
        let mut names: Vec<_> = std::fs::read_dir(dir.path().join("reader-pins"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        names.sort();
        names
    };
    let before = pins();
    std::fs::remove_file(dir.path().join(format!("reader-pins/{run}.lock"))).unwrap();
    assert!(CoordinatorStore::open_read_only(dir.path()).is_err());
    assert!(CoordinatorStore::open(dir.path(), 2).is_err());
    assert_eq!(pins().len() + 1, before.len(), "no pin was made");
}

/// Acceptance (xii), the parent's side: nothing that names an attempt of a
/// pruned run makes a row for it, and the other attempts of the same
/// worker go on.
#[test]
fn an_attempt_of_a_pruned_run_gets_no_row_back_and_the_worker_s_other_attempts_go_on() {
    let (_dir, store) = store();
    let (run, _, gone) = ended_run(&store, 5);
    let live_run = RunId::generate();
    store
        .create_run(&live_run, None, None, &json!({}), 5)
        .unwrap();
    let task = TaskId::generate();
    store.create_task(&task, &live_run, &json!({}), 5).unwrap();
    let live = AttemptId::generate();
    store
        .create_attempt(
            NewAttempt {
                attempt_id: live.clone(),
                task_id: task,
                worker_id: WorkerId::parse(WORKER).unwrap(),
                payload_hash: Digest::of_bytes(b"payload"),
                permit_id: None,
            },
            5,
        )
        .unwrap();
    store.mark_pruning_for_test(&run);
    prune_all_rows(&store, &run);
    let before = sizes(&store);
    // What a worker that still holds the attempt can make the parent do.
    assert!(store
        .transition_attempt(&gone, AttemptState::Active, 9)
        .is_err());
    assert!(store
        .prepare_log_progress(&gone, crate::wire::LogStream::Stdout, 9)
        .is_err());
    assert!(store
        .close_collection(&gone, None, crate::results::plan::Verdict::GaveUp, false, 9)
        .is_err());
    assert!(store.get_attempt(&gone).unwrap().is_none());
    assert_eq!(sizes(&store), before);
    assert!(mentions(&store, &[gone.to_string()]).is_empty());
    store
        .transition_attempt(&live, AttemptState::Preparing, 9)
        .unwrap();
    assert_eq!(
        store.get_attempt(&live).unwrap().unwrap().state,
        AttemptState::Preparing
    );
}

/// A submit of an unchanged tree writes the `source_ref` of its source and
/// later inserts its run, both under the source lock held shared. The last
/// transaction of a pruned run that named the same source deletes that
/// `source_ref` only under the lock held exclusively, so it never falls
/// between the two (the insert of the run would fail on its foreign key).
#[test]
fn the_last_rows_of_a_run_wait_for_a_submit_of_its_source() {
    let (_dir, store) = store();
    let source = SourceId::generate();
    let digest = ManifestDigest::parse(&format!("sha256:{}", "a".repeat(64))).unwrap();
    store.put_source_ref(&source, &digest, 1).unwrap();
    let run = RunId::generate();
    store
        .create_run(&run, Some(&source), None, &json!({}), 1)
        .unwrap();
    store.mark_pruning_for_test(&run);
    // The submit: the lock held shared, its `source_ref`, then its run.
    let shared = store.source_mutation_guard().unwrap();
    store.put_source_ref(&source, &digest, 2).unwrap();
    let again = RunId::generate();
    store
        .enqueue_guarded_after(
            &again,
            Some(&source),
            &json!({}),
            &[(TaskId::generate(), json!({}))],
            3,
            shared,
            || {
                // The pass of the pruning reaches the last transaction here.
                let sources = crate::source::SourceStore::open(&store.state_dir).unwrap();
                assert!(sources.prune_rows_guard().unwrap().is_none());
                let page = store.prune_rows(&run, ROWS_PER_TRANSACTION, None).unwrap();
                assert_eq!(page.state, RowsState::Source);
                assert!(store.has_run(&run).unwrap());
            },
        )
        .unwrap();
    // The next pass deletes the run; the source stays, named by the submit.
    let page = store
        .prune_rows(&run, ROWS_PER_TRANSACTION, Some(&exclusive(&store)))
        .unwrap();
    assert_eq!(page.state, RowsState::Done);
    assert!(!store.has_run(&run).unwrap() && store.has_run(&again).unwrap());
    assert_eq!(count(&store, "SELECT count(*) FROM source_ref"), 1);
}

/// Acceptance (xii): an event that names an attempt without a row is
/// refused, so no event is left that the pruning of no run deletes.
#[test]
fn an_event_of_an_attempt_without_a_row_is_refused() {
    let (_dir, store) = store();
    let attempt = AttemptId::generate();
    let before = count(&store, "SELECT count(*) FROM event");
    assert!(matches!(
        store.append_event(None, Some(&attempt), "worker_excluded", &json!({}), 1),
        Err(StorageError::NotFound { .. })
    ));
    assert!(matches!(
        store.append_events_unseen(
            None,
            &attempt,
            "artifact_missing",
            "name",
            &[("out".into(), json!({"name": "out"}))],
            1
        ),
        Err(StorageError::NotFound { .. })
    ));
    assert_eq!(count(&store, "SELECT count(*) FROM event"), before);
}
