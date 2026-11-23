//! The worker database of a group (spec chapter 24): the migration that
//! rebuilds it per submitting member, and what a member can and cannot reach.

use super::policy::Access;
use super::*;
use crate::error::ErrorCode;
use crate::ids::*;
use crate::state::AttemptState;
use serde_json::json;
use std::path::Path;

fn worker() -> WorkerId {
    WorkerId::parse("mini-a").unwrap()
}

fn hash(c: char) -> PayloadHash {
    PayloadHash::parse(&format!("sha256:{}", c.to_string().repeat(64))).unwrap()
}

fn acceptance(a: &AttemptId, task: &TaskId, h: char, epoch: u64) -> NewAcceptance {
    NewAcceptance {
        attempt_id: a.clone(),
        run_id: RunId::generate(),
        task_id: task.clone(),
        generation: Generation(1),
        payload_hash: hash(h),
        coordinator_epoch: CoordinatorEpoch(epoch),
        spec: json!({"cmd": "true"}),
    }
}

/// A worker database as the version before the group left it (step 3).
fn step_3(dir: &Path) -> Db {
    let steps = schema::steps(DbKind::Worker);
    let db = db::open_with(
        &dir.join("worker.db"),
        "worker",
        &steps[..3],
        schema::compat(3),
        Access::ReadWrite,
    )
    .unwrap();
    db.write(|tx| {
        tx.execute(
            "INSERT INTO worker_state (singleton, worker_id, agent_epoch, max_coordinator_epoch)
             VALUES (1, 'mini-a', 2, 9)",
            [],
        )?;
        Ok(())
    })
    .unwrap();
    db
}

fn old_attempt(db: &Db, id: &str, state: &'static str) {
    let id = id.to_string();
    db.write(move |tx| {
        let terminal = state == "terminal";
        tx.execute(
            "INSERT INTO worker_attempt (attempt_id, run_id, task_id, generation, payload_hash,
                coordinator_epoch, spec_json, state, termination_json, receipts_json,
                accepted_at_ms, terminal_at_ms)
             VALUES (?1, 'run_x', ?1, 1, 'h', 9, '{}', ?2, ?3, '{}', 1, ?4)",
            rusqlite::params![
                id,
                state,
                terminal.then_some("{}"),
                terminal.then_some(5_i64)
            ],
        )?;
        tx.execute(
            "INSERT INTO event (run_id, attempt_id, attempt_seq, kind, payload_json, at_ms)
             VALUES ('run_x', ?1, 1, 'attempt_accepted', 'null', 1)",
            [&id],
        )?;
        Ok(())
    })
    .unwrap();
}

/// The result of an ended attempt was collected by whoever submitted it.
fn collected(db: &Db, id: &str) {
    let id = id.to_string();
    db.write(move |tx| {
        tx.execute(
            "UPDATE worker_attempt SET acked_at_ms = 6 WHERE attempt_id = ?1",
            [&id],
        )?;
        Ok(())
    })
    .unwrap();
}

fn count(db: &Db, sql: &'static str) -> i64 {
    db.call(move |c| Ok(c.query_row(sql, [], |r| r.get(0))?))
        .unwrap()
}

fn version(dir: &Path) -> String {
    let c = rusqlite::Connection::open(dir.join("worker.db")).unwrap();
    c.query_row(
        "SELECT value FROM schema_meta WHERE key='schema_version'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn a_worker_database_with_a_live_attempt_is_refused_with_the_count_and_left_as_it_was() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let old = step_3(dir.path());
    old_attempt(&old, "att_live1", "active");
    old_attempt(&old, "att_live2", "accepted");
    old_attempt(&old, "att_done", "terminal");
    drop(old);
    for _ in 0..2 {
        let e = WorkerStore::open(dir.path(), &worker()).err().unwrap();
        let StorageError::MigrationRefused(message) = &e else {
            panic!("{e:?}");
        };
        assert!(
            message.starts_with("2 attempt(s) on this worker are still live and 1 ended"),
            "{message}"
        );
        assert!(message.contains("collected"), "{message}");
        assert!(message.contains("belt workers drain"), "{message}");
        assert_eq!(e.code(), ErrorCode::ProtocolIncompatible);
        assert!(!e.is_retryable());
        // Nothing moved: the version, the rows, the old columns.
        assert_eq!(version(dir.path()), "3");
        let c = rusqlite::Connection::open(dir.path().join("worker.db")).unwrap();
        let (attempts, epoch): (i64, i64) = c
            .query_row(
                "SELECT (SELECT count(*) FROM worker_attempt), max_coordinator_epoch FROM worker_state",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((attempts, epoch), (3, 9));
    }
    // The previous version still opens it.
    let steps = schema::steps(DbKind::Worker);
    let again = db::open_with(
        &dir.path().join("worker.db"),
        "worker",
        &steps[..3],
        schema::compat(3),
        Access::ReadWrite,
    )
    .unwrap();
    assert_eq!(count(&again, "SELECT count(*) FROM worker_attempt"), 3);
}

/// An ended attempt whose result nobody collected is as much in the way as a
/// live one: the rebuild would drop the result. Each of the two alone
/// refuses, with both counts, and collecting the result lets the step run.
#[test]
fn a_worker_database_with_an_uncollected_result_is_refused_until_it_is_collected() {
    let refusal = |dir: &Path| {
        let e = WorkerStore::open(dir, &worker()).err().unwrap();
        let StorageError::MigrationRefused(message) = e else {
            panic!("{e:?}");
        };
        assert_eq!(version(dir), "3");
        assert!(message.contains("results are collected"), "{message}");
        assert!(message.contains("previous version"), "{message}");
        message
    };
    // Only uncollected results.
    let dir = crate::test_tmp::tempdir().unwrap();
    let old = step_3(dir.path());
    old_attempt(&old, "att_done1", "terminal");
    old_attempt(&old, "att_done2", "terminal");
    old_attempt(&old, "att_done3", "terminal");
    collected(&old, "att_done3");
    drop(old);
    let message = refusal(dir.path());
    assert!(
        message.starts_with("0 attempt(s) on this worker are still live and 2 ended attempt(s)"),
        "{message}"
    );
    // Only a live attempt: collected results do not count.
    let live = crate::test_tmp::tempdir().unwrap();
    let old = step_3(live.path());
    old_attempt(&old, "att_live", "active");
    old_attempt(&old, "att_done", "terminal");
    collected(&old, "att_done");
    drop(old);
    let message = refusal(live.path());
    assert!(
        message.starts_with("1 attempt(s) on this worker are still live and 0 ended attempt(s)"),
        "{message}"
    );
    // The previous version collects what is left; then the step runs.
    let steps = schema::steps(DbKind::Worker);
    let previous = db::open_with(
        &dir.path().join("worker.db"),
        "worker",
        &steps[..3],
        schema::compat(3),
        Access::ReadWrite,
    )
    .unwrap();
    collected(&previous, "att_done1");
    collected(&previous, "att_done2");
    drop(previous);
    let store = WorkerStore::open(dir.path(), &worker()).unwrap();
    assert_eq!(version(dir.path()), "7");
    // What was dropped is listed for the agent, in bounded pages, until the
    // agent says its directory is gone.
    assert_eq!(
        store.dropped_attempts(2).unwrap(),
        vec!["att_done1", "att_done2"]
    );
    assert_eq!(store.dropped_attempts(10).unwrap().len(), 3);
    assert!(store.forget_dropped_attempt("att_done1").unwrap());
    assert!(!store.forget_dropped_attempt("att_done1").unwrap());
    assert_eq!(
        store.dropped_attempts(10).unwrap(),
        vec!["att_done2", "att_done3"]
    );
}

#[test]
fn a_worker_database_without_live_attempts_migrates_and_drops_what_had_no_owner() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let old = step_3(dir.path());
    old_attempt(&old, "att_done", "terminal");
    collected(&old, "att_done");
    old.write(|tx| {
        tx.execute_batch(
            "INSERT INTO tombstone VALUES ('att_gone', 'retire', 9, 3);
             INSERT INTO lease VALUES ('att_done', 9, 4, 2, NULL);
             INSERT INTO log_spool VALUES ('att_done', 'stdout', 7, 0, 1);
             INSERT INTO command_launch (attempt_id, node_id, intent_at_ms) VALUES ('att_done', 'n', 2);
             INSERT INTO blob_ref (ref_id, attempt_id, kind, locator, created_at_ms)
                VALUES ('r1', 'att_done', 'log', 'att_done/stdout', 2),
                       ('r2', 'att_done', 'result', 'att_done/stdout', 2);",
        )?;
        Ok(())
    })
    .unwrap();
    let last_seq = count(&old, "SELECT max(seq) FROM event");
    drop(old);

    let store = WorkerStore::open(dir.path(), &worker()).unwrap();
    assert_eq!(version(dir.path()), "7");
    assert_eq!(store.worker_id(), &worker());
    for table in [
        "worker_attempt",
        "tombstone",
        "lease",
        "log_spool",
        "command_launch",
        "blob_ref",
        "member_epoch",
        "group_key",
        "group_member",
        "retired_identity",
        "upgrade_hold",
        "worker_identity_tail",
    ] {
        let n: i64 = store
            .db
            .call(move |c| {
                Ok(c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    // The epoch is counted again per member, from nothing.
    assert_eq!(store.max_coordinator_epoch().unwrap(), CoordinatorEpoch(0));
    // The dropped attempt is named, so that its directory can go.
    assert_eq!(store.dropped_attempts(10).unwrap(), vec!["att_done"]);
    assert!(!store.attempt_id_held("att_done").unwrap());
    // The files the dropped references named are deleted, once each.
    assert_eq!(store.pending_deletes().unwrap(), vec!["att_done/stdout"]);
    // The journal keeps counting where it was, and the agent epoch stays.
    let seq = store
        .append_event(None, None, "agent_note", &json!({}), 9)
        .unwrap();
    assert_eq!(seq as i64, last_seq + 1);
    assert_eq!(store.start_agent(10).unwrap(), AgentEpoch(3));
    // An attempt with the old id is a new attempt of whoever submits it, and
    // its journal starts at 1: the old events belong to no member.
    let member = store.for_member(&CoordinatorId::generate());
    let a = AttemptId::generate();
    member
        .submit(acceptance(&a, &TaskId::generate(), 'a', 1), 11)
        .unwrap();
    let events = member.events_of_attempt(&a, 10).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].attempt_seq, Some(1));
    // Opening again changes nothing.
    drop((store, member));
    WorkerStore::open(dir.path(), &worker()).unwrap();
    assert_eq!(version(dir.path()), "7");
}

struct Two {
    _dir: tempfile::TempDir,
    own: WorkerStore,
    a: WorkerStore,
    b: WorkerStore,
}

fn two() -> Two {
    let dir = crate::test_tmp::tempdir().unwrap();
    let own = WorkerStore::open(dir.path(), &worker()).unwrap();
    let a = own.for_member(&CoordinatorId::generate());
    let b = own.for_member(&CoordinatorId::generate());
    Two {
        _dir: dir,
        own,
        a,
        b,
    }
}

#[test]
fn a_member_reaches_only_its_own_attempts_and_the_worker_finds_any_by_id() {
    let t = two();
    let (x, task) = (AttemptId::generate(), TaskId::generate());
    let SubmitOutcome::Accepted(rec) = t.a.submit(acceptance(&x, &task, 'a', 1), 5).unwrap() else {
        panic!()
    };
    assert_eq!(Some(rec.coordinator_id.clone()), t.a.member());
    // The other member does not see it, by any reading.
    assert!(t.b.get(&x).unwrap().is_none());
    assert!(t.b.tombstone(&x).unwrap().is_none());
    assert!(t.b.get_result(&x).unwrap().is_none());
    assert!(t.b.lease(&x).unwrap().is_none());
    assert!(t.b.events_of_attempt(&x, 10).unwrap().is_empty());
    assert!(t.b.list_attempts(None, true, 10).unwrap().is_empty());
    assert!(t.b.list_nonterminal_of_member().unwrap().is_empty());
    assert_eq!(t.a.list_attempts(None, true, 10).unwrap().len(), 1);
    for e in [
        t.b.transition(&x, AttemptState::Preparing, 6)
            .err()
            .unwrap(),
        t.b.renew_lease(
            &x,
            crate::wire::LeaseKey {
                coordinator_epoch: CoordinatorEpoch(1),
                lease_seq: 1,
            },
            6,
        )
        .err()
        .unwrap(),
        t.b.ack_collected(&x, CoordinatorEpoch(1), 6).err().unwrap(),
        t.b.expire_lease(&x, 6).err().unwrap(),
    ] {
        assert_eq!(e.code(), ErrorCode::NotFound, "{e:?}");
    }
    // Its cancel and retire are about an attempt of its own that never
    // arrived: the first member's attempt is neither cancelled nor retired.
    assert_eq!(
        t.b.cancel(&x, CoordinatorEpoch(1), 7).unwrap(),
        CancelOutcome::Tombstoned
    );
    assert_eq!(t.b.retire(&x, CoordinatorEpoch(1), 8).unwrap(), None);
    let mine = t.a.get(&x).unwrap().unwrap();
    assert!(!mine.retired);
    assert_eq!(mine.cancel_requested_at_ms, None);
    assert!(t.a.tombstone(&x).unwrap().is_none());
    assert_eq!(t.b.tombstone(&x).unwrap().as_deref(), Some("retire"));
    // The worker's own handle (the agent, the supervisor) finds it by id
    // and writes the first member's row.
    assert_eq!(
        t.own.get(&x).unwrap().unwrap().coordinator_id,
        mine.coordinator_id
    );
    t.own.transition(&x, AttemptState::Preparing, 9).unwrap();
    assert_eq!(t.a.get(&x).unwrap().unwrap().state, AttemptState::Preparing);
    assert_eq!(t.own.list_nonterminal().unwrap().len(), 1);
    assert_eq!(t.own.count_nonterminal().unwrap(), 1);
    assert_eq!(t.a.events_of_attempt(&x, 10).unwrap().len(), 2);
}

#[test]
fn tombstones_and_task_ids_are_per_member_and_an_accepted_attempt_id_is_held_by_one() {
    let t = two();
    let (x, task) = (AttemptId::generate(), TaskId::generate());
    // A retire of one member that arrived first refuses that member's late
    // submit, and nobody else's.
    assert_eq!(t.a.retire(&x, CoordinatorEpoch(1), 1).unwrap(), None);
    let e = t.a.submit(acceptance(&x, &task, 'a', 1), 2).err().unwrap();
    assert!(matches!(e, StorageError::Retired(_)), "{e:?}");
    assert!(matches!(
        t.b.submit(acceptance(&x, &task, 'b', 1), 3).unwrap(),
        SubmitOutcome::Accepted(_)
    ));
    // The same task id is one live attempt per member, not per worker.
    let other = AttemptId::generate();
    let e =
        t.b.submit(acceptance(&other, &task, 'b', 1), 4)
            .err()
            .unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(_)), "{e:?}");
    let t2 = two();
    let (p, q) = (AttemptId::generate(), AttemptId::generate());
    t2.a.submit(acceptance(&p, &task, 'a', 1), 1).unwrap();
    t2.b.submit(acceptance(&q, &task, 'a', 1), 1).unwrap();
    assert_eq!(t2.own.count_nonterminal().unwrap(), 2);
    // An accepted attempt id is one member's on a worker: a second member
    // is refused the id, with nothing of the first one's attempt in the
    // answer and nothing stored. It is neither a conflict nor a retire.
    let e =
        t2.b.submit(acceptance(&p, &TaskId::generate(), 'c', 1), 2)
            .err()
            .unwrap();
    assert!(matches!(&e, StorageError::AttemptIdInUse(_)), "{e:?}");
    assert_eq!(e.code(), ErrorCode::Overloaded);
    assert!(t2.b.get(&p).unwrap().is_none());
    assert_eq!(t2.a.get(&p).unwrap().unwrap().payload_hash, hash('a'));
}

#[test]
fn the_coordinator_epoch_is_each_members_own() {
    let t = two();
    let task = TaskId::generate();
    t.a.submit(acceptance(&AttemptId::generate(), &task, 'a', 7), 1)
        .unwrap();
    assert_eq!(t.a.max_coordinator_epoch().unwrap(), CoordinatorEpoch(7));
    assert_eq!(t.b.max_coordinator_epoch().unwrap(), CoordinatorEpoch(0));
    // An older epoch of the other member is not stale.
    let x = AttemptId::generate();
    t.b.submit(acceptance(&x, &TaskId::generate(), 'a', 2), 2)
        .unwrap();
    assert_eq!(t.b.max_coordinator_epoch().unwrap(), CoordinatorEpoch(2));
    // An older epoch of the same member is.
    let e = t.a.cancel(&x, CoordinatorEpoch(6), 3).err().unwrap();
    assert!(
        matches!(e, StorageError::StaleEpoch { got, current } if got.0 == 6 && current.0 == 7),
        "{e:?}"
    );
    assert_eq!(t.a.max_coordinator_epoch().unwrap(), CoordinatorEpoch(7));
    // The lease key of an attempt is its member's.
    let key = |e, s| crate::wire::LeaseKey {
        coordinator_epoch: CoordinatorEpoch(e),
        lease_seq: s,
    };
    assert_eq!(
        t.b.renew_lease(&x, key(2, 1), 4).unwrap(),
        LeaseOutcome::Extended
    );
    assert_eq!(t.b.lease(&x).unwrap().unwrap().0, key(2, 1));
    assert!(t.a.lease(&x).unwrap().is_none());
}

#[test]
fn a_tombstone_of_an_attempt_that_never_arrived_goes_with_the_epoch_and_an_accepted_one_stays() {
    let t = two();
    let (never, known) = (AttemptId::generate(), AttemptId::generate());
    t.a.submit(acceptance(&known, &TaskId::generate(), 'a', 1), 1)
        .unwrap();
    t.a.cancel(&never, CoordinatorEpoch(1), 2).unwrap();
    t.a.retire(&known, CoordinatorEpoch(1), 3).unwrap();
    t.b.retire(&never, CoordinatorEpoch(1), 3).unwrap();
    assert_eq!(t.a.count_undelivered_tombstones().unwrap(), 1);
    // SQL cannot turn one kind of tombstone into the other (the tombstone of
    // an accepted attempt goes with it from stage 3 on, spec 25.6).
    let sql = |q: &'static str| t.own.db.call(move |c| Ok(c.execute(q, [])?));
    assert!(sql("UPDATE tombstone SET delivered = 0").is_err());
    assert!(sql("UPDATE tombstone SET delivered = 1").is_err());
    assert!(sql("UPDATE tombstone SET coordinator_id = 'co_x'").is_err());
    // The same epoch again keeps everything.
    t.a.cancel(&AttemptId::generate(), CoordinatorEpoch(1), 4)
        .unwrap();
    assert_eq!(t.a.count_undelivered_tombstones().unwrap(), 2);
    // The member's epoch moves on: its older undelivered tombstones go (a
    // late submit under the old epoch is stale), the accepted one and the
    // other member's stay.
    t.a.cancel(&AttemptId::generate(), CoordinatorEpoch(2), 5)
        .unwrap();
    assert_eq!(t.a.count_undelivered_tombstones().unwrap(), 1);
    assert!(t.a.tombstone(&never).unwrap().is_none());
    assert_eq!(t.a.tombstone(&known).unwrap().as_deref(), Some("retire"));
    assert_eq!(t.b.tombstone(&never).unwrap().as_deref(), Some("retire"));
    let e =
        t.a.submit(acceptance(&never, &TaskId::generate(), 'a', 1), 6)
            .err()
            .unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
}

#[test]
fn a_member_holds_a_bounded_number_of_undelivered_tombstones() {
    let t = two();
    let member = t.a.member().unwrap().to_string();
    t.own
        .db
        .write(move |tx| {
            tx.execute(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2)
                 INSERT INTO tombstone (coordinator_id, attempt_id, kind, coordinator_epoch, recorded_at_ms)
                 SELECT ?1, 'att_' || i, 'cancel', 1, 1 FROM n",
                rusqlite::params![member, MAX_UNDELIVERED_TOMBSTONES as i64 - 1],
            )?;
            Ok(())
        })
        .unwrap();
    let (last, over) = (AttemptId::generate(), AttemptId::generate());
    t.a.cancel(&last, CoordinatorEpoch(1), 2).unwrap();
    assert_eq!(
        t.a.count_undelivered_tombstones().unwrap(),
        MAX_UNDELIVERED_TOMBSTONES
    );
    for e in [
        t.a.cancel(&over, CoordinatorEpoch(1), 3).err().unwrap(),
        t.a.retire(&over, CoordinatorEpoch(1), 3).err().unwrap(),
    ] {
        assert!(matches!(e, StorageError::Overloaded(_)), "{e:?}");
        assert_eq!(e.code(), ErrorCode::Overloaded);
    }
    assert!(
        t.a.tombstone(&over).unwrap().is_none(),
        "nothing was stored"
    );
    // What is already recorded is answered again, and may become a retire.
    assert_eq!(
        t.a.cancel(&last, CoordinatorEpoch(1), 4).unwrap(),
        CancelOutcome::Tombstoned
    );
    assert_eq!(t.a.retire(&last, CoordinatorEpoch(1), 4).unwrap(), None);
    // An accepted attempt is retired whatever the count, and the other
    // member has its own count.
    let known = AttemptId::generate();
    t.a.submit(acceptance(&known, &TaskId::generate(), 'a', 1), 5)
        .unwrap();
    assert!(t
        .a
        .retire(&known, CoordinatorEpoch(1), 6)
        .unwrap()
        .is_some());
    assert_eq!(
        t.b.cancel(&over, CoordinatorEpoch(1), 7).unwrap(),
        CancelOutcome::Tombstoned
    );
    // The next epoch makes room.
    t.a.cancel(&over, CoordinatorEpoch(2), 8).unwrap();
    assert_eq!(t.a.count_undelivered_tombstones().unwrap(), 1);
}

#[test]
fn the_single_key_becomes_generation_one_and_a_rotation_is_number_and_fingerprint() {
    let t = two();
    assert_eq!(t.own.group_keys().unwrap(), (None, None));
    let generation = |n, f: &str| KeyGeneration {
        generation: n,
        fingerprint: f.into(),
    };
    assert_eq!(
        t.own.observe_group_key("fp-1").unwrap(),
        generation(1, "fp-1")
    );
    assert_eq!(
        t.own.observe_group_key("fp-1").unwrap(),
        generation(1, "fp-1")
    );
    // The next key of a rotation: the same one again is fine, another is not.
    t.own.set_next_group_key(&generation(5, "fp-5")).unwrap();
    t.own.set_next_group_key(&generation(5, "fp-5")).unwrap();
    let e = t
        .own
        .set_next_group_key(&generation(6, "fp-6"))
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::Immutable(_)), "{e:?}");
    assert_eq!(
        t.own.group_keys().unwrap(),
        (Some(generation(1, "fp-1")), Some(generation(5, "fp-5")))
    );
    // The key file now holds the next key: it is the current generation,
    // with the number it was given, and no next key is left.
    assert_eq!(
        t.own.observe_group_key("fp-5").unwrap(),
        generation(5, "fp-5")
    );
    assert_eq!(
        t.own.group_keys().unwrap(),
        (Some(generation(5, "fp-5")), None)
    );
    // A key that was sent again outside a rotation is the following one.
    assert_eq!(
        t.own.observe_group_key("fp-x").unwrap(),
        generation(6, "fp-x")
    );
}

#[test]
fn a_members_name_and_coordinator_id_are_fixed_together() {
    let t = two();
    assert_eq!(t.a.pin_member("main", 1).unwrap(), MemberPin::Pinned);
    assert_eq!(t.a.pin_member("main", 2).unwrap(), MemberPin::Pinned);
    assert_eq!(
        t.b.pin_member("main", 3).unwrap(),
        MemberPin::NameTaken {
            coordinator_id: t.a.member().unwrap().to_string()
        }
    );
    assert_eq!(
        t.a.pin_member("sub", 3).unwrap(),
        MemberPin::IdTaken {
            name: "main".into()
        }
    );
    assert_eq!(
        t.own.members().unwrap().len(),
        1,
        "a refusal records nothing"
    );
    assert_eq!(t.b.pin_member("sub", 4).unwrap(), MemberPin::Pinned);
    // Only the explicit replacement moves a name.
    t.b.replace_member_pin("main", 5).unwrap();
    assert_eq!(
        t.own.members().unwrap(),
        vec![("main".to_string(), t.b.member().unwrap().to_string())]
    );
}

#[test]
fn a_retired_identity_is_recorded_once_per_member() {
    let t = two();
    let boot = WorkerBootId::parse("boot1").unwrap();
    assert!(!t.a.identity_retired(&boot).unwrap());
    assert!(t.a.record_identity_retired(&boot, 1).unwrap());
    assert!(!t.a.record_identity_retired(&boot, 2).unwrap());
    assert!(t.a.identity_retired(&boot).unwrap());
    assert!(!t.b.identity_retired(&boot).unwrap());
    assert!(!t
        .a
        .identity_retired(&WorkerBootId::parse("boot2").unwrap())
        .unwrap());
}

/// The directories of the attempts step 4 dropped are removed by the agent:
/// only entries below the attempts root that are named by a dropped id,
/// never through a link, never one a member holds again, and a pass that
/// could not finish goes on later.
#[test]
fn the_directories_of_dropped_attempts_are_removed_and_nothing_else() {
    use crate::worker::agent::{remove_dropped_attempts, DroppedReport};
    use std::os::unix::fs::PermissionsExt;
    let dir = crate::test_tmp::tempdir().unwrap();
    let root = dir.path().join("attempts");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("keep"), b"x").unwrap();
    let ids: Vec<String> = (0..6).map(|_| AttemptId::generate().to_string()).collect();
    let [plain, link, absent, held, stuck, untouched] = &ids[..] else {
        unreachable!()
    };
    let old = step_3(dir.path());
    for id in [plain, link, absent, held, stuck] {
        old_attempt(&old, id, "terminal");
        collected(&old, id);
    }
    // A name that is not an attempt id, as old test data has it.
    old_attempt(&old, "att_done", "terminal");
    collected(&old, "att_done");
    drop(old);
    // An ordinary attempt directory, with a link out of the area inside.
    std::fs::create_dir_all(root.join(plain).join("artifacts")).unwrap();
    std::fs::write(root.join(plain).join("supervise.json"), b"{}").unwrap();
    std::fs::write(root.join(plain).join("workspace.json"), b"{}").unwrap();
    std::os::unix::fs::symlink(&outside, root.join(plain).join("artifacts/out")).unwrap();
    // A link where the directory would be.
    std::os::unix::fs::symlink(&outside, root.join(link)).unwrap();
    for id in [held, stuck, untouched, &"att_done".to_string()] {
        std::fs::create_dir_all(root.join(id).join("sub")).unwrap();
        std::fs::write(root.join(id).join("supervise.json"), b"{}").unwrap();
    }

    let store = WorkerStore::open(dir.path(), &worker()).unwrap();
    // A member submitted an attempt under one of the old ids since.
    store
        .for_member(&CoordinatorId::generate())
        .submit(
            acceptance(
                &AttemptId::parse(held).unwrap(),
                &TaskId::generate(),
                'a',
                1,
            ),
            11,
        )
        .unwrap();
    // One directory cannot be emptied for now.
    let locked = root.join(stuck).join("sub");
    std::fs::write(locked.join("f"), b"x").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    let root_user = rustix::process::geteuid().is_root();

    let first = remove_dropped_attempts(&root, &store).unwrap();
    assert!(!root.join(plain).exists());
    assert!(std::fs::symlink_metadata(root.join(link)).is_err());
    assert!(outside.join("keep").exists(), "nothing behind a link went");
    assert!(root.join(held).join("supervise.json").exists());
    assert!(root.join(untouched).join("supervise.json").exists());
    assert!(root.join("att_done").join("supervise.json").exists());
    if !root_user {
        assert_eq!(first.removed, 2);
        assert_eq!(first.skipped, 3, "no directory, not an id, held again");
        assert_eq!(first.failed.len(), 1);
        assert_eq!(&first.failed[0].0, stuck);
        assert_eq!(store.dropped_attempts(10).unwrap(), vec![stuck.clone()]);
        assert!(root.join(stuck).exists());
        // The next start goes on where this one stopped.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
        let second = remove_dropped_attempts(&root, &store).unwrap();
        assert_eq!(
            second,
            DroppedReport {
                removed: 1,
                skipped: 0,
                failed: vec![]
            }
        );
    }
    assert!(!root.join(stuck).exists());
    assert_eq!(store.dropped_attempts(10).unwrap(), Vec::<String>::new());
    assert_eq!(
        remove_dropped_attempts(&root, &store).unwrap(),
        DroppedReport::default()
    );
    assert!(root.join(untouched).join("supervise.json").exists());
}

/// The attempt's files, its supervisor and its commands are named by the
/// attempt id alone, so one worker holds an attempt id for one member at a
/// time. The second member is told so with a refusal that stores nothing
/// (not a verdict on its task), and nothing it does about that id reaches
/// the first member's attempt.
/// A52: the window is one budget over every member, and a submit beyond it
/// leaves nothing, no tombstone and no newer epoch either.
#[test]
fn a52_the_window_counts_every_member_and_a_refused_submit_stores_nothing() {
    let t = two();
    let (first, second) = (AttemptId::generate(), AttemptId::generate());
    let submit = |s: &WorkerStore, a: &AttemptId, epoch: u64, at: u64| {
        s.submit_within(acceptance(a, &TaskId::generate(), 'a', epoch), Some(2), at)
    };
    submit(&t.a, &first, 1, 1).unwrap();
    submit(&t.b, &second, 2, 2).unwrap();
    t.b.cancel(&AttemptId::generate(), CoordinatorEpoch(2), 3)
        .unwrap();
    let rows = |q: &'static str| {
        t.own
            .db
            .call(move |c| Ok(c.query_row(q, [], |r| r.get::<_, i64>(0))?))
            .unwrap()
    };
    let before = (
        rows("SELECT count(*) FROM worker_attempt"),
        rows("SELECT count(*) FROM tombstone"),
        rows("SELECT count(*) FROM event"),
    );
    // One of each member fills a window of two: a third is refused whoever
    // sends it, also under a newer epoch.
    let third = AttemptId::generate();
    for store in [&t.a, &t.b] {
        let e = submit(store, &third, 9, 4).err().unwrap();
        assert!(
            matches!(e, StorageError::Capacity { live: 2, window: 2 }),
            "{e:?}"
        );
        assert_eq!(e.code(), ErrorCode::Overloaded);
        assert!(e.leaves_nothing() && !e.is_retryable());
        assert!(store.get(&third).unwrap().is_none());
        assert!(store.tombstone(&third).unwrap().is_none());
    }
    let after = (
        rows("SELECT count(*) FROM worker_attempt"),
        rows("SELECT count(*) FROM tombstone"),
        rows("SELECT count(*) FROM event"),
    );
    assert_eq!(after, before);
    assert_eq!(t.a.max_coordinator_epoch().unwrap(), CoordinatorEpoch(1));
    assert_eq!(t.b.max_coordinator_epoch().unwrap(), CoordinatorEpoch(2));
    assert_eq!(t.b.count_undelivered_tombstones().unwrap(), 1);
    // A resend of an accepted attempt adds nothing and is not refused.
    let again = t.a.get(&first).unwrap().unwrap();
    let resend = NewAcceptance {
        run_id: again.run_id.clone(),
        task_id: again.task_id.clone(),
        ..acceptance(&first, &again.task_id, 'a', 1)
    };
    assert!(matches!(
        t.a.submit_within(resend, Some(2), 5).unwrap(),
        SubmitOutcome::Existing(_)
    ));
    // What is about the attempt is answered before the window: a tombstone,
    // an older epoch, an id in use, a live attempt of the task.
    let gone = AttemptId::generate();
    t.a.retire(&gone, CoordinatorEpoch(1), 6).unwrap();
    let e = submit(&t.a, &gone, 1, 7).err().unwrap();
    assert!(matches!(e, StorageError::Retired(_)), "{e:?}");
    let e = submit(&t.b, &third, 1, 7).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    let e = submit(&t.b, &first, 2, 7).err().unwrap();
    assert!(matches!(e, StorageError::AttemptIdInUse(_)), "{e:?}");
    let live = NewAcceptance {
        task_id: again.task_id.clone(),
        ..acceptance(&third, &again.task_id, 'a', 1)
    };
    let e = t.a.submit_within(live, Some(2), 7).err().unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(_)), "{e:?}");
    // A wider window, or none, takes it; and an attempt that ended gives
    // its place back.
    assert!(submit(&t.a, &third, 1, 8).is_err());
    assert!(matches!(
        t.a.submit_within(acceptance(&third, &TaskId::generate(), 'a', 1), Some(3), 8)
            .unwrap(),
        SubmitOutcome::Accepted(_)
    ));
    assert!(matches!(
        t.b.submit(
            acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 2),
            9
        )
        .unwrap(),
        SubmitOutcome::Accepted(_)
    ));
}

#[test]
fn an_attempt_id_another_member_holds_is_refused_and_nothing_is_stored() {
    let t = two();
    let (x, task) = (AttemptId::generate(), TaskId::generate());
    t.a.submit(acceptance(&x, &task, 'a', 1), 1).unwrap();
    t.b.cancel(&AttemptId::generate(), CoordinatorEpoch(2), 2)
        .unwrap();
    let before = t.a.get(&x).unwrap().unwrap();
    let rows = |q: &'static str| {
        t.own
            .db
            .call(move |c| Ok(c.query_row(q, [], |r| r.get::<_, i64>(0))?))
            .unwrap()
    };
    let (attempts, tombstones, events) = (
        rows("SELECT count(*) FROM worker_attempt"),
        rows("SELECT count(*) FROM tombstone"),
        rows("SELECT count(*) FROM event"),
    );
    // The same id, whatever else the submit says, and under a newer epoch.
    for (task_b, payload) in [(task.clone(), 'a'), (TaskId::generate(), 'b')] {
        let e =
            t.b.submit(acceptance(&x, &task_b, payload, 7), 3)
                .err()
                .unwrap();
        assert!(
            matches!(&e, StorageError::AttemptIdInUse(id) if id == &x),
            "{e:?}"
        );
        assert_eq!(e.code(), ErrorCode::Overloaded);
        assert!(e.leaves_nothing() && !e.is_retryable());
    }
    assert_eq!(rows("SELECT count(*) FROM worker_attempt"), attempts);
    assert_eq!(rows("SELECT count(*) FROM tombstone"), tombstones);
    assert_eq!(rows("SELECT count(*) FROM event"), events);
    assert!(t.b.get(&x).unwrap().is_none());
    // Not even the epoch the refused submit carried, and what a newer epoch
    // purges is still there.
    assert_eq!(t.b.max_coordinator_epoch().unwrap(), CoordinatorEpoch(2));
    assert_eq!(t.b.count_undelivered_tombstones().unwrap(), 1);
    // The member takes the attempt off the worker: a tombstone of its own,
    // which refuses a late copy of its submit. The first member's attempt is
    // as it was.
    assert_eq!(t.b.retire(&x, CoordinatorEpoch(2), 4).unwrap(), None);
    assert_eq!(t.b.tombstone(&x).unwrap().as_deref(), Some("retire"));
    let e = t.b.submit(acceptance(&x, &task, 'a', 2), 5).err().unwrap();
    assert!(matches!(e, StorageError::Retired(_)), "{e:?}");
    assert_eq!(t.a.get(&x).unwrap().unwrap(), before);
    assert!(t.a.tombstone(&x).unwrap().is_none());
    // And goes on under a new attempt id, for the same task.
    let y = AttemptId::generate();
    assert!(matches!(
        t.b.submit(acceptance(&y, &task, 'a', 2), 6).unwrap(),
        SubmitOutcome::Accepted(_)
    ));
    // The first member's own resend still finds its attempt.
    assert!(matches!(
        t.a.submit(
            NewAcceptance {
                run_id: before.run_id.clone(),
                ..acceptance(&x, &task, 'a', 1)
            },
            7
        )
        .unwrap(),
        SubmitOutcome::Existing(_)
    ));
}

/// A refusal that says "nothing was done" leaves nothing: not the newer
/// epoch it carried, and the tombstones that epoch would have purged stay.
/// A verdict about the attempt keeps the epoch, as before.
#[test]
fn a_refused_request_leaves_the_epoch_and_the_tombstones_as_they_were() {
    let t = two();
    let (live, task, never) = (
        AttemptId::generate(),
        TaskId::generate(),
        AttemptId::generate(),
    );
    t.a.submit(acceptance(&live, &task, 'a', 1), 1).unwrap();
    t.a.cancel(&never, CoordinatorEpoch(1), 2).unwrap();
    let state = || {
        (
            t.a.max_coordinator_epoch().unwrap(),
            t.a.count_undelivered_tombstones().unwrap(),
            t.a.tombstone(&never).unwrap(),
        )
    };
    let before = state();
    assert_eq!(before.0, CoordinatorEpoch(1));
    // Another attempt of the task while one is live, under a newer epoch.
    let e =
        t.a.submit(acceptance(&AttemptId::generate(), &task, 'a', 4), 3)
            .err()
            .unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(_)), "{e:?}");
    assert_eq!(state(), before);
    // So the epoch in between is not stale. Another payload for the live
    // attempt is a verdict: its epoch is kept, and the purge with it.
    let e =
        t.a.submit(acceptance(&live, &task, 'b', 2), 4)
            .err()
            .unwrap();
    assert!(matches!(e, StorageError::PayloadConflict(_)), "{e:?}");
    assert_eq!(t.a.max_coordinator_epoch().unwrap(), CoordinatorEpoch(2));
    assert!(t.a.tombstone(&never).unwrap().is_none());
    let e = t.a.cancel(&live, CoordinatorEpoch(1), 5).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
}

/// The tombstone limit refuses without a trace as well, and a newer epoch
/// makes room before the limit is looked at.
#[test]
fn the_tombstone_limit_leaves_nothing_and_a_newer_epoch_makes_room_first() {
    let t = two();
    let member = t.a.member().unwrap().to_string();
    t.own
        .db
        .write(move |tx| {
            tx.execute(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2)
                 INSERT INTO tombstone (coordinator_id, attempt_id, kind, coordinator_epoch, recorded_at_ms)
                 SELECT ?1, 'att_' || i, 'cancel', 3, 1 FROM n",
                rusqlite::params![member, MAX_UNDELIVERED_TOMBSTONES as i64],
            )?;
            tx.execute(
                "INSERT INTO member_epoch (coordinator_id, max_epoch) VALUES (?1, 2)",
                [&member],
            )?;
            Ok(())
        })
        .unwrap();
    // The tombstones are of epoch 3, so epoch 3 purges none of them: the
    // request is refused, and the epoch it carried is not kept.
    let over = AttemptId::generate();
    let e = t.a.retire(&over, CoordinatorEpoch(3), 2).err().unwrap();
    assert!(matches!(e, StorageError::Overloaded(_)), "{e:?}");
    assert!(e.leaves_nothing());
    assert_eq!(t.a.max_coordinator_epoch().unwrap(), CoordinatorEpoch(2));
    assert_eq!(
        t.a.count_undelivered_tombstones().unwrap(),
        MAX_UNDELIVERED_TOMBSTONES
    );
    // Epoch 4 purges them and is then served.
    assert_eq!(t.a.retire(&over, CoordinatorEpoch(4), 3).unwrap(), None);
    assert_eq!(t.a.count_undelivered_tombstones().unwrap(), 1);
    assert_eq!(t.a.max_coordinator_epoch().unwrap(), CoordinatorEpoch(4));
}

#[test]
fn an_accepted_attempt_records_its_source_and_the_migration_fills_live_rows_only() {
    // spec 25.8 5b (a), 25.13: the deletion pass finds a source's live
    // attempts by the column, so a new row has it from its insert...
    let t = two();
    let x = AttemptId::generate();
    let mut new = acceptance(&x, &TaskId::generate(), 'a', 1);
    new.spec = json!({"cmd": "true", "source_id": "src_01JABCDEFGHJKMNPQRSTVWXYZ0"});
    t.a.submit(new, 5).unwrap();
    assert_eq!(
        count(
            &t.own.db,
            "SELECT count(*) FROM worker_attempt WHERE source_id='src_01JABCDEFGHJKMNPQRSTVWXYZ0'"
        ),
        1
    );
    // ...and the migration fills it from `spec_json` for the live rows of
    // a database made before it, not for the terminal ones
    let dir = crate::test_tmp::tempdir().unwrap();
    let steps = schema::steps(DbKind::Worker);
    let db = db::open_with(
        &dir.path().join("worker.db"),
        "worker",
        &steps[..6],
        schema::compat(6),
        Access::ReadWrite,
    )
    .unwrap();
    db.write(|tx| {
        for (id, state) in [("live", "accepted"), ("done", "terminal")] {
            tx.execute(
                "INSERT INTO worker_attempt (attempt_id, run_id, task_id, generation, payload_hash,
                    coordinator_epoch, spec_json, state, termination_json, receipts_json,
                    accepted_at_ms, terminal_at_ms, coordinator_id)
                 VALUES (?1, 'run_x', ?1, 1, 'h', 9, '{\"source_id\":\"src_s\"}', ?2, ?3, '{}', 1, ?4, 'c')",
                rusqlite::params![
                    id,
                    state,
                    (state == "terminal").then_some("{}"),
                    (state == "terminal").then_some(5_i64)
                ],
            )?;
        }
        Ok(())
    })
    .unwrap();
    drop(db);
    let db = db::open_with(
        &dir.path().join("worker.db"),
        "worker",
        &steps,
        schema::compat(steps.len()),
        Access::ReadWrite,
    )
    .unwrap();
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM worker_attempt WHERE attempt_id='live' AND source_id='src_s'"
        ),
        1
    );
    assert_eq!(
        count(
            &db,
            "SELECT count(*) FROM worker_attempt WHERE attempt_id='done' AND source_id IS NULL"
        ),
        1
    );
}
