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
        assert!(message.starts_with("2 attempt(s)"), "{message}");
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

#[test]
fn a_worker_database_without_live_attempts_migrates_and_drops_what_had_no_owner() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let old = step_3(dir.path());
    old_attempt(&old, "att_done", "terminal");
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
    assert_eq!(version(dir.path()), "4");
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
    assert_eq!(version(dir.path()), "4");
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
fn the_same_ids_from_two_members_do_not_retire_or_conflict_with_each_other() {
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
    assert!(matches!(&e, StorageError::Invalid(_)), "{e:?}");
    assert_eq!(e.code(), ErrorCode::InvalidInput);
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
    // SQL cannot delete the tombstone of an accepted attempt, nor turn one
    // kind of tombstone into the other.
    let sql = |q: &'static str| t.own.db.call(move |c| Ok(c.execute(q, [])?));
    assert!(sql("DELETE FROM tombstone WHERE delivered = 1").is_err());
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
