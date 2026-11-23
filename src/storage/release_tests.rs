//! Stage 3 of the retention on the worker database (spec 25.6): what an
//! acknowledgement, a release and the ack wait delete, the dedup records
//! that stay, and `belt group forget`.

use super::*;
use crate::error::ErrorCode;
use crate::ids::*;
use crate::state::{AttemptState, Exit, Outcome, Termination};
use serde_json::json;

fn hash(c: char) -> PayloadHash {
    PayloadHash::parse(&format!("sha256:{}", c.to_string().repeat(64))).unwrap()
}

fn acceptance(a: &AttemptId, h: char) -> NewAcceptance {
    NewAcceptance {
        attempt_id: a.clone(),
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        payload_hash: hash(h),
        coordinator_epoch: CoordinatorEpoch(1),
        spec: json!({"cmd": "true"}),
    }
}

fn ok() -> Termination {
    Termination::new(Outcome::Succeeded, Some(Exit::Code(0)), None).unwrap()
}

struct T {
    _dir: tempfile::TempDir,
    own: WorkerStore,
    m: WorkerStore,
    member: CoordinatorId,
}

fn store() -> T {
    let dir = crate::test_tmp::tempdir().unwrap();
    let own = WorkerStore::open(dir.path(), &WorkerId::parse("mini-a").unwrap()).unwrap();
    let member = CoordinatorId::generate();
    let m = own.for_member(&member);
    T {
        _dir: dir,
        own,
        m,
        member,
    }
}

/// An accepted attempt that ended at `at`, with a log reference.
fn ended(t: &T, at: u64) -> (AttemptId, NewAcceptance) {
    let a = AttemptId::generate();
    let new = acceptance(&a, 'a');
    t.m.submit(new.clone(), at).unwrap();
    t.own
        .add_blob_ref(
            &NewBlobRef {
                ref_id: format!("log-{a}"),
                attempt_id: a.to_string(),
                kind: BlobKind::Log,
                locator: format!("{a}/log"),
            },
            at,
        )
        .unwrap();
    walk(&t.m, &a, at);
    (a, new)
}

/// Take an accepted attempt to terminal.
fn walk(m: &WorkerStore, a: &AttemptId, at: u64) {
    for state in [
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
        AttemptState::Finalizing,
    ] {
        m.transition(a, state, at).unwrap();
    }
    m.finalize(a, ok(), at).unwrap();
}

fn count(t: &T, sql: &'static str) -> i64 {
    t.own
        .db
        .call(move |c| Ok(c.query_row(sql, [], |r| r.get(0))?))
        .unwrap()
}

#[test]
fn an_acknowledgement_with_the_feature_deletes_the_attempt_and_leaves_a_released_record() {
    let t = store();
    let (a, new) = ended(&t, 10);
    assert!(t
        .m
        .ack_and_release(&a, CoordinatorEpoch(1), 20, false)
        .unwrap());
    assert!(t.m.get(&a).unwrap().is_none());
    assert!(t.m.events_of_attempt(&a, 10).unwrap().is_empty());
    for table in [
        "command_launch",
        "lease",
        "log_spool",
        "blob_ref",
        "tombstone",
    ] {
        let n: i64 = t
            .own
            .db
            .call(move |c| {
                Ok(c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    let rec = t.m.attempt_record(&a).unwrap().unwrap();
    assert!(rec.released);
    assert_eq!(rec.termination, Some(ok()));
    assert_eq!(t.own.dropped_attempts(10).unwrap(), vec![a.to_string()]);
    assert_eq!(t.own.pending_deletes().unwrap(), vec![format!("{a}/log")]);
    // A late copy of its submit is not run again.
    let e = t.m.submit(new, 30).err().unwrap();
    assert_eq!(e.code(), ErrorCode::Retired, "{e:?}");
    // Without the feature the row stays, as before.
    let (b, _) = ended(&t, 10);
    t.m.ack_collected_as_read(&b, CoordinatorEpoch(1), 20, false)
        .unwrap();
    assert!(t.m.get(&b).unwrap().unwrap().acked_at_ms.is_some());
    assert!(t.m.attempt_record(&b).unwrap().is_none());
}

#[test]
fn a_release_deletes_a_terminal_attempt_refuses_a_live_one_and_releases_a_record() {
    let t = store();
    let live = AttemptId::generate();
    t.m.submit(acceptance(&live, 'b'), 1).unwrap();
    let e =
        t.m.release_attempt(&live, CoordinatorEpoch(1), 2)
            .err()
            .unwrap();
    assert_eq!(e.code(), ErrorCode::Overloaded, "{e:?}");
    assert!(t.m.get(&live).unwrap().is_some());
    let (a, _) = ended(&t, 1);
    assert_eq!(
        t.m.release_attempt(&a, CoordinatorEpoch(1), 3).unwrap(),
        Released::Dropped
    );
    // Sent again while the record stays: the same answer.
    assert_eq!(
        t.m.release_attempt(&a, CoordinatorEpoch(1), 4).unwrap(),
        Released::Record
    );
    t.own
        .forget_released(vec![(t.member.clone(), a.clone())])
        .unwrap();
    let e =
        t.m.release_attempt(&a, CoordinatorEpoch(1), 5)
            .err()
            .unwrap();
    assert_eq!(e.code(), ErrorCode::NotFound);
    // Another member's release does not reach it.
    let (b, _) = ended(&t, 1);
    let other = t.own.for_member(&CoordinatorId::generate());
    assert_eq!(
        other
            .release_attempt(&b, CoordinatorEpoch(1), 5)
            .err()
            .unwrap()
            .code(),
        ErrorCode::NotFound
    );
    assert!(t.m.get(&b).unwrap().is_some());
}

#[test]
fn the_ack_wait_turns_rows_into_unreleased_records_that_only_a_release_frees() {
    let t = store();
    let (old, new) = ended(&t, 100);
    let (young, _) = ended(&t, 900);
    let gone = t.own.expire_unreleased(1_000, 500, 1_000).unwrap();
    assert_eq!(gone, vec![old.to_string()]);
    assert!(t.m.get(&young).unwrap().is_some());
    let rec = t.m.attempt_record(&old).unwrap().unwrap();
    assert!(!rec.released);
    // A late submit is refused; the record stays until released.
    assert_eq!(
        t.m.submit(new, 1_100).err().unwrap().code(),
        ErrorCode::Retired
    );
    assert_eq!(t.own.purge_released_records(1_000).unwrap(), 0);
    let mut both = vec![old.clone(), young.clone()];
    both.sort();
    assert_eq!(t.m.list_unreleased(None, 500).unwrap(), (both, false));
    assert_eq!(
        t.m.release_attempt(&old, CoordinatorEpoch(1), 1_200)
            .unwrap(),
        Released::Record
    );
    assert_eq!(t.own.purge_released_records(1_000).unwrap(), 1);
    assert!(t.m.attempt_record(&old).unwrap().is_none());
    assert_eq!(
        t.m.list_unreleased(None, 500).unwrap(),
        (vec![young], false)
    );
}

#[test]
fn list_unreleased_pages_through_more_than_six_hundred_attempts() {
    let t = store();
    let mut all = Vec::new();
    for i in 0..650 {
        let (a, _) = ended(&t, i);
        all.push(a);
    }
    // Half of them only left an unreleased record.
    t.own.expire_unreleased(10_000, 0, 325).unwrap();
    // Acknowledged or released ones are not listed.
    let (acked, _) = ended(&t, 1);
    t.m.ack_and_release(&acked, CoordinatorEpoch(1), 2, false)
        .unwrap();
    all.sort();
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let (page, more) = t.m.list_unreleased(after.as_ref(), 500).unwrap();
        assert!(page.len() <= 500);
        after = page.last().cloned();
        seen.extend(page);
        if !more {
            break;
        }
    }
    assert_eq!(seen, all);
}

#[test]
fn a_member_at_its_bound_of_records_is_refused_a_new_attempt_and_no_one_else() {
    let t = store();
    let full = MAX_ATTEMPT_RECORDS;
    let member = t.member.to_string();
    t.own
        .db
        .write(move |tx| {
            for i in 0..full {
                tx.execute(
                    "INSERT INTO attempt_record VALUES (?1, ?2, 'r', 't', 1, 'h', 1, NULL, 0, 0)",
                    rusqlite::params![member, format!("at_{i:08}")],
                )?;
            }
            Ok(())
        })
        .unwrap();
    let a = AttemptId::generate();
    let e = t.m.submit(acceptance(&a, 'c'), 1).err().unwrap();
    assert!(
        matches!(e, StorageError::AttemptRecords(n) if n == full),
        "{e:?}"
    );
    assert!(t.m.get(&a).unwrap().is_none());
    let other = t.own.for_member(&CoordinatorId::generate());
    other.submit(acceptance(&a, 'c'), 1).unwrap();
}

#[test]
fn the_rows_an_older_version_acknowledged_go_with_no_record_and_later_ones_stay() {
    let t = store();
    let (before, _) = ended(&t, 1);
    t.m.ack_collected(&before, CoordinatorEpoch(1), 5).unwrap();
    let (after, _) = ended(&t, 1);
    t.m.ack_collected(&after, CoordinatorEpoch(1), 50).unwrap();
    let (unacked, _) = ended(&t, 1);
    assert_eq!(
        t.own.drop_acked_before(10, 1_000).unwrap(),
        vec![before.to_string()]
    );
    assert!(t.m.get(&before).unwrap().is_none());
    assert!(t.m.attempt_record(&before).unwrap().is_none());
    assert!(t.m.get(&after).unwrap().is_some());
    assert!(t.m.get(&unacked).unwrap().is_some());
    assert_eq!(count(&t, "SELECT count(*) FROM attempt_record"), 0);
}

#[test]
fn events_of_no_attempt_go_by_age_and_an_attempts_events_stay_with_it() {
    let t = store();
    let (a, _) = ended(&t, 1);
    for at in [10, 20, 30] {
        t.own
            .append_event(None, None, "agent_started", &json!({}), at)
            .unwrap();
    }
    assert_eq!(t.own.prune_unowned_events(25, 1).unwrap(), 1);
    assert_eq!(t.own.prune_unowned_events(25, 1_000).unwrap(), 1);
    assert_eq!(
        count(&t, "SELECT count(*) FROM event WHERE attempt_id IS NULL"),
        1
    );
    assert!(!t.m.events_of_attempt(&a, 10).unwrap().is_empty());
}

#[test]
fn forgetting_a_member_deletes_its_rows_and_refuses_it_for_good() {
    let t = store();
    let b = t.own.for_member(&CoordinatorId::generate());
    let pinned = t.member.clone();
    let m = &t.m;
    m.pin_member("mac-b", 1).unwrap();
    let live = AttemptId::generate();
    m.submit(acceptance(&live, 'd'), 1).unwrap();
    let e = t.own.forget_member("mac-b", 2).err().unwrap();
    assert!(e.to_string().contains("1 attempts"), "{e}");
    walk(m, &live, 3);
    let done = AttemptId::generate();
    m.submit(acceptance(&done, 'e'), 1).unwrap();
    walk(m, &done, 4);
    m.release_attempt(&done, CoordinatorEpoch(1), 5).unwrap();
    let other = AttemptId::generate();
    b.submit(acceptance(&other, 'f'), 1).unwrap();
    let dropped = t.own.forget_member("mac-b", 6).unwrap();
    assert_eq!(dropped, vec![live.to_string()]);
    assert!(t.own.member_refused(&pinned).unwrap());
    assert!(m.get(&live).unwrap().is_none());
    assert!(m.attempt_record(&done).unwrap().is_none());
    assert!(t.own.member_pin("mac-b").unwrap().is_none());
    assert!(b.get(&other).unwrap().is_some());
    assert!(!t.own.member_refused(&b.member().unwrap()).unwrap());
    assert_eq!(
        t.own.forget_member("mac-b", 7).err().unwrap().code(),
        ErrorCode::NotFound
    );
}
