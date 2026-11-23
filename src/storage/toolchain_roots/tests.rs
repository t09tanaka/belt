use super::*;
use crate::ids::{CoordinatorId, WorkerId};

const ROOT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn store() -> (tempfile::TempDir, WorkerStore) {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap();
    (dir, store)
}

fn attempt(dir: &tempfile::TempDir, state: &str) -> String {
    let db = rusqlite::Connection::open(dir.path().join("worker.db")).unwrap();
    crate::service::worker_service::tests::put_attempt(&db, &CoordinatorId::generate(), 1, state);
    db.query_row(
        "SELECT attempt_id FROM worker_attempt ORDER BY rowid DESC LIMIT 1",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

/// Spec 25.7 (b) acceptance (i), both orders: a bind before the claim
/// leaves a reference and the claim fails; a claim before the bind leaves
/// `evicting`, and the bind answers that the root is not there.
#[test]
fn a_bind_and_a_claim_are_ordered_one_way_or_the_other() {
    let (dir, s) = store();
    s.toolchain_installed(ROOT, 1).unwrap();
    let a = attempt(&dir, "active");
    assert_eq!(s.toolchain_bind(ROOT, &a, 2).unwrap(), Bind::Bound);
    assert!(!s.toolchain_claim(ROOT, None).unwrap());
    let b = attempt(&dir, "active");
    let db = rusqlite::Connection::open(dir.path().join("worker.db")).unwrap();
    db.execute("DELETE FROM toolchain_ref", []).unwrap();
    assert!(s.toolchain_claim(ROOT, None).unwrap());
    assert_eq!(s.toolchain_bind(ROOT, &b, 3).unwrap(), Bind::Absent);
    assert!(!s.toolchain_referenced(ROOT).unwrap());
    // A builder does not install into a root that is being removed.
    assert!(!s.toolchain_hold_for_install(ROOT, "builder").unwrap());
    s.toolchain_forget(ROOT).unwrap();
    assert!(s.toolchain_hold_for_install(ROOT, "builder").unwrap());
    assert!(s.toolchain_roots().unwrap().is_empty());
}

/// The reference of an attempt goes in the transaction that makes the
/// attempt terminal, and with its row; a terminal attempt binds nothing.
#[test]
fn an_attempt_reference_goes_when_the_attempt_ends_or_goes() {
    let (dir, s) = store();
    s.toolchain_installed(ROOT, 1).unwrap();
    let a = attempt(&dir, "active");
    assert_eq!(s.toolchain_bind(ROOT, &a, 2).unwrap(), Bind::Bound);
    let db = rusqlite::Connection::open(dir.path().join("worker.db")).unwrap();
    db.execute(
        "UPDATE worker_attempt SET state = 'terminal', termination_json = '{}', terminal_at_ms = 3 WHERE attempt_id = ?1",
        [&a],
    )
    .unwrap();
    assert!(!s.toolchain_referenced(ROOT).unwrap());
    let b = attempt(&dir, "active");
    assert_eq!(s.toolchain_bind(ROOT, &b, 2).unwrap(), Bind::Bound);
    db.execute("DELETE FROM worker_attempt WHERE attempt_id = ?1", [&b])
        .unwrap();
    assert!(!s.toolchain_referenced(ROOT).unwrap());
    let c = attempt(&dir, "terminal");
    assert_eq!(s.toolchain_bind(ROOT, &c, 2).unwrap(), Bind::Absent);
    assert!(!s.toolchain_referenced(ROOT).unwrap());
}

/// A consumer the worker has no attempt row for is a warm-up; its
/// reference goes when it is released.
#[test]
fn a_warm_up_reference_holds_until_released() {
    let (_dir, s) = store();
    s.toolchain_installed(ROOT, 1).unwrap();
    assert_eq!(s.toolchain_bind(ROOT, "warm", 5).unwrap(), Bind::Bound);
    assert_eq!(s.toolchain_holders("warmup").unwrap(), vec!["warm"]);
    assert!(!s.toolchain_claim(ROOT, None).unwrap());
    s.toolchain_release("warmup", "warm").unwrap();
    assert_eq!(s.toolchain_roots().unwrap()[0].last_used_ms, 5);
    assert!(!s.toolchain_claim(ROOT, Some(5)).unwrap(), "used at 5");
    assert!(s.toolchain_claim(ROOT, Some(6)).unwrap());
}

/// The roots on disk before these tables get rows once; a second call does
/// nothing (a root removed since does not come back).
#[test]
fn the_roots_found_at_the_first_start_get_rows_once() {
    let (_dir, s) = store();
    assert!(!s.toolchain_roots_seeded().unwrap());
    assert!(s.seed_toolchain_roots(vec![ROOT.into()], 7).unwrap());
    assert!(s.toolchain_roots_seeded().unwrap());
    assert!(s.toolchain_claim(ROOT, None).unwrap());
    s.toolchain_forget(ROOT).unwrap();
    assert!(!s.seed_toolchain_roots(vec![ROOT.into()], 8).unwrap());
    assert!(s.toolchain_roots().unwrap().is_empty());
}
