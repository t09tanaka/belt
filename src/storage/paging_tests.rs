//! The paged reads the coordinator walks instead of loading a whole set:
//! what a walk yields when rows come and go between its pages, and that no
//! read returns more than [`PAGE_MAX`] rows however many there are.

use super::*;
use crate::ids::*;
use crate::state::{Exit, Outcome};
use serde_json::json;

struct Fx {
    _dir: tempfile::TempDir,
    store: CoordinatorStore,
    run: RunId,
}

fn fx() -> Fx {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let run = RunId::generate();
    store.create_run(&run, None, None, &json!({}), 1).unwrap();
    Fx {
        _dir: dir,
        store,
        run,
    }
}

fn worker(name: &str) -> WorkerId {
    WorkerId::parse(name).unwrap()
}

impl Fx {
    /// A live attempt of a new task on `worker`.
    fn attempt(&self, worker_name: &str, at_ms: u64) -> AttemptRecord {
        let task = TaskId::generate();
        self.store
            .create_task(&task, &self.run, &json!({}), 1)
            .unwrap();
        let payload_hash = PayloadHash::parse(&format!("sha256:{}", "a".repeat(64))).unwrap();
        self.store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: task,
                    worker_id: worker(worker_name),
                    payload_hash,
                    permit_id: None,
                },
                at_ms,
            )
            .unwrap()
    }

    fn end(&self, id: &AttemptId) {
        for s in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            self.store.transition_attempt(id, s, 20).unwrap();
        }
        let done = Termination::new(Outcome::Succeeded, Some(Exit::Code(0)), None).unwrap();
        self.store.finalize_attempt(id, done, 30).unwrap();
    }

    /// An unacknowledged placement of `attempt` on its worker.
    fn placement(&self, attempt: &AttemptRecord) -> u64 {
        self.store
            .record_placement(
                &attempt.task_id,
                &attempt.worker_id,
                Some(&attempt.attempt_id),
                &json!({"n": 1}),
                5,
            )
            .unwrap()
    }

    /// Many live attempts on `mini-a`, each with an unacknowledged placement,
    /// created at the given times: copies of one that the store made, written
    /// in one transaction (a commit per row would take minutes for thousands).
    fn bulk(&self, times: impl Iterator<Item = u64>) -> Vec<(AttemptId, u64)> {
        let template = self.attempt("mini-a", 1);
        let made_from = self.placement(&template);
        let mut c = rusqlite::Connection::open(self._dir.path().join("coordinator.db")).unwrap();
        let tx = c.transaction().unwrap();
        let mut out = Vec::new();
        for at_ms in times {
            let (task, attempt) = (TaskId::generate(), AttemptId::generate());
            let from = template.attempt_id.to_string();
            tx.execute(
                "INSERT INTO task (task_id, run_id, spec_json, created_at_ms)
                 SELECT ?1, run_id, spec_json, created_at_ms FROM task WHERE task_id = ?2",
                rusqlite::params![task.to_string(), template.task_id.to_string()],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO attempt (attempt_id, task_id, worker_id, generation, payload_hash,
                     coordinator_epoch, state, receipts_json, created_at_ms)
                 SELECT ?1, ?2, worker_id, generation, payload_hash, coordinator_epoch, state,
                     receipts_json, ?3 FROM attempt WHERE attempt_id = ?4",
                rusqlite::params![attempt.to_string(), task.to_string(), at_ms as i64, from],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO placement (task_id, worker_id, attempt_id, demand_json, created_at_ms)
                 SELECT ?1, worker_id, ?2, demand_json, created_at_ms
                 FROM placement WHERE attempt_id = ?3",
                rusqlite::params![task.to_string(), attempt.to_string(), from],
            )
            .unwrap();
            out.push((attempt, tx.last_insert_rowid() as u64));
        }
        tx.commit().unwrap();
        // The copies are what is walked; the one they were made from is not.
        self.end(&template.attempt_id);
        self.store.ack_placement(made_from, 2).unwrap();
        out
    }

    fn bulk_attempts(&self, times: impl Iterator<Item = u64>) -> Vec<AttemptId> {
        self.bulk(times).into_iter().map(|(a, _)| a).collect()
    }

    fn bulk_placements(&self, count: usize) -> Vec<u64> {
        self.bulk((0..count).map(|_| 10))
            .into_iter()
            .map(|(_, p)| p)
            .collect()
    }

    /// Walk the live attempts of `mini-a` as the coordinator does: until a
    /// read is empty. `between` runs after each read that was not.
    fn walk_live(&self, mut between: impl FnMut(usize)) -> (Vec<AttemptId>, Vec<usize>) {
        let (mut walked, mut sizes, mut after) = (Vec::new(), Vec::new(), None);
        loop {
            let page = self
                .store
                .live_attempt_page(&worker("mini-a"), after, PAGE_MAX)
                .unwrap();
            sizes.push(page.len());
            let Some((last, _)) = page.last() else {
                return (walked, sizes);
            };
            after = Some(*last);
            walked.extend(page.into_iter().map(|(_, l)| l.record.attempt_id));
            between(sizes.len());
        }
    }

    fn walk_unacked(&self, mut between: impl FnMut(usize)) -> (Vec<u64>, Vec<usize>) {
        let (mut walked, mut sizes, mut after) = (Vec::new(), Vec::new(), 0);
        loop {
            let page = self
                .store
                .unacked_demand_page(&worker("mini-a"), after, PAGE_MAX)
                .unwrap();
            sizes.push(page.len());
            let Some(last) = page.last() else {
                return (walked, sizes);
            };
            after = last.placement_id;
            walked.extend(page.iter().map(|p| p.placement_id));
            between(sizes.len());
        }
    }
}

#[test]
fn a_live_walk_ends_at_the_page_bound_and_one_past_it() {
    let f = fx();
    assert_eq!(f.walk_live(|_| ()), (vec![], vec![0]));
    let mut expected = f.bulk_attempts((0..PAGE_MAX).map(|_| 10));
    assert_eq!(f.walk_live(|_| ()), (expected.clone(), vec![64, 0]));
    expected.push(f.attempt("mini-a", 10).attempt_id);
    assert_eq!(f.walk_live(|_| ()), (expected, vec![64, 1, 0]));
}

#[test]
fn a_live_walk_neither_skips_nor_repeats_when_attempts_come_and_go_between_pages() {
    let f = fx();
    // Several attempts share a creation time, also across a page boundary.
    let initial = f.bulk_attempts((0..150u64).map(|i| 10 + i / 40));
    let mut added = Vec::new();
    let (walked, sizes) = f.walk_live(|read| match read {
        1 => {
            // Already walked: the first and the last row of the page read.
            f.end(&initial[0]);
            f.end(&initial[63]);
            // Not walked yet: the first row of the next page, one inside it,
            // and one of the page after.
            f.end(&initial[64]);
            f.end(&initial[100]);
            f.store.begin_retire(&initial[140], "moved", 40).unwrap();
            f.store.confirm_retire(&initial[140], 41).unwrap();
            // Placed meanwhile, here and elsewhere.
            added.push(f.attempt("mini-a", 50).attempt_id);
            f.attempt("mini-b", 50);
        }
        2 => {
            f.end(&initial[65]);
            f.end(&initial[149]);
            added.push(f.attempt("mini-a", 50).attempt_id);
        }
        _ => {}
    });
    // What was live for the whole walk comes exactly once, in order; what
    // ended before its page was read does not come; what was placed during
    // the walk comes once, after the rest.
    let mut expected: Vec<AttemptId> = initial
        .iter()
        .enumerate()
        .filter(|(i, _)| ![64, 100, 140, 149].contains(i))
        .map(|(_, id)| id.clone())
        .collect();
    expected.extend(added);
    assert_eq!(walked, expected);
    assert_eq!(sizes, vec![64, 64, 20, 0]);
}

#[test]
fn one_live_attempt_is_read_by_its_id_only_on_its_worker_and_while_live() {
    let f = fx();
    let a = f.attempt("mini-a", 10);
    let other = f.attempt("mini-b", 10);
    let lookup = |w: &str, id: &AttemptId| f.store.live_attempt(&worker(w), id).unwrap();
    let found = lookup("mini-a", &a.attempt_id).unwrap();
    assert_eq!(found.record, a);
    assert_eq!((found.placement, found.retire_reason), (None, None));
    assert!(lookup("mini-b", &a.attempt_id).is_none());
    assert!(lookup("mini-a", &other.attempt_id).is_none());
    assert!(lookup("mini-a", &AttemptId::generate()).is_none());

    // The same as its row in the page: placement and pending retire included.
    let placement_id = f.placement(&a);
    f.store.begin_retire(&a.attempt_id, "ack lost", 11).unwrap();
    let found = lookup("mini-a", &a.attempt_id).unwrap();
    assert_eq!(found.placement.as_ref().unwrap().placement_id, placement_id);
    assert_eq!(found.retire_reason.as_deref(), Some("ack lost"));
    let page = f
        .store
        .live_attempt_page(&worker("mini-a"), None, PAGE_MAX)
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].1, found);

    f.store.confirm_retire(&a.attempt_id, 12).unwrap();
    assert!(lookup("mini-a", &a.attempt_id).is_none());
    f.end(&other.attempt_id);
    assert!(lookup("mini-b", &other.attempt_id).is_none());
}

#[test]
fn an_unacked_walk_ends_at_the_page_bound_and_one_past_it() {
    let f = fx();
    assert_eq!(f.walk_unacked(|_| ()), (vec![], vec![0]));
    let mut expected = f.bulk_placements(PAGE_MAX as usize);
    let a = f.attempt("mini-a", 10);
    assert_eq!(f.walk_unacked(|_| ()), (expected.clone(), vec![64, 0]));
    expected.push(f.placement(&a));
    assert_eq!(f.walk_unacked(|_| ()), (expected.clone(), vec![64, 1, 0]));

    // A caller cannot ask for more than a page, and a page is whole rows.
    let capped = f
        .store
        .unacked_demand_page(&worker("mini-a"), 0, u32::MAX)
        .unwrap();
    assert_eq!(capped.len(), 64);
    assert_eq!(capped[..], f.store.unacked_demand().unwrap()[..64]);
    let two = f
        .store
        .unacked_demand_page(&worker("mini-a"), expected[2], 2)
        .unwrap();
    assert_eq!(
        two.iter().map(|p| p.placement_id).collect::<Vec<_>>(),
        expected[3..5]
    );
}

#[test]
fn an_unacked_walk_neither_skips_nor_repeats_when_placements_come_and_go_between_pages() {
    let f = fx();
    let (a, b) = (f.attempt("mini-a", 10), f.attempt("mini-b", 10));
    // Placements of another worker lie before, between and after them.
    f.placement(&b);
    let mut initial = f.bulk_placements(100);
    f.placement(&b);
    initial.extend(f.bulk_placements(50));
    f.placement(&b);
    let mut added = Vec::new();
    let (walked, sizes) = f.walk_unacked(|read| match read {
        1 => {
            for i in [0, 63, 64, 100] {
                f.store.ack_placement(initial[i], 20).unwrap();
            }
            added.push(f.placement(&a));
            f.placement(&b);
        }
        2 => {
            f.store.ack_placement(initial[65], 20).unwrap();
            f.store.ack_placement(initial[149], 20).unwrap();
            added.push(f.placement(&a));
        }
        _ => {}
    });
    let mut expected: Vec<u64> = initial
        .iter()
        .enumerate()
        .filter(|(i, _)| ![64, 100, 149].contains(i))
        .map(|(_, id)| *id)
        .collect();
    expected.extend(added);
    assert_eq!(walked, expected);
    assert_eq!(sizes, vec![64, 64, 21, 0]);
}

/// Thousands of live attempts, each with an unacknowledged placement: every
/// read stays within the page bound, and the walks still cover all of them.
#[test]
fn no_read_exceeds_the_page_bound_with_thousands_of_live_attempts() {
    const LIVE: usize = 3000;
    let f = fx();
    let (attempts, placements): (Vec<_>, Vec<_>) = f
        .bulk((0..LIVE).map(|i| 10 + (i / 500) as u64))
        .into_iter()
        .unzip();
    let bound = PAGE_MAX as usize;
    assert_eq!(bound, 64);

    let (walked, sizes) = f.walk_live(|_| ());
    assert!(sizes.iter().all(|n| *n <= bound), "{sizes:?}");
    assert_eq!(sizes.len(), LIVE / bound + 2);
    assert_eq!(walked, attempts);

    let (walked, sizes) = f.walk_unacked(|_| ());
    assert!(sizes.iter().all(|n| *n <= bound), "{sizes:?}");
    assert_eq!(sizes.len(), LIVE / bound + 2);
    assert_eq!(walked, placements);

    // Whatever is asked for, from wherever.
    for after in [None, Some((10, 0)), Some((12, 1500))] {
        let page = f
            .store
            .live_attempt_page(&worker("mini-a"), after, u32::MAX)
            .unwrap();
        assert_eq!(page.len(), bound);
    }
    for after in [0, placements[1500]] {
        let page = f
            .store
            .unacked_demand_page(&worker("mini-a"), after, u32::MAX)
            .unwrap();
        assert_eq!(page.len(), bound);
    }
}

// ------------------------------------------------------------ the indexes

/// The plan of `sql` (one `?1`..`?3` per parameter) as one string.
fn plan_of(db: &Db, sql: &str, params: usize) -> String {
    let sql = format!("EXPLAIN QUERY PLAN {sql}");
    db.call(move |c| {
        let mut q = c.prepare(&sql)?;
        let args: Vec<&dyn rusqlite::ToSql> = (0..params).map(|_| &1i64 as _).collect();
        let rows = q
            .query_map(args.as_slice(), |r| r.get::<_, String>(3))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows.join("\n"))
    })
    .unwrap()
}

const UNACKED_SQL: &str = "SELECT placement_id FROM placement WHERE worker_id = ?1 AND acked_at_ms IS NULL AND placement_id > ?2 ORDER BY placement_id LIMIT ?3";
const LIVE_SQL: &str = "SELECT attempt_id FROM attempt WHERE worker_id=?1 AND state <> 'terminal' AND retired=0 AND (created_at_ms>?2 OR (created_at_ms=?2 AND rowid>?3)) ORDER BY created_at_ms,rowid LIMIT ?4";

fn assert_indexed(store: &CoordinatorStore) {
    let unacked = plan_of(&store.db, UNACKED_SQL, 3);
    assert!(unacked.contains("placement_unacked"), "{unacked}");
    assert!(!unacked.contains("SCAN"), "{unacked}");
    let live = plan_of(&store.db, LIVE_SQL, 4);
    assert!(live.contains("attempt_worker_live"), "{live}");
    assert!(!live.contains("SCAN"), "{live}");
    assert!(!live.contains("TEMP B-TREE"), "{live}");
}

#[test]
fn the_page_queries_seek_by_partial_indexes_not_by_scanning_history() {
    assert_indexed(&fx().store);
}

#[test]
fn a_database_without_the_page_indexes_is_upgraded_to_have_them() {
    use super::policy::Access;
    let dir = crate::test_tmp::tempdir().unwrap();
    let path = dir.path().join("coordinator.db");
    let all = schema::steps(DbKind::Coordinator);
    let before = all.len() - 1;
    let old = db::open_with(
        &path,
        "coordinator",
        &all[..before],
        schema::compat(before),
        Access::ReadWrite,
    )
    .unwrap();
    let plan = plan_of(&old, UNACKED_SQL, 3);
    assert!(!plan.contains("placement_unacked"), "{plan}");
    drop(old);
    let store = CoordinatorStore::open(dir.path(), 2).unwrap();
    let version = store
        .db
        .call(|c| {
            Ok(c.query_row(
                "SELECT value FROM schema_meta WHERE key='schema_version'",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .unwrap();
    assert_eq!(version, all.len().to_string());
    assert_indexed(&store);
    // Opening again runs nothing.
    drop(store);
    assert_indexed(&CoordinatorStore::open(dir.path(), 3).unwrap());
}
