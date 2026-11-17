//! Parent result retention: one bounded run page per tick.
//! A page that fails keeps its data and is tried again later; it never stops
//! the resident. Losing the fence or the owner does, and the tick loop sees that.
use super::*;
use crate::results::retention::{owned_blocking, page, Cursor, PageReport};

/// The line logged for a failed page: a count only. The error can name a
/// private path or a locator, so it is never printed.
fn held(failures: &mut u64) -> String {
    *failures = failures.saturating_add(1);
    format!("result retention page held (validation/I/O); failures={failures}")
}

pub(super) async fn run(
    coordinator: Arc<Coordinator>,
    files: Arc<ResultStore>,
    retention_days: u32,
    cadence: Duration,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(cadence);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = Cursor::default();
    let mut failures = 0u64;
    loop {
        tokio::select! { _ = stop.changed() => return Ok(()), _ = timer.tick() => {} }
        if *stop.borrow() || coordinator.is_fenced() {
            return Ok(());
        }
        let files = files.clone();
        // Keep owner + exclusive pin alive inside the blocking job and
        // await its drain before token/process-owner cleanup on shutdown.
        let (next, result) = owned_blocking(coordinator.clone(), move |owner| {
            if owner.is_fenced() {
                return (cursor, Ok(PageReport::default()));
            }
            let result = page(owner.store(), &files, &mut cursor, retention_days);
            (cursor, result)
        })
        .await?;
        cursor = next;
        if result.is_err() {
            // GC is optional. A claim that cannot be finished (its file cannot
            // be unlinked, an ancestor was replaced, a locator is not ours)
            // stays pending with its data. The cursor already moved past that
            // run, so the other runs are served before it is tried again.
            eprintln!("{}", held(&mut failures));
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::state::{Outcome, Termination};
    use crate::storage::NewAttempt;
    use crate::wire::LogStream;

    // The clock is quarantined for one full retention on the monotonic clock,
    // so only a zero retention lets a real loop reach deletion in a test.
    const RETENTION_DAYS: u32 = 0;

    struct Fixture {
        root: tempfile::TempDir,
        owner: Arc<Coordinator>,
        files: Arc<ResultStore>,
    }
    impl Fixture {
        fn new() -> Self {
            let root = crate::test_tmp::tempdir().unwrap();
            let state = root.path().join("parent");
            let config = crate::config::resolve(&crate::config::Inputs {
                overlay: &crate::config::Overlay::default(),
                project: None,
                legacy: None,
                user: None,
            })
            .unwrap();
            let store = CoordinatorStore::open(&state, 1).unwrap();
            let owner = Arc::new(
                config
                    .runtime_settings()
                    .unwrap()
                    .start_coordinator(store, Arc::new(crate::scheduler::UnknownCosts))
                    .unwrap(),
            );
            let files = Arc::new(ResultStore::open(&state.join("results")).unwrap());
            Self { root, owner, files }
        }
        fn expired(&self) -> (RunId, AttemptId) {
            expired(self.owner.store(), &self.files)
        }
        fn directory(&self, attempt: &AttemptId) -> PathBuf {
            self.root
                .path()
                .join("parent/results")
                .join(attempt.as_str())
        }
        fn pending(&self, run: &RunId) -> usize {
            let db = self.owner.store();
            let guard = db.result_gc_guard(run).unwrap();
            db.pending_result_page(&guard, run, 32).unwrap().len()
        }
    }
    /// A run whose only attempt ended long ago with a stored log: its result
    /// is past any retention as soon as the clock is trusted.
    pub(in crate::service::resident) fn expired(
        db: &CoordinatorStore,
        files: &ResultStore,
    ) -> (RunId, AttemptId) {
        let (run, task, attempt) = (RunId::generate(), TaskId::generate(), AttemptId::generate());
        db.create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        db.create_task(&task, &run, &serde_json::json!({}), 1)
            .unwrap();
        db.create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task,
                worker_id: WorkerId::parse("test").unwrap(),
                payload_hash: Digest::of_bytes(b"payload"),
                permit_id: None,
            },
            2,
        )
        .unwrap();
        for state in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
        ] {
            db.transition_attempt(&attempt, state, 3).unwrap();
        }
        db.prepare_log_progress(&attempt, LogStream::Stdout, 3)
            .unwrap();
        files
            .append_log(&attempt, LogStream::Stdout, 0, b"retained")
            .unwrap();
        assert!(db
            .commit_log_progress(&attempt, LogStream::Stdout, 0, 8, true, false, 0, 4)
            .unwrap());
        db.transition_attempt(&attempt, AttemptState::Finalizing, 5)
            .unwrap();
        db.finalize_attempt(
            &attempt,
            Termination::new(Outcome::Succeeded, None, None).unwrap(),
            6,
        )
        .unwrap();
        db.close_collection(
            &attempt,
            None,
            crate::results::plan::Verdict::GaveUp,
            false,
            7,
        )
        .unwrap();
        (run, attempt)
    }
    /// `expired`, with its deletion already claimed durably (a page of an
    /// earlier life): replayed by any later page, whatever its clock says.
    pub(in crate::service::resident) fn claimed(
        db: &CoordinatorStore,
        files: &ResultStore,
    ) -> (RunId, AttemptId) {
        let (run, attempt) = expired(db, files);
        let guard = db.result_gc_guard(&run).unwrap();
        let page = |wall, mono| {
            db.claim_result_page(&guard, &run, "", 32, wall, mono, Some("earlier".into()), 0)
                .unwrap()
        };
        page(100, 10);
        assert_eq!(page(110, 20).removed, 1);
        assert_eq!(db.pending_result_page(&guard, &run, 32).unwrap().len(), 1);
        (run, attempt)
    }
    async fn until(what: &str, done: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(20), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out: {what}"));
    }

    #[test]
    fn a_failed_page_is_counted_without_its_cause() {
        let mut failures = 0;
        assert_eq!(
            held(&mut failures),
            "result retention page held (validation/I/O); failures=1"
        );
        assert_eq!(
            held(&mut failures),
            "result retention page held (validation/I/O); failures=2"
        );
        failures = u64::MAX;
        assert!(held(&mut failures).ends_with(&format!("failures={}", u64::MAX)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claim_that_cannot_be_deleted_neither_stops_the_loop_nor_the_other_runs() {
        let f = Fixture::new();
        let (stuck_run, stuck) = f.expired();
        // The attempt directory was replaced by a link after its result was
        // stored. Deletion walks without following links, so every page of
        // this run fails, and what the link points at is never touched.
        let outside = f.root.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("kept"), b"outside").unwrap();
        let moved = f.root.path().join("moved");
        std::fs::rename(f.directory(&stuck), &moved).unwrap();
        std::os::unix::fs::symlink(&outside, f.directory(&stuck)).unwrap();
        let others: Vec<_> = (0..3).map(|_| f.expired()).collect();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run(
            f.owner.clone(),
            f.files.clone(),
            RETENTION_DAYS,
            Duration::from_millis(10),
            rx,
        ));
        // Runs on both sides of the stuck one, in cursor order, are collected.
        until("the other runs' results are deleted", || {
            others.iter().all(|(_, a)| !f.directory(a).exists())
        })
        .await;
        // A run that expires after the failures began is collected as well.
        let (_, late) = f.expired();
        until("a later run's result is deleted", || {
            !f.directory(&late).exists()
        })
        .await;
        assert!(!task.is_finished(), "a failed page stopped the loop");
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        // The stuck claim and every byte behind it are still there.
        assert_eq!(f.pending(&stuck_run), 1);
        for (run, _) in &others {
            assert_eq!(f.pending(run), 0);
        }
        assert!(std::fs::symlink_metadata(f.directory(&stuck))
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(outside.join("kept")).unwrap(), b"outside");
        assert_eq!(
            std::fs::read(moved.join("logs/stdout.log")).unwrap(),
            b"retained"
        );
        // A restarted loop starts from a fresh cursor, meets the stuck run
        // again and still does not stop.
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run(
            f.owner.clone(),
            f.files.clone(),
            RETENTION_DAYS,
            Duration::from_millis(10),
            rx,
        ));
        let (_, after_restart) = f.expired();
        until("a run is collected after the restart", || {
            !f.directory(&after_restart).exists()
        })
        .await;
        assert!(
            !task.is_finished(),
            "a failed page stopped the restarted loop"
        );
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(f.pending(&stuck_run), 1);
        // Once the directory is back, the pending claim is replayed and
        // finished. (Repaired between two lives: a page that met neither the
        // link nor the directory would take the claim's target as gone.)
        std::fs::remove_file(f.directory(&stuck)).unwrap();
        std::fs::rename(&moved, f.directory(&stuck)).unwrap();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run(
            f.owner.clone(),
            f.files.clone(),
            RETENTION_DAYS,
            Duration::from_millis(10),
            rx,
        ));
        until("the repaired claim is finished", || {
            !f.directory(&stuck).exists()
        })
        .await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(f.pending(&stuck_run), 0);
        assert_eq!(std::fs::read(outside.join("kept")).unwrap(), b"outside");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_resident_ends_the_loop_without_a_page() {
        let f = Fixture::new();
        let (_, attempt) = f.expired();
        let (stop, rx) = watch::channel(true);
        run(
            f.owner.clone(),
            f.files.clone(),
            RETENTION_DAYS,
            Duration::from_millis(10),
            rx,
        )
        .await
        .unwrap();
        drop(stop);
        assert!(f.directory(&attempt).exists());
    }
}
