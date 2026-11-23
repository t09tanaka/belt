//! Parent result retention: one bounded run page per tick, and then one
//! bounded pass of the pruning of runs (spec 25.5) in the same blocking job,
//! so the two never overlap (25.11).
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

/// The line logged when a page left entries pending: counts only.
fn entries_held(failures: u64, entries: u32) -> String {
    format!("result retention entries held (I/O); entries={entries}; failures={failures}")
}

/// The line logged when a pass of the pruning kept something: counts only.
/// With the gates that held runs at step 4, by name.
pub(super) fn pruning_held(report: &run_pruning::PruneReport) -> String {
    let mut line = format!(
        "run pruning held (validation/I/O); kept={}; continuing={}",
        report.unknown, report.continuing
    );
    if report.kept_events > 0 {
        line.push_str(&format!("; kept_events={}", report.kept_events));
    }
    if !report.gates.is_empty() {
        let gates: Vec<String> = report
            .gates
            .iter()
            .map(|(gate, runs)| format!("{}:{runs}", gate.as_str()))
            .collect();
        line.push_str(&format!("; gate={}", gates.join(",")));
    }
    line
}

/// `inputs` is the parent's source store and Vault: with them every tick
/// also prunes runs. Without them (a test of the result pages alone) the
/// loop only collects results.
pub(super) async fn run(
    coordinator: Arc<Coordinator>,
    files: Arc<ResultStore>,
    inputs: Option<Arc<CapturedInputs>>,
    retention_days: u32,
    cadence: Duration,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(cadence);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = Cursor::default();
    let mut pruning = run_pruning::Pruning::default();
    let mut failures = 0u64;
    loop {
        tokio::select! { _ = stop.changed() => return Ok(()), _ = timer.tick() => {} }
        if *stop.borrow() || coordinator.is_fenced() {
            return Ok(());
        }
        // Paused while a new version is on trial (§24.8): what it would
        // delete stays deleted after a rollback, while its record comes back.
        if coordinator.in_trial() {
            continue;
        }
        let files = files.clone();
        let inputs = inputs.clone();
        // Keep owner + exclusive pin alive inside the blocking job and
        // await its drain before token/process-owner cleanup on shutdown.
        let (next, result, pruned) = owned_blocking(coordinator.clone(), move |owner| {
            if owner.is_fenced() || owner.in_trial() {
                return ((cursor, pruning), Ok(PageReport::default()), None);
            }
            let result = page(owner.store(), &files, &mut cursor, retention_days);
            // The pruning follows the page in this job: a page never runs
            // while a run is pruned. A pass that fails keeps its data, like
            // a page that fails.
            let pruned = inputs.map(|inputs| {
                if owner.is_fenced() || owner.in_trial() {
                    return Ok(run_pruning::PruneReport::default());
                }
                run_pruning::pass(
                    owner.store(),
                    &files,
                    &inputs.sources,
                    &inputs.vault,
                    &mut cursor,
                    &mut pruning,
                    retention_days,
                    run_pruning::Sample::platform(),
                )
            });
            ((cursor, pruning), result, pruned)
        })
        .await?;
        (cursor, pruning) = next;
        match pruned {
            Some(Ok(report)) if report.unknown > 0 || report.kept_events > 0 || !report.gates.is_empty() => {
                eprintln!("{}", pruning_held(&report))
            }
            // Never the error itself: it can name a private path.
            Some(Err(_)) => eprintln!("{}", held(&mut failures)),
            _ => {}
        }
        if let Ok(report) = &result {
            if report.failed > 0 {
                eprintln!("{}", entries_held(failures, report.failed));
            }
        }
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
            let guard = crate::test_tmp::gc_guard(db, run);
            db.pending_result_page(&guard, run, 32).unwrap().len()
        }
    }
    /// A run whose only attempt ended long ago with a stored log: its result
    /// is past any retention as soon as the clock is trusted.
    pub(in crate::service::resident) fn expired(
        db: &CoordinatorStore,
        files: &ResultStore,
    ) -> (RunId, AttemptId) {
        let run = RunId::generate();
        db.create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        let attempt = expired_attempt(db, files, &run);
        (run, attempt)
    }
    /// One more expired attempt (its own task) in an existing run.
    fn expired_attempt(db: &CoordinatorStore, files: &ResultStore, run: &RunId) -> AttemptId {
        let (task, attempt) = (TaskId::generate(), AttemptId::generate());
        db.create_task(&task, run, &serde_json::json!({}), 1)
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
        attempt
    }
    /// `expired`, with its deletion already claimed durably (a page of an
    /// earlier life): replayed by any later page, whatever its clock says.
    pub(in crate::service::resident) fn claimed(
        db: &CoordinatorStore,
        files: &ResultStore,
    ) -> (RunId, AttemptId) {
        let (run, attempt) = expired(db, files);
        let guard = crate::test_tmp::gc_guard(db, &run);
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
    fn held_entries_are_logged_as_counts_only() {
        assert_eq!(
            entries_held(3, 2),
            "result retention entries held (I/O); entries=2; failures=3"
        );
    }

    /// Restores permissions when dropped, so a failing assertion leaves
    /// nothing the temporary directory cannot delete.
    struct Restore(PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
        }
    }

    #[test]
    fn an_unremovable_claim_does_not_hold_back_the_claims_behind_it() {
        use std::os::unix::fs::PermissionsExt;
        if rustix::process::geteuid().is_root() {
            return; // permissions do not bind root
        }
        let f = Fixture::new();
        let db = f.owner.store();
        let run = RunId::generate();
        db.create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        let attempts = [
            expired_attempt(db, &f.files, &run),
            expired_attempt(db, &f.files, &run),
        ];
        let guard = crate::test_tmp::gc_guard(db, &run);
        let claim = |wall, mono| {
            db.claim_result_page(&guard, &run, "", 32, wall, mono, Some("earlier".into()), 0)
                .unwrap()
        };
        claim(100, 10);
        assert_eq!(claim(110, 20).removed, 2);
        let pending = db.pending_result_page(&guard, &run, 32).unwrap();
        assert_eq!(pending.len(), 2);
        // The first claim in page order cannot be unlinked: its directory is
        // read-only, so its entries cannot be removed.
        let first = attempts
            .iter()
            .find(|a| pending[0].locator.contains(a.as_str()))
            .unwrap();
        let second = attempts.iter().find(|a| *a != first).unwrap();
        let stuck = f.directory(first);
        let _restore = Restore(stuck.clone());
        std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o500)).unwrap();
        drop(guard);
        let report = crate::test_tmp::settled_page(db, &f.files, RETENTION_DAYS);
        assert_eq!((report.deleted, report.failed), (1, 1));
        assert!(!f.directory(second).exists());
        assert!(stuck.exists());
        assert_eq!(f.pending(&run), 1);
        // Once it can be removed, a later page finishes it.
        std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o755)).unwrap();
        let report = crate::test_tmp::settled_page(db, &f.files, RETENTION_DAYS);
        assert_eq!((report.deleted, report.failed), (1, 0));
        assert_eq!(f.pending(&run), 0);
        assert!(!stuck.exists());
    }

    #[test]
    fn a_foreign_locator_still_fails_the_page_and_removes_nothing() {
        let f = Fixture::new();
        let db = f.owner.store();
        let (run, attempt) = claimed(db, &f.files);
        let outside = f.root.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("kept"), b"outside").unwrap();
        // A claim pointing outside the store is not ours; it cannot be forged
        // through the public API, so the check is on the predicate the page uses.
        assert!(!ResultStore::owns(outside.to_str().unwrap()));
        assert!(!ResultStore::owns("../x"));
        // The claim that is ours is removed without following anything else.
        let report = crate::test_tmp::settled_page(db, &f.files, RETENTION_DAYS);
        assert_eq!((report.deleted, report.failed), (1, 0));
        assert!(!f.directory(&attempt).exists());
        assert_eq!(f.pending(&run), 0);
        assert_eq!(std::fs::read(outside.join("kept")).unwrap(), b"outside");
    }

    #[test]
    fn a_replaced_attempt_directory_is_counted_and_never_followed() {
        let f = Fixture::new();
        let db = f.owner.store();
        let (run, attempt) = claimed(db, &f.files);
        let outside = f.root.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("kept"), b"outside").unwrap();
        std::fs::rename(f.directory(&attempt), f.root.path().join("moved")).unwrap();
        std::os::unix::fs::symlink(&outside, f.directory(&attempt)).unwrap();
        let report = crate::test_tmp::settled_page(db, &f.files, RETENTION_DAYS);
        assert_eq!((report.deleted, report.failed), (0, 1));
        assert_eq!(f.pending(&run), 1);
        assert_eq!(std::fs::read(outside.join("kept")).unwrap(), b"outside");
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
            None,
            RETENTION_DAYS,
            Duration::from_millis(10),
            rx,
        ));
        // Runs on both sides of the stuck one, in cursor order, are collected.
        until("the other runs' results are deleted", || {
            others.iter().all(|(_, a)| !f.directory(a).exists())
        })
        .await;
        // Each run is paged independently. Collection of the other runs
        // does not establish that the stuck run has been claimed yet.
        until("the stuck run has a pending deletion", || {
            f.pending(&stuck_run) == 1
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
            None,
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
            None,
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
    async fn a57_on_trial_retention_deletes_nothing_and_resumes_after_the_commit() {
        let f = Fixture::new();
        let state = f.root.path().join("worker");
        std::fs::create_dir(&state).unwrap();
        f.owner
            .set_trial_marker(crate::worker::control::trial_path(&state));
        crate::worker::control::begin_trial(&state).unwrap();
        // A durable claim: any page at all would finish it and delete this.
        let (run_id, attempt) = claimed(f.owner.store(), &f.files);
        let (_, expired) = f.expired();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run(
            f.owner.clone(),
            f.files.clone(),
            None,
            RETENTION_DAYS,
            Duration::from_millis(10),
            rx,
        ));
        // Many cadences pass while the trial lasts.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!task.is_finished(), "the trial stopped the loop");
        assert!(f.directory(&attempt).exists());
        assert!(f.directory(&expired).exists());
        assert_eq!(f.pending(&run_id), 1);
        // The commit ends the trial, and the loop goes on where it was.
        crate::worker::control::end_trial(&state).unwrap();
        until("the claim is finished after the commit", || {
            !f.directory(&attempt).exists() && !f.directory(&expired).exists()
        })
        .await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(f.pending(&run_id), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_resident_ends_the_loop_without_a_page() {
        let f = Fixture::new();
        // A durable claim: any page at all would finish it and delete this.
        let (run_id, attempt) = claimed(f.owner.store(), &f.files);
        let (stop, rx) = watch::channel(true);
        tokio::time::timeout(
            Duration::from_secs(20),
            run(
                f.owner.clone(),
                f.files.clone(),
                None,
                RETENTION_DAYS,
                Duration::from_millis(10),
                rx,
            ),
        )
        .await
        .expect("the loop went on after the stop")
        .unwrap();
        drop(stop);
        assert!(f.directory(&attempt).exists());
        assert_eq!(f.pending(&run_id), 1);
    }
}
