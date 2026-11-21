//! Parent input retention: one bounded registration OR deletion page per tick.
//! All filesystem work and cursors stay with the process owner until actual drain.
use super::*;
use crate::source::retention::{PreviewCursor, RetentionCursor, VaultCursor};

#[derive(Default)]
struct Driver {
    phase: Phase,
    registered: bool,
    registration: PreviewCursor,
    source: RetentionCursor,
    vault: VaultCursor,
}

#[derive(Default, Clone, Copy)]
enum Phase {
    #[default]
    Registration,
    Vault,
    Source,
}

#[derive(Default)]
struct Report {
    paths: u32,
    unknown: u32,
    /// Leftover Vault publication temporaries the sweep kept (not failures).
    tmp_retained: u32,
}

/// What the loop counted so far, and the lines it logs: counts only, never a
/// path or a raw error (both can hold private paths or manifest bytes).
#[derive(Default)]
struct Tally {
    /// Retention targets and pages held by validation or I/O.
    failures: u64,
    /// Leftover temporaries seen by the latest Vault page that had any. They
    /// stay until someone removes them, so they are counted apart from
    /// `failures` and logged once, not on every page.
    leftovers: u32,
    leftovers_logged: bool,
}
impl Tally {
    fn page(&mut self, report: &Report) -> Vec<String> {
        let mut lines = Vec::new();
        if report.unknown > 0 {
            self.failures = self.failures.saturating_add(u64::from(report.unknown));
            lines.push(format!(
                "input retention targets retained; failures={}",
                self.failures
            ));
        }
        if report.tmp_retained > 0 {
            self.leftovers = report.tmp_retained;
            if !self.leftovers_logged {
                self.leftovers_logged = true;
                lines.push(format!(
                    "Vault publication temporaries left in place (not provably ours, or not removable); leftovers={}",
                    self.leftovers
                ));
            }
        }
        lines
    }
    /// A page that failed for a reason other than a busy lock.
    fn held(&mut self) -> String {
        self.failures = self.failures.saturating_add(1);
        format!(
            "input retention page held (validation/I/O); failures={}",
            self.failures
        )
    }
}

struct Sample {
    wall: u64,
    mono: u64,
    boot: Option<String>,
}
impl Sample {
    fn platform() -> Self {
        Self {
            wall: crate::platform::clock::wall_ms(),
            mono: crate::platform::clock::monotonic_ms(),
            boot: crate::platform::boot_id(),
        }
    }
}

impl Driver {
    // The deterministic sample seam is local to this helper, never configuration
    // or an environment clock override. Source and Vault persist separate clocks.
    fn page(
        &mut self,
        owner: &CoordinatorStore,
        captured: &CapturedInputs,
        retention_days: u32,
        sample: Sample,
    ) -> anyhow::Result<Report> {
        let mut report = Report::default();
        let phase = self.phase;
        self.phase = match phase {
            Phase::Registration => Phase::Vault,
            Phase::Vault => Phase::Source,
            Phase::Source if !self.registered => Phase::Registration,
            Phase::Source => Phase::Vault,
        };
        match phase {
            Phase::Registration => {
                // Legacy discovery/register_step is independently bounded. It
                // never authorizes deletion; new claims require done in both APIs.
                let page = captured
                    .sources
                    .retention_registration_page(owner, &mut self.registration)?;
                self.registered = page.registration_done;
                report.unknown = u32::from(page.manifest_limit.is_some());
            }
            Phase::Vault => {
                let page = captured.sources.vault_retention_page(
                    owner,
                    &captured.vault,
                    &mut self.vault,
                    sample.wall,
                    sample.mono,
                    sample.boot,
                )?;
                report.paths = page.paths;
                report.unknown = page.unknown;
                report.tmp_retained = page.tmp_retained;
            }
            Phase::Source => {
                let page = captured.sources.retention_page(
                    owner,
                    &mut self.source,
                    sample.wall,
                    sample.mono,
                    sample.boot,
                    crate::results::plan::retention_ms(retention_days),
                )?;
                report.paths = page.paths;
                report.unknown = page.unknown;
            }
        }
        Ok(report)
    }
}

pub(super) async fn run(
    coordinator: Arc<Coordinator>,
    captured: Arc<CapturedInputs>,
    retention_days: u32,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    #[cfg(not(test))]
    let cadence = Duration::from_secs(60);
    #[cfg(test)]
    let cadence = captured.retention_test.cadence();
    let mut timer = tokio::time::interval(cadence);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut driver = Driver::default();
    let mut tally = Tally::default();
    loop {
        tokio::select! { _ = stop.changed() => return Ok(()), _ = timer.tick() => {} }
        if *stop.borrow() || coordinator.is_fenced() {
            return Ok(());
        }
        let inputs = captured.clone();
        // No select/cancellation of this wait: resident shutdown joins this loop
        // before owner cleanup. Even an externally aborted waiter leaves owner,
        // CapturedInputs and all synchronous guards in the blocking closure.
        let (next, result) =
            crate::results::retention::owned_blocking(coordinator.clone(), move |owner| {
                if owner.is_fenced() {
                    return (driver, Ok(Report::default()));
                }
                let result =
                    driver.page(owner.store(), &inputs, retention_days, Sample::platform());
                (driver, result)
            })
            .await?;
        driver = next;
        if let Ok(report) = &result {
            debug_assert!(report.paths <= 32);
            for line in tally.page(report) {
                eprintln!("{line}");
            }
        }
        if let Err(error) = result {
            let busy = error.chain().any(|cause| {
                cause
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::WouldBlock)
            });
            if !busy {
                // GC is optional. Any uncertain identity/index/proof/I/O error
                // retains data and retries on a later bounded tick. Never print
                // raw errors that can contain private paths or manifest bytes.
                eprintln!("{}", tally.held());
            }
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct TestControl {
    cadence_ms: std::sync::atomic::AtomicU64,
}
#[cfg(test)]
impl TestControl {
    pub(super) fn cadence(&self) -> Duration {
        let ms = self.cadence_ms.load(std::sync::atomic::Ordering::SeqCst);
        if ms == 0 {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(ms)
        }
    }
    pub(super) fn fast(&self) {
        self.cadence_ms
            .store(10, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{CaptureParams, SourceRequest};
    use crate::state::{AttemptState, Outcome, Termination};
    use crate::storage::NewAttempt;
    use std::collections::BTreeMap;

    // `run` is also the usual name of a run identifier in these tests.
    use super::run as run_loop;

    struct Fixture {
        root: tempfile::TempDir,
        input: PathBuf,
        owner: Arc<Coordinator>,
        captured: Arc<CapturedInputs>,
    }
    impl Fixture {
        fn new() -> Self {
            let root = crate::test_tmp::tempdir().unwrap();
            let input = root.path().join("original");
            std::fs::create_dir(&input).unwrap();
            std::fs::write(input.join("file"), b"original").unwrap();
            std::fs::write(input.join(".env"), b"TOKEN=private-fixture\n").unwrap();
            let state = root.path().join("parent");
            let config = crate::config::resolve(&crate::config::Inputs {
                overlay: &crate::config::Overlay::default(),
                project: None,
                legacy: None,
                user: None,
            })
            .unwrap();
            assert_eq!(config.retention_days.value, 7);
            let store = CoordinatorStore::open(&state, 1).unwrap();
            let owner = Arc::new(
                config
                    .runtime_settings()
                    .unwrap()
                    .start_coordinator(store, Arc::new(crate::scheduler::UnknownCosts))
                    .unwrap(),
            );
            let captured = Arc::new(CapturedInputs {
                retention_test: TestControl::default(),
                transfer: Arc::new(Mutex::new(())),
                sources: crate::source::SourceStore::open(&state).unwrap(),
                vault: crate::secrets::vault::Vault::open(&state).unwrap(),
                key_path: {
                    let path = root.path().join("key");
                    crate::secrets::key::init(&path).unwrap();
                    path
                },
                env: BTreeMap::new(),
                parent_state: state,
                local_worker: WorkerId::parse("test").unwrap(),
            });
            Self {
                root,
                input,
                owner,
                captured,
            }
        }
        fn bind(&self, secret: bool) -> RunId {
            let run = RunId::generate();
            let declared = if secret {
                vec![local_ci::source::SecretSpec {
                    glob: ".env".into(),
                    required: true,
                }]
            } else {
                vec![]
            };
            crate::secrets::capture::capture_run(
                &BTreeMap::new(),
                &self.captured.sources,
                &self.captured.vault,
                &crate::secrets::capture::SecretInputs {
                    key: &self.captured.current_key().unwrap(),
                    declared: &declared,
                },
                &run,
                &SourceRequest::Directory {
                    dir: self.input.clone(),
                },
                &CaptureParams::default(),
            )
            .unwrap();
            run
        }
        fn terminal(&self, run: &RunId, closed: bool) {
            self.terminal_tasks(run, closed, 1);
        }
        fn terminal_tasks(&self, run: &RunId, closed: bool, count: usize) {
            self.terminal_tasks_at(run, closed, count, 0);
        }
        /// Every recorded time of the run is `at` plus a few milliseconds.
        fn terminal_tasks_at(&self, run: &RunId, closed: bool, count: usize, at: u64) {
            let db = self.owner.store();
            let source = self.captured.sources.source_of_run(run).unwrap().unwrap();
            let manifest = self.captured.sources.manifest(&source).unwrap().unwrap();
            let id = crate::source::classify::source_id_of_ci(&source).unwrap();
            let digest = Digest::parse(&manifest.manifest_digest)
                .or_else(|_| Digest::parse(&format!("sha256:{}", manifest.manifest_digest)))
                .unwrap();
            db.put_source_ref(&id, &digest, at + 1).unwrap();
            db.create_run(run, Some(&id), None, &serde_json::json!({}), at + 1)
                .unwrap();
            for _ in 0..count {
                let task = TaskId::generate();
                let attempt = AttemptId::generate();
                db.create_task(&task, run, &serde_json::json!({}), at + 1)
                    .unwrap();
                db.create_attempt(
                    NewAttempt {
                        attempt_id: attempt.clone(),
                        task_id: task,
                        worker_id: WorkerId::parse("test").unwrap(),
                        payload_hash: Digest::of_bytes(b"payload"),
                        permit_id: None,
                    },
                    at + 2,
                )
                .unwrap();
                for state in [
                    AttemptState::Preparing,
                    AttemptState::Ready,
                    AttemptState::Starting,
                    AttemptState::Active,
                    AttemptState::Finalizing,
                ] {
                    db.transition_attempt(&attempt, state, at + 3).unwrap();
                }
                db.finalize_attempt(
                    &attempt,
                    Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(9)), None)
                        .unwrap(),
                    at + 4,
                )
                .unwrap();
                if closed {
                    db.close_collection(
                        &attempt,
                        None,
                        crate::results::plan::Verdict::GaveUp,
                        false,
                        at + 5,
                    )
                    .unwrap();
                }
            }
        }

        fn manifest_path(&self, run: &RunId) -> PathBuf {
            let id = self.captured.sources.source_of_run(run).unwrap().unwrap();
            self.captured
                .sources
                .root()
                .join("manifests")
                .join(format!("{id}.json"))
        }
        fn ticks(&self, driver: &mut Driver, n: u64, start: u64) -> u32 {
            let mut max_paths = 0;
            for i in 0..n {
                let report = driver
                    .page(
                        self.owner.store(),
                        &self.captured,
                        7,
                        Sample {
                            wall: 100 + start + i,
                            mono: 10 + start + i,
                            boot: Some("fixture-boot".into()),
                        },
                    )
                    .unwrap();
                assert!(report.paths <= 32);
                max_paths = max_paths.max(report.paths);
            }
            max_paths
        }
    }

    #[test]
    fn leftover_temporaries_are_counted_apart_from_failures_and_logged_once() {
        let mut tally = Tally::default();
        assert!(tally.page(&Report::default()).is_empty());
        let leftover = Report {
            tmp_retained: 2,
            ..Default::default()
        };
        assert_eq!(
            tally.page(&leftover),
            ["Vault publication temporaries left in place (not provably ours, or not removable); leftovers=2"]
        );
        // The same leftovers on every later page add no line and no failure.
        for _ in 0..3 {
            assert!(tally.page(&leftover).is_empty());
        }
        assert_eq!((tally.failures, tally.leftovers), (0, 2));
        let both = Report {
            paths: 0,
            unknown: 3,
            tmp_retained: 1,
        };
        assert_eq!(
            tally.page(&both),
            ["input retention targets retained; failures=3"]
        );
        assert_eq!((tally.failures, tally.leftovers), (3, 1));
        // A page without leftovers keeps the last count that had any.
        assert!(tally.page(&Report::default()).is_empty());
        assert_eq!(tally.leftovers, 1);
        assert_eq!(
            tally.held(),
            "input retention page held (validation/I/O); failures=4"
        );
        tally.failures = u64::MAX;
        tally.held();
        tally.page(&both);
        assert_eq!(tally.failures, u64::MAX);
    }
    #[test]
    fn vault_page_reports_a_leftover_temporary_as_a_leftover_not_a_failure() {
        let f = Fixture::new();
        let run = f.bind(true);
        // Named like a publication temporary, but a link: never ours to remove.
        let leftover = f
            .captured
            .parent_state
            .join("secrets")
            .join(format!(".{run}.json.{}.tmp", ulid::Ulid::new()));
        std::os::unix::fs::symlink(f.input.join(".env"), &leftover).unwrap();
        let mut driver = Driver::default();
        let mut seen = 0;
        for i in 0..6 {
            let report = driver
                .page(
                    f.owner.store(),
                    &f.captured,
                    7,
                    Sample {
                        wall: 100 + i,
                        mono: 10 + i,
                        boot: Some("fixture-boot".into()),
                    },
                )
                .unwrap();
            assert_eq!(report.unknown, 0);
            assert!(report.tmp_retained <= 1);
            seen += report.tmp_retained;
        }
        // Every Vault page of the six ticks saw it; none removed it.
        assert!(seen >= 2, "{seen}");
        assert!(std::fs::symlink_metadata(&leftover).is_ok());
        assert_eq!(
            std::fs::read(f.input.join(".env")).unwrap(),
            b"TOKEN=private-fixture\n"
        );
    }
    #[test]
    fn default_source_seven_days_and_vault_zero_have_independent_clocks_and_exact_scope() {
        let f = Fixture::new();
        for i in 0u32..65 {
            std::fs::write(f.input.join(format!("file-{i}")), i.to_le_bytes()).unwrap();
        }
        let run = f.bind(true);
        f.terminal(&run, true);
        let manifest = f.manifest_path(&run);
        let vault = f
            .captured
            .parent_state
            .join("secrets")
            .join(format!("{run}.json"));
        let extra = f.captured.sources.root().join("unknown-extra");
        std::fs::write(&extra, b"extra").unwrap();
        let key_sentinel = f.root.path().join("key-sentinel");
        std::fs::write(&key_sentinel, b"key").unwrap();
        let cache = f.captured.sources.root().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::write(cache.join("sentinel"), b"cache").unwrap();
        let mut driver = Driver::default();
        f.ticks(&mut driver, 12, 0);
        assert!(
            !vault.exists(),
            "zero-delay Vault never made automatic progress"
        );
        assert!(manifest.exists(), "source inherited Vault's zero retention");
        assert_eq!(f.ticks(&mut driver, 12, 604_800_100), 32);
        assert!(
            !manifest.exists(),
            "seven-day source page did not remove eligible copy"
        );
        assert_eq!(
            std::fs::read(f.input.join(".env")).unwrap(),
            b"TOKEN=private-fixture\n"
        );
        assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
        for (path, expected) in [
            (extra, b"extra".as_slice()),
            (key_sentinel, b"key".as_slice()),
            (cache.join("sentinel"), b"cache".as_slice()),
        ] {
            assert_eq!(std::fs::read(path).unwrap(), expected);
        }
    }

    #[test]
    fn retention_deletes_bundles_captured_before_key_switch() {
        let f = Fixture::new();
        let run = f.bind(true);
        f.terminal(&run, true);
        let source = f.captured.sources.source_of_run(&run).unwrap().unwrap();
        let entries = f
            .captured
            .sources
            .manifest(&source)
            .unwrap()
            .unwrap()
            .secret_files;
        std::fs::remove_file(&f.captured.key_path).unwrap();
        crate::secrets::key::init(&f.captured.key_path).unwrap();
        assert!(f
            .captured
            .vault
            .load(&run, &entries, &f.captured.current_key().unwrap())
            .is_err());
        let mut driver = Driver::default();
        for i in 0..12 {
            let report = driver
                .page(
                    f.owner.store(),
                    &f.captured,
                    7,
                    Sample {
                        wall: 10_000 + i,
                        mono: 10_000 + i,
                        boot: Some("boot".into()),
                    },
                )
                .unwrap();
            assert_eq!(report.unknown, 0);
        }
        assert!(!f
            .captured
            .parent_state
            .join("secrets")
            .join(format!("{run}.json"))
            .exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn automatic_loop_registers_and_erases_only_closed_vault_copies() {
        let f = Fixture::new();
        let eligible = f.bind(true);
        f.terminal_tasks(&eligible, true, 128);
        let incomplete = f.bind(true);
        f.terminal(&incomplete, false);
        let unsubmitted = f.bind(true);
        let eligible_path = f
            .captured
            .parent_state
            .join("secrets")
            .join(format!("{eligible}.json"));
        let entries = f
            .captured
            .sources
            .manifest(
                &f.captured
                    .sources
                    .source_of_run(&eligible)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
            .unwrap()
            .secret_files;
        let held = f
            .captured
            .vault
            .load(&eligible, &entries, &f.captured.current_key().unwrap())
            .unwrap();
        f.captured.retention_test.fast();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run(f.owner.clone(), f.captured.clone(), 7, rx));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            eligible_path.exists(),
            "automatic GC bypassed actual Vault reader lease"
        );
        drop(held);
        let mut max_status = Duration::ZERO;
        let cleaned = tokio::time::timeout(Duration::from_secs(10), async {
            while eligible_path.exists() {
                let started = std::time::Instant::now();
                let owner = f.owner.clone();
                let id = eligible.clone();
                tokio::task::spawn_blocking(move || owner.store().query_run(id))
                    .await
                    .unwrap()
                    .unwrap();
                f.owner.tick().await.unwrap();
                max_status = max_status.max(started.elapsed());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        cleaned.unwrap();
        eprintln!("128-task automatic retention fixture max status+tick latency: {max_status:?}");
        assert!(max_status < Duration::from_secs(2));
        assert!(f.manifest_path(&eligible).exists());
        assert!(f.captured.vault.has(&incomplete));
        assert!(f.captured.vault.has(&unsubmitted));
        assert_eq!(
            std::fs::read(f.input.join(".env")).unwrap(),
            b"TOKEN=private-fixture\n"
        );
    }
    #[test]
    fn wall_jump_unknown_boot_and_reboot_never_shorten_automatic_source_retention() {
        const WEEK: u64 = 7 * 24 * 60 * 60 * 1000;
        let f = Fixture::new();
        let run = f.bind(false);
        // The run ends just after the first quarantine is over.
        f.terminal_tasks_at(&run, true, 1, 1_000 + WEEK);
        let manifest = f.manifest_path(&run);
        let mut driver = Driver::default();
        // Twelve consecutive pages reach every phase several times.
        let mut pages = |wall: u64, mono: u64, boot: Option<&str>| {
            for i in 0..12 {
                let report = driver
                    .page(
                        f.owner.store(),
                        &f.captured,
                        7,
                        Sample {
                            wall: wall + i,
                            mono: mono + i,
                            boot: boot.map(str::to_owned),
                        },
                    )
                    .unwrap();
                assert!(report.paths <= 32);
            }
        };
        pages(1_000, 10, Some("first-boot"));
        pages(1_000 + WEEK + 100, 10 + WEEK + 100, Some("first-boot"));
        assert!(
            manifest.exists(),
            "retention did not start at the run's end"
        );
        // The quarantine is over; the wall clock alone moves past the retention.
        pages(1_000 + 3 * WEEK, 10 + WEEK + 200, Some("first-boot"));
        assert!(manifest.exists(), "a wall jump authorized deletion");
        // No boot identity: the monotonic clock proves nothing.
        pages(1_000 + 5 * WEEK, 10 + 3 * WEEK, None);
        assert!(manifest.exists(), "an unknown boot authorized deletion");
        // A reboot starts the quarantine again, whatever the wall clock says.
        pages(1_000 + 6 * WEEK, 50, Some("second-boot"));
        pages(
            1_000 + 7 * WEEK - 1_000,
            50 + WEEK - 1_000,
            Some("second-boot"),
        );
        assert!(manifest.exists(), "a reboot shortened the quarantine");
        pages(
            1_000 + 7 * WEEK + 1_000,
            50 + WEEK + 1_000,
            Some("second-boot"),
        );
        assert!(
            !manifest.exists(),
            "a consistent clock never resumed deletion"
        );
        assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn automatic_loop_recovers_a_crashed_vault_publication_while_deletion_is_held() {
        let f = Fixture::new();
        let run = f.bind(true);
        f.terminal(&run, true);
        // Unknown legacy input holds every new claim of both stores.
        let unknown = f.captured.sources.root().join("manifests/unknown-entry");
        std::fs::write(&unknown, b"unknown").unwrap();
        // The parent died between linking the bundle and unlinking its
        // temporary name: the bundle has two names and no reader accepts it.
        let secrets = f.captured.parent_state.join("secrets");
        let bundle = secrets.join(format!("{run}.json"));
        let orphan = secrets.join(format!(".{run}.json.{}.tmp", ulid::Ulid::new()));
        std::fs::hard_link(&bundle, &orphan).unwrap();
        assert!(!f.captured.vault.has(&run));
        f.captured.retention_test.fast();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run_loop(f.owner.clone(), f.captured.clone(), 7, rx));
        let recovered = tokio::time::timeout(Duration::from_secs(10), async {
            while orphan.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        recovered.unwrap();
        // Readable again, and still retained: the hold stays in force.
        assert!(f.captured.vault.has(&run));
        assert!(bundle.exists());
        assert!(f.manifest_path(&run).exists());
        assert_eq!(std::fs::read(unknown).unwrap(), b"unknown");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn automatic_unknown_legacy_is_retained_without_stopping_runtime() {
        let f = Fixture::new();
        let eligible = f.bind(true);
        f.terminal(&eligible, true);
        let unknown = f.captured.sources.root().join("manifests/unknown-entry");
        std::fs::write(&unknown, b"unknown").unwrap();
        f.captured.retention_test.fast();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(run(f.owner.clone(), f.captured.clone(), 7, rx));
        tokio::time::sleep(Duration::from_millis(100)).await;
        let alive = !task.is_finished();
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert!(alive);
        assert!(f.captured.vault.has(&eligible));
        assert!(f.manifest_path(&eligible).exists());
        assert_eq!(std::fs::read(unknown).unwrap(), b"unknown");
    }
}
