//! The acceptance tests of spec 25.5 that need the files: a whole parent
//! state (database, results, source store, Vault) and the pass of the
//! driver. The rows alone are tested in `storage::prune_tests`.

use super::*;
use crate::results::pin::RunPin;
use crate::source::{CaptureParams, SourceRequest};
use crate::state::{AttemptState, Outcome, Termination};
use crate::storage::NewAttempt;
use crate::wire::LogStream;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

/// The clock of the result collection is quarantined for one full retention
/// on the monotonic clock, so only a zero retention lets a test reach a
/// deletion by age.
const RETENTION_DAYS: u32 = 0;

struct Fixture {
    root: tempfile::TempDir,
    input: PathBuf,
    state: PathBuf,
    owner: Option<Arc<Coordinator>>,
    captured: Option<Arc<CapturedInputs>>,
    files: Option<Arc<ResultStore>>,
    gc: Cursor,
    pruning: Pruning,
    tick: u64,
    epoch: u64,
    /// The input retention loop has not finished its registration yet.
    unregistered: bool,
}

/// What one run of the fixture put on disk.
struct Made {
    run: RunId,
    attempts: Vec<AttemptId>,
    source: String,
}
impl Made {
    fn ids(&self) -> Vec<String> {
        std::iter::once(self.run.to_string())
            .chain(self.attempts.iter().map(ToString::to_string))
            .collect()
    }
}

impl Fixture {
    fn new() -> Self {
        let root = crate::test_tmp::tempdir().unwrap();
        let input = root.path().join("original");
        std::fs::create_dir(&input).unwrap();
        std::fs::write(input.join("file"), b"original").unwrap();
        std::fs::write(input.join(".env"), b"TOKEN=private-fixture\n").unwrap();
        let state = root.path().join("parent");
        crate::secrets::key::init(&root.path().join("key")).unwrap();
        let mut f = Self {
            files: None,
            root,
            input,
            state,
            owner: None,
            captured: None,
            gc: Cursor::default(),
            pruning: Pruning::default(),
            tick: 0,
            epoch: 0,
            unregistered: false,
        };
        f.start();
        f
    }
    /// A resident process: the owner of the database and its stores, with
    /// nothing in memory.
    fn start(&mut self) {
        self.epoch += 1;
        let config = crate::config::resolve(&crate::config::Inputs {
            overlay: &crate::config::Overlay::default(),
            project: None,
            legacy: None,
            user: None,
        })
        .unwrap();
        let store = CoordinatorStore::open(&self.state, self.epoch).unwrap();
        self.owner = Some(Arc::new(
            config
                .runtime_settings()
                .unwrap()
                .start_coordinator(store, Arc::new(crate::scheduler::UnknownCosts))
                .unwrap(),
        ));
        self.captured = Some(Arc::new(CapturedInputs {
            retention_test: input_retention::TestControl::default(),
            transfer: Arc::new(Mutex::new(())),
            sources: crate::source::SourceStore::open(&self.state).unwrap(),
            vault: crate::secrets::vault::Vault::open(&self.state).unwrap(),
            key_path: self.root.path().join("key"),
            env: BTreeMap::new(),
            parent_state: self.state.clone(),
            local_worker: WorkerId::parse("test").unwrap(),
        }));
        self.files = Some(Arc::new(
            ResultStore::open(&self.state.join("results")).unwrap(),
        ));
        self.gc = Cursor::default();
        self.pruning = Pruning::default();
    }
    fn files(&self) -> &Arc<ResultStore> {
        self.files.as_ref().unwrap()
    }
    /// The resident is stopped and started again: what it held in memory
    /// is gone, and the scan at start runs.
    fn restart(&mut self) -> (u32, u32) {
        set_fault(None);
        self.owner = None;
        self.captured = None;
        self.start();
        self.db().sweep_orphan_pins()
    }
    fn db(&self) -> &CoordinatorStore {
        self.owner.as_ref().unwrap().store()
    }
    fn inputs(&self) -> &CapturedInputs {
        self.captured.as_ref().unwrap()
    }
    /// Capture the input directory for a new run, with its secret bundle.
    fn bind(&self) -> RunId {
        let run = RunId::generate();
        let declared = vec![local_ci::source::SecretSpec {
            glob: ".env".into(),
            required: true,
        }];
        crate::secrets::capture::capture_run(
            &BTreeMap::new(),
            &self.inputs().sources,
            &self.inputs().vault,
            &crate::secrets::capture::SecretInputs {
                key: &self.inputs().current_key().unwrap(),
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
    /// A run with everything a run leaves in the parent state: its capture,
    /// `tasks` ended tasks with a stored log each, one attempt with only
    /// the leftover of a transfer, a bundle of its source, an in-place
    /// binding, the receipt of a cancel and an event that names only an
    /// attempt. `ended` is false for a run that still has a task to place.
    fn run(&self, tasks: usize, ended: bool) -> Made {
        let run = self.bind();
        let db = self.db();
        let sources = &self.inputs().sources;
        let source = sources.source_of_run(&run).unwrap().unwrap();
        let manifest = sources.manifest(&source).unwrap().unwrap();
        let id = crate::source::classify::source_id_of_ci(&source).unwrap();
        let digest = Digest::parse(&manifest.manifest_digest)
            .or_else(|_| Digest::parse(&format!("sha256:{}", manifest.manifest_digest)))
            .unwrap();
        db.put_source_ref(&id, &digest, 1).unwrap();
        db.create_run(&run, Some(&id), None, &serde_json::json!({}), 1)
            .unwrap();
        let mut attempts = Vec::new();
        for i in 0..tasks + 1 {
            let (task, attempt) = (TaskId::generate(), AttemptId::generate());
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
            if i < tasks {
                db.prepare_log_progress(&attempt, LogStream::Stdout, 3)
                    .unwrap();
                self.files()
                    .append_log(&attempt, LogStream::Stdout, 0, b"retained")
                    .unwrap();
                assert!(db
                    .commit_log_progress(&attempt, LogStream::Stdout, 0, 8, true, false, 0, 4)
                    .unwrap());
            } else {
                // No reference: only what an unfinished transfer left.
                let tmp = self.state.join(format!("results/{attempt}/tmp"));
                std::fs::create_dir_all(&tmp).unwrap();
                std::fs::write(tmp.join("artifact.part"), b"half").unwrap();
            }
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
            db.append_event(
                None,
                Some(&attempt),
                "worker_excluded",
                &serde_json::json!({}),
                7,
            )
            .unwrap();
            attempts.push(attempt);
        }
        if !ended {
            db.create_task(&TaskId::generate(), &run, &serde_json::json!({}), 1)
                .unwrap();
        } else {
            db.cancel_control(
                crate::service::completion::Target::Run(run.clone()),
                0,
                format!("cancel-{run}"),
                8,
            )
            .unwrap();
        }
        let bundles = sources.root().join("bundles");
        std::fs::create_dir_all(&bundles).unwrap();
        std::fs::write(
            bundles.join(format!("{source}-{}.bundle", "0".repeat(32))),
            b"bundle",
        )
        .unwrap();
        let bindings = self.state.join("in-place");
        match std::fs::DirBuilder::new().mode(0o700).create(&bindings) {
            Err(e) if e.kind() != io::ErrorKind::AlreadyExists => panic!("{e}"),
            _ => {}
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(bindings.join(format!("{run}.json")))
            .unwrap();
        Made {
            run,
            attempts,
            source,
        }
    }
    /// The registration of the source index is the work of the input
    /// retention loop; no object is deleted before it is done.
    fn register(&self) {
        let mut registration = crate::source::retention::PreviewCursor::default();
        while !self
            .inputs()
            .sources
            .retention_registration_page(self.db(), &mut registration)
            .unwrap()
            .registration_done
        {}
    }
    fn pass(&mut self) -> anyhow::Result<PruneReport> {
        self.tick += 1;
        let captured = self.captured.clone().unwrap();
        let owner = self.owner.clone().unwrap();
        if !self.unregistered {
            self.register();
        }
        let files = self.files().clone();
        pass(
            owner.store(),
            &files,
            &captured.sources,
            &captured.vault,
            &mut self.gc,
            &mut self.pruning,
            RETENTION_DAYS,
            Sample {
                wall: 1_000 + self.tick,
                mono: 100 + self.tick,
                boot: Some("fixture-boot".into()),
            },
        )
    }
    /// Passes until no marked run is left (the first only starts the clock).
    fn prune(&mut self) -> u32 {
        let mut finished = 0;
        for _ in 0..20 {
            let report = self.pass().unwrap();
            finished += report.finished;
            if !report.clock_held && self.db().marked_run_count().unwrap() == 0 {
                return finished;
            }
        }
        panic!("the pruning did not reach its end");
    }
    /// Every path below the parent state, relative to it.
    fn paths(&self) -> Vec<String> {
        fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                out.push(path.strip_prefix(base).unwrap().display().to_string());
                if path.is_dir() && !path.is_symlink() {
                    walk(&path, base, out);
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.state, &self.state, &mut out);
        out.sort();
        out
    }
    fn named(&self, ids: &[String]) -> Vec<String> {
        self.paths()
            .into_iter()
            .filter(|path| ids.iter().any(|id| path.contains(id)))
            .collect()
    }
    /// The rows of the source index that name one of `ids`.
    fn indexed(&self, ids: &[String]) -> Vec<String> {
        let c = rusqlite::Connection::open_with_flags(
            self.state.join("sources/retention.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut found = Vec::new();
        for table in [
            "root",
            "member",
            "graph",
            "run_expiry",
            "vault_delete_claim",
            "import_session",
            "capture_owner",
        ] {
            let mut s = c.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let columns = s.column_count();
            let mut rows = s.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                for i in 0..columns {
                    if let rusqlite::types::Value::Text(text) = row.get(i).unwrap() {
                        if ids.iter().any(|id| text.contains(id)) {
                            found.push(format!("{table}: {text}"));
                        }
                    }
                }
            }
        }
        found
    }
    fn regular_files(&self, below: &str) -> Vec<String> {
        self.paths()
            .into_iter()
            .filter(|path| path.starts_with(below) && self.state.join(path).is_file())
            .collect()
    }
    /// Acceptance (i): nothing of the run is left, in any table and at any
    /// path of the parent state.
    fn nothing_left(&self, made: &Made) {
        let ids = made.ids();
        assert_eq!(self.named(&ids), Vec::<String>::new());
        assert_eq!(self.db().mentions_for_test(&ids), Vec::<String>::new());
        assert_eq!(self.indexed(&ids), Vec::<String>::new());
        assert_eq!(self.db().marked_run_count().unwrap(), 0);
    }
    /// The source of the run went with it, as its last run.
    fn source_gone(&self, made: &Made) {
        let source = [made.source.clone()];
        assert_eq!(self.named(&source), Vec::<String>::new());
        assert_eq!(self.indexed(&source), Vec::<String>::new());
        assert_eq!(self.regular_files("sources/objects"), Vec::<String>::new());
        assert_eq!(
            self.regular_files("sources/manifests"),
            Vec::<String>::new()
        );
        assert_eq!(self.regular_files("sources/bundles"), Vec::<String>::new());
        let id = crate::source::classify::source_id_of_ci(&made.source).unwrap();
        assert_eq!(
            self.db().mentions_for_test(&[id.to_string()]),
            Vec::<String>::new()
        );
    }
    fn hidden(&self, run: &RunId) {
        assert!(self.db().query_run(run.clone()).unwrap().is_none());
        assert!(self
            .db()
            .query_runs(10, None)
            .unwrap()
            .items
            .iter()
            .all(|listed| &listed.run_id != run));
    }
}

/// Acceptance (i): a run that ended and whose retention passed goes through
/// the steps and leaves none of the per-run entries of spec 25.10, "親の
/// state directory": no row, no result, no input, no file named by its ID.
#[test]
fn a_run_past_its_retention_leaves_nothing_of_itself() {
    let mut f = Fixture::new();
    let made = f.run(2, true);
    // The owner row of its capture, as a submit that was killed before it
    // released the row leaves it.
    f.inputs().sources.own_capture(&made.run).unwrap();
    assert!(f
        .indexed(&made.ids())
        .iter()
        .any(|row| row.starts_with("capture_owner:")));
    // What the run has on disk before: every kind of entry the steps name.
    let before = f.named(&made.ids());
    for expected in [
        format!("reader-pins/{}.lock", made.run),
        format!("secrets/{}.json", made.run),
        format!("in-place/{}.json", made.run),
        format!("sources/runs/{}", made.run),
        format!("sources/runs/{}.lock", made.run),
        format!("results/{}/tmp/artifact.part", made.attempts[2]),
    ] {
        assert!(before.contains(&expected), "{expected} in {before:?}");
    }
    assert!(before
        .iter()
        .any(|path| path.starts_with(&format!("results/{}/", made.attempts[0]))));
    assert!(!f.regular_files("sources/objects").is_empty());
    assert!(!f.regular_files("sources/manifests").is_empty());
    assert!(!f.indexed(&made.ids()).is_empty());
    assert!(f.db().mentions_for_test(&made.ids()).len() > 10);

    // The first pass only starts the clock: nothing is judged by age.
    let first = f.pass().unwrap();
    assert!(first.clock_held);
    assert_eq!((first.marked, first.finished), (0, 0));
    assert_eq!(f.named(&made.ids()), before);
    assert_eq!(f.prune(), 1);
    f.nothing_left(&made);
    f.source_gone(&made);
    // A later pass finds nothing to do, and the result collection makes no
    // pin for the run.
    let again = f.pass().unwrap();
    assert_eq!((again.marked, again.finished, again.continuing), (0, 0, 0));
    f.nothing_left(&made);
}

/// A run that has not ended is not touched, however old it is.
#[test]
fn a_run_that_has_not_ended_keeps_everything() {
    let mut f = Fixture::new();
    let made = f.run(1, false);
    let before = (f.named(&made.ids()), f.db().mentions_for_test(&made.ids()));
    for _ in 0..3 {
        let report = f.pass().unwrap();
        assert_eq!((report.marked, report.finished), (0, 0));
    }
    assert_eq!(
        (f.named(&made.ids()), f.db().mentions_for_test(&made.ids())),
        before
    );
    assert!(f.db().query_run(made.run.clone()).unwrap().is_some());
}

/// Acceptance (ii) and (vi): the resident is stopped right after each step;
/// the run is hidden from then on, and the pass after the restart does the
/// rest and ends in the same state.
#[test]
fn a_pass_after_a_stop_at_any_step_does_the_rest() {
    for stage in [
        "after_mark",
        "after_vault",
        "after_results",
        "prune_after_roots",
        "prune_after_files",
        "after_inputs",
        "during_rows",
        "after_rows",
    ] {
        let mut f = Fixture::new();
        let made = f.run(2, true);
        // More rows than one transaction deletes.
        let run = made.run.to_string();
        f.db()
            .db
            .write(move |tx| {
                tx.execute_batch(&format!(
                    "INSERT INTO event(run_id,attempt_id,attempt_seq,kind,payload_json,at_ms)
                       WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1500)
                       SELECT '{run}',NULL,NULL,'filler','null',8 FROM n;"
                ))?;
                Ok(())
            })
            .unwrap();
        assert!(f.pass().unwrap().clock_held);
        set_fault(Some(stage));
        crate::source::retention::set_prune_fault(Some(stage));
        let stopped = f.pass();
        set_fault(None);
        crate::source::retention::set_prune_fault(None);
        match stopped {
            Ok(report) => assert_eq!(
                (report.marked, report.finished, report.continuing),
                (1, 0, 1),
                "{stage}"
            ),
            Err(_) => assert_eq!(stage, "after_mark"),
        }
        // From the mark on the run is not shown, whatever is left of it.
        f.hidden(&made.run);
        let rows_left = f.db().has_run(&made.run).unwrap();
        assert_eq!(rows_left, stage != "after_rows", "{stage}");
        if stage != "after_rows" {
            assert_eq!(f.db().marked_run_count().unwrap(), 1, "{stage}");
        }
        let (removed, _) = f.restart();
        // A stop between the last transaction and the unlink of the pin
        // leaves the pin of a run without a row: the scan at start takes it.
        assert_eq!(removed, u32::from(stage == "after_rows"), "{stage}");
        if rows_left {
            f.hidden(&made.run);
            assert_eq!(f.prune(), 1, "{stage}");
        }
        f.nothing_left(&made);
        f.source_gone(&made);
    }
}

/// Acceptance (v): while a reader holds the shared pin, step 3 does not
/// begin: the results, the inputs and the rows of the run stay. The run is
/// hidden all the same, and it goes once the reader is done.
#[test]
fn a_reader_holds_the_results_until_it_is_done() {
    let mut f = Fixture::new();
    let made = f.run(1, true);
    let reader = RunPin::shared(&f.state, &made.run).unwrap();
    let log = f.state.join(format!("results/{}", made.attempts[0]));
    f.pass().unwrap();
    for _ in 0..3 {
        let report = f.pass().unwrap();
        assert_eq!((report.finished, report.continuing), (0, 1));
    }
    f.hidden(&made.run);
    assert!(log.exists(), "the file a reader may have open stays");
    assert!(f.db().has_run(&made.run).unwrap());
    assert!(f.state.join(format!("sources/runs/{}", made.run)).exists());
    assert!(!f.regular_files("sources/objects").is_empty());
    drop(reader);
    assert_eq!(f.prune(), 1);
    f.nothing_left(&made);
}

/// Acceptance (vii): the manifest, the objects and the bundle of a source
/// that another run names stay, and that run is whole.
#[test]
fn what_another_run_shares_stays() {
    let mut f = Fixture::new();
    let made = f.run(1, true);
    let other = f.run(1, false);
    assert_eq!(made.source, other.source, "one source for both runs");
    let objects = f.regular_files("sources/objects");
    let manifests = f.regular_files("sources/manifests");
    let bundles = f.regular_files("sources/bundles");
    let kept = (
        f.named(&other.ids()),
        f.db().mentions_for_test(&other.ids()),
    );
    assert!(!objects.is_empty() && !manifests.is_empty() && bundles.len() == 1);
    assert_eq!(f.prune(), 1);
    f.nothing_left(&made);
    assert_eq!(f.regular_files("sources/objects"), objects);
    assert_eq!(f.regular_files("sources/manifests"), manifests);
    assert_eq!(f.regular_files("sources/bundles"), bundles);
    assert_eq!(
        (
            f.named(&other.ids()),
            f.db().mentions_for_test(&other.ids())
        ),
        kept
    );
    assert!(f.db().query_run(other.run.clone()).unwrap().is_some());
    let id = crate::source::classify::source_id_of_ci(&other.source).unwrap();
    assert!(!f.db().mentions_for_test(&[id.to_string()]).is_empty());
}

/// Acceptance (xiii): the result collection was inside the run when the run
/// was pruned. Its position is dropped, and no page after that makes the
/// pin of the run again.
#[test]
fn the_result_collection_does_not_come_back_to_a_pruned_run() {
    let mut f = Fixture::new();
    let made = f.run(1, true);
    f.pass().unwrap();
    f.gc = Cursor::at(&made.run);
    assert_eq!(f.prune(), 1);
    assert!(
        !f.gc.names(&made.run),
        "the position inside the pruned run is dropped"
    );
    let pin = f.state.join(format!("reader-pins/{}.lock", made.run));
    // A position that survived anyhow (set here by hand) makes no pin
    // either: the guard of a page refuses a run without a row.
    f.gc = Cursor::at(&made.run);
    for _ in 0..5 {
        let owner = f.owner.clone().unwrap();
        let files = f.files().clone();
        crate::results::retention::page(owner.store(), &files, &mut f.gc, RETENTION_DAYS).unwrap();
        assert!(!pin.exists());
    }
    f.nothing_left(&made);
}

/// One pass is bounded (spec 25.11): a run with more result paths than a
/// pass deletes is continued by the next passes, and nothing but the pass
/// decides how far a pass goes.
#[test]
fn a_pass_is_bounded_and_the_next_one_continues() {
    let mut f = Fixture::new();
    let made = f.run(40, true);
    f.pass().unwrap();
    let report = f.pass().unwrap();
    assert_eq!(
        (report.marked, report.finished, report.continuing),
        (1, 0, 1)
    );
    let left = made
        .attempts
        .iter()
        .filter(|attempt| f.state.join(format!("results/{attempt}")).exists())
        .count();
    assert_eq!(left, 41 - 32, "32 result paths go in one pass");
    assert!(
        f.db().has_run(&made.run).unwrap(),
        "the rows wait for step 3"
    );
    assert_eq!(f.prune(), 1);
    f.nothing_left(&made);
}

/// The bound of result paths is the pass's, not each run's (spec 25.5):
/// the directories of the attempts count against it with the claims, and
/// the next pass continues the sweep where this one stopped.
#[test]
fn many_runs_share_the_bound_of_result_paths_of_a_pass() {
    let mut f = Fixture::new();
    let made: Vec<Made> = (0..20).map(|_| f.run(1, true)).collect();
    let dirs = |f: &Fixture| {
        made.iter()
            .flat_map(|m| &m.attempts)
            .filter(|attempt| f.state.join(format!("results/{attempt}")).exists())
            .count()
    };
    assert_eq!(dirs(&f), 40);
    f.pass().unwrap();
    let report = f.pass().unwrap();
    assert_eq!(report.marked, 20);
    assert!(report.continuing > 0, "{report:?}");
    // Claims and directories together: at most 32 paths in the pass.
    assert!(40 - dirs(&f) <= 32, "{} directories went", 40 - dirs(&f));
    assert_eq!(f.prune(), 20 - report.finished);
    for m in &made {
        f.nothing_left(m);
    }
}

/// The bound of input paths is the pass's too: runs that each hold objects
/// of their own share 32 paths a pass, and the next passes continue.
#[test]
fn many_runs_share_the_bound_of_input_paths_of_a_pass() {
    let mut f = Fixture::new();
    let made: Vec<Made> = (0..20)
        .map(|i| {
            std::fs::write(f.input.join("file"), format!("version {i}")).unwrap();
            f.run(0, true)
        })
        .collect();
    f.register();
    let before = f.regular_files("sources/objects").len()
        + f.regular_files("sources/manifests").len();
    assert!(before > 32, "{before} input files");
    f.pass().unwrap();
    let report = f.pass().unwrap();
    assert_eq!(report.marked, 20);
    let after = f.regular_files("sources/objects").len()
        + f.regular_files("sources/manifests").len();
    assert!(before - after <= 32, "{} input files went", before - after);
    assert!(report.continuing > 0, "{report:?}");
    assert_eq!(f.prune(), 20 - report.finished);
    for m in &made {
        f.nothing_left(m);
    }
}

/// The loop of the resident prunes: with the parent's stores it calls the
/// pass after its page, and not while a version is on trial (25.1, 8).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_retention_loop_prunes_a_due_run_but_not_on_trial() {
    let f = Fixture::new();
    let made = f.run(1, true);
    f.register();
    let worker = f.root.path().join("worker");
    std::fs::create_dir(&worker).unwrap();
    let owner = f.owner.clone().unwrap();
    owner.set_trial_marker(crate::worker::control::trial_path(&worker));
    crate::worker::control::begin_trial(&worker).unwrap();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(result_retention::run(
        owner.clone(),
        f.files().clone(),
        f.captured.clone(),
        RETENTION_DAYS,
        Duration::from_millis(10),
        rx,
    ));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!owner.store().run_is_pruned(&made.run).unwrap());
    assert!(owner.store().query_run(made.run.clone()).unwrap().is_some());
    crate::worker::control::end_trial(&worker).unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while owner.store().has_run(&made.run).unwrap() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the loop did not prune the run");
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    f.nothing_left(&made);
}

/// The authority of every deletion is the mark, read again where the
/// deletion happens: for a run without it, no step deletes anything.
#[test]
fn nothing_of_a_run_without_the_mark_is_deleted_by_any_step() {
    let f = Fixture::new();
    let made = f.run(1, true);
    f.register();
    let before = (
        f.paths(),
        f.db().mentions_for_test(&made.ids()),
        f.indexed(&made.ids()),
    );
    let (db, inputs) = (f.db(), f.inputs());
    assert!(inputs
        .sources
        .prune_run_vault(db, &inputs.vault, &made.run, &mut PruneBudget::pass())
        .is_err());
    assert!(inputs
        .sources
        .prune_run_inputs(
            db,
            &made.run,
            &mut AbandonCursor::default(),
            &mut PruneBudget::pass()
        )
        .is_err());
    assert!(db.prune_claim_results(&made.run, 32, 9).is_err());
    assert!(db.prune_pending_results(&made.run, 32).is_err());
    assert!(db.prune_pin(&made.run).is_err());
    assert!(db.prune_rows(&made.run, ROWS_PER_TRANSACTION).is_err());
    assert_eq!(
        (
            f.paths(),
            f.db().mentions_for_test(&made.ids()),
            f.indexed(&made.ids()),
        ),
        before
    );
    assert!(f.db().query_run(made.run.clone()).unwrap().is_some());
}

/// The order of the steps: the rows of a run go only after its inputs. While
/// the source store cannot delete (here: its index is not registered yet),
/// the run keeps its row and its mark, and a later pass does the rest.
#[test]
fn the_rows_wait_until_the_inputs_are_gone() {
    let mut f = Fixture::new();
    let made = f.run(1, true);
    f.unregistered = true;
    f.pass().unwrap();
    for _ in 0..3 {
        let report = f.pass().unwrap();
        assert_eq!((report.finished, report.continuing), (0, 1));
    }
    assert!(f.db().has_run(&made.run).unwrap());
    assert!(!f.db().mentions_for_test(&made.ids()[1..]).is_empty());
    assert!(f.state.join(format!("sources/runs/{}", made.run)).exists());
    assert!(f
        .state
        .join(format!("reader-pins/{}.lock", made.run))
        .exists());
    // Step 3 came before: the results are gone already.
    assert!(!f
        .state
        .join(format!("results/{}", made.attempts[0]))
        .exists());
    f.unregistered = false;
    assert_eq!(f.prune(), 1);
    f.nothing_left(&made);
}

/// Spec 25.13: the source index of an earlier binary gets the index the
/// pruning deletes the import sessions of a run by, when it is opened.
#[test]
fn a_source_index_from_before_gets_the_index_of_import_sessions_by_run() {
    let mut f = Fixture::new();
    let db = f.state.join("sources/retention.db");
    let indexed = || -> bool {
        rusqlite::Connection::open(&db)
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='import_session_run')",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert!(indexed(), "a new index has it");
    f.owner = None;
    f.captured = None;
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("DROP INDEX import_session_run")
        .unwrap();
    assert!(!indexed());
    f.start();
    assert!(indexed(), "an index of an earlier binary gets it");
}
