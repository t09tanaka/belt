//! Spec 25.4 (b) and (c): what a capture that was never submitted leaves, and
//! what the resident's abandonment deletes of it.
use super::deletion::set_fault;
use super::*;
use crate::ids::{RunId, WorkerId};
use crate::platform::owned_temp::OwnerRecord;
use crate::secrets::capture::{capture_run, SecretInputs};
use crate::secrets::vault::Vault;
use crate::source::{CaptureParams, SourceRequest};
use crate::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::path::Path;

const FILES: usize = 40;

struct Parts {
    root: tempfile::TempDir,
    state: PathBuf,
    input: PathBuf,
    owner: CoordinatorStore,
    sources: SourceStore,
    vault: Vault,
}

fn input_tree(root: &Path, files: usize, tag: &str) -> PathBuf {
    let input = root.join(format!("original-{tag}"));
    std::fs::create_dir(&input).unwrap();
    for i in 0..files {
        std::fs::write(input.join(format!("file-{i}")), format!("{tag} {i}")).unwrap();
    }
    std::fs::write(input.join(".env"), b"TOKEN=private-fixture\n").unwrap();
    input
}

impl Parts {
    fn new() -> Self {
        let root = crate::test_tmp::tempdir().unwrap();
        let state = root.path().join("parent");
        crate::secrets::key::init(&root.path().join("key")).unwrap();
        let input = input_tree(root.path(), FILES, "a");
        let owner = CoordinatorStore::open(&state, 1).unwrap();
        let sources = SourceStore::open(&state).unwrap();
        let vault = Vault::open(&state).unwrap();
        // The discovery of manifests from before the index is over: until
        // it is, no object of the store is deleted by anyone.
        let mut cursor = PreviewCursor::default();
        let mut done = false;
        for _ in 0..8 {
            done = sources
                .retention_registration_page(&owner, &mut cursor)
                .unwrap()
                .registration_done;
            if done {
                break;
            }
        }
        assert!(done);
        Self {
            root,
            state,
            input,
            owner,
            sources,
            vault,
        }
    }
    fn capture_of(&self, run: &RunId, input: &Path) -> crate::source::Captured {
        let key = crate::secrets::key::load(&self.root.path().join("key")).unwrap();
        capture_run(
            &BTreeMap::new(),
            &self.sources,
            &self.vault,
            &SecretInputs {
                key: &key,
                declared: &[local_ci::source::SecretSpec {
                    glob: ".env".into(),
                    required: true,
                }],
            },
            run,
            &SourceRequest::Directory { dir: input.into() },
            &CaptureParams::default(),
        )
        .unwrap()
    }
    /// A whole capture of `run` owned by `owner`, with its in-place binding.
    fn captured(&self, owner: &OwnerRecord) -> RunId {
        let run = RunId::generate();
        self.sources.own_capture_as(&run, owner).unwrap();
        let got = self.capture_of(&run, &self.input);
        bind_in_place(&self.state, &run, &got, &self.input);
        run
    }
    fn pass(&self) -> AbandonReport {
        self.sources
            .abandon_captures(
                &self.owner,
                &self.vault,
                AbandonScope::All,
                &mut AbandonCursor::default(),
            )
            .unwrap()
    }
    /// Passes until a pass has nothing to continue: the page bound is 32
    /// files, and a capture holds more.
    fn passes(&self) -> AbandonReport {
        for _ in 0..16 {
            let report = self.pass();
            assert!(report.paths <= 32, "{report:?}");
            if report.continuing == 0 {
                return report;
            }
        }
        panic!("the abandonment did not finish: {:?}", self.pass());
    }
    fn count(&self, sql: &str) -> i64 {
        self.sources
            .retention
            .connect()
            .unwrap()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    }
    fn index_rows(&self) -> i64 {
        self.count(
            "SELECT (SELECT count(*) FROM capture_owner)+(SELECT count(*) FROM root)
             +(SELECT count(*) FROM member)+(SELECT count(*) FROM graph)
             +(SELECT count(*) FROM import_session)+(SELECT count(*) FROM run_expiry)
             +(SELECT count(*) FROM vault_delete_claim)
             +(SELECT count(*) FROM source_delete_claim WHERE done=0)",
        )
    }
    fn stored_files(&self) -> Vec<PathBuf> {
        let mut found = Vec::new();
        let sources = self.state.join("sources");
        for (dir, deep) in [("objects", true), ("manifests", false)] {
            let Ok(entries) = std::fs::read_dir(sources.join(dir)) else {
                continue;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                if deep && path.is_dir() {
                    found.extend(std::fs::read_dir(&path).unwrap().map(|e| e.unwrap().path()));
                } else {
                    found.push(path);
                }
            }
        }
        found
    }
    /// The files a run ID names: the run record, the run lock, the plaintext
    /// bundle, the Vault bundle, the in-place binding.
    fn run_files(&self, run: &RunId) -> Vec<PathBuf> {
        [
            self.state.join("sources/runs").join(run.as_str()),
            self.state.join("sources/runs").join(format!("{run}.lock")),
            self.state
                .join("sources/secrets")
                .join(format!("{run}.json")),
            self.state.join("secrets").join(format!("{run}.json")),
            self.state.join("in-place").join(format!("{run}.json")),
        ]
        .into_iter()
        .filter(|path| std::fs::symlink_metadata(path).is_ok())
        .collect()
    }
    /// Nothing of the capture of `run` is left: no row of the index, no
    /// object or manifest file, no file the run ID names.
    fn assert_nothing_left(&self, run: &RunId) {
        let left: Vec<(&str, i64)> = [
            "capture_owner",
            "root",
            "member",
            "graph",
            "import_session",
            "run_expiry",
            "vault_delete_claim",
            "source_delete_claim WHERE done=0",
        ]
        .into_iter()
        .map(|table| (table, self.count(&format!("SELECT count(*) FROM {table}"))))
        .filter(|(_, rows)| *rows > 0)
        .collect();
        assert_eq!(left, vec![]);
        assert_eq!(self.index_rows(), 0);
        assert_eq!(self.stored_files(), Vec::<PathBuf>::new());
        assert_eq!(self.run_files(run), Vec::<PathBuf>::new());
    }
}

fn bind_in_place(state: &Path, run: &RunId, got: &crate::source::Captured, input: &Path) {
    let digest = crate::ids::Digest::parse(&got.manifest.manifest_digest)
        .or_else(|_| crate::ids::Digest::parse(&format!("sha256:{}", got.manifest.manifest_digest)))
        .unwrap();
    crate::workspace::in_place::bind(
        state,
        run,
        &got.manifest.source_id,
        &digest,
        &WorkerId::parse("local").unwrap(),
        input,
        vec![],
    )
    .unwrap();
}

fn me() -> OwnerRecord {
    OwnerRecord::this_process().unwrap()
}
/// The record of a process that ended here: an owner that is proven gone.
fn gone() -> OwnerRecord {
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    OwnerRecord { pid, ..me() }
}
/// An owner on another machine (the same host name: the place does not hold
/// one), with a PID that no process has here.
fn elsewhere() -> OwnerRecord {
    OwnerRecord {
        pid: gone().pid,
        ..OwnerRecord::on_another_machine()
    }
}

/// The capture of a submitting command, stopped at one of the kill points.
#[test]
#[ignore = "owned subprocess helper"]
fn capture_child() {
    let Ok(state) = std::env::var("BELT_CAPTURE_CHILD_STATE") else {
        return;
    };
    let state = PathBuf::from(state);
    let run = RunId::parse(&std::env::var("BELT_CAPTURE_CHILD_RUN").unwrap()).unwrap();
    let input = PathBuf::from(std::env::var("BELT_CAPTURE_CHILD_INPUT").unwrap());
    let (sources, guard) = SourceStore::open_foreground(&state).unwrap();
    sources.own_capture(&run).unwrap();
    stop_point("owned");
    let vault = Vault::open(&state).unwrap();
    let key = crate::secrets::key::load(&state.parent().unwrap().join("key")).unwrap();
    let got = capture_run(
        &BTreeMap::new(),
        &sources,
        &vault,
        &SecretInputs {
            key: &key,
            declared: &[local_ci::source::SecretSpec {
                glob: ".env".into(),
                required: true,
            }],
        },
        &run,
        &SourceRequest::Directory { dir: input.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    drop(guard);
    bind_in_place(&state, &run, &got, &input);
    stop_point("bound_in_place");
    panic!("the child was given no kill point");
}

fn killed_at(parts: &Parts, point: &str) -> RunId {
    let run = RunId::generate();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "source::retention::abandon_tests::capture_child",
            "--ignored",
            "--nocapture",
        ])
        .env("BELT_CAPTURE_CHILD_STATE", &parts.state)
        .env("BELT_CAPTURE_CHILD_RUN", run.as_str())
        .env("BELT_CAPTURE_CHILD_INPUT", &parts.input)
        .env("BELT_TEST_CAPTURE_STOP", point)
        .stderr(std::process::Stdio::null());
    let mut child = crate::test_child::OwnedChild::spawn(command).unwrap();
    // While the owner lives, a pass deletes nothing of its capture.
    let before = (
        parts.index_rows(),
        parts.stored_files(),
        parts.run_files(&run),
    );
    let alive = parts.pass();
    // Inside its capture the owner holds the source lock, so the pass does
    // not even read; after it, the pass reads the row and leaves it.
    assert!(
        (alive.busy || alive.in_progress == 1) && alive.finished + alive.continuing == 0,
        "{point}: {alive:?}"
    );
    assert_eq!(
        before,
        (
            parts.index_rows(),
            parts.stored_files(),
            parts.run_files(&run)
        ),
        "{point}"
    );
    child.kill().unwrap();
    let _ = child.wait();
    run
}

/// (i) A command killed at each point of its capture leaves, after the next
/// passes, no row of the index, no object or manifest, and no file its run
/// ID names. What must exist at each point is asserted first, so a kill
/// point that moved does not turn the test into one of an empty store.
#[test]
fn a_capture_killed_at_any_point_leaves_nothing_after_the_next_passes() {
    // (point, rows of `root` then, the files the run ID names then)
    for (point, roots, named) in [
        ("owned", 0, vec![]),
        ("locked", 0, vec!["lock"]),
        ("pending", 1, vec!["lock"]),
        ("copied", 2, vec!["lock", "plain"]),
        ("bound", 2, vec!["record", "lock", "plain"]),
        ("handing_off", 2, vec!["record", "lock", "plain"]),
        ("handed_off", 1, vec!["record", "lock", "plain"]),
        ("adopted", 1, vec!["record", "lock", "vault"]),
        (
            "bound_in_place",
            1,
            vec!["record", "lock", "vault", "in-place"],
        ),
    ] {
        let parts = Parts::new();
        let run = killed_at(&parts, point);
        assert_eq!(
            parts.count("SELECT count(*) FROM capture_owner WHERE state='capturing'"),
            1,
            "{point}"
        );
        assert_eq!(parts.count("SELECT count(*) FROM root"), roots, "{point}");
        let kinds: Vec<&str> = parts
            .run_files(&run)
            .iter()
            .map(|path| {
                let text = path.to_str().unwrap();
                if text.contains("/in-place/") {
                    "in-place"
                } else if text.contains("/sources/secrets/") {
                    "plain"
                } else if text.contains("/sources/runs/") && text.ends_with(".lock") {
                    "lock"
                } else if text.contains("/sources/runs/") {
                    "record"
                } else {
                    "vault"
                }
            })
            .collect();
        assert_eq!(kinds, named, "{point}");
        if point == "handing_off" {
            // In the middle of step 6: both roots hold members.
            assert_eq!(
                parts.count("SELECT count(DISTINCT root) FROM member"),
                2,
                "{point}"
            );
        }
        let report = parts.passes();
        assert_eq!(
            (report.finished, report.unknown),
            (1, 0),
            "{point}: {report:?}"
        );
        parts.assert_nothing_left(&run);
    }
}

/// The owner row is the first durable write: at the first kill point the
/// store holds the row and nothing else of the capture, and the pending root
/// is named by the row from the moment it exists.
#[test]
fn the_owner_row_exists_before_anything_of_the_capture_and_names_the_pending_root() {
    let parts = Parts::new();
    let run = RunId::generate();
    parts.sources.own_capture_as(&run, &me()).unwrap();
    assert_eq!(parts.index_rows(), 1);
    assert_eq!(parts.run_files(&run), Vec::<PathBuf>::new());
    assert_eq!(
        parts.sources.capture_owner(&run).unwrap().unwrap().root,
        None
    );
    let pending = parts.sources.capture_pending_root(Some(&run)).unwrap();
    assert_eq!(
        parts.sources.capture_owner(&run).unwrap().unwrap().root,
        Some(pending)
    );
    // The same command may capture its run again; nobody else takes the
    // capture over, and a capture that was given up is not claimed again.
    parts.sources.own_capture_as(&run, &me()).unwrap();
    assert!(parts.sources.own_capture_as(&run, &gone()).is_err());
    assert_eq!(
        parts
            .sources
            .capture_owner(&run)
            .unwrap()
            .unwrap()
            .verdict(),
        CaptureVerdict::InProgress
    );
    parts.sources.give_up_capture(&run).unwrap();
    assert!(parts.sources.own_capture_as(&run, &me()).is_err());
}

/// The owner row names one pending root. A second capture of the run while
/// the root of the first exists is refused and writes nothing, so the first
/// root keeps its name and goes with the abandonment; once that root is gone
/// (the hand-off deleted it) the run may get another; a capture that has no
/// row is never refused.
#[test]
fn a_second_pending_root_is_refused_while_the_row_names_one_that_exists() {
    let parts = Parts::new();
    let run = RunId::generate();
    parts.sources.own_capture_as(&run, &gone()).unwrap();
    let first = parts.sources.capture_pending_root(Some(&run)).unwrap();
    let roots = || parts.count("SELECT count(*) FROM root WHERE kind='pending'");
    assert!(parts.sources.capture_pending_root(Some(&run)).is_err());
    assert_eq!(roots(), 1);
    assert_eq!(
        parts.sources.capture_owner(&run).unwrap().unwrap().root,
        Some(first.clone())
    );
    // Without a row nothing names a root, and nothing is refused.
    let draft = RunId::generate();
    parts.sources.capture_pending_root(Some(&draft)).unwrap();
    parts.sources.capture_pending_root(Some(&draft)).unwrap();
    parts.sources.capture_pending_root(None).unwrap();
    assert_eq!(roots(), 4);
    parts
        .sources
        .retention
        .connect()
        .unwrap()
        .execute("DELETE FROM root WHERE id<>?1", [&first])
        .unwrap();
    // The abandonment finds the first root by the row and leaves no root
    // without a source behind: nothing holds the other deletions.
    let report = parts.passes();
    assert_eq!(report.finished, 1, "{report:?}");
    assert_eq!(parts.count("SELECT count(*) FROM root"), 0);
    // A row whose root is gone names nothing: the next root is its.
    let again = RunId::generate();
    parts.sources.own_capture_as(&again, &me()).unwrap();
    let old = parts.sources.capture_pending_root(Some(&again)).unwrap();
    parts
        .sources
        .retention
        .connect()
        .unwrap()
        .execute("DELETE FROM root WHERE id=?1", [&old])
        .unwrap();
    let new = parts.sources.capture_pending_root(Some(&again)).unwrap();
    assert_eq!(
        parts.sources.capture_owner(&again).unwrap().unwrap().root,
        Some(new)
    );
}

/// An interrupt does not stop the blocking capture: the command gives up and
/// the capture goes on until the process ends. A pending root created after
/// that would have no name: the row is `given_up`, or the abandonment has
/// already deleted it. No root is written in either case; a capture that
/// never had a row (a plan draft) is not held to this.
#[test]
fn a_capture_that_was_given_up_creates_no_pending_root() {
    let parts = Parts::new();
    let roots = || parts.count("SELECT count(*) FROM root");
    let run = RunId::generate();
    parts.sources.own_capture_as(&run, &me()).unwrap();
    parts.sources.give_up_capture(&run).unwrap();
    assert!(parts.sources.capture_pending_root(Some(&run)).is_err());
    assert_eq!(roots(), 0);
    assert_eq!(
        parts.sources.capture_owner(&run).unwrap().unwrap().root,
        None
    );
    // Another handle on the store (it did not write the row) is refused by
    // the row itself.
    let other = SourceStore::open(&parts.state).unwrap();
    assert!(other.capture_pending_root(Some(&run)).is_err());
    assert_eq!(roots(), 0);
    // The abandonment deleted the row: the handle still knows it owned one.
    let report = parts.passes();
    assert_eq!(report.finished, 1, "{report:?}");
    assert!(parts.sources.capture_owner(&run).unwrap().is_none());
    assert!(parts.sources.capture_pending_root(Some(&run)).is_err());
    assert_eq!(roots(), 0);
    // A capture that never had a row goes on without one.
    let draft = RunId::generate();
    parts.sources.capture_pending_root(Some(&draft)).unwrap();
    assert_eq!(roots(), 1);
}

/// "3の途中": the owner died while the members of its pending root were
/// being registered. The root has no source and an unfinished graph, which
/// holds every other deletion of the store; the abandonment exempts exactly
/// this root, deletes the files it names and the root. The files local-ci
/// wrote that no row names yet stay: nothing proves whose they are.
#[test]
fn a_capture_killed_while_registering_loses_what_its_root_names_and_only_that() {
    let parts = Parts::new();
    let run = RunId::generate();
    let got = parts.capture_of(&run, &parts.input);
    let files = parts.stored_files().len();
    assert_eq!(files, FILES + 1);
    // Take the index back to "local-ci wrote the files, nothing is registered".
    parts
        .sources
        .retention
        .connect()
        .unwrap()
        .execute_batch("DELETE FROM member; DELETE FROM graph; DELETE FROM root; DELETE FROM object; DELETE FROM publication;")
        .unwrap();
    parts.sources.own_capture_as(&run, &gone()).unwrap();
    let pending = parts.sources.capture_pending_root(Some(&run)).unwrap();
    // One bounded step: 32 of the 41 members.
    assert!(!parts
        .sources
        .retention
        .register_step(
            &pending,
            "pending",
            &got.manifest.ci_source_id,
            1,
            &mut None,
            u64::MAX
        )
        .unwrap());
    let registered = parts.count("SELECT count(*) FROM member") as usize;
    assert!(registered > 0 && registered < files, "{registered}");
    assert_eq!(
        parts.count("SELECT count(*) FROM root WHERE source IS NULL")
            + parts.count("SELECT count(*) FROM graph WHERE done=0"),
        2
    );
    let report = parts.passes();
    assert_eq!((report.finished, report.unknown), (1, 0), "{report:?}");
    assert_eq!(parts.index_rows(), 0);
    assert_eq!(parts.run_files(&run), Vec::<PathBuf>::new());
    assert_eq!(parts.stored_files().len(), files - registered);
}

/// An abandoned root that a pass could not finish (one of its files cannot
/// be proven ours) stays, with no source, as `abandoned`. It protects
/// nothing, so it holds none of the other pages: not the page of expired
/// runs (B), not the Vault page, not the preview. The same root holds all
/// three while it is still `pending`.
#[test]
fn an_abandoned_root_that_stays_holds_no_other_page() {
    let parts = Parts::new();
    let run = RunId::generate();
    let got = parts.capture_of(&run, &parts.input);
    parts
        .sources
        .retention
        .connect()
        .unwrap()
        .execute_batch("DELETE FROM member; DELETE FROM graph; DELETE FROM root; DELETE FROM object; DELETE FROM publication; DELETE FROM run_expiry; DELETE FROM import_session;")
        .unwrap();
    parts.sources.own_capture_as(&run, &me()).unwrap();
    let pending = parts.sources.capture_pending_root(Some(&run)).unwrap();
    // One bounded step, as when the owner stops while registering: the root
    // has no source and its graph is unfinished.
    assert!(!parts
        .sources
        .retention
        .register_step(
            &pending,
            "pending",
            &got.manifest.ci_source_id,
            1,
            &mut None,
            u64::MAX
        )
        .unwrap());
    assert_eq!(
        (
            parts.count("SELECT count(*) FROM root WHERE source IS NULL AND kind='pending'"),
            parts.count("SELECT count(*) FROM graph WHERE done=0"),
        ),
        (1, 1)
    );
    let mut clock = 0u64;
    let mut held = |parts: &Parts| -> (bool, bool, bool) {
        let mut last = (true, true, true);
        // A clock holds by itself until its quarantine (here 10) is over.
        for _ in 0..4 {
            clock += 5;
            let boot = || Some("boot".to_string());
            last = (
                parts
                    .sources
                    .retention_page(
                        &parts.owner,
                        &mut RetentionCursor::default(),
                        100 + clock,
                        10 + clock,
                        boot(),
                        10,
                    )
                    .unwrap()
                    .held,
                parts
                    .sources
                    .vault_retention_page(
                        &parts.owner,
                        &parts.vault,
                        &mut VaultCursor::default(),
                        100 + clock,
                        10 + clock,
                        boot(),
                    )
                    .unwrap()
                    .held,
                parts
                    .sources
                    .retention_preview(
                        &parts.owner,
                        &mut PreviewCursor::default(),
                        100 + clock,
                        10 + clock,
                        boot(),
                        10,
                    )
                    .unwrap()
                    .held,
            );
        }
        last
    };
    assert_eq!(held(&parts), (true, true, true));
    // The owner gives up; one object file has a second name, so the pass
    // cannot prove it and the root stays.
    let path: String = parts
        .sources
        .retention
        .connect()
        .unwrap()
        .query_row(
            "SELECT o.path FROM member m JOIN object o ON o.id=m.object WHERE m.root=?1 ORDER BY m.object LIMIT 1",
            [&pending],
            |r| r.get(0),
        )
        .unwrap();
    let file = parts.state.join("sources").join(&path);
    assert!(file.is_file(), "{path}");
    std::fs::hard_link(&file, parts.root.path().join("second-name")).unwrap();
    parts.sources.give_up_capture(&run).unwrap();
    for _ in 0..3 {
        let report = parts.pass();
        assert_eq!((report.finished, report.continuing), (0, 1), "{report:?}");
    }
    assert_eq!(
        (
            parts.count("SELECT count(*) FROM root WHERE source IS NULL AND kind='abandoned'"),
            parts.count("SELECT count(*) FROM root"),
            parts.count("SELECT count(*) FROM member"),
        ),
        (1, 1, 1)
    );
    assert_eq!(held(&parts), (false, false, false));
    assert!(file.is_file());
}

/// (ii) A capture whose owner is alive is never touched, by any number of
/// passes (a resident that restarts runs the same pass first).
#[test]
fn a_capture_whose_owner_is_alive_is_left_whole() {
    let parts = Parts::new();
    let run = parts.captured(&me());
    // A finished capture leaves nothing of its pending root: the root, its
    // members and its graph went with the hand-over to the run root.
    let others = format!(
        "SELECT (SELECT count(*) FROM root WHERE id<>'{run}')+(SELECT count(*) FROM member WHERE root<>'{run}')+(SELECT count(*) FROM graph WHERE root<>'{run}')"
    );
    assert_eq!(parts.count(&others), 0);
    let before = (
        parts.index_rows(),
        parts.stored_files(),
        parts.run_files(&run),
    );
    for _ in 0..3 {
        let report = parts.pass();
        assert_eq!(
            (report.examined, report.in_progress, report.paths),
            (1, 1, 0)
        );
    }
    assert_eq!(
        before,
        (
            parts.index_rows(),
            parts.stored_files(),
            parts.run_files(&run)
        )
    );
    assert_eq!(
        parts
            .sources
            .capture_owner(&run)
            .unwrap()
            .unwrap()
            .verdict(),
        CaptureVerdict::InProgress
    );
}

/// The owner said it will not submit: the capture goes with the next passes
/// although the owner process is alive.
#[test]
fn a_capture_its_owner_gave_up_is_deleted_while_the_owner_lives() {
    let parts = Parts::new();
    let run = parts.captured(&me());
    parts.sources.give_up_capture(&run).unwrap();
    assert_eq!(parts.passes().finished, 1);
    parts.assert_nothing_left(&run);
}

/// (v) An owner recorded on another machine cannot be judged here: its capture
/// stays whole and is counted.
#[test]
fn a_capture_owned_on_another_host_is_kept_and_counted() {
    let parts = Parts::new();
    let run = parts.captured(&elsewhere());
    let before = (
        parts.index_rows(),
        parts.stored_files(),
        parts.run_files(&run),
    );
    let report = parts.pass();
    assert_eq!((report.elsewhere, report.finished, report.paths), (1, 0, 0));
    assert_eq!(
        before,
        (
            parts.index_rows(),
            parts.stored_files(),
            parts.run_files(&run)
        )
    );
    assert_eq!(
        parts
            .sources
            .capture_owner(&run)
            .unwrap()
            .unwrap()
            .verdict(),
        CaptureVerdict::Elsewhere
    );
}

/// (vi) The object files of an abandoned capture go only when no other root
/// holds them: a second capture of the same input keeps every file, and
/// loses nothing when the first is deleted.
#[test]
fn an_abandoned_capture_keeps_the_objects_another_root_holds() {
    let parts = Parts::new();
    let kept = parts.captured(&me());
    let files = parts.stored_files();
    let abandoned = parts.captured(&gone());
    assert_eq!(
        parts.stored_files(),
        files,
        "the same content, the same files"
    );
    let report = parts.passes();
    assert_eq!(
        (report.finished, report.in_progress, report.paths),
        (1, 1, 0)
    );
    assert_eq!(parts.stored_files(), files);
    assert_eq!(parts.run_files(&abandoned), Vec::<PathBuf>::new());
    assert_eq!(parts.run_files(&kept).len(), 4);
    assert_eq!(parts.count("SELECT count(*) FROM capture_owner"), 1);
    assert_eq!(
        parts.count("SELECT count(*) FROM member") as usize,
        files.len()
    );
    // And the other way round: once the second is abandoned too, they go.
    parts.sources.give_up_capture(&kept).unwrap();
    assert_eq!(parts.passes().finished, 1);
    parts.assert_nothing_left(&kept);
}

/// A run that has a row took its capture over: whatever the owner row says,
/// only the row is deleted.
#[test]
fn the_capture_of_a_run_that_has_a_row_loses_only_its_owner_row() {
    for owner in [gone(), me(), elsewhere()] {
        let parts = Parts::new();
        let run = parts.captured(&owner);
        parts
            .owner
            .create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        let before = (parts.stored_files(), parts.run_files(&run));
        let report = parts.pass();
        assert_eq!(
            (report.handed_off, report.finished, report.paths),
            (1, 0, 0)
        );
        assert_eq!(parts.count("SELECT count(*) FROM capture_owner"), 0);
        assert_eq!(before, (parts.stored_files(), parts.run_files(&run)));
        assert_eq!(parts.count("SELECT count(*) FROM root WHERE kind='run'"), 1);
    }
}

/// A refused enqueue ends the capture even though its owner is alive and
/// still `capturing`; without the refusal the same call deletes nothing.
#[test]
fn a_refusal_abandons_a_capture_its_live_owner_still_claims() {
    let parts = Parts::new();
    let run = parts.captured(&me());
    let judged = parts
        .sources
        .abandon_captures(
            &parts.owner,
            &parts.vault,
            AbandonScope::Run(&run),
            &mut AbandonCursor::default(),
        )
        .unwrap();
    assert_eq!((judged.in_progress, judged.paths), (1, 0));
    assert_eq!(parts.run_files(&run).len(), 4);
    let mut cursor = AbandonCursor::default();
    for _ in 0..4 {
        parts
            .sources
            .abandon_captures(
                &parts.owner,
                &parts.vault,
                AbandonScope::Refused(&run),
                &mut cursor,
            )
            .unwrap();
    }
    parts.assert_nothing_left(&run);
}

/// A file that is not provably ours (here: the run record has a second
/// name) is kept, with the owner row that names it; once it is provable the
/// next pass finishes.
#[test]
fn a_file_that_is_not_provably_ours_keeps_the_capture_until_it_is() {
    let parts = Parts::new();
    let run = parts.captured(&gone());
    let record = parts.state.join("sources/runs").join(run.as_str());
    let second = parts.root.path().join("second-name");
    std::fs::hard_link(&record, &second).unwrap();
    for _ in 0..3 {
        let report = parts.pass();
        assert_eq!((report.finished, report.continuing), (0, 1), "{report:?}");
        assert!(report.unknown >= 1);
    }
    assert!(record.exists());
    assert_eq!(parts.count("SELECT count(*) FROM capture_owner"), 1);
    // The steps after the record did not run: the roots and the lock stay.
    assert!(parts.count("SELECT count(*) FROM root") > 0);
    assert!(parts
        .state
        .join("sources/runs")
        .join(format!("{run}.lock"))
        .exists());
    std::fs::remove_file(second).unwrap();
    assert_eq!(parts.passes().finished, 1);
    parts.assert_nothing_left(&run);
}

/// A symlink planted at a name of the run is not followed and not removed.
#[test]
fn a_symlink_at_a_name_of_the_run_is_neither_followed_nor_removed() {
    let parts = Parts::new();
    let run = parts.captured(&gone());
    let outside = parts.root.path().join("outside");
    std::fs::write(&outside, b"not ours").unwrap();
    let binding = parts.state.join("in-place").join(format!("{run}.json"));
    std::fs::remove_file(&binding).unwrap();
    std::os::unix::fs::symlink(&outside, &binding).unwrap();
    let report = parts.pass();
    assert_eq!((report.finished, report.continuing), (0, 1));
    assert!(std::fs::symlink_metadata(&binding).unwrap().is_symlink());
    assert_eq!(std::fs::read(&outside).unwrap(), b"not ours");
}

/// A pass that dies between two steps is continued by the next from the
/// owner row: every step is idempotent and the row goes last.
#[test]
fn an_abandonment_that_stops_between_steps_is_finished_by_a_later_pass() {
    for stage in [
        "abandon_after_rows",
        "abandon_after_vault",
        "abandon_after_files",
        "abandon_after_roots",
        "after_claim",
        "before_unlink",
        "after_unlink",
        "before_completion",
    ] {
        let parts = Parts::new();
        let run = parts.captured(&gone());
        set_fault(Some(stage));
        let stopped = parts.sources.abandon_captures(
            &parts.owner,
            &parts.vault,
            AbandonScope::All,
            &mut AbandonCursor::default(),
        );
        set_fault(None);
        if let Ok(report) = &stopped {
            assert_eq!(report.finished, 0, "{stage}: {report:?}");
        }
        assert_eq!(
            parts.count("SELECT count(*) FROM capture_owner"),
            1,
            "{stage}"
        );
        let report = parts.passes();
        assert_eq!(report.finished, 1, "{stage}: {report:?}");
        parts.assert_nothing_left(&run);
    }
}

/// The parent rows of a cancel that was accepted for a capture in progress
/// go with the abandoned capture, and nothing of another run is touched.
#[test]
fn the_cancel_rows_of_an_abandoned_capture_are_deleted_with_it() {
    use crate::service::completion::Target;
    let parts = Parts::new();
    let run = parts.captured(&gone());
    let other = RunId::generate();
    parts
        .owner
        .create_run(&other, None, None, &serde_json::json!({}), 1)
        .unwrap();
    for (target, id) in [(&run, "of-capture"), (&other, "of-run")] {
        parts
            .owner
            .cancel_control_of_capture(Target::Run(target.clone()), 0, id.into(), 5)
            .unwrap();
    }
    let rows = |run: &RunId| -> (i64, i64, i64) {
        let run = run.to_string();
        parts
            .owner
            .db
            .call(move |c| {
                Ok((
                    c.query_row(
                        "SELECT count(*) FROM run_cancel_intent WHERE run_id=?1",
                        [&run],
                        |r| r.get(0),
                    )?,
                    c.query_row(
                        "SELECT count(*) FROM control_request WHERE run_id=?1",
                        [&run],
                        |r| r.get(0),
                    )?,
                    c.query_row("SELECT count(*) FROM event WHERE run_id=?1", [&run], |r| {
                        r.get(0)
                    })?,
                ))
            })
            .unwrap()
    };
    assert_eq!(rows(&run), (1, 1, 1));
    let of_other = rows(&other);
    assert_eq!((of_other.0, of_other.1), (1, 1));
    assert_eq!(parts.passes().finished, 1);
    assert_eq!(rows(&run), (0, 0, 0));
    assert_eq!(rows(&other), of_other);
    parts.assert_nothing_left(&run);
}

/// (xiii) A capture that is not made for a submission (a plan draft writes
/// into its own store) writes no owner row.
#[test]
fn a_capture_without_an_owner_writes_no_owner_row() {
    let parts = Parts::new();
    let run = RunId::generate();
    parts.capture_of(&run, &parts.input);
    assert_eq!(parts.count("SELECT count(*) FROM capture_owner"), 0);
    // (vii) And such a capture, which no row owns, is never deleted here.
    let before = (parts.stored_files(), parts.run_files(&run));
    let report = parts.pass();
    assert_eq!((report.examined, report.paths), (0, 0));
    assert_eq!(before, (parts.stored_files(), parts.run_files(&run)));
}

/// An index of the version before the owner table gets the table on its
/// next open and keeps its version, so the binary before this one opens it.
#[test]
fn an_index_from_before_the_owner_table_gets_it_and_keeps_its_version() {
    let parts = Parts::new();
    let c = parts.sources.retention.connect().unwrap();
    c.execute_batch("DROP TABLE capture_owner;").unwrap();
    drop(c);
    let again = SourceStore::open(&parts.state).unwrap();
    assert_eq!(again.capture_owners().unwrap(), vec![]);
    let version: u32 = again
        .retention
        .connect()
        .unwrap()
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 1);
}
