//! `belt upgrade` against a fake service manager and a private temporary
//! HOME. Nothing here reaches launchctl, systemctl or a real service file;
//! the binaries are small files whose digests name their versions.
use super::*;
use crate::ids::{RunId, TaskId};
use crate::storage::{CoordinatorStore, WorkerStore};
use macos::{policy::Plan, LaunchError, Receipt};
use std::{cell::RefCell, os::unix::fs::PermissionsExt, rc::Rc};

struct Unknown;
impl Launcher for Unknown {
    fn launchctl(&mut self, _: &[String]) -> Result<(), LaunchError> {
        panic!("unknown service must not start")
    }
    fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
        Err(LaunchError::NotApplied("unknown".into()))
    }
}

/// Jobs by name, each with the service file it was started from.
#[derive(Default)]
struct World {
    loaded: BTreeMap<String, String>,
    calls: Vec<Vec<String>>,
    /// A job that does not stop when it is booted out.
    stuck: bool,
}
#[derive(Clone, Default)]
struct Fake(Rc<RefCell<World>>);
fn label_of(word: &str) -> String {
    let name = word.rsplit('/').next().unwrap();
    name.strip_suffix(".plist").unwrap_or(name).to_string()
}
impl Launcher for Fake {
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError> {
        let mut w = self.0.borrow_mut();
        w.calls.push(arguments.to_vec());
        match arguments[0].as_str() {
            "bootstrap" => {
                let text = fs::read_to_string(&arguments[2]).unwrap();
                let label = label_of(&arguments[2]);
                assert!(w.loaded.insert(label, text).is_none(), "started twice");
                Ok(())
            }
            "bootout" => {
                if !w.stuck {
                    w.loaded.remove(&label_of(arguments.last().unwrap()));
                }
                Ok(())
            }
            _ => Err(LaunchError::NotApplied("unexpected".into())),
        }
    }
    fn verify_loaded(&mut self, plan: &Plan, _: &Receipt) -> Result<(), LaunchError> {
        if self.0.borrow().loaded.contains_key(plan.label()) {
            Ok(())
        } else {
            Err(LaunchError::NotApplied("not loaded".into()))
        }
    }
    fn loaded_state(&mut self, plan: &Plan, _: &Receipt) -> LoadedState {
        if self.0.borrow().loaded.contains_key(plan.label()) {
            LoadedState::Match
        } else {
            LoadedState::Absent
        }
    }
}
impl Fake {
    fn running(&self, label: &str) -> Option<String> {
        self.0.borrow().loaded.get(label).cloned()
    }
    fn starts(&self) -> usize {
        let w = self.0.borrow();
        w.calls.iter().filter(|c| c[0] == "bootstrap").count()
    }
}

fn never() -> bool {
    false
}

const OPTIONS: Options = Options {
    wait: Duration::ZERO,
    deadline: COMMIT_DEADLINE,
    interruptible: false,
    interrupted: never,
};

struct Host {
    _dir: tempfile::TempDir,
    env: BTreeMap<String, String>,
    paths: Paths,
    old: String,
    new: String,
    new_exe: PathBuf,
    fake: Fake,
    label: String,
    guard: String,
    run: RunId,
    task: TaskId,
}
impl Host {
    /// An initialized machine whose resident runs the old version, with a
    /// run of its own in progress and one finished attempt on its worker.
    async fn new() -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        let source = root.join("source");
        fs::write(&source, b"test executable").unwrap();
        let new_exe = root.join("new");
        fs::write(&new_exe, b"new version").unwrap();
        let env = BTreeMap::from([
            ("HOME".into(), root.to_str().unwrap().into()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            (
                "XDG_CONFIG_HOME".into(),
                root.join("config").to_str().unwrap().into(),
            ),
        ]);
        let context = ContextInput {
            env: env.clone(),
            executable: source.clone(),
        };
        assert!(initialize(context, &mut Unknown).await.is_err());
        let paths = Paths::resolve(&env).unwrap();
        let spec = bootstrap::read_input(&paths.input).unwrap();
        drop(WorkerStore::open(&paths.worker, &spec.worker_id).unwrap());
        let store = CoordinatorStore::open(&paths.state, 1).unwrap();
        let binding = paths.worker.join("worker-binding.json");
        fs::write(
            &binding,
            serde_json::to_vec(&(1, store.cluster_id(), &spec.worker_id, "key")).unwrap(),
        )
        .unwrap();
        fs::set_permissions(&binding, fs::Permissions::from_mode(0o600)).unwrap();
        let run = RunId::generate();
        store
            .create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        let task = TaskId::generate();
        store
            .create_task(&task, &run, &serde_json::json!({}), 1)
            .unwrap();
        drop(store);
        let db = rusqlite::Connection::open(paths.worker.join("worker.db")).unwrap();
        ws::tests::put_attempt(&db, &CoordinatorId::generate(), 1, "terminal");
        drop(db);
        let journal = read_journal(&paths.journal).unwrap();
        let plan = plan(&paths, &journal).unwrap();
        let guard = guard_plan(&paths, &journal).unwrap();
        let fake = Fake::default();
        fake.0
            .borrow_mut()
            .loaded
            .insert(plan.label().to_string(), plan.plist().to_string());
        Self {
            old: ws::sha256_file(&source).unwrap(),
            new: ws::sha256_file(&new_exe).unwrap(),
            label: plan.label().to_string(),
            guard: guard.label().to_string(),
            _dir: dir,
            run,
            task,
            env,
            paths,
            new_exe,
            fake,
        }
    }
    fn exe(&self, digest: &str) -> PathBuf {
        self.paths.config.join("bin").join(digest).join("belt")
    }
    fn resident_link(&self) -> PathBuf {
        fs::read_link(self.paths.config.join("bin/resident")).unwrap()
    }
    /// What the new version does while it runs: it finishes the member's own
    /// run and its worker records another attempt.
    fn write_as_new_version(&self) {
        let store = CoordinatorStore::open(&self.paths.state, 1).unwrap();
        store.cancel_task(&self.task, 2).unwrap();
        store.close_run(&self.run, 2).unwrap();
        drop(store);
        let db = rusqlite::Connection::open(self.paths.worker.join("worker.db")).unwrap();
        ws::tests::put_attempt(&db, &CoordinatorId::generate(), 2, "terminal");
    }
    fn worker_attempts(&self) -> i64 {
        let db = rusqlite::Connection::open(self.paths.worker.join("worker.db")).unwrap();
        db.query_row("SELECT count(*) FROM worker_attempt", [], |r| r.get(0))
            .unwrap()
    }
    fn own_run_open(&self) -> bool {
        CoordinatorStore::open_read_only(&self.paths.state)
            .unwrap()
            .has_nonterminal_runs()
            .unwrap()
    }
    /// The machine is exactly as before the upgrade: the old version runs
    /// from its own files, with both databases as they were kept.
    fn assert_old_version_back(&self) {
        let spec = bootstrap::read_input(&self.paths.input).unwrap();
        assert_eq!(spec.executable, self.exe(&self.old));
        managed_journal(&spec).unwrap();
        assert_eq!(self.resident_link(), Path::new(&self.old));
        let running = self.fake.running(&self.label).unwrap();
        assert!(running.contains(self.exe(&self.old).to_str().unwrap()));
        assert!(self.own_run_open(), "the parent database went back");
        assert_eq!(self.worker_attempts(), 1, "the worker database went back");
        self.assert_finished();
    }
    fn assert_finished(&self) {
        assert!(self.fake.running(&self.guard).is_none());
        assert!(!self
            .paths
            .services
            .join(format!("{}.plist", self.guard))
            .exists());
        assert!(!record_path(&self.paths).exists());
        assert!(!control::held(&self.paths.worker));
        assert!(!control::in_trial(&self.paths.worker));
        for name in [KEPT_JOURNAL, KEPT_INPUT, KEPT_PARENT_DB, KEPT_WORKER_DB] {
            assert!(!kept(&self.paths, name).exists(), "{name} kept");
        }
        // The old binary is never touched by an upgrade or its rollback.
        assert_eq!(fs::read(self.exe(&self.old)).unwrap(), b"test executable");
    }
}

#[tokio::test]
async fn a57_upgrade_moves_bin_resident_and_leaves_the_old_binary_untouched() {
    let h = Host::new().await;
    control::write(&h.paths.worker, Control::draining()).unwrap();
    let mut fake = h.fake.clone();
    let mut checked = 0;
    let upgraded = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        OPTIONS,
        &|| 0,
        &mut |spec: &Spec, launcher: &mut Fake| {
            checked += 1;
            assert_eq!(spec.executable, h.exe(&h.new));
            // During the trial the hold stays, and the guard watches.
            assert!(control::held(&h.paths.worker));
            // The new version promises nothing until the commit.
            assert!(control::in_trial(&h.paths.worker));
            assert!(launcher.running(&h.guard).is_some());
            assert!(record_path(&h.paths).exists());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(checked, 1);
    assert_eq!(
        upgraded,
        Upgraded {
            from: h.old.clone(),
            to: h.new.clone()
        }
    );
    assert_eq!(h.resident_link(), Path::new(&h.new));
    assert_eq!(fs::read(h.exe(&h.new)).unwrap(), b"new version");
    let spec = bootstrap::read_input(&h.paths.input).unwrap();
    assert_eq!(spec.executable, h.exe(&h.new));
    // Every receipt of the managed resident holds for the new version.
    managed_journal(&spec).unwrap();
    let running = h.fake.running(&h.label).unwrap();
    assert!(running.contains(h.exe(&h.new).to_str().unwrap()));
    // Nothing of the switch touched the databases.
    assert!(h.own_run_open());
    assert_eq!(h.worker_attempts(), 1);
    h.assert_finished();
    // The user's own drain outlives the commit.
    assert_eq!(control::read(&h.paths.worker).unwrap(), Control::draining());
    // The same version again is refused before anything is held.
    let again = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        OPTIONS,
        &|| 0,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert!(again.to_string().contains("already runs"), "{again:#}");
    assert!(!control::held(&h.paths.worker));
}

#[tokio::test]
async fn a57_health_failure_restores_both_databases_together_and_the_previous_version() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let error = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        OPTIONS,
        &|| 0,
        &mut |_: &Spec, _: &mut Fake| {
            h.write_as_new_version();
            assert!(!h.own_run_open());
            assert_eq!(h.worker_attempts(), 2);
            anyhow::bail!("no hello")
        },
    )
    .unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("health check") && text.contains("restored"),
        "{text}"
    );
    h.assert_old_version_back();
    // The old version was started again once: its attempts are adopted from
    // the kept records, nothing is placed anew.
    assert_eq!(fake.starts(), 3, "guard, new version, old version");
}

#[tokio::test]
async fn a57_no_commit_by_the_deadline_rolls_back_without_the_new_version_answering() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 1_000).unwrap();
    // The new version runs, writes, and never answers; the CLI is gone.
    h.write_as_new_version();
    let deadline = 1_000 + COMMIT_DEADLINE.as_millis() as u64;
    assert!(!guard_tick(&h.paths.config, &mut fake, deadline - 1).unwrap());
    assert_eq!(h.resident_link(), Path::new(&h.new));
    assert!(control::held(&h.paths.worker));
    assert!(guard_tick(&h.paths.config, &mut fake, deadline).unwrap());
    h.assert_old_version_back();
    // A commit after the deadline changes nothing and says so.
    let late = commit(&trial, &mut fake).unwrap_err();
    assert!(late.to_string().contains("rolled back"), "{late:#}");
    h.assert_old_version_back();
    // A guard started again later finds nothing to watch.
    assert!(guard_tick(&h.paths.config, &mut fake, deadline + 1).unwrap());
}

#[tokio::test]
async fn a57_the_guard_rolls_back_files_a_newer_version_wrote_with_fields_it_does_not_know() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    for file in [&h.paths.journal, &h.paths.input] {
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(file).unwrap()).unwrap();
        value["field_of_a_newer_version"] = serde_json::json!(1);
        fs::write(file, serde_json::to_vec(&value).unwrap()).unwrap();
    }
    let mut record: serde_json::Value =
        serde_json::from_slice(&fs::read(record_path(&h.paths)).unwrap()).unwrap();
    record["field_of_a_newer_version"] = serde_json::json!(1);
    fs::write(record_path(&h.paths), serde_json::to_vec(&record).unwrap()).unwrap();
    let deadline = COMMIT_DEADLINE.as_millis() as u64;
    assert!(guard_tick(&h.paths.config, &mut fake, deadline).unwrap());
    h.assert_old_version_back();
}

#[tokio::test]
async fn a57_a_database_the_new_version_left_unreadable_is_still_rolled_back() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    for database in [
        h.paths.state.join("coordinator.db"),
        h.paths.worker.join("worker.db"),
    ] {
        fs::write(&database, b"not a database at all").unwrap();
        let mut wal = database.clone().into_os_string();
        wal.push("-wal");
        fs::write(&wal, b"not a log either").unwrap();
    }
    let deadline = COMMIT_DEADLINE.as_millis() as u64;
    assert!(guard_tick(&h.paths.config, &mut fake, deadline).unwrap());
    // The log of the broken database is gone before anything reads again.
    for directory in [&h.paths.state, &h.paths.worker] {
        assert!(!directory.join("coordinator.db-wal").exists());
        assert!(!directory.join("worker.db-wal").exists());
    }
    h.assert_old_version_back();
}

#[tokio::test]
async fn a57_rollback_never_restores_under_a_version_that_does_not_stop() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    h.write_as_new_version();
    fake.0.borrow_mut().stuck = true;
    let deadline = COMMIT_DEADLINE.as_millis() as u64;
    let error = guard_tick(&h.paths.config, &mut fake, deadline).unwrap_err();
    assert!(
        error.to_string().contains("not confirmed stopped"),
        "{error:#}"
    );
    // Nothing went back while the new version may still write.
    assert!(!h.own_run_open());
    assert_eq!(h.worker_attempts(), 2);
    assert!(record_path(&h.paths).exists());
    // Once it stops, the next look finishes the rollback; no commit can
    // win in between.
    fake.0.borrow_mut().stuck = false;
    assert!(guard_tick(&h.paths.config, &mut fake, deadline).unwrap());
    h.assert_old_version_back();
}

#[tokio::test]
async fn a57_a_commit_cut_short_after_its_record_is_finished_by_the_guard() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    // The CLI removed the record (the commit) and died at once.
    fs::remove_file(record_path(&h.paths)).unwrap();
    assert!(control::held(&h.paths.worker));
    assert!(guard_tick(&h.paths.config, &mut fake, 0).unwrap());
    assert_eq!(h.resident_link(), Path::new(&h.new));
    assert!(h
        .fake
        .running(&h.label)
        .unwrap()
        .contains(h.exe(&h.new).to_str().unwrap()));
    h.assert_finished();
}

#[tokio::test]
async fn a57_workers_resume_does_not_lift_the_upgrade_hold() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    let spec = bootstrap::read_input(&h.paths.input).unwrap();
    let store = CoordinatorStore::open_read_only(&h.paths.state).unwrap();
    let identity = macos::WorkerIdentity {
        cluster_id: store.cluster_id().clone(),
        worker_id: spec.worker_id,
    };
    drop(store);
    let mut marker = ws::MarkerControl {
        state: h.paths.worker.clone(),
        wait: Duration::ZERO,
        interval: Duration::from_millis(10),
    };
    use macos::WorkerControl as _;
    marker
        .request(&identity, macos::WorkerRequest::Resume)
        .unwrap();
    assert_eq!(control::read(&h.paths.worker).unwrap(), Control::default());
    assert!(control::held(&h.paths.worker));
    commit(&trial, &mut fake).unwrap();
    assert!(!control::held(&h.paths.worker));
}

/// The journal as a crash between the two journal writes of a key rotation
/// leaves it: `switching_key` names the staged key's receipt.
fn leave_key_switch(h: &Host) {
    let mut j = super::super::read_raw_journal(&h.paths.journal).unwrap();
    let staged = crate::group::next_key_path(&h.paths.key);
    crate::secrets::key::init(&staged).unwrap();
    let mut next = Proof::read(&staged).unwrap();
    next.path = h.paths.key.clone();
    j.switching_key = Some(next);
    fs::write(&h.paths.journal, serde_json::to_vec(&j).unwrap()).unwrap();
}

/// Neither the live journal nor the copy kept for a rollback names a key
/// switch, and both receipts match the key on disk.
fn assert_journals_settled(h: &Host) {
    for file in [h.paths.journal.clone(), kept(&h.paths, KEPT_JOURNAL)] {
        assert!(!fs::read_to_string(&file).unwrap().contains("switching_key"));
        let j = super::super::read_raw_journal(&file).unwrap();
        j.files
            .iter()
            .find(|p| p.path == h.paths.key)
            .unwrap()
            .verify()
            .unwrap();
    }
}

#[tokio::test]
async fn a_leftover_key_switch_before_the_rename_is_cleared_before_the_journal_is_kept() {
    let h = Host::new().await;
    leave_key_switch(&h);
    let old_key = fs::read(&h.paths.key).unwrap();
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    assert_journals_settled(&h);
    assert_eq!(fs::read(&h.paths.key).unwrap(), old_key);
}

#[tokio::test]
async fn a_leftover_key_switch_after_the_rename_is_cleared_before_the_journal_is_kept() {
    let h = Host::new().await;
    leave_key_switch(&h);
    fs::rename(crate::group::next_key_path(&h.paths.key), &h.paths.key).unwrap();
    let new_key = fs::read(&h.paths.key).unwrap();
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    assert_journals_settled(&h);
    assert_eq!(fs::read(&h.paths.key).unwrap(), new_key);
}

#[tokio::test]
async fn an_unresolvable_leftover_key_switch_stops_the_upgrade_with_nothing_changed() {
    let h = Host::new().await;
    leave_key_switch(&h);
    let bytes = fs::read(&h.paths.key).unwrap();
    let mut replacement = tempfile::NamedTempFile::new_in(&h.paths.config).unwrap();
    std::io::Write::write_all(&mut replacement, &bytes).unwrap();
    replacement.persist(&h.paths.key).unwrap();
    let journal = fs::read(&h.paths.journal).unwrap();
    let mut fake = h.fake.clone();
    assert!(begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).is_err());
    assert_eq!(fs::read(&h.paths.journal).unwrap(), journal);
    assert!(fake.0.borrow().calls.is_empty());
    assert!(!control::held(&h.paths.worker));
    assert!(!record_path(&h.paths).exists());
    assert_eq!(h.resident_link(), Path::new(&h.old));
}

#[tokio::test]
async fn a57_a_failure_before_the_switch_lifts_the_hold_and_changes_nothing() {
    let h = Host::new().await;
    // A different file already has the new version's name.
    let directory = h.exe(&h.new).parent().unwrap().to_owned();
    fs::create_dir_all(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(h.exe(&h.new), b"something else").unwrap();
    let mut fake = h.fake.clone();
    let error = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        OPTIONS,
        &|| 0,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("refusing to overwrite"),
        "{error:#}"
    );
    assert_eq!(fs::read(h.exe(&h.new)).unwrap(), b"something else");
    assert!(fake.0.borrow().calls.is_empty());
    assert!(!control::held(&h.paths.worker));
    assert!(!record_path(&h.paths).exists());
    assert_eq!(h.resident_link(), Path::new(&h.old));
}

#[tokio::test]
async fn a57_live_work_keeps_the_old_version_and_lifts_the_hold() {
    let h = Host::new().await;
    let db = rusqlite::Connection::open(h.paths.worker.join("worker.db")).unwrap();
    ws::tests::put_attempt(&db, &CoordinatorId::generate(), 3, "active");
    drop(db);
    let mut fake = h.fake.clone();
    let error = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        OPTIONS,
        &|| 0,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("still live"), "{error:#}");
    assert!(fake.0.borrow().calls.is_empty());
    assert!(!control::held(&h.paths.worker));
    assert!(!h.exe(&h.new).exists());
}

#[tokio::test]
async fn a57_a_wait_for_work_longer_than_the_trial_window_still_reaches_health_and_commits() {
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
    let h = Host::new().await;
    let db = rusqlite::Connection::open(h.paths.worker.join("worker.db")).unwrap();
    ws::tests::put_attempt(&db, &CoordinatorId::generate(), 3, "active");
    drop(db);
    let now = std::sync::Arc::new(AtomicU64::new(1_000));
    let later = 1_000 + 10 * COMMIT_DEADLINE.as_millis() as u64;
    // The work finishes after far longer than the trial window, by this clock.
    let finisher = {
        let (now, database) = (now.clone(), h.paths.worker.join("worker.db"));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            now.store(later, SeqCst);
            rusqlite::Connection::open(database)
                .unwrap()
                .execute(
                    "UPDATE worker_attempt SET state = 'terminal', termination_json = '{}', terminal_at_ms = 2 WHERE state = 'active'",
                    [],
                )
                .unwrap();
        })
    };
    let clock = move || now.load(SeqCst);
    let mut fake = h.fake.clone();
    let mut checked = 0;
    let options = Options {
        wait: Duration::from_secs(10),
        ..OPTIONS
    };
    let upgraded = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        options,
        &clock,
        &mut |_: &Spec, launcher: &mut Fake| {
            checked += 1;
            // The guard, looking now, finds the trial within its window.
            assert!(!guard_tick(&h.paths.config, launcher, clock()).unwrap());
            Ok(())
        },
    )
    .unwrap();
    finisher.join().unwrap();
    assert_eq!(checked, 1);
    assert_eq!(upgraded.to, h.new);
    assert_eq!(h.resident_link(), Path::new(&h.new));
    h.assert_finished();
}

#[tokio::test]
async fn a57_a_hold_left_without_a_record_is_lifted_by_the_next_upgrade_of_any_build() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    // The CLI was killed while it waited: a hold, nothing else.
    control::hold(&h.paths.worker).unwrap();
    let same = h.exe(&h.old);
    let error = upgrade(&h.env, &same, &mut fake, OPTIONS, &|| 0, &mut |_, _| Ok(())).unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("already runs") && text.contains("hold") && text.contains("lifted"),
        "{text}"
    );
    assert!(!control::held(&h.paths.worker));
    assert!(fake.0.borrow().calls.is_empty());
    // Cut short after a trial marker too: the next upgrade lifts both and runs.
    control::hold(&h.paths.worker).unwrap();
    control::begin_trial(&h.paths.worker).unwrap();
    upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        OPTIONS,
        &|| 0,
        &mut |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(h.resident_link(), Path::new(&h.new));
    h.assert_finished();
}

#[tokio::test]
async fn a57_a_trial_record_whose_guard_never_ran_is_rolled_back_by_the_next_upgrade() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    // The CLI died after the record and before the guard: nothing switched.
    control::hold(&h.paths.worker).unwrap();
    private_dir_with(&upgrade_dir(&h.paths), &mut NativeDurability).unwrap();
    wsu::copy_private(&h.paths.journal, &kept(&h.paths, KEPT_JOURNAL)).unwrap();
    wsu::copy_private(&h.paths.input, &kept(&h.paths, KEPT_INPUT)).unwrap();
    let record = Record {
        schema: 1,
        from: h.old.clone(),
        to: h.new.clone(),
        deadline_ms: u64::MAX,
        backed_up: false,
        phase: Phase::Trial,
    };
    write_record(&h.paths, &record).unwrap();
    let same = h.exe(&h.old);
    let error = upgrade(&h.env, &same, &mut fake, OPTIONS, &|| 0, &mut |_, _| Ok(())).unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("already runs") && text.contains("no guard"),
        "{text}"
    );
    h.assert_old_version_back();
}

#[tokio::test]
async fn a57_a_switched_trial_whose_guard_is_gone_is_rolled_back_by_the_next_upgrade() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, &|| 0).unwrap();
    h.write_as_new_version();
    // The guard is gone and the CLI with it; the deadline is far ahead.
    fake.0.borrow_mut().loaded.remove(&h.guard);
    let same = h.exe(&h.old);
    let error = upgrade(&h.env, &same, &mut fake, OPTIONS, &|| 0, &mut |_, _| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("no guard"), "{error:#}");
    h.assert_old_version_back();
}

/// What a healthy new version `to` shows the CLI, with no attempt adopted.
fn healthy(to: &str) -> Seen {
    Seen {
        build: crate::service::workers::ResidentBuild::Running {
            binary_sha256: to.to_string(),
            features: crate::protocol::session::offered_features(),
            required_features: crate::protocol::session::required_features(),
        },
        hello: Ok(crate::protocol::session::offered_features()),
        adopted: Vec::new(),
        proof: control::TrialProof::default(),
    }
}

#[test]
fn a57_health_asks_for_the_build_its_hello_features_and_a_renewal() {
    use crate::service::workers::ResidentBuild;
    let to = "new-version";
    judged(to, &healthy(to)).unwrap();
    let refused = |seen: Seen, words: &str| {
        let error = judged(to, &seen).unwrap_err();
        assert!(format!("{error:#}").contains(words), "{error:#}");
    };
    // Another build, or one that negotiates otherwise, is not this one.
    let mut other = healthy(to);
    other.build = ResidentBuild::Running {
        binary_sha256: "old-version".into(),
        features: crate::protocol::session::offered_features(),
        required_features: crate::protocol::session::required_features(),
    };
    refused(other, "answers as build old-version");
    let mut fewer = healthy(to);
    fewer.build = ResidentBuild::Running {
        binary_sha256: to.into(),
        features: crate::protocol::session::offered_features(),
        required_features: Vec::new(),
    };
    refused(fewer, "other features");
    let mut silent = healthy(to);
    silent.build = ResidentBuild::Unknown {
        reason: "it did not answer".into(),
    };
    refused(silent, "did not say which build");
    // A hello that lacks a required feature, or is refused, is not health.
    let mut lacking = healthy(to);
    lacking.hello = Ok(crate::protocol::session::offered_features()
        .into_iter()
        .filter(|f| f.as_str() != crate::group::MEMBERSHIP_FEATURE)
        .collect());
    refused(lacking, crate::group::MEMBERSHIP_FEATURE);
    let mut refusing = healthy(to);
    refusing.hello = Err("required features missing: group_membership_v1".into());
    refused(refusing, "did not answer hello");
    // Adopted attempts: one renewed lease is health; none is not, even with
    // renewals of attempts it did not adopt.
    let mut adopted = healthy(to);
    adopted.adopted = vec!["a-1".into(), "a-2".into()];
    refused(adopted, "renewed the lease of none of the 2");
    let mut stranger = healthy(to);
    stranger.adopted = vec!["a-1".into()];
    stranger.proof.renewed = vec!["a-9".into()];
    refused(stranger, "renewed the lease of none of the 1");
    let mut renewed = healthy(to);
    renewed.adopted = vec!["a-1".into(), "a-2".into()];
    renewed.proof.renewed = vec!["a-2".into()];
    renewed.proof.unrenewed = vec!["a-1".into()];
    judged(to, &renewed).unwrap();
    // When a worker said of every one that it has no lease left (ended
    // before it was collected), there is nothing to renew.
    let mut ended = healthy(to);
    ended.adopted = vec!["a-1".into(), "a-2".into()];
    ended.proof.unrenewed = vec!["a-1".into()];
    refused(ended, "renewed the lease of none");
    let mut all_ended = healthy(to);
    all_ended.adopted = vec!["a-1".into(), "a-2".into()];
    all_ended.proof.unrenewed = vec!["a-2".into(), "a-1".into()];
    judged(to, &all_ended).unwrap();
}

/// The upgrade with a health check that judges what the new version shows,
/// the proof of renewal read from where its coordinator writes it.
fn upgrade_seeing(
    h: &Host,
    fake: &mut Fake,
    adjust: &dyn Fn(&mut Seen),
    renews: Option<&str>,
) -> anyhow::Result<Upgraded> {
    upgrade(
        &h.env,
        &h.new_exe,
        fake,
        OPTIONS,
        &|| 0,
        &mut |spec: &Spec, _: &mut Fake| {
            // The new version on trial: its coordinator notes a renewal.
            if let Some(attempt) = renews {
                control::note_renewal(&control::trial_path(&h.paths.worker), attempt)?;
            }
            let mut seen = healthy(version_of(&spec.executable));
            adjust(&mut seen);
            seen.proof = control::trial_proof(&h.paths.worker)?;
            judged(version_of(&spec.executable), &seen)
        },
    )
}

#[tokio::test]
async fn a57_a_new_version_whose_hello_lacks_a_required_feature_is_not_committed() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let error = upgrade_seeing(
        &h,
        &mut fake,
        &|seen| {
            seen.hello = Ok(crate::protocol::session::offered_features()
                .into_iter()
                .filter(|f| f.as_str() != crate::group::SHARED_ADMISSION_FEATURE)
                .collect())
        },
        None,
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("did not agree"), "{error:#}");
    h.assert_old_version_back();
}

#[tokio::test]
async fn a57_a_new_version_that_adopts_and_never_renews_is_not_committed() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let error = upgrade_seeing(
        &h,
        &mut fake,
        &|seen| seen.adopted = vec!["a-1".into()],
        None,
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("renewed the lease of none"),
        "{error:#}"
    );
    h.assert_old_version_back();
    // The proof of that trial went with it.
    assert_eq!(
        control::trial_proof(&h.paths.worker).unwrap(),
        control::TrialProof::default()
    );
}

#[tokio::test]
async fn a57_a_healthy_new_version_that_renews_what_it_adopted_is_committed() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let upgraded = upgrade_seeing(
        &h,
        &mut fake,
        &|seen| seen.adopted = vec!["a-1".into()],
        Some("a-1"),
    )
    .unwrap();
    assert_eq!(upgraded.to, h.new);
    assert_eq!(h.resident_link(), Path::new(&h.new));
    h.assert_finished();
    // The commit ends the proof with the trial.
    assert_eq!(
        control::trial_proof(&h.paths.worker).unwrap(),
        control::TrialProof::default()
    );
}

#[tokio::test]
async fn a57_ctrl_c_while_work_finishes_lifts_the_hold_and_switches_nothing() {
    let h = Host::new().await;
    let db = rusqlite::Connection::open(h.paths.worker.join("worker.db")).unwrap();
    ws::tests::put_attempt(&db, &CoordinatorId::generate(), 3, "active");
    drop(db);
    let mut fake = h.fake.clone();
    // Ctrl-C arrives during the wait for the live attempt.
    fn pressed() -> bool {
        true
    }
    let options = Options {
        wait: Duration::from_secs(600),
        interruptible: true,
        interrupted: pressed,
        ..OPTIONS
    };
    let started = std::time::Instant::now();
    let error = upgrade(
        &h.env,
        &h.new_exe,
        &mut fake,
        options,
        &|| 0,
        &mut |_, _| panic!("nothing is switched, so nothing is checked"),
    )
    .unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("interrupted while accepted work finished; nothing was switched")
            && text.contains("the upgrade hold is lifted"),
        "{text}"
    );
    // It did not wait for the work.
    assert!(started.elapsed() < Duration::from_secs(60));
    assert!(fake.0.borrow().calls.is_empty());
    assert!(!control::held(&h.paths.worker));
    assert!(!control::in_trial(&h.paths.worker));
    assert!(!record_path(&h.paths).exists());
    assert!(!h.exe(&h.new).exists());
    assert_eq!(h.resident_link(), Path::new(&h.old));
}
