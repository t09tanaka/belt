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

const OPTIONS: Options = Options {
    wait: Duration::ZERO,
    deadline: COMMIT_DEADLINE,
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
    let again = upgrade(&h.env, &h.new_exe, &mut fake, OPTIONS, &mut |_, _| Ok(())).unwrap_err();
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
    let trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, 1_000).unwrap();
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
async fn a57_rollback_never_restores_under_a_version_that_does_not_stop() {
    let h = Host::new().await;
    let mut fake = h.fake.clone();
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, 0).unwrap();
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
    let _trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, 0).unwrap();
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
    let trial = begin(&h.env, &h.new_exe, &mut fake, OPTIONS, 0).unwrap();
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

#[tokio::test]
async fn a57_a_failure_before_the_switch_lifts_the_hold_and_changes_nothing() {
    let h = Host::new().await;
    // A different file already has the new version's name.
    let directory = h.exe(&h.new).parent().unwrap().to_owned();
    fs::create_dir_all(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(h.exe(&h.new), b"something else").unwrap();
    let mut fake = h.fake.clone();
    let error = upgrade(&h.env, &h.new_exe, &mut fake, OPTIONS, &mut |_, _| Ok(())).unwrap_err();
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
    let error = upgrade(&h.env, &h.new_exe, &mut fake, OPTIONS, &mut |_, _| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("still live"), "{error:#}");
    assert!(fake.0.borrow().calls.is_empty());
    assert!(!control::held(&h.paths.worker));
    assert!(!h.exe(&h.new).exists());
}
