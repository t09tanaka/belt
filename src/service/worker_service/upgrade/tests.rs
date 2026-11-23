//! Version switches against a fake launcher and a private temporary HOME.
//! Nothing here reaches launchctl, ssh or a real service directory.
use super::super::tests::Fake;
use super::*;

/// The journal exactly as the first released helper reads it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct JournalV1 {
    schema_version: u32,
    uid: u32,
    cluster: ClusterId,
    worker_id: WorkerId,
    binary_sha256: String,
    env: BTreeMap<String, String>,
    receipt: Option<macos::Receipt>,
}

struct Host {
    _dir: tempfile::TempDir,
    env: BTreeMap<String, String>,
    old_exe: PathBuf,
    new_exe: PathBuf,
    old: String,
    new: String,
    fake: Fake,
    cluster: ClusterId,
    input_at_install: Vec<u8>,
}
const OPTIONS: Options = Options {
    cancel_running: false,
    all_submitters: false,
    submitter: None,
    wait: Duration::ZERO,
    ready_timeout: Duration::from_secs(2),
};
fn failing(at: Step) -> impl FnMut(Step) -> anyhow::Result<()> {
    move |step| {
        ensure!(step != at, "injected failure at {step:?}");
        Ok(())
    }
}
impl Host {
    /// A host with the old version installed, started and bound.
    fn new() -> Self {
        Self::on(policy::Backend::Launchd)
    }
    fn on(backend: policy::Backend) -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path().to_owned();
        let home = root.join("h");
        fs::create_dir(&home).unwrap();
        let (old_exe, new_exe) = (root.join("belt-old"), root.join("belt-new"));
        fs::write(&old_exe, b"old version").unwrap();
        fs::write(&new_exe, b"new version").unwrap();
        let env = BTreeMap::from([
            ("HOME".to_string(), home.to_str().unwrap().to_string()),
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ]);
        let fake = Fake::default();
        fake.0.borrow_mut().state = home.join(".belt");
        fake.0.borrow_mut().backend = backend;
        let cluster = ClusterId::generate();
        let old = sha256_file(&old_exe).unwrap();
        let request = InstallRequest {
            membership: None,
            system_user: false,
            schema: 1,
            cluster: cluster.clone(),
            worker_id: WorkerId::parse("mini-a").unwrap(),
            key_file_text: format!("{}\n", "ab".repeat(64)),
            binary_sha256: old.clone(),
        };
        install(
            &env,
            &request,
            &old_exe,
            fake.clone(),
            OPTIONS.ready_timeout,
        )
        .unwrap();
        let paths = Paths::resolve_for(&env, backend).unwrap();
        let binding = paths.state.join("worker-binding.json");
        fs::write(
            &binding,
            serde_json::to_vec(&(1, &cluster, &request.worker_id, "key-digest")).unwrap(),
        )
        .unwrap();
        fs::set_permissions(&binding, fs::Permissions::from_mode(0o600)).unwrap();
        Host {
            input_at_install: fs::read(&paths.input).unwrap(),
            new: sha256_file(&new_exe).unwrap(),
            _dir: dir,
            env,
            old_exe,
            new_exe,
            old,
            fake,
            cluster,
        }
    }
    fn paths(&self) -> Paths {
        Paths::resolve_for(&self.env, self.fake.backend()).unwrap()
    }
    fn request(&self, digest: &str) -> SwitchRequest {
        SwitchRequest {
            schema: 1,
            cluster: self.cluster.clone(),
            worker_id: WorkerId::parse("mini-a").unwrap(),
            binary_sha256: digest.to_string(),
        }
    }
    fn upgrade(&self, hook: Hook) -> anyhow::Result<Status> {
        upgrade(
            &self.env,
            &self.request(&self.new),
            &self.new_exe,
            self.fake.clone(),
            OPTIONS,
            hook,
        )
    }
    fn rollback(&self, hook: Hook) -> anyhow::Result<Status> {
        rollback(
            &self.env,
            &self.request(&self.old),
            self.fake.clone(),
            OPTIONS,
            hook,
        )
    }
    fn commit(&self, digest: &str) -> anyhow::Result<Status> {
        commit(&self.env, &self.request(digest), self.fake.clone())
    }
    fn status(&self) -> Status {
        status(&self.env, self.fake.clone()).unwrap()
    }
    fn journal(&self) -> Vec<u8> {
        fs::read(self.paths().journal).unwrap()
    }
    /// A real worker database with one marker table, as the old version left it.
    fn database(&self) -> PathBuf {
        let paths = self.paths();
        drop(
            crate::storage::WorkerStore::open(&paths.state, &WorkerId::parse("mini-a").unwrap())
                .unwrap(),
        );
        let path = paths.state.join("worker.db");
        sql(&path, "CREATE TABLE left_by_old (x)");
        path
    }
    fn launchctl(&self) -> Vec<String> {
        let world = self.fake.0.borrow();
        world.calls.iter().map(|c| c[0].clone()).collect()
    }
    fn current(&self) -> PathBuf {
        fs::read_link(self.paths().current_link()).unwrap()
    }
    fn input(&self) -> super::super::super::worker::WorkerSpec {
        super::super::super::worker::read_input(&self.paths().input).unwrap()
    }
    /// Once the parent has committed, the host serves `digest`: journal, link,
    /// input and service agree, the journal is at rest in its first shape,
    /// and no version binary changed (the previous one may have been removed).
    fn assert_serving(&self, digest: &str) {
        let paths = self.paths();
        self.commit(digest).unwrap();
        let status = self.status();
        assert!(!status.drained_by_upgrade && !status.switch_running);
        assert_eq!(status.binary_sha256, digest);
        assert_eq!(status.loaded, Some(true));
        assert!(status.ready);
        assert_eq!(status.switching_from, None);
        assert_eq!(self.current(), Path::new(digest));
        assert_eq!(self.input().executable, paths.executable(digest));
        let journal: JournalV1 =
            serde_json::from_slice(&fs::read(&paths.journal).unwrap()).unwrap();
        assert_eq!(journal.binary_sha256, digest);
        let receipt = journal.receipt.expect("a service file is registered");
        let plist = fs::read_to_string(receipt.path()).unwrap();
        assert!(plist.contains(paths.executable(digest).to_str().unwrap()));
        // The previous version is gone once the commit has looked (spec
        // 25.7 (a)), or, when it is still there or is the one served, intact.
        match fs::read(paths.executable(&self.old)) {
            Ok(bytes) => assert_eq!(bytes, b"old version"),
            Err(_) => assert_ne!(digest, self.old, "the served version is there"),
        }
        assert_eq!(
            status.bridge_binary.as_deref(),
            paths.bridge_binary().to_str()
        );
    }
}

fn sql(database: &Path, statement: &str) {
    rusqlite::Connection::open(database)
        .unwrap()
        .execute_batch(statement)
        .unwrap();
}
fn tables(database: &Path) -> Vec<String> {
    let connection =
        rusqlite::Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE name LIKE '%_by_%' ORDER BY name")
        .unwrap();
    let names = statement.query_map([], |row| row.get(0)).unwrap();
    names.map(Result::unwrap).collect()
}
/// The steps an upgrade of a host without a recorded switch passes.
fn upgrade_steps() -> impl Iterator<Item = Step> {
    STEPS.into_iter().filter(|step| *step != Step::Restore)
}

#[test]
fn a_digest_is_sixty_four_lower_case_hex_digits() {
    assert!(is_digest(&"0123456789abcdef".repeat(4)));
    assert!(!is_digest(&"a".repeat(63)));
    assert!(!is_digest(&"a".repeat(65)));
    assert!(!is_digest(&"A".repeat(64)));
    assert!(!is_digest(&"g".repeat(64)));
    assert!(!is_digest(&format!("{}/", "a".repeat(63))));
    assert!(!is_digest(&format!("{}`", "a".repeat(63))));
    assert!(!is_digest(""));
}

#[test]
fn upgrade_places_the_version_stops_the_old_service_and_starts_the_new_one() {
    let h = Host::new();
    let status = h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(status.binary_sha256, h.new);
    // Before the commit the previous version keeps the exact input it ran with.
    let paths = h.paths();
    assert_eq!(
        fs::read(version_input(&paths, &h.old)).unwrap(),
        h.input_at_install
    );
    h.assert_serving(&h.new);
    assert_eq!(h.launchctl(), ["bootstrap", "bootout", "bootstrap"]);
    // The worker stays drained until the caller has checked it and resumes.
    assert!(status.draining && !status.cancel_running);
    assert_eq!(fs::read(paths.executable(&h.new)).unwrap(), b"new version");
    // The commit removed the previous version (spec 25.7 (a)).
    assert!(!version_input(&paths, &h.old).exists());
}

#[test]
fn upgrade_to_the_installed_version_changes_nothing() {
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    let before = (h.launchctl(), fs::read(h.paths().journal).unwrap());
    let status = h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(status.binary_sha256, h.new);
    assert_eq!(
        (h.launchctl(), fs::read(h.paths().journal).unwrap()),
        before
    );
}

#[test]
fn rollback_returns_to_the_previous_version_with_its_own_input() {
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    let upgraded_input = fs::read(h.paths().input).unwrap();
    h.rollback(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.old);
    assert_eq!(fs::read(h.paths().input).unwrap(), h.input_at_install);
    // And forward again with the input the new version ran with.
    h.upgrade(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.new);
    assert_eq!(fs::read(h.paths().input).unwrap(), upgraded_input);
}

#[test]
fn an_upgrade_that_fails_at_any_step_can_be_rolled_back() {
    for step in upgrade_steps() {
        let h = Host::new();
        let error = h.upgrade(&mut failing(step)).unwrap_err();
        assert!(format!("{error:#}").contains("injected"), "{step:?}");
        h.rollback(&mut |_| Ok(())).unwrap();
        h.assert_serving(&h.old);
        assert_eq!(
            fs::read(h.paths().input).unwrap(),
            h.input_at_install,
            "{step:?}"
        );
    }
}

#[test]
fn an_upgrade_that_stopped_at_any_step_is_finished_by_running_it_again() {
    for step in upgrade_steps() {
        let h = Host::new();
        h.upgrade(&mut failing(step)).unwrap_err();
        h.upgrade(&mut |_| Ok(())).unwrap();
        h.assert_serving(&h.new);
    }
}

#[test]
fn a_rollback_that_fails_reports_the_host_as_it_is_and_can_be_run_again() {
    for failed in [Step::Publish, Step::Settle] {
        for step in STEPS {
            let h = Host::new();
            h.upgrade(&mut failing(failed)).unwrap_err();
            let between = h.status();
            if h.rollback(&mut failing(step)).is_ok() {
                // The step was not part of this rollback.
                h.assert_serving(&h.old);
                continue;
            }
            let status = h.status();
            // Never a served version without a loaded, ready service.
            if status.switching_from.is_none() && status.loaded == Some(true) {
                assert!(
                    status.binary_sha256 == between.binary_sha256 || status.binary_sha256 == h.old
                );
            }
            if status.loaded != Some(true) {
                assert!(!status.ready, "{failed:?} {step:?}");
            }
            h.rollback(&mut |_| Ok(())).unwrap();
            h.assert_serving(&h.old);
        }
    }
}

#[test]
fn a_host_between_versions_says_so_and_claims_no_service() {
    let h = Host::new();
    h.upgrade(&mut failing(Step::Publish)).unwrap_err();
    let status = h.status();
    assert_eq!(status.switching_from.as_deref(), Some(h.old.as_str()));
    assert_eq!(status.binary_sha256, h.new);
    assert_eq!(status.loaded, None);
    assert!(!status.ready && status.draining);
    assert_eq!(status.bridge_binary, None);
    // The lifecycle commands do not pretend the service exists.
    assert!(start(&h.env, h.fake.clone(), OPTIONS.ready_timeout).is_err());
    assert_eq!(h.launchctl(), ["bootstrap", "bootout"]);
}

#[test]
fn a_recorded_service_file_that_was_never_published_is_published_again() {
    let h = Host::new();
    h.upgrade(&mut failing(Step::Start)).unwrap_err();
    let journal = read_journal(&h.paths()).unwrap().unwrap();
    fs::remove_file(journal.receipt.unwrap().path()).unwrap();
    h.upgrade(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.new);
}

#[test]
fn a_service_file_that_is_missing_without_a_switch_is_not_replaced() {
    let h = Host::new();
    let journal = read_journal(&h.paths()).unwrap().unwrap();
    fs::remove_file(journal.receipt.unwrap().path()).unwrap();
    assert!(h.upgrade(&mut |_| Ok(())).is_err());
    assert_eq!(h.status().binary_sha256, h.old);
}

#[test]
fn the_old_service_file_is_removed_only_while_it_is_still_ours() {
    let h = Host::new();
    let old_receipt = read_journal(&h.paths()).unwrap().unwrap().receipt.unwrap();
    h.upgrade(&mut failing(Step::RemoveOld)).unwrap_err();
    fs::write(old_receipt.path(), b"someone else's file").unwrap();
    let error = h.upgrade(&mut |_| Ok(())).unwrap_err();
    assert!(
        format!("{error:#}").contains("refusing removal"),
        "{error:#}"
    );
    assert_eq!(
        fs::read(old_receipt.path()).unwrap(),
        b"someone else's file"
    );
    assert_eq!(h.status().switching_from.as_deref(), Some(h.old.as_str()));
}

#[test]
fn upgrade_refuses_another_identity_another_binary_and_an_unknown_request() {
    let h = Host::new();
    let run = |request: &SwitchRequest| {
        upgrade(
            &h.env,
            request,
            &h.new_exe,
            h.fake.clone(),
            OPTIONS,
            &mut |_| Ok(()),
        )
    };
    let mut request = h.request(&h.new);
    request.schema = 2;
    assert!(run(&request).is_err());
    let mut request = h.request(&h.new);
    request.cluster = ClusterId::generate();
    assert!(run(&request).is_err());
    let mut request = h.request(&h.new);
    request.worker_id = WorkerId::parse("mini-b").unwrap();
    assert!(run(&request).is_err());
    // The parent named another binary than the one that runs.
    assert!(run(&h.request(&h.old)).is_err());
    assert!(run(&h.request("../../../x")).is_err());
    h.assert_serving(&h.old);
    assert_eq!(h.launchctl(), ["bootstrap"]);
    assert!(absent(&h.paths().executable(&h.new)).unwrap());
}

#[test]
fn rollback_refuses_a_version_that_is_absent_or_no_longer_its_bytes() {
    let h = Host::new();
    let to = |digest: &str| {
        rollback(
            &h.env,
            &h.request(digest),
            h.fake.clone(),
            OPTIONS,
            &mut |_| Ok(()),
        )
    };
    // Nothing was switched, so there is nothing to return to.
    let error = to(&h.new).unwrap_err();
    assert!(format!("{error:#}").contains("no uncommitted switch"));
    h.upgrade(&mut |_| Ok(())).unwrap();
    fs::write(h.paths().executable(&h.old), b"tampered").unwrap();
    let error = to(&h.old).unwrap_err();
    assert!(format!("{error:#}").contains("differs from its name"));
    fs::remove_file(h.paths().executable(&h.old)).unwrap();
    let error = to(&h.old).unwrap_err();
    assert!(format!("{error:#}").contains("not installed"));
    assert_eq!(h.status().binary_sha256, h.new);
    assert_eq!(h.launchctl(), ["bootstrap", "bootout", "bootstrap"]);
}

#[test]
fn an_unreadable_database_or_a_foreign_binding_fails_before_anything_stops() {
    let h = Host::new();
    let paths = h.paths();
    fs::write(paths.state.join("worker.db"), b"not a database").unwrap();
    let error = h.upgrade(&mut |_| Ok(())).unwrap_err();
    assert!(
        format!("{error:#}").contains("cannot read the worker database"),
        "{error:#}"
    );
    fs::remove_file(paths.state.join("worker.db")).unwrap();
    let binding = paths.state.join("worker-binding.json");
    fs::write(
        &binding,
        serde_json::to_vec(&(1, ClusterId::generate(), "mini-a", "k")).unwrap(),
    )
    .unwrap();
    // The drain of the first attempt is the upgrade's and is said to be.
    let status = h.status();
    assert!(status.draining && status.drained_by_upgrade);
    let before = h.journal();
    assert!(h.upgrade(&mut |_| Ok(())).is_err());
    // A state that is bound to another identity is not touched at all.
    assert_eq!(h.journal(), before);
    // The old service was never touched, and the copy the version check
    // refused is gone (spec 25.7 (a)).
    assert_eq!(h.launchctl(), ["bootstrap"]);
    assert_eq!(h.current(), Path::new(&h.old));
    assert!(!paths.executable(&h.new).exists());
}

#[test]
fn an_input_of_another_worker_is_incompatible() {
    let h = Host::new();
    let paths = h.paths();
    let mut spec = h.input();
    spec.worker_id = WorkerId::parse("mini-b").unwrap();
    fs::write(&paths.input, serde_json::to_vec(&spec).unwrap()).unwrap();
    let error = h.upgrade(&mut |_| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("another worker"), "{error:#}");
    let mut spec = h.input();
    spec.worker_id = WorkerId::parse("mini-a").unwrap();
    spec.state_dir = paths.home.join("elsewhere");
    fs::write(&paths.input, serde_json::to_vec(&spec).unwrap()).unwrap();
    assert!(h.upgrade(&mut |_| Ok(())).is_err());
    assert_eq!(h.launchctl(), ["bootstrap"]);
}

#[test]
fn an_unknown_loaded_state_stops_and_switches_nothing() {
    let h = Host::new();
    h.fake.0.borrow_mut().unknown = true;
    let error = h.upgrade(&mut |_| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("unknown"), "{error:#}");
    h.fake.0.borrow_mut().unknown = false;
    h.assert_serving(&h.old);
    assert_eq!(h.launchctl(), ["bootstrap"]);
}

#[test]
fn a_service_that_was_already_stopped_is_switched_without_a_bootout() {
    let h = Host::new();
    stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    h.upgrade(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.new);
    assert_eq!(h.launchctl(), ["bootstrap", "bootout", "bootstrap"]);
}

#[test]
fn a_rejected_start_leaves_the_switch_open_until_it_is_finished_or_undone() {
    let h = Host::new();
    h.fake.0.borrow_mut().fail_start = true;
    assert!(h.upgrade(&mut |_| Ok(())).is_err());
    let status = h.status();
    assert_eq!(status.switching_from.as_deref(), Some(h.old.as_str()));
    assert_eq!(status.loaded, Some(false));
    assert!(!status.ready);
    h.fake.0.borrow_mut().fail_start = false;
    h.rollback(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.old);
}

#[test]
fn the_current_link_moves_by_rename_and_is_left_alone_when_right() {
    let h = Host::new();
    let paths = h.paths();
    let link = paths.current_link();
    let before = fs::symlink_metadata(&link).unwrap().modified().unwrap();
    point_current(&paths, &h.old).unwrap();
    assert_eq!(
        fs::symlink_metadata(&link).unwrap().modified().unwrap(),
        before
    );
    // A stale temporary link of an earlier run does not block the move.
    let stale = link
        .parent()
        .unwrap()
        .join(crate::platform::owned_temp::testing::dead_name(
            "dddddddddddd",
        ));
    std::os::unix::fs::symlink("stale", &stale).unwrap();
    point_current(&paths, &h.new).unwrap();
    assert_eq!(h.current(), Path::new(&h.new));
    assert!(absent(&stale).unwrap());
}

#[test]
fn a_kept_input_that_names_another_version_is_refused() {
    let h = Host::new();
    let paths = h.paths();
    fs::create_dir(paths.config.join("bin").join(&h.new)).unwrap();
    let kept = version_input(&paths, &h.new);
    fs::write(&kept, &h.input_at_install).unwrap();
    fs::set_permissions(&kept, fs::Permissions::from_mode(0o600)).unwrap();
    let error = input_for(&paths, &h.new).unwrap_err();
    assert!(format!("{error:#}").contains("another version"));
    assert_eq!(input_for(&paths, &h.old).unwrap(), h.input_at_install);
}

#[test]
fn a_request_with_an_unknown_field_is_refused_by_the_helper() {
    let h = Host::new();
    let mut value = serde_json::to_value(h.request(&h.new)).unwrap();
    let run = |action, body: &serde_json::Value| {
        handle(
            action,
            &h.env,
            &serde_json::to_vec(body).unwrap(),
            &h.new_exe,
            h.fake.clone(),
            OPTIONS,
        )
    };
    assert_eq!(run(Action::Upgrade, &value).unwrap().binary_sha256, h.new);
    value["binary_sha256"] = h.old.clone().into();
    assert_eq!(run(Action::Rollback, &value).unwrap().binary_sha256, h.old);
    value["extra"] = true.into();
    assert!(run(Action::Upgrade, &value).is_err());
    assert!(run(Action::Rollback, &value).is_err());
    let _ = &h.old_exe;
}

#[test]
fn a_stop_that_waited_on_live_attempts_says_nothing_was_switched() {
    let original = || anyhow::anyhow!("the original reason");
    let text = |live| stop_failure(live, original()).to_string();
    assert_eq!(
        text(Some(2)),
        "2 attempt(s) are still live after waiting; nothing was switched"
    );
    assert_eq!(text(Some(1)).split(' ').next(), Some("1"));
    // Idle or unreadable: the stop failed for its own reason.
    assert_eq!(text(Some(0)), "the original reason");
    assert_eq!(text(None), "the original reason");
}

// --- the switch is kept until the parent commits (H1, M1) -------------------

#[test]
fn a_started_version_stays_revocable_until_the_parent_commits() {
    let h = Host::new();
    let status = h.upgrade(&mut |_| Ok(())).unwrap();
    // The new version runs, and the host still knows where it came from.
    assert_eq!(status.loaded, Some(true));
    assert!(status.ready);
    assert_eq!(status.switching_from.as_deref(), Some(h.old.as_str()));
    assert_eq!(h.status().switching_from.as_deref(), Some(h.old.as_str()));
    // A parent that died here and came back can still return to the old one.
    h.rollback(&mut |_| Ok(())).unwrap();
    let status = h.status();
    assert_eq!(status.binary_sha256, h.old);
    assert_eq!(status.switching_from, None);
    assert_eq!(status.loaded, Some(true));
    h.assert_serving(&h.old);
}

#[test]
fn commit_forgets_the_previous_version_and_nothing_can_be_rolled_back_after_it() {
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    let status = h.commit(&h.new).unwrap();
    assert_eq!(status.switching_from, None);
    assert_eq!((status.loaded, status.ready), (Some(true), true));
    assert!(!status.draining && !status.drained_by_upgrade);
    let at_rest = h.journal();
    let _: JournalV1 = serde_json::from_slice(&at_rest).unwrap();
    // Again: nothing more changes.
    h.commit(&h.new).unwrap();
    assert_eq!(h.journal(), at_rest);
    let error = h.rollback(&mut |_| Ok(())).unwrap_err();
    assert!(
        format!("{error:#}").contains("a committed upgrade cannot be rolled back"),
        "{error:#}"
    );
    assert_eq!(h.journal(), at_rest);
    assert_eq!(h.launchctl(), ["bootstrap", "bootout", "bootstrap"]);
}

#[test]
fn commit_names_the_installed_version_and_needs_a_finished_switch() {
    let h = Host::new();
    h.upgrade(&mut failing(Step::Publish)).unwrap_err();
    let before = h.journal();
    let error = h.commit(&h.new).unwrap_err();
    assert!(format!("{error:#}").contains("unfinished"), "{error:#}");
    let error = h.commit(&h.old).unwrap_err();
    assert!(format!("{error:#}").contains("not the one to commit"));
    let mut foreign = h.request(&h.new);
    foreign.cluster = ClusterId::generate();
    assert!(commit(&h.env, &foreign, h.fake.clone()).is_err());
    assert_eq!(h.journal(), before);
    assert!(h.status().draining);
}

#[test]
fn a_drain_the_upgrade_made_is_recorded_from_its_first_step_and_resumed_by_commit() {
    for step in upgrade_steps().filter(|step| *step != Step::Drain) {
        let h = Host::new();
        h.upgrade(&mut failing(step)).unwrap_err();
        let status = h.status();
        assert!(status.draining && status.drained_by_upgrade, "{step:?}");
        // The next run finds its own drain and the commit ends it.
        h.upgrade(&mut |_| Ok(())).unwrap();
        assert!(h.status().drained_by_upgrade, "{step:?}");
        let status = h.commit(&h.new).unwrap();
        assert!(!status.draining && !status.drained_by_upgrade, "{step:?}");
        assert!(!Control::effective(&h.paths().state).drain);
    }
    // Before the drain step nothing is drained or recorded.
    let h = Host::new();
    let before = h.journal();
    h.upgrade(&mut failing(Step::Drain)).unwrap_err();
    assert_eq!(h.journal(), before);
    assert!(!h.status().draining);
}

#[test]
fn a_drain_the_operator_made_stays_theirs_through_upgrade_and_commit() {
    let h = Host::new();
    drain(&h.env, h.fake.clone()).unwrap();
    let status = h.upgrade(&mut |_| Ok(())).unwrap();
    assert!(status.draining && !status.drained_by_upgrade);
    let status = h.commit(&h.new).unwrap();
    assert!(status.draining && !status.drained_by_upgrade);
    assert_eq!(status.switching_from, None);
    let _: JournalV1 = serde_json::from_slice(&h.journal()).unwrap();
}

#[test]
fn a_recorded_drain_whose_marker_was_never_written_is_written_by_the_next_run() {
    let h = Host::new();
    h.upgrade(&mut failing(Step::Place)).unwrap_err();
    // As if the first run had died between the journal and the marker.
    control::write(&h.paths().state, Control::default()).unwrap();
    h.upgrade(&mut failing(Step::Place)).unwrap_err();
    let status = h.status();
    assert!(status.draining && status.drained_by_upgrade);
}

#[test]
fn the_operators_drain_and_resume_take_the_drain_over_from_the_upgrade() {
    let h = Host::new();
    h.upgrade(&mut failing(Step::Compat)).unwrap_err();
    assert!(h.status().drained_by_upgrade);
    let status = drain(&h.env, h.fake.clone()).unwrap();
    assert!(status.draining && !status.drained_by_upgrade);
    // The upgrade no longer resumes what the operator asked for.
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert!(h.commit(&h.new).unwrap().draining);

    let h = Host::new();
    h.upgrade(&mut failing(Step::Compat)).unwrap_err();
    let status = resume(&h.env, h.fake.clone()).unwrap();
    assert!(!status.draining && !status.drained_by_upgrade);
    let _: JournalV1 = serde_json::from_slice(&h.journal()).unwrap();
}

#[test]
fn resume_is_refused_while_a_switch_awaits_its_commit() {
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    let before = h.journal();
    let error = resume(&h.env, h.fake.clone()).unwrap_err();
    assert!(
        format!("{error:#}").contains("awaits its commit"),
        "{error:#}"
    );
    assert_eq!(h.journal(), before);
    assert!(h.status().draining);
    // Draining again is harmless and makes the drain the operator's.
    assert!(drain(&h.env, h.fake.clone()).unwrap().draining);
    h.commit(&h.new).unwrap();
    assert!(!resume(&h.env, h.fake.clone()).unwrap().draining);
}

// --- the worker database is saved and put back (M3) -------------------------

#[test]
fn rollback_before_commit_puts_the_saved_database_back_before_the_old_version_starts() {
    let h = Host::new();
    let database = h.database();
    let saved = saved_database(&h.paths(), &h.old);
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(tables(&saved), ["left_by_old"]);
    assert_eq!(
        fs::metadata(&saved).unwrap().permissions().mode() & 0o777,
        0o600
    );
    // The new version migrates; an idle connection keeps its log file alive.
    let held = rusqlite::Connection::open(&database).unwrap();
    held.execute_batch("CREATE TABLE made_by_new (x)").unwrap();
    assert!(
        fs::metadata(h.paths().state.join("worker.db-wal"))
            .unwrap()
            .len()
            > 0
    );
    let mut seen_at_start = Vec::new();
    h.rollback(&mut |step| {
        if step == Step::Start {
            seen_at_start = tables(&database);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(seen_at_start, ["left_by_old"]);
    assert_eq!(tables(&database), ["left_by_old"]);
    // The old version reads it, and the copy is gone with the switch.
    crate::storage::WorkerStore::open_read_only(&h.paths().state).unwrap();
    assert!(absent(&saved).unwrap());
    h.assert_serving(&h.old);
}

#[test]
fn the_saved_database_holds_what_was_still_in_the_log() {
    let h = Host::new();
    let database = h.database();
    // Written but not yet moved into the database file.
    let held = rusqlite::Connection::open(&database).unwrap();
    held.execute_batch("CREATE TABLE logged_by_old (x)")
        .unwrap();
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(
        tables(&saved_database(&h.paths(), &h.old)),
        ["left_by_old", "logged_by_old"]
    );
}

#[test]
fn the_saved_database_is_never_replaced_once_the_switch_is_recorded() {
    let h = Host::new();
    let database = h.database();
    h.fake.0.borrow_mut().fail_start = true;
    h.upgrade(&mut |_| Ok(())).unwrap_err();
    h.fake.0.borrow_mut().fail_start = false;
    sql(&database, "CREATE TABLE made_by_new (x)");
    // Forward again, half back, forward again: the copy stays the old one's.
    h.upgrade(&mut |_| Ok(())).unwrap();
    h.rollback(&mut failing(Step::Settle)).unwrap_err();
    sql(&database, "CREATE TABLE made_by_old_later (x)");
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(tables(&saved_database(&h.paths(), &h.old)), ["left_by_old"]);
    assert_eq!(tables(&database), ["left_by_old", "made_by_old_later"]);
    h.rollback(&mut |_| Ok(())).unwrap();
    assert_eq!(tables(&database), ["left_by_old"]);
}

#[test]
fn a_rollback_that_died_after_the_old_version_started_does_not_replace_its_database() {
    let h = Host::new();
    let database = h.database();
    h.upgrade(&mut |_| Ok(())).unwrap();
    h.rollback(&mut failing(Step::Settle)).unwrap_err();
    assert_eq!(h.status().loaded, Some(true));
    sql(&database, "CREATE TABLE made_by_old_later (x)");
    let mut steps = Vec::new();
    h.rollback(&mut |step| {
        steps.push(step);
        Ok(())
    })
    .unwrap();
    assert!(!steps.contains(&Step::Restore), "{steps:?}");
    assert_eq!(tables(&database), ["left_by_old", "made_by_old_later"]);
}

#[test]
fn a_restore_that_failed_is_done_again_by_the_next_rollback() {
    let h = Host::new();
    let database = h.database();
    h.upgrade(&mut |_| Ok(())).unwrap();
    sql(&database, "CREATE TABLE made_by_new (x)");
    for step in [Step::Restore, Step::Input, Step::Publish] {
        h.rollback(&mut failing(step)).unwrap_err();
        // Nothing of either version runs while the database is replaced.
        assert_eq!(h.status().loaded, None, "{step:?}");
    }
    h.rollback(&mut |_| Ok(())).unwrap();
    assert_eq!(tables(&database), ["left_by_old"]);
    h.assert_serving(&h.old);
}

#[test]
fn commit_deletes_the_saved_database() {
    let h = Host::new();
    h.database();
    let saved = saved_database(&h.paths(), &h.old);
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert!(!absent(&saved).unwrap());
    h.commit(&h.new).unwrap();
    assert!(absent(&saved).unwrap());
}

#[test]
fn a_recorded_switch_whose_saved_database_is_gone_refuses_to_roll_back() {
    let h = Host::new();
    let database = h.database();
    h.upgrade(&mut |_| Ok(())).unwrap();
    sql(&database, "CREATE TABLE made_by_new (x)");
    fs::remove_file(saved_database(&h.paths(), &h.old)).unwrap();
    let error = h.rollback(&mut |_| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("is gone"), "{error:#}");
    // The newer database was left as it is; going forward still works.
    assert_eq!(tables(&database), ["left_by_old", "made_by_new"]);
    h.upgrade(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.new);
}

#[test]
fn a_host_without_a_database_saves_none_and_drops_a_stale_copy() {
    let h = Host::new();
    let paths = h.paths();
    let saved = saved_database(&paths, &h.old);
    fs::write(&saved, b"left by an attempt that was never recorded").unwrap();
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert!(absent(&saved).unwrap());
    let journal = read_journal(&paths).unwrap().unwrap();
    assert!(!journal.switch.unwrap().database_saved);
    let mut steps = Vec::new();
    h.rollback(&mut |step| {
        steps.push(step);
        Ok(())
    })
    .unwrap();
    assert!(!steps.contains(&Step::Restore));
    h.assert_serving(&h.old);
}

#[test]
fn an_attempt_that_was_never_recorded_leaves_a_copy_the_next_one_replaces() {
    let h = Host::new();
    let database = h.database();
    h.upgrade(&mut failing(Step::Record)).unwrap_err();
    // The old version was started again and wrote more.
    h.rollback(&mut |_| Ok(())).unwrap();
    assert_eq!(h.status().loaded, Some(true));
    sql(&database, "CREATE TABLE more_by_old (x)");
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(
        tables(&saved_database(&h.paths(), &h.old)),
        ["left_by_old", "more_by_old"]
    );
}

#[test]
fn a_database_that_is_in_use_is_neither_copied_nor_replaced() {
    let h = Host::new();
    let database = h.database();
    checkpoint(&database).unwrap();
    // A reader that pins the log while more is written to it.
    let reader = rusqlite::Connection::open(&database).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader
        .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
        .unwrap();
    let writer = rusqlite::Connection::open(&database).unwrap();
    writer
        .execute_batch("CREATE TABLE more_by_old (x)")
        .unwrap();
    let error = checkpoint(&database).unwrap_err();
    assert!(format!("{error:#}").contains("in use"), "{error:#}");
    let error = h.upgrade(&mut |_| Ok(())).unwrap_err();
    assert!(format!("{error:#}").contains("in use"), "{error:#}");
    assert_eq!(h.status().switching_from, None);
    drop(reader);
    // Not a file: nothing is opened through a link.
    let elsewhere = h.paths().home.join("elsewhere.db");
    fs::rename(&database, &elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &database).unwrap();
    assert!(checkpoint(&database).is_err());
}

// --- one switch at a time (M2) ----------------------------------------------

#[test]
fn every_changing_action_is_refused_while_another_helper_holds_the_switch_lock() {
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    let before = (h.journal(), h.launchctl());
    let held = switch_lock(&h.paths(), true, Duration::ZERO)
        .unwrap()
        .expect("the lock is free");
    let refused = [
        h.upgrade(&mut |_| Ok(())),
        h.rollback(&mut |_| Ok(())),
        h.commit(&h.new),
        start(&h.env, h.fake.clone(), Duration::ZERO),
        stop(&h.env, h.fake.clone(), false, Duration::ZERO),
        drain(&h.env, h.fake.clone()),
        resume(&h.env, h.fake.clone()),
    ];
    for result in refused {
        let error = result.unwrap_err();
        assert!(
            format!("{error:#}").contains("a version switch is running"),
            "{error:#}"
        );
    }
    assert_eq!((h.journal(), h.launchctl()), before);
    // Status still answers, and says what is going on.
    let status = h.status();
    assert!(status.switch_running);
    assert_eq!(status.binary_sha256, h.new);
    drop(held);
    assert!(!h.status().switch_running);
    h.rollback(&mut |_| Ok(())).unwrap();
    h.assert_serving(&h.old);
}

#[test]
fn a_running_switch_holds_the_lock_from_its_first_step_to_its_last() {
    let h = Host::new();
    let mut seen = Vec::new();
    h.upgrade(&mut |step| {
        let status = h.status();
        let second = h.rollback(&mut |_| Ok(())).unwrap_err();
        seen.push((
            step,
            status.switch_running,
            format!("{second:#}").contains("a version switch is running"),
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(seen.len(), STEPS.len() - 1);
    assert!(seen
        .iter()
        .all(|(_, running, refused)| *running && *refused));
    // And lets go of it when it returns, also by an error.
    assert!(!h.status().switch_running);
    h.rollback(&mut failing(Step::Stop)).unwrap_err();
    assert!(!h.status().switch_running);
}

#[test]
fn two_statuses_at_once_do_not_take_each_other_for_a_switch() {
    let h = Host::new();
    let paths = h.paths();
    let reader = switch_lock(&paths, false, Duration::ZERO).unwrap();
    assert!(reader.is_some());
    assert!(!h.status().switch_running);
    // A writer waits a little for a reader, then gives up without an error.
    let started = Instant::now();
    assert!(switch_lock(&paths, true, Duration::from_millis(40))
        .unwrap()
        .is_none());
    assert!(started.elapsed() >= Duration::from_millis(40));
    drop(reader);
    assert!(switch_lock(&paths, true, Duration::ZERO).unwrap().is_some());
    let mode = fs::metadata(paths.config.join("worker-service.lock"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn the_helper_reads_a_commit_request_and_refuses_an_unknown_field() {
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    let mut value = serde_json::to_value(h.request(&h.new)).unwrap();
    let run = |body: &serde_json::Value| {
        handle(
            Action::Commit,
            &h.env,
            &serde_json::to_vec(body).unwrap(),
            &h.new_exe,
            h.fake.clone(),
            OPTIONS,
        )
    };
    value["extra"] = true.into();
    assert!(run(&value).is_err());
    assert!(h.status().switching_from.is_some());
    value.as_object_mut().unwrap().remove("extra");
    assert_eq!(run(&value).unwrap().switching_from, None);
}

#[test]
fn a_status_at_rest_keeps_the_shape_the_first_release_reads() {
    let h = Host::new();
    let value = serde_json::to_value(h.status()).unwrap();
    let mut keys: Vec<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "binary_sha256",
            "bridge_binary",
            "cancel_running",
            "cluster",
            "draining",
            "live_attempts",
            "loaded",
            "ready",
            // Added with the shared task-traffic-controller scope; an older
            // reader ignores it (`Status` does not deny unknown fields).
            "traffic_scope",
            "worker_id"
        ]
    );
    h.upgrade(&mut |_| Ok(())).unwrap();
    let value = serde_json::to_value(h.status()).unwrap();
    assert_eq!(value["drained_by_upgrade"], true);
    assert_eq!(value["switching_from"], h.old.as_str());
    assert!(value.get("switch_running").is_none());
}

#[test]
fn a_stop_and_start_before_the_commit_do_not_end_the_drain() {
    let restart = |h: &Host, cancel_running| {
        stop(&h.env, h.fake.clone(), cancel_running, Duration::ZERO).unwrap();
        start(&h.env, h.fake.clone(), OPTIONS.ready_timeout).unwrap()
    };
    // The switch awaits its commit and the upgrade owns the drain.
    let h = Host::new();
    h.upgrade(&mut |_| Ok(())).unwrap();
    for cancel_running in [false, true] {
        let status = restart(&h, cancel_running);
        assert_eq!(status.loaded, Some(true));
        assert!(status.draining && !status.cancel_running && status.drained_by_upgrade);
    }
    assert!(!h.commit(&h.new).unwrap().draining);
    // The operator drained before the upgrade: the same while it is uncommitted.
    let h = Host::new();
    drain(&h.env, h.fake.clone()).unwrap();
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert!(restart(&h, false).draining);
    // The upgrade drained and died before any switch was recorded.
    let h = Host::new();
    h.upgrade(&mut failing(Step::Compat)).unwrap_err();
    let status = restart(&h, false);
    assert!(status.draining && status.drained_by_upgrade);
    // With nothing of an upgrade left, a stop ends its own drain as before.
    h.commit(&h.old).unwrap();
    assert!(!restart(&h, false).draining);
}

/// The same switch on a systemd host: drain, stop the old unit, replace the
/// unit file, start the new one, in the order the shared sequence fixes.
#[test]
fn a_systemd_upgrade_stops_the_old_unit_then_starts_the_new_one() {
    let h = Host::on(policy::Backend::Systemd);
    let status = h.upgrade(&mut |_| Ok(())).unwrap();
    assert_eq!(status.binary_sha256, h.new);
    h.assert_serving(&h.new);
    let calls = h.fake.0.borrow().calls.clone();
    let verbs: Vec<_> = calls.iter().map(|c| c[1].as_str()).collect();
    assert_eq!(verbs, ["start", "stop", "start"]);
    // The unit is replaced between the stop and the second start.
    let paths = h.paths();
    let unit = fs::read_to_string(
        fs::read_dir(&paths.services)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path(),
    )
    .unwrap();
    assert!(unit.contains(&paths.executable(&h.new).display().to_string()));
    assert!(!unit.contains(&paths.executable(&h.old).display().to_string()));
    assert_eq!(fs::read_dir(&paths.services).unwrap().count(), 1);
    assert!(status.draining);
    let committed = h.commit(&h.new).unwrap();
    assert!(!committed.draining && committed.switching_from.is_none());
}

// --- the trial marker and forget.lock around the copies (spec 25.1 rule 8) ---

fn group_spec(h: &Host) -> crate::service::bootstrap::Spec {
    crate::service::bootstrap::Spec {
        state_dir: h.paths().state,
        worker_state_dir: h.paths().state,
        worker_id: WorkerId::parse("mini-a").unwrap(),
        cluster_key_path: h.paths().state.join("deliberately-absent-key"),
        executable: h.new_exe.clone(),
        env: h.env.clone(),
        repo_root: None,
        expected_cluster: None,
        managed_service: None,
    }
}

#[test]
fn the_copy_and_the_putting_back_of_the_database_wait_for_forget_lock_and_change_nothing_without_it(
) {
    let h = Host::new();
    let database = h.database();
    let paths = h.paths();
    let saved = saved_database(&paths, &h.old);
    {
        let _held =
            crate::storage::ProcessLock::acquire(&paths.state, crate::service::group::FORGET_LOCK)
                .unwrap();
        // The copy is refused while another holder deletes: no copy, no trial.
        save_database(&paths, &h.old).unwrap_err();
        assert!(absent(&saved).unwrap());
        assert!(!crate::worker::control::in_trial(&paths.state));
    }
    assert!(save_database(&paths, &h.old).unwrap());
    sql(&database, "CREATE TABLE made_by_new (x)");
    {
        let _held =
            crate::storage::ProcessLock::acquire(&paths.state, crate::service::group::FORGET_LOCK)
                .unwrap();
        // Putting it back is refused too: the database stays as it is.
        restore_database(&paths, &h.old).unwrap_err();
        assert!(tables(&database).contains(&"made_by_new".to_string()));
    }
    restore_database(&paths, &h.old).unwrap();
    assert_eq!(tables(&database), ["left_by_old"]);
}

#[test]
fn a_worker_switch_is_on_trial_from_the_copy_until_it_is_undone_or_committed() {
    // Committed.
    let h = Host::new();
    h.database();
    let state = h.paths().state;
    let mut seen = Vec::new();
    h.upgrade(&mut |step| {
        seen.push((step, crate::worker::control::in_trial(&state)));
        Ok(())
    })
    .unwrap();
    for (step, on_trial) in &seen {
        let index = |s: &Step| STEPS.iter().position(|x| x == s).unwrap();
        let after_copy = index(step) >= index(&Step::Record);
        assert_eq!(*on_trial, after_copy, "{step:?}");
    }
    assert!(crate::worker::control::in_trial(&state));
    // The window the marker covers: a forget refuses, and writes nothing.
    let spec = group_spec(&h);
    let member = crate::ids::CoordinatorId::generate().to_string();
    let e = crate::service::group::forget(&h.env, &spec, &member).unwrap_err();
    assert!(e.message.contains("on trial"), "{e:?}");
    h.commit(&h.new).unwrap();
    assert!(!crate::worker::control::in_trial(&state));

    // Undone.
    let h = Host::new();
    h.database();
    let state = h.paths().state;
    h.upgrade(&mut |_| Ok(())).unwrap();
    assert!(crate::worker::control::in_trial(&state));
    h.rollback(&mut |_| Ok(())).unwrap();
    assert!(!crate::worker::control::in_trial(&state));
}

#[test]
fn a_switch_that_stopped_before_it_was_recorded_leaves_no_trial_marker() {
    // Stopped at Record: the copy began a trial, the journal has no switch.
    let h = Host::new();
    h.database();
    let state = h.paths().state;
    h.upgrade(&mut failing(Step::Record)).unwrap_err();
    assert!(crate::worker::control::in_trial(&state));
    assert!(read_journal(&h.paths()).unwrap().unwrap().switch.is_none());
    h.rollback(&mut |_| Ok(())).unwrap();
    assert!(!crate::worker::control::in_trial(&state));

    // Same, and the old version is simply committed.
    let h = Host::new();
    h.database();
    let state = h.paths().state;
    h.upgrade(&mut failing(Step::Record)).unwrap_err();
    assert!(crate::worker::control::in_trial(&state));
    h.commit(&h.old).unwrap();
    assert!(!crate::worker::control::in_trial(&state));
}
