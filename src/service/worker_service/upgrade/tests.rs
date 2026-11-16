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
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path().to_owned();
        let home = root.join("h");
        fs::create_dir(&home).unwrap();
        let (old_exe, new_exe) = (root.join("rtr-old"), root.join("rtr-new"));
        fs::write(&old_exe, b"old version").unwrap();
        fs::write(&new_exe, b"new version").unwrap();
        let env = BTreeMap::from([
            ("HOME".to_string(), home.to_str().unwrap().to_string()),
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ]);
        let fake = Fake::default();
        fake.0.borrow_mut().state = home.join(".remote-task-runner");
        let cluster = ClusterId::generate();
        let old = sha256_file(&old_exe).unwrap();
        let request = InstallRequest {
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
        let paths = Paths::resolve(&env).unwrap();
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
        Paths::resolve(&self.env).unwrap()
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
    fn status(&self) -> Status {
        status(&self.env, self.fake.clone()).unwrap()
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
    /// The host serves `digest`: journal, link, input and service agree, the
    /// journal is at rest in its first shape, and no version binary changed.
    fn assert_serving(&self, digest: &str) {
        let paths = self.paths();
        let status = self.status();
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
        assert_eq!(
            fs::read(paths.executable(&self.old)).unwrap(),
            b"old version"
        );
        assert_eq!(
            status.bridge_binary.as_deref(),
            paths.bridge_binary().to_str()
        );
    }
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
    h.assert_serving(&h.new);
    assert_eq!(h.launchctl(), ["bootstrap", "bootout", "bootstrap"]);
    // The worker stays drained until the caller has checked it and resumes.
    assert!(status.draining && !status.cancel_running);
    let paths = h.paths();
    assert_eq!(fs::read(paths.executable(&h.new)).unwrap(), b"new version");
    // The previous version keeps the exact input it ran with.
    assert_eq!(
        fs::read(version_input(&paths, &h.old)).unwrap(),
        h.input_at_install
    );
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
    for step in STEPS {
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
    for step in STEPS {
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
    let error = to(&h.new).unwrap_err();
    assert!(format!("{error:#}").contains("not installed"));
    h.upgrade(&mut |_| Ok(())).unwrap();
    fs::write(h.paths().executable(&h.old), b"tampered").unwrap();
    let error = to(&h.old).unwrap_err();
    assert!(format!("{error:#}").contains("differs from its name"));
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
    assert!(h.upgrade(&mut |_| Ok(())).is_err());
    // Placement alone happened: the old service was never touched.
    assert_eq!(h.launchctl(), ["bootstrap"]);
    assert_eq!(h.current(), Path::new(&h.old));
    assert!(!h.status().draining);
    assert_eq!(fs::read(paths.executable(&h.new)).unwrap(), b"new version");
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
        .join(format!(".current.{}", std::process::id()));
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
