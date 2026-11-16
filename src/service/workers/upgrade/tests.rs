//! `workers upgrade` against an in-process stand-in for ssh. The stand-in
//! runs the real worker-host helper with a fake launcher in a private HOME:
//! no ssh, no LaunchAgent and no user file is touched.
use super::*;
use crate::service::worker_service::{
    handle, install, sha256_file, tests::Fake, upgrade as host, Action, InstallRequest, Options,
    Paths as WorkerPaths,
};
use host::Step;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

const OPTIONS: Options = Options {
    cancel_running: false,
    wait: Duration::ZERO,
    ready_timeout: Duration::from_secs(2),
};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    /// The helper stops at this step.
    At(Step),
    /// ssh cannot reach the host; nothing runs.
    Unreachable,
    /// The action runs, but its answer never arrives.
    LostReply,
    /// Nothing goes wrong this time (before a later fault of the same action).
    Pass,
}

struct FakeRemote {
    staging: PathBuf,
    env: BTreeMap<String, String>,
    fake: Fake,
    os: &'static str,
    /// The helper actions that were asked for, in order.
    actions: Vec<String>,
    transfers: usize,
    truncate_transfer: bool,
    /// `(action, fault, only once)`.
    faults: Vec<(&'static str, Fault, bool)>,
    bridges: Vec<String>,
    /// The bridge fails while `bin/current` names this version.
    bridge_fails_on: Option<String>,
    bridge_answer: Option<(ClusterId, WorkerId)>,
}
impl FakeRemote {
    fn paths(&self) -> WorkerPaths {
        WorkerPaths::resolve(&self.env).unwrap()
    }
    fn fault(&mut self, action: &str) -> Option<Fault> {
        let index = self.faults.iter().position(|(a, _, _)| *a == action)?;
        let (_, fault, once) = self.faults[index];
        if once {
            self.faults.remove(index);
        }
        Some(fault)
    }
    fn digest_in(shell: &str) -> String {
        let at = shell.find("/rtr-").expect("a staged path") + 5;
        shell[at..at + 64].to_string()
    }
}
fn output(code: i32, stdout: String) -> RemoteOutput {
    RemoteOutput {
        code: Some(code),
        stdout: stdout.into_bytes(),
        stderr: vec![],
    }
}
impl Remote for FakeRemote {
    fn parent_os(&self) -> &str {
        self.os
    }
    fn bridge_hello(
        &mut self,
        _: &SshTarget,
        binary: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        self.bridges.push(binary.to_string());
        let paths = self.paths();
        let current = fs::read_link(paths.current_link()).map_err(|e| e.to_string())?;
        if self.bridge_fails_on.as_deref().map(Path::new) == Some(current.as_path()) {
            return Err("exit 127".into());
        }
        if !fs::metadata(binary).map_err(|e| e.to_string())?.is_file() {
            return Err("not a file".into());
        }
        if let Some(answer) = &self.bridge_answer {
            return Ok(answer.clone());
        }
        let input = crate::service::worker::read_input(&paths.input).map_err(|e| e.to_string())?;
        Ok((input.expected_cluster, input.worker_id))
    }
    fn run(
        &mut self,
        _: &SshTarget,
        shell: &str,
        stdin: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        let staged = self.staging.join(format!("rtr-{}", Self::digest_in(shell)));
        if shell.starts_with("uname -s") {
            let state = if staged.exists() { "staged" } else { "absent" };
            return Ok(output(
                0,
                format!("Darwin\n{}\n{state}\n", super::super::local_arch()),
            ));
        }
        if shell.starts_with("set -eu") {
            self.transfers += 1;
            let bytes = if self.truncate_transfer {
                &stdin[..stdin.len() / 2]
            } else {
                stdin
            };
            // The staging command publishes only bytes of the expected digest.
            let part = self.staging.join("part");
            fs::write(&part, bytes).unwrap();
            if sha256_file(&part).unwrap() != Self::digest_in(shell) {
                fs::remove_file(&part).unwrap();
                return Ok(output(3, String::new()));
            }
            fs::rename(&part, &staged).unwrap();
            return Ok(output(0, String::new()));
        }
        let action = ["status", "drain", "resume", "upgrade", "rollback"]
            .into_iter()
            .find(|a| shell.contains(&format!(" worker-service {a} --wait-secs 600")))
            .expect("a helper action");
        assert!(shell.starts_with("exec \"$HOME/.remote-task-runner-bootstrap/rtr-"));
        self.actions.push(action.to_string());
        let fault = self.fault(action);
        if fault == Some(Fault::Unreachable) {
            return Ok(output(255, String::new()));
        }
        let mut hook = |step: Step| match fault {
            Some(Fault::At(at)) if at == step => anyhow::bail!("injected failure at {step:?}"),
            _ => Ok(()),
        };
        let fake = self.fake.clone();
        let result = match action {
            "upgrade" => host::upgrade(
                &self.env,
                &serde_json::from_slice(stdin).unwrap(),
                &staged,
                fake,
                OPTIONS,
                &mut hook,
            ),
            "rollback" => host::rollback(
                &self.env,
                &serde_json::from_slice(stdin).unwrap(),
                fake,
                OPTIONS,
                &mut hook,
            ),
            other => {
                let action = match other {
                    "status" => Action::Status,
                    "drain" => Action::Drain,
                    _ => Action::Resume,
                };
                handle(action, &self.env, stdin, &staged, fake, OPTIONS)
            }
        };
        if fault == Some(Fault::LostReply) {
            return Ok(output(255, String::new()));
        }
        Ok(match result {
            Ok(status) => output(
                0,
                format!("{}\n", serde_json::json!({"ok": true, "status": status})),
            ),
            Err(error) => output(
                1,
                format!(
                    "{}\n",
                    serde_json::json!({"ok": false, "error": format!("{error:#}")})
                ),
            ),
        })
    }
}

struct World {
    _dir: tempfile::TempDir,
    env: BTreeMap<String, String>,
    spec: Spec,
    exe: PathBuf,
    old: String,
    new: String,
    remote: FakeRemote,
}
impl World {
    /// A parent whose worker `mini-a` runs the old version.
    fn new() -> Self {
        Self::registered(true)
    }
    fn registered(with_binary: bool) -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path().to_owned();
        let home = |name: &str| {
            let home = root.join(name);
            fs::create_dir(&home).unwrap();
            BTreeMap::from([
                ("HOME".to_string(), home.to_str().unwrap().to_string()),
                ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ])
        };
        let (env, worker_env) = (home("parent"), home("worker"));
        let key_path = root.join("parent/.config/remote-task-runner/cluster.key");
        crate::secrets::key::init(&key_path).unwrap();
        let (old_exe, exe) = (root.join("rtr-old"), root.join("rtr"));
        fs::write(&old_exe, b"old version").unwrap();
        fs::write(&exe, b"new version").unwrap();
        let cluster = ClusterId::generate();
        let paths = WorkerPaths::resolve(&worker_env).unwrap();
        let fake = Fake::default();
        fake.0.borrow_mut().state = paths.state.clone();
        let old = sha256_file(&old_exe).unwrap();
        let request = InstallRequest {
            schema: 1,
            cluster: cluster.clone(),
            worker_id: WorkerId::parse("mini-a").unwrap(),
            key_file_text: format!("{}\n", "ab".repeat(64)),
            binary_sha256: old.clone(),
        };
        let status = install(
            &worker_env,
            &request,
            &old_exe,
            fake.clone(),
            OPTIONS.ready_timeout,
        )
        .unwrap();
        let binding = paths.state.join("worker-binding.json");
        fs::write(
            &binding,
            serde_json::to_vec(&(1, &cluster, &request.worker_id, "k")).unwrap(),
        )
        .unwrap();
        fs::set_permissions(&binding, fs::Permissions::from_mode(0o600)).unwrap();
        let binary = status.bridge_binary.unwrap();
        register(
            &env,
            "mini-a",
            "ci@mini-a.local",
            with_binary.then_some(binary.as_str()),
        )
        .unwrap();
        let staging = root.join("staging");
        fs::create_dir(&staging).unwrap();
        World {
            spec: Spec {
                state_dir: root.join("coordinator"),
                worker_state_dir: root.join("local-worker"),
                worker_id: WorkerId::parse("local").unwrap(),
                cluster_key_path: key_path,
                executable: exe.clone(),
                env: BTreeMap::new(),
                repo_root: None,
                expected_cluster: Some(cluster),
                managed_service: None,
            },
            new: sha256_file(&exe).unwrap(),
            remote: FakeRemote {
                staging,
                env: worker_env,
                fake,
                os: "macos",
                actions: vec![],
                transfers: 0,
                truncate_transfer: false,
                faults: vec![],
                bridges: vec![],
                bridge_fails_on: None,
                bridge_answer: None,
            },
            _dir: dir,
            env,
            exe,
            old,
        }
    }
    fn upgrade(&mut self) -> Result<UpgradeResult, ApiError> {
        upgrade(&mut self.remote, &self.env, &self.spec, &self.exe, "mini-a")
    }
    fn host(&self) -> Status {
        handle(
            Action::Status,
            &self.remote.env,
            &[],
            &self.exe,
            self.remote.fake.clone(),
            OPTIONS,
        )
        .unwrap()
    }
    /// The host serves `digest`, is not draining, and the old binary is intact.
    fn assert_serving(&self, digest: &str) {
        let status = self.host();
        assert_eq!(not_serving(&status, digest), None);
        assert!(!status.draining);
        let paths = self.remote.paths();
        assert_eq!(
            fs::read_link(paths.current_link()).unwrap(),
            Path::new(digest)
        );
        assert_eq!(
            fs::read(paths.executable(&self.old)).unwrap(),
            b"old version"
        );
    }
    fn actions(&self) -> Vec<&str> {
        self.remote.actions.iter().map(String::as_str).collect()
    }
}

#[test]
fn upgrade_drains_switches_checks_the_bridge_and_resumes() {
    let mut w = World::new();
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::Upgraded);
    assert_eq!(
        (result.from.as_str(), result.to.as_str()),
        (&*w.old, &*w.new)
    );
    assert!(!result.draining && !result.status.draining);
    assert_eq!(result.status.binary_sha256, w.new);
    assert_eq!(w.actions(), ["status", "drain", "upgrade", "resume"]);
    assert_eq!(w.remote.transfers, 1);
    // The check went through the registered, version-independent path.
    let bridge = w.remote.paths().bridge_binary();
    assert_eq!(w.remote.bridges, [bridge.to_str().unwrap()]);
    w.assert_serving(&w.new);
    let json = serde_json::to_value(&result).unwrap();
    assert_eq!(json["outcome"], "upgraded");
    assert_eq!(json["id"], "mini-a");
}

#[test]
fn a_worker_that_already_has_the_version_is_checked_and_left_alone() {
    let mut w = World::new();
    w.upgrade().unwrap();
    w.remote.actions.clear();
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::AlreadyCurrent);
    assert_eq!(
        (result.from.as_str(), result.to.as_str()),
        (&*w.new, &*w.new)
    );
    assert_eq!(w.actions(), ["status"]);
    assert_eq!((w.remote.transfers, w.remote.bridges.len()), (1, 2));
    assert_eq!(
        serde_json::to_value(&result).unwrap()["outcome"],
        "already_current"
    );
    // The same version without a working bridge is not reported as fine.
    w.remote.bridge_fails_on = Some(w.new.clone());
    w.remote.actions.clear();
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error.message.contains("did not complete hello")
            && error.message.contains("no previous version is known")
            && error.message.contains("last observed: version"),
        "{}",
        error.message
    );
    // It was serving work before the check and is not left drained.
    assert_eq!(
        w.actions(),
        ["status", "drain", "upgrade", "status", "resume"]
    );
    assert!(!w.host().draining);
}

#[test]
fn a_worker_the_operator_drained_is_upgraded_and_stays_drained() {
    let mut w = World::new();
    handle(
        Action::Drain,
        &w.remote.env,
        &[],
        &w.exe,
        w.remote.fake.clone(),
        OPTIONS,
    )
    .unwrap();
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::Upgraded);
    assert!(result.draining && result.status.draining);
    assert_eq!(w.actions(), ["status", "upgrade"]);
    assert!(w.host().draining);
}

#[test]
fn a_failure_at_any_step_leaves_the_previous_version_serving() {
    for step in host::STEPS {
        let mut w = World::new();
        w.remote.faults.push(("upgrade", Fault::At(step), false));
        let error = w.upgrade().unwrap_err();
        assert_eq!(error.code, ErrorCode::Conflict, "{step:?}");
        let unchanged = matches!(step, Step::Place | Step::Compat | Step::Stop);
        if unchanged {
            assert!(
                error.message.contains("was not upgraded")
                    && error.message.contains("is still serving"),
                "{step:?}: {}",
                error.message
            );
            assert_eq!(
                w.actions(),
                ["status", "drain", "upgrade", "status", "resume"]
            );
        } else {
            assert!(
                error.message.contains("rolled back to version")
                    && error.message.contains("serving again"),
                "{step:?}: {}",
                error.message
            );
            assert_eq!(
                w.actions(),
                ["status", "drain", "upgrade", "status", "rollback", "resume"]
            );
        }
        assert!(error.message.contains("injected failure"), "{step:?}");
        assert!(!error.message.contains("draining"), "{}", error.message);
        w.assert_serving(&w.old);
    }
}

#[test]
fn a_new_version_that_fails_its_bridge_check_is_rolled_back() {
    let mut w = World::new();
    w.remote.bridge_fails_on = Some(w.new.clone());
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error.message.contains("did not complete hello")
            && error.message.contains("rolled back to version"),
        "{}",
        error.message
    );
    assert_eq!(
        w.actions(),
        ["status", "drain", "upgrade", "rollback", "resume"]
    );
    // Once for the new version, once for the restored one.
    assert_eq!(w.remote.bridges.len(), 2);
    w.assert_serving(&w.old);
}

#[test]
fn a_bridge_that_answers_as_someone_else_fails_the_check_both_ways() {
    let mut w = World::new();
    w.remote.bridge_answer = Some((
        w.spec.expected_cluster.clone().unwrap(),
        WorkerId::parse("mini-b").unwrap(),
    ));
    let error = w.upgrade().unwrap_err();
    // The previous version cannot be confirmed either: nothing is claimed.
    assert!(
        error.message.contains("the rollback failed")
            && error.message.contains("different worker or cluster")
            && error
                .message
                .contains("not confirmed serving either version"),
        "{}",
        error.message
    );
    assert!(!error.message.contains("serving again"));
    w.remote.bridge_answer = Some((ClusterId::generate(), WorkerId::parse("mini-a").unwrap()));
    assert!(w.upgrade().is_err());
}

#[test]
fn a_failed_rollback_says_what_was_seen_and_claims_neither_version() {
    for step in [
        Step::Stop,
        Step::Record,
        Step::RemoveOld,
        Step::Input,
        Step::Link,
        Step::Publish,
        Step::Start,
        Step::Settle,
    ] {
        let mut w = World::new();
        w.remote.bridge_fails_on = Some(w.new.clone());
        w.remote.faults.push(("rollback", Fault::At(step), true));
        let error = w.upgrade().unwrap_err();
        assert_eq!(error.code, ErrorCode::Conflict, "{step:?}");
        let message = &error.message;
        assert!(
            message.contains("and the rollback failed (worker helper refused: injected failure")
                && message.contains("last observed: version")
                && message.contains("not confirmed serving either version"),
            "{step:?}: {message}"
        );
        assert!(
            !message.contains("serving again") && !message.contains("was upgraded"),
            "{step:?}: {message}"
        );
        // What it says about the service is what the host shows.
        let status = w.host();
        assert!(message.contains(&observed(&status)), "{step:?}: {message}");
        assert!(status.draining, "{step:?}");
        // Running it again finishes the upgrade from wherever it stopped.
        w.remote.bridge_fails_on = None;
        let result = w.upgrade().unwrap();
        assert_eq!(not_serving(&w.host(), &w.new), None, "{step:?}");
        // A rollback that never recorded its switch left the new version
        // installed and drained; it stays drained until the operator resumes.
        let kept = matches!(step, Step::Stop | Step::Record);
        assert_eq!(result.draining, kept, "{step:?}");
        assert_eq!(w.host().draining, kept, "{step:?}");
        assert_eq!(
            result.outcome == Outcome::AlreadyCurrent,
            step == Step::Stop,
            "{step:?}"
        );
    }
}

#[test]
fn an_interrupted_switch_is_finished_by_running_upgrade_again() {
    for step in [Step::RemoveOld, Step::Publish, Step::Start] {
        let mut w = World::new();
        // An earlier run died on the host between the two versions.
        w.remote.faults.push(("upgrade", Fault::At(step), true));
        w.remote.faults.push(("status", Fault::Pass, true));
        w.remote.faults.push(("status", Fault::Unreachable, false));
        let error = w.upgrade().unwrap_err();
        assert_eq!(error.code, ErrorCode::CommunicationUnknown);
        assert_eq!(w.actions(), ["status", "drain", "upgrade", "status"]);
        w.remote.faults.clear();
        w.remote.actions.clear();
        let result = w.upgrade().unwrap();
        assert_eq!(result.outcome, Outcome::Upgraded);
        assert_eq!(result.from, w.old);
        // It was drained by the run that began the switch, so it is resumed.
        assert_eq!(w.actions(), ["status", "upgrade", "resume"]);
        w.assert_serving(&w.new);
    }
}

#[test]
fn an_unobservable_host_is_reported_as_unknown() {
    // Unreachable from the start: nothing was asked of the worker.
    let mut w = World::new();
    w.remote.faults.push(("status", Fault::LostReply, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert_eq!(w.actions(), ["status"]);
    // Unreachable from the switch on: the outcome is not guessed.
    let mut w = World::new();
    w.remote.faults.push(("upgrade", Fault::Unreachable, false));
    w.remote.faults.push(("status", Fault::Pass, true));
    w.remote.faults.push(("status", Fault::Unreachable, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(
        error.message.contains("could not be observed afterwards")
            && error.message.contains("may be on either version"),
        "{}",
        error.message
    );
    assert!(!error.message.contains("rolled back") && !error.message.contains("still serving"));
    assert_eq!(w.actions(), ["status", "drain", "upgrade", "status"]);
}

#[test]
fn an_unreachable_upgrade_call_that_changed_nothing_is_not_an_upgrade() {
    let mut w = World::new();
    w.remote.faults.push(("upgrade", Fault::Unreachable, true));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(
        error.message.contains("was not upgraded") && error.message.contains("still serving"),
        "{}",
        error.message
    );
    assert_eq!(
        w.actions(),
        ["status", "drain", "upgrade", "status", "resume"]
    );
    w.assert_serving(&w.old);
}

#[test]
fn a_lost_answer_after_a_finished_switch_is_checked_and_accepted() {
    let mut w = World::new();
    w.remote.faults.push(("upgrade", Fault::LostReply, true));
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::Upgraded);
    assert_eq!(
        w.actions(),
        ["status", "drain", "upgrade", "status", "resume"]
    );
    w.assert_serving(&w.new);
}

#[test]
fn a_failed_resume_is_said_without_undoing_the_upgrade() {
    let mut w = World::new();
    w.remote.faults.push(("resume", Fault::Unreachable, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error.message.contains("was upgraded to version")
            && error
                .message
                .contains("could not be resumed and is draining"),
        "{}",
        error.message
    );
    assert!(!w.actions().contains(&"rollback"));
    let status = w.host();
    assert_eq!(not_serving(&status, &w.new), None);
    assert!(status.draining);
    // The same for a version that was kept.
    let mut w = World::new();
    w.remote
        .faults
        .push(("upgrade", Fault::At(Step::Compat), false));
    w.remote.faults.push(("resume", Fault::Unreachable, false));
    let error = w.upgrade().unwrap_err();
    assert!(
        error
            .message
            .contains("still serving; it could not be resumed"),
        "{}",
        error.message
    );
}

#[test]
fn a_failed_drain_stops_before_anything_is_installed() {
    let mut w = World::new();
    w.remote.faults.push(("drain", Fault::Unreachable, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(
        error.message.contains("draining failed"),
        "{}",
        error.message
    );
    assert_eq!(w.actions(), ["status", "drain"]);
    let paths = w.remote.paths();
    assert!(fs::symlink_metadata(paths.executable(&w.new)).is_err());
    // A drain whose answer was lost left the marker; the message says so.
    assert!(error.message.contains("may be draining"));
}

#[test]
fn a_damaged_transfer_is_never_run() {
    let mut w = World::new();
    w.remote.truncate_transfer = true;
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(error.message.contains("could not be staged"));
    assert!(w.actions().is_empty());
    assert_eq!(fs::read_dir(&w.remote.staging).unwrap().count(), 0);
    w.assert_serving(&w.old);
}

#[test]
fn upgrade_needs_a_registered_worker_a_cluster_and_a_mac_parent() {
    let mut w = World::new();
    let error = upgrade(&mut w.remote, &w.env, &w.spec, &w.exe, "mini-b").unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    let error = upgrade(&mut w.remote, &w.env, &w.spec, &w.exe, "bad name").unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    w.remote.os = "linux";
    assert_eq!(w.upgrade().unwrap_err().code, ErrorCode::InvalidInput);
    w.remote.os = "macos";
    w.spec.expected_cluster = None;
    assert_eq!(w.upgrade().unwrap_err().code, ErrorCode::InvalidInput);
    assert!(w.actions().is_empty() && w.remote.transfers == 0);
}

#[test]
fn a_host_that_answers_for_another_cluster_is_refused() {
    let mut w = World::new();
    w.spec.expected_cluster = Some(ClusterId::generate());
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(w.actions(), ["status"]);
}

#[test]
fn a_registration_without_a_binary_path_gains_it_after_the_check() {
    let mut w = World::registered(false);
    w.upgrade().unwrap();
    let bridge = w.remote.paths().bridge_binary();
    assert_eq!(w.remote.bridges, [bridge.to_str().unwrap()]);
    let registration = registered(&w.env, "mini-a").unwrap().unwrap();
    assert_eq!(registration.binary.as_deref(), bridge.to_str());
}

fn status(version: &str, from: Option<&str>) -> Status {
    Status {
        worker_id: WorkerId::parse("mini-a").unwrap(),
        cluster: ClusterId::generate(),
        binary_sha256: version.into(),
        loaded: Some(true),
        ready: true,
        draining: false,
        cancel_running: false,
        live_attempts: Some(0),
        bridge_binary: None,
        switching_from: from.map(str::to_owned),
    }
}

#[test]
fn the_previous_version_is_the_one_that_is_not_the_target() {
    let previous = |version, from, target| previous_version(&status(version, from), target);
    assert_eq!(previous("old", None, "new").as_deref(), Some("old"));
    assert_eq!(previous("new", Some("old"), "new").as_deref(), Some("old"));
    // An interrupted rollback: the target is where the switch started.
    assert_eq!(previous("old", Some("new"), "new").as_deref(), Some("old"));
    assert_eq!(previous("new", None, "new"), None);
    assert_eq!(previous("new", Some("new"), "new"), None);
}

#[test]
fn serving_needs_the_version_a_loaded_service_and_a_ready_agent() {
    assert_eq!(not_serving(&status("v", None), "v"), None);
    let why = |s: &Status| not_serving(s, "v").unwrap();
    assert!(why(&status("v", Some("u"))).contains("unfinished"));
    assert!(why(&status("0123456789abcdef", None)).contains("reports version 0123456789ab"));
    let mut s = status("v", None);
    s.loaded = Some(false);
    assert!(why(&s).contains("not loaded"));
    s.loaded = None;
    assert!(why(&s).contains("could not be identified"));
    s.loaded = Some(true);
    s.ready = false;
    assert!(why(&s).contains("does not accept connections"));
}

#[test]
fn the_observed_state_names_every_part() {
    let mut s = status("0123456789abcdef", None);
    assert_eq!(
        observed(&s),
        "last observed: version 0123456789ab, service loaded, agent ready, not draining"
    );
    s.switching_from = Some("u".into());
    s.loaded = Some(false);
    s.ready = false;
    s.draining = true;
    assert_eq!(
        observed(&s),
        "last observed: version 0123456789ab (switch unfinished), service not loaded, agent not ready, draining"
    );
    s.loaded = None;
    assert!(observed(&s).contains("service unknown"));
    assert_eq!(short("abc"), "abc");
}

#[test]
fn the_staged_command_is_a_fixed_string_around_the_digest() {
    assert_eq!(
        staged_command(&"a".repeat(64), "status"),
        format!(
            "exec \"$HOME/.remote-task-runner-bootstrap/rtr-{}\" worker-service status --wait-secs 600",
            "a".repeat(64)
        )
    );
}
