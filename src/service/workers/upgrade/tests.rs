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
    /// Another helper holds the host's switch lock while this action runs.
    Busy,
}

struct FakeRemote {
    staging: PathBuf,
    env: BTreeMap<String, String>,
    fake: Fake,
    os: &'static str,
    /// The helper actions that were asked for, in order.
    actions: Vec<String>,
    /// How long the parent was willing to wait for each of them.
    timeouts: Vec<Duration>,
    transfers: usize,
    truncate_transfer: bool,
    /// `(action, fault, only once)`.
    faults: Vec<(&'static str, Fault, bool)>,
    bridges: Vec<String>,
    /// The bridge fails while `bin/current` names this version.
    bridge_fails_on: Option<String>,
    bridge_answer: Option<(ClusterId, WorkerId)>,
    /// What each probe required, in order.
    requirements: Vec<Vec<String>>,
    /// The features a version offers; every other version offers them all.
    features_of: BTreeMap<String, Vec<String>>,
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
        target: &SshTarget,
        binary: &str,
        cluster: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        let required = crate::protocol::session::required_features();
        self.bridge_hello_with(target, binary, cluster, &required)
            .map(|hello| (hello.cluster, hello.worker))
    }
    fn bridge_hello_with(
        &mut self,
        _: &SshTarget,
        binary: &str,
        _: &ClusterId,
        required: &[String],
    ) -> Result<BridgeHello, String> {
        self.bridges.push(binary.to_string());
        self.requirements.push(required.to_vec());
        let paths = self.paths();
        let current = fs::read_link(paths.current_link()).map_err(|e| e.to_string())?;
        if self.bridge_fails_on.as_deref().map(Path::new) == Some(current.as_path()) {
            return Err("exit 127".into());
        }
        if !fs::metadata(binary).map_err(|e| e.to_string())?.is_file() {
            return Err("not a file".into());
        }
        let features = self
            .features_of
            .get(current.to_str().unwrap())
            .cloned()
            .unwrap_or_else(crate::protocol::session::offered_features);
        if let Some(missing) = required.iter().find(|f| !features.contains(f)) {
            return Err(format!("required features missing: {missing}"));
        }
        let input = crate::service::worker::read_input(&paths.input).map_err(|e| e.to_string())?;
        let (cluster, worker) = self
            .bridge_answer
            .clone()
            .unwrap_or((input.expected_cluster, input.worker_id));
        Ok(BridgeHello {
            cluster,
            worker,
            features,
        })
    }
    fn run(
        &mut self,
        _: &SshTarget,
        shell: &str,
        stdin: &[u8],
        timeout: Duration,
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
        let action = ["status", "commit", "upgrade", "rollback"]
            .into_iter()
            .find(|a| shell.contains(&format!(" worker-service {a} --wait-secs 600")))
            .expect("a helper action");
        assert!(shell.starts_with("exec \"$HOME/.remote-task-runner-bootstrap/rtr-"));
        self.actions.push(action.to_string());
        self.timeouts.push(timeout);
        let fault = self.fault(action);
        if fault == Some(Fault::Unreachable) {
            return Ok(output(255, String::new()));
        }
        let mut hook = |step: Step| match fault {
            Some(Fault::At(at)) if at == step => anyhow::bail!("injected failure at {step:?}"),
            _ => Ok(()),
        };
        let fake = self.fake.clone();
        let _held = (fault == Some(Fault::Busy)).then(|| {
            crate::service::worker_service::switch_lock(&self.paths(), true, Duration::ZERO)
                .unwrap()
                .expect("the lock is free")
        });
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
            "commit" => handle(Action::Commit, &self.env, stdin, &staged, fake, OPTIONS),
            _ => handle(Action::Status, &self.env, stdin, &staged, fake, OPTIONS),
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
    resident: ResidentBuild,
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
                timeouts: vec![],
                transfers: 0,
                truncate_transfer: false,
                faults: vec![],
                bridges: vec![],
                bridge_fails_on: None,
                bridge_answer: None,
                requirements: vec![],
                features_of: BTreeMap::new(),
            },
            resident: ResidentBuild::NotRunning,
            _dir: dir,
            env,
            exe,
            old,
        }
    }
    fn upgrade(&mut self) -> Result<UpgradeResult, ApiError> {
        let resident = self.resident.clone();
        upgrade(
            &mut self.remote,
            &self.env,
            &self.spec,
            &self.exe,
            "mini-a",
            |_| resident,
        )
    }
    /// An earlier parent ran the host's upgrade and died at `stop`, or right
    /// after the helper answered when `stop` is `None`.
    fn earlier_run(&self, stop: Option<Step>) {
        let request = host::SwitchRequest {
            schema: 1,
            cluster: self.spec.expected_cluster.clone().unwrap(),
            worker_id: WorkerId::parse("mini-a").unwrap(),
            binary_sha256: self.new.clone(),
        };
        let result = host::upgrade(
            &self.remote.env,
            &request,
            &self.exe,
            self.remote.fake.clone(),
            OPTIONS,
            &mut |step| {
                anyhow::ensure!(Some(step) != stop, "the parent died");
                Ok(())
            },
        );
        assert_eq!(result.is_ok(), stop.is_none());
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
    /// The host serves `digest` with nothing left open: no drain, no recorded
    /// switch, no saved database. The old binary is intact.
    fn assert_serving(&self, digest: &str) {
        let status = self.host();
        assert_eq!(not_serving(&status, digest), None);
        assert!(!status.draining && !status.drained_by_upgrade);
        assert_eq!(status.switching_from, None);
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

fn required() -> Vec<String> {
    crate::protocol::session::required_features()
}
fn none() -> Vec<String> {
    Vec::new()
}

#[test]
fn upgrade_switches_checks_the_bridge_and_commits() {
    let mut w = World::new();
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::Upgraded);
    assert_eq!(
        (result.from.as_str(), result.to.as_str()),
        (&*w.old, &*w.new)
    );
    assert!(!result.draining && !result.status.draining);
    assert_eq!(result.status.binary_sha256, w.new);
    assert_eq!(result.status.switching_from, None);
    assert_eq!(w.actions(), ["status", "upgrade", "commit"]);
    // The switch may wait for running work; the others are short questions.
    assert_eq!(w.remote.timeouts, [120, 900, 120].map(Duration::from_secs));
    assert_eq!(w.remote.transfers, 1);
    // Both checks went through the registered, version-independent path:
    // the old version is asked what it agrees to, the new one must agree to
    // what the resident requires.
    let bridge = w.remote.paths().bridge_binary();
    assert_eq!(w.remote.bridges, [bridge.to_str().unwrap(); 2]);
    assert_eq!(w.remote.requirements, [none(), required()]);
    w.assert_serving(&w.new);
    let json = serde_json::to_value(&result).unwrap();
    assert_eq!(json["outcome"], "upgraded");
    assert_eq!(json["id"], "mini-a");
    assert_eq!(
        describe("mini-a", &result),
        format!(
            "worker mini-a upgraded from version {} to {}",
            &w.old[..12],
            &w.new[..12]
        )
    );
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
    assert_eq!((w.remote.transfers, w.remote.bridges.len()), (1, 3));
    assert_eq!(
        serde_json::to_value(&result).unwrap()["outcome"],
        "already_current"
    );
    assert_eq!(
        describe("mini-a", &result),
        format!("worker mini-a already has version {}", &w.new[..12])
    );
    // The same version without a working bridge is not reported as fine.
    w.remote.bridge_fails_on = Some(w.new.clone());
    w.remote.actions.clear();
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error.message.contains("did not complete hello")
            && error.message.contains("no previous version is known")
            && error.message.contains("last observed: version")
            && error.message.contains("drained by the upgrade")
            && error.message.contains("run workers upgrade again"),
        "{}",
        error.message
    );
    assert_eq!(w.actions(), ["status", "upgrade", "status"]);
    // A version that failed its check is not put back to work; the host
    // remembers whose drain it is, and the next good run ends it.
    assert!(w.host().drained_by_upgrade);
    w.remote.bridge_fails_on = None;
    w.remote.actions.clear();
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::AlreadyCurrent);
    assert!(!result.draining);
    assert_eq!(w.actions(), ["status", "commit"]);
    w.assert_serving(&w.new);
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
    assert_eq!(w.actions(), ["status", "upgrade", "commit"]);
    let status = w.host();
    assert!(status.draining && !status.drained_by_upgrade);
    assert_eq!(status.switching_from, None);
    assert!(describe("mini-a", &result).ends_with("; it was draining before and still is"));
    // Already current and still the operator's drain: said, not changed.
    w.remote.actions.clear();
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::AlreadyCurrent);
    assert!(result.draining);
    assert_eq!(w.actions(), ["status"]);
    assert!(describe("mini-a", &result).ends_with("; it was draining before and still is"));
}

#[test]
fn a_failure_at_any_step_leaves_the_previous_version_serving() {
    for step in host::STEPS {
        if step == Step::Restore {
            continue;
        }
        let mut w = World::new();
        w.remote.faults.push(("upgrade", Fault::At(step), false));
        if step == Step::Settle {
            // The new version runs and the switch is recorded: the helper
            // only failed to answer, and the check decides.
            assert_eq!(w.upgrade().unwrap().outcome, Outcome::Upgraded);
            assert_eq!(w.actions(), ["status", "upgrade", "status", "commit"]);
            w.assert_serving(&w.new);
            continue;
        }
        let error = w.upgrade().unwrap_err();
        assert_eq!(error.code, ErrorCode::Conflict, "{step:?}");
        let unchanged = matches!(step, Step::Drain | Step::Place | Step::Compat | Step::Stop);
        if unchanged {
            assert!(
                error.message.contains("was not upgraded")
                    && error.message.contains("is still serving"),
                "{step:?}: {}",
                error.message
            );
            assert_eq!(w.actions(), ["status", "upgrade", "status", "commit"]);
        } else {
            assert!(
                error.message.contains("rolled back to version")
                    && error.message.contains("serving again"),
                "{step:?}: {}",
                error.message
            );
            assert_eq!(
                w.actions(),
                ["status", "upgrade", "status", "rollback", "commit"]
            );
        }
        assert!(error.message.contains("injected failure"), "{step:?}");
        assert!(!error.message.contains("drain"), "{}", error.message);
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
    assert_eq!(w.actions(), ["status", "upgrade", "rollback", "commit"]);
    assert_eq!(w.remote.timeouts[2], Duration::from_secs(900));
    // The old version before, the new one, the restored one.
    assert_eq!(w.remote.bridges.len(), 3);
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
    // Nothing passed a check, so the drain was not released.
    assert!(!w.actions().contains(&"commit"));
    assert!(w.host().drained_by_upgrade);
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
        assert!(status.draining && status.drained_by_upgrade, "{step:?}");
        // Running it again finishes the upgrade from wherever it stopped,
        // and ends the drain the first run made.
        w.remote.bridge_fails_on = None;
        let result = w.upgrade().unwrap();
        assert_eq!(result.outcome, Outcome::Upgraded, "{step:?}");
        assert!(!result.draining, "{step:?}");
        w.assert_serving(&w.new);
    }
}

#[test]
fn an_interrupted_switch_is_finished_by_running_upgrade_again() {
    for step in [Step::Place, Step::RemoveOld, Step::Publish, Step::Start] {
        let mut w = World::new();
        // An earlier run died on the host between the two versions.
        w.remote.faults.push(("upgrade", Fault::At(step), true));
        w.remote.faults.push(("status", Fault::Pass, true));
        w.remote.faults.push(("status", Fault::Unreachable, false));
        let error = w.upgrade().unwrap_err();
        assert_eq!(error.code, ErrorCode::CommunicationUnknown);
        assert_eq!(w.actions(), ["status", "upgrade", "status"]);
        w.remote.faults.clear();
        w.remote.actions.clear();
        let result = w.upgrade().unwrap();
        assert_eq!(result.outcome, Outcome::Upgraded);
        assert_eq!(result.from, w.old);
        // It was drained by the run that began the switch, so it is resumed.
        assert!(!result.draining, "{step:?}");
        assert_eq!(w.actions(), ["status", "upgrade", "commit"]);
        w.assert_serving(&w.new);
    }
}

#[test]
fn a_parent_that_died_before_its_check_leaves_a_switch_the_next_run_commits() {
    let mut w = World::new();
    w.earlier_run(None);
    let left = w.host();
    assert_eq!(not_serving(&left, &w.new), None);
    assert!(left.draining && left.drained_by_upgrade);
    assert_eq!(left.switching_from.as_deref(), Some(w.old.as_str()));
    let result = w.upgrade().unwrap();
    // Not "already current, still drained": the first run's drain is ended.
    assert_eq!(result.outcome, Outcome::Upgraded);
    assert_eq!(result.from, w.old);
    assert!(!result.draining);
    assert_eq!(w.actions(), ["status", "upgrade", "commit"]);
    w.assert_serving(&w.new);
}

#[test]
fn a_parent_that_died_before_its_check_can_still_roll_back_on_the_next_run() {
    let mut w = World::new();
    w.earlier_run(None);
    w.remote.bridge_fails_on = Some(w.new.clone());
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error.message.contains("rolled back to version") && error.message.contains("serving again"),
        "{}",
        error.message
    );
    assert!(!error.message.contains("drain"), "{}", error.message);
    assert_eq!(w.actions(), ["status", "upgrade", "rollback", "commit"]);
    w.assert_serving(&w.old);
}

#[test]
fn a_drain_made_by_a_run_that_died_before_the_switch_is_ended_by_the_next_run() {
    for stop in [Step::Place, Step::Compat, Step::Stop] {
        let mut w = World::new();
        w.earlier_run(Some(stop));
        let left = w.host();
        assert!(left.draining && left.drained_by_upgrade, "{stop:?}");
        assert_eq!(left.switching_from, None);
        let result = w.upgrade().unwrap();
        assert_eq!(result.outcome, Outcome::Upgraded);
        assert!(!result.draining, "{stop:?}");
        assert!(!describe("mini-a", &result).contains("drain"));
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
    assert_eq!(w.actions(), ["status", "upgrade", "status"]);
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
    assert_eq!(w.actions(), ["status", "upgrade", "status", "commit"]);
    // The previous version was asked, with what it agreed to before.
    assert_eq!(w.remote.requirements, [none(), required()]);
    w.assert_serving(&w.old);
}

#[test]
fn a_previous_version_that_does_not_answer_is_not_called_still_serving() {
    let mut w = World::new();
    w.remote.bridge_fails_on = Some(w.old.clone());
    w.remote
        .faults
        .push(("upgrade", Fault::At(Step::Compat), false));
    let error = w.upgrade().unwrap_err();
    assert!(
        !error.message.contains("still serving") && !error.message.contains("serving again"),
        "{}",
        error.message
    );
    assert!(
        error
            .message
            .contains("the previous version did not pass its check")
            && error
                .message
                .contains("not confirmed serving either version")
            && error.message.contains("drained by the upgrade"),
        "{}",
        error.message
    );
    // It is not put back to work unchecked.
    assert!(!w.actions().contains(&"commit"));
    assert!(w.host().draining);
}

#[test]
fn a_lost_answer_after_a_finished_switch_is_checked_and_accepted() {
    let mut w = World::new();
    w.remote.faults.push(("upgrade", Fault::LostReply, true));
    let result = w.upgrade().unwrap();
    assert_eq!(result.outcome, Outcome::Upgraded);
    assert_eq!(w.actions(), ["status", "upgrade", "status", "commit"]);
    w.assert_serving(&w.new);
}

#[test]
fn a_failed_commit_is_said_without_undoing_the_upgrade() {
    let mut w = World::new();
    w.remote.faults.push(("commit", Fault::Unreachable, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error.message.contains("was upgraded to version")
            && error.message.contains("the commit did not finish")
            && error.message.contains("run workers upgrade again"),
        "{}",
        error.message
    );
    assert!(!w.actions().contains(&"rollback"));
    let status = w.host();
    assert_eq!(not_serving(&status, &w.new), None);
    assert!(status.draining && status.drained_by_upgrade);
    // The advice works.
    w.remote.faults.clear();
    assert!(!w.upgrade().unwrap().draining);
    w.assert_serving(&w.new);
    // The same for a version that was kept.
    let mut w = World::new();
    w.remote
        .faults
        .push(("upgrade", Fault::At(Step::Compat), false));
    w.remote.faults.push(("commit", Fault::Unreachable, false));
    let error = w.upgrade().unwrap_err();
    assert!(
        error
            .message
            .contains("still serving; the commit did not finish"),
        "{}",
        error.message
    );
    // And for a finished upgrade whose commit was cut short on the host.
    let mut w = World::new();
    w.upgrade().unwrap();
    let paths = w.remote.paths();
    crate::worker::control::write(&paths.state, crate::worker::control::Control::draining())
        .unwrap();
    let mut journal: serde_json::Value =
        serde_json::from_slice(&fs::read(&paths.journal).unwrap()).unwrap();
    journal["drained_by_upgrade"] = true.into();
    fs::write(&paths.journal, serde_json::to_vec(&journal).unwrap()).unwrap();
    w.remote.faults.push(("commit", Fault::Unreachable, true));
    let error = w.upgrade().unwrap_err();
    assert!(
        error.message.contains("has version")
            && error.message.contains("the commit did not finish"),
        "{}",
        error.message
    );
}

#[test]
fn a_registration_that_cannot_be_updated_is_said_after_the_worker_was_resumed() {
    let mut w = World::registered(false);
    let config = w.env["HOME"].clone() + "/.config/remote-task-runner";
    let lock = |mode| fs::set_permissions(&config, fs::Permissions::from_mode(mode)).unwrap();
    lock(0o500);
    let error = w.upgrade();
    lock(0o700);
    let error = error.unwrap_err();
    assert!(
        error.message.contains("was upgraded to version")
            && error
                .message
                .contains("and is not draining, but its registration could not be updated"),
        "{}",
        error.message
    );
    assert_eq!(w.actions(), ["status", "upgrade", "commit"]);
    w.assert_serving(&w.new);
    // An operator's drain is named as theirs.
    let mut w = World::registered(false);
    handle(
        Action::Drain,
        &w.remote.env,
        &[],
        &w.exe,
        w.remote.fake.clone(),
        OPTIONS,
    )
    .unwrap();
    let config = w.env["HOME"].clone() + "/.config/remote-task-runner";
    fs::set_permissions(&config, fs::Permissions::from_mode(0o500)).unwrap();
    let error = w.upgrade();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(error
        .unwrap_err()
        .message
        .contains("and is still draining as it was before, but its registration"),);
}

// --- one switch at a time ----------------------------------------------------

#[test]
fn a_host_that_is_switching_already_is_left_alone() {
    let mut w = World::new();
    w.remote.faults.push(("status", Fault::Busy, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(
        error
            .message
            .contains("a version switch is running on its host")
            && error.message.contains("nothing was changed"),
        "{}",
        error.message
    );
    assert_eq!(w.actions(), ["status"]);
    w.remote.faults.clear();
    w.assert_serving(&w.old);
}

#[test]
fn a_switch_that_is_still_running_after_its_answer_was_lost_is_not_rolled_back() {
    let mut w = World::new();
    w.remote.faults.push(("upgrade", Fault::Unreachable, true));
    w.remote.faults.push(("status", Fault::Pass, true));
    w.remote.faults.push(("status", Fault::Busy, false));
    let error = w.upgrade().unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(
        error
            .message
            .contains("a version switch is still running there")
            && error.message.contains("nothing was rolled back or resumed")
            && error.message.contains("last observed: version"),
        "{}",
        error.message
    );
    // Neither a second switch nor a release of the drain was sent.
    assert_eq!(w.actions(), ["status", "upgrade", "status"]);
}

#[test]
fn a_second_upgrade_that_meets_the_lock_changes_nothing_and_says_so() {
    let mut w = World::new();
    w.remote.faults.push(("upgrade", Fault::Busy, true));
    let error = w.upgrade().unwrap_err();
    assert!(
        error
            .message
            .contains("a version switch is running on this host")
            && error.message.contains("is still serving"),
        "{}",
        error.message
    );
    assert!(!w.actions().contains(&"rollback"));
    w.assert_serving(&w.old);
}

// --- the resident and the features ------------------------------------------

fn running(digest: &str) -> ResidentBuild {
    ResidentBuild::Running {
        binary_sha256: digest.into(),
        features: crate::protocol::session::offered_features(),
        required_features: required(),
    }
}

#[test]
fn a_resident_of_another_build_refuses_the_upgrade_before_anything_is_sent() {
    let mut w = World::new();
    for resident in [
        running(&w.old),
        ResidentBuild::Unknown {
            reason: "it did not answer".into(),
        },
    ] {
        w.resident = resident;
        let error = w.upgrade().unwrap_err();
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(
            error.message.contains("was not upgraded")
                && error.message.contains("restart the resident")
                && error.message.contains("nothing was changed"),
            "{}",
            error.message
        );
    }
    assert!(w.actions().is_empty());
    assert_eq!((w.remote.transfers, w.remote.bridges.len()), (0, 0));
    w.assert_serving(&w.old);
    // The same build: the upgrade goes ahead with what that resident requires.
    w.resident = running(&w.new);
    w.upgrade().unwrap();
    assert_eq!(w.remote.requirements, [none(), required()]);
    w.assert_serving(&w.new);
}

#[test]
fn the_resident_decides_what_the_health_check_requires() {
    let new = "n".repeat(64);
    assert_eq!(
        resident_requirement(&new, ResidentBuild::NotRunning),
        Ok(required())
    );
    assert_eq!(resident_requirement(&new, running(&new)), Ok(required()));
    let other = resident_requirement(&new, running(&"o".repeat(64))).unwrap_err();
    assert_eq!(
        other,
        "the running resident is build oooooooooooo, this binary is nnnnnnnnnnnn"
    );
    let unknown = resident_requirement(
        &new,
        ResidentBuild::Unknown {
            reason: "why".into(),
        },
    )
    .unwrap_err();
    assert!(unknown.contains("did not say which build it is (why)"));
    // The same digest with other features cannot be this binary.
    for (features, required_features) in [
        (vec!["x".to_string()], required()),
        (crate::protocol::session::offered_features(), none()),
    ] {
        let build = ResidentBuild::Running {
            binary_sha256: new.clone(),
            features,
            required_features,
        };
        assert!(resident_requirement(&new, build)
            .unwrap_err()
            .contains("other features"));
    }
}

#[test]
fn the_previous_version_is_asked_only_for_what_it_agreed_to_before() {
    let s = |list: &[&str]| list.iter().map(|f| f.to_string()).collect::<Vec<_>>();
    let wanted = s(&["a", "b"]);
    assert_eq!(previous_requirement(&wanted, None), none());
    assert_eq!(previous_requirement(&wanted, Some(&[])), none());
    assert_eq!(
        previous_requirement(&wanted, Some(&s(&["b", "c"]))),
        s(&["b"])
    );
    assert_eq!(previous_requirement(&wanted, Some(&s(&["b", "a"]))), wanted);
}

#[test]
fn a_rollback_is_checked_with_the_features_the_previous_version_has() {
    // The old version lacks what this build requires; the new one fails its
    // check. The old one is still found serving after the rollback.
    let mut w = World::new();
    w.remote
        .features_of
        .insert(w.old.clone(), vec!["log_resume".into()]);
    w.remote.features_of.insert(w.new.clone(), vec![]);
    let error = w.upgrade().unwrap_err();
    assert!(
        error.message.contains("required features missing")
            && error.message.contains("rolled back to version")
            && error.message.contains("serving again"),
        "{}",
        error.message
    );
    assert_eq!(w.remote.requirements, [none(), required(), none()]);
    w.assert_serving(&w.old);
    // A run that finds the switch already made cannot ask the old version
    // first; it requires nothing of it.
    let mut w = World::new();
    w.earlier_run(None);
    w.remote.features_of.insert(w.old.clone(), vec![]);
    w.remote.features_of.insert(w.new.clone(), vec![]);
    let error = w.upgrade().unwrap_err();
    assert!(error.message.contains("serving again"), "{}", error.message);
    assert_eq!(w.remote.requirements, [required(), none()]);
    w.assert_serving(&w.old);
    // What the old version agreed to is required of it again.
    let mut w = World::new();
    w.remote.bridge_fails_on = Some(w.new.clone());
    w.upgrade().unwrap_err();
    assert_eq!(w.remote.requirements, [none(), required(), required()]);
}

#[test]
fn a_double_that_models_no_features_answers_the_requiring_probe_as_the_plain_one() {
    struct Plain;
    impl Remote for Plain {
        fn run(
            &mut self,
            _: &SshTarget,
            _: &str,
            _: &[u8],
            _: Duration,
        ) -> Result<RemoteOutput, String> {
            Err("unused".into())
        }
        fn bridge_hello(
            &mut self,
            _: &SshTarget,
            binary: &str,
            cluster: &ClusterId,
        ) -> Result<(ClusterId, WorkerId), String> {
            if binary == "missing" {
                return Err("exit 127".into());
            }
            Ok((cluster.clone(), WorkerId::parse("mini-a").unwrap()))
        }
    }
    let target = target_of("ci@mini-a.local").unwrap();
    let cluster = ClusterId::generate();
    let hello = Plain
        .bridge_hello_with(&target, "rtr", &cluster, &required())
        .unwrap();
    assert_eq!(
        hello,
        BridgeHello {
            cluster: cluster.clone(),
            worker: WorkerId::parse("mini-a").unwrap(),
            features: vec![],
        }
    );
    assert!(Plain
        .bridge_hello_with(&target, "missing", &cluster, &[])
        .is_err());
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
    let not_running = |_: &Spec| ResidentBuild::NotRunning;
    let error = upgrade(
        &mut w.remote,
        &w.env,
        &w.spec,
        &w.exe,
        "mini-b",
        not_running,
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    let error = upgrade(
        &mut w.remote,
        &w.env,
        &w.spec,
        &w.exe,
        "bad name",
        not_running,
    )
    .unwrap_err();
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
    assert_eq!(w.remote.bridges, [bridge.to_str().unwrap(); 2]);
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
        drained_by_upgrade: false,
        switch_running: false,
        traffic_scope: None,
        traffic_hold: None,
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
    // A started version whose switch awaits its commit is serving.
    assert_eq!(not_serving(&status("v", Some("u")), "v"), None);
    let why = |s: &Status| not_serving(s, "v").unwrap();
    assert!(why(&status("0123456789abcdef", None)).contains("reports version 0123456789ab"));
    let mut s = status("v", None);
    s.loaded = Some(false);
    assert!(why(&s).contains("not loaded"));
    s.loaded = None;
    assert!(why(&s).contains("could not be identified"));
    s.switching_from = Some("u".into());
    assert!(why(&s).contains("unfinished"));
    s.loaded = Some(false);
    assert!(why(&s).contains("not loaded"));
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
    s.drained_by_upgrade = true;
    assert!(observed(&s).ends_with(", not draining"));
    s.switching_from = Some("fedcba9876543210".into());
    s.loaded = Some(false);
    s.ready = false;
    s.draining = true;
    assert_eq!(
        observed(&s),
        "last observed: version 0123456789ab (switch from fedcba987654 not committed), service not loaded, agent not ready, drained by the upgrade"
    );
    s.drained_by_upgrade = false;
    assert!(observed(&s).ends_with(", draining"));
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

#[test]
fn without_resident_credentials_no_resident_is_running() {
    let mut w = World::new();
    // No state directory, so no token and no socket.
    assert_eq!(resident_build(&w.spec), ResidentBuild::NotRunning);
    w.spec.expected_cluster = None;
    assert_eq!(resident_build(&w.spec), ResidentBuild::NotRunning);
}
