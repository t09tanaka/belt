//! The parent against an in-process stand-in for ssh. The stand-in runs the
//! real worker-host helper with a fake launcher and a private HOME, so no
//! LaunchAgent is registered and no ssh, sudo or user file is touched.
use super::*;
use crate::ids::ClusterId;
use crate::service::worker_service::{handle, tests::Fake, Action, Options, Paths as WorkerPaths};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

struct FakeRemote {
    root: PathBuf,
    env: BTreeMap<String, String>,
    fake: Fake,
    reachable: bool,
    os: &'static str,
    log: Vec<String>,
    stdins: Vec<Vec<u8>>,
}
impl FakeRemote {
    fn staged(&self) -> PathBuf {
        self.root.join("staged")
    }
    fn reply(result: anyhow::Result<Status>) -> RemoteOutput {
        match result {
            Ok(status) => RemoteOutput {
                code: Some(0),
                stdout: format!("{}\n", serde_json::json!({"ok": true, "status": status}))
                    .into_bytes(),
                stderr: vec![],
            },
            Err(error) => RemoteOutput {
                code: Some(1),
                stdout: format!(
                    "{}\n",
                    serde_json::json!({"ok": false, "error": format!("{error:#}")})
                )
                .into_bytes(),
                stderr: vec![],
            },
        }
    }
}
impl Remote for FakeRemote {
    fn run(
        &mut self,
        _: &SshTarget,
        shell: &str,
        stdin: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        self.log.push(shell.to_string());
        self.stdins.push(stdin.to_vec());
        if !self.reachable {
            return Ok(RemoteOutput {
                code: Some(255),
                stdout: vec![],
                stderr: b"ssh: connect refused".to_vec(),
            });
        }
        let ok = |stdout: String| RemoteOutput {
            code: Some(0),
            stdout: stdout.into_bytes(),
            stderr: vec![],
        };
        let options = |cancel_running| Options {
            cancel_running,
            wait: Duration::ZERO,
            ready_timeout: Duration::from_secs(2),
        };
        if shell.starts_with("uname -s") {
            let staged = if self.staged().exists() {
                "staged"
            } else {
                "absent"
            };
            return Ok(ok(format!("{}\n{}\n{staged}\n", self.os, local_arch())));
        }
        if shell.starts_with("set -eu") {
            fs::write(self.staged(), stdin).unwrap();
            fs::set_permissions(self.staged(), fs::Permissions::from_mode(0o700)).unwrap();
            return Ok(ok(String::new()));
        }
        let (action, cancel) = if shell.starts_with("exec \"$HOME/.remote-task-runner-bootstrap/") {
            assert!(shell.ends_with("worker-service install"));
            (Action::Install, false)
        } else {
            let action = ["start", "stop", "drain", "resume"]
                .into_iter()
                .find(|a| shell.contains(&format!("'worker-service' '{a}'")))
                .expect("a helper action");
            (
                match action {
                    "start" => Action::Start,
                    "stop" => Action::Stop,
                    "drain" => Action::Drain,
                    _ => Action::Resume,
                },
                shell.contains("'--cancel-running'"),
            )
        };
        let result = handle(
            action,
            &self.env,
            stdin,
            &self.staged(),
            self.fake.clone(),
            options(cancel),
        );
        if action == Action::Install && result.is_ok() {
            // What the running agent writes on its first start.
            let request: InstallRequest = serde_json::from_slice(stdin).unwrap();
            let state = WorkerPaths::resolve(&self.env).unwrap().state;
            let path = state.join("worker-binding.json");
            fs::write(
                &path,
                serde_json::to_vec(&(1, &request.cluster, &request.worker_id, "k")).unwrap(),
            )
            .unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        Ok(Self::reply(result))
    }
}

struct World {
    _dir: tempfile::TempDir,
    env: BTreeMap<String, String>,
    spec: Spec,
    exe: PathBuf,
    config: PathBuf,
    remote: FakeRemote,
}
impl World {
    fn new() -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path().to_owned();
        let make_home = |name: &str| {
            let home = root.join(name);
            fs::create_dir(&home).unwrap();
            BTreeMap::from([
                ("HOME".to_string(), home.to_str().unwrap().to_string()),
                ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ])
        };
        let env = make_home("parent");
        let worker_env = make_home("worker");
        let key_path = root.join("parent/.config/remote-task-runner/cluster.key");
        crate::secrets::key::init(&key_path).unwrap();
        let exe = root.join("rtr");
        fs::write(&exe, b"pretend binary").unwrap();
        let fake = Fake::default();
        fake.0.borrow_mut().state = WorkerPaths::resolve(&worker_env).unwrap().state;
        let spec = Spec {
            state_dir: root.join("coordinator"),
            worker_state_dir: root.join("local-worker"),
            worker_id: WorkerId::parse("local").unwrap(),
            cluster_key_path: key_path,
            executable: exe.clone(),
            env: BTreeMap::new(),
            repo_root: None,
            expected_cluster: Some(ClusterId::generate()),
            managed_service: None,
        };
        World {
            config: user_config_path(&env).unwrap(),
            remote: FakeRemote {
                root: root.clone(),
                env: worker_env,
                fake,
                reachable: true,
                os: "Darwin",
                log: vec![],
                stdins: vec![],
            },
            _dir: dir,
            env,
            spec,
            exe,
        }
    }
    fn add(&mut self, name: &str, ssh: Option<&str>, install: bool) -> Result<AddResult, ApiError> {
        add(
            &mut self.remote,
            &self.env,
            &self.spec,
            &self.exe,
            name,
            ssh,
            install,
        )
    }
    fn lifecycle(&mut self, name: &str, action: Lifecycle) -> Result<Status, ApiError> {
        lifecycle(&mut self.remote, &self.env, &self.exe, name, action)
    }
    fn config_text(&self) -> String {
        fs::read_to_string(&self.config).unwrap_or_default()
    }
    fn calls(&self) -> Vec<String> {
        self.remote
            .fake
            .0
            .borrow()
            .calls
            .iter()
            .map(|c| c[0].clone())
            .collect()
    }
}

#[test]
fn add_install_stages_installs_starts_and_registers_last() {
    let mut w = World::new();
    let result = w.add("mini-a", Some("ci@mini-a.local"), true).unwrap();
    assert!(result.registered && result.installed);
    let status = result.status.unwrap();
    assert!(status.ready && status.loaded == Some(true));
    assert_eq!(status.worker_id.as_str(), "mini-a");
    assert!(w
        .config_text()
        .contains("[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\n"));
    let user = crate::config::parse::parse_user("t", &w.config_text()).unwrap();
    assert_eq!(user.workers.len(), 1);
    // The staged bytes are this binary; the key rides on stdin, never in a shell string.
    assert_eq!(fs::read(w.remote.staged()).unwrap(), b"pretend binary");
    let key = fs::read_to_string(&w.spec.cluster_key_path).unwrap();
    assert!(w
        .remote
        .stdins
        .iter()
        .any(|s| String::from_utf8_lossy(s).contains(key.trim())));
    assert!(w
        .remote
        .log
        .iter()
        .all(|c| !c.contains(key.trim()) && !c.contains("mini-a")));
    assert_eq!(w.calls(), ["bootstrap"]);
}

#[test]
fn a_repeated_add_install_is_idempotent_and_does_not_restage() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    let staged = w
        .remote
        .stdins
        .iter()
        .filter(|s| s.as_slice() == b"pretend binary")
        .count();
    let again = w.add("mini-a", Some("ci@h"), true).unwrap();
    assert!(!again.registered && again.status.unwrap().ready);
    assert_eq!(
        w.remote
            .stdins
            .iter()
            .filter(|s| s.as_slice() == b"pretend binary")
            .count(),
        staged
    );
    assert_eq!(w.calls(), ["bootstrap"]);
    assert_eq!(w.config_text().matches("[[workers]]").count(), 1);
}

#[test]
fn a_failed_install_registers_nothing_and_a_retry_continues() {
    let mut w = World::new();
    w.remote.fake.0.borrow_mut().fail_start = true;
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(!w.config_text().contains("mini-a"));
    w.remote.fake.0.borrow_mut().fail_start = false;
    assert!(w.add("mini-a", Some("ci@h"), true).unwrap().registered);
    assert!(w.config_text().contains("mini-a"));
}

#[test]
fn an_unreachable_host_is_an_unknown_outcome_and_registers_nothing() {
    let mut w = World::new();
    w.remote.reachable = false;
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(!w.config_text().contains("mini-a"));
    for action in [
        Lifecycle::Start,
        Lifecycle::Drain,
        Lifecycle::Resume,
        Lifecycle::Stop {
            cancel_running: true,
        },
    ] {
        w.remote.reachable = true;
        w.add("mini-a", Some("ci@h"), false).ok();
        w.remote.reachable = false;
        assert_eq!(
            w.lifecycle("mini-a", action).unwrap_err().code,
            ErrorCode::CommunicationUnknown,
            "{action:?}"
        );
    }
}

#[test]
fn a_host_that_is_not_a_matching_macos_is_refused_before_staging() {
    let mut w = World::new();
    w.remote.os = "Linux";
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(!w.remote.staged().exists());
}

#[test]
fn add_validates_its_input() {
    let mut w = World::new();
    assert_eq!(
        w.add("mini-a", None, false).unwrap_err().code,
        ErrorCode::InvalidInput
    );
    assert_eq!(
        w.add("bad name", Some("ci@h"), false).unwrap_err().code,
        ErrorCode::InvalidInput
    );
    assert_eq!(
        w.add("mini-a", Some("-oProxyCommand=x"), false)
            .unwrap_err()
            .code,
        ErrorCode::InvalidInput
    );
    assert_eq!(
        w.add("local", Some("ci@h"), false).unwrap_err().code,
        ErrorCode::Conflict
    );
    assert!(w.remote.log.is_empty());
    assert!(!w.config_text().contains("[[workers]]"));
}

#[test]
fn add_without_install_registers_only_and_refuses_a_changed_destination() {
    let mut w = World::new();
    let first = w.add("mini-a", Some("ci@h"), false).unwrap();
    assert!(first.registered && !first.installed && first.status.is_none());
    assert!(w.remote.log.is_empty());
    assert_eq!(
        w.add("mini-a", Some("ci@other"), false).unwrap_err().code,
        ErrorCode::Conflict
    );
}

#[test]
fn registration_keeps_existing_configuration_bytes() {
    let w = World::new();
    fs::create_dir_all(w.config.parent().unwrap()).unwrap();
    let original = "schema_version = 1\n# keep me\n[coordinator]\nlocal_worker_priority = 3";
    fs::write(&w.config, original).unwrap();
    register(&w.env, "mini-a", "ci@h").unwrap();
    let text = w.config_text();
    assert!(text.starts_with(original));
    assert!(text.contains("id = \"mini-a\""));
}

#[test]
fn install_then_stop_then_start_on_one_host() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    let stopped = w
        .lifecycle(
            "mini-a",
            Lifecycle::Stop {
                cancel_running: false,
            },
        )
        .unwrap();
    assert_eq!(stopped.loaded, Some(false));
    assert!(!stopped.ready);
    assert_eq!(w.calls(), ["bootstrap", "bootout"]);
    let started = w.lifecycle("mini-a", Lifecycle::Start).unwrap();
    assert!(started.ready && started.loaded == Some(true));
    assert_eq!(w.calls(), ["bootstrap", "bootout", "bootstrap"]);
}

#[test]
fn drain_and_resume_toggle_placement_and_cancel_running_stops_the_service() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    assert!(w.lifecycle("mini-a", Lifecycle::Drain).unwrap().draining);
    assert!(!w.lifecycle("mini-a", Lifecycle::Resume).unwrap().draining);
    let stopped = w
        .lifecycle(
            "mini-a",
            Lifecycle::Stop {
                cancel_running: true,
            },
        )
        .unwrap();
    assert_eq!(stopped.loaded, Some(false));
    assert!(w.remote.log.last().unwrap().contains("'--cancel-running'"));
    assert!(w
        .remote
        .log
        .iter()
        .filter(|c| c.contains("'stop'"))
        .all(|c| c.contains("'--wait-secs' '600'")));
}

#[test]
fn lifecycle_needs_a_registered_worker_and_an_answer_for_that_worker() {
    let mut w = World::new();
    assert_eq!(
        w.lifecycle("ghost", Lifecycle::Drain).unwrap_err().code,
        ErrorCode::NotFound
    );
    w.add("mini-a", Some("ci@h"), true).unwrap();
    // Another registration pointing at the same host must not accept mini-a's answer.
    w.add("mini-b", Some("ci@h"), false).unwrap();
    assert_eq!(
        w.lifecycle("mini-b", Lifecycle::Drain).unwrap_err().code,
        ErrorCode::Conflict
    );
}

#[test]
fn replies_that_prove_nothing_are_unknown_not_success() {
    let reply = |code, stdout: &str| RemoteOutput {
        code,
        stdout: stdout.as_bytes().to_vec(),
        stderr: vec![],
    };
    let code_of = |o: RemoteOutput| parse_reply(&o).unwrap_err().code;
    assert_eq!(
        code_of(reply(Some(255), "")),
        ErrorCode::CommunicationUnknown
    );
    assert_eq!(code_of(reply(Some(0), "")), ErrorCode::CommunicationUnknown);
    assert_eq!(
        code_of(reply(Some(0), "not json\n")),
        ErrorCode::CommunicationUnknown
    );
    assert_eq!(code_of(reply(None, "{\"ok\":true}\n")), ErrorCode::Conflict);
    assert_eq!(
        code_of(reply(Some(0), "{\"ok\":true,\"status\":{}}\n")),
        ErrorCode::CommunicationUnknown
    );
    let refused = parse_reply(&reply(Some(1), "{\"ok\":false,\"error\":\"busy\"}\n")).unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert!(refused.message.contains("busy"));
}

#[test]
fn helper_commands_are_fixed_strings_without_names_or_paths() {
    let digest = "a".repeat(64);
    let stop = helper_command(
        &digest,
        Lifecycle::Stop {
            cancel_running: true,
        },
        600,
    );
    assert!(stop.contains("'worker-service' 'stop' '--wait-secs' '600' '--cancel-running'"));
    assert!(stop.contains(&format!("rtr-{digest}")));
    assert!(stop.contains("exec 'remote-task-runner'"));
    let plain = helper_command(&digest, Lifecycle::Drain, 600);
    assert!(!plain.contains("--cancel-running"));
    assert!(stage_command(&digest).contains(&format!("rtr-{digest}.part")));
    assert!(install_command(&digest).ends_with("worker-service install"));
}
