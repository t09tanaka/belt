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
    /// Every `(binary, cluster)` the bridge check was asked about.
    bridges: Vec<String>,
    /// What the bridge answers instead of the installed worker's own identity.
    bridge_answer: Option<(ClusterId, WorkerId)>,
    bridge_fails: bool,
    /// Replaces the answer to `worker-service env`.
    env_answer: Option<Vec<u8>>,
    /// Answers to `worker-service env`, used first, one per call.
    env_queue: std::collections::VecDeque<Vec<u8>>,
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
    fn parent_os(&self) -> &str {
        // The parent is a Mac whatever machine the test runs on.
        "macos"
    }
    fn bridge_hello(
        &mut self,
        _: &SshTarget,
        binary: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        self.bridges.push(binary.to_string());
        if self.bridge_fails {
            return Err("exit 127".into());
        }
        // The path must exist and run on this host, as ssh would find it.
        let meta = fs::metadata(binary).map_err(|e| format!("{binary}: {e}"))?;
        if !meta.is_file() {
            return Err("not a file".into());
        }
        if let Some(answer) = &self.bridge_answer {
            return Ok(answer.clone());
        }
        let input =
            crate::service::worker::read_input(&WorkerPaths::resolve(&self.env).unwrap().input)
                .map_err(|e| e.to_string())?;
        Ok((input.expected_cluster, input.worker_id))
    }
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
        if shell.contains("'worker-service' 'env'") {
            if let Some(answer) = self.env_queue.pop_front() {
                return Ok(ok(String::from_utf8(answer).unwrap()));
            }
            if let Some(answer) = &self.env_answer {
                return Ok(ok(String::from_utf8(answer.clone()).unwrap()));
            }
            // The real helper, as the installed worker's own binary runs it.
            return Ok(
                match crate::service::worker_service::env_report(&self.env) {
                    Ok(report) => ok(format!(
                        "{}\n",
                        serde_json::json!({"ok": true, "env": report})
                    )),
                    Err(e) => ok(format!(
                        "{}\n",
                        serde_json::json!({"ok": false, "error": format!("{e:#}")})
                    )),
                },
            );
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
    /// One entry per resident reload request `add` made.
    reloads: std::cell::RefCell<Vec<()>>,
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
                bridges: vec![],
                bridge_answer: None,
                bridge_fails: false,
                env_answer: None,
                env_queue: Default::default(),
            },
            reloads: std::cell::RefCell::new(vec![]),
            _dir: dir,
            env,
            spec,
            exe,
        }
    }
    fn add(&mut self, name: &str, ssh: Option<&str>, install: bool) -> Result<AddResult, ApiError> {
        let reloads = &self.reloads;
        add(
            &mut self.remote,
            &self.env,
            &self.spec,
            &self.exe,
            name,
            ssh,
            install,
            |_| {
                reloads.borrow_mut().push(());
                ResidentReload::NotRunning
            },
        )
    }
    fn lifecycle(&mut self, name: &str, action: Lifecycle) -> Result<Status, ApiError> {
        lifecycle(
            &mut self.remote,
            &self.env,
            &self.spec,
            &self.exe,
            name,
            action,
        )
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
    register(&w.env, "mini-a", "ci@h", None).unwrap();
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
        None,
        &digest,
        Lifecycle::Stop {
            cancel_running: true,
        },
        600,
    );
    assert!(stop.contains("'worker-service' 'stop' '--wait-secs' '600' '--cancel-running'"));
    assert!(stop.contains(&format!("rtr-{digest}")));
    assert!(stop.contains("exec 'remote-task-runner'"));
    let plain = helper_command(None, &digest, Lifecycle::Drain, 600);
    assert!(!plain.contains("--cancel-running"));
    assert!(stage_command(&digest).contains(&format!("rtr-{digest}.part")));
    assert!(install_command(&digest).ends_with("worker-service install"));
}

fn status_with(loaded: Option<bool>, ready: bool, draining: bool) -> Status {
    Status {
        worker_id: WorkerId::parse("mini-a").unwrap(),
        cluster: ClusterId::generate(),
        binary_sha256: "0".repeat(64),
        loaded,
        ready,
        draining,
        cancel_running: false,
        live_attempts: Some(0),
        bridge_binary: None,
        traffic_scope: None,
        traffic_hold: None,
        switching_from: None,
        drained_by_upgrade: false,
        switch_running: false,
    }
}

#[test]
fn a_command_reports_only_the_state_the_host_showed() {
    let stop = Lifecycle::Stop {
        cancel_running: false,
    };
    let code = |a, s: &Status| check_outcome(a, s).map_err(|e| e.code);
    assert_eq!(
        code(Lifecycle::Start, &status_with(Some(true), true, false)),
        Ok(())
    );
    assert_eq!(
        code(Lifecycle::Start, &status_with(Some(true), false, false)),
        Ok(())
    );
    for loaded in [Some(false), None] {
        assert_eq!(
            code(Lifecycle::Start, &status_with(loaded, false, false)),
            Err(ErrorCode::Conflict)
        );
    }
    assert_eq!(code(stop, &status_with(Some(false), false, false)), Ok(()));
    for loaded in [Some(true), None] {
        assert_eq!(
            code(stop, &status_with(loaded, true, false)),
            Err(ErrorCode::Conflict)
        );
    }
    assert_eq!(
        code(Lifecycle::Drain, &status_with(Some(true), true, true)),
        Ok(())
    );
    assert_eq!(
        code(Lifecycle::Drain, &status_with(Some(true), true, false)),
        Err(ErrorCode::Conflict)
    );
    assert_eq!(
        code(Lifecycle::Resume, &status_with(Some(true), true, false)),
        Ok(())
    );
    assert_eq!(
        code(Lifecycle::Resume, &status_with(Some(true), true, true)),
        Err(ErrorCode::Conflict)
    );
}

#[test]
fn the_sentence_matches_what_happened() {
    let up = status_with(Some(true), true, false);
    let waiting = status_with(Some(true), false, false);
    assert_eq!(describe("w", Lifecycle::Start, &up), "worker w started");
    assert!(describe("w", Lifecycle::Start, &waiting).contains("not accepting connections"));
    assert_eq!(
        describe(
            "w",
            Lifecycle::Stop {
                cancel_running: true
            },
            &up
        ),
        "worker w stopped"
    );
    assert_eq!(describe("w", Lifecycle::Drain, &up), "worker w draining");
    assert_eq!(describe("w", Lifecycle::Resume, &up), "worker w resumed");
}

#[test]
fn add_install_records_the_absolute_binary_and_the_bridge_is_checked_with_it() {
    let mut w = World::new();
    let result = w.add("mini-a", Some("ci@h"), true).unwrap();
    let binary = result.binary.clone().expect("a recorded binary");
    assert!(Path::new(&binary).is_absolute());
    assert_eq!(w.remote.bridges, std::slice::from_ref(&binary));
    let user = crate::config::parse::parse_user("t", &w.config_text()).unwrap();
    assert_eq!(user.workers[0].binary.as_deref(), Some(binary.as_str()));
    assert_eq!(w.reloads.borrow().len(), 1);
    // The lifecycle helper runs that exact path, not a name from ssh's PATH.
    w.lifecycle("mini-a", Lifecycle::Drain).unwrap();
    let last = w.remote.log.last().unwrap();
    assert!(last.starts_with(&format!("exec '{binary}' 'worker-service' 'drain'")));
    assert!(!last.contains("exec 'remote-task-runner'"));
}

#[test]
fn a_bridge_that_does_not_start_or_answers_for_someone_else_registers_nothing() {
    let mut w = World::new();
    w.remote.bridge_fails = true;
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(error.message.contains("nothing was registered"));
    assert!(!w.config_text().contains("mini-a"));
    w.remote.bridge_fails = false;
    w.remote.bridge_answer = Some((ClusterId::generate(), WorkerId::parse("mini-a").unwrap()));
    assert_eq!(
        w.add("mini-a", Some("ci@h"), true).unwrap_err().code,
        ErrorCode::Conflict
    );
    w.remote.bridge_answer = Some((
        w.spec.expected_cluster.clone().unwrap(),
        WorkerId::parse("other").unwrap(),
    ));
    assert_eq!(
        w.add("mini-a", Some("ci@h"), true).unwrap_err().code,
        ErrorCode::Conflict
    );
    assert!(!w.config_text().contains("mini-a"));
    // A good bridge on the retry registers.
    w.remote.bridge_answer = None;
    assert!(w.add("mini-a", Some("ci@h"), true).unwrap().registered);
}

#[test]
fn the_local_worker_name_is_refused_before_any_remote_step() {
    let mut w = World::new();
    let error = w.add("local", Some("ci@h"), true).unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(w.remote.log.is_empty() && w.remote.bridges.is_empty());
    assert!(!w.remote.staged().exists());
}

#[test]
fn rerunning_install_adds_the_binary_to_an_older_registration_and_keeps_other_bytes() {
    let mut w = World::new();
    fs::create_dir_all(w.config.parent().unwrap()).unwrap();
    let original = "schema_version = 1\n# keep me\n[[workers]]\nid = \"first\"\nssh = \"x@y\"\n\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\nlabels = [\"mac\"]\n\n[[workers]]\nid = \"last\"\nssh = \"z@y\"\n";
    fs::write(&w.config, original).unwrap();
    let result = w.add("mini-a", Some("ci@h"), true).unwrap();
    assert!(!result.registered);
    let binary = result.binary.unwrap();
    let text = w.config_text();
    let user = crate::config::parse::parse_user("t", &text).unwrap();
    let by = |id: &str| user.workers.iter().find(|x| x.id == id).unwrap().clone();
    assert_eq!(by("mini-a").binary.as_deref(), Some(binary.as_str()));
    assert_eq!(by("mini-a").labels, ["mac"]);
    assert_eq!(by("first").binary, None);
    assert_eq!(by("last").binary, None);
    assert!(text.contains("# keep me"));
    // Once recorded, the same install changes nothing more.
    let before = w.config_text();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    assert_eq!(w.config_text(), before);
    assert_eq!(
        w.remote.bridges.len(),
        2,
        "every install proves the bridge again"
    );
}

#[test]
fn a_binary_that_cannot_be_recorded_is_reported() {
    let w = World::new();
    fs::create_dir_all(w.config.parent().unwrap()).unwrap();
    fs::write(
        &w.config,
        "schema_version = 1\n[[workers]]\nid='mini-a'\nssh='h'\n",
    )
    .unwrap();
    assert!(set_binary(&w.env, "mini-a", "/b/rtr").is_ok());
    assert!(w.config_text().contains("binary = \"/b/rtr\""));
    let error = set_binary(&w.env, "ghost", "/b/rtr").unwrap_err();
    assert!(error.message.contains("binary = \"/b/rtr\""));
    // A relative path never reaches the file.
    assert!(set_binary(&w.env, "mini-a", "rtr").is_err());
}

#[test]
fn a_different_recorded_binary_is_a_conflict() {
    let mut w = World::new();
    fs::create_dir_all(w.config.parent().unwrap()).unwrap();
    fs::write(
        &w.config,
        "schema_version = 1\n[[workers]]\nid='mini-a'\nssh='ci@h'\nbinary='/elsewhere/rtr'\n",
    )
    .unwrap();
    assert_eq!(
        w.add("mini-a", Some("ci@h"), true).unwrap_err().code,
        ErrorCode::Conflict
    );
}

#[test]
fn add_reports_what_the_running_resident_did() {
    for answer in [
        ResidentReload::Reloaded {
            connecting: vec!["mini-a".into()],
        },
        ResidentReload::RestartRequired {
            reason: "no answer".into(),
        },
    ] {
        let mut w2 = World::new();
        let expected = format!("{answer:?}");
        let mut once = Some(answer);
        let result = add(
            &mut w2.remote,
            &w2.env,
            &w2.spec,
            &w2.exe,
            "mini-a",
            Some("ci@h"),
            false,
            |_| once.take().unwrap(),
        )
        .unwrap();
        assert_eq!(format!("{:?}", result.resident), expected);
        let json = serde_json::to_value(&result).unwrap();
        assert!(json["resident"]["state"].is_string());
    }
}

#[test]
fn a_lifecycle_answer_from_another_cluster_is_refused() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    w.spec.expected_cluster = Some(ClusterId::generate());
    assert_eq!(
        w.lifecycle("mini-a", Lifecycle::Drain).unwrap_err().code,
        ErrorCode::Conflict
    );
    w.spec.expected_cluster = None;
    assert_eq!(
        w.lifecycle("mini-a", Lifecycle::Drain).unwrap_err().code,
        ErrorCode::InvalidInput
    );
}

#[test]
fn helper_command_runs_a_registered_path_with_its_quoting() {
    let digest = "a".repeat(64);
    let command = helper_command(Some("/w/my dir/rtr"), &digest, Lifecycle::Resume, 5);
    assert_eq!(
        command,
        "exec '/w/my dir/rtr' 'worker-service' 'resume' '--wait-secs' '5'"
    );
}

/// Run a staging command through the real `sh` against a private HOME.
fn sh(command: &str, home: &Path, stdin: &[u8]) -> std::process::Output {
    use std::io::Write as _;
    let mut child = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/bin:/bin")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn staging_publishes_only_a_binary_with_the_expected_digest() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let home = dir.path();
    let bytes = b"the whole binary".to_vec();
    let digest = worker_service::sha256_file({
        let f = home.join("src");
        fs::write(&f, &bytes).unwrap();
        &f.clone()
    })
    .unwrap();
    let probe = |home: &Path| {
        String::from_utf8_lossy(&sh(&probe_command(&digest), home, b"").stdout).into_owned()
    };
    assert!(probe(home).ends_with("absent\n"));
    // A cut-off transfer is removed and nothing is published.
    let cut = sh(&stage_command(&digest), home, &bytes[..5]);
    assert_eq!(cut.status.code(), Some(3));
    let staging = home.join(".remote-task-runner-bootstrap");
    assert!(!staging.join(format!("rtr-{digest}")).exists());
    assert!(!staging.join(format!("rtr-{digest}.part")).exists());
    // The whole transfer is published and probed as staged.
    let full = sh(&stage_command(&digest), home, &bytes);
    assert_eq!(full.status.code(), Some(0), "{:?}", full.stderr);
    assert_eq!(
        fs::read(staging.join(format!("rtr-{digest}"))).unwrap(),
        bytes
    );
    assert!(probe(home).ends_with("staged\n"));
    // An executable with other content is not "staged".
    fs::write(staging.join(format!("rtr-{digest}")), b"damaged").unwrap();
    assert!(probe(home).ends_with("absent\n"));
}

#[tokio::test]
async fn hello_names_who_answered_and_fails_when_nothing_does() {
    let cluster = ClusterId::generate();
    let (parent, peer) = crate::protocol::transport::duplex_pair(4096);
    let identity = crate::protocol::hello::WorkerIdentity {
        capabilities: None,
        worker_id: WorkerId::parse("mini-a").unwrap(),
        worker_boot_id: crate::ids::WorkerBootId::parse("boot1").unwrap(),
        agent_epoch: crate::ids::AgentEpoch(1),
        profile: "p".into(),
        profile_fingerprint: crate::ids::Digest::of_bytes(b"p"),
        coordinator_epoch: CoordinatorEpoch(0),
    };
    let served = cluster.clone();
    let server = tokio::spawn(async move {
        let _accepted =
            crate::protocol::accept(peer, crate::protocol::ServerConfig::new(served, identity))
                .await
                .unwrap();
        std::future::pending::<()>().await;
    });
    let hello = hello_on(parent, &cluster, None).await.unwrap();
    assert_eq!(
        (hello.cluster, hello.worker.as_str()),
        (cluster.clone(), "mini-a")
    );
    assert!(hello
        .features
        .contains(&crate::outputs::FEATURE.to_string()));
    server.abort();
    // A child that exits at once (ssh could not run the binary) is an error.
    let argv: Vec<String> = ["/bin/sh", "-c", "exit 127"].map(String::from).into();
    assert!(hello_through(&argv, &cluster, None).await.is_err());
    assert!(
        hello_through(&["/nonexistent/ssh".to_string()], &cluster, None)
            .await
            .is_err()
    );
}

/// A worker of another version: it speaks the protocol and offers `features`.
fn worker_offering(
    cluster: &ClusterId,
    features: &[&str],
) -> (crate::protocol::Io, tokio::task::JoinHandle<()>) {
    let (parent, peer) = crate::protocol::transport::duplex_pair(4096);
    let identity = crate::protocol::hello::WorkerIdentity {
        capabilities: None,
        worker_id: WorkerId::parse("mini-a").unwrap(),
        worker_boot_id: crate::ids::WorkerBootId::parse("boot1").unwrap(),
        agent_epoch: crate::ids::AgentEpoch(1),
        profile: "p".into(),
        profile_fingerprint: crate::ids::Digest::of_bytes(b"p"),
        coordinator_epoch: CoordinatorEpoch(0),
    };
    let mut config = crate::protocol::ServerConfig::new(cluster.clone(), identity);
    config.local.features = features.iter().map(|f| f.to_string()).collect();
    let server = tokio::spawn(async move {
        if let Ok(_accepted) = crate::protocol::accept(peer, config).await {
            std::future::pending::<()>().await;
        }
    });
    (parent, server)
}

#[tokio::test]
async fn a_probe_requires_exactly_what_the_caller_lists() {
    let cluster = ClusterId::generate();
    // A version that lacks some of what this build offers.
    let older = ["log_resume", "blob_resume"];
    let lacking = crate::forecast::COMMAND_FEATURE.to_string();
    let (io, server) = worker_offering(&cluster, &older);
    let hello = hello_on(io, &cluster, None).await.unwrap();
    assert_eq!(hello.worker.as_str(), "mini-a");
    // It says what it agreed to: nothing it lacks.
    assert!(hello.features.contains(&"log_resume".to_string()));
    assert!(!hello.features.contains(&lacking));
    server.abort();
    // Asked for nothing, or for what it has, it passes.
    let (io, server) = worker_offering(&cluster, &older);
    assert!(hello_on(io, &cluster, Some(&[])).await.is_ok());
    server.abort();
    let (io, server) = worker_offering(&cluster, &older);
    let has = ["log_resume".to_string()];
    assert!(hello_on(io, &cluster, Some(&has)).await.is_ok());
    server.abort();
    // Asked for what it lacks, it is refused.
    let (io, server) = worker_offering(&cluster, &older);
    let error = hello_on(io, &cluster, Some(&[lacking])).await.unwrap_err();
    assert!(error.contains("required features missing"), "{error}");
    server.abort();
}

#[test]
fn what_this_build_offers_and_requires_is_one_list() {
    let offered = crate::protocol::session::offered_features();
    let required = crate::protocol::session::required_features();
    assert_eq!(required, [crate::outputs::FEATURE]);
    assert!(required.iter().all(|f| offered.contains(f)));
    for feature in crate::protocol::hello::SUPPORTED_FEATURES {
        assert!(offered.iter().any(|f| f == feature));
    }
}

#[test]
fn a_resident_that_is_not_there_is_not_running_and_one_that_is_silent_is_unknown() {
    use crate::service::resident::Reply;
    let io = |kind| Err(anyhow::Error::from(std::io::Error::from(kind)));
    assert_eq!(
        resident_answer(io(std::io::ErrorKind::ConnectionRefused)),
        ResidentBuild::NotRunning
    );
    assert_eq!(
        resident_answer(io(std::io::ErrorKind::NotFound)),
        ResidentBuild::NotRunning
    );
    // It accepted the connection and closed it: a resident older than the question.
    let silent = resident_answer(io(std::io::ErrorKind::UnexpectedEof));
    assert!(
        matches!(&silent, ResidentBuild::Unknown { reason } if reason.starts_with("it did not answer: ")),
        "{silent:?}"
    );
    assert!(matches!(
        resident_answer(Err(anyhow::anyhow!("not an io error"))),
        ResidentBuild::Unknown { .. }
    ));
    assert_eq!(
        resident_answer(Ok(Reply::Build {
            binary_sha256: "d".into(),
            features: vec!["f".into()],
            required_features: vec!["r".into()],
        })),
        ResidentBuild::Running {
            binary_sha256: "d".into(),
            features: vec!["f".into()],
            required_features: vec!["r".into()],
        }
    );
    assert_eq!(
        resident_answer(Ok(Reply::Error {
            message: "why".into()
        })),
        ResidentBuild::Unknown {
            reason: "why".into()
        }
    );
    assert_eq!(
        resident_answer(Ok(Reply::Stopping)),
        ResidentBuild::Unknown {
            reason: "it did not understand the question".into()
        }
    );
}

/// The installed worker runs only a directory of fake tools, so no real
/// tool (and no Xcode launcher) is probed.
fn quiet(w: &World) {
    let state = WorkerPaths::resolve(&w.remote.env).unwrap().state;
    let bin = w.remote.root.join("worker-bin");
    fs::create_dir_all(&bin).unwrap();
    let text = format!(
        "schema_version=1\nactive='t'\n[profiles.t]\npath=[{:?}]\nshell='/bin/sh'\ncache_namespace='t'\n",
        fs::canonicalize(&bin).unwrap().to_str().unwrap()
    );
    let path = state.join("profiles.toml");
    fs::write(&path, text).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn parent_report() -> EnvReport {
    use crate::toolchain::envdiff::{Docker, Observed, ToolFact};
    EnvReport {
        schema_version: 1,
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        tools: BTreeMap::from([(
            "git".to_string(),
            ToolFact {
                observed: Observed::Available,
                version: Some("0.0.0-parent".into()),
                managed: None,
            },
        )]),
        helpers: BTreeMap::from([(
            "remote-task-runner".to_string(),
            Some(env!("CARGO_PKG_VERSION").to_string()),
        )]),
        docker: Docker::NotRunnable,
        disk_free_bytes: None,
        profile: "default".into(),
        required_tools: Vec::new(),
        profile_digest: None,
        policy_digest: None,
    }
}

#[test]
fn diff_asks_the_installed_worker_binary_and_compares_with_the_parent() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@mini-a.local"), true).unwrap();
    quiet(&w);
    w.remote.log.clear();
    let diffs = diff_workers(&mut w.remote, &w.env, &w.exe, Some("mini-a"), || {
        Ok(parent_report())
    })
    .unwrap();
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].worker, "mini-a");
    let get = |name: &str| diffs[0].items.iter().find(|i| i.subject == name).unwrap();
    // The helper measured the worker itself: its own version and a real disk.
    assert_eq!(get("remote-task-runner").class, envdiff::Class::Same);
    assert_ne!(get("git").class, envdiff::Class::Same);
    assert!(get("disk").worker.is_some());
    assert!(!diffs[0].all_same);
    // One fixed command: no worker name, no path from the caller.
    assert_eq!(w.remote.log.len(), 1);
    assert!(w.remote.log[0].contains("'worker-service' 'env'"));
    assert!(!w.remote.log[0].contains("mini-a"));
    let text = render_diff(&diffs);
    assert!(text.starts_with("worker mini-a\n"));
    assert!(text.contains("remote-task-runner"));
    assert!(text.contains("same"));
}

#[test]
fn diff_without_a_name_covers_every_registered_worker_and_names_one_that_is_not() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@a"), true).unwrap();
    quiet(&w);
    let all = diff_workers(&mut w.remote, &w.env, &w.exe, None, || Ok(parent_report())).unwrap();
    assert_eq!(
        all.iter().map(|d| d.worker.as_str()).collect::<Vec<_>>(),
        ["mini-a"]
    );
    let err = diff_workers(&mut w.remote, &w.env, &w.exe, Some("ghost"), || {
        Ok(parent_report())
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    let err = diff_workers(&mut w.remote, &w.env, &w.exe, Some("bad name"), || {
        Ok(parent_report())
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidInput);
    // The parent that cannot be measured is an internal error, before any ssh.
    w.remote.log.clear();
    let err = diff_workers(&mut w.remote, &w.env, &w.exe, Some("mini-a"), || {
        Err("no profile".into())
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
    assert!(w.remote.log.is_empty());
}

fn reply_error(body: &str) -> ApiError {
    parse_env_reply(
        &RemoteOutput {
            code: Some(1),
            stdout: body.as_bytes().to_vec(),
            stderr: vec![],
        },
        "mini-a",
    )
    .unwrap_err()
}

#[test]
fn diff_never_turns_a_bad_answer_into_a_difference() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@a"), true).unwrap();
    for answer in [
        // An older binary: clap's usage error, not JSON.
        "error: invalid value 'env' for '<ACTION>'\n",
        "{\"ok\":true,\"env\":{\"schema_version\":1}}\n",
        "{\"ok\":false,\"error\":\"profile broken\"}\n",
    ] {
        w.remote.env_answer = Some(answer.as_bytes().to_vec());
        let err = diff_workers(&mut w.remote, &w.env, &w.exe, Some("mini-a"), || {
            Ok(parent_report())
        })
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Conflict, "{answer}");
    }
    assert!(reply_error("{\"ok\":false,\"error\":\"profile broken\"}")
        .message
        .contains("profile broken"));
    assert!(reply_error("{\"ok\":false}")
        .message
        .contains("unspecified"));
    // An older binary (nothing on stdout, or a usage text) is told to upgrade.
    for body in ["", "usage"] {
        let message = reply_error(body).message;
        assert!(message.contains("too old"), "{message}");
        assert!(message.contains("workers upgrade mini-a"), "{message}");
    }
    // A login banner before the report is not a missing feature.
    let message = reply_error("Welcome!\n{\"ok\":true,\"env\":{}}\n").message;
    assert!(message.contains("login banner"), "{message}");
    assert!(!message.contains("upgrade"), "{message}");
    // A newer or older schema is named, a damaged report of this schema too.
    let newer = reply_error(&format!(
        "{{\"ok\":true,\"env\":{{\"schema_version\":{}}}}}",
        envdiff::SCHEMA + 1
    ))
    .message;
    assert!(
        newer.contains("newer") && !newer.contains("workers upgrade"),
        "{newer}"
    );
    let older = reply_error("{\"ok\":true,\"env\":{\"schema_version\":0}}").message;
    assert!(
        older.contains("older") && older.contains("workers upgrade mini-a"),
        "{older}"
    );
    let damaged = reply_error(&format!(
        "{{\"ok\":true,\"env\":{{\"schema_version\":{}}}}}",
        envdiff::SCHEMA
    ))
    .message;
    assert!(damaged.contains("damaged"), "{damaged}");
    // Text the worker sent is shown without control characters.
    let message = reply_error("{\"ok\":false,\"error\":\"bad\\u001b[31m red\\nnext\"}").message;
    assert!(!message.chars().any(char::is_control), "{message:?}");
    assert!(message.contains("bad[31m rednext"), "{message}");
    assert!(plain(&"x".repeat(500)).len() == 200);
    w.remote.reachable = false;
    w.remote.env_answer = None;
    let err = diff_workers(&mut w.remote, &w.env, &w.exe, Some("mini-a"), || {
        Ok(parent_report())
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
}

#[test]
fn the_env_command_is_fixed_with_or_without_a_registered_binary() {
    assert_eq!(
        env_command(Some("/opt/rtr/bin"), "abc"),
        "exec '/opt/rtr/bin' 'worker-service' 'env'"
    );
    let staged = env_command(None, "abc");
    assert!(staged.contains("rtr-abc"));
    assert!(staged.contains("'worker-service' 'env'"));
}

#[test]
fn render_lists_each_class_with_its_steps() {
    let diffs = vec![WorkerDiff {
        worker: "w".into(),
        all_same: false,
        error: None,
        items: vec![
            Item {
                subject: "node".into(),
                class: envdiff::Class::AutoInstallable,
                parent: Some("20".into()),
                worker: None,
                steps: Some("run workers sync".into()),
            },
            Item {
                subject: "docker".into(),
                class: envdiff::Class::Same,
                parent: None,
                worker: None,
                steps: None,
            },
        ],
    }];
    let text = render_diff(&diffs);
    assert_eq!(text.lines().count(), 3);
    assert!(text.contains("node") && text.contains("auto_installable"));
    assert!(text.contains("parent=20 worker=-  -> run workers sync"));
    assert!(text.lines().last().unwrap().ends_with("parent=- worker=-"));
    // A worker that could not be measured says why; control characters from
    // the worker's report never reach the terminal.
    let diffs = vec![
        WorkerDiff {
            worker: "down".into(),
            all_same: false,
            error: Some("unreachable".into()),
            items: Vec::new(),
        },
        WorkerDiff {
            worker: "odd".into(),
            all_same: false,
            error: None,
            items: vec![Item {
                subject: "node".into(),
                class: envdiff::Class::Differs,
                parent: Some("20".into()),
                worker: Some("\u{1b}[2J18".into()),
                steps: None,
            }],
        },
    ];
    let text = render_diff(&diffs);
    assert!(text.contains("worker down\n  could not be measured: unreachable\n"));
    assert!(!text.contains('\u{1b}'));
    assert!(text.contains("worker=[2J18"));
}

#[test]
fn diff_of_every_worker_goes_on_after_one_that_fails_and_says_when_there_is_none() {
    let mut w = World::new();
    // No workers: said, and an error rather than an empty success.
    let err =
        diff_workers(&mut w.remote, &w.env, &w.exe, None, || Ok(parent_report())).unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert!(err.message.contains("no workers are registered"));
    w.add("mini-a", Some("ci@host-a"), true).unwrap();
    w.add("mini-b", Some("ci@host-b"), false).unwrap();
    quiet(&w);
    // The first answers nonsense; the second is measured.
    w.remote.env_queue.push_back(b"not json".to_vec());
    let all = diff_workers(&mut w.remote, &w.env, &w.exe, None, || Ok(parent_report())).unwrap();
    assert_eq!(all.len(), 2);
    assert!(all[0].error.as_deref().unwrap().contains("too old"));
    assert!(all[0].items.is_empty() && !all[0].all_same);
    assert!(all[1].error.is_none() && !all[1].items.is_empty());
    // A worker named on the command line that fails is an error.
    w.remote.env_queue.push_back(b"not json".to_vec());
    let err = diff_workers(&mut w.remote, &w.env, &w.exe, Some("mini-a"), || {
        Ok(parent_report())
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
}

mod sync {
    use super::*;
    use crate::toolchain::envdiff::{Docker, Managed, Observed, ToolFact};
    use crate::toolchain::envsync::{Blocked, InstallStep};

    fn node(version: &str, installed: &[&str]) -> ToolFact {
        ToolFact {
            observed: Observed::Available,
            version: Some(version.into()),
            managed: Some(Managed {
                inventory: vec!["18.0.0".into(), "20.1.0".into()],
                installed: installed.iter().map(|v| v.to_string()).collect(),
            }),
        }
    }
    fn parent() -> EnvReport {
        let mut r = parent_report();
        r.docker = Docker::Runnable;
        r.tools.clear();
        r.helpers.clear();
        r.tools.insert(
            "node".into(),
            ToolFact {
                observed: Observed::Available,
                version: Some("20.1.0".into()),
                managed: None,
            },
        );
        r
    }
    fn worker(version: &str, installed: &[&str], docker: Docker) -> Vec<u8> {
        let mut r = parent();
        r.docker = docker;
        r.tools.insert("node".into(), node(version, installed));
        r.disk_free_bytes = Some(100 << 30);
        format!("{}\n", serde_json::json!({"ok": true, "env": r})).into_bytes()
    }

    #[derive(Default)]
    struct Recorder {
        calls: Vec<(String, InstallStep)>,
        fail: Option<String>,
        /// Stops the sync at the install of this version.
        stop_at: Option<(String, ApiError)>,
        /// The worker says it had the version already.
        already: bool,
    }
    impl Installer for Recorder {
        fn install(&mut self, worker: &str, step: &InstallStep) -> Result<Installed, InstallError> {
            self.calls.push((worker.into(), step.clone()));
            if let Some((version, error)) = &self.stop_at {
                if *version == step.version {
                    return Err(InstallError::Stop(error.clone()));
                }
            }
            let how = if self.already {
                Installed::Already
            } else {
                Installed::Now
            };
            self.fail
                .clone()
                .map_or(Ok(how), |reason| Err(InstallError::Failed(reason)))
        }
    }

    fn world() -> World {
        let mut w = World::new();
        w.add("mini-a", Some("ci@a"), true).unwrap();
        w.remote.log.clear();
        w
    }
    fn run(
        w: &mut World,
        installer: &mut dyn Installer,
        project: Option<&[crate::toolchain::versions::EffectiveRequirement]>,
    ) -> Result<SyncResult, ApiError> {
        sync_worker(
            &mut w.remote,
            &w.env,
            &w.exe,
            "mini-a",
            || Ok(parent()),
            project,
            installer,
        )
    }

    #[test]
    fn what_the_manager_may_install_is_installed_and_then_checked() {
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
            worker("18.0.0", &["18.0.0", "20.1.0"], Docker::Runnable),
        ]
        .into();
        let mut installer = Recorder::default();
        let done = run(&mut w, &mut installer, None).unwrap();
        assert_eq!(done.worker, "mini-a");
        assert_eq!(done.installed.len(), 1);
        assert_eq!(installer.calls.len(), 1);
        assert_eq!(installer.calls[0].0, "mini-a");
        assert_eq!(installer.calls[0].1.tool, "node");
        assert_eq!(installer.calls[0].1.version, "20.1.0");
        // Measured before and after, nothing else asked of the host.
        assert_eq!(w.remote.log.len(), 2);
    }

    #[test]
    fn a_version_the_worker_had_already_is_checked_again_and_not_said_to_be_installed() {
        // The parent's measurement missed it; the worker says it is there.
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
            worker("18.0.0", &["18.0.0", "20.1.0"], Docker::Runnable),
        ]
        .into();
        let mut installer = Recorder {
            already: true,
            ..Recorder::default()
        };
        let done = run(&mut w, &mut installer, None).unwrap();
        assert_eq!(installer.calls.len(), 1);
        assert!(done.installed.is_empty(), "nothing was installed here");
        assert!(!render_sync(&done, false).contains("installed"));
        // Measured again all the same: what the worker says must be seen.
        assert_eq!(w.remote.log.len(), 2);
        // And when it is not seen afterwards, that is blocked, not success.
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
        ]
        .into();
        let mut installer = Recorder {
            already: true,
            ..Recorder::default()
        };
        let error = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(error.code, ErrorCode::Blocked);
        assert!(
            error.message.contains("node 20.1.0 is still missing"),
            "{}",
            error.message
        );
        assert!(
            !error.message.contains("installed node"),
            "{}",
            error.message
        );
        assert_eq!(w.remote.log.len(), 2);
    }

    #[test]
    fn a_worker_already_in_sync_installs_nothing_and_asks_once() {
        let mut w = world();
        w.remote.env_queue = [worker("20.1.0", &["20.1.0"], Docker::Runnable)].into();
        let mut installer = Recorder::default();
        let done = run(&mut w, &mut installer, None).unwrap();
        assert!(done.installed.is_empty() && installer.calls.is_empty());
        assert!(done.in_sync);
        assert_eq!(w.remote.log.len(), 1);
    }

    #[test]
    fn a_difference_sync_cannot_change_is_never_called_in_sync() {
        // Another architecture and no free disk: `diff` shows both.
        let mut w = world();
        let mut r: serde_json::Value =
            serde_json::from_slice(&worker("20.1.0", &["20.1.0"], Docker::Runnable)).unwrap();
        r["env"]["arch"] = "other-arch".into();
        r["env"]["disk_free_bytes"] = 0.into();
        w.remote.env_queue = [serde_json::to_vec(&r).unwrap()].into();
        let mut installer = Recorder::default();
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::Blocked);
        assert!(err.message.contains("platform"), "{}", err.message);
        assert!(
            err.message.contains("free at least 2 GiB"),
            "{}",
            err.message
        );
        assert!(installer.calls.is_empty());
    }

    #[test]
    fn what_was_installed_is_said_when_the_second_measurement_fails_or_a_rest_is_blocked() {
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
            b"not json".to_vec(),
        ]
        .into();
        let mut installer = Recorder::default();
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert!(
            err.message.contains("installed node 20.1.0"),
            "{}",
            err.message
        );
        assert_eq!(err.code, ErrorCode::Conflict);
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::NotRunnable),
            worker("18.0.0", &["18.0.0", "20.1.0"], Docker::NotRunnable),
        ]
        .into();
        let err = run(&mut w, &mut Recorder::default(), None).unwrap_err();
        assert!(
            err.message
                .contains("installed before stopping: node 20.1.0"),
            "{}",
            err.message
        );
    }

    #[test]
    fn what_needs_a_person_is_blocked_with_the_steps_and_nothing_is_installed_for_it() {
        let mut w = world();
        w.remote.env_queue = [worker("20.1.0", &["20.1.0"], Docker::NotRunnable)].into();
        let mut installer = Recorder::default();
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::Blocked);
        assert_eq!(
            err.blocked_reason,
            Some(crate::error::BlockedReason::EnvironmentMismatch)
        );
        assert!(err.message.contains("environment_mismatch"));
        assert!(err.message.contains("Colima"), "{}", err.message);
        assert!(installer.calls.is_empty());
    }

    #[test]
    fn an_automatic_install_comes_first_and_the_manual_rest_is_still_reported() {
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::NotRunnable),
            worker("18.0.0", &["18.0.0", "20.1.0"], Docker::NotRunnable),
        ]
        .into();
        let mut installer = Recorder::default();
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(installer.calls.len(), 1);
        assert!(err.message.contains("Colima"));
        assert!(!err.message.contains("node 20.1.0 was not installed"));
        assert_eq!(err.message.matches("\n- ").count(), 1);
    }

    #[test]
    fn a_failed_install_is_blocked_and_never_success() {
        let mut w = world();
        w.remote.env_queue = [worker("18.0.0", &["18.0.0"], Docker::Runnable)].into();
        let mut installer = Recorder {
            fail: Some("the manager refused".into()),
            ..Default::default()
        };
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::Blocked);
        assert!(err.message.contains("node 20.1.0 was not installed"));
        assert!(err.message.contains("the manager refused"));
        // Nothing was installed, so nothing is measured again.
        assert_eq!(w.remote.log.len(), 1);
    }

    #[test]
    fn an_install_that_is_not_visible_afterwards_is_blocked() {
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
        ]
        .into();
        let mut installer = Recorder::default();
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert!(err
            .message
            .contains("node 20.1.0 is still missing after its install"));
    }

    #[test]
    fn an_installer_that_cannot_go_on_ends_the_sync_with_its_own_error() {
        // No resident, an old worker: not a blocked version, the sync stops.
        let mut w = world();
        w.remote.env_queue = [worker("18.0.0", &["18.0.0"], Docker::NotRunnable)].into();
        let mut installer = Recorder {
            stop_at: Some((
                "20.1.0".into(),
                ApiError::new(
                    ErrorCode::ProtocolIncompatible,
                    "run workers upgrade mini-a",
                ),
            )),
            ..Default::default()
        };
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::ProtocolIncompatible);
        assert_eq!(err.message, "run workers upgrade mini-a");
        // Nothing was installed: nothing is measured again, nothing is listed.
        assert_eq!(w.remote.log.len(), 1);
    }

    #[test]
    fn a_stop_after_an_install_still_says_what_was_installed() {
        let mut r = parent();
        r.tools.insert("node".into(), node("18.0.0", &["18.0.0"]));
        r.tools.insert(
            "python".into(),
            ToolFact {
                observed: Observed::Available,
                version: Some("3.11.0".into()),
                managed: Some(Managed {
                    inventory: vec!["3.12.0".into()],
                    installed: vec![],
                }),
            },
        );
        let mut p = parent();
        p.tools.insert(
            "python".into(),
            ToolFact {
                observed: Observed::Available,
                version: Some("3.12.0".into()),
                managed: None,
            },
        );
        let mut installer = Recorder {
            stop_at: Some((
                "3.12.0".into(),
                ApiError::new(ErrorCode::CommunicationUnknown, "no answer"),
            )),
            ..Default::default()
        };
        let mut measured = 0;
        let err = sync_measured(
            "mini-a",
            &p,
            || {
                measured += 1;
                Ok(r.clone())
            },
            None,
            &mut installer,
        )
        .unwrap_err();
        assert_eq!(installer.calls.len(), 2);
        assert_eq!(err.code, ErrorCode::CommunicationUnknown);
        assert_eq!(
            err.message,
            "installed node 20.1.0, but python 3.12.0 was not installed: no answer"
        );
        assert_eq!(measured, 1);
    }

    #[test]
    fn the_line_of_a_finished_sync_says_in_sync_exactly_when_the_result_does() {
        let done = |in_sync, installed: &[&str]| SyncResult {
            worker: "mini-a".into(),
            installed: installed
                .iter()
                .map(|v| InstallStep {
                    tool: "node".into(),
                    version: v.to_string(),
                })
                .collect(),
            in_sync,
        };
        for project in [false, true] {
            assert_eq!(
                render_sync(&done(true, &[]), project),
                "worker mini-a is in sync"
            );
            let not = render_sync(&done(false, &[]), project);
            assert!(
                not.starts_with("worker mini-a is not in sync with the parent"),
                "{not}"
            );
            assert!(!not.contains("is in sync"), "{not}");
            assert!(not.contains("workers diff mini-a"), "{not}");
            // The project is only named when one was given.
            assert_eq!(not.contains("project"), project, "{not}");
            // The JSON says the same as the line.
            assert_eq!(
                serde_json::to_value(done(false, &[])).unwrap()["in_sync"],
                false
            );
        }
        assert_eq!(
            render_sync(&done(true, &["20.1.0", "22.0.0"]), false),
            "worker mini-a is in sync; installed node 20.1.0, node 22.0.0"
        );
        assert!(render_sync(&done(false, &["20.1.0"]), true).ends_with("; installed node 20.1.0"));
    }

    #[test]
    fn a_project_requirement_sets_the_version() {
        let mut w = world();
        w.remote.env_queue = [
            worker("18.0.0", &["18.0.0"], Docker::Runnable),
            worker("18.0.0", &["18.0.0", "20.1.0"], Docker::Runnable),
        ]
        .into();
        let reqs = [crate::toolchain::versions::EffectiveRequirement {
            tool: crate::toolchain::versions::ToolId::parse("node").unwrap(),
            constraints: vec![crate::toolchain::versions::Constraint::parse("^20").unwrap()],
            provenance: vec![],
        }];
        let mut installer = Recorder::default();
        run(&mut w, &mut installer, Some(&reqs)).unwrap();
        assert_eq!(installer.calls[0].1.version, "20.1.0");
    }

    #[test]
    fn the_parent_that_cannot_be_measured_or_a_worker_that_cannot_answer_installs_nothing() {
        let mut w = world();
        let mut installer = Recorder::default();
        let err = sync_worker(
            &mut w.remote,
            &w.env,
            &w.exe,
            "mini-a",
            || Err("no profile".into()),
            None,
            &mut installer,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        w.remote.env_answer = Some(b"garbage".to_vec());
        let err = run(&mut w, &mut installer, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::Conflict);
        assert!(installer.calls.is_empty());
        let err = sync_worker(
            &mut w.remote,
            &w.env,
            &w.exe,
            "ghost",
            || Ok(parent()),
            None,
            &mut installer,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[test]
    fn the_blocked_message_lists_every_line() {
        let err = blocked_error(
            "w",
            &[],
            &[
                Blocked {
                    subject: "a".into(),
                    missing: "a is missing".into(),
                    steps: "do a".into(),
                },
                Blocked {
                    subject: "b".into(),
                    missing: "b is missing".into(),
                    steps: "do b".into(),
                },
            ],
        );
        assert_eq!(
            err.message,
            "worker w is not in sync (environment_mismatch):\n- a is missing: do a\n- b is missing: do b"
        );
    }

    #[test]
    fn project_version_files_are_read_bounded_and_not_through_links() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        assert!(read_project_requirements(&root.join("missing")).is_err());
        // A directory without any version file is said, not "in sync".
        let err = read_project_requirements(root).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(
            err.message.contains("no version files found"),
            "{}",
            err.message
        );
        fs::write(root.join(".node-version"), "20.1.0\n").unwrap();
        let reqs = read_project_requirements(root).unwrap();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].tool.as_str(), "node");
        // A link is refused, not followed.
        fs::remove_file(root.join(".node-version")).unwrap();
        fs::write(root.join("real"), "20\n").unwrap();
        std::os::unix::fs::symlink(root.join("real"), root.join(".node-version")).unwrap();
        assert!(read_project_requirements(root).is_err());
        fs::remove_file(root.join(".node-version")).unwrap();
        // Too large.
        fs::write(
            root.join(".nvmrc"),
            vec![b'1'; crate::toolchain::declarations::MAX_DOCUMENT_BYTES + 1],
        )
        .unwrap();
        assert!(read_project_requirements(root).is_err());
        // Not text.
        fs::write(root.join(".nvmrc"), [0xff, 0xfe]).unwrap();
        assert!(read_project_requirements(root).is_err());
        // A linked `.fvm` directory is not followed to a file outside.
        fs::remove_file(root.join(".nvmrc")).unwrap();
        let outside = crate::test_tmp::tempdir().unwrap();
        fs::write(
            outside.path().join("fvm_config.json"),
            "{\"flutterSdkVersion\":\"3.0.0\"}",
        )
        .unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join(".fvm")).unwrap();
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message.contains(".fvm/fvm_config.json"),
            "{}",
            err.message
        );
        fs::remove_file(root.join(".fvm")).unwrap();
        fs::write(root.join(".nvmrc"), "18\n").unwrap();
        // Contradicting requirements.
        fs::write(root.join(".nvmrc"), "18\n").unwrap();
        fs::write(root.join(".node-version"), "20\n").unwrap();
        assert!(read_project_requirements(root).is_err());
    }
}
