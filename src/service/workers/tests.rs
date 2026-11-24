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
    /// What `uname -m` prints; the machine the test runs on when `None`.
    machine: Option<&'static str>,
    /// The parent's OS, as Rust names it.
    parent: &'static str,
    /// What the staged binary prints for `--version`, and its exit status;
    /// this build's own version when `None`.
    version: Option<(i32, String)>,
    log: Vec<String>,
    stdins: Vec<Vec<u8>>,
    captured: Vec<u8>,
    /// Every `(binary, cluster)` the bridge check was asked about.
    bridges: Vec<String>,
    /// What the bridge answers instead of the installed worker's own identity.
    bridge_answer: Option<(ClusterId, WorkerId)>,
    bridge_fails: bool,
    own_runs_active: Option<bool>,
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
    fn own_runs_active(
        &mut self,
        _: &SshTarget,
        _: &str,
        _: &Spec,
        _: &str,
    ) -> Result<Option<bool>, String> {
        if !self.reachable {
            return Err("SSH unavailable".into());
        }
        Ok(self.own_runs_active)
    }
    fn parent_os(&self) -> &str {
        // The parent is a Mac whatever machine the test runs on, unless a
        // test says otherwise.
        self.parent
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
            all_submitters: false,
            submitter: None,
            wait: Duration::ZERO,
            ready_timeout: Duration::from_secs(2),
        };
        if shell.starts_with("uname -s") {
            let staged = if self.staged().exists() {
                "staged"
            } else {
                "absent"
            };
            let machine = self.machine.unwrap_or(local_arch());
            return Ok(ok(format!("{}\n{machine}\n{staged}\n", self.os)));
        }
        if shell.ends_with(" --version") {
            assert!(self.staged().exists(), "asked before anything was staged");
            let (code, stdout) = self.version.clone().unwrap_or((0, own_version()));
            return Ok(RemoteOutput {
                code: Some(code),
                stdout: stdout.into_bytes(),
                stderr: vec![],
            });
        }
        if shell.starts_with("rm -f ") {
            let _ = fs::remove_file(self.staged());
            return Ok(ok(String::new()));
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
        let (action, cancel) = if shell.starts_with("exec \"$HOME/.belt-bootstrap/") {
            assert!(shell.ends_with("worker-service install"));
            (Action::Install, false)
        } else {
            let action = ["start", "stop", "drain", "resume", "install"]
                .into_iter()
                .find(|a| shell.contains(&format!("'worker-service' '{a}'")))
                .expect("a helper action");
            (
                match action {
                    "install" => Action::Install,
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
            Options {
                all_submitters: shell.contains("'--all-submitters'"),
                ..options(cancel)
            },
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
        let output = Self::reply(result);
        self.captured.extend_from_slice(&output.stdout);
        self.captured.extend_from_slice(&output.stderr);
        Ok(output)
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
        let key_path = root.join("parent/.config/belt/cluster.key");
        crate::secrets::key::init(&key_path).unwrap();
        let exe = root.join("belt");
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
        fs::create_dir_all(&spec.state_dir).unwrap();
        crate::storage::CoordinatorStore::initialize_identity(
            &spec.state_dir,
            spec.expected_cluster.as_ref().unwrap(),
            &crate::ids::CoordinatorId::generate(),
            |stage| {
                fs::rename(stage, spec.state_dir.join("coordinator.db"))?;
                Ok(())
            },
        )
        .unwrap();
        World {
            config: user_config_path(&env).unwrap(),
            remote: FakeRemote {
                root: root.clone(),
                env: worker_env,
                fake,
                reachable: true,
                os: "Darwin",
                machine: None,
                parent: "macos",
                version: None,
                log: vec![],
                stdins: vec![],
                captured: vec![],
                bridges: vec![],
                bridge_answer: None,
                bridge_fails: false,
                own_runs_active: Some(false),
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
            AddOptions {
                install,
                ..AddOptions::default()
            },
            |_| {
                reloads.borrow_mut().push(());
                ResidentReload::NotRunning
            },
        )
    }
    fn add_with(&mut self, name: &str, options: AddOptions<'_>) -> Result<AddResult, ApiError> {
        add(
            &mut self.remote,
            &self.env,
            &self.spec,
            &self.exe,
            name,
            Some("ci@h"),
            options,
            |_| ResidentReload::NotRunning,
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
    let paths = WorkerPaths::resolve(&w.remote.env).unwrap();
    for path in [&paths.journal, &paths.input] {
        assert!(!fs::read_to_string(path).unwrap().contains(key.trim()));
    }
    assert!(!format!("{status:?}").contains(key.trim()));
    for payload in &w.remote.stdins {
        if let Ok(request) = serde_json::from_slice::<InstallRequest>(payload) {
            assert!(!format!("{request:?}").contains(key.trim()));
        }
    }
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
    register(&w.env, "mini-a", "ci@h", None, false).unwrap();
    for action in [
        Lifecycle::Start,
        Lifecycle::Drain,
        Lifecycle::Resume,
        Lifecycle::Stop {
            cancel_running: true,
            all_submitters: false,
        },
    ] {
        w.remote.reachable = false;
        assert_eq!(
            w.lifecycle("mini-a", action).unwrap_err().code,
            ErrorCode::CommunicationUnknown,
            "{action:?}"
        );
    }
}

/// The other machine of a pair: never the one the test runs on.
fn other_machine() -> &'static str {
    if local_arch() == "arm64" {
        "x86_64"
    } else {
        "arm64"
    }
}

#[test]
fn the_own_binary_is_not_sent_to_a_host_of_another_platform() {
    for (os, machine, triple) in [
        ("Linux", None, "-unknown-linux-gnu"),
        ("Darwin", Some(other_machine()), "-apple-darwin"),
    ] {
        let mut w = World::new();
        w.remote.os = os;
        w.remote.machine = machine;
        let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(error.message.contains(triple), "{}", error.message);
        assert!(
            error.message.contains("--binary <path>"),
            "{}",
            error.message
        );
        // Only the read-only probe ran, and nothing was registered.
        assert_eq!(w.remote.log.len(), 1);
        assert!(!w.remote.staged().exists());
        assert!(!w.config_text().contains("mini-a"));
    }
    // A platform no worker runs on is refused even with a named build.
    let mut w = World::new();
    w.remote.os = "FreeBSD";
    let exe = w.exe.clone();
    let error = w
        .add_with(
            "mini-a",
            AddOptions {
                install: true,
                binary: Some(&exe),
                ..AddOptions::default()
            },
        )
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(!w.remote.staged().exists());
}

#[test]
fn a_named_build_is_what_is_sent_to_a_host_of_another_platform() {
    let mut w = World::new();
    w.remote.os = "Linux";
    w.remote.machine = Some("x86_64");
    // The build for the worker: other bytes than the parent's own binary.
    let build = w.exe.with_file_name("belt-linux");
    fs::write(&build, b"a build for the worker").unwrap();
    let result = w
        .add_with(
            "mini-a",
            AddOptions {
                install: true,
                binary: Some(&build),
                ..AddOptions::default()
            },
        )
        .unwrap();
    assert!(result.registered && result.installed);
    assert_eq!(
        fs::read(w.remote.staged()).unwrap(),
        b"a build for the worker"
    );
    let digest = worker_service::sha256_file(&build).unwrap();
    assert_ne!(digest, worker_service::sha256_file(&w.exe).unwrap());
    // Every remote step names the build that was sent, and the version was
    // asked for after staging and before the install.
    let steps: Vec<&str> = w.remote.log.iter().map(String::as_str).collect();
    assert!(steps.iter().all(|step| step.contains(&digest)), "{steps:?}");
    assert!(steps[0].starts_with("uname -s; uname -m;"));
    assert!(steps[1].starts_with("set -eu"));
    assert!(steps[2].ends_with(" --version"));
    assert!(steps[3].ends_with("worker-service install"));
    assert_eq!(result.status.unwrap().binary_sha256, digest);
}

#[test]
fn a_linux_parent_sends_its_own_binary_to_a_linux_host_of_its_architecture() {
    let mut w = World::new();
    w.remote.parent = "linux";
    w.remote.os = "Linux";
    let result = w.add("mini-a", Some("ci@h"), true).unwrap();
    assert!(result.installed);
    // To a Mac it does not.
    let mut w = World::new();
    w.remote.parent = "linux";
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert!(error.message.contains("-apple-darwin"), "{}", error.message);
}

#[test]
fn a_build_that_does_not_report_the_parents_version_is_removed_before_any_install() {
    for (code, said) in [
        (0, "belt 0.0.0-other\n"),
        // It reports the right words and fails, or cannot run at all.
        (1, ""),
        (126, "cannot execute binary file\n"),
    ] {
        let mut w = World::new();
        w.remote.version = Some((code, said.to_string()));
        let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidInput, "{code}");
        assert!(
            error.message.contains("nothing was installed or switched"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("the staged copy was removed"),
            "{}",
            error.message
        );
        assert!(error.message.contains(own_version().trim()));
        assert!(!w.remote.staged().exists());
        assert!(!w
            .remote
            .log
            .iter()
            .any(|step| step.contains("worker-service")));
        assert!(w.calls().is_empty());
        assert!(!w.config_text().contains("mini-a"));
    }
    // The parent's own version with a failing exit status is not a match.
    let mut w = World::new();
    w.remote.version = Some((3, own_version()));
    assert!(w.add("mini-a", Some("ci@h"), true).is_err());
    assert!(!w.remote.staged().exists());
    // ssh itself failing says nothing about the copy: it is kept.
    let mut w = World::new();
    w.remote.version = Some((255, String::new()));
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert_eq!(error.code, ErrorCode::CommunicationUnknown);
    assert!(w.remote.staged().exists());
}

#[test]
fn the_staging_commands_use_whichever_digest_tool_the_host_has() {
    let digest = "ab".repeat(32);
    for command in [stage_command(&digest), probe_command(&digest)] {
        assert!(command.contains("command -v sha256sum"), "{command}");
        assert!(command.contains("shasum -a 256"), "{command}");
        assert!(command.contains(&digest));
    }
    assert!(probe_command(&digest).starts_with("uname -s; uname -m;"));
    assert!(stage_command(&digest).starts_with("set -eu;"));
    assert_eq!(
        version_command(&digest),
        format!("exec \"$HOME/.belt-bootstrap/belt-{digest}\" --version")
    );
    assert_eq!(
        unstage_command(&digest),
        format!("rm -f \"$HOME/.belt-bootstrap/belt-{digest}\"")
    );
    assert!(own_version().starts_with("belt "));
    assert!(own_version().contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn an_ephemeral_worker_is_registered_and_never_installed_or_controlled() {
    let mut w = World::new();
    let ephemeral = AddOptions {
        ephemeral: true,
        ..AddOptions::default()
    };
    let result = w.add_with("colab-1", ephemeral).unwrap();
    assert!(result.registered && result.ephemeral && !result.installed);
    assert_eq!(serde_json::to_value(&result).unwrap()["ephemeral"], true);
    assert!(w.config_text().contains("ephemeral = true"));
    let registration = registered(&w.env, "colab-1").unwrap().unwrap();
    assert!(registration.ephemeral);
    assert!(registrations(&w.env).unwrap()[0].ephemeral);
    // Adding it again changes nothing, with or without the flag.
    assert!(!w.add_with("colab-1", ephemeral).unwrap().registered);
    let again = w.add("colab-1", Some("ci@h"), false).unwrap();
    assert!(!again.registered && again.ephemeral);
    assert_eq!(w.config_text().matches("ephemeral = true").count(), 1);
    // No install, in either spelling, and no build to send.
    let exe = w.exe.clone();
    for options in [
        AddOptions {
            install: true,
            ..AddOptions::default()
        },
        AddOptions {
            install: true,
            ephemeral: true,
            ..AddOptions::default()
        },
        AddOptions {
            ephemeral: true,
            binary: Some(&exe),
            ..AddOptions::default()
        },
    ] {
        let error = w.add_with("colab-1", options).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidInput);
        assert!(error.message.contains("no service"), "{}", error.message);
    }
    for action in [
        Lifecycle::Start,
        Lifecycle::Drain,
        Lifecycle::Resume,
        Lifecycle::Stop {
            cancel_running: false,
            all_submitters: false,
        },
        Lifecycle::Stop {
            cancel_running: true,
            all_submitters: false,
        },
    ] {
        let error = w.lifecycle("colab-1", action).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidInput, "{action:?}");
        assert!(error.message.contains("no service"), "{}", error.message);
    }
    // Not one remote command was run for it.
    assert!(w.remote.log.is_empty() && w.remote.bridges.is_empty());
    // A persistent worker stays one; enrollment is not ephemeral.
    w.add("mini-a", Some("ci@h"), true).unwrap();
    assert!(!registered(&w.env, "mini-a").unwrap().unwrap().ephemeral);
    assert!(
        !serde_json::to_value(w.add("mini-a", Some("ci@h"), false).unwrap())
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("ephemeral")
    );
    assert_eq!(
        w.add_with("mini-a", ephemeral).unwrap_err().code,
        ErrorCode::Conflict
    );
    // `--binary` is what `--install` sends, and nothing without it.
    let error = w
        .add_with(
            "mini-b",
            AddOptions {
                binary: Some(&exe),
                ..AddOptions::default()
            },
        )
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.contains("--install"), "{}", error.message);
}

#[test]
fn forget_preserves_the_members_remaining_wait() {
    let message =
        crate::coordinator::identity::ForgetRefusal::Waiting { remaining_ms: 1201 }.message();
    let error = forget("mini-a", |_| {
        Ok(crate::service::resident::Reply::ControlError {
            error: ApiError::new(ErrorCode::Conflict, message.clone()),
        })
    })
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(error.message, message);
    assert!(error.message.contains("2 seconds remaining"));
    assert!(error.message.contains("group key changed"));
}

#[test]
fn forget_reports_only_what_the_resident_confirmed() {
    use crate::service::resident::Reply;
    let attempt = crate::ids::AttemptId::generate();
    let asked = std::cell::RefCell::new(Vec::new());
    let result = forget("colab-1", |worker| {
        asked.borrow_mut().push(worker.to_string());
        Ok(Reply::WorkerForgotten {
            identity_retired: true,
            ended: vec![attempt.clone()],
        })
    })
    .unwrap();
    assert_eq!(*asked.borrow(), ["colab-1"]);
    assert_eq!(
        result,
        ForgetResult {
            id: "colab-1".into(),
            identity_retired: true,
            ended: vec![attempt],
        }
    );
    let text = describe_forget(&result);
    assert!(
        text.contains("colab-1") && text.contains("is retired"),
        "{text}"
    );
    assert!(text.ends_with("1 attempt ended"), "{text}");
    let nothing = describe_forget(&ForgetResult {
        id: "colab-1".into(),
        identity_retired: false,
        ended: vec![],
    });
    assert!(nothing.contains("no identity was recorded"), "{nothing}");
    assert!(nothing.contains("held no attempt"), "{nothing}");
    // The resident's refusal is passed on as it is.
    let refused = forget("mini-a", |_| {
        Ok(Reply::ControlError {
            error: ApiError::new(ErrorCode::Conflict, "the worker is connected"),
        })
    })
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert_eq!(refused.message, "the worker is connected");
    // Anything else is not a forget.
    let stopping = forget("colab-1", |_| Ok(Reply::Stopping)).unwrap_err();
    assert_eq!(stopping.code, ErrorCode::CommunicationUnknown);
    assert!(stopping.message.contains("nothing was forgotten"));
    let other = forget("colab-1", |_| {
        Ok(Reply::WorkersReloaded { connecting: vec![] })
    });
    let other = other.unwrap_err();
    assert_eq!(other.code, ErrorCode::Conflict);
    assert!(other.message.contains("restart it"), "{}", other.message);
    let silent = forget("colab-1", |_| Err("it is not running".into())).unwrap_err();
    assert_eq!(silent.code, ErrorCode::CommunicationUnknown);
    assert!(
        silent.message.contains("it is not running"),
        "{}",
        silent.message
    );
    // A name that cannot be a worker is refused before anything is asked.
    let bad = forget("bad name", |_| panic!("asked")).unwrap_err();
    assert_eq!(bad.code, ErrorCode::InvalidInput);
    // Without a cluster there is no resident to ask.
    let mut w = World::new();
    w.spec.expected_cluster = None;
    let why = ask_resident_to_forget(&w.spec, WorkerId::parse("colab-1").unwrap()).unwrap_err();
    assert!(why.contains("run init"), "{why}");
    // With one, but no resident: nothing is started for it.
    let w = World::new();
    let why = ask_resident_to_forget(&w.spec, WorkerId::parse("colab-1").unwrap()).unwrap_err();
    assert_eq!(why, "it is not running");
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
fn add_without_install_hands_over_membership_and_refuses_a_changed_destination() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    fs::remove_file(&w.config).unwrap();
    let calls = w.calls();
    let first = w.add("mini-a", Some("ci@h"), false).unwrap();
    assert!(first.registered && !first.installed && first.status.unwrap().membership_only);
    assert_eq!(w.calls(), calls);
    assert_eq!(
        w.add("mini-a", Some("ci@other"), false).unwrap_err().code,
        ErrorCode::Conflict
    );
}

#[test]
fn install_on_a_host_that_already_has_belt_does_not_leave_a_staged_binary() {
    let mut w = World::new();
    w.add("mini-a", Some("ci@h"), true).unwrap();
    let again = w.add("mini-a", Some("ci@h"), true).unwrap();
    assert!(again.status.unwrap().membership_only);
    assert!(!w.remote.staged().exists());
}

#[test]
fn registration_keeps_existing_configuration_bytes() {
    let w = World::new();
    fs::create_dir_all(w.config.parent().unwrap()).unwrap();
    let original = "schema_version = 1\n# keep me\n[coordinator]\nlocal_worker_priority = 3";
    fs::write(&w.config, original).unwrap();
    register(&w.env, "mini-a", "ci@h", None, false).unwrap();
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
                all_submitters: false,
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
                all_submitters: false,
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
    register(&w.env, "mini-b", "ci@h", None, false).unwrap();
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
            all_submitters: false,
        },
        600,
        None,
    );
    assert!(stop.contains("'worker-service' 'stop' '--wait-secs' '600' '--cancel-running'"));
    assert!(stop.contains(&format!("belt-{digest}")));
    assert!(stop.contains("exec 'belt'"));
    let id = crate::ids::CoordinatorId::generate();
    let all = helper_command(
        None,
        &digest,
        Lifecycle::Stop {
            cancel_running: false,
            all_submitters: true,
        },
        600,
        Some(&id),
    );
    assert!(all.contains(&format!(
        "'--wait-secs' '600' '--all-submitters' '--submitter' '{id}'"
    )));
    let plain = helper_command(None, &digest, Lifecycle::Drain, 600, Some(&id));
    assert!(!plain.contains("--submitter"));
    assert!(!plain.contains("--cancel-running"));
    assert!(stage_command(&digest).contains(&format!("belt-{digest}.part")));
    assert!(install_command(&digest).ends_with("worker-service install"));
}

fn status_with(loaded: Option<bool>, ready: bool, draining: bool) -> Status {
    Status {
        membership_only: false,
        coordinator_id: None,
        system_user: false,
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
        all_submitters: false,
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
                cancel_running: true,
                all_submitters: false
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
    assert!(!last.contains("exec 'belt'"));
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
    assert!(set_binary(&w.env, "mini-a", "/b/belt").is_ok());
    assert!(w.config_text().contains("binary = \"/b/belt\""));
    let error = set_binary(&w.env, "ghost", "/b/belt").unwrap_err();
    assert!(error.message.contains("binary = \"/b/belt\""));
    // A relative path never reaches the file.
    assert!(set_binary(&w.env, "mini-a", "belt").is_err());
}

#[test]
fn a_different_recorded_binary_is_a_conflict() {
    let mut w = World::new();
    fs::create_dir_all(w.config.parent().unwrap()).unwrap();
    fs::write(
        &w.config,
        "schema_version = 1\n[[workers]]\nid='mini-a'\nssh='ci@h'\nbinary='/elsewhere/belt'\n",
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
        w2.add("mini-a", Some("ci@h"), true).unwrap();
        let expected = format!("{answer:?}");
        let mut once = Some(answer);
        let result = add(
            &mut w2.remote,
            &w2.env,
            &w2.spec,
            &w2.exe,
            "mini-a",
            Some("ci@h"),
            AddOptions::default(),
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
    let command = helper_command(Some("/w/my dir/belt"), &digest, Lifecycle::Resume, 5, None);
    assert_eq!(
        command,
        "exec '/w/my dir/belt' 'worker-service' 'resume' '--wait-secs' '5'"
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
    let staging = home.join(".belt-bootstrap");
    assert!(!staging.join(format!("belt-{digest}")).exists());
    assert!(!staging.join(format!("belt-{digest}.part")).exists());
    // The whole transfer is published and probed as staged.
    let full = sh(&stage_command(&digest), home, &bytes);
    assert_eq!(full.status.code(), Some(0), "{:?}", full.stderr);
    assert_eq!(
        fs::read(staging.join(format!("belt-{digest}"))).unwrap(),
        bytes
    );
    assert!(probe(home).ends_with("staged\n"));
    // An executable with other content is not "staged".
    fs::write(staging.join(format!("belt-{digest}")), b"damaged").unwrap();
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
        coordinator_epoch: crate::ids::CoordinatorEpoch(0),
    };
    let served = cluster.clone();
    let server = tokio::spawn(async move {
        let _accepted = crate::protocol::accept(
            peer,
            crate::protocol::ServerConfig::new(
                served,
                identity,
                crate::group::Keys::only(crate::group::test_key()),
            ),
        )
        .await
        .unwrap();
        std::future::pending::<()>().await;
    });
    let hello = hello_on(parent, &cluster, crate::group::test_key(), None)
        .await
        .unwrap();
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
    assert!(
        hello_through(&argv, &cluster, crate::group::test_key(), None)
            .await
            .is_err()
    );
    assert!(hello_through(
        &["/nonexistent/ssh".to_string()],
        &cluster,
        crate::group::test_key(),
        None
    )
    .await
    .is_err());
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
        coordinator_epoch: crate::ids::CoordinatorEpoch(0),
    };
    let mut config = crate::protocol::ServerConfig::new(
        cluster.clone(),
        identity,
        crate::group::Keys::only(crate::group::test_key()),
    );
    config.local.features = features.iter().map(|f| f.to_string()).collect();
    let server = tokio::spawn(async move {
        if let Ok(_accepted) = crate::protocol::accept(peer, config).await {
            std::future::pending::<()>().await;
        }
    });
    (parent, server)
}

#[tokio::test]
async fn bridge_diagnostics_preserve_group_refusal_codes_without_key_material() {
    let cluster = ClusterId::generate();
    let features = crate::protocol::hello::SUPPORTED_FEATURES;
    let (io, server) = worker_offering(&cluster, features);
    let error = hello_on(io, &ClusterId::generate(), crate::group::test_key(), None)
        .await
        .unwrap_err();
    assert_eq!(error, "the worker bridge refused hello: group_mismatch");
    server.await.unwrap();
    let (io, server) = worker_offering(&cluster, features);
    let key = crate::secrets::key::ClusterKey::from_bytes(vec![42; 64]).unwrap();
    let error = hello_on(io, &cluster, key, None).await.unwrap_err();
    assert_eq!(error, "the worker bridge refused hello: group_key_mismatch");
    assert!(!error.contains(&"2a".repeat(64)));
    server.await.unwrap();
}

#[tokio::test]
async fn a_probe_requires_exactly_what_the_caller_lists() {
    let cluster = ClusterId::generate();
    // A version that lacks some of what this build offers.
    let older = ["log_resume", "blob_resume"];
    let lacking = crate::forecast::COMMAND_FEATURE.to_string();
    let (io, server) = worker_offering(&cluster, &older);
    let hello = hello_on(io, &cluster, crate::group::test_key(), None)
        .await
        .unwrap();
    assert_eq!(hello.worker.as_str(), "mini-a");
    // It says what it agreed to: nothing it lacks.
    assert!(hello.features.contains(&"log_resume".to_string()));
    assert!(!hello.features.contains(&lacking));
    server.abort();
    // Asked for nothing, or for what it has, it passes.
    let (io, server) = worker_offering(&cluster, &older);
    assert!(hello_on(io, &cluster, crate::group::test_key(), Some(&[]))
        .await
        .is_ok());
    server.abort();
    let (io, server) = worker_offering(&cluster, &older);
    let has = ["log_resume".to_string()];
    assert!(hello_on(io, &cluster, crate::group::test_key(), Some(&has))
        .await
        .is_ok());
    server.abort();
    // Asked for what it lacks, it is refused.
    let (io, server) = worker_offering(&cluster, &older);
    let error = hello_on(io, &cluster, crate::group::test_key(), Some(&[lacking]))
        .await
        .unwrap_err();
    assert!(error.contains("required features missing"), "{error}");
    server.abort();
}

#[test]
fn what_this_build_offers_and_requires_is_one_list() {
    let offered = crate::protocol::session::offered_features();
    let required = crate::protocol::session::required_features();
    assert_eq!(
        required,
        [
            crate::outputs::FEATURE,
            crate::group::MEMBERSHIP_FEATURE,
            crate::group::IDENTITY_RETIRED_FEATURE,
            crate::group::SHARED_ADMISSION_FEATURE
        ]
    );
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
            "belt".to_string(),
            Some(crate::workspace::embedded::BUILD_ID.to_owned()),
        )]),
        docker: Docker::NotRunnable,
        disk_free_bytes: None,
        profile: "default".into(),
        required_tools: Vec::new(),
        profile_digest: None,
        policy_digest: None,
        rustup: Vec::new(),
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
    assert_eq!(get("belt").class, envdiff::Class::Same);
    assert_ne!(get("git").class, envdiff::Class::Same);
    assert!(get("disk").worker.is_some());
    assert!(!diffs[0].all_same);
    // One fixed command: no worker name, no path from the caller.
    assert_eq!(w.remote.log.len(), 1);
    assert!(w.remote.log[0].contains("'worker-service' 'env'"));
    assert!(!w.remote.log[0].contains("mini-a"));
    let text = render_diff(&diffs);
    assert!(text.starts_with("worker mini-a\n"));
    assert!(text.contains("belt"));
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
        env_command(Some("/opt/belt/bin"), "abc"),
        "exec '/opt/belt/bin' 'worker-service' 'env'"
    );
    let staged = env_command(None, "abc");
    assert!(staged.contains("belt-abc"));
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
    register(&w.env, "mini-b", "ci@host-b", None, false).unwrap();
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
            err.message.contains("no version files or workflows found"),
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

    fn workflow(root: &Path, dir: &str, name: &str, text: &str) {
        fs::create_dir_all(root.join(dir)).unwrap();
        fs::write(root.join(dir).join(name), text).unwrap();
    }
    fn job(version: &str) -> String {
        format!(
            "name: t\non: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version: {version}\n"
        )
    }

    #[test]
    fn project_requirements_include_the_setup_inputs_of_the_workflows() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        // Only a workflow: its setup input is the requirement.
        workflow(root, ".github/workflows", "ci.yml", &job("20.1.0"));
        let reqs = read_project_requirements(root).unwrap();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].tool.as_str(), "node");
        // `.ci/workflows` is read as well, and a second version of the tool
        // is a requirement of its own.
        workflow(root, ".ci/workflows", "other.yaml", &job("18.2.0"));
        assert_eq!(read_project_requirements(root).unwrap().len(), 2);
        // Files that are not workflows are not read.
        workflow(root, ".ci/workflows", "notes.txt", "not yaml: [");
        assert_eq!(read_project_requirements(root).unwrap().len(), 2);
        // A version file the workflow names is read from the project.
        workflow(
            root,
            ".github/workflows",
            "file.yml",
            "name: t\non: push\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: .tool-node\n",
        );
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message.contains("missing_version_file"),
            "{}",
            err.message
        );
        fs::write(root.join(".tool-node"), "16.5.0\n").unwrap();
        assert_eq!(read_project_requirements(root).unwrap().len(), 3);
        // An unresolved expression stays an expression, for the plan to say.
        workflow(
            root,
            ".github/workflows",
            "m.yml",
            &job("${{ matrix.node }}"),
        );
        let reqs = read_project_requirements(root).unwrap();
        assert!(reqs.iter().any(|r| r
            .constraints
            .iter()
            .any(|c| matches!(c, crate::toolchain::versions::Constraint::Expression(_)))));
    }

    #[test]
    fn project_workflows_contradicting_the_project_files_fail_with_a_location() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join(".node-version"), "18.0.0\n").unwrap();
        // The workflow's own step is narrower than the project's file, and
        // they cannot both hold.
        workflow(root, ".github/workflows", "ci.yml", &job("20.1.0"));
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message.contains("conflicting_versions"),
            "{}",
            err.message
        );
        // A workflow that does not parse is said with its name.
        fs::remove_file(root.join(".node-version")).unwrap();
        workflow(root, ".github/workflows", "ci.yml", "jobs: [");
        let err = read_project_requirements(root).unwrap_err();
        assert!(err.message.contains("ci.yml"), "{}", err.message);
    }

    #[test]
    fn a_structured_version_file_of_a_setup_step_is_skipped_and_said() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join(".node-version"), "20.1.0\n").unwrap();
        fs::write(root.join("go.mod"), "module x\n\ngo 1.22\n").unwrap();
        let step = "name: t\non: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/setup-go@v5\n        with:\n          go-version-file: go.mod\n";
        workflow(root, ".github/workflows", "ci.yml", step);
        // `go.mod` is not read as a version: nothing is required of go, the
        // rest of the project is, and the line says which step.
        let (reqs, skipped) = read_project_setup(root).unwrap();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].tool.as_str(), "node");
        assert_eq!(
            skipped,
            [".github/workflows/ci.yml:7: go: the version file go.mod is not a plain version file; no go version is taken from this step"]
        );
        assert_eq!(read_project_requirements(root).unwrap(), reqs);
        // Nothing skipped, nothing said.
        fs::remove_dir_all(root.join(".github")).unwrap();
        assert_eq!(read_project_setup(root).unwrap(), (reqs, vec![]));
        // A version file that is named and missing is the error it was.
        workflow(root, ".github/workflows", "ci.yml", step);
        fs::remove_file(root.join("go.mod")).unwrap();
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message
                .contains("missing_version_file at .github/workflows/ci.yml:9"),
            "{}",
            err.message
        );
    }

    #[test]
    fn project_workflows_are_not_read_through_links() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        let outside = crate::test_tmp::tempdir().unwrap();
        fs::write(outside.path().join("ci.yml"), job("20.1.0")).unwrap();
        // A linked workflow directory is not followed: nothing is found.
        fs::create_dir_all(root.join(".github")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.join(".github/workflows")).unwrap();
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message.contains("no version files or workflows"),
            "{}",
            err.message
        );
        // A linked workflow file is refused.
        fs::remove_file(root.join(".github/workflows")).unwrap();
        fs::create_dir(root.join(".github/workflows")).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("ci.yml"),
            root.join(".github/workflows/ci.yml"),
        )
        .unwrap();
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message.contains("not a small regular file"),
            "{}",
            err.message
        );
        // Too many workflows.
        fs::remove_file(root.join(".github/workflows/ci.yml")).unwrap();
        for n in 0..=crate::toolchain::declarations::MAX_DOCUMENTS {
            fs::write(root.join(format!(".github/workflows/w{n}.yml")), "").unwrap();
        }
        let err = read_project_requirements(root).unwrap_err();
        assert!(
            err.message.contains("too many workflow files"),
            "{}",
            err.message
        );
    }
}

/// Hands out copies of itself, so the asks of `fan_out` run together; every
/// ask for an env measurement meets the others first, and is answered with
/// `reply`.
struct Together {
    gate: Option<std::sync::Arc<meeting::Gate>>,
    reply: Vec<u8>,
}
impl Remote for Together {
    fn split(&self) -> Option<Box<dyn Remote + Send>> {
        Some(Box::new(Together {
            gate: self.gate.clone(),
            reply: self.reply.clone(),
        }))
    }
    fn run(
        &mut self,
        _: &SshTarget,
        shell: &str,
        _: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        assert!(shell.contains("'worker-service' 'env'"), "{shell}");
        if let Some(gate) = &self.gate {
            gate.meet()?;
        }
        Ok(RemoteOutput {
            code: Some(0),
            stdout: self.reply.clone(),
            stderr: vec![],
        })
    }
    fn bridge_hello(
        &mut self,
        _: &SshTarget,
        _: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        Err("unused".into())
    }
}

#[test]
fn diff_of_every_worker_asks_them_at_the_same_time_and_keeps_the_order() {
    let w = World::new();
    for (name, host) in [
        ("mini-a", "ci@host-a"),
        ("mini-b", "ci@host-b"),
        ("mini-c", "ci@host-c"),
    ] {
        // Each entry is measured by Together below. Registering it needs no
        // service installation in the shared fixture home.
        register(&w.env, name, host, None, false).unwrap();
    }
    let mut measured = parent_report();
    measured.disk_free_bytes = Some(100 * 1024 * 1024 * 1024);
    let reply = format!("{}\n", serde_json::json!({"ok": true, "env": &measured}));
    let mut together = Together {
        // Nobody is answered before all three are asking: a serial run fails.
        gate: Some(meeting::Gate::new(3)),
        reply: reply.into_bytes(),
    };
    let all = diff_workers(&mut together, &w.env, &w.exe, None, || Ok(measured.clone())).unwrap();
    assert_eq!(
        all.iter().map(|d| d.worker.as_str()).collect::<Vec<_>>(),
        ["mini-a", "mini-b", "mini-c"]
    );
    assert!(
        all.iter().all(|d| d.error.is_none() && d.all_same),
        "{all:?}"
    );
    // A named worker is one ask: no gate of three is waited for.
    together.gate = None;
    let one = diff_workers(&mut together, &w.env, &w.exe, Some("mini-b"), || {
        Ok(measured)
    })
    .unwrap();
    assert_eq!(one.len(), 1);
}

/// Counts how many asks are inside `run` at once.
struct Counting {
    inside: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    most: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    splits: bool,
}
impl Remote for Counting {
    fn split(&self) -> Option<Box<dyn Remote + Send>> {
        self.splits.then(|| {
            Box::new(Counting {
                inside: self.inside.clone(),
                most: self.most.clone(),
                splits: true,
            }) as Box<dyn Remote + Send>
        })
    }
    fn run(
        &mut self,
        _: &SshTarget,
        _: &str,
        _: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.inside.fetch_add(1, SeqCst) + 1;
        self.most.fetch_max(now, SeqCst);
        std::thread::sleep(Duration::from_millis(40));
        self.inside.fetch_sub(1, SeqCst);
        Ok(RemoteOutput {
            code: Some(0),
            stdout: vec![],
            stderr: vec![],
        })
    }
    fn bridge_hello(
        &mut self,
        _: &SshTarget,
        _: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        Err("unused".into())
    }
}

#[test]
fn fan_out_is_bounded_ordered_and_serial_when_the_remote_cannot_split() {
    use std::sync::atomic::Ordering::SeqCst;
    let target = SshTarget::parse("ci@host").unwrap();
    let ask = |remote: &mut dyn Remote, n: usize| {
        remote
            .run(&target, "x", &[], Duration::from_secs(1))
            .unwrap();
        n
    };
    let counting = |splits| Counting {
        inside: Default::default(),
        most: Default::default(),
        splits,
    };
    let mut wide = counting(true);
    let got = fan_out(&mut wide, (0..20).collect(), &ask);
    assert_eq!(got, (0..20).collect::<Vec<_>>());
    let most = wide.most.load(SeqCst);
    assert!((2..=MAX_PARALLEL_WORKERS).contains(&most), "{most} at once");
    let mut serial = counting(false);
    assert_eq!(
        fan_out(&mut serial, (0..5).collect(), &ask),
        (0..5).collect::<Vec<_>>()
    );
    assert_eq!(serial.most.load(SeqCst), 1);
    // One item, and none, are not split.
    let mut one = counting(true);
    assert_eq!(fan_out(&mut one, vec![7], &ask), [7]);
    assert_eq!(one.most.load(SeqCst), 1);
    assert!(fan_out(&mut one, Vec::<usize>::new(), &ask).is_empty());
}

#[test]
fn both_runs_the_two_at_the_same_time_when_it_can_and_in_order_when_it_cannot() {
    use std::sync::atomic::Ordering::SeqCst;
    let target = SshTarget::parse("ci@host").unwrap();
    let ask = |remote: &mut dyn Remote| {
        remote
            .run(&target, "x", &[], Duration::from_secs(1))
            .unwrap();
    };
    for (splits, expected) in [(true, 2), (false, 1)] {
        let mut remote = Counting {
            inside: Default::default(),
            most: Default::default(),
            splits,
        };
        let most = remote.most.clone();
        let (a, b) = both(
            &mut remote,
            |r| {
                ask(r);
                1
            },
            |r| {
                ask(r);
                2
            },
        );
        assert_eq!((a, b), (1, 2));
        assert_eq!(most.load(SeqCst), expected, "splits: {splits}");
    }
}

/// The staging commands as a host's `/bin/sh` runs them, in a private HOME:
/// what the fakes above only recognize by their first words.
#[test]
fn the_staging_commands_run_in_a_real_shell() {
    use std::io::Write as _;
    use std::process::{Command, Stdio};
    let home = crate::test_tmp::tempdir().unwrap();
    let sh = |command: &str, stdin: &[u8]| {
        let mut child = Command::new("/bin/sh")
            .args(["-c", command])
            .env_clear()
            .env("HOME", home.path())
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(stdin).unwrap();
        let out = child.wait_with_output().unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
        )
    };
    // A script stands in for the binary: it is what `--version` runs.
    let bytes = b"#!/bin/sh\necho 'belt 9.9.9'\n";
    let source = home.path().join("source");
    fs::write(&source, bytes).unwrap();
    let digest = worker_service::sha256_file(&source).unwrap();
    let staged = home.path().join(format!(".belt-bootstrap/belt-{digest}"));
    let last = |text: &str| text.lines().last().unwrap_or("").to_string();

    let (code, probe) = sh(&probe_command(&digest), b"");
    assert_eq!((code, last(&probe).as_str()), (Some(0), "absent"));
    assert_eq!(probe.lines().count(), 3, "{probe}");
    // Bytes of another digest are refused and leave nothing behind.
    let (code, _) = sh(&stage_command(&digest), b"something else");
    assert_eq!(code, Some(3));
    assert!(!staged.exists());
    assert_eq!(fs::read_dir(staged.parent().unwrap()).unwrap().count(), 0);
    // The right bytes are published, executable and private.
    let (code, _) = sh(&stage_command(&digest), bytes);
    assert_eq!(code, Some(0));
    assert_eq!(fs::read(&staged).unwrap(), bytes);
    assert_eq!(
        fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let (code, probe) = sh(&probe_command(&digest), b"");
    assert_eq!((code, last(&probe).as_str()), (Some(0), "staged"));
    let parsed = target::parse_probe(&probe);
    assert!(parsed.staged && !parsed.system.is_empty() && !parsed.machine.is_empty());
    let (code, version) = sh(&version_command(&digest), b"");
    assert_eq!((code, version.as_str()), (Some(0), "belt 9.9.9\n"));
    // A copy whose content changed is not the staged binary.
    fs::write(&staged, b"tampered").unwrap();
    assert_eq!(last(&sh(&probe_command(&digest), b"").1), "absent");
    let (code, _) = sh(&unstage_command(&digest), b"");
    assert_eq!(code, Some(0));
    assert!(!staged.exists());
}

#[test]
fn a_preconfigured_system_worker_asks_for_the_system_manager_and_never_installs_a_user_service() {
    let mut w = World::new();
    register(&w.env, "mini-a", "ci@h", None, false).unwrap();
    let config = w.config_text() + "service_mode = \"system-user\"\n";
    fs::write(&w.config, &config).unwrap();
    // The host is an ordinary temp HOME with no administrator registration.
    assert!(w.add("mini-a", Some("ci@h"), true).is_err());
    assert!(w
        .remote
        .stdins
        .iter()
        .filter_map(|bytes| serde_json::from_slice::<InstallRequest>(bytes).ok())
        .any(|request| request.system_user));
    assert!(w.calls().is_empty());
    assert_eq!(w.config_text(), config);
    let paths = WorkerPaths::resolve(&w.remote.env).unwrap();
    assert!(!paths.services.exists());
}

// An installed, stopped service exercises enrollment without requiring a socket.
// Running resident reuse is covered by tests/it/init.rs.
fn stopped_member() -> World {
    let mut w = World::new();
    w.remote.fake.0.borrow_mut().fail_start = true;
    assert!(w.add("mini-a", Some("ci@h"), true).is_err());
    w.remote.fake.0.borrow_mut().fail_start = false;
    let paths = WorkerPaths::resolve(&w.remote.env).unwrap();
    let binding = paths.state.join("worker-binding.json");
    fs::write(
        &binding,
        serde_json::to_vec(&(
            1,
            w.spec.expected_cluster.as_ref().unwrap(),
            "mini-a",
            "key-digest",
        ))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&binding, fs::Permissions::from_mode(0o600)).unwrap();
    crate::storage::WorkerStore::open(&paths.state, &WorkerId::parse("mini-a").unwrap()).unwrap();
    register(
        &w.env,
        "mini-a",
        "ci@h",
        paths.bridge_binary().to_str(),
        false,
    )
    .unwrap();
    w
}

#[test]
fn membership_worker_only_replaces_caller_pin_and_keeps_service_and_binary() {
    use crate::ids::CoordinatorId;
    use crate::storage::WorkerStore;
    let mut w = stopped_member();
    let paths = WorkerPaths::resolve(&w.remote.env).unwrap();
    let target = WorkerStore::open(&paths.state, &WorkerId::parse("mini-a").unwrap()).unwrap();
    target
        .for_member(&CoordinatorId::generate())
        .replace_member_pin("local", 1)
        .unwrap();
    let input = fs::read(&paths.input).unwrap();
    let journal = fs::read(&paths.journal).unwrap();
    let calls = w.calls();
    let result = w.add("mini-a", Some("ci@h"), false).unwrap();
    assert!(result.status.as_ref().unwrap().coordinator_id.is_none());
    let caller = crate::storage::CoordinatorStore::open_read_only(&w.spec.state_dir).unwrap();
    assert_eq!(
        target.members().unwrap(),
        vec![("local".into(), caller.coordinator_id().to_string())]
    );
    assert!(!w.spec.worker_state_dir.join("worker.db").exists());
    assert_eq!(fs::read(&paths.input).unwrap(), input);
    assert_eq!(fs::read(&paths.journal).unwrap(), journal);
    assert_eq!(w.calls(), calls);
    assert_eq!(fs::read_dir(paths.services).unwrap().count(), 1);
    let key = fs::read_to_string(&w.spec.cluster_key_path).unwrap();
    assert!(w.remote.log.iter().all(|argv| !argv.contains(key.trim())));
    assert!(!format!("{result:?}").contains(key.trim()));
    assert!(!String::from_utf8_lossy(&w.remote.captured).contains(key.trim()));
    assert!(!String::from_utf8(journal).unwrap().contains(key.trim()));
}

#[test]
fn membership_other_group_requires_move_and_clears_old_authentication() {
    use crate::ids::CoordinatorId;
    use crate::storage::{KeyGeneration, WorkerStore};
    let mut w = stopped_member();
    let paths = WorkerPaths::resolve(&w.remote.env).unwrap();
    let target = WorkerStore::open(&paths.state, &WorkerId::parse("mini-a").unwrap()).unwrap();
    target
        .for_member(&CoordinatorId::generate())
        .pin_member("old-peer", 1)
        .unwrap();
    target.observe_group_key("old-current").unwrap();
    target
        .set_next_group_key(&KeyGeneration {
            generation: 2,
            fingerprint: "old-next".into(),
        })
        .unwrap();
    fs::write(crate::group::next_key_path(&paths.key), b"old next key").unwrap();
    let old = w.spec.expected_cluster.clone().unwrap();
    w.spec.expected_cluster = Some(ClusterId::generate());
    crate::storage::CoordinatorStore::move_group(
        &w.spec.state_dir,
        w.spec.expected_cluster.as_ref().unwrap(),
    )
    .unwrap();
    fs::write(&w.spec.cluster_key_path, format!("{}\n", "cd".repeat(64))).unwrap();
    let before = [
        fs::read(&paths.input).unwrap(),
        fs::read(&paths.journal).unwrap(),
        fs::read(&paths.key).unwrap(),
    ];
    let pins = target.members().unwrap();
    let error = w.add("mini-a", Some("ci@h"), true).unwrap_err();
    assert!(error.message.contains(old.as_str()));
    assert_eq!(target.members().unwrap(), pins);
    assert_eq!(
        [
            fs::read(&paths.input).unwrap(),
            fs::read(&paths.journal).unwrap(),
            fs::read(&paths.key).unwrap()
        ],
        before
    );
    let result = w
        .add_with(
            "mini-a",
            AddOptions {
                install: true,
                move_group: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        result.status.unwrap().cluster,
        w.spec.expected_cluster.clone().unwrap()
    );
    assert_eq!(target.members().unwrap().len(), 1);
    let (current, next) = target.group_keys().unwrap();
    assert_eq!(current.unwrap().generation, 1);
    assert!(next.is_none());
    assert!(!crate::group::next_key_path(&paths.key).exists());
    assert_eq!(fs::read_dir(paths.services).unwrap().count(), 1);
}

#[test]
fn membership_move_refuses_foreign_live_attempts_and_uncollected_results() {
    use crate::ids::CoordinatorId;
    use crate::storage::WorkerStore;
    for terminal in [false, true] {
        let mut w = stopped_member();
        let paths = WorkerPaths::resolve(&w.remote.env).unwrap();
        let target = WorkerStore::open(&paths.state, &WorkerId::parse("mini-a").unwrap()).unwrap();
        let db = rusqlite::Connection::open(paths.state.join("worker.db")).unwrap();
        db.execute("INSERT INTO worker_attempt (coordinator_id, attempt_id, run_id, task_id, generation, payload_hash, coordinator_epoch, spec_json, state, termination_json, receipts_json, accepted_at_ms, terminal_at_ms)
            VALUES (?1, 'attempt_test', 'run_test', 'task_test', 1, 'h', 1, '{}', ?2, ?3, '{}', 1, ?4)", rusqlite::params![CoordinatorId::generate().as_str(), if terminal { "terminal" } else { "active" }, terminal.then_some("{}"), terminal.then_some(2)]).unwrap();
        let before = fs::read(&paths.journal).unwrap();
        let pins = target.members().unwrap();
        let old = w.spec.expected_cluster.clone().unwrap();
        w.spec.expected_cluster = Some(ClusterId::generate());
        crate::storage::CoordinatorStore::move_group(
            &w.spec.state_dir,
            w.spec.expected_cluster.as_ref().unwrap(),
        )
        .unwrap();
        fs::write(&w.spec.cluster_key_path, format!("{}\n", "cd".repeat(64))).unwrap();
        let error = w
            .add_with(
                "mini-a",
                AddOptions {
                    install: true,
                    move_group: true,
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert!(error.message.contains("live work"), "{error:?}");
        assert!(error.message.contains(old.as_str()));
        assert_eq!(fs::read(&paths.journal).unwrap(), before);
        assert_eq!(target.members().unwrap(), pins);
        assert_eq!(w.calls(), ["bootstrap"]);
    }
}

mod rotate;

#[test]
fn removing_workers_preserves_full_subtables_and_rejects_ambiguous_split() {
    let text = "[[workers]]\nid = 'keep'\nssh = 'keep'\n[workers.extra]\na = 'kept'\n[[workers.jobs]]\nname = 'kept-job'\n[[workers]]\nid = 'remove'\nssh = 'remove'\n[workers.extra]\nb = 'removed'\n[[workers.jobs]]\nname = 'removed-job'\n";
    let edited = drop_worker_tables(text, &["remove".into()]).unwrap();
    assert!(edited.contains("kept-job"));
    assert!(!edited.contains("removed"));
    // A quoted subtable header is valid TOML but outside the splitter's grammar.
    let crafted = text.replace("[workers.extra]", "[workers . extra]");
    assert!(drop_worker_tables(&crafted, &["remove".into()]).is_err());
    let world = World::new();
    let path = user_config_path(&world.env).unwrap();
    fs::write(&path, &crafted).unwrap();
    assert!(remove_registrations(&world.env, &["remove".into()]).is_err());
    assert_eq!(fs::read_to_string(path).unwrap(), crafted);
}

#[test]
fn stop_checks_own_runs_and_old_peers_even_when_cancelling() {
    for active in [Some(true), None] {
        for cancel_running in [false, true] {
            let mut w = stopped_member();
            w.remote.own_runs_active = active;
            let before = w.remote.log.len();
            let error = w
                .lifecycle(
                    "mini-a",
                    Lifecycle::Stop {
                        cancel_running,
                        all_submitters: false,
                    },
                )
                .unwrap_err();
            assert!(
                error.message.contains(if active.is_none() {
                    "cannot tell"
                } else {
                    "own non-terminal runs"
                }),
                "{error:?}"
            );
            assert_eq!(w.remote.log.len(), before, "refused before remote stop");
            let status = w
                .lifecycle(
                    "mini-a",
                    Lifecycle::Stop {
                        cancel_running,
                        all_submitters: true,
                    },
                )
                .unwrap();
            assert_eq!(status.loaded, Some(false));
        }
    }
}

#[test]
fn stopped_bridge_requires_remote_missing_agent_diagnostic() {
    let message =
        b"error: worker agent is not running (cannot connect to /tmp/agent.sock): missing";
    assert!(stopped_bridge(
        Some(ErrorCode::NotFound.exit_code()),
        message
    ));
    assert!(!stopped_bridge(Some(255), message));
    assert!(!stopped_bridge(
        Some(ErrorCode::NotFound.exit_code()),
        b"permission denied"
    ));
}

#[test]
fn unreachable_stop_has_override_guidance() {
    let mut w = stopped_member();
    w.remote.reachable = false;
    let error = w
        .lifecycle(
            "mini-a",
            Lifecycle::Stop {
                cancel_running: false,
                all_submitters: false,
            },
        )
        .unwrap_err();
    assert!(error.message.contains("cannot be reached"));
    assert!(error.message.contains("--all-submitters"));
}
