//! The real binary's `workers` commands against a fake `ssh` placed first on
//! PATH. The fake never starts a remote helper, so no launchctl, LaunchAgent,
//! ~/.ssh or sudo is involved. Remote install/start/stop with a fake launcher
//! is covered in the library tests of the service modules.
use belt::error::ErrorCode;
use belt::ids::{ClusterId, WorkerId};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Output;

const BIN: &str = env!("CARGO_BIN_EXE_belt");

pub(super) struct Fx {
    dir: super::common::Tmp,
}
impl Fx {
    pub(super) fn new() -> Self {
        let dir = super::common::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("home")).unwrap();
        std::fs::create_dir(dir.path().join("bin")).unwrap();
        Fx { dir }
    }
    pub(super) fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.path().join(name)
    }
    /// An initialized coordinator: the retained input names the cluster, as
    /// `init` leaves it. Nothing is started.
    pub(super) fn init(&self) -> ClusterId {
        self.init_with_env(Default::default())
    }
    /// As `init`, with the service environment the coordinator retained.
    pub(super) fn init_with_env(
        &self,
        env: std::collections::BTreeMap<String, String>,
    ) -> ClusterId {
        use belt::service::bootstrap::{write_input, Spec};
        let dir = self.path("home/.config/belt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let private = |name: &str| {
            let path = self.path(name);
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
            path
        };
        belt::secrets::key::init(&self.path("cluster.key")).unwrap();
        std::fs::write(self.path("belt"), b"x").unwrap();
        std::fs::set_permissions(self.path("belt"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        let state_dir = private("coordinator");
        let cluster = belt::storage::CoordinatorStore::open(&state_dir, 1)
            .unwrap()
            .cluster_id()
            .clone();
        let spec = Spec {
            state_dir,
            worker_state_dir: private("local-worker"),
            worker_id: WorkerId::parse("local").unwrap(),
            cluster_key_path: self.path("cluster.key"),
            executable: self.path("belt"),
            env,
            repo_root: None,
            expected_cluster: Some(cluster.clone()),
            managed_service: None,
        };
        write_input(&dir.join("coordinator.json"), &spec).unwrap();
        cluster
    }
    pub(super) fn config(&self, text: &str) {
        let dir = self.path("home/.config/belt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("config.toml"), text).unwrap();
    }
    /// A fake ssh: records its argv (one per line) and runs `body`.
    pub(super) fn ssh(&self, body: &str) {
        let script = format!(
            "#!/bin/sh\nif [ -S \"$HOME/.belt/agent.sock\" ]; then case \"$*\" in *\"worker\"*\"--stdio\"*) exec '{BIN}' worker --stdio ;; esac; fi\nprintf '%s\\n' \"$@\" > {}\n{body}\n",
            self.path("argv").display()
        );
        let path = self.path("bin/ssh");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    pub(super) fn run(&self, args: &[&str]) -> Output {
        std::process::Command::new(BIN)
            .arg("--json")
            .args(args)
            .env_clear()
            .env("HOME", self.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.path("bin").display()),
            )
            .output()
            .unwrap()
    }
    /// A real authenticated protocol connection for the lifecycle preflight.
    fn inspect_peer(&self, cluster: &ClusterId, active: Option<bool>) -> tokio::runtime::Runtime {
        use belt::ids::*;
        use belt::protocol::{accept, Io, ServerConfig};
        use belt::wire::*;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let state = self.path("home/.belt");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
        let listener = belt::protocol::transport::listen_unix(&state.join("agent.sock")).unwrap();
        let key = belt::secrets::key::load(&self.path("cluster.key")).unwrap();
        let cluster = cluster.clone();
        runtime.spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let config = ServerConfig::new(
                    cluster.clone(),
                    super::proto_common::identity("mini-a"),
                    belt::group::Keys::only(key.clone()),
                );
                tokio::spawn(async move {
                    let accepted = accept(Io::from_stream(stream), config).await.unwrap();
                    super::proto_common::serve(
                        accepted.incoming,
                        std::sync::Arc::new(move |request| {
                            assert!(matches!(request, Request::Inspect(_)));
                            Some(Ok(Response::Inspect(InspectResponse {
                                own_runs_active: active,
                                preparation_observations: None,
                                capabilities: None,
                                schema_version: SchemaVersion::CURRENT,
                                worker_id: WorkerId::parse("mini-a").unwrap(),
                                worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
                                agent_epoch: AgentEpoch(1),
                                snapshot_digest: Digest::of_bytes(b"snapshot"),
                                policy_digest: Digest::of_bytes(b"policy"),
                                active_task: 0,
                                prefetch_pending: 0,
                                max_active_task: 1,
                                prefetch_per_worker: 1,
                                disk_free_bytes: 0,
                                draining: false,
                                admission: None,
                            })))
                        }),
                    );
                });
            }
        });
        drop(_guard);
        runtime
    }
    pub(super) fn argv(&self) -> Vec<String> {
        std::fs::read_to_string(self.path("argv"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

fn answer(cluster: &ClusterId, worker: &str, draining: bool, loaded: bool) -> String {
    format!(
        "printf '%s\\n' '{}'",
        serde_json::json!({"ok": true, "status": {
            "worker_id": WorkerId::parse(worker).unwrap(),
            "cluster": cluster,
            "binary_sha256": "0".repeat(64),
            "loaded": loaded, "ready": loaded, "draining": draining,
            "cancel_running": false, "live_attempts": 0
        }})
    )
}

pub(super) fn json(out: &Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout).expect("stdout is one JSON document")
}

#[test]
fn an_unregistered_worker_is_not_found_and_no_ssh_runs() {
    let fx = Fx::new();
    fx.init();
    fx.ssh("exit 0");
    for args in [
        &["workers", "drain", "ghost"][..],
        &["workers", "resume", "ghost"],
        &["workers", "start", "ghost"],
        &["workers", "stop", "ghost", "--cancel-running"],
    ] {
        let out = fx.run(args);
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::NotFound.exit_code()),
            "{args:?}"
        );
        assert_eq!(json(&out)["error"]["code"], "NOT_FOUND");
    }
    assert!(!fx.path("argv").exists());
}

#[test]
fn drain_runs_one_fixed_ssh_command_and_reports_the_helper_answer() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\n");
    let cluster = fx.init();
    fx.ssh(&answer(&cluster, "mini-a", true, true));
    let out = fx.run(&["workers", "drain", "mini-a"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = json(&out);
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"]["draining"], true);
    let argv = fx.argv();
    let at = argv
        .iter()
        .position(|a| a == "--")
        .expect("destination follows --");
    assert_eq!(argv[at + 1], "ci@mini-a.local");
    assert_eq!(argv.len(), at + 3, "one remote command word: {argv:?}");
    assert!(argv[at + 2].contains("'worker-service' 'drain'"));
    assert!(!argv[at + 2].contains("mini-a"));
    assert!(argv.iter().any(|a| a == "BatchMode=yes"));
}

#[test]
fn stop_with_cancel_running_is_requested_explicitly() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    let cluster = fx.init();
    let _peer = fx.inspect_peer(&cluster, Some(false));
    fx.ssh(&answer(&cluster, "mini-a", false, false));
    assert_eq!(
        fx.run(&["workers", "stop", "mini-a"]).status.code(),
        Some(0)
    );
    assert!(!fx.argv().last().unwrap().contains("--cancel-running"));
    assert_eq!(
        fx.run(&["workers", "stop", "mini-a", "--cancel-running"])
            .status
            .code(),
        Some(0)
    );
    assert!(fx.argv().last().unwrap().contains("'--cancel-running'"));
}

#[test]
fn an_unreachable_worker_is_never_reported_as_stopped() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    fx.init();
    fx.ssh("exit 255");
    for args in [
        &["workers", "stop", "mini-a"][..],
        &["workers", "drain", "mini-a"],
        &["workers", "start", "mini-a"],
        &["workers", "resume", "mini-a"],
    ] {
        let out = fx.run(args);
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::CommunicationUnknown.exit_code()),
            "{args:?}"
        );
        let value = json(&out);
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "COMMUNICATION_UNKNOWN");
    }
}

#[test]
fn a_silent_ssh_success_is_not_a_success() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    fx.init();
    fx.ssh("exit 0");
    let out = fx.run(&["workers", "stop", "mini-a"]);
    assert_eq!(
        out.status.code(),
        Some(ErrorCode::CommunicationUnknown.exit_code())
    );
}

#[test]
fn add_needs_an_initialized_coordinator_and_changes_nothing_without_it() {
    let fx = Fx::new();
    fx.ssh("exit 0");
    let out = fx.run(&["workers", "add", "mini-a", "--ssh", "ci@h", "--install"]);
    assert_eq!(out.status.code(), Some(ErrorCode::NotFound.exit_code()));
    assert!(!fx.path("argv").exists());
    assert!(!Path::new(&fx.path("home/.config/belt/config.toml")).exists());
}

#[test]
fn text_output_goes_to_stdout_on_success_and_stderr_on_failure() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    let cluster = fx.init();
    fx.ssh(&answer(&cluster, "mini-a", true, true));
    let run = |args: &[&str]| {
        std::process::Command::new(BIN)
            .args(args)
            .env_clear()
            .env("HOME", fx.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", fx.path("bin").display()),
            )
            .output()
            .unwrap()
    };
    let ok = run(&["workers", "drain", "mini-a"]);
    assert_eq!(
        String::from_utf8_lossy(&ok.stdout).trim(),
        "worker mini-a draining"
    );
    let bad = run(&["workers", "drain", "ghost"]);
    assert!(bad.stdout.is_empty() && !bad.stderr.is_empty());
}

#[test]
fn an_answer_that_does_not_show_the_requested_state_is_not_reported_as_success() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    let cluster = fx.init();
    let _peer = fx.inspect_peer(&cluster, Some(false));
    // The host says the service is still loaded after stop, and not draining after drain.
    fx.ssh(&answer(&cluster, "mini-a", false, true));
    for args in [
        &["workers", "stop", "mini-a"][..],
        &["workers", "drain", "mini-a"],
    ] {
        let out = fx.run(args);
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::Conflict.exit_code()),
            "{args:?}"
        );
        assert_eq!(json(&out)["error"]["code"], "CONFLICT");
    }
    // An answer from another cluster is refused outright.
    fx.ssh(&answer(&ClusterId::generate(), "mini-a", true, true));
    let out = fx.run(&["workers", "drain", "mini-a"]);
    assert_eq!(out.status.code(), Some(ErrorCode::Conflict.exit_code()));
}

mod a44 {
    use super::*;
    use belt::toolchain::envdiff::{Docker, EnvReport, Managed, Observed, ToolFact};
    use std::collections::BTreeMap;

    fn shim(dir: &Path, name: &str, body: &str) {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The parent is measured through its real profile, which runs only fake
    /// tools; the worker is whatever the fake ssh prints.
    fn fixture() -> Fx {
        let fx = Fx::new();
        fx.init();
        let tools = fx.path("tools");
        std::fs::create_dir(&tools).unwrap();
        shim(&tools, "node", "echo v20.1.0");
        shim(&tools, "xcodebuild", "echo 'Xcode 16.0'");
        let sdk = fx.path("sdk");
        std::fs::create_dir(&sdk).unwrap();
        shim(&tools, "xcrun", &format!("printf '%s' '{}'", sdk.display()));
        shim(&tools, "docker", "echo 27.0.1");
        let profiles = fx.path("local-worker/profiles.toml");
        std::fs::write(
            &profiles,
            format!(
                "schema_version=1\nactive='t'\n[profiles.t]\npath=[{:?}]\nshell='/bin/sh'\ncache_namespace='t'\n",
                std::fs::canonicalize(&tools).unwrap().to_str().unwrap()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&profiles, std::fs::Permissions::from_mode(0o600)).unwrap();
        fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\n");
        fx
    }

    fn worker_report() -> EnvReport {
        let tool = |observed, version: Option<&str>, managed| ToolFact {
            observed,
            version: version.map(str::to_string),
            managed,
        };
        EnvReport {
            schema_version: 1,
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            tools: BTreeMap::from([
                (
                    "node".to_string(),
                    tool(
                        Observed::Available,
                        Some("18.0.0"),
                        Some(Managed {
                            inventory: vec!["18.0.0".into(), "20.1.0".into()],
                            installed: vec!["18.0.0".into()],
                        }),
                    ),
                ),
                ("xcode".to_string(), tool(Observed::Unavailable, None, None)),
            ]),
            helpers: BTreeMap::from([(
                "belt".to_string(),
                Some(belt::workspace::embedded::BUILD_ID.to_owned()),
            )]),
            docker: Docker::NotRunnable,
            disk_free_bytes: Some(100 << 30),
            profile: "t".into(),
            required_tools: Vec::new(),
            profile_digest: Some("sha256:worker".into()),
            policy_digest: None,
        }
    }

    fn answer(report: &EnvReport) -> String {
        format!(
            "printf '%s\\n' '{}'",
            serde_json::json!({"ok": true, "env": report})
        )
    }

    fn has_item(value: &serde_json::Value, subject: &str) -> bool {
        value["result"]["workers"][0]["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["subject"] == subject)
    }

    fn item(value: &serde_json::Value, subject: &str) -> serde_json::Value {
        value["result"]["workers"][0]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["subject"] == subject)
            .unwrap_or_else(|| panic!("no item {subject}"))
            .clone()
    }

    #[test]
    fn diff_classifies_the_five_ways_a_worker_can_differ() {
        let fx = fixture();
        fx.ssh(&answer(&worker_report()));
        let out = fx.run(&["workers", "diff", "mini-a"]);
        assert_eq!(
            out.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        let value = json(&out);
        assert_eq!(value["ok"], true);
        assert_eq!(value["result"]["workers"][0]["worker"], "mini-a");
        assert_eq!(value["result"]["workers"][0]["all_same"], false);
        // A version the worker's manager may install: automatic.
        assert_eq!(item(&value, "node")["class"], "auto_installable");
        assert_eq!(item(&value, "node")["parent"], "20.1.0");
        assert_eq!(item(&value, "node")["worker"], "18.0.0");
        // Never installed automatically.
        let xcode = item(&value, "xcode");
        assert_eq!(xcode["class"], "manual_required");
        assert!(xcode["steps"].as_str().unwrap().contains("license"));
        let docker = item(&value, "docker");
        assert_eq!(docker["class"], "manual_required");
        assert!(docker["steps"].as_str().unwrap().contains("Colima"));
        // The built-in tools are compared by this binary's build, not by an
        // external `--version`.
        assert_eq!(item(&value, "belt")["class"], "same");
        assert!(!has_item(&value, "local-ci"));
        assert!(!has_item(&value, "task-traffic-controller"));
        assert_eq!(item(&value, "platform")["class"], "same");
        // Nothing measures a policy digest yet; the profile digest differs by
        // host and is only shown.
        assert_eq!(item(&value, "policy_digest")["class"], "not_measured");
        assert_eq!(item(&value, "profile")["class"], "same");
        assert_eq!(item(&value, "disk")["class"], "same");
        // One ssh call, a fixed helper command.
        let argv = fx.argv().join(" ");
        assert!(argv.contains("worker-service"), "{argv}");
    }

    #[test]
    fn diff_of_every_worker_and_plain_text() {
        let fx = fixture();
        fx.ssh(&answer(&worker_report()));
        let out = std::process::Command::new(BIN)
            .args(["workers", "diff"])
            .env_clear()
            .env("HOME", fx.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", fx.path("bin").display()),
            )
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(0));
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.starts_with("worker mini-a\n"), "{text}");
        assert!(text.contains("auto_installable"), "{text}");
        assert!(text.contains("manual_required"), "{text}");
    }

    #[test]
    fn a_worker_that_cannot_answer_is_an_error_never_an_empty_difference() {
        let fx = fixture();
        fx.ssh("echo \"error: invalid value 'env'\" >&2; exit 2");
        let out = fx.run(&["workers", "diff", "mini-a"]);
        assert_eq!(out.status.code(), Some(ErrorCode::Conflict.exit_code()));
        assert_eq!(json(&out)["error"]["code"], "CONFLICT");
        fx.ssh("exit 255");
        let out = fx.run(&["workers", "diff", "mini-a"]);
        assert_eq!(out.status.code(), Some(ErrorCode::Conflict.exit_code()));
        let out = fx.run(&["workers", "diff", "ghost"]);
        assert_eq!(out.status.code(), Some(ErrorCode::NotFound.exit_code()));
    }

    #[test]
    fn sync_blocks_what_needs_a_person_with_the_steps() {
        let fx = fixture();
        // The worker's manager has node 20.1.0 installed; only xcode, docker
        // and the helpers differ.
        let mut report = worker_report();
        report.tools.insert(
            "node".into(),
            ToolFact {
                observed: Observed::Available,
                version: Some("20.1.0".into()),
                managed: None,
            },
        );
        fx.ssh(&answer(&report));
        let out = fx.run(&["workers", "sync", "mini-a"]);
        assert_eq!(out.status.code(), Some(ErrorCode::Blocked.exit_code()));
        let value = json(&out);
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "BLOCKED");
        assert_eq!(value["error"]["blocked_reason"], "environment_mismatch");
        let message = value["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("sudo xcodebuild -license accept"),
            "{message}"
        );
        assert!(message.contains("Colima"), "{message}");
        assert!(!message.contains("task-traffic-controller"), "{message}");
        assert!(!message.contains("node"), "{message}");
    }

    #[test]
    fn sync_without_a_resident_installs_nothing_starts_none_and_says_so() {
        // The real command: the worker lacks node 20.1.0, its manager may
        // install it, and no resident runs to carry the request.
        let fx = fixture();
        fx.ssh(&answer(&worker_report()));
        let out = fx.run(&["workers", "sync", "mini-a"]);
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::CommunicationUnknown.exit_code())
        );
        let message = json(&out)["error"]["message"].as_str().unwrap().to_string();
        assert!(message.contains("the resident is not running"), "{message}");
        assert!(!message.contains("in sync"), "{message}");
    }

    #[test]
    fn sync_with_a_project_follows_its_version_files() {
        let fx = fixture();
        let project = fx.path("project");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(project.join(".node-version"), "18.0.0\n").unwrap();
        // The worker already runs 18.0.0: the project is satisfied for node.
        fx.ssh(&answer(&worker_report()));
        let out = fx.run(&[
            "workers",
            "sync",
            "mini-a",
            "--project",
            project.to_str().unwrap(),
        ]);
        let message = json(&out)["error"]["message"].as_str().unwrap().to_string();
        assert!(!message.contains("node"), "{message}");
        // A project that is not a directory is refused before any ssh.
        let out = fx.run(&["workers", "sync", "mini-a", "--project", "/no/such/dir"]);
        assert_eq!(out.status.code(), Some(ErrorCode::InvalidInput.exit_code()));
    }
}

#[test]
fn workers_a54_stop_refuses_own_runs_and_old_peers_unless_all_submitters() {
    for active in [Some(true), None] {
        let fx = Fx::new();
        fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
        let cluster = fx.init();
        let _peer = fx.inspect_peer(&cluster, active);
        fx.ssh(&answer(&cluster, "mini-a", false, false));
        for cancel in [false, true] {
            let mut args = vec!["workers", "stop", "mini-a"];
            if cancel {
                args.push("--cancel-running");
            }
            let out = fx.run(&args);
            assert_eq!(
                out.status.code(),
                Some(ErrorCode::Conflict.exit_code()),
                "{}",
                String::from_utf8_lossy(&out.stdout)
            );
            let value = json(&out);
            assert!(value["error"]["message"]
                .as_str()
                .unwrap()
                .contains(if active.is_none() {
                    "cannot tell"
                } else {
                    "own non-terminal runs"
                }));
            assert!(!fx.path("argv").exists(), "no stop helper was invoked");
            args.push("--all-submitters");
            assert!(fx.run(&args).status.success());
            assert!(fx.argv().last().unwrap().contains("'--all-submitters'"));
            std::fs::remove_file(fx.path("argv")).unwrap();
        }
    }
}

#[test]
fn workers_stop_already_stopped_and_unreachable_are_distinct() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    let cluster = fx.init();
    fx.ssh(&format!(
        "case \"$*\" in *\"worker\"*\"--stdio\"*) exec '{BIN}' worker --stdio ;; esac\n{}",
        answer(&cluster, "mini-a", false, false)
    ));
    let out = fx.run(&["workers", "stop", "mini-a"]);
    assert!(
        out.status.success(),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fx.argv().last().unwrap().contains("'stop'"));
    fx.ssh("echo 'SSH unreachable' >&2; exit 255");
    let out = fx.run(&["workers", "stop", "mini-a"]);
    assert!(!out.status.success());
    let value = json(&out);
    let message = value["error"]["message"].as_str().unwrap();
    assert!(message.contains("cannot be reached"), "{message}");
    assert!(message.contains("--all-submitters"), "{message}");
}
