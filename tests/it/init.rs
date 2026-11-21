//! Process composition only: no launchctl invocation or real LaunchAgents path.
use belt::{
    service::{
        bootstrap,
        init::{initialize, ContextInput},
        macos::{policy::Plan, LaunchError, Launcher, LoadedState, Receipt},
        resident::{Client, Operation, Reply},
    },
    storage::CoordinatorStore,
};
use std::{
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct State {
    child: Option<Child>,
    starts: usize,
    manual: Vec<Child>,
    stderr: Vec<std::fs::File>,
}
impl State {
    fn stderr(&mut self) -> Stdio {
        let file = tempfile::tempfile().unwrap();
        let output = file.try_clone().unwrap();
        self.stderr.push(file);
        Stdio::from(output)
    }
    fn diagnose(&mut self) {
        for child in self.manual.iter_mut().chain(self.child.iter_mut()) {
            eprintln!(
                "owned daemon pid={} status={:?}",
                child.id(),
                child.try_wait()
            );
        }
        for file in &mut self.stderr {
            let bytes = file.metadata().map(|m| m.len()).ok();
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut prefix = Vec::new();
            let _ = file.take(4096).read_to_end(&mut prefix);
            let text = String::from_utf8_lossy(&prefix);
            // Never print arbitrary child output: it can include paths or inputs.
            let classes: Vec<_> = [
                "panicked",
                "Resource temporarily unavailable",
                "Too many open files",
                "startup lock",
                "identity",
                "OS boot identity unavailable",
                "database",
                "permission",
            ]
            .into_iter()
            .filter(|class| text.contains(class))
            .collect();
            eprintln!("owned daemon stderr bytes={bytes:?} classes={classes:?}");
        }
    }
}
impl Drop for State {
    fn drop(&mut self) {
        for child in &mut self.manual {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
struct OwnedLauncher(Arc<Mutex<State>>);
impl Launcher for OwnedLauncher {
    fn running_identity(
        &mut self,
        _: &Plan,
        _: &Receipt,
    ) -> Result<belt::service::macos::RunningIdentity, LaunchError> {
        let state = self.0.lock().unwrap();
        let pid = state
            .child
            .as_ref()
            .ok_or_else(|| LaunchError::NotApplied("no owned child".into()))?
            .id();
        belt::service::macos::RunningIdentity::observe(pid)
            .map_err(|_| LaunchError::NotApplied("owned child identity unknown".into()))
    }

    fn loaded_state(&mut self, _: &Plan, _: &Receipt) -> LoadedState {
        if self.0.lock().unwrap().child.is_some() {
            LoadedState::Match
        } else {
            LoadedState::Absent
        }
    }
    fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
        Ok(())
    }
    fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
        assert_eq!(args[0], "bootstrap");
        let plist = std::fs::read_to_string(&args[2]).unwrap();
        assert!(plist.contains("<key>RunAtLoad</key><true/>"));
        assert!(!plist.contains("SECRET_TOKEN"));
        // Read only the input selected by this test's private config path, never shell parsing.
        start_owned(&self.0, &self_input(&args[2]));
        Ok(())
    }
}
/// Start the daemon the managed file names, as the service manager would.
fn start_owned(state: &Mutex<State>, input: &std::path::Path) {
    let spec = bootstrap::read_input(input).unwrap();
    let mut c = test_command(&spec.executable);
    c.args(bootstrap::daemon_argv(input).unwrap())
        .envs(&spec.env)
        .current_dir(spec.repo_root.as_ref().unwrap())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut state = state.lock().unwrap();
    c.stderr(state.stderr());
    assert!(state.child.is_none());
    state.child = Some(c.spawn().unwrap());
    state.starts += 1;
}
/// A systemd user manager that owns one child: no `systemctl` runs.
struct OwnedSystemd(Arc<Mutex<State>>, std::path::PathBuf);
impl Launcher for OwnedSystemd {
    fn backend(&self) -> belt::service::macos::policy::Backend {
        belt::service::macos::policy::Backend::Systemd
    }
    fn running_identity(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
    ) -> Result<belt::service::macos::RunningIdentity, LaunchError> {
        OwnedLauncher(self.0.clone()).running_identity(plan, receipt)
    }
    fn loaded_state(&mut self, plan: &Plan, receipt: &Receipt) -> LoadedState {
        OwnedLauncher(self.0.clone()).loaded_state(plan, receipt)
    }
    fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
        Ok(())
    }
    fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
        assert_eq!((args[0].as_str(), args[1].as_str()), ("--user", "start"));
        // The unit is where systemd reads a user's units, under this test's root.
        let unit =
            std::fs::read_to_string(self.1.join("config/systemd/user").join(&args[2])).unwrap();
        assert!(unit.contains("Restart=always") && unit.contains("WantedBy=default.target"));
        assert!(!unit.contains("SECRET_TOKEN"));
        start_owned(&self.0, &self.1.join("config/belt/coordinator.json"));
        Ok(())
    }
}
// Instrumented child processes keep profiles outside the private service HOME.
// This belongs only to the test harness, never the managed service environment.
fn test_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut command = Command::new(program);
    command.env_clear().env("TOKIO_WORKER_THREADS", "2");
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        command.env("LLVM_PROFILE_FILE", profile);
    }
    command
}
fn self_input(plist: &str) -> std::path::PathBuf {
    // HOME/Library/LaunchAgents/<label>.plist -> HOME/config/belt/coordinator.json
    std::path::Path::new(plist)
        .ancestors()
        .nth(3)
        .unwrap()
        .join("config/belt/coordinator.json")
}
fn context(root: &std::path::Path) -> ContextInput {
    ContextInput {
        env: BTreeMap::from([
            ("HOME".into(), root.to_str().unwrap().into()),
            (
                "XDG_CONFIG_HOME".into(),
                root.join("config").to_str().unwrap().into(),
            ),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("SECRET_TOKEN".into(), "never persist".into()),
        ]),
        executable: env!("CARGO_BIN_EXE_belt").into(),
    }
}
#[test]
fn concurrent_init_reuses_one_real_combined_resident_and_epoch() {
    let root = crate::test_tmp::tempdir().unwrap();
    let context = context(root.path());
    let state = Arc::new(Mutex::new(State::default()));
    let mut threads = vec![];
    for _ in 0..2 {
        let context = context.clone();
        let state = state.clone();
        threads.push(std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(initialize(context, &mut OwnedLauncher(state.clone())))
                .unwrap_or_else(|error| {
                    state.lock().unwrap().diagnose();
                    panic!("initialization failed: {error}");
                })
        }));
    }
    let a = threads.remove(0).join().unwrap();
    let b = threads.remove(0).join().unwrap();
    assert_eq!(a.epoch, b.epoch);
    assert_eq!(a.worker_id, b.worker_id);
    assert_eq!(a.cluster_id, b.cluster_id);
    assert_eq!(state.lock().unwrap().starts, 1);
    let input = root.path().join("config/belt/coordinator.json");
    let spec = bootstrap::read_input(&input).unwrap();
    assert_eq!(spec.worker_state_dir, root.path().join(".belt"));
    assert!(spec.worker_state_dir.join("worker.db").is_file());
    assert!(!root.path().join(".local/state/belt/local-worker").exists());
    let group = test_command(env!("CARGO_BIN_EXE_belt"))
        .args(["group", "--json"])
        .envs(&context.env)
        .output()
        .unwrap();
    assert!(
        group.status.success(),
        "{}",
        String::from_utf8_lossy(&group.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&group.stdout).unwrap();
    assert_eq!(report["result"]["group_id"], a.cluster_id.to_string());
    assert_eq!(report["result"]["name"], a.worker_id.to_string());
    assert_eq!(report["result"]["current"]["number"], 1);
    assert!(report["result"]["next"].is_null());
    assert_eq!(report["result"]["members"], serde_json::json!([]));
    let key = std::fs::read_to_string(&spec.cluster_key_path).unwrap();
    assert!(!String::from_utf8_lossy(&group.stdout).contains(key.trim()));
    assert!(!String::from_utf8_lossy(&group.stderr).contains(key.trim()));
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(store.epoch(), a.epoch);
    drop(store);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        assert!(matches!(
            bootstrap::health(&spec).await.unwrap(),
            Reply::Health { ready: true, .. }
        ));
        let mut wrong_worker = spec.clone();
        wrong_worker.worker_id = belt::ids::WorkerId::parse("foreign-worker").unwrap();
        assert!(bootstrap::health(&wrong_worker).await.is_err());
        let mut wrong_cluster = spec.clone();
        wrong_cluster.expected_cluster = Some(belt::ids::ClusterId::generate());
        assert!(bootstrap::health(&wrong_cluster).await.is_err());
        let client = Client::load(&spec.state_dir, a.cluster_id).unwrap();
        assert!(matches!(
            client.request(Operation::Shutdown).await.unwrap(),
            Reply::Stopping
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.socket_path().exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    });
    let mut guard = state.lock().unwrap();
    assert!(guard.child.take().unwrap().wait().unwrap().success());
    drop(guard);
    // An automatic service restart must refuse a different database before epoch claim.
    std::fs::rename(
        spec.state_dir.join("coordinator.db"),
        spec.state_dir.join("original.db"),
    )
    .unwrap();
    let foreign = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
    let foreign_id = foreign.cluster_id().clone();
    let foreign_epoch = foreign.epoch();
    drop(foreign);
    let out = test_command(&spec.executable)
        .args(bootstrap::daemon_argv(&input).unwrap())
        .envs(&spec.env)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let after = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(after.cluster_id(), &foreign_id);
    assert_eq!(after.epoch(), foreign_epoch);
}
/// On Linux `init` goes to the systemd user manager. With a runtime
/// directory that has no manager behind it, the real `systemctl` (or its
/// absence) can only fail to answer: the command reaches the service
/// backend, is not "not implemented", reports no success and starts nothing.
#[cfg(target_os = "linux")]
#[test]
fn linux_public_init_reaches_the_systemd_backend_and_claims_nothing_without_a_manager() {
    let root = crate::test_tmp::tempdir().unwrap();
    let runtime = root.path().join("run");
    std::fs::create_dir(&runtime).unwrap();
    let out = test_command(env!("CARGO_BIN_EXE_belt"))
        .args(["--json", "init"])
        .env("HOME", root.path())
        .env("PATH", "/usr/bin:/bin")
        .env("XDG_RUNTIME_DIR", &runtime)
        .env(
            "DBUS_SESSION_BUS_ADDRESS",
            format!("unix:path={}", runtime.join("bus").display()),
        )
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert_ne!(out.status.code(), Some(70));
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["ok"], false);
    assert_ne!(value["error"]["code"], "NOT_IMPLEMENTED");
    assert!(value["error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("init: "));
    // The Linux form was prepared, and no LaunchAgent directory was made.
    assert!(root.path().join(".config/systemd/user").is_dir());
    assert!(!root.path().join("Library").exists());
}

/// The whole of `init` with a systemd manager that owns one child, on every
/// OS: the unit is written where systemd reads it, the daemon it names
/// becomes the registered resident, and running `init` again starts nothing.
#[test]
fn init_with_a_systemd_manager_registers_one_real_combined_resident() {
    let root = crate::test_tmp::tempdir().unwrap();
    let state = Arc::new(Mutex::new(State::default()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut manager = OwnedSystemd(state.clone(), root.path().to_owned());
    let run = |manager: &mut OwnedSystemd| {
        runtime
            .block_on(initialize(context(root.path()), manager))
            .unwrap_or_else(|error| {
                state.lock().unwrap().diagnose();
                panic!("initialization failed: {error}");
            })
    };
    let first = run(&mut manager);
    assert!(first.initialized);
    let again = run(&mut manager);
    assert_eq!(
        (first.cluster_id, first.worker_id, first.epoch),
        (again.cluster_id, again.worker_id, again.epoch)
    );
    assert_eq!(state.lock().unwrap().starts, 1);
    let units: Vec<_> = std::fs::read_dir(root.path().join("config/systemd/user"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".service"))
        .collect();
    assert_eq!(units.len(), 1, "{units:?}");
    assert!(!root.path().join("Library").exists());
    // The daemon checked its own managed registration from the journal, and
    // answers as the registered resident.
    let input = root.path().join("config/belt/coordinator.json");
    let spec = bootstrap::read_input(&input).unwrap();
    runtime.block_on(async {
        assert!(matches!(
            bootstrap::health(&spec).await.unwrap(),
            Reply::Health { ready: true, .. }
        ));
    });
}

#[test]
fn manual_resident_and_failed_registered_daemon_are_not_initialized() {
    struct Competing(Arc<Mutex<State>>);
    impl Launcher for Competing {
        fn loaded_state(&mut self, _: &Plan, _: &Receipt) -> LoadedState {
            if self.0.lock().unwrap().child.is_some() {
                LoadedState::Match
            } else {
                LoadedState::Absent
            }
        }
        fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
            Ok(())
        }
        fn running_identity(
            &mut self,
            p: &Plan,
            r: &Receipt,
        ) -> Result<belt::service::macos::RunningIdentity, LaunchError> {
            OwnedLauncher(self.0.clone()).running_identity(p, r)
        }
        fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
            assert_eq!(args[0], "bootstrap");
            let input = self_input(&args[2]);
            let spec = bootstrap::read_input(&input).unwrap();
            let spawn = || {
                let stderr = self.0.lock().unwrap().stderr();
                test_command(&spec.executable)
                    .args(bootstrap::daemon_argv(&input).unwrap())
                    .envs(&spec.env)
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(stderr)
                    .spawn()
                    .unwrap()
            };
            let manual = spawn();
            self.0.lock().unwrap().manual.push(manual);
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !spec.state_dir.join("parent/owner.token").exists() {
                if std::time::Instant::now() >= deadline {
                    self.0.lock().unwrap().diagnose();
                    panic!("owned daemon did not create owner token");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let registered = spawn();
            let mut state = self.0.lock().unwrap();
            state.child = Some(registered);
            state.starts += 1;
            Ok(())
        }
    }
    let root = crate::test_tmp::tempdir().unwrap();
    let context = context(root.path());
    let state = Arc::new(Mutex::new(State::default()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let error = runtime
        .block_on(initialize(context, &mut Competing(state.clone())))
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("service identity changed after health"));
    let spec = bootstrap::read_input(&root.path().join("config/belt/coordinator.json")).unwrap();
    runtime.block_on(async {
        let health = bootstrap::health(&spec).await.unwrap();
        let Reply::Health {
            cluster_id,
            process,
            ..
        } = health
        else {
            panic!("manual owner health")
        };
        assert_eq!(process.pid, state.lock().unwrap().manual[0].id());
        let client = Client::load(&spec.state_dir, cluster_id).unwrap();
        assert!(matches!(
            client.request(Operation::Shutdown).await.unwrap(),
            Reply::Stopping
        ));
    });
    let mut guard = state.lock().unwrap();
    assert!(!guard.child.take().unwrap().wait().unwrap().success());
    assert!(guard.manual.pop().unwrap().wait().unwrap().success());
}

#[test]
fn workers_add_replaces_both_member_pins_without_a_second_resident() {
    use belt::ids::{CoordinatorId, WorkerId};
    use belt::service::{worker_service, workers};
    use belt::storage::WorkerStore;
    use std::path::PathBuf;
    struct Helper {
        env: BTreeMap<String, String>,
        state: Arc<Mutex<State>>,
        exe: PathBuf,
        captured: Vec<u8>,
        argv: Vec<String>,
    }
    impl workers::Remote for Helper {
        fn run(
            &mut self,
            _: &belt::protocol::ssh::SshTarget,
            shell: &str,
            stdin: &[u8],
            _: Duration,
        ) -> Result<workers::RemoteOutput, String> {
            self.argv.push(shell.into());
            assert!(shell.contains("'worker-service' 'install'"));
            let status = worker_service::handle(
                worker_service::Action::Install,
                &self.env,
                stdin,
                &self.exe,
                OwnedLauncher(self.state.clone()),
                worker_service::Options {
                    cancel_running: false,
                    all_submitters: false,
                    submitter: None,
                    wait: Duration::ZERO,
                    ready_timeout: Duration::ZERO,
                },
            )
            .map_err(|e| e.to_string())?;
            let stdout =
                serde_json::to_vec(&serde_json::json!({"ok": true, "status": status})).unwrap();
            self.captured.extend_from_slice(&stdout);
            Ok(workers::RemoteOutput {
                code: Some(0),
                stdout,
                stderr: vec![],
            })
        }
        fn bridge_hello(
            &mut self,
            _: &belt::protocol::ssh::SshTarget,
            _: &str,
            _: &belt::ids::ClusterId,
        ) -> Result<(belt::ids::ClusterId, WorkerId), String> {
            panic!("enrollment must reuse the existing resident")
        }
    }
    let target_root = crate::test_tmp::tempdir().unwrap();
    let target_context = context(target_root.path());
    let state = Arc::new(Mutex::new(State::default()));
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let target = rt
        .block_on(initialize(
            target_context.clone(),
            &mut OwnedLauncher(state.clone()),
        ))
        .unwrap();
    let target_spec =
        bootstrap::read_input(&target_root.path().join("config/belt/coordinator.json")).unwrap();
    let caller_root = crate::test_tmp::tempdir().unwrap();
    let caller_context = context(caller_root.path());
    let caller_state = caller_root.path().join("coordinator");
    std::fs::create_dir(&caller_state).unwrap();
    let caller = CoordinatorStore::open(&caller_state, 1).unwrap();
    let caller_id = caller.coordinator_id().clone();
    drop(caller);
    let db = rusqlite::Connection::open(caller_state.join("coordinator.db")).unwrap();
    db.execute(
        "INSERT INTO cluster SELECT ?1, coordinator_id, created_at_ms FROM cluster",
        [target.cluster_id.as_str()],
    )
    .unwrap();
    db.execute(
        "UPDATE coordinator_state SET cluster_id = ?1",
        [target.cluster_id.as_str()],
    )
    .unwrap();
    db.execute(
        "DELETE FROM cluster WHERE cluster_id <> ?1",
        [target.cluster_id.as_str()],
    )
    .unwrap();
    drop(db);
    let local_worker = caller_root.path().join("worker");
    std::fs::create_dir(&local_worker).unwrap();
    let local_name = WorkerId::parse("caller").unwrap();
    let local = WorkerStore::open(&local_worker, &local_name).unwrap();
    local
        .for_member(&CoordinatorId::generate())
        .pin_member(target.worker_id.as_str(), 1)
        .unwrap();
    let remote = WorkerStore::open(&target_spec.worker_state_dir, &target.worker_id).unwrap();
    remote
        .for_member(&CoordinatorId::generate())
        .replace_member_pin("caller", 1)
        .unwrap();
    let spec = bootstrap::Spec {
        state_dir: caller_state,
        worker_state_dir: local_worker,
        worker_id: local_name,
        cluster_key_path: target_spec.cluster_key_path.clone(),
        executable: caller_context.executable.clone(),
        env: caller_context.env.clone(),
        repo_root: None,
        expected_cluster: Some(target.cluster_id.clone()),
        managed_service: None,
    };
    let mut helper = Helper {
        env: target_context.env,
        state: state.clone(),
        exe: target_context.executable,
        captured: vec![],
        argv: vec![],
    };
    let before = state.lock().unwrap().starts;
    let result = workers::add(
        &mut helper,
        &caller_context.env,
        &spec,
        &caller_context.executable,
        target.worker_id.as_str(),
        Some("fake@target"),
        workers::AddOptions::default(),
        |_| workers::ResidentReload::NotRunning,
    )
    .unwrap();
    assert_eq!(
        result.status.as_ref().unwrap().coordinator_id.as_ref(),
        Some(&target.coordinator_id)
    );
    assert!(remote
        .members()
        .unwrap()
        .contains(&("caller".into(), caller_id.to_string())));
    assert_eq!(
        local.members().unwrap(),
        vec![(
            target.worker_id.to_string(),
            target.coordinator_id.to_string()
        )]
    );
    assert_eq!(state.lock().unwrap().starts, before);
    assert!(!target_root
        .path()
        .join("config/belt/worker-service.json")
        .exists());
    assert!(
        bootstrap::read_input(&target_root.path().join("config/belt/coordinator.json")).is_ok()
    );
    let key = std::fs::read_to_string(&target_spec.cluster_key_path).unwrap();
    assert!(helper.argv.iter().all(|arg| !arg.contains(key.trim())));
    assert!(!String::from_utf8_lossy(&helper.captured).contains(key.trim()));
    for log in &mut state.lock().unwrap().stderr {
        log.seek(SeekFrom::Start(0)).unwrap();
        let mut text = String::new();
        log.read_to_string(&mut text).unwrap();
        assert!(!text.contains(key.trim()));
    }
}

/// A real init service, with private host admission independent of host load.
struct InitFixture {
    root: tempfile::TempDir,
    context: ContextInput,
    state: Arc<Mutex<State>>,
}
impl InitFixture {
    fn new() -> Self {
        let root = crate::test_tmp::tempdir().unwrap();
        let context = context(root.path());
        let config = root.path().join("config/belt");
        std::fs::create_dir_all(&config).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(config.join("config.toml"), "schema_version = 1\n[coordinator]\nmax_active_task = 2\nheartbeat_seconds = 1\n[scheduler]\nprefetch_per_worker = 0\n").unwrap();
        Self {
            root,
            context,
            state: Arc::new(Mutex::new(State::default())),
        }
    }
    fn admit_commands(&self, n: u32) {
        crate::common::fixture_worker_traffic_budget(&self.context.env, n * 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_worker_bridge_shares_local_database_and_admission_window() {
    use belt::{
        coordinator::TaskSpec,
        ids::*,
        protocol::{connect, Io},
        storage::WorkerStore,
        wire::*,
    };
    let f = InitFixture::new();
    f.admit_commands(2);
    let initialized = initialize(f.context.clone(), &mut OwnedLauncher(f.state.clone()))
        .await
        .unwrap_or_else(|error| {
            f.state.lock().unwrap().diagnose();
            panic!("initialization failed: {error}");
        });
    let spec = bootstrap::read_input(&f.root.path().join("config/belt/coordinator.json")).unwrap();
    let source = f.root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input"), "snapshot").unwrap();
    let captured = belt::source::capture(
        &spec.env,
        &belt::source::SourceStore::open(&spec.worker_state_dir).unwrap(),
        &belt::source::SourceRequest::Directory { dir: source },
        &belt::source::CaptureParams::default(),
    )
    .unwrap();
    let release = f.root.path().join("release");
    let payload = AttemptPayload {
        ci_task: None,
        toolchains: None,
        outputs: vec![],
        requirements: Default::default(),
        in_place: false,
        setup: None,
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: captured.manifest.source_id.clone(),
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest))
            .or_else(|_| Digest::parse(&captured.manifest.manifest_digest))
            .unwrap(),
        permit_id: None,
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                format!(
                    "i=0; while [ ! -f '{}' ] && [ $i -lt 600 ]; do sleep 0.05; i=$((i+1)); done",
                    release.display()
                ),
            ],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        lease_ms: 60_000,
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![],
    };
    let local = Client::load(&spec.state_dir, initialized.cluster_id.clone()).unwrap();
    let task: TaskSpec = serde_json::from_value(serde_json::to_value(&payload).unwrap()).unwrap();
    assert!(matches!(
        local
            .request(belt::service::resident::Operation::Submit {
                run_id: payload.run_id.clone(),
                source_id: payload.source_id.clone(),
                tasks: vec![(payload.task_id.clone(), task)],
            })
            .await
            .unwrap(),
        Reply::Accepted { .. }
    ));
    let store = WorkerStore::open_read_only(&spec.worker_state_dir).unwrap();
    let member = store.for_member(&initialized.coordinator_id);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if member.list_attempts(None, false, 10).unwrap().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut bridge = tokio::process::Command::from(test_command(&spec.executable))
        .args(["worker", "--stdio"])
        .envs(&spec.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let connected = connect(
        Io::new(bridge.stdout.take().unwrap(), bridge.stdin.take().unwrap()),
        crate::proto_common::client_cfg_keyed(&initialized.cluster_id, &spec.cluster_key_path),
    )
    .await
    .unwrap();
    assert_eq!(connected.worker.worker_id, initialized.worker_id);
    let remote_id = crate::proto_common::coordinator_of(&initialized.cluster_id);
    assert_ne!(remote_id, initialized.coordinator_id);
    let mut remote_payload = payload.clone();
    remote_payload.run_id = RunId::generate();
    remote_payload.task_id = TaskId::generate();
    let remote_attempt = AttemptId::generate();
    let submit = |attempt, payload| {
        Request::SubmitAttempt(
            SubmitAttemptRequest::new(RequestId::generate(), CoordinatorEpoch(1), attempt, payload)
                .unwrap(),
        )
    };
    assert!(matches!(
        connected
            .client
            .call(
                submit(remote_attempt.clone(), remote_payload.clone()),
                Duration::from_secs(5)
            )
            .await
            .unwrap(),
        Response::SubmitAttempt(_)
    ));
    let remote = store.for_member(&remote_id);
    assert_eq!(
        remote.list_attempts(None, false, 10).unwrap()[0].attempt_id,
        remote_attempt
    );
    assert_eq!(member.list_attempts(None, false, 10).unwrap().len(), 1);
    let rejected = AttemptId::generate();
    remote_payload.task_id = TaskId::generate();
    let response = connected
        .client
        .call(
            submit(rejected.clone(), remote_payload.clone()),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(response, belt::protocol::CallError::Remote(ref e) if e.overloaded_reason == Some(belt::error::OverloadedReason::Capacity)),
        "{response:?}"
    );
    assert_eq!(remote.list_attempts(None, true, 10).unwrap().len(), 1);
    std::fs::write(release, "done").unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if member.list_attempts(None, false, 10).unwrap().is_empty()
                && remote.list_attempts(None, false, 10).unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(connected);
    // Closing the protocol streams ends the bridge without signalling a process.
    tokio::time::timeout(Duration::from_secs(5), bridge.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        local
            .request(belt::service::resident::Operation::Shutdown)
            .await
            .unwrap(),
        Reply::Stopping
    ));
}

/// Tracks each registered service separately while owning only private test children.
#[derive(Clone)]
struct TakeoverLauncher {
    state: Arc<Mutex<State>>,
    active: Arc<Mutex<Option<std::path::PathBuf>>>,
    fail_init: bool,
}
impl TakeoverLauncher {
    fn stop_owned(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(mut child) = state.child.take() {
            if child.try_wait().unwrap().is_none() {
                assert!(belt::platform::signal_pid(child.id(), libc::SIGTERM));
            }
            child.wait().unwrap();
        }
        *self.active.lock().unwrap() = None;
    }
}
impl Launcher for TakeoverLauncher {
    fn loaded_state(&mut self, _: &Plan, receipt: &Receipt) -> LoadedState {
        if self.active.lock().unwrap().as_deref() == Some(receipt.path()) {
            LoadedState::Match
        } else {
            LoadedState::Absent
        }
    }
    fn verify_loaded(&mut self, plan: &Plan, receipt: &Receipt) -> Result<(), LaunchError> {
        if self.loaded_state(plan, receipt) == LoadedState::Match {
            Ok(())
        } else {
            Err(LaunchError::NotApplied("not registered".into()))
        }
    }
    fn running_identity(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
    ) -> Result<belt::service::macos::RunningIdentity, LaunchError> {
        self.verify_loaded(plan, receipt)?;
        OwnedLauncher(self.state.clone()).running_identity(plan, receipt)
    }
    fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
        if args[0] == "bootout" {
            assert_eq!(
                self.active.lock().unwrap().as_deref(),
                Some(std::path::Path::new(&args[2]))
            );
            self.stop_owned();
            return Ok(());
        }
        assert_eq!(args[0], "bootstrap");
        assert!(self.active.lock().unwrap().is_none());
        let worker = args[2].contains(".worker.");
        if !worker && self.fail_init {
            return Err(LaunchError::NotApplied(
                "injected init startup failure".into(),
            ));
        }
        let input = self_input(&args[2]);
        if worker {
            let input = input.with_file_name("worker.json");
            let spec = belt::service::worker::read_input(&input).unwrap();
            let mut state = self.state.lock().unwrap();
            let child = test_command(&spec.executable)
                .args(["worker", "--input"])
                .arg(&input)
                .envs(&spec.env)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(state.stderr())
                .spawn()
                .unwrap();
            state.child = Some(child);
            state.starts += 1;
        } else {
            start_owned(&self.state, &input);
        }
        *self.active.lock().unwrap() = Some(args[2].clone().into());
        Ok(())
    }
}

#[test]
fn worker_only_init_preserves_membership_and_retires_old_service_or_rolls_back() {
    use belt::{ids::*, service::worker_service as ws, storage::WorkerStore};
    for fail_init in [false, true] {
        let root = crate::test_tmp::tempdir().unwrap();
        let context = context(root.path());
        let mut launcher = TakeoverLauncher {
            state: Arc::new(Mutex::new(State::default())),
            active: Arc::new(Mutex::new(None)),
            fail_init,
        };
        let request = ws::InstallRequest {
            membership: None,
            system_user: false,
            schema: 1,
            cluster: ClusterId::generate(),
            worker_id: WorkerId::parse("member-a").unwrap(),
            key_file_text: format!("{}\n", "ab".repeat(64)),
            binary_sha256: ws::sha256_file(&context.executable).unwrap(),
        };
        ws::install(
            &context.env,
            &request,
            &context.executable,
            launcher.clone(),
            Duration::from_secs(20),
        )
        .unwrap();
        let paths = ws::Paths::resolve(&context.env).unwrap();
        let old_service = launcher.active.lock().unwrap().clone().unwrap();
        let journal = std::fs::read(&paths.journal).unwrap();
        let peer = CoordinatorId::generate();
        let store = WorkerStore::open(&paths.state, &request.worker_id).unwrap();
        store.for_member(&peer).pin_member("peer", 1).unwrap();
        let mut pins = store.members().unwrap();
        drop(store);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(initialize(context.clone(), &mut launcher));
        if fail_init {
            let error = result.unwrap_err();
            assert!(!error.to_string().contains("rollback failed"), "{error}");
            assert_eq!(launcher.active.lock().unwrap().as_ref(), Some(&old_service));
            assert_eq!(std::fs::read(&paths.journal).unwrap(), journal);
            assert!(old_service.exists());
            assert!(!paths.config.join("init.json").exists());
            assert!(!paths.config.join("coordinator.json").exists());
        } else {
            let receipt = result.unwrap_or_else(|e| {
                launcher.state.lock().unwrap().diagnose();
                panic!("{e}")
            });
            assert_eq!(receipt.cluster_id, request.cluster);
            assert_eq!(receipt.worker_id, request.worker_id);
            assert_ne!(receipt.coordinator_id, peer);
            // The new local coordinator pins its own member name; earlier pins stay.
            pins.push((
                request.worker_id.to_string(),
                receipt.coordinator_id.to_string(),
            ));
            pins.sort();
            assert!(!paths.journal.exists());
            assert!(!old_service.exists());
            assert_ne!(launcher.active.lock().unwrap().as_ref(), Some(&old_service));
            let spec = bootstrap::read_input(&paths.config.join("coordinator.json")).unwrap();
            assert_eq!(spec.cluster_key_path, paths.key);
            assert_eq!(spec.worker_state_dir, paths.state);
            // Re-running init recognizes the adopted key and the same service.
            let again = rt
                .block_on(initialize(context.clone(), &mut launcher))
                .unwrap();
            assert_eq!(again.coordinator_id, receipt.coordinator_id);
            // Start/stop now manage the same init resident (§24.2); upgrade
            // must still refuse to replace it with a worker-only service.
            let active = launcher.active.lock().unwrap().clone().unwrap();
            let started = ws::start(&context.env, launcher.clone(), Duration::ZERO).unwrap();
            assert_eq!(started.loaded, Some(true));
            assert_eq!(launcher.active.lock().unwrap().as_ref(), Some(&active));
            let stopped = ws::stop(&context.env, launcher.clone(), false, Duration::ZERO).unwrap();
            assert_eq!(stopped.loaded, Some(false));
            assert!(launcher.active.lock().unwrap().is_none());
            let error = ws::upgrade::upgrade(
                &context.env,
                &ws::upgrade::SwitchRequest {
                    schema: 1,
                    cluster: request.cluster.clone(),
                    worker_id: request.worker_id.clone(),
                    binary_sha256: request.binary_sha256.clone(),
                },
                &context.executable,
                launcher.clone(),
                ws::Options {
                    cancel_running: false,
                    all_submitters: false,
                    submitter: None,
                    wait: Duration::ZERO,
                    ready_timeout: Duration::ZERO,
                },
                &mut |_| Ok(()),
            )
            .unwrap_err();
            assert!(error.to_string().contains("init resident"));
            assert_eq!(launcher.state.lock().unwrap().starts, 2);
        }
        assert_eq!(
            std::fs::read_to_string(&paths.key).unwrap(),
            request.key_file_text
        );
        assert_eq!(
            WorkerStore::open_read_only(&paths.state)
                .unwrap()
                .members()
                .unwrap(),
            pins
        );
        launcher.stop_owned();
    }
}
