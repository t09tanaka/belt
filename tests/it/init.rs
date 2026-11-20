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
