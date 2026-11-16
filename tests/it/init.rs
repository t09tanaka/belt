//! Process composition only: no launchctl invocation or real LaunchAgents path.
use remote_task_runner::{
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
    ) -> Result<remote_task_runner::service::macos::RunningIdentity, LaunchError> {
        let state = self.0.lock().unwrap();
        let pid = state
            .child
            .as_ref()
            .ok_or_else(|| LaunchError::NotApplied("no owned child".into()))?
            .id();
        remote_task_runner::service::macos::RunningIdentity::observe(pid)
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
        let input = self_input(&args[2]);
        let spec = bootstrap::read_input(&input).unwrap();
        let mut c = test_command(&spec.executable);
        c.args(bootstrap::daemon_argv(&input).unwrap())
            .envs(&spec.env)
            .current_dir(spec.repo_root.as_ref().unwrap())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut state = self.0.lock().unwrap();
        c.stderr(state.stderr());
        assert!(state.child.is_none());
        state.child = Some(c.spawn().unwrap());
        state.starts += 1;
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
    // HOME/Library/LaunchAgents/<label>.plist -> HOME/config/remote-task-runner/coordinator.json
    std::path::Path::new(plist)
        .ancestors()
        .nth(3)
        .unwrap()
        .join("config/remote-task-runner/coordinator.json")
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
        executable: env!("CARGO_BIN_EXE_remote-task-runner").into(),
    }
}
#[test]
fn concurrent_init_reuses_one_real_combined_resident_and_epoch() {
    let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
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
    let input = root
        .path()
        .join("config/remote-task-runner/coordinator.json");
    let spec = bootstrap::read_input(&input).unwrap();
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
        wrong_worker.worker_id =
            remote_task_runner::ids::WorkerId::parse("foreign-worker").unwrap();
        assert!(bootstrap::health(&wrong_worker).await.is_err());
        let mut wrong_cluster = spec.clone();
        wrong_cluster.expected_cluster = Some(remote_task_runner::ids::ClusterId::generate());
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
#[cfg(not(target_os = "macos"))]
#[test]
fn linux_public_init_refuses_without_filesystem_changes() {
    let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let out = test_command(env!("CARGO_BIN_EXE_remote-task-runner"))
        .args(["--json", "init"])
        .env("HOME", root.path())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(70));
    assert!(std::fs::read_dir(root.path()).unwrap().next().is_none());
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
        ) -> Result<remote_task_runner::service::macos::RunningIdentity, LaunchError> {
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
    let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
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
    let spec = bootstrap::read_input(
        &root
            .path()
            .join("config/remote-task-runner/coordinator.json"),
    )
    .unwrap();
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
