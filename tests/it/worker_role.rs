//! Public foreground worker and bridge are separate owned child processes.
use remote_task_runner::ids::*;
use remote_task_runner::protocol::{connect, transport::Io, Connected};
use remote_task_runner::service::worker::WorkerSpec;
use remote_task_runner::wire::*;
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
const BIN: &str = env!("CARGO_BIN_EXE_remote-task-runner");
const T: Duration = Duration::from_secs(15);
async fn kill_owned(child: &mut tokio::process::Child) {
    let pid = child.id().expect("owned child still live");
    assert!(pid > 1);
    let start = super::common::identity_of(pid).expect("owned child start identity");
    super::common::kill_recorded(pid, &start);
    tokio::time::timeout(T, child.wait())
        .await
        .unwrap()
        .unwrap();
}
struct Fixture {
    root: tempfile::TempDir,
    spec: WorkerSpec,
    input: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("rtr-wr-")
            .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
            .unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let state = root.path().join(".remote-task-runner");
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
        let key = root.path().join("key");
        remote_task_runner::secrets::key::init(&key).unwrap();
        let spec = WorkerSpec {
            schema: 1,
            expected_cluster: ClusterId::generate(),
            worker_id: WorkerId::parse("persistent-remote").unwrap(),
            state_dir: state,
            cluster_key_path: key,
            executable: BIN.into(),
            env: BTreeMap::from([
                ("HOME".into(), root.path().display().to_string()),
                ("PATH".into(), "/usr/bin:/bin:/opt/homebrew/bin".into()),
            ]),
            config: "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\n"
                .into(),
        };
        let input = root.path().join("worker.json");
        let this = Self { root, spec, input };
        this.write();
        this
    }
    fn write(&self) {
        std::fs::write(&self.input, serde_json::to_vec(&self.spec).unwrap()).unwrap();
        std::fs::set_permissions(&self.input, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    // Inherit harness environment (including LLVM_PROFILE_FILE) for instrumented
    // owned children; never add test instrumentation to WorkerSpec.env.
    fn spawn(&self) -> tokio::process::Child {
        tokio::process::Command::new(BIN)
            .args(["worker", "--input"])
            .arg(&self.input)
            .env("HOME", self.spec.env.get("HOME").unwrap())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }
    async fn ready(&self, child: &mut tokio::process::Child) {
        tokio::time::timeout(T, async {
            loop {
                if child.try_wait().unwrap().is_some() {
                    let mut bytes = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(
                        child.stderr.as_mut().unwrap(),
                        &mut bytes,
                    )
                    .await
                    .unwrap();
                    panic!("worker failed: {}", String::from_utf8_lossy(&bytes));
                }
                if tokio::net::UnixStream::connect(self.spec.state_dir.join("agent.sock"))
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn bridge(&self) -> (tokio::process::Child, Connected) {
        let mut child = tokio::process::Command::new(BIN)
            .args(["worker", "--stdio"])
            .env("HOME", self.spec.env.get("HOME").unwrap())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let io = Io::new(child.stdout.take().unwrap(), child.stdin.take().unwrap());
        let connected = connect(
            io,
            super::proto_common::client_cfg(&self.spec.expected_cluster),
        )
        .await
        .unwrap();
        (child, connected)
    }
    async fn stop(&self, child: &mut tokio::process::Child) {
        let pid = child.id().expect("owned child still live");
        assert!(pid > 1);
        let start = super::common::identity_of(pid).expect("owned child start identity");
        assert_eq!(
            remote_task_runner::platform::process_start_identity(pid),
            remote_task_runner::platform::StartIdentity::Identity(start)
        );
        remote_task_runner::platform::signal_pid(pid, libc::SIGTERM);
        assert!(tokio::time::timeout(T, child.wait())
            .await
            .unwrap()
            .unwrap()
            .success());
        assert!(!self.spec.state_dir.join("agent.sock").exists());
    }
    async fn rejects(&self) {
        let out = tokio::time::timeout(T, self.spawn().wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(!out.status.success(), "unexpected worker start");
    }
}

#[tokio::test]
async fn worker_identity_survives_restart_and_bridge_loss_preserves_task() {
    use remote_task_runner::source::{capture, CaptureParams, SourceRequest, SourceStore};
    let f = Fixture::new();
    let source = f.root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input"), "captured").unwrap();
    let store = SourceStore::open(&f.spec.state_dir).unwrap();
    let captured = capture(
        &f.spec.env,
        &store,
        &SourceRequest::Directory { dir: source },
        &CaptureParams::default(),
    )
    .unwrap();
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    let (mut bridge, connected) = f.bridge().await;
    let initial = connected.worker.clone();
    assert_eq!(initial.worker_id, f.spec.worker_id);
    assert_eq!(
        initial.worker_boot_id.as_str(),
        remote_task_runner::platform::boot_id().unwrap()
    );
    let side_effect = f.root.path().join("effect");
    let started = f.root.path().join("started");
    let release = f.root.path().join("release");
    let payload = AttemptPayload {
        setup: None,
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: captured.manifest.source_id,
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(&captured.manifest.manifest_digest)
            .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
            .unwrap(),
        permit_id: None,
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                format!(
                    "printf '%s' $$ > '{}'; i=0; while [ ! -f '{}' ] && [ $i -lt 200 ]; do sleep 0.05; i=$((i+1)); done; printf done >> '{}'",
                    started.display(),
                    release.display(),
                    side_effect.display()
                ),
            ],
            cwd: String::new(),
            env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
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
    let attempt = AttemptId::generate();
    let request = Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(1),
            attempt.clone(),
            payload,
        )
        .unwrap(),
    );
    assert!(matches!(
        connected.client.call(request.clone(), T).await.unwrap(),
        Response::SubmitAttempt(_)
    ));
    kill_owned(&mut bridge).await;
    drop(connected);
    tokio::time::timeout(T, async {
        while super::common::read_pid(&started).is_none() {
            assert!(worker.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let command_pid = super::common::read_pid(&started).expect("running command PID");
    assert!(command_pid > 1);
    let command_start =
        super::common::identity_of(command_pid as u32).expect("running command identity");
    // The accepted task has its own supervisor; killing only our worker owner
    // leaves it running while there is no bridge or resident worker process.
    kill_owned(&mut worker).await;
    assert!(f.spec.state_dir.join("agent.sock").exists());
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    let (mut adoption_bridge, adopted) = f.bridge().await;
    assert!(adopted.worker.agent_epoch > initial.agent_epoch);
    assert_eq!(
        remote_task_runner::platform::process_start_identity(command_pid as u32),
        remote_task_runner::platform::StartIdentity::Identity(command_start.clone())
    );
    // Command exec/PID-file creation precedes the supervisor's durable process
    // identity and Running receipt. A hello is not an adoption-ready barrier.
    let mut last_status = None;
    let active = tokio::time::timeout(T, async {
        loop {
            assert!(
                !release.exists(),
                "command gate must remain closed during adoption"
            );
            assert_eq!(
                remote_task_runner::platform::process_start_identity(command_pid as u32),
                remote_task_runner::platform::StartIdentity::Identity(command_start.clone())
            );
            let response = adopted
                .client
                .call(
                    Request::GetAttempt(GetAttemptRequest {
                        schema_version: SchemaVersion::CURRENT,
                        request_id: RequestId::generate(),
                        attempt_id: attempt.clone(),
                    }),
                    T,
                )
                .await
                .unwrap();
            let Response::GetAttempt(status) = response else {
                panic!("unexpected adoption response: {response:?}");
            };
            assert_eq!(status.attempt_id, attempt);
            assert_eq!(status.agent_epoch, adopted.worker.agent_epoch);
            assert!(
                matches!(
                    status.state,
                    remote_task_runner::state::AttemptState::Starting
                        | remote_task_runner::state::AttemptState::Active
                ),
                "unexpected adoption state: {status:?}"
            );
            let ready = status.state == remote_task_runner::state::AttemptState::Active
                && status
                    .receipts
                    .reached(remote_task_runner::state::Receipt::Running);
            last_status = Some(status);
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        active.is_ok(),
        "adoption did not become active: {last_status:?}"
    );
    let store =
        remote_task_runner::storage::WorkerStore::open_read_only(&f.spec.state_dir).unwrap();
    let launches = store.launches(&attempt).unwrap();
    assert_eq!(store.count_launches().unwrap(), 1);
    assert_eq!(launches.len(), 1);
    let identity = launches[0]
        .2
        .as_ref()
        .expect("durable command process identity");
    assert_eq!(identity.pid, command_pid as u32);
    assert_eq!(identity.start_identity, command_start);
    drop(store);
    kill_owned(&mut adoption_bridge).await;
    drop(adopted);
    std::fs::write(&release, "finish").unwrap();

    tokio::time::timeout(T, async {
        while !side_effect.exists() {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read_to_string(&side_effect).unwrap(), "done");
    let (mut bridge, connected) = f.bridge().await;
    assert!(connected.worker.agent_epoch > initial.agent_epoch);
    assert!(
        matches!(connected.client.call(request, T).await.unwrap(), Response::SubmitAttempt(r) if r.duplicate)
    );
    tokio::time::timeout(T, async {
        loop {
            let status = connected.client.call(Request::GetAttempt(GetAttemptRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(), attempt_id: attempt.clone(),
            }), T).await.unwrap();
            if matches!(status, Response::GetAttempt(s) if s.state == remote_task_runner::state::AttemptState::Terminal) { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap();
    let response = connected
        .client
        .call(
            Request::GetResult(GetResultRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: attempt.clone(),
                generation: Generation(1),
            }),
            T,
        )
        .await
        .unwrap();
    let Response::GetResult(result) = response else {
        panic!("unexpected result response: {response:?}");
    };
    assert_eq!(result.attempt_id, attempt);
    assert_eq!(
        result.termination.outcome(),
        remote_task_runner::state::Outcome::Succeeded
    );
    kill_owned(&mut bridge).await;
    drop(connected);
    f.stop(&mut worker).await;
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    let (mut bridge, connected) = f.bridge().await;
    assert_eq!(connected.worker.worker_id, initial.worker_id);
    assert_eq!(connected.worker.cluster_id, initial.cluster_id);
    assert_eq!(connected.worker.worker_boot_id, initial.worker_boot_id);
    assert!(connected.worker.agent_epoch > initial.agent_epoch);
    let response = connected
        .client
        .call(
            Request::GetResult(GetResultRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: attempt.clone(),
                generation: Generation(1),
            }),
            T,
        )
        .await
        .unwrap();
    assert!(
        matches!(response, Response::GetResult(ref restored) if restored == &result),
        "durable result changed: {response:?}"
    );
    kill_owned(&mut bridge).await;
    drop(connected);
    f.stop(&mut worker).await;
    let store =
        remote_task_runner::storage::WorkerStore::open_read_only(&f.spec.state_dir).unwrap();
    assert_eq!(store.count_launches().unwrap(), 1);
    assert_eq!(std::fs::read_to_string(side_effect).unwrap(), "done");
}

#[tokio::test]
async fn worker_rejects_invalid_private_input_foreign_socket_and_owner() {
    let mut f = Fixture::new();
    f.spec.schema = 2;
    f.write();
    f.rejects().await;
    assert!(!f.spec.state_dir.join("worker.db").exists());
    f.spec.schema = 1;
    f.write();
    f.spec
        .env
        .insert("SECRET_TOKEN".into(), "must not capture".into());
    f.write();
    f.rejects().await;
    f.spec.env.remove("SECRET_TOKEN");
    let key = f.spec.cluster_key_path.clone();
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
    f.write();
    f.rejects().await;
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let moved_key = f.root.path().join("actual-key");
    std::fs::rename(&key, &moved_key).unwrap();
    std::os::unix::fs::symlink(&moved_key, &key).unwrap();
    f.rejects().await;
    std::fs::remove_file(&key).unwrap();
    std::fs::rename(&moved_key, &key).unwrap();
    let original_state = f.spec.state_dir.clone();
    f.spec.state_dir = f.root.path().join("different-worker");
    f.write();
    f.rejects().await;
    f.spec.state_dir = original_state;
    f.write();
    let mut json = serde_json::to_value(&f.spec).unwrap();
    json["unexpected"] = true.into();
    std::fs::write(&f.input, serde_json::to_vec(&json).unwrap()).unwrap();
    f.rejects().await;
    f.write();
    std::fs::set_permissions(&f.input, std::fs::Permissions::from_mode(0o644)).unwrap();
    f.rejects().await;
    f.write();
    std::fs::write(&f.input, vec![b' '; 262_145]).unwrap();
    f.rejects().await;
    f.write();
    let moved_input = f.root.path().join("actual-input");
    std::fs::rename(&f.input, &moved_input).unwrap();
    std::os::unix::fs::symlink(&moved_input, &f.input).unwrap();
    f.rejects().await;
    std::fs::remove_file(&f.input).unwrap();
    std::fs::rename(&moved_input, &f.input).unwrap();
    let socket = f.spec.state_dir.join("agent.sock");
    std::fs::write(&socket, "foreign").unwrap();
    f.rejects().await;
    assert_eq!(std::fs::read_to_string(&socket).unwrap(), "foreign");
    std::fs::remove_file(&socket).unwrap();
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    f.rejects().await;
    f.stop(&mut worker).await;
    f.spec.expected_cluster = ClusterId::generate();
    f.write();
    f.rejects().await;
    for args in [
        vec!["worker", "--input", "x", "--stdio"],
        vec!["worker", "--input", "x", "--ephemeral"],
        vec!["worker", "--stdio", "--ephemeral"],
    ] {
        let out = tokio::process::Command::new(BIN)
            .args(args)
            .output()
            .await
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
    }
}

#[tokio::test]
async fn worker_never_reclaims_unknown_or_uncertain_socket() {
    let f = Fixture::new();
    let socket = f.spec.state_dir.join("agent.sock");
    let unknown = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    f.rejects().await;
    assert!(socket.exists());
    drop(unknown);
    // This listener belongs exclusively to this fixture, never a shared agent.
    std::fs::remove_file(&socket).unwrap();
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    kill_owned(&mut worker).await;
    let receipt = f.spec.state_dir.join("worker-socket.json");
    std::fs::write(&receipt, b"{").unwrap();
    f.rejects().await;
    assert!(socket.exists());
}

#[tokio::test]
async fn worker_staged_socket_fits_near_unix_path_limit() {
    let mut f = Fixture::new();
    let tail = "/.remote-task-runner/agent.sock";
    let component = 100 - f.root.path().as_os_str().len() - 1 - tail.len();
    let home = f.root.path().join("h".repeat(component));
    std::fs::create_dir(&home).unwrap();
    std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = home.join(".remote-task-runner");
    std::fs::rename(&f.spec.state_dir, &state).unwrap();
    f.spec.state_dir = state;
    f.spec.env.insert("HOME".into(), home.display().to_string());
    f.write();
    assert_eq!(f.spec.state_dir.join("agent.sock").as_os_str().len(), 100);
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    let (mut bridge, connected) = f.bridge().await;
    assert_eq!(connected.worker.worker_id, f.spec.worker_id);
    kill_owned(&mut bridge).await;
    drop(connected);
    f.stop(&mut worker).await;
}

#[tokio::test]
async fn worker_missing_binding_never_reenrolls_existing_database() {
    use std::os::unix::fs::MetadataExt;
    let mut f = Fixture::new();
    let mut worker = f.spawn();
    f.ready(&mut worker).await;
    f.stop(&mut worker).await;
    let binding = f.spec.state_dir.join("worker-binding.json");
    std::fs::remove_file(&binding).unwrap();
    let db = f.spec.state_dir.join("worker.db");
    let epoch = || {
        let connection =
            rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        connection
            .query_row("SELECT agent_epoch FROM worker_state", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    let original_epoch = epoch();
    let bytes = std::fs::read(&db).unwrap();
    let metadata = std::fs::metadata(&db).unwrap();
    let identity = |m: std::fs::Metadata| {
        (
            m.dev(),
            m.ino(),
            m.mode(),
            m.uid(),
            m.gid(),
            m.nlink(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
        )
    };
    let original_metadata = identity(metadata);
    let original_cluster = f.spec.expected_cluster.clone();
    let original_key = f.spec.cluster_key_path.clone();
    let different_key = f.root.path().join("different-key");
    remote_task_runner::secrets::key::init(&different_key).unwrap();
    // Even the same cluster/key cannot establish missing enrollment provenance.
    for variant in 0..3 {
        f.spec.expected_cluster = if variant == 1 {
            ClusterId::generate()
        } else {
            original_cluster.clone()
        };
        f.spec.cluster_key_path = if variant == 2 {
            different_key.clone()
        } else {
            original_key.clone()
        };
        f.write();
        f.rejects().await;
        assert!(!binding.exists());
        assert_eq!(std::fs::read(&db).unwrap(), bytes);
        assert_eq!(identity(std::fs::metadata(&db).unwrap()), original_metadata);
        assert_eq!(epoch(), original_epoch);
    }
}

#[tokio::test]
async fn worker_enrollment_lock_stays_held_through_agent_start() {
    use remote_task_runner::config::runtime::WorkerInputs;
    let mut f = Fixture::new();
    let bin = f.root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let reached = f.root.path().join("preflight");
    let release = f.root.path().join("release-preflight");
    super::common::write_executable(&bin.join("git"), &format!(
        "#!/bin/sh\nprintf ready > '{}'\ni=0; while [ ! -f '{}' ] && [ $i -lt 300 ]; do /bin/sleep 0.05; i=$((i+1)); done\nexec /usr/bin/git \"$@\"\n", reached.display(), release.display()
    ));
    f.spec
        .env
        .insert("PATH".into(), format!("{}:/usr/bin:/bin", bin.display()));
    f.write();
    let mut role = f.spawn();
    tokio::time::timeout(T, async {
        while !reached.exists() {
            assert!(role.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let binding = f.spec.state_dir.join("worker-binding.json");
    let enrolled = std::fs::read(&binding).unwrap();
    assert!(!f.spec.state_dir.join("worker.db").exists());
    // A legacy public Agent entry (which knows only worker ID, no cluster)
    // attempts to intervene after binding while role host preflight is paused.
    let config = remote_task_runner::config::resolve(&remote_task_runner::config::Inputs {
        overlay: &Default::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    let runtime = config
        .worker_runtime(WorkerInputs {
            state_dir: f.spec.state_dir.clone(),
            worker_id: f.spec.worker_id.clone(),
            boot_id: WorkerBootId::parse(&remote_task_runner::platform::boot_id().unwrap())
                .unwrap(),
            supervisor_exe: BIN.into(),
            cluster_key_path: f.spec.cluster_key_path.clone(),
            env: BTreeMap::from([
                ("HOME".into(), f.spec.env["HOME"].clone()),
                ("PATH".into(), "/usr/bin:/bin".into()),
            ]),
        })
        .unwrap();
    let error = tokio::task::spawn_blocking(move || runtime.start())
        .await
        .unwrap()
        .err()
        .expect("legacy agent must not acquire the role gate");
    assert_eq!(error.code, remote_task_runner::error::ErrorCode::Conflict);
    assert!(!f.spec.state_dir.join("worker.db").exists());
    assert_eq!(std::fs::read(&binding).unwrap(), enrolled);
    std::fs::write(&release, "continue").unwrap();
    f.ready(&mut role).await;
    let (mut bridge, connected) = f.bridge().await;
    assert_eq!(connected.worker.agent_epoch, AgentEpoch(1));
    kill_owned(&mut bridge).await;
    drop(connected);
    f.stop(&mut role).await;
}
