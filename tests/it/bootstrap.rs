//! Real owner processes, all under private temporary state and environment.
use remote_task_runner::ids::{ClusterId, WorkerId};
use remote_task_runner::service::bootstrap::{ensure, Spec};
use remote_task_runner::service::resident::{Client, Operation, Reply};
use remote_task_runner::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::Duration;

fn fixture() -> (super::common::DaemonRoot, Spec) {
    let root = crate::test_tmp::prefixed("rtr-boot-").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = root.path().join("parent");
    let worker = root.path().join("worker");
    for dir in [&state, &worker] {
        std::fs::create_dir(dir).unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let key = root.path().join("key");
    remote_task_runner::secrets::key::init(&key).unwrap();
    let mut spec = Spec {
        state_dir: state,
        worker_state_dir: worker,
        worker_id: WorkerId::parse("persistent-local").unwrap(),
        cluster_key_path: key,
        executable: env!("CARGO_BIN_EXE_remote-task-runner").into(),
        env: BTreeMap::from([
            ("HOME".into(), root.path().display().to_string()),
            (
                "PATH".into(),
                format!(
                    "{}:/usr/bin:/bin",
                    super::common::tool_stubs(root.path()).display()
                ),
            ),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ]),
        repo_root: None,
        expected_cluster: None,
        managed_service: None,
    };
    // Instrumented CLI/daemon/supervisor descendants keep the harness's private
    // profile destination (including %p/%m), outside captured inputs and HOME.
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        spec.env.insert(
            "LLVM_PROFILE_FILE".into(),
            profile
                .into_string()
                .expect("coverage profile path is UTF-8"),
        );
    }
    // Every daemon these tests start, directly or through the CLI, owns this
    // state directory; the root stops it when the test ends or panics.
    let root = super::common::DaemonRoot::new(root, &spec.state_dir);
    (root, spec)
}
// Decode only public diagnostic enums. Journal payloads also contain private
// commands, paths and arbitrary remote error messages; never format those.
/// A worker tool path holding only the named host tools, next to the base
/// system directory. A worker measures every native tool found on its path
/// under a short deadline, at startup and on each inspection. A launcher that
/// answers too late only at startup changes the worker identity afterwards,
/// the coordinator excludes the worker, and the task waits for good. The
/// whole host PATH offers many such launchers; these fixtures do not test
/// tool discovery, so they offer none.
fn private_tool_path(root: &std::path::Path, names: &[&str]) -> String {
    let bin = root.join("bin");
    std::fs::create_dir(&bin).unwrap();
    for name in names {
        let tool = std::env::split_paths(&std::env::var_os("PATH").unwrap())
            .map(|dir| dir.join(name))
            .find(|path| path.is_file())
            .unwrap_or_else(|| panic!("{name} must be installed for this fixture"));
        std::os::unix::fs::symlink(std::fs::canonicalize(tool).unwrap(), bin.join(name)).unwrap();
    }
    format!("{}:/bin", bin.display())
}

fn coordinator_event_summary(
    event: &remote_task_runner::storage::Event,
) -> (
    &str,
    Option<remote_task_runner::error::ErrorCode>,
    Option<remote_task_runner::error::BlockedReason>,
) {
    let code = event
        .payload
        .get("code")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let reason = event
        .payload
        .get("reason")
        .or_else(|| event.payload.get("blocked_reason"))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    (&event.kind, code, reason)
}

// Classify fixed server diagnostics without ever printing their arbitrary text.
fn coordinator_refusal_class(event: &remote_task_runner::storage::Event) -> Option<&'static str> {
    let message = event.payload.get("message")?.as_str()?;
    match message {
        "toolchain_baseline_mismatch" => Some("native_baseline_mismatch"),
        "worker profile identity unavailable" => Some("profile_identity_unavailable"),
        "worker configured profile fingerprint mismatch" => Some("configured_profile_mismatch"),
        "worker profile gained a native observation" => Some("profile_gained_observation"),
        "worker profile native observation changed" => Some("profile_observation_changed"),
        "worker profile executable or context changed" => {
            Some("profile_executable_or_context_changed")
        }
        "worker capabilities changed or unmet: " => Some("profile_report_unavailable"),
        "toolchain registry unavailable" => Some("registry_unavailable"),
        "accepted base profile changed" => Some("accepted_profile_changed"),
        "activated tool identity changed" => Some("activation_changed"),
        "toolchain preparation unavailable" => Some("preparation_unavailable"),
        "proposal worker requirements mismatch" => Some("proposal_requirements_mismatch"),
        _ if message.starts_with("worker capabilities changed or unmet: ") => {
            Some("requirements_unmet")
        }
        _ if message.starts_with("toolchain unavailable: ") => Some("registry_plan_unavailable"),
        _ => None,
    }
}

/// Why the worker refused or failed an attempt before any command ran. Only
/// the event kinds and the typed code and message of the agent's own refusals
/// are printed, never other payloads.
fn diagnose_worker(spec: &Spec) {
    let Ok(store) =
        remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
    else {
        eprintln!("worker journal readable=false");
        return;
    };
    // The whole journal, a page at a time, without the periodic renewals that
    // would otherwise fill it.
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let Ok(page) = store.events_after(after, 128) else {
            eprintln!("worker journal readable=false");
            return;
        };
        let Some(last) = page.last() else { break };
        after = last.seq;
        events.extend(
            page.into_iter()
                .filter(|event| event.kind != "lease_renewal_observed"),
        );
        if events.len() > 4096 {
            break;
        }
    }
    let origin = events.first().map_or(0, |event| event.at_ms);
    for event in events.iter().rev().take(96).rev() {
        // The last characters tell two attempts apart; the time is relative
        // to the first event, so a silent wait shows as a gap.
        let attempt = event
            .attempt_id
            .as_deref()
            .and_then(|id| id.get(id.len().saturating_sub(6)..))
            .unwrap_or("-");
        let at = event.at_ms.saturating_sub(origin);
        match event.kind.as_str() {
            "agent_environment_changed" | "agent_prepare_failed" | "agent_spawn_failed" => {
                eprintln!(
                    "worker event: +{at}ms {attempt} {} code={} reason={} message={}",
                    event.kind,
                    event.payload["code"],
                    event.payload["blocked_reason"],
                    event.payload["message"]
                )
            }
            "attempt_state" => eprintln!(
                "worker event: +{at}ms {attempt} attempt_state {}->{}",
                event.payload["from"], event.payload["to"]
            ),
            "agent_setup_unavailable" => eprintln!(
                "worker event: +{at}ms {attempt} {} reason={}",
                event.kind, event.payload["reason"]
            ),
            kind => eprintln!("worker event: +{at}ms {attempt} {kind}"),
        }
    }
}

fn diagnose_coordinator(spec: &Spec) {
    let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) else {
        eprintln!("coordinator journal readable=false");
        return;
    };
    let Ok(events) = store.events_after(0, 128) else {
        eprintln!("coordinator journal readable=false");
        return;
    };
    for event in events.iter().rev().take(32).rev() {
        let (kind, code, reason) = coordinator_event_summary(event);
        let refusal = coordinator_refusal_class(event);
        eprintln!("coordinator event: {kind} code={code:?} reason={reason:?} refusal={refusal:?}");
    }
}

#[test]
fn coordinator_timeout_diagnostic_keeps_only_kind_and_typed_codes() {
    let mut event = remote_task_runner::storage::Event {
        seq: 1,
        run_id: None,
        attempt_id: None,
        attempt_seq: None,
        kind: "submit_deferred".into(),
        at_ms: 0,
        payload: serde_json::json!({"code":"BLOCKED", "reason":"secret_unavailable",
            "message":"PRIVATE_DIAGNOSTIC_SENTINEL", "payload":{"env":"PRIVATE_DIAGNOSTIC_SENTINEL"}}),
    };
    assert_eq!(
        coordinator_event_summary(&event),
        (
            "submit_deferred",
            Some(remote_task_runner::error::ErrorCode::Blocked),
            Some(remote_task_runner::error::BlockedReason::SecretUnavailable)
        )
    );
    assert!(
        !format!("{:?}", coordinator_event_summary(&event)).contains("PRIVATE_DIAGNOSTIC_SENTINEL")
    );
    assert_eq!(coordinator_refusal_class(&event), None);
    for (message, expected) in [
        ("toolchain_baseline_mismatch", "native_baseline_mismatch"),
        (
            "worker capabilities changed or unmet: ",
            "profile_report_unavailable",
        ),
        (
            "worker capabilities changed or unmet: PRIVATE_DIAGNOSTIC_SENTINEL",
            "requirements_unmet",
        ),
    ] {
        event.payload["message"] = serde_json::json!(message);
        assert_eq!(coordinator_refusal_class(&event), Some(expected));
        assert!(!format!("{:?}", coordinator_refusal_class(&event))
            .contains("PRIVATE_DIAGNOSTIC_SENTINEL"));
    }
    event.payload = serde_json::json!({"code":"PRIVATE_DIAGNOSTIC_SENTINEL", "reason":"PRIVATE_DIAGNOSTIC_SENTINEL"});
    assert_eq!(
        coordinator_event_summary(&event),
        ("submit_deferred", None, None)
    );
}

// Private fixture metadata only: never dump argv, env, paths, journal payloads,
// or arbitrary parse errors. Cap reads even when a child leaves malformed files.
pub(super) fn diagnose_attempt(spec: &Spec, attempt: &remote_task_runner::ids::AttemptId) {
    use remote_task_runner::supervisor::spec::{command_id, SuperviseSpec};
    use task_traffic_controller::managed::Sidecar;
    fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Option<T> {
        let file = std::fs::File::open(path).ok()?;
        if file.metadata().ok()?.len() > 256 * 1024 {
            return None;
        }
        serde_json::from_reader(file.take(256 * 1024)).ok()
    }
    let dir = spec
        .worker_state_dir
        .join("attempts")
        .join(attempt.as_str());
    let input = dir.join("supervise.json");
    let Some(supervise) = read_json::<SuperviseSpec>(&input) else {
        eprintln!("supervisor spec exists={} readable=false", input.exists());
        return;
    };
    let traffic = &supervise.traffic_state_dir;
    let ledger_path = traffic.join("ledger.json");
    let ledger = read_json::<serde_json::Value>(&ledger_path);
    eprintln!(
        "private TTC ledger exists={} readable={} entries={:?}",
        ledger_path.exists(),
        ledger.is_some(),
        ledger
            .as_ref()
            .and_then(|l| l.get("entries_v2").or_else(|| l.get("entries")))
            .and_then(|e| e.as_array())
            .map(Vec::len)
    );
    for index in 0..supervise.nodes.len().min(8) {
        let id = command_id(attempt, index);
        let sidecar_path = traffic.join("managed").join(format!("{id}.json"));
        let sidecar = read_json::<Sidecar>(&sidecar_path);
        eprintln!(
            "private TTC node={index} pipe_exists={} lease_exists={} sidecar_exists={} readable={}",
            remote_task_runner::supervisor::spool::logs_dir(&supervise.spool_dir(), attempt)
                .join(format!("node-{index}.pipe"))
                .exists(),
            traffic.join("leases").join(format!("{id}.lock")).exists(),
            sidecar_path.exists(),
            sidecar.is_some()
        );
        if let Some(sc) = sidecar {
            eprintln!(
                "private TTC phase={:?} owner_pid={} child_pid={:?} terminal={} descendants={}",
                sc.phase,
                sc.owner_pid,
                sc.child.map(|c| c.pid),
                sc.terminal.is_some(),
                sc.descendants.len()
            );
        }
    }
}
async fn shutdown(client: &Client) {
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
}
fn load(spec: &Spec) -> Client {
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    Client::load(&spec.state_dir, store.cluster_id().clone()).unwrap()
}

#[test]
#[ignore = "invoked as a dedicated launcher subprocess by the process regression"]
fn launcher_process() {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes).unwrap();
    let spec: Spec = serde_json::from_slice(&bytes).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(ensure(spec)).unwrap();
    // Runtime teardown and launcher exit must leave the owner running.
}

#[test]
#[ignore = "invoked as a dedicated panicking subprocess by the cleanup regression"]
fn panicking_fixture_process() {
    let mut record = String::new();
    std::io::stdin().read_to_string(&mut record).unwrap();
    let (_root, spec) = fixture();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (pid, start) = rt.block_on(async {
        let client = ensure(spec).await.unwrap();
        let pid = super::common::listener_pid(client.socket_path())
            .await
            .unwrap();
        (pid, super::common::identity_of(pid).unwrap())
    });
    std::fs::write(record.trim(), format!("{pid}\n{start}")).unwrap();
    // The test body fails before any explicit shutdown, as a failed assert does.
    panic!("fixture test failed with a live owner");
}

/// The task-traffic-controller scope of a worker started from `env`.
fn worker_traffic(env: &BTreeMap<String, String>) -> remote_task_runner::config::traffic::Scope {
    remote_task_runner::config::traffic::resolve(env).unwrap()
}

#[tokio::test]
async fn panicking_fixture_test_leaves_no_owner_process() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let record = dir.path().join("owner");
    let mut helper = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bootstrap::panicking_fixture_process",
            "--ignored",
            "--nocapture",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    helper
        .stdin
        .take()
        .unwrap()
        .write_all(record.to_str().unwrap().as_bytes())
        .unwrap();
    let output = tokio::task::spawn_blocking(move || helper.wait_with_output().unwrap())
        .await
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("fixture test failed with a live owner"),
        "{stderr}"
    );
    let text = std::fs::read_to_string(&record).unwrap();
    let (pid, start) = text.split_once('\n').unwrap();
    let pid: u32 = pid.parse().unwrap();
    assert!(pid > 1);
    // The fixture root stopped the owner before the helper process ended.
    assert_ne!(
        remote_task_runner::platform::process_start_identity(pid),
        remote_task_runner::platform::StartIdentity::Identity(start.to_string())
    );
}

#[tokio::test]
async fn owner_survives_launcher_exit_and_rotation_reconnects() {
    let (_root, spec) = fixture();
    let mut launcher = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bootstrap::launcher_process",
            "--ignored",
            "--nocapture",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    launcher
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&spec).unwrap())
        .unwrap();
    let output = tokio::task::spawn_blocking(move || launcher.wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let client = load(&spec);
    let epoch = match client.request(Operation::Ping).await.unwrap() {
        Reply::Ready { epoch } => epoch,
        other => panic!("{other:?}"),
    };
    shutdown(&client).await;
    let mut again = spec.clone();
    again.expected_cluster = Some(client.cluster_id().clone());
    let fresh = ensure(again).await.unwrap();
    assert!(
        matches!(fresh.request(Operation::Ping).await.unwrap(), Reply::Ready { epoch: e } if e != epoch)
    );
    assert!(client.request(Operation::Ping).await.is_err());
    assert_eq!(fresh.cluster_id(), client.cluster_id());
    shutdown(&fresh).await;
}

#[tokio::test]
async fn concurrent_startup_has_one_owner_and_foreign_cluster_is_refused() {
    let (_root, spec) = fixture();
    let mut joins = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let copy = spec.clone();
        joins.spawn(ensure(copy));
    }
    let mut clients = Vec::new();
    while let Some(result) = joins.join_next().await {
        clients.push(result.unwrap().unwrap());
    }
    let first = clients.first().unwrap();
    let before = first.request(Operation::Ping).await.unwrap();
    for client in &clients {
        assert_eq!(client.cluster_id(), first.cluster_id());
        assert_eq!(
            serde_json::to_value(client.request(Operation::Ping).await.unwrap()).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
    }
    let mut foreign = spec.clone();
    foreign.expected_cluster = Some(ClusterId::generate());
    assert!(ensure(foreign)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("foreign"));
    shutdown(first).await;
}

#[tokio::test]
async fn unsafe_keys_and_tokens_are_rejected_without_changing_files() {
    let (_root, spec) = fixture();
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(ensure(spec.clone()).await.is_err());
    assert!(!spec.state_dir.join("coordinator.db").exists());
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let key = std::fs::read(&spec.cluster_key_path).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&spec.cluster_key_path)
        .unwrap()
        .set_len(1 << 34)
        .unwrap();
    assert!(ensure(spec.clone())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("exceeds"));
    assert!(!spec.state_dir.join("coordinator.db").exists());
    std::fs::write(&spec.cluster_key_path, key).unwrap();
    let client = ensure(spec.clone()).await.unwrap();
    let token = client.socket_path().parent().unwrap().join("owner.token");
    let original = std::fs::read(&token).unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let alias = token.with_extension("alias");
    std::fs::hard_link(&token, &alias).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::remove_file(alias).unwrap();
    let saved = token.with_extension("saved");
    std::fs::rename(&token, &saved).unwrap();
    std::os::unix::fs::symlink(&saved, &token).unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::remove_file(&token).unwrap();
    std::fs::rename(saved, &token).unwrap();
    std::fs::write(&token, "bad-token").unwrap();
    assert!(Client::load(&spec.state_dir, client.cluster_id().clone()).is_err());
    std::fs::write(&token, original).unwrap();
    shutdown(&client).await;
}

#[tokio::test]
async fn retained_input_is_private_idempotent_and_restarts_through_shared_argv() {
    use remote_task_runner::service::bootstrap::{daemon_argv, write_input};
    let (root, spec) = fixture();
    let input = root.path().join("resident.json");
    let managed = write_input(&input, &spec).unwrap();
    let same = write_input(&input, &spec).unwrap();
    assert_eq!(
        (managed.device, managed.inode, &managed.sha256),
        (same.device, same.inode, &same.sha256)
    );
    assert_eq!(
        std::fs::metadata(&input).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut changed = spec.clone();
    changed.worker_id = WorkerId::parse("different").unwrap();
    assert!(write_input(&input, &changed).is_err());
    let mut previous = None;
    for _ in 0..2 {
        let mut child = tokio::process::Command::new(&spec.executable)
            .args(daemon_argv(&input).unwrap())
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let client = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(child.try_wait().unwrap().is_none(), "daemon exited early");
                if let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) {
                    if let Ok(client) = Client::load(&spec.state_dir, store.cluster_id().clone()) {
                        if matches!(
                            client.request(Operation::Ping).await,
                            Ok(Reply::Ready { .. })
                        ) {
                            break client;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(spec.worker_state_dir.join("boot_id")).unwrap(),
            remote_task_runner::platform::boot_id().unwrap()
        );
        let worker =
            remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                .unwrap();
        assert_eq!(worker.worker_id(), &spec.worker_id);
        let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
        if let Some(epoch) = previous {
            assert_ne!(store.epoch(), epoch);
        }
        previous = Some(store.epoch());
        shutdown(&client).await;
        assert!(child.wait().await.unwrap().success());
        assert!(input.exists());
    }
    std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o644)).unwrap();
    let failed = tokio::process::Command::new(&spec.executable)
        .args(daemon_argv(&input).unwrap())
        .stdin(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(!failed.status.success());
    assert!(write_input(&input, &spec).is_err());
}

#[tokio::test]
async fn failed_child_and_interrupted_startup_do_not_keep_startup_lock() {
    let (root, spec) = fixture();
    // System utilities may be symlinks, rejected before startup by the private
    // executable checks. Exercise an actual failed child on every platform.
    let executable = root.path().join("failed-child");
    let started = root.path().join("failed-child-started");
    std::fs::write(
        &executable,
        "#!/bin/sh\n: > \"$BOOTSTRAP_FAILED_MARKER\"\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut failed = spec.clone();
    failed.executable = executable;
    failed.env.insert(
        "BOOTSTRAP_FAILED_MARKER".into(),
        started.to_str().unwrap().into(),
    );
    let error = ensure(failed).await.err().expect("child must fail startup");
    assert!(started.exists(), "failed child did not start: {error:#}");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(spec.state_dir.join("startup.lock"))
        .unwrap();
    lock.lock().unwrap();
    let waiting = tokio::spawn(ensure(spec.clone()));
    tokio::time::sleep(Duration::from_millis(80)).await;
    waiting.abort();
    assert!(waiting.await.err().unwrap().is_cancelled());
    drop(lock);
    let client = ensure(spec).await.unwrap();
    shutdown(&client).await;
}

/// What a prepared template leaves under the worker state: directories
/// without write permission, which a plain recursive removal cannot empty.
fn seal_template(worker: &std::path::Path) -> std::path::PathBuf {
    let template = worker.join("setup/templates/sha256-sealed");
    std::fs::create_dir_all(template.join("tree")).unwrap();
    std::fs::write(template.join("tree/prepared.txt"), b"x").unwrap();
    for dir in [template.join("tree"), template.clone()] {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    }
    template
}

#[tokio::test]
async fn failed_bootstrap_leaves_no_fixture_directory() {
    let (root, spec) = fixture();
    let path = root.path().to_path_buf();
    assert!(path.starts_with(crate::test_tmp::root()));
    let executable = root.path().join("failed-child");
    std::fs::write(&executable, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut failed = spec.clone();
    failed.executable = executable;
    ensure(failed).await.err().expect("child must fail startup");
    seal_template(&spec.worker_state_dir);
    drop(root);
    assert!(!path.exists(), "the failed fixture left {path:?}");
}

#[tokio::test]
async fn stopped_owner_with_sealed_templates_leaves_no_fixture_directory() {
    let (root, spec) = fixture();
    let path = root.path().to_path_buf();
    let client = ensure(spec.clone()).await.unwrap();
    client.request(Operation::Ping).await.unwrap();
    // The owner and its worker are running and have written their state.
    assert!(spec.worker_state_dir.join("worker.db").exists());
    seal_template(&spec.worker_state_dir);
    // No explicit shutdown: the root stops the owner, then removes everything.
    drop(root);
    assert!(!path.exists(), "the stopped fixture left {path:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_queries_cli_configured_fixture_does_not_advance_epoch() {
    use remote_task_runner::ids::{RunId, TaskId};
    use serde_json::json;
    let (root, spec) = fixture();
    let run = RunId::generate();
    let task = TaskId::generate();
    {
        let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
        owner
            .create_run(&run, None, None, &json!({"env":"PRIVATE_PAYLOAD"}), 1)
            .unwrap();
        owner
            .create_task(&task, &run, &json!({"argv":["PRIVATE_PAYLOAD"]}), 1)
            .unwrap();
        owner.close_run(&run, 2).unwrap();
    }
    let dir = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    remote_task_runner::service::bootstrap::write_input(&dir.join("coordinator.json"), &spec)
        .unwrap();
    // Simulate v3 with durable rows and no owner, removing later indexes and
    // feature markers as well as tables so every migration can run again.
    let db = rusqlite::Connection::open(spec.state_dir.join("coordinator.db")).unwrap();
    db.execute_batch(
        "DROP TRIGGER result_gc_ref_insert; DROP TABLE result_gc_ref; DROP TABLE result_delete_claim; DROP TABLE result_gc_clock; DROP INDEX attempt_task_key; DROP TABLE collection_log; DROP TRIGGER task_inherit_cancel; DROP TRIGGER task_keep_cancel; DROP TRIGGER attempt_cancel_gate; DROP TRIGGER run_inherit_control_revision; DROP TABLE run_cancel_intent; DROP TABLE control_request; ALTER TABLE run DROP COLUMN control_revision; ALTER TABLE task DROP COLUMN min_generation; ALTER TABLE task DROP COLUMN authorization_revision; DROP INDEX task_run_key; DROP INDEX event_task_waiting; DROP INDEX placement_unacked; DROP INDEX attempt_worker_live; DROP TABLE worker_identity; DROP TABLE worker_identity_tombstone; DROP TABLE ci_test_timing; DROP TABLE workflow_download; DROP TABLE workflow_artifact; DROP TABLE ci_driver_transition; DROP TRIGGER matrix_attempt_ended; DROP INDEX matrix_permit_held; DELETE FROM schema_meta WHERE key IN ('execution_requirements_v1','frozen_toolchains_v1'); UPDATE schema_meta SET value='3' WHERE key='schema_version';",
    )
    .unwrap();
    let old_epoch: i64 = db
        .query_row("SELECT epoch FROM coordinator_state", [], |r| r.get(0))
        .unwrap();
    drop(db);
    for args in [
        vec!["--json", "ls"],
        vec!["--json", "show", run.as_str()],
        vec!["--json", "show", task.as_str()],
    ] {
        let output = Command::new(&spec.executable)
            .args(args)
            .current_dir(root.path())
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(output.stderr.is_empty());
    }
    assert!(
        !spec.state_dir.join("parent").exists(),
        "read-only CLI started owner IPC"
    );
    assert!(
        !spec.state_dir.join("startup.lock").exists(),
        "read-only CLI entered bootstrap"
    );
    let db = rusqlite::Connection::open_with_flags(
        spec.state_dir.join("coordinator.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    assert_eq!(
        db.query_row("SELECT epoch FROM coordinator_state", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        old_epoch
    );
    assert_eq!(
        db.query_row(
            "SELECT value FROM schema_meta WHERE key='schema_version'",
            [],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "3"
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='task_run_key'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    drop(db);
    let client = ensure(spec.clone()).await.unwrap();
    let epoch = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .epoch();
    for args in [
        vec!["--json", "ls", "--limit", "1"],
        vec!["--json", "show", run.as_str()],
        vec!["--json", "show", task.as_str()],
    ] {
        let output = Command::new(&spec.executable)
            .args(args)
            .current_dir(root.path())
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(output.stderr.is_empty());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains("PRIVATE_PAYLOAD"));
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["ok"], true);
    }
    for (id, code) in [
        ("invalid".to_string(), 2),
        (RunId::generate().to_string(), 73),
    ] {
        let output = Command::new(&spec.executable)
            .args(["--json", "show", &id])
            .current_dir(root.path())
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(code));
        assert!(output.stderr.is_empty());
    }
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    shutdown(&client).await;
}

#[tokio::test]
async fn readonly_queries_refuse_foreign_cluster_and_unsafe_key_without_owner() {
    let (_root, mut spec) = fixture();
    let epoch = {
        let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
        owner.epoch()
    };
    spec.expected_cluster = Some(ClusterId::generate());
    assert!(
        remote_task_runner::service::query::Reader::open(spec.clone())
            .await
            .is_err()
    );
    spec.expected_cluster = None;
    std::fs::set_permissions(
        &spec.cluster_key_path,
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(
        remote_task_runner::service::query::Reader::open(spec.clone())
            .await
            .is_err()
    );
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    assert!(!spec.state_dir.join("startup.lock").exists());
    assert!(!spec.state_dir.join("parent").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_cli_submit_captures_worktree_and_secrets_then_outlives_caller() {
    use remote_task_runner::ids::TaskId;
    use remote_task_runner::service::bootstrap::write_input;
    let (root, mut spec) = fixture();
    let repo = root.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["config", "user.name", "Fixture"]);
    std::fs::write(repo.join("input.txt"), "committed").unwrap();
    std::fs::write(repo.join(".gitignore"), "private.env\n").unwrap();
    std::fs::write(
        repo.join(".remote-task-runner.toml"),
        "schema_version = 1\n[source]\nsecret_files = [{ glob = 'private.env', required = true }]\n",
    )
    .unwrap();
    git(&["add", "."]);
    git(&["commit", "-qm", "fixture"]);
    std::fs::write(repo.join("input.txt"), "dirty-snapshot").unwrap();
    let secret = "fixture-secret-value-abcdefghijkl";
    std::fs::write(repo.join("private.env"), format!("TOKEN={secret}\n")).unwrap();
    spec.env.insert(
        "PATH".into(),
        private_tool_path(root.path(), &["git", "fast-worktree", "wc"]),
    );
    let configdir = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&configdir).unwrap();
    std::fs::set_permissions(&configdir, std::fs::Permissions::from_mode(0o700)).unwrap();
    write_input(&configdir.join("coordinator.json"), &spec).unwrap();
    let output=tokio::process::Command::new(&spec.executable).args(["--json","submit","--","/bin/sh","-c","set -e; sleep 1; test \"$(cat input.txt)\" = dirty-snapshot; test \"$(wc -c <private.env)\" -eq 40; cat private.env"])
        .current_dir(&repo).env_clear().envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile|("LLVM_PROFILE_FILE",profile))).envs(&spec.env).output().await.unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["state"], "coordinator_accepted");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    let task = TaskId::parse(value["result"]["task_id"].as_str().unwrap()).unwrap();
    let second = tokio::process::Command::new(&spec.executable)
        .args(["--json", "submit", "--", "/usr/bin/true"])
        .current_dir(&repo)
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .output()
        .await
        .unwrap();
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
    let second: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(
        second["result"]["source_id"], value["result"]["source_id"],
        "same immutable input must be reusable"
    );
    let second_task = TaskId::parse(second["result"]["task_id"].as_str().unwrap()).unwrap();
    std::fs::write(repo.join("input.txt"), "changed-after-caller-exit").unwrap();
    std::fs::write(repo.join("private.env"), "changed-after-capture").unwrap();
    let client = load(&spec);
    let attempts = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await
                .unwrap()
            {
                if attempts
                    .iter()
                    .any(|a| a.state == remote_task_runner::state::AttemptState::Terminal)
                {
                    break attempts;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let attempts = match attempts {
        Ok(attempts) => attempts,
        Err(error) => {
            let snapshot = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await;
            eprintln!("timed out task snapshot: {snapshot:?}");
            diagnose_coordinator(&spec);
            if let Ok(Reply::Task { attempts }) = &snapshot {
                let worker = remote_task_runner::storage::WorkerStore::open_read_only(
                    &spec.worker_state_dir,
                )
                .unwrap();
                for a in attempts.iter().take(8) {
                    diagnose_attempt(&spec, &a.attempt_id);
                    eprintln!(
                        "worker launch intents={:?}",
                        worker.launches(&a.attempt_id).map(|l| l.len())
                    );
                    if let Some(rec) = worker.get(&a.attempt_id).unwrap() {
                        eprintln!("worker state: {:?}", rec.state);
                    }
                    for event in worker.events_of_attempt(&a.attempt_id, 30).unwrap() {
                        eprintln!("worker event: {}", event.kind);
                    }
                }
            }
            shutdown(&client).await;
            panic!("terminal timeout: {error}");
        }
    };
    if attempts.len() != 1
        || attempts[0].termination.as_ref().map(|end| end.outcome())
            != Some(remote_task_runner::state::Outcome::Succeeded)
    {
        let worker =
            remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir);
        for attempt in attempts.iter().take(8) {
            diagnose_attempt(&spec, &attempt.attempt_id);
            if let Ok(worker) = &worker {
                if let Ok(Some(record)) = worker.get(&attempt.attempt_id) {
                    eprintln!("terminal fixture worker state: {:?}", record.state);
                }
                if let Ok(events) = worker.events_of_attempt(&attempt.attempt_id, 30) {
                    for event in events {
                        eprintln!("terminal fixture worker event: {}", event.kind);
                    }
                }
            }
        }
        // Drain the private owner before reporting terminal preparation or
        // execution failures, just as the timeout branch does.
        shutdown(&client).await;
        panic!("submitted fixture did not succeed: {attempts:?}");
    }
    let attempt_dir = spec
        .worker_state_dir
        .join("attempts")
        .join(attempts[0].attempt_id.as_str());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !attempt_dir.join("workspace.json").exists()
                && !attempt_dir.join("supervise.json").exists()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let results =
        remote_task_runner::results::ResultStore::open(&spec.state_dir.join("results")).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if results
                .read_result(&attempts[0].attempt_id)
                .unwrap()
                .is_some()
                && results
                    .log_len(
                        &attempts[0].attempt_id,
                        remote_task_runner::wire::LogStream::Stdout,
                    )
                    .unwrap()
                    > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let log = results
        .read_log(
            &attempts[0].attempt_id,
            remote_task_runner::wire::LogStream::Stdout,
        )
        .unwrap();
    assert!(!String::from_utf8_lossy(&log).contains(secret));
    assert!(
        !log.is_empty(),
        "collected masked stdout must survive workspace cleanup"
    );
    let sources = open_sources(&spec.state_dir);
    let id =
        remote_task_runner::ids::RunId::parse(value["result"]["run_id"].as_str().unwrap()).unwrap();
    let ci = sources.source_of_run(&id).unwrap().unwrap();
    let manifest = sources.manifest(&ci).unwrap().unwrap();
    assert!(!manifest
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.0 == b"private.env"));
    assert_eq!(manifest.secret_files.len(), 1);
    let vault = remote_task_runner::secrets::vault::Vault::open(&spec.state_dir).unwrap();
    let key = remote_task_runner::secrets::key::load(&spec.cluster_key_path).unwrap();
    assert!(vault.has(&id));
    assert_eq!(
        vault.load(&id, &manifest.secret_files, &key).unwrap().files[0]
            .content
            .0,
        format!("TOKEN={secret}\n").as_bytes()
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: second_task.clone(),
                })
                .await
                .unwrap()
            {
                if let Some(a) = attempts
                    .iter()
                    .find(|a| a.state == remote_task_runner::state::AttemptState::Terminal)
                {
                    if a.termination.as_ref().unwrap().outcome()
                        != remote_task_runner::state::Outcome::Succeeded
                    {
                        eprintln!("second attempt: {a:?}");
                        let worker = remote_task_runner::storage::WorkerStore::open_read_only(
                            &spec.worker_state_dir,
                        )
                        .unwrap();
                        for event in worker.events_of_attempt(&a.attempt_id, 40).unwrap() {
                            eprintln!(
                                "second event {}: code={:?} message={:?}",
                                event.kind,
                                event.payload.get("code"),
                                event.payload.get("message")
                            );
                        }
                        eprintln!(
                            "second result: {:?}",
                            worker.get_result(&a.attempt_id).unwrap()
                        );
                        shutdown(&client).await;
                        panic!("same-source second attempt failed");
                    }
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    // Same-run retry must use the retained run bundle after the file changes.
    let target = remote_task_runner::service::completion::Target::Task(task.clone());
    let accepted = client
        .request(Operation::Retry {
            target: target.clone(),
            expected_control_revision: 0,
            request_id: "retained-secret".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(accepted, Reply::RetryAccepted { .. }),
        "{accepted:?}"
    );
    let (_sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        remote_task_runner::service::control::wait(&spec, target.clone(), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    if report.cli_exit_code != 0 {
        eprintln!("retained retry snapshot: {:?}", report.snapshot);
        if let Ok(Reply::Task { attempts }) = client
            .request(Operation::Task {
                task_id: task.clone(),
            })
            .await
        {
            let worker =
                remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                    .unwrap();
            for attempt in attempts.iter().take(8) {
                diagnose_attempt(&spec, &attempt.attempt_id);
                for event in worker.events_of_attempt(&attempt.attempt_id, 30).unwrap() {
                    eprintln!(
                        "retry event: {} code={:?}",
                        event.kind,
                        event.payload.get("code")
                    );
                }
            }
        }
        shutdown(&client).await;
        panic!("retained retry failed");
    }
    vault.erase(&id).unwrap();
    let unavailable = client
        .request(Operation::Retry {
            target,
            expected_control_revision: 1,
            request_id: "erased-secret".into(),
        })
        .await
        .unwrap();
    let Reply::ControlError { error } = unavailable else {
        panic!("{unavailable:?}")
    };
    assert_eq!(
        error.blocked_reason,
        Some(remote_task_runner::error::BlockedReason::SecretUnavailable)
    );
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(owner.attempts_of_task(&task).unwrap().len(), 2);
    assert_eq!(
        owner
            .completion(remote_task_runner::service::completion::Target::Run(
                id.clone()
            ))
            .unwrap()
            .control_revision,
        1
    );
    drop(owner);
    let count_runs = || {
        let db = rusqlite::Connection::open_with_flags(
            spec.state_dir.join("coordinator.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        db.query_row("SELECT COUNT(*) FROM run", [], |row| row.get::<_, i64>(0))
            .unwrap()
    };
    let before = count_runs();
    std::fs::remove_file(repo.join("private.env")).unwrap();
    let failed = tokio::process::Command::new(&spec.executable)
        .args(["--json", "submit", "--", "/usr/bin/true"])
        .current_dir(&repo)
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .output()
        .await
        .unwrap();
    assert!(!failed.status.success());
    let error: serde_json::Value = serde_json::from_slice(&failed.stdout).unwrap();
    assert_eq!(error["ok"], false);
    assert_eq!(count_runs(), before, "failed capture dispatched a run");
    shutdown(&client).await;
}

#[test]
fn command_capture_directory_is_bound_once_and_source_view_reuses_it() {
    use remote_task_runner::service::{context::Context, input};
    let (root, spec) = fixture();
    let dir = root.path().join("directory");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("input"), "first immutable input").unwrap();
    std::fs::write(dir.join("ignored.txt"), "excluded by directory policy").unwrap();
    std::fs::write(
        dir.join(".remote-task-runner.toml"),
        "schema_version=1\n[[outputs]]\nname='report'\npaths=['report.txt']\n[source]\nexclude=['ignored.txt']\n",
    ).unwrap();
    let mut context = Context::load(spec.env.clone(), dir.clone(), spec).unwrap();
    assert!(context.repo_root.is_none());
    assert_eq!(context.config.outputs.len(), 1);
    let run = remote_task_runner::ids::RunId::generate();
    let first = input::capture(&context, &run, None).unwrap();
    assert!(!first
        .manifest
        .object_manifest
        .entries
        .iter()
        .any(|entry| entry.path.0 == b"ignored.txt"));
    std::fs::write(dir.join("input"), "later content").unwrap();
    let second = input::capture(&context, &run, None).unwrap();
    assert_eq!(first.manifest.source_id, second.manifest.source_id);
    let view = root.path().join("view");
    input::source_view(&context, &run, &view).unwrap();
    assert_eq!(
        std::fs::read_to_string(view.join("input")).unwrap(),
        "first immutable input"
    );
    assert_eq!(
        std::fs::read_to_string(dir.join("input")).unwrap(),
        "later content"
    );
    assert!(
        !context.state_dir().join("coordinator.db").exists(),
        "capture took writable owner state"
    );
    assert!(input::capture(
        &context,
        &remote_task_runner::ids::RunId::generate(),
        Some("HEAD")
    )
    .is_err());
    context.config.source_mode.value = remote_task_runner::config::model::SourceMode::Commit;
    assert!(input::capture(&context, &remote_task_runner::ids::RunId::generate(), None).is_err());
    let mut invalid_env = context.env.clone();
    invalid_env.insert(
        "GIT_DIR".into(),
        root.path().join("absent-git-dir").display().to_string(),
    );
    assert!(
        Context::load(invalid_env, dir, context.bootstrap.clone()).is_err(),
        "invalid Git context must not become a directory snapshot"
    );
}

#[test]
fn uninitialized_submit_is_typed_without_creating_config_or_state() {
    let root = crate::test_tmp::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
        .args(["--json", "submit", "--", "true"])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .env("HOME", root.path())
        .env("PATH", "/usr/bin:/bin")
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error"]["code"], "NOT_FOUND");
    assert!(value["error"]["message"].as_str().unwrap().contains("init"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn command_commit_capture_requires_and_preserves_explicit_revision() {
    use remote_task_runner::service::{context::Context, input};
    let (root, spec) = fixture();
    let repo = root.path().join("commit-repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    std::fs::write(repo.join("input"), "explicit old revision").unwrap();
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    let old = git(&["rev-parse", "HEAD"]).trim().to_string();
    std::fs::write(repo.join("input"), "new HEAD").unwrap();
    git(&["commit", "-qam", "second"]);
    std::fs::write(repo.join("input"), "dirty worktree").unwrap();
    let mut context = Context::load(spec.env.clone(), repo.clone(), spec).unwrap();
    context.config.source_mode.value = remote_task_runner::config::model::SourceMode::Commit;
    assert!(input::capture(&context, &remote_task_runner::ids::RunId::generate(), None).is_err());
    let run = remote_task_runner::ids::RunId::generate();
    let captured = input::capture(&context, &run, Some(&old)).unwrap();
    assert_eq!(
        captured.manifest.original_head.as_deref(),
        Some(old.as_str())
    );
    let view = root.path().join("commit-view");
    input::source_view(&context, &run, &view).unwrap();
    assert_eq!(
        std::fs::read_to_string(view.join("input")).unwrap(),
        "explicit old revision"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("input")).unwrap(),
        "dirty worktree"
    );
}

/// Opens the source store of a state directory a daemon is running on. The open
/// is nonblocking by design and the daemon takes the exclusive source guard for
/// its retention passes, so a test that looks at the store while the daemon is
/// up sees a transient `WouldBlock`; only a guard that stays held fails it.
fn open_sources(state: &std::path::Path) -> remote_task_runner::source::SourceStore {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match remote_task_runner::source::SourceStore::open(state) {
            Err(error)
                if std::time::Instant::now() < deadline
                    && error.chain().any(|cause| {
                        cause
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|io| io.kind() == std::io::ErrorKind::WouldBlock)
                    }) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => return other.unwrap(),
        }
    }
}
fn control_fixture() -> (super::common::DaemonRoot, Spec, std::path::PathBuf) {
    let (root, spec) = fixture();
    let config = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        config.join("config.toml"),
        "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\n",
    )
    .unwrap();
    remote_task_runner::service::bootstrap::write_input(&config.join("coordinator.json"), &spec)
        .unwrap();
    let input = root.path().join("input");
    std::fs::create_dir(&input).unwrap();
    std::fs::write(input.join("input.txt"), "snapshot").unwrap();
    (root, spec, input)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_control_exec_exit_and_offline_wait_ignore_current_project_and_git() {
    let (_root, spec, input) = control_fixture();
    let mut run = None;
    for (script, exit) in [("exit 0", 0), ("exit 23", 23), ("kill -TERM $$", 143)] {
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new(&spec.executable)
                .args(["--json", "exec", "--", "/bin/sh", "-c", script])
                .env_clear()
                .envs(
                    std::env::var_os("LLVM_PROFILE_FILE")
                        .map(|profile| ("LLVM_PROFILE_FILE", profile)),
                )
                .envs(&spec.env)
                .current_dir(&input)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            output.status.code(),
            Some(exit),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["result"]["state"], "settled");
        assert_eq!(value["result"]["snapshot"]["collection_pending_tasks"], 0);
        if exit == 23 {
            run = Some(
                value["result"]["target"]["id"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }
    let client = load(&spec);
    let epoch = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .epoch();
    shutdown(&client).await;
    std::fs::write(
        input.join(".remote-task-runner.toml"),
        "invalid project config [",
    )
    .unwrap();
    for args in [
        vec!["ls"],
        vec!["show", run.as_deref().unwrap()],
        vec!["wait", run.as_deref().unwrap()],
    ] {
        let output = tokio::process::Command::new(&spec.executable)
            .arg("--json")
            .args(&args)
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&spec.env)
            .env("PATH", "/does-not-contain-git")
            .current_dir(&input)
            .output()
            .await
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if args[0] == "wait" { 23 } else { 0 }),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(serde_json::from_slice::<serde_json::Value>(&output.stdout).is_ok());
    }
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_interrupt_before_acceptance_is_durable_and_late_submit_never_starts() {
    use remote_task_runner::service::{
        completion::Target,
        context::Context,
        control,
        query::{Query, Response},
    };
    let (_root, spec, input) = control_fixture();
    let context = Context::load(spec.env.clone(), input, spec.clone()).unwrap();
    let (sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    sender.send(()).unwrap();
    let report = control::exec(
        context.clone(),
        vec!["/bin/sh".into(), "-c".into(), "exit 99".into()],
        &mut signals,
    )
    .await
    .unwrap();
    assert_eq!(report.state, "stop_confirmed");
    assert_eq!(report.cli_exit_code, 130);
    assert!(report.cancel_acknowledged);
    assert!(!report.snapshot.as_ref().unwrap().accepted);
    let Target::Run(run) = report.target else {
        panic!("exec must bind a run")
    };
    let task = report.task_id.unwrap();
    remote_task_runner::service::submit::command_with_ids(
        context.clone(),
        vec!["/bin/sh".into(), "-c".into(), "exit 99".into()],
        None,
        run.clone(),
        task,
    )
    .await
    .unwrap();
    let reader = context.readonly().await.unwrap();
    let Response::Completion { snapshot } = reader
        .query(Query::Completion {
            target: Target::Run(run),
        })
        .await
        .unwrap()
    else {
        panic!("completion")
    };
    assert!(snapshot.accepted);
    assert_eq!(snapshot.task_count, 1);
    assert_eq!(snapshot.exit_code(), Some(130));
    assert!(CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .list_nonterminal_attempts()
        .unwrap()
        .is_empty());
    let missing = remote_task_runner::ids::TaskId::generate().to_string();
    let error = tokio::process::Command::new(&spec.executable)
        .args(["--json", "cancel", &missing])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .env("PATH", "/no-git")
        .current_dir(&context.cwd)
        .output()
        .await
        .unwrap();
    assert_eq!(
        error.status.code(),
        Some(73),
        "{}",
        String::from_utf8_lossy(&error.stdout)
    );
    let absent = remote_task_runner::ids::RunId::generate().to_string();
    let receipt = tokio::process::Command::new(&spec.executable)
        .args(["--json", "cancel", &absent])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .env("PATH", "/no-git")
        .current_dir(&context.cwd)
        .output()
        .await
        .unwrap();
    assert!(receipt.status.success());
    let value: serde_json::Value = serde_json::from_slice(&receipt.stdout).unwrap();
    assert_eq!(value["result"]["cancel_acknowledged"], true);
    assert_eq!(value["result"]["snapshot"]["accepted"], false);
    shutdown(&load(&spec)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_wait_interrupt_detaches_without_cancelling_work() {
    use remote_task_runner::service::{completion::Target, context::Context, control};
    let (_root, spec, input) = control_fixture();
    let context = Context::load(spec.env.clone(), input, spec.clone()).unwrap();
    let receipt = remote_task_runner::service::submit::command(
        context.clone(),
        vec!["/bin/sh".into(), "-c".into(), "sleep 1; exit 0".into()],
        None,
    )
    .await
    .unwrap();
    let (sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    sender.send(()).unwrap();
    let target = Target::Task(receipt.task_id.clone());
    let detached = control::wait(&spec, target.clone(), &mut signals)
        .await
        .unwrap();
    assert_eq!(detached.state, "wait_interrupted");
    assert!(!detached.cancel_requested);
    assert!(CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .task_cancel_requested(&receipt.task_id)
        .unwrap()
        .is_none());
    let finished = tokio::time::timeout(
        Duration::from_secs(20),
        control::wait(&spec, target, &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(finished.cli_exit_code, 0);
    shutdown(&load(&spec)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_real_exec_sigint_only_signals_owned_cli_and_confirms_worker_stop() {
    control_exec_sigint(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_place_real_exec_sigint_confirms_stop_and_preserves_original() {
    control_exec_sigint(true).await;
}
async fn control_exec_sigint(in_place: bool) {
    let (_root, spec, input) = control_fixture();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(if in_place {
            vec![
                "--json",
                "exec",
                "--in-place",
                "--",
                "/bin/sh",
                "-c",
                "sleep 30",
            ]
        } else {
            vec!["--json", "exec", "--", "/bin/sh", "-c", "sleep 30"]
        })
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .current_dir(&input)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "CLI exited before its command started"
            );
            if let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) {
                if store
                    .list_nonterminal_attempts()
                    .unwrap()
                    .iter()
                    .any(|a| a.receipts.running_at_ms.is_some())
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|elapsed| {
        diagnose_coordinator(&spec);
        diagnose_worker(&spec);
        panic!("no attempt reached running: {elapsed}");
    });
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    let output = tokio::time::timeout(Duration::from_secs(35), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(output.status.code(), Some(130 | 143)),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["state"], "stop_confirmed");
    assert_eq!(value["result"]["cancel_acknowledged"], true);
    assert!(CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .list_nonterminal_attempts()
        .unwrap()
        .is_empty());
    shutdown(&load(&spec)).await;
    assert!(input.is_dir());
    assert_eq!(
        std::fs::read_to_string(input.join("input.txt")).unwrap(),
        "snapshot"
    );
    if in_place {
        assert_eq!(value["result"]["snapshot"]["in_place"], true);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_wait_reopens_after_owner_restart_without_new_attempt() {
    use remote_task_runner::service::{completion::Target, context::Context, control};
    let (_root, spec, input) = control_fixture();
    let context = Context::load(spec.env.clone(), input, spec.clone()).unwrap();
    let receipt = remote_task_runner::service::submit::command(
        context,
        vec!["/bin/sh".into(), "-c".into(), "sleep 3; exit 0".into()],
        None,
    )
    .await
    .unwrap();
    let (_sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let reading_spec = spec.clone();
    let target = Target::Task(receipt.task_id.clone());
    let waiting =
        tokio::spawn(async move { control::wait(&reading_spec, target, &mut signals).await });
    let client = load(&spec);
    shutdown(&client).await;
    let restarted = ensure(spec.clone()).await.unwrap();
    let waited = tokio::time::timeout(Duration::from_secs(20), waiting).await;
    let result = match waited {
        Ok(result) => result.unwrap().unwrap(),
        Err(error) => {
            let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
            eprintln!(
                "restart completion: {:?}",
                store
                    .completion(remote_task_runner::service::completion::Target::Task(
                        receipt.task_id.clone()
                    ))
                    .unwrap()
            );
            let attempts = store.attempts_of_task(&receipt.task_id).unwrap();
            let worker =
                remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                    .unwrap();
            for attempt in attempts {
                eprintln!(
                    "parent attempt: state={:?} observation={:?} termination={:?}",
                    attempt.state, attempt.observation, attempt.termination
                );
                for event in worker.events_of_attempt(&attempt.attempt_id, 40).unwrap() {
                    eprintln!(
                        "restart event {} code={:?} reason={:?} verdict={:?}",
                        event.kind,
                        event.payload.get("code"),
                        event.payload.get("reason"),
                        event.payload.get("verdict")
                    );
                }
            }
            shutdown(&restarted).await;
            panic!("wait did not settle after owner restart: {error}");
        }
    };
    assert_eq!(result.cli_exit_code, 0);
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .attempts_of_task(&receipt.task_id)
            .unwrap()
            .len(),
        1
    );
    shutdown(&restarted).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_exec_succeeded_compute_but_missing_collection_exits_78() {
    use remote_task_runner::service::{context::Context, control};
    let (_root, spec, input) = control_fixture();
    let mut context = Context::load(spec.env.clone(), input, spec.clone()).unwrap();
    context
        .config
        .outputs
        .push(remote_task_runner::config::resolve::OutputSpec {
            name: "required.txt".into(),
            paths: vec!["not-created.txt".into()],
            required: true,
            when: remote_task_runner::config::model::OutputWhen::Always,
        });
    let (_sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    // The outer bound must exceed the 20s the exec itself allows for the
    // resident to start (and the run after it): with an equal bound a slow
    // start on a loaded or instrumented runner trips this unwrap before the
    // exec can report anything.
    let report = tokio::time::timeout(
        Duration::from_secs(60),
        control::exec(
            context,
            vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
            &mut signals,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.state, "settled");
    assert_eq!(report.cli_exit_code, 78);
    let snapshot = report.snapshot.unwrap();
    assert_eq!(
        snapshot.first_termination.unwrap().outcome(),
        remote_task_runner::state::Outcome::Succeeded
    );
    assert_eq!(snapshot.collection_failed_tasks, 1);
    assert_eq!(snapshot.collection_pending_tasks, 0);
    shutdown(&load(&spec)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_same_run_cli_uses_original_inputs_and_restart_receipt() {
    use remote_task_runner::ids::{RunId, TaskId};
    use remote_task_runner::service::{completion::Target, control};
    let (_root, spec, input) = control_fixture();
    let output = tokio::process::Command::new(&spec.executable)
        .args([
            "--json",
            "exec",
            "--",
            "/bin/sh",
            "-c",
            "cat input.txt; exit 23",
        ])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|value| ("LLVM_PROFILE_FILE", value)))
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(23),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let initial: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let run = RunId::parse(initial["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let task = TaskId::parse(initial["result"]["task_id"].as_str().unwrap()).unwrap();
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let old = owner.attempts_of_task(&task).unwrap().remove(0);
    drop(owner);
    std::fs::write(input.join("input.txt"), "changed after initial capture").unwrap();
    // Retry ignores current project configuration and does not need git.
    std::fs::write(input.join(".remote-task-runner.toml"), "invalid [ config").unwrap();
    let client = load(&spec);
    // Treat this accepted cancellation response as lost by the transport.
    let cancelled = client
        .request(Operation::Cancel {
            target: Target::Run(run.clone()),
            expected_control_revision: 0,
            request_id: "lost-cancel-response".into(),
        })
        .await
        .unwrap();
    let Reply::Cancelled {
        snapshot: cancel_ack,
    } = cancelled
    else {
        panic!("{cancelled:?}")
    };
    assert_eq!(cancel_ack.control_revision, 1);
    shutdown(&client).await;
    let retry = tokio::process::Command::new(&spec.executable)
        .args(["--json", "retry", run.as_str()])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|value| ("LLVM_PROFILE_FILE", value)))
        .envs(&spec.env)
        .env("PATH", "/no-git")
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    assert!(
        retry.status.success(),
        "{} {}",
        String::from_utf8_lossy(&retry.stdout),
        String::from_utf8_lossy(&retry.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&retry.stdout).unwrap();
    assert_eq!(value["result"]["state"], "queued");
    let receipt = value["result"]["retry_receipt"].clone();
    assert_eq!(receipt["run_id"], run.as_str());
    assert_eq!(receipt["control_revision"], 2);
    for forbidden in [
        "changed after initial capture",
        "commands",
        "manifest_digest",
        "snapshot",
    ] {
        assert!(!receipt.to_string().contains(forbidden));
    }
    let (_sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        control::wait(&spec, Target::Run(run.clone()), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.cli_exit_code, 23);
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let attempts = owner.attempts_of_task(&task).unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], old);
    assert_eq!(attempts[1].generation.0, 2);
    drop(owner);
    let results =
        remote_task_runner::results::ResultStore::open(&spec.state_dir.join("results")).unwrap();
    let log = results
        .read_log(
            &attempts[1].attempt_id,
            remote_task_runner::wire::LogStream::Stdout,
        )
        .unwrap();
    assert!(String::from_utf8_lossy(&log).contains("snapshot"));
    let client = load(&spec);
    assert!(matches!(
        client
            .request(Operation::Cancel {
                request_id: RunId::generate().to_string(),
                target: Target::Run(run.clone()),
                expected_control_revision: 0
            })
            .await
            .unwrap(),
        Reply::ControlError { .. }
    ));
    shutdown(&client).await;
    let client = ensure(spec.clone()).await.unwrap();
    let replay = client
        .request(Operation::Retry {
            target: Target::Run(run.clone()),
            expected_control_revision: 1,
            request_id: receipt["request_id"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    let Reply::RetryAccepted { receipt: replayed } = replay else {
        panic!("{replay:?}")
    };
    assert_eq!(serde_json::to_value(replayed).unwrap(), receipt);
    let sources = open_sources(&spec.state_dir);
    let source = sources.source_of_run(&run).unwrap().unwrap();
    let manifest = sources.manifest(&source).unwrap().unwrap();
    let blob = &manifest.object_manifest.entries[0].hash;
    std::fs::remove_file(sources.object_store().unwrap().object_path(blob)).unwrap();
    let rejected = client
        .request(Operation::Retry {
            target: Target::Run(run.clone()),
            expected_control_revision: 2,
            request_id: "missing-source".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(rejected, Reply::ControlError { .. }),
        "{rejected:?}"
    );
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(
        owner
            .completion(Target::Run(run.clone()))
            .unwrap()
            .control_revision,
        2
    );
    assert_eq!(owner.attempts_of_task(&task).unwrap().len(), 2);
    assert!(
        !owner
            .completion(Target::Run(run.clone()))
            .unwrap()
            .cancel_requested
    );
    drop(owner);
    let replayed = client
        .request(Operation::Cancel {
            target: Target::Run(run.clone()),
            expected_control_revision: 0,
            request_id: "lost-cancel-response".into(),
        })
        .await
        .unwrap();
    let Reply::Cancelled {
        snapshot: replayed_ack,
    } = replayed
    else {
        panic!("{replayed:?}")
    };
    assert_eq!(
        serde_json::to_value(cancel_ack).unwrap(),
        serde_json::to_value(replayed_ack).unwrap()
    );
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let snapshot = owner.completion(Target::Run(run)).unwrap();
    assert_eq!(snapshot.control_revision, 2);
    assert!(!snapshot.cancel_requested);
    drop(owner);
    shutdown(&client).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_logs_offline_raw_ndjson_and_sigint_detach_without_owner_or_git() {
    use remote_task_runner::ids::{AttemptId, Digest, RunId, TaskId};
    use remote_task_runner::state::AttemptState;
    use remote_task_runner::storage::NewAttempt;
    use remote_task_runner::wire::LogStream;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let (root, spec, _) = control_fixture();
    let run = RunId::generate();
    let task = TaskId::generate();
    let attempt = AttemptId::generate();
    let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
    owner
        .create_run(
            &run,
            None,
            None,
            &serde_json::json!({"secret":"NO_LOG_PAYLOAD"}),
            1,
        )
        .unwrap();
    owner
        .create_task(&task, &run, &serde_json::json!({}), 1)
        .unwrap();
    owner
        .create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task.clone(),
                worker_id: spec.worker_id.clone(),
                payload_hash: Digest::of_bytes(b"p"),
                permit_id: None,
            },
            2,
        )
        .unwrap();
    for state in [
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
    ] {
        owner.transition_attempt(&attempt, state, 3).unwrap();
    }
    let files =
        remote_task_runner::results::store::ResultStore::open(&spec.state_dir.join("results"))
            .unwrap();
    owner
        .prepare_log_progress(&attempt, LogStream::Stdout, 3)
        .unwrap();
    files
        .append_log(&attempt, LogStream::Stdout, 0, b"\xff\0active")
        .unwrap();
    owner
        .commit_log_progress(&attempt, LogStream::Stdout, 0, 8, false, false, 0, 4)
        .unwrap();
    let epoch = owner.epoch();
    drop(owner);
    let mut snapshot = tokio::process::Command::new(&spec.executable);
    snapshot
        .args(["logs", task.as_str()])
        .env_clear()
        .envs(&spec.env)
        .env("PATH", "/no-programs")
        .current_dir(root.path());
    let output = tokio::time::timeout(Duration::from_secs(10), snapshot.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"\xff\0active");
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(["--json", "logs", task.as_str(), "--follow"])
        .env_clear()
        .envs(&spec.env)
        .env("PATH", "/no-programs")
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().unwrap());
    let mut first = String::new();
    tokio::time::timeout(Duration::from_secs(10), stdout.read_line(&mut first))
        .await
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.status.code(), Some(130));
    let mut rest = Vec::new();
    stdout.read_to_end(&mut rest).await.unwrap();
    let all = [first.as_bytes(), rest.as_slice()].concat();
    let records = all
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_slice::<serde_json::Value>(s).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        records
            .iter()
            .filter(|r| r["data"]["kind"] == "final")
            .count(),
        1
    );
    assert_eq!(records.last().unwrap()["data"]["summary"]["detached"], true);
    let reader = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(reader.epoch(), epoch);
    assert!(reader.task_cancel_requested(&task).unwrap().is_none());
    assert!(reader
        .get_attempt(&attempt)
        .unwrap()
        .unwrap()
        .cancel_requested_at_ms
        .is_none());
    assert!(!spec.state_dir.join("parent").exists());
    drop(reader);
    let owner = CoordinatorStore::open(&spec.state_dir, 5).unwrap();
    let bytes = vec![b'x'; 1024 * 1024];
    files
        .append_log(&attempt, LogStream::Stdout, 8, &bytes)
        .unwrap();
    owner
        .commit_log_progress(
            &attempt,
            LogStream::Stdout,
            8,
            8 + bytes.len() as u64,
            false,
            false,
            0,
            6,
        )
        .unwrap();
    drop(owner);
    let mut blocked = tokio::process::Command::new(&spec.executable)
        .args(["--json", "logs", task.as_str(), "--follow"])
        .env_clear()
        .envs(&spec.env)
        .env("PATH", "/no-programs")
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = blocked.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    let mut unconsumed = tokio::io::BufReader::new(blocked.stdout.take().unwrap());
    let mut first = String::new();
    tokio::time::timeout(Duration::from_secs(10), unconsumed.read_line(&mut first))
        .await
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    // Deliberately stop reading while an encoded 64-KiB chunk fills the pipe.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(blocked.try_wait().unwrap().is_none());
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    // wait() does NOT drain stdout. Detach must finish even with no consumer.
    let status = tokio::time::timeout(Duration::from_secs(3), blocked.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(130));
    assert!(CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .task_cancel_requested(&task)
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_logs_closed_run_final_blocked_stdout_still_detaches() {
    use remote_task_runner::ids::RunId;
    use std::os::fd::OwnedFd;
    let (root, spec, _) = control_fixture();
    let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
    let run = RunId::generate();
    owner
        .create_run(&run, None, None, &serde_json::json!({}), 1)
        .unwrap();
    owner.close_run(&run, 2).unwrap();
    let epoch = owner.epoch();
    drop(owner);
    // This closed no-work run emits no status/chunk records: its only stdout
    // operation is final. Prefill an owned socket so that final cannot finish.
    let (_unconsumed, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();
    let buffer = [0_u8; 16384];
    let mut filled = 0;
    loop {
        match writer.write(&buffer) {
            Ok(n) => {
                assert!(n > 0);
                filled += n;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("prefill stdout: {error}"),
        }
    }
    assert!(filled > 0);
    writer.set_nonblocking(false).unwrap();
    let stdout: OwnedFd = writer.into();
    let temporary = root.path().join("reader-tmp");
    std::fs::create_dir(&temporary).unwrap();
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(["--json", "logs", run.as_str(), "--follow"])
        .env_clear()
        .envs(&spec.env)
        .env("TMPDIR", &temporary)
        .env("PATH", "/no-programs")
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if std::fs::read_dir(&temporary).unwrap().next().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Allow the empty, read-only sweep to reach its sole final write.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(child.try_wait().unwrap().is_none());
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(130));
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    assert_eq!(
        std::fs::read_dir(&temporary).unwrap().count(),
        0,
        "cursor directory leaked during blocked final detach"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_outputs_blocked_stdout_detaches_without_cancel_or_owner() {
    use remote_task_runner::ids::RunId;
    use std::os::fd::OwnedFd;
    let (root, spec, _) = control_fixture();
    let owner = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
    let run = RunId::generate();
    owner
        .create_run(&run, None, None, &serde_json::json!({}), 1)
        .unwrap();
    owner.close_run(&run, 2).unwrap();
    drop(owner);
    // Prefill an owned socket: observation output must remain interruptible.
    let (_unconsumed, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
    writer.set_nonblocking(true).unwrap();
    let buffer = [0_u8; 16384];
    let mut filled = 0;
    loop {
        match writer.write(&buffer) {
            Ok(n) => {
                assert!(n > 0);
                filled += n;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("prefill stdout: {error}"),
        }
    }
    assert!(filled > 0);
    writer.set_nonblocking(false).unwrap();
    let stdout: OwnedFd = writer.into();
    let temporary = root.path().join("reader-tmp");
    std::fs::create_dir(&temporary).unwrap();
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(["--json", "outputs", run.as_str(), "--dir"])
        .arg(root.path().join("exported"))
        .env_clear()
        .envs(&spec.env)
        .env("TMPDIR", &temporary)
        .env("PATH", "/no-programs")
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if root.path().join("exported").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Allow the read-only observer to reach its blocked status write.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(child.try_wait().unwrap().is_none());
    let gc_owner = CoordinatorStore::open(&spec.state_dir, 10).unwrap();
    assert!(
        matches!(gc_owner.result_gc_guard(&run), Err(remote_task_runner::storage::StorageError::Io(e)) if e.kind()==std::io::ErrorKind::WouldBlock)
    );
    let epoch = gc_owner.epoch();
    drop(gc_owner);
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(130));

    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    let gc_owner = CoordinatorStore::open(&spec.state_dir, 11).unwrap();
    drop(gc_owner.result_gc_guard(&run).unwrap());
    drop(gc_owner);
    assert!(
        !CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .completion(remote_task_runner::service::completion::Target::Run(run))
            .unwrap()
            .cancel_requested
    );
    assert_eq!(
        std::fs::read_dir(&temporary).unwrap().count(),
        0,
        "output reader leaked a temporary file"
    );
}

fn seed_public_output(
    spec: &Spec,
    bytes: &[u8],
    encoding: remote_task_runner::wire::ArtifactEncoding,
) -> (
    remote_task_runner::ids::RunId,
    remote_task_runner::ids::TaskId,
    remote_task_runner::ids::AttemptId,
    remote_task_runner::wire::ArtifactRecord,
) {
    use remote_task_runner::ids::{AttemptId, Digest, RunId, SourceId, TaskId};
    use remote_task_runner::results::ResultStore;
    use remote_task_runner::state::{AttemptState, Outcome, Termination};
    use remote_task_runner::storage::{ArtifactReceipt, NewAttempt};
    use remote_task_runner::wire::ArtifactRecord;
    let db = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
    let run = RunId::generate();
    let task = TaskId::generate();
    let attempt = AttemptId::generate();
    db.create_run(&run, None, None, &serde_json::json!({}), 1)
        .unwrap();
    db.create_task(&task, &run, &serde_json::json!({}), 1)
        .unwrap();
    db.create_attempt(
        NewAttempt {
            attempt_id: attempt.clone(),
            task_id: task.clone(),
            worker_id: spec.worker_id.clone(),
            payload_hash: Digest::of_bytes(b"payload"),
            permit_id: None,
        },
        2,
    )
    .unwrap();
    for state in [
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
        AttemptState::Finalizing,
    ] {
        db.transition_attempt(&attempt, state, 3).unwrap();
    }
    db.finalize_attempt(
        &attempt,
        Termination::new(Outcome::Succeeded, None, None).unwrap(),
        4,
    )
    .unwrap();
    let artifact = ArtifactRecord {
        encoding,
        name: "bundle".into(),
        path: "PRIVATE_DECLARATION_SENTINEL".into(),
        size: bytes.len() as u64,
        digest: Digest::of_bytes(bytes),
        source_id: SourceId::generate(),
        plan_digest: Digest::of_bytes(b"plan"),
        attempt_id: attempt.clone(),
        profile_fingerprint: Digest::of_bytes(b"profile"),
    };
    db.begin_collection(
        &attempt,
        &Digest::of_bytes(b"result"),
        &serde_json::json!({"artifacts":[artifact.clone()]}),
        &[(artifact.name.clone(), true)],
        &[remote_task_runner::storage::ResultRef {
            ref_id: format!("{attempt}:result"),
            kind: remote_task_runner::storage::BlobKind::Result,
            locator: ResultStore::result_locator(&attempt),
        }],
        5,
    )
    .unwrap();
    db.record_collected_artifact(
        &ArtifactReceipt {
            attempt_id: attempt.clone(),
            name: artifact.name.clone(),
            rel_path: artifact.path.clone(),
            size: artifact.size,
            digest: artifact.digest.clone(),
            source_id: artifact.source_id.clone(),
            plan_digest: artifact.plan_digest.clone(),
            profile_fingerprint: artifact.profile_fingerprint.to_string(),
        },
        &remote_task_runner::storage::ResultRef {
            ref_id: format!("{attempt}:artifact:{}", artifact.name),
            kind: remote_task_runner::storage::BlobKind::Result,
            locator: ResultStore::artifact_locator(&attempt, &artifact.digest),
        },
        6,
    )
    .unwrap();
    let store = ResultStore::open(&spec.state_dir.join("results")).unwrap();
    store
        .append_part(&attempt, &artifact.digest, 0, bytes)
        .unwrap();
    store
        .publish_artifact(&attempt, &artifact.digest, artifact.size)
        .unwrap();
    drop(store);
    drop(db);
    (run, task, attempt, artifact)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_outputs_retained_corruption_is_internal_before_publication() {
    use remote_task_runner::ids::Digest;
    use remote_task_runner::results::ResultStore;
    use remote_task_runner::wire::ArtifactEncoding;
    let mut builder = tar::Builder::new(Vec::new());
    for path in ["ordinary", "nested/.GiT/config"] {
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_mode(0o600);
        header.set_cksum();
        builder
            .append_data(&mut header, path, b"x".as_slice())
            .unwrap();
    }
    let reserved = builder.into_inner().unwrap();
    let mut checksum = reserved.clone();
    checksum[0] ^= 1;
    for bytes in [reserved, checksum, vec![0; 511]] {
        let (root, spec, _) = control_fixture();
        let (run, task, attempt, artifact) =
            seed_public_output(&spec, &bytes, ArtifactEncoding::TarV1);
        let destination = root.path().join("exported");
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(&spec.executable)
                .args(["--json", "outputs", task.as_str(), "--dir"])
                .arg(&destination)
                .env_clear()
                .envs(&spec.env)
                .env("PATH", "/no-programs")
                .current_dir(root.path())
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            output.status.code(),
            Some(79),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("INTERNAL"), "{text}");
        assert!(!text.contains("PRIVATE_DECLARATION_SENTINEL"));
        assert_eq!(std::fs::read_dir(&destination).unwrap().count(), 0);
        let files = ResultStore::open(&spec.state_dir.join("results")).unwrap();
        std::fs::remove_file(files.artifact_path(&attempt, &artifact.digest)).unwrap();
        let missing = tokio::process::Command::new(&spec.executable)
            .args(["--json", "outputs", task.as_str(), "--dir"])
            .arg(&destination)
            .env_clear()
            .envs(&spec.env)
            .env("PATH", "/no-programs")
            .current_dir(root.path())
            .output()
            .await
            .unwrap();
        assert_eq!(
            missing.status.code(),
            Some(79),
            "live reference must preserve corruption classification"
        );
        files
            .append_part(&attempt, &artifact.digest, 0, &bytes)
            .unwrap();
        files
            .publish_artifact(&attempt, &artifact.digest, artifact.size)
            .unwrap();
        let db = CoordinatorStore::open(&spec.state_dir, 20).unwrap();
        db.close_collection(
            &attempt,
            Some(&Digest::of_bytes(b"result")),
            remote_task_runner::results::plan::Verdict::Complete,
            true,
            8,
        )
        .unwrap();
        let guard = crate::test_tmp::gc_guard(&db, &run);
        assert!(
            db.claim_result_page(&guard, &run, "", 64, 100, 10, Some("boot".into()), 10)
                .unwrap()
                .clock_held
        );
        assert_eq!(
            db.claim_result_page(&guard, &run, "", 64, 110, 20, Some("boot".into()), 10)
                .unwrap()
                .removed,
            2
        );
        let claims = db.pending_result_page(&guard, &run, 64).unwrap();
        drop(guard);
        drop(db); // Simulate owner crash after durable claim, before filesystem deletion.
        for phase in 0..2 {
            let unavailable = tokio::process::Command::new(&spec.executable)
                .args(["--json", "outputs", task.as_str(), "--dir"])
                .arg(&destination)
                .env_clear()
                .envs(&spec.env)
                .env("PATH", "/no-programs")
                .current_dir(root.path())
                .output()
                .await
                .unwrap();
            assert_eq!(
                unavailable.status.code(),
                Some(73),
                "phase {phase}: {}",
                String::from_utf8_lossy(&unavailable.stdout)
            );
            assert_eq!(std::fs::read_dir(&destination).unwrap().count(), 0);
            if phase == 0 {
                assert!(files.artifact_path(&attempt, &artifact.digest).exists());
                let restarted = CoordinatorStore::open(&spec.state_dir, 30).unwrap();
                let guard = crate::test_tmp::gc_guard(&restarted, &run);
                for claim in &claims {
                    files.remove(&claim.locator).unwrap();
                    restarted.finish_result_delete(&guard, claim).unwrap();
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_place_exec_updates_original_directory_and_retries_mutable_input_after_restart() {
    use remote_task_runner::{
        ids::{RunId, TaskId},
        service::{completion::Target, control},
    };
    let (_root, spec, input) = control_fixture();
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(&spec.executable)
            .args([
                "--json",
                "exec",
                "--in-place",
                "--",
                "/bin/sh",
                "-c",
                "cat input.txt > output.txt; exit 23",
            ])
            .env_clear()
            .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|v| ("LLVM_PROFILE_FILE", v)))
            .envs(&spec.env)
            .current_dir(&input)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        out.status.code(),
        Some(23),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["result"]["snapshot"]["in_place"], true);
    let run = RunId::parse(value["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let task = TaskId::parse(value["result"]["task_id"].as_str().unwrap()).unwrap();
    assert_eq!(
        std::fs::read_to_string(input.join("output.txt")).unwrap(),
        "snapshot"
    );
    shutdown(&load(&spec)).await;
    std::fs::write(input.join("input.txt"), "mutable retry").unwrap();
    std::fs::write(input.join(".remote-task-runner.toml"), "invalid [ config").unwrap();
    let report = control::retry(&spec, Target::Run(run.clone()))
        .await
        .unwrap();
    assert!(report.retry_receipt.is_some());
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let done = control::wait(&spec, Target::Run(run.clone()), &mut signals)
        .await
        .unwrap();
    assert_eq!(done.cli_exit_code, 23);
    assert!(done.snapshot.unwrap().in_place);
    assert_eq!(
        std::fs::read_to_string(input.join("output.txt")).unwrap(),
        "mutable retry"
    );
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(store.attempts_of_task(&task).unwrap().len(), 2);
    let public = serde_json::to_string(&store.query_task(task).unwrap()).unwrap();
    assert!(public.contains("\"in_place\":true"));
    assert!(!public.contains(input.to_str().unwrap()));
    let source = open_sources(&spec.state_dir);
    let source_id = source.source_of_run(&run).unwrap().unwrap();
    let manifest = source.manifest(&source_id).unwrap().unwrap();
    let baseline = input.parent().unwrap().join("baseline-check");
    source.materialize(&manifest, &baseline).unwrap();
    assert_eq!(
        std::fs::read_to_string(baseline.join("input.txt")).unwrap(),
        "snapshot"
    );
    shutdown(&load(&spec)).await;
    assert!(input.is_dir());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_place_secret_values_are_read_only_masked_and_inventory_changes_reject_retry() {
    use remote_task_runner::{
        ids::{RunId, TaskId},
        service::{completion::Target, control},
        wire::LogStream,
    };
    let (_root, spec, input) = control_fixture();
    for args in [
        vec!["init", "-q"],
        vec!["config", "user.name", "Fixture"],
        vec!["config", "user.email", "fixture@example.invalid"],
    ] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(&input)
            .envs(&spec.env)
            .status()
            .unwrap()
            .success());
    }
    std::fs::write(input.join(".gitignore"), ".env*\n").unwrap();
    std::fs::write(
        input.join(".remote-task-runner.toml"),
        "schema_version = 1\n[source]\nsecret_files = [\".env*\"]\n",
    )
    .unwrap();
    assert!(Command::new("git")
        .args(["add", "."])
        .current_dir(&input)
        .envs(&spec.env)
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args(["commit", "-qm", "fixture"])
        .current_dir(&input)
        .envs(&spec.env)
        .status()
        .unwrap()
        .success());
    let secret = input.join(".env.test");
    std::fs::write(&secret, "TOKEN=initial-private-secret\n").unwrap();
    let original_mode = std::fs::metadata(&secret).unwrap().permissions().mode();
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(&spec.executable)
            .args([
                "--json",
                "exec",
                "--in-place",
                "--",
                "/bin/sh",
                "-c",
                "cat .env.test; exit 23",
            ])
            .env_clear()
            .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|v| ("LLVM_PROFILE_FILE", v)))
            .envs(&spec.env)
            .current_dir(&input)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        out.status.code(),
        Some(23),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run = RunId::parse(value["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let task = TaskId::parse(value["result"]["task_id"].as_str().unwrap()).unwrap();
    let old = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .attempts_of_task(&task)
        .unwrap()
        .remove(0);
    let files =
        remote_task_runner::results::ResultStore::open(&spec.state_dir.join("results")).unwrap();
    let log = files.read_log(&old.attempt_id, LogStream::Stdout).unwrap();
    assert!(!String::from_utf8_lossy(&log).contains("initial-private-secret"));
    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "TOKEN=initial-private-secret\n"
    );
    assert_eq!(
        std::fs::metadata(&secret).unwrap().permissions().mode(),
        original_mode
    );
    remote_task_runner::secrets::vault::Vault::open(&spec.state_dir)
        .unwrap()
        .erase(&run)
        .unwrap();
    std::fs::write(&secret, "TOKEN=changed-private-secret\n").unwrap();
    control::retry(&spec, Target::Run(run.clone()))
        .await
        .unwrap();
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let done = control::wait(&spec, Target::Run(run.clone()), &mut signals)
        .await
        .unwrap();
    assert_eq!(done.cli_exit_code, 23);
    let second = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .attempts_of_task(&task)
        .unwrap()
        .pop()
        .unwrap();
    let log = files
        .read_log(&second.attempt_id, LogStream::Stdout)
        .unwrap();
    assert!(!String::from_utf8_lossy(&log).contains("changed-private-secret"));
    std::fs::write(input.join(".env.new"), "TOKEN=new-private-secret\n").unwrap();
    assert!(control::retry(&spec, Target::Run(run)).await.is_err());
    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "TOKEN=changed-private-secret\n"
    );
    assert!(input.join(".env.new").exists());
    shutdown(&load(&spec)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_place_preparation_runs_in_original_and_artifacts_use_normal_collection() {
    use remote_task_runner::service::{context::Context, control};
    let (_root, spec, input) = control_fixture();
    let mut context = Context::load(spec.env.clone(), input.clone(), spec.clone()).unwrap();
    context.config.prepare_commands = vec![vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf preparation > prepared.txt".into(),
    ]];
    context
        .config
        .outputs
        .push(remote_task_runner::config::resolve::OutputSpec {
            name: "kept".into(),
            paths: vec!["output.txt".into()],
            required: true,
            when: remote_task_runner::config::model::OutputWhen::Always,
        });
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let done = control::exec_mode(
        context,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "cat prepared.txt > output.txt".into(),
        ],
        true,
        &mut signals,
    )
    .await
    .unwrap();
    assert_eq!(done.cli_exit_code, 0);
    assert!(done.snapshot.unwrap().in_place);
    assert_eq!(
        std::fs::read_to_string(input.join("output.txt")).unwrap(),
        "preparation"
    );
    let task = done.task_id.unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert_eq!(store.attempts_of_task(&task).unwrap().len(), 1);
    shutdown(&load(&spec)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_place_retry_rejects_original_subdirectory_replaced_by_external_symlink() {
    use remote_task_runner::{
        ids::{RunId, TaskId},
        service::{completion::Target, control},
    };
    let (root, spec, input) = control_fixture();
    for args in [
        vec!["init", "-q"],
        vec!["config", "user.name", "Fixture"],
        vec!["config", "user.email", "fixture@example.invalid"],
    ] {
        assert!(Command::new("git")
            .args(args)
            .current_dir(&input)
            .envs(&spec.env)
            .status()
            .unwrap()
            .success());
    }
    let sub = input.join("sub");
    std::fs::create_dir(&sub).unwrap();
    std::fs::write(sub.join("input.txt"), "original subdirectory").unwrap();
    std::fs::write(input.join(".gitignore"), ".env.test\n").unwrap();
    std::fs::write(
        input.join(".remote-task-runner.toml"),
        "schema_version = 1\n[source]\nsecret_files = [\".env.test\"]\n",
    )
    .unwrap();
    assert!(Command::new("git")
        .args(["add", "."])
        .current_dir(&input)
        .envs(&spec.env)
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args(["commit", "-qm", "fixture"])
        .current_dir(&input)
        .envs(&spec.env)
        .status()
        .unwrap()
        .success());
    let secret = input.join(".env.test");
    std::fs::write(&secret, "TOKEN=private-original-secret\n").unwrap();
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(&spec.executable)
            .args([
                "--json",
                "exec",
                "--in-place",
                "--",
                "/bin/sh",
                "-c",
                "printf ran > marker; exit 23",
            ])
            .env_clear()
            .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|v| ("LLVM_PROFILE_FILE", v)))
            .envs(&spec.env)
            .current_dir(&sub)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        out.status.code(),
        Some(23),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run = RunId::parse(value["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let task = TaskId::parse(value["result"]["task_id"].as_str().unwrap()).unwrap();
    assert!(sub.join("marker").exists());
    shutdown(&load(&spec)).await;
    let saved = input.join("saved-sub");
    std::fs::rename(&sub, &saved).unwrap();
    let outside = root.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, &sub).unwrap();
    control::retry(&spec, Target::Run(run.clone()))
        .await
        .unwrap();
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let done = tokio::time::timeout(
        Duration::from_secs(30),
        control::wait(&spec, Target::Run(run), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(done.cli_exit_code, 0);
    assert_ne!(done.cli_exit_code, 23);
    assert!(!outside.join("marker").exists());
    assert_eq!(
        std::fs::read_to_string(saved.join("input.txt")).unwrap(),
        "original subdirectory"
    );
    assert_eq!(
        std::fs::read_to_string(&secret).unwrap(),
        "TOKEN=private-original-secret\n"
    );
    let attempts = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .attempts_of_task(&task)
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(attempts[1].receipts.running_at_ms.is_none());
    shutdown(&load(&spec)).await;
    assert!(input.is_dir());
    assert!(saved.join("marker").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_outputs_sigint_drains_blocked_tar_worker_before_releasing_pin() {
    use std::os::fd::OwnedFd;
    let (root, spec, _) = control_fixture();
    let mut builder = tar::Builder::new(Vec::new());
    for n in 0..2048 {
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_mode(0o600);
        header.set_cksum();
        builder
            .append_data(&mut header, format!("member-{n:04}"), b"x".as_slice())
            .unwrap();
    }
    let bytes = builder.into_inner().unwrap();
    let (run, task, attempt, _) = seed_public_output(
        &spec,
        &bytes,
        remote_task_runner::wire::ArtifactEncoding::TarV1,
    );
    let (_unconsumed, writer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stdout: OwnedFd = writer.into();
    let temporary = root.path().join("reader-tmp");
    std::fs::create_dir(&temporary).unwrap();
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(["--json", "outputs", run.as_str(), "--dir"])
        .arg(root.path().join("exported"))
        .env_clear()
        .envs(&spec.env)
        .env("TMPDIR", &temporary)
        .env("PATH", "/no-programs")
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if root
                .path()
                .join("exported")
                .join(task.as_str())
                .join(attempt.as_str())
                .join("bundle/member-0000")
                .exists()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // The bounded channel and unread stdout leave the owned worker mid-export.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(child.try_wait().unwrap().is_none());
    let gc_owner = CoordinatorStore::open(&spec.state_dir, 10).unwrap();
    assert!(
        matches!(gc_owner.result_gc_guard(&run), Err(remote_task_runner::storage::StorageError::Io(e)) if e.kind()==std::io::ErrorKind::WouldBlock)
    );
    let epoch = gc_owner.epoch();
    drop(gc_owner);
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(130));

    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    let gc_owner = CoordinatorStore::open(&spec.state_dir, 11).unwrap();
    drop(gc_owner.result_gc_guard(&run).unwrap());
    drop(gc_owner);
    assert!(
        !CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .completion(remote_task_runner::service::completion::Target::Run(run))
            .unwrap()
            .cancel_requested
    );
    let members = root
        .path()
        .join("exported")
        .join(task.as_str())
        .join(attempt.as_str())
        .join("bundle");
    let names = std::fs::read_dir(&members)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect::<Vec<_>>();
    assert!(!names.is_empty() && names.len() < 2048);
    assert!(
        names
            .iter()
            .all(|name| name.to_str().unwrap().starts_with("member-")),
        "owned destination temp leaked"
    );
    assert_eq!(
        std::fs::read_dir(&temporary).unwrap().count(),
        0,
        "output reader leaked a temporary file"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capability_cli_named_profile_and_requires_use_actual_path_and_freeze_retry() {
    use remote_task_runner::service::{completion::Target, control};
    use remote_task_runner::toolchain::capability::ExecutionRequirements;
    let (root, spec, input) = control_fixture();
    let bin = root.path().join("profile-bin");
    // The host tools this test does not use are answered by the fixture.
    let stubs = root.path().join("tool-stubs");
    std::fs::create_dir(&bin).unwrap();
    crate::common::write_executable(&bin.join("node"), "#!/bin/sh\nprintf 'v22.0.0\\n'\n");
    // The shell is named through a symlink, as /bin/sh is on many Linux hosts.
    let shell = root.path().join("shell-link/sh");
    std::fs::create_dir(shell.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink("/bin/sh", &shell).unwrap();
    let profile_text=format!("schema_version=1\nactive='node22'\n[profiles.node22]\npath=[{:?}, {stubs:?}, '/usr/bin', '/bin']\nshell={shell:?}\ncache_namespace='node22'\nrequired_tools=['node']\nallowed_env=['MARKER','GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n[profiles.node22.env]\nMARKER='profile-constant'\n",bin.to_str().unwrap());
    std::fs::write(spec.worker_state_dir.join("profiles.toml"), profile_text).unwrap();
    std::fs::set_permissions(
        spec.worker_state_dir.join("profiles.toml"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    std::fs::write(
        input.join(".remote-task-runner.toml"),
        "schema_version=1\n[[outputs]]\nname='report'\npaths=['report.txt']\nrequired=true\nwhen='always'\n",
    ).unwrap();
    let output=tokio::time::timeout(Duration::from_secs(30),tokio::process::Command::new(&spec.executable).args(["--json","exec","--profile","node22","--requires","node","--","/bin/sh","-c","test \"$(node --version)\" = v22.0.0 && test \"$MARKER\" = profile-constant || exit 99; echo report > report.txt; exit 23"]).env_clear().envs(std::env::var_os("LLVM_PROFILE_FILE").map(|v|("LLVM_PROFILE_FILE",v))).envs(&spec.env).current_dir(&input).output()).await.unwrap_or_else(|elapsed| {
        diagnose_coordinator(&spec);
        diagnose_worker(&spec);
        panic!("named-profile exec did not finish: {elapsed}");
    }).unwrap();
    if output.status.code() != Some(23) {
        diagnose_worker(&spec);
    }
    assert_eq!(
        output.status.code(),
        Some(23),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let initial: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let run =
        remote_task_runner::ids::RunId::parse(initial["result"]["target"]["id"].as_str().unwrap())
            .unwrap();
    let task =
        remote_task_runner::ids::TaskId::parse(initial["result"]["task_id"].as_str().unwrap())
            .unwrap();
    let expected = ExecutionRequirements::parse(Some("node22".into()), &["node".into()]).unwrap();
    std::fs::write(input.join(".remote-task-runner.toml"), "invalid [ config").unwrap();
    let retry = tokio::process::Command::new(&spec.executable)
        .args(["--json", "retry", run.as_str()])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|v| ("LLVM_PROFILE_FILE", v)))
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let (_sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        control::wait(&spec, Target::Run(run), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.cli_exit_code, 23);
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let attempts = owner.attempts_of_task(&task).unwrap();
    assert_eq!(attempts.len(), 2);
    let worker =
        remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir).unwrap();
    for attempt in attempts {
        let accepted = worker.get(&attempt.attempt_id).unwrap().unwrap();
        let payload: remote_task_runner::wire::AttemptPayload =
            serde_json::from_value(accepted.spec.clone()).unwrap();
        assert_eq!(payload.requirements, expected);
        assert_eq!(payload.outputs.len(), 1);
        assert_eq!(payload.outputs[0].name, "report");
        let artifacts = owner.persisted_artifacts(&attempt.attempt_id).unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].0, "report");
        assert!(!accepted.spec["_worker_profile_fingerprint"].is_null());
    }
    shutdown(&load(&spec)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_exec_nested_git_cwd_uses_snapshot_root_and_preserves_in_place() {
    let (root, mut spec) = fixture();
    spec.env.insert(
        "PATH".into(),
        private_tool_path(root.path(), &["git", "fast-worktree"]),
    );
    let repo = root.path().join("project");
    let sub = repo.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(repo.join("parent.txt"), "root input").unwrap();
    std::fs::write(sub.join("input.txt"), "committed nested input").unwrap();
    for args in [
        vec!["init", "-q"],
        vec!["config", "user.name", "Fixture"],
        vec!["config", "user.email", "fixture@example.invalid"],
        vec!["add", "."],
        vec!["commit", "-qm", "nested fixture"],
    ] {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    std::fs::write(sub.join("input.txt"), "dirty nested input").unwrap();
    let config = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        config.join("config.toml"),
        "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\n",
    )
    .unwrap();
    let retained = config.join("coordinator.json");
    remote_task_runner::service::bootstrap::write_input(&retained, &spec).unwrap();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(remote_task_runner::service::bootstrap::daemon_argv(&retained).unwrap())
        .env_clear()
        .envs(&spec.env)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let client = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "private owner exited early"
            );
            if let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) {
                if let Ok(client) = Client::load(&spec.state_dir, store.cluster_id().clone()) {
                    if matches!(
                        client.request(Operation::Ping).await,
                        Ok(Reply::Ready { .. })
                    ) {
                        break client;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let results = async {
        for in_place in [false, true] {
            let marker = if in_place { "nested-in-place" } else { "nested-snapshot" };
            let script = format!("set -eu; test \"$(cat input.txt)\" = 'dirty nested input'; test \"$(cat ../parent.txt)\" = 'root input'; test \"${{PWD##*/}}\" = sub; printf '%s' {marker} > proof.txt; cat proof.txt");
            let mut command = tokio::process::Command::new(&spec.executable);
            command.args(["--json", "exec"]);
            if in_place { command.arg("--in-place"); }
            let output = tokio::time::timeout(Duration::from_secs(40), command.args(["--", "/bin/sh", "-c", &script]).env_clear().envs(&spec.env).current_dir(&sub).output()).await.map_err(|_| "nested exec timed out".to_string())?.map_err(|e|e.to_string())?;
            if !output.status.success() { return Err(format!("nested exec failed: {} {}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr))); }
            let value: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|e|e.to_string())?;
            if value["result"]["snapshot"]["in_place"].as_bool().unwrap_or(false) != in_place { return Err("wrong execution mode".into()); }
            let task = value["result"]["task_id"].as_str().ok_or("missing task id")?;
            let logs = tokio::process::Command::new(&spec.executable).args(["logs", task]).env_clear().envs(&spec.env).current_dir(&sub).output().await.map_err(|e|e.to_string())?;
            if !logs.status.success() || !String::from_utf8_lossy(&logs.stdout).contains(marker) { return Err("nested command did not produce its expected collected log".into()); }
            if in_place {
                if std::fs::read_to_string(sub.join("proof.txt")).map_err(|e|e.to_string())? != marker { return Err("in-place command did not write in the original subdirectory".into()); }
            } else if sub.join("proof.txt").exists() { return Err("snapshot command changed the original subdirectory".into()); }
        }
        Ok::<_, String>(())
    }.await;
    shutdown(&client).await;
    assert!(child.wait().await.unwrap().success());
    results.unwrap();
    assert_eq!(
        std::fs::read_to_string(sub.join("input.txt")).unwrap(),
        "dirty nested input"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("parent.txt")).unwrap(),
        "root input"
    );
}

#[tokio::test]
async fn estimate_offline_100_calls_without_git_leave_db_epoch_and_source_roots_unchanged() {
    let (_root, mut spec, input) = control_fixture();
    let store = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
    let epoch = store.epoch();
    drop(store);
    let db = std::fs::read(spec.state_dir.join("coordinator.db")).unwrap();
    spec.env.insert("PATH".into(), "/nonexistent".into());
    for _ in 0..100 {
        let context = remote_task_runner::service::context::Context::discover_estimate(
            spec.env.clone(),
            input.clone(),
        )
        .unwrap();
        let answer = remote_task_runner::service::estimate::command(
            context,
            vec!["echo".into(), "hello".into()],
        )
        .await
        .unwrap();
        assert!(!answer.online);
        assert!(answer.conditional && answer.decision.is_none() && answer.forecasts.is_empty());
    }
    assert_eq!(
        db,
        std::fs::read(spec.state_dir.join("coordinator.db")).unwrap()
    );
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    assert!(!spec.state_dir.join("parent").exists());
    assert!(!spec.state_dir.join("startup.lock").exists());
    assert!(!spec.state_dir.join("sources").exists());
    assert!(!spec.worker_state_dir.join("traffic").exists());
    assert!(!worker_traffic(&spec.env).state_dir.exists());
    assert!(!spec.worker_state_dir.join("clones").exists());
}

#[tokio::test]
async fn estimate_known_task_without_database_does_not_initialize_state() {
    let (_root, spec, input) = control_fixture();
    let missing = remote_task_runner::ids::TaskId::generate();
    let error = remote_task_runner::service::estimate::task(spec.clone(), missing.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        remote_task_runner::error::ErrorCode::InvalidInput
    );
    let output = tokio::process::Command::new(&spec.executable)
        .args(["estimate", "--task", missing.as_str(), "--json"])
        .env_clear()
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(!spec.state_dir.join("coordinator.db").exists());
    assert!(!spec.state_dir.join("owner.json").exists());
    assert!(!spec.state_dir.join("startup.lock").exists());
    assert!(!spec.state_dir.join("sources").exists());
    assert!(!spec.worker_state_dir.join("traffic").exists());
    assert!(!worker_traffic(&spec.env).state_dir.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn estimate_reads_real_worker_execution_history_through_authenticated_owner() {
    let (root, mut spec, repo) = control_fixture();
    // This history test needs the real workspace tool, but no optional host
    // managers. Keep Inspect independent of unrelated tool startup latency.
    let tool = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|path| path.join("fast-worktree"))
        .find(|path| path.is_file())
        .expect("real fast-worktree must be installed for this fixture");
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    std::os::unix::fs::symlink(
        std::fs::canonicalize(tool).unwrap(),
        bin.join("fast-worktree"),
    )
    .unwrap();
    spec.env
        .insert("PATH".into(), format!("{}:/usr/bin:/bin", bin.display()));
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["add", "."]);
    git(&["commit", "-qm", "fixture"]);
    // The retained fixture input has a deliberately minimal PATH; keep its
    // existing content and start the explicit owned child with the intended env.
    let resident_input = repo.parent().unwrap().join("history-resident.json");
    remote_task_runner::service::bootstrap::write_input(&resident_input, &spec).unwrap();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(remote_task_runner::service::bootstrap::daemon_argv(&resident_input).unwrap())
        .env_clear()
        .envs(&spec.env)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let client = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) {
                if let Ok(client) = Client::load(&spec.state_dir, store.cluster_id().clone()) {
                    if matches!(
                        client.request(Operation::Ping).await,
                        Ok(Reply::Ready { .. })
                    ) {
                        break client;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let mut successful_task = None;
    for _ in 0..2 {
        let output = tokio::time::timeout(
            Duration::from_secs(40),
            tokio::process::Command::new(&spec.executable)
                .args(["--json", "exec", "--", "/bin/sh", "-c", "sleep 1"])
                .env_clear()
                .envs(&spec.env)
                .current_dir(&repo)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        successful_task = Some(
            remote_task_runner::ids::TaskId::parse(result["result"]["task_id"].as_str().unwrap())
                .unwrap(),
        );
    }
    let successful_task = successful_task.unwrap();
    let first_known =
        remote_task_runner::service::estimate::task(spec.clone(), successful_task.clone())
            .await
            .unwrap();
    assert!(first_known.online);
    assert_eq!(first_known.forecasts.len(), 1, "{}", first_known.human());
    assert_ne!(
        first_known.forecasts[0].target.runtime_source,
        remote_task_runner::wire::RuntimeSource::Measured
    );
    assert!(!first_known.components.is_empty());
    // This uses the successful transfer/collection producers, not hand-inserted stats.
    assert!(
        first_known.components.iter().any(|entry| matches!(
            entry.costs.collection.kind,
            remote_task_runner::scheduler::measured::EvidenceKind::Measured
        )),
        "{:?}",
        first_known.components
    );
    std::fs::write(
        repo.join("input.txt"),
        "changed after successful captured execution",
    )
    .unwrap();
    std::fs::write(
        repo.join(".remote-task-runner.toml"),
        "deliberately invalid current project config",
    )
    .unwrap();
    let changed =
        remote_task_runner::service::estimate::task(spec.clone(), successful_task.clone())
            .await
            .unwrap();
    assert_eq!(
        changed.forecasts[0].target.plan_digest,
        first_known.forecasts[0].target.plan_digest
    );
    assert_eq!(
        serde_json::to_value(&changed.components[0].costs.collection.provenance).unwrap(),
        serde_json::to_value(&first_known.components[0].costs.collection.provenance).unwrap()
    );
    assert_eq!(
        changed.components[0].costs.collection.ms,
        first_known.components[0].costs.collection.ms
    );
    // Remove intentionally invalid config before exercising the original raw-command path.
    std::fs::remove_file(repo.join(".remote-task-runner.toml")).unwrap();
    let history = task_traffic_controller::history::read_all(
        &worker_traffic(&spec.env).state_dir.join("history.jsonl"),
    )
    .unwrap();
    assert_eq!(history.len(), 2);
    assert!(history.iter().all(|record| record.wall_secs >= 1.0
        && record.exit_code == 0
        && record.measurement_id.is_some()));
    let context = remote_task_runner::service::context::Context::discover_estimate(
        spec.env.clone(),
        repo.clone(),
    )
    .unwrap();
    let answer = remote_task_runner::service::estimate::command(
        context,
        vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()],
    )
    .await
    .unwrap();
    shutdown(&client).await;
    assert!(child.wait().await.unwrap().success());
    let before_startup_lock = spec.state_dir.join("startup.lock").exists();
    let before_db = std::fs::read(spec.state_dir.join("coordinator.db")).unwrap();
    let before_epoch = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .epoch();
    // Public CLI task mode must work from a changed checkout with no Git in PATH.
    let mut offline_env = spec.env.clone();
    offline_env.insert("PATH".into(), "/nonexistent".into());
    std::fs::write(
        repo.join(".remote-task-runner.toml"),
        "invalid project config",
    )
    .unwrap();
    for _ in 0..100 {
        let offline =
            remote_task_runner::service::estimate::task(spec.clone(), successful_task.clone())
                .await
                .unwrap();
        assert!(!offline.online && offline.decision.is_none());
        assert_eq!(offline.source_kind, "captured");
    }
    let output = tokio::process::Command::new(&spec.executable)
        .args(["estimate", "--task", successful_task.as_str(), "--json"])
        .env_clear()
        .envs(&offline_env)
        .current_dir(&repo)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["online"], false);
    assert_eq!(value["result"]["source_kind"], "captured");
    let unknown = remote_task_runner::service::estimate::task(
        spec.clone(),
        remote_task_runner::ids::TaskId::generate(),
    )
    .await
    .unwrap_err();
    assert_eq!(unknown.code, remote_task_runner::error::ErrorCode::NotFound);
    let invalid = tokio::process::Command::new(&spec.executable)
        .args(["estimate", "--task", "not-a-task", "--json"])
        .env_clear()
        .envs(&offline_env)
        .current_dir(&repo)
        .output()
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&invalid.stdout).unwrap();
    assert_eq!(value["error"]["code"], "INVALID_INPUT");
    assert_eq!(
        before_db,
        std::fs::read(spec.state_dir.join("coordinator.db")).unwrap()
    );
    assert_eq!(
        before_epoch,
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch()
    );
    assert_eq!(
        before_startup_lock,
        spec.state_dir.join("startup.lock").exists()
    );
    assert!(answer.online, "{}", answer.human());
    assert_eq!(answer.forecasts.len(), 1, "{}", answer.human());
    let forecast = &answer.forecasts[0];
    assert_eq!(
        forecast.target.runtime_source,
        remote_task_runner::wire::RuntimeSource::Measured,
        "{}",
        answer.human()
    );
    assert_eq!(forecast.target.history_count, 2);
    assert!(forecast.target.execution_ms.is_some());
    assert!(forecast.target.conditional);
}

// Publish a valid same-plan, same-version replacement in this fixture's own
// private root. Only the node script comment and its exact tree identity change;
// both the receipt validator and registry detection must accept the replacement.
fn replace_private_node_receipt(
    state: &std::path::Path,
    profile: &remote_task_runner::toolchain::profile::Profile,
    bundle: &remote_task_runner::toolchain::contract::FrozenBundle,
    node: &std::path::Path,
) {
    use remote_task_runner::toolchain::{
        adapters::{Availability, FileIdentity, InstallationReceipt},
        registry::Registry,
        versions::ToolId,
    };
    let registry = Registry::load(state, profile, profile.report().profile_fingerprint).unwrap();
    let tool = ToolId::parse("node").unwrap();
    let evidence = bundle.evidence.iter().find(|e| e.tool == tool).unwrap();
    let request = registry
        .request(&bundle.frozen, &tool, Some(evidence))
        .unwrap();
    let plan = remote_task_runner::toolchain::adapters::plan(
        request.kind,
        &request.frozen,
        &request.tool,
        &request.evidence,
        &request.private_root,
        &request.host,
    )
    .unwrap();
    let path = plan.root().join("receipt.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let previous: InstallationReceipt = serde_json::from_value(value.clone()).unwrap();
    previous.validate(&plan).unwrap();
    let mut bytes = std::fs::read(node).unwrap();
    bytes.extend_from_slice(
        format!("\n# valid replacement after {}\n", previous.digest()).as_bytes(),
    );
    std::fs::write(node, bytes).unwrap();
    let identity = FileIdentity::capture(node).unwrap();
    let entry = value["tree"]["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|file| file["path"].as_str() == node.to_str())
        .unwrap();
    *entry = serde_json::to_value(identity).unwrap();
    let replacement: InstallationReceipt = serde_json::from_value(value).unwrap();
    replacement.validate(&plan).unwrap();
    assert_eq!(replacement.version(), previous.version());
    assert_ne!(replacement.digest(), previous.digest());
    std::fs::write(path, serde_json::to_vec(&replacement).unwrap()).unwrap();
    let Availability::Installed(detected) = registry.detect(&request).unwrap() else {
        panic!("valid replacement must remain installed");
    };
    assert_eq!(detected.digest(), replacement.digest());
    let version = Command::new(node)
        .arg("--version")
        .env_clear()
        .output()
        .unwrap();
    assert!(version.status.success());
    assert_eq!(version.stdout, b"v20.0.0\n");
}

/// A42, the not-installable half: a pinned version the operator never
/// authorized leaves no compatible worker (`blocked: no_compatible_worker`,
/// `capability_mismatch`) before the command or any manager runs, and the base
/// profile stays as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_toolchain_public_exec_blocks_what_cannot_be_installed_before_running() {
    use remote_task_runner::toolchain::profile::Profile;
    let (root, spec, input) = control_fixture();
    let host = root.path().join("private-managers");
    let stubs = root.path().join("tool-stubs");
    std::fs::create_dir(&host).unwrap();
    crate::common::write_executable(&host.join("node"), "#!/bin/sh\nprintf 'v22.0.0\\n'\n");
    let profile_text = format!("schema_version=1\nactive='base22'\n[profiles.base22]\npath=[{:?},{stubs:?},'/usr/bin','/bin']\nshell='/bin/sh'\ncache_namespace='base22'\nrequired_tools=['node']\nallowed_env=['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n", host.to_str().unwrap());
    let profile_file = spec.worker_state_dir.join("profiles.toml");
    std::fs::write(&profile_file, &profile_text).unwrap();
    std::fs::set_permissions(&profile_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let profile = Profile::load(&spec.worker_state_dir, &spec.env).unwrap();
    let manager = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{}/calls'\n",
        host.display()
    );
    crate::common::write_executable(&host.join("manager"), &manager);
    crate::common::write_executable(&host.join("node-build"), &manager);
    let registry = format!("schema_version=1\nactive_profile='base22'\nprofile_definition={:?}\n[[recipes]]\ntool='node'\nkind='Nodenv'\nmanager={:?}\ndependencies=[{:?}]\nsearch_path=[{:?}{}]\ninventory=['20.0.0']\n", profile.definition_digest().as_str(), host.join("manager").to_str().unwrap(),host.join("node-build").to_str().unwrap(),host.to_str().unwrap(), crate::common::system_search_path_toml());
    let registry_file = spec.worker_state_dir.join("toolchains.toml");
    std::fs::write(&registry_file, registry).unwrap();
    std::fs::set_permissions(&registry_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let escaped = host.join("consumer-ran");
    let script = format!("echo unsafe > '{}'", escaped.display());
    // The pinned Node version is not in the operator inventory.
    std::fs::write(input.join(".node-version"), "21.0.0\n").unwrap();
    let context = remote_task_runner::service::context::Context::load(
        spec.env.clone(),
        input.clone(),
        spec.clone(),
    )
    .unwrap();
    let accepted = remote_task_runner::service::submit::command(
        context,
        vec!["/bin/sh".into(), "-c".into(), script],
        None,
    )
    .await
    .unwrap();
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let mut waiting = None;
    for _ in 0..600 {
        waiting =
            owner.events_after(0, 512).unwrap().into_iter().find(|e| {
                e.kind == "task_waiting" && e.payload["task"] == accepted.task_id.as_str()
            });
        if waiting.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let payload = waiting
        .expect("the task must be reported as blocked")
        .payload;
    assert_eq!(payload["blocked_reason"], "no_compatible_worker");
    assert_eq!(payload["decision"]["winner"], serde_json::Value::Null);
    assert_eq!(
        payload["decision"]["excluded"][0]["why"], "capability_mismatch",
        "{payload}"
    );
    assert!(!escaped.exists(), "the command ran anyway");
    assert!(!host.join("calls").exists(), "a manager ran");
    assert_eq!(std::fs::read_to_string(profile_file).unwrap(), profile_text);
    shutdown(&load(&spec)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_toolchain_public_exec_installs_once_activates_setup_isolates_and_retries() {
    use remote_task_runner::service::{completion::Target, context::Context, control};
    use remote_task_runner::toolchain::profile::Profile;
    let (root, spec, input) = control_fixture();
    let host = root.path().join("private-managers");
    // The host tools this test does not use are answered by the fixture.
    let stubs = root.path().join("tool-stubs");
    std::fs::create_dir(&host).unwrap();
    crate::common::write_executable(&host.join("node"), "#!/bin/sh\nprintf 'v22.0.0\\n'\n");
    // Both commands are alive at once: the second waits for the first, which
    // has to be admitted meanwhile. The default admission budget is the
    // host's core count and the default weight is 2, so on a 3-core host the
    // first stays ready until the second gave up; a busy host holds a start
    // as well. This test is about toolchains, so its admission is fixed here
    // instead of taken from the host.
    let traffic_config = worker_traffic(&spec.env).config_file;
    let traffic = traffic_config.parent().unwrap();
    std::fs::create_dir_all(traffic).unwrap();
    std::fs::set_permissions(traffic, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        &traffic_config,
        "budget = 8\ncpu_threshold_percent = 100.0\nmemory_reserve_mib = 0\n",
    )
    .unwrap();
    let profile_text = format!("schema_version=1\nactive='base22'\n[profiles.base22]\npath=[{:?},{stubs:?},'/usr/bin','/bin']\nshell='/bin/sh'\ncache_namespace='base22'\nrequired_tools=['node']\nallowed_env=['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n", host.to_str().unwrap());
    let profile_file = spec.worker_state_dir.join("profiles.toml");
    std::fs::write(&profile_file, &profile_text).unwrap();
    std::fs::set_permissions(&profile_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let profile = Profile::load(&spec.worker_state_dir, &spec.env).unwrap();
    let manager = format!(
        r#"#!/bin/sh
set -eu
printf '%s\n' "$1" >> '{host}/calls'
version="$1"
bin="$NODENV_ROOT/versions/$version/bin"
/bin/mkdir -p "$bin"
/usr/bin/printf '#!/bin/sh\necho v%s\n' "$version" > "$bin/node"
/bin/chmod 700 "$bin/node"
"#,
        host = host.display()
    );
    crate::common::write_executable(&host.join("manager"), &manager);
    crate::common::write_executable(&host.join("node-build"), &manager);
    let registry = format!("schema_version=1\nactive_profile='base22'\nprofile_definition={:?}\n[[recipes]]\ntool='node'\nkind='Nodenv'\nmanager={:?}\ndependencies=[{:?}]\nsearch_path=[{:?}{}]\ninventory=['20.0.0','22.0.0']\n", profile.definition_digest().as_str(), host.join("manager").to_str().unwrap(),host.join("node-build").to_str().unwrap(),host.to_str().unwrap(), crate::common::system_search_path_toml());
    let registry_file = spec.worker_state_dir.join("toolchains.toml");
    std::fs::write(&registry_file, registry).unwrap();
    std::fs::set_permissions(&registry_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(input.join(".node-version"), "20.0.0\n").unwrap();
    // The public submit capture path, with the existing project builder enabled.
    let mut context = Context::load(spec.env.clone(), input.clone(), spec.clone()).unwrap();
    context.config.prepare_commands = vec![vec![
        "/bin/sh".into(),
        "-c".into(),
        "node --version > prepared-node.txt".into(),
    ]];
    let ready = host.join("node20-ready");
    let release = host.join("node20-release");
    let first_script = format!("test \"$(node --version)\" = v20.0.0 && test \"$(cat prepared-node.txt)\" = v20.0.0 || exit 91; : > '{}'; i=0; while [ ! -f '{}' ] && [ $i -lt 400 ]; do /bin/sleep 0.05; i=$((i+1)); done; test -f '{}' || exit 92; test \"$(node --version)\" = v20.0.0 || exit 93; exit 23", ready.display(), release.display(), release.display());
    let first = remote_task_runner::service::submit::command(
        context,
        vec!["/bin/sh".into(), "-c".into(), first_script],
        None,
    )
    .await
    .unwrap();
    // A different snapshot requests Node22 without changing operator defaults.
    std::fs::write(input.join(".node-version"), "22.0.0\n").unwrap();
    let second_script = format!("test \"$(node --version)\" = v22.0.0 || exit 94; i=0; while [ ! -f '{}' ] && [ $i -lt 400 ]; do /bin/sleep 0.05; i=$((i+1)); done; test -f '{}' || exit 95; : > '{}'", ready.display(), ready.display(), release.display());
    let output = tokio::process::Command::new(&spec.executable)
        .args(["--json", "exec", "--", "/bin/sh", "-c", &second_script])
        .env_clear()
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    if !output.status.success() {
        diagnose_coordinator(&spec);
        diagnose_worker(&spec);
    }
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let done = tokio::time::timeout(
        Duration::from_secs(45),
        control::wait(&spec, Target::Run(first.run_id.clone()), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    if done.cli_exit_code != 23 {
        eprintln!("public activation completion: {done:?}");
        let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
        let worker =
            remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                .unwrap();
        for attempt in owner.attempts_of_task(&first.task_id).unwrap() {
            eprintln!(
                "private preparation diagnostics: {:?}",
                worker
                    .events_of(&attempt.attempt_id, "agent_prepare_failed")
                    .unwrap()
            );
        }
        shutdown(&load(&spec)).await;
        panic!("activated setup and command must both see Node20");
    }
    // Retry keeps the original capture and exact evidence even after changing the checkout.
    control::retry(&spec, Target::Run(first.run_id.clone()))
        .await
        .unwrap();
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let done = tokio::time::timeout(
        Duration::from_secs(45),
        control::wait(&spec, Target::Run(first.run_id.clone()), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(done.cli_exit_code, 23);
    std::fs::write(input.join(".node-version"), "20.0.0\n").unwrap();
    let public20 = tokio::process::Command::new(&spec.executable)
        .args([
            "--json",
            "exec",
            "--",
            "/bin/sh",
            "-c",
            "test \"$(node --version)\" = v20.0.0",
        ])
        .env_clear()
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    if !public20.status.success() {
        shutdown(&load(&spec)).await;
        panic!(
            "public Node20 exec failed: {}",
            String::from_utf8_lossy(&public20.stderr)
        );
    }
    let calls = std::fs::read_to_string(host.join("calls")).unwrap();
    assert_eq!(calls.lines().filter(|v| *v == "20.0.0").count(), 1);
    assert_eq!(calls.lines().filter(|v| *v == "22.0.0").count(), 1);
    assert_eq!(std::fs::read_to_string(profile_file).unwrap(), profile_text);
    let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let attempts = owner.attempts_of_task(&first.task_id).unwrap();
    assert_eq!(attempts.len(), 2);
    let worker =
        remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir).unwrap();
    let mut bundles = Vec::new();
    let mut node20 = None;
    for attempt in attempts {
        let accepted = worker.get(&attempt.attempt_id).unwrap().unwrap();
        let payload: remote_task_runner::wire::AttemptPayload =
            serde_json::from_value(accepted.spec).unwrap();
        bundles.push(payload.toolchains.unwrap());
        let binding: remote_task_runner::toolchain::contract::ExecutionBinding =
            serde_json::from_slice(
                &std::fs::read(
                    spec.worker_state_dir
                        .join("attempts")
                        .join(attempt.attempt_id.as_str())
                        .join("execution-binding.json"),
                )
                .unwrap(),
            )
            .unwrap();
        node20 = std::env::split_paths(&binding.activation.environment["PATH"])
            .next()
            .map(|bin| bin.join("node"));
        assert_eq!(binding.activation.environment["HOME"], spec.env["HOME"]);
        assert_ne!(binding.fingerprint(), &profile.report().profile_fingerprint);
    }
    assert_eq!(bundles[0], bundles[1]);
    let node20 = node20.unwrap();
    // The binding is already accepted while project setup waits. A fully valid
    // replacement receipt must reject both a later launch and preparation recovery.
    for recover in [false, true] {
        let ready = host.join(if recover {
            "recover-setup-ready"
        } else {
            "waiting-setup-ready"
        });
        let release = host.join(if recover {
            "recover-setup-release"
        } else {
            "waiting-setup-release"
        });
        let escaped = host.join(if recover {
            "recover-consumer-ran"
        } else {
            "waiting-consumer-ran"
        });
        let setup_script = format!(": > '{}'; i=0; while [ ! -f '{}' ] && [ $i -lt 800 ]; do /bin/sleep 0.05; i=$((i+1)); done; test -f '{}'", ready.display(), release.display(), release.display());
        let mut context = Context::load(spec.env.clone(), input.clone(), spec.clone()).unwrap();
        context.config.prepare_commands = vec![vec!["/bin/sh".into(), "-c".into(), setup_script]];
        let consumer_script = format!("echo unsafe > '{}'", escaped.display());
        let waiting = remote_task_runner::service::submit::command(
            context,
            vec!["/bin/sh".into(), "-c".into(), consumer_script],
            None,
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let accepted = owner
            .attempts_of_task(&waiting.task_id)
            .unwrap()
            .pop()
            .unwrap();
        let binding_path = spec
            .worker_state_dir
            .join("attempts")
            .join(accepted.attempt_id.as_str())
            .join("execution-binding.json");
        let original_binding = std::fs::read(&binding_path).unwrap();
        assert!(worker.launches(&accepted.attempt_id).unwrap().is_empty());
        let payload: remote_task_runner::wire::AttemptPayload =
            serde_json::from_value(worker.get(&accepted.attempt_id).unwrap().unwrap().spec)
                .unwrap();
        if recover {
            shutdown(&load(&spec)).await;
        }
        replace_private_node_receipt(
            &spec.worker_state_dir,
            &profile,
            payload.toolchains.as_deref().unwrap(),
            &node20,
        );
        if recover {
            ensure(spec.clone()).await.unwrap();
        }
        std::fs::write(&release, "release").unwrap();
        let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
        let refused = tokio::time::timeout(
            Duration::from_secs(30),
            control::wait(&spec, Target::Run(waiting.run_id), &mut signals),
        )
        .await
        .unwrap()
        .unwrap();
        if refused.cli_exit_code == 0 || escaped.exists() {
            shutdown(&load(&spec)).await;
            panic!("a waiting/recovered consumer used a replaced installation receipt");
        }
        assert!(worker.launches(&accepted.attempt_id).unwrap().is_empty());
        assert_eq!(std::fs::read(binding_path).unwrap(), original_binding);
        assert_eq!(std::fs::read_to_string(host.join("calls")).unwrap(), calls);
    }
    // A running supervisor keeps its accepted immutable execution binding even
    // when a later owner observes drift. It must be adopted, never relaunched.
    let adoption_ready = host.join("adoption-ready");
    let adoption_release = host.join("adoption-release");
    let adoption_script = format!("test \"$(node --version)\" = v20.0.0 || exit 96; : > '{}'; i=0; while [ ! -f '{}' ] && [ $i -lt 800 ]; do /bin/sleep 0.05; i=$((i+1)); done; test -f '{}' || exit 97; exit 23", adoption_ready.display(), adoption_release.display(), adoption_release.display());
    let context = Context::load(spec.env.clone(), input.clone(), spec.clone()).unwrap();
    let adopting = remote_task_runner::service::submit::command(
        context,
        vec!["/bin/sh".into(), "-c".into(), adoption_script],
        None,
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !adoption_ready.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let active = owner
        .attempts_of_task(&adopting.task_id)
        .unwrap()
        .pop()
        .unwrap();
    let binding_path = spec
        .worker_state_dir
        .join("attempts")
        .join(active.attempt_id.as_str())
        .join("execution-binding.json");
    let accepted_binding = std::fs::read(&binding_path).unwrap();
    assert_eq!(worker.launches(&active.attempt_id).unwrap().len(), 1);
    let active_payload: remote_task_runner::wire::AttemptPayload =
        serde_json::from_value(worker.get(&active.attempt_id).unwrap().unwrap().spec).unwrap();
    shutdown(&load(&spec)).await;
    replace_private_node_receipt(
        &spec.worker_state_dir,
        &profile,
        active_payload.toolchains.as_deref().unwrap(),
        &node20,
    );
    ensure(spec.clone()).await.unwrap();
    assert_eq!(std::fs::read(&binding_path).unwrap(), accepted_binding);
    assert_eq!(worker.launches(&active.attempt_id).unwrap().len(), 1);
    std::fs::write(&adoption_release, "release").unwrap();
    let (_tx, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let adopted = tokio::time::timeout(
        Duration::from_secs(30),
        control::wait(&spec, Target::Run(adopting.run_id), &mut signals),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(adopted.cli_exit_code, 23);
    assert_eq!(worker.launches(&active.attempt_id).unwrap().len(), 1);
    // Invalid bytes still block a new task without repair/install, independently
    // of the valid replacement receipt checks above.
    crate::common::write_executable(&node20, "#!/bin/sh\necho v99.0.0\n");
    let escaped = host.join("drift-consumer-ran");
    let drift_script = format!("echo unsafe > '{}'", escaped.display());
    let drift = tokio::process::Command::new(&spec.executable)
        .args(["--json", "exec", "--", "/bin/sh", "-c", &drift_script])
        .env_clear()
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    assert!(!drift.status.success());
    assert!(!escaped.exists());
    assert_eq!(std::fs::read_to_string(host.join("calls")).unwrap(), calls);
    let native = Command::new(host.join("node"))
        .arg("--version")
        .env_clear()
        .output()
        .unwrap();
    assert_eq!(native.stdout, b"v22.0.0\n");
    shutdown(&load(&spec)).await;
}

#[test]
fn ci_plan_public_cli_frozen_worktree_commit_directory_and_private_failures() {
    use remote_task_runner::service::{context::Context, planning};
    let (root, spec) = fixture();
    let config = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    remote_task_runner::service::bootstrap::write_input(&config.join("coordinator.json"), &spec)
        .unwrap();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(repo.join(".github/workflows")).unwrap();
    let workflow = repo.join(".github/workflows/ci.yml");
    let old = "on: push\njobs:\n  old_job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo PRIVATE_SCRIPT_SENTINEL\n";
    let dirty = old.replace("old_job", "dirty_job");
    std::fs::write(&workflow, old).unwrap();
    std::fs::write(repo.join(".gitignore"), "private.env\n").unwrap();
    std::fs::write(
        repo.join(".remote-task-runner.toml"),
        "schema_version=1\n[source]\nsecret_files=[{glob='private.env',required=true}]\n",
    )
    .unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    let commit = git(&["rev-parse", "HEAD"]).trim().to_string();
    std::fs::write(&workflow, old.replace("old_job", "head_job")).unwrap();
    git(&["commit", "-qam", "second"]);
    std::fs::write(&workflow, &dirty).unwrap();
    std::fs::write(repo.join("private.env"), "TOKEN=PRIVATE_SECRET_SENTINEL").unwrap();
    let epoch = {
        let store = CoordinatorStore::open(&spec.state_dir, 1).unwrap();
        store.epoch()
    };
    let initial_events: i64 = rusqlite::Connection::open(spec.state_dir.join("coordinator.db"))
        .unwrap()
        .query_row("SELECT count(*) FROM event", [], |row| row.get(0))
        .unwrap();
    let call = |dir: &std::path::Path, args: &[&str]| {
        let output = Command::new(&spec.executable)
            .args(args)
            .current_dir(dir)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        for hidden in [
            "PRIVATE_SCRIPT_SENTINEL",
            "PRIVATE_SECRET_SENTINEL",
            root.path().to_str().unwrap(),
        ] {
            assert!(!text.contains(hidden), "{text}");
        }
        serde_json::from_str::<serde_json::Value>(&text).unwrap()
    };
    let value = call(&repo, &["--json", "ci", "plan"]);
    assert_eq!(value["ok"], true, "{value}");
    assert_eq!(value["result"]["nodes"][0]["job"], "dirty_job");
    let value = call(
        &repo,
        &[
            "--json",
            "ci",
            "--snapshot",
            "commit",
            "--ref",
            &commit,
            "plan",
        ],
    );
    assert_eq!(value["ok"], true, "{value}");
    assert_eq!(value["result"]["nodes"][0]["job"], "old_job");
    assert_eq!(std::fs::read_to_string(&workflow).unwrap(), dirty);
    for args in [
        vec!["--json", "ci", "--snapshot", "commit", "plan"],
        vec!["--json", "ci", "--snapshot", "unknown", "plan"],
        vec![
            "--json",
            "ci",
            "--snapshot",
            "worktree",
            "--ref",
            "HEAD",
            "plan",
        ],
    ] {
        assert_eq!(call(&repo, &args)["ok"], false);
    }
    let context = Context::load(spec.env.clone(), repo.clone(), spec.clone()).unwrap();
    let prepared = planning::prepare(context, None).unwrap();
    let digest = prepared.plan().frozen().digest.clone();
    assert_eq!(
        prepared
            .source_store()
            .source_of_run(prepared.run_id())
            .unwrap()
            .as_deref(),
        Some(prepared.manifest().source_id.as_str())
    );
    let bindings = std::fs::read_dir(prepared.source_store().root().join("runs"))
        .unwrap()
        .filter(|e| {
            !e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".lock")
        })
        .count();
    assert_eq!(bindings, 1, "prepare must bind exactly one capture");
    let pinned = prepared.plan().frozen().source.pinned.as_ref().unwrap();
    assert_eq!(pinned.manifest_digest, prepared.manifest().manifest_digest);
    assert_eq!(
        prepared.plan().frozen().source.source_id,
        prepared.manifest().source_id
    );

    assert_eq!(prepared.manifest().secret_files.len(), 1);
    assert!(!prepared
        .manifest()
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.0 == b"private.env"));
    let draft = prepared.source_store().root().parent().unwrap().to_owned();
    std::fs::write(&workflow, old.replace("old_job", "after_prepare")).unwrap();
    assert_eq!(prepared.plan().frozen().digest, digest);
    assert_eq!(prepared.plan().summary().nodes[0].job, "dirty_job");
    prepared.close().unwrap();
    assert!(!draft.exists());
    let directory = root.path().join("directory");
    std::fs::create_dir_all(directory.join(".github/workflows")).unwrap();
    std::fs::write(directory.join(".github/workflows/ci.yml"), old).unwrap();
    assert_eq!(call(&directory, &["--json", "ci", "plan"])["ok"], true);
    std::fs::write(
        directory.join(".github/workflows/ci.yml"),
        "on:\n  push:\n    paths: ['PRIVATE_FILTER_SENTINEL']\njobs: {}\n",
    )
    .unwrap();
    let failed = call(&directory, &["--json", "ci", "plan"]);
    assert_eq!(failed["ok"], false);
    assert!(!failed.to_string().contains("PRIVATE_FILTER_SENTINEL"));
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        epoch
    );
    let database = rusqlite::Connection::open(spec.state_dir.join("coordinator.db")).unwrap();
    for table in ["run", "task", "attempt", "event"] {
        let count: i64 = database
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            count,
            if table == "event" { initial_events } else { 0 },
            "planning changed {table}"
        );
    }
    let names: Vec<_> = std::fs::read_dir(&spec.state_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !names
            .iter()
            .any(|n| n.starts_with("ci-plan-") || n.ends_with(".sock")),
        "{names:?}"
    );
}

// A28: jobs that write the same file stay one task on one host, and the plan
// says why in both output forms without printing scripts or host paths.
#[test]
fn ci_plan_shows_why_a_shared_workspace_task_is_not_split() {
    let (root, spec) = fixture();
    let config = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    remote_task_runner::service::bootstrap::write_input(&config.join("coordinator.json"), &spec)
        .unwrap();
    let project = root.path().join("project");
    std::fs::create_dir_all(project.join(".github/workflows")).unwrap();
    std::fs::write(
        project.join(".github/workflows/ci.yml"),
        "on: push\njobs:\n  first:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo PRIVATE_SCRIPT_SENTINEL > shared.txt\n  second:\n    needs: first\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo PRIVATE_SCRIPT_SENTINEL > shared.txt\n  alone:\n    needs: second\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo PRIVATE_SCRIPT_SENTINEL\n",
    )
    .unwrap();
    let call = |args: &[&str]| {
        let output = Command::new(&spec.executable)
            .args(args)
            .current_dir(&project)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        for hidden in ["PRIVATE_SCRIPT_SENTINEL", root.path().to_str().unwrap()] {
            assert!(!text.contains(hidden), "{text}");
        }
        text
    };
    let value: serde_json::Value = serde_json::from_str(&call(&["--json", "ci", "plan"])).unwrap();
    let result = &value["result"];
    let tasks = result["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 2, "{value}");
    let shared = &tasks[0];
    assert_eq!(shared["jobs"], serde_json::json!(["first", "second"]));
    assert_eq!(shared["grouping"], "shared_outputs");
    assert_eq!(shared["shared_by_jobs"], true);
    assert_eq!(tasks[1]["jobs"], serde_json::json!(["alone"]));
    assert_eq!(tasks[1]["grouping"], serde_json::Value::Null);
    assert_eq!(tasks[1]["depends_on"], serde_json::json!([shared["id"]]));
    assert_eq!(result["dependency_layer_width"], 1);
    let group = result["shard_groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["shards"] == serde_json::json!([shared["id"]]))
        .unwrap();
    assert_eq!(group["split"], false);
    assert!(group["not_split"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("shared_workspace")));

    let text = call(&["ci", "plan"]);
    let (shared, alone) = (
        shared["id"].as_str().unwrap(),
        tasks[1]["id"].as_str().unwrap(),
    );
    for line in [
        "1 workflows, 2 tasks, 3 nodes".to_string(),
        "2 tasks can be placed independently, widest dependency layer 1".to_string(),
        format!("task {shared} keeps 2 jobs on one host: shared_outputs"),
        format!("task {alone} waits for {shared}"),
    ] {
        assert!(text.lines().any(|l| l == line), "{line}\n{text}");
    }
    assert!(
        text.lines().any(|l| l
            .strip_prefix(&format!(
                "task {} is not split: [",
                group["id"].as_str().unwrap()
            ))
            .is_some_and(|rest| rest.contains("shared_workspace"))),
        "{text}"
    );
}

#[test]
fn ci_plan_captured_rtr_workflow_directories_never_use_live_config() {
    use remote_task_runner::{
        config::{model::SourceMode, Origin, Overlay},
        service::{context::Context, planning},
    };
    let (root, spec) = fixture();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(repo.join("old-ci")).unwrap();
    let yaml = "on: push\njobs:\n  old_job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo safe\n";
    std::fs::write(repo.join("old-ci/ci.yml"), yaml).unwrap();
    let config = repo.join(".remote-task-runner.toml");
    std::fs::write(
        &config,
        "schema_version=1\n[ci]\nworkflows_dirs=['old-ci']\n",
    )
    .unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["add", "."]);
    git(&["commit", "-qm", "old"]);
    let old = git(&["rev-parse", "HEAD"]).trim().to_owned();
    std::fs::create_dir(repo.join("live-ci")).unwrap();
    std::fs::write(
        repo.join("live-ci/ci.yml"),
        yaml.replace("old_job", "live_job"),
    )
    .unwrap();
    std::fs::write(
        &config,
        "schema_version=1\n[ci]\nworkflows_dirs=['live-ci']\n",
    )
    .unwrap();
    let context = Context::load(spec.env.clone(), repo.clone(), spec.clone()).unwrap();
    let prepared = planning::prepare(context, None).unwrap();
    assert_eq!(prepared.plan().summary().nodes[0].job, "live_job");
    assert_eq!(
        prepared.plan().frozen().config.workflow_dirs,
        vec!["live-ci"]
    );
    std::fs::write(
        &config,
        "schema_version=1\n[ci]\nworkflows_dirs=['missing-private-directory']\n",
    )
    .unwrap();
    assert_eq!(prepared.plan().summary().nodes[0].job, "live_job");
    prepared.close().unwrap();
    let context = Context::load_with_overlay(
        spec.env.clone(),
        repo.clone(),
        spec.clone(),
        &Overlay {
            source_mode: Some(SourceMode::Commit),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(context.config.source_mode.origin, Origin::Cli);
    let prepared = planning::prepare(context, Some(&old)).unwrap();
    assert_eq!(prepared.plan().summary().nodes[0].job, "old_job");
    assert_eq!(
        prepared.plan().frozen().config.workflow_dirs,
        vec!["old-ci"]
    );
    prepared.close().unwrap();
    let directory = root.path().join("plain");
    std::fs::create_dir_all(directory.join("custom-ci")).unwrap();
    std::fs::write(
        directory.join("custom-ci/ci.yml"),
        yaml.replace("old_job", "directory_job"),
    )
    .unwrap();
    std::fs::write(
        directory.join(".remote-task-runner.toml"),
        "schema_version=1\n[ci]\nworkflows_dirs=['custom-ci']\n",
    )
    .unwrap();
    let context = Context::load(spec.env.clone(), directory.clone(), spec.clone()).unwrap();
    let prepared = planning::prepare(context, None).unwrap();
    assert_eq!(prepared.plan().summary().nodes[0].job, "directory_job");
    assert_eq!(prepared.plan().frozen().source.execution_commit, None);
    assert_eq!(prepared.plan().frozen().source.base, None);
    assert_eq!(
        prepared
            .plan()
            .frozen()
            .source
            .pinned
            .as_ref()
            .unwrap()
            .kind,
        local_ci::source::SourceKind::Directory
    );

    assert_eq!(
        prepared.plan().frozen().config.workflow_dirs,
        vec!["custom-ci"]
    );
    let draft = prepared.source_store().root().parent().unwrap().to_owned();
    drop(prepared);
    assert!(!draft.exists());
    let context = Context::load(spec.env.clone(), directory.clone(), spec).unwrap();
    std::fs::write(
        directory.join(".local-ci.toml"),
        "workflows-dirs=['other-ci']\n",
    )
    .unwrap();
    let error = planning::prepare(context, None).err().unwrap();
    assert_eq!(error.message, "captured planning configuration is invalid");
}

#[test]
fn ci_plan_submodules_and_distinct_capture_bindings_keep_exact_identity() {
    use remote_task_runner::service::{context::Context, planning};
    let (root, spec) = fixture();
    let git = |dir: &std::path::Path, args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(output.status.success(), "fixture git failed");
    };
    let module = root.path().join("module");
    let project = root.path().join("project");
    for dir in [&module, &project] {
        std::fs::create_dir_all(dir).unwrap();
        git(dir, &["init", "-q"]);
        git(dir, &["config", "user.name", "Fixture"]);
        git(dir, &["config", "user.email", "fixture@example.invalid"]);
        std::fs::write(dir.join("tracked"), "base").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-qm", "initial"]);
    }
    git(
        &project,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            module.to_str().unwrap(),
            "module",
        ],
    );
    std::fs::create_dir_all(project.join(".github/workflows")).unwrap();
    let workflow = project.join(".github/workflows/ci.yml");
    std::fs::write(&workflow, "on: push\njobs:\n  first:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo private\n").unwrap();
    git(&project, &["add", "."]);
    git(&project, &["commit", "-qm", "workflow and submodule"]);
    std::fs::write(project.join("module/tracked"), "dirty module").unwrap();
    let context = Context::load(spec.env.clone(), project.clone(), spec.clone()).unwrap();
    let first = planning::prepare(context, None).unwrap();
    assert_eq!(first.manifest().submodules.len(), 1);
    assert!(first.manifest().submodules[0].dirty);
    let nested = first.manifest().submodules[0].source_id.as_ref().unwrap();
    assert!(first.source_store().manifest(nested).unwrap().is_some());
    std::fs::write(&workflow, "on: push\njobs:\n  second:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo private\n").unwrap();
    let second = planning::prepare(
        Context::load(spec.env.clone(), project, spec.clone()).unwrap(),
        None,
    )
    .unwrap();
    assert_ne!(first.manifest().source_id, second.manifest().source_id);
    for prepared in [&first, &second] {
        assert_eq!(
            prepared
                .source_store()
                .source_of_run(prepared.run_id())
                .unwrap()
                .as_ref(),
            Some(&prepared.manifest().source_id)
        );
        assert_eq!(
            prepared
                .plan()
                .frozen()
                .source
                .pinned
                .as_ref()
                .unwrap()
                .manifest_digest,
            prepared.manifest().manifest_digest
        );
    }
    assert_eq!(first.plan().summary().nodes[0].job, "first");
    assert_eq!(second.plan().summary().nodes[0].job, "second");
    first.close().unwrap();
    second.close().unwrap();
    assert_eq!(
        std::fs::read_to_string(module.join("tracked")).unwrap(),
        "base"
    );
    assert!(!spec.state_dir.join("coordinator.db").exists());
}

// ---------------------------------------------------------------------------
// `workers sync`: the warm-up through the running resident (A44, sync half)
// ---------------------------------------------------------------------------

/// A local worker whose registry may install node 20.0.0 and 22.0.0 with a
/// fixture manager. The manager writes each version it is asked for to
/// `calls`, fails while `fail` exists and waits while `hold` exists. Nothing
/// leaves the fixture's directory.
fn sync_fixture() -> (super::common::DaemonRoot, Spec, std::path::PathBuf) {
    use remote_task_runner::toolchain::profile::Profile;
    let (root, spec, _input) = control_fixture();
    let host = root.path().join("private-managers");
    let stubs = root.path().join("tool-stubs");
    std::fs::create_dir(&host).unwrap();
    crate::common::write_executable(&host.join("node"), "#!/bin/sh\nprintf 'v18.0.0\\n'\n");
    // The worker's scope is the command's (HOME / XDG), not `<state>/traffic`.
    let traffic_config = worker_traffic(&spec.env).config_file;
    let traffic = traffic_config.parent().unwrap();
    std::fs::create_dir_all(traffic).unwrap();
    std::fs::set_permissions(traffic, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        &traffic_config,
        "budget = 8\ncpu_threshold_percent = 100.0\nmemory_reserve_mib = 0\n",
    )
    .unwrap();
    let profile_text = format!("schema_version=1\nactive='base18'\n[profiles.base18]\npath=[{:?},{stubs:?},'/usr/bin','/bin']\nshell='/bin/sh'\ncache_namespace='base18'\nrequired_tools=['node']\nallowed_env=['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n", host.to_str().unwrap());
    let profile_file = spec.worker_state_dir.join("profiles.toml");
    std::fs::write(&profile_file, &profile_text).unwrap();
    std::fs::set_permissions(&profile_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let profile = Profile::load(&spec.worker_state_dir, &spec.env).unwrap();
    let manager = format!(
        r#"#!/bin/sh
set -eu
printf '%s\n' "$1" >> '{host}/calls'
if [ -e '{host}/fail' ]; then exit 19; fi
while [ -e '{host}/hold' ]; do /bin/sleep 0.05; done
version="$1"
bin="$NODENV_ROOT/versions/$version/bin"
/bin/mkdir -p "$bin"
/usr/bin/printf '#!/bin/sh\necho v%s\n' "$version" > "$bin/node"
/bin/chmod 700 "$bin/node"
"#,
        host = host.display()
    );
    crate::common::write_executable(&host.join("manager"), &manager);
    crate::common::write_executable(&host.join("node-build"), &manager);
    let registry = format!("schema_version=1\nactive_profile='base18'\nprofile_definition={:?}\n[[recipes]]\ntool='node'\nkind='Nodenv'\nmanager={:?}\ndependencies=[{:?}]\nsearch_path=[{:?}{}]\ninventory=['20.0.0','22.0.0']\n", profile.definition_digest().as_str(), host.join("manager").to_str().unwrap(),host.join("node-build").to_str().unwrap(),host.to_str().unwrap(), crate::common::system_search_path_toml());
    let registry_file = spec.worker_state_dir.join("toolchains.toml");
    std::fs::write(&registry_file, registry).unwrap();
    std::fs::set_permissions(&registry_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    (root, spec, host)
}

fn node_requirement(
    version: &str,
) -> Vec<remote_task_runner::toolchain::versions::EffectiveRequirement> {
    use remote_task_runner::toolchain::versions::{Constraint, EffectiveRequirement, ToolId};
    vec![EffectiveRequirement {
        tool: ToolId::parse("node").unwrap(),
        constraints: vec![Constraint::parse(version).unwrap()],
        provenance: vec![],
    }]
}

/// The real measurement of the local worker, reduced to node: the other
/// tools are stubs that answer nothing here, so they cannot be measured, and
/// what cannot be measured is (rightly) never called in sync.
fn measure(spec: &Spec) -> remote_task_runner::toolchain::envdiff::EnvReport {
    let mut report =
        remote_task_runner::toolchain::envreport::collect(&spec.worker_state_dir, &spec.env)
            .unwrap();
    report.tools.retain(|name, _| name == "node");
    report
}

/// `workers sync` for the local worker, the way the command runs it: measure,
/// install through the running resident, measure again.
async fn sync_node(
    spec: &Spec,
    version: &str,
) -> Result<remote_task_runner::service::workers::SyncResult, remote_task_runner::error::ApiError> {
    use remote_task_runner::service::workers::{sync_measured, warmup::ResidentWarmup};
    let (spec, version) = (spec.clone(), version.to_string());
    tokio::task::spawn_blocking(move || {
        let parent = measure(&spec);
        let mut installer = ResidentWarmup::new(spec.clone())
            .paced(Duration::from_millis(50), Duration::from_secs(60));
        sync_measured(
            spec.worker_id.as_str(),
            &parent,
            || Ok(measure(&spec)),
            Some(&node_requirement(&version)),
            &mut installer,
        )
    })
    .await
    .unwrap()
}

fn calls_of(host: &std::path::Path, version: &str) -> usize {
    std::fs::read_to_string(host.join("calls"))
        .unwrap_or_default()
        .lines()
        .filter(|line| *line == version)
        .count()
}

fn installed_node(spec: &Spec) -> Vec<String> {
    measure(spec).tools["node"]
        .managed
        .as_ref()
        .expect("the registry manages node")
        .installed
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_sync_installs_through_the_resident_then_measures_again_and_needs_the_resident() {
    let (_root, spec, host) = sync_fixture();
    let client = ensure(spec.clone()).await.unwrap();
    assert!(installed_node(&spec).is_empty());
    // The request reaches the worker, the worker installs, the second
    // measurement sees it.
    let done = sync_node(&spec, "20.0.0").await.unwrap();
    assert_eq!(done.installed.len(), 1);
    assert_eq!(done.installed[0].tool, "node");
    assert_eq!(done.installed[0].version, "20.0.0");
    assert_eq!(installed_node(&spec), ["20.0.0"]);
    assert_eq!(calls_of(&host, "20.0.0"), 1);
    assert!(done.in_sync);
    let line = remote_task_runner::service::workers::render_sync(&done, true);
    assert_eq!(
        line,
        format!(
            "worker {} is in sync; installed node 20.0.0",
            spec.worker_id
        )
    );
    // Again: nothing to install, nothing is run.
    let again = sync_node(&spec, "20.0.0").await.unwrap();
    assert!(again.installed.is_empty());
    assert_eq!(calls_of(&host, "20.0.0"), 1);
    // A warm-up of what is installed answers `unchanged` and runs no manager.
    let Reply::EnvSync { results } = client
        .request(Operation::EnvSync {
            worker: spec.worker_id.clone(),
            sync_id: remote_task_runner::ids::RequestId::generate(),
            tools: vec![remote_task_runner::wire::ToolRequirement {
                name: "node".into(),
                version: "20.0.0".into(),
            }],
        })
        .await
        .unwrap()
    else {
        panic!("the resident must answer the warm-up")
    };
    assert_eq!(
        results[0].outcome,
        remote_task_runner::wire::SyncOutcome::Unchanged
    );
    assert_eq!(calls_of(&host, "20.0.0"), 1);
    // A worker the resident does not hold is said.
    let Reply::ControlError { error } = client
        .request(Operation::EnvSync {
            worker: WorkerId::parse("nobody").unwrap(),
            sync_id: remote_task_runner::ids::RequestId::generate(),
            tools: vec![],
        })
        .await
        .unwrap()
    else {
        panic!("an unknown worker is an error")
    };
    assert_eq!(error.code, remote_task_runner::error::ErrorCode::NotFound);
    // Without a resident nothing is installed, none is started, and it is said.
    shutdown(&client).await;
    let error = sync_node(&spec, "22.0.0").await.unwrap_err();
    assert!(
        error.message.contains("the resident is not running"),
        "{}",
        error.message
    );
    assert_eq!(calls_of(&host, "22.0.0"), 0);
    assert!(!client.socket_path().exists(), "sync started a resident");
    assert_eq!(installed_node(&spec), ["20.0.0"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_warm_up_is_blocked_and_two_syncs_at_once_share_one_install() {
    let (_root, spec, host) = sync_fixture();
    let client = ensure(spec.clone()).await.unwrap();
    // The manager fails: blocked with what was not installed, never success.
    std::fs::write(host.join("fail"), "").unwrap();
    let error = sync_node(&spec, "20.0.0").await.unwrap_err();
    assert_eq!(error.code, remote_task_runner::error::ErrorCode::Blocked);
    assert_eq!(
        error.blocked_reason,
        Some(remote_task_runner::error::BlockedReason::EnvironmentMismatch)
    );
    assert!(
        error.message.contains("node 20.0.0 was not installed"),
        "{}",
        error.message
    );
    assert!(installed_node(&spec).is_empty());
    std::fs::remove_file(host.join("fail")).unwrap();
    // H1: the failed install left a private root without a receipt. The
    // same version installs on the next sync; it is not blocked for good.
    let again = sync_node(&spec, "20.0.0").await.unwrap();
    assert_eq!(again.installed.len(), 1);
    assert_eq!(again.installed[0].version, "20.0.0");
    assert_eq!(calls_of(&host, "20.0.0"), 2);
    assert_eq!(installed_node(&spec), ["20.0.0"]);
    // Two syncs of the same version at once: one manager run, both done.
    std::fs::write(host.join("hold"), "").unwrap();
    let first = tokio::spawn({
        let spec = spec.clone();
        async move { sync_node(&spec, "22.0.0").await }
    });
    let second = tokio::spawn({
        let spec = spec.clone();
        async move { sync_node(&spec, "22.0.0").await }
    });
    for _ in 0..600 {
        if calls_of(&host, "22.0.0") > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(calls_of(&host, "22.0.0"), 1, "the install must have begun");
    // Both are asking by now; the second one joins instead of installing.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!first.is_finished() && !second.is_finished());
    assert_eq!(calls_of(&host, "22.0.0"), 1);
    std::fs::remove_file(host.join("hold")).unwrap();
    for done in [first.await.unwrap(), second.await.unwrap()] {
        let done = done.unwrap();
        assert_eq!(done.installed.len(), 1);
        assert_eq!(done.installed[0].version, "22.0.0");
    }
    assert_eq!(calls_of(&host, "22.0.0"), 1);
    assert_eq!(installed_node(&spec), ["20.0.0", "22.0.0"]);
    shutdown(&client).await;
}

async fn wait_calls(host: &std::path::Path, version: &str, wanted: usize) {
    for _ in 0..1200 {
        if calls_of(host, version) >= wanted {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the manager was not asked for {version} {wanted} time(s)");
}

/// M4 (c), and a worker that restarts in the middle of an install: the local
/// worker lives in the resident, so both go away and come back together.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_goes_on_over_a_restart_of_the_resident_in_the_middle_of_an_install() {
    let (_root, spec, host) = sync_fixture();
    let client = ensure(spec.clone()).await.unwrap();
    std::fs::write(host.join("hold"), "").unwrap();
    let sync = tokio::spawn({
        let spec = spec.clone();
        async move { sync_node(&spec, "22.0.0").await }
    });
    // The manager runs: the sync was answered `pending` well before that.
    wait_calls(&host, "22.0.0", 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!sync.is_finished());
    shutdown(&client).await;
    // While nothing listens, the sync asks again instead of ending.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !sync.is_finished(),
        "the sync ended while the resident was away"
    );
    let client = ensure(spec.clone()).await.unwrap();
    std::fs::remove_file(host.join("hold")).unwrap();
    let done = tokio::time::timeout(Duration::from_secs(120), sync)
        .await
        .expect("the sync must end after the resident is back")
        .unwrap()
        .unwrap();
    assert_eq!(done.installed.len(), 1);
    assert_eq!(done.installed[0].version, "22.0.0");
    assert_eq!(installed_node(&spec), ["22.0.0"]);
    // The stopped installer left a root without a receipt; it was cleared
    // and the version installed again, or the first one finished: never
    // more than one installer at a time, never a failure.
    assert!((1..=2).contains(&calls_of(&host, "22.0.0")));
    shutdown(&client).await;
}

/// M4 (d), before the worker forgets the interrupted warm-up: the install
/// the first sync started goes on, and the second sync does not start
/// another one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_sync_after_an_interrupted_one_finds_or_joins_the_same_install() {
    let (_root, spec, host) = sync_fixture();
    let client = ensure(spec.clone()).await.unwrap();
    std::fs::write(host.join("hold"), "").unwrap();
    // The first sync: one look, then nobody asks any more (Ctrl-C).
    let Reply::EnvSync { results } = client
        .request(Operation::EnvSync {
            worker: spec.worker_id.clone(),
            sync_id: remote_task_runner::ids::RequestId::generate(),
            tools: vec![remote_task_runner::wire::ToolRequirement {
                name: "node".into(),
                version: "20.0.0".into(),
            }],
        })
        .await
        .unwrap()
    else {
        panic!("the resident must answer the warm-up")
    };
    assert_eq!(
        results[0].outcome,
        remote_task_runner::wire::SyncOutcome::Pending
    );
    wait_calls(&host, "20.0.0", 1).await;
    let second = tokio::spawn({
        let spec = spec.clone();
        async move { sync_node(&spec, "20.0.0").await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!second.is_finished());
    assert_eq!(calls_of(&host, "20.0.0"), 1);
    std::fs::remove_file(host.join("hold")).unwrap();
    let done = tokio::time::timeout(Duration::from_secs(120), second)
        .await
        .expect("the second sync must end")
        .unwrap()
        .unwrap();
    assert!(done.in_sync);
    assert!(done.installed.len() <= 1);
    assert_eq!(calls_of(&host, "20.0.0"), 1, "one install, shared");
    assert_eq!(installed_node(&spec), ["20.0.0"]);
    shutdown(&client).await;
}

/// M4 (e): the install of a warm-up passes the worker's
/// task-traffic-controller gate. With the budget taken, its entry waits in
/// the worker's ledger and the manager is not run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_install_of_a_warm_up_waits_for_the_traffic_gate_of_the_worker() {
    use task_traffic_controller::config::Config;
    use task_traffic_controller::{managed, Context};
    let (_root, spec, host) = sync_fixture();
    let client = ensure(spec.clone()).await.unwrap();
    let scope = worker_traffic(&spec.env);
    let cfg = Config {
        budget: 8,
        cpu_threshold_percent: 100.0,
        memory_reserve_mib: 0,
        ..Config::default()
    };
    let ctx = Context::new(task_traffic_controller::paths::Paths::new(
        &scope.state_dir,
        &scope.config_file,
    ));
    // The whole budget of the worker is taken by something else.
    let id = "warm-up-admission-blocker".to_string();
    let command = managed::CommandSpec {
        command_id: id.clone(),
        spec_digest: "private-fixture".into(),
        opts: task_traffic_controller::exec::Opts {
            label: None,
            weight: Some(8),
            front: false,
            key: None,
            argv: vec!["/usr/bin/true".into()],
            cwd: host.clone(),
            env: None,
            stdout_log: None,
            parent: None,
        },
        attempt: None,
        services: vec![],
    };
    let managed::Registration::Registered(_, guard) =
        managed::register(&ctx, &cfg, &command).unwrap()
    else {
        panic!("fresh blocker")
    };
    assert!(matches!(
        managed::try_admit(&ctx, cfg.limits(), &id, 0.0).unwrap(),
        managed::TryAdmit::Admitted
    ));
    let entries = || {
        task_traffic_controller::ledger::Ledger::load(&ctx.paths.ledger_file())
            .map(|ledger| ledger.entries.len())
            .unwrap_or(0)
    };
    assert_eq!(entries(), 1);
    let sync = tokio::spawn({
        let spec = spec.clone();
        async move { sync_node(&spec, "20.0.0").await }
    });
    // The installer registers in the worker's ledger, behind the blocker.
    for _ in 0..1200 {
        if entries() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        entries(),
        2,
        "the install did not enter the worker's ledger"
    );
    // It is not admitted: the manager cannot start during this wait.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(calls_of(&host, "20.0.0"), 0);
    assert!(!sync.is_finished());
    managed::finish(&ctx, guard, managed::TerminalKind::Cancelled).unwrap();
    let done = tokio::time::timeout(Duration::from_secs(120), sync)
        .await
        .expect("the sync must end once the gate opens")
        .unwrap()
        .unwrap();
    assert_eq!(done.installed.len(), 1);
    assert_eq!(calls_of(&host, "20.0.0"), 1);
    assert_eq!(installed_node(&spec), ["20.0.0"]);
    shutdown(&client).await;
}

/// The project file's output plan reaches the worker through the public
/// commands: `success`, `failure` and `always` follow the command's outcome,
/// and globs and several paths select the members of one output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_exec_output_conditions_globs_and_multiple_paths_follow_the_outcome() {
    let (root, spec, input) = control_fixture();
    std::fs::write(
        input.join(".remote-task-runner.toml"),
        "schema_version=1\n\
         [[outputs]]\nname='passed'\npaths=['ok/**']\nrequired=true\nwhen='success'\n\
         [[outputs]]\nname='failed'\npaths=['fail/*.log','extra/note.txt']\nrequired=true\nwhen='failure'\n\
         [[outputs]]\nname='both'\npaths=['reports/**','coverage/*.txt']\n",
    )
    .unwrap();
    let script = "mkdir ok fail fail/deep extra reports reports/unit coverage || exit 98; \
                  echo ok > ok/done; echo log > fail/a.log; echo skipped > fail/a.txt; \
                  echo deep > fail/deep/b.log; echo note > extra/note.txt; \
                  echo report > reports/unit/r.xml; echo cover > coverage/c.txt; \
                  echo raw > coverage/c.bin; exit \"$1\"";
    let temporary = root.path().join("reader-tmp");
    std::fs::create_dir(&temporary).unwrap();
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let profile = || std::env::var_os("LLVM_PROFILE_FILE").map(|v| ("LLVM_PROFILE_FILE", v));
    for (code, stored, skipped) in [
        (23, ["both", "failed"], "passed"),
        (0, ["both", "passed"], "failed"),
    ] {
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new(&spec.executable)
                .args(["--json", "exec", "--", "/bin/sh", "-c", script, "sh"])
                .arg(code.to_string())
                .env_clear()
                .envs(profile())
                .envs(&spec.env)
                .current_dir(&input)
                .output(),
        )
        .await
        .unwrap_or_else(|elapsed| {
            diagnose_coordinator(&spec);
            diagnose_worker(&spec);
            panic!("output-plan exec did not finish: {elapsed}");
        })
        .unwrap();
        // A required output whose condition does not hold is not a missing one:
        // the command's own exit code is what the caller sees.
        assert_eq!(
            output.status.code(),
            Some(code),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let done: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let run = done["result"]["target"]["id"].as_str().unwrap().to_string();
        let task =
            remote_task_runner::ids::TaskId::parse(done["result"]["task_id"].as_str().unwrap())
                .unwrap();
        let attempt = {
            let owner = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
            let attempts = owner.attempts_of_task(&task).unwrap();
            assert_eq!(attempts.len(), 1);
            let names: Vec<String> = owner
                .persisted_artifacts(&attempts[0].attempt_id)
                .unwrap()
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            assert_eq!(names, stored, "exit {code}");
            attempts[0].attempt_id.clone()
        };

        let exported = root.path().join(format!("exported-{code}"));
        let export = tokio::process::Command::new(&spec.executable)
            .args(["--json", "outputs", &run, "--dir"])
            .arg(&exported)
            .env_clear()
            .envs(profile())
            .envs(&spec.env)
            .env("TMPDIR", &temporary)
            .env("PATH", "/no-programs")
            .current_dir(root.path())
            .output()
            .await
            .unwrap();
        assert!(
            export.status.success(),
            "{} {}",
            String::from_utf8_lossy(&export.stdout),
            String::from_utf8_lossy(&export.stderr)
        );
        let prefix = exported.join(task.as_str()).join(attempt.as_str());
        let text = |path: &str| std::fs::read_to_string(prefix.join(path)).unwrap();
        // `**` descends, `*` stays in one directory, and a literal path is one member.
        assert_eq!(text("both/reports/unit/r.xml"), "report\n");
        assert_eq!(text("both/coverage/c.txt"), "cover\n");
        assert!(!prefix.join("both/coverage/c.bin").exists());
        assert!(!prefix.join(skipped).exists());
        if code == 0 {
            assert_eq!(text("passed/ok/done"), "ok\n");
        } else {
            assert_eq!(text("failed/fail/a.log"), "log\n");
            assert_eq!(text("failed/extra/note.txt"), "note\n");
            assert!(!prefix.join("failed/fail/a.txt").exists());
            assert!(!prefix.join("failed/fail/deep").exists());
        }
    }
    shutdown(&load(&spec)).await;
}

// ---------------------------------------------------------------- `ci`

/// A project of one workflow in the directory `ci` is run from. Every step
/// leaves a line in `marks/<job>`, outside the workspace: what ran, and how
/// often, is read from there.
fn ci_project(root: &std::path::Path, input: &std::path::Path, jobs: &str) -> std::path::PathBuf {
    let marks = root.join("marks");
    std::fs::create_dir_all(&marks).unwrap();
    std::fs::create_dir_all(input.join(".github/workflows")).unwrap();
    let workflow = format!("on: push\njobs:\n{jobs}").replace("MARKS", marks.to_str().unwrap());
    std::fs::write(input.join(".github/workflows/ci.yml"), workflow).unwrap();
    marks
}

fn ci_command(spec: &Spec, input: &std::path::Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(&spec.executable);
    command
        .args(["--json", "ci"])
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .current_dir(input);
    command
}

async fn ci_output(spec: &Spec, input: &std::path::Path) -> (Option<i32>, serde_json::Value) {
    ci_output_with(spec, input, &[]).await
}

/// [`ci_output`] with more arguments after `ci`.
async fn ci_output_with(
    spec: &Spec,
    input: &std::path::Path,
    more: &[&str],
) -> (Option<i32>, serde_json::Value) {
    let mut command = ci_command(spec, input);
    command.args(more);
    let waited = tokio::time::timeout(Duration::from_secs(240), command.output());
    let output = match waited.await {
        Ok(output) => output.unwrap(),
        Err(elapsed) => {
            diagnose_coordinator(spec);
            diagnose_worker(spec);
            panic!("ci did not end: {elapsed}");
        }
    };
    let value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "{e}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code(), value)
}

/// The jobs of the report, by name: status and why it did not run.
fn ci_jobs(value: &serde_json::Value) -> BTreeMap<String, (String, Option<String>)> {
    let jobs = value["result"]["jobs"].as_array().unwrap();
    jobs.iter()
        .map(|job| {
            let text = |key: &str| job[key].as_str().map(str::to_string);
            let name = text("job").unwrap_or_else(|| text("plan_task").unwrap());
            (name, (text("status").unwrap(), text("skipped")))
        })
        .collect()
}

fn ci_marks(marks: &std::path::Path, job: &str) -> Vec<String> {
    let text = std::fs::read_to_string(marks.join(job)).unwrap_or_default();
    text.lines().map(str::to_string).collect()
}

/// How many attempts each task of the run had, by the task of the plan.
fn ci_attempts(spec: &Spec, value: &serde_json::Value) -> BTreeMap<String, usize> {
    let run =
        remote_task_runner::ids::RunId::parse(value["result"]["target"]["id"].as_str().unwrap())
            .unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let rows = store.ci_run_tasks(&run).unwrap();
    rows.into_iter()
        .map(|row| {
            let task = row.spec["ci"]["task"]["task_id"]
                .as_str()
                .unwrap()
                .to_string();
            (task, store.attempts_of_task(&row.task_id).unwrap().len())
        })
        .collect()
}

/// `ci` end to end with the real binary, the resident and its local worker:
/// a workflow file, real processes, three jobs in a chain of `needs`. Each
/// job is a task of its own, runs once, and the next one gets the outputs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_runs_a_chain_of_needs_with_outputs_and_every_job_once() {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(
        root.path(),
        &input,
        "  build:\n    runs-on: ubuntu-latest\n    outputs:\n      version: ${{ steps.v.outputs.version }}\n    steps:\n      - run: echo ran >> MARKS/build\n      - id: v\n        run: echo \"version=1.2.3\" >> \"$GITHUB_OUTPUT\"\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    outputs:\n      seen: ${{ steps.s.outputs.seen }}\n    steps:\n      - id: s\n        run: |\n          echo \"got ${{ needs.build.outputs.version }}\" >> MARKS/test\n          echo \"seen=${{ needs.build.outputs.version }}\" >> \"$GITHUB_OUTPUT\"\n  deploy:\n    needs: test\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"deploy ${{ needs.test.outputs.seen }}\" >> MARKS/deploy\n",
    );
    let (code, value) = ci_output(&spec, &input).await;
    assert_eq!(code, Some(0), "{value}");
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"]["state"], "settled");
    assert_eq!(value["result"]["snapshot"]["task_count"], 3);
    assert!(value["result"]["plan_digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
    let jobs = ci_jobs(&value);
    for job in ["build", "test", "deploy"] {
        assert_eq!(jobs[job], ("success".to_string(), None), "{job}: {value}");
    }
    assert_eq!(ci_marks(&marks, "build"), ["ran"]);
    assert_eq!(ci_marks(&marks, "test"), ["got 1.2.3"]);
    assert_eq!(ci_marks(&marks, "deploy"), ["deploy 1.2.3"]);
    let attempts = ci_attempts(&spec, &value);
    assert_eq!(attempts.len(), 3, "a task per job: {attempts:?}");
    assert!(attempts.values().all(|n| *n == 1), "{attempts:?}");
    shutdown(&load(&spec)).await;
}

/// A worker that holds as many frozen plans as it may does not take one
/// more. The run does not wait without a word: it gets the event
/// `ci_plan_refused` once, the worker is left out of the placement by name
/// (shown with the task), and when the worker has room again the plan is
/// delivered and the run goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_says_why_it_waits_for_a_worker_whose_plan_store_is_full_and_goes_on_after() {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(
        root.path(),
        &input,
        "  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/build\n",
    );
    // As many plans as a worker keeps, none of them the plan of this run.
    let plans = remote_task_runner::worker::ci_task::plans_dir(&spec.worker_state_dir);
    std::fs::create_dir_all(&plans).unwrap();
    let fillers: Vec<std::path::PathBuf> = (0..remote_task_runner::worker::ci_task::MAX_PLANS)
        .map(|n| {
            let digest =
                remote_task_runner::ids::Digest::of_bytes(format!("filler {n}").as_bytes());
            let path =
                remote_task_runner::worker::ci_task::plan_path(&spec.worker_state_dir, &digest);
            std::fs::write(&path, b"{}").unwrap();
            path
        })
        .collect();

    let mut command = ci_command(&spec, &input);
    command.kill_on_drop(true);
    let running = tokio::spawn(async move { command.output().await.unwrap() });
    let events = |kind: &str| -> Vec<remote_task_runner::storage::Event> {
        let Ok(store) = CoordinatorStore::open_read_only(&spec.state_dir) else {
            return Vec::new();
        };
        let all = store.events_after(0, 10_000).unwrap_or_default();
        all.into_iter().filter(|e| e.kind == kind).collect()
    };
    let started = std::time::Instant::now();
    let (refused, waiting) = loop {
        let (refused, waiting) = (events("ci_plan_refused"), events("task_waiting"));
        if !refused.is_empty() && !waiting.is_empty() {
            break (refused, waiting);
        }
        if started.elapsed() > Duration::from_secs(120) {
            diagnose_coordinator(&spec);
            diagnose_worker(&spec);
            panic!("the run did not say why it waits");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(!running.is_finished());
    let reason = refused[0].payload["reason"].as_str().unwrap().to_string();
    assert!(reason.contains("OVERLOADED"), "{reason}");
    assert!(refused[0].payload["worker"].is_string());
    let excluded = &waiting[0].payload["decision"]["excluded"];
    assert_eq!(excluded[0]["why"], "input_refused", "{excluded}");
    assert_eq!(excluded[0]["unmet"], serde_json::json!([reason]));
    // As `show` reads it.
    let run = remote_task_runner::ids::RunId::parse(refused[0].run_id.as_deref().unwrap()).unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let tasks = store.query_tasks(run, 16, None).unwrap();
    assert_eq!(tasks.items.len(), 1);
    let placement = tasks.items[0].placement.clone().unwrap();
    assert_eq!(placement.excluded[0].unmet, std::slice::from_ref(&reason));
    drop(store);
    assert!(ci_marks(&marks, "build").is_empty());

    // The worker has room again: the delivery that is tried again arrives.
    for path in &fillers {
        std::fs::remove_file(path).unwrap();
    }
    let output = match tokio::time::timeout(Duration::from_secs(240), running).await {
        Ok(output) => output.unwrap(),
        Err(elapsed) => {
            diagnose_coordinator(&spec);
            diagnose_worker(&spec);
            panic!("ci did not end: {elapsed}");
        }
    };
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(0), "{value}");
    assert_eq!(ci_jobs(&value)["build"].0, "success", "{value}");
    assert_eq!(ci_marks(&marks, "build"), ["ran"]);
    // Told once, however often the delivery was tried.
    assert_eq!(events("ci_plan_refused").len(), 1);
    shutdown(&load(&spec)).await;
}

/// A job that fails: the jobs that need it are skipped without a step
/// started (and so is what needs those), `if: always()` and `if: failure()`
/// run, and the run is a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_skips_what_needs_a_failed_job_and_runs_always_and_failure() {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(
        root.path(),
        &input,
        "  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/build; exit 3\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/test\n  deploy:\n    needs: test\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/deploy\n  report:\n    needs: test\n    if: always()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"test was ${{ needs.test.result }}\" >> MARKS/report\n  alarm:\n    needs: build\n    if: failure()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/alarm\n  calm:\n    needs: build\n    if: success()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/calm\n",
    );
    let (code, value) = ci_output(&spec, &input).await;
    assert!(code.is_some_and(|code| code != 0), "{code:?} {value}");
    assert_eq!(value["ok"], false);
    assert_eq!(value["result"]["state"], "settled");
    assert_eq!(value["result"]["snapshot"]["task_count"], 6);
    let jobs = ci_jobs(&value);
    assert_eq!(jobs["build"].0, "failure", "{value}");
    assert_eq!(
        jobs["test"],
        ("skipped".to_string(), Some("needs failed".to_string()))
    );
    assert_eq!(jobs["deploy"].0, "skipped", "{value}");
    assert_eq!(jobs["calm"].0, "skipped", "{value}");
    assert_eq!(jobs["report"].0, "success", "{value}");
    assert_eq!(jobs["alarm"].0, "success", "{value}");
    assert_eq!(ci_marks(&marks, "build"), ["ran"]);
    assert_eq!(ci_marks(&marks, "report"), ["test was skipped"]);
    assert_eq!(ci_marks(&marks, "alarm"), ["ran"]);
    for skipped in ["test", "deploy", "calm"] {
        assert!(ci_marks(&marks, skipped).is_empty(), "{skipped} ran");
    }
    let attempts = ci_attempts(&spec, &value);
    assert_eq!(attempts.len(), 6);
    assert!(attempts.values().all(|n| *n == 1), "{attempts:?}");
    shutdown(&load(&spec)).await;
}

/// Ctrl-C on `ci`: the job that runs is stopped, the job that waits for it
/// is never placed, and the run settles as cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_sigint_cancels_the_running_job_and_the_ones_that_wait() {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(
        root.path(),
        &input,
        "  first:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo started >> MARKS/first; sleep 60\n  second:\n    needs: first\n    if: always()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/second\n",
    );
    let mut child = ci_command(&spec, &input)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    assert!(pid > 1);
    let identity = remote_task_runner::platform::process_start_identity(pid);
    assert!(identity.identity().is_some());
    let started = tokio::time::timeout(Duration::from_secs(120), async {
        while ci_marks(&marks, "first").is_empty() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "ci exited before its job started"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    started.await.unwrap_or_else(|elapsed| {
        diagnose_coordinator(&spec);
        diagnose_worker(&spec);
        panic!("the first job did not start: {elapsed}");
    });
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    assert!(remote_task_runner::platform::signal_pid(pid, libc::SIGINT));
    let output = tokio::time::timeout(Duration::from_secs(60), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(output.status.code(), Some(130 | 143)),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["result"]["state"], "stop_confirmed");
    assert_eq!(value["result"]["cancel_acknowledged"], true);
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    assert!(store.list_nonterminal_attempts().unwrap().is_empty());
    assert!(store.ci_waiting_runs(8).unwrap().is_empty());
    let attempts = ci_attempts(&spec, &value);
    let mut counts: Vec<usize> = attempts.values().copied().collect();
    counts.sort();
    assert_eq!(
        counts,
        [0, 1],
        "the job that waited was never placed: {attempts:?}"
    );
    // Not even `always()` runs after the run was cancelled before it was placed.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(ci_marks(&marks, "second").is_empty());
    assert_eq!(ci_marks(&marks, "first"), ["started"]);
    shutdown(&load(&spec)).await;
}

/// `ci` on a Git repository: the worktree as it is (with what is not
/// committed), and a commit named on the command line.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_runs_the_worktree_of_a_git_repository_and_a_named_commit() {
    let (root, mut spec) = fixture();
    // The worker makes the workspace of a Git source with fast-worktree.
    spec.env.insert(
        "PATH".into(),
        private_tool_path(root.path(), &["git", "fast-worktree"]),
    );
    let config = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        config.join("config.toml"),
        "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\n",
    )
    .unwrap();
    remote_task_runner::service::bootstrap::write_input(&config.join("coordinator.json"), &spec)
        .unwrap();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let jobs = |word: &str| {
        format!(
            "  build:\n    runs-on: ubuntu-latest\n    outputs:\n      word: ${{{{ steps.w.outputs.word }}}}\n    steps:\n      - id: w\n        run: echo \"word={word}\" >> \"$GITHUB_OUTPUT\"\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"${{{{ needs.build.outputs.word }}}}\" >> MARKS/test\n"
        )
    };
    let marks = ci_project(root.path(), &repo, &jobs("committed"));
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    // Not committed: the worktree says another word.
    ci_project(root.path(), &repo, &jobs("dirty"));

    let (code, value) = ci_output(&spec, &repo).await;
    assert_eq!(code, Some(0), "{value}");
    assert_eq!(ci_marks(&marks, "test"), ["dirty"]);
    let more = ["--snapshot", "commit", "--ref", "HEAD"];
    let (code, value) = ci_output_with(&spec, &repo, &more).await;
    assert_eq!(code, Some(0), "{value}");
    assert_eq!(ci_marks(&marks, "test"), ["dirty", "committed"]);
    let jobs = ci_jobs(&value);
    assert_eq!(jobs["build"].0, "success");
    assert_eq!(jobs["test"].0, "success");
    shutdown(&load(&spec)).await;
}

/// A32, end to end with the real binary and a fake `gh` (a script that
/// writes down how it was called; nothing here reaches GitHub): the success
/// of a dirty worktree is not posted to the original HEAD, a run of exactly
/// what the commit holds is posted to it once, and a post that fails leaves
/// the result and the exit code as they were and is shown with the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a32_ci_posts_a_status_only_for_what_the_commit_holds_and_a_failed_post_changes_no_result()
{
    let (root, mut spec) = fixture();
    spec.env.insert(
        "PATH".into(),
        private_tool_path(root.path(), &["git", "fast-worktree"]),
    );
    let (calls, refuse) = (root.path().join("gh-calls"), root.path().join("gh-refuse"));
    let gh = root.path().join("bin/gh");
    std::fs::write(
        &gh,
        format!(
            "#!/bin/sh\nprintf \x27%s\\n\x27 \"$*\" >> {calls:?}\nif [ -e {refuse:?} ]; then echo \x27gh: Bad credentials (HTTP 401)\x27 >&2; exit 1; fi\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let config = root.path().join(".config/remote-task-runner");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        config.join("config.toml"),
        "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\n",
    )
    .unwrap();
    remote_task_runner::service::bootstrap::write_input(&config.join("coordinator.json"), &spec)
        .unwrap();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let jobs = |word: &str| {
        format!("  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo {word} >> MARKS/build\n")
    };
    ci_project(root.path(), &repo, &jobs("committed"));
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["remote", "add", "origin", "git@github.com:owner/name.git"]);
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    let head = git(&["rev-parse", "HEAD"]);
    let called = || std::fs::read_to_string(&calls).unwrap_or_default();
    let status = |value: &serde_json::Value| value["result"]["github_status"].clone();

    // A tracked file changed and nothing committed: computed, never posted.
    ci_project(root.path(), &repo, &jobs("dirty"));
    let (code, value) = ci_output(&spec, &repo).await;
    assert_eq!(code, Some(0), "{value}");
    let dirty = status(&value);
    assert_eq!(dirty["computed"], "success", "{value}");
    assert_eq!(dirty["posting"], "skipped", "{value}");
    assert_eq!(dirty["original_head"], head.as_str());
    assert!(dirty["source_digest"].is_string());
    let why = dirty["reason"].as_str().unwrap();
    assert!(why.contains("uncommitted or synthetic content"), "{why}");
    assert_eq!(called(), "", "a dirty snapshot posts nothing");

    // Only a file Git does not know yet: the same.
    git(&["checkout", "-q", "."]);
    std::fs::write(repo.join("untracked.txt"), "new\n").unwrap();
    let (code, value) = ci_output(&spec, &repo).await;
    assert_eq!(code, Some(0), "{value}");
    assert_eq!(status(&value)["computed"], "success", "{value}");
    assert_eq!(status(&value)["posting"], "skipped", "{value}");
    let why = status(&value)["reason"].as_str().unwrap().to_string();
    assert!(why.contains("uncommitted or synthetic content"), "{why}");
    assert_eq!(called(), "");

    // The commit itself, while the worktree is still dirty: posted to it.
    let more = ["--snapshot", "commit", "--ref", "HEAD"];
    let (code, value) = ci_output_with(&spec, &repo, &more).await;
    assert_eq!(code, Some(0), "{value}");
    let posted = status(&value);
    assert_eq!(posted["posting"], "posted", "{value}");
    assert_eq!(posted["sha"], head.as_str());
    assert_eq!(posted["state"], "success");
    assert_eq!(
        called(),
        format!(
            "api repos/owner/name/statuses/{head} -f state=success -f context=local-ci -f description=success: pass 1 / fail 0 / error 0 / skip 0\n"
        )
    );

    // A clean worktree is what its HEAD holds; `--no-status` posts nothing.
    std::fs::remove_file(repo.join("untracked.txt")).unwrap();
    let (code, value) = ci_output_with(&spec, &repo, &["--no-status"]).await;
    assert_eq!(code, Some(0), "{value}");
    assert_eq!(status(&value)["reason"], "--no-status", "{value}");
    assert_eq!(called().lines().count(), 1);

    // The post is refused: the result and the exit code stay, and the
    // failure is said apart from them and kept with the run.
    std::fs::write(&refuse, "").unwrap();
    let more = ["--status-context", "other"];
    let (code, value) = ci_output_with(&spec, &repo, &more).await;
    assert_eq!(code, Some(0), "{value}");
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"]["state"], "settled");
    let failed = status(&value);
    assert_eq!(failed["computed"], "success", "{value}");
    assert_eq!(failed["posting"], "failed", "{value}");
    assert_eq!(failed["sha"], head.as_str());
    assert_eq!(failed["context"], "other");
    assert_eq!(failed["error"], "gh: Bad credentials (HTTP 401)");
    let last = called();
    let last = last.lines().last().unwrap();
    assert!(last.contains("-f context=other"), "{last}");
    assert_eq!(called().lines().count(), 2, "one attempt, no retry");
    let run = value["result"]["target"]["id"].as_str().unwrap();
    let shown = Command::new(&spec.executable)
        .args(["--json", "show", run])
        .current_dir(&repo)
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)))
        .envs(&spec.env)
        .output()
        .unwrap();
    let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(shown["data"]["run"]["github_status"], failed, "{shown}");

    // No task a worker was given carries anything of the status.
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let run = remote_task_runner::ids::RunId::parse(run).unwrap();
    for row in store.ci_run_tasks(&run).unwrap() {
        let text = row.spec.to_string();
        assert!(
            !text.contains("statuses") && !text.contains("other"),
            "{text}"
        );
    }
    shutdown(&load(&spec)).await;
}
