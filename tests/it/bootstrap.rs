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

fn fixture() -> (tempfile::TempDir, Spec) {
    let root = tempfile::Builder::new()
        .prefix("rtr-boot-")
        .tempdir_in(std::fs::canonicalize("/tmp").unwrap())
        .unwrap();
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
            ("PATH".into(), "/usr/bin:/bin".into()),
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
    (root, spec)
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
    // Simulate the pre-index schema with durable rows and no running owner.
    let db = rusqlite::Connection::open(spec.state_dir.join("coordinator.db")).unwrap();
    db.execute_batch(
        "DROP TRIGGER task_inherit_cancel; DROP TRIGGER task_keep_cancel; DROP TRIGGER attempt_cancel_gate; DROP TRIGGER run_inherit_control_revision; DROP TABLE run_cancel_intent; DROP TABLE control_request; ALTER TABLE run DROP COLUMN control_revision; ALTER TABLE task DROP COLUMN min_generation; ALTER TABLE task DROP COLUMN authorization_revision; DROP INDEX task_run_key; UPDATE schema_meta SET value='3' WHERE key='schema_version';",
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
    spec.env
        .insert("PATH".into(), std::env::var("PATH").unwrap());
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
            if let Ok(Reply::Task { attempts }) = &snapshot {
                let worker = remote_task_runner::storage::WorkerStore::open_read_only(
                    &spec.worker_state_dir,
                )
                .unwrap();
                for a in attempts {
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
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].termination.as_ref().unwrap().outcome(),
        remote_task_runner::state::Outcome::Succeeded,
        "{attempts:?}"
    );
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
    let sources = remote_task_runner::source::SourceStore::open(&spec.state_dir).unwrap();
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
    assert_eq!(report.cli_exit_code, 0);
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
    let mut context = Context::load(spec.env.clone(), dir.clone(), spec).unwrap();
    let run = remote_task_runner::ids::RunId::generate();
    let first = input::capture(&context, &run, None).unwrap();
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
    let root = tempfile::tempdir().unwrap();
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

fn control_fixture() -> (tempfile::TempDir, Spec, std::path::PathBuf) {
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
    let (_root, spec, input) = control_fixture();
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(["--json", "exec", "--", "/bin/sh", "-c", "sleep 30"])
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
    .unwrap();
    assert_eq!(
        remote_task_runner::platform::process_start_identity(pid),
        identity
    );
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(pid).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
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
    let report = tokio::time::timeout(
        Duration::from_secs(20),
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
    let sources = remote_task_runner::source::SourceStore::open(&spec.state_dir).unwrap();
    let source = sources.source_of_run(&run).unwrap().unwrap();
    let manifest = sources.manifest(&source).unwrap().unwrap();
    let blob = &manifest.object_manifest.entries[0].hash;
    std::fs::remove_file(sources.object_store().object_path(blob)).unwrap();
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
