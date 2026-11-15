//! Private IPC and real local-worker composition; no process bootstrap claim.
use remote_task_runner::config::runtime::WorkerInputs;
use remote_task_runner::config::{resolve, Inputs as ConfigInputs, Overlay, ResolvedConfig};
use remote_task_runner::coordinator::TaskSpec;
use remote_task_runner::ids::*;
use remote_task_runner::platform::wall_ms;
use remote_task_runner::results::CollectConfig;
use remote_task_runner::service::resident::{
    Inputs, Operation, Reply, Request, Resident, MAX_REQUEST_BYTES,
};
use remote_task_runner::source::{capture_for_run, CaptureParams, SourceRequest, SourceStore};
use remote_task_runner::state::Outcome;
use remote_task_runner::storage::CoordinatorStore;
use remote_task_runner::wire::{CommandSpec, LogStream};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

fn root() -> tempfile::TempDir {
    crate::test_tmp::prefixed("rtr-res-").unwrap()
}
fn config() -> ResolvedConfig {
    let mut c = resolve(&ConfigInputs {
        overlay: &Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    c.heartbeat_seconds.value = 1;
    c.stale_seconds.value = 3;
    c
}
fn inputs(root: &Path) -> Inputs {
    remote_task_runner::secrets::key::init(&root.join("key")).unwrap();
    Inputs {
        state_dir: root.join("parent-state"),
        worker: WorkerInputs {
            state_dir: root.join("worker"),
            worker_id: WorkerId::parse("local").unwrap(),
            boot_id: WorkerBootId::parse("boot-test").unwrap(),
            supervisor_exe: env!("CARGO_BIN_EXE_remote-task-runner").into(),
            cluster_key_path: root.join("key"),
            env: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), root.display().to_string()),
                ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
            ]),
        },
        collection_interval: Duration::from_millis(30),
        collection: CollectConfig::default(),
    }
}
async fn stop(
    client: &remote_task_runner::service::resident::Client,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
async fn raw(path: &Path, request: &Request) -> UnixStream {
    let mut stream = UnixStream::connect(path).await.unwrap();
    let bytes = serde_json::to_vec(request).unwrap();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_socket_keeps_accepted_work_running_after_submitter_disconnects_and_generic_outputs(
) {
    let root = root();
    let input = inputs(root.path());
    std::fs::create_dir(&input.worker.state_dir).unwrap();
    std::fs::set_permissions(
        &input.worker.state_dir,
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input.txt"), "snapshot\n").unwrap();
    let run = RunId::generate();
    // Source forwarding is outside this layer: seed the immutable worker store.
    let store = SourceStore::open(&input.worker.state_dir).unwrap();
    let captured = capture_for_run(
        &input.worker.env,
        &store,
        &run,
        &SourceRequest::Directory { dir: source },
        &CaptureParams::default(),
    )
    .unwrap();
    let resident = Resident::start(&config(), input).await.unwrap();
    let client = resident.client();
    let runner = tokio::spawn(resident.run());
    let task = TaskId::generate();
    let manifest = &captured.manifest.manifest_digest;
    let spec = TaskSpec {
        toolchains: None,
        outputs: vec![
            remote_task_runner::wire::OutputDecl {
                name: "bundle".into(),
                paths: vec!["reports/**".into(), "coverage/**".into(), "reports".into()],
                if_missing: remote_task_runner::wire::MissingPolicy::Required,
                when: remote_task_runner::wire::OutputWhen::Success,
            },
            remote_task_runner::wire::OutputDecl {
                name: "unsafe".into(),
                paths: vec!["forbidden".into()],
                if_missing: remote_task_runner::wire::MissingPolicy::Optional,
                when: remote_task_runner::wire::OutputWhen::Always,
            },
        ],
        requirements: Default::default(),
        in_place: false,
        setup: None,
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(manifest)
            .or_else(|_| Digest::parse(&format!("sha256:{manifest}")))
            .unwrap(),
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "mkdir reports coverage; ln -s /etc/hosts forbidden; printf report > reports/a; printf cover > coverage/b; sleep 1; cat input.txt".into(),
            ],
            cwd: String::new(),
            env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![remote_task_runner::wire::ArtifactDecl { name:"raw".into(), path:"input.txt".into(), if_missing:remote_task_runner::wire::MissingPolicy::Required }],
    };
    let submitter = client.clone();
    let reply = submitter
        .request(Operation::Submit {
            run_id: run.clone(),
            source_id: captured.manifest.source_id,
            tasks: vec![(task.clone(), spec)],
        })
        .await
        .unwrap();
    assert!(
        matches!(&reply, Reply::Accepted { run_id } if run_id == &run),
        "{reply:?}"
    );
    drop(submitter); // The submitting connection has already closed.
    let attempt = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await
                .unwrap()
            {
                if let Some(a) = attempts.into_iter().next() {
                    if a.receipts.collected_at_ms.is_some() {
                        break a;
                    }
                    if a.termination
                        .is_some_and(|t| t.outcome() != Outcome::Succeeded)
                    {
                        panic!("unexpected termination: {:?}", a.termination);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempt.termination.unwrap().outcome(), Outcome::Succeeded);
    stop(&client, runner).await;
    let files =
        remote_task_runner::results::ResultStore::open(&root.path().join("parent-state/results"))
            .unwrap();
    assert_eq!(
        files
            .read_log(&attempt.attempt_id, LogStream::Stdout)
            .unwrap(),
        b"snapshot\n"
    );
    let result: remote_task_runner::wire::GetResultResponse =
        serde_json::from_slice(&files.read_result(&attempt.attempt_id).unwrap().unwrap()).unwrap();
    assert!(result.outputs_evaluated);
    assert_eq!(result.artifacts.len(), 2);
    assert_eq!(result.artifacts_not_stored.len(), 1);
    assert_eq!(result.artifacts_not_stored[0].name, "unsafe");
    assert_eq!(
        result.artifacts_not_stored[0].reason,
        remote_task_runner::wire::NotStoredReason::Refused
    );
    let bundle = result
        .artifacts
        .iter()
        .find(|a| a.name == "bundle")
        .unwrap();
    assert_eq!(bundle.name, "bundle");
    assert_eq!(
        bundle.encoding,
        remote_task_runner::wire::ArtifactEncoding::TarV1
    );
    let bytes = std::fs::read(files.artifact_path(&attempt.attempt_id, &bundle.digest)).unwrap();
    assert_eq!(Digest::of_bytes(&bytes), bundle.digest);
    assert_eq!(bytes.len() as u64, bundle.size);
    let mut archive = tar::Archive::new(bytes.as_slice());
    let paths: Vec<_> = archive
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().to_str().unwrap().to_owned())
        .collect();
    assert_eq!(paths, ["coverage/b", "reports/a"]);
    let worker =
        remote_task_runner::storage::WorkerStore::open_read_only(&root.path().join("worker"))
            .unwrap();
    let journal = worker
        .events_of(&attempt.attempt_id, "artifacts_stored")
        .unwrap();
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].payload["outputs_evaluated"], true);
    assert_eq!(journal[0].payload["artifacts"][0]["encoding"], "tar_v1");
    let immutable = worker.get_result(&attempt.attempt_id).unwrap().unwrap();
    assert_eq!(
        remote_task_runner::wire::payload_hash(&immutable).unwrap(),
        result.result_digest
    );
    // Public CLI works offline without Git, project configuration or an owner.
    let config_root = root.path().join("config");
    let config_dir = config_root.join("remote-task-runner");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::set_permissions(&config_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = remote_task_runner::service::bootstrap::Spec {
        state_dir: root.path().join("parent-state"),
        worker_state_dir: root.path().join("worker"),
        worker_id: WorkerId::parse("local").unwrap(),
        cluster_key_path: root.path().join("key"),
        executable: env!("CARGO_BIN_EXE_remote-task-runner").into(),
        env: BTreeMap::new(),
        repo_root: None,
        expected_cluster: None,
        managed_service: None,
    };
    remote_task_runner::service::bootstrap::write_input(
        &config_dir.join("coordinator.json"),
        &spec,
    )
    .unwrap();
    let before = CoordinatorStore::open_read_only(&spec.state_dir)
        .unwrap()
        .epoch();
    let no_git = root.path().join("no-git");
    std::fs::create_dir(&no_git).unwrap();
    let exported = root.path().join("exported");
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
        .env_clear()
        .env("XDG_CONFIG_HOME", &config_root)
        .env("PATH", "/unavailable")
        .current_dir(&no_git)
        .args(["--json", "outputs", run.as_str(), "--dir"])
        .arg(&exported)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        lines
            .iter()
            .filter(|l| l["data"]["kind"] == "final")
            .count(),
        1
    );
    assert_eq!(lines.last().unwrap()["data"]["summary"]["exported"], 3);
    assert_eq!(lines.last().unwrap()["data"]["summary"]["refused"], 1);
    let prefix = exported
        .join(task.as_str())
        .join(attempt.attempt_id.as_str());
    assert_eq!(std::fs::read(prefix.join("raw")).unwrap(), b"snapshot\n");
    assert_eq!(
        std::fs::read(prefix.join("bundle/reports/a")).unwrap(),
        b"report"
    );
    assert_eq!(
        std::fs::read(prefix.join("bundle/coverage/b")).unwrap(),
        b"cover"
    );
    assert_eq!(
        CoordinatorStore::open_read_only(&spec.state_dir)
            .unwrap()
            .epoch(),
        before
    );
    let conflict = tokio::process::Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
        .env_clear()
        .env("XDG_CONFIG_HOME", &config_root)
        .current_dir(&no_git)
        .args([
            "--json",
            "outputs",
            task.as_str(),
            "--attempt",
            attempt.attempt_id.as_str(),
            "--dir",
        ])
        .arg(&exported)
        .output()
        .await
        .unwrap();
    assert_eq!(conflict.status.code(), Some(72));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_has_one_writable_owner_and_client_queries_never_advance_epoch() {
    let root = root();
    let resident = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    let client = resident.client();
    let state = root.path().join("parent-state");
    let epoch = CoordinatorStore::open_read_only(&state).unwrap().epoch();
    assert!(Resident::start(&config(), inputs(root.path()))
        .await
        .is_err());
    assert!(CoordinatorStore::open(&state, wall_ms()).is_err());
    let runner = tokio::spawn(resident.run());
    for _ in 0..8 {
        assert!(
            matches!(client.request(Operation::Task { task_id: TaskId::generate() }).await.unwrap(), Reply::Task { attempts } if attempts.is_empty())
        );
    }
    assert_eq!(
        CoordinatorStore::open_read_only(&state).unwrap().epoch(),
        epoch
    );
    stop(&client, runner).await;
    let restarted = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    assert_ne!(
        CoordinatorStore::open_read_only(&state).unwrap().epoch(),
        epoch
    );
    drop(restarted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_rejects_foreign_credentials_oversized_frames_and_schema() {
    let root = root();
    let resident = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    let client = resident.client();
    assert_eq!(
        std::fs::metadata(client.socket_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(client.socket_path().parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let token = std::fs::read_to_string(client.socket_path().parent().unwrap().join("owner.token"))
        .unwrap();
    let runner = tokio::spawn(resident.run());
    let rejected = RunId::generate();
    for (version, cluster, owner) in [
        (2, ClusterId::generate(), token.clone()),
        (2, client.cluster_id().clone(), "wrong".into()),
        (1, client.cluster_id().clone(), token),
    ] {
        for operation in [
            Operation::Shutdown,
            Operation::Cancel {
                request_id: RunId::generate().to_string(),
                expected_control_revision: 0,
                target: remote_task_runner::service::completion::Target::Run(rejected.clone()),
            },
        ] {
            let mut stream = raw(
                client.socket_path(),
                &Request {
                    schema_version: version,
                    cluster_id: cluster.clone(),
                    owner_token: owner.clone(),
                    operation,
                },
            )
            .await;
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        }
    }
    let mut stream = UnixStream::connect(client.socket_path()).await.unwrap();
    stream
        .write_u32(MAX_REQUEST_BYTES as u32 + 1)
        .await
        .unwrap();
    let mut byte = [0];
    assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    assert!(matches!(
        client
            .request(Operation::Task {
                task_id: TaskId::generate()
            })
            .await
            .unwrap(),
        Reply::Task { .. }
    ));
    assert!(matches!(
        client
            .request(Operation::Query {
                query: remote_task_runner::service::query::Query::Completion {
                    target: remote_task_runner::service::completion::Target::Run(rejected)
                }
            })
            .await
            .unwrap(),
        Reply::QueryError {
            code: remote_task_runner::error::ErrorCode::NotFound,
            ..
        }
    ));
    stop(&client, runner).await;
}

#[tokio::test]
async fn resident_rejects_symlinked_or_shared_state_before_opening_database() {
    let root = root();
    let target = root.path().join("target");
    std::fs::create_dir(&target).unwrap();
    let mut input = inputs(root.path());
    std::os::unix::fs::symlink(&target, &input.state_dir).unwrap();
    assert!(Resident::start(&config(), input).await.is_err());
    assert!(!target.join("coordinator.db").exists());
    input = inputs(root.path());
    input.state_dir = target;
    assert!(Resident::start(&config(), input).await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn resident_startup_does_not_block_the_executor_during_git_preflight() {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    let root = root();
    let bin = root.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let ready = bin.join("ready");
    let release = bin.join("release");
    assert!(std::process::Command::new("mkfifo")
        .arg(&ready)
        .arg(&release)
        .status()
        .unwrap()
        .success());
    // Hold both FIFO ends open before starting Git. The watchdog can always
    // queue the release, even if startup fails before Git opens either FIFO.
    let open_fifo = |path| {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .unwrap()
    };
    let mut ready = open_fifo(&ready);
    let mut release = open_fifo(&release);
    let git = bin.join("git");
    std::fs::write(
        &git,
        r#"#!/bin/sh
dir=${0%/*}
printf '%s\n' "$*" >> "$dir/calls"
# Profile discovery queries Git with its own timeout. Gate the following
# Machine preflight instead, so a synchronous regression requires our watchdog.
# Later capability probes must not wait for a spent watchdog.
if mkdir "$dir/discovered" 2>/dev/null; then
    :
elif mkdir "$dir/claimed" 2>/dev/null; then
    printf 'ready\n' > "$dir/ready"
    read -r released < "$dir/release"
fi
printf 'git version 2.50.0\n'
"#,
    )
    .unwrap();
    std::fs::set_permissions(&git, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let observer = std::thread::spawn(move || {
        let mut poll = libc::pollfd {
            fd: ready.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // Git may start after other native probes under instrumentation. Match
        // bootstrap's 20-second startup budget for reaching the gate; only the
        // separate five-second response below measures executor responsiveness.
        let observed = unsafe { libc::poll(&mut poll, 1, 20_000) } == 1;
        let mut message = [0; 6];
        let observed = observed && ready.read_exact(&mut message).is_ok() && message == *b"ready\n";
        let progressed = observed
            && ready_tx.send(()).is_ok()
            && release_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        // Do this before reporting any assertion failure, including no reader.
        release.write_all(b"release\n").unwrap();
        // Keep both FIFOs open until startup has consumed the release, even if
        // readiness timed out before Git opened the ready FIFO.
        (progressed, ready, release)
    });
    let releaser = tokio::spawn(async move {
        if ready_rx.await.is_ok() {
            let _ = release_tx.send(());
        }
    });
    let mut input = inputs(root.path());
    input
        .worker
        .env
        .insert("PATH".into(), format!("{}:/usr/bin:/bin", bin.display()));
    let started = Resident::start(&config(), input).await;
    releaser.await.unwrap();
    let (progressed, _ready, _release) = observer.join().unwrap();
    let resident = started.unwrap();
    let calls = std::fs::read_to_string(bin.join("calls")).unwrap();
    let client = resident.client();
    stop(&client, tokio::spawn(resident.run())).await;
    assert!(
        progressed,
        "startup blocked the sole executor thread until the watchdog released Git"
    );
    assert!(
        calls.lines().count() >= 2,
        "exercise subsequent Git probes: {calls:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn failed_listener_startup_releases_both_owner_and_agent_locks() {
    let root = root();
    let input = inputs(root.path());
    let ipc = input.state_dir.join("parent");
    std::fs::create_dir_all(&ipc).unwrap();
    for directory in [&input.state_dir, &ipc] {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let occupied = ipc.join("request.sock");
    std::fs::write(&occupied, b"preserve this file").unwrap();
    assert!(Resident::start(&config(), input).await.is_err());
    assert_eq!(std::fs::read(&occupied).unwrap(), b"preserve this file");
    // No leftover owner or Agent lock may prevent a clean subsequent startup.
    std::fs::remove_file(occupied).unwrap();
    let resident = Resident::start(&config(), inputs(root.path()))
        .await
        .unwrap();
    let client = resident.client();
    stop(&client, tokio::spawn(resident.run())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_records_verified_source_transfer_and_complete_terminal_tail() {
    resident_measured_fixture(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resident_computation_and_collection_survive_measurement_writer_failure() {
    resident_measured_fixture(true).await;
}
async fn resident_measured_fixture(metrics_fail: bool) {
    let root = root();
    let input = inputs(root.path());
    let state = input.state_dir.clone();
    let env = input.worker.env.clone();
    std::fs::create_dir(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    let source = root.path().join("source-measured");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("payload"), vec![b'x'; 128 * 1024]).unwrap();
    let run = RunId::generate();
    let sources = SourceStore::open(&state).unwrap();
    let vault = remote_task_runner::secrets::vault::Vault::open(&state).unwrap();
    let key = remote_task_runner::secrets::key::load(&root.path().join("key")).unwrap();
    let captured = remote_task_runner::secrets::capture::capture_run(
        &env,
        &sources,
        &vault,
        &remote_task_runner::secrets::capture::SecretInputs {
            key: &key,
            declared: &[],
        },
        &run,
        &SourceRequest::Directory { dir: source },
        &CaptureParams::default(),
    )
    .unwrap();
    let manifest = &captured.manifest.manifest_digest;
    let spec = TaskSpec {
        toolchains: None,
        outputs: vec![],
        requirements: Default::default(),
        in_place: false,
        setup: None,
        plan_digest: Digest::of_bytes(b"measured-plan"),
        manifest_digest: Digest::parse(manifest)
            .or_else(|_| Digest::parse(&format!("sha256:{manifest}")))
            .unwrap(),
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), "cat payload".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![],
    };
    let resident = Resident::start(&config(), input).await.unwrap();
    if metrics_fail {
        let db = rusqlite::Connection::open(state.join("parent/stats/costs-v1.sqlite")).unwrap();
        db.execute_batch("CREATE TRIGGER fixture_deny_metrics BEFORE INSERT ON costs BEGIN SELECT RAISE(ABORT,'fixture telemetry writer failure'); END;").unwrap();
    }
    let client = resident.client();
    let runner = tokio::spawn(resident.run());
    let task = TaskId::generate();
    let reply = client
        .request(Operation::SubmitCaptured {
            run_id: run,
            source_id: captured.manifest.source_id,
            tasks: vec![(task.clone(), spec)],
        })
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    let attempt = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Reply::Task { attempts } = client
                .request(Operation::Task {
                    task_id: task.clone(),
                })
                .await
                .unwrap()
            {
                if let Some(attempt) = attempts
                    .into_iter()
                    .find(|a| a.receipts.collected_at_ms.is_some())
                {
                    break attempt;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempt.termination.unwrap().outcome(), Outcome::Succeeded);
    // Preview must not create captured sources, admissions or measurement rows.
    let source_files = std::fs::read_dir(sources.root()).unwrap().count();
    for _ in 0..(if metrics_fail { 0 } else { 100 }) {
        let reply = client
            .request(Operation::Estimate {
                query: remote_task_runner::service::estimate::Query {
                    version: 1,
                    argv: vec!["/bin/true".into()],
                    repository_id: None,
                    cwd_relative: String::new(),
                    requirements: Default::default(),
                },
            })
            .await
            .unwrap();
        assert!(matches!(reply, Reply::Estimate { .. }), "{reply:?}");
    }
    assert_eq!(
        std::fs::read_dir(sources.root()).unwrap().count(),
        source_files
    );
    stop(&client, runner).await;
    let db = rusqlite::Connection::open(state.join("parent/stats/costs-v1.sqlite")).unwrap();
    let samples: Vec<(String, i64)> = db
        .prepare("SELECT receipt,duration FROM costs ORDER BY receipt")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    if metrics_fail {
        assert!(samples.is_empty(), "{samples:?}");
        return;
    }
    assert_eq!(samples.len(), 2, "{samples:?}");
    assert!(samples
        .iter()
        .any(|(receipt, _)| receipt.starts_with("transfer:")));
    assert!(samples
        .iter()
        .any(|(receipt, _)| receipt.starts_with("terminal-tail:")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_exits_cleanly_when_its_state_directory_is_gone_or_replaced() {
    for replaced in [false, true] {
        let root = root();
        let resident = Resident::start(&config(), inputs(root.path()))
            .await
            .unwrap();
        let client = resident.client();
        let runner = tokio::spawn(resident.run());
        assert!(matches!(
            client.request(Operation::Ping).await.unwrap(),
            Reply::Ready { .. }
        ));
        let state = root.path().join("parent-state");
        // One atomic step: the path stops naming the directory this owner opened.
        std::fs::rename(&state, root.path().join("moved")).unwrap();
        let foreign = state.join("parent/request.sock");
        if replaced {
            // Another owner's directory under the same name is not ours to clean.
            std::fs::create_dir_all(foreign.parent().unwrap()).unwrap();
            std::fs::write(&foreign, b"foreign").unwrap();
        }
        // No shutdown request is sent: the heartbeat tick notices on its own.
        let result = tokio::time::timeout(Duration::from_secs(10), runner)
            .await
            .expect("owner keeps running without its state directory")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert!(client.request(Operation::Ping).await.is_err());
        if replaced {
            assert_eq!(std::fs::read(&foreign).unwrap(), b"foreign");
        }
    }
}
