use remote_task_runner::config::resolve::WorkerSpec;
use remote_task_runner::config::runtime::{WorkerInputs, WorkerTarget};
use remote_task_runner::config::{resolve, Inputs, Overlay, ResolvedConfig};
use remote_task_runner::ids::*;
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{accept, connect, ClientConfig, Connected, ServerConfig};
use remote_task_runner::scheduler::UnknownCosts;
use remote_task_runner::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::sync::Arc;

fn config() -> ResolvedConfig {
    resolve(&Inputs {
        overlay: &Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap()
}
fn inputs(root: &std::path::Path) -> WorkerInputs {
    WorkerInputs {
        state_dir: root.join("worker"),
        worker_id: WorkerId::parse("local").unwrap(),
        boot_id: WorkerBootId::parse("boot-1").unwrap(),
        supervisor_exe: env!("CARGO_BIN_EXE_remote-task-runner").into(),
        cluster_key_path: root.join("cluster.key"),
        env: BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), root.display().to_string()),
        ]),
    }
}

#[test]
fn runtime_factory_enables_setup_and_starts_agent() {
    let root = crate::test_tmp::tempdir().unwrap();
    let mut cfg = config();
    cfg.max_active_task.value = 3;
    cfg.prefetch_per_worker.value = 2;
    cfg.log_spool_limit_bytes.value = 8192;
    cfg.term_grace_seconds.value = 4;
    cfg.profile.value = Some("custom".into());
    let input = inputs(root.path());
    let runtime = cfg.worker_runtime(input.clone()).unwrap();
    assert_eq!(runtime.agent_config().state_dir, input.state_dir);
    assert_eq!(
        runtime.setup_config().state_dir,
        runtime.agent_config().state_dir
    );
    assert_eq!(
        runtime.setup_config().worker_id,
        runtime.agent_config().worker_id
    );
    assert_eq!(
        runtime.setup_config().supervisor_exe,
        runtime.agent_config().supervisor_exe
    );
    for (key, value) in &input.env {
        assert_eq!(runtime.setup_config().env.get(key), Some(value));
    }
    assert_eq!(runtime.agent_config().profile, "default");
    assert!(runtime
        .setup_config()
        .profile
        .starts_with("default:sha256:"));
    assert_ne!(
        runtime.agent_config().profile_fingerprint,
        Digest::of_bytes(b"")
    );
    assert_eq!(
        runtime.setup_config().traffic_state_dir,
        runtime.agent_config().traffic_state_dir
    );
    assert_eq!(
        runtime.setup_config().traffic_config,
        runtime.agent_config().traffic_config
    );
    assert_eq!(runtime.setup_config().term_grace_ms, 4000);
    assert_eq!(runtime.setup_config().log_limit_bytes, 8192);
    assert_eq!(runtime.setup_config().lease_ms, 60_000);
    assert_eq!(
        runtime.setup_config().poll_ms,
        runtime.agent_config().poll_ms
    );
    assert_eq!(
        runtime.setup_config().kill_wait_ms,
        runtime.agent_config().kill_wait_ms
    );
    assert_eq!(
        runtime.setup_config().inhibit_sleep,
        runtime.agent_config().inhibit_sleep
    );
    assert_eq!(runtime.agent_config().retention_ms, 7 * 86400 * 1000);
    assert!(runtime.open_host().unwrap().setups().is_some());
    let agent = runtime.start().unwrap();
    assert_eq!(agent.config().max_active_task, 3);
    assert_eq!(agent.config().prefetch_per_worker, 2);
    assert_eq!(agent.config().log_limit_bytes, 8192);
    assert_eq!(agent.config().forecast_ttl_ms, 2000);
    agent.shutdown();
}

#[test]
fn runtime_factory_preserves_git_preflight_error() {
    let root = crate::test_tmp::tempdir().unwrap();
    let mut input = inputs(root.path());
    input.env.insert(
        "PATH".into(),
        root.path().join("missing").display().to_string(),
    );
    let err = config()
        .worker_runtime(input)
        .unwrap()
        .open_host()
        .err()
        .unwrap();
    assert_eq!(err.code, remote_task_runner::error::ErrorCode::Blocked);
    assert!(err.message.contains("Git") || err.message.contains("git"));
}

async fn connected(c: &remote_task_runner::coordinator::Coordinator, name: &str) -> Connected {
    let (near, far) = duplex_pair(65536);
    let cluster = c.store().cluster_id().clone();
    let client = ClientConfig::new(
        cluster.clone(),
        c.store().coordinator_id().clone(),
        c.epoch(),
    );
    let server = ServerConfig::new(cluster, super::proto_common::identity(name));
    let (connection, accepted) = tokio::join!(connect(near, client), accept(far, server));
    // Keep the server side alive for the lifetime of the test.
    tokio::spawn(async move {
        let mut incoming = accepted.unwrap().incoming;
        while let Some(_request) = incoming.next().await {}
    });
    connection.unwrap()
}

#[tokio::test]
async fn runtime_attach_applies_only_verified_identity_priorities() {
    let root = crate::test_tmp::tempdir().unwrap();
    let mut cfg = config();
    cfg.local_worker_priority.value = -9;
    let mut priority = cfg.local_worker_priority.clone();
    priority.value = 21;
    cfg.workers.push(WorkerSpec {
        id: "remote".into(),
        ssh: "host".into(),
        binary: None,
        service_mode: None,
        labels: vec![],
        ephemeral: true,
        priority,
    });
    let settings = cfg.runtime_settings().unwrap();
    let c = settings
        .start_coordinator(
            CoordinatorStore::open(root.path(), 1).unwrap(),
            Arc::new(UnknownCosts),
        )
        .unwrap();
    let local = WorkerId::parse("local").unwrap();
    let remote = WorkerId::parse("remote").unwrap();
    cfg.attach_worker(
        &c,
        WorkerTarget::Local(&local),
        connected(&c, "local").await,
    )
    .await
    .unwrap();
    cfg.attach_worker(
        &c,
        WorkerTarget::Remote("remote"),
        connected(&c, "remote").await,
    )
    .await
    .unwrap();
    assert_eq!(c.scheduler().priority_of(&local), -9);
    assert_eq!(c.scheduler().priority_of(&remote), 21);
    assert_eq!(c.scheduler().cfg, settings.scheduler);
    let other = WorkerId::parse("other").unwrap();
    for target in [
        WorkerTarget::Local(&local),
        WorkerTarget::Remote("remote"),
        WorkerTarget::Remote("missing"),
    ] {
        assert!(cfg
            .attach_worker(&c, target, connected(&c, "other").await)
            .await
            .is_err());
    }
    assert_eq!(c.scheduler().priority_of(&other), 0);
    let foreign_root = crate::test_tmp::tempdir().unwrap();
    let foreign = settings
        .start_coordinator(
            CoordinatorStore::open(foreign_root.path(), 1).unwrap(),
            Arc::new(UnknownCosts),
        )
        .unwrap();
    assert!(cfg
        .attach_worker(
            &c,
            WorkerTarget::Local(&local),
            connected(&foreign, "local").await
        )
        .await
        .is_err());
    cfg.local_worker.value = false;
    assert!(cfg
        .attach_worker(
            &c,
            WorkerTarget::Local(&local),
            connected(&c, "local").await
        )
        .await
        .is_err());
    assert_eq!(c.scheduler().priority_of(&local), -9);
}

/// A worker attached the way a remote one is (`attach_worker`): `workers
/// sync` is sent to a peer whose hello agreed on `env_sync_v1`, and never to
/// one that did not offer it.
#[tokio::test]
async fn runtime_attach_carries_whether_the_worker_answers_env_sync() {
    use remote_task_runner::error::ErrorCode;
    use remote_task_runner::ids::RequestId;
    use remote_task_runner::wire::{ToolRequirement, ENV_SYNC_FEATURE};
    let root = crate::test_tmp::tempdir().unwrap();
    let mut cfg = config();
    for id in ["new", "old"] {
        cfg.workers.push(WorkerSpec {
            id: id.into(),
            ssh: "host".into(),
            binary: None,
            service_mode: None,
            labels: vec![],
            ephemeral: true,
            priority: cfg.local_worker_priority.clone(),
        });
    }
    let settings = cfg.runtime_settings().unwrap();
    let c = settings
        .start_coordinator(
            CoordinatorStore::open(root.path(), 1).unwrap(),
            Arc::new(UnknownCosts),
        )
        .unwrap();
    cfg.attach_worker(&c, WorkerTarget::Remote("new"), connected(&c, "new").await)
        .await
        .unwrap();
    // An older worker: the same hello without the feature.
    let (near, far) = duplex_pair(65536);
    let cluster = c.store().cluster_id().clone();
    let client = ClientConfig::new(
        cluster.clone(),
        c.store().coordinator_id().clone(),
        c.epoch(),
    );
    let mut server = ServerConfig::new(cluster, super::proto_common::identity("old"));
    server.local.features.retain(|f| f != ENV_SYNC_FEATURE);
    let (connection, accepted) = tokio::join!(connect(near, client), accept(far, server));
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = seen.clone();
    tokio::spawn(async move {
        let mut incoming = accepted.unwrap().incoming;
        while let Some(_request) = incoming.next().await {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    cfg.attach_worker(&c, WorkerTarget::Remote("old"), connection.unwrap())
        .await
        .unwrap();
    let tools = || {
        vec![ToolRequirement {
            name: "node".into(),
            version: "20.0.0".into(),
        }]
    };
    let old = WorkerId::parse("old").unwrap();
    let refused = c
        .env_sync(&old, RequestId::generate(), tools())
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::ProtocolIncompatible);
    assert!(
        refused.message.contains("workers upgrade old"),
        "{}",
        refused.message
    );
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 0);
    // The new one is sent the request (this fixture's worker answers
    // nothing, so there is no outcome: not the refusal above).
    let new = WorkerId::parse("new").unwrap();
    let sent = c
        .env_sync(&new, RequestId::generate(), tools())
        .await
        .unwrap_err();
    assert_ne!(
        sent.code,
        ErrorCode::ProtocolIncompatible,
        "{}",
        sent.message
    );
}
