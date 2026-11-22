use belt::config::resolve::WorkerSpec;
use belt::config::runtime::{WorkerInputs, WorkerTarget};
use belt::config::{resolve, Inputs, Overlay, ResolvedConfig};
use belt::ids::*;
use belt::protocol::transport::duplex_pair;
use belt::protocol::{accept, connect, ClientConfig, Connected, ServerConfig};
use belt::scheduler::UnknownCosts;
use belt::storage::CoordinatorStore;
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
        supervisor_exe: env!("CARGO_BIN_EXE_belt").into(),
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
    cfg.warmup_idle_seconds.value = 7;
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
        if key == "PATH" {
            let mut path = std::env::split_paths(&runtime.setup_config().env[key]);
            assert!(path
                .next()
                .unwrap()
                .starts_with(input.state_dir.join("tool-bin")));
            assert!(path.eq(std::env::split_paths(value)));
        } else {
            assert_eq!(runtime.setup_config().env.get(key), Some(value));
        }
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
    // The scope of the task-traffic-controller CLI under this HOME, for the
    // agent, its forecasts, the supervisors and the setup builders alike.
    assert_eq!(
        runtime.agent_config().traffic_state_dir,
        root.path().join(".local/state/task-traffic-controller")
    );
    assert_eq!(
        runtime.agent_config().traffic_config,
        Some(
            root.path()
                .join(".config/task-traffic-controller/config.toml")
        )
    );
    assert_eq!(runtime.setup_config().term_grace_ms, 4000);
    assert_eq!(runtime.agent_config().warmup_idle_ms, 7000);
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
fn the_worker_scope_follows_the_state_home_and_reaches_every_command() {
    let root = crate::test_tmp::tempdir().unwrap();
    let mut input = inputs(root.path());
    let state_home = root.path().join("xdg-state");
    input
        .env
        .insert("XDG_STATE_HOME".into(), state_home.display().to_string());
    let runtime = config().worker_runtime(input.clone()).unwrap();
    let scope = state_home.join("task-traffic-controller");
    assert_eq!(runtime.agent_config().traffic_state_dir, scope);
    assert_eq!(runtime.setup_config().traffic_state_dir, scope);
    assert_eq!(
        runtime.setup_config().traffic_config,
        runtime.agent_config().traffic_config
    );
    // What a nested task-traffic-controller needs to find the same ledger.
    let profile = runtime.agent_config().capability_profile.clone().unwrap();
    let step = profile.execution_environment(&BTreeMap::new()).unwrap();
    assert_eq!(step["XDG_STATE_HOME"], input.env["XDG_STATE_HOME"]);
    assert_eq!(step["HOME"], input.env["HOME"]);
    assert_eq!(step["TASK_TRAFFIC_CONTROLLER_NESTED"], "inherit");
    // A setup step gets the same, on top of the toolchain environment (which
    // the keys of prepared work are made from, and which does not hold it).
    let setup = runtime.setup_config();
    assert_eq!(setup.scope_env.as_ref(), Some(profile.scope_environment()));
    assert_eq!(
        setup.scope_env.as_ref().unwrap()["XDG_STATE_HOME"],
        input.env["XDG_STATE_HOME"]
    );
    assert!(!setup.env.contains_key("XDG_STATE_HOME"));
    // The worker's own helpers (git, the workspace tools, the tool probes)
    // run with what a step gets.
    assert_eq!(profile.host_environment(), step);

    // Without anything to resolve the scope from, there is no worker.
    input.env.remove("XDG_STATE_HOME");
    input.env.remove("HOME");
    let error = config().worker_runtime(input).err().unwrap().to_string();
    assert!(error.contains("task-traffic-controller scope"), "{error}");
}

#[test]
fn a_live_command_in_the_former_worker_scope_refuses_the_start() {
    use task_traffic_controller::{ledger::Ledger, lock, Context, Paths};
    let root = crate::test_tmp::tempdir().unwrap();
    let input = inputs(root.path());
    let legacy = input.state_dir.join("traffic");
    let ctx = Context::new(Paths::new(&legacy, legacy.join("config.toml")));
    let mut ledger = Ledger::default();
    ledger.entries.push(
        serde_json::from_value(serde_json::json!({
            "id": "old-run", "pid": std::process::id(), "state": "running", "front": false,
            "weight": 1, "predicted_rss": 0, "repo": "", "argv": "x", "key": "k",
            "seq": 1, "enqueued_at": "2030-01-01T00:00:00Z",
        }))
        .unwrap(),
    );
    ledger.save(&ctx.paths.ledger_file()).unwrap();
    let lease = lock::take_lease(&ctx, "old-run").unwrap();
    let error = config()
        .worker_runtime(input.clone())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("live commands (old-run)"), "{error}");
    assert!(error.contains(&legacy.display().to_string()), "{error}");
    // Nothing was started in the shared scope behind the refusal.
    assert!(!root.path().join(".local").exists());
    // Once the old command is over the worker moves; the old scope stays.
    drop(lease);
    let runtime = config().worker_runtime(input).unwrap();
    assert_eq!(
        runtime.agent_config().traffic_state_dir,
        root.path().join(".local/state/task-traffic-controller")
    );
    assert!(legacy.join("ledger.json").exists());
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
    assert_eq!(err.code, belt::error::ErrorCode::Blocked);
    assert!(err.message.contains("Git") || err.message.contains("git"));
}

async fn connected(c: &belt::coordinator::Coordinator, name: &str) -> Connected {
    connected_without(c, name, &[]).await
}

async fn connected_without(
    c: &belt::coordinator::Coordinator,
    name: &str,
    missing: &[&str],
) -> Connected {
    let (near, far) = duplex_pair(65536);
    let cluster = c.store().cluster_id().clone();
    let client = ClientConfig::new(
        cluster.clone(),
        c.store().coordinator_id().clone(),
        c.epoch(),
        super::proto_common::membership("parent"),
    );
    let mut server = ServerConfig::new(
        cluster,
        super::proto_common::identity(name),
        belt::group::Keys::only(super::proto_common::group_key()),
    );
    server
        .local
        .features
        .retain(|f| !missing.contains(&f.as_str()));
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

#[tokio::test]
async fn dispatch_and_artifact_tasks_require_their_negotiated_features() {
    use belt::coordinator::policy::unmet_ci;
    use belt::wire::{MATRIX_DISPATCH_FEATURE, WORKFLOW_ARTIFACTS_FEATURE};

    let fx = super::coordinator::fx();
    let coordinator = fx.open(&[]);
    let config = config();
    let worker = WorkerId::parse("local").unwrap();
    let mut spec = super::coordinator::spec();
    spec.commands.clear();
    spec.ci = Some(
        serde_json::from_value(serde_json::json!({
            "task": {
                "plan_digest": spec.plan_digest,
                "task_id": "ci.yml/build",
                "inputs": {"needs": {}},
                "dispatch": {},
                "artifact_workflow": "test.yml"
            }
        }))
        .unwrap(),
    );
    let cases: &[(&[&str], Option<&str>)] = &[
        (
            &[MATRIX_DISPATCH_FEATURE, WORKFLOW_ARTIFACTS_FEATURE],
            Some("protocol feature ci_matrix_dispatch_v1"),
        ),
        (
            &[WORKFLOW_ARTIFACTS_FEATURE],
            Some("protocol feature ci_workflow_artifacts_v1"),
        ),
        (&[], None),
    ];
    for &(missing, expected) in cases {
        let connection = connected_without(&coordinator, worker.as_str(), missing).await;
        let link = config
            .attach_gated_worker(
                &coordinator,
                WorkerTarget::Local(&worker),
                connection,
                Arc::default(),
            )
            .await
            .unwrap();
        assert_eq!(unmet_ci(&spec, link.as_ref()), expected);
    }
}

/// A worker attached the way a remote one is (`attach_worker`): `workers
/// sync` is sent to a peer whose hello agreed on `env_sync_v1`, and never to
/// one that did not offer it.
#[tokio::test]
async fn runtime_attach_carries_whether_the_worker_answers_env_sync() {
    use belt::error::ErrorCode;
    use belt::ids::RequestId;
    use belt::wire::{ToolRequirement, ENV_SYNC_FEATURE};
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
        super::proto_common::membership("parent"),
    );
    let mut server = ServerConfig::new(
        cluster,
        super::proto_common::identity("old"),
        belt::group::Keys::only(super::proto_common::group_key()),
    );
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

/// Spec 24.3: a retired identity that answers again is not attached, and is
/// told that it was retired before the connection goes. The notice is the
/// only thing sent to it, it is sent on each such connection (it may not
/// have arrived), and the first sending is recorded.
#[tokio::test]
async fn a_retired_identity_that_answers_is_told_so_and_is_not_attached() {
    use belt::wire::{IdentityRetiredResponse, Request, Response, SchemaVersion};
    let root = crate::test_tmp::tempdir().unwrap();
    let mut cfg = config();
    let priority = cfg.local_worker_priority.clone();
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
    let remote = WorkerId::parse("remote").unwrap();
    let boot = super::proto_common::identity("remote").worker_boot_id;
    cfg.attach_worker(
        &c,
        WorkerTarget::Remote("remote"),
        connected(&c, "remote").await,
    )
    .await
    .unwrap();
    c.detach(&remote).await.unwrap();
    let forgotten = c.forget_worker(&remote, Some(true)).await.unwrap();
    assert!(forgotten.is_ok_and(|f| f.identity.as_deref() == Some(boot.as_str())));
    assert_eq!(
        c.store().identity_retired_notice(&remote, &boot).unwrap(),
        None
    );

    let mut first_sent = None;
    for _ in 0..2 {
        let (near, far) = duplex_pair(65536);
        let cluster = c.store().cluster_id().clone();
        let client = ClientConfig::new(
            cluster.clone(),
            c.store().coordinator_id().clone(),
            c.epoch(),
            super::proto_common::membership("parent"),
        );
        let server = ServerConfig::new(
            cluster,
            super::proto_common::identity("remote"),
            belt::group::Keys::only(super::proto_common::group_key()),
        );
        let (connection, accepted) = tokio::join!(connect(near, client), accept(far, server));
        let (seen, mut requests) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut incoming = accepted.unwrap().incoming;
            while let Some(inbound) = incoming.next().await {
                let _ = seen.send(inbound.request.clone());
                if let Request::IdentityRetired(r) = &inbound.request {
                    let answer = Response::IdentityRetired(IdentityRetiredResponse {
                        schema_version: SchemaVersion::CURRENT,
                        worker_id: r.worker_id.clone(),
                        worker_boot_id: r.worker_boot_id.clone(),
                    });
                    let _ = inbound.responder.respond(answer).await;
                }
            }
        });
        let refused = cfg
            .attach_worker(&c, WorkerTarget::Remote("remote"), connection.unwrap())
            .await
            .err()
            .unwrap();
        assert_eq!(refused.code, belt::error::ErrorCode::Conflict);
        assert!(refused.message.contains("retired"), "{}", refused.message);
        let Some(Request::IdentityRetired(notice)) = requests.recv().await else {
            panic!("the first request to a retired identity is the notice")
        };
        assert_eq!(&notice.coordinator_id, c.store().coordinator_id());
        assert_eq!(notice.worker_id, remote);
        assert_eq!(notice.worker_boot_id, boot);
        // Nothing else was sent to it: no submit, no renew, no source.
        assert!(requests.try_recv().is_err());
        let sent = c.store().identity_retired_notice(&remote, &boot).unwrap();
        assert!(sent.is_some());
        assert_eq!(
            *first_sent.get_or_insert(sent),
            sent,
            "the first one is kept"
        );
    }
}
