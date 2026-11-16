use super::*;
use crate::config::resolve::Loaded;
use crate::config::{Inputs as ConfigInputs, Overlay};
use crate::coordinator::InputGate;

fn config(text: &str) -> ResolvedConfig {
    let user = Loaded {
        path: "fixture".into(),
        value: crate::config::parse::parse_user("fixture", text).unwrap(),
    };
    crate::config::resolve(&ConfigInputs {
        overlay: &Overlay::default(),
        project: None,
        legacy: None,
        user: Some(&user),
    })
    .unwrap()
}

#[test]
fn local_only_startup_does_not_require_private_ssh_registration_config() {
    let root = crate::test_tmp::tempdir().unwrap();
    let env = BTreeMap::from([("XDG_CONFIG_HOME".into(), root.path().display().to_string())]);
    let dir = root.path().join("remote-task-runner");
    private_dir(&dir).unwrap();
    let path = dir.join("config.toml");
    let text = "schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\n";
    std::fs::write(&path, text).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let local = config(text);
    assert!(snapshot(&local, &env, &WorkerId::parse("local").unwrap())
        .unwrap()
        .workers
        .is_empty());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
}

#[test]
fn registration_is_private_bounded_and_snapshotted_from_user_only() {
    let root = crate::test_tmp::tempdir().unwrap();
    let env = BTreeMap::from([("XDG_CONFIG_HOME".into(), root.path().display().to_string())]);
    let dir = root.path().join("remote-task-runner");
    private_dir(&dir).unwrap();
    let path = dir.join("config.toml");
    let stale = config("schema_version=1\n[[workers]]\nid='stale'\nssh='stale-host'\n");
    assert!(snapshot(&stale, &env, &WorkerId::parse("local").unwrap())
        .unwrap()
        .workers
        .is_empty());
    let text = "schema_version=1\n[[workers]]\nid='remote-a'\nssh='peer-a'\npriority=12\n";
    std::fs::write(&path, text).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(snapshot(&stale, &env, &WorkerId::parse("local").unwrap()).is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let pinned = snapshot(&stale, &env, &WorkerId::parse("local").unwrap()).unwrap();
    assert_eq!(pinned.workers.len(), 1);
    assert_eq!(pinned.workers[0].id, "remote-a");
    assert_eq!(pinned.workers[0].ssh, "peer-a");
    assert_eq!(pinned.workers[0].priority.value, 12);
    std::fs::write(&path, "schema_version=1\n").unwrap();
    assert_eq!(pinned.workers.len(), 1);
    assert!(snapshot(&stale, &env, &WorkerId::parse("local").unwrap())
        .unwrap()
        .workers
        .is_empty());
    std::fs::write(
        &path,
        "schema_version=1\n[[workers]]\nid='ephemeral'\nssh='peer'\nephemeral=true\n",
    )
    .unwrap();
    assert!(snapshot(&stale, &env, &WorkerId::parse("local").unwrap())
        .unwrap_err()
        .to_string()
        .contains("unsupported"));
    std::fs::write(&path, " ".repeat(262145)).unwrap();
    assert!(snapshot(&stale, &env, &WorkerId::parse("local").unwrap()).is_err());
}

async fn resident(root: &Path) -> Resident {
    super::super::tests::delivery_resident(root).await
}

async fn connected(
    resident: &Resident,
    name: &str,
) -> (
    crate::protocol::session::Connected,
    tokio::task::JoinHandle<()>,
) {
    let (parent, peer) = crate::protocol::transport::duplex_pair(4096);
    let mut identity = crate::worker::serve::identity(&resident.agent).unwrap();
    identity.worker_id = WorkerId::parse(name).unwrap();
    let cluster = resident.client.cluster_id.clone();
    let server_cluster = cluster.clone();
    let task = tokio::spawn(async move {
        let _accepted = crate::protocol::session::accept(
            peer,
            crate::protocol::session::ServerConfig::new(server_cluster, identity),
        )
        .await
        .unwrap();
        std::future::pending::<()>().await;
    });
    let connected = connect(
        parent,
        ClientConfig::new(
            cluster,
            resident.coordinator.store().coordinator_id().clone(),
            resident.coordinator.epoch(),
        ),
    )
    .await
    .unwrap();
    (connected, task)
}

#[tokio::test]
async fn unregistered_and_wrong_identity_never_attach_and_old_cleanup_preserves_replacement() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = resident(root.path()).await;
    let config = config("schema_version=1\n[[workers]]\nid='remote-a'\nssh='peer-a'\n");
    let (wrong, wrong_peer) = connected(&resident, "remote-b").await;
    assert!(config
        .attach_gated_worker(
            &resident.coordinator,
            WorkerTarget::Remote("remote-a"),
            wrong,
            Arc::default()
        )
        .await
        .is_err());
    let (unknown, unknown_peer) = connected(&resident, "remote-a").await;
    assert!(config
        .attach_gated_worker(
            &resident.coordinator,
            WorkerTarget::Remote("unregistered"),
            unknown,
            Arc::default()
        )
        .await
        .is_err());
    let (first, first_peer) = connected(&resident, "remote-a").await;
    let gate = Arc::new(InputGate::default());
    let old = config
        .attach_gated_worker(
            &resident.coordinator,
            WorkerTarget::Remote("remote-a"),
            first,
            gate.clone(),
        )
        .await
        .unwrap();
    let id = WorkerId::parse("remote-a").unwrap();
    let generation = gate.generation();
    let (second, second_peer) = connected(&resident, "remote-a").await;
    let new_gate = Arc::new(InputGate::default());
    assert_ne!(generation, new_gate.generation());
    let new = config
        .attach_gated_worker(
            &resident.coordinator,
            WorkerTarget::Remote("remote-a"),
            second,
            new_gate,
        )
        .await
        .unwrap();
    gate.invalidate().await;
    assert_ne!(generation, gate.generation());
    assert!(!resident.coordinator.detach_link(&id, &old).await.unwrap());
    assert!(resident.coordinator.detach_link(&id, &new).await.unwrap());
    assert!(!resident.coordinator.detach_link(&id, &new).await.unwrap());
    for peer in [wrong_peer, unknown_peer, first_peer, second_peer] {
        peer.abort();
    }
}

#[tokio::test]
async fn stopped_handshake_reaps_only_its_owned_ssh_child() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = resident(root.path()).await;
    let config = config("schema_version=1\n[[workers]]\nid='remote-a'\nssh='peer-a'\n");
    let worker = config.workers[0].clone();
    let pidfile = root.path().join("owned.pid");
    let untouched = tokio::process::Command::new("/bin/sleep")
        .arg("60")
        .kill_on_drop(false)
        .spawn()
        .unwrap();
    let mut untouched = OwnedSsh::new(untouched);
    let (stop, rx) = watch::channel(false);
    let job = tokio::spawn(run_with_argv(
        Arc::new(config),
        worker,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "echo $$ > \"$1\"; exec /bin/sleep 60".into(),
            "fixture".into(),
            pidfile.display().to_string(),
        ],
        resident.coordinator.clone(),
        resident.captured.clone(),
        rx,
    ));
    let pid = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&pidfile) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(pid > 1);
    let _ = stop.send(true);
    tokio::time::timeout(Duration::from_secs(2), job)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::platform::process_start_identity(pid),
        crate::platform::StartIdentity::NoProcess
    );
    assert!(untouched
        .child
        .as_mut()
        .unwrap()
        .try_wait()
        .unwrap()
        .is_none());
    untouched.reap().await;
}

#[tokio::test]
async fn local_identity_conflict_is_rejected_before_resident_state_changes() {
    let root = crate::test_tmp::tempdir().unwrap();
    let env = BTreeMap::from([("XDG_CONFIG_HOME".into(), root.path().display().to_string())]);
    let dir = root.path().join("remote-task-runner");
    private_dir(&dir).unwrap();
    let text = "schema_version=1\n[[workers]]\nid='local'\nssh='remote-alias'\n";
    let path = dir.join("config.toml");
    std::fs::write(&path, text).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let before = std::fs::read_dir(root.path()).unwrap().count();
    let error = Resident::start(
        &config(text),
        Inputs {
            state_dir: root.path().join("parent-state"),
            worker: WorkerInputs {
                state_dir: root.path().join("worker-state"),
                worker_id: WorkerId::parse("local").unwrap(),
                boot_id: WorkerBootId::parse("boot").unwrap(),
                supervisor_exe: "/bin/false".into(),
                cluster_key_path: root.path().join("missing-key"),
                env,
            },
            collection_interval: Duration::from_millis(20),
            collection: Default::default(),
        },
    )
    .await
    .err()
    .expect("conflicting registration must fail");
    assert!(error.to_string().contains("conflicts with local worker"));
    assert!(!root.path().join("parent-state").exists());
    assert!(!root.path().join("worker-state").exists());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), before);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
}

#[tokio::test]
async fn stopping_before_invalidation_rejects_late_ready_and_finishes_nested_verify() {
    use crate::wire::{BlobVerifyResponse, Response, SchemaVersion};
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = resident(root.path()).await;
    let run = RunId::generate();
    let source = SourceId::generate();
    let digest = Digest::of_bytes(b"manifest");
    resident
        .coordinator
        .store()
        .put_source_ref(&source, &digest, 1)
        .unwrap();
    resident
        .coordinator
        .enqueue(
            run.clone(),
            source.clone(),
            vec![(
                TaskId::generate(),
                super::super::tests::delivery_spec(digest.clone(), false),
            )],
        )
        .await
        .unwrap();
    let (parent, peer) = crate::protocol::transport::duplex_pair(4096);
    let identity = crate::worker::serve::identity(&resident.agent).unwrap();
    let cluster = resident.client.cluster_id.clone();
    let server_cluster = cluster.clone();
    let (seen, received) = tokio::sync::oneshot::channel();
    let (release, late) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut accepted = crate::protocol::session::accept(
            peer,
            crate::protocol::session::ServerConfig::new(server_cluster, identity),
        )
        .await
        .unwrap();
        let inbound = accepted.incoming.next().await.unwrap();
        assert!(matches!(
            inbound.request,
            crate::wire::Request::BlobVerify(_)
        ));
        seen.send(()).unwrap();
        late.await.unwrap();
        let _ = inbound
            .responder
            .respond(Response::BlobVerify(BlobVerifyResponse {
                schema_version: SchemaVersion::CURRENT,
                ready: true,
                bundle_needed: false,
                have: vec![],
                present: vec![],
                missing: vec![],
                corrupt: vec![],
            }))
            .await;
    });
    let connected = connect(
        parent,
        ClientConfig::new(
            cluster,
            resident.coordinator.store().coordinator_id().clone(),
            resident.coordinator.epoch(),
        ),
    )
    .await
    .unwrap();
    let gate = Arc::new(InputGate::default());
    let old = gate.generation();
    let (cancel, rx) = watch::channel(false);
    let job = tokio::spawn(delivery::sources(
        resident.coordinator.clone(),
        resident.captured.clone(),
        connected.client,
        resident.worker_id.clone(),
        gate.clone(),
        rx,
    ));
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    let _ = cancel.send(true);
    gate.invalidate().await;
    // An in-flight Inspect from the old link may still reset boot metadata.
    gate.observe(WorkerBootId::parse("late-boot").unwrap(), AgentEpoch(2))
        .await;
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), job)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.await.unwrap();
    gate.publish(
        old,
        crate::coordinator::ReadyInput {
            run_id: run,
            source_id: source,
            manifest_digest: digest,
        },
    );
    assert!(gate.snapshot().is_empty());
    assert_ne!(old, gate.generation());
}

fn write_registration(root: &Path, text: &str) {
    let dir = root.join(".config/remote-task-runner");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.toml");
    std::fs::write(&path, text).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn a_reload_reads_the_registration_even_when_startup_had_no_remote_worker() {
    let root = crate::test_tmp::tempdir().unwrap();
    let env = BTreeMap::from([("HOME".into(), root.path().display().to_string())]);
    let local = WorkerId::parse("local").unwrap();
    let started = config("schema_version=1\n");
    assert!(snapshot(&started, &env, &local).unwrap().workers.is_empty());
    // Nothing registered yet: an empty reload is not an error.
    assert!(reload(&started, &env, &local).unwrap().workers.is_empty());
    write_registration(
        root.path(),
        "schema_version=1\n[[workers]]\nid='remote-a'\nssh='peer-a'\nbinary='/w/bin/current/remote-task-runner'\n",
    );
    let fresh = reload(&started, &env, &local).unwrap();
    assert_eq!(fresh.workers.len(), 1);
    assert_eq!(
        fresh.workers[0].binary.as_deref(),
        Some("/w/bin/current/remote-task-runner")
    );
    // The same refusals as startup.
    write_registration(
        root.path(),
        "schema_version=1\n[[workers]]\nid='local'\nssh='peer-a'\n",
    );
    assert!(reload(&started, &env, &local).is_err());
}

#[test]
fn the_bridge_runs_the_registered_absolute_binary_or_the_bare_name() {
    let c = config(
        "schema_version=1\n[[workers]]\nid='a'\nssh='h'\nbinary='/w/my dir/rtr'\n[[workers]]\nid='b'\nssh='h'\n",
    );
    let words = |w: &WorkerSpec| bridge_command(w).unwrap().shell_string();
    assert_eq!(words(&c.workers[0]), "'/w/my dir/rtr' 'worker' '--stdio'");
    assert_eq!(
        words(&c.workers[1]),
        "'remote-task-runner' 'worker' '--stdio'"
    );
}

#[tokio::test]
async fn a_worker_added_after_startup_is_dialed_without_a_restart() {
    let root = crate::test_tmp::tempdir().unwrap();
    let mut resident = resident(root.path()).await;
    let dialed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let record = dialed.clone();
    // No real ssh: the dial only records who it was asked to reach.
    resident.dial = Arc::new(move |config, worker, _, _, _| {
        assert!(config.workers.iter().any(|w| w.id == worker.id));
        record.lock().unwrap().push(worker.id.clone());
        Box::pin(async {})
    });
    let client = resident.client();
    let runner = tokio::spawn(resident.run());
    let reload = |client: Client| async move {
        match client.request(Operation::ReloadWorkers).await.unwrap() {
            Reply::WorkersReloaded { connecting } => connecting,
            other => panic!("{other:?}"),
        }
    };
    assert!(reload(client.clone()).await.is_empty());
    write_registration(
        root.path(),
        "schema_version=1\n[[workers]]\nid='remote-a'\nssh='peer-a'\n",
    );
    assert_eq!(reload(client.clone()).await, ["remote-a"]);
    // Asking again dials nothing twice.
    assert!(reload(client.clone()).await.is_empty());
    write_registration(
        root.path(),
        "schema_version=1\n[[workers]]\nid='remote-a'\nssh='peer-a'\n[[workers]]\nid='remote-b'\nssh='peer-b'\n",
    );
    assert_eq!(reload(client.clone()).await, ["remote-b"]);
    assert_eq!(*dialed.lock().unwrap(), ["remote-a", "remote-b"]);
    // An unusable registration is an answer, not a crash.
    write_registration(root.path(), "not toml [");
    match client.request(Operation::ReloadWorkers).await.unwrap() {
        Reply::ControlError { error } => assert!(error.message.contains("unusable")),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
