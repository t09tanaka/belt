use super::Operation;
use super::*;
use crate::coordinator::{LinkError, WorkerLink};
use crate::state::{Outcome, Receipt};
use crate::storage::{NewAttempt, Seen};
use crate::wire::*;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Slow {
    active: AttemptStatus,
    collecting: AtomicBool,
    offline: AtomicBool,
    renewals: AtomicUsize,
    append_bytes: bool,
}
impl WorkerLink for Slow {
    fn call(
        &self,
        request: crate::wire::Request,
    ) -> Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + '_>> {
        Box::pin(async move {
            if self.offline.load(Ordering::SeqCst) {
                return Err(LinkError::Unconfirmed);
            }
            match request {
                crate::wire::Request::Inspect(_) => Ok(Response::Inspect(InspectResponse {
                    preparation_observations: None,
                    capabilities: None,
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: WorkerId::parse("local").unwrap(),
                    worker_boot_id: self.active.worker_boot_id.clone(),
                    agent_epoch: AgentEpoch(1),
                    snapshot_digest: Digest::of_bytes(b"snapshot"),
                    policy_digest: Digest::of_bytes(b"policy"),
                    active_task: 1,
                    prefetch_pending: 0,
                    max_active_task: 2,
                    prefetch_per_worker: 1,
                    disk_free_bytes: 1,
                    draining: false,
                })),
                crate::wire::Request::GetAttempt(_) => {
                    Ok(Response::GetAttempt(self.active.clone()))
                }
                crate::wire::Request::RenewLease(r) => {
                    self.renewals.fetch_add(1, Ordering::SeqCst);
                    Ok(Response::RenewLease(RenewLeaseResponse {
                        schema_version: SchemaVersion::CURRENT,
                        attempt_id: r.attempt_id,
                        lease_seq: r.lease_seq,
                        extended: true,
                        lease_remaining_ms: r.lease_ms,
                        state: AttemptState::Active,
                    }))
                }
                crate::wire::Request::ReadLog(r) if self.append_bytes => {
                    self.collecting.store(true, Ordering::SeqCst);
                    Ok(Response::ReadLog(ReadLogResponse {
                        schema_version: SchemaVersion::CURRENT,
                        offset: r.offset,
                        data: Bytes(b"active".to_vec()),
                        next_offset: r.offset + 6,
                        eof: false,
                        log_truncated: false,
                        dropped_bytes: 0,
                    }))
                }
                crate::wire::Request::ReadLog(_) | crate::wire::Request::GetResult(_) => {
                    self.collecting.store(true, Ordering::SeqCst);
                    std::future::pending().await
                }
                _ => Err(LinkError::Unconfirmed),
            }
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_collection_does_not_block_lease_renewals_and_shutdown_preserves_unknown() {
    collection_shutdown_fixture(false, false, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drains_log_append_and_commit_before_releasing_owner() {
    collection_shutdown_fixture(true, false, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_pin_contention_keeps_resident_ipc_and_lease_renewal_alive() {
    collection_shutdown_fixture(false, true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn input_gc_blocking_page_keeps_ipc_renewals_and_owner_until_stop_drain() {
    collection_shutdown_fixture(false, false, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_gc_claim_that_cannot_be_deleted_keeps_the_resident_and_its_work_running() {
    collection_shutdown_fixture(false, false, false, true).await;
}

async fn collection_shutdown_fixture(
    append_bytes: bool,
    gc_contention: bool,
    input_pause: bool,
    gc_stuck: bool,
) {
    let root = crate::test_tmp::prefixed("rtr-clocks-").unwrap();
    let mut config = crate::config::resolve(&crate::config::Inputs {
        overlay: &crate::config::Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    config.heartbeat_seconds.value = 1;
    config.stale_seconds.value = 3;
    crate::secrets::key::init(&root.path().join("key")).unwrap();
    let state = root.path().join("parent");
    let mut resident = Resident::start(
        &config,
        Inputs {
            state_dir: state.clone(),
            worker: WorkerInputs {
                state_dir: root.path().join("worker"),
                worker_id: WorkerId::parse("local").unwrap(),
                boot_id: WorkerBootId::parse("boot-test").unwrap(),
                supervisor_exe: "/bin/false".into(),
                cluster_key_path: root.path().join("key"),
                env: BTreeMap::from([
                    ("PATH".into(), "/usr/bin:/bin".into()),
                    ("HOME".into(), root.path().display().to_string()),
                ]),
            },
            collection_interval: Duration::from_millis(10),
            collection: CollectConfig {
                budget_ms: 5000,
                ..Default::default()
            },
        },
    )
    .await
    .unwrap();
    let db = resident.coordinator.store();
    let run = RunId::generate();
    let source = SourceId::generate();
    let worker = WorkerId::parse("local").unwrap();
    let digest = Digest::of_bytes(b"manifest");
    db.put_source_ref(&source, &digest, 1).unwrap();
    db.create_run(&run, Some(&source), None, &serde_json::json!({}), 1)
        .unwrap();
    let mut active = None;
    for terminal in [false, true] {
        // The append-drain case needs only the active attempt. A terminal
        // GetResult would stall the next live-log pass if its first budget
        // elapsed before append, obscuring the write this case must observe.
        if append_bytes && terminal {
            continue;
        }
        let task = TaskId::generate();
        let attempt = AttemptId::generate();
        let payload = AttemptPayload {
            ci_task: None,
            toolchains: None,
            outputs: Vec::new(),
            requirements: Default::default(),
            in_place: false,
            setup: None,
            run_id: run.clone(),
            task_id: task.clone(),
            generation: Generation(1),
            source_id: source.clone(),
            plan_digest: digest.clone(),
            manifest_digest: digest.clone(),
            permit_id: None,
            commands: vec![CommandSpec {
                setup_step: None,
                node_id: NodeId::parse("main").unwrap(),
                argv: vec!["/usr/bin/true".into()],
                cwd: String::new(),
                env: BTreeMap::new(),
                depends_on: vec![],
                timeout_ms: None,
            }],
            lease_ms: 60000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: vec![],
        };
        db.create_task(&task, &run, &serde_json::json!({}), 1)
            .unwrap();
        db.create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task.clone(),
                worker_id: worker.clone(),
                payload_hash: Digest::of_bytes(b"payload"),
                permit_id: None,
            },
            1,
        )
        .unwrap();
        let placement = db
            .record_placement(
                &task,
                &worker,
                Some(&attempt),
                &serde_json::json!({"payload":payload}),
                1,
            )
            .unwrap();
        db.ack_placement(placement, 1).unwrap();
        let state = if terminal {
            AttemptState::Terminal
        } else {
            AttemptState::Active
        };
        let termination =
            terminal.then(|| Termination::new(Outcome::Succeeded, None, None).unwrap());
        let mut receipts = Receipts::default();
        receipts.mark(Receipt::WorkerAccepted, 1);
        db.record_seen(
            &attempt,
            true,
            Some(Seen {
                state,
                termination,
                receipts,
            }),
            Observation::Known,
            1,
        )
        .unwrap();
        if !terminal {
            active = Some(AttemptStatus {
                schema_version: SchemaVersion::CURRENT,
                attempt_id: attempt,
                run_id: run.clone(),
                task_id: task,
                generation: Generation(1),
                worker_boot_id: WorkerBootId::parse("boot-test").unwrap(),
                agent_epoch: AgentEpoch(1),
                state,
                receipts,
                spawned_at_ms: Some(1),
                observation: Observation::Known,
                termination: None,
                nodes: vec![],
                steps: vec![],
                retired: false,
                log_truncated: false,
                log_dropped_bytes: 0,
            });
        }
    }
    let slow = Arc::new(Slow {
        active: active.unwrap(),
        collecting: AtomicBool::new(false),
        offline: AtomicBool::new(false),
        renewals: AtomicUsize::new(0),
        append_bytes,
    });
    // Replace only this test's local protocol server with a controlled link.
    resident.worker_connection.abort();
    resident.worker_connection = tokio::spawn(std::future::pending());
    resident.coordinator.attach(worker, slow.clone()).await;
    let append_pause = append_bytes.then(|| resident.files.pause_next_log_append());
    let input_pause = input_pause.then(|| {
        resident.captured.retention_test.fast();
        resident.captured.sources.pause_retention_page()
    });
    // Two runs have a durable deletion claim, which the loop replays even
    // while its clock is quarantined. One can never be finished: its attempt
    // directory became a link, which deletion does not follow.
    let stuck = gc_stuck.then(|| {
        let expired =
            || result_retention::tests::claimed(resident.coordinator.store(), &resident.files).1;
        let directory = |attempt: &AttemptId| state.join("results").join(attempt.as_str());
        let (stuck, other) = (directory(&expired()), directory(&expired()));
        let outside = root.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("kept"), b"outside").unwrap();
        std::fs::rename(&stuck, root.path().join("moved")).unwrap();
        std::os::unix::fs::symlink(&outside, &stuck).unwrap();
        resident.captured.retention_test.fast();
        (stuck, other, outside)
    });
    let input_sources = resident.captured.sources.clone();
    let client = resident.client();
    let guard =
        gc_contention.then(|| crate::test_tmp::gc_guard(resident.coordinator.store(), &run));
    let mut runner = tokio::spawn(resident.run());
    if let Some((entered, _)) = &input_pause {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if entered.try_recv().is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    if let Some((_, other, _)) = &stuck {
        // The other run is collected only after the stuck one has failed at
        // least one page or was passed by the cursor; both must keep going.
        tokio::time::timeout(Duration::from_secs(20), async {
            while other.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("result GC of the other run stopped");
    }
    if gc_contention || input_pause.is_some() || gc_stuck {
        tokio::time::timeout(Duration::from_secs(4), async {
            while slow.renewals.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!runner.is_finished(), "GC contention stopped the resident");
        if gc_contention {
            assert!(
                !slow.collecting.load(Ordering::SeqCst),
                "collection bypassed the exclusive pin"
            );
        }
        tokio::time::timeout(
            Duration::from_secs(2),
            client.query(crate::service::query::Query::Run {
                run_id: run.clone(),
            }),
        )
        .await
        .unwrap()
        .unwrap();
    }
    drop(guard);
    // The next pass resumes collection after the exclusive guard is released.
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut append_entered = append_pause.is_none();
        loop {
            // GetResult also marks collecting, and a live-log pass can exhaust
            // its budget before reading logs. Observe the actual append before
            // taking the link offline; shutdown must drain this exact write.
            if let Some((entered, _)) = &append_pause {
                if !append_entered {
                    append_entered = match entered.try_recv() {
                        Ok(()) => true,
                        Err(std::sync::mpsc::TryRecvError::Empty) => false,
                        Err(e) => panic!("append pause disconnected: {e}"),
                    };
                }
            }
            if append_entered
                && slow.collecting.load(Ordering::SeqCst)
                && slow.renewals.load(Ordering::SeqCst) >= 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    slow.offline.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let record = CoordinatorStore::open_read_only(&state)
                .unwrap()
                .get_attempt(&slow.active.attempt_id)
                .unwrap()
                .unwrap();
            if record.observation == Observation::Unknown {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let spec = crate::service::bootstrap::Spec {
        state_dir: state.clone(),
        worker_state_dir: root.path().join("worker"),
        worker_id: WorkerId::parse("local").unwrap(),
        cluster_key_path: root.path().join("key"),
        // Never started: the live owner answers. A private name instead of a
        // system tool, whose path is a symlink on some hosts and is refused.
        executable: root.path().join("owner-is-already-running"),
        env: BTreeMap::from([
            ("HOME".into(), root.path().display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
        ]),
        repo_root: None,
        expected_cluster: Some(client.cluster_id().clone()),
        managed_service: None,
    };
    // The spec itself must reach the live owner; a refused spec would only
    // show below as an unacknowledged cancellation.
    crate::service::bootstrap::ensure(spec.clone())
        .await
        .expect("spec reaches the live owner");
    let (_sender, mut signals) = tokio::sync::mpsc::unbounded_channel();
    let target = crate::service::completion::Target::Run(run.clone());
    let pending = crate::service::control::cancel_and_stop(
        &spec,
        target,
        &mut signals,
        Duration::from_millis(500),
    )
    .await;
    assert_eq!(pending.cli_exit_code, 77);
    assert!(pending.cancel_requested);
    assert!(pending.cancel_acknowledged);
    assert_eq!(pending.state, "cancel_requested");
    assert!(!pending.snapshot.unwrap().stop_confirmed());
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    if let Some((_, resume)) = append_pause.or(input_pause) {
        let still_running = tokio::time::timeout(Duration::from_millis(100), &mut runner)
            .await
            .is_err();
        let owner_retained = matches!(
            CoordinatorStore::open(&state, 1),
            Err(crate::storage::StorageError::Locked(_))
        );
        if !append_bytes
            && (input_sources.mutation_guard().is_ok()
                || crate::secrets::vault::Vault::open(&state).is_ok())
        {
            resume.send(()).unwrap();
            panic!("source/Vault EX released before blocking retention page drained");
        }
        resume.send(()).unwrap();
        assert!(
            still_running,
            "shutdown abandoned filesystem work in flight"
        );
        assert!(
            owner_retained,
            "owner lock released before filesystem IO drained"
        );
    }
    tokio::time::timeout(Duration::from_secs(7), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if let Some((stuck, _, outside)) = &stuck {
        // The resident ended by the shutdown above, not by the failing page,
        // and the claim's data is where it was.
        assert!(std::fs::symlink_metadata(stuck)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(outside.join("kept")).unwrap(), b"outside");
        assert_eq!(
            std::fs::read(root.path().join("moved/logs/stdout.log")).unwrap(),
            b"retained"
        );
    }
    let record = CoordinatorStore::open_read_only(&state)
        .unwrap()
        .get_attempt(&slow.active.attempt_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.observation, Observation::Unknown);
    assert_eq!(record.state, AttemptState::Active);
    assert!(record.termination.is_none());
    assert!(record.cancel_requested_at_ms.is_some());
    if append_bytes {
        let progress = CoordinatorStore::open_read_only(&state)
            .unwrap()
            .log_progress(&slow.active.attempt_id, LogStream::Stdout)
            .unwrap()
            .unwrap();
        assert_eq!(progress.committed_bytes, 6);
        assert_eq!(
            std::fs::read(
                state
                    .join("results")
                    .join(slow.active.attempt_id.as_str())
                    .join("logs/stdout.log")
            )
            .unwrap(),
            b"active"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_acceptance_survives_stalled_source_and_shutdown() {
    let root = crate::test_tmp::prefixed("rtr-stop-source-").unwrap();
    crate::secrets::key::init(&root.path().join("key")).unwrap();
    let config = crate::config::resolve(&crate::config::Inputs {
        overlay: &crate::config::Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    let state = root.path().join("parent");
    let inputs = || Inputs {
        state_dir: state.clone(),
        worker: WorkerInputs {
            state_dir: root.path().join("worker"),
            worker_id: WorkerId::parse("local").unwrap(),
            boot_id: WorkerBootId::parse("boot-stop-test").unwrap(),
            supervisor_exe: "/bin/false".into(),
            cluster_key_path: root.path().join("key"),
            env: BTreeMap::from([
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), root.path().display().to_string()),
            ]),
        },
        collection_interval: Duration::from_millis(20),
        collection: Default::default(),
    };
    let mut resident = Resident::start(&config, inputs()).await.unwrap();
    let client = resident.client();
    let captured = resident.captured.clone();
    let held_transfer = captured.transfer.clone().lock_owned().await;
    let directory = root.path().join("input");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("data"), "fixed input").unwrap();
    let operation = || {
        let run = RunId::generate();
        let source = crate::source::capture_for_run(
            &captured.env,
            &captured.sources,
            &run,
            &crate::source::SourceRequest::Directory {
                dir: directory.clone(),
            },
            &Default::default(),
        )
        .unwrap();
        let digest = Digest::parse(&source.manifest.manifest_digest)
            .or_else(|_| Digest::parse(&format!("sha256:{}", source.manifest.manifest_digest)))
            .unwrap();
        Operation::SubmitCaptured {
            run_id: run,
            source_id: source.manifest.source_id,
            tasks: vec![(
                TaskId::generate(),
                TaskSpec {
                    ci: None,
                    toolchains: None,
                    outputs: Vec::new(),
                    requirements: Default::default(),
                    in_place: false,
                    setup: None,
                    plan_digest: Digest::of_bytes(b"plan"),
                    manifest_digest: digest,
                    commands: vec![CommandSpec {
                        node_id: NodeId::parse("main").unwrap(),
                        argv: vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
                        cwd: String::new(),
                        env: Default::default(),
                        depends_on: vec![],
                        timeout_ms: None,
                        setup_step: None,
                    }],
                    command_timeout_ms: None,
                    deadline_ms: None,
                    resource_wait_timeout_ms: None,
                    has_secret_bundle: false,
                    artifacts: vec![],
                },
            )],
        }
    };
    let first = operation();
    let second = operation();
    // Stall only source delivery; real coordinator control keeps its original link.
    let (parent, worker) = UnixStream::pair().unwrap();
    let identity = crate::worker::serve::identity(&resident.agent).unwrap();
    let cluster = client.cluster_id().clone();
    let server_cluster = cluster.clone();
    let (seen, received) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut accepted = crate::protocol::session::accept(
            Io::from_stream(worker),
            crate::protocol::session::ServerConfig::new(server_cluster, identity),
        )
        .await
        .unwrap();
        loop {
            let inbound = accepted.incoming.next().await.unwrap();
            if matches!(inbound.request, crate::wire::Request::BlobVerify(_)) {
                inbound
                    .responder
                    .respond(Response::BlobVerify(BlobVerifyResponse {
                        schema_version: SchemaVersion::CURRENT,
                        ready: false,
                        bundle_needed: false,
                        have: vec![],
                        present: vec![],
                        missing: vec![],
                        corrupt: vec![],
                    }))
                    .await
                    .unwrap();
            } else {
                assert!(matches!(inbound.request, crate::wire::Request::BlobPut(_)));
                let _ = seen.send(());
                std::future::pending::<()>().await;
                drop(inbound);
                break;
            }
        }
    });
    resident.worker_client = connect(
        Io::from_stream(parent),
        ClientConfig::new(
            cluster,
            resident.coordinator.store().coordinator_id().clone(),
            resident.coordinator.epoch(),
        ),
    )
    .await
    .unwrap()
    .client;
    let (gc_entered, gc_resume) = captured.sources.pause_retention_page();
    let gc_sources = captured.sources.clone();
    let gc_owner = resident.coordinator.clone();
    // One real GC page, rather than repeatedly colliding a 10ms test cadence
    // with the independent source-delivery timer.
    let gc = tokio::task::spawn_blocking(move || {
        gc_sources.retention_page(
            gc_owner.store(),
            &mut Default::default(),
            wall_ms(),
            crate::platform::clock::monotonic_ms(),
            crate::platform::boot_id(),
            crate::results::plan::retention_ms(7),
        )
    });
    let owner = tokio::spawn(resident.run());
    tokio::task::spawn_blocking(move || gc_entered.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .unwrap();
    let waits_before = captured.sources.foreground_wait_count();
    let busy_client = client.clone();
    let busy_input: Operation =
        serde_json::from_value(serde_json::to_value(&first).unwrap()).unwrap();
    let busy = tokio::spawn(async move { busy_client.request(busy_input).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while captured.sources.foreground_wait_count() == waits_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        client.request(Operation::Ping).await.unwrap(),
        Reply::Ready { .. }
    ));
    let busy_reply = tokio::time::timeout(Duration::from_secs(3), busy)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(&busy_reply, Reply::ControlError { error } if error.code == crate::error::ErrorCode::SourceBusy),
        "actual busy reply: {busy_reply:?}"
    );
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.request(first).await });
    let second_client = client.clone();
    let second = tokio::spawn(async move { second_client.request(second).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while captured.sources.foreground_wait_count() < waits_before + 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!first.is_finished() && !second.is_finished());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), client.request(Operation::Ping))
            .await
            .unwrap()
            .unwrap(),
        Reply::Ready { .. }
    ));
    gc_resume.send(()).unwrap();
    gc.await.unwrap().unwrap();
    for task in [first, second] {
        let reply = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(reply, Reply::Accepted { .. }),
            "actual acceptance reply: {reply:?}"
        );
    }
    drop(held_transfer);
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(2), owner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The source peer remains blocked. Reacquiring the non-waiting writer
    // proves source IO does not retain the stopped coordinator owner.
    assert!(!peer.is_finished());
    let store = CoordinatorStore::open(&state, wall_ms()).unwrap();
    // Durable acceptance survives while source provisioning remains unready.
    assert_eq!(store.queued_tasks(10).unwrap().len(), 2);
    assert!(store.list_nonterminal_attempts().unwrap().is_empty());
    drop(store);
    let guard = tokio::time::timeout(
        Duration::from_secs(2),
        captured.transfer.clone().lock_owned(),
    )
    .await
    .unwrap();
    let transfer_file =
        super::super::resident::secure_file(&captured.sources.root().join("transfer.lock"), true)
            .unwrap();
    transfer_file.try_lock().unwrap();
    drop(transfer_file);
    drop(guard);
    let restart = tokio::time::timeout(Duration::from_secs(20), Resident::start(&config, inputs()))
        .await
        .unwrap()
        .unwrap();
    let restarted = restart.client();
    let owner = tokio::spawn(restart.run());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let store = CoordinatorStore::open_read_only(&state).unwrap();
            if store.queued_tasks(10).unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        restarted.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(2), owner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // The canceled transfer also drains its source lease and cross-process file lock.
    let guard = tokio::time::timeout(
        Duration::from_secs(2),
        captured.transfer.clone().lock_owned(),
    )
    .await
    .unwrap();
    let file =
        super::super::resident::secure_file(&captured.sources.root().join("transfer.lock"), true)
            .unwrap();
    file.try_lock().unwrap();
    drop(guard);
    peer.abort();
    let _ = peer.await;
}

pub(super) async fn delivery_resident(root: &Path) -> Resident {
    delivery_resident_on(root, "/usr/bin:/bin").await
}
/// `path` is the tool search path of the worker profile. Every worker
/// inspection measures the native tools found there, one bounded subprocess
/// each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_starts_on_a_registration_marker_left_with_two_names_and_repairs_it() {
    let root = crate::test_tmp::tempdir().unwrap();
    let secrets = root.path().join("parent/secrets");
    let marker = secrets.join("parent-retention.json");
    let launch = |resident: Resident| async move {
        let client = resident.client();
        let runner = tokio::spawn(resident.run());
        (client, runner)
    };
    let wait = |what: &'static str, done: Box<dyn Fn() -> bool + Send>| async move {
        tokio::time::timeout(Duration::from_secs(20), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out: {what}"));
    };
    // A first life registers the parent Vault: pin first, then the marker.
    let resident = delivery_resident_on(root.path(), "/usr/bin:/bin").await;
    resident.captured.retention_test.fast();
    let (client, runner) = launch(resident).await;
    let published = marker.clone();
    wait(
        "the marker is published",
        Box::new(move || published.exists()),
    )
    .await;
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    runner.await.unwrap().unwrap();
    // As if that registration had died between its link and its unlink.
    let orphan = secrets.join(format!(".parent-retention.json.{}.tmp", ulid::Ulid::new()));
    std::fs::hard_link(&marker, &orphan).unwrap();
    let bytes = std::fs::read(&marker).unwrap();
    // The resident and the command-line input path both open this Vault.
    assert!(crate::secrets::vault::Vault::open(&root.path().join("parent")).is_ok());
    let resident = delivery_resident_on(root.path(), "/usr/bin:/bin").await;
    // Starting read the marker and removed nothing.
    assert!(orphan.exists());
    assert_eq!(std::fs::metadata(&marker).unwrap().nlink(), 2);
    resident.captured.retention_test.fast();
    let (client, runner) = launch(resident).await;
    let leftover = orphan.clone();
    wait(
        "the Vault page removes the temporary name",
        Box::new(move || !leftover.exists()),
    )
    .await;
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    runner.await.unwrap().unwrap();
    assert_eq!(std::fs::metadata(&marker).unwrap().nlink(), 1);
    assert_eq!(std::fs::read(&marker).unwrap(), bytes);
    let mut names: Vec<_> = std::fs::read_dir(&secrets)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["parent-retention.json", "retention.lock"]);
}
async fn delivery_resident_on(root: &Path, path: &str) -> Resident {
    crate::secrets::key::init(&root.join("key")).unwrap();
    let config = crate::config::resolve(&crate::config::Inputs {
        overlay: &crate::config::Overlay::default(),
        project: None,
        legacy: None,
        user: None,
    })
    .unwrap();
    Resident::start(
        &config,
        Inputs {
            state_dir: root.join("parent"),
            worker: WorkerInputs {
                state_dir: root.join("worker"),
                worker_id: WorkerId::parse("local").unwrap(),
                boot_id: WorkerBootId::parse("boot-delivery").unwrap(),
                supervisor_exe: "/bin/false".into(),
                cluster_key_path: root.join("key"),
                env: BTreeMap::from([
                    ("PATH".into(), path.into()),
                    ("HOME".into(), root.display().to_string()),
                ]),
            },
            collection_interval: Duration::from_millis(20),
            collection: Default::default(),
        },
    )
    .await
    .unwrap()
}
pub(super) fn delivery_spec(digest: Digest, secrets: bool) -> TaskSpec {
    TaskSpec {
        ci: None,
        toolchains: None,
        outputs: Vec::new(),
        requirements: Default::default(),
        in_place: false,
        setup: None,
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: digest,
        commands: vec![CommandSpec {
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/true".into()],
            cwd: String::new(),
            env: Default::default(),
            depends_on: vec![],
            timeout_ms: None,
            setup_step: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: secrets,
        artifacts: vec![],
    }
}
async fn secret_peer(
    resident: &Resident,
    id: WorkerId,
    blocked: bool,
) -> (
    crate::protocol::Client,
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<SecretBundlePutRequest>,
) {
    let (parent, worker) = UnixStream::pair().unwrap();
    let mut identity = crate::worker::serve::identity(&resident.agent).unwrap();
    identity.worker_id = id;
    let cluster = resident.client.cluster_id.clone();
    let server_cluster = cluster.clone();
    let (sent, received) = tokio::sync::mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let mut accepted = crate::protocol::session::accept(
            Io::from_stream(worker),
            crate::protocol::session::ServerConfig::new(server_cluster, identity),
        )
        .await
        .unwrap();
        while let Some(inbound) = accepted.incoming.next().await {
            let crate::wire::Request::SecretBundlePut(request) = inbound.request else {
                panic!("unexpected operation");
            };
            let response = SecretBundlePutResponse {
                schema_version: SchemaVersion::CURRENT,
                attempt_id: request.attempt_id.clone(),
                verified: true,
                placed: request.files.len() as u32,
            };
            sent.send(request).unwrap();
            if blocked {
                std::future::pending::<()>().await;
            } else {
                inbound
                    .responder
                    .respond(Response::SecretBundlePut(response))
                    .await
                    .unwrap();
            }
        }
    });
    let client = connect(
        Io::from_stream(parent),
        ClientConfig::new(
            cluster,
            resident.coordinator.store().coordinator_id().clone(),
            resident.coordinator.epoch(),
        ),
    )
    .await
    .unwrap()
    .client;
    (client, peer, received)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_worker_secret_jobs_route_exact_attempts_and_resend_after_session_reset() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    let mut expected = Vec::new();
    for (name, text) in [
        ("worker-a", "TOKEN=first-value\n"),
        ("worker-b", "TOKEN=second-value\n"),
    ] {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join(".env"), text).unwrap();
        std::fs::write(directory.join("ordinary"), name).unwrap();
        let run = RunId::generate();
        let capture = &resident.captured;
        let captured = crate::secrets::capture::capture_run(
            &capture.env,
            &capture.sources,
            &capture.vault,
            &crate::secrets::capture::SecretInputs {
                key: &capture.key,
                declared: &[local_ci::source::SecretSpec {
                    glob: ".env".into(),
                    required: true,
                }],
            },
            &run,
            &crate::source::SourceRequest::Directory { dir: directory },
            &Default::default(),
        )
        .unwrap();
        let source = captured.manifest.source_id;
        let digest = Digest::parse(&captured.manifest.manifest_digest)
            .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
            .unwrap();
        let task = TaskId::generate();
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
                vec![(task.clone(), delivery_spec(digest.clone(), true))],
            )
            .await
            .unwrap();
        let worker = WorkerId::parse(name).unwrap();
        let attempt = AttemptId::generate();
        let db = resident.coordinator.store();
        db.create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task.clone(),
                worker_id: worker.clone(),
                payload_hash: Digest::of_bytes(name.as_bytes()),
                permit_id: None,
            },
            1,
        )
        .unwrap();
        let payload = AttemptPayload {
            ci_task: None,
            toolchains: None,
            outputs: Vec::new(),
            requirements: Default::default(),
            in_place: false,
            setup: None,
            run_id: run,
            task_id: task.clone(),
            generation: Generation(1),
            source_id: source,
            plan_digest: Digest::of_bytes(b"plan"),
            manifest_digest: digest,
            permit_id: None,
            commands: vec![],
            lease_ms: 60000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: true,
            artifacts: vec![],
        };
        db.record_placement(
            &task,
            &worker,
            Some(&attempt),
            &serde_json::json!({"payload":payload}),
            1,
        )
        .unwrap();
        expected.push((worker, attempt, text.as_bytes().to_vec()));
    }
    let (a, peer_a, mut seen_a) = secret_peer(&resident, expected[0].0.clone(), true).await;
    let (b, peer_b, mut seen_b) = secret_peer(&resident, expected[1].0.clone(), false).await;
    let (stop, rx) = watch::channel(false);
    let gate_a = Arc::new(crate::coordinator::InputGate::default());
    let gate_b = Arc::new(crate::coordinator::InputGate::default());
    let job_a = tokio::spawn(delivery::secrets(
        resident.coordinator.clone(),
        resident.captured.clone(),
        a,
        expected[0].0.clone(),
        gate_a,
        rx.clone(),
    ));
    let job_b = tokio::spawn(delivery::secrets(
        resident.coordinator.clone(),
        resident.captured.clone(),
        b,
        expected[1].0.clone(),
        gate_b.clone(),
        rx,
    ));
    let first_a = tokio::time::timeout(Duration::from_secs(2), seen_a.recv())
        .await
        .unwrap()
        .unwrap();
    let first_b = tokio::time::timeout(Duration::from_secs(2), seen_b.recv())
        .await
        .unwrap()
        .unwrap();
    for (request, (_, attempt, text)) in [(first_a, &expected[0]), (first_b, &expected[1])] {
        assert_eq!(&request.attempt_id, attempt);
        assert_eq!(request.generation, Generation(1));
        assert_eq!(request.files.len(), 1);
        assert_eq!(&request.files[0].content.0, text);
    }
    gate_b
        .observe(WorkerBootId::parse("reset-boot").unwrap(), AgentEpoch(9))
        .await;
    let resent = tokio::time::timeout(Duration::from_secs(2), seen_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resent.attempt_id, expected[1].1);
    assert_eq!(resent.generation, Generation(1));
    stop.send(true).unwrap();
    job_a.await.unwrap().unwrap();
    job_b.await.unwrap().unwrap();
    peer_a.abort();
    peer_b.abort();
    let _ = peer_a.await;
    let _ = peer_b.await;
}

#[tokio::test]
async fn captured_submit_losing_admission_race_is_rejected_without_queue_mutation() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    let (mut client, server) = UnixStream::pair().unwrap();
    let (stop, _) = watch::channel(true);
    let job = tokio::spawn(handle(
        server,
        resident.coordinator.clone(),
        resident.sources.clone(),
        resident.captured.clone(),
        resident.worker_client.clone(),
        resident.worker_id.clone(),
        resident.client.clone(),
        Arc::new(Mutex::new(true)),
        stop,
        mpsc::channel(1).0,
    ));
    write_message(
        &mut client,
        &super::Request {
            schema_version: 2,
            cluster_id: resident.client.cluster_id.clone(),
            owner_token: resident.client.owner_token.clone(),
            operation: Operation::SubmitCaptured {
                run_id: RunId::generate(),
                source_id: SourceId::generate(),
                tasks: vec![(
                    TaskId::generate(),
                    delivery_spec(Digest::of_bytes(b"manifest"), false),
                )],
            },
        },
    )
    .await
    .unwrap();
    let reply: Reply = read_message(&mut client).await.unwrap();
    assert!(matches!(reply,Reply::Error {message} if message.contains("stopping")));
    job.await.unwrap().unwrap();
    assert!(resident
        .coordinator
        .store()
        .queued_tasks(1)
        .unwrap()
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_prefetch_verifies_legacy_inputs_and_advances_after_full_cache_evaluation() {
    let root = crate::test_tmp::tempdir().unwrap();
    let resident = delivery_resident(root.path()).await;
    let directory = root.path().join("input");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("data"), "snapshot").unwrap();
    let first_run = RunId::generate();
    let captured = crate::source::capture_for_run(
        &resident.captured.env,
        &resident.sources,
        &first_run,
        &crate::source::SourceRequest::Directory { dir: directory },
        &Default::default(),
    )
    .unwrap();
    let source = captured.manifest.source_id;
    let digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .unwrap();
    resident
        .coordinator
        .store()
        .put_source_ref(&source, &digest, 1)
        .unwrap();
    let mut runs = vec![first_run];
    runs.extend((0..64).map(|_| RunId::generate()));
    for run in &runs {
        resident
            .coordinator
            .enqueue(
                run.clone(),
                source.clone(),
                vec![(TaskId::generate(), delivery_spec(digest.clone(), false))],
            )
            .await
            .unwrap();
    }
    let (parent, worker) = UnixStream::pair().unwrap();
    let identity = crate::worker::serve::identity(&resident.agent).unwrap();
    let cluster = resident.client.cluster_id.clone();
    let server_cluster = cluster.clone();
    let ready = Arc::new(AtomicBool::new(false));
    let available = ready.clone();
    let (seen, mut received) = tokio::sync::mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let mut accepted = crate::protocol::session::accept(
            Io::from_stream(worker),
            crate::protocol::session::ServerConfig::new(server_cluster, identity),
        )
        .await
        .unwrap();
        while let Some(inbound) = accepted.incoming.next().await {
            assert!(matches!(
                inbound.request,
                crate::wire::Request::BlobVerify(_)
            ));
            seen.send(()).unwrap();
            inbound
                .responder
                .respond(Response::BlobVerify(BlobVerifyResponse {
                    schema_version: SchemaVersion::CURRENT,
                    ready: available.load(Ordering::SeqCst),
                    bundle_needed: false,
                    have: vec![],
                    present: vec![],
                    missing: vec![],
                    corrupt: vec![],
                }))
                .await
                .unwrap();
        }
    });
    let client = connect(
        Io::from_stream(parent),
        ClientConfig::new(
            cluster,
            resident.coordinator.store().coordinator_id().clone(),
            resident.coordinator.epoch(),
        ),
    )
    .await
    .unwrap()
    .client;
    let gate = Arc::new(crate::coordinator::InputGate::default());
    let (stop, rx) = watch::channel(false);
    let job = tokio::spawn(delivery::sources(
        resident.coordinator.clone(),
        resident.captured.clone(),
        client,
        resident.worker_id.clone(),
        gate.clone(),
        rx,
    ));
    tokio::time::timeout(Duration::from_secs(2), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        gate.snapshot().is_empty(),
        "manifest existence cannot grant readiness"
    );
    ready.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), async {
        while gate.snapshot().len() != 64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let cached = gate.snapshot();
    let deferred = runs
        .iter()
        .find(|run| !cached.iter().any(|i| &i.run_id == *run))
        .unwrap()
        .clone();
    assert!(!gate.evict_evaluated());
    let generation = gate.generation();
    for input in gate.snapshot() {
        gate.evaluated(generation, &input.run_id);
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while !gate.snapshot().iter().any(|i| i.run_id == deferred) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(gate.snapshot().len() <= 64);
    stop.send(true).unwrap();
    job.await.unwrap().unwrap();
    peer.abort();
    let _ = peer.await;
}

#[test]
fn retry_controls_require_revision_and_private_ipc_version() {
    let target = super::super::completion::Target::Run(RunId::generate());
    let encoded = serde_json::to_value(Operation::Cancel {
        request_id: RunId::generate().to_string(),
        target: target.clone(),
        expected_control_revision: 7,
    })
    .unwrap();
    assert_eq!(encoded["expected_control_revision"], 7);
    let mut missing_id = encoded.clone();
    missing_id.as_object_mut().unwrap().remove("request_id");
    assert!(serde_json::from_value::<Operation>(missing_id).is_err());
    let mut missing = encoded;
    missing
        .as_object_mut()
        .unwrap()
        .remove("expected_control_revision");
    assert!(serde_json::from_value::<Operation>(missing).is_err());
    let retry = Operation::Retry {
        target,
        expected_control_revision: 8,
        request_id: "same-request".into(),
    };
    let mut encoded = serde_json::to_value(retry).unwrap();
    assert_eq!(encoded["expected_control_revision"], 8);
    encoded.as_object_mut().unwrap().remove("request_id");
    assert!(serde_json::from_value::<Operation>(encoded).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_preflight_keeps_admission_and_db_available_and_rechecks_stopping() {
    use crate::service::completion::Target;
    for stopping in [false, true] {
        let (_root, coordinator, run) = retry_owner();
        let admission = Arc::new(Mutex::new(false));
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let (_stop, stopping_rx) = watch::channel(false);
        let pending = tokio::spawn(retry_control(
            coordinator.clone(),
            admission.clone(),
            RetryCommand {
                target: Target::Run(run.clone()),
                expected: 0,
                request_id: "slow-input".into(),
            },
            Arc::new(()),
            stopping_rx,
            move |_| {
                entered.send(()).unwrap();
                resume.recv().unwrap();
                Ok(false)
            },
        ));
        tokio::time::timeout(Duration::from_secs(5), observed)
            .await
            .unwrap()
            .unwrap();
        let other = RunId::generate();
        let owner = coordinator.clone();
        let target = other.clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut gate = admission.lock().await;
            tokio::task::spawn_blocking(move || {
                owner.store().cancel_control(
                    Target::Run(target),
                    0,
                    RunId::generate().to_string(),
                    5,
                )
            })
            .await
            .unwrap()
            .unwrap();
            *gate = stopping;
        })
        .await
        .unwrap();
        assert!(
            coordinator
                .store()
                .completion(Target::Run(other))
                .unwrap()
                .cancel_requested
        );
        release.send(()).unwrap();
        let reply = pending.await.unwrap().unwrap();
        if stopping {
            assert!(matches!(reply, Reply::ControlError { .. }));
            assert_eq!(
                coordinator
                    .store()
                    .completion(Target::Run(run))
                    .unwrap()
                    .control_revision,
                0
            );
        } else {
            assert!(matches!(reply, Reply::RetryAccepted { .. }), "{reply:?}");
            assert_eq!(
                coordinator
                    .store()
                    .completion(Target::Run(run))
                    .unwrap()
                    .control_revision,
                1
            );
        }
    }
}

fn retry_owner() -> (tempfile::TempDir, Arc<Coordinator>, RunId) {
    use crate::coordinator::CoordinatorConfig;
    use crate::state::Exit;
    let root = crate::test_tmp::prefixed("rtr-retry-").unwrap();
    let store = CoordinatorStore::open(root.path(), 1).unwrap();
    let run = RunId::generate();
    let source = SourceId::generate();
    let task = TaskId::generate();
    let digest = Digest::of_bytes(b"retained");
    store.put_source_ref(&source, &digest, 1).unwrap();
    store
        .enqueue(
            &run,
            Some(&source),
            &serde_json::json!({}),
            &[(
                task.clone(),
                serde_json::json!({"manifest_digest":digest,"has_secret_bundle":false}),
            )],
            1,
        )
        .unwrap();
    let attempt = AttemptId::generate();
    store
        .create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task,
                worker_id: WorkerId::parse("local").unwrap(),
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
        store.transition_attempt(&attempt, state, 3).unwrap();
    }
    store
        .finalize_attempt(
            &attempt,
            Termination::new(Outcome::Failed, Some(Exit::Code(1)), None).unwrap(),
            4,
        )
        .unwrap();
    let coordinator = Arc::new(Coordinator::start(store, CoordinatorConfig::default()).unwrap());
    (root, coordinator, run)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_stopping_releases_owner_while_input_io_retains_cross_process_lease() {
    use crate::service::completion::Target;
    let (root, coordinator, run) = retry_owner();
    let sources = crate::source::SourceStore::open(root.path()).unwrap();
    let transfer = Arc::new(Mutex::new(()));
    let (stop, mut stopping) = watch::channel(false);
    let lease =
        super::super::forward::lease(&sources, &mut stopping, transfer.clone().lock_owned().await)
            .await
            .unwrap();
    let file = secure_file(&sources.root().join("transfer.lock"), true).unwrap();
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let pending = tokio::spawn(retry_control(
        coordinator,
        Arc::new(Mutex::new(false)),
        RetryCommand {
            target: Target::Run(run.clone()),
            expected: 0,
            request_id: "stopped-preflight".into(),
        },
        lease,
        stopping,
        move |_| {
            entered.send(()).unwrap();
            resume.recv().unwrap();
            Ok(false)
        },
    ));
    tokio::time::timeout(Duration::from_secs(5), observed)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        file.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    assert!(sources.retention.guard(true).is_err());
    stop.send(true).unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(reply, Reply::ControlError { .. }));
    // The detached preflight has no Coordinator or CoordinatorStore ownership.
    let restarted = CoordinatorStore::open(root.path(), 9).unwrap();
    assert_eq!(
        restarted
            .completion(Target::Run(run))
            .unwrap()
            .control_revision,
        0
    );
    assert!(matches!(
        file.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    assert!(sources.retention.guard(true).is_err());
    release.send(()).unwrap();
    let guard = tokio::time::timeout(Duration::from_secs(2), transfer.lock_owned())
        .await
        .unwrap();
    file.try_lock().unwrap();
    drop(sources.retention.guard(true).unwrap());
    drop(guard);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_submit_publishes_reader_pin_before_immediate_logs_follow() {
    let root = crate::test_tmp::tempdir().unwrap();
    let mut resident = delivery_resident(root.path()).await;
    // Keep every worker offline, so no collector can backfill a missing pin.
    resident.worker_connection.abort();
    resident.worker_connection = tokio::spawn(std::future::pending());
    resident
        .coordinator
        .detach(&WorkerId::parse("local").unwrap())
        .await
        .unwrap();
    let directory = root.path().join("input");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("data"), b"snapshot").unwrap();
    let run = RunId::generate();
    let captured = crate::source::capture_for_run(
        &resident.captured.env,
        &resident.sources,
        &run,
        &crate::source::SourceRequest::Directory { dir: directory },
        &Default::default(),
    )
    .unwrap();
    let source = captured.manifest.source_id;
    let digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .unwrap();
    let task = TaskId::generate();
    let state = root.path().join("parent");
    let client = resident.client();
    let runner = tokio::spawn(resident.run());
    let accepted = client
        .request(Operation::Submit {
            run_id: run.clone(),
            source_id: source,
            tasks: vec![(task, delivery_spec(digest, false))],
        })
        .await
        .unwrap();
    assert!(matches!(accepted, Reply::Accepted { run_id } if run_id == run));
    // The CLI's exact follow observer must acquire its read-only pin before
    // querying log metadata, even though this run has no attempt/results yet.
    let mut observer = crate::service::logs::Observer::new(
        crate::service::query::Reader::Connected(client.clone()),
        state.join("results"),
        crate::service::completion::Target::Run(run.clone()),
        None,
    )
    .unwrap();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let pending = observer
        .observe(false, true, &mut out, &mut err)
        .await
        .unwrap();
    assert_eq!(pending.pending_tasks, 1);
    assert!(String::from_utf8_lossy(&out).contains("\"pending\":true"));
    // Follow stays attached awaiting work instead of failing legacy-unavailable.
    assert!(tokio::time::timeout(
        Duration::from_millis(300),
        observer.observe(true, true, &mut out, &mut err)
    )
    .await
    .is_err());
    assert!(err.is_empty());
    let reader = CoordinatorStore::open_read_only(&state).unwrap();
    assert!(reader.query_tasks(run, 1, None).unwrap().items[0]
        .current_attempt
        .is_none());
    drop(observer);
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(7), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn capability_private_ipc_prevents_old_owner_from_discarding_requirements() {
    use crate::toolchain::capability::ExecutionRequirements;
    let root = crate::test_tmp::tempdir().unwrap();
    let socket = root.path().join("old-owner.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let client = Client {
        socket,
        cluster_id: ClusterId::generate(),
        owner_token: "a".repeat(64),
    };
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: super::Request = read_message(&mut stream).await.unwrap();
        assert_eq!(request.schema_version, 3);
        let Operation::SubmitCaptured { tasks, .. } = request.operation else {
            panic!()
        };
        assert!(!tasks[0].1.requirements.is_empty());
        write_message(
            &mut stream,
            &Reply::Error {
                message: "old owner supports only private IPC v2".into(),
            },
        )
        .await
        .unwrap();
    });
    let mut task = delivery_spec(Digest::of_bytes(b"m"), false);
    task.requirements =
        ExecutionRequirements::parse(Some("node22".into()), &["node".into()]).unwrap();
    let op = Operation::SubmitCaptured {
        run_id: RunId::generate(),
        source_id: SourceId::generate(),
        tasks: vec![(TaskId::generate(), task)],
    };
    assert!(op.requires_capabilities());
    let reply = client.request(op).await.unwrap();
    assert!(matches!(reply, Reply::Error { .. }));
    server.await.unwrap();
    assert!(!Operation::Ping.requires_capabilities());
    assert!(!Operation::Submit {
        run_id: RunId::generate(),
        source_id: SourceId::generate(),
        tasks: vec![(
            TaskId::generate(),
            delivery_spec(Digest::of_bytes(b"m"), false)
        )]
    }
    .requires_capabilities());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn estimate_ipc_100_reads_preserve_rows_traffic_sources_and_readiness() {
    let root = crate::test_tmp::prefixed("rtr-estimate-").unwrap();
    // Each of the 200 reads inspects the worker, and an inspection measures
    // every native tool on the profile path with one subprocess each. On
    // macOS /usr/bin holds launchers for git, xcodebuild, java, python3 and
    // ruby; the git launcher alone starts xcodebuild and takes a third of a
    // second, which made these reads last minutes. The profile here searches
    // a private directory with one tool: a git that answers the version
    // question itself (with the oldest version a worker accepts) and hands
    // every other call to the host git.
    let tools = root.path().join("tools");
    std::fs::create_dir(&tools).unwrap();
    let git = tools.join("git");
    // Written by a child process: no descriptor of this multi-threaded test
    // process is ever open for writing on a file that is executed.
    let status = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(
            "printf '%s\\n' '#!/bin/sh' \
             'if [ \"$1\" = --version ]; then echo \"git version 2.31.0\"; exit 0; fi' \
             'exec /usr/bin/git \"$@\"' > \"$0\" && chmod 700 \"$0\"",
        )
        .arg(&git)
        .status()
        .unwrap();
    assert!(status.success());
    let resident = delivery_resident_on(root.path(), tools.to_str().unwrap()).await;
    let coordinator = resident.coordinator.clone();
    let gate = resident.input_gate.clone();
    let client = resident.client();
    gate.observe(
        resident.agent.config().boot_id.clone(),
        resident.agent.agent_epoch(),
    )
    .await;
    let task_id = TaskId::generate();
    let run_id = RunId::generate();
    let checkout = root.path().join("original-checkout");
    std::fs::create_dir(&checkout).unwrap();
    std::fs::write(checkout.join("command.txt"), "original input").unwrap();
    let captured = crate::source::capture_for_run(
        &resident.captured.env,
        &resident.sources,
        &run_id,
        &crate::source::SourceRequest::Directory {
            dir: checkout.clone(),
        },
        &Default::default(),
    )
    .unwrap();
    let source_id = captured.manifest.source_id;
    let digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .unwrap();
    let mut frozen = delivery_spec(digest, false);
    frozen.commands[0]
        .env
        .insert("FROZEN".into(), "original".into());
    let mut second = frozen.commands[0].clone();
    second.node_id = NodeId::parse("second").unwrap();
    second.depends_on = vec![frozen.commands[0].node_id.clone()];
    second.argv = vec!["/bin/echo".into(), "stored second command".into()];
    frozen.commands.push(second);
    let frozen_json = serde_json::to_value(&frozen).unwrap();
    coordinator
        .store()
        .put_source_ref(
            &source_id,
            &frozen.manifest_digest,
            crate::platform::wall_ms(),
        )
        .unwrap();
    coordinator
        .store()
        .enqueue(
            &run_id,
            Some(&source_id),
            &serde_json::json!({}),
            &[(task_id.clone(), frozen_json.clone())],
            crate::platform::wall_ms(),
        )
        .unwrap();
    // Closed runs remain estimable and cannot be scheduled by the owner's tick.
    coordinator
        .store()
        .close_run(&run_id, crate::platform::wall_ms())
        .unwrap();
    // Keep the resident's unrelated retention loop outside the measured snapshot.
    let _reader_pin =
        crate::results::pin::RunPin::shared(&root.path().join("parent"), &run_id).unwrap();
    let worker = WorkerId::parse("local").unwrap();
    let mut cost_query = crate::scheduler::measured::CostQuery::task(
        &frozen_json,
        Some(source_id.as_str()),
        gate.cost_transport(),
    );
    let model = coordinator.scheduler().model();
    cost_query.collection_policy = model.collection_policy();
    for phase in [
        crate::scheduler::measured::Phase::Transfer,
        crate::scheduler::measured::Phase::TerminalTail,
    ] {
        let sample = crate::scheduler::measured::Sample::verified(
            &worker,
            &cost_query,
            phase,
            format!("captured-{phase:?}"),
            crate::platform::monotonic_ms().saturating_sub(100),
            10,
        )
        .unwrap();
        model.record(sample);
    }
    model.drain();
    fn stable_evidence(evidence: crate::scheduler::measured::CostEvidence) -> serde_json::Value {
        let mut value = serde_json::to_value(evidence).unwrap();
        for name in ["transfer", "setup", "collection"] {
            value[name]
                .as_object_mut()
                .unwrap()
                .remove("remaining_ttl_ms");
        }
        value
    }
    let captured_evidence = stable_evidence(model.evidence(&worker, &cost_query).unwrap());
    assert!(captured_evidence.to_string().contains("measured"));
    std::fs::write(checkout.join("command.txt"), "changed after capture").unwrap();
    let before_generation = gate.generation();
    let before_readiness = gate.snapshot();
    let before_preferred = gate.preferred();
    fn rows(coordinator: &Coordinator) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
        coordinator
            .store()
            .db
            .call(|db| {
                let tables = db
                    .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let mut all = vec![];
                for table in tables {
                    let mut statement = db.prepare(&format!(
                        "SELECT * FROM \"{}\" ORDER BY rowid",
                        table.replace('"', "\"\"")
                    ))?;
                    let columns = statement.column_count();
                    let rows = statement
                        .query_map([], |r| {
                            (0..columns)
                                .map(|i| r.get(i))
                                .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    all.push((table, rows));
                }
                Ok(all)
            })
            .unwrap()
    }
    fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut out = BTreeMap::new();
        if root.exists() {
            for e in std::fs::read_dir(root).unwrap() {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    out.extend(files(&e.path()));
                } else {
                    out.insert(e.path(), std::fs::read(e.path()).unwrap());
                }
            }
        }
        out
    }
    // Isolate read-only IPC evidence from the enabled background input GC.
    // Production uses the same nonblocking busy skip while a reader holds SH.
    let _source_gc_pin = resident.captured.sources.mutation_guard().unwrap();
    let before_rows = rows(&coordinator);
    let before_sources = files(&root.path().join("parent/sources"));
    let traffic = root.path().join(".local/state/task-traffic-controller");
    let before_traffic = files(&traffic);
    let before_epoch = coordinator.epoch();
    // Every read asks the worker's inspection, which runs a subprocess per
    // tool and lasts a third of a second at best. On a loaded machine a read
    // can take longer than the production wait of one second, and the worker
    // is then left out as unreached: this test is about what the reads
    // preserve, not about that wait.
    coordinator
        .scheduler()
        .set_forecast_wait_for_test(Duration::from_secs(60));
    let runner = tokio::spawn(resident.run());
    // One read holds its worker inspection for a live CPU sample of 300 ms
    // (task-traffic-controller), so 200 reads one after another cannot take
    // less than a minute. The same 100 reads are sent 20 at a time; every
    // reply is still checked.
    async fn hundred_reads(client: &Client, operation: impl Fn() -> Operation) -> Vec<Reply> {
        let mut replies = vec![];
        for _ in 0..5 {
            let mut wave = tokio::task::JoinSet::new();
            for _ in 0..20 {
                let client = client.clone();
                let operation = operation();
                wave.spawn(async move { client.request(operation).await.unwrap() });
            }
            while let Some(reply) = wave.join_next().await {
                replies.push(reply.unwrap());
            }
        }
        assert_eq!(replies.len(), 100);
        replies
    }
    let fresh = hundred_reads(&client, || Operation::Estimate {
        query: crate::service::estimate::Query {
            version: 1,
            argv: vec!["echo".into()],
            repository_id: None,
            cwd_relative: String::new(),
            requirements: Default::default(),
        },
    })
    .await;
    for reply in fresh {
        match reply {
            Reply::Estimate { estimate } => assert!(estimate.online && estimate.conditional),
            other => panic!("unexpected estimate reply: {other:?}"),
        }
    }
    let known = hundred_reads(&client, || Operation::EstimateTask {
        task_id: task_id.clone(),
    })
    .await;
    for reply in known {
        match reply {
            Reply::Estimate { estimate } => {
                assert!(estimate.online && estimate.conditional);
                assert_eq!(estimate.source_kind, "captured");
                assert_eq!(estimate.forecasts.len(), 1, "{}", estimate.human());
                assert_eq!(estimate.forecasts[0].target.plan_digest, frozen.plan_digest);
                assert_ne!(
                    estimate.forecasts[0].target.runtime_source,
                    RuntimeSource::Measured
                );
                assert_eq!(estimate.components.len(), 1);
                assert!(serde_json::to_value(&estimate.components[0])
                    .unwrap()
                    .to_string()
                    .contains("measured"));
                assert_eq!(
                    stable_evidence(estimate.components[0].costs.clone()),
                    captured_evidence
                );
            }
            other => panic!("unexpected known-task reply: {other:?}"),
        }
    }
    match client
        .request(Operation::EstimateTask {
            task_id: TaskId::generate(),
        })
        .await
        .unwrap()
    {
        Reply::ControlError { error } => assert_eq!(error.code, crate::error::ErrorCode::NotFound),
        other => panic!("unexpected missing-task reply: {other:?}"),
    }
    assert_eq!(
        coordinator
            .store()
            .estimate_input(&task_id)
            .unwrap()
            .unwrap()
            .spec,
        frozen_json
    );
    assert_eq!(before_preferred, gate.preferred());
    assert_eq!(before_rows, rows(&coordinator));
    assert_eq!(before_epoch, coordinator.epoch());
    assert_eq!(before_generation, gate.generation());
    assert_eq!(before_readiness, gate.snapshot());
    assert_eq!(before_sources, files(&root.path().join("parent/sources")));
    assert_eq!(before_traffic, files(&traffic));
    assert!(!root.path().join("worker/clones").exists());
    client.request(Operation::Shutdown).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_rpc_waits_for_source_gc_before_input_lease_and_reports_typed_expiry() {
    use crate::service::completion::Target;
    let root = crate::test_tmp::tempdir().unwrap();
    let mut resident = delivery_resident(root.path()).await;
    resident.worker_connection.abort();
    resident.worker_connection = tokio::spawn(std::future::pending());
    let captured = resident.captured.clone();
    let input = root.path().join("input");
    std::fs::create_dir(&input).unwrap();
    std::fs::write(input.join("data"), "retained retry").unwrap();
    let run = RunId::generate();
    let source = crate::source::capture_for_run(
        &captured.env,
        &captured.sources,
        &run,
        &crate::source::SourceRequest::Directory { dir: input },
        &Default::default(),
    )
    .unwrap();
    let digest = Digest::parse(&source.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", source.manifest.manifest_digest)))
        .unwrap();
    let task = TaskId::generate();
    let store = resident.coordinator.store();
    store
        .put_source_ref(&source.manifest.source_id, &digest, 1)
        .unwrap();
    store
        .enqueue(
            &run,
            Some(&source.manifest.source_id),
            &serde_json::json!({}),
            &[(
                task.clone(),
                serde_json::to_value(delivery_spec(digest, false)).unwrap(),
            )],
            1,
        )
        .unwrap();
    let attempt = AttemptId::generate();
    store
        .create_attempt(
            NewAttempt {
                attempt_id: attempt.clone(),
                task_id: task,
                worker_id: WorkerId::parse("local").unwrap(),
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
        store.transition_attempt(&attempt, state, 3).unwrap();
    }
    store
        .finalize_attempt(
            &attempt,
            Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(1)), None).unwrap(),
            4,
        )
        .unwrap();
    let (entered, resume) = captured.sources.pause_retention_page();
    let sources = captured.sources.clone();
    let coordinator = resident.coordinator.clone();
    let gc = tokio::task::spawn_blocking(move || {
        sources.retention_page(
            coordinator.store(),
            &mut Default::default(),
            wall_ms(),
            crate::platform::clock::monotonic_ms(),
            crate::platform::boot_id(),
            crate::results::plan::retention_ms(7),
        )
    });
    tokio::task::spawn_blocking(move || entered.recv_timeout(Duration::from_secs(2)))
        .await
        .unwrap()
        .unwrap();
    let client = resident.client();
    let owner = tokio::spawn(resident.run());
    let retry = || Operation::Retry {
        target: Target::Run(run.clone()),
        expected_control_revision: 0,
        request_id: RunId::generate().to_string(),
    };
    let waits = captured.sources.foreground_wait_count();
    let first_client = client.clone();
    let first_request = retry();
    let first = tokio::spawn(async move { first_client.request(first_request).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while captured.sources.foreground_wait_count() == waits {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        client.request(Operation::Ping).await.unwrap(),
        Reply::Ready { .. }
    ));
    let reply = first.await.unwrap().unwrap();
    assert!(
        matches!(reply, Reply::ControlError { error } if error.code == crate::error::ErrorCode::SourceBusy)
    );
    let next_client = client.clone();
    let next_request = retry();
    let next = tokio::spawn(async move { next_client.request(next_request).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while captured.sources.foreground_wait_count() < waits + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!next.is_finished());
    resume.send(()).unwrap();
    gc.await.unwrap().unwrap();
    let reply = next.await.unwrap().unwrap();
    assert!(matches!(reply, Reply::RetryAccepted { .. }), "{reply:?}");
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.await.unwrap().unwrap();
}

#[test]
fn a_run_of_ci_tasks_is_a_request_an_older_resident_refuses() {
    let ci = TaskSpec {
        ci: Some(crate::coordinator::CiSpec {
            shard_retry_forbidden: false,
            task: crate::wire::CiTask {
                plan_digest: Digest::of_bytes(b"plan"),
                task_id: "t".into(),
                inputs: serde_json::json!({}),
            },
            hold: false,
            steps: 1,
            unrunnable: None,
            shard_group: None,
            joined: false,
            join_failure: None,
            rerun: false,
        }),
        commands: vec![],
        ..delivery_spec(Digest::of_bytes(b"manifest"), false)
    };
    let submit = |spec: TaskSpec| Operation::SubmitCaptured {
        run_id: RunId::generate(),
        source_id: SourceId::generate(),
        tasks: vec![(TaskId::generate(), spec)],
    };
    assert!(submit(ci.clone()).requires_ci());
    assert!(!submit(delivery_spec(Digest::of_bytes(b"manifest"), false)).requires_ci());
    assert!(!Operation::Ping.requires_ci());
    let plain = Operation::Submit {
        run_id: RunId::generate(),
        source_id: SourceId::generate(),
        tasks: vec![(TaskId::generate(), ci)],
    };
    assert!(plain.requires_ci());
}
