//! Optional identity-only observation for the selected stored-task preview worker.
//! Never a readiness proof, duration, historical sample or admission input.
use super::*;
use crate::ids::Digest;
use crate::setup::preparation_probe::{PreparationProbe, PreparationProbeInput, ProfileSample};
use std::sync::Weak;

pub(super) struct IdentityContext {
    link: Weak<dyn WorkerLink>,
    generation: u64,
    identity: (WorkerBootId, AgentEpoch),
    parent_boot: String,
    profile: Digest,
    sample: ProfileSample,
    deadline: u64,
    input: Digest,
    owner: crate::ids::BuilderId,
    probe: PreparationProbe,
}
impl Coordinator {
    /// Exactly one optional query, with complete stored input; no actor held over IO.
    pub(super) async fn query_preparation_identity(
        &self,
        worker: &WorkerId,
        input: &QueuedTask,
        spec: &TaskSpec,
        profile: Digest,
        expected_identity: (WorkerBootId, AgentEpoch),
        expected_attachment: Arc<dyn WorkerLink>,
    ) -> bool {
        let Ok(_query) = self.preparation_query.try_lock() else {
            return false;
        };
        let Some(source) = input.source_id.as_ref() else {
            return false;
        };
        // Do not accept a caller's projection or reconstructed partial specification.
        if serde_json::from_value::<TaskSpec>(input.spec.clone())
            .ok()
            .as_ref()
            != Some(spec)
        {
            return false;
        }
        let Some(link) = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(worker)
            .cloned()
        else {
            return false;
        };
        if !Arc::ptr_eq(&link, &expected_attachment)
            || self.is_fenced()
            || !link.preparation_probe_negotiated()
        {
            return false;
        }
        let Some(inputs) = link.placement_inputs() else {
            return false;
        };
        let Some(identity) = inputs.identity.clone() else {
            return false;
        };
        if identity != expected_identity {
            return false;
        }
        let Some(parent_boot) = crate::platform::boot_id() else {
            return false;
        };
        let query_input = PreparationProbeInput {
            manifest_digest: &spec.manifest_digest,
            source_id: source,
            run_id: &input.run_id,
            setup: &spec.setup,
            frozen: &None,
            in_place: spec.in_place,
        };
        let frozen = spec.toolchains.clone().map(Box::new);
        let query_input = PreparationProbeInput {
            frozen: &frozen,
            ..query_input
        };
        if crate::setup::preparation_probe::input_budget(&query_input).is_err() {
            return false;
        }
        let request = crate::wire::PreparationProbeRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            expected_profile_fingerprint: profile.clone(),
            manifest_digest: spec.manifest_digest.clone(),
            source_id: source.clone(),
            run_id: input.run_id.clone(),
            setup: spec.setup.clone(),
            frozen,
            in_place: spec.in_place,
        };
        let Some(binding) = bounded_binding(&(
            worker,
            &identity,
            &request.manifest_digest,
            source,
            &request.run_id,
            spec,
        )) else {
            return false;
        };
        let sent = crate::platform::monotonic_ms();
        let response = tokio::time::timeout(
            Duration::from_millis(self.sched.cfg.forecast_wait_ms.min(5000)),
            link.read_only_call(Request::PreparationProbe(request)),
        )
        .await;
        let received = crate::platform::monotonic_ms();
        let Ok(Ok(Response::PreparationProbe(response))) = response else {
            return false;
        };
        let PreparationProbe::Entries {
            owner_generation,
            provenance,
            entries,
        } = &response.probe
        else {
            return false;
        };
        let Some(mut deadline) = provenance.deadline(sent, received) else {
            return false;
        };
        if response.schema_version != SchemaVersion::CURRENT
            || response.worker_id != *worker
            || (response.worker_boot_id.clone(), response.agent_epoch) != identity
            || entries.len() > 16
            || (provenance.verification_started_monotonic_ms.is_none()
                && entries.iter().any(|entry| matches!(entry.state,
                    crate::setup::preparation_probe::PreparationProbeState::VerifiedAvailable { .. })))
            || self.is_fenced()
            || crate::platform::boot_id().as_deref() != Some(parent_boot.as_str())
        {
            return false;
        }
        let Ok(_attachment) = self.placement_fence.try_lock() else {
            return false;
        };
        if !self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(worker)
            .is_some_and(|current| Arc::ptr_eq(current, &link))
        {
            return false;
        }
        let Some(fence) = link.placement_fence() else {
            return false;
        };
        let Ok(_generation) = fence.try_lock() else {
            return false;
        };
        let Some(current) = link.placement_inputs() else {
            return false;
        };
        if current.generation != inputs.generation
            || current.identity.as_ref() != Some(&identity)
            || !link.preparation_probe_negotiated()
        {
            return false;
        }
        let mut cache = self
            .preparation_identities
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(old) = cache
            .get(worker)
            .filter(|old| old.parent_boot == parent_boot)
        {
            if old.identity == identity
                && old.profile == profile
                && old.sample.native_boot != provenance.profile.native_boot
            {
                return false;
            }
            // Same profile sample cannot receive a later deadline, even on rebinding.
            if old.identity == identity
                && old.profile == profile
                && old.sample.native_boot == provenance.profile.native_boot
            {
                if old.sample.sequence > provenance.profile.sequence {
                    return false;
                }
                if old.sample.sequence == provenance.profile.sequence {
                    if old.sample != provenance.profile {
                        return false;
                    }
                    deadline = deadline.min(old.deadline);
                }
            }
            // Ownership cannot silently change within the same sampled context.
            if old.link.upgrade().is_some_and(|v| Arc::ptr_eq(&v, &link))
                && old.generation == inputs.generation
                && old.identity == identity
                && old.profile == profile
                && old.owner != *owner_generation
            {
                return false;
            }
        }
        let now = crate::platform::monotonic_ms();
        if now < received
            || received >= deadline
            || now >= deadline
            || crate::platform::boot_id().as_deref() != Some(parent_boot.as_str())
        {
            return false;
        }
        if cache.len() >= 256 && !cache.contains_key(worker) {
            return false;
        }
        cache.insert(
            worker.clone(),
            IdentityContext {
                link: Arc::downgrade(&link),
                generation: inputs.generation,
                identity,
                parent_boot,
                profile,
                sample: provenance.profile.clone(),
                deadline,
                input: binding.clone(),
                owner: owner_generation.clone(),
                probe: response.probe,
            },
        );
        // Consume the bounded context only as an identity observation; no cost scalar.
        cache.get(worker).is_some_and(|v| {
            v.input == binding
                && matches!(&v.probe, PreparationProbe::Entries { entries, .. }
                if entries.iter().any(|entry| entry.key.is_some() && matches!(entry.state,
                    crate::setup::preparation_probe::PreparationProbeState::ConcretePlan
                    | crate::setup::preparation_probe::PreparationProbeState::VerifiedAvailable { .. }
                    | crate::setup::preparation_probe::PreparationProbeState::Active { .. })))
        })
    }
}

/// Bound serialization before allocation, including the full stored command/environment input.
fn bounded_binding(value: &impl serde::Serialize) -> Option<Digest> {
    struct Bytes(Vec<u8>);
    impl std::io::Write for Bytes {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > (256usize * 1024).saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("preparation input budget"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut bytes = Bytes(Vec::new());
    serde_json::to_writer(&mut bytes, value).ok()?;
    Some(Digest::of_bytes(&bytes.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{BuilderId, NodeId};
    use crate::setup::preparation_probe::{
        PreparationProbeEntry, PreparationProbeState, ProbeProvenance,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Link {
        gate: Arc<InputGate>,
        response: Mutex<crate::wire::PreparationProbeResponse>,
        supported: AtomicBool,
        calls: AtomicUsize,
        entered: tokio::sync::Notify,
        resume: tokio::sync::Semaphore,
        hold: AtomicBool,
        owner: Weak<Coordinator>,
    }
    impl WorkerLink for Link {
        fn preparation_probe_negotiated(&self) -> bool {
            self.supported.load(Ordering::SeqCst)
        }
        fn placement_inputs(&self) -> Option<PlacementInputs> {
            Some(self.gate.placement_inputs())
        }
        fn placement_fence(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
            Some(self.gate.placement_fence())
        }
        fn call(&self, _: Request) -> super::super::link::LinkFuture<'_> {
            panic!("identity query used mutating link")
        }
        fn read_only_call(&self, request: Request) -> super::super::link::LinkFuture<'_> {
            Box::pin(async move {
                if let Request::Inspect(_) = &request {
                    let r = self.response.lock().unwrap();
                    return Ok(Response::Inspect(crate::wire::InspectResponse {
                        own_runs_active: None,
                        admission: None,
                        preparation_observations: None,
                        capabilities: Some(crate::toolchain::capability::WorkerCapabilities {
                            disk_free_bytes: None,
                            resource_budget: None,
                            cpu_cores: None,
                            memory_bytes: None,
                            schema_version: 1,
                            os: "macos".into(),
                            arch: "aarch64".into(),
                            profile: "default".into(),
                            profile_digest: Digest::of_bytes(b"definition"),
                            execution_uid: 501,
                            protocol_versions: vec![1],
                            profile_fingerprint: Digest::of_bytes(b"profile"),
                            tools: BTreeMap::new(),
                            features: [
                                crate::wire::PREPARATION_PROBE_FEATURE.into(),
                                crate::forecast::COMMAND_FEATURE.into(),
                            ]
                            .into(),
                            rustup: Vec::new(),
                        }),
                        schema_version: SchemaVersion::CURRENT,
                        worker_id: r.worker_id.clone(),
                        worker_boot_id: r.worker_boot_id.clone(),
                        agent_epoch: r.agent_epoch,
                        snapshot_digest: Digest::of_bytes(b"snapshot"),
                        policy_digest: Digest::of_bytes(b"policy"),
                        active_task: 0,
                        prefetch_pending: 0,
                        max_active_task: 8,
                        prefetch_per_worker: 2,
                        disk_free_bytes: 1000000,
                        draining: false,
                    }));
                }
                let forecast = match &request {
                    Request::Forecast(r) => Some(r),
                    Request::ForecastCommand(r) => Some(&r.forecast),
                    _ => None,
                };
                if let Some(forecast) = forecast {
                    let r = self.response.lock().unwrap();
                    let response = crate::wire::ForecastResponse {
                        foreign_demand: 0,
                        schema_version: SchemaVersion::CURRENT,
                        worker_id: r.worker_id.clone(),
                        worker_boot_id: r.worker_boot_id.clone(),
                        agent_epoch: r.agent_epoch,
                        snapshot_digest: Digest::of_bytes(b"snapshot"),
                        policy_digest: Digest::of_bytes(b"policy"),
                        ttl_ms: 5000,
                        coverage: vec![],
                        target: crate::wire::ForecastTarget {
                            plan_digest: forecast.workload.plan_digest.clone(),
                            ready_in_ms: None,
                            queue_wait_after_ready_ms: None,
                            execution_ms: None,
                            finish_in_ms: None,
                            confidence: crate::wire::Confidence::Low,
                            runtime_source: crate::wire::RuntimeSource::Unknown,
                            history_count: 0,
                            conditional: true,
                            overrun: false,
                            wait_reason: None,
                            assumptions: vec![],
                            null_reasons: BTreeMap::new(),
                            ranking: None,
                        },
                    };
                    return Ok(if matches!(request, Request::ForecastCommand(_)) {
                        Response::ForecastCommand(response)
                    } else {
                        Response::Forecast(response)
                    });
                }
                let Request::PreparationProbe(request) = request else {
                    panic!("unexpected request");
                };
                assert_eq!(
                    request.expected_profile_fingerprint,
                    Digest::of_bytes(b"profile")
                );
                assert_eq!(request.manifest_digest, Digest::of_bytes(b"manifest"));
                assert!(self.owner.upgrade().unwrap().actor.try_lock().is_ok());
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                if self.hold.load(Ordering::SeqCst) {
                    self.resume.acquire().await.unwrap().forget();
                }
                Ok(Response::PreparationProbe(
                    self.response.lock().unwrap().clone(),
                ))
            })
        }
    }
    struct Fixture {
        _temp: tempfile::TempDir,
        c: Arc<Coordinator>,
        worker: WorkerId,
        link: Arc<Link>,
        input: QueuedTask,
        spec: TaskSpec,
    }
    impl Fixture {
        async fn new() -> Self {
            let temp = crate::test_tmp::tempdir().unwrap();
            let c = Arc::new(
                Coordinator::start(
                    CoordinatorStore::open(temp.path(), 1).unwrap(),
                    CoordinatorConfig::default(),
                )
                .unwrap(),
            );
            let worker = WorkerId::parse("preparation-worker").unwrap();
            let boot = WorkerBootId::parse("worker-boot").unwrap();
            let gate = Arc::new(InputGate::default());
            gate.observe(boot.clone(), AgentEpoch(1)).await;
            let link = Arc::new(Link {
                gate,
                response: Mutex::new(crate::wire::PreparationProbeResponse {
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: worker.clone(),
                    worker_boot_id: boot,
                    agent_epoch: AgentEpoch(1),
                    probe: PreparationProbe::Entries {
                        owner_generation: BuilderId::generate(),
                        provenance: ProbeProvenance {
                            profile: ProfileSample {
                                native_boot: "native-worker".into(),
                                sequence: 1,
                                completed_monotonic_ms: 100,
                                ttl_ms: 5000,
                            },
                            observed_monotonic_ms: 100,
                            observation_ttl_ms: 5000,
                            verification_started_monotonic_ms: None,
                        },
                        entries: vec![PreparationProbeEntry {
                            kind: crate::setup::observation::PreparationKind::WorkspaceSetup,
                            key: Some(Digest::of_bytes(b"actual-key")),
                            state: PreparationProbeState::ConcretePlan,
                        }],
                    },
                }),
                supported: AtomicBool::new(true),
                calls: AtomicUsize::new(0),
                entered: tokio::sync::Notify::new(),
                resume: tokio::sync::Semaphore::new(0),
                hold: AtomicBool::new(false),
                owner: Arc::downgrade(&c),
            });
            c.attach(worker.clone(), link.clone()).await;
            let spec = TaskSpec {
                ci: None,
                toolchains: None,
                requirements: Default::default(),
                in_place: false,
                local: false,
                setup: None,
                plan_digest: Digest::of_bytes(b"plan"),
                manifest_digest: Digest::of_bytes(b"manifest"),
                commands: vec![CommandSpec {
                    setup_step: None,
                    node_id: NodeId::parse("main").unwrap(),
                    argv: vec!["true".into()],
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
                outputs: vec![],
            };
            let input = QueuedTask {
                task_id: TaskId::generate(),
                run_id: RunId::generate(),
                source_id: Some(SourceId::generate()),
                spec: serde_json::to_value(&spec).unwrap(),
                created_at_ms: 0,
                queue_rowid: 1,
            };
            Self {
                _temp: temp,
                c,
                worker,
                link,
                input,
                spec,
            }
        }
        async fn query(&self) -> bool {
            self.c
                .query_preparation_identity(
                    &self.worker,
                    &self.input,
                    &self.spec,
                    Digest::of_bytes(b"profile"),
                    (WorkerBootId::parse("worker-boot").unwrap(), AgentEpoch(1)),
                    self.link.clone(),
                )
                .await
        }
    }
    #[tokio::test]
    async fn preparation_identity_stored_estimate_queries_selected_worker_only_without_cost() {
        let f = Fixture::new().await;
        if let PreparationProbe::Entries {
            provenance,
            entries,
            ..
        } = &mut f.link.response.lock().unwrap().probe
        {
            provenance.verification_started_monotonic_ms = Some(100);
            entries[0].kind = crate::setup::observation::PreparationKind::ToolInstall;
            entries[0].state = PreparationProbeState::VerifiedAvailable {
                proof: Digest::of_bytes(b"bounded complete sample"),
            };
        }
        f.c.store
            .put_source_ref(
                f.input.source_id.as_ref().unwrap(),
                &f.spec.manifest_digest,
                0,
            )
            .unwrap();
        f.c.store
            .enqueue(
                &f.input.run_id,
                f.input.source_id.as_ref(),
                &serde_json::json!({}),
                &[(f.input.task_id.clone(), f.input.spec.clone())],
                1,
            )
            .unwrap();
        f.c.store.close_run(&f.input.run_id, 2).unwrap();
        let estimate = f.c.estimate_task(f.input.task_id.clone()).await.unwrap();
        assert_eq!(
            estimate.decision.as_ref().unwrap().winner.as_ref(),
            Some(&f.worker)
        );
        assert!(estimate
            .assumptions
            .iter()
            .any(|s| s.contains("preparation identity observed")));
        assert_eq!(f.link.calls.load(Ordering::SeqCst), 1);
        assert_eq!(f.c.preparation_identities.lock().unwrap().len(), 1);
        let command =
            f.c.estimate(crate::service::estimate::Query {
                version: 1,
                argv: vec!["true".into()],
                repository_id: None,
                cwd_relative: String::new(),
                requirements: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(f.link.calls.load(Ordering::SeqCst), 1);
        assert!(!command
            .assumptions
            .iter()
            .any(|s| s.contains("preparation identity observed")));
        assert!(estimate
            .decision
            .unwrap()
            .compared
            .iter()
            .all(|w| w.costs.ms[decide::SETUP].is_none()));
    }
    #[tokio::test]
    async fn preparation_identity_observation_is_bounded_readonly_and_never_renews_sample() {
        let mut f = Fixture::new().await;
        assert!(f.query().await);
        let deadline = f.c.preparation_identities.lock().unwrap()[&f.worker].deadline;
        assert!(f.query().await);
        assert_eq!(
            f.c.preparation_identities.lock().unwrap()[&f.worker].deadline,
            deadline
        );
        assert_eq!(f.link.calls.load(Ordering::SeqCst), 2);
        // Exact context changes are observed under a new binding without renewing the sample.
        f.spec.commands[0]
            .env
            .insert("FLAG".into(), "changed".into());
        f.input.spec = serde_json::to_value(&f.spec).unwrap();
        assert!(f.query().await);
        assert_eq!(
            f.c.preparation_identities.lock().unwrap()[&f.worker].deadline,
            deadline
        );
        {
            let c = f.c.preparation_identities.lock().unwrap();
            assert_eq!(c.len(), 1);
            assert!(matches!(
                c[&f.worker].probe,
                PreparationProbe::Entries { .. }
            ));
        }
        {
            let PreparationProbe::Entries { provenance, .. } =
                &mut f.link.response.lock().unwrap().probe
            else {
                unreachable!()
            };
            provenance.observed_monotonic_ms = 5100;
        }
        assert!(!f.query().await);
    }
    #[tokio::test]
    async fn preparation_identity_owner_and_original_sample_conflicts_stay_unknown() {
        let mut f = Fixture::new().await;
        assert!(f.query().await);
        let before = f.c.preparation_identities.lock().unwrap()[&f.worker]
            .owner
            .clone();
        if let PreparationProbe::Entries {
            owner_generation, ..
        } = &mut f.link.response.lock().unwrap().probe
        {
            *owner_generation = BuilderId::generate();
        }
        f.spec.commands[0]
            .env
            .insert("changed".into(), "input".into());
        f.input.spec = serde_json::to_value(&f.spec).unwrap();
        assert!(!f.query().await);
        assert_eq!(
            f.c.preparation_identities.lock().unwrap()[&f.worker].owner,
            before
        );
        if let PreparationProbe::Entries {
            owner_generation,
            provenance,
            ..
        } = &mut f.link.response.lock().unwrap().probe
        {
            *owner_generation = before;
            provenance.profile.completed_monotonic_ms += 1;
            provenance.observed_monotonic_ms += 1;
        }
        assert!(!f.query().await);
    }
    #[tokio::test]
    async fn preparation_identity_missing_context_old_peer_and_wrong_identity_are_unknown() {
        let mut f = Fixture::new().await;
        f.input.source_id = None;
        assert!(!f.query().await);
        f.input.source_id = Some(SourceId::generate());
        f.spec.commands[0].argv.push("unbound".into());
        assert!(!f.query().await);
        f.input.spec = serde_json::to_value(&f.spec).unwrap();
        f.link.supported.store(false, Ordering::SeqCst);
        assert!(!f.query().await);
        assert_eq!(f.link.calls.load(Ordering::SeqCst), 0);
        f.link.supported.store(true, Ordering::SeqCst);
        f.link.response.lock().unwrap().agent_epoch = AgentEpoch(2);
        assert!(!f.query().await);
        assert!(f.c.preparation_identities.lock().unwrap().is_empty());
        assert!(bounded_binding(&"x".repeat(256 * 1024)).is_none());
    }
    #[tokio::test]
    async fn preparation_identity_rejects_attachment_and_input_gate_drift_during_io() {
        for replace in [false, true] {
            let f = Arc::new(Fixture::new().await);
            f.link.hold.store(true, Ordering::SeqCst);
            let task = {
                let f = f.clone();
                tokio::spawn(async move { f.query().await })
            };
            f.link.entered.notified().await;
            // The global optional query cap prevents a second concurrent request.
            assert!(!f.query().await);
            if replace {
                struct Absent;
                impl WorkerLink for Absent {
                    fn call(&self, _: Request) -> super::super::link::LinkFuture<'_> {
                        Box::pin(async { Err(LinkError::Unconfirmed) })
                    }
                }
                f.c.attach(f.worker.clone(), Arc::new(Absent)).await;
            } else {
                f.link
                    .gate
                    .observe(WorkerBootId::parse("reboot").unwrap(), AgentEpoch(2))
                    .await;
            }
            f.link.resume.add_permits(1);
            assert!(!task.await.unwrap());
            assert!(f.c.preparation_identities.lock().unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn preparation_identity_complete_sample_requires_valid_original_verification_start() {
        let f = Fixture::new().await;
        for started in [None, Some(99), Some(101)] {
            if let PreparationProbe::Entries {
                provenance,
                entries,
                ..
            } = &mut f.link.response.lock().unwrap().probe
            {
                provenance.verification_started_monotonic_ms = started;
                entries[0].state = PreparationProbeState::VerifiedAvailable {
                    proof: Digest::of_bytes(b"sample"),
                };
            }
            assert!(!f.query().await);
            assert!(f.c.preparation_identities.lock().unwrap().is_empty());
        }
        if let PreparationProbe::Entries { provenance, .. } =
            &mut f.link.response.lock().unwrap().probe
        {
            provenance.verification_started_monotonic_ms = Some(100);
        }
        assert!(f.query().await);
    }
}
