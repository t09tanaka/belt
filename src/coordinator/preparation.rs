//! Follow-only optional telemetry. No setup scalar is inferred from this history.
use super::*;
use crate::scheduler::measured::{PreparationSample, Transport};
use crate::setup::observation_transport::{
    ObservationCursor, ObservationPage, ObservationPageRequest, PAGE_LIMIT,
};
use std::sync::Weak;

pub(super) struct CursorState {
    link: Weak<dyn WorkerLink>,
    generation: u64,
    cursor: ObservationCursor,
}
pub(super) struct RawPage {
    pub worker: WorkerId,
    pub link: Arc<dyn WorkerLink>,
    pub inputs: PlacementInputs,
    pub transport: Transport,
    pub request: ObservationPageRequest,
    pub response_boot: WorkerBootId,
    pub response_epoch: AgentEpoch,
    pub page: ObservationPage,
    pub parent_boot: String,
    pub sent: u64,
    pub received: u64,
}
impl Coordinator {
    pub(super) fn preparation_request(
        &self,
        worker: &WorkerId,
        link: &Arc<dyn WorkerLink>,
        inputs: Option<&PlacementInputs>,
    ) -> Option<ObservationPageRequest> {
        if !link.preparation_observations_negotiated() || link.cost_transport().is_none() {
            return None;
        }
        let inputs = inputs.filter(|v| v.identity.is_some())?;
        let cursors = self
            .preparation_cursors
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let cursor = cursors
            .get(worker)
            .filter(|state| {
                state.generation == inputs.generation
                    && state
                        .link
                        .upgrade()
                        .is_some_and(|old| Arc::ptr_eq(&old, link))
            })
            .map(|state| state.cursor.clone());
        Some(ObservationPageRequest { cursor })
    }
    pub(super) fn ingest_preparation(&self, pages: Vec<RawPage>) {
        for raw in pages.into_iter().take(32) {
            if self.is_fenced()
                || crate::platform::boot_id().as_deref() != Some(raw.parent_boot.as_str())
            {
                continue;
            }
            let Ok(_attachment) = self.placement_fence.try_lock() else {
                continue;
            };
            if !self
                .workers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&raw.worker)
                .is_some_and(|current| Arc::ptr_eq(current, &raw.link))
            {
                continue;
            }
            let Some(fence) = raw.link.placement_fence() else {
                continue;
            };
            let Ok(_generation) = fence.try_lock() else {
                continue;
            };
            let Some(current) = raw.link.placement_inputs() else {
                continue;
            };
            if current.generation != raw.inputs.generation
                || current.identity != raw.inputs.identity
                || current.identity.as_ref()
                    != Some(&(raw.response_boot.clone(), raw.response_epoch))
                || raw.transport.worker_boot != raw.response_boot
                || raw.transport.agent_epoch != raw.response_epoch
                || raw.link.cost_transport().as_ref() != Some(&raw.transport)
                || !raw.link.preparation_observations_negotiated()
            {
                continue;
            }
            let validated = validate(&raw);
            let mut cursors = self
                .preparation_cursors
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some((cursor, samples)) = validated else {
                cursors.remove(&raw.worker);
                continue;
            };
            cursors.retain(|_, state| state.link.strong_count() > 0);
            if cursors.len() < 4096 || cursors.contains_key(&raw.worker) {
                cursors.insert(
                    raw.worker.clone(),
                    CursorState {
                        link: Arc::downgrade(&raw.link),
                        generation: current.generation,
                        cursor,
                    },
                );
            }
            drop(cursors);
            // A full/failed queue may lose telemetry, never placement or leases.
            for sample in samples {
                self.sched.model().record_preparation(sample);
            }
        }
    }
}
fn validate(raw: &RawPage) -> Option<(ObservationCursor, Vec<PreparationSample>)> {
    let ObservationPage::Available {
        owner_generation,
        native_boot,
        monotonic_now_ms,
        entries,
        cursor,
        pass_complete,
        overflow: _,
    } = &raw.page
    else {
        return None;
    };
    if entries.len() > PAGE_LIMIT
        || cursor.owner_generation != *owner_generation
        || cursor.native_boot != *native_boot
        || cursor.monotonic_now_ms != *monotonic_now_ms
        || cursor.after_sequence > cursor.upper_sequence
        || cursor.upper_sequence > 4096
        || *pass_complete != (cursor.after_sequence == cursor.upper_sequence)
        || raw.received < raw.sent
        || [raw.sent, raw.received, *monotonic_now_ms].contains(&u64::MAX)
    {
        return None;
    }
    let after = if let Some(previous) = &raw.request.cursor {
        if previous.owner_generation != *owner_generation
            || previous.native_boot != *native_boot
            || previous.after_sequence > previous.upper_sequence
            || previous.monotonic_now_ms > *monotonic_now_ms
            || (previous.after_sequence < previous.upper_sequence
                && cursor.upper_sequence != previous.upper_sequence)
            || cursor.upper_sequence < previous.upper_sequence
        {
            return None;
        }
        previous.after_sequence
    } else {
        0
    };
    let expected = (cursor.upper_sequence.checked_sub(after)?).min(PAGE_LIMIT as u64) as usize;
    if entries.len() != expected
        || cursor.after_sequence != after.checked_add(entries.len() as u64)?
    {
        return None;
    }
    let mut samples = Vec::with_capacity(entries.len());
    let mut builders = BTreeSet::new();
    for (index, entry) in entries.iter().enumerate() {
        if entry.sequence != after.checked_add(index as u64 + 1)?
            || !builders.insert(entry.observation.binding.builder_id.clone())
        {
            return None;
        }
        samples.push(PreparationSample::verified(
            &raw.worker,
            &raw.transport,
            owner_generation,
            native_boot,
            entry.sequence,
            &entry.observation,
            &raw.parent_boot,
            raw.sent,
            raw.received,
            *monotonic_now_ms,
        )?);
    }
    // Empty responses still have to prove a usable native clock and boot.
    if native_boot.is_empty() || native_boot.len() > 128 {
        return None;
    }
    Some((cursor.clone(), samples))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{decide::Costs, CostModel};
    use crate::setup::observation::{
        PreparationBinding, PreparationKind, PreparationObservation, POLICY,
    };
    use crate::setup::observation_transport::SequencedObservation;
    use std::sync::atomic::AtomicUsize;
    struct Feed {
        owner: crate::ids::BuilderId,
        values: Vec<PreparationObservation>,
    }
    struct Link {
        gate: Arc<InputGate>,
        feed: Mutex<Feed>,
        questions: Mutex<Vec<Option<ObservationPageRequest>>>,
        supported: bool,
    }
    impl WorkerLink for Link {
        fn preparation_observations_negotiated(&self) -> bool {
            self.supported
        }
        fn placement_inputs(&self) -> Option<PlacementInputs> {
            Some(self.gate.placement_inputs())
        }
        fn placement_fence(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
            Some(self.gate.placement_fence())
        }
        fn cost_transport(&self) -> Option<Transport> {
            self.gate.cost_transport()
        }
        fn call(&self, request: Request) -> super::super::link::LinkFuture<'_> {
            Box::pin(async move {
                let Request::Inspect(request) = request else {
                    return Err(LinkError::Unconfirmed);
                };
                self.questions
                    .lock()
                    .unwrap()
                    .push(request.preparation_observations.clone());
                let page = request.preparation_observations.map(|request| {
                    let feed = self.feed.lock().unwrap();
                    let now = crate::platform::monotonic_ms();
                    let after = request.cursor.as_ref().map_or(0, |v| v.after_sequence);
                    let upper = request
                        .cursor
                        .as_ref()
                        .filter(|v| v.after_sequence < v.upper_sequence)
                        .map_or(feed.values.len() as u64, |v| v.upper_sequence);
                    let entries: Vec<_> = feed
                        .values
                        .iter()
                        .enumerate()
                        .skip(after as usize)
                        .take((upper - after).min(64) as usize)
                        .map(|(n, value)| SequencedObservation {
                            sequence: n as u64 + 1,
                            observation: value.clone(),
                        })
                        .collect();
                    let after = after + entries.len() as u64;
                    ObservationPage::Available {
                        owner_generation: feed.owner.clone(),
                        native_boot: "remote-native".into(),
                        monotonic_now_ms: now,
                        entries,
                        cursor: ObservationCursor {
                            owner_generation: feed.owner.clone(),
                            native_boot: "remote-native".into(),
                            after_sequence: after,
                            upper_sequence: upper,
                            monotonic_now_ms: now,
                        },
                        pass_complete: after == upper,
                        overflow: true,
                    }
                });
                Ok(Response::Inspect(InspectResponse {
                    preparation_observations: page,
                    capabilities: None,
                    schema_version: SchemaVersion::CURRENT,
                    worker_id: WorkerId::parse("worker-a").unwrap(),
                    worker_boot_id: WorkerBootId::parse("worker-boot").unwrap(),
                    agent_epoch: AgentEpoch(1),
                    snapshot_digest: crate::ids::Digest::of_bytes(b"snapshot"),
                    policy_digest: crate::ids::Digest::of_bytes(b"policy"),
                    active_task: 0,
                    prefetch_pending: 0,
                    max_active_task: 2,
                    prefetch_per_worker: 1,
                    disk_free_bytes: 1000000,
                    draining: false,
                }))
            })
        }
    }
    #[derive(Default)]
    struct Spy {
        samples: Mutex<Vec<PreparationSample>>,
        owner: Mutex<Option<Weak<Coordinator>>>,
        after_actor: AtomicUsize,
    }
    impl CostModel for Spy {
        fn costs(&self, _: &WorkerId, _: &QueuedTask) -> Costs {
            Costs::default()
        }
        fn record_preparation(&self, sample: PreparationSample) {
            if let Some(owner) = self.owner.lock().unwrap().as_ref().and_then(Weak::upgrade) {
                assert!(owner.actor.try_lock().is_ok());
                self.after_actor.fetch_add(1, Ordering::SeqCst);
            }
            self.samples.lock().unwrap().push(sample);
        }
    }
    fn sample(owner: &crate::ids::BuilderId) -> PreparationObservation {
        PreparationObservation {
            binding: PreparationBinding {
                builder_id: crate::ids::BuilderId::generate(),
                attempt_id: AttemptId::generate(),
                owner_generation: owner.clone(),
                kind: PreparationKind::ToolInstall,
                key: crate::ids::Digest::of_bytes(b"recipe"),
            },
            policy: POLICY.into(),
            nodes_digest: crate::ids::Digest::of_bytes(b"nodes"),
            node_count: 1,
            boot: "remote-native".into(),
            finished_monotonic_ms: crate::platform::monotonic_ms().saturating_sub(100),
            duration_ms: 10,
        }
    }
    async fn fixture(
        count: usize,
        supported: bool,
    ) -> (tempfile::TempDir, Arc<Coordinator>, Arc<Link>, Arc<Spy>) {
        let dir = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        let spy = Arc::new(Spy::default());
        let sched = Scheduler::new(Default::default(), spy.clone()).unwrap();
        let c = Arc::new(Coordinator::start_with(store, Default::default(), sched).unwrap());
        *spy.owner.lock().unwrap() = Some(Arc::downgrade(&c));
        let gate = Arc::new(InputGate::with_transport_policy(
            "opaque-private-policy".into(),
        ));
        gate.observe(WorkerBootId::parse("worker-boot").unwrap(), AgentEpoch(1))
            .await;
        let owner = crate::ids::BuilderId::generate();
        let values = (0..count).map(|_| sample(&owner)).collect();
        let link = Arc::new(Link {
            gate,
            feed: Mutex::new(Feed { owner, values }),
            questions: Mutex::default(),
            supported,
        });
        c.attach(WorkerId::parse("worker-a").unwrap(), link.clone())
            .await;
        (dir, c, link, spy)
    }
    #[tokio::test]
    async fn follow_paginates_after_actor_release_and_estimate_never_ingests() {
        let (_d, c, link, spy) = fixture(130, true).await;
        for expected in [64, 128, 130] {
            c.tick().await.unwrap();
            assert_eq!(spy.samples.lock().unwrap().len(), expected);
        }
        let before = c
            .preparation_cursors
            .lock()
            .unwrap()
            .get(&WorkerId::parse("worker-a").unwrap())
            .unwrap()
            .cursor
            .clone();
        let _ = c
            .estimate(crate::service::estimate::Query {
                version: 1,
                argv: vec!["true".into()],
                repository_id: None,
                cwd_relative: "".into(),
                requirements: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(spy.samples.lock().unwrap().len(), 130);
        assert!(link.questions.lock().unwrap().last().unwrap().is_none());
        assert_eq!(
            c.preparation_cursors
                .lock()
                .unwrap()
                .get(&WorkerId::parse("worker-a").unwrap())
                .unwrap()
                .cursor,
            before
        );
        {
            let mut feed = link.feed.lock().unwrap();
            let owner = feed.owner.clone();
            feed.values.push(sample(&owner));
        }
        c.tick().await.unwrap();
        assert_eq!(spy.samples.lock().unwrap().len(), 131);
        assert_eq!(spy.after_actor.load(Ordering::SeqCst), 131);
    }
    #[tokio::test]
    async fn stale_attachment_generation_and_legacy_link_cannot_ingest() {
        let (_d, c, link, spy) = fixture(1, true).await;
        let worker = WorkerId::parse("worker-a").unwrap();
        let raw = Mutex::new(Vec::new());
        c.follow(worker.clone(), link.clone(), c.deadline(), &raw)
            .await
            .unwrap();
        link.gate.invalidate().await;
        c.ingest_preparation(raw.into_inner().unwrap());
        assert!(spy.samples.lock().unwrap().is_empty());
        link.gate
            .observe(WorkerBootId::parse("worker-boot").unwrap(), AgentEpoch(1))
            .await;
        let raw = Mutex::new(Vec::new());
        c.follow(worker.clone(), link.clone(), c.deadline(), &raw)
            .await
            .unwrap();
        c.workers.lock().unwrap().insert(
            worker.clone(),
            Arc::new(Budgeted {
                inner: link.clone(),
                deadline: c.deadline(),
            }),
        );
        c.ingest_preparation(raw.into_inner().unwrap());
        assert!(spy.samples.lock().unwrap().is_empty());
        let (_d, c, link, spy) = fixture(1, false).await;
        c.tick().await.unwrap();
        assert!(spy.samples.lock().unwrap().is_empty());
        assert!(link.questions.lock().unwrap()[0].is_none());
    }
    #[tokio::test]
    async fn malformed_clock_and_owner_reset_drop_only_optional_page() {
        let (_d, c, link, spy) = fixture(1, true).await;
        c.tick().await.unwrap();
        assert_eq!(spy.samples.lock().unwrap().len(), 1);
        {
            let mut feed = link.feed.lock().unwrap();
            feed.owner = crate::ids::BuilderId::generate();
            let owner = feed.owner.clone();
            feed.values = vec![sample(&owner)];
        }
        c.tick().await.unwrap();
        assert_eq!(spy.samples.lock().unwrap().len(), 1);
        c.tick().await.unwrap();
        assert_eq!(spy.samples.lock().unwrap().len(), 2);
        let raw = Mutex::new(Vec::new());
        c.follow(
            WorkerId::parse("worker-a").unwrap(),
            link.clone(),
            c.deadline(),
            &raw,
        )
        .await
        .unwrap();
        let mut raw = raw.into_inner().unwrap().pop().unwrap();
        raw.received = raw.sent.saturating_sub(1);
        c.ingest_preparation(vec![raw]);
        assert_eq!(spy.samples.lock().unwrap().len(), 2);
        assert!(c.preparation_cursors.lock().unwrap().is_empty());
        c.tick().await.unwrap();
        assert_eq!(spy.samples.lock().unwrap().len(), 3);
    }
    #[tokio::test]
    async fn malformed_page_identity_sequence_clock_and_bounds_never_change_tick_results() {
        let (_d, c, link, spy) = fixture(1, true).await;
        for variant in 0..11 {
            let pending = Mutex::new(Vec::new());
            let (reached, report) = c
                .follow(
                    WorkerId::parse("worker-a").unwrap(),
                    link.clone(),
                    c.deadline(),
                    &pending,
                )
                .await
                .unwrap();
            assert!(reached.is_some());
            assert_eq!(report, TickReport::default());
            let mut raw = pending.into_inner().unwrap().pop().unwrap();
            let ObservationPage::Available {
                entries,
                cursor,
                pass_complete,
                monotonic_now_ms,
                ..
            } = &mut raw.page
            else {
                panic!()
            };
            match variant {
                0 => entries[0].sequence = 2,
                1 => cursor.upper_sequence = 2,
                2 => {
                    entries[0].observation.binding.owner_generation =
                        crate::ids::BuilderId::generate()
                }
                3 => entries[0].observation.node_count = 0,
                4 => entries[0].observation.policy = "unverified".into(),
                5 => entries[0].observation.boot = "other-native".into(),
                6 => {
                    entries[0].observation.finished_monotonic_ms =
                        monotonic_now_ms.checked_add(1).unwrap()
                }
                7 => cursor.monotonic_now_ms = monotonic_now_ms.checked_add(1).unwrap(),
                8 => *pass_complete = false,
                9 => {
                    let value = entries[0].clone();
                    entries.resize(65, value);
                }
                _ => raw.response_boot = WorkerBootId::parse("other-worker-boot").unwrap(),
            }
            c.ingest_preparation(vec![raw]);
            assert!(spy.samples.lock().unwrap().is_empty());
        }
        assert_eq!(c.tick().await.unwrap(), TickReport::default());
        assert_eq!(spy.samples.lock().unwrap().len(), 1);
    }
}
