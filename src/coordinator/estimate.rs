//! Preview gathers only read operations; it never enters the placement actor.
use super::*;
use crate::error::ApiError;
use crate::ids::{Digest, NodeId};
/// A worker a preview may compare: it was inspected, takes the task and has
/// room. Nothing here was written anywhere.
struct Reachable {
    worker_id: WorkerId,
    link: Arc<dyn WorkerLink>,
    boot: WorkerBootId,
    epoch: AgentEpoch,
    load: u32,
    cost_query: scheduler::measured::CostQuery,
    costs: decide::Costs,
}
/// What a worker said to the forecast question of a preview.
enum Said {
    Answer(Instant, Box<crate::wire::ForecastResponse>),
    /// It answered with an error: compared without a number, as a placement
    /// compares a ready worker that refused the question.
    Refused,
    Silent,
}
type LookFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<Reachable, Excluded>> + Send + 'a>>;
type SaidFuture<'a> = Pin<Box<dyn Future<Output = Said> + Send + 'a>>;
impl Coordinator {
    pub async fn estimate(
        &self,
        query: crate::service::estimate::Query,
    ) -> Result<crate::service::estimate::Estimate, ApiError> {
        query.check()?;
        self.preview(query, None).await
    }

    pub async fn estimate_task(
        &self,
        task_id: TaskId,
    ) -> Result<crate::service::estimate::Estimate, ApiError> {
        let input = crate::service::estimate::frozen_input(self.store(), &task_id)?;
        let spec: TaskSpec = serde_json::from_value(input.spec.clone())
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        self.preview(
            crate::service::estimate::Query {
                version: 1,
                argv: vec![],
                repository_id: None,
                cwd_relative: String::new(),
                requirements: spec.requirements,
            },
            Some(input),
        )
        .await
    }

    async fn preview(
        &self,
        query: crate::service::estimate::Query,
        input: Option<QueuedTask>,
    ) -> Result<crate::service::estimate::Estimate, ApiError> {
        let spec = input
            .as_ref()
            .map(|input| serde_json::from_value::<TaskSpec>(input.spec.clone()))
            .transpose()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        let parent_local = self
            .parent_local
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let invalid = |e: CoordinatorError| ApiError::new(ErrorCode::Internal, e.to_string());
        let workers: Vec<_> = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, link)| (id.clone(), link.clone()))
            .collect();
        let ids: Vec<_> = workers.iter().map(|(id, _)| id.clone()).collect();
        let (loads, pending) = self
            .db(move |s| {
                let mut loads = BTreeMap::new();
                let mut pending = BTreeMap::new();
                for id in ids {
                    loads.insert(id.clone(), s.worker_load(&id)?);
                    pending.insert(id.clone(), scheduler::pending_plans(s, &id)?);
                }
                Ok((loads, pending))
            })
            .await
            .map_err(invalid)?;
        // The workers this task is not placed on again; a read of the queue.
        let refused = match &input {
            Some(input) => self.refused_by(&input.task_id).await.map_err(invalid)?,
            None => BTreeSet::new(),
        };
        let plan_digest = match &spec {
            Some(spec) => spec.plan_digest.clone(),
            None => Digest::of_bytes(
                &serde_json::to_vec(&query)
                    .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?,
            ),
        };
        let attempt = AttemptId::generate();
        let preparation_profiles = Mutex::new(BTreeMap::new());
        let looks: Vec<LookFuture<'_>> = workers
            .into_iter()
            .map(|(worker_id, link)| {
                let preparation_profiles = &preparation_profiles;
                let query = &query;
                let loads = &loads;
                let refused = &refused;
                let input = input.as_ref();
                let spec = spec.as_ref();
                let parent_local = &parent_local;
                let look = async move {
                    let out = |worker_id, why, unmet| Excluded {
                        worker_id,
                        why,
                        unmet,
                    };
                    let response = tokio::time::timeout(
                        self.sched.forecast_wait(),
                        link.read_only_call(Request::Inspect(crate::wire::InspectRequest {
                            preparation_observations: None,
                            schema_version: SchemaVersion::CURRENT,
                            request_id: RequestId::generate(),
                        })),
                    )
                    .await;
                    let Ok(Ok(Response::Inspect(inspect))) = response else {
                        return Err(out(worker_id, Exclusion::Unreached, vec![]));
                    };
                    if spec.is_some() && inspect.worker_id == worker_id {
                        if let Some(cap) = inspect.capabilities.as_ref() {
                            let mut profiles = preparation_profiles
                                .lock()
                                .unwrap_or_else(|e| e.into_inner());
                            if profiles.len() < 256 {
                                profiles.insert(
                                    worker_id.clone(),
                                    (
                                        cap.profile_fingerprint.clone(),
                                        inspect.worker_boot_id.clone(),
                                        inspect.agent_epoch,
                                        Arc::clone(&link),
                                    ),
                                );
                            }
                        }
                    }
                    let bundle = spec.and_then(|spec| spec.toolchains.as_ref());
                    let mut bundle_valid = bundle.is_none();
                    if let Some(bundle) = bundle {
                        let request =
                            Request::ResolveToolchains(crate::wire::ResolveToolchainsRequest {
                                schema_version: SchemaVersion::CURRENT,
                                request_id: RequestId::generate(),
                                bundle: bundle.clone(),
                                requirements: query.requirements.clone(),
                            });
                        let response = tokio::time::timeout(
                            self.sched.forecast_wait(),
                            link.read_only_call(request),
                        )
                        .await;
                        bundle_valid = matches!(
                            response,
                            Ok(Ok(Response::ResolveToolchains(ref resolved)))
                                if resolved.bundle == *bundle
                        );
                    }
                    let mut unmet = crate::toolchain::capability::missing_with_installable(
                        &query.requirements,
                        inspect.capabilities.as_ref(),
                        bundle.filter(|_| bundle_valid),
                    );
                    unmet = crate::toolchain::envdiff::with_steps(
                        unmet,
                        &query.requirements.requires,
                        inspect.capabilities.as_ref(),
                    );
                    if !bundle_valid {
                        unmet.push(
                            "stored frozen toolchains are not proven eligible on this worker"
                                .into(),
                        );
                    }
                    if spec.is_some_and(|spec| spec.in_place)
                        && parent_local.as_ref() != Some(&worker_id)
                    {
                        unmet.push("in-place task requires the parent-local worker".into());
                    }
                    if spec
                        .is_some_and(|spec| policy::lacks_ci_task(spec, link.ci_task_negotiated()))
                    {
                        unmet.push(policy::CI_TASK_UNMET.to_string());
                    }
                    let command_supported = inspect
                        .capabilities
                        .as_ref()
                        .is_some_and(|c| c.features.contains(crate::forecast::COMMAND_FEATURE));
                    if spec.is_none() && !command_supported {
                        unmet.push("forecast_command_v1 unavailable".into());
                    }
                    let load = loads[&worker_id];
                    let limits = self.cfg.limits.stricter(Limits {
                        max_active_task: inspect.max_active_task,
                        prefetch_per_worker: inspect.prefetch_per_worker,
                    });
                    let why = if inspect.worker_id != worker_id {
                        Some(Exclusion::Unreached)
                    } else if !unmet.is_empty() {
                        Some(Exclusion::CapabilityMismatch)
                    } else if inspect.draining {
                        Some(Exclusion::Draining)
                    } else if !policy::has_room(load, limits) {
                        Some(Exclusion::Full)
                    } else if refused.contains(&worker_id) {
                        Some(Exclusion::RefusedTask)
                    } else {
                        None
                    };
                    if let Some(why) = why {
                        return Err(out(worker_id, why, unmet));
                    }
                    let transport = link.cost_transport().filter(|transport| {
                        transport.worker_boot == inspect.worker_boot_id
                            && transport.agent_epoch == inspect.agent_epoch
                    });
                    let mut cost_query = match input {
                        Some(input) => scheduler::measured::CostQuery::task(
                            &input.spec,
                            input.source_id.as_ref().map(|source| source.as_str()),
                            transport,
                        ),
                        None => scheduler::measured::CostQuery {
                            transport,
                            ..Default::default()
                        },
                    };
                    if let (Some(input), Some(spec)) = (input, spec) {
                        let same = |snapshot: &PlacementInputs| {
                            snapshot.identity.as_ref().is_some_and(|(boot, epoch)| {
                                boot == &inspect.worker_boot_id && *epoch == inspect.agent_epoch
                            }) && snapshot.ready.iter().any(|ready| {
                                ready.run_id == input.run_id
                                    && Some(&ready.source_id) == input.source_id.as_ref()
                                    && ready.manifest_digest == spec.manifest_digest
                            })
                        };
                        cost_query.ready = cost_query.transport.is_some()
                            && link.placement_inputs().is_some_and(|s| same(&s));
                    }
                    let measured = self.sched.query(&worker_id, &cost_query);
                    // The same assembly a placement compares by.
                    let costs = match input {
                        Some(input) => scheduler::preview::merge_costs(
                            measured,
                            self.sched.costs(&worker_id, input),
                        ),
                        None => measured,
                    };
                    Ok(Reachable {
                        worker_id,
                        link,
                        boot: inspect.worker_boot_id,
                        epoch: inspect.agent_epoch,
                        load: load.active.saturating_add(load.pending),
                        cost_query,
                        costs,
                    })
                };
                Box::pin(look) as LookFuture<'_>
            })
            .collect();
        let mut excluded = vec![];
        let mut reached = vec![];
        for look in join_all(looks).await {
            match look {
                Ok(worker) => reached.push(worker),
                Err(reason) => excluded.push(reason),
            }
        }
        // As a placement does: what nobody measured for a worker is the worst
        // known figure before the workers are asked, so the question carries
        // the same time before the task is ready there.
        let mut costs: Vec<_> = reached.iter().map(|w| w.costs).collect();
        decide::fill_costs(&mut costs);
        for (worker, costs) in reached.iter_mut().zip(costs) {
            worker.costs = costs;
        }
        let questions: Vec<SaidFuture<'_>> = reached
            .iter()
            .map(|worker| {
                let query = &query;
                let pending = &pending;
                let attempt = &attempt;
                let plan_digest = &plan_digest;
                let spec = spec.as_ref();
                let question = async move {
                    let workload = match spec {
                        Some(spec) => policy::workload_of_spec(
                            attempt,
                            spec,
                            decide::ready_delay_ms(&worker.costs),
                        ),
                        None => scheduler::workload_of(
                            attempt,
                            plan_digest,
                            &[CommandSpec {
                                node_id: NodeId::parse("command").expect("static node"),
                                argv: query.argv.clone(),
                                cwd: query.cwd_relative.clone(),
                                env: BTreeMap::new(),
                                depends_on: vec![],
                                timeout_ms: None,
                                setup_step: None,
                            }],
                            None,
                        ),
                    };
                    let forecast = ForecastRequest {
                        schema_version: SchemaVersion::CURRENT,
                        request_id: RequestId::generate(),
                        coordinator_epoch: self.epoch(),
                        workload,
                        pending: pending.get(&worker.worker_id).cloned().unwrap_or_default(),
                    };
                    let request = if spec.is_some() {
                        Request::Forecast(forecast)
                    } else {
                        Request::ForecastCommand(crate::wire::ForecastCommandRequest {
                            schema_version: SchemaVersion::CURRENT,
                            request_id: RequestId::generate(),
                            coordinator_epoch: self.epoch(),
                            version: 1,
                            repository_id: query.repository_id.clone(),
                            cwd_relative: query.cwd_relative.clone(),
                            argv: query.argv.clone(),
                            forecast,
                        })
                    };
                    // The age of the answer counts from before the question left.
                    let sent = Instant::now();
                    let answer = tokio::time::timeout(
                        self.sched.forecast_wait(),
                        worker.link.read_only_call(request),
                    )
                    .await;
                    match answer {
                        Ok(Ok(Response::Forecast(answer))) if spec.is_some() => {
                            Said::Answer(sent, Box::new(answer))
                        }
                        Ok(Ok(Response::ForecastCommand(answer))) if spec.is_none() => {
                            Said::Answer(sent, Box::new(answer))
                        }
                        Ok(Err(LinkError::Remote(e)))
                            if spec.is_some() && e.code != ErrorCode::StaleEpoch =>
                        {
                            Said::Refused
                        }
                        _ => Said::Silent,
                    }
                };
                Box::pin(question) as SaidFuture<'_>
            })
            .collect();
        let answers = join_all(questions).await;
        let now = Instant::now();
        let mut candidates = vec![];
        let mut forecasts = vec![];
        let mut evidence_queries = BTreeMap::new();
        for (worker, said) in reached.into_iter().zip(answers) {
            let outlook = match said {
                Said::Silent => {
                    excluded.push(Excluded {
                        worker_id: worker.worker_id,
                        why: Exclusion::NoForecast,
                        unmet: vec![],
                    });
                    continue;
                }
                Said::Refused => Outlook::unusable(decide::Unusable::Refused),
                Said::Answer(sent, answer) => {
                    let age = now.saturating_duration_since(sent).as_millis();
                    let outlook = decide::outlook_of(
                        &Asked {
                            worker_id: &worker.worker_id,
                            plan_digest: &plan_digest,
                            boot_id: &worker.boot,
                            agent_epoch: worker.epoch,
                        },
                        &answer,
                        u64::try_from(age).unwrap_or(u64::MAX),
                        self.sched.cfg.rules.forecast_ttl_ms,
                    );
                    forecasts.push(*answer);
                    outlook
                }
            };
            evidence_queries.insert(worker.worker_id.clone(), worker.cost_query);
            candidates.push(Candidate {
                priority: self.sched.priority_of(&worker.worker_id),
                worker_id: worker.worker_id,
                load: worker.load,
                costs: worker.costs,
                outlook,
            });
        }
        let decision = decide::decide(candidates, excluded, self.sched.cfg.rules);
        let components = decision
            .compared
            .iter()
            .filter_map(|compared| {
                let mut costs = self.sched.evidence(
                    &compared.worker_id,
                    evidence_queries.get(&compared.worker_id)?,
                )?;
                costs.fallback(&compared.costs);
                Some(crate::service::estimate::WorkerCostEvidence {
                    worker_id: compared.worker_id.clone(),
                    costs,
                })
            })
            .collect();
        let identity_observed = if let (Some(input), Some(spec), Some(worker)) =
            (input.as_ref(), spec.as_ref(), decision.winner.as_ref())
        {
            let profile = preparation_profiles
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(worker)
                .cloned();
            if let Some((profile, boot, epoch, attachment)) = profile {
                self.query_preparation_identity(
                    worker,
                    input,
                    spec,
                    profile,
                    (boot, epoch),
                    attachment,
                )
                .await
            } else {
                false
            }
        } else {
            false
        };
        let assumptions = if let Some(spec) = &spec {
            let mut notes = vec![
                "hypothetical rerun using stored commands, environment, setup, frozen toolchains and output plan; not remaining runtime".into(),
                "captured source kind is unknown; native runtime history correlation is unproven; fallback values rank candidates only".into(),
                "preview is conditional on current worker state; unobserved setup and additional inputs remain unknown".into(),
            ];
            if identity_observed {
                notes.push("selected worker preparation identity observed; availability, activation and setup delay remain unknown".into());
            }
            if spec.in_place {
                notes.push("in-place workspace content remains mutable; stored specification does not freeze its files".into());
            }
            if spec.has_secret_bundle {
                notes.push("secret input availability and preparation time are unobserved".into());
            }
            notes
        } else {
            vec!["transfer, setup and result collection costs are unknown; no source was captured".into(), "preview is conditional on current worker state and command history; fallback values rank candidates only".into()]
        };
        Ok(crate::service::estimate::Estimate {
            components,
            version: 1,
            online: true,
            source_kind: if spec.is_some() {
                "captured"
            } else {
                "unknown"
            }
            .into(),
            conditional: true,
            decision: Some(decision),
            forecasts,
            assumptions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct ReadOnly {
        calls: AtomicUsize,
    }
    impl WorkerLink for ReadOnly {
        fn call(&self, _: Request) -> super::super::link::LinkFuture<'_> {
            panic!("estimate entered mutation-aware link")
        }
        fn read_only_call(&self, request: Request) -> super::super::link::LinkFuture<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                assert!(matches!(request, Request::Inspect(_)));
                Err(LinkError::Unconfirmed)
            })
        }
    }
    fn rows(c: &Coordinator) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
        c.store
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
                        .query_map([], |row| {
                            (0..columns)
                                .map(|i| row.get(i))
                                .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    all.push((table, rows));
                }
                Ok(all)
            })
            .unwrap()
    }
    #[tokio::test]
    async fn estimate_100_calls_preserve_every_durable_row_epoch_and_actor_state() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        let c = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
        let link = Arc::new(ReadOnly {
            calls: AtomicUsize::new(0),
        });
        c.attach(WorkerId::parse("readonly").unwrap(), link.clone())
            .await;
        let task = TaskId::generate();
        let run = RunId::generate();
        let mut spec = TaskSpec {
            ci: None,
            toolchains: None,
            outputs: vec![],
            requirements: Default::default(),
            in_place: false,
            setup: None,
            plan_digest: Digest::of_bytes(b"plan"),
            manifest_digest: Digest::of_bytes(b"manifest"),
            commands: vec![CommandSpec {
                setup_step: None,
                node_id: NodeId::parse("main").unwrap(),
                argv: vec!["/bin/true".into()],
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
        let mut second = spec.commands[0].clone();
        second.node_id = NodeId::parse("second").unwrap();
        second.depends_on = vec![spec.commands[0].node_id.clone()];
        spec.commands.push(second);
        c.store
            .enqueue(
                &run,
                None,
                &serde_json::json!({}),
                &[(task.clone(), serde_json::to_value(spec).unwrap())],
                1,
            )
            .unwrap();
        c.store.close_run(&run, 2).unwrap();
        let before = rows(&c);
        let epoch = c.epoch();
        let lease_seq = c.lease_seq.load(Ordering::SeqCst);
        let _actor = c.actor.lock().await;
        for _ in 0..100 {
            let query = crate::service::estimate::Query {
                version: 1,
                argv: vec!["echo".into(), "hello".into()],
                repository_id: None,
                cwd_relative: String::new(),
                requirements: Default::default(),
            };
            let answer = tokio::time::timeout(Duration::from_secs(1), c.estimate(query))
                .await
                .unwrap()
                .unwrap();
            assert!(answer.online && answer.conditional);
            assert!(answer.decision.unwrap().winner.is_none());
            let known = tokio::time::timeout(Duration::from_secs(1), c.estimate_task(task.clone()))
                .await
                .unwrap()
                .unwrap();
            assert!(known.online && known.conditional);
        }
        assert_eq!(link.calls.load(Ordering::SeqCst), 200);
        assert_eq!(before, rows(&c));
        assert_eq!(epoch, c.epoch());
        assert_eq!(lease_seq, c.lease_seq.load(Ordering::SeqCst));
    }
}
