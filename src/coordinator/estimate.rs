//! Preview gathers only read operations; it never enters the placement actor.
use super::*;
use crate::error::ApiError;
use crate::ids::{Digest, NodeId};
type PreviewFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = std::result::Result<
                    (
                        Candidate,
                        crate::wire::ForecastResponse,
                        Instant,
                        WorkerBootId,
                        AgentEpoch,
                        scheduler::measured::CostQuery,
                    ),
                    Excluded,
                >,
            > + Send
            + 'a,
    >,
>;
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
                for id in ids {
                    loads.insert(id.clone(), s.worker_load(&id)?);
                }
                Ok((loads, s.unacked_demand()?))
            })
            .await
            .map_err(invalid)?;
        let mut excluded = vec![];
        let mut candidates = vec![];
        let mut forecasts = vec![];
        let mut evidence_queries = BTreeMap::new();
        let plan_digest = match &spec {
            Some(spec) => spec.plan_digest.clone(),
            None => Digest::of_bytes(
                &serde_json::to_vec(&query)
                    .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?,
            ),
        };
        let attempt = AttemptId::generate();
        let questions = workers
            .into_iter()
            .map(|(worker_id, link)| {
                let query = &query;
                let loads = &loads;
                let pending = &pending;
                let attempt = &attempt;
                let plan_digest = &plan_digest;
                let input = input.as_ref();
                let spec = spec.as_ref();
                let parent_local = &parent_local;
                let question = async move {
                    let response = tokio::time::timeout(
                        Duration::from_millis(self.sched.cfg.forecast_wait_ms),
                        link.read_only_call(Request::Inspect(crate::wire::InspectRequest {
                            preparation_observations: None,
                            schema_version: SchemaVersion::CURRENT,
                            request_id: RequestId::generate(),
                        })),
                    )
                    .await;
                    let Ok(Ok(Response::Inspect(inspect))) = response else {
                        return Err(Excluded {
                            worker_id,
                            why: Exclusion::Unreached,
                            unmet: vec![],
                        });
                    };
                    let bundle = spec.and_then(|spec| spec.toolchains.as_ref());
                    let mut bundle_valid = bundle.is_none();
                    if let Some(bundle) = bundle {
                        let response = tokio::time::timeout(
                            Duration::from_millis(self.sched.cfg.forecast_wait_ms),
                            link.read_only_call(Request::ResolveToolchains(crate::wire::ResolveToolchainsRequest {
                                schema_version: SchemaVersion::CURRENT,
                                request_id: RequestId::generate(),
                                bundle: bundle.clone(),
                                requirements: query.requirements.clone(),
                            })),
                        ).await;
                        bundle_valid = matches!(response, Ok(Ok(Response::ResolveToolchains(ref resolved))) if resolved.bundle == *bundle);
                    }
                    let mut unmet = crate::toolchain::capability::missing_with_installable(
                        &query.requirements,
                        inspect.capabilities.as_ref(),
                        bundle.filter(|_| bundle_valid),
                    );
                    if !bundle_valid {
                        unmet.push("stored frozen toolchains are not proven eligible on this worker".into());
                    }
                    if spec.is_some_and(|spec| spec.in_place) && parent_local.as_ref() != Some(&worker_id) {
                        unmet.push("in-place task requires the parent-local worker".into());
                    }
                    let command_supported = inspect
                        .capabilities
                        .as_ref()
                        .is_some_and(|c| c.features.contains(crate::forecast::COMMAND_FEATURE));
                    if spec.is_none() && !command_supported {
                        unmet.push("forecast_command_v1 unavailable".into());
                    }
                    let load = loads[&worker_id];
                    let why = if inspect.worker_id != worker_id {
                        Some(Exclusion::Unreached)
                    } else if !unmet.is_empty() {
                        Some(Exclusion::CapabilityMismatch)
                    } else if inspect.draining {
                        Some(Exclusion::Draining)
                    } else if !policy::has_room(
                        load,
                        self.cfg.limits.stricter(Limits {
                            max_active_task: inspect.max_active_task,
                            prefetch_per_worker: inspect.prefetch_per_worker,
                        }),
                    ) {
                        Some(Exclusion::Full)
                    } else {
                        None
                    };
                    if let Some(why) = why {
                        return Err(Excluded {
                            worker_id,
                            why,
                            unmet,
                        });
                    }
                    let transport = link.cost_transport().filter(|transport| {
                        transport.worker_boot == inspect.worker_boot_id && transport.agent_epoch == inspect.agent_epoch
                    });
                    let mut cost_query = match input {
                        Some(input) => scheduler::measured::CostQuery::task(
                            &input.spec, input.source_id.as_ref().map(|source| source.as_str()), transport,
                        ),
                        None => scheduler::measured::CostQuery { transport, ..Default::default() },
                    };
                    if let (Some(input), Some(spec)) = (input, spec) {
                        cost_query.ready = cost_query.transport.is_some() && link.placement_inputs().is_some_and(|snapshot| {
                            snapshot.identity.as_ref().is_some_and(|(boot, epoch)| boot == &inspect.worker_boot_id && *epoch == inspect.agent_epoch)
                            && snapshot.ready.iter().any(|ready| ready.run_id == input.run_id
                                && Some(&ready.source_id) == input.source_id.as_ref()
                                && ready.manifest_digest == spec.manifest_digest)
                        });
                    }
                    let costs = self.sched.query(&worker_id, &cost_query);
                    let workload = if let Some(spec) = spec {
                        scheduler::workload_of(attempt, plan_digest, &spec.commands, decide::ready_delay_ms(&costs))
                    } else { scheduler::workload_of(
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
                    ) };
                    let forecast = ForecastRequest {
                        schema_version: SchemaVersion::CURRENT,
                        request_id: RequestId::generate(),
                        coordinator_epoch: self.epoch(),
                        workload,
                        pending: scheduler::pending_of(pending, &worker_id),
                    };
                    let request = if spec.is_some() {
                        Request::Forecast(forecast)
                    } else { Request::ForecastCommand(crate::wire::ForecastCommandRequest {
                        schema_version: SchemaVersion::CURRENT,
                        request_id: RequestId::generate(),
                        coordinator_epoch: self.epoch(),
                        version: 1,
                        repository_id: query.repository_id.clone(),
                        cwd_relative: query.cwd_relative.clone(),
                        argv: query.argv.clone(),
                        forecast,
                    }) };
                    let sent = Instant::now();
                    let answer = tokio::time::timeout(
                        Duration::from_millis(self.sched.cfg.forecast_wait_ms),
                        link.read_only_call(request),
                    )
                    .await;
                    let answer = match answer {
                        Ok(Ok(Response::Forecast(answer))) if spec.is_some() => answer,
                        Ok(Ok(Response::ForecastCommand(answer))) if spec.is_none() => answer,
                        _ => return Err(Excluded { worker_id, why: Exclusion::NoForecast, unmet: vec![] }),
                    };
                    let outlook = decide::outlook_of(
                        &Asked {
                            worker_id: &worker_id,
                            plan_digest,
                            boot_id: &inspect.worker_boot_id,
                            agent_epoch: inspect.agent_epoch,
                        },
                        &answer,
                        u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX),
                        self.sched.cfg.rules.forecast_ttl_ms,
                    );
                    Ok((
                        Candidate {
                            worker_id: worker_id.clone(),
                            priority: self.sched.priority_of(&worker_id),
                            load: load.active.saturating_add(load.pending),
                            costs,
                            outlook,
                        },
                        answer,
                        sent,
                        inspect.worker_boot_id,
                        inspect.agent_epoch,
                        cost_query,
                    ))
                };
                Box::pin(question) as PreviewFuture<'_>
            })
            .collect();
        for answer in join_all(questions).await {
            match answer {
                Ok((mut candidate, forecast, sent, boot, epoch, cost_query)) => {
                    evidence_queries.insert(candidate.worker_id.clone(), cost_query);
                    candidate.outlook = decide::outlook_of(
                        &Asked {
                            worker_id: &candidate.worker_id,
                            plan_digest: &plan_digest,
                            boot_id: &boot,
                            agent_epoch: epoch,
                        },
                        &forecast,
                        u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX),
                        self.sched.cfg.rules.forecast_ttl_ms,
                    );
                    candidates.push(candidate);
                    forecasts.push(forecast);
                }
                Err(reason) => excluded.push(reason),
            }
        }
        let mut costs: Vec<_> = candidates.iter().map(|c| c.costs).collect();
        decide::fill_costs(&mut costs);
        for (candidate, costs) in candidates.iter_mut().zip(costs) {
            candidate.costs = costs;
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
        let assumptions = if let Some(spec) = &spec {
            let mut notes = vec![
                "hypothetical rerun using stored commands, environment, setup, frozen toolchains and output plan; not remaining runtime".into(),
                "captured source kind is unknown; native runtime history correlation is unproven; fallback values rank candidates only".into(),
                "preview is conditional on current worker state; unobserved setup and additional inputs remain unknown".into(),
            ];
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
        let dir = tempfile::tempdir().unwrap();
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
