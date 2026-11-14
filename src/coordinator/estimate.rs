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
        let plan_digest = Digest::of_bytes(
            &serde_json::to_vec(&query)
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?,
        );
        let attempt = AttemptId::generate();
        let questions = workers
            .into_iter()
            .map(|(worker_id, link)| {
                let query = &query;
                let loads = &loads;
                let pending = &pending;
                let attempt = &attempt;
                let plan_digest = &plan_digest;
                let question = async move {
                    let response = tokio::time::timeout(
                        Duration::from_millis(self.sched.cfg.forecast_wait_ms),
                        link.read_only_call(Request::Inspect(crate::wire::InspectRequest {
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
                    let mut unmet = crate::toolchain::capability::missing(
                        &query.requirements,
                        inspect.capabilities.as_ref(),
                    );
                    let command_supported = inspect
                        .capabilities
                        .as_ref()
                        .is_some_and(|c| c.features.contains(crate::forecast::COMMAND_FEATURE));
                    if !command_supported {
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
                    let workload = scheduler::workload_of(
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
                    );
                    let forecast = ForecastRequest {
                        schema_version: SchemaVersion::CURRENT,
                        request_id: RequestId::generate(),
                        coordinator_epoch: self.epoch(),
                        workload,
                        pending: scheduler::pending_of(pending, &worker_id),
                    };
                    let request = crate::wire::ForecastCommandRequest {
                        schema_version: SchemaVersion::CURRENT,
                        request_id: RequestId::generate(),
                        coordinator_epoch: self.epoch(),
                        version: 1,
                        repository_id: query.repository_id.clone(),
                        cwd_relative: query.cwd_relative.clone(),
                        argv: query.argv.clone(),
                        forecast,
                    };
                    let sent = Instant::now();
                    let answer = tokio::time::timeout(
                        Duration::from_millis(self.sched.cfg.forecast_wait_ms),
                        link.read_only_call(Request::ForecastCommand(request)),
                    )
                    .await;
                    let Ok(Ok(Response::ForecastCommand(answer))) = answer else {
                        return Err(Excluded {
                            worker_id,
                            why: Exclusion::NoForecast,
                            unmet: vec![],
                        });
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
                            costs: self.sched.query(
                                &worker_id,
                                &scheduler::measured::CostQuery {
                                    transport: link.cost_transport(),
                                    ..Default::default()
                                },
                            ),
                            outlook,
                        },
                        answer,
                        sent,
                        inspect.worker_boot_id,
                        inspect.agent_epoch,
                    ))
                };
                Box::pin(question) as PreviewFuture<'_>
            })
            .collect();
        for answer in join_all(questions).await {
            match answer {
                Ok((mut candidate, forecast, sent, boot, epoch)) => {
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
                    &scheduler::measured::CostQuery::default(),
                )?;
                costs.fallback(&compared.costs);
                Some(crate::service::estimate::WorkerCostEvidence {
                    worker_id: compared.worker_id.clone(),
                    costs,
                })
            })
            .collect();
        Ok(crate::service::estimate::Estimate {components,version:1,online:true,source_kind:"unknown".into(),conditional:true,decision:Some(decision),forecasts,assumptions:vec!["transfer, setup and result collection costs are unknown; no source was captured".into(),"preview is conditional on current worker state and command history; fallback values rank candidates only".into()]})
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
        }
        assert_eq!(link.calls.load(Ordering::SeqCst), 100);
        assert_eq!(before, rows(&c));
        assert_eq!(epoch, c.epoch());
        assert_eq!(lease_seq, c.lease_seq.load(Ordering::SeqCst));
    }
}
