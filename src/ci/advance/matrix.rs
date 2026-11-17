//! Durable host for local-ci's matrix driver. A ready action gives the queue
//! its exact inputs. Only a proved stop and closed collection settles a claim.
use super::*;
use local_ci::driver::{
    ActionKind as CoreAction, CoreDecision, Driver, DriverConfig, Input as CoreInput,
    DRIVER_VERSION,
};
use local_ci::wire::Digested;
use serde::{Deserialize, Serialize};

pub(super) fn applies(plan: &FrozenPlan) -> bool {
    plan.tasks
        .iter()
        .any(|task| task.variant.is_some() || task.dynamic_matrix)
}

const MAX_ADMITTED: usize = 4096;

pub(super) fn driver(plan: &FrozenPlan) -> anyhow::Result<Driver> {
    Driver::new(
        plan,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: MAX_ADMITTED,
        },
    )
}

/// References to receipts rather than their logs and job outputs. The host
/// verifies the collected bytes again when reconstructing the SDK transition.
#[derive(Serialize, Deserialize)]
enum Event {
    Advance,
    Cancel,
    Claim {
        task: String,
        token: String,
    },
    Dispatched {
        task: String,
        token: String,
    },
    Settled {
        task: String,
        token: String,
        attempt: AttemptId,
        receipt: Digest,
    },
    Fenced {
        task: String,
        token: String,
    },
    Ack {
        action: String,
    },
}

#[derive(Serialize, Deserialize)]
struct Record {
    event: Event,
    transition_hash: Digest,
}

struct Claim {
    token: String,
    inputs: TaskInputs,
    dispatched: bool,
}

struct Journal<'a> {
    driver: Driver,
    claims: BTreeMap<String, Claim>,
    store: &'a CoordinatorStore,
    results: &'a Path,
    run: &'a RunId,
    authorization: u64,
}

impl Event {
    fn input(&self, results: &Path) -> Result<CoreInput, Fault> {
        Ok(match self {
            Self::Advance => CoreInput::Advance,
            Self::Cancel => CoreInput::Cancel,
            Self::Claim { task, token } => CoreInput::Claim {
                task: task.clone(),
                token: token.clone(),
            },
            Self::Dispatched { task, token } => CoreInput::MarkDispatched {
                task: task.clone(),
                token: token.clone(),
            },
            Self::Settled {
                task,
                token,
                attempt,
                receipt,
            } => {
                let result = read_result(results, attempt, receipt).map_err(unrunnable)?;
                CoreInput::Settle {
                    task: task.clone(),
                    token: token.clone(),
                    result_digest: result.digest().map_err(unrunnable)?,
                    result: Box::new(result),
                }
            }
            Self::Fenced { task, token } => CoreInput::Fence {
                task: task.clone(),
                token: token.clone(),
            },
            Self::Ack { action } => CoreInput::Ack {
                action: action.clone(),
            },
        })
    }
}

impl<'a> Journal<'a> {
    fn open(
        store: &'a CoordinatorStore,
        results: &'a Path,
        run: &'a RunId,
        plan: &FrozenPlan,
        state_dir: &Path,
    ) -> Result<Self, Fault> {
        let driver = if plan.tasks.iter().any(|t| t.dynamic_matrix) {
            let source = crate::source::SourceStore::open(state_dir)
                .map_err(|e| Fault::Transient(e.to_string()))?;
            let bound = source
                .source_of_run(run)
                .map_err(|e| Fault::Transient(e.to_string()))?;
            if bound.as_deref() != Some(&plan.source.source_id) {
                return Err(unrunnable(
                    "the matrix plan does not belong to the run's retained source",
                ));
            }
            let manifest = source
                .manifest(&plan.source.source_id)
                .map_err(|e| Fault::Transient(e.to_string()))?
                .ok_or_else(|| unrunnable("the matrix evaluation source is missing"))?;
            source
                .object_store()
                .map_err(|e| Fault::Transient(e.to_string()))?
                .with_sdk(|objects| {
                    let pinned =
                        local_ci::source_execute::PinnedExecution::new(plan, &manifest, objects)?;
                    let verified = pinned.evaluation_source()?;
                    Driver::with_evaluation(
                        plan,
                        DriverConfig {
                            schema_version: DRIVER_VERSION,
                            max_parallel: MAX_ADMITTED,
                        },
                        &verified,
                        crate::ci::execute::portable_facts(),
                    )
                })
                .map_err(unrunnable)?
        } else {
            driver(plan).map_err(unrunnable)?
        };
        let authorization = store.ci_driver_authorization(run).map_err(transient)?;
        let mut journal = Self {
            driver,
            claims: BTreeMap::new(),
            store,
            results,
            run,
            authorization,
        };
        while let Some(text) = store
            .ci_driver_record(run, authorization, journal.driver.revision() + 1)
            .map_err(transient)?
        {
            let record: Record = serde_json::from_str(&text).map_err(unrunnable)?;
            journal.step(record.event, Some(record.transition_hash))?;
        }
        if journal.driver.revision() == 0 {
            journal.step(Event::Advance, None)?;
        }
        Ok(journal)
    }

    fn step(&mut self, event: Event, replay_hash: Option<Digest>) -> Result<(), Fault> {
        let transition = self
            .driver
            .propose(event.input(self.results)?)
            .map_err(unrunnable)?;
        let hash = Digest::of_bytes(&serde_json::to_vec(&transition).map_err(unrunnable)?);
        let claim = if let Event::Claim { task, token } = &event {
            let inputs = self
                .driver
                .pending_actions()
                .iter()
                .find_map(|action| match &action.kind {
                    CoreAction::Ready {
                        task: ready,
                        inputs,
                    } if ready == task => Some(inputs.clone()),
                    CoreAction::DerivedReady { dispatch, inputs } if dispatch.task_id() == task => {
                        Some(inputs.clone())
                    }
                    _ => None,
                })
                .ok_or_else(|| unrunnable("matrix task was not ready at claim"))?;
            Some((
                task.clone(),
                Claim {
                    token: token.clone(),
                    inputs,
                    dispatched: false,
                },
            ))
        } else {
            None
        };
        let record = Record {
            event,
            transition_hash: hash,
        };
        if let Some(expected) = replay_hash {
            if expected != record.transition_hash {
                return Err(unrunnable("matrix transition changed during replay"));
            }
        } else {
            let text = serde_json::to_string(&record).map_err(unrunnable)?;
            if !self
                .store
                .append_ci_driver_record(self.run, self.authorization, transition.revision(), text)
                .map_err(transient)?
            {
                return Err(Fault::Transient(
                    "matrix progression advanced concurrently".into(),
                ));
            }
        }
        self.driver.apply(&transition).map_err(unrunnable)?;
        match record.event {
            Event::Dispatched { task, .. } => {
                self.claims
                    .get_mut(&task)
                    .ok_or_else(|| unrunnable("unknown dispatched claim"))?
                    .dispatched = true;
            }
            Event::Settled { task, .. } | Event::Fenced { task, .. } => {
                self.claims.remove(&task);
            }
            _ => {}
        }
        if let Some((task, claim)) = claim {
            self.claims.insert(task, claim);
        }
        Ok(())
    }
}

pub(super) fn advance(
    store: &CoordinatorStore,
    state_dir: &Path,
    results: &Path,
    run: &RunId,
    plan: &FrozenPlan,
    members: &BTreeMap<String, Member>,
    project: Option<&str>,
    now_ms: u64,
    pass: &mut Pass,
) -> Result<(), Fault> {
    let mut journal = Journal::open(store, results, run, plan, state_dir)?;
    if !journal.driver.cancelled()
        && store
            .ci_run_cancelled(run, journal.authorization)
            .map_err(transient)?
    {
        journal.step(Event::Cancel, None)?;
    }
    let member = |task: &str| {
        members
            .get(task)
            .ok_or_else(|| unrunnable(format!("matrix task {task} is missing from the run")))
    };
    let fail_fast_reason = |m: &Member| {
        if m.spec.ci.as_ref().is_some_and(|ci| ci.matrix.is_some()) {
            MATRIX_FAIL_FAST_REASON
        } else {
            crate::ci::shards::FAIL_FAST_REASON
        }
    };
    // Finish old owners first. Unknown/retiring attempts and open collection keep their claims.
    for task in journal.claims.keys().cloned().collect::<Vec<_>>() {
        let m = member(&task)?;
        let claim = &journal.claims[&task];
        let token = claim.token.clone();
        if m.spec.ci.as_ref().is_some_and(|ci| ci.hold) && !m.row.cancelled {
            release(store, run, m, &claim.inputs, now_ms, pass).map_err(transient)?;
        }
        if m.row.attempt.is_some() && !claim.dispatched {
            journal.step(
                Event::Dispatched {
                    task: task.clone(),
                    token: token.clone(),
                },
                None,
            )?;
        }
        let state = m
            .row
            .attempt
            .as_ref()
            .map(|a| (a.termination.as_ref(), a.collection.as_deref()));
        match standing(m.row.cancelled, state) {
            Standing::Open => continue,
            Standing::NeverRan => journal.step(Event::Fenced { task, token }, None)?,
            Standing::Ended(_) => {
                let attempt = &m.row.attempt.as_ref().expect("ended attempt").attempt_id;
                let receipt = store
                    .persisted_artifacts(attempt)
                    .map_err(transient)?
                    .into_iter()
                    .find(|(name, _)| name == RESULT_ARTIFACT);
                if let Some((_, receipt)) = receipt
                    .filter(|(_, d)| read_result(results, attempt, d).is_ok_and(|r| r.task == task))
                {
                    journal.step(
                        Event::Settled {
                            task,
                            token,
                            attempt: attempt.clone(),
                            receipt,
                        },
                        None,
                    )?;
                } else {
                    journal.step(Event::Fenced { task, token }, None)?;
                }
            }
        }
    }
    // Materialize complete membership in one transaction. On the next pass
    // the durable task ids are available as ownership tokens.
    let mut expansions: BTreeMap<String, Vec<(TaskId, serde_json::Value)>> = BTreeMap::new();
    for action in journal.driver.pending_actions() {
        let CoreAction::DerivedReady { dispatch, inputs } = &action.kind else {
            continue;
        };
        let origin = member(dispatch.origin())?;
        if origin
            .row
            .spec
            .pointer("/ci/expanded_for")
            .and_then(|v| v.as_u64())
            == Some(journal.authorization)
        {
            continue;
        }
        let t = plan
            .task(dispatch.origin())
            .ok_or_else(|| unrunnable("unknown dynamic matrix origin"))?;
        let job = plan
            .workflows
            .iter()
            .find(|w| w.file == t.workflow)
            .and_then(|w| w.job(&t.jobs[0]))
            .ok_or_else(|| unrunnable("missing matrix job"))?;
        let mut spec = origin.spec.clone();
        let ci = spec
            .ci
            .as_mut()
            .ok_or_else(|| unrunnable("matrix origin has no CI spec"))?;
        ci.dynamic_origin = false;
        ci.core_outcome = None;
        ci.task.task_id = dispatch.task_id().to_string();
        let demand = local_ci::plan_task_demand(plan, dispatch.origin()).map_err(unrunnable)?;
        ci.history_keys = crate::ci::timing::keys(
            &demand,
            &crate::ci::timing::task_scope(plan, &ci.task.task_id),
        );
        ci.task.inputs = serde_json::to_value(inputs).map_err(unrunnable)?;
        ci.task.dispatch = Some(serde_json::to_value(dispatch).map_err(unrunnable)?);
        ci.matrix = Some(crate::coordinator::MatrixAdmission {
            group: dispatch.origin().to_string(),
            max_parallel: u32::try_from(job.max_parallel.unwrap_or(MAX_ADMITTED))
                .ok()
                .and_then(std::num::NonZeroU32::new)
                .ok_or_else(|| unrunnable("invalid matrix limit"))?,
        });
        expansions
            .entry(dispatch.origin().to_string())
            .or_default()
            .push((
                TaskId::generate(),
                serde_json::to_value(spec).map_err(unrunnable)?,
            ));
    }
    if !expansions.is_empty() {
        for (origin, children) in expansions {
            if member(&origin)?.row.cancelled {
                return Err(unrunnable("dynamic matrix cancelled before expansion"));
            }
            store
                .expand_ci_matrix(
                    run,
                    &member(&origin)?.row.task_id,
                    journal.authorization,
                    children,
                    now_ms,
                )
                .map_err(|e| match e {
                    crate::storage::StorageError::Immutable(_)
                    | crate::storage::StorageError::Invalid(_) => unrunnable(e),
                    _ => transient(e),
                })?;
        }
        return Ok(());
    }
    for action in journal.driver.pending_actions().to_vec() {
        let kind = match action.kind {
            CoreAction::DerivedReady { dispatch, inputs } => {
                let m = member(dispatch.task_id())?;
                if m.spec.ci.as_ref().and_then(|ci| ci.task.dispatch.as_ref())
                    != Some(&serde_json::to_value(&dispatch).map_err(unrunnable)?)
                {
                    return Err(unrunnable("dynamic matrix row names another dispatch"));
                }
                CoreAction::Ready {
                    task: dispatch.task_id().to_string(),
                    inputs,
                }
            }
            other => other,
        };
        match kind {
            CoreAction::Ready { task, inputs } => {
                let m = member(&task)?;
                if journal.driver.admitted() >= MAX_ADMITTED {
                    continue;
                }
                if let Some(group) = m.spec.ci.as_ref().and_then(|ci| ci.matrix.as_ref()) {
                    let admitted = journal
                        .claims
                        .keys()
                        .filter(|id| {
                            members
                                .get(*id)
                                .and_then(|m| m.spec.ci.as_ref()?.matrix.as_ref())
                                .is_some_and(|m| m.group == group.group)
                        })
                        .count();
                    if admitted >= group.max_parallel.get() as usize {
                        continue;
                    }
                }
                journal.step(
                    Event::Claim {
                        task: task.clone(),
                        token: m.row.task_id.to_string(),
                    },
                    None,
                )?;
                if !m.row.cancelled && m.spec.ci.as_ref().is_some_and(|ci| ci.hold) {
                    release(store, run, m, &inputs, now_ms, pass).map_err(transient)?;
                }
            }
            CoreAction::Cancel { task, .. } => {
                stop(
                    store,
                    run,
                    &member(&task)?.row,
                    fail_fast_reason(member(&task)?),
                    now_ms,
                    pass,
                )
                .map_err(transient)?;
                journal.step(Event::Ack { action: action.id }, None)?;
            }
            CoreAction::Finished => {}
            CoreAction::DerivedReady { .. } => {
                return Err(unrunnable("derived matrix task is missing its dispatch"))
            }
        }
    }
    if journal.driver.infrastructure_failure().is_some() {
        return Err(unrunnable(
            "the matrix driver exhausted its bounded result storage",
        ));
    }
    for (task, m) in members {
        if matches!(
            journal.driver.core_decision(task),
            Some((CoreDecision::UnstartedSkipped, Some("fail-fast")))
        ) {
            stop(store, run, &m.row, fail_fast_reason(m), now_ms, pass).map_err(transient)?;
        }
    }
    for group in plan.shard_groups.iter().filter(|g| g.split) {
        if let Some(join) = journal.driver.shard_join(&group.id) {
            let mut observations = Vec::new();
            let verified = group
                .shards
                .iter()
                .enumerate()
                .map(|(index, task)| {
                    let Some(m) = members.get(task) else {
                        return Ok(false);
                    };
                    let result = ended(store, results, task, &m.row).map_err(transient)?;
                    Ok(match result {
                        Some(Input::Settle { result, .. }) => {
                            let verified = crate::ci::shards::verified(
                                group,
                                index,
                                &result.outcomes,
                                result.shard.as_ref(),
                            );
                            if verified && join.outcome.status == JobStatus::Success {
                                if let (Some(receipt), Some(attempt)) =
                                    (&result.shard, &m.row.attempt)
                                {
                                    observations.push((
                                        attempt.worker.clone(),
                                        crate::ci::progress::receipt_timings(receipt),
                                    ));
                                }
                            }
                            verified
                        }
                        _ => false,
                    })
                })
                .collect::<Result<Vec<_>, Fault>>()?;
            let rerun = crate::ci::shards::rerun(group, &verified, join.outcome.status);
            let failure = (join.outcome.status == JobStatus::Failure).then(|| {
                join.diagnostic
                    .as_deref()
                    .unwrap_or("a shard of the job failed")
            });
            if store
                .join_ci_shards_with_timings(
                    run,
                    &group.id,
                    failure,
                    &rerun,
                    now_ms,
                    group_timing_scope(plan, group, project).map(|scope| (scope, observations)),
                )
                .map_err(transient)?
            {
                pass.joined.push(group.id.clone());
            }
        }
    }
    for origin in plan.tasks.iter().filter(|t| t.dynamic_matrix) {
        if let Some(outcome) = journal.driver.outcome(&origin.workflow, &origin.jobs[0]) {
            let failure =
                (outcome.status == JobStatus::Failure).then_some("a dynamic matrix variant failed");
            store
                .join_ci_shards(run, &origin.id, failure, &[], now_ms)
                .map_err(transient)?;
            if store
                .complete_ci_origin(
                    run,
                    &member(&origin.id)?.row.task_id,
                    journal.authorization,
                    outcome.status.as_str(),
                    now_ms,
                )
                .map_err(transient)?
            {
                pass.joined.push(origin.id.clone());
            }
        }
    }
    for group in &plan.graph.nodes {
        if !matches!(group.kind, local_ci::dag::NodeKind::MatrixGroup { .. }) {
            continue;
        }
        if let Some(result) = journal.driver.outcome(&group.workflow, &group.job) {
            let failure =
                (result.status == JobStatus::Failure).then_some("a matrix variant failed");
            if store
                .join_ci_shards(run, &group.id, failure, &[], now_ms)
                .map_err(transient)?
            {
                pass.joined.push(group.id.clone());
            }
        }
    }
    Ok(())
}
