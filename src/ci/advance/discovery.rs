//! Durable transition from discovery attempts to a fully partitioned run.
use super::*;
use crate::ci::discovery::{apply, DiscoveryStep, WorkerListing};
use local_ci::shard::{adapters::Isolation, PartitionOpts};

fn metadata(row: &CiTaskRow) -> Option<&serde_json::Value> {
    row.spec.pointer("/ci/task/inputs/rtr_discovery")
}

pub(super) fn is_probe(row: &CiTaskRow) -> bool {
    metadata(row).is_some()
}

fn read_listing(
    store: &CoordinatorStore,
    results: &Path,
    probe: &CiTaskRow,
    step: DiscoveryStep,
) -> Result<Option<WorkerListing>, Fault> {
    let attempt = probe.attempt.as_ref();
    let state = standing(
        probe.cancelled,
        attempt.map(|a| (a.termination.as_ref(), a.collection.as_deref())),
    );
    if state == Standing::Open {
        return Ok(None);
    }
    if let Some(attempt) = attempt.filter(|_| state == Standing::Ended(Outcome::Succeeded)) {
        let kept = store
            .persisted_artifacts(&attempt.attempt_id)
            .map_err(transient)?;
        if let Some((_, digest)) = kept.iter().find(|(name, _)| name == RESULT_ARTIFACT) {
            let path = results
                .join(attempt.attempt_id.as_str())
                .join("artifacts")
                .join(digest.as_str().trim_start_matches("sha256:"));
            let bytes =
                read_capped(&path, MAX_RESULT_BYTES).map_err(|e| plan_read_fault(digest, &e))?;
            if &Digest::of_bytes(&bytes) != digest {
                return Err(unrunnable("discovery artifact digest mismatch"));
            }
            let listing: WorkerListing = serde_json::from_slice(&bytes).map_err(unrunnable)?;
            if listing.step != step {
                return Err(unrunnable(
                    "discovery artifact belongs to another plan or job",
                ));
            }
            return Ok(Some(listing));
        }
    }
    Ok(Some(step.failed()))
}

/// True while this pass owns a pending discovery stage, including the pass
/// that applied it. The next pass reads the new plan from scratch.
pub(super) fn advance(
    store: &CoordinatorStore,
    state_dir: &Path,
    results: &Path,
    run: &RunId,
    rows: &[CiTaskRow],
    now_ms: u64,
    pass: &mut Pass,
) -> Result<bool, Fault> {
    let mut probes = rows
        .iter()
        .filter(|r| metadata(r).is_some_and(|m| m["applied"] == false));
    let Some(first) = probes.next() else {
        return Ok(false);
    };
    let first_spec: TaskSpec = serde_json::from_value(first.spec.clone()).map_err(unrunnable)?;
    let base = first_spec.plan_digest.clone();
    pass.plans.insert(base.clone());
    let plan: FrozenPlan =
        serde_json::from_slice(&read_plan(state_dir, &base)?).map_err(unrunnable)?;
    let capacity = metadata(first)
        .and_then(|m| m["capacity"].as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| unrunnable("invalid shard capacity"))?;
    let mut policies = BTreeMap::new();
    let mut listings = Vec::new();
    let mut ids = Vec::new();
    for probe in std::iter::once(first).chain(probes) {
        let id = probe
            .spec
            .pointer("/ci/task/task_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| unrunnable("missing discovery task id"))?;
        let step = DiscoveryStep::from_task(&plan, id)
            .ok_or_else(|| unrunnable("unexpected discovery task"))?;
        let Some(listing) = read_listing(store, results, probe, step)? else {
            return Ok(true);
        };
        let policy = metadata(probe)
            .and_then(|m| m.get("policy"))
            .filter(|v| !v.is_null());
        if let Some(value) = policy {
            let p: crate::config::model::ShardPolicy =
                serde_json::from_value(value.clone()).map_err(unrunnable)?;
            if p.workflow != listing.step.workflow || p.job != listing.step.job {
                return Err(unrunnable("shard policy belongs to another job"));
            }
            policies.insert((p.workflow.clone(), p.job.clone()), p);
        }
        ids.push(probe.task_id.clone());
        listings.push(listing);
    }
    let project = metadata(first)
        .and_then(|m| m["project"].as_str())
        .ok_or_else(|| unrunnable("missing timing scope"))?;
    let mut histories = BTreeMap::new();
    for listing in &listings {
        let key = listing.history_key(&plan, project).map_err(unrunnable)?;
        let history = store
            .ci_test_timings(key.clone(), listing.discovered.tests.clone())
            .map_err(transient)?;
        histories.insert(key, history);
    }
    let split = apply(
        &plan,
        &listings,
        capacity,
        &PartitionOpts::default(),
        |listing| {
            let key = listing.history_key(&plan, project)?;
            Ok(histories.get(&key).cloned().unwrap_or_default())
        },
        |step| {
            policies
                .get(&(step.workflow.clone(), step.job.clone()))
                .and_then(|p| p.isolation.clone())
                .map(|evidence| Isolation::Declared { evidence })
                .unwrap_or_default()
        },
    )
    .map_err(unrunnable)?;
    let (digest, _) = keep_plan(state_dir, &split).map_err(|e| Fault::Transient(e.to_string()))?;
    let specs = crate::service::ci_run::task_specs(
        &split,
        &digest,
        &first_spec.manifest_digest,
        &first_spec.requirements,
        first_spec.command_timeout_ms,
    )
    .map_err(|e| unrunnable(e.message))?;
    let by_id: BTreeMap<_, _> = specs.into_iter().collect();
    let mut replacements = Vec::new();
    for row in rows.iter().filter(|r| !is_probe(r)) {
        let id = row
            .spec
            .pointer("/ci/task/task_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| unrunnable("missing CI task id"))?;
        let original = plan
            .task(id)
            .ok_or_else(|| unrunnable("original task missing from plan"))?;
        let names: Vec<&str> = if split.task(id).is_some() {
            vec![id]
        } else {
            split
                .shard_groups
                .iter()
                .find(|g| {
                    g.split && g.workflow == original.workflow && g.source_jobs == original.jobs
                })
                .ok_or_else(|| unrunnable("partition lost a CI task"))?
                .shards
                .iter()
                .map(String::as_str)
                .collect()
        };
        let mut parts = Vec::with_capacity(names.len());
        for (i, name) in names.into_iter().enumerate() {
            let spec = by_id
                .get(name)
                .ok_or_else(|| unrunnable("partitioned task missing"))?;
            let task = if i == 0 {
                row.task_id.clone()
            } else {
                TaskId::generate()
            };
            let mut spec = spec.clone();
            if let Some(shard) = split.task(name).and_then(|t| t.shard.as_ref()) {
                let runner = shard.runner.runner.as_str();
                let allowed = policies
                    .get(&(original.workflow.clone(), original.jobs[0].clone()))
                    .and_then(|p| p.allow_retry)
                    .unwrap_or(!matches!(runner, "playwright" | "cypress"));
                if let Some(ci) = &mut spec.ci {
                    ci.shard_retry_forbidden = !allowed;
                }
            }
            parts.push((task, serde_json::to_value(spec).map_err(unrunnable)?));
        }
        replacements.push((row.task_id.clone(), parts));
    }
    store
        .apply_ci_discovery(run, ids, replacements, now_ms)
        .map_err(transient)?;
    Ok(true)
}
