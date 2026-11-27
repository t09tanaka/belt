//! Durable transition from discovery attempts to a fully partitioned run.
use super::*;
use crate::ci::discovery::{apply, DiscoveryStep, WorkerListing};
use local_ci::shard::adapters::Isolation;

pub(super) fn metadata(row: &CiTaskRow) -> Option<&serde_json::Value> {
    row.spec.pointer("/ci/task/inputs/belt_discovery")
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
    let initial: FrozenPlan =
        serde_json::from_slice(&read_plan(state_dir, &base)?).map_err(unrunnable)?;
    let current = crate::ci::discovery::active_plan(rows)
        .map(Digest::parse)
        .transpose()
        .map_err(unrunnable)?
        .unwrap_or_else(|| base.clone());
    let plan = if current == base {
        initial.clone()
    } else {
        serde_json::from_slice(&read_plan(state_dir, &current)?).map_err(unrunnable)?
    };
    pass.plans.insert(current.clone());
    let capacity = metadata(first)
        .and_then(|m| m["capacity"].as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| unrunnable("invalid shard capacity"))?;
    let per_worker = metadata(first)
        .and_then(|m| m["per_worker"].as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(capacity);
    let matrix = super::matrix::applies(&plan);
    let mut waiting = false;
    let mut policies = BTreeMap::new();
    let mut listings = Vec::new();
    let mut ids = Vec::new();
    let mut placements = BTreeMap::new();
    for probe in std::iter::once(first).chain(probes) {
        if !probe.cancelled
            && metadata(probe)
                .and_then(|m| m["original_task"].as_str())
                .is_some_and(|target| {
                    rows.iter().any(|row| {
                        row.cancelled
                            && row
                                .spec
                                .pointer("/ci/task/task_id")
                                .and_then(|v| v.as_str())
                                == Some(target)
                    })
                })
        {
            waiting = true;
            stop(
                store,
                run,
                probe,
                "discovery target was cancelled",
                now_ms,
                pass,
            )
            .map_err(transient)?;
            continue;
        }
        let id = probe
            .spec
            .pointer("/ci/task/task_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| unrunnable("missing discovery task id"))?;
        let step = DiscoveryStep::from_task(&initial, id)
            .ok_or_else(|| unrunnable("unexpected discovery task"))?;
        let Some(mut listing) = read_listing(store, results, probe, step)? else {
            waiting = true;
            continue;
        };
        listing.step.plan_digest = plan.digest.clone();
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
        if let Some(attempt) = &probe.attempt {
            if let Some(decision) =
                crate::scheduler::reason_of(store, &attempt.attempt_id).map_err(transient)?
            {
                let workers = decision
                    .compared
                    .iter()
                    .map(|c| {
                        (
                            c.worker_id.to_string(),
                            per_worker.saturating_sub(c.load as usize).max(1),
                        )
                    })
                    .collect::<Vec<_>>();
                let transfer = crate::scheduler::decide::TRANSFER;
                let lower_bound = decision
                    .compared
                    .iter()
                    .map(|candidate| {
                        if candidate.costs.assumed[transfer] {
                            0
                        } else {
                            candidate.costs.ms[transfer].unwrap_or(0)
                        }
                    })
                    .min()
                    .unwrap_or(0);
                placements.insert(
                    (listing.step.workflow.clone(), listing.step.job.clone()),
                    (workers, lower_bound),
                );
            }
        }
        ids.push(probe.task_id.clone());
        listings.push(listing);
    }
    if matrix && waiting {
        return Ok(true);
    }
    if listings.is_empty() {
        return Ok(false);
    }
    let project = metadata(first)
        .and_then(|m| m["project"].as_str())
        .ok_or_else(|| unrunnable("missing timing scope"))?;
    // Keep the original jobs, probes and their replacement members inside
    // the queue limit even if every discovered job expands at once.
    let max_shards =
        1 + crate::storage::MAX_TASKS_PER_RUN.saturating_sub(rows.len()) / listings.len().max(1);
    let runnable_siblings = rows
        .iter()
        .filter(|row| {
            !is_probe(row)
                && !row.cancelled
                && row.spec.pointer("/ci/hold").and_then(|v| v.as_bool()) != Some(true)
                && row.attempt.as_ref().is_none_or(|a| a.termination.is_none())
        })
        .count();
    let mut histories = BTreeMap::new();
    for listing in &listings {
        let key = listing.history_key(&plan, project).map_err(unrunnable)?;
        let history = store
            .ci_test_timings(key.clone(), listing.discovered.tests.clone())
            .map_err(transient)?;
        let placement = placements.get(&(listing.step.workflow.clone(), listing.step.job.clone()));
        let (workers, transfer) = placement
            .map(|(w, t)| (w.as_slice(), *t))
            .unwrap_or((&[], 0));
        let effective = if workers.is_empty() {
            capacity
        } else {
            workers
                .iter()
                .fold(0usize, |sum, (_, places)| sum.saturating_add(*places))
                .min(capacity)
        };
        let count = effective
            .saturating_sub(runnable_siblings)
            .div_ceil(listings.len())
            .max(1)
            .min(max_shards);
        let count = if listing.discovered.complete {
            count.min(listing.discovered.tests.len().max(1))
        } else {
            count
        };
        // Round-robin available slots so one worker cannot consume the
        // capacity of a different worker when they have unequal loads.
        let mut slots = Vec::new();
        for layer in 0..count {
            for (worker, free) in workers {
                if *free > layer && slots.len() < count {
                    slots.push(worker.clone());
                }
            }
            if slots.len() == count {
                break;
            }
        }
        let model = crate::ci::discovery::timing::calibrate(&history, &slots, count, transfer);
        histories.insert(key, (model, count));
    }
    let split = apply(
        &plan,
        &listings,
        |listing| {
            let key = listing.history_key(&plan, project)?;
            let (model, count) = histories
                .get(&key)
                .ok_or_else(|| anyhow::anyhow!("missing discovery timing model"))?;
            Ok((model.history.clone(), model.costs.clone(), *count))
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
        if !listings.iter().any(|l| {
            l.step.workflow == original.workflow
                && original.jobs.as_slice() == std::slice::from_ref(&l.step.job)
        }) {
            continue;
        }
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
        // Each shard prepares what its own job needs (spec 22.1), not what
        // the first probe's job needed.
        let own: TaskSpec = serde_json::from_value(row.spec.clone()).map_err(unrunnable)?;
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
            spec.requirements = with_own_preparation(&spec.requirements, &own.requirements);
            if let Some(shard) = split.task(name).and_then(|t| t.shard.as_ref()) {
                let runner = shard.runner.runner.as_str();
                let allowed = policies
                    .get(&(original.workflow.clone(), original.jobs[0].clone()))
                    .and_then(|p| p.allow_retry)
                    .unwrap_or(!matches!(runner, "playwright" | "cypress"));
                if let Some(ci) = &mut spec.ci {
                    ci.shard_retry_forbidden = !allowed;
                    ci.task.verify_shard_version = true;
                    if let Some(listing) = listings.iter().find(|listing| {
                        listing.step.workflow == original.workflow
                            && listing.step.job == original.jobs[0]
                    }) {
                        let key = listing.history_key(&plan, project).map_err(unrunnable)?;
                        ci.shard_preferred_worker = histories
                            .get(&key)
                            .and_then(|(m, _)| m.slots.get(i))
                            .map(|w| crate::ids::WorkerId::parse(w))
                            .transpose()
                            .map_err(unrunnable)?;
                    }
                }
            }
            parts.push((task, serde_json::to_value(spec).map_err(unrunnable)?));
        }
        replacements.push((row.task_id.clone(), parts));
    }
    store
        .apply_ci_discovery(run, &current, &digest, ids, replacements, now_ms)
        .map_err(transient)?;
    Ok(true)
}

/// `shard`'s requirements with the workspace preparation
/// ([`Requirement::Prepare`] and [`Requirement::SkipPreset`]) of `own`, the
/// spec of the job the shard was split from (spec 22.1). Preparation is per
/// task, so a shard must not inherit another job's.
///
/// [`Requirement::Prepare`]: crate::toolchain::capability::Requirement::Prepare
/// [`Requirement::SkipPreset`]: crate::toolchain::capability::Requirement::SkipPreset
fn with_own_preparation(
    shard: &crate::toolchain::capability::ExecutionRequirements,
    own: &crate::toolchain::capability::ExecutionRequirements,
) -> crate::toolchain::capability::ExecutionRequirements {
    use crate::toolchain::capability::Requirement;
    let preparation =
        |r: &Requirement| matches!(r, Requirement::Prepare(_) | Requirement::SkipPreset(_));
    let mut out = shard.clone();
    out.requires.retain(|r| !preparation(r));
    out.requires
        .extend(own.requires.iter().filter(|r| preparation(r)).cloned());
    out.requires.sort();
    out.requires.dedup();
    out
}

#[cfg(test)]
mod preparation_tests {
    use super::*;
    use crate::toolchain::capability::{ExecutionRequirements, Requirement};

    fn reqs(requires: Vec<Requirement>) -> ExecutionRequirements {
        ExecutionRequirements {
            profile: None,
            requires,
        }
    }

    #[test]
    fn each_split_job_keeps_its_own_preparation() {
        // The first probe is a Rust job: it skips the npm preset. A jest job
        // split in the same pass prepares node; a Rust shard must not.
        let rust = reqs(vec![
            Requirement::Tool("cargo".into()),
            Requirement::SkipPreset("npm".into()),
        ]);
        let jest = reqs(vec![
            Requirement::Tool("cargo".into()),
            Requirement::Prepare("node".into()),
        ]);
        // task_specs built every shard from the first probe's requirements.
        let jest_shard = with_own_preparation(&rust, &jest);
        assert!(jest_shard
            .requires
            .contains(&Requirement::Prepare("node".into())));
        assert!(jest_shard.skipped_presets().is_empty());
        assert!(jest_shard
            .requires
            .contains(&Requirement::Tool("cargo".into())));
        let rust_shard = with_own_preparation(&jest, &rust);
        assert!(!rust_shard
            .requires
            .contains(&Requirement::Prepare("node".into())));
        assert_eq!(rust_shard.skipped_presets(), ["npm"]);
    }
}
