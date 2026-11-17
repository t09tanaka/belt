//! Bind a worker's listing to the frozen job and use the same runner adapter
//! for listing, partitioning and execution. The listing carries no worker paths.

pub(crate) mod timing;
mod worker;

use local_ci::dag::{NotSplit, ShardRequest};
use local_ci::shard::{
    adapters::{self, CargoTest, Cypress, Isolation, Jest, Playwright, Safety, Vitest},
    partition_tests, CommandOutput, Discovered, History, PartitionOpts, RunnerAdapter,
};
use local_ci::FrozenPlan;
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryStep {
    pub plan_digest: String,
    pub workflow: String,
    pub job: String,
    pub step: usize,
}

/// Only the frozen plan supplies the command, the working directory, and
/// whether failure of a shard stops its peers.
struct Target<'a> {
    command: &'a str,
    dir: String,
    fail_fast: bool,
}

impl DiscoveryStep {
    fn target<'a>(&self, plan: &'a FrozenPlan) -> anyhow::Result<Target<'a>> {
        anyhow::ensure!(self.plan_digest == plan.digest, "discovery plan changed");
        let workflow = plan
            .workflows
            .iter()
            .find(|w| w.file == self.workflow)
            .ok_or_else(|| anyhow::anyhow!("discovery workflow missing"))?;
        let job = workflow
            .jobs
            .iter()
            .find(|j| j.id == self.job)
            .ok_or_else(|| anyhow::anyhow!("discovery job missing"))?;
        let step = job
            .steps
            .get(self.step)
            .ok_or_else(|| anyhow::anyhow!("discovery step missing"))?;
        let command = step
            .run
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing runner command"))?;
        let raw = step
            .working_directory
            .as_deref()
            .or(job.working_directory.as_deref())
            .or(workflow.working_directory.as_deref())
            .unwrap_or("")
            .trim();
        anyhow::ensure!(
            Path::new(raw)
                .components()
                .all(|p| matches!(p, Component::CurDir | Component::Normal(_)))
                && !raw.contains("${{"),
            "runner directory must be relative to the captured source"
        );
        Ok(Target {
            command,
            dir: local_ci::plan::norm_dir(raw),
            fail_fast: job.fail_fast,
        })
    }
}

/// A job with more than one test step cannot be split by replacing just one:
/// every other test would be run in every shard.
pub fn candidates(plan: &FrozenPlan) -> Vec<DiscoveryStep> {
    let mut steps = Vec::new();
    for group in &plan.shard_groups {
        let [id] = group.source_jobs.as_slice() else {
            continue;
        };
        if group.split
            || group
                .not_split
                .iter()
                .any(|r| *r != NotSplit::NoRunnerAdapter)
        {
            continue;
        }
        let Some(w) = plan.workflows.iter().find(|w| w.file == group.workflow) else {
            continue;
        };
        let Some(j) = w.jobs.iter().find(|j| &j.id == id) else {
            continue;
        };
        if j.matrix.is_some() || j.container || j.reusable.is_some() {
            continue;
        }
        let mut runners = j
            .steps
            .iter()
            .enumerate()
            .filter(|(_, s)| s.run.as_deref().and_then(adapters::parse_command).is_some());
        if let (Some((step, _)), None) = (runners.next(), runners.next()) {
            let candidate = DiscoveryStep {
                plan_digest: plan.digest.clone(),
                workflow: w.file.clone(),
                job: id.clone(),
                step,
            };
            // These steps will also precede every actual shard. Only a
            // reproducible setup may be replayed for discovery.
            let safe_setup = j.steps[..step].iter().all(|s| {
                s.run.as_deref().is_some_and(local_ci::plan::is_installer)
                    || s.uses
                        .as_deref()
                        .and_then(|u| u.split_once('@'))
                        .is_some_and(|(action, _)| {
                            matches!(
                                action,
                                "actions/checkout"
                                    | "actions/setup-node"
                                    | "actions/setup-python"
                                    | "actions/setup-java"
                                    | "dtolnay/rust-toolchain"
                            )
                        })
            });
            if safe_setup && candidate.target(plan).is_ok() {
                steps.push(candidate);
            }
        }
    }
    steps
}

fn adapter(
    command: &str,
    version: &str,
    root: &str,
    isolation: Isolation,
) -> anyhow::Result<Box<dyn RunnerAdapter>> {
    let safety = Safety::default();
    if let Some(a) = CargoTest::from_command(version, command, safety) {
        return Ok(Box::new(a));
    }
    if let Some(a) = Vitest::from_command(version, root, command, safety) {
        return Ok(Box::new(a));
    }
    if let Some(a) = Jest::from_command(version, root, command, safety) {
        return Ok(Box::new(a));
    }
    if let Some(a) = Playwright::from_command(version, command, isolation.clone()) {
        return Ok(Box::new(a));
    }
    if let Some(a) = Cypress::from_command(version, command, isolation) {
        return Ok(Box::new(a));
    }
    anyhow::bail!("no runner adapter for the frozen step")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerListing {
    pub step: DiscoveryStep,
    pub runner_version: String,
    /// Lower bound on repeated preparation work, excluding admission waits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_ms: Option<u64>,
    pub discovered: Discovered,
}

impl DiscoveryStep {
    /// Parse the output of the supervised listing in the worker workspace.
    pub fn parse_listing(
        self,
        plan: &FrozenPlan,
        workspace: &Path,
        version: String,
        output: CommandOutput,
    ) -> anyhow::Result<WorkerListing> {
        let target = self.target(plan)?;
        let workspace = workspace.canonicalize()?;
        let root = workspace.join(&target.dir).canonicalize()?;
        anyhow::ensure!(
            root.starts_with(&workspace),
            "discovery directory escaped the workspace"
        );
        anyhow::ensure!(!version.trim().is_empty(), "missing runner version");
        let runner = adapter(
            target.command,
            &version,
            root.to_str()
                .ok_or_else(|| anyhow::anyhow!("runner directory is not UTF-8"))?,
            Isolation::None,
        )?;
        let discovered = runner.discover(&output).unwrap_or_else(|_| Discovered {
            blocked: vec![NotSplit::DiscoveryFailed],
            ..Default::default()
        });
        Ok(WorkerListing {
            step: self,
            runner_version: version,
            setup_ms: None,
            discovered: validated_listing(discovered, &runner.info().runner),
        })
    }
}

fn validated_listing(discovered: Discovered, runner: &str) -> Discovered {
    let files = matches!(runner, "vitest" | "jest" | "cypress");
    if discovered.tests.iter().any(|identity| {
        identity.is_empty()
            || identity.chars().any(char::is_control)
            || (files
                && !Path::new(identity)
                    .components()
                    .all(|p| matches!(p, Component::Normal(_) | Component::CurDir)))
    }) {
        return Discovered {
            blocked: vec![NotSplit::ManifestMismatch],
            ..Default::default()
        };
    }
    discovered
}

impl WorkerListing {
    pub fn history_key(&self, plan: &FrozenPlan, project: &str) -> anyhow::Result<String> {
        let target = self.step.target(plan)?;
        let runner = adapter(target.command, &self.runner_version, "", Isolation::None)?;
        Ok(timing_key(
            project,
            &self.step.workflow,
            &self.step.job,
            &target.dir,
            runner.info(),
        ))
    }

    pub fn partition(
        &self,
        plan: &FrozenPlan,
        history: &History,
        count: usize,
        costs: &PartitionOpts,
        isolation: Isolation,
    ) -> anyhow::Result<ShardRequest> {
        let target = self.step.target(plan)?;
        let runner = adapter(target.command, &self.runner_version, "", isolation)?;
        let discovered = validated_listing(self.discovered.clone(), &runner.info().runner);
        let mut costs = costs.clone();
        costs.fixed_cost_ms = costs
            .fixed_cost_ms
            .saturating_add(self.setup_ms.unwrap_or(0));
        let partition = partition_tests(runner.as_ref(), &discovered, history, count, &costs);
        Ok(ShardRequest {
            workflow: self.step.workflow.clone(),
            job: self.step.job.clone(),
            step: self.step.step,
            dir: target.dir,
            fail_fast: target.fail_fast,
            plan: partition,
            discovered,
        })
    }
}

pub fn timing_key(
    project: &str,
    workflow: &str,
    job: &str,
    dir: &str,
    runner: &local_ci::shard::RunnerInfo,
) -> String {
    crate::ids::Digest::of_bytes(
        &serde_json::to_vec(&(project, workflow, job, dir, runner)).expect("timing key serializes"),
    )
    .to_string()
}

/// Put the original tasks on hold until every listing is available. The
/// coordinate lives in a separate task so a retry can never run an uncertain
/// test again just to discover it.
pub fn stage(
    plan: &FrozenPlan,
    specs: &mut Vec<(String, crate::coordinator::TaskSpec)>,
    capacity: usize,
    project: &str,
    policies: &[crate::config::model::ShardPolicy],
) {
    let mut probes = Vec::new();
    for step in candidates(plan) {
        let Some(task) = plan.tasks.iter().find(|t| {
            t.workflow == step.workflow && t.jobs.as_slice() == std::slice::from_ref(&step.job)
        }) else {
            continue;
        };
        let Some((_, spec)) = specs.iter_mut().find(|(name, _)| name == &task.id) else {
            continue;
        };
        let mut probe = spec.clone();
        spec.ci.as_mut().expect("CI task").hold = true;
        let ci = probe.ci.as_mut().expect("specs came from a CI plan");
        let id = step.task_id();
        ci.task.task_id = id.clone();
        let policy = policies
            .iter()
            .find(|p| p.workflow == step.workflow && p.job == step.job);
        ci.task.inputs["rtr_discovery"] = serde_json::json!({"original_task":task.id,"capacity": capacity, "project": project, "applied": false, "policy": policy});

        ci.steps = u32::try_from(step.step + 2).unwrap_or(u32::MAX);
        probes.push((id, probe));
    }
    specs.extend(probes);
}

/// The immutable version of the DAG that contains all committed discoveries.
pub fn active_plan(rows: &[crate::storage::CiTaskRow]) -> Option<&str> {
    rows.iter().find_map(|r| {
        r.spec
            .pointer("/ci/task/inputs/rtr_discovery/active_plan")
            .and_then(|v| v.as_str())
    })
}

/// Freeze the listings ready in this pass. A prior decision stays attached
/// to its job as downstream jobs become ready to be listed.
pub fn apply(
    plan: &FrozenPlan,
    listings: &[WorkerListing],
    count: usize,
    evidence: impl Fn(&WorkerListing) -> anyhow::Result<(History, PartitionOpts)>,
    isolation: impl Fn(&DiscoveryStep) -> Isolation,
) -> anyhow::Result<FrozenPlan> {
    let expected = candidates(plan);
    anyhow::ensure!(!listings.is_empty(), "empty discovery batch");
    let mut requests = plan.shard_requests();
    let mut seen = std::collections::BTreeSet::new();
    for listing in listings {
        let expected_step = &listing.step;
        anyhow::ensure!(
            expected.contains(expected_step)
                && seen.insert((&expected_step.workflow, &expected_step.job)),
            "unexpected or duplicated discovery step"
        );
        let (history, costs) = evidence(listing)?;
        requests.push(listing.partition(
            plan,
            &history,
            count,
            &costs,
            isolation(expected_step),
        )?);
    }
    let mut next = plan.with_shards(&requests)?;
    // The SDK exports requests only for split groups. Earlier single-job
    // decisions still explain why a runner had to keep its job together.
    for group in &mut next.shard_groups {
        if let Some(previous) = plan
            .shard_groups
            .iter()
            .find(|old| old.id == group.id && !old.split && old.runner.is_some())
        {
            *group = previous.clone();
        }
    }
    next.digest = next.compute_digest()?;
    Ok(next)
}

#[cfg(test)]
mod tests;
