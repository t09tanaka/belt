//! Bind a worker's listing to the frozen job and use the same runner adapter
//! for listing, partitioning and execution. The listing carries no worker paths.

use local_ci::dag::{NotSplit, ShardRequest};
use local_ci::shard::{
    adapters::{self, CargoTest, Cypress, Isolation, Jest, Playwright, Safety, Vitest},
    partition_tests, CommandOutput, Discovered, History, Invocation, PartitionOpts, RunnerAdapter,
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
            .unwrap_or("");
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
        if !j.needs.is_empty() || j.matrix.is_some() || j.container || j.reusable.is_some() {
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
            if candidate.target(plan).is_ok() {
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
    pub discovered: Discovered,
}

impl DiscoveryStep {
    /// Invoke through the worker's supervised executor in its prepared workspace.
    pub fn discover(
        self,
        plan: &FrozenPlan,
        workspace: &Path,
        version: String,
        invoke: impl FnOnce(&Invocation, &Path) -> anyhow::Result<CommandOutput>,
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
        let output = invoke(&runner.discover_command(), &root)?;
        let discovered = runner.discover(&output).unwrap_or_else(|_| Discovered {
            blocked: vec![NotSplit::DiscoveryFailed],
            ..Default::default()
        });
        Ok(WorkerListing {
            step: self,
            runner_version: version,
            discovered,
        })
    }
}

impl WorkerListing {
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
        let partition = partition_tests(runner.as_ref(), &self.discovered, history, count, costs);
        Ok(ShardRequest {
            workflow: self.step.workflow.clone(),
            job: self.step.job.clone(),
            step: self.step.step,
            dir: target.dir,
            fail_fast: target.fail_fast,
            plan: partition,
            discovered: self.discovered.clone(),
        })
    }
}

/// Freeze all worker listings together, preserving already split jobs. A
/// partial or duplicated batch cannot replace a previously frozen decision.
pub fn apply(
    plan: &FrozenPlan,
    listings: &[WorkerListing],
    count: usize,
    costs: &PartitionOpts,
    history: impl Fn(&WorkerListing) -> anyhow::Result<History>,
    isolation: impl Fn(&DiscoveryStep) -> Isolation,
) -> anyhow::Result<FrozenPlan> {
    let expected = candidates(plan);
    anyhow::ensure!(
        expected.len() == listings.len(),
        "incomplete discovery batch"
    );
    let mut requests = plan.shard_requests();
    for expected_step in expected {
        let listing = listings
            .iter()
            .find(|r| r.step == expected_step)
            .ok_or_else(|| anyhow::anyhow!("discovery step missing from batch"))?;
        requests.push(listing.partition(
            plan,
            &history(listing)?,
            count,
            costs,
            isolation(&expected_step),
        )?);
    }
    plan.with_shards(&requests)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::execute::tests::fixture;
    use local_ci::shard::Basis;

    const WORKFLOW: &str =
        "on: push\njobs:\n  js:\n    runs-on: ubuntu-latest\n    steps:\n      - run: npx jest\n";

    #[test]
    fn an_exact_worker_listing_is_split_with_history_and_bound_to_its_plan() {
        let fixture = fixture(WORKFLOW);
        let plan = fixture.plan;
        let candidate = candidates(&plan).pop().unwrap();
        let listing = candidate
            .discover(&plan, &fixture.area, "30.2".into(), |inv, root| {
                assert!(inv.argv.iter().any(|s| s == "--listTests"));
                Ok(CommandOutput {
                    stdout: ["a.js", "b.js", "c.js", "d.js"]
                        .iter()
                        .map(|n| root.join(n).display().to_string())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    stderr: String::new(),
                    success: true,
                    exit: Some(0),
                })
            })
            .unwrap();
        assert_eq!(listing.discovered.tests, ["a.js", "b.js", "c.js", "d.js"]);
        let costs = PartitionOpts::default();
        let timing = History {
            millis: [("a.js", 90), ("b.js", 30), ("c.js", 30), ("d.js", 30)]
                .into_iter()
                .map(|(s, n)| (s.to_owned(), n))
                .collect(),
        };
        let request = listing
            .partition(&plan, &timing, 2, &costs, Isolation::None)
            .unwrap();
        assert_eq!(request.plan.basis, Basis::History);
        assert!(request.plan.shards.iter().all(|s| s.load == 90));
        assert_eq!(
            listing
                .partition(&plan, &History::default(), 2, &costs, Isolation::None)
                .unwrap()
                .plan
                .basis,
            Basis::Fallback
        );
        let split = apply(
            &plan,
            std::slice::from_ref(&listing),
            2,
            &costs,
            |_| Ok(timing.clone()),
            |_| Isolation::None,
        )
        .unwrap();
        split.verify().unwrap();
        assert!(split.shard_groups[0].split);
        assert_eq!(split.shard_groups[0].manifest, listing.discovered.tests);
        assert!(listing
            .partition(&split, &timing, 2, &costs, Isolation::None)
            .is_err());
        assert!(apply(
            &plan,
            &[],
            2,
            &costs,
            |_| Ok(timing.clone()),
            |_| Isolation::None
        )
        .is_err());
    }
}

impl DiscoveryStep {
    /// No ordinary task can have this id: all ordinary ids contain the
    /// workflow/job separator. An older worker refuses an unknown id.
    pub fn task_id(&self) -> String {
        let bytes = serde_json::to_vec(self).expect("discovery coordinates serialize");
        format!("rtr-discover-{}", crate::ids::Digest::of_bytes(&bytes))
    }

    pub fn from_task(plan: &FrozenPlan, id: &str) -> Option<Self> {
        if !id.starts_with("rtr-discover-") {
            return None;
        }
        candidates(plan).into_iter().find(|c| c.task_id() == id)
    }

    /// The source binding and setup steps are those of the actual job. The
    /// final two commands measure the installed version and enumerate tests;
    /// the tests themselves and post-test steps do not run in this attempt.
    pub fn execution_plan(&self, plan: &FrozenPlan) -> anyhow::Result<FrozenPlan> {
        let target = self.target(plan)?;
        let runner = adapter(target.command, "", "", Isolation::None)?;
        let listing = adapters::script_of(&runner.discover_command())
            .ok_or_else(|| anyhow::anyhow!("runner listing cannot be represented as a step"))?;
        let version = match runner.info().runner.as_str() {
            "cargo-test" => "cargo --version",
            "vitest" => "node -p \"require('vitest/package.json').version\"",
            "jest" => "node -p \"require('jest/package.json').version\"",
            "playwright" => "node -p \"require('playwright/package.json').version\"",
            "cypress" => "node -p \"require('cypress/package.json').version\"",
            name => anyhow::bail!("unknown runner {name}"),
        };
        let mut probe = plan.to_plan();
        probe.selected.retain(|s| s.wf.file == self.workflow);
        let workflow = &mut probe.selected[0].wf;
        workflow.jobs.retain(|job| job.id == self.job);
        let job = &mut workflow.jobs[0];
        anyhow::ensure!(
            job.needs.is_empty() && job.matrix.is_none() && job.services.is_empty(),
            "discovery needs an independent prepared job"
        );
        job.outputs.clear();
        job.steps.truncate(self.step + 1);
        let test = &mut job.steps[self.step];
        test.run = Some(version.to_owned());
        let mut listing_step = test.clone();
        listing_step.id = None;
        listing_step.run = Some(listing);
        job.steps.push(listing_step);
        probe.units = vec![local_ci::plan::Unit {
            wf: 0,
            jobs: vec![self.job.clone()],
            exclusive: false,
        }];
        plan.with_plan(probe)
    }

    // The listing is optional planning evidence. A failed version probe,
    // setup, or listing yields an explanation and leaves the real job whole.
    pub fn failed(self) -> WorkerListing {
        WorkerListing {
            step: self,
            runner_version: "unavailable".into(),
            discovered: Discovered {
                blocked: vec![NotSplit::DiscoveryFailed],
                ..Default::default()
            },
        }
    }

    pub fn result(
        self,
        plan: &FrozenPlan,
        workspace: &Path,
        run_dir: &Path,
        result: &local_ci::TaskResult,
    ) -> anyhow::Result<WorkerListing> {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        let step_log = |index: usize, cap: u64| -> Option<String> {
            let ran = result.jobs.first()?.steps.get(index)?;
            if ran.exit != Some(0) {
                return None;
            }
            let path = ran.log.as_ref()?;
            if !path.starts_with(run_dir) {
                return None;
            }
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(path)
                .ok()?;
            if !file.metadata().ok()?.is_file() {
                return None;
            }
            let mut buf = String::new();
            file.take(cap + 1).read_to_string(&mut buf).ok()?;
            (buf.len() as u64 <= cap).then_some(buf)
        };
        let Some(version) = step_log(self.step, 4096) else {
            return Ok(self.failed());
        };
        let Some(list) = step_log(self.step + 1, super::advance::MAX_RESULT_BYTES) else {
            return Ok(self.failed());
        };
        let version = version.trim();
        if version.is_empty() || version.len() > 256 || version.chars().any(char::is_control) {
            return Ok(self.failed());
        }
        // The supervisor tees both streams into the one step log. The cargo
        // adapter counts harness summaries in stdout and target lines in stderr.
        self.parse_listing(
            plan,
            workspace,
            version.to_owned(),
            CommandOutput {
                stdout: list.clone(),
                stderr: list,
                success: true,
                exit: Some(0),
            },
        )
    }
    pub fn parse_listing(
        self,
        plan: &FrozenPlan,
        workspace: &Path,
        version: String,
        output: CommandOutput,
    ) -> anyhow::Result<WorkerListing> {
        self.discover(plan, workspace, version, |_, _| Ok(output))
    }
}
