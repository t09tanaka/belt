//! Workspace-local discovery plan, supervised output and attempt identity.
use super::*;

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
        let job = probe
            .selected
            .iter_mut()
            .find(|selected| selected.wf.file == self.workflow)
            .and_then(|selected| selected.wf.jobs.iter_mut().find(|job| job.id == self.job))
            .ok_or_else(|| anyhow::anyhow!("missing discovery job"))?;
        anyhow::ensure!(
            job.matrix.is_none() && job.services.is_empty(),
            "discovery needs a prepared job"
        );
        job.outputs.clear();
        job.steps.truncate(self.step + 1);
        let test = &mut job.steps[self.step];
        test.run = Some(version.to_owned());
        let mut listing_step = test.clone();
        listing_step.id = None;
        listing_step.run = Some(listing);
        job.steps.push(listing_step);
        // Keep predecessors in the graph so the SDK resolves needs and job
        // conditions exactly as the ordinary task does. The host executes
        // only this job, supplying the already collected predecessor outputs.
        plan.with_plan(probe)
    }

    // The listing is optional planning evidence. A failed version probe,
    // setup, or listing yields an explanation and leaves the real job whole.
    pub fn failed(self) -> WorkerListing {
        WorkerListing {
            step: self,
            runner_version: "unavailable".into(),
            setup_ms: None,
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
        let step_log = |index: usize, cap: u64| -> Option<String> {
            let ran = result.jobs.first()?.steps.get(index)?;
            if ran.exit != Some(0) {
                return None;
            }
            let path = ran.log.as_ref()?;
            let relative = path.strip_prefix(run_dir).ok()?;
            let file = crate::platform::open_beneath(run_dir, relative).ok()?;
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
        let setup_ms = result.jobs.first().and_then(|job| {
            let elapsed: f64 = job
                .steps
                .get(..self.step)?
                .iter()
                .map(|step| step.secs)
                .sum();
            let wait = job.time.as_ref()?.wait_queue_secs;
            preparation_ms(elapsed, wait)
        });
        let mut listing = self.parse_listing(
            plan,
            workspace,
            version.to_owned(),
            CommandOutput {
                stdout: list.clone(),
                stderr: list,
                success: true,
                exit: Some(0),
            },
        )?;
        listing.setup_ms = setup_ms;
        Ok(listing)
    }
}

// Only a job-wide admission wait is available. Subtracting all of it gives
// a lower bound on the preparation work repeated by each shard.
fn preparation_ms(elapsed: f64, wait: f64) -> Option<u64> {
    let elapsed = std::time::Duration::try_from_secs_f64(elapsed).ok()?;
    let wait = std::time::Duration::try_from_secs_f64(wait).ok()?;
    elapsed.saturating_sub(wait).as_millis().try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::preparation_ms;

    #[test]
    fn preparation_excludes_admission_wait_and_rejects_invalid_measurements() {
        assert_eq!(preparation_ms(2.5, 1.25), Some(1250));
        assert_eq!(preparation_ms(2.0, 3.0), Some(0));
        assert_eq!(preparation_ms(f64::NAN, 0.0), None);
        assert_eq!(preparation_ms(0.0, -1.0), None);
    }
}
