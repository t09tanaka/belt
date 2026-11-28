//! The threads each job of a run may use, so that the jobs running at once do
//! not ask for more CPU than the machine has.
//!
//! local-ci does not admit jobs by CPU itself: with task-traffic-controller
//! every step waits there for its turn, and the share is the weight it asks
//! for. This module only works out the share and tells the job's tools.

use crate::context::ExecContext;
use crate::dag::{self, MatrixPlan};
use crate::frozen::FrozenPlan;
use crate::plan;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;

/// The variables a job is given its share in, unless the caller's
/// environment, the workflow, the job or the step sets them.
pub const JOB_THREAD_ENV: [&str; 3] = [
    "CARGO_BUILD_JOBS",
    "NEXTEST_TEST_THREADS",
    "RUST_TEST_THREADS",
];

/// With task-traffic-controller, a job is not given fewer threads than this
/// (while the budget has them): three jobs with four threads each rather than
/// twelve jobs with one.
const MIN_SHARE: usize = 4;

/// Units at once without task-traffic-controller, when `--parallel` is not given.
pub const UNGATED_PARALLEL: usize = 4;

/// How the CPU of a run is divided between its jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Share {
    /// What is divided: task-traffic-controller's budget, or the cores of the
    /// machine without it.
    pub budget: usize,
    /// The jobs the share is worked out for.
    pub jobs_at_once: usize,
    /// The threads of each job.
    pub threads_per_job: usize,
}

/// The share of each job, or `None` when one job has the whole budget (it is
/// then run as before: no variable is set and no weight is asked for).
///
/// `parallel` is `--parallel` when it was given, `gated` whether steps wait in
/// task-traffic-controller, `peak` the most jobs the plan can have running at
/// once (see [`peak`], worked out for the units at once the run has). With
/// `--parallel`, the jobs at once are all of `peak`: the matrix entries of the
/// units run together whatever the number of units at once.
pub fn share(budget: usize, parallel: Option<usize>, gated: bool, peak: usize) -> Option<Share> {
    let budget = budget.max(1);
    let jobs_at_once = match parallel {
        Some(_) => peak.max(1),
        None => {
            let wanted = if gated {
                budget / MIN_SHARE
            } else {
                UNGATED_PARALLEL
            };
            wanted.clamp(1, peak.max(1))
        }
    };
    let threads_per_job = (budget / jobs_at_once).max(1);
    (threads_per_job < budget).then_some(Share {
        budget,
        jobs_at_once,
        threads_per_job,
    })
}

/// The most jobs of `fp` that run at once when `units_at_once` units do: the
/// widest of them, one job each, or as many as their matrices are wide where
/// the entries of a matrix run together. The entries of a matrix run together
/// when `matrix_at_once` (task-traffic-controller keeps the CPU in check) and
/// the job does not hold the per-repository lock, as in `exec`: an exclusive
/// job of a locked unit runs its entries one after another. `isolated`: the
/// run gives every job its own ports (portool), so only a unit with
/// `services:` is locked. A matrix that is only known when it runs counts as
/// wide as `strategy.max-parallel` lets it be.
pub fn peak(
    fp: &FrozenPlan,
    matrix_at_once: bool,
    isolated: bool,
    units_at_once: usize,
    all_os_legs: bool,
) -> usize {
    let mut widths: Vec<usize> = fp
        .units
        .iter()
        .map(|u| {
            if !matrix_at_once {
                return 1;
            }
            let locked = u.exclusive
                && (!isolated
                    || u.jobs
                        .iter()
                        .any(|id| !fp.job(u.workflow, id).services.is_empty()));
            u.jobs
                .iter()
                .map(|id| {
                    let job = fp.job(u.workflow, id);
                    if plan::is_exclusive(&job.to_job()) && locked {
                        return 1;
                    }
                    let entries = match dag::matrix_plan(job) {
                        MatrixPlan::None => 1,
                        // the legs of another OS that a leg of this host stands in
                        // for do not run (see `os_legs`)
                        MatrixPlan::Static(v) if all_os_legs => v.len(),
                        MatrixPlan::Static(v) => crate::os_legs::running_legs(
                            crate::os_legs::Family::host(),
                            &job.to_job(),
                            &v,
                        ),
                        MatrixPlan::Dynamic(_) => usize::MAX,
                    };
                    entries.min(job.max_parallel.unwrap_or(usize::MAX)).max(1)
                })
                .max()
                .unwrap_or(1)
        })
        .collect();
    widths.sort_unstable_by(|a, b| b.cmp(a));
    widths
        .into_iter()
        .take(units_at_once.max(1))
        .fold(0usize, usize::saturating_add)
}

/// The cores of this machine.
pub fn cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// task-traffic-controller's budget (`limits.budget` of `inspect`), or why it
/// cannot be read (an older controller, a failure).
pub fn controller_budget(ctx: &ExecContext, controller: &Path) -> Result<usize, String> {
    let out = ctx
        .command(controller)
        .arg("inspect")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("`inspect` could not be run: {e}"))?;
    if !out.status.success() {
        return Err(format!("`inspect` failed ({})", out.status));
    }
    parse_budget(&String::from_utf8_lossy(&out.stdout))
        .ok_or_else(|| "`inspect` has no budget".to_string())
}

fn parse_budget(json: &str) -> Option<usize> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let b = v.get("limits")?.get("budget")?.as_u64()?;
    (b > 0).then_some(b as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(budget: usize, jobs_at_once: usize, threads_per_job: usize) -> Option<Share> {
        Some(Share {
            budget,
            jobs_at_once,
            threads_per_job,
        })
    }

    #[test]
    fn gated_jobs_get_at_least_four_threads_while_the_budget_has_them() {
        // nine jobs on twelve cores: three at once with four threads, not nine with one
        assert_eq!(share(12, None, true, 9), s(12, 3, 4));
        assert_eq!(share(10, None, true, 9), s(10, 2, 5));
        assert_eq!(share(16, None, true, 9), s(16, 4, 4));
        // the sum of the shares never exceeds the budget
        for budget in 1..=64 {
            for peak in 1..=20 {
                if let Some(x) = share(budget, None, true, peak) {
                    assert!(x.jobs_at_once * x.threads_per_job <= budget, "{x:?}");
                    assert!(x.threads_per_job >= MIN_SHARE.min(budget / 2), "{x:?}");
                }
            }
        }
    }

    #[test]
    fn a_job_that_runs_alone_keeps_the_whole_budget() {
        assert_eq!(share(12, None, true, 1), None);
        assert_eq!(share(12, None, false, 1), None);
        assert_eq!(share(12, Some(1), true, 1), None);
        // a budget too small to divide
        assert_eq!(share(4, None, true, 9), None);
        assert_eq!(share(1, None, false, 9), None);
        assert_eq!(share(0, None, true, 9), None);
    }

    #[test]
    fn fewer_jobs_than_places_share_between_themselves_only() {
        assert_eq!(share(12, None, true, 2), s(12, 2, 6));
        assert_eq!(share(12, Some(8), true, 3), s(12, 3, 4));
        // one unit at a time whose matrix entries run together still divides
        assert_eq!(share(12, Some(1), true, 4), s(12, 4, 3));
    }

    #[test]
    fn parallel_decides_when_it_is_given() {
        assert_eq!(share(12, Some(6), true, 6), s(12, 6, 2));
        assert_eq!(share(12, Some(2), false, 2), s(12, 2, 6));
        // more places than cores: one thread each, never none
        assert_eq!(share(4, Some(16), false, 16), s(4, 16, 1));
    }

    #[test]
    fn without_the_controller_the_default_places_divide_the_cores() {
        assert_eq!(share(12, None, false, 9), s(12, 4, 3));
        assert_eq!(share(4, None, false, 9), s(4, 4, 1));
    }

    #[test]
    fn the_budget_is_read_from_inspect() {
        assert_eq!(
            parse_budget(r#"{"entries":[],"limits":{"budget":12}}"#),
            Some(12)
        );
        assert_eq!(parse_budget(r#"{"limits":{"budget":0}}"#), None);
        assert_eq!(parse_budget(r#"{"limits":{}}"#), None);
        assert_eq!(parse_budget("not json"), None);
    }
}
