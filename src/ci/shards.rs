//! The shards of a split job, as the parent of a run decides about them:
//! which tasks are the shards of which job, whether a shard failed, which
//! shards a failed one stops (fail-fast), and the one outcome of the job
//! once every shard ended.
//!
//! Nothing here does IO or keeps state. The plan says what the shards are
//! (`ShardGroup` of the frozen plan); a split is never made here. A job that
//! was not split is one task like any other and is not looked at.
//!
//! The rules are those of the shared driver of the core: a receipt counts
//! only when it names its task and its job, the tests of all shards are
//! checked against the manifest of the group
//! (`local_ci::complete::aggregate_shard_group`), and whatever is wrong
//! makes the job a failure.

use local_ci::dag::{ExecutionTask, ShardGroup};
use local_ci::shard::{ShardOutcome, TestStatus};
use local_ci::task::{JobOutcome, JobStatus};
use local_ci::{FrozenPlan, WireTaskResult};
use std::collections::{BTreeMap, BTreeSet};

/// What is kept of the problems of a split job is at most this long.
pub const MAX_DIAGNOSTIC_CHARS: usize = 512;

/// Why a shard is stopped when another shard of its job failed.
pub const FAIL_FAST_REASON: &str = "another shard of its job failed (fail-fast)";

/// The split job `task` is a shard of.
pub fn group_of<'a>(plan: &'a FrozenPlan, task: &ExecutionTask) -> Option<&'a ShardGroup> {
    let shard = task.shard.as_ref()?;
    plan.shard_groups
        .iter()
        .find(|g| g.split && g.id == shard.group)
}

/// What is wrong with the shards of `plan` as tasks a parent places, or
/// `None`: every shard is the task its split job names at its index, runs
/// the one job of it, and a split job has all of its shards (two at least).
pub fn shape_fault(plan: &FrozenPlan) -> Option<String> {
    for task in &plan.tasks {
        let Some(shard) = &task.shard else { continue };
        let Some(group) = group_of(plan, task) else {
            return Some(format!(
                "task `{}` is a shard of no split job of the plan",
                task.id
            ));
        };
        if group.shards.get(shard.index) != Some(&task.id) || shard.count != group.shards.len() {
            return Some(format!(
                "task `{}` is not shard {} of the {} of `{}`",
                task.id,
                shard.index,
                group.shards.len(),
                group.id
            ));
        }
        if task.jobs.len() != 1 || task.jobs != group.source_jobs {
            return Some(format!(
                "task `{}` does not run the one job of `{}`",
                task.id, group.id
            ));
        }
    }
    let is_shard_of = |id: &String, group: &ShardGroup| {
        plan.tasks
            .iter()
            .any(|t| &t.id == id && t.shard.as_ref().is_some_and(|s| s.group == group.id))
    };
    plan.shard_groups
        .iter()
        .filter(|g| g.split)
        .find(|g| g.shards.len() < 2 || !g.shards.iter().all(|id| is_shard_of(id, g)))
        .map(|g| format!("the split job `{}` does not have its shards", g.id))
}

/// The receipt of a shard in its result: only one that names the task and
/// its job. A task that is no shard has none.
pub fn receipt<'a>(task: &ExecutionTask, result: &'a WireTaskResult) -> Option<&'a ShardOutcome> {
    task.shard.as_ref()?;
    result
        .shard
        .as_ref()
        .filter(|r| r.shard_id == task.id && task.jobs.first() == Some(&r.job_id))
}

/// The tests of the shard did not all pass.
pub fn receipt_failed(receipt: &ShardOutcome) -> bool {
    !receipt.exit_ok || receipt.tests.iter().any(|t| t.status == TestStatus::Failed)
}

/// A shard that ended failed: its job did, or its tests.
pub fn shard_failed(outcomes: &[JobOutcome], receipt: Option<&ShardOutcome>) -> bool {
    outcomes.iter().any(|o| o.status == JobStatus::Failure) || receipt.is_some_and(receipt_failed)
}

/// The shards of `group` to stop: with fail-fast, once a shard of it failed,
/// every shard that has not ended. `ended` says of each shard that ended
/// whether it failed.
pub fn to_cancel(group: &ShardGroup, ended: &BTreeMap<String, bool>) -> Vec<String> {
    let failed = group.shards.iter().any(|s| ended.get(s) == Some(&true));
    if !group.fail_fast || !failed {
        return Vec::new();
    }
    group
        .shards
        .iter()
        .filter(|s| !ended.contains_key(*s))
        .cloned()
        .collect()
}

/// The outputs two of `parts` give different values, as sentences. The
/// shards of one job run the same job: an output they disagree on has no
/// one value to publish.
pub fn conflicting_outputs(parts: &[JobOutcome]) -> Vec<String> {
    let mut first: BTreeMap<&str, &str> = BTreeMap::new();
    let mut out = BTreeSet::new();
    for (k, v) in parts.iter().flat_map(|p| &p.outputs) {
        if *first.entry(k).or_insert(v) != v {
            out.insert(k.as_str());
        }
    }
    out.into_iter()
        .map(|k| format!("the shards gave different values for the output `{k}`"))
        .collect()
}

/// The job that its shards are, from the outcome each gave: a failure when
/// one failed, else cancelled when one was, skipped when all were, and a
/// success otherwise.
pub fn combine(job: &str, parts: &[JobOutcome]) -> JobOutcome {
    let any = |s: JobStatus| parts.iter().any(|p| p.status == s);
    let status = if any(JobStatus::Failure) {
        JobStatus::Failure
    } else if any(JobStatus::Cancelled) {
        JobStatus::Cancelled
    } else if !parts.is_empty() && parts.iter().all(|p| p.status == JobStatus::Skipped) {
        JobStatus::Skipped
    } else {
        JobStatus::Success
    };
    JobOutcome {
        job: job.to_string(),
        variant: None,
        status,
        outputs: parts.iter().flat_map(|p| p.outputs.clone()).collect(),
        skipped: parts
            .iter()
            .find_map(|p| p.skipped.clone())
            .filter(|_| status != JobStatus::Success),
        ancestor_failed: parts.iter().any(|p| p.ancestor_failed),
    }
}

/// What is wrong with the shards of `group` taken together: a shard without
/// a receipt (or with two), a test that is not reported exactly once by the
/// shard it was given to, a failed test, an output the shards disagree on.
pub fn problems(
    group: &ShardGroup,
    parts: &[JobOutcome],
    receipts: &[ShardOutcome],
) -> Vec<String> {
    let mut out = local_ci::complete::aggregate_shard_group(group, receipts).problems();
    out.extend(conflicting_outputs(parts));
    out
}

/// The problems as one text of bounded length, or `None` without any.
pub fn diagnostic(problems: &[String]) -> Option<String> {
    (!problems.is_empty()).then(|| {
        problems
            .join("; ")
            .chars()
            .take(MAX_DIAGNOSTIC_CHARS)
            .collect()
    })
}

/// The one outcome of a split job whose shards all ended: `parts` are the
/// outcomes of its shards and `receipts` what their tests did. A job whose
/// shards all passed is still a failure when something is wrong with them
/// together, and says what.
pub fn join(group: &ShardGroup, parts: &[JobOutcome], receipts: &[ShardOutcome]) -> JobOutcome {
    let mut outcome = combine(&group.source_jobs.join("+"), parts);
    if outcome.status == JobStatus::Success {
        if let Some(why) = diagnostic(&problems(group, parts, receipts)) {
            outcome.status = JobStatus::Failure;
            outcome.skipped = Some(why);
        }
    }
    outcome
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ci::execute::tests::fixture;
    use local_ci::dag::ShardRequest;
    use local_ci::shard::adapters::{CargoTest, Safety};
    use local_ci::shard::{partition_tests, Discovered, History, PartitionOpts, TestResult};

    pub(crate) const SPLIT: &str = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    outputs:\n      v: ${{ steps.t.outputs.v }}\n    steps:\n      - id: t\n        run: cargo test\n  report:\n    needs: test\n    if: always()\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo report\n";

    /// A request to split `job` of `plan` into `count` shards of `tests`.
    pub(crate) fn request(
        plan: &FrozenPlan,
        job: &str,
        tests: &[&str],
        count: usize,
        fail_fast: bool,
    ) -> ShardRequest {
        let adapter = CargoTest::new("1.0.0", &[], Safety::default());
        let discovered = Discovered {
            tests: tests.iter().map(|t| t.to_string()).collect(),
            complete: true,
            blocked: vec![],
        };
        let opts = PartitionOpts::default();
        let shards = partition_tests(&adapter, &discovered, &History::default(), count, &opts);
        let task = plan.tasks.iter().find(|t| t.jobs == [job]).unwrap();
        ShardRequest {
            workflow: task.workflow.clone(),
            job: job.into(),
            plan: shards,
            step: 0,
            dir: String::new(),
            discovered,
            fail_fast,
        }
    }

    /// The plan of [`SPLIT`] with `test` split in two shards of four tests.
    pub(crate) fn split_plan(fail_fast: bool) -> FrozenPlan {
        let plan = fixture(SPLIT).plan;
        let asked = request(&plan, "test", &["a", "b", "c", "d"], 2, fail_fast);
        let plan = plan.with_shards(&[asked]).unwrap();
        assert!(plan.shard_groups.iter().any(|g| g.split), "not split");
        plan
    }

    pub(crate) fn group(plan: &FrozenPlan) -> &ShardGroup {
        plan.shard_groups.iter().find(|g| g.split).unwrap()
    }

    pub(crate) fn outcome(job: &str, status: JobStatus) -> JobOutcome {
        JobOutcome {
            job: job.into(),
            variant: None,
            status,
            outputs: BTreeMap::new(),
            skipped: None,
            ancestor_failed: false,
        }
    }

    /// The receipt of shard `at` of the group: every test it was given, with
    /// `status`.
    pub(crate) fn receipt_of(group: &ShardGroup, at: usize, status: TestStatus) -> ShardOutcome {
        ShardOutcome {
            job_id: group.source_jobs[0].clone(),
            shard_id: group.shards[at].clone(),
            tests: group.assignments[at]
                .iter()
                .map(|t| TestResult {
                    identity: t.clone(),
                    status,
                })
                .collect(),
            exit_ok: status != TestStatus::Failed,
            parsed: true,
        }
    }

    fn result(task: &str, shard: Option<ShardOutcome>) -> WireTaskResult {
        WireTaskResult {
            schema_version: local_ci::wire::WIRE_SCHEMA_VERSION,
            task: task.into(),
            jobs: vec![],
            outcomes: vec![],
            receipt_ignored: None,
            artifacts: vec![],
            shard,
        }
    }

    #[test]
    fn a_shard_belongs_to_the_split_job_that_names_it_and_a_plain_task_to_none() {
        let plan = split_plan(false);
        let g = group(&plan);
        assert_eq!(g.shards.len(), 2);
        for id in &g.shards {
            let task = plan.task(id).unwrap();
            assert_eq!(group_of(&plan, task).map(|g| &g.id), Some(&g.id));
        }
        let report = plan.tasks.iter().find(|t| t.jobs == ["report"]).unwrap();
        assert!(group_of(&plan, report).is_none());
        // A group that is not split has no shards, whatever a task says.
        let mut unsplit = plan.clone();
        unsplit
            .shard_groups
            .iter_mut()
            .for_each(|g| g.split = false);
        let task = unsplit.task(&g.shards[0]).unwrap();
        assert!(group_of(&unsplit, task).is_none());
        // Nor is a shard of another group one of this.
        let mut other = plan.clone();
        other.tasks[0].shard.as_mut().unwrap().group = "elsewhere".into();
        assert!(group_of(&other, &other.tasks[0]).is_none());
    }

    #[test]
    fn the_shards_of_a_plan_are_placed_only_when_they_are_what_their_job_names() {
        let plan = split_plan(false);
        assert_eq!(shape_fault(&plan), None);
        assert_eq!(shape_fault(&fixture(SPLIT).plan), None);
        let at = plan.tasks.iter().position(|t| t.shard.is_some()).unwrap();
        let broken = |change: &dyn Fn(&mut FrozenPlan)| {
            let mut plan = plan.clone();
            change(&mut plan);
            shape_fault(&plan).unwrap()
        };
        let no_group = broken(&|p| p.tasks[at].shard.as_mut().unwrap().group = "gone".into());
        assert!(no_group.contains("no split job"), "{no_group}");
        let index = broken(&|p| {
            let shard = p.tasks[at].shard.as_mut().unwrap();
            shard.index = 1 - shard.index;
        });
        assert!(index.contains("is not shard"), "{index}");
        let count = broken(&|p| p.tasks[at].shard.as_mut().unwrap().count = 3);
        assert!(count.contains("is not shard"), "{count}");
        let other_job = broken(&|p| p.tasks[at].jobs = vec!["report".into()]);
        assert!(other_job.contains("the one job"), "{other_job}");
        let two_jobs = broken(&|p| {
            let jobs = vec!["test".to_string(), "report".to_string()];
            p.tasks[at].jobs = jobs.clone();
            p.shard_groups
                .iter_mut()
                .filter(|g| g.split)
                .for_each(|g| g.source_jobs = jobs.clone());
        });
        assert!(two_jobs.contains("the one job"), "{two_jobs}");
        // A split job that lost a shard, or names a task that is not one.
        let last = plan.tasks.iter().rposition(|t| t.shard.is_some()).unwrap();
        let lost = broken(&|p| {
            p.tasks[last].shard = None;
        });
        assert!(lost.contains("does not have its shards"), "{lost}");
        let gone = broken(&|p| {
            p.tasks.remove(last);
        });
        assert!(gone.contains("does not have its shards"), "{gone}");
        let one = broken(&|p| {
            p.tasks.remove(last);
            for g in p.shard_groups.iter_mut().filter(|g| g.split) {
                g.shards.truncate(1);
            }
            p.tasks[at].shard.as_mut().unwrap().count = 1;
        });
        assert!(one.contains("does not have its shards"), "{one}");
        // A task of another group under the name of a shard is not a shard
        // of this one.
        let foreign = broken(&|p| {
            let mut copy = p.shard_groups.iter().find(|g| g.split).unwrap().clone();
            copy.id = "other".into();
            copy.shards = vec![p.tasks[at].id.clone(), p.tasks[last].id.clone()];
            p.shard_groups.push(copy);
        });
        assert!(foreign.contains("`other`"), "{foreign}");
    }

    #[test]
    fn a_receipt_counts_only_for_the_shard_and_the_job_it_names() {
        let plan = split_plan(false);
        let g = group(&plan);
        let task = plan.task(&g.shards[0]).unwrap();
        let good = receipt_of(g, 0, TestStatus::Passed);
        assert_eq!(
            receipt(task, &result(&task.id, Some(good.clone()))),
            Some(&good)
        );
        assert_eq!(receipt(task, &result(&task.id, None)), None);
        let other_shard = receipt_of(g, 1, TestStatus::Passed);
        assert_eq!(receipt(task, &result(&task.id, Some(other_shard))), None);
        let mut other_job = good.clone();
        other_job.job_id = "report".into();
        assert_eq!(receipt(task, &result(&task.id, Some(other_job))), None);
        // A task that is no shard has no receipt, whatever its result holds.
        let plain = plan.tasks.iter().find(|t| t.jobs == ["report"]).unwrap();
        let mut forged = good;
        forged.shard_id = plain.id.clone();
        forged.job_id = "report".into();
        assert_eq!(receipt(plain, &result(&plain.id, Some(forged))), None);
    }

    #[test]
    fn a_shard_failed_when_its_job_or_one_of_its_tests_did() {
        let plan = split_plan(false);
        let g = group(&plan);
        let passed = receipt_of(g, 0, TestStatus::Passed);
        assert!(!receipt_failed(&passed));
        assert!(!receipt_failed(&receipt_of(g, 0, TestStatus::Skipped)));
        let mut one_failed = passed.clone();
        one_failed.tests[0].status = TestStatus::Failed;
        assert!(receipt_failed(&one_failed));
        let mut exit = passed.clone();
        exit.exit_ok = false;
        assert!(receipt_failed(&exit));

        let ok = [outcome("test", JobStatus::Success)];
        assert!(!shard_failed(&ok, Some(&passed)));
        assert!(!shard_failed(&ok, None));
        assert!(!shard_failed(&[], None));
        assert!(shard_failed(&ok, Some(&one_failed)));
        assert!(shard_failed(
            &[outcome("test", JobStatus::Failure)],
            Some(&passed)
        ));
        assert!(shard_failed(&[outcome("test", JobStatus::Failure)], None));
        // A shard that was cancelled or skipped did not fail.
        for status in [JobStatus::Cancelled, JobStatus::Skipped] {
            assert!(!shard_failed(&[outcome("test", status)], None));
        }
    }

    #[test]
    fn fail_fast_stops_the_shards_that_have_not_ended_once_one_failed() {
        let plan = split_plan(true);
        let g = group(&plan);
        let (a, b) = (g.shards[0].clone(), g.shards[1].clone());
        let ended = |list: &[(&String, bool)]| -> BTreeMap<String, bool> {
            list.iter().map(|(id, f)| ((*id).clone(), *f)).collect()
        };
        assert!(to_cancel(g, &ended(&[])).is_empty());
        assert!(to_cancel(g, &ended(&[(&a, false)])).is_empty());
        assert_eq!(to_cancel(g, &ended(&[(&a, true)])), [b.clone()]);
        assert_eq!(to_cancel(g, &ended(&[(&b, true)])), [a.clone()]);
        assert!(to_cancel(g, &ended(&[(&a, true), (&b, false)])).is_empty());
        // The failure of a task that is no shard of the group stops nothing.
        let stranger = "another".to_string();
        assert!(to_cancel(g, &ended(&[(&stranger, true)])).is_empty());
        // Without fail-fast the other shards go on.
        let plan = split_plan(false);
        assert!(to_cancel(group(&plan), &ended(&[(&a, true)])).is_empty());
    }

    #[test]
    fn the_shards_combine_to_the_status_of_the_job_in_the_order_of_gravity() {
        use JobStatus::*;
        let of = |statuses: &[JobStatus]| -> JobStatus {
            let parts: Vec<JobOutcome> = statuses.iter().map(|s| outcome("x", *s)).collect();
            combine("test", &parts).status
        };
        assert_eq!(of(&[Success, Success]), Success);
        assert_eq!(of(&[Success, Failure]), Failure);
        assert_eq!(of(&[Cancelled, Failure]), Failure);
        assert_eq!(of(&[Success, Cancelled]), Cancelled);
        assert_eq!(of(&[Skipped, Cancelled]), Cancelled);
        assert_eq!(of(&[Skipped, Skipped]), Skipped);
        assert_eq!(of(&[Skipped, Success]), Success);
        assert_eq!(of(&[]), Success);

        let mut first = outcome("x", Success);
        first.outputs.insert("v".into(), "1".into());
        let mut second = outcome("x", Cancelled);
        second.outputs.insert("w".into(), "2".into());
        second.skipped = Some("stopped".into());
        second.ancestor_failed = true;
        let job = combine("test", &[first.clone(), second.clone()]);
        assert_eq!(job.job, "test");
        assert_eq!(job.variant, None);
        let outputs: Vec<(&str, &str)> = job
            .outputs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(outputs, [("v", "1"), ("w", "2")]);
        assert_eq!(job.skipped.as_deref(), Some("stopped"));
        assert!(job.ancestor_failed);
        assert!(!combine("test", &[first.clone()]).ancestor_failed);
        // A job that is a success says nothing about a skip.
        let mut noted = first.clone();
        noted.skipped = Some("a note".into());
        assert_eq!(combine("test", &[noted]).skipped, None);
    }

    #[test]
    fn outputs_the_shards_disagree_on_are_named_once_each() {
        let with = |pairs: &[(&str, &str)]| {
            let mut o = outcome("test", JobStatus::Success);
            for (k, v) in pairs {
                o.outputs.insert(k.to_string(), v.to_string());
            }
            o
        };
        let same = [with(&[("v", "1")]), with(&[("v", "1"), ("w", "2")])];
        assert!(conflicting_outputs(&same).is_empty());
        let differ = [
            with(&[("v", "1"), ("w", "2")]),
            with(&[("v", "9"), ("w", "2")]),
            with(&[("v", "8")]),
        ];
        assert_eq!(
            conflicting_outputs(&differ),
            ["the shards gave different values for the output `v`"]
        );
    }

    #[test]
    fn the_problems_are_kept_as_one_bounded_text() {
        assert_eq!(diagnostic(&[]), None);
        let two = ["one".to_string(), "two".to_string()];
        assert_eq!(diagnostic(&two).as_deref(), Some("one; two"));
        let long = ["é".repeat(MAX_DIAGNOSTIC_CHARS + 1)];
        let kept = diagnostic(&long).unwrap();
        assert_eq!(kept.chars().count(), MAX_DIAGNOSTIC_CHARS);
        assert_eq!(MAX_DIAGNOSTIC_CHARS, 512);
        assert!(FAIL_FAST_REASON.contains("fail-fast"));
    }

    #[test]
    fn a_split_job_is_a_success_only_when_every_test_is_there_once_and_passed() {
        let plan = split_plan(false);
        let g = group(&plan);
        let ok = [
            outcome("test", JobStatus::Success),
            outcome("test", JobStatus::Success),
        ];
        let both = [
            receipt_of(g, 0, TestStatus::Passed),
            receipt_of(g, 1, TestStatus::Passed),
        ];
        assert!(problems(g, &ok, &both).is_empty());
        let job = join(g, &ok, &both);
        assert_eq!((job.job.as_str(), job.status), ("test", JobStatus::Success));
        assert_eq!(job.skipped, None);

        // A shard without a receipt: its tests are missing, the job fails.
        let missing = join(g, &ok, &both[..1]);
        assert_eq!(missing.status, JobStatus::Failure);
        let why = missing.skipped.unwrap();
        assert!(
            why.contains(&format!("missing: shard {}", g.shards[1])),
            "{why}"
        );
        // A test that one shard left out.
        let mut short = both.clone();
        let dropped = short[1].tests.pop().unwrap().identity;
        let partial = join(g, &ok, &short);
        assert_eq!(partial.status, JobStatus::Failure);
        assert!(partial.skipped.unwrap().contains(&dropped));
        // A test reported by both shards.
        let mut twice = both.clone();
        let copy = twice[0].tests[0].clone();
        twice[1].tests.push(copy);
        assert_eq!(join(g, &ok, &twice).status, JobStatus::Failure);
        // Outputs the shards disagree on.
        let mut differ = ok.clone();
        differ[0].outputs.insert("v".into(), "1".into());
        differ[1].outputs.insert("v".into(), "2".into());
        let conflict = join(g, &differ, &both);
        assert_eq!(conflict.status, JobStatus::Failure);
        assert!(conflict.skipped.unwrap().contains("output `v`"));
        assert_eq!(problems(g, &differ, &both).len(), 1);

        // A job that is no success already keeps its status and its reason.
        let mut cancelled = ok.clone();
        cancelled[1].status = JobStatus::Cancelled;
        cancelled[1].skipped = Some("cancelled".into());
        let job = join(g, &cancelled, &both[..1]);
        assert_eq!(job.status, JobStatus::Cancelled);
        assert_eq!(job.skipped.as_deref(), Some("cancelled"));
        let failed = [
            receipt_of(g, 0, TestStatus::Failed),
            receipt_of(g, 1, TestStatus::Passed),
        ];
        let mut parts = ok.clone();
        parts[0].status = JobStatus::Failure;
        let job = join(g, &parts, &failed);
        assert_eq!(job.status, JobStatus::Failure);
        assert_eq!(job.skipped, None);
        assert!(problems(g, &parts, &failed)
            .iter()
            .any(|p| p.starts_with("failed: ")));
    }
}
