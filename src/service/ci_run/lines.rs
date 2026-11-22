//! The lines of a run, as `ci` reports them: what a task's row and result
//! say, the one line of a split job after its shards, and the text printed.
//! Nothing here reads a store or a file.

use super::{JobLine, Part};
use crate::ids::TaskId;
use local_ci::WireTaskResult;

/// Does a task of the run have lines? A discovery probe does not: it only
/// finds the plan.
pub(super) fn has_lines(inputs: &serde_json::Value) -> bool {
    inputs.get("belt_discovery").is_none()
}

/// Is a split job in `plan`? Without one there is nothing to mark.
pub(super) fn has_split_job(plan: &local_ci::FrozenPlan) -> bool {
    plan.shard_groups.iter().any(|group| group.split)
}

/// The lines of one task: a line for each job its result outcomes, or one
/// line of the task when it left no result (`core_outcome`, else
/// `no_result`).
pub(super) fn row_lines(
    task_id: &TaskId,
    plan_task: &str,
    unrunnable: Option<&str>,
    core_outcome: Option<&str>,
    result: Option<WireTaskResult>,
) -> Vec<JobLine> {
    let line = |job: Option<String>, status: &str, skipped: Option<String>| JobLine {
        task_id: task_id.clone(),
        plan_task: plan_task.to_string(),
        job,
        status: status.to_string(),
        part: None,
        skipped,
        reason: unrunnable.map(str::to_string),
        superseded: false,
    };
    match result {
        Some(result) => result
            .outcomes
            .into_iter()
            .map(|o| line(Some(o.job), o.status.as_str(), o.skipped))
            .collect(),
        None => vec![line(None, core_outcome.unwrap_or("no_result"), None)],
    }
}

/// The lines of the split jobs of `plan`: each shard's lines are marked,
/// and a job whose shards all ended gets one line more, after them, with
/// the one outcome of the job (as the progression of the run makes it).
pub(super) fn mark_split_jobs(
    lines: &mut Vec<JobLine>,
    plan: &local_ci::FrozenPlan,
    progress: &crate::ci::progress::NeedsProgression,
) {
    use crate::ci::progress::Progression;
    for group in plan.shard_groups.iter().filter(|g| g.split) {
        let mut last = None;
        for (at, line) in lines.iter_mut().enumerate() {
            if group.shards.contains(&line.plan_task) {
                line.part = Some(Part::Shard);
                last = Some(at);
            }
        }
        let job = group.source_jobs.join("+");
        let (Some(last), Some(outcome)) = (last, progress.outcome(&group.workflow, &job)) else {
            continue;
        };
        let first = lines.iter().find(|line| line.plan_task == group.shards[0]);
        let line = JobLine {
            task_id: first.unwrap_or(&lines[last]).task_id.clone(),
            plan_task: group.id.clone(),
            job: Some(job),
            status: outcome.status.as_str().to_string(),
            part: Some(Part::Job),
            // A job that failed says what is wrong with its shards.
            skipped: outcome
                .skipped
                .clone()
                .filter(|_| outcome.status.as_str() != "failure"),
            reason: progress.split_job_problems(&group.id).map(str::to_string),
            superseded: false,
        };
        // The job counts once: its shards stay as lines, no more.
        for line in lines
            .iter_mut()
            .filter(|l| group.shards.contains(&l.plan_task))
        {
            line.superseded = true;
        }
        lines.insert(last + 1, line);
    }
}

/// One line per job, as `ci` prints it.
pub fn render_jobs(jobs: &[JobLine]) -> String {
    jobs.iter()
        .map(|line| {
            // A shard is named as the task it is: its job is on a line
            // of its own.
            let job = line
                .job
                .as_deref()
                .filter(|_| line.part != Some(Part::Shard));
            let name = job.unwrap_or(&line.plan_task);
            match line.skipped.as_ref().or(line.reason.as_ref()) {
                Some(why) => format!("{name}: {} ({why})\n", line.status),
                None => format!("{name}: {}\n", line.status),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_discovery_probe_has_no_lines_and_a_plan_needs_a_split_group_to_be_marked() {
        assert!(has_lines(&serde_json::json!({})));
        assert!(has_lines(&serde_json::json!({"other": 1})));
        assert!(!has_lines(&serde_json::json!({"belt_discovery": null})));
        assert!(!has_lines(
            &serde_json::json!({"belt_discovery": {"project": "p"}})
        ));
        let mut plan = split_plan(false);
        assert!(has_split_job(&plan));
        for group in &mut plan.shard_groups {
            group.split = false;
        }
        assert!(!has_split_job(&plan));
        plan.shard_groups.clear();
        assert!(!has_split_job(&plan));
    }

    use super::*;
    use crate::ci::progress::{Input, NeedsProgression, Progression};
    use crate::ci::shards::tests::{group, split_plan};
    use local_ci::task::JobStatus;

    #[test]
    fn a_job_is_printed_with_its_status_and_why_it_did_not_run() {
        let line = |job: Option<&str>, status: &str, skipped: Option<&str>| JobLine {
            task_id: TaskId::generate(),
            plan_task: "ci.yml/deploy".into(),
            job: job.map(str::to_string),
            status: status.into(),
            part: None,
            skipped: skipped.map(str::to_string),
            reason: None,
            superseded: false,
        };
        let unrunnable = JobLine {
            reason: Some("the frozen plan is gone".into()),
            ..line(None, "no_result", None)
        };
        let text = render_jobs(&[
            line(Some("build"), "failure", None),
            line(Some("test"), "skipped", Some("needs failed")),
            line(None, "no_result", None),
            unrunnable,
        ]);
        assert_eq!(
            text,
            "build: failure\ntest: skipped (needs failed)\nci.yml/deploy: no_result\nci.yml/deploy: no_result (the frozen plan is gone)\n"
        );
        assert_eq!(render_jobs(&[]), "");
        // A shard is named as its task; the split job has its own line.
        let part = |part: Part, status: &str, why: Option<&str>| JobLine {
            part: Some(part),
            ..line(Some("test"), status, why)
        };
        let text = render_jobs(&[
            part(Part::Shard, "success", None),
            part(Part::Job, "failure", Some("missing: shard x")),
        ]);
        assert_eq!(
            text,
            "ci.yml/deploy: success\ntest: failure (missing: shard x)\n"
        );

        let shard = part(Part::Shard, "failure", None);
        let json = serde_json::to_value(&shard).unwrap();
        assert_eq!(json["part"], "shard");
        let json = serde_json::to_value(line(None, "no_result", None)).unwrap();
        assert!(json.get("part").is_none());
    }

    fn result_with(outcomes: &[(&str, JobStatus, Option<&str>)]) -> WireTaskResult {
        WireTaskResult {
            schema_version: local_ci::wire::WIRE_SCHEMA_VERSION,
            task: "t".into(),
            jobs: vec![],
            outcomes: outcomes
                .iter()
                .map(|(job, status, skipped)| local_ci::task::JobOutcome {
                    job: job.to_string(),
                    variant: None,
                    status: *status,
                    outputs: Default::default(),
                    skipped: skipped.map(str::to_string),
                    ancestor_failed: false,
                })
                .collect(),
            receipt_ignored: None,
            artifacts: vec![],
            shard: None,
        }
    }

    #[test]
    fn a_task_has_a_line_per_job_of_its_result_or_one_of_its_own() {
        let id = TaskId::generate();
        let result = result_with(&[
            ("a", JobStatus::Success, None),
            ("b", JobStatus::Cancelled, Some("needs failed")),
        ]);
        let lines = row_lines(&id, "wf/a+b", Some("why"), Some("ignored"), Some(result));
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].task_id, id);
        assert_eq!(lines[0].plan_task, "wf/a+b");
        assert_eq!(lines[0].job.as_deref(), Some("a"));
        assert_eq!(lines[0].status, "success");
        assert_eq!(lines[0].skipped, None);
        assert_eq!(lines[0].part, None);
        assert_eq!(lines[0].reason.as_deref(), Some("why"));
        assert_eq!(lines[1].job.as_deref(), Some("b"));
        assert_eq!(lines[1].status, "cancelled");
        assert_eq!(lines[1].skipped.as_deref(), Some("needs failed"));
        // A result without a job has no line.
        assert!(row_lines(&id, "p", None, None, Some(result_with(&[]))).is_empty());
        // No result: the line of the task, with what the core said or none.
        let lines = row_lines(&id, "p", None, Some("cancelled"), None);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].job, None);
        assert_eq!(lines[0].status, "cancelled");
        assert_eq!(lines[0].reason, None);
        assert_eq!(row_lines(&id, "p", None, None, None)[0].status, "no_result");
    }

    fn shard_lines(plan: &local_ci::FrozenPlan) -> Vec<JobLine> {
        group(plan)
            .shards
            .iter()
            .map(|shard| JobLine {
                task_id: TaskId::generate(),
                plan_task: shard.clone(),
                job: Some("test".into()),
                status: "failure".into(),
                part: None,
                skipped: None,
                reason: None,
                superseded: false,
            })
            .collect()
    }

    fn fenced(plan: &local_ci::FrozenPlan, status: JobStatus, shards: usize) -> NeedsProgression {
        let mut progress = NeedsProgression::new(plan).unwrap();
        for task in group(plan).shards.iter().take(shards) {
            progress
                .step(Input::Fence {
                    task: task.clone(),
                    status,
                    reason: "stopped".into(),
                })
                .unwrap();
        }
        progress
    }

    #[test]
    fn the_shards_of_a_split_job_are_marked_and_the_job_follows_them_once_it_has_its_outcome() {
        let plan = split_plan(false);
        let g = group(&plan).clone();
        let mut other = JobLine {
            task_id: TaskId::generate(),
            plan_task: "elsewhere".into(),
            job: Some("report".into()),
            status: "success".into(),
            part: None,
            skipped: None,
            reason: None,
            superseded: false,
        };
        // A shard that has not ended: the shards are marked, the job has no line.
        let mut lines = shard_lines(&plan);
        lines.push(other.clone());
        mark_split_jobs(&mut lines, &plan, &fenced(&plan, JobStatus::Failure, 1));
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].part, Some(Part::Shard));
        assert_eq!(lines[1].part, Some(Part::Shard));
        assert_eq!(lines[2], other);
        // No line of the job yet: the shards are what counts.
        assert!(lines.iter().all(|line| !line.superseded));

        // Both ended in failure: the line of the job follows the last shard.
        let mut lines = shard_lines(&plan);
        let first = lines[0].task_id.clone();
        lines.push(other.clone());
        let progress = fenced(&plan, JobStatus::Failure, 2);
        mark_split_jobs(&mut lines, &plan, &progress);
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[2].part, Some(Part::Job));
        assert_eq!(lines[2].plan_task, g.id);
        assert_eq!(lines[2].task_id, first);
        assert_eq!(lines[2].job.as_deref(), Some("test"));
        assert_eq!(lines[2].status, "failure");
        // A job that failed says what is wrong with its shards, not why it
        // was skipped.
        assert_eq!(lines[2].skipped, None);
        assert_eq!(
            lines[2].reason.as_deref(),
            progress.split_job_problems(&g.id)
        );
        assert!(lines[2].reason.is_some());
        assert_eq!(lines[3], other);
        // The job counts, its shards do not, and a line of another task
        // is left as it is.
        assert_eq!(
            lines.iter().map(|l| l.superseded).collect::<Vec<_>>(),
            [true, true, false, false]
        );

        // A job that did not fail keeps its reason for not running.
        let mut lines = shard_lines(&plan);
        mark_split_jobs(&mut lines, &plan, &fenced(&plan, JobStatus::Cancelled, 2));
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[2].status, "cancelled");
        assert_eq!(lines[2].skipped.as_deref(), Some("stopped"));

        // A job that is not split, or whose shards have no line, adds none.
        other.part = None;
        let mut lines = vec![other.clone()];
        mark_split_jobs(&mut lines, &plan, &fenced(&plan, JobStatus::Failure, 2));
        assert_eq!(lines, vec![other]);
        let mut unsplit = plan.clone();
        for g in &mut unsplit.shard_groups {
            g.split = false;
        }
        let mut lines = shard_lines(&plan);
        mark_split_jobs(&mut lines, &unsplit, &fenced(&plan, JobStatus::Failure, 2));
        assert_eq!(lines.len(), 2);
        assert!(lines.iter().all(|line| line.part.is_none()));
    }
}
