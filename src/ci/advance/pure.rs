//! The decisions of [`super`] that read and write nothing: what a task row
//! stands for, how an ended attempt is told to the progression, what a
//! release or a stop does to a task, and the failure of a joined job. The
//! host does the reading and the writing around them.

use super::super::progress::Input;
use crate::coordinator::TaskSpec;
use crate::ids::AttemptId;
use crate::state::{Outcome, Termination};
use local_ci::task::{JobStatus, TaskInputs};
use local_ci::WireTaskResult;

/// The inputs of a task (the outcomes of the jobs it needs) are sent in the
/// payload of its attempt, and a payload has to fit one frame of a connection
/// (2 MiB). Inputs larger than this are not sent: the task cannot be run.
pub const MAX_RELEASED_INPUT_BYTES: usize = 1 << 20;

/// Where a task stands, as far as its row says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// It has not ended (or its collection is still open).
    Open,
    /// It was cancelled before an attempt was made.
    NeverRan,
    /// Its attempt ended this way and the collection of its result is closed.
    Ended(Outcome),
}

pub fn standing(
    cancelled: bool,
    attempt: Option<(Option<&Termination>, Option<&str>)>,
) -> Standing {
    match attempt {
        None if cancelled => Standing::NeverRan,
        None => Standing::Open,
        Some((Some(termination), Some(_))) => Standing::Ended(termination.outcome()),
        Some(_) => Standing::Open,
    }
}

/// What the jobs of a task count as when its attempt ended without a result
/// the run can use.
pub fn status_without_result(outcome: Outcome) -> JobStatus {
    match outcome {
        Outcome::Cancelled => JobStatus::Cancelled,
        _ => JobStatus::Failure,
    }
}

/// Do the inputs fit the payload of an attempt?
pub fn inputs_fit(inputs: &TaskInputs) -> bool {
    serde_json::to_vec(inputs).is_ok_and(|json| json.len() <= MAX_RELEASED_INPUT_BYTES)
}

/// How a task whose attempt ended with `outcome` is told to the
/// progression: `read` is what reading its result gave.
pub fn ended_input(task: &str, outcome: Outcome, read: Result<WireTaskResult, String>) -> Input {
    let fence = |status, reason: String| Input::Fence {
        task: task.to_string(),
        status,
        reason,
    };
    match read {
        Ok(result) if result.task == task => Input::Settle {
            task: task.to_string(),
            result: Box::new(result),
        },
        Ok(_) => fence(JobStatus::Failure, "its result is of another task".into()),
        Err(why) => fence(status_without_result(outcome), why),
    }
}

/// A task cancelled before an attempt was made.
pub fn never_ran(task: &str) -> Input {
    Input::Fence {
        task: task.to_string(),
        status: JobStatus::Cancelled,
        reason: "cancelled".into(),
    }
}

/// Why a task cannot be released: its inputs do not fit a payload.
pub fn oversize_reason() -> String {
    format!("the results of the jobs it needs are larger than {MAX_RELEASED_INPUT_BYTES} bytes")
}

/// The spec of a held task as it is released: not held, with its inputs.
pub fn released_spec(spec: &TaskSpec, inputs: serde_json::Value) -> TaskSpec {
    let mut spec = spec.clone();
    if let Some(ci) = spec.ci.as_mut() {
        ci.hold = false;
        ci.task.inputs = inputs;
    }
    spec
}

/// What stopping a shard that has not ended does.
#[derive(Debug, PartialEq, Eq)]
pub enum Stop<'a> {
    /// A cancel was requested for it already: nothing is written.
    Leave,
    /// It runs: its attempt gets a cancel.
    Attempt(&'a AttemptId),
    /// It was never placed: it is cancelled with the reason.
    Unplaced,
}

pub fn stop_step(cancelled: bool, attempt: Option<&AttemptId>) -> Stop<'_> {
    match (cancelled, attempt) {
        (true, _) => Stop::Leave,
        (false, Some(attempt)) => Stop::Attempt(attempt),
        (false, None) => Stop::Unplaced,
    }
}

/// What a joined job that is a failure says: what is wrong with its shards,
/// or that a shard failed.
pub fn join_failure(status: JobStatus, problems: Option<&str>) -> Option<String> {
    (status == JobStatus::Failure)
        .then(|| problems.unwrap_or("a shard of the job failed").to_string())
}

/// The key the timings of a split job are kept under; none without a
/// project or a runner.
pub fn timing_scope(
    project: Option<&str>,
    group: &local_ci::dag::ShardGroup,
    dir: &str,
) -> Option<String> {
    Some(super::super::discovery::timing_key(
        project?,
        &group.workflow,
        group.source_jobs.first()?,
        dir,
        group.runner.as_ref()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Exit;

    fn result_of(task: &str) -> WireTaskResult {
        serde_json::from_value(serde_json::json!({
            "schema_version": local_ci::wire::WIRE_SCHEMA_VERSION,
            "task": task, "jobs": [], "outcomes": [], "receipt_ignored": null, "artifacts": [],
        }))
        .unwrap()
    }

    #[test]
    fn a_task_stands_open_until_its_attempt_ended_and_its_collection_closed() {
        let done = Termination::new(Outcome::Succeeded, Some(Exit::Code(0)), None).unwrap();
        assert_eq!(standing(false, None), Standing::Open);
        assert_eq!(standing(true, None), Standing::NeverRan);
        assert_eq!(standing(false, Some((None, None))), Standing::Open);
        assert_eq!(standing(true, Some((None, None))), Standing::Open);
        assert_eq!(standing(false, Some((Some(&done), None))), Standing::Open);
        assert_eq!(
            standing(false, Some((None, Some("complete")))),
            Standing::Open
        );
        assert_eq!(
            standing(true, Some((Some(&done), Some("gave_up")))),
            Standing::Ended(done.outcome())
        );
    }

    #[test]
    fn without_a_result_a_cancelled_attempt_counts_as_cancelled_and_any_other_as_failed() {
        assert_eq!(
            status_without_result(Outcome::Cancelled),
            JobStatus::Cancelled
        );
        for outcome in [
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::TimedOut,
            Outcome::Interrupted,
        ] {
            assert_eq!(status_without_result(outcome), JobStatus::Failure);
        }
    }

    #[test]
    fn inputs_are_released_only_when_they_fit_a_payload() {
        let outcome = |bytes: usize| local_ci::task::JobOutcome {
            job: "a".into(),
            variant: None,
            status: JobStatus::Success,
            outputs: [("o".to_string(), "x".repeat(bytes))].into(),
            skipped: None,
            ancestor_failed: false,
        };
        let inputs = |bytes| {
            let o = outcome(bytes);
            TaskInputs::new([&o], [&o], false)
        };
        let small = inputs(16);
        let overhead = serde_json::to_vec(&small).unwrap().len() - 16;
        assert!(inputs_fit(&small));
        assert!(inputs_fit(&inputs(MAX_RELEASED_INPUT_BYTES - overhead)));
        assert!(!inputs_fit(&inputs(
            MAX_RELEASED_INPUT_BYTES - overhead + 1
        )));
    }

    fn fence(task: &str, status: &str, reason: &str) -> String {
        format!("Fence {{ task: {task:?}, status: {status}, reason: {reason:?} }}")
    }

    #[test]
    fn an_ended_task_settles_with_its_own_result_and_is_fenced_otherwise() {
        let settled = ended_input("t", Outcome::Succeeded, Ok(result_of("t")));
        let text = format!("{settled:?}");
        assert!(text.starts_with("Settle { task: \"t\", result: "), "{text}");
        assert!(text.contains("task: \"t\","), "{text}");
        let other = ended_input("t", Outcome::Succeeded, Ok(result_of("u")));
        assert_eq!(
            format!("{other:?}"),
            fence("t", "Failure", "its result is of another task")
        );
        for (outcome, want) in [
            (Outcome::Cancelled, "Cancelled"),
            (Outcome::Failed, "Failure"),
        ] {
            let lost = ended_input("t", outcome, Err("gone".into()));
            assert_eq!(format!("{lost:?}"), fence("t", want, "gone"));
        }
        assert_eq!(
            format!("{:?}", never_ran("t")),
            fence("t", "Cancelled", "cancelled")
        );
    }

    #[test]
    fn a_release_clears_the_hold_and_sets_the_inputs() {
        assert_eq!(
            oversize_reason(),
            "the results of the jobs it needs are larger than 1048576 bytes"
        );
        let f = crate::ci::execute::tests::fixture(crate::ci::execute::tests::WORKFLOW);
        let digest = crate::ids::Digest::of_bytes(b"plan");
        let manifest = crate::ids::Digest::of_bytes(b"manifest");
        let specs = crate::service::ci_run::task_specs(
            &f.plan,
            &digest,
            &manifest,
            &Default::default(),
            None,
        )
        .unwrap();
        let held = specs
            .iter()
            .map(|(_, spec)| spec)
            .find(|spec| spec.ci.as_ref().is_some_and(|ci| ci.hold))
            .expect("a task that waits");
        let released = released_spec(held, serde_json::json!({"x": 1}));
        let ci = released.ci.as_ref().unwrap();
        assert!(!ci.hold);
        assert_eq!(ci.task.inputs, serde_json::json!({"x": 1}));
        // The held one is left as it was.
        assert!(held.ci.as_ref().unwrap().hold);
        // A spec that is not of a CI task has nothing to release.
        let mut plain = held.clone();
        plain.ci = None;
        assert!(released_spec(&plain, serde_json::json!(1)).ci.is_none());
    }

    #[test]
    fn a_stop_leaves_a_cancelled_shard_stops_a_running_one_and_cancels_an_unplaced_one() {
        let attempt = AttemptId::generate();
        assert_eq!(stop_step(true, Some(&attempt)), Stop::Leave);
        assert_eq!(stop_step(true, None), Stop::Leave);
        assert_eq!(stop_step(false, Some(&attempt)), Stop::Attempt(&attempt));
        assert_eq!(stop_step(false, None), Stop::Unplaced);
    }

    #[test]
    fn a_failed_job_says_what_is_wrong_with_its_shards() {
        assert_eq!(join_failure(JobStatus::Success, Some("p")), None);
        assert_eq!(join_failure(JobStatus::Cancelled, None), None);
        assert_eq!(
            join_failure(JobStatus::Failure, Some("p")),
            Some("p".to_string())
        );
        assert_eq!(
            join_failure(JobStatus::Failure, None),
            Some("a shard of the job failed".to_string())
        );
    }

    #[test]
    fn the_timings_of_a_split_job_are_kept_under_its_project_job_directory_and_runner() {
        let runner = local_ci::shard::RunnerInfo {
            runner: "cargo-test".into(),
            ..Default::default()
        };
        let group = |jobs: &[&str], runner: Option<local_ci::shard::RunnerInfo>| {
            local_ci::dag::ShardGroup {
                id: "g".into(),
                workflow: "w".into(),
                source_jobs: jobs.iter().map(|j| j.to_string()).collect(),
                shards: vec![],
                split: true,
                not_split: vec![],
                aggregation: None,
                runner,
                manifest: vec![],
                fail_fast: false,
                assignments: vec![],
                manifest_exact: false,
            }
        };
        let g = group(&["a", "b"], Some(runner.clone()));
        let want = crate::ci::discovery::timing_key("p", "w", "a", "d", &runner);
        assert_eq!(timing_scope(Some("p"), &g, "d"), Some(want.clone()));
        assert_ne!(timing_scope(Some("q"), &g, "d"), Some(want.clone()));
        assert_ne!(timing_scope(Some("p"), &g, "e"), Some(want));
        assert_eq!(timing_scope(None, &g, "d"), None);
        assert_eq!(
            timing_scope(Some("p"), &group(&[], Some(runner)), "d"),
            None
        );
        assert_eq!(timing_scope(Some("p"), &group(&["a"], None), "d"), None);
    }
}
