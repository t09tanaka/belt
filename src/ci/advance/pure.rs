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

/// A task no worker could be placed for (`why`, spec 22.1): a failed job,
/// not a cancelled one, so the jobs that need it settle as after any
/// failure.
pub fn unplaceable(task: &str, why: &str) -> Input {
    Input::Fence {
        task: task.to_string(),
        status: JobStatus::Failure,
        reason: why.to_string(),
    }
}

/// Why the task of `spec` cannot be placed on any worker, if it cannot.
pub fn unplaceable_reason(spec: &serde_json::Value) -> Option<&str> {
    spec.pointer("/ci/unplaceable")?.as_str()
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

/// The plan digest a run is carried out under, as far as a task of it names
/// it: a task naming another plan is only allowed when a discovery probe
/// said so.
pub fn plan_agrees(
    current: &crate::ids::Digest,
    task: &crate::ids::Digest,
    allowable: &std::collections::BTreeSet<crate::ids::Digest>,
) -> bool {
    current == task || allowable.contains(task)
}

/// The project a run names: the first discovery marker that has one.
pub fn discovery_project<'a>(
    specs: impl IntoIterator<Item = &'a serde_json::Value>,
) -> Option<String> {
    specs.into_iter().find_map(|spec| {
        spec.pointer("/ci/task/inputs/belt_discovery/project")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    })
}

/// Have all the shards of a split job been marked joined? `joined` says it
/// of a shard; none for a shard that is not a CI task of the run.
pub fn all_joined(shards: &[String], joined: impl Fn(&str) -> Option<bool>) -> bool {
    shards.iter().all(|id| joined(id) == Some(true))
}

/// Is a task that is ready held back (so that releasing it is the step)?
/// `hold` is its mark, none for a task that is not a CI task.
pub fn is_held(hold: Option<bool>) -> bool {
    hold == Some(true)
}

/// The result of an ended attempt, read from what it kept: `kept` are its
/// artifacts as `(name, digest)`, `read` reads the one named `wanted`.
pub fn result_of_kept(
    kept: &[(String, crate::ids::Digest)],
    wanted: &str,
    read: impl FnOnce(&crate::ids::Digest) -> Result<WireTaskResult, String>,
) -> Result<WireTaskResult, String> {
    match kept.iter().find(|(name, _)| name == wanted) {
        None => Err("its result was not collected".to_string()),
        Some((_, digest)) => read(digest),
    }
}

/// What releasing a held task does.
#[derive(Debug, PartialEq)]
pub enum Release {
    /// Its inputs do not fit a payload: it is cancelled with the reason.
    Refuse(String),
    /// It goes into the queue with this spec.
    Send(serde_json::Value),
}

pub fn release_step(spec: &TaskSpec, inputs: &TaskInputs) -> Result<Release, serde_json::Error> {
    // A task no worker resolves is never let into the queue, even when a
    // retry opened it again (spec 22.1).
    if let Some(why) = spec.ci.as_ref().and_then(|ci| ci.unplaceable.as_ref()) {
        return Ok(Release::Refuse(why.clone()));
    }
    if !inputs_fit(inputs) {
        return Ok(Release::Refuse(oversize_reason()));
    }
    serde_json::to_value(inputs)
        .and_then(|inputs| serde_json::to_value(released_spec(spec, inputs)))
        .map(Release::Send)
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

    #[test]
    fn a_task_may_name_the_plan_of_the_run_or_one_a_probe_allowed() {
        let (a, b, c) = (
            crate::ids::Digest::of_bytes(b"a"),
            crate::ids::Digest::of_bytes(b"b"),
            crate::ids::Digest::of_bytes(b"c"),
        );
        let allowed: std::collections::BTreeSet<_> = [b.clone()].into();
        let none = Default::default();
        assert!(plan_agrees(&a, &a, &none));
        assert!(!plan_agrees(&a, &b, &none));
        assert!(plan_agrees(&a, &b, &allowed));
        assert!(!plan_agrees(&a, &c, &allowed));
    }

    #[test]
    fn the_project_is_the_first_discovery_marker_that_names_one() {
        let marked = |project: serde_json::Value| serde_json::json!({"ci": {"task": {"inputs": {"belt_discovery": {"project": project}}}}});
        let none = serde_json::json!({"ci": {"task": {"inputs": {}}}});
        let number = marked(serde_json::json!(3));
        let (one, two) = (marked("one".into()), marked("two".into()));
        assert_eq!(discovery_project([&none, &number]), None);
        assert_eq!(discovery_project(Vec::<&serde_json::Value>::new()), None);
        assert_eq!(
            discovery_project([&none, &number, &one, &two]),
            Some("one".to_string())
        );
    }

    #[test]
    fn a_split_job_is_joined_when_every_shard_is_marked_joined() {
        let shards = vec!["a".to_string(), "b".to_string()];
        let of = |a: Option<bool>, b: Option<bool>| move |id: &str| if id == "a" { a } else { b };
        assert!(all_joined(&shards, of(Some(true), Some(true))));
        assert!(!all_joined(&shards, of(Some(true), Some(false))));
        assert!(!all_joined(&shards, of(Some(false), Some(true))));
        assert!(!all_joined(&shards, of(Some(true), None)));
        assert!(all_joined(&[], of(None, None)));
    }

    #[test]
    fn only_a_task_marked_held_is_released() {
        assert!(is_held(Some(true)));
        assert!(!is_held(Some(false)));
        assert!(!is_held(None));
    }

    #[test]
    fn the_result_is_read_from_the_artifact_of_that_name_or_missing() {
        let (x, y) = (
            crate::ids::Digest::of_bytes(b"x"),
            crate::ids::Digest::of_bytes(b"y"),
        );
        let kept = vec![
            ("other".to_string(), x.clone()),
            ("result".to_string(), y.clone()),
        ];
        let got = result_of_kept(&kept, "result", |d| {
            assert_eq!(d, &y);
            Ok(result_of("t"))
        });
        assert_eq!(got.unwrap().task, "t");
        let unreadable = |_: &crate::ids::Digest| Err("unreadable".to_string());
        let got = result_of_kept(&kept, "result", unreadable);
        assert_eq!(got.unwrap_err(), "unreadable");
        let got = result_of_kept(&kept[..1], "result", unreadable);
        assert_eq!(got.unwrap_err(), "its result was not collected");
        let got = result_of_kept(&[], "result", unreadable);
        assert_eq!(got.unwrap_err(), "its result was not collected");
    }

    #[test]
    fn releasing_sends_the_spec_with_its_inputs_or_refuses_oversize_ones() {
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
        let small = inputs(4);
        let want = serde_json::to_value(released_spec(held, serde_json::to_value(&small).unwrap()))
            .unwrap();
        assert_eq!(release_step(held, &small).unwrap(), Release::Send(want));
        assert_eq!(
            release_step(held, &inputs(MAX_RELEASED_INPUT_BYTES)).unwrap(),
            Release::Refuse(oversize_reason())
        );
        // A task no worker can be placed for is refused whatever its inputs
        // are, even after a retry opened it again (spec 22.1).
        let mut unplaceable = held.clone();
        unplaceable.ci.as_mut().unwrap().unplaceable = Some("no worker".into());
        assert_eq!(
            release_step(&unplaceable, &small).unwrap(),
            Release::Refuse("no worker".into())
        );
    }

    /// A task no worker could be placed for is a failed job with the
    /// refusal as its reason, not a cancelled one (spec 22.1).
    #[test]
    fn an_unplaceable_task_is_a_failure_with_its_reason() {
        assert_eq!(
            format!(
                "{:?}",
                unplaceable("t", "no coherent worker proposal (rust=1.89)")
            ),
            fence("t", "Failure", "no coherent worker proposal (rust=1.89)")
        );
        let spec = serde_json::json!({"ci": {"unplaceable": "why"}});
        assert_eq!(unplaceable_reason(&spec), Some("why"));
        assert_eq!(unplaceable_reason(&serde_json::json!({"ci": {}})), None);
    }
}
