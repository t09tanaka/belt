//! The host of [`super::progress`]: what the parent reads and writes to let a
//! CI run go on.
//!
//! Nothing of a run is kept in memory between two passes. Every task of the
//! run is a row of the coordinator's store from the moment the run was
//! accepted (a task that waits for the jobs it needs is held there, not
//! placed), the frozen plan is a file under its digest, and the result of a
//! task is an artifact of its attempt. A pass replays what ended into a
//! fresh [`NeedsProgression`] and carries out what it makes ready: it writes
//! the inputs into the waiting task and lets it into the queue, in one
//! transaction that a cancel wins. A parent that restarts goes on from the
//! same rows, and a task is never made, or run, twice.

use super::progress::{ActionKind, Input, NeedsProgression, Progression};
use crate::coordinator::TaskSpec;
use crate::ids::{AttemptId, Digest, RunId, TaskId};
use crate::state::{Outcome, Termination};
use crate::storage::{CiTaskRow, CoordinatorStore};
use crate::supervisor::ci_task::RESULT_ARTIFACT;
use local_ci::task::{JobStatus, TaskInputs};
use local_ci::{FrozenPlan, WireTaskResult};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// The inputs of a task (the outcomes of the jobs it needs) are sent in the
/// payload of its attempt, and a payload has to fit one frame of a connection
/// (2 MiB). Inputs larger than this are not sent: the task cannot be run.
pub const MAX_RELEASED_INPUT_BYTES: usize = 1 << 20;
/// The result of a task is read only up to this size.
pub const MAX_RESULT_BYTES: u64 = 16 << 20;
/// How many runs one pass looks at.
const RUNS_PER_PASS: u32 = 64;

/// Keep the frozen plan of a run on the parent, under the digest of the
/// bytes a worker is sent. Returns the digest and the bytes.
pub fn keep_plan(state_dir: &Path, plan: &FrozenPlan) -> std::io::Result<(Digest, Vec<u8>)> {
    let bytes = serde_json::to_vec(plan).map_err(std::io::Error::other)?;
    let digest = crate::worker::ci_task::keep_plan(state_dir, &bytes)?;
    Ok((digest, bytes))
}

/// The bytes of a plan the parent keeps, only when they hash to `digest`.
pub fn plan_bytes(state_dir: &Path, digest: &Digest) -> Result<Vec<u8>, String> {
    let path = crate::worker::ci_task::plan_path(state_dir, digest);
    let limit = crate::supervisor::ci_task::MAX_PLAN_BYTES;
    let bytes = read_capped(&path, limit).map_err(|e| format!("the frozen plan {digest}: {e}"))?;
    if &Digest::of_bytes(&bytes) != digest {
        return Err(format!("the frozen plan {digest} does not have its digest"));
    }
    Ok(bytes)
}

fn read_capped(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let mut bytes = Vec::new();
    file.take(limit.saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::other(format!("it is larger than {limit} bytes")));
    }
    Ok(bytes)
}

/// Read the result of a CI task from the artifact the parent collected:
/// `results/<attempt>/artifacts/<hex of digest>`, only when the bytes hash
/// to the digest of its receipt.
pub fn read_result(
    results: &Path,
    attempt: &AttemptId,
    digest: &Digest,
) -> Result<WireTaskResult, String> {
    let text = digest.to_string();
    let hex = text.strip_prefix("sha256:").unwrap_or(&text);
    let path = results.join(attempt.as_str()).join("artifacts").join(hex);
    let bytes = read_capped(&path, MAX_RESULT_BYTES).map_err(|e| e.to_string())?;
    if &Digest::of_bytes(&bytes) != digest {
        return Err("the bytes do not have the digest of the receipt".into());
    }
    let text = String::from_utf8(bytes).map_err(|e| e.to_string())?;
    WireTaskResult::from_json(&text).map_err(|e| format!("{e:#}"))
}

/// Where a task of a run stands, from its row alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// It waits, is queued, runs, or its result is still being collected.
    Open,
    /// It was cancelled before it was placed: it will never run.
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

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Pass {
    /// Tasks that were let into the queue with their inputs.
    pub released: Vec<TaskId>,
    /// Tasks that cannot be run (and were cancelled), with the reason.
    pub unrunnable: Vec<(TaskId, String)>,
    /// Runs that could not be looked at, with the reason.
    pub faults: Vec<(RunId, String)>,
    /// The plans of the runs that still have a task waiting.
    pub plans: std::collections::BTreeSet<Digest>,
}

/// Let every run that has a task waiting go on as far as it can.
/// `state_dir` holds the frozen plans, `results` what was collected. A run
/// that cannot be read is left as it is (and reported in `faults`); the
/// others go on.
pub fn advance_runs(
    store: &CoordinatorStore,
    state_dir: &Path,
    results: &Path,
    now_ms: u64,
) -> crate::storage::Result<Pass> {
    let mut pass = Pass::default();
    for run in store.ci_waiting_runs(RUNS_PER_PASS)? {
        if let Err(why) = advance_run(store, state_dir, results, &run, now_ms, &mut pass) {
            pass.faults.push((run, why));
        }
    }
    Ok(pass)
}

struct Member {
    row: CiTaskRow,
    spec: TaskSpec,
}

fn advance_run(
    store: &CoordinatorStore,
    state_dir: &Path,
    results: &Path,
    run: &RunId,
    now_ms: u64,
    pass: &mut Pass,
) -> Result<(), String> {
    let fault = |e: crate::storage::StorageError| e.to_string();
    let mut members: BTreeMap<String, Member> = BTreeMap::new();
    let mut digest = None;
    for row in store.ci_run_tasks(run).map_err(fault)? {
        let spec: TaskSpec = serde_json::from_value(row.spec.clone())
            .map_err(|e| format!("task {}: {e}", row.task_id))?;
        let Some(ci) = &spec.ci else {
            return Err(format!("task {} is not a CI task", row.task_id));
        };
        if digest.get_or_insert_with(|| ci.task.plan_digest.clone()) != &ci.task.plan_digest {
            return Err("the tasks of the run name different plans".into());
        }
        if members
            .insert(ci.task.task_id.clone(), Member { row, spec })
            .is_some()
        {
            return Err("two tasks of the run are the same task of the plan".into());
        }
    }
    let digest = digest.ok_or("the run has no task")?;
    pass.plans.insert(digest.clone());
    let plan: FrozenPlan = serde_json::from_slice(&plan_bytes(state_dir, &digest)?)
        .map_err(|e| format!("the frozen plan {digest}: {e}"))?;
    let mut progress = NeedsProgression::new(&plan).map_err(|e| e.to_string())?;

    // What ended, the tasks a task depends on before the task itself.
    loop {
        let mut settled = false;
        for task in &plan.tasks {
            if progress.is_settled(&task.id) || !progress.dependencies_settled(&task.id) {
                continue;
            }
            let Some(member) = members.get(&task.id) else {
                return Err(format!("the run has no task `{}` of its plan", task.id));
            };
            let Some(input) = ended(store, results, &task.id, &member.row).map_err(fault)? else {
                continue;
            };
            progress.step(input).map_err(|e| e.to_string())?;
            settled = true;
        }
        if !settled {
            break;
        }
    }
    progress.step(Input::Advance).map_err(|e| e.to_string())?;
    for action in progress.pending_actions().to_vec() {
        if let ActionKind::Ready { task, inputs } = &action.kind {
            let member = members.get(task).ok_or("the plan changed")?;
            if member.spec.ci.as_ref().is_some_and(|ci| ci.hold) {
                release(store, run, member, inputs, now_ms, pass).map_err(fault)?;
            }
        }
        progress
            .step(Input::Ack { action: action.id })
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// How the task of `row` ended, as an input of the progression; `None`
/// while it has not (or its result is still being collected).
fn ended(
    store: &CoordinatorStore,
    results: &Path,
    task: &str,
    row: &CiTaskRow,
) -> crate::storage::Result<Option<Input>> {
    let attempt = row
        .attempt
        .as_ref()
        .map(|a| (a.termination.as_ref(), a.collection.as_deref()));
    let fence = |status, reason: String| Input::Fence {
        task: task.to_string(),
        status,
        reason,
    };
    Ok(match (standing(row.cancelled, attempt), &row.attempt) {
        (Standing::NeverRan, _) => Some(fence(JobStatus::Cancelled, "cancelled".into())),
        (Standing::Ended(outcome), Some(attempt)) => {
            let kept = store.persisted_artifacts(&attempt.attempt_id)?;
            let read = match kept.iter().find(|(name, _)| name == RESULT_ARTIFACT) {
                None => Err("its result was not collected".to_string()),
                Some((_, digest)) => read_result(results, &attempt.attempt_id, digest),
            };
            Some(match read {
                Ok(result) if result.task == task => Input::Settle {
                    task: task.to_string(),
                    result: Box::new(result),
                },
                Ok(_) => fence(JobStatus::Failure, "its result is of another task".into()),
                Err(why) => fence(status_without_result(outcome), why),
            })
        }
        _ => None,
    })
}

fn release(
    store: &CoordinatorStore,
    run: &RunId,
    member: &Member,
    inputs: &TaskInputs,
    now_ms: u64,
    pass: &mut Pass,
) -> crate::storage::Result<()> {
    let task_id = &member.row.task_id;
    let invalid = |e: serde_json::Error| crate::storage::StorageError::Invalid(e.to_string());
    if !inputs_fit(inputs) {
        let why = format!(
            "the results of the jobs it needs are larger than {MAX_RELEASED_INPUT_BYTES} bytes"
        );
        let payload = serde_json::json!({ "task": task_id.to_string(), "reason": why });
        store.append_event(Some(run), None, "ci_task_unrunnable", &payload, now_ms)?;
        store.cancel_task(task_id, now_ms)?;
        pass.unrunnable.push((task_id.clone(), why));
        return Ok(());
    }
    let mut spec = member.spec.clone();
    if let Some(ci) = spec.ci.as_mut() {
        ci.hold = false;
        ci.task.inputs = serde_json::to_value(inputs).map_err(invalid)?;
    }
    let spec = serde_json::to_value(&spec).map_err(invalid)?;
    if store.release_ci_task(task_id, &spec, now_ms)? {
        pass.released.push(task_id.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Exit;

    #[test]
    fn a_task_stands_open_until_its_attempt_ended_and_its_collection_closed() {
        let done = Termination::new(Outcome::Succeeded, Some(Exit::Code(0)), None).unwrap();
        assert_eq!(standing(false, None), Standing::Open);
        assert_eq!(standing(true, None), Standing::NeverRan);
        assert_eq!(standing(false, Some((None, None))), Standing::Open);
        assert_eq!(standing(true, Some((None, None))), Standing::Open);
        assert_eq!(standing(false, Some((Some(&done), None))), Standing::Open);
        assert_eq!(standing(false, Some((None, Some("complete")))), Standing::Open);
        assert_eq!(
            standing(true, Some((Some(&done), Some("gave_up")))),
            Standing::Ended(done.outcome())
        );
    }

    #[test]
    fn without_a_result_a_cancelled_attempt_counts_as_cancelled_and_any_other_as_failed() {
        assert_eq!(status_without_result(Outcome::Cancelled), JobStatus::Cancelled);
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
        assert!(!inputs_fit(&inputs(MAX_RELEASED_INPUT_BYTES - overhead + 1)));
    }

    #[test]
    fn a_plan_and_a_result_are_read_only_when_they_hash_to_their_digest() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let f = crate::ci::execute::tests::fixture(crate::ci::execute::tests::WORKFLOW);
        let (digest, bytes) = keep_plan(dir.path(), &f.plan).unwrap();
        assert_eq!(digest, Digest::of_bytes(&bytes));
        assert_eq!(plan_bytes(dir.path(), &digest).unwrap(), bytes);
        let path = crate::worker::ci_task::plan_path(dir.path(), &digest);
        std::fs::write(&path, b"{}").unwrap();
        assert!(plan_bytes(dir.path(), &digest).unwrap_err().contains("digest"));
        let absent = Digest::of_bytes(b"absent");
        assert!(plan_bytes(dir.path(), &absent).is_err());

        let attempt = AttemptId::generate();
        let text = serde_json::json!({
            "schema_version": local_ci::wire::WIRE_SCHEMA_VERSION,
            "task": "t", "jobs": [], "outcomes": [], "receipt_ignored": null, "artifacts": [],
        })
        .to_string();
        let digest = Digest::of_bytes(text.as_bytes());
        let hex = digest.to_string().replace("sha256:", "");
        let kept = dir.path().join(attempt.as_str()).join("artifacts");
        std::fs::create_dir_all(&kept).unwrap();
        std::fs::write(kept.join(&hex), &text).unwrap();
        assert_eq!(read_result(dir.path(), &attempt, &digest).unwrap().task, "t");
        // Not JSON of a result.
        let junk = Digest::of_bytes(b"junk");
        let junk_hex = junk.to_string().replace("sha256:", "");
        std::fs::write(kept.join(&junk_hex), b"junk").unwrap();
        assert!(read_result(dir.path(), &attempt, &junk).is_err());
        // Other bytes under the name of the receipt.
        std::fs::write(kept.join(&hex), b"{}").unwrap();
        let e = read_result(dir.path(), &attempt, &digest).unwrap_err();
        assert!(e.contains("digest"), "{e}");
        // Nothing there.
        assert!(read_result(dir.path(), &AttemptId::generate(), &digest).is_err());
        // Larger than a result may be.
        let large = dir.path().join("large");
        std::fs::write(&large, vec![b'x'; 9]).unwrap();
        assert!(read_capped(&large, 8).is_err());
        assert_eq!(read_capped(&large, 9).unwrap().len(), 9);
    }
}
