//! Pure decisions of the coordinator: whether a worker has room, and what to
//! do next about an attempt given what the parent recorded and what its
//! worker answered.
//!
//! Nothing here touches the database, the clock or a connection. The rules
//! (spec chapters 9 to 12) in one place:
//!
//! * no answer is not an answer: the attempt becomes `unknown` and is kept;
//! * an attempt is sent again only to its own worker, and only while that
//!   worker never acknowledged it;
//! * a cancel or a retire is delivered before any resend;
//! * a task is free for another worker only after the retire was confirmed
//!   by the worker that held it.

use crate::error::ErrorCode;
use crate::state::{AttemptState, Outcome};
use crate::storage::WorkerLoad;
use crate::wire::{AdmissionWindow, RetireStatus};

/// How many tasks a worker may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Tasks whose command may be running (`starting` onward).
    pub max_active_task: u32,
    /// Tasks placed beyond the active limit, waiting to start.
    pub prefetch_per_worker: u32,
}

impl Limits {
    /// The stricter of the coordinator's setting and the worker's own.
    pub fn stricter(self, other: Limits) -> Limits {
        Limits {
            max_active_task: self.max_active_task.min(other.max_active_task),
            prefetch_per_worker: self.prefetch_per_worker.min(other.prefetch_per_worker),
        }
    }
}

/// Whether one more task may be placed on a worker with this load: the
/// tasks it holds, started or not, stay below the active limit plus the
/// prefetch. So tasks that have not started fill the free active slots and
/// the prefetch, and a worker never holds more than both together.
pub fn has_room(load: WorkerLoad, limits: Limits) -> bool {
    let held = u64::from(load.active) + u64::from(load.pending);
    held < u64::from(limits.max_active_task) + u64::from(limits.prefetch_per_worker)
}

/// What the parent recorded about an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Facts {
    /// The worker's acceptance was recorded.
    pub acked: bool,
    pub cancel_requested: bool,
    /// A retire was asked for and is not confirmed.
    pub retire_requested: bool,
}

/// What the worker answered when asked for the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    /// The worker holds the attempt.
    Status(AttemptState),
    /// The worker has neither the attempt nor a tombstone for it.
    NotFound,
    /// A tombstone without an attempt behind it: it never arrived and can
    /// never start there.
    Tombstone,
    /// No answer. Nothing is known.
    Unreachable,
}

/// The next thing to do about an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Keep it as `unknown`: not sent again, not placed elsewhere, not failed.
    KeepUnknown,
    /// Ask the worker to retire it; only its answer frees the task.
    Retire,
    /// Deliver the cancel.
    Cancel,
    /// It was cancelled before it ever arrived: the parent fixes `cancelled`.
    SettleCancelled,
    /// Send the same attempt to the same worker (again).
    Submit,
    /// It lives on the worker: mirror its state and renew its lease.
    Follow,
    /// It ended on the worker: mirror the result.
    Settle,
}

/// Decide the next step. Retire comes before cancel, and both before a
/// resend.
pub fn next_step(facts: Facts, answer: Answer) -> Step {
    match answer {
        Answer::Unreachable => Step::KeepUnknown,
        _ if facts.retire_requested => Step::Retire,
        Answer::Status(AttemptState::Terminal) => Step::Settle,
        Answer::Status(state) => {
            if facts.cancel_requested && !cancel_delivered(state) {
                Step::Cancel
            } else {
                Step::Follow
            }
        }
        Answer::Tombstone => {
            if facts.cancel_requested {
                Step::SettleCancelled
            } else {
                Step::KeepUnknown
            }
        }
        Answer::NotFound => {
            if facts.cancel_requested {
                Step::Cancel
            } else if facts.acked {
                Step::KeepUnknown
            } else {
                Step::Submit
            }
        }
    }
}

/// A worker in one of these states is already acting on a cancel.
fn cancel_delivered(state: AttemptState) -> bool {
    matches!(state, AttemptState::Cancelling | AttemptState::Finalizing)
}

/// What a submit that was answered with an error means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Not taken now, and nothing says it never will be: the same attempt is
    /// sent to the same worker again later.
    Later,
    /// A tombstone answered: the next look at the attempt settles it.
    Tombstoned,
    /// A newer coordinator exists. This one stops.
    Fenced,
    /// The worker will never take this attempt: retire it there, then fail
    /// it here.
    Never,
    /// The worker stored nothing of the attempt and the answered send was
    /// the only one: no copy of it exists anywhere. The attempt is closed
    /// here without a retire and the task is placed again, as its next
    /// generation.
    Close(Unstored),
    /// The worker stored nothing of the attempt, but a copy of its submit
    /// may still be on its way (it was sent before, a send was never
    /// answered, or how often it was sent is not known). The attempt is
    /// retired on the worker first; the task is placed again once the
    /// worker confirmed that nothing started.
    Replace(Unstored),
}

/// Why a worker stored nothing of a submit it answered with `OVERLOADED`,
/// where the task goes on as its next generation instead of waiting for the
/// same attempt to be taken (spec 11 and 24.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unstored {
    /// The worker's admission window is full, with the attempts of every
    /// member.
    Capacity,
    /// An attempt of another member holds the attempt id there.
    AttemptIdInUse,
}

/// How often the submit of an attempt was sent before the one that was just
/// answered, as recorded before each send (`None`: not known).
///
/// Exactly one copy ever left only when none was sent before. With one or
/// more before (answered or not: an unanswered send is counted too), or an
/// unknown count, a late copy may still arrive.
pub fn sent_once(sends_before: Option<u64>) -> bool {
    sends_before == Some(0)
}

/// Whether a worker's `inspect` answers that its admission window has room.
/// This is what ends the exclusion of a worker that refused a submit for
/// capacity (spec 24.4). A worker that does not say (`None`) is not held
/// back.
pub fn answers_room(admission: Option<&AdmissionWindow>) -> bool {
    admission.is_none_or(|window| window.has_room())
}

/// Whether nothing new is placed on a worker because its admission window
/// is full with the work of every member.
///
/// `refused`: it refused a submit for capacity and its `inspect` has not
/// answered room since. That alone keeps it out, whatever is counted: the
/// worker is the one that knows.
///
/// Otherwise, before anything is refused: the attempts of the other members
/// its `inspect` counted in this tick, plus what this parent holds there by
/// its own record now (`load`; the worker's count of this parent's attempts
/// is as old as the `inspect`, and a retire confirmed since has freed a
/// place). A worker that did not say (`None`) is not held back by this.
pub fn window_closed(refused: bool, admission: Option<&AdmissionWindow>, load: WorkerLoad) -> bool {
    let held = u64::from(load.active) + u64::from(load.pending);
    refused || admission.is_some_and(|window| window.foreign.saturating_add(held) >= window.limit)
}

/// Read the error a worker answered a submit with.
///
/// Only an answer that is about the attempt itself is final: its input is
/// invalid, or the worker holds the same attempt with another payload
/// (`payload_mismatch` says so by its typed reason). Every other code, a
/// fault of the worker, a conflict with something else it holds, a code
/// this version does not know, is no verdict on the attempt and never fails
/// the task. `OVERLOADED` with a reason that says the worker stored nothing
/// and will not take this attempt as it is (`unstored`, from its typed
/// reason) moves the task on to its next generation: at once when this send
/// was the only one (`sent_once`), after a retire otherwise.
pub fn submit_refusal(
    code: ErrorCode,
    payload_mismatch: bool,
    unstored: Option<Unstored>,
    sent_once: bool,
) -> Refusal {
    match (code, unstored) {
        (ErrorCode::Retired, _) => Refusal::Tombstoned,
        (ErrorCode::StaleEpoch, _) => Refusal::Fenced,
        (ErrorCode::InvalidInput, _) => Refusal::Never,
        (ErrorCode::Conflict, _) if payload_mismatch => Refusal::Never,
        (ErrorCode::Overloaded, Some(why)) if sent_once => Refusal::Close(why),
        (ErrorCode::Overloaded, Some(why)) => Refusal::Replace(why),
        _ => Refusal::Later,
    }
}

/// How the attempt stands on its worker after a retire was answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// The worker holds only the tombstone: the attempt never arrived.
    Never,
    /// The attempt is terminal there, with this outcome.
    With(Outcome),
    /// No answer, or one that says neither of the above.
    Unknown,
}

/// What the answer to a retire means for the parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireVerdict {
    /// Never started, or stopped without completing: the retire is
    /// confirmed and the task may be placed again.
    Confirmed,
    /// The command ran to an end of its own, before the retire or while it
    /// was carried out: its result stands and the task is not placed again.
    Adopt,
    /// The worker has not stopped it yet. The attempt keeps its task and
    /// its slot.
    Wait,
    /// The worker says it is over, and how it ended is not known: the
    /// attempt is kept as `unknown`.
    Unknown,
}

/// The attempt is judged by how it ended, not by which word the retire was
/// answered with. The command may end on its own between the worker's look
/// and its stop (answered `stopped`), and a retire whose first answer was
/// lost is answered `already_terminal` the second time: all of these must
/// come to the conclusion the outcome gives.
pub fn retire_verdict(status: RetireStatus, ended: Ended) -> RetireVerdict {
    match (status, ended) {
        (RetireStatus::Unconfirmed, _) => RetireVerdict::Wait,
        (_, Ended::Unknown) => RetireVerdict::Unknown,
        (_, Ended::With(outcome)) => {
            if ended_on_its_own(outcome) {
                RetireVerdict::Adopt
            } else {
                RetireVerdict::Confirmed
            }
        }
        (RetireStatus::NotStarted, Ended::Never) => RetireVerdict::Confirmed,
        // Stopped or ended, and yet not there: the two answers contradict.
        (RetireStatus::Stopped | RetireStatus::AlreadyTerminal, Ended::Never) => {
            RetireVerdict::Unknown
        }
    }
}

/// The command ran to an end of its own (`cancelled` and `interrupted` are
/// stops from outside).
pub fn ended_on_its_own(outcome: Outcome) -> bool {
    matches!(
        outcome,
        Outcome::Succeeded | Outcome::Failed | Outcome::TimedOut
    )
}

/// Whether an attempt that was just placed may be sent: not when a cancel or
/// a retire for it was recorded in the meantime (those are delivered first,
/// by the next look at it).
pub fn may_send(facts: Facts) -> bool {
    !facts.cancel_requested && !facts.retire_requested
}

/// What a worker that cannot be given a CI task lacks, as a placement
/// records it for the worker it left out.
pub fn unmet_ci(spec: &super::TaskSpec, link: &dyn super::WorkerLink) -> Option<&'static str> {
    if let Some(reason) = missing_ci_feature(
        spec,
        link.ci_task_negotiated(),
        link.ci_discovery_negotiated(),
    ) {
        return Some(reason);
    }
    if spec
        .ci
        .as_ref()
        .is_some_and(|ci| ci.task.dispatch.is_some())
        && !link.matrix_dispatch_negotiated()
    {
        return Some("protocol feature ci_matrix_dispatch_v1");
    }
    if spec
        .ci
        .as_ref()
        .is_some_and(|ci| ci.task.artifact_workflow.is_some())
        && !link.workflow_artifacts_negotiated()
    {
        return Some("protocol feature ci_workflow_artifacts_v1");
    }
    None
}

pub const DISCOVERY_UNMET: &str = "protocol feature ci_discovery_v1";
pub const CI_TASK_UNMET: &str = "protocol feature ci_task_v1";

/// Is a worker left out of the placement of `spec` because the task is a CI
/// task and its connection did not negotiate `ci_task_v1`?
pub fn lacks_ci_task(spec: &super::TaskSpec, negotiated: bool) -> bool {
    spec.ci.is_some() && !negotiated
}

pub fn missing_ci_feature(
    spec: &super::TaskSpec,
    ci: bool,
    discovery: bool,
) -> Option<&'static str> {
    if lacks_ci_task(spec, ci) {
        return Some(CI_TASK_UNMET);
    }
    (spec
        .ci
        .as_ref()
        .is_some_and(|s| crate::wire::requires_discovery_feature(&s.task))
        && !discovery)
        .then_some(DISCOVERY_UNMET)
}

/// The result of a CI task is one more required artifact of its attempt: do
/// the declarations of the task leave room for it?
pub fn ci_declarations_fit(artifacts: usize, outputs: usize) -> bool {
    artifacts.saturating_add(outputs) < crate::outputs::MAX_OUTPUTS
}

/// Why a worker is left out of the placement of a task of `run` although
/// it is attached: it did not take the input of the run. A worker where the
/// input is ready was not refused (a refusal that is still listed is stale).
pub fn input_refusal(
    ready: bool,
    refusals: Vec<(crate::ids::RunId, String)>,
    run: &crate::ids::RunId,
) -> Option<String> {
    if ready {
        return None;
    }
    refusals
        .into_iter()
        .find(|(refused, _)| refused == run)
        .map(|(_, why)| why)
}

/// The work a forecast is asked for: the commands of the task, or for a CI
/// task the commands its plan says it starts (one node per step, in order).
pub fn workload_of_spec(
    attempt_id: &crate::ids::AttemptId,
    spec: &super::TaskSpec,
    ready_delay_ms: Option<u64>,
) -> crate::wire::WorkloadPlan {
    match &spec.ci {
        Some(ci) => {
            let mut workload = crate::scheduler::ci_workload_of(
                attempt_id,
                &spec.plan_digest,
                ci.steps,
                ready_delay_ms,
            );
            for (node, key) in workload.nodes.iter_mut().zip(&ci.history_keys) {
                node.ci_history_key.clone_from(key);
            }
            workload
        }
        None => crate::scheduler::workload_of(
            attempt_id,
            &spec.plan_digest,
            &spec.commands,
            ready_delay_ms,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(active: u32, pending: u32) -> WorkerLoad {
        WorkerLoad { active, pending }
    }

    fn limits(max_active_task: u32, prefetch_per_worker: u32) -> Limits {
        Limits {
            max_active_task,
            prefetch_per_worker,
        }
    }

    fn task_spec(ci: bool) -> crate::coordinator::TaskSpec {
        let digest = crate::ids::Digest::of_bytes(b"plan");
        crate::coordinator::TaskSpec {
            ci: ci.then(|| crate::coordinator::CiSpec {
                history_keys: Vec::new(),
                dynamic_origin: false,
                core_outcome: None,
                matrix: None,
                shard_retry_forbidden: false,
                shard_preferred_worker: None,
                task: crate::wire::CiTask {
                    verify_shard_version: false,
                    artifact_workflow: None,
                    dispatch: None,
                    plan_digest: digest.clone(),
                    task_id: "ci.yml/build".into(),
                    inputs: serde_json::json!({"needs": {}}),
                },
                hold: false,
                steps: 1,
                unrunnable: None,
                shard_group: None,
                joined: false,
                join_failure: None,
                rerun: false,
            }),
            toolchains: None,
            outputs: Vec::new(),
            requirements: Default::default(),
            in_place: false,
            setup: None,
            plan_digest: digest.clone(),
            manifest_digest: digest,
            commands: Vec::new(),
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn placement_reports_the_first_required_ci_feature_the_worker_lacks() {
        use crate::coordinator::tests::Silent;
        use crate::wire::{
            CI_DISCOVERY_FEATURE, CI_TASK_FEATURE, MATRIX_DISPATCH_FEATURE,
            WORKFLOW_ARTIFACTS_FEATURE,
        };

        // Base CI support precedes discovery, matrix dispatch, and artifacts
        // when more than one required feature is absent from the worker.
        let features = [
            CI_TASK_FEATURE,
            CI_DISCOVERY_FEATURE,
            MATRIX_DISPATCH_FEATURE,
            WORKFLOW_ARTIFACTS_FEATURE,
        ];
        let subsets = |features: &[&'static str]| -> Vec<Vec<&'static str>> {
            (0..1usize << features.len())
                .map(|mask| {
                    features
                        .iter()
                        .enumerate()
                        .filter_map(|(index, feature)| {
                            (mask & (1 << index) != 0).then_some(*feature)
                        })
                        .collect()
                })
                .collect()
        };
        let optional_requirements = subsets(&features[1..]);
        for negotiated in subsets(&features) {
            let link = Silent::with_features(negotiated);
            assert_eq!(unmet_ci(&task_spec(false), &link), None);
            for optional in &optional_requirements {
                let mut spec = task_spec(true);
                let task = &mut spec.ci.as_mut().unwrap().task;
                task.verify_shard_version = optional.contains(&CI_DISCOVERY_FEATURE);
                task.dispatch = optional
                    .contains(&MATRIX_DISPATCH_FEATURE)
                    .then(|| serde_json::json!({}));
                task.artifact_workflow = optional
                    .contains(&WORKFLOW_ARTIFACTS_FEATURE)
                    .then(|| "ci.yml".into());
                let expected = std::iter::once(&CI_TASK_FEATURE)
                    .chain(optional)
                    .find(|feature| !link.features.contains(feature))
                    .map(|feature| format!("protocol feature {feature}"));
                assert_eq!(
                    unmet_ci(&spec, &link),
                    expected.as_deref(),
                    "optional {optional:?}, negotiated {:?}",
                    link.features
                );
            }
        }
    }

    #[test]
    fn a_version_bound_shard_requires_the_discovery_protocol_on_its_execution_worker() {
        let mut spec = task_spec(true);
        assert_eq!(missing_ci_feature(&spec, true, false), None);
        spec.ci.as_mut().unwrap().task.verify_shard_version = true;
        assert_eq!(
            missing_ci_feature(&spec, true, false),
            Some("protocol feature ci_discovery_v1")
        );
        assert_eq!(missing_ci_feature(&spec, true, true), None);
    }

    #[test]
    fn only_a_ci_task_is_kept_from_a_worker_that_did_not_negotiate_ci_tasks() {
        assert!(lacks_ci_task(&task_spec(true), false));
        assert!(!lacks_ci_task(&task_spec(true), true));
        assert!(!lacks_ci_task(&task_spec(false), false));
        assert!(!lacks_ci_task(&task_spec(false), true));
        assert_eq!(CI_TASK_UNMET, "protocol feature ci_task_v1");
    }

    #[test]
    fn a_worker_is_left_out_for_the_input_it_refused_only_while_it_is_not_ready() {
        let (a, b) = (crate::ids::RunId::generate(), crate::ids::RunId::generate());
        let refusals = || {
            vec![
                (a.clone(), "full".to_string()),
                (b.clone(), "part-way".to_string()),
            ]
        };
        assert_eq!(
            input_refusal(false, refusals(), &a).as_deref(),
            Some("full")
        );
        assert_eq!(
            input_refusal(false, refusals(), &b).as_deref(),
            Some("part-way")
        );
        assert_eq!(input_refusal(true, refusals(), &a), None);
        assert_eq!(input_refusal(false, Vec::new(), &a), None);
        let other = crate::ids::RunId::generate();
        assert_eq!(input_refusal(false, refusals(), &other), None);
    }

    #[test]
    fn the_declarations_of_a_ci_task_leave_room_for_its_result() {
        let max = crate::outputs::MAX_OUTPUTS;
        assert_eq!(max, 1024);
        assert!(ci_declarations_fit(0, 0));
        // 1023 declarations and the result are 1024.
        assert!(ci_declarations_fit(max - 2, 0));
        assert!(ci_declarations_fit(max - 1, 0));
        assert!(ci_declarations_fit(0, max - 1));
        assert!(ci_declarations_fit(1000, 23));
        assert!(!ci_declarations_fit(max, 0));
        assert!(!ci_declarations_fit(0, max));
        assert!(!ci_declarations_fit(1000, 24));
        assert!(!ci_declarations_fit(max + 1, 0));
        assert!(!ci_declarations_fit(1, max));
        assert!(!ci_declarations_fit(usize::MAX, 1));
        assert!(!ci_declarations_fit(usize::MAX, usize::MAX));
    }

    #[test]
    fn a_forecast_is_asked_for_the_steps_of_a_ci_task_or_the_commands_of_another() {
        let attempt = crate::ids::AttemptId::generate();
        let mut ci = task_spec(true);
        ci.ci.as_mut().unwrap().steps = 3;
        let plan = workload_of_spec(&attempt, &ci, Some(40));
        assert_eq!(
            plan,
            crate::scheduler::ci_workload_of(&attempt, &ci.plan_digest, 3, Some(40))
        );
        assert_eq!(plan.nodes.len(), 3);
        let plain = task_spec(false);
        assert_eq!(
            workload_of_spec(&attempt, &plain, None),
            crate::scheduler::workload_of(&attempt, &plain.plan_digest, &plain.commands, None)
        );
    }

    #[test]
    fn room_is_the_free_active_slots_plus_the_prefetch() {
        let l = limits(2, 1);
        assert!(has_room(load(0, 0), l));
        assert!(has_room(load(0, 2), l));
        assert!(!has_room(load(0, 3), l));
        assert!(has_room(load(1, 1), l));
        assert!(!has_room(load(1, 2), l));
        assert!(has_room(load(2, 0), l));
        assert!(!has_room(load(2, 1), l));
        assert!(!has_room(load(3, 0), l));
        assert!(!has_room(load(5, 0), l), "over the limit is not room");
        // No prefetch: only free active slots.
        assert!(has_room(load(1, 0), limits(2, 0)));
        assert!(!has_room(load(2, 0), limits(2, 0)));
        assert!(!has_room(load(0, 0), limits(0, 0)));
        assert!(has_room(load(0, 0), limits(0, 1)));
        // Neither sum wraps.
        assert!(!has_room(
            load(u32::MAX, u32::MAX),
            limits(u32::MAX, u32::MAX)
        ));
        assert!(has_room(load(u32::MAX, 0), limits(u32::MAX, 1)));
        assert!(has_room(load(1, u32::MAX), limits(u32::MAX, 2)));
        assert!(!has_room(load(1, u32::MAX), limits(u32::MAX, 1)));
    }

    #[test]
    fn the_stricter_limit_wins_field_by_field() {
        assert_eq!(limits(2, 1).stricter(limits(1, 3)), limits(1, 1));
        assert_eq!(limits(1, 3).stricter(limits(2, 1)), limits(1, 1));
        assert_eq!(limits(4, 0).stricter(limits(4, 0)), limits(4, 0));
    }

    fn facts(acked: bool, cancel_requested: bool, retire_requested: bool) -> Facts {
        Facts {
            acked,
            cancel_requested,
            retire_requested,
        }
    }

    const ANSWERS: [Answer; 5] = [
        Answer::Status(AttemptState::Active),
        Answer::Status(AttemptState::Terminal),
        Answer::NotFound,
        Answer::Tombstone,
        Answer::Unreachable,
    ];

    #[test]
    fn no_answer_keeps_the_attempt_unknown_whatever_was_asked_for() {
        for acked in [false, true] {
            for cancel in [false, true] {
                for retire in [false, true] {
                    let f = facts(acked, cancel, retire);
                    assert_eq!(next_step(f, Answer::Unreachable), Step::KeepUnknown);
                }
            }
        }
    }

    #[test]
    fn a_pending_retire_goes_first_on_every_answer() {
        for answer in ANSWERS {
            if answer == Answer::Unreachable {
                continue;
            }
            for acked in [false, true] {
                for cancel in [false, true] {
                    assert_eq!(
                        next_step(facts(acked, cancel, true), answer),
                        Step::Retire,
                        "{answer:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn an_attempt_the_worker_holds_is_followed_cancelled_or_settled() {
        for acked in [false, true] {
            for state in AttemptState::ALL {
                let plain = next_step(facts(acked, false, false), Answer::Status(state));
                let cancelled = next_step(facts(acked, true, false), Answer::Status(state));
                match state {
                    AttemptState::Terminal => {
                        assert_eq!(plain, Step::Settle);
                        assert_eq!(cancelled, Step::Settle);
                    }
                    AttemptState::Cancelling | AttemptState::Finalizing => {
                        assert_eq!(plain, Step::Follow);
                        assert_eq!(cancelled, Step::Follow, "{state:?} already stops");
                    }
                    _ => {
                        assert_eq!(plain, Step::Follow);
                        assert_eq!(cancelled, Step::Cancel, "{state:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn an_attempt_the_worker_does_not_hold_is_resent_only_while_unacknowledged() {
        assert_eq!(
            next_step(facts(false, false, false), Answer::NotFound),
            Step::Submit
        );
        assert_eq!(
            next_step(facts(true, false, false), Answer::NotFound),
            Step::KeepUnknown,
            "an acknowledged attempt that is gone is not started again"
        );
        // A cancel is delivered instead of a resend, then settled by the
        // tombstone it leaves.
        for acked in [false, true] {
            assert_eq!(
                next_step(facts(acked, true, false), Answer::NotFound),
                Step::Cancel
            );
            assert_eq!(
                next_step(facts(acked, true, false), Answer::Tombstone),
                Step::SettleCancelled
            );
            assert_eq!(
                next_step(facts(acked, false, false), Answer::Tombstone),
                Step::KeepUnknown
            );
        }
    }

    #[test]
    fn only_an_answer_about_the_attempt_itself_refuses_a_submit_for_good() {
        let whys = [
            None,
            Some(Unstored::Capacity),
            Some(Unstored::AttemptIdInUse),
        ];
        for code in ErrorCode::ALL {
            for mismatch in [false, true] {
                for why in whys {
                    for once in [false, true] {
                        let want = match (code, mismatch, why, once) {
                            (ErrorCode::Retired, _, _, _) => Refusal::Tombstoned,
                            (ErrorCode::StaleEpoch, _, _, _) => Refusal::Fenced,
                            (ErrorCode::InvalidInput, _, _, _) => Refusal::Never,
                            (ErrorCode::Conflict, true, _, _) => Refusal::Never,
                            (ErrorCode::Overloaded, _, Some(why), true) => Refusal::Close(why),
                            (ErrorCode::Overloaded, _, Some(why), false) => Refusal::Replace(why),
                            _ => Refusal::Later,
                        };
                        assert_eq!(
                            submit_refusal(code, mismatch, why, once),
                            want,
                            "{code:?} {mismatch} {why:?} {once}"
                        );
                    }
                }
            }
        }
        // A fault of the worker, or a conflict with something else it holds
        // (also one whose reason an older or newer peer did not name), is
        // not a verdict on the attempt.
        for code in [
            ErrorCode::Internal,
            ErrorCode::CommunicationUnknown,
            ErrorCode::Blocked,
            ErrorCode::NotImplemented,
            ErrorCode::ProtocolIncompatible,
            ErrorCode::NotFound,
            ErrorCode::Overloaded,
            ErrorCode::Conflict,
        ] {
            for once in [false, true] {
                assert_eq!(
                    submit_refusal(code, false, None, once),
                    Refusal::Later,
                    "{code:?}"
                );
            }
        }
        assert_eq!(
            submit_refusal(ErrorCode::Conflict, true, None, false),
            Refusal::Never
        );
        assert_eq!(
            submit_refusal(ErrorCode::Internal, true, None, false),
            Refusal::Later
        );
        // The reason belongs to `OVERLOADED`: under another code it says
        // nothing, and a plain `OVERLOADED` is sent again as before.
        for why in [Unstored::Capacity, Unstored::AttemptIdInUse] {
            assert_eq!(
                submit_refusal(ErrorCode::Overloaded, false, Some(why), true),
                Refusal::Close(why)
            );
            assert_eq!(
                submit_refusal(ErrorCode::Overloaded, false, Some(why), false),
                Refusal::Replace(why)
            );
            assert_eq!(
                submit_refusal(ErrorCode::Conflict, false, Some(why), true),
                Refusal::Later
            );
        }
    }

    #[test]
    fn only_a_first_send_that_was_answered_is_the_only_copy() {
        assert!(sent_once(Some(0)));
        // Sent before, answered or not; or not known (after a restart of a
        // database that did not count).
        assert!(!sent_once(Some(1)) && !sent_once(Some(2)) && !sent_once(Some(u64::MAX)));
        assert!(!sent_once(None));
    }

    #[test]
    fn a_window_is_closed_only_when_the_worker_said_it_is_full() {
        let window = |limit, own, foreign| AdmissionWindow {
            limit,
            used: own + foreign,
            own,
            foreign,
        };
        let load = |active, pending| WorkerLoad { active, pending };
        assert!(!window_closed(false, None, load(9, 9)));
        // The others' attempts and this parent's own fill it together.
        assert!(!window_closed(false, Some(&window(3, 0, 0)), load(0, 0)));
        assert!(!window_closed(false, Some(&window(3, 0, 2)), load(0, 0)));
        assert!(window_closed(false, Some(&window(3, 0, 3)), load(0, 0)));
        assert!(window_closed(false, Some(&window(3, 0, 4)), load(0, 0)));
        assert!(!window_closed(false, Some(&window(3, 1, 1)), load(1, 0)));
        assert!(window_closed(false, Some(&window(3, 1, 1)), load(1, 1)));
        assert!(window_closed(false, Some(&window(3, 1, 1)), load(0, 2)));
        assert!(window_closed(false, Some(&window(3, 0, 1)), load(2, 0)));
        assert!(window_closed(false, Some(&window(0, 0, 0)), load(0, 0)));
        // This parent's part is its own record now, not the worker's count
        // from the `inspect`: an attempt whose retire was confirmed since
        // has given its place back, and one placed since has taken one.
        assert!(!window_closed(false, Some(&window(1, 1, 0)), load(0, 0)));
        assert!(window_closed(false, Some(&window(2, 0, 1)), load(0, 1)));
        assert!(!window_closed(
            false,
            Some(&window(u64::MAX, 0, u64::MAX - 1)),
            load(0, 0)
        ));
        assert!(window_closed(
            false,
            Some(&window(u64::MAX, 0, u64::MAX)),
            load(1, 1)
        ));
        // A worker that refused for capacity stays out whatever is counted,
        // until its `inspect` answers room.
        assert!(window_closed(true, None, load(0, 0)));
        assert!(window_closed(true, Some(&window(3, 0, 0)), load(0, 0)));
        assert!(window_closed(true, Some(&window(3, 1, 2)), load(0, 0)));
        assert!(answers_room(None));
        assert!(answers_room(Some(&window(3, 0, 0))));
        assert!(answers_room(Some(&window(3, 1, 1))));
        assert!(!answers_room(Some(&window(3, 2, 1))));
        assert!(!answers_room(Some(&window(3, 3, 1))));
        assert!(!answers_room(Some(&window(0, 0, 0))));
    }

    #[test]
    fn a_retire_is_judged_by_how_the_attempt_ended_whatever_the_answer_was() {
        use Outcome::*;
        use RetireStatus::*;
        use RetireVerdict::{Adopt, Confirmed, Wait};
        let all = [Succeeded, Failed, Cancelled, TimedOut, Interrupted];
        // Not stopped yet: nothing else matters.
        assert_eq!(retire_verdict(Unconfirmed, Ended::Never), Wait);
        assert_eq!(retire_verdict(Unconfirmed, Ended::Unknown), Wait);
        for o in all {
            assert_eq!(retire_verdict(Unconfirmed, Ended::With(o)), Wait);
        }
        for status in [NotStarted, Stopped, AlreadyTerminal] {
            // How it ended is not known: never a reason to place it again.
            assert_eq!(
                retire_verdict(status, Ended::Unknown),
                RetireVerdict::Unknown
            );
            // The same conclusion for every word, also for `stopped` when
            // the command ended on its own while it was being stopped.
            assert_eq!(retire_verdict(status, Ended::With(Succeeded)), Adopt);
            assert_eq!(retire_verdict(status, Ended::With(Failed)), Adopt);
            assert_eq!(retire_verdict(status, Ended::With(TimedOut)), Adopt);
            assert_eq!(retire_verdict(status, Ended::With(Cancelled)), Confirmed);
            assert_eq!(retire_verdict(status, Ended::With(Interrupted)), Confirmed);
        }
        // Only a tombstone: it never arrived. Any other word contradicts.
        assert_eq!(retire_verdict(NotStarted, Ended::Never), Confirmed);
        assert_eq!(
            retire_verdict(Stopped, Ended::Never),
            RetireVerdict::Unknown
        );
        assert_eq!(
            retire_verdict(AlreadyTerminal, Ended::Never),
            RetireVerdict::Unknown
        );
        assert!(ended_on_its_own(Succeeded));
        assert!(ended_on_its_own(Failed));
        assert!(ended_on_its_own(TimedOut));
        assert!(!ended_on_its_own(Cancelled));
        assert!(!ended_on_its_own(Interrupted));
    }

    #[test]
    fn a_placed_attempt_is_sent_only_without_a_cancel_or_a_retire() {
        for acked in [false, true] {
            assert!(may_send(facts(acked, false, false)));
            assert!(!may_send(facts(acked, true, false)));
            assert!(!may_send(facts(acked, false, true)));
            assert!(!may_send(facts(acked, true, true)));
        }
    }
}
