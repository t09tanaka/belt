//! Pure completion reduction: computation, collection and observation stay distinct.
use crate::{
    error::{ApiError, ErrorCode},
    ids::*,
    state::{AttemptState, Exit, Observation, Outcome, Termination},
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Target {
    Run(RunId),
    Task(TaskId),
}
impl Target {
    pub fn parse(id: &str) -> Result<Self, ApiError> {
        if id.starts_with("run_") {
            RunId::parse(id).map(Self::Run)
        } else {
            TaskId::parse(id).map(Self::Task)
        }
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    pub task_id: TaskId,
    pub code: ErrorCode,
    pub exit_code: i32,
    pub termination: Option<Termination>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub in_place: bool,
    pub target: Target,
    pub accepted: bool,
    pub control_revision: u64,
    pub cancel_requested: bool,
    pub task_count: u64,
    pub computed_tasks: u64,
    pub uncertain_tasks: u64,
    pub collection_pending_tasks: u64,
    pub collection_failed_tasks: u64,
    pub first_termination: Option<Termination>,
    pub first_compute_failure: Option<Failure>,
    pub first_collection_failure: Option<Failure>,
}
impl Snapshot {
    pub fn new(target: Target, accepted: bool, cancel_requested: bool) -> Self {
        Self {
            in_place: false,
            target,
            accepted,
            control_revision: 0,
            cancel_requested,
            task_count: 0,
            computed_tasks: 0,
            uncertain_tasks: 0,
            collection_pending_tasks: 0,
            collection_failed_tasks: 0,
            first_termination: None,
            first_compute_failure: None,
            first_collection_failure: None,
        }
    }
    /// Called in TaskId order from one durable read transaction.
    pub fn task(
        &mut self,
        id: TaskId,
        state: Option<AttemptState>,
        observation: Option<Observation>,
        termination: Option<Termination>,
        cancelled: bool,
        collection: Option<&str>,
    ) {
        self.task_count += 1;
        if self.task_count == 1 {
            self.first_termination = termination;
        }
        let unplaced = state.is_none() && cancelled;
        let computed = unplaced || (state == Some(AttemptState::Terminal) && termination.is_some());
        if !computed {
            self.uncertain_tasks += u64::from(matches!(
                observation,
                Some(Observation::Unknown | Observation::Reconciling)
            ));
            return;
        }
        self.computed_tasks += 1;
        if !unplaced && collection.is_none() {
            self.collection_pending_tasks += 1;
        }
        let failure = if unplaced {
            Some((ErrorCode::CommandFailed, crate::error::exit::CANCELLED))
        } else {
            termination.and_then(compute_failure)
        };
        if let Some((code, exit_code)) = failure {
            if self.first_compute_failure.is_none() {
                self.first_compute_failure = Some(Failure {
                    task_id: id.clone(),
                    code,
                    exit_code,
                    termination,
                });
            }
        }
        if matches!(collection, Some("required_missing" | "gave_up")) {
            self.collection_failed_tasks += 1;
            if self.first_collection_failure.is_none() {
                self.first_collection_failure = Some(Failure {
                    task_id: id,
                    code: ErrorCode::CollectionFailed,
                    exit_code: ErrorCode::CollectionFailed.exit_code(),
                    termination,
                });
            }
        }
    }
    pub fn stop_confirmed(&self) -> bool {
        if self.accepted {
            (self.task_count > 0 && self.computed_tasks == self.task_count)
                || (self.task_count == 0 && self.cancel_requested)
        } else {
            self.cancel_requested
        }
    }
    pub fn settled(&self) -> bool {
        self.stop_confirmed() && self.collection_pending_tasks == 0
    }
    pub fn exit_code(&self) -> Option<i32> {
        if !self.settled() {
            return None;
        }
        if !self.accepted || self.task_count == 0 {
            return Some(crate::error::exit::CANCELLED);
        }
        Some(
            self.first_compute_failure
                .as_ref()
                .or(self.first_collection_failure.as_ref())
                .map_or(0, |f| f.exit_code),
        )
    }
}
pub fn compute_failure(t: Termination) -> Option<(ErrorCode, i32)> {
    if let Some(Exit::Code(n)) = t.exit() {
        if n != 0 {
            return Some((
                ErrorCode::CommandFailed,
                ErrorCode::CommandFailed.exit_code_with(Some(n)),
            ));
        }
    }
    if let Some(Exit::Signal(n)) = t.exit() {
        return Some((ErrorCode::Signal, ErrorCode::Signal.exit_code_with(Some(n))));
    }
    match t.outcome() {
        Outcome::Succeeded => None,
        Outcome::Cancelled => Some((ErrorCode::CommandFailed, crate::error::exit::CANCELLED)),
        Outcome::TimedOut => Some((ErrorCode::CommandFailed, crate::error::exit::TIMED_OUT)),
        Outcome::Interrupted | Outcome::Failed => {
            Some((ErrorCode::CommandFailed, crate::error::exit::FAILURE))
        }
    }
}

/// Durable metadata receipt; does not contain input or secret material.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryReceipt {
    pub request_id: String,
    pub run_id: RunId,
    pub control_revision: u64,
    pub task_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_generation: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{TimeoutInfo, TimeoutKind, TimeoutPhase};
    fn id() -> TaskId {
        TaskId::generate()
    }
    fn target() -> Target {
        Target::Run(RunId::generate())
    }
    fn term(outcome: Outcome, exit: Option<Exit>) -> Termination {
        Termination::new(
            outcome,
            exit,
            if outcome == Outcome::TimedOut {
                Some(TimeoutInfo {
                    kind: TimeoutKind::Command,
                    phase: TimeoutPhase::Running,
                })
            } else {
                None
            },
        )
        .unwrap()
    }
    #[test]
    fn parses_exact_typed_targets() {
        let run = RunId::generate();
        let task = id();
        assert!(matches!(Target::parse(&run.to_string()).unwrap(),Target::Run(v) if v==run));
        assert!(matches!(Target::parse(&task.to_string()).unwrap(),Target::Task(v) if v==task));
        for value in ["run_bad", "bad", "task_bad"] {
            assert_eq!(
                Target::parse(value).unwrap_err().code,
                ErrorCode::InvalidInput
            );
        }
    }
    #[test]
    fn original_failure_and_signal_take_priority_over_runner_outcomes() {
        assert_eq!(
            compute_failure(term(Outcome::TimedOut, None)),
            Some((ErrorCode::CommandFailed, 124))
        );
        for outcome in [
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::Cancelled,
            Outcome::TimedOut,
            Outcome::Interrupted,
        ] {
            for (exit, expected) in [
                (
                    None,
                    match outcome {
                        Outcome::Succeeded => None,
                        Outcome::Cancelled => Some((ErrorCode::CommandFailed, 130)),
                        Outcome::TimedOut => Some((ErrorCode::CommandFailed, 124)),
                        _ => Some((ErrorCode::CommandFailed, 1)),
                    },
                ),
                (Some(Exit::Signal(15)), Some((ErrorCode::Signal, 143))),
                (Some(Exit::Code(77)), Some((ErrorCode::CommandFailed, 77))),
                (
                    Some(Exit::Code(0)),
                    match outcome {
                        Outcome::Succeeded => None,
                        Outcome::Cancelled => Some((ErrorCode::CommandFailed, 130)),
                        Outcome::TimedOut => Some((ErrorCode::CommandFailed, 124)),
                        _ => Some((ErrorCode::CommandFailed, 1)),
                    },
                ),
            ] {
                let Ok(t) = Termination::new(
                    outcome,
                    exit,
                    if outcome == Outcome::TimedOut {
                        Some(TimeoutInfo {
                            kind: TimeoutKind::Command,
                            phase: TimeoutPhase::Running,
                        })
                    } else {
                        None
                    },
                ) else {
                    continue;
                };
                assert_eq!(compute_failure(t), expected);
            }
        }
        assert_eq!(
            compute_failure(term(Outcome::Failed, Some(Exit::Code(23)))),
            Some((ErrorCode::CommandFailed, 23))
        );
    }
    #[test]
    fn observes_pending_unknown_cancel_and_closed_collection_separately() {
        for (state, termination) in [
            (AttemptState::Terminal, None),
            (
                AttemptState::Active,
                Some(term(Outcome::Succeeded, Some(Exit::Code(0)))),
            ),
        ] {
            let mut snapshot = Snapshot::new(target(), true, false);
            snapshot.task(
                id(),
                Some(state),
                Some(Observation::Unknown),
                termination,
                false,
                Some("complete"),
            );
            assert_eq!(
                snapshot.computed_tasks, 0,
                "state and termination must both prove completion"
            );
            assert!(!snapshot.stop_confirmed());
            assert_eq!(snapshot.exit_code(), None);
        }

        for observation in [
            None,
            Some(Observation::Known),
            Some(Observation::Unknown),
            Some(Observation::Reconciling),
        ] {
            let mut snapshot = Snapshot::new(target(), true, false);
            snapshot.task(
                id(),
                Some(AttemptState::Active),
                observation,
                None,
                true,
                None,
            );
            assert_eq!(snapshot.task_count, 1);
            assert_eq!(snapshot.computed_tasks, 0);
            assert_eq!(
                snapshot.uncertain_tasks,
                u64::from(matches!(
                    observation,
                    Some(Observation::Unknown | Observation::Reconciling)
                ))
            );
            assert!(!snapshot.stop_confirmed());
            assert!(!snapshot.settled());
            assert_eq!(snapshot.exit_code(), None);
        }
        let mut snapshot = Snapshot::new(target(), true, true);
        snapshot.task(id(), None, None, None, true, None);
        assert!(snapshot.stop_confirmed());
        assert!(snapshot.settled());
        assert_eq!(snapshot.exit_code(), Some(130));
        assert_eq!(snapshot.first_compute_failure.unwrap().termination, None);
        let mut snapshot = Snapshot::new(target(), true, false);
        snapshot.task(id(), None, None, None, false, None);
        assert_eq!(snapshot.exit_code(), None);
        for collection in [
            None,
            Some("complete"),
            Some("required_missing"),
            Some("gave_up"),
        ] {
            let mut snapshot = Snapshot::new(target(), true, false);
            let t = term(Outcome::Succeeded, Some(Exit::Code(0)));
            snapshot.task(
                id(),
                Some(AttemptState::Terminal),
                Some(Observation::Unknown),
                Some(t),
                false,
                collection,
            );
            assert_eq!(snapshot.first_termination, Some(t));
            assert!(snapshot.stop_confirmed());
            assert_eq!(
                snapshot.collection_pending_tasks,
                u64::from(collection.is_none())
            );
            assert_eq!(
                snapshot.collection_failed_tasks,
                u64::from(matches!(collection, Some("required_missing" | "gave_up")))
            );
            assert_eq!(
                snapshot.exit_code(),
                match collection {
                    None => None,
                    Some("complete") => Some(0),
                    _ => Some(78),
                }
            );
        }
    }
    #[test]
    fn run_reduction_is_stable_and_never_turns_cancel_receipt_into_stop() {
        for accepted in [false, true] {
            for cancelled in [false, true] {
                let snapshot = Snapshot::new(target(), accepted, cancelled);
                assert_eq!(snapshot.control_revision, 0);
                assert_eq!(snapshot.stop_confirmed(), cancelled);
                assert_eq!(
                    snapshot.exit_code(),
                    if cancelled { Some(130) } else { None }
                );
            }
        }
        let mut snapshot = Snapshot::new(target(), true, true);
        let first = id();
        let next = id();
        snapshot.task(
            first.clone(),
            Some(AttemptState::Terminal),
            Some(Observation::Known),
            Some(term(Outcome::Failed, Some(Exit::Code(23)))),
            true,
            Some("gave_up"),
        );
        snapshot.task(
            next.clone(),
            Some(AttemptState::Terminal),
            Some(Observation::Known),
            Some(term(Outcome::Failed, Some(Exit::Code(9)))),
            true,
            Some("required_missing"),
        );
        assert_eq!(snapshot.task_count, 2);
        assert_eq!(snapshot.computed_tasks, 2);
        assert_eq!(snapshot.collection_failed_tasks, 2);
        assert_eq!(
            snapshot.first_compute_failure.as_ref().unwrap().task_id,
            first
        );
        assert_eq!(
            snapshot.first_collection_failure.as_ref().unwrap().task_id,
            first
        );
        assert_eq!(snapshot.exit_code(), Some(23));
        let mut snapshot = Snapshot::new(target(), true, true);
        snapshot.task(
            next,
            Some(AttemptState::Terminal),
            None,
            Some(term(Outcome::Succeeded, Some(Exit::Code(0)))),
            true,
            Some("complete"),
        );
        assert_eq!(
            snapshot.exit_code(),
            Some(0),
            "natural completion must not become cancelled"
        );
    }
}
