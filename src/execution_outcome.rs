//! Executor-owned observations, not a portable physical-stop capability.
use crate::cleanup::CleanupIncomplete;
use crate::task::TaskResult;
use anyhow::Result;
use std::path::PathBuf;

#[derive(Clone, Default)]
pub(crate) struct PendingOwnership {
    pub native_groups: usize,
    pub native_uncertain: Vec<u32>,
    pub external_executor: bool,
    pub containers: Vec<String>,
    pub creation_candidates: Vec<String>,
    pub compose: Vec<String>,
    pub unverified_compose: Vec<String>,
    pub port_paths: Vec<PathBuf>,
}
impl PendingOwnership {
    /// Absence in this registry is not a physical-stop capability. Wrapper,
    /// remote execution and engine-domain evidence still need host validation.
    pub(crate) fn no_known_pending(&self) -> bool {
        self.native_groups == 0
            && self.native_uncertain.is_empty()
            && !self.external_executor
            && self.containers.is_empty()
            && self.creation_candidates.is_empty()
            && self.compose.is_empty()
            && self.unverified_compose.is_empty()
            && self.port_paths.is_empty()
    }
}
impl std::fmt::Debug for PendingOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingOwnership")
            .field("native_groups", &self.native_groups)
            .field("native_uncertain", &self.native_uncertain.len())
            .field("external_executor", &self.external_executor)
            .field("containers", &self.containers.len())
            .field("creation_candidates", &self.creation_candidates.len())
            .field("compose", &self.compose.len())
            .field("unverified_compose", &self.unverified_compose.len())
            .field("port_paths", &self.port_paths.len())
            .finish()
    }
}

pub(crate) struct CleanupOutcome {
    pub diagnostics: Vec<CleanupIncomplete>,
    pub pending: PendingOwnership,
}
/// Keep actual execution data even when final cleanup/source validation fails.
/// The persistent unit host must use its own resource lifetime, not this fresh
/// attempt's context-wide cleanup policy.
pub(crate) struct AttemptExecutionOutcome {
    pub execution: Result<TaskResult>,
    pub cleanup: CleanupOutcome,
    pub final_error: Option<anyhow::Error>,
}
impl AttemptExecutionOutcome {
    pub(crate) fn legacy_result(self) -> Result<TaskResult> {
        for diagnostic in &self.cleanup.diagnostics {
            eprintln!("local-ci: warning: {diagnostic}");
        }
        if self.cleanup.diagnostics.is_empty() && !self.cleanup.pending.no_known_pending() {
            eprintln!("local-ci: warning: execution ownership remains unconfirmed");
        }
        if let Some(error) = self.final_error {
            return Err(TaskCleanupFailure {
                execution: self.execution,
                cleanup: self.cleanup,
                error,
            }
            .into());
        }
        self.execution
    }
}
/// The legacy Result shape cannot return two values, but the original result
/// remains owned by the error instead of being overwritten by cleanup's error.
struct TaskCleanupFailure {
    execution: Result<TaskResult>,
    cleanup: CleanupOutcome,
    error: anyhow::Error,
}
impl std::fmt::Display for TaskCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task cleanup incomplete: {}", self.error)
    }
}
impl std::fmt::Debug for TaskCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskCleanupFailure")
            .field("has_actual_result", &self.execution.is_ok())
            .field("pending", &self.cleanup.pending)
            .finish_non_exhaustive()
    }
}
impl std::error::Error for TaskCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(
            self.execution
                .as_ref()
                .err()
                .map_or(self.error.as_ref(), |e| e.as_ref()),
        )
    }
}

#[cfg(test)]
pub(crate) fn retained_result(error: &anyhow::Error) -> Option<&TaskResult> {
    error
        .downcast_ref::<TaskCleanupFailure>()?
        .execution
        .as_ref()
        .ok()
}
