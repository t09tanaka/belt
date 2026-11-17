//! A reusable adapter over an already frozen core CI plan.
//!
//! Snapshot capture and CLI wiring belong to the caller. This module never
//! interprets CLI arguments, captures source, registers demand or runs steps.

pub mod advance;
pub mod execute;
pub mod progress;
pub mod status;
pub mod summary;

use local_ci::FrozenPlan;
use summary::PlanSummary;

/// The exact plan an executor must reuse, together with its safe display view.
/// Deliberately neither Debug nor Serialize: core plans contain scripts/env.
pub struct CiPlan {
    frozen: FrozenPlan,
    summary: PlanSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("CI plan exceeds the input limit")]
    InputLimit,
    #[error("invalid core CI plan")]
    InvalidCorePlan,
    #[error("invalid CI planning metadata")]
    InvalidMetadata,
    #[error("core cannot describe this task without execution")]
    UnsupportedDemand,
}

impl CiPlan {
    /// Accept the caller's one frozen plan; do not re-resolve its source or
    /// selection. Verification is local and does not run workflow commands.
    pub fn from_frozen(frozen: FrozenPlan) -> Result<Self, PlanError> {
        frozen.verify().map_err(|_| PlanError::InvalidCorePlan)?;
        let summary = summary::summarize(&frozen)?;
        Ok(Self { frozen, summary })
    }

    pub fn frozen(&self) -> &FrozenPlan {
        &self.frozen
    }

    pub fn summary(&self) -> &PlanSummary {
        &self.summary
    }

    pub fn into_frozen(self) -> FrozenPlan {
        self.frozen
    }
}
