//! Durable state (spec chapter 10, 11, 12, 13, 18).
//!
//! The coordinator and every worker keep a local SQLite database (WAL,
//! `synchronous = FULL`) as the source of truth. A [`Db`] owns one connection
//! on a dedicated thread; the repositories ([`CoordinatorStore`],
//! [`WorkerStore`]) are synchronous, so async code calls them through
//! `tokio::task::spawn_blocking`. Every operation that must survive a crash is
//! one transaction, committed before the method returns.
//!
//! The state machine lives in `crate::state`; storage only applies its
//! verdicts, guarded by conditional updates. The decisions that are pure
//! (schema compatibility, idempotent submit, launch claims, lease renewal, GC
//! selection) live in [`policy`].

mod common;
mod coordinator;
pub use coordinator::query;
mod db;
#[cfg(test)]
mod group_tests;
mod lock;
#[cfg(test)]
mod paging_tests;
pub mod policy;
#[cfg(test)]
mod pruning_tests;
#[cfg(test)]
mod retry_dependents_tests;
mod schema;
#[cfg(test)]
mod tests;
mod toolchain_roots;
mod worker;

pub use common::{BlobKind, BlobRef, Event, GcReport, NewBlobRef, DEFAULT_GC_BATCH};
pub(crate) use coordinator::MAX_TASKS_PER_RUN;
pub(crate) use coordinator::RUN_PRUNED;
pub use coordinator::{
    ArtifactReceipt, AttemptRecord, Collectable, CollectionRow, CoordinatorStore, Forgotten,
    InputCursor, InputPage, LiveAttempt, LiveAttemptPage, NewAttempt, Permit, PlaceInput,
    PlaceOutcome, Placement, PreparedRetry, QueuedTask, ResultRef, RetireEnd, RetryPreparation,
    RunStanding, Seen, WorkerLoad, PAGE_MAX,
};
pub use coordinator::{CiAttempt, CiTaskRow, WorkflowArtifact, WorkflowUpload};
pub use coordinator::{LiveLogTarget, LogProgress, ResultDeleteClaim, ResultGcPage};
pub use db::Db;
pub use lock::ProcessLock;
pub use toolchain_roots::{Bind, RootRow};
pub(crate) use worker::MembershipImage;
pub use worker::{
    AcceptedAttempt, CancelOutcome, KeyGeneration, LaunchClaim, LeaseOutcome, LogPosition,
    MemberPin, NewAcceptance, ProcessIdentity, SubmitOutcome, WorkerStore, DEFAULT_ACK_WAIT_MS,
    MAX_UNDELIVERED_TOMBSTONES,
};

use crate::error::ErrorCode;
use crate::ids::{AttemptId, CoordinatorEpoch};
use crate::state::{AttemptState, Termination, TransitionError};
use policy::Incompat;
use std::path::PathBuf;
use thiserror::Error;

/// Which of the two databases a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Coordinator,
    Worker,
}

impl DbKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DbKind::Coordinator => "coordinator",
            DbKind::Worker => "worker",
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sqlite: {0}")]
    Sqlite(rusqlite::Error),
    /// Another connection held the database past the busy timeout. Nothing of
    /// this call was committed; the caller may retry it.
    #[error("database is busy (retry)")]
    Busy,
    /// The locator's last reference was dropped by GC and the file is being
    /// deleted. Make a new file under another locator.
    #[error("locator {0} is queued for deletion")]
    LocatorPendingDelete(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("database is not usable by this binary: {0:?}")]
    Incompatible(Incompat),
    /// A migration step whose precondition does not hold. Nothing was
    /// changed; the message says what to do.
    #[error("the database cannot be migrated yet: {0}")]
    MigrationRefused(String),
    /// A per-member limit is reached. Nothing of the call was committed.
    #[error("{0}")]
    Overloaded(String),
    #[error("database holds a {found} schema, expected {expected}")]
    KindMismatch { expected: String, found: String },
    #[error("another process holds {}", .0.display())]
    Locked(PathBuf),
    #[error("{what} not found")]
    NotFound { what: String },
    #[error("attempt {0} was submitted with a different payload")]
    PayloadConflict(AttemptId),
    #[error("attempt {0} is retired")]
    Retired(AttemptId),
    #[error("coordinator epoch {got} is older than {current}")]
    StaleEpoch {
        got: CoordinatorEpoch,
        current: CoordinatorEpoch,
    },
    #[error("task already has a live attempt ({0})")]
    LiveAttemptExists(String),
    /// The attempt id is held by an attempt of another member on this
    /// worker (the attempt's files, its supervisor and its commands are
    /// named by the attempt id alone). Nothing of the call was committed.
    #[error(
        "attempt id {0} is in use on this worker; place the task again under a new attempt id"
    )]
    AttemptIdInUse(AttemptId),
    /// The admission window of this worker is full, counted over every
    /// member's attempts that are not terminal. Nothing of the call was
    /// committed.
    #[error("{live} attempts are not terminal on this worker and its window is {window}")]
    Capacity { live: u64, window: u64 },
    #[error("lease for {0} cannot be renewed")]
    LeaseNotRenewable(AttemptId),
    #[error(transparent)]
    Transition(#[from] TransitionError),
    #[error("attempt {attempt} is {state:?}: {why}")]
    IllegalState {
        attempt: String,
        state: AttemptState,
        why: &'static str,
    },
    #[error("captured inputs are temporarily busy; retry the request")]
    SourceBusy,
    #[error("retained secret bundle unavailable; submit a new run")]
    SecretUnavailable,
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("already fixed and different: {0}")]
    Immutable(String),
    #[error("stored data is corrupt: {0}")]
    Corrupt(String),
    #[error("database thread is gone")]
    Closed,
    #[error("a storage operation panicked")]
    Panicked,
}

impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        match e.sqlite_error_code() {
            Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                StorageError::Busy
            }
            _ => StorageError::Sqlite(e),
        }
    }
}

impl StorageError {
    /// True when the same call may succeed later without any input change
    /// (database or foreground source retention contention). Everything else
    /// is a verdict or a fault.
    pub fn is_retryable(&self) -> bool {
        matches!(self, StorageError::Busy | StorageError::SourceBusy)
    }

    /// True for a refusal that says "nothing was done": the request left no
    /// trace at all, not even the newer coordinator epoch it carried, and
    /// nothing it would have purged is gone. A verdict about the attempt
    /// itself (retired, another payload, not renewable, ...) is not one of
    /// these: the worker has seen the newer coordinator and says so.
    pub fn leaves_nothing(&self) -> bool {
        matches!(
            self,
            StorageError::Overloaded(_)
                | StorageError::AttemptIdInUse(_)
                | StorageError::Capacity { .. }
                | StorageError::LiveAttemptExists(_)
                | StorageError::Invalid(_)
        )
    }

    /// The wire error code this maps to.
    pub fn code(&self) -> ErrorCode {
        match self {
            StorageError::Incompatible(_)
            | StorageError::KindMismatch { .. }
            | StorageError::MigrationRefused(_) => ErrorCode::ProtocolIncompatible,
            StorageError::Overloaded(_)
            | StorageError::AttemptIdInUse(_)
            | StorageError::Capacity { .. } => ErrorCode::Overloaded,
            StorageError::Locked(_)
            | StorageError::PayloadConflict(_)
            | StorageError::Immutable(_)
            | StorageError::LiveAttemptExists(_)
            | StorageError::LocatorPendingDelete(_) => ErrorCode::Conflict,
            StorageError::SecretUnavailable => ErrorCode::Blocked,
            StorageError::SourceBusy => ErrorCode::SourceBusy,
            StorageError::NotFound { .. } => ErrorCode::NotFound,
            StorageError::Retired(_) => ErrorCode::Retired,
            StorageError::StaleEpoch { .. } => ErrorCode::StaleEpoch,
            StorageError::LeaseNotRenewable(_) => ErrorCode::LeaseNotRenewable,
            StorageError::Invalid(_) | StorageError::Transition(_) => ErrorCode::InvalidInput,
            StorageError::IllegalState { .. } => ErrorCode::Conflict,
            StorageError::Sqlite(_)
            | StorageError::Busy
            | StorageError::Io(_)
            | StorageError::Corrupt(_)
            | StorageError::Closed
            | StorageError::Panicked => ErrorCode::Internal,
        }
    }
}

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

/// Whether a terminal write won or found an earlier result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finalized {
    /// This call fixed the termination.
    Won(Termination),
    /// Somebody fixed it first; this is theirs.
    AlreadyTerminal(Termination),
}

impl Finalized {
    pub fn termination(&self) -> Termination {
        match self {
            Finalized::Won(t) | Finalized::AlreadyTerminal(t) => *t,
        }
    }

    pub fn won(&self) -> bool {
        matches!(self, Finalized::Won(_))
    }
}

pub(crate) fn to_i64(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| StorageError::Invalid(format!("{v} does not fit in i64")))
}

pub(crate) fn to_u64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| StorageError::Corrupt(format!("negative value {v}")))
}
