//! Structured error codes and their single mapping to process exit codes.
//!
//! The JSON envelope tells apart: signal, bad input, unconfirmed communication
//! (`COMMUNICATION_UNKNOWN`), and artifact collection failure
//! (`COLLECTION_FAILED`). The CLI exit code alone is never the job's state.

use serde::{Deserialize, Serialize};

/// Process exit codes. The single definition site.
pub mod exit {
    /// Success.
    pub const SUCCESS: i32 = 0;
    /// A command ran and failed generically (the original code is passed through when known).
    pub const FAILURE: i32 = 1;
    /// Conventional cancellation/local interrupt result when no command exit exists.
    pub const CANCELLED: i32 = 130;
    /// A timed out command without a more specific command exit.
    pub const TIMED_OUT: i32 = 124;
    /// Invalid input (bad arguments); clap also exits with this code.
    pub const INPUT_ERROR: i32 = 2;
    /// The command is defined but not implemented yet.
    pub const NOT_IMPLEMENTED: i32 = 70;
    /// The source is being captured by another process.
    pub const SOURCE_BUSY: i32 = 71;
    /// Same attempt, different payload.
    pub const CONFLICT: i32 = 72;
    /// The addressed object does not exist.
    pub const NOT_FOUND: i32 = 73;
    /// Worker and coordinator protocols do not match.
    pub const PROTOCOL_INCOMPATIBLE: i32 = 74;
    /// A stale coordinator epoch or agent epoch was used.
    pub const STALE_EPOCH: i32 = 75;
    /// Dispatch is blocked (no worker, secret, or environment).
    pub const BLOCKED: i32 = 76;
    /// The execution state could not be confirmed.
    pub const COMMUNICATION_UNKNOWN: i32 = 77;
    /// The command finished but required outputs were not collected.
    pub const COLLECTION_FAILED: i32 = 78;
    /// Unexpected internal failure.
    pub const INTERNAL: i32 = 79;
    /// Refused for now, nothing was done: ask again later. Outside `70..=79`
    /// (all taken); `EX_UNAVAILABLE` of sysexits.
    pub const OVERLOADED: i32 = 69;
    /// Conventional base for "terminated by signal N": `128 + N`.
    pub const SIGNAL_BASE: i32 = 128;
}

/// Highest signal number accepted by [`ErrorCode::exit_code_with`].
pub const MAX_SIGNAL: i32 = 64;
/// Highest exit code a process can report.
pub const MAX_EXIT_CODE: i32 = 255;

/// Machine-readable error code, serialized as `SCREAMING_SNAKE_CASE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    NotImplemented,
    InvalidInput,
    SourceBusy,
    Conflict,
    NotFound,
    ProtocolIncompatible,
    StaleEpoch,
    Blocked,
    CommunicationUnknown,
    CollectionFailed,
    CommandFailed,
    Signal,
    Internal,
    /// The attempt was retired (a tombstone exists). A late `submit_attempt`
    /// and any other mutation are refused with this code. `get_attempt` on an
    /// attempt the worker knows answers a status with `retired: true`; on a
    /// tombstone that has no accepted attempt behind it (a cancel or retire
    /// that arrived first) there is no status to build, so the worker answers
    /// this error. Neither case is `NOT_FOUND`.
    Retired,
    /// `renew_lease` could not extend the lease: the attempt is expired,
    /// cancelling, finalizing, terminal or retired. A merely stale or replayed
    /// renew is not an error; it answers `extended: false`.
    LeaseNotRenewable,
    /// The peer refused the request for now and committed nothing of it: too
    /// many requests wait, or its database was busy. The same request may be
    /// sent again. Unlike `SOURCE_BUSY` this says nothing about the input.
    Overloaded,
    /// The input a request needs is not in the worker's source store (spec
    /// 25.8 5b (b)): a new `submit_attempt` whose source is not there, or a
    /// publication whose blobs or snapshot refs went away. Nothing of the
    /// request was stored; the parent delivers the source again and then
    /// sends the same request. Sent only to a peer that agreed
    /// `input_missing_v1` (an older peer cannot read the code).
    InputMissing,
}

/// Why a task is blocked (accompanies `ErrorCode::Blocked`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockedReason {
    NoCompatibleWorker,
    SecretUnavailable,
    EnvironmentMismatch,
}

/// Why a request was refused with `CONFLICT`, when the caller can act on
/// the reason (accompanies `ErrorCode::Conflict`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ConflictReason {
    /// The Git bundle of a source is built on commits the worker's clone of
    /// the repository does not hold. Nothing of the bundle was kept. `have`
    /// lists commits the clone does hold (see `blob_verify`): a bundle built
    /// on those is taken.
    BundleBaseMissing { have: Vec<String> },
    /// A `submit_attempt` names an attempt the worker already holds with
    /// another payload. Sending the same request again never succeeds.
    PayloadMismatch,
    /// A reason this version does not know (a newer peer sent it). What it
    /// carried is dropped: the caller treats the error as a plain
    /// `CONFLICT`.
    #[serde(other)]
    Unknown,
}

/// Why a request was refused with `OVERLOADED`, when the caller can act on
/// the reason (accompanies `ErrorCode::Overloaded`). With a reason or
/// without one, nothing of the request was stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum OverloadedReason {
    /// A `submit_attempt` names an attempt id that an attempt of another
    /// member holds on this worker (spec 24.4). Sending the same attempt
    /// again does not help while that one is kept: the task is placed again
    /// under a new attempt id.
    AttemptIdInUse,
    /// A `submit_attempt` found the admission window of this worker full,
    /// counted over the attempts of every member (spec 24.4). The worker
    /// stored nothing, no tombstone either: an attempt that was sent once
    /// and got this answer exists nowhere.
    Capacity,
    /// A `submit_attempt` of a member that holds 10,000 dedup records on
    /// this worker (spec 25.6 (c)): the member reconciles with
    /// `list_unreleased` and releases what it no longer reads.
    AttemptRecords,
    /// A new `submit_attempt` found the worker's source store busy: a
    /// deleting pass holds its mutation lock for one step (spec 25.8 5b
    /// (a)). The worker stored nothing and did not wait; the same attempt
    /// may be sent again.
    SourceStore,
    /// A reason this version does not know (a newer peer sent it): the
    /// caller treats the error as a plain `OVERLOADED`.
    #[serde(other)]
    Unknown,
}

impl ErrorCode {
    /// Every code.
    pub const ALL: [ErrorCode; 17] = [
        ErrorCode::NotImplemented,
        ErrorCode::InvalidInput,
        ErrorCode::SourceBusy,
        ErrorCode::Conflict,
        ErrorCode::NotFound,
        ErrorCode::ProtocolIncompatible,
        ErrorCode::StaleEpoch,
        ErrorCode::Blocked,
        ErrorCode::CommunicationUnknown,
        ErrorCode::CollectionFailed,
        ErrorCode::CommandFailed,
        ErrorCode::Signal,
        ErrorCode::Internal,
        ErrorCode::Retired,
        ErrorCode::LeaseNotRenewable,
        ErrorCode::Overloaded,
        ErrorCode::InputMissing,
    ];

    /// The wire string, identical to the serde form.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::NotImplemented => "NOT_IMPLEMENTED",
            ErrorCode::InvalidInput => "INVALID_INPUT",
            ErrorCode::SourceBusy => "SOURCE_BUSY",
            ErrorCode::Conflict => "CONFLICT",
            ErrorCode::NotFound => "NOT_FOUND",
            ErrorCode::ProtocolIncompatible => "PROTOCOL_INCOMPATIBLE",
            ErrorCode::StaleEpoch => "STALE_EPOCH",
            ErrorCode::Blocked => "BLOCKED",
            ErrorCode::CommunicationUnknown => "COMMUNICATION_UNKNOWN",
            ErrorCode::CollectionFailed => "COLLECTION_FAILED",
            ErrorCode::CommandFailed => "COMMAND_FAILED",
            ErrorCode::Signal => "SIGNAL",
            ErrorCode::Internal => "INTERNAL",
            ErrorCode::Retired => "RETIRED",
            ErrorCode::LeaseNotRenewable => "LEASE_NOT_RENEWABLE",
            ErrorCode::Overloaded => "OVERLOADED",
            ErrorCode::InputMissing => "INPUT_MISSING",
        }
    }

    /// The process exit code for this error. `Signal` and `CommandFailed`
    /// pass the command's own code through when known; use
    /// [`ErrorCode::exit_code_with`].
    pub fn exit_code(self) -> i32 {
        match self {
            ErrorCode::NotImplemented => exit::NOT_IMPLEMENTED,
            ErrorCode::InvalidInput => exit::INPUT_ERROR,
            ErrorCode::SourceBusy => exit::SOURCE_BUSY,
            ErrorCode::Conflict => exit::CONFLICT,
            ErrorCode::NotFound => exit::NOT_FOUND,
            ErrorCode::ProtocolIncompatible => exit::PROTOCOL_INCOMPATIBLE,
            ErrorCode::StaleEpoch => exit::STALE_EPOCH,
            ErrorCode::Blocked => exit::BLOCKED,
            ErrorCode::CommunicationUnknown => exit::COMMUNICATION_UNKNOWN,
            ErrorCode::CollectionFailed => exit::COLLECTION_FAILED,
            ErrorCode::CommandFailed => exit::FAILURE,
            ErrorCode::Signal => exit::SIGNAL_BASE,
            ErrorCode::Internal => exit::INTERNAL,
            // Both are "the addressed attempt no longer accepts this", the
            // same family as a payload conflict; the JSON code tells them apart.
            ErrorCode::Retired | ErrorCode::LeaseNotRenewable => exit::CONFLICT,
            ErrorCode::Overloaded | ErrorCode::InputMissing => exit::OVERLOADED,
        }
    }

    /// Like [`ErrorCode::exit_code`], but with the command's own detail:
    ///
    /// * `Signal` with `n` in `1..=64` gives `128 + n`;
    /// * `CommandFailed` with `n` in `1..=255` passes `n` through;
    /// * a detail outside those ranges (including `0`, which would read as
    ///   success) is a broken report and becomes `INTERNAL`.
    ///
    /// A command may exit with any code, including `70..=79`, which this
    /// crate reserves for its own errors, so the process exit code alone never
    /// tells a command failure from a runner error. Decide the settled state
    /// from the JSON `code` (and `--json` output), not from the exit code.
    pub fn exit_code_with(self, detail: Option<i32>) -> i32 {
        match (self, detail) {
            (ErrorCode::Signal, Some(n)) if (1..=MAX_SIGNAL).contains(&n) => exit::SIGNAL_BASE + n,
            (ErrorCode::CommandFailed, Some(n)) if (1..=MAX_EXIT_CODE).contains(&n) => n,
            (ErrorCode::Signal | ErrorCode::CommandFailed, Some(_)) => exit::INTERNAL,
            _ => self.exit_code(),
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error body carried in `--json` envelopes and protocol error frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    /// Set when `code` is `BLOCKED`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<BlockedReason>,
    /// May be set when `code` is `CONFLICT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_reason: Option<ConflictReason>,
    /// Set when the group proof of a connection was refused (spec 24.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_refusal: Option<GroupRefusal>,
    /// May be set when `code` is `OVERLOADED`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overloaded_reason: Option<OverloadedReason>,
    /// Set when `code` is `INPUT_MISSING`: which source is missing, and at
    /// a publication the blobs that are not in the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_missing: Option<InputMissing>,
}

/// What an `INPUT_MISSING` refusal names (spec 25.8 5b (b)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputMissing {
    /// The source that is not in the store (as the request named it).
    pub source_id: String,
    /// At a publication: the blobs (`sha256:` digests) that are not there.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<String>,
}

/// Why a member refused another member's group proof. The connection is
/// closed; nothing was accepted on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum GroupRefusal {
    /// The peer belongs to another group.
    GroupMismatch,
    /// The peer holds another group key. `fingerprint` is that of the key
    /// the refusing member signs with: a member that holds it as its next
    /// key has missed the switch and makes it now.
    GroupKeyMismatch { fingerprint: String },
    /// The name is recorded with another coordinator id, or the coordinator
    /// id under another name.
    MemberIdentityMismatch,
    /// The peer told this worker that it retired the identity it still has.
    IdentityRetired,
    /// `belt group forget` removed this member on the worker (spec 25.6
    /// (d)): its coordinator id is refused for good.
    MemberForgotten,
    /// A reason this version does not know (a newer peer sent it): the
    /// caller treats the refusal as a plain refused proof.
    #[serde(other)]
    Unknown,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        ApiError {
            code,
            message: message.into(),
            blocked_reason: None,
            conflict_reason: None,
            group_refusal: None,
            overloaded_reason: None,
            input_missing: None,
        }
    }

    /// `INPUT_MISSING` for `source_id`, with the blobs that are missing.
    pub fn input_missing(
        source_id: impl Into<String>,
        missing: Vec<String>,
        message: impl Into<String>,
    ) -> Self {
        ApiError {
            input_missing: Some(InputMissing {
                source_id: source_id.into(),
                missing,
            }),
            ..ApiError::new(ErrorCode::InputMissing, message)
        }
    }

    /// `OVERLOADED` with a reason the caller acts on.
    pub fn overloaded(reason: OverloadedReason, message: impl Into<String>) -> Self {
        ApiError {
            overloaded_reason: Some(reason),
            ..ApiError::new(ErrorCode::Overloaded, message)
        }
    }

    /// The refusal of a group proof.
    pub fn group_refused(reason: GroupRefusal, message: impl Into<String>) -> Self {
        ApiError {
            group_refusal: Some(reason),
            ..ApiError::new(ErrorCode::ProtocolIncompatible, message)
        }
    }

    pub fn conflict(reason: ConflictReason, message: impl Into<String>) -> Self {
        ApiError {
            code: ErrorCode::Conflict,
            message: message.into(),
            blocked_reason: None,
            group_refusal: None,
            overloaded_reason: None,
            input_missing: None,
            conflict_reason: Some(reason),
        }
    }

    pub fn blocked(reason: BlockedReason, message: impl Into<String>) -> Self {
        ApiError {
            code: ErrorCode::Blocked,
            message: message.into(),
            blocked_reason: Some(reason),
            group_refusal: None,
            overloaded_reason: None,
            input_missing: None,
            conflict_reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashSet;

    #[test]
    fn wire_string_matches_serde_for_every_code() {
        for c in ErrorCode::ALL {
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(json, format!("\"{}\"", c.as_str()));
            assert_eq!(serde_json::from_str::<ErrorCode>(&json).unwrap(), c);
            assert_eq!(c.to_string(), c.as_str());
        }
    }

    #[test]
    fn exit_codes_are_unique_except_documented_overlaps() {
        let mut seen = HashSet::new();
        for c in ErrorCode::ALL {
            let documented_overlap = matches!(c, ErrorCode::Retired | ErrorCode::LeaseNotRenewable);
            assert!(
                seen.insert(c.exit_code()) || documented_overlap,
                "duplicate exit code for {c}"
            );
            assert_ne!(c.exit_code(), exit::SUCCESS);
        }
    }

    #[test]
    fn exit_code_mapping_is_pinned() {
        assert_eq!(ErrorCode::NotImplemented.exit_code(), 70);
        assert_eq!(ErrorCode::InvalidInput.exit_code(), 2);
        assert_eq!(ErrorCode::SourceBusy.exit_code(), 71);
        assert_eq!(ErrorCode::Conflict.exit_code(), 72);
        assert_eq!(ErrorCode::NotFound.exit_code(), 73);
        assert_eq!(ErrorCode::ProtocolIncompatible.exit_code(), 74);
        assert_eq!(ErrorCode::StaleEpoch.exit_code(), 75);
        assert_eq!(ErrorCode::Blocked.exit_code(), 76);
        assert_eq!(ErrorCode::CommunicationUnknown.exit_code(), 77);
        assert_eq!(ErrorCode::CollectionFailed.exit_code(), 78);
        assert_eq!(ErrorCode::CommandFailed.exit_code(), 1);
        assert_eq!(ErrorCode::Signal.exit_code(), 128);
        assert_eq!(ErrorCode::Internal.exit_code(), 79);
        assert_eq!(ErrorCode::Retired.exit_code(), 72);
        assert_eq!(ErrorCode::LeaseNotRenewable.exit_code(), 72);
        assert_eq!(ErrorCode::Overloaded.exit_code(), 69);
        assert_eq!(exit::SUCCESS, 0);
    }

    #[test]
    fn signal_and_command_failure_pass_detail_through() {
        assert_eq!(ErrorCode::Signal.exit_code_with(Some(15)), 143);
        assert_eq!(ErrorCode::Signal.exit_code_with(None), 128);
        assert_eq!(ErrorCode::CommandFailed.exit_code_with(Some(42)), 42);
        assert_eq!(ErrorCode::CommandFailed.exit_code_with(None), 1);
        assert_eq!(ErrorCode::Conflict.exit_code_with(Some(9)), 72);
    }

    #[test]
    fn command_failure_detail_is_range_checked() {
        let f = |n| ErrorCode::CommandFailed.exit_code_with(Some(n));
        assert_eq!(f(1), 1);
        assert_eq!(f(255), 255);
        assert_eq!(f(0), exit::INTERNAL);
        assert_eq!(f(256), exit::INTERNAL);
        assert_eq!(f(-1), exit::INTERNAL);
        // a command exit code may collide with the reserved range; it passes through
        assert_eq!(f(exit::CONFLICT), exit::CONFLICT);
    }

    #[test]
    fn signal_detail_is_range_checked() {
        let f = |n| ErrorCode::Signal.exit_code_with(Some(n));
        assert_eq!(f(1), 129);
        assert_eq!(f(64), 192);
        assert_eq!(f(0), exit::INTERNAL);
        assert_eq!(f(65), exit::INTERNAL);
        assert_eq!(f(-9), exit::INTERNAL);
    }

    #[test]
    fn the_four_distinguished_outcomes_have_distinct_codes() {
        let four = [
            ErrorCode::Signal,
            ErrorCode::InvalidInput,
            ErrorCode::CommunicationUnknown,
            ErrorCode::CollectionFailed,
        ];
        let strings: HashSet<_> = four.iter().map(|c| c.as_str()).collect();
        let codes: HashSet<_> = four.iter().map(|c| c.exit_code()).collect();
        assert_eq!(strings.len(), 4);
        assert_eq!(codes.len(), 4);
    }

    #[test]
    fn an_overload_can_say_why_and_an_unknown_reason_is_a_plain_overload() {
        let e = ApiError::overloaded(OverloadedReason::AttemptIdInUse, "held");
        assert_eq!(e.code, ErrorCode::Overloaded);
        assert_eq!(e.message, "held");
        assert_eq!((e.blocked_reason, &e.conflict_reason), (None, &None));
        assert_eq!(e.group_refusal, None);
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], "OVERLOADED");
        assert_eq!(
            v["overloaded_reason"],
            json!({"reason": "attempt_id_in_use"})
        );
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
        let full = ApiError::overloaded(OverloadedReason::Capacity, "full");
        let v = serde_json::to_value(&full).unwrap();
        assert_eq!(v["overloaded_reason"], json!({"reason": "capacity"}));
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), full);
        // A reason of a newer peer, with whatever it carries.
        for reason in [
            json!({"reason": "quota"}),
            json!({"reason": "later", "n": 3}),
        ] {
            let v = json!({"code": "OVERLOADED", "message": "m", "overloaded_reason": reason});
            let e: ApiError = serde_json::from_value(v).unwrap();
            assert_eq!(e.code, ErrorCode::Overloaded);
            assert_eq!(e.overloaded_reason, Some(OverloadedReason::Unknown));
        }
        // No reason: the field is not there, in either direction.
        let plain = ApiError::new(ErrorCode::Overloaded, "busy");
        assert_eq!(plain.overloaded_reason, None);
        assert!(serde_json::to_value(&plain)
            .unwrap()
            .get("overloaded_reason")
            .is_none());
        for other in [
            ApiError::conflict(ConflictReason::PayloadMismatch, "c"),
            ApiError::blocked(BlockedReason::SecretUnavailable, "b"),
            ApiError::group_refused(GroupRefusal::GroupMismatch, "g"),
        ] {
            assert_eq!(other.overloaded_reason, None);
        }
    }

    #[test]
    fn blocked_reasons_serialize_snake_case() {
        let e = ApiError::blocked(BlockedReason::SecretUnavailable, "bundle lost");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], "BLOCKED");
        assert_eq!(v["blocked_reason"], "secret_unavailable");
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
        for (r, s) in [
            (BlockedReason::NoCompatibleWorker, "no_compatible_worker"),
            (BlockedReason::EnvironmentMismatch, "environment_mismatch"),
        ] {
            assert_eq!(serde_json::to_string(&r).unwrap(), format!("\"{s}\""));
        }
    }

    #[test]
    fn a_conflict_can_say_why_in_a_form_the_caller_reads() {
        let have = vec!["0123456789abcdef0123456789abcdef01234567".to_string()];
        let e = ApiError::conflict(
            ConflictReason::BundleBaseMissing { have: have.clone() },
            "send it again",
        );
        assert_eq!(e.code, ErrorCode::Conflict);
        assert_eq!(e.message, "send it again");
        assert_eq!(e.blocked_reason, None);
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], "CONFLICT");
        assert_eq!(v["conflict_reason"]["reason"], "bundle_base_missing");
        assert_eq!(v["conflict_reason"]["have"][0], have[0]);
        assert!(v.get("blocked_reason").is_none());
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
        let blocked = ApiError::blocked(BlockedReason::SecretUnavailable, "x");
        assert_eq!(blocked.conflict_reason, None);
    }

    #[test]
    fn a_group_refusal_of_a_newer_peer_is_read_as_unknown() {
        let unknown: GroupRefusal =
            serde_json::from_value(json!({"reason": "some_later_reason", "x": 1})).unwrap();
        assert_eq!(unknown, GroupRefusal::Unknown);
        let known: GroupRefusal =
            serde_json::from_value(json!({"reason": "member_forgotten"})).unwrap();
        assert_eq!(known, GroupRefusal::MemberForgotten);
    }

    #[test]
    fn a_conflict_reason_of_a_newer_peer_is_read_as_unknown() {
        for reason in [
            json!({"reason": "clone_is_being_moved"}),
            json!({"reason": "clone_is_being_moved", "have": ["x"], "retry_after_ms": 5}),
        ] {
            let v = json!({"code": "CONFLICT", "message": "later", "conflict_reason": reason});
            let e: ApiError = serde_json::from_value(v).unwrap();
            assert_eq!(e.code, ErrorCode::Conflict);
            assert_eq!(e.message, "later");
            assert_eq!(e.conflict_reason, Some(ConflictReason::Unknown));
        }
        // a reason that is known keeps what it carries
        let known: ConflictReason =
            serde_json::from_value(json!({"reason": "bundle_base_missing", "have": ["a"]}))
                .unwrap();
        assert_eq!(
            known,
            ConflictReason::BundleBaseMissing {
                have: vec!["a".into()]
            }
        );
        assert_eq!(
            serde_json::to_value(ConflictReason::Unknown).unwrap(),
            json!({"reason": "unknown"})
        );
        let e = ApiError::conflict(ConflictReason::PayloadMismatch, "other payload");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["conflict_reason"], json!({"reason": "payload_mismatch"}));
        let back: ApiError = serde_json::from_value(v).unwrap();
        assert_eq!(back.conflict_reason, Some(ConflictReason::PayloadMismatch));
    }

    #[test]
    fn plain_errors_omit_blocked_reason() {
        let e = ApiError::new(ErrorCode::Conflict, "payload differs");
        let v = serde_json::to_value(&e).unwrap();
        assert!(v.get("blocked_reason").is_none());
        assert!(v.get("conflict_reason").is_none());
        assert_eq!(v["message"], "payload differs");
        assert_eq!(serde_json::from_value::<ApiError>(v).unwrap(), e);
    }
}

/// Deterministic aggregate failure precedence for partial read/export results.
pub fn result_read_precedence(old: Option<ErrorCode>, new: ErrorCode) -> ErrorCode {
    fn rank(code: ErrorCode) -> u8 {
        match code {
            ErrorCode::Internal => 6,
            ErrorCode::InvalidInput | ErrorCode::Conflict => 5,
            ErrorCode::CommunicationUnknown => 4,
            ErrorCode::CollectionFailed => 3,
            ErrorCode::NotFound => 2,
            _ => 1,
        }
    }
    old.filter(|code| rank(*code) >= rank(new)).unwrap_or(new)
}
#[cfg(test)]
mod result_read_tests {
    use super::*;
    #[test]
    fn result_read_precedence_orders_partial_outcomes_and_preserves_ties() {
        let codes = [
            ErrorCode::Overloaded,
            ErrorCode::NotFound,
            ErrorCode::CollectionFailed,
            ErrorCode::CommunicationUnknown,
            ErrorCode::Conflict,
            ErrorCode::InvalidInput,
            ErrorCode::Internal,
        ];
        for (i, a) in codes.iter().enumerate() {
            assert_eq!(result_read_precedence(None, *a), *a);
            assert_eq!(result_read_precedence(Some(*a), *a), *a);
            for (j, b) in codes.iter().enumerate() {
                let expected = if i >= j || (i == 4 && j == 5) { *a } else { *b };
                assert_eq!(result_read_precedence(Some(*a), *b), expected);
            }
        }
    }
}
