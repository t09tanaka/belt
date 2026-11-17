//! Pure decisions about the identity of an ephemeral worker (spec chapter
//! 15). An ephemeral worker keeps its registered name and reports a new
//! identity with every start. The parent remembers the identity it saw last
//! and the identities it retired; nothing here touches the database or a
//! connection.
//!
//! * the first identity, and the same identity again, are let in;
//! * a new identity under the same name replaces the one before it: that
//!   one is retired, and what it held ends as `interrupted`;
//! * a retired identity is never let in again, whatever else is recorded;
//! * `workers forget` is for an ephemeral worker that is not connected.

use crate::state::Outcome;

/// What an identity presented at `hello` means to the parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Nothing was recorded for the worker: this identity is its first.
    First,
    /// The identity seen last: a reconnect.
    Same,
    /// Another identity than the one seen last: a new start. The previous
    /// identity is retired.
    Replaced,
    /// The identity was retired. It is refused.
    Retired,
}

/// Decide on `presented`. `recorded` is the identity seen last for the
/// worker; `retired` says whether `presented` has a tombstone.
pub fn admit(recorded: Option<&str>, presented: &str, retired: bool) -> Admission {
    if retired {
        return Admission::Retired;
    }
    match recorded {
        None => Admission::First,
        Some(seen) if seen == presented => Admission::Same,
        Some(_) => Admission::Replaced,
    }
}

/// Why `workers forget` is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgetRefusal {
    /// The name is not registered.
    NotRegistered,
    /// A persistent worker comes back with its own record of its attempts.
    NotEphemeral,
    /// A connected worker answers for its attempts itself.
    Connected,
}

impl ForgetRefusal {
    pub fn message(self) -> &'static str {
        match self {
            ForgetRefusal::NotRegistered => "no such worker is registered",
            ForgetRefusal::NotEphemeral => {
                "forget is only for an ephemeral worker (ephemeral = true); a persistent worker reports its own attempts when it returns"
            }
            ForgetRefusal::Connected => {
                "the worker is connected; forget declares a worker gone and is refused while it answers"
            }
        }
    }
}

/// Whether `workers forget` may go ahead. `ephemeral` is `None` for a name
/// that is not registered.
pub fn may_forget(ephemeral: Option<bool>, connected: bool) -> Result<(), ForgetRefusal> {
    match ephemeral {
        None => Err(ForgetRefusal::NotRegistered),
        Some(false) => Err(ForgetRefusal::NotEphemeral),
        Some(true) if connected => Err(ForgetRefusal::Connected),
        Some(true) => Ok(()),
    }
}

/// How an attempt of a retired identity ends: `cancelled` when its cancel
/// was already asked for, `interrupted` otherwise. Never `failed`: nothing
/// is known about how the command went.
pub fn outcome_of_lost(cancel_requested: bool) -> Outcome {
    if cancel_requested {
        Outcome::Cancelled
    } else {
        Outcome::Interrupted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retired_identity_is_refused_whatever_is_recorded() {
        for recorded in [None, Some("a"), Some("b")] {
            assert_eq!(admit(recorded, "a", true), Admission::Retired);
        }
    }

    #[test]
    fn the_first_the_same_and_a_new_identity_are_told_apart() {
        assert_eq!(admit(None, "a", false), Admission::First);
        assert_eq!(admit(Some("a"), "a", false), Admission::Same);
        assert_eq!(admit(Some("a"), "b", false), Admission::Replaced);
    }

    #[test]
    fn forget_is_for_a_registered_ephemeral_worker_that_is_not_connected() {
        assert_eq!(may_forget(Some(true), false), Ok(()));
        assert_eq!(may_forget(Some(true), true), Err(ForgetRefusal::Connected));
        for connected in [false, true] {
            assert_eq!(
                may_forget(Some(false), connected),
                Err(ForgetRefusal::NotEphemeral)
            );
            assert_eq!(
                may_forget(None, connected),
                Err(ForgetRefusal::NotRegistered)
            );
        }
    }

    #[test]
    fn every_refusal_says_what_stands_in_the_way() {
        assert!(ForgetRefusal::NotRegistered
            .message()
            .contains("no such worker"));
        assert!(ForgetRefusal::NotEphemeral
            .message()
            .contains("ephemeral = true"));
        assert!(ForgetRefusal::Connected.message().contains("is connected"));
    }

    #[test]
    fn a_lost_attempt_is_interrupted_unless_its_cancel_was_asked_for() {
        assert_eq!(outcome_of_lost(false), Outcome::Interrupted);
        assert_eq!(outcome_of_lost(true), Outcome::Cancelled);
    }
}
