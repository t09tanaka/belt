//! The owner of something that cannot carry a marker file: the same three
//! facts a marker holds (the place, the PID, the start identity), kept by the
//! caller in a row. The proof that an owner is gone is the one of the markers
//! ([`super::proven_gone`]); nothing here signals a process.

use super::{one_line, process_start_identity, proven_gone, Owner, Place, StartIdentity};

/// What can be said, from this process, about a recorded owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerState {
    /// The same host and PID namespace, and not proven gone.
    Present,
    /// Proven gone: no such process, or the PID belongs to a later one.
    Gone,
    /// Another host or PID namespace, an unreadable place, or a record that
    /// does not parse: nothing can be proven either way.
    Elsewhere,
}

/// An owner as it is stored: `place` is three lines (host, PID namespace,
/// boot), `start` is the start identity of the process `pid`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnerRecord {
    pub(crate) place: String,
    pub(crate) pid: u32,
    pub(crate) start: String,
}

impl OwnerRecord {
    /// This process. `None` when its place or its start cannot be read: then
    /// nothing may be recorded as owned.
    pub(crate) fn this_process() -> Option<Self> {
        let Place { host, pid_ns, boot } = Place::here()?;
        let pid = std::process::id();
        let StartIdentity::Identity(start) = process_start_identity(pid) else {
            return None;
        };
        one_line(&start)?;
        Some(Self {
            place: format!("{host}\n{pid_ns}\n{boot}"),
            pid,
            start,
        })
    }

    fn owner(&self) -> Option<Owner> {
        let mut lines = self.place.split('\n');
        let host = one_line(lines.next()?)?;
        let pid_ns = one_line(lines.next()?)?;
        let boot = one_line(lines.next()?)?;
        if lines.next().is_some() || self.pid <= 1 || i32::try_from(self.pid).is_err() {
            return None;
        }
        Some(Owner {
            place: Place {
                host: host.to_string(),
                pid_ns: pid_ns.to_string(),
                boot: boot.to_string(),
            },
            pid: self.pid,
            start: one_line(&self.start)?.to_string(),
        })
    }

    pub(crate) fn state(&self) -> OwnerState {
        let (Some(owner), Some(here)) = (self.owner(), Place::here()) else {
            return OwnerState::Elsewhere;
        };
        if owner.place.host != here.host || owner.place.pid_ns != here.pid_ns {
            return OwnerState::Elsewhere;
        }
        // The PID is read only where it can be the owner's: here, in this boot.
        let now = if owner.place == here {
            process_start_identity(owner.pid)
        } else {
            StartIdentity::Unknown
        };
        if proven_gone(&owner, &here, &now) {
            OwnerState::Gone
        } else {
            OwnerState::Present
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ended_pid() -> u32 {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    #[test]
    fn this_process_is_present_and_an_ended_one_is_gone() {
        let me = OwnerRecord::this_process().unwrap();
        assert_eq!(me.state(), OwnerState::Present);
        let ended = OwnerRecord {
            pid: ended_pid(),
            ..me.clone()
        };
        assert_eq!(ended.state(), OwnerState::Gone);
    }

    #[test]
    fn another_host_or_namespace_or_a_broken_record_proves_nothing() {
        let me = OwnerRecord::this_process().unwrap();
        let lines: Vec<&str> = me.place.split('\n').collect();
        let (host, pid_ns, boot) = (lines[0], lines[1], lines[2]);
        let record = |place: String, pid: u32| OwnerRecord {
            place,
            pid,
            start: me.start.clone(),
        };
        // A PID that no process has here: only the place keeps the record.
        let free = ended_pid();
        assert_eq!(
            record(format!("{host}-other\n{pid_ns}\n{boot}"), free).state(),
            OwnerState::Elsewhere
        );
        assert_eq!(
            record(format!("{host}\n{pid_ns}-other\n{boot}"), free).state(),
            OwnerState::Elsewhere
        );
        assert_eq!(
            record("one line".into(), free).state(),
            OwnerState::Elsewhere
        );
        assert_eq!(record(me.place.clone(), 1).state(), OwnerState::Elsewhere);
        assert_eq!(record(me.place.clone(), 0).state(), OwnerState::Elsewhere);
    }

    #[test]
    fn the_same_host_in_another_boot_is_gone_with_that_boot() {
        let me = OwnerRecord::this_process().unwrap();
        let (rest, _boot) = me.place.rsplit_once('\n').unwrap();
        let earlier = OwnerRecord {
            place: format!("{rest}\nan-earlier-boot"),
            ..me
        };
        assert_eq!(earlier.state(), OwnerState::Gone);
    }
}
