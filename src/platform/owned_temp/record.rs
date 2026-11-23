//! The owner of something that cannot carry a marker file: the same three
//! facts a marker holds (the place, the PID, the start identity), kept by the
//! caller in a row. The proof that an owner is gone is the one of the markers
//! ([`super::proven_gone`]); nothing here signals a process.

use super::{
    machine_of, one_line, process_start_identity, proven_gone, Owner, Place, StartIdentity,
    MACHINE_LINE,
};

/// What can be said, from this process, about a recorded owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerState {
    /// The same machine and PID namespace, and not proven gone.
    Present,
    /// Proven gone: no such process, or the PID belongs to a later one.
    Gone,
    /// Another machine or PID namespace, a place that cannot be read here,
    /// or a record that does not parse (one that names a host instead of a
    /// machine is such a record): nothing can be proven either way.
    Elsewhere,
}

/// An owner as it is stored: `place` is three lines (`machine:` and the
/// machine identity, the PID namespace, the boot), `start` is the start
/// identity of the process `pid`. A host name is not part of the place: two
/// machines may share one (spec 25.4 (a)).
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
        let Place {
            machine,
            pid_ns,
            boot,
        } = Place::here()?;
        let pid = std::process::id();
        let StartIdentity::Identity(start) = process_start_identity(pid) else {
            return None;
        };
        one_line(&start)?;
        Some(Self {
            place: format!("{MACHINE_LINE}{machine}\n{pid_ns}\n{boot}"),
            pid,
            start,
        })
    }

    /// This process as it would be recorded on another machine that has the
    /// same host name: only the machine identity differs.
    #[cfg(test)]
    pub(crate) fn on_another_machine() -> Self {
        let me = Self::this_process().unwrap();
        let (machine, rest) = me.place.split_once('\n').unwrap();
        let other = super::testing::other_hex(machine.strip_prefix(MACHINE_LINE).unwrap());
        Self {
            place: format!("{MACHINE_LINE}{other}\n{rest}"),
            ..me
        }
    }

    fn owner(&self) -> Option<Owner> {
        let mut lines = self.place.split('\n');
        // Only a machine identity as this version writes it is one: a host
        // name, or an identity in another spelling, names no machine.
        let machine = lines.next()?.strip_prefix(MACHINE_LINE)?;
        if machine_of(machine).as_deref() != Some(machine) {
            return None;
        }
        let pid_ns = one_line(lines.next()?)?;
        let boot = one_line(lines.next()?)?;
        if lines.next().is_some() || self.pid <= 1 || i32::try_from(self.pid).is_err() {
            return None;
        }
        Some(Owner {
            place: Place {
                machine: machine.to_string(),
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
        // The one rule of the markers says where nothing can be proven.
        if owner.place.machine != here.machine || owner.place.pid_ns != here.pid_ns {
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
    fn another_machine_or_namespace_or_a_broken_record_proves_nothing() {
        let me = OwnerRecord::this_process().unwrap();
        let lines: Vec<&str> = me.place.split('\n').collect();
        let (machine, pid_ns, boot) = (lines[0], lines[1], lines[2]);
        let record = |place: String, pid: u32| OwnerRecord {
            place,
            pid,
            start: me.start.clone(),
        };
        // A PID that no process has here: only the place keeps the record.
        let free = ended_pid();
        assert_eq!(
            record(format!("{machine}\n{pid_ns}-other\n{boot}"), free).state(),
            OwnerState::Elsewhere
        );
        // A first line that is not a machine identity as it is written: a
        // host name (a row of an earlier build), no prefix, another spelling.
        let hex = machine.strip_prefix(MACHINE_LINE).unwrap();
        for first in [
            "a-host-name".to_string(),
            hex.to_string(),
            format!("{MACHINE_LINE}{}", hex.to_uppercase()),
            format!("{MACHINE_LINE}{}", "0".repeat(32)),
            format!("{MACHINE_LINE}{hex}0"),
        ] {
            assert_eq!(
                record(format!("{first}\n{pid_ns}\n{boot}"), free).state(),
                OwnerState::Elsewhere,
                "{first}"
            );
        }
        assert_eq!(
            record("one line".into(), free).state(),
            OwnerState::Elsewhere
        );
        assert_eq!(record(me.place.clone(), 1).state(), OwnerState::Elsewhere);
        assert_eq!(record(me.place.clone(), 0).state(), OwnerState::Elsewhere);
    }

    /// Two machines may have the same host name and share the store (a home
    /// on NFS, a cloned VM). The owner on the other one is alive there for
    /// all this process can tell: another boot ID and a PID that is free
    /// here, which would prove an owner of this machine gone, prove nothing.
    #[test]
    fn another_machine_with_the_same_host_name_is_never_gone() {
        let me = OwnerRecord::this_process().unwrap();
        let lines: Vec<&str> = me.place.split('\n').collect();
        let hex = lines[0].strip_prefix(MACHINE_LINE).unwrap();
        let other = super::super::testing::other_hex(hex);
        assert!(machine_of(&other).is_some());
        for boot in [lines[2], "another-boot"] {
            for pid in [ended_pid(), std::process::id()] {
                let there = OwnerRecord {
                    place: format!("{MACHINE_LINE}{other}\n{}\n{boot}", lines[1]),
                    pid,
                    start: me.start.clone(),
                };
                assert_eq!(there.state(), OwnerState::Elsewhere, "{boot} {pid}");
            }
        }
    }

    #[test]
    fn the_same_machine_in_another_boot_is_gone_with_that_boot() {
        let me = OwnerRecord::this_process().unwrap();
        let (rest, _boot) = me.place.rsplit_once('\n').unwrap();
        let earlier = OwnerRecord {
            place: format!("{rest}\nan-earlier-boot"),
            ..me
        };
        assert_eq!(earlier.state(), OwnerState::Gone);
    }
}
