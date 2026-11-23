//! When a released dedup record may go (spec 25.6 (c), "解放済みの記録を
//! 消してよい条件"). In memory only: after a restart there is no connection
//! and no request in flight, and the start deletes every released record.
//!
//! Each connection numbers the submits it takes from its queue, at the
//! moment it takes them (before a request waits for a permit), and keeps
//! the numbers of those still being handled. A connection is *settled*
//! when it takes nothing more (it ended) and every submit it took has been
//! handled. The record of a release received on connection C, when C had
//! taken `r` submits, goes once
//!
//! 1. every submit C took with a number up to `r` has been handled, and
//! 2. every other connection of the same member that was open at the
//!    release has settled.
//!
//! Both are checked when a submit is handled and when a connection
//! settles; what became true is returned for the caller to delete by key.

use crate::ids::{AttemptId, CoordinatorId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};

/// Where a request was taken: its connection and how many submits the
/// connection had taken by then (this one included, for a submit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Taken {
    pub conn: u64,
    pub submits: u64,
}

#[derive(Debug)]
struct Conn {
    member: CoordinatorId,
    taken: u64,
    in_flight: BTreeSet<u64>,
    open: bool,
}

#[derive(Debug)]
struct Pending {
    member: CoordinatorId,
    attempt: AttemptId,
    at: Option<Taken>,
    others: BTreeSet<u64>,
}

#[derive(Debug, Default)]
struct Inner {
    next: u64,
    conns: BTreeMap<u64, Conn>,
    pending: Vec<Pending>,
}

/// The released records waiting for the submits that may be behind them.
#[derive(Debug, Default)]
pub struct Releases(Mutex<Inner>);

/// The records whose release no submit can be behind any more.
pub type Ripe = Vec<(CoordinatorId, AttemptId)>;

impl Releases {
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A connection of `member` was proven; its number.
    pub fn open(&self, member: &CoordinatorId) -> u64 {
        let mut i = self.inner();
        i.next += 1;
        let n = i.next;
        i.conns.insert(
            n,
            Conn {
                member: member.clone(),
                taken: 0,
                in_flight: BTreeSet::new(),
                open: true,
            },
        );
        n
    }

    /// A request was taken from `conn`; a submit gets the next number and
    /// is in flight until [`Self::handled`].
    pub fn take(&self, conn: u64, submit: bool) -> Taken {
        let mut i = self.inner();
        let Some(c) = i.conns.get_mut(&conn) else {
            return Taken { conn, submits: 0 };
        };
        if submit {
            c.taken += 1;
            let n = c.taken;
            c.in_flight.insert(n);
        }
        Taken {
            conn,
            submits: c.taken,
        }
    }

    /// The submit `taken` was handled (answered, failed or panicked).
    pub fn handled(&self, taken: Taken) -> Ripe {
        let mut i = self.inner();
        if let Some(c) = i.conns.get_mut(&taken.conn) {
            c.in_flight.remove(&taken.submits);
        }
        ripe(&mut i)
    }

    /// `conn` takes nothing more.
    pub fn close(&self, conn: u64) -> Ripe {
        let mut i = self.inner();
        if let Some(c) = i.conns.get_mut(&conn) {
            c.open = false;
        }
        ripe(&mut i)
    }

    /// A release of `attempt` by `member` was committed, received at `at`
    /// (`None`: not over a connection). Its record may go when the result
    /// says so, now or at a later event.
    pub fn released(&self, member: &CoordinatorId, attempt: &AttemptId, at: Option<Taken>) -> Ripe {
        let mut i = self.inner();
        let own = at.map(|t| t.conn);
        let others = i
            .conns
            .iter()
            .filter(|(n, c)| Some(**n) != own && c.member == *member && !settled(c))
            .map(|(n, _)| *n)
            .collect();
        i.pending.push(Pending {
            member: member.clone(),
            attempt: attempt.clone(),
            at,
            others,
        });
        ripe(&mut i)
    }

    /// Records waiting, for the tests and diagnostics.
    pub fn waiting(&self) -> usize {
        self.inner().pending.len()
    }
}

fn settled(c: &Conn) -> bool {
    !c.open && c.in_flight.is_empty()
}

fn ripe(i: &mut Inner) -> Ripe {
    let conns = &i.conns;
    let gone = |n: &u64| conns.get(n).is_none_or(settled);
    let mut out = Vec::new();
    i.pending.retain(|p| {
        let before = p.at.is_none_or(|t| {
            conns
                .get(&t.conn)
                .is_none_or(|c| c.in_flight.range(..=t.submits).next().is_none())
        });
        if before && p.others.iter().all(gone) {
            out.push((p.member.clone(), p.attempt.clone()));
            false
        } else {
            true
        }
    });
    // A settled connection no pending release waits on is forgotten.
    let waited: BTreeSet<u64> = i
        .pending
        .iter()
        .flat_map(|p| p.others.iter().copied().chain(p.at.map(|t| t.conn)))
        .collect();
    i.conns.retain(|n, c| !settled(c) || waited.contains(n));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (CoordinatorId, AttemptId) {
        (CoordinatorId::generate(), AttemptId::generate())
    }

    #[test]
    fn a_release_waits_for_the_submits_taken_before_it_on_its_connection() {
        let r = Releases::default();
        let (m, a) = ids();
        let c = r.open(&m);
        let s1 = r.take(c, true);
        let at = r.take(c, false);
        assert!(r.released(&m, &a, Some(at)).is_empty());
        // A submit taken after the release does not hold it.
        let s2 = r.take(c, true);
        assert_eq!(r.handled(s1), vec![(m.clone(), a.clone())]);
        assert!(r.handled(s2).is_empty());
        assert_eq!(r.waiting(), 0);
    }

    #[test]
    fn a_release_waits_until_the_other_open_connections_of_the_member_settle() {
        let r = Releases::default();
        let (m, a) = ids();
        let other_member = CoordinatorId::generate();
        let old = r.open(&m);
        let foreign = r.open(&other_member);
        let left = r.take(old, true);
        let c = r.open(&m);
        let at = r.take(c, false);
        assert!(r.released(&m, &a, Some(at)).is_empty());
        // Closing is not enough while a submit it took is in flight.
        assert!(r.close(old).is_empty());
        assert_eq!(r.handled(left), vec![(m.clone(), a.clone())]);
        // Another member's connection never held it.
        let _ = foreign;
        // A connection opened after the release does not hold the next one.
        let (_, b) = ids();
        let late = r.open(&m);
        let at = r.take(c, false);
        let _ = late;
        let other = r.released(&m, &b, Some(at));
        assert!(other.is_empty(), "the late connection was open at this release");
        assert_eq!(r.close(late), vec![(m, b)]);
    }

    #[test]
    fn a_release_with_nothing_in_flight_is_ripe_at_once() {
        let r = Releases::default();
        let (m, a) = ids();
        let c = r.open(&m);
        let s = r.take(c, true);
        assert!(r.handled(s).is_empty());
        let at = r.take(c, false);
        assert_eq!(r.released(&m, &a, Some(at)), vec![(m.clone(), a.clone())]);
        let (_, b) = ids();
        assert_eq!(r.released(&m, &b, None), vec![(m, b)]);
    }

    #[test]
    fn ten_thousand_releases_on_one_connection_leave_nothing_waiting() {
        let r = Releases::default();
        let m = CoordinatorId::generate();
        let c = r.open(&m);
        let mut ripe = 0;
        for _ in 0..10_050 {
            let s = r.take(c, true);
            ripe += r.handled(s).len();
            let at = r.take(c, false);
            ripe += r.released(&m, &AttemptId::generate(), Some(at)).len();
        }
        assert_eq!(ripe, 10_050);
        assert_eq!(r.waiting(), 0);
        assert_eq!(r.inner().conns.len(), 1);
    }
}
