//! Pure admission decision (spec "Admission" and "Live guards").
//!
//! Nothing in this module touches the file system, the clock, or the OS.
//! The caller loads the ledger, reaps dead entries, takes a live snapshot
//! and hands everything in; this module only decides.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Waiting,
    Running,
}

/// The slice of a ledger entry that admission needs.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueEntry {
    pub id: String,
    pub state: State,
    pub front: bool,
    pub weight: u32,
    /// Enqueue order; lower is earlier.
    pub seq: u64,
    /// Cores a running entry's process tree was last measured using, or
    /// None when not measured (yet).
    pub using: Option<f64>,
    /// Seconds a waiting entry has been queued (0 for a running one).
    pub waited_secs: f64,
    /// Learned wall time, or None when unknown.
    pub expected_secs: Option<f64>,
    /// A running entry that started ahead of a waiting one that did not
    /// fit (see [`admit`]).
    pub backfilled: bool,
    /// The entry whose command started this one (a nested `exec`). It
    /// counts only while that entry is running (see [`is_nested`]).
    pub parent: Option<String>,
    /// Seconds a running entry has been running (0 for a waiting one).
    pub ran_secs: f64,
}

/// What the machine looks like right now.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LiveSnapshot {
    pub available_memory: u64,
    /// Global CPU busy fraction, 0.0..=1.0, sampled just now.
    pub cpu_busy: f64,
    /// One-minute load average (runnable processes), 0.0 when unknown.
    pub load_avg: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub budget: u32,
    pub memory_reserve: u64,
    /// Hold the next start while `cpu_busy` is above this (0.0..=1.0).
    pub cpu_threshold: f64,
    /// After waiting this long, the CPU guard no longer holds the head.
    pub cpu_guard_max_wait_secs: f64,
    /// A `--front` head is held while the load average is at or above this
    /// (0 disables the overload guard)...
    pub front_overload_load: f64,
    /// ...for at most this long.
    pub front_overload_max_wait_secs: f64,
    /// A waiting entry whose learned wall time is at most this may start
    /// ahead of entries that do not fit the budget (0 disables).
    pub backfill_max_job_secs: f64,
    /// No new entry passes a normal entry that backfilled ones are holding
    /// up once it has waited this long.
    pub backfill_max_wait_secs: f64,
    /// The first nested entry of a parent that has none running is not
    /// held by the budget; the live guards hold it for at most this long.
    pub nested_guard_max_wait_secs: f64,
    /// A nested entry that has waited this long starts whatever the
    /// budget says, once every nested entry of the same parent that is
    /// running has also run this long (0 disables).
    pub nested_sibling_max_wait_secs: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Reason {
    /// Someone is ahead in the queue; `ahead` is how many.
    NotHead {
        ahead: usize,
    },
    /// Could start ahead of an entry that does not fit, but that entry is
    /// already being held up by others that did, and has waited long
    /// enough (or is `--front`).
    Reserved {
        ahead: usize,
    },
    Budget {
        used: u32,
        budget: u32,
    },
    Memory {
        available: u64,
        need: u64,
    },
    Cpu {
        busy: f64,
        threshold: f64,
    },
    /// A `--front` head held briefly because the machine is overloaded.
    Overload {
        load: f64,
        limit: f64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Admitted,
    Wait(Reason),
}

/// The running entry `e` was started from, if it still runs. A parent that
/// is gone (or was never in the ledger) leaves `e` a top-level entry.
fn running_parent<'a>(entries: &'a [QueueEntry], e: &QueueEntry) -> Option<&'a QueueEntry> {
    let parent = e.parent.as_deref()?;
    entries
        .iter()
        .find(|p| p.id == parent && p.state == State::Running)
}

/// Whether `e` is nested: started by the command of an entry that is
/// still running.
pub fn is_nested(entries: &[QueueEntry], e: &QueueEntry) -> bool {
    running_parent(entries, e).is_some()
}

/// Waiting entries in admission order: nested entries first (they belong
/// to work that already holds budget, and that work cannot finish, and so
/// cannot free its budget, before they have run), then `--front` entries,
/// then the rest; by seq within each.
pub fn queue_order(entries: &[QueueEntry]) -> Vec<&QueueEntry> {
    let mut waiting: Vec<&QueueEntry> = entries
        .iter()
        .filter(|e| e.state == State::Waiting)
        .collect();
    waiting.sort_by_key(|e| (!is_nested(entries, e), !e.front, e.seq));
    waiting
}

/// The cores a running entry was last measured using, rounded, so a
/// command hovering just above its weight is not charged a whole extra
/// slot. A parent's measurement leaves out what its nested entries run.
fn measured(e: &QueueEntry) -> u32 {
    e.using.map(|u| u.round() as u32).unwrap_or(0)
}

/// What a running entry takes from the budget on its own account: its
/// declared weight, or the cores it was measured using when that is more.
/// What its nested entries take is not in here (see [`used_budget`]).
pub fn charged(e: &QueueEntry) -> u32 {
    e.weight.max(measured(e))
}

/// How running entries are counted (see `count_for`).
#[derive(Clone, Copy)]
struct Count {
    /// Declared weights only, not what was measured.
    declared: bool,
    /// Entries that started by backfill are left out.
    skip_backfilled: bool,
}

/// What the running entry `e` and everything nested in it take from the
/// budget: its weight, or what it uses itself plus what its running
/// nested entries take, when that is more. So nested entries use up their
/// parent's weight first, and only what goes beyond it is charged on top:
/// nothing is charged twice.
fn tree_charge(entries: &[QueueEntry], e: &QueueEntry, count: Count) -> u32 {
    if count.skip_backfilled && e.backfilled {
        return 0;
    }
    let own = if count.declared { 0 } else { measured(e) };
    let nested: u32 = entries
        .iter()
        .filter(|c| c.state == State::Running && c.parent.as_deref() == Some(e.id.as_str()))
        .map(|c| tree_charge(entries, c, count))
        .sum();
    e.weight.max(own + nested)
}

fn used(entries: &[QueueEntry], count: Count) -> u32 {
    entries
        .iter()
        .filter(|e| e.state == State::Running && !is_nested(entries, e))
        .map(|e| tree_charge(entries, e, count))
        .sum()
}

/// What running entries take from the budget. A top-level entry takes
/// its weight, or what it uses itself plus what its running nested entries
/// take when that is more; a nested entry is counted inside its parent.
pub fn used_budget(entries: &[QueueEntry]) -> u32 {
    used(
        entries,
        Count {
            declared: false,
            skip_backfilled: false,
        },
    )
}

/// How the budget is counted for the waiting entry `e` that has waited
/// `waited` seconds. A `--front` entry is held by what running entries
/// measure beyond their weight only for `front_overload_max_wait_secs`;
/// after that the declared weights decide, as before measurement existed.
/// With `skip_backfilled`, entries that started by backfill are left out.
fn count_for(e: &QueueEntry, waited: f64, limits: Limits, skip_backfilled: bool) -> Count {
    Count {
        declared: e.front && waited >= limits.front_overload_max_wait_secs,
        skip_backfilled,
    }
}

/// The budget in use once the waiting entry `e` has started. For a
/// top-level entry that is its weight more than now; for a nested one
/// only what does not fit in its parent's share.
fn used_once_started(entries: &[QueueEntry], e: &QueueEntry, count: Count, budget: u32) -> u32 {
    let started: Vec<QueueEntry> = entries
        .iter()
        .map(|q| QueueEntry {
            state: if q.id == e.id {
                State::Running
            } else {
                q.state
            },
            weight: if q.id == e.id {
                clamped(e, budget)
            } else {
                q.weight
            },
            ..q.clone()
        })
        .collect();
    used(&started, count)
}

/// The running entries nested in the same parent as `e`, when `e` is
/// nested and is the first waiting entry of that parent in the queue;
/// `None` otherwise.
fn running_siblings<'a>(
    entries: &'a [QueueEntry],
    order: &[&QueueEntry],
    e: &QueueEntry,
) -> Option<Vec<&'a QueueEntry>> {
    let parent = running_parent(entries, e)?;
    let sibling = |q: &QueueEntry| q.parent.as_deref() == Some(parent.id.as_str());
    let first = order.iter().find(|q| sibling(q))?;
    (first.id == e.id).then(|| {
        entries
            .iter()
            .filter(|q| q.state == State::Running && sibling(q))
            .collect()
    })
}

/// Whether a nested entry that has waited `waited` seconds behind the
/// `running` entries of its parent has waited long enough to start
/// whatever the budget says: a running nested command may itself be
/// waiting for this one. One entry is let through per
/// `nested_sibling_max_wait_secs`, since each that starts must have run
/// that long before the next may.
fn overdue(running: &[&QueueEntry], waited: f64, limits: Limits) -> bool {
    let limit = limits.nested_sibling_max_wait_secs;
    !running.is_empty()
        && limit > 0.0
        && waited >= limit
        && running.iter().all(|r| r.ran_secs >= limit)
}

/// The weight admission counts for `e`: floor 1, ceiling the budget.
fn clamped(e: &QueueEntry, budget: u32) -> u32 {
    e.weight.min(budget).max(1)
}

/// A command expected to finish soon: its learned wall time is known and
/// at most `backfill_max_job_secs`.
pub fn is_short(e: &QueueEntry, limits: Limits) -> bool {
    limits.backfill_max_job_secs > 0.0
        && e.expected_secs
            .is_some_and(|s| s <= limits.backfill_max_job_secs)
}

enum Pass {
    Yes,
    No,
    Reserved,
}

/// Whether the entry at `pos` in `order` may start ahead of everyone
/// before it (backfill). It must be short, and each entry ahead must
/// either not fit the budget or be unable to start before it anyway (a
/// non-head that is not short). An entry ahead that is held up only by
/// backfilled entries is not passed again once it is `--front` or has
/// waited `backfill_max_wait_secs`, so backfill delays it by at most the
/// short commands already running.
fn may_pass(entries: &[QueueEntry], order: &[&QueueEntry], pos: usize, limits: Limits) -> Pass {
    if !is_short(order[pos], limits) {
        return Pass::No;
    }
    for (i, a) in order[..pos].iter().enumerate() {
        let fits = |skip_backfilled| {
            let count = count_for(a, a.waited_secs, limits, skip_backfilled);
            used_once_started(entries, a, count, limits.budget) <= limits.budget
        };
        if fits(false) {
            // `a` fits: it starts first, as the head or as a backfill.
            if i == 0 || is_short(a, limits) {
                return Pass::No;
            }
            continue;
        }
        let held_by_backfill = fits(true);
        if held_by_backfill && (a.front || a.waited_secs >= limits.backfill_max_wait_secs) {
            return Pass::Reserved;
        }
    }
    Pass::Yes
}

/// Decide whether the waiting entry `me` may start now.
///
/// `predicted_rss` is the memory the candidate is expected to need (0 when
/// unknown). `waited_secs` is how long the candidate has been queued. `live`
/// may be `None` when the caller could not read the OS; the guards are then
/// skipped. The starvation rule (skip guards when nothing managed is
/// running) is applied here so it is covered by unit tests. The CPU guard
/// is skipped for `--front` entries (a human is waiting, and load task-traffic-controller
/// does not manage must not hold them) and once `cpu_guard_max_wait_secs`
/// has elapsed. A `--front` entry is instead held by the overload guard,
/// while the load average is at or above `front_overload_load`, for at
/// most `front_overload_max_wait_secs`. The memory guard always applies.
///
/// Only the head of the queue starts, except that a short command behind
/// heads that do not fit may start first when it fits (see `may_pass`);
/// the budget and the guards then apply to it as to a head.
///
/// A nested entry (one whose `parent` is running) is ahead of every
/// top-level one, and the budget counts it inside its parent's share
/// first. The first nested entry of a parent that has none running is
/// not held by the queue or the budget (its parent's command waits for
/// it, so that budget would never come back), only by the live guards
/// and by those for at most `nested_guard_max_wait_secs`: a command that
/// runs its nested commands one at a time never waits for the budget, and
/// each running parent goes at most one nested entry over it. Any other
/// nested entry waits until it fits, but for at most
/// `nested_sibling_max_wait_secs` (see `overdue`).
pub fn admit(
    entries: &[QueueEntry],
    me: &str,
    predicted_rss: u64,
    waited_secs: f64,
    limits: Limits,
    live: Option<LiveSnapshot>,
) -> Decision {
    let order = queue_order(entries);
    let Some(pos) = order.iter().position(|e| e.id == me) else {
        // Not in the queue at all: treat as "someone is ahead" so the
        // caller re-registers rather than starting un-gated.
        return Decision::Wait(Reason::NotHead { ahead: usize::MAX });
    };
    let candidate = order[pos];
    let siblings = running_siblings(entries, &order, candidate);
    if siblings
        .as_ref()
        .is_some_and(|running| overdue(running, waited_secs, limits))
    {
        return Decision::Admitted;
    }
    // The first nested entry of a parent that has none running: the
    // parent's command is certainly waiting for it, so the budget that
    // command holds never comes back by waiting. Neither the queue nor
    // the budget holds it; the live guards do, for a bounded time.
    let only_child = siblings.is_some_and(|running| running.is_empty());
    if !only_child {
        if pos != 0 {
            match may_pass(entries, &order, pos, limits) {
                Pass::Yes => {}
                Pass::No => return Decision::Wait(Reason::NotHead { ahead: pos }),
                Pass::Reserved => return Decision::Wait(Reason::Reserved { ahead: pos }),
            }
        }
        let count = count_for(candidate, waited_secs, limits, false);
        if used_once_started(entries, candidate, count, limits.budget) > limits.budget {
            return Decision::Wait(if pos == 0 {
                Reason::Budget {
                    used: used(entries, count),
                    budget: limits.budget,
                }
            } else {
                Reason::NotHead { ahead: pos }
            });
        }
    }
    let anything_running = entries.iter().any(|e| e.state == State::Running);
    if !anything_running {
        return Decision::Admitted;
    }
    let Some(live) = live else {
        return Decision::Admitted;
    };
    if only_child && waited_secs >= limits.nested_guard_max_wait_secs {
        return Decision::Admitted;
    }
    let need = predicted_rss.saturating_add(limits.memory_reserve);
    if live.available_memory < need {
        return Decision::Wait(Reason::Memory {
            available: live.available_memory,
            need,
        });
    }
    if candidate.front {
        let overloaded =
            limits.front_overload_load > 0.0 && live.load_avg >= limits.front_overload_load;
        if overloaded && waited_secs < limits.front_overload_max_wait_secs {
            return Decision::Wait(Reason::Overload {
                load: live.load_avg,
                limit: limits.front_overload_load,
            });
        }
        return Decision::Admitted;
    }
    if waited_secs < limits.cpu_guard_max_wait_secs && live.cpu_busy > limits.cpu_threshold {
        return Decision::Wait(Reason::Cpu {
            busy: live.cpu_busy,
            threshold: limits.cpu_threshold,
        });
    }
    Decision::Admitted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(id: &str, state: State, front: bool, weight: u32, seq: u64) -> QueueEntry {
        QueueEntry {
            id: id.into(),
            state,
            front,
            weight,
            seq,
            using: None,
            waited_secs: 0.0,
            expected_secs: None,
            backfilled: false,
            parent: None,
            ran_secs: 0.0,
        }
    }
    fn ran(mut q: QueueEntry, secs: f64) -> QueueEntry {
        q.ran_secs = secs;
        q
    }
    fn using(mut q: QueueEntry, cores: f64) -> QueueEntry {
        q.using = Some(cores);
        q
    }
    const LIMITS: Limits = Limits {
        budget: 12,
        memory_reserve: 2 << 30,
        cpu_threshold: 0.85,
        cpu_guard_max_wait_secs: 120.0,
        front_overload_load: 24.0,
        front_overload_max_wait_secs: 30.0,
        backfill_max_job_secs: 300.0,
        backfill_max_wait_secs: 300.0,
        nested_guard_max_wait_secs: 60.0,
        nested_sibling_max_wait_secs: 600.0,
    };
    /// A waiting entry expected to take `secs`, queued for `waited`.
    fn short(mut q: QueueEntry, secs: f64) -> QueueEntry {
        q.expected_secs = Some(secs);
        q
    }
    fn waited(mut q: QueueEntry, secs: f64) -> QueueEntry {
        q.waited_secs = secs;
        q
    }
    fn backfilled(mut q: QueueEntry) -> QueueEntry {
        q.backfilled = true;
        q
    }
    fn calm() -> LiveSnapshot {
        LiveSnapshot {
            available_memory: 64 << 30,
            cpu_busy: 0.1,
            load_avg: 2.0,
        }
    }

    #[test]
    fn empty_queue_admits_head() {
        let q = [e("a", State::Waiting, false, 4, 1)];
        assert_eq!(
            admit(&q, "a", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
    }

    #[test]
    fn unknown_id_waits() {
        let q = [e("a", State::Waiting, false, 4, 1)];
        assert_eq!(
            admit(&q, "zz", 0, 0.0, LIMITS, None),
            Decision::Wait(Reason::NotHead { ahead: usize::MAX })
        );
    }

    #[test]
    fn fifo_by_seq() {
        let q = [
            e("b", State::Waiting, false, 1, 2),
            e("a", State::Waiting, false, 1, 1),
        ];
        assert_eq!(admit(&q, "a", 0, 0.0, LIMITS, None), Decision::Admitted);
        assert_eq!(
            admit(&q, "b", 0, 0.0, LIMITS, None),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
    }

    #[test]
    fn front_jumps_but_is_fifo_among_fronts() {
        let q = [
            e("a", State::Waiting, false, 1, 1),
            e("f2", State::Waiting, true, 1, 3),
            e("f1", State::Waiting, true, 1, 2),
        ];
        let ids: Vec<&str> = queue_order(&q).iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["f1", "f2", "a"]);
        assert_eq!(admit(&q, "f1", 0, 0.0, LIMITS, None), Decision::Admitted);
        assert_eq!(
            admit(&q, "a", 0, 0.0, LIMITS, None),
            Decision::Wait(Reason::NotHead { ahead: 2 })
        );
    }

    #[test]
    fn head_reserves_until_budget_fits() {
        let q = [
            e("r", State::Running, false, 10, 1),
            e("heavy", State::Waiting, false, 4, 2),
            e("light", State::Waiting, false, 1, 3),
        ];
        assert_eq!(
            admit(&q, "heavy", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 10,
                budget: 12
            })
        );
        // light would fit, but it is not the head.
        assert_eq!(
            admit(&q, "light", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
    }

    #[test]
    fn exact_fit_is_admitted() {
        let q = [
            e("r", State::Running, false, 8, 1),
            e("w", State::Waiting, false, 4, 2),
        ];
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
    }

    #[test]
    fn oversized_weight_is_clamped_to_budget() {
        let q = [e("w", State::Waiting, false, 99, 1)];
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        let q = [
            e("r", State::Running, false, 1, 1),
            e("w", State::Waiting, false, 99, 2),
        ];
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 1,
                budget: 12
            })
        );
    }

    #[test]
    fn zero_weight_counts_as_one() {
        let q = [
            e("r", State::Running, false, 12, 1),
            e("w", State::Waiting, false, 0, 2),
        ];
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, None),
            Decision::Wait(Reason::Budget {
                used: 12,
                budget: 12
            })
        );
    }

    #[test]
    fn memory_guard_holds_head() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        let live = LiveSnapshot {
            available_memory: 1 << 30,
            ..calm()
        };
        assert_eq!(
            admit(&q, "w", 3 << 30, 0.0, LIMITS, Some(live)),
            Decision::Wait(Reason::Memory {
                available: 1 << 30,
                need: 5 << 30
            })
        );
    }

    #[test]
    fn memory_guard_uses_reserve_even_when_rss_unknown() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        let live = LiveSnapshot {
            available_memory: (2 << 30) - 1,
            ..calm()
        };
        assert!(matches!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(live)),
            Decision::Wait(Reason::Memory { .. })
        ));
        let live = LiveSnapshot {
            available_memory: 2 << 30,
            ..calm()
        };
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(live)),
            Decision::Admitted
        );
    }

    #[test]
    fn cpu_guard_holds_head() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        let live = LiveSnapshot {
            cpu_busy: 0.9,
            ..calm()
        };
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(live)),
            Decision::Wait(Reason::Cpu {
                busy: 0.9,
                threshold: 0.85
            })
        );
        let live = LiveSnapshot {
            cpu_busy: 0.85,
            ..calm()
        };
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(live)),
            Decision::Admitted
        );
    }

    #[test]
    fn front_skips_cpu_guard_but_not_memory_guard() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("f", State::Waiting, true, 2, 2),
        ];
        let busy = LiveSnapshot {
            cpu_busy: 1.0,
            ..calm()
        };
        assert_eq!(
            admit(&q, "f", 0, 0.0, LIMITS, Some(busy)),
            Decision::Admitted
        );
        let low_mem = LiveSnapshot {
            available_memory: 0,
            cpu_busy: 1.0,
            ..calm()
        };
        assert!(matches!(
            admit(&q, "f", 0, 0.0, LIMITS, Some(low_mem)),
            Decision::Wait(Reason::Memory { .. })
        ));
    }

    #[test]
    fn front_waits_briefly_when_the_machine_is_overloaded() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("f", State::Waiting, true, 2, 2),
        ];
        let at = |load_avg| LiveSnapshot {
            load_avg,
            cpu_busy: 1.0,
            ..calm()
        };
        assert_eq!(
            admit(&q, "f", 0, 0.0, LIMITS, Some(at(24.0))),
            Decision::Wait(Reason::Overload {
                load: 24.0,
                limit: 24.0
            })
        );
        assert_eq!(
            admit(&q, "f", 0, 29.9, LIMITS, Some(at(300.0))),
            Decision::Wait(Reason::Overload {
                load: 300.0,
                limit: 24.0
            })
        );
        assert_eq!(
            admit(&q, "f", 0, 30.0, LIMITS, Some(at(300.0))),
            Decision::Admitted
        );
        assert_eq!(
            admit(&q, "f", 0, 0.0, LIMITS, Some(at(23.9))),
            Decision::Admitted
        );
        let off = Limits {
            front_overload_load: 0.0,
            ..LIMITS
        };
        assert_eq!(
            admit(&q, "f", 0, 0.0, off, Some(at(300.0))),
            Decision::Admitted
        );
        // Not a front entry: the CPU guard, not the overload guard.
        let n = [
            e("r", State::Running, false, 2, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        assert!(matches!(
            admit(&n, "w", 0, 0.0, LIMITS, Some(at(300.0))),
            Decision::Wait(Reason::Cpu { .. })
        ));
        // Nothing managed running: the starvation rule still wins.
        let alone = [e("f", State::Waiting, true, 2, 1)];
        assert_eq!(
            admit(&alone, "f", 0, 0.0, LIMITS, Some(at(300.0))),
            Decision::Admitted
        );
    }

    #[test]
    fn cpu_guard_gives_up_after_max_wait() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        let busy = LiveSnapshot {
            cpu_busy: 1.0,
            ..calm()
        };
        assert!(matches!(
            admit(&q, "w", 0, 119.9, LIMITS, Some(busy)),
            Decision::Wait(Reason::Cpu { .. })
        ));
        assert_eq!(
            admit(&q, "w", 0, 120.0, LIMITS, Some(busy)),
            Decision::Admitted
        );
    }

    #[test]
    fn starvation_rule_skips_guards_when_nothing_runs() {
        let q = [e("w", State::Waiting, false, 2, 1)];
        let live = LiveSnapshot {
            available_memory: 0,
            cpu_busy: 1.0,
            ..calm()
        };
        assert_eq!(
            admit(&q, "w", 8 << 30, 0.0, LIMITS, Some(live)),
            Decision::Admitted
        );
    }

    #[test]
    fn missing_snapshot_skips_guards() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        assert_eq!(
            admit(&q, "w", 8 << 30, 0.0, LIMITS, None),
            Decision::Admitted
        );
    }

    #[test]
    fn budget_is_checked_before_guards() {
        let q = [
            e("r", State::Running, false, 12, 1),
            e("w", State::Waiting, false, 1, 2),
        ];
        let live = LiveSnapshot {
            available_memory: 0,
            ..calm()
        };
        assert!(matches!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(live)),
            Decision::Wait(Reason::Budget { .. })
        ));
    }

    #[test]
    fn running_entry_is_charged_what_it_uses_beyond_its_weight() {
        let r = e("r", State::Running, false, 2, 1);
        assert_eq!(charged(&r), 2);
        assert_eq!(charged(&using(r.clone(), 0.3)), 2);
        assert_eq!(charged(&using(r.clone(), 2.49)), 2);
        assert_eq!(charged(&using(r.clone(), 2.5)), 3);
        assert_eq!(charged(&using(r.clone(), 5.1)), 5);
        assert_eq!(charged(&using(r, f64::NAN)), 2);
    }

    #[test]
    fn measured_use_holds_the_head_when_declared_weight_would_not() {
        let declared = [
            e("r", State::Running, false, 1, 1),
            e("w", State::Waiting, false, 2, 2),
        ];
        let budget4 = Limits {
            budget: 4,
            ..LIMITS
        };
        assert_eq!(
            admit(&declared, "w", 0, 0.0, budget4, Some(calm())),
            Decision::Admitted
        );
        let measured = [using(declared[0].clone(), 3.0), declared[1].clone()];
        assert_eq!(
            admit(&measured, "w", 0, 0.0, budget4, Some(calm())),
            Decision::Wait(Reason::Budget { used: 3, budget: 4 })
        );
        assert_eq!(used_budget(&measured), 3);
        // A waiting entry's measurement (there is none in practice) is not
        // charged.
        let q = [using(e("w", State::Waiting, false, 1, 1), 9.0)];
        assert_eq!(used_budget(&q), 0);
    }

    #[test]
    fn front_is_held_by_measured_use_only_for_a_while() {
        let q = [
            using(e("r", State::Running, false, 1, 1), 12.0),
            e("f", State::Waiting, true, 2, 2),
        ];
        let held = Decision::Wait(Reason::Budget {
            used: 12,
            budget: 12,
        });
        assert_eq!(admit(&q, "f", 0, 0.0, LIMITS, Some(calm())), held);
        assert_eq!(admit(&q, "f", 0, 29.9, LIMITS, Some(calm())), held);
        assert_eq!(
            admit(&q, "f", 0, 30.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        // Declared weights still hold it however long it waits.
        let full = [
            e("r", State::Running, false, 11, 1),
            e("f", State::Waiting, true, 2, 2),
        ];
        assert_eq!(
            admit(&full, "f", 0, 999.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 11,
                budget: 12
            })
        );
        // A normal entry is held by measured use for as long as it lasts.
        let n = [
            using(e("r", State::Running, false, 1, 1), 12.0),
            e("w", State::Waiting, false, 2, 2),
        ];
        assert!(matches!(
            admit(&n, "w", 0, 999.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget { .. })
        ));
    }

    /// The shape that held a queue for 50 minutes: two idle resident
    /// commands hold 4 of 12, a `--front` head of weight 9 cannot fit, and
    /// three short commands behind it each would.
    fn idle_residents_and_a_heavy_front_head() -> Vec<QueueEntry> {
        vec![
            using(e("dev1", State::Running, false, 2, 1), 0.0),
            using(e("dev2", State::Running, false, 2, 2), 0.0),
            waited(e("head", State::Waiting, true, 9, 3), 3000.0),
            waited(short(e("t1", State::Waiting, false, 2, 4), 40.0), 2900.0),
            waited(short(e("t2", State::Waiting, false, 4, 5), 90.0), 2900.0),
            waited(short(e("t3", State::Waiting, false, 3, 6), 60.0), 2900.0),
        ]
    }

    #[test]
    fn short_commands_start_behind_a_head_that_does_not_fit() {
        let q = idle_residents_and_a_heavy_front_head();
        assert_eq!(
            admit(&q, "head", 0, 3000.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 4,
                budget: 12
            })
        );
        assert_eq!(
            admit(&q, "t1", 0, 2900.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        // t2 fits too, but t1 is ahead of it and fits: t1 goes first.
        assert_eq!(
            admit(&q, "t2", 0, 2900.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 2 })
        );
        // Once t1 runs, t2 fits (4 + 2 + 4 = 10), and then t3 does not.
        let mut q = q;
        q[3].state = State::Running;
        q[3].backfilled = true;
        assert_eq!(
            admit(&q, "t2", 0, 2900.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        q[4].state = State::Running;
        q[4].backfilled = true;
        assert_eq!(
            admit(&q, "t3", 0, 2900.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        // The head is held by the residents, not by t1 and t2 (4 + 9 > 12),
        // so its long wait does not close backfill.
        assert_eq!(used_budget(&q), 10);
    }

    #[test]
    fn backfill_passes_only_short_commands_with_history() {
        let mut q = idle_residents_and_a_heavy_front_head();
        q[3].expected_secs = None;
        assert_eq!(
            admit(&q, "t1", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        // An unknown command ahead cannot start before the head either, so
        // the short one behind it may pass both.
        assert_eq!(
            admit(&q, "t2", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        q[3].expected_secs = Some(300.0);
        assert_eq!(
            admit(&q, "t1", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        q[3].expected_secs = Some(300.1);
        assert_eq!(
            admit(&q, "t1", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        let off = Limits {
            backfill_max_job_secs: 0.0,
            ..LIMITS
        };
        q[3].expected_secs = Some(0.0);
        assert_eq!(
            admit(&q, "t1", 0, 0.0, off, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
    }

    #[test]
    fn a_head_that_fits_is_never_passed() {
        let q = [
            e("r", State::Running, false, 2, 1),
            e("head", State::Waiting, false, 2, 2),
            short(e("s", State::Waiting, false, 1, 3), 1.0),
        ];
        assert_eq!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
    }

    #[test]
    fn backfill_must_fit_and_passes_the_guards() {
        let q = [
            e("r", State::Running, false, 10, 1),
            e("head", State::Waiting, false, 4, 2),
            short(e("s", State::Waiting, false, 3, 3), 1.0),
        ];
        assert_eq!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        let q = [
            e("r", State::Running, false, 10, 1),
            e("head", State::Waiting, false, 4, 2),
            short(e("s", State::Waiting, false, 2, 3), 1.0),
        ];
        assert_eq!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        let busy = LiveSnapshot {
            cpu_busy: 1.0,
            ..calm()
        };
        assert!(matches!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(busy)),
            Decision::Wait(Reason::Cpu { .. })
        ));
    }

    #[test]
    fn a_head_held_up_by_backfill_closes_it_after_a_while() {
        // Without the backfilled entry the head would fit (8 + 4 <= 12).
        let q = |head_waited| {
            [
                e("r", State::Running, false, 8, 1),
                backfilled(e("b", State::Running, false, 2, 2)),
                waited(e("head", State::Waiting, false, 4, 3), head_waited),
                short(e("s", State::Waiting, false, 1, 4), 1.0),
            ]
        };
        assert_eq!(
            admit(&q(299.9), "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        assert_eq!(
            admit(&q(300.0), "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Reserved { ahead: 1 })
        );
        // Held up by what is not backfilled: no cap, however long.
        let q = [
            e("r", State::Running, false, 9, 1),
            backfilled(e("b", State::Running, false, 1, 2)),
            waited(e("head", State::Waiting, false, 4, 3), 9999.0),
            short(e("s", State::Waiting, false, 1, 4), 1.0),
        ];
        assert_eq!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
    }

    #[test]
    fn a_front_entry_held_up_by_backfill_is_not_passed_again() {
        let q = [
            e("r", State::Running, false, 8, 1),
            backfilled(e("b", State::Running, false, 2, 2)),
            e("f", State::Waiting, true, 4, 3),
            short(e("s", State::Waiting, false, 1, 4), 1.0),
        ];
        assert_eq!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Reserved { ahead: 1 })
        );
        // ...but one blocked by something else is.
        let q = [
            e("r", State::Running, false, 10, 1),
            e("f", State::Waiting, true, 4, 2),
            short(e("s", State::Waiting, false, 1, 3), 1.0),
        ];
        assert_eq!(
            admit(&q, "s", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
    }

    #[test]
    fn a_front_entry_ahead_sees_declared_weights_after_its_window() {
        // Declared 2, measured 9. Past its window the front entry fits by
        // declared weights (2 + 4), so it is the one to start, not `s`.
        let q = |front_waited| {
            [
                using(e("r", State::Running, false, 2, 1), 9.0),
                waited(e("f", State::Waiting, true, 4, 2), front_waited),
                short(e("s", State::Waiting, false, 1, 3), 1.0),
            ]
        };
        assert_eq!(
            admit(&q(30.0), "s", 0, 0.0, LIMITS, None),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        // Within it, 9 + 4 does not fit, and `s` (9 + 1) does.
        assert_eq!(
            admit(&q(29.9), "s", 0, 0.0, LIMITS, None),
            Decision::Admitted
        );
    }

    /// `q` as started by the command of the entry `parent`.
    fn child(mut q: QueueEntry, parent: &str) -> QueueEntry {
        q.parent = Some(parent.into());
        q
    }
    /// Nothing is free: no memory, every core busy, a high load.
    fn saturated() -> LiveSnapshot {
        LiveSnapshot {
            available_memory: 0,
            cpu_busy: 1.0,
            load_avg: 99.0,
        }
    }

    #[test]
    fn nested_entries_use_up_their_parents_weight_before_they_are_charged() {
        let p = e("p", State::Running, false, 4, 1);
        let c = |id: &str, seq| child(e(id, State::Running, false, 2, seq), "p");
        // One and two children fit in the parent's 4: nothing on top.
        assert_eq!(used_budget(&[p.clone(), c("c1", 2)]), 4);
        assert_eq!(used_budget(&[p.clone(), c("c1", 2), c("c2", 3)]), 4);
        // The third does not: the group takes what its children take.
        assert_eq!(
            used_budget(&[p.clone(), c("c1", 2), c("c2", 3), c("c3", 4)]),
            6
        );
        // What the parent's own processes use comes on top of its children.
        assert_eq!(
            used_budget(&[using(p.clone(), 1.4), c("c1", 2), c("c2", 3)]),
            5
        );
        assert_eq!(used_budget(&[using(p.clone(), 3.0), c("c1", 2)]), 5);
        // A child is charged what it uses beyond its weight, as any entry.
        assert_eq!(used_budget(&[p.clone(), using(c("c1", 2), 5.2)]), 5);
        // A waiting child takes nothing yet.
        assert_eq!(
            used_budget(&[p, child(e("w", State::Waiting, false, 9, 2), "p")]),
            4
        );
    }

    #[test]
    fn nesting_is_counted_to_any_depth() {
        let q = [
            e("p", State::Running, false, 1, 1),
            child(e("c", State::Running, false, 2, 2), "p"),
            child(e("g", State::Running, false, 5, 3), "c"),
            child(e("h", State::Running, false, 1, 4), "c"),
            e("other", State::Running, false, 3, 5),
        ];
        assert_eq!(used_budget(&q), 6 + 3);
    }

    #[test]
    fn an_entry_whose_parent_is_not_running_is_top_level() {
        // The parent is gone from the ledger (it finished, or the lease
        // named was never there): the entry is charged on its own.
        let gone = [
            child(e("c", State::Running, false, 3, 2), "gone"),
            e("r", State::Running, false, 2, 3),
        ];
        assert!(!is_nested(&gone, &gone[0]));
        assert_eq!(used_budget(&gone), 5);
        // A parent that is in the ledger but not running does not count
        // either.
        let waiting = [
            e("p", State::Waiting, false, 4, 1),
            child(e("c", State::Running, false, 3, 2), "p"),
        ];
        assert!(!is_nested(&waiting, &waiting[1]));
        assert_eq!(used_budget(&waiting), 3);
        let running = [
            e("p", State::Running, false, 4, 1),
            child(e("c", State::Running, false, 3, 2), "p"),
        ];
        assert!(is_nested(&running, &running[1]));
        assert!(!is_nested(&running, &running[0]));
    }

    #[test]
    fn entries_that_name_each_other_as_parent_are_not_followed_for_ever() {
        // Cannot be written by `exec` (a parent runs before its child
        // exists); a ledger edited by hand must still not hang admission.
        let q = [
            child(e("a", State::Running, false, 3, 1), "b"),
            child(e("b", State::Running, false, 3, 2), "a"),
            e("r", State::Running, false, 2, 3),
            e("w", State::Waiting, false, 1, 4),
        ];
        assert_eq!(used_budget(&q), 2);
        assert_eq!(
            admit(&q, "w", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
    }

    #[test]
    fn nested_entries_are_ahead_of_top_level_ones() {
        let q = [
            e("p", State::Running, false, 1, 1),
            e("front", State::Waiting, true, 1, 2),
            e("top", State::Waiting, false, 1, 3),
            child(e("late", State::Waiting, false, 1, 4), "p"),
            child(e("late-front", State::Waiting, true, 1, 5), "p"),
            // Its parent is not running: queued as a top-level entry.
            child(e("orphan", State::Waiting, false, 1, 6), "gone"),
        ];
        let ids: Vec<&str> = queue_order(&q).iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["late-front", "late", "front", "top", "orphan"]);
    }

    #[test]
    fn a_nested_entry_that_fits_in_its_parents_share_starts_when_the_budget_is_full() {
        // The parent holds the whole budget and one child runs in it.
        let family = |weight| {
            [
                e("p", State::Running, false, 12, 1),
                child(e("c1", State::Running, false, 4, 2), "p"),
                child(e("c2", State::Waiting, false, weight, 3), "p"),
            ]
        };
        // 4 + 8 is exactly the parent's 12: nothing more is taken.
        assert_eq!(
            admit(&family(8), "c2", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        // One more does not fit, and it has a running sibling to wait for.
        assert_eq!(
            admit(&family(9), "c2", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 12,
                budget: 12
            })
        );
        // With a running sibling the guards hold it like any other entry.
        assert_eq!(
            admit(&family(8), "c2", 0, 0.0, LIMITS, Some(saturated())),
            Decision::Wait(Reason::Memory {
                available: 0,
                need: 2 << 30
            })
        );
    }

    #[test]
    fn the_only_nested_entry_of_a_parent_is_not_held_by_the_budget() {
        // The parent uses the whole budget itself and waits for its child:
        // holding the child for the budget would hold both for ever.
        let q = [
            using(e("p", State::Running, false, 12, 1), 12.0),
            e("other", State::Running, false, 6, 2),
            e("top", State::Waiting, true, 1, 3),
            child(e("c", State::Waiting, false, 12, 4), "p"),
        ];
        assert_eq!(used_budget(&q), 18);
        assert_eq!(
            admit(&q, "c", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        // Without a reading of the machine there is nothing to hold it.
        assert_eq!(admit(&q, "c", 0, 0.0, LIMITS, None), Decision::Admitted);
    }

    #[test]
    fn the_guards_hold_an_only_nested_entry_for_a_bounded_time() {
        let q = [
            using(e("p", State::Running, false, 12, 1), 12.0),
            child(e("c", State::Waiting, false, 12, 2), "p"),
        ];
        // Memory is short: several parents each starting one more command
        // must not push the machine into swap.
        assert_eq!(
            admit(&q, "c", 0, 59.9, LIMITS, Some(saturated())),
            Decision::Wait(Reason::Memory {
                available: 0,
                need: 2 << 30
            })
        );
        let busy = LiveSnapshot {
            cpu_busy: 0.95,
            ..calm()
        };
        assert_eq!(
            admit(&q, "c", 0, 59.9, LIMITS, Some(busy)),
            Decision::Wait(Reason::Cpu {
                busy: 0.95,
                threshold: 0.85
            })
        );
        // Its parent cannot finish without it: it starts anyway in the end.
        assert_eq!(
            admit(&q, "c", 0, 60.0, LIMITS, Some(saturated())),
            Decision::Admitted
        );
        // That is for the only child. One with a running sibling that
        // fits the budget is held by the guards like any entry.
        let fits = [
            e("p", State::Running, false, 12, 1),
            child(e("c1", State::Running, false, 4, 2), "p"),
            child(e("c2", State::Waiting, false, 4, 3), "p"),
        ];
        assert_eq!(
            admit(&fits, "c2", 0, 60.0, LIMITS, Some(saturated())),
            Decision::Wait(Reason::Memory {
                available: 0,
                need: 2 << 30
            })
        );
    }

    #[test]
    fn a_nested_entry_starts_once_it_and_its_running_siblings_have_waited_long_enough() {
        // `c1` may be waiting for `c2` (a server and its client): `c2`
        // does not wait for the budget for ever.
        let family = |c1_ran, limits: Limits, waited_secs| {
            let q = [
                e("p", State::Running, false, 12, 1),
                ran(child(e("c1", State::Running, false, 12, 2), "p"), c1_ran),
                child(e("c2", State::Waiting, false, 12, 3), "p"),
                child(e("c3", State::Waiting, false, 12, 4), "p"),
            ];
            move |id: &str| admit(&q, id, 0, waited_secs, limits, Some(saturated()))
        };
        let budget = Decision::Wait(Reason::Budget {
            used: 12,
            budget: 12,
        });
        assert_eq!(family(600.0, LIMITS, 599.9)("c2"), budget);
        assert_eq!(family(600.0, LIMITS, 600.0)("c2"), Decision::Admitted);
        // One at a time: the sibling that started last has not run long
        // enough yet, and the next in the queue goes first.
        assert_eq!(family(599.9, LIMITS, 600.0)("c2"), budget);
        assert_eq!(
            family(600.0, LIMITS, 600.0)("c3"),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        // 0 turns it off.
        let off = Limits {
            nested_sibling_max_wait_secs: 0.0,
            ..LIMITS
        };
        assert_eq!(family(900.0, off, 900.0)("c2"), budget);
    }

    #[test]
    fn the_sibling_limit_is_not_what_frees_an_only_nested_entry_from_the_guards() {
        let limits = Limits {
            nested_guard_max_wait_secs: 1000.0,
            ..LIMITS
        };
        let q = [
            e("p", State::Running, false, 12, 1),
            child(e("c", State::Waiting, false, 12, 2), "p"),
        ];
        assert_eq!(
            admit(&q, "c", 0, 700.0, limits, Some(saturated())),
            Decision::Wait(Reason::Memory {
                available: 0,
                need: 2 << 30
            })
        );
    }

    #[test]
    fn two_parents_that_each_wait_for_a_nested_entry_both_get_it() {
        // Neither child fits beside the other family; each parent waits
        // for its own, so each gets one, in whatever order they ask.
        let q = [
            e("p1", State::Running, false, 6, 1),
            e("p2", State::Running, false, 6, 2),
            child(e("c1", State::Waiting, false, 9, 3), "p1"),
            child(e("c2", State::Waiting, false, 9, 4), "p2"),
        ];
        for id in ["c2", "c1"] {
            assert_eq!(
                admit(&q, id, 0, 0.0, LIMITS, Some(calm())),
                Decision::Admitted,
                "{id}"
            );
        }
    }

    #[test]
    fn nested_entries_of_one_parent_start_in_queue_order() {
        let q = [
            e("p", State::Running, false, 12, 1),
            child(e("c1", State::Waiting, false, 12, 2), "p"),
            child(e("c2", State::Waiting, false, 12, 3), "p"),
        ];
        assert_eq!(
            admit(&q, "c1", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        assert_eq!(
            admit(&q, "c2", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
        // Once the first runs, the second waits for the budget it takes.
        let q = [
            e("p", State::Running, false, 12, 1),
            child(e("c1", State::Running, false, 12, 2), "p"),
            child(e("c2", State::Waiting, false, 12, 3), "p"),
        ];
        assert_eq!(
            admit(&q, "c2", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 12,
                budget: 12
            })
        );
    }

    #[test]
    fn a_top_level_head_that_does_not_fit_does_not_hold_a_nested_entry() {
        // `big` waits for the budget `p` holds, and `p` cannot finish
        // before its nested entries have run: they go first.
        let q = [
            e("p", State::Running, false, 6, 1),
            child(e("c1", State::Running, false, 2, 2), "p"),
            e("big", State::Waiting, true, 12, 3),
            child(e("c2", State::Waiting, false, 2, 4), "p"),
        ];
        assert_eq!(
            admit(&q, "c2", 0, 0.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
        assert_eq!(
            admit(&q, "big", 0, 0.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::NotHead { ahead: 1 })
        );
    }

    #[test]
    fn a_front_nested_entry_sees_declared_weights_after_its_window() {
        // `p` uses 9 cores itself, far beyond its weight of 2.
        let family = |waited_secs| {
            [
                e("r", State::Running, false, 10, 1),
                using(e("p", State::Running, false, 2, 2), 9.0),
                child(e("c1", State::Running, false, 1, 3), "p"),
                waited(child(e("c2", State::Waiting, true, 1, 4), "p"), waited_secs),
            ]
        };
        // Measured: 10 + (9 + 1 + 1) is over the budget.
        assert_eq!(
            admit(&family(29.0), "c2", 0, 29.0, LIMITS, Some(calm())),
            Decision::Wait(Reason::Budget {
                used: 20,
                budget: 12
            })
        );
        // Declared: the two children fit exactly in the parent's 2.
        assert_eq!(
            admit(&family(30.0), "c2", 0, 30.0, LIMITS, Some(calm())),
            Decision::Admitted
        );
    }

    #[test]
    fn used_budget_ignores_waiting() {
        let q = [
            e("r", State::Running, false, 3, 1),
            e("r2", State::Running, false, 4, 2),
            e("w", State::Waiting, false, 100, 3),
        ];
        assert_eq!(used_budget(&q), 7);
    }
}
