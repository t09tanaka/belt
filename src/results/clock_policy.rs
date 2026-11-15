//! Pure conservative retention cutoff; wall time alone never authorizes deletion.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockState {
    pub boot: Option<String>,
    pub effective: u64,
    pub observed: u64,
    pub mono: u64,
    pub ready: u64,
    pub retention: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockStep {
    pub state: Option<ClockState>,
    pub cutoff: Option<u64>,
}

/// Bootstrap/reboot quarantines for a full retention. Same-boot restart credits
/// only persisted monotonic elapsed time. Invalid samples do not move the baseline.
///
/// A wall step is judged per sample: the wall advance since the last accepted
/// observation against the monotonic advance over the same interval. Slow drift
/// between the two clocks therefore never accumulates into a jump. A step of
/// more than one second in either direction starts a new full quarantine on
/// the monotonic clock and re-bases onto the new wall reading, so one step
/// delays deletion by one retention instead of stopping it until restart.
pub fn step(
    previous: Option<&ClockState>,
    boot: Option<&str>,
    wall: u64,
    mono: u64,
    retention: u64,
) -> ClockStep {
    let hold = || ClockStep {
        state: previous.cloned(),
        cutoff: None,
    };
    if wall == 0 || mono == u64::MAX || wall > i64::MAX as u64 {
        return hold();
    }
    let Some(old) = previous
        .filter(|old| boot.is_some() && old.boot.as_deref() == boot && retention <= old.retention)
    else {
        let Some(ready) = mono.checked_add(retention) else {
            return hold();
        };
        return ClockStep {
            state: Some(ClockState {
                boot: boot.map(str::to_owned),
                effective: wall,
                observed: wall,
                mono,
                ready,
                retention,
            }),
            cutoff: None,
        };
    };
    let Some(elapsed) = mono.checked_sub(old.mono) else {
        return hold();
    };
    let Some(predicted) = old.effective.checked_add(elapsed) else {
        return hold();
    };
    let jumped = if wall < old.observed {
        if old.observed - wall <= 1000 {
            // A small backward correction: wait for the wall to pass it again.
            return hold();
        }
        true
    } else {
        (wall - old.observed).saturating_sub(elapsed) > 1000
    };
    if jumped {
        // `old.ready` is an earlier monotonic reading plus `old.retention`, so
        // this never ends before the quarantine the old basis already required.
        let Some(ready) = mono.checked_add(old.retention) else {
            return hold();
        };
        return ClockStep {
            state: Some(ClockState {
                effective: wall,
                observed: wall,
                mono,
                ready,
                ..old.clone()
            }),
            cutoff: None,
        };
    }
    let effective = predicted.min(wall);
    let state = ClockState {
        effective,
        observed: wall,
        mono,
        ..old.clone()
    };
    let cutoff = if mono >= old.ready {
        Some(effective)
    } else {
        None
    };
    ClockStep {
        state: Some(state),
        cutoff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn seed() -> ClockState {
        step(None, Some("a"), 100, 10, 20).state.unwrap()
    }
    #[test]
    fn quarantine_and_same_boot_continuation() {
        let s = seed();
        assert_eq!(s.ready, 30);
        assert_eq!(s.retention, 20);
        assert_eq!(s.effective, 100);
        assert_eq!(s.observed, 100);
        assert_eq!(s.mono, 10);
        assert_eq!(s.boot.as_deref(), Some("a"));
        assert_eq!(step(Some(&s), Some("a"), 119, 29, 20).cutoff, None);
        assert_eq!(step(Some(&s), Some("a"), 120, 30, 20).cutoff, Some(120));
        assert_eq!(step(Some(&s), Some("a"), 121, 31, 20).cutoff, Some(121));
        let restart = step(Some(&s), Some("b"), 120, 3, 20);
        assert_eq!(restart.cutoff, None);
        assert_eq!(restart.state.unwrap().ready, 23);
        assert_eq!(step(Some(&s), None, 120, 30, 20).cutoff, None);
        let longer = step(Some(&s), Some("a"), 120, 30, 21);
        assert_eq!(longer.cutoff, None);
        assert_eq!(longer.state.unwrap().ready, 51);
        let unknown = step(None, None, 100, 10, 20).state.unwrap();
        assert_eq!(
            step(Some(&unknown), None, 120, 30, 20).state.unwrap().ready,
            50
        );
        let boundary = step(None, Some("a"), i64::MAX as u64, 0, 0);
        assert_eq!(boundary.state.unwrap().effective, i64::MAX as u64);
        let zero = step(None, Some("a"), 100, 10, 0);
        assert_eq!(zero.cutoff, None);
        assert_eq!(
            step(zero.state.as_ref(), Some("a"), 100, 10, 0).cutoff,
            Some(100)
        );
    }
    #[test]
    fn clock_faults_never_authorize_or_poison_baseline() {
        let s = seed();
        for (wall, mono) in [
            (0, 30),
            (120, u64::MAX),
            (i64::MAX as u64 + 1, 30),
            (120, 9),
            (99, 30),
        ] {
            assert_eq!(
                step(Some(&s), Some("a"), wall, mono, 0),
                ClockStep {
                    state: Some(s.clone()),
                    cutoff: None
                }
            );
        }
        assert_eq!(
            step(None, Some("a"), 100, u64::MAX - 1, 2),
            ClockStep {
                state: None,
                cutoff: None
            }
        );
        assert_eq!(
            step(None, Some("a"), 0, 10, 0),
            ClockStep {
                state: None,
                cutoff: None
            }
        );
        assert_eq!(
            step(None, Some("a"), 100, u64::MAX, 0),
            ClockStep {
                state: None,
                cutoff: None
            }
        );
        let huge = ClockState {
            effective: u64::MAX,
            ..s.clone()
        };
        assert_eq!(step(Some(&huge), Some("a"), 120, 30, 0).cutoff, None);
        assert_eq!(step(Some(&s), Some("a"), 1120, 30, 20).cutoff, Some(120));
        assert_eq!(step(Some(&s), Some("a"), 1121, 30, 20).cutoff, None);
        let behind = step(Some(&s), Some("a"), 110, 30, 20);
        assert_eq!(behind.cutoff, Some(110));
        assert_eq!(behind.state.unwrap().observed, 110);
    }
    fn rebased(wall: u64, mono: u64, ready: u64) -> ClockState {
        ClockState {
            boot: Some("a".into()),
            effective: wall,
            observed: wall,
            mono,
            ready,
            retention: 20,
        }
    }
    #[test]
    fn forward_jump_requarantines_then_follows_the_new_wall() {
        let s = seed();
        let jump = step(Some(&s), Some("a"), 5000, 30, 20);
        assert_eq!(
            jump,
            ClockStep {
                state: Some(rebased(5000, 30, 50)),
                cutoff: None
            }
        );
        let jumped = jump.state.unwrap();
        // Both clocks keep advancing after the step, as they do on a real host.
        let waiting = step(Some(&jumped), Some("a"), 5019, 49, 20);
        assert_eq!(waiting.cutoff, None);
        assert_eq!(waiting.state.as_ref().unwrap().ready, 50);
        assert_eq!(
            step(waiting.state.as_ref(), Some("a"), 5020, 50, 20).cutoff,
            Some(5020)
        );
        assert_eq!(
            step(Some(&jumped), Some("a"), 5021, 51, 20).cutoff,
            Some(5021)
        );
        // A second step inside the quarantine starts it again from that sample.
        let again = step(Some(&jumped), Some("a"), 9000, 40, 20);
        assert_eq!(
            again,
            ClockStep {
                state: Some(rebased(9000, 40, 60)),
                cutoff: None
            }
        );
        assert_eq!(
            step(again.state.as_ref(), Some("a"), 9019, 59, 20).cutoff,
            None
        );
        assert_eq!(
            step(again.state.as_ref(), Some("a"), 9020, 60, 20).cutoff,
            Some(9020)
        );
        // The quarantine keeps the recorded retention when the caller's shrank.
        assert_eq!(
            step(Some(&s), Some("a"), 5000, 30, 5).state,
            Some(rebased(5000, 30, 50))
        );
        // Retention zero (secret erasure) resumes on the very next sample.
        let zero = step(None, Some("a"), 100, 10, 0).state.unwrap();
        let erased = step(Some(&zero), Some("a"), 9000, 11, 0);
        assert_eq!(erased.cutoff, None);
        assert_eq!(
            step(erased.state.as_ref(), Some("a"), 9001, 12, 0).cutoff,
            Some(9001)
        );
        // An unrepresentable quarantine end holds the old baseline.
        let late = ClockState {
            mono: u64::MAX - 30,
            ready: u64::MAX - 10,
            ..s.clone()
        };
        assert_eq!(
            step(Some(&late), Some("a"), 5000, u64::MAX - 19, 20),
            ClockStep {
                state: Some(late.clone()),
                cutoff: None
            }
        );
        let last = step(Some(&late), Some("a"), 5000, u64::MAX - 20, 20);
        assert_eq!(last.cutoff, None);
        assert_eq!(last.state.unwrap().ready, u64::MAX);
    }
    #[test]
    fn jump_is_judged_per_sample_and_drift_never_accumulates() {
        let s = seed();
        // The tolerance is against the last observation, not the lagging basis.
        let lagging = ClockState {
            effective: 50,
            ..s.clone()
        };
        assert_eq!(
            step(Some(&lagging), Some("a"), 1120, 30, 20).cutoff,
            Some(70)
        );
        assert_eq!(
            step(Some(&lagging), Some("a"), 1121, 30, 20).state,
            Some(rebased(1121, 30, 50))
        );
        // A wall that runs 10% fast drifts two seconds ahead without a jump.
        let mut state = ClockState {
            ready: 10,
            ..s.clone()
        };
        for n in 1..=2000u64 {
            let next = step(Some(&state), Some("a"), 100 + 11 * n, 10 + 10 * n, 20);
            assert_eq!(next.cutoff, Some(100 + 10 * n), "sample {n}");
            state = next.state.unwrap();
        }
        assert_eq!(state.observed - state.effective, 2000);
        assert_eq!(state.ready, 10);
    }
    #[test]
    fn backward_step_holds_briefly_or_requarantines() {
        let s = ClockState {
            effective: 5000,
            observed: 5000,
            ..seed()
        };
        // Up to one second back: nothing moves until the wall passes it again.
        for wall in [4999, 4000] {
            assert_eq!(
                step(Some(&s), Some("a"), wall, 30, 20),
                ClockStep {
                    state: Some(s.clone()),
                    cutoff: None
                }
            );
        }
        assert_eq!(step(Some(&s), Some("a"), 5000, 30, 20).cutoff, Some(5000));
        // Further back: re-base lower after one more full retention.
        let back = step(Some(&s), Some("a"), 3999, 30, 20);
        assert_eq!(
            back,
            ClockStep {
                state: Some(rebased(3999, 30, 50)),
                cutoff: None
            }
        );
        assert_eq!(
            step(back.state.as_ref(), Some("a"), 4018, 49, 20).cutoff,
            None
        );
        assert_eq!(
            step(back.state.as_ref(), Some("a"), 4019, 50, 20).cutoff,
            Some(4019)
        );
    }
    #[test]
    fn forward_glitch_corrected_back_recovers_after_one_retention() {
        let glitch = step(Some(&seed()), Some("a"), 900_000, 30, 20);
        let fixed = step(glitch.state.as_ref(), Some("a"), 121, 31, 20);
        assert_eq!(
            fixed,
            ClockStep {
                state: Some(rebased(121, 31, 51)),
                cutoff: None
            }
        );
        assert_eq!(
            step(fixed.state.as_ref(), Some("a"), 141, 51, 20).cutoff,
            Some(141)
        );
    }
    #[test]
    fn restart_with_an_implausible_wall_gap_quarantines() {
        // Same boot: the persisted observation exposes the gap as a jump.
        let s = seed();
        let resumed = step(Some(&s), Some("a"), 900_000, 40, 20);
        assert_eq!(
            resumed,
            ClockStep {
                state: Some(rebased(900_000, 40, 60)),
                cutoff: None
            }
        );
        // New boot: a full quarantine regardless of how far the wall moved.
        let reboot = step(Some(&s), Some("b"), 900_000, 3, 20);
        assert_eq!(reboot.cutoff, None);
        let reboot = reboot.state.unwrap();
        assert_eq!(reboot.ready, 23);
        assert_eq!(step(Some(&reboot), Some("b"), 900_019, 22, 20).cutoff, None);
        assert_eq!(
            step(Some(&reboot), Some("b"), 900_020, 23, 20).cutoff,
            Some(900_020)
        );
    }
}
