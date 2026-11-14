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
    if wall < old.observed {
        return hold();
    }
    let effective = predicted.min(wall);
    let state = ClockState {
        effective,
        observed: wall,
        mono,
        ..old.clone()
    };
    // A forward wall jump cannot advance effective time. Pause claiming until
    // monotonic time catches it; never rebase effective to the jumped wall.
    let cutoff = if mono >= old.ready && wall.saturating_sub(predicted) <= 1000 {
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
        let jump = step(Some(&s), Some("a"), 5000, 30, 20);
        assert_eq!(jump.cutoff, None);
        let jumped = jump.state.unwrap();
        assert_eq!(jumped.effective, 120);
        assert_eq!(jumped.mono, 30);
        assert_eq!(
            step(Some(&jumped), Some("a"), 5000, 4910, 20).cutoff,
            Some(5000)
        );
        assert_eq!(step(Some(&s), Some("a"), 1120, 30, 20).cutoff, Some(120));
        assert_eq!(step(Some(&s), Some("a"), 1121, 30, 20).cutoff, None);
        let behind = step(Some(&s), Some("a"), 110, 30, 20);
        assert_eq!(behind.cutoff, Some(110));
        assert_eq!(behind.state.unwrap().observed, 110);
    }
}
