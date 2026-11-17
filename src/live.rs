//! Live machine snapshot via `sysinfo` (spec "Live guards").

use crate::admit::LiveSnapshot;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};

/// Two CPU samples this far apart give the busy fraction "right now". Used
/// when the process has no recent sample to measure from.
pub const CPU_SAMPLE_INTERVAL: Duration = Duration::from_millis(300);

/// A previous sample older than this is not used as the start of the
/// interval: the average over a long stretch would dilute "right now".
pub const CPU_SAMPLE_MAX_AGE: Duration = Duration::from_secs(5);

/// What to do for the CPU figure, given how long ago this process last
/// sampled the CPU (`None`: never).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CpuPlan {
    /// The last sample is too recent for the counters to have moved
    /// meaningfully: return the figure computed then.
    Reuse,
    /// Sample once, without waiting: the busy fraction since the last sample.
    Refresh,
    /// No usable last sample: sample, wait [`CPU_SAMPLE_INTERVAL`], sample.
    Resample,
}

fn cpu_plan(age: Option<Duration>, min: Duration, max: Duration) -> CpuPlan {
    match age {
        Some(age) if age < min => CpuPlan::Reuse,
        Some(age) if age <= max => CpuPlan::Refresh,
        _ => CpuPlan::Resample,
    }
}

/// CPU counters that report the busy fraction (0.0..=1.0) since the
/// previous `sample` call (the first call only sets the starting point).
trait CpuProbe {
    fn sample(&mut self) -> f64;
}

struct SysinfoCpu(System);

impl SysinfoCpu {
    fn new() -> Self {
        Self(System::new_with_specifics(RefreshKind::nothing()))
    }
}

impl CpuProbe for SysinfoCpu {
    fn sample(&mut self) -> f64 {
        self.0.refresh_cpu_usage();
        (self.0.global_cpu_usage() as f64 / 100.0).clamp(0.0, 1.0)
    }
}

/// The CPU counters of this process and the last figure computed from them.
struct CpuState<P> {
    probe: P,
    /// When the last sample was taken, and the busy fraction it gave.
    last: Option<(Instant, f64)>,
    /// Shortest interval the probe can measure over.
    min: Duration,
}

impl<P: CpuProbe> CpuState<P> {
    fn new(probe: P, min: Duration) -> Self {
        Self {
            probe,
            last: None,
            min,
        }
    }

    /// The busy fraction "now". `now` is read before a sample to decide and
    /// after it to stamp, so an interval judged long enough here is never
    /// shorter on the clock the probe keeps for itself.
    fn busy(&mut self, now: impl Fn() -> Instant, sleep: impl FnOnce(Duration)) -> f64 {
        let age = self.last.map(|(at, _)| now().saturating_duration_since(at));
        let busy = match (cpu_plan(age, self.min, CPU_SAMPLE_MAX_AGE), self.last) {
            (CpuPlan::Reuse, Some((_, busy))) => return busy,
            (CpuPlan::Refresh, _) => self.probe.sample(),
            _ => {
                self.probe.sample();
                sleep(CPU_SAMPLE_INTERVAL.max(self.min));
                self.probe.sample()
            }
        };
        self.last = Some((now(), busy));
        busy
    }
}

/// One per process, so a long-lived caller pays the sampling wait once.
///
/// The lock is held across the wait of a resample on purpose. A caller that
/// arrives meanwhile waits at most that one interval (what every call cost
/// before the state was kept) and then reuses the figure just measured,
/// instead of each of them sampling and waiting on its own. Nothing else is
/// done under this lock.
static CPU: Mutex<Option<CpuState<SysinfoCpu>>> = Mutex::new(None);

fn cpu_busy() -> f64 {
    shared_busy(
        &CPU,
        || CpuState::new(SysinfoCpu::new(), sysinfo::MINIMUM_CPU_UPDATE_INTERVAL),
        Instant::now,
        std::thread::sleep,
    )
}

/// [`CpuState::busy`] on the state behind `shared`, made by `init` on the
/// first call. The lock is held for the whole call, the wait included.
fn shared_busy<P: CpuProbe>(
    shared: &Mutex<Option<CpuState<P>>>,
    init: impl FnOnce() -> CpuState<P>,
    now: impl Fn() -> Instant,
    sleep: impl FnOnce(Duration),
) -> f64 {
    // A panic while sampling leaves nothing half-written that matters: the
    // next call measures again from whatever the counters hold.
    let mut guard = shared.lock().unwrap_or_else(|e| e.into_inner());
    guard.get_or_insert_with(init).busy(now, sleep)
}

/// The machine right now. Memory and load average are read on every call.
/// The CPU figure is the busy fraction since this process last sampled it
/// when that was between `sysinfo::MINIMUM_CPU_UPDATE_INTERVAL` and
/// [`CPU_SAMPLE_MAX_AGE`] ago; the previous figure when it was more recent;
/// and otherwise (first call, or the last one is too old) two samples
/// [`CPU_SAMPLE_INTERVAL`] apart, which blocks for that long.
pub fn snapshot() -> Option<LiveSnapshot> {
    let sys = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()),
    );
    if sys.total_memory() == 0 {
        return None;
    }
    let cpu_busy = cpu_busy();
    Some(LiveSnapshot {
        available_memory: available_memory().unwrap_or_else(|| sys.available_memory()),
        cpu_busy,
        load_avg: System::load_average().one.max(0.0),
    })
}

/// Memory a new command can get without swapping: free (which includes
/// speculative), inactive and purgeable pages. `sysinfo` 0.33 also
/// subtracts the pages the compressor occupies, which are none of these, so
/// with a few tens of GiB compressed it reports 0 and holds every command
/// that does not run alone.
#[cfg(target_os = "macos")]
fn available_memory() -> Option<u64> {
    let mut stat: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
    let mut count = libc::HOST_VM_INFO64_COUNT;
    #[allow(deprecated)]
    let host = unsafe { libc::mach_host_self() };
    let ok = unsafe {
        libc::host_statistics64(
            host,
            libc::HOST_VM_INFO64,
            &mut stat as *mut libc::vm_statistics64 as *mut _,
            &mut count,
        )
    } == libc::KERN_SUCCESS;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if !ok || page <= 0 {
        return None;
    }
    Some(pages_available(&stat).saturating_mul(page as u64))
}

#[cfg(target_os = "macos")]
fn pages_available(stat: &libc::vm_statistics64) -> u64 {
    u64::from(stat.free_count)
        .saturating_add(u64::from(stat.inactive_count))
        .saturating_add(u64::from(stat.purgeable_count))
}

#[cfg(not(target_os = "macos"))]
fn available_memory() -> Option<u64> {
    None
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[test]
    fn compressed_pages_do_not_shrink_what_is_available() {
        let mut stat: libc::vm_statistics64 = unsafe { std::mem::zeroed() };
        stat.free_count = 85_000;
        stat.inactive_count = 1_800_000;
        stat.purgeable_count = 4_000;
        stat.compressor_page_count = 2_100_000;
        assert_eq!(pages_available(&stat), 1_889_000);
    }

    #[test]
    fn reads_the_machine() {
        let bytes = available_memory().expect("host_statistics64");
        assert!(bytes > 0);
    }
}

#[cfg(test)]
mod cpu_tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    const MIN: Duration = Duration::from_millis(200);

    /// Returns the queued figures in order and counts the calls.
    struct Scripted {
        figures: Vec<f64>,
        calls: usize,
    }

    impl CpuProbe for Scripted {
        fn sample(&mut self) -> f64 {
            self.calls += 1;
            self.figures.remove(0)
        }
    }

    /// A clock that only moves when told to, and a sleep that moves it.
    struct Harness {
        start: Instant,
        offset: Cell<Duration>,
        sleeps: RefCell<Vec<Duration>>,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                start: Instant::now(),
                offset: Cell::new(Duration::ZERO),
                sleeps: RefCell::new(Vec::new()),
            }
        }

        fn advance(&self, by: Duration) {
            self.offset.set(self.offset.get() + by);
        }

        fn busy(&self, state: &mut CpuState<Scripted>) -> f64 {
            state.busy(
                || self.start + self.offset.get(),
                |d| {
                    self.sleeps.borrow_mut().push(d);
                    self.advance(d);
                },
            )
        }
    }

    fn state(figures: &[f64]) -> CpuState<Scripted> {
        CpuState::new(
            Scripted {
                figures: figures.to_vec(),
                calls: 0,
            },
            MIN,
        )
    }

    #[test]
    fn plan_boundaries() {
        let max = CPU_SAMPLE_MAX_AGE;
        let ms = Duration::from_millis(1);
        assert_eq!(cpu_plan(None, MIN, max), CpuPlan::Resample);
        assert_eq!(cpu_plan(Some(Duration::ZERO), MIN, max), CpuPlan::Reuse);
        assert_eq!(cpu_plan(Some(MIN - ms), MIN, max), CpuPlan::Reuse);
        assert_eq!(cpu_plan(Some(MIN), MIN, max), CpuPlan::Refresh);
        assert_eq!(cpu_plan(Some(max), MIN, max), CpuPlan::Refresh);
        assert_eq!(cpu_plan(Some(max + ms), MIN, max), CpuPlan::Resample);
    }

    #[test]
    fn first_call_waits_the_sample_interval_between_two_samples() {
        let h = Harness::new();
        let mut s = state(&[0.0, 0.4]);
        assert_eq!(h.busy(&mut s), 0.4);
        assert_eq!(s.probe.calls, 2);
        assert_eq!(*h.sleeps.borrow(), vec![CPU_SAMPLE_INTERVAL]);
    }

    #[test]
    fn the_wait_is_never_shorter_than_the_probe_can_measure() {
        let h = Harness::new();
        let long = CPU_SAMPLE_INTERVAL * 2;
        let mut s = CpuState::new(
            Scripted {
                figures: vec![0.0, 0.4],
                calls: 0,
            },
            long,
        );
        h.busy(&mut s);
        assert_eq!(*h.sleeps.borrow(), vec![long]);
    }

    #[test]
    fn a_call_right_after_returns_the_last_figure_without_sampling() {
        let h = Harness::new();
        let mut s = state(&[0.0, 0.4]);
        h.busy(&mut s);
        h.advance(MIN - Duration::from_millis(1));
        assert_eq!(h.busy(&mut s), 0.4);
        assert_eq!(s.probe.calls, 2);
        assert_eq!(h.sleeps.borrow().len(), 1);
    }

    #[test]
    fn reusing_does_not_move_the_start_of_the_interval() {
        let h = Harness::new();
        let mut s = state(&[0.0, 0.4, 0.7]);
        h.busy(&mut s);
        // Two calls 150 ms apart: the second is 300 ms after the sample, so
        // it measures even though it is only 150 ms after the reuse.
        h.advance(Duration::from_millis(150));
        assert_eq!(h.busy(&mut s), 0.4);
        h.advance(Duration::from_millis(150));
        assert_eq!(h.busy(&mut s), 0.7);
        assert_eq!(h.sleeps.borrow().len(), 1);
    }

    #[test]
    fn a_later_call_samples_once_without_waiting() {
        let h = Harness::new();
        let mut s = state(&[0.0, 0.4, 0.7, 0.2]);
        h.busy(&mut s);
        h.advance(MIN);
        assert_eq!(h.busy(&mut s), 0.7);
        h.advance(CPU_SAMPLE_MAX_AGE);
        assert_eq!(h.busy(&mut s), 0.2);
        assert_eq!(s.probe.calls, 4);
        assert_eq!(h.sleeps.borrow().len(), 1);
    }

    #[test]
    fn a_stale_sample_is_measured_again_from_scratch() {
        let h = Harness::new();
        let mut s = state(&[0.0, 0.4, 0.9, 0.1]);
        h.busy(&mut s);
        h.advance(CPU_SAMPLE_MAX_AGE + Duration::from_millis(1));
        // The first sample of the pair (0.9, the average over the stale
        // stretch) is thrown away.
        assert_eq!(h.busy(&mut s), 0.1);
        assert_eq!(s.probe.calls, 4);
        assert_eq!(
            *h.sleeps.borrow(),
            vec![CPU_SAMPLE_INTERVAL, CPU_SAMPLE_INTERVAL]
        );
    }

    #[test]
    fn concurrent_callers_share_one_measurement() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

        // The clock stands still, so every caller after the one that
        // measures finds a sample taken "just now" and reuses it. A probe
        // asked for a third figure would panic.
        let shared: Mutex<Option<CpuState<Scripted>>> = Mutex::new(None);
        let start = Instant::now();
        let slept_ms = AtomicU64::new(0);
        let sleeps = AtomicUsize::new(0);
        let inits = AtomicUsize::new(0);
        let figures: Vec<f64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        shared_busy(
                            &shared,
                            || {
                                inits.fetch_add(1, Ordering::SeqCst);
                                state(&[0.0, 0.4])
                            },
                            || start,
                            |d| {
                                sleeps.fetch_add(1, Ordering::SeqCst);
                                slept_ms.fetch_add(d.as_millis() as u64, Ordering::SeqCst);
                            },
                        )
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(figures, vec![0.4; 8]);
        assert_eq!(inits.load(Ordering::SeqCst), 1);
        assert_eq!(sleeps.load(Ordering::SeqCst), 1);
        assert_eq!(
            slept_ms.load(Ordering::SeqCst),
            CPU_SAMPLE_INTERVAL.as_millis() as u64
        );
        let guard = shared.lock().unwrap();
        assert_eq!(guard.as_ref().unwrap().probe.calls, 2);
    }

    #[test]
    fn a_caller_that_panicked_under_the_lock_does_not_stop_the_next() {
        let shared: Mutex<Option<CpuState<Scripted>>> = Mutex::new(None);
        let start = Instant::now();
        let panicked = std::thread::scope(|scope| {
            scope
                .spawn(|| shared_busy(&shared, || state(&[]), || start, |_| {}))
                .join()
                .is_err()
        });
        assert!(panicked);
        assert!(shared.is_poisoned());
        // The state made by the first caller is still there (no sample yet),
        // so this one measures from scratch on a refilled probe.
        shared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
            .unwrap()
            .probe
            .figures = vec![0.0, 0.3];
        let busy = shared_busy(&shared, || unreachable!(), || start, |_| {});
        assert_eq!(busy, 0.3);
    }
}
