//! The CLI's [`Supervisor`]: process-global signal handlers, kept out of
//! the library because they cannot be shared by concurrent runs.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use task_traffic_controller::Supervisor;

/// One bit per signal number received and not yet delivered to the child
/// or taken: no signal replaces another.
static PENDING: AtomicU64 = AtomicU64::new(0);
/// The running child, `(pid << 1) | leads_a_group`, or 0 for none: the
/// handler forwards to it directly, so a signal that lands just before the
/// blocking wait is not left for an EINTR that never comes.
static CHILD: AtomicI64 = AtomicI64::new(0);
/// Handlers between "about to read [`CHILD`]" and "`kill` returned".
/// Invariant: a pid is signalled only while unreaped. The handler can run
/// on any thread that does not block the signal, so it may be preempted
/// between loading [`CHILD`] and calling `kill`; `child_exited` therefore
/// clears [`CHILD`] and then waits for this to reach 0, and only after
/// that does the caller reap, so a recycled pid is never signalled.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Address of this thread's `errno`.
unsafe fn errno_location() -> *mut libc::c_int {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        libc::__error()
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    {
        libc::__errno_location()
    }
}

/// Takes the bit for `sig` out of [`PENDING`]; true only for the one caller
/// that cleared it, so a signal is delivered exactly once.
fn claim(sig: libc::c_int) -> bool {
    let bit = 1u64 << sig;
    PENDING.fetch_and(!bit, Ordering::SeqCst) & bit != 0
}

/// Async-signal-safe: atomics and `kill` only. Records the signal first,
/// then forwards it itself if a child is published. The runner publishes
/// the child and only then drains [`PENDING`]; with both sides sequentially
/// consistent, at least one of them sees the signal, and `claim` lets only
/// one deliver it.
extern "C" fn on_signal(sig: libc::c_int) {
    // The interrupted code may be between a failing call and its `errno`
    // check: leave `errno` as found.
    let saved_errno = unsafe { *errno_location() };
    forward_from_handler(sig);
    unsafe { *errno_location() = saved_errno };
}

fn forward_from_handler(sig: libc::c_int) {
    if !(1..64).contains(&sig) {
        return;
    }
    PENDING.fetch_or(1u64 << sig, Ordering::SeqCst);
    // Raised before CHILD is read, lowered after `kill` returns.
    IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    let child = CHILD.load(Ordering::SeqCst);
    let pid = child >> 1;
    // Never 0 (own group), 1 (everything) or negative.
    if pid > 1 && claim(sig) {
        unsafe {
            if child & 1 == 1 {
                libc::killpg(pid as libc::pid_t, sig);
            } else {
                libc::kill(pid as libc::pid_t, sig);
            }
        }
    }
    IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
}

/// Does nothing. Used instead of `SIG_IGN` for the terminal's SIGINT and
/// SIGQUIT while the command runs: an ignored disposition would survive
/// `exec` and leave the command unable to be interrupted, whereas a handler
/// is reset to the default by `exec`.
extern "C" fn ignore_in_wrapper(_sig: libc::c_int) {}

/// Signals that were `SIG_IGN` when the process started (one bit each),
/// read once before the first handler is installed.
static STARTED_IGNORED: AtomicU64 = AtomicU64::new(0);
static CAPTURE: std::sync::Once = std::sync::Once::new();

/// Shell convention: a non-interactive shell starts background jobs with
/// SIGINT/SIGQUIT ignored and `nohup` starts with SIGHUP ignored. What the
/// process started with is kept, so the wrapped command inherits it just as
/// a bare `cmd &` / `nohup cmd` would.
fn capture_initial_dispositions() {
    CAPTURE.call_once(|| {
        let mut bits = 0u64;
        for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGHUP, libc::SIGTERM] {
            unsafe {
                let mut old: libc::sigaction = std::mem::zeroed();
                if libc::sigaction(sig, std::ptr::null(), &mut old) == 0
                    && old.sa_sigaction == libc::SIG_IGN
                {
                    bits |= 1u64 << sig;
                }
            }
        }
        STARTED_IGNORED.store(bits, Ordering::SeqCst);
    });
}

fn started_ignored(sig: libc::c_int) -> bool {
    STARTED_IGNORED.load(Ordering::SeqCst) & (1u64 << sig) != 0
}

fn set_handler(sig: libc::c_int, handler: libc::sighandler_t) {
    // The first call here precedes every handler installation.
    capture_initial_dispositions();
    // Ignored at start-up: leave it ignored in every phase.
    if started_ignored(sig) {
        return;
    }
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = 0; // no SA_RESTART: wait4 must return EINTR
        libc::sigaction(sig, &sa, std::ptr::null_mut());
    }
}

fn record() -> libc::sighandler_t {
    on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t
}

/// Handlers for the one gated run this process performs.
pub struct ProcessSignals;

impl Supervisor for ProcessSignals {
    /// While waiting in the queue: SIGINT/SIGTERM/SIGHUP set a flag so the
    /// loop can remove its entry and stop.
    fn waiting(&self) {
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            set_handler(sig, record());
        }
    }

    fn running(&self) {
        // Terminal-originated signals reach the child directly (same
        // process group); a second copy from us would make test runners
        // force-quit. A no-op handler, not SIG_IGN, so the command does
        // not inherit an ignored SIGINT/SIGQUIT across `exec`.
        let noop = ignore_in_wrapper as extern "C" fn(libc::c_int) as libc::sighandler_t;
        set_handler(libc::SIGINT, noop);
        set_handler(libc::SIGQUIT, noop);
        for sig in [libc::SIGTERM, libc::SIGHUP] {
            set_handler(sig, record());
        }
        // An inherited SIG_IGN would auto-reap the child while its pid is
        // still published; the child also starts with the default.
        set_handler(libc::SIGCHLD, libc::SIG_DFL);
    }

    fn finished(&self) {
        for sig in [
            libc::SIGINT,
            libc::SIGQUIT,
            libc::SIGTERM,
            libc::SIGHUP,
            libc::SIGCHLD,
        ] {
            set_handler(sig, libc::SIG_DFL);
        }
    }

    fn take_pending(&self) -> i32 {
        loop {
            let bits = PENDING.load(Ordering::SeqCst);
            if bits == 0 {
                return 0;
            }
            let sig = bits.trailing_zeros() as i32;
            if claim(sig) {
                return sig;
            }
        }
    }

    fn child_started(&self, pid: u32, group: bool) {
        if pid > 1 {
            CHILD.store(((pid as i64) << 1) | i64::from(group), Ordering::SeqCst);
        }
    }

    fn child_exited(&self) {
        CHILD.store(0, Ordering::SeqCst);
        // A handler that loaded the old value may still be about to call
        // `kill`: do not return (and let the caller reap) until none is.
        while IN_FLIGHT.load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
            std::thread::yield_now();
        }
    }
}

/// The run is recorded: put the handled signals back to their defaults so
/// a SIGINT is not swallowed on the way to exit.
pub fn restore() {
    ProcessSignals.finished();
}

/// Terminate ourselves the way the child did, so the calling shell sees
/// the same signal death. Falls back to exiting 128+N. (The default is set
/// on purpose here even for a signal ignored at start-up: the child died of
/// it, so the child had reset it itself.)
pub fn reraise(sig: i32) -> ! {
    // Only now, with the outcome recorded, do the handled signals go back
    // to their defaults: earlier, a second SIGTERM would kill the process
    // before the exit file was written.
    ProcessSignals.finished();
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
    std::process::exit(128 + sig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::Command;
    use std::sync::Mutex;

    // The statics are process-wide: one test at a time.
    static SERIAL: Mutex<()> = Mutex::new(());

    fn drain() {
        while ProcessSignals.take_pending() != 0 {}
    }

    #[test]
    fn two_signals_before_they_are_taken_are_both_kept() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        drain();
        on_signal(libc::SIGTERM);
        on_signal(libc::SIGINT);
        on_signal(libc::SIGTERM);
        let mut got = vec![ProcessSignals.take_pending(), ProcessSignals.take_pending()];
        got.sort();
        assert_eq!(got, vec![libc::SIGINT, libc::SIGTERM]);
        assert_eq!(ProcessSignals.take_pending(), 0);
    }

    #[test]
    fn a_signal_reaches_a_published_child_without_any_wait_interrupt() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        drain();
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        ProcessSignals.child_started(child.id(), false);
        on_signal(libc::SIGTERM);
        let status = child.wait().unwrap();
        ProcessSignals.child_exited();
        assert_eq!(status.signal(), Some(libc::SIGTERM));
        // Forwarded by the handler, so nothing is left to take.
        assert_eq!(ProcessSignals.take_pending(), 0);
    }

    #[test]
    fn nothing_is_signalled_without_a_published_child() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        drain();
        for pid in [0u32, 1] {
            ProcessSignals.child_started(pid, true);
            assert_eq!(CHILD.load(Ordering::SeqCst), 0);
        }
        // Handler with no child only records.
        on_signal(libc::SIGHUP);
        assert_eq!(ProcessSignals.take_pending(), libc::SIGHUP);
    }

    #[test]
    fn child_exited_waits_for_handlers_in_flight() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        drain();
        ProcessSignals.child_started(std::process::id().max(2), false);
        IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let t = std::thread::spawn(move || {
            go_tx.send(()).unwrap();
            ProcessSignals.child_exited();
            done_tx.send(()).unwrap();
        });
        go_rx.recv().unwrap();
        // Published value is cleared first, so no new handler can use it.
        while CHILD.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
        assert!(
            done_rx.try_recv().is_err(),
            "returned with a handler in flight"
        );
        IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        done_rx.recv().unwrap();
        t.join().unwrap();
        assert_eq!(IN_FLIGHT.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn handler_keeps_errno() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        drain();
        unsafe { *errno_location() = libc::ENOENT };
        on_signal(libc::SIGHUP);
        assert_eq!(unsafe { *errno_location() }, libc::ENOENT);
        drain();
    }
}
