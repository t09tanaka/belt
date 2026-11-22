//! The CLI's [`Supervisor`]: process-global signal handlers, kept out of
//! the library because they cannot be shared by concurrent runs.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use task_traffic_controller::Supervisor;

/// One bit per signal number received and not yet delivered to the child
/// or taken: no signal replaces another.
static PENDING: AtomicU64 = AtomicU64::new(0);
/// The running child, `(pid << 1) | leads_a_group`, or 0 for none: the
/// handler forwards to it directly, so a signal that lands just before the
/// blocking wait is not left for an EINTR that never comes.
static CHILD: AtomicI64 = AtomicI64::new(0);

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
    if !(1..64).contains(&sig) {
        return;
    }
    PENDING.fetch_or(1u64 << sig, Ordering::SeqCst);
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
}

fn set_handler(sig: libc::c_int, handler: libc::sighandler_t) {
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
        // force-quit.
        set_handler(libc::SIGINT, libc::SIG_IGN);
        set_handler(libc::SIGQUIT, libc::SIG_IGN);
        for sig in [libc::SIGTERM, libc::SIGHUP] {
            set_handler(sig, record());
        }
    }

    fn finished(&self) {
        for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGTERM, libc::SIGHUP] {
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
    }
}

/// The run is recorded: put the handled signals back to their defaults so
/// a SIGINT is not swallowed on the way to exit.
pub fn restore() {
    ProcessSignals.finished();
}

/// Terminate ourselves the way the child did, so the calling shell sees
/// the same signal death. Falls back to exiting 128+N.
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
}
