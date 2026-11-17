//! Native step ownership. A direct child exit is not a wrapper drain proof.
use crate::context::CancelHandle;
use std::cell::RefCell;
use std::io;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// One matrix entry/recipe owns completed step groups until all its scripts
/// finish, so a background server remains usable by subsequent steps.
#[derive(Default)]
pub(super) struct NativeScope(RefCell<Vec<NativeStep>>);
impl NativeScope {
    fn retain(&self, mut step: NativeStep) {
        step.stop_writer();
        self.0.borrow_mut().push(step);
    }
}

pub(super) struct NativeStep {
    child: Option<Child>,
    cancel: CancelHandle,
    stop: Arc<AtomicBool>,
    writer: Option<JoinHandle<()>>,
    external_wrapper: bool,
    registered: bool,
}
impl NativeStep {
    pub(super) fn new(child: Child, cancel: CancelHandle, external_wrapper: bool) -> Self {
        let mut this = Self {
            child: Some(child),
            cancel,
            stop: Arc::new(AtomicBool::new(false)),
            writer: None,
            external_wrapper,
            registered: false,
        };
        if this.observe().is_ok() {
            this.cancel.track_pgid(this.pid());
            this.registered = true;
        } else {
            this.cancel.record_native_uncertain(this.pid() as u32);
        }
        // Cancellation before registration must not miss this new child.
        if this.cancel.is_cancelled() {
            this.signal(libc::SIGTERM);
        }
        this
    }
    pub(super) fn pid(&self) -> i32 {
        self.child.as_ref().unwrap().id() as i32
    }
    pub(super) fn writer_stop(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }
    pub(super) fn set_writer(&mut self, writer: JoinHandle<()>) {
        self.writer = Some(writer);
    }

    /// WNOWAIT preserves the leader's kernel identity until signal/untrack.
    pub(super) fn observe(&self) -> io::Result<Option<ExitStatus>> {
        observe(self.pid())
    }
    pub(super) fn signal(&self, signal: i32) {
        // A failed ownership check never authorizes a signal to a recycled PID.
        if self.registered && self.observe().is_ok() {
            unsafe {
                libc::killpg(self.pid(), signal);
            }
        }
    }
    pub(super) fn finish(self, scope: &NativeScope) {
        scope.retain(self);
    }
    fn stop_writer(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(writer) = self.writer.take() {
            if writer.join().is_err() {
                self.cancel.record_native_uncertain(self.pid() as u32);
            }
        }
    }
    fn drain(&mut self) {
        self.stop_writer();
        let Some(mut child) = self.child.take() else {
            return;
        };
        let pid = child.id() as i32;
        let identity = observe(pid);
        if identity.is_ok() {
            // The unreaped leader pins the original process-group identity.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        } else {
            self.cancel.record_native_uncertain(pid as u32);
        }
        // cancel() and untrack share a mutex: no registered stale PGID survives wait.
        if self.registered {
            self.cancel.untrack_pgid(pid);
        }
        if identity.is_ok() {
            if child.wait().is_err() {
                self.cancel.record_native_uncertain(pid as u32);
            } else {
                // Signal 0 only observes. If an old member or a newly reused
                // group remains, conservatively retain uncertainty; never kill it.
                let exists = unsafe { libc::killpg(pid, 0) };
                if exists == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
                    self.cancel.record_native_uncertain(pid as u32);
                }
            }
        }
        if self.external_wrapper {
            // TTC may own grandchildren in another PGID. Its direct child
            // returning, even normally, does not authenticate their termination.
            self.cancel.record_native_uncertain(pid as u32);
        }
    }
}
impl Drop for NativeStep {
    fn drop(&mut self) {
        self.drain();
    }
}

fn observe(pid: i32) -> io::Result<Option<ExitStatus>> {
    loop {
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if unsafe { info.si_pid() } == 0 {
            return Ok(None);
        }
        let raw = match info.si_code {
            libc::CLD_EXITED => (unsafe { info.si_status() }) << 8,
            libc::CLD_KILLED => unsafe { info.si_status() },
            libc::CLD_DUMPED => (unsafe { info.si_status() }) | 0x80,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected native child state",
                ))
            }
        };
        return Ok(Some(ExitStatus::from_raw(raw)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ExecContext;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::{ChildStdout, Command, Stdio};
    use std::time::{Duration, Instant};

    fn command(program: impl AsRef<std::ffi::OsStr>) -> Command {
        ExecContext::new([("PATH".to_string(), "/usr/bin:/bin".to_string())].into())
            .command(program)
    }
    fn spawn(script: &str) -> (Child, ChildStdout) {
        let mut child = command("/bin/sh")
            .args(["-c", script])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let fd = output.as_raw_fd();
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            assert!(flags >= 0);
            assert_eq!(libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK), 0);
        }
        (child, output)
    }
    fn exited(step: &NativeStep) -> ExitStatus {
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = step.observe().unwrap() {
                return status;
            }
            assert!(Instant::now() < until, "owned child did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn byte(output: &mut ChildStdout) -> Option<u8> {
        let mut b = [0];
        match output.read(&mut b) {
            Ok(0) => None,
            Ok(_) => Some(b[0]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Some(0),
            Err(error) => panic!("owned pipe read: {error}"),
        }
    }
    fn await_byte(output: &mut ChildStdout, expected: u8) {
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            if byte(output) == Some(expected) {
                return;
            }
            assert!(
                Instant::now() < until,
                "owned child did not reach handshake"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn eof(output: &mut ChildStdout) {
        let until = Instant::now() + Duration::from_secs(3);
        while byte(output).is_some() {
            assert!(Instant::now() < until, "owned process still holds pipe");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    #[test]
    fn native_step_preserves_exit_until_owned_scope_drains() {
        let cancel = CancelHandle::new();
        let (child, mut output) = spawn("printf a; exit 7");
        let step = NativeStep::new(child, cancel.clone(), false);
        assert_eq!(exited(&step).code(), Some(7));
        let scope = NativeScope::default();
        step.finish(&scope);
        assert_eq!(cancel.native_group_count(), 1);
        drop(scope);
        assert_eq!(cancel.native_group_count(), 0);
        assert!(cancel.native_uncertain().is_empty());
        eof(&mut output);
    }
    #[test]
    fn native_background_survives_step_and_only_its_scope_is_stopped() {
        let root = tempfile::tempdir().unwrap();
        let proceed = root.path().join("proceed");
        let mut cmd = command("/bin/sh");
        cmd.args(["-c", "(printf r; while [ ! -f \"$PROCEED\" ]; do sleep 0.01; done; printf u; while :; do sleep 1; done) & exit 0"])
            .env_clear().env("PATH", "/usr/bin:/bin").env("PROCEED", &proceed)
            .stdout(Stdio::piped()).stderr(Stdio::null()).stdin(Stdio::null()).process_group(0);
        let mut child = cmd.spawn().unwrap();
        let mut output = child.stdout.take().unwrap();
        unsafe {
            let fd = output.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFL);
            assert_eq!(libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK), 0);
        }
        let cancel = CancelHandle::new();
        let step = NativeStep::new(child, cancel.clone(), false);
        await_byte(&mut output, b'r');
        assert!(exited(&step).success());
        let scope = NativeScope::default();
        step.finish(&scope);
        std::fs::write(proceed, b"continue").unwrap();
        await_byte(&mut output, b'u');
        let (other, mut other_output) = spawn("printf o; while :; do sleep 1; done");
        let other_cancel = CancelHandle::new();
        let other_step = NativeStep::new(other, other_cancel.clone(), false);
        await_byte(&mut other_output, b'o');
        drop(scope);
        eof(&mut output);
        assert!(other_step.observe().unwrap().is_none());
        assert_eq!(other_cancel.native_group_count(), 1);
        drop(other_step);
        eof(&mut other_output);
        assert_eq!(cancel.native_group_count(), 0);
        assert_eq!(other_cancel.native_group_count(), 0);
    }
    #[test]
    fn native_error_and_unwind_stop_writer_and_owned_group() {
        for unwind in [false, true] {
            let (child, mut output) = spawn("printf r; while :; do sleep 1; done");
            let cancel = CancelHandle::new();
            let joined = Arc::new(AtomicBool::new(false));
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> io::Result<()> {
                    let mut step = NativeStep::new(child, cancel.clone(), false);
                    await_byte(&mut output, b'r');
                    let stop = step.writer_stop();
                    let joined = joined.clone();
                    step.set_writer(std::thread::spawn(move || {
                        while !stop.load(Ordering::SeqCst) {
                            std::thread::yield_now();
                        }
                        joined.store(true, Ordering::SeqCst);
                    }));
                    if unwind {
                        panic!("injected post-spawn unwind");
                    }
                    Err(io::Error::other("injected post-spawn error"))
                }));
            assert!(result.is_err() || result.unwrap().is_err());
            assert!(joined.load(Ordering::SeqCst));
            assert_eq!(cancel.native_group_count(), 0);
            eof(&mut output);
        }
    }
    #[test]
    fn native_cancel_before_registration_and_external_wrapper_uncertainty() {
        let cancel = CancelHandle::new();
        cancel.cancel();
        let (child, mut output) = spawn("while :; do sleep 1; done");
        let step = NativeStep::new(child, cancel.clone(), false);
        assert!(!exited(&step).success());
        drop(step);
        eof(&mut output);
        assert_eq!(cancel.native_group_count(), 0);
        let wrapper = CancelHandle::new();
        let (child, mut output) = spawn("exit 0");
        let step = NativeStep::new(child, wrapper.clone(), true);
        assert!(exited(&step).success());
        drop(step);
        eof(&mut output);
        assert_eq!(wrapper.native_uncertain().len(), 1);
    }
    #[test]
    fn native_already_reaped_identity_is_never_registered_for_signalling() {
        let (mut child, mut output) = spawn("exit 0");
        child.wait().unwrap();
        let cancel = CancelHandle::new();
        let step = NativeStep::new(child, cancel.clone(), false);
        assert_eq!(cancel.native_group_count(), 0);
        assert_eq!(
            step.observe().unwrap_err().raw_os_error(),
            Some(libc::ECHILD)
        );
        drop(step);
        cancel.cancel();
        eof(&mut output);
        assert!(!cancel.native_uncertain().is_empty());
    }
    #[test]
    #[ignore = "explicit private installed TTC wrapper acceptance"]
    // This explicitly invoked acceptance fixture selects an installed binary;
    // production commands and their environments still use ExecContext.
    fn native_real_ttc_wrapper_keeps_external_stop_unconfirmed() {
        // read when the test is built: the library does not read the process environment
        let Some(bin) = option_env!("SDK_TEST_TTC_BIN") else {
            panic!("set installed TTC binary explicitly")
        };
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join(".config/task-traffic-controller");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::write(
            config.join("config.toml"),
            "budget = 2\ncpu_threshold_percent = 100.0\nmemory_reserve_mib = 0\n",
        )
        .unwrap();
        let child = command(bin)
            .args([
                "exec",
                "--key",
                "owned-native-fixture",
                "--",
                "/bin/sh",
                "-c",
                "exit 0",
            ])
            .env_clear()
            .env("HOME", root.path())
            .env("PATH", "/usr/bin:/bin")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let cancel = CancelHandle::new();
        let step = NativeStep::new(child, cancel.clone(), true);
        assert!(exited(&step).success());
        let scope = NativeScope::default();
        step.finish(&scope);
        drop(scope);
        assert_eq!(cancel.native_group_count(), 0);
        assert_eq!(cancel.native_uncertain().len(), 1);
    }
}
