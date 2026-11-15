//! Private subprocess rendezvous, independent of libtest's stdout formatting.
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const READY_ENV: &str = "RTR_TEST_CHILD_READY";
const READY: &[u8] = b"ready\n";
const TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn publish_ready() -> io::Result<()> {
    let path =
        PathBuf::from(std::env::var_os(READY_ENV).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing child rendezvous")
        })?);
    let pending = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)?;
    file.write_all(READY)?;
    drop(file);
    std::fs::rename(pending, path)
}

pub(crate) struct OwnedChild {
    child: Child,
    ready: PathBuf,
    // The child has been stopped and reaped before this directory is dropped.
    _directory: tempfile::TempDir,
}

impl OwnedChild {
    fn start(mut command: Command) -> io::Result<Self> {
        let directory = crate::test_tmp::prefixed("rtr-test-child-")?;
        let ready = directory.path().join("ready");
        let child = command
            .env(READY_ENV, &ready)
            .stdout(Stdio::null())
            .spawn()?;
        Ok(Self {
            child,
            ready,
            _directory: directory,
        })
    }

    pub(crate) fn spawn(command: Command) -> io::Result<Self> {
        let mut child = Self::start(command)?;
        child.wait_ready(TIMEOUT)?;
        Ok(child)
    }

    fn wait_ready(&mut self, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("child ended before acquiring fixture ownership: {status}"),
                ));
            }
            match File::open(&self.ready) {
                Ok(file) => {
                    let mut bytes = Vec::new();
                    file.take(READY.len() as u64 + 1).read_to_end(&mut bytes)?;
                    return if bytes == READY {
                        Ok(())
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid child receipt",
                        ))
                    };
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "child readiness timed out",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub(crate) fn release(&mut self) -> io::Result<()> {
        self.child
            .stdin
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "missing child release pipe"))?
            .write_all(b"x")
    }

    pub(crate) fn kill(&mut self) -> io::Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        Ok(())
    }

    fn wait_for(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "child completion timed out",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        self.wait_for(TIMEOUT)
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Never signal an already reaped PID, including an unexpected ECHILD.
        // Other wait errors leave our Child identity unreaped (e.g. EINTR).
        // These private helpers do not spawn descendants.
        let needs_kill = match self.child.try_wait() {
            Ok(None) => true,
            Ok(Some(_)) => false,
            Err(error) => error.raw_os_error() != Some(libc::ECHILD),
        };
        if needs_kill {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(mode: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "test_child::tests::fixture_child",
                "--ignored",
                "--nocapture",
            ])
            .env("RTR_TEST_CHILD_MODE", mode)
            .stdin(Stdio::piped());
        command
    }

    fn reaped(pid: u32) {
        let mut status = 0;
        // Read-only ownership check: a live or zombie child would return 0/PID.
        assert_eq!(
            unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[test]
    #[ignore = "owned subprocess helper"]
    fn fixture_child() {
        let mode = std::env::var("RTR_TEST_CHILD_MODE").unwrap();
        if mode == "exit" {
            return;
        }
        if mode == "ready" {
            publish_ready().unwrap();
        } else {
            let path = PathBuf::from(std::env::var_os(READY_ENV).unwrap());
            std::fs::write(path.with_extension("started"), b"started").unwrap();
        }
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).unwrap();
    }

    #[test]
    fn unique_receipts_and_real_completion() {
        let mut a = OwnedChild::spawn(command("ready")).unwrap();
        let mut b = OwnedChild::spawn(command("ready")).unwrap();
        assert_ne!(a.ready, b.ready);
        for child in [&mut a, &mut b] {
            let pid = child.child.id();
            child.release().unwrap();
            assert!(child.wait().unwrap().success());
            reaped(pid);
        }
    }

    #[test]
    fn premature_exit_and_missing_receipt_do_not_leave_children() {
        let mut exited = OwnedChild::start(command("exit")).unwrap();
        let pid = exited.child.id();
        assert_eq!(
            exited.wait_ready(TIMEOUT).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        drop(exited);
        reaped(pid);

        let mut silent = OwnedChild::start(command("silent")).unwrap();
        let pid = silent.child.id();
        let deadline = Instant::now() + TIMEOUT;
        while !silent.ready.with_extension("started").exists() {
            assert!(silent.child.try_wait().unwrap().is_none());
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            silent
                .wait_ready(Duration::from_millis(50))
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        drop(silent);
        reaped(pid);
    }

    #[test]
    fn completion_timeout_and_panic_reap_the_owned_child() {
        let mut child = OwnedChild::spawn(command("ready")).unwrap();
        let pid = child.child.id();
        assert_eq!(
            child
                .wait_for(Duration::from_millis(50))
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        drop(child);
        reaped(pid);

        let child = OwnedChild::spawn(command("ready")).unwrap();
        let pid = child.child.id();
        assert!(std::panic::catch_unwind(move || {
            let _child = child;
            panic!("exercise fixture unwind");
        })
        .is_err());
        reaped(pid);
    }
}
