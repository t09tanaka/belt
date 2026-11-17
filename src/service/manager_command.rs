//! Bounded command execution shared by the native service managers.
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const OUTPUT_LIMIT: usize = 64 * 1024;
pub struct CommandOutput {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}
pub trait CommandRunner {
    /// Implementations bound elapsed time and both output streams. No output
    /// may be logged: a foreign job's argv/environment may contain secrets.
    fn run(&mut self, arguments: &[String]) -> Result<CommandOutput, String>;
}

/// Run one service-manager command with a cleared environment (plus `env`),
/// no stdin and bounded time and output. The child is the process spawned
/// here, never an inferred PID.
pub(crate) fn run_bounded(
    program: &str,
    arguments: &[String],
    env: &[(String, String)],
    deadline: Duration,
) -> Result<CommandOutput, String> {
    let mut child = Command::new(program)
        .args(arguments)
        .env_clear()
        .envs(env.iter().map(|(k, v)| (k, v)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| "service manager spawn failed")?;
    let result = (|| {
        let mut stdout = child.stdout.take().ok_or("stdout unavailable")?;
        let mut stderr = child.stderr.take().ok_or("stderr unavailable")?;
        for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
            // Only this invocation's pipes; never a foreign process or fd.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return Err("pipe setup failed".into());
            }
        }
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut out_done = false;
        let mut err_done = false;
        let deadline = Instant::now() + deadline;
        loop {
            out_done |= drain(&mut stdout, &mut out)?;
            err_done |= drain(&mut stderr, &mut err)?;
            if let Some(status) = child
                .try_wait()
                .map_err(|_| "service manager wait failed")?
            {
                if out_done && err_done {
                    return Ok(CommandOutput {
                        success: status.success(),
                        exit_code: status.code(),
                        stdout: out,
                        stderr: err,
                    });
                }
            }
            if Instant::now() >= deadline {
                return Err("service manager deadline exceeded".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    })();
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result
}
fn drain(reader: &mut impl Read, output: &mut Vec<u8>) -> Result<bool, String> {
    let mut buf = [0; 4096];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => return Ok(true),
            Ok(n) => {
                if output.len() + n > OUTPUT_LIMIT {
                    return Err("service manager output limit exceeded".into());
                }
                output.extend_from_slice(&buf[..n]);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(_) => return Err("service manager output read failed".into()),
        }
    }
}
