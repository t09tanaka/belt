//! Bounded launchctl transport. Captured output is never forwarded to logs.
use super::{LaunchError, Launcher, LoadedState, Plan, Receipt, RunningIdentity};
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const OUTPUT_LIMIT: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);
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
#[derive(Default)]
pub struct NativeRunner;
impl CommandRunner for NativeRunner {
    fn run(&mut self, arguments: &[String]) -> Result<CommandOutput, String> {
        let mut child = Command::new("/bin/launchctl")
            .args(arguments)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| "launchctl spawn failed")?;
        let result = (|| {
            let mut stdout = child.stdout.take().ok_or("stdout unavailable")?;
            let mut stderr = child.stderr.take().ok_or("stderr unavailable")?;
            for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
                // Only this invocation's pipes; never a foreign process or fd.
                let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
                if flags < 0
                    || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
                {
                    return Err("pipe setup failed".into());
                }
            }
            let mut out = Vec::new();
            let mut err = Vec::new();
            let mut out_done = false;
            let mut err_done = false;
            let deadline = Instant::now() + DEADLINE;
            loop {
                out_done |= drain(&mut stdout, &mut out)?;
                err_done |= drain(&mut stderr, &mut err)?;
                if let Some(status) = child.try_wait().map_err(|_| "launchctl wait failed")? {
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
                    return Err("launchctl deadline exceeded".into());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        })();
        if result.is_err() {
            // This Child is the process we just spawned, never an inferred PID.
            let _ = child.kill();
            let _ = child.wait();
        }
        result
    }
}
fn drain(reader: &mut impl Read, output: &mut Vec<u8>) -> Result<bool, String> {
    let mut buf = [0; 4096];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => return Ok(true),
            Ok(n) => {
                if output.len() + n > OUTPUT_LIMIT {
                    return Err("launchctl output limit exceeded".into());
                }
                output.extend_from_slice(&buf[..n]);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(_) => return Err("launchctl output read failed".into()),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub name: String,
    pub uid: u32,
}
pub trait AccountLookup {
    fn lookup(&mut self, username: &str) -> Result<Option<Account>, String>;
}
#[derive(Default)]
pub struct NativeAccounts;
impl AccountLookup for NativeAccounts {
    fn lookup(&mut self, username: &str) -> Result<Option<Account>, String> {
        let name = std::ffi::CString::new(username).map_err(|_| "invalid account name")?;
        let mut bytes = vec![0; 64 * 1024];
        let mut record = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        // getpwnam_r owns no process or service state. All pointed-to data is
        // copied before its caller-owned backing buffer goes out of scope.
        let code = unsafe {
            libc::getpwnam_r(
                name.as_ptr(),
                record.as_mut_ptr(),
                bytes.as_mut_ptr(),
                bytes.len(),
                &mut result,
            )
        };
        if code != 0 {
            return Err("account lookup failed".into());
        }
        if result.is_null() {
            return Ok(None);
        }
        let record = unsafe { record.assume_init() };
        let name = unsafe { std::ffi::CStr::from_ptr(record.pw_name) }
            .to_str()
            .map_err(|_| "account encoding")?
            .to_owned();
        Ok(Some(Account {
            name,
            uid: record.pw_uid,
        }))
    }
}
pub struct NativeLauncher<R = NativeRunner, A = NativeAccounts> {
    runner: R,
    accounts: A,
}
impl Default for NativeLauncher {
    fn default() -> Self {
        Self::new(NativeRunner)
    }
}
impl<R: CommandRunner> NativeLauncher<R> {
    pub fn new(runner: R) -> Self {
        Self::with_accounts(runner, NativeAccounts)
    }
}
impl<R: CommandRunner, A: AccountLookup> NativeLauncher<R, A> {
    pub fn with_accounts(runner: R, accounts: A) -> Self {
        Self { runner, accounts }
    }
}
impl<R: CommandRunner, A: AccountLookup> NativeLauncher<R, A> {
    fn verify_output(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
        bytes: &[u8],
    ) -> Result<(), LaunchError> {
        let observed =
            super::observe::parse(bytes).map_err(|why| LaunchError::NotApplied(why.into()))?;
        let account = observed
            .username()
            .map(|name| self.accounts.lookup(name))
            .transpose()
            .map_err(|_| LaunchError::NotApplied("loaded account unconfirmed".into()))?
            .flatten();
        observed
            .verify(
                plan,
                receipt.path(),
                account.as_ref().map(|a| (a.name.as_str(), a.uid)),
            )
            .map_err(|why| LaunchError::NotApplied(why.into()))
    }
}
impl<R: CommandRunner, A: AccountLookup> Launcher for NativeLauncher<R, A> {
    fn running_identity(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
    ) -> Result<RunningIdentity, LaunchError> {
        let mut original = None;
        for _ in 0..2 {
            let output = self
                .runner
                .run(&["print".into(), plan.service_target()])
                .map_err(|_| LaunchError::NotApplied("running-job query failed".into()))?;
            if !output.success || !output.stderr.is_empty() || output.stdout.len() > OUTPUT_LIMIT {
                return Err(LaunchError::NotApplied(
                    "running-job query unconfirmed".into(),
                ));
            }
            self.verify_output(plan, receipt, &output.stdout)?;
            let pid = super::observe::parse(&output.stdout)
                .and_then(|o| o.running_pid())
                .map_err(|why| LaunchError::NotApplied(why.into()))?;
            let identity = RunningIdentity::observe(pid).map_err(|_| {
                LaunchError::NotApplied("running process identity unconfirmed".into())
            })?;
            if let Some(previous) = &original {
                if previous != &identity {
                    return Err(LaunchError::NotApplied("running service changed".into()));
                }
            }
            original = Some(identity);
        }
        let identity = original.expect("two observations");
        identity
            .verify_live()
            .map_err(|_| LaunchError::NotApplied("running service stopped or changed".into()))?;
        Ok(identity)
    }
    fn loaded_state(&mut self, plan: &Plan, receipt: &Receipt) -> LoadedState {
        let Ok(output) = self.runner.run(&["print".into(), plan.service_target()]) else {
            return LoadedState::Unknown;
        };
        let domain = match plan.mode() {
            super::policy::Mode::User { uid } => format!("user gui: {uid}"),
            super::policy::Mode::SystemUser { .. } => "system".into(),
        };
        let absent = format!(
            "Bad request.\nCould not find service \"{}\" in domain for {domain}\n",
            plan.label()
        );
        if !output.success
            && output.exit_code == Some(113)
            && output.stdout.is_empty()
            && output.stderr == absent.as_bytes()
        {
            return LoadedState::Absent;
        }
        if !output.success || !output.stderr.is_empty() || output.stdout.len() > OUTPUT_LIMIT {
            return LoadedState::Unknown;
        }
        if self.verify_output(plan, receipt, &output.stdout).is_ok() {
            LoadedState::Match
        } else {
            LoadedState::Unknown
        }
    }
    fn verify_loaded(&mut self, plan: &Plan, receipt: &Receipt) -> Result<(), LaunchError> {
        let output = self
            .runner
            .run(&["print".into(), plan.service_target()])
            .map_err(|_| LaunchError::NotApplied("loaded-job query failed".into()))?;
        if !output.success || !output.stderr.is_empty() || output.stdout.len() > OUTPUT_LIMIT {
            return Err(LaunchError::NotApplied(
                "loaded-job query unconfirmed".into(),
            ));
        }
        self.verify_output(plan, receipt, &output.stdout)
    }
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError> {
        let output = self
            .runner
            .run(arguments)
            .map_err(LaunchError::OutcomeUnknown)?;
        if !output.success
            || output.stdout.len() > OUTPUT_LIMIT
            || output.stderr.len() > OUTPUT_LIMIT
        {
            return Err(LaunchError::OutcomeUnknown(
                "launchctl did not confirm success".into(),
            ));
        }
        Ok(())
    }
}
