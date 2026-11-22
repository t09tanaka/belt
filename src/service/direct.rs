//! `belt exec` on a host where no resident runs (direct execution, spec
//! chapter 4 and 16): the command runs here, in the caller's directory and
//! environment, behind the embedded task-traffic-controller core with the
//! ledger, scope and policy the standalone `task-traffic-controller exec`
//! uses. No snapshot, run or attempt is made.

use crate::error::{ApiError, ErrorCode};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use task_traffic_controller::config::Config;
use task_traffic_controller::exec::{self, Gated, Hooks, NestedMode, Nesting, Opts};
use task_traffic_controller::runner::{self, Exit};
use task_traffic_controller::{Context, Paths, Supervisor};

/// The one line on stderr that says where the command ran.
pub const NOTICE: &str =
    "belt: no resident is running on this host; the command runs here (direct execution, no snapshot and no run)";

/// How long a resident has to answer the probe.
const PROBE: Duration = Duration::from_secs(5);

/// The standalone CLI's override of how a nested `exec` is admitted.
const NESTED_ENV: &str = "TASK_TRAFFIC_CONTROLLER_NESTED";
/// The standalone CLI's override of the ledger lock timeout (tests).
const LEDGER_LOCK_TIMEOUT_ENV: &str = "TASK_TRAFFIC_CONTROLLER_LEDGER_LOCK_TIMEOUT_MS";
/// The standalone CLI's name for a run when `--label` is not given.
const LABEL_ENV: &str = "TASK_TRAFFIC_CONTROLLER_LABEL";

#[derive(Debug, PartialEq, Eq)]
pub enum Resident {
    Uninitialized,
    Stopped,
    Running,
}

/// Direct execution requires both local service endpoints to be absent.
pub fn resident_status(env: &BTreeMap<String, String>) -> Result<Resident, ApiError> {
    let unknown = |reason: String| ApiError::new(ErrorCode::CommunicationUnknown, reason);
    let spec = match super::context::runtime_spec(env) {
        Ok(spec) => Some(spec),
        Err(error) if error.code == ErrorCode::NotFound => None,
        Err(error) => return Err(error),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| unknown(e.to_string()))?;
    runtime.block_on(async {
        if let Some(spec) = &spec {
            let socket = spec.state_dir.join("parent/request.sock");
            if listening(&socket).await? {
                let client =
                    super::bootstrap::load_client(spec).map_err(|e| unknown(e.to_string()))?;
                return match tokio::time::timeout(
                    PROBE,
                    client.request(super::resident::Operation::Ping),
                )
                .await
                {
                    Ok(Ok(super::resident::Reply::Ready { .. })) => Ok(Resident::Running),
                    _ => Err(unknown("the coordinator did not confirm readiness".into())),
                };
            }
            // No answer on the socket is not "no resident": one that is still
            // starting has not bound it yet, and one that hangs does not
            // answer. A resident holds `startup.lock` while it is being
            // started and `coordinator.lock` for as long as it lives (the
            // system drops both when the process dies), so a stale socket
            // file left by a dead one is not mistaken for one.
            for name in OWNERSHIP_LOCKS {
                if lock_held(&spec.state_dir, name)? {
                    return Err(unknown(format!(
                        "the coordinator is starting or not answering ({name} is held in {}); the command was not run",
                        spec.state_dir.display()
                    )));
                }
            }
            if listening(&spec.worker_state_dir.join("agent.sock")).await? {
                return Err(unknown(
                    "a worker agent is running; start the coordinator".into(),
                ));
            }
        }
        let home = env
            .get("HOME")
            .ok_or_else(|| unknown("HOME is unavailable".into()))?;
        if listening(&PathBuf::from(home).join(".belt/agent.sock")).await? {
            return Err(unknown(
                "a worker agent is running; start the coordinator".into(),
            ));
        }
        Ok(if spec.is_some() {
            Resident::Stopped
        } else {
            Resident::Uninitialized
        })
    })
}

/// The files a resident locks (`flock`) while it starts and while it lives.
const OWNERSHIP_LOCKS: [&str; 2] = ["startup.lock", "coordinator.lock"];

/// Whether another process holds the lock `name` in `dir`. Read-only: a file
/// that does not exist is not held and is never created, and the lock taken
/// to find out (shared, so concurrent probes do not see each other) is
/// released at once.
fn lock_held(dir: &std::path::Path, name: &str) -> Result<bool, ApiError> {
    use std::fs::{File, OpenOptions, TryLockError};
    use std::os::unix::fs::OpenOptionsExt;
    let path = dir.join(name);
    let file: File = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => {
            return Err(ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!("cannot confirm that {} is free: {e}", path.display()),
            ))
        }
    };
    match file.try_lock_shared() {
        Ok(()) => {
            let _ = file.unlock();
            Ok(false)
        }
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(e)) => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            format!("cannot confirm that {} is free: {e}", path.display()),
        )),
    }
}

async fn listening(path: &std::path::Path) -> Result<bool, ApiError> {
    use std::io::ErrorKind;
    match tokio::time::timeout(PROBE, tokio::net::UnixStream::connect(path)).await {
        Ok(Ok(_)) => Ok(true),
        Ok(Err(e)) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused) => {
            Ok(false)
        }
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            format!("cannot confirm service at {}", path.display()),
        )),
    }
}

/// What `exec` was given besides the command, as direct execution sees it.
#[derive(Debug, Default, Clone, Copy)]
pub struct Flags {
    pub snapshot: bool,
    pub git_ref: bool,
    pub in_place: bool,
    pub profile: bool,
    pub requires: bool,
}

/// The refusal of an option that needs a run, or `None`. `--local` is not
/// here: direct execution is already local.
pub fn refusal(flags: Flags) -> Option<ApiError> {
    let option = if flags.snapshot {
        "--snapshot"
    } else if flags.git_ref {
        "--ref"
    } else if flags.in_place {
        "--in-place"
    } else if flags.profile {
        "--profile"
    } else if flags.requires {
        "--requires"
    } else {
        return None;
    };
    Some(not_running(&format!(
        "{option} needs a run, and no resident is running on this host"
    )))
}

/// The refusal of something that needs a resident, with how to start one.
pub fn not_running(what: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Blocked,
        format!(
            "{what}; start the resident first (`belt init` registers it as a service), or drop the option to run the command here"
        ),
    )
}

/// How a direct run ended, for `--json`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Outcome {
    pub mode: &'static str,
    pub exit_code: i32,
    /// The signal that ended the command, or that stopped it while it waited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// False when it was stopped before it was admitted.
    pub started: bool,
}

impl Outcome {
    fn of(gated: Gated) -> Self {
        let (exit_code, signal, started) = match gated {
            Gated::Finished(Exit::Code(code)) => (code, None, true),
            Gated::Finished(Exit::Signal(sig)) => (128 + sig, Some(sig), true),
            Gated::Interrupted(sig) => (128 + sig, Some(sig), false),
        };
        Outcome {
            mode: "direct",
            exit_code,
            signal,
            started,
        }
    }
}

/// Keep stdout available for the final JSON document. The core inherits the
/// process descriptors, so redirect command output for the duration of admission.
struct JsonOutput(std::os::fd::OwnedFd);

impl JsonOutput {
    fn redirect() -> std::io::Result<Self> {
        use std::os::fd::AsFd;
        let stdout = std::io::stdout();
        let saved = stdout.as_fd().try_clone_to_owned()?;
        // Both standard descriptors are borrowed. dup2 replaces stdout atomically.
        if unsafe { libc::dup2(libc::STDERR_FILENO, libc::STDOUT_FILENO) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self(saved))
    }
}

impl Drop for JsonOutput {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        // The saved descriptor remains owned until after stdout is restored.
        unsafe { libc::dup2(self.0.as_raw_fd(), libc::STDOUT_FILENO) };
    }
}

/// Run `argv` here. Prints [`NOTICE`] first; the exit code is the command's
/// (128 + N for a signal).
pub fn run(
    env: &BTreeMap<String, String>,
    cwd: PathBuf,
    argv: Vec<String>,
    json: bool,
) -> Result<Outcome, ApiError> {
    let _redirect = json
        .then(JsonOutput::redirect)
        .transpose()
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    eprintln!("{NOTICE}");
    let scope =
        crate::config::traffic::resolve(env).map_err(|e| ApiError::new(ErrorCode::Blocked, e))?;
    let mut ctx = Context::new(Paths::new(&scope.state_dir, &scope.config_file))
        .with_supervisor(Arc::new(Signals))
        .with_tmp_roots(env.get("TMPDIR").cloned().into_iter().collect());
    if let Some(ms) = env
        .get(LEDGER_LOCK_TIMEOUT_ENV)
        .and_then(|v| v.parse().ok())
    {
        ctx = ctx.with_ledger_lock_timeout(Duration::from_millis(ms));
    }
    let var = |name: &str| env.get(name).filter(|v| !v.is_empty()).map(String::as_str);
    let result = (|| {
        let cfg = Config::load(ctx.paths.config_file())?;
        let mut opts = Opts {
            label: var(LABEL_ENV).map(str::to_string),
            argv,
            cwd,
            ..Opts::default()
        };
        let mode = match var(NESTED_ENV) {
            None => NestedMode::default(),
            Some(text) => NestedMode::parse(text).ok_or_else(|| {
                task_traffic_controller::error::Error::Spawn(format!(
                    "{NESTED_ENV} is `admit` or `inherit`, not {text:?}"
                ))
            })?,
        };
        match exec::nesting(
            &ctx,
            mode,
            var("TASK_TRAFFIC_CONTROLLER_LEASE"),
            var(task_traffic_controller::marker::ENV_VAR),
            var(task_traffic_controller::marker::SCOPE_VAR),
        )? {
            Nesting::Inherit(lease) => {
                let out = runner::run(&ctx, &opts.argv, &opts.cwd, &lease, false)?;
                return Ok(Gated::Finished(out.exit));
            }
            Nesting::Child(parent) => opts.parent = Some(parent),
            Nesting::Top => {}
        }
        exec::run_gated(
            &ctx,
            &cfg,
            &opts,
            &exec::new_id(),
            &Hooks {
                cancelled: &|| false,
                progress: &|line| eprintln!("{line}"),
                progress_tty: std::io::IsTerminal::is_terminal(&std::io::stderr()),
            },
        )
    })();
    Signals.finished();
    match result {
        Ok(gated) => Ok(Outcome::of(gated)),
        Err(task_traffic_controller::error::Error::Spawn(e)) => {
            Err(ApiError::new(ErrorCode::InvalidInput, e))
        }
        Err(e) => Err(ApiError::new(
            ErrorCode::Blocked,
            format!(
                "task-traffic-controller in {}: {e}; nothing was changed, `belt doctor` shows the scope",
                scope.state_dir.display()
            ),
        )),
    }
}

static PENDING: AtomicI32 = AtomicI32::new(0);

/// SIGQUIT while the command runs: the command has it from the terminal.
extern "C" fn on_terminal(_: libc::c_int) {}

extern "C" fn on_signal(sig: libc::c_int) {
    PENDING.store(sig, Ordering::SeqCst);
}

fn set_handler(sig: libc::c_int, handler: libc::sighandler_t) {
    // SAFETY: installs a handler that only stores to an atomic.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = handler;
        libc::sigemptyset(&mut action.sa_mask);
        // No SA_RESTART: `wait4` must return EINTR so a signal is forwarded.
        action.sa_flags = 0;
        libc::sigaction(sig, &action, std::ptr::null_mut());
    }
}

/// Signal handling for the one run of this process. The command stays in
/// belt's own process group, so a terminal's Ctrl-C and job control reach it
/// as if it ran without belt; belt does not create a group and does not
/// signal one. Grandchildren are the command's own responsibility, the same
/// as running the command without belt.
///
/// SIGINT, SIGTERM and SIGHUP that belt itself receives are recorded. Waiting
/// for admission, the run is stopped and its entry removed before the process
/// ends, and the command is never started. Running, the runner forwards the
/// signal to the command's pid alone (never a group), once per signal belt
/// received. A terminal's Ctrl-C reaches belt and the command both, so the
/// command then gets SIGINT twice, once from the terminal and once forwarded.
///
/// Handlers do nothing but store (SIGQUIT: nothing at all) rather than
/// ignoring the signal: an ignored signal stays ignored in the command
/// started after it, while a handler is reset by `exec`.
struct Signals;

impl Supervisor for Signals {
    fn waiting(&self) {
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            set_handler(
                sig,
                on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t,
            );
        }
    }

    fn running(&self) {
        set_handler(
            libc::SIGQUIT,
            on_terminal as extern "C" fn(libc::c_int) as libc::sighandler_t,
        );
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            set_handler(
                sig,
                on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t,
            );
        }
    }

    fn finished(&self) {
        for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGTERM, libc::SIGHUP] {
            set_handler(sig, libc::SIG_DFL);
        }
    }

    fn take_pending(&self) -> i32 {
        PENDING.swap(0, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_that_need_a_run_are_refused_and_local_is_not() {
        assert!(refusal(Flags::default()).is_none());
        for (flags, option) in [
            (
                Flags {
                    snapshot: true,
                    ..Flags::default()
                },
                "--snapshot",
            ),
            (
                Flags {
                    git_ref: true,
                    ..Flags::default()
                },
                "--ref",
            ),
            (
                Flags {
                    in_place: true,
                    ..Flags::default()
                },
                "--in-place",
            ),
            (
                Flags {
                    profile: true,
                    ..Flags::default()
                },
                "--profile",
            ),
            (
                Flags {
                    requires: true,
                    ..Flags::default()
                },
                "--requires",
            ),
        ] {
            let error = refusal(flags).unwrap();
            assert_eq!(error.code, ErrorCode::Blocked);
            assert!(error.message.starts_with(option), "{}", error.message);
            assert!(error.message.contains("belt init"), "{}", error.message);
        }
    }

    #[test]
    fn an_outcome_keeps_the_exit_code_and_says_128_plus_a_signal() {
        assert_eq!(
            Outcome::of(Gated::Finished(Exit::Code(23))),
            Outcome {
                mode: "direct",
                exit_code: 23,
                signal: None,
                started: true
            }
        );
        let killed = Outcome::of(Gated::Finished(Exit::Signal(15)));
        assert_eq!(
            (killed.exit_code, killed.signal, killed.started),
            (143, Some(15), true)
        );
        let stopped = Outcome::of(Gated::Interrupted(2));
        assert_eq!(
            (stopped.exit_code, stopped.signal, stopped.started),
            (130, Some(2), false)
        );
        let json = serde_json::to_value(Outcome::of(Gated::Finished(Exit::Code(0)))).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"mode":"direct","exit_code":0,"started":true})
        );
    }

    #[test]
    fn an_uninitialized_host_has_no_resident() {
        let home = crate::test_tmp::tempdir().unwrap();
        let env = BTreeMap::from([("HOME".to_string(), home.path().display().to_string())]);
        assert_eq!(resident_status(&env).unwrap(), Resident::Uninitialized);
        assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
    }
}
