//! `belt exec` on a host where no resident runs (direct execution, spec
//! chapter 4 and 16): the command runs here, in the caller's directory and
//! environment, behind the embedded task-traffic-controller core with the
//! ledger, scope and policy the standalone `task-traffic-controller exec`
//! uses. No snapshot, run or attempt is made.

use crate::error::{ApiError, ErrorCode};
use crate::platform;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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
///
/// Known limit: a resident started by launchd or systemd holds neither lock
/// nor socket between its process start and the moment it opens the
/// coordinator store (`open_bound`), so a probe in that interval says the
/// resident is stopped. The daemon start takes no lock of its own to close
/// it, because `bootstrap::ensure` already holds `startup.lock` while it
/// starts the daemon.
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

/// One bit per signal number received and not yet delivered to the command or
/// taken by the runner: no signal replaces another.
static PENDING: AtomicU64 = AtomicU64::new(0);

/// The running command's pid, or 0 for none. The handler forwards to it
/// directly, so a signal that lands just before the runner blocks in its wait
/// is not left for an interruption that never comes.
static CHILD: AtomicU32 = AtomicU32::new(0);

/// Handlers between "about to read [`CHILD`]" and "the signal was sent".
/// Invariant: a pid is signalled only while unreaped. A handler can run on any
/// thread that does not block the signal, so it may be preempted between
/// loading [`CHILD`] and signalling; `child_exited` therefore clears [`CHILD`]
/// and then waits for this to reach 0, and the runner reaps only after that,
/// so a recycled pid is never signalled.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// SIGQUIT while the command runs: the command has it from the terminal.
extern "C" fn on_terminal(_: libc::c_int) {}

/// Address of this thread's `errno`.
unsafe fn errno_location() -> *mut libc::c_int {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    {
        // SAFETY: returns the calling thread's errno slot.
        unsafe { libc::__error() }
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    {
        // SAFETY: returns the calling thread's errno slot.
        unsafe { libc::__errno_location() }
    }
}

/// Takes the bit for `sig` out of [`PENDING`]; true only for the one caller
/// that cleared it, so a signal is delivered exactly once.
fn claim(sig: libc::c_int) -> bool {
    let bit = 1u64 << sig;
    PENDING.fetch_and(!bit, Ordering::SeqCst) & bit != 0
}

/// Async-signal-safe: atomics and `platform::signal_pid` (one `kill`) only.
/// Records the signal first, then forwards it itself if a command is
/// published. The runner publishes the command and only then drains
/// [`PENDING`]; with both sides sequentially consistent at least one of them
/// sees the signal, and `claim` lets only one of them deliver it. `errno` is
/// left as found: the interrupted code may be between a failing call and its
/// check.
extern "C" fn on_signal(sig: libc::c_int) {
    // SAFETY: reads and restores this thread's errno slot.
    let saved_errno = unsafe { *errno_location() };
    record_and_forward(sig);
    // SAFETY: as above.
    unsafe { *errno_location() = saved_errno };
}

fn record_and_forward(sig: libc::c_int) {
    if !(1..64).contains(&sig) {
        return;
    }
    PENDING.fetch_or(1u64 << sig, Ordering::SeqCst);
    // Raised before CHILD is read, lowered after the signal is sent.
    IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
    let pid = CHILD.load(Ordering::SeqCst);
    // `signal_pid` refuses pid 0, 1 and anything that wraps negative.
    if pid != 0 && claim(sig) {
        platform::signal_pid(pid, sig);
    }
    IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
}

/// The descriptor of belt's controlling terminal, or -1 when it has none.
static TTY: AtomicI32 = AtomicI32::new(-1);

/// Find the controlling terminal once, before any handler is installed:
/// stdin when it is a terminal, else `/dev/tty`. The descriptor is kept for
/// the life of the process so the handler can query it.
fn find_tty() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(find_tty_once);
}

fn find_tty_once() {
    use std::os::fd::IntoRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    // SAFETY: isatty only inspects a descriptor.
    let fd = if unsafe { libc::isatty(libc::STDIN_FILENO) } == 1 {
        libc::STDIN_FILENO
    } else {
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOCTTY | libc::O_CLOEXEC)
            .open("/dev/tty")
            .map_or(-1, IntoRawFd::into_raw_fd)
    };
    TTY.store(fd, Ordering::SeqCst);
}

/// Whether belt forwards `sig` to the command. The terminal sends Ctrl-C's
/// SIGINT to the whole foreground group, belt and the command both, and many
/// programs treat a second SIGINT as "force quit" and skip their cleanup, so
/// a SIGINT is not forwarded when belt's group is the terminal's foreground
/// group (the command already has it). SIGTERM and SIGHUP are always forwarded.
fn forwards(sig: libc::c_int, terminal_foreground: bool) -> bool {
    sig != libc::SIGINT || !terminal_foreground
}

/// The handler while the command runs: [`on_signal`] minus a SIGINT the
/// terminal already delivered to the command. A SIGINT sent with `kill` to
/// belt alone while belt is in the terminal's foreground group is therefore
/// not forwarded either; the terminal case is the one that matters here.
extern "C" fn on_running_signal(sig: libc::c_int) {
    let fd = TTY.load(Ordering::SeqCst);
    // SAFETY: tcgetpgrp and getpgrp are async-signal-safe and touch no memory.
    let foreground = fd >= 0 && unsafe { libc::tcgetpgrp(fd) == libc::getpgrp() };
    running_signal(sig, foreground);
}

fn running_signal(sig: libc::c_int, terminal_foreground: bool) {
    if forwards(sig, terminal_foreground) {
        on_signal(sig);
    }
}

fn set_handler(sig: libc::c_int, handler: libc::sighandler_t) {
    // SAFETY: installs an async-signal-safe handler.
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
/// ends, and the command is never started. Running, each signal is forwarded
/// to the command's pid alone (never a group), exactly once: pending signals
/// are a bitmask, so two different signals close together are both kept, and
/// the runner publishes the command's pid (`child_started`) so the handler
/// forwards at once, even when the signal lands just before the runner blocks
/// in its wait. `child_exited` runs before the command is reaped and waits
/// for handlers in flight, so a reaped pid is never signalled. A terminal's
/// Ctrl-C already reaches the command, so it is not recorded or forwarded
/// again (see [`on_running_signal`]).
///
/// Handlers only touch atomics and send one signal (SIGQUIT: nothing at all)
/// rather than ignoring the signal: an ignored signal stays ignored in the
/// command started after it, while a handler is reset by `exec`.
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
        find_tty();
        set_handler(
            libc::SIGQUIT,
            on_terminal as extern "C" fn(libc::c_int) as libc::sighandler_t,
        );
        for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
            set_handler(
                sig,
                on_running_signal as extern "C" fn(libc::c_int) as libc::sighandler_t,
            );
        }
        // An inherited SIG_IGN would auto-reap the command while its pid is
        // still published.
        set_handler(libc::SIGCHLD, libc::SIG_DFL);
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

    /// belt signals the command alone. The runner passes `group` only for a
    /// managed command, which belt's direct exec never is; if it ever did,
    /// nothing is published, so the handler never signals a group (the runner
    /// itself still would, in its drain).
    fn child_started(&self, pid: u32, group: bool) {
        if pid > 1 && !group {
            CHILD.store(pid, Ordering::SeqCst);
        }
    }

    fn child_exited(&self) {
        CHILD.store(0, Ordering::SeqCst);
        // A handler that loaded the old value may still be about to signal:
        // do not return (and let the runner reap) until none is.
        while IN_FLIGHT.load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The statics are process-wide: one test at a time.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        while Signals.take_pending() != 0 {}
        Signals.child_exited();
        guard
    }

    fn spawn_sleep() -> std::process::Child {
        use std::os::unix::process::CommandExt;
        let mut cmd = std::process::Command::new("sleep");
        cmd.arg("30");
        // SAFETY: signal() is async-signal-safe. A parent started in the
        // background hands SIGINT down as ignored, which would hide the signal.
        unsafe {
            cmd.pre_exec(|| {
                libc::signal(libc::SIGINT, libc::SIG_DFL);
                Ok(())
            });
        }
        cmd.spawn().unwrap()
    }

    #[test]
    fn two_different_signals_before_they_are_taken_are_both_kept() {
        let _g = serial();
        on_signal(libc::SIGTERM);
        on_signal(libc::SIGINT);
        on_signal(libc::SIGTERM);
        assert_eq!(Signals.take_pending(), libc::SIGINT);
        assert_eq!(Signals.take_pending(), libc::SIGTERM);
        assert_eq!(Signals.take_pending(), 0);
    }

    #[test]
    fn the_handler_reaches_a_published_child_without_a_wait_interrupt() {
        use std::os::unix::process::ExitStatusExt;
        let _g = serial();
        let mut child = spawn_sleep();
        Signals.child_started(child.id(), false);
        on_signal(libc::SIGTERM);
        let status = child.wait().unwrap();
        Signals.child_exited();
        assert_eq!(status.signal(), Some(libc::SIGTERM));
        // Delivered by the handler, so the drain has nothing left to send.
        assert_eq!(Signals.take_pending(), 0);
    }

    #[test]
    fn nothing_is_signalled_without_a_published_child_or_for_pid_0_or_1_or_a_group() {
        let _g = serial();
        for (pid, group) in [(0u32, false), (1, false), (std::process::id(), true)] {
            Signals.child_started(pid, group);
            assert_eq!(CHILD.load(Ordering::SeqCst), 0, "pid {pid} group {group}");
        }
        // No child: the handler only records, and the signal stays for the runner.
        on_signal(libc::SIGHUP);
        assert_eq!(Signals.take_pending(), libc::SIGHUP);
        assert_eq!(Signals.take_pending(), 0);
    }

    #[test]
    fn child_exited_waits_for_handlers_in_flight() {
        let _g = serial();
        // A pid that is never signalled here: no handler runs in this test.
        Signals.child_started(std::process::id(), false);
        IN_FLIGHT.fetch_add(1, Ordering::SeqCst);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let t = std::thread::spawn(move || {
            go_tx.send(()).unwrap();
            Signals.child_exited();
            done_tx.send(()).unwrap();
        });
        go_rx.recv().unwrap();
        // The published pid is cleared first, so no new handler can use it.
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
    fn a_sigint_with_the_terminal_foreground_is_neither_recorded_nor_delivered() {
        use std::os::unix::process::ExitStatusExt;
        let _g = serial();
        let mut child = spawn_sleep();
        Signals.child_started(child.id(), false);
        running_signal(libc::SIGINT, true);
        assert_eq!(Signals.take_pending(), 0);
        assert!(
            child.try_wait().unwrap().is_none(),
            "the child was signalled"
        );
        // SIGTERM is forwarded in the foreground; SIGINT is when it is not.
        running_signal(libc::SIGTERM, true);
        assert_eq!(child.wait().unwrap().signal(), Some(libc::SIGTERM));
        Signals.child_exited();
        let mut other = spawn_sleep();
        Signals.child_started(other.id(), false);
        running_signal(libc::SIGINT, false);
        assert_eq!(other.wait().unwrap().signal(), Some(libc::SIGINT));
        Signals.child_exited();
    }

    #[test]
    fn the_handler_keeps_errno() {
        let _g = serial();
        // SAFETY: this thread's errno slot.
        unsafe { *errno_location() = libc::ENOENT };
        on_signal(libc::SIGHUP);
        // SAFETY: as above.
        assert_eq!(unsafe { *errno_location() }, libc::ENOENT);
        assert_eq!(Signals.take_pending(), libc::SIGHUP);
    }

    #[test]
    fn a_signal_number_outside_the_mask_is_ignored() {
        let _g = serial();
        on_signal(0);
        on_signal(64);
        assert_eq!(Signals.take_pending(), 0);
    }

    #[test]
    fn sigint_is_not_forwarded_only_while_the_terminal_has_it_in_the_foreground() {
        assert!(!forwards(libc::SIGINT, true));
        assert!(forwards(libc::SIGINT, false));
        for sig in [libc::SIGTERM, libc::SIGHUP] {
            assert!(forwards(sig, true));
            assert!(forwards(sig, false));
        }
    }

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
