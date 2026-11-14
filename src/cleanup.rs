//! Bounded, attempt-owned Docker control and cleanup. No global discovery or fallback.
use crate::context::ExecContext;
use anyhow::{bail, Context, Result};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

const MAX_OUTPUT: usize = 64 * 1024;

/// A cleanup failure is an environment diagnostic, not a failing test.
#[derive(Debug)]
pub(crate) struct CleanupIncomplete(pub(crate) String);

impl std::fmt::Display for CleanupIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cleanup incomplete: {}", self.0)
    }
}

// Keep the leader unreaped until its entire group has been signalled. Owning
// this unreaped Child pins its PID/start identity, including after it exits;
// there is no PID lookup/reuse window. Never signal a caller-provided group.
struct OwnedGroup(Child);
impl Drop for OwnedGroup {
    fn drop(&mut self) {
        let pid = self.0.id() as i32;
        if pid > 1 {
            // SAFETY: spawn created this group; the owned leader is not reaped.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
        let _ = self.0.wait();
    }
}

/// Control commands stop promptly when the attempt is cancelled. Cleanup
/// commands use the same runner but must still run after cancellation.
pub(crate) fn docker_control(
    ctx: &ExecContext,
    args: &[&str],
    timeout: Duration,
) -> Result<Vec<u8>> {
    docker_with_cancel(ctx, args, timeout, true)
}

fn docker(ctx: &ExecContext, args: &[&str], timeout: Duration) -> Result<Vec<u8>> {
    docker_with_cancel(ctx, args, timeout, false)
}

fn docker_with_cancel(
    ctx: &ExecContext,
    args: &[&str],
    timeout: Duration,
    stop_on_cancel: bool,
) -> Result<Vec<u8>> {
    if stop_on_cancel && ctx.cancel.is_cancelled() {
        bail!("Docker command cancelled");
    }
    let mut child = OwnedGroup(
        ctx.command("docker")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .context("start Docker command")?,
    );
    #[cfg(test)]
    tests::observe_spawn(&child.0, args, timeout);
    let mut stdout = child.0.stdout.take().context("missing stdout")?;
    let mut stderr = child.0.stderr.take().context("missing stderr")?;
    for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
        // SAFETY: both descriptors are owned pipes with independent read ends.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
                return Err(std::io::Error::last_os_error()).context("nonblocking Docker pipe");
            }
        }
    }
    let started = Instant::now();
    let mut output = Vec::new();
    let mut errors = Vec::new();
    let mut eof = [false; 2];
    loop {
        if stop_on_cancel && ctx.cancel.is_cancelled() {
            bail!("Docker command cancelled");
        }
        if started.elapsed() >= timeout {
            bail!("Docker command deadline exceeded");
        }
        for (i, (pipe, bytes)) in [
            (&mut stdout as &mut dyn Read, &mut output),
            (&mut stderr as &mut dyn Read, &mut errors),
        ]
        .into_iter()
        .enumerate()
        {
            let mut buf = [0; 4096];
            // One read per stream per tick: a continuous writer cannot starve
            // the other stream or the deadline check.
            match pipe.read(&mut buf) {
                Ok(0) => eof[i] = true,
                Ok(n) => {
                    if bytes.len() + n > MAX_OUTPUT {
                        bail!("Docker command output limit exceeded");
                    }
                    bytes.extend_from_slice(&buf[..n]);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => (),
                Err(e) => return Err(e).context("read Docker command output"),
            }
        }
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: inspect only our owned child without reaping it. Drop kills
        // descendants before wait releases the leader's PID identity.
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                child.0.id(),
                &mut status,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error()).context("inspect Docker child");
        }
        if unsafe { status.si_pid() } != 0 && eof.iter().all(|v| *v) {
            if status.si_code != libc::CLD_EXITED || unsafe { status.si_status() } != 0 {
                // Do not expose daemon stderr, which can contain credentials.
                bail!("Docker command exited unsuccessfully");
            }
            return Ok(output);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Only names from this attempt's tracked resource list are accepted. No
/// listing, stale-PID heuristic, or unfiltered fallback can broaden the scope.
pub(crate) fn remove_tracked_containers(
    ctx: &ExecContext,
    names: &[String],
) -> Vec<CleanupIncomplete> {
    remove_tracked_with_timeout(ctx, names, Duration::from_secs(30))
}

fn remove_tracked_with_timeout(
    ctx: &ExecContext,
    names: &[String],
    timeout: Duration,
) -> Vec<CleanupIncomplete> {
    if names.is_empty() {
        return vec![];
    }
    for name in names {
        if name.is_empty()
            || name.len() > 255
            || !name.as_bytes()[0].is_ascii_alphanumeric()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
        {
            return vec![CleanupIncomplete("invalid tracked container name".into())];
        }
    }
    let mut args = vec!["rm", "-f", "--"];
    args.extend(names.iter().map(String::as_str));
    match docker(ctx, &args, timeout) {
        Ok(_) => vec![],
        Err(error) => vec![CleanupIncomplete(format!(
            "remove tracked containers: {error:#}"
        ))],
    }
}

pub(crate) fn compose_down(ctx: &ExecContext, projects: &[String]) -> Vec<CleanupIncomplete> {
    compose_down_with_budgets(
        ctx,
        projects,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
}

#[cfg(test)]
fn compose_down_with_timeout(
    ctx: &ExecContext,
    projects: &[String],
    timeout: Duration,
) -> Vec<CleanupIncomplete> {
    compose_down_with_budgets(ctx, projects, timeout, timeout)
}

fn compose_down_with_budgets(
    ctx: &ExecContext,
    projects: &[String],
    query_timeout: Duration,
    action_timeout: Duration,
) -> Vec<CleanupIncomplete> {
    let mut failures = Vec::new();
    for (index, project) in projects.iter().enumerate() {
        let result = (|| -> Result<()> {
            // Compose's documented project grammar. Validate before building
            // argv or invoking Docker, even though projects come from our run.
            if project.is_empty()
                || project.len() > 255
                || !project.as_bytes()[0].is_ascii_lowercase()
                    && !project.as_bytes()[0].is_ascii_digit()
                || !project
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
            {
                bail!("invalid Compose project name");
            }
            let label = format!("label=com.docker.compose.project={project}");
            let bytes = docker(ctx, &["ps", "-aq", "--filter", &label], query_timeout)
                .context("query owned Compose containers")?;
            let ids = std::str::from_utf8(&bytes).context("invalid container ID encoding")?;
            for id in ids.lines() {
                if !matches!(id.len(), 12 | 64)
                    || !id
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                {
                    bail!("invalid container ID list");
                }
            }
            if !ids.is_empty() {
                docker(
                    ctx,
                    &["compose", "-p", project, "down", "--remove-orphans"],
                    action_timeout,
                )
                .context("take down owned Compose project")?;
            }
            Ok(())
        })();
        if let Err(e) = result {
            failures.push(CleanupIncomplete(format!("{e:#}")));
            // A broken/unavailable daemon must not cost one deadline per
            // project. Every skipped project remains explicitly incomplete.
            for _ in &projects[index + 1..] {
                failures.push(CleanupIncomplete(
                    "project not attempted after an earlier cleanup failure".into(),
                ));
            }
            break;
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    // Parent-owned receipts do not depend on the stub being scheduled before
    // a short deadline. Selected adversarial fixtures additionally establish
    // their causal state before the production runtime budget starts.
    struct SpawnReceipt {
        pid: i32,
        args: Vec<String>,
        started: Instant,
        timeout: Duration,
    }
    type SpawnObserver = Box<dyn FnMut(&Child, &[&str], Duration)>;
    std::thread_local! {
        static OBSERVER: std::cell::RefCell<Option<SpawnObserver>> = const { std::cell::RefCell::new(None) };
    }
    pub(super) fn observe_spawn(child: &Child, args: &[&str], timeout: Duration) {
        OBSERVER.with_borrow_mut(|observer| {
            if let Some(observer) = observer {
                observer(child, args, timeout);
            }
        });
    }
    fn observed<T>(
        ready: Option<(String, std::path::PathBuf, bool)>,
        run: impl FnOnce() -> T,
    ) -> (T, Vec<SpawnReceipt>) {
        let receipts = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let recorded = receipts.clone();
        OBSERVER.with_borrow_mut(|observer| {
            assert!(observer.is_none());
            *observer = Some(Box::new(move |child, args, timeout| {
                if let Some((phase, marker, wait_for_exit)) = &ready {
                    if args.first() == Some(&phase.as_str()) {
                        let setup = Instant::now();
                        while !marker.exists() {
                            assert!(
                                setup.elapsed() < Duration::from_secs(10),
                                "stub startup marker missing"
                            );
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    }
                    if args.first() != Some(&phase.as_str()) || *wait_for_exit {
                        // The prerequisite query must complete before testing
                        // the down phase's deadline, even on a busy scheduler.
                        let setup = Instant::now();
                        loop {
                            let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
                            let rc = unsafe {
                                libc::waitid(
                                    libc::P_PID,
                                    child.id(),
                                    &mut status,
                                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                                )
                            };
                            assert_eq!(rc, 0);
                            if unsafe { status.si_pid() } != 0 {
                                break;
                            }
                            assert!(
                                setup.elapsed() < Duration::from_secs(10),
                                "prerequisite stub did not exit"
                            );
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    }
                }
                recorded.borrow_mut().push(SpawnReceipt {
                    pid: child.id() as i32,
                    args: args.iter().map(|arg| (*arg).to_owned()).collect(),
                    started: Instant::now(),
                    timeout,
                });
            }));
        });
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                OBSERVER.with_borrow_mut(|observer| *observer = None);
            }
        }
        let reset = Reset;
        let result = run();
        drop(reset);
        (
            result,
            std::rc::Rc::try_unwrap(receipts).ok().unwrap().into_inner(),
        )
    }

    fn fixture(script: &str) -> (tempfile::TempDir, ExecContext) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("docker");
        std::fs::write(
            &file,
            format!("#!/bin/sh\necho \"$*\" >> \"$HOME/calls\"\n{script}\n"),
        )
        .unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        let ctx = ExecContext::new(
            [
                ("HOME".into(), dir.path().display().to_string()),
                (
                    "PATH".into(),
                    format!("{}:/usr/bin:/bin", dir.path().display()),
                ),
            ]
            .into_iter()
            .collect(),
        );
        (dir, ctx)
    }
    fn run(ctx: &ExecContext) -> Vec<CleanupIncomplete> {
        // Normal behavior is not a subprocess-startup benchmark. Explicit timeout
        // fixtures below keep their short deadline and reaping assertions.
        compose_down_with_timeout(ctx, &["owned-project".into()], Duration::from_secs(2))
    }
    fn calls(dir: &tempfile::TempDir) -> String {
        std::fs::read_to_string(dir.path().join("calls")).unwrap_or_default()
    }
    #[test]
    fn control_cancel_is_prompt_and_reaps_owned_child_but_cleanup_still_runs() {
        let (dir, ctx) = fixture("echo $$ > \"$HOME/pid\"; exec sleep 20");
        let cancel = ctx.cancel.clone();
        let ready = dir.path().join("pid");
        let signal = std::thread::spawn(move || {
            let start = Instant::now();
            while !ready.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "stub startup marker missing"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            let cancelled = Instant::now();
            cancel.cancel();
            cancelled
        });
        let (result, receipts) = observed(None, || {
            docker_control(&ctx, &["info"], Duration::from_secs(20))
        });
        let cancelled = signal.join().unwrap();
        assert!(result.unwrap_err().to_string().contains("cancelled"));
        assert!(cancelled.elapsed() < Duration::from_secs(3));
        assert_eq!(receipts.len(), 1);
        let pid = receipts[0].pid;
        assert_eq!(
            unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
        std::fs::write(
            dir.path().join("docker"),
            "#!/bin/sh\necho \"$*\" >> \"$HOME/calls\"\nexit 0\n",
        )
        .unwrap();
        assert!(docker_control(&ctx, &["logs"], Duration::from_millis(250)).is_err());
        assert!(!calls(&dir).lines().any(|line| line == "logs"));
        assert!(run(&ctx).is_empty());
        assert!(remove_tracked_with_timeout(
            &ctx,
            &["owned-container".into()],
            Duration::from_secs(2)
        )
        .is_empty());
        assert!(calls(&dir).contains("rm -f -- owned-container"));
    }

    #[test]
    fn tracked_removal_validates_scope_and_reports_blockage_and_failure() {
        let (dir, ctx) = fixture("exit 0");
        assert!(remove_tracked_with_timeout(&ctx, &[], Duration::from_millis(250)).is_empty());
        assert!(calls(&dir).is_empty());
        for name in ["", "--all", "one two", "a\n", &"a".repeat(256)] {
            assert!(
                !remove_tracked_with_timeout(&ctx, &[name.into()], Duration::from_millis(250))
                    .is_empty()
            );
        }
        assert!(calls(&dir).is_empty());
        assert!(remove_tracked_with_timeout(
            &ctx,
            &["one_1".into(), "two.2".into()],
            Duration::from_secs(2)
        )
        .is_empty());
        assert_eq!(calls(&dir), "rm -f -- one_1 two.2\n");
        for script in ["exec sleep 20", "echo secret-daemon-error >&2; exit 1"] {
            let (_dir, ctx) = fixture(script);
            let errors =
                remove_tracked_with_timeout(&ctx, &["owned".into()], Duration::from_millis(250));
            assert_eq!(errors.len(), 1);
            assert!(errors[0].to_string().contains("remove tracked"));
            assert!(!errors[0].to_string().contains("secret-daemon-error"));
        }
    }

    #[test]
    fn control_drains_both_pipes_and_keeps_daemon_errors_private() {
        let (_dir, ctx) = fixture("(dd if=/dev/zero bs=32768 count=1 2>/dev/null) >&2\n(dd if=/dev/zero bs=32768 count=1 2>/dev/null)");
        assert_eq!(
            docker_control(&ctx, &["logs"], Duration::from_secs(2))
                .unwrap()
                .len(),
            32768
        );
        let (_dir, ctx) = fixture("echo daemon-private >&2; echo partial; exit 1");
        let error = docker_control(&ctx, &["inspect"], Duration::from_secs(2)).unwrap_err();
        assert!(!format!("{error:#}").contains("daemon-private"));
        let (_dir, ctx) =
            fixture("while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' >&2; done");
        assert!(docker_control(&ctx, &["logs"], Duration::from_secs(2))
            .unwrap_err()
            .to_string()
            .contains("output limit"));
    }

    #[test]
    fn cancellation_handle_removes_only_tracked_names_even_after_cancel() {
        let (dir, ctx) = fixture("exit 0");
        ctx.cancel.track_container("owned-a".into());
        ctx.cancel.track_container("owned-b".into());
        ctx.cancel.cancel();
        ctx.cancel.remove_containers(&ctx);
        assert_eq!(calls(&dir), "rm -f -- owned-a owned-b\n");
        ctx.cancel.remove_containers(&ctx);
        assert_eq!(calls(&dir).lines().count(), 1);
    }

    #[test]
    fn cleanup_normal_and_empty() {
        for ids in [
            "",
            "012345abcdef",
            "012345abcdef\nabcdef012345",
            &"a".repeat(64),
        ] {
            let (dir, ctx) = fixture(&format!("[ \"$1\" = ps ] && printf '%s' '{ids}'\nexit 0"));
            let errors = run(&ctx);
            assert!(errors.is_empty(), "{errors:?}");
            let log = calls(&dir);
            assert!(
                log.starts_with("ps -aq --filter label=com.docker.compose.project=owned-project\n")
            );
            assert_eq!(
                log.contains("compose -p owned-project down --remove-orphans"),
                !ids.is_empty()
            );
        }
    }
    #[test]
    fn cleanup_refuses_bad_ids_partial_nonzero_and_excessive_output() {
        for script in [
            "printf '012345abcdef\n'; exit 1",
            "kill -TERM $$",
            "echo invalid",
            "echo 012345abcdeF",
            "printf '\\377'",
            "printf '012345abcdef\n\n'",
            "printf ' '; exit 0",
            "while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done",
            "while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' >&2; done",
        ] {
            let (dir, ctx) = fixture(script);
            assert!(!run(&ctx).is_empty(), "{script}");
            assert!(
                !calls(&dir).lines().any(|line| line.starts_with("compose ")),
                "{script}"
            );
        }
    }
    #[test]
    fn cleanup_refuses_invalid_projects_without_invoking_docker() {
        let (dir, ctx) = fixture("exit 0");
        for project in ["", "--help", "A", "a b", "a\n", &"a".repeat(256)] {
            assert!(!compose_down_with_timeout(
                &ctx,
                &[project.into()],
                Duration::from_millis(250)
            )
            .is_empty());
        }
        assert!(calls(&dir).is_empty());
    }
    #[test]
    fn cleanup_bounds_blocked_query_and_down_and_reaps_owned_leader() {
        for phase in ["ps", "compose"] {
            let (dir, ctx) = fixture(&format!(
                "if [ \"$1\" = {phase} ]; then sleep 0.35; echo $$ > \"$HOME/pid\"; exec sleep 20; fi\necho 012345abcdef"));
            let (errors, receipts) =
                observed(Some((phase.into(), dir.path().join("pid"), false)), || {
                    compose_down_with_timeout(
                        &ctx,
                        &["owned-project".into()],
                        Duration::from_millis(250),
                    )
                });
            assert_eq!(errors.len(), 1);
            assert!(errors[0].to_string().contains("deadline"));
            let blocked = receipts.last().unwrap();
            assert_eq!(blocked.args[0], phase);
            assert!(blocked.started.elapsed() < Duration::from_secs(3));
            if phase == "ps" {
                assert!(!calls(&dir).lines().any(|line| line.starts_with("compose ")));
            }
            let pid = blocked.pid;
            // waitpid is read-only here: ECHILD proves the helper already reaped it.
            assert_eq!(
                unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
                -1
            );
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::ECHILD)
            );
        }
    }
    #[test]
    fn cleanup_bounds_inherited_pipe_after_leader_exits() {
        let (dir, ctx) = fixture("sleep 0.35; sleep 20 &\necho $! > \"$HOME/plugin\"\nexit 0");
        let (errors, receipts) =
            observed(Some(("ps".into(), dir.path().join("plugin"), true)), || {
                compose_down_with_timeout(
                    &ctx,
                    &["owned-project".into()],
                    Duration::from_millis(250),
                )
            });
        assert!(!errors.is_empty());
        assert_eq!(receipts.len(), 1);
        assert!(receipts[0].started.elapsed() < Duration::from_secs(3));
        assert!(!calls(&dir).lines().any(|line| line.starts_with("compose ")));
        // The leader exits before the timeout, but stays unreaped until the
        // helper kills its owned process group, including its pipe holder.
        let pid = std::fs::read_to_string(dir.path().join("plugin")).unwrap();
        let status = ctx
            .command("ps")
            .args(["-p", pid.trim(), "-o", "stat="])
            .output()
            .unwrap();
        // An orphan can briefly remain as a zombie until the OS reaps it;
        // it must no longer be an executing pipe holder.
        assert!(
            !status.status.success()
                || String::from_utf8_lossy(&status.stdout)
                    .trim()
                    .starts_with('Z')
        );
    }
    #[test]
    fn cleanup_output_limit_is_inclusive_on_both_streams() {
        for redirect in ["", ">&2"] {
            for count in [MAX_OUTPUT, MAX_OUTPUT + 1] {
                let (_dir, ctx) = fixture(&format!(
                    "(dd if=/dev/zero bs={count} count=1 2>/dev/null) {redirect}"
                ));
                let result = docker(&ctx, &["ps"], Duration::from_secs(2));
                if count == MAX_OUTPUT {
                    let output = result.unwrap();
                    assert_eq!(output.len(), if redirect.is_empty() { count } else { 0 });
                } else {
                    assert!(result.unwrap_err().to_string().contains("output limit"));
                }
            }
        }
    }
    #[test]
    fn cleanup_selects_shorter_read_only_budget_and_preserves_action_budget() {
        let (_dir, ctx) = fixture("[ \"$1\" = ps ] && echo 012345abcdef; exit 0");
        let (errors, receipts) = observed(None, || compose_down(&ctx, &["owned-project".into()]));
        assert!(errors.is_empty());
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0].args[0], "ps");
        assert_eq!(receipts[0].timeout, Duration::from_secs(5));
        assert_eq!(receipts[1].args[0], "compose");
        assert_eq!(receipts[1].timeout, Duration::from_secs(30));
        let (errors, receipts) = observed(None, || run(&ctx));
        assert!(errors.is_empty());
        assert_eq!(receipts.len(), 2);
        assert!(receipts
            .iter()
            .all(|receipt| receipt.timeout == Duration::from_secs(2)));
    }

    #[test]
    fn cleanup_failure_marks_unattempted_projects_without_more_queries() {
        let (_dir, ctx) = fixture("exit 1");
        let (failures, receipts) = observed(None, || {
            compose_down_with_timeout(
                &ctx,
                &["one".into(), "two".into(), "three".into()],
                Duration::from_millis(250),
            )
        });
        assert_eq!(failures.len(), 3);
        assert!(failures[1].to_string().contains("not attempted"));
        assert!(failures[2].to_string().contains("not attempted"));
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].args[0], "ps");
    }
    #[test]
    fn cleanup_reaps_successful_owned_child() {
        let (dir, ctx) = fixture("echo $$ > \"$HOME/pid\"; exit 0");
        assert!(run(&ctx).is_empty());
        let pid = std::fs::read_to_string(dir.path().join("pid"))
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert_eq!(
            unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }
    #[test]
    fn cleanup_reports_failed_down_without_repeating_or_fallback() {
        let (dir, ctx) = fixture("[ \"$1\" = ps ] && { echo 012345abcdef; exit 0; }\nexit 2");
        let errors = run(&ctx);
        assert_eq!(errors.len(), 1);
        assert!(
            errors[0].to_string().contains("take down owned"),
            "{errors:?}"
        );
        assert_eq!(calls(&dir).lines().count(), 2);
    }
}
