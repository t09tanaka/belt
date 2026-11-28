//! The wrapper handles the terminal's SIGINT/SIGQUIT itself while the
//! command runs, but the command must still start with the default
//! dispositions: an ignored signal survives `exec`.

use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};

fn wrapper(dir: &std::path::Path, script: &str) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_task-traffic-controller"));
    c.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", dir)
        .env("XDG_STATE_HOME", dir.join("state"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .current_dir(dir)
        .stdin(Stdio::null())
        .args(["exec", "--", "sh", "-c", script]);
    // Default dispositions whatever launched the tests; the ignoring tests
    // override this afterwards.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut c, || {
            for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGHUP, libc::SIGTERM] {
                libc::signal(sig, libc::SIG_DFL);
            }
            Ok(())
        });
    }
    c
}

fn run(dir: &std::path::Path, script: &str) -> (std::process::ExitStatus, String) {
    let o = wrapper(dir, script).output().unwrap();
    (o.status, String::from_utf8_lossy(&o.stdout).into_owned())
}

/// Like [`run`], but the wrapper itself starts with `sig` ignored, as a
/// background job of a non-interactive shell (SIGINT/SIGQUIT) or `nohup`
/// (SIGHUP) would.
fn run_started_ignoring(
    dir: &std::path::Path,
    script: &str,
    sig: libc::c_int,
) -> (std::process::ExitStatus, String) {
    use std::os::unix::process::CommandExt;
    let mut c = wrapper(dir, script);
    unsafe {
        c.pre_exec(move || {
            libc::signal(sig, libc::SIG_IGN);
            Ok(())
        });
    }
    let o = c.output().unwrap();
    (o.status, String::from_utf8_lossy(&o.stdout).into_owned())
}

/// The shell signals itself (its own pid, which it owns): with the default
/// disposition it dies before printing; with an inherited SIG_IGN it would
/// carry on.
#[test]
fn the_command_starts_with_sigint_at_its_default() {
    let dir = tempfile::tempdir().unwrap();
    let (status, out) = run(dir.path(), "kill -INT $$; echo reached");
    assert!(!out.contains("reached"), "SIGINT was ignored: {out:?}");
    assert!(
        status.signal() == Some(libc::SIGINT) || status.code() == Some(128 + libc::SIGINT),
        "{status:?}"
    );
}

#[test]
fn the_command_starts_with_sigquit_at_its_default() {
    let dir = tempfile::tempdir().unwrap();
    // No core file for the deliberate SIGQUIT.
    let (status, out) = run(dir.path(), "ulimit -c 0; kill -QUIT $$; echo reached");
    assert!(!out.contains("reached"), "SIGQUIT was ignored: {out:?}");
    assert!(
        status.signal() == Some(libc::SIGQUIT) || status.code() == Some(128 + libc::SIGQUIT),
        "{status:?}"
    );
}

#[test]
fn a_sigint_ignored_at_start_stays_ignored_for_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let (status, out) =
        run_started_ignoring(dir.path(), "kill -INT $$; echo reached", libc::SIGINT);
    assert!(out.contains("reached"), "{status:?} {out:?}");
    assert_eq!(status.code(), Some(0), "{status:?}");
}

#[test]
fn a_sigquit_ignored_at_start_stays_ignored_for_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let (status, out) = run_started_ignoring(
        dir.path(),
        "ulimit -c 0; kill -QUIT $$; echo reached",
        libc::SIGQUIT,
    );
    assert!(out.contains("reached"), "{status:?} {out:?}");
}

#[test]
fn a_sighup_ignored_at_start_stays_ignored_for_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let (status, out) =
        run_started_ignoring(dir.path(), "kill -HUP $$; echo reached", libc::SIGHUP);
    assert!(out.contains("reached"), "{status:?} {out:?}");
    assert_eq!(status.code(), Some(0), "{status:?}");
}

/// Regression guard: the no-op handler must not become the default
/// disposition, or a SIGINT sent to the wrapper alone would kill it while
/// the command is still running.
#[test]
fn the_wrapper_survives_a_sigint_while_the_command_runs() {
    use std::io::{Read, Write};
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    let mkfifo = |name: &str| {
        let path = dir.path().join(name);
        let c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
        path
    };
    let ready = mkfifo("ready");
    let go = mkfifo("go");
    let script = format!(
        "echo up > '{}'; read _x < '{}'; exit 7",
        ready.display(),
        go.display()
    );
    let mut child = wrapper(dir.path(), &script).spawn().unwrap();
    // Blocks until the command has started and wrote to the pipe.
    let mut buf = String::new();
    std::fs::File::open(&ready)
        .unwrap()
        .read_to_string(&mut buf)
        .unwrap();
    assert_eq!(buf.trim(), "up");
    // The wrapper's own pid: a process this test spawned.
    let pid = child.id() as libc::pid_t;
    assert!(pid > 1);
    assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0);
    // Release the command; blocks until it is reading.
    let mut g = std::fs::OpenOptions::new().write(true).open(&go).unwrap();
    g.write_all(b"x\n").unwrap();
    drop(g);
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(7), "{status:?}");
}
