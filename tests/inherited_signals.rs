//! The wrapper handles the terminal's SIGINT/SIGQUIT itself while the
//! command runs, but the command must still start with the default
//! dispositions: an ignored signal survives `exec`.

use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};

fn run(dir: &std::path::Path, script: &str) -> (std::process::ExitStatus, String) {
    let o = Command::new(env!("CARGO_BIN_EXE_task-traffic-controller"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", dir)
        .env("XDG_STATE_HOME", dir.join("state"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .current_dir(dir)
        .stdin(Stdio::null())
        .args(["exec", "--", "sh", "-c", script])
        .output()
        .unwrap();
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
