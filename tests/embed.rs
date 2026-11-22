//! A program that embeds the `fast-worktree` command with
//! `cli::run_with_helper`: the background work re-execs the helper it
//! names, not the running binary (this test binary). On Linux an embedding
//! binary started through a link named `fast-worktree` would otherwise
//! re-exec itself under its real name and fail.
//!
//! One test in its own binary: it sets the process environment and cwd.
#![cfg(feature = "cli")]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn background_work_re_execs_the_given_helper() {
    let t = tempfile::tempdir().unwrap();
    let base = t.path().canonicalize().unwrap();
    std::env::set_var("FAST_WORKTREE_CACHE", base.join("cache"));
    std::env::set_var("HOME", base.join("home"));
    std::env::set_var("XDG_CONFIG_HOME", base.join("xdg"));
    std::env::set_var("GIT_CONFIG_NOSYSTEM", "1");
    std::env::set_var("GIT_CONFIG_GLOBAL", "/dev/null");
    for (k, v) in [
        ("GIT_AUTHOR_NAME", "t"),
        ("GIT_AUTHOR_EMAIL", "t@example.com"),
        ("GIT_COMMITTER_NAME", "t"),
        ("GIT_COMMITTER_EMAIL", "t@example.com"),
    ] {
        std::env::set_var(k, v);
    }
    fs::create_dir_all(base.join("home")).unwrap();

    let up = base.join("upstream");
    fs::create_dir_all(&up).unwrap();
    git(&up, &["init", "-q", "-b", "main"]);
    fs::write(up.join(".fast-worktree.toml"), "[pool]\nsize = 1\n").unwrap();
    git(&up, &["add", ".fast-worktree.toml"]);
    git(&up, &["commit", "-q", "-m", "init"]);
    git(&base, &["clone", "-q", "upstream", "main"]);
    let main = base.join("main");

    // The helper only records how it was started.
    let calls = base.join("helper.log");
    let helper = base.join("bin/fast-worktree");
    fs::create_dir_all(helper.parent().unwrap()).unwrap();
    fs::write(
        &helper,
        format!("#!/bin/sh\necho \"$0 $*\" >> '{}'\n", calls.display()),
    )
    .unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();

    std::env::set_current_dir(&main).unwrap();
    let wt = base.join("wt");
    let code = fast_worktree::cli::run_with_helper(
        [
            std::ffi::OsStr::new("fast-worktree"),
            std::ffi::OsStr::new("add"),
            wt.as_os_str(),
            std::ffi::OsStr::new("-b"),
            std::ffi::OsStr::new("topic"),
        ],
        Some(helper.clone()),
    );
    assert_eq!(code, 0);
    assert!(wt.join(".git").exists());

    // `add` started `pool fill` detached; wait for it to run.
    let deadline = Instant::now() + Duration::from_secs(20);
    let log = loop {
        let log = fs::read_to_string(&calls).unwrap_or_default();
        if !log.is_empty() || Instant::now() > deadline {
            break log;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(
        log,
        format!("{} pool fill --after {}\n", helper.display(), wt.display())
    );

    // A usage error returns clap's code instead of exiting the process.
    assert_eq!(
        fast_worktree::cli::run_with_helper(["fast-worktree", "no-such-command"], None),
        2
    );
    assert_eq!(
        fast_worktree::cli::run_with_helper(["fast-worktree", "--version"], None),
        0
    );
}
