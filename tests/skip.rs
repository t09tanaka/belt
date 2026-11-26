//! `CreateOptions::skip` and `add --skip-preset`: a skipped preset runs
//! nothing (no `node -v`, no `npm ci`), takes no spare from the pool, and a
//! later worktree without the skip still gets everything.
//!
//! One test in its own binary: it sets the process environment.
#![cfg(feature = "cli")]

use fast_worktree::{Checkout, CreateOptions, Output, Repository, Skip, Start};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_fast-worktree");

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

fn tool(bin: &Path, name: &str, log: &Path, body: &str) {
    let p = bin.join(name);
    fs::write(
        &p,
        format!(
            "#!/bin/sh\necho \"{name} $*\" >> '{}'\n{body}\n",
            log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn a_skipped_preset_runs_nothing_and_takes_no_spare() {
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

    // Fake node and npm that record every call; `npm ci` makes node_modules.
    let bin = base.join("bin");
    fs::create_dir_all(&bin).unwrap();
    let log = base.join("tools.log");
    tool(&bin, "node", &log, "echo v20.0.0");
    tool(
        &bin,
        "npm",
        &log,
        "mkdir -p node_modules/x && echo '{\"name\":\"x\",\"version\":\"1.0.0\"}' > node_modules/x/package.json",
    );
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    std::env::set_var("PATH", &path);

    let up = base.join("upstream");
    fs::create_dir_all(up.join("examples/a")).unwrap();
    git(&up, &["init", "-q", "-b", "main"]);
    fs::write(up.join(".fast-worktree.toml"), "[pool]\nsize = 1\n").unwrap();
    fs::write(up.join("examples/a/package.json"), "{\"name\":\"a\"}\n").unwrap();
    fs::write(
        up.join("examples/a/package-lock.json"),
        concat!(
            "{\"name\":\"a\",\"lockfileVersion\":3,\"packages\":{",
            "\"\":{\"name\":\"a\",\"dependencies\":{\"x\":\"1.0.0\"}},",
            "\"node_modules/x\":{\"version\":\"1.0.0\"}}}\n"
        ),
    )
    .unwrap();
    git(&up, &["add", "."]);
    git(&up, &["commit", "-q", "-m", "init"]);
    git(&base, &["clone", "-q", "upstream", "main"]);
    let main = base.join("main");

    let repo = Repository::open(&main)
        .unwrap()
        .with_output(Output::Capture)
        .with_helper(None)
        .with_env([("PATH", path.as_str())]);
    repo.fill_pool().unwrap();
    let spare_calls = fs::read_to_string(&log).unwrap_or_default();
    assert!(
        spare_calls.contains("npm"),
        "the spare ran npm: {spare_calls}"
    );
    fs::remove_file(&log).unwrap();

    let opts = |dir: &str| {
        CreateOptions::new(
            base.join(dir),
            Checkout::Detached(Start::Rev("HEAD".into())),
        )
    };

    // Skipped: nothing of npm runs, and the npm-ready spare is not taken.
    let made = repo
        .create_worktree(&opts("skipped").with_skip([Skip::preset("npm")]))
        .unwrap();
    assert!(made.setup_ok);
    assert!(!made.from_pool);
    assert_eq!(fs::read_to_string(&log).unwrap_or_default(), "");
    assert!(!made.path.join("examples/a/node_modules").exists());

    // By directory too.
    let made = repo
        .create_worktree(&opts("by-dir").with_skip([Skip::dir("examples/a")]))
        .unwrap();
    assert!(made.setup_ok && !made.from_pool);
    assert_eq!(fs::read_to_string(&log).unwrap_or_default(), "");

    // The CLI flag does the same.
    let out = Command::new(BIN)
        .args([
            "add",
            "../cli",
            "-b",
            "topic",
            "HEAD",
            "--skip-preset",
            "npm",
        ])
        .current_dir(&main)
        .env("FAST_WORKTREE_POOL_SIZE", "0")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(fs::read_to_string(&log).unwrap_or_default(), "");
    assert!(!base.join("cli/examples/a/node_modules").exists());

    // An unknown preset is an error, before anything is made.
    assert!(repo
        .create_worktree(&opts("unknown").with_skip([Skip::preset("nope")]))
        .is_err());
    assert!(!base.join("unknown").exists());

    // Without the skip the dependencies are prepared as before: the skipped
    // worktrees left no cache entry or spare that hides them.
    let made = repo.create_worktree(&opts("full")).unwrap();
    assert!(made.setup_ok);
    assert!(made.path.join("examples/a/node_modules").exists());
}
