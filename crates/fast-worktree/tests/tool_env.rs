//! A caller that activates a toolchain per call: the commands of a call run
//! with the environment it gives (`Repository::with_env`), and the toolchain
//! identity it gives (`Repository::with_tool_identity`) keeps caches made
//! with different toolchains apart.

use fast_worktree::{Checkout, CreateOptions, Output, Repository, Start};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

const CONFIG: &str = r#"[pool]
size = 0

[[clone]]
path = "gen"
key = ["gen.lock"]
key_commands = ["mytool"]
install = 'mkdir -p gen && mytool > gen/v && echo x >> "$FAST_WORKTREE_MAIN/../installs"'
"#;

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

fn tool(dir: &Path, prints: &str) {
    fs::create_dir_all(dir).unwrap();
    let p = dir.join("mytool");
    fs::write(&p, format!("#!/bin/sh\necho {prints}\n")).unwrap();
    fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn commands_run_with_the_given_env_and_the_identity_is_part_of_the_key() {
    let t = tempfile::tempdir().unwrap();
    let base = t.path().canonicalize().unwrap();
    std::env::set_var("FAST_WORKTREE_CACHE", base.join("cache"));
    std::env::set_var("HOME", base.join("home"));
    std::env::set_var("XDG_CONFIG_HOME", base.join("xdg"));
    std::env::set_var("GIT_CONFIG_NOSYSTEM", "1");
    std::env::set_var("GIT_CONFIG_GLOBAL", "/dev/null");
    std::env::remove_var("FAST_WORKTREE_KEY_IDENTITY");
    for (k, v) in [
        ("GIT_AUTHOR_NAME", "t"),
        ("GIT_AUTHOR_EMAIL", "t@example.com"),
        ("GIT_COMMITTER_NAME", "t"),
        ("GIT_COMMITTER_EMAIL", "t@example.com"),
    ] {
        std::env::set_var(k, v);
    }
    fs::create_dir_all(base.join("home")).unwrap();
    // `mytool` is on no PATH of this process, only on the one given.
    let bin = base.join("toolchain/bin");
    tool(&bin, "tool-a");
    let path = std::env::join_paths(
        std::iter::once(bin.clone())
            .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();

    let main = base.join("main");
    fs::create_dir_all(&main).unwrap();
    git(&main, &["init", "-q", "-b", "main"]);
    fs::write(main.join("gen.lock"), "v1\n").unwrap();
    fs::write(main.join(".fast-worktree.toml"), CONFIG).unwrap();
    fs::write(main.join(".gitignore"), "gen/\n").unwrap();
    git(&main, &["add", "-A"]);
    git(&main, &["commit", "-q", "-m", "init"]);

    let repo = |identity: Option<&str>| {
        Repository::open(&main)
            .unwrap()
            .with_output(Output::Capture)
            .with_helper(None)
            .with_env([("PATH", path.clone())])
            .with_tool_identity(identity.map(str::to_string))
    };
    let installs = || {
        fs::read_to_string(base.join("installs"))
            .unwrap_or_default()
            .lines()
            .count()
    };
    let create = |name: &str, identity: Option<&str>| {
        let wt = base.join(name);
        let made = repo(identity)
            .create_worktree(&CreateOptions::new(
                &wt,
                Checkout::Detached(Start::Rev("HEAD".into())),
            ))
            .unwrap();
        assert!(made.setup_ok, "{name}: {:?}", made.log);
        assert_eq!(
            fs::read_to_string(wt.join("gen/v")).unwrap(),
            "tool-a\n",
            "{name}: the install ran with the given PATH"
        );
    };

    // The key command and the install find `mytool` on the given PATH.
    create("wt1", None);
    assert_eq!(installs(), 1);
    // Same outputs of the key commands, but an identity: not wt1's key.
    create("wt2", Some("toolchain-1"));
    assert_eq!(installs(), 2, "an identity must not take wt1's tree");
    // Another identity: neither wt1's nor wt2's.
    create("wt3", Some("toolchain-2"));
    assert_eq!(
        installs(),
        3,
        "different identities must never share a tree"
    );
    // The same identity again: cloned, not installed (clones are made on
    // macOS only; elsewhere every worktree installs).
    let cloned = if cfg!(target_os = "macos") { 0 } else { 1 };
    create("wt4", Some("toolchain-1"));
    assert_eq!(installs(), 3 + cloned, "the same identity shares its tree");
    create("wt5", None);
    assert_eq!(
        installs(),
        3 + 2 * cloned,
        "no identity is the CLI's key, as before"
    );
}
