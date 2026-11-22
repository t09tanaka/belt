//! A program that embeds the library gets the automatic deletions without
//! calling anything but what it uses already: creating and removing
//! worktrees, and `sweep_trash` for the cache.

use fast_worktree::{
    sweep_trash, Checkout, CreateOptions, Output, RemoveMode, RemoveOptions, Repository, Start,
};
use std::fs;
use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn age(path: &Path, secs_ago: u64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let ok = Command::new("perl")
        .args(["-e", "utime($ARGV[0], $ARGV[0], $ARGV[1]) or exit 1"])
        .arg((now - secs_ago).to_string())
        .arg(path)
        .status()
        .unwrap()
        .success();
    assert!(ok);
}

fn wait_gone(p: &Path) {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while p.exists() && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

#[test]
fn library_calls_make_the_sweep() {
    let t = tempfile::tempdir().unwrap();
    let base = t.path().canonicalize().unwrap();
    let cache = base.join("cache");
    std::env::set_var("FAST_WORKTREE_CACHE", &cache);
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
    let main = base.join("main");
    fs::create_dir_all(&main).unwrap();
    git(&main, &["init", "-q", "-b", "trunk"]);
    fs::write(main.join("f"), "x").unwrap();
    git(&main, &["add", "f"]);
    git(&main, &["commit", "-qm", "one"]);
    let head = git(&main, &["rev-parse", "HEAD"]).trim().to_string();
    let refs = |dir: &Path| {
        git(
            dir,
            &[
                "for-each-ref",
                "--format=%(refname)",
                "refs/fast-worktree/kept/",
            ],
        )
    };

    let repo = Repository::open(&main)
        .unwrap()
        .with_output(Output::Discard)
        .with_helper(None);

    // Creating a worktree deletes the old kept refs of its repository.
    let old = "refs/fast-worktree/kept/a-20200101-120000/head";
    git(&main, &["update-ref", old, &head]);
    let made = repo
        .create_worktree(&CreateOptions::new(
            base.join("wt1"),
            Checkout::Detached(Start::Rev("HEAD".into())),
        ))
        .unwrap();
    assert!(refs(&main).is_empty(), "{}", refs(&main));

    // So does removing one, an hour later.
    git(&main, &["update-ref", old, &head]);
    for e in fs::read_dir(&cache).unwrap().flatten() {
        let _ = fs::remove_file(e.path().join(".retention-stamp"));
    }
    repo.remove_worktree(&made.path, &RemoveOptions::new(RemoveMode::Discard))
        .unwrap();
    assert!(refs(&main).is_empty(), "{}", refs(&main));

    // `sweep_trash` walks the cache of every repository.
    let proj = cache.join("gone-0123abcd/app");
    let key = "0123456789abcdef-v20.1.0";
    fs::create_dir_all(proj.join(key)).unwrap();
    age(&proj.join(key), 8 * 86_400);
    let _ = fs::remove_file(cache.join(".retention-stamp"));
    sweep_trash();
    wait_gone(&proj);
    assert!(!proj.exists());
}
