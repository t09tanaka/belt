//! A program that embeds the library gets the automatic deletions without
//! calling anything but what it uses already: creating and removing
//! worktrees, and `sweep_trash` for the cache.

use fast_worktree::{
    sweep_trash, Checkout, CreateOptions, KeptRefDeletion, Output, RemoveMode, RemoveOptions,
    Repository, Start,
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

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn age(path: &Path, secs_ago: u64) {
    let ok = Command::new("perl")
        .args(["-e", "utime($ARGV[0], $ARGV[0], $ARGV[1]) or exit 1"])
        .arg((now() - secs_ago).to_string())
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
        .with_output(Output::Capture)
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
    // The deletion of a ref is in the log of the call that made it.
    let said = made
        .log
        .iter()
        .any(|l| l.text.contains(old) && l.text.contains(&head));
    assert!(said, "{:?}", made.log);
    // And in the result, for a caller that does not read the log.
    assert_eq!(made.deleted_kept_refs.len(), 1, "{:?}", made);
    let d = &made.deleted_kept_refs[0];
    assert_eq!((d.name.as_str(), d.commit.as_str()), (old, head.as_str()));
    assert_eq!(d.reason, KeptRefDeletion::Expired);

    // So does removing one, an hour later.
    git(&main, &["update-ref", old, &head]);
    for e in fs::read_dir(&cache).unwrap().flatten() {
        let _ = fs::remove_file(e.path().join(".retention-stamp"));
    }
    let quiet = repo.clone().with_output(Output::Discard);
    let removed = quiet
        .remove_worktree(&made.path, &RemoveOptions::new(RemoveMode::Discard))
        .unwrap();
    assert!(refs(&main).is_empty(), "{}", refs(&main));
    assert!(removed.log.is_empty());
    assert_eq!(removed.deleted_kept_refs.len(), 1, "{removed:?}");
    assert_eq!(removed.deleted_kept_refs[0].name, old);

    // `prune_kept_refs` goes by age alone, as it always did: a ref that
    // a branch has made needless stays until it is that old.
    let stamp = |secs_ago: u64| {
        let out = Command::new("perl")
            .args([
                "-MPOSIX",
                "-e",
                "print strftime('%Y%m%d-%H%M%S', gmtime($ARGV[0]))",
            ])
            .arg((now() - secs_ago).to_string())
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let held = format!("refs/fast-worktree/kept/held-{}/head", stamp(2 * 86_400));
    git(&main, &["update-ref", &held, &head]);
    assert_eq!(repo.prune_kept_refs(30).unwrap(), 0);
    assert_eq!(repo.prune_kept_refs(3).unwrap(), 0);
    assert!(refs(&main).contains(&held));
    // A fill deletes none either: it has nowhere to say so.
    fs::create_dir_all(base.join("xdg/fast-worktree")).unwrap();
    fs::write(
        base.join("xdg/fast-worktree/config.toml"),
        "[pool]\nsize = 1\n",
    )
    .unwrap();
    for e in fs::read_dir(&cache).unwrap().flatten() {
        let _ = fs::remove_file(e.path().join(".retention-stamp"));
    }
    let _ = repo.fill_pool();
    fs::remove_file(base.join("xdg/fast-worktree/config.toml")).unwrap();
    assert!(refs(&main).contains(&held));
    // The automatic rule, asked for: it is held by the branch.
    let swept = repo.sweep_kept_refs().unwrap();
    assert_eq!(swept.len(), 1, "{swept:?}");
    assert_eq!(swept[0].name, held);
    assert!(
        matches!(&swept[0].reason, KeptRefDeletion::HeldBy { holder, .. } if holder == "trunk"),
        "{swept:?}"
    );
    assert!(refs(&main).is_empty(), "{}", refs(&main));
    // And age alone still deletes what is that old, held or not.
    git(&main, &["update-ref", &held, &head]);
    assert_eq!(repo.prune_kept_refs(2).unwrap(), 1);
    assert!(refs(&main).is_empty(), "{}", refs(&main));

    // `sweep_trash` walks the cache of every repository.
    let proj = cache.join("gone-0123abcd/app");
    let key = "0123456789abcdef-v20.1.0";
    fs::create_dir_all(proj.join(key)).unwrap();
    fs::write(proj.join(format!("{key}.ok")), "").unwrap();
    age(&proj.join(key), 8 * 86_400);
    let _ = fs::remove_file(cache.join(".retention-stamp"));
    sweep_trash();
    wait_gone(&proj.join(key));
    assert!(!proj.join(key).exists());
}
