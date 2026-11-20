//! The library and the `fast-worktree` binary working on one repository and
//! one state directory at the same time: the pool, the cache, the trash,
//! the kept refs and git's own registration of worktrees must come out
//! consistent, and each side must be able to use what the other made.

use fast_worktree::{
    Checkout, CreateOptions, Error, LogStream, Output, RemoveMode, RemoveOptions, Repository, Start,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output as ProcOutput};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_fast-worktree");

const CONFIG: &str = r#"[pool]
size = 2

[[post_add]]
run = 'echo ran > .post_add_ran'

[[clone]]
path = "gen"
key = ["gen.lock"]
install = "mkdir -p gen && cp gen.lock gen/installed"
"#;

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

fn cli(dir: &Path, args: &[&str]) -> ProcOutput {
    Command::new(BIN)
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap()
}

#[track_caller]
fn cli_ok(dir: &Path, args: &[&str]) -> String {
    let out = cli(dir, args);
    assert!(
        out.status.success(),
        "fast-worktree {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Wait until `cond` holds, for at most `secs` seconds.
fn eventually(secs: u64, what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(secs),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The entries of `dir`, not counting the hidden lock file.
fn entries(dir: &Path) -> usize {
    fs::read_dir(dir).map_or(0, |d| {
        d.flatten()
            .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
            .count()
    })
}

/// Wait until no background refill of the pool is running: a `pool fill`
/// that finds the lock free runs to its end, and says nothing of another.
fn settle(main: &Path) {
    eventually(60, "the background refills to finish", || {
        !cli_ok(main, &["pool", "fill"]).contains("another fill is running")
    });
}

#[track_caller]
fn assert_prepared(wt: &Path) {
    assert!(wt.join("gen/installed").is_file(), "no deps in {wt:?}");
    assert!(wt.join(".post_add_ran").is_file(), "no hook in {wt:?}");
    assert!(wt.join(".env").is_file(), "no include in {wt:?}");
}

#[test]
fn the_library_and_the_cli_share_one_repository_and_one_state_directory() {
    let t = tempfile::tempdir().unwrap();
    let base = t.path().canonicalize().unwrap();
    let cache = base.join("cache");
    // The same environment for this process and the binary it starts.
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

    let up = base.join("upstream");
    fs::create_dir_all(&up).unwrap();
    git(&up, &["init", "-q", "-b", "main"]);
    fs::write(up.join("gen.lock"), "v1\n").unwrap();
    fs::write(up.join(".fast-worktree.toml"), CONFIG).unwrap();
    fs::write(up.join(".gitignore"), "gen/\n.env\n.post_add_ran\n").unwrap();
    fs::write(up.join(".worktreeinclude"), "# secrets\n.env\n").unwrap();
    git(&up, &["add", "-A"]);
    git(&up, &["commit", "-q", "-m", "init"]);
    git(&base, &["clone", "-q", "upstream", "main"]);
    let main = base.join("main");
    fs::write(main.join(".env"), "SECRET=1\n").unwrap();

    let repo = Repository::open(&main)
        .unwrap()
        .with_output(Output::Capture)
        .with_helper(Some(PathBuf::from(BIN)));
    assert_eq!(repo.main_checkout(), main);

    // The effective config.
    let cfg = repo.config().unwrap();
    assert_eq!(cfg.pool_size, 2);
    assert_eq!(cfg.include, [".env"]);
    assert_eq!(cfg.config.post_add.len(), 1);
    assert_eq!(cfg.config.clone.len(), 1);
    assert!(cfg.file.is_some() && cfg.include_file.is_some());

    // Filling the pool from both sides at once: one fill runs, the other
    // finds the lock taken, and the pool ends with exactly its size.
    let child = Command::new(BIN)
        .args(["pool", "fill"])
        .current_dir(&main)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    repo.fill_pool().unwrap();
    let c = child.wait_with_output().unwrap();
    assert!(c.status.success(), "{}", String::from_utf8_lossy(&c.stderr));
    // Whoever lost the race returned at once; wait for the winner.
    eventually(60, "two spares", || {
        cli_ok(&main, &["pool", "status"]).contains("2 of 2 ready")
    });

    // Library creates and CLI adds, all at once.
    const N: usize = 4;
    let wts = base.join("wts");
    fs::create_dir_all(&wts).unwrap();
    let results = std::thread::scope(|s| {
        let mut lib = Vec::new();
        for i in 0..N {
            let repo = repo.clone();
            let path = wts.join(format!("lib{i}"));
            lib.push(s.spawn(move || {
                repo.create_worktree(&CreateOptions::new(
                    path,
                    Checkout::Detached(Start::Rev("main".into())),
                ))
            }));
        }
        let mut cli_runs = Vec::new();
        for i in 0..N {
            let main = main.clone();
            let path = wts.join(format!("cli{i}"));
            cli_runs.push(s.spawn(move || {
                cli(
                    &main,
                    &["add", path.to_str().unwrap(), "-b", &format!("cli-{i}")],
                )
            }));
        }
        (
            lib.into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>(),
            cli_runs
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>(),
        )
    });
    let mut from_pool = 0;
    for (i, r) in results.0.iter().enumerate() {
        let c = r.as_ref().unwrap_or_else(|e| panic!("lib{i}: {e}"));
        assert!(c.setup_ok, "lib{i}: {:?}", c.log);
        assert_eq!(c.path, wts.join(format!("lib{i}")));
        assert_prepared(&c.path);
        // Captured, not printed.
        assert!(
            c.log
                .iter()
                .any(|l| l.stream == LogStream::Stdout && l.text.starts_with("worktree ")),
            "{:?}",
            c.log
        );
        from_pool += usize::from(c.from_pool);
    }
    for (i, out) in results.1.iter().enumerate() {
        assert!(
            out.status.success(),
            "cli{i}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_prepared(&wts.join(format!("cli{i}")));
    }
    assert!(from_pool <= N);
    // Git's own bookkeeping is whole: nothing prunable, nothing half-made.
    // First let the background refills that the creations started finish.
    settle(&main);
    let listing = git(&main, &["worktree", "list", "--porcelain"]);
    assert!(!listing.contains("prunable"), "{listing}");
    for i in 0..N {
        assert!(listing.contains(&format!("wts/lib{i}")), "{listing}");
        assert!(listing.contains(&format!("wts/cli{i}")), "{listing}");
    }
    git(&main, &["fsck", "--no-progress"]);

    // The cache is shared: the CLI's installs left entries the library's
    // creations could use, and the other way round, without a mix-up.
    let cached = cli_ok(&main, &["cache", "ls"]);
    assert!(cached.contains("gen"), "{cached}");

    // A dirty worktree: refused when strict, typed; discarded when asked.
    let dirty = wts.join("lib0");
    fs::write(dirty.join("notes.txt"), "mine\n").unwrap();
    match repo.remove_worktree(
        &dirty,
        &RemoveOptions::new(RemoveMode::Strict {
            delete_branch: false,
        }),
    ) {
        Err(Error::Refused { reason, .. }) => {
            assert_eq!(reason, fast_worktree::Refusal::Dirty)
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert!(dirty.exists());
    assert!(matches!(
        repo.remove_worktree(
            &main,
            &RemoveOptions::new(RemoveMode::Strict {
                delete_branch: false
            })
        ),
        Err(Error::Refused {
            reason: fast_worktree::Refusal::MainCheckout,
            ..
        })
    ));

    // Removals from both sides at once: the library discards some, the CLI
    // removes others, the one with changes is kept with its HEAD in a ref.
    std::thread::scope(|s| {
        let lib: Vec<_> = (0..N)
            .map(|i| {
                let repo = repo.clone();
                let path = wts.join(format!("lib{i}"));
                s.spawn(move || {
                    repo.remove_worktree(&path, &RemoveOptions::new(RemoveMode::Discard))
                })
            })
            .collect();
        let cli_runs: Vec<_> = (0..N)
            .map(|i| {
                let main = main.clone();
                let path = wts.join(format!("cli{i}"));
                s.spawn(move || cli(&main, &["remove", path.to_str().unwrap()]))
            })
            .collect();
        for (i, h) in lib.into_iter().enumerate() {
            let r = h.join().unwrap().unwrap_or_else(|e| panic!("lib{i}: {e}"));
            assert!(r.complete, "lib{i}: {:?}", r.log);
            assert_eq!(r.kept, i == 0, "lib{i}");
            assert!(r.trash.is_some());
        }
        for (i, h) in cli_runs.into_iter().enumerate() {
            let out = h.join().unwrap();
            assert!(
                out.status.success(),
                "cli{i}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    });
    for i in 0..N {
        assert!(!wts.join(format!("lib{i}")).exists());
        assert!(!wts.join(format!("cli{i}")).exists());
    }
    let listing = git(&main, &["worktree", "list", "--porcelain"]);
    assert!(!listing.contains("wts/"), "{listing}");
    assert!(!listing.contains("prunable"), "{listing}");

    // The kept ref is visible to both, and the library can prune it.
    let kept = repo.kept_refs().unwrap();
    assert_eq!(kept.len(), 1, "{kept:?}");
    assert!(kept[0].kept_at.is_some());
    assert!(cli_ok(&main, &["kept", "ls"]).contains(&kept[0].name));
    assert_eq!(repo.prune_kept_refs(0).unwrap(), 1);
    assert!(repo.kept_refs().unwrap().is_empty());

    // The trash drains: only the kept worktree (for a day) is left in it.
    fast_worktree::sweep_trash();
    let trash = cache.join("trash");
    eventually(30, "the trash to drain", || entries(&trash) <= 1);
    assert_eq!(entries(&trash), 1, "the kept worktree stays");

    // The pool is intact after all of it.
    git(&main, &["worktree", "prune", "--dry-run", "-v"]);
    let status = cli_ok(&main, &["pool", "status"]);
    assert!(!status.is_empty());
    repo.fill_pool().unwrap();
}
