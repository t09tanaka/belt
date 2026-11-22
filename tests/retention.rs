//! What fast-worktree deletes by itself: every command that writes to the
//! cache, the pool, the trash or the kept refs also removes what is old
//! there, so nothing is kept without bound and no command has to be run to
//! clean up. Each test runs the binary with its own cache and home in a
//! temp directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_fast-worktree");
const DAY: u64 = 86_400;

struct World {
    _t: TempDir,
    base: PathBuf,
}

impl World {
    fn new() -> World {
        let t = tempfile::tempdir().unwrap();
        let base = t.path().canonicalize().unwrap();
        let w = World { _t: t, base };
        fs::create_dir_all(w.main()).unwrap();
        w.git(&w.main(), &["init", "-q", "-b", "trunk"]);
        fs::write(w.main().join("f"), "x").unwrap();
        w.git(&w.main(), &["add", "f"]);
        w.git(&w.main(), &["commit", "-qm", "one"]);
        w
    }

    fn main(&self) -> PathBuf {
        self.base.join("main")
    }

    fn cache(&self) -> PathBuf {
        self.base.join("cache")
    }

    fn env(&self, c: &mut Command) {
        c.env("HOME", self.base.join("home"))
            .env("FAST_WORKTREE_CACHE", self.cache())
            .env("XDG_CONFIG_HOME", self.base.join("config"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@example.com")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@example.com");
    }

    fn git(&self, dir: &Path, args: &[&str]) -> String {
        let mut c = Command::new("git");
        self.env(&mut c);
        let out = c.args(args).current_dir(dir).output().unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// `fast-worktree <args>` in the main checkout; must succeed.
    fn fw(&self, args: &[&str]) -> String {
        let mut c = Command::new(BIN);
        self.env(&mut c);
        let out = c.args(args).current_dir(self.main()).output().unwrap();
        assert!(
            out.status.success(),
            "fast-worktree {args:?}: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// A command that does a sweep: `add` of a new worktree.
    fn add(&self, name: &str) {
        let wt = self.base.join(name);
        self.fw(&["add", "-b", name, wt.to_str().unwrap(), "HEAD"]);
    }

    /// The cache directory of the main checkout (what `paths::repo_key`
    /// names): the one that `add` makes its stamp in.
    fn repo_dir(&self) -> PathBuf {
        fs::read_dir(self.cache())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.join(".retention-stamp").is_file())
            .expect("a repository directory with a stamp")
    }

    fn forget_stamps(&self) {
        let _ = fs::remove_file(self.cache().join(".retention-stamp"));
        for e in fs::read_dir(self.cache()).unwrap().flatten() {
            let _ = fs::remove_file(e.path().join(".retention-stamp"));
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Set the modification time of `path` to `secs_ago` seconds ago.
fn age(path: &Path, secs_ago: u64) {
    let t = now() - secs_ago;
    let ok = Command::new("perl")
        .args(["-e", "utime($ARGV[0], $ARGV[0], $ARGV[1]) or exit 1"])
        .arg(t.to_string())
        .arg(path)
        .status()
        .unwrap()
        .success();
    assert!(ok, "could not age {}", path.display());
}

fn wait_gone(p: &Path) {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while p.exists() && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// A pid that no process has: far beyond what the system hands out.
const DEAD_PID: u32 = 2_000_000_000;

#[test]
fn old_kept_refs_go_with_the_next_add_and_recent_ones_stay() {
    let w = World::new();
    let m = w.main();
    let head = w.git(&m, &["rev-parse", "HEAD"]).trim().to_string();
    let old = format!("refs/fast-worktree/kept/a-{}-120000/head", "20200101");
    let recent = "refs/fast-worktree/kept/b-29990101-120000/head";
    let undated = "refs/fast-worktree/kept/c/head";
    for r in [old.as_str(), recent, undated] {
        w.git(&m, &["update-ref", r, &head]);
    }
    w.add("wt1");
    let refs = w.git(
        &m,
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/fast-worktree/kept/",
        ],
    );
    assert!(!refs.contains("kept/a-"), "{refs}");
    assert!(
        refs.contains("kept/b-") && refs.contains("kept/c/"),
        "{refs}"
    );
}

#[test]
fn kept_refs_are_pruned_once_an_hour_at_most() {
    let w = World::new();
    let m = w.main();
    let head = w.git(&m, &["rev-parse", "HEAD"]).trim().to_string();
    w.add("wt1");
    let old = "refs/fast-worktree/kept/a-20200101-120000/head";
    w.git(&m, &["update-ref", old, &head]);
    // The walk of this hour is done: the next add leaves it.
    w.add("wt2");
    assert!(w.git(&m, &["for-each-ref", old]).contains("kept/a-"));
    // An hour later it is not.
    age(&w.repo_dir().join(".retention-stamp"), 3_601);
    w.add("wt3");
    assert!(w.git(&m, &["for-each-ref", old]).trim().is_empty());
}

#[test]
fn cache_entries_of_abandoned_projects_and_half_made_ones_are_removed() {
    let w = World::new();
    let proj = w.cache().join("gone-0123abcd/app");
    let key = "0123456789abcdef-v20.1.0";
    fs::create_dir_all(proj.join(key)).unwrap();
    fs::write(proj.join(key).join("f"), "x").unwrap();
    fs::write(proj.join(format!("{key}.ok")), "").unwrap();
    fs::write(proj.join(format!("{key}.src")), "/x").unwrap();
    let recent = "fedcba9876543210-v20.1.0";
    fs::create_dir_all(proj.join(recent)).unwrap();
    // A half-made entry: its run is gone, one with a live owner, and one
    // too young to tell.
    let dead = format!("{key}.tmp-{DEAD_PID}-0");
    let live = format!("{key}.tmp-{}-0", std::process::id());
    let young = format!("{key}.tmp-{DEAD_PID}-1");
    for t in [&dead, &live, &young] {
        fs::create_dir_all(proj.join(t)).unwrap();
    }
    age(&proj.join(key), 8 * DAY);
    age(&proj.join(&dead), 11 * 60);
    age(&proj.join(&live), 2 * 3_600);
    // A project that has nothing left but its lock file.
    let empty = w.cache().join("gone-0123abcd/lib");
    fs::create_dir_all(&empty).unwrap();
    fs::write(empty.join(".store.lock"), "").unwrap();

    w.add("wt1");

    wait_gone(&proj.join(key));
    assert!(!proj.join(key).exists(), "the old entry");
    assert!(!proj.join(format!("{key}.ok")).exists());
    assert!(!proj.join(format!("{key}.src")).exists());
    wait_gone(&proj.join(&dead));
    assert!(!proj.join(&dead).exists(), "half made, owner gone");
    assert!(proj.join(&live).is_dir(), "its owner is alive");
    assert!(proj.join(&young).is_dir(), "too young");
    assert!(proj.join(recent).is_dir(), "used recently");
    assert!(!empty.exists(), "an empty project directory goes");
    assert!(proj.is_dir());
}

#[test]
fn a_project_directory_goes_when_its_last_entry_does() {
    let w = World::new();
    let proj = w.cache().join("gone-0123abcd/app");
    let key = "0123456789abcdef-v20.1.0";
    fs::create_dir_all(proj.join(key)).unwrap();
    fs::write(proj.join(".store.lock"), "").unwrap();
    fs::write(proj.join(format!("{key}.ok")), "").unwrap();
    age(&proj.join(key), 8 * DAY);
    w.add("wt1");
    wait_gone(&proj);
    assert!(!proj.exists());
    assert!(
        !w.cache().join("gone-0123abcd").exists(),
        "and the repository directory with it"
    );
}

#[test]
fn the_cache_walk_is_made_once_an_hour_at_most() {
    let w = World::new();
    // The first run makes the cache directory; the second walks it.
    w.add("wt1");
    w.add("wt2");
    let proj = w.cache().join("gone-0123abcd/app");
    let key = "0123456789abcdef-v20.1.0";
    fs::create_dir_all(proj.join(key)).unwrap();
    age(&proj.join(key), 8 * DAY);
    w.add("wt3");
    assert!(proj.join(key).is_dir(), "the walk of this hour is done");
    age(&w.cache().join(".retention-stamp"), 3_601);
    w.add("wt4");
    wait_gone(&proj);
    assert!(!proj.exists());
}

#[test]
fn pool_leftovers_of_killed_runs_and_old_misses_are_removed() {
    let w = World::new();
    w.add("wt1");
    let pool = w.repo_dir().join("pool");
    fs::create_dir_all(&pool).unwrap();
    let old = now() - 2 * 3_600;
    let fresh = now();
    // A half-made spare with no fill running, one claimed by a process that
    // is gone, and one claimed by a live one.
    let building = pool.join(format!(".new-wt-{old}-1-0"));
    let claimed = pool.join(format!(".claimed-{DEAD_PID}-wt-{old}-1-1"));
    let live = pool.join(format!(".claimed-{}-wt-{old}-1-2", std::process::id()));
    let other = pool.join(".something-else");
    for d in [&building, &claimed, &live, &other] {
        fs::create_dir_all(d.join("sub")).unwrap();
    }
    let stale_miss = now() - 8 * DAY;
    fs::write(pool.join("misses"), format!("{stale_miss}\n{fresh}\n")).unwrap();
    let log = pool.join("fill.log");
    fs::write(&log, "old log").unwrap();
    age(&log, 8 * DAY);
    w.forget_stamps();

    w.add("wt2");

    wait_gone(&building);
    wait_gone(&claimed);
    assert!(!building.exists() && !claimed.exists());
    assert!(live.is_dir() && other.is_dir());
    assert_eq!(
        fs::read_to_string(pool.join("misses")).unwrap(),
        format!("{fresh}\n")
    );
    assert!(!log.exists());
}

#[test]
fn a_misses_file_with_nothing_recent_is_removed() {
    let w = World::new();
    w.add("wt1");
    let pool = w.repo_dir().join("pool");
    fs::create_dir_all(&pool).unwrap();
    fs::write(pool.join("misses"), format!("{}\n", now() - 8 * DAY)).unwrap();
    w.forget_stamps();
    w.add("wt2");
    assert!(!pool.join("misses").exists());
}

#[test]
fn the_trash_is_emptied_by_any_add() {
    let w = World::new();
    let trash = w.cache().join("trash");
    let orphan = trash.join(format!("{}-1-1-wt", now() - 700));
    let kept_due = trash.join(format!("keep-{}-{}-1-1-wt", now() - 5, now() - DAY));
    let kept_not_due = trash.join(format!("keep-{}-{}-1-2-wt", now() + DAY, now()));
    for d in [&orphan, &kept_due, &kept_not_due] {
        fs::create_dir_all(d.join("sub")).unwrap();
    }
    w.add("wt1");
    wait_gone(&orphan);
    wait_gone(&kept_due);
    assert!(!orphan.exists() && !kept_due.exists());
    assert!(kept_not_due.is_dir());
}

#[test]
fn the_manual_prune_applies_the_same_rules_to_half_made_entries() {
    let w = World::new();
    let proj = w.cache().join("gone-0123abcd/app");
    let key = "0123456789abcdef-v20.1.0";
    let dead = format!("{key}.tmp-{DEAD_PID}-0");
    fs::create_dir_all(proj.join(&dead)).unwrap();
    age(&proj.join(&dead), 11 * 60);
    w.fw(&["cache", "prune"]);
    wait_gone(&proj);
    assert!(!proj.exists());
}

#[test]
fn directories_with_nothing_left_in_them_are_removed_with_their_locks() {
    let w = World::new();
    let repo = w.cache().join("gone-0123abcd");
    let pool = repo.join("pool");
    fs::create_dir_all(&pool).unwrap();
    fs::write(pool.join("fill.lock"), "").unwrap();
    fs::write(repo.join("lifecycle.lock"), "").unwrap();
    age(&pool, 120);
    let trash = w.cache().join("trash");
    fs::create_dir_all(&trash).unwrap();
    fs::write(trash.join(".lock"), "").unwrap();
    age(&trash, 120);
    // The walk is the one of this hour's second command.
    w.add("wt1");
    w.add("wt2");
    assert!(!repo.exists(), "the repository directory and its locks go");
    assert!(!trash.exists(), "the empty trash goes");
}
