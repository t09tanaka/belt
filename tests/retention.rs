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

    /// A command that does a sweep: `add` of a new worktree. Returns what
    /// it wrote to stderr.
    fn add(&self, name: &str) -> String {
        let wt = self.base.join(name);
        let mut c = Command::new(BIN);
        self.env(&mut c);
        let out = c
            .args(["add", "-b", name, wt.to_str().unwrap(), "HEAD"])
            .current_dir(self.main())
            .output()
            .unwrap();
        let err = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(out.status.success(), "add {name}: {err}");
        err
    }

    /// A commit that no ref holds, made on top of `HEAD`.
    fn loose_commit(&self, message: &str) -> String {
        let m = self.main();
        self.git(
            &m,
            &["commit-tree", "HEAD^{tree}", "-p", "HEAD", "-m", message],
        )
        .trim()
        .to_string()
    }

    fn kept_refs(&self) -> String {
        self.git(
            &self.main(),
            &[
                "for-each-ref",
                "--format=%(refname)",
                "refs/fast-worktree/kept/",
            ],
        )
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

/// `YYYYMMDD-HHMMSS` (UTC) of `secs`, as the kept refs are named.
fn compact(secs: u64) -> String {
    let days = (secs / DAY) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let s = secs % DAY;
    format!(
        "{y:04}{m:02}{d:02}-{:02}{:02}{:02}",
        s / 3_600,
        s % 3_600 / 60,
        s % 60
    )
}

fn inode(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .ino()
}

/// Hold an exclusive `flock` on `path`, as a running command would.
fn hold(path: &Path) -> fs::File {
    use std::os::unix::io::AsRawFd;
    let f = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .unwrap();
    // SAFETY: a valid descriptor, unlocked when the file is closed.
    assert_eq!(
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    f
}

/// A cache entry as the tool stores one: the directory and its marker.
fn entry(project: &Path, key: &str, unused_secs: u64) -> PathBuf {
    let e = project.join(key);
    fs::create_dir_all(e.join("pkg")).unwrap();
    fs::write(project.join(format!("{key}.ok")), "").unwrap();
    age(&e, unused_secs);
    e
}

const KEY: &str = "0123456789abcdef-v20.1.0";

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
    let refs = w.kept_refs();
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
fn a_kept_ref_goes_at_once_when_a_branch_has_its_commit_and_after_30_days_when_nothing_has() {
    let w = World::new();
    let m = w.main();
    let head = w.git(&m, &["rev-parse", "HEAD"]).trim().to_string();
    let (alone, old, twice, detached, remote, tagged, nested) = (
        w.loose_commit("alone"),
        w.loose_commit("old"),
        w.loose_commit("twice"),
        w.loose_commit("detached"),
        w.loose_commit("remote"),
        w.loose_commit("tagged"),
        w.loose_commit("nested"),
    );
    // What somebody else can take away with a push and a `fetch --prune`
    // holds nothing; a tag and a branch (whatever its name) do.
    w.git(&m, &["update-ref", "refs/remotes/origin/theirs", &remote]);
    w.git(&m, &["tag", "-a", "-m", "v1", "v1", &tagged]);
    w.git(&m, &["branch", "feature/nested/x", &nested]);
    // A worktree with a detached HEAD holds no commit for this purpose: it
    // goes with one `git worktree remove`.
    let dw = w.base.join("dw");
    w.git(
        &m,
        &[
            "worktree",
            "add",
            "-q",
            "--detach",
            dw.to_str().unwrap(),
            &detached,
        ],
    );
    let name = |n: &str, ago: u64, leaf: &str| {
        format!(
            "refs/fast-worktree/kept/{n}-{}/{leaf}",
            compact(now() - ago)
        )
    };
    let hour = 3_600;
    let refs = [
        // On the branch `trunk`: protects nothing, deleted long before
        // its 30 days...
        (name("held", 26 * hour, "head"), &head, false),
        // ...but not while the removal that made it may still be running,
        // or the files it kept for a day can still be restored onto it.
        (name("young", 24 * hour, "head"), &head, true),
        // The only thing that holds its commit: kept for 30 days,
        (name("alone", 26 * hour, "head"), &alone, true),
        (name("almost", 30 * DAY - 300, "head"), &alone, true),
        // and deleted then.
        (name("old", 30 * DAY + 300, "head"), &old, false),
        // Two kept refs with one commit do not hold it for each other.
        (name("twice", 26 * hour, "head"), &twice, true),
        (name("again", 27 * hour, "head"), &twice, true),
        (name("detached", 26 * hour, "head"), &detached, true),
        // A submodule's commit is another repository's: never asked about.
        (name("sub", 26 * hour, "sub/lib"), &head, true),
        // Only a remote-tracking branch has it: held by nobody, 30 days.
        (name("remote", 26 * hour, "head"), &remote, true),
        (name("tagged", 26 * hour, "head"), &tagged, false),
        (name("nested", 26 * hour, "head"), &nested, false),
    ];
    for (r, commit, _) in &refs {
        w.git(&m, &["update-ref", r, commit]);
    }
    let err = w.add("wt1");
    let left = w.kept_refs();
    for (r, _, stays) in &refs {
        assert_eq!(left.contains(r.as_str()), *stays, "{r}\n{left}");
    }
    // Every deletion is said, one line each, with the ref, the commit and
    // why: what holds the commit, or that the 30 days passed.
    let line = |i: usize| {
        let mut lines = err.lines().filter(|l| l.contains(&refs[i].0));
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("no line for {} in:\n{err}", refs[i].0));
        assert_eq!(lines.next(), None, "one line for {}", refs[i].0);
        assert!(line.contains(refs[i].1.as_str()), "{line}");
        line.to_string()
    };
    assert!(line(4).contains("30 days passed"), "{}", line(4));
    assert!(line(4).contains("git gc"), "{}", line(4));
    // (`trunk` has it, and so has the tag, whose commit was made on top
    // of it: git names one of them.)
    assert!(
        line(0).contains("is held by trunk") || line(0).contains("is held by tags/v1"),
        "{}",
        line(0)
    );
    assert!(!line(0).contains("git gc"), "{}", line(0));
    assert!(line(10).contains("is held by tags/v1"), "{}", line(10));
    assert!(
        line(11).contains("is held by feature/nested/x"),
        "{}",
        line(11)
    );
    let deletions = err.lines().filter(|l| l.contains(": deleted ")).count();
    assert_eq!(deletions, 4, "{err}");
    // The manual command applies the same rules, and names both.
    w.git(&m, &["update-ref", &refs[0].0, &head]);
    let out = w.fw(&["kept", "prune"]);
    assert!(out.contains(&refs[0].0), "{out}");
    assert!(out.contains("its commit is held by "), "{out}");
    assert!(!w.kept_refs().contains(&refs[0].0));
    assert!(w.kept_refs().contains(&refs[2].0));
    assert!(w.kept_refs().contains(&refs[9].0), "remote-tracking only");
}

#[test]
fn a_kept_ref_whose_objects_are_gone_is_kept_rather_than_guessed_at() {
    let w = World::new();
    let m = w.main();
    let head = w.git(&m, &["rev-parse", "HEAD"]).trim().to_string();
    let r = format!(
        "refs/fast-worktree/kept/x-{}/head",
        compact(now() - 2 * DAY)
    );
    // One names an object that is not there: git says nothing about it,
    // so it is not taken to be on a branch; the other is answered for.
    w.git(&m, &["update-ref", &r, &head]);
    let broken = format!(
        "refs/fast-worktree/kept/y-{}/head",
        compact(now() - 2 * DAY)
    );
    let file = m.join(".git").join(&broken);
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(&file, format!("{}\n", "1".repeat(40))).unwrap();
    let err = w.add("wt1");
    assert!(w.kept_refs().contains(&broken), "{}", w.kept_refs());
    assert!(!w.kept_refs().contains(&r), "{}", w.kept_refs());
    assert!(!err.contains(&broken), "{err}");
}

#[test]
fn kept_refs_that_cannot_be_deleted_are_left_for_the_next_time_and_said_once() {
    let w = World::new();
    let m = w.main();
    let head = w.git(&m, &["rev-parse", "HEAD"]).trim().to_string();
    let refs: Vec<String> = (0..3)
        .map(|i| format!("refs/fast-worktree/kept/old{i}-20200101-12000{i}/head"))
        .collect();
    for r in &refs {
        w.git(&m, &["update-ref", r, &head]);
    }
    // As when another git is writing one of them: the deletion of all of
    // them is one transaction, which then fails.
    let busy = m.join(".git").join(format!("{}.lock", refs[1]));
    fs::write(&busy, "").unwrap();
    let err = w.add("wt1");
    for r in &refs {
        assert!(w.kept_refs().contains(r.as_str()), "{r} is left");
        assert!(!err.contains(r.as_str()), "nothing was deleted: {err}");
    }
    let said: Vec<&str> = err
        .lines()
        .filter(|l| l.contains("could not be deleted"))
        .collect();
    assert_eq!(said.len(), 1, "{err}");
    assert!(said[0].contains("3 refs"), "{err}");
    // The next time (an hour later) they go.
    fs::remove_file(&busy).unwrap();
    w.forget_stamps();
    let err = w.add("wt2");
    assert!(w.kept_refs().trim().is_empty(), "{}", w.kept_refs());
    assert_eq!(err.lines().filter(|l| l.contains(": deleted ")).count(), 3);
}

#[test]
fn a_fill_deletes_no_kept_ref_and_leaves_that_to_the_next_command() {
    let w = World::new();
    let m = w.main();
    let head = w.git(&m, &["rev-parse", "HEAD"]).trim().to_string();
    let old = "refs/fast-worktree/kept/a-20200101-120000/head";
    w.git(&m, &["update-ref", old, &head]);
    let config = w.base.join("config/fast-worktree");
    fs::create_dir_all(&config).unwrap();
    fs::write(config.join("config.toml"), "[pool]\nsize = 1\n").unwrap();
    // The fill itself cannot do much here (there is no origin to fetch):
    // its sweep is what is looked at. Its output would be in a log file
    // that nobody reads.
    let mut c = Command::new(BIN);
    w.env(&mut c);
    let out = c.args(["pool", "fill"]).current_dir(&m).output().unwrap();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(w.kept_refs().contains(old), "{said}");
    assert!(!said.contains(old), "{said}");
    // It did sweep (the repository's directory of the cache is there),
    // and did not use up the hour of the part it left out.
    let repo = fs::read_dir(w.cache())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.join(".main").is_file())
        .expect("the fill made its sweep");
    assert!(!repo.join(".retention-stamp").exists());
    // The next command that somebody waits for deletes it, and says so.
    fs::remove_file(config.join("config.toml")).unwrap();
    let err = w.add("wt1");
    assert!(w.kept_refs().trim().is_empty(), "{}", w.kept_refs());
    assert!(err.contains(old) && err.contains("30 days passed"), "{err}");
}

#[test]
fn unused_entries_are_removed_and_nothing_else_in_the_cache_root() {
    let w = World::new();
    let proj = w.cache().join("gone-0123abcd/app");
    let old = entry(&proj, KEY, 8 * DAY);
    fs::write(proj.join(format!("{KEY}.src")), "/x").unwrap();
    let recent = entry(&proj, "fedcba9876543210-v20.1.0", 6 * DAY);
    // A half-made entry whose run is gone but which was made just now, and
    // one that a live run is cloning: its modification time is its
    // source's, months old.
    let young = proj.join(format!("{KEY}.tmp-{DEAD_PID}-1"));
    let live = proj.join(format!("{KEY}.tmp-{}-0", std::process::id()));
    for t in [&young, &live] {
        fs::create_dir_all(t.join("pkg")).unwrap();
        age(t, 200 * DAY);
    }
    // Other things in a cache root that is shared: a key-like directory
    // with no marker, names that are no key, a directory that is no
    // repository's.
    let unmarked = proj.join("aaaaaaaaaaaaaaaa-v20.1.0");
    let theirs = [
        unmarked.clone(),
        proj.join("node_modules"),
        proj.join("build.tmp-1-0"),
        w.cache().join("gone-0123abcd/deep/er/tree"),
        w.cache().join("photos/2019/summer"),
        w.cache().join("backup-0123abcd/home/me"),
    ];
    for d in &theirs {
        fs::create_dir_all(d).unwrap();
        age(d, 400 * DAY);
    }
    fs::write(proj.join("notes.src"), "theirs").unwrap();
    // A project that has nothing left but its lock file.
    let empty = w.cache().join("gone-0123abcd/lib");
    fs::create_dir_all(&empty).unwrap();
    fs::write(empty.join(".store.lock"), "").unwrap();
    let lock = inode(&empty.join(".store.lock"));

    w.add("wt1");

    wait_gone(&old);
    assert!(!old.exists(), "the old entry");
    assert!(!proj.join(format!("{KEY}.ok")).exists());
    assert!(!proj.join(format!("{KEY}.src")).exists());
    assert!(recent.is_dir(), "used within 7 days");
    assert!(young.is_dir(), "half made, too young to tell");
    assert!(live.is_dir(), "its owner is alive");
    for d in &theirs {
        assert!(d.is_dir(), "{} is not ours", d.display());
    }
    assert!(proj.join("notes.src").is_file());
    // The project directory stays with its lock file, the same file: an
    // older version may be about to lock that path.
    assert_eq!(inode(&empty.join(".store.lock")), lock);
    // `cache prune` applies the same rules.
    age(&recent, 8 * DAY);
    w.fw(&["cache", "prune"]);
    wait_gone(&recent);
    assert!(!recent.exists());
    for d in &theirs {
        assert!(d.is_dir(), "{} is not ours", d.display());
    }
    assert!(young.is_dir() && live.is_dir());
}

#[test]
fn the_cache_walk_is_made_once_an_hour_at_most() {
    let w = World::new();
    // The first run makes the cache directory; the second walks it.
    w.add("wt1");
    w.add("wt2");
    let proj = w.cache().join("gone-0123abcd/app");
    let e = entry(&proj, KEY, 8 * DAY);
    w.add("wt3");
    assert!(e.is_dir(), "the walk of this hour is done");
    age(&w.cache().join(".retention-stamp"), 3_601);
    w.add("wt4");
    wait_gone(&e);
    assert!(!e.exists());
}

#[test]
fn a_walk_moves_a_bounded_number_and_the_next_command_goes_on() {
    let w = World::new();
    let proj = w.cache().join("gone-0123abcd/app");
    let entries: Vec<PathBuf> = (0..230)
        .map(|i| entry(&proj, &format!("{i:016x}-v20.1.0"), 8 * DAY))
        .collect();
    let left = || entries.iter().filter(|e| e.exists()).count();
    w.add("wt1");
    assert_eq!(left(), 30, "one walk moves 200 at most");
    assert!(
        !w.cache().join(".retention-stamp").exists(),
        "work is left: the next command walks, not the next hour's"
    );
    w.add("wt2");
    assert_eq!(left(), 0);
    assert!(w.cache().join(".retention-stamp").is_file());
    // All of it is deleted in the end (by one deleter for each walk).
    let trash = w.cache().join("trash");
    let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let pending = || {
        fs::read_dir(&trash)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name() != ".lock")
            .count()
    };
    while pending() > 0 && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert_eq!(pending(), 0);
}

#[test]
fn pool_leftovers_of_killed_runs_and_old_misses_are_removed() {
    let w = World::new();
    w.add("wt1");
    let pool = w.repo_dir().join("pool");
    fs::create_dir_all(&pool).unwrap();
    fs::write(pool.join("fill.lock"), "").unwrap();
    let lock = inode(&pool.join("fill.lock"));
    let made = now() - 2 * 3_600;
    let me = std::process::id();
    // What a fill that is gone was making, and what a live one is making;
    // a spare taken by a process that is gone, and one taken by a live one.
    let building = pool.join(format!(".new-wt-{made}-{DEAD_PID}-0"));
    let hooks = pool.join(format!(".no-hooks-wt-{made}-{DEAD_PID}-0"));
    let in_work = pool.join(format!(".new-wt-{made}-{me}-0"));
    let claimed = pool.join(format!(".claimed-{DEAD_PID}-wt-{made}-1-1"));
    let live = pool.join(format!(".claimed-{me}-wt-{made}-1-2"));
    let other = pool.join(".something-else");
    for d in [&building, &in_work, &claimed, &live, &other] {
        fs::create_dir_all(d.join("sub")).unwrap();
    }
    fs::create_dir_all(&hooks).unwrap();
    let fresh = now();
    let stale_miss = fresh - 8 * DAY;
    fs::write(pool.join("misses"), format!("{stale_miss}\n{fresh}\n")).unwrap();
    let log = pool.join("fill.log");
    fs::write(&log, "old log").unwrap();
    age(&log, 8 * DAY);
    w.forget_stamps();

    // A fill may be running (its lock is held): the sweep does not need
    // the lock to tell a leftover, and does not take it.
    let fill = hold(&pool.join("fill.lock"));
    w.add("wt2");
    drop(fill);

    // What a run that is gone left goes too, but not within ten minutes
    // of when it was last touched: its pid says nothing about a fill in
    // another container that shares this cache. (The inode change time
    // cannot be set back from here: that an older one goes is in the unit
    // test of `sweep_leftovers_aged`.)
    assert!(building.is_dir() && claimed.is_dir() && hooks.is_dir());
    assert!(in_work.is_dir() && live.is_dir() && other.is_dir());
    // Whatever the pid says, one made more than a day ago goes.
    let ancient = pool.join(format!(".new-wt-{}-{me}-7", now() - 2 * DAY));
    fs::create_dir_all(ancient.join("sub")).unwrap();
    w.forget_stamps();
    w.add("wt3");
    wait_gone(&ancient);
    assert!(!ancient.exists());
    assert!(building.is_dir() && claimed.is_dir());
    assert_eq!(
        fs::read_to_string(pool.join("misses")).unwrap(),
        format!("{fresh}\n")
    );
    assert!(!log.exists());
    assert_eq!(inode(&pool.join("fill.lock")), lock);
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
fn a_sweep_starts_no_second_deleter_while_one_is_at_work() {
    if !cfg!(target_os = "macos") {
        return; // the deleters queue on the lock only there
    }
    let w = World::new();
    let trash = w.cache().join("trash");
    let orphan = trash.join(format!("{}-1-1-wt", now() - 700));
    fs::create_dir_all(orphan.join("sub")).unwrap();
    // A deleter is at work: it holds the trash's lock.
    let deleter = hold(&trash.join(".lock"));
    w.add("wt1");
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert!(orphan.is_dir(), "left for the sweep of a later command");
    // Not one more process that waits in the queue for each command.
    let ps = Command::new("ps")
        .args(["-axo", "command"])
        .output()
        .unwrap();
    let waiting = String::from_utf8_lossy(&ps.stdout)
        .lines()
        .filter(|l| l.contains(orphan.to_str().unwrap()))
        .count();
    assert_eq!(waiting, 0, "a deleter was started behind the one at work");
    drop(deleter);
    w.add("wt2");
    wait_gone(&orphan);
    assert!(!orphan.exists());
}

#[test]
fn lock_files_are_never_unlinked_while_their_repository_is_there() {
    // The versions from 0.4.0 on take each of these locks on the file they
    // opened and never look at the path again, and several versions run
    // side by side. Had a command unlinked the file while one of them
    // waited on it, that one and the next to come would both hold "the
    // lock". So after every operation each path must still name the same
    // file (the same inode: a file that was unlinked and made again would
    // pass a check that it exists).
    let w = World::new();
    w.add("wt0");
    let repo = w.repo_dir();
    let trash = w.cache().join("trash");
    fs::create_dir_all(repo.join("pool")).unwrap();
    fs::create_dir_all(repo.join("app")).unwrap();
    fs::create_dir_all(&trash).unwrap();
    let locks = [
        w.main().join(".git/fast-worktree-registration.lock"),
        repo.join("lifecycle.lock"),
        repo.join("pool/fill.lock"),
        repo.join("app/.store.lock"),
        trash.join(".lock"),
        // Of a repository that no command has run in since the upgrade.
        w.cache().join("idle-0123abcd/lifecycle.lock"),
        w.cache().join("idle-0123abcd/pool/fill.lock"),
        w.cache().join("idle-0123abcd/app/.store.lock"),
    ];
    for l in &locks {
        fs::create_dir_all(l.parent().unwrap()).unwrap();
        if !l.exists() {
            fs::write(l, "").unwrap();
        }
    }
    let before: Vec<u64> = locks.iter().map(|l| inode(l)).collect();
    #[track_caller]
    fn same(locks: &[PathBuf], before: &[u64], after: &str) {
        for (l, ino) in locks.iter().zip(before) {
            assert!(l.is_file(), "{} is gone after {after}", l.display());
            assert_eq!(inode(l), *ino, "{} was replaced after {after}", l.display());
        }
    }
    same(&locks, &before, "nothing");
    // Everything is old and empty: the most a sweep could want to remove.
    for l in &locks {
        age(l.parent().unwrap(), 30 * DAY);
    }
    w.forget_stamps();
    w.add("wt1");
    same(&locks, &before, "add");
    w.forget_stamps();
    w.fw(&["remove", w.base.join("wt1").to_str().unwrap()]);
    same(&locks, &before, "remove");
    w.forget_stamps();
    w.fw(&["cache", "prune"]);
    same(&locks, &before, "cache prune");
    w.fw(&["kept", "prune"]);
    w.fw(&["install"]);
    same(&locks, &before, "kept prune and install");
}

/// A repository directory of the cache for a main checkout at `main`,
/// with what the commands of that repository leave there. Returns it.
fn recorded_repo(w: &World, main: &Path) -> PathBuf {
    fs::create_dir_all(main).unwrap();
    w.git(main, &["init", "-q", "-b", "trunk"]);
    w.git(main, &["commit", "-q", "--allow-empty", "-m", "one"]);
    recorded_repo_at(w, main)
}

/// [`recorded_repo`] for a repository that is there already.
fn recorded_repo_at(w: &World, main: &Path) -> PathBuf {
    let mut c = Command::new(BIN);
    w.env(&mut c);
    let name = main.file_name().unwrap().to_str().unwrap();
    let wt = main.with_extension("wt");
    let out = c
        .args(["add", "-b", name, wt.to_str().unwrap(), "HEAD"])
        .current_dir(main)
        .output()
        .unwrap();
    assert!(out.status.success());
    let repo = fs::read_dir(w.cache())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.join(".main").is_file() && record(p).0 == main)
        .expect("a repository directory that records its main checkout");
    entry(&repo.join("app"), KEY, 60);
    fs::create_dir_all(repo.join("pool/wt-1-1-0/node_modules")).unwrap();
    fs::create_dir_all(repo.join("pool/.new-wt-1-1-1/sub")).unwrap();
    fs::create_dir_all(repo.join("pool/.something-else")).unwrap();
    fs::write(repo.join("pool/fill.log"), "a fill").unwrap();
    for l in ["lifecycle.lock", "pool/fill.lock", "app/.store.lock"] {
        fs::write(repo.join(l), "").unwrap();
    }
    repo
}

/// What `<repo>/.main` records: the main checkout, and the three device
/// numbers when it has them.
fn record(repo: &Path) -> (PathBuf, Option<Vec<u64>>) {
    use std::os::unix::ffi::OsStrExt;
    let bytes = fs::read(repo.join(".main")).unwrap();
    let mut parts = bytes.splitn(2, |b| *b == 0);
    let main = PathBuf::from(std::ffi::OsStr::from_bytes(parts.next().unwrap()));
    let devices = parts.next().map(|d| {
        String::from_utf8_lossy(d)
            .split(' ')
            .map(|n| n.parse().unwrap())
            .collect()
    });
    (main, devices)
}

fn set_record(repo: &Path, main: &Path, devices: Option<&[u64]>) {
    let mut bytes = main.as_os_str().as_encoded_bytes().to_vec();
    if let Some(d) = devices {
        let d: Vec<String> = d.iter().map(u64::to_string).collect();
        bytes.push(0);
        bytes.extend(d.join(" ").bytes());
    }
    fs::write(repo.join(".main"), bytes).unwrap();
}

/// As when no command ran in the repository of `repo` for `secs`.
fn unused_for(repo: &Path, secs: u64) {
    age(&repo.join(".main"), secs);
    age(&repo.join("pool"), secs);
}

/// `fast-worktree cache retire <repos>`: what the walk starts detached,
/// run here to its end.
fn retire(w: &World, repos: &[&PathBuf]) {
    let mut c = Command::new(BIN);
    w.env(&mut c);
    let out = c
        .args(["cache", "retire"])
        .args(repos)
        .current_dir(&w.base)
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
}

const WEEK: u64 = 7 * DAY;

#[test]
fn the_directory_of_a_repository_goes_only_when_it_is_unused_and_surely_gone() {
    let w = World::new();
    let repo = |name: &str| recorded_repo(&w, &w.base.join(name));
    // Deleted, and no command for 7 days: gone.
    let gone = repo("repos/gone");
    // Deleted an hour ago, used 6 days ago: it may be back tomorrow.
    let recent = repo("repos/recent");
    // Deleted and unused, but something holds one of its locks.
    let busy = repo("repos/busy");
    // Unused, but there.
    let there = repo("repos/there");
    // Unused, and the directory it was in is missing too: as when the
    // volume `/Volumes/X` is not mounted.
    let unmounted = repo("volume/away");
    // Unused and missing, and the directory it was in is on another
    // device than when a command last ran there: as when the disk that
    // was mounted at that directory is not mounted, a network or FUSE
    // mount is down, or the path is another machine's.
    let other_device = repo("repos/other-device");
    // It was itself the root of a mount (another device than the
    // directory it was in).
    let mount_root = repo("repos/mount-root");
    // A record of before the devices were written.
    let old_record = repo("repos/old-record");
    // One that only older versions wrote to: no record of its checkout.
    let unknown = w.cache().join("old-0123abcd");
    fs::create_dir_all(unknown.join("pool/wt-1-1-0")).unwrap();
    fs::write(unknown.join("pool/fill.lock"), "").unwrap();
    fs::write(unknown.join("lifecycle.lock"), "").unwrap();

    for name in [
        "repos/gone",
        "repos/recent",
        "repos/busy",
        "volume",
        "repos/other-device",
        "repos/mount-root",
        "repos/old-record",
    ] {
        fs::remove_dir_all(w.base.join(name)).unwrap();
    }
    let (main, devices) = record(&other_device);
    let d = devices.expect("the devices are recorded");
    assert_eq!(d.len(), 3);
    assert_eq!(d[0], d[1], "the checkout is on its directory's device");
    set_record(&other_device, &main, Some(&[d[0] + 1, d[0] + 1, d[2]]));
    let (main, _) = record(&mount_root);
    set_record(&mount_root, &main, Some(&[d[0], d[0] + 1, d[2]]));
    let (main, _) = record(&old_record);
    set_record(&old_record, &main, None);
    let all = [
        &gone,
        &recent,
        &busy,
        &there,
        &unmounted,
        &other_device,
        &mount_root,
        &old_record,
        &unknown,
    ];
    for r in all {
        if r.join(".main").is_file() {
            unused_for(r, WEEK + 60);
        }
    }
    unused_for(&recent, WEEK - 3_600);
    let held = hold(&busy.join("lifecycle.lock"));
    w.forget_stamps();
    retire(&w, &all);
    wait_gone(&gone);
    assert!(!gone.exists(), "entries, spares and locks: all of it");

    // Not gone, or not surely: the directory, its entries and its lock
    // files stay.
    for kept in [&recent, &busy, &there, &unmounted, &other_device] {
        assert!(kept.join("app").join(KEY).is_dir(), "{}", kept.display());
        assert!(kept.join("lifecycle.lock").is_file());
        assert!(kept.join("pool/fill.lock").is_file());
        assert!(kept.join(".main").is_file());
    }
    for kept in [&mount_root, &old_record] {
        assert!(kept.join("app").join(KEY).is_dir(), "{}", kept.display());
    }
    assert!(
        unknown.join("pool/wt-1-1-0").is_dir(),
        "no record: as it was"
    );
    assert!(unknown.join("lifecycle.lock").is_file());
    // In use, or busy: its spares too.
    for kept in [&recent, &busy] {
        assert!(kept.join("pool/wt-1-1-0").is_dir(), "{}", kept.display());
        assert!(kept.join("pool/fill.log").is_file());
        assert!(!kept.join(".retention-stamp").exists(), "asked again");
    }
    // Unused: the spares, what a fill left half made and its log go,
    // whether or not the checkout can be reached. Nothing else does.
    for emptied in [&there, &unmounted, &other_device, &mount_root, &old_record] {
        let pool = emptied.join("pool");
        wait_gone(&pool.join("wt-1-1-0"));
        let mut left: Vec<String> = fs::read_dir(&pool)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(
            left,
            [".something-else", "fill.lock"],
            "{}",
            emptied.display()
        );
        // Looked at: not again for a day.
        assert!(emptied.join(".retention-stamp").is_file());
    }
    // Once the lock is free, it goes.
    drop(held);
    retire(&w, &[&busy]);
    wait_gone(&busy);
    assert!(!busy.exists());
    // A command in the repository says that it is in use again.
    let (main, _) = record(&there);
    let mut c = Command::new(BIN);
    w.env(&mut c);
    let wt = main.with_extension("wt2");
    let out = c
        .args(["add", "-b", "y", wt.to_str().unwrap(), "HEAD"])
        .current_dir(&main)
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    fs::create_dir_all(there.join("pool/wt-1-1-0")).unwrap();
    age(&there.join("pool"), WEEK + 60);
    let _ = fs::remove_file(there.join(".retention-stamp"));
    retire(&w, &[&there]);
    assert!(there.join("pool/wt-1-1-0").is_dir());
}

#[test]
fn a_walk_hands_the_unused_repositories_to_a_detached_process_and_does_not_wait() {
    let w = World::new();
    let gone = recorded_repo(&w, &w.base.join("repos/gone"));
    let there = recorded_repo(&w, &w.base.join("repos/there"));
    let recent = recorded_repo(&w, &w.base.join("repos/recent"));
    fs::remove_dir_all(w.base.join("repos/gone")).unwrap();
    fs::remove_dir_all(w.base.join("repos/recent")).unwrap();
    unused_for(&gone, WEEK + 60);
    unused_for(&there, WEEK + 60);
    w.forget_stamps();
    w.add("wt1");
    wait_gone(&gone);
    assert!(!gone.exists());
    wait_gone(&there.join("pool/wt-1-1-0"));
    assert!(!there.join("pool/wt-1-1-0").exists());
    assert!(there.join("lifecycle.lock").is_file());
    assert!(recent.join("pool/wt-1-1-0").is_dir(), "used this week");
    // The one that stays is not handed over again for a day, however
    // often the cache is walked: its stamp is not renewed.
    let stamp = there.join(".retention-stamp");
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !stamp.exists() && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    age(&stamp, 3_600);
    let before = fs::metadata(&stamp).unwrap().modified().unwrap();
    fs::create_dir_all(there.join("pool/wt-1-1-0")).unwrap();
    age(&there.join("pool"), WEEK + 60);
    let _ = fs::remove_file(w.cache().join(".retention-stamp"));
    w.add("wt2");
    std::thread::sleep(std::time::Duration::from_millis(1_500));
    assert_eq!(fs::metadata(&stamp).unwrap().modified().unwrap(), before);
    assert!(there.join("pool/wt-1-1-0").is_dir());
    // A day after it was looked at, it is.
    age(&stamp, DAY + 60);
    let _ = fs::remove_file(w.cache().join(".retention-stamp"));
    w.add("wt3");
    wait_gone(&there.join("pool/wt-1-1-0"));
    assert!(!there.join("pool/wt-1-1-0").exists());
}

#[test]
fn the_pool_of_an_old_name_is_emptied_and_the_one_in_use_is_not_touched() {
    let w = World::new();
    // The checkout was at `old`, was moved to `new`, and a symlink was
    // put at the old path: its commands now use another directory of the
    // cache, and the one of the old path is left with a spare.
    let (old, new) = (w.base.join("repos/old"), w.base.join("repos/new"));
    let old_key = recorded_repo(&w, &old);
    let registered = spare(&w, &old, &old_key, "wt-1700000000-2000000000-0");
    fs::rename(&old, &new).unwrap();
    std::os::unix::fs::symlink(&new, &old).unwrap();
    let new_key = recorded_repo_at(&w, &new);
    assert_ne!(old_key, new_key);
    assert_eq!(locked(&w, &new), ["fast-worktree pool"]);
    unused_for(&old_key, WEEK + 60);
    retire(&w, &[&old_key, &new_key]);
    // The old name's spares go, as any spare is discarded: the files and
    // the registration, found through the symlink.
    wait_gone(&registered);
    assert!(!registered.exists() && !old_key.join("pool/wt-1-1-0").exists());
    assert_eq!(locked(&w, &new), Vec::<String>::new());
    // What the repository uses now is as it was.
    assert!(new_key.join("pool/wt-1-1-0").is_dir());
    assert!(new_key.join("pool/fill.log").is_file());
    // The old name's directory stays: the checkout is there.
    assert!(old_key.join("lifecycle.lock").is_file());
}

/// The worktrees that git lists as locked, with the reason of each.
fn locked(w: &World, main: &Path) -> Vec<String> {
    let mut v: Vec<String> = w
        .git(main, &["worktree", "list", "--porcelain"])
        .lines()
        .filter_map(|l| l.strip_prefix("locked"))
        .map(|l| l.trim().to_string())
        .collect();
    v.sort();
    v
}

/// A spare as a fill leaves it: a locked worktree in the pool of `repo`.
fn spare(w: &World, main: &Path, repo: &Path, id: &str) -> PathBuf {
    let path = repo.join("pool").join(id);
    let p = path.to_str().unwrap();
    w.git(main, &["worktree", "add", "-q", "--detach", p, "HEAD"]);
    w.git(
        main,
        &["worktree", "lock", "--reason", "fast-worktree pool", p],
    );
    path
}

#[test]
fn the_registrations_of_spares_that_went_with_a_moved_checkout_are_dropped() {
    let w = World::new();
    let first = w.base.join("repos/app");
    let old_key = recorded_repo(&w, &first);
    let ours = "fast-worktree pool".to_string();
    let a = spare(&w, &first, &old_key, "wt-1700000000-2000000000-0");
    // A worktree that the user locked, on a disk that is not plugged in.
    let users = w.base.join("users-wt");
    let u = users.to_str().unwrap();
    w.git(&first, &["worktree", "add", "-q", "--detach", u, "HEAD"]);
    w.git(
        &first,
        &["worktree", "lock", "--reason", "on my usb disk", u],
    );
    fs::remove_dir_all(&users).unwrap();
    // And one locked with a reason that only looks like ours.
    let near = w.base.join("near-wt");
    let n = near.to_str().unwrap();
    w.git(&first, &["worktree", "add", "-q", "--detach", n, "HEAD"]);
    w.git(
        &first,
        &["worktree", "lock", "--reason", "fast-worktree pool 2", n],
    );
    fs::remove_dir_all(&near).unwrap();
    let all = vec![
        ours.clone(),
        "fast-worktree pool 2".to_string(),
        "on my usb disk".to_string(),
    ];
    assert_eq!(locked(&w, &first), all);

    // The checkout is moved. Its commands now use another directory of
    // the cache; the spare is still where it was, registered.
    let second = w.base.join("repos/moved");
    fs::rename(&first, &second).unwrap();
    let add = |main: &Path, name: &str| {
        let mut c = Command::new(BIN);
        w.env(&mut c);
        let wt = w.base.join(name);
        let out = c
            .args(["add", "-b", name, wt.to_str().unwrap(), "HEAD"])
            .current_dir(main)
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
    };
    w.forget_stamps();
    add(&second, "m1");
    assert_eq!(locked(&w, &second), all, "its directory is there");
    // A week later the old path's directory of the cache goes, with the
    // spare: nothing is at the old path, and git cannot be told.
    unused_for(&old_key, WEEK + 60);
    retire(&w, &[&old_key]);
    wait_gone(&old_key);
    assert!(!old_key.exists() && !a.exists());
    assert_eq!(locked(&w, &second), all, "still registered, and locked");
    // The next command of the repository, where it is now, drops that
    // registration, and no other.
    w.forget_stamps();
    add(&second, "m2");
    assert_eq!(locked(&w, &second), all[1..]);
    assert!(!second
        .join(".git/worktrees/wt-1700000000-2000000000-0")
        .exists());

    // Moved back: the same for what is left under the old name.
    fs::rename(&second, &first).unwrap();
    w.forget_stamps();
    add(&first, "m3");
    assert!(old_key.is_dir(), "the old name is in use again");
    let b = spare(&w, &first, &old_key, "wt-1700000001-2000000000-0");
    // (Not a ready one: the pool is off here, and the spares of a pool
    // that is off are discarded by every command.)
    let making = format!(".new-wt-{}-{}-0", now(), std::process::id());
    let c = spare(&w, &first, &old_key, &making);
    let d = spare(&w, &first, &old_key, "wt-1700000003-2000000000-0");
    // One went without git being told (as when the repository was unused
    // and could not be reached); one is there, being made by a fill that
    // is alive; one is being taken by an `add` right now, renamed and not
    // yet repaired.
    fs::remove_dir_all(&b).unwrap();
    fs::rename(
        &d,
        old_key.join("pool/.claimed-2000000000-wt-1700000003-2000000000-0"),
    )
    .unwrap();
    w.forget_stamps();
    add(&first, "m4");
    let mut expected = vec![ours.clone(), ours.clone()];
    expected.extend(all[1..].iter().cloned());
    assert_eq!(locked(&w, &first), expected);
    assert!(c.is_dir());
    assert!(!first
        .join(".git/worktrees/wt-1700000001-2000000000-0")
        .exists());
    assert!(first
        .join(".git/worktrees/wt-1700000003-2000000000-0")
        .is_dir());
}

#[test]
fn a_walk_does_not_wait_for_a_store_lock_of_another_repository() {
    let w = World::new();
    let proj = w.cache().join("other-0123abcd/app");
    let e = entry(&proj, KEY, 8 * DAY);
    // Records without an entry, which are removed under the store lock.
    let orphan = proj.join("fedcba9876543210-v20.1.0.ok");
    fs::write(&orphan, "").unwrap();
    let held = hold(&proj.join(".store.lock"));
    let start = std::time::Instant::now();
    w.add("wt1");
    assert!(
        start.elapsed() < std::time::Duration::from_secs(20),
        "the walk waited for another repository's lock"
    );
    assert!(orphan.is_file(), "left for the next walk");
    wait_gone(&e);
    assert!(!e.exists(), "what needs no lock is done");
    assert!(!w.cache().join(".retention-stamp").exists(), "work is left");
    drop(held);
    w.add("wt2");
    assert!(!orphan.exists());
}
