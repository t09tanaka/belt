//! Who owns the entries under `<cache>/wt`, and reclaiming the ones whose
//! owner is gone.
//!
//! A run removes its worktree, its copies and its tools directory when it
//! ends. A run that is killed (`SIGKILL`, a crash, a power loss) cannot, and
//! each entry holds a checkout and whatever the jobs built in it. So a run
//! holds an exclusive `flock` on `wt/.owner-<pid>` for as long as it lives,
//! and every run starts by removing the entries whose owner is provably gone:
//!
//! - the owner file exists and can be locked: the run that made it is dead;
//! - there is no owner file (the entry was made by a version that had none)
//!   and no process has the pid the name ends with.
//!
//! The age of an entry is never looked at, and an entry of a run that is alive
//! (in this process or another) is never touched. Entries kept with `--keep`
//! are marked in the owner file and stay until `local-ci cache prune --kept`.
//!
//! The sweep, the registration of a run and its release happen under one lock
//! of the cache (`wt/.lock`), so a new run can never take an owner
//! file that a sweep is about to remove.

use crate::context::ExecContext;
use anyhow::{Context, Result};
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const KEPT: &str = "kept";

/// The owner files this process holds: more than one run can be going on in
/// one process (the library, the tests), and they share the pid.
struct Held {
    file: File,
    runs: usize,
    keep: bool,
    /// Entries with this pid that were there before: kept by a dead run that
    /// had the same pid. They are not this process's to remove.
    before: BTreeSet<OsString>,
}

static HELD: Mutex<Option<HashMap<PathBuf, Held>>> = Mutex::new(None);

fn held<T>(f: impl FnOnce(&mut HashMap<PathBuf, Held>) -> T) -> T {
    let mut g = HELD.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(HashMap::new))
}

fn owner_file(cache: &Path, pid: u32) -> PathBuf {
    cache.join("wt").join(format!(".owner-{pid}"))
}

/// The lock of the cache, held while entries are swept and owners come and go.
fn cache_lock(cache: &Path) -> Result<File> {
    let dir = cache.join("wt");
    std::fs::create_dir_all(&dir)?;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(".lock"))
        .context("cannot open the cache lock")?;
    f.lock_exclusive()?;
    Ok(f)
}

/// The pid in the name of an entry, for the names local-ci gives and no
/// others: `<7 hex of the commit>-<pid>`, `…-u<n>-<pid>` and `…-c<n>-<pid>`,
/// each also with `.tools`. Anything else in `wt` is not local-ci's to
/// remove. Only a pid that can be a process of its own is accepted: never 0
/// or 1, never more than `i32::MAX`.
pub fn entry_pid(name: &str) -> Option<u32> {
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let base = name.strip_suffix(".tools").unwrap_or(name);
    let mut parts = base.split('-');
    let sha = parts.next()?;
    if sha.len() != 7 || !sha.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let pid = match (parts.next()?, parts.next(), parts.next()) {
        (pid, None, None) => pid,
        (copy, Some(pid), None) if copy.strip_prefix(['u', 'c']).is_some_and(digits) => pid,
        _ => return None,
    };
    if !digits(pid) {
        return None;
    }
    pid.parse::<u32>()
        .ok()
        .filter(|p| (2..=i32::MAX as u32).contains(p))
}

/// Whether a process with this pid exists. No signal is sent (signal 0 only
/// asks), and only for a pid [`entry_pid`] accepts.
fn pid_exists(pid: u32) -> bool {
    if !(2..=i32::MAX as u32).contains(&pid) {
        return true;
    }
    // SAFETY: signal 0 delivers nothing; the pid is greater than 1.
    let r = unsafe { libc::kill(pid as i32, 0) };
    r == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// What is known about the owner of an entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Its run is alive, or cannot be shown to be gone.
    InUse,
    /// Its run ended with `--keep`.
    Kept,
    /// Its run is gone without removing it.
    Stale,
    /// The name does not say whose it is; it is left alone.
    Unknown,
}

/// One entry of `<cache>/wt`.
#[derive(Clone, Debug)]
pub struct Entry {
    pub name: OsString,
    pub state: State,
}

fn owner_state(cache: &Path, pid: u32) -> State {
    let path = owner_file(cache, pid);
    if held(|h| h.contains_key(&path)) {
        return State::InUse;
    }
    match std::fs::symlink_metadata(&path) {
        Ok(m) if m.is_file() => {}
        // not a file local-ci wrote: nothing is proven
        Ok(_) => return State::InUse,
        Err(_) => {
            // This process holds nothing here (seen above), so an entry with
            // its own pid was left by a dead process that had the pid before.
            return if pid != std::process::id() && pid_exists(pid) {
                State::InUse
            } else {
                State::Stale
            };
        }
    }
    let Ok(mut f) = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    else {
        return State::InUse;
    };
    if f.try_lock_exclusive().is_err() {
        return State::InUse;
    }
    let mut text = String::new();
    let _ = f.read_to_string(&mut text);
    // Unlocked, not only closed: a child another thread is starting holds a
    // copy of the descriptor until its exec, and the lock with it.
    let _ = FileExt::unlock(&f);
    if text.trim() == KEPT {
        State::Kept
    } else {
        State::Stale
    }
}

/// The entries of `<cache>/wt` and the pids that have an owner file. Call
/// with the cache lock held.
fn scan(cache: &Path) -> (Vec<Entry>, BTreeSet<u32>) {
    let mut states: BTreeMap<u32, State> = BTreeMap::new();
    let mut entries = Vec::new();
    let mut owners = BTreeSet::new();
    for e in std::fs::read_dir(cache.join("wt"))
        .into_iter()
        .flatten()
        .flatten()
    {
        let name = e.file_name();
        let text = name.to_string_lossy();
        if let Some(pid) = text.strip_prefix(".owner-") {
            if let Ok(pid) = pid.parse::<u32>() {
                owners.insert(pid);
            }
            continue;
        }
        if text.starts_with('.') {
            // the lock of the cache, and nothing a run works in
            continue;
        }
        let state = match entry_pid(&text) {
            Some(pid) => *states.entry(pid).or_insert_with(|| owner_state(cache, pid)),
            None => State::Unknown,
        };
        entries.push(Entry { name, state });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    (entries, owners)
}

/// The entries whose name ends with `pid`.
fn names_of(cache: &Path, pid: u32) -> BTreeSet<OsString> {
    std::fs::read_dir(cache.join("wt"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name())
        .filter(|n| entry_pid(&n.to_string_lossy()) == Some(pid))
        .collect()
}

/// The git directory of the repository `entry` is registered in as a
/// worktree, when its `.git` file and the registration point at each other.
fn registered_in(entry: &Path) -> Option<PathBuf> {
    let dot_git = entry.join(".git");
    let meta = std::fs::symlink_metadata(&dot_git).ok()?;
    if !meta.is_file() || meta.len() > 4096 {
        return None;
    }
    let text = std::fs::read_to_string(&dot_git).ok()?;
    let admin = PathBuf::from(text.strip_prefix("gitdir: ")?.trim_end());
    let worktrees = admin.parent()?;
    if !admin.is_absolute() || worktrees.file_name()? != "worktrees" {
        return None;
    }
    // git records resolved paths; the cache may be reached through a link
    let back = std::fs::read_to_string(admin.join("gitdir")).ok()?;
    let back = std::fs::canonicalize(Path::new(back.trim_end()).parent()?).ok()?;
    if back != std::fs::canonicalize(entry).ok()? {
        return None;
    }
    worktrees.parent().map(Path::to_path_buf)
}

/// Take the entries out of `wt` and drop their registrations. A link is
/// unlinked, never followed; a directory is moved to the trash, which the
/// caller empties.
fn discard(ctx: &ExecContext, cache: &Path, names: &[OsString]) {
    let mut repos = BTreeSet::new();
    for name in names {
        let path = cache.join("wt").join(name);
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_dir() {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        repos.extend(registered_in(&path));
        if crate::git::move_to_trash(&path, &cache.join("trash")).is_none() {
            // remove_dir_all does not follow links either
            let _ = std::fs::remove_dir_all(&path);
        }
    }
    for repo in repos {
        if repo.is_dir() {
            let _ = crate::git::prune_worktrees(ctx, &repo);
        }
    }
}

/// What a sweep did and what it left.
#[derive(Clone, Debug, Default)]
pub struct Swept {
    /// The entries taken out of `wt`.
    pub removed: Vec<OsString>,
    pub in_use: usize,
    pub kept: usize,
    pub unknown: usize,
}

fn sweep_locked(ctx: &ExecContext, cache: &Path, kept_too: bool) -> Swept {
    let (entries, owners) = scan(cache);
    let mut out = Swept::default();
    let mut used = BTreeSet::new();
    for e in entries {
        let gone = match e.state {
            State::Stale => true,
            State::Kept => kept_too,
            State::InUse | State::Unknown => false,
        };
        if gone {
            out.removed.push(e.name);
            continue;
        }
        used.extend(entry_pid(&e.name.to_string_lossy()));
        match e.state {
            State::Kept => out.kept += 1,
            State::Unknown => out.unknown += 1,
            _ => out.in_use += 1,
        }
    }
    discard(ctx, cache, &out.removed);
    // an owner file with no entry left and no run behind it
    for pid in owners {
        if !used.contains(&pid) && owner_state(cache, pid) != State::InUse {
            let _ = std::fs::remove_file(owner_file(cache, pid));
        }
    }
    out
}

/// A run's hold on the entries it makes under `<cache>/wt`. Dropping it
/// removes what the run left there, on every way out of the run.
pub struct Owner<'a> {
    ctx: &'a ExecContext,
    cache: PathBuf,
    path: PathBuf,
}

/// Remove the entries of runs that are gone, then register this process as
/// the owner of the entries it is about to make. Call before making any.
pub fn enter<'a>(ctx: &'a ExecContext, cache: &Path) -> Result<Owner<'a>> {
    std::fs::create_dir_all(cache.join("wt"))?;
    let _lock = cache_lock(cache)?;
    sweep_locked(ctx, cache, false);
    crate::git::empty_trash(ctx, &cache.join("trash"));
    let pid = std::process::id();
    let path = owner_file(cache, pid);
    // what the sweep left with this pid was kept by a run before
    let before = names_of(cache, pid);
    held(|h| -> Result<()> {
        if let Some(e) = h.get_mut(&path) {
            e.runs += 1;
            return Ok(());
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .context("cannot record the owner of the worktrees")?;
        // no other process has this pid, and the sweep is done: at most a
        // child between its fork and its exec holds a copy for a moment
        file.lock_exclusive()
            .context("cannot lock the owner file of the worktrees")?;
        // until this run ends, a mark left by a run before is not its own
        let _ = file.set_len(0);
        h.insert(
            path.clone(),
            Held {
                file,
                runs: 1,
                keep: false,
                before,
            },
        );
        Ok(())
    })?;
    Ok(Owner {
        ctx,
        cache: cache.to_path_buf(),
        path,
    })
}

impl Owner<'_> {
    /// The run ends with `--keep`: its entries stay, marked as kept.
    pub fn keep(&self) {
        held(|h| {
            if let Some(e) = h.get_mut(&self.path) {
                e.keep = true;
            }
        });
    }
}

impl Drop for Owner<'_> {
    fn drop(&mut self) {
        let _lock = cache_lock(&self.cache);
        let last = held(|h| {
            let e = h.get_mut(&self.path)?;
            e.runs -= 1;
            if e.runs == 0 {
                h.remove(&self.path)
            } else {
                None
            }
        });
        let Some(mut last) = last else {
            return;
        };
        let pid = std::process::id();
        let mine: Vec<OsString> = names_of(&self.cache, pid).into_iter().collect();
        // Nothing is left when the run removed its worktrees itself; what is
        // (a copy of a thread that panicked, a removal that failed) goes now.
        let mut left = !last.keep && !mine.is_empty();
        if last.keep || mine.iter().any(|n| last.before.contains(n)) {
            if !last.keep {
                let mine: Vec<OsString> = mine
                    .into_iter()
                    .filter(|n| !last.before.contains(n))
                    .collect();
                left = !mine.is_empty();
                discard(self.ctx, &self.cache, &mine);
            }
            let _ = last.file.set_len(0);
            let _ = last.file.rewind();
            let _ = last.file.write_all(KEPT.as_bytes());
        } else {
            discard(self.ctx, &self.cache, &mine);
            let _ = std::fs::remove_file(&self.path);
        }
        if left {
            crate::git::empty_trash(self.ctx, &self.cache.join("trash"));
        }
        // unlocked, not only closed (see `owner_state`)
        let _ = FileExt::unlock(&last.file);
    }
}

/// Remove the entries of runs that are gone (and the kept ones with
/// `kept_too`) and delete everything in the trash before returning.
pub fn prune(ctx: &ExecContext, cache: &Path, kept_too: bool) -> Result<Swept> {
    let lock = cache_lock(cache)?;
    let swept = sweep_locked(ctx, cache, kept_too);
    // the deletion can take long, and runs must not wait for it
    drop(lock);
    crate::git::empty_trash_now(&cache.join("trash"));
    Ok(swept)
}

/// The entries of `<cache>/wt` and what is known of their owners.
pub fn entries(cache: &Path) -> Result<Vec<Entry>> {
    let _lock = cache_lock(cache)?;
    Ok(scan(cache).0)
}

/// The bytes the files under `path` take on disk. Links are counted as
/// links: what they point at is not looked at.
pub fn disk_usage(path: &Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    let own = meta.blocks() * 512;
    if !meta.is_dir() {
        return own;
    }
    own + std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| disk_usage(&e.path()))
        .sum::<u64>()
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests build their own commands
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    fn ctx() -> ExecContext {
        ExecContext::new(
            [
                ("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"),
                ("HOME", "/tmp"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        )
    }

    /// The pid of a process this test started and reaped: no process has it.
    fn dead_pid() -> u32 {
        let mut child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        pid
    }

    struct Cache {
        tmp: tempfile::TempDir,
    }

    impl Cache {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(tmp.path().join("cache/wt")).unwrap();
            Cache { tmp }
        }
        fn dir(&self) -> PathBuf {
            self.tmp.path().join("cache")
        }
        fn wt(&self, name: &str) -> PathBuf {
            self.dir().join("wt").join(name)
        }
        /// A worktree-like entry with a build directory in it.
        fn entry(&self, name: &str) -> PathBuf {
            let p = self.wt(name);
            std::fs::create_dir_all(p.join("target/debug")).unwrap();
            std::fs::write(p.join("target/debug/bin"), "built").unwrap();
            p
        }
        /// The names in `wt`, owner files left out.
        fn names(&self) -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(self.dir().join("wt"))
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|n| !n.starts_with('.'))
                .collect();
            v.sort();
            v
        }
        fn trash(&self) -> Vec<String> {
            std::fs::read_dir(self.dir().join("trash"))
                .into_iter()
                .flatten()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|n| n != ".lock")
                .collect()
        }
        fn state(&self, name: &str) -> State {
            entries(&self.dir())
                .unwrap()
                .into_iter()
                .find(|e| e.name.to_string_lossy() == name)
                .unwrap()
                .state
        }
    }

    #[test]
    fn the_pid_of_an_entry_is_one_a_process_of_its_own_can_have() {
        assert_eq!(entry_pid("abc1234-4242"), Some(4242));
        assert_eq!(entry_pid("abc1234-u3-4242"), Some(4242));
        assert_eq!(entry_pid("abc1234-c12-4242"), Some(4242));
        assert_eq!(entry_pid("abc1234-4242.tools"), Some(4242));
        assert_eq!(entry_pid("abc1234-2"), Some(2));
        assert_eq!(entry_pid("abc1234-2147483647"), Some(i32::MAX as u32));
        for name in [
            "abc1234-0",
            "abc1234-1",
            "abc1234-2147483648",
            "abc1234-99999999999999999999",
            "abc1234-",
            "abc1234-12x",
            "backup-2024",
            "abcdefg-4242",
            "abc123-4242",
            "abc12345-4242",
            "ABC1234-4242",
            "abc1234-x1-4242",
            "abc1234-u-4242",
            "abc1234-u1-c2-4242",
            "pinned-ab12cd-4242-3",
            "abc1234-4242-3",
            "abc1234-+12",
            "-4242",
            "4242",
            ".owner-4242",
            "abc1234-4242.other",
        ] {
            assert_eq!(entry_pid(name), None, "{name}");
        }
        // asking about a pid outside that range never reaches kill()
        for pid in [0, 1, i32::MAX as u32 + 1, u32::MAX] {
            assert!(pid_exists(pid), "{pid}");
        }
    }

    #[test]
    fn entries_of_a_dead_process_are_reclaimed_and_those_of_a_live_one_kept() {
        let c = Cache::new();
        /// Ends the process when the test ends, however it ends.
        struct Live(std::process::Child);
        impl Drop for Live {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut live = Live(
            Command::new("sleep")
                .arg("600")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let (alive, dead) = (live.0.id(), dead_pid());
        for pid in [alive, dead] {
            c.entry(&format!("abc1234-{pid}"));
            c.entry(&format!("abc1234-{pid}.tools"));
            c.entry(&format!("abc1234-u1-{pid}"));
            c.entry(&format!("abc1234-c2-{pid}"));
        }
        c.entry("notes");
        c.entry("backup-2024");
        assert_eq!(c.state(&format!("abc1234-{alive}")), State::InUse);
        assert_eq!(c.state(&format!("abc1234-u1-{dead}")), State::Stale);
        assert_eq!(c.state("notes"), State::Unknown);

        let swept = prune(&ctx(), &c.dir(), false).unwrap();
        assert_eq!(swept.removed.len(), 4);
        assert_eq!((swept.in_use, swept.kept, swept.unknown), (4, 0, 2));
        assert_eq!(
            c.names(),
            [
                format!("abc1234-{alive}"),
                format!("abc1234-{alive}.tools"),
                format!("abc1234-c2-{alive}"),
                format!("abc1234-u1-{alive}"),
                "backup-2024".to_string(),
                "notes".to_string(),
            ]
        );
        assert!(c.trash().is_empty(), "prune deletes before it returns");

        // the same entries once their process is gone
        live.0.kill().unwrap();
        live.0.wait().unwrap();
        prune(&ctx(), &c.dir(), false).unwrap();
        assert_eq!(c.names(), ["backup-2024", "notes"]);
    }

    #[test]
    fn a_held_owner_lock_protects_entries_whatever_the_pid_says() {
        let c = Cache::new();
        let pid = dead_pid();
        let name = format!("abc1234-{pid}");
        c.entry(&name);
        let owner = owner_file(&c.dir(), pid);
        std::fs::write(&owner, "").unwrap();
        // a run in another process holds its owner file like this
        let holder = File::open(&owner).unwrap();
        holder.lock_exclusive().unwrap();
        assert_eq!(c.state(&name), State::InUse);
        assert!(prune(&ctx(), &c.dir(), true).unwrap().removed.is_empty());
        assert_eq!(c.names(), std::slice::from_ref(&name));
        assert!(owner.exists());

        // the run died: the kernel dropped its lock
        FileExt::unlock(&holder).unwrap();
        drop(holder);
        assert_eq!(c.state(&name), State::Stale);
        assert_eq!(prune(&ctx(), &c.dir(), false).unwrap().removed.len(), 1);
        assert!(c.names().is_empty());
        assert!(!owner.exists(), "the owner file goes with its entries");
    }

    #[test]
    fn kept_entries_stay_until_they_are_asked_for() {
        let c = Cache::new();
        let pid = dead_pid();
        let name = format!("abc1234-{pid}");
        c.entry(&name);
        std::fs::write(owner_file(&c.dir(), pid), KEPT).unwrap();
        assert_eq!(c.state(&name), State::Kept);
        let swept = prune(&ctx(), &c.dir(), false).unwrap();
        assert_eq!((swept.removed.len(), swept.kept), (0, 1));
        assert_eq!(c.names(), [name]);
        assert_eq!(prune(&ctx(), &c.dir(), true).unwrap().removed.len(), 1);
        assert!(c.names().is_empty());
        assert!(!owner_file(&c.dir(), pid).exists());
    }

    #[test]
    fn links_are_removed_and_never_followed() {
        let c = Cache::new();
        let outside = c.tmp.path().join("outside");
        std::fs::create_dir_all(outside.join("sub")).unwrap();
        std::fs::write(outside.join("sub/precious"), "data").unwrap();
        let pid = dead_pid();
        // an entry that is itself a link out of the cache
        std::os::unix::fs::symlink(&outside, c.wt(&format!("abc1234-{pid}"))).unwrap();
        // links inside an entry, to a directory and to a file
        let e = c.entry(&format!("abc1234-u1-{pid}"));
        std::os::unix::fs::symlink(&outside, e.join("out-dir")).unwrap();
        std::os::unix::fs::symlink(outside.join("sub/precious"), e.join("target/out-file"))
            .unwrap();
        // and in what an earlier deletion left in the trash
        let old = c.dir().join("trash/old");
        std::fs::create_dir_all(&old).unwrap();
        std::os::unix::fs::symlink(&outside, old.join("out-dir")).unwrap();
        std::os::unix::fs::symlink(&outside, c.dir().join("trash/link")).unwrap();

        assert_eq!(
            disk_usage(&e.join("out-dir")),
            disk_usage(&e.join("target/out-file"))
        );
        assert_eq!(prune(&ctx(), &c.dir(), false).unwrap().removed.len(), 2);
        assert!(c.names().is_empty());
        assert!(c.trash().is_empty());
        assert_eq!(
            std::fs::read_to_string(outside.join("sub/precious")).unwrap(),
            "data"
        );
    }

    #[test]
    fn the_trash_is_emptied_even_when_a_directory_cannot_be_written() {
        use std::os::unix::fs::PermissionsExt;
        let c = Cache::new();
        let sealed = c.dir().join("trash/old/mod-cache");
        std::fs::create_dir_all(&sealed).unwrap();
        std::fs::write(sealed.join("file"), "x").unwrap();
        std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o500)).unwrap();
        prune(&ctx(), &c.dir(), false).unwrap();
        assert!(c.trash().is_empty());
    }

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(["-c", "user.email=t@example.com", "-c", "user.name=t"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}");
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    #[test]
    fn a_reclaimed_worktree_is_deregistered_from_its_repository() {
        let c = Cache::new();
        let repo = c.tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        git(&repo, &["init", "-q"]);
        std::fs::write(repo.join("a.txt"), "a").unwrap();
        git(&repo, &["add", "a.txt"]);
        git(&repo, &["commit", "-qm", "a"]);
        let pid = dead_pid();
        for name in [format!("abc1234-{pid}"), format!("abc1234-u1-{pid}")] {
            let wt = c.wt(&name);
            git(
                &repo,
                &["worktree", "add", "--detach", "-q", wt.to_str().unwrap()],
            );
        }
        assert_eq!(git(&repo, &["worktree", "list"]).lines().count(), 3);
        prune(&ctx(), &c.dir(), false).unwrap();
        assert!(c.names().is_empty());
        assert_eq!(git(&repo, &["worktree", "list"]).lines().count(), 1);
    }

    #[test]
    fn a_run_removes_what_it_made_when_it_ends_and_is_not_swept_while_it_lives() {
        let c = Cache::new();
        let (ctx, me) = (ctx(), std::process::id());
        let stale = format!("abc1234-{}", dead_pid());
        c.entry(&stale);
        let owner = enter(&ctx, &c.dir()).unwrap();
        assert!(c.names().is_empty(), "entering sweeps what dead runs left");
        let names = [
            format!("abc1234-c1-{me}"),
            format!("abc1234-u1-{me}"),
            format!("abc1234-{me}"),
            format!("abc1234-{me}.tools"),
        ];
        for n in &names {
            c.entry(n);
        }
        // a second run in this process, and a prune beside them
        let second = enter(&ctx, &c.dir()).unwrap();
        assert!(prune(&ctx, &c.dir(), true).unwrap().removed.is_empty());
        assert_eq!(c.state(&names[0]), State::InUse);
        drop(second);
        assert_eq!(c.names().len(), 4, "the first run still holds them");
        drop(owner);
        assert!(c.names().is_empty());
        assert!(!owner_file(&c.dir(), me).exists());
    }

    #[test]
    fn a_run_that_panics_still_removes_what_it_made() {
        let c = Cache::new();
        let (ctx, me) = (ctx(), std::process::id());
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _owner = enter(&ctx, &c.dir()).unwrap();
            c.entry(&format!("abc1234-u1-{me}"));
            panic!("a job thread failed");
        }));
        assert!(r.is_err());
        assert!(c.names().is_empty());
    }

    #[test]
    fn keep_leaves_the_entries_marked_and_a_later_run_with_the_same_pid_leaves_them() {
        let c = Cache::new();
        let (ctx, me) = (ctx(), std::process::id());
        let kept = format!("abc1234-{me}");
        let owner = enter(&ctx, &c.dir()).unwrap();
        c.entry(&kept);
        owner.keep();
        drop(owner);
        assert_eq!(c.names(), std::slice::from_ref(&kept));
        assert_eq!(c.state(&kept), State::Kept);

        let owner = enter(&ctx, &c.dir()).unwrap();
        c.entry(&format!("def5678-{me}"));
        assert_eq!(c.state(&kept), State::InUse);
        drop(owner);
        assert_eq!(c.names(), std::slice::from_ref(&kept));
        assert_eq!(c.state(&kept), State::Kept);
        prune(&ctx, &c.dir(), true).unwrap();
        assert!(c.names().is_empty());
    }

    #[test]
    fn entering_starts_the_deletion_of_what_was_left_in_the_trash() {
        let c = Cache::new();
        let outside = c.tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("precious"), "data").unwrap();
        let old = c.dir().join("trash/abc1234-4242.77.0");
        std::fs::create_dir_all(old.join("target")).unwrap();
        std::os::unix::fs::symlink(&outside, old.join("out")).unwrap();
        let stale = c.entry(&format!("abc1234-{}", dead_pid()));
        std::os::unix::fs::symlink(&outside, stale.join("out")).unwrap();
        drop(enter(&ctx(), &c.dir()).unwrap());
        assert!(c.names().is_empty());
        // the deletion runs in the background
        let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !c.trash().is_empty() && std::time::Instant::now() < until {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(c.trash().is_empty());
        assert_eq!(
            std::fs::read_to_string(outside.join("precious")).unwrap(),
            "data"
        );
    }
}
