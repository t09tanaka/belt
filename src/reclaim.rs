//! Who owns the entries under `<cache>/wt`, and reclaiming the ones whose
//! owner is gone.
//!
//! A run removes its worktree, its copies and its tools directory when it
//! ends. A run that is killed (`SIGKILL`, a crash, a power loss) cannot, and
//! each entry holds a checkout and whatever the jobs built in it. So a run
//! holds an exclusive `flock` on `wt/.owner-<pid>` for as long as it lives,
//! and every run starts by removing the entries whose owner is provably gone:
//! the owner file of their pid exists and can be locked, so the run that made
//! it is dead.
//!
//! Nothing else is removed by a run:
//!
//! - an entry kept with `--keep` has a mark of its own, `wt/.kept-<entry>`,
//!   and stays until `local-ci cache prune --kept`. The mark belongs to the
//!   entry, not to a pid: a later run that gets the same pid cannot lose it;
//! - an entry with no owner file was made by a version that recorded none,
//!   and may have been kept on purpose. It stays until
//!   `local-ci cache prune --legacy`, which removes it once no process has
//!   the pid in its name. A run that gets that pid marks the entry
//!   (`wt/.legacy-<entry>`) before it writes its own owner file, so the entry
//!   is never taken for one of that run;
//! - the age of an entry is never looked at, and an entry of a run that is
//!   alive (in this process or another) is never touched.
//!
//! The sweep, the registration of a run and its release happen under one lock
//! of the cache (`wt/.lock`), so a new run can never take an owner
//! file that a sweep is about to remove. Under that lock entries are only
//! renamed out of the way; `git worktree prune` and any deletion happen once
//! it is released.

use crate::context::ExecContext;
use anyhow::{bail, Context, Result};
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The prefix of the mark of an entry kept with `--keep`.
const KEPT: &str = ".kept-";
/// The prefix of the mark of an entry that had no owner file when a run with
/// the same pid started.
const LEGACY: &str = ".legacy-";
/// The prefix of an entry that could not be moved to the trash and waits in
/// `wt` to be deleted.
const GONE: &str = ".gone-";

/// The owner files this process holds: more than one run can be going on in
/// one process (the library, the tests), and they share the pid.
struct Held {
    file: File,
    runs: usize,
    keep: bool,
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
    if !cache.is_absolute() {
        bail!(
            "the cache directory `{}` is not an absolute path",
            cache.display()
        );
    }
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
    /// It has no owner record (a version before them made it) and no process
    /// has its pid. No run removes it; `cache prune --legacy` does.
    Legacy,
    /// The name does not say whose it is; it is left alone.
    Unknown,
}

/// One entry of `<cache>/wt`.
#[derive(Clone, Debug)]
pub struct Entry {
    pub name: OsString,
    pub state: State,
}

fn mark_path(cache: &Path, prefix: &str, entry: &OsStr) -> PathBuf {
    let mut name = OsString::from(prefix);
    name.push(entry);
    cache.join("wt").join(name)
}

fn has_mark(cache: &Path, prefix: &str, entry: &OsStr) -> bool {
    std::fs::symlink_metadata(mark_path(cache, prefix, entry)).is_ok_and(|m| m.is_file())
}

fn is_marked(cache: &Path, entry: &OsStr) -> bool {
    has_mark(cache, KEPT, entry) || has_mark(cache, LEGACY, entry)
}

fn set_mark(cache: &Path, prefix: &str, entry: &OsStr) -> bool {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(mark_path(cache, prefix, entry))
        .is_ok()
}

/// What the owner file of `pid` says of the entries that have no mark.
fn owner_state(cache: &Path, pid: u32) -> State {
    let path = owner_file(cache, pid);
    if held(|h| h.contains_key(&path)) {
        return State::InUse;
    }
    match std::fs::symlink_metadata(&path) {
        Ok(m) if m.is_file() => {}
        // not a file local-ci wrote: nothing is proven
        Ok(_) => return State::InUse,
        // no record of an owner: never a run's to remove
        Err(_) => {
            return if pid_exists(pid) {
                State::InUse
            } else {
                State::Legacy
            };
        }
    }
    let Ok(f) = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    else {
        return State::InUse;
    };
    if f.try_lock_exclusive().is_err() {
        return State::InUse;
    }
    // Unlocked, not only closed: a child another thread is starting holds a
    // copy of the descriptor until its exec, and the lock with it.
    let _ = FileExt::unlock(&f);
    State::Stale
}

/// What is in `<cache>/wt`.
#[derive(Default)]
struct Scan {
    entries: Vec<Entry>,
    /// The pids that have an owner file.
    owners: BTreeSet<u32>,
    /// The marks, and the entry each is the mark of.
    marks: Vec<(PathBuf, OsString)>,
    /// What a removal that could not use the trash left to delete.
    gone: Vec<PathBuf>,
}

/// Call with the cache lock held.
fn scan(cache: &Path) -> Scan {
    let mut states: BTreeMap<u32, State> = BTreeMap::new();
    let mut out = Scan::default();
    let wt = cache.join("wt");
    if !crate::git::is_real_dir(&wt) {
        return out;
    }
    for e in std::fs::read_dir(&wt).into_iter().flatten().flatten() {
        let name = e.file_name();
        let text = name.to_string_lossy();
        if let Some(pid) = text.strip_prefix(".owner-") {
            if let Ok(pid) = pid.parse::<u32>() {
                out.owners.insert(pid);
            }
            continue;
        }
        if let Some(entry) = [KEPT, LEGACY].iter().find_map(|p| text.strip_prefix(p)) {
            if entry_pid(entry).is_some() {
                out.marks.push((e.path(), OsString::from(entry)));
            }
            continue;
        }
        if let Some(moved) = text.strip_prefix(GONE) {
            if crate::git::is_moved_name(OsStr::new(moved)) {
                out.gone.push(e.path());
            }
            continue;
        }
        if text.starts_with('.') {
            // the lock of the cache, and nothing a run works in
            continue;
        }
        let state = match entry_pid(&text) {
            None => State::Unknown,
            Some(_) if has_mark(cache, KEPT, &name) => State::Kept,
            // the pid is that of a run that started after the entry was made
            Some(pid) if has_mark(cache, LEGACY, &name) => {
                if pid_exists(pid) {
                    State::InUse
                } else {
                    State::Legacy
                }
            }
            Some(pid) => *states.entry(pid).or_insert_with(|| owner_state(cache, pid)),
        };
        out.entries.push(Entry { name, state });
    }
    out.entries.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// The entries that have no mark, with the pid their name ends with: the
/// ones an owner file speaks for.
fn unmarked(cache: &Path) -> Vec<(OsString, u32)> {
    if !crate::git::is_real_dir(&cache.join("wt")) {
        return Vec::new();
    }
    std::fs::read_dir(cache.join("wt"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name())
        .filter_map(|n| Some((entry_pid(&n.to_string_lossy())?, n)))
        .filter(|(_, n)| !is_marked(cache, n))
        .map(|(pid, n)| (n, pid))
        .collect()
}

/// The entries without a mark whose name ends with `pid`.
fn names_of(cache: &Path, pid: u32) -> Vec<OsString> {
    let mut names: Vec<OsString> = unmarked(cache)
        .into_iter()
        .filter(|(_, p)| *p == pid)
        .map(|(n, _)| n)
        .collect();
    names.sort();
    names
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

/// What is left to do once the cache lock is released: neither waits for
/// another run (git's lock of a repository is held for a whole
/// `git worktree add`) nor takes long under the lock that every run needs.
#[derive(Default)]
#[must_use]
struct After {
    /// The repositories that had a worktree among the entries taken out.
    repos: BTreeSet<PathBuf>,
    /// Entries that could not be moved to the trash, under a hidden name.
    gone: Vec<PathBuf>,
}

impl After {
    /// Call without the cache lock.
    fn finish(self, ctx: &ExecContext) {
        for path in self.gone {
            crate::git::remove_now(&path);
        }
        for repo in self.repos {
            if repo.is_dir() {
                let _ = crate::git::prune_worktrees(ctx, &repo);
            }
        }
    }
}

/// Take the entries out of `wt`, by a rename each: into the trash, which the
/// caller empties, or, when that cannot be, to a hidden name in `wt` that
/// [`After::finish`] deletes. A link is unlinked, never followed. Call with
/// the cache lock held.
fn discard(cache: &Path, names: &[OsString]) -> After {
    let mut after = After::default();
    for name in names {
        let path = cache.join("wt").join(name);
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_dir() {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        after.repos.extend(registered_in(&path));
        if crate::git::move_to_trash(&path, &cache.join("trash")).is_none() {
            let mut hidden = OsString::from(GONE);
            hidden.push(crate::git::moved_name(name));
            let hidden = cache.join("wt").join(hidden);
            if std::fs::rename(&path, &hidden).is_ok() {
                after.gone.push(hidden);
            }
        }
    }
    after
}

/// What a sweep did and what it left.
#[derive(Clone, Debug, Default)]
pub struct Swept {
    /// The entries taken out of `wt`.
    pub removed: Vec<OsString>,
    pub in_use: usize,
    pub kept: usize,
    /// Left by a version that recorded no owner, by a process that is gone.
    pub legacy: usize,
    pub unknown: usize,
}

fn sweep_locked(cache: &Path, kept_too: bool, legacy_too: bool) -> (Swept, After) {
    let scan = scan(cache);
    let mut out = Swept::default();
    for e in scan.entries {
        let gone = match e.state {
            State::Stale => true,
            State::Kept => kept_too,
            State::Legacy => legacy_too,
            State::InUse | State::Unknown => false,
        };
        if gone {
            out.removed.push(e.name);
            continue;
        }
        match e.state {
            State::Kept => out.kept += 1,
            State::Legacy => out.legacy += 1,
            State::Unknown => out.unknown += 1,
            _ => out.in_use += 1,
        }
    }
    let mut after = discard(cache, &out.removed);
    after.gone.extend(scan.gone);
    // a mark whose entry is gone
    for (mark, entry) in scan.marks {
        if std::fs::symlink_metadata(cache.join("wt").join(entry)).is_err() {
            let _ = std::fs::remove_file(mark);
        }
    }
    // An owner file that speaks for no entry and has no run behind it. One
    // whose entries could not be taken out stays: without it they would
    // pass for entries that never had an owner.
    let spoken_for: BTreeSet<u32> = unmarked(cache).into_iter().map(|(_, pid)| pid).collect();
    for pid in scan.owners {
        if !spoken_for.contains(&pid) && owner_state(cache, pid) == State::Stale {
            let _ = std::fs::remove_file(owner_file(cache, pid));
        }
    }
    (out, after)
}

/// A run's hold on the entries it makes under `<cache>/wt`. Dropping it
/// removes what the run left there, on every way out of the run.
pub struct Owner<'a> {
    ctx: &'a ExecContext,
    cache: PathBuf,
    path: PathBuf,
    legacy: usize,
}

/// Remove the entries of runs that are gone, then register this process as
/// the owner of the entries it is about to make. Call before making any.
///
/// Fails when another live process holds the owner file of this pid: the
/// cache is then shared between pid namespaces, where a pid does not name
/// one process, and nothing in it can be told apart.
pub fn enter<'a>(ctx: &'a ExecContext, cache: &Path) -> Result<Owner<'a>> {
    let lock = cache_lock(cache)?;
    let (swept, after) = sweep_locked(cache, false, false);
    let pid = std::process::id();
    let path = owner_file(cache, pid);
    let registered = held(|h| -> Result<usize> {
        if let Some(e) = h.get_mut(&path) {
            e.runs += 1;
            return Ok(0);
        }
        let first = std::fs::symlink_metadata(&path).is_err();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .context("cannot record the owner of the worktrees")?;
        // Never waited for: the runs of this process share one handle, and
        // the sweep above is done, so only another process holds this lock.
        if let Err(e) = file.try_lock_exclusive() {
            if e.kind() == fs2::lock_contended_error().kind() {
                bail!(
                    "another process with the pid {pid} holds {}: the cache directory {} is \
                     shared with another pid namespace (a container that mounts it, a network \
                     file system). Give each its own cache directory with LOCAL_CI_HOME",
                    path.display(),
                    cache.display()
                );
            }
            return Err(e).context("cannot lock the owner file of the worktrees");
        }
        // Entries with this pid and no owner file until now were made by a
        // version that recorded none. Marked, they are not taken for entries
        // of this run, whatever becomes of it.
        let mut legacy = 0;
        if first {
            for name in names_of(cache, pid) {
                legacy += usize::from(set_mark(cache, LEGACY, &name));
            }
        }
        h.insert(
            path.clone(),
            Held {
                file,
                runs: 1,
                keep: false,
            },
        );
        Ok(legacy)
    });
    drop(lock);
    after.finish(ctx);
    let legacy = swept.legacy + registered?;
    crate::git::empty_trash(ctx, &cache.join("trash"));
    Ok(Owner {
        ctx,
        cache: cache.to_path_buf(),
        path,
        legacy,
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

    /// How many entries a version that recorded no owner left in the cache,
    /// by processes that are gone. No run removes them.
    pub fn legacy(&self) -> usize {
        self.legacy
    }
}

impl Drop for Owner<'_> {
    fn drop(&mut self) {
        let lock = cache_lock(&self.cache).ok();
        let last = held(|h| {
            let e = h.get_mut(&self.path)?;
            e.runs -= 1;
            if e.runs > 0 {
                return None;
            }
            let last = h.remove(&self.path)?;
            if lock.is_none() {
                // Nothing is decided without the lock of the cache. The owner
                // file is let go here, before another run of this process can
                // ask for it, and the next run reclaims what is left.
                let _ = FileExt::unlock(&last.file);
                return None;
            }
            Some(last)
        });
        let Some(last) = last else {
            return;
        };
        let pid = std::process::id();
        let mine = names_of(&self.cache, pid);
        // Nothing is left when the run removed its worktrees itself; what is
        // (a copy of a thread that panicked, a removal that failed) goes now.
        let after = if last.keep {
            for name in &mine {
                set_mark(&self.cache, KEPT, name);
            }
            After::default()
        } else {
            discard(&self.cache, &mine)
        };
        // the owner file stays for as long as it speaks for an entry
        if names_of(&self.cache, pid).is_empty() {
            let _ = std::fs::remove_file(&self.path);
        }
        // unlocked, not only closed (see `owner_state`)
        let _ = FileExt::unlock(&last.file);
        drop(lock);
        after.finish(self.ctx);
        if !last.keep && !mine.is_empty() {
            crate::git::empty_trash(self.ctx, &self.cache.join("trash"));
        }
    }
}

/// Remove the entries of runs that are gone (and the kept ones with
/// `kept_too`) and delete everything local-ci left in the trash before
/// returning.
pub fn prune(ctx: &ExecContext, cache: &Path, kept_too: bool) -> Result<Swept> {
    prune_all(ctx, cache, kept_too, false)
}

/// [`prune`], and with `legacy_too` also the entries that have no owner
/// record and whose pid no process has.
pub fn prune_all(
    ctx: &ExecContext,
    cache: &Path,
    kept_too: bool,
    legacy_too: bool,
) -> Result<Swept> {
    let lock = cache_lock(cache)?;
    let (swept, after) = sweep_locked(cache, kept_too, legacy_too);
    // the deletion can take long, and runs must not wait for it
    drop(lock);
    after.finish(ctx);
    crate::git::empty_trash_now(&cache.join("trash"));
    Ok(swept)
}

/// The entries of `<cache>/wt` and what is known of their owners.
pub fn entries(cache: &Path) -> Result<Vec<Entry>> {
    let _lock = cache_lock(cache)?;
    Ok(scan(cache).entries)
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
        // each had a run that recorded itself; the live one still holds its file
        let _held = {
            let owner = owner_file(&c.dir(), alive);
            std::fs::write(&owner, "").unwrap();
            let f = File::open(&owner).unwrap();
            f.lock_exclusive().unwrap();
            f
        };
        std::fs::write(owner_file(&c.dir(), dead), "").unwrap();
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
        FileExt::unlock(&_held).unwrap();
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
        std::fs::write(owner_file(&c.dir(), pid), "").unwrap();
        assert!(set_mark(&c.dir(), KEPT, OsStr::new(&name)));
        assert_eq!(c.state(&name), State::Kept);
        let swept = prune(&ctx(), &c.dir(), false).unwrap();
        assert_eq!((swept.removed.len(), swept.kept), (0, 1));
        assert_eq!(c.names(), std::slice::from_ref(&name));
        assert_eq!(prune(&ctx(), &c.dir(), true).unwrap().removed.len(), 1);
        assert!(c.names().is_empty());
        assert!(!owner_file(&c.dir(), pid).exists());
        assert!(!mark_path(&c.dir(), KEPT, OsStr::new(&name)).exists());
    }

    #[test]
    fn links_are_removed_and_never_followed() {
        let c = Cache::new();
        let outside = c.tmp.path().join("outside");
        std::fs::create_dir_all(outside.join("sub")).unwrap();
        std::fs::write(outside.join("sub/precious"), "data").unwrap();
        let pid = dead_pid();
        std::fs::write(owner_file(&c.dir(), pid), "").unwrap();
        // an entry that is itself a link out of the cache
        std::os::unix::fs::symlink(&outside, c.wt(&format!("abc1234-{pid}"))).unwrap();
        // links inside an entry, to a directory and to a file
        let e = c.entry(&format!("abc1234-u1-{pid}"));
        std::os::unix::fs::symlink(&outside, e.join("out-dir")).unwrap();
        std::os::unix::fs::symlink(outside.join("sub/precious"), e.join("target/out-file"))
            .unwrap();
        // and in what an earlier deletion left in the trash
        let old = c.dir().join("trash/abc1234-4242.77.0");
        std::fs::create_dir_all(&old).unwrap();
        std::os::unix::fs::symlink(&outside, old.join("out-dir")).unwrap();
        std::os::unix::fs::symlink(&outside, c.dir().join("trash/abc1234-4242.77.1")).unwrap();

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
    fn a_trash_or_a_wt_that_is_a_link_out_of_the_cache_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = tmp.path().join("cache");
        let outside = tmp.path().join("outside");
        let pid = dead_pid();
        for d in ["trash", "wt"] {
            let target = outside.join(d);
            std::fs::create_dir_all(target.join(format!("abc1234-{pid}"))).unwrap();
            std::fs::write(target.join("precious"), "data").unwrap();
            std::fs::create_dir_all(&cache).unwrap();
            std::os::unix::fs::symlink(&target, cache.join(d)).unwrap();
        }
        assert!(prune(&ctx(), &cache, true).unwrap().removed.is_empty());
        crate::git::empty_trash(&ctx(), &cache.join("trash"));
        for d in ["trash", "wt"] {
            assert!(outside.join(d).join(format!("abc1234-{pid}")).is_dir());
            assert!(outside.join(d).join("precious").is_file());
        }
    }

    #[test]
    fn the_trash_is_emptied_even_when_a_directory_cannot_be_written() {
        use std::os::unix::fs::PermissionsExt;
        let c = Cache::new();
        let sealed = c.dir().join("trash/abc1234-4242.77.0/mod-cache");
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
        std::fs::write(owner_file(&c.dir(), pid), "").unwrap();
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
        let dead = dead_pid();
        std::fs::write(owner_file(&c.dir(), dead), "").unwrap();
        c.entry(&format!("abc1234-{dead}"));
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
        assert_eq!(c.state(&kept), State::Kept);
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
        let dead = dead_pid();
        std::fs::write(owner_file(&c.dir(), dead), "").unwrap();
        let stale = c.entry(&format!("abc1234-{dead}"));
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

    /// What a `SIGKILL` leaves of a run: its lock is gone with the process,
    /// and nothing it would have done on the way out was done.
    fn killed(owner: Owner<'_>) {
        let h = held(|h| h.remove(&owner.path)).unwrap();
        FileExt::unlock(&h.file).unwrap();
        std::mem::forget(owner);
    }

    #[test]
    fn a_kept_entry_outlives_a_killed_run_that_had_the_same_pid() {
        let c = Cache::new();
        let (ctx, me) = (ctx(), std::process::id());
        let kept = format!("abc1234-{me}");
        let owner = enter(&ctx, &c.dir()).unwrap();
        c.entry(&kept);
        owner.keep();
        drop(owner);

        // a later run with the same pid, killed while it works
        let owner = enter(&ctx, &c.dir()).unwrap();
        let left = format!("def5678-{me}");
        c.entry(&left);
        killed(owner);
        assert_eq!(c.state(&kept), State::Kept);
        assert_eq!(c.state(&left), State::Stale);
        let swept = prune(&ctx, &c.dir(), false).unwrap();
        assert_eq!(swept.removed, [OsString::from(&left)]);
        assert_eq!(c.names(), std::slice::from_ref(&kept));
        assert_eq!(c.state(&kept), State::Kept);

        // asked for, the entry goes, and its mark with it
        prune(&ctx, &c.dir(), true).unwrap();
        assert!(c.names().is_empty());
        assert_eq!(std::fs::read_dir(c.dir().join("wt")).unwrap().count(), 1);
    }

    #[test]
    fn an_entry_with_no_owner_record_is_never_removed_by_a_run() {
        let c = Cache::new();
        let (ctx, me) = (ctx(), std::process::id());
        // made by a version that recorded no owner, perhaps kept on purpose
        let dead = format!("abc1234-{}", dead_pid());
        let same_pid = format!("abc1234-{me}");
        c.entry(&dead);
        c.entry(&same_pid);
        let mut both = [dead.clone(), same_pid.clone()];
        both.sort();
        assert!(prune(&ctx, &c.dir(), true).unwrap().removed.is_empty());

        let owner = enter(&ctx, &c.dir()).unwrap();
        assert_eq!(c.names(), both);
        // the run is killed: its owner file is left, and is not theirs
        c.entry(&format!("def5678-{me}"));
        killed(owner);
        assert_eq!(prune(&ctx, &c.dir(), true).unwrap().removed.len(), 1);
        assert_eq!(c.names(), both);
        // a run that ends in order leaves them too
        let owner = enter(&ctx, &c.dir()).unwrap();
        assert_eq!(owner.legacy(), 1);
        drop(owner);
        assert_eq!(c.names().len(), 2);
        assert_eq!(c.state(&dead), State::Legacy);

        // asked for, the one whose pid no process has goes; this process
        // has the pid of the other
        let swept = prune_all(&ctx, &c.dir(), false, true).unwrap();
        assert_eq!(swept.removed, [OsString::from(&dead)]);
        assert_eq!(c.names(), [same_pid]);
    }

    #[test]
    fn only_what_local_ci_moved_to_the_trash_is_deleted_from_it() {
        let c = Cache::new();
        let trash = c.dir().join("trash");
        let theirs = [
            "Documents",
            "backup.2024.11",
            "abc1234-4242.other.1",
            "x.1.2",
        ];
        for d in theirs {
            std::fs::create_dir_all(trash.join(d).join("sub")).unwrap();
        }
        std::fs::write(trash.join("notes.txt"), "data").unwrap();
        for d in [
            "abc1234-4242.77.0",
            "abc1234-u1-4242.tools.77.12",
            "abc1234-4242",
        ] {
            std::fs::create_dir_all(trash.join(d).join("sub")).unwrap();
        }
        prune(&ctx(), &c.dir(), true).unwrap();
        let mut left = c.trash();
        left.sort();
        let mut want: Vec<String> = theirs.iter().map(|s| s.to_string()).collect();
        want.push("notes.txt".into());
        want.sort();
        assert_eq!(left, want);
        crate::git::empty_trash(&ctx(), &trash);
        std::thread::sleep(std::time::Duration::from_millis(300));
        assert_eq!(c.trash().len(), want.len());
    }

    #[test]
    fn an_owner_file_held_by_another_process_with_this_pid_fails_the_run() {
        let c = Cache::new();
        let me = std::process::id();
        // a process in another pid namespace that shares the cache
        let path = owner_file(&c.dir(), me);
        std::fs::write(&path, "").unwrap();
        let holder = File::open(&path).unwrap();
        holder.lock_exclusive().unwrap();
        let theirs = format!("abc1234-{me}");
        c.entry(&theirs);

        let (tx, rx) = std::sync::mpsc::channel();
        let dir = c.dir();
        let t = std::thread::spawn(move || {
            let ctx = ctx();
            let r = enter(&ctx, &dir)
                .map(std::mem::forget)
                .map_err(|e| format!("{e:#}"));
            let _ = tx.send(r);
        });
        let got = rx.recv_timeout(std::time::Duration::from_secs(10));
        // let a run that waits go on, so that the test ends
        FileExt::unlock(&holder).unwrap();
        t.join().unwrap();
        let err = got.expect("the run waits for the lock").unwrap_err();
        assert!(err.contains(&c.dir().display().to_string()), "{err}");
        assert!(err.contains("pid namespace"), "{err}");
        assert_eq!(c.names(), [theirs]);
        assert!(path.exists());
    }
}
