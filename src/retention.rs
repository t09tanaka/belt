//! What is deleted without anyone asking for it.
//!
//! Everything fast-worktree keeps after a command has ended (cache entries,
//! the pool, the trash, the refs of `hook remove`) is deleted by rules that
//! run from the commands that write to the same places, not from a daemon
//! or a timer: [`run`] is called at the start of every `add`, `remove`,
//! `hook remove`, `install` and `pool fill`, and by the library calls
//! that do the same, so that a program that embeds the crate gets them
//! without calling anything. The manual commands (`cache prune`, `kept
//! prune`, `pool clear`) apply the same rules at once.
//!
//! A sweep that walks everything is done at most once an hour (a stamp
//! file's modification time says when it last started). A sweep never fails
//! the command that made it: what it cannot delete stays for the next one.

use crate::cmd::kept;
use crate::util::errln;
use crate::{cache, paths, pool, trash};
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

/// The most often that the walks over the cache and over a repository's
/// kept refs and pool are made.
pub const INTERVAL: Duration = Duration::from_secs(3_600);

/// The stamp file, in the cache root (for the cache) and in a repository's
/// directory of it (for its kept refs and pool): its modification time is
/// when the last walk there started.
const STAMP: &str = ".retention-stamp";

/// Whether a walk is due: the one before it (`stamp`, when there is a
/// record of one) started at least `interval` ago. A stamp from the future
/// (a clock that was set back) is due too, so that it cannot hold a walk
/// off for ever.
fn due(stamp: Option<SystemTime>, now: SystemTime, interval: Duration) -> bool {
    match stamp {
        None => true,
        Some(t) => now.duration_since(t).map_or(true, |age| age >= interval),
    }
}

/// Whether the walk the stamp `path` belongs to is due; when it is, the
/// stamp is renewed first, so that runs at the same time do not all walk.
fn start_walk(path: &Path) -> bool {
    let last = fs::metadata(path).and_then(|m| m.modified()).ok();
    if !due(last, SystemTime::now(), INTERVAL) {
        return false;
    }
    fs::write(path, b"").is_ok()
}

/// The sweep: finish deleting the trash (always: it is cheap), then, when
/// due, remove from the cache what is unused, half made or empty, in every
/// project of every repository; and, with the repository's `main`
/// checkout, delete its kept refs that are old and sweep its pool. See the
/// README for the table of what is kept and for how long.
pub fn run(main: Option<&Path>) {
    trash::sweep();
    let root = paths::cache_root();
    if root.is_dir() && start_walk(&root.join(STAMP)) {
        walk(&root);
    }
    let Some(main) = main else { return };
    let repo = root.join(paths::repo_key(main));
    if fs::create_dir_all(&repo).is_err() || !start_walk(&repo.join(STAMP)) {
        return;
    }
    match kept::prune_quiet(main, kept::PRUNE_DAYS) {
        Ok(0) | Err(_) => {}
        Ok(n) => errln!("fast-worktree: {n} old kept ref(s) could not be deleted"),
    }
}

/// The walk over the whole cache: every project of every repository (what
/// is unused, half made or empty, [`cache::prune_all`]), every pool
/// (leftovers of killed runs, [`pool::tidy`]), then the repository
/// directories, and the trash directory, that nothing is left in.
fn walk(root: &Path) {
    cache::prune_all(root, Duration::from_secs(cache::TTL_DAYS * 86_400));
    for repo in cache::repo_dirs(root) {
        let pool = repo.join("pool");
        if pool.is_dir() {
            pool::tidy(&pool);
        }
        remove_repo_if_empty(&repo);
    }
    trash::remove_if_empty();
}

/// Remove a repository's directory of the cache when nothing is in it but
/// its lifecycle lock and a stamp that is due anyway. The lock is taken
/// first (not waited for), and goes with the directory.
fn remove_repo_if_empty(repo: &Path) {
    let stamp = repo.join(STAMP);
    let stamp_due = due(
        fs::metadata(&stamp).and_then(|m| m.modified()).ok(),
        SystemTime::now(),
        INTERVAL,
    );
    let lock_name = crate::ecosystem::LIFECYCLE_LOCK;
    let only_ours = fs::read_dir(repo).is_ok_and(|d| {
        d.flatten().all(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            n == lock_name || (n == STAMP && stamp_due)
        })
    });
    if !only_ours {
        return;
    }
    let lock = repo.join(lock_name);
    let Ok(Some(_held)) = crate::util::lock_path(&lock, false, false) else {
        return;
    };
    let _ = fs::remove_file(&lock);
    let _ = fs::remove_file(&stamp);
    let _ = fs::remove_dir(repo);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_walk_is_due_once_the_interval_has_passed() {
        let now = SystemTime::now();
        let ago = |s: u64| Some(now - Duration::from_secs(s));
        let hour = INTERVAL.as_secs();
        assert!(due(None, now, INTERVAL));
        assert!(!due(ago(hour - 1), now, INTERVAL));
        assert!(due(ago(hour), now, INTERVAL));
        assert!(due(ago(hour + 1), now, INTERVAL));
        // A stamp from the future does not hold it off.
        assert!(due(Some(now + Duration::from_secs(60)), now, INTERVAL));
    }
}
