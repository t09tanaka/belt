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
//! the command that made it, and never makes it wait for another
//! repository: what it cannot delete now stays for the next one.
//!
//! Lock files are not deleted while their repository is there. The
//! versions from 0.4.0 on take each lock on the file they opened and do
//! not look at the path again, and several versions run side by side on
//! one machine: unlinking a lock file that one of them waits on would let
//! two processes hold "the lock". So a directory that holds a lock file
//! stays too. They go together, when the repository's main checkout is
//! gone ([`remove_gone`]).

use crate::cmd::kept;
use crate::util::errln;
use crate::{cache, paths, pool, trash};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

/// The most often that the walks over the cache and over a repository's
/// kept refs and pool are made.
pub const INTERVAL: Duration = Duration::from_secs(3_600);

/// The stamp file, in the cache root (for the cache) and in a repository's
/// directory of it (for its kept refs and pool): its modification time is
/// when the last walk there started.
const STAMP: &str = ".retention-stamp";

/// In a repository's directory of the cache: the path of its main checkout
/// (the directory's name only has a hash of it). It is how a walk made
/// from another repository can tell that this one is gone.
const MAIN_RECORD: &str = ".main";

/// The most that one walk over the cache moves into the trash. The walk is
/// made in the foreground of whatever command found it due, which must not
/// be held up by the first walk after an upgrade (hundreds of expired
/// entries); what is left is taken by the next command's walk.
const WALK_LIMIT: usize = 200;

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
/// A stamp that cannot be read is due, and so is one that cannot be
/// written (the walk is then made by every command, rather than never),
/// which is reported once.
fn start_walk(path: &Path) -> bool {
    static REPORTED: AtomicBool = AtomicBool::new(false);
    let last = fs::metadata(path).and_then(|m| m.modified()).ok();
    if !due(last, SystemTime::now(), INTERVAL) {
        return false;
    }
    if let Err(e) = fs::write(path, b"") {
        if !REPORTED.swap(true, Ordering::Relaxed) {
            errln!(
                "fast-worktree: cannot write {} ({e}): the cleanup it spaces out runs with every command",
                path.display()
            );
        }
    }
    true
}

/// The sweep. Always (it is cheap): start one deleter for what an
/// interrupted one left in the trash. When due: the walk over the whole
/// cache ([`walk`]). With the main checkout `main` of the repository the
/// command is for, also that repository's own things ([`own`]). See the
/// README for the table of what is kept and for how long.
pub fn run(main: Option<&Path>) {
    trash::sweep();
    let root = paths::cache_root();
    let stamp = root.join(STAMP);
    if root.is_dir() && start_walk(&stamp) && !walk(&root) {
        // Work is left: the next command goes on, not the next hour's.
        let _ = fs::remove_file(&stamp);
    }
    if let Some(main) = main {
        own(&root, main);
    }
}

/// What a command in a repository does for that repository. With every
/// command: note where its main checkout is, and discard the spares beyond
/// the pool size ([`pool::trim`]). At most once an hour: delete the kept
/// refs that are no longer needed ([`kept::prune_auto`]), the pool's
/// leftovers ([`pool::sweep_in`]), and the hidden siblings that removals on
/// another volume left next to its worktrees.
///
/// These run git and may wait for the repository's own locks, which is why
/// they are done here and never for another repository.
fn own(root: &Path, main: &Path) {
    let repo = root.join(paths::repo_key(main));
    if fs::create_dir_all(&repo).is_err() {
        return;
    }
    record_main(&repo, main);
    pool::trim(main);
    if !start_walk(&repo.join(STAMP)) {
        return;
    }
    kept::prune_auto(main);
    pool::sweep_in(main, &repo.join("pool"));
    trash::sweep_siblings(&worktree_parents(main));
}

/// Write the real path of `main` into the [`MAIN_RECORD`] of its directory
/// `repo` of the cache, unless it is there already.
fn record_main(repo: &Path, main: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let Ok(real) = main.canonicalize() else {
        return;
    };
    let record = repo.join(MAIN_RECORD);
    if fs::read(&record).is_ok_and(|b| b == real.as_os_str().as_bytes()) {
        return;
    }
    let _ = fs::write(&record, real.as_os_str().as_bytes());
}

/// The directories that hold the worktrees of the repository at `main`
/// (and `main` itself), read from the files git keeps, without running it.
fn worktree_parents(main: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let mut add = |p: Option<&Path>| {
        if let Some(p) = p.filter(|p| !p.as_os_str().is_empty()) {
            if !out.iter().any(|o| o == p) {
                out.push(p.to_path_buf());
            }
        }
    };
    add(main.parent());
    let entries = fs::read_dir(main.join(".git/worktrees"));
    for e in entries.into_iter().flatten().flatten() {
        let Ok(text) = fs::read_to_string(e.path().join("gitdir")) else {
            continue;
        };
        // `<worktree>/.git`, absolute or relative to the entry.
        let dot_git = e.path().join(text.trim_end_matches('\n'));
        add(dot_git.parent().and_then(Path::parent));
    }
    out
}

/// The main checkout that the repository directory `repo` of the cache
/// records, when it is gone: the record is this directory's (its hash is
/// the directory's name), the directory the checkout was in is still
/// there, and the checkout is not. A checkout on a volume that is not
/// mounted is not "gone": then the directory it was in is missing too.
/// `None` also for a directory with no record (one that only older
/// versions wrote to): it stays.
fn gone_main(repo: &Path) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let main = PathBuf::from(std::ffi::OsString::from_vec(
        fs::read(repo.join(MAIN_RECORD)).ok()?,
    ));
    let named = repo.file_name()?.to_str()?;
    (main.is_absolute() && paths::repo_key(&main) == named && is_gone(&main)).then_some(main)
}

fn is_gone(main: &Path) -> bool {
    main.parent().is_some_and(Path::is_dir)
        && fs::symlink_metadata(main).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
}

/// Move the whole directory `repo` of a repository whose main checkout
/// `main` is gone into the trash: its entries, its spares (their
/// registrations went with the repository) and its lock files. Returns
/// where it went; `None` when it stays.
///
/// This is the one place where lock files go. Nothing can take them
/// meaningfully any more: every command that locks one starts from a
/// worktree of the repository, and `cache prune` of an older version,
/// which walks all repositories, takes a `.store.lock` only to remove
/// records, which a rename of the directory does not disturb. They are
/// still taken first, without waiting (held: the directory stays this
/// time), and the checkout is looked for again once they are held.
fn remove_gone(repo: &Path, main: &Path) -> Option<PathBuf> {
    let mut locks = vec![repo.join(crate::ecosystem::LIFECYCLE_LOCK)];
    for (_, dir) in cache::dirs(repo) {
        locks.push(dir.join(pool::FILL_LOCK));
        locks.push(dir.join(cache::STORE_LOCK));
    }
    let mut held = Vec::new();
    for lock in locks.iter().filter(|l| l.is_file()) {
        match crate::util::lock_path(lock, false, false) {
            Ok(Some(f)) => held.push(f),
            Ok(None) => return None,
            // A file system without locks: nothing else holds it either.
            Err(_) => {}
        }
    }
    if !is_gone(main) {
        return None;
    }
    trash::move_aside(repo).ok()
}

/// The walk over the whole cache, made from a command in any repository:
/// the directories of repositories that are gone ([`remove_gone`]), and in
/// the others what is unused, half made or left without its entry
/// ([`cache::prune_repos`]). Returns whether nothing was left for the next
/// walk ([`WALK_LIMIT`]).
///
/// It must not hold up the command that makes it, nor be held up by
/// another repository: it lists directories and renames into the trash,
/// takes locks only without waiting, runs no git, and starts one deleter
/// for everything it moved. What needs more than that (a repository's kept
/// refs, its pool) is done by that repository's own commands ([`own`]).
fn walk(root: &Path) -> bool {
    let mut gone = Vec::new();
    let mut live = Vec::new();
    let mut complete = true;
    for repo in cache::repo_dirs(root) {
        match gone_main(&repo) {
            Some(_) if gone.len() >= WALK_LIMIT => complete = false,
            Some(main) => gone.extend(remove_gone(&repo, &main)),
            None => live.push(repo),
        }
    }
    let ttl = Duration::from_secs(cache::TTL_DAYS * 86_400);
    let (_, done) = cache::prune_repos(&live, ttl, WALK_LIMIT - gone.len());
    trash::spawn_rm_many(&gone);
    complete && done
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

    #[test]
    fn a_stamp_that_cannot_be_written_does_not_stop_the_walk() {
        let t = tempfile::tempdir().unwrap();
        // No such directory: the stamp can be neither read nor written.
        let stamp = t.path().join("missing/.retention-stamp");
        assert!(start_walk(&stamp) && start_walk(&stamp));
        // One that can be written spaces the walks out.
        let stamp = t.path().join(".retention-stamp");
        assert!(start_walk(&stamp));
        assert!(!start_walk(&stamp));
    }

    #[test]
    fn a_repository_is_gone_only_when_its_record_says_so() {
        use std::os::unix::ffi::OsStrExt;
        let t = tempfile::tempdir().unwrap();
        let base = t.path().canonicalize().unwrap();
        let main = base.join("work/app");
        fs::create_dir_all(&main).unwrap();
        let repo = base.join("cache").join(paths::repo_key(&main));
        fs::create_dir_all(&repo).unwrap();
        // No record (an older version made the directory): never gone.
        fs::remove_dir(&main).unwrap();
        assert_eq!(gone_main(&repo), None);
        fs::create_dir_all(&main).unwrap();
        record_main(&repo, &main);
        assert_eq!(
            fs::read(repo.join(MAIN_RECORD)).unwrap(),
            main.as_os_str().as_bytes()
        );
        assert_eq!(gone_main(&repo), None, "it is there");
        fs::remove_dir(&main).unwrap();
        assert_eq!(gone_main(&repo), Some(main.clone()));
        // The directory it was in is missing too (a volume that is not
        // mounted looks like this): not gone.
        fs::remove_dir(base.join("work")).unwrap();
        assert_eq!(gone_main(&repo), None);
        // A record that is another directory's proves nothing here.
        let other = base.join("cache/other-0123abcd");
        fs::create_dir_all(&other).unwrap();
        fs::create_dir_all(base.join("work")).unwrap();
        fs::copy(repo.join(MAIN_RECORD), other.join(MAIN_RECORD)).unwrap();
        assert_eq!(gone_main(&other), None);
        assert_eq!(gone_main(&repo), Some(main));
    }

    #[test]
    fn the_directories_of_worktrees_are_read_from_git_s_files() {
        let t = tempfile::tempdir().unwrap();
        let main = t.path().join("work/app");
        for (name, gitdir) in [("a", "/else/where/a/.git"), ("b", "/else/where/b/.git")] {
            let admin = main.join(".git/worktrees").join(name);
            fs::create_dir_all(&admin).unwrap();
            fs::write(admin.join("gitdir"), format!("{gitdir}\n")).unwrap();
        }
        assert_eq!(
            worktree_parents(&main),
            vec![t.path().join("work"), PathBuf::from("/else/where")]
        );
    }
}
