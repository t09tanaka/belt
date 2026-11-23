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
//! repository: in the command itself it only touches paths inside the
//! cache. What needs a look at where another repository is (is it gone?)
//! is handed to a detached process ([`retire`]), which may wait on a dead
//! network mount for as long as it takes without anyone waiting for it.
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
use crate::{cache, paths, pool, trash, util};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

/// The most often that the walks over the cache and over a repository's
/// kept refs and pool are made.
pub const INTERVAL: Duration = Duration::from_secs(3_600);

/// A repository that ran no command for this long is not in use: its
/// spares are discarded, and only then is it asked whether it is gone. The
/// same 7 days after which a cache entry that nothing cloned from goes:
/// past it, what an `add` there would find is no faster than a new
/// install.
pub const IDLE: Duration = Duration::from_secs(cache::TTL_DAYS * 86_400);

/// How long after the detached process looked at a repository that is not
/// in use, and left its directory in place, it is looked at again.
const RECHECK: Duration = Duration::from_secs(86_400);

/// The stamp file, in the cache root (for the cache) and in a repository's
/// directory of it (for its kept refs and pool): its modification time is
/// when the last walk there started. In the directory of a repository that
/// is not in use it is when the detached process last looked at it.
const STAMP: &str = ".retention-stamp";

/// In a repository's directory of the cache: the path of its main checkout
/// (the directory's name only has a hash of it) and the devices it was on
/// ([`Record`]). Its modification time is when a command last ran in that
/// repository. It is how a walk made from another repository can tell that
/// this one is not in use, and the detached process that it is gone.
const MAIN_RECORD: &str = ".main";

/// The most that one walk over the cache moves into the trash, and the
/// most repositories it hands to the detached process. The walk is made in
/// the foreground of whatever command found it due, which must not be held
/// up by the first walk after an upgrade (hundreds of expired entries);
/// what is left is taken by the next command's walk.
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
    sweep(main, true);
}

/// [`run`] for `pool fill`, which mostly runs in the background with its
/// output in a log file: everything but the hourly part of [`own`], which
/// deletes kept refs and has to say so where the user reads it. That part
/// is left, with its stamp, to the next command in the repository.
pub fn run_in_fill(main: &Path) {
    sweep(Some(main), false);
}

fn sweep(main: Option<&Path>, hourly: bool) {
    trash::sweep();
    let root = paths::cache_root();
    let stamp = root.join(STAMP);
    if root.is_dir() && start_walk(&stamp) && !walk(&root) {
        // Work is left: the next command goes on, not the next hour's.
        let _ = fs::remove_file(&stamp);
    }
    if let Some(main) = main {
        own(&root, main, hourly);
    }
}

/// What a command in a repository does for that repository. With every
/// command: note where its main checkout is and that it is in use
/// ([`record_main`]), and discard the spares beyond the pool size
/// ([`pool::trim`]). With `hourly`, at most once an hour: delete the kept
/// refs that are no longer needed ([`kept::prune_auto`]), the pool's
/// leftovers ([`pool::sweep_in`]), the registrations of spares that are no
/// longer there ([`pool::drop_dangling`]), and the hidden siblings that
/// removals on another volume left next to its worktrees.
///
/// These run git and may wait for the repository's own locks, which is why
/// they are done here and never for another repository.
fn own(root: &Path, main: &Path, hourly: bool) {
    let repo = root.join(paths::repo_key(main));
    if fs::create_dir_all(&repo).is_err() {
        return;
    }
    record_main(&repo, main);
    pool::trim(main);
    if !hourly || !start_walk(&repo.join(STAMP)) {
        return;
    }
    kept::prune_auto(main);
    pool::sweep_in(main, &repo.join("pool"));
    pool::drop_dangling(main);
    trash::sweep_siblings(&worktree_parents(main));
}

/// [`record_main`] for a repository that only ever installs (no `add`, no
/// removal): once its directory `repo` of the cache is there.
pub fn note_main(repo: &Path, main: &Path) {
    if repo.is_dir() {
        record_main(repo, main);
    }
}

/// The device numbers (`st_dev`) that a [`Record`] holds, as they were
/// when a command last ran in the repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Devices {
    /// Of the directory the main checkout is in.
    parent: u64,
    /// Of the main checkout.
    main: u64,
    /// Of the repository's directory of the cache.
    cache: u64,
}

/// What [`MAIN_RECORD`] holds: the real path of the main checkout, and,
/// after a NUL, three numbers ([`Devices`]). One written before the
/// numbers were added has the path alone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    main: PathBuf,
    devices: Option<Devices>,
}

fn encode(main: &Path, devices: Option<Devices>) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    let mut out = main.as_os_str().as_bytes().to_vec();
    if let Some(d) = devices {
        out.push(0);
        out.extend(format!("{} {} {}", d.parent, d.main, d.cache).bytes());
    }
    out
}

/// `None` for what is not a record (no absolute path). Numbers that cannot
/// be read are no numbers.
fn decode(bytes: &[u8]) -> Option<Record> {
    use std::os::unix::ffi::OsStrExt;
    let (path, rest) = match bytes.iter().position(|b| *b == 0) {
        Some(i) => (&bytes[..i], Some(&bytes[i + 1..])),
        None => (bytes, None),
    };
    let main = PathBuf::from(std::ffi::OsStr::from_bytes(path));
    if !main.is_absolute() {
        return None;
    }
    let devices = rest
        .and_then(|r| std::str::from_utf8(r).ok())
        .and_then(|text| {
            let mut n = text.split(' ').map(str::parse::<u64>);
            let d = Devices {
                parent: n.next()?.ok()?,
                main: n.next()?.ok()?,
                cache: n.next()?.ok()?,
            };
            n.next().is_none().then_some(d)
        });
    Some(Record { main, devices })
}

/// Write the real path of `main` and its devices into the [`MAIN_RECORD`]
/// of its directory `repo` of the cache; when it says so already, only its
/// modification time is renewed. Either way the file now says that a
/// command ran in the repository just now.
fn record_main(repo: &Path, main: &Path) {
    let Ok(real) = main.canonicalize() else {
        return;
    };
    let dev = |p: &Path| fs::metadata(p).ok().map(|m| m.dev());
    let devices = match (real.parent().and_then(dev), dev(&real), dev(repo)) {
        (Some(parent), Some(main), Some(cache)) => Some(Devices {
            parent,
            main,
            cache,
        }),
        _ => None,
    };
    let bytes = encode(&real, devices);
    let record = repo.join(MAIN_RECORD);
    if fs::read(&record).is_ok_and(|b| b == bytes) {
        cache::touch(&record);
    } else {
        let _ = fs::write(&record, bytes);
    }
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

/// What the cache alone says about the repository whose directory of it
/// is `repo`: nothing here touches where the repository is.
struct Local {
    record: Option<Record>,
    /// When a command last ran in it: the newer of the modification times
    /// of its [`MAIN_RECORD`] (every command of this version renews it)
    /// and of its pool directory (which a fill or an `add` of any version
    /// changes when it makes or takes a spare).
    last_used: Option<SystemTime>,
    /// When the detached process last looked at it ([`STAMP`]).
    examined: Option<SystemTime>,
}

fn local(repo: &Path) -> Local {
    let modified = |name: &str| {
        fs::metadata(repo.join(name))
            .and_then(|m| m.modified())
            .ok()
    };
    let last_used = modified(MAIN_RECORD).map(|t| modified("pool").map_or(t, |p| p.max(t)));
    Local {
        record: fs::read(repo.join(MAIN_RECORD))
            .ok()
            .and_then(|b| decode(&b)),
        last_used,
        examined: modified(STAMP),
    }
}

/// Whether a repository last used at `last_used` is not in use at `now`:
/// no command ran in it for `idle` or longer. Not when the time is unknown
/// or in the future (a clock that was set back): then it is in use.
fn unused(last_used: Option<SystemTime>, now: SystemTime, idle: Duration) -> bool {
    last_used.is_some_and(|t| now.duration_since(t).is_ok_and(|age| age >= idle))
}

/// Whether the walk hands the repository directory named `named` to the
/// detached process: it records a main checkout, the record is this
/// directory's (the hash of the recorded path is in the directory's name:
/// computed from the record's bytes, the path itself is not touched), it
/// is not in use ([`unused`]), and the detached process did not look at it
/// in the last [`RECHECK`]. A directory with no record (one that only
/// the versions up to 0.6.0 wrote to) is never handed over: it stays until a
/// command runs in its repository.
fn candidate(named: &str, l: &Local, now: SystemTime) -> bool {
    l.record
        .as_ref()
        .is_some_and(|r| paths::key_of_real(&r.main) == named)
        && unused(l.last_used, now, IDLE)
        && due(l.examined, now, RECHECK)
}

/// What a look at a path outside the cache found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    Found,
    /// The path does not exist (`ENOENT`).
    NotFound,
    /// Anything else: no permission, an I/O error, a mount that is down.
    Unknown,
}

/// What the detached process sees outside the cache now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outside {
    /// The device of the directory the main checkout was in; `None` when
    /// that directory is missing or cannot be read.
    parent: Option<u64>,
    /// The device of the repository's directory of the cache.
    cache: Option<u64>,
    main: Seen,
}

/// Whether a repository that is not in use is gone: its main checkout was
/// deleted. All of this must hold, and anything unknown keeps it:
///
/// - The record has the devices. One written before they were recorded
///   proves nothing, however long ago: the next command in the repository
///   writes them, and a repository that never runs one again may just as
///   well be on a disk that is never plugged in again.
/// - The checkout was not itself the root of a mount (it was on the device
///   of the directory it was in). The mount point of a volume that is not
///   mounted may be missing (`/Volumes/X` on macOS) while the directory it
///   is in is always there.
/// - The directory the checkout was in is there, and on the same device as
///   then. A disk that was mounted at `/mnt/data` leaves the empty
///   directory `/mnt/data` of another device when it is not mounted; the
///   same for a network or FUSE mount. When the checkout was on the
///   cache's device, "the same" is the cache's device now, so that a
///   device number that changed with a restart does not keep everything
///   for ever; otherwise it is the number itself.
/// - The checkout's path does not exist. Seen through a mount, an error
///   other than "no such file" is not that.
///
/// What this cannot tell apart from a deleted checkout: one that was moved
/// (its commands use another directory of the cache from then on, and
/// [`pool::drop_dangling`] drops the registrations of the spares that went
/// with this one), and one that only exists on another machine that shares
/// this cache and whose device numbers happen to be the same.
fn is_gone(devices: Option<Devices>, now: &Outside) -> bool {
    let Some(then) = devices else {
        return false;
    };
    if then.main != then.parent {
        return false;
    }
    let same_device = match now.parent {
        None => false,
        Some(p) if then.parent == then.cache => now.cache == Some(p),
        Some(p) => p == then.parent,
    };
    same_device && now.main == Seen::NotFound
}

fn seen(path: &Path) -> Seen {
    match fs::symlink_metadata(path) {
        Ok(_) => Seen::Found,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Seen::NotFound,
        Err(_) => Seen::Unknown,
    }
}

/// Look where the main checkout `main` was. This may wait for a mount for
/// ever: only the detached process does it.
fn look(main: &Path, repo: &Path) -> Outside {
    let dev = |p: &Path| fs::metadata(p).ok().map(|m| m.dev());
    Outside {
        parent: main.parent().and_then(dev),
        cache: dev(repo),
        main: seen(main),
    }
}

/// Move the whole directory `repo` of a repository whose main checkout
/// `main` is gone into the trash: its entries, its spares and its lock
/// files. Returns where it went; `None` when it stays.
///
/// This is the one place where lock files go. Nothing can take them
/// meaningfully any more: every command that locks one starts from a
/// worktree of the repository, and `cache prune` of an older version,
/// which walks all repositories, takes a `.store.lock` only to remove
/// records, which a rename of the directory does not disturb. They are
/// still taken first, without waiting (held: the directory stays this
/// time), and once they are held it is asked again whether the repository
/// is still not in use and its checkout still missing.
fn remove_gone(repo: &Path, main: &Path) -> Option<PathBuf> {
    let mut locks = vec![repo.join(crate::ecosystem::LIFECYCLE_LOCK)];
    for (_, dir) in cache::dirs(repo) {
        locks.push(dir.join(pool::FILL_LOCK));
        locks.push(dir.join(cache::STORE_LOCK));
    }
    let mut held = Vec::new();
    for lock in locks.iter().filter(|l| l.is_file()) {
        match util::lock_path(lock, false, false) {
            Ok(Some(f)) => held.push(f),
            Ok(None) => return None,
            // A file system without locks: nothing else holds it either.
            Err(_) => {}
        }
    }
    if !unused(local(repo).last_used, SystemTime::now(), IDLE) || seen(main) != Seen::NotFound {
        return None;
    }
    trash::move_aside(repo).ok()
}

/// What the detached process does for one repository directory `repo` of
/// the cache that the walk found not in use. It holds a lock on the
/// [`MAIN_RECORD`] meanwhile, so that a process that hangs on a dead mount
/// keeps only this repository from being looked at again, and the walks do
/// not start another one for it.
///
/// - Gone ([`is_gone`]): the whole directory goes ([`remove_gone`]).
/// - Else its pool is emptied ([`pool::retire`]): through git when the
///   checkout is there, and without it (the registrations stay for
///   [`pool::drop_dangling`]) when it cannot be reached.
///
/// What is moved into the trash is added to `moved`.
fn retire_one(repo: &Path, moved: &mut Vec<PathBuf>) {
    let record_path = repo.join(MAIN_RECORD);
    if !record_path.is_file() {
        return;
    }
    let _examining = match util::lock_path(&record_path, false, false) {
        Ok(Some(f)) => Some(f),
        Ok(None) => return,
        // A file system without locks.
        Err(_) => None,
    };
    let l = local(repo);
    let named = repo.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let Some(record) = l.record.filter(|r| paths::key_of_real(&r.main) == named) else {
        return;
    };
    // Again, now that it is locked: a command may have run meanwhile.
    if !unused(l.last_used, SystemTime::now(), IDLE) {
        return;
    }
    let outside = look(&record.main, repo);
    if is_gone(record.devices, &outside) {
        // Busy: not stamped, so that the next walk asks again.
        moved.extend(remove_gone(repo, &record.main));
        return;
    }
    let main = (outside.main == Seen::Found).then_some(record.main.as_path());
    if pool::retire(&repo.join("pool"), main, moved) {
        let _ = fs::write(repo.join(STAMP), b"");
    }
}

/// `cache retire <repo directories>` (hidden): the detached process of the
/// walk. Only directories of repositories in this cache are looked at. It
/// starts one deleter for everything it moved into the trash.
pub fn retire(repos: &[PathBuf]) {
    let root = paths::cache_root();
    let mut moved = Vec::new();
    for repo in repos {
        let ours = repo.parent() == Some(root.as_path())
            && repo
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(cache::is_repo_name);
        if ours {
            retire_one(repo, &mut moved);
        }
    }
    trash::spawn_rm_many(&moved);
}

/// Start [`retire`] for `repos` in a process of its own, detached, at the
/// lowest priority, and do not wait for it. Nothing is started when there
/// is no `fast-worktree` executable to run (a program that embeds the
/// library without a helper): then those repositories stay as they are.
fn spawn_retire(repos: &[PathBuf]) {
    if repos.is_empty() {
        return;
    }
    let Some(exe) = util::background_exe() else {
        return;
    };
    let prepare = |mut c: Command| {
        c.args(["cache", "retire"])
            .args(repos)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for v in ["GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_PREFIX"] {
            c.env_remove(v);
        }
        crate::pending::detach(&mut c);
        c
    };
    let mut nice = Command::new("nice");
    nice.args(["-n", "19"]).arg(&exe);
    let child = prepare(nice)
        .spawn()
        .or_else(|_| prepare(Command::new(&exe)).spawn());
    if let Ok(child) = child {
        util::release(child);
    }
}

/// Whether a detached process is looking at `repo` right now.
fn being_examined(repo: &Path) -> bool {
    matches!(
        util::lock_path(&repo.join(MAIN_RECORD), false, false),
        Ok(None)
    )
}

/// The walk over the whole cache, made from a command in any repository:
/// in every repository's directory, what is unused, half made or left
/// without its entry ([`cache::prune_repos`]); and the repositories that
/// are not in use ([`candidate`]) are handed to one detached process
/// ([`retire`]). Returns whether nothing was left for the next walk
/// ([`WALK_LIMIT`]).
///
/// It must not hold up the command that makes it, nor be held up by
/// another repository: it reads and renames only inside the cache, takes
/// locks only without waiting, runs no git, and starts one deleter for
/// everything it moved. It never looks at where another repository is: a
/// network mount that is down would hold the command for ever. What needs
/// that is done by the detached process, and what needs more than that (a
/// repository's kept refs) by that repository's own commands ([`own`]).
fn walk(root: &Path) -> bool {
    let repos = cache::repo_dirs(root);
    let now = SystemTime::now();
    let idle: Vec<PathBuf> = repos
        .iter()
        .filter(|repo| {
            repo.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|named| candidate(named, &local(repo), now))
                && !being_examined(repo)
        })
        .take(WALK_LIMIT)
        .cloned()
        .collect();
    spawn_retire(&idle);
    let ttl = Duration::from_secs(cache::TTL_DAYS * 86_400);
    let (_, done) = cache::prune_repos(&repos, ttl, WALK_LIMIT);
    done
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
    fn a_record_is_read_back_with_or_without_its_devices() {
        let d = Devices {
            parent: 16_777_234,
            main: 16_777_234,
            cache: 7,
        };
        let main = Path::new("/work/my app\n2");
        for devices in [Some(d), None] {
            assert_eq!(
                decode(&encode(main, devices)),
                Some(Record {
                    main: main.to_path_buf(),
                    devices
                })
            );
        }
        // Not a record at all.
        assert_eq!(decode(b""), None);
        assert_eq!(decode(b"relative/path"), None);
        assert_eq!(decode(b"\x001 2 3"), None);
        // Numbers that cannot be read are no numbers: the path stands.
        for bad in ["", "1 2", "1 2 3 4", "1 2 x", "1  2 3", "-1 2 3"] {
            let bytes = [b"/work/app\0", bad.as_bytes()].concat();
            assert_eq!(
                decode(&bytes),
                Some(Record {
                    main: PathBuf::from("/work/app"),
                    devices: None
                }),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn the_record_is_written_once_and_then_only_touched() {
        let t = tempfile::tempdir().unwrap();
        let base = t.path().canonicalize().unwrap();
        let (main, repo) = (base.join("work/app"), base.join("cache/app-0123abcd"));
        fs::create_dir_all(&main).unwrap();
        fs::create_dir_all(&repo).unwrap();
        record_main(&repo, &main);
        let record = decode(&fs::read(repo.join(MAIN_RECORD)).unwrap()).unwrap();
        assert_eq!(record.main, main);
        let d = record.devices.expect("the devices are recorded");
        assert_eq!(d.parent, fs::metadata(base.join("work")).unwrap().dev());
        assert_eq!(d.main, d.parent);
        assert_eq!(d.cache, fs::metadata(&repo).unwrap().dev());
        // The next command: the same inode, a new time.
        let ino = fs::metadata(repo.join(MAIN_RECORD)).unwrap().ino();
        let old = SystemTime::now() - Duration::from_secs(8 * 86_400);
        fs::File::options()
            .write(true)
            .open(repo.join(MAIN_RECORD))
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert!(unused(local(&repo).last_used, SystemTime::now(), IDLE));
        record_main(&repo, &main);
        assert_eq!(fs::metadata(repo.join(MAIN_RECORD)).unwrap().ino(), ino);
        assert!(!unused(local(&repo).last_used, SystemTime::now(), IDLE));
        // A record without the devices (an earlier format) is rewritten.
        fs::write(repo.join(MAIN_RECORD), encode(&main, None)).unwrap();
        record_main(&repo, &main);
        let again = decode(&fs::read(repo.join(MAIN_RECORD)).unwrap()).unwrap();
        assert_eq!(again.devices, Some(d));
    }

    #[test]
    fn a_pool_that_changed_says_the_repository_is_in_use() {
        // What a fill or an `add` of a version that does not write the
        // record does: it makes or takes a spare.
        let t = tempfile::tempdir().unwrap();
        let repo = t.path().join("app-0123abcd");
        fs::create_dir_all(&repo).unwrap();
        assert_eq!(local(&repo).last_used, None, "no record: unknown");
        fs::write(repo.join(MAIN_RECORD), b"/work/app").unwrap();
        let old = SystemTime::now() - Duration::from_secs(8 * 86_400);
        fs::File::options()
            .write(true)
            .open(repo.join(MAIN_RECORD))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let now = SystemTime::now();
        assert!(unused(local(&repo).last_used, now, IDLE));
        fs::create_dir_all(repo.join("pool")).unwrap();
        assert!(!unused(local(&repo).last_used, now, IDLE));
    }

    #[test]
    fn a_repository_is_unused_from_exactly_seven_days() {
        let now = SystemTime::now();
        let ago = |s: u64| Some(now - Duration::from_secs(s));
        let week = 7 * 86_400;
        assert_eq!(IDLE.as_secs(), week);
        assert!(!unused(ago(week - 1), now, IDLE));
        assert!(unused(ago(week), now, IDLE));
        assert!(unused(ago(week + 1), now, IDLE));
        assert!(!unused(ago(0), now, IDLE));
        // Unknown, or from the future: in use.
        assert!(!unused(None, now, IDLE));
        assert!(!unused(Some(now + Duration::from_secs(60)), now, IDLE));
    }

    #[test]
    fn the_walk_hands_over_only_what_the_cache_says_is_unused_and_its_own() {
        let now = SystemTime::now();
        let ago = |s: u64| Some(now - Duration::from_secs(s));
        let main = PathBuf::from("/nonexistent/work/app");
        let named = paths::key_of_real(&main);
        let record = |devices| {
            Some(Record {
                main: main.clone(),
                devices,
            })
        };
        let l = |record, used: u64, examined| Local {
            record,
            last_used: ago(used),
            examined,
        };
        let week = IDLE.as_secs();
        assert!(candidate(&named, &l(record(None), week, None), now));
        assert!(!candidate(&named, &l(record(None), week - 1, None), now));
        // No record: an older version's directory stays.
        assert!(!candidate(&named, &l(None, 10 * week, None), now));
        // A record that is another directory's proves nothing here.
        assert!(!candidate(
            "other-0123abcd",
            &l(record(None), week, None),
            now
        ));
        // Looked at less than a day ago: not again yet.
        let day = RECHECK.as_secs();
        assert!(!candidate(
            &named,
            &l(record(None), week, ago(day - 1)),
            now
        ));
        assert!(candidate(&named, &l(record(None), week, ago(day)), now));
        // The key is the hash of the recorded bytes: nothing is resolved.
        assert_eq!(named, paths::repo_key(&main));
    }

    #[test]
    fn a_repository_is_gone_only_when_everything_says_so() {
        let on = |parent, main, cache| {
            Some(Devices {
                parent,
                main,
                cache,
            })
        };
        let now = |parent, cache, main| Outside {
            parent,
            cache,
            main,
        };
        use Seen::{Found, NotFound, Unknown};
        // On the cache's volume, deleted.
        assert!(is_gone(on(5, 5, 5), &now(Some(5), Some(5), NotFound)));
        // The device numbers changed with a restart, for both alike.
        assert!(is_gone(on(5, 5, 5), &now(Some(9), Some(9), NotFound)));
        // The checkout is there, or cannot be looked at.
        assert!(!is_gone(on(5, 5, 5), &now(Some(5), Some(5), Found)));
        assert!(!is_gone(on(5, 5, 5), &now(Some(5), Some(5), Unknown)));
        // No devices in the record (an earlier format): never.
        assert!(!is_gone(None, &now(Some(5), Some(5), NotFound)));
        // The directory it was in is missing or unreadable.
        assert!(!is_gone(on(5, 5, 5), &now(None, Some(5), NotFound)));
        // It was on the cache's volume and that directory is now on
        // another device, or the cache cannot be looked at.
        assert!(!is_gone(on(5, 5, 5), &now(Some(6), Some(5), NotFound)));
        assert!(!is_gone(on(5, 5, 5), &now(Some(5), None, NotFound)));
        // On another volume than the cache (a disk at /mnt/data): deleted
        // while the disk is mounted.
        assert!(is_gone(on(8, 8, 5), &now(Some(8), Some(5), NotFound)));
        // The disk is not mounted: /mnt/data is a directory of the root
        // volume.
        assert!(!is_gone(on(8, 8, 5), &now(Some(5), Some(5), NotFound)));
        assert!(!is_gone(on(8, 8, 5), &now(Some(9), Some(5), NotFound)));
        // The checkout was the root of a mount itself (/Volumes/X): its
        // mount point may be missing while the volume is away.
        assert!(!is_gone(on(5, 8, 5), &now(Some(5), Some(5), NotFound)));
    }

    #[test]
    fn a_missing_path_is_told_from_one_that_cannot_be_looked_at() {
        let t = tempfile::tempdir().unwrap();
        fs::write(t.path().join("file"), "").unwrap();
        assert_eq!(seen(t.path()), Seen::Found);
        assert_eq!(seen(&t.path().join("missing")), Seen::NotFound);
        // Not a directory on the way: an error, but not "no such file".
        assert_eq!(seen(&t.path().join("file/below")), Seen::Unknown);
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
