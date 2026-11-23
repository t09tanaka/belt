//! flock helpers: the ledger lock, per-entry lease locks, and the liveness
//! probe that reaps dead entries (spec "Ledger and locking").

use crate::error::{Error, Result};
use crate::ledger::Ledger;
use crate::Context;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(20);
pub const LEDGER_LOCK_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `try_ledger` waits before giving up a round, short so a
/// waiting `exec` still answers signals promptly.
const TRY_ROUND: Duration = Duration::from_secs(1);

/// A held exclusive lock, released on drop.
#[derive(Debug)]
pub struct Lock {
    file: File,
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn open_lock_file(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?)
}

/// Whether `path` still names the file `file` is open on (same device and
/// inode). A lock file may be unlinked by whoever finds it unused, so a lock
/// only counts once this holds *after* the lock was taken: otherwise the
/// lock is on a file nobody else can find, and the next taker would lock a
/// new file of the same name at the same time.
pub(crate) fn names(file: &File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (file.metadata(), std::fs::metadata(path)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

/// Exclusive lock with polling and timeout. Only contention is retried.
/// The lock returned is on the file `path` names now ([`names`]).
pub fn acquire(path: &Path, timeout: Duration) -> Result<Lock> {
    let start = Instant::now();
    loop {
        let file = open_lock_file(path)?;
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed() >= timeout {
                        return Err(Error::LockTimeout(format!(
                            "timed out after {timeout:?} waiting for {}",
                            path.display()
                        )));
                    }
                    thread::sleep(POLL);
                }
                Err(e) => return Err(e.into()),
            }
        }
        if names(&file, path) {
            return Ok(Lock { file });
        }
        release(&file);
    }
}

/// Unlock explicitly. Closing the file is not enough: a process forked by
/// another thread at this moment holds a copy of the descriptor until it
/// execs, and the lock would stay with that copy.
fn release(file: &File) {
    let _ = FileExt::unlock(file);
}

/// [`acquire`] without waiting: `None` when the lock is busy.
pub fn try_acquire(path: &Path) -> Result<Option<Lock>> {
    match acquire(path, Duration::ZERO) {
        Ok(lock) => Ok(Some(lock)),
        Err(Error::LockTimeout(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Remove the lock file `path` unless somebody holds it. The file is
/// unlinked while this call holds its exclusive lock, taken without
/// waiting, so a taker that had already opened it either finds it busy or
/// gets the lock on a file the name no longer points to, notices
/// ([`names`]) and starts again. True when the file is gone afterwards.
pub fn remove_if_unheld(path: &Path) -> bool {
    loop {
        let Ok(file) = OpenOptions::new().read(true).write(true).open(path) else {
            return std::fs::symlink_metadata(path).is_err();
        };
        match file.try_lock_exclusive() {
            Ok(()) => {
                let gone = names(&file, path).then(|| std::fs::remove_file(path).is_ok());
                release(&file);
                match gone {
                    Some(gone) => return gone,
                    // Unlinked (and perhaps made again) meanwhile: look
                    // again.
                    None => continue,
                }
            }
            Err(_) => return false,
        }
    }
}

pub fn ledger(ctx: &Context) -> Result<Lock> {
    acquire(&ctx.paths.ledger_lock(), ctx.ledger_lock_timeout)
}

/// The ledger lock, or `None` when it stayed busy for a round (at most
/// `TRY_ROUND`), for callers that can skip a round and try again later.
pub fn try_ledger(ctx: &Context) -> Result<Option<Lock>> {
    match acquire(
        &ctx.paths.ledger_lock(),
        ctx.ledger_lock_timeout.min(TRY_ROUND),
    ) {
        Ok(lock) => Ok(Some(lock)),
        Err(Error::LockTimeout(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The ledger with dead entries removed, for readers.
pub fn load_reaped(ctx: &Context) -> Result<Ledger> {
    Ok(snapshot(ctx)?.ledger)
}

/// A ledger read by `snapshot`.
pub struct Snapshot {
    pub ledger: Ledger,
    /// False when the lock stayed busy and the file was read without it,
    /// so an entry still waiting for the lock to register is missing.
    pub locked: bool,
}

/// The ledger with dead entries removed. Under the lock the removal is
/// saved. When the lock stays busy, the file is read without it (saves
/// are atomic renames, so it is never half-written) and dead entries are
/// only left out of the returned copy.
pub fn snapshot(ctx: &Context) -> Result<Snapshot> {
    refresh_verdicts(ctx);
    let Some(_guard) = try_ledger(ctx)? else {
        let mut ledger = Ledger::load(&ctx.paths.ledger_file())?;
        // Without the lock nothing is reaped: a managed entry is kept
        // whatever its lease says (the reaper decides under the lock).
        ledger
            .entries
            .retain(|e| lease_is_held(ctx, &e.id) || ctx.paths.managed_file(&e.id).exists());
        return Ok(Snapshot {
            ledger,
            locked: false,
        });
    };
    let mut ledger = Ledger::load(&ctx.paths.ledger_file())?;
    if !reap_dead(ctx, &mut ledger).is_empty() {
        ledger.save(&ctx.paths.ledger_file())?;
    }
    Ok(Snapshot {
        ledger,
        locked: true,
    })
}

/// A ledger read without changing anything on disk.
pub struct ReadOnlySnapshot {
    pub ledger: Ledger,
    /// False when the lock file is missing or stayed exclusively held for
    /// a round, so the file was read without it.
    pub locked: bool,
    /// Ids whose lease is not held. They are kept in `ledger`: whether the
    /// owner is gone is the reaper's call, a reader must not count them as
    /// free capacity.
    pub suspect_dead: Vec<String>,
}

/// The ledger as it is, for readers that must not write (forecast). Unlike
/// [`snapshot`] nothing is created, reaped or saved: the lock file is only
/// opened if it already exists, and a shared lock (never an exclusive one)
/// is held while reading.
pub fn snapshot_read_only(ctx: &Context) -> Result<ReadOnlySnapshot> {
    let guard = match OpenOptions::new().read(true).open(ctx.paths.ledger_lock()) {
        Ok(file) => {
            let start = Instant::now();
            loop {
                match FileExt::try_lock_shared(&file) {
                    Ok(()) => break Some(file),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        if start.elapsed() >= ctx.ledger_lock_timeout.min(TRY_ROUND) {
                            break None;
                        }
                        thread::sleep(POLL);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let locked = guard.is_some();
    // Probe the leases while the shared lock is still held: an owner
    // removes its entry under the exclusive lock before dropping the
    // lease, so every entry read here still has its lease held.
    let read = Ledger::load(&ctx.paths.ledger_file()).map(|ledger| {
        let suspect_dead: Vec<String> = ledger
            .entries
            .iter()
            .filter(|e| !lease_is_held(ctx, &e.id))
            .map(|e| e.id.clone())
            .collect();
        (ledger, suspect_dead)
    });
    if let Some(f) = &guard {
        let _ = FileExt::unlock(f);
    }
    let (ledger, suspect_dead) = read?;
    Ok(ReadOnlySnapshot {
        ledger,
        locked,
        suspect_dead,
    })
}

/// Take the lease for `id`. Held by the owning task-traffic-controller process until it
/// exits; the fd is close-on-exec (Rust std default) so children never
/// inherit it.
pub fn take_lease(ctx: &Context, id: &str) -> Result<Lock> {
    let path = ctx.paths.lease_file(id);
    // Whoever looks whether a lease is held, or removes one nobody holds,
    // has it locked for an instant (and a released version looks at every
    // file in `leases/` during `gc`). That is not an owner: ask again for a
    // moment before believing it.
    let start = Instant::now();
    loop {
        if let Some(lock) = try_take_lease(ctx, id)? {
            return Ok(lock);
        }
        if start.elapsed() >= LEASE_PROBE_WAIT {
            return Err(Error::Ledger(format!(
                "lease {} already held",
                path.display()
            )));
        }
        thread::sleep(Duration::from_millis(2));
    }
}

/// How long [`take_lease`] takes a busy lease for somebody's look at it.
const LEASE_PROBE_WAIT: Duration = Duration::from_millis(250);

/// [`take_lease`], but `None` when another process holds it.
pub fn try_take_lease(ctx: &Context, id: &str) -> Result<Option<Lock>> {
    let path = ctx.paths.lease_file(id);
    loop {
        let file = open_lock_file(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) if names(&file, &path) => return Ok(Some(Lock { file })),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && names(&file, &path) => {
                return Ok(None)
            }
            // The file was unlinked between the open and the lock: the
            // lock (or its refusal) is about a file that is gone.
            Ok(()) => release(&file),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => {
                return Err(Error::Ledger(format!(
                    "lease {} cannot be locked: {e}",
                    path.display()
                )))
            }
        }
    }
}

/// True when some process currently holds the lease lock for `id`.
pub fn lease_is_held(ctx: &Context, id: &str) -> bool {
    #[cfg(test)]
    LEASE_LOOKS.with(|n| n.set(n.get() + 1));
    is_held(&ctx.paths.lease_file(id))
}

#[cfg(test)]
thread_local! {
    /// How often this thread looked at a lease ([`lease_is_held`]).
    pub(crate) static LEASE_LOOKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Take a *shared* lock on `path` (several holders allowed), held until drop.
pub fn take_shared(path: &Path) -> Result<Lock> {
    loop {
        let file = open_lock_file(path)?;
        file.lock_shared()?;
        if names(&file, path) {
            return Ok(Lock { file });
        }
        release(&file);
    }
}

/// True when at least one shared or exclusive holder exists for `path`.
pub fn is_held(path: &Path) -> bool {
    loop {
        let Ok(file) = OpenOptions::new().read(true).write(true).open(path) else {
            return false;
        };
        match file.try_lock_exclusive() {
            Ok(()) => {
                // Unlocked at once and explicitly: this look must not be
                // taken for a holder by the next one.
                release(&file);
                if names(&file, path) {
                    return false;
                }
                // Free, but not the file the name points to any more.
            }
            Err(e) => return e.kind() == std::io::ErrorKind::WouldBlock,
        }
    }
}

/// One assessment of a ledger entry, kept by the context that made it.
#[derive(Debug, Clone)]
pub struct Cached {
    /// The ledger entry it was made for (a new registration under the same
    /// id has another sequence number).
    pub seq: u64,
    /// When the assessment began.
    pub at: chrono::DateTime<chrono::Utc>,
    pub assessment: crate::managed::Assessment,
}

fn cache_of(ctx: &Context) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, Cached>> {
    ctx.reap_cache.lock().unwrap_or_else(|p| p.into_inner())
}

/// Assess, outside the ledger lock, every entry whose owner lease is not
/// held, and remember the answers for [`reap_dead`]. A process scan takes
/// hundreds of milliseconds, which nobody should wait for holding the
/// lock, and an entry that is kept is looked at again only every
/// [`crate::reap::REASSESS_SECS`] (per context), so waiters polling twice a
/// second do not rescan every process each time. Call it before taking the
/// lock; it takes none and writes nothing.
pub fn refresh_verdicts(ctx: &Context) {
    let Ok(ledger) = Ledger::load(&ctx.paths.ledger_file()) else {
        return;
    };
    cache_of(ctx).retain(|id, _| ledger.find(id).is_some());
    for e in &ledger.entries {
        if lease_is_held(ctx, &e.id) {
            cache_of(ctx).remove(&e.id);
            continue;
        }
        let now = ctx.clock.now();
        let prev = cache_of(ctx)
            .get(&e.id)
            .filter(|c| c.seq == e.seq)
            .map(|c| {
                (
                    c.assessment.verdict.is_release(),
                    (now - c.at).num_seconds(),
                )
            });
        if !crate::reap::reassess_due(prev, ctx.reassess_secs) {
            continue;
        }
        let assessment = crate::managed::assess(ctx, &e.id);
        cache_of(ctx).insert(
            e.id.clone(),
            Cached {
                seq: e.seq,
                at: now,
                assessment,
            },
        );
    }
}

/// The assessment to act on for `e` under the lock, when it is a release
/// reached recently, for this very entry and for the sidecar as it is now.
/// A kept answer is never returned: the entry is only released on a fresh
/// assessment that said so.
fn usable_release(ctx: &Context, e: &crate::ledger::Entry) -> Option<crate::managed::Assessment> {
    let c = cache_of(ctx).get(&e.id)?.clone();
    let age = (ctx.clock.now() - c.at).num_seconds();
    (c.seq == e.seq
        && c.assessment.verdict.is_release()
        && crate::reap::release_usable(age)
        && crate::managed::load_sidecar(ctx, &e.id) == c.assessment.sidecar)
        .then_some(c.assessment)
}

/// Remove ledger entries whose owner is gone, and their lease files.
/// Returns the ids removed. Caller holds the ledger lock.
///
/// A plain entry is gone when its lease is not held. A managed entry (one
/// with a sidecar, see [`crate::managed`]) also needs its child process
/// tree to be known stopped, or the machine to have rebooted; until then it
/// stays, charge and all. That proof needs process scans, which are not made
/// here: they are made by [`refresh_verdicts`] before the lock is taken, and
/// a managed entry is released here only on such a recent answer that still
/// matches the entry and its sidecar. Without one, only what needs no scan
/// (a plain entry, a finished or never-launched run, a reboot) is decided.
pub fn reap_dead(ctx: &Context, ledger: &mut Ledger) -> Vec<String> {
    let mut dead: Vec<String> = Vec::new();
    for e in &ledger.entries {
        if lease_is_held(ctx, &e.id) {
            continue;
        }
        let a =
            usable_release(ctx, e).unwrap_or_else(|| crate::managed::assess_unscanned(ctx, &e.id));
        if a.verdict.is_release() {
            crate::managed::settle_released(ctx, &a);
            dead.push(e.id.clone());
        }
    }
    for id in &dead {
        ledger.remove(id);
        cache_of(ctx).remove(id);
        remove_if_unheld(&ctx.paths.lease_file(id));
    }
    dead
}

/// Whether the lease file of an id without a ledger entry is a leftover: it
/// must be [`ORPHAN_LEASE_AFTER_SECS`] old. A younger one may belong to a
/// command between taking its lease and registering, and released versions
/// of this tool lock the path they opened without looking again, so the
/// file must not go while one of them may be between its open and its lock.
pub fn orphan_lease_due(
    modified: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    now.signed_duration_since(modified) >= chrono::Duration::seconds(ORPHAN_LEASE_AFTER_SECS)
}

/// See [`orphan_lease_due`]. An hour: the open and the lock are two system
/// calls apart.
pub const ORPHAN_LEASE_AFTER_SECS: i64 = 3600;

/// Remove lease files that no ledger entry refers to (left by crashes
/// between lease creation and ledger registration), once they are
/// [`ORPHAN_LEASE_AFTER_SECS`] old and only while nobody holds them
/// ([`remove_if_unheld`]). Returns how many were removed.
pub fn sweep_orphan_leases(ctx: &Context, ledger: &Ledger) -> usize {
    let old = old_lease_ids(ctx, &mut || true);
    remove_orphan_leases(ctx, ledger, &old, &mut || true)
}

/// The ids of the lease files that are [`ORPHAN_LEASE_AFTER_SECS`] old: the
/// ones [`remove_orphan_leases`] may remove. Only a listing; no lock is
/// needed or touched. `go` is asked before each file; false stops.
pub(crate) fn old_lease_ids(ctx: &Context, go: &mut dyn FnMut() -> bool) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(ctx.paths.leases_dir()) else {
        return Vec::new();
    };
    let now = ctx.clock.now();
    let mut ids = Vec::new();
    for ent in rd.flatten() {
        if !go() {
            break;
        }
        let name = ent.file_name();
        let Some(id) = name.to_str().and_then(|n| n.strip_suffix(".lock")) else {
            continue;
        };
        if ent
            .metadata()
            .and_then(|m| m.modified())
            .is_ok_and(|t| orphan_lease_due(t.into(), now))
        {
            ids.push(id.to_string());
        }
    }
    ids
}

/// Of the old lease files `ids` ([`old_lease_ids`]), remove those `ledger`
/// has no entry for and nobody holds. The caller holds the ledger lock and
/// read `ledger` under it. A lease is only looked at (locked for an
/// instant) here, after its age and the ledger said it is a leftover.
/// `allow` is asked before each; false stops.
pub(crate) fn remove_orphan_leases(
    ctx: &Context,
    ledger: &Ledger,
    ids: &[String],
    allow: &mut dyn FnMut() -> bool,
) -> usize {
    let mut n = 0;
    for id in ids {
        if ledger.find(id).is_some() {
            continue;
        }
        if !allow() {
            break;
        }
        if remove_if_unheld(&ctx.paths.lease_file(id)) {
            n += 1;
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admit::State;
    use crate::ledger::Entry;
    use crate::paths::Paths;
    use chrono::Utc;

    fn entry(id: &str, seq: u64) -> Entry {
        Entry {
            id: id.into(),
            pid: 1,
            state: State::Running,
            front: false,
            weight: 1,
            predicted_rss: 0,
            expected_secs: None,
            repo: String::new(),
            subdir: String::new(),
            argv: "x".into(),
            key: "k".into(),
            seq,
            enqueued_at: Utc::now(),
            started_at: Some(Utc::now()),
            using: None,
            backfilled: false,
            label: None,
            worktree: None,
            branch: None,
            parent: None,
            nested_cpu_secs: 0.0,
            extra: Default::default(),
        }
    }

    fn state_with(ids: &[&str]) -> (tempfile::TempDir, Context) {
        let d = tempfile::tempdir().unwrap();
        let ctx = Context::new(Paths::new(d.path().join("state"), d.path().join("c.toml")));
        let mut l = Ledger::default();
        for id in ids {
            let s = l.take_seq();
            l.entries.push(entry(id, s));
        }
        drop(ledger(&ctx).unwrap());
        l.save(&ctx.paths.ledger_file()).unwrap();
        (d, ctx)
    }

    #[test]
    fn a_lock_counts_only_on_the_file_its_name_points_to() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("x.lock");
        let file = open_lock_file(&path).unwrap();
        assert!(names(&file, &path));
        // Unlinked: the open file is nobody's.
        std::fs::remove_file(&path).unwrap();
        assert!(!names(&file, &path));
        // Made again under the same name: another file.
        let again = open_lock_file(&path).unwrap();
        assert!(!names(&file, &path));
        assert!(names(&again, &path));
        // A lock on the unlinked file holds nobody off the new one, and
        // does not make the name look held.
        file.lock_exclusive().unwrap();
        assert!(!is_held(&path));
        let got = acquire(&path, Duration::ZERO).unwrap();
        assert!(names(&got.file, &path));
        assert!(is_held(&path));
    }

    #[test]
    fn a_lock_file_goes_only_when_nobody_holds_it() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("x.lock");
        // Nothing there: gone already.
        assert!(remove_if_unheld(&path));
        let held = take_shared(&path).unwrap();
        assert!(!remove_if_unheld(&path));
        assert!(path.exists());
        drop(held);
        assert!(remove_if_unheld(&path));
        assert!(!path.exists());
        let held = acquire(&path, Duration::ZERO).unwrap();
        assert!(try_acquire(&path).unwrap().is_none());
        assert!(!remove_if_unheld(&path));
        drop(held);
        assert!(try_acquire(&path).unwrap().is_some());
    }

    #[test]
    fn a_busy_lease_is_asked_again_for_a_moment_and_then_believed() {
        let (_d, ctx) = state_with(&[]);
        let held = take_lease(&ctx, "a").unwrap();
        assert!(try_take_lease(&ctx, "a").unwrap().is_none());
        let start = Instant::now();
        assert!(take_lease(&ctx, "a").is_err());
        assert!(start.elapsed() >= LEASE_PROBE_WAIT);
        // Somebody only looking: gone within the wait.
        let ctx2 = ctx.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let t = thread::spawn(move || {
            tx.send(()).unwrap();
            take_lease(&ctx2, "a").is_ok()
        });
        rx.recv().unwrap();
        drop(held);
        assert!(t.join().unwrap());
    }

    #[test]
    fn a_lease_without_an_entry_is_a_leftover_only_after_an_hour() {
        let now = Utc::now();
        let at = |secs| now - chrono::Duration::seconds(secs);
        assert!(!orphan_lease_due(at(ORPHAN_LEASE_AFTER_SECS - 1), now));
        assert!(orphan_lease_due(at(ORPHAN_LEASE_AFTER_SECS), now));
        assert!(!orphan_lease_due(at(-5), now));
    }

    #[test]
    fn read_only_snapshot_judges_leases_under_the_shared_lock() {
        let (_d, ctx) = state_with(&["a", "b"]);
        let lease = take_lease(&ctx, "a").unwrap();
        let snap = snapshot_read_only(&ctx).unwrap();
        assert!(snap.locked);
        assert_eq!(snap.ledger.entries.len(), 2);
        // "a" has its owner, "b" has none; nothing is removed.
        assert_eq!(snap.suspect_dead, vec!["b".to_string()]);
        drop(lease);
        let snap = snapshot_read_only(&ctx).unwrap();
        assert_eq!(snap.suspect_dead.len(), 2);
    }

    #[test]
    fn read_only_snapshot_releases_its_shared_lock() {
        let (_d, ctx) = state_with(&["a"]);
        let _lease = take_lease(&ctx, "a").unwrap();
        snapshot_read_only(&ctx).unwrap();
        // An exclusive lock is available right after.
        let g = acquire(&ctx.paths.ledger_lock(), Duration::from_millis(100));
        assert!(g.is_ok());
    }

    #[test]
    fn read_only_snapshot_without_a_lock_file_creates_nothing() {
        let d = tempfile::tempdir().unwrap();
        let ctx = Context::new(Paths::new(d.path().join("state"), d.path().join("c.toml")));
        let snap = snapshot_read_only(&ctx).unwrap();
        assert!(!snap.locked);
        assert!(snap.ledger.entries.is_empty());
        assert!(!d.path().join("state").exists());
    }

    /// Run `snapshot_read_only` on its own thread, so the caller can hold
    /// the exclusive ledger lock meanwhile.
    fn snapshot_in_thread(
        ctx: &Context,
    ) -> std::sync::mpsc::Receiver<(ReadOnlySnapshot, Duration)> {
        let (tx, rx) = std::sync::mpsc::channel();
        let ctx = ctx.clone();
        thread::spawn(move || {
            let start = Instant::now();
            let snap = snapshot_read_only(&ctx).unwrap();
            let _ = tx.send((snap, start.elapsed()));
        });
        rx
    }

    /// Deterministic in the direction that matters: while an exclusive
    /// lock is held the snapshot must not return (the wait is bounded by
    /// `TRY_ROUND`, 1 s, far above the 200 ms checked here), and once the
    /// lock is released it must take the shared lock (`locked == true`)
    /// instead of reading without it.
    #[test]
    fn read_only_snapshot_waits_for_the_exclusive_lock() {
        let (_d, ctx) = state_with(&["a"]);
        let _lease = take_lease(&ctx, "a").unwrap();
        let held = ledger(&ctx).unwrap();
        let rx = snapshot_in_thread(&ctx);
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the snapshot finished while the exclusive lock was held"
        );
        drop(held);
        let (snap, _) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(snap.locked);
        assert_eq!(snap.ledger.entries.len(), 1);
    }

    /// If the exclusive lock stays held past the (shortened) round, the
    /// snapshot falls back to reading without the lock: `locked == false`,
    /// and not before the round has elapsed.
    #[test]
    fn read_only_snapshot_falls_back_unlocked_after_the_round() {
        let (_d, ctx) = state_with(&["a"]);
        let ctx = ctx.with_ledger_lock_timeout(Duration::from_millis(300));
        let _lease = take_lease(&ctx, "a").unwrap();
        let _held = ledger(&ctx).unwrap();
        let rx = snapshot_in_thread(&ctx);
        let (snap, took) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(!snap.locked);
        assert!(took >= Duration::from_millis(300), "{took:?}");
        assert_eq!(snap.ledger.entries.len(), 1);
    }
}
