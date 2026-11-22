//! Fast deletion: rename into a trash directory on the same volume (a
//! constant-time metadata operation), then delete in a detached child.

use crate::paths;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Trash entries older than this are assumed to be orphaned (their deleter
/// was killed) and are deleted again by the next run.
const SWEEP_AFTER_SECS: u64 = 600;

/// Name prefix of the hidden sibling used when the trash directory is on
/// another volume.
const BESIDE_PREFIX: &str = ".fast-worktree-trash-";

/// Name prefix of a note in the trash directory that records where such a
/// sibling is.
const NOTE_PREFIX: &str = "beside-";

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A name that is unique across threads and concurrent processes:
/// `<unix seconds>-<pid>-<counter><nanos>-<basename>`.
fn unique_name(path: &Path) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let base = path
        .file_name()
        .map(|b| b.to_string_lossy().into_owned())
        .unwrap_or_else(|| "x".into());
    format!("{}-{}-{n}{nanos}-{base}", now_secs(), std::process::id())
}

/// Name prefix of trash entries that are kept for a while instead of being
/// deleted at once: `keep-<unix seconds until>-...`.
const KEEP_PREFIX: &str = "keep-";

/// Move `path` out of the way and delete it in the background. Returns
/// where it went. Falls back to a hidden sibling when the trash directory
/// is on another volume, so the rename stays cheap.
pub fn trash(path: &Path) -> io::Result<PathBuf> {
    let dest = move_aside(path)?;
    spawn_rm(&dest);
    Ok(dest)
}

/// Move `path` into the trash as [`trash`] does, but start no deleter:
/// the caller takes what it needs out of it first, then calls
/// [`spawn_rm`]. One left there is deleted by a later [`sweep`].
pub fn move_aside(path: &Path) -> io::Result<PathBuf> {
    move_away(path, &unique_name(path))
}

/// Move `path` into the trash and leave it there for at least `secs`
/// seconds: a later run deletes it once that time has passed. Returns where
/// it went.
pub fn keep(path: &Path, secs: u64) -> io::Result<PathBuf> {
    let name = format!("{KEEP_PREFIX}{}-{}", now_secs() + secs, unique_name(path));
    move_away(path, &name)
}

/// Rename `path` into the trash as `name` (or next to itself when the trash
/// is on another volume).
fn move_away(path: &Path, name: &str) -> io::Result<PathBuf> {
    let dir = paths::trash_dir();
    let dest = match fs::create_dir_all(&dir) {
        Ok(()) => {
            let d = dir.join(name);
            let moved = fs::rename(path, &d).or_else(|e| {
                // The trash was empty and a sweep removed it just now.
                if e.kind() == io::ErrorKind::NotFound && path.exists() {
                    fs::create_dir_all(&dir)?;
                    fs::rename(path, &d)
                } else {
                    Err(e)
                }
            });
            match moved {
                Ok(()) => Some(d),
                Err(e) if e.raw_os_error() == Some(libc::EXDEV) => None,
                Err(e) => return Err(e),
            }
        }
        Err(_) => None,
    };
    let dest = match dest {
        Some(d) => d,
        None => {
            let d = beside(path, name, &dir)?;
            // The siblings of earlier removals here whose note was lost
            // are found by their names, where the next one is put.
            if let Some(parent) = d.parent() {
                sweep_siblings(&[parent.to_path_buf()]);
            }
            d
        }
    };
    Ok(dest)
}

/// Rename `path` to a hidden sibling, and leave a note in the trash
/// directory `dir` saying where it went, so that [`sweep`] finds it once
/// its name says it is stale: when its deleter was interrupted, or, for
/// [`keep`], when its time is up. The note is written first (after making
/// the trash directory again when it is gone), so that a sibling is never
/// there without one; when it cannot be written at all, that is said, and
/// the sibling is found by its name instead ([`sweep_siblings`]). Whether a
/// deleter starts at once is up to the caller: [`trash`] starts one,
/// [`keep`] does not.
fn beside(path: &Path, name: &str, dir: &Path) -> io::Result<PathBuf> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let file = format!("{BESIDE_PREFIX}{name}");
    let d = parent.join(&file);
    let real = parent
        .canonicalize()
        .map_or_else(|_| d.clone(), |p| p.join(&file));
    let note = dir.join(format!("{NOTE_PREFIX}{name}"));
    let write = || fs::write(&note, real.as_os_str().as_bytes());
    let noted = write().or_else(|_| fs::create_dir_all(dir).and_then(|()| write()));
    if let Err(e) = fs::rename(path, &d) {
        if noted.is_ok() {
            let _ = fs::remove_file(&note);
        }
        return Err(e);
    }
    if let Err(e) = noted {
        crate::util::errln!(
            "fast-worktree: {} could not be recorded in {} ({e}); it is deleted by a later \
             command in this repository, or by the next removal next to it",
            d.display(),
            dir.display()
        );
    }
    Ok(d)
}

/// The hidden siblings in the directories `parents` whose names say they
/// are stale: only names made here ([`BESIDE_PREFIX`] and a time).
fn stale_siblings(parents: &[PathBuf], now: u64) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for parent in parents {
        for e in fs::read_dir(parent).into_iter().flatten().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name
                .strip_prefix(BESIDE_PREFIX)
                .is_some_and(|rest| is_stale(rest, now))
            {
                out.push(e.path());
            }
        }
    }
    out
}

/// Delete the stale hidden siblings in `parents`: what a removal on another
/// volume than the trash left there, whether or not the trash still has
/// its note.
pub fn sweep_siblings(parents: &[PathBuf]) {
    spawn_rm_many(&stale_siblings(parents, now_secs()));
}

/// Whether `path` can be renamed away at all: its parent directory is
/// writable and `path` is not a mount point (a rename never crosses
/// volumes). Checked before anything that cannot be undone, such as the
/// `pre_remove` hooks.
pub fn can_move(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let parent = path.parent().unwrap_or(Path::new("/"));
    let dev = |p: &Path| p.metadata().map(|m| m.dev());
    match (dev(path), dev(parent)) {
        (Ok(a), Ok(b)) if a != b => return Err(format!("{} is a mount point", path.display())),
        (Err(e), _) | (_, Err(e)) => return Err(e.to_string()),
        _ => {}
    }
    let c = std::ffi::CString::new(parent.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    // SAFETY: a valid NUL-terminated path.
    if unsafe { libc::access(c.as_ptr(), libc::W_OK | libc::X_OK) } != 0 {
        return Err(format!(
            "{} is not writable ({})",
            parent.display(),
            io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// Start `rm -rf <path>` in its own process group with no stdio, and do
/// not wait for it: it outlives this process and ignores the terminal's
/// Ctrl-C.
pub fn spawn_rm(path: &Path) {
    spawn_rm_many(&[path.to_path_buf()]);
}

/// The most bytes of paths one deleter is given (far below what a command
/// line may hold).
const ARGS_LIMIT: usize = 128 * 1024;

/// One deleter for all of `paths` ([`spawn_rm`] for many): a walk that
/// moves hundreds of entries away starts one process, not one each. Only
/// the paths given are deleted, never the trash directory as a whole: it
/// also holds what is kept for a day, and what a running command has just
/// moved there. Returns how many processes were started (more than one
/// only past [`ARGS_LIMIT`]).
pub fn spawn_rm_many(paths: &[PathBuf]) -> usize {
    if paths.is_empty() {
        return 0;
    }
    let dir = paths::trash_dir();
    let lock = fs::create_dir_all(&dir).ok().map(|()| dir.join(".lock"));
    let groups = chunks(paths, ARGS_LIMIT);
    for group in &groups {
        spawn_rm_queued(group, lock.as_deref());
    }
    groups.len()
}

/// `paths` in order, split so that no group's paths are longer than
/// `limit` bytes together (a single longer path is a group of its own).
fn chunks(paths: &[PathBuf], limit: usize) -> Vec<&[PathBuf]> {
    let mut out = Vec::new();
    let (mut start, mut bytes) = (0, 0);
    for (i, p) in paths.iter().enumerate() {
        let len = p.as_os_str().len() + 1;
        if i > start && bytes + len > limit {
            out.push(&paths[start..i]);
            (start, bytes) = (i, 0);
        }
        bytes += len;
    }
    if start < paths.len() {
        out.push(&paths[start..]);
    }
    out
}

/// Whether a deleter is at work: it holds the trash's `.lock` (`lockf`,
/// which takes it with `flock`). Never true where the deleters do not
/// queue (off macOS).
fn deleter_running(dir: &Path) -> bool {
    cfg!(target_os = "macos")
        && matches!(
            crate::util::lock_path(&dir.join(".lock"), false, false),
            Ok(None)
        )
}

/// The deleter runs at the lowest CPU priority and, on macOS, behind the
/// deleters before it (`lockf` on `lock`), across processes. Several
/// worktrees removed together had their `rm -rf` side by side, each taking a
/// core, and as many again in fseventsd and the kernel. Only the CPU
/// priority is lowered: `taskpolicy -b` also throttled the disk, and on a
/// busy machine deleted a thousand files in two minutes.
fn spawn_rm_queued(paths: &[PathBuf], lock: Option<&Path>) {
    let mut cmd = Command::new("nice");
    cmd.args(["-n", "19"]);
    if let (true, Some(lock)) = (cfg!(target_os = "macos"), lock) {
        cmd.args(["lockf", "-k"]).arg(lock);
    }
    cmd.args(["rm", "-rf", "--"]).args(paths);
    match spawn_detached(cmd) {
        Ok(child) => crate::util::release(child),
        Err(_) => {
            let mut rm = Command::new("rm");
            rm.args(["-rf", "--"]).args(paths);
            if let Ok(child) = spawn_detached(rm) {
                crate::util::release(child);
            }
        }
    }
}

fn spawn_detached(mut cmd: Command) -> io::Result<std::process::Child> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
}

/// Delete again whatever an earlier, interrupted deleter left in the trash,
/// and the hidden siblings that the trash's notes point to: with one
/// deleter for all of it, and none while one is at work (what is stale
/// then is taken by the sweep of a later command), so that a slow deletion
/// is not started again by every command. Returns how many deleters were
/// started.
///
/// The trash directory and its `.lock` are never removed: every released
/// version queues its deleters on that path (`lockf -k`), and one that
/// waited on a file that was unlinked would delete side by side with the
/// next.
pub fn sweep() -> usize {
    let dir = paths::trash_dir();
    let stale = stale(&dir, now_secs());
    if stale.is_empty() || deleter_running(&dir) {
        return 0;
    }
    spawn_rm_many(&stale)
}

/// What [`sweep`] deletes, for the trash directory `dir` at time `now`. A
/// note whose sibling is gone is removed on the way, once the name says
/// stale (before that the sibling may be about to be put there).
fn stale(dir: &Path, now: u64) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !name.starts_with(NOTE_PREFIX) {
            if is_stale(&name, now) {
                out.push(e.path());
            }
            continue;
        }
        let Ok(bytes) = fs::read(e.path()) else {
            continue;
        };
        let target = PathBuf::from(OsStr::from_bytes(&bytes));
        // Only a sibling named by us, and only once its name says stale.
        let Some(theirs) = target
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix(BESIDE_PREFIX))
        else {
            continue;
        };
        if !is_stale(theirs, now) {
            continue;
        }
        if target.symlink_metadata().is_err() {
            let _ = fs::remove_file(e.path());
        } else {
            out.push(target);
        }
    }
    out
}

fn is_stale(name: &str, now: u64) -> bool {
    if let Some(rest) = name.strip_prefix(KEEP_PREFIX) {
        return match rest.split('-').next().and_then(|s| s.parse::<u64>().ok()) {
            Some(until) => until < now,
            None => false,
        };
    }
    match name.split('-').next().and_then(|s| s.parse::<u64>().ok()) {
        Some(t) => t + SWEEP_AFTER_SECS < now,
        // Not ours, or not named by us: leave it.
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_is_decided_by_the_name_timestamp() {
        assert!(is_stale("100-1-1-wt", 100 + SWEEP_AFTER_SECS + 1));
        assert!(!is_stale("100-1-1-wt", 100 + SWEEP_AFTER_SECS));
        assert!(!is_stale("readme", 1_000_000));
        assert!(!is_stale("keep-5000-100-1-1-wt", 4_999));
        assert!(!is_stale("keep-5000-100-1-1-wt", 5_000));
        assert!(is_stale("keep-5000-100-1-1-wt", 5_001));
        assert!(!is_stale("keep-x", 1_000_000));
    }

    #[test]
    fn a_sibling_is_noted_in_the_trash_and_swept_when_stale() {
        let t = tempfile::tempdir().unwrap();
        let base = t.path().canonicalize().unwrap();
        let dir = base.join("trash");
        fs::create_dir_all(&dir).unwrap();
        let wt = base.join("vol/wt");
        fs::create_dir_all(wt.join("sub")).unwrap();
        let d = beside(&wt, "100-1-1-wt", &dir).unwrap();
        assert_eq!(d, base.join("vol/.fast-worktree-trash-100-1-1-wt"));
        assert!(!wt.exists() && d.join("sub").is_dir());
        let note = dir.join("beside-100-1-1-wt");
        assert!(note.is_file());
        // Not stale yet: neither the sibling nor the note goes.
        assert!(stale(&dir, 100 + SWEEP_AFTER_SECS).is_empty());
        assert_eq!(stale(&dir, 100 + SWEEP_AFTER_SECS + 1), vec![d.clone()]);
        // Once the sibling is gone, so is the note; not before its name
        // says stale, when the sibling may be about to be put there.
        fs::remove_dir_all(&d).unwrap();
        assert!(stale(&dir, 100 + SWEEP_AFTER_SECS).is_empty());
        assert!(note.exists());
        assert!(stale(&dir, 100 + SWEEP_AFTER_SECS + 1).is_empty());
        assert!(!note.exists());
    }

    #[test]
    fn a_sibling_gets_its_note_even_when_the_trash_directory_is_gone() {
        let t = tempfile::tempdir().unwrap();
        let base = t.path().canonicalize().unwrap();
        // The trash directory is not there (removed by hand, say).
        let dir = base.join("cache/trash");
        let wt = base.join("vol/wt");
        fs::create_dir_all(&wt).unwrap();
        let d = beside(&wt, "keep-200-100-1-1-wt", &dir).unwrap();
        let note = dir.join("beside-keep-200-100-1-1-wt");
        assert_eq!(fs::read(&note).unwrap(), d.as_os_str().as_bytes());
        assert!(stale(&dir, 200).is_empty(), "kept until its time is up");
        assert_eq!(stale(&dir, 201), vec![d.clone()]);
        // A rename that fails leaves no note behind.
        assert!(beside(&base.join("vol/none"), "100-1-2-none", &dir).is_err());
        assert!(!dir.join("beside-100-1-2-none").exists());
    }

    #[test]
    fn a_sibling_without_a_note_is_found_by_its_name() {
        let t = tempfile::tempdir().unwrap();
        let base = t.path().canonicalize().unwrap();
        let vol = base.join("vol");
        // The trash directory cannot be made: its parent is a file.
        fs::write(base.join("cache"), "").unwrap();
        let dir = base.join("cache/trash");
        for n in ["a", "b"] {
            fs::create_dir_all(vol.join(n)).unwrap();
        }
        let due = beside(&vol.join("a"), "keep-200-100-1-1-a", &dir).unwrap();
        let not_due = beside(&vol.join("b"), "keep-900-100-1-2-b", &dir).unwrap();
        assert!(due.is_dir() && not_due.is_dir() && !dir.exists());
        // Names that are not made here are never taken.
        fs::create_dir_all(vol.join(".fast-worktree-trash-mine")).unwrap();
        fs::create_dir_all(vol.join("100-1-1-wt")).unwrap();
        let parents = [vol.clone(), base.join("missing")];
        assert!(stale_siblings(&parents, 200).is_empty(), "exactly due");
        assert_eq!(stale_siblings(&parents, 201), vec![due]);
    }

    #[test]
    fn many_paths_go_to_one_deleter() {
        let paths: Vec<PathBuf> = (0..500)
            .map(|i| PathBuf::from(format!("/cache/trash/1793000000-123-{i}-entry")))
            .collect();
        assert_eq!(chunks(&paths, ARGS_LIMIT).len(), 1);
        assert!(chunks(&[], ARGS_LIMIT).is_empty());
        // Past the limit they are split, in order, none lost.
        let split = chunks(&paths, 100);
        assert!(split.len() > 1);
        assert_eq!(split.concat(), paths);
        assert!(split.iter().all(|g| !g.is_empty()));
        let long = [PathBuf::from("x".repeat(300))];
        assert_eq!(chunks(&long, 100).len(), 1);
    }

    #[test]
    fn a_note_never_points_sweep_at_something_not_named_by_us() {
        let t = tempfile::tempdir().unwrap();
        let dir = t.path().join("trash");
        fs::create_dir_all(&dir).unwrap();
        let other = t.path().join("project");
        fs::create_dir_all(&other).unwrap();
        fs::write(dir.join("beside-100-1-1-x"), other.as_os_str().as_bytes()).unwrap();
        assert!(stale(&dir, u64::MAX / 2).is_empty());
        assert!(other.is_dir());
    }

    #[test]
    fn can_move_needs_a_writable_parent() {
        use std::os::unix::fs::PermissionsExt;
        let t = tempfile::tempdir().unwrap();
        let wt = t.path().join("p/wt");
        fs::create_dir_all(&wt).unwrap();
        assert_eq!(can_move(&wt), Ok(()));
        assert!(can_move(&t.path().join("p/missing")).is_err());
        let p = t.path().join("p");
        fs::set_permissions(&p, fs::Permissions::from_mode(0o555)).unwrap();
        // SAFETY: no arguments.
        let root = unsafe { libc::geteuid() } == 0;
        let r = can_move(&wt);
        fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).unwrap();
        if !root {
            assert!(r.unwrap_err().contains("not writable"));
        }
    }

    #[test]
    fn queued_deleters_all_finish() {
        let t = tempfile::tempdir().unwrap();
        let lock = t.path().join(".lock");
        let dirs: Vec<_> = (0..8).map(|i| t.path().join(format!("d{i}"))).collect();
        for d in &dirs {
            fs::create_dir_all(d.join("sub")).unwrap();
            fs::write(d.join("sub/f"), "x").unwrap();
            spawn_rm_queued(std::slice::from_ref(d), Some(&lock));
        }
        let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while dirs.iter().any(|d| d.exists()) && std::time::Instant::now() < until {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(dirs.iter().all(|d| !d.exists()));
        // `.lock` is not ours to sweep.
        assert!(!is_stale(".lock", u64::MAX / 2));
    }

    #[test]
    fn unique_names_differ() {
        let p = Path::new("/a/wt");
        let a = unique_name(p);
        let b = unique_name(p);
        assert_ne!(a, b);
        assert!(a.ends_with("-wt"));
    }
}
