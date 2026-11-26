//! `worker --ephemeral`: a worker that exists only while the user keeps it
//! running (a notebook cell, a Colab session).
//!
//! It registers no service and is never installed or upgraded by the parent.
//! It starts clean: a persistent worker's journal, or state at the canonical
//! worker directory that is not an ephemeral worker's, makes it refuse; its
//! own kind's leftover is cleared first (below). Each start has its own
//! identity (the boot identity carries a per-start instance), so a restarted
//! worker is never mistaken for the one before it. The parent reaches it
//! through `worker --stdio` over the tunnel the user prepared; no port is
//! opened here. On exit it removes the state directory it created, unless
//! work is still recorded in it. When told to end (SIGTERM, SIGINT, SIGHUP)
//! it first stops its live attempts like a cancel does and waits, bounded, for
//! their supervisors; only then is the state erased.
//!
//! A start that was killed leaves its state behind. The next start clears it
//! by itself when it can prove that nothing uses it any more (spec 25.4 (d)):
//! the state carries this role's marker (`ephemeral.json`, which exists only
//! once its writer holds `ephemeral.lock`), both the worker that created it
//! and this start see it on a local filesystem, no persistent worker is
//! installed (neither the worker service's journal nor the `init`
//! resident's), and both `agent.lock` and `ephemeral.lock` can be taken.
//! `ephemeral.lock` is held shared by the worker and, through an inherited
//! descriptor, by every process it starts, so it is free only when all of
//! them are gone. Without the proof nothing is removed and no process is
//! signalled. What is removed is reached only through the descriptor of the
//! directory that was examined, never again through its path, and never
//! across a filesystem mounted inside it.
//!
//! A start killed before its marker existed leaves a directory that holds
//! at most `ephemeral.lock` and `ephemeral.json.part`. The next start removes
//! exactly those names, under the lock, and nothing else (`unfinished`).
//! The exit of a worker that ran removes its state the same way a clearing
//! does, under the same locks (`clean_up`).
use super::WorkerSpec;
use crate::service::resident;
use crate::service::worker_service::{live_attempts, Paths};
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use std::ffi::{CStr, CString};
use std::fs::{File, Metadata, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) mod plan;

/// The owner marker. A complete one implies that its writer held
/// `ephemeral.lock` from before the marker existed.
pub(super) const MARKER: &str = "ephemeral.json";
/// The marker while it is written; renamed to `MARKER` when complete.
const MARKER_PART: &str = "ephemeral.json.part";
/// Held shared by the worker and by every process it starts.
const LOCK: &str = "ephemeral.lock";
const AGENT_LOCK: &str = "agent.lock";
/// Held by a worker for as long as it runs on the state.
const OWNER_LOCK: &str = "worker-owner.lock";
const MARKER_LIMIT: u64 = 64 * 1024;

/// `ephemeral.json`. `kind` and `filesystem` decide; the rest says which
/// start left the state, for the person reading a refusal.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct MarkerFile {
    schema: u32,
    kind: String,
    /// The OS boot (`platform::boot_id`) the worker started in.
    #[serde(default)]
    boot: Option<String>,
    #[serde(default)]
    pid: Option<u32>,
    /// The process start identity of `pid`.
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    instance: Option<String>,
    /// The filesystem the writer itself saw the state on, and whether it
    /// counted it as local. Written by `create_state`; a marker without it
    /// proves nothing.
    #[serde(default)]
    filesystem: Option<plan::Recorded>,
}

/// Where the code stands, for a test that needs something to happen exactly
/// there. Nothing else listens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// `create_state`: the directory exists and is empty.
    Directory,
    /// `create_state`: `ephemeral.lock` was created and is not locked yet.
    Made,
    /// `create_state`: `ephemeral.lock` exists and is held; no marker yet.
    Locked,
    /// A clearing: what can be read was read and the directory is locked;
    /// no lock file was opened yet.
    Examined,
    /// A clearing: the lock files are held; what they prove was not
    /// checked against the directory yet.
    Held,
    /// A clearing: every proof holds; nothing was removed yet.
    Proven,
    /// A clearing: this one of the names that go last is removed next.
    Last(&'static str),
}

fn nowhere(_: Step) {}

/// What a clearing asks of the host, so that a test can answer instead.
struct Host<'a> {
    /// Proof 3, asked when it is needed and not once at the start: whether
    /// a persistent worker's journal exists now.
    resident: &'a dyn Fn() -> anyhow::Result<bool>,
    filesystem_of: &'a dyn Fn(&File) -> plan::Filesystem,
    /// The device an entry is on, from its name and the `st_dev` read.
    device_of: &'a dyn Fn(&CStr, u64) -> u64,
    at: &'a dyn Fn(Step),
}

/// The device as `stat` reported it. A test answers otherwise, to stand in
/// for a filesystem mounted inside the state.
fn same_device(_: &CStr, device: u64) -> u64 {
    device
}

fn same_file(a: &Metadata, b: &Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

/// Device and inode of what `name` is in the directory, without following a
/// link.
fn identity_at(dir: &File, name: &CStr) -> std::io::Result<(u64, u64)> {
    let stat = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)?;
    // The fields' types differ between the platforms.
    #[allow(clippy::unnecessary_cast)]
    Ok((stat.st_dev as u64, stat.st_ino as u64))
}

fn is_named(dir: &File, name: &CStr, as_file: &Metadata) -> bool {
    identity_at(dir, name).is_ok_and(|id| id == (as_file.dev(), as_file.ino()))
}

fn c_name(name: &str) -> CString {
    CString::new(name).expect("a fixed name without a NUL")
}

/// Open `name` inside the opened directory, never through a link, and only
/// if it is a regular file of this user with one name that nobody else can
/// touch. With `OFlags::CREATE` it is made private.
fn private_at(dir: &File, name: &str, flags: OFlags) -> std::io::Result<File> {
    let file = File::from(rustix::fs::openat(
        dir,
        name,
        flags | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o077 != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "not a private file of this user",
        ));
    }
    Ok(file)
}

/// What the marker of a state left behind says.
struct Examined {
    kind: plan::Marker,
    /// Its content, when it parsed.
    file: Option<MarkerFile>,
    /// Which file it was and what it held, when it could be read: device,
    /// inode, bytes.
    read: Option<(u64, u64, Vec<u8>)>,
}

fn read_marker(dir: &File) -> Examined {
    let unreadable = || Examined {
        kind: plan::Marker::Unreadable,
        file: None,
        read: None,
    };
    match identity_at(dir, &c_name(MARKER)) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Examined {
                kind: plan::Marker::Absent,
                file: None,
                read: None,
            }
        }
        Err(_) => return unreadable(),
    }
    let mut bytes = Vec::new();
    let Ok(file) = private_at(dir, MARKER, OFlags::RDONLY) else {
        return unreadable();
    };
    let Ok(meta) = file.metadata() else {
        return unreadable();
    };
    let read = file.take(MARKER_LIMIT + 1).read_to_end(&mut bytes);
    if read.is_err() || bytes.len() as u64 > MARKER_LIMIT {
        return unreadable();
    }
    let (kind, file) = match serde_json::from_slice::<MarkerFile>(&bytes) {
        Ok(marker) if marker.kind == plan::MARKER_KIND => (plan::Marker::Ephemeral, Some(marker)),
        Ok(marker) => (plan::Marker::OtherKind, Some(marker)),
        Err(_) => return unreadable(),
    };
    Examined {
        kind,
        file,
        read: Some((meta.dev(), meta.ino(), bytes)),
    }
}

/// The filesystem the opened directory is on.
fn filesystem_of(dir: &File) -> plan::Filesystem {
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: a valid descriptor and a buffer of the size fstatfs writes.
    if unsafe { libc::fstatfs(dir.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return plan::Filesystem::Unknown;
    }
    // SAFETY: fstatfs succeeded and filled the buffer.
    let stat = unsafe { stat.assume_init() };
    #[cfg(target_os = "linux")]
    {
        // The magic is 32 bits wide; the field's type differs between libcs.
        #[allow(clippy::unnecessary_cast)]
        let magic = (stat.f_type as u64) & 0xFFFF_FFFF;
        plan::Filesystem::Magic(magic)
    }
    #[cfg(target_os = "macos")]
    {
        let name: Vec<u8> = stat
            .f_fstypename
            .iter()
            .take_while(|c| **c != 0)
            .map(|c| *c as u8)
            .collect();
        match String::from_utf8(name) {
            Ok(name) => plan::Filesystem::Name(name),
            Err(_) => plan::Filesystem::Unknown,
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = stat;
        plan::Filesystem::Unknown
    }
}

/// Whether the lock was taken exclusively, without waiting.
fn taken(lock: &File) -> std::io::Result<Option<crate::platform::flock::HeldBorrow<'_>>> {
    match crate::platform::flock::HeldBorrow::try_lock(lock) {
        Ok(held) => Ok(Some(held)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

/// The names in the opened directory. The listing is finished and closed
/// before anything is done with a name, so a walk holds one descriptor for
/// each level and no more.
fn names_in(dir: &impl AsFd) -> std::io::Result<Vec<CString>> {
    let mut names = Vec::new();
    for entry in rustix::fs::Dir::read_from(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name != c"." && name != c".." {
            names.push(name.to_owned());
        }
    }
    Ok(names)
}

/// A removal that stays on the filesystem of the state directory. A child
/// on another device is a filesystem mounted inside the state (a FUSE or
/// bind mount that is still alive in a workspace): it is neither entered
/// nor unlinked, and the directories above it stay. Every step goes from a
/// descriptor to a name inside it and never follows a link, so nothing
/// outside the opened directory can be reached, whatever is renamed
/// meanwhile.
///
/// Limit: a bind mount of a directory of the same filesystem has the same
/// device and is not told apart here.
struct Walk<'a> {
    /// The device of the state directory.
    device: u64,
    device_of: &'a dyn Fn(&CStr, u64) -> u64,
    /// What was kept because it is on another device, by its path inside
    /// the state.
    mounted: Vec<PathBuf>,
}

impl Walk<'_> {
    fn elsewhere(&mut self, name: &CStr, stat: &rustix::fs::Stat, here: &Path) -> bool {
        // The field's type differs between the platforms.
        #[allow(clippy::unnecessary_cast)]
        let device = (self.device_of)(name, stat.st_dev as u64);
        if device == self.device {
            return false;
        }
        self.mounted.push(here.to_owned());
        true
    }

    /// Remove `name` from the opened directory, with everything below it
    /// when it is a directory. Whether the name is gone.
    fn remove(&mut self, dir: &impl AsFd, name: &CStr, inside: &Path) -> std::io::Result<bool> {
        let here = inside.join(std::ffi::OsStr::from_bytes(name.to_bytes()));
        let stat = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)?;
        if self.elsewhere(name, &stat, &here) {
            return Ok(false);
        }
        // The field's type differs between the platforms.
        #[allow(clippy::unnecessary_cast)]
        let kind = FileType::from_raw_mode(stat.st_mode as rustix::fs::RawMode);
        if kind != FileType::Directory {
            rustix::fs::unlinkat(dir, name, AtFlags::empty())?;
            return Ok(true);
        }
        let below = rustix::fs::openat(
            dir,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        // What was opened, not what was looked at: a filesystem mounted on
        // the name in between is the one this descriptor is on.
        if self.elsewhere(name, &rustix::fs::fstat(&below)?, &here) {
            return Ok(false);
        }
        let mut emptied = true;
        for child in names_in(&below)? {
            emptied &= self.remove(&below, &child, &here)?;
        }
        drop(below);
        if !emptied {
            return Ok(false);
        }
        rustix::fs::unlinkat(dir, name, AtFlags::REMOVEDIR)?;
        Ok(true)
    }
}

fn directory(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
}

/// The directory that holds the state directory, opened, and the name the
/// state has in it.
fn place(state: &Path) -> std::io::Result<(File, CString)> {
    let (Some(parent), Some(name)) = (state.parent(), state.file_name()) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "it has no parent directory",
        ));
    };
    let name = CString::new(name.as_bytes())?;
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(parent)?;
    Ok((parent, name))
}

/// Unlink these names of the opened directory, in this order. One that is
/// not there is fine.
fn drop_names(dir: &File, names: &[&'static str], at: &dyn Fn(Step)) -> std::io::Result<()> {
    for name in names {
        at(Step::Last(name));
        match rustix::fs::unlinkat(dir, *name, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Remove the name of the directory that was examined and emptied. The name
/// goes only if it still is that directory; whatever sits there instead is
/// not this proof's to remove. Not recursive either: what appeared in the
/// directory meanwhile stays, and so does the directory.
fn remove_name(parent: &File, name: &CStr, seen: &Metadata, state: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        is_named(parent, name, seen),
        "worker state {} was replaced while it was cleared; what is at the path now was left in place",
        state.display()
    );
    rustix::fs::unlinkat(parent, name, AtFlags::REMOVEDIR).map_err(|e| {
        anyhow::anyhow!(
            "worker state {} could not be removed after it was cleared ({}); what is in it now was left in place",
            state.display(),
            std::io::Error::from(e)
        )
    })
}

/// Empty the directory that was proven unused and remove its name. The
/// data goes first, then the lock files, the marker last: a clearing that
/// was itself killed leaves a state the next start can still prove and
/// clear, and no name but `ephemeral.lock` outlives the marker.
///
/// Where a filesystem is mounted inside the state, what is on it and the
/// directories above it stay, and so do the locks and the marker: the state
/// remains one that the next start can prove again once the mount is gone.
fn erase(
    state: &Path,
    dir: &File,
    parent: &File,
    name: &CStr,
    seen: &Metadata,
    host: &Host<'_>,
) -> anyhow::Result<()> {
    let last = [OWNER_LOCK, AGENT_LOCK, LOCK, MARKER];
    let mut walk = Walk {
        device: seen.dev(),
        device_of: host.device_of,
        mounted: Vec::new(),
    };
    for entry in names_in(dir)? {
        if last.iter().any(|name| entry.as_bytes() == name.as_bytes()) {
            continue;
        }
        walk.remove(dir, &entry, Path::new(""))?;
    }
    if !walk.mounted.is_empty() {
        let mounted: Vec<String> = walk
            .mounted
            .iter()
            .map(|path| path.display().to_string())
            .collect();
        anyhow::bail!(
            "worker state {} was not cleared whole: another filesystem is mounted in it at {}. Nothing on a mounted filesystem is removed; the directories above it, the lock files and ephemeral.json were kept. Unmount it; the next `belt worker --ephemeral` then clears the rest",
            state.display(),
            mounted.join(", ")
        );
    }
    drop_names(dir, &last, host.at)?;
    remove_name(parent, name, seen, state)
}

/// What a clearing found and removed.
#[derive(Debug, PartialEq, Eq)]
enum Cleared {
    /// The state of an ephemeral worker that nothing uses any more.
    Left,
    /// What a start that was killed before its marker existed had made.
    Unfinished,
}

/// Clear the state an earlier ephemeral start left at `state`, when all five
/// proofs hold; otherwise fail naming the one that did not, with nothing
/// removed and no process signalled.
///
/// Proofs 1 to 3 are read first: a state with a marker that is not this
/// role's is never written to and never locked. (One without any marker is
/// `unfinished`'s.) Then, in this order:
///
/// - The directory itself is locked, so that one start clears at a time (two
///   that each made and took a lock file of their own would otherwise both
///   believe they hold `ephemeral.lock`).
/// - The lock files are opened inside the examined directory. A complete
///   marker implies its writer made `ephemeral.lock` before it, so a marker
///   without that file is what a clearing that was itself killed leaves; the
///   file is then made here, and nobody can hold a file that did not exist.
/// - Under the exclusive lock the marker is read again and must be the very
///   file that was examined, with the same bytes. Only then does "there is a
///   marker" count as proof 1: the marker of a start that is alive cannot be
///   read here without its lock being held.
///
/// Every lock stays held until the directory is gone.
fn clear(state: &Path, host: &Host<'_>) -> anyhow::Result<Cleared> {
    let kept = |why: String| {
        anyhow::anyhow!(
            "worker state {} was left by an earlier start and is kept: {why}",
            state.display()
        )
    };
    resident::check_private_dir(state)
        .map_err(|e| kept(format!("it is not a private directory of this user ({e})")))?;
    let dir = directory(state).map_err(|e| kept(format!("it cannot be opened ({e})")))?;
    let seen = dir.metadata()?;
    let (parent, name) =
        place(state).map_err(|e| kept(format!("its parent directory cannot be opened ({e})")))?;
    let examined = read_marker(&dir);
    if examined.kind == plan::Marker::Absent {
        unfinished(state, &dir, &seen, &parent, &name, host, &kept)?;
        return Ok(Cleared::Unfinished);
    }
    let by = examined.file.as_ref().map_or_else(String::new, |m| {
        format!(
            " (left by pid {} of boot {}, instance {})",
            m.pid.map_or_else(|| "?".into(), |p| p.to_string()),
            m.boot.as_deref().unwrap_or("?"),
            m.instance.as_deref().unwrap_or("?")
        )
    });
    plan::examined(
        &examined.kind,
        examined.file.as_ref().and_then(|m| m.filesystem.as_ref()),
        &(host.filesystem_of)(&dir),
        (host.resident)()?,
    )
    .map_err(&kept)?;
    let Some(_dir_held) = taken(&dir)? else {
        return Err(kept(format!(
            "another ephemeral start is clearing it right now{by}"
        )));
    };
    (host.at)(Step::Examined);
    let open = |name: &str| {
        private_at(&dir, name, OFlags::RDWR | OFlags::CREATE)
            .map_err(|e| kept(format!("its {name} cannot be examined ({e}){by}")))
    };
    let ephemeral_lock = open(LOCK)?;
    let ephemeral_held = taken(&ephemeral_lock)?;
    let ephemeral_taken = ephemeral_held.is_some();
    // agent.lock is only touched once no process of the earlier start is
    // left, so a worker that is starting right now is not disturbed.
    let agent_lock = if ephemeral_taken {
        Some(open(AGENT_LOCK)?)
    } else {
        None
    };
    let agent_held = match &agent_lock {
        Some(lock) => taken(lock)?,
        None => None,
    };
    let agent_taken = agent_held.is_some();
    plan::unused(ephemeral_taken, agent_taken).map_err(|why| kept(format!("{why}{by}")))?;
    (host.at)(Step::Held);
    if examined.read.is_none() || read_marker(&dir).read != examined.read {
        return Err(kept(format!(
            "its ephemeral.json changed while it was examined{by}"
        )));
    }
    // A lock on a file that another start has already removed proves nothing
    // about the state now at this path: it must still be the one examined,
    // and the lock held must be the one in it.
    if !(is_named(&parent, &name, &seen) && holds(&dir, &ephemeral_lock)) {
        return Err(kept("it was replaced while it was examined".into()));
    }
    (host.at)(Step::Proven);
    erase(state, &dir, &parent, &name, &seen, host)?;
    Ok(Cleared::Left)
}

/// Whether the lock file that is held is the one `ephemeral.lock` names in
/// the directory now. A hold on a file that was unlinked, or replaced under
/// its name, says nothing about who holds the one that is there.
fn holds(dir: &File, lock: &File) -> bool {
    lock.metadata()
        .is_ok_and(|held| is_named(dir, &c_name(LOCK), &held))
}

/// Remove what a start that was killed before its marker existed left: a
/// directory with nothing in it but `ephemeral.lock` and
/// `ephemeral.json.part` (spec 25.4 (d)). Also what a creation that failed
/// and was killed while it took back its files leaves, and what a clearing
/// leaves that was killed between its last unlink and its `rmdir`.
///
/// Nothing is written or locked before the listing, the filesystem and the
/// journals allow it. Then the directory is locked (one start at a time),
/// `ephemeral.lock` is opened, or made when it is not there, and taken
/// exclusively: a start that is creating this state holds it shared from
/// before its marker, or finds it existing or held and steps back
/// (`create_state`). With the lock held everything is read again. Only the
/// two names are unlinked; the directory goes by a non-recursive `rmdir`,
/// so whatever else appears in it stays. A refusal takes back the lock file
/// if this call made it and holds it, and nothing else.
///
/// An empty state directory can also be one that `init` or a worker's first
/// lock just made. `init` saves its journal before it makes the directory,
/// so the journals, read here after the directory was opened, exclude it.
/// For any other maker the next file it creates either is seen by the
/// second listing (kept), lands between that listing and the `rmdir` (which
/// then fails, kept), or is attempted after the `rmdir` and fails cleanly
/// with "not found", to be run again.
fn unfinished(
    state: &Path,
    dir: &File,
    seen: &Metadata,
    parent: &File,
    name: &CStr,
    host: &Host<'_>,
    kept: &dyn Fn(String) -> anyhow::Error,
) -> anyhow::Result<()> {
    let judge = || -> anyhow::Result<Result<(), String>> {
        let names = names_in(dir)?;
        let names: Vec<&[u8]> = names.iter().map(|name| name.to_bytes()).collect();
        Ok(plan::unfinished(
            &names,
            &(host.filesystem_of)(dir),
            (host.resident)()?,
        ))
    };
    judge()?.map_err(kept)?;
    let Some(_dir_held) = taken(dir)? else {
        return Err(kept(
            "another ephemeral start is clearing it right now".into(),
        ));
    };
    (host.at)(Step::Examined);
    let examine = |e: std::io::Error| kept(format!("its {LOCK} cannot be examined ({e})"));
    let (lock, made) = match private_at(dir, LOCK, OFlags::RDWR | OFlags::CREATE | OFlags::EXCL) {
        Ok(lock) => (lock, true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            (private_at(dir, LOCK, OFlags::RDWR).map_err(examine)?, false)
        }
        Err(e) => return Err(examine(e)),
    };
    let lock_held = taken(&lock)?;
    let held = lock_held.is_some();
    let proven = (|| {
        if !held {
            return Err(kept(
                "its ephemeral.lock is held: an ephemeral worker is creating it right now".into(),
            ));
        }
        (host.at)(Step::Held);
        judge()?.map_err(|why| kept(format!("it changed while it was examined: {why}")))?;
        if !(is_named(parent, name, seen) && holds(dir, &lock)) {
            return Err(kept("it was replaced while it was examined".into()));
        }
        Ok(())
    })();
    if let Err(refusal) = proven {
        if made && held && holds(dir, &lock) {
            let _ = rustix::fs::unlinkat(dir, LOCK, AtFlags::empty());
        }
        return Err(refusal);
    }
    (host.at)(Step::Proven);
    drop_names(dir, &[MARKER_PART, LOCK], host.at)?;
    remove_name(parent, name, seen, state)
}

/// The state directory this process created, and how to know it is still that one.
struct Created {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

/// What a creation has made so far.
#[derive(Default)]
struct Made {
    /// The names this call itself created in the directory.
    names: Vec<&'static str>,
    /// `ephemeral.lock`, held shared from the moment it is.
    lock: Option<File>,
    /// Another start is removing what it takes for a killed start's
    /// leftover: nothing here is this call's to touch any more.
    stepped_back: bool,
}

/// Create the state: the directory, then `ephemeral.lock` (a new file, never
/// one that was there) held shared, and only then the marker, complete in one
/// step (written beside and renamed). So a marker that can be read implies
/// that its writer has held the lock since before the marker existed, and a
/// start that clears can never meet a marked state whose owner is still on
/// its way to the lock. The marker records the filesystem this process sees
/// the directory on.
///
/// The returned descriptor holds `ephemeral.lock` shared. It is still
/// close-on-exec: [`create_and_hold`] lets every process started from here
/// inherit it.
///
/// Until the lock is held, another start may take the directory for what a
/// killed start left and remove it (`unfinished`). It makes or takes
/// `ephemeral.lock` first, so this call meets it in one of two ways: the
/// lock file already exists, or the one made here cannot be locked. Either
/// way this call steps back and touches nothing: the other start removes
/// the directory, and unlinking a name without holding its lock would not
/// provably unlink this call's own file.
///
/// Any other failure removes the names this call created, and only those,
/// while the lock is still held. A kill before the marker leaves at most
/// `ephemeral.lock` and `ephemeral.json.part`, which the next start removes.
fn create_state(
    path: &Path,
    marker: &MarkerFile,
    filesystem_of: impl Fn(&File) -> plan::Filesystem,
    at: &dyn Fn(Step),
) -> anyhow::Result<(Created, File)> {
    create_state_with(path, marker, filesystem_of, at, &File::try_lock_shared)
}

/// `create_state` with the shared lock taken by `lock_shared`, so that a
/// test can make it fail.
fn create_state_with(
    path: &Path,
    marker: &MarkerFile,
    filesystem_of: impl Fn(&File) -> plan::Filesystem,
    at: &dyn Fn(Step),
    lock_shared: &dyn Fn(&File) -> Result<(), TryLockError>,
) -> anyhow::Result<(Created, File)> {
    std::fs::DirBuilder::new().mode(0o700).create(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(meta.is_dir(), "worker state is not a directory");
    let dir = directory(path);
    let mut made = Made::default();
    let filled = (|| -> anyhow::Result<()> {
        let dir = dir.as_ref().map_err(|e| anyhow::anyhow!("{e}"))?;
        // Everything below is created inside the directory just made, not
        // inside whatever a concurrent start has put at its path.
        anyhow::ensure!(
            same_file(&dir.metadata()?, &meta),
            "worker state was replaced while it was created"
        );
        at(Step::Directory);
        let stepping_back = || {
            anyhow::anyhow!(
                "another ephemeral start is removing the directory at {} right now; start again",
                path.display()
            )
        };
        // Another party may have removed the fresh directory and made its own
        // at the same path before this call looked at it: only an empty
        // directory is this call's to fill.
        if !names_in(dir)?.is_empty() {
            made.stepped_back = true;
            return Err(stepping_back());
        }
        let lock = match private_at(dir, LOCK, OFlags::RDWR | OFlags::CREATE | OFlags::EXCL) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                made.stepped_back = true;
                return Err(stepping_back());
            }
            Err(e) => return Err(e.into()),
        };
        at(Step::Made);
        match lock_shared(&lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                made.stepped_back = true;
                return Err(stepping_back());
            }
            // Not held: unlinking the name would not provably unlink this
            // call's own file, so it stays for the next start to remove.
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        made.names.push(LOCK);
        made.lock = Some(lock);
        at(Step::Locked);
        let mut marker = marker.clone();
        marker.filesystem = Some(plan::recorded(&filesystem_of(dir)));
        let mut part = private_at(
            dir,
            MARKER_PART,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
        )?;
        made.names.push(MARKER_PART);
        part.write_all(&serde_json::to_vec(&marker)?)?;
        part.sync_all()?;
        rustix::fs::renameat(dir, MARKER_PART, dir, MARKER).map_err(std::io::Error::from)?;
        made.names.push(MARKER);
        dir.sync_all()?;
        anyhow::ensure!(
            std::fs::symlink_metadata(path).is_ok_and(|now| same_file(&now, &meta)),
            "worker state was replaced while it was created"
        );
        Ok(())
    })();
    match (filled, made.lock.take()) {
        (Ok(()), Some(alive)) => Ok((
            Created {
                path: path.to_owned(),
                dev: meta.dev(),
                ino: meta.ino(),
            },
            alive,
        )),
        (filled, held) => {
            // What this call made goes, the marker first, while its lock is
            // still held; anything else in the directory is left for the
            // next start to judge.
            if !made.stepped_back {
                if let Ok(dir) = &dir {
                    for name in [MARKER, MARKER_PART, LOCK] {
                        if made.names.contains(&name) {
                            let _ = rustix::fs::unlinkat(dir, name, AtFlags::empty());
                        }
                    }
                }
                if let Ok((parent, name)) = place(path) {
                    if is_named(&parent, &name, &meta) {
                        let _ = rustix::fs::unlinkat(&parent, &name, AtFlags::REMOVEDIR);
                    }
                }
            }
            drop(held);
            Err(filled
                .err()
                .unwrap_or_else(|| anyhow::anyhow!("worker state was created without its lock")))
        }
    }
}

/// [`create_state`], whose lock every process this one starts from now on
/// inherits (it is no longer close-on-exec), so the lock stays held until
/// the last of them is gone (spec 25.4 (d)). Call before any other thread
/// exists: a child another thread forks meanwhile would inherit it too (in
/// a test process, one that has nothing to do with the worker).
fn create_and_hold(
    path: &Path,
    marker: &MarkerFile,
    filesystem_of: impl Fn(&File) -> plan::Filesystem,
    at: &dyn Fn(Step),
) -> anyhow::Result<(Created, File)> {
    let (created, alive) = create_state(path, marker, filesystem_of, at)?;
    inherit(&alive)?;
    Ok((created, alive))
}

/// Let every process started from here inherit the descriptor: the
/// supervisors, the setup builders and, through them, the users' commands.
/// A `flock` belongs to the open file, so it is released only when the last
/// process holding a copy has closed it or ended.
fn inherit(lock: &File) -> std::io::Result<()> {
    let fd = lock.as_raw_fd();
    // SAFETY: plain fcntl calls on a descriptor this function borrows.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Proof 3: whether a persistent worker is installed on this host, by its
/// journals (the worker service's and the combined `init` resident's, which
/// runs a worker on the same state). Only "not found" means there is none:
/// a journal that cannot be looked at may be there, so the start refuses and
/// names the error.
fn resident_installed(journals: &[PathBuf]) -> anyhow::Result<bool> {
    let mut found = false;
    for journal in journals {
        match std::fs::symlink_metadata(journal) {
            Ok(_) => found = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => anyhow::bail!(
                "cannot tell whether a persistent worker is installed on this host: {} cannot be examined ({e})",
                journal.display()
            ),
        }
    }
    Ok(found)
}

/// Remove what was created, if nothing runs in it and it is still the same
/// directory (never a replacement that happens to sit at the path). `false`:
/// work is still recorded in it.
///
/// The removal is the one a clearing does, under the same locks. The
/// directory is locked, so no other start examines or clears it meanwhile
/// (it could otherwise read the marker after `ephemeral.lock` was unlinked,
/// make a lock of its own and clear at the same time). `worker-owner.lock`
/// and `agent.lock` are taken, so no worker runs on it. Only then are the
/// journals read again: a persistent worker installed while this one ran may
/// have put its own files into the directory, and then everything stays.
/// This process's own shared hold on `ephemeral.lock` is not turned into
/// an exclusive one: the directory lock already keeps a clearing out.
fn clean_up(created: &Created, alive: &File, host: &Host<'_>) -> anyhow::Result<bool> {
    if !plan::removable(live_attempts(&created.path).ok()) {
        return Ok(false);
    }
    let dir = directory(&created.path)?;
    let seen = dir.metadata()?;
    anyhow::ensure!(
        seen.dev() == created.dev && seen.ino() == created.ino,
        "worker state was replaced; left in place"
    );
    let (parent, name) = place(&created.path)?;
    let dir_held = taken(&dir)?;
    anyhow::ensure!(
        dir_held.is_some(),
        "another ephemeral start is examining the worker state right now; it was left in place, and the next `belt worker --ephemeral` clears it"
    );
    (host.at)(Step::Examined);
    let mut held = Vec::new();
    for name in [OWNER_LOCK, AGENT_LOCK] {
        let lock = private_at(&dir, name, OFlags::RDWR | OFlags::CREATE)?;
        let lock = match crate::platform::flock::HeldLock::try_lock(lock) {
            Ok(lock) => Some(lock),
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Error(e)) => return Err(e.into()),
        };
        anyhow::ensure!(
            lock.is_some(),
            "its {name} is held: a worker still uses this state; it was left in place"
        );
        held.push(lock);
    }
    (host.at)(Step::Held);
    anyhow::ensure!(
        !(host.resident)()?,
        "a persistent worker was installed on this host while this worker ran; everything in {} was left in place",
        created.path.display()
    );
    anyhow::ensure!(
        is_named(&parent, &name, &seen) && holds(&dir, alive),
        "worker state was replaced; left in place"
    );
    (host.at)(Step::Proven);
    erase(&created.path, &dir, &parent, &name, &seen, host)?;
    Ok(true)
}

pub struct Args {
    pub cluster: String,
    pub name: String,
    pub key_file: PathBuf,
}

fn start(args: &Args) -> anyhow::Result<()> {
    let env: std::collections::BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let executable = std::env::current_exe()?;
    let os_boot = crate::platform::boot_id()
        .ok_or_else(|| anyhow::anyhow!("OS boot identity unavailable"))?;
    let instance = ulid::Ulid::new().to_string();
    // Only the existence of a resident's journal matters; reading it is not
    // this role's business.
    let journals = Paths::resident_journals(&env)?;
    let resident = resident_installed(&journals)?;
    // Asked again wherever something is about to be removed.
    let host = Host {
        resident: &|| resident_installed(&journals),
        filesystem_of: &filesystem_of,
        device_of: &same_device,
        at: &nowhere,
    };
    let planned = plan::plan(
        &plan::Request {
            cluster: &args.cluster,
            name: &args.name,
            key_file: &args.key_file,
        },
        &plan::Host {
            env: &env,
            executable: &executable,
            os_boot: &os_boot,
            instance: &instance,
            resident_journal: resident,
        },
    )
    .map_err(anyhow::Error::msg)?;
    let spec: WorkerSpec = planned.spec;
    let home = std::path::PathBuf::from(&spec.env["HOME"]);
    match std::fs::symlink_metadata(&spec.state_dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        // Anything at the path, readable or not, is a state left behind.
        _ => {
            let what = match clear(&spec.state_dir, &host)? {
                Cleared::Left => "cleared the worker state an earlier ephemeral start left",
                Cleared::Unfinished => {
                    "removed what an ephemeral start that was killed while creating its state left"
                }
            };
            eprintln!("{what} at {}", spec.state_dir.display());
        }
    }
    let me = std::process::id();
    let marker = MarkerFile {
        schema: 1,
        kind: plan::MARKER_KIND.into(),
        boot: Some(os_boot.clone()),
        pid: Some(me),
        start: crate::platform::process_start_identity(me)
            .identity()
            .map(str::to_owned),
        instance: Some(instance.clone()),
        // Recorded by `create_state`, from the directory it makes.
        filesystem: None,
    };
    // Held until this process ends; its children hold their own copies.
    let (created, alive) = create_and_hold(&spec.state_dir, &marker, filesystem_of, &nowhere)?;
    let result = (|| {
        let config = spec.validate(&home)?;
        eprintln!(
            "ephemeral worker {} started as instance {instance}",
            spec.worker_id
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let done = runtime.block_on(super::run(spec, config, Some(planned.boot_id)));
        // A blocking pass that never returns must not hold the exit.
        runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        done
    })();
    // Exit 0 only for a complete erase (or an end that had nothing to erase
    // under a failure already being reported).
    let erased = match clean_up(&created, &alive, &host) {
        Ok(true) => Ok(()),
        Ok(false) => {
            let alive = crate::storage::WorkerStore::open_read_only(&created.path)
                .and_then(|s| s.list_nonterminal())
                .map(|l| {
                    l.iter()
                        .map(|r| r.attempt_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            eprintln!(
                "work is still recorded in the worker state; it was kept (attempts still alive: {alive}). \
                 Its cancel marker is set; once those attempts are gone, the next `belt worker --ephemeral` clears {} and starts",
                created.path.display()
            );
            Err(anyhow::anyhow!("worker state was kept"))
        }
        Err(e) => {
            eprintln!("worker state was not removed: {e}");
            Err(e)
        }
    };
    result.and(erased)
}

/// A resident worker never takes over an ephemeral worker's state, its
/// identity or its attempts (spec 25.4 (d)). Anything but a clear "no marker"
/// counts as marked, and so does what a creation that was killed leaves.
pub(super) fn marked(state: &Path) -> bool {
    // An unfinished creation counts: a resident never makes these names, and
    // writing into it would leave a state no ephemeral start may clear.
    std::iter::once(MARKER)
        .chain(plan::UNFINISHED_NAMES)
        .any(|name| {
            !matches!(
                std::fs::symlink_metadata(state.join(name)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound
            )
        })
}

/// The refusal of everything that would run, install or start a persistent
/// worker on a state an ephemeral worker made. Called before the first
/// write into the state, so a refusal leaves it as it was.
pub(crate) fn refuse_marked(state: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        !marked(state),
        "worker state {} belongs to an ephemeral worker (it has ephemeral.json); a persistent worker never takes it over. \
         Start `belt worker --ephemeral` once to clear it, or remove the directory",
        state.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests;

/// Foreground until stopped (SIGTERM or interrupt). Never installs anything.
pub fn main(args: &Args) -> i32 {
    match start(args) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: ephemeral worker failed: {e}");
            1
        }
    }
}
