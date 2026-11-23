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
//! directory that was examined, never again through its path.
use super::WorkerSpec;
use crate::service::resident;
use crate::service::worker_service::{live_attempts, Paths};
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use std::ffi::{CStr, CString};
use std::fs::{File, Metadata, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd};
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
    /// `create_state`: `ephemeral.lock` exists and is held; no marker yet.
    Locked,
    /// `reclaim`: proofs 1 to 3 were read; no lock file was opened yet.
    Examined,
    /// `reclaim`: every proof holds; nothing was removed yet.
    Proven,
}

fn nowhere(_: Step) {}

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
fn taken(lock: &File) -> std::io::Result<bool> {
    match lock.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
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

/// Remove `name` from the opened directory, with everything below it when
/// it is a directory. Every step goes from a descriptor to a name inside it
/// and never follows a link, so nothing outside the opened directory can be
/// reached, whatever is renamed meanwhile.
fn remove_at(dir: &impl AsFd, name: &CStr) -> std::io::Result<()> {
    let stat = rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        return Ok(rustix::fs::unlinkat(dir, name, AtFlags::empty())?);
    }
    let below = rustix::fs::openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    for child in names_in(&below)? {
        remove_at(&below, &child)?;
    }
    drop(below);
    Ok(rustix::fs::unlinkat(dir, name, AtFlags::REMOVEDIR)?)
}

fn directory(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
}

/// Clear the state an earlier ephemeral start left at `state`, when all five
/// proofs hold; otherwise fail naming the one that did not, with nothing
/// removed and no process signalled.
///
/// Proofs 1 to 3 are read first: a state without this role's marker is never
/// written to and never locked. Then, in this order:
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
/// Every lock stays held until the directory is gone. The marker and the
/// locks go last, so a clearing that was itself killed leaves a state the
/// next start can still prove and clear.
fn reclaim(
    state: &Path,
    resident_journal: bool,
    filesystem_of: impl Fn(&File) -> plan::Filesystem,
    at: &dyn Fn(Step),
) -> anyhow::Result<()> {
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
    let (Some(parent), Some(name)) = (state.parent(), state.file_name()) else {
        return Err(kept("it has no parent directory".into()));
    };
    let name = CString::new(std::os::unix::ffi::OsStrExt::as_bytes(name))?;
    let examined = read_marker(&dir);
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
        &filesystem_of(&dir),
        resident_journal,
    )
    .map_err(&kept)?;
    if !taken(&dir)? {
        return Err(kept(format!(
            "another ephemeral start is clearing it right now{by}"
        )));
    }
    at(Step::Examined);
    let open = |name: &str| {
        private_at(&dir, name, OFlags::RDWR | OFlags::CREATE)
            .map_err(|e| kept(format!("its {name} cannot be examined ({e}){by}")))
    };
    let ephemeral_lock = open(LOCK)?;
    let ephemeral_taken = taken(&ephemeral_lock)?;
    // agent.lock is only touched once no process of the earlier start is
    // left, so a worker that is starting right now is not disturbed.
    let agent_lock = if ephemeral_taken {
        Some(open(AGENT_LOCK)?)
    } else {
        None
    };
    let agent_taken = match &agent_lock {
        Some(lock) => taken(lock)?,
        None => false,
    };
    plan::unused(ephemeral_taken, agent_taken).map_err(|why| kept(format!("{why}{by}")))?;
    if examined.read.is_none() || read_marker(&dir).read != examined.read {
        return Err(kept(format!(
            "its ephemeral.json changed while it was examined{by}"
        )));
    }
    // A lock on a file that another start has already removed proves nothing
    // about the state now at this path: it must still be the one examined,
    // and the lock held must be the one in it.
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(parent)
        .map_err(|e| kept(format!("its parent directory cannot be opened ({e})")))?;
    let still = is_named(&parent, &name, &seen)
        && ephemeral_lock
            .metadata()
            .is_ok_and(|held| is_named(&dir, &c_name(LOCK), &held));
    if !still {
        return Err(kept("it was replaced while it was examined".into()));
    }
    at(Step::Proven);
    let last = [AGENT_LOCK, LOCK, MARKER];
    for entry in names_in(&dir)? {
        if last.iter().any(|name| entry.as_bytes() == name.as_bytes()) {
            continue;
        }
        remove_at(&dir, &entry)?;
    }
    for name in last {
        match rustix::fs::unlinkat(&dir, name, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(e) => return Err(std::io::Error::from(e).into()),
        }
    }
    // The examined directory is empty now. Its name goes only if the name
    // still is that directory; whatever sits there instead is not this
    // proof's to remove. Not recursive either: what appeared in it meanwhile
    // stays.
    anyhow::ensure!(
        is_named(&parent, &name, &seen),
        "worker state {} was replaced while it was cleared; what is at the path now was left in place",
        state.display()
    );
    rustix::fs::unlinkat(&parent, &name, AtFlags::REMOVEDIR).map_err(std::io::Error::from)?;
    Ok(())
}

/// The state directory this process created, and how to know it is still that one.
struct Created {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

/// Create the state: the directory, then `ephemeral.lock` (a new file, never
/// one that was there) held shared, and only then the marker, complete in one
/// step (written beside and renamed). So a marker that can be read implies
/// that its writer has held the lock since before the marker existed, and a
/// start that clears can never meet a marked state whose owner is still on
/// its way to the lock. The marker records the filesystem this process sees
/// the directory on.
///
/// The returned descriptor holds `ephemeral.lock` shared and is inherited by
/// every process this one starts (it is not close-on-exec), so the lock
/// stays held until the last of them is gone. Call before any other thread
/// exists.
///
/// A failure removes what was created. Only a kill between the directory and
/// the marker leaves a state without a marker, which the next start refuses.
fn create_state(
    path: &Path,
    marker: &MarkerFile,
    filesystem_of: impl Fn(&File) -> plan::Filesystem,
    at: &dyn Fn(Step),
) -> anyhow::Result<(Created, File)> {
    std::fs::DirBuilder::new().mode(0o700).create(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(meta.is_dir(), "worker state is not a directory");
    let dir = directory(path);
    let alive = (|| -> anyhow::Result<File> {
        let dir = dir.as_ref().map_err(|e| anyhow::anyhow!("{e}"))?;
        // Everything below is created inside the directory just made, not
        // inside whatever a concurrent start has put at its path.
        anyhow::ensure!(
            same_file(&dir.metadata()?, &meta),
            "worker state was replaced while it was created"
        );
        at(Step::Directory);
        let lock = private_at(dir, LOCK, OFlags::RDWR | OFlags::CREATE | OFlags::EXCL)?;
        match lock.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                anyhow::bail!("another ephemeral start is clearing this worker state")
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        at(Step::Locked);
        let mut marker = marker.clone();
        marker.filesystem = Some(plan::recorded(&filesystem_of(dir)));
        let mut part = private_at(
            dir,
            MARKER_PART,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL,
        )?;
        part.write_all(&serde_json::to_vec(&marker)?)?;
        part.sync_all()?;
        rustix::fs::renameat(dir, MARKER_PART, dir, MARKER).map_err(std::io::Error::from)?;
        dir.sync_all()?;
        anyhow::ensure!(
            std::fs::symlink_metadata(path).is_ok_and(|now| same_file(&now, &meta)),
            "worker state was replaced while it was created"
        );
        inherit(&lock)?;
        Ok(lock)
    })();
    let alive = match alive {
        Ok(alive) => alive,
        Err(e) => {
            // What this call made goes, the marker first; anything else in
            // the directory is left for the next start to judge.
            if let Ok(dir) = &dir {
                if dir.metadata().is_ok_and(|now| same_file(&now, &meta)) {
                    for name in [MARKER, MARKER_PART, LOCK] {
                        let _ = rustix::fs::unlinkat(dir, name, AtFlags::empty());
                    }
                }
            }
            if std::fs::symlink_metadata(path).is_ok_and(|now| same_file(&now, &meta)) {
                let _ = std::fs::remove_dir(path);
            }
            return Err(e);
        }
    };
    Ok((
        Created {
            path: path.to_owned(),
            dev: meta.dev(),
            ino: meta.ino(),
        },
        alive,
    ))
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
/// directory (never a replacement that happens to sit at the path).
fn clean_up(created: &Created) -> anyhow::Result<bool> {
    if !plan::removable(live_attempts(&created.path).ok()) {
        return Ok(false);
    }
    let meta = std::fs::symlink_metadata(&created.path)?;
    anyhow::ensure!(
        meta.is_dir() && meta.dev() == created.dev && meta.ino() == created.ino,
        "worker state was replaced; left in place"
    );
    std::fs::remove_dir_all(&created.path)?;
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
    let resident = resident_installed(&Paths::resident_journals(&env)?)?;
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
            reclaim(&spec.state_dir, resident, filesystem_of, &nowhere)?;
            eprintln!(
                "cleared the worker state an earlier ephemeral start left at {}",
                spec.state_dir.display()
            );
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
    let (created, _alive) = create_state(&spec.state_dir, &marker, filesystem_of, &nowhere)?;
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
    let erased = match clean_up(&created) {
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
/// counts as marked.
pub(super) fn marked(state: &Path) -> bool {
    !matches!(
        std::fs::symlink_metadata(state.join(MARKER)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound
    )
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
