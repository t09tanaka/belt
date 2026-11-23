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
//! the state carries this role's marker (`ephemeral.json`, written first), it
//! is on a local filesystem, no resident worker is installed, and both
//! `agent.lock` and `ephemeral.lock` can be taken. `ephemeral.lock` is held
//! shared by the worker and, through an inherited descriptor, by every
//! process it starts, so it is free only when all of them are gone. Without
//! the proof nothing is removed and no process is signalled.
use super::{open_private_lock, WorkerSpec};
use crate::service::resident;
use crate::service::worker_service::{live_attempts, Paths};
use std::fs::{File, Metadata, OpenOptions, TryLockError};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) mod plan;

/// The owner marker: the first file of an ephemeral worker's state.
pub(super) const MARKER: &str = "ephemeral.json";
/// Held shared by the worker and by every process it starts.
const LOCK: &str = "ephemeral.lock";
const AGENT_LOCK: &str = "agent.lock";
const MARKER_LIMIT: u64 = 64 * 1024;

/// `ephemeral.json`. Only `kind` decides anything; the rest says which start
/// left the state, for the person reading a refusal.
#[derive(serde::Serialize, serde::Deserialize)]
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
}

fn same_file(a: &Metadata, b: &Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

/// What the marker of a state left behind says, and its content when it parsed.
fn read_marker(state: &Path) -> (plan::Marker, Option<MarkerFile>) {
    let path = state.join(MARKER);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (plan::Marker::Absent, None),
        Err(_) => return (plan::Marker::Unreadable, None),
    }
    let mut bytes = Vec::new();
    let read = resident::secure_file(&path, false)
        .and_then(|file| file.take(MARKER_LIMIT + 1).read_to_end(&mut bytes));
    if read.is_err() || bytes.len() as u64 > MARKER_LIMIT {
        return (plan::Marker::Unreadable, None);
    }
    match serde_json::from_slice::<MarkerFile>(&bytes) {
        Ok(marker) if marker.kind == plan::MARKER_KIND => (plan::Marker::Ephemeral, Some(marker)),
        Ok(marker) => (plan::Marker::OtherKind, Some(marker)),
        Err(_) => (plan::Marker::Unreadable, None),
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

/// Clear the state an earlier ephemeral start left at `state`, when all five
/// proofs hold; otherwise fail naming the one that did not, with nothing
/// removed and no process signalled.
///
/// Proofs 1 to 3 are read first: a state without this role's marker is never
/// written to. The locks are then opened the way the worker opens its own (a
/// start killed between its marker and its locks leaves none, so a missing
/// lock file is created here; nobody can hold a file that did not exist, and
/// a start that is only now creating its own meets this one's exclusive lock
/// and gives up). Both locks stay held until the directory is gone.
///
/// The marker and the locks go last, so a clearing that was itself killed
/// leaves a state the next start can still prove and clear.
fn reclaim(
    state: &Path,
    resident_journal: bool,
    filesystem_of: impl Fn(&File) -> plan::Filesystem,
) -> anyhow::Result<()> {
    let kept = |why: String| {
        anyhow::anyhow!(
            "worker state {} was left by an earlier start and is kept: {why}",
            state.display()
        )
    };
    resident::check_private_dir(state)
        .map_err(|e| kept(format!("it is not a private directory of this user ({e})")))?;
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(state)
        .map_err(|e| kept(format!("it cannot be opened ({e})")))?;
    let seen = dir.metadata()?;
    let (marker, written) = read_marker(state);
    let by = written.map_or_else(String::new, |m| {
        format!(
            " (left by pid {} of boot {}, instance {})",
            m.pid.map_or_else(|| "?".into(), |p| p.to_string()),
            m.boot.as_deref().unwrap_or("?"),
            m.instance.as_deref().unwrap_or("?")
        )
    });
    plan::examined(&marker, &filesystem_of(&dir), resident_journal).map_err(&kept)?;
    let open = |name: &str| {
        open_private_lock(state, name)
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
    // A lock on a file that another start has already removed proves nothing
    // about the state now at this path: it must still be the one examined.
    let still = std::fs::symlink_metadata(state)
        .is_ok_and(|now| now.is_dir() && same_file(&now, &seen))
        && std::fs::symlink_metadata(state.join(LOCK)).is_ok_and(|now| {
            ephemeral_lock
                .metadata()
                .is_ok_and(|held| same_file(&now, &held))
        });
    if !still {
        return Err(kept("it was replaced while it was examined".into()));
    }
    let last = [AGENT_LOCK, LOCK, MARKER];
    for entry in std::fs::read_dir(state)? {
        let entry = entry?;
        if last.iter().any(|name| entry.file_name() == *name) {
            continue;
        }
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    for name in last {
        match std::fs::remove_file(state.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    // Not recursive: whatever appeared meanwhile is not this proof's to remove.
    std::fs::remove_dir(state)?;
    Ok(())
}

/// The state directory this process created, and how to know it is still that one.
struct Created {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

/// Create the state: the directory, then the marker as its first file, then
/// the liveness lock. The returned descriptor holds `ephemeral.lock` shared
/// and is inherited by every process this one starts (it is not
/// close-on-exec), so the lock stays held until the last of them is gone.
/// Call before any other thread exists.
fn create_state(path: &Path, marker: &MarkerFile) -> anyhow::Result<(Created, File)> {
    std::fs::DirBuilder::new().mode(0o700).create(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(meta.is_dir(), "worker state is not a directory");
    let alive = (|| -> anyhow::Result<File> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path.join(MARKER))?;
        file.write_all(&serde_json::to_vec(marker)?)?;
        file.sync_all()?;
        let lock = open_private_lock(path, LOCK)?;
        match lock.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                anyhow::bail!("another ephemeral start is clearing this worker state")
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        // The lock must be on the file of the directory just created, not on
        // one a concurrent start has removed together with it.
        let ours = std::fs::symlink_metadata(path).is_ok_and(|now| same_file(&now, &meta))
            && std::fs::symlink_metadata(path.join(LOCK))
                .is_ok_and(|now| lock.metadata().is_ok_and(|held| same_file(&now, &held)));
        anyhow::ensure!(ours, "worker state was replaced while it was created");
        inherit(&lock)?;
        Ok(lock)
    })();
    let alive = match alive {
        Ok(alive) => alive,
        Err(e) => {
            // Only an empty directory goes; anything else in it is left for
            // the next start to judge.
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
    let resident = std::fs::symlink_metadata(Paths::journal_path(&env)?).is_ok();
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
            reclaim(&spec.state_dir, resident, filesystem_of)?;
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
    };
    // Held until this process ends; its children hold their own copies.
    let (created, _alive) = create_state(&spec.state_dir, &marker)?;
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
