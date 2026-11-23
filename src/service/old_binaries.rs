//! Spec 25.7 (a): the version directories `<config>/bin/<digest>/` and the
//! tool links `tool-bin/<id>/` of versions nothing names any more.
//!
//! A version directory goes only when every thing that can name it has been
//! looked at and none does: the links `bin/current` and `bin/resident`, the
//! service definitions, the journals (with the trial's way back), the
//! running process itself. What can run a version without naming it in a
//! file (a setup builder, a supervisor, the upgrade guard) stops the whole
//! pass while it can be there. A place that cannot be read stops the pass
//! too: nothing is removed on a guess. The commands that place a version
//! (`init`, `worker-service install` and `upgrade`) hold `init.lock` or
//! `worker-service.lock` from before the directory is made until a journal
//! names it; the pass takes both without waiting, and a directory younger
//! than [`GRACE`] is kept besides (a first `init`, before `init.lock` exists).
//!
//! A `tool-bin/<id>/` goes with its version: when each of its links points
//! at `<config>/bin/<digest>/belt` and that directory is gone. Anything else
//! there (a link to a build outside `<config>/bin`, a name that cannot be
//! read) is kept.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::worker_service::upgrade::is_digest;

/// A version directory this young may be one a command is placing now.
pub(crate) const GRACE: Duration = Duration::from_secs(600);
/// Version directories removed in one pass at most (spec 25.11: bounded).
pub(crate) const PER_PASS: usize = 8;
/// A journal or service definition larger than this is not belt's.
const TEXT_LIMIT: u64 = 1 << 20;

/// Where the pass looks.
#[derive(Clone, Debug)]
pub(crate) struct Places {
    /// The configuration directory: `bin/`, the journals, `upgrade/`.
    pub config: PathBuf,
    /// The worker's state: `tool-bin/`, `setup/builders/`, `worker.db`.
    pub worker_state: PathBuf,
    /// The parent's state (its trial marker), when this host has one.
    pub parent_state: Option<PathBuf>,
    /// The directories of the service definitions of this user.
    pub services: Vec<PathBuf>,
}

impl Places {
    /// The places of this user's installation, as `env` names them.
    pub(crate) fn of(
        env: &BTreeMap<String, String>,
        backend: super::macos::policy::Backend,
    ) -> anyhow::Result<Self> {
        let paths = super::worker_service::Paths::resolve_for(env, backend)?;
        let parent = super::init::temporary_dirs(env, backend).map(|[state, _]| state);
        Ok(Self {
            config: paths.config,
            worker_state: paths.state,
            parent_state: parent,
            services: vec![paths.services],
        })
    }
    fn bin(&self) -> PathBuf {
        self.config.join("bin")
    }
}

/// Why a pass removed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Held {
    /// An upgrade is on trial (25.1 principle 8).
    Trial,
    /// The upgrade guard is registered or its definition is there.
    Guard,
    /// A command that places a version holds its lock.
    Placing,
    /// A setup builder's directory is there.
    Builder,
    /// The worker has an attempt that is not terminal (a supervisor).
    Supervisor,
    /// Something that could name a version could not be read.
    Unreadable,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub removed: Vec<String>,
    pub tool_bins: usize,
    pub held: Option<Held>,
}

/// One pass. `guard` answers whether the upgrade guard is registered or has
/// a definition; `switch_held`, that the caller holds `worker-service.lock`
/// already (the worker's commit).
pub(crate) fn sweep(
    places: &Places,
    guard: &mut dyn FnMut() -> anyhow::Result<bool>,
    switch_held: bool,
    now: SystemTime,
) -> Outcome {
    match pass(places, guard, switch_held, now) {
        Ok(outcome) => outcome,
        Err(held) => Outcome {
            held: Some(held),
            ..Outcome::default()
        },
    }
}

fn pass(
    places: &Places,
    guard: &mut dyn FnMut() -> anyhow::Result<bool>,
    switch_held: bool,
    now: SystemTime,
) -> Result<Outcome, Held> {
    if on_trial(places)? {
        return Err(Held::Trial);
    }
    if guard().map_err(|_| Held::Unreadable)? {
        return Err(Held::Guard);
    }
    // Held to the end: nothing places a version while the pass looks.
    let _init = try_lock(&places.config.join("init.lock"), false)?;
    let _switch = if switch_held {
        None
    } else {
        try_lock(&places.config.join("worker-service.lock"), true)?
    };
    if builders_present(&places.worker_state)? {
        return Err(Held::Builder);
    }
    if attempts_live(&places.worker_state)? {
        return Err(Held::Supervisor);
    }
    let named = Named::read(places)?;
    let mut outcome = Outcome::default();
    let bin = places.bin();
    for name in entries(&bin)? {
        if outcome.removed.len() >= PER_PASS {
            break;
        }
        if !is_digest(&name) || named.names(&name) {
            continue;
        }
        let dir = bin.join(&name);
        let Ok(meta) = fs::symlink_metadata(&dir) else {
            continue;
        };
        let young = meta
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .is_none_or(|age| age < GRACE);
        if !meta.is_dir() || meta.uid() != euid() || young {
            continue;
        }
        if fs::remove_dir_all(&dir).is_ok() {
            outcome.removed.push(name);
        }
    }
    outcome.tool_bins = sweep_tool_bins(places);
    Ok(outcome)
}

fn euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

/// A marker that cannot be told apart from none is a trial.
fn present(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(e) => e.kind() != io::ErrorKind::NotFound,
    }
}

fn on_trial(places: &Places) -> Result<bool, Held> {
    let mut markers = vec![
        places.config.join("upgrade").join("record.json"),
        crate::worker::control::trial_path(&places.worker_state),
    ];
    if let Some(parent) = &places.parent_state {
        markers.push(crate::worker::control::trial_path(parent));
    }
    Ok(markers.iter().any(|m| present(m)))
}

/// The lock at `path`, taken without waiting. `None` when there is no such
/// file and `create` is off (nothing that would take it has made it).
fn try_lock(path: &Path, create: bool) -> Result<Option<File>, Held> {
    use std::os::fd::AsRawFd;
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound && !create => return Ok(None),
        Err(_) => return Err(Held::Unreadable),
    };
    // SAFETY: `file` is an open descriptor owned here.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        Ok(Some(file))
    } else {
        Err(Held::Placing)
    }
}

/// The names in `dir`; none when it is not there.
fn entries(dir: &Path) -> Result<Vec<String>, Held> {
    match fs::read_dir(dir) {
        Ok(rd) => Ok(rd
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .collect()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(_) => Err(Held::Unreadable),
    }
}

/// A setup builder runs a version without naming it anywhere a file says
/// (spec 25.7 (a)): while any builder directory is there, nothing goes. A
/// builder's directory is removed only after its supervisor and what it
/// started are confirmed stopped (`Setups::reap`, `settle_old`).
fn builders_present(state: &Path) -> Result<bool, Held> {
    Ok(!entries(&state.join("setup").join("builders"))?.is_empty())
}

fn attempts_live(state: &Path) -> Result<bool, Held> {
    if !state
        .join("worker.db")
        .try_exists()
        .map_err(|_| Held::Unreadable)?
    {
        return Ok(false);
    }
    let store = crate::storage::WorkerStore::open_read_only(state).map_err(|_| Held::Unreadable)?;
    Ok(!store
        .list_nonterminal()
        .map_err(|_| Held::Unreadable)?
        .is_empty())
}

/// What names versions: the two links, the running executable, and the text
/// of each journal and service definition.
struct Named {
    links: Vec<String>,
    texts: Vec<String>,
}

impl Named {
    fn read(places: &Places) -> Result<Self, Held> {
        let bin = places.bin();
        let mut links = Vec::new();
        for link in ["current", "resident"] {
            match fs::read_link(bin.join(link)) {
                Ok(target) => links.push(target.to_string_lossy().into_owned()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(_) => return Err(Held::Unreadable),
            }
        }
        if let Ok(exe) = std::env::current_exe() {
            links.push(exe.to_string_lossy().into_owned());
        }
        let mut texts = Vec::new();
        let mut journals = vec![
            places.config.join("init.json"),
            places.config.join("worker-service.json"),
            places.config.join("upgrade").join("record.json"),
            places.config.join("upgrade").join("init.json"),
        ];
        let mut states = vec![places.worker_state.clone()];
        states.extend(places.parent_state.clone());
        for state in &states {
            for name in entries(state)? {
                if name.starts_with("upgrade-trial") {
                    journals.push(state.join(name));
                }
            }
        }
        for dir in &places.services {
            for name in entries(dir)? {
                journals.push(dir.join(name));
            }
        }
        for path in journals {
            if let Some(text) = read_text(&path)? {
                texts.push(text);
            }
        }
        Ok(Self { links, texts })
    }

    fn names(&self, digest: &str) -> bool {
        self.links
            .iter()
            .chain(&self.texts)
            .any(|text| text.contains(digest))
    }
}

/// The text of a regular file, `None` when it is not there or not a regular
/// file (a directory in the services directory is nobody's definition).
fn read_text(path: &Path) -> Result<Option<String>, Held> {
    let meta = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(Held::Unreadable),
    };
    if !meta.is_file() {
        return Ok(None);
    }
    if meta.len() > TEXT_LIMIT {
        // Too large to be belt's: it names nothing of belt's.
        return Ok(None);
    }
    match fs::read(path) {
        Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(Held::Unreadable),
    }
}

/// Remove each `tool-bin/<id>/` all of whose links point at a version
/// directory of this configuration that is gone. Returns how many went.
fn sweep_tool_bins(places: &Places) -> usize {
    let root = places.worker_state.join("tool-bin");
    let bin = places.bin();
    let Ok(names) = entries(&root) else {
        return 0;
    };
    let mut removed = 0;
    for name in names {
        let dir = root.join(&name);
        let Ok(meta) = fs::symlink_metadata(&dir) else {
            continue;
        };
        if !meta.is_dir() || meta.uid() != euid() {
            continue;
        }
        if gone_version_links(&dir, &bin) && fs::remove_dir_all(&dir).is_ok() {
            removed += 1;
        }
    }
    removed
}

fn gone_version_links(dir: &Path, bin: &Path) -> bool {
    let Ok(rd) = fs::read_dir(dir) else {
        return false;
    };
    let mut any = false;
    for entry in rd {
        let Ok(entry) = entry else {
            return false;
        };
        let Ok(target) = fs::read_link(entry.path()) else {
            return false;
        };
        let Some(version) = target.parent() else {
            return false;
        };
        let in_bin = version.parent() == Some(bin)
            && version
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(is_digest);
        if !in_bin || present(version) {
            return false;
        }
        any = true;
    }
    any
}

/// Spec 25.7 (a) at the start of the resident and of the worker: the same
/// pass as at the commit, for what the commit could not remove (a builder was
/// running). The guard is asked of the service manager.
pub(crate) fn sweep_at_start(env: &BTreeMap<String, String>) {
    let mut launcher = super::worker_service::HostLauncher::native();
    let backend = super::macos::Launcher::backend(&launcher);
    let Ok(places) = Places::of(env, backend) else {
        return;
    };
    let config = places.config.clone();
    let _ = sweep(
        &places,
        &mut || super::init::upgrade::guard_present(&config, &mut launcher),
        false,
        SystemTime::now(),
    );
}

/// How many version directories other than the ones `bin/current` and
/// `bin/resident` point at are kept (for `doctor`): named by something else
/// now, or not proven unnamed.
pub(crate) fn kept_versions(env: &BTreeMap<String, String>) -> usize {
    let Ok(paths) = super::worker_service::Paths::resolve(env) else {
        return 0;
    };
    let bin = paths.config.join("bin");
    let linked: Vec<String> = ["current", "resident"]
        .iter()
        .filter_map(|l| fs::read_link(bin.join(l)).ok())
        .map(|t| t.to_string_lossy().into_owned())
        .collect();
    entries(&bin)
        .unwrap_or_default()
        .iter()
        .filter(|n| is_digest(n) && !linked.iter().any(|l| l.contains(n.as_str())))
        .count()
}

#[cfg(test)]
mod tests;
