//! Temporary directories whose owner can be proven later.
//!
//! A guard removes its directory when it is dropped, but a process that is
//! killed, or that exits while another thread still holds a guard, runs no
//! destructor. So the directory also records who made it: a marker file with
//! the boot, the PID and the start identity of the owning process, and the
//! host and PID namespace in which those three mean anything. A later process
//! may remove such a directory only when that record proves the owner is
//! gone, and only a process on the same host and in the same PID namespace
//! can prove that: the parent directory may be shared between hosts or
//! containers. Every doubt keeps the directory. A directory without a
//! readable marker is never touched, and no process is ever signalled here.

use super::{boot_id, process_start_identity, StartIdentity};
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const MARKER: &str = "owner";
const INSIDE: &str = "d";
const MAX_MARKER_BYTES: u64 = 1024;
/// How much later than the recorded start (in seconds) the process that has
/// the PID now must have started on macOS to count as another process. The
/// start is wall-clock time there, so a small difference proves nothing.
const MAC_CLEARLY_LATER_SECS: u64 = 2;

/// Where a PID, a start identity and a boot ID can be compared: the same
/// facts read on another host, or in another PID namespace, say nothing about
/// the processes here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Place {
    host: String,
    /// Linux: the inode of `/proc/self/ns/pid`. macOS has no PID namespaces.
    pid_ns: String,
    boot: String,
}

impl Place {
    /// `None` when any of the three cannot be read: then nothing is recorded
    /// and nothing is swept.
    fn here() -> Option<Place> {
        Some(Place {
            host: hostname()?,
            pid_ns: pid_namespace()?,
            boot: boot_id()?,
        })
    }
}

/// One line of a marker: not empty and on one line.
fn one_line(s: &str) -> Option<&str> {
    (!s.is_empty() && !s.contains(['\n', '\r'])).then_some(s)
}

fn hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for `buf.len()` bytes.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    // Without a NUL the name may have been cut short: not known.
    let len = buf.iter().position(|b| *b == 0)?;
    one_line(std::str::from_utf8(&buf[..len]).ok()?).map(str::to_string)
}

fn pid_namespace() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let ino = fs::metadata("/proc/self/ns/pid").ok()?.ino();
        (ino != 0).then(|| format!("linux:{ino}"))
    }
    #[cfg(target_os = "macos")]
    {
        Some("mac:none".to_string())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// What a marker records.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Owner {
    place: Place,
    pid: u32,
    start: String,
}

impl Owner {
    /// The last newline is what says the record is complete.
    fn marker_text(&self) -> String {
        let Place { host, pid_ns, boot } = &self.place;
        format!("{boot}\n{}\n{}\n{host}\n{pid_ns}\n", self.pid, self.start)
    }
}

/// `<parent>/<prefix>XXXXXX/` holding the marker and the empty directory
/// [`OwnedTemp::path`] that the owner works in.
pub struct OwnedTemp {
    inside: PathBuf,
    // Removed when this guard is dropped, after `Drop::drop` below.
    dir: tempfile::TempDir,
}

impl OwnedTemp {
    pub fn new_in(parent: &Path, prefix: &str) -> io::Result<Self> {
        let dir = tempfile::Builder::new().prefix(prefix).tempdir_in(parent)?;
        let inside = dir.path().join(INSIDE);
        fs::create_dir(&inside)?;
        let pid = std::process::id();
        // Without every fact nothing can be proven later: no marker, and the
        // directory is only ever removed by this guard.
        if let (Some(place), StartIdentity::Identity(start)) =
            (Place::here(), process_start_identity(pid))
        {
            let mut marker = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(dir.path().join(MARKER))?;
            marker.write_all(Owner { place, pid, start }.marker_text().as_bytes())?;
        }
        Ok(Self { inside, dir })
    }

    /// For tests of other crates' sweeps (`tests/it`): the directory `dir`
    /// gets a marker that names `pid` and its `start` as the owner, in this
    /// place. A directory that is not made by [`OwnedTemp::new_in`] has none.
    #[doc(hidden)]
    pub fn write_marker_of(dir: &Path, pid: u32, start: &str) -> io::Result<()> {
        let place = Place::here().ok_or_else(|| io::Error::other("place unknown"))?;
        fs::write(
            dir.join(MARKER),
            Owner {
                place,
                pid,
                start: start.to_string(),
            }
            .marker_text(),
        )
    }

    /// Remove the directory now and say whether that worked (dropping the
    /// guard does the same and ignores the result).
    pub fn close(self) -> io::Result<()> {
        make_writable(self.dir.path());
        fs::remove_dir_all(self.dir.path())
    }

    /// The empty directory for the owner's use.
    pub fn path(&self) -> &Path {
        &self.inside
    }

    /// The directory that holds the marker: what a sweep looks at.
    pub fn outer(&self) -> &Path {
        self.dir.path()
    }
}

impl Drop for OwnedTemp {
    fn drop(&mut self) {
        // A read-only directory inside would stop the removal at its first entry.
        make_writable(self.dir.path());
    }
}

/// Give the owner write access to every directory under `root` (symbolic
/// links are not followed), so that the tree can be removed.
pub fn make_writable(root: &Path) {
    let mut todo = vec![root.to_path_buf()];
    while let Some(dir) = todo.pop() {
        let Ok(meta) = fs::symlink_metadata(&dir) else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let mut mode = meta.permissions();
        if mode.mode() & 0o700 != 0o700 {
            mode.set_mode(mode.mode() | 0o700);
            let _ = fs::set_permissions(&dir, mode);
        }
        if let Ok(entries) = fs::read_dir(&dir) {
            todo.extend(entries.flatten().map(|e| e.path()));
        }
    }
}

/// Remove every `<parent>/<prefix>*` directory whose marker proves that its
/// owner is gone. Returns how many were removed.
pub fn sweep(parent: &Path, prefix: &str) -> usize {
    let Ok(entries) = fs::read_dir(parent) else {
        return 0;
    };
    let here = Place::here();
    let mut removed = 0;
    for entry in entries.flatten() {
        let named = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(prefix));
        let path = entry.path();
        if named && owner_is_gone(&path, here.as_ref()) {
            make_writable(&path);
            if fs::remove_dir_all(&path).is_ok() {
                removed += 1;
            }
        }
    }
    removed
}

/// Remove the directory `dir` if its marker proves that its owner is gone.
/// Whether it was removed.
pub fn remove_if_owner_gone(dir: &Path) -> bool {
    if !owner_is_gone(dir, Place::here().as_ref()) {
        return false;
    }
    make_writable(dir);
    fs::remove_dir_all(dir).is_ok()
}

/// True only with proof: a real directory of this user, a complete marker,
/// and [`proven_gone`]. Every "cannot tell" is false.
fn owner_is_gone(dir: &Path, here: Option<&Place>) -> bool {
    let Ok(meta) = fs::symlink_metadata(dir) else {
        return false;
    };
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } {
        return false;
    }
    let (Some(owner), Some(here)) = (read_marker(&dir.join(MARKER)), here) else {
        return false;
    };
    // The PID is read only where it can be the owner's: here, in this boot.
    let now = if owner.place == *here {
        process_start_identity(owner.pid)
    } else {
        StartIdentity::Unknown
    };
    proven_gone(&owner, here, &now)
}

/// Whether `owner` is proven gone, seen from `here`, where `now` is what the
/// system says about the process that has the owner's PID.
///
/// - Another host or another PID namespace: never. The owner may be alive
///   where this process cannot see it.
/// - The same host and namespace but another boot: gone with that boot.
/// - The same boot: gone when no process has the PID, or when the process
///   that has it started clearly later than the owner did (the PID was
///   reused). A start that differs in any other way is not proof.
fn proven_gone(owner: &Owner, here: &Place, now: &StartIdentity) -> bool {
    if owner.place.host != here.host || owner.place.pid_ns != here.pid_ns {
        return false;
    }
    if owner.place.boot != here.boot {
        return true;
    }
    match now {
        StartIdentity::NoProcess => true,
        // A zombie that still is the owner is kept; so is an unreadable PID.
        StartIdentity::Identity(now) | StartIdentity::Zombie(now) => {
            clearly_later(now, &owner.start)
        }
        StartIdentity::Unknown => false,
    }
}

/// Whether the start identity `now` is certainly a later start than
/// `recorded`. Both must be of the same kind and parse; anything else is
/// false.
///
/// Linux counts clock ticks since boot, which never go back, so any later
/// tick is another process. macOS records wall-clock time, which can be
/// stepped, so only a start at least [`MAC_CLEARLY_LATER_SECS`] later counts.
fn clearly_later(now: &str, recorded: &str) -> bool {
    if let (Some(now), Some(recorded)) = (linux_ticks(now), linux_ticks(recorded)) {
        return now > recorded;
    }
    if let (Some(now), Some(recorded)) = (mac_micros(now), mac_micros(recorded)) {
        return now
            .checked_sub(recorded)
            .is_some_and(|later| later >= MAC_CLEARLY_LATER_SECS * 1_000_000);
    }
    false
}

fn digits(s: &str) -> Option<u64> {
    (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}

/// `linux:<ticks>`.
fn linux_ticks(start: &str) -> Option<u64> {
    digits(start.strip_prefix("linux:")?)
}

/// `mac:<sec>.<usec, six digits>` in microseconds.
fn mac_micros(start: &str) -> Option<u64> {
    let (sec, usec) = start.strip_prefix("mac:")?.split_once('.')?;
    if usec.len() != 6 {
        return None;
    }
    digits(sec)?
        .checked_mul(1_000_000)?
        .checked_add(digits(usec)?)
}

fn read_marker(path: &Path) -> Option<Owner> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .ok()?;
    let meta = file.metadata().ok()?;
    if !meta.is_file() || meta.len() > MAX_MARKER_BYTES {
        return None;
    }
    let mut text = String::new();
    file.take(MAX_MARKER_BYTES).read_to_string(&mut text).ok()?;
    parse_marker(&text)
}

/// Exactly five complete lines: boot, PID, start, host, PID namespace. A
/// marker of the older three-line form does not say where its PID is valid,
/// so it is not a record.
fn parse_marker(text: &str) -> Option<Owner> {
    let mut lines = text.strip_suffix('\n')?.split('\n');
    let boot = one_line(lines.next()?)?;
    let pid: u32 = lines.next()?.parse().ok().filter(|pid| *pid > 1)?;
    let start = one_line(lines.next()?)?;
    let host = one_line(lines.next()?)?;
    let pid_ns = one_line(lines.next()?)?;
    if lines.next().is_some() || i32::try_from(pid).is_err() {
        return None;
    }
    Some(Owner {
        place: Place {
            host: host.to_string(),
            pid_ns: pid_ns.to_string(),
            boot: boot.to_string(),
        },
        pid,
        start: start.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Single temporary files: the NAME proves the owner (spec 25.4 (a)).
//
// `.belt-tmp.<place hash>.<pid>.<start identity>.<random>`: the same three
// facts as a marker, with the place folded into 16 hex digits so that the
// name stays at most 71 bytes, far below `NAME_MAX`.
// ---------------------------------------------------------------------------

/// Every temporary file belt names itself starts with this. Directories that
/// other products share (LaunchAgents, systemd units) are only looked at
/// through it.
pub const FILE_PREFIX: &str = ".belt-tmp.";
const RANDOM_LEN: usize = 12;
const MAX_PID_DIGITS: usize = 10;
const MAX_START_DIGITS: usize = 20;
const NO_PLACE_HASH: &str = "0000000000000000";

/// The first 16 hex digits of SHA-256 over host, PID namespace and boot,
/// separated by NUL (which none of them can contain).
fn place_hash(place: &Place) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(place.host.as_bytes());
    h.update([0]);
    h.update(place.pid_ns.as_bytes());
    h.update([0]);
    h.update(place.boot.as_bytes());
    h.finalize()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A start identity as the decimal that goes into a name: Linux clock ticks,
/// or macOS microseconds since the epoch. `None` for any other form.
fn start_digits(start: &str) -> Option<String> {
    if let Some(ticks) = linux_ticks(start) {
        return Some(ticks.to_string());
    }
    mac_micros(start).map(|micros| micros.to_string())
}

/// The inverse of [`start_digits`], for the platform this runs on.
fn start_from_digits(digits_text: &str) -> Option<String> {
    let n = digits(digits_text)?;
    #[cfg(target_os = "linux")]
    {
        Some(format!("linux:{n}"))
    }
    #[cfg(target_os = "macos")]
    {
        Some(format!("mac:{}.{:06}", n / 1_000_000, n % 1_000_000))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = n;
        None
    }
}

/// `.belt-tmp.<hash>.<pid>.<start>.` for this process. When the place or the
/// start cannot be read, the hash is all zeros: such a name is never
/// recognised as anybody's, so it is only ever removed by its creator.
pub fn file_prefix() -> String {
    use std::sync::Mutex;
    static CACHE: Mutex<Option<(u32, String)>> = Mutex::new(None);
    let pid = std::process::id();
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((cached, prefix)) = cache.as_ref() {
        if *cached == pid {
            return prefix.clone();
        }
    }
    let (hash, start) = match (Place::here(), process_start_identity(pid)) {
        (Some(place), StartIdentity::Identity(start)) => (
            place_hash(&place),
            start_digits(&start).unwrap_or_else(|| "0".to_string()),
        ),
        _ => (NO_PLACE_HASH.to_string(), "0".to_string()),
    };
    let prefix = format!("{FILE_PREFIX}{hash}.{pid}.{start}.");
    *cache = Some((pid, prefix.clone()));
    prefix
}

/// A new temporary file (mode 0600) in `dir`, named by the rule above.
pub fn file_in(dir: impl AsRef<Path>) -> io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(&file_prefix())
        .rand_bytes(RANDOM_LEN)
        .tempfile_in(dir)
}

/// A name for a temporary file that the caller creates itself (with
/// `O_EXCL`, or by `openat`): the prefix of this process and 12 random
/// alphanumerics.
pub fn new_file_name() -> String {
    const ALPHABET: &[u8; 62] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut random = [0u8; RANDOM_LEN];
    // Without randomness the exclusive create below the caller does is what
    // keeps two names from meeting; the pid and a counter still differ.
    if getrandom::fill(&mut random).is_err() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        random.copy_from_slice(&[n.to_le_bytes(), n.to_be_bytes()].concat()[..RANDOM_LEN]);
    }
    let mut name = file_prefix();
    name.extend(
        random
            .iter()
            .map(|b| ALPHABET[usize::from(*b) % ALPHABET.len()] as char),
    );
    name
}

/// What a temporary file name says: the place hash and the owner.
#[derive(Debug, PartialEq, Eq)]
struct FileName {
    hash: String,
    pid: u32,
    start_digits: String,
}

fn parse_file_name(name: &str) -> Option<FileName> {
    let rest = name.strip_prefix(FILE_PREFIX)?;
    let mut parts = rest.split('.');
    let (hash, pid, start, random) = (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some()
        || hash.len() != 16
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || pid.len() > MAX_PID_DIGITS
        || start.len() > MAX_START_DIGITS
        || random.len() != RANDOM_LEN
        || !random.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return None;
    }
    let pid: u32 = digits(pid)?.try_into().ok().filter(|pid| *pid > 1)?;
    i32::try_from(pid).ok()?;
    digits(start)?;
    Some(FileName {
        hash: hash.to_string(),
        pid,
        start_digits: start.to_string(),
    })
}

/// How many entries of `dir` are named like belt's temporary files.
pub fn count_files(dir: &Path) -> usize {
    fs::read_dir(dir).map_or(0, |entries| {
        entries
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(FILE_PREFIX))
            .count()
    })
}

/// Whether `name` is a temporary file name of this very process.
pub fn is_own_file_name(name: &str) -> bool {
    parse_file_name(name).is_some_and(|n| n.pid == std::process::id())
}

/// How a sweep of one directory went.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FileSweep {
    pub removed: usize,
    /// Entries named like belt's temporary files that were left: another
    /// place, a live owner, an unreadable name, or not ours to remove.
    pub kept: usize,
}

/// One `readdir` of `dir`: remove the temporary files (named by the rule
/// above) whose owner is proven gone. Only regular files of this user with
/// one link are removed, symbolic links are never followed, and entries that
/// do not start with [`FILE_PREFIX`] are not looked at.
pub fn sweep_files(dir: &Path) -> FileSweep {
    let mut out = FileSweep::default();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    let here = Place::here();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|n| n.starts_with(FILE_PREFIX)) else {
            continue;
        };
        let path = entry.path();
        if file_owner_is_gone(&path, name, here.as_ref()) && fs::remove_file(&path).is_ok() {
            out.removed += 1;
        } else {
            out.kept += 1;
        }
    }
    out
}

/// True only with proof: a regular file of this user with one link, and a
/// name that [`name_owner_is_gone`].
fn file_owner_is_gone(path: &Path, name: &str, here: Option<&Place>) -> bool {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    // A link of a switch in progress is removed itself, never followed.
    let plain = (meta.is_file() && meta.nlink() == 1) || meta.file_type().is_symlink();
    if !plain || meta.uid() != unsafe { libc::geteuid() } {
        return false;
    }
    name_is_of_gone_owner(name, here)
}

/// Whether `name` is a temporary file name (see [`FILE_PREFIX`]) that parses,
/// was made in this place, and whose process is proven gone. The caller
/// still has to look at the file itself (type, owner, links).
pub fn name_owner_is_gone(name: &str) -> bool {
    name_is_of_gone_owner(name, Place::here().as_ref())
}

fn name_is_of_gone_owner(name: &str, here: Option<&Place>) -> bool {
    let (Some(parsed), Some(here)) = (parse_file_name(name), here) else {
        return false;
    };
    if parsed.hash != place_hash(here) {
        return false;
    }
    let Some(start) = start_from_digits(&parsed.start_digits) else {
        return false;
    };
    let owner = Owner {
        place: here.clone(),
        pid: parsed.pid,
        start,
    };
    proven_gone(&owner, here, &process_start_identity(parsed.pid))
}

/// What tests of the places that sweep need: owners that are gone or alive,
/// and the files and directories that such owners made.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A process that ended and was reaped: its PID and start identity.
    pub(crate) fn ended_process() -> (u32, String) {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        // Read while the child is held (alive or a zombie): certainly its own.
        let start = match process_start_identity(pid) {
            StartIdentity::Identity(start) | StartIdentity::Zombie(start) => start,
            other => panic!("unreadable child: {other:?}"),
        };
        child.wait().unwrap();
        (pid, start)
    }

    /// The start identity of this process, moved by `secs` (earlier when
    /// negative): another process that has this PID now.
    pub(crate) fn my_start_shifted(secs: i64) -> String {
        let StartIdentity::Identity(mine) = process_start_identity(std::process::id()) else {
            panic!("own start is readable");
        };
        if let Some(ticks) = linux_ticks(&mine) {
            return format!("linux:{}", ticks.checked_add_signed(secs).unwrap());
        }
        let micros = mac_micros(&mine).expect("a known start format");
        let micros = micros.checked_add_signed(secs * 1_000_000).unwrap();
        format!("mac:{}.{:06}", micros / 1_000_000, micros % 1_000_000)
    }

    pub(crate) fn this_start() -> String {
        my_start_shifted(0)
    }

    /// A temporary file name of the rule, as made by `pid` with `start`.
    pub(crate) fn name_of(pid: u32, start: &str, random: &str) -> String {
        let hash = place_hash(&Place::here().expect("a readable place"));
        format!(
            "{FILE_PREFIX}{hash}.{pid}.{}.{random}",
            start_digits(start).unwrap()
        )
    }

    /// The same, with a place hash that is not this one's.
    pub(crate) fn name_in_another_place(pid: u32, start: &str, random: &str) -> String {
        let name = name_of(pid, start, random);
        let here = &name[FILE_PREFIX.len()..FILE_PREFIX.len() + 16];
        let other = if here == "ffffffffffffffff" {
            "0123456789abcdef"
        } else {
            "ffffffffffffffff"
        };
        name.replacen(here, other, 1)
    }

    /// A name of a process that is gone.
    pub(crate) fn dead_name(random: &str) -> String {
        let (pid, start) = ended_process();
        name_of(pid, &start, random)
    }

    /// A name of this (live) process.
    pub(crate) fn live_name(random: &str) -> String {
        name_of(std::process::id(), &this_start(), random)
    }

    /// `<parent>/<prefix>XXXXXX/` with a marker of a gone owner (`dead`) or of this process.
    pub(crate) fn marker_dir(parent: &Path, prefix: &str, dead: bool) -> PathBuf {
        let dir = parent.join(format!(
            "{prefix}{}",
            if dead { "DEAD00" } else { "LIVE00" }
        ));
        fs::create_dir(&dir).unwrap();
        fs::create_dir(dir.join(INSIDE)).unwrap();
        let (pid, start) = if dead {
            ended_process()
        } else {
            (std::process::id(), this_start())
        };
        let owner = Owner {
            place: Place::here().unwrap(),
            pid,
            start,
        };
        fs::write(dir.join(MARKER), owner.marker_text()).unwrap();
        dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent() -> tempfile::TempDir {
        crate::test_tmp::tempdir().unwrap()
    }

    fn write_marker(dir: &Path, text: &str) {
        fs::write(dir.join(MARKER), text).unwrap();
    }

    /// A process that ended and was reaped: its PID and start identity.
    fn ended_process() -> (u32, String) {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        // Read while the child is held (alive or a zombie): certainly its own.
        let start = match process_start_identity(pid) {
            StartIdentity::Identity(start) | StartIdentity::Zombie(start) => start,
            other => panic!("unreadable child: {other:?}"),
        };
        child.wait().unwrap();
        (pid, start)
    }

    /// A five-line marker.
    fn marker(place: &Place, pid: u32, start: &str) -> String {
        Owner {
            place: place.clone(),
            pid,
            start: start.to_string(),
        }
        .marker_text()
    }

    fn place(host: &str, pid_ns: &str, boot: &str) -> Place {
        Place {
            host: host.into(),
            pid_ns: pid_ns.into(),
            boot: boot.into(),
        }
    }

    fn owner(start: &str) -> Owner {
        Owner {
            place: place("host", "ns", "boot"),
            pid: 4242,
            start: start.into(),
        }
    }

    /// The start identity of this process moved by `secs` (macOS) or as many
    /// ticks (Linux).
    fn my_start_shifted(secs: i64) -> String {
        let StartIdentity::Identity(mine) = process_start_identity(std::process::id()) else {
            panic!("own start is readable");
        };
        if let Some(ticks) = linux_ticks(&mine) {
            return format!("linux:{}", ticks.checked_add_signed(secs).unwrap());
        }
        let micros = mac_micros(&mine).expect("a known start format");
        let micros = micros.checked_add_signed(secs * 1_000_000).unwrap();
        format!("mac:{}.{:06}", micros / 1_000_000, micros % 1_000_000)
    }

    #[test]
    fn guard_removes_its_directory_even_with_read_only_content() {
        let parent = parent();
        let owned = OwnedTemp::new_in(parent.path(), "own-").unwrap();
        let outer = owned.outer().to_path_buf();
        assert!(fs::read_dir(owned.path()).unwrap().next().is_none());
        let recorded = read_marker(&outer.join(MARKER)).unwrap();
        assert_eq!(Some(&recorded.place), Place::here().as_ref());
        assert_eq!(Some(&recorded.place.boot), boot_id().as_ref());
        assert_eq!(recorded.pid, std::process::id());
        assert_eq!(
            StartIdentity::Identity(recorded.start),
            process_start_identity(recorded.pid)
        );
        let sealed = owned.path().join("sealed");
        fs::create_dir(&sealed).unwrap();
        fs::write(sealed.join("file"), b"x").unwrap();
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o555)).unwrap();
        drop(owned);
        assert!(!outer.exists());
        assert!(fs::read_dir(parent.path()).unwrap().next().is_none());
    }

    #[test]
    fn this_place_is_readable_and_fits_a_marker() {
        let here = Place::here().expect("macOS and Linux say where this is");
        assert_eq!(Place::here().as_ref(), Some(&here));
        for field in [&here.host, &here.pid_ns, &here.boot] {
            assert_eq!(one_line(field), Some(field.as_str()));
        }
        let text = marker(&here, std::process::id(), "mac:1.000000");
        assert!(text.len() as u64 <= MAX_MARKER_BYTES);
        assert_eq!(parse_marker(&text).unwrap().place, here);
    }

    #[test]
    fn one_line_rejects_what_would_break_a_marker() {
        assert_eq!(one_line("a b"), Some("a b"));
        assert_eq!(one_line(""), None);
        assert_eq!(one_line("a\nb"), None);
        assert_eq!(one_line("a\r"), None);
    }

    #[test]
    fn sweep_keeps_a_live_owner_and_removes_only_proven_dead_ones() {
        let parent = parent();
        let live = OwnedTemp::new_in(parent.path(), "own-").unwrap();
        let here = Place::here().unwrap();
        let Place { host, pid_ns, boot } = here.clone();
        let (pid, start) = ended_process();
        let me = std::process::id();
        let make = |name: &str, marker: Option<String>| {
            let dir = parent.path().join(name);
            fs::create_dir_all(dir.join(INSIDE).join("sealed")).unwrap();
            fs::set_permissions(
                dir.join(INSIDE).join("sealed"),
                fs::Permissions::from_mode(0o555),
            )
            .unwrap();
            if let Some(marker) = marker {
                write_marker(&dir, &marker);
            }
            dir
        };
        let full = marker(&here, pid, &start);
        let dead = make("own-dead", Some(full.clone()));
        let other_boot = make(
            "own-boot",
            Some(marker(&place(&host, &pid_ns, "not-this-boot"), me, &start)),
        );
        // This process has the PID, and started long after the record says.
        let reused = make(
            "own-reused",
            Some(marker(&here, me, &my_start_shifted(-3600))),
        );
        let with_pid = |pid: &str| format!("{boot}\n{pid}\n{start}\n{host}\n{pid_ns}\n");
        let kept = [
            make("own-unmarked", None),
            // The form without a host and a namespace, of a dead owner.
            make("own-old", Some(format!("{boot}\n{pid}\n{start}\n"))),
            make(
                "own-old-boot",
                Some(format!("not-this-boot\n{pid}\n{start}\n")),
            ),
            make(
                "own-no-ns",
                Some(format!("{boot}\n{pid}\n{start}\n{host}\n")),
            ),
            // The record of a dead owner from another host or namespace.
            make(
                "own-host",
                Some(marker(&place("not-this-host", &pid_ns, &boot), pid, &start)),
            ),
            make(
                "own-ns",
                Some(marker(&place(&host, "not-this-ns", &boot), pid, &start)),
            ),
            make(
                "own-host-boot",
                Some(marker(
                    &place("not-this-host", &pid_ns, "not-this-boot"),
                    pid,
                    &start,
                )),
            ),
            make(
                "own-ns-boot",
                Some(marker(
                    &place(&host, "not-this-ns", "not-this-boot"),
                    pid,
                    &start,
                )),
            ),
            // This process has the PID; the recorded start is not earlier.
            make("own-odd-start", Some(marker(&here, me, "not-my-start"))),
            make(
                "own-later-start",
                Some(marker(&here, me, &my_start_shifted(3600))),
            ),
            make("own-partial", Some(full.trim_end().to_string())),
            make("own-init", Some(with_pid("1"))),
            make("own-zero", Some(with_pid("0"))),
            make("own-negative", Some(with_pid("-1"))),
            make("own-huge", Some(with_pid("4294967295"))),
            make("own-extra", Some(format!("{full}more\n"))),
            make("own-empty", Some(String::new())),
            make("other-dead", Some(full.clone())),
        ];
        // The records above differ from `full` only in what keeps them.
        assert_eq!(with_pid(&pid.to_string()), full);
        // A marker that is a symbolic link to a valid record proves nothing.
        let linked = make("own-linked", None);
        std::os::unix::fs::symlink(dead.join(MARKER), linked.join(MARKER)).unwrap();
        // A matching name that is a link to a dead owner's directory, and a file.
        std::os::unix::fs::symlink(&dead, parent.path().join("own-alias")).unwrap();
        fs::write(parent.path().join("own-file"), b"").unwrap();

        assert_eq!(sweep(parent.path(), "own-"), 3);
        for gone in [&dead, &other_boot, &reused] {
            assert!(!gone.exists(), "{gone:?}");
        }
        for dir in kept.iter().chain([&linked]) {
            assert!(dir.join(INSIDE).join("sealed").is_dir(), "{dir:?}");
        }
        assert!(live.path().is_dir());
        assert!(parent.path().join("own-file").is_file());
        assert!(fs::symlink_metadata(parent.path().join("own-alias")).is_ok());
        assert_eq!(sweep(parent.path(), "own-"), 0);
        assert_eq!(sweep(&parent.path().join("missing"), "own-"), 0);
        for dir in kept.iter().chain([&linked]) {
            make_writable(dir);
        }
    }

    #[test]
    fn without_a_known_place_nothing_is_proven() {
        let parent = parent();
        let (pid, start) = ended_process();
        let here = Place::here().unwrap();
        let dir = parent.path().join("own-dead");
        fs::create_dir(&dir).unwrap();
        write_marker(&dir, &marker(&here, pid, &start));
        assert!(!owner_is_gone(&dir, None));
        assert!(owner_is_gone(&dir, Some(&here)));
    }

    #[test]
    fn a_pid_is_not_read_where_it_cannot_be_that_of_the_owner() {
        // The record names this live process with an early start: read here
        // it would be a reused PID. From another host or namespace it is not
        // read at all.
        let parent = parent();
        let here = Place::here().unwrap();
        let dir = parent.path().join("own-elsewhere");
        fs::create_dir(&dir).unwrap();
        let me = std::process::id();
        write_marker(&dir, &marker(&here, me, &my_start_shifted(-3600)));
        let other_host = place("not-this-host", &here.pid_ns, &here.boot);
        let other_ns = place(&here.host, "not-this-ns", &here.boot);
        assert!(!owner_is_gone(&dir, Some(&other_host)));
        assert!(!owner_is_gone(&dir, Some(&other_ns)));
        assert!(owner_is_gone(&dir, Some(&here)));
    }

    #[test]
    fn only_the_same_host_and_namespace_can_prove_an_owner_gone() {
        let owner = owner("linux:100");
        let here = place("host", "ns", "boot");
        let gone = StartIdentity::NoProcess;
        assert!(proven_gone(&owner, &here, &gone));
        assert!(!proven_gone(&owner, &place("other", "ns", "boot"), &gone));
        assert!(!proven_gone(&owner, &place("host", "other", "boot"), &gone));
        // Another boot counts only where the host and the namespace match.
        let unread = StartIdentity::Unknown;
        assert!(proven_gone(&owner, &place("host", "ns", "other"), &unread));
        assert!(!proven_gone(
            &owner,
            &place("other", "ns", "other"),
            &unread
        ));
        assert!(!proven_gone(
            &owner,
            &place("host", "other", "other"),
            &unread
        ));
        assert!(!proven_gone(
            &owner,
            &place("other", "other", "other"),
            &gone
        ));
    }

    #[test]
    fn in_the_same_boot_only_no_process_or_a_later_start_is_proof() {
        let owner = owner("linux:100");
        let here = place("host", "ns", "boot");
        let id = |s: &str| StartIdentity::Identity(s.into());
        let zombie = |s: &str| StartIdentity::Zombie(s.into());
        assert!(proven_gone(&owner, &here, &StartIdentity::NoProcess));
        assert!(!proven_gone(&owner, &here, &StartIdentity::Unknown));
        // The owner itself, alive or not yet reaped.
        assert!(!proven_gone(&owner, &here, &id("linux:100")));
        assert!(!proven_gone(&owner, &here, &zombie("linux:100")));
        // A later process has the PID.
        assert!(proven_gone(&owner, &here, &id("linux:101")));
        assert!(proven_gone(&owner, &here, &zombie("linux:101")));
        // A start that differs but is not later, or cannot be compared.
        assert!(!proven_gone(&owner, &here, &id("linux:99")));
        assert!(!proven_gone(&owner, &here, &zombie("linux:99")));
        assert!(!proven_gone(&owner, &here, &id("mac:101.000000")));
        assert!(!proven_gone(&owner, &here, &id("something")));
    }

    #[test]
    fn a_start_is_later_only_when_it_clearly_is() {
        // Linux: ticks since boot.
        assert!(clearly_later("linux:101", "linux:100"));
        assert!(!clearly_later("linux:100", "linux:100"));
        assert!(!clearly_later("linux:99", "linux:100"));
        // macOS: wall-clock time, with a margin.
        assert_eq!(MAC_CLEARLY_LATER_SECS, 2);
        assert!(clearly_later("mac:102.000000", "mac:100.000000"));
        assert!(clearly_later("mac:102.500000", "mac:100.500000"));
        assert!(clearly_later("mac:5000.000000", "mac:100.999999"));
        assert!(!clearly_later("mac:101.999999", "mac:100.000000"));
        assert!(!clearly_later("mac:102.499999", "mac:100.500000"));
        assert!(!clearly_later("mac:100.000000", "mac:100.000000"));
        assert!(!clearly_later("mac:100.000000", "mac:102.000000"));
        assert!(!clearly_later("mac:50.000000", "mac:5000.000000"));
        // Kinds that cannot be compared, and text that is not a start.
        for (now, recorded) in [
            ("linux:200", "mac:100.000000"),
            ("mac:200.000000", "linux:100"),
            ("200", "100"),
            ("linux:200", "linux:"),
            ("linux:+200", "linux:100"),
            ("linux:200", "linux:-1"),
            ("linux:99999999999999999999", "linux:100"),
            ("mac:200", "mac:100.000000"),
            ("mac:200.0", "mac:100.000000"),
            ("mac:200.0000000", "mac:100.000000"),
            ("mac:200.00000a", "mac:100.000000"),
            ("mac:.000000", "mac:100.000000"),
            ("mac:99999999999999999999.000000", "mac:100.000000"),
            ("mac:18446744073710.000000", "mac:100.000000"),
            ("", ""),
        ] {
            assert!(!clearly_later(now, recorded), "{now} {recorded}");
        }
        assert_eq!(mac_micros("mac:18446744073709.551615"), Some(u64::MAX));
        assert_eq!(mac_micros("mac:18446744073709.551616"), None);
        assert_eq!(mac_micros("mac:1.000002"), Some(1_000_002));
        assert_eq!(linux_ticks("linux:0"), Some(0));
    }

    #[test]
    fn a_marker_is_five_complete_lines() {
        let text = "boot\n42\nmac:1.000000\nhost\nns\n";
        assert_eq!(
            parse_marker(text),
            Some(Owner {
                place: place("host", "ns", "boot"),
                pid: 42,
                start: "mac:1.000000".into(),
            })
        );
        assert_eq!(parse_marker(text).unwrap().marker_text(), text);
        for bad in [
            // The older form, and every shorter or longer one.
            "boot\n42\nmac:1.000000\n",
            "boot\n42\nmac:1.000000\nhost\n",
            "boot\n42\nmac:1.000000\nhost\nns\nmore\n",
            "boot\n42\nmac:1.000000\nhost\nns\n\n",
            "boot\n42\nmac:1.000000\nhost\nns",
            // An empty field.
            "\n42\nmac:1.000000\nhost\nns\n",
            "boot\n42\n\nhost\nns\n",
            "boot\n42\nmac:1.000000\n\nns\n",
            "boot\n42\nmac:1.000000\nhost\n\n",
            "boot\n42\nmac:1.000000\nhost\r\nns\n",
            // A PID that is never an owner.
            "boot\n1\nmac:1.000000\nhost\nns\n",
            "boot\n0\nmac:1.000000\nhost\nns\n",
            "boot\n-1\nmac:1.000000\nhost\nns\n",
            "boot\n2147483648\nmac:1.000000\nhost\nns\n",
            "",
        ] {
            assert_eq!(parse_marker(bad), None, "{bad:?}");
        }
        assert!(parse_marker("boot\n2147483647\nmac:1.000000\nhost\nns\n").is_some());
        assert!(parse_marker("boot\n2\nmac:1.000000\nhost\nns\n").is_some());
    }

    #[test]
    fn new_names_fit_the_limit_and_parse_back() {
        let name = new_file_name();
        assert!(name.len() <= 71, "{name}");
        let parsed = parse_file_name(&name).expect("own name parses");
        assert_eq!(parsed.pid, std::process::id());
        assert_eq!(parsed.hash, place_hash(&Place::here().unwrap()));
        let dir = parent();
        let file = file_in(dir.path()).unwrap();
        let made = file
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            made.len() <= 71 && parse_file_name(&made).is_some(),
            "{made}"
        );
        assert!(!made.ends_with(".plist") && !made.ends_with(".service"));
        // the longest legal form is exactly 71 bytes
        let longest = format!(
            "{FILE_PREFIX}{}.4294967295.{}.{}",
            "a".repeat(16),
            "9".repeat(20),
            "x".repeat(12)
        );
        assert_eq!(longest.len(), 71);
    }

    #[test]
    fn names_that_do_not_say_who_made_them_do_not_parse() {
        let ok = testing::live_name("abcDEF123456");
        assert!(parse_file_name(&ok).is_some());
        let bad = [
            ok.replace(".belt-tmp.", ".belt-tmq."),
            format!("{ok}x"),
            ok[..ok.len() - 1].to_string(),
            format!("{ok}.more"),
            ok.replacen(&format!(".{}.", std::process::id()), ".1.", 1),
            ok.replacen(&format!(".{}.", std::process::id()), ".0.", 1),
            ok.replacen(&format!(".{}.", std::process::id()), ".-5.", 1),
            ok.replacen(&format!(".{}.", std::process::id()), ".99999999999.", 1),
            format!("{FILE_PREFIX}XYZ.1.1.abcDEF123456"),
            format!("{FILE_PREFIX}{}.5.1.abcDEF12345!", "a".repeat(16)),
            format!(
                "{FILE_PREFIX}{}.5.{}.abcDEF123456",
                "a".repeat(16),
                "1".repeat(21)
            ),
            ".tmpAbCd12".to_string(),
        ];
        for name in bad {
            assert_eq!(parse_file_name(&name), None, "{name}");
        }
    }

    /// Spec 25.4 (a) acceptance (i)-(iv) for single files and links.
    #[test]
    fn file_sweep_removes_only_what_the_name_proves_abandoned() {
        let dir = parent();
        let put = |name: &str| {
            let path = dir.path().join(name);
            fs::write(&path, b"x").unwrap();
            path
        };
        // (i) a process that is gone
        let gone = put(&testing::dead_name("aaaaaaaaaaa1"));
        // (iii) the PID of a live process, another start
        let reused = put(&testing::name_of(
            std::process::id(),
            &testing::my_start_shifted(-30),
            "aaaaaaaaaaa2",
        ));
        // (ii) a live process
        let live = put(&testing::live_name("aaaaaaaaaaa3"));
        // (iv) another place, a name that does not parse, and a link count of 2
        let elsewhere = put(&testing::name_in_another_place(
            testing::ended_process().0,
            &testing::ended_process().1,
            "aaaaaaaaaaa4",
        ));
        let garbled = put(&format!("{FILE_PREFIX}garbage"));
        let linked_name = testing::dead_name("aaaaaaaaaaa5");
        let linked = put(&linked_name);
        let twin = dir.path().join("twin");
        fs::hard_link(&linked, &twin).unwrap();
        // not named like ours: never looked at, even with a dead owner's look
        let foreign = put("com.other.product.plist");
        let old_style = put(".tmpAbC123");
        // a directory with a matching name, and a link to a file elsewhere
        let dir_named = dir.path().join(testing::dead_name("aaaaaaaaaaa6"));
        fs::create_dir(&dir_named).unwrap();
        let victim = put("victim");
        let link = dir.path().join(testing::dead_name("aaaaaaaaaaa7"));
        std::os::unix::fs::symlink(&victim, &link).unwrap();

        let report = sweep_files(dir.path());
        assert_eq!(report.removed, 3, "{report:?}");
        for removed in [&gone, &reused, &link] {
            assert!(fs::symlink_metadata(removed).is_err(), "{removed:?} stays");
        }
        for kept in [
            &live, &elsewhere, &garbled, &linked, &twin, &foreign, &old_style, &dir_named, &victim,
        ] {
            assert!(fs::symlink_metadata(kept).is_ok(), "{kept:?} was removed");
        }
        assert_eq!(report.kept, 5, "{report:?}");
        assert_eq!(count_files(dir.path()), 5);
    }

    /// Acceptance (i) as it happens: the owner is killed, the file stays, the next sweep clears it.
    #[test]
    fn a_file_of_a_killed_process_is_cleared_by_the_next_sweep() {
        let dir = parent();
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = child.id();
        let StartIdentity::Identity(start) = process_start_identity(pid) else {
            panic!("the child runs");
        };
        let path = dir
            .path()
            .join(testing::name_of(pid, &start, "bbbbbbbbbbb1"));
        fs::write(&path, b"x").unwrap();
        assert_eq!(sweep_files(dir.path()).removed, 0, "the owner runs");
        assert!(path.exists());
        child.kill().unwrap(); // SIGKILL, to a child this test started
        child.wait().unwrap();
        assert_eq!(sweep_files(dir.path()).removed, 1);
        assert!(!path.exists());
    }

    #[test]
    fn a_sweep_of_a_directory_that_is_not_there_is_empty() {
        let dir = parent();
        assert_eq!(
            sweep_files(&dir.path().join("missing")),
            FileSweep::default()
        );
    }

    #[test]
    fn marker_directories_are_swept_by_prefix_with_the_same_proof() {
        let dir = parent();
        let dead = testing::marker_dir(dir.path(), "ci-plan-", true);
        let live = testing::marker_dir(dir.path(), "ci-plan-", false);
        let unmarked = dir.path().join("ci-plan-NOMARK");
        fs::create_dir(&unmarked).unwrap();
        assert_eq!(sweep(dir.path(), "ci-plan-"), 1);
        assert!(!dead.exists() && live.exists() && unmarked.exists());
        assert!(!remove_if_owner_gone(&unmarked) && unmarked.exists());
    }

    /// Spec 25.4 (a) acceptance (vi): outside tests, `src/` creates temporary
    /// files and directories only through this module. A call of
    /// `tempfile::tempfile`/`tempfile_in` is the one other form: the file is
    /// unlinked at creation and has no name to find.
    #[test]
    fn no_tempfile_call_outside_the_rule() {
        fn is_test_file(path: &Path) -> bool {
            let name = path.file_name().unwrap().to_str().unwrap();
            path.components().any(|c| c.as_os_str() == "tests")
                || name == "tests.rs"
                || name.ends_with("_tests.rs")
                || name.starts_with("test_")
        }
        fn walk(dir: &Path, found: &mut Vec<String>) {
            for entry in fs::read_dir(dir).unwrap().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, found);
                    continue;
                }
                if path.extension().is_none_or(|x| x != "rs")
                    || is_test_file(&path)
                    || path.ends_with("src/platform/owned_temp.rs")
                {
                    continue;
                }
                let text = fs::read_to_string(&path).unwrap();
                let lines: Vec<&str> = text.lines().collect();
                let mut in_test_item = false;
                for (i, line) in lines.iter().enumerate() {
                    if line.trim() == "#[cfg(test)]" {
                        let next = lines.get(i + 1).map_or("", |next| next.trim_start());
                        if next.starts_with("mod ") || next.contains(" mod ") {
                            // the rest of the file is the test module
                            break;
                        }
                        // a test-only item: up to its closing brace in column 0
                        in_test_item = true;
                    }
                    if in_test_item {
                        in_test_item = *line != "}";
                        continue;
                    }
                    let code = line.split("//").next().unwrap_or("");
                    let mut rest = code;
                    while let Some(at) = rest.find("tempfile::") {
                        let after = &rest[at + "tempfile::".len()..];
                        if !(after.starts_with("tempfile(") || after.starts_with("tempfile_in(")) {
                            found.push(format!("{}:{}: {}", path.display(), i + 1, line.trim()));
                        }
                        rest = after;
                    }
                }
            }
        }
        let mut found = Vec::new();
        walk(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut found,
        );
        assert!(
            found.is_empty(),
            "tempfile outside the rule:\n{}",
            found.join("\n")
        );
    }
}
