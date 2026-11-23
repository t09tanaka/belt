//! Temporary directories whose owner can be proven later.
//!
//! A guard removes its directory when it is dropped, but a process that is
//! killed, or that exits while another thread still holds a guard, runs no
//! destructor. So the directory also records who made it: a marker file with
//! the boot, the PID and the start identity of the owning process, and the
//! machine and PID namespace in which those three mean anything. A later
//! process may remove such a directory only when that record proves the owner
//! is gone, and only a process on the same machine and in the same PID
//! namespace can prove that: the parent directory may be shared between hosts
//! or containers. The machine is its identity (the host UUID of macOS, the
//! machine ID of Linux), never its name: two hosts may have the same name. Every doubt keeps the directory. A directory without a
//! readable marker is never touched, and no process is ever signalled here.

use super::{boot_id, process_start_identity, StartIdentity};
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const MARKER: &str = "owner";
const INSIDE: &str = "d";
const MAX_MARKER_BYTES: u64 = 1024;
/// What the line of a marker that names the machine starts with. A marker of
/// an earlier version has the host name there, which says nothing about the
/// machine: such a marker is not a record.
const MACHINE_LINE: &str = "machine:";
/// How much later than the recorded start (in seconds) the process that has
/// the PID now must have started on macOS to count as another process. The
/// start is wall-clock time there, so a small difference proves nothing.
const MAC_CLEARLY_LATER_SECS: u64 = 2;

/// Where a PID, a start identity and a boot ID can be compared: the same
/// facts read on another machine, or in another PID namespace, say nothing
/// about the processes here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Place {
    /// [`machine_id`]: 32 lowercase hex digits.
    machine: String,
    /// Linux: the inode of `/proc/self/ns/pid`. macOS has no PID namespaces.
    pid_ns: String,
    boot: String,
}

impl Place {
    /// `None` when any of the three cannot be read: then nothing is recorded
    /// and nothing is swept.
    fn here() -> Option<Place> {
        Some(Place {
            machine: machine_id()?,
            pid_ns: pid_namespace()?,
            boot: boot_id()?,
        })
    }
}

/// One line of a marker: not empty and on one line.
fn one_line(s: &str) -> Option<&str> {
    (!s.is_empty() && !s.contains(['\n', '\r'])).then_some(s)
}

/// A machine identity as it is recorded: 32 hex digits (dashes of a UUID
/// and the case are dropped), not all zero. Anything else is not one.
fn machine_of(text: &str) -> Option<String> {
    let hex: String = text
        .chars()
        .filter(|c| *c != '-')
        .map(|c| c.to_ascii_lowercase())
        .collect();
    (is_hex(&hex, 32) && hex.bytes().any(|b| b != b'0')).then_some(hex)
}

/// The identity of this machine, which a host name is not: two hosts that
/// share a directory (a home on NFS) may have the same name, and then one
/// would take the other's live files for those of an earlier boot of its
/// own. macOS: the host UUID. Linux: the machine ID (`/etc/machine-id`, else
/// the one of D-Bus). `None` when the system has none that can be read: then
/// this process records nothing and proves nothing.
fn machine_id() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let mut id = [0u8; 16];
        let timeout = libc::timespec {
            tv_sec: 1,
            tv_nsec: 0,
        };
        // SAFETY: `id` is a writable buffer of the 16 bytes of a UUID and
        // `timeout` is a valid timespec.
        let rc = unsafe { libc::gethostuuid(id.as_mut_ptr(), &timeout) };
        (rc == 0).then(|| machine_of(&hex_of(&id))).flatten()
    }
    #[cfg(target_os = "linux")]
    {
        ["/etc/machine-id", "/var/lib/dbus/machine-id"]
            .iter()
            .find_map(|path| machine_of(fs::read_to_string(path).ok()?.trim()))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
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
        let Place {
            machine,
            pid_ns,
            boot,
        } = &self.place;
        format!(
            "{boot}\n{}\n{}\n{MACHINE_LINE}{machine}\n{pid_ns}\n",
            self.pid, self.start
        )
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

fn effective_uid() -> u32 {
    // SAFETY: plain syscall without arguments, which cannot fail.
    unsafe { libc::geteuid() }
}

/// What `lstat` says about an entry, as far as a sweep asks: the link itself,
/// never what it points to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Seen {
    directory: bool,
    regular: bool,
    symlink: bool,
    links: u64,
    uid: u32,
}

impl Seen {
    fn of(meta: &fs::Metadata) -> Seen {
        Seen {
            directory: meta.is_dir(),
            regular: meta.is_file(),
            symlink: meta.file_type().is_symlink(),
            links: meta.nlink(),
            uid: meta.uid(),
        }
    }

    /// A real directory of the user `euid`. Another user's is never touched,
    /// whatever its marker or name says: that user's processes are not ours
    /// to judge.
    fn is_own_directory(&self, euid: u32) -> bool {
        self.directory && self.uid == euid
    }

    /// A regular file of the user `euid` with one link, or a link of that
    /// user (a switch in progress: removed itself, never followed). A file
    /// with a second link has a name elsewhere that somebody may still use.
    fn is_own_file(&self, euid: u32) -> bool {
        ((self.regular && self.links == 1) || self.symlink) && self.uid == euid
    }
}

/// True only with proof: a real directory of this user, a complete marker,
/// and [`proven_gone`]. Every "cannot tell" is false.
fn owner_is_gone(dir: &Path, here: Option<&Place>) -> bool {
    let Ok(meta) = fs::symlink_metadata(dir) else {
        return false;
    };
    if !Seen::of(&meta).is_own_directory(effective_uid()) {
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
/// - Another machine or another PID namespace: never. The owner may be alive
///   where this process cannot see it.
/// - The same machine and namespace but another boot: gone with that boot.
/// - The same boot: gone when no process has the PID, or when the process
///   that has it started clearly later than the owner did (the PID was
///   reused). A start that differs in any other way is not proof.
fn proven_gone(owner: &Owner, here: &Place, now: &StartIdentity) -> bool {
    if owner.place.machine != here.machine || owner.place.pid_ns != here.pid_ns {
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

/// Exactly five complete lines: boot, PID, start, machine, PID namespace. A
/// marker of the older three-line form does not say where its PID is valid,
/// and one that names a host instead of a machine does not say it either: a
/// name is not an identity. Neither is a record.
fn parse_marker(text: &str) -> Option<Owner> {
    let mut lines = text.strip_suffix('\n')?.split('\n');
    let boot = one_line(lines.next()?)?;
    let pid: u32 = lines.next()?.parse().ok().filter(|pid| *pid > 1)?;
    let start = one_line(lines.next()?)?;
    let machine = machine_of(lines.next()?.strip_prefix(MACHINE_LINE)?)?;
    let pid_ns = one_line(lines.next()?)?;
    if lines.next().is_some() || i32::try_from(pid).is_err() {
        return None;
    }
    Some(Owner {
        place: Place {
            machine,
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
// `.belt-tmp.<place hash>.<boot hash>.<pid>.<start identity>.<random>`: the
// same facts as a marker. The machine and the PID namespace are folded into 16
// hex digits and the boot into 8 of its own, so that the name stays at most
// 80 bytes, far below `NAME_MAX`, and a file left by an earlier boot of this
// place can still be told from a file of another place.
// ---------------------------------------------------------------------------

/// Every temporary file belt names itself starts with this. Directories that
/// other products share (LaunchAgents, systemd units) are only looked at
/// through it.
pub const FILE_PREFIX: &str = ".belt-tmp.";
/// A stage: a file whose name `init` writes into its journal, so that a later
/// process finishes the publication. It is not a temporary file: no sweep of
/// temporary files looks at it, and only the recovery of that journal decides
/// about it ([`sweep_stages`]). The rest of the name is that of a temporary
/// file, so an abandoned stage can still be told from one in use.
pub const STAGE_PREFIX: &str = ".init-";
const RANDOM_LEN: usize = 12;
const MAX_PID_DIGITS: usize = 10;
const MAX_START_DIGITS: usize = 20;
const PLACE_HASH_LEN: usize = 16;
const BOOT_HASH_LEN: usize = 8;
const NO_PLACE_HASH: &str = "0000000000000000";
const NO_BOOT_HASH: &str = "00000000";

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The first 16 hex digits of SHA-256 over the machine identity and the PID
/// namespace, separated by NUL (which neither can contain). The boot is not part of it: see
/// [`boot_hash`].
fn place_hash(place: &Place) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(place.machine.as_bytes());
    h.update([0]);
    h.update(place.pid_ns.as_bytes());
    hex_of(&h.finalize()[..PLACE_HASH_LEN / 2])
}

/// The first 8 hex digits of SHA-256 over the boot ID. A name of the same
/// place with another boot hash is of an owner that went with that boot.
fn boot_hash(place: &Place) -> String {
    use sha2::{Digest, Sha256};
    hex_of(&Sha256::digest(place.boot.as_bytes())[..BOOT_HASH_LEN / 2])
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

/// `<place hash>.<boot hash>.<pid>.<start>.` for the process `pid` that is
/// in `place` and started at `start`. When the place (the boot is a part of
/// it) or the start is not known, or the start has no decimal form, the
/// hashes are all zeros and the start is `0`: such a name does not parse, so
/// it is never recognised as anybody's and only its creator removes it. A
/// real place is never paired with a made-up start, which a reader would
/// take for an owner that is gone.
fn owner_part(place: Option<&Place>, start: &StartIdentity, pid: u32) -> String {
    let known = match (place, start) {
        (Some(place), StartIdentity::Identity(start)) => {
            start_digits(start).map(|start| (place_hash(place), boot_hash(place), start))
        }
        _ => None,
    };
    let (place, boot, start) = known.unwrap_or_else(|| {
        (
            NO_PLACE_HASH.to_string(),
            NO_BOOT_HASH.to_string(),
            "0".to_string(),
        )
    });
    format!("{place}.{boot}.{pid}.{start}.")
}

/// [`owner_part`] of this process.
fn own_part() -> String {
    use std::sync::Mutex;
    static CACHE: Mutex<Option<(u32, String)>> = Mutex::new(None);
    let pid = std::process::id();
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((cached, part)) = cache.as_ref() {
        if *cached == pid {
            return part.clone();
        }
    }
    let part = owner_part(Place::here().as_ref(), &process_start_identity(pid), pid);
    *cache = Some((pid, part.clone()));
    part
}

/// `.belt-tmp.<place hash>.<boot hash>.<pid>.<start>.` for this process.
pub fn file_prefix() -> String {
    format!("{FILE_PREFIX}{}", own_part())
}

/// A new temporary file (mode 0600) in `dir`, named by the rule above.
pub fn file_in(dir: impl AsRef<Path>) -> io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(&file_prefix())
        .rand_bytes(RANDOM_LEN)
        .tempfile_in(dir)
}

/// A new stage (mode 0600) in `dir`: see [`STAGE_PREFIX`].
pub fn stage_in(dir: impl AsRef<Path>) -> io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(&format!("{STAGE_PREFIX}{}", own_part()))
        .rand_bytes(RANDOM_LEN)
        .tempfile_in(dir)
}

/// A name for a temporary file that the caller creates itself (with
/// `O_EXCL`, or by `openat`): the prefix of this process and 12 random
/// alphanumerics.
pub fn new_file_name() -> String {
    new_name(FILE_PREFIX)
}

/// A name for a stage that the caller creates itself: see [`STAGE_PREFIX`].
pub fn new_stage_name() -> String {
    new_name(STAGE_PREFIX)
}

fn new_name(kind: &str) -> String {
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
    let mut name = format!("{kind}{}", own_part());
    name.extend(
        random
            .iter()
            .map(|b| ALPHABET[usize::from(*b) % ALPHABET.len()] as char),
    );
    name
}

/// What a temporary file name says: the place, the boot and the owner.
#[derive(Debug, PartialEq, Eq)]
struct FileName {
    hash: String,
    boot: String,
    pid: u32,
    start_digits: String,
}

fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
fn parse_file_name(name: &str) -> Option<FileName> {
    parse_name(name, FILE_PREFIX)
}

/// `name` as `<kind><place hash>.<boot hash>.<pid>.<start>.<random>`.
fn parse_name(name: &str, kind: &str) -> Option<FileName> {
    let rest = name.strip_prefix(kind)?;
    let mut parts = rest.split('.');
    let (hash, boot, pid, start, random) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    if parts.next().is_some()
        || !is_hex(hash, PLACE_HASH_LEN)
        || !is_hex(boot, BOOT_HASH_LEN)
        || pid.len() > MAX_PID_DIGITS
        || start.len() > MAX_START_DIGITS
        || random.len() != RANDOM_LEN
        || !random.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return None;
    }
    let pid: u32 = digits(pid)?.try_into().ok().filter(|pid| *pid > 1)?;
    i32::try_from(pid).ok()?;
    // No process starts at 0: it is what a name without a known owner says.
    digits(start).filter(|start| *start > 0)?;
    Some(FileName {
        hash: hash.to_string(),
        boot: boot.to_string(),
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

/// Whether `name` is a temporary file name of this very process: its place,
/// boot, PID and start, not the PID alone (another process may have had it).
pub fn is_own_file_name(name: &str) -> bool {
    is_own_name(name, &file_prefix())
}

fn is_own_name(name: &str, own_prefix: &str) -> bool {
    name.strip_prefix(own_prefix).is_some_and(|random| {
        random.len() == RANDOM_LEN && random.bytes().all(|b| b.is_ascii_alphanumeric())
    })
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
/// one link, and links of this user (a switch in progress, removed as the
/// link, never followed), are removed; entries that do not start with
/// [`FILE_PREFIX`] are not looked at.
pub fn sweep_files(dir: &Path) -> FileSweep {
    sweep_named(dir, FILE_PREFIX)
}

/// The same for the stages ([`STAGE_PREFIX`]) in `dir`. Only for the owner
/// of the journal that can name a stage there, after its recovery found no
/// stage pending and while it holds the lock that keeps another recovery
/// out: a stage of a gone owner that the journal does not name is nobody's.
/// A stage in the older form (`.init-<ulid>`) does not say who made it and
/// is kept.
pub fn sweep_stages(dir: &Path) -> FileSweep {
    sweep_named(dir, STAGE_PREFIX)
}

fn sweep_named(dir: &Path, kind: &str) -> FileSweep {
    let mut out = FileSweep::default();
    let Ok(entries) = fs::read_dir(dir) else {
        return out;
    };
    let here = Place::here();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|n| n.starts_with(kind)) else {
            continue;
        };
        let path = entry.path();
        if file_owner_is_gone(&path, name, kind, here.as_ref()) && fs::remove_file(&path).is_ok() {
            out.removed += 1;
        } else {
            out.kept += 1;
        }
    }
    out
}

/// True only with proof: a regular file of this user with one link, and a
/// name that [`name_owner_is_gone`].
fn file_owner_is_gone(path: &Path, name: &str, kind: &str, here: Option<&Place>) -> bool {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    if !Seen::of(&meta).is_own_file(effective_uid()) {
        return false;
    }
    name_is_of_gone_owner(name, kind, here)
}

/// Whether `name` is a temporary file name (see [`FILE_PREFIX`]) that parses,
/// was made in this place, and whose process is proven gone. The caller
/// still has to look at the file itself (type, owner, links).
pub fn name_owner_is_gone(name: &str) -> bool {
    name_is_of_gone_owner(name, FILE_PREFIX, Place::here().as_ref())
}

/// The rule of [`proven_gone`], read from a name: another machine or PID
/// namespace (another place hash) proves nothing; the same place with
/// another boot hash is an owner that went with that boot; in the same boot
/// the process is looked at.
fn name_is_of_gone_owner(name: &str, kind: &str, here: Option<&Place>) -> bool {
    let (Some(parsed), Some(here)) = (parse_name(name, kind), here) else {
        return false;
    };
    if parsed.hash != place_hash(here) {
        return false;
    }
    if parsed.boot != boot_hash(here) {
        return true;
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

/// Whether `dir` certainly has no marker file of an [`OwnedTemp`]: one that
/// is there but unreadable, or a directory that cannot be looked into, is
/// not "without". A directory without one says nothing of its owner: only a
/// caller that knows no writer can exist (a lock held since before any
/// create) may remove it.
pub fn is_without_marker(dir: &Path) -> bool {
    matches!(fs::symlink_metadata(dir.join(MARKER)), Err(e) if e.kind() == io::ErrorKind::NotFound)
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
        let here = Place::here().expect("a readable place");
        format!(
            "{FILE_PREFIX}{}.{}.{pid}.{}.{random}",
            place_hash(&here),
            boot_hash(&here),
            start_digits(start).unwrap()
        )
    }

    pub(super) fn other_hex(here: &str) -> String {
        let fill = if here.starts_with('f') { "e" } else { "f" };
        fill.repeat(here.len())
    }

    /// The same, with a place hash that is not this one's.
    pub(crate) fn name_in_another_place(pid: u32, start: &str, random: &str) -> String {
        let here = place_hash(&Place::here().unwrap());
        name_of(pid, start, random).replacen(&here, &other_hex(&here), 1)
    }

    /// The same in this place, with a boot hash that is not this boot's.
    pub(crate) fn name_in_another_boot(pid: u32, start: &str, random: &str) -> String {
        let here = Place::here().unwrap();
        let (place, boot) = (place_hash(&here), boot_hash(&here));
        name_of(pid, start, random).replacen(
            &format!("{place}.{boot}."),
            &format!("{place}.{}.", other_hex(&boot)),
            1,
        )
    }

    /// `name` (of this process, as production code made it) as a process
    /// that is gone would have made it: only the PID and the start differ.
    pub(crate) fn with_ended_owner(name: &str) -> String {
        let (pid, start) = ended_process();
        let own = format!(
            ".{}.{}.",
            std::process::id(),
            start_digits(&this_start()).unwrap()
        );
        assert!(name.contains(&own), "{name} is not of this process");
        name.replacen(
            &own,
            &format!(".{pid}.{}.", start_digits(&start).unwrap()),
            1,
        )
    }

    /// A stage name ([`STAGE_PREFIX`]) with the owner of the temporary file name `name`.
    pub(crate) fn as_stage(name: &str) -> String {
        format!("{STAGE_PREFIX}{}", name.strip_prefix(FILE_PREFIX).unwrap())
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

    fn place(machine: &str, pid_ns: &str, boot: &str) -> Place {
        Place {
            machine: machine.into(),
            pid_ns: pid_ns.into(),
            boot: boot.into(),
        }
    }

    /// A machine identity that is not this machine's.
    fn other_machine() -> String {
        testing::other_hex(&Place::here().unwrap().machine)
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
        assert_eq!(machine_of(&here.machine).as_ref(), Some(&here.machine));
        for field in [&here.machine, &here.pid_ns, &here.boot] {
            assert_eq!(one_line(field), Some(field.as_str()));
        }
        let text = marker(&here, std::process::id(), "mac:1.000000");
        assert!(text.len() as u64 <= MAX_MARKER_BYTES);
        assert_eq!(parse_marker(&text).unwrap().place, here);
    }

    /// The place is the machine, not its name: two hosts with one name (and,
    /// on Linux, the PID namespace number every host starts with) that share
    /// a directory are two places, and neither proves anything about the
    /// other's files, whatever their boots and PIDs say.
    #[test]
    fn two_machines_with_one_name_are_two_places() {
        let here = Place::here().unwrap();
        let twin = place(&other_machine(), &here.pid_ns, "the-boot-of-the-twin");
        assert_ne!(place_hash(&twin), place_hash(&here));
        let (pid, start) = ended_process();
        // a file the twin made: its PID is free here and its boot is not ours
        let name = format!(
            "{FILE_PREFIX}{}.{}.{pid}.{}.abcDEF123456",
            place_hash(&twin),
            boot_hash(&twin),
            start_digits(&start).unwrap()
        );
        assert!(parse_file_name(&name).is_some());
        assert!(!name_is_of_gone_owner(&name, FILE_PREFIX, Some(&here)));
        let parent = parent();
        fs::write(parent.path().join(&name), b"live on the twin").unwrap();
        assert_eq!(
            sweep_files(parent.path()),
            FileSweep {
                removed: 0,
                kept: 1
            }
        );
        // the same for a marker directory
        let dir = parent.path().join("of-the-twin");
        fs::create_dir(&dir).unwrap();
        write_marker(&dir, &marker(&twin, pid, &start));
        assert!(!owner_is_gone(&dir, Some(&here)));
        // and a marker that names a host (an earlier version) is no record
        write_marker(
            &dir,
            &marker(&here, pid, &start)
                .replace(&format!("{MACHINE_LINE}{}", here.machine), "the-host-name"),
        );
        assert!(!owner_is_gone(&dir, Some(&here)));
        write_marker(&dir, &marker(&here, pid, &start));
        assert!(owner_is_gone(&dir, Some(&here)));
    }

    /// Another user's entry is never removed, and neither is a file with a
    /// second link or anything that is not a plain file, a link or (for a
    /// marker) a directory. The examination is a function of what `lstat`
    /// said, so that the rule for another uid, whose file a test cannot make
    /// without being root, is tested with the uid given.
    #[test]
    fn only_the_own_users_plain_entries_are_examined_further() {
        let parent = parent();
        let file = parent.path().join("f");
        fs::write(&file, b"x").unwrap();
        let me = effective_uid();
        let seen = Seen::of(&fs::symlink_metadata(&file).unwrap());
        assert_eq!(
            seen,
            Seen {
                directory: false,
                regular: true,
                symlink: false,
                links: 1,
                uid: me
            }
        );
        assert!(seen.is_own_file(me) && !seen.is_own_directory(me));
        // the same file as another user's, seen by us, and ours seen by another
        for (uid, euid) in [(me + 1, me), (me, me + 1), (0, me + 1), (me + 1, 0)] {
            assert!(!Seen { uid, ..seen }.is_own_file(euid), "{uid} {euid}");
        }
        for links in [0, 2, 3] {
            assert!(!Seen { links, ..seen }.is_own_file(me), "{links}");
        }
        fs::hard_link(&file, parent.path().join("second")).unwrap();
        let linked = Seen::of(&fs::symlink_metadata(&file).unwrap());
        assert_eq!(linked.links, 2);
        assert!(!linked.is_own_file(me));
        // a link is seen as the link, whatever it points to
        let link = parent.path().join("l");
        std::os::unix::fs::symlink(parent.path(), &link).unwrap();
        let seen = Seen::of(&fs::symlink_metadata(&link).unwrap());
        assert!(seen.symlink && !seen.directory && !seen.regular);
        assert!(seen.is_own_file(me) && !seen.is_own_directory(me));
        assert!(!Seen { uid: me + 1, ..seen }.is_own_file(me));
        // a directory
        let seen = Seen::of(&fs::symlink_metadata(parent.path()).unwrap());
        assert!(seen.directory && seen.is_own_directory(me) && !seen.is_own_file(me));
        assert!(!Seen { uid: me + 1, ..seen }.is_own_directory(me));
        assert!(!seen.is_own_directory(me + 1));
        // neither a file, a link nor a directory (a socket, a FIFO)
        let other = Seen {
            directory: false,
            regular: false,
            symlink: false,
            links: 1,
            uid: me,
        };
        assert!(!other.is_own_file(me) && !other.is_own_directory(me));
    }

    #[test]
    fn a_machine_identity_is_32_hex_digits_that_are_not_all_zero() {
        let id = "0123456789abcdef0123456789abcdef";
        assert_eq!(machine_of(id).as_deref(), Some(id));
        // the forms a system gives: upper case, a UUID with dashes
        assert_eq!(
            machine_of("01234567-89AB-CDEF-0123-456789ABCDEF").as_deref(),
            Some(id)
        );
        for bad in [
            "",
            // an image without one: empty, or the placeholder
            "uninitialized",
            "00000000000000000000000000000000",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "0123456789abcdef0123456789abcdeg",
            "0123456789abcdef0123456789abcdef\n",
        ] {
            assert_eq!(machine_of(bad), None, "{bad:?}");
        }
        assert!(machine_of("00000000000000000000000000000001").is_some());
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
        let Place {
            machine,
            pid_ns,
            boot,
        } = here.clone();
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
            Some(marker(&place(&machine, &pid_ns, "not-this-boot"), me, &start)),
        );
        // This process has the PID, and started long after the record says.
        let reused = make(
            "own-reused",
            Some(marker(&here, me, &my_start_shifted(-3600))),
        );
        let with_pid = |pid: &str| format!("{boot}\n{pid}\n{start}\n{MACHINE_LINE}{machine}\n{pid_ns}\n");
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
                Some(format!("{boot}\n{pid}\n{start}\n{MACHINE_LINE}{machine}\n")),
            ),
            // The record of a dead owner from another host or namespace.
            make(
                "own-host",
                Some(marker(&place(&other_machine(), &pid_ns, &boot), pid, &start)),
            ),
            make(
                "own-ns",
                Some(marker(&place(&machine, "not-this-ns", &boot), pid, &start)),
            ),
            make(
                "own-host-boot",
                Some(marker(
                    &place(&other_machine(), &pid_ns, "not-this-boot"),
                    pid,
                    &start,
                )),
            ),
            make(
                "own-ns-boot",
                Some(marker(
                    &place(&machine, "not-this-ns", "not-this-boot"),
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
        let other_host = place(&other_machine(), &here.pid_ns, &here.boot);
        let other_ns = place(&here.machine, "not-this-ns", &here.boot);
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
        let text = "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n";
        assert_eq!(
            parse_marker(text),
            Some(Owner {
                place: place("0123456789abcdef0123456789abcdef", "ns", "boot"),
                pid: 42,
                start: "mac:1.000000".into(),
            })
        );
        assert_eq!(parse_marker(text).unwrap().marker_text(), text);
        for bad in [
            // The older form, and every shorter or longer one.
            "boot\n42\nmac:1.000000\n",
            "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\n",
            "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\nmore\n",
            "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n\n",
            "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns",
            // An empty field.
            "\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n",
            "boot\n42\n\nmachine:0123456789abcdef0123456789abcdef\nns\n",
            "boot\n42\nmac:1.000000\n\nns\n",
            "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\n\n",
            "boot\n42\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\r\nns\n",
            // A host name where the machine belongs (an earlier version),
            // and what is not a machine identity.
            "boot\n42\nmac:1.000000\nhost\nns\n",
            "boot\n42\nmac:1.000000\n0123456789abcdef0123456789abcdef\nns\n",
            "boot\n42\nmac:1.000000\nmachine:host\nns\n",
            "boot\n42\nmac:1.000000\nmachine:00000000000000000000000000000000\nns\n",
            // A PID that is never an owner.
            "boot\n1\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n",
            "boot\n0\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n",
            "boot\n-1\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n",
            "boot\n2147483648\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n",
            "",
        ] {
            assert_eq!(parse_marker(bad), None, "{bad:?}");
        }
        assert!(parse_marker("boot\n2147483647\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n").is_some());
        assert!(parse_marker("boot\n2\nmac:1.000000\nmachine:0123456789abcdef0123456789abcdef\nns\n").is_some());
    }

    #[test]
    fn new_names_fit_the_limit_and_parse_back() {
        let name = new_file_name();
        assert!(name.len() <= 80, "{name}");
        let parsed = parse_file_name(&name).expect("own name parses");
        assert_eq!(parsed.pid, std::process::id());
        assert_eq!(parsed.hash, place_hash(&Place::here().unwrap()));
        assert_eq!(parsed.boot, boot_hash(&Place::here().unwrap()));
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
            made.len() <= 80 && parse_file_name(&made).is_some(),
            "{made}"
        );
        assert!(!made.ends_with(".plist") && !made.ends_with(".service"));
        // the longest legal form is exactly 80 bytes
        let longest = format!(
            "{FILE_PREFIX}{}.{}.2147483647.{}.{}",
            "a".repeat(16),
            "b".repeat(8),
            "1".repeat(20),
            "x".repeat(12)
        );
        assert_eq!(longest.len(), 80);
        assert!(parse_file_name(&longest).is_some());
    }

    /// The place hash is of the host and the PID namespace only; the boot
    /// has its own field, so the same place in another boot is recognised.
    #[test]
    fn the_boot_is_carried_apart_from_the_place() {
        let a = place("host", "ns", "boot-1");
        let b = place("host", "ns", "boot-2");
        assert_eq!(place_hash(&a), place_hash(&b));
        assert_ne!(boot_hash(&a), boot_hash(&b));
        assert_ne!(place_hash(&a), place_hash(&place("other", "ns", "boot-1")));
        assert_ne!(
            place_hash(&a),
            place_hash(&place("host", "other", "boot-1"))
        );
        // the separator keeps "ho"+"stns" apart from "host"+"ns"
        assert_ne!(place_hash(&a), place_hash(&place("ho", "stns", "boot-1")));
        assert_eq!(place_hash(&a).len(), 16);
        assert_eq!(boot_hash(&a).len(), 8);
    }

    /// A file of this place that an earlier boot left (power loss) is gone
    /// with that boot, whatever has its PID now; the same name with another
    /// place hash proves nothing.
    #[test]
    fn a_file_of_an_earlier_boot_of_this_place_is_swept() {
        let dir = parent();
        let me = std::process::id();
        // the PID and start of this live process: only the boot differs
        let earlier = testing::name_in_another_boot(me, &testing::this_start(), "cccccccccccc");
        let elsewhere = {
            let here = place_hash(&Place::here().unwrap());
            let other = if here.starts_with('f') { "e" } else { "f" }.repeat(16);
            earlier.replacen(&here, &other, 1)
        };
        assert_ne!(earlier, elsewhere);
        for name in [&earlier, &elsewhere] {
            fs::write(dir.path().join(name), b"x").unwrap();
        }
        assert!(name_owner_is_gone(&earlier));
        assert!(!name_owner_is_gone(&elsewhere));
        assert_eq!(
            sweep_files(dir.path()),
            FileSweep {
                removed: 1,
                kept: 1
            }
        );
        assert!(!dir.path().join(&earlier).exists() && dir.path().join(&elsewhere).exists());
    }

    /// Without a known place, boot or start the name carries no owner: zeros,
    /// which do not parse, never a real place with a made-up start.
    #[test]
    fn a_process_that_cannot_say_who_it_is_makes_names_of_nobody() {
        let here = place("host", "ns", "boot");
        let known = owner_part(
            Some(&here),
            &StartIdentity::Identity("linux:42".to_string()),
            77,
        );
        assert_eq!(
            known,
            format!("{}.{}.77.42.", place_hash(&here), boot_hash(&here))
        );
        assert!(parse_file_name(&format!("{FILE_PREFIX}{known}abcDEF123456")).is_some());
        let nobody = format!("{NO_PLACE_HASH}.{NO_BOOT_HASH}.77.0.");
        for (place, start) in [
            // a start that has no decimal form
            (Some(&here), StartIdentity::Identity("other:42".to_string())),
            (Some(&here), StartIdentity::Unknown),
            (Some(&here), StartIdentity::NoProcess),
            (Some(&here), StartIdentity::Zombie("linux:42".to_string())),
            // no place: the machine, the namespace or the boot is not readable
            (None, StartIdentity::Identity("linux:42".to_string())),
        ] {
            assert_eq!(owner_part(place, &start, 77), nobody, "{start:?}");
        }
        let name = format!("{FILE_PREFIX}{nobody}abcDEF123456");
        assert_eq!(parse_file_name(&name), None);
        assert!(!name_is_of_gone_owner(&name, FILE_PREFIX, Some(&here)));
        // the creator still knows its own
        assert!(is_own_name(&name, &format!("{FILE_PREFIX}{nobody}")));
    }

    /// A start of 0 is what "not known" is written as: with the place and
    /// boot of this process and a PID that is gone it still proves nothing.
    #[test]
    fn a_start_of_zero_is_nobody() {
        let (pid, start) = testing::ended_process();
        let real = testing::name_of(pid, &start, "abcDEF123456");
        assert!(name_owner_is_gone(&real));
        let digits = start_digits(&start).unwrap();
        let zero = real.replacen(&format!(".{pid}.{digits}."), &format!(".{pid}.0."), 1);
        assert_ne!(real, zero);
        assert_eq!(parse_file_name(&zero), None);
        assert!(!name_owner_is_gone(&zero));
    }

    /// Own means the whole prefix: place, boot, PID and start.
    #[test]
    fn only_a_name_with_the_whole_own_prefix_is_own() {
        let me = std::process::id();
        assert!(is_own_file_name(&new_file_name()));
        assert!(is_own_file_name(&testing::live_name("abcDEF123456")));
        for other in [
            // this PID in another place, in another boot, with another start
            testing::name_in_another_place(me, &testing::this_start(), "abcDEF123456"),
            testing::name_in_another_boot(me, &testing::this_start(), "abcDEF123456"),
            testing::name_of(me, &testing::my_start_shifted(-30), "abcDEF123456"),
            format!("{}x", testing::live_name("abcDEF123456")),
            format!("{}.more", testing::live_name("abcDEF12345")),
            testing::as_stage(&testing::live_name("abcDEF123456")),
        ] {
            assert!(!is_own_file_name(&other), "{other}");
        }
    }

    /// Stages are not temporary files: the file sweep does not look at them,
    /// and the stage sweep removes only those that name a gone owner.
    #[test]
    fn stages_are_swept_only_by_the_stage_sweep() {
        let dir = parent();
        let made = stage_in(dir.path()).unwrap().keep().unwrap().1;
        let made_name = made.file_name().unwrap().to_str().unwrap().to_string();
        assert!(made_name.starts_with(STAGE_PREFIX) && !made_name.starts_with(FILE_PREFIX));
        assert_eq!(
            made_name,
            testing::as_stage(&testing::live_name(&made_name[made_name.len() - 12..]))
        );
        assert!(new_stage_name().starts_with(&made_name[..made_name.len() - 12]));
        assert!(parse_name(&made_name, STAGE_PREFIX).is_some());
        let dead = dir
            .path()
            .join(testing::as_stage(&testing::dead_name("aaaaaaaaaaa1")));
        let old_form = dir.path().join(".init-01JD0000000000000000000000");
        let temporary = dir.path().join(testing::dead_name("aaaaaaaaaaa2"));
        for f in [&dead, &old_form, &temporary] {
            fs::write(f, b"x").unwrap();
        }
        assert_eq!(count_files(dir.path()), 1);
        assert_eq!(
            sweep_files(dir.path()),
            FileSweep {
                removed: 1,
                kept: 0
            }
        );
        assert!(dead.exists() && made.exists() && old_form.exists() && !temporary.exists());
        assert_eq!(
            sweep_stages(dir.path()),
            FileSweep {
                removed: 1,
                kept: 2
            }
        );
        assert!(!dead.exists() && made.exists() && old_form.exists());
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
            format!("{FILE_PREFIX}XYZ.{}.5.1.abcDEF123456", "b".repeat(8)),
            format!("{FILE_PREFIX}{}.5.1.abcDEF12345!", "a".repeat(16)),
            format!(
                "{FILE_PREFIX}{}.{}.5.1.abcDEF12345!",
                "a".repeat(16),
                "b".repeat(8)
            ),
            // the form without a boot field, a boot of the wrong length or alphabet
            format!("{FILE_PREFIX}{}.5.1.abcDEF123456", "a".repeat(16)),
            format!(
                "{FILE_PREFIX}{}.{}.5.1.abcDEF123456",
                "a".repeat(16),
                "b".repeat(7)
            ),
            format!(
                "{FILE_PREFIX}{}.{}.5.1.abcDEF123456",
                "a".repeat(16),
                "B".repeat(8)
            ),
            format!(
                "{FILE_PREFIX}{}.{}.5.{}.abcDEF123456",
                "a".repeat(16),
                "b".repeat(8),
                "1".repeat(21)
            ),
            // a start of 0
            format!(
                "{FILE_PREFIX}{}.{}.5.0.abcDEF123456",
                "a".repeat(16),
                "b".repeat(8)
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
        // "without a marker" is only what is certainly so
        assert!(is_without_marker(&unmarked) && !is_without_marker(&live));
        write_marker(&unmarked, "garbage");
        assert!(!is_without_marker(&unmarked));
        let file = dir.path().join("a-file");
        fs::write(&file, b"x").unwrap();
        assert!(!is_without_marker(&file), "ENOTDIR is not NotFound");
    }

    /// The lines (1-based) of the Rust source `text` that use the `tempfile`
    /// crate outside the rule, test-only items left out.
    ///
    /// - A use is `tempfile` as a path segment (`tempfile::…`, also after a
    ///   leading `::`) or as what a `use`/`extern crate` names (`use tempfile
    ///   as t`). The one form that is allowed is a call of
    ///   `tempfile::tempfile(` or `tempfile::tempfile_in(`.
    /// - A test-only item is what follows a `#[cfg(test)]`: up to the `;`
    ///   that ends it (`mod x;`, a `use`: nothing more is left out, the file
    ///   of the module is looked at as a file) or to the brace that closes
    ///   its block, counted from the first `{`, wherever the item is indented
    ///   and whether or not it is on one line.
    /// - `//` comments are dropped. Strings and block comments are NOT
    ///   understood: a brace or a `;` in one counts like any other (so a
    ///   test-only item may end early or late), and `tempfile::` in a string
    ///   is reported.
    fn tempfile_uses(text: &str) -> Vec<usize> {
        const ATTR: &str = "#[cfg(test)]";
        enum At {
            Code,
            /// After the attribute, before the item's `;` or first `{`.
            TestItem,
            /// Inside the block of a test-only item.
            TestBlock(usize),
        }
        fn is_ident(b: u8) -> bool {
            b.is_ascii_alphanumeric() || b == b'_'
        }
        /// Whether `code` (outside test-only items) uses the crate.
        fn uses(code: &str) -> bool {
            let bytes = code.as_bytes();
            let mut from = 0;
            while let Some(found) = code[from..].find("tempfile") {
                let (start, end) = (from + found, from + found + "tempfile".len());
                from = end;
                let whole = (start == 0 || !is_ident(bytes[start - 1]))
                    && bytes.get(end).is_none_or(|b| !is_ident(*b));
                if !whole {
                    continue;
                }
                let before = code[..start].trim_end();
                let after = code[end..].trim_start();
                // `a::tempfile` is a member of something else; `::tempfile` is the crate
                let member = before.strip_suffix("::").is_some_and(|path| {
                    path.as_bytes()
                        .last()
                        .is_some_and(|b| is_ident(*b) || *b == b'>')
                });
                let before = before.strip_suffix("::").unwrap_or(before).trim_end();
                let named = before.ends_with("use") || before.ends_with("extern crate");
                let path = after.strip_prefix("::").map(str::trim_start);
                let allowed = path
                    .is_some_and(|p| p.starts_with("tempfile(") || p.starts_with("tempfile_in("));
                if !member && !allowed && (path.is_some() || named) {
                    return true;
                }
            }
            false
        }
        let mut at = At::Code;
        let mut found = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.split("//").next().unwrap_or("");
            // the parts of the line that are outside test-only items
            let mut code = String::new();
            let mut rest = line;
            while !rest.is_empty() {
                match at {
                    At::Code => match rest.find(ATTR) {
                        Some(attr) => {
                            code.push_str(&rest[..attr]);
                            code.push(' ');
                            rest = &rest[attr + ATTR.len()..];
                            at = At::TestItem;
                        }
                        None => {
                            code.push_str(rest);
                            rest = "";
                        }
                    },
                    At::TestItem => match rest.find(['{', ';']) {
                        Some(end) => {
                            at = if rest.as_bytes()[end] == b'{' {
                                At::TestBlock(1)
                            } else {
                                At::Code
                            };
                            rest = &rest[end + 1..];
                        }
                        None => rest = "",
                    },
                    At::TestBlock(depth) => match rest.find(['{', '}']) {
                        Some(brace) => {
                            at = match (rest.as_bytes()[brace] == b'{', depth) {
                                (true, depth) => At::TestBlock(depth + 1),
                                (false, 1) => At::Code,
                                (false, depth) => At::TestBlock(depth - 1),
                            };
                            rest = &rest[brace + 1..];
                        }
                        None => rest = "",
                    },
                }
            }
            if uses(&code) {
                found.push(i + 1);
            }
        }
        found
    }

    /// The scanner itself: what it must report and what it must leave out.
    #[test]
    fn the_tempfile_scanner_sees_through_test_only_items() {
        // a `#[cfg(test)] mod x;` does not hide the rest of the file
        let declared = "\
#[cfg(test)]
mod tests;
fn f() { tempfile::tempdir(); }
";
        assert_eq!(tempfile_uses(declared), [3]);
        // an indented or one-line test-only item ends at its own brace
        let indented = "\
impl S {
    #[cfg(test)]
    fn only_in_tests() {
        if true { tempfile::tempdir(); }
    }
    fn real() { tempfile::NamedTempFile::new(); }
}
#[cfg(test)] fn one_line() { tempfile::tempdir(); }
fn after() { tempfile::Builder::new(); }
    #[cfg(test)]
    use tempfile::TempDir;
fn last() -> tempfile::TempDir { todo!() }
";
        assert_eq!(tempfile_uses(indented), [6, 9, 12]);
        // the crate under another name, or named without a path after it
        let renamed = "\
use tempfile as t;
use ::tempfile;
extern crate tempfile;
use tempfile::{self as u, tempdir};
fn f() { ::tempfile::tempdir(); }
";
        assert_eq!(tempfile_uses(renamed), [1, 2, 3, 4, 5]);
        // the block of a test module is left out whole; what follows is not
        let module = "\
fn before() {}
#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    fn f() { let _ = tempfile::tempdir(); }
    mod deeper { fn g() { tempfile::tempdir(); } }
}
fn after() { tempfile::tempdir(); }
";
        assert_eq!(tempfile_uses(module), [9]);
        // the allowed form, comments, and names that only contain the word
        let allowed = "\
fn f() {
    let a = tempfile::tempfile();
    let b = tempfile::tempfile_in(dir);
    let c = ::tempfile::tempfile();
    // tempfile::tempdir() in a comment
    let tempfile = 1; let d = my_tempfile::new(); let e = x.tempfile;
    let f = store::tempfile::make(); let g = tempfile_in(dir);
    let h = tempfile::tempfile_into(dir);
}
";
        assert_eq!(tempfile_uses(allowed), [8]);
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
                for line in tempfile_uses(&text) {
                    found.push(format!(
                        "{}:{line}: {}",
                        path.display(),
                        lines[line - 1].trim()
                    ));
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
