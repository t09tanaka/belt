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
}
