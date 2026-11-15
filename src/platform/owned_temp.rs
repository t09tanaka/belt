//! Temporary directories whose owner can be proven later.
//!
//! A guard removes its directory when it is dropped, but a process that is
//! killed, or that exits while another thread still holds a guard, runs no
//! destructor. So the directory also records who made it: a marker file with
//! the boot, the PID and the start identity of the owning process. A later
//! process may remove such a directory only when that record proves the owner
//! is gone. A directory without a readable marker is never touched, and no
//! process is ever signalled here.

use super::{boot_id, process_start_identity, StartIdentity};
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const MARKER: &str = "owner";
const INSIDE: &str = "d";
const MAX_MARKER_BYTES: u64 = 512;

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
        // Without both facts nothing can be proven later: no marker, and the
        // directory is only ever removed by this guard.
        if let (Some(boot), StartIdentity::Identity(start)) =
            (boot_id(), process_start_identity(pid))
        {
            let mut marker = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(dir.path().join(MARKER))?;
            // The last newline is what says the record is complete.
            marker.write_all(format!("{boot}\n{pid}\n{start}\n").as_bytes())?;
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
    let boot = boot_id();
    let mut removed = 0;
    for entry in entries.flatten() {
        let named = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(prefix));
        let path = entry.path();
        if named && owner_is_gone(&path, boot.as_deref()) {
            make_writable(&path);
            if fs::remove_dir_all(&path).is_ok() {
                removed += 1;
            }
        }
    }
    removed
}

/// True only with proof: a real directory of this user, a complete marker,
/// and either another boot or a definite answer that the recorded process no
/// longer has its PID. Every "cannot tell" is false.
fn owner_is_gone(dir: &Path, boot_now: Option<&str>) -> bool {
    let Ok(meta) = fs::symlink_metadata(dir) else {
        return false;
    };
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } {
        return false;
    }
    let (Some((boot, pid, start)), Some(boot_now)) = (read_marker(&dir.join(MARKER)), boot_now)
    else {
        return false;
    };
    if boot != boot_now {
        return true;
    }
    match process_start_identity(pid) {
        StartIdentity::NoProcess => true,
        StartIdentity::Identity(now) => now != start,
        // A zombie that still is the owner is kept; so is an unreadable PID.
        StartIdentity::Zombie(now) => now != start,
        StartIdentity::Unknown => false,
    }
}

fn read_marker(path: &Path) -> Option<(String, u32, String)> {
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

fn parse_marker(text: &str) -> Option<(String, u32, String)> {
    let mut lines = text.strip_suffix('\n')?.split('\n');
    let boot = lines.next().filter(|boot| !boot.is_empty())?;
    let pid: u32 = lines.next()?.parse().ok().filter(|pid| *pid > 1)?;
    let start = lines.next().filter(|start| !start.is_empty())?;
    if lines.next().is_some() || i32::try_from(pid).is_err() {
        return None;
    }
    Some((boot.to_string(), pid, start.to_string()))
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

    #[test]
    fn guard_removes_its_directory_even_with_read_only_content() {
        let parent = parent();
        let owned = OwnedTemp::new_in(parent.path(), "own-").unwrap();
        let outer = owned.outer().to_path_buf();
        assert!(fs::read_dir(owned.path()).unwrap().next().is_none());
        let (boot, pid, start) = read_marker(&outer.join(MARKER)).unwrap();
        assert_eq!(Some(boot), boot_id());
        assert_eq!(pid, std::process::id());
        assert_eq!(StartIdentity::Identity(start), process_start_identity(pid));
        let sealed = owned.path().join("sealed");
        fs::create_dir(&sealed).unwrap();
        fs::write(sealed.join("file"), b"x").unwrap();
        fs::set_permissions(&sealed, fs::Permissions::from_mode(0o555)).unwrap();
        drop(owned);
        assert!(!outer.exists());
        assert!(fs::read_dir(parent.path()).unwrap().next().is_none());
    }

    #[test]
    fn sweep_keeps_a_live_owner_and_removes_only_proven_dead_ones() {
        let parent = parent();
        let live = OwnedTemp::new_in(parent.path(), "own-").unwrap();
        let boot = boot_id().unwrap();
        let (pid, start) = ended_process();
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
        let dead = make("own-dead", Some(format!("{boot}\n{pid}\n{start}\n")));
        let other_boot = make(
            "own-boot",
            Some(format!("not-this-boot\n{}\n{start}\n", std::process::id())),
        );
        let me = std::process::id();
        let reused = make("own-reused", Some(format!("{boot}\n{me}\nnot-my-start\n")));
        let kept = [
            make("own-unmarked", None),
            make("own-partial", Some(format!("{boot}\n{pid}\n{start}"))),
            make("own-init", Some(format!("{boot}\n1\n{start}\n"))),
            make("own-zero", Some(format!("{boot}\n0\n{start}\n"))),
            make("own-negative", Some(format!("{boot}\n-1\n{start}\n"))),
            make("own-huge", Some(format!("{boot}\n4294967295\n{start}\n"))),
            make("own-extra", Some(format!("{boot}\n{pid}\n{start}\nmore\n"))),
            make("own-empty", Some(String::new())),
            make("other-dead", Some(format!("{boot}\n{pid}\n{start}\n"))),
        ];
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
    fn without_a_known_boot_nothing_is_proven() {
        let parent = parent();
        let (pid, start) = ended_process();
        let dir = parent.path().join("own-dead");
        fs::create_dir(&dir).unwrap();
        write_marker(&dir, &format!("boot\n{pid}\n{start}\n"));
        assert!(!owner_is_gone(&dir, None));
        assert!(owner_is_gone(&dir, Some("boot")));
    }
}
