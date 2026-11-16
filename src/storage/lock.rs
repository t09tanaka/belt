//! The coordinator's process lock: one coordinator per state directory.

use super::{Result, StorageError};
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

/// An exclusive advisory lock (`flock`) on a file in the state directory. It
/// is released when this value is dropped or the process dies, so a crashed
/// coordinator never leaves a stale lock behind.
#[derive(Debug)]
pub struct ProcessLock {
    _file: File,
    path: PathBuf,
}

impl ProcessLock {
    /// Take the lock, creating the directory and the file if needed. A second
    /// holder (in this or another process) fails with [`StorageError::Locked`].
    pub fn acquire(dir: &Path, name: &str) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file, path }),
            Err(TryLockError::WouldBlock) => Err(StorageError::Locked(path)),
            Err(TryLockError::Error(e)) => Err(e.into()),
        }
    }

    /// Transfer an already-held private file descriptor without dropping its lock.
    /// The path must identify this exact owned regular file, never another gate.
    pub(crate) fn adopt_private(file: File, path: PathBuf) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let actual = file.metadata()?;
        let named = std::fs::symlink_metadata(&path)?;
        if !actual.is_file()
            || !named.is_file()
            || actual.nlink() != 1
            || actual.uid() != rustix::process::geteuid().as_raw()
            || actual.mode() & 0o077 != 0
            || actual.dev() != named.dev()
            || actual.ino() != named.ino()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "unsafe transferred agent lock",
            )
            .into());
        }
        // Re-locking the same descriptor never releases its existing ownership.
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file, path }),
            Err(TryLockError::WouldBlock) => Err(StorageError::Locked(path)),
            Err(TryLockError::Error(e)) => Err(e.into()),
        }
    }

    /// Check the exact still-named owner inode without opening another file.
    /// Existing coordinator locks may be 0644 under their private state root.
    pub(crate) fn check_current(&self) -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let actual = self._file.metadata()?;
        let named = std::fs::symlink_metadata(&self.path)?;
        if !actual.is_file()
            || !named.is_file()
            || actual.uid() != rustix::process::geteuid().as_raw()
            || actual.nlink() != 1
            || actual.mode() & 0o022 != 0
            || (actual.dev(), actual.ino()) != (named.dev(), named.ino())
        {
            return Err(StorageError::Invalid(
                "coordinator ownership lock changed or unsafe".into(),
            ));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // A child between fork and exec can still hold this open-file
        // description, even though the descriptor is close-on-exec. Closing
        // our descriptor alone would then leave the stopped owner's flock
        // held until that child execs. Release ownership explicitly first.
        // Drop cannot report unlock failures; closing the file remains the
        // fallback and must never panic during another error's unwinding.
        let _ = self._file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_fails_until_the_first_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let first = ProcessLock::acquire(dir.path(), "x.lock").unwrap();
        let err = ProcessLock::acquire(dir.path(), "x.lock").unwrap_err();
        assert!(matches!(err, StorageError::Locked(_)), "{err:?}");
        assert_eq!(err.code(), crate::error::ErrorCode::Conflict);
        // A different name is a different lock.
        ProcessLock::acquire(dir.path(), "y.lock").unwrap();
        assert!(first.path().ends_with("x.lock"));
        drop(first);
        ProcessLock::acquire(dir.path(), "x.lock").unwrap();
    }

    #[test]
    fn dropping_owner_releases_lock_even_with_an_inherited_description() {
        let dir = tempfile::tempdir().unwrap();
        let owner = ProcessLock::acquire(dir.path(), "x.lock").unwrap();
        // dup and fork share the same open-file description. Keep that
        // description alive without creating a child in the test process.
        let inherited = owner._file.try_clone().unwrap();
        assert!(ProcessLock::acquire(dir.path(), "x.lock").is_err());
        drop(owner);
        let next = ProcessLock::acquire(dir.path(), "x.lock").unwrap();
        drop(inherited);
        assert!(ProcessLock::acquire(dir.path(), "x.lock").is_err());
        drop(next);
        ProcessLock::acquire(dir.path(), "x.lock").unwrap();
    }
}
