//! One supervisor per attempt: an exclusive lock held for the supervisor's
//! whole life. The operating system drops it when the process dies, however
//! it dies, so a lock that can be taken proves that the supervisor is gone
//! (recovery relies on it before it stops anything).

use super::error::Result;
use crate::ids::AttemptId;
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct SupervisorLock {
    _file: File,
}

impl Drop for SupervisorLock {
    fn drop(&mut self) {
        // A concurrent fork can retain this open file description until exec.
        // Closing our descriptor alone would then leave the lock held after
        // an acknowledgement has replied that the attempt is released.
        // SAFETY: flock only borrows this live descriptor.
        unsafe { libc::flock(self._file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub fn lock_path(state_dir: &Path, attempt: &AttemptId) -> PathBuf {
    state_dir
        .join("supervisors")
        .join(format!("{attempt}.lock"))
}

impl SupervisorLock {
    /// Take the lock, or `None` if a live supervisor holds it.
    pub fn try_acquire(state_dir: &Path, attempt: &AttemptId) -> Result<Option<SupervisorLock>> {
        let path = lock_path(state_dir, attempt);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        // SAFETY: the fd is valid for the call.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            return Ok(Some(SupervisorLock { _file: file }));
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Ok(None)
        } else {
            Err(err.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_owner_releases_lock_even_with_an_inherited_description() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let a = AttemptId::generate();
        let owner = SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .unwrap();
        // dup and fork share the same open file description, so no child is
        // needed to reproduce a descriptor inherited by a concurrent spawn.
        let inherited = owner._file.try_clone().unwrap();
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_none());
        drop(owner);
        let next = SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .expect("the owner released the lock before the inherited descriptor closed");
        drop(inherited);
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_none());
        drop(next);
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_some());
    }

    #[test]
    fn the_lock_is_exclusive_and_released_on_drop() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let a = AttemptId::generate();
        let first = SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .unwrap();
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_none());
        // Another attempt is independent.
        let b = AttemptId::generate();
        assert!(SupervisorLock::try_acquire(dir.path(), &b)
            .unwrap()
            .is_some());
        drop(first);
        assert!(SupervisorLock::try_acquire(dir.path(), &a)
            .unwrap()
            .is_some());
    }
}
