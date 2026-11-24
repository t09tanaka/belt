//! A held `flock` that is released when it is dropped, not when the last
//! copy of its open file description closes.
//!
//! `flock` belongs to the open file description. Every `fork` copies the
//! descriptor table, so a child that a thread of this process forks while the
//! lock is held keeps the description open until its `exec` closes the
//! close-on-exec descriptor. `std::process::Command` forks (instead of
//! `posix_spawn`) whenever it has a `pre_exec` closure or sets `PATH` for a
//! bare program name, and even `posix_spawn` on Linux copies the table for a
//! moment. Closing our descriptor alone then leaves the lock held for that
//! window: on a loaded machine long enough that the next owner sees "busy".
//! An explicit `LOCK_UN` releases the lock on the shared description at once.

use std::fs::File;

/// A file whose (shared or exclusive) `flock` the caller has taken.
#[derive(Debug)]
pub struct HeldLock(File);

impl HeldLock {
    /// Wrap a file whose lock is already held.
    pub fn new(locked: File) -> Self {
        HeldLock(locked)
    }

    /// Hand the still-locked file to an owner that releases it itself (for
    /// example a [`crate::storage::lock::ProcessLock`]).
    pub fn into_file(self) -> File {
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: `this` is never dropped, so the file is moved out once.
        unsafe { std::ptr::read(&this.0) }
    }

    /// Take `file` exclusively, waiting.
    pub fn lock(file: File) -> std::io::Result<Self> {
        file.lock()?;
        Ok(HeldLock(file))
    }

    /// Take `file` exclusively without waiting.
    pub fn try_lock(file: File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock()?;
        Ok(HeldLock(file))
    }
}

/// The lock of a borrowed file, released explicitly when this is dropped,
/// for a caller that keeps using the file (for example as a directory
/// descriptor) while it holds the lock.
#[derive(Debug)]
pub struct HeldBorrow<'a>(&'a File);

impl<'a> HeldBorrow<'a> {
    /// Take `file` exclusively without waiting.
    pub fn try_lock(file: &'a File) -> Result<Self, std::fs::TryLockError> {
        file.try_lock()?;
        Ok(HeldBorrow(file))
    }
}

impl Drop for HeldBorrow<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

impl std::ops::Deref for HeldLock {
    type Target = File;
    fn deref(&self) -> &File {
        &self.0
    }
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        // Drop cannot report an unlock failure; closing stays the fallback.
        let _ = self.0.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_releases_the_lock_while_a_copy_of_the_description_lives() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let path = dir.path().join("x.lock");
        let open = || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let file = open();
        file.try_lock().unwrap();
        // dup shares the open file description, as a forked child does
        let inherited = file.try_clone().unwrap();
        let held = HeldLock::new(file);
        assert!(open().try_lock().is_err());
        drop(held);
        open().try_lock().unwrap();
        drop(inherited);
    }

    #[test]
    fn a_borrowed_hold_releases_on_drop_while_the_file_and_a_copy_live() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let path = dir.path().join("x.lock");
        let open = || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let file = open();
        let inherited = file.try_clone().unwrap();
        let held = HeldBorrow::try_lock(&file).unwrap();
        assert!(open().try_lock().is_err());
        drop(held);
        open().try_lock().unwrap();
        drop((file, inherited));
    }

    #[test]
    fn into_file_keeps_the_lock_for_the_next_owner() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let path = dir.path().join("x.lock");
        let open = || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let file = HeldLock::try_lock(open()).unwrap().into_file();
        assert!(open().try_lock().is_err());
        drop(file);
        open().try_lock().unwrap();
    }
}
