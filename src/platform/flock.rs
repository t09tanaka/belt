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
}
