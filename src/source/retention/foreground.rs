//! Bounded foreground Source SH acquisition, only outside actor/admission IO.
use super::{open_permanent_file, Guard, Index, LockedFile, SourceStore};
use anyhow::{ensure, Context, Result};
use std::fs::{File, TryLockError};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Ordinary control replies have a 5s deadline. Leave time to report busy,
// rather than waiting until the client itself loses the response.
const WAIT: Duration = Duration::from_secs(2);
#[derive(Debug, thiserror::Error)]
#[error("captured inputs are temporarily busy; retry the request")]
pub(crate) struct ForegroundBusy;

impl Index {
    pub(super) fn foreground_guard(&self) -> Result<Guard> {
        self.foreground_guard_for(WAIT)
    }
    fn foreground_guard_for(&self, timeout: Duration) -> Result<Guard> {
        self.current()
            .context("check source root for foreground lock")?;
        // Retain this exact candidate inode throughout the wait. Reopening a
        // name after WouldBlock would accept a replaced permanent lock.
        let file = open_permanent_file(&self.root, "mutation.lock")
            .context("open foreground source mutation lock")?;
        let deadline = Instant::now() + timeout;
        #[cfg(test)]
        let mut observed_busy = false;
        loop {
            self.check_foreground_lock(&file)?;
            if Instant::now() >= deadline {
                return Err(ForegroundBusy.into());
            }
            match file.try_lock_shared() {
                Ok(()) => {
                    // Explicit unlock on every error too: a concurrent fork
                    // may retain the open description until its exec completes.
                    let locked = LockedFile(file);
                    self.check_foreground_lock(&locked.0)?;
                    locked.0.sync_all().context("sync foreground source lock")?;
                    self.root
                        .sync_all()
                        .context("sync foreground source parent")?;
                    return Ok(Guard {
                        _file: Arc::new(locked),
                        root: self.path.clone(),
                        index: self.clone(),
                    });
                }
                Err(TryLockError::WouldBlock) => {
                    #[cfg(test)]
                    if !observed_busy {
                        self.foreground_waits
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        observed_busy = true;
                    }
                    std::thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(10)),
                    );
                }
                Err(TryLockError::Error(error)) => return Err(error.into()),
            }
        }
    }
    fn check_foreground_lock(&self, file: &File) -> Result<()> {
        self.current().context("check foreground source root")?;
        let opened = file.metadata()?;
        ensure!(
            opened.is_file()
                && opened.nlink() == 1
                && opened.uid() == rustix::process::geteuid().as_raw()
                && opened.mode() & 0o077 == 0,
            "unsafe foreground source lock"
        );
        let named = rustix::fs::statat(
            &*self.root,
            "mutation.lock",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )
        .context("stat foreground source mutation lock")?;
        // st_dev differs between macOS and Linux.
        #[allow(clippy::unnecessary_cast)]
        let named_device = named.st_dev as u64;
        ensure!(
            (opened.dev(), opened.ino()) == (named_device, named.st_ino),
            "foreground source lock changed"
        );
        Ok(())
    }
}
impl SourceStore {
    /// Caller must be in blocking input/preflight work without an actor or
    /// admission lock. Best-effort/actor-sensitive callers keep mutation_guard.
    pub(crate) fn foreground_guard(&self) -> Result<Guard> {
        self.retention.foreground_guard()
    }
    #[cfg(test)]
    pub(crate) fn foreground_wait_count(&self) -> usize {
        self.retention
            .foreground_waits
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    fn store() -> (tempfile::TempDir, SourceStore) {
        let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
        let store = SourceStore::open(root.path()).unwrap();
        (root, store)
    }
    fn entered(store: &SourceStore) {
        let until = Instant::now() + Duration::from_secs(1);
        while store.foreground_wait_count() == 0 {
            assert!(
                Instant::now() < until,
                "foreground did not reach real lock contention"
            );
            std::thread::yield_now();
        }
    }
    #[test]
    fn foreground_wait_retains_shared_guard_after_actual_exclusive_drain() {
        let (_root, store) = store();
        let exclusive = store.retention.guard(true).unwrap();
        assert!(store.mutation_guard().is_err()); // Existing try API is unchanged.
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| store.foreground_guard());
            entered(&store);
            drop(exclusive);
            let shared = waiting.join().unwrap().unwrap();
            assert!(store.retention.guard(true).is_err());
            let clone = shared.clone();
            drop(shared);
            assert!(store.retention.guard(true).is_err());
            drop(clone);
            assert!(store.retention.guard(true).is_ok());
        });
        let (_opened, shared) =
            SourceStore::open_foreground(store.root().parent().unwrap()).unwrap();
        assert!(store.retention.guard(true).is_err());
        drop(shared);
        assert!(store.retention.guard(true).is_ok());
    }
    #[test]
    fn foreground_wait_expiry_is_typed_and_never_bypasses_exclusion() {
        let (_root, store) = store();
        let _exclusive = store.retention.guard(true).unwrap();
        let started = Instant::now();
        let error = store
            .retention
            .foreground_guard_for(Duration::from_millis(30))
            .unwrap_err();
        assert!(error.is::<ForegroundBusy>());
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert_eq!(
            crate::source::classify::api_error_of(&error).code,
            crate::error::ErrorCode::SourceBusy
        );
        assert!(store.mutation_guard().is_err());
    }
    #[test]
    fn foreground_wait_rejects_replaced_permanent_lock_instead_of_reopening() {
        let (_root, store) = store();
        let _exclusive = store.retention.guard(true).unwrap();
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| store.foreground_guard());
            entered(&store);
            let lock = store.root().join("mutation.lock");
            let retained = store.root().join("old-mutation.lock");
            std::fs::rename(&lock, &retained).unwrap();
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&lock)
                .unwrap();
            drop(file);
            let error = waiting.join().unwrap().unwrap_err();
            assert!(!error.is::<ForegroundBusy>());
            assert!(format!("{error:#}").contains("source lock changed"));
            std::fs::remove_file(&lock).unwrap();
            std::fs::rename(retained, lock).unwrap();
        });
    }
    #[test]
    fn foreground_wait_rejects_root_and_mode_drift() {
        for replace_root in [true, false] {
            let (root, store) = store();
            let _exclusive = store.retention.guard(true).unwrap();
            std::thread::scope(|scope| {
                let waiting = scope.spawn(|| store.foreground_guard());
                entered(&store);
                if replace_root {
                    std::fs::rename(store.root(), root.path().join("old-sources")).unwrap();
                    std::fs::create_dir(store.root()).unwrap();
                } else {
                    std::fs::set_permissions(
                        store.root().join("mutation.lock"),
                        std::fs::Permissions::from_mode(0o644),
                    )
                    .unwrap();
                }
                let error = waiting.join().unwrap().unwrap_err();
                assert!(!error.is::<ForegroundBusy>());
                if replace_root {
                    std::fs::remove_dir(store.root()).unwrap();
                    std::fs::rename(root.path().join("old-sources"), store.root()).unwrap();
                } else {
                    std::fs::set_permissions(
                        store.root().join("mutation.lock"),
                        std::fs::Permissions::from_mode(0o600),
                    )
                    .unwrap();
                }
            });
        }
    }
}
