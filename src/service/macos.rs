//! macOS service foundations. No public worker installation is wired here yet.
//!
//! `Plan` accepts argv from a runtime bootstrap builder; never put credentials in
//! argv/environment. Supply private configuration file references instead. This
//! module does not equate launchd registration with worker readiness or drain.
//! Lifecycle leases coordinate cooperating writers. Same-UID/root actors and
//! the service root are trusted; loaded observations and path bootout are not
//! an atomic identity-conditioned removal and do not protect against external
//! privileged replacement between the last observation and bootout.
pub mod policy;
use anyhow::Context;
use policy::Plan;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Weak};

/// Separate from launchd operations: implementations must acknowledge actual
/// worker state. Failure must remain an error, never a simulated success.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerRequest {
    Drain,
    Resume,
    Stop { cancel_running: bool },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerIdentity {
    pub cluster_id: crate::ids::ClusterId,
    pub worker_id: crate::ids::WorkerId,
}
/// Acknowledgement must be authenticated and bound to this expected identity.
/// Stop acknowledges the requested cancellation/preservation contract, not just
/// receipt of a packet. Unavailable or unverified workers must return an error.
pub trait WorkerControl {
    fn request(&mut self, identity: &WorkerIdentity, request: WorkerRequest) -> anyhow::Result<()>;
}
/// A backend must distinguish proven rejection from an uncertain outcome.
#[derive(Debug, thiserror::Error)]
pub enum LaunchError {
    #[error("launchctl not applied: {0}")]
    NotApplied(String),
    #[error("launchctl outcome unknown: {0}")]
    OutcomeUnknown(String),
}
#[derive(Debug, PartialEq, Eq)]
pub enum LoadedState {
    Absent,
    Match,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunningIdentity {
    pub pid: u32,
    pub start: String,
    pub boot: String,
}
impl RunningIdentity {
    /// Read-only kernel observation. Zero, init, zombies and unknowns are not owners.
    pub fn observe(pid: u32) -> anyhow::Result<Self> {
        anyhow::ensure!(
            pid > 1 && i32::try_from(pid).is_ok(),
            "invalid resident pid"
        );
        let start = match crate::platform::process_start_identity(pid) {
            crate::platform::StartIdentity::Identity(start) => start,
            _ => anyhow::bail!("resident process identity unknown"),
        };
        let boot =
            crate::platform::boot_id().ok_or_else(|| anyhow::anyhow!("boot identity unknown"))?;
        Ok(Self { pid, start, boot })
    }
    pub fn verify_live(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            Self::observe(self.pid)? == *self,
            "resident process changed or stopped"
        );
        Ok(())
    }
}
pub trait Launcher {
    /// The service manager this launcher drives. Plans, the managed file's
    /// place and the command words follow it.
    fn backend(&self) -> policy::Backend {
        policy::Backend::Launchd
    }
    fn running_identity(&mut self, _: &Plan, _: &Receipt) -> Result<RunningIdentity, LaunchError> {
        Err(LaunchError::NotApplied(
            "running service identity unavailable".into(),
        ))
    }
    fn loaded_state(&mut self, plan: &Plan, receipt: &Receipt) -> LoadedState {
        if self.verify_loaded(plan, receipt).is_ok() {
            LoadedState::Match
        } else {
            LoadedState::Unknown
        }
    }
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError>;
    /// Independently and freshly verify the loaded domain/label and executable,
    /// argv/config reference against this exact plan and managed file. A plist
    /// receipt alone is insufficient. Must reject missing/foreign/unknown jobs.
    fn verify_loaded(&mut self, plan: &Plan, receipt: &Receipt) -> Result<(), LaunchError>;
}
/// A borrowed launcher drives the same service manager.
impl<L: Launcher + ?Sized> Launcher for &mut L {
    fn backend(&self) -> policy::Backend {
        (**self).backend()
    }
    fn running_identity(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
    ) -> Result<RunningIdentity, LaunchError> {
        (**self).running_identity(plan, receipt)
    }
    fn loaded_state(&mut self, plan: &Plan, receipt: &Receipt) -> LoadedState {
        (**self).loaded_state(plan, receipt)
    }
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError> {
        (**self).launchctl(arguments)
    }
    fn verify_loaded(&mut self, plan: &Plan, receipt: &Receipt) -> Result<(), LaunchError> {
        (**self).verify_loaded(plan, receipt)
    }
}
pub(crate) mod native;
pub mod observe;
pub use native::{
    Account, AccountLookup, CommandOutput, CommandRunner, NativeAccounts, NativeLauncher,
    NativeRunner,
};
/// Evidence returned by the store; persist only in caller-owned private state.
/// Never reconstruct or adopt evidence from an arbitrary existing plist.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Receipt {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    digest: [u8; 32],
}
impl Receipt {
    /// A system definition independently checked against the expected plan and
    /// the root-owned file on disk. It remains under administrator ownership.
    pub(super) fn system(path: PathBuf, metadata: &std::fs::Metadata, digest: [u8; 32]) -> Self {
        Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            uid: metadata.uid(),
            digest,
        }
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}
pub trait ManagedFiles {
    type LifecycleLease;
    /// Hold across a complete cooperative install/start/stop/remove operation.
    fn lifecycle_lock(&mut self) -> anyhow::Result<Self::LifecycleLease>;
    fn install(&mut self, plan: &Plan) -> Result<Receipt, InstallStartError>;
    fn verify(&mut self, receipt: &Receipt) -> anyhow::Result<()>;
    fn remove(&mut self, receipt: &Receipt) -> anyhow::Result<()>;
}
/// The filesystem root is explicit. It must exist, be owned by this effective
/// user, and not be writable by other users. No home/system path is inferred.
/// In system mode this is the administrator file owner, independently of the
/// non-root execution identity in the plan. Permission failures propagate;
/// callers must report required administrator action rather than invoke sudo.
pub struct DirectoryFiles {
    root: PathBuf,
    directory: File,
    lease: RefCell<Weak<DirectoryLock>>,
}
impl DirectoryFiles {
    pub fn open(root: &Path) -> anyhow::Result<Self> {
        anyhow::ensure!(root.is_absolute(), "service root must be absolute");
        let mut prefix = PathBuf::new();
        for c in root.components() {
            anyhow::ensure!(
                matches!(c, Component::RootDir | Component::Normal(_)),
                "service root traversal"
            );
            prefix.push(c);
            anyhow::ensure!(
                !std::fs::symlink_metadata(&prefix)?.file_type().is_symlink(),
                "symlinked service root"
            );
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
            .open(root)?;
        let meta = directory.metadata()?;
        anyhow::ensure!(
            meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o022 == 0,
            "unsafe service root owner/permissions"
        );
        Ok(Self {
            root: root.into(),
            directory,
            lease: RefCell::new(Weak::new()),
        })
    }
    fn lock(&self) -> anyhow::Result<Arc<DirectoryLock>> {
        use std::os::fd::AsRawFd;
        let file = self.directory.try_clone()?;
        // A root can be renamed after construction; do not operate through a
        // substituted pathname even when the original directory is still open.
        let current = std::fs::symlink_metadata(&self.root)?;
        let held = file.metadata()?;
        anyhow::ensure!(
            current.dev() == held.dev()
                && current.ino() == held.ino()
                && current.is_dir()
                && current.mode() & 0o022 == 0
                && current.uid() == unsafe { libc::geteuid() },
            "service root replaced"
        );
        if let Some(lease) = self.lease.borrow().upgrade() {
            return Ok(lease);
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let lease = Arc::new(DirectoryLock(file));
        *self.lease.borrow_mut() = Arc::downgrade(&lease);
        Ok(lease)
    }
    /// Read-only snapshot for health/bootstrap; never wait on a writer's lifecycle lease.
    pub(crate) fn verify_read_only(&self, receipt: &Receipt) -> anyhow::Result<()> {
        let held = self.directory.metadata()?;
        let current = std::fs::symlink_metadata(&self.root)?;
        anyhow::ensure!(
            held.dev() == current.dev()
                && held.ino() == current.ino()
                && current.uid() == unsafe { libc::geteuid() }
                && current.mode() & 0o022 == 0,
            "service root changed"
        );
        anyhow::ensure!(
            self.read(receipt.path())?.0 == *receipt,
            "managed service identity/digest changed"
        );
        Ok(())
    }
    fn read(&self, path: &Path) -> anyhow::Result<(Receipt, Vec<u8>)> {
        anyhow::ensure!(
            path.parent() == Some(self.root.as_path()),
            "receipt outside service root"
        );
        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let m = f.metadata()?;
        anyhow::ensure!(
            m.is_file()
                && m.nlink() == 1
                && m.uid() == unsafe { libc::geteuid() }
                && m.mode() & 0o077 == 0
                && m.len() <= 1024 * 1024,
            "unsafe managed service file"
        );
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        let digest = Sha256::digest(&bytes).into();
        Ok((
            Receipt {
                path: path.into(),
                device: m.dev(),
                inode: m.ino(),
                uid: m.uid(),
                digest,
            },
            bytes,
        ))
    }
}
impl DirectoryFiles {
    /// `before_publish` may record the receipt, not the path of the staged
    /// file: that is a temporary file, which any later start may remove once
    /// this process is gone (spec 25.4 (a)).
    pub(crate) fn install_journaled(
        &mut self,
        plan: &Plan,
        before_publish: impl FnOnce(&Receipt, &Path) -> anyhow::Result<()>,
    ) -> Result<Receipt, InstallStartError> {
        self.install_journaled_with_sync(plan, before_publish, |path| File::open(path)?.sync_all())
    }
    /// The same, for a caller whose journal names the staged file so that a
    /// later process finishes the publication (`init`): the file is a stage
    /// ([`crate::platform::owned_temp::STAGE_PREFIX`]), which no sweep of
    /// temporary files touches.
    pub(crate) fn install_staged(
        &mut self,
        plan: &Plan,
        before_publish: impl FnOnce(&Receipt, &Path) -> anyhow::Result<()>,
    ) -> Result<Receipt, InstallStartError> {
        self.install_prepared(
            plan,
            true,
            File::sync_all,
            |_, _| Ok(()),
            |receipt, stage| {
                File::open(
                    stage
                        .parent()
                        .ok_or_else(|| anyhow::anyhow!("stage parent missing"))?,
                )?
                .sync_all()?;
                before_publish(receipt, stage)
            },
        )
    }
    pub(crate) fn install_journaled_with_sync(
        &mut self,
        plan: &Plan,
        before_publish: impl FnOnce(&Receipt, &Path) -> anyhow::Result<()>,
        sync_stage_directory: impl FnOnce(&Path) -> io::Result<()>,
    ) -> Result<Receipt, InstallStartError> {
        self.install_prepared(
            plan,
            false,
            File::sync_all,
            |_, _| Ok(()),
            |receipt, stage| {
                sync_stage_directory(
                    stage
                        .parent()
                        .ok_or_else(|| anyhow::anyhow!("stage parent missing"))?,
                )?;
                before_publish(receipt, stage)
            },
        )
    }
    fn install_with(
        &mut self,
        plan: &Plan,
        sync: impl FnOnce(&File) -> io::Result<()>,
        rollback: impl FnOnce(&Self, &Receipt) -> anyhow::Result<()>,
    ) -> Result<Receipt, InstallStartError> {
        self.install_prepared(plan, false, sync, rollback, |_, _| Ok(()))
    }
    fn install_prepared(
        &mut self,
        plan: &Plan,
        journaled_stage: bool,
        sync: impl FnOnce(&File) -> io::Result<()>,
        rollback: impl FnOnce(&Self, &Receipt) -> anyhow::Result<()>,
        before_publish: impl FnOnce(&Receipt, &Path) -> anyhow::Result<()>,
    ) -> Result<Receipt, InstallStartError> {
        let _lock = self.lock().map_err(InstallStartError::without_receipt)?;
        let (tmp, receipt) = (|| -> anyhow::Result<_> {
            let target = self.root.join(plan.filename());
            // Never claim or overwrite a pre-existing file, even if bytes match.
            let mut tmp = if journaled_stage {
                crate::platform::owned_temp::stage_in(&self.root)?
            } else {
                crate::platform::owned_temp::file_in(&self.root)?
            };
            tmp.as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))?;
            tmp.write_all(plan.plist().as_bytes())?;
            tmp.as_file().sync_all()?;
            // Construct evidence before publishing, so post-publication errors can
            // be rolled back only when the published inode is still ours.
            let m = tmp.as_file().metadata()?;
            let receipt = Receipt {
                path: target.clone(),
                device: m.dev(),
                inode: m.ino(),
                uid: m.uid(),
                digest: Sha256::digest(plan.plist().as_bytes()).into(),
            };
            Ok((tmp, receipt))
        })()
        .map_err(InstallStartError::without_receipt)?;
        // A publication error may be ambiguous; preserve the prepared inode and
        // digest even if verification/cleanup subsequently cannot run.
        if let Err(source) = before_publish(&receipt, tmp.path()) {
            // The callback may have durably renamed its journal before a later
            // fsync failed. Retain the exact staged inode on every callback error.
            tmp.keep().map_err(|error| InstallStartError {
                receipt: Some(receipt.clone()),
                source: error.into(),
            })?;
            return Err(InstallStartError {
                receipt: Some(receipt),
                source,
            });
        }
        if let Err(source) = rustix::fs::renameat_with(
            rustix::fs::CWD,
            tmp.path(),
            rustix::fs::CWD,
            receipt.path(),
            rustix::fs::RenameFlags::NOREPLACE,
        ) {
            // A committed journal can already reference this exact staged inode.
            tmp.keep().map_err(|error| InstallStartError {
                receipt: Some(receipt.clone()),
                source: error.into(),
            })?;
            return Err(InstallStartError {
                receipt: Some(receipt),
                source: std::io::Error::from(source).into(),
            });
        }
        // Atomic rename leaves no hardlink/nlink=2 power-cut window. Drop only
        // attempts cleanup of the old staging name, never the published target.
        drop(tmp);
        if let Err(error) = sync(&self.directory) {
            return match rollback(self, &receipt) {
                Ok(()) => Err(InstallStartError::without_receipt(error.into())),
                Err(cleanup) => Err(InstallStartError {
                    receipt: Some(receipt),
                    source: anyhow::anyhow!(
                        "directory sync failed: {error}; rollback failed: {cleanup}"
                    ),
                }),
            };
        }
        Ok(receipt)
    }
}

pub struct DirectoryLock(File);
impl Drop for DirectoryLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
impl ManagedFiles for DirectoryFiles {
    type LifecycleLease = Arc<DirectoryLock>;
    fn lifecycle_lock(&mut self) -> anyhow::Result<Self::LifecycleLease> {
        self.lock()
    }
    fn install(&mut self, plan: &Plan) -> Result<Receipt, InstallStartError> {
        self.install_with(plan, File::sync_all, |files, receipt| {
            anyhow::ensure!(
                files.read(receipt.path())?.0 == *receipt,
                "rollback identity changed"
            );
            std::fs::remove_file(receipt.path())?;
            Ok(())
        })
    }
    fn verify(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        let _lock = self.lock()?;
        anyhow::ensure!(
            self.read(&receipt.path)?.0 == *receipt,
            "managed service identity/digest changed"
        );
        Ok(())
    }
    fn remove(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        let _lock = self.lock()?;
        anyhow::ensure!(
            self.read(&receipt.path)?.0 == *receipt,
            "refusing removal of changed service file"
        );
        std::fs::remove_file(&receipt.path)?;
        self.directory.sync_all()?;
        Ok(())
    }
}

/// Failed composition retains ownership evidence whenever a file remains.
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct InstallStartError {
    pub receipt: Option<Receipt>,
    #[source]
    source: anyhow::Error,
}
impl InstallStartError {
    pub(super) fn without_receipt(source: anyhow::Error) -> Self {
        Self {
            receipt: None,
            source,
        }
    }
}
pub struct Manager<F, L, W> {
    pub identity: WorkerIdentity,
    pub files: F,
    pub launcher: L,
    pub worker: W,
}
impl<F: ManagedFiles, L: Launcher, W: WorkerControl> Manager<F, L, W> {
    pub fn install(&mut self, plan: &Plan) -> Result<Receipt, InstallStartError> {
        let _lease = self
            .files
            .lifecycle_lock()
            .map_err(InstallStartError::without_receipt)?;
        self.files.install(plan)
    }
    /// Idempotence requires explicit ownership evidence, never file adoption.
    pub fn ensure_installed(&mut self, plan: &Plan, receipt: &Receipt) -> anyhow::Result<()> {
        let _lease = self.files.lifecycle_lock()?;
        self.files.verify(receipt)?;
        anyhow::ensure!(
            receipt.path.file_name().and_then(|n| n.to_str()) == Some(plan.filename().as_str())
                && receipt.digest == <[u8; 32]>::from(Sha256::digest(plan.plist().as_bytes())),
            "installed plan differs"
        );
        Ok(())
    }
    pub fn start(&mut self, plan: &Plan, receipt: &Receipt) -> anyhow::Result<()> {
        let _lease = self.files.lifecycle_lock()?;
        self.ensure_installed(plan, receipt)?;
        Ok(self.launcher.launchctl(
            &plan
                .start_arguments(receipt.path())
                .map_err(anyhow::Error::msg)?,
        )?)
    }
    /// Only a proven NotApplied result permits file rollback. An uncertain
    /// bootstrap may have registered the job; retain evidence and require
    /// reconciliation. Never bootout after an error: the label may belong to
    /// an existing, unrelated job that rejected our bootstrap.
    pub fn install_and_start(&mut self, plan: &Plan) -> Result<Receipt, InstallStartError> {
        let _lease = self
            .files
            .lifecycle_lock()
            .map_err(InstallStartError::without_receipt)?;
        let receipt = self.install(plan)?;
        let args = self
            .ensure_installed(plan, &receipt)
            .and_then(|()| {
                plan.start_arguments(receipt.path())
                    .map_err(anyhow::Error::msg)
            })
            .map_err(|source| InstallStartError {
                receipt: Some(receipt.clone()),
                source,
            })?;
        match self.launcher.launchctl(&args) {
            Ok(()) => Ok(receipt),
            Err(start @ LaunchError::NotApplied(_)) => {
                if let Err(remove) = self.files.remove(&receipt) {
                    return Err(InstallStartError {
                        receipt: Some(receipt),
                        source: anyhow::anyhow!(
                            "start failed: {start}; file rollback failed: {remove}"
                        ),
                    });
                }
                Err(InstallStartError {
                    receipt: None,
                    source: start.into(),
                })
            }
            Err(start @ LaunchError::OutcomeUnknown(_)) => Err(InstallStartError {
                receipt: Some(receipt),
                source: anyhow::anyhow!("{start}; retaining managed file for reconciliation"),
            }),
        }
    }
    /// Worker acknowledgement precedes bootout. Cancel semantics belong to the
    /// worker backend, never launchctl's process stopping behavior.
    pub fn stop(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
        cancel_running: bool,
    ) -> anyhow::Result<()> {
        self.stop_checked(plan, receipt, cancel_running, || Ok(()))
    }

    pub(crate) fn stop_checked(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
        cancel_running: bool,
        check: impl FnOnce() -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let _lease = self.files.lifecycle_lock()?;
        self.ensure_installed(plan, receipt)?;
        self.launcher.verify_loaded(plan, receipt)?;
        self.worker
            .request(&self.identity, WorkerRequest::Stop { cancel_running })?;
        self.ensure_installed(plan, receipt).context(
            "worker acknowledged stop; managed-file reconciliation required; bootout not applied",
        )?;
        self.launcher.verify_loaded(plan, receipt).context(
            "worker acknowledged stop; loaded-job reconciliation required; bootout not applied",
        )?;
        check()?;
        Ok(self.launcher.launchctl(
            &plan
                .stop_arguments(receipt.path())
                .map_err(anyhow::Error::msg)?,
        )?)
    }
    pub fn drain(&mut self) -> anyhow::Result<()> {
        self.worker.request(&self.identity, WorkerRequest::Drain)
    }
    pub fn resume(&mut self) -> anyhow::Result<()> {
        self.worker.request(&self.identity, WorkerRequest::Resume)
    }
    /// Caller must first stop/confirm the service; removing a plist alone does
    /// not stop launchd or cancel worker attempts.
    pub fn remove(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        let _lease = self.files.lifecycle_lock()?;
        self.files.remove(receipt)
    }
}
#[cfg(test)]
mod tests;
