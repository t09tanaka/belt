//! Capabilities persisted only in trusted, private worker attempt state.
use super::{PinnedExecution, PinnedWorkspace};
use crate::exec::Workspace;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

/// An accepted worker attempt, distinct from project-controlled input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptBinding {
    pub attempt: String,
    pub generation: u64,
}
impl AttemptBinding {
    fn validate(&self) -> Result<()> {
        if self.attempt.is_empty()
            || self.attempt.len() > 256
            || !self
                .attempt
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            bail!("invalid accepted attempt identity");
        }
        Ok(())
    }
}

/// A fixed SDK directory outside the project workspace. The caller must supply
/// its trusted worker-state directory FD, never a directory from a submission.
/// All directories below that trust root must already be private and owned.
/// This has the same-UID trust boundary as the worker accepted-attempt records;
/// it is not a signature against a malicious process with the worker UID.
pub struct PrivateReceiptDirectory {
    directory: File,
}
fn private_directory(file: &File) -> Result<()> {
    let md = file.metadata()?;
    if !md.is_dir() || md.uid() != unsafe { libc::geteuid() } || md.mode() & 0o077 != 0 {
        bail!("worker receipt ancestor is not an owned private directory");
    }
    Ok(())
}
fn open_at(parent: &File, name: &[u8], flags: i32) -> Result<File> {
    let name = CString::new(name)?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}
impl PrivateReceiptDirectory {
    /// Open an existing validated attempt directory below the trusted state FD
    /// and create/open its fixed `.local-ci-captured` private SDK child.
    pub fn open(trusted_worker_state: &File, attempt_relative: &Path) -> Result<Self> {
        private_directory(trusted_worker_state)?;
        let components: Vec<_> = attempt_relative.components().collect();
        if components.is_empty() || components.len() > 8 {
            bail!("invalid private attempt directory");
        }
        let mut parent = trusted_worker_state.try_clone()?;
        for component in components {
            let Component::Normal(name) = component else {
                bail!("attempt directory must be relative and have plain components");
            };
            parent = open_at(&parent, name.as_bytes(), libc::O_RDONLY | libc::O_DIRECTORY)?;
            private_directory(&parent)?;
        }
        let child = c".local-ci-captured";
        let status = unsafe { libc::mkdirat(parent.as_raw_fd(), child.as_ptr(), 0o700) };
        if status != 0
            && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let directory = open_at(
            &parent,
            child.to_bytes(),
            libc::O_RDONLY | libc::O_DIRECTORY,
        )?;
        private_directory(&directory)?;
        parent.sync_all()?;
        Ok(Self { directory })
    }
    fn read(&self) -> Result<Receipt> {
        private_directory(&self.directory)?;
        let file = open_at(&self.directory, b"workspace.json", libc::O_RDONLY)?;
        validate_file(&file)?;
        let mut bytes = Vec::new();
        file.take(65537).read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            bail!("worker receipt is too large");
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}
fn validate_file(file: &File) -> Result<()> {
    let md = file.metadata()?;
    if !md.is_file()
        || md.uid() != unsafe { libc::geteuid() }
        || md.mode() & 0o077 != 0
        || md.nlink() != 1
        || md.len() > 65536
    {
        bail!("worker receipt is not an owned private regular file");
    }
    Ok(())
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u32,
    binding: AttemptBinding,
    plan: String,
    source: String,
    manifest: String,
    root: PathBuf,
    device: u64,
    inode: u64,
    repo_root: PathBuf,
    run_dir: PathBuf,
    tools: PathBuf,
    lock_file: PathBuf,
}
impl PinnedWorkspace {
    /// Persist this already-verified capability before restoring secrets or
    /// running setup. Existing identical receipts are idempotent; never replace
    /// another attempt or root. The directory is outside project control.
    pub fn write_receipt(
        &self,
        input: &PinnedExecution<'_>,
        directory: &PrivateReceiptDirectory,
        binding: &AttemptBinding,
    ) -> Result<()> {
        input.validate(input.plan)?;
        self.validate(input)?;
        binding.validate()?;
        let receipt = Receipt {
            version: 1,
            binding: binding.clone(),
            plan: self.plan_digest.clone(),
            source: input.manifest.source_id.clone(),
            manifest: input.manifest.manifest_digest.clone(),
            root: self.root.clone(),
            device: self.device,
            inode: self.inode,
            repo_root: self.workspace.repo_root.clone(),
            run_dir: self.workspace.run_dir.clone(),
            tools: self.workspace.tools.clone(),
            lock_file: self.workspace.lock_file.clone(),
        };
        private_directory(&directory.directory)?;
        let bytes = serde_json::to_vec(&receipt)?;
        if bytes.len() > 65536 {
            bail!("worker receipt is too large");
        }
        // An immutable receipt is published once. The cooperating writer lock
        // makes rename atomic without replacing an existing different binding.
        let writer = open_at(
            &directory.directory,
            b"receipt.lock",
            libc::O_RDWR | libc::O_CREAT,
        )?;
        validate_file(&writer)?;
        fs2::FileExt::try_lock_exclusive(&writer)?;
        match directory.read() {
            Ok(existing) if existing == receipt => return Ok(()),
            Ok(_) => bail!("cannot replace another captured workspace receipt"),
            Err(error) => {
                if !error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
                {
                    return Err(error);
                }
            }
        }
        let temporary = CString::new(format!("receipt-{}.tmp", super::new_namespace()?))?;
        let mut file = open_at(
            &directory.directory,
            temporary.to_bytes(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        )?;
        let result = (|| -> Result<()> {
            validate_file(&file)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            let status = unsafe {
                libc::renameat(
                    directory.directory.as_raw_fd(),
                    temporary.as_ptr(),
                    directory.directory.as_raw_fd(),
                    c"workspace.json".as_ptr(),
                )
            };
            if status != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            directory.directory.sync_all()?;
            Ok(())
        })();
        drop(file);
        if result.is_err() {
            unsafe {
                libc::unlinkat(directory.directory.as_raw_fd(), temporary.as_ptr(), 0);
            }
        }
        result?;
        Ok(())
    }
}
impl PinnedExecution<'_> {
    /// Recover an SDK-issued receipt from trusted worker state after legitimate
    /// setup/secret mutations. No source copy or content revalidation occurs.
    /// Every root and execution path must still equal the verified receipt.
    pub fn restore_workspace(
        &self,
        workspace: Workspace,
        directory: &PrivateReceiptDirectory,
        expected: &AttemptBinding,
    ) -> Result<PinnedWorkspace> {
        self.validate(self.plan)?;
        expected.validate()?;
        let receipt = directory.read()?;
        if receipt.version != 1
            || &receipt.binding != expected
            || receipt.plan != self.plan.digest
            || receipt.source != self.manifest.source_id
            || receipt.manifest != self.manifest.manifest_digest
            || workspace.repo_root != receipt.repo_root
            || workspace.run_dir != receipt.run_dir
            || workspace.tools != receipt.tools
            || workspace.lock_file != receipt.lock_file
            || std::fs::canonicalize(&workspace.root)? != receipt.root
        {
            bail!("worker captured receipt binding mismatch");
        }
        let restored = PinnedWorkspace {
            workspace,
            plan_digest: receipt.plan,
            root: receipt.root,
            device: receipt.device,
            inode: receipt.inode,
            namespace: super::new_namespace()?,
            sequence: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        restored.validate(self)?;
        Ok(restored)
    }
}
