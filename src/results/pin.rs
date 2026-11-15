//! Persistent run lock inodes. Only the owner creates them; no code removes them.
use crate::ids::RunId;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

/// A single descriptor held for an entire read, writer mutation or deletion page.
#[derive(Debug)]
pub struct RunPin {
    _file: File,
    run: RunId,
    exclusive: bool,
}

fn checked(file: &File, path: &Path) -> io::Result<()> {
    let a = file.metadata()?;
    let b = fs::symlink_metadata(path)?;
    if !a.is_file()
        || !b.is_file()
        || a.nlink() != 1
        || a.uid() != rustix::process::geteuid().as_raw()
        || a.mode() & 0o077 != 0
        || a.dev() != b.dev()
        || a.ino() != b.ino()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe result pin inode",
        ));
    }
    Ok(())
}

impl RunPin {
    /// Owner-only durable creation before publishing a run or result reference.
    pub(crate) fn ensure(state: &Path, run: &RunId) -> io::Result<()> {
        let dir = state.join("reader-pins");
        match fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => File::open(state)?.sync_all()?,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        let meta = fs::symlink_metadata(&dir)?;
        if !meta.is_dir()
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.mode() & 0o077 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe result pin directory",
            ));
        }
        let path = dir.join(format!("{}.lock", run.as_str()));
        let f = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)?,
            Err(e) => return Err(e),
        };
        checked(&f, &path)?;
        f.sync_all()?;
        File::open(dir)?.sync_all()
    }

    /// Read-only, nonblocking acquisition. A legacy/missing pin fails closed.
    pub fn shared(state: &Path, run: &RunId) -> io::Result<Self> {
        Self::acquire(state, run, false)
    }
    pub(crate) fn exclusive(state: &Path, run: &RunId) -> io::Result<Self> {
        Self::acquire(state, run, true)
    }
    pub(crate) fn check_exclusive(&self, state: &Path, run: &RunId) -> io::Result<()> {
        if !self.exclusive || self.run != *run {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "wrong result GC guard",
            ));
        }
        checked(&self._file, &state.join(format!("reader-pins/{run}.lock")))
    }
    fn acquire(state: &Path, run: &RunId, exclusive: bool) -> io::Result<Self> {
        let relative = format!("reader-pins/{}.lock", run.as_str());
        let f = crate::platform::open_beneath(state, Path::new(&relative))?;
        let path = state.join(relative);
        checked(&f, &path)?;
        let op = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        } | libc::LOCK_NB;
        // SAFETY: flock only borrows this live descriptor; drop releases its lock.
        use std::os::fd::AsRawFd;
        if unsafe { libc::flock(f.as_raw_fd(), op) } != 0 {
            return Err(io::Error::last_os_error());
        }
        checked(&f, &path)?;
        Ok(Self {
            _file: f,
            run: run.clone(),
            exclusive,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_readers_exclude_gc_and_inode_survives_release() {
        let dir = tempfile::tempdir().unwrap();
        let run = RunId::generate();
        assert!(RunPin::shared(dir.path(), &run).is_err());
        assert!(!dir.path().join("reader-pins").exists());
        RunPin::ensure(dir.path(), &run).unwrap();
        let path = dir.path().join(format!("reader-pins/{run}.lock"));
        let inode = fs::metadata(&path).unwrap().ino();
        let a = RunPin::shared(dir.path(), &run).unwrap();
        let b = RunPin::shared(dir.path(), &run).unwrap();
        assert_eq!(
            RunPin::exclusive(dir.path(), &run).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(a);
        drop(b);
        let gc = RunPin::exclusive(dir.path(), &run).unwrap();
        assert_eq!(
            RunPin::shared(dir.path(), &run).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(gc);
        RunPin::ensure(dir.path(), &run).unwrap();
        assert_eq!(fs::metadata(path).unwrap().ino(), inode);
    }
    #[test]
    #[ignore = "owned subprocess helper"]
    fn pin_subprocess_holder() {
        let Ok(state) = std::env::var("RTR_PIN_CHILD_STATE") else {
            return;
        };
        use std::io::Read;
        let run = RunId::parse(&std::env::var("RTR_PIN_CHILD_RUN").unwrap()).unwrap();
        let _pin = if std::env::var_os("RTR_PIN_CHILD_EXCLUSIVE").is_some() {
            RunPin::exclusive(Path::new(&state), &run)
        } else {
            RunPin::shared(Path::new(&state), &run)
        }
        .unwrap();
        crate::test_child::publish_ready().unwrap();
        let mut release = [0u8; 1];
        std::io::stdin().read_exact(&mut release).unwrap();
    }
    #[test]
    fn pins_exclude_across_processes_without_inheriting_descriptors() {
        use std::process::{Command, Stdio};
        let dir = tempfile::tempdir().unwrap();
        let run = RunId::generate();
        RunPin::ensure(dir.path(), &run).unwrap();
        for exclusive in [false, true] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "results::pin::tests::pin_subprocess_holder",
                    "--ignored",
                    "--nocapture",
                ])
                .env("RTR_PIN_CHILD_STATE", dir.path())
                .env("RTR_PIN_CHILD_RUN", run.as_str())
                .stdin(Stdio::piped());
            if exclusive {
                command.env("RTR_PIN_CHILD_EXCLUSIVE", "1");
            }
            let mut child = crate::test_child::OwnedChild::spawn(command).unwrap();
            assert_eq!(
                RunPin::exclusive(dir.path(), &run).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            if exclusive {
                assert_eq!(
                    RunPin::shared(dir.path(), &run).unwrap_err().kind(),
                    io::ErrorKind::WouldBlock
                );
            } else {
                RunPin::shared(dir.path(), &run).unwrap();
            }
            child.release().unwrap();
            assert!(child.wait().unwrap().success());
            RunPin::exclusive(dir.path(), &run).unwrap();
        }
    }
    #[test]
    fn unsafe_pin_and_changed_inode_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let run = RunId::generate();
        RunPin::ensure(dir.path(), &run).unwrap();
        let path = dir.path().join(format!("reader-pins/{run}.lock"));
        let pin = RunPin::exclusive(dir.path(), &run).unwrap();
        fs::rename(&path, path.with_extension("old")).unwrap();
        RunPin::ensure(dir.path(), &run).unwrap();
        assert!(pin.check_exclusive(dir.path(), &run).is_err());
        drop(pin);
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(RunPin::shared(dir.path(), &run).is_err());
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("missing", &path).unwrap();
        assert!(RunPin::shared(dir.path(), &run).is_err());
        assert!(RunPin::ensure(dir.path(), &run).is_err());
    }
}
