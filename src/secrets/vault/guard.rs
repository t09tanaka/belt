//! Cross-process ownership of held plaintext and exact deletion targets.
use super::*;
use anyhow::{ensure, Context};
use rustix::fs::{AtFlags, Mode, OFlags};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
const DIR: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const READ: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);
const MAX: u64 = 32 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct Lease {
    root: Arc<File>,
    path: PathBuf,
    lock: File,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.lock.unlock();
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    dev: u64,
    ino: u64,
    size: u64,
    uid: u32,
    mode: u32,
    links: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl Identity {
    fn of(file: &File) -> anyhow::Result<Self> {
        let m = file.metadata()?;
        Ok(Self {
            dev: m.dev(),
            ino: m.ino(),
            size: m.len(),
            uid: m.uid(),
            mode: m.mode(),
            links: m.nlink(),
            modified: (m.mtime(), m.mtime_nsec()),
            changed: (m.ctime(), m.ctime_nsec()),
        })
    }
    fn private(&self) -> bool {
        self.mode & 0o170000 == 0o100000
            && self.uid == rustix::process::geteuid().as_raw()
            && self.links == 1
            && self.mode & 0o7777 == 0o600
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claim {
    run: RunId,
    root: (u64, u64),
    identity: Option<Identity>,
    digest: Option<String>,
}
fn directory(file: &File) -> anyhow::Result<(u64, u64)> {
    let m = file.metadata()?;
    ensure!(
        m.is_dir() && m.uid() == rustix::process::geteuid().as_raw() && m.mode() & 0o077 == 0,
        "unsafe Vault directory"
    );
    Ok((m.dev(), m.ino()))
}
// st_dev is signed on macOS and u64 on Linux.
#[allow(clippy::unnecessary_cast)]
impl Lease {
    pub(crate) fn acquire(path: &Path, root: Arc<File>, exclusive: bool) -> anyhow::Result<Self> {
        let lock: File = match rustix::fs::openat(
            &*root,
            "retention.lock",
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::EXIST) => rustix::fs::openat(
                &*root,
                "retention.lock",
                OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?,
            Err(e) => return Err(e.into()),
        }
        .into();
        ensure!(Identity::of(&lock)?.private(), "unsafe Vault lock");
        let locked = if exclusive {
            lock.try_lock()
        } else {
            lock.try_lock_shared()
        };
        locked.map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => std::io::Error::from(ErrorKind::WouldBlock),
            std::fs::TryLockError::Error(e) => e,
        })?;
        let lease = Self {
            root,
            path: path.into(),
            lock,
        };
        lease.current()?;
        let named = rustix::fs::statat(&*lease.root, "retention.lock", AtFlags::SYMLINK_NOFOLLOW)?;
        let held = lease.lock.metadata()?;
        ensure!(
            (named.st_dev as u64, named.st_ino) == (held.dev(), held.ino()),
            "Vault lock changed"
        );
        lease.lock.sync_all()?;
        lease.root.sync_all()?;
        Ok(lease)
    }
    pub(crate) fn current(&self) -> anyhow::Result<()> {
        ensure!(self.path.is_absolute(), "Vault root must be absolute");
        let mut opened: File = rustix::fs::open("/", DIR, Mode::empty())?.into();
        for part in self.path.components() {
            match part {
                std::path::Component::RootDir => {}
                std::path::Component::Normal(name) => {
                    opened = rustix::fs::openat(&opened, name, DIR, Mode::empty())?.into()
                }
                _ => anyhow::bail!("noncanonical Vault root"),
            }
        }
        ensure!(
            directory(&opened)? == directory(&self.root)?,
            "Vault root changed"
        );
        let named = rustix::fs::statat(&*self.root, "retention.lock", AtFlags::SYMLINK_NOFOLLOW)?;
        let held = self.lock.metadata()?;
        ensure!(
            (named.st_dev as u64, named.st_ino) == (held.dev(), held.ino())
                && Identity::of(&self.lock)?.private(),
            "Vault lock changed"
        );
        Ok(())
    }
    fn read_open(&self, name: &str) -> anyhow::Result<Option<(Vec<u8>, Identity, File)>> {
        self.current()?;
        let file: File = match rustix::fs::openat(&*self.root, name, READ, Mode::empty()) {
            Ok(fd) => fd.into(),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let before = Identity::of(&file)?;
        let limit = if name == "parent-retention.json" {
            65536
        } else {
            MAX
        };
        if !before.private() {
            return Err(SecretError::Insecure {
                what: "secret bundle",
                path: self.path.join(name),
            }
            .into());
        }
        ensure!(before.size <= limit, "oversized Vault file");
        let mut bytes = Vec::new();
        (&file).take(before.size + 1).read_to_end(&mut bytes)?;
        ensure!(
            Identity::of(&file)? == before && bytes.len() as u64 == before.size,
            "Vault file changed"
        );
        let named = rustix::fs::statat(&*self.root, name, AtFlags::SYMLINK_NOFOLLOW)?;
        ensure!(
            (named.st_dev as u64, named.st_ino) == (before.dev, before.ino),
            "Vault file replaced"
        );
        Ok(Some((bytes, before, file)))
    }
    fn read(&self, name: &str) -> anyhow::Result<Option<(Vec<u8>, Identity)>> {
        Ok(self
            .read_open(name)?
            .map(|(bytes, identity, _)| (bytes, identity)))
    }
    fn parent_registration(
        &self,
    ) -> anyhow::Result<Option<crate::source::retention::VaultRegistration>> {
        let state = self.path.parent().context("Vault state parent")?;
        // The permanent lock also pins parent classification: losing the
        // optional marker and even the source tree cannot downgrade a parent.
        if let Some(registration) = self.pinned_registration()? {
            ensure!(
                registration.matches_parent(state),
                "Vault pinned source path mismatch"
            );
            if let Some((bytes, _)) = self.read("parent-retention.json")? {
                ensure!(
                    serde_json::from_slice::<crate::source::retention::VaultRegistration>(&bytes)?
                        == registration,
                    "Vault marker and classification pin disagree"
                );
            }
            return Ok(Some(registration));
        }
        if let Some((bytes, _)) = self.read("parent-retention.json")? {
            let registration: crate::source::retention::VaultRegistration =
                serde_json::from_slice(&bytes)?;
            ensure!(
                registration.matches_parent(state),
                "Vault registration source path mismatch"
            );
            Ok(Some(registration))
        } else {
            crate::source::retention::recover_vault_registration(state)
        }
    }
    fn pinned_registration(
        &self,
    ) -> anyhow::Result<Option<crate::source::retention::VaultRegistration>> {
        use std::os::unix::fs::FileExt;
        let before = Identity::of(&self.lock)?;
        ensure!(
            before.private() && before.size <= 65536,
            "unsafe Vault classification pin"
        );
        if before.size == 0 {
            return Ok(None);
        }
        let mut bytes = vec![0; before.size as usize];
        self.lock.read_exact_at(&mut bytes, 0)?;
        ensure!(
            Identity::of(&self.lock)? == before,
            "Vault classification pin changed"
        );
        Ok(Some(serde_json::from_slice(&bytes)?))
    }
    pub(crate) fn check_parent(&self) -> anyhow::Result<()> {
        if let Some(registration) = self.parent_registration()? {
            crate::source::retention::check_vault_registration(&registration)?;
        }
        Ok(())
    }
    pub(crate) fn registered(&self) -> anyhow::Result<bool> {
        Ok(self.parent_registration()?.is_some())
    }
    pub(crate) fn available(&self, run: &RunId) -> anyhow::Result<()> {
        if let Some(registration) = self.parent_registration()? {
            crate::source::retention::check_vault_run(&registration, run)?;
        }
        Ok(())
    }
    pub(crate) fn has(&self, run: &RunId) -> anyhow::Result<bool> {
        self.current()?;
        let name = format!("{run}.json");
        let file: File = match rustix::fs::openat(&*self.root, name.as_str(), READ, Mode::empty()) {
            Ok(fd) => fd.into(),
            Err(rustix::io::Errno::NOENT) => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        let identity = Identity::of(&file)?;
        if !identity.private() {
            return Err(SecretError::Insecure {
                what: "secret bundle",
                path: self.path.join(name),
            }
            .into());
        }
        ensure!(identity.size <= MAX, "oversized Vault file");
        Ok(true)
    }
    pub(crate) fn bundle(&self, run: &RunId) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.read(&format!("{run}.json"))?.map(|x| x.0))
    }
    pub(crate) fn publish(&self, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.current()?;
        let tmp = format!(".{name}.{}.tmp", ulid::Ulid::new());
        let mut file: File = rustix::fs::openat(
            &*self.root,
            tmp.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?
        .into();
        let result = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            self.current()?;
            #[cfg(test)]
            PUBLISH_HOOK.with(|hook| {
                if let Some(hook) = hook.borrow().as_ref() {
                    hook();
                }
            });
            match rustix::fs::linkat(
                &*self.root,
                tmp.as_str(),
                &*self.root,
                name,
                AtFlags::empty(),
            ) {
                Ok(()) => {}
                Err(rustix::io::Errno::EXIST) => {
                    ensure!(
                        self.read(name)?.is_some(),
                        "existing Vault publication disappeared"
                    );
                }
                Err(error) => return Err(error.into()),
            }
            rustix::fs::unlinkat(&*self.root, tmp.as_str(), AtFlags::empty())?;
            self.root.sync_all()?;
            anyhow::Ok(())
        })();
        if result.is_err() {
            let _ = rustix::fs::unlinkat(&*self.root, tmp.as_str(), AtFlags::empty());
        }
        result
    }
    pub(crate) fn register(
        &self,
        registration: &crate::source::retention::VaultRegistration,
    ) -> anyhow::Result<()> {
        if let Some(pinned) = self.pinned_registration()? {
            ensure!(
                pinned == *registration,
                "Vault parent classification changed"
            );
        } else {
            use std::os::unix::fs::FileExt;
            self.lock
                .write_all_at(&serde_json::to_vec(registration)?, 0)?;
            self.lock.sync_all()?;
        }
        if let Some((bytes, _)) = self.read("parent-retention.json")? {
            ensure!(
                serde_json::from_slice::<crate::source::retention::VaultRegistration>(&bytes)?
                    == *registration,
                "Vault parent registration changed"
            );
        } else {
            self.publish("parent-retention.json", &serde_json::to_vec(registration)?)?;
            let (bytes, _) = self
                .read("parent-retention.json")?
                .context("missing parent registration")?;
            ensure!(
                serde_json::from_slice::<crate::source::retention::VaultRegistration>(&bytes)?
                    == *registration,
                "Vault parent registration publication mismatch"
            );
        }
        Ok(())
    }
    pub(crate) fn reserve(&self, run: &RunId, budget: &mut u64, passes: u64) -> anyhow::Result<()> {
        self.current()?;
        let name = format!("{run}.json");
        let fd: File = match rustix::fs::openat(&*self.root, name.as_str(), READ, Mode::empty()) {
            Ok(fd) => fd.into(),
            Err(rustix::io::Errno::NOENT) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let identity = Identity::of(&fd)?;
        ensure!(
            identity.private() && identity.size <= MAX,
            "unsafe or oversized Vault budget target"
        );
        let charge = identity
            .size
            .checked_mul(passes)
            .context("Vault budget overflow")?;
        ensure!(
            charge <= *budget,
            "Vault validation page byte budget exhausted"
        );
        *budget -= charge;
        Ok(())
    }
    pub(crate) fn claim(&self, run: &RunId) -> anyhow::Result<Claim> {
        Ok(self.proof(run)?.0)
    }
    pub(crate) fn proof(&self, run: &RunId) -> anyhow::Result<(Claim, Option<Vec<u8>>)> {
        use sha2::{Digest as _, Sha256};
        let leaf = self.read(&format!("{run}.json"))?;
        let claim = Claim {
            run: run.clone(),
            root: directory(&self.root)?,
            identity: leaf.as_ref().map(|x| x.1.clone()),
            digest: leaf.as_ref().map(|x| format!("{:x}", Sha256::digest(&x.0))),
        };
        Ok((claim, leaf.map(|x| x.0)))
    }
    pub(crate) fn drain(&self, run: &RunId, claim: &Claim) -> anyhow::Result<()> {
        use sha2::{Digest as _, Sha256};
        self.current()?;
        ensure!(
            run == &claim.run && directory(&self.root)? == claim.root,
            "Vault claim anchor mismatch"
        );
        let name = format!("{run}.json");
        if let Some((bytes, identity, file)) = self.read_open(&name)? {
            ensure!(
                Some(&identity) == claim.identity.as_ref()
                    && Some(format!("{:x}", Sha256::digest(bytes))) == claim.digest,
                "Vault claim target changed"
            );
            self.current()?;
            let named = rustix::fs::statat(&*self.root, name.as_str(), AtFlags::SYMLINK_NOFOLLOW)?;
            ensure!(
                (named.st_dev as u64, named.st_ino) == (identity.dev, identity.ino),
                "Vault target changed before unlink"
            );
            ensure!(
                Identity::of(&file)? == identity,
                "Vault metadata changed before unlink"
            );
            retention_fault("before_unlink")?;
            rustix::fs::unlinkat(&*self.root, name.as_str(), AtFlags::empty())
                .context("unlink exact Vault claim")?;
            retention_fault("after_unlink")?;
        }
        self.root.sync_all()?;
        retention_fault("after_fsync")?;
        Ok(())
    }
}
#[cfg(test)]
thread_local! {static PUBLISH_HOOK:std::cell::RefCell<Option<Box<dyn Fn()>>>=const{std::cell::RefCell::new(None)};}
#[cfg(test)]
thread_local! {static FAULT:std::cell::Cell<Option<&'static str>>=const{std::cell::Cell::new(None)};}
#[cfg(test)]
pub(crate) fn set_retention_fault(stage: Option<&'static str>) {
    FAULT.with(|f| f.set(stage));
}
pub(crate) fn retention_fault(stage: &str) -> anyhow::Result<()> {
    #[cfg(test)]
    if FAULT.with(|f| f.get() == Some(stage)) {
        anyhow::bail!("owned Vault fault {stage}");
    }
    let _ = stage;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn fixture() -> (tempfile::TempDir, Vault, RunId) {
        let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
        let vault = Vault::open(tmp.path()).unwrap();
        let run = RunId::generate();
        let lease = vault.lease(true).unwrap();
        lease
            .publish(&format!("{run}.json"), b"owned-fixture")
            .unwrap();
        drop(lease);
        (tmp, vault, run)
    }
    #[test]
    fn vault_exact_leaf_rejects_same_inode_tampering_and_permissions() {
        let (_tmp, v, r) = fixture();
        let lease = v.lease(true).unwrap();
        let claim = lease.claim(&r).unwrap();
        std::fs::write(v.path(&r), b"tampered-data").unwrap();
        assert!(lease.drain(&r, &claim).is_err());
        assert!(v.path(&r).exists());
        std::fs::set_permissions(v.path(&r), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(lease.claim(&r).is_err());
    }
    #[test]
    fn vault_exact_leaf_rejects_symlink_hardlink_and_missing_claim_recreation() {
        let (tmp, v, r) = fixture();
        let lease = v.lease(true).unwrap();
        std::fs::hard_link(v.path(&r), tmp.path().join("another-link")).unwrap();
        assert!(lease.claim(&r).is_err());
        std::fs::remove_file(tmp.path().join("another-link")).unwrap();
        std::fs::remove_file(v.path(&r)).unwrap();
        let absent = lease.claim(&r).unwrap();
        lease.publish(&format!("{r}.json"), b"recreated").unwrap();
        assert!(lease.drain(&r, &absent).is_err());
        std::fs::remove_file(v.path(&r)).unwrap();
        std::os::unix::fs::symlink(tmp.path().join("original-sentinel"), v.path(&r)).unwrap();
        assert!(lease.claim(&r).is_err());
    }
    #[test]
    fn vault_stale_root_and_lock_path_swaps_fail_closed() {
        let (tmp, v, r) = fixture();
        let lease = v.lease(true).unwrap();
        let claim = lease.claim(&r).unwrap();
        std::fs::rename(&v.dir, tmp.path().join("old-secrets")).unwrap();
        std::fs::create_dir(&v.dir).unwrap();
        std::fs::set_permissions(&v.dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(lease.drain(&r, &claim).is_err());
        assert!(!v.has(&r));
        assert!(tmp
            .path()
            .join("old-secrets")
            .join(format!("{r}.json"))
            .exists());
        drop(lease);
        std::fs::remove_dir(&v.dir).unwrap();
        std::fs::rename(tmp.path().join("old-secrets"), &v.dir).unwrap();
        let lease = v.lease(true).unwrap();
        std::fs::rename(v.dir.join("retention.lock"), v.dir.join("old-lock")).unwrap();
        std::fs::write(v.dir.join("retention.lock"), b"").unwrap();
        std::fs::set_permissions(
            v.dir.join("retention.lock"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert!(lease.current().is_err());
    }
    #[test]
    fn vault_direct_concurrent_adopt_keeps_first_durable_publication() {
        use std::sync::{mpsc, Barrier};
        let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
        let run = RunId::generate();
        let key = ClusterKey::from_bytes(vec![9; 32]).unwrap();
        let entries = |content: &[u8]| {
            vec![SecretEntry {
                path: RelPath(b".env".to_vec()),
                hmac: secret_digest(key.as_bytes(), &RelPath(b".env".to_vec()), content),
            }]
        };
        let first = b"TOKEN=first-owned-fixture";
        let second = b"TOKEN=second-owned-fixture";
        let copies = [tmp.path().join("first"), tmp.path().join("second")];
        for (path, content) in copies.iter().zip([first.as_slice(), second.as_slice()]) {
            let bytes=serde_json::to_vec(&serde_json::json!({"schema_version":1,"files":[{"path":".env","content_hex":crate::secrets::plan::hex_encode(content)}]})).unwrap();
            write_private(path, &bytes).unwrap();
        }
        let barrier = Arc::new(Barrier::new(2));
        let (published, ready) = mpsc::channel();
        let state = tmp.path().to_path_buf();
        let r = run.clone();
        let k = key.clone();
        let e = entries(first);
        let b = Arc::clone(&barrier);
        let copy = copies[0].clone();
        let winner = std::thread::spawn(move || {
            PUBLISH_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    b.wait();
                }))
            });
            let v = Vault::open(&state).unwrap();
            v.adopt(&r, &copy, &e, &k).unwrap();
            published.send(()).unwrap();
        });
        let state = tmp.path().to_path_buf();
        let r = run.clone();
        let k = key.clone();
        let e = entries(second);
        let copy = copies[1].clone();
        let later = std::thread::spawn(move || {
            PUBLISH_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    barrier.wait();
                    ready.recv_timeout(Duration::from_secs(5)).unwrap();
                }))
            });
            Vault::open(&state)
                .unwrap()
                .adopt(&r, &copy, &e, &k)
                .unwrap();
        });
        winner.join().unwrap();
        later.join().unwrap();
        let v = Vault::open(tmp.path()).unwrap();
        assert_eq!(
            v.load(&run, &entries(first), &key).unwrap().files[0]
                .content
                .0,
            first
        );
        assert!(matches!(
            v.load(&run, &entries(second), &key),
            Err(SecretError::Tampered { .. })
        ));
    }
    #[test]
    fn vault_last_lease_explicitly_unlocks_despite_an_inherited_description() {
        let (_tmp, v, _r) = fixture();
        let lease = v.lease(false).unwrap();
        let inherited = lease.lock.try_clone().unwrap();
        drop(lease);
        assert!(v.lease(true).is_ok());
        drop(inherited);
    }
}
