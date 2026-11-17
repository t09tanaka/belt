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
    exclusive: bool,
}
/// What one bounded sweep of crashed publications did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TmpSweep {
    /// Orphaned temporaries unlinked.
    pub(crate) removed: u32,
    /// Entries named like a temporary that are not provably one of ours
    /// (a link, a directory, another owner or mode, an unrelated hard link).
    pub(crate) retained: u32,
}
/// The name a publication temporary `.<published>.<ulid>.tmp` was going to be
/// published as. Only the two names `publish` is ever called with qualify: a
/// run bundle and the parent registration.
fn orphan_tmp_target(name: &str) -> Option<&str> {
    let body = name.strip_prefix('.')?.strip_suffix(".tmp")?;
    let (published, id) = body.rsplit_once('.')?;
    ulid::Ulid::from_string(id).ok()?;
    let known = published == "parent-retention.json"
        || published
            .strip_suffix(".json")
            .is_some_and(|run| RunId::parse(run).is_ok());
    known.then_some(published)
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
            exclusive,
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
    /// Unlink what a `publish` that died left behind: at most `limit`
    /// plaintext temporaries per call. Every temporary of this directory is
    /// created and removed by a `publish` holding a lease, so under the
    /// exclusive lease none belongs to a live writer and no age or clock is
    /// consulted. A crash after the link and before the unlink leaves the
    /// published file with two links, which every reader refuses; removing
    /// the temporary name restores it. A temporary is never published here.
    pub(crate) fn sweep_orphan_tmp(&self, limit: u32) -> anyhow::Result<TmpSweep> {
        ensure!(
            self.exclusive,
            "Vault temporary sweep requires exclusive ownership"
        );
        self.current()?;
        let euid = rustix::process::geteuid().as_raw();
        // Names first: unlinking while a directory stream is open may skip entries.
        let mut names = Vec::new();
        for entry in rustix::fs::Dir::read_from(&*self.root)? {
            let entry = entry?;
            if let Ok(name) = entry.file_name().to_str() {
                if orphan_tmp_target(name).is_some() {
                    names.push(name.to_owned());
                }
            }
        }
        names.sort();
        let mut sweep = TmpSweep::default();
        for name in names {
            let published = orphan_tmp_target(&name).context("Vault temporary name")?;
            let stat = rustix::fs::statat(&*self.root, name.as_str(), AtFlags::SYMLINK_NOFOLLOW)?;
            let ours = stat.st_mode as u32 & 0o170000 == 0o100000
                && stat.st_uid == euid
                && stat.st_mode as u32 & 0o7777 == 0o600;
            let orphan = ours
                && match stat.st_nlink as u64 {
                    1 => true,
                    // Only the publication this temporary was linked to.
                    2 => rustix::fs::statat(&*self.root, published, AtFlags::SYMLINK_NOFOLLOW)
                        .is_ok_and(|p| (p.st_dev, p.st_ino) == (stat.st_dev, stat.st_ino)),
                    _ => false,
                };
            if !orphan {
                sweep.retained += 1;
                continue;
            }
            if sweep.removed == limit {
                break;
            }
            rustix::fs::unlinkat(&*self.root, name.as_str(), AtFlags::empty())
                .context("unlink orphaned Vault temporary")?;
            sweep.removed += 1;
        }
        if sweep.removed > 0 {
            self.root.sync_all()?;
        }
        Ok(sweep)
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
        let tmp = crate::test_tmp::tempdir().unwrap();
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
        let tmp = crate::test_tmp::tempdir().unwrap();
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
    fn private_file(path: &std::path::Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn tmp_name(published: &str) -> String {
        format!(".{published}.{}.tmp", ulid::Ulid::new())
    }
    #[test]
    fn orphan_tmp_target_accepts_only_publication_temporaries() {
        let run = RunId::generate();
        let id = ulid::Ulid::new();
        let bundle = format!("{run}.json");
        assert_eq!(
            orphan_tmp_target(&format!(".{bundle}.{id}.tmp")),
            Some(bundle.as_str())
        );
        assert_eq!(
            orphan_tmp_target(&format!(".parent-retention.json.{id}.tmp")),
            Some("parent-retention.json")
        );
        for other in [
            format!("{bundle}.{id}.tmp"),
            format!(".{bundle}.{id}"),
            format!(".{bundle}.{id}.tmp2"),
            format!(".{bundle}.not-a-ulid.tmp"),
            format!(".{bundle}.tmp"),
            format!(".{run}.{id}.tmp"),
            format!(".{run}.txt.{id}.tmp"),
            format!(".other.json.{id}.tmp"),
            format!(".retention.lock.{id}.tmp"),
            format!(".{id}.tmp"),
            ".tmp".into(),
            bundle.clone(),
        ] {
            assert_eq!(orphan_tmp_target(&other), None, "{other}");
        }
    }
    #[test]
    fn orphan_sweep_recovers_a_publication_that_died_before_or_after_the_link() {
        let (_tmp, v, r) = fixture();
        let bundle = format!("{r}.json");
        // Died before the link: plaintext that was never published.
        let unpublished = v.dir.join(tmp_name(&format!("{}.json", RunId::generate())));
        private_file(&unpublished, b"never-published-private");
        // Died after the link and before the unlink: one inode, two names.
        let linked = v.dir.join(tmp_name(&bundle));
        std::fs::hard_link(v.path(&r), &linked).unwrap();
        assert!(!v.has(&r));
        let shared = v.lease(false).unwrap();
        assert!(shared.bundle(&r).is_err());
        // A shared holder cannot prove that no writer is alive.
        assert!(shared.sweep_orphan_tmp(32).is_err());
        assert!(unpublished.exists() && linked.exists());
        drop(shared);
        let lease = v.lease(true).unwrap();
        assert_eq!(
            lease.sweep_orphan_tmp(32).unwrap(),
            TmpSweep {
                removed: 2,
                retained: 0
            }
        );
        assert!(!unpublished.exists() && !linked.exists());
        assert_eq!(lease.bundle(&r).unwrap().unwrap(), b"owned-fixture");
        assert_eq!(lease.sweep_orphan_tmp(32).unwrap(), TmpSweep::default());
        drop(lease);
        assert!(v.has(&r));
        let mut names: Vec<_> = std::fs::read_dir(&v.dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["retention.lock".to_string(), bundle]);
    }
    #[test]
    fn orphan_sweep_retains_whatever_is_not_provably_a_dead_publication() {
        let (tmp, v, r) = fixture();
        let bundle = format!("{r}.json");
        let outside = tmp.path().join("outside");
        private_file(&outside, b"outside-private");
        // A link named like a temporary is neither followed nor removed.
        let link = v.dir.join(tmp_name(&bundle));
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let directory = v.dir.join(tmp_name(&bundle));
        std::fs::create_dir(&directory).unwrap();
        // Two links, but the other name is not this temporary's publication.
        let foreign = v.dir.join(tmp_name(&bundle));
        std::fs::hard_link(&outside, &foreign).unwrap();
        // Linked to a publication of another name.
        let other = v.dir.join(tmp_name(&format!("{}.json", RunId::generate())));
        std::fs::hard_link(v.path(&r), &other).unwrap();
        let readable = v.dir.join(tmp_name(&format!("{}.json", RunId::generate())));
        std::fs::write(&readable, b"shared-mode").unwrap();
        std::fs::set_permissions(&readable, std::fs::Permissions::from_mode(0o644)).unwrap();
        // Not a publication temporary at all: not even counted.
        let unrelated = v.dir.join(".unrelated.tmp");
        private_file(&unrelated, b"unrelated");
        let lease = v.lease(true).unwrap();
        assert_eq!(
            lease.sweep_orphan_tmp(32).unwrap(),
            TmpSweep {
                removed: 0,
                retained: 5
            }
        );
        for kept in [&link, &directory, &foreign, &other, &readable, &unrelated] {
            assert!(std::fs::symlink_metadata(kept).is_ok(), "{kept:?}");
        }
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside-private");
        assert_eq!(std::fs::read(v.path(&r)).unwrap(), b"owned-fixture");
        // The three-name inode is still refused: nothing was promoted or repaired.
        assert!(lease.bundle(&r).is_err());
    }
    #[test]
    fn orphan_sweep_is_bounded_per_call_and_resumes() {
        let (_tmp, v, r) = fixture();
        for _ in 0..33 {
            private_file(
                &v.dir.join(tmp_name(&format!("{}.json", RunId::generate()))),
                b"never-published-private",
            );
        }
        let lease = v.lease(true).unwrap();
        assert_eq!(lease.sweep_orphan_tmp(32).unwrap().removed, 32);
        assert_eq!(std::fs::read_dir(&v.dir).unwrap().count(), 3);
        assert_eq!(lease.sweep_orphan_tmp(32).unwrap().removed, 1);
        assert_eq!(lease.sweep_orphan_tmp(0).unwrap(), TmpSweep::default());
        assert_eq!(lease.bundle(&r).unwrap().unwrap(), b"owned-fixture");
    }
}
