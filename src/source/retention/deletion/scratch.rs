//! The scratch directory of a capture, and the files it reserved (spec 25.4
//! (b)).
//!
//! A capture stages every new file of the store under one directory,
//! `staging/<ulid>`, named after its pending root `capture:<ulid>`. The root
//! is committed (with its name in the `capture_owner` row, when the capture
//! has one) before the directory is made, so a directory that exists is named
//! by a row. Before anything is installed in the store, the capture records
//! every file it is about to install as a member of its pending root: a
//! reservation. A capture that ends anywhere leaves a scratch directory and
//! reservations, both named by the row, and nothing else.
use super::*;
use local_ci::source::{Intent, IntentKind};
use rustix::fs::{AtFlags, FileType, Mode};

/// The directory of the store that holds the scratch directories.
pub(in crate::source) const STAGING: &str = "staging";

/// The name of the scratch directory of the pending root `root`. Only the
/// shape `capture:<ULID>` gives a name: the value comes from a row, and it
/// becomes a path component.
fn name_of(root: &str) -> Result<&str> {
    let name = root
        .strip_prefix("capture:")
        .filter(|name| name.len() == 26 && ulid::Ulid::from_string(name).is_ok())
        .filter(|name| name.bytes().all(|b| b.is_ascii_alphanumeric()))
        .context("a capture's root has no scratch name")?;
    Ok(name)
}

/// A reservation: the destination local-ci named, and the key of the object
/// in the index.
pub(in crate::source) type Reserved = Vec<(PathBuf, String)>;

impl Index {
    fn staging(&self) -> Result<Option<File>> {
        match rustix::fs::openat(&*self.root, STAGING, DIR, Mode::empty()) {
            Ok(fd) => {
                let file: File = fd.into();
                directory(&file, true)?;
                Ok(Some(file))
            }
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Make the scratch directory of the pending root `root`, which is
    /// committed. The name is new: one that exists is not used.
    pub(in crate::source) fn create_scratch(&self, root: &str) -> Result<PathBuf> {
        let name = name_of(root)?;
        match rustix::fs::mkdirat(&*self.root, STAGING, Mode::from_raw_mode(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let staging = self.staging()?.context("the staging directory is gone")?;
        rustix::fs::mkdirat(&staging, name, Mode::from_raw_mode(0o700))
            .context("cannot make the scratch directory of the capture")?;
        Ok(self.path.join(STAGING).join(name))
    }

    /// Remove the scratch directory of `root` with everything in it. Absence
    /// is success. Every step goes from an opened directory to a name in it
    /// and follows no link; an entry of another user or on another file
    /// system, and a directory a capture still holds its lock on, stop the
    /// removal with an error and stay. A name with a second link is only
    /// dropped here (an installed file keeps its name in the store).
    pub(in crate::source) fn remove_scratch(&self, root: &str) -> Result<()> {
        let name = name_of(root)?;
        let Some(staging) = self.staging()? else {
            return Ok(());
        };
        let device = rustix::fs::fstat(&staging)?.st_dev;
        remove_entry(&staging, name.as_ref(), device, true)?;
        staging.sync_all()?;
        Ok(())
    }

    /// Record everything a capture is about to install as members of its
    /// pending root, in one transaction, before the first file is installed.
    /// A secret bundle keyed by source has no class in the index and is
    /// refused: the capture names the path of its bundle.
    pub(in crate::source) fn reserve_capture(
        &self,
        root: &str,
        intents: &[Intent],
    ) -> Result<Reserved> {
        let mut rows = Vec::with_capacity(intents.len());
        for intent in intents {
            let (key, path) = match &intent.kind {
                IntentKind::Object { hash, .. } => {
                    ensure!(
                        super::super::super::diff::is_blob_hash(hash),
                        "invalid source digest"
                    );
                    (
                        format!("b:{hash}"),
                        format!("objects/{}/{hash}", &hash[..2]),
                    )
                }
                IntentKind::Manifest { source_id, .. } => {
                    ensure!(
                        super::super::super::classify::source_id_of_ci(source_id).is_some(),
                        "invalid source manifest identity"
                    );
                    (
                        format!("m:{source_id}"),
                        format!("manifests/{source_id}.json"),
                    )
                }
                IntentKind::SecretBundle { .. } => {
                    anyhow::bail!("a secret bundle keyed by source cannot be reserved")
                }
            };
            ensure!(
                intent.destination.ends_with(&path),
                "a staged file is bound for a place the index does not name"
            );
            rows.push((intent.destination.clone(), key, path));
        }
        let mut c = self.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1 AND kind='pending' AND source IS NULL)",
            [root],
            |r| r.get(0),
        )?;
        ensure!(pending, "the pending root of the capture is gone");
        for (_, key, path) in &rows {
            Self::check_publication(&tx, key)?;
            tx.execute(
                "INSERT OR IGNORE INTO publication VALUES(?1,1,'live')",
                [key],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO object VALUES(?1,?2)",
                params![key, path],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO member VALUES(?1,?2)",
                params![root, key],
            )?;
        }
        tx.commit()?;
        Ok(rows.into_iter().map(|(to, key, _)| (to, key)).collect())
    }

    /// Give back the reservations of `root` for `keys`: an install that
    /// stopped part-way did not create those files, so the capture holds
    /// nothing of them.
    pub(in crate::source) fn unreserve_capture(&self, root: &str, keys: &[&str]) -> Result<()> {
        let mut c = self.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1 AND kind='pending' AND source IS NULL)",
            [root],
            |r| r.get(0),
        )?;
        ensure!(pending, "the pending root of the capture is gone");
        for key in keys {
            tx.execute(
                "DELETE FROM member WHERE root=?1 AND object=?2",
                params![root, key],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Whether the file of `object` is provably not in the store: its name,
    /// or a directory above it, does not exist. Anything that cannot be
    /// looked at is not absence.
    pub(super) fn destination_absent(&self, object: &str, path: &str) -> bool {
        let look = || -> Result<bool> {
            self.check_anchor()?;
            let parts = components(object, path)?;
            let mut dir = self.root.try_clone()?;
            for (at, part) in parts.iter().enumerate() {
                match rustix::fs::statat(&dir, part.as_str(), AtFlags::SYMLINK_NOFOLLOW) {
                    Err(rustix::io::Errno::NOENT) => return Ok(true),
                    Err(error) => return Err(error.into()),
                    Ok(_) => {}
                }
                if at + 1 < parts.len() {
                    dir = rustix::fs::openat(&dir, part.as_str(), DIR, Mode::empty())?.into();
                }
            }
            Ok(false)
        };
        look().unwrap_or(false)
    }
}

/// Whether an entry (its device and owner, as `lstat` gave them) is on the
/// file system of the staging directory and belongs to this user. Anything
/// else is a file system mounted inside, or somebody else's file: it stays.
fn is_ours<D: PartialEq>(entry: (D, u32), here: (D, u32)) -> bool {
    entry.0 == here.0 && entry.1 == here.1
}

#[cfg(test)]
mod tests {
    /// Another user's file cannot be made without root, and a mount not at
    /// all: the rule is checked on what `lstat` would say of them.
    #[test]
    fn an_entry_of_another_user_or_on_another_file_system_is_not_ours() {
        assert!(super::is_ours((7u64, 501), (7, 501)));
        assert!(!super::is_ours((7u64, 0), (7, 501)));
        assert!(!super::is_ours((8u64, 501), (7, 501)));
    }
}

fn names_in(dir: &File) -> Result<Vec<std::ffi::CString>> {
    let mut names = Vec::new();
    for entry in rustix::fs::Dir::read_from(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name != c"." && name != c".." {
            names.push(name.to_owned());
        }
    }
    Ok(names)
}

fn remove_entry(
    dir: &File,
    name: &std::ffi::OsStr,
    device: rustix::fs::Dev,
    top: bool,
) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let stat = match rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(rustix::io::Errno::NOENT) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        is_ours(
            (stat.st_dev, stat.st_uid),
            (device, rustix::process::geteuid().as_raw())
        ),
        "an entry of a capture's scratch directory is not provably ours"
    );
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        ensure!(!top, "a capture's scratch directory is not a directory");
        rustix::fs::unlinkat(dir, name, AtFlags::empty())?;
        return Ok(());
    }
    let below: File = rustix::fs::openat(dir, name, DIR, Mode::empty())?.into();
    let opened = rustix::fs::fstat(&below)?;
    ensure!(
        (opened.st_dev, opened.st_ino) == (stat.st_dev, stat.st_ino),
        "a capture's scratch directory changed while it was removed"
    );
    if top {
        // The capture that stages here holds this lock until it returns.
        match below.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                anyhow::bail!("a capture's scratch directory is in use")
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
    for child in names_in(&below)? {
        remove_entry(
            &below,
            std::ffi::OsStr::from_bytes(child.to_bytes()),
            device,
            false,
        )?;
    }
    rustix::fs::unlinkat(dir, name, AtFlags::REMOVEDIR)?;
    Ok(())
}
