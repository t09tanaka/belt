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

/// Why a deletion kept a capture's scratch directory (#155 (a)): it is
/// counted by kind in the log and in `doctor`, never with its path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::source) enum ScratchKept {
    /// An entry on another device or of another user, or the directory is
    /// no directory.
    NotOurs,
    /// The capture that stages there still holds its lock.
    InUse,
    /// An entry changed while it was removed.
    Changed,
}

impl ScratchKept {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotOurs => "not_ours",
            Self::InUse => "in_use",
            Self::Changed => "changed",
        }
    }
    fn parse(text: &str) -> Option<Self> {
        [Self::NotOurs, Self::InUse, Self::Changed]
            .into_iter()
            .find(|k| k.as_str() == text)
    }
}

impl std::fmt::Display for ScratchKept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotOurs => "an entry of a capture's scratch directory is not provably ours",
            Self::InUse => "a capture's scratch directory is in use",
            Self::Changed => "a capture's scratch directory changed while it was removed",
        })
    }
}

impl std::error::Error for ScratchKept {}

/// Scratch directories kept, by kind.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ScratchCounts {
    pub not_ours: u32,
    pub in_use: u32,
    pub changed: u32,
}

impl ScratchCounts {
    fn add_n(&mut self, kind: ScratchKept, by: u32) {
        let n = match kind {
            ScratchKept::NotOurs => &mut self.not_ours,
            ScratchKept::InUse => &mut self.in_use,
            ScratchKept::Changed => &mut self.changed,
        };
        *n = n.saturating_add(by);
    }
    /// Counts the kind of `error` when it is a kept scratch directory;
    /// whether it was one.
    pub(in crate::source) fn count(&mut self, error: &anyhow::Error) -> bool {
        match error.chain().find_map(|e| e.downcast_ref::<ScratchKept>()) {
            Some(kind) => {
                self.add_n(*kind, 1);
                true
            }
            None => false,
        }
    }
    pub fn merge(&mut self, other: Self) {
        self.not_ours = self.not_ours.saturating_add(other.not_ours);
        self.in_use = self.in_use.saturating_add(other.in_use);
        self.changed = self.changed.saturating_add(other.changed);
    }
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
    /// For a log line: counts only.
    pub fn text(&self) -> String {
        format!(
            "not_ours:{},in_use:{},changed:{}",
            self.not_ours, self.in_use, self.changed
        )
    }
}

/// For `doctor`: the scratch directories the deletions keep now, by kind
/// (one `meta` row per root, written by the deletion, removed when the
/// directory goes); read like [`super::super::inspect_owners`].
pub fn inspect_scratch_kept(state: &Path) -> Result<ScratchCounts> {
    let database = state.join("sources").join("retention.db");
    let mut counts = ScratchCounts::default();
    if !database.is_file() {
        return Ok(counts);
    }
    let c = Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(1))?;
    // A row whose root or directory went another way is not counted; the
    // next pass removes it (`Index::prune_scratch_kept`).
    let mut s = match c.prepare(
        "SELECT substr(key,14), value FROM meta WHERE key>'scratch_kept:' AND key<'scratch_kept;' AND EXISTS(SELECT 1 FROM root WHERE id=substr(meta.key,14))",
    ) {
        Ok(s) => s,
        Err(rusqlite::Error::SqliteFailure(_, Some(text))) if text.contains("no such table") => {
            return Ok(counts)
        }
        Err(error) => return Err(error.into()),
    };
    let store = state.join("sources");
    let rows = s.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (root, kind) = row?;
        if let Some(kind) = ScratchKept::parse(&kind) {
            if scratch_present(&store, &root) {
                counts.add_n(kind, 1);
            }
        }
    }
    Ok(counts)
}

/// Whether the scratch directory of `root` (or whatever stands at its name)
/// is still in the store at `store`.
fn scratch_present(store: &Path, root: &str) -> bool {
    name_of(root)
        .is_ok_and(|name| std::fs::symlink_metadata(store.join(STAGING).join(name)).is_ok())
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
        self.remove_scratch_of(root, Top::Probed)
    }

    /// [`Self::remove_scratch`] by a deletion that holds `c`: the kind of a
    /// kept directory is noted for `doctor` (one row per root), and the
    /// note goes when the directory does.
    pub(in crate::source) fn remove_scratch_noted(&self, c: &Connection, root: &str) -> Result<()> {
        let removed = self.remove_scratch(root);
        let kind = removed.as_ref().err().and_then(|e| {
            e.chain()
                .find_map(|e| e.downcast_ref::<ScratchKept>())
                .copied()
        });
        match (&removed, kind) {
            (Ok(()), _) => {
                c.execute("DELETE FROM meta WHERE key='scratch_kept:'||?1", [root])?;
            }
            (Err(_), Some(kind)) => {
                c.execute(
                    "INSERT INTO meta(key,value) VALUES('scratch_kept:'||?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE value<>excluded.value",
                    params![root, kind.as_str()],
                )?;
            }
            (Err(_), None) => {}
        }
        removed
    }

    /// Drop the `scratch_kept` notes whose directory or root went another
    /// way (the capture removed its own scratch, or the root row was
    /// deleted): a note lives no longer than what it counts.
    pub(in crate::source) fn prune_scratch_kept(&self, c: &Connection) -> Result<()> {
        let roots: Vec<String> = c
            .prepare(
                "SELECT substr(key,14) FROM meta WHERE key>'scratch_kept:' AND key<'scratch_kept;'",
            )?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for root in roots {
            if !scratch_present(&self.path, &root) {
                c.execute("DELETE FROM meta WHERE key='scratch_kept:'||?1", [&root])?;
            }
        }
        c.execute(
            "DELETE FROM meta WHERE key>'scratch_kept:' AND key<'scratch_kept;' AND NOT EXISTS(SELECT 1 FROM root WHERE id=substr(meta.key,14))",
            [],
        )?;
        Ok(())
    }

    /// [`Index::remove_scratch`] by the capture that made the directory,
    /// after local-ci returned and released its lock. The lock is not asked
    /// for: a process another thread forked may still hold a copy of
    /// local-ci's descriptor until it executes, and with it the lock.
    pub(in crate::source) fn remove_own_scratch(&self, root: &str) -> Result<()> {
        self.remove_scratch_of(root, Top::Own)
    }

    fn remove_scratch_of(&self, root: &str, top: Top) -> Result<()> {
        let name = name_of(root)?;
        let Some(staging) = self.staging()? else {
            return Ok(());
        };
        let device = rustix::fs::fstat(&staging)?.st_dev;
        remove_entry(&staging, name.as_ref(), device, Some(top))?;
        staging.sync_all()?;
        Ok(())
    }

    /// Record everything a capture is about to install as members of its
    /// pending root, in one transaction, before the first file is installed.
    /// A secret bundle keyed by source (`secrets/<source>.json`) is not
    /// reserved: it is no object of the index, and it has a rule of its own
    /// (it lives until its caller adopts it, and what a crash leaves is
    /// removed by `secrets::capture::sweep_store_copies`, under the lock of
    /// the captures that write one). The capture of a run never makes one: it
    /// names the path of its bundle.
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
                IntentKind::SecretBundle { .. } => continue,
            };
            ensure!(
                intent.destination == self.path.join(&path),
                "a staged file is bound for a place the index does not name"
            );
            rows.push((intent.destination.clone(), key, path));
        }
        let mut c = self.connect_foreground()?;
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
        let mut c = self.connect_foreground()?;
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
            // The rows the reservation alone made go with it (the rule of
            // the abandonment): no member and no claim of any kind is left.
            let unheld = "NOT EXISTS(SELECT 1 FROM member WHERE object=?1) AND NOT EXISTS(SELECT 1 FROM source_delete_claim WHERE object=?1)";
            crate::source::retention::Index::forget_object(&tx, key, unheld)?;
            tx.execute(
                &format!("DELETE FROM publication WHERE object=?1 AND {unheld}"),
                [key],
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

/// How the scratch directory itself is taken.
#[derive(Clone, Copy, PartialEq)]
enum Top {
    /// By the capture that made it.
    Own,
    /// By anyone else: only when no capture holds its lock.
    Probed,
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

#[cfg(test)]
type Hook = Option<Box<dyn FnMut(&std::ffi::OsStr)>>;

#[cfg(test)]
thread_local! {
    /// Run between the `lstat` of a directory and its `openat`, in tests.
    static BETWEEN: std::cell::RefCell<Hook> =
        const { std::cell::RefCell::new(None) };
}

fn remove_entry(
    dir: &File,
    name: &std::ffi::OsStr,
    device: rustix::fs::Dev,
    top: Option<Top>,
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
        ScratchKept::NotOurs
    );
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        ensure!(top.is_none(), ScratchKept::NotOurs);
        rustix::fs::unlinkat(dir, name, AtFlags::empty())?;
        return Ok(());
    }
    #[cfg(test)]
    BETWEEN.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(name)
        }
    });
    let below: File = rustix::fs::openat(dir, name, DIR, Mode::empty())?.into();
    let opened = rustix::fs::fstat(&below)?;
    ensure!(
        (opened.st_dev, opened.st_ino) == (stat.st_dev, stat.st_ino),
        ScratchKept::Changed
    );
    let _below_held = if top == Some(Top::Probed) {
        // The capture that stages here holds this lock until it returns.
        match crate::platform::flock::HeldBorrow::try_lock(&below) {
            Ok(held) => Some(held),
            Err(TryLockError::WouldBlock) => return Err(ScratchKept::InUse.into()),
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    } else {
        None
    };
    for child in names_in(&below)? {
        remove_entry(
            &below,
            std::ffi::OsStr::from_bytes(child.to_bytes()),
            device,
            None,
        )?;
    }
    rustix::fs::unlinkat(dir, name, AtFlags::REMOVEDIR)?;
    Ok(())
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

    fn opened(path: &std::path::Path) -> std::fs::File {
        std::fs::File::open(path).unwrap()
    }

    fn device(path: &std::path::Path) -> rustix::fs::Dev {
        rustix::fs::stat(path).unwrap().st_dev
    }

    /// An entry that is not on the file system the removal started on stays,
    /// and so does everything under it.
    #[test]
    fn an_entry_on_another_device_is_not_removed() {
        let root = tempfile::tempdir().unwrap();
        let scratch = root.path().join("s");
        std::fs::create_dir_all(scratch.join("objects")).unwrap();
        std::fs::write(scratch.join("objects/f"), b"x").unwrap();
        let other = device(root.path()).wrapping_add(1);
        let error = super::remove_entry(
            &opened(root.path()),
            "s".as_ref(),
            other,
            Some(super::Top::Own),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("not provably ours"),
            "{error:#}"
        );
        assert!(scratch.join("objects/f").exists());
    }

    /// A directory swapped for another between its `lstat` and its `openat`
    /// is not entered: neither it nor the one that was there is emptied.
    #[test]
    fn a_directory_swapped_while_it_is_removed_stays() {
        let root = tempfile::tempdir().unwrap();
        let scratch = root.path().join("s");
        std::fs::create_dir(&scratch).unwrap();
        std::fs::write(scratch.join("f"), b"x").unwrap();
        let aside = root.path().join("aside");
        let swap = root.path().join("swap");
        std::fs::create_dir(&swap).unwrap();
        std::fs::write(swap.join("g"), b"y").unwrap();
        let (from, to, other) = (scratch.clone(), aside.clone(), swap.clone());
        super::BETWEEN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_| {
                std::fs::rename(&from, &to).unwrap();
                std::fs::rename(&other, &from).unwrap();
            }))
        });
        let error = super::remove_entry(
            &opened(root.path()),
            "s".as_ref(),
            device(root.path()),
            Some(super::Top::Own),
        )
        .unwrap_err();
        super::BETWEEN.with(|hook| *hook.borrow_mut() = None);
        assert!(format!("{error:#}").contains("changed while"), "{error:#}");
        assert!(aside.join("f").exists());
        assert!(scratch.join("g").exists());
    }
}
