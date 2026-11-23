//! Persistent roots and cross-process exclusion for captured inputs.
//! Explicit parent retention pages are separate from the nondeleting preview.
mod capture_owner;
mod deletion;
mod foreground;
mod import_owner;
pub(crate) use capture_owner::stop_point;
pub use capture_owner::{inspect_corrupt_held, inspect_owners, CaptureOwner, CaptureVerdict};
pub use deletion::ImportReleaseReport;
pub(in crate::source) use deletion::Reserved;
pub use deletion::{AbandonCursor, AbandonReport, AbandonScope};
pub(crate) use foreground::ForegroundBusy;
pub use import_owner::{is_import_token_id, ForgetReport};
mod vault_retention;
use super::SourceStore;
use anyhow::{ensure, Context, Result};
pub use deletion::{RetentionCursor, RetentionPage};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::fs::{File, TryLockError};
use std::io::{self, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
pub(crate) use vault_retention::{
    check_vault_registration, check_vault_run, recover_vault_registration, VaultRegistration,
};
pub use vault_retention::{VaultCursor, VaultPage};

#[derive(Debug, Clone)]
pub struct Guard {
    _file: Arc<LockedFile>,
    pub(crate) root: PathBuf,
    index: Index,
}
#[derive(Debug)]
struct LockedFile(File);
impl Drop for LockedFile {
    fn drop(&mut self) {
        // A concurrent fork can retain this open file description until exec.
        // Release the lock when the last guard ends, even while that inherited
        // descriptor remains open; cloned guards still share this lifetime.
        let _ = self.0.unlock();
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Index {
    path: PathBuf,
    database: PathBuf,
    root: Arc<File>,
    /// The runs whose owner row this handle wrote ([`SourceStore::own_capture`]):
    /// their pending root is named by that row or is not created. One entry
    /// for each capture a command owns.
    owned: Arc<std::sync::Mutex<std::collections::BTreeSet<String>>>,
    #[cfg(test)]
    reads: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    verifications: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    page_pause: Arc<std::sync::Mutex<Option<PagePause>>>,
    #[cfg(test)]
    foreground_waits: Arc<std::sync::atomic::AtomicUsize>,
}
#[cfg(test)]
#[derive(Debug)]
struct PagePause {
    entered: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}
#[cfg(test)]
impl Index {
    fn pause_page(&self) {
        let pause = self.page_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            let _ = pause.resume.recv();
        }
    }
}
#[cfg(test)]
impl SourceStore {
    pub(crate) fn pause_retention_page(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, wait) = std::sync::mpsc::channel();
        let (resume, drain) = std::sync::mpsc::channel();
        *self.retention.page_pause.lock().unwrap() = Some(PagePause {
            entered,
            resume: drain,
        });
        (wait, resume)
    }
}
const DIR: rustix::fs::OFlags = rustix::fs::OFlags::RDONLY
    .union(rustix::fs::OFlags::DIRECTORY)
    .union(rustix::fs::OFlags::NOFOLLOW)
    .union(rustix::fs::OFlags::CLOEXEC);
fn open_permanent_file(root: &File, name: &str) -> Result<File> {
    let flags =
        rustix::fs::OFlags::RDWR | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC;
    // Give first creation one atomic winner instead of racing nonexclusive
    // O_CREAT lookups, which produced native ENOENT in the regression setup.
    let file = match rustix::fs::openat(
        root,
        name,
        flags | rustix::fs::OFlags::CREATE | rustix::fs::OFlags::EXCL,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    ) {
        Ok(file) => Ok(file),
        Err(rustix::io::Errno::EXIST) => {
            rustix::fs::openat(root, name, flags, rustix::fs::Mode::empty())
        }
        Err(error) => Err(error),
    }?;
    Ok(file.into())
}
impl Index {
    pub(crate) fn open(root: &Path) -> Result<Self> {
        Self::open_guarded(root, false).map(|(index, _guard)| index)
    }
    pub(crate) fn open_foreground(root: &Path) -> Result<(Self, Guard)> {
        Self::open_guarded(root, true)
    }
    fn open_guarded(root: &Path, foreground: bool) -> Result<(Self, Guard)> {
        let this = Self {
            path: root.into(),
            database: std::fs::canonicalize(root)
                .context("resolve source root")?
                .join("retention.db"),
            root: Arc::new(
                rustix::fs::open(root, DIR, rustix::fs::Mode::empty())
                    .context("open source root")?
                    .into(),
            ),
            owned: Default::default(),
            #[cfg(test)]
            reads: Default::default(),
            #[cfg(test)]
            verifications: Default::default(),
            #[cfg(test)]
            page_pause: Default::default(),
            #[cfg(test)]
            foreground_waits: Default::default(),
        };
        let guard = if foreground {
            this.foreground_guard()
        } else {
            this.guard(false)
        }
        .context("initial source guard")?;
        this.prepare_index_file()?;
        this.initialize_schema()
            .context("initialize source index")?;
        Ok((this, guard))
    }

    fn initialize_schema(&self) -> Result<()> {
        let mut db = self.connect().context("connect source index")?;
        let version: u32 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version == 1 {
            return self.ensure_capture_owner_table(&mut db);
        }
        ensure!(version == 0, "unknown source index version {version}");
        // A new index and an unversioned legacy index use the same idempotent
        // migration. Publish the marker atomically with the tables and rows.
        db.pragma_update(None, "synchronous", "FULL")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS root(id TEXT PRIMARY KEY,kind TEXT NOT NULL,source TEXT);
          CREATE INDEX IF NOT EXISTS root_kind ON root(kind,id);
          CREATE INDEX IF NOT EXISTS root_unknown ON root(source,kind,id);
          CREATE TABLE IF NOT EXISTS object(id TEXT PRIMARY KEY,path TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS member(root TEXT NOT NULL,object TEXT NOT NULL,PRIMARY KEY(root,object));
          CREATE INDEX IF NOT EXISTS member_object ON member(object,root);
          CREATE TABLE IF NOT EXISTS graph(root TEXT NOT NULL,source TEXT NOT NULL,done INTEGER NOT NULL DEFAULT 0,position INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(root,source));
          CREATE INDEX IF NOT EXISTS graph_pending ON graph(root,done,source);
          CREATE INDEX IF NOT EXISTS graph_incomplete ON graph(done,root,source);
          CREATE TABLE IF NOT EXISTS publication(object TEXT PRIMARY KEY,incarnation INTEGER NOT NULL CHECK(incarnation>0),state TEXT NOT NULL CHECK(state IN ('live','claimed')));
          CREATE INDEX IF NOT EXISTS publication_state ON publication(state,object);
          CREATE TABLE IF NOT EXISTS run_expiry(run_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,binding_digest TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS source_delete_claim(object TEXT NOT NULL,incarnation INTEGER NOT NULL,claim_json TEXT NOT NULL,done INTEGER NOT NULL CHECK(done IN (0,1)),PRIMARY KEY(object,incarnation));
          CREATE INDEX IF NOT EXISTS source_delete_pending ON source_delete_claim(done,object,incarnation);
          CREATE TABLE IF NOT EXISTS vault_delete_claim(run_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,binding_digest TEXT NOT NULL,claim_json TEXT NOT NULL,done INTEGER NOT NULL CHECK(done IN (0,1)));
          CREATE INDEX IF NOT EXISTS vault_delete_pending ON vault_delete_claim(done,run_id);
          CREATE TABLE IF NOT EXISTS import_session(token TEXT PRIMARY KEY,state TEXT NOT NULL CHECK(state IN ('pending','publishing','published','bound')),source TEXT,run_id TEXT);
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          INSERT OR IGNORE INTO meta VALUES('legacy_registration','pending');")?;
        tx.execute_batch(capture_owner::TABLE)?;
        tx.pragma_update(None, "user_version", 1)?;
        tx.commit()?;
        Ok(())
    }
    fn prepare_index_file(&self) -> Result<()> {
        use rustix::fs::{statat, AtFlags, FileType, Mode, OFlags, RenameFlags};
        // Closing any independently opened descriptor for a live SQLite inode
        // drops this process's POSIX locks. Existing databases are inspected
        // only with stat; SQLite alone owns their descriptors.
        match statat(&*self.root, "retention.db", AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => {}
            Err(rustix::io::Errno::NOENT) => {
                // Close our preparation descriptor BEFORE the inode becomes
                // the database name another connection can open.
                let temporary = format!(".retention-init-{}.tmp", ulid::Ulid::new());
                let file: File = rustix::fs::openat(
                    &*self.root,
                    &temporary,
                    OFlags::RDWR
                        | OFlags::CREATE
                        | OFlags::EXCL
                        | OFlags::NOFOLLOW
                        | OFlags::CLOEXEC,
                    Mode::RUSR | Mode::WUSR,
                )?
                .into();
                let synced = file.sync_all();
                drop(file);
                let published = synced.and_then(|()| {
                    rustix::fs::renameat_with(
                        &*self.root,
                        &temporary,
                        &*self.root,
                        "retention.db",
                        RenameFlags::NOREPLACE,
                    )
                    .map_err(io::Error::from)
                });
                if published.is_err() {
                    rustix::fs::unlinkat(&*self.root, &temporary, AtFlags::empty())
                        .context("remove unpublished source index temporary")?;
                }
                match published {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
                self.root.sync_all().context("sync source index parent")?;
            }
            Err(error) => return Err(error.into()),
        }
        let metadata = statat(&*self.root, "retention.db", AtFlags::SYMLINK_NOFOLLOW)?;
        ensure!(
            FileType::from_raw_mode(metadata.st_mode) == FileType::RegularFile
                && metadata.st_nlink == 1
                && metadata.st_uid == rustix::process::geteuid().as_raw()
                && metadata.st_mode & 0o077 == 0,
            "unsafe source index"
        );
        self.current()?;
        Ok(())
    }
    fn current(&self) -> Result<()> {
        let a = self.root.metadata()?;
        let b = std::fs::symlink_metadata(&self.path)?;
        ensure!(
            b.is_dir() && (a.dev(), a.ino()) == (b.dev(), b.ino()),
            "source root changed"
        );
        let canonical =
            std::fs::symlink_metadata(self.database.parent().context("source index parent")?)?;
        ensure!(
            canonical.is_dir() && (a.dev(), a.ino()) == (canonical.dev(), canonical.ino()),
            "canonical source root changed"
        );
        Ok(())
    }
    pub(crate) fn guard(&self, exclusive: bool) -> Result<Guard> {
        self.current().context("check source root for lock")?;
        let file = open_permanent_file(&self.root, "mutation.lock")
            .context("open source mutation lock")?;
        let m = file.metadata()?;
        ensure!(
            m.is_file()
                && m.nlink() == 1
                && m.uid() == rustix::process::geteuid().as_raw()
                && m.mode() & 0o077 == 0,
            "unsafe source mutation lock"
        );
        let lock = if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        lock.map_err(|e| match e {
            TryLockError::WouldBlock => io::Error::from(io::ErrorKind::WouldBlock),
            TryLockError::Error(e) => e,
        })
        .context("acquire source mutation lock")?;
        let opened = rustix::fs::fstat(&file)?;
        let named = rustix::fs::statat(
            &*self.root,
            "mutation.lock",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )
        .context("stat source mutation lock")?;
        ensure!(
            (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
            "source lock changed"
        );
        file.sync_all().context("sync source mutation lock")?;
        self.root
            .sync_all()
            .context("sync source mutation parent")?;
        Ok(Guard {
            _file: Arc::new(LockedFile(file)),
            root: self.path.clone(),
            index: self.clone(),
        })
    }
    fn connect(&self) -> Result<Connection> {
        self.connect_with_timeout(std::time::Duration::from_secs(1))
    }

    // Transfers from independent connections and processes publish short
    // transactions to the same index. Give foreground writes the five-second
    // busy budget used by the coordinator database. Maintenance keeps its
    // shorter budget so it can yield between pages.
    fn connect_for_import(&self) -> Result<Connection> {
        self.connect_with_timeout(std::time::Duration::from_secs(5))
    }

    fn connect_with_timeout(&self, timeout: std::time::Duration) -> Result<Connection> {
        self.current()?;
        let c = Connection::open_with_flags(
            &self.database,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        c.busy_timeout(timeout)?;
        Ok(c)
    }
    pub(crate) fn pending(&self, id: &str) -> Result<()> {
        self.connect()?.execute(
            "INSERT OR IGNORE INTO root(id,kind) VALUES(?1,'pending')",
            [id],
        )?;
        Ok(())
    }
    fn load_validated(&self, id: &str, limit: u64) -> Result<Validated> {
        ensure!(
            super::classify::source_id_of_ci(id).is_some(),
            "invalid source manifest identity"
        );
        let directory: File =
            rustix::fs::openat(&*self.root, "manifests", DIR, rustix::fs::Mode::empty())?.into();
        let file: File = rustix::fs::openat(
            &directory,
            format!("{id}.json"),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?
        .into();
        let metadata = file.metadata()?;
        ensure!(metadata.is_file(), "source manifest is not regular");
        if metadata.len() > limit {
            return Err(ManifestLimit {
                source_id: id.into(),
                bytes: metadata.len(),
                limit,
            }
            .into());
        }
        #[cfg(test)]
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Exactly the opened file's measured size plus one: growth cannot turn
        // a bounded background load into an unbounded serde stream.
        let manifest: local_ci::source::SourceManifest =
            serde_json::from_reader((&file).take(metadata.len().saturating_add(1)))?;
        ensure!(
            manifest.source_id == id,
            "source manifest identity mismatch"
        );
        super::store::verify_manifest(&manifest)?;
        #[cfg(test)]
        self.verifications
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let validated = Validated {
            manifest: Arc::new(manifest),
            file: Some(file),
            directory: Some(directory),
            metadata: Some(metadata),
        };
        validated.check(self)?;
        Ok(validated)
    }
    pub(crate) fn register_held(
        &self,
        root: &str,
        kind: &str,
        manifest: Arc<local_ci::source::SourceManifest>,
    ) -> Result<()> {
        if self.complete(root, &manifest.source_id)? {
            return Ok(());
        }
        super::store::verify_manifest(&manifest)?;
        #[cfg(test)]
        self.verifications
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let id = manifest.source_id.clone();
        let mut cache = Some(Validated {
            manifest,
            file: None,
            directory: None,
            metadata: None,
        });
        while !self.register_step(root, kind, &id, 32, &mut cache, u64::MAX)? {
            stop_point("registering");
        }
        Ok(())
    }
    fn complete(&self, root: &str, id: &str) -> Result<bool> {
        let found: Option<Option<String>> = self
            .connect()?
            .query_row("SELECT source FROM root WHERE id=?1", [root], |r| r.get(0))
            .optional()?;
        if let Some(Some(source)) = found {
            ensure!(source == id, "source root already bound");
            return Ok(true);
        }
        Ok(false)
    }
    /// Copy the producer's private completed pending root before run binding.
    /// The token is held by that capture; GC cannot expire any root under SH.
    pub(crate) fn copy_root(&self, origin: &str, root: &str, kind: &str, id: &str) -> Result<()> {
        if self.complete(root, id)? {
            return Ok(());
        }
        let mut c = self.connect()?;
        let source: Option<String> = c
            .query_row("SELECT source FROM root WHERE id=?1", [origin], |r| {
                r.get(0)
            })
            .optional()?
            .flatten();
        ensure!(
            source.as_deref() == Some(id),
            "source root handoff mismatch"
        );
        c.execute(
            "INSERT OR IGNORE INTO root(id,kind) VALUES(?1,'pending')",
            [root],
        )?;
        let mut after = String::new();
        loop {
            let keys: Vec<String> = {
                let mut s=c.prepare("SELECT object FROM member WHERE root=?1 AND object>?2 ORDER BY object LIMIT 32")?;
                let rows = s.query_map(params![origin, after], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            if keys.is_empty() {
                break;
            }
            after = keys.last().unwrap().clone();
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for key in keys {
                tx.execute(
                    "INSERT OR IGNORE INTO member VALUES(?1,?2)",
                    params![root, key],
                )?;
            }
            tx.commit()?;
        }
        c.execute(
            "UPDATE root SET source=?2,kind=?3 WHERE id=?1",
            params![root, id, kind],
        )?;
        Ok(())
    }
    /// Capture may finish its own graph; the resident uses bounded steps instead.
    pub(crate) fn register(&self, root: &str, kind: &str, id: &str) -> Result<()> {
        if self.complete(root, id)? {
            return Ok(());
        }
        let mut cache = None;
        while !self.register_step(root, kind, id, 32, &mut cache, u64::MAX)? {}
        Ok(())
    }
    fn register_step(
        &self,
        root: &str,
        kind: &str,
        id: &str,
        limit: u32,
        cache: &mut Option<Validated>,
        manifest_limit: u64,
    ) -> Result<bool> {
        let mut c = self.connect()?;
        let old: Option<Option<String>> = c
            .query_row("SELECT source FROM root WHERE id=?1", [root], |r| r.get(0))
            .optional()?;
        ensure!(
            old.flatten().as_deref().is_none_or(|old| old == id),
            "source root already bound"
        );
        c.execute(
            "INSERT OR IGNORE INTO root(id,kind) VALUES(?1,'pending')",
            [root],
        )?;
        c.execute(
            "INSERT OR IGNORE INTO graph(root,source) VALUES(?1,?2)",
            params![root, id],
        )?;
        for _ in 0..limit {
            // Finish the current manifest before newly discovered children,
            // including children whose source IDs sort before their parent.
            let preferred = cache
                .as_ref()
                .map(|cached| cached.manifest.source_id.as_str());
            let current: Option<(String, u32)> = if let Some(source) = preferred {
                c.query_row(
                    "SELECT source,position FROM graph WHERE root=?1 AND source=?2 AND done=0",
                    params![root, source],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?
            } else {
                None
            };
            let next=match current {Some(current)=>Some(current),None=>c.query_row("SELECT source,position FROM graph WHERE root=?1 AND done=0 ORDER BY source LIMIT 1",[root],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u32>(1)?))).optional()?};
            let Some((next, position)) = next else {
                c.execute(
                    "UPDATE root SET source=?2,kind=?3 WHERE id=?1",
                    params![root, id, kind],
                )?;
                *cache = None;
                return Ok(true);
            };
            // One validated manifest is retained between pages. Every page
            // checks FD identity, not the full JSON/digest again.
            if cache
                .as_ref()
                .is_none_or(|cached| cached.manifest.source_id != next)
            {
                *cache = Some(self.load_validated(&next, manifest_limit)?);
            }
            let cached = cache.as_ref().unwrap();
            cached.check(self)?;
            let m = &cached.manifest;
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let key = format!("m:{}", m.source_id);
            let path = format!("manifests/{}.json", m.source_id);
            Self::check_publication(&tx, &key)?;
            tx.execute(
                "INSERT OR IGNORE INTO publication VALUES(?1,1,'live')",
                [&key],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO object VALUES(?1,?2)",
                params![key, path],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO member VALUES(?1,?2)",
                params![root, key],
            )?;
            let total = m.object_manifest.entries.len() + m.submodules.len();
            let end = (position as usize + 32).min(total);
            for position in position as usize..end {
                if let Some(b) = m.object_manifest.entries.get(position) {
                    ensure!(super::diff::is_blob_hash(&b.hash), "invalid source digest");
                    let key = format!("b:{}", b.hash);
                    let path = format!("objects/{}/{}", &b.hash[..2], b.hash);
                    Self::check_publication(&tx, &key)?;
                    tx.execute(
                        "INSERT OR IGNORE INTO publication VALUES(?1,1,'live')",
                        [&key],
                    )?;
                    tx.execute(
                        "INSERT OR IGNORE INTO object VALUES(?1,?2)",
                        params![key, path],
                    )?;
                    tx.execute(
                        "INSERT OR IGNORE INTO member VALUES(?1,?2)",
                        params![root, key],
                    )?;
                } else if let Some(child) =
                    &m.submodules[position - m.object_manifest.entries.len()].source_id
                {
                    tx.execute(
                        "INSERT OR IGNORE INTO graph(root,source) VALUES(?1,?2)",
                        params![root, child],
                    )?;
                }
            }
            tx.execute(
                "UPDATE graph SET done=?3,position=?4 WHERE root=?1 AND source=?2",
                params![root, next, end == total, u32::try_from(end)?],
            )?;
            tx.commit()?;
        }
        Ok(false)
    }
    // Tokenless legacy imports deliberately keep their independent roots.
    // A published graph is not proof that some other import has handed off.
    fn release_import_page(&self, token: &ImportToken, received: &str) -> Result<bool> {
        self.check_token(token)?;
        let mut c = self.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let keys: Vec<String> = {
            let mut s = tx.prepare("SELECT a.object FROM member a WHERE a.root=?1 AND EXISTS(SELECT 1 FROM member b WHERE b.root=?2 AND b.object=a.object) ORDER BY a.object LIMIT 32")?;
            let rows = s.query_map(params![token.root(), received], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for key in &keys {
            tx.execute(
                "DELETE FROM member WHERE root=?1 AND object=?2",
                params![token.root(), key],
            )?;
        }
        let remains: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM member WHERE root=?1)",
            [token.root()],
            |r| r.get(0),
        )?;
        if !remains {
            tx.execute("DELETE FROM root WHERE id=?1", [token.root()])?;
        }
        tx.commit()?;
        Ok(keys.len() < 32)
    }
    fn check_token_store(&self, token: &ImportToken) -> Result<()> {
        self.current()?;
        let m = self.root.metadata()?;
        ensure!(
            (m.dev(), m.ino()) == token.store,
            "foreign source import token"
        );
        Ok(())
    }

    fn check_token(&self, token: &ImportToken) -> Result<()> {
        self.check_token_store(token)?;
        let exists: bool = self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM import_session WHERE token=?1)",
            [&token.id],
            |r| r.get(0),
        )?;
        ensure!(exists, "unknown source import token");
        Ok(())
    }
    pub(crate) fn import_blob(&self, token: &ImportToken, hash: &str) -> Result<()> {
        self.check_token_store(token)?;
        let mut c = self.connect_for_import()?;
        // Reserve the writer before reading. Competing Deferred readers cannot
        // both upgrade to a writer, even with SQLite's busy timeout installed.
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: bool = tx
            .query_row(
                "SELECT state='pending' FROM import_session WHERE token=?1",
                [&token.id],
                |r| r.get(0),
            )
            .optional()?
            .context("unknown source import token")?;
        ensure!(pending, "source import already published");
        let key = format!("b:{hash}");
        Self::check_publication(&tx, &key)?;
        tx.execute(
            "INSERT OR IGNORE INTO publication VALUES(?1,1,'live')",
            [&key],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO object VALUES(?1,?2)",
            params![key, format!("objects/{}/{hash}", &hash[..2])],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO member VALUES(?1,?2)",
            params![token.root(), key],
        )?;
        tx.commit()?;
        Ok(())
    }
    fn check_publication(c: &Connection, object: &str) -> Result<()> {
        let claimed: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM publication WHERE object=?1 AND state='claimed')",
            [object],
            |r| r.get(0),
        )?;
        ensure!(!claimed, "source publication unavailable: deletion claimed");
        Ok(())
    }
    pub(crate) fn publication(&self, object: &str) -> Result<PublicationIncarnation> {
        let c = self.connect()?;
        Self::check_publication(&c, object)?;
        let n: Option<i64> = c
            .query_row(
                "SELECT incarnation FROM publication WHERE object=?1",
                [object],
                |r| r.get(0),
            )
            .optional()?;
        Ok(PublicationIncarnation(u64::try_from(n.unwrap_or(1))?))
    }
    pub(crate) fn ensure_run(&self, run: &crate::ids::RunId) -> Result<()> {
        let expired: bool = self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_expiry WHERE run_id=?1)",
            [run.as_str()],
            |r| r.get(0),
        )?;
        ensure!(!expired, "retained source unavailable: run expired");
        Ok(())
    }
    /// Remove only a positively handed-off pending root, after a durable run root.
    pub(crate) fn handoff(&self, pending: &str, run: &str) -> Result<()> {
        let mut c = self.connect()?;
        let tx = c.transaction()?;
        let source: Option<String> = tx
            .query_row(
                "SELECT source FROM root WHERE id=?1 AND kind='run'",
                [run],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let pending_source: Option<String> = tx
            .query_row(
                "SELECT source FROM root WHERE id=?1 AND kind='pending'",
                [pending],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        ensure!(
            source.is_some() && source == pending_source,
            "source root handoff mismatch"
        );
        tx.commit()?;
        while !self.handoff_page(pending, run)? {
            stop_point("handing_off");
        }
        Ok(())
    }
    fn handoff_page(&self, pending: &str, run: &str) -> Result<bool> {
        let mut c = self.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Destination must contain each membership before its source is released.
        let keys: Vec<String> = {
            let mut s =
                tx.prepare("SELECT object FROM member WHERE root=?1 ORDER BY object LIMIT 32")?;
            let rows = s.query_map([pending], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for key in &keys {
            let copied: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM member WHERE root=?1 AND object=?2)",
                params![run, key],
                |r| r.get(0),
            )?;
            ensure!(copied, "incomplete source root handoff");
            tx.execute(
                "DELETE FROM member WHERE root=?1 AND object=?2",
                params![pending, key],
            )?;
        }
        let done = keys.len() < 32;
        if done {
            // The graph of a root is read only while the root exists: it
            // goes with the root, not after it (spec 25.1, 4).
            tx.execute("DELETE FROM graph WHERE root=?1", [pending])?;
            tx.execute("DELETE FROM root WHERE id=?1", [pending])?;
        }
        tx.commit()?;
        Ok(done)
    }
}
/// A publication generation is independent of content-addressed SourceID/hash.
/// B will advance it only after a durable old deletion claim is completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationIncarnation(u64);
impl PublicationIncarnation {
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Durable per-import ownership. Dropping a token never abandons its data.
#[derive(Debug, Clone)]
pub struct ImportToken {
    id: String,
    store: (u64, u64),
}
impl ImportToken {
    fn root(&self) -> String {
        format!("import:{}", self.id)
    }
    pub(crate) fn received_root(&self) -> String {
        format!("receive:{}", self.id)
    }
}
impl Guard {
    /// The SDK discovers content IDs while writing. Pending claims therefore
    /// fence the whole raw SDK operation until B completes their incarnations.
    pub(crate) fn ensure_sdk_available(&self) -> Result<()> {
        Self::check_sdk_available(&self.index.connect()?)
    }
    fn check_sdk_available(c: &Connection) -> Result<()> {
        let claimed: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM publication WHERE state='claimed')",
            [],
            |r| r.get(0),
        )?;
        ensure!(!claimed, "source SDK unavailable: deletion claimed");
        Ok(())
    }
    pub fn ensure_run_available(&self, run: &crate::ids::RunId) -> Result<()> {
        self.index.ensure_run(run)
    }
    pub fn ensure_source_available(&self, id: &str) -> Result<PublicationIncarnation> {
        ensure!(
            super::classify::source_id_of_ci(id).is_some(),
            "invalid source identity"
        );
        self.index.publication(&format!("m:{id}"))
    }
    pub fn ensure_blob_available(&self, hash: &str) -> Result<PublicationIncarnation> {
        ensure!(super::diff::is_blob_hash(hash), "invalid source digest");
        self.index.publication(&format!("b:{hash}"))
    }
}
impl SourceStore {
    /// Nonblocking; acquire before coordinator operations, never inside its actor/txn.
    pub fn mutation_guard(&self) -> Result<Guard> {
        self.retention.guard(false)
    }
    pub fn begin_import(&self) -> Result<ImportToken> {
        let _guard = self.mutation_guard()?;
        let metadata = self.retention.root.metadata()?;
        let token = ImportToken {
            id: ulid::Ulid::new().to_string(),
            store: (metadata.dev(), metadata.ino()),
        };
        let mut c = self.retention.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Check the outstanding claim and register this root atomically.
        Guard::check_sdk_available(&tx)?;
        tx.execute(
            "INSERT INTO import_session(token,state) VALUES(?1,'pending')",
            [&token.id],
        )?;
        tx.execute(
            // A receipt's membership is exact: `import_blob` writes the
            // object and the member in one claim-checked transaction, before
            // the file. It is not a root of unknown membership (spec 25.8).
            "INSERT INTO root(id,kind) VALUES(?1,'receipt')",
            [token.root()],
        )?;
        tx.commit()?;
        Ok(token)
    }
    pub(crate) fn import_publication_root(&self, token: &ImportToken, id: &str) -> Result<String> {
        self.retention.check_token(token)?;
        let mut c = self.retention.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, source): (String, Option<String>) = tx.query_row(
            "SELECT state,source FROM import_session WHERE token=?1",
            [&token.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            state != "bound" && source.as_deref().is_none_or(|s| s == id),
            "source import binding mismatch"
        );
        if state == "pending" {
            tx.execute(
                "UPDATE import_session SET state='publishing',source=?2 WHERE token=?1",
                params![token.id, id],
            )?;
        }
        tx.commit()?;
        Ok(token.received_root())
    }
    pub(crate) fn finish_import(&self, token: &ImportToken, id: &str) -> Result<()> {
        let root = token.received_root();
        ensure!(
            self.retention.complete(&root, id)?,
            "incomplete source import publication"
        );
        self.retention.connect()?.execute(
            "UPDATE import_session SET state='published',source=?2 WHERE token=?1",
            params![token.id, id],
        )?;
        while !self.retention.release_import_page(token, &root)? {}
        Ok(())
    }
    /// Handoff this import only after the exact run binding is durable.
    pub fn bind_import(
        &self,
        token: &ImportToken,
        run: &crate::ids::RunId,
        record: &super::store::RunRecord,
    ) -> Result<()> {
        let guard = self.mutation_guard()?;
        guard.ensure_run_available(run)?;
        self.retention.check_token(token)?;
        let mut c = self.retention.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, id, bound_run): (String, Option<String>, Option<String>) = tx.query_row(
            "SELECT state,source,run_id FROM import_session WHERE token=?1",
            [&token.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        ensure!(
            bound_run.as_deref().is_none_or(|id| id == run.as_str()),
            "source import already bound to another run"
        );
        ensure!(
            (state == "published" || state == "bound")
                && id.as_deref() == Some(record.ci_source_id.as_str()),
            "source import not published"
        );
        tx.execute(
            "UPDATE import_session SET run_id=?2 WHERE token=?1",
            params![token.id, run.as_str()],
        )?;
        tx.commit()?;
        let bound = self.bind_run(run, record)?;
        ensure!(bound == *record, "source import run binding mismatch");
        if state != "bound" {
            while !self
                .retention
                .handoff_page(&token.received_root(), run.as_str())?
            {}
            c.execute(
                "UPDATE import_session SET state='bound',run_id=?2 WHERE token=?1",
                params![token.id, run.as_str()],
            )?;
        }
        Ok(())
    }
    /// `run` is the run this capture is made for, if any: its owner row,
    /// when it has one, names the new root from the same transaction.
    pub(crate) fn capture_pending_root(&self, run: Option<&crate::ids::RunId>) -> Result<String> {
        let id = format!("capture:{}", ulid::Ulid::new());
        self.retention.pending_of_capture(&id, run)?;
        Ok(id)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("manifest {source_id} has {bytes} bytes; background registration limit is {limit}; increase PreviewCursor.manifest_byte_limit to register it")]
pub struct ManifestLimit {
    pub source_id: String,
    pub bytes: u64,
    pub limit: u64,
}
#[derive(Debug)]
struct Validated {
    manifest: Arc<local_ci::source::SourceManifest>,
    file: Option<File>,
    directory: Option<File>,
    metadata: Option<std::fs::Metadata>,
}
impl Validated {
    fn check(&self, index: &Index) -> Result<()> {
        index.current()?;
        if let (Some(file), Some(dir), Some(before)) = (&self.file, &self.directory, &self.metadata)
        {
            let after = file.metadata()?;
            ensure!(
                after.len() == before.len()
                    && after.mtime() == before.mtime()
                    && after.mtime_nsec() == before.mtime_nsec()
                    && after.ctime() == before.ctime()
                    && after.ctime_nsec() == before.ctime_nsec(),
                "source manifest changed during registration"
            );
            let opened = rustix::fs::fstat(file)?;
            let named = rustix::fs::statat(
                dir,
                format!("{}.json", self.manifest.source_id),
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            )?;
            ensure!(
                (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
                "source manifest identity changed"
            );
            let opened = rustix::fs::fstat(dir)?;
            let named = rustix::fs::statat(
                &*index.root,
                "manifests",
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            )?;
            ensure!(
                (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
                "source manifests directory changed"
            );
        }
        Ok(())
    }
}

/// Only a dry-run cursor/report: no root expiry, deletion claim, or filesystem removal.
#[derive(Default)]
pub struct PreviewCursor {
    run: Option<String>,
    object: String,
    legacy: Option<rustix::fs::Dir>,
    legacy_pending: Option<String>,
    validated: Option<Validated>,
    /// Default receiver limit (64 MiB) when None. Raising it explicitly permits
    /// registration of larger legacy inputs without changing source usability.
    pub manifest_byte_limit: Option<u64>,
}
#[derive(Debug, Default)]
pub struct Preview {
    /// True only after the persisted bounded legacy traversal is complete.
    pub registration_done: bool,
    pub held: bool,
    pub registered: u32,
    pub manifest_limit: Option<ManifestLimit>,
    pub run: Option<crate::ids::RunId>,
    pub eligible: bool,
    /// Proven owned paths unique to this root, at most 32 per page. Shared data is kept.
    pub candidate_paths: Vec<PathBuf>,
    pub shared: u32,
}
impl SourceStore {
    /// A bounded inspection page under the source lock. It never makes inputs unavailable.
    #[allow(clippy::too_many_arguments)]
    pub fn retention_preview(
        &self,
        owner: &crate::storage::CoordinatorStore,
        cursor: &mut PreviewCursor,
        wall: u64,
        mono: u64,
        boot: Option<String>,
        retention: u64,
    ) -> Result<Preview> {
        self.retention_preview_inner(owner, cursor, wall, mono, boot, retention, false)
    }

    /// Bounded nondeleting legacy registration only. No run-history proof or
    /// retention clock is touched. Unknown/oversized inputs remain protected.
    pub(crate) fn retention_registration_page(
        &self,
        owner: &crate::storage::CoordinatorStore,
        cursor: &mut PreviewCursor,
    ) -> Result<Preview> {
        match self.retention_preview_inner(owner, cursor, 0, 0, None, 0, true) {
            Ok(page) => Ok(page),
            Err(error) => {
                // A failed directory entry must never be skipped on the next
                // automatic pass and later mistaken for a closed traversal.
                let busy = error.chain().any(|cause| {
                    cause
                        .downcast_ref::<io::Error>()
                        .is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock)
                });
                if !busy {
                    *cursor = PreviewCursor::default();
                }
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn retention_preview_inner(
        &self,
        owner: &crate::storage::CoordinatorStore,
        cursor: &mut PreviewCursor,
        wall: u64,
        mono: u64,
        boot: Option<String>,
        retention: u64,
        registration_only: bool,
    ) -> Result<Preview> {
        let _guard = self.retention.guard(true)?;
        ensure!(
            owner
                .db
                .path()
                .parent()
                .map(|state| state.join("sources"))
                .as_deref()
                == Some(self.root()),
            "foreign coordinator source store"
        );
        if registration_only {
            owner.check_input_store(&self.retention.path, &self.retention.root)?;
        }
        let c = self.retention.connect()?;
        let backfill: String = c.query_row(
            "SELECT value FROM meta WHERE key='legacy_registration'",
            [],
            |r| r.get(0),
        )?;
        if backfill != "done" {
            if cursor.legacy_pending.is_none() {
                cursor.legacy_pending=c.query_row("SELECT substr(id,8) FROM root WHERE kind='pending' AND source IS NULL AND id LIKE 'legacy:%' ORDER BY id LIMIT 1",[],|r|r.get(0)).optional()?;
            }
            if let Some(id) = cursor.legacy_pending.clone() {
                let limit = cursor.manifest_byte_limit.unwrap_or(
                    crate::worker::blob_policy::BlobLimits::default().max_manifest_bytes,
                );
                let done = match self.retention.register_step(
                    &format!("legacy:{id}"),
                    "legacy",
                    &id,
                    1,
                    &mut cursor.validated,
                    limit,
                ) {
                    Ok(done) => done,
                    Err(error) => {
                        let limit = error.downcast::<ManifestLimit>()?;
                        return Ok(Preview {
                            held: true,
                            manifest_limit: Some(limit),
                            ..Default::default()
                        });
                    }
                };
                if done {
                    cursor.legacy_pending = None;
                }
                return Ok(Preview {
                    held: true,
                    registered: u32::from(done),
                    ..Default::default()
                });
            }
            if cursor.legacy.is_none() {
                let fd = rustix::fs::openat(
                    &*self.retention.root,
                    "manifests",
                    DIR,
                    rustix::fs::Mode::empty(),
                )?;
                cursor.legacy = Some(rustix::fs::Dir::read_from(&fd)?);
            }
            for _ in 0..32 {
                let Some(entry) = cursor.legacy.as_mut().unwrap().next() else {
                    c.execute(
                        "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                        [],
                    )?;
                    cursor.legacy = None;
                    break;
                };
                let entry = entry?;
                let name = entry.file_name().to_str()?;
                if name == "." || name == ".." {
                    continue;
                }
                let id = name
                    .strip_suffix(".json")
                    .context("unknown legacy source entry")?;
                ensure!(
                    super::classify::source_id_of_ci(id).is_some(),
                    "invalid legacy source id"
                );
                let indexed: bool = c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM object WHERE id=?1)",
                    [format!("m:{id}")],
                    |r| r.get(0),
                )?;
                if !indexed {
                    cursor.legacy_pending = Some(id.into());
                    break;
                }
            }
            return Ok(Preview {
                held: true,
                ..Default::default()
            });
        }
        if registration_only {
            return Ok(Preview {
                registration_done: true,
                ..Default::default()
            });
        }
        let unknown: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE source IS NULL AND kind NOT IN ('abandoned','receipt'))",
            [],
            |r| r.get(0),
        )?;
        if unknown {
            return Ok(Preview {
                held: true,
                ..Default::default()
            });
        }
        let previous: Option<String> = c
            .query_row("SELECT value FROM meta WHERE key='clock'", [], |r| r.get(0))
            .optional()?;
        let previous = previous
            .as_deref()
            .map(serde_json::from_str::<crate::results::clock_policy::ClockState>)
            .transpose()?;
        let step = crate::results::clock_policy::step(
            previous.as_ref(),
            boot.as_deref(),
            wall,
            mono,
            retention,
        );
        if let Some(state) = step.state {
            c.execute("INSERT INTO meta VALUES('clock',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(&state)?])?;
        }
        // A clock Hold must not consume candidate cursors: a later valid sample
        // still needs to inspect the same bounded page.
        if step.cutoff.is_none() {
            return Ok(Preview {
                held: true,
                ..Default::default()
            });
        }
        let run: Option<(String,String)> = c
            .query_row(
                "SELECT id,source FROM root WHERE kind='run' AND id>coalesce(?1,'') ORDER BY id LIMIT 1",
                [&cursor.run],
                |r| Ok((r.get(0)?,r.get(1)?)),
            )
            .optional()?;
        let Some((run, source)) = run else {
            cursor.run = None;
            cursor.object.clear();
            return Ok(Preview::default());
        };
        let parsed = crate::ids::RunId::parse(&run)?;
        let source =
            super::classify::source_id_of_ci(&source).context("invalid run source root")?;
        let ended = owner.input_retention_end(&parsed, &source).ok().flatten();
        let eligible =
            matches!((ended,step.cutoff),(Some(end),Some(now)) if end<=now && now-end>=retention);
        let mut report = Preview {
            run: Some(parsed),
            held: step.cutoff.is_none(),
            eligible,
            ..Default::default()
        };
        if !eligible {
            cursor.run = Some(run);
            cursor.object.clear();
            return Ok(report);
        }
        let objects: Vec<(String, String)> = {
            let mut s=c.prepare("SELECT o.id,o.path FROM member m JOIN object o ON o.id=m.object WHERE m.root=?1 AND m.object>?2 ORDER BY m.object LIMIT 32")?;
            let rows = s.query_map(params![run, cursor.object], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let more = objects.len() == 32;
        for (key, path) in objects {
            cursor.object = key.clone();
            let shared: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1 AND root<>?2)",
                params![key, run],
                |r| r.get(0),
            )?;
            if shared {
                report.shared += 1;
            } else if eligible {
                report.candidate_paths.push(PathBuf::from(path));
            }
        }
        if !more {
            cursor.run = Some(run);
            cursor.object.clear();
        }
        Ok(report)
    }
}

#[cfg(test)]
mod abandon_tests;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AttemptId, Digest, RunId, TaskId, WorkerId};
    use crate::source::{capture, capture_for_run, CaptureParams, SourceRequest};
    use crate::state::{AttemptState, Outcome, Termination};
    use crate::storage::{CoordinatorStore, NewAttempt};
    use sha2::Digest as _;
    use std::collections::BTreeMap;
    struct Fixture {
        _root: tempfile::TempDir,
        state: PathBuf,
        input: PathBuf,
        store: SourceStore,
        owner: CoordinatorStore,
    }
    impl Fixture {
        fn new() -> Self {
            let root = crate::test_tmp::tempdir().unwrap();
            let state = root.path().join("state");
            let input = root.path().join("original");
            std::fs::create_dir(&input).unwrap();
            std::fs::write(input.join("file"), b"original").unwrap();
            let store = SourceStore::open(&state).unwrap();
            let owner = CoordinatorStore::open(&state, 1).unwrap();
            Self {
                _root: root,
                state,
                input,
                store,
                owner,
            }
        }
        fn bind(&self, run: &RunId) {
            capture_for_run(
                &BTreeMap::new(),
                &self.store,
                run,
                &SourceRequest::Directory {
                    dir: self.input.clone(),
                },
                &CaptureParams::default(),
            )
            .unwrap();
        }
        /// Drive `attempt` from accepted to a closed, collected result; each
        /// step is stamped `from`, `from + 1`, ...
        fn settle(&self, attempt: &AttemptId, from: u64) {
            for state in [
                AttemptState::Preparing,
                AttemptState::Ready,
                AttemptState::Starting,
                AttemptState::Active,
                AttemptState::Finalizing,
            ] {
                self.owner.transition_attempt(attempt, state, from).unwrap();
            }
            self.owner
                .finalize_attempt(
                    attempt,
                    Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(9)), None)
                        .unwrap(),
                    from + 1,
                )
                .unwrap();
            self.owner
                .close_collection(
                    attempt,
                    None,
                    crate::results::plan::Verdict::GaveUp,
                    false,
                    from + 2,
                )
                .unwrap();
        }
        fn terminal(&self, run: &RunId) -> (TaskId, AttemptId) {
            let id = self.store.source_of_run(run).unwrap().unwrap();
            let source = super::super::classify::source_id_of_ci(&id).unwrap();
            let manifest = self.store.manifest(&id).unwrap().unwrap();
            let digest = Digest::parse(&manifest.manifest_digest)
                .or_else(|_| Digest::parse(&format!("sha256:{}", manifest.manifest_digest)))
                .unwrap();
            self.owner.put_source_ref(&source, &digest, 1).unwrap();
            self.owner
                .create_run(run, Some(&source), None, &serde_json::json!({}), 1)
                .unwrap();
            let task = TaskId::generate();
            let attempt = AttemptId::generate();
            self.owner
                .create_task(&task, run, &serde_json::json!({}), 1)
                .unwrap();
            self.owner
                .create_attempt(
                    NewAttempt {
                        attempt_id: attempt.clone(),
                        task_id: task.clone(),
                        worker_id: WorkerId::parse("test").unwrap(),
                        payload_hash: Digest::of_bytes(b"payload"),
                        permit_id: None,
                    },
                    2,
                )
                .unwrap();
            self.settle(&attempt, 3);
            (task, attempt)
        }
        /// `run`, bound to its source, with one task that has no attempt.
        fn unplaced_run(&self, run: &RunId, spec: &serde_json::Value) -> TaskId {
            let id = self.store.source_of_run(run).unwrap().unwrap();
            let source = super::super::classify::source_id_of_ci(&id).unwrap();
            let manifest = self.store.manifest(&id).unwrap().unwrap();
            let digest = Digest::parse(&manifest.manifest_digest)
                .or_else(|_| Digest::parse(&format!("sha256:{}", manifest.manifest_digest)))
                .unwrap();
            self.owner.put_source_ref(&source, &digest, 1).unwrap();
            self.owner
                .create_run(run, Some(&source), None, &serde_json::json!({}), 1)
                .unwrap();
            let task = TaskId::generate();
            self.owner.create_task(&task, run, spec, 1).unwrap();
            task
        }
        fn spare(&self, task: &TaskId, at: u64) -> AttemptId {
            let attempt = AttemptId::generate();
            self.owner
                .create_attempt(
                    NewAttempt {
                        attempt_id: attempt.clone(),
                        task_id: task.clone(),
                        worker_id: WorkerId::parse("test").unwrap(),
                        payload_hash: Digest::of_bytes(b"payload"),
                        permit_id: None,
                    },
                    at,
                )
                .unwrap();
            attempt
        }
        /// A second task of `run` whose first attempt was retired and
        /// confirmed (through the storage API, so it has no termination) and
        /// whose next attempt ended and was collected, at 13.
        fn retired_then_ended(&self, run: &RunId) -> TaskId {
            let task = TaskId::generate();
            self.owner
                .create_task(&task, run, &serde_json::json!({}), 6)
                .unwrap();
            let retired = self.spare(&task, 7);
            self.owner.begin_retire(&retired, "lost_ack", 8).unwrap();
            self.owner.confirm_retire(&retired, 9).unwrap();
            let next = self.spare(&task, 10);
            self.settle(&next, 11);
            let record = self.owner.get_attempt(&retired).unwrap().unwrap();
            assert!(record.retired_at_ms.is_some() && record.terminal_at_ms.is_none());
            task
        }
        /// A second task of `run` whose only attempt was retired: requested
        /// at 8 and confirmed at 20.
        fn only_retired(&self, run: &RunId) -> (TaskId, AttemptId) {
            let task = TaskId::generate();
            self.owner
                .create_task(&task, run, &serde_json::json!({}), 6)
                .unwrap();
            let lone = self.spare(&task, 7);
            self.owner.begin_retire(&lone, "lost_ack", 8).unwrap();
            (task, lone)
        }
        fn cancel_at(&self, task: &TaskId, at: u64) {
            let task = task.to_string();
            self.owner
                .db
                .call(move |c| {
                    c.execute(
                        "UPDATE task SET cancel_requested_at_ms=?1 WHERE task_id=?2",
                        rusqlite::params![at as i64, task],
                    )?;
                    Ok(())
                })
                .unwrap();
        }
        fn expired(&self, run: &RunId) -> bool {
            self.store
                .retention
                .connect()
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM run_expiry WHERE run_id=?1)",
                    [run.as_str()],
                    |r| r.get(0),
                )
                .unwrap()
        }
        fn source_end(&self, run: &RunId) -> Option<u64> {
            let source = super::super::classify::source_id_of_ci(
                &self.store.source_of_run(run).unwrap().unwrap(),
            )
            .unwrap();
            self.owner.input_retention_end(run, &source).unwrap()
        }
        fn preview(&self, cursor: &mut PreviewCursor, wall: u64, mono: u64) -> Preview {
            self.store
                .retention_preview(&self.owner, cursor, wall, mono, Some("boot".into()), 10)
                .unwrap()
        }
    }
    #[test]
    fn capture_pending_before_publication_and_crash_roots_are_retained() {
        let f = Fixture::new();
        let store = f.store.clone();
        let hook: local_ci::source::Hook = Arc::new(move |stage| {
            if matches!(stage, local_ci::source::Stage::BeforePublish) {
                let unknown: u32 = store.retention.connect()?.query_row(
                    "SELECT count(*) FROM root WHERE source IS NULL",
                    [],
                    |r| r.get(0),
                )?;
                assert_eq!(unknown, 1);
                assert!(store.retention.guard(true).is_err());
                anyhow::bail!("owned fault before publish");
            }
            Ok(())
        });
        assert!(capture(
            &BTreeMap::new(),
            &f.store,
            &SourceRequest::Directory {
                dir: f.input.clone()
            },
            &CaptureParams {
                hook: Some(hook),
                ..Default::default()
            }
        )
        .is_err());
        let reopened = SourceStore::open(&f.state).unwrap();
        let mut cursor = PreviewCursor::default();
        for n in 0..3 {
            assert!(
                reopened
                    .retention_preview(
                        &f.owner,
                        &mut cursor,
                        100 + n,
                        10 + n,
                        Some("boot".into()),
                        10
                    )
                    .unwrap()
                    .held
            );
        }
        assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
    }
    #[test]
    fn durable_run_handoff_and_shared_digest_membership_survive_reopen() {
        let f = Fixture::new();
        let a = RunId::generate();
        let b = RunId::generate();
        f.bind(&a);
        f.bind(&b);
        f.terminal(&a);
        f.terminal(&b);
        let store = SourceStore::open(&f.state).unwrap();
        let c = store.retention.connect().unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM root WHERE kind='pending'", [], |r| {
                r.get::<_, u32>(0)
            })
            .unwrap(),
            0
        );
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM member WHERE object LIKE 'b:%'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            2
        );
        let mut cursor = PreviewCursor::default();
        f.preview(&mut cursor, 100, 10);
        f.preview(&mut cursor, 100, 10);
        let report = f.preview(&mut cursor, 110, 20);
        assert!(report.eligible);
        assert!(report.candidate_paths.is_empty());
        assert_eq!(report.shared, 2);
        assert!(store.source_of_run(&a).unwrap().is_some());
    }
    #[test]
    fn preview_is_bounded_indexed_and_never_erases_terminal_failed_inputs() {
        let f = Fixture::new();
        for n in 0..70 {
            std::fs::write(f.input.join(format!("f{n:03}")), format!("unique-{n}")).unwrap();
        }
        let run = RunId::generate();
        f.bind(&run);
        let (_, attempt) = f.terminal(&run);
        let mut cursor = PreviewCursor::default();
        assert!(f.preview(&mut cursor, 100, 10).held);
        assert!(f.preview(&mut cursor, 100, 10).held);
        let mut sizes = Vec::new();
        for n in 0..3 {
            let report = f.preview(&mut cursor, 110 + n, 20 + n);
            assert!(report.eligible);
            sizes.push(report.candidate_paths.len());
            for path in report.candidate_paths {
                assert!(f.store.root().join(path).is_file());
            }
        }
        assert_eq!(sizes, vec![32, 32, 8]);
        assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
        assert_eq!(
            f.owner.collection(&attempt).unwrap().unwrap().verdict,
            Some(crate::results::plan::Verdict::GaveUp)
        );
        let plan: Vec<String> = {
            let c = f.store.retention.connect().unwrap();
            let mut s=c.prepare("EXPLAIN QUERY PLAN SELECT id FROM root WHERE kind='run' AND id>'' ORDER BY id LIMIT 1").unwrap();
            s.query_map([], |r| r.get(3))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        };
        assert!(plan.iter().any(|p| p.contains("root_kind")), "{plan:?}");
        assert!(!plan.iter().any(|p| p.contains("TEMP B-TREE")), "{plan:?}");
    }
    #[test]
    fn clock_and_all_generations_require_positive_proof() {
        let f = Fixture::new();
        let run = RunId::generate();
        f.bind(&run);
        let (task, attempt) = f.terminal(&run);
        let source =
            super::super::classify::source_id_of_ci(&f.store.source_of_run(&run).unwrap().unwrap())
                .unwrap();
        assert_eq!(
            f.owner
                .input_retention_end(&run, &crate::ids::SourceId::generate())
                .unwrap(),
            None
        );
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), Some(5));
        f.owner
            .set_observation(&attempt, crate::state::Observation::Unknown)
            .unwrap();
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), None);
        f.owner
            .set_observation(&attempt, crate::state::Observation::Known)
            .unwrap();
        let mut cursor = PreviewCursor::default();
        f.preview(&mut cursor, 100, 10);
        assert!(f.preview(&mut cursor, 100, 10).held);
        let jumped = f.preview(&mut cursor, 1_000_000, 11);
        assert!(jumped.held && !jumped.eligible);
        let queued = TaskId::generate();
        f.owner
            .create_task(&queued, &run, &serde_json::json!({}), 6)
            .unwrap();
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), None);
        assert!(f.owner.get_attempt(&attempt).unwrap().is_some());
        assert_ne!(task, queued);
    }
    /// A run whose task has a retired attempt before its result: the retire
    /// was confirmed (`retired = 1`), so the worker no longer reads the
    /// inputs and the end is reached; a retire that is only requested still
    /// holds the inputs.
    #[test]
    fn a_confirmed_retired_attempt_does_not_hold_the_inputs_but_an_unconfirmed_one_does() {
        let f = Fixture::new();
        let run = RunId::generate();
        f.bind(&run);
        let (_, first) = f.terminal(&run);
        let source =
            super::super::classify::source_id_of_ci(&f.store.source_of_run(&run).unwrap().unwrap())
                .unwrap();
        let spare = |task: &TaskId, at| {
            let attempt = AttemptId::generate();
            f.owner
                .create_attempt(
                    NewAttempt {
                        attempt_id: attempt.clone(),
                        task_id: task.clone(),
                        worker_id: WorkerId::parse("test").unwrap(),
                        payload_hash: Digest::of_bytes(b"payload"),
                        permit_id: None,
                    },
                    at,
                )
                .unwrap();
            attempt
        };
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), Some(5));
        // Second task: a retire is requested but not confirmed.
        let other = TaskId::generate();
        f.owner
            .create_task(&other, &run, &serde_json::json!({}), 6)
            .unwrap();
        let held = spare(&other, 7);
        f.owner.begin_retire(&held, "lost_ack", 8).unwrap();
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), None);
        // Confirmed: still no current attempt, so the task waits for a new one.
        f.owner.confirm_retire(&held, 9).unwrap();
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), None);
        // The new attempt ends: the retired one no longer matters.
        let second = spare(&other, 10);
        assert_eq!(f.owner.input_retention_end(&run, &source).unwrap(), None);
        f.settle(&second, 11);
        assert_eq!(
            f.owner.input_retention_end(&run, &source).unwrap(),
            Some(13)
        );
        assert!(f.owner.get_attempt(&first).unwrap().is_some());
    }
    /// A task whose only attempt was retired and confirmed has nothing left
    /// that could read the inputs once it is cancelled or its run is closed.
    /// The retention starts at the later of the two: cancelling (or closing)
    /// days after the retire still leaves the whole retention to retry in.
    #[test]
    fn a_task_with_only_retired_attempts_ends_at_the_later_of_the_retire_and_the_cancel() {
        // (cancelled at, run closed at, expected end); the retire is confirmed at 20.
        for (cancel, close, end) in [
            (Some(21), None, 21),
            (Some(20), None, 20),
            (Some(19), None, 20),
            (Some(90), None, 90),
            (None, Some(21), 21),
            (None, Some(19), 20),
            (Some(30), Some(40), 40),
            (Some(40), Some(30), 40),
        ] {
            let f = Fixture::new();
            let run = RunId::generate();
            f.bind(&run);
            f.terminal(&run);
            let (task, lone) = f.only_retired(&run);
            // Before the retire is confirmed: neither a cancel nor a close ends the run.
            if let Some(at) = cancel.filter(|at| *at < 20) {
                f.cancel_at(&task, at);
                assert_eq!(f.source_end(&run), None);
            }
            if let Some(at) = close.filter(|at| *at < 20) {
                f.owner.close_run(&run, at).unwrap();
                assert_eq!(f.source_end(&run), None);
            }
            f.owner.confirm_retire(&lone, 20).unwrap();
            if cancel.is_none_or(|at| at >= 20) && close.is_none_or(|at| at >= 20) {
                // Retired and neither cancelled nor closed: waiting for a new attempt.
                assert_eq!(f.source_end(&run), None);
            }
            if let Some(at) = cancel.filter(|at| *at >= 20) {
                f.cancel_at(&task, at);
            }
            if let Some(at) = close.filter(|at| *at >= 20) {
                f.owner.close_run(&run, at).unwrap();
            }
            assert_eq!(f.source_end(&run), Some(end), "{cancel:?} {close:?}");
        }
    }
    /// The cancel of a task that already ended moves the end too, as it does
    /// for the files the jobs of a run hand to each other.
    #[test]
    fn a_cancel_after_every_attempt_ended_moves_the_input_end() {
        let f = Fixture::new();
        let run = RunId::generate();
        f.bind(&run);
        let (task, _) = f.terminal(&run);
        assert_eq!(f.source_end(&run), Some(5));
        f.cancel_at(&task, 4);
        assert_eq!(f.source_end(&run), Some(5));
        f.cancel_at(&task, 6);
        assert_eq!(f.source_end(&run), Some(6));
        f.owner.close_run(&run, 7).unwrap();
        assert_eq!(f.source_end(&run), Some(7));
    }
    /// Spec 25.3 (a), rule 2. A run none of whose tasks was ever placed has
    /// no attempt to end: it ends when the last of them is cancelled, or
    /// when it is closed. Until then a task that waits to be placed, held or
    /// not, still reads the inputs.
    #[test]
    fn a_run_whose_tasks_were_never_placed_ends_at_their_cancel_or_at_its_close() {
        for held in [false, true] {
            let f = Fixture::new();
            let run = RunId::generate();
            f.bind(&run);
            let spec = serde_json::json!({"ci": {"hold": held}});
            let first = f.unplaced_run(&run, &spec);
            let second = TaskId::generate();
            f.owner.create_task(&second, &run, &spec, 2).unwrap();
            assert_eq!(f.source_end(&run), None, "both wait to be placed");
            f.cancel_at(&first, 30);
            assert_eq!(f.source_end(&run), None, "one still waits");
            f.cancel_at(&second, 20);
            assert_eq!(f.source_end(&run), Some(30), "the later cancel");

            let f = Fixture::new();
            let run = RunId::generate();
            f.bind(&run);
            f.unplaced_run(&run, &spec);
            f.owner.close_run(&run, 40).unwrap();
            assert_eq!(f.source_end(&run), Some(40), "a closed run places nothing");
        }
    }
    /// Spec 25.3 (a), rule 2. A probe whose discovery was applied and the
    /// origin of a matrix whose members were joined are never placed
    /// themselves: they hold the inputs only until then, and the end is the
    /// one of the tasks that ran.
    #[test]
    fn a_task_that_is_never_placed_itself_holds_the_inputs_only_until_it_is_settled() {
        for (pending, settled) in [
            (
                serde_json::json!({"ci": {"task": {"inputs": {"belt_discovery": {"applied": false}}}}}),
                serde_json::json!({"ci": {"task": {"inputs": {"belt_discovery": {"applied": true}}}}}),
            ),
            (
                serde_json::json!({"ci": {"dynamic_origin": true}}),
                serde_json::json!({"ci": {"dynamic_origin": true, "core_outcome": "success"}}),
            ),
        ] {
            let f = Fixture::new();
            let run = RunId::generate();
            f.bind(&run);
            f.terminal(&run);
            assert_eq!(f.source_end(&run), Some(5));
            let task = TaskId::generate();
            f.owner.create_task(&task, &run, &pending, 6).unwrap();
            assert_eq!(f.source_end(&run), None, "{pending}");
            let (id, spec) = (task.to_string(), settled.to_string());
            f.owner
                .db
                .call(move |c| {
                    c.execute(
                        "UPDATE task SET spec_json=?1 WHERE task_id=?2",
                        rusqlite::params![spec, id],
                    )?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(f.source_end(&run), Some(5), "{settled}");
        }
    }
    #[test]
    fn exclusive_preview_fences_enqueue_without_holding_coordinator_transaction() {
        let f = Fixture::new();
        let ex = f.store.retention.guard(true).unwrap();
        let run = RunId::generate();
        let task = TaskId::generate();
        let before = std::time::Instant::now();
        assert!(f
            .owner
            .enqueue(
                &run,
                None,
                &serde_json::json!({}),
                &[(task, serde_json::json!({}))],
                1
            )
            .is_err());
        assert!(before.elapsed() < std::time::Duration::from_secs(1));
        assert!(f.owner.query_run(run).unwrap().is_none());
        drop(ex);
        assert!(f.store.mutation_guard().is_ok());
    }
    #[test]
    fn legacy_registration_is_bounded_and_ancestor_symlink_fails_closed() {
        let root = crate::test_tmp::tempdir().unwrap();
        let input = root.path().join("original");
        std::fs::create_dir(&input).unwrap();
        std::fs::write(input.join("sentinel"), b"original").unwrap();
        let state = root.path().join("state");
        let captured = local_ci::source::capture_directory(
            &local_ci::ExecContext::new(BTreeMap::new()),
            &input,
            &local_ci::source::CaptureOptions {
                store: Some(state.join("sources")),
                ..Default::default()
            },
        )
        .unwrap();
        let store = SourceStore::open(&state).unwrap();
        let owner = CoordinatorStore::open(&state, 1).unwrap();
        let mut cursor = PreviewCursor::default();
        for n in 0..6 {
            let _ = store
                .retention_preview(
                    &owner,
                    &mut cursor,
                    100 + n,
                    10 + n,
                    Some("boot".into()),
                    10,
                )
                .unwrap();
        }
        let c = store.retention.connect().unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM root WHERE kind='legacy'", [], |r| r
                .get::<_, u32>(
                0
            ))
            .unwrap(),
            1
        );
        assert!(store
            .manifest(&captured.manifest.source_id)
            .unwrap()
            .is_some());
        // Owned rename and external alias: registration never follows an ancestor link.
        std::fs::rename(
            store.root().join("manifests"),
            store.root().join("saved-manifests"),
        )
        .unwrap();
        std::os::unix::fs::symlink(&input, store.root().join("manifests")).unwrap();
        c.execute(
            "UPDATE meta SET value='pending' WHERE key='legacy_registration'",
            [],
        )
        .unwrap();
        assert!(store
            .retention_preview(
                &owner,
                &mut PreviewCursor::default(),
                110,
                20,
                Some("boot".into()),
                10
            )
            .is_err());
        assert_eq!(std::fs::read(input.join("sentinel")).unwrap(), b"original");
    }
    #[test]
    fn concurrent_first_lock_open_keeps_one_inode_and_shared_exclusion() {
        // Keep the database out of this native first-creation regression:
        // every participant starts with the same retained directory FD, and
        // no participant ever removes or replaces the lock file.
        for _ in 0..32 {
            let dir = crate::test_tmp::tempdir().unwrap();
            let path = std::fs::canonicalize(dir.path()).unwrap();
            let index = Index {
                database: path.join("retention.db"),
                root: Arc::new(
                    rustix::fs::open(&path, DIR, rustix::fs::Mode::empty())
                        .unwrap()
                        .into(),
                ),
                path,
                owned: Default::default(),
                reads: Default::default(),
                verifications: Default::default(),
                page_pause: Default::default(),
                foreground_waits: Default::default(),
            };
            let gate = std::sync::Barrier::new(8);
            let guards = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|_| {
                        let index = &index;
                        let gate = &gate;
                        scope.spawn(move || {
                            gate.wait();
                            index.guard(false).unwrap()
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect::<Vec<_>>()
            });
            let named = std::fs::symlink_metadata(index.path.join("mutation.lock")).unwrap();
            for guard in &guards {
                let opened = guard._file.0.metadata().unwrap();
                assert_eq!((opened.dev(), opened.ino()), (named.dev(), named.ino()));
            }
            assert!(index.guard(true).is_err());
            drop(guards);
            let exclusive = index.guard(true).unwrap();
            assert!(index.guard(false).is_err());
            drop(exclusive);
            assert!(index.guard(false).is_ok());
        }
    }
    #[test]
    fn concurrent_first_index_file_open_keeps_one_private_inode() {
        for _ in 0..32 {
            let dir = crate::test_tmp::tempdir().unwrap();
            let path = std::fs::canonicalize(dir.path()).unwrap();
            let index = Index {
                database: path.join("retention.db"),
                root: Arc::new(
                    rustix::fs::open(&path, DIR, rustix::fs::Mode::empty())
                        .unwrap()
                        .into(),
                ),
                path,
                owned: Default::default(),
                reads: Default::default(),
                verifications: Default::default(),
                page_pause: Default::default(),
                foreground_waits: Default::default(),
            };
            let gate = std::sync::Barrier::new(8);
            let identities = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|_| {
                        let index = &index;
                        let gate = &gate;
                        scope.spawn(move || {
                            gate.wait();
                            index.prepare_index_file().unwrap();
                            let metadata = rustix::fs::statat(
                                &*index.root,
                                "retention.db",
                                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                            )
                            .unwrap();
                            (metadata.st_dev, metadata.st_ino)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .collect::<Vec<_>>()
            });
            let named = rustix::fs::statat(
                &*index.root,
                "retention.db",
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            )
            .unwrap();
            assert_eq!(named.st_mode & 0o777, 0o600);
            assert_eq!(named.st_nlink, 1);
            for (device, inode) in identities {
                assert_eq!((device, inode), (named.st_dev, named.st_ino));
            }
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }
    #[test]
    fn source_index_aliases_and_public_file_fail_before_sqlite_open() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        for kind in ["symlink", "hardlink", "public"] {
            let f = Fixture::new();
            let sentinel = f.input.join("sentinel");
            std::fs::write(&sentinel, b"untouched non-database").unwrap();
            std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600)).unwrap();
            let path = f.store.root().join("retention.db");
            std::fs::rename(&path, f.store.root().join("saved.db")).unwrap();
            match kind {
                "symlink" => symlink(&sentinel, &path).unwrap(),
                "hardlink" => std::fs::hard_link(&sentinel, &path).unwrap(),
                "public" => {
                    std::fs::write(&path, b"public non-database").unwrap();
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let error = Index::open(f.store.root()).unwrap_err();
            assert!(
                error.to_string().contains("unsafe source index"),
                "{error:#}"
            );
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"untouched non-database");
        }
    }
    #[test]
    fn mutation_lock_aliases_and_retargeted_source_root_fail_closed() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        for hardlink in [false, true] {
            let f = Fixture::new();
            let sentinel = f.input.join("sentinel");
            std::fs::write(&sentinel, b"private sentinel").unwrap();
            std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600)).unwrap();
            let lock = f.store.root().join("mutation.lock");
            std::fs::rename(&lock, f.store.root().join("saved.lock")).unwrap();
            if hardlink {
                std::fs::hard_link(&sentinel, &lock).unwrap();
            } else {
                symlink(&sentinel, &lock).unwrap();
            }
            assert!(f.store.mutation_guard().is_err());
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"private sentinel");
        }
        let f = Fixture::new();
        let retained = f.store.mutation_guard().unwrap();
        let root = f.store.root();
        std::fs::rename(root, root.with_file_name("saved-sources")).unwrap();
        std::fs::create_dir(root).unwrap();
        assert!(f.store.mutation_guard().is_err());
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
        assert_eq!(retained._file.0.metadata().unwrap().nlink(), 1);
    }
    #[test]
    fn last_guard_unlocks_even_when_an_inherited_descriptor_remains_open() {
        let f = Fixture::new();
        for exclusive in [false, true] {
            let guard = f.store.retention.guard(exclusive).unwrap();
            let inherited = guard._file.0.try_clone().unwrap();
            let cloned = guard.clone();
            drop(guard);
            assert!(f.store.retention.guard(true).is_err());
            let independent = (!exclusive).then(|| f.store.mutation_guard().unwrap());
            drop(cloned);
            if independent.is_some() {
                assert!(f.store.retention.guard(true).is_err());
            }
            drop(independent);
            assert!(f.store.retention.guard(true).is_ok());
            drop(inherited);
        }
    }
    #[test]
    #[ignore = "owned SQLite lock probe subprocess"]
    fn source_index_writer_probe_child() {
        let path = std::env::var_os("BELT_SOURCE_INDEX_PROBE").expect("private probe database");
        let db = Connection::open_with_flags(
            Path::new(&path),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .unwrap();
        db.busy_timeout(std::time::Duration::ZERO).unwrap();
        match db.execute_batch("BEGIN IMMEDIATE") {
            Ok(()) => {
                db.execute_batch("ROLLBACK").unwrap();
                println!("BELT_INDEX_PROBE=ACQUIRED");
            }
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DatabaseBusy =>
            {
                println!("BELT_INDEX_PROBE=BUSY");
            }
            other => panic!("unexpected SQLite lock probe: {other:?}"),
        }
    }
    #[test]
    fn unversioned_source_indexes_keep_existing_registration_data_on_upgrade() {
        let f = Fixture::new();
        let db = f.store.retention.connect().unwrap();
        db.execute(
            "UPDATE meta SET value='done' WHERE key='legacy_registration'",
            [],
        )
        .unwrap();
        db.pragma_update(None, "user_version", 0).unwrap();
        Index::open(f.store.root()).unwrap();
        let version: u32 = db
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1);
        let registration: String = db
            .query_row(
                "SELECT value FROM meta WHERE key='legacy_registration'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(registration, "done");
    }

    #[test]
    fn opening_another_index_preserves_live_sqlite_writer_exclusion() {
        fn probe(path: &Path) -> String {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "source::retention::tests::source_index_writer_probe_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("BELT_SOURCE_INDEX_PROBE", path)
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            String::from_utf8(output.stdout).unwrap()
        }
        let f = Fixture::new();
        let first = f.store.retention.connect().unwrap();
        first.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(probe(&f.store.retention.database).contains("BELT_INDEX_PROBE=BUSY"));
        // The schema is already initialized. Opening it succeeds while the
        // writer is active and never releases that writer's exclusion.
        let reopened = Index::open(f.store.root()).unwrap();
        drop(reopened);
        assert!(!first.is_autocommit());
        let after = probe(&f.store.retention.database);
        assert!(after.contains("BELT_INDEX_PROBE=BUSY"), "{after}");
        first.execute_batch("ROLLBACK").unwrap();
        assert!(probe(&f.store.retention.database).contains("BELT_INDEX_PROBE=ACQUIRED"));
    }
    #[test]
    #[ignore = "owned helper subprocess"]
    fn source_lock_child() {
        use std::io::{Read, Write};
        let state = std::env::var_os("BELT_SOURCE_LOCK_HELPER").expect("helper path");
        let store = SourceStore::open(Path::new(&state)).unwrap();
        let _guard = store.mutation_guard().unwrap();
        println!("READY");
        std::io::stdout().flush().unwrap();
        let mut bytes = Vec::new();
        std::io::stdin().read_to_end(&mut bytes).unwrap();
    }
    #[test]
    fn source_shared_lock_excludes_another_process_until_drain() {
        use std::io::BufRead;
        let f = Fixture::new();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "source::retention::tests::source_lock_child",
                "--ignored",
                "--nocapture",
            ])
            .env("BELT_SOURCE_LOCK_HELPER", &f.state)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut out = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            assert!(out.read_line(&mut line).unwrap() > 0);
            if line.contains("READY") {
                break;
            }
            line.clear();
        }
        assert!(f.store.retention.guard(true).is_err());
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        drop(f.store.retention.guard(true).unwrap());
    }
    #[test]
    fn preview_rejects_foreign_coordinator_and_misnamed_manifest_identity() {
        let f = Fixture::new();
        let other = Fixture::new();
        assert!(f
            .store
            .retention_preview(
                &other.owner,
                &mut PreviewCursor::default(),
                100,
                10,
                Some("boot".into()),
                10
            )
            .is_err());
        let first = RunId::generate();
        f.bind(&first);
        let a = f.store.source_of_run(&first).unwrap().unwrap();
        std::fs::write(f.input.join("file"), b"different-private-fixture").unwrap();
        let second = RunId::generate();
        f.bind(&second);
        let b = f.store.source_of_run(&second).unwrap().unwrap();
        assert_ne!(a, b);
        std::fs::copy(
            f.store.object_store().unwrap().manifest_path(&b),
            f.store.object_store().unwrap().manifest_path(&a),
        )
        .unwrap();
        assert!(f
            .store
            .retention
            .load_validated(&a, u64::MAX)
            .unwrap_err()
            .to_string()
            .contains("identity mismatch"));
    }
    #[test]
    fn source_root_symlink_is_rejected_before_external_store_creation() {
        let root = crate::test_tmp::tempdir().unwrap();
        let state = root.path().join("state");
        let original = root.path().join("original");
        std::fs::create_dir(&state).unwrap();
        std::fs::create_dir(&original).unwrap();
        std::fs::write(original.join("sentinel"), b"kept").unwrap();
        std::os::unix::fs::symlink(&original, state.join("sources")).unwrap();
        assert!(SourceStore::open(&state).is_err());
        assert!(!original.join("objects").exists());
        assert_eq!(std::fs::read(original.join("sentinel")).unwrap(), b"kept");
    }
    #[test]
    fn foreground_indexes_held_manifest_once_and_run_bind_never_rereads_it() {
        use std::sync::atomic::Ordering::SeqCst;
        let f = Fixture::new();
        for n in 0..130 {
            std::fs::write(f.input.join(format!("file-{n:03}")), format!("value-{n}")).unwrap();
        }
        let run = RunId::generate();
        f.bind(&run);
        assert_eq!(
            f.store.retention.reads.load(SeqCst),
            0,
            "producer manifest was read from disk again"
        );
        assert_eq!(
            f.store.retention.verifications.load(SeqCst),
            1,
            "index verification repeated per reference page or run binding"
        );
        assert!(f.store.source_of_run(&run).unwrap().is_some());
    }
    #[test]
    fn legacy_manifest_is_parsed_verified_once_across_pages_and_once_after_restart() {
        use std::sync::atomic::Ordering::SeqCst;
        let root = crate::test_tmp::tempdir().unwrap();
        let state = root.path().join("state");
        let input = root.path().join("original");
        std::fs::create_dir(&input).unwrap();
        for n in 0..130 {
            std::fs::write(input.join(format!("file-{n:03}")), format!("value-{n}")).unwrap();
        }
        let capture = local_ci::source::capture_directory(
            &local_ci::ExecContext::new(BTreeMap::new()),
            &input,
            &local_ci::source::CaptureOptions {
                store: Some(state.join("sources")),
                ..Default::default()
            },
        )
        .unwrap();
        let store = SourceStore::open(&state).unwrap();
        let owner = CoordinatorStore::open(&state, 1).unwrap();
        let mut cursor = PreviewCursor::default();
        for n in 0..2 {
            store
                .retention_preview(
                    &owner,
                    &mut cursor,
                    100 + n,
                    10 + n,
                    Some("boot".into()),
                    10,
                )
                .unwrap();
        }
        assert_eq!(store.retention.reads.load(SeqCst), 1);
        assert_eq!(store.retention.verifications.load(SeqCst), 1);
        let c = store.retention.connect().unwrap();
        let root_id = format!("legacy:{}", capture.manifest.source_id);
        assert_eq!(
            c.query_row(
                "SELECT position FROM graph WHERE root=?1 AND source=?2",
                params![root_id, capture.manifest.source_id],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            32
        );
        assert!(c
            .query_row("SELECT source FROM root WHERE id=?1", [&root_id], |r| {
                r.get::<_, Option<String>>(0)
            })
            .unwrap()
            .is_none());
        // Discard the in-memory validated cursor as on owner restart. The
        // durable root stays pending; a fresh cursor reads once and resumes64.
        drop(cursor);
        let mut cursor = PreviewCursor::default();
        store
            .retention_preview(&owner, &mut cursor, 102, 12, Some("boot".into()), 10)
            .unwrap();
        assert_eq!(
            c.query_row(
                "SELECT position FROM graph WHERE root=?1 AND source=?2",
                params![root_id, capture.manifest.source_id],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            64
        );
        for n in 3..12 {
            store
                .retention_preview(
                    &owner,
                    &mut cursor,
                    100 + n,
                    10 + n,
                    Some("boot".into()),
                    10,
                )
                .unwrap();
        }
        assert_eq!(
            store.retention.reads.load(SeqCst),
            2,
            "manifest reread for a 32-reference page"
        );
        assert_eq!(store.retention.verifications.load(SeqCst), 2);
        assert_eq!(
            c.query_row("SELECT kind FROM root WHERE id=?1", [root_id], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "legacy"
        );
        assert_eq!(std::fs::read(input.join("file-000")).unwrap(), b"value-0");
    }
    #[test]
    fn oversized_legacy_is_typed_recoverable_hold_and_explicit_limit_resumes() {
        use std::sync::atomic::Ordering::SeqCst;
        let root = crate::test_tmp::tempdir().unwrap();
        let state = root.path().join("state");
        let input = root.path().join("original");
        std::fs::create_dir(&input).unwrap();
        std::fs::write(input.join("file"), b"kept").unwrap();
        let capture = local_ci::source::capture_directory(
            &local_ci::ExecContext::new(BTreeMap::new()),
            &input,
            &local_ci::source::CaptureOptions {
                store: Some(state.join("sources")),
                ..Default::default()
            },
        )
        .unwrap();
        let store = SourceStore::open(&state).unwrap();
        let owner = CoordinatorStore::open(&state, 1).unwrap();
        let mut cursor = PreviewCursor {
            manifest_byte_limit: Some(1),
            ..Default::default()
        };
        store
            .retention_preview(&owner, &mut cursor, 100, 10, Some("boot".into()), 10)
            .unwrap();
        let report = store
            .retention_preview(&owner, &mut cursor, 101, 11, Some("boot".into()), 10)
            .unwrap();
        assert!(report.held && !report.eligible && report.candidate_paths.is_empty());
        let diagnostic = report.manifest_limit.unwrap();
        assert_eq!(diagnostic.source_id, capture.manifest.source_id);
        assert_eq!(diagnostic.limit, 1);
        assert!(diagnostic.bytes > 1);
        assert_eq!(store.retention.reads.load(SeqCst), 0);
        cursor.manifest_byte_limit = Some(diagnostic.bytes);
        for n in 2..7 {
            store
                .retention_preview(
                    &owner,
                    &mut cursor,
                    100 + n,
                    10 + n,
                    Some("boot".into()),
                    10,
                )
                .unwrap();
        }
        assert_eq!(store.retention.reads.load(SeqCst), 1);
        assert_eq!(store.retention.verifications.load(SeqCst), 1);
        assert!(store
            .manifest(&capture.manifest.source_id)
            .unwrap()
            .is_some());
        assert_eq!(std::fs::read(input.join("file")).unwrap(), b"kept");
    }
    #[test]
    fn queued_children_before_large_parent_never_preempt_its_validated_cursor() {
        use std::sync::atomic::Ordering::SeqCst;
        let f = Fixture::new();
        let base = capture(
            &BTreeMap::new(),
            &f.store,
            &SourceRequest::Directory {
                dir: f.input.clone(),
            },
            &CaptureParams::default(),
        )
        .unwrap();
        let child = f
            .store
            .manifest(&base.manifest.ci_source_id)
            .unwrap()
            .unwrap();
        let mut children = (0..128)
            .map(|n| {
                let mut m = child.clone();
                m.branch = Some(format!("child-{n}"));
                local_ci::source::seal_manifest(m)
            })
            .collect::<Vec<_>>();
        children.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        children.truncate(65);
        for n in 0..130 {
            std::fs::write(
                f.input.join(format!("parent-{n:03}")),
                format!("parent-value-{n}"),
            )
            .unwrap();
        }
        let captured = capture(
            &BTreeMap::new(),
            &f.store,
            &SourceRequest::Directory {
                dir: f.input.clone(),
            },
            &CaptureParams::default(),
        )
        .unwrap();
        let mut parent = f
            .store
            .manifest(&captured.manifest.ci_source_id)
            .unwrap()
            .unwrap();
        parent.submodules = children
            .iter()
            .enumerate()
            .map(|(n, m)| local_ci::source::SubmoduleRef {
                path: local_ci::source::RelPath(format!("sub-{n:03}").into_bytes()),
                revision: "0".repeat(40),
                source_id: Some(m.source_id.clone()),
                dirty: true,
            })
            .collect();
        parent = (0..1000)
            .find_map(|n| {
                let mut m = parent.clone();
                m.branch = Some(format!("parent-{n}"));
                let m = local_ci::source::seal_manifest(m);
                (m.source_id > children.last().unwrap().source_id).then_some(m)
            })
            .expect("parent after all65 child IDs");
        let _guard = f.store.mutation_guard().unwrap();
        for manifest in children.iter().chain(std::iter::once(&parent)) {
            super::super::store::verify_manifest(manifest).unwrap();
            std::fs::write(
                f.store
                    .object_store()
                    .unwrap()
                    .manifest_path(&manifest.source_id),
                serde_json::to_vec(manifest).unwrap(),
            )
            .unwrap();
        }
        f.store.retention.reads.store(0, SeqCst);
        f.store.retention.verifications.store(0, SeqCst);
        f.store
            .retention
            .register_held("foreground-parent", "pending", Arc::new(parent.clone()))
            .unwrap();
        assert_eq!(
            f.store.retention.reads.load(SeqCst),
            65,
            "held parent was evicted and reread"
        );
        assert_eq!(f.store.retention.verifications.load(SeqCst), 66);
        f.store.retention.reads.store(0, SeqCst);
        f.store.retention.verifications.store(0, SeqCst);
        let mut cache = None;
        let mut completed = false;
        for _ in 0..200 {
            if f.store
                .retention
                .register_step(
                    "legacy-parent",
                    "legacy",
                    &parent.source_id,
                    1,
                    &mut cache,
                    64 << 20,
                )
                .unwrap()
            {
                completed = true;
                break;
            }
        }
        assert!(completed);
        assert_eq!(
            f.store.retention.reads.load(SeqCst),
            66,
            "parent reparsed after child insert page"
        );
        assert_eq!(f.store.retention.verifications.load(SeqCst), 66);
    }
    #[test]
    fn import_tokens_do_not_release_another_owner_of_the_same_digest() {
        let f = Fixture::new();
        let run = RunId::generate();
        f.bind(&run);
        let id = f.store.source_of_run(&run).unwrap().unwrap();
        let m = f.store.manifest(&id).unwrap().unwrap();
        let entry = &m.object_manifest.entries[0];
        let want = super::super::diff::BlobWant {
            hash: entry.hash.clone(),
            size: entry.size,
        };
        let bytes = f.store.read_blob(&entry.hash).unwrap();
        let state = f._root.path().join("receiver");
        let receiver = SourceStore::open(&state).unwrap();
        let a = receiver.begin_import().unwrap();
        let b = receiver.begin_import().unwrap();
        receiver
            .ingest_blob_with_token(&a, &want, &mut &bytes[..])
            .unwrap();
        receiver
            .ingest_blob_with_token(&b, &want, &mut &bytes[..])
            .unwrap();
        receiver.publish_received_with_token(&a, &m, None).unwrap();
        assert!(receiver
            .ingest_blob_with_token(&a, &want, &mut &bytes[..])
            .is_err());
        let count = |store: &SourceStore, root: String| {
            store
                .retention
                .connect()
                .unwrap()
                .query_row("SELECT count(*) FROM member WHERE root=?1", [root], |r| {
                    r.get::<_, u32>(0)
                })
                .unwrap()
        };
        assert_eq!(count(&receiver, a.root()), 0);
        assert_eq!(count(&receiver, b.root()), 1);
        // A legacy publish cannot release the token of an unrelated importer.
        receiver.publish_received(&m, None).unwrap();
        drop(receiver);
        let reopened = SourceStore::open(&state).unwrap();
        assert_eq!(count(&reopened, b.root()), 1);
        assert!(reopened.has_blob(&entry.hash));
        // The token of a different physical store is rejected before reading.
        let foreign = f.store.begin_import().unwrap();
        let mut input = std::io::Cursor::new(bytes.clone());
        assert!(reopened
            .ingest_blob_with_token(&foreign, &want, &mut input)
            .is_err());
        assert_eq!(input.position(), 0);
        let forged = ImportToken {
            id: ulid::Ulid::new().to_string(),
            store: b.store,
        };
        assert!(reopened
            .ingest_blob_with_token(&forged, &want, &mut &bytes[..])
            .is_err());
        // Positive handoff to one durable run releases only this received root.
        let record = f.store.run_record(&run).unwrap().unwrap();
        reopened.bind_import(&a, &run, &record).unwrap();
        reopened.bind_import(&a, &run, &record).unwrap();
        assert_eq!(count(&reopened, a.received_root()), 0);
        assert_eq!(count(&reopened, b.root()), 1);
        assert_eq!(reopened.source_of_run(&run).unwrap().unwrap(), id);
    }

    #[test]
    fn blob_receivers_wait_for_a_competing_source_index_writer() {
        let f = Fixture::new();
        let data = b"a shared transfer";
        let want = super::super::diff::BlobWant {
            hash: format!("{:x}", sha2::Sha256::digest(data)),
            size: data.len() as u64,
        };
        let token = f.store.begin_import().unwrap();
        let writer = f.store.retention.connect().unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let entering = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let legacy = scope.spawn(|| {
                entering.wait();
                f.store.ingest_blob(&want, &mut &data[..])
            });
            let bound = scope.spawn(|| {
                entering.wait();
                f.store
                    .ingest_blob_with_token(&token, &want, &mut &data[..])
            });
            entering.wait();
            // Hold the external writer longer than a maintenance connection's
            // one-second wait, but within the foreground intake deadline.
            std::thread::sleep(std::time::Duration::from_millis(1400));
            writer.execute_batch("COMMIT").unwrap();
            let legacy_result = legacy.join().unwrap();
            let bound_result = bound.join().unwrap();
            assert!(
                legacy_result.is_ok() && bound_result.is_ok(),
                "{legacy_result:?}, {bound_result:?}"
            );
        });
        assert_eq!(f.store.read_blob(&want.hash).unwrap(), data);
    }

    #[test]
    fn legacy_intake_keeps_independent_roots_without_hash_global_handoff() {
        let f = Fixture::new();
        let bytes = b"same bytes";
        let hash = format!("{:x}", sha2::Sha256::digest(bytes));
        let want = super::super::diff::BlobWant {
            hash: hash.clone(),
            size: bytes.len() as u64,
        };
        f.store.ingest_blob(&want, &mut &bytes[..]).unwrap();
        f.store.ingest_blob(&want, &mut &bytes[..]).unwrap();
        let c = f.store.retention.connect().unwrap();
        let owners: u32 = c
            .query_row(
                "SELECT count(*) FROM member WHERE object=?1",
                [format!("b:{hash}")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(owners, 2);
        let roots: u32 = c
            .query_row(
                "SELECT count(*) FROM root WHERE source IS NULL AND id LIKE 'import:%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(roots, 2);
    }

    #[test]
    fn handoff_pages_are_bounded_and_restart_overretains_until_destination_proof() {
        let f = Fixture::new();
        let c = f.store.retention.connect().unwrap();
        for root in ["producer", "destination"] {
            c.execute(
                "INSERT INTO root VALUES(?1,'pending','src-00000000000000000000000000000000')",
                [root],
            )
            .unwrap();
        }
        for n in 0..65 {
            let key = format!("b:{n:064x}");
            for root in ["producer", "destination"] {
                c.execute("INSERT INTO member VALUES(?1,?2)", params![root, key])
                    .unwrap();
            }
        }
        assert!(!f
            .store
            .retention
            .handoff_page("producer", "destination")
            .unwrap());
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM member WHERE root='producer'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            33
        );
        let reopened = SourceStore::open(&f.state).unwrap();
        // A failed destination-proof page commits no partial release.
        c.execute(
            "DELETE FROM member WHERE root='destination' AND object=?1",
            [format!("b:{:064x}", 32)],
        )
        .unwrap();
        assert!(reopened
            .retention
            .handoff_page("producer", "destination")
            .is_err());
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM member WHERE root='producer'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            33
        );
        c.execute(
            "INSERT INTO member VALUES('destination',?1)",
            [format!("b:{:064x}", 32)],
        )
        .unwrap();
        assert!(!reopened
            .retention
            .handoff_page("producer", "destination")
            .unwrap());
        assert!(reopened
            .retention
            .handoff_page("producer", "destination")
            .unwrap());
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM member WHERE root='destination'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            65
        );
    }

    #[test]
    fn import_release_pages_retain_other_tokens_and_unpublished_extra_blobs() {
        let f = Fixture::new();
        let a = f.store.begin_import().unwrap();
        let b = f.store.begin_import().unwrap();
        for n in 0..65 {
            for token in [&a, &b] {
                f.store
                    .retention
                    .import_blob(token, &format!("{n:064x}"))
                    .unwrap();
            }
        }
        let c = f.store.retention.connect().unwrap();
        c.execute("INSERT INTO root VALUES('published','received','src-00000000000000000000000000000000')",[]).unwrap();
        c.execute(
            "INSERT INTO member SELECT 'published',object FROM member WHERE root=?1 AND object<>?2",
            params![a.root(), format!("b:{:064x}", 64)],
        )
        .unwrap();
        assert!(!f
            .store
            .retention
            .release_import_page(&a, "published")
            .unwrap());
        let reopened = SourceStore::open(&f.state).unwrap();
        assert!(!reopened
            .retention
            .release_import_page(&a, "published")
            .unwrap());
        assert!(reopened
            .retention
            .release_import_page(&a, "published")
            .unwrap());
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM member WHERE root=?1",
                [a.root()],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM member WHERE root=?1",
                [b.root()],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            65
        );
        assert!(c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1 AND source IS NULL)",
                [a.root()],
                |r| r.get::<_, bool>(0)
            )
            .unwrap());
    }

    #[test]
    fn fixture_run_fence_does_not_change_content_identity_or_new_publication_generation() {
        use sha2::Digest as _;
        let f = Fixture::new();
        let old = RunId::generate();
        f.bind(&old);
        let record = f.store.run_record(&old).unwrap().unwrap();
        let id = record.ci_source_id.clone();
        let m = f.store.manifest(&id).unwrap().unwrap();
        let hash = m.object_manifest.entries[0].hash.clone();
        // Test-only future fence. Production has no expiry/claim mutator in A.
        {
            let _exclusive = f.store.retention.guard(true).unwrap();
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "INSERT INTO run_expiry VALUES(?1,?2,?3)",
                    params![old.as_str(), id, m.manifest_digest],
                )
                .unwrap();
        }
        assert!(f.store.source_of_run(&old).is_err());
        assert!(f.store.bind_run(&old, &record).is_err());
        assert!(capture_for_run(
            &BTreeMap::new(),
            &f.store,
            &old,
            &SourceRequest::Directory {
                dir: f.input.clone()
            },
            &CaptureParams::default()
        )
        .is_err());
        let new = RunId::generate();
        f.bind(&new);
        assert_eq!(f.store.source_of_run(&new).unwrap().unwrap(), id);
        {
            let _exclusive = f.store.retention.guard(true).unwrap();
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "UPDATE publication SET state='claimed' WHERE object=?1",
                    [format!("b:{hash}")],
                )
                .unwrap();
        }
        assert!(f.store.read_blob(&hash).is_err());
        assert!(f.store.open_blob_verified(&hash).is_err());
        assert!(f
            .store
            .object_store()
            .unwrap()
            .put_bytes(b"original")
            .is_err());
        assert!(capture(
            &BTreeMap::new(),
            &f.store,
            &SourceRequest::Directory {
                dir: f.input.clone()
            },
            &CaptureParams::default()
        )
        .is_err());
        // Simulate B's completed-claim/new-incarnation boundary, without unlink.
        {
            let _exclusive = f.store.retention.guard(true).unwrap();
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "UPDATE publication SET state='live',incarnation=2 WHERE object=?1",
                    [format!("b:{hash}")],
                )
                .unwrap();
        }
        assert_eq!(
            f.store
                .mutation_guard()
                .unwrap()
                .ensure_blob_available(&hash)
                .unwrap()
                .get(),
            2
        );
        assert!(f.store.source_of_run(&old).is_err());
        assert_eq!(f.store.read_blob(&hash).unwrap(), b"original");
        assert_eq!(format!("{:x}", sha2::Sha256::digest(b"original")), hash);
        assert_eq!(f.store.source_of_run(&new).unwrap().unwrap(), id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn opened_reader_keeps_source_shared_guard_after_async_waiter_cancellation() {
        use std::io::Read as _;
        let f = Fixture::new();
        let (hash, _) = f
            .store
            .object_store()
            .unwrap()
            .put_bytes(b"held-reader")
            .unwrap();
        let mut reader = f.store.open_blob_verified(&hash).unwrap().file;
        let (entered, seen) = tokio::sync::oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let (drained, drain) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                resume.recv().unwrap();
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).unwrap();
                assert_eq!(bytes, b"held-reader");
                drop(reader);
                drained.send(()).unwrap();
            })
            .await
            .unwrap();
        });
        seen.await.unwrap();
        waiter.abort();
        let _ = waiter.await;
        assert!(f.store.retention.guard(true).is_err());
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), drain)
            .await
            .unwrap()
            .unwrap();
        assert!(f.store.retention.guard(true).is_ok());
    }
    #[test]
    fn guarded_sdk_view_retains_shared_ownership_and_claim_fences_its_callback() {
        let f = Fixture::new();
        let view = f.store.object_store().unwrap();
        view.with_sdk(|_| {
            assert!(f.store.retention.guard(true).is_err());
            Ok(())
        })
        .unwrap();
        drop(view);
        let (hash, _) = f
            .store
            .object_store()
            .unwrap()
            .put_bytes(b"sdk-fence")
            .unwrap();
        {
            let _exclusive = f.store.retention.guard(true).unwrap();
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "UPDATE publication SET state='claimed' WHERE object=?1",
                    [format!("b:{hash}")],
                )
                .unwrap();
        }
        let c = f.store.retention.connect().unwrap();
        let before: u32 = c
            .query_row("SELECT count(*) FROM root", [], |r| r.get(0))
            .unwrap();
        assert!(f.store.begin_import().is_err());
        assert_eq!(
            c.query_row("SELECT count(*) FROM root", [], |r| r.get::<_, u32>(0))
                .unwrap(),
            before
        );
        let view = f.store.object_store().unwrap();
        let mut called = false;
        assert!(view
            .with_sdk(|_| {
                called = true;
                Ok(())
            })
            .is_err());
        assert!(!called);
        drop(view);
        assert!(f.store.retention.guard(true).is_ok());
    }
    fn deletion_ready(f: &Fixture) -> (RunId, deletion::RetentionCursor) {
        let run = RunId::generate();
        f.bind(&run);
        f.terminal(&run);
        // Explicit private fixture registration completion; captures already
        // indexed their complete graph. No real state is used by these tests.
        f.store
            .retention
            .connect()
            .unwrap()
            .execute(
                "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                [],
            )
            .unwrap();
        let mut cursor = deletion::RetentionCursor::default();
        assert!(
            f.store
                .retention_page(&f.owner, &mut cursor, 100, 10, Some("boot".into()), 10)
                .unwrap()
                .held
        );
        (run, cursor)
    }
    fn deletion_tick(
        f: &Fixture,
        cursor: &mut deletion::RetentionCursor,
    ) -> deletion::RetentionPage {
        f.store
            .retention_page(&f.owner, cursor, 120, 30, Some("boot".into()), 10)
            .unwrap()
    }
    #[test]
    fn deletion_pages_33_and_65_preserve_original_and_metadata() {
        for count in [33, 65] {
            let f = Fixture::new();
            for n in 0..count {
                std::fs::write(f.input.join(format!("input{n}")), format!("data{n}")).unwrap();
            }
            let (run, mut cursor) = deletion_ready(&f);
            let mut examined = 0;
            let mut removed = 0;
            for _ in 0..8 {
                let page = deletion_tick(&f, &mut cursor);
                assert!(page.examined <= 32 && page.claimed <= 32);
                assert!(page.errors.is_empty(), "{:?}", page.errors);
                examined += page.examined;
                removed += page.removed;
            }
            assert_eq!(examined, count + 2);
            assert_eq!(removed, count + 2);
            assert!(f.input.join("file").exists());
            assert!(f.store.root().join("retention.db").exists());
            assert!(f.store.root().join("mutation.lock").exists());
            assert!(f.store.root().join("runs").join(run.as_str()).exists());
            assert!(f
                .store
                .mutation_guard()
                .unwrap()
                .ensure_run_available(&run)
                .is_err());
        }
    }
    /// The real deletion of a run with a confirmed retired attempt (which has
    /// no termination): the run is expired as the preview says, the run after
    /// it in id order is reached too, and neither can be retried afterwards.
    #[test]
    fn deletion_expires_a_run_with_a_retired_attempt_and_the_run_after_it() {
        let f = Fixture::new();
        let (one, mut cursor) = deletion_ready(&f);
        let two = RunId::generate();
        f.bind(&two);
        f.terminal(&two);
        let (first, later) = if one.as_str() < two.as_str() {
            (one, two)
        } else {
            (two, one)
        };
        f.retired_then_ended(&first);
        assert_eq!(f.source_end(&first), Some(13));
        let source = f.store.source_of_run(&first).unwrap().unwrap();
        f.preview(&mut PreviewCursor::default(), 100, 10);
        let preview = f.preview(&mut PreviewCursor::default(), 120, 30);
        assert_eq!(preview.run.as_ref(), Some(&first));
        assert!(preview.eligible && !preview.held);
        let page = deletion_tick(&f, &mut cursor);
        assert_eq!(page.run.as_deref(), Some(first.as_str()));
        assert!(page.errors.is_empty(), "{:?}", page.errors);
        assert_eq!((page.shared, page.unknown), (2, 0));
        assert!(f.expired(&first) && !f.expired(&later));
        let mut removed = 0;
        for _ in 0..4 {
            let page = deletion_tick(&f, &mut cursor);
            assert!(page.errors.is_empty(), "{:?}", page.errors);
            removed += page.removed;
        }
        assert_eq!(removed, 2);
        assert!(f.expired(&later));
        assert!(!f
            .store
            .root()
            .join("manifests")
            .join(format!("{source}.json"))
            .exists());
        for run in [&first, &later] {
            assert!(f
                .store
                .mutation_guard()
                .unwrap()
                .ensure_run_available(run)
                .is_err());
        }
    }
    /// A run whose record cannot be read keeps its inputs and is reported;
    /// the runs after it are still examined, in this pass and the next.
    #[test]
    fn deletion_goes_on_past_a_run_whose_record_cannot_be_read() {
        let f = Fixture::new();
        let (one, mut cursor) = deletion_ready(&f);
        let two = RunId::generate();
        f.bind(&two);
        f.terminal(&two);
        let (first, later) = if one.as_str() < two.as_str() {
            (one, two)
        } else {
            (two, one)
        };
        let broken = first.to_string();
        f.owner
            .db
            .call(move |c| {
                c.execute(
                    "UPDATE attempt SET terminal_at_ms='unreadable' WHERE task_id IN (SELECT task_id FROM task WHERE run_id=?1)",
                    [broken],
                )?;
                Ok(())
            })
            .unwrap();
        let source = super::super::classify::source_id_of_ci(
            &f.store.source_of_run(&first).unwrap().unwrap(),
        )
        .unwrap();
        assert!(f.owner.input_retention_end(&first, &source).is_err());
        let preview = f.preview(&mut PreviewCursor::default(), 120, 30);
        assert_eq!(preview.run.as_ref(), Some(&first));
        assert!(!preview.eligible);
        let page = deletion_tick(&f, &mut cursor);
        assert_eq!(page.run.as_deref(), Some(first.as_str()));
        assert_eq!((page.unknown, page.errors.len()), (1, 1));
        assert_eq!((page.claimed, page.shared, page.removed), (0, 0, 0));
        let page = deletion_tick(&f, &mut cursor);
        assert_eq!(page.run.as_deref(), Some(later.as_str()));
        assert!(page.errors.is_empty(), "{:?}", page.errors);
        // The next passes report the unreadable run again and nothing else.
        let mut again = 0;
        for _ in 0..8 {
            let page = deletion_tick(&f, &mut cursor);
            if page.run.as_deref() == Some(first.as_str()) {
                assert_eq!((page.unknown, page.errors.len()), (1, 1));
                again += 1;
            } else {
                assert!(page.errors.is_empty(), "{:?}", page.errors);
            }
        }
        assert!(again >= 2, "{again}");
        assert!(f.expired(&later) && !f.expired(&first));
        assert!(f
            .store
            .mutation_guard()
            .unwrap()
            .ensure_run_available(&first)
            .is_ok());
        assert!(f
            .store
            .mutation_guard()
            .unwrap()
            .ensure_run_available(&later)
            .is_err());
    }
    #[test]
    fn deletion_shared_source_survives_until_last_terminal_run() {
        let f = Fixture::new();
        let (one, mut cursor) = deletion_ready(&f);
        let two = RunId::generate();
        f.bind(&two);
        let source = f.store.run_record(&one).unwrap().unwrap().ci_source_id;
        let first = deletion_tick(&f, &mut cursor);
        assert_eq!(first.shared, 2);
        assert_eq!(first.removed, 0);
        assert!(f.store.manifest(&source).unwrap().is_some());
        f.terminal(&two);
        for _ in 0..4 {
            deletion_tick(&f, &mut cursor);
        }
        assert!(!f
            .store
            .root()
            .join("manifests")
            .join(format!("{source}.json"))
            .exists());
    }
    #[test]
    fn deletion_claim_crash_boundaries_replay_only_durable_exact_claims() {
        for stage in [
            "after_claim",
            "before_unlink",
            "after_unlink",
            "after_fsync",
            "before_completion",
        ] {
            let f = Fixture::new();
            let (_run, mut cursor) = deletion_ready(&f);
            deletion::set_fault(Some(stage));
            let interrupted =
                f.store
                    .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10);
            deletion::set_fault(None);
            if stage == "after_claim" {
                assert!(interrupted.is_err());
            } else {
                assert!(interrupted.unwrap().unknown > 0);
            }
            let pending: u32 = f
                .store
                .retention
                .connect()
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM source_delete_claim WHERE done=0",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(pending > 0);
            let page = deletion_tick(&f, &mut cursor);
            assert!(page.removed > 0, "{stage}: {page:?}");
            assert!(page.errors.is_empty(), "{stage}: {page:?}");
        }
    }
    #[test]
    fn deletion_clock_holds_also_stop_physical_claim_replay() {
        let f = Fixture::new();
        let (_run, mut cursor) = deletion_ready(&f);
        deletion::set_fault(Some("after_claim"));
        assert!(f
            .store
            .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10)
            .is_err());
        deletion::set_fault(None);
        let c = f.store.retention.connect().unwrap();
        let path:String=c.query_row("SELECT o.path FROM source_delete_claim d JOIN object o ON o.id=d.object WHERE d.done=0 LIMIT 1",[],|r|r.get(0)).unwrap();
        for (wall, mono, boot) in [
            (0, 31, Some("boot".into())),
            (119, 31, Some("boot".into())),
            (120, 29, Some("boot".into())),
            (9000, 31, Some("boot".into())),
            (120, u64::MAX, Some("boot".into())),
            (120, 31, None),
            (120, 31, Some("reboot".into())),
        ] {
            let page = f
                .store
                .retention_page(&f.owner, &mut cursor, wall, mono, boot, 10)
                .unwrap();
            assert!(page.held);
            assert_eq!(page.removed, 0);
            assert!(f.store.root().join(&path).exists());
        }
    }
    #[test]
    fn deletion_unclaimed_missing_and_unsafe_leaves_keep_membership() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        for mode in [
            "missing",
            "symlink",
            "hardlink",
            "writable",
            "directory",
            "wrong_content",
        ] {
            let f = Fixture::new();
            let (_run, mut cursor) = deletion_ready(&f);
            let c = f.store.retention.connect().unwrap();
            let (key, path): (String, String) = c
                .query_row(
                    "SELECT id,path FROM object WHERE id LIKE 'b:%' LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            let full = f.store.root().join(&path);
            match mode {
                "missing" => std::fs::remove_file(&full).unwrap(),
                "symlink" => {
                    std::fs::remove_file(&full).unwrap();
                    symlink(f.input.join("file"), &full).unwrap();
                }
                "hardlink" => std::fs::hard_link(&full, f.input.join("alias")).unwrap(),
                "writable" => {
                    std::fs::set_permissions(&full, std::fs::Permissions::from_mode(0o666)).unwrap()
                }
                "directory" => {
                    std::fs::remove_file(&full).unwrap();
                    std::fs::create_dir(&full).unwrap();
                }
                "wrong_content" => std::fs::write(&full, b"changed!").unwrap(),
                _ => unreachable!(),
            }
            let page = deletion_tick(&f, &mut cursor);
            assert!(page.unknown > 0, "{mode}");
            assert!(c
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1)",
                    [&key],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap());
            assert!(!c
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM source_delete_claim WHERE object=?1)",
                    [&key],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap());
            assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
        }
    }
    #[test]
    fn deletion_rejects_foreign_owner_moved_database_and_shared_reader() {
        let f = Fixture::new();
        let (_run, mut cursor) = deletion_ready(&f);
        let guard = f.store.mutation_guard().unwrap();
        assert!(deletion_tick(&f, &mut cursor).busy);
        drop(guard);
        let foreign = Fixture::new();
        assert!(f
            .store
            .retention_page(
                &foreign.owner,
                &mut cursor,
                120,
                30,
                Some("boot".into()),
                10
            )
            .is_err());
        std::fs::rename(f.state.join("coordinator.db"), f.state.join("moved.db")).unwrap();
        std::fs::write(f.state.join("coordinator.db"), b"replacement").unwrap();
        assert!(f
            .store
            .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10)
            .is_err());
    }
    #[test]
    fn deletion_completed_claim_never_targets_same_hash_new_incarnation() {
        let f = Fixture::new();
        let (run, mut cursor) = deletion_ready(&f);
        let source = f.store.run_record(&run).unwrap().unwrap().ci_source_id;
        let first = deletion_tick(&f, &mut cursor);
        assert_eq!(first.removed, 2);
        let next = RunId::generate();
        f.bind(&next);
        assert_eq!(
            f.store.run_record(&next).unwrap().unwrap().ci_source_id,
            source
        );
        let c = f.store.retention.connect().unwrap();
        assert!(c
            .query_row("SELECT min(incarnation)>=2 FROM publication", [], |r| r
                .get::<_, bool>(0))
            .unwrap());
        for _ in 0..4 {
            let page = deletion_tick(&f, &mut cursor);
            assert_eq!(page.removed, 0);
        }
        assert!(f.store.manifest(&source).unwrap().is_some());
        assert!(f
            .store
            .mutation_guard()
            .unwrap()
            .ensure_run_available(&run)
            .is_err());
    }
    #[test]
    fn deletion_queue_collection_unknown_and_digest_mismatch_do_not_expire() {
        for blocked in ["queued", "unknown", "collection", "digest", "termination"] {
            let f = Fixture::new();
            let (run, mut cursor) = deletion_ready(&f);
            match blocked {
                "queued" => {
                    f.owner
                        .create_task(&TaskId::generate(), &run, &serde_json::json!({}), 6)
                        .unwrap();
                }
                "unknown" => {
                    f.owner
                        .db
                        .call(|c| {
                            c.execute("UPDATE attempt SET observation='unknown'", [])?;
                            Ok(())
                        })
                        .unwrap();
                }
                "collection" => {
                    f.owner
                        .db
                        .call(|c| {
                            c.execute("UPDATE collection SET closed_at_ms=NULL,verdict=NULL", [])?;
                            Ok(())
                        })
                        .unwrap();
                }
                "digest" => {
                    f.owner
                        .db
                        .call(|c| {
                            c.execute("UPDATE source_ref SET manifest_digest='sha256:wrong'", [])?;
                            Ok(())
                        })
                        .unwrap();
                }
                "termination" => {
                    f.owner
                        .db
                        .call(|c| {
                            c.execute("UPDATE attempt SET termination_json='{}'", [])?;
                            Ok(())
                        })
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let page = deletion_tick(&f, &mut cursor);
            assert_eq!(page.claimed, 0, "{blocked}");
            assert!(
                f.store
                    .mutation_guard()
                    .unwrap()
                    .ensure_run_available(&run)
                    .is_ok(),
                "{blocked}"
            );
        }
    }
    #[test]
    fn deletion_pending_import_and_sentinels_are_retained() {
        let f = Fixture::new();
        let (run, mut cursor) = deletion_ready(&f);
        let token = f.store.begin_import().unwrap();
        let want = super::super::diff::BlobWant {
            hash: format!("{:x}", sha2::Sha256::digest(b"pending")),
            size: 7,
        };
        f.store
            .ingest_blob_with_token(&token, &want, &mut &b"pending"[..])
            .unwrap();
        for relative in [
            "keys/master",
            "bundles/cache",
            "git/objects/sentinel",
            "unknown",
        ] {
            let path = f.store.root().join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"keep").unwrap();
        }
        // a parent's capture in progress: a root of unknown membership
        f.store.capture_pending_root(None).unwrap();
        assert!(deletion_tick(&f, &mut cursor).held);
        assert!(f
            .store
            .mutation_guard()
            .unwrap()
            .ensure_run_available(&run)
            .is_ok());
        for relative in [
            "keys/master",
            "bundles/cache",
            "git/objects/sentinel",
            "unknown",
        ] {
            assert_eq!(
                std::fs::read(f.store.root().join(relative)).unwrap(),
                b"keep"
            );
        }
        assert!(f.store.has_blob(&want.hash));
    }
    #[test]
    fn deletion_is_not_held_by_a_receipt_and_keeps_its_files() {
        let f = Fixture::new();
        let (run, mut cursor) = deletion_ready(&f);
        let token = f.store.begin_import().unwrap();
        let want = super::super::diff::BlobWant {
            hash: format!("{:x}", sha2::Sha256::digest(b"pending")),
            size: 7,
        };
        f.store
            .ingest_blob_with_token(&token, &want, &mut &b"pending"[..])
            .unwrap();
        for relative in [
            "keys/master",
            "bundles/cache",
            "git/objects/sentinel",
            "unknown",
        ] {
            let path = f.store.root().join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"keep").unwrap();
        }
        // a receipt's membership is exact (spec 25.8): it holds nothing
        // back but its own files
        let _ = &run;
        assert!(!deletion_tick(&f, &mut cursor).held);
        for relative in [
            "keys/master",
            "bundles/cache",
            "git/objects/sentinel",
            "unknown",
        ] {
            assert_eq!(
                std::fs::read(f.store.root().join(relative)).unwrap(),
                b"keep"
            );
        }
        assert!(f.store.has_blob(&want.hash));
    }
    #[test]
    fn deletion_indexed_submodule_graph_is_bounded_without_parent_reload() {
        use std::sync::atomic::Ordering::SeqCst;
        let f = Fixture::new();
        let base = RunId::generate();
        f.bind(&base);
        let record = f.store.run_record(&base).unwrap().unwrap();
        let manifest = f.store.manifest(&record.ci_source_id).unwrap().unwrap();
        let children = (0..65)
            .map(|n| {
                let mut child = manifest.clone();
                child.branch = Some(format!("child{n}"));
                local_ci::source::seal_manifest(child)
            })
            .collect::<Vec<_>>();
        let mut parent = manifest.clone();
        parent.submodules = children
            .iter()
            .enumerate()
            .map(|(n, m)| local_ci::source::SubmoduleRef {
                path: local_ci::source::RelPath(format!("sub{n}").into_bytes()),
                revision: "0".repeat(40),
                source_id: Some(m.source_id.clone()),
                dirty: true,
            })
            .collect();
        let parent = local_ci::source::seal_manifest(parent);
        for m in children.iter().chain(std::iter::once(&parent)) {
            std::fs::write(
                f.store
                    .root()
                    .join("manifests")
                    .join(format!("{}.json", m.source_id)),
                serde_json::to_vec(m).unwrap(),
            )
            .unwrap();
        }
        let run = RunId::generate();
        f.store
            .bind_run(
                &run,
                &super::super::store::RunRecord {
                    ci_source_id: parent.source_id.clone(),
                    ..record
                },
            )
            .unwrap();
        f.terminal(&run);
        f.store
            .retention
            .connect()
            .unwrap()
            .execute(
                "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                [],
            )
            .unwrap();
        f.store.retention.reads.store(0, SeqCst);
        let mut cursor = deletion::RetentionCursor::default();
        assert!(
            f.store
                .retention_page(&f.owner, &mut cursor, 100, 10, Some("boot".into()), 10)
                .unwrap()
                .held
        );
        let mut removed = 0;
        let mut examined = 0;
        for _ in 0..8 {
            let page = deletion_tick(&f, &mut cursor);
            assert!(page.paths <= 32 && page.examined <= 32);
            assert!(page.errors.is_empty(), "{page:?}");
            removed += page.removed;
            if page.run.as_deref() == Some(run.as_str()) {
                examined += page.examined;
            }
        }
        assert_eq!(removed, 66);
        assert_eq!(examined, 67);
        assert!(f.store.retention.reads.load(SeqCst) <= 4);
        assert!(f.store.manifest(&manifest.source_id).unwrap().is_some());
    }
    #[test]
    fn deletion_ancestor_swap_claim_identity_and_missing_notfound_rules() {
        use std::os::unix::fs::symlink;
        for swap in ["ancestor", "leaf", "symlink", "root"] {
            let f = Fixture::new();
            let (_run, mut cursor) = deletion_ready(&f);
            deletion::set_fault(Some("after_claim"));
            assert!(f
                .store
                .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10)
                .is_err());
            deletion::set_fault(None);
            let c = f.store.retention.connect().unwrap();
            let path:String=c.query_row("SELECT o.path FROM source_delete_claim d JOIN object o ON o.id=d.object WHERE done=0 LIMIT 1",[],|r|r.get(0)).unwrap();
            let full = f.store.root().join(&path);
            match swap {
                "ancestor" => {
                    let parent = full.parent().unwrap();
                    std::fs::rename(parent, parent.with_extension("old")).unwrap();
                    std::fs::create_dir(parent).unwrap();
                    std::fs::write(&full, b"original").unwrap();
                }
                "leaf" => {
                    std::fs::rename(&full, full.with_extension("old")).unwrap();
                    std::fs::write(&full, b"original").unwrap();
                }
                "symlink" => {
                    std::fs::remove_file(&full).unwrap();
                    symlink(f.input.join("file"), &full).unwrap();
                }
                "root" => {
                    let root = f.store.root();
                    std::fs::rename(root, root.with_extension("old")).unwrap();
                    std::fs::create_dir(root).unwrap();
                }
                _ => unreachable!(),
            }
            let replay =
                f.store
                    .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10);
            if swap == "root" {
                assert!(replay.is_err());
            } else {
                let page = replay.unwrap();
                assert_eq!(page.removed, 0);
                assert!(page.unknown > 0);
            }
            assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
        }
    }
    #[test]
    fn coordinator_live_file_proof_does_not_release_another_connections_posix_lock() {
        let f = Fixture::new();
        let path = f.state.join("coordinator.db");
        // An independent connection's writer lock is still process-wide POSIX
        // state. The actor's proof must not open and close this inode.
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        f.owner.db.check_current_file().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "source::retention::tests::source_index_writer_probe_child",
                "--ignored",
                "--nocapture",
            ])
            .env("BELT_SOURCE_INDEX_PROBE", &path)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(String::from_utf8(output.stdout)
            .unwrap()
            .contains("BELT_INDEX_PROBE=BUSY"));
        writer.execute_batch("ROLLBACK").unwrap();
    }
    #[test]
    fn deletion_expired_run_blocks_direct_task_and_attempt_creation() {
        let f = Fixture::new();
        let (run, mut cursor) = deletion_ready(&f);
        let task: String = f
            .owner
            .db
            .call(|c| Ok(c.query_row("SELECT task_id FROM task LIMIT 1", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(deletion_tick(&f, &mut cursor).removed, 2);
        assert!(f
            .owner
            .create_task(&TaskId::generate(), &run, &serde_json::json!({}), 130)
            .is_err());
        assert!(f
            .owner
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: TaskId::parse(&task).unwrap(),
                    worker_id: WorkerId::parse("test").unwrap(),
                    payload_hash: Digest::of_bytes(b"payload"),
                    permit_id: None
                },
                130
            )
            .is_err());
        let count: i64 = f
            .owner
            .db
            .call(|c| Ok(c.query_row("SELECT count(*) FROM attempt", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(count, 1);
    }
    #[test]
    fn deletion_expiry_crash_order_and_invalid_paths_overretain() {
        for stage in ["before_expiry", "after_expiry"] {
            let f = Fixture::new();
            let (run, mut cursor) = deletion_ready(&f);
            deletion::set_fault(Some(stage));
            assert!(f
                .store
                .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10)
                .is_err());
            deletion::set_fault(None);
            let c = f.store.retention.connect().unwrap();
            let members: i64 = c
                .query_row(
                    "SELECT count(*) FROM member WHERE root=?1",
                    [run.as_str()],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(members, 2);
            assert_eq!(
                f.store
                    .mutation_guard()
                    .unwrap()
                    .ensure_run_available(&run)
                    .is_err(),
                stage == "after_expiry"
            );
            assert_eq!(deletion_tick(&f, &mut cursor).removed, 2);
        }
        for path in [
            "../original/file",
            "objects/00/invalid",
            "keys/master",
            "manifests/src-ABC.json",
        ] {
            let f = Fixture::new();
            let (_run, mut cursor) = deletion_ready(&f);
            let c = f.store.retention.connect().unwrap();
            c.execute("UPDATE object SET path=?1 WHERE id LIKE 'b:%'", [path])
                .unwrap();
            let page = deletion_tick(&f, &mut cursor);
            assert!(page.unknown > 0);
            assert_eq!(std::fs::read(f.input.join("file")).unwrap(), b"original");
        }
    }
    #[test]
    fn deletion_symlinked_state_ancestor_never_creates_expiry() {
        use std::os::unix::fs::symlink;
        let f = Fixture::new();
        let (run, mut cursor) = deletion_ready(&f);
        let moved = f.state.with_extension("saved");
        std::fs::rename(&f.state, &moved).unwrap();
        symlink(&moved, &f.state).unwrap();
        assert!(f
            .store
            .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10)
            .is_err());
        std::fs::remove_file(&f.state).unwrap();
        std::fs::rename(&moved, &f.state).unwrap();
        assert!(f
            .store
            .mutation_guard()
            .unwrap()
            .ensure_run_available(&run)
            .is_ok());
    }
    #[test]
    fn deletion_retired_terminal_current_generation_is_still_queued() {
        let f = Fixture::new();
        let (run, mut cursor) = deletion_ready(&f);
        f.owner
            .db
            .call(|c| {
                c.execute("UPDATE attempt SET retired=1,retired_at_ms=6", [])?;
                Ok(())
            })
            .unwrap();
        assert!(
            f.owner
                .input_need(&run, &WorkerId::parse("test").unwrap())
                .unwrap()
                .0
        );
        let page = deletion_tick(&f, &mut cursor);
        assert_eq!(page.claimed, 0);
        assert!(f
            .store
            .mutation_guard()
            .unwrap()
            .ensure_run_available(&run)
            .is_ok());
    }
    #[test]
    fn deletion_owner_lock_replacement_and_symlink_fail_closed() {
        use std::os::unix::fs::symlink;
        for symlinked in [false, true] {
            let f = Fixture::new();
            let (run, mut cursor) = deletion_ready(&f);
            let path = f.state.join("coordinator.lock");
            let saved = f.state.join("owner.saved");
            std::fs::rename(&path, &saved).unwrap();
            if symlinked {
                symlink(&saved, &path).unwrap()
            } else {
                std::fs::write(&path, b"replacement").unwrap()
            }
            assert!(f
                .store
                .retention_page(&f.owner, &mut cursor, 120, 30, Some("boot".into()), 10)
                .is_err());
            assert!(f
                .store
                .mutation_guard()
                .unwrap()
                .ensure_run_available(&run)
                .is_ok());
        }
    }
    #[test]
    fn deletion_large_unproven_leaf_is_held_before_hashing_or_claiming() {
        let f = Fixture::new();
        let (_run, mut cursor) = deletion_ready(&f);
        let c = f.store.retention.connect().unwrap();
        let (object, path): (String, String) = c
            .query_row(
                "SELECT id,path FROM object WHERE id LIKE 'b:%' LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(f.store.root().join(&path))
            .unwrap();
        file.set_len(64 * 1024 * 1024).unwrap();
        drop(file);
        let page = deletion_tick(&f, &mut cursor);
        assert!(page.errors.iter().any(|e| e.contains("byte budget")));
        assert!(f.store.root().join(&path).exists());
        assert!(c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1)",
                [&object],
                |r| r.get::<_, bool>(0)
            )
            .unwrap());
        assert!(!c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM source_delete_claim WHERE object=?1)",
                [&object],
                |r| r.get::<_, bool>(0)
            )
            .unwrap());
    }
    mod vault_cases {
        use super::*;
        use crate::secrets::capture::{capture_run, SecretInputs};
        use crate::secrets::{key::ClusterKey, vault::Vault};
        use std::os::unix::fs::PermissionsExt;
        fn prepared() -> (Fixture, RunId, Vault, ClusterKey) {
            let f = Fixture::new();
            let run = RunId::generate();
            let key = ClusterKey::from_bytes(vec![7; 32]).unwrap();
            let vault = Vault::open(&f.state).unwrap();
            secret_bind(&f, &run, &vault, &key);
            f.terminal(&run);
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                    [],
                )
                .unwrap();
            (f, run, vault, key)
        }
        fn secret_bind(f: &Fixture, run: &RunId, vault: &Vault, key: &ClusterKey) {
            std::fs::write(f.input.join(".env"), b"TOKEN=original-private-fixture\n").unwrap();
            capture_run(
                &BTreeMap::new(),
                &f.store,
                vault,
                &SecretInputs {
                    key,
                    declared: &[local_ci::source::SecretSpec {
                        glob: ".env".into(),
                        required: true,
                    }],
                },
                run,
                &SourceRequest::Directory {
                    dir: f.input.clone(),
                },
                &CaptureParams::default(),
            )
            .unwrap();
        }
        fn tick(
            f: &Fixture,
            v: &Vault,
            _k: &ClusterKey,
            c: &mut VaultCursor,
            wall: u64,
            mono: u64,
        ) -> VaultPage {
            f.store
                .vault_retention_page(&f.owner, v, c, wall, mono, Some("boot".into()))
                .unwrap()
        }
        fn entries(f: &Fixture, r: &RunId) -> Vec<local_ci::source::SecretEntry> {
            f.store
                .manifest(&f.store.source_of_run(r).unwrap().unwrap())
                .unwrap()
                .unwrap()
                .secret_files
        }
        #[test]
        fn source_gc_preserves_secret_proof_before_vault_and_pending_claim() {
            let (f, r, v, k) = prepared();
            let source = f.store.source_of_run(&r).unwrap().unwrap();
            let mut b = RetentionCursor::default();
            assert!(
                f.store
                    .retention_page(&f.owner, &mut b, 100, 10, Some("boot".into()), 10)
                    .unwrap()
                    .held
            );
            assert_eq!(deletion_tick(&f, &mut b).claimed, 0);
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            b = RetentionCursor::default();
            assert_eq!(deletion_tick(&f, &mut b).claimed, 0);
            crate::secrets::vault::set_retention_fault(Some("after_claim"));
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            crate::secrets::vault::set_retention_fault(None);
            assert_eq!((page.claimed, page.unknown), (1, 1));
            b = RetentionCursor::default();
            assert_eq!(deletion_tick(&f, &mut b).claimed, 0);
            assert!(f.store.manifest(&source).unwrap().is_some());
            let db = f.store.retention.connect().unwrap();
            assert_eq!(
                db.query_row("SELECT count(*) FROM run_expiry", [], |r| r
                    .get::<_, u32>(0))
                    .unwrap(),
                0
            );
            assert_eq!(tick(&f, &v, &k, &mut c, 102, 12).removed, 1);
            // A completed row with the wrong binding is not authority either.
            let binding: String = db
                .query_row("SELECT binding_digest FROM vault_delete_claim", [], |r| {
                    r.get(0)
                })
                .unwrap();
            db.execute("UPDATE vault_delete_claim SET binding_digest='wrong'", [])
                .unwrap();
            b = RetentionCursor::default();
            assert_eq!(deletion_tick(&f, &mut b).claimed, 0);
            db.execute("UPDATE vault_delete_claim SET binding_digest=?1", [binding])
                .unwrap();
            b = RetentionCursor::default();
            assert!(deletion_tick(&f, &mut b).removed > 0);
            assert!(!f.state.join("secrets").join(format!("{r}.json")).exists());
        }
        /// A secret run with a confirmed retired attempt: its Vault copy
        /// is claimed and erased, then the source is expired, and neither
        /// store admits a retry afterwards.
        #[test]
        fn vault_and_source_gc_release_a_secret_run_with_a_retired_attempt() {
            let (f, r, v, k) = prepared();
            f.retired_then_ended(&r);
            let mut b = RetentionCursor::default();
            assert!(
                f.store
                    .retention_page(&f.owner, &mut b, 100, 10, Some("boot".into()), 10)
                    .unwrap()
                    .held
            );
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            assert!(v.has(&r));
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert!(page.errors.is_empty(), "{:?}", page.errors);
            assert_eq!((page.claimed, page.removed, page.unknown), (1, 1, 0));
            assert!(!v.has(&r));
            assert!(v.ensure_run_available(&r).is_err());
            let page = deletion_tick(&f, &mut b);
            assert!(page.errors.is_empty(), "{:?}", page.errors);
            assert!(page.removed > 0);
            assert!(f.expired(&r));
            assert!(f
                .store
                .mutation_guard()
                .unwrap()
                .ensure_run_available(&r)
                .is_err());
        }
        #[test]
        fn source_gc_shared_manifest_waits_for_each_secret_run_failure() {
            let (f, r, v, k) = prepared();
            let other = RunId::generate();
            secret_bind(&f, &other, &v, &k);
            f.terminal(&other);
            let source = f.store.source_of_run(&r).unwrap().unwrap();
            assert_eq!(f.store.source_of_run(&other).unwrap().unwrap(), source);
            let path = f.state.join("secrets").join(format!("{other}.json"));
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!((page.claimed, page.removed, page.unknown), (1, 1, 1));
            let mut b = RetentionCursor::default();
            f.store
                .retention_page(&f.owner, &mut b, 100, 10, Some("boot".into()), 10)
                .unwrap();
            for _ in 0..4 {
                deletion_tick(&f, &mut b);
            }
            assert!(f.store.manifest(&source).unwrap().is_some());
            let db = f.store.retention.connect().unwrap();
            assert!(!db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM run_expiry WHERE run_id=?1)",
                    [other.as_str()],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap());
            assert!(db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM member WHERE root=?1)",
                    [other.as_str()],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            c = VaultCursor::default();
            assert_eq!(tick(&f, &v, &k, &mut c, 102, 12).removed, 1);
            for _ in 0..4 {
                deletion_tick(&f, &mut b);
            }
            assert!(!db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM member WHERE root=?1)",
                    [other.as_str()],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap());
        }
        #[test]
        fn vault_missing_marker_recovers_parent_fence_for_existing_and_reopened() {
            let (f, r, v, k) = prepared();
            let files = entries(&f, &r);
            let path = f.state.join("secrets").join(format!("{r}.json"));
            let bytes = std::fs::read(&path).unwrap();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            assert_eq!(tick(&f, &v, &k, &mut c, 101, 11).removed, 1);
            let marker = f.state.join("secrets/parent-retention.json");
            std::fs::rename(&marker, marker.with_extension("renamed")).unwrap();
            let reopened = Vault::open(&f.state).unwrap();
            for vault in [&v, &reopened] {
                assert!(!vault.has(&r));
                assert!(matches!(
                    vault.load(&r, &files, &k),
                    Err(crate::secrets::SecretError::Unavailable { .. })
                ));
                let copy = f._root.path().join("attempted-replacement");
                std::fs::write(&copy, &bytes).unwrap();
                std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o600)).unwrap();
                assert!(matches!(
                    vault.adopt(&r, &copy, &files, &k),
                    Err(crate::secrets::SecretError::Unavailable { .. })
                ));
                let result = capture_run(
                    &BTreeMap::new(),
                    &f.store,
                    vault,
                    &SecretInputs {
                        key: &k,
                        declared: &[local_ci::source::SecretSpec {
                            glob: ".env".into(),
                            required: true,
                        }],
                    },
                    &r,
                    &SourceRequest::Directory {
                        dir: f.input.clone(),
                    },
                    &CaptureParams::default(),
                )
                .unwrap_err();
                assert_eq!(
                    result.blocked_reason,
                    Some(crate::error::BlockedReason::SecretUnavailable)
                );
                assert!(!path.exists());
            }
            // Source index loss together with marker loss is still fail closed.
            let database = f.state.join("sources/retention.db");
            std::fs::rename(&database, database.with_extension("renamed")).unwrap();
            assert!(!v.has(&r));
            assert!(v.load(&r, &files, &k).is_err());
            assert!(Vault::open(&f.state).is_err());
            assert!(!database.exists());
            std::fs::rename(database.with_extension("renamed"), &database).unwrap();
            let sources = f.state.join("sources");
            std::fs::rename(&sources, sources.with_extension("renamed")).unwrap();
            assert!(!v.has(&r));
            assert!(Vault::open(&f.state).is_err());
            std::fs::rename(sources.with_extension("renamed"), &sources).unwrap();
        }
        #[test]
        fn vault_classification_pin_malformed_or_conflicting_marker_fails_closed() {
            use std::os::unix::fs::FileExt;
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            let marker = f.state.join("secrets/parent-retention.json");
            let original = std::fs::read(&marker).unwrap();
            let plaintext = f.state.join("secrets").join(format!("{r}.json"));
            let original_plaintext = std::fs::read(&plaintext).unwrap();
            let mut wrong: serde_json::Value = serde_json::from_slice(&original).unwrap();
            wrong["database"][1] = serde_json::json!(0);
            std::fs::write(&marker, serde_json::to_vec(&wrong).unwrap()).unwrap();
            assert!(!v.has(&r));
            assert!(Vault::open(&f.state).is_err());
            assert!(f
                .store
                .vault_retention_page(&f.owner, &v, &mut c, 101, 11, Some("boot".into()))
                .is_err());
            assert_eq!(std::fs::read(&plaintext).unwrap(), original_plaintext);
            std::fs::write(&marker, &original).unwrap();
            let db = f.store.retention.connect().unwrap();
            let mut wrong_meta: serde_json::Value = serde_json::from_slice(&original).unwrap();
            wrong_meta["database"][1] = serde_json::json!(0);
            db.execute(
                "UPDATE meta SET value=?1 WHERE key='vault_registration'",
                [serde_json::to_string(&wrong_meta).unwrap()],
            )
            .unwrap();
            assert!(!v.has(&r));
            assert!(Vault::open(&f.state).is_err());
            db.execute(
                "UPDATE meta SET value=?1 WHERE key='vault_registration'",
                [std::str::from_utf8(&original).unwrap()],
            )
            .unwrap();
            let lock = std::fs::OpenOptions::new()
                .write(true)
                .open(f.state.join("secrets/retention.lock"))
                .unwrap();
            lock.write_all_at(b"!", 0).unwrap();
            lock.sync_all().unwrap();
            assert!(!v.has(&r));
            assert!(Vault::open(&f.state).is_err());
            assert!(f.state.join("secrets/retention.lock").exists());
            assert!(f
                .store
                .vault_retention_page(&f.owner, &v, &mut c, 101, 11, Some("boot".into()))
                .is_err());
            assert_eq!(std::fs::read(&plaintext).unwrap(), original_plaintext);
        }
        #[test]
        fn vault_marker_missing_recovers_from_index_before_lock_pin() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            // Model a crash after meta commit, before the first lock pin/marker.
            let lock = std::fs::OpenOptions::new()
                .write(true)
                .open(f.state.join("secrets/retention.lock"))
                .unwrap();
            lock.set_len(0).unwrap();
            lock.sync_all().unwrap();
            std::fs::remove_file(f.state.join("secrets/parent-retention.json")).unwrap();
            let reopened = Vault::open(&f.state).unwrap();
            assert!(reopened.has(&r));
            assert_eq!(tick(&f, &v, &k, &mut c, 101, 11).removed, 1);
            assert!(!reopened.has(&r));
        }
        /// A crash that leaves the classification pin (`retention.lock`) or the
        /// registration marker cut short must not let a pass delete anything,
        /// and the same files made whole again let the next pass go on.
        #[test]
        fn vault_cut_short_pin_or_marker_keeps_every_copy_until_restored() {
            use std::os::unix::fs::FileExt;
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let secrets = f.state.join("secrets");
            let plaintext = secrets.join(format!("{r}.json"));
            let kept = std::fs::read(&plaintext).unwrap();
            let lock_path = secrets.join("retention.lock");
            let marker = secrets.join("parent-retention.json");
            let pin = std::fs::read(&lock_path).unwrap();
            let published = std::fs::read(&marker).unwrap();
            assert!(!pin.is_empty() && !published.is_empty());
            let fail_closed = |c: &mut VaultCursor| {
                assert!(!v.has(&r));
                assert!(Vault::open(&f.state).is_err());
                assert!(f
                    .store
                    .vault_retention_page(&f.owner, &v, c, 101, 11, Some("boot".into()))
                    .is_err());
                assert_eq!(std::fs::read(&plaintext).unwrap(), kept);
                assert!(lock_path.exists() && marker.exists());
            };
            let lock = std::fs::OpenOptions::new()
                .write(true)
                .open(&lock_path)
                .unwrap();
            for cut in [1, pin.len() / 2, pin.len() - 1] {
                lock.set_len(cut as u64).unwrap();
                lock.sync_all().unwrap();
                fail_closed(&mut c);
            }
            lock.write_all_at(&pin, 0).unwrap();
            lock.set_len(pin.len() as u64).unwrap();
            lock.sync_all().unwrap();
            for cut in [1, published.len() / 2, published.len() - 1] {
                std::fs::write(&marker, &published[..cut]).unwrap();
                fail_closed(&mut c);
            }
            std::fs::write(&marker, &published).unwrap();
            assert!(v.has(&r));
            assert_eq!(tick(&f, &v, &k, &mut c, 102, 12).removed, 1);
            assert!(!v.has(&r));
        }
        /// An empty pin beside an intact marker is the crash before the pin was
        /// written: the marker stands for the registration, and the next pass
        /// pins it again and deletes exactly what it should.
        #[test]
        fn vault_empty_pin_with_an_intact_marker_is_pinned_again() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let lock_path = f.state.join("secrets/retention.lock");
            let lock = std::fs::OpenOptions::new()
                .write(true)
                .open(&lock_path)
                .unwrap();
            lock.set_len(0).unwrap();
            lock.sync_all().unwrap();
            let reopened = Vault::open(&f.state).unwrap();
            assert!(reopened.has(&r));
            assert!(f.state.join("secrets/parent-retention.json").exists());
            assert_eq!(tick(&f, &v, &k, &mut c, 101, 11).removed, 1);
            assert!(!reopened.has(&r));
        }
        #[test]
        fn vault_parent_tombstone_deletes_only_private_copy_and_never_expires_source() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!(
                (page.claimed, page.removed, page.unknown),
                (1, 1, 0),
                "{:?}",
                page
            );
            assert!(!v.has(&r));
            assert!(matches!(
                v.load(&r, &[], &k),
                Err(crate::secrets::SecretError::Unavailable { .. })
            ));
            assert!(f
                .store
                .mutation_guard()
                .unwrap()
                .ensure_run_available(&r)
                .is_ok());
            assert_eq!(
                std::fs::read(f.input.join(".env")).unwrap(),
                b"TOKEN=original-private-fixture\n"
            );
            let unavailable = capture_run(
                &BTreeMap::new(),
                &f.store,
                &v,
                &SecretInputs {
                    key: &k,
                    declared: &[],
                },
                &r,
                &SourceRequest::Directory {
                    dir: f._root.path().join("nonexistent-original"),
                },
                &CaptureParams::default(),
            )
            .unwrap_err();
            assert_eq!(
                unavailable.blocked_reason,
                Some(crate::error::BlockedReason::SecretUnavailable)
            );
            assert!(f.store.source_of_run(&r).unwrap().is_some());
            let db = f.store.retention.connect().unwrap();
            assert_eq!(
                db.query_row("SELECT count(*) FROM run_expiry", [], |r| r
                    .get::<_, u32>(0))
                    .unwrap(),
                0
            );
            // A later recreation is retained, but cannot make the run available.
            let path = f.state.join("secrets").join(format!("{r}.json"));
            std::fs::write(&path, b"recreated").unwrap();
            c = VaultCursor::default();
            assert_eq!(tick(&f, &v, &k, &mut c, 102, 12).paths, 0);
            assert!(path.exists());
            let reopened = Vault::open(&f.state).unwrap();
            assert!(!reopened.has(&r));
            let copy = f._root.path().join("new-copy");
            std::fs::write(&copy, b"private").unwrap();
            assert!(matches!(
                reopened.adopt(&r, &copy, &[], &k),
                Err(crate::secrets::SecretError::Unavailable { .. })
            ));
        }
        #[test]
        fn vault_held_clone_and_direct_read_keep_gc_busy() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let held = v.load(&r, &entries(&f, &r), &k).unwrap();
            let clone = held.clone();
            drop(held);
            assert!(tick(&f, &v, &k, &mut c, 101, 11).busy);
            assert!(v.has(&r));
            drop(clone);
            assert_eq!(tick(&f, &v, &k, &mut c, 101, 11).removed, 1);
        }
        #[test]
        fn vault_claim_replay_clock_holds_and_changed_target_stays() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            crate::secrets::vault::set_retention_fault(Some("after_claim"));
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!((page.claimed, page.unknown), (1, 1));
            crate::secrets::vault::set_retention_fault(None);
            let path = f.state.join("secrets").join(format!("{r}.json"));
            assert!(path.exists());
            assert!(!v.has(&r));
            assert!(tick(&f, &v, &k, &mut c, 99, 12).held);
            assert!(path.exists());
            // Replacement has the same bytes and permissions, a different inode.
            let bytes = std::fs::read(&path).unwrap();
            let other = path.with_extension("replacement");
            std::fs::write(&other, bytes).unwrap();
            std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::rename(&other, &path).unwrap();
            let p = tick(&f, &v, &k, &mut c, 102, 12);
            assert_eq!((p.paths, p.removed, p.unknown), (1, 0, 1));
            assert!(path.exists());
        }
        #[test]
        fn vault_missing_claim_target_replays_only_after_valid_clock() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            crate::secrets::vault::set_retention_fault(Some("after_unlink"));
            let p = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!((p.claimed, p.unknown), (1, 1));
            crate::secrets::vault::set_retention_fault(None);
            assert!(!f.state.join("secrets").join(format!("{r}.json")).exists());
            assert!(tick(&f, &v, &k, &mut c, 99, 12).held);
            let p = tick(&f, &v, &k, &mut c, 102, 12);
            assert_eq!((p.paths, p.removed), (1, 1));
            assert_eq!(tick(&f, &v, &k, &mut c, 103, 13).paths, 0);
        }
        #[test]
        fn vault_registered_missing_index_never_becomes_worker_mode() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            let db = f.state.join("sources/retention.db");
            let saved = db.with_extension("saved");
            std::fs::rename(&db, &saved).unwrap();
            assert!(!v.has(&r));
            assert!(matches!(
                v.load(&r, &[], &k),
                Err(crate::secrets::SecretError::Unavailable { .. })
            ));
            assert!(Vault::open(&f.state).is_err());
            std::fs::rename(saved, db).unwrap();
            assert!(v.has(&r));
        }
        #[test]
        fn vault_page_is_bounded_and_requires_terminal_exact_proof() {
            let f = Fixture::new();
            let v = Vault::open(&f.state).unwrap();
            let k = ClusterKey::from_bytes(vec![7; 32]).unwrap();
            for _ in 0..33 {
                let r = RunId::generate();
                secret_bind(&f, &r, &v, &k);
                f.terminal(&r);
            }
            let active = RunId::generate();
            secret_bind(&f, &active, &v, &k);
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                    [],
                )
                .unwrap();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            let p = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!((p.examined, p.claimed), (32, 32));
            let p = tick(&f, &v, &k, &mut c, 102, 12);
            assert_eq!((p.examined, p.claimed), (2, 1));
            assert!(v.load(&active, &[], &k).is_ok());
        }
        #[test]
        fn vault_page_recovers_crashed_publication_despite_holds_and_then_claims() {
            let (f, r, v, k) = prepared();
            let secrets = f.state.join("secrets");
            let bundle = secrets.join(format!("{r}.json"));
            let bytes = std::fs::read(&bundle).unwrap();
            let tmp =
                |published: &str| secrets.join(format!(".{published}.{}.tmp", ulid::Ulid::new()));
            // The parent died after linking the bundle and before unlinking
            // the temporary name, and once more before linking another one.
            let linked = tmp(&format!("{r}.json"));
            std::fs::hard_link(&bundle, &linked).unwrap();
            let unpublished = tmp(&format!("{}.json", RunId::generate()));
            std::fs::write(&unpublished, b"TOKEN=never-published-fixture\n").unwrap();
            std::fs::set_permissions(&unpublished, std::fs::Permissions::from_mode(0o600)).unwrap();
            let link = tmp(&format!("{r}.json"));
            std::os::unix::fs::symlink(f.input.join(".env"), &link).unwrap();
            assert!(!v.has(&r));
            assert!(v.load(&r, &entries(&f, &r), &k).is_err());
            let mut c = VaultCursor::default();
            // A reader of the Vault keeps the page, and the sweep, away.
            let reader = v.lease(false).unwrap();
            let page = tick(&f, &v, &k, &mut c, 100, 10);
            assert!(page.busy);
            assert_eq!(page.tmp_removed, 0);
            assert!(linked.exists() && unpublished.exists());
            drop(reader);
            // The first sample always holds the clock; recovery does not wait for it.
            let page = tick(&f, &v, &k, &mut c, 100, 10);
            assert!(page.held);
            // The link named like a temporary is a leftover, not a retention failure.
            assert_eq!(
                (
                    page.tmp_removed,
                    page.tmp_retained,
                    page.unknown,
                    page.claimed,
                    page.removed
                ),
                (2, 1, 0, 0, 0)
            );
            assert!(!linked.exists() && !unpublished.exists());
            assert!(std::fs::symlink_metadata(&link).is_ok());
            assert_eq!(
                std::fs::read(f.input.join(".env")).unwrap(),
                b"TOKEN=original-private-fixture\n"
            );
            assert_eq!(std::fs::read(&bundle).unwrap(), bytes);
            assert!(v.has(&r));
            assert_eq!(v.load(&r, &entries(&f, &r), &k).unwrap().files.len(), 1);
            // Readable again, the terminal run's copy is claimed and erased as usual.
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!(
                (
                    page.tmp_removed,
                    page.tmp_retained,
                    page.unknown,
                    page.claimed,
                    page.removed
                ),
                (0, 1, 0, 1, 1)
            );
            assert!(!bundle.exists());
            assert!(std::fs::symlink_metadata(&link).is_ok());
        }
        #[test]
        fn vault_symlinked_state_ancestor_holds_the_page_and_keeps_every_file() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let orphan = f
                .state
                .join("secrets")
                .join(format!(".{r}.json.{}.tmp", ulid::Ulid::new()));
            std::fs::write(&orphan, b"TOKEN=never-published-fixture\n").unwrap();
            std::fs::set_permissions(&orphan, std::fs::Permissions::from_mode(0o600)).unwrap();
            // After startup the state directory becomes reachable only through a link.
            let moved = f.state.with_extension("saved");
            std::fs::rename(&f.state, &moved).unwrap();
            std::os::unix::fs::symlink(&moved, &f.state).unwrap();
            assert!(f
                .store
                .vault_retention_page(&f.owner, &v, &mut c, 101, 11, Some("boot".into()))
                .is_err());
            let bundle = moved.join("secrets").join(format!("{r}.json"));
            assert!(bundle.exists());
            assert!(moved
                .join("secrets")
                .join(orphan.file_name().unwrap())
                .exists());
            // Restoring the real directory resumes on the next page.
            std::fs::remove_file(&f.state).unwrap();
            std::fs::rename(&moved, &f.state).unwrap();
            let db = f.store.retention.connect().unwrap();
            assert_eq!(
                db.query_row("SELECT count(*) FROM vault_delete_claim", [], |r| r
                    .get::<_, u32>(0))
                    .unwrap(),
                0
            );
            drop(db);
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!((page.tmp_removed, page.claimed, page.removed), (1, 1, 1));
            assert!(!orphan.exists());
        }
        fn marker_tmp(secrets: &Path) -> PathBuf {
            secrets.join(format!(".parent-retention.json.{}.tmp", ulid::Ulid::new()))
        }
        #[test]
        fn vault_registration_marker_left_with_two_names_opens_and_the_next_page_repairs_it() {
            // The first registration died between linking the marker and
            // unlinking its temporary name. The classification pin was synced
            // before that, so the marker is read as it is; only the page's
            // sweep, under the exclusive lease, removes the leftover name.
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let secrets = f.state.join("secrets");
            let marker = secrets.join("parent-retention.json");
            let bytes = std::fs::read(&marker).unwrap();
            let orphan = marker_tmp(&secrets);
            std::fs::hard_link(&marker, &orphan).unwrap();
            let files = entries(&f, &r);
            for _ in 0..2 {
                let opened = Vault::open(&f.state).unwrap();
                assert!(opened.has(&r));
                assert_eq!(opened.load(&r, &files, &k).unwrap().files.len(), 1);
                // Opening and reading unlink nothing.
                assert!(orphan.exists());
                assert_eq!(std::fs::metadata(&marker).unwrap().nlink(), 2);
            }
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!(
                (page.tmp_removed, page.tmp_retained, page.unknown),
                (1, 0, 0)
            );
            assert!(!orphan.exists());
            assert_eq!(std::fs::metadata(&marker).unwrap().nlink(), 1);
            assert_eq!(std::fs::read(&marker).unwrap(), bytes);
            // Retention went on in the same page: the terminal run's copy is erased.
            assert_eq!((page.claimed, page.removed), (1, 1));
            assert!(Vault::open(&f.state).is_ok());
        }
        #[test]
        fn vault_registration_marker_with_any_other_second_name_stays_refused() {
            use std::os::unix::fs::{symlink, PermissionsExt};
            type Break = fn(&Path, &Path, &Path);
            let cases: [(&str, Break, &str); 7] = [
                (
                    "a second name that is not a temporary",
                    |secrets, marker, _| {
                        std::fs::hard_link(marker, secrets.join("parent-retention.json.bak"))
                            .unwrap()
                    },
                    "has two names and the other is not its own publication temporary",
                ),
                (
                    "a second name outside the Vault",
                    |_, marker, state| std::fs::hard_link(marker, state.join("elsewhere")).unwrap(),
                    "has two names and the other is not its own publication temporary",
                ),
                (
                    "a link named like the temporary",
                    |secrets, marker, state| {
                        symlink(marker, marker_tmp(secrets)).unwrap();
                        std::fs::hard_link(marker, state.join("elsewhere")).unwrap();
                    },
                    "has two names and the other is not its own publication temporary",
                ),
                (
                    "another file named like the temporary",
                    |secrets, marker, state| {
                        let copy = marker_tmp(secrets);
                        std::fs::copy(marker, &copy).unwrap();
                        std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o600))
                            .unwrap();
                        std::fs::hard_link(marker, state.join("elsewhere")).unwrap();
                    },
                    "has two names and the other is not its own publication temporary",
                ),
                (
                    "three names",
                    |secrets, marker, state| {
                        std::fs::hard_link(marker, marker_tmp(secrets)).unwrap();
                        std::fs::hard_link(marker, state.join("elsewhere")).unwrap();
                    },
                    "must be a regular file readable by its owner only",
                ),
                (
                    "a readable marker",
                    |secrets, marker, _| {
                        std::fs::hard_link(marker, marker_tmp(secrets)).unwrap();
                        std::fs::set_permissions(marker, std::fs::Permissions::from_mode(0o644))
                            .unwrap();
                    },
                    "must be a regular file readable by its owner only",
                ),
                (
                    "no classification pin",
                    |secrets, marker, _| {
                        std::fs::hard_link(marker, marker_tmp(secrets)).unwrap();
                        std::fs::OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .open(secrets.join("retention.lock"))
                            .unwrap();
                    },
                    "has two names and no classification pin",
                ),
            ];
            for (case, break_marker, reason) in cases {
                let (f, r, v, k) = prepared();
                let mut c = VaultCursor::default();
                assert!(tick(&f, &v, &k, &mut c, 100, 10).held, "{case}");
                let secrets = f.state.join("secrets");
                let marker = secrets.join("parent-retention.json");
                break_marker(&secrets, &marker, &f.state);
                let names = |dir: &Path| {
                    let mut names: Vec<_> = std::fs::read_dir(dir)
                        .unwrap()
                        .map(|e| e.unwrap().file_name())
                        .collect();
                    names.sort();
                    names
                };
                let before = (names(&secrets), names(&f.state));
                let error = match Vault::open(&f.state) {
                    Ok(_) => panic!("{case}: opened"),
                    Err(error) => error.to_string(),
                };
                assert!(error.contains("registered parent Vault"), "{case}: {error}");
                assert!(error.contains(reason), "{case}: {error}");
                assert!(v.load(&r, &entries(&f, &r), &k).is_err(), "{case}");
                // Refusing removed and repaired nothing.
                assert_eq!((names(&secrets), names(&f.state)), before, "{case}");
                assert!(secrets.join(format!("{r}.json")).exists(), "{case}");
            }
        }
        #[test]
        fn vault_page_goes_on_past_a_temporary_it_cannot_unlink() {
            let (f, r, v, k) = prepared();
            let mut c = VaultCursor::default();
            assert!(tick(&f, &v, &k, &mut c, 100, 10).held);
            let secrets = f.state.join("secrets");
            let mut stuck = Vec::new();
            for _ in 0..3 {
                let name = format!(".{}.json.{}.tmp", RunId::generate(), ulid::Ulid::new());
                std::fs::write(secrets.join(&name), b"TOKEN=never-published-fixture\n").unwrap();
                std::fs::set_permissions(
                    secrets.join(&name),
                    std::fs::Permissions::from_mode(0o600),
                )
                .unwrap();
                stuck.push(name);
            }
            stuck.sort();
            // The first entry of the page cannot be unlinked (an immutable
            // flag, say). The other temporaries and the retention go on.
            crate::secrets::vault::set_sweep_fault(Some(("unlink", stuck[0].clone())));
            let page = tick(&f, &v, &k, &mut c, 101, 11);
            assert_eq!(
                (
                    page.tmp_removed,
                    page.tmp_retained,
                    page.unknown,
                    page.claimed,
                    page.removed
                ),
                (2, 1, 0, 1, 1)
            );
            assert!(secrets.join(&stuck[0]).exists());
            assert!(!secrets.join(&stuck[1]).exists() && !secrets.join(&stuck[2]).exists());
            assert!(!secrets.join(format!("{r}.json")).exists());
            // Once it can be removed, a later page removes it.
            crate::secrets::vault::set_sweep_fault(None);
            let page = tick(&f, &v, &k, &mut c, 102, 12);
            assert_eq!((page.tmp_removed, page.tmp_retained), (1, 0));
            assert!(!secrets.join(&stuck[0]).exists());
        }
        #[test]
        fn vault_secret_free_terminal_run_keeps_source_only_retry_available() {
            let f = Fixture::new();
            let r = RunId::generate();
            f.bind(&r);
            f.terminal(&r);
            let v = Vault::open(&f.state).unwrap();
            let k = ClusterKey::from_bytes(vec![7; 32]).unwrap();
            f.store
                .retention
                .connect()
                .unwrap()
                .execute(
                    "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                    [],
                )
                .unwrap();
            let mut c = VaultCursor::default();
            tick(&f, &v, &k, &mut c, 100, 10);
            assert_eq!(tick(&f, &v, &k, &mut c, 101, 11).claimed, 0);
            assert!(v.load(&r, &[], &k).is_ok());
            v.lease(true)
                .unwrap()
                .publish(&format!("{r}.json"), b"unknown-owned-fixture")
                .unwrap();
            c = VaultCursor::default();
            let page = tick(&f, &v, &k, &mut c, 102, 12);
            assert_eq!((page.claimed, page.unknown), (0, 1));
            assert!(v.has(&r));
        }
        #[test]
        fn vault_unregistered_erase_still_allows_worker_reupload() {
            let (f, r, v, k) = prepared();
            let files = entries(&f, &r);
            let held = v.load(&r, &files, &k).unwrap();
            let path = f.state.join("secrets").join(format!("{r}.json"));
            let bytes = std::fs::read(&path).unwrap();
            v.erase(&r).unwrap();
            let copy = f._root.path().join("worker-copy");
            std::fs::write(&copy, bytes).unwrap();
            std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o600)).unwrap();
            v.adopt(&r, &copy, &files, &k).unwrap();
            assert!(v.has(&r));
            drop(held);
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn vault_encoded_queue_outlives_timeout_and_dropped_sender_until_writer_death() {
            queued_sender(false).await;
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn vault_encoded_queue_and_current_frame_release_after_owned_writer_abort() {
            queued_sender(true).await;
        }
        async fn queued_sender(abort: bool) {
            use crate::protocol::mux::{run_writer, Outbox, Shared};
            use crate::protocol::Client;
            use crate::secrets::send::send_bundle;
            use std::time::Duration;
            use tokio::io::AsyncReadExt;
            let (f, r, v, k) = prepared();
            let mut cursor = VaultCursor::default();
            tick(&f, &v, &k, &mut cursor, 100, 10);
            let held = v.load(&r, &entries(&f, &r), &k).unwrap();
            let shared = Shared::new(crate::protocol::MAX_FRAME_LEN);
            let outbox = Outbox::new();
            let client = Client::new(Arc::clone(&shared), Arc::clone(&outbox));
            assert!(send_bundle(
                &client,
                &held,
                crate::ids::CoordinatorEpoch(1),
                &AttemptId::generate(),
                crate::ids::Generation(1),
                Duration::from_millis(5)
            )
            .await
            .is_err());
            let attempt = AttemptId::generate();
            assert!(tokio::time::timeout(
                Duration::from_millis(5),
                send_bundle(
                    &client,
                    &held,
                    crate::ids::CoordinatorEpoch(1),
                    &attempt,
                    crate::ids::Generation(1),
                    Duration::from_secs(60)
                )
            )
            .await
            .is_err());
            drop(held);
            assert!(tick(&f, &v, &k, &mut cursor, 101, 11).busy);
            let (ours, mut peer) = tokio::io::duplex(1);
            let writer = tokio::spawn(run_writer(ours, Arc::clone(&outbox), Arc::clone(&shared)));
            tokio::time::timeout(Duration::from_secs(5), peer.read_exact(&mut [0; 1]))
                .await
                .unwrap()
                .unwrap();
            assert!(tick(&f, &v, &k, &mut cursor, 101, 11).busy);
            if abort {
                writer.abort();
                assert!(writer.await.unwrap_err().is_cancelled());
                drop(peer);
            } else {
                drop(peer);
                tokio::time::timeout(Duration::from_secs(5), writer)
                    .await
                    .unwrap()
                    .unwrap();
            }
            // Client/outbox remain alive: dead queued frames must be discarded.
            assert!(client.is_closed());
            assert_eq!(tick(&f, &v, &k, &mut cursor, 101, 11).removed, 1);
        }
        #[test]
        fn vault_fresh_exact_owner_digest_queue_and_collection_proofs_are_required() {
            for kind in ["digest", "unknown", "retired", "collection", "queued"] {
                let (f, r, v, k) = prepared();
                let mut c = VaultCursor::default();
                tick(&f, &v, &k, &mut c, 100, 10);
                if kind == "queued" {
                    f.owner
                        .create_task(&TaskId::generate(), &r, &serde_json::json!({}), 6)
                        .unwrap();
                } else {
                    f.owner
                        .db
                        .call(move |db| {
                            match kind {
                                "digest" => db.execute(
                                    "UPDATE source_ref SET manifest_digest='sha256:wrong'",
                                    [],
                                )?,
                                "unknown" => {
                                    db.execute("UPDATE attempt SET observation='unknown'", [])?
                                }
                                "retired" => {
                                    db.execute("UPDATE attempt SET retired=1,retired_at_ms=6", [])?
                                }
                                "collection" => db.execute(
                                    "UPDATE collection SET closed_at_ms=NULL,verdict=NULL",
                                    [],
                                )?,
                                _ => unreachable!(),
                            };
                            Ok(())
                        })
                        .unwrap();
                }
                let page = tick(&f, &v, &k, &mut c, 101, 11);
                assert_eq!(page.claimed, 0, "{kind}: {page:?}");
                assert!(v.has(&r), "{kind}");
            }
        }
    }
}
