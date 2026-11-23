//! Persistent roots and cross-process exclusion for captured inputs.
//! Explicit parent retention pages are separate from the nondeleting preview.
mod capture_owner;
mod deletion;
mod foreground;
mod import_owner;
pub(crate) use capture_owner::stop_point;
pub use capture_owner::{
    inspect_corrupt_held, inspect_owners, inspect_retry_failing, CaptureOwner, CaptureVerdict,
};
#[cfg(test)]
pub use deletion::set_prune_fault;
pub(in crate::source) use deletion::Reserved;
pub use deletion::{abandoned_imports_read_only, ImportReleaseReport};
pub use deletion::{
    AbandonCursor, AbandonReport, AbandonScope, ExclusiveGuard, Gate, PruneBudget, PruneInputs,
};
pub(crate) use foreground::ForegroundBusy;
pub use import_owner::{is_import_token_id, ForgetReport};
mod vault_retention;
use super::SourceStore;
use anyhow::{ensure, Context, Result};
pub use deletion::{ReceivedPassInputs, ReceivedPassReport, SourceUse};
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
    /// When this process last started a recount of `store_bytes`.
    recounted: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    #[cfg(test)]
    reads: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    verifications: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    page_pause: Arc<std::sync::Mutex<Option<PagePause>>>,
    #[cfg(test)]
    foreground_waits: Arc<std::sync::atomic::AtomicUsize>,
    /// How many times a lock take synced the lock file (tests only).
    #[cfg(test)]
    lock_syncs: Arc<std::sync::atomic::AtomicUsize>,
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
/// The file and whether this call created it.
fn open_permanent_file(root: &File, name: &str) -> Result<(File, bool)> {
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
        Ok(file) => Ok((file, true)),
        Err(rustix::io::Errno::EXIST) => {
            rustix::fs::openat(root, name, flags, rustix::fs::Mode::empty()).map(|f| (f, false))
        }
        Err(error) => Err(error),
    }?;
    Ok((file.0.into(), file.1))
}
/// Why the store's mutation lock cannot be taken for a reason other than
/// being busy (read only, makes nothing): the file is not a plain file of
/// this user with one link and no group or other access, or cannot be
/// opened. `None`: there is no lock file yet, or it is fine. A store like
/// this refuses every acceptance as INTERNAL (spec 25.8 5b (a)).
pub fn mutation_lock_problem(state_dir: &Path) -> Option<String> {
    let path = state_dir.join("sources").join("mutation.lock");
    let m = match std::fs::symlink_metadata(&path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
        Err(e) => return Some(format!("cannot be examined: {e}")),
    };
    if !m.is_file() {
        return Some("is not a plain file".into());
    }
    if m.nlink() != 1 || m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o077 != 0 {
        return Some("has another owner, more than one link, or group/other access".into());
    }
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
    {
        Ok(_) => None,
        Err(e) => Some(format!("cannot be opened: {e}")),
    }
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
            recounted: Default::default(),
            #[cfg(test)]
            reads: Default::default(),
            #[cfg(test)]
            verifications: Default::default(),
            #[cfg(test)]
            page_pause: Default::default(),
            #[cfg(test)]
            foreground_waits: Default::default(),
            #[cfg(test)]
            lock_syncs: Default::default(),
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
            self.ensure_capture_owner_table(&mut db)?;
            self.ensure_import_session_index(&db)?;
            return Self::ensure_size_accounting(&mut db);
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
          CREATE INDEX IF NOT EXISTS import_session_run ON import_session(run_id);
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          INSERT OR IGNORE INTO meta VALUES('legacy_registration','pending');")?;
        tx.execute_batch(capture_owner::TABLE)?;
        tx.pragma_update(None, "user_version", 1)?;
        tx.commit()?;
        Self::ensure_size_accounting(&mut db)
    }
    /// Stage 5b (spec 25.13): the table `object_bytes` with the total
    /// `store_bytes`, `root.used_ms` and its index, the index `graph(source,
    /// root)` and the tables `clone_ref` and `legacy_queue`. Additive tables
    /// only, so the version stays 1 and an older binary still opens and
    /// writes the index: no column is added to a table an older binary
    /// inserts into positionally (`object` is one; `root` is always written
    /// with its columns named). Everything is created idempotently on every
    /// open; the write lock is taken only when something is missing. The
    /// rows that were there before are filled by the worker's passes:
    /// `bytes_fill` and `clone_ref_fill` hold their cursors (`done` at the
    /// end).
    fn ensure_size_accounting(db: &mut Connection) -> Result<()> {
        const CHECK: &str = "SELECT NOT EXISTS(SELECT 1 FROM pragma_table_info('object') WHERE name='bytes')
            AND EXISTS(SELECT 1 FROM pragma_table_info('root') WHERE name='used_ms')
            AND (SELECT count(*) FROM sqlite_master WHERE name IN ('object_bytes','clone_ref','legacy_queue','legacy_retry','clone_ref_retry','bytes_retry','root_used','graph_source','clone_ref_root'))=9
            AND (SELECT count(*) FROM meta WHERE key IN ('store_bytes','bytes_fill','clone_ref_fill'))=3";
        let ready: bool = db.query_row(CHECK, [], |r| r.get(0))?;
        if ready {
            return Ok(());
        }
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let used: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('root') WHERE name='used_ms')",
            [],
            |r| r.get(0),
        )?;
        if !used {
            tx.execute_batch("ALTER TABLE root ADD COLUMN used_ms INTEGER;")?;
        }
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS object_bytes(id TEXT PRIMARY KEY,bytes INTEGER NOT NULL);
             CREATE INDEX IF NOT EXISTS root_used ON root(kind,used_ms);
             CREATE INDEX IF NOT EXISTS graph_source ON graph(source,root);
             CREATE TABLE IF NOT EXISTS clone_ref(clone TEXT NOT NULL,source TEXT NOT NULL,root TEXT NOT NULL,PRIMARY KEY(clone,source,root));
             CREATE INDEX IF NOT EXISTS clone_ref_root ON clone_ref(root);
             CREATE TABLE IF NOT EXISTS legacy_queue(name TEXT PRIMARY KEY);
             CREATE TABLE IF NOT EXISTS legacy_retry(name TEXT PRIMARY KEY,attempts INTEGER NOT NULL,next_ms INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS clone_ref_retry(root TEXT PRIMARY KEY,attempts INTEGER NOT NULL,next_ms INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS bytes_retry(id TEXT PRIMARY KEY,attempts INTEGER NOT NULL,next_ms INTEGER NOT NULL);
             INSERT OR IGNORE INTO meta VALUES('store_bytes','0');
             INSERT OR IGNORE INTO meta VALUES('bytes_fill','');
             INSERT OR IGNORE INTO meta VALUES('clone_ref_fill','');",
        )?;
        // An index an earlier build of this change made has the column
        // `object.bytes`, which an older binary's positional insert into
        // `object` cannot fill: its counted sizes move to `object_bytes` and
        // the column goes.
        let column: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('object') WHERE name='bytes')",
            [],
            |r| r.get(0),
        )?;
        if column {
            tx.execute_batch(
                "INSERT OR IGNORE INTO object_bytes SELECT id,bytes FROM object WHERE bytes IS NOT NULL;
                 ALTER TABLE object DROP COLUMN bytes;",
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Record that `object`'s file is placed with `bytes` bytes: only the
    /// change from no `object_bytes` row to one adds to `store_bytes` (spec
    /// 25.8 5b (c)). Call inside the caller's transaction.
    pub(super) fn record_bytes(c: &Connection, object: &str, bytes: u64) -> Result<()> {
        let changed = c.execute(
            "INSERT OR IGNORE INTO object_bytes(id,bytes) SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM object WHERE id=?1)",
            params![object, i64::try_from(bytes)?],
        )?;
        if changed == 1 {
            Self::add_store_bytes(c, object, i64::try_from(bytes)?)?;
        }
        Ok(())
    }
    /// Add `delta` to `store_bytes` while it holds a non-negative integer
    /// (anything else is invalid and waits for the recount), and to the
    /// running sum of a recount that has already passed `object`.
    fn add_store_bytes(c: &Connection, object: &str, delta: i64) -> Result<()> {
        c.execute(
            "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+?1 AS TEXT) WHERE key='store_bytes' AND value<>'' AND value NOT GLOB '*[^0-9]*'",
            [delta],
        )?;
        c.execute(
            "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+?1 AS TEXT) WHERE key='bytes_recount_sum' AND EXISTS(SELECT 1 FROM meta WHERE key='bytes_recount' AND ?2<=value)",
            params![delta, object],
        )?;
        Ok(())
    }
    /// Delete `object`'s row when `condition` (SQL over `?1`) holds, with
    /// its `object_bytes` row, taking its counted bytes off `store_bytes` in
    /// the same transaction.
    pub(super) fn forget_object(c: &Connection, object: &str, condition: &str) -> Result<()> {
        let gone: Option<String> = c
            .query_row(
                &format!("DELETE FROM object WHERE id=?1 AND {condition} RETURNING id"),
                [object],
                |r| r.get(0),
            )
            .optional()?;
        if gone.is_none() {
            return Ok(());
        }
        let bytes: Option<i64> = c
            .query_row(
                "DELETE FROM object_bytes WHERE id=?1 RETURNING bytes",
                [object],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(bytes) = bytes {
            Self::add_store_bytes(c, object, -bytes)?;
        }
        Ok(())
    }
    /// The size of `path` (relative to the store) when it is a regular file
    /// there, from a stat; `None` when there is no file (or no regular one)
    /// at that name. Any other failure (`EACCES`, `EIO`…) is an error: it
    /// says nothing about the file, and the caller tries again later.
    fn stat_bytes(&self, path: &str) -> Result<Option<u64>> {
        match rustix::fs::statat(&*self.root, path, rustix::fs::AtFlags::SYMLINK_NOFOLLOW) {
            Ok(st) => Ok((rustix::fs::FileType::from_raw_mode(st.st_mode)
                == rustix::fs::FileType::RegularFile)
                .then_some(st.st_size as u64)),
            Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTDIR) => Ok(None),
            Err(error) => Err(anyhow::Error::from(error).context(format!("stat {path}"))),
        }
    }
    fn record_stat(&self, c: &Connection, object: &str, path: &str) -> Result<()> {
        if let Some(bytes) = self.stat_bytes(path)? {
            Self::record_bytes(c, object, bytes)?;
        }
        Ok(())
    }
    /// The import placed `object`'s file with `bytes` bytes: the size row
    /// and the total are written in one transaction.
    pub(crate) fn placed(&self, object: &str, bytes: u64) -> Result<()> {
        let mut c = self.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::record_bytes(&tx, object, bytes)?;
        tx.commit()?;
        Ok(())
    }
    /// One page of the fill of the rows with no size (spec 25.8 5b (c),
    /// 25.13): up to `limit` object rows, first those of `bytes_retry` that
    /// are due and then those in id order from the cursor `meta.bytes_fill`,
    /// are stat-ed and counted when not counted yet; a row with no file
    /// stays uncounted and is counted when its file is placed. A stat that
    /// fails for another reason (`EACCES`…) gets a row in `bytes_retry`
    /// (tried again after the delay of [`Self::defer`]) and the page goes
    /// on, so one object never blocks the rest; the row goes when the
    /// object is counted or gone. The fill is finished (`done`) when the
    /// cursor has reached the end and `bytes_retry` is empty; each recount
    /// starts it again from the start (rows an older binary added after a
    /// rollback have no size). Returns `true` once it is finished.
    pub(crate) fn fill_bytes(&self, limit: u32) -> Result<bool> {
        let mut c = self.connect()?;
        let now = crate::platform::clock::wall_ms() as i64;
        let cursor = Self::fill_cursor(&c, "bytes_fill")?;
        let due: Vec<(String, Option<String>, bool)> = {
            let mut s = c.prepare("SELECT r.id,o.path,EXISTS(SELECT 1 FROM object_bytes b WHERE b.id=r.id) FROM bytes_retry r LEFT JOIN object o ON o.id=r.id WHERE r.next_ms<=?1 OR r.next_ms>?1+3600000 ORDER BY r.id LIMIT ?2")?;
            let rows = s.query_map(params![now, limit], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let fresh: Vec<(String, String, bool)> = match &cursor {
            Some(cursor) => {
                let mut s = c.prepare("SELECT o.id,o.path,EXISTS(SELECT 1 FROM object_bytes b WHERE b.id=o.id) FROM object o WHERE o.id>?1 ORDER BY o.id LIMIT ?2")?;
                let rows = s.query_map(params![cursor, limit], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
                rows.collect::<std::result::Result<_, _>>()?
            }
            None => Vec::new(),
        };
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows = due
            .iter()
            .map(|(id, path, counted)| (id, path.as_deref(), *counted))
            .chain(
                fresh
                    .iter()
                    .map(|(id, path, counted)| (id, Some(path.as_str()), *counted)),
            );
        for (id, path, counted) in rows {
            let outcome = match path {
                Some(path) if !counted => self.record_stat(&tx, id, path),
                _ => Ok(()),
            };
            match outcome {
                Ok(()) => {
                    tx.execute("DELETE FROM bytes_retry WHERE id=?1", [id])?;
                }
                Err(e) => {
                    eprintln!("source store size fill of {id}: {e:#}");
                    Self::defer(&tx, "bytes_retry", "id", id, now)?;
                }
            }
        }
        if cursor.is_some() {
            let next = if fresh.len() < limit as usize {
                "done"
            } else {
                &fresh.last().unwrap().0
            };
            tx.execute("UPDATE meta SET value=?1 WHERE key='bytes_fill'", [next])?;
        }
        let finished = Self::fill_cursor(&tx, "bytes_fill")?.is_none()
            && !tx.query_row("SELECT EXISTS(SELECT 1 FROM bytes_retry)", [], |r| {
                r.get::<_, bool>(0)
            })?;
        tx.commit()?;
        Ok(finished)
    }
    fn meta_value(c: &Connection, key: &str) -> Result<Option<String>> {
        Ok(
            c.query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
                .optional()?,
        )
    }
    /// One page of the recount of `store_bytes` (spec 25.8 5b (c)). After a
    /// rollback an older binary deletes `object` rows without their sizes,
    /// adds rows without sizes, and completes `received` roots without
    /// `used_ms` and `clone_ref` rows. A recount starts when the fills are
    /// finished: at the first page of this process, then once an hour, and
    /// at once whenever `store_bytes` is not a non-negative integer. It first
    /// sets the cursors of both fills back to the start
    /// (`meta.bytes_recount_wait`), waits until both are finished again
    /// (they touch only rows with no size and roots with no `used_ms`, so
    /// doing them again counts nothing twice), and then walks `object_bytes`
    /// in id order, `limit` rows a page, deletes the rows with no `object`
    /// row and sums the rest (`meta.bytes_recount` is its cursor,
    /// `bytes_recount_sum` its sum; a change to a row the cursor has passed
    /// also changes the sum); at the end the sum becomes `store_bytes`. The
    /// state is read first without a transaction; the write lock is taken
    /// only when there is work. Returns `true` when no recount is left in
    /// progress.
    pub(crate) fn recount_bytes(&self, limit: u32) -> Result<bool> {
        const EVERY: std::time::Duration = std::time::Duration::from_secs(3600);
        let mut c = self.connect()?;
        let fills_done = || -> Result<bool> {
            Ok(Self::fill_cursor(&c, "bytes_fill")?.is_none()
                && Self::fill_cursor(&c, "clone_ref_fill")?.is_none()
                && !c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM clone_ref_retry) OR EXISTS(SELECT 1 FROM bytes_retry)",
                    [],
                    |r| r.get::<_, bool>(0),
                )?)
        };
        let waiting = Self::meta_value(&c, "bytes_recount_wait")?.is_some();
        let cursor = Self::meta_value(&c, "bytes_recount")?;
        if waiting {
            if !fills_done()? {
                return Ok(false);
            }
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(
                "DELETE FROM meta WHERE key='bytes_recount_wait';
                 INSERT OR REPLACE INTO meta VALUES('bytes_recount','');
                 INSERT OR REPLACE INTO meta VALUES('bytes_recount_sum','0');",
            )?;
            tx.commit()?;
            return Ok(false);
        }
        let Some(cursor) = cursor else {
            let due = self
                .recounted
                .lock()
                .unwrap()
                .is_none_or(|at| at.elapsed() >= EVERY)
                || self.store_bytes()?.is_none();
            if !due || !fills_done()? {
                return Ok(true);
            }
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(
                "UPDATE meta SET value='' WHERE key IN ('bytes_fill','clone_ref_fill');
                 INSERT OR REPLACE INTO meta VALUES('bytes_recount_wait','1');",
            )?;
            tx.commit()?;
            *self.recounted.lock().unwrap() = Some(std::time::Instant::now());
            return Ok(false);
        };
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows: Vec<(String, i64, bool)> = {
            let mut s = tx.prepare("SELECT b.id,b.bytes,EXISTS(SELECT 1 FROM object o WHERE o.id=b.id) FROM object_bytes b WHERE b.id>?1 ORDER BY b.id LIMIT ?2")?;
            let rows = s.query_map(params![cursor, limit], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let mut sum = 0i64;
        for (id, bytes, kept) in &rows {
            if *kept {
                sum += bytes;
            } else {
                tx.execute("DELETE FROM object_bytes WHERE id=?1", [id])?;
            }
        }
        tx.execute(
            "UPDATE meta SET value=CAST(CAST(value AS INTEGER)+?1 AS TEXT) WHERE key='bytes_recount_sum'",
            [sum],
        )?;
        let finished = rows.len() < limit as usize;
        if finished {
            tx.execute_batch(
                "UPDATE meta SET value=(SELECT value FROM meta WHERE key='bytes_recount_sum') WHERE key='store_bytes';
                 DELETE FROM meta WHERE key IN ('bytes_recount','bytes_recount_sum');",
            )?;
        } else {
            tx.execute(
                "UPDATE meta SET value=?1 WHERE key='bytes_recount'",
                [&rows.last().unwrap().0],
            )?;
        }
        tx.commit()?;
        Ok(finished)
    }
    /// Record a failure of `key` that may pass (spec 25.8 5b (h)5, (e)):
    /// the row in `table` counts the attempts and is tried again after a
    /// delay that doubles from 10 s up to 1 hour. The time is the wall
    /// clock, because a row outlives the process; a `next_ms` more than an
    /// hour ahead (the clock went back) counts as due.
    fn defer(c: &Connection, table: &str, column: &str, key: &str, now: i64) -> Result<()> {
        c.execute(
            &format!(
                "INSERT INTO {table}({column},attempts,next_ms) VALUES(?1,1,?2+10000)
                 ON CONFLICT({column}) DO UPDATE SET attempts=attempts+1,next_ms=?2+min(10000<<min(attempts,9),3600000)"
            ),
            params![key, now],
        )?;
        Ok(())
    }
    /// One page of the `clone_ref` migration (spec 25.8 5b (e)): for up to
    /// `limit` `received` roots, first those of `clone_ref_retry` that are
    /// due and then those from the cursor `meta.clone_ref_fill`, reading at
    /// most `budget` bytes of manifests (at least one manifest), a row for
    /// each of the root's graph rows, in the clone of the root's manifest's
    /// `repository_id` (where the code before 5b put the refs). A root whose
    /// manifest is not valid gets no rows; one that cannot be read now (any
    /// other error) gets a row in `clone_ref_retry` and the page goes on
    /// (the row goes when the root is done or gone, so the table is never
    /// bigger than the roots). The same page gives a root with no `used_ms`
    /// the time of the fill (spec 25.8 5b (c)). `true` once the cursor is at
    /// the end and no retry row is left. Only roots with no `used_ms` are
    /// selected (a root an older binary completed never gets one, and a
    /// marked root is skipped without its manifest being read, so a recount
    /// reads the manifests of the unmarked roots only); a retry row of a
    /// root that was marked meanwhile is dropped.
    pub(crate) fn fill_clone_refs(&self, limit: u32, budget: u64) -> Result<bool> {
        let mut c = self.connect()?;
        let now = crate::platform::clock::wall_ms() as i64;
        let cursor = Self::fill_cursor(&c, "clone_ref_fill")?;
        let due: Vec<(String, Option<String>)> = {
            let mut s = c.prepare("SELECT r.root,o.source FROM clone_ref_retry r LEFT JOIN root o ON o.id=r.root AND o.kind='received' AND o.used_ms IS NULL WHERE r.next_ms<=?1 OR r.next_ms>?1+3600000 ORDER BY r.root LIMIT ?2")?;
            let rows = s.query_map(params![now, limit], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let fresh: Vec<(String, Option<String>)> = match &cursor {
            Some(cursor) => {
                let mut s = c.prepare("SELECT id,source FROM root WHERE kind='received' AND used_ms IS NULL AND id>?1 AND source IS NOT NULL ORDER BY id LIMIT ?2")?;
                let rows = s.query_map(params![cursor, limit], |r| Ok((r.get(0)?, r.get(1)?)))?;
                rows.collect::<std::result::Result<_, _>>()?
            }
            None => Vec::new(),
        };
        enum Outcome {
            Done(Option<String>),
            Gone,
            Retry,
        }
        let manifest_limit = crate::worker::blob_policy::BlobLimits::default().max_manifest_bytes;
        let mut outcomes: Vec<(&str, Outcome, bool)> = Vec::new();
        let mut read = 0u64;
        let mut stopped = false;
        let mut fresh_seen = 0;
        for (root, source, is_fresh) in due
            .iter()
            .map(|(r, s)| (r, s, false))
            .chain(fresh.iter().map(|(r, s)| (r, s, true)))
        {
            let Some(source) = source else {
                outcomes.push((root, Outcome::Gone, is_fresh));
                continue;
            };
            // (the first manifest of a page is read whatever its size)
            let left = if read == 0 {
                manifest_limit
            } else {
                manifest_limit.min(budget.saturating_sub(read))
            };
            let outcome = match self.load_validated(source, left) {
                Ok(v) => {
                    read += v.metadata.as_ref().map_or(0, |m| m.len()).max(1);
                    Outcome::Done(v.manifest.repository_id.clone())
                }
                Err(e) if left < manifest_limit && e.is::<ManifestLimit>() => {
                    stopped = true;
                    break;
                }
                Err(e) if manifest_invalid(&e) => Outcome::Done(None),
                Err(e) => {
                    eprintln!("source store clone_ref fill of {root}: {e:#}");
                    Outcome::Retry
                }
            };
            if is_fresh {
                fresh_seen += 1;
            }
            outcomes.push((root, outcome, is_fresh));
        }
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (root, outcome, _) in &outcomes {
            match outcome {
                Outcome::Done(repository) => {
                    if let Some(repository) = repository {
                        tx.execute(
                            "INSERT OR IGNORE INTO clone_ref(clone,source,root) SELECT ?2,source,root FROM graph WHERE root=?1",
                            params![root, crate::workspace::plan::clone_dir_name(repository)],
                        )?;
                    }
                    tx.execute(
                        "UPDATE root SET used_ms=?2 WHERE id=?1 AND used_ms IS NULL",
                        params![root, now],
                    )?;
                    tx.execute("DELETE FROM clone_ref_retry WHERE root=?1", [root])?;
                }
                Outcome::Gone => {
                    tx.execute("DELETE FROM clone_ref_retry WHERE root=?1", [root])?;
                }
                Outcome::Retry => Self::defer(&tx, "clone_ref_retry", "root", root, now)?,
            }
        }
        if cursor.is_some() {
            let next = if !stopped && fresh_seen == fresh.len() && fresh.len() < limit as usize {
                Some("done")
            } else {
                outcomes
                    .iter()
                    .rev()
                    .find(|(_, _, is_fresh)| *is_fresh)
                    .map(|(root, _, _)| *root)
            };
            if let Some(next) = next {
                tx.execute(
                    "UPDATE meta SET value=?1 WHERE key='clone_ref_fill'",
                    [next],
                )?;
            }
        }
        let finished = Self::fill_cursor(&tx, "clone_ref_fill")?.is_none()
            && !tx.query_row("SELECT EXISTS(SELECT 1 FROM clone_ref_retry)", [], |r| {
                r.get::<_, bool>(0)
            })?;
        tx.commit()?;
        Ok(finished)
    }
    fn fill_cursor(c: &Connection, key: &str) -> Result<Option<String>> {
        let v: Option<String> = c
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
            .optional()?;
        Ok(v.filter(|v| v != "done"))
    }
    /// Whether the legacy registration may still register `id` (spec 25.8
    /// 5b (h)5): `id` is still queued, and its `m:` row may exist before its
    /// root is complete. (Before the manifests are listed no registration
    /// has begun, and an `m:` row is a publication's, which has not
    /// finished.)
    pub(crate) fn legacy_queued(&self, id: &str) -> Result<bool> {
        Ok(self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM legacy_queue WHERE name=?1)",
            [id],
            |r| r.get(0),
        )?)
    }
    /// One step of the registration of the manifests received before stage
    /// 5a, which have no `receive:<id>` root (spec 25.8 5b (h)5). The first
    /// call lists `manifests/` once into `legacy_queue` (one row per
    /// manifest there then, without its root); each call then works on the
    /// first queued manifest that is not waiting in `legacy_retry`, at most
    /// one manifest read and 992 (31 pages of 32) entries, and the next call
    /// goes on from where the graph rows stopped. A manifest that is not
    /// valid (its JSON, identity or seal, a missing manifest of its own or
    /// of a child, or the size limit) leaves its root `abandoned` with no
    /// source and no graph rows, and its row leaves the queue; its members,
    /// the `m:` row among them, stay on the abandoned root, whose release
    /// (5a's abandonment, 5b's step 0) deletes what it can prove. Any other
    /// error keeps the queue row and records it in `legacy_retry` (tried
    /// again after a doubling delay, [`Self::defer`]), so that the next call
    /// goes on with the next manifest; it is never abandoned for that.
    /// Nothing is done in a store with a parent's root. `true` once the
    /// queue is empty (`legacy_retry` holds only queued names). The marker
    /// `legacy_registration` is left as it is: turning it `done` opens the
    /// gate of the deleting passes and is part of the change that brings
    /// them, which may do so only while neither table has a row.
    pub(crate) fn register_legacy(&self) -> Result<bool> {
        if !self.legacy_registration_pending()? || self.holds_parent_roots()? {
            return Ok(true);
        }
        let mut c = self.connect()?;
        let listed: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='legacy_queue')",
            [],
            |r| r.get(0),
        )?;
        if !listed {
            let fd = rustix::fs::openat(&*self.root, "manifests", DIR, rustix::fs::Mode::empty());
            let mut names = Vec::new();
            match fd {
                Ok(fd) => {
                    for entry in rustix::fs::Dir::read_from(&fd)? {
                        let entry = entry?;
                        let Ok(name) = entry.file_name().to_str() else {
                            continue;
                        };
                        if let Some(id) = name.strip_suffix(".json") {
                            if super::classify::source_id_of_ci(id).is_some() {
                                names.push(id.to_string());
                            }
                        }
                    }
                }
                Err(rustix::io::Errno::NOENT) => {}
                Err(error) => return Err(error.into()),
            }
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            for id in &names {
                tx.execute(
                    "INSERT OR IGNORE INTO legacy_queue SELECT ?1 WHERE NOT EXISTS(SELECT 1 FROM root WHERE id='receive:'||?1 AND source=?1)",
                    [id],
                )?;
            }
            tx.execute("INSERT INTO meta VALUES('legacy_queue','listed')", [])?;
            tx.commit()?;
        }
        let now = crate::platform::clock::wall_ms() as i64;
        let Some(id): Option<String> = c
            .query_row(
                "SELECT q.name FROM legacy_queue q LEFT JOIN legacy_retry r ON r.name=q.name
                 WHERE r.name IS NULL OR r.next_ms<=?1 OR r.next_ms>?1+3600000 ORDER BY q.name LIMIT 1",
                [now],
                |r| r.get(0),
            )
            .optional()?
        else {
            return Ok(!c.query_row("SELECT EXISTS(SELECT 1 FROM legacy_queue)", [], |r| {
                r.get::<_, bool>(0)
            })?);
        };
        let manifest_limit = crate::worker::blob_policy::BlobLimits::default().max_manifest_bytes;
        let root = format!("receive:{id}");
        let step = (|| -> Result<bool> {
            // (registered meanwhile, by a receipt or through no queue row)
            if self.received_row(&id)? {
                return Ok(true);
            }
            self.pending(&root)?;
            let mut cache = None;
            let options = Step {
                pages: 31,
                loads: 1,
                mark: true,
            };
            self.register_step(&root, "received", &id, options, &mut cache, manifest_limit)
        })();
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        match step {
            Ok(false) => return Ok(false),
            Ok(true) => {}
            Err(error) if manifest_invalid(&error) => {
                let changed = tx.execute(
                    "UPDATE root SET kind='abandoned',source=NULL WHERE id=?1 AND source IS NULL",
                    [&root],
                )?;
                if changed == 1 {
                    tx.execute("DELETE FROM graph WHERE root=?1", [&root])?;
                }
            }
            Err(error) => {
                Self::defer(&tx, "legacy_retry", "name", &id, now)?;
                tx.commit()?;
                return Err(error.context(format!("legacy registration of {id}")));
            }
        }
        tx.execute("DELETE FROM legacy_queue WHERE name=?1", [&id])?;
        tx.execute("DELETE FROM legacy_retry WHERE name=?1", [&id])?;
        let empty: bool =
            tx.query_row("SELECT NOT EXISTS(SELECT 1 FROM legacy_queue)", [], |r| {
                r.get(0)
            })?;
        tx.commit()?;
        Ok(empty)
    }
    /// The total of the counted object files (`meta.store_bytes`), or
    /// `None` while it is not a non-negative integer (it waits for the
    /// recount, [`Self::recount_bytes`]).
    pub(crate) fn store_bytes(&self) -> Result<Option<u64>> {
        let v: Option<String> = self
            .connect()?
            .query_row("SELECT value FROM meta WHERE key='store_bytes'", [], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(v.and_then(|v| v.parse().ok()))
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
    fn sync_lock_created(&self, file: &File) -> Result<()> {
        #[cfg(test)]
        self.lock_syncs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        file.sync_all().context("sync source mutation lock")?;
        self.root
            .sync_all()
            .context("sync source mutation parent")?;
        Ok(())
    }
    pub(crate) fn guard(&self, exclusive: bool) -> Result<Guard> {
        self.current().context("check source root for lock")?;
        let (file, created) = open_permanent_file(&self.root, "mutation.lock")
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
        // Durability: the lock file holds no data, only its existence matters,
        // and a crash that loses it just has the next take create it again.
        // So the file and its directory entry are synced once, by the take
        // that created it, not by every take: acceptance takes the shared
        // lock on each request and must not pay two fsyncs (F_FULLFSYNC on
        // macOS) under `submitting`.
        if created {
            self.sync_lock_created(&file)?;
        }
        Ok(Guard {
            _file: Arc::new(LockedFile(file)),
            root: self.path.clone(),
            index: self.clone(),
        })
    }
    /// [`Self::guard`] shared, waiting up to `limit` for an exclusive holder.
    pub(crate) fn guard_waiting(&self, limit: std::time::Duration) -> Result<Guard> {
        let start = std::time::Instant::now();
        let mut pause = std::time::Duration::from_millis(10);
        loop {
            match self.guard(false) {
                Err(e)
                    if e.downcast_ref::<io::Error>()
                        .is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock)
                        && start.elapsed() + pause < limit =>
                {
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(std::time::Duration::from_millis(50));
                }
                other => return other,
            }
        }
    }
    /// [`Self::guard`] exclusive, waiting up to `limit`; `None` when it was
    /// not had in time.
    pub(crate) fn guard_exclusive_waiting(
        &self,
        limit: std::time::Duration,
    ) -> Result<Option<Guard>> {
        let start = std::time::Instant::now();
        let mut pause = std::time::Duration::from_millis(10);
        loop {
            match self.guard(true) {
                Ok(g) => return Ok(Some(g)),
                Err(e)
                    if e.downcast_ref::<io::Error>()
                        .is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock) =>
                {
                    if start.elapsed() + pause >= limit {
                        return Ok(None);
                    }
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(std::time::Duration::from_millis(500));
                }
                Err(e) => return Err(e),
            }
        }
    }
    /// The empty `receive:%` roots (no source yet: a publication cut before
    /// its registration), at most `limit`.
    pub(crate) fn empty_receive_roots(&self, limit: u32) -> Result<Vec<String>> {
        let c = self.connect()?;
        let mut s = c.prepare(
            "SELECT id FROM root WHERE kind='pending' AND source IS NULL AND id LIKE 'receive:%' ORDER BY id LIMIT ?1",
        )?;
        let rows = s.query_map([limit], |r| r.get(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }
    /// Whether the store holds a parent's root (`run`, `import`): a worker
    /// pass then changes nothing (spec 25.8 5b, the store is not shared).
    pub(crate) fn holds_parent_roots(&self) -> Result<bool> {
        Ok(self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE kind IN ('run','import'))",
            [],
            |r| r.get(0),
        )?)
    }
    /// An empty receive root whose manifest cannot be proven: abandoned (its
    /// members go by the later passes), and its graph rows removed.
    pub(crate) fn abandon_empty_receive_root(&self, root: &str) -> Result<bool> {
        let mut c = self.connect_for_import()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE root SET kind='abandoned' WHERE id=?1 AND kind='pending' AND source IS NULL",
            [root],
        )?;
        if changed == 1 {
            tx.execute("DELETE FROM graph WHERE root=?1", [root])?;
        }
        tx.commit()?;
        Ok(changed == 1)
    }
    /// Completed claim rows an older version kept (`done=1`): inert, they
    /// only kept their object's publication row. Deleted a page of
    /// `limit` at a time, at most `pages` pages per call (the rest goes on
    /// the next call); returns how many went. Fewer than `limit * pages`
    /// means none are left.
    pub(crate) fn forget_completed_claims(&self, limit: u32, pages: u32) -> Result<u64> {
        let mut gone = 0;
        for _ in 0..pages {
            let n = self.connect()?.execute(
                "DELETE FROM source_delete_claim WHERE rowid IN (SELECT rowid FROM source_delete_claim WHERE done=1 LIMIT ?1)",
                [limit],
            )?;
            gone += n as u64;
            if n < limit as usize {
                break;
            }
        }
        Ok(gone)
    }
    pub(crate) fn received_row(&self, id: &str) -> Result<bool> {
        Ok(self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE id='receive:'||?1 AND source=?1)",
            [id],
            |r| r.get(0),
        )?)
    }
    /// The time of the root `receive:<id>` when it is `received` (spec 25.8
    /// 5b (c), an acceptance). Only that root: a carried submodule is kept
    /// by its parent's graph row.
    pub(crate) fn touch_received(&self, id: &str, at_ms: u64) -> Result<()> {
        self.connect()?.execute(
            "UPDATE root SET used_ms=MAX(COALESCE(used_ms,0),?2) WHERE id='receive:'||?1 AND source=?1 AND kind='received'",
            params![id, i64::try_from(at_ms)?],
        )?;
        Ok(())
    }
    pub(crate) fn legacy_registration_pending(&self) -> Result<bool> {
        Ok(self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='legacy_registration' AND value<>'done')",
            [],
            |r| r.get(0),
        )?)
    }
    pub(crate) fn has_object(&self, id: &str) -> Result<bool> {
        Ok(self.connect()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM object WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?)
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
    /// The root of a publication that is being registered. A root that an
    /// earlier settlement abandoned (with no source) is pending again: the
    /// publication that crashes after this makes new graph and member rows
    /// under it, which start-up settlement must pick up (it selects only
    /// `pending`).
    pub(crate) fn pending(&self, id: &str) -> Result<()> {
        self.connect()?.execute(
            "INSERT INTO root(id,kind) VALUES(?1,'pending')
             ON CONFLICT(id) DO UPDATE SET kind='pending' WHERE kind='abandoned' AND source IS NULL",
            [id],
        )?;
        Ok(())
    }
    fn load_validated(&self, id: &str, limit: u64) -> Result<Validated> {
        ensure!(
            super::classify::source_id_of_ci(id).is_some(),
            InvalidManifest("invalid source manifest identity".into())
        );
        // (a manifest that is not there, or is a link, is invalid; any other
        // failure to open it says nothing about the manifest)
        let absent = |e: rustix::io::Errno| -> anyhow::Error {
            match e {
                rustix::io::Errno::NOENT | rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR => {
                    InvalidManifest(format!("source manifest {id}: {e}")).into()
                }
                e => e.into(),
            }
        };
        let directory: File =
            rustix::fs::openat(&*self.root, "manifests", DIR, rustix::fs::Mode::empty())
                .map_err(absent)?
                .into();
        let file: File = rustix::fs::openat(
            &directory,
            format!("{id}.json"),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(absent)?
        .into();
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file(),
            InvalidManifest("source manifest is not regular".into())
        );
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
            serde_json::from_reader((&file).take(metadata.len().saturating_add(1)))
                .map_err(|e| InvalidManifest(format!("source manifest {id}: {e}")))?;
        ensure!(
            manifest.source_id == id,
            InvalidManifest("source manifest identity mismatch".into())
        );
        super::store::verify_manifest(&manifest).map_err(|e| InvalidManifest(format!("{e:#}")))?;
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
        self.register_held_marked(root, kind, manifest, false)
    }
    /// [`Self::register_held`] for a `received` root that is published (the
    /// requested source of a receipt, not a submodule's source it carries):
    /// the root records the time and its `clone_ref` rows (spec 25.8 5b (c),
    /// (e)), also when it is published again after it was complete.
    pub(crate) fn register_published(
        &self,
        root: &str,
        manifest: Arc<local_ci::source::SourceManifest>,
    ) -> Result<()> {
        self.register_held_marked(root, "received", manifest, true)
    }
    fn register_held_marked(
        &self,
        root: &str,
        kind: &str,
        manifest: Arc<local_ci::source::SourceManifest>,
        mark: bool,
    ) -> Result<()> {
        if self.complete(root, &manifest.source_id)? {
            if mark {
                let mut c = self.connect()?;
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                Self::mark_published(&tx, root, manifest.repository_id.as_deref())?;
                tx.commit()?;
            }
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
        let options = Step {
            mark,
            ..Step::pages(32)
        };
        while !self.register_step(root, kind, &id, options, &mut cache, u64::MAX)? {
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
        while !self.register_step(root, kind, id, Step::pages(32), &mut cache, u64::MAX)? {}
        Ok(())
    }
    /// The time and the `clone_ref` rows of a published `received` root
    /// (spec 25.8 5b (c), (e)): one row for each of its graph rows, in the
    /// clone of its manifest's `repository_id`, where the receipt put the
    /// refs of all of them.
    fn mark_published(c: &Connection, root: &str, repository: Option<&str>) -> Result<()> {
        c.execute(
            "UPDATE root SET used_ms=MAX(COALESCE(used_ms,0),?2) WHERE id=?1",
            params![root, crate::platform::clock::wall_ms() as i64],
        )?;
        if let Some(repository) = repository {
            c.execute(
                "INSERT OR IGNORE INTO clone_ref(clone,source,root) SELECT ?2,source,root FROM graph WHERE root=?1",
                params![root, crate::workspace::plan::clone_dir_name(repository)],
            )?;
        }
        Ok(())
    }
    fn register_step(
        &self,
        root: &str,
        kind: &str,
        id: &str,
        options: Step,
        cache: &mut Option<Validated>,
        manifest_limit: u64,
    ) -> Result<bool> {
        let mut loads = options.loads;
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
        for _ in 0..options.pages {
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
                if !options.mark {
                    c.execute(
                        "UPDATE root SET source=?2,kind=?3 WHERE id=?1",
                        params![root, id, kind],
                    )?;
                    *cache = None;
                    return Ok(true);
                }
                // A published root records when it was last used and, in the
                // same transaction as its completion, which refs its source
                // and the sources of its graph have in the clone of its
                // repository (spec 25.8 5b (c), (e)).
                let repository = match cache.as_ref() {
                    Some(cached) if cached.manifest.source_id == id => {
                        cached.manifest.repository_id.clone()
                    }
                    _ => {
                        if loads == 0 {
                            return Ok(false);
                        }
                        // (the last read of this call)
                        self.load_validated(id, manifest_limit)?
                            .manifest
                            .repository_id
                            .clone()
                    }
                };
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute(
                    "UPDATE root SET source=?2,kind=?3 WHERE id=?1",
                    params![root, id, kind],
                )?;
                Self::mark_published(&tx, root, repository.as_deref())?;
                tx.commit()?;
                *cache = None;
                return Ok(true);
            };
            // One validated manifest is retained between pages. Every page
            // checks FD identity, not the full JSON/digest again.
            if cache
                .as_ref()
                .is_none_or(|cached| cached.manifest.source_id != next)
            {
                if loads == 0 {
                    return Ok(false);
                }
                loads -= 1;
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
            self.record_stat(&tx, &key, &path)?;
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
                    self.record_stat(&tx, &key, &path)?;
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
    /// The shared mutation lock for a reader that serves an accepted attempt
    /// or a receipt (spec 25.8 5b (a), (h)2): it waits for a deleting pass,
    /// which holds the exclusive lock for one step only, retrying from 10 ms
    /// doubling up to 50 ms, and gives up after 120 s with the error the
    /// nonblocking lock gives. Acceptance never calls this (it answers
    /// `OVERLOADED` without waiting); call it on a blocking thread.
    /// Tests only: the legacy registration is done, or `id` has its
    /// `receive:` row, or its root is `abandoned`.
    #[cfg(test)]
    pub(crate) fn set_for_test(&self, sql: &str, id: &str) {
        self.retention
            .connect()
            .unwrap()
            .execute(sql, [id])
            .unwrap();
    }
    /// The exclusive lock, as a deleting pass's step holds it (tests only).
    #[cfg(test)]
    pub(crate) fn exclusive_step_for_test(&self) -> Guard {
        self.retention.guard(true).unwrap()
    }
    pub fn mutation_guard_waiting(&self) -> Result<Guard> {
        self.retention
            .guard_waiting(std::time::Duration::from_secs(120))
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

/// How far one call of `register_step` goes: `pages` pages of 32 entries
/// and `loads` manifest reads; `mark` says the root is a published
/// `received` root (time and `clone_ref` rows at its completion).
#[derive(Clone, Copy, Debug)]
struct Step {
    pages: u32,
    loads: u32,
    mark: bool,
}
impl Step {
    fn pages(pages: u32) -> Self {
        Self {
            pages,
            loads: u32::MAX,
            mark: false,
        }
    }
}
/// A manifest that can never be registered as it is: not there, not a
/// regular file, not JSON, not its source's, not sealed.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct InvalidManifest(String);
/// Whether `e` says the manifest itself is invalid (or over the size
/// limit), not that it could not be read now.
fn manifest_invalid(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|c| c.is::<InvalidManifest>() || c.is::<ManifestLimit>())
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
                    Step::pages(1),
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
mod received_tests;
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
                recounted: Default::default(),
                reads: Default::default(),
                verifications: Default::default(),
                page_pause: Default::default(),
                foreground_waits: Default::default(),
                lock_syncs: Default::default(),
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
                recounted: Default::default(),
                reads: Default::default(),
                verifications: Default::default(),
                page_pause: Default::default(),
                foreground_waits: Default::default(),
                lock_syncs: Default::default(),
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
        assert_eq!(
            store
                .retention
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
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
        assert_eq!(
            store
                .retention
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
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
        assert_eq!(
            store
                .retention
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
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
        f.store
            .retention
            .reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
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
        f.store
            .retention
            .reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
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
                    Step::pages(1),
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
        let hash = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(bytes));
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
                "INSERT INTO root(id,kind,source) VALUES(?1,'pending','src-00000000000000000000000000000000')",
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
        c.execute("INSERT INTO root(id,kind,source) VALUES('published','received','src-00000000000000000000000000000000')",[]).unwrap();
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
        assert!({
            let bytes: &[u8] = b"original";
            let hash = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(bytes));
            let want = super::super::diff::BlobWant {
                hash: hash.clone(),
                size: bytes.len() as u64,
            };
            f.store
                .ingest_blob(&want, &mut &bytes[..])
                .map(|()| (hash, want.size))
        }
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
        let (hash, _) = {
            let bytes: &[u8] = b"held-reader";
            let hash = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(bytes));
            let want = super::super::diff::BlobWant {
                hash: hash.clone(),
                size: bytes.len() as u64,
            };
            f.store
                .ingest_blob(&want, &mut &bytes[..])
                .map(|()| (hash, want.size))
        }
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
    fn an_attempts_sdk_view_waits_per_call_and_holds_no_lock_between_calls() {
        let f = Fixture::new();
        let view = f.store.object_store_per_call();
        // a call waits for a step that is under way
        let exclusive = f.store.retention.guard(true).unwrap();
        let holder = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            drop(exclusive);
        });
        let started = std::time::Instant::now();
        view.with_sdk(|_| {
            // and the task's reads hold no lock: a step can run among them
            assert!(f.store.retention.guard(true).is_ok());
            Ok(())
        })
        .unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(250));
        holder.join().unwrap();
        // the next call after another step goes on
        drop(f.store.retention.guard(true).unwrap());
        view.with_sdk(|_| Ok(())).unwrap();
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
        let (hash, _) = {
            let bytes: &[u8] = b"sdk-fence";
            let hash = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(bytes));
            let want = super::super::diff::BlobWant {
                hash: hash.clone(),
                size: bytes.len() as u64,
            };
            f.store
                .ingest_blob(&want, &mut &bytes[..])
                .map(|()| (hash, want.size))
        }
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
        f.store
            .retention
            .reads
            .store(0, std::sync::atomic::Ordering::SeqCst);
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
    /// A received source `S` with its one blob, from the fixture's capture.
    fn received_fixture(
        f: &Fixture,
    ) -> (
        SourceStore,
        local_ci::source::SourceManifest,
        super::super::diff::BlobWant,
        Vec<u8>,
    ) {
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
        let receiver = SourceStore::open(&f._root.path().join("receiver")).unwrap();
        (receiver, m, want, bytes)
    }
    fn receive_root(store: &SourceStore, id: &str) -> Option<Option<String>> {
        store
            .retention
            .connect()
            .unwrap()
            .query_row(
                "SELECT source FROM root WHERE id='receive:'||?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
    }
    /// The sum of the sizes of the object files the index has a row for.
    fn files_on_disk(store: &SourceStore) -> u64 {
        let c = store.retention.connect().unwrap();
        let mut s = c.prepare("SELECT path FROM object").unwrap();
        let paths: Vec<String> = s
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        paths
            .iter()
            .filter_map(|p| std::fs::symlink_metadata(store.root().join(p)).ok())
            .map(|m| m.len())
            .sum()
    }
    #[test]
    fn store_bytes_counts_each_placed_file_once_and_drops_it_with_the_file() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        // (g)13: the import counts the blob once it is placed, registration
        // counts the manifest from a stat, and a second publication of the
        // same source adds nothing (only the change from empty counts)
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        assert_eq!(receiver.store_bytes().unwrap().unwrap(), want.size);
        receiver.publish_received(&m, None).unwrap();
        receiver.publish_received(&m, None).unwrap();
        let total = receiver.store_bytes().unwrap().unwrap();
        assert!(total > want.size);
        assert_eq!(total, files_on_disk(&receiver));
        // a failed receipt's own blob goes with its token's release, and
        // its bytes with it (the gate is open once registration is done)
        let other = SourceStore::open(&f._root.path().join("other")).unwrap();
        other
            .retention
            .connect()
            .unwrap()
            .execute(
                "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                [],
            )
            .unwrap();
        let token = other.begin_import().unwrap();
        other
            .ingest_blob_with_token(&token, &want, &mut &bytes[..])
            .unwrap();
        assert_eq!(other.store_bytes().unwrap().unwrap(), want.size);
        other
            .release_import(SourceStore::import_token_id(&token))
            .unwrap();
        assert!(!other
            .root()
            .join(format!("objects/{}/{}", &want.hash[..2], want.hash))
            .exists());
        assert_eq!(other.store_bytes().unwrap().unwrap(), 0);
        assert_eq!(files_on_disk(&other), 0);
    }
    #[test]
    fn the_bytes_fill_counts_rows_made_before_it_and_skips_missing_files() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        let expected = receiver.store_bytes().unwrap().unwrap();
        // as the migration leaves an index made before 5b, plus a row whose
        // file was never placed
        let c = receiver.retention.connect().unwrap();
        c.execute_batch(
            "DELETE FROM object_bytes; UPDATE meta SET value='0' WHERE key='store_bytes';
             UPDATE meta SET value='' WHERE key='bytes_fill';
             INSERT INTO object(id,path) VALUES('b:0000','objects/00/0000');",
        )
        .unwrap();
        assert!(!receiver.retention.fill_bytes(1).unwrap());
        assert!(receiver.retention.fill_bytes(1000).unwrap());
        assert_eq!(receiver.store_bytes().unwrap().unwrap(), expected);
        let missing: Option<i64> = c
            .query_row(
                "SELECT bytes FROM object_bytes WHERE id='b:0000'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(missing, None);
        // done: a later page reads nothing
        assert!(receiver.retention.fill_bytes(1000).unwrap());
        assert_eq!(receiver.store_bytes().unwrap().unwrap(), expected);
    }
    /// Spec 25.8 5b (b): a publication whose blob is not in the store is
    /// refused as gone (`INPUT_MISSING`, the blobs named), not as `corrupt`;
    /// once the blobs are there it publishes.
    #[test]
    fn a_publication_whose_blob_is_gone_names_it() {
        let f = Fixture::new();
        let (receiver, m, want, _bytes) = received_fixture(&f);
        let e = receiver.publish_received(&m, None).unwrap_err();
        let gone = crate::source::store::input_gone(&e).unwrap_or_else(|| panic!("{e:#}"));
        assert_eq!(gone.source, m.source_id);
        assert!(gone.missing.contains(&want.hash), "{gone:?}");
        for entry in &m.object_manifest.entries {
            let w = super::super::diff::BlobWant {
                hash: entry.hash.clone(),
                size: entry.size,
            };
            let bytes = f.store.read_blob(&entry.hash).unwrap();
            receiver.ingest_blob(&w, &mut &bytes[..]).unwrap();
        }
        receiver.publish_received(&m, None).unwrap();
    }

    #[test]
    fn a_used_time_never_goes_back_with_the_wall_clock() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        let far = i64::MAX / 4;
        let c = receiver.retention.connect().unwrap();
        c.execute(
            "UPDATE root SET used_ms=?2 WHERE id='receive:'||?1",
            params![&m.source_id, far],
        )
        .unwrap();
        let used = || -> i64 {
            c.query_row(
                "SELECT used_ms FROM root WHERE id='receive:'||?1",
                [&m.source_id],
                |r| r.get(0),
            )
            .unwrap()
        };
        receiver.retention.touch_received(&m.source_id, 5).unwrap();
        assert_eq!(used(), far);
        receiver.publish_received(&m, None).unwrap();
        assert_eq!(used(), far);
        // forward still moves it
        receiver
            .retention
            .touch_received(&m.source_id, far as u64 + 7)
            .unwrap();
        assert_eq!(used(), far + 7);
    }
    #[test]
    fn a_mutation_lock_that_cannot_be_taken_is_reported_but_a_busy_one_is_not() {
        let f = Fixture::new();
        let state = f.store.retention.path.parent().unwrap().to_path_buf();
        let lock = state.join("sources").join("mutation.lock");
        assert_eq!(mutation_lock_problem(&state), None);
        // busy (an exclusive step holds it) is not a problem
        let held = f.store.retention.guard(true).unwrap();
        assert_eq!(mutation_lock_problem(&state), None);
        drop(held);
        std::fs::set_permissions(&lock, std::os::unix::fs::PermissionsExt::from_mode(0o666))
            .unwrap();
        assert!(mutation_lock_problem(&state).unwrap().contains("access"));
        assert!(f.store.retention.guard(false).is_err());
        std::fs::set_permissions(&lock, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        assert_eq!(mutation_lock_problem(&state), None);
        std::fs::remove_file(&lock).unwrap();
        assert_eq!(mutation_lock_problem(&state), None);
    }
    #[test]
    fn only_the_take_that_creates_the_lock_file_syncs_it() {
        use std::sync::atomic::Ordering::SeqCst;
        let f = Fixture::new();
        let index = &f.store.retention;
        let lock = index.path.join("mutation.lock");
        // an earlier take created it and synced it
        drop(index.guard(false).unwrap());
        let before = index.lock_syncs.load(SeqCst);
        let a = index.guard(false).unwrap();
        let b = index.guard(false).unwrap();
        drop((a, b));
        drop(index.guard(true).unwrap());
        assert_eq!(index.lock_syncs.load(SeqCst), before);
        // a take that has to create it syncs once
        std::fs::remove_file(&lock).unwrap();
        drop(index.guard(false).unwrap());
        assert_eq!(index.lock_syncs.load(SeqCst), before + 1);
    }
    #[test]
    fn publication_records_used_time_and_the_clone_refs_of_its_graph() {
        let f = Fixture::new();
        let (receiver, mut m, want, bytes) = received_fixture(&f);
        m.repository_id = Some("github:example/repo".into());
        let m = local_ci::source::seal_manifest(m);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        let before = crate::platform::clock::wall_ms() as i64;
        receiver.publish_received(&m, None).unwrap();
        let c = receiver.retention.connect().unwrap();
        let used: Option<i64> = c
            .query_row(
                "SELECT used_ms FROM root WHERE id='receive:'||?1",
                [&m.source_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(used.unwrap() >= before);
        let refs = |c: &Connection| -> Vec<(String, String)> {
            let mut s = c
                .prepare(
                    "SELECT clone,source FROM clone_ref WHERE root='receive:'||?1 ORDER BY source",
                )
                .unwrap();
            s.query_map([&m.source_id], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        let expected: Vec<(String, String)> = m
            .repository_id
            .iter()
            .map(|r| {
                (
                    crate::workspace::plan::clone_dir_name(r),
                    m.source_id.clone(),
                )
            })
            .collect();
        assert!(!expected.is_empty(), "the fixture names its repository");
        assert_eq!(refs(&c), expected);
        // the migration fills the same rows for a root made before 5b
        c.execute_batch(
            "DELETE FROM clone_ref; UPDATE root SET used_ms=NULL; UPDATE meta SET value='' WHERE key='clone_ref_fill';",
        )
        .unwrap();
        assert!(receiver.retention.fill_clone_refs(32, 64 << 20).unwrap());
        assert_eq!(refs(&c), expected);
    }
    #[test]
    fn legacy_manifests_are_registered_from_the_queue_and_unregistrable_ones_abandoned() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        let id = m.source_id.clone();
        // (h)5: a manifest received before 5a (a file and no `m:` row) and
        // one whose blob is not readable as a manifest
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        let path = receiver.object_store().unwrap().manifest_path(&id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        let broken = "src-00000000000000000000000000000001";
        std::fs::write(path.with_file_name(format!("{broken}.json")), b"{").unwrap();
        while !receiver.retention.register_legacy().unwrap() {}
        let c = receiver.retention.connect().unwrap();
        let root = |id: &str| -> Option<(String, Option<String>)> {
            c.query_row(
                "SELECT kind,source FROM root WHERE id='receive:'||?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .unwrap()
        };
        assert_eq!(root(&id), Some(("received".into(), Some(id.clone()))));
        assert!(receiver.retention.has_object(&format!("m:{id}")).unwrap());
        assert_eq!(root(broken), Some(("abandoned".into(), None)));
        let queued: i64 = c
            .query_row("SELECT count(*) FROM legacy_queue", [], |r| r.get(0))
            .unwrap();
        assert_eq!(queued, 0);
        // the marker that opens deletion is not turned here
        assert!(receiver.retention.legacy_registration_pending().unwrap());
        // a store with a parent's root is left as it is (g)35
        let parent = SourceStore::open(&f._root.path().join("parent")).unwrap();
        let pc = parent.retention.connect().unwrap();
        pc.execute(
            "INSERT INTO root(id,kind,source) VALUES('run:x','run','s')",
            [],
        )
        .unwrap();
        assert!(parent.retention.register_legacy().unwrap());
        let listed: bool = pc
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM meta WHERE key='legacy_queue')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!listed);
    }
    /// A parent source with one submodule source, the parent's id sorting
    /// before the child's, both naming a repository.
    fn family(
        f: &Fixture,
    ) -> (
        SourceStore,
        local_ci::source::SourceManifest,
        local_ci::source::SourceManifest,
        super::super::diff::BlobWant,
        Vec<u8>,
    ) {
        let (receiver, m, want, bytes) = received_fixture(f);
        for n in 0.. {
            let mut child = m.clone();
            child.branch = Some(format!("child{n}"));
            child.repository_id = Some("github:example/child".into());
            let child = local_ci::source::seal_manifest(child);
            let mut parent = m.clone();
            parent.repository_id = Some("github:example/parent".into());
            parent.submodules = vec![local_ci::source::SubmoduleRef {
                path: local_ci::source::RelPath(b"sub".to_vec()),
                revision: "0".repeat(40),
                source_id: Some(child.source_id.clone()),
                dirty: true,
            }];
            let parent = local_ci::source::seal_manifest(parent);
            if parent.source_id < child.source_id {
                return (receiver, parent, child, want, bytes);
            }
        }
        unreachable!()
    }
    fn write_manifest(store: &SourceStore, m: &local_ci::source::SourceManifest) {
        let path = store.object_store().unwrap().manifest_path(&m.source_id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(m).unwrap()).unwrap();
    }
    fn set_mode(path: &std::path::Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    #[test]
    fn the_object_table_keeps_the_columns_an_older_binary_inserts() {
        let f = Fixture::new();
        let path = f._root.path().join("receiver");
        let store = SourceStore::open(&path).unwrap();
        let c = store.retention.connect().unwrap();
        // the insert of the code before 5b, positional with two values
        c.execute("INSERT INTO object VALUES('b:old','objects/ol/old')", [])
            .unwrap();
        // an index an earlier build of 5b made: the column goes, its counted
        // size moves to `object_bytes`, and what is missing is made again
        c.execute_batch(
            "ALTER TABLE object ADD COLUMN bytes INTEGER; UPDATE object SET bytes=7 WHERE id='b:old';
             DELETE FROM object_bytes; DROP TABLE clone_ref; DELETE FROM meta WHERE key='clone_ref_fill';",
        )
        .unwrap();
        drop(c);
        drop(store);
        let store = SourceStore::open(&path).unwrap();
        let c = store.retention.connect().unwrap();
        let column: bool = c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('object') WHERE name='bytes')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!column);
        let moved: i64 = c
            .query_row("SELECT bytes FROM object_bytes WHERE id='b:old'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(moved, 7);
        c.execute("INSERT INTO clone_ref VALUES('c','s','r')", [])
            .unwrap();
        c.execute("INSERT INTO object VALUES('b:new','objects/ne/new')", [])
            .unwrap();
    }
    #[test]
    fn the_recount_drops_sizes_an_older_binary_left_and_repairs_an_invalid_total() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        let total = receiver.store_bytes().unwrap().unwrap();
        // the first recount of the process runs once the fill is done
        while !receiver.retention.fill_bytes(1000).unwrap() {}
        while !(receiver.retention.fill_bytes(1000).unwrap()
            & receiver.retention.fill_clone_refs(32, 64 << 20).unwrap()
            & receiver.retention.recount_bytes(1).unwrap())
        {}
        assert_eq!(receiver.store_bytes().unwrap(), Some(total));
        // an older binary deletes the blob's object row and not its size
        let c = receiver.retention.connect().unwrap();
        c.execute("DELETE FROM object WHERE id='b:'||?1", [&want.hash])
            .unwrap();
        // a total that is not a number is not read as 0 and not added to
        c.execute("UPDATE meta SET value='x' WHERE key='store_bytes'", [])
            .unwrap();
        assert_eq!(receiver.store_bytes().unwrap(), None);
        c.execute(
            "INSERT INTO object VALUES('b:extra','objects/ex/extra')",
            [],
        )
        .unwrap();
        receiver.retention.placed("b:extra", 5).unwrap();
        assert_eq!(receiver.store_bytes().unwrap(), None);
        // (an invalid total starts a recount at once)
        while !(receiver.retention.fill_bytes(1000).unwrap()
            & receiver.retention.fill_clone_refs(32, 64 << 20).unwrap()
            & receiver.retention.recount_bytes(1).unwrap())
        {}
        assert_eq!(receiver.store_bytes().unwrap(), Some(total - want.size + 5));
        let left: bool = c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM object_bytes WHERE id='b:'||?1)",
                [&want.hash],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!left);
        // within the hour no new recount starts
        c.execute("UPDATE meta SET value='0' WHERE key='store_bytes'", [])
            .unwrap();
        assert!(receiver.retention.recount_bytes(1).unwrap());
        assert_eq!(receiver.store_bytes().unwrap(), Some(0));
    }
    #[test]
    fn legacy_registration_retries_transient_errors_and_goes_a_bounded_step_at_a_time() {
        let f = Fixture::new();
        let (receiver, parent, child, want, bytes) = family(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        write_manifest(&receiver, &parent);
        write_manifest(&receiver, &child);
        // a parent whose child's manifest is missing
        let mut orphan = parent.clone();
        orphan.branch = Some("orphan".into());
        orphan.submodules[0].source_id = Some("src-ffffffffffffffffffffffffffffffff".into());
        let orphan = local_ci::source::seal_manifest(orphan);
        write_manifest(&receiver, &orphan);
        let broken = "src-00000000000000000000000000000001";
        let dir = receiver.root().join("manifests");
        std::fs::write(dir.join(format!("{broken}.json")), b"{").unwrap();
        let c = receiver.retention.connect().unwrap();
        let queued = || -> i64 {
            c.query_row("SELECT count(*) FROM legacy_queue", [], |r| r.get(0))
                .unwrap()
        };
        // the broken one sorts first and is abandoned
        assert!(!receiver.retention.register_legacy().unwrap());
        assert_eq!(queued(), 3);
        // a manifest directory that cannot be read now abandons nothing
        set_mode(&dir, 0o000);
        let refused = receiver.retention.register_legacy();
        set_mode(&dir, 0o755);
        assert!(refused.is_err());
        assert_eq!(queued(), 3);
        // (M-c) the failed one waits in `legacy_retry` and does not block
        // the next one
        let (waiting, attempts): (String, i64) = c
            .query_row("SELECT name,attempts FROM legacy_retry", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(attempts, 1);
        c.execute("UPDATE legacy_retry SET attempts=8", []).unwrap();
        assert!(receiver.retention.register_legacy().is_ok());
        let attempts: i64 = c
            .query_row(
                "SELECT attempts FROM legacy_retry WHERE name=?1",
                [&waiting],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            attempts, 8,
            "a waiting manifest is not tried before it is due"
        );
        c.execute("UPDATE legacy_retry SET next_ms=0", []).unwrap();
        // one manifest read per call: some calls end without a row leaving
        let mut partial = false;
        for _ in 0..100 {
            let before = queued();
            let done = receiver.retention.register_legacy().unwrap();
            partial |= queued() == before;
            if done {
                break;
            }
        }
        assert_eq!(queued(), 0);
        let retried: i64 = c
            .query_row("SELECT count(*) FROM legacy_retry", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retried, 0);
        assert!(partial, "a step stopped at its budget");
        let root = |id: &str| -> Option<(String, Option<String>)> {
            c.query_row(
                "SELECT kind,source FROM root WHERE id='receive:'||?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .unwrap()
        };
        for m in [&parent, &child] {
            assert_eq!(
                root(&m.source_id),
                Some(("received".into(), Some(m.source_id.clone())))
            );
        }
        assert_eq!(root(&orphan.source_id), Some(("abandoned".into(), None)));
        // the abandoned root keeps its manifest member for its release
        let held: bool = c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM member WHERE root='receive:'||?1 AND object='m:'||?1)",
                [&orphan.source_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(held);
    }
    #[test]
    fn the_clone_ref_fill_keeps_its_budget_and_its_place_on_a_transient_error() {
        let f = Fixture::new();
        let (receiver, parent, child, want, bytes) = family(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver
            .publish_received_all(&[child.clone(), parent.clone()], None)
            .unwrap();
        let c = receiver.retention.connect().unwrap();
        c.execute_batch(
            "DELETE FROM clone_ref; UPDATE root SET used_ms=NULL; UPDATE meta SET value='' WHERE key='clone_ref_fill';",
        )
        .unwrap();
        let cursor = || -> String {
            c.query_row(
                "SELECT value FROM meta WHERE key='clone_ref_fill'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        let dir = receiver.root().join("manifests");
        set_mode(&dir, 0o000);
        let refused = receiver.retention.fill_clone_refs(32, 64 << 20);
        set_mode(&dir, 0o755);
        // (M-c) the page goes on past roots it cannot read now; they wait
        // in `clone_ref_retry` and the fill is not done while they do
        assert!(!refused.unwrap());
        assert_eq!(cursor(), "done");
        let retried: i64 = c
            .query_row("SELECT count(*) FROM clone_ref_retry", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retried, 2);
        assert!(!receiver.retention.fill_clone_refs(32, 64 << 20).unwrap());
        c.execute("UPDATE clone_ref_retry SET attempts=8,next_ms=0", [])
            .unwrap();
        let state = f._root.path().join("state");
        std::fs::create_dir_all(state.join("sources")).unwrap();
        std::fs::copy(
            receiver.root().join("retention.db"),
            state.join("sources").join("retention.db"),
        )
        .unwrap();
        assert_eq!(inspect_retry_failing(&state).unwrap(), (0, 2, 0));
        // a budget of one byte reads one manifest a page
        assert!(!receiver.retention.fill_clone_refs(32, 1).unwrap());
        assert!(receiver.retention.fill_clone_refs(32, 1).unwrap());
        let unused: i64 = c
            .query_row(
                "SELECT count(*) FROM root WHERE kind='received' AND used_ms IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(unused, 0);
        let refs: i64 = c
            .query_row(
                "SELECT count(*) FROM clone_ref WHERE root='receive:'||?1",
                [&parent.source_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(refs, 2);
    }
    #[test]
    fn only_the_requested_root_is_marked_and_a_republication_marks_it_again() {
        let f = Fixture::new();
        let (receiver, parent, child, want, bytes) = family(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver
            .publish_received_all(&[child.clone(), parent.clone()], None)
            .unwrap();
        let c = receiver.retention.connect().unwrap();
        let used = |id: &str| -> Option<i64> {
            c.query_row(
                "SELECT used_ms FROM root WHERE id='receive:'||?1",
                [id],
                |r| r.get(0),
            )
            .unwrap()
        };
        let refs = |root: &str| -> Vec<(String, String)> {
            let mut s = c
                .prepare(
                    "SELECT clone,source FROM clone_ref WHERE root='receive:'||?1 ORDER BY source",
                )
                .unwrap();
            s.query_map([root], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        // (c) and (e): the child's root reached as a member has neither
        assert_eq!(used(&child.source_id), None);
        assert!(refs(&child.source_id).is_empty());
        assert!(used(&parent.source_id).is_some());
        let clone = crate::workspace::plan::clone_dir_name("github:example/parent");
        assert_eq!(
            refs(&parent.source_id),
            vec![
                (clone.clone(), parent.source_id.clone()),
                (clone, child.source_id.clone())
            ]
        );
        // (g)12: publishing the complete source again records the time
        c.execute(
            "UPDATE root SET used_ms=1 WHERE id='receive:'||?1",
            [&parent.source_id],
        )
        .unwrap();
        receiver.publish_received(&parent, None).unwrap();
        assert!(used(&parent.source_id).unwrap() > 1);
    }
    #[test]
    fn a_recount_fills_again_what_an_older_binary_left_without_sizes_or_times() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        let step = |r: &SourceStore| {
            for _ in 0..100 {
                let a = r.retention.fill_bytes(1000).unwrap();
                let b = r.retention.fill_clone_refs(32, 64 << 20).unwrap();
                if r.retention.recount_bytes(1000).unwrap() && a && b {
                    return;
                }
            }
            panic!("the fills and the recount did not finish");
        };
        step(&receiver);
        let total = receiver.store_bytes().unwrap().unwrap();
        // after a rollback: an older binary's rows with no size, a root it
        // completed with no time and no refs (both fills are `done`)
        let c = receiver.retention.connect().unwrap();
        c.execute_batch(
            "DELETE FROM object_bytes; UPDATE root SET used_ms=NULL; DELETE FROM clone_ref;",
        )
        .unwrap();
        // the next recount (here: the total is invalid) runs the fills again
        c.execute("UPDATE meta SET value='x' WHERE key='store_bytes'", [])
            .unwrap();
        step(&receiver);
        assert_eq!(receiver.store_bytes().unwrap(), Some(total));
        let unused: bool = c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM root WHERE kind='received' AND used_ms IS NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!unused);
    }
    #[test]
    fn a_stat_that_fails_for_another_reason_is_retried_and_never_blocks_the_bytes_fill() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        let c = receiver.retention.connect().unwrap();
        c.execute_batch(
            "DELETE FROM object_bytes; UPDATE meta SET value='' WHERE key='bytes_fill';",
        )
        .unwrap();
        let cursor = || -> String {
            c.query_row("SELECT value FROM meta WHERE key='bytes_fill'", [], |r| {
                r.get(0)
            })
            .unwrap()
        };
        let retried = || -> i64 {
            c.query_row("SELECT count(*) FROM bytes_retry", [], |r| r.get(0))
                .unwrap()
        };
        let dir = receiver.root().join("objects");
        set_mode(&dir, 0o000);
        let refused = receiver.retention.fill_bytes(1000);
        // the cursor passed the object (no head-of-line block); the fill is
        // not done while a retry row is left
        let first = refused.unwrap();
        assert!(!first);
        assert_eq!(cursor(), "done");
        assert!(retried() >= 1);
        // not due yet: the fill stays open
        let again = receiver.retention.fill_bytes(1000).unwrap();
        assert!(!again);
        c.execute("UPDATE bytes_retry SET attempts=8,next_ms=0", [])
            .unwrap();
        let state = f._root.path().join("state");
        std::fs::create_dir_all(state.join("sources")).unwrap();
        std::fs::copy(
            receiver.root().join("retention.db"),
            state.join("sources").join("retention.db"),
        )
        .unwrap();
        let failing = inspect_retry_failing(&state).unwrap();
        // still unreadable when due: the row stays
        let due = receiver.retention.fill_bytes(1000).unwrap();
        set_mode(&dir, 0o755);
        assert!(!due);
        assert!(failing.2 >= 1);
        c.execute("UPDATE bytes_retry SET next_ms=0", []).unwrap();
        assert!(receiver.retention.fill_bytes(1000).unwrap());
        let counted: bool = c
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM object_bytes WHERE id='b:'||?1)",
                [&want.hash],
                |r| r.get(0),
            )
            .unwrap();
        assert!(counted);
        assert_eq!(retried(), 0);
        // a retry row of an object that is gone goes too
        c.execute("INSERT INTO bytes_retry VALUES('b:gone',1,0)", [])
            .unwrap();
        assert!(receiver.retention.fill_bytes(1000).unwrap());
        assert_eq!(retried(), 0);
    }
    #[test]
    fn a_recount_reads_no_manifest_of_a_root_that_has_its_time() {
        let f = Fixture::new();
        let (receiver, parent, child, want, bytes) = family(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        write_manifest(&receiver, &parent);
        write_manifest(&receiver, &child);
        receiver
            .publish_received_all(&[child.clone(), parent.clone()], None)
            .unwrap();
        let c = receiver.retention.connect().unwrap();
        let restart = || {
            c.execute("UPDATE meta SET value='' WHERE key='clone_ref_fill'", [])
                .unwrap();
            receiver
                .retention
                .reads
                .store(0, std::sync::atomic::Ordering::SeqCst);
        };
        // the first pass marks the roots a publication does not (a carried one)
        restart();
        assert!(receiver.retention.fill_clone_refs(32, 64 << 20).unwrap());
        // every root is marked: the recount's pass reads no manifest
        restart();
        assert!(receiver.retention.fill_clone_refs(32, 64 << 20).unwrap());
        assert_eq!(
            receiver
                .retention
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        // a root an older binary completed has no time and no refs: only
        // its manifest is read, and it is filled
        c.execute("DELETE FROM clone_ref", []).unwrap();
        c.execute(
            "UPDATE root SET used_ms=NULL WHERE id='receive:'||?1",
            [&parent.source_id],
        )
        .unwrap();
        restart();
        assert!(receiver.retention.fill_clone_refs(32, 64 << 20).unwrap());
        assert_eq!(
            receiver
                .retention
                .reads
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let unused: i64 = c
            .query_row(
                "SELECT count(*) FROM root WHERE kind='received' AND used_ms IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(unused, 0);
        let refs: i64 = c
            .query_row(
                "SELECT count(*) FROM clone_ref WHERE root='receive:'||?1",
                [&parent.source_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(refs >= 1);
    }
    #[test]
    fn a_manifest_the_legacy_registration_is_working_on_counts_as_received() {
        let f = Fixture::new();
        let (receiver, parent, child, want, bytes) = family(&f);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        write_manifest(&receiver, &parent);
        write_manifest(&receiver, &child);
        let mut seen_partial = false;
        for _ in 0..20 {
            let done = receiver.retention.register_legacy().unwrap();
            let guard = receiver.mutation_guard().unwrap();
            for m in [&parent, &child] {
                assert!(
                    receiver.received(&guard, &m.source_id).unwrap(),
                    "{} across a tick",
                    m.source_id
                );
            }
            seen_partial |= receiver
                .retention
                .has_object(&format!("m:{}", child.source_id))
                .unwrap()
                && !receiver.retention.received_row(&child.source_id).unwrap();
            if done {
                break;
            }
        }
        assert!(seen_partial, "a tick ended with the child's m: row only");
    }
    #[test]
    fn refused_publication_leaves_no_empty_receive_root() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        // (g)25: the blob is not here, the publication is refused, and no
        // `receive:<S>` row with an empty source stays behind
        assert!(receiver.publish_received(&m, None).is_err());
        assert_eq!(receive_root(&receiver, &m.source_id), None);
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        assert_eq!(
            receive_root(&receiver, &m.source_id),
            Some(Some(m.source_id.clone()))
        );
    }
    #[test]
    fn a_source_is_in_the_store_by_its_receive_row_not_by_its_file() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        let id = m.source_id.clone();
        let present = |store: &SourceStore| {
            let held = store.mutation_guard().unwrap();
            store.received(&held, &id).unwrap()
        };
        assert!(!present(&receiver));
        // a manifest file with no `m:` row counts while the legacy
        // registration is pending...
        let path = receiver.object_store().unwrap().manifest_path(&id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(present(&receiver));
        // ...but not once it is done
        let c = receiver.retention.connect().unwrap();
        c.execute(
            "UPDATE meta SET value='done' WHERE key='legacy_registration'",
            [],
        )
        .unwrap();
        assert!(!present(&receiver));
        c.execute(
            "UPDATE meta SET value='pending' WHERE key='legacy_registration'",
            [],
        )
        .unwrap();
        // and not when an `m:` row says it belongs to a publication that has
        // not finished (the file is written before the root's source)
        c.execute(
            "INSERT INTO object(id,path) VALUES('m:'||?1,'manifests/x')",
            [&id],
        )
        .unwrap();
        assert!(!present(&receiver));
        c.execute("DELETE FROM object WHERE id='m:'||?1", [&id])
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver.publish_received(&m, None).unwrap();
        c.execute(
            "UPDATE meta SET value='done' WHERE key='legacy_registration'",
            [],
        )
        .unwrap();
        assert!(present(&receiver));
    }
    #[test]
    fn a_waiting_reader_outlasts_one_exclusive_step_and_gives_up_at_its_limit() {
        let f = Fixture::new();
        let exclusive = f.store.retention.guard(true).unwrap();
        // the nonblocking lock fails at once; a short limit gives up with it
        assert!(f.store.mutation_guard().is_err());
        let started = std::time::Instant::now();
        let e = f
            .store
            .retention
            .guard_waiting(std::time::Duration::from_millis(150))
            .err()
            .unwrap();
        assert!(
            e.chain().any(|c| c
                .downcast_ref::<io::Error>()
                .is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock)),
            "{e:#}"
        );
        assert!(started.elapsed() >= std::time::Duration::from_millis(100));
        // a waiting reader gets the lock once the step ends
        let (go, gone) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            gone.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(200));
            drop(exclusive);
        });
        go.send(()).unwrap();
        let _shared = f.store.mutation_guard_waiting().unwrap();
        holder.join().unwrap();
    }
    #[test]
    fn a_completed_deletion_claim_leaves_no_row() {
        let f = Fixture::new();
        let (_run, mut cursor) = deletion_ready(&f);
        let c = f.store.retention.connect().unwrap();
        let mut claimed = false;
        for _ in 0..20 {
            let page = deletion_tick(&f, &mut cursor);
            claimed |= page.removed > 0;
        }
        assert!(claimed, "the fixture deleted nothing");
        // (g)29: no `done=1` row is kept; `publication.incarnation` keeps
        // the generation
        assert_eq!(
            c.query_row("SELECT count(*) FROM source_delete_claim", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn the_shared_release_detaches_only_an_abandoned_roots_memberships() {
        let f = Fixture::new();
        let (receiver, _m, want, bytes) = received_fixture(&f);
        let a = receiver.begin_import().unwrap();
        let b = receiver.begin_import().unwrap();
        for t in [&a, &b] {
            receiver
                .ingest_blob_with_token(t, &want, &mut &bytes[..])
                .unwrap();
        }
        let members = |root: String| -> i64 {
            receiver
                .retention
                .connect()
                .unwrap()
                .query_row("SELECT count(*) FROM member WHERE root=?1", [root], |r| {
                    r.get(0)
                })
                .unwrap()
        };
        // a live token's shared membership stays (5a round-3 L4)
        let _held = receiver.mutation_guard().unwrap();
        receiver.retention.release_shared_root(&a.root()).unwrap();
        assert_eq!(members(a.root()), 1);
        drop(_held);
        receiver
            .release_import(SourceStore::import_token_id(&a))
            .unwrap();
        assert_eq!(members(a.root()), 0);
        assert_eq!(members(b.root()), 1);
    }
    #[test]
    fn start_settles_empty_receive_roots_by_what_their_file_proves() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        let id = m.source_id.clone();
        // a publication cut after its root and file, before registration
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        receiver
            .retention
            .pending(&format!("receive:{id}"))
            .unwrap();
        let path = receiver.object_store().unwrap().manifest_path(&id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        // and one cut before its file
        let lost = "receive:0000000000000000000000000000000000000000000000000000000000000000";
        receiver.retention.pending(lost).unwrap();
        let kind = |root: &str| -> Option<String> {
            receiver
                .retention
                .connect()
                .unwrap()
                .query_row("SELECT kind FROM root WHERE id=?1", [root], |r| r.get(0))
                .optional()
                .unwrap()
        };
        // busy: nothing is settled until the lock is had (shown with a
        // short wait; the start waits up to 120 s)
        {
            let _shared = receiver.mutation_guard().unwrap();
            assert!(receiver
                .retention
                .guard_exclusive_waiting(std::time::Duration::from_millis(50))
                .unwrap()
                .is_none());
        }
        assert_eq!(receiver.settle_empty_receive_roots().unwrap(), Some((1, 1)));
        assert_eq!(receive_root(&receiver, &id), Some(Some(id.clone())));
        assert_eq!(kind(lost).as_deref(), Some("abandoned"));
        // a store with a parent's root is left as it is
        let c = receiver.retention.connect().unwrap();
        c.execute("INSERT INTO root(id,kind) VALUES('run:x','run')", [])
            .unwrap();
        c.execute(
            "INSERT INTO root(id,kind) VALUES('receive:1111111111111111111111111111111111111111111111111111111111111111','pending')",
            [],
        )
        .unwrap();
        assert_eq!(receiver.settle_empty_receive_roots().unwrap(), None);
    }
    #[test]
    fn abandoned_receive_roots_are_not_settled_again_and_do_not_hide_a_new_one() {
        let f = Fixture::new();
        let (receiver, _m, _want, _bytes) = received_fixture(&f);
        let c = receiver.retention.connect().unwrap();
        for i in 0..1001u32 {
            c.execute(
                "INSERT INTO root(id,kind) VALUES(?1,'abandoned')",
                [format!("receive:{i:064x}")],
            )
            .unwrap();
        }
        // only abandoned roots: nothing to settle, the start does not wait
        let _shared = receiver.mutation_guard().unwrap();
        assert_eq!(receiver.settle_empty_receive_roots().unwrap(), None);
        drop(_shared);
        // a new cut publication after 1,001 abandoned ones is settled
        let fresh = format!("receive:{}", "f".repeat(64));
        receiver.retention.pending(&fresh).unwrap();
        assert_eq!(receiver.settle_empty_receive_roots().unwrap(), Some((0, 1)));
        let kind: String = c
            .query_row("SELECT kind FROM root WHERE id=?1", [&fresh], |r| r.get(0))
            .unwrap();
        assert_eq!(kind, "abandoned");
    }
    #[test]
    fn publishing_again_into_an_abandoned_root_makes_it_pending_for_settlement() {
        let f = Fixture::new();
        let (receiver, _m, _want, _bytes) = received_fixture(&f);
        let root = format!("receive:{}", "a".repeat(64));
        let c = receiver.retention.connect().unwrap();
        c.execute("INSERT INTO root(id,kind) VALUES(?1,'abandoned')", [&root])
            .unwrap();
        receiver.retention.pending(&root).unwrap();
        let kind = |r: &str| -> String {
            c.query_row("SELECT kind FROM root WHERE id=?1", [r], |x| x.get(0))
                .unwrap()
        };
        assert_eq!(kind(&root), "pending");
        // a received root, and one that has a source, are left as they are
        let other = format!("receive:{}", "b".repeat(64));
        c.execute(
            "INSERT INTO root(id,kind,source) VALUES(?1,'abandoned',?2)",
            [&other, &"b".repeat(64)],
        )
        .unwrap();
        receiver.retention.pending(&other).unwrap();
        assert_eq!(kind(&other), "abandoned");
        let done = format!("receive:{}", "c".repeat(64));
        c.execute(
            "INSERT INTO root(id,kind,source) VALUES(?1,'received',?2)",
            [&done, &"c".repeat(64)],
        )
        .unwrap();
        receiver.retention.pending(&done).unwrap();
        assert_eq!(kind(&done), "received");
    }
    #[test]
    fn a_root_whose_registration_fails_is_abandoned_and_the_rest_settled() {
        let f = Fixture::new();
        let (receiver, m, want, bytes) = received_fixture(&f);
        let id = m.source_id.clone();
        receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        let root = format!("receive:{id}");
        receiver.retention.pending(&root).unwrap();
        let path = receiver.object_store().unwrap().manifest_path(&id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();
        // the cut registration had found a child whose manifest is gone
        let c = receiver.retention.connect().unwrap();
        c.execute(
            "INSERT INTO graph(root,source) VALUES(?1,?2)",
            [&root, &"e".repeat(64)],
        )
        .unwrap();
        let lost = format!("receive:{}", "0".repeat(64));
        receiver.retention.pending(&lost).unwrap();
        assert_eq!(receiver.settle_empty_receive_roots().unwrap(), Some((0, 2)));
        for r in [&root, &lost] {
            let kind: String = c
                .query_row("SELECT kind FROM root WHERE id=?1", [r], |x| x.get(0))
                .unwrap();
            assert_eq!(kind, "abandoned");
        }
        let graph: i64 = c
            .query_row("SELECT count(*) FROM graph WHERE root=?1", [&root], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(graph, 0);
    }
    #[test]
    fn completed_claim_rows_of_an_older_version_are_deleted_in_pages() {
        let f = Fixture::new();
        let c = f.store.retention.connect().unwrap();
        for i in 0..5 {
            c.execute(
                "INSERT INTO source_delete_claim VALUES(?1,1,'{}',1)",
                [format!("b:{i:064x}")],
            )
            .unwrap();
        }
        c.execute(
            "INSERT INTO source_delete_claim VALUES('b:live',1,'{}',0)",
            [],
        )
        .unwrap();
        assert_eq!(f.store.retention.forget_completed_claims(2, 2).unwrap(), 4);
        assert_eq!(f.store.retention.forget_completed_claims(2, 2).unwrap(), 1);
        let left: Vec<String> = c
            .prepare("SELECT object FROM source_delete_claim")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(left, ["b:live"]);
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
