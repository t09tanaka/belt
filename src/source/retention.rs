//! Persistent roots and cross-process exclusion for captured inputs.
//! This first stage does not expire roots or delete source files.
use super::SourceStore;
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Guard {
    _file: Arc<File>,
    pub(crate) root: PathBuf,
}
#[derive(Debug, Clone)]
pub(crate) struct Index {
    path: PathBuf,
    database: PathBuf,
    root: Arc<File>,
}
const DIR: rustix::fs::OFlags = rustix::fs::OFlags::RDONLY
    .union(rustix::fs::OFlags::DIRECTORY)
    .union(rustix::fs::OFlags::NOFOLLOW)
    .union(rustix::fs::OFlags::CLOEXEC);
impl Index {
    pub(crate) fn open(root: &Path) -> Result<Self> {
        let this = Self {
            path: root.into(),
            database: std::fs::canonicalize(root)?.join("retention.db"),
            root: Arc::new(rustix::fs::open(root, DIR, rustix::fs::Mode::empty())?.into()),
        };
        let _guard = this.guard(false)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join("retention.db"))?;
        let m = file.metadata()?;
        ensure!(
            m.is_file()
                && m.nlink() == 1
                && m.uid() == rustix::process::geteuid().as_raw()
                && m.mode() & 0o077 == 0,
            "unsafe source index"
        );
        file.sync_all()?;
        this.root.sync_all()?;
        this.connect()?.execute_batch("PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS root(id TEXT PRIMARY KEY,kind TEXT NOT NULL,source TEXT);
          CREATE INDEX IF NOT EXISTS root_kind ON root(kind,id);
          CREATE INDEX IF NOT EXISTS root_unknown ON root(source,kind,id);
          CREATE TABLE IF NOT EXISTS object(id TEXT PRIMARY KEY,path TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS member(root TEXT NOT NULL,object TEXT NOT NULL,PRIMARY KEY(root,object));
          CREATE INDEX IF NOT EXISTS member_object ON member(object,root);
          CREATE TABLE IF NOT EXISTS graph(root TEXT NOT NULL,source TEXT NOT NULL,done INTEGER NOT NULL DEFAULT 0,position INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(root,source));
          CREATE INDEX IF NOT EXISTS graph_pending ON graph(root,done,source);
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          INSERT OR IGNORE INTO meta VALUES('legacy_registration','pending');")?;
        Ok(this)
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
        self.current()?;
        let file: File = rustix::fs::openat(
            &*self.root,
            "mutation.lock",
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )?
        .into();
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
        })?;
        let opened = rustix::fs::fstat(&file)?;
        let named = rustix::fs::statat(
            &*self.root,
            "mutation.lock",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        ensure!(
            (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
            "source lock changed"
        );
        file.sync_all()?;
        self.root.sync_all()?;
        Ok(Guard {
            _file: Arc::new(file),
            root: self.path.clone(),
        })
    }
    fn connect(&self) -> Result<Connection> {
        self.current()?;
        let c = Connection::open_with_flags(
            &self.database,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        c.busy_timeout(std::time::Duration::from_secs(1))?;
        Ok(c)
    }
    pub(crate) fn pending(&self, id: &str) -> Result<()> {
        self.connect()?.execute(
            "INSERT OR IGNORE INTO root(id,kind) VALUES(?1,'pending')",
            [id],
        )?;
        Ok(())
    }
    fn load_manifest(&self, id: &str) -> Result<local_ci::source::SourceManifest> {
        ensure!(
            super::classify::source_id_of_ci(id).is_some(),
            "invalid source manifest identity"
        );
        let dir = rustix::fs::openat(&*self.root, "manifests", DIR, rustix::fs::Mode::empty())?;
        let file: File = rustix::fs::openat(
            &dir,
            format!("{id}.json"),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?
        .into();
        ensure!(file.metadata()?.is_file(), "source manifest is not regular");
        let manifest: local_ci::source::SourceManifest = serde_json::from_reader(file)?;
        ensure!(
            manifest.source_id == id,
            "source manifest identity mismatch"
        );
        let opened = rustix::fs::fstat(&dir)?;
        let named = rustix::fs::statat(
            &*self.root,
            "manifests",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        ensure!(
            (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
            "source manifests directory changed"
        );
        Ok(manifest)
    }
    /// Capture may finish its own graph; the resident uses bounded steps instead.
    pub(crate) fn register(
        &self,
        store: &SourceStore,
        root: &str,
        kind: &str,
        id: &str,
    ) -> Result<()> {
        while !self.register_step(store, root, kind, id, 32)? {}
        Ok(())
    }
    fn register_step(
        &self,
        _store: &SourceStore,
        root: &str,
        kind: &str,
        id: &str,
        limit: u32,
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
            let next: Option<(String,u32)> = c
                .query_row(
                    "SELECT source,position FROM graph WHERE root=?1 AND done=0 ORDER BY source LIMIT 1",
                    [root],
                    |r| Ok((r.get(0)?,r.get(1)?)),
                )
                .optional()?;
            let Some((next, position)) = next else {
                c.execute(
                    "UPDATE root SET source=?2,kind=?3 WHERE id=?1",
                    params![root, id, kind],
                )?;
                return Ok(true);
            };
            // All filesystem IO occurs outside the source-index transaction.
            let m = self.load_manifest(&next)?;
            super::store::verify_manifest(&m)?;
            let tx = c.transaction()?;
            let key = format!("m:{}", m.source_id);
            let path = format!("manifests/{}.json", m.source_id);
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
    pub(crate) fn finish_imports(&self, root: &str) -> Result<()> {
        // Published graph membership is durable before any pending blob root is released.
        self.connect()?.execute("DELETE FROM root WHERE kind='pending' AND source IS NULL AND id IN (SELECT 'blob:'||substr(object,3) FROM member WHERE root=?1 AND object LIKE 'b:%')",[root])?;
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
        tx.execute("DELETE FROM member WHERE root=?1", [pending])?;
        tx.execute("DELETE FROM root WHERE id=?1 AND kind='pending'", [pending])?;
        tx.commit()?;
        Ok(())
    }
}
impl SourceStore {
    /// Nonblocking; acquire before coordinator operations, never inside its actor/txn.
    pub fn mutation_guard(&self) -> Result<Guard> {
        self.retention.guard(false)
    }
    pub(crate) fn capture_pending_root(&self) -> Result<String> {
        let id = format!("capture:{}", ulid::Ulid::new());
        self.retention.pending(&id)?;
        Ok(id)
    }
}

/// Only a dry-run cursor/report: no root expiry, deletion claim, or filesystem removal.
#[derive(Default)]
pub struct PreviewCursor {
    run: Option<String>,
    object: String,
    legacy: Option<rustix::fs::Dir>,
    legacy_pending: Option<String>,
}
#[derive(Debug, Default)]
pub struct Preview {
    pub held: bool,
    pub registered: u32,
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
                let done = self.retention.register_step(
                    self,
                    &format!("legacy:{id}"),
                    "legacy",
                    &id,
                    1,
                )?;
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
        let unknown: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE source IS NULL)",
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
mod tests {
    use super::*;
    use crate::ids::{AttemptId, Digest, RunId, TaskId, WorkerId};
    use crate::source::{capture, capture_for_run, CaptureParams, SourceRequest};
    use crate::state::{AttemptState, Outcome, Termination};
    use crate::storage::{CoordinatorStore, NewAttempt};
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
            let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
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
            for state in [
                AttemptState::Preparing,
                AttemptState::Ready,
                AttemptState::Starting,
                AttemptState::Active,
                AttemptState::Finalizing,
            ] {
                self.owner.transition_attempt(&attempt, state, 3).unwrap();
            }
            self.owner
                .finalize_attempt(
                    &attempt,
                    Termination::new(Outcome::Failed, Some(crate::state::Exit::Code(9)), None)
                        .unwrap(),
                    4,
                )
                .unwrap();
            self.owner
                .close_collection(
                    &attempt,
                    None,
                    crate::results::plan::Verdict::GaveUp,
                    false,
                    5,
                )
                .unwrap();
            (task, attempt)
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
        let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
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
    #[ignore = "owned helper subprocess"]
    fn source_lock_child() {
        use std::io::{Read, Write};
        let state = std::env::var_os("RTR_SOURCE_LOCK_HELPER").expect("helper path");
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
            .env("RTR_SOURCE_LOCK_HELPER", &f.state)
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
            f.store.object_store().manifest_path(&b),
            f.store.object_store().manifest_path(&a),
        )
        .unwrap();
        assert!(f
            .store
            .retention
            .load_manifest(&a)
            .unwrap_err()
            .to_string()
            .contains("identity mismatch"));
    }
    #[test]
    fn source_root_symlink_is_rejected_before_external_store_creation() {
        let root = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
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
}
