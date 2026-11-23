//! The coordinator database: cluster and epoch, runs, task, attempts,
//! placement, retire tombstones, matrix permits, the event journal, artifact
//! receipts and the references GC works on.

use super::common::{self, from_json, json, parse_state};
use super::policy::{Access, GcCandidate};
use super::Db;
use super::{
    to_i64, to_u64, DbKind, Event, Finalized, NewBlobRef, ProcessLock, Result, StorageError,
};
#[cfg(test)]
use super::{BlobRef, GcReport, DEFAULT_GC_BATCH};
use crate::ids::{
    AttemptId, ClusterId, CoordinatorEpoch, CoordinatorId, Digest, Generation, IdError,
    ManifestDigest, PayloadHash, PermitId, RunId, SourceId, TaskId, WorkerId,
};
use crate::state::{AttemptState, Collection, Observation, Receipt, Receipts, Termination};
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde_json::Value;
use std::path::Path;

mod retention;
mod workflow_artifact;
pub use retention::{ResultDeleteClaim, ResultGcPage};
pub use workflow_artifact::{WorkflowArtifact, WorkflowUpload};
mod collect;
mod live_log;
pub use live_log::{LiveLogTarget, LogProgress};
mod ci;
pub use ci::{CiAttempt, CiTaskRow};
mod control;
mod identity;
pub use identity::Forgotten;
mod retry;
pub use retry::{PreparedRetry, RetryPreparation};
pub mod query;
mod queue;
pub use collect::{Collectable, CollectionRow, ResultRef, RunStanding};
pub(crate) use queue::MAX_TASKS_PER_RUN;
pub use queue::{
    InputCursor, InputPage, LiveAttempt, LiveAttemptPage, PlaceInput, PlaceOutcome, QueuedTask,
    RetireEnd, Seen, WorkerLoad, PAGE_MAX,
};

const ATTEMPT: &str = "attempt";

const PLACEMENT_COLS: &str =
    "placement_id, task_id, worker_id, attempt_id, demand_json, created_at_ms, acked_at_ms";

fn placement_from_row(r: &rusqlite::Row<'_>) -> Result<Placement> {
    Ok(Placement {
        placement_id: to_u64(r.get(0)?)?,
        task_id: id("task_id", TaskId::parse(&r.get::<_, String>(1)?))?,
        worker_id: id("worker_id", WorkerId::parse(&r.get::<_, String>(2)?))?,
        attempt_id: r
            .get::<_, Option<String>>(3)?
            .map(|a| id("attempt_id", AttemptId::parse(&a)))
            .transpose()?,
        demand: from_json("demand", &r.get::<_, String>(4)?)?,
        created_at_ms: to_u64(r.get(5)?)?,
        acked_at_ms: r.get::<_, Option<i64>>(6)?.map(to_u64).transpose()?,
    })
}

fn id<T>(what: &str, r: std::result::Result<T, IdError>) -> Result<T> {
    r.map_err(|e| StorageError::Corrupt(format!("{what}: {e}")))
}

/// A new attempt. The store assigns the generation and stamps the current
/// coordinator epoch.
#[derive(Debug, Clone)]
pub struct NewAttempt {
    pub attempt_id: AttemptId,
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub payload_hash: PayloadHash,
    /// A matrix permit to bind to this attempt in the same commit.
    pub permit_id: Option<PermitId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub attempt_id: AttemptId,
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub generation: Generation,
    pub payload_hash: PayloadHash,
    pub coordinator_epoch: CoordinatorEpoch,
    pub permit_id: Option<PermitId>,
    pub state: AttemptState,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
    pub observation: Observation,
    pub cancel_requested_at_ms: Option<u64>,
    pub retired: bool,
    /// When the retire was confirmed.
    pub retired_at_ms: Option<u64>,
    /// When the coordinator gave up collecting the outputs.
    pub collection_failed_at_ms: Option<u64>,
    pub created_at_ms: u64,
    pub terminal_at_ms: Option<u64>,
}

const ATTEMPT_COLS: &str = "attempt_id, task_id, worker_id, generation, payload_hash,
    coordinator_epoch, permit_id, state, termination_json, receipts_json, observation,
    cancel_requested_at_ms, retired, created_at_ms, terminal_at_ms, retired_at_ms,
    collection_failed_at_ms";

fn observation_str(o: Observation) -> &'static str {
    match o {
        Observation::Known => "known",
        Observation::Reconciling => "reconciling",
        Observation::Unknown => "unknown",
    }
}

fn parse_observation(s: &str) -> Result<Observation> {
    match s {
        "known" => Ok(Observation::Known),
        "reconciling" => Ok(Observation::Reconciling),
        "unknown" => Ok(Observation::Unknown),
        other => Err(StorageError::Corrupt(format!("observation {other:?}"))),
    }
}

fn attempt_from_row(r: &Row<'_>) -> Result<AttemptRecord> {
    let state = parse_state(&r.get::<_, String>(7)?)?;
    let termination: Option<Termination> = r
        .get::<_, Option<String>>(8)?
        .as_deref()
        .map(|s| from_json("termination", s))
        .transpose()?;
    crate::state::check_termination(state, termination.as_ref())
        .map_err(|e| StorageError::Corrupt(e.to_string()))?;
    Ok(AttemptRecord {
        attempt_id: id("attempt_id", AttemptId::parse(&r.get::<_, String>(0)?))?,
        task_id: id("task_id", TaskId::parse(&r.get::<_, String>(1)?))?,
        worker_id: id("worker_id", WorkerId::parse(&r.get::<_, String>(2)?))?,
        generation: Generation(to_u64(r.get(3)?)?),
        payload_hash: id("payload_hash", PayloadHash::parse(&r.get::<_, String>(4)?))?,
        coordinator_epoch: CoordinatorEpoch(to_u64(r.get(5)?)?),
        permit_id: r
            .get::<_, Option<String>>(6)?
            .map(|s| id("permit_id", PermitId::parse(&s)))
            .transpose()?,
        state,
        termination,
        receipts: from_json("receipts", &r.get::<_, String>(9)?)?,
        observation: parse_observation(&r.get::<_, String>(10)?)?,
        cancel_requested_at_ms: r.get::<_, Option<i64>>(11)?.map(to_u64).transpose()?,
        retired: r.get::<_, i64>(12)? == 1,
        created_at_ms: to_u64(r.get(13)?)?,
        terminal_at_ms: r.get::<_, Option<i64>>(14)?.map(to_u64).transpose()?,
        retired_at_ms: r.get::<_, Option<i64>>(15)?.map(to_u64).transpose()?,
        collection_failed_at_ms: r.get::<_, Option<i64>>(16)?.map(to_u64).transpose()?,
    })
}

fn load_attempt(conn: &Connection, attempt_id: &str) -> Result<Option<AttemptRecord>> {
    conn.query_row(
        &format!("SELECT {ATTEMPT_COLS} FROM attempt WHERE attempt_id = ?1"),
        [attempt_id],
        |r| Ok(attempt_from_row(r)),
    )
    .optional()?
    .transpose()
}

fn run_of(conn: &Connection, attempt_id: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT h.run_id FROM attempt a JOIN task h USING (task_id)
             WHERE a.attempt_id = ?1",
            [attempt_id],
            |r| r.get(0),
        )
        .optional()?)
}

/// Confirm the retire tombstone of an attempt (it must exist).
fn confirm_tombstone(conn: &Connection, aid: &str, at_ms: u64) -> Result<()> {
    let n = conn.execute(
        "UPDATE retire_tombstone SET confirmed_at_ms = COALESCE(confirmed_at_ms, ?1)
         WHERE attempt_id = ?2",
        params![to_i64(at_ms)?, aid],
    )?;
    if n != 1 {
        return Err(not_found(format!("retire tombstone for {aid}")));
    }
    Ok(())
}

/// The confirmed retire of an attempt that did not end: the attempt is
/// retired and its task is free for a new one.
fn free_by_retire(conn: &Connection, aid: &str, at_ms: u64) -> Result<()> {
    let in_place: bool = conn.query_row("SELECT coalesce(json_extract(t.spec_json,'$.in_place'),0) FROM task t JOIN attempt a USING(task_id) WHERE a.attempt_id=?1",[aid],|r|r.get(0))?;
    if in_place {
        return Err(StorageError::Invalid(
            "in-place cannot be automatically retried".into(),
        ));
    }
    confirm_tombstone(conn, aid, at_ms)?;
    free_attempt(conn, aid, at_ms)?;
    let run = run_of(conn, aid)?;
    common::append_event(
        conn,
        run.as_deref(),
        Some(aid),
        "retire_confirmed",
        &Value::Null,
        at_ms,
    )?;
    Ok(())
}

/// Retire an attempt that did not end: its task is free for a new one, and
/// it puts no demand on its worker any more.
fn free_attempt(conn: &Connection, aid: &str, at_ms: u64) -> Result<()> {
    let n = conn.execute(
        "UPDATE attempt SET retired = 1, retired_at_ms = COALESCE(retired_at_ms, ?2)
         WHERE attempt_id = ?1 AND state <> 'terminal'",
        params![aid, to_i64(at_ms)?],
    )?;
    if n != 1 {
        // A result that is fixed is never taken back by a retire: the task
        // would be placed again.
        return Err(StorageError::IllegalState {
            attempt: aid.to_string(),
            state: AttemptState::Terminal,
            why: "a terminal attempt is not retired",
        });
    }
    // A retired attempt puts no demand on its worker any more.
    conn.execute(
        "UPDATE placement SET acked_at_ms = COALESCE(acked_at_ms, ?2)
         WHERE attempt_id = ?1",
        params![aid, to_i64(at_ms)?],
    )?;
    Ok(())
}

fn not_found(what: impl Into<String>) -> StorageError {
    StorageError::NotFound { what: what.into() }
}

/// Why a run that carries the mark `run.pruning_at_ms` is refused (spec
/// 25.3 (b)). The mark is never cleared and the rows of the run are deleted
/// only after it, so a run that carries it does not exist for any reader or
/// writer: the answer is `NOT_FOUND`, and this reason is in its message.
pub(crate) const RUN_PRUNED: &str = "run_pruned";

fn run_pruned(run: &str) -> StorageError {
    not_found(format!("run {run} ({RUN_PRUNED})"))
}

/// Whether `run` carries the mark. `known` is false for a file from before
/// the column, which has no marked run. A run without a row has no mark.
fn run_marked(c: &Connection, run: &str, known: bool) -> Result<bool> {
    if !known {
        return Ok(false);
    }
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1 AND pruning_at_ms IS NOT NULL)",
        [run],
        |r| r.get(0),
    )?)
}

/// Refuse to write to a marked run. For the transaction that makes the row:
/// the check and the row are one commit. Only a writable store calls this,
/// and its file has the column.
fn refuse_pruned(c: &Connection, run: &str) -> Result<()> {
    if run_marked(c, run, true)? {
        return Err(run_pruned(run));
    }
    Ok(())
}

/// [`refuse_pruned`] for the run of `task`. A task without a row is left to
/// the caller, which answers for it as before.
fn refuse_pruned_task(c: &Connection, task: &str) -> Result<()> {
    let run: Option<String> = c
        .query_row(
            "SELECT r.run_id FROM task t JOIN run r USING(run_id) WHERE t.task_id=?1 AND r.pruning_at_ms IS NOT NULL",
            [task],
            |r| r.get(0),
        )
        .optional()?;
    match run {
        Some(run) => Err(run_pruned(&run)),
        None => Ok(()),
    }
}

/// The condition of a run row `alias` that carries no mark, for a scan.
fn unmarked(alias: &str, known: bool) -> String {
    if known {
        format!("{alias}.pruning_at_ms IS NULL")
    } else {
        "1".into()
    }
}

/// A global matrix permit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permit {
    pub permit_id: PermitId,
    pub group_id: String,
    pub attempt_id: Option<AttemptId>,
    pub held: bool,
}

/// A placement and the demand it reserved on a worker.
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    pub placement_id: u64,
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub attempt_id: Option<AttemptId>,
    pub demand: Value,
    pub created_at_ms: u64,
    pub acked_at_ms: Option<u64>,
}

/// An artifact receipt (spec chapter 13). `required` rows are declared first
/// and filled in once the parent verified and persisted the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReceipt {
    pub attempt_id: AttemptId,
    pub name: String,
    pub rel_path: String,
    pub size: u64,
    pub digest: Digest,
    pub source_id: SourceId,
    pub plan_digest: Digest,
    pub profile_fingerprint: String,
}

/// The coordinator's database plus the process lock and epoch of this run.
pub struct CoordinatorStore {
    pub(crate) db: Db,
    cluster_id: ClusterId,
    coordinator_id: CoordinatorId,
    epoch: CoordinatorEpoch,
    /// False for a step-1 file opened read-only: its tasks have no cancel
    /// column (no task of it was ever cancelled before it was placed).
    task_cancel: bool,
    retry_boundaries: bool,
    /// False for a file from before the third step opened read-only: it has
    /// no `collection` table (no collection of it was ever begun).
    collections: bool,
    /// False for a file from before `run.pruning_at_ms` opened read-only:
    /// no run of it carries the mark.
    pruning: bool,
    state_dir: std::path::PathBuf,
    _lock: Option<ProcessLock>,
}

impl CoordinatorStore {
    /// Source lock is always acquired before entering a coordinator DB operation.
    /// Nonblocking acquisition never waits under the actor/admission/transaction.
    pub(crate) fn source_mutation_guard(&self) -> Result<crate::source::retention::Guard> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }

        crate::source::SourceStore::open(&self.state_dir)
            .and_then(|source| source.mutation_guard())
            .map_err(|e| StorageError::Invalid(format!("source mutation guard: {e:#}")))
    }
    /// Foreground input preflight only: call outside actor/admission ownership.
    pub(crate) fn source_mutation_guard_foreground(
        &self,
    ) -> Result<crate::source::retention::Guard> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }
        crate::source::SourceStore::open_foreground(&self.state_dir)
            .map(|(_source, guard)| guard)
            .map_err(|error| {
                if error.is::<crate::source::retention::ForegroundBusy>() {
                    StorageError::SourceBusy
                } else {
                    StorageError::Invalid(format!("source mutation guard: {error:#}"))
                }
            })
    }
    /// Resolve immutable target identity, lock one run, then validate fresh metadata.
    /// Export/read commands must retain this pin for their whole invocation.
    ///
    /// The order is "shared pin, then the mark, then the files" (spec 25.3
    /// (b)): the pruning of a run writes the mark, then takes the pin
    /// exclusively, then deletes. A check before the pin alone would let the
    /// mark, the deletion and the release all fall between it and the pin.
    /// So the target is read again once the pin is held, the mark with it in
    /// one snapshot, and a reader that is refused there drops the pin and
    /// opens no file.
    pub fn result_read_pin(
        &self,
        target: &crate::service::completion::Target,
        attempt: Option<&AttemptId>,
    ) -> Result<crate::results::pin::RunPin> {
        self.result_read_pin_after(target, attempt, || ())
    }
    /// [`Self::result_read_pin`]; `between` runs after the first check of
    /// the target and before the pin is taken (a test stops a reader there).
    pub(crate) fn result_read_pin_after(
        &self,
        target: &crate::service::completion::Target,
        attempt: Option<&AttemptId>,
        between: impl FnOnce(),
    ) -> Result<crate::results::pin::RunPin> {
        let run = self.result_target_run(target, attempt)?;
        between();
        let pin =
            crate::results::pin::RunPin::shared(&self.state_dir, &run).map_err(
                |error| {
                    match error.kind() {
                std::io::ErrorKind::NotFound => not_found(
                    "result reader pin unavailable; compatible owner must backfill legacy state",
                ),
                std::io::ErrorKind::WouldBlock => {
                    StorageError::Locked(self.state_dir.join(format!("reader-pins/{run}.lock")))
                }
                _ => StorageError::Io(error),
            }
                },
            )?;
        // A refusal returns here, which drops the pin.
        if self.result_target_run(target, attempt)? != run {
            return Err(StorageError::Invalid(
                "result target changed while acquiring pin".into(),
            ));
        }
        Ok(pin)
    }
    /// The run of a target that exists and carries no mark, read in one
    /// snapshot.
    fn result_target_run(
        &self,
        target: &crate::service::completion::Target,
        attempt: Option<&AttemptId>,
    ) -> Result<RunId> {
        use crate::service::completion::Target;
        let pruning = self.pruning;
        let (target, attempt) = (target.clone(), attempt.map(ToString::to_string));
        self.db.call(move |c| {
            let c = &c.unchecked_transaction()?;
            let run = match (&target, &attempt) {
                (Target::Run(run), None) => {
                    let known: bool = c.query_row(
                        "SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",
                        [run.as_str()],
                        |r| r.get(0),
                    )?;
                    if !known {
                        return Err(not_found("run"));
                    }
                    run.to_string()
                }
                (Target::Task(task), attempt) => {
                    let run: Option<String> = c
                        .query_row(
                            "SELECT run_id FROM task WHERE task_id=?1",
                            [task.as_str()],
                            |r| r.get(0),
                        )
                        .optional()?;
                    let run = run.ok_or_else(|| not_found("task"))?;
                    if let Some(attempt) = attempt {
                        let owned: bool = c.query_row(
                            "SELECT EXISTS(SELECT 1 FROM attempt WHERE attempt_id=?1 AND task_id=?2)",
                            params![attempt, task.as_str()],
                            |r| r.get(0),
                        )?;
                        if !owned {
                            return Err(not_found("attempt of selected task"));
                        }
                    }
                    run
                }
                _ => return Err(StorageError::Invalid("attempt requires task".into())),
            };
            if run_marked(c, &run, pruning)? {
                return Err(run_pruned(&run));
            }
            id("run_id", RunId::parse(&run))
        })
    }
    /// The shared pin of the run of `attempt`, to write its results under.
    /// This makes the pin file, so it is taken only for an attempt whose
    /// task and run have their rows and whose run carries no mark, read in
    /// one snapshot (spec 25.5): the pin of a run that is being pruned is
    /// never made again.
    pub(crate) fn result_writer_pin(
        &self,
        attempt: &AttemptId,
    ) -> Result<crate::results::pin::RunPin> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }
        let attempt = attempt.to_string();
        let run = self.db.call(move |c| {
            let row: Option<(String, bool)> = c
                .query_row(
                    "SELECT r.run_id, r.pruning_at_ms IS NOT NULL FROM attempt a
                     JOIN task t USING(task_id) JOIN run r USING(run_id) WHERE a.attempt_id=?1",
                    [&attempt],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match row {
                None => Err(not_found("attempt, its task or its run")),
                Some((run, true)) => Err(run_pruned(&run)),
                Some((run, false)) => id("run_id", RunId::parse(&run)),
            }
        })?;
        crate::results::pin::RunPin::ensure(&self.state_dir, &run)?;
        Ok(crate::results::pin::RunPin::shared(&self.state_dir, &run)?)
    }
    /// Whether `run` carries the mark of a pruned run. A reader that builds
    /// its answer from several reads asks this before the first of them and
    /// [`Self::run_is_whole`] after the last: the mark is never cleared and
    /// the rows of a run are deleted only after it, so a run that has its
    /// row and no mark after the last read had all its rows at every read
    /// before.
    pub fn run_is_pruned(&self, run: &RunId) -> Result<bool> {
        let (run, pruning) = (run.to_string(), self.pruning);
        self.db.call(move |c| run_marked(c, &run, pruning))
    }
    /// Refuse a read of a task whose run carries the mark. A task without a
    /// row is left to the caller.
    pub(crate) fn refuse_pruned_task_read(&self, task: &TaskId) -> Result<()> {
        if !self.pruning {
            return Ok(());
        }
        let task = task.to_string();
        self.db.call(move |c| refuse_pruned_task(c, &task))
    }
    /// Whether `run` has its row and carries no mark, in one snapshot. This
    /// is what a reader asks after its last read: a run whose pruning ran to
    /// its end meanwhile has no row, and no mark to see either.
    pub fn run_is_whole(&self, run: &RunId) -> Result<bool> {
        let run = run.to_string();
        let unmarked = unmarked("run", self.pruning);
        self.db.call(move |c| {
            Ok(c.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1 AND {unmarked})"),
                [run],
                |r| r.get(0),
            )?)
        })
    }
    /// [`Self::run_is_whole`] for the run of `task`, which has its row too.
    pub(crate) fn task_is_whole(&self, task: &TaskId) -> Result<bool> {
        let task = task.to_string();
        let unmarked = unmarked("r", self.pruning);
        self.db.call(move |c| {
            Ok(c.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM task t JOIN run r USING(run_id) WHERE t.task_id=?1 AND {unmarked})"),
                [task],
                |r| r.get(0),
            )?)
        })
    }
    /// Prepare a private, fully initialized metadata database without claiming an epoch.
    /// The publish callback journals the staged inode before its no-replace rename.
    /// Existing databases are inspected read-only under the owner lock, never migrated.
    pub(crate) fn initialize_identity(
        state_dir: &Path,
        cluster: &ClusterId,
        coordinator: &CoordinatorId,
        publish: impl FnOnce(&Path) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let _owner = ProcessLock::acquire(state_dir, "coordinator.lock")?;
        let target = state_dir.join("coordinator.db");
        if target.try_exists()? {
            let existing = Self::open_read_only(state_dir)?;
            anyhow::ensure!(
                existing.cluster_id() == cluster && existing.coordinator_id() == coordinator,
                "foreign coordinator metadata; no migration or epoch change applied"
            );
            return Ok(());
        }
        let stage = tempfile::Builder::new()
            .prefix(".init-")
            .tempfile_in(state_dir)?;
        let db = Db::open(stage.path(), DbKind::Coordinator, Access::ReadWrite)?;
        let cl = cluster.clone();
        let co = coordinator.clone();
        db.write(move |tx| {
            tx.execute(
                "INSERT INTO cluster VALUES (?1, ?2, 0)",
                params![cl.as_str(), co.as_str()],
            )?;
            tx.execute(
                "INSERT INTO coordinator_state (singleton, cluster_id, epoch) VALUES (1, ?1, 0)",
                [cl.as_str()],
            )?;
            Ok(())
        })?;
        db.call(|conn| {
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
            let mode: String = conn.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))?;
            if mode != "delete" {
                return Err(StorageError::Corrupt(
                    "metadata checkpoint unconfirmed".into(),
                ));
            }
            Ok(())
        })?;
        drop(db);
        stage.as_file().sync_all()?;
        // Retain the staged inode when publication/journal outcome is uncertain.
        let (_file, path) = stage.keep()?;
        publish(&path)?;
        Ok(())
    }

    /// Open the coordinator state in `state_dir`: take the process lock (a
    /// second coordinator fails with `Locked`), migrate the database, create
    /// the cluster on first use and start a new coordinator epoch.
    pub fn open(state_dir: &Path, now_ms: u64) -> Result<Self> {
        Self::open_bound(state_dir, now_ms, None)
    }
    pub(crate) fn open_bound(
        state_dir: &Path,
        now_ms: u64,
        binding: Option<(ClusterId, CoordinatorId)>,
    ) -> Result<Self> {
        let lock = ProcessLock::acquire(state_dir, "coordinator.lock")?;
        if let Some((cluster, coordinator)) = binding {
            let existing = Self::open_read_only(state_dir)?;
            if existing.cluster_id() != &cluster || existing.coordinator_id() != &coordinator {
                return Err(StorageError::Corrupt(
                    "managed coordinator metadata binding differs; no migration/epoch change"
                        .into(),
                ));
            }
        }
        let db = Db::open(
            &state_dir.join("coordinator.db"),
            DbKind::Coordinator,
            Access::ReadWrite,
        )?;
        let (cluster_id, coordinator_id, epoch) = db.write(move |tx| {
            let existing: Option<(String, String)> = tx
                .query_row(
                    "SELECT cluster_id, coordinator_id FROM cluster LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let (cluster, coordinator) = match existing {
                Some((c, o)) => (
                    id("cluster_id", ClusterId::parse(&c))?,
                    id("coordinator_id", CoordinatorId::parse(&o))?,
                ),
                None => {
                    let c = ClusterId::generate();
                    let o = CoordinatorId::generate();
                    tx.execute(
                        "INSERT INTO cluster VALUES (?1, ?2, ?3)",
                        params![c.as_str(), o.as_str(), to_i64(now_ms)?],
                    )?;
                    (c, o)
                }
            };
            tx.execute(
                "INSERT INTO coordinator_state (singleton, cluster_id, epoch) VALUES (1, ?1, 1)
                 ON CONFLICT (singleton) DO UPDATE SET epoch = epoch + 1",
                [cluster.as_str()],
            )?;
            let epoch: i64 =
                tx.query_row("SELECT epoch FROM coordinator_state", [], |r| r.get(0))?;
            common::append_event(
                tx,
                None,
                None,
                "coordinator_started",
                &serde_json::json!({"epoch": epoch}),
                now_ms,
            )?;
            Ok((cluster, coordinator, CoordinatorEpoch(to_u64(epoch)?)))
        })?;
        Ok(Self {
            db,
            cluster_id,
            coordinator_id,
            epoch,
            task_cancel: true,
            retry_boundaries: true,
            collections: true,
            pruning: true,
            state_dir: state_dir.to_path_buf(),
            _lock: Some(lock),
        })
    }

    /// Read the state of a coordinator without taking the lock, bumping the
    /// epoch or migrating. Writes fail.
    pub fn open_read_only(state_dir: &Path) -> Result<Self> {
        let db = Db::open(
            &state_dir.join("coordinator.db"),
            DbKind::Coordinator,
            Access::ReadOnly,
        )?;
        let (cluster_id, coordinator_id, epoch, task_cancel, collections) = db.call(|c| {
            let collections: bool = c.query_row(
                "SELECT count(*) > 0 FROM sqlite_master
                 WHERE type = 'table' AND name = 'collection'",
                [],
                |r| r.get(0),
            )?;
            let task_cancel: bool = c.query_row(
                "SELECT count(*) > 0 FROM pragma_table_info('task')
                 WHERE name = 'cancel_requested_at_ms'",
                [],
                |r| r.get(0),
            )?;
            let (cl, co, ep): (String, String, i64) = c
                .query_row(
                    "SELECT s.cluster_id, c.coordinator_id, s.epoch
                     FROM coordinator_state s JOIN cluster c USING (cluster_id)",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?
                .ok_or_else(|| not_found("coordinator state"))?;
            Ok((
                id("cluster_id", ClusterId::parse(&cl))?,
                id("coordinator_id", CoordinatorId::parse(&co))?,
                CoordinatorEpoch(to_u64(ep)?),
                task_cancel,
                collections,
            ))
        })?;
        let db_has_retry = db.call(|c| {
            Ok(c.query_row(
                "SELECT count(*)>0 FROM pragma_table_info('task') WHERE name='min_generation'",
                [],
                |r| r.get(0),
            )?)
        })?;
        let pruning = db.call(|c| {
            Ok(c.query_row(
                "SELECT count(*)>0 FROM pragma_table_info('run') WHERE name='pruning_at_ms'",
                [],
                |r| r.get(0),
            )?)
        })?;
        Ok(Self {
            db,
            cluster_id,
            coordinator_id,
            epoch,
            task_cancel,
            retry_boundaries: db_has_retry,
            collections,
            pruning,
            state_dir: state_dir.to_path_buf(),
            _lock: None,
        })
    }

    pub fn epoch(&self) -> CoordinatorEpoch {
        self.epoch
    }

    /// Execution only: empty runs and terminal tasks awaiting collection are idle.
    pub fn has_nonterminal_runs(&self) -> Result<bool> {
        let boundary = if self.retry_boundaries {
            "AND aa.generation >= t.min_generation"
        } else {
            ""
        };
        let cancelled = if self.task_cancel {
            "t.cancel_requested_at_ms IS NULL"
        } else {
            "1"
        };
        let sql = format!("SELECT EXISTS(
            SELECT 1 FROM task t LEFT JOIN attempt a ON a.attempt_id = (
                SELECT aa.attempt_id FROM attempt aa
                WHERE aa.task_id = t.task_id AND aa.retired = 0 {boundary}
                ORDER BY aa.generation DESC LIMIT 1)
            WHERE coalesce(json_extract(t.spec_json, '$.ci.task.inputs.belt_discovery.applied'), 0) = 0
              AND ((a.attempt_id IS NULL AND ({cancelled})) OR a.state <> 'terminal'))");
        self.db
            .call(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
    }

    /// Work still owned by this member prevents moving its group.
    pub fn has_group_work(&self) -> Result<bool> {
        self.db.call(|c| Ok(c.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempt a LEFT JOIN collection c USING(attempt_id)
             WHERE (a.state <> 'terminal' AND a.retired = 0)
                OR (a.state = 'terminal' AND c.acked_at_ms IS NULL AND a.collection_failed_at_ms IS NULL))",
            [], |r| r.get(0),
        )?))
    }

    /// Record membership maintenance alongside a running coordinator without
    /// taking its process lock or advancing its epoch.
    pub(crate) fn membership_writer(state: &Path) -> Result<Self> {
        let mut store = Self::open_read_only(state)?;
        store.db = Db::open_without_migration(&state.join("coordinator.db"), DbKind::Coordinator)?;
        Ok(store)
    }

    /// Explicit, idle enrollment; keeps the member's coordinator identity.
    pub(crate) fn move_group(state: &Path, cluster: &ClusterId) -> Result<()> {
        let db = Db::open(
            &state.join("coordinator.db"),
            DbKind::Coordinator,
            Access::ReadWrite,
        )?;
        let cl = cluster.to_string();
        db.write(move |tx| {
            tx.execute(
                "INSERT OR IGNORE INTO cluster (cluster_id, coordinator_id, created_at_ms)
                SELECT ?1, coordinator_id, created_at_ms FROM cluster
                WHERE cluster_id = (SELECT cluster_id FROM coordinator_state)",
                [&cl],
            )?;
            tx.execute("UPDATE coordinator_state SET cluster_id = ?1", [&cl])?;
            tx.execute("DELETE FROM cluster WHERE cluster_id <> ?1", [&cl])?;
            Ok(())
        })
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn coordinator_id(&self) -> &CoordinatorId {
        &self.coordinator_id
    }

    // ------------------------------------------------------- inputs and runs

    /// Record a captured source. Repeating it with the same digest is a no-op;
    /// a different digest for the same id is a conflict.
    pub fn put_source_ref(
        &self,
        source_id: &SourceId,
        manifest: &ManifestDigest,
        at_ms: u64,
    ) -> Result<()> {
        let (sid, digest) = (source_id.to_string(), manifest.to_string());
        self.db.write(move |tx| {
            put_ref(
                tx,
                "source_ref",
                "source_id",
                "manifest_digest",
                &sid,
                &digest,
                at_ms,
            )
        })
    }

    /// Record a secret bundle reference (never its content).
    pub fn put_secret_bundle_ref(
        &self,
        bundle_id: &str,
        digest: &Digest,
        at_ms: u64,
    ) -> Result<()> {
        let (bid, digest) = (bundle_id.to_string(), digest.to_string());
        self.db.write(move |tx| {
            put_ref(
                tx,
                "secret_bundle_ref",
                "bundle_id",
                "digest",
                &bid,
                &digest,
                at_ms,
            )
        })
    }

    pub fn create_run(
        &self,
        run_id: &RunId,
        source_id: Option<&SourceId>,
        secret_bundle_id: Option<&str>,
        spec: &Value,
        at_ms: u64,
    ) -> Result<()> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }
        crate::results::pin::RunPin::ensure(&self.state_dir, run_id)?;
        let run = run_id.to_string();
        let source = source_id.map(ToString::to_string);
        let bundle = secret_bundle_id.map(str::to_string);
        let spec = spec.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "INSERT INTO run (run_id, source_id, secret_bundle_id, spec_json, created_at_ms)
                 VALUES (?1,?2,?3,?4,?5)",
                params![run, source, bundle, spec, to_i64(at_ms)?],
            )?;
            common::append_event(tx, Some(&run), None, "run_created", &Value::Null, at_ms)?;
            Ok(())
        })
    }

    pub fn close_run(&self, run_id: &RunId, at_ms: u64) -> Result<()> {
        let run = run_id.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "UPDATE run SET closed_at_ms = ?1 WHERE run_id = ?2 AND closed_at_ms IS NULL",
                params![to_i64(at_ms)?, run],
            )?;
            Ok(())
        })
    }

    pub fn create_task(
        &self,
        task_id: &TaskId,
        run_id: &RunId,
        spec: &Value,
        at_ms: u64,
    ) -> Result<()> {
        let mutation = self.source_mutation_guard()?;
        mutation
            .ensure_run_available(run_id)
            .map_err(|error| StorageError::Invalid(format!("{error:#}")))?;
        let (task, run, spec) = (task_id.to_string(), run_id.to_string(), spec.to_string());
        self.db.write(move |tx| {
            refuse_pruned(tx, &run)?;
            tx.execute(
                "INSERT INTO task (task_id, run_id, spec_json, created_at_ms)
                 VALUES (?1,?2,?3,?4)",
                params![task, run, spec, to_i64(at_ms)?],
            )?;
            Ok(())
        })
    }

    // ------------------------------------------------------------- attempts

    /// Create the next attempt of a task in `Accepted`, with the
    /// `coordinator accepted` receipt. Fails with `LiveAttemptExists` while an
    /// earlier attempt of the same task is neither terminal nor confirmed
    /// retired: a new attempt needs the old one's end (or retire) first.
    pub fn create_attempt(&self, new: NewAttempt, at_ms: u64) -> Result<AttemptRecord> {
        let retained_mutation = self.source_mutation_guard()?;
        let mutation = retained_mutation.clone();
        let epoch = self.epoch;
        self.db.write(move |tx| {
            let task = new.task_id.to_string();
            let run = tx
                .query_row("SELECT run_id FROM task WHERE task_id = ?1", [&task], |r| {
                    r.get::<_, String>(0)
                })
                .optional()?
                .ok_or_else(|| not_found(format!("task {task}")))?;
            refuse_pruned(tx, &run)?;
            mutation
                .ensure_run_available(
                    &RunId::parse(&run).map_err(|e| StorageError::Invalid(e.to_string()))?,
                )
                .map_err(|error| StorageError::Invalid(format!("{error:#}")))?;
            let live: Option<String> = tx
                .query_row(
                    "SELECT attempt_id FROM attempt
                     WHERE task_id = ?1 AND state <> 'terminal' AND retired = 0",
                    [&task],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(live) = live {
                return Err(StorageError::LiveAttemptExists(live));
            }
            let generation: i64 = tx.query_row(
                "SELECT COALESCE(MAX(generation), 0) + 1 FROM attempt WHERE task_id = ?1",
                [&task],
                |r| r.get(0),
            )?;
            let mut receipts = Receipts::default();
            receipts.mark(Receipt::CoordinatorAccepted, at_ms);
            let aid = new.attempt_id.to_string();
            tx.execute(
                "INSERT INTO attempt (attempt_id, task_id, worker_id, generation, payload_hash,
                    coordinator_epoch, permit_id, state, receipts_json, created_at_ms)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,'accepted',?8,?9)",
                params![
                    aid,
                    task,
                    new.worker_id.as_str(),
                    generation,
                    new.payload_hash.as_str(),
                    to_i64(epoch.0)?,
                    new.permit_id.as_ref().map(PermitId::as_str),
                    json(&receipts),
                    to_i64(at_ms)?
                ],
            )?;
            if let Some(p) = &new.permit_id {
                bind_permit(tx, p.as_str(), &aid, &run)?;
            }
            common::append_event(
                tx,
                Some(&run),
                Some(&aid),
                "attempt_created",
                &serde_json::json!({"worker": new.worker_id.as_str(), "generation": generation}),
                at_ms,
            )?;
            load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))
        })
    }

    pub fn get_attempt(&self, attempt_id: &AttemptId) -> Result<Option<AttemptRecord>> {
        let aid = attempt_id.to_string();
        self.db.call(move |c| load_attempt(c, &aid))
    }

    /// Every attempt that is not terminal, for reconciliation after a
    /// restart. Recovery adopts them; [`AttemptState::recovery_must_not_respawn`]
    /// says which must never be started again.
    pub fn list_nonterminal_attempts(&self) -> Result<Vec<AttemptRecord>> {
        self.db.call(|c| {
            let mut s = c.prepare(&format!(
                "SELECT {ATTEMPT_COLS} FROM attempt WHERE state <> 'terminal' ORDER BY created_at_ms, attempt_id"
            ))?;
            let rows = s.query_map([], |r| Ok(attempt_from_row(r)))?;
            rows.map(|r| r?).collect()
        })
    }

    /// Apply a transition the state machine allows (never to `Terminal`).
    pub fn transition_attempt(
        &self,
        attempt_id: &AttemptId,
        to: AttemptState,
        at_ms: u64,
    ) -> Result<AttemptState> {
        let aid = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = run_of(tx, &aid)?;
            common::transition(tx, ATTEMPT, &aid, to, at_ms, run.as_deref())
        })
    }

    /// Fix the termination, once. A second caller gets the first one's result.
    pub fn finalize_attempt(
        &self,
        attempt_id: &AttemptId,
        termination: Termination,
        at_ms: u64,
    ) -> Result<Finalized> {
        let aid = attempt_id.to_string();
        self.db.write(move |tx| {
            let run = run_of(tx, &aid)?;
            common::finalize(tx, ATTEMPT, &aid, termination, at_ms, run.as_deref())
        })
    }

    pub fn mark_receipt(
        &self,
        attempt_id: &AttemptId,
        receipt: Receipt,
        at_ms: u64,
    ) -> Result<bool> {
        let aid = attempt_id.to_string();
        self.db
            .write(move |tx| common::mark_receipt(tx, ATTEMPT, &aid, receipt, at_ms))
    }

    pub fn set_observation(&self, attempt_id: &AttemptId, o: Observation) -> Result<()> {
        let aid = attempt_id.to_string();
        self.db.write(move |tx| {
            let n = tx.execute(
                "UPDATE attempt SET observation = ?1 WHERE attempt_id = ?2",
                params![observation_str(o), aid],
            )?;
            if n == 1 {
                Ok(())
            } else {
                Err(not_found(format!("attempt {aid}")))
            }
        })
    }

    /// Persist a cancel request (first time wins). It is recorded whatever the
    /// state: the cancel is durable before anything else happens, and a
    /// terminal attempt simply keeps its result.
    pub fn request_cancel(&self, attempt_id: &AttemptId, at_ms: u64) -> Result<bool> {
        let aid = attempt_id.to_string();
        self.db.write(move |tx| {
            let n = tx.execute(
                "UPDATE attempt SET cancel_requested_at_ms = ?1
                 WHERE attempt_id = ?2 AND cancel_requested_at_ms IS NULL",
                params![to_i64(at_ms)?, aid],
            )?;
            if n == 0 {
                // Either it was already requested or the attempt is unknown.
                common::read_core(tx, ATTEMPT, &aid)?;
                return Ok(false);
            }
            let run = run_of(tx, &aid)?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&aid),
                "cancel_requested",
                &Value::Null,
                at_ms,
            )?;
            Ok(true)
        })
    }

    // --------------------------------------------------------------- retire

    /// Persist the retire tombstone (idempotent). Do this before asking the
    /// worker to retire, and before creating a new attempt elsewhere.
    pub fn begin_retire(&self, attempt_id: &AttemptId, reason: &str, at_ms: u64) -> Result<()> {
        self.begin_retire_noting(attempt_id, reason, None, at_ms)
            .map(|_| ())
    }

    /// [`Self::begin_retire`], with an event `note` (kind, payload) on the
    /// attempt that is written in the same transaction as the tombstone: it
    /// is there exactly when this call wrote the tombstone with `reason`.
    /// Returns whether it did (`false`: a retire was already asked for, with
    /// whatever reason it had; nothing was written).
    pub fn begin_retire_noting(
        &self,
        attempt_id: &AttemptId,
        reason: &str,
        note: Option<(&str, &Value)>,
        at_ms: u64,
    ) -> Result<bool> {
        let (aid, reason) = (attempt_id.to_string(), reason.to_string());
        let note = note.map(|(kind, payload)| (kind.to_string(), payload.clone()));
        self.db.write(move |tx| {
            let worker: String = tx
                .query_row(
                    "SELECT worker_id FROM attempt WHERE attempt_id = ?1",
                    [&aid],
                    |r| r.get(0),
                )
                .optional()?
                .ok_or_else(|| not_found(format!("attempt {aid}")))?;
            let n = tx.execute(
                "INSERT OR IGNORE INTO retire_tombstone
                    (attempt_id, worker_id, reason, requested_at_ms) VALUES (?1,?2,?3,?4)",
                params![aid, worker, reason, to_i64(at_ms)?],
            )?;
            if n == 1 {
                let run = run_of(tx, &aid)?;
                common::append_event(
                    tx,
                    run.as_deref(),
                    Some(&aid),
                    "retire_requested",
                    &Value::Null,
                    at_ms,
                )?;
                if let Some((kind, payload)) = &note {
                    common::append_event(tx, run.as_deref(), Some(&aid), kind, payload, at_ms)?;
                }
            }
            Ok(n == 1)
        })
    }

    /// The worker confirmed the attempt never started or has stopped. Needs
    /// the tombstone from [`Self::begin_retire`]; frees the task for a new
    /// attempt. The tombstone itself stays. An attempt that is terminal
    /// keeps its result and is not retired (`IllegalState`).
    pub fn confirm_retire(&self, attempt_id: &AttemptId, at_ms: u64) -> Result<()> {
        let aid = attempt_id.to_string();
        self.db.write(move |tx| free_by_retire(tx, &aid, at_ms))
    }

    /// `(requested_at_ms, confirmed_at_ms)` of the tombstone, if any.
    pub fn retire_tombstone(&self, attempt_id: &AttemptId) -> Result<Option<(u64, Option<u64>)>> {
        let aid = attempt_id.to_string();
        self.db.call(move |c| {
            c.query_row(
                "SELECT requested_at_ms, confirmed_at_ms FROM retire_tombstone WHERE attempt_id = ?1",
                [&aid],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?)),
            )
            .optional()?
            .map(|(a, b)| Ok((to_u64(a)?, b.map(to_u64).transpose()?)))
            .transpose()
        })
    }

    // ------------------------------------------------------------ placement

    /// Reserve `demand` on a worker. It stays in [`Self::unacked_demand`]
    /// until the worker's acceptance is acknowledged.
    pub fn record_placement(
        &self,
        task_id: &TaskId,
        worker_id: &WorkerId,
        attempt_id: Option<&AttemptId>,
        demand: &Value,
        at_ms: u64,
    ) -> Result<u64> {
        let (task, w) = (task_id.to_string(), worker_id.to_string());
        let a = attempt_id.map(ToString::to_string);
        let demand = demand.to_string();
        self.db.write(move |tx| {
            tx.execute(
                "INSERT INTO placement (task_id, worker_id, attempt_id, demand_json, created_at_ms)
                 VALUES (?1,?2,?3,?4,?5)",
                params![task, w, a, demand, to_i64(at_ms)?],
            )?;
            to_u64(tx.last_insert_rowid())
        })
    }

    pub fn ack_placement(&self, placement_id: u64, at_ms: u64) -> Result<()> {
        self.db.write(move |tx| {
            let n = tx.execute(
                "UPDATE placement SET acked_at_ms = COALESCE(acked_at_ms, ?1) WHERE placement_id = ?2",
                params![to_i64(at_ms)?, to_i64(placement_id)?],
            )?;
            if n == 1 {
                Ok(())
            } else {
                Err(not_found(format!("placement {placement_id}")))
            }
        })
    }

    /// The demand of placements no worker acknowledged yet. A placement
    /// leaves this list when its worker accepted the attempt, and also when
    /// the attempt ended or its retire was confirmed without that.
    ///
    /// Everything at once, in memory: for tests and diagnostics. The
    /// coordinator walks [`Self::unacked_demand_page`] instead.
    pub fn unacked_demand(&self) -> Result<Vec<Placement>> {
        self.db.call(|c| {
            let mut s = c.prepare(&format!(
                "SELECT {PLACEMENT_COLS} FROM placement
                 WHERE acked_at_ms IS NULL ORDER BY placement_id"
            ))?;
            let rows = s.query_map([], |r| Ok(placement_from_row(r)))?;
            rows.map(|row| row?).collect()
        })
    }

    /// A page of [`Self::unacked_demand`] for one worker: its placements
    /// after `after` (a `placement_id`; 0 starts), oldest first, at most
    /// [`PAGE_MAX`] rows. Ids only grow, so passing the last id back yields
    /// every placement that stays unacknowledged for the whole walk exactly
    /// once; one acknowledged or made during the walk is yielded at most once.
    pub fn unacked_demand_page(
        &self,
        worker_id: &WorkerId,
        after: u64,
        limit: u32,
    ) -> Result<Vec<Placement>> {
        let worker = worker_id.to_string();
        let after = to_i64(after)?;
        self.db.call(move |c| {
            let mut s = c.prepare(&format!(
                "SELECT {PLACEMENT_COLS} FROM placement
                 WHERE worker_id = ?1 AND acked_at_ms IS NULL AND placement_id > ?2
                 ORDER BY placement_id LIMIT ?3"
            ))?;
            let rows = s.query_map(
                params![worker, after, i64::from(limit.min(PAGE_MAX))],
                |r| Ok(placement_from_row(r)),
            )?;
            rows.map(|row| row?).collect()
        })
    }

    // -------------------------------------------------------- matrix permits

    pub fn create_matrix_group(
        &self,
        group_id: &str,
        run_id: &RunId,
        max_parallel: u32,
        at_ms: u64,
    ) -> Result<()> {
        let (g, r) = (group_id.to_string(), run_id.to_string());
        self.db.write(move |tx| {
            tx.execute(
                "INSERT INTO matrix_group (group_id, run_id, max_parallel, created_at_ms)
                 VALUES (?1,?2,?3,?4)",
                params![g, r, max_parallel, to_i64(at_ms)?],
            )?;
            Ok(())
        })
    }

    /// Take one permit of the group, durably, before submitting. `None` when
    /// every permit is held. Asking again with a permit id that exists returns
    /// that permit as it is, without taking another slot: a retry after a
    /// crash gets its permit back. That includes a permit that was already
    /// released (`held == false`); it no longer counts against the group and
    /// the caller must not submit under it (use a new permit id).
    pub fn acquire_permit(
        &self,
        group_id: &str,
        permit_id: &PermitId,
        at_ms: u64,
    ) -> Result<Option<Permit>> {
        let (g, p) = (group_id.to_string(), permit_id.clone());
        self.db.write(move |tx| reserve_permit(tx, &g, &p, at_ms))
    }

    /// Give the permit back. Refused while its attempt is neither terminal nor
    /// confirmed retired: an unknown attempt keeps its slot.
    pub fn release_permit(&self, permit_id: &PermitId, at_ms: u64) -> Result<()> {
        let p = permit_id.to_string();
        self.db.write(move |tx| {
            let permit = load_permit(tx, &p)?.ok_or_else(|| not_found(format!("permit {p}")))?;
            if let Some(a) = &permit.attempt_id {
                let rec = load_attempt(tx, a.as_str())?
                    .ok_or_else(|| not_found(format!("attempt {a}")))?;
                if !(rec.state.is_terminal() || rec.retired) {
                    return Err(StorageError::IllegalState {
                        attempt: a.to_string(),
                        state: rec.state,
                        why: "permit stays held until terminal or confirmed retired",
                    });
                }
            }
            tx.execute(
                "UPDATE matrix_permit SET held = 0, released_at_ms = COALESCE(released_at_ms, ?1)
                 WHERE permit_id = ?2",
                params![to_i64(at_ms)?, p],
            )?;
            Ok(())
        })
    }

    pub fn get_permit(&self, permit_id: &PermitId) -> Result<Option<Permit>> {
        let p = permit_id.to_string();
        self.db.call(move |c| load_permit(c, &p))
    }

    pub fn held_permits(&self, group_id: &str) -> Result<u32> {
        let g = group_id.to_string();
        self.db.call(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM matrix_permit WHERE group_id = ?1 AND held = 1",
                [&g],
                |r| r.get(0),
            )?)
        })
    }

    // --------------------------------------------------------------- events

    pub fn append_event(
        &self,
        run_id: Option<&RunId>,
        attempt_id: Option<&AttemptId>,
        kind: &str,
        payload: &Value,
        at_ms: u64,
    ) -> Result<u64> {
        let (r, a) = (
            run_id.map(ToString::to_string),
            attempt_id.map(ToString::to_string),
        );
        let (kind, payload) = (kind.to_string(), payload.clone());
        self.db.write(move |tx| {
            common::append_event(tx, r.as_deref(), a.as_deref(), &kind, &payload, at_ms)
        })
    }

    pub fn events_after(&self, after_seq: u64, limit: u32) -> Result<Vec<Event>> {
        self.db
            .call(move |c| common::events_after(c, after_seq, limit))
    }

    /// The events of one attempt with one `kind`, oldest first.
    pub fn events_of(&self, attempt_id: &AttemptId, kind: &str) -> Result<Vec<Event>> {
        let (a, k) = (attempt_id.to_string(), kind.to_string());
        self.db.call(move |c| common::events_of(c, &a, &k))
    }

    /// For each `(key, payload)` of `wanted`, append an event of `kind` to the
    /// attempt unless it already has one of that kind whose `payload[key_field]`
    /// is `key`; the check and the appends are one transaction, so concurrent
    /// callers cannot both append. Returns how many were appended.
    pub fn append_events_unseen(
        &self,
        run_id: Option<&RunId>,
        attempt_id: &AttemptId,
        kind: &str,
        key_field: &str,
        wanted: &[(String, Value)],
        at_ms: u64,
    ) -> Result<u32> {
        let r = run_id.map(ToString::to_string);
        let (a, k, f) = (
            attempt_id.to_string(),
            kind.to_string(),
            key_field.to_string(),
        );
        let wanted = wanted.to_vec();
        self.db.write(move |tx| {
            common::append_events_unseen(tx, r.as_deref(), &a, &k, &f, &wanted, at_ms)
        })
    }

    // ------------------------------------------------------------ artifacts

    /// Declare an output the attempt must (or may) produce.
    pub fn declare_artifact(
        &self,
        attempt_id: &AttemptId,
        name: &str,
        required: bool,
    ) -> Result<()> {
        let (a, n) = (attempt_id.to_string(), name.to_string());
        self.db.write(move |tx| {
            common::read_core(tx, ATTEMPT, &a)?;
            tx.execute(
                "INSERT OR IGNORE INTO artifact_receipt (attempt_id, name, required) VALUES (?1,?2,?3)",
                params![a, n, required],
            )?;
            Ok(())
        })
    }

    /// Record a verified, persisted artifact. Immutable once recorded: the
    /// same receipt again is a no-op, a different one is a conflict.
    pub fn record_artifact(&self, r: &ArtifactReceipt, at_ms: u64) -> Result<()> {
        let r = r.clone();
        self.db
            .write(move |tx| Self::record_artifact_in(tx, &r, at_ms))
    }

    /// [`Self::record_artifact`] in the transaction of the caller.
    pub(super) fn record_artifact_in(
        tx: &rusqlite::Transaction<'_>,
        r: &ArtifactReceipt,
        at_ms: u64,
    ) -> Result<()> {
        {
            let a = r.attempt_id.to_string();
            common::read_core(tx, ATTEMPT, &a)?;
            let persisted: Option<Option<i64>> = tx
                .query_row(
                    "SELECT persisted_at_ms FROM artifact_receipt WHERE attempt_id = ?1 AND name = ?2",
                    params![a, r.name],
                    |row| row.get(0),
                )
                .optional()?;
            let size = to_i64(r.size)?;
            match persisted {
                Some(Some(_)) => {
                    let same: i64 = tx.query_row(
                        "SELECT count(*) FROM artifact_receipt
                         WHERE attempt_id = ?1 AND name = ?2 AND rel_path = ?3 AND size = ?4
                           AND digest = ?5 AND source_id = ?6 AND plan_digest = ?7
                           AND profile_fingerprint = ?8",
                        params![
                            a,
                            r.name,
                            r.rel_path,
                            size,
                            r.digest.as_str(),
                            r.source_id.as_str(),
                            r.plan_digest.as_str(),
                            r.profile_fingerprint
                        ],
                        |row| row.get(0),
                    )?;
                    if same == 1 {
                        return Ok(());
                    }
                    return Err(StorageError::Immutable(format!(
                        "artifact {} of {a}",
                        r.name
                    )));
                }
                Some(None) => {
                    tx.execute(
                        "UPDATE artifact_receipt SET rel_path=?3, size=?4, digest=?5, source_id=?6,
                            plan_digest=?7, profile_fingerprint=?8, persisted_at_ms=?9
                         WHERE attempt_id=?1 AND name=?2",
                        params![
                            a,
                            r.name,
                            r.rel_path,
                            size,
                            r.digest.as_str(),
                            r.source_id.as_str(),
                            r.plan_digest.as_str(),
                            r.profile_fingerprint,
                            to_i64(at_ms)?
                        ],
                    )?;
                }
                None => {
                    tx.execute(
                        "INSERT INTO artifact_receipt (attempt_id, name, required, rel_path, size,
                            digest, source_id, plan_digest, profile_fingerprint, persisted_at_ms)
                         VALUES (?1,?2,0,?3,?4,?5,?6,?7,?8,?9)",
                        params![
                            a,
                            r.name,
                            r.rel_path,
                            size,
                            r.digest.as_str(),
                            r.source_id.as_str(),
                            r.plan_digest.as_str(),
                            r.profile_fingerprint,
                            to_i64(at_ms)?
                        ],
                    )?;
                }
            }
            Ok(())
        }
    }

    /// Set the `collected` receipt. Only when the attempt is terminal and
    /// every required artifact has a persisted receipt (the derivation is
    /// [`Collection::derive`]). Returns whether this call set it.
    pub fn mark_collected(&self, attempt_id: &AttemptId, at_ms: u64) -> Result<bool> {
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let mut core = common::read_core(tx, ATTEMPT, &a)?;
            let (required, persisted) = artifact_counts(tx, &a)?;
            if Collection::derive(core.state, required, persisted, false) != Collection::Collected {
                return Err(StorageError::IllegalState {
                    attempt: a,
                    state: core.state,
                    why: "required outputs are not all collected",
                });
            }
            if !core.receipts.mark(Receipt::Collected, at_ms) {
                return Ok(false);
            }
            tx.execute(
                "UPDATE attempt SET receipts_json = ?1 WHERE attempt_id = ?2",
                params![json(&core.receipts), a],
            )?;
            let run = run_of(tx, &a)?;
            common::append_event(
                tx,
                run.as_deref(),
                Some(&a),
                "collected",
                &Value::Null,
                at_ms,
            )?;
            Ok(true)
        })
    }

    // ------------------------------------------------------- references, GC

    pub fn add_blob_ref(&self, r: &NewBlobRef, at_ms: u64) -> Result<()> {
        let r = r.clone();
        self.db.write(move |tx| common::add_blob_ref(tx, &r, at_ms))
    }

    /// Mark a reference as in use (a cache entry another job reads).
    pub fn pin_blob(&self, ref_id: &str) -> Result<u32> {
        let r = ref_id.to_string();
        self.db.write(move |tx| common::pin(tx, &r, 1))
    }

    pub fn unpin_blob(&self, ref_id: &str) -> Result<u32> {
        let r = ref_id.to_string();
        self.db.write(move |tx| common::pin(tx, &r, -1))
    }

    /// The files waiting to be deleted: their last reference was dropped by
    /// GC. Delete each file, then call [`Self::complete_delete`].
    pub fn pending_deletes(&self) -> Result<Vec<String>> {
        self.db.call(|c| common::pending_deletes(c))
    }

    /// The file of `locator` is deleted: forget it, so the name may be used
    /// again. Idempotent; returns whether a queued locator was removed.
    pub fn complete_delete(&self, locator: &str) -> Result<bool> {
        let l = locator.to_string();
        self.db.write(move |tx| common::complete_delete(tx, &l))
    }

    /// Record that the coordinator gave up collecting the outputs of a
    /// terminal attempt (`COLLECTION_FAILED` is final). From then on GC no
    /// longer waits for the `collected` receipt or the missing artifacts: the
    /// retention is measured from this moment (or the terminal time, if
    /// later). Refused for an attempt that is not terminal or is already
    /// collected. First time wins; returns whether this call recorded it.
    pub fn record_collection_failed(&self, attempt_id: &AttemptId, at_ms: u64) -> Result<bool> {
        let a = attempt_id.to_string();
        self.db.write(move |tx| {
            let core = common::read_core(tx, ATTEMPT, &a)?;
            if !core.state.is_terminal() || core.receipts.reached(Receipt::Collected) {
                return Err(StorageError::IllegalState {
                    attempt: a,
                    state: core.state,
                    why: "collection can only fail for a terminal attempt that is not collected",
                });
            }
            let n = tx.execute(
                "UPDATE attempt SET collection_failed_at_ms = ?1
                 WHERE attempt_id = ?2 AND collection_failed_at_ms IS NULL",
                params![to_i64(at_ms)?, a],
            )?;
            if n == 1 {
                let run = run_of(tx, &a)?;
                common::append_event(
                    tx,
                    run.as_deref(),
                    Some(&a),
                    "collection_failed",
                    &Value::Null,
                    at_ms,
                )?;
            }
            Ok(n == 1)
        })
    }

    /// Drop the references of attempts that ended (and, when their outputs
    /// were collected, were collected) at least `retention_ms` ago and whose
    /// required outputs are collected, or that ended without
    /// anything to wait for: the retire was confirmed
    /// ([`Self::confirm_retire`]) or collection was given up
    /// ([`Self::record_collection_failed`]). Running, awaiting-collection,
    /// pinned and incomplete ones stay. A file whose last reference goes is
    /// queued in `pending_delete` in the same transaction; a new reference to
    /// it is refused until `complete_delete`. Retire tombstones, dedup records
    /// and the event journal are never touched. The pass is a series of
    /// transactions of [`DEFAULT_GC_BATCH`] references; a busy database is
    /// [`StorageError::Busy`] and the pass can simply be run again.
    #[cfg(test)]
    pub fn gc(&self, now_ms: u64, retention_ms: u64) -> Result<GcReport> {
        self.gc_batched(now_ms, retention_ms, DEFAULT_GC_BATCH)
    }

    #[cfg(test)]
    pub(crate) fn gc_batched(
        &self,
        now_ms: u64,
        retention_ms: u64,
        batch: u32,
    ) -> Result<GcReport> {
        common::gc_all(
            &self.db,
            now_ms,
            retention_ms,
            None,
            batch,
            |conn, r: &BlobRef| {
                let rec = load_attempt(conn, &r.attempt_id)?
                    .ok_or_else(|| not_found(format!("attempt {}", r.attempt_id)))?;
                let (required, persisted) = artifact_counts(conn, &r.attempt_id)?;
                // What was collected late is kept for the whole retention
                // from then: the end that counts is the later of the two.
                let closed = collect::closed_at(conn, &r.attempt_id)?;
                Ok(GcCandidate {
                    terminal_at_ms: rec.terminal_at_ms.map(|t| t.max(closed.unwrap_or(0))),
                    retired_at_ms: rec.retired_at_ms,
                    collected: rec.receipts.reached(Receipt::Collected),
                    collection_failed_at_ms: rec.collection_failed_at_ms,
                    missing_required_artifacts: required - persisted,
                    pins: r.pins,
                })
            },
        )
    }
}

fn artifact_counts(conn: &Connection, attempt_id: &str) -> Result<(u32, u32)> {
    Ok(conn.query_row(
        "SELECT count(*), count(persisted_at_ms) FROM artifact_receipt
         WHERE attempt_id = ?1 AND required = 1",
        [attempt_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?)
}

fn put_ref(
    conn: &Connection,
    table: &str,
    id_col: &str,
    digest_col: &str,
    key: &str,
    digest: &str,
    at_ms: u64,
) -> Result<()> {
    let existing: Option<String> = conn
        .query_row(
            &format!("SELECT {digest_col} FROM {table} WHERE {id_col} = ?1"),
            [key],
            |r| r.get(0),
        )
        .optional()?;
    match existing {
        Some(d) if d == digest => Ok(()),
        Some(_) => Err(StorageError::Immutable(format!("{table} {key}"))),
        None => {
            conn.execute(
                &format!(
                    "INSERT INTO {table} ({id_col}, {digest_col}, created_at_ms) VALUES (?1,?2,?3)"
                ),
                params![key, digest, to_i64(at_ms)?],
            )?;
            Ok(())
        }
    }
}

fn load_permit(conn: &Connection, permit_id: &str) -> Result<Option<Permit>> {
    let row = conn
        .query_row(
            "SELECT permit_id, group_id, attempt_id, held FROM matrix_permit WHERE permit_id = ?1",
            [permit_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    row.map(|(p, g, a, held)| {
        Ok(Permit {
            permit_id: id("permit_id", PermitId::parse(&p))?,
            group_id: g,
            attempt_id: a
                .map(|a| id("attempt_id", AttemptId::parse(&a)))
                .transpose()?,
            held: held == 1,
        })
    })
    .transpose()
}

fn reserve_permit(tx: &Connection, g: &str, p: &PermitId, at_ms: u64) -> Result<Option<Permit>> {
    if let Some(existing) = load_permit(tx, p.as_str())? {
        if existing.group_id != g {
            return Err(StorageError::Invalid(format!(
                "permit {p} belongs to another group"
            )));
        }
        return Ok(Some(existing));
    }
    let max: i64 = tx
        .query_row(
            "SELECT max_parallel FROM matrix_group WHERE group_id = ?1",
            [g],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| not_found(format!("matrix group {g}")))?;
    let held: i64 = tx.query_row(
        "SELECT count(*) FROM matrix_permit WHERE group_id = ?1 AND held = 1",
        [g],
        |r| r.get(0),
    )?;
    if held >= max {
        return Ok(None);
    }
    tx.execute(
        "INSERT INTO matrix_permit (permit_id, group_id, created_at_ms) VALUES (?1,?2,?3)",
        params![p.as_str(), g, to_i64(at_ms)?],
    )?;
    load_permit(tx, p.as_str())
}

fn bind_permit(conn: &Connection, permit_id: &str, attempt_id: &str, run: &str) -> Result<()> {
    let permit =
        load_permit(conn, permit_id)?.ok_or_else(|| not_found(format!("permit {permit_id}")))?;
    if !permit.held || permit.attempt_id.is_some() {
        return Err(StorageError::Invalid(format!(
            "permit {permit_id} is released or already bound"
        )));
    }
    let bound = conn.execute(
        "UPDATE matrix_permit SET attempt_id = ?1 WHERE permit_id = ?2
         AND group_id IN (SELECT group_id FROM matrix_group WHERE run_id = ?3)",
        params![attempt_id, permit_id, run],
    )?;
    if bound == 0 {
        return Err(StorageError::Invalid(format!(
            "permit {permit_id} belongs to another run"
        )));
    }
    Ok(())
}
