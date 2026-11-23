//! The parent rows of the pruning of a run (spec 25.5): the mark (step 1),
//! the results of a marked run (step 3), its rows (step 6), and the records
//! that belong to no run. Filesystem work is never performed in a
//! transaction; the driver (`service::resident::run_pruning`) orders the
//! steps.
//!
//! The mark is never cleared, so everything a later step needs is decided
//! with it: a run is marked only when it has ended as spec 25.3 (a) reads
//! it, its retention has passed on the clock of the result collection, and
//! every termination of it is one this version can read. After the mark no
//! step asks for a period, an acknowledgement or a clock again.
use super::retention::{finish_claim, pending_claims, run_end_on};
use super::*;
use crate::results::{
    clock_policy::{self, ClockState},
    pin::RunPin,
    store::ResultStore,
};

/// Spec 25.11: one transaction deletes at most this many rows.
pub const ROWS_PER_TRANSACTION: u32 = 1000;
/// Spec 25.5: a test timing that was not written for this long is deleted
/// (the days after which an unused setup template goes).
pub const TEST_TIMING_MS: u64 = 14 * 24 * 60 * 60 * 1000;

#[derive(Debug, Default)]
pub struct MarkPage {
    /// The clock is held: no period is judged, nothing was marked.
    pub clock_held: bool,
    /// The time the periods of this pass are measured at.
    pub now: Option<u64>,
    pub examined: u32,
    pub marked: Vec<RunId>,
    /// Where the next page continues; `None` after the last run.
    pub next: Option<RunId>,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum RowsState {
    /// The transaction was full: more rows of the run are left.
    #[default]
    More,
    /// A result reference or a deletion claim of the run is left: step 3
    /// comes first.
    Results,
    /// Only the last transaction is left, the one that deletes the row of
    /// the run and its `source_ref`: it needs the source lock held
    /// exclusively.
    Source,
    /// No row of the run is left.
    Done,
}

#[derive(Debug, Default)]
pub struct RowsPage {
    pub rows: u32,
    pub state: RowsState,
    /// The attempts whose rows this transaction deleted, with their worker:
    /// read before the delete, and kept in memory only (spec 25.5, step 7).
    pub attempts: Vec<(WorkerId, AttemptId)>,
    /// Events of the attempts this transaction deleted that name another
    /// run: kept, as that run's. No writer of this version makes one
    /// (every writer names the run of its attempt).
    pub kept_events: u32,
}

/// The rows of a run below its attempts, children first (spec 25.5, "段6の
/// 行を消す順", 1 and 3 to 5). `?1` is the run, `?2` what is left of the
/// transaction's bound. Every statement finds its rows by an index.
const BELOW_ATTEMPTS: &[&str] = &[
    "DELETE FROM event WHERE seq IN (SELECT seq FROM event WHERE run_id=?1 ORDER BY seq LIMIT ?2)",
    // The events that name an attempt and no run (`Coordinator::note`). An
    // event of an attempt of this run that names another run is that run's,
    // and stays (counted in `RowsPage::kept_events`).
    "DELETE FROM event WHERE seq IN (SELECT e.seq FROM task t JOIN attempt a USING(task_id) JOIN event e ON e.attempt_id=a.attempt_id WHERE t.run_id=?1 AND (e.run_id IS NULL OR e.run_id=?1) LIMIT ?2)",
    "DELETE FROM blob_ref WHERE ref_id IN (SELECT b.ref_id FROM task t JOIN attempt a USING(task_id) JOIN blob_ref b ON b.attempt_id=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM workflow_download WHERE (consumer,action_key) IN (SELECT d.consumer,d.action_key FROM task t JOIN attempt a USING(task_id) JOIN workflow_download d ON d.consumer=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM workflow_upload_inflight WHERE (producer,action_key) IN (SELECT u.producer,u.action_key FROM task t JOIN attempt a USING(task_id) JOIN workflow_upload_inflight u ON u.producer=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM workflow_artifact WHERE (run_id,workflow,name,version) IN (SELECT run_id,workflow,name,version FROM workflow_artifact WHERE run_id=?1 LIMIT ?2)",
    "DELETE FROM artifact_receipt WHERE (attempt_id,name) IN (SELECT r.attempt_id,r.name FROM task t JOIN attempt a USING(task_id) JOIN artifact_receipt r ON r.attempt_id=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM collection_log WHERE (attempt_id,stream) IN (SELECT l.attempt_id,l.stream FROM task t JOIN attempt a USING(task_id) JOIN collection_log l ON l.attempt_id=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM collection WHERE attempt_id IN (SELECT c.attempt_id FROM task t JOIN attempt a USING(task_id) JOIN collection c ON c.attempt_id=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM retire_tombstone WHERE attempt_id IN (SELECT r.attempt_id FROM task t JOIN attempt a USING(task_id) JOIN retire_tombstone r ON r.attempt_id=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM matrix_permit WHERE permit_id IN (SELECT p.permit_id FROM task t JOIN attempt a USING(task_id) JOIN matrix_permit p ON p.attempt_id=a.attempt_id WHERE t.run_id=?1 LIMIT ?2)",
    "DELETE FROM matrix_permit WHERE permit_id IN (SELECT p.permit_id FROM matrix_group g JOIN matrix_permit p ON p.group_id=g.group_id WHERE g.run_id=?1 LIMIT ?2)",
    "DELETE FROM placement WHERE placement_id IN (SELECT p.placement_id FROM task t JOIN placement p ON p.task_id=t.task_id WHERE t.run_id=?1 LIMIT ?2)",
];
/// 6: the attempts, read as they go.
const ATTEMPTS: &str = "DELETE FROM attempt WHERE attempt_id IN (SELECT a.attempt_id FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1 LIMIT ?2) RETURNING worker_id,attempt_id";
/// 7 and 8: what names the run or a task of it, then the tasks.
const ABOVE_ATTEMPTS: &[&str] = &[
    "DELETE FROM matrix_group WHERE group_id IN (SELECT group_id FROM matrix_group WHERE run_id=?1 LIMIT ?2)",
    "DELETE FROM ci_driver_transition WHERE (run_id,authorization,revision) IN (SELECT run_id,authorization,revision FROM ci_driver_transition WHERE run_id=?1 LIMIT ?2)",
    "DELETE FROM run_cancel_intent WHERE run_id=?1 AND ?2>0",
    "DELETE FROM control_request WHERE request_id IN (SELECT request_id FROM control_request WHERE run_id=?1 LIMIT ?2)",
    "DELETE FROM task WHERE task_id IN (SELECT task_id FROM task WHERE run_id=?1 LIMIT ?2)",
];

/// `Some(true)` for a marked run, `Some(false)` for a run without the mark,
/// `None` for a run without a row.
fn marked(c: &Connection, run: &str) -> Result<Option<bool>> {
    Ok(c.query_row(
        "SELECT pruning_at_ms IS NOT NULL FROM run WHERE run_id=?1",
        [run],
        |r| r.get(0),
    )
    .optional()?)
}

/// Every deletion of the pruning is of a marked run, said again in the
/// transaction that deletes.
fn require_marked(c: &Connection, run: &str) -> Result<()> {
    match marked(c, run)? {
        Some(true) => Ok(()),
        Some(false) => Err(StorageError::Invalid(format!(
            "run {run} is not marked for pruning"
        ))),
        None => Err(not_found(format!("run {run}"))),
    }
}

/// Every termination of the run is one this version reads (the check the
/// retention of the inputs makes, `input_retention_binding`): a run with a
/// record that cannot be read is kept whole.
fn terminations_read(c: &Connection, run: &str) -> Result<bool> {
    let mut statement = c.prepare(
        "SELECT a.termination_json FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1 AND a.retired=0",
    )?;
    let mut rows = statement.query([run])?;
    while let Some(row) = rows.next()? {
        let Some(termination) = row.get::<_, Option<String>>(0)? else {
            return Ok(false);
        };
        if serde_json::from_str::<Termination>(&termination).is_err() {
            return Ok(false);
        }
    }
    Ok(true)
}

impl CoordinatorStore {
    fn pruning_owner(&self) -> Result<()> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }
        Ok(())
    }

    /// Step 1. One page of runs after `after`, in the order of their IDs:
    /// each run that has ended, whose retention has passed and whose record
    /// can be read gets the mark, in the transaction that judged it. The
    /// clock is the one of the result collection (chapter 23); while it is
    /// held nothing is marked.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn mark_ended_runs(
        &self,
        after: Option<RunId>,
        limit: u32,
        wall: u64,
        mono: u64,
        boot: Option<String>,
        retention: u64,
    ) -> Result<MarkPage> {
        self.pruning_owner()?;
        self.db.write(move |tx| {
            let previous: Option<String> = tx
                .query_row(
                    "SELECT state_json FROM result_gc_clock WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let previous = previous
                .as_deref()
                .map(|s| from_json::<ClockState>("result GC clock", s))
                .transpose()?;
            let step = clock_policy::step(previous.as_ref(), boot.as_deref(), wall, mono, retention);
            if let Some(state) = step.state {
                tx.execute(
                    "INSERT INTO result_gc_clock VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET state_json=excluded.state_json",
                    [json(&state)],
                )?;
            }
            let Some(now) = step.cutoff else {
                return Ok(MarkPage {
                    clock_held: true,
                    ..Default::default()
                });
            };
            let runs: Vec<String> = {
                let mut s = tx.prepare(
                    "SELECT run_id FROM run WHERE pruning_at_ms IS NULL AND run_id > coalesce(?1,'') ORDER BY run_id LIMIT ?2",
                )?;
                let rows = s.query_map(params![after.map(String::from), limit], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let mut page = MarkPage {
                now: Some(now),
                examined: runs.len() as u32,
                ..Default::default()
            };
            for run in &runs {
                let Some(ended) = run_end_on(tx, run)? else {
                    continue;
                };
                if ended > now || now - ended < retention || !terminations_read(tx, run)? {
                    continue;
                }
                tx.execute(
                    "UPDATE run SET pruning_at_ms=?2 WHERE run_id=?1 AND pruning_at_ms IS NULL",
                    params![run, to_i64(now)?],
                )?;
                page.marked.push(id("run_id", RunId::parse(run))?);
            }
            if runs.len() == limit as usize {
                page.next = runs
                    .last()
                    .map(|run| id("run_id", RunId::parse(run)))
                    .transpose()?;
            }
            Ok(page)
        })
    }

    /// The marked runs after `after`: what a pass continues. Found by the
    /// index of marked runs, never by reading every run.
    pub(crate) fn marked_runs(&self, after: Option<RunId>, limit: u32) -> Result<Vec<RunId>> {
        self.db.call(move |c| {
            let mut s = c.prepare(
                "SELECT run_id FROM run WHERE pruning_at_ms IS NOT NULL AND run_id > coalesce(?1,'') ORDER BY run_id LIMIT ?2",
            )?;
            let rows = s.query_map(params![after.map(String::from), limit], |r| {
                r.get::<_, String>(0)
            })?;
            rows.map(|run| id("run_id", RunId::parse(&run?))).collect()
        })
    }

    /// How many runs carry the mark: runs whose pruning has not reached its
    /// end (for `doctor`).
    pub fn marked_run_count(&self) -> Result<u64> {
        if !self.pruning {
            return Ok(0);
        }
        self.db.call(|c| {
            to_u64(c.query_row(
                "SELECT count(*) FROM run WHERE pruning_at_ms IS NOT NULL",
                [],
                |r| r.get(0),
            )?)
        })
    }

    /// Step 3. The exclusive pin of a marked run, taken without waiting.
    /// `None` when the run has no pin file: a reader only opens the file,
    /// and what makes it makes it for a run without the mark only, so a
    /// marked run without the file has no reader and gets none. A reader
    /// that holds the pin is `WouldBlock`.
    pub(crate) fn prune_pin(&self, run: &RunId) -> Result<Option<RunPin>> {
        self.pruning_owner()?;
        let known = run.to_string();
        self.db.call(move |c| require_marked(c, &known))?;
        match RunPin::exclusive(&self.state_dir, run) {
            Ok(pin) => Ok(Some(pin)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    /// Step 3. Drop at most `limit` result references of a marked run and
    /// claim the deletion of each file no reference names any more. The
    /// retention is not judged again: the mark settled it. Returns how many
    /// references were dropped (0: none is left) and how many of them name
    /// a file that is not of the result store (such a file is not ours to
    /// delete; only the reference goes).
    pub(crate) fn prune_claim_results(
        &self,
        run: &RunId,
        limit: u32,
        at_ms: u64,
    ) -> Result<(u32, u32)> {
        self.pruning_owner()?;
        let run = run.to_string();
        self.db.write(move |tx| {
            require_marked(tx, &run)?;
            let refs: Vec<(String, String, String)> = {
                let mut s = tx.prepare(
                    "SELECT b.ref_id,b.attempt_id,b.locator FROM result_gc_ref g JOIN blob_ref b USING(ref_id) WHERE g.run_id=?1 ORDER BY g.ref_id LIMIT ?2",
                )?;
                let rows =
                    s.query_map(params![run, limit], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let mut foreign = 0;
            for (reference, attempt, locator) in &refs {
                tx.execute("DELETE FROM blob_ref WHERE ref_id=?1", [reference])?;
                if !ResultStore::owns(locator) {
                    foreign += 1;
                    continue;
                }
                let named: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM blob_ref WHERE locator=?1)",
                    [locator],
                    |r| r.get(0),
                )?;
                if !named {
                    tx.execute(
                        "INSERT OR IGNORE INTO pending_delete(locator,queued_at_ms) VALUES(?1,?2)",
                        params![locator, to_i64(at_ms)?],
                    )?;
                    tx.execute(
                        "INSERT OR IGNORE INTO result_delete_claim(locator,run_id,attempt_id) VALUES(?1,?2,?3)",
                        params![locator, run, attempt],
                    )?;
                }
            }
            Ok((refs.len() as u32, foreign))
        })
    }

    /// Step 3. The deletion claims of a marked run that are not finished.
    pub(crate) fn prune_pending_results(
        &self,
        run: &RunId,
        limit: u32,
    ) -> Result<Vec<ResultDeleteClaim>> {
        let run = run.to_string();
        self.db.call(move |c| {
            require_marked(c, &run)?;
            pending_claims(c, &run, limit)
        })
    }

    /// Step 3. The file of `claim` is gone: forget the claim.
    pub(crate) fn prune_finish_result(&self, claim: &ResultDeleteClaim) -> Result<()> {
        self.pruning_owner()?;
        let claim = claim.clone();
        self.db.write(move |tx| {
            require_marked(tx, claim.run.as_str())?;
            finish_claim(tx, &claim)
        })
    }

    /// Step 3. One page of the attempts of a run after `(task, attempt)`,
    /// with the position after it: the directories of their results.
    pub(crate) fn run_attempts_page(
        &self,
        run: &RunId,
        after: (String, String),
        limit: u32,
    ) -> Result<Vec<(String, AttemptId)>> {
        let run = run.to_string();
        self.db.call(move |c| {
            let mut s = c.prepare(
                "SELECT t.task_id,a.attempt_id FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1 AND (t.task_id,a.attempt_id)>(?2,?3) ORDER BY t.task_id,a.attempt_id LIMIT ?4",
            )?;
            let rows = s.query_map(params![run, after.0, after.1, limit], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            rows.map(|row| {
                let (task, attempt) = row?;
                Ok((task, id("attempt_id", AttemptId::parse(&attempt))?))
            })
            .collect()
        })
    }

    /// Step 4. The source of a run, and whether another run that has a row
    /// names the same source. `None` for a run without a row.
    pub(crate) fn run_source(&self, run: &RunId) -> Result<Option<(Option<SourceId>, bool)>> {
        let run = run.to_string();
        self.db.call(move |c| {
            let row: Option<(Option<String>, bool)> = c
                .query_row(
                    "SELECT r.source_id, EXISTS(SELECT 1 FROM run o WHERE o.source_id=r.source_id AND o.run_id<>r.run_id) FROM run r WHERE r.run_id=?1",
                    [&run],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            row.map(|(source, shared)| {
                Ok((
                    source
                        .map(|source| id("source_id", SourceId::parse(&source)))
                        .transpose()?,
                    shared,
                ))
            })
            .transpose()
        })
    }

    /// Step 6. One transaction of at most `limit` rows of a marked run, in
    /// the order of spec 25.5: children before parents, so the foreign keys
    /// hold after every transaction. The last transaction deletes the row of
    /// the run and, with it, the source and the secret bundle records no run
    /// names any more. That one runs only with `source`, the source lock
    /// held exclusively, so that no submit is between its `put_source_ref`
    /// and the insert of its run (both under the lock held shared).
    pub(crate) fn prune_rows(
        &self,
        run: &RunId,
        limit: u32,
        source: Option<&crate::source::retention::ExclusiveGuard>,
    ) -> Result<RowsPage> {
        self.pruning_owner()?;
        if let Some(source) = source {
            if source.root() != self.state_dir.join("sources") {
                return Err(StorageError::Invalid("foreign source guard".into()));
            }
        }
        let locked = source.is_some();
        let run = run.to_string();
        self.db.write(move |tx| {
            let mut page = RowsPage::default();
            match marked(tx, &run)? {
                None => {
                    page.state = RowsState::Done;
                    return Ok(page);
                }
                Some(false) => {
                    return Err(StorageError::Invalid(format!(
                        "run {run} is not marked for pruning"
                    )))
                }
                Some(true) => {}
            }
            // 2: what step 3 left unfinished is finished by step 3.
            let results: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM result_delete_claim WHERE run_id=?1) OR EXISTS(SELECT 1 FROM result_gc_ref WHERE run_id=?1)",
                [&run],
                |r| r.get(0),
            )?;
            if results {
                page.state = RowsState::Results;
                return Ok(page);
            }
            let mut left = limit;
            for sql in BELOW_ATTEMPTS {
                let n = tx.execute(sql, params![run, left])? as u32;
                page.rows += n;
                left -= n;
                if left == 0 {
                    return Ok(page);
                }
            }
            {
                let mut s = tx.prepare(ATTEMPTS)?;
                let rows = s.query_map(params![run, left], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?;
                for row in rows {
                    let (worker, attempt) = row?;
                    page.attempts.push((
                        id("worker_id", WorkerId::parse(&worker))?,
                        id("attempt_id", AttemptId::parse(&attempt))?,
                    ));
                }
            }
            for (_, attempt) in &page.attempts {
                let kept: u32 = tx.query_row(
                    "SELECT count(*) FROM event WHERE attempt_id=?1 AND run_id IS NOT NULL AND run_id<>?2",
                    params![attempt.as_str(), run],
                    |r| r.get(0),
                )?;
                page.kept_events += kept;
            }
            let n = page.attempts.len() as u32;
            page.rows += n;
            left -= n;
            if left == 0 {
                return Ok(page);
            }
            for sql in ABOVE_ATTEMPTS {
                let n = tx.execute(sql, params![run, left])? as u32;
                page.rows += n;
                left -= n;
                if left == 0 {
                    return Ok(page);
                }
            }
            // 9: the run, and what only it named.
            if !locked {
                page.state = RowsState::Source;
                return Ok(page);
            }
            let (source, bundle): (Option<String>, Option<String>) = tx.query_row(
                "SELECT source_id,secret_bundle_id FROM run WHERE run_id=?1",
                [&run],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            tx.execute("DELETE FROM run WHERE run_id=?1", [&run])?;
            page.rows += 1;
            if let Some(source) = source {
                tx.execute(
                    "DELETE FROM source_ref WHERE source_id=?1 AND NOT EXISTS(SELECT 1 FROM run WHERE source_id=?1)",
                    [&source],
                )?;
            }
            if let Some(bundle) = bundle {
                tx.execute(
                    "DELETE FROM secret_bundle_ref WHERE bundle_id=?1 AND NOT EXISTS(SELECT 1 FROM run WHERE secret_bundle_id=?1)",
                    [&bundle],
                )?;
            }
            page.state = RowsState::Done;
            Ok(page)
        })
    }

    /// The records that belong to no run (spec 25.5): the events that name
    /// neither a run nor an attempt go `retention` after they were written,
    /// in the order of their sequence, and a test timing goes 14 days after
    /// it was last written. `now` is the time [`Self::mark_ended_runs`]
    /// answered: without one (the clock is held) nothing is deleted by age.
    /// At most `limit` rows of each.
    pub(crate) fn prune_unowned(&self, now: u64, retention: u64, limit: u32) -> Result<(u32, u32)> {
        self.pruning_owner()?;
        self.db.write(move |tx| {
            let mut events = 0;
            if let Some(before) = now.checked_sub(retention) {
                events = tx.execute(
                    "DELETE FROM event WHERE at_ms<=?1 AND seq IN (SELECT seq FROM event WHERE run_id IS NULL AND attempt_id IS NULL ORDER BY seq LIMIT ?2)",
                    params![to_i64(before)?, limit],
                )? as u32;
            }
            let mut timings = 0;
            if let Some(before) = now.checked_sub(TEST_TIMING_MS) {
                timings = tx.execute(
                    "DELETE FROM ci_test_timing WHERE (scope,identity,worker) IN (SELECT scope,identity,worker FROM ci_test_timing WHERE written_at_ms<=?1 ORDER BY written_at_ms LIMIT ?2)",
                    params![to_i64(before)?, limit],
                )? as u32;
            }
            Ok((events, timings))
        })
    }

    /// Step 8. Unlink the pin file of a run whose rows are gone and let go
    /// of the pin.
    pub(crate) fn prune_unpin(&self, pin: RunPin) -> Result<()> {
        self.pruning_owner()?;
        Ok(pin.unlink(&self.state_dir)?)
    }

    /// The scan at start (spec 25.11): a pin file of a run that has no row
    /// is what a stop between step 6 and step 8 left. One `readdir`, one
    /// lookup by primary key for each entry. The resident calls it before
    /// it serves a request, so it does not race the making of a pin (the
    /// pin of a new run is made before its row). Returns the pins removed
    /// and the entries left in place that are not the pin of a run with a
    /// row (a name that is not a run's, a pin that is held, an error):
    /// those are never deleted here.
    pub(crate) fn sweep_orphan_pins(&self) -> (u32, u32) {
        let (mut removed, mut kept) = (0, 0);
        if self._lock.is_none() {
            return (0, 0);
        }
        let Ok(entries) = std::fs::read_dir(self.state_dir.join("reader-pins")) else {
            return (0, 0);
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let run = name
                .to_str()
                .and_then(|name| name.strip_suffix(".lock"))
                .and_then(|id| RunId::parse(id).ok());
            let Some(run) = run else {
                kept += 1;
                continue;
            };
            match self.has_run(&run) {
                Ok(true) => {}
                Ok(false) => {
                    let gone = RunPin::exclusive(&self.state_dir, &run)
                        .and_then(|pin| pin.unlink(&self.state_dir));
                    match gone {
                        Ok(()) => removed += 1,
                        Err(_) => kept += 1,
                    }
                }
                Err(_) => kept += 1,
            }
        }
        (removed, kept)
    }
}
