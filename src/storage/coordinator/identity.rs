//! The identity of an ephemeral worker, as the coordinator remembers it:
//! the one seen last, and the ones that were retired. Retiring an identity
//! ends what it held in the same commit, so a tombstone never exists beside
//! an attempt that still waits for the worker it names.

use super::collect::close_collection_in;
use super::{load_attempt, not_found, run_of, CoordinatorStore, ATTEMPT};
use crate::coordinator::identity::{admit, outcome_of_lost, Admission};
use crate::ids::{AttemptId, WorkerBootId, WorkerId};
use crate::results::plan::Verdict;
use crate::state::{AttemptState, Termination};
use crate::storage::common;
use crate::storage::policy::state_path;
use crate::storage::{to_i64, Result, StorageError};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;

/// What `forget_worker` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Forgotten {
    /// The identity that was retired. `None`: the worker was never seen, or
    /// its last identity was retired before.
    pub identity: Option<String>,
    /// The attempts that were ended, oldest first.
    pub ended: Vec<AttemptId>,
}

fn recorded(conn: &Connection, worker: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT boot_id FROM worker_identity WHERE worker_id = ?1",
            [worker],
            |r| r.get(0),
        )
        .optional()?)
}

fn retired(conn: &Connection, worker: &str, boot: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM worker_identity_tombstone
            WHERE worker_id = ?1 AND boot_id = ?2)",
        [worker, boot],
        |r| r.get(0),
    )?)
}

/// Retire the identity recorded for `worker` (if any) and end every attempt
/// the worker still holds: no identity that could report them will be
/// listened to again. Each attempt ends `interrupted` (`cancelled` when its
/// cancel was asked for), its demand is closed, a retire that was waiting
/// for the worker is confirmed, and its collection is closed as given up,
/// since nothing can be fetched from a worker that is gone.
fn retire(tx: &Connection, worker: &str, at_ms: u64) -> Result<Forgotten> {
    let at = to_i64(at_ms)?;
    let identity = recorded(tx, worker)?;
    if let Some(boot) = &identity {
        tx.execute(
            "INSERT OR IGNORE INTO worker_identity_tombstone
                (worker_id, boot_id, reason, recorded_at_ms) VALUES (?1,?2,?3,?4)",
            params![worker, boot, "forgotten", at],
        )?;
        tx.execute("DELETE FROM worker_identity WHERE worker_id = ?1", [worker])?;
    }
    let live: Vec<String> = {
        let mut query = tx.prepare(
            "SELECT attempt_id FROM attempt
             WHERE worker_id = ?1 AND state <> 'terminal' AND retired = 0
             ORDER BY created_at_ms, rowid",
        )?;
        let rows = query.query_map([worker], |r| r.get(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let mut ended = Vec::with_capacity(live.len());
    for aid in live {
        let before = load_attempt(tx, &aid)?.ok_or_else(|| not_found(format!("attempt {aid}")))?;
        let cancelled: bool = tx.query_row(
            "SELECT a.cancel_requested_at_ms IS NOT NULL OR t.cancel_requested_at_ms IS NOT NULL
             FROM attempt a JOIN task t USING (task_id) WHERE a.attempt_id = ?1",
            [&aid],
            |r| r.get(0),
        )?;
        let termination = Termination::new(outcome_of_lost(cancelled), None, None)
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        let run = run_of(tx, &aid)?;
        tx.execute(
            "UPDATE retire_tombstone SET confirmed_at_ms = COALESCE(confirmed_at_ms, ?1)
             WHERE attempt_id = ?2",
            params![at, aid],
        )?;
        for step in state_path(before.state, AttemptState::Terminal).unwrap_or_default() {
            if step == AttemptState::Terminal {
                common::finalize(tx, ATTEMPT, &aid, termination, at_ms, run.as_deref())?;
            } else {
                common::transition(tx, ATTEMPT, &aid, step, at_ms, run.as_deref())?;
            }
        }
        tx.execute(
            "UPDATE placement SET acked_at_ms = COALESCE(acked_at_ms, ?1) WHERE attempt_id = ?2",
            params![at, aid],
        )?;
        tx.execute(
            "UPDATE attempt SET observation = 'known' WHERE attempt_id = ?1",
            [&aid],
        )?;
        common::append_event(
            tx,
            run.as_deref(),
            Some(&aid),
            "worker_identity_retired",
            &json!({"worker": worker, "reason": "forgotten"}),
            at_ms,
        )?;
        close_collection_in(tx, &aid, None, Verdict::GaveUp, false, at_ms)?;
        ended.push(AttemptId::parse(&aid).map_err(|e| StorageError::Invalid(e.to_string()))?);
    }
    Ok(Forgotten { identity, ended })
}

impl CoordinatorStore {
    /// Record the first identity. Only explicit forget can release its name.
    pub fn admit_worker_identity(
        &self,
        worker: &WorkerId,
        boot: &WorkerBootId,
        at_ms: u64,
    ) -> Result<Admission> {
        let (worker, boot) = (worker.to_string(), boot.to_string());
        self.db.write(move |tx| {
            let admission = admit(
                recorded(tx, &worker)?.as_deref(),
                &boot,
                retired(tx, &worker, &boot)?,
            );
            if admission != Admission::First {
                return Ok(admission);
            }
            tx.execute(
                "INSERT INTO worker_identity (worker_id, boot_id, first_seen_at_ms)
                 VALUES (?1,?2,?3)",
                params![worker, boot, to_i64(at_ms)?],
            )?;
            Ok(admission)
        })
    }

    /// Whether `boot` was retired for `worker`.
    pub fn worker_identity_retired(&self, worker: &WorkerId, boot: &WorkerBootId) -> Result<bool> {
        let (worker, boot) = (worker.to_string(), boot.to_string());
        self.db.call(move |c| retired(c, &worker, &boot))
    }

    /// The identity recorded last for `worker`.
    pub fn worker_identity(&self, worker: &WorkerId) -> Result<Option<String>> {
        let worker = worker.to_string();
        self.db.call(move |c| recorded(c, &worker))
    }

    /// `workers forget`: the user declared the worker gone. Its identity is
    /// retired and every attempt it still holds ends, in one commit. Running
    /// it again changes nothing.
    pub fn forget_worker(&self, worker: &WorkerId, at_ms: u64) -> Result<Forgotten> {
        let worker = worker.to_string();
        self.db.write(move |tx| retire(tx, &worker, at_ms))
    }
}
