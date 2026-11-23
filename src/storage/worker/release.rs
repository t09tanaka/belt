//! Stage 3 of the retention (spec 25.6): an attempt is deleted as soon as
//! its member says it reads no more of it (`ack_collected`,
//! `release_attempt`), or after the ack wait, and a small dedup record
//! answers a late submit of it in its place.
//!
//! Every deletion of an attempt is [`drop_attempt`]: the row, its children,
//! its events and its tombstone go in one transaction, which also writes the
//! record (or not, for the rows an older version acknowledged) and queues the
//! id in `dropped_attempt`; the agent then removes the directory and the
//! supervisor lock and forgets the id ([`crate::worker::agent`]).

use super::{owner_in, under_epoch, WorkerStore};
use crate::ids::{AttemptId, CoordinatorEpoch, CoordinatorId};
use crate::state::Termination;
use crate::storage::{common::from_json, to_i64, to_u64, Result, StorageError};
use rusqlite::{params, Connection, OptionalExtension};

/// Dedup records a member may hold on one worker, released and unreleased
/// together (spec 25.6 (c)). The value of the undelivered tombstones' bound.
pub const MAX_ATTEMPT_RECORDS: u64 = 10_000;

/// Rows one transaction of a startup or tick pass handles (spec 25.11).
pub const RELEASE_BATCH: u32 = 1_000;

/// `list_unreleased` answers at most this many ids per page.
pub const MAX_UNRELEASED_PAGE: u32 = 500;

/// `refused_member` holds at most this many rows (spec 25.6 (d)).
pub const MAX_REFUSED_MEMBERS: u64 = 1_024;

/// What [`drop_attempt`] leaves in place of the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leave {
    /// A released record: the member reads no more of the attempt. It goes
    /// once every submit read before the release has been handled.
    Released,
    /// An unreleased record: nobody released the attempt within the ack
    /// wait. It stays until the member releases it.
    Unreleased,
    /// Nothing: a row an older version acknowledged (spec 25.6 (c2)).
    Nothing,
}

/// What `release_attempt` found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Released {
    /// The attempt was deleted now; its directory is queued.
    Dropped,
    /// Only a record was left of it; it is a released record now.
    Record,
}

/// What a dedup record says about an attempt that is gone.
#[derive(Debug, Clone, PartialEq)]
pub struct DedupRecord {
    pub attempt_id: AttemptId,
    pub run_id: String,
    pub task_id: String,
    pub generation: u64,
    pub termination: Option<Termination>,
    pub released: bool,
}

/// Delete the attempt `aid` of `owner` in the caller's transaction and leave
/// `leave` in its place. The files its references named and no other
/// reference names are queued for deletion; the id goes to
/// `dropped_attempt`, whose follow-up removes the directory and the lock.
/// Returns false when there is no such row.
pub(crate) fn drop_attempt(
    tx: &Connection,
    owner: &str,
    aid: &str,
    leave: Leave,
    at_ms: u64,
) -> Result<bool> {
    let row: Option<(String, String, i64, String, i64, Option<String>)> = tx
        .query_row(
            "SELECT run_id, task_id, generation, payload_hash, coordinator_epoch, termination_json
             FROM worker_attempt WHERE coordinator_id = ?1 AND attempt_id = ?2",
            [owner, aid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .optional()?;
    let Some((run, task, generation, hash, epoch, termination)) = row else {
        return Ok(false);
    };
    let locators: Vec<String> = {
        let mut s = tx.prepare(
            "SELECT DISTINCT locator FROM blob_ref WHERE coordinator_id = ?1 AND attempt_id = ?2",
        )?;
        let rows = s.query_map([owner, aid], |r| r.get(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    for table in ["command_launch", "lease", "log_spool", "blob_ref", "tombstone", "event"] {
        tx.execute(
            &format!("DELETE FROM {table} WHERE coordinator_id = ?1 AND attempt_id = ?2"),
            [owner, aid],
        )?;
    }
    for locator in locators {
        let left: i64 = tx.query_row(
            "SELECT count(*) FROM blob_ref WHERE locator = ?1",
            [&locator],
            |r| r.get(0),
        )?;
        if left == 0 {
            tx.execute(
                "INSERT OR IGNORE INTO pending_delete (locator, queued_at_ms) VALUES (?1, ?2)",
                params![locator, to_i64(at_ms)?],
            )?;
        }
    }
    tx.execute(
        "DELETE FROM worker_attempt WHERE coordinator_id = ?1 AND attempt_id = ?2",
        [owner, aid],
    )?;
    let released = match leave {
        Leave::Released => Some(1),
        Leave::Unreleased => Some(0),
        Leave::Nothing => None,
    };
    if let Some(released) = released {
        // An unreleased record that is released now becomes a released one;
        // a released one never goes back.
        tx.execute(
            "INSERT INTO attempt_record (coordinator_id, attempt_id, run_id, task_id, generation,
                payload_hash, coordinator_epoch, termination_json, released, recorded_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT (coordinator_id, attempt_id) DO UPDATE SET released = max(released, ?9)",
            params![
                owner,
                aid,
                run,
                task,
                generation,
                hash,
                epoch,
                termination,
                released,
                to_i64(at_ms)?
            ],
        )?;
    }
    tx.execute(
        "INSERT OR IGNORE INTO dropped_attempt (attempt_id) VALUES (?1)",
        [aid],
    )?;
    Ok(true)
}

/// Whether `owner` may get one more record: refused with
/// [`StorageError::AttemptRecords`] at [`MAX_ATTEMPT_RECORDS`].
pub(crate) fn record_room(conn: &Connection, owner: &str) -> Result<()> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM attempt_record WHERE coordinator_id = ?1",
        [owner],
        |r| r.get(0),
    )?;
    let n = to_u64(n)?;
    if n >= MAX_ATTEMPT_RECORDS {
        return Err(StorageError::AttemptRecords(n));
    }
    Ok(())
}

/// Whether a record of `aid` exists for `owner`.
pub(crate) fn recorded(conn: &Connection, owner: &str, aid: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM attempt_record WHERE coordinator_id = ?1 AND attempt_id = ?2",
            [owner, aid],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

impl WorkerStore {
    /// `release_attempt` (spec 25.6 (a)): delete a terminal attempt and leave
    /// a released record, or release the record that is all that is left of
    /// it. An attempt that is not terminal is refused with
    /// [`StorageError::Overloaded`] (send again later) and nothing changes;
    /// an id with neither row nor record is [`StorageError::NotFound`].
    pub fn release_attempt(
        &self,
        attempt_id: &AttemptId,
        epoch: CoordinatorEpoch,
        at_ms: u64,
    ) -> Result<Released> {
        let a = attempt_id.to_string();
        under_epoch(&self.db, &self.scope, attempt_id, epoch, move |tx, o| {
            let state: Option<String> = tx
                .query_row(
                    "SELECT state FROM worker_attempt WHERE coordinator_id = ?1 AND attempt_id = ?2",
                    [o, a.as_str()],
                    |r| r.get(0),
                )
                .optional()?;
            match state.as_deref() {
                Some("terminal") => {
                    drop_attempt(tx, o, &a, Leave::Released, at_ms)?;
                    Ok(Released::Dropped)
                }
                Some(_) => Err(StorageError::Overloaded(format!(
                    "attempt {a} is not terminal; it stops by its lease or a retire, release it again later"
                ))),
                None => {
                    let n = tx.execute(
                        "UPDATE attempt_record SET released = 1
                         WHERE coordinator_id = ?1 AND attempt_id = ?2",
                        [o, a.as_str()],
                    )?;
                    if n == 1 {
                        Ok(Released::Record)
                    } else {
                        Err(StorageError::NotFound {
                            what: format!("attempt {a}"),
                        })
                    }
                }
            }
        })
    }

    /// The dedup record of an attempt of this handle's member, if any.
    pub fn attempt_record(&self, attempt_id: &AttemptId) -> Result<Option<DedupRecord>> {
        let (scope, a) = (self.scope.clone(), attempt_id.to_string());
        self.db.call(move |c| {
            let o = owner_in(c, &scope, Some(&a))?;
            let row: Option<(String, String, i64, Option<String>, i64)> = c
                .query_row(
                    "SELECT run_id, task_id, generation, termination_json, released
                     FROM attempt_record WHERE coordinator_id = ?1 AND attempt_id = ?2",
                    [&o, &a],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
                .optional()?;
            let Some((run_id, task_id, generation, termination, released)) = row else {
                return Ok(None);
            };
            Ok(Some(DedupRecord {
                attempt_id: AttemptId::parse(&a)
                    .map_err(|e| StorageError::Corrupt(format!("attempt id: {e}")))?,
                run_id,
                task_id,
                generation: to_u64(generation)?,
                termination: termination
                    .map(|t| from_json("termination", &t))
                    .transpose()?,
                released: released == 1,
            }))
        })
    }

    /// One page of the terminal attempts of this handle's member that are
    /// neither acknowledged nor released: the rows and the unreleased
    /// records, in attempt id order after `after`, at most `limit`
    /// ([`MAX_UNRELEASED_PAGE`]). The flag says whether more follow.
    pub fn list_unreleased(
        &self,
        after: Option<&AttemptId>,
        limit: u32,
    ) -> Result<(Vec<AttemptId>, bool)> {
        let Some(member) = self.scope.clone() else {
            return Err(StorageError::Invalid(
                "list_unreleased is a member's question".into(),
            ));
        };
        let after = after.map(|a| a.to_string()).unwrap_or_default();
        let limit = limit.clamp(1, MAX_UNRELEASED_PAGE);
        self.db.call(move |c| {
            let take = i64::from(limit) + 1;
            let mut ids: Vec<String> = Vec::new();
            for sql in [
                "SELECT attempt_id FROM worker_attempt
                 WHERE coordinator_id = ?1 AND attempt_id > ?2 AND state = 'terminal'
                   AND acked_at_ms IS NULL
                 ORDER BY attempt_id LIMIT ?3",
                "SELECT attempt_id FROM attempt_record
                 WHERE coordinator_id = ?1 AND attempt_id > ?2 AND released = 0
                 ORDER BY attempt_id LIMIT ?3",
            ] {
                let mut s = c.prepare(sql)?;
                let rows = s.query_map(params![member, after, take], |r| r.get(0))?;
                for row in rows {
                    ids.push(row?);
                }
            }
            ids.sort();
            ids.dedup();
            let more = ids.len() > limit as usize;
            ids.truncate(limit as usize);
            let ids = ids
                .iter()
                .map(|a| {
                    AttemptId::parse(a)
                        .map_err(|e| StorageError::Corrupt(format!("attempt id: {e}")))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((ids, more))
        })
    }

    /// The ack wait (spec 25.6 (b)): up to `batch` terminal attempts that
    /// ended at least `ack_wait_ms` ago and got neither `ack_collected` nor
    /// `release_attempt` are deleted, each leaving an unreleased record.
    /// Selected by row: an older row whose directory is gone goes too.
    /// Returns the ids, whose directories are queued.
    pub fn expire_unreleased(
        &self,
        now_ms: u64,
        ack_wait_ms: u64,
        batch: u32,
    ) -> Result<Vec<String>> {
        let cutoff = now_ms.saturating_sub(ack_wait_ms);
        self.db.write(move |tx| {
            let rows: Vec<(String, String)> = {
                let mut s = tx.prepare(
                    "SELECT coordinator_id, attempt_id FROM worker_attempt
                     WHERE state = 'terminal' AND acked_at_ms IS NULL AND terminal_at_ms <= ?1
                     ORDER BY terminal_at_ms LIMIT ?2",
                )?;
                let rows = s.query_map(params![to_i64(cutoff)?, batch], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for (owner, aid) in &rows {
                drop_attempt(tx, owner, aid, Leave::Unreleased, now_ms)?;
            }
            Ok(rows.into_iter().map(|(_, a)| a).collect())
        })
    }

    /// Spec 25.6 (c2): up to `batch` terminal rows that an older version
    /// acknowledged before `before_ms` (the start of this agent) are deleted
    /// as an acknowledgement is now, with no record. Never on trial.
    pub fn drop_acked_before(&self, before_ms: u64, batch: u32) -> Result<Vec<String>> {
        self.db.write(move |tx| {
            let rows: Vec<(String, String)> = {
                let mut s = tx.prepare(
                    "SELECT coordinator_id, attempt_id FROM worker_attempt
                     WHERE acked_at_ms IS NOT NULL AND acked_at_ms < ?1 AND state = 'terminal'
                     ORDER BY acked_at_ms LIMIT ?2",
                )?;
                let rows = s.query_map(params![to_i64(before_ms)?, batch], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for (owner, aid) in &rows {
                drop_attempt(tx, owner, aid, Leave::Nothing, before_ms)?;
            }
            Ok(rows.into_iter().map(|(_, a)| a).collect())
        })
    }

    /// At the start of the agent, before any connection: no submit is in
    /// flight, so every released record may go. One batch; returns how many
    /// went (call again while it is `batch`).
    pub fn purge_released_records(&self, batch: u32) -> Result<usize> {
        self.db.write(move |tx| {
            Ok(tx.execute(
                "DELETE FROM attempt_record WHERE (coordinator_id, attempt_id) IN (
                     SELECT coordinator_id, attempt_id FROM attempt_record
                     WHERE released = 1 LIMIT ?1)",
                [batch],
            )?)
        })
    }

    /// The released records whose release no submit can be behind any more
    /// (the connection tracker decided it). Unreleased ones are not touched.
    pub fn forget_released(&self, records: Vec<(CoordinatorId, AttemptId)>) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        self.db.write(move |tx| {
            let mut n = 0;
            for (member, attempt) in &records {
                n += tx.execute(
                    "DELETE FROM attempt_record
                     WHERE coordinator_id = ?1 AND attempt_id = ?2 AND released = 1",
                    [member.as_str(), attempt.as_str()],
                )?;
            }
            Ok(n)
        })
    }

    /// Spec 25.6 (e): events of no attempt older than `before_ms`, the
    /// oldest first, at most `batch`. Returns how many went.
    pub fn prune_unowned_events(&self, before_ms: u64, batch: u32) -> Result<usize> {
        self.db.write(move |tx| {
            Ok(tx.execute(
                "DELETE FROM event WHERE seq IN (
                     SELECT seq FROM event WHERE attempt_id IS NULL AND at_ms < ?1
                     ORDER BY seq LIMIT ?2)",
                params![to_i64(before_ms)?, batch],
            )?)
        })
    }

    /// Whether `belt group forget` removed this coordinator id here.
    pub fn member_refused(&self, member: &CoordinatorId) -> Result<bool> {
        let m = member.to_string();
        self.db.call(move |c| {
            Ok(c.query_row(
                "SELECT 1 FROM refused_member WHERE coordinator_id = ?1",
                [&m],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
        })
    }

    /// `belt group forget <member>` on this worker (spec 25.6 (d)): every
    /// row of the member goes and its coordinator id is refused for good.
    /// Refused while the member is on this machine's list (`listed`), while
    /// it has attempts that are not terminal (the count is in the error),
    /// or at [`MAX_REFUSED_MEMBERS`]. Returns the member's attempt ids, whose
    /// directories are queued in `dropped_attempt`. A name this worker has
    /// never seen is [`StorageError::NotFound`].
    pub fn forget_member(&self, name: &str, at_ms: u64) -> Result<Vec<String>> {
        let name = name.to_string();
        self.db.write(move |tx| {
            let member: Option<String> = tx
                .query_row(
                    "SELECT coordinator_id FROM group_member WHERE name = ?1",
                    [&name],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(member) = member else {
                return Err(StorageError::NotFound {
                    what: format!("member {name} on this worker"),
                });
            };
            let live: i64 = tx.query_row(
                "SELECT count(*) FROM worker_attempt WHERE coordinator_id = ?1 AND state <> 'terminal'",
                [&member],
                |r| r.get(0),
            )?;
            if live > 0 {
                return Err(StorageError::Invalid(format!(
                    "member {name} has {live} attempts that are not terminal here; wait until their leases expire"
                )));
            }
            let refused: i64 =
                tx.query_row("SELECT count(*) FROM refused_member", [], |r| r.get(0))?;
            if to_u64(refused)? >= MAX_REFUSED_MEMBERS {
                return Err(StorageError::Invalid(format!(
                    "{refused} members are refused here already"
                )));
            }
            let attempts: Vec<String> = {
                let mut s =
                    tx.prepare("SELECT attempt_id FROM worker_attempt WHERE coordinator_id = ?1")?;
                let rows = s.query_map([&member], |r| r.get(0))?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            for a in &attempts {
                drop_attempt(tx, &member, a, Leave::Nothing, at_ms)?;
            }
            for table in [
                "attempt_record",
                "tombstone",
                "event",
                "member_epoch",
                "retired_identity",
                "group_member",
            ] {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE coordinator_id = ?1"),
                    [&member],
                )?;
            }
            tx.execute(
                "INSERT OR REPLACE INTO refused_member (coordinator_id, name, refused_at_ms)
                 VALUES (?1, ?2, ?3)",
                params![member, name, to_i64(at_ms)?],
            )?;
            Ok(attempts)
        })
    }
}
