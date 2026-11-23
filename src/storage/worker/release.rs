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
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
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
    for table in [
        "command_launch",
        "lease",
        "log_spool",
        "blob_ref",
        "tombstone",
        "event",
    ] {
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

/// Refuse every submit of a member `belt group forget` removed here.
pub(crate) fn not_refused(conn: &Connection, owner: &str) -> Result<()> {
    let refused = conn
        .query_row(
            "SELECT 1 FROM refused_member WHERE coordinator_id = ?1",
            [owner],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if refused {
        return Err(StorageError::Invalid(
            "this worker forgot this member (belt group forget)".into(),
        ));
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

    /// The stored uptime sample ([`uptime_step`]).
    pub fn stored_uptime(&self) -> Result<Option<Uptime>> {
        self.db.call(|c| {
            Ok(c.query_row(
                "SELECT boot, mono, uptime_ms FROM uptime WHERE singleton = 1",
                [],
                |r| {
                    Ok(Uptime {
                        boot: r.get(0)?,
                        mono: to_u64(r.get(1)?).unwrap_or(0),
                        uptime_ms: to_u64(r.get(2)?).unwrap_or(0),
                    })
                },
            )
            .optional()?)
        })
    }

    /// Store `u`; with `mark`, also note the newest event at this uptime,
    /// which (e) prunes by. Marks a full `keep_ms` older than the newest
    /// one that is old enough to prune by go.
    pub fn store_uptime(&self, u: &Uptime, mark: bool) -> Result<()> {
        let u = u.clone();
        self.db.write(move |tx| {
            tx.execute(
                "INSERT INTO uptime VALUES (1, ?1, ?2, ?3)
                 ON CONFLICT(singleton) DO UPDATE SET boot = excluded.boot,
                     mono = excluded.mono, uptime_ms = excluded.uptime_ms",
                params![u.boot, to_i64(u.mono)?, to_i64(u.uptime_ms)?],
            )?;
            if mark {
                tx.execute(
                    "INSERT OR IGNORE INTO uptime_mark
                     SELECT ?1, COALESCE(MAX(seq), 0) FROM event",
                    [to_i64(u.uptime_ms)?],
                )?;
            }
            Ok(())
        })
    }

    /// The newest uptime mark, if any.
    pub fn last_uptime_mark(&self) -> Result<Option<u64>> {
        self.db.call(|c| {
            let v: Option<i64> =
                c.query_row("SELECT MAX(uptime_ms) FROM uptime_mark", [], |r| r.get(0))?;
            Ok(v.and_then(|v| to_u64(v).ok()))
        })
    }

    /// Terminal rows not yet stamped get `uptime_ms` (at most `batch`):
    /// their wait starts when the agent first sees them ended, never before.
    pub fn stamp_terminal_uptime(&self, uptime_ms: u64, batch: u32) -> Result<usize> {
        self.db.write(move |tx| {
            Ok(tx.execute(
                "UPDATE worker_attempt SET terminal_uptime_ms = ?1
                 WHERE (coordinator_id, attempt_id) IN (
                     SELECT coordinator_id, attempt_id FROM worker_attempt
                     WHERE state = 'terminal' AND terminal_uptime_ms IS NULL LIMIT ?2)",
                params![to_i64(uptime_ms)?, batch],
            )?)
        })
    }

    /// The ack wait (spec 25.6 (b)): up to `batch` terminal attempts that
    /// ended at least `ack_wait_ms` ago and got neither `ack_collected` nor
    /// `release_attempt` are deleted, each leaving an unreleased record.
    /// Selected by row: an older row whose directory is gone goes too.
    /// Returns the ids, whose directories are queued.
    pub fn expire_unreleased(
        &self,
        uptime_ms: u64,
        now_ms: u64,
        ack_wait_ms: u64,
        batch: u32,
    ) -> Result<Vec<String>> {
        // Judged by uptime only: no wall clock and no downtime counts.
        let Some(cutoff) = uptime_ms.checked_sub(ack_wait_ms) else {
            return Ok(Vec::new());
        };
        self.db.write(move |tx| {
            let rows: Vec<(String, String)> = {
                let mut s = tx.prepare(
                    "SELECT coordinator_id, attempt_id FROM worker_attempt
                     WHERE state = 'terminal' AND acked_at_ms IS NULL AND terminal_uptime_ms <= ?1
                     ORDER BY terminal_uptime_ms LIMIT ?2",
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

    /// Spec 25.6 (e): events of no attempt written before the newest uptime
    /// mark at least `keep_ms` of uptime old, the oldest first, at most
    /// `batch`; the marks before that one go. Returns how many events went.
    pub fn prune_unowned_events(&self, uptime_ms: u64, keep_ms: u64, batch: u32) -> Result<usize> {
        let Some(old) = uptime_ms.checked_sub(keep_ms) else {
            return Ok(0);
        };
        self.db.write(move |tx| {
            let mark: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT uptime_ms, event_seq FROM uptime_mark WHERE uptime_ms <= ?1
                     ORDER BY uptime_ms DESC LIMIT 1",
                    [to_i64(old)?],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            let Some((at, seq)) = mark else {
                return Ok(0);
            };
            let n = tx.execute(
                "DELETE FROM event WHERE seq IN (
                     SELECT seq FROM event WHERE attempt_id IS NULL AND seq <= ?1
                     ORDER BY seq LIMIT ?2)",
                params![seq, batch],
            )?;
            tx.execute("DELETE FROM uptime_mark WHERE uptime_ms < ?1", [at])?;
            Ok(n)
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

/// The worker's accumulated uptime (spec 25.6 (b)): `uptime_ms` at the
/// monotonic reading `mono` of boot `boot`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uptime {
    pub boot: String,
    pub mono: u64,
    pub uptime_ms: u64,
}

/// Stored at most once a minute of uptime (and at a new boot, at shutdown).
pub const UPTIME_WRITE_MS: u64 = 60_000;

/// The most one step adds: a stop of the agent or a sleep of the machine
/// between two samples counts at most this long (spec 25.6 (b)).
pub const UPTIME_STEP_MAX_MS: u64 = 2 * UPTIME_WRITE_MS;

/// The uptime now, from the stored sample, and whether to store the new
/// one. The same boot adds the monotonic time since the sample, at most
/// [`UPTIME_STEP_MAX_MS`]: the clock runs through an agent stop and a sleep,
/// which are not uptime. A step is made from the last *stored* sample, so
/// ticks between writes accumulate in full; a capped step is over a minute,
/// so it is always stored and the next one starts from its reading. A new
/// boot (or no sample) adds nothing, so downtime never counts and what was
/// not stored before a reboot (under a minute) only delays. The wall clock
/// is not an input. `None` when the clock failed (`u64::MAX`): no step.
pub fn uptime_step(stored: Option<&Uptime>, boot: &str, mono: u64) -> Option<(Uptime, bool)> {
    if mono == u64::MAX {
        return None;
    }
    Some(match stored {
        Some(s) if s.boot == boot => {
            let elapsed = mono.saturating_sub(s.mono);
            let counted = elapsed.min(UPTIME_STEP_MAX_MS);
            let u = Uptime {
                boot: boot.to_string(),
                mono: s.mono.max(mono),
                uptime_ms: s.uptime_ms.saturating_add(counted),
            };
            (u, elapsed >= UPTIME_WRITE_MS)
        }
        _ => (
            Uptime {
                boot: boot.to_string(),
                mono,
                uptime_ms: stored.map_or(0, |s| s.uptime_ms),
            },
            true,
        ),
    })
}

#[cfg(test)]
mod uptime_tests {
    use super::*;

    /// Downtime is not counted, and a machine up 12 hours a day reaches a
    /// 7-day wait after 14 days.
    #[test]
    fn uptime_counts_only_running_time_across_boots() {
        let h = 3_600_000;
        let mut stored: Option<Uptime> = None;
        for day in 0..14u64 {
            let boot = format!("boot-{day}");
            // Boot at an arbitrary monotonic origin; tick each minute.
            let origin = day * 7;
            let mut last = None;
            for minute in 0..=(12 * 60) {
                let (u, write) =
                    uptime_step(stored.as_ref(), &boot, origin + minute * 60_000).unwrap();
                if write {
                    stored = Some(u.clone());
                }
                last = Some(u);
            }
            assert_eq!(last.unwrap().uptime_ms, (day + 1) * 12 * h);
        }
        assert_eq!(stored.unwrap().uptime_ms, 14 * 12 * h);
    }

    #[test]
    fn a_tick_within_the_minute_is_not_stored() {
        let (s, w) = uptime_step(None, "b", 10).unwrap();
        assert!(w);
        assert_eq!(s.uptime_ms, 0);
        let (u, w) = uptime_step(Some(&s), "b", 59_000).unwrap();
        assert!(!w);
        assert_eq!(u.uptime_ms, 58_990);
        assert!(uptime_step(Some(&s), "b", 60_010).unwrap().1);
        // A new boot carries the uptime and adds nothing.
        let (r, w) = uptime_step(Some(&u), "c", 5).unwrap();
        assert!(w);
        assert_eq!(r.uptime_ms, 58_990);
    }

    #[test]
    fn a_stop_or_a_sleep_adds_at_most_two_minutes() {
        let hour = 3_600_000;
        let (s, _) = uptime_step(None, "b", 1_000).unwrap();
        // One step over the 7-day wait plus an hour: capped, and stored.
        let (u, write) = uptime_step(Some(&s), "b", 1_000 + 7 * 24 * hour + hour).unwrap();
        assert_eq!(u.uptime_ms, UPTIME_STEP_MAX_MS);
        assert!(write);
        // The next ticks start from the capped step's reading and add in full.
        let (v, write) = uptime_step(Some(&u), "b", u.mono + 30_000).unwrap();
        assert_eq!(v.uptime_ms, UPTIME_STEP_MAX_MS + 30_000);
        assert!(!write);
        // A gap of exactly the cap is counted in full.
        let (w, _) = uptime_step(Some(&u), "b", u.mono + UPTIME_STEP_MAX_MS).unwrap();
        assert_eq!(w.uptime_ms, 2 * UPTIME_STEP_MAX_MS);
    }

    #[test]
    fn a_failed_clock_makes_no_step() {
        let (s, _) = uptime_step(None, "b", 1_000).unwrap();
        assert!(uptime_step(Some(&s), "b", u64::MAX).is_none());
        assert!(uptime_step(None, "b", u64::MAX).is_none());
    }
}
