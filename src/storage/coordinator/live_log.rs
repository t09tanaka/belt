//! Durable committed prefixes, separate from the final collection receipt.
//! The collector must hold its collecting guard over recovery, append and commit;
//! a DB fence cannot fence filesystem writes by itself.
use super::{
    attempt_from_row, collect::add_ref, load_attempt, not_found, CoordinatorStore, ResultRef,
    ATTEMPT_COLS,
};
use crate::ids::{AttemptId, WorkerId};
use crate::results::store::{ResultStore, VerifiedLogPrefix};
use crate::storage::{common::BlobKind, to_i64, to_u64, Result, StorageError};
use crate::wire::LogStream;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogProgress {
    pub committed_bytes: u64,
    pub eof: bool,
    pub log_truncated: bool,
    pub dropped_bytes: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LiveLogTarget {
    pub record: super::AttemptRecord,
    /// The persisted payload; the collector resolves its source node from this.
    pub payload: Value,
}

fn stream_key(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn load(conn: &Connection, attempt: &str, stream: &str) -> Result<Option<LogProgress>> {
    conn.query_row(
        "SELECT committed_bytes,eof,log_truncated,dropped_bytes,updated_at_ms
         FROM collection_log WHERE attempt_id=?1 AND stream=?2",
        params![attempt, stream],
        |r| {
            Ok((|| {
                Ok(LogProgress {
                    committed_bytes: to_u64(r.get(0)?)?,
                    eof: r.get(1)?,
                    log_truncated: r.get(2)?,
                    dropped_bytes: to_u64(r.get(3)?)?,
                    updated_at_ms: to_u64(r.get(4)?)?,
                })
            })())
        },
    )
    .optional()?
    .transpose()
}

fn prepare(
    tx: &rusqlite::Transaction<'_>,
    attempt: &str,
    stream: &str,
    at: u64,
) -> Result<LogProgress> {
    let record =
        load_attempt(tx, attempt)?.ok_or_else(|| not_found(format!("attempt {attempt}")))?;
    add_ref(
        tx,
        attempt,
        &ResultRef {
            ref_id: format!("{attempt}:logs"),
            kind: BlobKind::Log,
            locator: ResultStore::logs_locator(&record.attempt_id),
        },
        at,
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO collection_log(attempt_id,stream,updated_at_ms) VALUES(?1,?2,?3)",
        params![attempt, stream, to_i64(at)?],
    )?;
    load(tx, attempt, stream)?.ok_or_else(|| not_found("log progress"))
}

impl CoordinatorStore {
    /// Bounded keyset page of current, accepted, nonretired attempts with a payload.
    /// An empty page means wrap the caller's cursor, not that a run is complete.
    pub fn query_live_log_targets(
        &self,
        worker: &WorkerId,
        after: Option<&AttemptId>,
        limit: u32,
    ) -> Result<Vec<LiveLogTarget>> {
        let (worker, after) = (worker.to_string(), after.map(ToString::to_string));
        let boundary = if self.retry_boundaries {
            "AND generation >= (SELECT min_generation FROM task WHERE task_id=attempt.task_id)"
        } else {
            ""
        };
        let open = if self.collections {
            "AND NOT EXISTS(SELECT 1 FROM collection c WHERE c.attempt_id=attempt.attempt_id AND c.closed_at_ms IS NOT NULL)"
        } else {
            ""
        };
        self.db.call(move |c| {
            let mut stmt = c.prepare(&format!(
                "SELECT {ATTEMPT_COLS},
                    (SELECT json_extract(demand_json,'$.payload') FROM placement p
                     WHERE p.attempt_id=attempt.attempt_id ORDER BY placement_id LIMIT 1)
                 FROM attempt
                 WHERE worker_id=?1 AND retired=0 AND (?2 IS NULL OR attempt_id>?2)
                   AND generation=(SELECT MAX(a.generation) FROM attempt a
                       WHERE a.task_id=attempt.task_id AND a.retired=0)
                   {boundary} {open}
                   AND json_extract(receipts_json,'$.worker_accepted_at_ms') IS NOT NULL
                   AND EXISTS(SELECT 1 FROM placement p WHERE p.attempt_id=attempt.attempt_id
                       AND json_type(p.demand_json,'$.payload')='object')
                 ORDER BY attempt_id LIMIT ?3"
            ))?;
            let rows = stmt.query_map(params![worker, after, limit.min(64)], |r| {
                Ok((attempt_from_row(r), r.get::<_, String>(17)?))
            })?;
            rows.map(|r| {
                let (record, payload) = r?;
                Ok(LiveLogTarget {
                    record: record?,
                    payload: crate::storage::common::from_json("payload", &payload)?,
                })
            })
            .collect()
        })
    }

    pub fn log_progress(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
    ) -> Result<Option<LogProgress>> {
        let attempt = attempt.to_string();
        self.db.call(move |c| load(c, &attempt, stream_key(stream)))
    }

    /// Read only bounded scalar metadata, including final receipts from older
    /// schemas. Never deserialize the potentially large collection logs_json.
    pub fn query_log_progress(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
    ) -> Result<Option<LogProgress>> {
        let id = attempt.clone();
        let attempt = attempt.to_string();
        let collections = self.collections;
        let progress = self.db.call(move |c| {
            let live: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='collection_log')", [], |r| r.get(0))?;
            if live {
                if let Some(progress) = load(c, &attempt, stream_key(stream))? {
                    return Ok(Some(progress));
                }
            }
            if !collections { return Ok(None); }
            // A normal two-stream final receipt is well below this bound.
            // Check byte length before JSON functions can allocate a parse tree
            // for a corrupt or unexpectedly huge legacy receipt.
            let oversized: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM collection WHERE attempt_id=?1 AND octet_length(logs_json)>4096)", [&attempt], |r|r.get(0))?;
            if oversized {return Err(StorageError::Corrupt("oversized legacy log receipt".into()));}
            let path = format!("$.{}", stream_key(stream));
            let row = c.query_row(
                "SELECT json_extract(logs_json,?2||'.bytes'),json_extract(logs_json,?2||'.complete'),json_extract(logs_json,?2||'.log_truncated'),json_extract(logs_json,?2||'.dropped_bytes') FROM collection WHERE attempt_id=?1 AND logs_json IS NOT NULL",
                params![attempt,path], |r| Ok((r.get::<_,Option<i64>>(0)?,r.get::<_,Option<bool>>(1)?,r.get::<_,Option<bool>>(2)?,r.get::<_,Option<i64>>(3)?))
            ).optional()?;
            match row {
                Some((Some(bytes),Some(eof),Some(truncated),Some(dropped))) => Ok(Some(LogProgress{committed_bytes:to_u64(bytes)?,eof,log_truncated:truncated,dropped_bytes:to_u64(dropped)?,updated_at_ms:0})),
                None | Some((None,None,None,None)) => Ok(None),
                _ => Err(StorageError::Corrupt("invalid final log metadata".into())),
            }
        })?;
        if progress.is_some() {
            self.result_data_available(
                &id,
                &crate::results::store::ResultStore::logs_locator(&id),
            )?;
        }
        Ok(progress)
    }

    /// Atomically register the GC reference BEFORE any filesystem creation.
    /// No collection receipt is created, closed or reset by this call.
    pub fn prepare_log_progress(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
        at: u64,
    ) -> Result<LogProgress> {
        let attempt = attempt.to_string();
        self.db.write(move |tx| {
            super::refuse_pruned_attempt(tx, &attempt)?;
            prepare(tx, &attempt, stream_key(stream), at)
        })
    }

    /// Call only after append/fsync. False means a stale offset; no bytes were
    /// published. Exact duplicate receipts are idempotent. Stream facts cannot
    /// regress, including on equal-offset metadata updates.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_log_progress(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
        expected: u64,
        next: u64,
        eof: bool,
        truncated: bool,
        dropped: u64,
        at: u64,
    ) -> Result<bool> {
        let attempt = attempt.to_string();
        self.db.write(move |tx| {
            super::refuse_pruned_attempt(tx, &attempt)?;
            let old = load(tx, &attempt, stream_key(stream))?
                .ok_or_else(|| not_found("prepared log progress"))?;
            if next < expected
                || dropped < old.dropped_bytes
                || (old.eof && (!eof || next != old.committed_bytes))
                || (old.log_truncated && !truncated)
            {
                return Err(StorageError::Immutable("log progress regression".into()));
            }
            if old.committed_bytes == next
                && old.eof == eof
                && old.log_truncated == truncated
                && old.dropped_bytes == dropped
            {
                return Ok(true);
            }
            if old.committed_bytes != expected {
                return Ok(false);
            }
            // Refuse resurrection if GC queued the locator between passes.
            prepare(tx, &attempt, stream_key(stream), at)?;
            tx.execute(
                "UPDATE collection_log SET committed_bytes=?1,eof=?2,log_truncated=?3,
                    dropped_bytes=?4,updated_at_ms=?5
                 WHERE attempt_id=?6 AND stream=?7 AND committed_bytes=?8",
                params![
                    to_i64(next)?,
                    eof,
                    truncated,
                    to_i64(dropped)?,
                    to_i64(at)?,
                    attempt,
                    stream_key(stream),
                    to_i64(expected)?
                ],
            )?;
            Ok(true)
        })
    }

    /// Bootstrap only a filesystem-verified FINAL legacy receipt. The token
    /// binds attempt, stream, digest and size; arbitrary physical lengths never
    /// become committed offsets. Existing progress always wins.
    pub fn bootstrap_log_progress(
        &self,
        verified: VerifiedLogPrefix,
        at: u64,
    ) -> Result<Option<LogProgress>> {
        self.db.write(move |tx| {
            let a = verified.attempt().as_str();
            super::refuse_pruned_attempt(tx, a)?;
            let stream = stream_key(verified.stream());
            if let Some(old) = load(tx, a, stream)? {
                return Ok(Some(old));
            }
            let logs: Option<String> = tx
                .query_row(
                    "SELECT logs_json FROM collection WHERE attempt_id=?1",
                    [a],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let Some(logs) = logs else {
                return Ok(None);
            };
            let logs: Value = crate::storage::common::from_json("logs receipt", &logs)?;
            let receipt = &logs[stream];
            if receipt["sha256"].as_str() != Some(verified.digest().as_str())
                || receipt["bytes"].as_u64() != Some(verified.bytes())
            {
                return Ok(None);
            }
            let (Some(eof), Some(truncated), Some(dropped)) = (
                receipt["complete"].as_bool(),
                receipt["log_truncated"].as_bool(),
                receipt["dropped_bytes"].as_u64(),
            ) else {
                return Ok(None);
            };
            prepare(tx, a, stream, at)?;
            tx.execute(
                "UPDATE collection_log SET committed_bytes=?1,eof=?2,log_truncated=?3,
                    dropped_bytes=?4 WHERE attempt_id=?5 AND stream=?6",
                params![
                    to_i64(verified.bytes())?,
                    eof,
                    truncated,
                    to_i64(dropped)?,
                    a,
                    stream
                ],
            )?;
            load(tx, a, stream)
        })
    }
}
