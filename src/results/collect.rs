//! Collecting the outputs of ended attempts from their workers (spec
//! chapter 13).
//!
//! For each terminal attempt whose collection is not closed, one pass takes
//! the steps [`plan::next_step`] names, each from what is durable in the
//! parent: read the result and record it with its digest, read the logs to
//! their end mark, fetch every stored artifact the parent declared, verify
//! and publish it, and only then send `ack_collected` for the recorded
//! digest. The collection is closed (and the attempt `collected`) in the
//! commit that records the worker's acknowledgement.
//!
//! Nothing here runs a command. A pass that is cut off anywhere (the parent
//! crashed, the link is gone, the time is up) is continued by the next pass
//! from the same durable facts: the logs from the bytes that are here, an
//! artifact from the bytes of its `.part` file.
//!
//! The acknowledgement is the last step and is never early: it releases the
//! worker's copy for good. Right before it, everything the collection
//! recorded is read back from the result store (the result is written
//! again from its record, each log and each artifact is hashed against its
//! recorded length and digest) and synced; what is not there as recorded is
//! collected again instead.
//!
//! What goes wrong with one attempt stays with that attempt: the pass goes
//! on with the others. Only what asking again cannot change counts toward
//! giving up ([`plan::judge_fault`]); everything else waits, a little
//! longer each time.
//!
//! The acknowledgement is by digest. A worker that holds another result
//! than the one that was read (an attempt that was settled without a result
//! can get one later) refuses it; the result is then read again and
//! collected from its start, so a result that was read while incomplete is
//! never acknowledged.

use super::plan::{
    self, AckRefusal, ChunkFacts, ChunkVerdict, Fault, FaultVerdict, LogFacts, LogVerdict, LogWait,
    Mismatch, Progress, RecordFacts, Reread, Standing, Step, Verdict,
};
use super::store::{ArtifactState, Publish, ResultStore, StoreError};
use crate::coordinator::{LinkError, WorkerLink};
use crate::ids::{AttemptId, Digest, NodeId, RequestId, WorkerId};
use crate::platform::wall_ms;
use crate::state::{Receipt, Termination};
use crate::storage::{
    ArtifactReceipt, AttemptRecord, BlobKind, Collectable, CoordinatorStore, LogProgress,
    ResultRef, StorageError,
};
use crate::supervisor::spool_plan::relative_path;
use crate::wire::{
    payload_hash, AckCollectedRequest, ArtifactRecord, AttemptPayload, BlobGetRequest,
    GetResultRequest, GetResultResponse, LogStream, MissingPolicy, ReadLogRequest, Request,
    Response, SchemaVersion,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectConfig {
    /// The most bytes asked for with one `blob_get` or `read_log`.
    pub chunk_bytes: u32,
    /// An artifact above this is not fetched.
    pub max_artifact_bytes: u64,
    /// The most bytes of one log stream that are kept.
    pub max_log_bytes: u64,
    /// The most attempts of one worker one pass looks at.
    pub scan: u32,
    /// How long one pass may take per worker.
    pub budget_ms: u64,
    /// How long a collection whose worker answered with an error (or whose
    /// own files failed) waits before its next try; doubled with every wait
    /// in a row, up to 64 times this.
    pub backoff_ms: u64,
}

impl Default for CollectConfig {
    fn default() -> Self {
        CollectConfig {
            chunk_bytes: 262_144,
            max_artifact_bytes: 1_073_741_824,
            max_log_bytes: 1_073_741_824,
            scan: 256,
            budget_ms: 10_000,
            backoff_ms: 1_000,
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectReport {
    /// Closed with every required output here.
    pub collected: Vec<AttemptId>,
    /// Closed, and a required artifact does not exist.
    pub required_missing: Vec<AttemptId>,
    /// Given up.
    pub gave_up: Vec<AttemptId>,
    /// Still to be continued.
    pub pending: Vec<AttemptId>,
    /// Of the pending ones: what the parent itself could not do (read the
    /// payload it sent, write its files or its records). They wait, are
    /// never acknowledged as they are, and keep nothing else from being
    /// collected.
    pub faults: Vec<(AttemptId, String)>,
}

impl CollectReport {
    pub fn merge(&mut self, other: CollectReport) {
        self.collected.extend(other.collected);
        self.required_missing.extend(other.required_missing);
        self.gave_up.extend(other.gave_up);
        self.pending.extend(other.pending);
        self.faults.extend(other.faults);
    }

    fn closed(&mut self, attempt: &AttemptId, verdict: Verdict) {
        match verdict {
            Verdict::Complete => self.collected.push(attempt.clone()),
            Verdict::RequiredMissing => self.required_missing.push(attempt.clone()),
            Verdict::GaveUp => self.gave_up.push(attempt.clone()),
        }
    }
}

#[derive(Debug, Error)]
pub enum CollectError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Files(#[from] StoreError),
    /// A worker has seen a newer coordinator epoch.
    #[error("a newer coordinator took over")]
    Fenced,
    #[error("a blocking call was aborted")]
    Aborted,
}

type Result<T, E = CollectError> = std::result::Result<T, E>;

/// Where one pass left a collection.
enum End {
    Closed(Verdict),
    /// To be continued by the next pass.
    Pending,
    /// To be continued, after this went wrong.
    Waits(Fault, String),
}

/// Where one step left the collection.
enum Flow {
    /// A step was taken; look again.
    Again,
    Closed(Verdict),
    /// Nothing more can be done now.
    Later,
    Fault(Fault, String),
}

/// The artifacts the parent declared, and the node whose log is read.
struct Declared {
    artifacts: Vec<(String, bool)>,
    node: Option<NodeId>,
    payload: AttemptPayload,
}

/// What the parent declared is what it sent. Without it nothing is known
/// about what has to be fetched, and "nothing was declared" would release
/// the worker's artifacts unfetched: a payload that is not recorded or
/// cannot be read is a fault, never an empty declaration.
fn declared_of(payload: &Option<Value>) -> std::result::Result<Declared, &'static str> {
    let Some(payload) = payload else {
        return Err("the payload that was sent is not recorded");
    };
    let Ok(payload) = serde_json::from_value::<AttemptPayload>(payload.clone()) else {
        return Err("the payload that was sent cannot be read");
    };
    Ok(Declared {
        artifacts: payload
            .artifacts
            .iter()
            .map(|d| (d.name.clone(), d.if_missing == MissingPolicy::Required))
            .collect(),
        node: payload.commands.first().map(|c| c.node_id.clone()),
        payload,
    })
}

impl Declared {
    fn names(&self, result: &GetResultResponse) -> Vec<(String, bool)> {
        self.artifacts
            .iter()
            .cloned()
            .chain(
                self.payload
                    .outputs
                    .iter()
                    .filter(|d| {
                        crate::outputs::eligible(
                            d.when,
                            result.termination.outcome(),
                            result.outputs_evaluated,
                        )
                    })
                    .map(|d| (d.name.clone(), d.if_missing == MissingPolicy::Required)),
            )
            .collect()
    }
}

/// The collections that wait before their next try, and until when. In
/// memory: a parent that starts again tries each once.
#[derive(Debug, Default)]
pub struct Waits(Mutex<BTreeMap<AttemptId, (u32, Instant)>>);

impl Waits {
    fn map(&self) -> std::sync::MutexGuard<'_, BTreeMap<AttemptId, (u32, Instant)>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn waiting(&self, attempt: &AttemptId) -> bool {
        self.map()
            .get(attempt)
            .is_some_and(|(_, until)| Instant::now() < *until)
    }

    fn put_off(&self, attempt: &AttemptId, base_ms: u64) {
        let mut map = self.map();
        let entry = map.entry(attempt.clone()).or_insert((0, Instant::now()));
        entry.1 = Instant::now() + Duration::from_millis(plan::backoff_ms(base_ms, entry.0));
        entry.0 = entry.0.saturating_add(1);
    }

    /// Live faults can leave selection through retirement/termination without
    /// another successful poll. Bound this retry hint to one full live sweep.
    /// Eviction only permits an earlier retry; durable offsets remain the truth.
    fn put_off_live(&self, attempt: &AttemptId, base_ms: u64) {
        self.put_off(attempt, base_ms);
        let mut map = self.map();
        while map.len() > 8 * 64 {
            map.pop_first();
        }
    }

    fn clear(&self, attempt: &AttemptId) {
        self.map().remove(attempt);
    }
}

/// The result of an attempt that has none stored: its termination only.
fn bare_result(termination: &Termination) -> Value {
    json!({ "termination": termination, "artifacts": [] })
}

fn fault_of(e: LinkError) -> Fault {
    match e {
        LinkError::Remote(e) => Fault::Remote(e.code),
        LinkError::Unconfirmed => Fault::Unreachable,
    }
}

/// What the result says about each declared artifact.
fn verdict_of(declared: &[(String, bool)], result: &GetResultResponse) -> Verdict {
    let standings: Vec<(bool, Standing)> = declared
        .iter()
        .map(|(name, required)| {
            let stored = result.artifacts.iter().any(|a| &a.name == name);
            let not_stored = result
                .artifacts_not_stored
                .iter()
                .find(|n| &n.name == name)
                .map(|n| n.reason);
            (*required, plan::standing(stored, not_stored))
        })
        .collect();
    plan::judge_artifacts(&standings)
}

/// The stored artifacts of a result that the parent declared, in the order
/// of the result. What a worker lists beyond the declaration is not fetched.
fn wanted<'a>(
    declared: &[(String, bool)],
    result: &'a GetResultResponse,
) -> Vec<&'a ArtifactRecord> {
    result
        .artifacts
        .iter()
        .filter(|a| declared.iter().any(|(name, _)| name == &a.name))
        .collect()
}

/// One pass over the collections of one worker.
pub struct Collector {
    store: Arc<CoordinatorStore>,
    files: Arc<ResultStore>,
    cfg: CollectConfig,
    waits: Arc<Waits>,
    live_waits: Arc<Waits>,
    stop: Option<tokio::sync::watch::Receiver<bool>>,
    collecting: Option<Arc<tokio::sync::OwnedMutexGuard<()>>>,
}

enum LogRead {
    Stopped,
    Progress(LogProgress),
    Wait(LogProgress),
    Fault(Fault, String),
}

impl Collector {
    pub fn new(
        store: Arc<CoordinatorStore>,
        files: Arc<ResultStore>,
        cfg: CollectConfig,
    ) -> Collector {
        Collector {
            store,
            files,
            cfg,
            waits: Arc::default(),
            live_waits: Arc::default(),
            stop: None,
            collecting: None,
        }
    }

    /// Use the waits of the passes before this one.
    pub fn with_waits(mut self, waits: Arc<Waits>) -> Collector {
        self.waits = waits;
        self
    }

    pub(crate) fn with_live_waits(mut self, waits: Arc<Waits>) -> Self {
        self.live_waits = waits;
        self
    }

    pub(crate) fn with_collecting(
        mut self,
        collecting: Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Self {
        self.collecting = Some(collecting);
        self
    }

    pub(crate) fn with_stop(mut self, stop: Option<tokio::sync::watch::Receiver<bool>>) -> Self {
        self.stop = stop;
        self
    }

    fn stopping(&self) -> bool {
        self.stop.as_ref().is_some_and(|stop| *stop.borrow())
    }

    pub fn config(&self) -> CollectConfig {
        self.cfg
    }

    async fn db<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&CoordinatorStore) -> crate::storage::Result<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let collecting = self.collecting.clone();
        match tokio::task::spawn_blocking(move || {
            // Retain the same writer guard through a commit even if its async
            // waiter is dropped before this blocking job is scheduled.
            let out = f(&store);
            drop(collecting);
            out
        })
        .await
        {
            Ok(out) => Ok(out?),
            Err(_) => Err(CollectError::Aborted),
        }
    }

    async fn fs<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&ResultStore) -> std::result::Result<T, StoreError> + Send + 'static,
    {
        let files = Arc::clone(&self.files);
        let owner = Arc::clone(&self.store);
        let collecting = self.collecting.clone();
        match tokio::task::spawn_blocking(move || {
            // A dropped async caller cannot cancel a blocking file mutation.
            // Retain the process owner lock until that mutation has drained;
            // an interrupted DB commit leaves only a recoverable private tail.
            let out = f(&files);
            drop(collecting);
            drop(owner);
            out
        })
        .await
        {
            Ok(out) => Ok(out?),
            Err(_) => Err(CollectError::Aborted),
        }
    }

    /// Continue every open collection of `worker` until it is closed, has
    /// to wait, or `deadline` passed. `link` is `None` for a worker that is
    /// not connected: only what needs no worker is closed then. What fails
    /// for one attempt is reported for it and the others are continued; the
    /// pass itself fails only when a newer coordinator took over or the
    /// open collections cannot be listed.
    pub async fn collect_worker(
        &self,
        worker: &WorkerId,
        link: Option<&dyn WorkerLink>,
        deadline: Instant,
    ) -> Result<CollectReport> {
        let (w, scan) = (worker.clone(), self.cfg.scan);
        let open = self.db(move |s| s.collectable(Some(&w), scan)).await?;
        let mut report = CollectReport::default();
        for c in open {
            let a = c.record.attempt_id.clone();
            if self.waits.waiting(&a) {
                report.pending.push(a);
                continue;
            }
            let (fault, why) = match self.one(link, &c, deadline).await {
                Ok(End::Closed(verdict)) => {
                    self.waits.clear(&a);
                    report.closed(&a, verdict);
                    continue;
                }
                Ok(End::Pending) => {
                    report.pending.push(a);
                    continue;
                }
                Ok(End::Waits(fault, why)) => (fault, why),
                Err(e @ (CollectError::Fenced | CollectError::Aborted)) => return Err(e),
                Err(e) => (Fault::Local, e.to_string()),
            };
            if plan::backs_off(fault) {
                self.waits.put_off(&a, self.cfg.backoff_ms);
            }
            if fault == Fault::Local {
                report.faults.push((a.clone(), why));
            }
            report.pending.push(a);
        }
        Ok(report)
    }

    /// Work on one collection. An `Err` is about this attempt only, unless
    /// it is `Fenced` or `Aborted`.
    async fn one(
        &self,
        link: Option<&dyn WorkerLink>,
        c: &Collectable,
        deadline: Instant,
    ) -> Result<End> {
        let a = c.record.attempt_id.clone();
        let declared = match declared_of(&c.payload) {
            Ok(declared) => declared,
            Err(why) => return Ok(End::Waits(Fault::Local, why.to_string())),
        };
        if c.settled_here || !c.record.receipts.reached(Receipt::WorkerAccepted) {
            // No worker holds a result of this attempt: the one the parent
            // fixed is all there is.
            let verdict = self.close_local(&c.record, &declared).await?;
            return Ok(End::Closed(verdict));
        }
        let Some(link) = link else {
            return Ok(End::Pending);
        };
        loop {
            if self.stopping() || Instant::now() >= deadline {
                return Ok(End::Pending);
            }
            let (fault, why) = match self.step(link, &c.record, &declared, deadline).await? {
                Flow::Again => {
                    self.waits.clear(&a);
                    continue;
                }
                Flow::Closed(verdict) => {
                    self.tidy(&a).await;
                    return Ok(End::Closed(verdict));
                }
                Flow::Later => return Ok(End::Pending),
                Flow::Fault(fault, why) => (fault, why),
            };
            return match plan::judge_fault(fault) {
                FaultVerdict::Later => Ok(End::Waits(fault, why)),
                FaultVerdict::Fenced => Err(CollectError::Fenced),
                FaultVerdict::Strike => {
                    let (id, text) = (a.clone(), why.clone());
                    let strikes = self
                        .db(move |s| s.strike_collection(&id, &text, wall_ms()))
                        .await?;
                    if !plan::gives_up(strikes) {
                        return Ok(End::Waits(fault, why));
                    }
                    let id = a.clone();
                    self.db(move |s| {
                        s.close_collection(&id, None, Verdict::GaveUp, false, wall_ms())
                    })
                    .await?;
                    self.tidy(&a).await;
                    Ok(End::Closed(Verdict::GaveUp))
                }
            };
        }
    }

    /// Remove what unfinished transfers of a closed collection left. What
    /// cannot be removed now goes with the files of the attempt.
    async fn tidy(&self, attempt: &AttemptId) {
        let a = attempt.clone();
        let _ = self.fs(move |f| f.clear_tmp(&a)).await;
    }

    /// Close the collection of an attempt no worker has a result of.
    async fn close_local(&self, rec: &AttemptRecord, declared: &Declared) -> Result<Verdict> {
        let Some(termination) = rec.termination else {
            return Err(StorageError::Corrupt(format!(
                "terminal attempt {} has no termination",
                rec.attempt_id
            ))
            .into());
        };
        let result = bare_result(&termination);
        let digest = payload_hash(&result)
            .map_err(|e| StorageError::Invalid(format!("result digest: {e}")))?;
        let standings: Vec<(bool, Standing)> = declared
            .artifacts
            .iter()
            .map(|(_, required)| (*required, Standing::Unreported))
            .collect();
        let verdict = plan::judge_artifacts(&standings);
        let (a, names) = (rec.attempt_id.clone(), declared.artifacts.clone());
        self.db(move |s| {
            let at = wall_ms();
            s.begin_collection(&a, &digest, &result, &names, &[], at)?;
            s.set_collection_logs(&a, &digest, &json!({}), at)?;
            s.close_collection(&a, Some(&digest), verdict, false, at)?;
            Ok(())
        })
        .await?;
        Ok(verdict)
    }

    async fn step(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
        deadline: Instant,
    ) -> Result<Flow> {
        let a = rec.attempt_id.clone();
        let (row, published) = self
            .db(move |s| Ok((s.collection(&a)?, s.persisted_artifacts(&a)?)))
            .await?;
        let closed = row.as_ref().and_then(|r| r.closed_at_ms.and(r.verdict));
        let recorded: Option<(Digest, Value)> = row
            .as_ref()
            .and_then(|r| Some((r.result_digest.clone()?, r.result.clone()?)));
        let current: Option<(Digest, Value, GetResultResponse)> =
            recorded.and_then(|(digest, value)| {
                let result = serde_json::from_value(value.clone()).ok()?;
                Some((digest, value, result))
            });
        let all: Vec<ArtifactRecord> = current
            .iter()
            .flat_map(|(_, _, result)| wanted(&declared.names(result), result))
            .cloned()
            .collect();
        // Published is what has this name and this digest. A name that is
        // published with other bytes than the result records now is not
        // fetched over it: an artifact never changes.
        let here = |w: &ArtifactRecord| {
            published
                .iter()
                .any(|(n, d)| n == &w.name && d == &w.digest)
        };
        let changed = all.iter().any(|w| {
            published
                .iter()
                .any(|(n, d)| n == &w.name && d != &w.digest)
        });
        let left: Vec<&ArtifactRecord> = all.iter().filter(|w| !here(w)).collect();
        let progress = Progress {
            has_result: current.is_some(),
            logs_done: row.as_ref().is_some_and(|r| r.logs.is_some()),
            artifacts_left: u32::try_from(left.len()).unwrap_or(u32::MAX),
            closed: closed.is_some(),
        };
        match (plan::next_step(progress), current, closed) {
            (Step::Done, _, Some(verdict)) => Ok(Flow::Closed(verdict)),
            (Step::FetchLogs, Some((digest, _, result)), _) => {
                self.logs(link, rec, declared, &digest, &result, deadline)
                    .await
            }
            (Step::FetchArtifact, Some(_), _) if changed => {
                Ok(bad("an artifact that changed after it was published"))
            }
            (Step::FetchArtifact, Some(_), _) => match left.first() {
                Some(artifact) => self.artifact(link, rec, artifact, deadline).await,
                None => Ok(Flow::Again),
            },
            (Step::Ack, Some((digest, value, result)), _) => {
                let logs = row.and_then(|r| r.logs).unwrap_or(Value::Null);
                if !self.sealed(rec, &value, &logs, &all).await? {
                    return Ok(Flow::Again);
                }
                self.ack(link, rec, declared, &digest, &result).await
            }
            // Without a recorded result nothing else is known.
            _ => match self.read_result(link, rec, declared).await? {
                Ok(_) => Ok(Flow::Again),
                Err(flow) => Ok(flow),
            },
        }
    }

    /// Whether everything the collection recorded is in the result store,
    /// durable, as recorded: the last thing before the acknowledgement,
    /// which is final. The result is written again from its record; each
    /// log and each artifact is read back and compared with its recorded
    /// length and digest. What is not as recorded is taken back from the
    /// record (`false`), and the collection goes on from there.
    async fn sealed(
        &self,
        rec: &AttemptRecord,
        result: &Value,
        logs: &Value,
        artifacts: &[ArtifactRecord],
    ) -> Result<bool> {
        let a = rec.attempt_id.clone();
        let streams: Vec<(LogStream, Option<(Digest, u64)>)> =
            [(LogStream::Stdout, "stdout"), (LogStream::Stderr, "stderr")]
                .into_iter()
                .map(|(stream, name)| {
                    let digest = logs[name]["sha256"]
                        .as_str()
                        .and_then(|d| Digest::parse(d).ok());
                    (stream, digest.zip(logs[name]["bytes"].as_u64()))
                })
                .collect();
        let mut unpublished = Vec::new();
        for (stream, recorded) in &streams {
            let (id, stream) = (a.clone(), *stream);
            let mut progress = self.db(move |s| s.log_progress(&id, stream)).await?;
            if progress.is_none() {
                if let Some((digest, bytes)) = recorded {
                    let (id, digest, bytes) = (a.clone(), digest.clone(), *bytes);
                    if let Some(verified) = self
                        .fs(move |f| f.verify_legacy_log_prefix(&id, stream, &digest, bytes))
                        .await?
                    {
                        progress = self
                            .db(move |s| s.bootstrap_log_progress(verified, wall_ms()))
                            .await?;
                    }
                }
            }
            if let Some(progress) = progress {
                if recorded.as_ref().map(|(_, bytes)| *bytes) != Some(progress.committed_bytes) {
                    return Err(StorageError::Corrupt(
                        "final receipt differs from committed log prefix".into(),
                    )
                    .into());
                }
                let (id, bytes) = (a.clone(), progress.committed_bytes);
                self.fs(move |f| f.recover_log_prefix(&id, stream, bytes))
                    .await?;
            } else {
                // A legacy file with no published progress may still use the
                // old repair contract. Never discard a published live prefix.
                unpublished.push(stream);
            }
        }
        let (bytes, wanted) = (result.to_string().into_bytes(), artifacts.to_vec());
        let id = a.clone();
        let (logs_ok, published_bad, lost) = self
            .fs(move |f| {
                f.write_result(&id, &bytes)?;
                let mut logs_ok = true;
                let mut published_bad = false;
                for (stream, recorded) in &streams {
                    let valid = match recorded {
                        Some((digest, bytes)) => f.verify_log(&id, *stream, digest, *bytes)?,
                        None => false,
                    };
                    if !valid {
                        logs_ok = false;
                        if unpublished.contains(stream) {
                            f.discard_log(&id, *stream)?;
                        } else {
                            published_bad = true;
                        }
                    }
                }
                let mut lost = Vec::new();
                for w in &wanted {
                    if !f.verify_artifact(&id, &w.digest, w.size)? {
                        lost.push(w.name.clone());
                    }
                }
                if logs_ok && lost.is_empty() {
                    f.seal(&id)?;
                }
                Ok((logs_ok, published_bad, lost))
            })
            .await?;
        if published_bad {
            return Err(StorageError::Corrupt(
                "published log prefix differs from its final receipt".into(),
            )
            .into());
        }
        if logs_ok && lost.is_empty() {
            return Ok(true);
        }
        self.db(move |s| s.recollect(&a, !logs_ok, &lost, wall_ms()))
            .await?;
        Ok(false)
    }

    /// Read the result, check it and record it as the one that is
    /// collected. Returns its digest.
    async fn read_result(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
    ) -> Result<std::result::Result<Digest, Flow>> {
        let request = Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: rec.attempt_id.clone(),
            generation: rec.generation,
        });
        let result = match link.call(request).await {
            Ok(Response::GetResult(r)) => r,
            Ok(_) => return Ok(Err(bad("another answer than a result"))),
            Err(e) => return Ok(Err(Flow::Fault(fault_of(e), "get_result".into()))),
        };
        if let Err(why) = self.check_result(rec, declared, &result) {
            return Ok(Err(bad(why)));
        }
        let Ok(value) = serde_json::to_value(&result) else {
            return Ok(Err(bad("a result that cannot be recorded")));
        };
        // The file first, whole and durable; then the record and the GC
        // references of the files in one commit. A parent that dies in
        // between has a file without a record and reads the result again.
        let bytes = value.to_string().into_bytes();
        let a = rec.attempt_id.clone();
        self.fs(move |f| f.write_result(&a, &bytes)).await?;
        let (a, digest, names) = (
            rec.attempt_id.clone(),
            result.result_digest.clone(),
            declared.names(&result),
        );
        self.db(move |s| {
            let refs = [
                ResultRef {
                    ref_id: format!("{a}:result"),
                    kind: BlobKind::Result,
                    locator: ResultStore::result_locator(&a),
                },
                ResultRef {
                    ref_id: format!("{a}:logs"),
                    kind: BlobKind::Log,
                    locator: ResultStore::logs_locator(&a),
                },
            ];
            s.begin_collection(&a, &digest, &value, &names, &refs, wall_ms())
        })
        .await?;
        Ok(Ok(result.result_digest))
    }

    /// A result is about this attempt, ended as the parent knows it, and
    /// lists artifacts that can be kept here.
    fn check_result(
        &self,
        rec: &AttemptRecord,
        declared: &Declared,
        result: &GetResultResponse,
    ) -> std::result::Result<(), &'static str> {
        if result.attempt_id != rec.attempt_id || result.generation != rec.generation {
            return Err("a result of another attempt");
        }
        if Some(result.termination) != rec.termination {
            return Err("a result with another termination than the attempt's");
        }
        if declared.payload.outputs.is_empty() && result.outputs_evaluated {
            return Err("raw-only result claims generic output evaluation");
        }
        let started = result.termination.outcome() == crate::state::Outcome::Succeeded
            || result.termination.exit().is_some()
            || rec.receipts.reached(Receipt::Running)
            || result
                .termination
                .timeout()
                .is_some_and(|t| t.phase == crate::state::TimeoutPhase::Running);
        if !declared.payload.outputs.is_empty() && !result.outputs_evaluated && started {
            return Err("started attempt omitted immutable output evaluation");
        }
        let names: BTreeSet<&str> = result
            .artifacts
            .iter()
            .map(|a| a.name.as_str())
            .chain(result.artifacts_not_stored.iter().map(|n| n.name.as_str()))
            .collect();
        plan::check_lists(
            result.artifacts.len(),
            result.artifacts_not_stored.len(),
            names.len(),
        )?;
        for artifact in &result.artifacts {
            if declared.payload.outputs.iter().any(|d| {
                d.name == artifact.name
                    && !crate::outputs::eligible(
                        d.when,
                        result.termination.outcome(),
                        result.outputs_evaluated,
                    )
            }) {
                return Err("an ineligible output is listed as stored");
            }
            let expected = if declared
                .payload
                .artifacts
                .iter()
                .any(|d| d.name == artifact.name)
            {
                Some(crate::wire::ArtifactEncoding::Raw)
            } else if declared.payload.outputs.iter().any(|d| {
                d.name == artifact.name
                    && crate::outputs::eligible(
                        d.when,
                        result.termination.outcome(),
                        result.outputs_evaluated,
                    )
            }) {
                Some(crate::wire::ArtifactEncoding::TarV1)
            } else {
                None
            };
            if expected.is_some_and(|e| e != artifact.encoding)
                || (expected.is_none() && artifact.encoding != crate::wire::ArtifactEncoding::Raw)
            {
                return Err("output encoding or eligibility does not match its declaration");
            }

            let of_payload = declared.payload.source_id == artifact.source_id
                && declared.payload.plan_digest == artifact.plan_digest;
            plan::check_record(RecordFacts {
                same_attempt: artifact.attempt_id == rec.attempt_id && of_payload,
                path_is_name: artifact.path == artifact.name,
                name_ok: relative_path(&artifact.name).is_some(),
                size: artifact.size,
                max_size: self.cfg.max_artifact_bytes,
            })?;
        }
        for missing in &result.artifacts_not_stored {
            if let Some(d) = declared
                .payload
                .outputs
                .iter()
                .find(|d| d.name == missing.name)
            {
                if !crate::outputs::eligible(
                    d.when,
                    result.termination.outcome(),
                    result.outputs_evaluated,
                ) || missing.if_missing != d.if_missing
                    || d.paths.first() != Some(&missing.path)
                {
                    return Err("output refusal does not match its eligible declaration");
                }
            }
        }
        Ok(())
    }

    async fn prepare_log_prefix(
        &self,
        attempt: &AttemptId,
        stream: LogStream,
    ) -> Result<LogProgress> {
        let a = attempt.clone();
        let old = self.db(move |s| s.log_progress(&a, stream)).await?;
        if old.is_none() {
            let a = attempt.clone();
            let receipt = self
                .db(move |s| s.collection(&a))
                .await?
                .and_then(|c| c.logs)
                .and_then(|logs| {
                    logs.get(match stream {
                        LogStream::Stdout => "stdout",
                        LogStream::Stderr => "stderr",
                    })
                    .cloned()
                });
            if let Some(receipt) = receipt {
                if let (Some(digest), Some(bytes)) = (
                    receipt["sha256"]
                        .as_str()
                        .and_then(|v| Digest::parse(v).ok()),
                    receipt["bytes"].as_u64(),
                ) {
                    let a = attempt.clone();
                    let verified = self
                        .fs(move |f| f.verify_legacy_log_prefix(&a, stream, &digest, bytes))
                        .await?
                        .ok_or_else(|| {
                            StorageError::Corrupt(
                                "legacy final log prefix differs from its receipt".into(),
                            )
                        })?;
                    self.db(move |s| s.bootstrap_log_progress(verified, wall_ms()))
                        .await?;
                } else {
                    return Err(
                        StorageError::Corrupt("invalid legacy final log receipt".into()).into(),
                    );
                }
            }
        }
        let a = attempt.clone();
        let progress = self
            .db(move |s| s.prepare_log_progress(&a, stream, wall_ms()))
            .await?;
        Ok(progress)
    }

    /// A single durable step, shared by active and final collection. The caller
    /// owns the coordinator collecting guard; never cancel this future during FS IO.
    async fn read_log_step(
        &self,
        link: &dyn WorkerLink,
        attempt: &AttemptId,
        node: &NodeId,
        stream: LogStream,
        deadline: Instant,
    ) -> Result<LogRead> {
        let progress = self.prepare_log_prefix(attempt, stream).await?;
        let offset = progress.committed_bytes;
        let a = attempt.clone();
        self.fs(move |f| f.recover_log_prefix(&a, stream, offset))
            .await?;
        if progress.eof {
            return Ok(LogRead::Progress(progress));
        }
        let max_bytes = self.cfg.chunk_bytes.min(65536).min(
            self.cfg
                .max_log_bytes
                .saturating_sub(offset)
                .min(u64::from(u32::MAX)) as u32,
        );
        // A parent cap is not an EOF. A zero-byte probe can still observe a
        // real end mark, but must never invent one for a growing worker spool.
        let request = Request::ReadLog(ReadLogRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: attempt.clone(),
            node_id: node.clone(),
            stream,
            offset,
            max_bytes,
        });
        if self.stopping() {
            return Ok(LogRead::Stopped);
        }
        let mut stop = self.stop.clone();
        let stopped = async {
            if let Some(stop) = stop.as_mut() {
                if !*stop.borrow() {
                    let _ = stop.changed().await;
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        // Only network IO is cancellable. Once append begins, await its fsync
        // and paired fenced commit before releasing the collecting guard.
        let answer = tokio::select! {
            _ = stopped => return Ok(LogRead::Stopped),
            answer = tokio::time::timeout_at(deadline, link.call(request)) => answer,
        };
        let r = match answer {
            Ok(Ok(Response::ReadLog(r))) => r,
            Ok(Ok(_)) => {
                return Ok(LogRead::Fault(
                    Fault::BadAnswer,
                    "another answer than log bytes".into(),
                ))
            }
            Ok(Err(e)) => return Ok(LogRead::Fault(fault_of(e), "read_log".into())),
            Err(_) => {
                return Ok(LogRead::Fault(
                    Fault::Unreachable,
                    "read_log deadline".into(),
                ))
            }
        };
        let wait = match plan::check_log(LogFacts {
            want_offset: offset,
            max: u64::from(max_bytes),
            limit: self.cfg.max_log_bytes,
            got_offset: r.offset,
            got_len: r.data.0.len() as u64,
            got_next: r.next_offset,
            got_eof: r.eof,
        }) {
            LogVerdict::Bad(why) => return Ok(LogRead::Fault(Fault::BadAnswer, why.into())),
            LogVerdict::Wait => true,
            LogVerdict::Append { .. } => false,
        };
        let next = r.next_offset;
        let a = attempt.clone();
        self.fs(move |f| f.append_log(&a, stream, offset, &r.data.0))
            .await?;
        let a = attempt.clone();
        let (eof, truncated, dropped) = (r.eof, r.log_truncated, r.dropped_bytes);
        if !self
            .db(move |s| {
                s.commit_log_progress(&a, stream, offset, next, eof, truncated, dropped, wall_ms())
            })
            .await?
        {
            return Ok(LogRead::Fault(Fault::Local, "stale log offset".into()));
        }
        let progress = LogProgress {
            committed_bytes: next,
            eof,
            log_truncated: truncated,
            dropped_bytes: dropped,
            updated_at_ms: wall_ms(),
        };
        Ok(if wait {
            LogRead::Wait(progress)
        } else {
            LogRead::Progress(progress)
        })
    }

    /// Poll a bounded keyset page. Each target receives at most one chunk per
    /// stream, so an indefinitely growing spool cannot starve another attempt.
    pub async fn collect_live_worker(
        &self,
        worker: &WorkerId,
        link: &dyn WorkerLink,
        after: Option<AttemptId>,
        deadline: Instant,
    ) -> Result<(Option<AttemptId>, CollectReport)> {
        let (w, scan) = (worker.clone(), self.cfg.scan.min(64));
        let wrap = after.is_some();
        let mut page = self
            .db(move |s| s.query_live_log_targets(&w, after.as_ref(), scan))
            .await?;
        if page.is_empty() && wrap {
            let w = worker.clone();
            page = self
                .db(move |s| s.query_live_log_targets(&w, None, scan))
                .await?;
        }
        let mut cursor = None;
        let mut report = CollectReport::default();
        for target in page {
            if self.stopping() || Instant::now() >= deadline {
                break;
            }
            let a = target.record.attempt_id;
            cursor = Some(a.clone());
            // Final collection owns terminal attempts and its established RPC
            // order/fault policy; live polling only precedes termination.
            if target.record.termination.is_some() {
                continue;
            }
            if self.live_waits.waiting(&a) {
                continue;
            }
            let declared = match declared_of(&Some(target.payload)) {
                Ok(d) => d,
                Err(why) => {
                    report.faults.push((a, why.into()));
                    continue;
                }
            };
            let Some(node) = declared.node else {
                report
                    .faults
                    .push((a, "no source node to read logs".into()));
                continue;
            };
            let mut retry = false;
            for (index, stream) in [LogStream::Stdout, LogStream::Stderr]
                .into_iter()
                .enumerate()
            {
                if self.stopping() || Instant::now() >= deadline {
                    break;
                }
                let id = a.clone();
                if self
                    .db(move |s| s.get_attempt(&id))
                    .await?
                    .is_none_or(|rec| rec.retired || rec.termination.is_some())
                {
                    break;
                }
                let step_deadline = if index == 0 {
                    Instant::now() + deadline.saturating_duration_since(Instant::now()) / 2
                } else {
                    deadline
                };
                match self
                    .read_log_step(link, &a, &node, stream, step_deadline)
                    .await
                {
                    Ok(LogRead::Fault(fault, why)) => {
                        retry = true;
                        if matches!(fault, Fault::BadAnswer | Fault::Local) {
                            report.faults.push((a.clone(), why));
                        }
                    }
                    Ok(_) => {}
                    Err(e @ (CollectError::Fenced | CollectError::Aborted)) => return Err(e),
                    Err(e) => {
                        retry = true;
                        report.faults.push((a.clone(), e.to_string()));
                    }
                }
            }
            if retry {
                self.live_waits.put_off_live(&a, self.cfg.backoff_ms);
            } else {
                self.live_waits.clear(&a);
            }
        }
        Ok((cursor, report))
    }

    /// Read both streams to their end and record what they hold.
    async fn logs(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
        digest: &Digest,
        result: &GetResultResponse,
        deadline: Instant,
    ) -> Result<Flow> {
        let has_stored_result = payload_hash(&bare_result(&result.termination))
            .map(|bare| &bare != digest)
            .unwrap_or(true);
        let Some(node) = declared.node.clone().or_else(|| NodeId::parse("log").ok()) else {
            return Ok(bad("no node to read the log of"));
        };
        let mut summary = serde_json::Map::new();
        for (stream, name) in [(LogStream::Stdout, "stdout"), (LogStream::Stderr, "stderr")] {
            let info = loop {
                if self.stopping() || Instant::now() >= deadline {
                    return Ok(Flow::Later);
                }
                let (progress, complete) = match self
                    .read_log_step(link, &rec.attempt_id, &node, stream, deadline)
                    .await?
                {
                    LogRead::Stopped => return Ok(Flow::Later),
                    LogRead::Fault(fault, why) => return Ok(Flow::Fault(fault, why)),
                    LogRead::Wait(progress) => match plan::log_wait(has_stored_result) {
                        LogWait::Retry => return Ok(Flow::Later),
                        LogWait::TakeIncomplete => (progress, false),
                    },
                    LogRead::Progress(progress) => {
                        if !progress.eof {
                            continue;
                        }
                        (progress, true)
                    }
                };
                let offset = progress.committed_bytes;
                // What is here now is what the acknowledgement vouches for:
                // its length and digest are recorded and compared again
                // before it is sent.
                let a = rec.attempt_id.clone();
                let (sha256, here) = self.fs(move |f| f.log_digest(&a, stream)).await?;
                if here != offset {
                    return Err(StoreError::Offset {
                        want: offset,
                        have: here,
                    }
                    .into());
                }
                break json!({
                    "bytes": offset,
                    "sha256": sha256.as_str(),
                    "complete": complete,
                    "log_truncated": progress.log_truncated,
                    "dropped_bytes": progress.dropped_bytes,
                });
            };
            summary.insert(name.to_string(), info);
        }
        let (a, digest, logs) = (
            rec.attempt_id.clone(),
            digest.clone(),
            Value::Object(summary),
        );
        self.db(move |s| s.set_collection_logs(&a, &digest, &logs, wall_ms()))
            .await?;
        Ok(Flow::Again)
    }

    /// Fetch what is missing of one artifact, verify it and publish it.
    async fn artifact(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        artifact: &ArtifactRecord,
        deadline: Instant,
    ) -> Result<Flow> {
        let a = rec.attempt_id.clone();
        loop {
            let (id, digest, size) = (a.clone(), artifact.digest.clone(), artifact.size);
            let state = self
                .fs(move |f| f.artifact_state(&id, &digest, size))
                .await?;
            let offset = match state {
                ArtifactState::Published => break,
                ArtifactState::Partial(offset) => offset,
            };
            if offset == artifact.size {
                let (id, digest, size) = (a.clone(), artifact.digest.clone(), artifact.size);
                let published = self
                    .fs(move |f| f.publish_artifact(&id, &digest, size))
                    .await?;
                let whole = match published {
                    Publish::Published => break,
                    Publish::Mismatch { whole } => whole,
                };
                match plan::judge_mismatch(whole) {
                    Mismatch::Strike => {
                        return Ok(Flow::Fault(
                            Fault::Corrupt,
                            format!("artifact {} does not have its digest", artifact.name),
                        ))
                    }
                    // Bytes a parent before this one left, perhaps torn:
                    // they are gone now, and the artifact is fetched from
                    // its start.
                    Mismatch::Refetch => continue,
                }
            }
            if self.stopping() || Instant::now() >= deadline {
                return Ok(Flow::Later);
            }
            let request = Request::BlobGet(BlobGetRequest {
                schema_version: SchemaVersion::CURRENT,
                request_id: RequestId::generate(),
                attempt_id: a.clone(),
                digest: artifact.digest.clone(),
                offset,
                max_bytes: self.cfg.chunk_bytes,
            });
            let r = match link.call(request).await {
                Ok(Response::BlobGet(r)) => r,
                Ok(_) => return Ok(bad("another answer than a chunk")),
                Err(e) => return Ok(Flow::Fault(fault_of(e), "blob_get".into())),
            };
            let verdict = plan::check_chunk(ChunkFacts {
                same_digest: r.digest == artifact.digest,
                want_offset: offset,
                size: artifact.size,
                max: u64::from(self.cfg.chunk_bytes),
                got_offset: r.offset,
                got_total: r.total_size,
                got_len: r.data.0.len() as u64,
                got_eof: r.eof,
            });
            match verdict {
                ChunkVerdict::Bad(why) => return Ok(bad(why)),
                ChunkVerdict::Append { .. } => {
                    let (id, digest, data) = (a.clone(), artifact.digest.clone(), r.data.0);
                    self.fs(move |f| f.append_part(&id, &digest, offset, &data))
                        .await?;
                }
            }
        }
        let receipt = ArtifactReceipt {
            attempt_id: a.clone(),
            name: artifact.name.clone(),
            rel_path: artifact.path.clone(),
            size: artifact.size,
            digest: artifact.digest.clone(),
            source_id: artifact.source_id.clone(),
            plan_digest: artifact.plan_digest.clone(),
            profile_fingerprint: artifact.profile_fingerprint.to_string(),
        };
        self.db(move |s| {
            let file = ResultRef {
                ref_id: format!("{a}:artifact:{}", receipt.name),
                kind: BlobKind::Result,
                locator: ResultStore::artifact_locator(&a, &receipt.digest),
            };
            s.record_collected_artifact(&receipt, &file, wall_ms())
        })
        .await?;
        Ok(Flow::Again)
    }

    /// Everything of the recorded result is verified and durable here: tell
    /// the worker, by digest, and close on its answer.
    async fn ack(
        &self,
        link: &dyn WorkerLink,
        rec: &AttemptRecord,
        declared: &Declared,
        digest: &Digest,
        result: &GetResultResponse,
    ) -> Result<Flow> {
        let a = rec.attempt_id.clone();
        let request = Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: rec.generation,
            result_digest: digest.clone(),
        });
        match link.call(request).await {
            Ok(Response::AckCollected(r)) if r.attempt_id == a && r.released => {
                let verdict = verdict_of(&declared.names(result), result);
                let digest = digest.clone();
                self.db(move |s| s.close_collection(&a, Some(&digest), verdict, true, wall_ms()))
                    .await?;
                Ok(Flow::Closed(verdict))
            }
            Ok(_) => Ok(bad("an acknowledgement that released nothing")),
            Err(LinkError::Remote(e)) => match plan::ack_refusal(e.code) {
                // The worker holds another result than the one that was read.
                AckRefusal::OtherResult => match self.read_result(link, rec, declared).await? {
                    Err(flow) => Ok(flow),
                    Ok(now) => match plan::judge_reread(&now == digest) {
                        Reread::Restart => Ok(Flow::Again),
                        Reread::Contradiction => {
                            Ok(bad("an acknowledgement refused for the result it gave"))
                        }
                    },
                },
                // The worker has nothing of the attempt any more (an
                // acknowledgement whose answer was lost, sent again after
                // the worker deleted what it released). Everything is here,
                // verified: the collection is closed, as not acknowledged.
                AckRefusal::Gone => {
                    let verdict = verdict_of(&declared.names(result), result);
                    let digest = digest.clone();
                    self.db(move |s| {
                        s.close_collection(&a, Some(&digest), verdict, false, wall_ms())
                    })
                    .await?;
                    Ok(Flow::Closed(verdict))
                }
                AckRefusal::Fault => Ok(Flow::Fault(Fault::Remote(e.code), "ack_collected".into())),
            },
            Err(e) => Ok(Flow::Fault(fault_of(e), "ack_collected".into())),
        }
    }
}

fn bad(why: &str) -> Flow {
    Flow::Fault(Fault::BadAnswer, why.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_retry_hints_stay_bounded_when_faulted_attempts_leave_selection() {
        let waits = Waits::default();
        for _ in 0..1024 {
            waits.put_off_live(&AttemptId::generate(), 60_000);
        }
        assert_eq!(waits.map().len(), 8 * 64);
        let attempt = AttemptId::generate();
        let counts = Waits::default();
        counts.put_off_live(&attempt, 60_000);
        counts.put_off_live(&attempt, 60_000);
        assert_eq!(counts.map()[&attempt].0, 2);
        counts.clear(&attempt);
        assert!(counts.map().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_filesystem_await_keeps_owner_until_blocking_mutation_drains() {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(CoordinatorStore::open(root.path(), 1).unwrap());
        let owner = Arc::downgrade(&store);
        let files = Arc::new(ResultStore::open(&root.path().join("results")).unwrap());
        let attempt = AttemptId::generate();
        let id = attempt.clone();
        let writer = Arc::new(tokio::sync::Mutex::new(()));
        let guard = Arc::new(writer.clone().lock_owned().await);
        let collector =
            Collector::new(store, files.clone(), CollectConfig::default()).with_collecting(guard);
        let (entered, receive_entered) = std::sync::mpsc::channel();
        let (resume, receive_resume) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            collector
                .fs(move |files| {
                    files.append_log(&id, LogStream::Stdout, 0, b"uncommitted")?;
                    entered.send(()).unwrap();
                    receive_resume
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                    Ok(())
                })
                .await
        });
        tokio::task::spawn_blocking(move || receive_entered.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let retained = matches!(
            CoordinatorStore::open(root.path(), 1),
            Err(StorageError::Locked(_))
        );
        let writer_retained = writer.try_lock().is_err();
        resume.send(()).unwrap();
        assert!(
            writer_retained,
            "abort released collecting guard before blocking FS completed"
        );
        assert!(
            retained,
            "abort released owner before blocking FS completed"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while owner.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let _drained = tokio::time::timeout(Duration::from_secs(2), writer.lock())
            .await
            .unwrap();
        assert_eq!(
            files.read_log(&attempt, LogStream::Stdout).unwrap(),
            b"uncommitted"
        );
    }
}
