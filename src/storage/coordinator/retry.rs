//! Retry preflight releases the DB thread; commit rechecks a bounded metadata scan.
use super::control::{control_receipt, resolve, revision};
use super::*;
use crate::service::completion::{RetryReceipt, Target};
use crate::state::Outcome;
use sha2::{Digest as _, Sha256};

const PAGE: i64 = 64;

pub enum RetryPreparation {
    Replay(RetryReceipt),
    Prepared(PreparedRetry),
}

pub struct PreparedRetry {
    target: Target,
    expected: u64,
    request_id: String,
    pub(crate) run_id: RunId,
    pub(crate) source_id: SourceId,
    pub(crate) manifest_digest: Digest,
    has_secret_bundle: bool,
    pub(crate) in_place: bool,
    fingerprint: [u8; 32],
}

struct Binding {
    run: RunId,
    source: SourceId,
    digest: Digest,
    selected: Option<String>,
}
struct Scan {
    count: u64,
    boundary: Option<u64>,
    secret: bool,
    fingerprint: [u8; 32],
}
struct TaskRow {
    /// It was cancelled by fail-fast before it was ever placed.
    stopped_unplaced: bool,
    shard_retry_forbidden: bool,
    /// A shard of a failed split job that did not pass on its own, whatever
    /// its attempt says (`ci.rerun`).
    rerun: bool,
    /// The split job it is a shard of.
    shard_group: Option<String>,
    task: String,
    min: i64,
    generation: Option<i64>,
    state: Option<String>,
    observation: Option<String>,
    termination: Option<String>,
    retired: Option<bool>,
    unsafe_history: bool,
    manifest: Option<String>,
    secret: Option<bool>,
    in_place: bool,
}

impl CoordinatorStore {
    /// Read-only preconditions and metadata, without retaining a DB transaction.
    pub fn prepare_retry(
        &self,
        target: Target,
        expected: u64,
        request_id: String,
    ) -> Result<RetryPreparation> {
        let source_guard = self.source_mutation_guard_foreground()?;
        if request_id.is_empty() || request_id.len() > 128 {
            return Err(StorageError::Invalid("invalid retry request ID".into()));
        }
        let preparation=self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            if let Some(receipt) = control_receipt(&tx, &target, expected, &request_id, "retry")? {
                return Ok(RetryPreparation::Replay(receipt));
            }
            let binding = binding(&tx, &target, expected)?;
            let scan = scan(&tx, &binding, None)?;
            let in_place: bool=tx.query_row("SELECT count(*)>0 AND min(coalesce(json_extract(spec_json,'$.in_place'),0))=1 FROM task WHERE run_id=?1 AND (?2 IS NULL OR task_id=?2)", rusqlite::params![binding.run.as_str(),binding.selected.as_deref()],|r|r.get(0))?;
            Ok(RetryPreparation::Prepared(PreparedRetry {
                target,
                expected,
                request_id,
                run_id: binding.run,
                source_id: binding.source,
                manifest_digest: binding.digest,
                has_secret_bundle: scan.secret,
                in_place,
                fingerprint: scan.fingerprint,
            }))
        })?;
        if let RetryPreparation::Prepared(ref prepared) = preparation {
            source_guard
                .ensure_sdk_available()
                .map_err(|e| StorageError::Invalid(e.to_string()))?;
            source_guard
                .ensure_run_available(&prepared.run_id)
                .map_err(|e| StorageError::Invalid(e.to_string()))?;
            if let Some(id) = crate::source::classify::ci_id_of(&prepared.source_id) {
                source_guard
                    .ensure_source_available(&id)
                    .map_err(|e| StorageError::Invalid(e.to_string()))?;
            }
        }
        Ok(preparation)
    }

    /// Filesystem validation has finished. Recheck everything before authorizing work.
    /// The caller must retain the cross-process source/Vault input lease through
    /// validation and commit; a prepared token itself is not an input retention pin.
    pub fn commit_retry(
        &self,
        prepared: PreparedRetry,
        verified_secret: bool,
        at_ms: u64,
    ) -> Result<RetryReceipt> {
        let source = self.source_mutation_guard()?;
        self.commit_retry_guarded(prepared, verified_secret, at_ms, source)
    }
    pub(crate) fn commit_retry_guarded(
        &self,
        prepared: PreparedRetry,
        verified_secret: bool,
        at_ms: u64,
        source: crate::source::retention::Guard,
    ) -> Result<RetryReceipt> {
        if source.root != self.state_dir.join("sources") {
            return Err(StorageError::Invalid("foreign source guard".into()));
        }
        source
            .ensure_sdk_available()
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        source
            .ensure_run_available(&prepared.run_id)
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        if let Some(id) = crate::source::classify::ci_id_of(&prepared.source_id) {
            source
                .ensure_source_available(&id)
                .map_err(|e| StorageError::Invalid(e.to_string()))?;
        }
        let _source = source;

        self.db.write(move |tx| {
            if let Some(receipt) = control_receipt(tx, &prepared.target, prepared.expected, &prepared.request_id, "retry")? {
                return Ok(receipt);
            }
            let binding = binding(tx, &prepared.target, prepared.expected)?;
            if binding.run != prepared.run_id
                || binding.source != prepared.source_id
                || binding.digest != prepared.manifest_digest
                || verified_secret != prepared.has_secret_bundle
            {
                return Err(StorageError::Immutable(
                    "retry input binding changed".into(),
                ));
            }
            let next = prepared
                .expected
                .checked_add(1)
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or_else(|| StorageError::Invalid("control revision exhausted".into()))?;
            let scan = scan(tx, &binding, Some(next))?;
            if scan.fingerprint != prepared.fingerprint || scan.secret != verified_secret {
                return Err(StorageError::Immutable(
                    "retry task snapshot changed".into(),
                ));
            }
            tx.execute(
                "UPDATE run SET control_revision=?1 WHERE run_id=?2 AND control_revision=?3",
                params![
                    to_i64(next)?,
                    binding.run.as_str(),
                    to_i64(prepared.expected)?
                ],
            )?;
            let receipt = RetryReceipt {
                request_id: prepared.request_id.clone(),
                run_id: binding.run.clone(),
                control_revision: next,
                task_count: scan.count,
                task_id: binding
                    .selected
                    .as_deref()
                    .map(TaskId::parse)
                    .transpose()
                    .map_err(|e| StorageError::Corrupt(e.to_string()))?,
                min_generation: scan.boundary,
            };
            tx.execute(
                "INSERT INTO control_request(request_id,operation,target_json,expected_revision,reply_json) VALUES(?1,'retry',?2,?3,?4)",
                params![
                    prepared.request_id,
                    json(&prepared.target),
                    to_i64(prepared.expected)?,
                    json(&receipt)
                ],
            )?;
            common::append_event(
                tx,
                Some(binding.run.as_str()),
                None,
                "retry_requested",
                &serde_json::to_value(&receipt)
                    .map_err(|e| StorageError::Invalid(e.to_string()))?,
                at_ms,
            )?;
            Ok(receipt)
        })
    }

    #[cfg(test)]
    pub fn retry<F>(
        &self,
        target: Target,
        expected: u64,
        request_id: String,
        at_ms: u64,
        validate: F,
    ) -> Result<RetryReceipt>
    where
        F: FnOnce(&RunId, &SourceId, &Digest) -> Result<bool>,
    {
        match self.prepare_retry(target, expected, request_id)? {
            RetryPreparation::Replay(receipt) => Ok(receipt),
            RetryPreparation::Prepared(prepared) => {
                let secret = validate(
                    &prepared.run_id,
                    &prepared.source_id,
                    &prepared.manifest_digest,
                )?;
                self.commit_retry(prepared, secret, at_ms)
            }
        }
    }
}

/// A shard of the split job `group` of `run` runs again: the job has no
/// outcome any more, for any of its shards, until all of them ended again.
///
/// Only after the scan of a retry read every task: the mark that a shard
/// did not pass on its own (`ci.rerun`) goes with the outcome, and a shard
/// on a later page of the scan is still to be chosen by it.
fn unjoin(c: &Connection, run: &RunId, group: &str) -> Result<()> {
    c.execute(
        "UPDATE task SET spec_json=json_remove(spec_json,'$.ci.joined','$.ci.join_failure','$.ci.rerun')
         WHERE run_id=?1 AND json_extract(spec_json,'$.ci.shard_group')=?2",
        params![run.as_str(), group],
    )?;
    Ok(())
}

fn binding(c: &Connection, target: &Target, expected: u64) -> Result<Binding> {
    let (run, selected) = resolve(c, target)?;
    let (closed, source, digest): (Option<i64>, Option<String>, Option<String>) = c.query_row(
        "SELECT r.closed_at_ms,r.source_id,s.manifest_digest FROM run r LEFT JOIN source_ref s USING(source_id) WHERE r.run_id=?1",
        [&run], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).optional()?.ok_or_else(|| not_found(format!("run {run}")))?;
    if closed.is_some() {
        return Err(StorageError::Immutable(
            "closed run cannot be retried".into(),
        ));
    }
    if revision(c, &run)? != expected {
        return Err(StorageError::Immutable("stale control revision".into()));
    }
    expected
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| StorageError::Invalid("control revision exhausted".into()))?;
    let missing = || StorageError::Invalid("retained source unavailable".into());
    Ok(Binding {
        run: id("run_id", RunId::parse(&run))?,
        source: id("source_id", SourceId::parse(&source.ok_or_else(missing)?))?,
        digest: id(
            "manifest_digest",
            Digest::parse(&digest.ok_or_else(missing)?),
        )?,
        selected,
    })
}

/// Pages are fully read and dropped before task updates on the same connection.
/// What a later page is chosen by (`ci.rerun`) is not written before the last.
fn scan(c: &Connection, binding: &Binding, authorize: Option<u64>) -> Result<Scan> {
    let mut after = String::new();
    let mut count = 0_u64;
    let mut boundary = None;
    let mut secret = None;
    let mut hash = Sha256::new();
    // The split jobs a shard of which runs again.
    let mut unjoined = std::collections::BTreeSet::new();
    loop {
        let page = {
            let mut stmt = c.prepare(
                "SELECT t.task_id,t.min_generation,a.generation,a.state,a.observation,a.termination_json,a.retired,
                 EXISTS(SELECT 1 FROM attempt x LEFT JOIN retire_tombstone z USING(attempt_id) WHERE x.task_id=t.task_id AND x.retired=0 AND (x.state<>'terminal' OR (z.attempt_id IS NOT NULL AND z.confirmed_at_ms IS NULL))),
                 json_extract(t.spec_json,'$.manifest_digest'),json_extract(t.spec_json,'$.has_secret_bundle'),coalesce(json_extract(t.spec_json,'$.in_place'),0),
                 t.cancel_requested_at_ms IS NOT NULL AND coalesce(json_extract(t.spec_json,'$.ci.unrunnable'),'')=?5,
                 coalesce(json_extract(t.spec_json,'$.ci.rerun'),0),json_extract(t.spec_json,'$.ci.shard_group'),coalesce(json_extract(t.spec_json,'$.ci.shard_retry_forbidden'),0)
                 FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id ORDER BY aa.generation DESC LIMIT 1)
                 WHERE t.run_id=?1 AND (?2 IS NULL OR t.task_id=?2) AND t.task_id>?3 ORDER BY t.task_id LIMIT ?4",
            )?;
            let rows = stmt.query_map(
                params![
                    binding.run.as_str(),
                    binding.selected,
                    after,
                    PAGE,
                    crate::ci::shards::FAIL_FAST_REASON
                ],
                |r| {
                    Ok(TaskRow {
                        task: r.get(0)?,
                        min: r.get(1)?,
                        generation: r.get(2)?,
                        state: r.get(3)?,
                        observation: r.get(4)?,
                        termination: r.get(5)?,
                        retired: r.get(6)?,
                        unsafe_history: r.get(7)?,
                        manifest: r.get(8)?,
                        secret: r.get(9)?,
                        in_place: r.get(10)?,
                        stopped_unplaced: r.get(11)?,
                        rerun: r.get(12)?,
                        shard_group: r.get(13)?,
                        shard_retry_forbidden: r.get(14)?,
                    })
                },
            )?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        if page.is_empty() {
            break;
        }
        after = page.last().expect("nonempty page").task.clone();
        for row in page {
            // A shard that fail-fast cancelled before it was ever placed has
            // no attempt to prove a stop: nothing of it ever ran. A retry
            // lets it into the queue again (its cancel and the reason are
            // taken away). Any other task without an attempt is refused.
            if row.generation.is_none() && row.stopped_unplaced {
                let flag = row
                    .secret
                    .ok_or_else(|| StorageError::Corrupt("task secret flag missing".into()))?;
                if secret.is_some_and(|v| v != flag) {
                    return Err(StorageError::Immutable(
                        "task secret binding changed".into(),
                    ));
                }
                secret = Some(flag);
                hash.update((row.task.len() as u64).to_be_bytes());
                hash.update(row.task.as_bytes());
                hash.update(b"stopped-unplaced");
                hash.update(row.min.to_be_bytes());
                hash.update([u8::from(flag), u8::from(row.in_place)]);
                count = count
                    .checked_add(1)
                    .ok_or_else(|| StorageError::Invalid("task count exhausted".into()))?;
                if let Some(revision) = authorize {
                    c.execute("UPDATE task SET authorization_revision=?1,cancel_requested_at_ms=NULL,spec_json=json_remove(spec_json,'$.ci.unrunnable') WHERE task_id=?2",
                        params![to_i64(revision)?, row.task])?;
                    unjoined.extend(row.shard_group);
                }
                continue;
            }
            let generation = row.generation.ok_or_else(|| {
                StorageError::Immutable(
                    "retry requires a terminal attempt; submit a new run".into(),
                )
            })?;
            if generation < row.min
                || row.state.as_deref() != Some("terminal")
                || row.observation.as_deref() != Some("known")
                || row.retired != Some(false)
                || row.unsafe_history
            {
                return Err(StorageError::Immutable(
                    "retry requires known terminal stopping without pending retirement".into(),
                ));
            }
            let termination = row.termination.ok_or_else(|| {
                StorageError::Immutable("terminal stopping proof unavailable".into())
            })?;
            let term: Termination = from_json("termination", &termination)?;
            let manifest = row
                .manifest
                .ok_or_else(|| StorageError::Corrupt("task manifest digest missing".into()))?;
            if id("manifest_digest", Digest::parse(&manifest))? != binding.digest {
                return Err(StorageError::Immutable(
                    "task source binding changed".into(),
                ));
            }
            let flag = row
                .secret
                .ok_or_else(|| StorageError::Corrupt("task secret flag missing".into()))?;
            if secret.is_some_and(|v| v != flag) {
                return Err(StorageError::Immutable(
                    "task secret binding changed".into(),
                ));
            }
            secret = Some(flag);
            for value in [
                row.task.as_bytes(),
                manifest.as_bytes(),
                termination.as_bytes(),
            ] {
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value);
            }
            hash.update(generation.to_be_bytes());
            hash.update(row.min.to_be_bytes());
            hash.update([u8::from(flag), u8::from(row.in_place)]);
            // A shard whose attempt succeeded is kept only when it passed on
            // its own: one whose tests failed, or that did not report them
            // all, runs again with the rest of its failed job.
            if binding.selected.is_none() && term.outcome() == Outcome::Succeeded && !row.rerun {
                continue;
            }
            if row.shard_retry_forbidden {
                return Err(StorageError::Immutable("retry of this shard requires allow_retry = true in its captured ci.sharding policy".into()));
            }
            let next = generation
                .checked_add(1)
                .ok_or_else(|| StorageError::Invalid("attempt generation exhausted".into()))?;
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::Invalid("task count exhausted".into()))?;
            if binding.selected.is_some() {
                boundary = Some(to_u64(next)?);
            }
            if let Some(revision) = authorize {
                c.execute("UPDATE task SET min_generation=?1,authorization_revision=?2,cancel_requested_at_ms=NULL WHERE task_id=?3",
                    params![next, to_i64(revision)?, row.task])?;
                unjoined.extend(row.shard_group);
            }
        }
    }
    for group in &unjoined {
        unjoin(c, &binding.run, group)?;
    }
    if count == 0 {
        return Err(StorageError::Invalid(
            "no unsuccessful terminal tasks to retry".into(),
        ));
    }
    Ok(Scan {
        count,
        boundary,
        secret: secret.expect("selected task exists"),
        fingerprint: hash.finalize().into(),
    })
}
