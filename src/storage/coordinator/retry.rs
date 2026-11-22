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
    /// The frozen plan and the results read once, outside any transaction.
    evidence: Evidence,
}

/// What an attempt of a job that needs a retried job left: whether any step
/// of it ran.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    /// A job of it ran (or ended in a way that is not a skip).
    Ran,
    /// Every job of it was skipped: no step ran.
    SkippedOnly,
    /// The result cannot be read (it was removed or no longer matches).
    Unavailable,
}

/// The files a retry needs, read before the transaction of the scan so no
/// file is read (or hashed) while the DB is held: the frozen plans of the
/// run (or why one is unusable), and the verdict of each result of an attempt
/// that is the last of a task, by attempt and digest of its result.
#[derive(Default)]
struct Evidence {
    plans: std::collections::BTreeMap<String, std::result::Result<local_ci::FrozenPlan, String>>,
    verdicts: std::collections::BTreeMap<(String, String), Verdict>,
}

/// What of the run the files are read for: the plans and the results of the
/// last attempt of each task (plan digest, plan task, attempt, result digest).
struct Candidates {
    plans: Vec<String>,
    results: Vec<(String, String, String, String)>,
}

enum Probe {
    Replay(RetryReceipt),
    Read(Candidates),
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
    /// The task of the frozen plan, and the plan.
    plan_task: Option<(String, String)>,
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
        let state_dir = self.state_dir.clone();
        let (probe_target, probe_request) = (target.clone(), request_id.clone());
        let candidates = match self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            if let Some(receipt) =
                control_receipt(&tx, &probe_target, expected, &probe_request, "retry")?
            {
                return Ok(Probe::Replay(receipt));
            }
            let binding = binding(&tx, &probe_target, expected)?;
            Ok(Probe::Read(candidates(&tx, &binding)?))
        })? {
            Probe::Replay(receipt) => return Ok(RetryPreparation::Replay(receipt)),
            Probe::Read(candidates) => candidates,
        };
        // The files are read here, with no transaction open.
        let evidence = Evidence::read(&state_dir, candidates);
        let preparation=self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            if let Some(receipt) = control_receipt(&tx, &target, expected, &request_id, "retry")? {
                return Ok(RetryPreparation::Replay(receipt));
            }
            let binding = binding(&tx, &target, expected)?;
            let scan = scan(&tx, &binding, None, &evidence)?;
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
                evidence,
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
            let scan = scan(tx, &binding, Some(next), &prepared.evidence)?;
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
         WHERE run_id=?1 AND coalesce(json_extract(spec_json,'$.ci.shard_group'),json_extract(spec_json,'$.ci.matrix.group'))=?2",
        params![run.as_str(), group],
    )?;
    Ok(())
}

const SHARD_FORBIDDEN: &str =
    "retry of this shard requires allow_retry = true in its captured ci.sharding policy";

/// The last attempt of a task that still counts (the same pick everywhere).
const LIVE_ATTEMPT: &str = "SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id AND aa.retired=0 AND (aa.generation>=t.min_generation OR aa.state<>'terminal') ORDER BY aa.generation DESC LIMIT 1";

/// What the files of the retry are read for.
fn candidates(c: &Connection, binding: &Binding) -> Result<Candidates> {
    let plans = c
        .prepare(
            "SELECT DISTINCT json_extract(spec_json,'$.ci.task.plan_digest') FROM task
             WHERE run_id=?1 AND json_extract(spec_json,'$.ci.task.plan_digest') IS NOT NULL",
        )?
        .query_map([binding.run.as_str()], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let results = c
        .prepare(&format!(
            "SELECT json_extract(t.spec_json,'$.ci.task.plan_digest'),json_extract(t.spec_json,'$.ci.task.task_id'),a.attempt_id,r.digest
             FROM task t JOIN attempt a ON a.attempt_id=({LIVE_ATTEMPT})
             JOIN artifact_receipt r ON r.attempt_id=a.attempt_id AND r.name=?2 AND r.persisted_at_ms IS NOT NULL
             WHERE t.run_id=?1 AND a.state='terminal'
             AND json_extract(t.spec_json,'$.ci.task.plan_digest') IS NOT NULL AND json_extract(t.spec_json,'$.ci.task.task_id') IS NOT NULL"
        ))?
        .query_map(
            params![
                binding.run.as_str(),
                crate::supervisor::ci_task::RESULT_ARTIFACT
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Candidates { plans, results })
}

impl Evidence {
    /// Read each plan once and the result of each task that needs another
    /// (only those can be opened again).
    fn read(state_dir: &std::path::Path, candidates: Candidates) -> Self {
        let mut evidence = Evidence::default();
        for plan in candidates.plans {
            let read = Digest::parse(&plan)
                .map_err(|e| format!("the frozen plan is unavailable ({e})"))
                .and_then(|digest| {
                    crate::ci::advance::plan_bytes(state_dir, &digest)
                        .map_err(|why| format!("the frozen plan is unavailable ({why})"))
                })
                .and_then(|bytes| {
                    serde_json::from_slice::<local_ci::FrozenPlan>(&bytes)
                        .map_err(|e| format!("the frozen plan is unreadable ({e})"))
                })
                .map_err(|why| {
                    format!("retry cannot tell which jobs need the retried ones: {why}")
                });
            evidence.plans.insert(plan, read);
        }
        let results = state_dir.join("results");
        for (plan, task, attempt, digest) in candidates.results {
            let needs_another = matches!(
                evidence.plans.get(&plan),
                Some(Ok(p)) if p.tasks.iter().any(|t| t.id == task && !t.depends_on.is_empty())
            );
            if needs_another {
                let verdict = verdict_of(&results, &attempt, &digest);
                evidence.verdicts.insert((attempt, digest), verdict);
            }
        }
        evidence
    }
}

/// The result of the attempt: whether it ran a step.
fn verdict_of(results: &std::path::Path, attempt: &str, digest: &str) -> Verdict {
    let (Ok(attempt), Ok(digest)) = (AttemptId::parse(attempt), Digest::parse(digest)) else {
        return Verdict::Unavailable;
    };
    match crate::ci::advance::read_result(results, &attempt, &digest) {
        Ok(r)
            if !r.outcomes.is_empty()
                && r.outcomes
                    .iter()
                    .all(|o| o.status == local_ci::task::JobStatus::Skipped) =>
        {
            Verdict::SkippedOnly
        }
        Ok(_) => Verdict::Ran,
        Err(_) => Verdict::Unavailable,
    }
}

/// The verdict of the attempt's result, from what was read before the
/// transaction. A result the retry was not prepared with means the run changed.
fn verdict(c: &Connection, evidence: &Evidence, attempt: &str) -> Result<Verdict> {
    let digest: Option<String> = c
        .query_row(
            "SELECT digest FROM artifact_receipt WHERE attempt_id=?1 AND name=?2 AND persisted_at_ms IS NOT NULL",
            params![attempt, crate::supervisor::ci_task::RESULT_ARTIFACT],
            |r| r.get(0),
        )
        .optional()?;
    let Some(digest) = digest else {
        return Ok(Verdict::Unavailable);
    };
    evidence
        .verdicts
        .get(&(attempt.to_string(), digest))
        .copied()
        .ok_or_else(|| StorageError::Immutable("retry task snapshot changed".into()))
}

/// A task of the plan that needs a retried one, as the run holds it.
struct Dependent {
    task: String,
    held: bool,
    attempt: Option<String>,
    generation: Option<i64>,
    state: Option<String>,
    group: Option<String>,
    shard_retry_forbidden: bool,
    /// It was cancelled before it had an attempt, with a reason (its input
    /// could not be made, or fail-fast): it never ran.
    stopped: bool,
}

/// What a retry does to the jobs of the plan that need a retried job: none
/// may keep the results of its earlier attempt without a word.
/// - One that did not start waits again (it is held) and is given the new
///   results when the retried job ends. One that was cancelled before it had
///   an attempt (with a reason) is let in again, as the retry of a task
///   fail-fast cancelled is.
/// - One that ended without running a step (every job of it was skipped)
///   did not use those results: it is opened again with the retry (its
///   generation is raised and it is held), and the progression decides anew
///   whether it runs. The jobs that need it are looked at the same way.
/// - One that ran (running, or ended after its steps) is a refusal when one
///   job was selected (retry it too, or the run). A retry of the run keeps it.
///   So is one whose result can no longer be read: it is not known not to
///   have run.
///
/// `retried` names the tasks of the plan that run again, by plan digest and
/// task. Returns the tasks of the run opened again, with the generation of
/// their last attempt (to count and to hash). The tasks the main scan skips
/// (discovered or dynamic ones) are not looked at, and a shard opened again
/// needs `allow_retry` as for a retry of the shard itself.
fn reopen_dependents(
    c: &Connection,
    binding: &Binding,
    retried: &[(String, String)],
    authorize: Option<u64>,
    evidence: &Evidence,
) -> Result<Vec<(String, i64)>> {
    let mut reopened = Vec::new();
    let Some((digest, _)) = retried.first() else {
        return Ok(reopened);
    };
    let plan = match evidence.plans.get(digest) {
        Some(Ok(plan)) => plan,
        Some(Err(why)) => return Err(StorageError::Immutable(why.clone())),
        None => {
            return Err(StorageError::Immutable(
                "retry task snapshot changed".into(),
            ))
        }
    };
    let mut seen: std::collections::BTreeSet<String> =
        retried.iter().map(|(_, task)| task.clone()).collect();
    let mut queue: Vec<String> = seen.iter().cloned().collect();
    let mut groups = std::collections::BTreeSet::new();
    while let Some(of) = queue.pop() {
        for dependent in plan.tasks.iter().filter(|t| t.depends_on.contains(&of)) {
            if !seen.insert(dependent.id.clone()) {
                continue;
            }
            let rows = {
                let mut stmt = c.prepare(&format!(
                    "SELECT t.task_id,coalesce(json_extract(t.spec_json,'$.ci.hold'),0),a.attempt_id,a.generation,a.state,
                     coalesce(json_extract(t.spec_json,'$.ci.shard_group'),json_extract(t.spec_json,'$.ci.matrix.group')),
                     coalesce(json_extract(t.spec_json,'$.ci.shard_retry_forbidden'),0),
                     t.cancel_requested_at_ms IS NOT NULL AND json_extract(t.spec_json,'$.ci.unrunnable') IS NOT NULL
                     FROM task t LEFT JOIN attempt a ON a.attempt_id=({LIVE_ATTEMPT})
                     WHERE t.run_id=?1 AND json_extract(t.spec_json,'$.ci.task.task_id')=?2
                     AND coalesce(json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery.applied'),0)=0
                     AND coalesce(json_extract(t.spec_json,'$.ci.dynamic_origin'),0)=0"
                ))?;
                let rows = stmt
                    .query_map(params![binding.run.as_str(), dependent.id], |r| {
                        Ok(Dependent {
                            task: r.get(0)?,
                            held: r.get(1)?,
                            attempt: r.get(2)?,
                            generation: r.get(3)?,
                            state: r.get(4)?,
                            group: r.get(5)?,
                            shard_retry_forbidden: r.get(6)?,
                            stopped: r.get(7)?,
                        })
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            };
            let mut go_on = false;
            for row in rows {
                let Dependent {
                    task,
                    held,
                    attempt,
                    generation,
                    state,
                    group,
                    shard_retry_forbidden,
                    stopped,
                } = row;
                let (Some(attempt), Some(generation)) = (attempt, generation) else {
                    // It did not start: it waits for the new results.
                    if stopped {
                        if shard_retry_forbidden {
                            return Err(StorageError::Immutable(SHARD_FORBIDDEN.into()));
                        }
                        reopened.push((task.clone(), -1));
                        if let Some(revision) = authorize {
                            c.execute(
                                "UPDATE task SET authorization_revision=?1,cancel_requested_at_ms=NULL,
                                 spec_json=json_set(json_remove(spec_json,'$.ci.unrunnable'),'$.ci.hold',json('true')) WHERE task_id=?2",
                                params![to_i64(revision)?, task],
                            )?;
                            groups.extend(group);
                        }
                    } else if !held && authorize.is_some() {
                        c.execute(
                            "UPDATE task SET spec_json=json_set(spec_json,'$.ci.hold',json('true')) WHERE task_id=?1",
                            [&task],
                        )?;
                    }
                    go_on = true;
                    continue;
                };
                let verdict = if state.as_deref() == Some("terminal") {
                    verdict(c, evidence, &attempt)?
                } else {
                    Verdict::Ran
                };
                if verdict == Verdict::SkippedOnly {
                    if shard_retry_forbidden {
                        return Err(StorageError::Immutable(SHARD_FORBIDDEN.into()));
                    }
                    reopened.push((task.clone(), generation));
                    if let Some(revision) = authorize {
                        c.execute(
                            "UPDATE task SET min_generation=?1,authorization_revision=?2,cancel_requested_at_ms=NULL,
                             spec_json=json_set(json_remove(spec_json,'$.ci.unrunnable'),'$.ci.hold',json('true')) WHERE task_id=?3",
                            params![generation + 1, to_i64(revision)?, task],
                        )?;
                        groups.extend(group);
                    }
                    go_on = true;
                } else if let Some(selected) = binding.selected.as_deref() {
                    let end = if verdict == Verdict::Unavailable {
                        "its result is no longer available; retry the run".to_string()
                    } else {
                        format!("retry {task} as well or retry the run")
                    };
                    return Err(StorageError::Immutable(format!(
                        "retry of {selected} would leave {task} ({}) with the results of the earlier attempt; {end}",
                        dependent.id
                    )));
                }
            }
            if go_on {
                queue.push(dependent.id.clone());
            }
        }
    }
    for group in &groups {
        unjoin(c, &binding.run, group)?;
    }
    reopened.sort();
    Ok(reopened)
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
fn scan(
    c: &Connection,
    binding: &Binding,
    authorize: Option<u64>,
    evidence: &Evidence,
) -> Result<Scan> {
    let mut after = String::new();
    let mut count = 0_u64;
    let mut boundary = None;
    let mut secret = None;
    let mut hash = Sha256::new();
    // The split jobs a shard of which runs again.
    let mut unjoined = std::collections::BTreeSet::new();
    // The tasks of the plan that run again, not shards.
    let mut retried: Vec<(String, String)> = Vec::new();
    loop {
        let page = {
            let mut stmt = c.prepare(
                "SELECT t.task_id,t.min_generation,a.generation,a.state,a.observation,a.termination_json,a.retired,
                 EXISTS(SELECT 1 FROM attempt x LEFT JOIN retire_tombstone z USING(attempt_id) WHERE x.task_id=t.task_id AND x.retired=0 AND (x.state<>'terminal' OR (z.attempt_id IS NOT NULL AND z.confirmed_at_ms IS NULL))),
                 json_extract(t.spec_json,'$.manifest_digest'),json_extract(t.spec_json,'$.has_secret_bundle'),coalesce(json_extract(t.spec_json,'$.in_place'),0),
                 t.cancel_requested_at_ms IS NOT NULL AND coalesce(json_extract(t.spec_json,'$.ci.unrunnable'),'') IN (?5,?6),
                 coalesce(json_extract(t.spec_json,'$.ci.rerun'),0),coalesce(json_extract(t.spec_json,'$.ci.shard_group'),json_extract(t.spec_json,'$.ci.matrix.group')),coalesce(json_extract(t.spec_json,'$.ci.shard_retry_forbidden'),0),json_extract(t.spec_json,'$.ci.task.plan_digest'),json_extract(t.spec_json,'$.ci.task.task_id')
                 FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id ORDER BY aa.generation DESC LIMIT 1)
                 WHERE t.run_id=?1 AND coalesce(json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery.applied'),0)=0 AND coalesce(json_extract(t.spec_json,'$.ci.dynamic_origin'),0)=0 AND (?2 IS NULL OR t.task_id=?2) AND t.task_id>?3 ORDER BY t.task_id LIMIT ?4",
            )?;
            let rows = stmt.query_map(
                params![
                    binding.run.as_str(),
                    binding.selected,
                    after,
                    PAGE,
                    crate::ci::shards::FAIL_FAST_REASON,
                    crate::ci::advance::MATRIX_FAIL_FAST_REASON
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
                        plan_task: r
                            .get::<_, Option<String>>(15)?
                            .zip(r.get::<_, Option<String>>(16)?),
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
                return Err(StorageError::Immutable(SHARD_FORBIDDEN.into()));
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
                c.execute("UPDATE task SET min_generation=?1,authorization_revision=?2,cancel_requested_at_ms=NULL,spec_json=json_remove(spec_json,'$.ci.unrunnable') WHERE task_id=?3",
                    params![next, to_i64(revision)?, row.task])?;
                unjoined.extend(row.shard_group.clone());
            }
            if row.shard_group.is_none() {
                retried.extend(row.plan_task);
            }
        }
    }
    if let Some(revision) = authorize {
        c.execute("UPDATE task SET spec_json=json_remove(spec_json,'$.ci.core_outcome') WHERE run_id=?1 AND json_extract(spec_json,'$.ci.dynamic_origin')=1", [binding.run.as_str()])?;
        // Rebind predecessor results before a retried dependent may run.
        // The matrix driver additionally claims every task, including roots.
        c.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.hold',json('true'))
            WHERE run_id=?1 AND authorization_revision=?2 AND (
                EXISTS(SELECT 1 FROM json_each(task.spec_json,'$.ci.task.inputs.needs')) OR EXISTS(
                SELECT 1 FROM task m WHERE m.run_id=?1 AND (json_type(m.spec_json,'$.ci.matrix')='object' OR json_extract(m.spec_json,'$.ci.dynamic_origin')=1)))",
            params![binding.run.as_str(),to_i64(revision)?])?;
    }
    for group in &unjoined {
        unjoin(c, &binding.run, group)?;
    }
    if count > 0 {
        // The jobs opened again are counted and hashed in both modes: they are
        // part of what the retry changes.
        for (task, generation) in reopen_dependents(c, binding, &retried, authorize, evidence)? {
            for value in [task.as_bytes(), b"reopened"] {
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value);
            }
            hash.update(generation.to_be_bytes());
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::Invalid("task count exhausted".into()))?;
        }
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
