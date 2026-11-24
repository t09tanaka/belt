//! Every fresh control intent advances the run fence; retransmissions replay metadata.
use super::*;
use crate::service::completion::{Snapshot, Target};
use serde::de::DeserializeOwned;

impl CoordinatorStore {
    /// A fresh internal invocation, never a transport retransmission.
    pub fn cancel_run(&self, run: &RunId, at_ms: u64) -> Result<()> {
        let target = Target::Run(run.clone());
        self.db.write(move |tx| {
            let (run, _) = resolve(tx, &target)?;
            let expected = revision(tx, &run)?;
            apply_cancel(
                tx,
                target,
                expected,
                RunId::generate().to_string(),
                at_ms,
                false,
            )
            .map(|_| ())
        })
    }

    /// [`Self::cancel_run`] under the premise of
    /// [`Self::cancel_control_of_capture`]: the capture of the run is in
    /// progress.
    #[cfg(test)]
    pub(crate) fn cancel_run_of_capture(&self, run: &RunId, at_ms: u64) -> Result<()> {
        let target = Target::Run(run.clone());
        self.db.write(move |tx| {
            let (run, _) = resolve(tx, &target)?;
            let expected = revision(tx, &run)?;
            apply_cancel(
                tx,
                target,
                expected,
                RunId::generate().to_string(),
                at_ms,
                true,
            )
            .map(|_| ())
        })
    }

    /// A fresh internal invocation; worker delivery uses request_cancel instead.
    pub fn cancel_task(&self, task_id: &TaskId, at_ms: u64) -> Result<Option<AttemptRecord>> {
        let target = Target::Task(task_id.clone());
        let task = task_id.to_string();
        self.db.write(move |tx| {
            let (run, _) = resolve(tx, &target)?;
            let expected = revision(tx, &run)?;
            apply_cancel(
                tx,
                target,
                expected,
                RunId::generate().to_string(),
                at_ms,
                false,
            )?;
            let live: Option<String> = tx.query_row(
                "SELECT attempt_id FROM attempt WHERE task_id=?1 AND state<>'terminal' AND retired=0",
                [&task], |row| row.get(0),
            ).optional()?;
            live.map(|id| load_attempt(tx, &id)?.ok_or_else(|| not_found(format!("attempt {id}")))).transpose()
        })
    }

    /// Bind a fail-fast decision to the live attempt seen by the CI pass.
    /// Concurrent passes, user cancellation and a terminal/retried attempt
    /// cannot acquire a second or incorrect reason.
    pub(crate) fn stop_ci_attempt(
        &self,
        run: &RunId,
        task: &TaskId,
        attempt: &AttemptId,
        reason: &str,
        at_ms: u64,
    ) -> Result<bool> {
        let (run, task, attempt, reason) = (
            run.to_string(),
            task.clone(),
            attempt.to_string(),
            reason.to_owned(),
        );
        self.db.write(move |tx| {
            let live: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM task t JOIN attempt a USING(task_id)
                 WHERE t.run_id=?1 AND t.task_id=?2 AND a.attempt_id=?3
                 AND t.cancel_requested_at_ms IS NULL AND a.generation>=t.min_generation
                 AND a.state<>'terminal' AND a.retired=0)",
                params![run, task.as_str(), attempt],
                |r| r.get(0),
            )?;
            if !live {
                return Ok(false);
            }
            let expected = revision(tx, &run)?;
            apply_cancel(
                tx,
                Target::Task(task.clone()),
                expected,
                RunId::generate().to_string(),
                at_ms,
                false,
            )?;
            tx.execute(
                "UPDATE task SET spec_json=json_set(spec_json,'$.ci.unrunnable',?2) WHERE task_id=?1",
                params![task.as_str(), reason],
            )?;
            common::append_event(
                tx,
                Some(&run),
                Some(&attempt),
                "ci_fail_fast_requested",
                &serde_json::json!({"task": task, "reason": reason}),
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// The CI tasks of `run` that were never placed (only `only`, when one
    /// is named) cannot be run: write why into each (`ci.unrunnable` of its
    /// spec), cancel it, and record the event `ci_task_unrunnable`, all in
    /// one transaction. A task that has an attempt, or that a cancel was
    /// requested for, is left as it is. Returns the tasks this cancelled:
    /// none when there was nothing left to do, so a caller that sees the
    /// same fault again writes nothing more.
    pub fn fail_ci_tasks(
        &self,
        run: &RunId,
        only: Option<&TaskId>,
        reason: &str,
        at_ms: u64,
    ) -> Result<Vec<TaskId>> {
        let (run, only, reason) = (
            run.to_string(),
            only.map(|task| task.to_string()),
            reason.to_string(),
        );
        self.db
            .write(move |tx| fail_ci_tasks_on(tx, &run, only.as_deref(), &reason, at_ms))
    }
}

/// [`CoordinatorStore::fail_ci_tasks`] in the transaction `tx`.
pub(super) fn fail_ci_tasks_on(
    tx: &Connection,
    run: &str,
    only: Option<&str>,
    reason: &str,
    at_ms: u64,
) -> Result<Vec<TaskId>> {
    let tasks: Vec<String> = {
        let mut query = tx.prepare(
            "SELECT t.task_id FROM task t WHERE t.run_id=?1 AND (?2 IS NULL OR t.task_id=?2)
             AND t.cancel_requested_at_ms IS NULL
             AND json_type(t.spec_json,'$.ci')='object' AND json_extract(t.spec_json,'$.ci.core_outcome') IS NULL
             AND NOT EXISTS (SELECT 1 FROM attempt a WHERE a.task_id=t.task_id AND a.retired=0 AND (a.generation>=t.min_generation OR a.state<>'terminal'))
             ORDER BY t.created_at_ms,t.rowid",
        )?;
        let rows = query.query_map(params![run, only], |row| row.get(0))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    let mut failed = Vec::new();
    for task in tasks {
        tx.execute(
            "UPDATE task SET spec_json=json_set(spec_json,'$.ci.unrunnable',?2) WHERE task_id=?1",
            params![task, reason],
        )?;
        let id = TaskId::parse(&task).map_err(|e| StorageError::Invalid(e.to_string()))?;
        let expected = revision(tx, run)?;
        apply_cancel(
            tx,
            Target::Task(id.clone()),
            expected,
            RunId::generate().to_string(),
            at_ms,
            false,
        )?;
        common::append_event(
            tx,
            Some(run),
            None,
            "ci_task_unrunnable",
            &serde_json::json!({ "task": task, "reason": reason }),
            at_ms,
        )?;
        failed.push(id);
    }
    Ok(failed)
}

impl CoordinatorStore {
    /// A transport request retains its original identity and expected revision.
    /// A run that has no row is refused (`NOT_FOUND`) and nothing is written
    /// (spec 25.4 (b)); the cancel of a capture in progress is
    /// [`Self::cancel_control_of_capture`].
    pub fn cancel_control(
        &self,
        target: Target,
        expected: u64,
        request_id: String,
        at_ms: u64,
    ) -> Result<Snapshot> {
        self.db
            .write(move |tx| apply_cancel(tx, target, expected, request_id, at_ms, false))
    }

    /// [`Self::cancel_control`] for a run whose capture is in progress: the
    /// caller holds the source guard and has read, under it, that the run's
    /// `capture_owner` row is `capturing` and its owner is alive here. Only
    /// then may the cancel of a run without a row be recorded (the enqueue
    /// that follows inherits it; if none follows, the abandonment of the
    /// capture deletes the rows). The row of the run is looked for again in
    /// this transaction: a run that has one is cancelled as any other.
    pub fn cancel_control_of_capture(
        &self,
        target: Target,
        expected: u64,
        request_id: String,
        at_ms: u64,
    ) -> Result<Snapshot> {
        self.db
            .write(move |tx| apply_cancel(tx, target, expected, request_id, at_ms, true))
    }

    /// Whether the run has a row. A marked run (spec 25.3) has one.
    pub fn has_run(&self, run: &RunId) -> Result<bool> {
        let run = run.to_string();
        self.db.call(move |c| run_exists(c, &run))
    }

    /// The parent rows of a run that was never submitted (spec 25.4 (b),
    /// "消すもの" 1): its cancel intent, the receipts of its cancel requests
    /// and its events. The caller holds the source lock exclusively, so no
    /// enqueue of the run commits meanwhile. Returns `true`, and deletes
    /// nothing, when the run has a row: then these rows are the run's.
    /// Each transaction looks for the row again and deletes at most 1,000
    /// rows of a table.
    pub(crate) fn forget_unsubmitted_run(&self, run: &RunId) -> Result<bool> {
        let run = run.to_string();
        loop {
            let run = run.clone();
            let (exists, more) = self.db.write(move |tx| {
                if run_exists(tx, &run)? {
                    return Ok((true, false));
                }
                tx.execute("DELETE FROM run_cancel_intent WHERE run_id=?1", [&run])?;
                let receipts = tx.execute(
                    "DELETE FROM control_request WHERE request_id IN (SELECT request_id FROM control_request WHERE run_id=?1 LIMIT 1000)",
                    [&run],
                )?;
                let events = tx.execute(
                    "DELETE FROM event WHERE seq IN (SELECT seq FROM event WHERE run_id=?1 ORDER BY seq LIMIT 1000)",
                    [&run],
                )?;
                Ok((false, receipts == 1000 || events == 1000))
            })?;
            if exists || !more {
                return Ok(exists);
            }
        }
    }

    pub fn completion(&self, target: Target) -> Result<Snapshot> {
        let (cancel, collections, retry) =
            (self.task_cancel, self.collections, self.retry_boundaries);
        let pruning = self.pruning;
        self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            // The mark and the rows of the answer are one snapshot.
            if pruning {
                refuse_pruned_target(&tx, &target)?;
            }
            completion_on(&tx, target, cancel, collections, retry)
        })
    }
}

fn run_exists(c: &Connection, run: &str) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",
        [run],
        |row| row.get(0),
    )?)
}

/// `capturing` is true only for the cancel of a capture in progress (spec
/// 25.4 (b)): every other cancel of a run without a row is `NOT_FOUND`, and
/// writes no intent, no event and no receipt.
fn apply_cancel(
    c: &Connection,
    target: Target,
    expected: u64,
    request_id: String,
    at_ms: u64,
    capturing: bool,
) -> Result<Snapshot> {
    check_request_id(&request_id)?;
    refuse_pruned_target(c, &target)?;
    if let Some(snapshot) = control_receipt(c, &target, expected, &request_id, "cancel")? {
        return Ok(snapshot);
    }
    let (run, task) = resolve(c, &target)?;
    if !capturing && !run_exists(c, &run)? {
        return Err(no_run(&run));
    }
    if revision(c, &run)? != expected {
        return Err(StorageError::Immutable("stale control revision".into()));
    }
    let next = expected
        .checked_add(1)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or_else(|| StorageError::Invalid("control revision exhausted".into()))?;
    let at = to_i64(at_ms)?;
    c.execute(
        "UPDATE run SET control_revision=?1 WHERE run_id=?2 AND control_revision=?3",
        params![to_i64(next)?, run, to_i64(expected)?],
    )?;
    if task.is_none() {
        c.execute("INSERT INTO run_cancel_intent(run_id,requested_at_ms,control_revision) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET control_revision=excluded.control_revision",
            params![run, at, to_i64(next)?])?;
        common::append_event(
            c,
            Some(&run),
            None,
            "run_cancel_requested",
            &Value::Null,
            at_ms,
        )?;
    }
    c.execute("UPDATE task SET cancel_requested_at_ms=COALESCE(cancel_requested_at_ms,?1) WHERE run_id=?2 AND (?3 IS NULL OR task_id=?3)",
        params![at, run, task])?;
    c.execute("INSERT INTO event(run_id,attempt_id,attempt_seq,kind,payload_json,at_ms) SELECT ?1,a.attempt_id,(SELECT COALESCE(MAX(e.attempt_seq),0)+1 FROM event e WHERE e.attempt_id=a.attempt_id),'cancel_requested','null',?2 FROM attempt a JOIN task t USING(task_id) WHERE t.run_id=?1 AND (?3 IS NULL OR t.task_id=?3) AND a.state<>'terminal' AND a.retired=0 AND a.cancel_requested_at_ms IS NULL",
        params![run, at, task])?;
    c.execute("UPDATE attempt SET cancel_requested_at_ms=?1 WHERE task_id IN (SELECT task_id FROM task WHERE run_id=?2 AND (?3 IS NULL OR task_id=?3)) AND state<>'terminal' AND retired=0 AND cancel_requested_at_ms IS NULL",
        params![at, run, task])?;
    let snapshot = completion_on(c, target.clone(), true, true, true)?;
    c.execute("INSERT INTO control_request(request_id,operation,target_json,expected_revision,reply_json,run_id) VALUES(?1,'cancel',?2,?3,?4,?5)",
        params![request_id, json(&target), to_i64(expected)?, json(&snapshot), run])?;
    Ok(snapshot)
}

fn check_request_id(request_id: &str) -> Result<()> {
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(StorageError::Invalid("invalid control request ID".into()));
    }
    Ok(())
}

/// Refuse a control request whose run carries the mark of a pruned run
/// (spec 25.3 (b)). Every caller reads this before [`control_receipt`]: the
/// receipt of a request is deleted with its run, so a retransmission to a
/// marked run is answered `run_pruned` while the receipt is still there,
/// and not the stored answer first and "not found" later. A target without
/// a row carries no mark and is answered as before.
pub(super) fn refuse_pruned_target(c: &Connection, target: &Target) -> Result<()> {
    match target {
        Target::Run(run) => refuse_pruned(c, run.as_str()),
        Target::Task(task) => refuse_pruned_task(c, task.as_str()),
    }
}

pub(super) fn control_receipt<T: DeserializeOwned>(
    c: &Connection,
    target: &Target,
    expected: u64,
    request_id: &str,
    operation: &str,
) -> Result<Option<T>> {
    check_request_id(request_id)?;
    let previous: Option<(String, String, i64, String)> = c.query_row(
        "SELECT operation,target_json,expected_revision,reply_json FROM control_request WHERE request_id=?1",
        [request_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional()?;
    previous
        .map(|(old_operation, old_target, old_revision, reply)| {
            if old_operation != operation
                || old_target != json(target)
                || to_u64(old_revision)? != expected
            {
                return Err(StorageError::Immutable(
                    "control request ID was reused".into(),
                ));
            }
            from_json("control receipt", &reply)
        })
        .transpose()
}

fn completion_on(
    c: &Connection,
    target: Target,
    task_cancel: bool,
    collections: bool,
    retry: bool,
) -> Result<Snapshot> {
    let has_intents: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='run_cancel_intent')", [], |row| row.get(0))?;
    let (run, task) = resolve(c, &target)?;
    let accepted: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",
        [&run],
        |row| row.get(0),
    )?;
    // When the run was cancelled as a whole, if it was.
    let cancelled_at: Option<i64> = if has_intents {
        c.query_row(
            "SELECT requested_at_ms FROM run_cancel_intent WHERE run_id=?1",
            [&run],
            |row| row.get(0),
        )
        .optional()?
    } else {
        None
    };
    let cancelled = cancelled_at.is_some();
    if !accepted && !cancelled {
        return Err(no_run(&run));
    }
    let mut snapshot = Snapshot::new(
        target,
        accepted,
        if accepted && retry { false } else { cancelled },
    );
    if retry {
        snapshot.control_revision = revision(c, &run)?;
    }
    let boundary = if retry {
        "AND aa.generation>=t.min_generation"
    } else {
        ""
    };
    let cancel = if task_cancel {
        "t.cancel_requested_at_ms IS NOT NULL"
    } else {
        "0"
    };
    let cancel_at = if task_cancel {
        "t.cancel_requested_at_ms"
    } else {
        "NULL"
    };
    let (join, verdict) = if collections {
        (
            "LEFT JOIN collection x ON x.attempt_id=a.attempt_id",
            "CASE WHEN x.closed_at_ms IS NOT NULL THEN x.verdict ELSE NULL END",
        )
    } else {
        ("", "NULL")
    };
    let sql = format!("SELECT t.task_id,{cancel},a.state,a.observation,a.termination_json,{verdict},coalesce(json_extract(t.spec_json,'$.in_place'),0),coalesce(json_extract(t.spec_json,'$.ci.shard_group'),json_extract(t.spec_json,'$.ci.matrix.group')) IS NOT NULL AND coalesce(json_extract(t.spec_json,'$.ci.joined'),0)=0,json_extract(t.spec_json,'$.ci.join_failure') IS NOT NULL,coalesce(json_extract(t.spec_json,'$.ci.shard_group'),json_extract(t.spec_json,'$.ci.matrix.group')) IS NOT NULL,{cancel_at},coalesce(json_extract(t.spec_json,'$.ci.core_outcome'),CASE WHEN json_extract(t.spec_json,'$.ci.unplaceable') IS NOT NULL AND a.attempt_id IS NULL THEN 'failure' END) FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id AND aa.retired=0 {boundary} ORDER BY aa.generation DESC LIMIT 1) {join} WHERE t.run_id=?1 AND (?2 IS NOT NULL OR coalesce(json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery.applied'),0)=0) AND (?2 IS NULL OR t.task_id=?2) ORDER BY t.task_id");
    let mut stmt = c.prepare(&sql)?;
    // The first shard of a split job that is a failure (see below).
    let mut job_failed = None;
    // Whether a shard was cancelled (stopped by fail-fast), the tasks that
    // were cancelled, and the first task that failed of itself: unless a
    // cancel of the whole run stopped a task, a run with a stopped shard failed of the
    // latter, wherever the ids of the cancelled ones sort. A task of another
    // job that was cancelled did not fail of itself either.
    let mut stopped_shard = false;
    let mut cancelled_tasks = std::collections::BTreeSet::new();
    let mut real_failure = None;
    // A run that was cancelled as a whole while a shard of it was still to
    // end is cancelled, whatever a task of it had failed of before: whether
    // that cancel stopped a shard, and the first task it stopped. A shard
    // fail-fast had stopped before, or a cancel that came when everything
    // had ended, changes nothing.
    let mut run_stopped_shard = false;
    let mut first_run_stopped = None;
    let whole_run = task.is_none();
    let mut rows = stmt.query(params![run, task])?;
    while let Some(row) = rows.next()? {
        let task = id("task_id", TaskId::parse(&row.get::<_, String>(0)?))?;
        if let Some(status) = row.get::<_, Option<String>>(11)? {
            snapshot.task_count += 1;
            snapshot.computed_tasks += 1;
            if status == "failure" {
                snapshot.job_failure(task, None);
            } else if status == "cancelled" {
                snapshot.cancel_requested = true;
                snapshot
                    .first_compute_failure
                    .get_or_insert(crate::service::completion::Failure {
                        task_id: task,
                        code: crate::error::ErrorCode::CommandFailed,
                        exit_code: crate::error::exit::CANCELLED,
                        termination: None,
                    });
            }
            continue;
        }
        snapshot.in_place |= row.get::<_, bool>(6)?;
        let cancel: bool = row.get(1)?;
        let state: Option<String> = row.get(2)?;
        let observation: Option<String> = row.get(3)?;
        let termination: Option<String> = row.get(4)?;
        let collection: Option<String> = row.get(5)?;
        let state = state.map(|value| common::parse_state(&value)).transpose()?;
        let observation = observation
            .map(|value| {
                serde_json::from_value(serde_json::json!(value))
                    .map_err(|e| StorageError::Corrupt(e.to_string()))
            })
            .transpose()?;
        let termination: Option<Termination> = termination
            .map(|value| from_json("termination", &value))
            .transpose()?;
        if collection
            .as_deref()
            .is_some_and(|value| !matches!(value, "complete" | "required_missing" | "gave_up"))
        {
            return Err(StorageError::Corrupt("invalid collection verdict".into()));
        }
        snapshot.cancel_requested |= cancel;
        // A shard whose split job has no outcome yet is not done with: the
        // run is settled only when the job is joined (the progression of
        // the run does that once every shard ended). A job that is joined
        // as a failure makes the run one.
        let unjoined: bool = row.get(7)?;
        if whole_run && row.get::<_, bool>(8)? && job_failed.is_none() {
            job_failed = Some((task.clone(), termination));
        }
        let shard: bool = row.get(9)?;
        let was_cancelled = match (&state, &termination) {
            (None, _) => cancel,
            (_, Some(t)) => t.outcome() == crate::state::Outcome::Cancelled,
            _ => false,
        };
        let cancel_at: Option<i64> = row.get(10)?;
        let by_run_cancel =
            was_cancelled && cancelled_at.is_some_and(|run| cancel_at.is_some_and(|at| at >= run));
        if whole_run && by_run_cancel {
            run_stopped_shard |= shard;
            if first_run_stopped.is_none() {
                first_run_stopped = Some((task.clone(), termination));
            }
        }
        if was_cancelled {
            stopped_shard |= shard;
            cancelled_tasks.insert(task.clone());
        } else if real_failure.is_none() {
            let failed =
                termination.filter(|t| crate::service::completion::compute_failure(*t).is_some());
            real_failure = failed.map(|t| (task.clone(), t));
        }
        let collection = collection.as_deref().filter(|_| !(whole_run && unjoined));
        snapshot.task(task, state, observation, termination, cancel, collection);
    }
    let first_was_cancelled = snapshot
        .first_compute_failure
        .as_ref()
        .is_some_and(|f| cancelled_tasks.contains(&f.task_id));
    // A cancel of the whole run that stopped nothing is as none here.
    if whole_run && first_run_stopped.is_none() && stopped_shard && first_was_cancelled {
        match real_failure {
            Some((task, termination)) => snapshot.prefer_failure(task, termination),
            // No attempt failed of itself: the job did (its tests).
            None if job_failed.is_some() => snapshot.first_compute_failure = None,
            None => {}
        }
    }
    if let Some((task_id, termination)) = first_run_stopped.filter(|_| run_stopped_shard) {
        snapshot.first_compute_failure = Some(crate::service::completion::Failure {
            task_id,
            code: crate::error::ErrorCode::CommandFailed,
            exit_code: crate::error::exit::CANCELLED,
            termination,
        });
    }
    if let Some((task, termination)) = job_failed {
        snapshot.job_failure(task, termination);
    }
    if snapshot.task_count == 0 && cancelled {
        snapshot.cancel_requested = true;
    }
    Ok(snapshot)
}

pub(super) fn resolve(c: &Connection, target: &Target) -> Result<(String, Option<String>)> {
    match target {
        Target::Run(run) => Ok((run.to_string(), None)),
        Target::Task(task) => {
            let run: Option<String> = c
                .query_row(
                    "SELECT run_id FROM task WHERE task_id=?1",
                    [task.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            Ok((
                run.ok_or_else(|| not_found(format!("task {task}")))?,
                Some(task.to_string()),
            ))
        }
    }
}
pub(super) fn revision(c: &Connection, run: &str) -> Result<u64> {
    let value: i64 = c.query_row("SELECT COALESCE((SELECT control_revision FROM run WHERE run_id=?1),(SELECT control_revision FROM run_cancel_intent WHERE run_id=?1),0)", [run], |row| row.get(0))?;
    to_u64(value)
}
