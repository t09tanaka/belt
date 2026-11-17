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
            apply_cancel(tx, target, expected, RunId::generate().to_string(), at_ms).map(|_| ())
        })
    }

    /// A fresh internal invocation; worker delivery uses request_cancel instead.
    pub fn cancel_task(&self, task_id: &TaskId, at_ms: u64) -> Result<Option<AttemptRecord>> {
        let target = Target::Task(task_id.clone());
        let task = task_id.to_string();
        self.db.write(move |tx| {
            let (run, _) = resolve(tx, &target)?;
            let expected = revision(tx, &run)?;
            apply_cancel(tx, target, expected, RunId::generate().to_string(), at_ms)?;
            let live: Option<String> = tx.query_row(
                "SELECT attempt_id FROM attempt WHERE task_id=?1 AND state<>'terminal' AND retired=0",
                [&task], |row| row.get(0),
            ).optional()?;
            live.map(|id| load_attempt(tx, &id)?.ok_or_else(|| not_found(format!("attempt {id}")))).transpose()
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
        self.db.write(move |tx| {
            let tasks: Vec<String> = {
                let mut query = tx.prepare(
                    "SELECT t.task_id FROM task t WHERE t.run_id=?1 AND (?2 IS NULL OR t.task_id=?2)
                     AND t.cancel_requested_at_ms IS NULL
                     AND json_type(t.spec_json,'$.ci')='object'
                     AND NOT EXISTS (SELECT 1 FROM attempt a WHERE a.task_id=t.task_id)
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
                let expected = revision(tx, &run)?;
                apply_cancel(
                    tx,
                    Target::Task(id.clone()),
                    expected,
                    RunId::generate().to_string(),
                    at_ms,
                )?;
                common::append_event(
                    tx,
                    Some(&run),
                    None,
                    "ci_task_unrunnable",
                    &serde_json::json!({ "task": task, "reason": reason }),
                    at_ms,
                )?;
                failed.push(id);
            }
            Ok(failed)
        })
    }

    /// A transport request retains its original identity and expected revision.
    pub fn cancel_control(
        &self,
        target: Target,
        expected: u64,
        request_id: String,
        at_ms: u64,
    ) -> Result<Snapshot> {
        self.db
            .write(move |tx| apply_cancel(tx, target, expected, request_id, at_ms))
    }

    pub fn completion(&self, target: Target) -> Result<Snapshot> {
        let (cancel, collections, retry) =
            (self.task_cancel, self.collections, self.retry_boundaries);
        self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            completion_on(&tx, target, cancel, collections, retry)
        })
    }
}

fn apply_cancel(
    c: &Connection,
    target: Target,
    expected: u64,
    request_id: String,
    at_ms: u64,
) -> Result<Snapshot> {
    if let Some(snapshot) = control_receipt(c, &target, expected, &request_id, "cancel")? {
        return Ok(snapshot);
    }
    let (run, task) = resolve(c, &target)?;
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
    c.execute("INSERT INTO control_request(request_id,operation,target_json,expected_revision,reply_json) VALUES(?1,'cancel',?2,?3,?4)",
        params![request_id, json(&target), to_i64(expected)?, json(&snapshot)])?;
    Ok(snapshot)
}

pub(super) fn control_receipt<T: DeserializeOwned>(
    c: &Connection,
    target: &Target,
    expected: u64,
    request_id: &str,
    operation: &str,
) -> Result<Option<T>> {
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(StorageError::Invalid("invalid control request ID".into()));
    }
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
    let cancelled = if has_intents {
        c.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=?1)",
            [&run],
            |row| row.get::<_, bool>(0),
        )?
    } else {
        false
    };
    if !accepted && !cancelled {
        return Err(not_found(format!("run {run}")));
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
    let (join, verdict) = if collections {
        (
            "LEFT JOIN collection x ON x.attempt_id=a.attempt_id",
            "CASE WHEN x.closed_at_ms IS NOT NULL THEN x.verdict ELSE NULL END",
        )
    } else {
        ("", "NULL")
    };
    let sql = format!("SELECT t.task_id,{cancel},a.state,a.observation,a.termination_json,{verdict},coalesce(json_extract(t.spec_json,'$.in_place'),0),json_extract(t.spec_json,'$.ci.shard_group') IS NOT NULL AND coalesce(json_extract(t.spec_json,'$.ci.joined'),0)=0,json_extract(t.spec_json,'$.ci.join_failure') IS NOT NULL FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id AND aa.retired=0 {boundary} ORDER BY aa.generation DESC LIMIT 1) {join} WHERE t.run_id=?1 AND (?2 IS NULL OR t.task_id=?2) ORDER BY t.task_id");
    let mut stmt = c.prepare(&sql)?;
    // The first shard of a split job that is a failure (see below).
    let mut job_failed = None;
    let whole_run = task.is_none();
    let mut rows = stmt.query(params![run, task])?;
    while let Some(row) = rows.next()? {
        let task = id("task_id", TaskId::parse(&row.get::<_, String>(0)?))?;
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
        let termination = termination
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
        let collection = collection.as_deref().filter(|_| !(whole_run && unjoined));
        snapshot.task(task, state, observation, termination, cancel, collection);
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
