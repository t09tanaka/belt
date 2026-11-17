//! What the progression of a CI run reads and writes: the tasks of a run
//! that wait for the jobs they need (`ci.hold` in their spec), how the tasks
//! of the run ended, and the release of a waiting task with its inputs.
//!
//! A waiting task is a row of `task` like any other, made when the run was
//! accepted: the run is not settled before it ended, a cancel of the run
//! reaches it, and it is never made twice. It is only not placed
//! ([`super::queue`] leaves a held task out of the queue).
use super::*;

/// One task of a run, as the progression of the run reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct CiTaskRow {
    pub task_id: TaskId,
    pub spec: Value,
    /// A cancel of the task (or of its run) was requested.
    pub cancelled: bool,
    /// The attempt that counts for the task now: the newest one that was
    /// not retired, of the generation a retry asked for.
    pub attempt: Option<CiAttempt>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CiAttempt {
    pub attempt_id: AttemptId,
    /// How it ended; `None` while it has not.
    pub termination: Option<Termination>,
    /// The verdict of its collection, once the collection is closed
    /// (`complete`, `required_missing`, `gave_up`).
    pub collection: Option<String>,
}

const HELD: &str = "coalesce(json_extract(t.spec_json,'$.ci.hold'),0)=1";
/// A shard of a split job that has not been joined yet: its job has no
/// outcome, whether the shard was cancelled or not.
const UNJOINED_SHARD: &str = "json_extract(t.spec_json,'$.ci.shard_group') IS NOT NULL
    AND coalesce(json_extract(t.spec_json,'$.ci.joined'),0)=0";

/// The task `s` ended, as the progression reads it (`ci::advance::standing`):
/// it was cancelled and no attempt counts for it, or the attempt that counts
/// is terminal and its collection is closed.
const ENDED_S: &str =
    "((s.cancel_requested_at_ms IS NOT NULL AND NOT EXISTS (SELECT 1 FROM attempt aa
        WHERE aa.task_id=s.task_id AND aa.retired=0 AND aa.generation>=s.min_generation))
    OR EXISTS (SELECT 1 FROM attempt a JOIN collection x ON x.attempt_id=a.attempt_id
        WHERE a.attempt_id=(SELECT aa.attempt_id FROM attempt aa
            WHERE aa.task_id=s.task_id AND aa.retired=0 AND aa.generation>=s.min_generation
            ORDER BY aa.generation DESC LIMIT 1)
        AND a.state='terminal' AND x.closed_at_ms IS NOT NULL))";

impl CoordinatorStore {
    /// The runs that have a task waiting for the jobs it needs, or a shard
    /// of a split job that is not joined yet (a failed shard may have to
    /// stop the others, and the job needs its one outcome before the run is
    /// settled), oldest first. A waiting task that was cancelled waits for
    /// nothing; a shard is joined even when it was. A closed run has
    /// neither.
    pub fn ci_waiting_runs(&self, limit: u32) -> Result<Vec<RunId>> {
        self.db.call(move |c| {
            let mut query = c.prepare(&format!(
                "SELECT t.run_id FROM task t JOIN run r ON r.run_id=t.run_id
                 WHERE (({HELD} AND t.cancel_requested_at_ms IS NULL) OR ({UNJOINED_SHARD}))
                 AND r.closed_at_ms IS NULL
                 GROUP BY t.run_id ORDER BY min(t.created_at_ms), min(t.rowid) LIMIT ?1"
            ))?;
            let rows = query.query_map([i64::from(limit)], |r| r.get::<_, String>(0))?;
            rows.map(|run| id("run_id", RunId::parse(&run?))).collect()
        })
    }

    /// Every task of `run`, in the order they were made.
    pub fn ci_run_tasks(&self, run: &RunId) -> Result<Vec<CiTaskRow>> {
        let run = run.to_string();
        self.db.call(move |c| {
            let mut query = c.prepare(
                "SELECT t.task_id,t.spec_json,t.cancel_requested_at_ms IS NOT NULL,
                        a.attempt_id,a.state,a.termination_json,
                        CASE WHEN x.closed_at_ms IS NOT NULL THEN x.verdict ELSE NULL END
                 FROM task t
                 LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa
                     WHERE aa.task_id=t.task_id AND aa.retired=0 AND aa.generation>=t.min_generation
                     ORDER BY aa.generation DESC LIMIT 1)
                 LEFT JOIN collection x ON x.attempt_id=a.attempt_id
                 WHERE t.run_id=?1 ORDER BY t.created_at_ms,t.rowid",
            )?;
            let mut rows = query.query([&run])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let attempt: Option<String> = row.get(3)?;
                let state: Option<String> = row.get(4)?;
                let termination: Option<String> = row.get(5)?;
                let attempt = match attempt {
                    None => None,
                    Some(attempt) => {
                        let terminal = state.map(|s| parse_state(&s)).transpose()?
                            == Some(AttemptState::Terminal);
                        Some(CiAttempt {
                            attempt_id: id("attempt_id", AttemptId::parse(&attempt))?,
                            termination: termination
                                .filter(|_| terminal)
                                .map(|t| from_json("termination", &t))
                                .transpose()?,
                            collection: row.get(6)?,
                        })
                    }
                };
                out.push(CiTaskRow {
                    task_id: id("task_id", TaskId::parse(&row.get::<_, String>(0)?))?,
                    spec: from_json("task spec", &row.get::<_, String>(1)?)?,
                    cancelled: row.get(2)?,
                    attempt,
                });
            }
            Ok(out)
        })
    }

    /// The split job `group` of `run` has its one outcome: mark every shard
    /// of it as joined, with what is wrong when the job is a failure, and
    /// record the event `ci_job_joined`, in one transaction. Returns whether
    /// anything changed (a job that is joined already is left as it is).
    /// `rerun` names the shards (by their id in the plan) a retry of the run
    /// runs again although their attempts succeeded.
    ///
    /// The caller read the shards before this: nothing is written unless
    /// every shard of the job has ended now, in this transaction (a retry
    /// in between let some run again, and the outcome that was read is of
    /// attempts that no longer count).
    pub fn join_ci_shards(
        &self,
        run: &RunId,
        group: &str,
        failure: Option<&str>,
        rerun: &[String],
        at_ms: u64,
    ) -> Result<bool> {
        self.join_ci_shards_with_timings(run, group, failure, rerun, at_ms, None)
    }

    /// Keep verified runner times in the very transaction that accepts the
    /// group. Replaying a join cannot write older observations back.
    pub fn join_ci_shards_with_timings(
        &self,
        run: &RunId,
        group: &str,
        failure: Option<&str>,
        rerun: &[String],
        at_ms: u64,
        timings: Option<(String, local_ci::shard::History)>,
    ) -> Result<bool> {
        let (run, group, failure, rerun) = (
            run.to_string(),
            group.to_string(),
            failure.map(str::to_string),
            rerun.to_vec(),
        );
        self.db.write(move |tx| {
            let changed = tx.execute(
                &format!(
                    "UPDATE task AS t SET spec_json=CASE WHEN ?3 IS NULL
                        THEN json_set(t.spec_json,'$.ci.joined',json('true'))
                        ELSE json_set(t.spec_json,'$.ci.joined',json('true'),'$.ci.join_failure',?3) END
                     WHERE t.run_id=?1 AND json_extract(t.spec_json,'$.ci.shard_group')=?2
                     AND {UNJOINED_SHARD}
                     AND NOT EXISTS (SELECT 1 FROM task s WHERE s.run_id=?1
                        AND json_extract(s.spec_json,'$.ci.shard_group')=?2 AND NOT {ENDED_S})"
                ),
                params![run, group, failure],
            )?;
            if changed == 0 {
                return Ok(false);
            }
            if let Some((scope, history)) = timings {
                let mut insert = tx.prepare_cached("INSERT INTO ci_test_timing(scope,identity,millis) VALUES(?1,?2,?3)
                    ON CONFLICT(scope,identity) DO UPDATE SET millis=excluded.millis")?;
                for (identity, millis) in history.millis {
                    insert.execute(params![scope, identity, to_i64(millis)?])?;
                }
            }
            for task in &rerun {
                tx.execute(
                    "UPDATE task SET spec_json=json_set(spec_json,'$.ci.rerun',json('true'))
                     WHERE run_id=?1 AND json_extract(spec_json,'$.ci.shard_group')=?2
                     AND json_extract(spec_json,'$.ci.task.task_id')=?3",
                    params![run, group, task],
                )?;
            }
            common::append_event(
                tx,
                Some(&run),
                None,
                "ci_job_joined",
                &serde_json::json!({ "job": group, "failure": failure, "rerun": rerun }),
                at_ms,
            )?;
            Ok(true)
        })
    }

    /// The run can never go on (`why`): every split job of it that is not
    /// joined yet is a failure with that reason, so the run does not wait
    /// for an outcome nobody can make. Returns how many shards changed.
    pub fn fail_ci_joins(&self, run: &RunId, why: &str, at_ms: u64) -> Result<usize> {
        let (run, why) = (run.to_string(), why.to_string());
        self.db.write(move |tx| {
            let changed = tx.execute(
                &format!(
                    "UPDATE task AS t SET spec_json=json_set(t.spec_json,
                        '$.ci.joined',json('true'),'$.ci.join_failure',?2)
                     WHERE t.run_id=?1 AND {UNJOINED_SHARD}"
                ),
                params![run, why],
            )?;
            if changed > 0 {
                common::append_event(
                    tx,
                    Some(&run),
                    None,
                    "ci_job_joined",
                    &serde_json::json!({ "job": Value::Null, "failure": why }),
                    at_ms,
                )?;
            }
            Ok(changed)
        })
    }

    /// The timings used to make one partition. Read only the identities in
    /// this listing: obsolete files in a repository cannot grow the snapshot.
    pub fn ci_test_timings(
        &self,
        scope: String,
        identities: Vec<String>,
    ) -> Result<local_ci::shard::History> {
        self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            let mut history = local_ci::shard::History::default();
            {
                let mut query = tx.prepare_cached(
                    "SELECT millis FROM ci_test_timing WHERE scope=?1 AND identity=?2",
                )?;
                for identity in identities {
                    let millis: Option<i64> = query
                        .query_row(params![scope, identity], |r| r.get(0))
                        .optional()?;
                    if let Some(millis) = millis {
                        history.millis.insert(identity, to_u64(millis)?);
                    }
                }
            }
            tx.commit()?;
            Ok(history)
        })
    }

    /// Commit a frozen partition once. The first shard keeps the submitter's
    /// task id. Later shards inherit any per-task cancellation that won the
    /// race. The run-cancel trigger also applies to every inserted row.
    pub fn apply_ci_discovery(
        &self,
        run: &RunId,
        probe_ids: Vec<TaskId>,
        replacements: Vec<(TaskId, Vec<(TaskId, Value)>)>,
        at_ms: u64,
    ) -> Result<bool> {
        let run = run.to_string();
        self.db.write(move |tx| {
            for id in &probe_ids {
                let pending: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM task WHERE run_id=?1 AND task_id=?2
                     AND json_extract(spec_json,'$.ci.task.inputs.rtr_discovery.applied')=0)",
                    params![run, id.as_str()], |r| r.get(0))?;
                if !pending { return Ok(false); }
            }
            for (old, parts) in replacements {
                let cancelled: Option<i64> = tx.query_row(
                    "SELECT cancel_requested_at_ms FROM task t WHERE run_id=?1 AND task_id=?2
                     AND json_extract(spec_json,'$.ci.hold')=1
                     AND NOT EXISTS(SELECT 1 FROM attempt a WHERE a.task_id=t.task_id)",
                    params![run, old.as_str()], |r| r.get(0))?;
                for (id, spec) in parts {
                    if id == old {
                        tx.execute("UPDATE task SET spec_json=?3 WHERE run_id=?1 AND task_id=?2",
                            params![run, id.as_str(), spec.to_string()])?;
                    } else {
                        tx.execute("INSERT INTO task(task_id,run_id,spec_json,created_at_ms,cancel_requested_at_ms) VALUES(?1,?2,?3,?4,?5)",
                            params![id.as_str(),run,spec.to_string(),to_i64(at_ms)?,cancelled])?;
                    }
                }
            }
            for id in probe_ids {
                tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.task.inputs.rtr_discovery.applied',json('true')) WHERE run_id=?1 AND task_id=?2",
                    params![run, id.as_str()])?;
            }
            common::append_event(tx,Some(&run),None,"ci_discovery_applied",&serde_json::json!({}),at_ms)?;
            Ok(true)
        })
    }

    /// Let a waiting task into the queue with the spec that holds its inputs
    /// (`spec` no longer has `ci.hold`). Nothing changes, and `false` is
    /// returned, when the task does not wait any more or a cancel was
    /// requested for it: the check and the change are one transaction, so a
    /// cancel that came first wins.
    pub fn release_ci_task(&self, task_id: &TaskId, spec: &Value, at_ms: u64) -> Result<bool> {
        let hold = spec.get("ci").and_then(|ci| ci.get("hold"));
        if hold.is_some_and(|hold| hold != &Value::Bool(false)) {
            return Err(StorageError::Invalid(
                "a released task does not wait".into(),
            ));
        }
        let (task, spec) = (task_id.to_string(), spec.to_string());
        self.db.write(move |tx| {
            let changed = tx.execute(
                &format!(
                    "UPDATE task AS t SET spec_json=?2 WHERE t.task_id=?1 AND {HELD}
                     AND t.cancel_requested_at_ms IS NULL
                     AND NOT EXISTS (SELECT 1 FROM attempt a WHERE a.task_id=t.task_id)"
                ),
                params![task, spec],
            )?;
            if changed == 0 {
                return Ok(false);
            }
            let run: String =
                tx.query_row("SELECT run_id FROM task WHERE task_id=?1", [&task], |row| {
                    row.get(0)
                })?;
            common::append_event(
                tx,
                Some(&run),
                None,
                "ci_task_ready",
                &serde_json::json!({ "task": task }),
                at_ms,
            )?;
            Ok(true)
        })
    }
}
