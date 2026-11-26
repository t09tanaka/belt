//! What the progression of a CI run reads and writes: the tasks of a run
//! that wait for the jobs they need (`ci.hold` in their spec), how the tasks
//! of the run ended, and the release of a waiting task with its inputs.
//!
//! A waiting task is a row of `task` like any other, made when the run was
//! accepted: the run is not settled before it ended, a cancel of the run
//! reaches it, and it is never made twice. It is only not placed
//! ([`super::queue`] leaves a held task out of the queue).
use super::*;

/// How many of the newest runs the duration of a CI job is looked up in
/// (spec 配置の手順 1).
const DURATION_RUNS: i64 = 20;
/// How old a run may be for its durations to count: 30 days.
pub const DURATION_MAX_AGE_MS: u64 = 30 * 24 * 60 * 60 * 1000;

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
    pub worker: String,
    /// How it ended; `None` while it has not.
    pub termination: Option<Termination>,
    /// The verdict of its collection, once the collection is closed
    /// (`complete`, `required_missing`, `gave_up`).
    pub collection: Option<String>,
}

const HELD: &str = "coalesce(json_extract(t.spec_json,'$.ci.hold'),0)=1";
/// A shard of a split job that has not been joined yet: its job has no
/// outcome, whether the shard was cancelled or not.
const UNJOINED_SHARD: &str = "coalesce(json_extract(t.spec_json,'$.ci.shard_group'),json_extract(t.spec_json,'$.ci.matrix.group')) IS NOT NULL
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
    pub(crate) fn ci_run_cancelled(&self, run: &RunId, authorization: u64) -> Result<bool> {
        let run = run.to_string();
        self.db.call(move |c|Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM run_cancel_intent WHERE run_id=?1 AND control_revision>=?2)",params![run,to_i64(authorization)?],|r|r.get(0))?))
    }

    /// A retry creates a new progression for the resulting set of task generations.
    pub(crate) fn ci_driver_authorization(&self, run: &RunId) -> Result<u64> {
        let run = run.to_string();
        self.db.call(move |c| {
            to_u64(c.query_row(
                "SELECT coalesce(max(authorization_revision),0) FROM task WHERE run_id=?1",
                [run],
                |r| r.get(0),
            )?)
        })
    }

    /// Read one revision at a time. A transition can contain a whole validated result.
    pub(crate) fn ci_driver_record(
        &self,
        run: &RunId,
        authorization: u64,
        revision: u64,
    ) -> Result<Option<String>> {
        let run = run.to_string();
        self.db.call(move |c| Ok(c.query_row(
            "SELECT record FROM ci_driver_transition WHERE run_id=?1 AND authorization=?2 AND revision=?3",
            params![run,to_i64(authorization)?,to_i64(revision)?], |r| r.get(0)).optional()?))
    }

    /// Commit only the next revision of the current task generations. A second
    /// observer of an older revision must reconstruct the winner before acting.
    pub(crate) fn append_ci_driver_record(
        &self,
        run: &RunId,
        authorization: u64,
        revision: u64,
        record: String,
    ) -> Result<bool> {
        let run = run.to_string();
        self.db.write(move |tx| { super::refuse_pruned(tx, &run)?; Ok(tx.execute(
            "INSERT INTO ci_driver_transition(run_id,authorization,revision,record)
             SELECT ?1,?2,?3,?4 WHERE ?3=(SELECT coalesce(max(revision),0)+1 FROM ci_driver_transition WHERE run_id=?1 AND authorization=?2)
             AND ?2=(SELECT max(authorization_revision) FROM task WHERE run_id=?1)
             AND EXISTS (SELECT 1 FROM run WHERE run_id=?1 AND closed_at_ms IS NULL)",
            params![run,to_i64(authorization)?,to_i64(revision)?,record])? == 1) })
    }

    /// Add the complete derived membership before releasing any variant. The
    /// transaction and its event are replay-safe across a coordinator restart.
    pub(crate) fn expand_ci_matrix(
        &self,
        run: &RunId,
        origin: &TaskId,
        authorization: u64,
        members: Vec<(TaskId, Value)>,
        at_ms: u64,
    ) -> Result<bool> {
        let (run, origin) = (run.to_string(), origin.to_string());
        self.db.write(move|tx|{
            super::refuse_pruned(tx,&run)?;
            let current:i64=tx.query_row("SELECT coalesce(max(authorization_revision),0) FROM task WHERE run_id=?1",[&run],|r|r.get(0))?;
            if to_u64(current)?!=authorization { return Err(StorageError::Busy); }
            let mut stmt=tx.prepare("SELECT json_extract(spec_json,'$.ci.task.task_id'),json_extract(spec_json,'$.ci.task.dispatch')
                FROM task WHERE run_id=?1 AND json_extract(spec_json,'$.ci.task.dispatch.origin')=(
                  SELECT json_extract(spec_json,'$.ci.task.task_id') FROM task WHERE run_id=?1 AND task_id=?2)")?;
            let stored=stmt.query_map(params![run,origin],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<std::result::Result<std::collections::BTreeMap<_,_>,_>>()?;
            drop(stmt);
            if !stored.is_empty() {
                if stored.len()!=members.len() || members.iter().any(|(_,spec)| {
                    let task=&spec["ci"]["task"]; let found=task["task_id"].as_str().and_then(|id|stored.get(id));
                    !found.is_some_and(|text|serde_json::from_str::<Value>(text).ok().as_ref()==Some(&task["dispatch"]))
                }) { return Err(StorageError::Immutable("matrix membership or its inputs changed on retry; submit a new run".into())); }
                tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.expanded_for',?1) WHERE task_id=?2",params![current,origin])?;
                return Ok(false);
            }
            let eligible:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM task t JOIN run r USING(run_id) WHERE t.run_id=?1 AND t.task_id=?2 AND r.closed_at_ms IS NULL AND t.cancel_requested_at_ms IS NULL AND json_extract(t.spec_json,'$.ci.dynamic_origin')=1)",params![run,origin],|r|r.get(0))?;
            if !eligible { return Ok(false); }
            let count:i64=tx.query_row("SELECT count(*) FROM task WHERE run_id=?1",[&run],|r|r.get(0))?;
            if count as usize+members.len()>super::queue::MAX_TASKS_PER_RUN { return Err(StorageError::Invalid("expanded CI run exceeds task limit".into())); }
            let names:Vec<_>=members.iter().map(|(_,v)|v["ci"]["task"]["task_id"].clone()).collect();
            for (id,spec) in members {
                tx.execute("INSERT INTO task(task_id,run_id,spec_json,created_at_ms,authorization_revision) VALUES(?1,?2,?3,?4,?5)",
                    params![id.as_str(),run,spec.to_string(),to_i64(at_ms)?,current])?;
            }
            tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.expanded_for',?1) WHERE task_id=?2",params![current,origin])?;
            let seq:i64=tx.query_row("SELECT count(*)+1 FROM event WHERE run_id=?1 AND kind='matrix_expanded'",[&run],|r|r.get(0))?;
            common::append_event(tx,Some(&run),None,"matrix_expanded",&serde_json::json!({"origin":origin,"expansion_seq":seq,"tasks":names}),at_ms)?;
            Ok(true)
        })
    }

    pub(crate) fn complete_ci_origin(
        &self,
        run: &RunId,
        origin: &TaskId,
        authorization: u64,
        status: &str,
        at_ms: u64,
    ) -> Result<bool> {
        let (run, origin, status) = (run.to_string(), origin.to_string(), status.to_string());
        self.db.write(move|tx|{
            super::refuse_pruned(tx, &run)?;
            let changed=tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.core_outcome',?3) WHERE run_id=?1 AND task_id=?2
                AND json_extract(spec_json,'$.ci.dynamic_origin')=1 AND json_extract(spec_json,'$.ci.core_outcome') IS NULL
                AND ?4=(SELECT max(authorization_revision) FROM task WHERE run_id=?1)",
                params![run,origin,status,to_i64(authorization)?])?;
            if changed>0 { common::append_event(tx,Some(&run),None,"ci_matrix_joined",&serde_json::json!({"origin":origin,"status":status}),at_ms)?; }
            Ok(changed>0)
        })
    }

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
                 WHERE (({HELD} AND t.cancel_requested_at_ms IS NULL AND json_extract(t.spec_json,'$.ci.core_outcome') IS NULL) OR ({UNJOINED_SHARD}))
                 AND r.closed_at_ms IS NULL AND r.pruning_at_ms IS NULL
                 GROUP BY t.run_id ORDER BY min(t.created_at_ms), min(t.rowid) LIMIT ?1"
            ))?;
            let rows = query.query_map([i64::from(limit)], |r| r.get::<_, String>(0))?;
            rows.map(|run| id("run_id", RunId::parse(&run?))).collect()
        })
    }

    /// Keep what became of the GitHub status of `run` in its journal. The
    /// journal is append-only: a later record is the current one.
    pub fn record_github_status(&self, run: &RunId, record: &Value, at_ms: u64) -> Result<()> {
        let (run, record) = (run.to_string(), record.clone());
        self.db.write(move |tx| {
            let known = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",
                [&run],
                |r| r.get::<_, bool>(0),
            )?;
            if !known {
                return Err(StorageError::NotFound { what: "run".into() });
            }
            super::refuse_pruned(tx, &run)?;
            common::append_event(tx, Some(&run), None, GITHUB_STATUS_EVENT, &record, at_ms)?;
            Ok(())
        })
    }

    /// The last recorded GitHub status of `run`.
    pub fn github_status_of(&self, run: &RunId) -> Result<Option<Value>> {
        let run = run.to_string();
        self.db.call(move |c| github_status(c, &run))
    }

    /// Every task of `run`, in the order they were made.
    ///
    /// A run that carries the mark of a pruned run is not found, by the same
    /// snapshot that reads its tasks (spec 25.3 (b)): rows of it may be gone.
    pub fn ci_run_tasks(&self, run: &RunId) -> Result<Vec<CiTaskRow>> {
        let run = run.to_string();
        let pruning = self.pruning;
        self.db.call(move |c| {
            let c = &c.unchecked_transaction()?;
            if super::run_marked(c, &run, pruning)? {
                return Err(super::run_pruned(&run));
            }
            let mut query = c.prepare(
                "SELECT t.task_id,t.spec_json,t.cancel_requested_at_ms IS NOT NULL,
                        a.attempt_id,a.state,a.termination_json,
                        CASE WHEN x.closed_at_ms IS NOT NULL THEN x.verdict ELSE NULL END,a.worker_id
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
                            worker: row.get(7)?,
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
        timings: Option<(String, Vec<(String, local_ci::shard::History)>)>,
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
                     WHERE t.run_id=?1 AND coalesce(json_extract(t.spec_json,'$.ci.shard_group'),json_extract(t.spec_json,'$.ci.matrix.group'))=?2
                     AND {UNJOINED_SHARD}
                     AND NOT EXISTS (SELECT 1 FROM task s WHERE s.run_id=?1
                        AND coalesce(json_extract(s.spec_json,'$.ci.shard_group'),json_extract(s.spec_json,'$.ci.matrix.group'))=?2 AND NOT {ENDED_S})"
                ),
                params![run, group, failure],
            )?;
            if changed == 0 {
                return Ok(false);
            }
            if let Some((scope, history)) = timings {
                let mut insert = tx.prepare_cached("INSERT INTO ci_test_timing(scope,worker,identity,millis,written_at_ms) VALUES(?1,?2,?3,?4,?5)
                    ON CONFLICT(scope,identity,worker) DO UPDATE SET millis=excluded.millis,written_at_ms=excluded.written_at_ms")?;
                // Spec 25.5: a timing that is not written for 14 days goes.
                let written = to_i64(at_ms)?;
                for (worker, shard) in history {
                    for (identity, millis) in shard.millis {
                        insert.execute(params![scope, worker, identity, to_i64(millis)?, written])?;
                    }
                }
            }
            for task in &rerun {
                tx.execute(
                    "UPDATE task SET spec_json=json_set(spec_json,'$.ci.rerun',json('true'))
                     WHERE run_id=?1 AND coalesce(json_extract(spec_json,'$.ci.shard_group'),json_extract(spec_json,'$.ci.matrix.group'))=?2
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

    /// How long each CI job named by `keys` (`CiSpec::duration_key`) took
    /// (from the attempt's creation to its end), looked up in the newest
    /// [`DURATION_RUNS`] runs created at or after `since_ms` that have one
    /// of these jobs (spec 配置の手順 1): runs of other projects or of plain
    /// commands do not push a job's history out. The newest success is the
    /// measure; a job that has not succeeded there takes its newest failure,
    /// which ran until the job itself stopped. A cancelled, interrupted,
    /// timed-out or retired attempt ended when something else stopped it, and
    /// a discovery probe and a shard member are not a measure of the job.
    pub fn ci_job_durations(
        &self,
        keys: std::collections::BTreeSet<String>,
        since_ms: u64,
    ) -> Result<std::collections::BTreeMap<String, u64>> {
        let mut found = std::collections::BTreeMap::new();
        if keys.is_empty() {
            return Ok(found);
        }
        let wanted =
            serde_json::to_string(&keys).map_err(|e| StorageError::Invalid(e.to_string()))?;
        self.db.call(move |c| {
            let mut query = c.prepare_cached(
                "SELECT json_extract(t.spec_json,'$.ci.duration_key'), a.terminal_at_ms - a.created_at_ms,
                        json_extract(a.termination_json,'$.outcome') = 'succeeded'
                 FROM (SELECT run_id FROM run r WHERE created_at_ms >= ?1
                         AND EXISTS(SELECT 1 FROM task k WHERE k.run_id = r.run_id
                           AND json_extract(k.spec_json,'$.ci.duration_key') IN (SELECT value FROM json_each(?3)))
                       ORDER BY created_at_ms DESC LIMIT ?2) r
                 JOIN task t ON t.run_id = r.run_id
                 JOIN attempt a ON a.task_id = t.task_id
                 WHERE json_extract(t.spec_json,'$.ci.duration_key') IN (SELECT value FROM json_each(?3))
                   AND json_extract(t.spec_json,'$.ci.shard_group') IS NULL
                   AND json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery') IS NULL
                   AND t.cancel_requested_at_ms IS NULL
                   AND a.cancel_requested_at_ms IS NULL
                   AND a.retired = 0
                   AND a.state = 'terminal'
                   AND json_extract(a.termination_json,'$.outcome') IN ('succeeded','failed')
                 ORDER BY a.terminal_at_ms DESC",
            )?;
            let mut rows = query.query(params![to_i64(since_ms)?, DURATION_RUNS, wanted])?;
            let mut failed = std::collections::BTreeMap::new();
            while let Some(row) = rows.next()? {
                let key: String = row.get(0)?;
                let millis = u64::try_from(row.get::<_, i64>(1)?).unwrap_or(0);
                let target = if row.get::<_, bool>(2)? {
                    &mut found
                } else {
                    &mut failed
                };
                target.entry(key).or_insert(millis);
            }
            for (key, millis) in failed {
                found.entry(key).or_insert(millis);
            }
            Ok(found)
        })
    }

    /// The timings used to make one partition. Read only the identities in
    /// this listing: obsolete files in a repository cannot grow the snapshot.
    pub fn ci_test_timings(
        &self,
        scope: String,
        identities: Vec<String>,
    ) -> Result<std::collections::BTreeMap<String, local_ci::shard::History>> {
        self.db.call(move |c| {
            let tx = c.unchecked_transaction()?;
            let mut history = std::collections::BTreeMap::<String, local_ci::shard::History>::new();
            {
                let mut query = tx.prepare_cached(
                    "SELECT worker,millis FROM ci_test_timing WHERE scope=?1 AND identity=?2",
                )?;
                for identity in identities {
                    let mut rows = query.query(params![scope, identity])?;
                    while let Some(row) = rows.next()? {
                        let worker: String = row.get(0)?;
                        history
                            .entry(worker)
                            .or_default()
                            .millis
                            .insert(identity.clone(), to_u64(row.get(1)?)?);
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
        previous: &Digest,
        next: &Digest,
        probe_ids: Vec<TaskId>,
        replacements: Vec<(TaskId, Vec<(TaskId, Value)>)>,
        at_ms: u64,
    ) -> Result<bool> {
        let run = run.to_string();
        let (previous, next) = (previous.to_string(), next.to_string());
        self.db.write(move |tx| {
            super::refuse_pruned(tx, &run)?;
            for id in &probe_ids {
                let pending: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM task WHERE run_id=?1 AND task_id=?2
                     AND json_extract(spec_json,'$.ci.task.inputs.belt_discovery.applied')=0 AND coalesce(json_extract(spec_json,'$.ci.task.inputs.belt_discovery.active_plan'),json_extract(spec_json,'$.plan_digest'))=?3)",
                    params![run, id.as_str(),previous], |r| r.get(0))?;
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
                tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.task.inputs.belt_discovery.applied',json('true'),'$.ci.task.inputs.belt_discovery.applied_plan',?3) WHERE run_id=?1 AND task_id=?2",
                    params![run, id.as_str(),next])?;
            }
            tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.ci.task.inputs.belt_discovery.active_plan',?2) WHERE run_id=?1 AND json_type(spec_json,'$.ci.task.inputs.belt_discovery')='object'",params![run,next])?;
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
                    "UPDATE task AS t SET spec_json=CASE WHEN json_type(t.spec_json,'$.ci.task.inputs.belt_discovery')='object'
                        THEN json_set(?2,'$.ci.task.inputs.belt_discovery',json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery'))
                        ELSE ?2 END WHERE t.task_id=?1 AND {HELD}
                     AND t.cancel_requested_at_ms IS NULL
                     AND NOT EXISTS (SELECT 1 FROM attempt a WHERE a.task_id=t.task_id AND a.retired=0 AND (a.generation>=t.min_generation OR a.state<>'terminal'))"
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

/// The kind of the run-level event that keeps a GitHub status.
pub const GITHUB_STATUS_EVENT: &str = "github_status";

/// The newest status event of `run`. Only the events of no attempt are
/// walked (`event_attempt` seeks them), newest first.
pub(crate) fn github_status(c: &Connection, run: &str) -> Result<Option<Value>> {
    let raw = c
        .query_row(
            "SELECT payload_json FROM event
             WHERE attempt_id IS NULL AND run_id=?1 AND kind=?2 ORDER BY seq DESC LIMIT 1",
            params![run, GITHUB_STATUS_EVENT],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    Ok(raw.and_then(|text| serde_json::from_str(&text).ok()))
}
