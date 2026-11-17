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

impl CoordinatorStore {
    /// The runs that have a task waiting for the jobs it needs, oldest
    /// first. A task that was cancelled, or whose run is closed, waits for
    /// nothing.
    pub fn ci_waiting_runs(&self, limit: u32) -> Result<Vec<RunId>> {
        self.db.call(move |c| {
            let mut query = c.prepare(&format!(
                "SELECT t.run_id FROM task t JOIN run r ON r.run_id=t.run_id
                 WHERE {HELD} AND t.cancel_requested_at_ms IS NULL AND r.closed_at_ms IS NULL
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
