//! Bounded result-only claims. Filesystem work is never performed in a transaction.
use super::*;
use crate::results::{
    clock_policy::{self, ClockState},
    pin::RunPin,
    store::ResultStore,
};
use crate::storage::policy::{decide_gc, GcVerdict};

#[derive(Debug, Default)]
pub struct ResultGcPage {
    pub examined: u32,
    pub removed: u32,
    pub queued: u32,
    pub foreign: u32,
    pub next: Option<String>,
    pub clock_held: bool,
}
#[derive(Debug, Clone)]
pub struct ResultDeleteClaim {
    pub locator: String,
    pub run: RunId,
    pub attempt: AttemptId,
}

impl CoordinatorStore {
    /// Fresh scalar all-generation input-retention proof. No source lock is taken
    /// here: the caller holds source EX before entering this metadata operation.
    pub fn input_retention_end(&self, run: &RunId, source: &SourceId) -> Result<Option<u64>> {
        let run = run.to_string();
        let source = source.to_string();
        self.db
            .call(move |c| input_retention_end_on(c, &run, &source))
    }

    #[cfg(test)]
    pub(crate) fn workflow_run_end_for_test(&self, run: &RunId) -> Result<Option<u64>> {
        let run = run.to_string();
        self.db.call(move |c| workflow_run_end(c, &run))
    }

    /// Source EX must already be held. Require this live writable owner and a
    /// physical source directory belonging to its configured state directory.
    pub(crate) fn check_input_store(
        &self,
        source: &std::path::Path,
        held: &std::fs::File,
    ) -> anyhow::Result<String> {
        use std::os::unix::fs::MetadataExt;
        self._lock
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("read-only input retention owner"))?
            .check_current()?;
        let state: std::fs::File = rustix::fs::open(
            &self.state_dir,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?
        .into();
        let physical =
            rustix::fs::statat(&state, "sources", rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
        let actual = held.metadata()?;
        // st_dev's integer type differs between macOS and Linux.
        #[allow(clippy::unnecessary_cast)]
        let physical_device = physical.st_dev as u64;
        anyhow::ensure!(
            rustix::fs::FileType::from_raw_mode(physical.st_mode)
                == rustix::fs::FileType::Directory
                && (physical_device, physical.st_ino) == (actual.dev(), actual.ino()),
            "foreign coordinator source store"
        );
        let named = std::fs::symlink_metadata(source)?;
        anyhow::ensure!(
            (named.dev(), named.ino()) == (actual.dev(), actual.ino()) && named.is_dir(),
            "source store changed"
        );
        self.db.check_current_file()?;
        let db = rustix::fs::statat(
            &state,
            "coordinator.db",
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        anyhow::ensure!(
            rustix::fs::FileType::from_raw_mode(db.st_mode) == rustix::fs::FileType::RegularFile
                && db.st_nlink == 1
                && db.st_uid == rustix::process::geteuid().as_raw(),
            "unsafe coordinator input proof database"
        );
        let state = state.metadata()?;
        Ok(format!(
            "{}:{}:{}:{}:{}",
            self.coordinator_id,
            state.dev(),
            state.ino(),
            db.st_dev,
            db.st_ino
        ))
    }

    /// Exact run/source/digest proof. The generation history is scanned, not a
    /// constant-time page; typed termination validation never collects that
    /// history into a root-sized Vec. Source EX prevents retry admission here.
    pub(crate) fn input_retention_binding(
        &self,
        run: &RunId,
        source: &SourceId,
        digest: &str,
    ) -> Result<Option<(u64, String)>> {
        let (run, source, digest) = (run.to_string(), source.to_string(), digest.to_owned());
        self.db.call(move |c| {
            let Some(ended)=input_retention_end_on(c,&run,&source)? else {return Ok(None)};
            let exact:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM run r JOIN source_ref s USING(source_id) WHERE r.run_id=?1 AND r.source_id=?2 AND s.manifest_digest=?3)",params![run,source,digest],|r|r.get(0))?;
            if !exact{return Ok(None)}
            // A retired attempt has no termination and is not part of the proof
            // (`input_retention_end_on`); a missing one is a hold, never an error.
            let mut statement=c.prepare("SELECT a.termination_json FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1 AND a.retired=0")?;
            let mut rows=statement.query([&run])?;
            while let Some(row)=rows.next()? {
                let Some(termination)=row.get::<_,Option<String>>(0)? else {return Ok(None)};
                if serde_json::from_str::<crate::state::Termination>(&termination).is_err(){return Ok(None)}
            }
            Ok(Some((ended,format!("{run}:{source}:{digest}"))))
        })
    }

    /// Authorize retained bytes from fresh durable references, never filesystem
    /// existence or an old receipt. The invocation must already hold its run pin.
    pub(super) fn result_data_available(&self, attempt: &AttemptId, locator: &str) -> Result<()> {
        let (attempt, locator) = (attempt.to_string(), locator.to_owned());
        self.db.call(move |c| {
            let referenced: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM blob_ref WHERE attempt_id=?1 AND locator=?2)",
                params![attempt, locator], |r| r.get(0),
            )?;
            let claims: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='result_delete_claim')",
                [], |r| r.get(0),
            )?;
            let claimed = claims && c.query_row(
                "SELECT EXISTS(SELECT 1 FROM result_delete_claim WHERE locator=?1)",
                [&locator], |r| r.get::<_, bool>(0),
            )?;
            if !referenced || claimed {
                return Err(not_found("result data unavailable: expired, unreferenced or deletion claimed"));
            }
            Ok(())
        })
    }

    /// One durable run pin backfill, bounded by the caller's keyset run page.
    ///
    /// This makes the pin file, so it answers "not found" and touches no
    /// pin for a run that has no row or carries the mark of a pruned run
    /// (spec 25.5): the collection of results never makes the pin of such a
    /// run again, and its caller goes on to the next run.
    pub fn result_gc_guard(&self, run: &RunId) -> Result<RunPin> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }
        let id = run.to_string();
        self.db.call(move |c| {
            let known: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM run WHERE run_id=?1)",
                [&id],
                |r| r.get(0),
            )?;
            if !known {
                return Err(not_found(format!("run {id}")));
            }
            refuse_pruned(c, &id)
        })?;
        RunPin::ensure(&self.state_dir, run)?;
        Ok(RunPin::exclusive(&self.state_dir, run)?)
    }
    /// Claim at most 64 refs of one exclusively pinned run. Clock state and
    /// claims commit together. Unknown/unACK/open/active work always blocks.
    // Explicit samples permit deterministic fault/restart tests; production
    // samples are supplied only by the bounded retention driver.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_result_page(
        &self,
        guard: &RunPin,
        run: &RunId,
        after: &str,
        limit: u32,
        wall: u64,
        mono: u64,
        boot: Option<String>,
        retention: u64,
    ) -> Result<ResultGcPage> {
        guard.check_exclusive(&self.state_dir, run)?;
        if !(1..=64).contains(&limit) {
            return Err(StorageError::Invalid(
                "result GC page limit must be 1..=64".into(),
            ));
        }
        let (run, after) = (run.to_string(), after.to_string());
        self.db.write(move |tx| {
            let previous:Option<String>=tx.query_row("SELECT state_json FROM result_gc_clock WHERE singleton=1",[],|r|r.get(0)).optional()?;
            let previous=previous.as_deref().map(|s|from_json::<ClockState>("result GC clock",s)).transpose()?;
            let step=clock_policy::step(previous.as_ref(),boot.as_deref(),wall,mono,retention);
            if let Some(state)=step.state { tx.execute("INSERT INTO result_gc_clock VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET state_json=excluded.state_json",[json(&state)])?; }
            let Some(now)=step.cutoff else {return Ok(ResultGcPage{clock_held:true,..Default::default()})};
            let refs:Vec<(String,String,String,u32)>= {
                let mut s=tx.prepare("SELECT b.ref_id,b.attempt_id,b.locator,b.pins FROM result_gc_ref g JOIN blob_ref b USING(ref_id) WHERE g.run_id=?1 AND g.ref_id>?2 ORDER BY g.ref_id LIMIT ?3")?;
                let rows=s.query_map(params![run,after,limit],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
                rows.collect::<std::result::Result<_,_>>()?
            };
            let mut report=ResultGcPage{examined:refs.len() as u32,next:if refs.len()==limit as usize {refs.last().map(|r|r.0.clone())}else{None},..Default::default()};
            // Every reference of a page is of this one run: its end is read once.
            let mut run_end:Option<Option<u64>>=None;
            for (reference,attempt,locator,pins) in refs {
                if !ResultStore::owns(&locator) {report.foreign+=1;continue;}
                // A later job of the run may still download what an attempt
                // that ended long ago uploaded: such a file stays while the
                // run is open, and for the whole retention after its end.
                let later=if reference.starts_with(workflow_artifact::REF_PREFIX) {
                    let end=match run_end {Some(end)=>end,None=>*run_end.insert(workflow_run_end(tx,&run)?)};
                    let Some(end)=end else {continue};
                    end
                } else {0};
                let rec=load_attempt(tx,&attempt)?.ok_or_else(||not_found("GC attempt"))?;
                let blocked:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt a LEFT JOIN collection c USING(attempt_id) WHERE a.attempt_id=?1 AND (a.observation<>'known' OR (a.state<>'terminal' AND a.retired=0) OR (a.retired=0 AND (c.closed_at_ms IS NULL OR (c.acked_at_ms IS NULL AND a.collection_failed_at_ms IS NULL)))))",[&attempt],|r|r.get(0))?;
                if blocked {continue;}
                let (required,persisted)=artifact_counts(tx,&attempt)?;
                let closed=collect::closed_at(tx,&attempt)?;
                let facts=GcCandidate{terminal_at_ms:rec.terminal_at_ms.map(|t|t.max(closed.unwrap_or(0)).max(later)),retired_at_ms:rec.retired_at_ms.map(|t|t.max(later)),collected:rec.receipts.reached(Receipt::Collected),collection_failed_at_ms:rec.collection_failed_at_ms,missing_required_artifacts:required-persisted,pins};
                if facts.ended_at_ms().is_some_and(|ended|ended>now) {continue;}
                if decide_gc(&facts,now,retention,None)!=GcVerdict::Collect {continue;}
                // A foreign run sharing a locator is retained rather than assigning
                // an ambiguous deletion owner. Last-ref removal and association are atomic.
                let foreign:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM blob_ref b JOIN attempt a USING(attempt_id) JOIN task t USING(task_id) WHERE b.locator=?1 AND t.run_id<>?2)",params![locator,run],|r|r.get(0))?;
                if foreign {continue;}
                tx.execute("DELETE FROM blob_ref WHERE ref_id=?1",[reference])?;
                report.removed+=1;
                let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM blob_ref WHERE locator=?1)",[&locator],|r|r.get(0))?;
                if !exists {
                    tx.execute("INSERT INTO pending_delete(locator,queued_at_ms) VALUES(?1,?2)",params![locator,to_i64(now)?])?;
                    tx.execute("INSERT INTO result_delete_claim(locator,run_id,attempt_id) VALUES(?1,?2,?3)",params![locator,run,attempt])?;
                    report.queued+=1;
                }
            }
            Ok(report)
        })
    }
    /// Only associated claims are replayable. Legacy pending rows stay untouched.
    pub fn pending_result_page(
        &self,
        guard: &RunPin,
        run: &RunId,
        limit: u32,
    ) -> Result<Vec<ResultDeleteClaim>> {
        guard.check_exclusive(&self.state_dir, run)?;
        if !(1..=64).contains(&limit) {
            return Err(StorageError::Invalid(
                "result delete page limit must be 1..=64".into(),
            ));
        }
        let run = run.to_string();
        self.db.call(move |c| pending_claims(c, &run, limit))
    }
    pub fn finish_result_delete(&self, guard: &RunPin, claim: &ResultDeleteClaim) -> Result<()> {
        guard.check_exclusive(&self.state_dir, &claim.run)?;
        let claim = claim.clone();
        self.db.write(move |tx| finish_claim(tx, &claim))
    }
}

/// When the run ended, for the files its jobs hand to each other; `None`
/// while a job may still ask for one. A run has ended when no attempt of it
/// is still live and every task is finished, by the current attempt the
/// listing of runs counts (`query::run_facts`): that attempt has its
/// termination, or the task was cancelled before it was placed. A task that
/// is never placed itself is finished too: a probe whose discovery was
/// applied, and the origin of a matrix once its members were joined. The end
/// is the latest moment an attempt ended, a task was cancelled or the run
/// was closed.
///
/// A task is settled as in spec 25.3 (a) rule 2, the predicate of
/// `input_retention_end_on`: its current attempt has a termination, or it
/// has no current attempt and was cancelled, is an applied probe or a joined
/// matrix origin, or its run is closed (a closed run places no task).
fn workflow_run_end(c: &Connection, run: &str) -> Result<Option<u64>> {
    let (live, ended): (bool, i64) = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1 AND a.retired=0 AND a.state<>'terminal')
              OR EXISTS(SELECT 1 FROM task t LEFT JOIN attempt a ON a.attempt_id=(SELECT aa.attempt_id FROM attempt aa WHERE aa.task_id=t.task_id AND aa.retired=0 AND aa.generation>=t.min_generation ORDER BY aa.generation DESC LIMIT 1)
                 WHERE t.run_id=?1 AND a.termination_json IS NULL AND (t.cancel_requested_at_ms IS NULL OR a.attempt_id IS NOT NULL)
                 AND (a.attempt_id IS NOT NULL OR EXISTS(SELECT 1 FROM run r WHERE r.run_id=t.run_id AND r.closed_at_ms IS NULL))
                 AND coalesce(json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery.applied'),0)=0
                 AND NOT (coalesce(json_extract(t.spec_json,'$.ci.dynamic_origin'),0)=1 AND json_extract(t.spec_json,'$.ci.core_outcome') IS NOT NULL)),
            max((SELECT coalesce(max(coalesce(a.terminal_at_ms,a.retired_at_ms,0)),0) FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1),
                (SELECT coalesce(max(t.cancel_requested_at_ms),0) FROM task t WHERE t.run_id=?1),
                (SELECT coalesce(max(closed_at_ms),0) FROM run WHERE run_id=?1))",
        [run],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if live {
        return Ok(None);
    }
    to_u64(ended).map(Some)
}

/// Caller holds source EX; history scan is deliberate and not a bounded-ref claim.
/// A retired attempt (`retired = 1` only after the worker confirmed it is not
/// running) never reads the inputs again, so it is skipped by the "every
/// attempt ended and collected" proof and only moves the end time. The end
/// is the latest moment an attempt ended, was retired or was collected, a
/// task was cancelled or the run was closed (as in `workflow_run_end`): a
/// task left with only retired attempts ends by its cancel or by the close
/// of its run, and the retention to retry in starts there, not at the retire.
///
/// A task that has no attempt at all is judged the same way (spec 25.3 (a),
/// rule 2): it holds the inputs while it may still be placed, and no longer
/// once it was cancelled, its run was closed, or it is a task that is never
/// placed itself, as `workflow_run_end` reads it (a probe whose discovery
/// was applied, the origin of a matrix whose members were joined).
fn input_retention_end_on(c: &Connection, run: &str, source: &str) -> Result<Option<u64>> {
    let ended: Option<i64> = c.query_row(
        &format!(
            "SELECT CASE WHEN
              EXISTS(SELECT 1 FROM run WHERE run_id=?1 AND source_id=?2)
              AND EXISTS(SELECT 1 FROM task WHERE run_id=?1)
              AND {RUN_ENDED}
              THEN {RUN_END}
              ELSE NULL END"
        ),
        params![run, source],
        |r| r.get(0),
    )?;
    ended.map(to_u64).transpose()
}

/// Spec 25.3 (a), rules 1 to 4, for the run `?1`: no task of it may still
/// be placed, and every attempt that was not retired is terminal, observed
/// and collected. One text for the retention of the inputs and for the
/// pruning of the run (spec 25.5), so the two never judge a run differently.
const RUN_ENDED: &str = "NOT EXISTS(SELECT 1 FROM task t WHERE t.run_id=?1 AND
                t.cancel_requested_at_ms IS NULL AND EXISTS(SELECT 1 FROM run r WHERE r.run_id=t.run_id AND r.closed_at_ms IS NULL)
                AND coalesce(json_extract(t.spec_json,'$.ci.task.inputs.belt_discovery.applied'),0)=0
                AND NOT (coalesce(json_extract(t.spec_json,'$.ci.dynamic_origin'),0)=1 AND json_extract(t.spec_json,'$.ci.core_outcome') IS NOT NULL)
                AND NOT EXISTS(SELECT 1 FROM attempt a WHERE a.task_id=t.task_id AND a.retired=0 AND (a.generation>=t.min_generation OR a.state<>'terminal')))
              AND NOT EXISTS(SELECT 1 FROM task t JOIN attempt a USING(task_id) LEFT JOIN collection x USING(attempt_id)
                WHERE t.run_id=?1 AND a.retired=0 AND (a.observation<>'known' OR a.state<>'terminal' OR a.terminal_at_ms IS NULL OR a.termination_json IS NULL OR json_valid(a.termination_json)=0 OR x.closed_at_ms IS NULL OR x.verdict IS NULL OR x.verdict NOT IN ('complete','required_missing','gave_up')))";
/// The end of a run that [`RUN_ENDED`] holds for: the latest of the ends
/// spec 25.3 (a) lists.
const RUN_END: &str = "max((SELECT coalesce(max(max(coalesce(a.terminal_at_ms,a.retired_at_ms,0),coalesce(x.closed_at_ms,0))),0) FROM task t JOIN attempt a USING(task_id) LEFT JOIN collection x USING(attempt_id) WHERE t.run_id=?1),
                (SELECT coalesce(max(t.cancel_requested_at_ms),0) FROM task t WHERE t.run_id=?1),
                (SELECT coalesce(max(closed_at_ms),0) FROM run WHERE run_id=?1))";

/// When the run ended, for its pruning (spec 25.5): the predicate of
/// [`input_retention_end_on`], whatever the source of the run (a run may
/// have none). A run without a task has ended only once it is closed: until
/// then its first task may still be made, and a run that was just accepted
/// must not look like one that ended at time zero.
pub(super) fn run_end_on(c: &Connection, run: &str) -> Result<Option<u64>> {
    let ended: Option<i64> = c.query_row(
        &format!(
            "SELECT CASE WHEN
              (EXISTS(SELECT 1 FROM task WHERE run_id=?1)
               OR EXISTS(SELECT 1 FROM run WHERE run_id=?1 AND closed_at_ms IS NOT NULL))
              AND {RUN_ENDED}
              THEN {RUN_END}
              ELSE NULL END"
        ),
        [run],
        |r| r.get(0),
    )?;
    ended.map(to_u64).transpose()
}

/// The deletion claims of a run that are not finished, in the order of
/// their locators.
pub(super) fn pending_claims(
    c: &Connection,
    run: &str,
    limit: u32,
) -> Result<Vec<ResultDeleteClaim>> {
    let mut s = c.prepare(
        "SELECT locator,attempt_id FROM result_delete_claim WHERE run_id=?1 ORDER BY locator LIMIT ?2",
    )?;
    let rows = s.query_map(params![run, limit], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (locator, attempt) = row?;
        out.push(ResultDeleteClaim {
            locator,
            run: id("run_id", RunId::parse(run))?,
            attempt: id("attempt_id", AttemptId::parse(&attempt))?,
        });
    }
    Ok(out)
}

/// The file of `claim` is gone: the claim and the locator are forgotten in
/// one transaction.
pub(super) fn finish_claim(tx: &Connection, claim: &ResultDeleteClaim) -> Result<()> {
    let n = tx.execute(
        "DELETE FROM result_delete_claim WHERE locator=?1 AND run_id=?2 AND attempt_id=?3",
        params![claim.locator, claim.run.as_str(), claim.attempt.as_str()],
    )?;
    if n != 1 {
        return Err(StorageError::Invalid(
            "result deletion claim changed".into(),
        ));
    }
    common::complete_delete(tx, &claim.locator)?;
    Ok(())
}
