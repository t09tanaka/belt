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
            let mut statement=c.prepare("SELECT a.termination_json FROM task t JOIN attempt a USING(task_id) WHERE t.run_id=?1")?;
            let mut rows=statement.query([&run])?;
            while let Some(row)=rows.next()? {
                let termination:String=row.get(0)?;
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
    pub fn result_gc_guard(&self, run: &RunId) -> Result<RunPin> {
        if self._lock.is_none() {
            return Err(StorageError::Invalid("read-only owner".into()));
        }
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
            for (reference,attempt,locator,pins) in refs {
                if !ResultStore::owns(&locator) {report.foreign+=1;continue;}
                let rec=load_attempt(tx,&attempt)?.ok_or_else(||not_found("GC attempt"))?;
                let blocked:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt a LEFT JOIN collection c USING(attempt_id) WHERE a.attempt_id=?1 AND (a.observation<>'known' OR (a.state<>'terminal' AND a.retired=0) OR (a.retired=0 AND (c.closed_at_ms IS NULL OR (c.acked_at_ms IS NULL AND a.collection_failed_at_ms IS NULL)))))",[&attempt],|r|r.get(0))?;
                if blocked {continue;}
                let (required,persisted)=artifact_counts(tx,&attempt)?;
                let closed=collect::closed_at(tx,&attempt)?;
                let facts=GcCandidate{terminal_at_ms:rec.terminal_at_ms.map(|t|t.max(closed.unwrap_or(0))),retired_at_ms:rec.retired_at_ms,collected:rec.receipts.reached(Receipt::Collected),collection_failed_at_ms:rec.collection_failed_at_ms,missing_required_artifacts:required-persisted,pins};
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
        self.db.call(move |c| {
            let mut s=c.prepare("SELECT locator,attempt_id FROM result_delete_claim WHERE run_id=?1 ORDER BY locator LIMIT ?2")?;
            let rows=s.query_map(params![run,limit],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
            let mut out=Vec::new();
            for row in rows {let (locator,attempt)=row?;out.push(ResultDeleteClaim{locator,run:id("run_id",RunId::parse(&run))?,attempt:id("attempt_id",AttemptId::parse(&attempt))?});}
            Ok(out)
        })
    }
    pub fn finish_result_delete(&self, guard: &RunPin, claim: &ResultDeleteClaim) -> Result<()> {
        guard.check_exclusive(&self.state_dir, &claim.run)?;
        let (locator, run, attempt) = (
            claim.locator.clone(),
            claim.run.to_string(),
            claim.attempt.to_string(),
        );
        self.db.write(move |tx| {
            let n = tx.execute(
                "DELETE FROM result_delete_claim WHERE locator=?1 AND run_id=?2 AND attempt_id=?3",
                params![locator, run, attempt],
            )?;
            if n != 1 {
                return Err(StorageError::Invalid(
                    "result deletion claim changed".into(),
                ));
            }
            common::complete_delete(tx, &locator)?;
            Ok(())
        })
    }
}

/// Caller holds source EX; history scan is deliberate and not a bounded-ref claim.
fn input_retention_end_on(c: &Connection, run: &str, source: &str) -> Result<Option<u64>> {
    let ended:Option<i64>=c.query_row("SELECT CASE WHEN
              EXISTS(SELECT 1 FROM run WHERE run_id=?1 AND source_id=?2)
              AND EXISTS(SELECT 1 FROM task WHERE run_id=?1)
              AND NOT EXISTS(SELECT 1 FROM task t WHERE t.run_id=?1 AND
                (NOT EXISTS(SELECT 1 FROM attempt a WHERE a.task_id=t.task_id)
                 OR (t.cancel_requested_at_ms IS NULL AND EXISTS(SELECT 1 FROM run r WHERE r.run_id=t.run_id AND r.closed_at_ms IS NULL) AND NOT EXISTS(SELECT 1 FROM attempt a WHERE a.task_id=t.task_id AND a.retired=0 AND (a.generation>=t.min_generation OR a.state<>'terminal')))))
              AND NOT EXISTS(SELECT 1 FROM task t JOIN attempt a USING(task_id) LEFT JOIN collection x USING(attempt_id)
                WHERE t.run_id=?1 AND (a.observation<>'known' OR a.state<>'terminal' OR a.terminal_at_ms IS NULL OR a.termination_json IS NULL OR json_valid(a.termination_json)=0 OR x.closed_at_ms IS NULL OR x.verdict IS NULL OR x.verdict NOT IN ('complete','required_missing','gave_up')))
              THEN (SELECT max(max(coalesce(a.terminal_at_ms,0),coalesce(x.closed_at_ms,0))) FROM task t JOIN attempt a USING(task_id) JOIN collection x USING(attempt_id) WHERE t.run_id=?1)
              ELSE NULL END",params![run,source],|r|r.get(0))?;
    ended.map(to_u64).transpose()
}
