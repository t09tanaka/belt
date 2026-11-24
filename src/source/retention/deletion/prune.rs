//! The steps of the pruning of a run that touch the source store and the
//! Vault (spec 25.5, steps 2, 4 and 5).
//!
//! Everything here runs under source EX, taken without waiting, as the
//! deletion of an abandoned capture does: no capture writes meanwhile and no
//! run row appears. What authorizes the deletion is the mark of the run in
//! the parent database, read again under the lock; the mark is never
//! cleared, and a marked run is refused every reader, every retry and every
//! delivery (spec 25.3), so nothing reads its inputs again. No period and no
//! clock is asked: the mark settled them.
//!
//! Each call starts from what is still there (the Vault bundle, the roots
//! and claims of the index, the files named by the run ID), so a pass after
//! a crash does the rest. The steps of the deletion of an abandoned capture
//! are used as they are: the Vault's own claim, unlink and completion, and
//! the release of a root's members with the proof of every file.
use super::abandon::{would_block, AbandonCursor, AbandonReport, Budget};
use super::*;
use crate::ids::RunId;
use crate::secrets::vault::Vault;

/// What one call did to the inputs of a marked run.
#[derive(Debug, Default)]
pub struct PruneInputs {
    /// The source lock or the Vault was held by someone: nothing was done,
    /// a later pass tries again.
    pub busy: bool,
    /// Nothing of the step is left.
    pub done: bool,
    /// Object and manifest files claimed in this call (at most 32).
    pub paths: u32,
    /// What could not be proven or deleted: kept, and counted.
    pub unknown: u32,
    /// Scratch directories kept (also in `unknown`), by kind.
    pub scratch: super::ScratchCounts,
    pub errors: Vec<String>,
    /// The gate that held step 4, if one did.
    pub gate: Option<super::Gate>,
}

/// The input bound of one pass of the pruning (spec 25.5): 32 object and
/// manifest paths and 64 MiB of validation, shared by every run of the pass.
#[derive(Debug)]
pub struct PruneBudget(Budget);

impl PruneBudget {
    /// The bound of one pass.
    pub fn pass() -> Self {
        Self(Budget {
            paths: PAGE as u32,
            bytes: BYTE_BUDGET,
        })
    }

    /// True when the pass can claim no more input path.
    pub fn exhausted(&self) -> bool {
        self.0.paths == 0
    }
}

/// The source lock held exclusively, taken without waiting: the last row
/// transaction of a pruned run holds it while it deletes the run's
/// `source_ref` (spec 25.5, step 6). A submit writes `source_ref` and later
/// inserts its run under the same lock held shared, so under this one no
/// submit is between the two.
#[derive(Debug)]
pub struct ExclusiveGuard(Guard);

impl ExclusiveGuard {
    pub(crate) fn root(&self) -> &std::path::Path {
        &self.0.root
    }
}

impl SourceStore {
    /// `None` while a submit or a capture holds the source lock.
    pub fn prune_rows_guard(&self) -> Result<Option<ExclusiveGuard>> {
        match self.retention.guard(true) {
            Ok(guard) => Ok(Some(ExclusiveGuard(guard))),
            Err(error) if would_block(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl Index {
    /// The index the pruning finds the import sessions of a run by (spec
    /// 25.5, 25.13). A new index has it from its first open; an index of
    /// version 1 gets it on its next open. The version stays 1, as for the
    /// owner table: nothing an older binary reads changed.
    pub(in crate::source::retention) fn ensure_import_session_index(
        &self,
        db: &Connection,
    ) -> Result<()> {
        let present: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='import_session_run')",
            [],
            |r| r.get(0),
        )?;
        if !present {
            db.execute_batch(
                "CREATE INDEX IF NOT EXISTS import_session_run ON import_session(run_id);",
            )?;
        }
        Ok(())
    }
}

impl SourceStore {
    /// The index connection of a deletion for the marked run `run`, under
    /// the source lock. `None` when the lock is busy.
    fn prune_open(
        &self,
        owner: &crate::storage::CoordinatorStore,
        run: &RunId,
    ) -> Result<Option<(Guard, Connection)>> {
        let guard = match self.retention.guard(true) {
            Ok(guard) => guard,
            Err(error) if would_block(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        self.retention.check_anchor()?;
        let identity = owner.check_input_store(&self.retention.path, &self.retention.root)?;
        let c = self.retention.connect()?;
        let registered: Option<String> = c
            .query_row(
                "SELECT value FROM meta WHERE key='retention_owner'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        ensure!(
            registered.as_deref().is_none_or(|old| old == identity),
            "source retention owner identity changed"
        );
        c.execute(
            "INSERT OR IGNORE INTO meta VALUES('retention_owner',?1)",
            [&identity],
        )?;
        // The authority of every deletion below, read under the lock.
        ensure!(
            owner.run_is_pruned(run)?,
            "the inputs of a run that is not marked for pruning are not deleted"
        );
        Ok(Some((guard, c)))
    }

    /// Step 2: the secret bundle of the run in the parent Vault. Usually it
    /// went when the run ended (chapter 23, C); what is left goes by the
    /// Vault's own claim, unlink and completion.
    pub fn prune_run_vault(
        &self,
        owner: &crate::storage::CoordinatorStore,
        vault: &Vault,
        run: &RunId,
        budget: &mut PruneBudget,
    ) -> Result<PruneInputs> {
        let Some((_guard, c)) = self.prune_open(owner, run)? else {
            return Ok(PruneInputs {
                busy: true,
                ..Default::default()
            });
        };
        let done = self.abandon_vault(owner, vault, &c, run, &mut budget.0.bytes, "run_pruned")?;
        Ok(PruneInputs {
            busy: !done,
            done,
            ..Default::default()
        })
    }

    /// Steps 4 and 5: the root of the run with the manifests and objects no
    /// other root holds, the bundles of its source when no other run names
    /// the source, then the files and index rows named by the run ID, and
    /// last the run lock (25.1 (A)).
    pub fn prune_run_inputs(
        &self,
        owner: &crate::storage::CoordinatorStore,
        run: &RunId,
        cursor: &mut AbandonCursor,
        budget: &mut PruneBudget,
    ) -> Result<PruneInputs> {
        // The bound of the pass is reached: a later pass continues.
        if budget.exhausted() {
            return Ok(PruneInputs::default());
        }
        let Some((_guard, mut c)) = self.prune_open(owner, run)? else {
            return Ok(PruneInputs {
                busy: true,
                ..Default::default()
            });
        };
        // 4. The root of the run stops protecting its members; a pending
        // root its capture left (named by the owner row) goes with it.
        let pending = self
            .retention
            .capture_owner(run.as_str())?
            .and_then(|row| row.root);
        // The scratch directory of that capture goes first, whole, as in the
        // deletion of an abandoned capture: a file whose install was cut
        // short still has its staged name, and a file with two names is not
        // provably ours.
        if let Some(root) = pending.as_deref() {
            if let Err(error) = self.retention.remove_scratch_noted(&c, root) {
                // (a kept directory is counted by its kind; the run waits
                // for a later pass)
                let mut scratch = super::ScratchCounts::default();
                if !scratch.count(&error) {
                    return Err(error);
                }
                return Ok(PruneInputs {
                    unknown: 1,
                    scratch,
                    errors: vec![format!("{error:#}")],
                    ..Default::default()
                });
            }
        }
        let mut report = AbandonReport::default();
        let gone = self.abandon_roots(
            &mut c,
            run,
            pending.as_deref(),
            &mut budget.0,
            cursor,
            &mut report,
        )?;
        let mut out = PruneInputs {
            paths: report.paths,
            unknown: report.unknown,
            errors: report.errors,
            gate: report.gate,
            ..Default::default()
        };
        if !gone {
            return Ok(out);
        }
        fault("prune_after_roots")?;
        // The bundles of a source go with the last run that names it. A
        // submit holds the source lock shared from its check of the source
        // to the insert of its run row (`enqueue_guarded`), so under this
        // lock "no other run" holds; a bundle that is asked for again is
        // written again. The lock is let go when this call returns: the
        // `source_ref` of the source goes in step 6, in the last row
        // transaction, which takes the lock exclusively again for it.
        if let Some((Some(source), false)) = owner.run_source(run)? {
            if let Some(ci) = crate::source::classify::ci_id_of(&source) {
                self.remove_bundles_held(&ci)?;
            }
        }
        // 5. The files named by the run ID.
        self.retention
            .unlink_run_file("secrets", true, &format!("{run}.json"))?;
        self.retention
            .unlink_run_file("../in-place", true, &format!("{run}.json"))?;
        self.retention
            .unlink_run_file("runs", false, run.as_str())?;
        fault("prune_after_files")?;
        // The index rows of the run. An unfinished claim of the Vault stops
        // here: step 2 of a later pass finishes it.
        c.execute(
            "DELETE FROM vault_delete_claim WHERE run_id=?1 AND done=1",
            [run.as_str()],
        )?;
        let unfinished: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM vault_delete_claim WHERE run_id=?1)",
            [run.as_str()],
            |r| r.get(0),
        )?;
        if unfinished {
            return Ok(out);
        }
        c.execute("DELETE FROM import_session WHERE run_id=?1", [run.as_str()])?;
        c.execute("DELETE FROM run_expiry WHERE run_id=?1", [run.as_str()])?;
        c.execute("DELETE FROM capture_owner WHERE run_id=?1", [run.as_str()])?;
        // Last, the run lock: only the capture of this run opens it, and
        // that capture ended before the run had a row.
        self.retention
            .unlink_run_file("runs", false, &format!("{run}.lock"))?;
        out.done = true;
        Ok(out)
    }
}

/// Stop the steps of this thread right after `stage` (a test of a resident
/// that is stopped there).
#[cfg(test)]
pub fn set_prune_fault(stage: Option<&'static str>) {
    super::set_fault(stage);
}
