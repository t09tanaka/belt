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
    pub errors: Vec<String>,
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
    ) -> Result<PruneInputs> {
        let Some((_guard, c)) = self.prune_open(owner, run)? else {
            return Ok(PruneInputs {
                busy: true,
                ..Default::default()
            });
        };
        let mut bytes = BYTE_BUDGET;
        let done = self.abandon_vault(owner, vault, &c, run, &mut bytes, "run_pruned")?;
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
    ) -> Result<PruneInputs> {
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
        let mut report = AbandonReport::default();
        let mut budget = Budget {
            paths: PAGE as u32,
            bytes: BYTE_BUDGET,
        };
        let gone = self.abandon_roots(
            &mut c,
            run,
            pending.as_deref(),
            &mut budget,
            cursor,
            &mut report,
        )?;
        let mut out = PruneInputs {
            paths: report.paths,
            unknown: report.unknown,
            errors: report.errors,
            ..Default::default()
        };
        if !gone {
            return Ok(out);
        }
        fault("prune_after_roots")?;
        // The bundles of a source go with the last run that names it. A run
        // row appears only under the source lock held shared, so "no other
        // run" holds while this runs; a bundle that is asked for again is
        // written again.
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
