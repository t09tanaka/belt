//! Deleting a capture that was never submitted (spec 25.4 (b)).
//!
//! Everything here runs under source EX. Every capture holds the source lock
//! shared from the moment it opens the store until its run is bound and its
//! secret bundle is in the Vault, and an enqueue holds it shared until its
//! run row is committed. So while this code runs no capture writes, and no
//! run row appears. What is deleted is named by one `capture_owner` row (the
//! run ID and the pending root it carries) and by nothing else: no directory
//! is listed here. A capture is deleted only when its owner wrote `given_up`,
//! is proven gone, or had its enqueue refused by this resident, and only
//! after the parent database said, again, that the run has no row.
//!
//! The order is the order of the spec's list: the parent rows of the run's
//! cancel, the Vault bundle, the plaintext bundle, the in-place binding, the
//! run record, the roots with their objects, the run lock, and last the owner
//! row. Each step is idempotent, and a step that cannot finish (a file that
//! cannot be proven ours, a busy Vault, the page budget) leaves the owner row,
//! so a later pass continues from the row.
use super::*;
use crate::ids::RunId;
use crate::secrets::vault::Vault;
use crate::source::retention::{CaptureOwner, CaptureVerdict};

/// Which owner rows a call looks at.
#[derive(Debug, Clone, Copy)]
pub enum AbandonScope<'a> {
    /// Every row: the pass of the retention driver.
    All,
    /// One run, judged like any other (the refusal of a cancel).
    Run(&'a RunId),
    /// One run whose enqueue this resident just refused: the refusal ends the
    /// capture, whatever its owner's state, unless the run has a row.
    Refused(&'a RunId),
}

#[derive(Debug, Default)]
pub struct AbandonReport {
    /// The source lock was held by a capture or an enqueue: nothing was read.
    pub busy: bool,
    pub examined: u32,
    /// `capturing` with a live owner: untouched.
    pub in_progress: u32,
    /// Rows whose owner cannot be judged from here: untouched, for `doctor`.
    pub elsewhere: u32,
    /// Rows of a run that has a row: only the owner row was deleted.
    pub handed_off: u32,
    /// Abandoned captures of which nothing is left.
    pub finished: u32,
    /// Abandoned captures a later pass continues.
    pub continuing: u32,
    /// Object and manifest files claimed in this call (at most 32).
    pub paths: u32,
    pub unknown: u32,
    pub errors: Vec<String>,
}

/// Where the last pass stopped inside one root, so that members that cannot
/// be proven do not hide the ones after them.
#[derive(Debug, Default)]
pub struct AbandonCursor {
    root: String,
    object: String,
}

struct Budget {
    paths: u32,
    bytes: u64,
}

enum Outcome {
    HandedOff,
    Finished,
    Continuing,
}

fn would_block(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::WouldBlock)
    })
}

/// Unlink `name` in `dir` when it is provably a file belt wrote there: a
/// regular file of this user with one name. Absence is success. Anything
/// else (a symlink, a directory, another owner, a second link) is an error,
/// and the file stays.
fn unlink_owned(dir: &File, name: &str) -> Result<()> {
    use rustix::fs::{AtFlags, FileType};
    let stat = match rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(rustix::io::Errno::NOENT) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile
            && stat.st_uid == rustix::process::geteuid().as_raw()
            && stat.st_nlink == 1,
        "a file of an abandoned capture is not provably ours"
    );
    rustix::fs::unlinkat(dir, name, AtFlags::empty())?;
    dir.sync_all()?;
    Ok(())
}

impl Index {
    /// A directory of this store (or, with `..`, of its state directory) in
    /// which a file of a run is named by the run ID. `None` when it does not
    /// exist: then no such file exists.
    fn run_directory(&self, name: &str, private: bool) -> Result<Option<File>> {
        match rustix::fs::openat(&*self.root, name, DIR, rustix::fs::Mode::empty()) {
            Ok(fd) => {
                let file: File = fd.into();
                directory(&file, private)?;
                Ok(Some(file))
            }
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    fn unlink_run_file(&self, directory: &str, private: bool, name: &str) -> Result<()> {
        match self.run_directory(directory, private)? {
            Some(dir) => unlink_owned(&dir, name),
            None => Ok(()),
        }
    }
}

impl SourceStore {
    /// One bounded pass over the owner rows in `scope`. Never waits for the
    /// source lock: a busy lock is reported and a later pass does the work.
    pub fn abandon_captures(
        &self,
        owner: &crate::storage::CoordinatorStore,
        vault: &Vault,
        scope: AbandonScope<'_>,
        cursor: &mut AbandonCursor,
    ) -> Result<AbandonReport> {
        let _guard = match self.retention.guard(true) {
            Ok(guard) => guard,
            Err(error) if would_block(&error) => {
                return Ok(AbandonReport {
                    busy: true,
                    ..Default::default()
                })
            }
            Err(error) => return Err(error),
        };
        let rows = match scope {
            AbandonScope::All => self.retention.capture_owners()?,
            AbandonScope::Run(run) | AbandonScope::Refused(run) => self
                .retention
                .capture_owner(run.as_str())?
                .into_iter()
                .collect(),
        };
        let mut report = AbandonReport::default();
        if rows.is_empty() {
            return Ok(report);
        }
        #[cfg(test)]
        self.retention.pause_page();
        self.retention.check_anchor()?;
        let identity = owner.check_input_store(&self.retention.path, &self.retention.root)?;
        let mut c = self.retention.connect()?;
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
        let refused = matches!(scope, AbandonScope::Refused(_));
        let mut budget = Budget {
            paths: PAGE as u32,
            bytes: BYTE_BUDGET,
        };
        for row in rows {
            report.examined += 1;
            let before = report.unknown;
            match self.abandon_one(
                owner,
                vault,
                &mut c,
                &row,
                refused,
                &mut budget,
                cursor,
                &mut report,
            ) {
                Ok(Some(Outcome::HandedOff)) => report.handed_off += 1,
                Ok(Some(Outcome::Finished)) => report.finished += 1,
                Ok(Some(Outcome::Continuing)) => report.continuing += 1,
                Ok(None) => {}
                Err(error) => {
                    report.continuing += 1;
                    if report.unknown == before {
                        report.unknown += 1;
                    }
                    report.errors.push(format!("{error:#}"));
                }
            }
        }
        Ok(report)
    }

    #[allow(clippy::too_many_arguments)]
    fn abandon_one(
        &self,
        owner: &crate::storage::CoordinatorStore,
        vault: &Vault,
        c: &mut Connection,
        row: &CaptureOwner,
        refused: bool,
        budget: &mut Budget,
        cursor: &mut AbandonCursor,
        report: &mut AbandonReport,
    ) -> Result<Option<Outcome>> {
        let run = RunId::parse(&row.run)?;
        let forget_owner = |c: &Connection| -> Result<()> {
            c.execute("DELETE FROM capture_owner WHERE run_id=?1", [run.as_str()])?;
            Ok(())
        };
        // A run that has a row took its capture over, whoever its owner is.
        if owner.has_run(&run)? {
            forget_owner(c)?;
            return Ok(Some(Outcome::HandedOff));
        }
        match row.verdict() {
            CaptureVerdict::Abandoned => {}
            _ if refused => {}
            CaptureVerdict::InProgress => {
                report.in_progress += 1;
                return Ok(None);
            }
            CaptureVerdict::Elsewhere => {
                report.elsewhere += 1;
                return Ok(None);
            }
        }
        // 1. The parent rows of the run's cancel, in the transaction that
        // says again that the run has no row.
        if owner.forget_unsubmitted_run(&run)? {
            forget_owner(c)?;
            return Ok(Some(Outcome::HandedOff));
        }
        if !row.given_up {
            // The refusal, or the proof that the owner is gone, is durable
            // before anything of the capture is deleted: a later pass needs
            // no second proof.
            c.execute(
                "UPDATE capture_owner SET state='given_up' WHERE run_id=?1",
                [run.as_str()],
            )?;
        }
        fault("abandon_after_rows")?;
        // 2. The Vault bundle, by the Vault's own claim, unlink and
        // completion, and then the completed claim.
        if !self.abandon_vault(owner, vault, c, &run, &mut budget.bytes)? {
            return Ok(Some(Outcome::Continuing));
        }
        fault("abandon_after_vault")?;
        // 3. to 5. The plaintext bundle, the in-place binding, the run record.
        self.retention
            .unlink_run_file("secrets", true, &format!("{run}.json"))?;
        self.retention
            .unlink_run_file("../in-place", true, &format!("{run}.json"))?;
        self.retention
            .unlink_run_file("runs", false, run.as_str())?;
        fault("abandon_after_files")?;
        // 6. The run root and the pending root, with what only they hold.
        if !self.abandon_roots(c, &run, row.root.as_deref(), budget, cursor, report)? {
            return Ok(Some(Outcome::Continuing));
        }
        fault("abandon_after_roots")?;
        // 7. The run lock: only the capture of this run opens it, and that
        // capture is over (25.1 (A)).
        self.retention
            .unlink_run_file("runs", false, &format!("{run}.lock"))?;
        // 8. Last, the row that named all of it.
        forget_owner(c)?;
        Ok(Some(Outcome::Finished))
    }

    /// False when the Vault is busy: nothing was done, a later pass tries
    /// again.
    fn abandon_vault(
        &self,
        owner: &crate::storage::CoordinatorStore,
        vault: &Vault,
        c: &Connection,
        run: &RunId,
        bytes: &mut u64,
    ) -> Result<bool> {
        ensure!(
            vault.matches_source(&self.retention.path),
            "Vault is not this parent's Vault"
        );
        let lease = match vault.lease(true) {
            Ok(lease) => lease,
            Err(error) => {
                let error = anyhow::Error::new(error);
                if would_block(&error) {
                    return Ok(false);
                }
                return Err(error);
            }
        };
        let claimed: Option<(String, bool)> = c
            .query_row(
                "SELECT claim_json,done FROM vault_delete_claim WHERE run_id=?1",
                [run.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let pending = match claimed {
            Some((_, true)) => None,
            Some((raw, false)) => {
                let claim: crate::secrets::vault::Claim = serde_json::from_str(&raw)?;
                let (registration, _) = self.vault_authority(owner, c, &lease)?;
                lease.reserve(run, bytes, 1)?;
                Some((registration, claim))
            }
            None if lease.has(run)? => {
                let (registration, _) = self.vault_authority(owner, c, &lease)?;
                lease.reserve(run, bytes, 2)?;
                let claim = vault.claim_for_retention(&lease, run)?;
                registration.check(c)?;
                // The claim of a run that never had a row names no source
                // binding: the run ID alone names the bundle.
                c.execute(
                    "INSERT INTO vault_delete_claim(run_id,source_id,binding_digest,claim_json,done) VALUES(?1,'','capture_abandoned',?2,0)",
                    params![run.as_str(), serde_json::to_string(&claim)?],
                )?;
                crate::secrets::vault::retention_fault("after_claim")?;
                Some((registration, claim))
            }
            None => None,
        };
        if let Some((registration, claim)) = pending {
            registration.check(c)?;
            lease.drain(run, &claim)?;
            c.execute(
                "UPDATE vault_delete_claim SET done=1 WHERE run_id=?1 AND done=0",
                [run.as_str()],
            )?;
        }
        c.execute(
            "DELETE FROM vault_delete_claim WHERE run_id=?1 AND done=1",
            [run.as_str()],
        )?;
        Ok(true)
    }

    /// True when neither root is left.
    fn abandon_roots(
        &self,
        c: &mut Connection,
        run: &RunId,
        pending: Option<&str>,
        budget: &mut Budget,
        cursor: &mut AbandonCursor,
        report: &mut AbandonReport,
    ) -> Result<bool> {
        if let Some(pending) = pending {
            ensure!(
                pending.starts_with("capture:"),
                "a capture owner names a root that is not a capture's"
            );
        }
        let named: Vec<&str> = std::iter::once(run.as_str()).chain(pending).collect();
        // An abandoned root protects nothing: it stops counting as a root
        // whose membership is unknown, and its unfinished graph is dropped.
        let tx = c.transaction()?;
        for root in &named {
            tx.execute("UPDATE root SET kind='abandoned' WHERE id=?1", [root])?;
            tx.execute("DELETE FROM graph WHERE root=?1", [root])?;
        }
        tx.commit()?;
        let mut roots = Vec::new();
        for root in named {
            let exists: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1)",
                [root],
                |r| r.get(0),
            )?;
            if exists {
                roots.push(root);
            }
        }
        if !roots.is_empty() {
            // The gates of every other deletion of an object: nothing is
            // unlinked while an unindexed manifest, a root of unknown
            // membership or an unfinished graph could need it.
            let held: bool = c.query_row(
                "SELECT (SELECT value FROM meta WHERE key='legacy_registration')<>'done' OR EXISTS(SELECT 1 FROM root WHERE source IS NULL AND kind<>'abandoned') OR EXISTS(SELECT 1 FROM graph WHERE done=0)",
                [],
                |r| r.get(0),
            )?;
            if held {
                return Ok(false);
            }
            // A claim of this deletion that a crash left is finished first.
            // The claims of the page of expired runs are that page's.
            let replay: Vec<String> = {
                let mut s = c.prepare("SELECT claim_json FROM source_delete_claim WHERE done=0 ORDER BY object,incarnation LIMIT 32")?;
                let rows = s.query_map([], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            for raw in replay {
                let claim: Claim = serde_json::from_str(&raw)?;
                if !claim.abandoned {
                    continue;
                }
                if budget.paths == 0 {
                    return Ok(false);
                }
                budget.paths -= 1;
                report.paths += 1;
                if let Err(error) = self.retention.drain_claim(c, &claim, &mut budget.bytes) {
                    report.unknown += 1;
                    report.errors.push(format!("{error:#}"));
                    return Ok(false);
                }
            }
        }
        let mut gone = true;
        for root in roots {
            if cursor.root != root {
                cursor.root = root.to_owned();
                cursor.object.clear();
            }
            loop {
                let objects: Vec<(String, String)> = {
                    let mut s = c.prepare("SELECT m.object,o.path FROM member m JOIN object o ON o.id=m.object WHERE m.root=?1 AND m.object>?2 ORDER BY m.object LIMIT 32")?;
                    let rows =
                        s.query_map(params![root, cursor.object], |r| Ok((r.get(0)?, r.get(1)?)))?;
                    rows.collect::<std::result::Result<_, _>>()?
                };
                for (object, path) in &objects {
                    if budget.paths == 0 {
                        return Ok(false);
                    }
                    cursor.object = object.clone();
                    match self.retention.release_member(
                        c,
                        root,
                        object,
                        path,
                        &mut budget.bytes,
                        true,
                    )? {
                        Released::Shared => {}
                        Released::Unproven(error) => {
                            report.unknown += 1;
                            report.errors.push(error);
                        }
                        Released::Claimed(drained) => {
                            budget.paths -= 1;
                            report.paths += 1;
                            if let Err(error) = drained {
                                report.unknown += 1;
                                report.errors.push(format!("{error:#}"));
                            }
                        }
                    }
                }
                if objects.len() < PAGE {
                    cursor.object.clear();
                    break;
                }
            }
            // The row of a root goes when no membership is left; a member
            // that could not be proven keeps it, and the owner row with it.
            let removed = c.execute(
                "DELETE FROM root WHERE id=?1 AND kind='abandoned' AND NOT EXISTS(SELECT 1 FROM member WHERE root=?1)",
                [root],
            )?;
            gone &= removed == 1;
        }
        if gone {
            c.execute("DELETE FROM import_session WHERE run_id=?1", [run.as_str()])?;
            c.execute("DELETE FROM run_expiry WHERE run_id=?1", [run.as_str()])?;
        }
        Ok(gone)
    }
}
