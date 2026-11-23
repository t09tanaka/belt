//! The release of a worker's import tokens whose owner ended (spec 25.8,
//! stage 5a): the memberships of an abandoned `import:<token>` root go
//! through [`Index::release_member`], so a file only that token held is
//! claimed and unlinked, and one another root holds keeps that root.
//!
//! The caller holds the source mutation lock exclusively. A claim is made
//! only when it can drain in the same section: while a root of unknown
//! membership or an unfinished graph exists (another receipt in flight, a
//! registration), objects no other root holds keep their membership and
//! wait for a later pass; shared ones are detached all the same.
use super::*;

/// What one pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ImportReleaseReport {
    /// Token roots (and their rows) that are gone.
    pub released: usize,
    /// Files that were claimed and unlinked.
    pub unlinked: usize,
    /// Memberships kept for a later pass: the store was not quiet, or the
    /// file could not be proven (for `doctor`).
    pub kept: usize,
    /// The pass stopped at its bound or at a failed drain.
    pub stopped: bool,
}

/// Memberships one pass looks at.
const MEMBERS: usize = 1000;

impl Index {
    /// One bounded pass over the abandoned token roots.
    pub(in crate::source::retention) fn release_abandoned_imports(
        &self,
    ) -> Result<ImportReleaseReport> {
        let mut report = ImportReleaseReport::default();
        let mut c = self.connect()?;
        let mut budget = BYTE_BUDGET;
        // A claim a crash left is finished first.
        let replay: Vec<String> = {
            let mut s = c.prepare("SELECT claim_json FROM source_delete_claim WHERE done=0 ORDER BY object,incarnation LIMIT 32")?;
            let rows = s.query_map([], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        // the gates of every other deletion (legacy registration, a root
        // of unknown membership, an unfinished graph)
        let quiet = |c: &Connection| -> Result<bool> { Ok(Gate::closed(c)?.is_none()) };
        if !replay.is_empty() {
            if !quiet(&c)? {
                report.stopped = true;
                return Ok(report);
            }
            for raw in replay {
                let claim: Claim = serde_json::from_str(&raw)?;
                if self.drain_claim(&mut c, &claim, &mut budget).is_err() {
                    report.stopped = true;
                    return Ok(report);
                }
            }
        }
        let roots: Vec<String> = {
            let mut s = c.prepare(
                "SELECT id FROM root WHERE kind='abandoned' AND id LIKE 'import:%' ORDER BY id LIMIT 32",
            )?;
            let rows = s.query_map([], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let mut looked = 0usize;
        for root in roots {
            let mut after = String::new();
            loop {
                let objects: Vec<(String, String)> = {
                    let mut s = c.prepare("SELECT m.object,o.path FROM member m JOIN object o ON o.id=m.object WHERE m.root=?1 AND m.object>?2 ORDER BY m.object LIMIT 32")?;
                    let rows = s.query_map(params![root, after], |r| Ok((r.get(0)?, r.get(1)?)))?;
                    rows.collect::<std::result::Result<_, _>>()?
                };
                for (object, path) in &objects {
                    after = object.clone();
                    if looked == MEMBERS {
                        report.stopped = true;
                        return Ok(report);
                    }
                    looked += 1;
                    let shared: bool = c.query_row(
                        "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1 AND root<>?2)",
                        params![object, root],
                        |r| r.get(0),
                    )?;
                    if !shared && !quiet(&c)? {
                        report.kept += 1;
                        continue;
                    }
                    if !shared && self.destination_absent(object, path) {
                        // Named before its file was written, and never
                        // written: nothing to prove or unlink.
                        let tx = c.transaction()?;
                        tx.execute(
                            "DELETE FROM member WHERE root=?1 AND object=?2",
                            params![root, object],
                        )?;
                        let unheld = "NOT EXISTS(SELECT 1 FROM member WHERE object=?1) AND NOT EXISTS(SELECT 1 FROM source_delete_claim WHERE object=?1)";
                        tx.execute(
                            &format!("DELETE FROM object WHERE id=?1 AND {unheld}"),
                            [object],
                        )?;
                        tx.execute(
                            &format!("DELETE FROM publication WHERE object=?1 AND {unheld}"),
                            [object],
                        )?;
                        tx.commit()?;
                        continue;
                    }
                    match self.release_member(
                        &mut c,
                        &root,
                        object,
                        path,
                        &mut budget,
                        true,
                        true,
                    )? {
                        Released::Shared(_) => {}
                        Released::Unproven(_) => report.kept += 1,
                        Released::Claimed(Ok(())) => report.unlinked += 1,
                        Released::Claimed(Err(_)) => {
                            // durable: the next pass replays it
                            report.stopped = true;
                            return Ok(report);
                        }
                    }
                }
                if objects.len() < 32 {
                    break;
                }
            }
            let tx = c.transaction()?;
            let removed = tx.execute(
                "DELETE FROM root WHERE id=?1 AND kind='abandoned' AND NOT EXISTS(SELECT 1 FROM member WHERE root=?1)",
                [&root],
            )?;
            if removed == 1 {
                tx.execute("DELETE FROM graph WHERE root=?1", [&root])?;
                tx.execute(
                    "DELETE FROM import_session WHERE token=?1 AND state='pending' AND source IS NULL AND run_id IS NULL",
                    [root.trim_start_matches("import:")],
                )?;
                report.released += 1;
            }
            tx.commit()?;
        }
        Ok(report)
    }
}
