//! The release of a worker's import tokens whose owner ended (spec 25.8,
//! stage 5a): the memberships of an abandoned `import:<token>` root go
//! through [`Index::release_member`], so a file only that token held is
//! claimed and unlinked, and one another root holds keeps that root.
//!
//! A pass has two parts. Under the shared mutation lock, memberships another
//! root holds are detached and empty token roots go. Under the exclusive
//! lock, and only while every gate is open (no legacy registration left, no
//! root of unknown membership, no unfinished graph), files no other root
//! holds are claimed and unlinked; until then they keep their membership
//! (counted by `doctor`).
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

/// Memberships one pass changes.
const MEMBERS: usize = 1000;

/// Where the exclusive pass resumes (a root id), so a root whose members
/// cannot be proven does not stop the others.
const CURSOR: &str = "import_release_after";

impl Index {
    /// Is there anything the pass under the exclusive lock would do? Only
    /// once the gates are open: while one is closed no claim can be made, and
    /// the exclusive lock would only make every other writer fail.
    pub(in crate::source::retention) fn import_release_due(&self) -> Result<bool> {
        let c = self.connect()?;
        if Gate::closed(&c)?.is_some() {
            return Ok(false);
        }
        Ok(c.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE kind='abandoned' AND id LIKE 'import:%')",
            [],
            |r| r.get(0),
        )?)
    }

    /// The members of the abandoned token root `root` whose file was never
    /// written and that no other root holds go with their object rows (as
    /// 25.4 (b) does). Needs only the shared lock: nothing drains under it.
    pub(in crate::source::retention) fn forget_absent_members(&self, root: &str) -> Result<()> {
        let mut c = self.connect()?;
        let mut after = String::new();
        loop {
            let objects: Vec<(String, String)> = {
                let mut s = c.prepare("SELECT m.object,o.path FROM member m JOIN object o ON o.id=m.object WHERE m.root=?1 AND m.object>?2 AND NOT EXISTS(SELECT 1 FROM member b WHERE b.object=m.object AND b.root<>m.root) ORDER BY m.object LIMIT 32")?;
                let rows = s.query_map(params![root, after], |r| Ok((r.get(0)?, r.get(1)?)))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            for (object, path) in &objects {
                after = object.clone();
                if self.destination_absent(object, path) {
                    forget_member(&mut c, root, object)?;
                }
            }
            if objects.len() < 32 {
                return Ok(());
            }
        }
    }

    /// The part of a pass that needs only the shared lock (the caller holds
    /// it): memberships another root holds are detached, and abandoned token
    /// roots left without members go with their rows. Only roots with such
    /// work are visited, so roots that wait for the gates cost nothing and
    /// never stop the pass; at most [`MEMBERS`] rows change.
    pub(in crate::source::retention) fn release_shared_imports(
        &self,
    ) -> Result<ImportReleaseReport> {
        let mut report = ImportReleaseReport::default();
        let mut c = self.connect_for_import()?;
        let mut changed = 0usize;
        let mut after = String::new();
        loop {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let roots: Vec<String> = {
                let mut s = tx.prepare("SELECT r.id FROM root r WHERE r.kind='abandoned' AND r.id LIKE 'import:%' AND r.id>?1 AND (NOT EXISTS(SELECT 1 FROM member m WHERE m.root=r.id) OR EXISTS(SELECT 1 FROM member m JOIN member o ON o.object=m.object AND o.root<>m.root WHERE m.root=r.id)) ORDER BY r.id LIMIT 32")?;
                let rows = s.query_map([&after], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let Some(last) = roots.last() else {
                tx.commit()?;
                return Ok(report);
            };
            let last = last.clone();
            let mut full = false;
            for root in &roots {
                let left = MEMBERS - changed;
                let detached = tx.execute(
                    "DELETE FROM member WHERE rowid IN (SELECT m.rowid FROM member m WHERE m.root=?1 AND EXISTS(SELECT 1 FROM member o WHERE o.object=m.object AND o.root<>m.root) LIMIT ?2)",
                    params![root, left as i64],
                )?;
                changed += detached;
                if changed == MEMBERS {
                    full = true;
                    break;
                }
                if tx.execute(
                    "DELETE FROM root WHERE id=?1 AND kind='abandoned' AND NOT EXISTS(SELECT 1 FROM member WHERE root=?1)",
                    [root],
                )? == 1
                {
                    tx.execute("DELETE FROM graph WHERE root=?1", [root])?;
                    tx.execute(
                        "DELETE FROM import_session WHERE token=?1 AND state='pending' AND source IS NULL AND run_id IS NULL",
                        [root.trim_start_matches("import:")],
                    )?;
                    report.released += 1;
                }
            }
            tx.commit()?;
            if full {
                report.stopped = true;
                return Ok(report);
            }
            after = last;
        }
    }

    /// The part of a pass that claims files no other root holds, under the
    /// exclusive lock and only while every gate is open. It resumes after
    /// the root the last pass ended at; at most [`MEMBERS`] memberships are
    /// looked at.
    pub(in crate::source::retention) fn release_abandoned_imports(
        &self,
    ) -> Result<ImportReleaseReport> {
        let mut report = ImportReleaseReport::default();
        let mut c = self.connect()?;
        let mut budget = BYTE_BUDGET;
        if Gate::closed(&c)?.is_some() {
            report.stopped = true;
            return Ok(report);
        }
        // A claim a crash left is finished first.
        let replay: Vec<String> = {
            let mut s = c.prepare("SELECT claim_json FROM source_delete_claim WHERE done=0 ORDER BY object,incarnation LIMIT 32")?;
            let rows = s.query_map([], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for raw in replay {
            let claim: Claim = serde_json::from_str(&raw)?;
            if self.drain_claim(&mut c, &claim, &mut budget).is_err() {
                report.stopped = true;
                return Ok(report);
            }
        }
        let start: String = c
            .query_row("SELECT value FROM meta WHERE key=?1", [CURSOR], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or_default();
        let mut roots: Vec<String> = {
            let mut s = c.prepare(
                "SELECT id FROM root WHERE kind='abandoned' AND id LIKE 'import:%' AND id>?1 ORDER BY id LIMIT 32",
            )?;
            let rows = s.query_map([&start], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if roots.is_empty() && !start.is_empty() {
            // wrap around
            let mut s = c.prepare(
                "SELECT id FROM root WHERE kind='abandoned' AND id LIKE 'import:%' ORDER BY id LIMIT 32",
            )?;
            let rows = s.query_map([], |r| r.get(0))?;
            roots = rows.collect::<std::result::Result<_, _>>()?;
        }
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
                    if !shared && self.destination_absent(object, path) {
                        // Named before its file was written, and never
                        // written: nothing to prove or unlink.
                        forget_member(&mut c, &root, object)?;
                        continue;
                    }
                    // (a shared member is only detached: nothing to examine)
                    match self.release_member(
                        &mut c,
                        &root,
                        object,
                        path,
                        &mut budget,
                        true,
                        !shared,
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
            tx.execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![CURSOR, root],
            )?;
            tx.commit()?;
        }
        Ok(report)
    }

    /// For `doctor`: the abandoned token roots, their memberships, and the
    /// bytes of the files only they hold (no paths).
    pub(in crate::source::retention) fn abandoned_imports(&self) -> Result<(u64, u64, u64)> {
        let c = self.connect()?;
        let (roots, members): (i64, i64) = c.query_row(
            "SELECT (SELECT count(*) FROM root WHERE kind='abandoned' AND id LIKE 'import:%'), (SELECT count(*) FROM member m JOIN root r ON r.id=m.root WHERE r.kind='abandoned' AND r.id LIKE 'import:%')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut bytes = 0u64;
        let mut s = c.prepare("SELECT o.path FROM member m JOIN root r ON r.id=m.root JOIN object o ON o.id=m.object WHERE r.kind='abandoned' AND r.id LIKE 'import:%' AND NOT EXISTS(SELECT 1 FROM member b WHERE b.object=m.object AND b.root<>m.root)")?;
        let paths = s.query_map([], |r| r.get::<_, String>(0))?;
        for path in paths {
            if let Ok(m) = std::fs::symlink_metadata(self.path.join(path?)) {
                if m.is_file() {
                    bytes += m.len();
                }
            }
        }
        Ok((roots as u64, members as u64, bytes))
    }
}

/// Remove the membership `object` of `root`, and the object's rows when no
/// root and no claim names it any more.
fn forget_member(c: &mut Connection, root: &str, object: &str) -> Result<()> {
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
    Ok(())
}
