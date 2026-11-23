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

/// Memberships one exclusive pass looks at.
const MEMBERS: usize = 1000;

/// Memberships one shared patrol looks at (one agent tick).
const PATROL: usize = 256;
const PATROL_ROOT: &str = "import_shared_after";
const PATROL_OBJECT: &str = "import_shared_object";

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
    /// it), for the one root `root`: memberships another root holds are
    /// detached, a page of 32 per short IMMEDIATE transaction, and the root
    /// goes with its rows when no member is left. Its cost is that root's
    /// memberships (one publication's), not the store's.
    pub(in crate::source::retention) fn release_shared_root(
        &self,
        root: &str,
    ) -> Result<ImportReleaseReport> {
        let mut report = ImportReleaseReport::default();
        let mut c = self.connect_for_import()?;
        let mut after = String::new();
        loop {
            let (finished, last, _) = shared_page(&mut c, root, &after, &mut report)?;
            if finished {
                return Ok(report);
            }
            after = last;
        }
    }

    /// The shared part of the agent's pass: a patrol over the memberships
    /// of the abandoned token roots, at most [`PATROL`] per call, resuming
    /// where the last call stopped (`meta`, [`PATROL_ROOT`] and
    /// [`PATROL_OBJECT`]) and wrapping around. However many roots wait for
    /// the gates, a call costs the same.
    pub(in crate::source::retention) fn release_shared_imports(
        &self,
    ) -> Result<ImportReleaseReport> {
        let mut report = ImportReleaseReport::default();
        let mut c = self.connect_for_import()?;
        let get = |c: &Connection, key: &str| -> Result<Option<String>> {
            Ok(
                c.query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
                    .optional()?,
            )
        };
        let stored_root = get(&c, PATROL_ROOT)?;
        let mut root_after = stored_root.clone().unwrap_or_default();
        // `Some`: part-way through `root_after`, after this member
        let stored_object = get(&c, PATROL_OBJECT)?;
        let mut object_after = stored_object.clone();
        let mut looked = 0usize;
        let mut wrapped = false;
        while looked < PATROL {
            let root: Option<String> = match &object_after {
                Some(_) => Some(root_after.clone()),
                None => c
                    .query_row(
                        "SELECT id FROM root WHERE kind='abandoned' AND id LIKE 'import:%' AND id>?1 ORDER BY id LIMIT 1",
                        [&root_after],
                        |r| r.get(0),
                    )
                    .optional()?,
            };
            let Some(root) = root else {
                if wrapped || root_after.is_empty() {
                    break;
                }
                wrapped = true;
                root_after.clear();
                continue;
            };
            let mut after = object_after.take().unwrap_or_default();
            loop {
                let (finished, last, rows) = shared_page(&mut c, &root, &after, &mut report)?;
                // (the rows looked at, not the pages: a root of few
                // members costs what it has; an empty one counts one)
                looked += rows.max(1);
                if finished {
                    root_after = root.clone();
                    object_after = None;
                    break;
                }
                after = last;
                if looked >= PATROL {
                    root_after = root.clone();
                    object_after = Some(after.clone());
                    break;
                }
            }
        }
        // (nothing is written when the cursor did not move)
        if stored_root.as_deref() == Some(root_after.as_str()) && stored_object == object_after {
            return Ok(report);
        }
        let tx = c.transaction()?;
        tx.execute(
            "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![PATROL_ROOT, root_after],
        )?;
        match &object_after {
            Some(o) => tx.execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![PATROL_OBJECT, o],
            )?,
            None => tx.execute("DELETE FROM meta WHERE key=?1", [PATROL_OBJECT])?,
        };
        tx.commit()?;
        Ok(report)
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
}

/// Paths `doctor` stats for [`abandoned_imports_read_only`], at most.
const DOCTOR_PATHS: i64 = 10_000;

/// For `doctor`: the abandoned token roots below `state_dir`, their
/// memberships, and the bytes of the files only they hold (no paths; of at
/// most [`DOCTOR_PATHS`] files; the last value is `true` when there were more,
/// so the byte total is a lower bound). Read only; `None` when there is no
/// index.
pub fn abandoned_imports_read_only(
    state_dir: &std::path::Path,
) -> Result<Option<(u64, u64, u64, bool)>> {
    let sources = state_dir.join("sources");
    let path = sources.join("retention.db");
    if !path.is_file() {
        return Ok(None);
    }
    let c = Connection::open_with_flags(
        &path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    c.busy_timeout(std::time::Duration::from_secs(1))?;
    let (roots, members): (i64, i64) = c.query_row(
        "SELECT (SELECT count(*) FROM root WHERE kind='abandoned' AND id LIKE 'import:%'), (SELECT count(*) FROM member m JOIN root r ON r.id=m.root WHERE r.kind='abandoned' AND r.id LIKE 'import:%')",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let paths: Vec<String> = {
        let mut s = c.prepare("SELECT o.path FROM member m JOIN root r ON r.id=m.root JOIN object o ON o.id=m.object WHERE r.kind='abandoned' AND r.id LIKE 'import:%' AND NOT EXISTS(SELECT 1 FROM member b WHERE b.object=m.object AND b.root<>m.root) LIMIT ?1")?;
        let rows = s.query_map([DOCTOR_PATHS + 1], |r| r.get(0))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    drop(c);
    let truncated = paths.len() as i64 > DOCTOR_PATHS;
    let bytes = paths
        .iter()
        .take(DOCTOR_PATHS as usize)
        .filter_map(|p| std::fs::symlink_metadata(sources.join(p)).ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum();
    Ok(Some((roots as u64, members as u64, bytes, truncated)))
}

/// One page (32 memberships after `after`, by object) of the abandoned
/// token root `root` in one IMMEDIATE transaction: the ones another root
/// holds are detached; when the page is the last, the root goes if it has
/// no member left. Returns whether the root is finished, and the last
/// membership looked at.
fn shared_page(
    c: &mut Connection,
    root: &str,
    after: &str,
    report: &mut ImportReleaseReport,
) -> Result<(bool, String, usize)> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Only an abandoned root's memberships are detached here: a live token
    // (or any other root) passed by mistake keeps all of them.
    let abandoned: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1 AND kind='abandoned')",
        [root],
        |r| r.get(0),
    )?;
    if !abandoned {
        return Ok((true, String::new(), 0));
    }
    let objects: Vec<(String, bool)> = {
        let mut s = tx.prepare("SELECT m.object, EXISTS(SELECT 1 FROM member o WHERE o.object=m.object AND o.root<>m.root) FROM member m WHERE m.root=?1 AND m.object>?2 ORDER BY m.object LIMIT 32")?;
        let rows = s.query_map(params![root, after], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    for (object, shared) in &objects {
        if *shared {
            tx.execute(
                "DELETE FROM member WHERE root=?1 AND object=?2",
                params![root, object],
            )?;
        }
    }
    let finished = objects.len() < 32;
    if finished
        && tx.execute(
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
    tx.commit()?;
    let last = objects.last().map(|(o, _)| o.clone()).unwrap_or_default();
    Ok((finished, last, objects.len()))
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
    crate::source::retention::Index::forget_object(&tx, object, unheld)?;
    tx.execute(
        &format!("DELETE FROM publication WHERE object=?1 AND {unheld}"),
        [object],
    )?;
    tx.commit()?;
    Ok(())
}
