//! The deleting pass of a worker's received sources (spec 25.8 5b (d)).
//!
//! A pass is a run of short sections, each under the exclusive mutation
//! lock taken without waiting. Step 0 finishes what is left (claims a crash
//! left, the members of abandoned `import:`, `drop:` and `receive:` roots);
//! step 1 is the gate; steps 2 and 3 pick the oldest unreferenced received
//! source the rules of (c) allow and detach it to a `drop:` root, whose
//! members the next sections release; step 4's `ref_drop:` rows record the
//! clone refs the detach orphaned (their removal is a later change).
//!
//! The worker agent runs it on a thread of its own, at most once a minute
//! and back to back while it makes progress (`Agent::received_passes`,
//! spec 25.8 5b (h)6), never on trial. Until `legacy_registration` is
//! `done` its gate keeps every pass from changing anything.
use super::import_release::forget_member;
use super::*;

/// The newest-first answer about one source from outside the index (spec
/// 25.8 5b (a)): `worker.db` and the in-process pins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceUse {
    /// A non-terminal attempt of any member names it, or a reader pins it.
    Live,
    /// No live use; the newest `terminal_at_ms` of an attempt that named it,
    /// if any (spec L6: used at that time).
    Ended(Option<i64>),
}

/// What a pass is given.
pub struct ReceivedPassInputs<'a> {
    /// Trusted clock samples (chapter 23), as the parent's pages take them.
    pub wall: u64,
    pub mono: u64,
    pub boot: Option<String>,
    /// 14 days in production.
    pub max_age_ms: u64,
    /// 20 GiB in production.
    pub cap_bytes: u64,
    /// Read under the exclusive lock; an error counts as referenced. It is
    /// asked with the source's two ids: the CI id (`src-<32 hex>`, the form
    /// the index and the in-process pins use) and the same source as a
    /// [`crate::ids::SourceId`] (`src_<ULID>`, the form `worker.db`'s
    /// `source_use` takes), converted by
    /// [`crate::source::classify::source_id_of_ci`]. `None` there is an id
    /// that does not convert: the pass keeps such a source whatever the
    /// answer, so a caller may answer `Live` without a lookup.
    pub uses: &'a dyn Fn(&str, Option<&crate::ids::SourceId>) -> Result<SourceUse>,
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReceivedPassReport {
    /// Sections run.
    pub sections: usize,
    /// Sources detached (their `receive:` root became a `drop:` root).
    pub detached: usize,
    /// Files claimed and unlinked.
    pub unlinked: usize,
    /// Rows removed without a file (shared memberships, absent files,
    /// replayed claims, empty roots).
    pub forgotten: usize,
    /// Memberships kept: the file could not be proven (or is larger than a
    /// section's budget).
    pub kept: usize,
    /// The store holds a parent's root: nothing was done.
    pub parent: bool,
    /// The exclusive lock was busy.
    pub busy: bool,
    /// A gate was closed.
    pub gate: Option<Gate>,
    /// The clock held: only the cap rule ran.
    pub clock_held: bool,
    /// A claim could not be drained: the pass stopped (the next one replays).
    pub stuck: bool,
    /// The pass stopped at its section bound with progress made: the caller
    /// may start the next one at once.
    pub again: bool,
}

/// Sections in one pass, and of them at most this many for step 0.
const SECTIONS: usize = 64;
const STEP0_SECTIONS: usize = 32;
/// Rows step 0 looks at in one section.
const ROWS: usize = 1000;
/// Candidates one section checks.
const CANDIDATES: usize = 32;
/// Where step 0 resumes: `root` and `object`, separated by a newline.
const DROP_CURSOR: &str = "drop_cursor";
/// The source clock of the expiry (its own key: `clock` is the parent's).
const CLOCK: &str = "received_clock";
/// The pause between sections, so a waiting reader takes the lock first.
const PAUSE: std::time::Duration = std::time::Duration::from_millis(100);

/// Step 0's page: the abandoned roots from the cursor in `root_kind`
/// order, then each one's members by the primary key (no scan of the other
/// roots' members, no sort).
const STEP0_ROOTS: &str = "SELECT id FROM root WHERE kind='abandoned' AND id>=?1
     AND (id LIKE 'import:%' OR id LIKE 'drop:%' OR id LIKE 'receive:%')
     AND EXISTS(SELECT 1 FROM member WHERE root=root.id) ORDER BY id LIMIT ?2";
const STEP0_MEMBERS: &str = "SELECT m.object,o.path,b.bytes FROM member m
     JOIN object o ON o.id=m.object LEFT JOIN object_bytes b ON b.id=m.object
     WHERE m.root=?1 AND m.object>?2 ORDER BY m.object LIMIT ?3";
/// The candidates of steps 2 and 3, oldest first: the roots without a time
/// (counted as -1) by id, then the others by time and id, each in the order
/// of `root_used_id` (no sort of every received root). A root waiting for
/// its `clone_ref` retry is not one.
const UNTIMED: &str = "SELECT r.id,r.source,r.used_ms FROM root r WHERE r.kind='received' AND r.used_ms IS NULL AND r.id>?1
     AND r.source IS NOT NULL AND r.id='receive:'||r.source
     AND NOT EXISTS(SELECT 1 FROM graph g WHERE g.source=r.source AND g.root<>r.id)
     AND NOT EXISTS(SELECT 1 FROM clone_ref_retry t WHERE t.root=r.id)
     ORDER BY r.id LIMIT ?2";
const TIMED: &str = "SELECT r.id,r.source,r.used_ms FROM root r WHERE r.kind='received' AND r.used_ms IS NOT NULL AND (r.used_ms,r.id)>(?1,?2)
     AND r.source IS NOT NULL AND r.id='receive:'||r.source
     AND NOT EXISTS(SELECT 1 FROM graph g WHERE g.source=r.source AND g.root<>r.id)
     AND NOT EXISTS(SELECT 1 FROM clone_ref_retry t WHERE t.root=r.id)
     ORDER BY r.used_ms,r.id LIMIT ?3";
/// The queries whose plans a test checks.
#[cfg(test)]
pub(in crate::source::retention) const PLANNED: [&str; 4] =
    [STEP0_ROOTS, STEP0_MEMBERS, UNTIMED, TIMED];

/// A member step 0 looks at: root, object, path, counted bytes.
type Member = (String, String, String, Option<i64>);

/// What step 0 did in a section.
enum Leftover {
    /// Nothing was left.
    None,
    /// Rows changed: the section ends there.
    Progress,
    /// Rows were looked at and none could change (unproven files).
    Stuck,
    /// A claim did not drain: the pass stops.
    Stop,
}

impl SourceStore {
    /// One pass (spec 25.8 5b (d)). The caller has checked that no version
    /// is on trial; a store that holds a parent's root is left alone.
    pub fn received_pass(&self, inputs: &ReceivedPassInputs) -> Result<ReceivedPassReport> {
        let mut report = ReceivedPassReport::default();
        if self.retention.holds_parent_roots()? {
            report.parent = true;
            return Ok(report);
        }
        let mut step0 = 0usize;
        let mut cutoff: Option<Option<u64>> = None;
        let mut after: Option<(Option<i64>, String)> = None;
        // (steps 2 and 3 end at the first candidate the rules do not allow;
        // the sections go on while step 0 has work)
        let mut detaching = true;
        for section in 0..SECTIONS {
            if section > 0 {
                std::thread::sleep(PAUSE);
            }
            let _guard = match self.retention.guard(true) {
                Ok(guard) => guard,
                Err(e) if super::super::import_owner::would_block(&e) => {
                    report.busy = true;
                    return Ok(report);
                }
                Err(e) => return Err(e),
            };
            self.retention.check_anchor()?;
            let mut c = self.retention.connect()?;
            // (the parent's roots are looked at again under the lock)
            if self.retention.holds_parent_roots()? {
                report.parent = true;
                return Ok(report);
            }
            if let Some(gate) = Gate::closed(&c)? {
                report.gate = Some(gate);
                return Ok(report);
            }
            report.sections += 1;
            let mut budget = BYTE_BUDGET;
            if step0 < STEP0_SECTIONS {
                match self.retention.leftovers(&mut c, &mut budget, &mut report)? {
                    Leftover::None => {}
                    Leftover::Progress => {
                        step0 += 1;
                        continue;
                    }
                    Leftover::Stuck => {
                        // (not again in this pass: the cursor moved on)
                        step0 = STEP0_SECTIONS;
                        continue;
                    }
                    Leftover::Stop => {
                        report.stuck = true;
                        return Ok(report);
                    }
                }
            }
            if !detaching {
                return Ok(report);
            }
            let now = match cutoff {
                Some(now) => now,
                None => {
                    let now = self.retention.received_clock(&c, inputs)?;
                    report.clock_held = now.is_none();
                    cutoff = Some(now);
                    now
                }
            };
            let detached = self.retention.detach_section(
                &mut c,
                &mut budget,
                inputs,
                now,
                &mut after,
                &mut report,
            )?;
            if detached.is_none() {
                detaching = false;
            }
        }
        report.again = true;
        Ok(report)
    }
}

impl Index {
    /// Step 0 alone with `budget`, for tests: what it returned and did.
    #[cfg(test)]
    pub(in crate::source::retention) fn leftovers_with(
        &self,
        budget: u64,
    ) -> Result<(&'static str, ReceivedPassReport)> {
        let mut c = self.connect()?;
        let mut budget = budget;
        let mut report = ReceivedPassReport::default();
        let what = match self.leftovers(&mut c, &mut budget, &mut report)? {
            Leftover::None => "none",
            Leftover::Progress => "progress",
            Leftover::Stuck => "stuck",
            Leftover::Stop => "stop",
        };
        Ok((what, report))
    }

    /// The trusted now of the expiry, `None` while the clock holds.
    fn received_clock(&self, c: &Connection, inputs: &ReceivedPassInputs) -> Result<Option<u64>> {
        let previous: Option<String> = c
            .query_row("SELECT value FROM meta WHERE key=?1", [CLOCK], |r| r.get(0))
            .optional()?;
        let previous = previous
            .as_deref()
            .map(serde_json::from_str::<crate::results::clock_policy::ClockState>)
            .transpose()?;
        let step = crate::results::clock_policy::step(
            previous.as_ref(),
            inputs.boot.as_deref(),
            inputs.wall,
            inputs.mono,
            inputs.max_age_ms,
        );
        if let Some(state) = step.state {
            c.execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![CLOCK, serde_json::to_string(&state)?],
            )?;
        }
        Ok(step.cutoff)
    }

    /// Step 0: claims a crash left, then the members of the abandoned roots
    /// after the cursor.
    fn leftovers(
        &self,
        c: &mut Connection,
        budget: &mut u64,
        report: &mut ReceivedPassReport,
    ) -> Result<Leftover> {
        let replay: Vec<String> = {
            let mut s = c.prepare("SELECT claim_json FROM source_delete_claim WHERE done=0 ORDER BY object,incarnation LIMIT 32")?;
            let rows = s.query_map([], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if !replay.is_empty() {
            for raw in replay {
                let claim: Claim = serde_json::from_str(&raw)?;
                // (one the rest of this section cannot verify waits for
                // the next one, whose budget is whole)
                if claim.leaf.size > *budget {
                    return Ok(Leftover::Progress);
                }
                if self.drain_claim(c, &claim, budget).is_err() {
                    return Ok(Leftover::Stop);
                }
                report.forgotten += 1;
            }
            return Ok(Leftover::Progress);
        }
        let stored: String = c
            .query_row("SELECT value FROM meta WHERE key=?1", [DROP_CURSOR], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or_default();
        let (root_after, object_after) = stored.split_once('\n').unwrap_or(("", ""));
        let page = |c: &Connection, root: &str, object: &str| -> Result<Vec<Member>> {
            let roots: Vec<String> = {
                let mut s = c.prepare(STEP0_ROOTS)?;
                let rows = s.query_map(params![root, ROWS as i64], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let mut members = Vec::new();
            let mut s = c.prepare(STEP0_MEMBERS)?;
            for id in roots {
                let left = ROWS - members.len();
                if left == 0 {
                    break;
                }
                let from = if id == root { object } else { "" };
                let rows = s.query_map(params![id, from, left as i64], |r| {
                    Ok((id.clone(), r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
                for row in rows {
                    members.push(row?);
                }
            }
            Ok(members)
        };
        let mut members = page(c, root_after, object_after)?;
        if members.is_empty() && !stored.is_empty() {
            members = page(c, "", "")?;
        }
        // Roots left empty (by a shared detach elsewhere, or a member a
        // crash took) go in the same section.
        // (their graph rows go in the same transaction)
        let empty = {
            const EMPTY: &str = "kind='abandoned' AND (id LIKE 'drop:%' OR id LIKE 'receive:%' OR id LIKE 'import:%') AND NOT EXISTS(SELECT 1 FROM member WHERE root=root.id)";
            let tx = c.transaction()?;
            // (an empty `import:` root takes its token's unused session with
            // it, as the per-root removal below does; step 0's page skips
            // empty roots, so they go here)
            tx.execute(
                &format!("DELETE FROM import_session WHERE state='pending' AND source IS NULL AND run_id IS NULL AND 'import:'||token IN (SELECT id FROM root WHERE {EMPTY})"),
                [],
            )?;
            tx.execute(
                &format!("DELETE FROM graph WHERE root IN (SELECT id FROM root WHERE {EMPTY})"),
                [],
            )?;
            let empty = tx.execute(&format!("DELETE FROM root WHERE {EMPTY}"), [])?;
            tx.commit()?;
            empty
        };
        report.forgotten += empty;
        if members.is_empty() {
            if !stored.is_empty() {
                c.execute("DELETE FROM meta WHERE key=?1", [DROP_CURSOR])?;
            }
            return Ok(if empty > 0 {
                Leftover::Progress
            } else {
                Leftover::None
            });
        }
        let mut changed = empty > 0;
        let mut cursor = stored.clone();
        let mut touched = std::collections::BTreeSet::new();
        // What the unproven files' reads took: once it reaches half of the
        // section's budget, the section ends (the hashing of files that
        // stay is bounded like that of files that go).
        let start = *budget;
        let mut unproven_read = 0u64;
        for (root, object, path, bytes) in &members {
            if unproven_read >= start / 2 {
                break;
            }
            // A known size the rest of the section cannot verify waits for
            // the next section (the cursor stays before it); one larger
            // than a whole section's half is kept (spec: the large-blob
            // rule is a later change).
            let half = BYTE_BUDGET / 2;
            match bytes.and_then(|b| u64::try_from(b).ok()) {
                Some(bytes) => {
                    if bytes <= half && bytes > *budget / 2 {
                        break;
                    }
                }
                // (an unknown size may be up to half a section: with less
                // than half of it left, it waits for the next section)
                None => {
                    if *budget < start / 2 {
                        break;
                    }
                }
            }
            cursor = format!("{root}\n{object}");
            touched.insert(root.clone());
            let shared: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1 AND root<>?2)",
                params![object, root],
                |r| r.get(0),
            )?;
            if !shared && self.destination_absent(object, path) {
                forget_member(c, root, object)?;
                report.forgotten += 1;
                changed = true;
                continue;
            }
            let before = *budget;
            match self.release_member(c, root, object, path, budget, true, false)? {
                Released::Shared(_) => {
                    report.forgotten += 1;
                    changed = true;
                }
                Released::Unproven(_) => {
                    report.kept += 1;
                    unproven_read += before - *budget;
                }
                Released::Claimed(Ok(())) => {
                    report.unlinked += 1;
                    changed = true;
                }
                Released::Claimed(Err(_)) => return Ok(Leftover::Stop),
            }
        }
        let tx = c.transaction()?;
        for root in &touched {
            let removed = tx.execute(
                "DELETE FROM root WHERE id=?1 AND kind='abandoned' AND NOT EXISTS(SELECT 1 FROM member WHERE root=?1)",
                [root],
            )?;
            if removed == 1 {
                tx.execute("DELETE FROM graph WHERE root=?1", [root])?;
                if let Some(token) = root.strip_prefix("import:") {
                    tx.execute(
                        "DELETE FROM import_session WHERE token=?1 AND state='pending' AND source IS NULL AND run_id IS NULL",
                        [token],
                    )?;
                }
                report.forgotten += 1;
                changed = true;
            }
        }
        if cursor != stored {
            tx.execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![DROP_CURSOR, cursor],
            )?;
        }
        tx.commit()?;
        Ok(if changed {
            Leftover::Progress
        } else {
            Leftover::Stuck
        })
    }

    /// Steps 2 and 3: up to [`CANDIDATES`] candidates, oldest first after
    /// `after`; the first one the rules do not allow ends the pass
    /// (`None`). `Some(n)`: the section checked its candidates and
    /// detached `n`.
    fn detach_section(
        &self,
        c: &mut Connection,
        budget: &mut u64,
        inputs: &ReceivedPassInputs,
        now: Option<u64>,
        after: &mut Option<(Option<i64>, String)>,
        report: &mut ReceivedPassReport,
    ) -> Result<Option<usize>> {
        // The cap counts only once every size is known (spec (g)13); what
        // the `drop:` roots alone hold is already on its way out.
        let filled: bool = c.query_row(
            // (and no size waits for a retry, and no recount runs: its
            // `store_bytes` is rewritten when it ends)
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='bytes_fill' AND value='done')
             AND NOT EXISTS(SELECT 1 FROM bytes_retry)
             AND NOT EXISTS(SELECT 1 FROM meta WHERE key IN ('bytes_recount','bytes_recount_wait'))",
            [],
            |r| r.get(0),
        )?;
        let total: Option<u64> = c
            .query_row("SELECT value FROM meta WHERE key='store_bytes'", [], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
            .and_then(|v| v.parse().ok());
        let mut over = match (filled, total) {
            (true, Some(total)) => {
                let leaving: i64 = c.query_row(
                    "SELECT coalesce(sum(b.bytes),0) FROM object_bytes b WHERE b.id IN (SELECT m.object FROM member m WHERE m.root>='drop:' AND m.root<'drop;')
                     AND NOT EXISTS(SELECT 1 FROM member o WHERE o.object=b.id AND NOT (o.root>='drop:' AND o.root<'drop;'))",
                    [],
                    |r| r.get(0),
                )?;
                Some(total.saturating_sub(u64::try_from(leaving).unwrap_or(0)))
            }
            _ => None,
        };
        // A requested source's time is known once the `clone_ref` fill
        // (which writes it for the roots made before) is done and no root
        // waits for its retry; until then nothing is detached.
        let timed: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='clone_ref_fill' AND value='done')
             AND NOT EXISTS(SELECT 1 FROM clone_ref_retry)",
            [],
            |r| r.get(0),
        )?;
        if !timed {
            return Ok(None);
        }
        // (L5) Nor before this process started its recount, which rewinds
        // both fills: an older binary may have left roots without a time
        // or a size since this store last counted (spec 25.8 5b (c)).
        if self.recounted.lock().unwrap().is_none() {
            return Ok(None);
        }
        let expired_before = now.map(|now| now.saturating_sub(inputs.max_age_ms));
        type Candidate = (String, String, Option<i64>);
        let fetch =
            |out: &mut Vec<Candidate>, sql: &str, p: &[&dyn rusqlite::ToSql]| -> Result<()> {
                let mut s = c.prepare(sql)?;
                let rows = s.query_map(p, |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
                for row in rows {
                    out.push(row?);
                }
                Ok(())
            };
        let mut candidates: Vec<Candidate> = Vec::new();
        let (used_after, id_after) = match after.clone() {
            None => (None, String::new()),
            Some((used, id)) => (used, id),
        };
        let mut timed_from = (i64::MIN, String::new());
        if used_after.is_none() {
            fetch(&mut candidates, UNTIMED, &[&id_after, &(CANDIDATES as i64)])?;
        } else {
            timed_from = (used_after.unwrap_or(i64::MIN), id_after);
        }
        let room = CANDIDATES - candidates.len();
        if room > 0 {
            fetch(
                &mut candidates,
                TIMED,
                &[&timed_from.0, &timed_from.1, &(room as i64)],
            )?;
        }
        if candidates.is_empty() {
            return Ok(None);
        }
        let mut detached = 0;
        for (root, source, used_ms) in &candidates {
            *after = Some((*used_ms, root.clone()));
            let used = &used_ms.unwrap_or(-1);
            let expired = expired_before.is_some_and(|before| *used < before as i64);
            let capped = over.is_some_and(|bytes| bytes > inputs.cap_bytes);
            if !expired && !capped {
                // (oldest first: no later one is allowed either)
                return Ok(None);
            }
            // (a) (1) and the pins, read under the lock; an error, or an id
            // that does not convert, keeps it
            let id = crate::source::classify::source_id_of_ci(source);
            match (inputs.uses)(source, id.as_ref()) {
                _ if id.is_none() => continue,
                Ok(SourceUse::Live) | Err(_) => continue,
                Ok(SourceUse::Ended(Some(at))) if at > *used => {
                    // L6: used at its attempt's terminal time; looked at
                    // again in its new place in the order.
                    c.execute(
                        "UPDATE root SET used_ms=MAX(ifnull(used_ms,0),?2) WHERE id=?1 AND kind='received'",
                        params![root, at],
                    )?;
                    continue;
                }
                Ok(SourceUse::Ended(_)) => {}
            }
            let Some(freed) = self.detach(c, root, source)? else {
                continue;
            };
            detached += 1;
            report.detached += 1;
            if let Some(bytes) = over.as_mut() {
                *bytes = bytes.saturating_sub(freed);
            }
            // The manifest goes in this section (step 3): the source is not
            // in the store from here on.
            let manifest = format!("m:{source}");
            let held: Option<(String, String)> = c
                .query_row(
                    "SELECT m.root,o.path FROM member m JOIN object o ON o.id=m.object WHERE m.object=?1 AND m.root LIKE 'drop:'||?2||':%'",
                    params![manifest, source],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((drop, path)) = held {
                let shared: bool = c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1 AND root<>?2)",
                    params![manifest, drop],
                    |r| r.get(0),
                )?;
                if !shared && self.destination_absent(&manifest, &path) {
                    forget_member(c, &drop, &manifest)?;
                } else {
                    match self.release_member(c, &drop, &manifest, &path, budget, true, false)? {
                        Released::Claimed(Ok(())) => report.unlinked += 1,
                        Released::Claimed(Err(_)) => {
                            report.stuck = true;
                            return Ok(None);
                        }
                        Released::Unproven(_) => report.kept += 1,
                        Released::Shared(_) => {}
                    }
                }
            }
        }
        Ok(Some(detached))
    }

    /// Step 3's transaction: `receive:<source>` is checked again (still
    /// received, not named by another root's graph), its members move to a
    /// new `drop:<source>:<ulid>` root, and its row, graph rows and
    /// `clone_ref` rows go; a `(clone, source)` with no `clone_ref` row left
    /// gets a `ref_drop:` row (step 4). Returns the bytes only this root
    /// held, alone or with `drop:` roots (whose bytes the cap already counts as
    /// leaving), or `None` when it no longer qualifies.
    fn detach(&self, c: &mut Connection, root: &str, source: &str) -> Result<Option<u64>> {
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let still: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1 AND kind='received' AND source=?2)
             AND NOT EXISTS(SELECT 1 FROM graph WHERE source=?2 AND root<>?1)",
            params![root, source],
            |r| r.get(0),
        )?;
        if !still {
            return Ok(None);
        }
        let freed: i64 = tx.query_row(
            "SELECT coalesce(sum(b.bytes),0) FROM member m JOIN object_bytes b ON b.id=m.object
             WHERE m.root=?1 AND NOT EXISTS(SELECT 1 FROM member o WHERE o.object=m.object AND o.root<>?1
             AND NOT (o.root>='drop:' AND o.root<'drop;'))",
            [root],
            |r| r.get(0),
        )?;
        let drop = format!("drop:{source}:{}", ulid::Ulid::new());
        tx.execute(
            "INSERT INTO root(id,kind,source) VALUES(?1,'abandoned',NULL)",
            [&drop],
        )?;
        tx.execute(
            "UPDATE member SET root=?2 WHERE root=?1",
            params![root, drop],
        )?;
        // (every source the root's clones named: its own and those of the
        // children it carried)
        let refs: Vec<(String, String)> = {
            let mut s = tx.prepare("SELECT DISTINCT clone,source FROM clone_ref WHERE root=?1")?;
            let rows = s.query_map([root], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        tx.execute("DELETE FROM clone_ref WHERE root=?1", [root])?;
        for (clone, named) in refs {
            tx.execute(
                "INSERT OR IGNORE INTO meta(key,value) SELECT 'ref_drop:'||?1||':'||?2,'' WHERE NOT EXISTS(SELECT 1 FROM clone_ref WHERE clone=?1 AND source=?2)",
                params![clone, named],
            )?;
        }
        tx.execute("DELETE FROM graph WHERE root=?1", [root])?;
        tx.execute("DELETE FROM root WHERE id=?1", [root])?;
        tx.commit()?;
        Ok(Some(u64::try_from(freed).unwrap_or(0)))
    }
}
