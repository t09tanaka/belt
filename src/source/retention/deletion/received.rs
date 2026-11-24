//! The deleting pass of a worker's received sources (spec 25.8 5b (d)).
//!
//! A pass is a run of short sections, each under the exclusive mutation
//! lock taken without waiting. Step 0 finishes what is left (claims a crash
//! left, the members of abandoned `import:`, `drop:` and `receive:` roots);
//! step 1 is the gate; steps 2 and 3 pick the oldest unreferenced received
//! source the rules of (c) allow and detach it to a `drop:` root, whose
//! members the next sections release; step 4's `ref_drop:` rows record the
//! clone refs the detach orphaned, removed under the clone's import lock
//! taken without waiting. Step 0 also claims the files only an abandoned
//! import token holds (the agent tick takes no exclusive lock), and a blob
//! larger than half a section is claimed under the lock and hashed with it
//! let go.
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
    /// Asked at each section under the exclusive lock (and so after the
    /// agent's lock may have gone): when it is true the pass returns
    /// without detaching anything (the agent is stopping, or a version is
    /// on trial, spec 25.1 8).
    pub stop: &'a dyn Fn() -> bool,
    /// Step 4 (spec (e)), called with no source lock held: under the import
    /// lock of the clone `clone` taken without waiting, unless `row.held()`
    /// remove the refs of `source` (CI id), then call `row.done()`.
    /// `Ok(false)` when that lock is busy (the row waits for its next try).
    pub refs: &'a dyn Fn(&str, &str, &RefRow<'_>) -> Result<bool>,
    /// The gc of (e), called with no source lock held for one clone whose
    /// `gc:<clone>` row is due (at most one a pass): run
    /// `git gc --prune=14.days.ago` there under the clone's import lock
    /// tried without waiting, bounded, ended when the given stop says so.
    /// `Busy` waits [`GC_BUSY_MS`] (other clones go first meanwhile),
    /// `Stopped` the next pass; `Ran`, `Refused` and an error (a failure
    /// or the bound) wait a day, the last two counted as failures.
    pub gc: &'a GcFn<'a>,
}

/// The gc of (e) for one clone, given the pass's stop.
pub type GcFn<'a> = dyn Fn(&str, &dyn Fn() -> bool) -> Result<CloneGc> + 'a;

/// What the gc of (e) did in one clone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneGc {
    /// It ran (or there is no clone).
    Ran,
    /// The clone's import lock was busy.
    Busy,
    /// The stop ended it.
    Stopped,
    /// The clone has `extensions.preciousObjects` or a
    /// `gc.recentObjectsHook`: nothing is collected.
    Refused,
}

/// A gc of (e) that failed (or was not run: `extensions.preciousObjects`),
/// for the agent's event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcFailure {
    pub clone: String,
    pub message: String,
    /// Failures in a row, up to [`GC_FAILS_MAX`].
    pub failures: i64,
}

/// One step 4 row, for the caller's `refs`, which calls both under the
/// clone's import lock.
pub struct RefRow<'a> {
    /// Whether the source has a `clone_ref` row again (received again): its
    /// refs then stay.
    pub held: &'a dyn Fn() -> Result<bool>,
    /// Removes the row (and its next try), in a short transaction.
    pub done: &'a dyn Fn() -> Result<()>,
    /// The pass's `stop`: git still running is ended and the row stays.
    pub stop: &'a dyn Fn() -> bool,
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
    /// `ref_drop:` rows done (step 4): the refs went, or the source was
    /// received again and they stay.
    pub refs: usize,
    /// Clones whose gc of (e) ran (or failed: it waits a day all the same).
    pub gc: usize,
    /// The gc of (e) of this pass failed (review round 1, L6).
    pub gc_failed: Option<GcFailure>,
    /// The clone whose gc of (e) ran in this pass and succeeded: a later
    /// failure of it is news again.
    pub gc_ran: Option<String>,
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
/// How long a pass waits for the exclusive lock again after a large hash
/// (and then for the shared one, to withdraw the claim).
const RETAKE: std::time::Duration = std::time::Duration::from_secs(2);
/// The pause between sections, so a waiting reader takes the lock first.
const PAUSE: std::time::Duration = std::time::Duration::from_millis(100);

/// Step 0's page: the abandoned roots from the cursor in `root_kind`
/// order, then each one's members by the primary key (no scan of the other
/// roots' members, no sort).
/// A root whose next try is not due (`drop_retry`, written when one of its
/// members could not be proven) is passed over, except the one the cursor
/// is in, so a sweep through it ends; a next try more than an hour ahead
/// (the clock went back) is due.
const STEP0_ROOTS: &str = "SELECT id FROM root WHERE kind='abandoned' AND id>=?1
     AND (id LIKE 'import:%' OR id LIKE 'drop:%' OR id LIKE 'receive:%')
     AND EXISTS(SELECT 1 FROM member WHERE root=root.id)
     AND (id=?1 OR NOT EXISTS(SELECT 1 FROM drop_retry t WHERE t.root=root.id AND t.next_ms>?3 AND t.next_ms<=?3+3600000))
     ORDER BY id LIMIT ?2";
/// How long a root with an unproven member waits before step 0 reads it
/// again (spec 25.8 5b, 「rootごとに次に試す時刻」).
const RETRY_MS: i64 = 3_600_000;
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
/// Abandoned roots left without a member (by a shared detach elsewhere, or
/// a member a crash took): at most [`EMPTY_LIMIT`] go in a section, so a
/// store with many of them is not scanned and rewritten in one lock hold.
const EMPTY_ROOTS: &str = "SELECT id FROM root WHERE kind='abandoned'
     AND (id LIKE 'drop:%' OR id LIKE 'receive:%' OR id LIKE 'import:%')
     AND NOT EXISTS(SELECT 1 FROM member WHERE root=root.id) LIMIT ?1";
const EMPTY_LIMIT: i64 = 64;
/// Step 4's rows whose next try is due (one more than an hour ahead is due:
/// the clock went back).
const REF_ROWS: &str = "SELECT m.key FROM meta m WHERE m.key>='ref_drop:' AND m.key<'ref_drop;'
     AND NOT EXISTS(SELECT 1 FROM ref_retry t WHERE t.key=m.key AND t.next_ms>?1 AND t.next_ms<=?1+3600000)
     ORDER BY m.key LIMIT ?2";
const REF_HELD: &str = "SELECT 1 FROM clone_ref WHERE clone=?1 AND source=?2 LIMIT 1";
/// How long a step 4 row whose import lock was busy waits.
const REF_BUSY_MS: i64 = 60_000;
/// The rows of (e)'s gc: `gc:<clone>`, valued `<last gc ms>\n<deadline
/// ms>`, followed by `\n<failures in a row>\n<next try ms>` when either is
/// not 0 (one per clone; a clone whose refs never went has none).
const GC_ROWS: &str = "SELECT key,value FROM meta WHERE key>='gc:' AND key<'gc;' ORDER BY key";
/// At most one gc a day per clone.
const GC_EVERY_MS: i64 = 86_400_000;
/// A clone's gc rows live this long after its last ref removal (15 days:
/// Git's 14 days of grace and one daily gc after them).
const GC_KEEP_MS: i64 = 15 * 86_400_000;
/// How long a clone whose import lock was busy waits (review round 1, L5:
/// the other clones get their turn meanwhile).
const GC_BUSY_MS: i64 = 600_000;
/// The bound of a row's failure count.
pub const GC_FAILS_MAX: i64 = 1000;
/// From this many failures in a row `doctor` shows the clone.
pub const GC_FAILS_SHOWN: i64 = 3;

/// A `gc:` row's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GcRow {
    last: i64,
    deadline: i64,
    fails: i64,
    next: i64,
}

fn gc_value(value: &str) -> Option<GcRow> {
    let fields: Vec<i64> = value
        .split('\n')
        .map(|f| f.parse().ok())
        .collect::<Option<_>>()?;
    match fields[..] {
        [last, deadline] => Some(GcRow {
            last,
            deadline,
            fails: 0,
            next: 0,
        }),
        [last, deadline, fails, next] if (0..=GC_FAILS_MAX).contains(&fails) => Some(GcRow {
            last,
            deadline,
            fails,
            next,
        }),
        _ => None,
    }
}

/// A failed gc at `now` (review round 1, L6): it waits a day as after a
/// run, and is counted.
fn gc_failed_at(r: &mut GcRow, now: i64) {
    r.last = now;
    r.fails = (r.fails + 1).min(GC_FAILS_MAX);
    r.next = 0;
}

fn gc_text(r: GcRow) -> String {
    if r.fails == 0 && r.next == 0 {
        format!("{}\n{}", r.last, r.deadline)
    } else {
        format!("{}\n{}\n{}\n{}", r.last, r.deadline, r.fails, r.next)
    }
}

/// The clones whose gc failed [`GC_FAILS_SHOWN`] times in a row or more
/// (`doctor`), in the open index `c`.
pub(crate) fn gc_failing(c: &Connection) -> Result<u64> {
    let mut s = c.prepare(GC_ROWS)?;
    let rows = s.query_map([], |r| r.get::<_, String>(1))?;
    let mut n = 0;
    for value in rows {
        if gc_value(&value?).is_some_and(|r| r.fails >= GC_FAILS_SHOWN) {
            n += 1;
        }
    }
    Ok(n)
}

/// A ref removal in `clone` at `now`: its `gc:` row keeps its last gc (none
/// yet: 0), its failures and next try, and gets the deadline `now` + 15
/// days. In the caller's transaction.
fn gc_mark(c: &Connection, clone: &str, now: u64) -> Result<()> {
    let key = format!("gc:{clone}");
    let old = c
        .query_row("SELECT value FROM meta WHERE key=?1", [&key], |r| {
            r.get::<_, String>(0)
        })
        .optional()?
        .as_deref()
        .and_then(gc_value);
    let row = GcRow {
        deadline: now as i64 + GC_KEEP_MS,
        ..old.unwrap_or(GcRow {
            last: 0,
            deadline: 0,
            fails: 0,
            next: 0,
        })
    };
    c.execute(
        "INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![key, gc_text(row)],
    )?;
    Ok(())
}
/// The queries whose plans a test checks.
#[cfg(test)]
pub(in crate::source::retention) const PLANNED: [&str; 7] = [
    STEP0_ROOTS,
    STEP0_MEMBERS,
    UNTIMED,
    TIMED,
    EMPTY_ROOTS,
    REF_ROWS,
    REF_HELD,
];

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
    /// A large file was claimed (or a claim too large to hash in the
    /// section is left): the caller hashes it with the lock let go.
    Large(Box<Claim>),
}

impl SourceStore {
    /// One pass (spec 25.8 5b (d)). The caller has checked that no version
    /// is on trial; a store that holds a parent's root is left alone.
    pub fn received_pass(&self, inputs: &ReceivedPassInputs) -> Result<ReceivedPassReport> {
        let mut report = self.received_sections(inputs)?;
        // The gc of (e), after the sections, with no source lock held (a
        // store that holds a parent's roots is left alone, and a stopped
        // pass does not start one).
        // (the stop is not asked here: the gc's git asks it at once)
        if !report.parent {
            self.retention.gc_clones(inputs, &mut report)?;
        }
        Ok(report)
    }

    /// Whether `source`'s refs in `clone` are still named (the removal of
    /// a discarded work directory, (g)7 and (g)29): a `clone_ref` row, or
    /// rows not known yet (the fill is not `done`, or a root waits for its
    /// retry: kept, as step 4 keeps its rows then). Its caller holds the
    /// clone's import lock, which every writer of `clone_ref` holds.
    pub fn clone_ref_held(&self, clone: &str, source: &str) -> Result<bool> {
        let c = self.retention.connect()?;
        Ok(c.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM meta WHERE key='clone_ref_fill' AND value='done')
             OR EXISTS(SELECT 1 FROM clone_ref_retry)
             OR EXISTS(SELECT 1 FROM clone_ref WHERE clone=?1 AND source=?2)",
            params![clone, source],
            |r| r.get(0),
        )?)
    }

    /// A discarded work directory of `source` (CI id) whose bundle went
    /// into `clone` (review round 1, L2, L9): unless [`Self::clone_ref_held`]
    /// says its refs are still named, a `ref_drop:<clone>:<source>` row,
    /// which step 4 removes the refs for (with its own checks, under the
    /// import lock) and which writes the clone's `gc:` row. No git runs
    /// here. Whether a row is there now.
    pub fn queue_ref_drop(&self, clone: &str, source: &str) -> Result<bool> {
        let c = self.retention.connect()?;
        c.execute(
            "INSERT OR IGNORE INTO meta(key,value) SELECT 'ref_drop:'||?1||':'||?2,''
             WHERE EXISTS(SELECT 1 FROM meta WHERE key='clone_ref_fill' AND value='done')
             AND NOT EXISTS(SELECT 1 FROM clone_ref_retry)
             AND NOT EXISTS(SELECT 1 FROM clone_ref WHERE clone=?1 AND source=?2)",
            params![clone, source],
        )?;
        Ok(c.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='ref_drop:'||?1||':'||?2)",
            params![clone, source],
            |r| r.get(0),
        )?)
    }

    /// Refs went from `clone` outside step 4 (the refs a refused or
    /// discarded bundle made): its `gc:` row, as step 4 writes it.
    pub fn note_ref_removal(&self, clone: &str) -> Result<()> {
        let c = self.retention.connect()?;
        c.execute_batch("BEGIN IMMEDIATE")?;
        match gc_mark(&c, clone, crate::platform::clock::wall_ms()) {
            Ok(()) => c.execute_batch("COMMIT")?,
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                return Err(e);
            }
        }
        Ok(())
    }

    fn received_sections(&self, inputs: &ReceivedPassInputs) -> Result<ReceivedPassReport> {
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
        let mut large = true;
        for section in 0..SECTIONS {
            if section > 0 {
                std::thread::sleep(PAUSE);
            }
            let guard = match self.retention.guard(true) {
                Ok(guard) => guard,
                Err(e) if super::super::import_owner::would_block(&e) => {
                    report.busy = true;
                    return Ok(report);
                }
                Err(e) => return Err(e),
            };
            if (inputs.stop)() {
                return Ok(report);
            }
            self.retention.check_anchor()?;
            let mut c = self.retention.connect()?;
            // (the parent's roots are looked at again under the lock)
            if self.retention.holds_parent_roots()? {
                report.parent = true;
                return Ok(report);
            }
            // (a claim whose file is gone completes whatever the gate)
            report.unlinked += self.retention.complete_gone_claims(&mut c)?;
            if let Some(gate) = Gate::closed(&c)? {
                report.gate = Some(gate);
                return Ok(report);
            }
            report.sections += 1;
            let mut budget = section_budget();
            if step0 < STEP0_SECTIONS {
                match self.retention.leftovers(
                    &mut c,
                    &mut budget,
                    inputs.wall,
                    large,
                    &mut report,
                )? {
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
                    Leftover::Large(claim) => {
                        // One per pass ((g)14), hashed with the lock let go.
                        large = false;
                        step0 += 1;
                        drop(guard);
                        let digest = self.retention.hash_outside(&claim, inputs.stop);
                        // The lock is waited for, briefly; without it the
                        // claim is withdrawn under the shared lock (which
                        // no exclusive holder, so no drain, shares), and
                        // only when neither comes does it stay for the
                        // next pass ((M1): a claim fences the store's SDK).
                        let Some(_guard) = self.retention.guard_exclusive_waiting(RETAKE)? else {
                            if let Ok(_shared) = self.retention.guard_waiting(RETAKE) {
                                self.retention.withdraw_large(&claim, inputs.wall)?;
                            }
                            report.busy = true;
                            return Ok(report);
                        };
                        let mut c = self.retention.connect()?;
                        let gate = Gate::closed(&c)?;
                        if (inputs.stop)() || gate.is_some() {
                            report.gate = gate;
                            self.retention.withdraw_large(&claim, inputs.wall)?;
                            return Ok(report);
                        }
                        match self
                            .retention
                            .finish_large(&mut c, &claim, digest, inputs.wall)?
                        {
                            Some(true) => report.unlinked += 1,
                            Some(false) => report.kept += 1,
                            None => {
                                report.stuck = true;
                                return Ok(report);
                            }
                        }
                        continue;
                    }
                }
            }
            let ran = detaching;
            if detaching {
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
            // Step 4: the rows (earlier detaches' and this section's) are
            // read under the lock; their git runs after it is let go.
            let rows = self.retention.ref_rows(&c, inputs.wall)?;
            drop(c);
            drop(guard);
            self.retention.drop_refs(rows, inputs, &mut report)?;
            if !ran {
                return Ok(report);
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
        let what = match self.leftovers(&mut c, &mut budget, now_ms(), true, &mut report)? {
            Leftover::None => "none",
            Leftover::Progress => "progress",
            Leftover::Stuck => "stuck",
            Leftover::Stop => "stop",
            Leftover::Large(_) => "large",
        };
        Ok((what, report))
    }

    /// The end of a large claim, under the exclusive lock taken again:
    /// `digest` is what [`Index::hash_outside`] read. The content its name
    /// says (or an absent file) is unlinked (`Some(true)`); anything else
    /// withdraws the claim and the file stays with its root, whose next try
    /// waits (`Some(false)`). A claim another pass finished meanwhile is
    /// done (`Some(false)`). `None`: a claim that could neither drain nor be
    /// withdrawn (an older shape): the pass stops.
    fn finish_large(
        &self,
        c: &mut Connection,
        claim: &Claim,
        digest: Result<Option<String>>,
        now: u64,
    ) -> Result<Option<bool>> {
        let still: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_delete_claim WHERE object=?1 AND incarnation=?2 AND done=0)",
            params![claim.object, claim.incarnation],
            |r| r.get(0),
        )?;
        if !still {
            return Ok(Some(false));
        }
        let expected = match &claim.root {
            Some(_) => claim.object.strip_prefix("b:").unwrap_or(""),
            None => claim.digest.as_str(),
        };
        let content = match &digest {
            Ok(None) => true,
            Ok(Some(d)) => !expected.is_empty() && d == expected,
            Err(_) => false,
        };
        if content {
            match self.drain(c, claim, None) {
                Ok(()) => return Ok(Some(true)),
                // (protected again, its root no longer abandoned, or the
                // file changed: nothing was unlinked, so it is withdrawn)
                Err(e) if e.downcast_ref::<super::Untouched>().is_some() => {}
                Err(_) => return Ok(None),
            }
        }
        if claim.root.is_none() {
            // (an older claim cannot be withdrawn; `doctor` shows it)
            return Ok(None);
        }
        // (a claim whose file is gone stays for a later drain)
        self.withdraw_large(claim, now)?;
        Ok(Some(false))
    }

    /// The large claim `claim` is withdrawn, under a lock that keeps every
    /// drain out (the exclusive one, or the shared one): its file and
    /// membership stay, and its root waits for its next try. An older claim
    /// (no root) stays, and so does a claim whose file is not seen there
    /// right now (review round 2, M-a): a crash or a failed drain after the
    /// unlink left it, and only its drain may end it (withdrawn, the object
    /// would be live with no file). `true`: withdrawn.
    fn withdraw_large(&self, claim: &Claim, now: u64) -> Result<bool> {
        let Some(root) = &claim.root else {
            return Ok(false);
        };
        if !self.large_leaf_present(claim) {
            return Ok(false);
        }
        let mut c = self.connect()?;
        self.unclaim(&mut c, claim)?;
        retry_root(&c, root, now)?;
        Ok(true)
    }

    /// Step 4 (spec (e)), under the exclusive lock: up to [`CANDIDATES`]
    /// `ref_drop:<clone>:<source>` rows whose next try is due, once the
    /// `clone_ref` fill (which writes rows without the import lock) is done
    /// and no root waits for its retry; their next tries without a row go.
    fn ref_rows(&self, c: &Connection, now: u64) -> Result<Vec<String>> {
        c.execute(
            "DELETE FROM ref_retry WHERE NOT EXISTS(SELECT 1 FROM meta WHERE key=ref_retry.key)",
            [],
        )?;
        let filled: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key='clone_ref_fill' AND value='done')
             AND NOT EXISTS(SELECT 1 FROM clone_ref_retry)",
            [],
            |r| r.get(0),
        )?;
        if !filled {
            return Ok(Vec::new());
        }
        let mut s = c.prepare(REF_ROWS)?;
        let rows = s.query_map(params![now as i64, CANDIDATES as i64], |r| r.get(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Step 4 with the exclusive lock let go: each row under its clone's
    /// import lock taken without waiting (the caller's `refs`), which also
    /// covers the check of `clone_ref` (a source received again keeps its
    /// refs) and the row's removal. A busy lock or a failure keeps the row
    /// and gives it a next try ([`REF_BUSY_MS`], [`RETRY_MS`]), so the rows
    /// after it are not starved.
    ///
    /// Review round 3 (M-1): every writer of a clone's `clone_ref` rows
    /// holds that clone's import lock (a file lock, so across processes):
    /// a publication (the unpack's or its own, waited for with a bound),
    /// the fill (tried, a busy clone sends its root to `clone_ref_retry`).
    /// Under it, the check of `clone_ref`, the git and the row's removal
    /// here are one step. Lock order (review round 4, M-B; each lock taken
    /// after another is tried or waited for with a bound, and a holder of
    /// the exclusive lock waits for nothing, so no cycle waits): a
    /// publication holds its source's lock, then waits for the shared
    /// source lock, then waits with a bound for the import lock; the fill
    /// takes the shared source lock, then tries the import lock; a drain
    /// holds the exclusive source lock and no import lock; step 4 holds no
    /// source lock and tries the import lock (its `held` and `done` only
    /// read and write the index).
    ///
    /// Gits in a clone and their locks: under the import lock run step 4's
    /// `update-ref -d`, the bundle unpack of a publication and the removal
    /// of the refs a refused or discarded bundle made, the only writers of the
    /// `refs/local-ci/{snapshots,index}/<source>` refs; under the clone's
    /// own lock run `hold_clone_at`'s `checkout -f` and `settle`'s
    /// `branch -D` and `worktree prune`; fast-worktree's add and remove
    /// take neither. So after a killed git only the `.lock` of those
    /// source refs is removed, never `packed-refs.lock` or `index.lock`.
    fn drop_refs(
        &self,
        rows: Vec<String>,
        inputs: &ReceivedPassInputs,
        report: &mut ReceivedPassReport,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let c = self.connect()?;
        for key in rows {
            if (inputs.stop)() {
                break;
            }
            let Some((clone, source)) = key["ref_drop:".len()..].split_once(':') else {
                continue;
            };
            let held = || -> Result<bool> {
                Ok(
                    c.query_row(REF_HELD, params![clone, source], |r| r.get::<_, i64>(0))
                        .optional()?
                        .is_some(),
                )
            };
            let done = || -> Result<()> {
                c.execute_batch("BEGIN IMMEDIATE")?;
                let removed = (|| -> Result<()> {
                    c.execute("DELETE FROM meta WHERE key=?1", [&key])?;
                    c.execute("DELETE FROM ref_retry WHERE key=?1", [&key])?;
                    // (e): refs went from the clone (not received again):
                    // its gc is due for the next 15 days
                    let held = c
                        .query_row(REF_HELD, params![clone, source], |r| r.get::<_, i64>(0))
                        .optional()?
                        .is_some();
                    if !held {
                        gc_mark(&c, clone, inputs.wall)?;
                    }
                    Ok(())
                })();
                match removed {
                    Ok(()) => c.execute_batch("COMMIT")?,
                    Err(e) => {
                        let _ = c.execute_batch("ROLLBACK");
                        return Err(e);
                    }
                }
                Ok(())
            };
            let row = RefRow {
                held: &held,
                done: &done,
                stop: inputs.stop,
            };
            let wait = match (inputs.refs)(clone, source, &row) {
                Ok(true) => {
                    report.refs += 1;
                    continue;
                }
                Ok(false) => REF_BUSY_MS,
                Err(_) => RETRY_MS,
            };
            c.execute(
                "INSERT INTO ref_retry(key,next_ms) SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM meta WHERE key=?1)
                 ON CONFLICT(key) DO UPDATE SET next_ms=excluded.next_ms",
                params![key, inputs.wall as i64 + wait],
            )?;
        }
        Ok(())
    }

    /// The gc of (e): the `gc:<clone>` rows whose deadline has passed go
    /// (as do rows not in this form); a deadline more than 15 days ahead
    /// (the clock went back) is brought to 15 days from now (review round
    /// 1, L1). Then the clone whose last gc is the oldest of those a day
    /// old or more (one with a last gc more than a day ahead counts: the
    /// clock went back) and whose next try is due is given to `inputs.gc`.
    /// A run writes the time of this pass as its last gc; a failure too,
    /// counted; a busy lock writes a next try [`GC_BUSY_MS`] on; a stop
    /// writes nothing (the next pass tries again).
    fn gc_clones(
        &self,
        inputs: &ReceivedPassInputs,
        report: &mut ReceivedPassReport,
    ) -> Result<()> {
        let now = inputs.wall as i64;
        let c = self.connect()?;
        let rows: Vec<(String, String)> = {
            let mut s = c.prepare(GC_ROWS)?;
            let rows = s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let mut due: Option<(i64, String)> = None;
        for (key, value) in rows {
            let Some(mut row) = gc_value(&value).filter(|r| now < r.deadline) else {
                // (past its deadline, or garbage: the row goes, nothing is
                // kept for it but Git's objects, which stay)
                c.execute(
                    "DELETE FROM meta WHERE key=?1 AND value=?2",
                    params![key, value],
                )?;
                continue;
            };
            if row.deadline > now + GC_KEEP_MS {
                row.deadline = now + GC_KEEP_MS;
                c.execute(
                    "UPDATE meta SET value=?3 WHERE key=?1 AND value=?2",
                    params![key, value, gc_text(row)],
                )?;
            }
            let last = row.last;
            let ready = last <= 0 || last + GC_EVERY_MS <= now || last > now + GC_EVERY_MS;
            let next = row.next <= now || row.next > now + GC_BUSY_MS;
            if ready && next && due.as_ref().is_none_or(|(l, _)| last < *l) {
                due = Some((last, key));
            }
        }
        let Some((_, key)) = due else {
            return Ok(());
        };
        let clone = &key["gc:".len()..];
        // (what the row becomes, whether the gc counts as run, its failure)
        type Change = fn(&mut GcRow, i64);
        let (change, ran, failure): (Change, bool, Option<String>) =
            match (inputs.gc)(clone, inputs.stop) {
                Ok(CloneGc::Stopped) => return Ok(()),
                Ok(CloneGc::Busy) => (|r, now| r.next = now + GC_BUSY_MS, false, None),
                Ok(CloneGc::Ran) => (
                    |r, now| {
                        r.last = now;
                        r.fails = 0;
                        r.next = 0;
                    },
                    true,
                    None,
                ),
                Ok(CloneGc::Refused) => (
                    gc_failed_at,
                    true,
                    Some("extensions.preciousObjects or gc.recentObjectsHook is set in the clone: nothing is collected".into()),
                ),
                Err(e) => (gc_failed_at, true, Some(format!("{e:#}"))),
            };
        c.execute_batch("BEGIN IMMEDIATE")?;
        let written = (|| -> Result<Option<GcRow>> {
            let value: Option<String> = c
                .query_row("SELECT value FROM meta WHERE key=?1", [&key], |r| r.get(0))
                .optional()?;
            let Some(mut row) = value.as_deref().and_then(gc_value) else {
                return Ok(None);
            };
            change(&mut row, now);
            c.execute(
                "UPDATE meta SET value=?2 WHERE key=?1",
                params![key, gc_text(row)],
            )?;
            Ok(Some(row))
        })();
        let row = match written {
            Ok(row) => {
                c.execute_batch("COMMIT")?;
                row
            }
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                return Err(e);
            }
        };
        if ran {
            report.gc += 1;
        }
        if ran && failure.is_none() {
            report.gc_ran = Some(clone.to_string());
        }
        if let Some(message) = failure {
            report.gc_failed = Some(GcFailure {
                clone: clone.to_string(),
                message,
                failures: row.map_or(1, |r| r.fails),
            });
        }
        Ok(())
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
        now: u64,
        large: bool,
        report: &mut ReceivedPassReport,
    ) -> Result<Leftover> {
        // (next tries of roots that are gone go with them)
        c.execute(
            "DELETE FROM drop_retry WHERE NOT EXISTS(SELECT 1 FROM root WHERE id=drop_retry.root)",
            [],
        )?;
        let replay: Vec<String> = {
            let mut s = c.prepare("SELECT claim_json FROM source_delete_claim WHERE done=0 ORDER BY object,incarnation LIMIT 32")?;
            let rows = s.query_map([], |r| r.get(0))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        if !replay.is_empty() {
            for raw in replay {
                let claim: Claim = serde_json::from_str(&raw)?;
                // A large claim, or one the section cannot hash (it starts
                // here with its whole budget), is hashed with the lock let
                // go: one per pass; after it, step 0 waits for the next.
                if claim.root.is_some() || claim.leaf.size > *budget {
                    return Ok(if large {
                        Leftover::Large(Box::new(claim))
                    } else {
                        Leftover::Stuck
                    });
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
                let rows = s.query_map(params![root, ROWS as i64, now as i64], |r| r.get(0))?;
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
            let tx = c.transaction()?;
            let ids: Vec<String> = {
                let mut s = tx.prepare(EMPTY_ROOTS)?;
                let rows = s.query_map([EMPTY_LIMIT], |r| r.get(0))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let mut removed = 0;
            for id in &ids {
                // (an empty `import:` root takes its token's unused
                // session with it, as the per-root removal below does;
                // step 0's page skips empty roots, so they go here)
                if let Some(token) = id.strip_prefix("import:") {
                    tx.execute(
                        "DELETE FROM import_session WHERE token=?1 AND state='pending' AND source IS NULL AND run_id IS NULL",
                        [token],
                    )?;
                }
                tx.execute("DELETE FROM graph WHERE root=?1", [id])?;
                tx.execute("DELETE FROM drop_retry WHERE root=?1", [id])?;
                removed += tx.execute("DELETE FROM root WHERE id=?1", [id])?;
            }
            tx.commit()?;
            removed
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
        let mut claimed = None;
        let mut retry = std::collections::BTreeSet::new();
        for (root, object, path, bytes) in &members {
            if unproven_read >= start / 2 {
                break;
            }
            let shared: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM member WHERE object=?1 AND root<>?2)",
                params![object, root],
                |r| r.get(0),
            )?;
            // A size the in-lock proof (half a section) cannot read is a
            // large file: a blob no other root holds is claimed here and
            // hashed with the lock let go, one per pass (spec (d) 「1回の
            // 上限」); a manifest that large is kept. A known size the rest
            // of the section cannot verify waits for the next section (the
            // cursor stays before it).
            let half = start / 2;
            let size = match bytes.and_then(|b| u64::try_from(b).ok()) {
                Some(size) => Some(size),
                None if !shared => self.leaf_size(object, path),
                None => None,
            };
            match size {
                Some(size) if size > half && !shared && object.starts_with("b:") => {
                    if !large {
                        break;
                    }
                    cursor = format!("{root}\n{object}");
                    touched.insert(root.clone());
                    match self.claim_large(c, root, object, path)? {
                        Some(claim) => {
                            claimed = Some(Box::new(claim));
                            break;
                        }
                        None => {
                            report.kept += 1;
                            retry.insert(root.clone());
                            continue;
                        }
                    }
                }
                Some(size) => {
                    if size <= half && size > *budget / 2 {
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
                    retry.insert(root.clone());
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
                tx.execute("DELETE FROM drop_retry WHERE root=?1", [root])?;
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
        for root in &retry {
            retry_root(&tx, root, now)?;
        }
        tx.commit()?;
        if let Some(claim) = claimed {
            return Ok(Leftover::Large(claim));
        }
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
        move_to_drop(&tx, root, source)?;
        tx.commit()?;
        Ok(Some(u64::try_from(freed).unwrap_or(0)))
    }
}

/// `root` (the `received` root of `source`) leaves, in the caller's
/// transaction: its members move to a new `abandoned` root
/// `drop:<source>:<ulid>`, its graph and `clone_ref` rows go, and a
/// `(clone, source)` with no `clone_ref` row left gets a `ref_drop:` row.
fn move_to_drop(tx: &Connection, root: &str, source: &str) -> Result<()> {
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
    Ok(())
}

/// Review round 3 (H-1): the file of `object` is gone and its claim
/// completes, in the caller's transaction. Every `received` root that holds
/// it, and every `received` root whose graph names one of those (a parent
/// carrying it), leaves as a detach does: its source is no longer in the
/// store, so the next attempt that names it is refused with
/// `INPUT_MISSING` and the parent sends it again (spec 25.8 5b (b), (h)8).
/// Then every membership of `object` goes (a `pending` root's publication
/// checks its files again and answers `INPUT_MISSING` as well).
pub(super) fn release_holders(tx: &Connection, object: &str) -> Result<()> {
    let mut queue: Vec<(String, String)> = {
        let mut s = tx.prepare(
            "SELECT DISTINCT r.id,r.source FROM member m JOIN root r ON r.id=m.root
             WHERE m.object=?1 AND r.kind='received' AND r.source IS NOT NULL",
        )?;
        let rows = s.query_map([object], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    while let Some((root, source)) = queue.pop() {
        let parents: Vec<(String, String)> = {
            let mut s = tx.prepare(
                "SELECT DISTINCT r.id,r.source FROM graph g JOIN root r ON r.id=g.root
                 WHERE g.source=?1 AND g.root<>?2 AND r.kind='received' AND r.source IS NOT NULL",
            )?;
            let rows = s.query_map(params![source, root], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        let still: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM root WHERE id=?1 AND kind='received')",
            [&root],
            |r| r.get(0),
        )?;
        if still {
            move_to_drop(tx, &root, &source)?;
        }
        queue.extend(parents);
    }
    tx.execute("DELETE FROM member WHERE object=?1", [object])?;
    Ok(())
}

/// The root `root` has a member that could not be proven: step 0 reads it
/// again after [`RETRY_MS`] (one row per abandoned root, gone with it).
fn retry_root(c: &Connection, root: &str, now: u64) -> Result<()> {
    c.execute(
        "INSERT INTO drop_retry(root,next_ms) SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM root WHERE id=?1)
         ON CONFLICT(root) DO UPDATE SET next_ms=excluded.next_ms",
        params![root, now as i64 + RETRY_MS],
    )?;
    Ok(())
}

#[cfg(test)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
thread_local! { static BUDGET: std::cell::Cell<u64> = const { std::cell::Cell::new(BYTE_BUDGET) }; }
/// A section's byte budget (64 MiB; tests set a small one, (g)14).
fn section_budget() -> u64 {
    #[cfg(test)]
    return BUDGET.get();
    #[cfg(not(test))]
    BYTE_BUDGET
}
#[cfg(test)]
pub(in crate::source::retention) fn set_budget(bytes: u64) {
    BUDGET.set(bytes);
}
