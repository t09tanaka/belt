//! What the tool keeps on disk, for how long, and what removes it.
//!
//! Most things are removed at the moment they stop being needed (a lease
//! and a ledger entry when the command ends, a job's output when `wait` has
//! printed it, a measurement marker when its record is trimmed). The sweep
//! here is the backstop for what a crash left behind and for what only
//! time makes old. `task-traffic-controller gc` runs it on demand
//! ([`sweep`]); [`sweep_if_due`] runs it by itself, at most once every
//! [`SWEEP_EVERY_SECS`], after a command has finished and released its
//! entry ([`crate::exec::run_gated`], [`crate::managed::finish`]).
//!
//! | what | kept | removed by |
//! |---|---|---|
//! | `ledger.json` entries | while the command lives | its end; dead ones on the fly and by the sweep |
//! | `leases/<id>.lock` | while the entry lives | the end of the command; the sweep, an hour after a crash left one without an entry |
//! | `history.jsonl` | newest `history_keep` records per key, and only keys seen within `history_keep_days` | sweep |
//! | `measurements/*.applied` | while the history keeps the record | the trim that drops the record |
//! | `measurements/*.pending` | none (a dead attempt's) | sweep |
//! | `managed/<id>.json` | `managed_keep_days` after the command ended; an hour when it was never launched and has no entry, or is not JSON | sweep |
//! | `jobs/<id>.log` | at most `jobs_log_max_mib`, while running too; until `wait` printed it | the runner, `wait`, sweep |
//! | `jobs/<id>.json`, `.exit` | `jobs_keep_days` after the job finished or was found dead | sweep |
//! | job files without a readable descriptor, temporary files | an hour, and only when their owner is gone | sweep |
//! | `ledger.lock`, `history.lock`, `last-sweep`, `leases/`, `jobs/`, `managed/` | fixed: every command, of every released version, opens them | never |
//!
//! Nothing here removes what a live process may use: an entry, a held
//! lease, a job a `wait` holds, a temporary file whose writer is alive. A
//! lock file is only removed while the sweep holds its lock. The automatic
//! sweep never waits for a lock (a busy stage is left for the next try,
//! [`RETRY_AFTER_SECS`] later), never looks at processes other than with
//! signal 0, does at most [`AUTO_MAX_ACTIONS`] removals and
//! [`AUTO_MAX_CUTS`] log cuts a time, and never fails the command that
//! triggered it.

use crate::config::Config;
use crate::error::Result;
use crate::jobs::{self, Job};
use crate::ledger::Ledger;
use crate::{lock, managed, Context};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::path::Path;

/// The automatic sweep runs at most this often (per state directory).
pub const SWEEP_EVERY_SECS: i64 = 6 * 3600;

/// A sweep that left something undone (a lock was busy, or it reached
/// [`AUTO_MAX_ACTIONS`]) is tried again this long after.
pub const RETRY_AFTER_SECS: i64 = 600;

/// The most files one automatic sweep removes, marks or cuts. What is left
/// is done by the next one, [`RETRY_AFTER_SECS`] later.
pub const AUTO_MAX_ACTIONS: usize = 500;

/// The most job logs one automatic sweep cuts: a cut copies the part it
/// keeps, up to `jobs_log_max_mib`.
pub const AUTO_MAX_CUTS: usize = 4;

/// A job with no exit file, no ledger entry and no lease older than this is
/// dead (its runner was killed). Far above the few seconds a runner needs
/// to register.
pub const DEAD_JOB_AFTER_SECS: i64 = 3600;

/// A leftover temporary file, a job file without a readable descriptor, or
/// a managed sidecar that never got its ledger entry must be this old
/// before the sweep removes it (an atomic write takes milliseconds; this is
/// a margin for a stalled machine).
pub const STRAY_AFTER_SECS: i64 = 3600;

/// How long the retention numbers another program asked for in this state
/// directory stay in force after its last sweep ([`effective`]).
pub const ASK_TTL_SECS: i64 = 30 * 86_400;

/// At most this many failed steps are spelled out in [`Report::errors`].
const MAX_ERRORS: usize = 8;

/// The numbers the rules use, from a [`Config`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Keep {
    pub history_keep: u64,
    pub history_keep_days: u64,
    pub jobs_keep_days: u64,
    pub jobs_log_max_mib: u64,
    pub managed_keep_days: u64,
}

impl Keep {
    pub fn of(cfg: &Config) -> Keep {
        Keep {
            history_keep: cfg.history_keep as u64,
            history_keep_days: cfg.history_keep_days,
            jobs_keep_days: cfg.jobs_keep_days,
            jobs_log_max_mib: cfg.jobs_log_max_mib,
            managed_keep_days: cfg.managed_keep_days,
        }
    }

    fn fields(&self) -> [(&'static str, u64); 5] {
        [
            ("history_keep", self.history_keep),
            ("history_keep_days", self.history_keep_days),
            ("jobs_keep_days", self.jobs_keep_days),
            ("jobs_log_max_mib", self.jobs_log_max_mib),
            ("managed_keep_days", self.managed_keep_days),
        ]
    }

    fn with(mut self, name: &str, value: u64) -> Keep {
        match name {
            "history_keep" => self.history_keep = value,
            "history_keep_days" => self.history_keep_days = value,
            "jobs_keep_days" => self.jobs_keep_days = value,
            "jobs_log_max_mib" => self.jobs_log_max_mib = value,
            "managed_keep_days" => self.managed_keep_days = value,
            _ => {}
        }
        self
    }

    /// `jobs_log_max_mib` in bytes.
    pub fn jobs_log_max_bytes(&self) -> u64 {
        self.jobs_log_max_mib.saturating_mul(1024 * 1024)
    }
}

/// A number somebody asked the sweep of this state directory to keep by.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ask {
    pub value: u64,
    pub at: DateTime<Utc>,
}

/// The numbers a sweep goes by, and what to record for the next one.
///
/// Programs with different configurations may share a state directory (the
/// `task-traffic-controller` command and a program that embeds the
/// library). Neither may remove what the other still wants, so for each
/// number the larger one wins: `asked` is what the stamp file recorded from
/// earlier sweeps, and a recorded number larger than `own` stays in force
/// for [`ASK_TTL_SECS`] after the sweep that last asked for at least that
/// much. A program that stops using the directory (or lowers its number)
/// therefore stops counting 30 days later.
pub fn effective(
    own: &Keep,
    asked: &BTreeMap<String, Ask>,
    now: DateTime<Utc>,
) -> (Keep, BTreeMap<String, Ask>) {
    let mut keep = *own;
    let mut record = BTreeMap::new();
    for (name, mine) in own.fields() {
        let other = asked
            .get(name)
            .filter(|a| a.value > mine && a.at <= now && !age_at_least(a.at, now, ASK_TTL_SECS));
        let ask = match other {
            Some(a) => a.clone(),
            None => Ask {
                value: mine,
                at: now,
            },
        };
        keep = keep.with(name, ask.value);
        record.insert(name.to_string(), ask);
    }
    (keep, record)
}

/// What a program that is not sweeping writes into the stamp, if anything:
/// its own numbers, when one of them is larger than what is recorded (or
/// nothing valid is), or when its own record is [`ASK_REFRESH_SECS`] old.
/// Without this only the program that happens to do the sweep would be
/// heard, and a program that never sweeps would be forgotten after
/// [`ASK_TTL_SECS`] while in daily use. `None`: nothing to write.
pub fn refreshed_asks(
    own: &Keep,
    asked: &BTreeMap<String, Ask>,
    now: DateTime<Utc>,
) -> Option<BTreeMap<String, Ask>> {
    let (_, record) = effective(own, asked, now);
    let stale = record.iter().any(|(name, ask)| match asked.get(name) {
        Some(old) if old == ask => false,
        // Ours either way (a larger valid one would have been kept as it
        // is): new or changed, or the same number with an old date.
        Some(old) if old.value == ask.value => {
            old.at > now || age_at_least(old.at, now, ASK_REFRESH_SECS)
        }
        _ => true,
    });
    stale.then_some(record)
}

/// How old a program's own recorded numbers may get before it writes them
/// again ([`refreshed_asks`]): at most one small write a day per program.
pub const ASK_REFRESH_SECS: i64 = 86_400;

/// `days` days before `now`; `None` when that is before the calendar
/// begins, so no number in a configuration can make the arithmetic fail:
/// nothing is then old enough.
pub fn days_before(now: DateTime<Utc>, days: u64) -> Option<DateTime<Utc>> {
    let days = Duration::try_days(i64::try_from(days).ok()?)?;
    now.checked_sub_signed(days)
}

fn age_at_least(t: DateTime<Utc>, now: DateTime<Utc>, secs: i64) -> bool {
    now.signed_duration_since(t) >= Duration::seconds(secs)
}

/// What a sweep did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Report {
    pub reaped: usize,
    pub history_dropped: usize,
    pub jobs_removed: usize,
    pub jobs_found_dead: usize,
    pub logs_cut: usize,
    pub markers_removed: usize,
    pub receipts_removed: usize,
    pub strays_removed: usize,
    /// Steps that failed; the others still ran.
    pub errors: Vec<String>,
    /// False when something was left for the next sweep: a lock was busy,
    /// or the sweep reached [`AUTO_MAX_ACTIONS`].
    pub complete: bool,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "reaped {} dead entries, trimmed {} history records, removed {} old jobs, \
             removed {} old measurement markers, removed {} old managed receipts, \
             marked {} dead jobs finished, cut {} long job logs, removed {} stray files",
            self.reaped,
            self.history_dropped,
            self.jobs_removed,
            self.markers_removed,
            self.receipts_removed,
            self.jobs_found_dead,
            self.logs_cut,
            self.strays_removed,
        )
    }
}

// --------------------------------------------------------------- stamp

/// The stamp file (`last-sweep`): when the sweep last finished, when it was
/// last tried, and the numbers asked for ([`effective`]).
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stamp {
    /// When a sweep last did everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complete: Option<DateTime<Utc>>,
    /// When a sweep was last started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub asked: BTreeMap<String, Ask>,
}

/// What reading the stamp file gave.
#[derive(Debug, Clone, PartialEq)]
pub enum StampRead {
    /// No file: no sweep was ever due here.
    Absent,
    /// A file that cannot be read or understood.
    Unreadable,
    Read(Stamp),
}

/// What the text of a stamp file says; `None` when it says nothing usable
/// (empty, cut short, not ours).
pub fn parse_stamp(text: &str) -> Option<Stamp> {
    serde_json::from_str(text.trim()).ok()
}

/// What to do about the automatic sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// First use of this state directory: only write the stamp. Nothing
    /// can be old yet.
    Arm,
    Sweep,
    No,
}

/// Only an absent stamp arms the clock. A stamp that cannot be read or
/// understood means a sweep is due (a full disk leaves an empty stamp, and
/// that is when the sweep is needed). Otherwise a sweep is due
/// [`SWEEP_EVERY_SECS`] after the last complete one, or at once when that
/// is in the future (the clock went back) or never happened — but not
/// within [`RETRY_AFTER_SECS`] of the last try.
pub fn due(read: &StampRead, now: DateTime<Utc>) -> Due {
    let stamp = match read {
        StampRead::Absent => return Due::Arm,
        StampRead::Unreadable => return Due::Sweep,
        StampRead::Read(s) => s,
    };
    if stamp
        .attempt
        .is_some_and(|a| a <= now && !age_at_least(a, now, RETRY_AFTER_SECS))
    {
        return Due::No;
    }
    match stamp.complete {
        Some(last) if last <= now && !age_at_least(last, now, SWEEP_EVERY_SECS) => Due::No,
        _ => Due::Sweep,
    }
}

fn read_stamp(path: &Path) -> StampRead {
    match std::fs::read_to_string(path) {
        Ok(text) => parse_stamp(&text).map_or(StampRead::Unreadable, StampRead::Read),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => StampRead::Absent,
        Err(_) => StampRead::Unreadable,
    }
}

/// Written in place (the file keeps its inode, which the single-sweeper
/// lock is on). A reader that catches it half-written finds it unreadable,
/// which only means "due".
fn write_stamp(path: &Path, stamp: &Stamp) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(
        path,
        serde_json::to_string(stamp).map_err(std::io::Error::other)?,
    )
}

/// Run the sweep when it is due ([`due`]); `None` when it is not, or when
/// another process is sweeping. Failed steps are in [`Report::errors`];
/// nothing is printed and nothing fails the caller, so an embedder may
/// ignore the result.
///
/// The library calls this itself at the end of [`crate::exec::run_gated`]
/// and of [`crate::managed::finish`]: after the command, never before it. A
/// program that writes state by other means (only
/// [`crate::managed::record_measurement`], say) calls it from its own entry
/// points, after its work.
pub fn sweep_if_due(ctx: &Context, cfg: &Config) -> Option<Report> {
    sweep_if_due_keep(ctx, &Keep::of(cfg))
}

pub(crate) fn sweep_if_due_keep(ctx: &Context, own: &Keep) -> Option<Report> {
    let path = ctx.paths.sweep_stamp();
    let read = read_stamp(&path);
    let now = ctx.clock.now();
    match due(&read, now) {
        Due::No => {
            // Not sweeping: still say what this program keeps by, so the
            // one that does sweep goes by the larger numbers.
            if let StampRead::Read(stamp) = read {
                if let Some(asked) = refreshed_asks(own, &stamp.asked, now) {
                    if let Ok(Some(_one)) = lock::try_acquire(&path) {
                        let _ = write_stamp(&path, &Stamp { asked, ..stamp });
                    }
                }
            }
            return None;
        }
        Due::Arm => {
            let armed = Stamp {
                complete: Some(now),
                attempt: None,
                asked: effective(own, &BTreeMap::new(), now).1,
            };
            // A stamp that cannot be written must not mean "never sweep".
            if write_stamp(&path, &armed).is_ok() {
                return None;
            }
        }
        Due::Sweep => {}
    }
    // One sweeper at a time, without waiting. When the stamp cannot even
    // be opened the sweep still runs.
    let _one = match lock::try_acquire(&path) {
        Ok(None) => return None,
        Ok(Some(lock)) => Some(lock),
        Err(_) => None,
    };
    Some(run(ctx, own, read, true))
}

/// Apply every retention rule now (what `gc` does): the same rules as the
/// automatic sweep, but it waits for the locks, has no limit on how much it
/// removes, and scans processes to release managed entries whose owner is
/// gone. Every step runs even when an earlier one failed.
pub fn sweep(ctx: &Context, cfg: &Config) -> Report {
    let read = read_stamp(&ctx.paths.sweep_stamp());
    run(ctx, &Keep::of(cfg), read, false)
}

/// What is left of a sweep's allowance.
struct Allowance {
    left: usize,
    /// Log cuts left: each copies up to `jobs_log_max_mib`.
    cuts: usize,
    /// Something was left undone.
    short: bool,
}

impl Allowance {
    /// Whether one more action may be done; notes when it may not.
    fn take(&mut self) -> bool {
        if self.left == 0 {
            self.short = true;
            return false;
        }
        self.left -= 1;
        true
    }

    fn take_cut(&mut self) -> bool {
        if self.cuts == 0 {
            self.short = true;
            return false;
        }
        self.cuts -= 1;
        true
    }
}

struct Errors {
    shown: Vec<String>,
    more: usize,
}

impl Errors {
    fn note(&mut self, what: impl fmt::Display) {
        if self.shown.len() < MAX_ERRORS {
            self.shown.push(what.to_string());
        } else {
            self.more += 1;
        }
    }

    fn ok<T>(&mut self, name: &str, res: Result<T>) -> Option<T> {
        res.map_err(|e| self.note(format_args!("{name}: {e}"))).ok()
    }

    fn done(mut self) -> Vec<String> {
        if self.more > 0 {
            self.shown.push(format!("and {} more", self.more));
        }
        self.shown
    }
}

fn run(ctx: &Context, own: &Keep, read: StampRead, auto: bool) -> Report {
    let now = ctx.clock.now();
    let path = ctx.paths.sweep_stamp();
    let before = match read {
        StampRead::Read(s) => s,
        _ => Stamp::default(),
    };
    let (keep, asked) = effective(own, &before.asked, now);
    // Before the work, so that a process arriving meanwhile stays away.
    let mut stamp = Stamp {
        complete: before.complete,
        attempt: Some(now),
        asked,
    };
    let mut stamp_error = write_stamp(&path, &stamp).err();

    let mut r = Report::default();
    let mut errors = Errors {
        shown: Vec::new(),
        more: 0,
    };
    let mut allow = Allowance {
        left: if auto { AUTO_MAX_ACTIONS } else { usize::MAX },
        cuts: if auto { AUTO_MAX_CUTS } else { usize::MAX },
        short: false,
    };

    // The ledger, dead entries, leases nobody has, managed receipts: all
    // under the ledger lock.
    if !auto {
        lock::refresh_verdicts(ctx);
    }
    let guard = if auto {
        errors
            .ok("ledger", lock::try_acquire(&ctx.paths.ledger_lock()))
            .flatten()
    } else {
        errors.ok("ledger", lock::ledger(ctx))
    };
    let mut ledger: Option<Ledger> = None;
    match guard {
        None => allow.short = true,
        Some(_guard) => {
            if let Some((l, reaped)) = errors.ok("ledger", reap(ctx)) {
                r.reaped = reaped;
                lock::sweep_orphan_leases(ctx, &l);
                r.receipts_removed =
                    managed::gc_sidecars_locked(ctx, &l, keep.managed_keep_days, &mut || {
                        allow.take()
                    });
                ledger = Some(l);
            }
        }
    }

    // The history and the measurement markers: under the history lock.
    let guard = if auto {
        errors
            .ok("history", lock::try_acquire(&ctx.paths.history_lock()))
            .flatten()
    } else {
        errors.ok(
            "history",
            lock::acquire(&ctx.paths.history_lock(), ctx.ledger_lock_timeout),
        )
    };
    match guard {
        None => allow.short = true,
        Some(_guard) => {
            r.history_dropped = errors
                .ok(
                    "history",
                    managed::trim_history_locked(
                        ctx,
                        usize::try_from(keep.history_keep).unwrap_or(usize::MAX),
                        // No cutoff when the number of days is beyond the
                        // calendar: no key is then too old.
                        days_before(now, keep.history_keep_days),
                    ),
                )
                .unwrap_or(0);
            r.markers_removed = errors
                .ok("measurements", managed::gc_measurements_locked(ctx))
                .unwrap_or(0);
        }
    }

    if let Some(ledger) = &ledger {
        sweep_jobs(ctx, &keep, ledger, &mut r, &mut errors, &mut allow);
        sweep_strays(ctx, ledger, &mut r, &mut errors, &mut allow);
    }

    r.complete = !allow.short;
    if r.complete {
        stamp.complete = Some(now);
        stamp.attempt = None;
        if let Err(e) = write_stamp(&path, &stamp) {
            stamp_error = Some(e);
        }
    }
    if let Some(e) = stamp_error {
        errors.note(format_args!(
            "{}: {e}; the cleanup ran, and runs again after the next command until this file \
             can be written",
            path.display()
        ));
    }
    r.errors = errors.done();
    r
}

/// Reap dead ledger entries. Returns the ledger as it is now and how many
/// entries were reaped. The caller holds the ledger lock.
fn reap(ctx: &Context) -> Result<(Ledger, usize)> {
    let mut ledger = Ledger::load(&ctx.paths.ledger_file())?;
    let dead = lock::reap_dead(ctx, &mut ledger);
    if !dead.is_empty() {
        ledger.save(&ctx.paths.ledger_file())?;
    }
    Ok((ledger, dead.len()))
}

// ------------------------------------------------------------- sidecars

/// What a `managed/<id>.json` says.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SidecarKind {
    /// Its command ended then.
    Ended(DateTime<Utc>),
    /// Registered and never launched: with no ledger entry, what a crash
    /// between writing the sidecar and saving the ledger left.
    NeverLaunched,
    /// Not JSON at all: a write that was cut short.
    Unparseable,
    /// A launch was begun, or the file is another version's: only
    /// `resolve` (or its owner) settles it.
    Other,
}

/// What the sweep knows about one `managed/<id>.json`.
#[derive(Debug, Clone, PartialEq)]
pub struct SidecarView {
    pub kind: SidecarKind,
    /// The ledger has an entry for it.
    pub in_ledger: bool,
    /// Its owner holds the lease.
    pub lease_held: bool,
    pub modified: DateTime<Utc>,
}

/// Whether a managed sidecar goes. Never while the ledger has its entry or
/// its owner holds the lease. One that records an end goes
/// `managed_keep_days` after it (a repeated registration finds the outcome
/// until then). One that was never launched, or is not JSON, goes once the
/// file is [`STRAY_AFTER_SECS`] old: nothing was started for it. One whose
/// launch was begun stays: a process of it may be alive, and removing the
/// record would let the same id be started again (see `resolve`).
pub fn sidecar_due(v: &SidecarView, now: DateTime<Utc>, keep_days: u64) -> bool {
    if v.in_ledger || v.lease_held {
        return false;
    }
    match v.kind {
        SidecarKind::Ended(t) => days_before(now, keep_days).is_some_and(|cutoff| t < cutoff),
        SidecarKind::NeverLaunched | SidecarKind::Unparseable => {
            age_at_least(v.modified, now, STRAY_AFTER_SECS)
        }
        SidecarKind::Other => false,
    }
}

// ---------------------------------------------------------------- jobs

/// What the sweep knows about one job.
#[derive(Debug, Clone, PartialEq)]
pub struct JobView {
    pub finished_at: Option<DateTime<Utc>>,
    pub submitted_at: DateTime<Utc>,
    /// The runner's exit file exists, and when it was written.
    pub exit_written: Option<DateTime<Utc>>,
    /// Somebody may still use it: its runner has a ledger entry or holds
    /// its lease, or a `wait` holds its waiter lock. This is the liveness
    /// test, not a guess from a pid.
    pub live: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JobAction {
    Keep,
    /// Delete all its files.
    Remove,
    /// Give a job whose runner is gone the finish time it never got, so the
    /// usual rule can remove it `jobs_keep_days` later.
    MarkFinished(DateTime<Utc>),
}

pub fn job_action(v: &JobView, now: DateTime<Utc>, keep_days: u64) -> JobAction {
    if v.live {
        return JobAction::Keep;
    }
    match v.finished_at {
        Some(t) if days_before(now, keep_days).is_some_and(|cutoff| t < cutoff) => {
            JobAction::Remove
        }
        Some(_) => JobAction::Keep,
        None => match v.exit_written {
            Some(t) => JobAction::MarkFinished(t),
            None if age_at_least(v.submitted_at, now, DEAD_JOB_AFTER_SECS) => {
                JobAction::MarkFinished(now)
            }
            None => JobAction::Keep,
        },
    }
}

/// One file in a directory the sweep looks at.
#[derive(Debug, Clone, PartialEq)]
pub struct FileInfo {
    pub name: String,
    pub modified: DateTime<Utc>,
}

fn old_enough(f: &FileInfo, now: DateTime<Utc>) -> bool {
    age_at_least(f.modified, now, STRAY_AFTER_SECS)
}

/// Whose temporary file a name is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempOwner {
    /// Ours, made by this process id ([`crate::paths::TEMP_PREFIX`]).
    Pid(u32),
    /// Ours by its prefix, but the rest says nothing: kept.
    Unknown,
    /// The name released versions gave their temporary files (`.tmp` and
    /// six letters or digits). It names no owner.
    Legacy,
}

/// `None` for a name that is not a temporary file of this tool.
pub fn temp_owner(name: &str) -> Option<TempOwner> {
    if let Some(rest) = name.strip_prefix(crate::paths::TEMP_PREFIX) {
        return Some(
            rest.split_once('.')
                .and_then(|(pid, _)| pid.parse().ok())
                .map_or(TempOwner::Unknown, TempOwner::Pid),
        );
    }
    let rest = name.strip_prefix(".tmp")?;
    (!rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphanumeric()))
        .then_some(TempOwner::Legacy)
}

/// Temporary files of an interrupted atomic write that nothing can still
/// be using: [`STRAY_AFTER_SECS`] old, and their writer gone (`alive` says
/// whether a pid is; a writer stopped for longer than that hour keeps its
/// file). A name with no readable pid is kept. A released version's file
/// names no owner and goes by age alone, and only where `legacy` allows
/// (the tool's own directories). Any other name is kept.
pub fn stale_temp_files(
    files: &[FileInfo],
    alive: &dyn Fn(u32) -> bool,
    legacy: bool,
    now: DateTime<Utc>,
) -> Vec<String> {
    files
        .iter()
        .filter(|f| {
            old_enough(f, now)
                && match temp_owner(&f.name) {
                    Some(TempOwner::Pid(pid)) => !alive(pid),
                    Some(TempOwner::Legacy) => legacy,
                    Some(TempOwner::Unknown) | None => false,
                }
        })
        .map(|f| f.name.clone())
        .collect()
}

/// Whether the process `pid` exists, asked with signal 0 only. A pid that
/// must never be signalled (0, 1, or one that would wrap to a negative
/// number and so name a group or everything) counts as alive.
pub fn pid_alive(pid: u32) -> bool {
    if pid <= 1 || pid > i32::MAX as u32 {
        return true;
    }
    // SAFETY: signal 0 delivers nothing; the pid is a single process.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// The ids in `jobs/` that have no readable descriptor and whose files
/// (`<id>.json`, `.log`, `.exit`, `.waiter`) are all [`STRAY_AFTER_SECS`]
/// old and not of a live job: what a crash while removing a job left, or a
/// descriptor that was damaged. `readable` are the ids whose descriptor
/// loads (those are the job rule's). Files with any other name are not
/// looked at.
pub fn orphan_job_ids(
    files: &[FileInfo],
    readable: &HashSet<String>,
    live: &dyn Fn(&str) -> bool,
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut all_old: BTreeMap<&str, bool> = BTreeMap::new();
    for f in files {
        let Some((id, ext)) = f.name.rsplit_once('.') else {
            continue;
        };
        if id.is_empty() || !matches!(ext, "json" | "log" | "exit" | "waiter") {
            continue;
        }
        *all_old.entry(id).or_insert(true) &= old_enough(f, now);
    }
    all_old
        .into_iter()
        .filter(|(id, old)| *old && !readable.contains(*id) && !live(id))
        .map(|(id, _)| id.to_string())
        .collect()
}

fn list_files(dir: &Path) -> Vec<FileInfo> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.flatten()
        .filter_map(|e| {
            let m = e.metadata().ok()?;
            if !m.is_file() {
                return None;
            }
            Some(FileInfo {
                name: e.file_name().into_string().ok()?,
                modified: DateTime::<Utc>::from(m.modified().ok()?),
            })
        })
        .collect()
}

fn job_is_live(ctx: &Context, ledger: &Ledger, id: &str) -> bool {
    ledger.find(id).is_some() || lock::lease_is_held(ctx, id) || jobs::has_waiter(ctx, id)
}

/// Apply [`job_action`] to every job, and cut the logs of the jobs that are
/// over. A job that fails is noted and the next one is looked at.
fn sweep_jobs(
    ctx: &Context,
    keep: &Keep,
    ledger: &Ledger,
    r: &mut Report,
    errors: &mut Errors,
    allow: &mut Allowance,
) {
    let now = ctx.clock.now();
    let max = keep.jobs_log_max_bytes();
    let Some(all) = errors.ok("jobs", jobs::list(ctx)) else {
        return;
    };
    for job in all {
        let live = job_is_live(ctx, ledger, &job.id);
        let exit_written = std::fs::metadata(ctx.paths.job_exit(&job.id))
            .and_then(|m| m.modified())
            .ok()
            .map(DateTime::<Utc>::from);
        let view = JobView {
            finished_at: job.finished_at,
            submitted_at: job.submitted_at,
            exit_written,
            live,
        };
        let action = job_action(&view, now, keep.jobs_keep_days);
        if action != JobAction::Keep && !allow.take() {
            return;
        }
        match action {
            JobAction::Remove => {
                // False when a `wait` took the job meanwhile: it stays.
                if jobs::remove_files(ctx, &job.id) {
                    r.jobs_removed += 1;
                }
                continue;
            }
            JobAction::MarkFinished(t) => {
                let id = job.id.clone();
                match mark_finished(ctx, job, t) {
                    Ok(()) => r.jobs_found_dead += 1,
                    Err(e) => errors.note(format_args!("job {id}: {e}")),
                }
                cut_log(ctx, &id, live, max, r, errors, allow);
            }
            JobAction::Keep => cut_log(ctx, &job.id, live, max, r, errors, allow),
        }
    }
}

/// Cut the log of a job that is over (a running job's runner cuts its own;
/// a job a `wait` holds may be being read).
fn cut_log(
    ctx: &Context,
    id: &str,
    live: bool,
    max: u64,
    r: &mut Report,
    errors: &mut Errors,
    allow: &mut Allowance,
) {
    let over = std::fs::metadata(ctx.paths.job_log(id)).is_ok_and(|m| m.len() > max);
    if live || !over || !allow.take_cut() {
        return;
    }
    match jobs::cap_log(ctx, id, max) {
        Ok(true) => r.logs_cut += 1,
        Ok(false) => {}
        Err(e) => errors.note(format_args!("job {id}: log: {e}")),
    }
}

fn mark_finished(ctx: &Context, mut job: Job, at: DateTime<Utc>) -> Result<()> {
    job.finished_at = Some(at);
    jobs::save(ctx, &job)
}

/// Leftover temporary files and the files of jobs without a readable
/// descriptor. Temporary files: in the state directory, `jobs/` and
/// `managed/`, and, by this version's own name only, in the directory of a
/// history file kept elsewhere (that directory is not ours).
fn sweep_strays(
    ctx: &Context,
    ledger: &Ledger,
    r: &mut Report,
    errors: &mut Errors,
    allow: &mut Allowance,
) {
    let now = ctx.clock.now();
    let state = ctx.paths.state_dir().to_path_buf();
    let mut dirs = vec![
        (state.clone(), true),
        (ctx.paths.jobs_dir(), true),
        (ctx.paths.managed_dir(), true),
    ];
    if let Some(dir) = ctx.paths.history_file().parent() {
        if dir != state {
            dirs.push((dir.to_path_buf(), false));
        }
    }
    for (dir, legacy) in dirs {
        for name in stale_temp_files(&list_files(&dir), &pid_alive, legacy, now) {
            if !allow.take() {
                return;
            }
            match std::fs::remove_file(dir.join(&name)) {
                Ok(()) => r.strays_removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => errors.note(format_args!("{}: {e}", dir.join(&name).display())),
            }
        }
    }
    let Some(jobs) = errors.ok("jobs", jobs::list(ctx)) else {
        return;
    };
    let readable: HashSet<String> = jobs.into_iter().map(|j| j.id).collect();
    let live = |id: &str| job_is_live(ctx, ledger, id);
    for id in orphan_job_ids(&list_files(&ctx.paths.jobs_dir()), &readable, &live, now) {
        if !allow.take() {
            return;
        }
        if jobs::remove_files(ctx, &id) {
            r.strays_removed += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    const DAY: i64 = 86_400;

    fn stamp(complete: Option<i64>, attempt: Option<i64>) -> StampRead {
        StampRead::Read(Stamp {
            complete: complete.map(t),
            attempt: attempt.map(t),
            asked: BTreeMap::new(),
        })
    }

    #[test]
    fn only_an_absent_stamp_arms_the_clock_and_an_unreadable_one_is_due() {
        assert_eq!(due(&StampRead::Absent, t(0)), Due::Arm);
        assert_eq!(due(&StampRead::Unreadable, t(0)), Due::Sweep);
        // Readable, but it never recorded a complete sweep.
        assert_eq!(due(&stamp(None, None), t(0)), Due::Sweep);
        assert_eq!(parse_stamp(""), None);
        assert_eq!(parse_stamp("2026-01-01T00:00:00Z"), None);
        assert_eq!(parse_stamp("{\"complete\":\"2026-01-"), None);
        assert_eq!(parse_stamp(" {} \n"), Some(Stamp::default()));
    }

    #[test]
    fn the_sweep_is_due_exactly_after_the_interval_or_from_a_future_stamp() {
        let now = t(0);
        assert_eq!(due(&stamp(Some(-SWEEP_EVERY_SECS + 1), None), now), Due::No);
        assert_eq!(due(&stamp(Some(-SWEEP_EVERY_SECS), None), now), Due::Sweep);
        assert_eq!(due(&stamp(Some(0), None), now), Due::No);
        assert_eq!(due(&stamp(Some(1), None), now), Due::Sweep);
    }

    #[test]
    fn an_unfinished_sweep_is_tried_again_after_the_retry_time_and_not_before() {
        let now = t(0);
        let old = Some(-2 * SWEEP_EVERY_SECS);
        assert_eq!(due(&stamp(old, Some(-RETRY_AFTER_SECS + 1)), now), Due::No);
        assert_eq!(due(&stamp(old, Some(-RETRY_AFTER_SECS)), now), Due::Sweep);
        assert_eq!(due(&stamp(None, Some(0)), now), Due::No);
        // A try recorded in the future (the clock went back) holds nothing.
        assert_eq!(due(&stamp(old, Some(1)), now), Due::Sweep);
        // A recent try does not make a fresh complete sweep due.
        assert_eq!(due(&stamp(Some(-5), Some(-RETRY_AFTER_SECS)), now), Due::No);
    }

    fn keep(n: u64) -> Keep {
        Keep {
            history_keep: n,
            history_keep_days: n,
            jobs_keep_days: n,
            jobs_log_max_mib: n,
            managed_keep_days: n,
        }
    }

    #[test]
    fn the_larger_number_wins_for_thirty_days_after_it_was_last_asked_for() {
        let now = t(0);
        let ask = |value, at| Ask { value, at: t(at) };
        let mut asked = BTreeMap::new();
        // Larger and recent: wins and keeps its date.
        asked.insert("history_keep".to_string(), ask(200, -5));
        // Larger but exactly at the limit: no longer counts.
        asked.insert("history_keep_days".to_string(), ask(90, -ASK_TTL_SECS));
        // One second inside the limit: still counts.
        asked.insert("jobs_keep_days".to_string(), ask(90, -ASK_TTL_SECS + 1));
        // Equal: ours, dated now. Smaller: ours.
        asked.insert("jobs_log_max_mib".to_string(), ask(50, -5));
        asked.insert("managed_keep_days".to_string(), ask(7, -5));
        // Not a number of ours: dropped.
        asked.insert("other".to_string(), ask(9999, -5));
        let (got, record) = effective(&keep(50), &asked, now);
        assert_eq!(
            got,
            Keep {
                history_keep: 200,
                history_keep_days: 50,
                jobs_keep_days: 90,
                jobs_log_max_mib: 50,
                managed_keep_days: 50,
            }
        );
        assert_eq!(record.len(), 5);
        assert_eq!(record["history_keep"], ask(200, -5));
        assert_eq!(record["history_keep_days"], ask(50, 0));
        assert_eq!(record["jobs_keep_days"], ask(90, -ASK_TTL_SECS + 1));
        assert_eq!(record["jobs_log_max_mib"], ask(50, 0));
        assert_eq!(record["managed_keep_days"], ask(50, 0));
        // An ask dated in the future (the clock went back) does not count.
        let mut future = BTreeMap::new();
        future.insert("history_keep".to_string(), ask(200, 1));
        assert_eq!(effective(&keep(50), &future, now).0, keep(50));
        // The program that asked for more refreshes its own date.
        let (_, again) = effective(&keep(200), &record, now);
        assert_eq!(again["history_keep"], ask(200, 0));
    }

    #[test]
    fn a_program_that_does_not_sweep_records_its_numbers_once_a_day_or_when_they_rise() {
        let now = t(0);
        let ask = |value, at| Ask { value, at: t(at) };
        let all = |value, at| -> BTreeMap<String, Ask> {
            keep(0)
                .fields()
                .iter()
                .map(|(n, _)| (n.to_string(), ask(value, at)))
                .collect()
        };
        // Nothing recorded: write ours.
        assert_eq!(
            refreshed_asks(&keep(50), &BTreeMap::new(), now),
            Some(all(50, 0))
        );
        // Ours, recorded less than a day ago: nothing to write.
        assert_eq!(
            refreshed_asks(&keep(50), &all(50, -ASK_REFRESH_SECS + 1), now),
            None
        );
        // A day old: written again, so it does not run out while in use.
        assert_eq!(
            refreshed_asks(&keep(50), &all(50, -ASK_REFRESH_SECS), now),
            Some(all(50, 0))
        );
        // Ours is larger than the record: written at once.
        assert_eq!(
            refreshed_asks(&keep(200), &all(50, -5), now),
            Some(all(200, 0))
        );
        // Another program's larger numbers: left exactly as they are.
        assert_eq!(refreshed_asks(&keep(50), &all(200, -5 * DAY), now), None);
        // ...until they run out.
        assert_eq!(
            refreshed_asks(&keep(50), &all(200, -ASK_TTL_SECS), now),
            Some(all(50, 0))
        );
        // A date in the future is replaced.
        assert_eq!(
            refreshed_asks(&keep(50), &all(50, 5), now),
            Some(all(50, 0))
        );
    }

    #[test]
    fn no_number_of_days_can_make_the_date_arithmetic_fail() {
        let now = t(0);
        assert_eq!(days_before(now, 0), Some(now));
        assert_eq!(days_before(now, 1), Some(t(-DAY)));
        assert_eq!(days_before(now, u64::MAX), None);
        assert_eq!(days_before(now, i64::MAX as u64), None);
        assert_eq!(days_before(now, 200_000_000), None);
        assert_eq!(keep(u64::MAX).jobs_log_max_bytes(), u64::MAX);
        assert_eq!(keep(16).jobs_log_max_bytes(), 16 << 20);
        // Nothing is old enough then: kept, not removed, and no panic.
        let v = view(Some(-3000 * DAY), -3001 * DAY, None, false);
        assert_eq!(job_action(&v, now, u64::MAX), JobAction::Keep);
        let s = sidecar(Some(-3000 * DAY), false, false, 3000 * DAY);
        assert!(!sidecar_due(&s, now, u64::MAX));
    }

    fn view(finished: Option<i64>, submitted: i64, exit: Option<i64>, live: bool) -> JobView {
        JobView {
            finished_at: finished.map(t),
            submitted_at: t(submitted),
            exit_written: exit.map(t),
            live,
        }
    }

    #[test]
    fn a_finished_job_goes_after_keep_days_and_not_before() {
        let now = t(0);
        let keep = |fin| job_action(&view(Some(fin), -10 * DAY, Some(fin), false), now, 7);
        assert_eq!(keep(-7 * DAY), JobAction::Keep);
        assert_eq!(keep(-7 * DAY - 1), JobAction::Remove);
        // Zero days: gone as soon as it is in the past.
        let v = view(Some(-1), -10, Some(-1), false);
        assert_eq!(job_action(&v, now, 0), JobAction::Remove);
        let v = view(Some(0), -10, Some(0), false);
        assert_eq!(job_action(&v, now, 0), JobAction::Keep);
    }

    #[test]
    fn a_live_job_is_never_touched() {
        let v = view(Some(-30 * DAY), -31 * DAY, Some(-30 * DAY), true);
        assert_eq!(job_action(&v, t(0), 7), JobAction::Keep);
        let v = view(None, -30 * DAY, None, true);
        assert_eq!(job_action(&v, t(0), 7), JobAction::Keep);
    }

    #[test]
    fn a_job_whose_runner_is_gone_gets_a_finish_time() {
        let now = t(0);
        // Not dead yet: the runner may still be starting.
        let young = view(None, -DEAD_JOB_AFTER_SECS + 1, None, false);
        assert_eq!(job_action(&young, now, 7), JobAction::Keep);
        let dead = view(None, -DEAD_JOB_AFTER_SECS, None, false);
        assert_eq!(job_action(&dead, now, 7), JobAction::MarkFinished(now));
        // It wrote its exit but not the finish time: that is the time.
        let done = view(None, -5, Some(-3), false);
        assert_eq!(job_action(&done, now, 7), JobAction::MarkFinished(t(-3)));
    }

    fn sidecar(ended: Option<i64>, in_ledger: bool, lease_held: bool, age: i64) -> SidecarView {
        SidecarView {
            kind: ended.map_or(SidecarKind::NeverLaunched, |s| SidecarKind::Ended(t(s))),
            in_ledger,
            lease_held,
            modified: t(-age),
        }
    }

    #[test]
    fn a_sidecar_goes_by_its_end_or_by_its_age_when_it_never_got_an_entry() {
        let now = t(0);
        // Ended: keep_days after the end, whatever the file's age.
        assert!(!sidecar_due(
            &sidecar(Some(-7 * DAY), false, false, 9 * DAY),
            now,
            7
        ));
        assert!(sidecar_due(
            &sidecar(Some(-7 * DAY - 1), false, false, 0),
            now,
            7
        ));
        // No end recorded (or unreadable): an hour after it was written.
        assert!(!sidecar_due(
            &sidecar(None, false, false, STRAY_AFTER_SECS - 1),
            now,
            7
        ));
        assert!(sidecar_due(
            &sidecar(None, false, false, STRAY_AFTER_SECS),
            now,
            7
        ));
        let of = |kind, age| SidecarView {
            kind,
            ..sidecar(None, false, false, age)
        };
        assert!(!sidecar_due(
            &of(SidecarKind::Unparseable, STRAY_AFTER_SECS - 1),
            now,
            7
        ));
        assert!(sidecar_due(
            &of(SidecarKind::Unparseable, STRAY_AFTER_SECS),
            now,
            7
        ));
        // A launch that was begun is never the sweep's to settle.
        assert!(!sidecar_due(&of(SidecarKind::Other, 900 * DAY), now, 7));
        // Never with an entry or a held lease.
        assert!(!sidecar_due(&sidecar(None, true, false, 9 * DAY), now, 7));
        assert!(!sidecar_due(&sidecar(None, false, true, 9 * DAY), now, 7));
        assert!(!sidecar_due(
            &sidecar(Some(-9 * DAY), true, false, 9 * DAY),
            now,
            7
        ));
        assert!(!sidecar_due(
            &sidecar(Some(-9 * DAY), false, true, 9 * DAY),
            now,
            7
        ));
    }

    fn file(name: &str, age: i64) -> FileInfo {
        FileInfo {
            name: name.into(),
            modified: t(-age),
        }
    }

    #[test]
    fn a_temp_name_says_whose_it_is() {
        assert_eq!(temp_owner(".ttc-tmp.123.AbC"), Some(TempOwner::Pid(123)));
        assert_eq!(temp_owner(".ttc-tmp.0.x"), Some(TempOwner::Pid(0)));
        assert_eq!(temp_owner(".ttc-tmp.x.AbC"), Some(TempOwner::Unknown));
        assert_eq!(temp_owner(".ttc-tmp.123"), Some(TempOwner::Unknown));
        assert_eq!(temp_owner(".ttc-tmp.-5.x"), Some(TempOwner::Unknown));
        assert_eq!(temp_owner(".ttc-tmp."), Some(TempOwner::Unknown));
        assert_eq!(temp_owner(".tmpAbC123"), Some(TempOwner::Legacy));
        assert_eq!(temp_owner(".tmp"), None);
        assert_eq!(temp_owner(".tmp.x"), None);
        assert_eq!(temp_owner("tmpAbC123"), None);
        assert_eq!(temp_owner("ledger.json"), None);
    }

    #[test]
    fn temp_files_go_at_the_age_limit_when_their_writer_is_gone_and_other_names_stay() {
        let now = t(0);
        let alive = |pid: u32| pid == 7;
        let files = [
            file(".ttc-tmp.8.gone", STRAY_AFTER_SECS),
            file(".ttc-tmp.8.young", STRAY_AFTER_SECS - 1),
            file(".ttc-tmp.7.stopped", 10 * DAY),
            file(".ttc-tmp.x.unknown", 10 * DAY),
            file(".tmpAbC123", STRAY_AFTER_SECS),
            file(".tmpYoung1", STRAY_AFTER_SECS - 1),
            file("ledger.json", 10 * DAY),
            file("tmp-not-ours", 10 * DAY),
        ];
        assert_eq!(
            stale_temp_files(&files, &alive, true, now),
            vec![".ttc-tmp.8.gone", ".tmpAbC123"]
        );
        // Where the directory is not ours, only names of this version go.
        assert_eq!(
            stale_temp_files(&files, &alive, false, now),
            vec![".ttc-tmp.8.gone"]
        );
    }

    #[test]
    fn a_pid_that_must_not_be_signalled_counts_as_alive() {
        assert!(pid_alive(0));
        assert!(pid_alive(1));
        assert!(pid_alive(u32::MAX));
        assert!(pid_alive(i32::MAX as u32 + 1));
        assert!(pid_alive(std::process::id()));
        // A child that has exited and been reaped is gone.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        assert!(!pid_alive(pid));
    }

    #[test]
    fn job_files_without_a_readable_descriptor_go_unless_young_live_or_unknown() {
        let now = t(0);
        let readable: HashSet<String> = ["kept".to_string()].into();
        let live = |id: &str| id == "alive";
        let files = [
            file("kept.log", 9 * DAY),
            file("kept.json", 9 * DAY),
            file("gone.log", STRAY_AFTER_SECS),
            file("gone.exit", STRAY_AFTER_SECS),
            file("gone.waiter", STRAY_AFTER_SECS),
            // A descriptor that does not load is not a descriptor.
            file("damaged.json", STRAY_AFTER_SECS),
            file("damaged.log", 9 * DAY),
            // One young file keeps the whole id.
            file("young.log", 9 * DAY),
            file("young.json", STRAY_AFTER_SECS - 1),
            file("alive.log", 9 * DAY),
            file("notes.txt", 9 * DAY),
            file("README", 9 * DAY),
            file(".log", 9 * DAY),
        ];
        assert_eq!(
            orphan_job_ids(&files, &readable, &live, now),
            vec!["damaged", "gone"]
        );
    }
}
