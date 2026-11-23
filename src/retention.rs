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
//! | `managed/<id>.json` | `managed_keep_days` after the command ended, or after it was last written when a launch was begun and it has no entry; an hour when it was never launched and has no entry, or is not JSON | sweep |
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
//! [`AUTO_MAX_CUTS`] log cuts a time, takes at most [`SWEEP_BUDGET`], ends
//! at its next item when a stop is asked for, and never fails the command
//! that triggered it, or keeps it from returning.
//!
//! The numbers it goes by are the larger of the caller's and those of the
//! configuration file of the state directory ([`Keep::at_least`]).

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
use std::time::Instant;

/// The automatic sweep runs at most this often (per state directory).
pub const SWEEP_EVERY_SECS: i64 = 6 * 3600;

/// A sweep that left something undone (a lock was busy, it reached
/// [`AUTO_MAX_ACTIONS`] or [`SWEEP_BUDGET`], or it was stopped) is tried
/// again this long after.
pub const RETRY_AFTER_SECS: i64 = 600;

/// The most files one automatic sweep removes, marks or cuts. What is left
/// is done by the next one, [`RETRY_AFTER_SECS`] later.
pub const AUTO_MAX_ACTIONS: usize = 500;

/// The most job logs one automatic sweep cuts: a cut copies the part it
/// keeps, up to `jobs_log_max_mib`.
pub const AUTO_MAX_CUTS: usize = 4;

/// How long one automatic sweep may take, by the wall clock
/// ([`Context::sweep_budget`]). The sweep runs in the process whose command
/// just ended, so it is what the caller waits for after its result is
/// known: a fifth of a second is not noticed after a command worth
/// queueing, and is some thousand small files. It is split in
/// [`SWEEP_SLICES`] equal parts ([`slice_of`]) and looked at between two
/// items (a file looked at or removed, one log cut, one rewrite of the
/// history, one listing of a directory), so each part ends at most one
/// item late. `gc` has no budget.
pub const SWEEP_BUDGET: std::time::Duration = std::time::Duration::from_millis(200);

/// The parts the budget is split in: each of the four stages (ledger and
/// receipts, history, jobs, stray files) has one for looking and one for
/// removing.
pub const SWEEP_SLICES: u32 = 8;

/// The time one stage of the sweep has for looking, and again for
/// removing, out of `budget`. The two are counted from the stage's first
/// look and from its first removal, so a stage that has more to look at
/// than its time allows still removes what it found, and the stages after
/// it still run: every sweep gets something done, however large a
/// directory is, and the oldest files are looked at first.
pub fn slice_of(budget: std::time::Duration) -> std::time::Duration {
    budget / SWEEP_SLICES
}

/// A job with no exit file, no ledger entry and no lease older than this is
/// dead (its runner was killed). Far above the few seconds a runner needs
/// to register.
pub const DEAD_JOB_AFTER_SECS: i64 = 3600;

/// A leftover temporary file, a job file without a readable descriptor, or
/// a managed sidecar that never got its ledger entry must be this old
/// before the sweep removes it (an atomic write takes milliseconds; this is
/// a margin for a stalled machine).
pub const STRAY_AFTER_SECS: i64 = 3600;

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

    /// The numbers a sweep goes by: for each one the larger of the
    /// caller's own (`self`) and the one in the configuration file of the
    /// state directory (`file`; the defaults when there is no such file or
    /// it is not valid).
    ///
    /// Programs with different configurations may share a state directory
    /// (the `task-traffic-controller` command and a program that embeds the
    /// library), and neither may remove what the other still wants. The
    /// file is what the command goes by, so every sweep reads it; nothing
    /// is recorded, and a number changed in the file counts from the next
    /// sweep on.
    pub fn at_least(self, file: &Keep) -> Keep {
        Keep {
            history_keep: self.history_keep.max(file.history_keep),
            history_keep_days: self.history_keep_days.max(file.history_keep_days),
            jobs_keep_days: self.jobs_keep_days.max(file.jobs_keep_days),
            jobs_log_max_mib: self.jobs_log_max_mib.max(file.jobs_log_max_mib),
            managed_keep_days: self.managed_keep_days.max(file.managed_keep_days),
        }
    }

    /// `jobs_log_max_mib` in bytes.
    pub fn jobs_log_max_bytes(&self) -> u64 {
        self.jobs_log_max_mib.saturating_mul(1024 * 1024)
    }
}

/// Said once in a process: the configuration file cannot be used.
static CONFIG_SAID: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The retention numbers of the configuration file of this state directory
/// ([`crate::Paths::config_file`]: the file the `task-traffic-controller`
/// command reads). No file: the defaults. A file that cannot be read or is
/// not valid: the defaults too, and that is noted, once in a process.
fn file_keep(ctx: &Context, errors: &mut Errors) -> Keep {
    let path = ctx.paths.config_file();
    match Config::load(path) {
        Ok(cfg) => Keep::of(&cfg),
        Err(e) => {
            if !CONFIG_SAID.swap(true, std::sync::atomic::Ordering::SeqCst) {
                errors.note(format_args!(
                    "{}: {e}; the cleanup goes by the defaults where they keep more than \
                     the numbers of this program",
                    path.display()
                ));
            }
            Keep::of(&Config::default())
        }
    }
}

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
    /// the sweep reached [`AUTO_MAX_ACTIONS`] or its time budget, or a stop
    /// was asked for.
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

/// The stamp file (`last-sweep`): when the sweep last finished and when it
/// was last tried. Only what the throttle needs.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stamp {
    /// When a sweep last did everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complete: Option<DateTime<Utc>>,
    /// When a sweep was last started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<DateTime<Utc>>,
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

/// Written whole or not at all: a temporary file beside it, then a rename,
/// so no reader ever finds half a stamp. (The file the name points to
/// changes with every write; see [`sweep_if_due`] for why the lock on it
/// still lets only one process sweep.)
fn write_stamp(path: &Path, stamp: &Stamp) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;
    let mut tmp = crate::paths::temp_file_in(dir)?;
    tmp.write_all(
        serde_json::to_string(stamp)
            .map_err(std::io::Error::other)?
            .as_bytes(),
    )?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

/// Run the sweep when it is due ([`due`]); `None` when it is not, or when
/// another process is sweeping. Failed steps are in [`Report::errors`];
/// nothing is printed and nothing fails the caller, so an embedder may
/// ignore the result.
///
/// The library calls this itself at the end of [`crate::exec::run_gated`]
/// and of [`crate::managed::finish`]: after the command, never before it,
/// and only when the command ended by itself. A program that writes state
/// by other means (only [`crate::managed::record_measurement`], say) calls
/// it from its own entry points, after its work.
///
/// It takes at most [`Context::sweep_budget`] and ends early, at the next
/// item, when [`crate::Supervisor::stop_pending`] says a stop was asked
/// for; what is left is done [`RETRY_AFTER_SECS`] later.
///
/// One process sweeps at a time: the stamp is read again under a lock on
/// the stamp file, and the sweeper writes its start into the stamp before
/// it does anything. A process that locks the file an earlier write
/// replaced therefore still reads, by name, a stamp that says "not due".
pub fn sweep_if_due(ctx: &Context, cfg: &Config) -> Option<Report> {
    sweep_if_due_keep(ctx, &Keep::of(cfg), &|| false)
}

/// [`sweep_if_due`] with the caller's numbers and one more thing that can
/// ask for a stop.
pub(crate) fn sweep_if_due_keep(
    ctx: &Context,
    own: &Keep,
    stop: &dyn Fn() -> bool,
) -> Option<Report> {
    let stop = || stop() || ctx.supervisor.stop_pending();
    if stop() {
        return None;
    }
    let path = ctx.paths.sweep_stamp();
    let now = ctx.clock.now();
    match due(&read_stamp(&path), now) {
        Due::No => return None,
        Due::Arm => {
            let armed = Stamp {
                complete: Some(now),
                attempt: None,
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
    // Again, now that nobody else can be deciding: another process may
    // just have swept, or be sweeping with its lock on the file that was
    // here a moment ago.
    let before = match read_stamp(&path) {
        StampRead::Read(s) if due(&StampRead::Read(s.clone()), now) == Due::No => return None,
        StampRead::Read(s) => s,
        _ => Stamp::default(),
    };
    Some(run(ctx, own, before, Some(ctx.sweep_budget), &stop))
}

/// The sweep the library runs by itself after a command: when it is due,
/// within the budget, and never in the caller's way. `stop` is one more
/// thing that can ask for a stop (a caller's own cancellation). Each failed
/// step is given to `say` as a `task-traffic-controller: cleanup: ...`
/// line, and a line already said in this process is not said again. A
/// panic in the sweep ends here: it is said once and the caller goes on.
pub(crate) fn after_command(
    ctx: &Context,
    own: &Keep,
    stop: &dyn Fn() -> bool,
    say: &dyn Fn(&str),
) {
    let swept = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sweep_if_due_keep(ctx, own, stop)
    }));
    let lines = match swept {
        Ok(report) => report.map_or(Vec::new(), |r| r.errors),
        Err(_) => vec![
            "stopped by an internal error (see above); the command is not \
                        affected, and the cleanup is tried again later"
                .to_string(),
        ],
    };
    for line in lines {
        if first_time(&line) {
            // Whoever is told may fail too: that is not the caller's
            // business either.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                say(&format!("task-traffic-controller: cleanup: {line}"))
            }));
        }
    }
}

/// Whether `line` was not said before in this process (the first
/// [`SAID_MAX`] different lines are remembered; after that nothing new is
/// said).
fn first_time(line: &str) -> bool {
    static SAID: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
    let mut said = SAID.lock().unwrap_or_else(|p| p.into_inner());
    if said.len() >= SAID_MAX || said.iter().any(|s| s == line) {
        return false;
    }
    said.push(line.to_string());
    true
}

/// How many different cleanup lines a process says ([`after_command`]).
const SAID_MAX: usize = 32;

/// Apply every retention rule now (what `gc` does): the same rules as the
/// automatic sweep, but it waits for the locks, has no limit on how much it
/// removes or how long it takes, and scans processes to release managed
/// entries whose owner is gone. Every step runs even when an earlier one
/// failed.
///
/// It does not take the sweeper's lock (an automatic sweep at the same
/// time does no harm: every stage has its own lock); it writes the stamp
/// the same way, whole, so it cannot damage it.
pub fn sweep(ctx: &Context, cfg: &Config) -> Report {
    let before = match read_stamp(&ctx.paths.sweep_stamp()) {
        StampRead::Read(s) => s,
        _ => Stamp::default(),
    };
    run(ctx, &Keep::of(cfg), before, None, &|| false)
}

/// Whether the time is up: `end` is when (`None`: never).
pub fn time_is_up(end: Option<Instant>, now: Instant) -> bool {
    end.is_some_and(|end| now >= end)
}

/// What is left of a sweep's allowance.
struct Allowance<'a> {
    left: usize,
    /// Log cuts left: each copies up to `jobs_log_max_mib`.
    cuts: usize,
    /// How long a stage may look, and how long it may remove
    /// ([`slice_of`]; `None`: no budget).
    slice: Option<std::time::Duration>,
    /// When the stage that is running must stop looking, once it has
    /// begun to.
    look: Option<Instant>,
    /// When it must stop removing, once it has begun to.
    act: Option<Instant>,
    stop: &'a dyn Fn() -> bool,
    /// Something was left undone.
    short: bool,
}

impl Allowance<'_> {
    /// A stage begins: its time for looking starts with its first look,
    /// its time for removing with its first removal.
    fn begin(&mut self) {
        self.look = None;
        self.act = None;
    }

    /// Whether there is time left until `end`, which is set at the first
    /// call, and nobody asked for a stop. Notes when not.
    fn within(&mut self, looking: bool) -> bool {
        let now = Instant::now();
        let slice = self.slice;
        let end = if looking {
            &mut self.look
        } else {
            &mut self.act
        };
        if end.is_none() {
            *end = slice.map(|s| now + s);
        }
        if time_is_up(*end, now) || (self.stop)() {
            self.short = true;
            return false;
        }
        true
    }

    /// Whether the sweep may look at its next item.
    fn go(&mut self) -> bool {
        self.within(true)
    }

    /// Whether one more action may be done; notes when it may not.
    fn take(&mut self) -> bool {
        if !self.within(false) {
            return false;
        }
        if self.left == 0 {
            self.short = true;
            return false;
        }
        self.left -= 1;
        true
    }

    fn take_cut(&mut self) -> bool {
        if !self.within(false) {
            return false;
        }
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

/// The sweep. `budget`: `None` is `gc` (wait for the locks, no limits).
fn run(
    ctx: &Context,
    own: &Keep,
    before: Stamp,
    budget: Option<std::time::Duration>,
    stop: &dyn Fn() -> bool,
) -> Report {
    let auto = budget.is_some();
    let now = ctx.clock.now();
    let path = ctx.paths.sweep_stamp();
    // Before the work, so that a process arriving meanwhile stays away.
    let mut stamp = Stamp {
        complete: before.complete,
        attempt: Some(now),
    };
    let mut stamp_error = write_stamp(&path, &stamp).err();

    let mut r = Report::default();
    let mut errors = Errors {
        shown: Vec::new(),
        more: 0,
    };
    let keep = own.at_least(&file_keep(ctx, &mut errors));
    let mut allow = Allowance {
        left: if auto { AUTO_MAX_ACTIONS } else { usize::MAX },
        cuts: if auto { AUTO_MAX_CUTS } else { usize::MAX },
        slice: budget.map(slice_of),
        look: None,
        act: None,
        stop,
        short: false,
    };

    // Stage 1: dead ledger entries, leases nobody has, managed receipts.
    // What may go is found first, without any lock (the listing, the reads
    // and the parsing); the ledger lock is then held only to look at each
    // of those again and remove it, which the allowance bounds.
    allow.begin();
    let leases = lock::old_lease_ids(ctx, &mut || true);
    let sidecars = managed::sidecar_candidates(ctx, keep.managed_keep_days, &mut || allow.go());
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
                lock::remove_orphan_leases(ctx, &l, &leases, &mut || allow.take());
                r.receipts_removed = managed::remove_sidecars_locked(
                    ctx,
                    &l,
                    &sidecars,
                    keep.managed_keep_days,
                    &mut || allow.take(),
                );
                ledger = Some(l);
            }
        }
    }
    // The jobs and the stray files only need to know which ids have an
    // entry. With the lock busy the ledger is read as it is (it is replaced
    // whole, never written in place): an entry added since is a command
    // that holds its lease, which is asked before anything of it goes.
    let ledger = ledger.or_else(|| errors.ok("ledger", Ledger::load(&ctx.paths.ledger_file())));

    // Stage 2: the history and the measurement markers, under the history
    // lock. One rewrite of the history is one item.
    allow.begin();
    if allow.go() {
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
                            // No cutoff when the number of days is beyond
                            // the calendar: no key is then too old.
                            days_before(now, keep.history_keep_days),
                        ),
                    )
                    .unwrap_or(0);
                if allow.go() {
                    r.markers_removed = errors
                        .ok("measurements", managed::gc_measurements_locked(ctx))
                        .unwrap_or(0);
                }
            }
        }
    }

    if let Some(ledger) = &ledger {
        allow.begin();
        let unreadable = sweep_jobs(ctx, &keep, ledger, &mut r, &mut errors, &mut allow);
        allow.begin();
        sweep_strays(ctx, ledger, &unreadable, &mut r, &mut errors, &mut allow);
    } else {
        allow.short = true;
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
    /// A launch was begun, or the file is another version's. With a
    /// ledger entry it is the reaper's or `resolve`'s to settle; without
    /// one nothing ever looks at it again.
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
/// launch was begun, or that another version wrote, goes when the file was
/// last written `managed_keep_days` ago: with an entry it is held for
/// `resolve`, but without one (the ledger was lost, or the release could
/// not be written down) nothing charges for it or settles it any more, and
/// the same time a finished one is remembered is given to it.
///
/// The caller asks for the lease last, and only for a sidecar that would
/// go with `lease_held` false: looking at a lease locks it for an instant,
/// which a released version that is taking it does not forgive.
pub fn sidecar_due(v: &SidecarView, now: DateTime<Utc>, keep_days: u64) -> bool {
    if v.in_ledger || v.lease_held {
        return false;
    }
    let kept_days = |t: DateTime<Utc>| days_before(now, keep_days).is_some_and(|cutoff| t < cutoff);
    match v.kind {
        SidecarKind::Ended(t) => kept_days(t),
        SidecarKind::NeverLaunched | SidecarKind::Unparseable => {
            age_at_least(v.modified, now, STRAY_AFTER_SECS)
        }
        SidecarKind::Other => kept_days(v.modified),
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
/// descriptor that was damaged. An id has no readable descriptor when no
/// `<id>.json` is among `files`, or when it is in `unreadable` (the ids
/// whose descriptor was found not to load; the others are the job rule's).
/// Whether a job is live is asked last. Files with any other name are not
/// looked at.
pub fn orphan_job_ids(
    files: &[FileInfo],
    unreadable: &HashSet<String>,
    live: &dyn Fn(&str) -> bool,
    now: DateTime<Utc>,
) -> Vec<String> {
    // Per id: every file is old, and there is a descriptor.
    let mut seen: BTreeMap<&str, (bool, bool)> = BTreeMap::new();
    for f in files {
        let Some((id, ext)) = f.name.rsplit_once('.') else {
            continue;
        };
        if id.is_empty() || !matches!(ext, "json" | "log" | "exit" | "waiter") {
            continue;
        }
        let e = seen.entry(id).or_insert((true, false));
        e.0 &= old_enough(f, now);
        e.1 |= ext == "json";
    }
    seen.into_iter()
        .filter(|(id, (old, described))| {
            *old && (!described || unreadable.contains(*id)) && !live(id)
        })
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

/// Whether somebody may still use job `id`. The ledger is looked at first
/// (it costs nothing) and the locks last: looking at a lease locks it for
/// an instant.
fn job_is_live(ctx: &Context, ledger: &Ledger, id: &str) -> bool {
    ledger.find(id).is_some() || jobs::has_waiter(ctx, id) || lock::lease_is_held(ctx, id)
}

/// Apply [`job_action`] to every job, and cut the logs of the jobs that are
/// over. A job that fails is noted and the next one is looked at. The
/// descriptors are read one by one, the one written longest ago first, so
/// a sweep that runs out of time has looked at the jobs most likely to go.
/// Returns the ids whose descriptor it found not to load.
fn sweep_jobs(
    ctx: &Context,
    keep: &Keep,
    ledger: &Ledger,
    r: &mut Report,
    errors: &mut Errors,
    allow: &mut Allowance,
) -> HashSet<String> {
    let now = ctx.clock.now();
    let max = keep.jobs_log_max_bytes();
    let mut unreadable = HashSet::new();
    let Some(ids) = errors.ok("jobs", jobs::ids_oldest_first(ctx)) else {
        return unreadable;
    };
    for id in ids {
        if !allow.go() {
            break;
        }
        let Ok(job) = jobs::load(ctx, &id) else {
            unreadable.insert(id);
            continue;
        };
        let exit_written = match job.finished_at {
            Some(_) => None,
            None => std::fs::metadata(ctx.paths.job_exit(&job.id))
                .and_then(|m| m.modified())
                .ok()
                .map(DateTime::<Utc>::from),
        };
        // What would be done if nobody used the job: whether somebody does
        // is asked last, and only when there is something to do.
        let view = JobView {
            finished_at: job.finished_at,
            submitted_at: job.submitted_at,
            exit_written,
            live: false,
        };
        let action = job_action(&view, now, keep.jobs_keep_days);
        let over = std::fs::metadata(ctx.paths.job_log(&job.id)).is_ok_and(|m| m.len() > max);
        if (action == JobAction::Keep && !over) || job_is_live(ctx, ledger, &job.id) {
            continue;
        }
        if action != JobAction::Keep && !allow.take() {
            break;
        }
        match action {
            JobAction::Remove => {
                // False when a `wait` took the job meanwhile: it stays.
                if jobs::remove_files(ctx, &id) {
                    r.jobs_removed += 1;
                }
                continue;
            }
            JobAction::MarkFinished(t) => match mark_finished(ctx, job, t) {
                Ok(()) => r.jobs_found_dead += 1,
                Err(e) => errors.note(format_args!("job {id}: {e}")),
            },
            JobAction::Keep => {}
        }
        if over && allow.take_cut() {
            // Nothing when a `wait` took the job meanwhile: it may be
            // reading the log.
            match jobs::cap_log_unwaited(ctx, &id, max) {
                Ok(Some(true)) => r.logs_cut += 1,
                Ok(_) => {}
                Err(e) => errors.note(format_args!("job {id}: log: {e}")),
            }
        }
    }
    unreadable
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
    unreadable: &HashSet<String>,
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
    let live = |id: &str| job_is_live(ctx, ledger, id);
    for id in orphan_job_ids(&list_files(&ctx.paths.jobs_dir()), unreadable, &live, now) {
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
    fn each_number_is_the_larger_of_the_callers_and_the_files() {
        let own = Keep {
            history_keep: 10,
            history_keep_days: 20,
            jobs_keep_days: 30,
            jobs_log_max_mib: 40,
            managed_keep_days: 50,
        };
        let file = Keep {
            history_keep: 11,
            history_keep_days: 19,
            jobs_keep_days: 31,
            jobs_log_max_mib: 39,
            managed_keep_days: 51,
        };
        let both = Keep {
            history_keep: 11,
            history_keep_days: 20,
            jobs_keep_days: 31,
            jobs_log_max_mib: 40,
            managed_keep_days: 51,
        };
        assert_eq!(own.at_least(&file), both);
        assert_eq!(file.at_least(&own), both);
        assert_eq!(own.at_least(&own), own);
    }

    #[test]
    fn the_time_is_up_at_the_end_and_never_without_one() {
        let now = Instant::now();
        let ms = std::time::Duration::from_millis;
        assert!(!time_is_up(None, now));
        assert!(!time_is_up(Some(now + ms(1)), now));
        assert!(time_is_up(Some(now), now));
        assert!(time_is_up(Some(now), now + ms(1)));
    }

    #[test]
    fn the_budget_is_split_in_eight_equal_parts() {
        let ms = std::time::Duration::from_millis;
        assert_eq!(slice_of(SWEEP_BUDGET), ms(25));
        assert_eq!(slice_of(ms(8)), ms(1));
        assert_eq!(slice_of(ms(0)), ms(0));
    }

    /// Looking and removing each have their own time in a stage, counted
    /// from the first look and the first removal: what was found can be
    /// removed even when looking took all its time.
    #[test]
    fn a_stage_that_ran_out_of_time_looking_still_removes() {
        let mut allow = Allowance {
            left: 2,
            cuts: 1,
            slice: Some(std::time::Duration::from_secs(3600)),
            look: None,
            act: None,
            stop: &|| false,
            short: false,
        };
        assert!(allow.go());
        // Its time for looking is over.
        allow.look = Some(Instant::now());
        assert!(!allow.go());
        assert!(allow.short);
        // Removing has not begun: it has its whole time, and its count.
        assert!(allow.take() && allow.take_cut() && allow.take());
        assert!(!allow.take() && !allow.take_cut());
        // The next stage starts afresh; a stop ends everything.
        allow.begin();
        assert!(allow.go());
        allow.left = 1;
        allow.cuts = 1;
        allow.act = Some(Instant::now());
        assert!(!allow.take() && !allow.take_cut());
        assert_eq!((allow.left, allow.cuts), (1, 1));
        allow.begin();
        allow.stop = &|| true;
        assert!(!allow.go() && !allow.take() && !allow.take_cut());
        // No budget: never out of time.
        allow.stop = &|| false;
        allow.slice = None;
        allow.begin();
        assert!(allow.go() && allow.take());
        assert_eq!((allow.look, allow.act), (None, None));
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
        // A launch that was begun, or another version's file: keep_days
        // after it was last written.
        assert!(!sidecar_due(&of(SidecarKind::Other, 7 * DAY), now, 7));
        assert!(sidecar_due(&of(SidecarKind::Other, 7 * DAY + 1), now, 7));
        for held in [(true, false), (false, true)] {
            let v = SidecarView {
                kind: SidecarKind::Other,
                ..sidecar(None, held.0, held.1, 900 * DAY)
            };
            assert!(!sidecar_due(&v, now, 7));
        }
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

    /// Looking at a lease locks it for an instant, which a released
    /// version that is just taking it takes for an owner. So a sweep looks
    /// at no lease at all unless age, ledger and exit record have already
    /// said that something goes.
    #[test]
    fn a_sweep_with_nothing_due_looks_at_no_lease() {
        use crate::{FakeClock, FixedBoot, FixedLive, Paths};
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(FakeClock::new(Utc::now()));
        let mut ctx = Context::new(Paths::new(dir.path().join("s"), dir.path().join("c.toml")))
            .with_live(Arc::new(FixedLive(None)))
            .with_boot(Arc::new(FixedBoot(Some("b".into()))))
            .with_clock(clock.clone());
        ctx.sweep_budget = std::time::Duration::from_secs(3600);
        let now = ctx.clock.now();
        let job = |id: &str, submitted: DateTime<Utc>, finished: Option<DateTime<Utc>>| {
            let mut v = serde_json::json!({
                "id": id, "argv": ["true"], "cwd": "/", "submitted_at": submitted,
                "front": false, "submitter_pid": 1,
            });
            if let Some(t) = finished {
                v["finished_at"] = serde_json::json!(t);
            }
            let j: Job = serde_json::from_value(v).unwrap();
            jobs::save(&ctx, &j).unwrap();
            std::fs::write(ctx.paths.job_log(id), "out\n").unwrap();
        };
        // A job that ended an hour ago, one that is running (young, no
        // exit), and their lease files, as a released version leaves them.
        job(
            "done",
            now - Duration::hours(2),
            Some(now - Duration::hours(1)),
        );
        job("young", now - Duration::minutes(5), None);
        std::fs::create_dir_all(ctx.paths.leases_dir()).unwrap();
        for id in ["done", "young", "fresh"] {
            std::fs::write(ctx.paths.lease_file(id), "").unwrap();
        }
        // A sidecar that is being written (not JSON yet, not an hour old).
        std::fs::create_dir_all(ctx.paths.managed_dir()).unwrap();
        std::fs::write(ctx.paths.managed_file("fresh"), "{").unwrap();
        std::fs::write(
            ctx.paths.sweep_stamp(),
            r#"{"complete":"2000-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        lock::LEASE_LOOKS.with(|n| n.set(0));
        let report = sweep_if_due(&ctx, &Config::default()).expect("due");
        assert!(report.complete, "{report:?}");
        assert_eq!(lock::LEASE_LOOKS.with(|n| n.get()), 0);
        assert_eq!(jobs::list(&ctx).unwrap().len(), 2);

        // Something is due (a job dead for more than an hour, a sidecar
        // nobody finished writing an hour ago): now, and only for those,
        // the lease is asked, and a held one keeps both.
        job("dead", now - Duration::hours(2), None);
        std::fs::write(ctx.paths.managed_file("cut"), "{").unwrap();
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(2 * 3600);
        std::fs::File::options()
            .write(true)
            .open(ctx.paths.managed_file("cut"))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let held = [
            lock::take_lease(&ctx, "dead").unwrap(),
            lock::take_lease(&ctx, "cut").unwrap(),
        ];
        lock::LEASE_LOOKS.with(|n| n.set(0));
        let report = sweep(&ctx, &Config::default());
        assert_eq!((report.jobs_found_dead, report.receipts_removed), (0, 0));
        assert_eq!(lock::LEASE_LOOKS.with(|n| n.get()), 2);
        drop(held);
        let report = sweep(&ctx, &Config::default());
        assert_eq!((report.jobs_found_dead, report.receipts_removed), (1, 1));
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
        let unreadable: HashSet<String> = ["damaged".to_string()].into();
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
            // A descriptor nobody found damaged is the job rule's.
            file("whole.json", 9 * DAY),
            file("notes.txt", 9 * DAY),
            file("README", 9 * DAY),
            file(".log", 9 * DAY),
        ];
        assert_eq!(
            orphan_job_ids(&files, &unreadable, &live, now),
            vec!["damaged", "gone"]
        );
    }
}
