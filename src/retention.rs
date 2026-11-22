//! What the tool keeps on disk, for how long, and what removes it.
//!
//! One sweep applies every rule; `task-traffic-controller gc` runs it on
//! demand, and [`sweep_if_due`] runs it by itself, at most once every
//! [`SWEEP_EVERY_SECS`], from the commands that write state (`exec`,
//! `submit`, [`crate::exec::run_gated`], [`crate::managed::register`]). A
//! machine where nobody runs `gc` stays bounded.
//!
//! | what | kept | removed by |
//! |---|---|---|
//! | `ledger.json` | live entries | dead entries are reaped on the fly and by the sweep |
//! | `leases/<id>.lock` | while the entry lives | the end of the command; the sweep, when no entry has it |
//! | `history.jsonl` | newest `history_keep` records per key, and only keys seen within `history_keep_days` | sweep |
//! | `measurements/*.applied` | while the history keeps the record | sweep |
//! | `measurements/*.pending` | none (a dead attempt's) | sweep |
//! | `managed/<id>.json` | `managed_keep_days` after the command ended | sweep |
//! | `jobs/<id>.*` | `jobs_keep_days` after the job finished or was found dead | sweep |
//! | `jobs/<id>.log` | at most `jobs_log_max_mib` once the job is over | the end of the job; sweep for a dead one |
//! | `jobs/*` without a descriptor, `.tmp*` files | an hour | sweep |
//! | `ledger.lock`, `history.lock`, `last-sweep`, `ledger.json` | fixed: read by every command | never |
//!
//! Nothing here removes what a live process may use: a lease that is held,
//! a ledger entry, the history while its lock is held. A sweep never fails
//! the command that triggered it.

use crate::config::Config;
use crate::error::Result;
use crate::jobs::{self, Job};
use crate::ledger::Ledger;
use crate::{lock, managed, Context};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashSet;
use std::fmt;
use std::path::Path;

/// The automatic sweep runs at most this often (per state directory).
pub const SWEEP_EVERY_SECS: i64 = 6 * 3600;

/// A job with no exit file, no ledger entry and no lease older than this is
/// dead (its runner was killed). Far above the few seconds a runner needs
/// to register.
pub const DEAD_JOB_AFTER_SECS: i64 = 3600;

/// A leftover temporary file, or a job file without its descriptor, must be
/// this old before the sweep removes it (an atomic write takes
/// milliseconds; this is a margin for a stalled machine).
pub const STRAY_AFTER_SECS: i64 = 3600;

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

/// Whether the automatic sweep is due, given when it last ran. A stamp in
/// the future (the clock went back) counts as due.
pub fn due(last: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    last > now || now - last >= Duration::seconds(SWEEP_EVERY_SECS)
}

fn read_stamp(ctx: &Context) -> Option<DateTime<Utc>> {
    let text = std::fs::read_to_string(ctx.paths.sweep_stamp()).ok()?;
    DateTime::parse_from_rfc3339(text.trim())
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn write_stamp(ctx: &Context) {
    let path = ctx.paths.sweep_stamp();
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(path, ctx.clock.now().to_rfc3339());
}

/// Run the sweep when it has not run for [`SWEEP_EVERY_SECS`]; `None` when
/// it is not due. The first call in a state directory only starts the
/// clock. Failed steps are in [`Report::errors`]; nothing is printed and
/// nothing fails the caller, so an embedder may ignore the result.
///
/// The library calls this itself from [`crate::exec::run_gated`] and
/// [`crate::managed::register`]. A program that writes state by other
/// means (only [`crate::managed::record_measurement`], say) calls it from
/// its own entry points.
pub fn sweep_if_due(ctx: &Context, cfg: &Config) -> Option<Report> {
    match read_stamp(ctx) {
        None => {
            write_stamp(ctx);
            None
        }
        Some(last) if due(last, ctx.clock.now()) => Some(sweep(ctx, cfg)),
        Some(_) => None,
    }
}

/// Apply every retention rule now (what `gc` does). Every step runs even
/// when an earlier one failed.
pub fn sweep(ctx: &Context, cfg: &Config) -> Report {
    // First, so that a second process arriving meanwhile finds it fresh.
    write_stamp(ctx);
    let mut r = Report::default();
    let mut errors = Vec::new();

    lock::refresh_verdicts(ctx);
    let ledger: Option<Ledger> = ok(&mut errors, "ledger", reap(ctx)).map(|(ledger, reaped)| {
        r.reaped = reaped;
        ledger
    });

    let cutoff = ctx.clock.now() - Duration::days(cfg.history_keep_days as i64);
    r.history_dropped = ok(
        &mut errors,
        "history",
        managed::trim_history_aged(ctx, cfg.history_keep, Some(cutoff)),
    )
    .unwrap_or(0);
    if let Some(ledger) = &ledger {
        if let Some((removed, dead, cut)) = ok(&mut errors, "jobs", sweep_jobs(ctx, cfg, ledger)) {
            r.jobs_removed = removed;
            r.jobs_found_dead = dead;
            r.logs_cut = cut;
        }
        r.strays_removed = ok(&mut errors, "stray files", sweep_strays(ctx, ledger)).unwrap_or(0);
    }
    r.markers_removed = ok(&mut errors, "measurements", managed::gc_measurements(ctx)).unwrap_or(0);
    r.receipts_removed = ok(
        &mut errors,
        "managed receipts",
        managed::gc_sidecars(ctx, cfg.managed_keep_days),
    )
    .unwrap_or(0);
    r.errors = errors;
    r
}

fn ok<T>(errors: &mut Vec<String>, name: &str, res: Result<T>) -> Option<T> {
    match res {
        Ok(v) => Some(v),
        Err(e) => {
            errors.push(format!("{name}: {e}"));
            None
        }
    }
}

/// Reap dead ledger entries and orphan leases under the ledger lock.
/// Returns the ledger as it is now and how many entries were reaped.
fn reap(ctx: &Context) -> Result<(Ledger, usize)> {
    let _guard = lock::ledger(ctx)?;
    let mut ledger = Ledger::load(&ctx.paths.ledger_file())?;
    let dead = lock::reap_dead(ctx, &mut ledger);
    lock::sweep_orphan_leases(ctx, &ledger);
    if !dead.is_empty() {
        ledger.save(&ctx.paths.ledger_file())?;
    }
    Ok((ledger, dead.len()))
}

// ---------------------------------------------------------------- jobs

/// What the sweep knows about one job.
#[derive(Debug, Clone, PartialEq)]
pub struct JobView {
    pub finished_at: Option<DateTime<Utc>>,
    pub submitted_at: DateTime<Utc>,
    /// The runner's exit file exists, and when it was written.
    pub exit_written: Option<DateTime<Utc>>,
    /// Its runner may still be running: it has a ledger entry or holds its
    /// lease. This is the liveness test, not a guess from a pid.
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
        Some(t) if t < now - Duration::days(keep_days as i64) => JobAction::Remove,
        Some(_) => JobAction::Keep,
        None => match v.exit_written {
            Some(t) => JobAction::MarkFinished(t),
            None if now - v.submitted_at >= Duration::seconds(DEAD_JOB_AFTER_SECS) => {
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
    now - f.modified >= Duration::seconds(STRAY_AFTER_SECS)
}

/// Temporary files of an interrupted atomic write (`tempfile`'s `.tmp`
/// prefix), old enough that no write can still be using them.
pub fn stale_temp_files(files: &[FileInfo], now: DateTime<Utc>) -> Vec<String> {
    files
        .iter()
        .filter(|f| f.name.starts_with(".tmp") && old_enough(f, now))
        .map(|f| f.name.clone())
        .collect()
}

/// Files of a job whose descriptor is gone (a crash while removing it, or a
/// `wait` that took the waiter lock meanwhile): `<id>.log`, `.exit` and
/// `.waiter` with no `<id>.json`, old enough, and not of a live job. Any
/// other name is kept.
pub fn orphan_job_files(
    files: &[FileInfo],
    ids: &HashSet<String>,
    live: &dyn Fn(&str) -> bool,
    now: DateTime<Utc>,
) -> Vec<String> {
    files
        .iter()
        .filter(|f| {
            let Some((id, ext)) = f.name.rsplit_once('.') else {
                return false;
            };
            matches!(ext, "log" | "exit" | "waiter")
                && !id.is_empty()
                && !ids.contains(id)
                && old_enough(f, now)
                && !live(id)
        })
        .map(|f| f.name.clone())
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
    ledger.find(id).is_some() || lock::lease_is_held(ctx, id)
}

/// Apply [`job_action`] to every job, and cut the logs of the jobs that are
/// over. Returns (jobs removed, jobs found dead, logs cut).
fn sweep_jobs(ctx: &Context, cfg: &Config, ledger: &Ledger) -> Result<(usize, usize, usize)> {
    let now = ctx.clock.now();
    let max = cfg.jobs_log_max_mib * 1024 * 1024;
    let (mut removed, mut dead, mut cut) = (0, 0, 0);
    for job in jobs::list(ctx)? {
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
        match job_action(&view, now, cfg.jobs_keep_days) {
            JobAction::Remove => {
                jobs::remove_files(ctx, &job.id);
                removed += 1;
                continue;
            }
            JobAction::MarkFinished(t) => {
                mark_finished(ctx, job.clone(), t)?;
                dead += 1;
            }
            JobAction::Keep => {}
        }
        if !live && jobs::cap_log(ctx, &job.id, max)? {
            cut += 1;
        }
    }
    Ok((removed, dead, cut))
}

fn mark_finished(ctx: &Context, mut job: Job, at: DateTime<Utc>) -> Result<()> {
    job.finished_at = Some(at);
    jobs::save(ctx, &job)
}

/// Orphaned job files and leftover temporary files, in the state directory
/// and `jobs/` only (a history file kept elsewhere is not swept: its
/// directory is not ours).
fn sweep_strays(ctx: &Context, ledger: &Ledger) -> Result<usize> {
    let now = ctx.clock.now();
    let mut n = 0;
    for dir in [ctx.paths.state_dir().to_path_buf(), ctx.paths.jobs_dir()] {
        for name in stale_temp_files(&list_files(&dir), now) {
            if std::fs::remove_file(dir.join(name)).is_ok() {
                n += 1;
            }
        }
    }
    let ids: HashSet<String> = jobs::list(ctx)?.into_iter().map(|j| j.id).collect();
    // A descriptor that cannot be read is still a descriptor: it keeps the
    // files of its job.
    let named: HashSet<String> = list_files(&ctx.paths.jobs_dir())
        .into_iter()
        .filter_map(|f| f.name.strip_suffix(".json").map(str::to_string))
        .chain(ids)
        .collect();
    let live = |id: &str| job_is_live(ctx, ledger, id) || jobs::has_waiter(ctx, id);
    let dir = ctx.paths.jobs_dir();
    for name in orphan_job_files(&list_files(&dir), &named, &live, now) {
        if std::fs::remove_file(dir.join(name)).is_ok() {
            n += 1;
        }
    }
    // Directories that hold nothing. Every creator retries when the
    // directory vanishes between its `create_dir_all` and its open.
    let _ = std::fs::remove_dir(&dir);
    let _ = std::fs::remove_dir(ctx.paths.leases_dir());
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn the_sweep_is_due_exactly_after_the_interval_or_from_a_future_stamp() {
        let now = t(0);
        assert!(!due(t(-SWEEP_EVERY_SECS + 1), now));
        assert!(due(t(-SWEEP_EVERY_SECS), now));
        assert!(!due(now, now));
        assert!(due(t(1), now));
    }

    fn view(finished: Option<i64>, submitted: i64, exit: Option<i64>, live: bool) -> JobView {
        JobView {
            finished_at: finished.map(t),
            submitted_at: t(submitted),
            exit_written: exit.map(t),
            live,
        }
    }

    const DAY: i64 = 86_400;

    #[test]
    fn a_finished_job_goes_after_keep_days_and_not_before() {
        let now = t(0);
        let keep = |fin| job_action(&view(Some(fin), -10 * DAY, Some(fin), false), now, 7);
        assert_eq!(keep(-7 * DAY), JobAction::Keep);
        assert_eq!(keep(-7 * DAY - 1), JobAction::Remove);
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

    fn file(name: &str, age: i64) -> FileInfo {
        FileInfo {
            name: name.into(),
            modified: t(-age),
        }
    }

    #[test]
    fn temp_files_go_at_the_age_limit_and_other_names_stay() {
        let now = t(0);
        let files = [
            file(".tmpAbC123", STRAY_AFTER_SECS),
            file(".tmpYoung", STRAY_AFTER_SECS - 1),
            file("ledger.json", 10 * DAY),
            file("tmp-not-ours", 10 * DAY),
        ];
        assert_eq!(stale_temp_files(&files, now), vec![".tmpAbC123"]);
    }

    #[test]
    fn job_files_without_a_descriptor_go_unless_young_live_or_unknown() {
        let now = t(0);
        let ids: HashSet<String> = ["kept".to_string()].into();
        let live = |id: &str| id == "alive";
        let files = [
            file("kept.log", 9 * DAY),
            file("gone.log", STRAY_AFTER_SECS),
            file("gone.exit", STRAY_AFTER_SECS),
            file("gone.waiter", STRAY_AFTER_SECS),
            file("young.log", STRAY_AFTER_SECS - 1),
            file("alive.log", 9 * DAY),
            file("gone.json", 9 * DAY),
            file("notes.txt", 9 * DAY),
            file("README", 9 * DAY),
            file(".log", 9 * DAY),
        ];
        assert_eq!(
            orphan_job_files(&files, &ids, &live, now),
            vec!["gone.log", "gone.exit", "gone.waiter"]
        );
    }
}
