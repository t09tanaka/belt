//! Asynchronous jobs (spec "v0.2 addendum"): a detached runner per job,
//! a JSON descriptor, a merged log, and an exit file.

use crate::error::{Error, Result};
use crate::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Job {
    pub id: String,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub weight: Option<u32>,
    pub front: bool,
    pub key: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    pub submitted_at: DateTime<Utc>,
    pub submitter_pid: u32,
    /// Keep running even when nobody is waiting.
    #[serde(default)]
    pub detach: bool,
    /// Cancelled by the runner because its waiter went away.
    #[serde(default)]
    pub orphaned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Fields this binary does not know, kept so that rewriting the file
    /// does not drop what a newer binary wrote.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Running,
    Done(i32),
    Lost,
}

impl JobState {
    pub fn label(self) -> String {
        match self {
            JobState::Queued => "queued".into(),
            JobState::Running => "running".into(),
            JobState::Done(c) => format!("done {c}"),
            JobState::Lost => "lost".into(),
        }
    }
}

/// A temporary file in `jobs/`, creating the directory.
fn jobs_tempfile(ctx: &Context) -> Result<tempfile::NamedTempFile> {
    fs::create_dir_all(ctx.paths.jobs_dir())?;
    Ok(crate::paths::temp_file_in(&ctx.paths.jobs_dir())?)
}

pub fn save(ctx: &Context, job: &Job) -> Result<()> {
    let path = ctx.paths.job_json(&job.id);
    let mut tmp = jobs_tempfile(ctx)?;
    serde_json::to_writer_pretty(&mut tmp, job)?;
    tmp.write_all(b"\n")?;
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

pub fn load(ctx: &Context, id: &str) -> Result<Job> {
    let bytes =
        fs::read(ctx.paths.job_json(id)).map_err(|e| Error::Ledger(format!("job {id}: {e}")))?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// The ids that have a descriptor (`<id>.json`), the one written longest
/// ago first: a listing only, no descriptor is read.
pub fn ids_oldest_first(ctx: &Context) -> Result<Vec<String>> {
    let rd = match fs::read_dir(ctx.paths.jobs_dir()) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut found = Vec::new();
    for ent in rd.flatten() {
        let name = ent.file_name();
        let Some(id) = name.to_str().and_then(|n| n.strip_suffix(".json")) else {
            continue;
        };
        let Ok(modified) = ent.metadata().and_then(|m| m.modified()) else {
            continue;
        };
        found.push((modified, id.to_string()));
    }
    found.sort();
    Ok(found.into_iter().map(|(_, id)| id).collect())
}

/// All jobs, newest first. Unreadable descriptors are skipped.
pub fn list(ctx: &Context) -> Result<Vec<Job>> {
    let mut out = Vec::new();
    let rd = match fs::read_dir(ctx.paths.jobs_dir()) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for ent in rd.flatten() {
        let name = ent.file_name();
        let Some(id) = name.to_str().and_then(|n| n.strip_suffix(".json")) else {
            continue;
        };
        if let Ok(job) = load(ctx, id) {
            out.push(job);
        }
    }
    out.sort_by_key(|j| std::cmp::Reverse(j.submitted_at));
    Ok(out)
}

pub fn read_exit(ctx: &Context, id: &str) -> Option<i32> {
    fs::read_to_string(ctx.paths.job_exit(id))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// Persist the outcome. Written once; later writers lose (the first
/// outcome is the real one, e.g. the runner's over a late `cancel`).
pub fn write_exit(ctx: &Context, id: &str, code: i32) {
    let path = ctx.paths.job_exit(id);
    if path.exists() {
        return;
    }
    if let Ok(mut tmp) = jobs_tempfile(ctx) {
        let _ = writeln!(tmp, "{code}");
        let _ = tmp.persist(&path);
    }
    if let Ok(mut job) = load(ctx, id) {
        job.finished_at = Some(ctx.clock.now());
        let _ = save(ctx, &job);
    }
}

/// Spawn the detached runner for `job`: own session, stdin from
/// /dev/null, stdout+stderr appended to the job log, env inherited. The
/// runner is `exe __run-job <id>`; it rebuilds its own context from the
/// environment it inherits, so the caller's `exe` must resolve the same
/// state directory as `ctx`.
pub fn spawn_runner(
    ctx: &Context,
    exe: &std::path::Path,
    job: &Job,
) -> Result<std::process::Child> {
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(ctx.paths.job_log(&job.id))?;
    let log_err = log.try_clone()?;
    let mut cmd = Command::new(exe);
    cmd.arg("__run-job")
        .arg(&job.id)
        .current_dir(&job.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    // SAFETY: setsid is async-signal-safe and only detaches the child from
    // our session so a terminal hangup or our exit never reaches it.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    cmd.spawn()
        .map_err(|e| Error::Spawn(format!("cannot start job runner: {e}")))
}

/// Hold this while waiting on a job; the runner treats "nobody holds it"
/// as "nobody cares" once the grace period is over.
pub fn take_waiter(ctx: &Context, id: &str) -> Result<crate::lock::Lock> {
    crate::lock::take_shared(&ctx.paths.job_waiter(id))
}

pub fn has_waiter(ctx: &Context, id: &str) -> bool {
    crate::lock::is_held(&ctx.paths.job_waiter(id))
}

pub fn mark_orphaned(ctx: &Context, id: &str) {
    if let Ok(mut job) = load(ctx, id) {
        job.orphaned = true;
        let _ = save(ctx, &job);
    }
}

/// Remove every file of job `id`, unless a `wait` holds it: the files go
/// while this call holds the waiter lock exclusively (taken without
/// waiting), so no `wait` can be reading them or start to. False when a
/// waiter holds the job and nothing was removed. The descriptor goes last
/// of the job's own files: it is what [`list`] (and so every later sweep)
/// finds the job by, so a crash half way leaves a job the next sweep
/// removes, not orphans nothing looks at.
pub fn remove_files(ctx: &Context, id: &str) -> bool {
    if !ctx.paths.jobs_dir().is_dir() {
        return true;
    }
    let Ok(Some(_held)) = crate::lock::try_acquire(&ctx.paths.job_waiter(id)) else {
        return false;
    };
    discard_log(ctx, id);
    for p in [
        ctx.paths.job_exit(id),
        ctx.paths.job_json(id),
        ctx.paths.job_waiter(id),
    ] {
        let _ = fs::remove_file(p);
    }
    true
}

/// Bytes kept free in a cut log for its first line.
const NOTE_ROOM: u64 = 160;

/// Empty the log of job `id` and remove it. Emptied first, in place: a
/// process the job left behind may still hold the file open for appending,
/// and what it had written would otherwise stay on disk, invisible, until it
/// exits. What such a process writes after this is the only space the
/// removed file can still take.
fn discard_log(ctx: &Context, id: &str) {
    let path = ctx.paths.job_log(id);
    if let Ok(f) = fs::OpenOptions::new().write(true).open(&path) {
        let _ = f.set_len(0);
    }
    let _ = fs::remove_file(path);
}

/// Remove the log and the waiter lock of job `id`: what is left once its
/// output has been handed over. Nothing is removed, and false returned,
/// while another `wait` holds the job.
pub fn remove_output(ctx: &Context, id: &str) -> bool {
    if !crate::lock::remove_if_unheld(&ctx.paths.job_waiter(id)) {
        return false;
    }
    discard_log(ctx, id);
    true
}

/// One cut at a time in a process, and no cut by the runner's watcher once
/// the job has its exit record: the watcher looks for the record and cuts
/// under this lock ([`cap_running_log`]), and the end of the job makes its
/// last cut and writes the record under it ([`end`]). Whoever sees the exit
/// record therefore reads a log that its runner no longer cuts.
static CUTTING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Record how job `id` ended, as its runner: the log is cut to `max_bytes`
/// first, so that whoever sees the exit file reads the log in its final
/// size (a failure to cut leaves it whole), and no cut of the runner's can
/// follow the record. Nothing when the job already has its record.
pub fn end(ctx: &Context, id: &str, code: i32, max_bytes: u64) {
    let _one = CUTTING.lock().unwrap_or_else(|p| p.into_inner());
    if read_exit(ctx, id).is_some() {
        return;
    }
    let _ = cut_log_held(ctx, id, max_bytes, keep_bytes(max_bytes, false));
    write_exit(ctx, id, code);
}

/// [`cap_log`] by somebody who is not the job's runner, for a job that is
/// over: the cut is made while this call holds the waiter lock exclusively
/// (taken without waiting, as [`remove_files`] does), so no `wait`, of this
/// or a released version, is reading the log or starts to. `None` when a
/// `wait` holds the job; nothing was cut then.
pub fn cap_log_unwaited(ctx: &Context, id: &str, max_bytes: u64) -> Result<Option<bool>> {
    let Some(_held) = crate::lock::try_acquire(&ctx.paths.job_waiter(id))? else {
        return Ok(None);
    };
    cap_log(ctx, id, max_bytes).map(Some)
}

/// How much of a log longer than `max_bytes` stays after a cut. At the end
/// of the job as much as fits behind the note; while it runs half, so that
/// a job that keeps writing is cut once per `max_bytes / 2` written, not on
/// every look.
pub fn keep_bytes(max_bytes: u64, running: bool) -> u64 {
    if running {
        max_bytes / 2
    } else {
        max_bytes - NOTE_ROOM.min(max_bytes / 2)
    }
}

/// Cut the log of job `id` to its last part (from the start of a line when
/// one is near) behind a one-line note, when it is longer than `max_bytes`.
/// Returns whether the log was cut.
///
/// The file is cut in place: the part to keep is copied aside, the same
/// file is truncated and the part written back. Whoever holds the log open
/// (it is opened for appending) goes on writing at the new end, so the cut
/// is safe while the job runs and frees the space at once. What a writer
/// appends between the copy and the truncation is lost, and what it appends
/// before the part is written back comes ahead of it.
pub fn cap_log(ctx: &Context, id: &str, max_bytes: u64) -> Result<bool> {
    cut_log(ctx, id, max_bytes, keep_bytes(max_bytes, false))
}

/// [`cap_log`] for a job that is still writing ([`keep_bytes`]), by its
/// runner. Nothing is cut once the job has its exit record: a `wait` may be
/// reading the log from then on ([`end`]).
pub fn cap_running_log(ctx: &Context, id: &str, max_bytes: u64) -> Result<bool> {
    let _one = CUTTING.lock().unwrap_or_else(|p| p.into_inner());
    if read_exit(ctx, id).is_some() {
        return Ok(false);
    }
    cut_log_held(ctx, id, max_bytes, keep_bytes(max_bytes, true))
}

fn cut_log(ctx: &Context, id: &str, max_bytes: u64, keep: u64) -> Result<bool> {
    let _one = CUTTING.lock().unwrap_or_else(|p| p.into_inner());
    cut_log_held(ctx, id, max_bytes, keep)
}

/// [`cut_log`] for a caller that holds [`CUTTING`].
fn cut_log_held(ctx: &Context, id: &str, max_bytes: u64, keep: u64) -> Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let path = ctx.paths.job_log(id);
    let mut f = match fs::OpenOptions::new().read(true).append(true).open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let len = f.metadata()?.len();
    if len <= max_bytes {
        return Ok(false);
    }
    f.seek(SeekFrom::Start(len - keep))?;
    // Start at the next line, unless the first 8 KiB has none.
    let mut head = vec![0u8; 8192.min(keep as usize)];
    let n = f.read(&mut head)?;
    let skip = head[..n]
        .iter()
        .position(|&b| b == b'\n')
        .map_or(0, |i| i as u64 + 1);
    let cut = len - keep + skip;
    // The part to keep, in a file that has no name (nothing is left behind
    // if this process dies). When it cannot be copied (a full disk), the log
    // is emptied all the same: bounding it matters most then.
    let aside = (|| -> std::io::Result<fs::File> {
        let mut tail = tempfile::tempfile_in(ctx.paths.jobs_dir())?;
        f.seek(SeekFrom::Start(cut))?;
        std::io::copy(&mut (&f).take(len - cut), &mut tail)?;
        tail.seek(SeekFrom::Start(0))?;
        Ok(tail)
    })();
    f.set_len(0)?;
    match aside {
        Ok(mut tail) => {
            writeln!(
                f,
                "[task-traffic-controller: the first {cut} bytes of this log were cut; it is kept to its last part]"
            )?;
            std::io::copy(&mut tail, &mut f)?;
        }
        Err(e) => writeln!(
            f,
            "[task-traffic-controller: the first {len} bytes of this log were cut; its last part could not be kept: {e}]"
        )?,
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_keeps_fields_this_binary_does_not_know() {
        let v = serde_json::json!({
            "id": "j", "argv": ["x"], "cwd": "/", "weight": null, "front": false,
            "key": null, "submitted_at": "2026-01-01T00:00:00Z", "submitter_pid": 1,
            "future": {"a": [1, {"b": null}]},
        });
        let mut job: Job = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(job.extra.len(), 1);
        job.finished_at = None;
        job.orphaned = true;
        let back = serde_json::to_value(&job).unwrap();
        assert_eq!(back["future"], v["future"]);
        assert_eq!(back["orphaned"], true);
        assert_eq!(
            back.as_object()
                .unwrap()
                .keys()
                .filter(|k| *k == "id")
                .count(),
            1
        );
    }

    fn ctx_in(d: &std::path::Path) -> Context {
        Context::new(crate::Paths::new(d.join("s"), d.join("c.toml")))
    }

    #[test]
    fn cap_log_keeps_the_tail_from_a_line_start_and_says_what_it_cut() {
        let d = tempfile::tempdir().unwrap();
        let ctx = ctx_in(d.path());
        fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
        let log = ctx.paths.job_log("j");
        fs::write(&log, "aaaa\nbbbb\ncccc\n").unwrap();
        // Exactly at the limit: untouched.
        assert!(!cap_log(&ctx, "j", 15).unwrap());
        assert_eq!(fs::read_to_string(&log).unwrap(), "aaaa\nbbbb\ncccc\n");
        // One byte over: half of the limit is room for the note, the 7 bytes
        // kept start inside "bbbb", so the log begins at the next line.
        assert!(cap_log(&ctx, "j", 14).unwrap());
        let got = fs::read_to_string(&log).unwrap();
        assert!(got.ends_with("]\ncccc\n"), "{got}");
        assert!(
            got.starts_with("[task-traffic-controller: the first 10 bytes"),
            "{got}"
        );
        assert!(!got.contains("bbbb"));
        // A missing log is not an error.
        assert!(!cap_log(&ctx, "none", 1).unwrap());
    }

    #[test]
    fn a_running_log_is_cut_to_half_and_a_finished_one_to_what_fits() {
        assert_eq!(keep_bytes(1000, true), 500);
        assert_eq!(keep_bytes(1000, false), 1000 - NOTE_ROOM);
        assert_eq!(keep_bytes(100, false), 50);
        assert_eq!(keep_bytes(1, true), 0);
    }

    /// The cut is made in the file itself: a writer that opened the log
    /// before goes on at the new end, in the same file, and the space of
    /// what was cut is free.
    #[test]
    fn a_cut_keeps_the_file_so_a_writer_that_holds_it_continues_at_the_new_end() {
        use std::os::unix::fs::MetadataExt;
        let d = tempfile::tempdir().unwrap();
        let ctx = ctx_in(d.path());
        fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
        let log = ctx.paths.job_log("j");
        let mut writer = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let line = "0123456789abcde\n";
        for _ in 0..1000 {
            writer.write_all(line.as_bytes()).unwrap();
        }
        let before = fs::metadata(&log).unwrap().ino();
        assert!(cap_running_log(&ctx, "j", 4000).unwrap());
        writer.write_all(b"after the cut\n").unwrap();
        let meta = fs::metadata(&log).unwrap();
        assert_eq!(meta.ino(), before);
        assert!(meta.len() <= 2000 + NOTE_ROOM + 14, "{}", meta.len());
        let got = fs::read_to_string(&log).unwrap();
        assert!(
            got.starts_with("[task-traffic-controller: the first "),
            "{got}"
        );
        assert!(got.ends_with("abcde\nafter the cut\n"), "{got}");
        // Nothing else is left in the directory.
        assert_eq!(fs::read_dir(ctx.paths.jobs_dir()).unwrap().count(), 1);
        // Within the limit now: a second look leaves it alone.
        assert!(!cap_running_log(&ctx, "j", 4000).unwrap());
    }

    #[test]
    fn a_job_a_wait_holds_keeps_every_file() {
        let d = tempfile::tempdir().unwrap();
        let ctx = ctx_in(d.path());
        fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
        for p in [
            ctx.paths.job_log("j"),
            ctx.paths.job_exit("j"),
            ctx.paths.job_json("j"),
        ] {
            fs::write(p, "x").unwrap();
        }
        let waiter = take_waiter(&ctx, "j").unwrap();
        assert!(!remove_files(&ctx, "j"));
        assert!(!remove_output(&ctx, "j"));
        assert_eq!(fs::read_dir(ctx.paths.jobs_dir()).unwrap().count(), 4);
        drop(waiter);
        assert!(remove_output(&ctx, "j"));
        assert!(!ctx.paths.job_log("j").exists() && ctx.paths.job_exit("j").exists());
        assert!(remove_files(&ctx, "j"));
        assert_eq!(fs::read_dir(ctx.paths.jobs_dir()).unwrap().count(), 0);
    }

    /// Whoever sees the exit record reads a log its runner no longer
    /// cuts: the record is written with the last cut, and the watcher's
    /// cut looks for the record under the same lock.
    #[test]
    fn the_runner_cuts_nothing_once_the_exit_is_recorded() {
        let d = tempfile::tempdir().unwrap();
        let ctx = ctx_in(d.path());
        std::fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
        let log = ctx.paths.job_log("j");
        std::fs::write(&log, "line\n".repeat(2000)).unwrap();
        end(&ctx, "j", 3, 4000);
        assert_eq!(read_exit(&ctx, "j"), Some(3));
        // Cut to the limit by the end itself.
        let len = std::fs::metadata(&log).unwrap().len();
        assert!(len <= 4000, "{len}");
        // Something the job left behind writes on: not the runner's to cut
        // any more.
        std::fs::write(&log, "line\n".repeat(2000)).unwrap();
        assert!(!cap_running_log(&ctx, "j", 4000).unwrap());
        assert_eq!(std::fs::metadata(&log).unwrap().len(), 10_000);
        // And the first record stands.
        end(&ctx, "j", 9, 4000);
        assert_eq!(read_exit(&ctx, "j"), Some(3));
        assert_eq!(std::fs::metadata(&log).unwrap().len(), 10_000);
    }

    /// A cut by somebody else than the runner is made under the waiter
    /// lock, taken without waiting: never while a `wait` holds the job.
    #[test]
    fn a_job_a_wait_holds_is_not_cut_by_the_sweep() {
        let d = tempfile::tempdir().unwrap();
        let ctx = ctx_in(d.path());
        std::fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
        let log = ctx.paths.job_log("j");
        std::fs::write(&log, "line\n".repeat(2000)).unwrap();
        let waiter = take_waiter(&ctx, "j").unwrap();
        assert_eq!(cap_log_unwaited(&ctx, "j", 4000).unwrap(), None);
        assert_eq!(std::fs::metadata(&log).unwrap().len(), 10_000);
        drop(waiter);
        assert_eq!(cap_log_unwaited(&ctx, "j", 4000).unwrap(), Some(true));
        assert!(std::fs::metadata(&log).unwrap().len() <= 4000);
        assert_eq!(cap_log_unwaited(&ctx, "j", 4000).unwrap(), Some(false));
        // The lock is free again afterwards.
        assert!(!has_waiter(&ctx, "j"));
    }

    #[test]
    fn cap_log_without_a_newline_in_reach_keeps_the_raw_tail() {
        let d = tempfile::tempdir().unwrap();
        let ctx = ctx_in(d.path());
        fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
        let log = ctx.paths.job_log("j");
        fs::write(&log, "abcdefghij").unwrap();
        assert!(cap_log(&ctx, "j", 4).unwrap());
        let got = fs::read_to_string(&log).unwrap();
        assert!(got.ends_with("]\nij"), "{got}");
        assert_eq!(fs::read_dir(ctx.paths.jobs_dir()).unwrap().count(), 1);
    }
}
