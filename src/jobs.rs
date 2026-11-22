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

/// A temporary file in `jobs/`, creating the directory. The retention
/// sweep removes the directory when it is empty, so a directory that
/// vanishes between the two calls is made again.
fn jobs_tempfile(ctx: &Context) -> Result<tempfile::NamedTempFile> {
    let mut last = None;
    for _ in 0..3 {
        fs::create_dir_all(ctx.paths.jobs_dir())?;
        match tempfile::NamedTempFile::new_in(ctx.paths.jobs_dir()) {
            Ok(t) => return Ok(t),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => last = Some(e),
            Err(e) => return Err(e.into()),
        }
    }
    Err(last.expect("loop ran").into())
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

/// Remove every file of job `id`. The descriptor goes last: it is what
/// [`list`] (and so every later sweep) finds the job by, so a crash half way
/// leaves a job the next sweep removes, not orphans nothing looks at.
pub fn remove_files(ctx: &Context, id: &str) {
    for p in [
        ctx.paths.job_log(id),
        ctx.paths.job_exit(id),
        ctx.paths.job_waiter(id),
        ctx.paths.job_json(id),
    ] {
        let _ = fs::remove_file(p);
    }
}

/// Bytes kept free in a cut log for its first line.
const NOTE_ROOM: u64 = 160;

/// Remove the log and the waiter lock of job `id`: what is left once its
/// output has been handed over.
pub fn remove_output(ctx: &Context, id: &str) {
    let _ = fs::remove_file(ctx.paths.job_log(id));
    let _ = fs::remove_file(ctx.paths.job_waiter(id));
}

/// Cut the log of job `id` to its last `max_bytes` (from the start of a
/// line when one is near) behind a one-line note, when it is longer. Call it
/// only once nothing writes the log any more: the tail is copied to a new
/// file that replaces the log. Returns whether the log was cut.
pub fn cap_log(ctx: &Context, id: &str, max_bytes: u64) -> Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let path = ctx.paths.job_log(id);
    let mut f = match fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let len = f.metadata()?.len();
    if len <= max_bytes {
        return Ok(false);
    }
    // Room for the note, so the cut log is within the limit and a second
    // look leaves it alone.
    let keep = max_bytes - NOTE_ROOM.min(max_bytes / 2);
    f.seek(SeekFrom::Start(len - keep))?;
    // Start at the next line, unless the first 8 KiB has none.
    let mut head = vec![0u8; 8192.min(keep as usize)];
    let n = f.read(&mut head)?;
    let skip = head[..n]
        .iter()
        .position(|&b| b == b'\n')
        .map_or(0, |i| i as u64 + 1);
    let cut = len - keep + skip;
    f.seek(SeekFrom::Start(cut))?;
    let mut tmp = jobs_tempfile(ctx)?;
    writeln!(
        tmp,
        "[task-traffic-controller: the first {cut} bytes of this log were cut; it is kept to its last part]"
    )?;
    std::io::copy(&mut f, &mut tmp)?;
    tmp.persist(&path).map_err(|e| e.error)?;
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
