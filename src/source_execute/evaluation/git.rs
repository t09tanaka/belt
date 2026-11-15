//! Read scheduling facts from a fixed Git commit, never from the checkout.
use super::{
    scheduling, EvaluationBinding, EvaluationSource, VerifiedEvaluationSource, MAX_SOURCE_BYTES,
    MAX_WORKFLOWS, VERSION,
};
use crate::{context::ExecContext, frozen::FrozenPlan};
use anyhow::{bail, ensure, Context, Result};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::process::CommandExt,
    path::Path,
    process::{Child, Stdio},
    time::{Duration, Instant},
};

const MAX_PATH: usize = 4096;
const MAX_STDERR: usize = 64 * 1024;
const REQUEST_TIME: Duration = Duration::from_secs(30);
const COMMAND_TIME: Duration = Duration::from_secs(5);

/// Recompute omitted scheduling facts from the plan's full, immutable Git
/// commit. Requires Git's `--no-lazy-fetch` option; older versions fail closed.
/// Reads at most 4MiB of workflow contents and 4096 regular workflow entries.
/// Git's object database and executable are the host's trusted Git boundary;
/// this is not a repository-wide object-database integrity scan.
/// No checkout, filter, fetch, credential prompt or worker execution is used.
pub fn git_evaluation_source(
    ctx: &ExecContext,
    repository: &Path,
    plan: &FrozenPlan,
) -> Result<VerifiedEvaluationSource> {
    plan.verify()?;
    plan.require_git_source()?;
    ensure!(
        plan.workflows.len() <= MAX_WORKFLOWS,
        "too many evaluation workflows"
    );
    let commit = plan
        .source
        .execution_commit
        .as_deref()
        .context("missing execution commit")?;
    ensure!(
        oid(commit),
        "evaluation source requires a full lowercase commit object ID"
    );
    ensure!(
        plan.source.source_id == format!("commit:{commit}"),
        "Git source identity mismatch"
    );
    let started = Instant::now();
    let format = command(
        ctx,
        repository,
        &["rev-parse", "--show-object-format"],
        16,
        started,
    )?;
    ensure!(
        matches!(
            (format.as_slice(), commit.len()),
            (b"sha1\n", 40) | (b"sha256\n", 64)
        ),
        "Git object format/commit mismatch"
    );
    ensure!(
        command(ctx, repository, &["cat-file", "-t", commit], 16, started)? == b"commit\n",
        "execution object is not a commit"
    );
    let mut workflows = Vec::new();
    let mut remaining = MAX_SOURCE_BYTES;
    for frozen in &plan.workflows {
        let path = &frozen.file;
        ensure!(
            !path.is_empty()
                && path.len() <= MAX_PATH
                && !path.bytes().any(|b| b == 0)
                && path.split('/').all(|c| !matches!(c, "" | "." | "..")),
            "noncanonical evaluation workflow path"
        );
        let entry = command(
            ctx,
            repository,
            &["ls-tree", "-l", "-z", "--full-tree", commit, "--", path],
            MAX_PATH + 256,
            started,
        )?;
        let (blob, size) = tree_entry(&entry, path, commit.len())?;
        ensure!(size <= remaining, "evaluation workflow exceeds byte bound");
        let bytes = command(
            ctx,
            repository,
            &["cat-file", "blob", blob],
            remaining,
            started,
        )?;
        ensure!(bytes.len() == size, "evaluation blob size mismatch");
        let object_id = command_input(
            ctx,
            repository,
            &["hash-object", "--no-filters", "--stdin"],
            128,
            started,
            Some(&bytes),
        )?;
        ensure!(
            object_id.strip_suffix(b"\n") == Some(blob.as_bytes()),
            "Git blob content/object ID mismatch"
        );
        remaining -= bytes.len();
        workflows.push(scheduling(
            frozen,
            &bytes,
            format!("{:x}", Sha256::digest(&bytes)),
        )?);
    }
    let record = EvaluationSource {
        schema_version: VERSION,
        plan_digest: plan.digest.clone(),
        source_id: plan.source.source_id.clone(),
        binding: EvaluationBinding::Git {
            execution_commit: commit.into(),
        },
        workflows,
    };
    ensure!(
        serde_json::to_vec(&record)?.len() <= MAX_SOURCE_BYTES,
        "evaluation source exceeds byte bound"
    );
    ensure!(!ctx.cancel.is_cancelled(), "Git evaluation cancelled");
    ensure!(
        started.elapsed() < REQUEST_TIME,
        "Git evaluation request deadline exceeded"
    );
    Ok(VerifiedEvaluationSource(record))
}

/// Decoding is never verification: recompute from the same repository and
/// frozen plan before accepting a transported scheduling record.
pub fn verify_git_evaluation_source(
    ctx: &ExecContext,
    repository: &Path,
    plan: &FrozenPlan,
    record: &EvaluationSource,
) -> Result<VerifiedEvaluationSource> {
    let verified = git_evaluation_source(ctx, repository, plan)?;
    ensure!(
        verified.record() == record,
        "evaluation source provenance mismatch"
    );
    Ok(verified)
}

fn oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn tree_entry<'a>(bytes: &'a [u8], path: &str, oid_len: usize) -> Result<(&'a str, usize)> {
    let line = bytes.strip_suffix(&[0]).context("missing Git tree entry")?;
    ensure!(!line.contains(&0), "multiple Git tree entries");
    let tab = line
        .iter()
        .position(|b| *b == b'\t')
        .context("invalid Git tree entry")?;
    ensure!(
        &line[tab + 1..] == path.as_bytes(),
        "Git tree path mismatch"
    );
    let fields: Vec<_> = std::str::from_utf8(&line[..tab])?
        .split_ascii_whitespace()
        .collect();
    ensure!(
        fields.len() == 4 && matches!(fields[0], "100644" | "100755") && fields[1] == "blob",
        "evaluation workflow is not a regular Git blob"
    );
    ensure!(
        oid(fields[2]) && fields[2].len() == oid_len,
        "invalid Git blob object ID"
    );
    Ok((
        fields[2],
        fields[3].parse().context("invalid Git blob size")?,
    ))
}

// The unreaped leader pins the group identity on every return path. Signal
// descendants before wait, including after the leader exits but holds no pipes.
// Cancellation is polled here rather than registering a reaped PID in ctx.
struct OwnedGroup(Child);
impl Drop for OwnedGroup {
    fn drop(&mut self) {
        let pid = self.0.id() as i32;
        if pid > 1 {
            // SAFETY: process_group(0) created this group and leader is unreaped.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
        let _ = self.0.wait();
    }
}

fn command(
    ctx: &ExecContext,
    repository: &Path,
    args: &[&str],
    max: usize,
    request_started: Instant,
) -> Result<Vec<u8>> {
    command_input(ctx, repository, args, max, request_started, None)
}

fn command_input(
    ctx: &ExecContext,
    repository: &Path,
    args: &[&str],
    max: usize,
    request_started: Instant,
    input: Option<&[u8]>,
) -> Result<Vec<u8>> {
    ensure!(!ctx.cancel.is_cancelled(), "Git evaluation cancelled");
    let remaining = REQUEST_TIME
        .checked_sub(request_started.elapsed())
        .context("Git evaluation request deadline exceeded")?;
    let timeout = remaining.min(COMMAND_TIME);
    let mut cmd = ctx.command("git");
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_INDEX_FILE",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_EXEC_PATH",
    ] {
        cmd.env_remove(name);
    }
    cmd.args([
        "--no-replace-objects",
        "--no-lazy-fetch",
        "--literal-pathspecs",
        "-C",
    ])
    .arg(repository)
    .args([
        "-c",
        "core.fsmonitor=false",
        "-c",
        "gc.auto=0",
        "-c",
        "maintenance.auto=false",
    ])
    .args(args)
    .env("GIT_CONFIG_NOSYSTEM", "1")
    .env("GIT_CONFIG_GLOBAL", "/dev/null")
    .env("GIT_TERMINAL_PROMPT", "0")
    .env("GIT_ALLOW_PROTOCOL", "")
    .env("GIT_OPTIONAL_LOCKS", "0")
    .stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    })
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .process_group(0);
    let started = Instant::now();
    let mut child = OwnedGroup(cmd.spawn().context("start Git evaluation command")?);
    #[cfg(test)]
    tests::observe_spawn(&child.0);
    let mut stdout = child.0.stdout.take().context("missing Git stdout")?;
    let mut stderr = child.0.stderr.take().context("missing Git stderr")?;
    let mut stdin = child.0.stdin.take();
    let mut input_offset = 0;
    for fd in [
        Some(stdout.as_raw_fd()),
        Some(stderr.as_raw_fd()),
        stdin.as_ref().map(AsRawFd::as_raw_fd),
    ]
    .into_iter()
    .flatten()
    {
        // SAFETY: the owned descriptors are pipe read ends.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
                return Err(std::io::Error::last_os_error()).context("nonblocking Git pipe");
            }
        }
    }
    let mut output = Vec::new();
    let mut errors = Vec::new();
    let mut eof = [false; 2];
    loop {
        ensure!(!ctx.cancel.is_cancelled(), "Git evaluation cancelled");
        ensure!(
            started.elapsed() < timeout,
            "Git evaluation command deadline exceeded"
        );
        if let Some(pipe) = &mut stdin {
            let input = input.context("missing Git input")?;
            if input_offset < input.len() {
                let end = input.len().min(input_offset + 65536);
                match pipe.write(&input[input_offset..end]) {
                    Ok(0) => bail!("Git evaluation input closed"),
                    Ok(n) => input_offset += n,
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ) => {}
                    Err(e) => return Err(e).context("write Git evaluation input"),
                }
            }
            if input_offset == input.len() {
                stdin.take();
            }
        }
        for (index, (pipe, bytes, limit)) in [
            (&mut stdout as &mut dyn Read, &mut output, max),
            (&mut stderr as &mut dyn Read, &mut errors, MAX_STDERR),
        ]
        .into_iter()
        .enumerate()
        {
            let mut buffer = [0; 65536];
            match pipe.read(&mut buffer) {
                Ok(0) => eof[index] = true,
                Ok(n) => {
                    ensure!(
                        bytes.len().checked_add(n).is_some_and(|n| n <= limit),
                        "Git evaluation output limit exceeded"
                    );
                    bytes.extend_from_slice(&buffer[..n]);
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Err(e).context("read Git evaluation output"),
            }
        }
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: inspect only our owned leader, leaving its PID pinned for Drop.
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                child.0.id(),
                &mut status,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error()).context("inspect Git evaluation child");
        }
        if unsafe { status.si_pid() } != 0 && eof.iter().all(|v| *v) {
            if status.si_code != libc::CLD_EXITED || unsafe { status.si_status() } != 0 {
                bail!("Git evaluation command failed (requires Git --no-lazy-fetch support and local objects)");
            }
            ensure!(
                input.is_none_or(|bytes| input_offset == bytes.len()),
                "Git evaluation input incomplete"
            );
            return Ok(output);
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(test)]
mod tests;
