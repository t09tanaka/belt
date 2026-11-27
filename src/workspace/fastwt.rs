//! The `fast-worktree` command (spec 22.5), behind a trait so that everything
//! above it can be tested without the tool, and the real one can be tested
//! where it is installed.

use super::git::Env;
use super::plan::{FwFailure, Tail};
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Make and remove the worktree of an attempt in a canonical clone.
pub trait FastWorktree: Send + Sync {
    /// A worktree at `dest` on the new branch `branch`, starting at the commit
    /// `start`, with the project's dependencies prepared (its
    /// `.fast-worktree.toml`, `post_add` hooks included), less the projects
    /// of the presets in `skip` (`--skip-preset`, spec 22.1): nothing of
    /// those runs.
    fn add(
        &self,
        repo: &Path,
        dest: &Path,
        branch: &str,
        start: &str,
        skip: &[String],
    ) -> Result<(), FwFailure>;
    /// Remove the clean worktree at `path` (`pre_remove` hooks run).
    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure>;
    /// Release this attempt's port reservation even after a previous cleanup
    /// removed its worktree. Implementations without a port ledger need no work.
    fn release_ports(&self, _repo: &Path, _path: &Path) -> Result<(), FwFailure> {
        Ok(())
    }
}

/// The variable fast-worktree adds to every key of its preparation caches
/// (the pool's and the clones').
const KEY_IDENTITY: &str = "FAST_WORKTREE_KEY_IDENTITY";

/// The cache identity of a preparation that skips `skip` (sorted, spec
/// 22.1): the activated toolchains' `identity`, if any, with the skip set,
/// so that what one skip set prepared is never reused for another. `None`
/// (the identity unchanged) when nothing is skipped.
pub fn key_identity(identity: Option<&str>, skip: &[String]) -> Option<String> {
    if skip.is_empty() {
        return None;
    }
    let skipped = format!("skip-preset:{}", skip.join(","));
    Some(match identity.filter(|id| !id.is_empty()) {
        Some(id) => format!("{id}+{skipped}"),
        None => skipped,
    })
}

/// How long one call may run unless the caller says otherwise: a `post_add`
/// hook installing dependencies is the slow part.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// How much of a call's output is kept: the end of each stream.
const KEEP_BYTES: usize = 64 * 1024;

/// How long to wait for the output streams to end once the process is gone
/// (a hook's daemon that outlived it may still hold them open).
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The installed `fast-worktree`, found on the PATH of `env`.
///
/// `env` is the whole environment the tool and the project's hooks run with
/// (the process's own is never read): the caller, the supervisor, passes what
/// they need, `HOME` and `PATH` always, and `NPM_CONFIG_*`, `PORTOOL_*` and the
/// like for the project's hooks. Without `HOME` or `PATH` a call fails with
/// [`FwFailure::NoEnv`] saying so, instead of a hook failing later for want
/// of it.
#[derive(Debug, Clone)]
pub struct CliFastWorktree {
    env: Env,
    timeout: Duration,
}

impl CliFastWorktree {
    /// With [`DEFAULT_TIMEOUT`] per call.
    pub fn new(env: Env) -> Self {
        CliFastWorktree {
            env,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// With `timeout` per call: past it the tool's whole process group is
    /// killed and the call is [`FwFailure::TimedOut`].
    pub fn with_timeout(env: Env, timeout: Duration) -> Self {
        CliFastWorktree { env, timeout }
    }

    fn run(&self, repo: &Path, args: &[&std::ffi::OsStr]) -> Result<(), FwFailure> {
        self.run_with(repo, args, None)
    }

    fn run_with(
        &self,
        repo: &Path,
        args: &[&std::ffi::OsStr],
        identity: Option<String>,
    ) -> Result<(), FwFailure> {
        let missing: Vec<String> = ["HOME", "PATH"]
            .into_iter()
            .filter(|k| !self.env.contains_key(*k))
            .map(str::to_string)
            .collect();
        if !missing.is_empty() {
            return Err(FwFailure::NoEnv { missing });
        }
        let mut child = Command::new("fast-worktree")
            .args(args)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .envs(identity.map(|id| (KEY_IDENTITY, id)))
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // its own group: a timeout kills the hooks it started too
            .process_group(0)
            .spawn()
            .map_err(|e| {
                // (a missing working directory also reads as NotFound)
                if e.kind() == std::io::ErrorKind::NotFound && repo.is_dir() {
                    FwFailure::Missing
                } else {
                    FwFailure::Failed {
                        code: None,
                        stderr: e.to_string(),
                    }
                }
            })?;
        let pgid = child.id() as libc::pid_t;
        let out = drain(child.stdout.take());
        let err = drain(child.stderr.take());
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break Some(s),
                Ok(None) if started.elapsed() >= self.timeout => break None,
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(e) => {
                    kill_group(pgid);
                    let _ = child.wait();
                    return Err(FwFailure::Failed {
                        code: None,
                        stderr: e.to_string(),
                    });
                }
            }
        };
        if status.is_none() {
            kill_group(pgid);
            let _ = child.wait();
        }
        let (out, err) = (out.finish(), err.finish());
        let Some(status) = status else {
            return Err(FwFailure::TimedOut {
                secs: self.timeout.as_secs(),
                stderr: if err.trim().is_empty() { out } else { err },
            });
        };
        if status.success() {
            return Ok(());
        }
        // its report of what failed is on stderr, FAILED lines included
        Err(FwFailure::Failed {
            code: status.code(),
            stderr: if err.trim().is_empty() { out } else { err },
        })
    }
}

fn kill_group(pgid: libc::pid_t) {
    // The guarded helper refuses group 0 (our own) and 1 (init).
    crate::platform::signal_process_group(pgid, libc::SIGKILL);
}

/// A stream being read on its own thread into a [`Tail`].
struct Drain {
    tail: Arc<Mutex<Tail>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn drain<R: Read + Send + 'static>(stream: Option<R>) -> Drain {
    let tail = Arc::new(Mutex::new(Tail::new(KEEP_BYTES)));
    let t = tail.clone();
    let thread = stream.map(|mut s| {
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            while let Ok(n) = s.read(&mut buf) {
                if n == 0 {
                    break;
                }
                t.lock().unwrap_or_else(|e| e.into_inner()).push(&buf[..n]);
            }
        })
    });
    Drain { tail, thread }
}

impl Drain {
    /// What was read; waits a little for the stream to end, and leaves the
    /// reader behind if something still holds it open.
    fn finish(self) -> String {
        if let Some(t) = self.thread {
            let until = Instant::now() + DRAIN_GRACE;
            while !t.is_finished() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let text = self.tail.lock().unwrap_or_else(|e| e.into_inner()).text();
        text
    }
}

impl FastWorktree for CliFastWorktree {
    fn add(
        &self,
        repo: &Path,
        dest: &Path,
        branch: &str,
        start: &str,
        skip: &[String],
    ) -> Result<(), FwFailure> {
        use std::ffi::OsStr;
        let mut args = vec![
            OsStr::new("add"),
            dest.as_os_str(),
            OsStr::new("-b"),
            OsStr::new(branch),
            OsStr::new(start),
        ];
        for preset in skip {
            args.push(OsStr::new("--skip-preset"));
            args.push(OsStr::new(preset));
        }
        let identity = key_identity(self.env.get(KEY_IDENTITY).map(String::as_str), skip);
        self.run_with(repo, &args, identity)
    }

    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        use std::ffi::OsStr;
        self.run(repo, &[OsStr::new("remove"), path.as_os_str()])
    }
    fn release_ports(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        portool::release_removed_in(&portool::Context::from_env(&self.env), repo, path)
            .map(|_| ())
            .map_err(|err| match err {
                portool::Error::UnsupportedRegistryVersion {
                    found, supported, ..
                } => FwFailure::IncompatibleRegistry {
                    message: super::embedded::registry_diagnostic(found, supported),
                },
                err => FwFailure::Failed {
                    code: None,
                    stderr: err.to_string(),
                },
            })
    }
}

#[cfg(test)]
mod tests {
    use super::key_identity;

    /// The skip set is part of the preparation cache identity (spec 22.1):
    /// what a preparation that skipped a preset made is never reused for one
    /// that skipped another, and nothing skipped keeps the identity as it was.
    #[test]
    fn the_skip_set_changes_the_cache_identity() {
        let npm = ["npm".to_string()];
        let both = ["dart".to_string(), "npm".to_string()];
        assert_eq!(key_identity(Some("sha256:a"), &[]), None);
        assert_eq!(key_identity(None, &[]), None);
        let a = key_identity(Some("sha256:a"), &npm).unwrap();
        assert_ne!(a, "sha256:a");
        assert!(a.starts_with("sha256:a"), "{a}");
        assert_ne!(Some(a), key_identity(Some("sha256:a"), &both));
        assert_ne!(key_identity(None, &npm), key_identity(None, &both));
        assert_eq!(key_identity(Some(""), &npm), key_identity(None, &npm));
    }
}
