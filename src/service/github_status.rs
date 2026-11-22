//! The GitHub commit status of a `ci` run, posted by the parent only.
//!
//! The decision is [`crate::ci::status`]; this module looks up what it needs
//! (the sealed manifest of the run's input, the repository's `origin` and the
//! tree of the commit the input was captured from) and carries the post out.
//! The credential is whatever `gh` on the parent uses. It is never part of a
//! task, a plan or anything else a worker receives, and it is not kept.

use super::context::Context;
use crate::ci::status::{self, Lookup, Post, Posting, Record, RunEnd, Source};
use crate::ids::RunId;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The network side of a post. Tests use a fake; nothing but [`Gh`] reaches
/// GitHub.
pub trait Poster: Send + Sync {
    /// `Err` is what the service answered, unedited.
    fn post(&self, post: &Post) -> Result<(), String>;
}

/// How long one post may take. `ci` has its result by then; a network that
/// hangs must not keep it from ending.
pub const POST_DEADLINE: Duration = Duration::from_secs(30);

/// `gh api repos/<owner>/<name>/statuses/<sha>`, the call local-ci makes.
pub struct Gh {
    /// The `PATH` `gh` is looked up in.
    path: Option<String>,
    /// Where `gh` runs: not the repository, so nothing in the checked-out
    /// tree is picked up as its configuration.
    cwd: PathBuf,
    deadline: Duration,
}

impl Gh {
    pub fn new(context: &Context) -> Self {
        Gh {
            path: context.env.get("PATH").cloned(),
            cwd: context.state_dir().to_path_buf(),
            deadline: POST_DEADLINE,
        }
    }

    /// `gh` from the absolute entries of `PATH` only: a relative entry would
    /// be resolved against a directory the repository controls.
    fn program(&self) -> Option<PathBuf> {
        let executable = |path: &PathBuf| {
            std::fs::metadata(path)
                .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        };
        std::env::split_paths(self.path.as_deref()?)
            .filter(|dir| dir.is_absolute())
            .map(|dir| dir.join("gh"))
            .find(executable)
    }
}

impl Poster for Gh {
    fn post(&self, post: &Post) -> Result<(), String> {
        let program = self.program().ok_or("gh is not installed")?;
        let mut child = Command::new(program)
            .args([
                "api",
                &format!("repos/{}/statuses/{}", post.repository, post.sha),
                "-f",
                &format!("state={}", post.state),
                "-f",
                &format!("context={}", post.context),
                "-f",
                &format!("description={}", post.description),
            ])
            .current_dir(&self.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Its own group, so a time-out ends what gh started too.
            .process_group(0)
            .spawn()
            .map_err(|e| format!("gh cannot be run: {e}"))?;
        let pgid = child.id();
        // Both pipes are read while gh runs: output larger than a pipe
        // holds must not stop gh and look like a hang.
        fn drain(pipe: Option<impl Read + Send + 'static>) -> std::thread::JoinHandle<Vec<u8>> {
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                if let Some(mut pipe) = pipe {
                    let _ = pipe.read_to_end(&mut bytes);
                }
                bytes
            })
        }
        let (stderr, stdout) = (drain(child.stderr.take()), drain(child.stdout.take()));
        let started = Instant::now();
        let exit = loop {
            match child.try_wait() {
                Ok(Some(exit)) => break exit,
                Ok(None) if started.elapsed() < self.deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                waited => {
                    // The group of our own unreaped child: its number cannot
                    // be reused, and the guarded helper refuses 0 and 1.
                    if let Ok(group) = i32::try_from(pgid) {
                        crate::platform::signal_process_group(group, libc::SIGKILL);
                    }
                    let _ = child.wait();
                    return Err(match waited {
                        Err(e) => format!("gh cannot be waited for: {e}"),
                        Ok(_) => "timed out".into(),
                    });
                }
            }
        };
        if exit.success() {
            return Ok(());
        }
        let mut said = stderr.join().unwrap_or_default();
        said.extend(stdout.join().unwrap_or_default());
        Err(String::from_utf8_lossy(&said).into_owned())
    }
}

/// What `ci` was asked about the status.
#[derive(Debug, Clone)]
pub struct Options {
    /// `false` with `--no-status`.
    pub enabled: bool,
    pub context: String,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            enabled: true,
            context: status::DEFAULT_CONTEXT.to_string(),
        }
    }
}

/// Decide the status of `run` from how it ended and post it when it may be.
/// Never fails: what could not be looked up or posted is in the record, and
/// the computed result is the same whatever became of the post.
pub fn settle(
    context: &Context,
    run: &RunId,
    end: &RunEnd<'_>,
    options: &Options,
    poster: &dyn Poster,
) -> Record {
    let computed = status::computed(end);
    let manifest = match super::input::source_manifest(context, run) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Record {
                computed,
                posting: Posting::Skipped {
                    reason: format!("the input of the run cannot be read: {}", error.message),
                },
                source_digest: None,
                original_head: None,
            }
        }
    };
    let git = |args: &[&str]| git(context, args);
    let origin = git(&["remote", "get-url", "origin"]);
    let gh_host = context.env.get("GH_HOST").filter(|host| !host.is_empty());
    let repository = status::repository(
        manifest.repository_id.as_deref(),
        origin.as_deref(),
        gh_host.map(String::as_str),
    );
    // A directory is no commit, whatever its manifest says.
    let directory = manifest.source_kind == local_ci::source::SourceKind::Directory;
    let head = manifest.original_head.as_deref().filter(|_| !directory);
    let tree = head.and_then(|head| {
        git(&[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{head}^{{tree}}"),
        ])
    });
    let description = status::description(computed, end.jobs);
    let decision = status::decide(
        computed,
        &Source {
            source_id: &manifest.source_id,
            original_head: head,
            worktree_tree: manifest.worktree_tree.as_deref(),
            dirty_submodule: manifest.submodules.iter().any(|sub| sub.dirty),
        },
        &Lookup {
            enabled: options.enabled,
            context: &options.context,
            description: &description,
            repository: repository
                .as_ref()
                .map(String::as_str)
                .map_err(String::as_str),
            tree_of_head: tree.as_deref(),
        },
    );
    Record {
        computed,
        posting: status::outcome(decision, |post| poster.post(post)),
        source_digest: Some(manifest.manifest_digest),
        original_head: manifest.original_head,
    }
}

/// One line of `git` in the repository `ci` was run from.
fn git(context: &Context, args: &[&str]) -> Option<String> {
    let repo: &Path = context.repo_root.as_deref()?;
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .env_clear()
        .envs(&context.env)
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    output.status.success().then(|| text.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post() -> Post {
        Post {
            repository: "owner/name".into(),
            sha: "0123456789abcdef".into(),
            state: "success",
            context: "local-ci".into(),
            description: "success: pass 1".into(),
        }
    }

    /// A fake `gh` in `<root>/bin`, and a poster that finds it there and
    /// runs it in `<root>/cwd`.
    fn fake(root: &Path, script: &str, deadline: Duration) -> Gh {
        let (bin, cwd) = (root.join("bin"), root.join("cwd"));
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&cwd).unwrap();
        let gh = bin.join("gh");
        std::fs::write(&gh, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
        Gh {
            // Only directories of the test: the host may have a real gh.
            path: Some(format!("relative:{}:{}", cwd.display(), bin.display())),
            cwd,
            deadline,
        }
    }

    #[test]
    fn gh_is_called_as_local_ci_calls_it_outside_the_repository() {
        let root = crate::test_tmp::tempdir().unwrap();
        let seen = root.path().join("seen");
        let script = format!("pwd > {seen:?}\nprintf '%s\\n' \"$@\" >> {seen:?}");
        let gh = fake(root.path(), &script, Duration::from_secs(20));
        assert_eq!(gh.post(&post()), Ok(()));
        let cwd = std::fs::canonicalize(root.path().join("cwd")).unwrap();
        assert_eq!(
            std::fs::read_to_string(&seen).unwrap(),
            format!(
                "{}\napi\nrepos/owner/name/statuses/0123456789abcdef\n-f\nstate=success\n-f\ncontext=local-ci\n-f\ndescription=success: pass 1\n",
                cwd.display()
            )
        );
    }

    #[test]
    fn a_refusal_is_returned_as_it_was_said() {
        let root = crate::test_tmp::tempdir().unwrap();
        let script = "echo 'gh: Bad credentials (HTTP 401)' >&2\necho body\nexit 1";
        let gh = fake(root.path(), script, Duration::from_secs(20));
        assert_eq!(
            gh.post(&post()),
            Err("gh: Bad credentials (HTTP 401)\nbody\n".into())
        );
    }

    #[test]
    fn a_refusal_larger_than_a_pipe_is_read_whole_and_is_no_timeout() {
        let root = crate::test_tmp::tempdir().unwrap();
        // 4000 lines of 50 bytes on each pipe: 200 KiB each.
        let line = "x".repeat(49);
        let script = format!(
            "echo 'gh: Server Error (HTTP 500)' >&2\ni=0\nwhile [ $i -lt 4000 ]; do echo {line}; echo {line} >&2; i=$((i+1)); done\nexit 1"
        );
        let gh = fake(root.path(), &script, Duration::from_secs(60));
        let said = gh.post(&post()).unwrap_err();
        assert!(said.len() > 2 * 128 * 1024, "{}", said.len());
        assert_eq!(said.lines().next(), Some("gh: Server Error (HTTP 500)"));
        assert_eq!(
            status::post_error("0123456789abcdef", &said),
            "gh: Server Error (HTTP 500)"
        );
    }

    #[test]
    fn a_post_that_hangs_is_stopped_at_the_deadline() {
        let root = crate::test_tmp::tempdir().unwrap();
        let gh = fake(root.path(), "exec sleep 600", Duration::from_millis(200));
        let started = Instant::now();
        assert_eq!(gh.post(&post()), Err("timed out".into()));
        assert!(started.elapsed() < Duration::from_secs(20));
    }

    fn alive(pid: u32) -> bool {
        crate::platform::signal_pid(pid, 0)
    }

    #[test]
    fn a_timeout_ends_what_gh_started_as_well() {
        let root = crate::test_tmp::tempdir().unwrap();
        let pids = root.path().join("pids");
        let script = format!("sleep 300 &\necho \"$$ $!\" > {pids:?}\nwait");
        let gh = fake(root.path(), &script, Duration::from_secs(2));
        assert_eq!(gh.post(&post()), Err("timed out".into()));
        let text = std::fs::read_to_string(&pids).unwrap();
        let ids: Vec<u32> = text
            .split_whitespace()
            .map(|n| n.parse().unwrap())
            .collect();
        assert_eq!(ids.len(), 2, "{text}");
        let deadline = Instant::now() + Duration::from_secs(10);
        while ids.iter().any(|&pid| alive(pid)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            ids.iter().map(|&p| alive(p)).collect::<Vec<_>>(),
            [false, false]
        );
    }

    #[test]
    fn gh_runs_as_leader_of_a_group_of_its_own_and_a_normal_post_is_unchanged() {
        let root = crate::test_tmp::tempdir().unwrap();
        let seen = root.path().join("seen");
        let script = format!(
            "echo \"$$ $(ps -o pgid= -p $$) $([ -t 0 ] && echo tty || echo notty)\" > {seen:?}\necho out\necho err >&2"
        );
        let gh = fake(root.path(), &script, Duration::from_secs(20));
        assert_eq!(gh.post(&post()), Ok(()));
        let text = std::fs::read_to_string(&seen).unwrap();
        let parts: Vec<&str> = text.split_whitespace().collect();
        assert_eq!(parts[0], parts[1], "gh leads its own group: {text}");
        assert_eq!(parts[2], "notty");
        // SAFETY: plain syscall without arguments.
        assert_ne!(parts[1], unsafe { libc::getpgrp() }.to_string());
    }

    #[test]
    fn gh_is_taken_only_from_an_absolute_path_entry_that_holds_an_executable() {
        let root = crate::test_tmp::tempdir().unwrap();
        let mut gh = fake(root.path(), "exit 0", Duration::from_secs(20));
        let bin = root.path().join("bin");
        assert_eq!(gh.program(), Some(bin.join("gh")));
        // Not executable: not it.
        let mode = std::fs::Permissions::from_mode(0o644);
        std::fs::set_permissions(bin.join("gh"), mode).unwrap();
        assert_eq!(gh.program(), None);
        assert_eq!(gh.post(&post()), Err("gh is not installed".into()));
        // A relative entry is never searched, and no PATH finds nothing.
        gh.path = Some("bin:.".into());
        assert_eq!(gh.program(), None);
        gh.path = None;
        assert_eq!(gh.program(), None);
    }
}
