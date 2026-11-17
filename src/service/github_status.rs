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
            .spawn()
            .map_err(|e| format!("gh cannot be run: {e}"))?;
        let started = Instant::now();
        let exit = loop {
            match child.try_wait() {
                Ok(Some(exit)) => break exit,
                Ok(None) if started.elapsed() < self.deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                waited => {
                    // Our own child, through its handle.
                    let _ = child.kill();
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
        let mut said = String::new();
        if let Some(mut stderr) = child.stderr.take() {
            let _ = stderr.read_to_string(&mut said);
        }
        if let Some(mut stdout) = child.stdout.take() {
            let _ = stdout.read_to_string(&mut said);
        }
        Err(said)
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
            path: Some(format!("relative:{}:/bin:/usr/bin", bin.display())),
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
    fn a_post_that_hangs_is_stopped_at_the_deadline() {
        let root = crate::test_tmp::tempdir().unwrap();
        let gh = fake(root.path(), "exec sleep 600", Duration::from_millis(200));
        let started = Instant::now();
        assert_eq!(gh.post(&post()), Err("timed out".into()));
        assert!(started.elapsed() < Duration::from_secs(20));
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
