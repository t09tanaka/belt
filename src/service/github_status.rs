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
use std::process::{Command, Stdio};

/// The network side of a post. Tests use a fake; nothing but [`Gh`] reaches
/// GitHub.
pub trait Poster: Send + Sync {
    /// `Err` is what the service answered, unedited.
    fn post(&self, post: &Post) -> Result<(), String>;
}

/// `gh api repos/<owner>/<name>/statuses/<sha>`, the call local-ci makes.
pub struct Gh;

impl Poster for Gh {
    fn post(&self, post: &Post) -> Result<(), String> {
        let output = Command::new("gh")
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
            .stdin(Stdio::null())
            .output()
            .map_err(|e| format!("gh cannot be run: {e}"))?;
        if output.status.success() {
            return Ok(());
        }
        Err(String::from_utf8_lossy(&output.stderr).into_owned()
            + &String::from_utf8_lossy(&output.stdout))
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
    let git = |args: &[&str]| -> Option<String> {
        let output = Command::new("git")
            .args(args)
            .current_dir(context.repo_root.as_deref()?)
            .env_clear()
            .envs(&context.env)
            .env("LC_ALL", "C")
            .stdin(Stdio::null())
            .output()
            .ok()?;
        let text = String::from_utf8(output.stdout).ok()?;
        output.status.success().then(|| text.trim().to_string())
    };
    let repository = git(&["remote", "get-url", "origin"]).and_then(|url| status::repo_slug(&url));
    let tree = manifest.original_head.as_deref().and_then(|head| {
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
            original_head: manifest.original_head.as_deref(),
            worktree_tree: manifest.worktree_tree.as_deref(),
            dirty_submodule: manifest.submodules.iter().any(|sub| sub.dirty),
        },
        &Lookup {
            enabled: options.enabled,
            context: &options.context,
            description: &description,
            repository: repository.as_deref(),
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
