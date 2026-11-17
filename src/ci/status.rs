//! The GitHub commit status of a `ci` run: what the run computed, whether
//! that may be posted to a commit, and what became of the post.
//!
//! Decisions only. Git, the source store and the network are with the
//! caller ([`crate::service::github_status`]); what they found comes in as
//! plain values. Only the parent posts: nothing here is part of a task a
//! worker receives.
//!
//! The computed result and the posting are separate facts. A post that was
//! skipped or failed never changes what the run computed.

use serde::{Deserialize, Serialize};

/// The context local-ci posts under; the default here for compatibility.
pub const DEFAULT_CONTEXT: &str = "local-ci";

/// What the run computed, decided only from what the parent collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Computed {
    /// Every expected task left a result and every job passed or was skipped.
    Success,
    /// A job failed.
    Failure,
    /// The run ended, but not with a full set of good results: a result is
    /// missing, a job ended in another way, or the run's exit is not zero.
    Error,
    /// The run was cancelled or its end was not observed.
    Cancelled,
    /// Results or required artifacts are still being collected.
    Pending,
}

impl Computed {
    pub fn as_str(self) -> &'static str {
        match self {
            Computed::Success => "success",
            Computed::Failure => "failure",
            Computed::Error => "error",
            Computed::Cancelled => "cancelled",
            Computed::Pending => "pending",
        }
    }

    /// The state GitHub is told, as local-ci maps it. `None`: not a final
    /// state, nothing is posted.
    pub fn github_state(self) -> Option<&'static str> {
        match self {
            Computed::Success | Computed::Failure | Computed::Error => Some(self.as_str()),
            Computed::Cancelled | Computed::Pending => None,
        }
    }
}

/// One line of what the parent collected: a job of a task, or the task
/// itself with the status `no_result`.
#[derive(Debug, Clone, Copy)]
pub struct Job<'a> {
    pub plan_task: &'a str,
    pub status: &'a str,
}

/// How the run ended, as the waiting side saw it.
#[derive(Debug, Clone, Copy)]
pub struct RunEnd<'a> {
    /// The run settled by itself (not an interrupted or unconfirmed wait).
    pub settled: bool,
    pub cancel_requested: bool,
    /// `Some(false)` only when every collection is closed.
    pub collection_pending: Option<bool>,
    pub exit_code: i32,
    /// The tasks of the frozen plan that were submitted.
    pub expected: &'a [&'a str],
    pub jobs: &'a [Job<'a>],
}

/// The result of the run. Success fails closed: it needs a settled run, a
/// closed collection, exit 0, at least one expected task, a line for each of
/// them and no line that is anything but `success` or `skipped`.
pub fn computed(end: &RunEnd<'_>) -> Computed {
    if !end.settled || end.cancel_requested {
        return Computed::Cancelled;
    }
    if end.collection_pending != Some(false) {
        return Computed::Pending;
    }
    if end.jobs.iter().any(|job| job.status == "failure") {
        return Computed::Failure;
    }
    let covered = |task: &&str| end.jobs.iter().any(|job| job.plan_task == *task);
    let good = |job: &Job<'_>| matches!(job.status, "success" | "skipped");
    if end.exit_code == 0
        && !end.expected.is_empty()
        && end.expected.iter().all(covered)
        && end.jobs.iter().all(good)
    {
        Computed::Success
    } else {
        Computed::Error
    }
}

/// `<state>: pass P / fail F / error E / skip S`, the shape of local-ci's
/// description.
pub fn description(computed: Computed, jobs: &[Job<'_>]) -> String {
    let count = |status: &str| jobs.iter().filter(|job| job.status == status).count();
    let (pass, fail, skip) = (count("success"), count("failure"), count("skipped"));
    let error = jobs.len() - pass - fail - skip;
    format!(
        "{}: pass {pass} / fail {fail} / error {error} / skip {skip}",
        computed.as_str()
    )
}

/// What the captured input says about itself (the sealed manifest).
#[derive(Debug, Clone, Copy)]
pub struct Source<'a> {
    pub source_id: &'a str,
    /// The commit the input was captured from. A directory has none.
    pub original_head: Option<&'a str>,
    /// The Git tree of exactly what was tested.
    pub worktree_tree: Option<&'a str>,
    /// A submodule was checked out with content its recorded revision does
    /// not hold. The superproject's tree does not show that.
    pub dirty_submodule: bool,
}

/// What is sent to GitHub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Post {
    /// `owner/name`.
    pub repository: String,
    pub sha: String,
    pub state: &'static str,
    pub context: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Post(Post),
    /// Not posted, and why.
    Skip(String),
}

/// What the caller looked up for the decision.
#[derive(Debug, Clone, Copy)]
pub struct Lookup<'a> {
    /// `--no-status` was not given.
    pub enabled: bool,
    pub context: &'a str,
    pub description: &'a str,
    /// `owner/name` on GitHub ([`repository`]), or why there is none to
    /// post to.
    pub repository: Result<&'a str, &'a str>,
    /// The tree of `original_head` in the repository, `None` when it is not
    /// a commit there.
    pub tree_of_head: Option<&'a str>,
}

fn short(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

/// May the result be posted, and to which commit? The only commit a run
/// speaks for is the one its input was captured from, and only when the
/// tested tree is that commit's tree: a dirty worktree, a synthetic commit
/// or a directory is never the original `HEAD`. The same gate holds for
/// every state, as in local-ci.
pub fn decide(computed: Computed, source: &Source<'_>, lookup: &Lookup<'_>) -> Decision {
    let skip = |why: String| Decision::Skip(why);
    if !lookup.enabled {
        return skip("--no-status".into());
    }
    let Some(state) = computed.github_state() else {
        return skip(format!("the run is {}", computed.as_str()));
    };
    let repository = match lookup.repository {
        Ok(repository) => repository,
        Err(why) => return skip(why.to_string()),
    };
    let Some(head) = source.original_head else {
        return skip("the run tested a directory snapshot, not a commit".into());
    };
    if source.dirty_submodule {
        return skip(format!(
            "the run tested a submodule with uncommitted content (source {})",
            source.source_id
        ));
    }
    let Some(tested) = source.worktree_tree else {
        return skip("the run recorded no tree of its input".into());
    };
    let Some(tree) = lookup.tree_of_head else {
        return skip(format!(
            "{} is not a commit of this repository",
            short(head)
        ));
    };
    if tree != tested {
        return skip(format!(
            "the run tested uncommitted or synthetic content (source {}), not what {} holds",
            source.source_id,
            short(head)
        ));
    }
    Decision::Post(Post {
        repository: repository.to_string(),
        sha: head.to_string(),
        state,
        context: lookup.context.to_string(),
        description: lookup.description.to_string(),
    })
}

/// What became of the post.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "posting", rename_all = "snake_case")]
pub enum Posting {
    Posted {
        sha: String,
        state: String,
        context: String,
    },
    Skipped {
        reason: String,
    },
    /// The result was computed and may be posted, but the post did not go
    /// through.
    Failed {
        sha: String,
        state: String,
        context: String,
        error: String,
    },
}

/// Carry out `decision` with `post` (the network, behind the caller).
pub fn outcome(decision: Decision, post: impl FnOnce(&Post) -> Result<(), String>) -> Posting {
    let request = match decision {
        Decision::Skip(reason) => return Posting::Skipped { reason },
        Decision::Post(request) => request,
    };
    let result = post(&request);
    let Post {
        sha,
        state,
        context,
        ..
    } = request;
    let state = state.to_string();
    match result {
        Ok(()) => Posting::Posted {
            sha,
            state,
            context,
        },
        Err(error) => Posting::Failed {
            error: post_error(&sha, &error),
            sha,
            state,
            context,
        },
    }
}

/// What a failed post says. A commit GitHub does not know (not pushed yet)
/// is named as that; anything else is the first line of what was answered.
pub fn post_error(sha: &str, raw: &str) -> String {
    if raw.contains("No commit found") {
        return format!("{} is not on GitHub yet", short(sha));
    }
    raw.lines().next().unwrap_or("").trim().to_string()
}

/// The host GitHub is reached at when `GH_HOST` does not name another.
pub const GITHUB_HOST: &str = "github.com";

/// Host and `owner/name` of a remote URL: `[user@]host:owner/name[.git]`
/// or `scheme://[user@]host[:port]/owner/name[.git]`. A local path, or a
/// path that is not exactly owner and name, is no such remote.
pub fn remote(url: &str) -> Option<(&str, String)> {
    let url = url.trim();
    let (authority, path) = match url.split_once("://") {
        Some((_, rest)) => rest.split_once('/')?,
        None => url
            .split_once(':')
            .filter(|(host, _)| !host.contains('/'))?,
    };
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = host.split_once(':').map_or(host, |(host, _)| host);
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    let (owner, name) = path.split_once('/')?;
    (!host.is_empty() && !name.is_empty() && !name.contains('/'))
        .then(|| (host, format!("{owner}/{name}")))
}

/// The repository on GitHub a status of this run may go to, or why there is
/// none. `origin` must be on the GitHub host (`gh_host`, else github.com),
/// and it must still be the repository the input was captured from
/// (`captured`, the manifest's `github:<owner>/<name>`): what is posted
/// goes where the tested content came from, not where `origin` points now.
pub fn repository(
    captured: Option<&str>,
    origin: Option<&str>,
    gh_host: Option<&str>,
) -> Result<String, String> {
    let origin = origin.ok_or("no origin remote")?;
    let (host, slug) = remote(origin).ok_or("origin is not a repository on GitHub")?;
    let wanted = gh_host.unwrap_or(GITHUB_HOST);
    if !host.eq_ignore_ascii_case(wanted) {
        return Err(format!("origin is on {host}, not on {wanted}"));
    }
    if captured != Some(format!("github:{slug}").as_str()) {
        return Err(format!(
            "origin is {slug} now, not the repository the input was captured from"
        ));
    }
    Ok(slug)
}

/// The status of a run as it is reported and kept: the computed result, the
/// posting, and what was tested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub computed: Computed,
    #[serde(flatten)]
    pub posting: Posting,
    /// The digest of the manifest of the tested input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_digest: Option<String>,
    /// The commit the input was captured from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_head: Option<String>,
}

/// The line `ci` prints.
pub fn render(record: &Record) -> String {
    match &record.posting {
        Posting::Posted {
            sha,
            state,
            context,
        } => format!("status: {context}={state} posted to {}", short(sha)),
        Posting::Skipped { reason } => format!("status: not posted ({reason})"),
        Posting::Failed {
            sha,
            state,
            context,
            error,
        } => format!(
            "status: {context}={state} could not be posted to {} ({error})",
            short(sha)
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: &str = "0123456789abcdef0123456789abcdef01234567";

    fn jobs<'a>(lines: &[(&'a str, &'a str)]) -> Vec<Job<'a>> {
        lines
            .iter()
            .map(|&(plan_task, status)| Job { plan_task, status })
            .collect()
    }

    fn end<'a>(expected: &'a [&'a str], jobs: &'a [Job<'a>]) -> RunEnd<'a> {
        RunEnd {
            settled: true,
            cancel_requested: false,
            collection_pending: Some(false),
            exit_code: 0,
            expected,
            jobs,
        }
    }

    #[test]
    fn success_needs_every_expected_task_and_only_good_lines() {
        let lines = jobs(&[("a", "success"), ("b", "skipped")]);
        assert_eq!(computed(&end(&["a", "b"], &lines)), Computed::Success);
        // a task of the plan left no line at all
        assert_eq!(computed(&end(&["a", "b", "c"], &lines)), Computed::Error);
        // nothing was expected: no success out of nothing
        assert_eq!(computed(&end(&[], &[])), Computed::Error);
        assert_eq!(computed(&end(&[], &lines)), Computed::Error);
        // the lines could not be read
        assert_eq!(computed(&end(&["a"], &[])), Computed::Error);
        let missing = jobs(&[("a", "success"), ("b", "no_result")]);
        assert_eq!(computed(&end(&["a", "b"], &missing)), Computed::Error);
        let cancelled = jobs(&[("a", "cancelled")]);
        assert_eq!(computed(&end(&["a"], &cancelled)), Computed::Error);
        let exit = RunEnd {
            exit_code: 1,
            ..end(&["a", "b"], &lines)
        };
        assert_eq!(computed(&exit), Computed::Error);
    }

    #[test]
    fn a_failed_job_is_a_failure_even_when_others_left_nothing() {
        let lines = jobs(&[("a", "failure"), ("b", "no_result")]);
        let failed = RunEnd {
            exit_code: 1,
            ..end(&["a", "b", "c"], &lines)
        };
        assert_eq!(computed(&failed), Computed::Failure);
    }

    #[test]
    fn nothing_is_final_before_the_run_settled_and_collection_closed() {
        let lines = jobs(&[("a", "success")]);
        let good = end(&["a"], &lines);
        let unsettled = RunEnd {
            settled: false,
            ..good
        };
        assert_eq!(computed(&unsettled), Computed::Cancelled);
        let cancelled = RunEnd {
            cancel_requested: true,
            ..good
        };
        assert_eq!(computed(&cancelled), Computed::Cancelled);
        for pending in [Some(true), None] {
            let open = RunEnd {
                collection_pending: pending,
                ..good
            };
            assert_eq!(computed(&open), Computed::Pending);
        }
        // a failed job does not make an open collection final
        let failed = jobs(&[("a", "failure")]);
        let open = RunEnd {
            collection_pending: Some(true),
            ..end(&["a"], &failed)
        };
        assert_eq!(computed(&open), Computed::Pending);
        // and a cancelled run is cancelled whatever was collected
        let stopped = RunEnd {
            cancel_requested: true,
            collection_pending: Some(true),
            ..end(&["a"], &failed)
        };
        assert_eq!(computed(&stopped), Computed::Cancelled);
    }

    #[test]
    fn only_final_states_are_told_to_github() {
        assert_eq!(Computed::Success.github_state(), Some("success"));
        assert_eq!(Computed::Failure.github_state(), Some("failure"));
        assert_eq!(Computed::Error.github_state(), Some("error"));
        assert_eq!(Computed::Cancelled.github_state(), None);
        assert_eq!(Computed::Pending.github_state(), None);
        assert_eq!(Computed::Cancelled.as_str(), "cancelled");
        assert_eq!(Computed::Pending.as_str(), "pending");
        assert_eq!(
            serde_json::to_string(&Computed::Pending).unwrap(),
            "\"pending\""
        );
    }

    #[test]
    fn the_description_counts_the_lines() {
        let lines = jobs(&[
            ("a", "success"),
            ("a", "success"),
            ("b", "failure"),
            ("c", "skipped"),
            ("d", "no_result"),
            ("e", "cancelled"),
        ]);
        assert_eq!(
            description(Computed::Failure, &lines),
            "failure: pass 2 / fail 1 / error 2 / skip 1"
        );
        assert_eq!(
            description(Computed::Error, &[]),
            "error: pass 0 / fail 0 / error 0 / skip 0"
        );
    }

    fn source() -> Source<'static> {
        Source {
            source_id: "src_1",
            original_head: Some(HEAD),
            worktree_tree: Some("tree1"),
            dirty_submodule: false,
        }
    }

    fn lookup() -> Lookup<'static> {
        Lookup {
            enabled: true,
            context: DEFAULT_CONTEXT,
            description: "success: pass 1 / fail 0 / error 0 / skip 0",
            repository: Ok("owner/name"),
            tree_of_head: Some("tree1"),
        }
    }

    fn skipped(computed: Computed, source: &Source<'_>, lookup: &Lookup<'_>) -> String {
        match decide(computed, source, lookup) {
            Decision::Skip(why) => why,
            Decision::Post(post) => panic!("posted {post:?}"),
        }
    }

    #[test]
    fn a_run_of_exactly_what_the_commit_holds_is_posted_to_it() {
        for (computed, state) in [
            (Computed::Success, "success"),
            (Computed::Failure, "failure"),
            (Computed::Error, "error"),
        ] {
            assert_eq!(
                decide(computed, &source(), &lookup()),
                Decision::Post(Post {
                    repository: "owner/name".into(),
                    sha: HEAD.into(),
                    state,
                    context: "local-ci".into(),
                    description: "success: pass 1 / fail 0 / error 0 / skip 0".into(),
                })
            );
        }
    }

    /// A32: the success of a dirty or synthetic snapshot is never the
    /// success of the original HEAD. Nor is its failure.
    #[test]
    fn a32_a_dirty_or_synthetic_snapshot_is_not_posted_to_the_original_head() {
        let dirty = Source {
            worktree_tree: Some("tree2"),
            ..source()
        };
        for computed in [Computed::Success, Computed::Failure, Computed::Error] {
            assert_eq!(
                skipped(computed, &dirty, &lookup()),
                "the run tested uncommitted or synthetic content (source src_1), not what 0123456 holds"
            );
        }
        let submodule = Source {
            dirty_submodule: true,
            ..source()
        };
        assert_eq!(
            skipped(Computed::Success, &submodule, &lookup()),
            "the run tested a submodule with uncommitted content (source src_1)"
        );
        let directory = Source {
            original_head: None,
            ..source()
        };
        assert_eq!(
            skipped(Computed::Success, &directory, &lookup()),
            "the run tested a directory snapshot, not a commit"
        );
        let no_tree = Source {
            worktree_tree: None,
            ..source()
        };
        assert_eq!(
            skipped(Computed::Success, &no_tree, &lookup()),
            "the run recorded no tree of its input"
        );
        let unknown = Lookup {
            tree_of_head: None,
            ..lookup()
        };
        assert_eq!(
            skipped(Computed::Success, &source(), &unknown),
            "0123456 is not a commit of this repository"
        );
    }

    #[test]
    fn what_is_not_final_or_not_wanted_is_not_posted() {
        let off = Lookup {
            enabled: false,
            ..lookup()
        };
        assert_eq!(skipped(Computed::Success, &source(), &off), "--no-status");
        assert_eq!(
            skipped(Computed::Cancelled, &source(), &lookup()),
            "the run is cancelled"
        );
        assert_eq!(
            skipped(Computed::Pending, &source(), &lookup()),
            "the run is pending"
        );
        let local = Lookup {
            repository: Err("no origin remote"),
            ..lookup()
        };
        assert_eq!(
            skipped(Computed::Success, &source(), &local),
            "no origin remote"
        );
    }

    #[test]
    fn a_short_head_is_named_whole() {
        let source = Source {
            original_head: Some("abc"),
            ..source()
        };
        let unknown = Lookup {
            tree_of_head: None,
            ..lookup()
        };
        assert_eq!(
            skipped(Computed::Success, &source, &unknown),
            "abc is not a commit of this repository"
        );
    }

    fn post() -> Post {
        match decide(Computed::Success, &source(), &lookup()) {
            Decision::Post(post) => post,
            Decision::Skip(why) => panic!("{why}"),
        }
    }

    #[test]
    fn the_outcome_says_posted_skipped_or_failed_and_posts_at_most_once() {
        let mut calls = Vec::new();
        let posted = outcome(Decision::Post(post()), |request| {
            calls.push(request.clone());
            Ok(())
        });
        assert_eq!(calls, [post()]);
        assert_eq!(
            posted,
            Posting::Posted {
                sha: HEAD.into(),
                state: "success".into(),
                context: "local-ci".into(),
            }
        );
        let skipped = outcome(Decision::Skip("why".into()), |_| {
            panic!("a skipped decision posts nothing")
        });
        assert_eq!(
            skipped,
            Posting::Skipped {
                reason: "why".into()
            }
        );
        let failed = outcome(Decision::Post(post()), |_| {
            Err(" gh: Bad credentials \nmore".into())
        });
        assert_eq!(
            failed,
            Posting::Failed {
                sha: HEAD.into(),
                state: "success".into(),
                context: "local-ci".into(),
                error: "gh: Bad credentials".into(),
            }
        );
    }

    #[test]
    fn a_commit_github_does_not_know_is_named_as_not_pushed() {
        assert_eq!(
            post_error(HEAD, "gh: No commit found for SHA: 0123 (HTTP 404)"),
            "0123456 is not on GitHub yet"
        );
        // Another refusal with the same code is said as it was answered.
        assert_eq!(
            post_error(HEAD, "gh: Validation Failed (HTTP 422)\nmore"),
            "gh: Validation Failed (HTTP 422)"
        );
        assert_eq!(post_error(HEAD, ""), "");
        assert_eq!(post_error(HEAD, "\nsecond"), "");
    }

    #[test]
    fn a_remote_url_names_its_host_owner_and_repository() {
        for (url, host) in [
            ("git@github.com:owner/name.git", "github.com"),
            ("github.com:owner/name", "github.com"),
            ("https://example.com/owner/name", "example.com"),
            ("https://user@example.com/owner/name.git/", "example.com"),
            ("ssh://git@example.com:22/owner/name.git\n", "example.com"),
            ("git@example.com:/owner/name", "example.com"),
        ] {
            assert_eq!(remote(url), Some((host, "owner/name".to_string())), "{url}");
        }
        for url in [
            "",
            "name",
            "/srv/git/owner/name.git",
            "../owner/name",
            "/srv/git:owner/name",
            "file:///owner/name",
            "https://example.com",
            "https://example.com/name",
            "https://example.com/group/owner/name",
            "git@example.com:name",
            "git@example.com:owner/.git",
        ] {
            assert_eq!(remote(url), None, "{url}");
        }
    }

    #[test]
    fn only_the_github_repository_the_input_came_from_is_posted_to() {
        let captured = Some("github:owner/name");
        let origin = Some("git@github.com:owner/name.git");
        assert_eq!(
            repository(captured, origin, None).as_deref(),
            Ok("owner/name")
        );
        assert_eq!(
            repository(captured, Some("https://GitHub.com/owner/name"), None).as_deref(),
            Ok("owner/name")
        );
        assert_eq!(
            repository(captured, None, None).unwrap_err(),
            "no origin remote"
        );
        assert_eq!(
            repository(captured, Some("/srv/git/owner/name"), None).unwrap_err(),
            "origin is not a repository on GitHub"
        );
        // Another forge with the same owner and name is not GitHub.
        let other = Some("git@example.com:owner/name.git");
        assert_eq!(
            repository(captured, other, None).unwrap_err(),
            "origin is on example.com, not on github.com"
        );
        // ... unless it is the GitHub host that was asked for.
        assert_eq!(
            repository(captured, other, Some("example.com")).as_deref(),
            Ok("owner/name")
        );
        assert_eq!(
            repository(captured, origin, Some("example.com")).unwrap_err(),
            "origin is on github.com, not on example.com"
        );
        // The origin was changed after the capture, or there was none then.
        for captured in [Some("github:owner/other"), Some("root:0123"), None] {
            assert_eq!(
                repository(captured, origin, None).unwrap_err(),
                "origin is owner/name now, not the repository the input was captured from"
            );
        }
    }

    #[test]
    fn the_record_is_kept_flat_and_printed_in_one_line() {
        let record = |posting| Record {
            computed: Computed::Success,
            posting,
            source_digest: Some("sha256:1".into()),
            original_head: Some(HEAD.into()),
        };
        let posted = record(Posting::Posted {
            sha: HEAD.into(),
            state: "success".into(),
            context: "local-ci".into(),
        });
        assert_eq!(
            serde_json::to_value(&posted).unwrap(),
            serde_json::json!({
                "computed": "success", "posting": "posted", "sha": HEAD,
                "state": "success", "context": "local-ci",
                "source_digest": "sha256:1", "original_head": HEAD,
            })
        );
        let again: Record = serde_json::from_value(serde_json::to_value(&posted).unwrap()).unwrap();
        assert_eq!(again, posted);
        assert_eq!(
            render(&posted),
            "status: local-ci=success posted to 0123456"
        );
        let skipped = Record {
            source_digest: None,
            original_head: None,
            ..record(Posting::Skipped {
                reason: "--no-status".into(),
            })
        };
        assert_eq!(
            serde_json::to_value(&skipped).unwrap(),
            serde_json::json!({"computed": "success", "posting": "skipped", "reason": "--no-status"})
        );
        assert_eq!(render(&skipped), "status: not posted (--no-status)");
        let failed = record(Posting::Failed {
            sha: HEAD.into(),
            state: "failure".into(),
            context: "ci".into(),
            error: "0123456 is not on GitHub yet".into(),
        });
        assert_eq!(
            render(&failed),
            "status: ci=failure could not be posted to 0123456 (0123456 is not on GitHub yet)"
        );
    }
}
