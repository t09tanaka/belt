//! `ci status <run> [--post]`: show, or post again, the GitHub status of a
//! run from what the parent recorded of it, without running anything.
//!
//! The status is decided by the same function the automatic post after `ci`
//! uses ([`super::ci_run::verdict_of`]), from the completion of the run and
//! the lines the parent collected, so what is sent is what the automatic post
//! would have sent at this point. Only the command's own process posts (the
//! credential stays with its `gh`); the attempt is kept with the run through
//! the resident like the automatic one, and `show` and `logs` read it there.

use super::{
    ci_run::{self, Ended},
    completion::Target,
    context::Context,
    github_status::{Options, Poster},
};
use crate::ci::status::{self, Decision, Record};
use crate::error::{ApiError, ErrorCode};
use crate::ids::RunId;
use serde::Serialize;
use std::sync::Arc;

/// What was sent, or would be.
#[derive(Debug, Serialize)]
pub struct Intent {
    pub repository: String,
    pub sha: String,
    pub state: &'static str,
    pub context: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct Reposted {
    pub run_id: RunId,
    /// `--post` was given: this call posted (or tried to).
    pub post: bool,
    /// The last status recorded with the run before this call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<Record>,
    pub computed: status::Computed,
    /// The post that is made (with `--post`) or would be made, when the
    /// status may be posted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send: Option<Intent>,
    /// Why it is not posted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// What became of the post, with `--post`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github_status: Option<Record>,
    /// With `--post`: whether the attempt was kept with the run. A skipped
    /// one is not, and neither is one the resident could not take.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_error: Option<String>,
}

impl Reposted {
    /// The exit code: 0 unless `--post` was asked and nothing was posted.
    pub fn ok(&self) -> bool {
        !self.post
            || matches!(
                &self.github_status,
                Some(Record {
                    posting: status::Posting::Posted { .. },
                    ..
                })
            )
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        match &self.previous {
            Some(previous) => out.push_str(&format!("previous: {}\n", status::render(previous))),
            None => out.push_str("previous: none recorded\n"),
        }
        match (&self.send, &self.reason) {
            (Some(send), _) if !self.post => out.push_str(&format!(
                "would post: {}={} to {} ({})\n",
                send.context,
                send.state,
                send.sha.get(..7).unwrap_or(&send.sha),
                send.description
            )),
            (None, Some(reason)) if !self.post => {
                out.push_str(&format!("would not post: {reason}\n"))
            }
            _ => {}
        }
        if let Some(record) = &self.github_status {
            out.push_str(&status::render(record));
            out.push('\n');
        }
        if let Some(why) = &self.record_error {
            out.push_str(&format!("not recorded with the run: {why}\n"));
        }
        out
    }
}

fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, message)
}

/// What the parent recorded of the run, read without writing anything.
struct Standing {
    previous: Option<Record>,
    end: Ended,
    expected: Vec<String>,
    jobs: Vec<ci_run::JobLine>,
}

/// A run that is being pruned has no status: it is not found (spec 25.3
/// (b)), whatever is left of its rows and files.
fn refuse_pruned(store: &crate::storage::CoordinatorStore, run: &RunId) -> Result<(), ApiError> {
    match store.run_is_pruned(run) {
        Ok(false) => Ok(()),
        Ok(true) => Err(ApiError::new(
            ErrorCode::NotFound,
            format!("run {run} not found ({})", crate::storage::RUN_PRUNED),
        )),
        Err(e) => Err(ApiError::new(ErrorCode::Internal, e.to_string())),
    }
}

fn read(context: &Context, run: &RunId) -> Result<Standing, ApiError> {
    let store = super::bootstrap::read_only_store(&context.bootstrap)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    read_from(&store, &context.bootstrap.state_dir, run)
}

/// The standing is put together from several reads of the rows and of the
/// result files. The mark is read before the first and after the last of
/// them: it is never cleared and nothing of a run is deleted before it, so a
/// run that has its row and no mark after the last read was whole at every
/// read. Otherwise the answer is "not found", and never a verdict on rows or
/// files that were being deleted (a job that "left no result", a status that "cannot be
/// decided").
fn read_from(
    store: &crate::storage::CoordinatorStore,
    state_dir: &std::path::Path,
    run: &RunId,
) -> Result<Standing, ApiError> {
    refuse_pruned(store, run)?;
    let standing = read_rows(store, state_dir, run);
    refuse_pruned(store, run)?;
    // A run whose pruning ran to its end meanwhile has no row, and no mark.
    match store.run_is_whole(run) {
        Ok(true) => standing,
        Ok(false) => Err(ApiError::new(
            ErrorCode::NotFound,
            format!("run {run} not found"),
        )),
        Err(e) => Err(ApiError::new(ErrorCode::Internal, e.to_string())),
    }
}

fn read_rows(
    store: &crate::storage::CoordinatorStore,
    state_dir: &std::path::Path,
    run: &RunId,
) -> Result<Standing, ApiError> {
    let internal = |why: String| ApiError::new(ErrorCode::Internal, why);
    let results = state_dir.join("results");
    let (expected, jobs) = ci_run::strict_report(store, state_dir, &results, run)
        .map_err(|why| invalid(format!("the status of {run} cannot be decided: {why}")))?;
    if expected.is_empty() {
        return Err(ApiError::new(
            ErrorCode::NotFound,
            format!("{run} is not a run of `ci`: there is no status to post"),
        ));
    }
    let snapshot = store
        .completion(Target::Run(run.clone()))
        .map_err(|e| internal(e.to_string()))?;
    let previous = store
        .github_status_of(run)
        .map_err(|e| internal(e.to_string()))?
        .and_then(|value| serde_json::from_value(value).ok());
    Ok(Standing {
        previous,
        end: Ended::of_snapshot(&snapshot),
        expected,
        jobs,
    })
}

/// Show the status of `run`, and with `post` post it again. A run that has
/// no status to post (it is not a run of `ci`, has not finished, was
/// cancelled, or its results can no longer be read) is refused and nothing
/// is posted or recorded.
pub async fn run(
    context: Context,
    run: RunId,
    post: bool,
    asked: Option<String>,
    poster: Arc<dyn Poster>,
) -> Result<Reposted, ApiError> {
    let (read_context, read_run) = (context.clone(), run.clone());
    let (standing, verdict) = tokio::task::spawn_blocking(move || {
        let standing = read(&read_context, &read_run)?;
        let options = Options {
            enabled: true,
            context: status::context_for(asked.as_deref(), standing.previous.as_ref()),
            read_only: !post,
        };
        let jobs: Vec<_> = standing
            .jobs
            .iter()
            .map(ci_run::JobLine::status_line)
            .collect();
        let expected: Vec<&str> = standing.expected.iter().map(String::as_str).collect();
        if let Some(why) = status::refusal(&standing.end.with(&expected, &jobs)) {
            return Err(invalid(format!("{read_run}: {why}")));
        }
        let verdict = ci_run::verdict_of(
            &read_context,
            &read_run,
            standing.end,
            &standing.expected,
            &standing.jobs,
            &options,
        );
        Ok((standing, verdict))
    })
    .await
    .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))??;
    let (send, reason) = match &verdict.decision {
        Decision::Post(sent) => (
            Some(Intent {
                repository: sent.repository.clone(),
                sha: sent.sha.clone(),
                state: sent.state,
                context: sent.context.clone(),
                description: sent.description.clone(),
            }),
            None,
        ),
        Decision::Skip(reason) => (None, Some(reason.clone())),
    };
    let computed = verdict.computed;
    let mut recorded = None;
    let mut record_error = None;
    let github_status = if post {
        let decision = verdict.decision.clone();
        let skipped = matches!(decision, Decision::Skip(_));
        let posted = tokio::task::spawn_blocking(move || {
            status::outcome(decision, |sent| poster.post(sent))
        })
        .await
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        let record = verdict.record(posted);
        // A status that was not posted for a reason of this call (another
        // checkout, a busy store) says nothing about what GitHub holds, and
        // would hide the last real attempt: it is shown, not kept.
        if skipped {
            recorded = Some(false);
        } else {
            match ci_run::keep_status(&context, run.clone(), record.clone()).await {
                Ok(()) => recorded = Some(true),
                Err(why) => {
                    eprintln!("warning: the status of the run was not recorded: {why}");
                    recorded = Some(false);
                    record_error = Some(why);
                }
            }
        }
        Some(record)
    } else {
        None
    };
    Ok(Reposted {
        run_id: run,
        post,
        previous: standing.previous,
        computed,
        send,
        reason,
        github_status,
        recorded,
        record_error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::status::{Computed, Posting};

    fn shown(post: bool, posting: Option<Posting>) -> Reposted {
        Reposted {
            run_id: RunId::generate(),
            post,
            previous: None,
            computed: Computed::Success,
            send: Some(Intent {
                repository: "owner/name".into(),
                sha: "0123456789abcdef".into(),
                state: "success",
                context: "local-ci".into(),
                description: "success: pass 1 / fail 0 / error 0 / skip 0".into(),
            }),
            reason: None,
            recorded: None,
            record_error: None,
            github_status: posting.map(|posting| Record {
                computed: Computed::Success,
                posting,
                source_digest: None,
                original_head: None,
            }),
        }
    }

    #[test]
    fn showing_is_ok_and_a_post_is_ok_only_when_it_went_through() {
        assert!(shown(false, None).ok());
        let skipped = Posting::Skipped { reason: "x".into() };
        let failed = Posting::Failed {
            sha: "0123456789abcdef".into(),
            state: "success".into(),
            context: "local-ci".into(),
            error: "x".into(),
        };
        let posted = Posting::Posted {
            sha: "0123456789abcdef".into(),
            state: "success".into(),
            context: "local-ci".into(),
        };
        assert!(!shown(true, Some(skipped)).ok());
        assert!(!shown(true, Some(failed)).ok());
        assert!(shown(true, Some(posted)).ok());
    }

    #[test]
    fn the_text_says_what_would_be_or_was_posted() {
        let mut view = shown(false, None);
        assert_eq!(
            view.render(),
            "previous: none recorded\nwould post: local-ci=success to 0123456 (success: pass 1 / fail 0 / error 0 / skip 0)\n"
        );
        view.send = None;
        view.reason = Some("--no-status".into());
        assert!(view.render().ends_with("would not post: --no-status\n"));
        let posted = shown(
            true,
            Some(Posting::Posted {
                sha: "0123456789abcdef".into(),
                state: "success".into(),
                context: "local-ci".into(),
            }),
        );
        assert_eq!(
            posted.render(),
            "previous: none recorded\nstatus: local-ci=success posted to 0123456\n"
        );
    }

    /// Spec 25.3 (b). A run that is being pruned has no status to show or
    /// to post: it is not found, also when rows of it are already gone, and
    /// never judged from what is left. The post is decided from this read
    /// alone, so nothing is posted.
    #[test]
    fn a_run_that_is_being_pruned_has_no_status_whatever_is_left_of_its_rows() {
        use crate::ids::TaskId;
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = crate::storage::CoordinatorStore::open(dir.path(), 1).unwrap();
        let (run, first, second) = (RunId::generate(), TaskId::generate(), TaskId::generate());
        let spec = serde_json::json!({});
        store.create_run(&run, None, None, &spec, 1).unwrap();
        store.create_task(&first, &run, &spec, 1).unwrap();
        store.create_task(&second, &run, &spec, 1).unwrap();
        let reader = crate::storage::CoordinatorStore::open_read_only(dir.path()).unwrap();
        let read = || read_from(&reader, dir.path(), &run).err().unwrap();
        // Without the mark the run is read, and judged by what it is.
        assert!(!read().message.contains(crate::storage::RUN_PRUNED));
        store.mark_pruning_for_test(&run);
        for delete in [None, Some(&second)] {
            if let Some(task) = delete {
                store.delete_task_for_test(task);
            }
            let error = read();
            assert_eq!(error.code, ErrorCode::NotFound);
            assert!(
                error.message.contains(crate::storage::RUN_PRUNED),
                "{error:?}"
            );
        }
    }
}
