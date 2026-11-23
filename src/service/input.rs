//! One immutable, private input capture shared by command submission and CI planning.
use super::context::Context;
use crate::error::{ApiError, ErrorCode};
use crate::ids::RunId;
use crate::secrets::{
    capture::{capture_run, SecretInputs},
    vault::Vault,
};
use crate::source::{CaptureParams, Captured, SourceRequest, SourceStore};

/// The revision is explicit when configured to capture a commit; it never silently becomes HEAD.
pub fn capture(
    context: &Context,
    run: &RunId,
    revision: Option<&str>,
) -> Result<Captured, ApiError> {
    capture_at(context, run, revision, context.state_dir(), true)
}

/// The owner of the capture of `run` will not submit it (spec 25.4 (b)): its
/// capture failed, its enqueue was refused, or it was interrupted. The row
/// stays for the resident, which deletes it and what it names. Best effort:
/// when this cannot be written, the end of this process says the same.
pub fn give_up(state: &std::path::Path, run: &RunId) {
    if !state.join("sources").join("retention.db").is_file() {
        return;
    }
    if let Ok((store, _source)) = SourceStore::open_foreground(state) {
        let _ = store.give_up_capture(run);
    }
}

/// [`give_up`] after a submission ended with `error`, off the async thread.
/// An unconfirmed submission is not given up: its enqueue may still commit.
pub async fn give_up_after(state: std::path::PathBuf, run: RunId, error: &ApiError) {
    if error.code == ErrorCode::CommunicationUnknown {
        return;
    }
    give_up_now(state, run).await;
}

pub async fn give_up_now(state: std::path::PathBuf, run: RunId) {
    let _ = tokio::task::spawn_blocking(move || give_up(&state, &run)).await;
}

/// Whether this command has written the owner row of the capture of `run`.
/// Before it has, nothing of the capture exists and there is nothing to
/// cancel.
pub async fn owns_capture(state: std::path::PathBuf, run: RunId) -> bool {
    tokio::task::spawn_blocking(move || {
        state.join("sources").join("retention.db").is_file()
            && SourceStore::open_foreground(&state)
                .and_then(|(store, _source)| store.capture_owner(&run))
                .is_ok_and(|row| row.is_some())
    })
    .await
    .unwrap_or(false)
}

/// Use the same capture path with an explicitly owned source/Vault destination.
/// Authentication and runtime identity continue to come from the original context.
/// The destination is the owner of what is captured (a plan draft: its
/// directory is removed as a whole), so no `capture_owner` row is written.
pub fn capture_into(
    context: &Context,
    run: &RunId,
    revision: Option<&str>,
    input_state: &std::path::Path,
) -> Result<Captured, ApiError> {
    capture_at(context, run, revision, input_state, false)
}

/// `own`: write the owner row of the capture first (spec 25.4 (b)). The
/// store's own fixed files exist before it; nothing of this capture does:
/// not the run lock, not the pending root, not an object, not the secret
/// bundle.
fn capture_at(
    context: &Context,
    run: &RunId,
    revision: Option<&str>,
    input_state: &std::path::Path,
    own: bool,
) -> Result<Captured, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    let request = match (context.repo_root.as_ref(), context.config.source_mode.value) {
        (None, crate::config::model::SourceMode::Commit) => {
            return Err(invalid(
                "commit snapshot requires a repository and an explicit revision".into(),
            ))
        }
        (None, _) if revision.is_some() => {
            return Err(invalid("a revision requires a repository".into()))
        }
        (None, _) => SourceRequest::Directory {
            dir: context.cwd.clone(),
        },
        (Some(_), crate::config::model::SourceMode::Worktree) if revision.is_none() => {
            SourceRequest::Worktree {
                dir: context.cwd.clone(),
            }
        }
        (Some(_), _) => SourceRequest::Commit {
            dir: context.cwd.clone(),
            rev: revision
                .ok_or_else(|| invalid("commit snapshot requires an explicit revision".into()))?
                .to_string(),
        },
    };
    let (store, _source) = SourceStore::open_foreground(input_state).map_err(|error| {
        if error.is::<crate::source::retention::ForegroundBusy>() {
            crate::source::classify::api_error_of(&error)
        } else {
            invalid(error.to_string())
        }
    })?;
    if own {
        store.own_capture(run).map_err(|error| {
            ApiError::new(
                ErrorCode::Internal,
                format!("cannot record the owner of the capture: {error:#}"),
            )
        })?;
    }
    let vault = Vault::open(input_state).map_err(|e| e.to_api())?;
    let key =
        crate::secrets::key::load(&context.bootstrap.cluster_key_path).map_err(|e| e.to_api())?;
    let declared: Vec<_> = context
        .config
        .secret_files
        .value
        .iter()
        .map(|s| local_ci::source::SecretSpec {
            glob: s.glob.clone(),
            required: s.required,
        })
        .collect();
    capture_run(
        &context.env,
        &store,
        &vault,
        &SecretInputs {
            key: &key,
            declared: &declared,
        },
        run,
        &request,
        &CaptureParams {
            include: context.config.include.value.clone(),
            exclude: context.config.exclude.value.clone(),
            ..Default::default()
        },
    )
}

/// Materialize a previously bound input for read-only planning; never recapture.
/// The caller owns the destination and must not execute project hooks in this view.
pub fn source_view(
    context: &Context,
    run: &RunId,
    destination: &std::path::Path,
) -> Result<(), ApiError> {
    let manifest = source_manifest(context, run)?;
    SourceStore::open_foreground(context.state_dir())
        .and_then(|(store, _source)| store.materialize(&manifest, destination))
        .map_err(|error| {
            if error.is::<crate::source::retention::ForegroundBusy>() {
                crate::source::classify::api_error_of(&error)
            } else {
                ApiError::new(ErrorCode::Internal, "cannot materialize captured source")
            }
        })
}

/// Load the bound descriptor without recapturing or restoring secrets.
pub fn source_manifest(
    context: &Context,
    run: &RunId,
) -> Result<local_ci::source::SourceManifest, ApiError> {
    source_manifest_in(context.state_dir(), run)
}

/// [`source_manifest`] for a caller that must not create anything: when this
/// host holds no source store, the input is not available.
pub fn source_manifest_existing(
    context: &Context,
    run: &RunId,
) -> Result<local_ci::source::SourceManifest, ApiError> {
    if !context
        .state_dir()
        .join("sources")
        .join("retention.db")
        .is_file()
    {
        return Err(ApiError::new(
            ErrorCode::NotFound,
            "this host holds no captured input",
        ));
    }
    source_manifest(context, run)
}

/// Resolve the same bound descriptor from an explicitly owned input store.
pub fn source_manifest_in(
    input_state: &std::path::Path,
    run: &RunId,
) -> Result<local_ci::source::SourceManifest, ApiError> {
    let internal = |_| ApiError::new(ErrorCode::Internal, "captured source is unavailable");
    let (store, _source) = SourceStore::open_foreground(input_state).map_err(|error| {
        if error.is::<crate::source::retention::ForegroundBusy>() {
            crate::source::classify::api_error_of(&error)
        } else {
            internal(error)
        }
    })?;
    let source = store
        .source_of_run(run)
        .map_err(internal)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "run input is unavailable"))?;
    store
        .manifest(&source)
        .map_err(internal)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "source manifest is unavailable"))
}
