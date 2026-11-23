//! Input snapshots (working tree, commit, directory) and transfer (spec
//! chapter 6).
//!
//! The capture itself is local-ci's staged capture (verified copy, at most
//! three passes, an original that is never written): every new file of the
//! store is written under the capture's own scratch directory, recorded as a
//! reservation of the capture's pending root, and only then installed (spec
//! 25.4 (b)).
//! This module puts them under the runner's state directory, binds a capture
//! to a run (one capture per run), maps refusals to the runner's error codes,
//! and adds the receiving side: difference planning, verified blob intake,
//! and path-safe unpacking.
//!
//! All of it is blocking (Git, file I/O): call it from a dedicated thread or
//! `spawn_blocking`.

pub mod classify;
mod commit;
pub mod diff;
pub mod manifest;
pub mod retention;
pub mod safety;
pub mod store;

use crate::error::{ApiError, ErrorCode};
use crate::ids::RunId;
use local_ci::source::{
    capture_directory_staged, capture_source_staged, CaptureOptions, Hook, Intent, SecretSpec,
    SourceKind, StagedInstallError, Staging,
};
use local_ci::ExecContext;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub use manifest::SourceManifest;
pub use store::{
    Inventory, ObjectsRepo, Peer, RequestSummary, RunRecord, SourceStore, TransferPlan,
};

/// What to capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRequest {
    /// The repository containing `dir`, as it is now (the default).
    Worktree { dir: PathBuf },
    /// The commit `rev` of the repository containing `dir`, as committed.
    Commit { dir: PathBuf, rev: String },
    /// A directory that is not a Git repository.
    Directory { dir: PathBuf },
}

impl SourceRequest {
    pub fn kind(&self) -> SourceKind {
        match self {
            SourceRequest::Worktree { .. } => SourceKind::Worktree,
            SourceRequest::Commit { .. } => SourceKind::Commit,
            SourceRequest::Directory { .. } => SourceKind::Directory,
        }
    }
}

#[derive(Clone, Default)]
pub struct CaptureParams {
    /// The base the logical context (`base`, `merge_base`) is taken against.
    pub base: Option<String>,
    /// Ignored, untracked files to take along (globs, repo-root relative).
    pub include: Vec<String>,
    /// Files to leave out (globs).
    pub exclude: Vec<String>,
    /// Passes before `SOURCE_BUSY`; clamped to `1..=3`.
    pub max_attempts: Option<u32>,
    /// A test seam, called between the steps of a capture (see
    /// [`local_ci::source::Stage`]).
    pub hook: Option<Hook>,
    /// Ignored files to send as secrets (chapter 22.3); built by
    /// [`crate::secrets::capture`], which adds what `.worktreeinclude` matches.
    pub secret_files: Vec<SecretSpec>,
    /// The cluster key the secret files' HMACs are made with.
    pub secret_key: Vec<u8>,
    /// Where local-ci writes the secret bundle: a path of the run, so that two
    /// runs of the same source never share a file. Created, never replaced
    /// (the directory must exist). `None`: the store's default, keyed by
    /// source.
    pub secret_bundle: Option<PathBuf>,
}

impl std::fmt::Debug for CaptureParams {
    /// The key is not shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureParams")
            .field("base", &self.base)
            .field("include", &self.include)
            .field("exclude", &self.exclude)
            .field("max_attempts", &self.max_attempts)
            .field("secret_files", &self.secret_files)
            .field("secret_bundle", &self.secret_bundle)
            .field("secret_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// A source that is fixed and published.
#[derive(Debug, Clone)]
pub struct Captured {
    pub manifest: SourceManifest,
    /// Where local-ci put the secret files of this capture (0600, in the
    /// source store, keyed by source). `None` when there are none, and for a
    /// run whose capture was made before: its bundle is already with the run.
    pub secrets: Option<PathBuf>,
    /// Things worth saying that did not stop the capture.
    pub warnings: Vec<String>,
    pub(crate) retention_root: Option<String>,
}

/// Capture `request` into `store`. Never writes the original. When the input
/// kept changing for the whole attempt budget the error is `SOURCE_BUSY`, and
/// nothing is published.
pub fn capture(
    env: &BTreeMap<String, String>,
    store: &SourceStore,
    request: &SourceRequest,
    params: &CaptureParams,
) -> Result<Captured, ApiError> {
    capture_of(env, store, None, request, params)
}

/// [`capture`] for the capture of `run`: the owner row of the run, when it
/// has one, names the pending root from the transaction that creates it
/// (spec 25.4 (b)).
fn capture_of(
    env: &BTreeMap<String, String>,
    store: &SourceStore,
    run: Option<&RunId>,
    request: &SourceRequest,
    params: &CaptureParams,
) -> Result<Captured, ApiError> {
    let _mutation = store
        .foreground_guard()
        .map_err(|e| classify::api_error_of(&e))?;
    _mutation
        .ensure_sdk_available()
        .map_err(|e| classify::api_error_of(&e))?;
    let retention_root = store
        .capture_pending_root(run)
        .map_err(|e| classify::api_error_of(&e))?;
    retention::stop_point("pending");
    // The root, and with it the name of the scratch directory, is committed:
    // from here on everything this capture writes is under that directory or
    // reserved by that root.
    let scratch = Scratch::create(store, &retention_root)?;
    let reserved = std::sync::Arc::new(std::sync::Mutex::new(retention::Reserved::new()));
    let staging = Staging {
        scratch_dir: scratch.path.clone(),
        before_install: {
            let (index, root, reserved) = (
                store.retention.clone(),
                retention_root.clone(),
                reserved.clone(),
            );
            std::sync::Arc::new(move |intents: &[Intent]| {
                retention::stop_point("staged");
                let rows = index.reserve_capture(&root, intents)?;
                *reserved.lock().unwrap_or_else(|e| e.into_inner()) = rows;
                retention::stop_point("reserved");
                Ok(())
            })
        },
    };
    let ctx = ExecContext::new(env.clone());
    let opts = CaptureOptions {
        store: Some(store.root().to_path_buf()),
        base: params.base.clone(),
        include: params.include.clone(),
        exclude: params.exclude.clone(),
        secret_files: params.secret_files.clone(),
        secret_key: params.secret_key.clone(),
        secret_root: None,
        secret_bundle: params.secret_bundle.clone(),
        max_attempts: classify::attempt_budget(params.max_attempts),
        hook: params.hook.clone(),
    };
    let staged = match request {
        SourceRequest::Worktree { dir } => capture_source_staged(&ctx, dir, &opts, &staging),
        SourceRequest::Commit { dir, rev } => {
            // the checkout is a part of this capture: it goes with the
            // scratch directory, whoever removes that
            let co = commit::checkout(env, &scratch.path, dir, rev, params.base.as_deref())
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, format!("{e:#}")))?;
            // the scratch repository has the base as a commit, not as a ref
            // the clean checkout has no ignored files: the secret files are
            // chosen and read in the worktree the commit came from
            let o = CaptureOptions {
                base: co.base.clone(),
                secret_root: Some(dir.clone()),
                ..opts
            };
            capture_source_staged(&ctx, &co.cwd, &o, &staging)
        }
        SourceRequest::Directory { dir } => capture_directory_staged(&ctx, dir, &opts, &staging),
    };
    let staged = match staged {
        Ok(staged) => staged,
        Err(error) => {
            // An install that stopped part-way created only what it lists as
            // installed. The rest is given back now: a file that was there
            // already (a dedup hit, or the one with other content that
            // stopped the install) is not this capture's, and one that was
            // not reached does not exist. What was installed stays reserved,
            // for the abandonment to release.
            if let Some(part) = error.downcast_ref::<StagedInstallError>() {
                let rows = reserved.lock().unwrap_or_else(|e| e.into_inner());
                let installed: std::collections::HashSet<&PathBuf> =
                    part.installed.iter().collect();
                let keys: Vec<&str> = rows
                    .iter()
                    .filter(|(to, _)| !installed.contains(to))
                    .map(|(_, key)| key.as_str())
                    .collect();
                // best effort, on the way out of an error: a reservation
                // that stays is released by the abandonment
                let _ = store.retention.unreserve_capture(&retention_root, &keys);
            }
            return Err(classify::api_error_of(&error));
        }
    };
    retention::stop_point("installed");
    let snap = staged.snapshot;
    // The scratch directory is removed by the capture that made it: after
    // the hand-over nothing names it.
    if let Err(error) = scratch.remove() {
        remove_secret_copy(snap.secrets.as_deref());
        return Err(classify::api_error_of(&error));
    }
    // from here on the store holds a plaintext secret bundle: every way out
    // that does not hand it to the caller removes it
    let ci_manifest = std::sync::Arc::new(snap.manifest);
    let checked = refuse_uninitialized_submodules(store, &ci_manifest).and_then(|()| {
        SourceManifest::from_ci(&ci_manifest, request.kind()).ok_or_else(|| {
            ApiError::new(
                ErrorCode::Internal,
                format!("unexpected source id {}", ci_manifest.source_id),
            )
        })
    });
    let manifest = match checked {
        Ok(m) => m,
        Err(e) => {
            remove_secret_copy(snap.secrets.as_deref());
            return Err(e);
        }
    };
    store
        .retention
        .register_held(&retention_root, "pending", ci_manifest)
        .map_err(|e| classify::api_error_of(&e))?;
    Ok(Captured {
        retention_root: Some(retention_root),
        manifest,
        secrets: snap.secrets,
        warnings: snap.warnings,
    })
}

/// The scratch directory of one capture (`staging/<ulid>` of the store, named
/// after the pending root). A capture that returns removes it; one that is
/// killed leaves it to the abandonment, which names it from the owner row.
struct Scratch {
    index: retention::Index,
    root: String,
    path: PathBuf,
    removed: bool,
}

impl Scratch {
    fn create(store: &SourceStore, root: &str) -> Result<Self, ApiError> {
        let path = store
            .retention
            .create_scratch(root)
            .map_err(|e| classify::api_error_of(&e))?;
        Ok(Self {
            index: store.retention.clone(),
            root: root.to_owned(),
            path,
            removed: false,
        })
    }
    fn remove(mut self) -> anyhow::Result<()> {
        self.removed = true;
        self.index.remove_own_scratch(&self.root)
    }
}

impl Drop for Scratch {
    /// The way out of an error, which this must not replace.
    fn drop(&mut self) {
        if !self.removed {
            let _ = self.index.remove_own_scratch(&self.root);
        }
    }
}

/// Remove the store's plaintext secret bundle, if there is one. Best effort:
/// it is called on the way out of an error, which it must not replace; a copy
/// that stays is found by the sweep at start
/// ([`crate::secrets::capture::sweep_store_copies`]).
fn remove_secret_copy(path: Option<&Path>) {
    if let Some(p) = path {
        let _ = std::fs::remove_file(p);
    }
}

/// A submodule that was not initialized in the original has no files to
/// capture, only a revision: a source made so would not hold what the
/// repository holds, so it is refused, with the path. (The refused capture
/// leaves its manifest and objects in the store, reserved by its pending
/// root; the abandonment of the capture releases them.)
fn refuse_uninitialized_submodules(
    store: &SourceStore,
    m: &local_ci::source::SourceManifest,
) -> Result<(), ApiError> {
    let internal = |e: anyhow::Error| classify::api_error_of(&e);
    for s in store.closure(m).map_err(internal)? {
        if let Some(sub) = s.submodules.iter().find(|x| x.source_id.is_none()) {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                format!(
                    "submodule {} is not initialized (run `git submodule update --init` there): a source needs its files, not only its revision",
                    sub.path.display()
                ),
            ));
        }
    }
    Ok(())
}

/// The part of a request that decides what is captured, for the run record.
fn summarize(request: &SourceRequest, params: &CaptureParams) -> RequestSummary {
    RequestSummary {
        kind: request.kind(),
        rev: match request {
            SourceRequest::Commit { rev, .. } => Some(rev.clone()),
            _ => None,
        },
        base: params.base.clone(),
        include: params.include.clone(),
        exclude: params.exclude.clone(),
    }
}

/// The capture of a run: made once, then the same source for every later
/// call (a re-dispatch, a late worker, a retry after a crash). The source is
/// bound to the run after it is published, so a record always names a source
/// that can be loaded.
pub fn capture_for_run(
    env: &BTreeMap<String, String>,
    store: &SourceStore,
    run: &RunId,
    request: &SourceRequest,
    params: &CaptureParams,
) -> Result<Captured, ApiError> {
    let internal = |e: anyhow::Error| classify::api_error_of(&e);
    let from_ci = |m: &local_ci::source::SourceManifest| {
        SourceManifest::from_ci(m, request.kind()).ok_or_else(|| {
            ApiError::new(
                ErrorCode::Internal,
                format!("bad source id {}", m.source_id),
            )
        })
    };
    let _mutation = store.foreground_guard().map_err(internal)?;
    _mutation.ensure_run_available(run).map_err(internal)?;
    _mutation.ensure_sdk_available().map_err(internal)?;
    // one capture of a run at a time, in this process and in others
    let _lock = store.lock_run(run).map_err(internal)?;
    retention::stop_point("locked");
    let wanted = RunRecord {
        ci_source_id: String::new(),
        source_kind: request.kind(),
        request: summarize(request, params),
    };
    if let Some(rec) = store.run_record(run).map_err(internal)? {
        if rec.source_kind != wanted.source_kind || rec.request != wanted.request {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "run {run} already has a source ({}) captured for {:?}; this request is {:?}",
                    rec.ci_source_id, rec.request, wanted.request
                ),
            ));
        }
        // the record names the source: a manifest that is gone is damage, not
        // a reason to capture again (the working tree may have moved on)
        let m = store
            .manifest(&rec.ci_source_id)
            .map_err(internal)?
            .ok_or_else(|| {
                ApiError::new(
                    ErrorCode::Internal,
                    format!(
                        "run {run} is bound to {}, whose manifest is missing from the store",
                        rec.ci_source_id
                    ),
                )
            })?;
        store
            .retention
            .register_held(run.as_str(), "run", std::sync::Arc::new(m.clone()))
            .map_err(internal)?;
        return Ok(Captured {
            retention_root: None,
            manifest: from_ci(&m)?,
            secrets: None,
            warnings: vec![],
        });
    }
    // no record binds the run yet, so a bundle at its path is the leftover of
    // a capture that did not get to bind: local-ci creates the file and never
    // replaces one
    if let Some(p) = &params.secret_bundle {
        match std::fs::remove_file(p) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(ApiError::new(
                    ErrorCode::Internal,
                    format!("secret bundle {}: {e}", p.display()),
                ))
            }
            _ => {}
        }
    }
    let mut got = capture_of(env, store, Some(run), request, params)?;
    let record = RunRecord {
        ci_source_id: got.manifest.ci_source_id.clone(),
        ..wanted
    };
    if let Some(pending) = got.retention_root.as_deref() {
        store
            .retention
            .copy_root(pending, run.as_str(), "run", &record.ci_source_id)
            .map_err(internal)?;
    }
    retention::stop_point("copied");
    let bound = match store.bind_run(run, &record) {
        Ok(b) => b,
        Err(e) => {
            remove_secret_copy(got.secrets.as_deref());
            return Err(internal(e));
        }
    };
    retention::stop_point("bound");
    if bound != record {
        remove_secret_copy(got.secrets.as_deref());
        return Err(ApiError::new(
            ErrorCode::Internal,
            format!(
                "run {run} was bound to {} while its capture was held",
                bound.ci_source_id
            ),
        ));
    }
    if let Some(pending) = got.retention_root.take() {
        store
            .retention
            .handoff(&pending, run.as_str())
            .map_err(internal)?;
    }
    retention::stop_point("handed_off");
    Ok(got)
}
