//! The real [`Host`]: the source store, fast-worktree workspaces and the
//! secret files of this worker's state directory.
//!
//! The source an attempt names has to be in the worker's store before the
//! attempt is prepared; it gets there through the blob operations of the
//! protocol ([`super::blobs`]).

use super::blobs::{BlobSink, Incoming};
use super::host::Host;
use crate::error::{ApiError, ErrorCode};
use crate::ids::AttemptId;
use crate::platform;
use crate::secrets::capture::sweep_store_copies;
use crate::secrets::key;
use crate::secrets::place::Placed;
use crate::secrets::place::WorkerSecrets;
use crate::secrets::vault::Vault;
use crate::secrets::SecretError;
use crate::setup::builder::{BuilderWorkspaces, SetupConfig, SetupPoll, Setups};
use crate::source::classify::ci_id_of;
use crate::source::SourceStore;
use crate::storage::AcceptedAttempt;
use crate::wire::{AttemptPayload, SecretFile};
use crate::workspace::{CliFastWorktree, Workspace, Workspaces};
use local_ci::source::SourceManifest as CiManifest;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Machine {
    in_place: Option<(PathBuf, crate::ids::WorkerId)>,
    state_dir: PathBuf,
    env: BTreeMap<String, String>,
    fw: Arc<CliFastWorktree>,
    setups: Option<Arc<Setups>>,
    store: SourceStore,
    workspaces: Workspaces,
    secrets: WorkerSecrets,
    key_path: PathBuf,
    incoming: Incoming,
}

fn internal(what: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, format!("{what}: {e}"))
}

impl Machine {
    /// `env` is what Git, fast-worktree and the project's hooks run with
    /// (`HOME` and `PATH` at least); `key_path` is the cluster key file.
    ///
    /// Refused (`BLOCKED`, `environment_mismatch`) when that Git cannot be
    /// run or is older than 2.31.
    pub fn open(
        state_dir: &Path,
        env: BTreeMap<String, String>,
        key_path: PathBuf,
    ) -> Result<Self, ApiError> {
        let store = SourceStore::open(state_dir).map_err(|e| internal("source store", e))?;
        let secrets = WorkerSecrets::open(state_dir).map_err(|e| e.to_api())?;
        let fw = Arc::new(CliFastWorktree::new(env.clone()));
        let workspaces = Workspaces::new(state_dir.to_path_buf(), env.clone(), fw.clone());
        // Before anything is served: a Git that cannot unpack a bundle would
        // answer for every source as if the source were at fault.
        workspaces.check_git()?;
        let incoming = Incoming::new(
            state_dir,
            store.clone(),
            Workspaces::new(state_dir.to_path_buf(), env.clone(), fw.clone()),
            env.clone(),
        );
        Ok(Machine {
            in_place: None,
            incoming,
            setups: None,
            env,
            fw,
            state_dir: state_dir.to_path_buf(),
            store,
            workspaces,
            secrets,
            key_path,
        })
    }

    /// The manifest of the source the attempt's payload names, and only if
    /// it is the manifest the payload names.
    fn manifest(&self, attempt: &AcceptedAttempt) -> Result<CiManifest, ApiError> {
        let payload: AttemptPayload = serde_json::from_value(attempt.spec.clone())
            .map_err(|e| internal("stored payload", e))?;
        self.manifest_of(&payload)
    }

    fn manifest_of(&self, payload: &AttemptPayload) -> Result<CiManifest, ApiError> {
        manifest_in(&self.store, payload)
    }

    /// Prepare the setup steps of attempts once per worker (spec chapter 8):
    /// `cfg` says how builders are run. Without it a payload's setup is not
    /// looked at and every attempt runs all of its commands. Before the
    /// agent starts: it clears what builders of an earlier run left.
    pub fn with_setup(mut self, cfg: SetupConfig) -> Result<Self, ApiError> {
        let ws = BuilderWs {
            store: self.store.clone(),
            workspaces: Workspaces::new(self.state_dir.clone(), self.env.clone(), self.fw.clone()),
        };
        self.setups = Some(Setups::open(cfg, Arc::new(ws))?);
        Ok(self)
    }

    pub(crate) fn with_in_place(mut self, parent: PathBuf, worker: crate::ids::WorkerId) -> Self {
        self.in_place = Some((parent, worker));
        self
    }
    fn original(&self, payload: &AttemptPayload) -> Result<PathBuf, ApiError> {
        let (parent, worker) = self.in_place.as_ref().ok_or_else(|| {
            ApiError::new(
                ErrorCode::InvalidInput,
                "in-place requires the parent local worker",
            )
        })?;
        Ok(crate::workspace::in_place::load(
            parent,
            &payload.run_id,
            &payload.source_id,
            &payload.manifest_digest,
            worker,
        )?
        .root)
    }
    fn original_secrets(&self, attempt: &AcceptedAttempt) -> Result<Option<Placed>, ApiError> {
        let payload: AttemptPayload = serde_json::from_value(attempt.spec.clone())
            .map_err(|e| internal("stored payload", e))?;
        if !payload.in_place {
            return Ok(None);
        }
        self.original(&payload)?;
        let m = self.manifest(attempt)?;
        let paths = m
            .secret_files
            .iter()
            .map(|s| s.path.display())
            .collect::<Vec<_>>();
        let (parent, worker) = self.in_place.as_ref().expect("original checked capability");
        let binding = crate::workspace::in_place::load(
            parent,
            &payload.run_id,
            &payload.source_id,
            &payload.manifest_digest,
            worker,
        )?;
        crate::workspace::in_place::read_secrets(&binding, &self.env, &paths).map(Some)
    }
    pub fn setups(&self) -> Option<&Arc<Setups>> {
        self.setups.as_ref()
    }
}

/// A frozen bundle is taken only when it is what this worker reads from the
/// captured source itself: the scope, and the requirements in it.
pub fn check_toolchain_source(
    store: &SourceStore,
    state_dir: &Path,
    m: &CiManifest,
    payload: &AttemptPayload,
) -> Result<(), ApiError> {
    let Some(bundle) = &payload.toolchains else {
        return Ok(());
    };
    let invalid = |e: &str| ApiError::new(crate::error::ErrorCode::InvalidInput, e);
    bundle.validate(&payload.manifest_digest).map_err(invalid)?;
    let project =
        std::str::from_utf8(&m.cwd_relative.0).map_err(|_| invalid("toolchain project scope"))?;
    // A plain command is frozen from the project's version files. A
    // CI task is frozen from those and the setup steps of its own
    // jobs: which jobs, and the values of its matrix variant, come
    // from the delivered plan, never from the bundle being checked.
    let Some(ci) = &payload.ci_task else {
        if bundle.frozen.scope != crate::toolchain::versions::Scope::project(project) {
            return Err(invalid("toolchain source scope mismatch"));
        }
        let declarations = crate::toolchain::snapshot::read_captured(store, m, project, &[])
            .map_err(|e| invalid(&e.code))?;
        let effective = crate::toolchain::versions::effective(&declarations, &bundle.frozen.scope)
            .map_err(|e| invalid(&e.code))?;
        if effective != bundle.frozen.requirements {
            return Err(invalid("toolchain source declarations mismatch"));
        }
        return Ok(());
    };
    let (plan, task) = super::ci_task::plan_task(state_dir, ci)?;
    // The function the parent froze with, and the check the parent made of
    // its own bundle: what it left out as not interpreted is left out here
    // as well.
    let source = crate::ci::toolchains::read_task(store, m, project, &task.workflow)
        .map_err(|e| invalid(&e.code))?;
    crate::ci::toolchains::check_frozen(&source, &plan, &task, project, &bundle.frozen)
        .map_err(|e| invalid(&e))
}

/// The manifest of the source the payload names, and only if it is the
/// manifest the payload names.
fn manifest_in(store: &SourceStore, payload: &AttemptPayload) -> Result<CiManifest, ApiError> {
    let missing = || {
        ApiError::new(
            ErrorCode::NotFound,
            format!("source {} is not in this worker's store", payload.source_id),
        )
    };
    let source = ci_id_of(&payload.source_id).ok_or_else(missing)?;
    let m = store
        .manifest(&source)
        .map_err(|e| internal("source store", format!("{e:#}")))?
        .ok_or_else(missing)?;
    let want = payload.manifest_digest.as_str();
    if m.manifest_digest != want && format!("sha256:{}", m.manifest_digest) != want {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            format!(
                "source {} has another manifest than the attempt names",
                payload.source_id
            ),
        ));
    }
    Ok(m)
}

/// The workspaces of the setup builders: made like an attempt's.
struct BuilderWs {
    store: SourceStore,
    workspaces: Workspaces,
}

impl BuilderWorkspaces for BuilderWs {
    fn create(&self, attempt: &AttemptId, payload: &AttemptPayload) -> Result<Workspace, ApiError> {
        let m = manifest_in(&self.store, payload)?;
        let repo = match (&m.execution_commit, &m.repository_id) {
            (Some(_), Some(id)) => Some(self.workspaces.ensure_clone(id, None)?),
            _ => None,
        };
        self.workspaces
            .create(attempt, &self.store, &m, repo.as_ref())
    }

    fn remove(&self, ws: &Workspace) -> Result<(), ApiError> {
        self.workspaces.remove(ws)
    }
}

impl Host for Machine {
    fn setup_owner(&self) -> Option<&Setups> {
        self.setups.as_deref()
    }
    fn preparation_observation_page(
        &self,
        request: &crate::setup::observation_transport::ObservationPageRequest,
    ) -> Option<crate::setup::observation_transport::ObservationPage> {
        self.setups
            .as_ref()
            .map(|setups| setups.preparation_observation_page(request))
    }
    fn toolchain_registry(
        &self,
        profile: &crate::toolchain::profile::Profile,
        base: crate::ids::Digest,
    ) -> Result<Option<crate::toolchain::registry::Registry>, ApiError> {
        let Some(setups) = &self.setups else {
            return Ok(None);
        };
        let root = setups.private_tool_root()?;
        crate::toolchain::registry::Registry::load(
            root.parent().expect("tool root parent"),
            profile,
            base,
        )
        .map(Some)
        .map_err(|e| ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, e))
    }
    fn toolchain_retry(&self, request: &crate::toolchain::preparation::PreparationRequest) {
        if let Some(s) = &self.setups {
            s.retry_tool(request);
        }
    }
    fn toolchain_poll(
        &self,
        attempt: &AttemptId,
        request: &crate::toolchain::preparation::PreparationRequest,
    ) -> Result<crate::toolchain::preparation::ToolPreparationPoll, ApiError> {
        self.setups
            .as_ref()
            .ok_or_else(|| {
                ApiError::blocked(
                    crate::error::BlockedReason::EnvironmentMismatch,
                    "toolchain preparation unavailable",
                )
            })?
            .poll_tool(attempt, request)
    }
    fn setup_poll_bound(
        &self,
        attempt: &AcceptedAttempt,
        payload: &AttemptPayload,
        workspace: &Workspace,
        binding: Option<&crate::toolchain::contract::ExecutionBinding>,
    ) -> Result<SetupPoll, ApiError> {
        match &self.setups {
            Some(s) => s.poll_bound(&attempt.attempt_id, payload, workspace, binding),
            None => self.setup_poll(attempt, payload, workspace),
        }
    }

    fn admit(&self, payload: &AttemptPayload) -> Result<(), ApiError> {
        if payload.in_place {
            self.original(payload)?;
            if payload.has_secret_bundle {
                return Err(ApiError::new(
                    ErrorCode::InvalidInput,
                    "in-place never accepts placed secrets",
                ));
            }
        }
        Ok(())
    }
    fn disable_core_dumps(&self) -> Result<(), ApiError> {
        platform::disable_core_dumps().map_err(|e| internal("core dumps", e))
    }

    fn sweep_store_copies(&self) -> Result<(), ApiError> {
        sweep_store_copies(&self.store, |_| false)?;
        // Spec 25.4 (a): what a killed process left in `tmp/`. Busy means a
        // capture is running; the next start looks again.
        let _ = self.store.sweep_temporaries();
        Ok(())
    }

    fn sweep_vault(&self) -> Result<(), ApiError> {
        // The agent does not know which runs ended (the coordinator does):
        // no bundle is erased here, only the vault's stale temporary files.
        let vault = Vault::open(&self.state_dir).map_err(|e| e.to_api())?;
        vault.sweep(|_| false).map(|_| ()).map_err(|e| e.to_api())
    }

    fn reconcile_secrets(&self, should_erase: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        let report = self
            .secrets
            .reconcile(should_erase)
            .map_err(|e| e.to_api())?;
        // Their secret files cannot be proven gone one by one: the whole
        // workspace goes, then the record. This runs at startup, before any
        // workspace is being made, which the sweep requires.
        let required = &report.workspace_required;
        if !required.is_empty() {
            let swept = self.workspaces.sweep_orphans(&|a| !required.contains(a))?;
            if let Some((path, why)) = swept.failed.first() {
                return Err(internal(
                    "workspace with secret files",
                    format!("{}: {why}", path.display()),
                ));
            }
            for attempt in required {
                self.secrets.forget(attempt).map_err(|e| e.to_api())?;
            }
        }
        if let Some((name, why)) = report.failed.first() {
            return Err(internal("secret files", format!("{name}: {why}")));
        }
        Ok(())
    }

    fn sweep_workspaces(&self, has_record: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError> {
        // A setup builder's workspace is not an attempt's: its owner is the
        // setup area, which settled the builders of an earlier run when it
        // was opened and names those it could not.
        let owned = |a: &AttemptId| {
            has_record(a) || self.setups.as_ref().is_some_and(|s| s.owns_workspace(a))
        };
        let report = self.workspaces.sweep_orphans(&owned)?;
        match report.failed.first() {
            Some((path, why)) => Err(internal(
                "orphan workspace",
                format!("{}: {why}", path.display()),
            )),
            None => Ok(()),
        }
    }

    fn prepare(
        &self,
        attempt: &AcceptedAttempt,
        payload: &AttemptPayload,
    ) -> Result<Workspace, ApiError> {
        if let Some(why) = crate::workspace::embedded::incompatible_registry(&self.env) {
            return Err(ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                why,
            ));
        }
        let m = self.manifest(attempt)?;
        check_toolchain_source(&self.store, &self.state_dir, &m, payload)?;

        if payload.in_place {
            let root = self.original(payload)?;
            return self.workspaces.open_in_place(&root, Path::new(""), true);
        }
        let repo = match (&m.execution_commit, &m.repository_id) {
            (Some(_), Some(id)) => Some(self.workspaces.ensure_clone(id, None)?),
            _ => None,
        };
        self.workspaces
            .create(&attempt.attempt_id, &self.store, &m, repo.as_ref())
    }

    fn setup_poll(
        &self,
        attempt: &AcceptedAttempt,
        payload: &AttemptPayload,
        workspace: &Workspace,
    ) -> Result<SetupPoll, ApiError> {
        match &self.setups {
            Some(s) => s.poll(&attempt.attempt_id, payload, workspace),
            None => Ok(SetupPoll::Done {
                skip: Default::default(),
                note: None,
            }),
        }
    }

    fn setup_release(&self, attempt: &AttemptId) {
        if let Some(s) = &self.setups {
            s.release(attempt);
        }
    }

    fn secret_paths(&self, payload: &AttemptPayload) -> Result<Option<Vec<String>>, ApiError> {
        match self.manifest_of(payload) {
            Ok(m) => Ok(Some(
                m.secret_files.iter().map(|s| s.path.display()).collect(),
            )),
            Err(e) if e.code == ErrorCode::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn place_secrets(
        &self,
        attempt: &AcceptedAttempt,
        workspace: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError> {
        if workspace.kind == crate::workspace::WorkspaceKind::InPlace {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "in-place never places secrets",
            ));
        }
        let m = self.manifest(attempt)?;
        let key = key::load_in_state(&self.state_dir, &self.key_path).map_err(|e| e.to_api())?;
        self.secrets
            .place(
                &key,
                &attempt.attempt_id,
                &workspace.root,
                files,
                &m.secret_files,
            )
            .map_err(|e| e.to_api())
    }

    fn secrets_ready(&self, attempt: &AcceptedAttempt) -> Result<bool, ApiError> {
        if self.original_secrets(attempt)?.is_some() {
            return Ok(true);
        }
        let m = self.manifest(attempt)?;
        match self
            .secrets
            .ensure_placed(&attempt.attempt_id, &m.secret_files)
        {
            Ok(()) => Ok(true),
            // Not all of them arrived yet.
            Err(SecretError::PathSetMismatch) => Ok(false),
            Err(e) => Err(e.to_api()),
        }
    }

    fn placed_secrets(&self, attempt: &AcceptedAttempt) -> Result<Placed, ApiError> {
        if let Some(placed) = self.original_secrets(attempt)? {
            return Ok(placed);
        }
        self.secrets
            .placed(&attempt.attempt_id)
            .map_err(|e| e.to_api())
    }

    fn blobs(&self) -> Option<&dyn BlobSink> {
        Some(&self.incoming)
    }

    fn cleanup(&self, attempt: &AttemptId, workspace: Option<&Workspace>) -> Result<(), ApiError> {
        self.setup_release(attempt);
        if workspace.is_some_and(|w| w.kind == crate::workspace::WorkspaceKind::InPlace) {
            return Ok(());
        }
        match self.secrets.erase(attempt) {
            Ok(_) => {}
            Err(e @ SecretError::WorkspaceRequired { .. }) => {
                // Without the workspace record nothing can be removed safely
                // here; the next startup's reconcile does it.
                let Some(ws) = workspace else {
                    return Err(e.to_api());
                };
                self.workspaces.remove(ws)?;
                self.secrets.forget(attempt).map_err(|e| e.to_api())?;
                return Ok(());
            }
            Err(e) => return Err(e.to_api()),
        }
        match workspace {
            Some(ws) if ws.root.exists() => self.workspaces.remove(ws),
            _ => Ok(()),
        }
    }
}
