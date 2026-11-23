//! The real [`Host`]: the source store, fast-worktree workspaces and the
//! secret files of this worker's state directory.
//!
//! The source an attempt names has to be in the worker's store before the
//! attempt is prepared; it gets there through the blob operations of the
//! protocol ([`super::blobs`]).

use super::blobs::{BlobSink, Incoming};
use super::host::Host;
use super::pins::{Pin, SourcePins};
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

/// The deleting pass's answer about one source (spec 25.8 5b (a), (h)7):
/// a pin, or an id `worker.db` cannot name, keeps it; otherwise the
/// attempts of every member decide. An error keeps it too (the pass reads
/// it as referenced).
fn source_use(
    pinned: bool,
    id: Option<&crate::ids::SourceId>,
    attempts: &super::host::SourceAttempts,
) -> anyhow::Result<crate::source::retention::SourceUse> {
    use crate::source::retention::SourceUse;
    let Some(id) = id.filter(|_| !pinned) else {
        return Ok(SourceUse::Live);
    };
    let (live, ended) = attempts(id)?;
    Ok(if live {
        SourceUse::Live
    } else {
        SourceUse::Ended(ended)
    })
}

/// A received source unused this long goes (spec 25.8 5b (c): 14 days).
const RECEIVED_MAX_AGE_MS: u64 = 14 * 24 * 3600 * 1000;
/// Above this many counted bytes the oldest go first (5b (c): 20 GiB).
const RECEIVED_CAP_BYTES: u64 = 20 << 30;

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
    /// What the readers that can outlive their attempt hold (spec 25.8 5b
    /// (h)7); shared with the setup builders' workspaces.
    pins: SourcePins,
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
            pins: SourcePins::default(),
        })
    }

    /// The manifest of the source the attempt's payload names, and only if
    /// it is the manifest the payload names.
    fn manifest(&self, attempt: &AcceptedAttempt) -> Result<CiManifest, ApiError> {
        let payload: AttemptPayload = serde_json::from_value(attempt.spec.clone())
            .map_err(|e| internal("stored payload", e))?;
        manifest_in(&self.store, &payload, Wait::Yes, None)
    }

    /// Prepare the setup steps of attempts once per worker (spec chapter 8):
    /// `cfg` says how builders are run. Without it a payload's setup is not
    /// looked at and every attempt runs all of its commands. Before the
    /// agent starts: it clears what builders of an earlier run left.
    pub fn with_setup(mut self, cfg: SetupConfig) -> Result<Self, ApiError> {
        let ws = BuilderWs {
            pins: self.pins.clone(),
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

/// [`check_toolchain_source`] for an accepted attempt: under one waited
/// shared lock, so its blob reads (which only try the lock) wait for a
/// deleting pass's step too (spec 25.8 5b (a)) instead of failing as an
/// invalid bundle.
fn check_toolchain_waiting(
    store: &SourceStore,
    state_dir: &Path,
    m: &CiManifest,
    payload: &AttemptPayload,
) -> Result<(), ApiError> {
    let _held = store
        .mutation_guard_waiting()
        .map_err(|e| internal("source store", format!("{e:#}")))?;
    check_toolchain_source(store, state_dir, m, payload)
}

/// Whether a reader waits for a deleting pass's step (an accepted attempt's
/// reader) or not (acceptance).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Wait {
    Yes,
    No,
}

/// The manifest of the source the payload names, and only if it is the
/// manifest the payload names.
fn manifest_in(
    store: &SourceStore,
    payload: &AttemptPayload,
    wait: Wait,
    held: Option<&crate::source::retention::Guard>,
) -> Result<CiManifest, ApiError> {
    let missing = || {
        ApiError::new(
            ErrorCode::NotFound,
            format!("source {} is not in this worker's store", payload.source_id),
        )
    };
    let source = ci_id_of(&payload.source_id).ok_or_else(missing)?;
    // An accepted attempt's reader waits for a deleting pass's step instead
    // of failing (spec 25.8 5b (a)); the read below shares this lock.
    // Acceptance only tries it (inside `store.manifest`).
    let _held = match wait {
        Wait::Yes => Some(
            store
                .mutation_guard_waiting()
                .map_err(|e| internal("source store", format!("{e:#}")))?,
        ),
        Wait::No => None,
    };
    // Acceptance passes the shared lock it holds (no second take).
    let m = match held {
        Some(g) => store.manifest_under(g, &source),
        None => store.manifest(&source),
    }
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

/// A pin on the source of `m`, or the error a missing source gives.
fn pin(pins: &SourcePins, store: &SourceStore, m: &CiManifest) -> Result<Pin, ApiError> {
    pins.pin(store, &m.source_id)
        .map_err(|e| ApiError::new(ErrorCode::NotFound, format!("{e:#}")))
}

/// The workspaces of the setup builders: made like an attempt's.
struct BuilderWs {
    pins: SourcePins,
    store: SourceStore,
    workspaces: Workspaces,
}

impl BuilderWorkspaces for BuilderWs {
    fn create(&self, attempt: &AttemptId, payload: &AttemptPayload) -> Result<Workspace, ApiError> {
        let m = manifest_in(&self.store, payload, Wait::Yes, None)?;
        // (the builder's thread is not joined by the attempt's release: it
        // pins the source for as long as it makes the workspace)
        let _pin = pin(&self.pins, &self.store, &m)?;
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
        // (the `prepare` thread reads on after a waiter gives up: it pins
        // the source until the workspace is made)
        let _pin = pin(&self.pins, &self.store, &m)?;
        // (its blob reads only try the shared lock: one waited lock held
        // around them makes them wait for a deleting pass's step too, spec
        // 25.8 5b (a), instead of failing as an invalid bundle)
        check_toolchain_waiting(&self.store, &self.state_dir, &m, payload)?;

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

    fn toolchain_tick(&self) {
        if let Some(s) = &self.setups {
            s.roots_tick();
        }
    }
    fn setup_release(&self, attempt: &AttemptId) {
        if let Some(s) = &self.setups {
            s.release(attempt);
        }
    }

    fn hold_source(&self) -> Result<Option<crate::source::retention::Guard>, ApiError> {
        match self.store.mutation_guard() {
            Ok(held) => Ok(Some(held)),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::WouldBlock) =>
            {
                Err(ApiError::overloaded(
                    crate::error::OverloadedReason::SourceStore,
                    "the source store is busy with a deleting step; send again",
                ))
            }
            Err(e) => Err(internal("source store", format!("{e:#}"))),
        }
    }

    fn source_used(&self, held: &crate::source::retention::Guard, payload: &AttemptPayload) {
        if let Some(id) = ci_id_of(&payload.source_id) {
            let _ = self.store.touch_received(held, &id);
        }
    }

    fn received_pass(
        &self,
        attempts: &super::host::SourceAttempts,
        stop: &dyn Fn() -> bool,
    ) -> Option<anyhow::Result<crate::source::retention::ReceivedPassReport>> {
        use crate::source::retention::ReceivedPassInputs;
        let uses = |ci: &str, id: Option<&crate::ids::SourceId>| {
            source_use(self.pins.pinned(ci), id, attempts)
        };
        Some(self.store.received_pass(&ReceivedPassInputs {
            wall: crate::platform::clock::wall_ms(),
            mono: crate::platform::clock::monotonic_ms(),
            boot: crate::platform::boot_id(),
            max_age_ms: RECEIVED_MAX_AGE_MS,
            cap_bytes: RECEIVED_CAP_BYTES,
            uses: &uses,
            stop,
        }))
    }

    fn source_ended(&self, source: &crate::ids::SourceId) {
        let Some(id) = ci_id_of(source) else {
            return;
        };
        if let Ok(held) = self.store.mutation_guard() {
            let _ = self.store.touch_received(&held, &id);
        }
    }

    fn source_present(
        &self,
        held: &crate::source::retention::Guard,
        payload: &AttemptPayload,
    ) -> Result<bool, ApiError> {
        let Some(id) = ci_id_of(&payload.source_id) else {
            return Ok(true);
        };
        self.store
            .received(held, &id)
            .map_err(|e| internal("source store", format!("{e:#}")))
    }

    fn secret_paths(
        &self,
        payload: &AttemptPayload,
        wait: bool,
        held: Option<&crate::source::retention::Guard>,
    ) -> Result<Option<Vec<String>>, ApiError> {
        let wait = if wait { Wait::Yes } else { Wait::No };
        match manifest_in(&self.store, payload, wait, held) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{Digest, Generation, RunId, SourceId, TaskId};
    use std::time::{Duration, Instant};

    fn payload() -> AttemptPayload {
        let d = Digest::of_bytes(b"x");
        AttemptPayload {
            ci_task: None,
            toolchains: None,
            outputs: Vec::new(),
            requirements: Default::default(),
            in_place: false,
            setup: None,
            run_id: RunId::generate(),
            task_id: TaskId::generate(),
            generation: Generation(1),
            source_id: SourceId::generate(),
            plan_digest: d.clone(),
            manifest_digest: d,
            permit_id: None,
            commands: Vec::new(),
            lease_ms: 60_000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: Vec::new(),
        }
    }

    /// One exclusive step, held for `ms` on another thread.
    fn step(store: &SourceStore, ms: u64) -> std::thread::JoinHandle<()> {
        let exclusive = store.exclusive_step_for_test();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(ms));
            drop(exclusive);
        })
    }

    fn digest_of(s: &str) -> Digest {
        Digest::parse(&if s.starts_with("sha256:") {
            s.to_string()
        } else {
            format!("sha256:{s}")
        })
        .unwrap()
    }

    /// A machine over the store of a captured source, and the payload that
    /// names that source.
    fn machine_with_source(
        f: &crate::ci::execute::tests::Fixture,
        toolchains: Option<Box<crate::toolchain::contract::FrozenBundle>>,
    ) -> (Machine, AttemptPayload) {
        let machine = Machine::open(
            &f.area.join("state"),
            std::env::vars().collect(),
            f.area.join("key"),
        )
        .unwrap();
        let mut p = payload();
        p.source_id = crate::source::classify::source_id_of_ci(&f.manifest.source_id).unwrap();
        p.manifest_digest = digest_of(&f.manifest.manifest_digest);
        p.toolchains = toolchains;
        (machine, p)
    }

    #[test]
    fn acceptance_does_not_wait_for_a_step_and_an_attempt_reader_does() {
        let f = crate::ci::execute::tests::fixture(crate::ci::execute::tests::WORKFLOW);
        let (machine, p) = machine_with_source(&f, None);
        // Acceptance: the answer under a held step is the untyped store
        // error at once (a source that is not here would be NotFound).
        let holder = step(&f.store, 600);
        let started = Instant::now();
        let e = machine.secret_paths(&p, false, None).unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal, "{e:?}");
        assert!(e.message.starts_with("source store"), "{e:?}");
        assert!(started.elapsed() < Duration::from_millis(500));
        holder.join().unwrap();
        // The accepted attempt's `prepare` reads on after the step.
        let holder = step(&f.store, 300);
        let started = Instant::now();
        let paths = machine.secret_paths(&p, true, None).unwrap();
        assert_eq!(paths, Some(Vec::new()));
        assert!(started.elapsed() >= Duration::from_millis(250));
        holder.join().unwrap();
    }

    #[test]
    fn the_toolchain_check_of_prepare_waits_for_a_step() {
        use crate::toolchain::versions::*;
        let f = crate::ci::execute::tests::fixture_with(
            crate::ci::execute::tests::WORKFLOW,
            |project| std::fs::write(project.join(".node-version"), "20.0.0\n").unwrap(),
        );
        // A bundle of the declarations the source holds: checking it reads
        // the version file's blob (a verified, try-only open).
        let declarations =
            crate::toolchain::snapshot::read_captured(&f.store, &f.manifest, "", &[]).unwrap();
        assert_eq!(declarations.len(), 1);
        let source = digest_of(&f.manifest.manifest_digest);
        let bundle = crate::toolchain::contract::FrozenBundle::seal_exact(
            FrozenToolchains::freeze(
                source,
                Digest::of_bytes(b"parent"),
                Scope::project(""),
                &declarations,
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            )
            .unwrap(),
        )
        .unwrap();
        let (_machine, p) = machine_with_source(&f, Some(Box::new(bundle)));
        // Without the waited lock the blob read fails under a step.
        let holder = step(&f.store, 300);
        assert!(check_toolchain_source(&f.store, f.area.as_path(), &f.manifest, &p).is_err());
        holder.join().unwrap();
        let holder = step(&f.store, 300);
        let started = Instant::now();
        check_toolchain_waiting(&f.store, f.area.as_path(), &f.manifest, &p).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(250));
        holder.join().unwrap();
    }
}

#[cfg(test)]
mod received_pass_tests {
    use super::source_use;
    use crate::source::retention::SourceUse;

    #[test]
    fn a_pin_a_live_attempt_or_an_unnamed_id_keeps_the_source() {
        let id = crate::ids::SourceId::generate();
        let ended = |_: &crate::ids::SourceId| -> anyhow::Result<(bool, Option<i64>)> {
            Ok((false, Some(7)))
        };
        let live =
            |_: &crate::ids::SourceId| -> anyhow::Result<(bool, Option<i64>)> { Ok((true, None)) };
        assert_eq!(
            source_use(false, Some(&id), &ended).unwrap(),
            SourceUse::Ended(Some(7))
        );
        assert_eq!(
            source_use(true, Some(&id), &ended).unwrap(),
            SourceUse::Live
        );
        assert_eq!(source_use(false, None, &ended).unwrap(), SourceUse::Live);
        assert_eq!(
            source_use(false, Some(&id), &live).unwrap(),
            SourceUse::Live
        );
    }
}
