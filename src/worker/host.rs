//! What the agent asks of the machine around it: the startup sweeps and the
//! preparation of an attempt's workspace. A trait, so the agent's own rules
//! (acceptance, recovery, order of the startup steps) are tested without Git,
//! fast-worktree or process-wide settings.
//!
//! All of it is blocking; the agent calls it from its own threads.

use super::blobs::BlobSink;
use crate::error::ApiError;
use crate::ids::AttemptId;
use crate::secrets::place::Placed;
use crate::setup::builder::SetupPoll;
use crate::storage::AcceptedAttempt;
use crate::wire::{AttemptPayload, SecretFile};
use crate::workspace::Workspace;

/// The environment a workspace's preparation (`fast-worktree add`) runs
/// with when the task has toolchains (spec 22.1): the activation's
/// variables, its `PATH` with the tool-bin directory first, and the
/// activated tools' identity, which goes into every preparation cache key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareToolchain {
    pub env: std::collections::BTreeMap<String, String>,
    pub identity: String,
}

impl PrepareToolchain {
    /// Under `binding`: the activation with the tool-bin directory `bin`
    /// first on its `PATH`, and the identity of the activated tools (the
    /// activation's installation digest: each tool with its identity).
    pub fn of(
        bin: Option<&std::path::Path>,
        binding: &crate::toolchain::contract::ExecutionBinding,
    ) -> Result<Self, String> {
        Ok(PrepareToolchain {
            env: crate::workspace::embedded::activated(bin, binding, &Default::default())?,
            identity: binding.activation.installation.as_str().to_string(),
        })
    }

    /// What `fast-worktree add` runs with: the worker's own variables
    /// `own`, overlaid by the activation's (its `PATH` among them), and the
    /// identity, which fast-worktree 0.8 adds to every preparation cache
    /// key: a `node_modules` prepared with one node is never reused for
    /// another.
    pub fn environment(
        &self,
        own: &std::collections::BTreeMap<String, String>,
    ) -> std::collections::BTreeMap<String, String> {
        let mut env = own.clone();
        env.extend(self.env.clone());
        env.insert("FAST_WORKTREE_KEY_IDENTITY".into(), self.identity.clone());
        env
    }
}

/// For each source, across every member: whether a non-terminal attempt
/// names it, and the newest `terminal_at_ms` of those that did.
pub type SourceAttempts<'a> =
    dyn Fn(&crate::ids::SourceId) -> anyhow::Result<(bool, Option<i64>)> + 'a;

pub trait Host: Send + Sync {
    /// Existing worker-local setup owner; access never initializes it.
    fn setup_owner(&self) -> Option<&crate::setup::builder::Setups> {
        None
    }
    /// Optional immutable telemetry; unavailable hosts make no cost claims.
    fn preparation_observation_page(
        &self,
        _request: &crate::setup::observation_transport::ObservationPageRequest,
    ) -> Option<crate::setup::observation_transport::ObservationPage> {
        None
    }
    fn toolchain_registry(
        &self,
        _profile: &crate::toolchain::profile::Profile,
        _base: crate::ids::Digest,
    ) -> Result<Option<crate::toolchain::registry::Registry>, ApiError> {
        Ok(None)
    }
    /// An explicit request for this version (a warm-up): a remembered
    /// install failure of it no longer holds the next install back.
    fn toolchain_retry(&self, _request: &crate::toolchain::preparation::PreparationRequest) {}
    fn toolchain_poll(
        &self,
        _attempt: &AttemptId,
        _request: &crate::toolchain::preparation::PreparationRequest,
    ) -> Result<crate::toolchain::preparation::ToolPreparationPoll, ApiError> {
        Err(ApiError::blocked(
            crate::error::BlockedReason::EnvironmentMismatch,
            "toolchain preparation is unavailable",
        ))
    }
    fn setup_poll_bound(
        &self,
        attempt: &AcceptedAttempt,
        payload: &AttemptPayload,
        workspace: &Workspace,
        binding: Option<&crate::toolchain::contract::ExecutionBinding>,
    ) -> Result<SetupPoll, ApiError> {
        if binding.is_some() {
            return Err(ApiError::blocked(
                crate::error::BlockedReason::EnvironmentMismatch,
                "bound setup is unavailable",
            ));
        }
        self.setup_poll(attempt, payload, workspace)
    }

    fn admit(&self, payload: &AttemptPayload) -> Result<(), ApiError> {
        if payload.in_place {
            return Err(ApiError::new(
                crate::error::ErrorCode::InvalidInput,
                "in-place requires the parent local worker",
            ));
        }
        Ok(())
    }
    /// The shared source mutation lock, taken without waiting by a new
    /// attempt's acceptance and held until its commit (spec 25.8 5b (a)):
    /// while it is held no deleting pass's step runs, so a source found here
    /// stays until the attempt that names it is terminal. A busy store is
    /// `OVERLOADED` (`source_store`). `None`: this host has no source store.
    fn hold_source(&self) -> Result<Option<crate::source::retention::Guard>, ApiError> {
        Ok(None)
    }
    /// The acceptance of an attempt that names the payload's source is
    /// committed: the source's `received` root gets the time (spec 25.8 5b
    /// (c)). Called under the lock of [`Self::hold_source`]; best effort.
    fn source_used(&self, _held: &crate::source::retention::Guard, _payload: &AttemptPayload) {}
    /// An attempt that named `source` is terminal: the source's `received`
    /// root gets the time (spec 25.8 5b (c)), under the shared lock taken
    /// without waiting; a busy store skips the record. Best effort.
    fn source_ended(&self, _source: &crate::ids::SourceId) {}
    /// One deleting pass of the received sources (spec 25.8 5b (d)), on
    /// the agent's own thread for it ((h)6). `attempts` answers, for every
    /// member, whether a non-terminal attempt names the source and the
    /// newest `terminal_at_ms` of those that did
    /// ([`crate::storage::WorkerStore::source_use`]); the host adds its
    /// in-process pins ((h)7). `stop` is asked at each section under the
    /// exclusive lock: when it is true the pass returns without detaching
    /// (the agent is stopping, or a version is on trial). `None`: this host
    /// has no source store.
    fn received_pass(
        &self,
        _attempts: &SourceAttempts,
        _stop: &dyn Fn() -> bool,
    ) -> Option<anyhow::Result<crate::source::retention::ReceivedPassReport>> {
        None
    }
    /// Is the payload's source in the store (spec 25.8 5b (b), the
    /// `received` root)? Asked by a new attempt's acceptance under the lock
    /// of [`Self::hold_source`]. A host without a source store, and a source
    /// id that names nothing the store could hold, answer `true`: the later
    /// check of the preparation decides, as before.
    fn source_present(
        &self,
        _held: &crate::source::retention::Guard,
        _payload: &AttemptPayload,
    ) -> Result<bool, ApiError> {
        Ok(true)
    }
    /// Once, before any secret is read (process-wide).
    fn disable_core_dumps(&self) -> Result<(), ApiError>;
    /// Plaintext secret bundles a crashed capture left in the source store.
    fn sweep_store_copies(&self) -> Result<(), ApiError>;
    /// Stale files of the bundle vault.
    fn sweep_vault(&self) -> Result<(), ApiError>;
    /// Erase the placed secret files of every attempt `should_erase` names
    /// (terminal or unknown ones).
    fn reconcile_secrets(&self, should_erase: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError>;
    /// Remove workspaces and scratch directories no live attempt owns. Runs
    /// before any [`Host::prepare`].
    fn sweep_workspaces(&self, has_record: &dyn Fn(&AttemptId) -> bool) -> Result<(), ApiError>;
    /// Check, before any toolchain is installed for it, that the attempt's
    /// frozen toolchains are what its captured source reads back to. The
    /// default has no source store.
    fn check_toolchain_source(
        &self,
        _attempt: &AcceptedAttempt,
        _payload: &AttemptPayload,
    ) -> Result<(), ApiError> {
        Ok(())
    }
    /// Make the attempt's workspace. Safe to call again for the same attempt
    /// (an agent that died half-way). `toolchain` is the task's activated
    /// toolchains, which the workspace's preparation runs with (spec 22.1);
    /// `None` for a task without any.
    fn prepare(
        &self,
        attempt: &AcceptedAttempt,
        payload: &AttemptPayload,
        toolchain: Option<&PrepareToolchain>,
    ) -> Result<Workspace, ApiError>;
    /// Where the setup of the attempt stands (spec chapter 8): its steps
    /// are prepared once per worker by a builder and copied into the
    /// attempt's workspace, which the agent calls this about, again and
    /// again, until it is not `Pending`. `Done` names the ids of the setup
    /// steps whose result is in the workspace; the agent leaves those
    /// commands out and runs every other one. The default has no setup.
    fn setup_poll(
        &self,
        _attempt: &AcceptedAttempt,
        _payload: &AttemptPayload,
        _workspace: &Workspace,
    ) -> Result<SetupPoll, ApiError> {
        Ok(SetupPoll::Done {
            skip: Default::default(),
            note: None,
        })
    }
    /// The attempt does not wait for its setup any more (it is stopped, or
    /// gave up waiting). Its reference to the builder goes; the last one
    /// stops the builder. Idempotent.
    fn setup_release(&self, _attempt: &AttemptId) {}
    /// Spec 25.7 (b): a pass over the toolchain roots (bounded, throttled).
    fn toolchain_tick(&self) {}
    /// The paths (relative to the workspace root) of the secret files of the
    /// source the payload names: the declared ones and what
    /// `.worktreeinclude` matched. `None` while that source is not in this
    /// worker's store. `wait`: whether the read waits for a deleting pass's
    /// step (the accepted attempt's `prepare`) or not (acceptance, which
    /// never waits, spec 25.8 5b (a)). `held`: the shared source lock the
    /// caller already holds (acceptance), read under instead of taking a
    /// second one.
    fn secret_paths(
        &self,
        payload: &AttemptPayload,
        wait: bool,
        held: Option<&crate::source::retention::Guard>,
    ) -> Result<Option<Vec<String>>, ApiError>;
    /// Place the files of a `secret_bundle_put` in the attempt's workspace.
    /// Returns how many are in place.
    fn place_secrets(
        &self,
        attempt: &AcceptedAttempt,
        workspace: &Workspace,
        files: &[SecretFile],
    ) -> Result<u32, ApiError>;
    /// Before the commands start: `Ok(true)` when every secret file the
    /// attempt's source names is in place, `Ok(false)` when some have not
    /// arrived yet, an error when what is there is wrong.
    fn secrets_ready(&self, attempt: &AcceptedAttempt) -> Result<bool, ApiError>;
    /// The secret values to mask in the output of the attempt's commands,
    /// from the secret files in place, and which files those are (what no
    /// artifact may be, under any name). Called after [`Host::secrets_ready`]
    /// said `true` and before the commands start; an error keeps them from
    /// starting.
    fn placed_secrets(&self, attempt: &AcceptedAttempt) -> Result<Placed, ApiError>;
    /// The attempt ended: erase its secret files and remove its workspace.
    /// Idempotent.
    fn cleanup(&self, attempt: &AttemptId, workspace: Option<&Workspace>) -> Result<(), ApiError>;
    /// Where the sources the coordinator sends are taken in (`blob_put`,
    /// `blob_verify`). `None`: this host does not receive sources.
    fn blobs(&self) -> Option<&dyn BlobSink> {
        None
    }
}
