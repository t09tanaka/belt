//! How the coordinator reaches a worker: one request, one answer. The real
//! link is a protocol [`Client`]; tests put a worker of their own behind it.

use crate::error::ApiError;
use crate::protocol::{CallError, Client};
use crate::wire::{Request, Response};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Why a call has no answer to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkError {
    /// The worker answered with an error.
    Remote(ApiError),
    /// No answer: the request may or may not have arrived, and may or may
    /// not have been carried out. Never a reason to start the work elsewhere.
    Unconfirmed,
}

pub(crate) type LinkFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Response, LinkError>> + Send + 'a>>;

use super::input_gate::{PlacementInputs, ReadyInput, State};

static NEXT_GATE_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
fn next_gate_generation() -> u64 {
    NEXT_GATE_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Synchronization adapter for one connection's bounded readiness metadata.
pub struct InputGate {
    state: std::sync::Mutex<State>,
    commit: std::sync::Arc<tokio::sync::Mutex<()>>,
    transport_policy: Option<String>,
    preferences: std::sync::Mutex<super::prefetch::Preferences>,
    /// The connection negotiated `ci_task_v1`: the frozen plan of a CI run
    /// is delivered with its source.
    ci_task: std::sync::atomic::AtomicBool,
    ci_discovery: std::sync::atomic::AtomicBool,
}
impl Default for InputGate {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(State::new(next_gate_generation())),
            commit: std::sync::Arc::default(),
            transport_policy: None,
            preferences: std::sync::Mutex::default(),
            ci_task: std::sync::atomic::AtomicBool::new(false),
            ci_discovery: std::sync::atomic::AtomicBool::new(false),
        }
    }
}
impl InputGate {
    pub fn with_transport_policy(policy: String) -> Self {
        Self {
            transport_policy: Some(policy),
            ..Self::default()
        }
    }
    pub fn set_ci_discovery(&self, negotiated: bool) {
        self.ci_discovery
            .store(negotiated, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn ci_discovery(&self) -> bool {
        self.ci_discovery.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn set_ci_task(&self, negotiated: bool) {
        self.ci_task
            .store(negotiated, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn ci_task(&self) -> bool {
        self.ci_task.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn cost_transport(&self) -> Option<crate::scheduler::measured::Transport> {
        let (worker_boot, agent_epoch) = self.placement_inputs().identity?;
        Some(crate::scheduler::measured::Transport {
            worker_boot,
            agent_epoch,
            policy: self.transport_policy.clone()?,
        })
    }
    pub fn publish_verified(&self, generation: u64, input: ReadyInput) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.publish(generation, input.clone());
        let snapshot = state.snapshot();
        snapshot.identity.is_some()
            && snapshot.generation == generation
            && snapshot.ready.contains(&input)
    }

    pub fn cold_eligible(&self, task: &crate::ids::TaskId) -> bool {
        self.preferences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .eligible(task, self.generation(), crate::platform::monotonic_ms())
    }
    pub fn prefer(&self, task: &crate::storage::QueuedTask, expected: u64, ttl: u64) -> bool {
        self.preferences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .choose(
                task,
                self.generation(),
                expected,
                ttl,
                crate::platform::monotonic_ms(),
            )
    }
    pub fn preferred(&self) -> Option<crate::storage::QueuedTask> {
        self.preferences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .preferred(self.generation(), crate::platform::monotonic_ms())
    }
    /// The worker refused the input of `run` (see [`State::refuse`]).
    pub fn refuse_input(&self, generation: u64, run: &crate::ids::RunId, why: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .refuse(generation, run, why)
    }
    pub fn input_refusals(&self) -> Vec<(crate::ids::RunId, String)> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .refusals()
    }
    pub fn fail_preference(&self, run: &crate::ids::RunId) {
        self.preferences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .fail(run, self.generation());
    }
    pub fn clear_preference(
        &self,
        task: Option<&crate::ids::TaskId>,
        run: Option<&crate::ids::RunId>,
    ) {
        self.preferences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear(task, run);
    }
    pub fn placement_fence(&self) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        self.commit.clone()
    }
    pub fn placement_inputs(&self) -> PlacementInputs {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot()
    }
    pub fn snapshot(&self) -> Vec<ReadyInput> {
        self.placement_inputs().ready
    }
    pub fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .generation()
    }
    pub async fn observe(&self, boot: crate::ids::WorkerBootId, epoch: crate::ids::AgentEpoch) {
        let _commit = self.commit.lock().await;
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observe(boot, epoch, next_gate_generation());
    }
    /// Invalidate this connection before removing it from placement eligibility.
    pub async fn invalidate(&self) {
        let _commit = self.commit.lock().await;
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = State::new(next_gate_generation());
    }
    pub fn publish(&self, generation: u64, input: ReadyInput) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .publish(generation, input);
    }
    pub fn evaluated(&self, generation: u64, run: &crate::ids::RunId) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .evaluated(generation, run);
    }
    pub fn evict_evaluated(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .evict_evaluated()
    }
    pub fn retain(&self, keep: impl FnMut(&ReadyInput) -> bool) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(keep);
    }
}

pub trait WorkerLink: Send + Sync {
    fn preparation_probe_negotiated(&self) -> bool {
        false
    }
    /// Exact hello negotiation; never inferred from opaque transport provenance.
    fn preparation_observations_negotiated(&self) -> bool {
        false
    }
    /// The worker answers `env_sync` (hello feature `env_sync_v1`).
    fn env_sync_negotiated(&self) -> bool {
        false
    }
    /// The worker takes CI tasks and the frozen plan they name (hello
    /// feature `ci_task_v1`). A worker that does not is given none.
    fn ci_task_negotiated(&self) -> bool {
        false
    }
    fn matrix_dispatch_negotiated(&self) -> bool {
        false
    }
    fn workflow_artifacts_negotiated(&self) -> bool {
        false
    }

    fn ci_discovery_negotiated(&self) -> bool {
        false
    }
    fn cold_eligible(&self, _task: &crate::ids::TaskId) -> bool {
        false
    }
    fn prefer(&self, _task: &crate::storage::QueuedTask, _expected: u64, _ttl: u64) -> bool {
        false
    }
    fn clear_preference(
        &self,
        _task: Option<&crate::ids::TaskId>,
        _run: Option<&crate::ids::RunId>,
    ) {
    }

    fn cost_transport(&self) -> Option<crate::scheduler::measured::Transport> {
        None
    }

    /// A stable fence for session-generation invalidation. Gated links await it
    /// before resetting generation; a placement holds it through SQLite commit.
    /// Cache eviction does not invalidate the worker's verified source facts.
    fn placement_fence(&self) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        None
    }
    fn placement_inputs(&self) -> Option<PlacementInputs> {
        None
    }
    /// The runs whose input this worker did not take (the frozen plan of a
    /// CI run it has no room for), with the reason, while that holds: it is
    /// left out of the placement of the tasks of those runs, by name.
    fn input_refusals(&self) -> Vec<(crate::ids::RunId, String)> {
        Vec::new()
    }
    fn inputs_evaluated(&self, _generation: u64, _run: &crate::ids::RunId) {}
    /// Inspection without observing source gates or changing placement state.
    fn read_only_call(&self, request: Request) -> LinkFuture<'_> {
        self.call(request)
    }
    fn call(&self, request: Request) -> LinkFuture<'_>;
}

/// A worker behind a protocol connection.
pub struct ClientLink {
    client: Client,
    timeout: Duration,
    gate: Option<std::sync::Arc<InputGate>>,
    preparation_observations: bool,
    preparation_probe: bool,
    env_sync: bool,
    ci_task: bool,
    matrix_dispatch: bool,
    ci_discovery: bool,
    workflow_artifacts: bool,
}

impl ClientLink {
    pub fn new(client: Client, timeout: Duration) -> ClientLink {
        ClientLink {
            client,
            timeout,
            gate: None,
            preparation_observations: false,
            preparation_probe: false,
            env_sync: false,
            ci_task: false,
            matrix_dispatch: false,
            ci_discovery: false,
            workflow_artifacts: false,
        }
    }
}

impl ClientLink {
    pub fn gated(client: Client, timeout: Duration, gate: std::sync::Arc<InputGate>) -> Self {
        Self {
            client,
            timeout,
            gate: Some(gate),
            preparation_observations: false,
            preparation_probe: false,
            env_sync: false,
            ci_task: false,
            matrix_dispatch: false,
            ci_discovery: false,
            workflow_artifacts: false,
        }
    }
}
impl ClientLink {
    pub fn with_preparation_probe(mut self, negotiated: bool) -> Self {
        self.preparation_probe = negotiated;
        self
    }
    pub fn with_preparation_observations(mut self, negotiated: bool) -> Self {
        self.preparation_observations = negotiated;
        self
    }
    pub fn with_env_sync(mut self, negotiated: bool) -> Self {
        self.env_sync = negotiated;
        self
    }
    pub fn with_matrix_dispatch(mut self, negotiated: bool) -> Self {
        self.matrix_dispatch = negotiated;
        self
    }

    pub fn with_ci_discovery(mut self, negotiated: bool) -> Self {
        self.ci_discovery = negotiated;
        self
    }

    pub fn with_workflow_artifacts(mut self, negotiated: bool) -> Self {
        self.workflow_artifacts = negotiated;
        self
    }
    pub fn with_ci_task(mut self, negotiated: bool) -> Self {
        self.ci_task = negotiated;
        self
    }
}
impl WorkerLink for ClientLink {
    fn preparation_probe_negotiated(&self) -> bool {
        self.preparation_probe
    }
    fn preparation_observations_negotiated(&self) -> bool {
        self.preparation_observations
    }
    fn env_sync_negotiated(&self) -> bool {
        self.env_sync
    }
    fn matrix_dispatch_negotiated(&self) -> bool {
        self.matrix_dispatch
    }
    fn workflow_artifacts_negotiated(&self) -> bool {
        self.workflow_artifacts
    }
    fn ci_task_negotiated(&self) -> bool {
        self.ci_task
    }

    fn ci_discovery_negotiated(&self) -> bool {
        self.ci_discovery
    }
    fn cold_eligible(&self, task: &crate::ids::TaskId) -> bool {
        self.gate.as_ref().is_some_and(|g| g.cold_eligible(task))
    }
    fn prefer(&self, task: &crate::storage::QueuedTask, expected: u64, ttl: u64) -> bool {
        self.gate
            .as_ref()
            .is_some_and(|g| g.prefer(task, expected, ttl))
    }
    fn clear_preference(&self, task: Option<&crate::ids::TaskId>, run: Option<&crate::ids::RunId>) {
        if let Some(gate) = &self.gate {
            gate.clear_preference(task, run);
        }
    }

    fn cost_transport(&self) -> Option<crate::scheduler::measured::Transport> {
        self.gate.as_ref()?.cost_transport()
    }

    fn read_only_call(&self, request: Request) -> LinkFuture<'_> {
        Box::pin(async move {
            let timeout = if matches!(request, Request::EnvReport(_)) {
                Duration::from_secs(60)
            } else {
                self.timeout
            };
            self.client
                .call(request, timeout)
                .await
                .map_err(|e| match e {
                    CallError::Remote(e) => LinkError::Remote(e),
                    _ => LinkError::Unconfirmed,
                })
        })
    }
    fn placement_fence(&self) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        self.gate.as_ref().map(|gate| gate.placement_fence())
    }
    fn inputs_evaluated(&self, generation: u64, run: &crate::ids::RunId) {
        if let Some(gate) = &self.gate {
            gate.evaluated(generation, run);
        }
    }
    fn placement_inputs(&self) -> Option<PlacementInputs> {
        self.gate.as_ref().map(|gate| gate.placement_inputs())
    }
    fn input_refusals(&self) -> Vec<(crate::ids::RunId, String)> {
        self.gate
            .as_ref()
            .map(|gate| gate.input_refusals())
            .unwrap_or_default()
    }
    fn call(&self, request: Request) -> LinkFuture<'_> {
        Box::pin(async move {
            match self.client.call(request, self.timeout).await {
                Ok(response) => {
                    if let (Some(gate), Response::Inspect(inspect)) = (&self.gate, &response) {
                        gate.observe(inspect.worker_boot_id.clone(), inspect.agent_epoch)
                            .await;
                    }
                    Ok(response)
                }
                Err(CallError::Remote(e)) => Err(LinkError::Remote(e)),
                // Not sent at all is treated like no answer: the attempt is
                // asked for again on the next pass, which decides.
                Err(_) => Err(LinkError::Unconfirmed),
            }
        })
    }
}

/// A link with a deadline: a call that is not answered by then has no
/// answer. What a stalled worker costs a tick is bounded by this, whatever
/// the link behind it does.
pub(super) struct Budgeted {
    pub(super) inner: std::sync::Arc<dyn WorkerLink>,
    pub(super) deadline: tokio::time::Instant,
}

impl WorkerLink for Budgeted {
    fn preparation_probe_negotiated(&self) -> bool {
        self.inner.preparation_probe_negotiated()
    }
    fn preparation_observations_negotiated(&self) -> bool {
        self.inner.preparation_observations_negotiated()
    }
    fn env_sync_negotiated(&self) -> bool {
        self.inner.env_sync_negotiated()
    }
    fn matrix_dispatch_negotiated(&self) -> bool {
        self.inner.matrix_dispatch_negotiated()
    }
    fn workflow_artifacts_negotiated(&self) -> bool {
        self.inner.workflow_artifacts_negotiated()
    }
    fn ci_task_negotiated(&self) -> bool {
        self.inner.ci_task_negotiated()
    }

    fn ci_discovery_negotiated(&self) -> bool {
        self.inner.ci_discovery_negotiated()
    }
    fn cold_eligible(&self, task: &crate::ids::TaskId) -> bool {
        self.inner.cold_eligible(task)
    }
    fn prefer(&self, task: &crate::storage::QueuedTask, expected: u64, ttl: u64) -> bool {
        self.inner.prefer(task, expected, ttl)
    }
    fn clear_preference(&self, task: Option<&crate::ids::TaskId>, run: Option<&crate::ids::RunId>) {
        self.inner.clear_preference(task, run);
    }

    fn cost_transport(&self) -> Option<crate::scheduler::measured::Transport> {
        self.inner.cost_transport()
    }

    fn read_only_call(&self, request: Request) -> LinkFuture<'_> {
        Box::pin(async move {
            tokio::time::timeout_at(self.deadline, self.inner.read_only_call(request))
                .await
                .unwrap_or(Err(LinkError::Unconfirmed))
        })
    }
    fn placement_fence(&self) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        self.inner.placement_fence()
    }
    fn inputs_evaluated(&self, generation: u64, run: &crate::ids::RunId) {
        self.inner.inputs_evaluated(generation, run);
    }
    fn placement_inputs(&self) -> Option<PlacementInputs> {
        self.inner.placement_inputs()
    }
    fn input_refusals(&self) -> Vec<(crate::ids::RunId, String)> {
        self.inner.input_refusals()
    }
    fn call(&self, request: Request) -> LinkFuture<'_> {
        Box::pin(async move {
            match tokio::time::timeout_at(self.deadline, self.inner.call(request)).await {
                Ok(answer) => answer,
                Err(_) => Err(LinkError::Unconfirmed),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;
    #[tokio::test]
    async fn source_gate_is_bounded_and_identity_changes_reject_late_completion() {
        let gate = InputGate::default();
        let input = || ReadyInput {
            run_id: RunId::generate(),
            source_id: SourceId::generate(),
            manifest_digest: Digest::of_bytes(b"manifest"),
        };
        let initial = gate.generation();
        for _ in 0..100 {
            gate.publish(initial, input());
        }
        assert_eq!(gate.snapshot().len(), 64);
        let boot = WorkerBootId::parse("boot").unwrap();
        gate.observe(boot.clone(), AgentEpoch(1)).await;
        assert!(gate.snapshot().is_empty());
        gate.publish(initial, input());
        assert!(gate.snapshot().is_empty());
        let generation = gate.generation();
        gate.publish(generation, input());
        gate.observe(boot.clone(), AgentEpoch(1)).await;
        assert_eq!(gate.snapshot().len(), 1);
        gate.observe(boot, AgentEpoch(2)).await;
        assert!(gate.snapshot().is_empty());
        gate.publish(generation, input());
        assert!(gate.snapshot().is_empty());
    }
}
