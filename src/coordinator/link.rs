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
}
impl Default for InputGate {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(State::new(next_gate_generation())),
            commit: std::sync::Arc::default(),
        }
    }
}
impl InputGate {
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
    /// A stable fence for session-generation invalidation. Gated links await it
    /// before resetting generation; a placement holds it through SQLite commit.
    /// Cache eviction does not invalidate the worker's verified source facts.
    fn placement_fence(&self) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        None
    }
    fn placement_inputs(&self) -> Option<PlacementInputs> {
        None
    }
    fn inputs_evaluated(&self, _generation: u64, _run: &crate::ids::RunId) {}
    fn call(&self, request: Request) -> LinkFuture<'_>;
}

/// A worker behind a protocol connection.
pub struct ClientLink {
    client: Client,
    timeout: Duration,
    gate: Option<std::sync::Arc<InputGate>>,
}

impl ClientLink {
    pub fn new(client: Client, timeout: Duration) -> ClientLink {
        ClientLink {
            client,
            timeout,
            gate: None,
        }
    }
}

impl ClientLink {
    pub fn gated(client: Client, timeout: Duration, gate: std::sync::Arc<InputGate>) -> Self {
        Self {
            client,
            timeout,
            gate: Some(gate),
        }
    }
}
impl WorkerLink for ClientLink {
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
    fn placement_fence(&self) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        self.inner.placement_fence()
    }
    fn inputs_evaluated(&self, generation: u64, run: &crate::ids::RunId) {
        self.inner.inputs_evaluated(generation, run);
    }
    fn placement_inputs(&self) -> Option<PlacementInputs> {
        self.inner.placement_inputs()
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
