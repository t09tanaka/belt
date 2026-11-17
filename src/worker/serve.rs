//! The agent on tokio: one task per connection, one blocking call per
//! request, and the periodic housekeeping.

use super::agent::{Agent, Session};
use crate::error::{ApiError, ErrorCode};
use crate::ids::ClusterId;
use crate::protocol::hello::WorkerIdentity;
use crate::protocol::transport::listen_unix;
use crate::protocol::{accept, ConnectError, InboundRequest, Io, ServerConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio::sync::{watch, Semaphore};

/// Requests of one agent that run at once. The rest wait in the connection's
/// inbound queue, which has its own limits.
pub const MAX_CONCURRENT_REQUESTS: usize = 64;

/// Why a connection ended.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The agent could not say who it is (the database did not answer). No
    /// `hello` was answered: an epoch made up here would be believed.
    #[error("worker identity: {}", .0.message)]
    Identity(ApiError),
    #[error(transparent)]
    Connect(#[from] ConnectError),
}

/// What the agent says about itself in `hello`.
pub fn identity(agent: &Agent) -> Result<WorkerIdentity, ApiError> {
    let cfg = agent.config();
    let coordinator_epoch = agent
        .store()
        .max_coordinator_epoch()
        .map_err(|e| ApiError::new(e.code(), format!("the highest coordinator epoch: {e}")))?;
    Ok(WorkerIdentity {
        capabilities: agent.capabilities(),
        worker_id: cfg.worker_id.clone(),
        worker_boot_id: cfg.boot_id.clone(),
        agent_epoch: agent.agent_epoch(),
        profile: cfg.profile.clone(),
        profile_fingerprint: cfg.profile_fingerprint.clone(),
        coordinator_epoch,
    })
}

/// Serve one connection until the peer closes it.
pub async fn serve_connection(
    agent: Arc<Agent>,
    io: Io,
    cluster_id: ClusterId,
    permits: Arc<Semaphore>,
) -> Result<(), ServeError> {
    serve_connection_offering(agent, io, cluster_id, permits, &[]).await
}

/// [`serve_connection`] with `more_features` offered in `hello` beside the
/// ones every worker of this build offers. For a feature whose worker side
/// is built and tested before the build offers it (`ci_task_v1`, until the
/// frozen plan can be delivered).
pub async fn serve_connection_offering(
    agent: Arc<Agent>,
    io: Io,
    cluster_id: ClusterId,
    permits: Arc<Semaphore>,
    more_features: &[&str],
) -> Result<(), ServeError> {
    let identity_agent = agent.clone();
    let me = tokio::task::spawn_blocking(move || identity(&identity_agent))
        .await
        .map_err(|e| {
            ServeError::Identity(ApiError::new(
                ErrorCode::Internal,
                format!("worker identity task: {e}"),
            ))
        })?
        .map_err(ServeError::Identity)?;
    let mut config = ServerConfig::new(cluster_id, me);
    let offered = &mut config.local.features;
    offered.extend(more_features.iter().map(|f| f.to_string()));
    let accepted = accept(io, config).await?;
    let session = Session {
        coordinator_epoch: accepted.coordinator_epoch,
    };
    let capabilities_negotiated = accepted
        .negotiated
        .features
        .iter()
        .any(|v| v == crate::toolchain::capability::FEATURE);
    let negotiated_features = accepted.negotiated.features;
    let mut incoming = accepted.incoming;
    loop {
        let Some(InboundRequest {
            mut request,
            responder,
        }) = incoming.next().await
        else {
            return Ok(());
        };
        // No further request is taken from this connection until this one
        // may run: what cannot run yet stays queued where the protocol layer
        // counts it. An idle connection holds no permit.
        let Ok(permit) = permits.clone().acquire_owned().await else {
            return Ok(());
        };
        let agent = agent.clone();
        let negotiated_features = negotiated_features.clone();
        tokio::spawn(async move {
            let uses_workflow_artifacts = matches!(
                &request,
                crate::wire::Request::WorkflowArtifact(_)
            ) || matches!(&request, crate::wire::Request::SubmitAttempt(r) if r.payload.ci_task.as_ref().is_some_and(|ci| ci.artifact_workflow.is_some()));
            if uses_workflow_artifacts
                && !negotiated_features
                    .iter()
                    .any(|f| f == crate::wire::WORKFLOW_ARTIFACTS_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "workflow artifact exchange was not negotiated",
                    ))
                    .await;
                return;
            }
            if matches!(&request, crate::wire::Request::PreparationProbe(_))
                && !negotiated_features
                    .iter()
                    .any(|v| v == crate::wire::PREPARATION_PROBE_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "preparation probe was not negotiated",
                    ))
                    .await;
                return;
            }
            if matches!(&request, crate::wire::Request::ForecastCommand(_))
                && !negotiated_features
                    .iter()
                    .any(|v| v == crate::forecast::COMMAND_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "command forecast was not negotiated",
                    ))
                    .await;
                return;
            }
            if matches!(&request, crate::wire::Request::EnvSync(_))
                && !negotiated_features
                    .iter()
                    .any(|v| v == crate::wire::ENV_SYNC_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "env_sync was not negotiated",
                    ))
                    .await;
                return;
            }
            let requires_toolchains = matches!(
                &request,
                crate::wire::Request::ResolveToolchains(_)
            ) || matches!(&request, crate::wire::Request::SubmitAttempt(r) if r.payload.toolchains.is_some());
            if requires_toolchains
                && !negotiated_features
                    .iter()
                    .any(|v| v == crate::toolchain::contract::FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "frozen toolchains were not negotiated",
                    ))
                    .await;
                return;
            }
            if matches!(&request, crate::wire::Request::SubmitAttempt(r) if r.payload.ci_task.as_ref().is_some_and(|ci| ci.dispatch.is_some()))
                && !negotiated_features
                    .iter()
                    .any(|f| f == crate::wire::MATRIX_DISPATCH_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "derived matrix dispatch was not negotiated",
                    ))
                    .await;
                return;
            }
            if matches!(&request, crate::wire::Request::SubmitAttempt(r) if r.payload.ci_task.is_some())
                && !negotiated_features
                    .iter()
                    .any(|v| v == crate::wire::CI_TASK_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "CI tasks were not negotiated",
                    ))
                    .await;
                return;
            }
            if !capabilities_negotiated
                && matches!(&request, crate::wire::Request::SubmitAttempt(r) if !r.payload.requirements.is_empty())
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "constrained attempt requires worker_capabilities_v1 negotiation",
                    ))
                    .await;
                drop(permit);
                return;
            }
            let observations_negotiated = negotiated_features
                .iter()
                .any(|v| v == crate::setup::observation_transport::FEATURE);
            if !observations_negotiated {
                if let crate::wire::Request::Inspect(r) = &mut request {
                    r.preparation_observations = None;
                }
            }
            let done = tokio::task::spawn_blocking(move || agent.handle(session, request)).await;
            let _ = match done {
                Ok(Ok(mut response)) => {
                    if let crate::wire::Response::Inspect(r) = &mut response {
                        if !observations_negotiated {
                            r.preparation_observations = None;
                        }
                        if !capabilities_negotiated {
                            r.capabilities = None;
                        } else if let Some(report) = &mut r.capabilities {
                            report.features.retain(|v| negotiated_features.contains(v));
                        }
                    }
                    responder.respond(response).await
                }
                Ok(Err(error)) => responder.fail(error).await,
                Err(_) => {
                    responder
                        .fail(ApiError::new(ErrorCode::Internal, "the request panicked"))
                        .await
                }
            };
            drop(permit);
        });
    }
}

/// Listen on the agent's socket until `stop` turns true. The state directory
/// is 0700 (the agent made it), which the listener requires.
pub async fn serve(
    agent: Arc<Agent>,
    cluster_id: ClusterId,
    tick: Duration,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener: UnixListener = listen_unix(&agent.config().socket_path())?;
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let mut ticker = tokio::time::interval(tick);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                // Not awaited: a slow pass (a cleanup, a GC) must not keep
                // connections waiting. The agent runs one pass at a time and
                // drops a tick that arrives during one.
                let agent = agent.clone();
                tokio::task::spawn_blocking(move || agent.tick());
            }
            conn = listener.accept() => {
                let (stream, _) = conn?;
                let (agent, cluster, permits) = (agent.clone(), cluster_id.clone(), permits.clone());
                tokio::spawn(async move {
                    let _ = serve_connection(agent, Io::from_stream(stream), cluster, permits).await;
                });
            }
        }
    }
}
