//! The agent on tokio: one task per connection, one blocking call per
//! request, and the periodic housekeeping.

use super::agent::{Agent, Session};
use crate::error::{ApiError, ErrorCode, GroupRefusal};
use crate::ids::{ClusterId, CoordinatorEpoch, CoordinatorId};
use crate::protocol::hello::WorkerIdentity;
use crate::protocol::transport::listen_unix;
use crate::protocol::{accept, ConnectError, InboundRequest, Io, ProvenMember, ServerConfig};
use crate::storage::MemberPin;
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
    /// The agent could not read its group key. No `hello` was answered.
    #[error("worker identity: {}", .0.message)]
    Identity(ApiError),
    #[error(transparent)]
    Connect(#[from] ConnectError),
}

/// What the agent tells a member whose group proof held, or why it refuses
/// it (spec 24.2, 24.3). The member's name and coordinator id are recorded
/// together at its first proof; a name that comes with another coordinator
/// id, or a coordinator id under another name, is refused and nothing is
/// recorded. So is a member that told this worker it retired the identity
/// the worker still has. The epoch in the answer is that member's own.
///
/// A probe ([`crate::protocol::ClientConfig::probe`]) names no coordinator:
/// it is told who answers, nothing is recorded, and no request is served.
pub fn admit(agent: &Agent, peer: &ProvenMember) -> Result<WorkerIdentity, ApiError> {
    agent.check_rekey()?;
    let cfg = agent.config();
    let mut me = WorkerIdentity {
        capabilities: agent.capabilities(),
        worker_id: cfg.worker_id.clone(),
        worker_boot_id: agent.advertised_boot(),
        agent_epoch: agent.agent_epoch(),
        profile: cfg.profile.clone(),
        profile_fingerprint: cfg.profile_fingerprint.clone(),
        coordinator_epoch: CoordinatorEpoch(0),
    };
    if peer.coordinator_id == CoordinatorId::nil() {
        return Ok(me);
    }
    let store = agent.store().for_member(&peer.coordinator_id);
    let failed = |what: &str, e: crate::storage::StorageError| {
        ApiError::new(e.code(), format!("{what}: {e}"))
    };
    let pin = store
        .pin_member(peer.member.as_str(), crate::platform::wall_ms())
        .map_err(|e| failed("the member record", e))?;
    let mismatch = |message: String| {
        Err(ApiError::group_refused(
            GroupRefusal::MemberIdentityMismatch,
            format!("{message}; choose a distinct member name with belt init --name when initializing a new host"),
        ))
    };
    match pin {
        MemberPin::Pinned => {}
        MemberPin::NameTaken { .. } => {
            return mismatch(format!(
                "the member name {} is recorded here with another coordinator id; a machine that was rebuilt or copied does not take over a name (add it again from this member to replace the record)",
                peer.member
            ))
        }
        MemberPin::IdTaken { name } => {
            return mismatch(format!(
                "this coordinator id is recorded here as member {name}, not {}",
                peer.member
            ))
        }
    }
    if store
        .identity_retired(&agent.advertised_boot())
        .map_err(|e| failed("the retired identities", e))?
    {
        return Err(ApiError::group_refused(
            GroupRefusal::IdentityRetired,
            "this member retired the identity this worker still has",
        ));
    }
    me.coordinator_epoch = store
        .max_coordinator_epoch()
        .map_err(|e| failed("the highest coordinator epoch", e))?;
    Ok(me)
}

/// Who the agent says it is, as a probe is told (no member, epoch 0).
pub fn identity(agent: &Agent) -> Result<WorkerIdentity, ApiError> {
    admit(
        agent,
        &ProvenMember {
            coordinator_id: CoordinatorId::nil(),
            coordinator_epoch: CoordinatorEpoch(0),
            member: agent.config().worker_id.clone(),
            key_fingerprint: String::new(),
        },
    )
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
    serve_proven(agent, io, cluster_id, permits, more_features, false).await
}

/// The init resident's in-process link to its own worker (a socketpair that
/// never leaves the process). A group key switch does not close it: the
/// proof guards links between machines, and the resident treats the end of
/// this link as fatal. A new worker identity (send-key) still closes it.
pub async fn serve_local_connection(
    agent: Arc<Agent>,
    io: Io,
    cluster_id: ClusterId,
    permits: Arc<Semaphore>,
) -> Result<(), ServeError> {
    serve_proven(agent, io, cluster_id, permits, &[], true).await
}

async fn serve_proven(
    agent: Arc<Agent>,
    io: Io,
    cluster_id: ClusterId,
    permits: Arc<Semaphore>,
    more_features: &[&str],
    in_process: bool,
) -> Result<(), ServeError> {
    // The keys are read for each connection: after a switch the new key is
    // the one a peer must prove with. Without a readable key nothing is
    // answered, not even `hello`.
    let key_path = agent.config().group_key_path.clone();
    let keys = tokio::task::spawn_blocking(move || crate::group::load_keys(&key_path))
        .await
        .map_err(|e| {
            ServeError::Identity(ApiError::new(
                ErrorCode::Internal,
                format!("group key task: {e}"),
            ))
        })?
        .map_err(|e| {
            ServeError::Identity(ApiError::new(
                ErrorCode::Internal,
                format!("the group key cannot be read: {e}"),
            ))
        })?;
    let connection_boot = agent.advertised_boot();
    let mut key_changes = agent.group_key_changes();
    key_changes.mark_unchanged();
    let admitting = agent.clone();
    let mut config = ServerConfig::admitting(
        cluster_id,
        agent.config().worker_id.clone(),
        keys,
        Arc::new(move |peer| admit(&admitting, peer)),
    );
    let offered = &mut config.local.features;
    offered.extend(more_features.iter().map(|f| f.to_string()));
    let accepted = accept(io, config).await?;
    if accepted.coordinator_id == CoordinatorId::nil() {
        // A probe: it was told who answers. Nothing is served on it.
        return Ok(());
    }
    let session = Session {
        coordinator_id: accepted.coordinator_id.clone(),
        coordinator_epoch: accepted.coordinator_epoch,
    };
    let proven_with = accepted.key_fingerprint.clone();
    let capabilities_negotiated = accepted
        .negotiated
        .features
        .iter()
        .any(|v| v == crate::toolchain::capability::FEATURE);
    let negotiated_features = accepted.negotiated.features;
    let mut incoming = accepted.incoming;
    loop {
        let next = tokio::select! {
            next = incoming.next() => next,
            changed = key_changes.changed() => {
                if changed.is_err() || connection_boot != agent.advertised_boot() {
                    return Ok(());
                }
                if in_process {
                    continue;
                }
                // The key was switched. A connection proven with a key this
                // worker still accepts (the one that is current now) stays;
                // any other is closed and the peer proves again.
                let path = agent.config().group_key_path.clone();
                let proven = proven_with.clone();
                let still =
                    tokio::task::spawn_blocking(move || crate::group::still_holds(&path, &proven))
                        .await
                        .unwrap_or(false);
                if still {
                    continue;
                }
                return Ok(());
            }
        };
        let Some(InboundRequest {
            mut request,
            responder,
        }) = next
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
        let session = session.clone();
        let request_boot = connection_boot.clone();
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
            if matches!(&request,crate::wire::Request::SubmitAttempt(r) if r.payload.ci_task.as_ref().is_some_and(crate::wire::requires_discovery_feature))
                && !negotiated_features
                    .iter()
                    .any(|f| f == crate::wire::CI_DISCOVERY_FEATURE)
            {
                let _ = responder
                    .fail(ApiError::new(
                        ErrorCode::ProtocolIncompatible,
                        "runner discovery was not negotiated",
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
            let done = tokio::task::spawn_blocking(move || {
                agent.handle_identity(session, request, Some(&request_boot))
            })
            .await;
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
