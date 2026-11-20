//! The handshake that starts a connection, on both sides: `hello`, then the
//! mutual group proof (spec 24.2).
//!
//! [`connect`] is the connecting member's: it sends `hello`, checks the
//! answer, proves that it holds the group key, checks the worker's proof and
//! only then returns a [`Client`]. [`accept`] is the worker's: it waits for
//! `hello`, negotiates, checks the peer's proof, and only then says who it
//! is and returns the [`Incoming`] queue of requests to serve. An
//! incompatible peer gets `PROTOCOL_INCOMPATIBLE` and the connection is
//! closed; a peer that is not a member gets the reason with it.

use super::frame::{Body, Frame, FrameError, MAX_FRAME_LEN};
use super::hello::{
    check_answer, check_proof, hello_response, negotiate, verify_response, LocalHello, Negotiated,
    WorkerIdentity, GROUP_FEATURES, PROTOCOL_VERSIONS, SUPPORTED_FEATURES,
};
use super::io::{read_frame, write_frame};
use super::mux::{start, Client, Incoming};
use super::transport::Io;
use crate::error::{ApiError, ErrorCode, GroupRefusal};
use crate::group::{self, Keys};
use crate::ids::{ClusterId, CoordinatorEpoch, CoordinatorId, RequestId, WorkerId};
use crate::secrets::key::ClusterKey;
use crate::wire::{
    ErrorResponse, GroupProofRequest, GroupProofResponse, HelloRequest, Message, Request, Response,
    SchemaVersion,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("{0}")]
    Frame(FrameError),
    /// The peer refused us with this error.
    #[error("peer refused hello: {} ({})", .0.message, .0.code)]
    Refused(ApiError),
    /// We refused the peer with this error (and told it, on the worker side).
    #[error("incompatible peer: {}", .0.message)]
    Incompatible(ApiError),
    #[error("unexpected first frame: {0}")]
    Unexpected(&'static str),
    #[error("hello timed out")]
    TimedOut,
    #[error("connection closed during hello")]
    Closed,
}

impl ConnectError {
    /// The error code to report for a failed connection.
    pub fn api_error(&self) -> ApiError {
        match self {
            ConnectError::Refused(e) | ConnectError::Incompatible(e) => e.clone(),
            ConnectError::TimedOut | ConnectError::Closed => {
                ApiError::new(ErrorCode::CommunicationUnknown, self.to_string())
            }
            ConnectError::Frame(_) | ConnectError::Unexpected(_) => {
                ApiError::new(ErrorCode::ProtocolIncompatible, self.to_string())
            }
        }
    }
}

impl ConnectError {
    /// Why the group proof was refused, by the peer or by this side.
    pub fn group_refusal(&self) -> Option<&GroupRefusal> {
        match self {
            ConnectError::Refused(e) | ConnectError::Incompatible(e) => e.group_refusal.as_ref(),
            _ => None,
        }
    }

    /// The fingerprint the peer showed when it refused this member's key.
    /// A member that holds that key as its next one has missed the switch
    /// ([`crate::group::switch_if_peer_is_on_next`]).
    pub fn peer_key_fingerprint(&self) -> Option<&str> {
        match self {
            ConnectError::Refused(ApiError {
                group_refusal: Some(GroupRefusal::GroupKeyMismatch { fingerprint }),
                ..
            }) => Some(fingerprint),
            _ => None,
        }
    }
}

impl From<FrameError> for ConnectError {
    fn from(e: FrameError) -> Self {
        ConnectError::Frame(e)
    }
}

async fn read_first(
    io: &mut Io,
    max_frame: usize,
    timeout: Duration,
) -> Result<Frame, ConnectError> {
    match tokio::time::timeout(timeout, read_frame(&mut io.reader, max_frame)).await {
        Err(_) => Err(ConnectError::TimedOut),
        Ok(Err(e)) => Err(e.into()),
        Ok(Ok(None)) => Err(ConnectError::Closed),
        Ok(Ok(Some(frame))) => Ok(frame),
    }
}

// ---------------------------------------------------------------------------
// coordinator side
// ---------------------------------------------------------------------------

/// Who the connecting member is in the group: its name and the key it
/// proves with (its current one).
#[derive(Debug, Clone)]
pub struct Membership {
    pub member: WorkerId,
    pub key: ClusterKey,
}

impl Membership {
    /// The member `member` with the key at `key_path`, read now: a key that
    /// was switched since the last connection is the one used.
    pub fn load(member: WorkerId, key_path: &Path) -> crate::secrets::Result<Self> {
        Ok(Membership {
            member,
            key: crate::secrets::key::load(key_path)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub cluster_id: ClusterId,
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
    pub membership: Membership,
    pub protocol_versions: Vec<u32>,
    pub features: Vec<String>,
    pub required_features: Vec<String>,
    pub hello_timeout: Duration,
    pub max_frame: usize,
}

impl ClientConfig {
    pub fn new(
        cluster_id: ClusterId,
        coordinator_id: CoordinatorId,
        coordinator_epoch: CoordinatorEpoch,
        membership: Membership,
    ) -> Self {
        ClientConfig {
            cluster_id,
            coordinator_id,
            coordinator_epoch,
            membership,
            protocol_versions: PROTOCOL_VERSIONS.to_vec(),
            features: SUPPORTED_FEATURES.iter().map(|f| f.to_string()).collect(),
            required_features: Vec::new(),
            hello_timeout: Duration::from_secs(10),
            max_frame: MAX_FRAME_LEN,
        }
    }

    /// A look at who answers, for a caller that is no coordinator (a check
    /// of a worker bridge from the command line). It proves the key like
    /// any connection and is told who the worker is; the worker records
    /// nothing about it and serves no request on it.
    pub fn probe(cluster_id: ClusterId, key: ClusterKey) -> Self {
        ClientConfig::new(
            cluster_id,
            CoordinatorId::nil(),
            CoordinatorEpoch(0),
            Membership {
                member: WorkerId::parse(PROBE_MEMBER).expect("a valid name"),
                key,
            },
        )
    }
}

/// The name a probe gives ([`ClientConfig::probe`]). It is never recorded.
pub const PROBE_MEMBER: &str = "belt-probe";

/// A connection that passed `hello` and on which both sides proved that
/// they hold the group key.
pub struct Connected {
    pub client: Client,
    /// The group both sides proved to belong to.
    pub cluster_id: ClusterId,
    /// The worker's identity, epoch and profile as it reported them after
    /// the proof. A worker id that differs from the one expected is a new
    /// identity (a restarted ephemeral worker), not a reconnect; the
    /// coordinator decides what that means.
    pub worker: GroupProofResponse,
    pub negotiated: Negotiated,
}

/// The features a coordinator of this build offers in `hello`.
pub fn offered_features() -> Vec<String> {
    SUPPORTED_FEATURES
        .iter()
        .map(|f| f.to_string())
        .chain([crate::outputs::FEATURE.to_string()])
        .collect()
}

/// The features without which a coordinator of this build refuses a worker.
pub fn required_features() -> Vec<String> {
    std::iter::once(crate::outputs::FEATURE)
        .chain(GROUP_FEATURES.iter().copied())
        .map(str::to_string)
        .collect()
}

pub async fn connect(io: Io, mut cfg: ClientConfig) -> Result<Connected, ConnectError> {
    cfg.required_features.extend(required_features());
    connect_requiring(io, cfg).await
}

/// `connect` that requires only what `cfg` lists, and the group proof: no
/// connection goes without that one. A check of a worker on another version
/// uses it: what that version must have is decided by the caller, not by
/// this build.
///
/// Nothing but `hello` and the proof is sent before the worker's own proof
/// holds: the [`Client`] is made only then, so no caller can send work,
/// secrets or source to a peer that did not prove it holds the key.
pub async fn connect_requiring(
    mut io: Io,
    mut cfg: ClientConfig,
) -> Result<Connected, ConnectError> {
    cfg.features.push(crate::outputs::FEATURE.into());
    if !cfg
        .required_features
        .iter()
        .any(|f| f == group::MEMBERSHIP_FEATURE)
    {
        cfg.required_features.push(group::MEMBERSHIP_FEATURE.into());
    }
    let hello = HelloRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        cluster_id: cfg.cluster_id.clone(),
        coordinator_id: cfg.coordinator_id.clone(),
        coordinator_epoch: cfg.coordinator_epoch,
        protocol_versions: cfg.protocol_versions.clone(),
        features: cfg.features.clone(),
    };
    let request_id = hello.request_id.clone();
    let frame = Frame::message(0, Message::Request(Request::Hello(hello.clone())));
    write_frame(&mut io.writer, &frame, cfg.max_frame).await?;
    let reply = read_first(&mut io, cfg.max_frame, cfg.hello_timeout).await?;
    let response = match reply.body {
        Body::Message(Message::Response {
            request_id: id,
            response: Response::Hello(resp),
        }) if id == request_id => resp,
        Body::Message(Message::Error(ErrorResponse { error, .. })) => {
            return Err(ConnectError::Refused(error))
        }
        _ => return Err(ConnectError::Unexpected("expected a hello response")),
    };
    let negotiated = verify_response(&hello, &cfg.required_features, &response)
        .map_err(ConnectError::Incompatible)?;

    // The proof, and the worker's. The same deadline as `hello` applies.
    let nonce = group::nonce().map_err(|e| {
        ConnectError::Incompatible(ApiError::new(ErrorCode::Internal, e.to_string()))
    })?;
    let key = &cfg.membership.key;
    let proof = GroupProofRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        member: cfg.membership.member.clone(),
        key_fingerprint: group::fingerprint(key),
        nonce: nonce.clone(),
        proof: group::connector_proof(
            key,
            &cfg.cluster_id,
            &cfg.coordinator_id,
            &cfg.membership.member,
            &response.nonce,
        ),
    };
    let proof_id = proof.request_id.clone();
    let frame = Frame::message(0, Message::Request(Request::GroupProof(proof)));
    write_frame(&mut io.writer, &frame, cfg.max_frame).await?;
    let reply = read_first(&mut io, cfg.max_frame, cfg.hello_timeout).await?;
    let worker = match reply.body {
        Body::Message(Message::Response {
            request_id: id,
            response: Response::GroupProof(resp),
        }) if id == proof_id => resp,
        Body::Message(Message::Error(ErrorResponse { error, .. })) => {
            return Err(ConnectError::Refused(error))
        }
        _ => return Err(ConnectError::Unexpected("expected a group proof response")),
    };
    if let Err(error) = check_answer(key, &nonce, &worker) {
        let _ = io.writer.shutdown().await;
        return Err(ConnectError::Incompatible(error));
    }
    let Io { reader, writer } = io;
    let (shared, outbox, _) = start(reader, writer, cfg.max_frame, false);
    Ok(Connected {
        client: Client::new(shared, outbox),
        cluster_id: cfg.cluster_id,
        worker,
        negotiated,
    })
}

// ---------------------------------------------------------------------------
// worker side
// ---------------------------------------------------------------------------

/// A peer whose group proof held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenMember {
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
    /// The name it gave.
    pub member: WorkerId,
    /// The fingerprint of the key it proved to hold.
    pub key_fingerprint: String,
}

/// The worker's decision on a proven member, and what it tells it about
/// itself. An error is sent as the refusal and the connection is closed.
pub type Admit = Arc<dyn Fn(&ProvenMember) -> Result<WorkerIdentity, ApiError> + Send + Sync>;

#[derive(Clone)]
pub struct ServerConfig {
    pub local: LocalHello,
    /// This member's name: what its own proof is made over.
    pub member: WorkerId,
    /// The keys a peer may prove with.
    pub keys: Keys,
    /// Runs after a proof held and before anything is answered, on a
    /// blocking thread.
    pub admit: Admit,
    pub hello_timeout: Duration,
    pub max_frame: usize,
}

impl ServerConfig {
    /// A worker that tells every proven member the same `identity`.
    pub fn new(cluster_id: ClusterId, identity: WorkerIdentity, keys: Keys) -> Self {
        let member = identity.worker_id.clone();
        Self::admitting(
            cluster_id,
            member,
            keys,
            Arc::new(move |_| Ok(identity.clone())),
        )
    }

    /// A worker named `member` that decides per proven member.
    pub fn admitting(cluster_id: ClusterId, member: WorkerId, keys: Keys, admit: Admit) -> Self {
        ServerConfig {
            local: LocalHello {
                cluster_id,
                versions: PROTOCOL_VERSIONS.to_vec(),
                features: SUPPORTED_FEATURES.iter().map(|f| f.to_string()).collect(),
                required_features: Vec::new(),
            },
            member,
            keys,
            admit,
            hello_timeout: Duration::from_secs(10),
            max_frame: MAX_FRAME_LEN,
        }
    }
}

/// A connection whose peer passed `hello` and proved its membership.
pub struct Accepted {
    pub incoming: Incoming,
    pub coordinator_id: CoordinatorId,
    pub coordinator_epoch: CoordinatorEpoch,
    /// The name the peer gave.
    pub member: WorkerId,
    /// The fingerprint of the key the peer proved to hold.
    pub key_fingerprint: String,
    pub negotiated: Negotiated,
}

async fn refuse(io: &mut Io, max_frame: usize, request_id: Option<RequestId>, error: ApiError) {
    let frame = Frame::message(
        0,
        Message::Error(ErrorResponse {
            schema_version: SchemaVersion::CURRENT,
            request_id,
            error,
        }),
    );
    let _ = write_frame(&mut io.writer, &frame, max_frame).await;
    let _ = io.writer.shutdown().await;
}

/// Serve the start of a connection: `hello`, then the peer's group proof.
/// Until the proof holds the worker answers nothing about itself (no
/// identity, epoch, profile or capability) and takes no other request; the
/// request queue is made only after it.
pub async fn accept(mut io: Io, mut cfg: ServerConfig) -> Result<Accepted, ConnectError> {
    // The group features are part of every worker of this build, whatever
    // else it offers: no connection goes without the proof.
    for required in std::iter::once(crate::outputs::FEATURE).chain(GROUP_FEATURES.iter().copied()) {
        if !cfg.local.features.iter().any(|f| f == required) {
            cfg.local.features.push(required.into());
        }
        cfg.local.required_features.push(required.into());
    }
    let first = read_first(&mut io, cfg.max_frame, cfg.hello_timeout).await?;
    let Body::Message(Message::Request(Request::Hello(hello))) = first.body else {
        let error = ApiError::new(ErrorCode::InvalidInput, "hello must be the first request");
        refuse(&mut io, cfg.max_frame, None, error).await;
        return Err(ConnectError::Unexpected("first frame was not hello"));
    };
    let negotiated = match negotiate(&cfg.local, &hello) {
        Ok(n) => n,
        Err(error) => {
            refuse(
                &mut io,
                cfg.max_frame,
                Some(hello.request_id.clone()),
                error.clone(),
            )
            .await;
            return Err(ConnectError::Incompatible(error));
        }
    };
    let nonce = match group::nonce() {
        Ok(n) => n,
        Err(e) => {
            let error = ApiError::new(ErrorCode::Internal, e.to_string());
            refuse(
                &mut io,
                cfg.max_frame,
                Some(hello.request_id.clone()),
                error.clone(),
            )
            .await;
            return Err(ConnectError::Incompatible(error));
        }
    };
    let reply = Frame::message(
        first.stream,
        Message::Response {
            request_id: hello.request_id.clone(),
            response: Response::Hello(hello_response(&negotiated, nonce.clone())),
        },
    );
    write_frame(&mut io.writer, &reply, cfg.max_frame).await?;

    let second = read_first(&mut io, cfg.max_frame, cfg.hello_timeout).await?;
    let Body::Message(Message::Request(Request::GroupProof(proof))) = second.body else {
        let error = ApiError::new(
            ErrorCode::InvalidInput,
            "the group proof must follow hello: nothing else is served before it",
        );
        refuse(&mut io, cfg.max_frame, None, error).await;
        return Err(ConnectError::Unexpected(
            "second frame was not the group proof",
        ));
    };
    let proof_id = proof.request_id.clone();
    let key = match check_proof(&cfg.local.cluster_id, &cfg.keys, &nonce, &hello, &proof) {
        Ok(key) => key,
        Err(error) => {
            refuse(&mut io, cfg.max_frame, Some(proof_id), error.clone()).await;
            return Err(ConnectError::Incompatible(error));
        }
    };
    let proven = ProvenMember {
        coordinator_id: hello.coordinator_id.clone(),
        coordinator_epoch: hello.coordinator_epoch,
        member: proof.member.clone(),
        key_fingerprint: proof.key_fingerprint.clone(),
    };
    let (admit, asked) = (cfg.admit.clone(), proven.clone());
    let admitted = tokio::task::spawn_blocking(move || admit(&asked))
        .await
        .unwrap_or_else(|e| {
            Err(ApiError::new(
                ErrorCode::Internal,
                format!("the admission of a member failed: {e}"),
            ))
        });
    let identity = match admitted {
        Ok(identity) => identity,
        Err(error) => {
            refuse(&mut io, cfg.max_frame, Some(proof_id), error.clone()).await;
            return Err(ConnectError::Incompatible(error));
        }
    };
    let answer = identity.response(
        &negotiated,
        group::receiver_proof(key, &cfg.member, &proof.nonce),
    );
    let reply = Frame::message(
        second.stream,
        Message::Response {
            request_id: proof_id,
            response: Response::GroupProof(answer),
        },
    );
    write_frame(&mut io.writer, &reply, cfg.max_frame).await?;
    let Io { reader, writer } = io;
    let (_, _, incoming) = start(reader, writer, cfg.max_frame, true);
    Ok(Accepted {
        incoming: incoming.expect("a serving connection has an inbound queue"),
        coordinator_id: proven.coordinator_id,
        coordinator_epoch: proven.coordinator_epoch,
        member: proven.member,
        key_fingerprint: proven.key_fingerprint,
        negotiated,
    })
}
