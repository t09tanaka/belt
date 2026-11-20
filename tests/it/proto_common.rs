//! Shared helpers for the protocol tests: a fake worker built on the public API.
#![allow(dead_code)]

use belt::error::ApiError;
use belt::group::Keys;
use belt::ids::*;
use belt::protocol::hello::WorkerIdentity;
use belt::protocol::transport::duplex_pair;
use belt::protocol::{
    accept, connect, Accepted, ClientConfig, Connected, Incoming, Membership, ServerConfig,
};
use belt::secrets::key::ClusterKey;
use belt::wire::*;
use std::sync::Arc;
use std::time::Duration;

pub const T: Duration = Duration::from_secs(5);

pub fn identity(name: &str) -> WorkerIdentity {
    WorkerIdentity {
        capabilities: None,
        worker_id: WorkerId::parse(name).unwrap(),
        worker_boot_id: WorkerBootId::parse("boot-1").unwrap(),
        agent_epoch: AgentEpoch(1),
        profile: "default".into(),
        profile_fingerprint: Digest::of_bytes(b"profile"),
        coordinator_epoch: CoordinatorEpoch(1),
    }
}

/// The group key of these tests.
pub fn group_key() -> ClusterKey {
    ClusterKey::from_bytes(vec![7; 32]).unwrap()
}

/// The member a test's coordinator connects as, holding [`group_key`].
pub fn membership(name: &str) -> Membership {
    Membership {
        member: WorkerId::parse(name).unwrap(),
        key: group_key(),
    }
}

/// Put [`group_key`] where an agent reads its key, as a key file is kept:
/// lower-case hex, readable by its owner only.
pub fn place_group_key(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let text: String = group_key()
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::fs::write(path, format!("{text}\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

/// A coordinator of its own: each call is another member of the group.
pub fn client_cfg(cluster: &ClusterId) -> ClientConfig {
    let coordinator = CoordinatorId::generate();
    let name = format!("m-{}", &coordinator.as_str()[3..].to_lowercase());
    ClientConfig::new(
        cluster.clone(),
        coordinator,
        CoordinatorEpoch(1),
        membership(&name),
    )
}

pub fn server_cfg(cluster: &ClusterId) -> ServerConfig {
    ServerConfig::new(cluster.clone(), identity("w1"), Keys::only(group_key()))
}

pub async fn pair(cluster: &ClusterId) -> (Connected, Accepted) {
    let (a, b) = duplex_pair(64 * 1024);
    let (c, s) = tokio::join!(
        connect(a, client_cfg(cluster)),
        accept(b, server_cfg(cluster))
    );
    (c.unwrap(), s.unwrap())
}

pub type Handler = Arc<dyn Fn(Request) -> Option<Result<Response, ApiError>> + Send + Sync>;

/// Serve every inbound request in its own task. A handler returning `None`
/// never answers (the responder is kept alive, so the connection stays open).
pub fn serve(mut incoming: Incoming, handler: Handler) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut silent = Vec::new();
        while let Some(inbound) = incoming.next().await {
            let handler = Arc::clone(&handler);
            match handler(inbound.request) {
                Some(Ok(r)) => {
                    tokio::spawn(async move {
                        let _ = inbound.responder.respond(r).await;
                    });
                }
                Some(Err(e)) => {
                    tokio::spawn(async move {
                        let _ = inbound.responder.fail(e).await;
                    });
                }
                None => silent.push(inbound.responder),
            }
        }
    })
}

pub fn ack_request() -> Request {
    Request::AckCollected(AckCollectedRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: AttemptId::generate(),
        generation: Generation(1),
        result_digest: Digest::of_bytes(b"r"),
    })
}

pub fn ack_response(req: &Request) -> Response {
    let Request::AckCollected(r) = req else {
        panic!("not an ack")
    };
    Response::AckCollected(AckCollectedResponse {
        schema_version: SchemaVersion::CURRENT,
        attempt_id: r.attempt_id.clone(),
        released: true,
    })
}

pub fn pseudo_random(n: usize) -> Vec<u8> {
    let mut x = 0x2545F491u32;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}
