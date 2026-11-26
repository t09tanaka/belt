//! `hello` negotiation (spec chapter 11). Pure: both sides' decisions, no I/O.
//!
//! `hello` settles the protocol version and the features and nothing else
//! (spec 24.2): which group the two sides belong to, and who the worker is,
//! are settled by the group proof that follows ([`check_proof`],
//! [`check_answer`]).
//!
//! A worker that does not fit gets `PROTOCOL_INCOMPATIBLE` and the connection
//! is closed; the coordinator never dispatches to it. The coordinator checks the
//! worker's answer as well, since the peer is not trusted to have chosen
//! something it offered.

use crate::error::GroupRefusal;
use crate::error::{ApiError, ErrorCode};
use crate::group::{self, Keys};
use crate::ids::{AgentEpoch, ClusterId, CoordinatorEpoch, Digest, WorkerBootId, WorkerId};
use crate::secrets::key::ClusterKey;
use crate::wire::{
    GroupProofRequest, GroupProofResponse, HelloRequest, HelloResponse, SchemaVersion,
};

/// Protocol versions this build speaks.
pub const PROTOCOL_VERSIONS: &[u32] = &[1];

/// Features this build implements. `blob_resume` is offset and digest resume of
/// blob transfer, `log_resume` the offset and sequence resume of logs.
pub const SUPPORTED_FEATURES: &[&str] = &[
    crate::wire::PREPARATION_PROBE_FEATURE,
    crate::setup::observation_transport::FEATURE,
    crate::forecast::COMMAND_FEATURE,
    "blob_resume",
    "log_resume",
    "output_bundles_v1",
    crate::toolchain::capability::FEATURE,
    crate::toolchain::contract::FEATURE,
    crate::wire::ENV_SYNC_FEATURE,
    crate::wire::CI_TASK_FEATURE,
    crate::wire::MATRIX_DISPATCH_FEATURE,
    crate::wire::CI_DISCOVERY_FEATURE,
    crate::wire::WORKFLOW_ARTIFACTS_FEATURE,
    group::MEMBERSHIP_FEATURE,
    group::IDENTITY_RETIRED_FEATURE,
    group::SHARED_ADMISSION_FEATURE,
    crate::wire::ATTEMPT_RELEASE_FEATURE,
    crate::wire::INPUT_MISSING_FEATURE,
    crate::toolchain::capability::SETUP_TOOLS_FEATURE,
    crate::toolchain::capability::SETUP_INSTALL_FEATURE,
    crate::toolchain::capability::PREPARE_SKIP_FEATURE,
];

/// The features no connection of this build goes without, on either side
/// (spec 24.10): there is no branch for a peer that lacks them.
pub const GROUP_FEATURES: &[&str] = &[
    group::MEMBERSHIP_FEATURE,
    group::IDENTITY_RETIRED_FEATURE,
    group::SHARED_ADMISSION_FEATURE,
];

/// What one side offers and requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalHello {
    pub cluster_id: ClusterId,
    pub versions: Vec<u32>,
    pub features: Vec<String>,
    /// Features without which the peer is refused.
    pub required_features: Vec<String>,
}

/// The agreed protocol version and features.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiated {
    pub version: u32,
    pub features: Vec<String>,
}

fn incompatible(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::ProtocolIncompatible, message)
}

/// Features in `ours` that `theirs` also lists, in our order.
fn common_features(ours: &[String], theirs: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for f in ours {
        if theirs.contains(f) && !out.contains(f) {
            out.push(f.clone());
        }
    }
    out
}

fn missing_required(required: &[String], have: &[String]) -> Option<String> {
    let missing: Vec<&str> = required
        .iter()
        .filter(|f| !have.contains(f))
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        None
    } else {
        Some(missing.join(", "))
    }
}

/// The worker's decision on a coordinator's `hello`. The `schema_version` is
/// read permissively by the wire layer and judged here.
pub fn negotiate(local: &LocalHello, req: &HelloRequest) -> Result<Negotiated, ApiError> {
    if let Err(e) = req.schema_version.ensure_current() {
        return Err(incompatible(e.to_string()));
    }
    let version = local
        .versions
        .iter()
        .filter(|v| req.protocol_versions.contains(v))
        .max()
        .copied()
        .ok_or_else(|| {
            incompatible(format!(
                "no common protocol version (worker speaks {:?}, coordinator offered {:?})",
                local.versions, req.protocol_versions
            ))
        })?;
    let features = common_features(&local.features, &req.features);
    if let Some(missing) = missing_required(&local.required_features, &features) {
        return Err(incompatible(format!(
            "required features missing: {missing}"
        )));
    }
    Ok(Negotiated { version, features })
}

/// The coordinator's decision on a worker's `hello` answer.
pub fn verify_response(
    offered: &HelloRequest,
    required_features: &[String],
    resp: &HelloResponse,
) -> Result<Negotiated, ApiError> {
    if let Err(e) = resp.schema_version.ensure_current() {
        return Err(incompatible(e.to_string()));
    }
    if !group::is_nonce(&resp.nonce) {
        return Err(incompatible("the hello answer carries no usable nonce"));
    }
    if !offered.protocol_versions.contains(&resp.protocol_version) {
        return Err(incompatible(format!(
            "worker chose protocol version {} that was not offered",
            resp.protocol_version
        )));
    }
    if let Some(extra) = resp.features.iter().find(|f| !offered.features.contains(f)) {
        return Err(incompatible(format!(
            "worker enabled feature {extra} that was not offered"
        )));
    }
    if let Some(missing) = missing_required(required_features, &resp.features) {
        return Err(incompatible(format!(
            "required features missing: {missing}"
        )));
    }
    Ok(Negotiated {
        version: resp.protocol_version,
        features: resp.features.clone(),
    })
}

/// The worker's answer to `hello`: what was negotiated and a nonce.
pub fn hello_response(negotiated: &Negotiated, nonce: String) -> HelloResponse {
    HelloResponse {
        schema_version: SchemaVersion::CURRENT,
        protocol_version: negotiated.version,
        features: negotiated.features.clone(),
        nonce,
    }
}

/// The receiving member's decision on a group proof: the key the peer
/// proved to hold. A peer of another group and a peer whose proof does not
/// hold under a key this member has (its current one or, during a rotation,
/// its next one) are refused with the reason. The key is looked up by the
/// fingerprint the peer names, and the proof decides.
pub fn check_proof<'k>(
    cluster_id: &ClusterId,
    keys: &'k Keys,
    receiver_nonce: &str,
    hello: &HelloRequest,
    proof: &GroupProofRequest,
) -> Result<&'k ClusterKey, ApiError> {
    if let Err(e) = proof.schema_version.ensure_current() {
        return Err(incompatible(e.to_string()));
    }
    if hello.cluster_id != *cluster_id {
        return Err(ApiError::group_refused(
            GroupRefusal::GroupMismatch,
            "cluster_id mismatch: this member belongs to another group",
        ));
    }
    let wrong_key = || {
        ApiError::group_refused(
            GroupRefusal::GroupKeyMismatch {
                fingerprint: group::fingerprint(&keys.current),
            },
            "the group key does not match the one this member holds",
        )
    };
    if !group::is_nonce(&proof.nonce) {
        return Err(incompatible("the group proof carries no usable nonce"));
    }
    let key = keys
        .by_fingerprint(&proof.key_fingerprint)
        .ok_or_else(wrong_key)?;
    let expected = group::connector_proof(
        key,
        cluster_id,
        &hello.coordinator_id,
        &proof.member,
        receiver_nonce,
    );
    if !group::proof_matches(&expected, &proof.proof) {
        return Err(wrong_key());
    }
    Ok(key)
}

/// The connecting member's decision on the answer to its proof. Until this
/// holds, nothing but `hello` and the proof has been sent to the peer.
pub fn check_answer(
    key: &ClusterKey,
    connector_nonce: &str,
    answer: &GroupProofResponse,
) -> Result<(), ApiError> {
    if let Err(e) = answer.schema_version.ensure_current() {
        return Err(incompatible(e.to_string()));
    }
    let expected = group::receiver_proof(key, &answer.worker_id, connector_nonce);
    if !group::proof_matches(&expected, &answer.proof) {
        return Err(ApiError::group_refused(
            GroupRefusal::GroupKeyMismatch {
                fingerprint: group::fingerprint(key),
            },
            "the worker did not prove that it holds the group key",
        ));
    }
    Ok(())
}

/// What a worker reports about itself once the peer proved its membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerIdentity {
    pub capabilities: Option<crate::toolchain::capability::WorkerCapabilities>,
    pub worker_id: WorkerId,
    pub worker_boot_id: WorkerBootId,
    pub agent_epoch: AgentEpoch,
    pub profile: String,
    pub profile_fingerprint: Digest,
    /// The highest epoch this worker has seen of the connecting member.
    pub coordinator_epoch: CoordinatorEpoch,
}

impl WorkerIdentity {
    /// The answer to a proof that held: this worker's own proof and who it is.
    pub fn response(&self, negotiated: &Negotiated, proof: String) -> GroupProofResponse {
        GroupProofResponse {
            proof,
            capabilities: negotiated
                .features
                .iter()
                .any(|v| v == crate::toolchain::capability::FEATURE)
                .then(|| self.capabilities.clone())
                .flatten()
                .map(|mut report| {
                    report.features.retain(|v| negotiated.features.contains(v));
                    report
                }),
            schema_version: SchemaVersion::CURRENT,
            worker_id: self.worker_id.clone(),
            worker_boot_id: self.worker_boot_id.clone(),
            agent_epoch: self.agent_epoch,
            profile: self.profile.clone(),
            profile_fingerprint: self.profile_fingerprint.clone(),
            coordinator_epoch: self.coordinator_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{CoordinatorId, RequestId};

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|x| x.to_string()).collect()
    }

    fn local(cluster: &ClusterId) -> LocalHello {
        LocalHello {
            cluster_id: cluster.clone(),
            versions: vec![1, 2],
            features: s(&["a", "b"]),
            required_features: vec![],
        }
    }

    fn request(cluster: &ClusterId) -> HelloRequest {
        HelloRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            cluster_id: cluster.clone(),
            coordinator_id: CoordinatorId::generate(),
            coordinator_epoch: CoordinatorEpoch(1),
            protocol_versions: vec![2, 3],
            features: s(&["b", "c"]),
        }
    }

    /// One version of a member, by what it speaks in `hello` on either side.
    struct Version {
        versions: Vec<u32>,
        features: Vec<String>,
    }
    impl Version {
        fn worker(&self, cluster: &ClusterId) -> LocalHello {
            LocalHello {
                cluster_id: cluster.clone(),
                versions: self.versions.clone(),
                features: self.features.clone(),
                required_features: vec![],
            }
        }
        fn submitter(&self, cluster: &ClusterId) -> HelloRequest {
            HelloRequest {
                protocol_versions: self.versions.clone(),
                features: self.features.clone(),
                ..request(cluster)
            }
        }
    }

    /// The submitter `from` reaches the worker `to`: both decide, and they
    /// agree on the same version and features.
    fn reach(from: &Version, to: &Version, cluster: &ClusterId) -> Negotiated {
        let asked = from.submitter(cluster);
        let chosen = negotiate(&to.worker(cluster), &asked).unwrap();
        let checked = verify_response(
            &asked,
            &crate::protocol::session::required_features(),
            &hello_response(&chosen, group::nonce().unwrap()),
        )
        .unwrap();
        assert_eq!(checked, chosen);
        checked
    }

    #[test]
    fn a57_versions_n_and_n_plus_1_reach_each_other_both_ways() {
        let cluster = ClusterId::generate();
        // N lacks an optional feature this build has; N+1 has one more that
        // this build does not know, and a protocol version more.
        let n = Version {
            versions: PROTOCOL_VERSIONS.to_vec(),
            features: crate::protocol::session::offered_features()
                .into_iter()
                .filter(|f| f.as_str() != crate::wire::ENV_SYNC_FEATURE)
                .collect(),
        };
        let mut next = Version {
            versions: PROTOCOL_VERSIONS.to_vec(),
            features: crate::protocol::session::offered_features(),
        };
        next.versions
            .push(PROTOCOL_VERSIONS.iter().max().unwrap() + 1);
        next.features.push("later_feature_v1".into());
        for (from, to) in [(&n, &next), (&next, &n)] {
            let agreed = reach(from, to, &cluster);
            assert_eq!(agreed.version, *PROTOCOL_VERSIONS.iter().max().unwrap());
            // Only what both have: no optional feature one side lacks.
            assert!(!agreed.features.iter().any(|f| f == "later_feature_v1"));
            assert!(!agreed
                .features
                .iter()
                .any(|f| f == crate::wire::ENV_SYNC_FEATURE));
            for f in crate::protocol::session::required_features() {
                assert!(agreed.features.contains(&f), "{f}");
            }
        }
        // Between two N+1 members the new things are used.
        let agreed = reach(&next, &next, &cluster);
        assert_eq!(agreed.version, *next.versions.iter().max().unwrap());
        assert!(agreed.features.iter().any(|f| f == "later_feature_v1"));
        // A peer without a feature required since the first public version
        // is no N: a member of this build does not use it as a worker.
        let older = Version {
            versions: PROTOCOL_VERSIONS.to_vec(),
            features: n
                .features
                .iter()
                .filter(|f| f.as_str() != group::SHARED_ADMISSION_FEATURE)
                .cloned()
                .collect(),
        };
        let asked = next.submitter(&cluster);
        let chosen = negotiate(&older.worker(&cluster), &asked).unwrap();
        assert!(verify_response(
            &asked,
            &crate::protocol::session::required_features(),
            &hello_response(&chosen, group::nonce().unwrap()),
        )
        .is_err());
    }

    fn identity() -> WorkerIdentity {
        WorkerIdentity {
            capabilities: None,
            worker_id: WorkerId::parse("w1").unwrap(),
            worker_boot_id: WorkerBootId::parse("boot1").unwrap(),
            agent_epoch: AgentEpoch(4),
            profile: "p".into(),
            profile_fingerprint: Digest::of_bytes(b"p"),
            coordinator_epoch: CoordinatorEpoch(9),
        }
    }

    #[test]
    fn capabilities_are_reported_only_when_negotiated_and_features_are_intersected() {
        use crate::toolchain::capability::{WorkerCapabilities, FEATURE};
        let mut id = identity();
        id.capabilities = Some(WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            cpu_cores: None,
            memory_bytes: None,
            schema_version: 1,
            os: "macos".into(),
            arch: "aarch64".into(),
            profile: "p".into(),
            profile_digest: Digest::of_bytes(b"definition"),
            execution_uid: 1,
            protocol_versions: vec![1],
            profile_fingerprint: Digest::of_bytes(b"p"),
            tools: Default::default(),
            features: [FEATURE.into(), "private-feature".into()].into(),
            rustup: Vec::new(),
            installs: Vec::new(),
            path_tools: Vec::new(),
        });
        let mut n = Negotiated {
            version: 1,
            features: vec![FEATURE.into()],
        };
        let response = id.response(&n, String::new());
        assert_eq!(
            response.capabilities.unwrap().features,
            [FEATURE.into()].into()
        );
        assert!(id
            .capabilities
            .as_ref()
            .unwrap()
            .features
            .contains("private-feature"));
        n.features.clear();
        assert!(id.response(&n, String::new()).capabilities.is_none());
        id.capabilities = None;
        n.features.push(FEATURE.into());
        assert!(id.response(&n, String::new()).capabilities.is_none());
    }

    fn code(e: ApiError) -> (ErrorCode, String) {
        (e.code, e.message)
    }

    /// The proof a member named `member` gives for `hello` under the test key.
    fn proof_of(hello: &HelloRequest, member: &str, receiver_nonce: &str) -> GroupProofRequest {
        proof_with(&group::test_key(), hello, member, receiver_nonce)
    }

    fn proof_with(
        key: &ClusterKey,
        hello: &HelloRequest,
        member: &str,
        receiver_nonce: &str,
    ) -> GroupProofRequest {
        let member = WorkerId::parse(member).unwrap();
        GroupProofRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            key_fingerprint: group::fingerprint(key),
            nonce: group::nonce().unwrap(),
            proof: group::connector_proof(
                key,
                &hello.cluster_id,
                &hello.coordinator_id,
                &member,
                receiver_nonce,
            ),
            member,
        }
    }

    fn other_key() -> ClusterKey {
        ClusterKey::from_bytes(vec![9; 32]).unwrap()
    }

    #[test]
    fn a_proof_holds_under_the_current_or_the_next_key_and_names_which() {
        let c = ClusterId::generate();
        let hello = request(&c);
        let nonce = group::nonce().unwrap();
        let keys = Keys {
            current: group::test_key(),
            next: Some(other_key()),
        };
        let current = check_proof(&c, &keys, &nonce, &hello, &proof_of(&hello, "main", &nonce));
        assert_eq!(current.unwrap(), &keys.current);
        let next = proof_with(&other_key(), &hello, "main", &nonce);
        assert_eq!(
            check_proof(&c, &keys, &nonce, &hello, &next).unwrap(),
            keys.next.as_ref().unwrap()
        );
        // Without a rotation the other key is not accepted.
        let only = Keys::only(group::test_key());
        let e = check_proof(&c, &only, &nonce, &hello, &next).unwrap_err();
        assert_eq!(
            e.group_refusal,
            Some(GroupRefusal::GroupKeyMismatch {
                fingerprint: group::fingerprint(&group::test_key())
            }),
            "the refusal shows the key this member signs with"
        );
        assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
    }

    #[test]
    fn a_proof_is_refused_when_anything_it_covers_differs() {
        let c = ClusterId::generate();
        let hello = request(&c);
        let nonce = group::nonce().unwrap();
        let keys = Keys::only(group::test_key());
        let good = proof_of(&hello, "main", &nonce);
        assert!(check_proof(&c, &keys, &nonce, &hello, &good).is_ok());
        let wrong_key =
            |e: ApiError| matches!(e.group_refusal, Some(GroupRefusal::GroupKeyMismatch { .. }));
        // Another nonce of the receiver: a proof is not replayed.
        let replay = check_proof(&c, &keys, &group::nonce().unwrap(), &hello, &good);
        assert!(wrong_key(replay.unwrap_err()));
        // Another name than the one proven.
        let mut renamed = good.clone();
        renamed.member = WorkerId::parse("sub").unwrap();
        assert!(wrong_key(
            check_proof(&c, &keys, &nonce, &hello, &renamed).unwrap_err()
        ));
        // Another coordinator id than the one proven.
        let mut stolen = hello.clone();
        stolen.coordinator_id = CoordinatorId::generate();
        assert!(wrong_key(
            check_proof(&c, &keys, &nonce, &stolen, &good).unwrap_err()
        ));
        // The right fingerprint with a proof made under another key, a cut
        // proof, an empty one.
        let mut forged = proof_with(&other_key(), &hello, "main", &nonce);
        forged.key_fingerprint = good.key_fingerprint.clone();
        assert!(wrong_key(
            check_proof(&c, &keys, &nonce, &hello, &forged).unwrap_err()
        ));
        for bad in [&good.proof[..63], ""] {
            let mut cut = good.clone();
            cut.proof = bad.into();
            assert!(wrong_key(
                check_proof(&c, &keys, &nonce, &hello, &cut).unwrap_err()
            ));
        }
        // A proof without a nonce of its own cannot be answered.
        let mut blind = good.clone();
        blind.nonce = "x".into();
        let e = check_proof(&c, &keys, &nonce, &hello, &blind).unwrap_err();
        assert!(e.message.contains("nonce") && e.group_refusal.is_none());
        let mut future = good.clone();
        future.schema_version = SchemaVersion(99);
        assert!(check_proof(&c, &keys, &nonce, &hello, &future).is_err());
    }

    #[test]
    fn the_connecting_side_refuses_a_worker_that_does_not_prove_the_key() {
        let n = Negotiated {
            version: 1,
            features: vec![],
        };
        let key = group::test_key();
        let nonce = group::nonce().unwrap();
        let me = identity();
        let good = me.response(&n, group::receiver_proof(&key, &me.worker_id, &nonce));
        assert!(check_answer(&key, &nonce, &good).is_ok());
        let refused = |answer: &GroupProofResponse, nonce: &str| {
            let e = check_answer(&key, nonce, answer).unwrap_err();
            assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
            assert!(matches!(
                e.group_refusal,
                Some(GroupRefusal::GroupKeyMismatch { .. })
            ));
        };
        // Another key, another nonce than this connection's, a name other
        // than the one the proof was made over, and the connecting side's
        // own proof sent back.
        refused(
            &me.response(
                &n,
                group::receiver_proof(&other_key(), &me.worker_id, &nonce),
            ),
            &nonce,
        );
        refused(&good, &group::nonce().unwrap());
        let mut renamed = good.clone();
        renamed.worker_id = WorkerId::parse("w2").unwrap();
        refused(&renamed, &nonce);
        let c = ClusterId::generate();
        let hello = request(&c);
        let mut echo = good.clone();
        echo.proof = proof_of(&hello, me.worker_id.as_str(), &nonce).proof;
        refused(&echo, &nonce);
        let mut empty = good.clone();
        empty.proof.clear();
        refused(&empty, &nonce);
        // An answer of another schema is not read at all: the versions do
        // not fit, which says nothing about the key.
        let mut other_schema = good.clone();
        other_schema.schema_version = SchemaVersion(SchemaVersion::CURRENT.0 + 1);
        let e = check_answer(&key, &nonce, &other_schema).unwrap_err();
        assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
        assert_eq!(e.group_refusal, None);
    }

    #[test]
    fn picks_the_highest_common_version_and_the_common_features() {
        let c = ClusterId::generate();
        let n = negotiate(&local(&c), &request(&c)).unwrap();
        assert_eq!(
            n,
            Negotiated {
                version: 2,
                features: s(&["b"])
            }
        );
        let mut req = request(&c);
        req.protocol_versions = vec![1, 2];
        assert_eq!(negotiate(&local(&c), &req).unwrap().version, 2);
        req.protocol_versions = vec![1];
        assert_eq!(negotiate(&local(&c), &req).unwrap().version, 1);
    }

    #[test]
    fn refuses_other_schema_versions_and_disjoint_versions_and_leaves_the_group_to_the_proof() {
        let c = ClusterId::generate();
        let mut req = request(&c);
        req.schema_version = SchemaVersion(99);
        let (code_, msg) = code(negotiate(&local(&c), &req).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("99"));

        // `hello` settles the version and the features only: a peer of
        // another group is refused by its proof, with the reason.
        let other = request(&ClusterId::generate());
        assert!(negotiate(&local(&c), &other).is_ok());
        let keys = Keys::only(group::test_key());
        let nonce = group::nonce().unwrap();
        let e =
            check_proof(&c, &keys, &nonce, &other, &proof_of(&other, "main", &nonce)).unwrap_err();
        assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
        assert_eq!(e.group_refusal, Some(GroupRefusal::GroupMismatch));
        assert!(e.message.contains("cluster_id"));

        let mut req = request(&c);
        req.protocol_versions = vec![5];
        let (code_, msg) = code(negotiate(&local(&c), &req).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("protocol version"));
        req.protocol_versions = vec![];
        assert!(negotiate(&local(&c), &req).is_err());
    }

    #[test]
    fn a_missing_required_feature_refuses_and_a_present_one_passes() {
        let c = ClusterId::generate();
        let mut l = local(&c);
        l.required_features = s(&["b"]);
        assert!(negotiate(&l, &request(&c)).is_ok());
        l.required_features = s(&["a", "c"]);
        let (code_, msg) = code(negotiate(&l, &request(&c)).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        // `c` is offered by the peer but unknown to us; `a` is ours but not offered.
        assert!(msg.contains("a, c"), "{msg}");
    }

    #[test]
    fn duplicate_offers_do_not_duplicate_features() {
        let c = ClusterId::generate();
        let mut l = local(&c);
        l.features = s(&["b", "b"]);
        assert_eq!(negotiate(&l, &request(&c)).unwrap().features, s(&["b"]));
    }

    #[test]
    fn the_coordinator_accepts_what_it_offered() {
        let c = ClusterId::generate();
        let req = request(&c);
        let n = negotiate(&local(&c), &req).unwrap();
        let resp = hello_response(&n, group::nonce().unwrap());
        assert_eq!(resp.protocol_version, 2);
        // The answer to `hello` is the version, the features and a nonce:
        // nothing in it says who answers.
        let said = serde_json::to_value(&resp).unwrap();
        let mut fields: Vec<&str> = said
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            ["features", "nonce", "protocol_version", "schema_version"]
        );
        // Who answers comes with the worker's own proof.
        let who = identity().response(&n, "proof".into());
        assert_eq!(who.proof, "proof");
        assert_eq!(who.worker_id, identity().worker_id);
        assert_eq!(who.agent_epoch, AgentEpoch(4));
        assert_eq!(who.coordinator_epoch, CoordinatorEpoch(9));
        assert_eq!(who.profile, "p");
        assert_eq!(who.worker_boot_id, identity().worker_boot_id);
        assert_eq!(who.profile_fingerprint, identity().profile_fingerprint);
        assert_eq!(verify_response(&req, &[], &resp).unwrap(), n);
        assert_eq!(verify_response(&req, &s(&["b"]), &resp).unwrap(), n);
    }

    #[test]
    fn the_coordinator_refuses_a_bad_answer() {
        let c = ClusterId::generate();
        let req = request(&c);
        let n = negotiate(&local(&c), &req).unwrap();
        let good = hello_response(&n, group::nonce().unwrap());

        let mut r = good.clone();
        r.schema_version = SchemaVersion(7);
        let (code_, msg) = code(verify_response(&req, &[], &r).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains('7'));

        for nonce in ["", "not-a-nonce"] {
            let mut r = good.clone();
            r.nonce = nonce.into();
            assert!(code(verify_response(&req, &[], &r).unwrap_err())
                .1
                .contains("nonce"));
        }

        let mut r = good.clone();
        r.protocol_version = 1;
        assert!(code(verify_response(&req, &[], &r).unwrap_err())
            .1
            .contains("not offered"));

        let mut r = good.clone();
        r.features = s(&["b", "zzz"]);
        assert!(code(verify_response(&req, &[], &r).unwrap_err())
            .1
            .contains("zzz"));

        let (code_, msg) = code(verify_response(&req, &s(&["c"]), &good).unwrap_err());
        assert_eq!(code_, ErrorCode::ProtocolIncompatible);
        assert!(msg.contains("required"));
    }

    #[test]
    fn the_shipped_constants_are_consistent() {
        assert!(!PROTOCOL_VERSIONS.is_empty());
        assert!(SUPPORTED_FEATURES.contains(&"blob_resume"));
        // The group features are offered and required by both sides.
        assert_eq!(group::MEMBERSHIP_FEATURE, "group_membership_v1");
        assert_eq!(group::IDENTITY_RETIRED_FEATURE, "identity_retired_v1");
        assert_eq!(group::SHARED_ADMISSION_FEATURE, "shared_admission_v1");
        assert_eq!(GROUP_FEATURES.len(), 3);
        for feature in GROUP_FEATURES {
            assert!(SUPPORTED_FEATURES.contains(feature));
            assert!(crate::protocol::session::required_features()
                .iter()
                .any(|f| f == feature));
        }
        // Offered, never required: a worker of this build takes the frozen
        // plan as a blob, and one that does not is simply given no CI task.
        assert!(SUPPORTED_FEATURES.contains(&"ci_task_v1"));
        assert_eq!(crate::wire::CI_TASK_FEATURE, "ci_task_v1");
        assert!(!crate::protocol::session::required_features()
            .iter()
            .any(|f| f == crate::wire::CI_TASK_FEATURE));
    }

    #[test]
    fn every_feature_a_capability_report_carries_is_negotiated() {
        // A connection keeps only the negotiated features of a report
        // (`WorkerIdentity::response` and the Inspect answer of
        // `worker::serve`), so a report feature this build does not offer
        // never reaches the parent and nothing that needs it is ever met.
        for feature in crate::toolchain::capability::FEATURES {
            assert!(
                SUPPORTED_FEATURES.contains(feature),
                "{feature} is reported but not offered"
            );
        }
    }
}
