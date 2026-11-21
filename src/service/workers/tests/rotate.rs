//! Reuse the workers-add private-home fixture; SSH dispatches the real helper.
use super::*;
use crate::{
    group,
    service::group::rotate,
    storage::{CoordinatorStore, WorkerStore},
};

struct Network {
    peers: Vec<World>,
    events: Vec<(String, String)>,
    fail: Option<(String, String, bool)>,
}
impl Remote for Network {
    fn bridge_hello(
        &mut self,
        _: &SshTarget,
        _: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        panic!("rotation uses only fixed helpers")
    }
    fn run(
        &mut self,
        target: &SshTarget,
        shell: &str,
        input: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        assert_eq!(shell, rotate::HELPER);
        let value: serde_json::Value = serde_json::from_slice(input).unwrap();
        let action = value["action"]["action"].as_str().unwrap().to_string();
        let name = target.as_str().to_string();
        self.events.push((name.clone(), action.clone()));
        let failing = self
            .fail
            .as_ref()
            .is_some_and(|(n, a, _)| n == &name && a == &action);
        if failing && !self.fail.as_ref().unwrap().2 {
            self.fail = None;
            return Err("injected failure".into());
        }
        let peer = self
            .peers
            .iter()
            .find(|p| p.spec.worker_id.as_str() == name)
            .unwrap();
        let result = rotate::accept(
            &peer.env,
            &peer.spec,
            serde_json::from_slice(input).unwrap(),
        );
        if failing {
            self.fail = None;
            return Err("lost reply".into());
        }
        let (code, reply) = match result {
            Ok(result) => (0, serde_json::json!({"ok":true,"result":result})),
            Err(error) => (1, serde_json::json!({"ok":false,"error":error})),
        };
        Ok(RemoteOutput {
            code: Some(code),
            stdout: serde_json::to_vec(&reply).unwrap(),
            stderr: vec![],
        })
    }
}
fn fixture() -> (World, Network) {
    let parent = World::new();
    let mut peers = Vec::new();
    for name in ["a", "b"] {
        let mut peer = World::new();
        peer.spec.worker_id = WorkerId::parse(name).unwrap();
        peer.spec.expected_cluster = parent.spec.expected_cluster.clone();
        fs::copy(&parent.spec.cluster_key_path, &peer.spec.cluster_key_path).unwrap();
        register(&parent.env, name, name, None, false).unwrap();
        register(&peer.env, "local", "local", None, false).unwrap();
        peers.push(peer);
    }
    (
        parent,
        Network {
            peers,
            events: vec![],
            fail: None,
        },
    )
}
fn keys(w: &World) -> group::Keys {
    group::load_keys(&w.spec.cluster_key_path).unwrap()
}
fn run(
    w: &World,
    n: &mut Network,
    remove: &[&str],
    skip: &[&str],
) -> Result<serde_json::Value, ApiError> {
    rotate::rotate(
        n,
        &w.env,
        &w.spec,
        &remove.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        &skip.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    )
}
#[test]
fn union_refuses_before_any_distribution() {
    let (w, mut n) = fixture();
    register(&n.peers[1].env, "unknown", "unknown", None, false).unwrap();
    let old = keys(&w).current;
    assert!(run(&w, &mut n, &[], &[])
        .unwrap_err()
        .message
        .contains("unknown"));
    assert_eq!(keys(&w).current, old);
    assert!(keys(&w).next.is_none());
    assert!(n.events.iter().all(|(_, a)| a == "Collect"));
}
#[test]
fn distribution_precedes_switch_and_local_switches_last() {
    let (w, mut n) = fixture();
    let old = keys(&w).current;
    let result = run(&w, &mut n, &[], &[]).unwrap();
    assert_ne!(keys(&w).current, old);
    assert_eq!(result["generation"], 2);
    assert_eq!(
        n.events.iter().map(|(_, a)| a.as_str()).collect::<Vec<_>>(),
        [
            "Collect",
            "Collect",
            "Distribute",
            "Distribute",
            "Switch",
            "Switch"
        ]
    );
    for peer in &n.peers {
        assert_eq!(keys(peer).current, keys(&w).current);
        assert!(keys(peer).next.is_none());
    }
}
#[test]
fn resume_mid_distribution_and_mid_switch_including_lost_reply() {
    for (action, after) in [
        ("Distribute", false),
        ("Distribute", true),
        ("Switch", false),
        ("Switch", true),
    ] {
        let (w, mut n) = fixture();
        let old = keys(&w).current;
        n.fail = Some(("b".into(), action.into(), after));
        assert!(run(&w, &mut n, &[], &[]).unwrap_err().message.contains("b"));
        assert_eq!(keys(&w).current, old, "local must switch last");
        let next = keys(&w).next.unwrap();
        if action == "Distribute" {
            assert!(n.peers.iter().all(|p| keys(p).current == old));
        }
        run(&w, &mut n, &[], &[]).unwrap();
        assert_eq!(keys(&w).current, next);
        assert!(n.peers.iter().all(|p| keys(p).current == next));
    }
}
#[test]
fn resume_after_local_self_switch_uses_recorded_fingerprint() {
    let (w, mut n) = fixture();
    n.fail = Some(("b".into(), "Switch".into(), false));
    run(&w, &mut n, &[], &[]).unwrap_err();
    let next = keys(&w).next.unwrap();
    assert!(
        group::switch_if_peer_is_on_next(&w.spec.cluster_key_path, &group::fingerprint(&next))
            .unwrap()
    );
    run(&w, &mut n, &[], &[]).unwrap();
    assert_eq!(keys(&w).current, next);
    assert!(keys(&w).next.is_none());
}
#[test]
fn foreign_next_is_not_overwritten() {
    let (w, mut n) = fixture();
    let foreign =
        crate::secrets::key::init(&group::next_key_path(&n.peers[0].spec.cluster_key_path))
            .unwrap();
    let old = keys(&n.peers[0]).current;
    let error = run(&w, &mut n, &[], &[]).unwrap_err();
    assert!(error.message.contains("finish the earlier rotation"));
    assert_eq!(keys(&n.peers[0]).next.unwrap(), foreign);
    assert_eq!(keys(&n.peers[0]).current, old);
    assert!(!n.events.iter().any(|(_, a)| a == "Switch"));
}
#[test]
fn skip_does_not_contact_or_change_deferred_peer() {
    let (w, mut n) = fixture();
    let old = keys(&n.peers[1]).current;
    run(&w, &mut n, &[], &["b"]).unwrap();
    assert_eq!(keys(&n.peers[1]).current, old);
    assert!(keys(&n.peers[1]).next.is_none());
    assert!(n.events.iter().all(|(name, _)| name != "b"));
}
#[test]
fn remove_records_timestamp_and_prints_manual_ssh_steps_even_on_failure() {
    let (w, mut n) = fixture();
    let old = keys(&n.peers[1]).current;
    n.fail = Some(("a".into(), "Distribute".into(), false));
    let error = run(&w, &mut n, &["b"], &[]).unwrap_err();
    assert!(error.message.contains("authorized_keys"));
    let removed = CoordinatorStore::open_read_only(&w.spec.state_dir)
        .unwrap()
        .removed_members()
        .unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].0, "b");
    assert!(removed[0].1 > 0);
    assert_eq!(
        registrations(&w.env)
            .unwrap()
            .iter()
            .map(|w| w.id.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
    let result = run(&w, &mut n, &["b"], &[]).unwrap();
    let text = result["guidance"].to_string();
    for expected in [
        "On local",
        "On a",
        "authorized_keys",
        "workers forget b",
        "lease_seconds",
        "TERM stop grace",
    ] {
        assert!(text.contains(expected), "{text}");
    }
    assert_eq!(keys(&n.peers[1]).current, old);
    assert!(n.events.iter().all(|(name, _)| name != "b"));
    assert_eq!(
        CoordinatorStore::open_read_only(&w.spec.state_dir)
            .unwrap()
            .removed_members()
            .unwrap(),
        removed
    );
    assert_eq!(
        WorkerStore::open_read_only(&w.spec.worker_state_dir)
            .unwrap()
            .group_keys()
            .unwrap()
            .0
            .unwrap()
            .generation,
        2
    );
}

#[test]
fn receiving_only_origin_records_removal_without_creating_coordinator() {
    let (mut w, mut n) = fixture();
    w.spec.state_dir = w.spec.worker_state_dir.clone();
    let path = w.spec.worker_state_dir.join("removed-members.json");
    run(&w, &mut n, &["b"], &[]).unwrap();
    let removed: BTreeMap<String, u64> = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert!(removed["b"] > 0);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(!w.spec.state_dir.join("coordinator.db").exists());
    assert_eq!(registrations(&w.env).unwrap().len(), 1);
}

#[test]
fn explicitly_removed_unknown_union_member_is_allowed() {
    let (w, mut n) = fixture();
    register(&n.peers[0].env, "removed", "removed", None, false).unwrap();
    run(&w, &mut n, &["removed"], &[]).unwrap();
    assert_eq!(
        CoordinatorStore::open_read_only(&w.spec.state_dir)
            .unwrap()
            .removed_members()
            .unwrap()[0]
            .0,
        "removed"
    );
}

#[test]
fn removing_a_registration_keeps_every_other_byte() {
    let text = "# my belt config\nschema_version = 1\n\n[[workers]]\nid = \"a\" # first\nssh = \"me@a\"\n\n[[workers]] # gone soon\nid = \"b\"\nssh = \"me@b\"\n\n[defaults]\n# keep\n";
    let kept = drop_worker_tables(text, &["b".to_string()]).unwrap();
    assert_eq!(
        kept,
        "# my belt config\nschema_version = 1\n\n[[workers]]\nid = \"a\" # first\nssh = \"me@a\"\n\n[defaults]\n# keep\n"
    );
    assert_eq!(drop_worker_tables(text, &[]).unwrap(), text);
}

#[test]
fn send_key_uses_current_even_with_next_and_replaces_pin_every_time() {
    let (w, mut n) = fixture();
    let own = WorkerStore::open(&w.spec.worker_state_dir, &w.spec.worker_id).unwrap();
    let former = crate::ids::CoordinatorId::generate();
    own.for_member(&former).replace_member_pin("a", 1).unwrap();
    crate::secrets::key::init(&group::next_key_path(&w.spec.cluster_key_path)).unwrap();
    let peer = &n.peers[0];
    let expected_id = CoordinatorStore::membership_writer(&peer.spec.state_dir)
        .unwrap()
        .coordinator_id()
        .clone();
    let peer_store = WorkerStore::open(&peer.spec.worker_state_dir, &peer.spec.worker_id).unwrap();
    let binding = peer.spec.worker_state_dir.join("worker-binding.json");
    crate::worker::rekey::write(
        &binding,
        &serde_json::to_vec(&(
            1,
            peer.spec.expected_cluster.as_ref().unwrap(),
            &peer.spec.worker_id,
            "old-digest",
        ))
        .unwrap(),
    )
    .unwrap();
    let next =
        crate::secrets::key::init(&group::next_key_path(&peer.spec.cluster_key_path)).unwrap();
    peer_store
        .set_next_group_key(&crate::storage::KeyGeneration {
            generation: 9,
            fingerprint: group::fingerprint(&next),
        })
        .unwrap();
    let first = rotate::send_key(&mut n, &w.env, &w.spec, "a").unwrap();
    assert_eq!(first["identity_regenerated"], true);
    assert_eq!(first["fingerprint"], group::fingerprint(&keys(&w).current));
    assert_eq!(
        group::fingerprint(&keys(&n.peers[0]).current),
        group::fingerprint(&keys(&w).current)
    );
    assert!(keys(&n.peers[0]).next.is_none());
    assert!(peer_store.group_keys().unwrap().1.is_none());
    {
        use sha2::Digest;
        let bound: serde_json::Value =
            serde_json::from_slice(&fs::read(&binding).unwrap()).unwrap();
        assert_eq!(
            bound[3],
            format!(
                "{:x}",
                sha2::Sha256::digest(fs::read(&n.peers[0].spec.cluster_key_path).unwrap())
            )
        );
        assert!(!n.peers[0]
            .spec
            .worker_state_dir
            .join("send-key.key")
            .exists());
    }
    assert_eq!(
        own.for_member(&expected_id).pin_member("a", 2).unwrap(),
        crate::storage::MemberPin::Pinned
    );
    assert_ne!(
        own.for_member(&former).pin_member("a", 3).unwrap(),
        crate::storage::MemberPin::Pinned
    );
    let second = rotate::send_key(&mut n, &w.env, &w.spec, "a").unwrap();
    assert_ne!(first["instance"], second["instance"]);
    assert_eq!(
        n.events,
        [
            ("a".into(), "SendKey".into()),
            ("a".into(), "SendKey".into())
        ]
    );
}

#[test]
fn send_key_names_an_unlisted_member_without_ssh() {
    let (w, mut n) = fixture();
    let error = rotate::send_key(&mut n, &w.env, &w.spec, "missing").unwrap_err();
    assert!(error.message.contains("missing"));
    assert!(error.message.contains("[[workers]]"));
    assert!(n.events.is_empty());
}
