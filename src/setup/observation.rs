//! Best-effort private preparation observations. No scheduling or warm-cache claims.
use crate::ids::{AttemptId, BuilderId, Digest, NodeId};
use crate::platform;
use crate::supervisor::spec::{command_id, spec_digest, SuperviseSpec};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

pub const POLICY: &str = "sum-ttc-admitted-run-wall-v1";
const MAX_FILE: u64 = 65536;
const MAX_NODES: usize = 256;

/// Opaque private builder identity, independent of receipt and artifact keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationBinding {
    pub builder_id: BuilderId,
    pub attempt_id: AttemptId,
    pub owner_generation: BuilderId,
    pub kind: PreparationKind,
    pub key: Digest,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreparationKind {
    WorkspaceSetup,
    ToolInstall,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeObservation {
    policy: String,
    binding: PreparationBinding,
    node_id: NodeId,
    command: Digest,
    spec: Digest,
    boot: String,
    finished_monotonic_ms: u64,
    duration_ms: u64,
}
/// A successful complete build, retained only by the current Setups owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationObservation {
    pub binding: PreparationBinding,
    pub policy: String,
    pub nodes_digest: Digest,
    pub node_count: u32,
    pub boot: String,
    pub finished_monotonic_ms: u64,
    pub duration_ms: u64,
}
fn file_name(node: &NodeId) -> String {
    format!(
        "preparation-{}.json",
        Digest::of_bytes(node.as_str().as_bytes())
            .as_str()
            .replace(':', "-")
    )
}
fn millis(seconds: f64) -> Option<u64> {
    let ms = seconds * 1000.0;
    // u64::MAX rounds up as f64: require strict inequality before casting.
    (ms.is_finite() && ms >= 0.0 && ms < u64::MAX as f64).then(|| ms.ceil() as u64)
}
pub(crate) fn read_private<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .ok()?;
    let meta = file.metadata().ok()?;
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o077 != 0
        || meta.len() > MAX_FILE
    {
        return None;
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_FILE + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_FILE {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}
/// Called only after successful execution and confirmed TTC finish. Queue time
/// is not included; TTC runner spawn overhead is included by the named policy.
pub(crate) fn record_node(spec: &SuperviseSpec, index: usize, seconds: f64) -> Option<()> {
    let binding = spec.preparation.as_ref()?;
    if binding.attempt_id != spec.attempt_id || spec.nodes.len() > MAX_NODES {
        return None;
    }
    let node = spec.nodes.get(index)?;
    let finished = platform::monotonic_ms();
    let duration = millis(seconds)?;
    if finished == u64::MAX || duration > finished {
        return None;
    }
    let observation = NodeObservation {
        policy: POLICY.into(),
        binding: binding.clone(),
        node_id: node.node_id.clone(),
        command: Digest::of_bytes(command_id(&spec.attempt_id, index).as_bytes()),
        spec: Digest::of_bytes(spec_digest(node).as_bytes()),
        boot: platform::boot_id()?,
        finished_monotonic_ms: finished,
        duration_ms: duration,
    };
    let bytes = serde_json::to_vec(&observation).ok()?;
    if bytes.len() as u64 > MAX_FILE {
        return None;
    }
    let mut tmp = crate::platform::owned_temp::file_in(&spec.state_dir).ok()?;
    tmp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))
        .ok()?;
    tmp.write_all(&bytes).ok()?;
    tmp.as_file().sync_all().ok()?;
    tmp.persist_noclobber(spec.state_dir.join(file_name(&node.node_id)))
        .ok()?;
    Some(())
}
/// Read all expected original observations before the builder directory is removed.
/// A missing/invalid/foreign node loses the whole aggregate.
pub(crate) fn aggregate(spec: &SuperviseSpec) -> Option<PreparationObservation> {
    let binding = spec.preparation.as_ref()?;
    if binding.attempt_id != spec.attempt_id
        || spec.nodes.is_empty()
        || spec.nodes.len() > MAX_NODES
    {
        return None;
    }
    let boot = platform::boot_id()?;
    let now = platform::monotonic_ms();
    if now == u64::MAX {
        return None;
    }
    let mut duration = 0u64;
    let mut finished = 0u64;
    let mut identities = Vec::new();
    for (index, node) in spec.nodes.iter().enumerate() {
        let value: NodeObservation = read_private(&spec.state_dir.join(file_name(&node.node_id)))?;
        let command = Digest::of_bytes(command_id(&spec.attempt_id, index).as_bytes());
        let expected = Digest::of_bytes(spec_digest(node).as_bytes());
        if value.policy != POLICY
            || value.binding != *binding
            || value.node_id != node.node_id
            || value.command != command
            || value.spec != expected
            || value.boot != boot
            || value.finished_monotonic_ms > now
            || value.finished_monotonic_ms == u64::MAX
            || value.duration_ms > value.finished_monotonic_ms
        {
            return None;
        }
        duration = duration.checked_add(value.duration_ms)?;
        finished = finished.max(value.finished_monotonic_ms);
        identities.push((node.node_id.clone(), command, expected));
    }
    if duration == u64::MAX {
        return None;
    }
    Some(PreparationObservation {
        binding: binding.clone(),
        policy: POLICY.into(),
        nodes_digest: Digest::of_bytes(&serde_json::to_vec(&identities).ok()?),
        node_count: u32::try_from(spec.nodes.len()).ok()?,
        boot,
        finished_monotonic_ms: finished,
        duration_ms: duration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(dir: &Path) -> SuperviseSpec {
        let attempt = AttemptId::generate();
        serde_json::from_value(serde_json::json!({
            "preparation": PreparationBinding { builder_id: BuilderId::generate(), attempt_id: attempt.clone(),
                owner_generation: BuilderId::generate(), kind: PreparationKind::WorkspaceSetup,
                key: Digest::of_bytes(b"opaque setup binding") },
            "state_dir": dir, "worker_id": "worker-observation", "attempt_id": attempt,
            "traffic_state_dir": dir, "nodes": [
                {"node_id":"setup-0","argv":["/usr/bin/true"],"cwd":dir},
                {"node_id":"setup-1","argv":["/usr/bin/true"],"cwd":dir}], "mask":[]
        })).unwrap()
    }
    #[test]
    fn complete_original_nodes_are_required_and_replay_cannot_refresh_them() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut spec = fixture(dir.path());
        assert!(aggregate(&spec).is_none(), "no recovered or missing proof");
        record_node(&spec, 0, 0.125).unwrap();
        assert!(aggregate(&spec).is_none(), "partial execution is Unknown");
        record_node(&spec, 1, 0.25).unwrap();
        let original = aggregate(&spec).unwrap();
        assert_eq!(original.duration_ms, 375, "sum active executions only");
        assert_eq!(original.node_count, 2);
        assert!(
            record_node(&spec, 0, 10.0).is_none(),
            "immutable no-clobber publication"
        );
        assert_eq!(aggregate(&spec), Some(original));
        spec.preparation.as_mut().unwrap().owner_generation = BuilderId::generate();
        assert!(
            aggregate(&spec).is_none(),
            "old owner must not import observations"
        );
    }
    #[test]
    fn observation_binding_boot_finish_and_command_are_verified() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut spec = fixture(dir.path());
        for index in 0..2 {
            record_node(&spec, index, 0.001).unwrap();
        }
        let node_path = spec.state_dir.join(file_name(&spec.nodes[0].node_id));
        let original: NodeObservation = read_private(&node_path).unwrap();
        for variant in 0..5 {
            let mut value = original.clone();
            match variant {
                0 => value.boot = "previous boot".into(),
                1 => value.finished_monotonic_ms = u64::MAX,
                2 => value.command = Digest::of_bytes(b"other command"),
                3 => value.binding.builder_id = BuilderId::generate(),
                _ => value.policy = "unknown producer policy".into(),
            }
            std::fs::write(&node_path, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(aggregate(&spec).is_none());
        }
        std::fs::write(&node_path, serde_json::to_vec(&original).unwrap()).unwrap();
        spec.nodes[0].argv.push("changed".into());
        assert!(aggregate(&spec).is_none());
    }
    #[test]
    fn durations_are_checked_and_never_saturate() {
        assert_eq!(millis(0.0), Some(0));
        assert_eq!(millis(0.0011), Some(2));
        for bad in [f64::NAN, f64::INFINITY, -0.1, u64::MAX as f64] {
            assert_eq!(millis(bad), None);
        }
    }
    #[test]
    fn private_observations_refuse_links_and_oversized_files() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let path = dir.path().join("observation");
        std::fs::write(&path, b"7").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_private::<u64>(&path), Some(7));
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert_eq!(read_private::<u64>(&link), None);
        std::fs::hard_link(&path, dir.path().join("hard")).unwrap();
        assert_eq!(read_private::<u64>(&path), None);
        std::fs::remove_file(dir.path().join("hard")).unwrap();
        std::fs::write(&path, vec![b'0'; MAX_FILE as usize + 1]).unwrap();
        assert_eq!(read_private::<u64>(&path), None);
    }
}
