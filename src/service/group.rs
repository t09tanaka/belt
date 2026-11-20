//! Read-only group reporting and bounded, secret-free connection diagnostics.
pub mod join;

use super::{bootstrap::Spec, resident, workers};
use crate::{
    error::{ApiError, ErrorCode, GroupRefusal},
    ids::WorkerId,
    storage::WorkerStore,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::{Path, PathBuf},
};

/// Only fixed reason codes reach diagnostics, never a peer's message or fingerprint.
pub(crate) fn refusal_code(reason: &GroupRefusal) -> &'static str {
    match reason {
        GroupRefusal::GroupMismatch => "group_mismatch",
        GroupRefusal::GroupKeyMismatch { .. } => "group_key_mismatch",
        GroupRefusal::MemberIdentityMismatch => "member_identity_mismatch",
        GroupRefusal::IdentityRetired => "identity_retired",
    }
}

fn refusal_path(state: &Path, member: &WorkerId) -> PathBuf {
    // The digest keeps even a future expansion of member names out of paths.
    state.join(format!(
        "group-refusal-{}.txt",
        crate::ids::Digest::of_bytes(member.as_str().as_bytes())
    ))
}

pub(crate) fn record_refusal(
    state: &Path,
    member: &WorkerId,
    reason: Option<&GroupRefusal>,
) -> std::io::Result<()> {
    resident::check_private_dir(state)?;
    let path = refusal_path(state, member);
    if let Some(reason) = reason {
        let mut staged = tempfile::NamedTempFile::new_in(state)?;
        staged.write_all(refusal_code(reason).as_bytes())?;
        staged.as_file().sync_all()?;
        staged.persist(path).map_err(|e| e.error)?;
    } else {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) fn observed_refusal(state: &Path, member: &WorkerId) -> std::io::Result<Option<String>> {
    let file = match resident::secure_file(&refusal_path(state, member), false) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut reason = String::new();
    file.take(64).read_to_string(&mut reason)?;
    if ![
        "group_mismatch",
        "group_key_mismatch",
        "member_identity_mismatch",
        "identity_retired",
    ]
    .contains(&reason.as_str())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid group refusal record",
        ));
    }
    Ok(Some(reason))
}

fn internal(e: impl std::fmt::Display) -> ApiError {
    ApiError::new(ErrorCode::Internal, e.to_string())
}

pub fn report(env: &BTreeMap<String, String>, spec: &Spec) -> Result<Value, ApiError> {
    let store = WorkerStore::open_read_only(&spec.worker_state_dir).map_err(internal)?;
    let (current, next) = store.group_keys().map_err(internal)?;
    let generation = |g: Option<crate::storage::KeyGeneration>| {
        g.map(|g| json!({"number":g.generation,"fingerprint":g.fingerprint}))
    };
    let entries = workers::registrations(env)?;
    let names = entries
        .iter()
        .map(|e| WorkerId::parse(&e.id).map_err(internal))
        .collect::<Result<Vec<_>, _>>()?;
    let observations = workers::observed_connections(spec, names.clone())?;
    let mut members = Vec::new();
    for (entry, name) in entries.into_iter().zip(names) {
        let observed = observations.as_ref().and_then(|m| m.get(&name));
        let refusal = observed_refusal(&spec.state_dir, &name).map_err(internal)?;
        members.push(json!({"name":entry.id,"ssh":entry.ssh,"connection":observed.map(|(connected,_)| if *connected {"connected"} else {"disconnected"}).unwrap_or("unknown"),"last_group_refusal":refusal}));
    }
    Ok(
        json!({"group_id":spec.expected_cluster,"name":spec.worker_id,"current":generation(current),"next":generation(next),"members":members}),
    )
}

pub fn command(json_output: bool) -> i32 {
    let env = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let result = load_spec(&env).and_then(|spec| report(&env, &spec));
    let text = result
        .as_ref()
        .map(|value| serde_json::to_string_pretty(value).expect("JSON value"))
        .unwrap_or_default();
    workers::emit(json_output, result, || text)
}

/// A receiving-only member can inspect its group without acquiring a coordinator identity.
pub(super) fn load_spec(env: &BTreeMap<String, String>) -> Result<Spec, ApiError> {
    match super::context::runtime_spec(env) {
        Ok(spec) => Ok(spec),
        Err(error) if error.code == ErrorCode::NotFound => {
            let paths = super::worker_service::Paths::resolve(env).map_err(internal)?;
            let worker = super::worker::read_input(&paths.input).map_err(internal)?;
            Ok(Spec {
                state_dir: worker.state_dir.clone(),
                worker_state_dir: worker.state_dir,
                worker_id: worker.worker_id,
                cluster_key_path: worker.cluster_key_path,
                executable: worker.executable,
                env: worker.env,
                repo_root: None,
                expected_cluster: Some(worker.expected_cluster),
                managed_service: None,
            })
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn report_reads_only_configured_members_and_key_metadata() {
        let root = crate::test_tmp::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let env = BTreeMap::from([("HOME".into(), root.path().to_str().unwrap().into())]);
        let config = crate::config::paths::user_config_path(&env).unwrap();
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::write(
            &config,
            "schema_version = 1\n[[workers]]\nid = 'peer'\nssh = 'peer-host'\n",
        )
        .unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        let worker = WorkerId::parse("local").unwrap();
        let store = WorkerStore::open(root.path(), &worker).unwrap();
        store.observe_group_key("current-fingerprint").unwrap();
        store
            .set_next_group_key(&crate::storage::KeyGeneration {
                generation: 2,
                fingerprint: "next-fingerprint".into(),
            })
            .unwrap();
        let cluster = crate::ids::ClusterId::generate();
        let spec = Spec {
            state_dir: root.path().into(),
            worker_state_dir: root.path().into(),
            worker_id: worker,
            cluster_key_path: root.path().join("deliberately-absent-key"),
            executable: root.path().join("belt"),
            env: env.clone(),
            repo_root: None,
            expected_cluster: Some(cluster.clone()),
            managed_service: None,
        };
        let peer = WorkerId::parse("peer").unwrap();
        record_refusal(
            root.path(),
            &peer,
            Some(&GroupRefusal::MemberIdentityMismatch),
        )
        .unwrap();
        record_refusal(
            root.path(),
            &WorkerId::parse("unlisted").unwrap(),
            Some(&GroupRefusal::GroupMismatch),
        )
        .unwrap();
        let result = report(&env, &spec).unwrap();
        assert_eq!(result["group_id"], cluster.to_string());
        assert_eq!(
            result["current"],
            json!({"number":1,"fingerprint":"current-fingerprint"})
        );
        assert_eq!(
            result["next"],
            json!({"number":2,"fingerprint":"next-fingerprint"})
        );
        assert_eq!(result["members"].as_array().unwrap().len(), 1);
        assert_eq!(result["members"][0]["name"], "peer");
        assert_eq!(result["members"][0]["connection"], "unknown");
        assert_eq!(
            result["members"][0]["last_group_refusal"],
            "member_identity_mismatch"
        );
        assert!(!spec.cluster_key_path.exists());
    }

    #[test]
    fn malformed_or_symlinked_diagnostics_are_not_reported_as_peer_messages() {
        let root = crate::test_tmp::tempdir().unwrap();
        let member = WorkerId::parse("peer").unwrap();
        let path = refusal_path(root.path(), &member);
        std::fs::write(&path, "untrusted text").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            observed_refusal(root.path(), &member).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(root.path().join("absent"), &path).unwrap();
        assert!(observed_refusal(root.path(), &member).is_err());
    }

    #[test]
    fn connection_diagnostics_never_store_peer_text_and_clear_only_after_success() {
        let root = crate::test_tmp::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let member = WorkerId::parse("peer").unwrap();
        let other = WorkerId::parse("other").unwrap();
        assert_eq!(observed_refusal(root.path(), &member).unwrap(), None);
        for (reason, expected) in [
            (GroupRefusal::GroupMismatch, "group_mismatch"),
            (
                GroupRefusal::GroupKeyMismatch {
                    fingerprint: "secret-peer-input".into(),
                },
                "group_key_mismatch",
            ),
            (
                GroupRefusal::MemberIdentityMismatch,
                "member_identity_mismatch",
            ),
            (GroupRefusal::IdentityRetired, "identity_retired"),
        ] {
            record_refusal(root.path(), &member, Some(&reason)).unwrap();
            assert_eq!(
                observed_refusal(root.path(), &member).unwrap().as_deref(),
                Some(expected)
            );
            assert_eq!(
                std::fs::read_to_string(refusal_path(root.path(), &member)).unwrap(),
                expected
            );
            assert_eq!(observed_refusal(root.path(), &other).unwrap(), None);
        }
        record_refusal(root.path(), &member, None).unwrap();
        record_refusal(root.path(), &member, None).unwrap();
        assert_eq!(observed_refusal(root.path(), &member).unwrap(), None);
    }
}
