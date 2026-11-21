//! Local send-key handoff. The marker contains only a fresh ULID; key bytes
//! live in a private key file, never a command argument or journal.
use crate::{ids::WorkerBootId, storage::WorkerStore};
use std::{io::Write, path::Path};

pub(crate) const PENDING: &str = "send-key.pending";
const KEY: &str = "send-key.key";
const INSTANCE: &str = "worker-instance";

pub(crate) fn write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap();
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| e.error)?;
    std::fs::File::open(parent)?.sync_all()
}
pub(crate) fn read(state: &Path, name: &str) -> std::io::Result<Option<String>> {
    use std::io::Read;
    match crate::service::resident::secure_file(&state.join(name), false) {
        Ok(file) => {
            let mut text = String::new();
            file.take(128).read_to_string(&mut text)?;
            Ok(Some(text))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
pub(crate) fn identity(state: &Path, boot: &WorkerBootId) -> anyhow::Result<WorkerBootId> {
    match read(state, INSTANCE)? {
        Some(instance) => {
            ulid::Ulid::from_string(&instance)?;
            Ok(WorkerBootId::parse(&format!("{boot}.{instance}"))?)
        }
        None => Ok(boot.clone()),
    }
}
/// The identity a pending send-key will publish, so the agent can advertise
/// it before `finish` removes the marker (pending still fences new work).
pub(crate) fn pending_identity(
    state: &Path,
    boot: &WorkerBootId,
) -> anyhow::Result<Option<WorkerBootId>> {
    match read(state, PENDING)? {
        Some(instance) => {
            ulid::Ulid::from_string(&instance)?;
            Ok(Some(WorkerBootId::parse(&format!("{boot}.{instance}"))?))
        }
        None => Ok(None),
    }
}
pub(crate) fn pending(state: &Path) -> bool {
    // An unreadable request also fences new work.
    !matches!(read(state, PENDING), Ok(None))
}
fn cleanup_stray_key(state: &Path) -> std::io::Result<()> {
    if read(state, PENDING)?.is_none() {
        match std::fs::remove_file(state.join(KEY)) {
            Ok(()) => std::fs::File::open(state)?.sync_all()?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
pub(crate) fn stage(state: &Path, key: &crate::secrets::key::ClusterKey) -> anyhow::Result<String> {
    anyhow::ensure!(
        !pending(state),
        "send-key already pending; wait for the agent to finish it"
    );
    cleanup_stray_key(state)?;
    let instance = ulid::Ulid::new().to_string();
    let text: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    write(&state.join(KEY), format!("{text}\n").as_bytes())?;
    write(&state.join(PENDING), instance.as_bytes())?;
    Ok(instance)
}
pub(crate) fn finish(state: &Path, key_path: &Path, store: &WorkerStore) -> anyhow::Result<()> {
    let Some(instance) = read(state, PENDING)? else {
        return Ok(());
    };
    ulid::Ulid::from_string(&instance)?;
    anyhow::ensure!(
        store.list_nonterminal()?.is_empty(),
        "old identity still holds live attempts"
    );
    let key = match crate::secrets::key::load(&state.join(KEY)) {
        Ok(key) => key,
        Err(error) => {
            // A crash after removing the staged key but before the marker:
            // only the already-published instance can complete this prefix.
            anyhow::ensure!(
                read(state, INSTANCE)?.as_ref() == Some(&instance),
                "{error}"
            );
            crate::secrets::key::load(key_path)?
        }
    };
    let text: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    write(key_path, format!("{text}\n").as_bytes())?;
    match std::fs::remove_file(crate::group::next_key_path(key_path)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    // Receiving-only services bind their enrollment to the key file digest.
    // Keep that existing binding usable on restart; do not create a new one.
    let binding = state.join("worker-binding.json");
    match crate::service::resident::secure_file(&binding, false) {
        Ok(file) => {
            use sha2::Digest;
            use std::io::Read;
            let mut recorded: (u32, crate::ids::ClusterId, crate::ids::WorkerId, String) =
                serde_json::from_reader(file.take(4096))?;
            anyhow::ensure!(
                recorded.0 == 1 && &recorded.2 == store.worker_id(),
                "foreign worker enrollment binding"
            );
            recorded.3 = format!("{:x}", sha2::Sha256::digest(format!("{text}\n").as_bytes()));
            write(&binding, &serde_json::to_vec(&recorded)?)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    store.observe_group_key(&crate::group::fingerprint(&key))?;
    store.drop_next_group_key()?;
    write(&state.join(INSTANCE), instance.as_bytes())?;
    write(&key_path.with_extension("connections"), instance.as_bytes())?;
    for name in [KEY, "group-rotation.json"] {
        match std::fs::remove_file(state.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    std::fs::remove_file(state.join(PENDING))?;
    std::fs::File::open(state)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stray_key_is_removed_but_pending_key_is_preserved() {
        let dir = crate::test_tmp::tempdir().unwrap();
        write(&dir.path().join(KEY), b"stray").unwrap();
        cleanup_stray_key(dir.path()).unwrap();
        assert!(!dir.path().join(KEY).exists());
        write(&dir.path().join(PENDING), b"pending").unwrap();
        write(&dir.path().join(KEY), b"retained").unwrap();
        cleanup_stray_key(dir.path()).unwrap();
        assert_eq!(read(dir.path(), KEY).unwrap().as_deref(), Some("retained"));
    }
}
