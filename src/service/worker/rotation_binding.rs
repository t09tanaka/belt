//! Write-ahead authorization for the worker binding when the group key switches.
use super::*;
use crate::worker::rekey::write;
use sha2::{Digest, Sha256};

const INTENT: &str = "worker-binding-rotation.json";

#[derive(Serialize, Deserialize)]
struct BindingChange {
    from: String,
    to: String,
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn expected(
    path: &Path,
    cluster: &ClusterId,
    worker: &WorkerId,
) -> anyhow::Result<Vec<u8>> {
    let mut key = Vec::new();
    resident::secure_file(path, false)?
        .take(4097)
        .read_to_end(&mut key)?;
    Ok(serde_json::to_vec(&(1, cluster, worker, digest(&key)))?)
}

pub(crate) fn prepare(spec: &crate::service::bootstrap::Spec, next: &Path) -> anyhow::Result<()> {
    if spec.state_dir != spec.worker_state_dir {
        return Ok(());
    }
    let dir = &spec.worker_state_dir;
    let binding = dir.join("worker-binding.json");
    let cluster = spec
        .expected_cluster
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("worker group missing"))?;
    let current = expected(&spec.cluster_key_path, cluster, &spec.worker_id)?;
    let mut existing = Vec::new();
    resident::secure_file(&binding, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut existing)?;
    reconcile(dir, &existing, &current)?;
    let next = expected(next, cluster, &spec.worker_id)?;
    let change = BindingChange {
        from: digest(&current),
        to: digest(&next),
    };
    write(&dir.join(INTENT), &serde_json::to_vec(&change)?)?;
    Ok(())
}

pub(super) fn reconcile(dir: &Path, existing: &[u8], expected: &[u8]) -> anyhow::Result<()> {
    if existing == expected {
        return Ok(());
    }
    let intent = dir.join(INTENT);
    let file = resident::secure_file(&intent, false)
        .map_err(|_| anyhow::anyhow!("foreign worker enrollment binding"))?;
    let change: BindingChange = serde_json::from_reader(file.take(4096))?;
    anyhow::ensure!(
        change.from == digest(existing) && change.to == digest(expected),
        "foreign worker enrollment binding"
    );
    write(&dir.join("worker-binding.json"), expected)?;
    // Keeping a committed intent is harmless. Both sides, including the
    // previous binding, must match; the next rotation replaces it atomically.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::bootstrap::Spec;

    #[test]
    fn restart_after_rotation_accepts_only_the_authorized_key_and_identity() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let cluster = ClusterId::generate();
        let worker = WorkerId::parse("w1").unwrap();
        let key = dir.path().join("cluster.key");
        let next = crate::group::next_key_path(&key);
        crate::secrets::key::init(&key).unwrap();
        crate::secrets::key::init(&next).unwrap();
        let spec = WorkerSpec {
            schema: 1,
            expected_cluster: cluster.clone(),
            worker_id: worker.clone(),
            state_dir: dir.path().to_owned(),
            cluster_key_path: key.clone(),
            executable: PathBuf::from("/belt"),
            env: BTreeMap::new(),
            config: String::new(),
        };
        bind(&spec).unwrap();
        prepare(
            &Spec {
                state_dir: spec.state_dir.clone(),
                worker_state_dir: spec.state_dir.clone(),
                worker_id: worker,
                cluster_key_path: key.clone(),
                executable: spec.executable.clone(),
                env: spec.env.clone(),
                repo_root: None,
                expected_cluster: Some(cluster),
                managed_service: None,
            },
            &next,
        )
        .unwrap();
        bind(&spec).unwrap();
        std::fs::rename(next, &key).unwrap();
        let mut foreign = spec.clone();
        foreign.worker_id = WorkerId::parse("w2").unwrap();
        assert!(bind(&foreign).is_err());
        bind(&spec).unwrap();
        bind(&spec).unwrap();
        std::fs::remove_file(&key).unwrap();
        crate::secrets::key::init(&key).unwrap();
        assert!(bind(&spec).is_err());
    }
}
