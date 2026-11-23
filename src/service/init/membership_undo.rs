//! Durable rollback image for the files spanning a membership DB transaction.
use super::*;
use crate::worker::control;

#[derive(Serialize, Deserialize)]
struct Undo {
    journal: Journal,
    key: Vec<u8>,
    input: Vec<u8>,
    control: control::Control,
    membership: Option<crate::storage::MembershipImage>,
    restart: bool,
}
fn path(paths: &Paths) -> PathBuf {
    paths.config.join("membership-undo.json")
}
fn replace(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut temp = crate::platform::owned_temp::file_in(path.parent().context("file parent")?)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    File::open(path.parent().context("file parent")?)?.sync_all()?;
    Ok(())
}
pub(super) fn prepare(
    paths: &Paths,
    previous: control::Control,
    restart: bool,
    snapshot_worker: bool,
) -> anyhow::Result<()> {
    let undo = Undo {
        journal: read_journal(&paths.journal)?,
        key: fs::read(&paths.key)?,
        input: fs::read(&paths.input)?,
        control: previous,
        membership: if snapshot_worker {
            Some(crate::storage::WorkerStore::open_read_only(&paths.worker)?.membership_image()?)
        } else {
            None
        },
        restart,
    };
    replace(&path(paths), &serde_json::to_vec(&undo)?)
}
pub(super) fn finish(paths: &Paths) -> anyhow::Result<()> {
    match fs::remove_file(path(paths)) {
        Ok(()) => File::open(&paths.config)?.sync_all().map_err(Into::into),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
/// The init lock is held by the caller, including during crash recovery.
pub(super) fn recover<L: Launcher>(paths: &Paths, launcher: &mut L) -> anyhow::Result<()> {
    if !path(paths).try_exists()? {
        return Ok(());
    }
    let mut bytes = Vec::new();
    resident::secure_file(&path(paths), false)?
        .take(LIMIT * 6 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= LIMIT * 6,
        "membership rollback image too large"
    );
    let mut undo: Undo = serde_json::from_slice(&bytes)?;
    let mut paths = paths.clone();
    if undo.journal.spec.cluster_key_path == paths.config.join("worker-cluster.key") {
        paths.key = undo.journal.spec.cluster_key_path.clone();
    }
    validate_journal(
        &undo.journal,
        &paths,
        rustix::process::geteuid().as_raw(),
        &undo.journal.binary_hash,
    )?;
    for proof in &undo.journal.files {
        if proof.path != paths.key && proof.path != paths.input {
            proof.verify()?;
        }
    }
    let plan = plan(&paths, &undo.journal)?;
    let receipt = undo
        .journal
        .plist
        .clone()
        .context("service receipt missing")?;
    let mut files = DirectoryFiles::open(&paths.services)?;
    let _lease = files.lifecycle_lock()?;
    files.verify(&receipt)?;
    match launcher.loaded_state(&plan, &receipt) {
        LoadedState::Match => {
            launcher.verify_loaded(&plan, &receipt)?;
            launcher.launchctl(
                &plan
                    .stop_arguments(receipt.path())
                    .map_err(anyhow::Error::msg)?,
            )?;
        }
        LoadedState::Unknown => {
            anyhow::bail!("loaded service identity unknown during membership recovery")
        }
        LoadedState::Absent => {}
    }
    ensure!(
        launcher.loaded_state(&plan, &receipt) == LoadedState::Absent,
        "service did not stop during membership recovery"
    );
    let result = (|| -> anyhow::Result<()> {
        let _owner = crate::storage::ProcessLock::acquire(&paths.state, "coordinator.lock")?;
        let _agent = crate::storage::ProcessLock::acquire(&paths.worker, "agent.lock")?;
        undo.journal
            .database
            .as_ref()
            .context("database receipt missing")?
            .verify_inode()?;
        crate::storage::CoordinatorStore::move_group(
            &paths.state,
            undo.journal.cluster.as_ref().context("group missing")?,
        )?;
        if let Some(membership) = &undo.membership {
            crate::storage::WorkerStore::open(&paths.worker, &undo.journal.spec.worker_id)?
                .restore_membership(membership)?;
        }
        for (path, bytes) in [(&paths.key, &undo.key), (&paths.input, &undo.input)] {
            replace(path, bytes)?;
            *undo
                .journal
                .files
                .iter_mut()
                .find(|p| &p.path == path)
                .context("receipt missing")? = Proof::read(path)?;
        }
        replace(&paths.journal, &serde_json::to_vec(&undo.journal)?)?;
        Ok(())
    })();
    // Attempt both restoration and restart, even when restoring files fails.
    let control = control::write(&paths.worker, undo.control);
    let start = if undo.restart {
        launcher.launchctl(
            &plan
                .start_arguments(receipt.path())
                .map_err(anyhow::Error::msg)?,
        )
    } else {
        Ok(())
    };
    result
        .and(control.map_err(Into::into))
        .and(start.map_err(Into::into))?;
    finish(&paths)
}
