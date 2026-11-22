//! Publish a new group key and preserve init's receipt across a crash.
use super::*;

pub(crate) fn install_key(staged: &Path, target: &Path) -> anyhow::Result<()> {
    let parent = target.parent().context("key parent missing")?;
    let journal_path = parent.join("init.json");
    let publish = || -> anyhow::Result<()> {
        fs::rename(staged, target)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    };
    if !journal_path.try_exists()? {
        let input = parent.join("coordinator.json");
        if input.try_exists()? {
            let spec = bootstrap::read_input(&input)?;
            ensure!(
                spec.managed_service.is_none() || spec.cluster_key_path != target,
                "managed key journal missing"
            );
        }
        return publish();
    }
    let lock = resident::secure_file(&parent.join("init.lock"), true)?;
    lock.try_lock()
        .context("managed service change in progress")?;
    let mut jf = JournalFile {
        path: journal_path.clone(),
        proof: Some(Proof::read(&journal_path)?),
    };
    let mut j = read_journal(&journal_path)?;
    if j.spec.cluster_key_path != target {
        ensure!(
            !j.files.iter().any(|proof| proof.path == target),
            "managed key receipt disagrees with input"
        );
        return publish();
    }
    ensure!(
        j.spec.managed_service.as_ref() == Some(&journal_path)
            && j.pending.is_none()
            && j.plist.is_some(),
        "managed key binding changed"
    );
    let index = j
        .files
        .iter()
        .position(|p| p.path == target)
        .context("managed key receipt missing")?;
    j.files[index].verify()?;
    crate::secrets::key::load(staged)?;
    let mut next = Proof::read(staged)?;
    next.path = target.to_owned();
    // Readers accept exactly one of the two recorded inodes during the rename.
    // No key bytes enter the init journal.
    j.switching_key = Some(next.clone());
    jf.save(&j)?;
    publish()?;
    next.verify()?;
    j.files[index] = next;
    j.switching_key = None;
    jf.save(&j)
}

/// A retry after the key rename also finishes the receipt on disk, so an older
/// binary can again read the ordinary init journal after rotation completes.
pub(crate) fn finish_key_change(spec: &Spec) -> anyhow::Result<()> {
    let Some(path) = &spec.managed_service else {
        return Ok(());
    };
    if read_raw_journal(path)?.switching_key.is_none() {
        return Ok(());
    }
    let lock = resident::secure_file(&path.with_file_name("init.lock"), true)?;
    lock.try_lock()
        .context("managed service change in progress")?;
    let (journal, _) = managed_journal(spec)?;
    ensure!(
        journal.switching_key.is_none(),
        "managed key switch is still pending"
    );
    JournalFile {
        path: path.clone(),
        proof: Some(Proof::read(path)?),
    }
    .save(&journal)
}
