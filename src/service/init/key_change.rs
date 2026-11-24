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
    let _lock = crate::platform::flock::HeldLock::try_lock(lock)
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
    clear_leftover(spec, false)
}

/// At a start: a leftover `switching_key` that `read_journal` resolves in
/// memory is also cleared on disk, so the journal stays readable by an older
/// binary (`belt upgrade` rollback). A change in progress holds `init.lock`
/// and will settle the journal itself, so a busy lock is not an error here.
pub(crate) fn settle_key_change(spec: &Spec) -> anyhow::Result<()> {
    clear_leftover(spec, true)
}

fn clear_leftover(spec: &Spec, skip_if_busy: bool) -> anyhow::Result<()> {
    let Some(path) = &spec.managed_service else {
        return Ok(());
    };
    if read_raw_journal(path)?.switching_key.is_none() {
        return Ok(());
    }
    let lock = resident::secure_file(&path.with_file_name("init.lock"), true)?;
    let _lock = match crate::platform::flock::HeldLock::try_lock(lock) {
        Ok(held) => held,
        Err(fs::TryLockError::WouldBlock) if skip_if_busy => return Ok(()),
        Err(fs::TryLockError::WouldBlock) => {
            anyhow::bail!("managed service change in progress")
        }
        Err(fs::TryLockError::Error(e)) => return Err(e.into()),
    };
    let (mut journal, _) = managed_journal(spec)?;
    if skip_if_busy {
        settle_held(path, &mut journal)
    } else {
        ensure!(
            journal.switching_key.is_none(),
            "managed key switch is still pending"
        );
        save_over(path, &journal)
    }
}

/// For a caller that already holds `init.lock` (`belt upgrade`, `belt init`):
/// `resolved` is what `read_journal` returned for `path`. A leftover
/// `switching_key` on disk is dropped from it and the journal is rewritten, so
/// nothing copied or retargeted from it carries the field. The rename did not
/// happen when a leftover is still in `resolved`; see `clear_leftover`.
pub(crate) fn settle_held(path: &Path, resolved: &mut Journal) -> anyhow::Result<()> {
    if read_raw_journal(path)?.switching_key.is_none() {
        return Ok(());
    }
    resolved.switching_key = None;
    save_over(path, resolved)
}

fn save_over(path: &Path, journal: &Journal) -> anyhow::Result<()> {
    JournalFile {
        path: path.to_owned(),
        proof: Some(Proof::read(path)?),
    }
    .save(journal)
}
