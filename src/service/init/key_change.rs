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
    if !journal_path.try_exists()? || read_journal(&journal_path)?.spec.cluster_key_path != target {
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
    ensure!(
        j.spec.managed_service.as_ref() == Some(&journal_path)
            && j.spec.cluster_key_path == target
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
