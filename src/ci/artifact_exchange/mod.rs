//! One bounded, replayable artifact message at a time between the supervisor
//! and the agent. The exchange directory is private to the accepted attempt.
use crate::ids::Digest;
use crate::wire::{
    ArtifactAction, ArtifactAnswer, ArtifactExchangeResult, ArtifactIntent, Bytes,
    WorkflowArtifactCommand,
};
use anyhow::{bail, ensure, Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub(crate) mod worker;
pub(crate) const CHUNK: usize = 128 * 1024;
pub(crate) const MAX_MESSAGE: usize = 1024 * 1024;

fn hex(id: &Digest) -> &str {
    id.as_str()
        .strip_prefix("sha256:")
        .expect("validated digest")
}
fn response_path(root: &Path, key: &Digest) -> PathBuf {
    root.join(format!("{}.answer", hex(key)))
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    ensure!(
        file.metadata()?.is_file(),
        "artifact message is not a regular file"
    );
    let mut bytes = Vec::new();
    file.take(MAX_MESSAGE as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "artifact message is too large");
    Ok(Some(serde_json::from_slice(&bytes)?))
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "artifact message is too large");
    let dir = path.parent().context("artifact message parent")?;
    let mut tmp = crate::platform::owned_temp::file_in(dir)?;
    tmp.write_all(&bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path)?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

pub(crate) fn respond(root: &Path, cmd: WorkflowArtifactCommand) -> Result<ArtifactExchangeResult> {
    let action: Option<ArtifactAction> = read_json(&root.join("pending.json"))?;
    match cmd {
        WorkflowArtifactCommand::Poll => Ok(ArtifactExchangeResult::Pending { action }),
        WorkflowArtifactCommand::Answer { key, answer } => {
            let dest = response_path(root, &key);
            if let Some(prior) = read_json::<ArtifactAnswer>(&dest)? {
                ensure!(prior == answer, "conflicting action replay");
            } else {
                ensure!(
                    action.as_ref().is_some_and(|a| a.key == key),
                    "artifact action no longer pending"
                );
                atomic_json(&dest, &answer)?;
            }
            Ok(ArtifactExchangeResult::Accepted)
        }
        WorkflowArtifactCommand::Read {
            key,
            digest,
            offset,
        } => {
            let action = action
                .filter(|a| a.key == key)
                .context("stale artifact read")?;
            let ArtifactIntent::Upload { receipt, .. } = action.intent else {
                bail!("not an upload")
            };
            let receipt: local_ci::artifact_io::Receipt = serde_json::from_value(receipt)?;
            let size = receipt
                .files
                .iter()
                .find(|f| f.sha256 == hex(&digest))
                .context("undeclared upload digest")?
                .size;
            ensure!(offset < size, "invalid chunk offset");
            let mut file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(root.join(hex(&key)).join(hex(&digest)))?;
            let meta = file.metadata()?;
            ensure!(
                meta.is_file() && meta.len() == size,
                "staged upload changed"
            );
            file.seek(SeekFrom::Start(offset))?;
            let mut data = vec![0; (size - offset).min(CHUNK as u64) as usize];
            file.read_exact(&mut data)?;
            Ok(ArtifactExchangeResult::Chunk { data: Bytes(data) })
        }
    }
}
