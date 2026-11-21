//! Private, secret-free contact record. An open session left by a crashed
//! resident is dated conservatively when forget first observes it disconnected.
use crate::ids::WorkerId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
#[derive(Serialize, Deserialize)]
pub(crate) struct Contact {
    pub fingerprint: String,
    pub at_ms: u64,
    pub open: bool,
}
fn path(state: &Path, worker: &WorkerId) -> PathBuf {
    state.join(format!(
        "member-contact-{}.json",
        crate::ids::Digest::of_bytes(worker.as_str().as_bytes())
    ))
}
pub(crate) fn read(state: &Path, worker: &WorkerId) -> std::io::Result<Option<Contact>> {
    use std::io::Read;
    match crate::service::resident::secure_file(&path(state, worker), false) {
        Ok(f) => Ok(Some(serde_json::from_reader(f.take(4096))?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
pub(crate) fn write(state: &Path, worker: &WorkerId, contact: &Contact) -> std::io::Result<()> {
    crate::worker::rekey::write(&path(state, worker), &serde_json::to_vec(contact)?)
}
