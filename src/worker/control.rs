//! Durable operator intent for one worker: stop taking new placement, and
//! optionally cancel what is running. A private file in the worker state
//! directory, written by the service helper and read by the agent on every
//! `inspect` and tick. A file that exists but cannot be read safely is
//! treated as a drain request, never as "not draining".
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const FILE: &str = "control.json";
const LIMIT: u64 = 4096;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    /// No new attempt is placed here; accepted attempts continue.
    pub drain: bool,
    /// Every live attempt is cancelled on each agent tick. Only meaningful
    /// together with `drain`, which `request_cancel` guarantees.
    pub cancel_running: bool,
}

impl Control {
    pub fn draining() -> Self {
        Control {
            drain: true,
            cancel_running: false,
        }
    }
    pub fn cancelling() -> Self {
        Control {
            drain: true,
            cancel_running: true,
        }
    }
    /// What the agent reports and acts on. A broken marker drains and does not cancel.
    pub fn effective(state_dir: &Path) -> Control {
        read(state_dir).unwrap_or_else(|_| Control::draining())
    }
}

pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(FILE)
}

/// Missing means no request. Anything unsafe or unparsable is an error.
pub fn read(state_dir: &Path) -> std::io::Result<Control> {
    let path = path(state_dir);
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Control::default()),
        Err(e) => return Err(e),
    };
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o077 != 0
    {
        return Err(invalid("unsafe worker control file"));
    }
    let mut bytes = Vec::new();
    (&mut file).take(LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LIMIT {
        return Err(invalid("worker control file too large"));
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid("worker control file unparsable"))
}

/// Atomic replace with the directory synced, so an acknowledged request survives a crash.
pub fn write(state_dir: &Path, control: Control) -> std::io::Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(state_dir)?;
    temporary
        .as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    temporary.write_all(&serde_json::to_vec(&control)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path(state_dir)).map_err(|e| e.error)?;
    std::fs::File::open(state_dir)?.sync_all()
}

/// The upgrade hold (`belt upgrade`): like a drain, no new attempt is
/// placed here, but it is a state of its own. `resume` writes only the
/// control file, so it never lifts the hold; only the upgrade's commit or
/// rollback does. A hold that cannot be told apart from none is a hold.
const HOLD: &str = "upgrade-hold.json";

pub fn held(state_dir: &Path) -> bool {
    present(&state_dir.join(HOLD))
}

/// Place the hold, durably. Placing it again changes nothing.
pub fn hold(state_dir: &Path) -> std::io::Result<()> {
    place_marker(state_dir, HOLD)
}

fn place_marker(state_dir: &Path, name: &str) -> std::io::Result<()> {
    place_file(state_dir, name, b"{}")
}

fn place_file(state_dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(state_dir)?;
    temporary
        .as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(state_dir.join(name))
        .map_err(|e| e.error)?;
    std::fs::File::open(state_dir)?.sync_all()
}

/// Lift the hold. Lifting a hold that is not there changes nothing.
pub fn lift_hold(state_dir: &Path) -> std::io::Result<()> {
    remove_marker(&state_dir.join(HOLD))
}

/// The trial of a new version (`belt upgrade`), from just before the switch
/// to the commit or the rollback. While it is there the worker answers every
/// state-changing request with a reasonless `OVERLOADED` and saves nothing,
/// and the coordinator only adopts and renews. A marker that cannot be told
/// apart from none is a trial.
const TRIAL: &str = "upgrade-trial.json";

pub fn trial_path(state_dir: &Path) -> PathBuf {
    state_dir.join(TRIAL)
}

pub fn present(marker: &Path) -> bool {
    match std::fs::symlink_metadata(marker) {
        Ok(_) => true,
        Err(e) => e.kind() != std::io::ErrorKind::NotFound,
    }
}

pub fn in_trial(state_dir: &Path) -> bool {
    present(&trial_path(state_dir))
}

/// A trial begins with no proof of a renewal: one left by an earlier trial
/// (a version that does not know the proof rolled it back) proves nothing.
pub fn begin_trial(state_dir: &Path) -> std::io::Result<()> {
    remove_marker(&state_dir.join(RENEWED))?;
    place_marker(state_dir, TRIAL)
}

pub fn end_trial(state_dir: &Path) -> std::io::Result<()> {
    remove_marker(&trial_path(state_dir))?;
    remove_marker(&state_dir.join(RENEWED))
}

/// What the version on trial did with the attempts it adopted (§24.8),
/// written by its coordinator next to the trial marker: those whose lease a
/// worker extended, and those a worker answered for that the trial does not
/// renew (ended, retired, cancelling, never arrived). The upgrade reads it
/// before the commit. Nothing else depends on it.
const RENEWED: &str = "upgrade-trial-renewed.json";
/// Room for the ids of every attempt a member is likely to adopt.
const PROOF_LIMIT: u64 = 1 << 20;
const PROOF_ENTRIES: usize = 4096;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialProof {
    pub renewed: Vec<String>,
    pub unrenewed: Vec<String>,
}

/// Note, next to the trial `marker`, that a worker extended the lease of
/// `attempt`. A note that is there is not written again.
pub fn note_renewal(marker: &Path, attempt: &str) -> std::io::Result<()> {
    note(marker, attempt, |p| &mut p.renewed)
}

/// Note, next to the trial `marker`, that a worker answered for `attempt`
/// and the trial does not renew it.
pub fn note_unrenewed(marker: &Path, attempt: &str) -> std::io::Result<()> {
    note(marker, attempt, |p| &mut p.unrenewed)
}

fn note(
    marker: &Path,
    attempt: &str,
    list: impl Fn(&mut TrialProof) -> &mut Vec<String>,
) -> std::io::Result<()> {
    let Some(dir) = marker.parent() else {
        return Err(invalid("trial marker without a directory"));
    };
    let mut proof = trial_proof(dir)?;
    let noted = list(&mut proof);
    if noted.iter().any(|a| a == attempt) || noted.len() >= PROOF_ENTRIES {
        return Ok(());
    }
    noted.push(attempt.to_string());
    let bytes = serde_json::to_vec(&proof).map_err(|_| invalid("trial proof"))?;
    place_file(dir, RENEWED, &bytes)
}

/// What the version on trial noted; nothing when it noted nothing. A proof
/// that cannot be read safely is an error, never an empty one.
pub fn trial_proof(state_dir: &Path) -> std::io::Result<TrialProof> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(state_dir.join(RENEWED))
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(TrialProof::default()),
        Err(e) => return Err(e),
    };
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != rustix::process::geteuid().as_raw()
        || meta.mode() & 0o077 != 0
    {
        return Err(invalid("unsafe trial proof"));
    }
    let mut bytes = Vec::new();
    (&mut file).take(PROOF_LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > PROOF_LIMIT {
        return Err(invalid("trial proof too large"));
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid("trial proof unparsable"))
}

fn remove_marker(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => std::fs::File::open(path.parent().unwrap_or(Path::new("/")))?.sync_all(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }

    #[test]
    fn a_trial_proof_notes_each_attempt_once_and_lives_only_as_long_as_its_trial() {
        let d = dir();
        let marker = trial_path(d.path());
        let none = TrialProof::default();
        assert_eq!(trial_proof(d.path()).unwrap(), none);
        begin_trial(d.path()).unwrap();
        note_renewal(&marker, "a-1").unwrap();
        note_renewal(&marker, "a-1").unwrap();
        note_unrenewed(&marker, "a-2").unwrap();
        assert_eq!(
            trial_proof(d.path()).unwrap(),
            TrialProof {
                renewed: vec!["a-1".into()],
                unrenewed: vec!["a-2".into()],
            }
        );
        // The commit ends it with the trial.
        end_trial(d.path()).unwrap();
        assert_eq!(trial_proof(d.path()).unwrap(), none);
        // One left behind by a rollback does not carry into the next trial.
        note_renewal(&marker, "stale").unwrap();
        begin_trial(d.path()).unwrap();
        assert_eq!(trial_proof(d.path()).unwrap(), none);
    }

    #[test]
    fn missing_file_requests_nothing() {
        let d = dir();
        assert_eq!(read(d.path()).unwrap(), Control::default());
        assert_eq!(Control::effective(d.path()), Control::default());
    }

    #[test]
    fn round_trips_each_request() {
        let d = dir();
        for control in [
            Control::draining(),
            Control::cancelling(),
            Control::default(),
        ] {
            write(d.path(), control).unwrap();
            assert_eq!(read(d.path()).unwrap(), control);
        }
        let mode = std::fs::metadata(path(d.path())).unwrap().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn a_broken_file_drains_but_does_not_cancel() {
        let d = dir();
        std::fs::write(path(d.path()), b"{").unwrap();
        std::fs::set_permissions(path(d.path()), std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read(d.path()).is_err());
        assert_eq!(Control::effective(d.path()), Control::draining());
    }

    #[test]
    fn unsafe_permissions_and_symlinks_are_refused() {
        let d = dir();
        std::fs::write(path(d.path()), br#"{"drain":false,"cancel_running":false}"#).unwrap();
        std::fs::set_permissions(path(d.path()), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read(d.path()).is_err());
        std::fs::remove_file(path(d.path())).unwrap();
        let target = d.path().join("target");
        std::fs::write(&target, br#"{"drain":false,"cancel_running":false}"#).unwrap();
        std::os::unix::fs::symlink(&target, path(d.path())).unwrap();
        assert!(read(d.path()).is_err());
    }

    #[test]
    fn the_hold_is_its_own_state_and_resume_does_not_lift_it() {
        let d = dir();
        assert!(!held(d.path()));
        hold(d.path()).unwrap();
        hold(d.path()).unwrap();
        assert!(held(d.path()));
        // What `resume` writes leaves the hold in place.
        write(d.path(), Control::default()).unwrap();
        assert!(held(d.path()));
        assert_eq!(read(d.path()).unwrap(), Control::default());
        lift_hold(d.path()).unwrap();
        lift_hold(d.path()).unwrap();
        assert!(!held(d.path()));
    }

    #[test]
    fn oversized_and_unknown_fields_are_refused() {
        let d = dir();
        let p = path(d.path());
        std::fs::write(&p, vec![b' '; 5000]).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read(d.path()).is_err());
        std::fs::write(&p, br#"{"drain":true,"cancel_running":false,"x":1}"#).unwrap();
        assert!(read(d.path()).is_err());
    }
}
