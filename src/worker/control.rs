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
