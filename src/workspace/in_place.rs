//! Private original-worktree binding. Never part of a transferable payload.
use crate::{
    error::{ApiError, ErrorCode},
    ids::*,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
#[derive(Serialize, Deserialize)]
pub struct Binding {
    pub root: PathBuf,
    pub source: SourceId,
    pub digest: ManifestDigest,
    pub worker: WorkerId,
    pub declared: Vec<(String, bool)>,
    dev: u64,
    ino: u64,
}
fn invalid() -> ApiError {
    ApiError::new(
        ErrorCode::InvalidInput,
        "in-place original worktree binding unavailable or changed",
    )
}
fn directory(state: &Path) -> Result<PathBuf, ApiError> {
    let dir = state.join("in-place");
    match fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(invalid()),
    }
    let m = fs::symlink_metadata(&dir).map_err(|_| invalid())?;
    if !m.is_dir() || m.mode() & 0o077 != 0 || m.uid() != unsafe { libc::geteuid() } {
        return Err(invalid());
    }
    Ok(dir)
}
pub fn bind(
    state: &Path,
    run: &RunId,
    source: &SourceId,
    digest: &ManifestDigest,
    worker: &WorkerId,
    root: &Path,
    declared: Vec<(String, bool)>,
) -> Result<(), ApiError> {
    let root = fs::canonicalize(root).map_err(|_| invalid())?;
    let m = fs::metadata(&root).map_err(|_| invalid())?;
    let b = Binding {
        root,
        source: source.clone(),
        digest: digest.clone(),
        worker: worker.clone(),
        declared,
        dev: m.dev(),
        ino: m.ino(),
    };
    let dir = directory(state)?;
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(dir.join(format!("{run}.json")))
        .map_err(|_| invalid())?;
    f.write_all(&serde_json::to_vec(&b).map_err(|_| invalid())?)
        .and_then(|_| f.sync_all())
        .and_then(|_| std::fs::File::open(&dir)?.sync_all())
        .map_err(|_| invalid())
}
pub fn read_secrets(
    binding: &Binding,
    env: &std::collections::BTreeMap<String, String>,
    paths: &[String],
) -> Result<crate::secrets::place::Placed, ApiError> {
    let declared = binding
        .declared
        .iter()
        .map(|(glob, required)| local_ci::source::SecretSpec {
            glob: glob.clone(),
            required: *required,
        })
        .collect::<Vec<_>>();
    if paths.is_empty() && binding.declared.is_empty() && !binding.root.join(".git").exists() {
        return Ok(crate::secrets::place::Placed::default());
    }
    let included = crate::secrets::worktreeinclude::ignored_matches(env, &binding.root)
        .map_err(|e| e.to_api())?;
    let mut selected =
        crate::secrets::worktreeinclude::secret_paths(env, &binding.root, &declared, &included)
            .map_err(|e| e.to_api())?;
    let mut expected = paths.to_vec();
    selected.sort();
    selected.dedup();
    expected.sort();
    expected.dedup();
    if selected != expected {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            "in-place secret inventory changed; submit a new run",
        ));
    }
    crate::secrets::place::read_original(&binding.root, &expected).map_err(|e| e.to_api())
}
pub fn load(
    state: &Path,
    run: &RunId,
    source: &SourceId,
    digest: &ManifestDigest,
    worker: &WorkerId,
) -> Result<Binding, ApiError> {
    use std::io::Read;
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory(state)?.join(format!("{run}.json")))
        .map_err(|_| invalid())?;
    let m = f.metadata().map_err(|_| invalid())?;
    if !m.is_file()
        || m.mode() & 0o077 != 0
        || m.uid() != unsafe { libc::geteuid() }
        || m.len() > 65536
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    f.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    let b: Binding = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    let m = fs::symlink_metadata(&b.root).map_err(|_| invalid())?;
    if !m.is_dir()
        || m.dev() != b.dev
        || m.ino() != b.ino
        || &b.source != source
        || &b.digest != digest
        || &b.worker != worker
    {
        return Err(invalid());
    }
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binding_checks_worker_source_digest_and_original_inode() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        fs::create_dir(&root).unwrap();
        let run = RunId::generate();
        let source = SourceId::generate();
        let digest = Digest::of_bytes(b"baseline");
        let worker = WorkerId::parse("parent-local").unwrap();
        bind(temp.path(), &run, &source, &digest, &worker, &root, vec![]).unwrap();
        assert!(load(temp.path(), &run, &source, &digest, &worker).is_ok());
        assert!(load(temp.path(), &run, &SourceId::generate(), &digest, &worker).is_err());
        assert!(load(
            temp.path(),
            &run,
            &source,
            &Digest::of_bytes(b"other"),
            &worker
        )
        .is_err());
        assert!(load(
            temp.path(),
            &run,
            &source,
            &digest,
            &WorkerId::parse("remote").unwrap()
        )
        .is_err());
        fs::rename(&root, temp.path().join("old")).unwrap();
        fs::create_dir(&root).unwrap();
        assert!(load(temp.path(), &run, &source, &digest, &worker).is_err());
    }
    #[test]
    fn original_secret_reader_never_writes_or_erases_and_rejects_links() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let path = root.join("secret");
        fs::write(&path, b"TOKEN=original-secret-value\n").unwrap();
        let before = fs::metadata(&path).unwrap();
        let paths = vec!["secret".to_string()];
        let values = crate::secrets::place::read_original(root, &paths).unwrap();
        assert!(values
            .values
            .values
            .contains(&b"original-secret-value".to_vec()));
        fs::write(&path, b"TOKEN=changed-secret-value\n").unwrap();
        let values = crate::secrets::place::read_original(root, &paths).unwrap();
        assert!(values
            .values
            .values
            .contains(&b"changed-secret-value".to_vec()));
        assert_eq!(fs::metadata(&path).unwrap().mode(), before.mode());
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("other", &path).unwrap();
        assert!(crate::secrets::place::read_original(root, &paths).is_err());
    }
}
