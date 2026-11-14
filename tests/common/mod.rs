//! Shared by the integration tests.

#![allow(dead_code)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// A temporary directory that can be removed even when the code under test
/// left read-only directories in it (the content-addressed stores are
/// read-only by design). `tempfile` ignores that failure and leaks the
/// directory, which a mutation run then cannot clear.
pub struct Tmp(tempfile::TempDir);

pub fn tempdir() -> std::io::Result<Tmp> {
    tempfile::tempdir().map(Tmp)
}

impl Tmp {
    pub fn path(&self) -> &Path {
        self.0.path()
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let mut todo = vec![self.0.path().to_path_buf()];
        while let Some(dir) = todo.pop() {
            let Ok(meta) = std::fs::symlink_metadata(&dir) else {
                continue;
            };
            if !meta.is_dir() {
                continue;
            }
            let mut mode = meta.permissions();
            mode.set_mode(mode.mode() | 0o700);
            let _ = std::fs::set_permissions(&dir, mode);
            if let Ok(rd) = std::fs::read_dir(&dir) {
                todo.extend(rd.flatten().map(|e| e.path()));
            }
        }
    }
}

/// Keep Docker-free execution fixtures independent of the host daemon. The
/// private PATH entry only answers cleanup's empty-container query; unexpected
/// Docker use fails instead of pretending a service was started.
pub fn private_docker_path(home: &Path, path: &str) -> String {
    use std::io::Write;
    let bin = home.join("fixture-docker-bin");
    std::fs::create_dir_all(&bin).unwrap();
    let mut staged = tempfile::NamedTempFile::new_in(&bin).unwrap();
    staged
        .write_all(b"#!/bin/sh\ncase \"$1\" in\n  ps) exit 0;;\n  *) exit 1;;\nesac\n")
        .unwrap();
    staged
        .as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o755))
        .unwrap();
    match staged.persist_noclobber(bin.join("docker")) {
        Ok(_) => (),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => panic!("private Docker fixture: {e}"),
    }
    format!("{}:{path}", bin.display())
}
