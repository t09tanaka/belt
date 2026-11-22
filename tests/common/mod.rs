//! Shared by the integration tests.

#![allow(dead_code)]

pub mod fake_exec;

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
    let bin = home.join("fixture-docker-bin");
    std::fs::create_dir_all(&bin).unwrap();
    // Replaced atomically, so a concurrent caller with the same home still
    // runs a complete copy.
    fake_exec::write_executable(
        &bin.join("docker"),
        "#!/bin/sh\ncase \"$1\" in\n  ps) exit 0;;\n  *) exit 1;;\nesac\n",
        0o755,
    );
    format!("{}:{path}", bin.display())
}

/// Install the npm dependencies of an example directory when `node_modules`
/// has no complete install (`node_modules/.package-lock.json`) newer than
/// `package-lock.json`, as CI does with `npm ci`.
/// Tests run in parallel, so the install is serialised by a file lock and
/// re-checked under it: one test installs, the others wait and reuse it.
/// Without npm this panics with the fix; it never skips, which would weaken
/// the gate.
pub fn ensure_npm_deps(dir: &Path) {
    use fs2::FileExt;
    // npm writes `node_modules/.package-lock.json` last, so a failed or
    // interrupted `npm ci` leaves it missing (stale) rather than looking fresh.
    let marker = dir.join("node_modules").join(".package-lock.json");
    let fresh = || {
        let mtime = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        let Some(m) = mtime(&marker) else {
            return false;
        };
        ["package-lock.json", "package.json"]
            .iter()
            .filter_map(|f| mtime(&dir.join(f)))
            .all(|src| m >= src)
    };
    if fresh() {
        return;
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(".npm-ci.lock"))
        .expect("cannot create the npm install lock");
    lock.lock_exclusive()
        .expect("cannot take the npm install lock");
    if !fresh() {
        let out = std::process::Command::new("npm")
            .args(["ci", "--no-audit", "--no-fund"])
            .current_dir(dir)
            .output()
            .unwrap_or_else(|e| {
                panic!(
                    "npm is required for the vitest shard tests; install Node or run \
                     `npm ci` in {} ({e})",
                    dir.display()
                )
            });
        assert!(
            out.status.success(),
            "`npm ci` in {} failed ({}):\n{}",
            dir.display(),
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let _ = lock.unlock();
}
