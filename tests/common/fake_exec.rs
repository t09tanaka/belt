//! Write a fake executable that a test is about to run.
//!
//! Shared by the unit tests (`src/lib.rs` pulls it in with `#[path]`) and the
//! integration tests (`tests/common/mod.rs`).
//!
//! `std::fs::write` followed by `exec` races on Linux: while this thread holds
//! the file open for writing, another test thread may `fork`, and its child
//! keeps a copy of that write fd until its own `exec` closes it (O_CLOEXEC).
//! An `execve` of the file in that window fails with ETXTBSY ("Text file
//! busy"). Closing early, or writing a temporary file and renaming it, does
//! not help: ETXTBSY is per inode, and a rename keeps the inode.
//!
//! So the test process never opens the executable for writing. A child `cat`
//! writes it to a temporary name, the child exits (its fds go with it), and
//! the test process only sets the mode and renames it into place, neither of
//! which needs a write fd.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Create (or atomically replace) `path` with `body` and permission `mode`.
#[allow(clippy::disallowed_methods)] // a test fixture, not an engine command
pub fn write_executable(path: &Path, body: impl AsRef<[u8]>, mode: u32) {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let name = path.file_name().expect("executable path has a file name");
    let tmp = path.with_file_name(format!(
        ".{}.{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut child = Command::new("/bin/sh")
        .args(["-c", "cat > \"$1\"", "sh"])
        .arg(&tmp)
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn sh to write the executable");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_ref())
        .unwrap();
    let st = child.wait().unwrap();
    assert!(st.success(), "writing {} failed: {st}", tmp.display());
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode)).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}
