//! Where tests put their temporary directories: all under one root that this
//! test process owns (`/tmp/rtr-t-XXXXXX/d`).
//!
//! Each directory is still an RAII guard. The root is removed when the
//! process exits, which also takes what a guard could not remove. A test
//! process that is killed (a harness timeout) runs neither, so the first use
//! in every test process removes the roots whose recorded owner is proven
//! gone. See `platform::owned_temp` for that proof.
//!
//! Shared by the unit tests and, through `#[path]`, the integration tests.

use remote_task_runner::platform::owned_temp::{sweep, OwnedTemp};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

const PREFIX: &str = "rtr-t-";

struct Root {
    owner: u32,
    // Taken, and so removed, by `remove_root` at exit.
    owned: Mutex<Option<OwnedTemp>>,
    path: std::path::PathBuf,
}

static ROOT: OnceLock<Root> = OnceLock::new();

extern "C" fn remove_root() {
    let Some(root) = ROOT.get() else {
        return;
    };
    // A forked copy of this process does not own the root.
    if std::process::id() != root.owner {
        return;
    }
    if let Ok(mut owned) = root.owned.try_lock() {
        owned.take();
    }
}

/// This process's root for temporary directories.
pub fn root() -> &'static Path {
    &ROOT
        .get_or_init(|| {
            let parent = std::fs::canonicalize("/tmp").expect("/tmp exists");
            sweep(&parent, PREFIX);
            let owned = OwnedTemp::new_in(&parent, PREFIX).expect("test root");
            let path = owned.path().to_path_buf();
            // The test harness ends through `exit`, which runs this.
            unsafe { libc::atexit(remove_root) };
            Root {
                owner: std::process::id(),
                owned: Mutex::new(Some(owned)),
                path,
            }
        })
        .path
}

/// A temporary directory under [`root`].
pub fn tempdir() -> std::io::Result<tempfile::TempDir> {
    tempfile::tempdir_in(root())
}

/// A temporary directory under [`root`] whose name starts with `prefix`.
pub fn prefixed(prefix: &str) -> std::io::Result<tempfile::TempDir> {
    tempfile::Builder::new().prefix(prefix).tempdir_in(root())
}
