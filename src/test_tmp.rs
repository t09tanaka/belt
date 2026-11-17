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
            let parent = match std::env::var_os("RTR_MUTATION_FIXTURE_PARENT") {
                // The mutation driver removes its private parent after the shard.
                // PID classification itself can be under mutation here.
                Some(private) => std::fs::canonicalize(private).expect("mutation fixture parent"),
                None => {
                    let shared = std::fs::canonicalize("/tmp").expect("/tmp exists");
                    sweep(&shared, PREFIX);
                    shared
                }
            };
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

/// How long a test waits for a run's GC lock that only a forked child of
/// another test can still hold.
const GC_LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// Retry `try_once` while it fails with `WouldBlock` (the lock is held).
///
/// The run lock is an `flock` and the guard's descriptor is close-on-exec,
/// but a test thread that forks (to start a child process) hands a copy of
/// every descriptor to the child until its `exec`. A lock dropped by one test
/// therefore stays held for that short moment, and the next non-blocking
/// acquisition sees `WouldBlock`. Production still declines and moves on;
/// only a test that never meant to contend waits here. Other errors and a
/// lock that stays held for [`GC_LOCK_WAIT`] still fail the test.
fn until_free<T>(
    mut try_once: impl FnMut() -> Result<T, remote_task_runner::storage::StorageError>,
) -> Result<T, remote_task_runner::storage::StorageError> {
    use remote_task_runner::storage::StorageError;
    let start = std::time::Instant::now();
    loop {
        match try_once() {
            Err(StorageError::Io(e))
                if e.kind() == std::io::ErrorKind::WouldBlock && start.elapsed() < GC_LOCK_WAIT =>
            {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            other => return other,
        }
    }
}

/// `result_gc_guard` for a test that does not itself hold the lock: waits
/// out a copy of it that a forked child of another test still holds.
pub fn gc_guard(
    store: &remote_task_runner::storage::CoordinatorStore,
    run: &remote_task_runner::ids::RunId,
) -> remote_task_runner::results::pin::RunPin {
    until_free(|| store.result_gc_guard(run)).unwrap()
}

// Shared with the integration tests through `#[path]`, which do not page.
#[allow(dead_code)]
/// One retention page with a fresh cursor, retried while its run's lock is
/// held by such a copy (the page reports `skipped` then, and did nothing).
pub fn settled_page(
    store: &remote_task_runner::storage::CoordinatorStore,
    files: &remote_task_runner::results::ResultStore,
    retention_days: u32,
) -> remote_task_runner::results::retention::PageReport {
    let start = std::time::Instant::now();
    loop {
        let mut cursor = remote_task_runner::results::retention::Cursor::default();
        let report =
            remote_task_runner::results::retention::page(store, files, &mut cursor, retention_days)
                .unwrap();
        if !report.skipped || start.elapsed() >= GC_LOCK_WAIT {
            return report;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}
