//! Shared by the integration tests.

#![allow(dead_code)]

use belt::platform::{process_group_of, process_start_identity, signal_pid, StartIdentity};
use belt::supervisor::identity::Leader;
use belt::supervisor::stop::{stop_group, Members, NoEvidence, StopTarget, StopTimes};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Inner fixture commands already run inside the harness's shared TTC lease.
/// Keep their private admission ledger, without depending on unrelated host load.
pub fn fixture_traffic_config(state: &Path) {
    fixture_traffic_config_file(&state.join("traffic/config.toml"), 2);
}

/// The same with another budget than the usual 2: the budget is counted in
/// weights, and a command weighs 2 by default.
pub fn fixture_traffic_budget(state: &Path, budget: u32) {
    fixture_traffic_config_file(&state.join("traffic/config.toml"), budget);
}

/// The same for a worker started from `env`, which reads the configuration
/// of the task-traffic-controller CLI of that environment.
pub fn fixture_worker_traffic_config(env: &std::collections::BTreeMap<String, String>) {
    fixture_worker_traffic_budget(env, 2);
}

/// The same with another budget than the usual 2.
pub fn fixture_worker_traffic_budget(
    env: &std::collections::BTreeMap<String, String>,
    budget: u32,
) {
    fixture_traffic_config_file(
        &belt::config::traffic::resolve(env).unwrap().config_file,
        budget,
    );
}

fn fixture_traffic_config_file(path: &Path, budget: u32) {
    let traffic = path.parent().unwrap();
    std::fs::create_dir_all(traffic).unwrap();
    std::fs::set_permissions(traffic, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = task_traffic_controller::config::Config {
        budget,
        cpu_threshold_percent: 100.0,
        memory_reserve_mib: 0,
        ..Default::default()
    };
    std::fs::write(path, toml::to_string(&config).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

/// A temporary directory that can be removed even though the content-addressed
/// stores under it are read-only by design (`tempfile` alone would leave it
/// until the test process exits).
pub struct Tmp(tempfile::TempDir);

pub fn tempdir() -> std::io::Result<Tmp> {
    crate::test_tmp::tempdir().map(Tmp)
}

impl Tmp {
    pub fn path(&self) -> &Path {
        self.0.path()
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        belt::platform::owned_temp::make_writable(self.0.path());
    }
}

/// Whether a process with this pid exists (signal 0 through the guarded helper).
pub fn alive(pid: i32) -> bool {
    u32::try_from(pid).is_ok_and(|p| signal_pid(p, 0))
}

/// The start identity of `pid`, for a process this test just started and
/// still holds: what a later cleanup compares before it signals.
pub fn identity_of(pid: u32) -> Option<String> {
    process_start_identity(pid).identity().map(str::to_string)
}

/// SIGKILL `pid` alone, only while it still is the process that had the
/// start identity `start`. A PID that was reused, or whose identity cannot
/// be read, is left alone.
pub fn kill_recorded(pid: u32, start: &str) {
    if process_start_identity(pid) == StartIdentity::Identity(start.to_string()) {
        signal_pid(pid, libc::SIGKILL);
    }
}

/// The command line of `pid`, as `ps` shows it.
fn command_line(pid: u32) -> Option<String> {
    let out = std::process::Command::new("ps")
        .args(["-ww", "-o", "command=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// SIGKILL `pid` alone, for a PID that a search of the process table gave
/// (`pgrep`): only when the process whose identity is read first has
/// `pattern` in its command line, and is still that process right before
/// the signal. A number that was reused since the search, or whose identity
/// cannot be read, is left alone.
pub fn kill_if_command(pid: i32, pattern: &str) {
    let Ok(pid) = u32::try_from(pid) else {
        return;
    };
    let Some(start) = identity_of(pid) else {
        return;
    };
    if command_line(pid).is_some_and(|c| c.contains(pattern)) {
        kill_recorded(pid, &start);
    }
}

/// Processes a test started itself and read while it held them (PID and
/// start identity): the evidence that a group whose leader has ended still
/// holds one of them.
pub struct RecordedMembers(pub Vec<(u32, String)>);

impl Members for RecordedMembers {
    fn proven(&self, pgid: i32) -> bool {
        self.0.iter().any(|(pid, start)| {
            let same = || process_start_identity(*pid) == StartIdentity::Identity(start.clone());
            // Read again after the group: the group read was of that process.
            same() && process_group_of(*pid) == Some(pgid) && same()
        })
    }
}

/// Stop the group led by `pid`, through the same gate the supervisor uses:
/// the leader must still have the start identity `start`. A reused PID or
/// an unreadable identity is never signalled, and neither is what a leader
/// that is gone left under its number (there is no evidence here of whose
/// that is).
pub fn kill_recorded_group(pid: u32, start: &str) {
    let Ok(pgid) = i32::try_from(pid) else {
        return;
    };
    let target = StopTarget {
        pid,
        pgid,
        leader: Leader::Known(start.to_string()),
    };
    let times = StopTimes {
        grace_ms: 0,
        kill_wait_ms: 2_000,
        poll_ms: 10,
    };
    let _ = stop_group(&target, times, &NoEvidence);
}

/// The pid of the process that listens on the Unix socket at `socket`, as the
/// kernel reports it for a new connection. None when nothing listens there.
pub async fn listener_pid(socket: &Path) -> Option<u32> {
    let stream = tokio::net::UnixStream::connect(socket).await.ok()?;
    let pid = stream.peer_cred().ok()?.pid()?;
    u32::try_from(pid).ok().filter(|pid| *pid > 1)
}

/// A private fixture root that stops the resident daemon of `state` when the
/// test ends, also while a failed assertion unwinds. The daemon runs detached
/// in its own process group, so nothing else ends it with the test.
pub struct DaemonRoot {
    state: std::path::PathBuf,
    // Removed after the daemon is stopped: fields drop after `Drop::drop`.
    dir: tempfile::TempDir,
}

impl DaemonRoot {
    pub fn new(dir: tempfile::TempDir, state: &Path) -> Self {
        Self {
            state: state.to_path_buf(),
            dir,
        }
    }
    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}

impl Drop for DaemonRoot {
    fn drop(&mut self) {
        stop_daemon(&self.state);
        // Prepared templates under the worker state are read-only by design,
        // and removal stops at the first directory it cannot empty.
        belt::platform::owned_temp::make_writable(self.dir.path());
    }
}

/// Wait up to `limit` until `pid` no longer is the live process that had the
/// start identity `start`. True when it is gone (or only a zombie remains).
async fn ended(pid: u32, start: &str, limit: std::time::Duration) -> bool {
    let same = || process_start_identity(pid) == StartIdentity::Identity(start.to_string());
    let _ = tokio::time::timeout(limit, async {
        while same() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    !same()
}

/// Stop the resident daemon that owns `state`, if one is listening: ask it to
/// shut down, wait until its process is gone, and only when it stays, SIGKILL
/// the listener's pid while it still has the start identity read here.
/// Never panics (it runs during unwinding) and never searches the process
/// table: a command line does not say which state directory a daemon owns.
pub fn stop_daemon(state: &Path) {
    use std::time::Duration;
    let socket = state.join("parent/request.sock");
    if !socket.exists() {
        return;
    }
    let state = state.to_path_buf();
    // A thread with its own runtime: the caller may be inside one.
    let worker = std::thread::spawn(move || {
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        runtime.block_on(async {
            let Some(pid) = listener_pid(&socket).await else {
                return;
            };
            let Some(start) = identity_of(pid) else {
                return;
            };
            let client = belt::storage::CoordinatorStore::open_read_only(&state)
                .ok()
                .and_then(|store| {
                    belt::service::resident::Client::load(&state, store.cluster_id().clone()).ok()
                });
            if let Some(client) = client {
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.request(belt::service::resident::Operation::Shutdown),
                )
                .await;
            }
            if !ended(pid, &start, Duration::from_secs(10)).await {
                kill_recorded(pid, &start);
                ended(pid, &start, Duration::from_secs(5)).await;
            }
        });
    });
    let _ = worker.join();
}

/// Wait until the resident that owns `state` has let go of its owner lock:
/// its socket is removed first, and its database is closed (its WAL
/// checkpointed) before the lock goes. The wait blocks on the lock itself,
/// so it ends when the resident does, however busy the machine is.
pub async fn wait_owner_released(state: &Path) {
    let lock = std::fs::File::open(state.join("coordinator.lock")).unwrap();
    let lock = tokio::task::spawn_blocking(move || {
        lock.lock_shared().unwrap();
        lock
    })
    .await
    .unwrap();
    lock.unlock().unwrap();
    assert!(
        !state.join("parent/request.sock").exists(),
        "the owner lock went before the socket"
    );
}

/// The pid written in a file, if it holds one.
pub fn read_pid(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// The host's system command directories as a toolchain registry must name
/// them: canonical and without repeats. `/bin` is a symlink to `usr/bin` on a
/// merged-usr Linux, and the registry refuses a search path through a symlink.
pub fn system_search_path() -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for dir in ["/usr/bin", "/bin"] {
        let real = std::fs::canonicalize(dir).unwrap();
        if !out.contains(&real) {
            out.push(real);
        }
    }
    out
}

/// [`system_search_path`] as the tail of a TOML array: `,'/usr/bin','/bin'`.
pub fn system_search_path_toml() -> String {
    system_search_path()
        .iter()
        .map(|dir| format!(",{:?}", dir.to_str().unwrap()))
        .collect()
}

/// A directory to put first in a fixture's PATH: every probed tool that the
/// fixture does not use is answered there, at once and always the same way,
/// instead of by whatever the host has under `/usr/bin` or a package manager
/// and however long that takes to start. `git` is left to the host: the
/// fixtures run it.
pub fn tool_stubs(root: &Path) -> std::path::PathBuf {
    let dir = root.join("tool-stubs");
    std::fs::create_dir(&dir).unwrap();
    for tool in [
        "node",
        "npm",
        "pnpm",
        "yarn",
        "rustc",
        "cargo",
        "flutter",
        "dart",
        "xcodebuild",
        "xcrun",
        "portool",
        "fast-worktree",
        "docker",
    ] {
        write_executable(&dir.join(tool), "#!/bin/sh\nexit 1\n");
    }
    dir
}

/// Write `text` to `path` and make it executable, so that it can be run at
/// once by a test that shares its process with other tests.
///
/// A file this process holds open for writing cannot be executed (ETXTBSY),
/// and another thread that forks at that moment hands the descriptor to its
/// child until the child execs. So the file is written by a short-lived child
/// instead: no descriptor of it ever exists in this process.
pub fn write_executable(path: &Path, text: &str) {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg("cat > \"$0\" && chmod 755 \"$0\"")
        .arg(path)
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();
    assert!(child.wait().unwrap().success(), "writing {path:?}");
}

/// `LLVM_PROFILE_FILE` when this test run is measured for coverage. A child
/// started with an emptied environment does not inherit it and would write
/// `default_*.profraw` into its working directory, unmeasured; pass this on
/// to such a child. Only tests use it: production keeps its environment.
pub fn profile_env() -> Option<(String, String)> {
    std::env::var("LLVM_PROFILE_FILE")
        .ok()
        .map(|v| ("LLVM_PROFILE_FILE".to_string(), v))
}

/// Publish a source captured straight into a worker's store the way a
/// delivery publishes it: a worker's store holds only what it received
/// (`receive:<S>`, spec 25.8 5b (b)), never a capture's own roots.
pub fn publish_as_received(store: &belt::source::SourceStore, ci_source_id: &str) {
    // (its commit, if any, was made in this store by the capture: the
    // publication's reachability check of a delivered bundle is not asked)
    let m = store.manifest(ci_source_id).unwrap().unwrap();
    store.publish_received_one(&m, false, true, None).unwrap();
}

/// The source store of a worker that is running, with its mutation lock held
/// shared for as long as the returned guard lives. Opening, capturing and
/// publishing each take that lock shared without waiting, and the worker's
/// retention passes hold it exclusively for one step (spec 25.8): wait,
/// bounded, until no pass does, then hold it across the whole seed so no
/// later step of it meets a pass. The wait is printed so a regression where
/// a pass holds the lock for seconds shows up.
pub fn open_live_store(
    state: &std::path::Path,
) -> (belt::source::SourceStore, belt::source::retention::Guard) {
    fn busy(e: &anyhow::Error) -> bool {
        e.chain().any(|c| {
            c.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::WouldBlock)
        })
    }
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(5);
    loop {
        let held = belt::source::SourceStore::open(state)
            .and_then(|store| store.mutation_guard().map(|guard| (store, guard)));
        match held {
            Ok(held) => {
                eprintln!(
                    "open_live_store: waited {:?} for the worker's retention pass",
                    started.elapsed()
                );
                return held;
            }
            Err(e) if std::time::Instant::now() < deadline && busy(&e) => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(e) => panic!("open the worker's source store: {e:#}"),
        }
    }
}
