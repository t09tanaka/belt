//! Real private worker roles, through a fixture-owned fake ssh executable.
//! No host SSH server, service manager, or user configuration is touched.
use remote_task_runner::coordinator::TaskSpec;
use remote_task_runner::ids::*;
use remote_task_runner::platform::wall_ms;
use remote_task_runner::service::bootstrap::Spec;
use remote_task_runner::service::resident::{Client, Operation, Reply};
use remote_task_runner::service::worker::WorkerSpec;
use remote_task_runner::state::{Observation, Outcome};
use remote_task_runner::storage::CoordinatorStore;
use remote_task_runner::wire::CommandSpec;
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
const BIN: &str = env!("CARGO_BIN_EXE_remote-task-runner");
const T: Duration = Duration::from_secs(40);

struct Owned {
    child: Option<tokio::process::Child>,
    pid: u32,
    start: String,
}
impl Owned {
    fn new(child: tokio::process::Child) -> Self {
        let pid = child.id().unwrap();
        assert!(pid > 1);
        let start = super::common::identity_of(pid).unwrap();
        Self {
            child: Some(child),
            pid,
            start,
        }
    }
    async fn wait(mut self) {
        tokio::time::timeout(T, self.child.as_mut().unwrap().wait())
            .await
            .unwrap()
            .unwrap();
        self.child.take();
    }
    async fn stop(self) {
        if super::common::identity_of(self.pid).as_deref() == Some(&self.start) {
            remote_task_runner::platform::signal_pid(self.pid, libc::SIGTERM);
        }
        self.wait().await;
    }
}
impl Drop for Owned {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            super::common::kill_recorded(self.pid, &self.start);
            tokio::spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}
fn private(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
}
fn write_private(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
fn spawn_worker(home: &Path, cluster: &ClusterId, key: &Path, id: &str) -> Owned {
    private(home);
    let state = home.join(".remote-task-runner");
    private(&state);
    let input = home.join("worker.json");
    let spec = WorkerSpec {
        schema: 1, expected_cluster: cluster.clone(), worker_id: WorkerId::parse(id).unwrap(),
        state_dir: state, cluster_key_path: key.to_owned(), executable: BIN.into(),
        env: BTreeMap::from([("HOME".into(), home.display().to_string()), ("PATH".into(), "/usr/bin:/bin".into())]),
        config: "schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\nmax_active_task=1\n[scheduler]\nprefetch_per_worker=0\n".into(),
    };
    write_private(&input, &serde_json::to_vec(&spec).unwrap());
    Owned::new(
        tokio::process::Command::new(BIN)
            .args(["worker", "--input"])
            .arg(input)
            .env("HOME", home)
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                std::fs::File::create(home.join("fixture-worker.stderr")).unwrap(),
            ))
            .spawn()
            .unwrap(),
    )
}
async fn wait_for(label: &str, mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(T, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
}
fn bridge(home: &Path, parent: u32) -> Option<(u32, String)> {
    let pid = std::fs::read_to_string(home.join("bridge.pid"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let process = remote_task_runner::platform::read_process(pid);
    if process.ppid != Some(parent) {
        return None;
    }
    Some((pid, process.start.identity()?.to_string()))
}
async fn submit(
    client: &Client,
    state: &Path,
    env: &BTreeMap<String, String>,
    key: &Path,
    root: &Path,
    name: &str,
    delay: u32,
) -> TaskId {
    let dir = root.join(format!("source-{name}"));
    private(&dir);
    std::fs::write(dir.join("ordinary"), name).unwrap();
    std::fs::write(dir.join(".env"), format!("TOKEN={name}-private\n")).unwrap();
    let run = RunId::generate();
    let source_store = remote_task_runner::source::SourceStore::open(state).unwrap();
    let vault = remote_task_runner::secrets::vault::Vault::open(state).unwrap();
    let key = remote_task_runner::secrets::key::load(key).unwrap();
    let capture = remote_task_runner::secrets::capture::capture_run(
        env,
        &source_store,
        &vault,
        &remote_task_runner::secrets::capture::SecretInputs {
            key: &key,
            declared: &[local_ci::source::SecretSpec {
                glob: ".env".into(),
                required: true,
            }],
        },
        &run,
        &remote_task_runner::source::SourceRequest::Directory { dir },
        &Default::default(),
    )
    .unwrap();
    let manifest = &capture.manifest.manifest_digest;
    let task = TaskId::generate();
    let count = remote_task_runner::protocol::ssh::quote(
        &root.join(format!("count-{name}")).display().to_string(),
    );
    let wait = if delay == u32::MAX {
        let release = remote_task_runner::protocol::ssh::quote(
            &root.join("release-local").display().to_string(),
        );
        format!(
            "i=0; while [ ! -e {release} ] && [ \"$i\" -lt 600 ]; do i=$((i+1)); sleep 0.05; done"
        )
    } else {
        format!("sleep {delay}")
    };
    let spec = TaskSpec {
        ci: None,
        toolchains: None,
        in_place: false, outputs: Vec::new(), requirements: Default::default(),
        setup: None, plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(manifest).or_else(|_| Digest::parse(&format!("sha256:{manifest}"))).unwrap(),
        commands: vec![CommandSpec { node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), format!("test \"$(cat .env)\" = \"TOKEN=$(cat ordinary)-private\" && echo once >> {count} && {wait}")],
            cwd: String::new(), env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            depends_on: vec![], timeout_ms: None, setup_step: None }],
        command_timeout_ms: None, deadline_ms: None, resource_wait_timeout_ms: None,
        has_secret_bundle: true, artifacts: vec![],
    };
    let reply = client
        .request(Operation::SubmitCaptured {
            run_id: run,
            source_id: capture.manifest.source_id,
            tasks: vec![(task.clone(), spec)],
        })
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Accepted { .. }), "{reply:?}");
    task
}
fn attempt(state: &Path, task: &TaskId) -> Option<remote_task_runner::storage::AttemptRecord> {
    CoordinatorStore::open_read_only(state)
        .unwrap()
        .latest_attempt_of_task(task)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_remote_roles_deliver_inputs_and_reconnect_same_attempt_while_offline_peers_do_not_block_local(
) {
    let root = crate::test_tmp::tempdir().unwrap();
    let root = root.path();
    private(root);
    let state = root.join("parent");
    let local = root.join("local");
    let xdg = root.join("config");
    let config_dir = xdg.join("remote-task-runner");
    let bin = root.join("bin");
    for dir in [&state, &local, &config_dir, &bin] {
        private(dir);
    }
    let key = root.join("key");
    remote_task_runner::secrets::key::init(&key).unwrap();
    let cluster = {
        let store = CoordinatorStore::open(&state, wall_ms()).unwrap();
        store.cluster_id().clone()
    };
    let homes = [root.join("peer-a"), root.join("peer-b")];
    let mut a = spawn_worker(&homes[0], &cluster, &key, "remote-a");
    let mut b = spawn_worker(&homes[1], &cluster, &key, "remote-b");
    for (home, worker) in homes.iter().zip([&mut a, &mut b]) {
        let ready = tokio::time::timeout(T, async {
            loop {
                if let Some(status) = worker.child.as_mut().unwrap().try_wait().unwrap() {
                    return Err(status);
                }
                if home.join(".remote-task-runner/agent.sock").exists() {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if !matches!(ready, Ok(Ok(()))) {
            let mut diagnostic = String::new();
            std::fs::File::open(home.join("fixture-worker.stderr"))
                .unwrap()
                .take(8192)
                .read_to_string(&mut diagnostic)
                .unwrap();
            panic!("owned worker startup {ready:?}: {diagnostic}");
        }
    }
    let wrong_home = root.join("wrong-home");
    private(&wrong_home);
    for home in &homes {
        std::fs::write(home.join("offline"), "initial offline peer").unwrap();
    }
    let fake = bin.join("ssh");
    let script = format!("#!/bin/sh\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nhome=$1\nshift\n[ \"$1\" = \"'remote-task-runner' 'worker' '--stdio'\" ] || exit 91\n[ ! -e \"$home/offline\" ] || exit 92\ndd if=/dev/zero bs=4096 count=32 1>&2 2>/dev/null\nprintf '%s\\n' $$ > \"$home/bridge.pid\"\ncase \"$home\" in */wrong-home) home={} ;; esac\nHOME=\"$home\" exec {} worker --stdio\n", remote_task_runner::protocol::ssh::quote(&homes[1].display().to_string()), remote_task_runner::protocol::ssh::quote(BIN));
    std::fs::write(&fake, script).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = format!("schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\nlocal_worker_priority=-100\nmax_active_task=1\n[scheduler]\nprefetch_per_worker=0\n[[workers]]\nid='remote-a'\nssh='{}'\npriority=100\n[[workers]]\nid='remote-b'\nssh='{}'\npriority=100\n[[workers]]\nid='offline'\nssh='{}'\npriority=100\n[[workers]]\nid='wrong-identity'\nssh='{}'\npriority=100\n", homes[0].display(), homes[1].display(), root.join("missing").display(), wrong_home.display());
    write_private(&config_dir.join("config.toml"), config.as_bytes());
    let spec = Spec {
        state_dir: state.clone(),
        worker_state_dir: local,
        worker_id: WorkerId::parse("local").unwrap(),
        cluster_key_path: key.clone(),
        executable: BIN.into(),
        repo_root: None,
        expected_cluster: Some(cluster.clone()),
        managed_service: None,
        env: BTreeMap::from([
            ("HOME".into(), root.display().to_string()),
            ("XDG_CONFIG_HOME".into(), xdg.display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ]),
    };
    let input = root.join("parent.json");
    write_private(&input, &serde_json::to_vec(&spec).unwrap());
    let parent_log = state.join("fixture-parent.stderr");
    let mut owner = Owned::new(
        tokio::process::Command::new(BIN)
            .args(["daemon", "--input"])
            .arg(&input)
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(&parent_log).unwrap()))
            .spawn()
            .unwrap(),
    );
    // The socket file appears at bind and becomes private (and the token
    // complete) a moment later, so wait for a client that loads, as the
    // product bootstrap does, not for the bare path.
    let mut loaded = None;
    wait_for("parent IPC socket", || {
        let exited = owner.child.as_mut().unwrap().try_wait().unwrap();
        assert!(
            exited.is_none(),
            "parent exited {exited:?}: {}",
            std::fs::read_to_string(&parent_log).unwrap()
        );
        loaded = Client::load(&state, cluster.clone()).ok();
        loaded.is_some()
    })
    .await;
    let client = loaded.unwrap();
    // Occupy the local slot first while both remotes are deliberately offline.
    let blocker = submit(&client, &state, &spec.env, &key, root, "blocker", u32::MAX).await;
    if tokio::time::timeout(T, async {
        while !root.join("count-blocker").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_err()
    {
        if let Some(attempt) = attempt(&state, &blocker) {
            eprintln!(
                "initial local state={:?} observation={:?}",
                attempt.state, attempt.observation
            );
            super::bootstrap::diagnose_attempt(&spec, &attempt.attempt_id);
            let worker =
                remote_task_runner::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
                    .unwrap();
            for event in worker.events_of_attempt(&attempt.attempt_id, 40).unwrap() {
                eprintln!("initial local event={}", event.kind);
            }
        } else {
            eprintln!("initial local attempt absent");
        }
        panic!("timed out waiting for initial local execution");
    }
    assert_eq!(
        attempt(&state, &blocker).unwrap().worker_id.as_str(),
        "local"
    );
    for home in &homes {
        std::fs::remove_file(home.join("offline")).unwrap();
    }
    // Both bridge owners are present before accepting the remote sources.
    for home in &homes {
        wait_for("verified remote bridge", || {
            bridge(home, owner.pid).is_some()
        })
        .await;
    }
    let first = submit(&client, &state, &spec.env, &key, root, "first", 12).await;
    wait_for("first remote execution", || {
        root.join("count-first").exists()
    })
    .await;
    let placed = attempt(&state, &first).unwrap();
    let index = match placed.worker_id.as_str() {
        "remote-a" => 0,
        "remote-b" => 1,
        other => panic!("expected remote, got {other}"),
    };
    let second = submit(&client, &state, &spec.env, &key, root, "second", 12).await;
    wait_for("second remote execution", || {
        root.join("count-second").exists()
    })
    .await;
    let second_placement = attempt(&state, &second).unwrap();
    assert_eq!(
        second_placement.worker_id.as_str(),
        if index == 0 { "remote-b" } else { "remote-a" }
    );
    let broken = &homes[index];
    std::fs::write(broken.join("offline"), "fixture-owned disconnect").unwrap();
    let (pid, start) = bridge(broken, owner.pid).unwrap();
    super::common::kill_recorded(pid, &start);
    wait_for("disconnected attempt unknown", || {
        attempt(&state, &first).is_some_and(|a| a.observation == Observation::Unknown)
    })
    .await;
    std::fs::write(root.join("release-local"), "release fixture task").unwrap();
    wait_for("local blocker collection", || {
        attempt(&state, &blocker).is_some_and(|a| a.receipts.collected_at_ms.is_some())
    })
    .await;
    let local = submit(&client, &state, &spec.env, &key, root, "local", 0).await;
    wait_for("local work collection", || {
        attempt(&state, &local).is_some_and(|a| a.receipts.collected_at_ms.is_some())
    })
    .await;
    assert_eq!(attempt(&state, &local).unwrap().worker_id.as_str(), "local");
    assert!(matches!(
        client.request(Operation::Ping).await.unwrap(),
        Reply::Ready { .. }
    ));
    std::fs::remove_file(broken.join("offline")).unwrap();
    for task in [&first, &second] {
        wait_for("remote result collection", || {
            attempt(&state, task).is_some_and(|a| a.receipts.collected_at_ms.is_some())
        })
        .await;
        let store = CoordinatorStore::open_read_only(&state).unwrap();
        let attempts = store.attempts_of_task(task).unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(
            attempts[0].termination.unwrap().outcome(),
            Outcome::Succeeded
        );
    }
    assert_eq!(
        attempt(&state, &first).unwrap().attempt_id,
        placed.attempt_id
    );
    for name in ["blocker", "first", "second", "local"] {
        assert_eq!(
            std::fs::read_to_string(root.join(format!("count-{name}"))).unwrap(),
            "once\n"
        );
    }
    let bridges: Vec<_> = homes.iter().filter_map(|h| bridge(h, owner.pid)).collect();
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.wait().await;
    for (pid, start) in bridges {
        assert_ne!(
            super::common::identity_of(pid).as_deref(),
            Some(start.as_str())
        );
    }
    assert_eq!(
        super::common::identity_of(a.pid).as_deref(),
        Some(a.start.as_str())
    );
    assert_eq!(
        super::common::identity_of(b.pid).as_deref(),
        Some(b.start.as_str())
    );
    a.stop().await;
    b.stop().await;
}
