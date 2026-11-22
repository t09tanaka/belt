//! Real private worker roles, through a fixture-owned fake ssh executable.
//! No host SSH server, service manager, or user configuration is touched.
use belt::coordinator::TaskSpec;
use belt::ids::*;
use belt::platform::wall_ms;
use belt::service::bootstrap::Spec;
use belt::service::resident::{Client, Operation, Reply};
use belt::service::worker::WorkerSpec;
use belt::state::{Observation, Outcome};
use belt::storage::CoordinatorStore;
use belt::wire::CommandSpec;
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
const BIN: &str = env!("CARGO_BIN_EXE_belt");
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
            belt::platform::signal_pid(self.pid, libc::SIGTERM);
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
    let state = home.join(".belt");
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
    let process = belt::platform::read_process(pid);
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
    let source_store = belt::source::SourceStore::open(state).unwrap();
    let vault = belt::secrets::vault::Vault::open(state).unwrap();
    let key = belt::secrets::key::load(key).unwrap();
    let capture = belt::secrets::capture::capture_run(
        env,
        &source_store,
        &vault,
        &belt::secrets::capture::SecretInputs {
            key: &key,
            declared: &[local_ci::source::SecretSpec {
                glob: ".env".into(),
                required: true,
            }],
        },
        &run,
        &belt::source::SourceRequest::Directory { dir },
        &Default::default(),
    )
    .unwrap();
    let manifest = &capture.manifest.manifest_digest;
    let task = TaskId::generate();
    let count =
        belt::protocol::ssh::quote(&root.join(format!("count-{name}")).display().to_string());
    let wait = if delay == u32::MAX {
        let release =
            belt::protocol::ssh::quote(&root.join(format!("release-{name}")).display().to_string());
        let fixture = belt::protocol::ssh::quote(&root.display().to_string());
        format!("while [ -d {fixture} ] && [ ! -e {release} ]; do sleep 0.05; done")
    } else {
        crate::fixture_process::script(&format!("sleep {delay}"))
    };
    let spec = TaskSpec {
        ci: None,
        toolchains: None,
        in_place: false, outputs: Vec::new(), requirements: Default::default(),
        local: false,
        setup: None, plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(manifest).or_else(|_| Digest::parse(&format!("sha256:{manifest}"))).unwrap(),
        commands: vec![CommandSpec { node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), format!("test \"$(cat .env)\" = \"TOKEN=$(cat ordinary)-private\" && echo once >> {count} && {wait}")],
            cwd: String::new(), env: BTreeMap::new(),
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
fn attempt(state: &Path, task: &TaskId) -> Option<belt::storage::AttemptRecord> {
    CoordinatorStore::open_read_only(state)
        .unwrap()
        .latest_attempt_of_task(task)
        .unwrap()
}

async fn start_parent(
    root: &Path,
    state: &Path,
    key: &Path,
    cluster: &ClusterId,
) -> (Owned, Client, Spec) {
    let spec = Spec {
        state_dir: state.to_owned(),
        worker_state_dir: root.join("local"),
        worker_id: WorkerId::parse("local").unwrap(),
        cluster_key_path: key.to_owned(),
        executable: BIN.into(),
        repo_root: None,
        expected_cluster: Some(cluster.clone()),
        managed_service: None,
        env: BTreeMap::from([
            ("HOME".into(), root.display().to_string()),
            (
                "XDG_CONFIG_HOME".into(),
                root.join("config").display().to_string(),
            ),
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
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", root.join("bin").display()),
            )
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
        loaded = Client::load(state, cluster.clone()).ok();
        loaded.is_some()
    })
    .await;
    let client = loaded.unwrap();
    (owner, client, spec)
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
    let config_dir = xdg.join("belt");
    let bin = root.join("bin");
    for dir in [&state, &local, &config_dir, &bin] {
        private(dir);
    }
    let key = root.join("key");
    belt::secrets::key::init(&key).unwrap();
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
                if home.join(".belt/agent.sock").exists() {
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
    let script = format!("#!/bin/sh\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nhome=$1\nshift\n[ \"$1\" = \"'belt' 'worker' '--stdio'\" ] || exit 91\n[ ! -e \"$home/offline\" ] || exit 92\ndd if=/dev/zero bs=4096 count=32 1>&2 2>/dev/null\nprintf '%s\\n' $$ > \"$home/bridge.pid\"\ncase \"$home\" in */wrong-home) home={} ;; esac\nHOME=\"$home\" exec {} worker --stdio\n", belt::protocol::ssh::quote(&homes[1].display().to_string()), belt::protocol::ssh::quote(BIN));
    std::fs::write(&fake, script).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = format!("schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\nlocal_worker_priority=-100\nmax_active_task=1\n[scheduler]\nprefetch_per_worker=0\n[[workers]]\nid='remote-a'\nssh='{}'\npriority=100\n[[workers]]\nid='remote-b'\nssh='{}'\npriority=100\n[[workers]]\nid='offline'\nssh='{}'\npriority=100\n[[workers]]\nid='wrong-identity'\nssh='{}'\npriority=100\n", homes[0].display(), homes[1].display(), root.join("missing").display(), wrong_home.display());
    write_private(&config_dir.join("config.toml"), config.as_bytes());
    let (owner, client, spec) = start_parent(root, &state, &key, &cluster).await;
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
                belt::storage::WorkerStore::open_read_only(&spec.worker_state_dir).unwrap();
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
    std::fs::write(root.join("release-blocker"), "release fixture task").unwrap();
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

/// A changed registration redials the worker (`ReloadWorkers`) while it holds
/// a live attempt: the new session adopts the same attempt, the command is
/// started once, and no second attempt is placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reload_with_a_live_attempt_readopts_it_and_starts_the_command_once() {
    let root = crate::test_tmp::tempdir().unwrap();
    let root = root.path();
    private(root);
    let state = root.join("parent");
    let config_dir = root.join("config").join("belt");
    let bin = root.join("bin");
    for dir in [&state, &root.join("local"), &config_dir, &bin] {
        private(dir);
    }
    let key = root.join("key");
    belt::secrets::key::init(&key).unwrap();
    let cluster = CoordinatorStore::open(&state, wall_ms())
        .unwrap()
        .cluster_id()
        .clone();
    let home = root.join("peer-a");
    let mut worker = spawn_worker(&home, &cluster, &key, "remote-a");
    wait_for("remote worker socket", || {
        assert!(worker.child.as_mut().unwrap().try_wait().unwrap().is_none());
        home.join(".belt/agent.sock").exists()
    })
    .await;
    let fake = bin.join("ssh");
    let script = format!("#!/bin/sh\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nhome=$1\nshift\n[ \"$1\" = \"'belt' 'worker' '--stdio'\" ] || exit 91\n[ ! -e \"$home/offline\" ] || exit 92\nprintf '%s\\n' $$ > \"$home/bridge.pid\"\nHOME=\"$home\" exec {} worker --stdio\n", belt::protocol::ssh::quote(BIN));
    std::fs::write(&fake, script).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).unwrap();
    let registration = |priority: u32| {
        format!("schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\nlocal_worker_priority=-100\nmax_active_task=1\n[scheduler]\nprefetch_per_worker=0\n[[workers]]\nid='remote-a'\nssh='{}'\npriority={priority}\n", home.display())
    };
    write_private(
        &config_dir.join("config.toml"),
        registration(100).as_bytes(),
    );
    let (owner, client, spec) = start_parent(root, &state, &key, &cluster).await;
    wait_for("verified remote bridge", || {
        bridge(&home, owner.pid).is_some()
    })
    .await;
    let first_bridge = bridge(&home, owner.pid).unwrap();
    let task = submit(&client, &state, &spec.env, &key, root, "first", u32::MAX).await;
    wait_for("remote execution", || root.join("count-first").exists()).await;
    let placed = attempt(&state, &task).unwrap();
    assert_eq!(placed.worker_id.as_str(), "remote-a");
    // Change the registration of the worker that runs the attempt.
    write_private(&config_dir.join("config.toml"), registration(90).as_bytes());
    match client.request(Operation::ReloadWorkers).await.unwrap() {
        Reply::WorkersReloaded { connecting } => assert_eq!(connecting, ["remote-a"]),
        other => panic!("{other:?}"),
    }
    wait_for("redialed bridge", || {
        bridge(&home, owner.pid).is_some_and(|b| b != first_bridge)
    })
    .await;
    std::fs::write(root.join("release-first"), "release").unwrap();
    std::fs::write(root.join("release-blocker"), "release").unwrap();
    wait_for("result collection", || {
        attempt(&state, &task).is_some_and(|a| a.receipts.collected_at_ms.is_some())
    })
    .await;
    let attempts = CoordinatorStore::open_read_only(&state)
        .unwrap()
        .attempts_of_task(&task)
        .unwrap();
    assert_eq!(attempts.len(), 1, "no second attempt is placed");
    assert_eq!(attempts[0].attempt_id, placed.attempt_id);
    assert_eq!(
        attempts[0].termination.unwrap().outcome(),
        Outcome::Succeeded
    );
    assert_eq!(
        std::fs::read_to_string(root.join("count-first")).unwrap(),
        "once\n"
    );
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.wait().await;
    worker.stop().await;
}

/// A46 across the real daemon, worker executable and stdio bridge. The SSH
/// executable stands in for the user's tunnel and can disconnect it without
/// stopping the worker it leads to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a46_a_tunnel_disconnect_keeps_the_ephemeral_attempt_until_explicit_forget() {
    let fixture = crate::test_tmp::tempdir().unwrap();
    let root = fixture.path();
    let state = root.join("parent");
    let home = root.join("ephemeral");
    let bin = root.join("bin");
    let config = root.join("config/belt");
    for dir in [root, &state, &root.join("local"), &home, &bin, &config] {
        private(dir);
    }
    let key = root.join("key");
    belt::secrets::key::init(&key).unwrap();
    let cluster = CoordinatorStore::open(&state, wall_ms())
        .unwrap()
        .cluster_id()
        .clone();
    let worker = WorkerId::parse("colab").unwrap();
    write_private(&config.join("config.toml"), format!(
        "schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\nmax_active_task=1\n[scheduler]\nprefetch_per_worker=0\n[[workers]]\nid='colab'\nssh='{}'\nephemeral=true\n",
        home.display()
    ).as_bytes());
    let ssh = bin.join("ssh");
    std::fs::write(
        &ssh,
        format!(
            r#"#!/bin/sh
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do shift; done
[ "$#" = 3 ] || exit 91
home=$2
[ "$3" = "'belt' 'worker' '--stdio'" ] || exit 91
[ ! -e "$home/offline" ] || exit 92
echo $$ > "$home/bridge.pid"
echo $$ >> "$home/bridge-dials"
HOME="$home" exec {} worker --stdio
"#,
            belt::protocol::ssh::quote(BIN)
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(home.join("offline"), "").unwrap();
    let agent = Owned::new(
        tokio::process::Command::new(BIN)
            .args(["worker", "--ephemeral", "--cluster"])
            .arg(cluster.to_string())
            .args(["--name", worker.as_str(), "--key-file"])
            .arg(&key)
            .env("HOME", &home)
            .env_remove("XDG_CONFIG_HOME")
            .env_remove("XDG_STATE_HOME")
            .env("PATH", "/bin:/usr/bin")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let (owner, client, spec) = start_parent(root, &state, &key, &cluster).await;
    let blocker = submit(&client, &state, &spec.env, &key, root, "blocker", u32::MAX).await;
    wait_for("local slot occupied", || {
        root.join("count-blocker").exists()
    })
    .await;
    std::fs::remove_file(home.join("offline")).unwrap();
    let store = CoordinatorStore::open_read_only(&state).unwrap();
    wait_for("ephemeral identity admitted", || {
        store.worker_identity(&worker).unwrap().is_some()
    })
    .await;
    let identity = WorkerBootId::parse(&store.worker_identity(&worker).unwrap().unwrap()).unwrap();
    let task = submit(
        &client,
        &state,
        &spec.env,
        &key,
        root,
        "ephemeral",
        u32::MAX,
    )
    .await;
    if tokio::time::timeout(T, async {
        while !root.join("count-ephemeral").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_err()
    {
        let attempts = store.attempts_of_task(&task).unwrap();
        let worker_events = belt::storage::WorkerStore::open_read_only(&home.join(".belt"))
            .and_then(|worker| worker.events_after(0, 200))
            .map(|events| events.into_iter().map(|e| e.kind).collect::<Vec<_>>());
        let refused: Vec<_> = attempts
            .iter()
            .flat_map(|attempt| {
                store
                    .events_of(&attempt.attempt_id, "submit_refused")
                    .unwrap()
            })
            .map(|event| event.payload)
            .collect();
        panic!("ephemeral command did not start; placements and states: {:?}; worker events: {worker_events:?}; submit refusals: {refused:?}; bridge dials: {:?}; parent log: {}",
            attempts.iter().map(|a| (&a.worker_id, a.state, a.observation, &a.termination)).collect::<Vec<_>>(),
            std::fs::read_to_string(home.join("bridge-dials")).ok(),
            std::fs::read_to_string(state.join("fixture-parent.stderr")).unwrap_or_default());
    }
    let placed = attempt(&state, &task).unwrap();
    assert_eq!(placed.worker_id, worker);
    std::fs::write(home.join("offline"), "").unwrap();
    let (pid, start) = bridge(&home, owner.pid).unwrap();
    super::common::kill_recorded(pid, &start);
    wait_for("unknown attempt", || {
        attempt(&state, &task).is_some_and(|a| a.observation == Observation::Unknown)
    })
    .await;
    std::fs::write(root.join("release-blocker"), "").unwrap();
    wait_for("available local slot", || {
        attempt(&state, &blocker).is_some_and(|a| a.receipts.collected_at_ms.is_some())
    })
    .await;
    let attempts = store.attempts_of_task(&task).unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].observation, Observation::Unknown);
    assert!(attempts[0].termination.is_none());
    let reply = client
        .request(Operation::ForgetWorker {
            worker: worker.clone(),
        })
        .await
        .unwrap();
    assert!(
        matches!(reply, Reply::WorkerForgotten { identity_retired: true, ended } if ended == vec![placed.attempt_id])
    );
    assert!(store.worker_identity_retired(&worker, &identity).unwrap());
    assert_eq!(
        attempt(&state, &task)
            .unwrap()
            .termination
            .unwrap()
            .outcome(),
        Outcome::Interrupted
    );
    // The worker is alive and still runs the command when it returns: the
    // parent does not attach it, tells it that the identity is retired and
    // closes the connection. The dial loop starts the next bridge only after
    // it has reaped the previous one, so two new dials prove that the
    // returning worker's first connection was closed.
    assert_eq!(
        super::common::identity_of(agent.pid).as_deref(),
        Some(agent.start.as_str())
    );
    assert!(home.join(".belt").exists());
    let previous_dials = std::fs::read_to_string(home.join("bridge-dials"))
        .unwrap()
        .lines()
        .count();
    std::fs::remove_file(home.join("offline")).unwrap();
    wait_for("returning worker connection closed and redialed", || {
        std::fs::read_to_string(home.join("bridge-dials"))
            .unwrap()
            .lines()
            .count()
            >= previous_dials + 2
    })
    .await;
    assert_eq!(store.worker_identity(&worker).unwrap(), None);
    assert!(store
        .identity_retired_notice(&worker, &identity)
        .unwrap()
        .is_some());
    // Told so, the worker stops the command without waiting for its lease
    // (nobody releases it here), and with nothing left running it ends by
    // itself and takes its state with it (spec 24.3).
    agent.wait().await;
    assert!(
        !home.join(".belt").exists(),
        "the ephemeral worker left its state behind"
    );
    assert!(!root.join("release-ephemeral").exists());
    assert_eq!(store.attempts_of_task(&task).unwrap().len(), 1);
    assert_eq!(
        std::fs::read_to_string(root.join("count-ephemeral")).unwrap(),
        "once\n"
    );
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.wait().await;
}

/// A38 from the public CLI through a resident, its real local worker, a
/// separately started worker over the stdio bridge, and real supervisors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a38_public_ci_delivers_needs_outputs_and_a_verified_multifile_artifact_to_another_worker()
{
    use belt::worker::control::{self, Control};
    let (root, spec, input) = super::bootstrap::control_fixture();
    let cluster = CoordinatorStore::open(&spec.state_dir, wall_ms())
        .unwrap()
        .cluster_id()
        .clone();
    let home = root.path().join("peer");
    let remote = spawn_worker(&home, &cluster, &spec.cluster_key_path, "peer");
    let remote_state = home.join(".belt");
    wait_for("private peer socket", || {
        remote_state.join("agent.sock").exists()
    })
    .await;
    control::write(&remote_state, Control::draining()).unwrap();
    let config = root.path().join(".config/belt/config.toml");
    let text="schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\n[[workers]]\nid='peer'\nssh='peer'\n";
    write_private(&config, text.as_bytes());
    let quote = belt::protocol::ssh::quote;
    let ssh=format!("#!/bin/sh\nwhile [ \"$#\" -gt 0 ] && [ \"$1\" != -- ]; do shift; done\n[ \"$#\" -eq 3 ] && [ \"$2\" = peer ] && [ \"$3\" = \"'belt' 'worker' '--stdio'\" ] || exit 91\nHOME={} exec {} worker --stdio\n",quote(home.to_str().unwrap()),quote(BIN));
    super::common::write_executable(&root.path().join("tool-stubs/ssh"), &ssh);
    let marks = super::bootstrap::ci_project(
        root.path(),
        &input,
        r#"  build:
    runs-on: ubuntu-latest
    outputs:
      id: ${{ steps.make.outputs.id }}
    steps:
      - id: make
        run: |
          mkdir -p out/sub
          echo abc > out/first
          echo def > out/sub/second
          echo id=release-417 >> "$GITHUB_OUTPUT"
      - uses: actions/upload-artifact@v4
        with:
          name: package
          path: out/
          if-no-files-found: error
      - run: |
          echo sent >> MARKS/build
          i=0
          while [ ! -f MARKS/resume ] && [ "$i" -lt 900 ]; do sleep 0.1; i=$((i+1)); done
          test -f MARKS/resume
  verify:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: package
          path: release
      - run: |
          test "${{ needs.build.outputs.id }}" = release-417
          test "$(cat release/first)" = abc
          test "$(cat release/sub/second)" = def
          echo verified >> MARKS/verify
"#,
    );
    let child = super::bootstrap::ci_command(&spec, &input)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for("producer passing the acknowledged upload step", || {
        marks.join("build").exists()
    })
    .await;
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let run = store
        .query_runs(8, None)
        .unwrap()
        .items
        .into_iter()
        .next()
        .unwrap()
        .run_id;
    let rows = store.ci_run_tasks(&run).unwrap();
    let workflow = rows[0].spec["ci"]["task"]["artifact_workflow"]
        .as_str()
        .unwrap();
    let receipts = store.workflow_artifacts(&run, workflow).unwrap();
    assert_eq!(receipts.len(), 1);
    let published = &receipts[0];
    assert_eq!(published.receipt.name, "package");
    assert_eq!(published.receipt.files.len(), 2);
    let latest = store.get_attempt(&published.producer).unwrap().unwrap();
    assert_eq!(latest.worker_id, spec.worker_id);
    assert!(latest.termination.is_none());
    control::write(&spec.worker_state_dir, Control::draining()).unwrap();
    control::write(&remote_state, Control::default()).unwrap();
    std::fs::write(marks.join("resume"), "ok").unwrap();
    let output = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["result"]["state"], "settled");
    assert_eq!(
        std::fs::read_to_string(marks.join("build")).unwrap(),
        "sent\n"
    );
    assert_eq!(
        std::fs::read_to_string(marks.join("verify")).unwrap(),
        "verified\n"
    );
    let rows = store.ci_run_tasks(&run).unwrap();
    assert_eq!(rows.len(), 2);
    let mut workers = std::collections::BTreeSet::new();
    for row in rows {
        let attempts = store.attempts_of_task(&row.task_id).unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(
            attempts[0].termination.unwrap().outcome(),
            Outcome::Succeeded
        );
        workers.insert(attempts[0].worker_id.to_string());
    }
    assert_eq!(
        workers,
        [spec.worker_id.to_string(), "peer".into()]
            .into_iter()
            .collect()
    );
    let client = Client::load(&spec.state_dir, cluster).unwrap();
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    remote.stop().await;
}

/// A member that switches to the next group key closes the links it made
/// itself: they were proven with the old key and are proven again with the
/// new one (spec 24.2). The peer here has not switched (it holds both keys
/// and would keep the old connection), so it is the connecting side that
/// closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_that_switches_the_key_closes_its_own_links_and_proves_again() {
    let fixture = crate::test_tmp::tempdir().unwrap();
    let root = fixture.path();
    let state = root.join("parent");
    let home = root.join("peer");
    let bin = root.join("bin");
    let config = root.join("config/belt");
    for dir in [root, &state, &root.join("local"), &home, &bin, &config] {
        private(dir);
    }
    // Each member has its own copy of the group key.
    let key = root.join("key");
    belt::secrets::key::init(&key).unwrap();
    let peer_key = home.join("key");
    let first_key = std::fs::read(&key).unwrap();
    write_private(&peer_key, &first_key);
    let cluster = CoordinatorStore::open(&state, wall_ms())
        .unwrap()
        .cluster_id()
        .clone();
    let mut peer = spawn_worker(&home, &cluster, &peer_key, "remote-a");
    wait_for("the peer's socket", || {
        assert!(peer.child.as_mut().unwrap().try_wait().unwrap().is_none());
        home.join(".belt/agent.sock").exists()
    })
    .await;
    write_private(
        &config.join("config.toml"),
        format!(
            "schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\n[[workers]]\nid='remote-a'\nssh='{}'\n",
            home.display()
        )
        .as_bytes(),
    );
    let ssh = bin.join("ssh");
    std::fs::write(
        &ssh,
        format!(
            r#"#!/bin/sh
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do shift; done
[ "$#" = 3 ] || exit 91
home=$2
[ "$3" = "'belt' 'worker' '--stdio'" ] || exit 91
echo $$ > "$home/bridge.pid"
echo $$ >> "$home/bridge-dials"
HOME="$home" exec {} worker --stdio
"#,
            belt::protocol::ssh::quote(BIN)
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (owner, client, _spec) = start_parent(root, &state, &key, &cluster).await;
    let dials = || {
        std::fs::read_to_string(home.join("bridge-dials"))
            .map(|t| t.lines().count())
            .unwrap_or(0)
    };
    // The peer records a member once its proof held.
    let proven = || {
        belt::storage::WorkerStore::open_read_only(&home.join(".belt"))
            .and_then(|s| s.members())
            .is_ok_and(|m| !m.is_empty())
    };
    wait_for("the link to the peer to be proven", || {
        proven() && bridge(&home, owner.pid).is_some()
    })
    .await;
    let (first, _) = bridge(&home, owner.pid).unwrap();
    let before = dials();

    // The first stage of a rotation reaches both members.
    let second = root.join("second-key");
    belt::secrets::key::init(&second).unwrap();
    let next_key = std::fs::read(&second).unwrap();
    assert_ne!(next_key, first_key);
    write_private(&belt::group::next_key_path(&peer_key), &next_key);
    write_private(&belt::group::next_key_path(&key), &next_key);
    // The second stage reaches only this member.
    assert!(belt::group::switch_to_next(&key).unwrap());
    wait_for("the link to be closed and made again", || {
        dials() > before && bridge(&home, owner.pid).is_some_and(|(pid, _)| pid != first)
    })
    .await;
    // The peer did not switch and did not restart: it was this member that
    // closed its link. The peer takes the new proof under its next key.
    assert_eq!(std::fs::read(&peer_key).unwrap(), first_key);
    assert_eq!(
        std::fs::read(belt::group::next_key_path(&peer_key)).unwrap(),
        next_key
    );
    assert_eq!(std::fs::read(&key).unwrap(), next_key);
    assert_eq!(
        super::common::identity_of(peer.pid).as_deref(),
        Some(peer.start.as_str())
    );
    // The new link holds: the same bridge is still there after the checks
    // of the following seconds, and nothing was dialed in between.
    let (again, start) = bridge(&home, owner.pid).unwrap();
    let settled = dials();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(bridge(&home, owner.pid), Some((again, start)));
    assert_eq!(dials(), settled);
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.wait().await;
    peer.stop().await;
}

/// The init resident has both an in-process worker link and an agent listener.
/// Only a peer's old-key link is retired when its key switches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_key_switch_closes_peer_link_but_keeps_local_worker_and_resident_alive() {
    let fixture = crate::test_tmp::tempdir().unwrap();
    let root = fixture.path();
    let state = root.join("parent");
    for dir in [
        root.to_owned(),
        state.clone(),
        root.join("local"),
        root.join("config/belt"),
    ] {
        private(&dir);
    }
    write_private(
        &root.join("config/belt/config.toml"),
        b"schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\n",
    );
    let key = root.join("key");
    belt::secrets::key::init(&key).unwrap();
    let cluster = CoordinatorStore::open(&state, wall_ms())
        .unwrap()
        .cluster_id()
        .clone();
    let (mut owner, client, spec) = start_parent(root, &state, &key, &cluster).await;
    let before = submit(&client, &state, &spec.env, &key, root, "before-switch", 0).await;
    wait_for("secret task before switch", || {
        attempt(&state, &before).is_some_and(|a| {
            a.termination
                .is_some_and(|t| t.outcome() == Outcome::Succeeded)
        })
    })
    .await;
    let peer_id = CoordinatorId::generate();
    let cfg = belt::protocol::ClientConfig::new(
        cluster.clone(),
        peer_id.clone(),
        CoordinatorEpoch(1),
        belt::protocol::Membership::load(WorkerId::parse("peer").unwrap(), &key).unwrap(),
    );
    let io = belt::protocol::transport::connect_unix(&spec.worker_state_dir.join("agent.sock"))
        .await
        .unwrap();
    let peer = belt::protocol::connect(io, cfg).await.unwrap();
    peer.client.ping(T).await.unwrap();
    assert!(
        belt::storage::WorkerStore::open_read_only(&spec.worker_state_dir)
            .unwrap()
            .members()
            .unwrap()
            .contains(&("peer".into(), peer_id.to_string()))
    );
    // This request traverses the resident's in-process link to its own agent.
    let report = || {
        client.request(Operation::EnvReport {
            worker: spec.worker_id.clone(),
        })
    };
    assert!(matches!(report().await.unwrap(), Reply::EnvReport { .. }));
    belt::secrets::key::init(&belt::group::next_key_path(&key)).unwrap();
    assert!(belt::group::switch_to_next(&key).unwrap());
    wait_for("old-key peer link to close", || peer.client.is_closed()).await;
    assert!(owner.child.as_mut().unwrap().try_wait().unwrap().is_none());
    assert!(matches!(report().await.unwrap(), Reply::EnvReport { .. }));
    let after = submit(&client, &state, &spec.env, &key, root, "after-switch", 0).await;
    wait_for("secret task after switch", || {
        attempt(&state, &after).is_some_and(|a| {
            a.termination
                .is_some_and(|t| t.outcome() == Outcome::Succeeded)
        })
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(root.join("count-after-switch")).unwrap(),
        "once\n"
    );
    assert_eq!(
        super::common::identity_of(owner.pid).as_deref(),
        Some(owner.start.as_str())
    );
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.wait().await;
}

// SendKey regenerates the worker identity even when the key bytes stay the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_send_key_reconnects_local_worker_without_restarting_resident() {
    let fixture = crate::test_tmp::tempdir().unwrap();
    let root = fixture.path();
    let state = root.join("parent");
    for dir in [
        root.to_owned(),
        state.clone(),
        root.join("local"),
        root.join("config/belt"),
    ] {
        private(&dir);
    }
    write_private(
        &root.join("config/belt/config.toml"),
        b"schema_version=1\n[coordinator]\nheartbeat_seconds=1\nstale_seconds=3\n",
    );
    let key = root.join("key");
    belt::secrets::key::init(&key).unwrap();
    let cluster = CoordinatorStore::open(&state, wall_ms())
        .unwrap()
        .cluster_id()
        .clone();
    let (mut owner, client, spec) = start_parent(root, &state, &key, &cluster).await;

    let identity = || {
        CoordinatorStore::open_read_only(&state)
            .unwrap()
            .worker_identity(&spec.worker_id)
            .unwrap()
            .unwrap()
    };
    assert!(matches!(
        client
            .request(Operation::EnvReport {
                worker: spec.worker_id.clone(),
            })
            .await
            .unwrap(),
        Reply::EnvReport { .. }
    ));

    let before = identity();

    // The same fixture-owned helper input used by worker_agent::group_membership.
    let home = root.join("helper-home");
    private(&home);
    let env = BTreeMap::from([("HOME".into(), home.display().to_string())]);
    let paths = belt::service::worker_service::Paths::resolve(&env).unwrap();
    private(paths.input.parent().unwrap());
    let worker = WorkerSpec {
        schema: 1,
        expected_cluster: cluster.clone(),
        worker_id: spec.worker_id.clone(),
        state_dir: spec.worker_state_dir.clone(),
        cluster_key_path: key.clone(),
        executable: BIN.into(),
        env,
        config: "schema_version=1\n".into(),
    };
    write_private(&paths.input, &serde_json::to_vec(&worker).unwrap());
    // Repeat to ensure the replacement connection can itself be replaced.
    let mut previous = before;
    for round in 0..2 {
        let old_name = format!("before-send-key-{round}");
        let old_task = submit(&client, &state, &spec.env, &key, root, &old_name, u32::MAX).await;
        wait_for("old identity command running", || {
            root.join(format!("count-{old_name}")).exists()
        })
        .await;
        let old_attempt = attempt(&state, &old_task).unwrap();
        let request = serde_json::json!({
            "cluster": cluster, "member": spec.worker_id,
            "action": {"action": "SendKey",
                       "key": belt::secrets::key::load(&key).unwrap().as_bytes()}
        });
        let mut helper = tokio::process::Command::new(BIN)
            .arg("group-rotate-helper")
            .env_clear()
            .env("HOME", &home)
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        {
            use tokio::io::AsyncWriteExt;
            helper
                .stdin
                .take()
                .unwrap()
                .write_all(&serde_json::to_vec(&request).unwrap())
                .await
                .unwrap();
        }
        let output = tokio::time::timeout(T, helper.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "helper: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let task = submit(
            &client,
            &state,
            &spec.env,
            &key,
            root,
            &format!("after-send-key-{round}"),
            0,
        )
        .await;
        wait_for("new local identity registered", || {
            assert!(owner.child.as_mut().unwrap().try_wait().unwrap().is_none());
            identity() != previous
        })
        .await;
        previous = identity();
        let Reply::WorkersListed { observations } = client
            .request(Operation::ListWorkers {
                names: vec![spec.worker_id.clone()],
            })
            .await
            .unwrap()
        else {
            panic!("expected worker observations");
        };
        assert_eq!(
            observations.get(&spec.worker_id),
            Some(&(true, Some(previous.clone())))
        );
        assert!(matches!(
            client
                .request(Operation::EnvReport {
                    worker: spec.worker_id.clone(),
                })
                .await
                .unwrap(),
            Reply::EnvReport { .. }
        ));
        wait_for("local source and secret delivery after send-key", || {
            attempt(&state, &task).is_some_and(|a| {
                a.termination
                    .is_some_and(|t| t.outcome() == Outcome::Succeeded)
            })
        })
        .await;
        let store = CoordinatorStore::open_read_only(&state).unwrap();
        let old_attempts = store.attempts_of_task(&old_task).unwrap();
        assert_eq!(old_attempts.len(), 1);
        assert_eq!(old_attempts[0].attempt_id, old_attempt.attempt_id);
        assert_eq!(
            old_attempts[0].termination.as_ref().unwrap().outcome(),
            Outcome::Interrupted
        );
        assert_eq!(
            std::fs::read_to_string(root.join(format!("count-{old_name}"))).unwrap(),
            "once\n"
        );
        let new_attempts = store.attempts_of_task(&task).unwrap();
        assert_eq!(new_attempts.len(), 1);
        assert!(new_attempts[0].collection_failed_at_ms.is_none());
        assert_eq!(
            new_attempts[0].termination.as_ref().unwrap().outcome(),
            Outcome::Succeeded
        );
        assert!(owner.child.as_mut().unwrap().try_wait().unwrap().is_none());
    }
    assert_eq!(
        super::common::identity_of(owner.pid).as_deref(),
        Some(owner.start.as_str())
    );
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    owner.wait().await;
}
