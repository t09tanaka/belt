//! `belt workers` (the listing): no workers, a connected one, a disconnected
//! one, and after `workers forget`, through the real binary against a real
//! resident and an ephemeral worker behind a fixture-owned fake ssh.
use super::remote_links::{bridge, private, start_parent, wait_for, write_private, Owned};
use belt::ids::*;
use belt::platform::wall_ms;
use belt::storage::CoordinatorStore;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_belt");

fn belt(root: &Path, args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .env_clear()
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("PATH", "/usr/bin:/bin")
        .output()
        .unwrap()
}

fn listing(root: &Path) -> serde_json::Value {
    let out = belt(root, &["workers", "--json"]);
    assert!(out.status.success(), "{out:?}");
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["ok"], true);
    value["result"]["workers"].clone()
}

fn text(root: &Path) -> String {
    let out = belt(root, &["workers"]);
    assert!(out.status.success(), "{out:?}");
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn no_registered_workers_is_said_plainly_in_text_and_json() {
    let fixture = crate::test_tmp::tempdir().unwrap();
    let root = fixture.path();
    private(&root.join("config"));
    private(&root.join("config/belt"));
    write_private(
        &root.join("config/belt/config.toml"),
        b"schema_version = 1\n",
    );
    assert_eq!(text(root), "no registered workers\n");
    assert_eq!(listing(root), serde_json::json!([]));
}

/// One ephemeral worker through its whole life as the listing shows it:
/// registered but not reached, connected, disconnected, and forgotten.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_listing_follows_connected_disconnected_and_forgotten() {
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
    belt::service::bootstrap::write_input(&config.join("coordinator.json"), &spec).unwrap();
    let store = CoordinatorStore::open_read_only(&state).unwrap();

    // Registered, and the resident has not reached it.
    let unreached = listing(root);
    assert_eq!(
        unreached,
        serde_json::json!([{"id":"colab","ssh":home.display().to_string(),
            "ephemeral":true,"connection":"disconnected","identity":null,
            "cpu_cores":null,"memory_bytes":null}])
    );
    assert_eq!(
        text(root),
        format!(
            "colab  {}  ephemeral  disconnected  -  -, -\n",
            home.display()
        )
    );

    // Connected: the identity it admitted is the one shown.
    std::fs::remove_file(home.join("offline")).unwrap();
    wait_for("worker identity admitted", || {
        store.worker_identity(&worker).unwrap().is_some()
    })
    .await;
    let identity = store.worker_identity(&worker).unwrap().unwrap();
    let connected = listing(root);
    // The cores and memory are what the worker reported, if it has yet.
    let mut connected = connected;
    for row in connected.as_array_mut().unwrap() {
        let row = row.as_object_mut().unwrap();
        assert!(row.remove("cpu_cores").is_some() && row.remove("memory_bytes").is_some());
    }
    assert_eq!(
        connected,
        serde_json::json!([{"id":"colab","ssh":home.display().to_string(),
            "ephemeral":true,"connection":"connected","identity":identity}])
    );
    let line = text(root);
    assert!(
        line.starts_with(&format!(
            "colab  {}  ephemeral  connected  {identity}  ",
            home.display()
        )),
        "{line}"
    );

    // Disconnected again: the link is cut and cannot be re-dialled.
    std::fs::write(home.join("offline"), "").unwrap();
    let (pid, start) = bridge(&home, owner.pid).unwrap();
    super::common::kill_recorded(pid, &start);
    wait_for("listing shows disconnected", || {
        listing(root)[0]["connection"] == "disconnected"
    })
    .await;
    assert!(text(root).contains("  ephemeral  disconnected  "));

    // Forget it. `forget` retires the identity; it does not delete the
    // registration (only `group rotate-key --remove` does, spec 24.2), so the
    // worker stays listed, disconnected and with no identity.
    let out = belt(root, &["workers", "forget", "colab"]);
    assert!(out.status.success(), "{out:?}");
    assert!(store
        .worker_identity_retired(&worker, &WorkerBootId::parse(&identity).unwrap())
        .unwrap());
    assert_eq!(
        listing(root),
        serde_json::json!([{"id":"colab","ssh":home.display().to_string(),
            "ephemeral":true,"connection":"disconnected","identity":null,
            "cpu_cores":null,"memory_bytes":null}])
    );
    assert_eq!(
        text(root),
        format!(
            "colab  {}  ephemeral  disconnected  -  -, -\n",
            home.display()
        )
    );

    assert!(matches!(
        client
            .request(belt::service::resident::Operation::Shutdown)
            .await
            .unwrap(),
        belt::service::resident::Reply::Stopping
    ));
    owner.wait().await;
    agent.stop().await;
}
