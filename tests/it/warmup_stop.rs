//! A warm-up nobody asks about for `warmup_idle_seconds` lets go of its
//! install, and the last reference stops the install: the manager and what it
//! started are gone within a bound, and a later sync installs again, once.

use belt::ids::RequestId;
use belt::service::bootstrap::{ensure, Spec};
use belt::service::resident::{Client, Operation, Reply};
use belt::wire::{SyncOutcome, ToolRequirement};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// A local worker whose registry may install node 20.0.0 with a manager that
/// logs each run to `calls`, records its pid and its child's pid in `pids`,
/// and waits while `hold` exists.
fn fixture() -> (crate::common::DaemonRoot, Spec, PathBuf) {
    use belt::toolchain::profile::Profile;
    let (root, spec, _input) = crate::bootstrap::control_fixture();
    std::fs::write(
        root.path().join(".config/belt/config.toml"),
        "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\nwarmup_idle_seconds = 1\n",
    )
    .unwrap();
    let host = root.path().join("private-managers");
    let stubs = root.path().join("tool-stubs");
    std::fs::create_dir(&host).unwrap();
    crate::common::write_executable(&host.join("node"), "#!/bin/sh\nprintf 'v18.0.0\\n'\n");
    let traffic_config = belt::config::traffic::resolve(&spec.env)
        .unwrap()
        .config_file;
    let traffic = traffic_config.parent().unwrap();
    std::fs::create_dir_all(traffic).unwrap();
    std::fs::set_permissions(traffic, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        &traffic_config,
        "budget = 8\ncpu_threshold_percent = 100.0\nmemory_reserve_mib = 0\n",
    )
    .unwrap();
    let profile_text = format!("schema_version=1\nactive='base18'\n[profiles.base18]\npath=[{:?},{stubs:?},'/usr/bin','/bin']\nshell='/bin/sh'\ncache_namespace='base18'\nrequired_tools=['node']\nallowed_env=['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n", host.to_str().unwrap());
    let profile_file = spec.worker_state_dir.join("profiles.toml");
    std::fs::write(&profile_file, &profile_text).unwrap();
    std::fs::set_permissions(&profile_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let profile = Profile::load(&spec.worker_state_dir, &spec.env).unwrap();
    let manager = format!(
        r#"#!/bin/sh
set -eu
printf '%s\n' "$1" >> '{host}/calls'
if [ -e '{host}/hold' ]; then
  /bin/sleep 300 &
  printf '%s %s\n' "$$" "$!" >> '{host}/pids'
  while [ -e '{host}/hold' ]; do /bin/sleep 0.05; done
fi
version="$1"
bin="$NODENV_ROOT/versions/$version/bin"
/bin/mkdir -p "$bin"
/usr/bin/printf '#!/bin/sh\necho v%s\n' "$version" > "$bin/node"
/bin/chmod 700 "$bin/node"
"#,
        host = host.display()
    );
    crate::common::write_executable(&host.join("manager"), &manager);
    crate::common::write_executable(&host.join("node-build"), &manager);
    let registry = format!("schema_version=1\nactive_profile='base18'\nprofile_definition={:?}\n[[recipes]]\ntool='node'\nkind='Nodenv'\nmanager={:?}\ndependencies=[{:?}]\nsearch_path=[{:?}{}]\ninventory=['20.0.0','22.0.0']\n", profile.definition_digest().as_str(), host.join("manager").to_str().unwrap(), host.join("node-build").to_str().unwrap(), host.to_str().unwrap(), crate::common::system_search_path_toml());
    let registry_file = spec.worker_state_dir.join("toolchains.toml");
    std::fs::write(&registry_file, registry).unwrap();
    std::fs::set_permissions(&registry_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    (root, spec, host)
}

fn calls(host: &Path) -> usize {
    std::fs::read_to_string(host.join("calls"))
        .unwrap_or_default()
        .lines()
        .filter(|l| *l == "20.0.0")
        .count()
}

/// The pids the manager recorded (its own and its child's).
fn recorded(host: &Path) -> Vec<u32> {
    std::fs::read_to_string(host.join("pids"))
        .unwrap_or_default()
        .split_whitespace()
        .filter_map(|p| p.parse().ok())
        .collect()
}

/// Whether `pid` is a live process (read through `ps`: nothing is signalled).
fn alive(pid: u32) -> bool {
    let out = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let stat = String::from_utf8_lossy(&out.stdout);
    let stat = stat.trim();
    !stat.is_empty() && !stat.starts_with('Z')
}

async fn warm(client: &Client, spec: &Spec, sync: &RequestId) -> SyncOutcome {
    let Reply::EnvSync { results } = client
        .request(Operation::EnvSync {
            worker: spec.worker_id.clone(),
            sync_id: sync.clone(),
            tools: vec![ToolRequirement {
                name: "node".into(),
                version: "20.0.0".into(),
            }],
        })
        .await
        .unwrap()
    else {
        panic!("the resident must answer the warm-up")
    };
    results[0].outcome
}

async fn until(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_idle_release_stops_the_install_and_a_later_sync_installs_once() {
    let (_root, spec, host) = fixture();
    let client = ensure(spec.clone()).await.unwrap();
    std::fs::write(host.join("hold"), "").unwrap();
    let first = RequestId::generate();
    // The coordinator keeps asking while it waits, and each ask restarts the
    // idle time of the warm-up: the install may take longer than that time
    // to reach the manager (a slow host) without being let go meanwhile. Only
    // once the manager runs does nobody ask any more.
    let end = Instant::now() + Duration::from_secs(20);
    while recorded(&host).len() != 2 {
        assert_eq!(warm(&client, &spec, &first).await, SyncOutcome::Pending);
        assert!(
            Instant::now() < end,
            "timed out waiting for the manager to run; calls {}, pids {:?}",
            calls(&host),
            recorded(&host)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let pids = recorded(&host);
    // Nobody asks again: the warm-up is let go after one second, and the
    // install it alone waited for is stopped, descendants included.
    until("the manager and its child to be gone", 15, || {
        pids.iter().all(|p| !alive(*p))
    })
    .await;
    // A later sync (hold gone) installs again, exactly once more.
    std::fs::remove_file(host.join("hold")).unwrap();
    let second = RequestId::generate();
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        match warm(&client, &spec, &second).await {
            SyncOutcome::Pending => {
                assert!(Instant::now() < end, "the second install did not end");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            outcome => {
                assert_eq!(outcome, SyncOutcome::Installed);
                break;
            }
        }
    }
    assert_eq!(calls(&host), 2, "one stopped run, one that installed");
    assert_eq!(
        warm(&client, &spec, &RequestId::generate()).await,
        SyncOutcome::Unchanged
    );
    assert_eq!(calls(&host), 2);
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
}
