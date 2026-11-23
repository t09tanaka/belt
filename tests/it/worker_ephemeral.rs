//! `worker --ephemeral` as the real binary, in a private HOME: it installs
//! nothing, starts with a fresh identity each time, refuses to take over a
//! resident worker's place and removes its state when it is stopped.
use belt::ids::ClusterId;
use belt::protocol::{connect, transport::Io};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_belt");
const T: Duration = Duration::from_secs(20);

struct Fx {
    root: tempfile::TempDir,
    cluster: ClusterId,
}
impl Fx {
    fn new() -> Self {
        let root = crate::test_tmp::prefixed("belt-eph-").unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        belt::secrets::key::init(&root.path().join("key")).unwrap();
        Fx {
            root,
            cluster: ClusterId::generate(),
        }
    }
    fn home(&self) -> &Path {
        self.root.path()
    }
    fn state(&self) -> PathBuf {
        self.home().join(".belt")
    }
    fn command(&self, extra: &[&str]) -> tokio::process::Command {
        let mut c = tokio::process::Command::new(BIN);
        c.args(["worker", "--ephemeral", "--cluster"])
            .arg(self.cluster.to_string())
            .args(["--name", "colab", "--key-file"])
            .arg(self.home().join("key"))
            .args(extra)
            .env("HOME", self.home())
            // The resident journal is found through the config home; an
            // inherited one would point past this private HOME.
            .env_remove("XDG_CONFIG_HOME")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        c
    }
    /// A running worker: the child, and the start identity read when it was spawned.
    fn spawn(&self) -> (tokio::process::Child, u32, String) {
        let child = self.command(&[]).spawn().unwrap();
        let pid = child.id().unwrap();
        assert!(pid > 1);
        let start = super::common::identity_of(pid).expect("child was live when spawned");
        (child, pid, start)
    }
    async fn ready(&self, child: &mut tokio::process::Child) {
        tokio::time::timeout(T, async {
            loop {
                if child.try_wait().unwrap().is_some() {
                    let mut bytes = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(
                        child.stderr.as_mut().unwrap(),
                        &mut bytes,
                    )
                    .await
                    .unwrap();
                    panic!("worker failed: {}", String::from_utf8_lossy(&bytes));
                }
                if tokio::net::UnixStream::connect(self.state().join("agent.sock"))
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    /// The identity the worker reports in `hello`, over the same bridge the parent uses.
    async fn hello(&self) -> (String, String) {
        let mut bridge = tokio::process::Command::new(BIN)
            .args(["worker", "--stdio"])
            .env("HOME", self.home())
            .env_remove("XDG_CONFIG_HOME")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let io = Io::new(bridge.stdout.take().unwrap(), bridge.stdin.take().unwrap());
        let connected = connect(
            io,
            super::proto_common::client_cfg_keyed(&self.cluster, &self.home().join("key")),
        )
        .await
        .unwrap();
        let identity = (
            connected.worker.worker_id.to_string(),
            connected.worker.worker_boot_id.to_string(),
        );
        // The ephemeral process has no installed service journal. The report
        // must come from the profile of the process serving this bridge.
        use belt::wire::{EnvReportRequest, Request, Response, SchemaVersion};
        let response = connected
            .client
            .call(
                Request::EnvReport(EnvReportRequest {
                    schema_version: SchemaVersion::CURRENT,
                    request_id: belt::ids::RequestId::generate(),
                    required: vec![],
                }),
                Duration::from_secs(60),
            )
            .await
            .unwrap();
        let Response::EnvReport(answer) = response else {
            panic!("unexpected environment reply")
        };
        let environment = answer.environment.expect("active worker's full report");
        assert_eq!(environment.schema_version, belt::toolchain::envdiff::SCHEMA);
        assert_eq!(environment.os, answer.os);
        assert_eq!(environment.arch, answer.arch);
        assert!(!environment.profile.is_empty());
        assert_eq!(
            answer.profile_fingerprint,
            connected.worker.profile_fingerprint
        );
        drop(connected);
        let _ = bridge.start_kill();
        let _ = bridge.wait().await;
        identity
    }
    async fn stop(&self, mut child: tokio::process::Child, pid: u32, start: String) {
        assert_eq!(
            belt::platform::process_start_identity(pid),
            belt::platform::StartIdentity::Identity(start)
        );
        assert!(belt::platform::signal_pid(pid, libc::SIGTERM));
        let status = tokio::time::timeout(T, child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
    /// Every entry of the state with its content, so "unchanged" also means
    /// "nothing was created".
    fn listing(&self) -> Vec<(String, Vec<u8>)> {
        let mut found: Vec<_> = std::fs::read_dir(self.state())
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    std::fs::read(entry.path()).unwrap_or_default(),
                )
            })
            .collect();
        found.sort();
        found
    }
    fn marker(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(self.state().join("ephemeral.json")).unwrap())
            .unwrap()
    }
    /// Nothing a resident worker would leave: no service file, no journal.
    fn assert_nothing_installed(&self) {
        assert!(!self.home().join("Library").exists());
        assert!(!self.home().join(".config/systemd").exists());
        assert!(!self.home().join(".config/belt").exists());
    }
}

#[tokio::test]
async fn it_runs_under_a_fresh_identity_installs_nothing_and_cleans_up() {
    let fx = Fx::new();
    let mut seen = Vec::new();
    for _ in 0..2 {
        let (mut child, pid, start) = fx.spawn();
        fx.ready(&mut child).await;
        assert!(fx.state().join("agent.sock").exists());
        let (worker, boot) = fx.hello().await;
        assert_eq!(worker, "colab");
        let os_boot = belt::platform::boot_id().unwrap();
        assert!(boot.starts_with(&format!("{os_boot}.")), "{boot}");
        fx.assert_nothing_installed();
        fx.stop(child, pid, start).await;
        // Its state was its own and is gone, so the next start is clean.
        assert!(!fx.state().exists());
        seen.push(boot);
    }
    assert_ne!(
        seen[0], seen[1],
        "a restart must not look like the same start"
    );
}

#[tokio::test]
async fn it_refuses_existing_state_and_a_resident_worker_and_changes_nothing() {
    let fx = Fx::new();
    let fx = &fx;
    // State left by a persistent worker, or by anything that is not an
    // ephemeral worker: it carries no ephemeral.json (spec 25.4 (d), (iii)).
    std::fs::create_dir(fx.state()).unwrap();
    std::fs::write(fx.state().join("marker"), "x").unwrap();
    let refused = |why: &'static str| async move {
        let before = fx.listing();
        let out = tokio::time::timeout(T, fx.command(&[]).spawn().unwrap().wait_with_output())
            .await
            .unwrap()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert_eq!(out.status.code(), Some(1), "{stderr}");
        assert!(stderr.contains(why), "{stderr}");
        assert_eq!(fx.listing(), before, "a refusal changed the state");
        assert_eq!(
            std::fs::read_to_string(fx.state().join("marker")).unwrap(),
            "x"
        );
    };
    // As anyone could have made it (open to others), then private.
    refused("is kept").await;
    std::fs::set_permissions(fx.state(), std::fs::Permissions::from_mode(0o700)).unwrap();
    refused("starts clean").await;
    refused("no ephemeral.json").await;
    // A marker of another kind, and one that does not parse.
    let marker = fx.state().join("ephemeral.json");
    let write = |text: &str| {
        std::fs::write(&marker, text).unwrap();
        std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).unwrap();
    };
    write(r#"{"schema":1,"kind":"resident_worker"}"#);
    refused("not of the ephemeral worker kind").await;
    write("not json");
    refused("cannot be read").await;
    std::fs::remove_dir_all(fx.state()).unwrap();
    // A resident worker's journal: the role must not exist beside it.
    let config = fx.home().join(".config/belt");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("worker-service.json"), "{}").unwrap();
    let out = tokio::time::timeout(T, fx.command(&[]).spawn().unwrap().wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("persistent worker is installed"));
    assert!(!fx.state().exists());
}

/// Spec 25.4 (d), (i) without a command: a worker that was killed leaves its
/// state behind; the next start proves that nothing uses it, clears it and
/// runs under a new identity.
#[tokio::test]
async fn a_killed_worker_leaves_state_that_the_next_start_clears_by_itself() {
    let fx = Fx::new();
    let (mut child, pid, start) = fx.spawn();
    fx.ready(&mut child).await;
    let (_, first) = fx.hello().await;
    let marker = fx.marker();
    assert_eq!(marker["kind"], "ephemeral_worker");
    assert_eq!(marker["pid"], pid);
    assert_eq!(marker["start"], start.as_str());
    assert_eq!(marker["boot"], belt::platform::boot_id().unwrap().as_str());
    assert_eq!(
        first,
        format!(
            "{}.{}",
            marker["boot"].as_str().unwrap(),
            marker["instance"].as_str().unwrap()
        )
    );
    super::common::kill_recorded(pid, &start);
    let status = tokio::time::timeout(T, child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
    // Killed: nothing was cleaned up.
    assert!(fx.state().join("ephemeral.json").exists());
    assert!(fx.state().join("ephemeral.lock").exists());
    std::fs::write(fx.state().join("left-over"), "old").unwrap();

    let (mut child, pid, start) = fx.spawn();
    fx.ready(&mut child).await;
    assert!(!fx.state().join("left-over").exists());
    let (worker, second) = fx.hello().await;
    assert_eq!(worker, "colab");
    assert_ne!(first, second, "a restart must not look like the same start");
    assert_eq!(fx.marker()["pid"], pid);
    assert_ne!(fx.marker()["instance"], marker["instance"]);
    fx.assert_nothing_installed();
    fx.stop(child, pid, start).await;
    assert!(!fx.state().exists());
}

/// Spec 25.4 (d), (v): a persistent worker never takes over a state that an
/// ephemeral worker made, and touches nothing in it.
#[tokio::test]
async fn a_persistent_worker_refuses_an_ephemeral_state_and_changes_nothing() {
    let fx = Fx::new();
    std::fs::create_dir(fx.state()).unwrap();
    std::fs::set_permissions(fx.state(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let marker = fx.state().join("ephemeral.json");
    std::fs::write(&marker, r#"{"schema":1,"kind":"ephemeral_worker"}"#).unwrap();
    std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o600)).unwrap();
    let spec = belt::service::worker::WorkerSpec {
        schema: 1,
        expected_cluster: fx.cluster.clone(),
        worker_id: belt::ids::WorkerId::parse("colab").unwrap(),
        state_dir: fx.state(),
        cluster_key_path: fx.home().join("key"),
        executable: BIN.into(),
        env: std::collections::BTreeMap::from([
            ("HOME".into(), fx.home().display().to_string()),
            ("PATH".into(), "/usr/bin:/bin".into()),
        ]),
        config: "schema_version=1\n".into(),
    };
    let input = fx.home().join("worker.json");
    std::fs::write(&input, serde_json::to_vec(&spec).unwrap()).unwrap();
    std::fs::set_permissions(&input, std::fs::Permissions::from_mode(0o600)).unwrap();
    let run = || async {
        let out = tokio::time::timeout(
            T,
            tokio::process::Command::new(BIN)
                .args(["worker", "--input"])
                .arg(&input)
                .env("HOME", fx.home())
                .env_remove("XDG_CONFIG_HOME")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap()
                .wait_with_output(),
        )
        .await
        .expect("the worker must refuse, not serve");
        let out = out.unwrap();
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let before = fx.listing();
    let (code, stderr) = run().await;
    assert_eq!(code, Some(1), "{stderr}");
    assert!(
        stderr.contains("belongs to an ephemeral worker"),
        "{stderr}"
    );
    assert!(stderr.contains("belt worker --ephemeral"), "{stderr}");
    assert_eq!(fx.listing(), before, "the refusal changed the state");
}

#[test]
fn the_command_line_needs_all_of_cluster_name_and_key_and_never_mixes_roles() {
    let run = |args: &[&str]| {
        std::process::Command::new(BIN)
            .args(args)
            .output()
            .unwrap()
            .status
            .code()
    };
    assert_eq!(run(&["worker", "--ephemeral"]), Some(2));
    assert_eq!(
        run(&["worker", "--ephemeral", "--cluster", "x", "--name", "n"]),
        Some(2)
    );
    assert_eq!(run(&["worker", "--cluster", "x"]), Some(2));
    assert_eq!(
        run(&[
            "worker",
            "--ephemeral",
            "--stdio",
            "--cluster",
            "x",
            "--name",
            "n",
            "--key-file",
            "/k"
        ]),
        Some(2)
    );
    // A bad value is reported by the worker itself, before anything exists.
    let home = crate::test_tmp::prefixed("belt-eph-").unwrap();
    let out = std::process::Command::new(BIN)
        .args([
            "worker",
            "--ephemeral",
            "--cluster",
            "nope",
            "--name",
            "n",
            "--key-file",
            "/k",
        ])
        .env("HOME", home.path())
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(!home.path().join(".belt").exists());
}
