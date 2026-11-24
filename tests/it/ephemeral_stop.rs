//! `worker --ephemeral` told to end (SIGTERM) while an attempt runs: it stops
//! the attempt through the supervisor as a cancel does, waits for it, erases
//! its state and exits 0; a command that ignores TERM ends by KILL within the
//! bound. A new ephemeral worker then starts on the same host.
use belt::ids::*;
use belt::protocol::{connect, transport::Io};
use belt::wire::*;
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_belt");
const T: Duration = Duration::from_secs(20);

fn pgrep(pattern: &str) -> Vec<i32> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", pattern])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

async fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + T;
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn worker_command(
    home: &std::path::Path,
    tools: &std::path::Path,
    cluster: &ClusterId,
    key: &std::path::Path,
) -> tokio::process::Command {
    let mut c = tokio::process::Command::new(BIN);
    c.args(["worker", "--ephemeral", "--cluster"])
        .arg(cluster.to_string())
        .args(["--name", "colab", "--key-file"])
        .arg(key)
        .env("HOME", home)
        .env("PATH", tools)
        .env_remove("XDG_CONFIG_HOME")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    c
}

/// Runs a real attempt on a real ephemeral worker, sends SIGTERM to that
/// worker only, and checks that everything ended within `bound`.
async fn stop_with_live_attempt(pre: &str, tail: &str, bound: Duration) {
    let root = crate::test_tmp::prefixed("belt-eph-").unwrap();
    let home = root.path();
    std::fs::set_permissions(home, std::fs::Permissions::from_mode(0o700)).unwrap();
    let key = home.join("key");
    belt::secrets::key::init(&key).unwrap();
    let cluster = ClusterId::generate();
    let state = home.join(".belt");
    let tools = crate::common::tool_stubs(home);
    // The attempt uses absolute shell/sleep paths and only needs mv on PATH;
    // the worker needs git to start. Nothing else of the host is on PATH, so
    // its native probes see only stubs that answer at once.
    std::os::unix::fs::symlink("/bin/mv", tools.join("mv")).unwrap();
    let git = std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join("git"))
                .find(|candidate| candidate.is_file())
        })
        .expect("git on the PATH of the test");
    std::os::unix::fs::symlink(git, tools.join("git")).unwrap();
    let mut worker = worker_command(home, &tools, &cluster, &key)
        .spawn()
        .unwrap();
    let worker_pid = worker.id().unwrap();
    assert!(worker_pid > 1);
    let worker_start = crate::common::identity_of(worker_pid).unwrap();
    tokio::time::timeout(T, async {
        while tokio::net::UnixStream::connect(state.join("agent.sock"))
            .await
            .is_err()
        {
            assert!(worker.try_wait().unwrap().is_none(), "worker ended early");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let source = home.join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input"), "snapshot").unwrap();
    let env = BTreeMap::from([
        ("HOME".to_string(), home.display().to_string()),
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
    ]);
    let (worker_sources, seed_hold) = crate::common::open_live_store(&state);
    let captured = belt::source::capture(
        &env,
        &worker_sources,
        &belt::source::SourceRequest::Directory { dir: source },
        &belt::source::CaptureParams::default(),
    )
    .unwrap();
    crate::common::publish_as_received(&worker_sources, &captured.manifest.ci_source_id);
    drop(seed_hold);
    let marker = home.join("command.pid");
    let script = format!(
        "fixture_dir='{guard}'; {pre}echo $$ > '{m}.tmp' && mv '{m}.tmp' '{m}' && {tail}",
        guard = home.display(),
        pre = pre,
        tail = tail,
        m = marker.display()
    );
    let payload = AttemptPayload {
        ci_task: None,
        toolchains: None,
        outputs: vec![],
        requirements: Default::default(),
        in_place: false,
        setup: None,
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: captured.manifest.source_id.clone(),
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest))
            .or_else(|_| Digest::parse(&captured.manifest.manifest_digest))
            .unwrap(),
        permit_id: None,
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/sh".into(), "-c".into(), script],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        // Far beyond the test: only the stop ends the attempt.
        lease_ms: 120_000,
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![],
    };
    let attempt = AttemptId::generate();
    let mut bridge = tokio::process::Command::new(BIN)
        .args(["worker", "--stdio"])
        .env("HOME", home)
        .env_remove("XDG_CONFIG_HOME")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let io = Io::new(bridge.stdout.take().unwrap(), bridge.stdin.take().unwrap());
    let connected = connect(io, crate::proto_common::client_cfg_keyed(&cluster, &key))
        .await
        .unwrap();
    let response = connected
        .client
        .call(
            Request::SubmitAttempt(
                SubmitAttemptRequest::new(
                    RequestId::generate(),
                    CoordinatorEpoch(1),
                    attempt.clone(),
                    payload,
                )
                .unwrap(),
            ),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert!(
        matches!(response, Response::SubmitAttempt(_)),
        "{response:?}"
    );
    wait_for("the command to start", || marker.exists()).await;
    let command: i32 = std::fs::read_to_string(&marker)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let supervisors = pgrep(&format!("{}/supervise.json", attempt.as_str()));
    assert!(!supervisors.is_empty(), "no supervisor found");
    assert!(crate::common::alive(command));
    drop(connected);
    let _ = bridge.start_kill();
    let _ = bridge.wait().await;

    // The worker ends while the attempt is live.
    assert_eq!(
        belt::platform::process_start_identity(worker_pid),
        belt::platform::StartIdentity::Identity(worker_start)
    );
    let signalled = Instant::now();
    assert!(belt::platform::signal_pid(worker_pid, libc::SIGTERM));
    let out = tokio::time::timeout(bound, worker.wait_with_output())
        .await
        .expect("the worker ends within the bound")
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "{out:?} {stderr}");
    assert!(signalled.elapsed() < bound, "{:?}", signalled.elapsed());
    // The command and the supervisor are gone, the state with them.
    assert!(!crate::common::alive(command), "the command still runs");
    wait_for("the supervisor to end", || {
        pgrep(&format!("{}/supervise.json", attempt.as_str())).is_empty()
    })
    .await;
    assert!(!state.exists(), "state was kept: {stderr}");

    // A new ephemeral worker starts on the same host and ends cleanly.
    let mut again = worker_command(home, &tools, &cluster, &key)
        .spawn()
        .unwrap();
    let again_pid = again.id().unwrap();
    assert!(again_pid > 1);
    wait_for("the new worker to listen", || {
        state.join("agent.sock").exists()
    })
    .await;
    assert!(
        again.try_wait().unwrap().is_none(),
        "the new worker ended early"
    );
    assert!(belt::platform::signal_pid(again_pid, libc::SIGTERM));
    let out = tokio::time::timeout(T, again.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    assert!(!state.exists());
}

/// A command that obeys TERM: the supervisor ends it within the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_attempt_is_stopped_and_the_state_erased_on_sigterm() {
    stop_with_live_attempt("", "exec /bin/sleep 60", Duration::from_secs(25)).await;
}

/// A command that ignores TERM: it ends by KILL after the grace, still within
/// the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_that_ignores_term_ends_by_kill_within_the_bound() {
    stop_with_live_attempt(
        "trap '' TERM; ",
        "while [ -d \"$fixture_dir\" ]; do /bin/sleep 0.05; done",
        Duration::from_secs(25),
    )
    .await;
}
