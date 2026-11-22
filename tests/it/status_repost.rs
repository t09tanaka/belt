//! `belt ci status <run> [--post]` with the real binary, a resident and a
//! fake `gh` (a script that writes down how it was called; nothing here
//! reaches GitHub).
use belt::service::bootstrap::Spec;
use belt::service::resident::{Client, Operation, Reply};
use belt::storage::CoordinatorStore;
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::Duration;

struct Project {
    _root: super::common::DaemonRoot,
    spec: Spec,
    repo: std::path::PathBuf,
    calls: std::path::PathBuf,
    refuse: std::path::PathBuf,
    head: String,
}

fn tool_path(root: &std::path::Path, names: &[&str]) -> String {
    let bin = root.join("bin");
    std::fs::create_dir(&bin).unwrap();
    for name in names {
        let tool = std::env::split_paths(&std::env::var_os("PATH").unwrap())
            .map(|dir| dir.join(name))
            .find(|path| path.is_file())
            .unwrap_or_else(|| panic!("{name} must be installed for this fixture"));
        std::os::unix::fs::symlink(std::fs::canonicalize(tool).unwrap(), bin.join(name)).unwrap();
    }
    format!("{}:/bin", bin.display())
}

/// A committed repository with a GitHub origin, one job (`step` is its
/// command) and a fake `gh` that fails while the `refuse` file exists.
fn project(step: &str) -> Project {
    let root = crate::test_tmp::prefixed("belt-repost-").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let (state, worker) = (root.path().join("parent"), root.path().join("worker"));
    for dir in [&state, &worker] {
        std::fs::create_dir(dir).unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let key = root.path().join("key");
    belt::secrets::key::init(&key).unwrap();
    let mut spec = Spec {
        state_dir: state,
        worker_state_dir: worker,
        worker_id: belt::ids::WorkerId::parse("persistent-local").unwrap(),
        cluster_key_path: key,
        executable: env!("CARGO_BIN_EXE_belt").into(),
        env: BTreeMap::from([
            ("HOME".into(), root.path().display().to_string()),
            ("PATH".into(), String::new()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ]),
        repo_root: None,
        expected_cluster: None,
        managed_service: None,
    };
    if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
        spec.env.insert(
            "LLVM_PROFILE_FILE".into(),
            profile
                .into_string()
                .expect("coverage profile path is UTF-8"),
        );
    }
    let root = super::common::DaemonRoot::new(root, &spec.state_dir);
    spec.env.insert(
        "PATH".into(),
        tool_path(root.path(), &["git", "fast-worktree"]),
    );
    let (calls, refuse) = (root.path().join("gh-calls"), root.path().join("gh-refuse"));
    let gh = root.path().join("bin/gh");
    std::fs::write(
        &gh,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> {calls:?}\nif [ -e {refuse:?} ]; then echo 'gh: Bad credentials (HTTP 401)' >&2; exit 1; fi\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let config = root.path().join(".config/belt");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        config.join("config.toml"),
        "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\n",
    )
    .unwrap();
    belt::service::bootstrap::write_input(&config.join("coordinator.json"), &spec).unwrap();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    super::bootstrap::ci_project(
        root.path(),
        &repo,
        &format!("  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: {step}\n"),
    );
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env_clear()
            .envs(&spec.env)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Fixture"]);
    git(&["config", "user.email", "fixture@example.invalid"]);
    git(&["remote", "add", "origin", "git@github.com:owner/name.git"]);
    git(&["add", "."]);
    git(&["commit", "-qm", "first"]);
    let head = git(&["rev-parse", "HEAD"]);
    Project {
        _root: root,
        spec,
        repo,
        calls,
        refuse,
        head,
    }
}

impl Project {
    fn belt(&self, args: &[&str]) -> std::process::Command {
        let mut command = Command::new(&self.spec.executable);
        command
            .args(args)
            .current_dir(&self.repo)
            .env_clear()
            .envs(
                std::env::var_os("LLVM_PROFILE_FILE").map(|profile| ("LLVM_PROFILE_FILE", profile)),
            )
            .envs(&self.spec.env);
        command
    }

    /// `belt --json <args>`: the exit code and the one JSON value on stdout.
    fn json(&self, args: &[&str]) -> (Option<i32>, serde_json::Value) {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        let output = self.belt(&all).output().unwrap();
        let value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
            panic!(
                "{e}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        });
        (output.status.code(), value)
    }

    fn calls(&self) -> Vec<String> {
        let text = std::fs::read_to_string(&self.calls).unwrap_or_default();
        text.lines().map(str::to_string).collect()
    }

    /// `ci --snapshot commit --ref HEAD` with `more`: the run and the result.
    fn ci(&self, more: &[&str]) -> (String, serde_json::Value) {
        let mut args = vec!["ci", "--snapshot", "commit", "--ref", "HEAD"];
        args.extend_from_slice(more);
        let (code, value) = self.json(&args);
        assert_eq!(code, Some(0), "{value}");
        let run = value["result"]["target"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        (run, value["result"]["github_status"].clone())
    }
}

async fn shutdown(spec: &Spec) {
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let client = Client::load(&spec.state_dir, store.cluster_id().clone()).unwrap();
    assert!(matches!(
        client.request(Operation::Shutdown).await.unwrap(),
        Reply::Stopping
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.socket_path().exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// A post that failed, and one skipped by `--no-status`, are posted again
/// with the same payload the automatic post sent; every attempt is kept with
/// the run and shown by `show` and `logs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_that_failed_or_was_skipped_is_posted_again_as_it_was_first_sent() {
    let p = project("true");
    std::fs::write(&p.refuse, "").unwrap();
    let (run, first) = p.ci(&[]);
    assert_eq!(first["posting"], "failed", "{first}");
    let sent = p.calls();
    assert_eq!(sent.len(), 1);
    let line = &sent[0];
    assert!(
        line.contains("-f description=success: pass 1 / fail 0 / error 0 / skip 0"),
        "{line}"
    );

    // Showing reads only: with no resident, and nothing is posted.
    shutdown(&p.spec).await;
    let (code, shown) = p.json(&["ci", "status", &run]);
    assert_eq!(code, Some(0), "{shown}");
    let result = &shown["result"];
    assert_eq!(result["post"], false);
    assert_eq!(result["previous"], first);
    assert_eq!(result["computed"], "success");
    assert_eq!(result["send"]["sha"], p.head.as_str());
    assert_eq!(result["send"]["repository"], "owner/name");
    assert_eq!(result["send"]["context"], "local-ci");
    assert!(result.get("github_status").is_none());
    assert_eq!(p.calls(), sent, "showing posts nothing");
    let text = p.belt(&["ci", "status", &run]).output().unwrap();
    let text = String::from_utf8_lossy(&text.stdout).into_owned();
    assert!(
        text.contains("previous: status: local-ci=success could not be posted"),
        "{text}"
    );
    assert!(text.contains("would post: local-ci=success to"), "{text}");

    // Still refused by GitHub: the attempt fails the command and is recorded.
    let (code, again) = p.json(&["ci", "status", &run, "--post"]);
    assert_eq!(code, Some(1), "{again}");
    assert_eq!(again["ok"], false);
    let status = &again["result"]["github_status"];
    assert_eq!(status["posting"], "failed", "{again}");
    assert_eq!(status["error"], "gh: Bad credentials (HTTP 401)");
    assert_eq!(p.calls().len(), 2);

    // Accepted now: byte-identical to the automatic post, and kept.
    std::fs::remove_file(&p.refuse).unwrap();
    let (code, posted) = p.json(&["ci", "status", &run, "--post"]);
    assert_eq!(code, Some(0), "{posted}");
    let status = &posted["result"]["github_status"];
    assert_eq!(status["posting"], "posted", "{posted}");
    assert_eq!(status["sha"], p.head.as_str());
    assert_eq!(status["source_digest"], first["source_digest"]);
    assert_eq!(status["original_head"], first["original_head"]);
    let sent_now = p.calls();
    assert_eq!(sent_now.len(), 3);
    assert_eq!(&sent_now[1], line);
    assert_eq!(&sent_now[2], line);
    let (_, run_view) = p.json(&["show", &run]);
    assert_eq!(
        run_view["data"]["run"]["github_status"], *status,
        "{run_view}"
    );
    let logs = p.belt(&["--json", "logs", &run]).output().unwrap();
    let logs = String::from_utf8_lossy(&logs.stdout).into_owned();
    let record = logs
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v["data"]["kind"] == "github_status")
        .unwrap_or_else(|| panic!("no github_status record: {logs}"));
    assert_eq!(record["data"]["github_status"]["posting"], "posted");

    // A skipped one (`--no-status`) is posted too, under the context asked.
    let (run, skipped) = p.ci(&["--no-status"]);
    assert_eq!(skipped["reason"], "--no-status");
    assert_eq!(p.calls().len(), 3);
    let (code, posted) = p.json(&["ci", "status", &run, "--post", "--status-context", "mine"]);
    assert_eq!(code, Some(0), "{posted}");
    assert_eq!(posted["result"]["previous"], skipped);
    assert_eq!(posted["result"]["github_status"]["context"], "mine");
    let calls = p.calls();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[3], line.replace("context=local-ci", "context=mine"));
    // Without a context asked for, the one last posted under is kept.
    let (_, kept) = p.json(&["ci", "status", &run]);
    assert_eq!(kept["result"]["send"]["context"], "mine");

    // What the run collected is gone: nothing is guessed from what is left
    // (that would post `error` over the success GitHub holds).
    let calls = p.calls().len();
    std::fs::remove_dir_all(p.spec.state_dir.join("results")).unwrap();
    for post in [
        &["ci", "status", &run][..],
        &["ci", "status", &run, "--post"][..],
    ] {
        let (code, gone) = p.json(post);
        assert_eq!(code, Some(2), "{gone}");
        let message = gone["error"]["message"].as_str().unwrap();
        assert!(message.contains("cannot be decided"), "{message}");
        assert!(message.contains("cannot be read"), "{message}");
    }
    assert_eq!(p.calls().len(), calls);
    shutdown(&p.spec).await;
}

/// A run that is still running or was cancelled has no state to post; a run
/// that is not a run of `ci` has none either. Nothing is posted or recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_is_running_cancelled_or_not_a_ci_run_is_refused_and_posts_nothing() {
    let p = project("sleep 120");
    let mut child = p
        .belt(&["--json", "ci", "--snapshot", "commit", "--ref", "HEAD"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let run = loop {
        let (_, listed) = p.json(&["ls"]);
        if let Some(id) = listed
            .to_string()
            .split('"')
            .find(|s| s.starts_with("run_") && s.len() > 20)
        {
            break id.to_string();
        }
        assert!(std::time::Instant::now() < deadline, "no run: {listed}");
        std::thread::sleep(Duration::from_millis(200));
    };
    let refused = |args: &[&str], needle: &str| {
        let (code, value) = p.json(args);
        assert_eq!(code, Some(2), "{value}");
        assert_eq!(value["ok"], false);
        let message = value["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(needle),
            "{message} (run {run:?}, {args:?})"
        );
    };
    refused(&["ci", "status", &run, "--post"], "has not finished yet");
    refused(&["ci", "status", &run], "has not finished yet");
    let (code, _) = p.json(&["cancel", &run]);
    assert!(code.is_some());
    child.wait().unwrap();
    refused(&["ci", "status", &run, "--post"], "was cancelled");
    let unknown = belt::ids::RunId::generate().to_string();
    let (code, value) = p.json(&["ci", "status", &unknown, "--post"]);
    assert_eq!(
        code,
        Some(belt::error::ErrorCode::NotFound.exit_code()),
        "{value}"
    );
    assert!(p.calls().is_empty(), "nothing was posted");
    let (_, view) = p.json(&["show", &run]);
    // Only the record the automatic post made when the run was cancelled.
    let kept = &view["data"]["run"]["github_status"];
    assert_eq!(kept["reason"], "the run is cancelled", "{view}");
    shutdown(&p.spec).await;
}
