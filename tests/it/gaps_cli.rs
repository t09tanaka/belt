//! CLI-level gap tests: refusals and blocked results seen through the real
//! binary and a real resident.

use crate::bootstrap::{control_fixture, start_resident};
use belt::storage::CoordinatorStore;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

/// What the user sees of an `exec` that no worker can take: the run waits
/// (`exec` does not end), the task's placement says why, and nothing ran.
struct Blocked {
    waiting: serde_json::Value,
    shown: serde_json::Value,
    ran: bool,
}

/// Run `belt exec <extra> -- sh -c 'echo > marker'` against a resident whose
/// only worker has the profile `base` (a node stub in its PATH), wait for the
/// task to be reported waiting, `show` it, and stop the CLI we spawned.
async fn exec_that_cannot_be_placed(extra: &[&str]) -> Blocked {
    let (root, spec, input) = control_fixture();
    let bin = root.path().join("profile-bin");
    std::fs::create_dir(&bin).unwrap();
    crate::common::write_executable(&bin.join("node"), "#!/bin/sh\nprintf 'v22.0.0\\n'\n");
    let stubs = root.path().join("tool-stubs");
    let profile = format!(
        "schema_version=1\nactive='base'\n[profiles.base]\npath=[{bin:?},{stubs:?},'/usr/bin','/bin']\nshell='/bin/sh'\ncache_namespace='base'\nrequired_tools=['node']\nallowed_env=['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n"
    );
    let file = spec.worker_state_dir.join("profiles.toml");
    std::fs::write(&file, profile).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    start_resident(&spec).await;
    let marker = root.path().join("ran");
    let script = format!("echo x > '{}'", marker.display());
    let mut child = tokio::process::Command::new(&spec.executable)
        .args(["--json", "exec"])
        .args(extra)
        .args(["--", "/bin/sh", "-c", &script])
        .env_clear()
        .envs(&spec.env)
        .current_dir(&input)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let mut waiting = None;
    for _ in 0..600 {
        waiting = store
            .events_after(0, 512)
            .unwrap()
            .into_iter()
            .find(|e| e.kind == "task_waiting");
        if waiting.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let waiting = waiting.expect("the task is reported waiting").payload;
    let task = waiting["task"].as_str().unwrap().to_string();
    let shown = tokio::process::Command::new(&spec.executable)
        .args(["--json", "show", &task])
        .env_clear()
        .envs(&spec.env)
        .current_dir(&input)
        .output()
        .await
        .unwrap();
    assert!(child.try_wait().unwrap().is_none(), "exec ended by itself");
    child.kill().await.unwrap();
    // Give a wrongly started command time to leave its mark.
    tokio::time::sleep(Duration::from_millis(300)).await;
    Blocked {
        waiting,
        shown: serde_json::from_slice(&shown.stdout).unwrap(),
        ran: marker.exists(),
    }
}

/// An unmet `--requires` (here: the other operating system) leaves no
/// compatible worker: the task waits as `blocked: no_compatible_worker`, the
/// unmet capability is shown with it, and the command never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exec_with_unmet_requires_is_blocked_with_the_unmet_list_and_never_runs() {
    let other_os = if cfg!(target_os = "macos") {
        "linux"
    } else {
        "macos"
    };
    let b = exec_that_cannot_be_placed(&["--requires", other_os]).await;
    let unmet = format!("Os({other_os:?}) unavailable");
    for placement in [
        &b.waiting["decision"],
        &b.shown["data"]["task"]["placement"],
    ] {
        let excluded = &placement["excluded"][0];
        assert_eq!(excluded["why"], "capability_mismatch", "{placement}");
        assert_eq!(excluded["unmet"][0], unmet.as_str(), "{placement}");
    }
    assert_eq!(b.waiting["blocked_reason"], "no_compatible_worker");
    assert_eq!(b.waiting["decision"]["winner"], serde_json::Value::Null);
    assert_eq!(
        b.shown["data"]["task"]["placement"]["blocked_reason"],
        "no_compatible_worker"
    );
    assert!(!b.ran, "the command ran with no compatible worker");
}

/// A profile no worker offers is the same: blocked, the profile named as
/// unmet, nothing runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exec_with_a_profile_no_worker_offers_is_blocked_and_never_runs() {
    let b = exec_that_cannot_be_placed(&["--profile", "nosuch"]).await;
    for placement in [
        &b.waiting["decision"],
        &b.shown["data"]["task"]["placement"],
    ] {
        let excluded = &placement["excluded"][0];
        assert_eq!(excluded["why"], "capability_mismatch", "{placement}");
        assert_eq!(
            excluded["unmet"][0], "profile nosuch unavailable",
            "{placement}"
        );
    }
    assert_eq!(b.waiting["blocked_reason"], "no_compatible_worker");
    assert_eq!(b.waiting["decision"]["winner"], serde_json::Value::Null);
    assert_eq!(
        b.shown["data"]["task"]["placement"]["blocked_reason"],
        "no_compatible_worker"
    );
    assert!(!b.ran, "the command ran with no compatible worker");
}

/// `ci` of a source that has secret files is refused when it is submitted
/// (spec: the workspace of a CI task must be the captured source alone):
/// `INVALID_INPUT` with the reason, and no run is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_of_a_source_with_secret_files_is_refused_and_stores_no_run() {
    let (root, spec, input) = control_fixture();
    crate::bootstrap::ci_project(
        root.path(),
        &input,
        "  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran >> MARKS/build\n",
    );
    std::fs::write(
        input.join(".belt.toml"),
        "schema_version=1\n[source]\nsecret_files=[{glob='private.env',required=true}]\n",
    )
    .unwrap();
    std::fs::write(input.join("private.env"), "TOKEN=x").unwrap();
    start_resident(&spec).await;
    let output = crate::bootstrap::ci_command(&spec, &input)
        .output()
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["ok"], false, "{value}");
    assert_eq!(value["error"]["code"], "INVALID_INPUT", "{value}");
    assert_eq!(
        value["error"]["message"],
        "a CI run of a source with secret files cannot be run yet: the workspace of a CI task \
         must be the captured source alone"
    );
    assert!(!output.status.success());
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let runs: i64 = rusqlite::Connection::open(spec.state_dir.join("coordinator.db"))
        .unwrap()
        .query_row("SELECT count(*) FROM run", [], |row| row.get(0))
        .unwrap();
    assert_eq!(runs, 0, "a refused CI run left a run row");
    assert!(store
        .events_after(0, 512)
        .unwrap()
        .iter()
        .all(|e| e.kind != "run_created"));
    assert!(!root.path().join("marks/build").exists());
}

/// R13 against real workers: the worker and the figures `estimate` gives for
/// a queued task are what the placement then records for its attempt. The
/// command ran on each worker before, but the one-host agents write no
/// task-traffic-controller history, so the figures are the no-history
/// fallback (`NoHistory`); history-backed figures are covered with fake
/// workers in `scheduler`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn estimate_of_a_queued_task_matches_the_placement_on_real_workers() {
    use crate::coordinator::{attempts, only, source, spec};
    use belt::ids::{RunId, TaskId};
    let h = crate::one_host::one_host(&["w-a", "w-b"]);
    let c = h.parent().await;
    let task_of = || {
        let mut task = spec();
        task.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()];
        task.commands[0]
            .env
            .insert("PATH".into(), "/usr/bin:/bin".into());
        (TaskId::generate(), task)
    };
    // The same command runs once on each worker.
    let first: Vec<_> = (0..2).map(|_| task_of()).collect();
    let ids: Vec<_> = first.iter().map(|(id, _)| id.clone()).collect();
    c.enqueue(RunId::generate(), source(&c), first)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    loop {
        c.tick().await.unwrap();
        let ended = |id: &TaskId| {
            attempts(&c, id)
                .last()
                .is_some_and(|a| a.termination.is_some())
        };
        if ids.iter().all(ended) {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(60), "history runs");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut on: Vec<_> = ids
        .iter()
        .map(|id| only(&c, id).worker_id.to_string())
        .collect();
    on.sort();
    assert_eq!(on, ["w-a", "w-b"], "one history run per worker");

    // The same command again: asked first, placed after.
    let (id, again) = task_of();
    c.enqueue(RunId::generate(), source(&c), vec![(id.clone(), again)])
        .await
        .unwrap();
    let preview = c.estimate_task(id.clone()).await.unwrap();
    assert!(preview.online && preview.conditional);
    let predicted = preview.decision.expect("an online preview decides");
    c.tick().await.unwrap();
    let attempt = only(&c, &id);
    let placed = belt::scheduler::reason_of(c.store(), &attempt.attempt_id)
        .unwrap()
        .expect("the decision is recorded with the placement");
    assert_eq!(predicted.winner, placed.winner, "the same worker");
    assert_eq!(predicted.winner.as_ref(), Some(&attempt.worker_id));
    assert_eq!(predicted.why, placed.why);
    assert_eq!(predicted.compared.len(), 2);
    assert_eq!(predicted, placed, "the same figures");
    c.cancel_task(&id).await.unwrap();
}
