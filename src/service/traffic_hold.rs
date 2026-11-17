//! A worker whose commands still run in another task-traffic-controller
//! scope does not serve, and does not exit either: an exit is restarted by
//! the service manager at once, over and over. It waits here, looking again
//! with a growing pause, and goes on when the other scope is drained
//! (`config::traffic::hold` says what counts).
//!
//! While it waits nobody renews a lease, so what still runs in the other
//! scope ends at the latest when its lease does.

use crate::config::traffic::{self, Hold};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};

/// Wait until the worker of `state_dir`, started with `env`, may serve.
/// `false`: the process was told to stop meanwhile. An environment the scope
/// cannot be resolved from is an error (waiting does not change it).
pub(crate) async fn wait_until_clear(
    state_dir: &Path,
    env: &BTreeMap<String, String>,
) -> anyhow::Result<bool> {
    wait_with(state_dir, env, traffic::hold_backoff_ms).await
}

async fn wait_with(
    state_dir: &Path,
    env: &BTreeMap<String, String>,
    pause_ms: fn(u32) -> u64,
) -> anyhow::Result<bool> {
    let scope = traffic::resolve(env).map_err(anyhow::Error::msg)?.state_dir;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut said: Option<Hold> = None;
    let mut looks = 0u32;
    loop {
        let (dir, to) = (state_dir.to_path_buf(), scope.clone());
        let hold = tokio::task::spawn_blocking(move || traffic::hold(&dir, &to)).await?;
        // For `worker-service status`. Written again at every look, so that
        // a record nobody renews is known to be stale; without it the worker
        // waits all the same.
        let _ = traffic::record_hold(state_dir, &hold, crate::platform::wall_ms());
        let Some(message) = hold.message() else {
            if said.is_some() {
                eprintln!("worker: the other task-traffic-controller scope is drained; starting");
            }
            return Ok(true);
        };
        // Once per state, not once per look.
        if said.as_ref() != Some(&hold) {
            eprintln!("worker: not serving yet: {message}");
            said = Some(hold.clone());
        }
        let stopped = tokio::select! {
            _ = terminate.recv() => true,
            _ = interrupt.recv() => true,
            _ = tokio::time::sleep(Duration::from_millis(pause_ms(looks))) => false,
        };
        if stopped {
            let _ = traffic::record_hold(state_dir, &Hold::Clear, crate::platform::wall_ms());
            return Ok(false);
        }
        looks = looks.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use task_traffic_controller::{ledger::Ledger, lock, Context, Paths};

    const T: Duration = Duration::from_secs(20);

    fn worker(root: &Path) -> (PathBuf, BTreeMap<String, String>) {
        let state = root.join("state");
        std::fs::create_dir_all(&state).unwrap();
        let env = BTreeMap::from([("HOME".to_string(), root.display().to_string())]);
        (state, env)
    }

    fn legacy_with_live_command(state: &Path) -> (PathBuf, lock::Lock) {
        let legacy = state.join(traffic::LEGACY_DIR);
        let ctx = Context::new(Paths::new(&legacy, legacy.join("config.toml")));
        let mut ledger = Ledger::default();
        ledger.entries.push(
            serde_json::from_value(serde_json::json!({
                "id": "old-run", "pid": std::process::id(), "state": "running", "front": false,
                "weight": 1, "predicted_rss": 0, "repo": "", "argv": "x", "key": "k",
                "seq": 1, "enqueued_at": "2030-01-01T00:00:00Z",
            }))
            .unwrap(),
        );
        ledger.save(&ctx.paths.ledger_file()).unwrap();
        let lease = lock::take_lease(&ctx, "old-run").unwrap();
        (legacy, lease)
    }

    async fn recorded(state: &Path, reason: &str) -> traffic::HoldRecord {
        let deadline = tokio::time::Instant::now() + T;
        loop {
            let now = crate::platform::wall_ms();
            if let Some(record) = traffic::recorded_hold(state, now) {
                if record.reason == reason {
                    return record;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no {reason} recorded"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_worker_waits_for_the_former_scope_instead_of_exiting() {
        let root = crate::test_tmp::tempdir().unwrap();
        let (state, env) = worker(root.path());
        // Nothing to wait for: at once, and nothing recorded.
        assert!(wait_with(&state, &env, |_| 10).await.unwrap());
        assert!(!state.join(traffic::HOLD_FILE).exists());

        let (legacy, lease) = legacy_with_live_command(&state);
        let waiting = {
            let (state, env) = (state.clone(), env.clone());
            tokio::spawn(async move { wait_with(&state, &env, |_| 10).await })
        };
        let record = recorded(&state, "waiting_for_previous_scope").await;
        assert!(
            record.message.contains("live commands (old-run)"),
            "{record:?}"
        );
        // Several looks later it is still there, neither gone nor failed.
        let first = record.checked_at_ms;
        let deadline = tokio::time::Instant::now() + T;
        while recorded(&state, "waiting_for_previous_scope")
            .await
            .checked_at_ms
            == first
        {
            assert!(tokio::time::Instant::now() < deadline, "never looked again");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!waiting.is_finished());
        // The old command ends: the worker goes on, the record goes.
        drop(lease);
        assert!(tokio::time::timeout(T, waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap());
        assert!(!state.join(traffic::HOLD_FILE).exists());
        assert!(legacy.join("ledger.json").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreadable_former_scope_is_reported_as_such_until_it_is_moved_aside() {
        let root = crate::test_tmp::tempdir().unwrap();
        let (state, env) = worker(root.path());
        let legacy = state.join(traffic::LEGACY_DIR);
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("ledger.json"), "{not json").unwrap();
        let waiting = {
            let (state, env) = (state.clone(), env.clone());
            tokio::spawn(async move { wait_with(&state, &env, |_| 10).await })
        };
        let record = recorded(&state, "previous_scope_unreadable").await;
        assert!(record.message.contains("move "), "{record:?}");
        assert!(!waiting.is_finished());
        std::fs::rename(&legacy, state.join("traffic.unreadable")).unwrap();
        assert!(tokio::time::timeout(T, waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap());
        assert!(!state.join(traffic::HOLD_FILE).exists());
    }

    #[tokio::test]
    async fn an_environment_without_a_scope_is_an_error_not_a_wait() {
        let root = crate::test_tmp::tempdir().unwrap();
        let (state, _) = worker(root.path());
        let error = wait_with(&state, &BTreeMap::new(), |_| 10)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("task-traffic-controller scope"), "{error}");
    }
}
