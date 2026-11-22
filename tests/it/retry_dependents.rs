//! R19: a retry of a job that a job of the run already used. The retry is
//! refused (naming the job) while the job that needs it started or ended with
//! the earlier results, so no job keeps stale inputs without a word.
use super::bootstrap::{ci_command, ci_project, control_fixture, start_resident};
use belt::ids::{RunId, TaskId};
use belt::service::completion::Target;
use belt::service::control;
use belt::storage::CoordinatorStore;
use std::time::Duration;

/// Job `a` counts its runs in its output; `b` records what it saw.
fn jobs(b_wait: &str) -> String {
    format!("  a:\n    runs-on: ubuntu-latest\n    outputs:\n      n: ${{{{ steps.c.outputs.n }}}}\n    steps:\n      - id: c\n        run: |\n          echo ran >> MARKS/a\n          echo \"n=$(wc -l < MARKS/a | tr -d ' ')\" >> \"$GITHUB_OUTPUT\"\n  b:\n    needs: a\n    runs-on: ubuntu-latest\n    steps:\n      - run: |\n          echo started >> MARKS/b_started\n          {b_wait}\n          echo \"b saw ${{{{ needs.a.outputs.n }}}}\" >> MARKS/b\n")
}

fn task_of(store: &CoordinatorStore, run: &RunId, job: &str) -> TaskId {
    let rows = store.ci_run_tasks(run).unwrap();
    let suffix = format!("::{job}");
    let row = rows
        .iter()
        .find(|r| {
            r.spec["ci"]["task"]["task_id"]
                .as_str()
                .unwrap()
                .ends_with(&suffix)
        })
        .unwrap();
    row.task_id.clone()
}

fn lines(path: std::path::PathBuf) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn refusal_names(message: &str, a: &TaskId, b: &TaskId) {
    assert!(
        message.contains(&format!("retry of {a} would leave {b}")),
        "{message}"
    );
    assert!(
        message.contains(&format!("retry {b} as well or retry the run")),
        "{message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_of_a_job_is_refused_when_the_job_that_needs_it_ended_with_the_earlier_results() {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(root.path(), &input, &jobs(":"));
    start_resident(&spec).await;
    let out = ci_command(&spec, &input).output().await.unwrap();
    assert!(out.status.success());
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run = RunId::parse(value["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let (a, b) = (task_of(&store, &run, "a"), task_of(&store, &run, "b"));
    let error = control::retry(&spec, Target::Task(a.clone()))
        .await
        .unwrap_err();
    refusal_names(&error.message, &a, &b);
    // Nothing ran again: b did not keep stale inputs without a word.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(lines(marks.join("a")), ["ran"]);
    assert_eq!(lines(marks.join("b")), ["b saw 1"]);
    assert_eq!(store.attempts_of_task(&a).unwrap().len(), 1);
    // The dependent alone can be retried.
    control::retry(&spec, Target::Task(b.clone()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_of_a_job_is_refused_while_the_job_that_needs_it_runs() {
    let (root, spec, input) = control_fixture();
    let gate = root.path().join("marks/gate");
    let wait = format!("while [ ! -f {} ]; do sleep 0.2; done", gate.display());
    let marks = ci_project(
        root.path(),
        &input,
        &crate::fixture_process::script(&jobs(&wait)),
    );
    start_resident(&spec).await;
    let mut child = ci_command(&spec, &input)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(120), async {
        while lines(marks.join("b_started")).is_empty() {
            assert!(child.try_wait().unwrap().is_none(), "ci ended early");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let run = store.query_runs(10, None).unwrap().items[0].run_id.clone();
    let (a, b) = (task_of(&store, &run, "a"), task_of(&store, &run, "b"));
    let error = control::retry(&spec, Target::Task(a.clone()))
        .await
        .unwrap_err();
    refusal_names(&error.message, &a, &b);
    std::fs::write(&gate, "").unwrap();
    let out = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(lines(marks.join("a")), ["ran"]);
    assert_eq!(lines(marks.join("b")), ["b saw 1"]);
    assert_eq!(store.attempts_of_task(&a).unwrap().len(), 1);
}
