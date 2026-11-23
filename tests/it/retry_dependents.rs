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
    // The refusal wrote nothing: no revision, no new attempt, nothing ran
    // again, so b did not keep stale inputs without a word.
    let snapshot = store.completion(Target::Run(run.clone())).unwrap();
    assert_eq!(snapshot.control_revision, 0);
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
    let wait = format!(
        "while [ ! -f {0} ] && [ -d '{1}' ]; do sleep 0.2; done",
        gate.display(),
        root.path().display()
    );
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

/// `a` counts its runs and fails until it ran `passes_at` times; `b` needs
/// `a` and `c` needs `b`, so both are skipped while `a` fails.
fn failing_jobs(passes_at: u32) -> String {
    format!("  a:\n    runs-on: ubuntu-latest\n    outputs:\n      n: ${{{{ steps.c.outputs.n }}}}\n    steps:\n      - id: c\n        run: |\n          echo ran >> MARKS/a\n          n=$(wc -l < MARKS/a | tr -d ' ')\n          echo \"n=$n\" >> \"$GITHUB_OUTPUT\"\n          [ \"$n\" -ge {passes_at} ]\n  b:\n    needs: a\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"b saw ${{{{ needs.a.outputs.n }}}}\" >> MARKS/b\n  c:\n    needs: b\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"c ran\" >> MARKS/c\n")
}

struct Failed {
    _root: super::common::DaemonRoot,
    spec: belt::service::bootstrap::Spec,
    marks: std::path::PathBuf,
    run: RunId,
    a: TaskId,
    store: CoordinatorStore,
}

/// The run of `failing_jobs` ended: `a` failed, `b` and `c` were skipped.
async fn failed_run(passes_at: u32) -> Failed {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(root.path(), &input, &failing_jobs(passes_at));
    start_resident(&spec).await;
    let out = ci_command(&spec, &input).output().await.unwrap();
    assert!(!out.status.success());
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run = RunId::parse(value["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let a = task_of(&store, &run, "a");
    assert_eq!(lines(marks.join("a")), ["ran"]);
    assert!(lines(marks.join("b")).is_empty() && lines(marks.join("c")).is_empty());
    Failed {
        _root: root,
        spec,
        marks,
        run,
        a,
        store,
    }
}

/// Wait for what the retry does: every job has the attempts it should (a
/// new one for each job opened again) and all of them ended. The run is not
/// asked, since it may still read as ended before the progression held the
/// jobs again.
async fn settle(f: &Failed, each: usize) {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let done = ["a", "b", "c"].iter().all(|job| {
                let task = task_of(&f.store, &f.run, job);
                let list = f.store.attempts_of_task(&task).unwrap();
                list.len() == each && list.iter().all(|a| a.state.is_terminal())
            });
            if done {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the jobs did not settle");
}

fn attempts(f: &Failed, job: &str) -> usize {
    let task = task_of(&f.store, &f.run, job);
    f.store.attempts_of_task(&task).unwrap().len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_of_the_failed_job_runs_the_jobs_that_were_skipped_for_it_once_with_the_new_result()
{
    let f = failed_run(2).await;
    control::retry(&f.spec, Target::Task(f.a.clone()))
        .await
        .unwrap();
    settle(&f, 2).await;
    assert_eq!(lines(f.marks.join("a")), ["ran", "ran"]);
    assert_eq!(lines(f.marks.join("b")), ["b saw 2"]);
    assert_eq!(lines(f.marks.join("c")), ["c ran"]);
    assert_eq!((attempts(&f, "b"), attempts(&f, "c")), (2, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_the_retried_job_fails_again_the_jobs_that_need_it_are_skipped_again() {
    let f = failed_run(99).await;
    control::retry(&f.spec, Target::Task(f.a.clone()))
        .await
        .unwrap();
    settle(&f, 2).await;
    assert_eq!(lines(f.marks.join("a")), ["ran", "ran"]);
    assert!(lines(f.marks.join("b")).is_empty() && lines(f.marks.join("c")).is_empty());
    assert_eq!((attempts(&f, "b"), attempts(&f, "c")), (2, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_of_the_run_runs_the_jobs_that_were_skipped_for_the_failed_job() {
    let f = failed_run(2).await;
    control::retry(&f.spec, Target::Run(f.run.clone()))
        .await
        .unwrap();
    settle(&f, 2).await;
    assert_eq!(lines(f.marks.join("b")), ["b saw 2"]);
    assert_eq!(lines(f.marks.join("c")), ["c ran"]);
}

/// `b` has a setup step and a step skipped by its own `if:`: its steps did
/// not all run, but the job did not skip as a job, so it is a job that ran.
const STEPS_SKIPPED: &str = "  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo a >> MARKS/a\n  b:\n    needs: a\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: echo b >> MARKS/b\n        if: \"false\"\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_whose_steps_were_all_skipped_by_their_own_if_is_a_job_that_ran() {
    let (root, spec, input) = control_fixture();
    let marks = ci_project(root.path(), &input, STEPS_SKIPPED);
    start_resident(&spec).await;
    let out = ci_command(&spec, &input).output().await.unwrap();
    assert!(out.status.success());
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let run = RunId::parse(value["result"]["target"]["id"].as_str().unwrap()).unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let (a, b) = (task_of(&store, &run, "a"), task_of(&store, &run, "b"));
    assert!(lines(marks.join("b")).is_empty(), "the step was skipped");
    let error = control::retry(&spec, Target::Task(a.clone()))
        .await
        .unwrap_err();
    refusal_names(&error.message, &a, &b);
}
