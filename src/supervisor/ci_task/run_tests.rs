//! A CI task under a real supervisor: a captured directory source, a real
//! workflow file, real step processes behind task-traffic-controller (its
//! state in the test's own directory). No network, no clock.

use super::*;
use crate::ci::execute::tests::{fixture, task_of, Fixture, WORKFLOW};
use crate::ids::*;
use crate::source::classify::source_id_of_ci;
use crate::state::{AttemptState, Outcome};
use crate::storage::{NewAcceptance, WorkerStore};
use crate::supervisor::run::run;
use crate::supervisor::spec::{ArtifactsSpec, MaskValues, SuperviseSpec};
use crate::supervisor::spool::SpoolReader;
use crate::wire::LogStream;
use local_ci::JobOutcome;

struct Sup {
    f: Fixture,
    store: WorkerStore,
    worker: WorkerId,
    plan_digest: Digest,
}

fn sup(workflow: &str) -> Sup {
    let f = fixture(workflow);
    let state = f.area.join("state");
    let worker = WorkerId::parse("w-ci").unwrap();
    let store = WorkerStore::open(&state, &worker).unwrap();
    let bytes = serde_json::to_vec(&f.plan).unwrap();
    let plan_digest = crate::worker::ci_task::store_plan(&state, &bytes).unwrap();
    Sup {
        f,
        store,
        worker,
        plan_digest,
    }
}

impl Sup {
    fn state(&self) -> PathBuf {
        self.f.area.join("state")
    }

    /// Accept an attempt for the task that holds `job`, with a workspace
    /// that is the captured source, and say what its supervisor runs.
    fn attempt(&self, job: &str, inputs: &TaskInputs) -> SuperviseSpec {
        let attempt = AttemptId::generate();
        self.store
            .submit(
                NewAcceptance {
                    attempt_id: attempt.clone(),
                    run_id: RunId::generate(),
                    task_id: TaskId::generate(),
                    generation: Generation(1),
                    payload_hash: PayloadHash::parse(&format!("sha256:{}", "a".repeat(64)))
                        .unwrap(),
                    coordinator_epoch: CoordinatorEpoch(1),
                    spec: serde_json::json!({}),
                },
                1,
            )
            .unwrap();
        self.store
            .transition(&attempt, AttemptState::Preparing, 2)
            .unwrap();
        self.store
            .transition(&attempt, AttemptState::Ready, 3)
            .unwrap();
        let at = self.f.area.join(attempt.as_str());
        let workspace = at.join("ws");
        self.f
            .store
            .materialize(&self.f.manifest, &workspace)
            .unwrap();
        let ci = CiTaskSpec {
            plan_path: crate::worker::ci_task::plan_path(&self.state(), &self.plan_digest),
            plan_digest: self.plan_digest.clone(),
            task_id: task_of(&self.f.plan, job).to_string(),
            inputs: serde_json::to_value(inputs).unwrap(),
            ci_source_id: self.f.manifest.source_id.clone(),
            workspace: workspace.clone(),
            scratch: at.join("ci"),
            env: [
                ("PATH".to_string(), std::env::var("PATH").unwrap()),
                ("HOME".to_string(), at.display().to_string()),
            ]
            .into(),
        };
        SuperviseSpec {
            preparation: None,
            profile_fingerprint: None,
            state_dir: self.state(),
            worker_id: self.worker.clone(),
            attempt_id: attempt,
            traffic_state_dir: self.f.area.join("traffic"),
            traffic_config: None,
            nodes: vec![],
            lease_ms: 60_000,
            term_grace_ms: 10_000,
            kill_wait_ms: 5_000,
            poll_ms: 25,
            command_timeout_ms: None,
            resource_wait_timeout_ms: None,
            deadline_ms: None,
            lease_seed: None,
            inhibit_sleep: false,
            spool_root: Some(self.f.area.join("spool")),
            log_limit_bytes: 1 << 30,
            workspace_warnings: vec![],
            mask: Default::default(),
            artifacts: Some(ArtifactsSpec {
                workspace,
                source_id: source_id_of_ci(&self.f.manifest.source_id).unwrap(),
                plan_digest: self.plan_digest.clone(),
                profile_fingerprint: Digest::of_bytes(b"profile"),
                max_bytes: 1 << 20,
                files: vec![],
                outputs: vec![],
                secret_paths: vec![],
                secret_files: vec![],
            }),
            prepared_only: false,
            ci: Some(ci),
        }
    }

    fn steps(&self, spec: &SuperviseSpec) -> Vec<String> {
        self.store
            .launches(&spec.attempt_id)
            .unwrap()
            .into_iter()
            .map(|(node, _, _)| node.to_string())
            .collect()
    }

    fn output(&self, spec: &SuperviseSpec) -> String {
        let r = SpoolReader::open(&spec.spool_dir(), &spec.attempt_id).unwrap();
        let (mut out, mut off) = (Vec::new(), 0);
        loop {
            let c = r.read(LogStream::Stdout, off, 1 << 20).unwrap();
            if c.data.is_empty() {
                return String::from_utf8(out).unwrap();
            }
            out.extend_from_slice(&c.data);
            off = c.next_offset;
        }
    }

    /// The result the attempt kept for the parent, when it kept one.
    fn result(&self, spec: &SuperviseSpec) -> Option<WireTaskResult> {
        let kept = spec
            .spool_dir()
            .join(spec.attempt_id.as_str())
            .join("artifacts")
            .join(RESULT_ARTIFACT);
        let text = std::fs::read_to_string(kept).ok()?;
        Some(WireTaskResult::from_json(&text).unwrap())
    }

    fn events(&self, spec: &SuperviseSpec, kind: &str) -> Vec<serde_json::Value> {
        let all = self.store.events_of(&spec.attempt_id, kind).unwrap();
        all.into_iter().map(|e| e.payload).collect()
    }
}

fn files_below(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(d) = todo.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                todo.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

fn ci_of(spec: &SuperviseSpec) -> &CiTaskSpec {
    spec.ci.as_ref().unwrap()
}

#[test]
fn a_job_that_needs_another_runs_in_another_attempt_from_the_first_ones_result() {
    let s = sup(WORKFLOW);

    let build = s.attempt("build", &TaskInputs::default());
    let report = run(&build).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Succeeded);
    assert_eq!(term.exit(), Some(Exit::Code(0)));
    assert_eq!(s.steps(&build), ["step-0"], "one node per step that ran");
    let built = s.result(&build).expect("the result is kept as an artifact");
    assert_eq!(built.outcomes.len(), 1);
    assert_eq!(built.outcomes[0].job, "build");
    assert_eq!(built.outcomes[0].status, JobStatus::Success);
    assert_eq!(built.outcomes[0].outputs["version"], "1.2.3");

    // The next attempt has its own workspace: only the outcome crosses.
    let outcomes: Vec<JobOutcome> = built.outcomes.clone();
    let inputs = TaskInputs::new(&outcomes, &outcomes, false);
    let test = s.attempt("test", &inputs);
    let report = run(&test).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Succeeded);
    assert_eq!(s.steps(&test), ["step-0"]);
    assert_eq!(
        std::fs::read_to_string(ci_of(&test).workspace.join("seen.txt")).unwrap(),
        "ran\n"
    );
    assert!(!ci_of(&build).workspace.join("seen.txt").exists());
    let tested = s.result(&test).unwrap();
    assert_eq!(tested.outcomes[0].status, JobStatus::Success);
}

#[test]
fn what_a_step_prints_is_masked_in_the_spool_and_in_the_step_log() {
    const SECRET: &str = "s3cr3t-value-0123456789";
    let s = sup(&format!(
        "on: push\njobs:\n  talk:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo before {SECRET} after\n"
    ));
    let mut spec = s.attempt("talk", &TaskInputs::default());
    spec.mask = MaskValues(vec![SECRET.to_string()]);
    let report = run(&spec).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Succeeded);
    let out = s.output(&spec);
    assert!(out.contains("before") && out.contains("after"), "{out:?}");
    assert!(!out.contains(SECRET), "{out:?}");
    // The copy beside the core's files holds the same masked bytes.
    let logs: Vec<String> = files_below(&ci_of(&spec).scratch.join("run"))
        .into_iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .collect();
    assert!(logs.iter().any(|l| l.contains("before")), "{logs:?}");
    assert!(logs.iter().all(|l| !l.contains(SECRET)), "{logs:?}");
}

#[test]
fn a_failed_job_fails_the_attempt_and_its_result_says_which() {
    let s = sup("on: push\njobs:\n  bad:\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 3\n      - run: echo never > after.txt\n");
    let spec = s.attempt("bad", &TaskInputs::default());
    let report = run(&spec).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Failed);
    assert_eq!(term.exit(), Some(Exit::Code(1)));
    assert_eq!(s.steps(&spec), ["step-0"], "nothing runs after the failure");
    assert!(!ci_of(&spec).workspace.join("after.txt").exists());
    let result = s.result(&spec).unwrap();
    assert_eq!(result.outcomes[0].status, JobStatus::Failure);
}

#[test]
fn a_workspace_that_is_not_the_source_fails_the_attempt_before_any_step() {
    let s = sup(WORKFLOW);
    let spec = s.attempt("build", &TaskInputs::default());
    std::fs::write(ci_of(&spec).workspace.join(".env"), "TOKEN=x").unwrap();
    let report = run(&spec).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Failed);
    assert_eq!(term.exit(), None, "no command ended");
    assert!(s.steps(&spec).is_empty());
    assert!(s.result(&spec).is_none());
    let said = s.events(&spec, "ci_task_failed");
    assert_eq!(said.len(), 1);
    assert!(
        said[0]["message"]
            .as_str()
            .unwrap()
            .contains("not the captured source"),
        "{said:?}"
    );
}

#[test]
fn a_plan_that_does_not_hash_to_the_digest_is_not_run() {
    let s = sup(WORKFLOW);
    let mut spec = s.attempt("build", &TaskInputs::default());
    spec.ci.as_mut().unwrap().plan_digest = Digest::of_bytes(b"another plan");
    let report = run(&spec).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Failed);
    assert!(s.steps(&spec).is_empty());
    assert!(s.events(&spec, "ci_task_failed")[0]["message"]
        .as_str()
        .unwrap()
        .contains("does not hash"));
}

#[test]
fn a_cancel_before_the_first_step_starts_nothing_not_even_an_always_step() {
    let s = sup("on: push\njobs:\n  job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo one > one.txt\n      - if: always()\n        run: echo two > two.txt\n");
    let spec = s.attempt("job", &TaskInputs::default());
    s.store
        .cancel(&spec.attempt_id, CoordinatorEpoch(1), 4)
        .unwrap();
    let report = run(&spec).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(s.steps(&spec).is_empty());
    assert!(!ci_of(&spec).workspace.join("one.txt").exists());
    assert!(!ci_of(&spec).workspace.join("two.txt").exists());
}

const SKIPPED: &str = "on: push\njobs:\n  never:\n    if: false\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ran > ran.txt\n";

#[test]
fn a_job_skipped_by_its_condition_starts_nothing_and_still_reports_its_outcome() {
    let s = sup(SKIPPED);
    let spec = s.attempt("never", &TaskInputs::default());
    let report = run(&spec).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Succeeded);
    assert!(s.steps(&spec).is_empty());
    assert!(!ci_of(&spec).workspace.join("ran.txt").exists());
    // The jobs that need it wait for this.
    let result = s.result(&spec).expect("kept without a step");
    assert_eq!(result.outcomes[0].status, JobStatus::Skipped);
}

#[test]
fn a_cancel_is_not_lost_when_the_task_asks_for_no_step() {
    let s = sup(SKIPPED);
    let spec = s.attempt("never", &TaskInputs::default());
    s.store
        .cancel(&spec.attempt_id, CoordinatorEpoch(1), 4)
        .unwrap();
    let report = run(&spec).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Cancelled);
    assert!(s.steps(&spec).is_empty());
}
