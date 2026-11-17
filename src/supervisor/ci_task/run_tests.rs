//! A CI task under a real supervisor: a captured directory source, a real
//! workflow file, real step processes behind task-traffic-controller (its
//! state in the test's own directory). No network. A test that needs time
//! to pass (a lease, a deadline, a time limit) only asserts what holds
//! however slow the machine is: a stop is waited for, never raced.

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
            artifact_workflow: None,
            dispatch: None,
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

// ---- What the supervisor does while the core works outside a step, and
// ---- what ends the attempt. Some of these give the supervisor a core of
// ---- their own: one that takes its time between steps.

use crate::state::{TimeoutKind, TimeoutPhase};
use crate::supervisor::run::run_using;
use crate::wire::LeaseKey;
use local_ci::ScriptPipe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

type Ran = std::result::Result<Vec<JobStatus>, String>;

/// A step as the core would ask for it: a shell script in the workspace.
fn step_of(ci: &CiTaskSpec, cancel: &CancelHandle, script: &str) -> StepRequest {
    StepRequest {
        key: None,
        argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
        cwd: ci.workspace.clone(),
        env: ci.env.clone().into_iter().collect(),
        log: ci.scratch.join("step.log"),
        timeout: None,
        script_pipe: None,
        cancel: cancel.clone(),
    }
}

/// Renew the lease of `spec` every 100 ms, as its coordinator would, until
/// `stop` is set.
fn renew_until(s: &Sup, spec: &SuperviseSpec, stop: &AtomicBool) {
    let store = WorkerStore::open(&s.state(), &s.worker).unwrap();
    let mut seq = 1;
    while !stop.load(Ordering::SeqCst) {
        let key = LeaseKey {
            coordinator_epoch: CoordinatorEpoch(1),
            lease_seq: seq,
        };
        // (Refused once the attempt is over: nothing to renew then.)
        let _ = store.renew_lease(&spec.attempt_id, key, 10 + seq);
        seq += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_renewed_lease_holds_while_the_core_works_outside_a_step_for_longer_than_it() {
    let s = sup(WORKFLOW);
    let mut spec = s.attempt("build", &TaskInputs::default());
    spec.lease_ms = 1_500;
    // More than two leases long without a step (a large workspace to
    // verify), while the coordinator renews every 100 ms. No step follows:
    // only the time outside a step is looked at here.
    let core = |_: &Path, _: &CiTaskSpec, _: Arc<dyn StepExecutor>, cancel: CancelHandle| -> Ran {
        let since = Instant::now();
        while since.elapsed() < Duration::from_millis(4_000) {
            if cancel.is_cancelled() {
                return Err("cancelled while the lease was renewed".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(vec![JobStatus::Success])
    };
    let stop = AtomicBool::new(false);
    let report = std::thread::scope(|t| {
        t.spawn(|| renew_until(&s, &spec, &stop));
        let report = run_using(&spec, &core);
        stop.store(true, Ordering::SeqCst);
        report
    })
    .unwrap();
    let term = report.termination.unwrap();
    assert_eq!(
        term.outcome(),
        Outcome::Succeeded,
        "{term:?}: stops {:?}, {} renewals seen",
        s.events(&spec, "supervisor_stop"),
        s.events(&spec, "lease_renewal_observed").len()
    );
    assert!(s.events(&spec, "supervisor_stop").is_empty());
    assert!(s.events(&spec, "ci_task_failed").is_empty());
    // The renewals were noticed outside any step: none was ever asked for.
    assert!(s.steps(&spec).is_empty());
    assert!(s.events(&spec, "lease_renewal_observed").len() >= 2);
}

#[test]
fn the_core_is_told_what_a_step_ran_and_what_it_waited_and_not_the_time_before_it() {
    let s = sup(WORKFLOW);
    let spec = s.attempt("build", &TaskInputs::default());
    let told = Mutex::new(None);
    let core =
        |_: &Path, ci: &CiTaskSpec, steps: Arc<dyn StepExecutor>, cancel: CancelHandle| -> Ran {
            // Time of the core's own before the step: no time of the step.
            std::thread::sleep(Duration::from_millis(500));
            let asked = Instant::now();
            let exit = steps
                .run(&step_of(ci, &cancel, "sleep 0.3; echo late > late.txt"))
                .map_err(|e| e.to_string())?;
            *told.lock().unwrap() = Some((exit, asked.elapsed().as_secs_f64()));
            Ok(vec![JobStatus::Success])
        };
    let report = run_using(&spec, &core).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Succeeded, "{term:?}");
    assert_eq!(s.steps(&spec), ["step-0"]);
    assert!(ci_of(&spec).workspace.join("late.txt").exists());
    let (told, answered_in) = told.lock().unwrap().expect("the step was answered");
    assert_eq!(told.exit, StepEnd::Code(0));
    assert!(told.run_secs >= 0.25, "{told:?}");
    let waited = told.queue_wait_secs.expect("the wait is told apart");
    assert!(waited >= 0.0, "{told:?}");
    assert!(
        told.run_secs + waited <= answered_in + 0.05,
        "{told:?} in {answered_in}"
    );
}

struct Idle {
    outcome: Outcome,
    timeout: Option<crate::state::TimeoutInfo>,
    /// The core saw its handle cancelled, after this long.
    cancelled_after: Option<Duration>,
    /// What the core was told for the step it asked for after that.
    late_step: StepEnd,
    spec: SuperviseSpec,
}

/// Run an attempt whose core asks for no step until it is cancelled (it
/// gives up after 20 s) and then asks for one anyway. `during` runs beside.
fn idle_until_stopped(
    s: &Sup,
    change: impl FnOnce(&mut SuperviseSpec),
    during: impl FnOnce(&SuperviseSpec) + Send,
) -> Idle {
    let mut spec = s.attempt("job", &TaskInputs::default());
    change(&mut spec);
    let seen = Mutex::new((None, None));
    let core =
        |_: &Path, ci: &CiTaskSpec, steps: Arc<dyn StepExecutor>, cancel: CancelHandle| -> Ran {
            let since = Instant::now();
            while !cancel.is_cancelled() && since.elapsed() < Duration::from_secs(20) {
                std::thread::sleep(Duration::from_millis(5));
            }
            let cancelled = cancel.is_cancelled().then(|| since.elapsed());
            let late = steps
                .run(&step_of(ci, &cancel, "echo late > late.txt"))
                .map_err(|e| e.to_string())?;
            *seen.lock().unwrap() = (cancelled, Some(late.exit));
            Ok(vec![JobStatus::Cancelled])
        };
    let report = std::thread::scope(|t| {
        t.spawn(|| during(&spec));
        run_using(&spec, &core)
    })
    .unwrap();
    let term = report.termination.unwrap();
    let (cancelled_after, late_step) = *seen.lock().unwrap();
    assert!(
        s.steps(&spec).is_empty(),
        "nothing is started after the stop"
    );
    assert!(!ci_of(&spec).workspace.join("late.txt").exists());
    Idle {
        outcome: term.outcome(),
        timeout: term.timeout(),
        cancelled_after,
        late_step: late_step.expect("the core was answered"),
        spec,
    }
}

const ONE_JOB: &str = "on: push\njobs:\n  job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo one > one.txt\n";

#[test]
fn a_cancel_while_the_core_works_outside_a_step_stops_the_core_at_once() {
    let s = sup(ONE_JOB);
    let idle = idle_until_stopped(
        &s,
        |_| {},
        |spec| {
            std::thread::sleep(Duration::from_millis(300));
            let store = WorkerStore::open(&s.state(), &s.worker).unwrap();
            store
                .cancel(&spec.attempt_id, CoordinatorEpoch(1), 4)
                .unwrap();
        },
    );
    assert_eq!(idle.outcome, Outcome::Cancelled);
    // Not at its next step (it asked for none): within a few looks.
    let after = idle.cancelled_after.expect("the core was cancelled");
    assert!(after < Duration::from_secs(5), "{after:?}");
    assert_eq!(idle.late_step, StepEnd::Cancelled);
    let stops = s.events(&idle.spec, "supervisor_stop");
    assert_eq!(stops.len(), 1, "{stops:?}");
    assert_eq!(stops[0]["reason"], "Cancel");
}

#[test]
fn a_lease_nobody_renews_expires_while_the_core_works_outside_a_step() {
    let s = sup(ONE_JOB);
    let idle = idle_until_stopped(&s, |spec| spec.lease_ms = 400, |_| {});
    assert_eq!(idle.outcome, Outcome::Interrupted);
    let after = idle.cancelled_after.expect("the core was cancelled");
    assert!(after < Duration::from_secs(5), "{after:?}");
    assert_eq!(idle.late_step, StepEnd::Cancelled);
}

#[test]
fn the_deadline_of_the_attempt_counts_while_the_core_works_outside_a_step() {
    let s = sup(ONE_JOB);
    let idle = idle_until_stopped(
        &s,
        |spec| {
            spec.deadline_ms = Some(400);
            // Neither of these is a limit of the time between steps.
            spec.command_timeout_ms = Some(1);
            spec.resource_wait_timeout_ms = Some(1);
        },
        |_| {},
    );
    assert_eq!(idle.outcome, Outcome::TimedOut);
    assert_eq!(idle.timeout.map(|t| t.kind), Some(TimeoutKind::Deadline));
    assert_eq!(idle.timeout.map(|t| t.phase), Some(TimeoutPhase::Running));
    assert!(idle.cancelled_after.expect("cancelled") < Duration::from_secs(5));
}

#[test]
fn a_step_whose_script_comes_through_a_pipe_is_an_error_of_the_step_and_starts_nothing() {
    let s = sup(ONE_JOB);
    let spec = s.attempt("job", &TaskInputs::default());
    let core =
        |_: &Path, ci: &CiTaskSpec, steps: Arc<dyn StepExecutor>, cancel: CancelHandle| -> Ran {
            let mut piped = step_of(ci, &cancel, "echo piped > piped.txt");
            // (Never read here: the step is refused before anything is opened.)
            piped.script_pipe = Some(ScriptPipe {
                read_fd: 0,
                child_fd: 3,
            });
            let refused = steps.run(&piped).expect_err("not run");
            // The core reports a step that could not be run as an error.
            Err(format!("{refused:#}"))
        };
    let report = run_using(&spec, &core).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Failed);
    assert_eq!(term.exit(), None);
    assert!(s.steps(&spec).is_empty());
    assert!(!ci_of(&spec).workspace.join("piped.txt").exists());
    let said = s.events(&spec, "ci_task_failed");
    assert_eq!(said[0]["message"], SCRIPT_PIPE_REFUSED, "{said:?}");
    assert!(SCRIPT_PIPE_REFUSED.contains("read from a pipe"));
}

/// A step that runs into its limit, and what the plan runs after it.
fn over_its_limit(first: &str) -> String {
    format!(
        "on: push\njobs:\n  job:\n    runs-on: ubuntu-latest\n    steps:\n{first}      - run: echo next > next.txt\n      - if: failure()\n        run: echo failure > failure.txt\n      - if: always()\n        run: echo always > always.txt\n"
    )
}

fn made(spec: &SuperviseSpec, names: &[&str]) -> Vec<bool> {
    names
        .iter()
        .map(|n| ci_of(spec).workspace.join(n).exists())
        .collect()
}

#[test]
fn a_step_over_its_timeout_minutes_fails_its_job_and_the_cleanup_steps_still_run() {
    let s = sup(&over_its_limit(
        "      - run: sleep 60\n        timeout-minutes: 0.01\n",
    ));
    let spec = s.attempt("job", &TaskInputs::default());
    let since = Instant::now();
    let report = run(&spec).unwrap();
    assert!(
        since.elapsed() < Duration::from_secs(40),
        "the step was stopped"
    );
    let term = report.termination.unwrap();
    // The attempt is not timed out: a job of the task failed.
    assert_eq!(term.outcome(), Outcome::Failed, "{term:?}");
    assert_eq!(term.exit(), Some(Exit::Code(1)));
    assert_eq!(term.timeout(), None);
    assert_eq!(
        made(&spec, &["next.txt", "failure.txt", "always.txt"]),
        [false, true, true]
    );
    assert_eq!(s.steps(&spec), ["step-0", "step-1", "step-2"]);
    let result = s.result(&spec).expect("the result is kept");
    assert_eq!(result.outcomes[0].status, JobStatus::Failure);
}

#[test]
fn what_follows_a_step_over_its_limit_with_continue_on_error_is_the_core_s_decision() {
    let s = sup(&over_its_limit(
        "      - run: sleep 60\n        timeout-minutes: 0.01\n        continue-on-error: true\n",
    ));
    let spec = s.attempt("job", &TaskInputs::default());
    let report = run(&spec).unwrap();
    let term = report.termination.unwrap();
    // The supervisor only reports the step as timed out and goes on serving
    // steps: the attempt is not timed out, the job has a result, and the
    // `always()` step ran. Whether the job counts as failed is the core's
    // rule for `continue-on-error` (not pinned here).
    assert_ne!(term.outcome(), Outcome::TimedOut, "{term:?}");
    assert_eq!(term.timeout(), None);
    assert_eq!(made(&spec, &["always.txt"]), [true]);
    assert!(s.steps(&spec).len() >= 2, "{:?}", s.steps(&spec));
    let result = s.result(&spec).expect("the result is kept");
    assert_eq!(
        term.outcome() == Outcome::Succeeded,
        result.outcomes[0].status == JobStatus::Success
    );
}

#[test]
fn the_limit_of_the_attempt_on_a_command_caps_a_step_and_is_a_result_of_the_step_too() {
    // The plan gives the step an hour; the attempt allows a command 600 ms.
    let s = sup(&over_its_limit(
        "      - run: sleep 60\n        timeout-minutes: 60\n",
    ));
    let mut spec = s.attempt("job", &TaskInputs::default());
    spec.command_timeout_ms = Some(600);
    let since = Instant::now();
    let report = run(&spec).unwrap();
    assert!(since.elapsed() < Duration::from_secs(40));
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::Failed, "{term:?}");
    assert_eq!(
        made(&spec, &["next.txt", "failure.txt", "always.txt"]),
        [false, true, true]
    );
}

const LONG_THEN_ALWAYS: &str = "on: push\njobs:\n  job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo started > started.txt; sleep 60\n      - if: always()\n        run: echo always > always.txt\n";

fn wait_for_file(path: &Path) {
    let end = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(Instant::now() < end, "{path:?} was never made");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_cancel_during_a_step_stops_it_and_no_always_step_follows() {
    let s = sup(LONG_THEN_ALWAYS);
    let spec = s.attempt("job", &TaskInputs::default());
    let report = std::thread::scope(|t| {
        t.spawn(|| {
            wait_for_file(&ci_of(&spec).workspace.join("started.txt"));
            let store = WorkerStore::open(&s.state(), &s.worker).unwrap();
            store
                .cancel(&spec.attempt_id, CoordinatorEpoch(1), 4)
                .unwrap();
        });
        run(&spec)
    })
    .unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Cancelled);
    assert_eq!(s.steps(&spec), ["step-0"]);
    assert_eq!(made(&spec, &["always.txt"]), [false]);
}

#[test]
fn a_lease_that_expires_during_a_step_stops_it_and_no_always_step_follows() {
    let s = sup(LONG_THEN_ALWAYS);
    let spec = s.attempt("job", &TaskInputs::default());
    let report = std::thread::scope(|t| {
        t.spawn(|| {
            // Once the step runs, the lease is marked expired where the
            // supervisor reads it (as the agent does when it runs out).
            wait_for_file(&ci_of(&spec).workspace.join("started.txt"));
            let store = WorkerStore::open(&s.state(), &s.worker).unwrap();
            store.expire_lease(&spec.attempt_id, 20).unwrap();
        });
        run(&spec)
    })
    .unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
    assert_eq!(s.steps(&spec), ["step-0"]);
    assert_eq!(made(&spec, &["started.txt", "always.txt"]), [true, false]);
}

#[test]
fn the_deadline_during_a_step_times_the_attempt_out_and_no_always_step_follows() {
    let s = sup(LONG_THEN_ALWAYS);
    let mut spec = s.attempt("job", &TaskInputs::default());
    spec.deadline_ms = Some(1_500);
    let report = run(&spec).unwrap();
    let term = report.termination.unwrap();
    assert_eq!(term.outcome(), Outcome::TimedOut);
    assert_eq!(term.timeout().map(|t| t.kind), Some(TimeoutKind::Deadline));
    assert_eq!(made(&spec, &["always.txt"]), [false]);
}

#[test]
fn a_result_that_holds_a_secret_value_is_not_kept_and_is_said_to_be_missing() {
    const SECRET: &str = "s3cr3t-value-0123456789";
    let s = sup(&WORKFLOW.replace("1.2.3", SECRET));
    let mut spec = s.attempt("build", &TaskInputs::default());
    spec.mask = MaskValues(vec![SECRET.to_string()]);
    let report = run(&spec).unwrap();
    // The task itself ran: what is refused is handing its outputs on.
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Succeeded);
    assert!(s.result(&spec).is_none(), "the result is not kept");
    let refused = s.events(&spec, "artifact_not_stored");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0]["name"], RESULT_ARTIFACT);
    assert!(!refused[0].to_string().contains(SECRET));
    // Nothing of the attempt that the parent reads holds the value.
    let kept = files_below(&spec.spool_dir().join(spec.attempt_id.as_str()));
    for path in kept {
        let bytes = std::fs::read(&path).unwrap_or_default();
        assert!(
            !String::from_utf8_lossy(&bytes).contains(SECRET),
            "{path:?}"
        );
    }
}

#[test]
fn a_task_that_started_no_command_keeps_its_result_and_looks_for_nothing_declared() {
    let s = sup(SKIPPED);
    let mut spec = s.attempt("never", &TaskInputs::default());
    // In the workspace from the start (a file of the source): a search for
    // it would find it.
    let declared = crate::supervisor::spec::ArtifactFile {
        name: "workflow".into(),
        source: ".github/workflows/ci.yml".into(),
        if_missing: crate::wire::MissingPolicy::Required,
    };
    spec.artifacts.as_mut().unwrap().files = vec![declared];
    let report = run(&spec).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Succeeded);
    assert!(s.steps(&spec).is_empty());
    assert!(
        s.result(&spec).is_some(),
        "the result is kept without a step"
    );
    let store = spec
        .spool_dir()
        .join(spec.attempt_id.as_str())
        .join("artifacts");
    assert!(!store.join("workflow").exists(), "nothing declared is kept");
    assert!(s.events(&spec, "artifact_not_stored").is_empty());
}

#[test]
fn a_declared_artifact_of_a_task_that_ran_a_step_is_kept_beside_its_result() {
    let s = sup(ONE_JOB);
    let mut spec = s.attempt("job", &TaskInputs::default());
    spec.artifacts.as_mut().unwrap().files = vec![crate::supervisor::spec::ArtifactFile {
        name: "one".into(),
        source: "one.txt".into(),
        if_missing: crate::wire::MissingPolicy::Required,
    }];
    let report = run(&spec).unwrap();
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Succeeded);
    let store = spec
        .spool_dir()
        .join(spec.attempt_id.as_str())
        .join("artifacts");
    assert_eq!(std::fs::read_to_string(store.join("one")).unwrap(), "one\n");
    assert!(s.result(&spec).is_some());
}

#[test]
fn a_launched_step_is_found_again_by_recovery_under_the_number_it_was_registered_with() {
    use crate::supervisor::recover::launched_index;
    let s = sup(ONE_JOB);
    let mut spec = s.attempt("job", &TaskInputs::default());
    assert_eq!(launched_index(&spec, "step-0"), Some(0));
    assert_eq!(launched_index(&spec, "step-12"), Some(12));
    assert_eq!(launched_index(&spec, "main"), None);
    assert_eq!(launched_index(&spec, "step-01"), None);
    // Only a CI task has steps: elsewhere a name is a node of the spec or
    // nothing.
    spec.ci = None;
    assert_eq!(launched_index(&spec, "step-0"), None);
}

#[test]
fn recovery_of_a_ci_task_whose_supervisor_was_lost_during_a_step_settles_it_and_removes_its_pipe() {
    use crate::platform::process_read::StartIdentity;
    use crate::storage::ProcessIdentity;
    use crate::supervisor::pump::Pipe;
    use crate::supervisor::recover::recover_on;
    use crate::supervisor::stop::fake::{FakeOs, NO_SUCH_PID};
    let s = sup(LONG_THEN_ALWAYS);
    let spec = s.attempt("job", &TaskInputs::default());
    let attempt = &spec.attempt_id;
    // What the lost supervisor left: the launch of its first step, with the
    // identity of the step's process, and the pipe of that step.
    let node = NodeId::parse("step-0").unwrap();
    s.store.claim_launch(attempt, &node, 10).unwrap();
    let boot = crate::platform::boot_id().expect("a boot id");
    let identity = ProcessIdentity {
        boot_id: WorkerBootId::parse(&boot).unwrap(),
        pid: NO_SUCH_PID,
        start_identity: "linux:1000".into(),
    };
    s.store
        .record_process_identity(attempt, &node, &identity, 11)
        .unwrap();
    let fifo = Pipe::path_for(&spec.spool_dir(), attempt, 0);
    std::fs::create_dir_all(fifo.parent().unwrap()).unwrap();
    let c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    // SAFETY: a valid NUL-terminated path.
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    // Another process has the number by now: nothing is signalled.
    let os = FakeOs::new([StartIdentity::Zombie("linux:2000".into())]);
    let report = recover_on(&spec, &s.store, Some(&os)).unwrap();
    assert!(os.signals.borrow().is_empty());
    assert_eq!(report.termination.unwrap().outcome(), Outcome::Interrupted);
    assert_eq!(
        s.events(&spec, "supervisor_recover_node")[0]["node"],
        "step-0"
    );
    assert!(!fifo.exists(), "the pipe of the step is removed");
    // Nothing of the plan ran after the loss, not even the `always()` step.
    assert_eq!(s.steps(&spec), ["step-0"]);
    assert_eq!(made(&spec, &["always.txt"]), [false]);
}
