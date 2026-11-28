//! A host built on the public API only: the shared driver decides what is
//! ready, the existing Runner executes it, and the journal replays.

use local_ci::driver::{
    Action, ActionKind, CoreDecision, Driver, DriverConfig, HostFailureReason, Input,
    PortableFacts, Transition, DISPATCH_VERSION, DRIVER_VERSION,
};
use local_ci::exec::{Opts, Workspace};
use local_ci::frozen::Filters;
use local_ci::source::{capture_directory, CaptureOptions, ObjectStore, SourceManifest};
use local_ci::source_execute::{
    execute_pinned_dispatch, execute_pinned_task, PinnedExecution, RefusePinnedForks,
};
use local_ci::wire::Digested;
use local_ci::{
    build_frozen_plan_from_source, ExecContext, FrozenPlan, JobStatus, PinnedPlanOptions,
    WireTaskResult,
};

const FILE: &str = ".github/workflows/ci.yml";
const WORKFLOW: &str = r#"on: push
jobs:
  seed:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.s.outputs.matrix }}
    steps:
      - id: s
        run: echo 'matrix={"include":[{"n":0},{"n":1}]}' >> "$GITHUB_OUTPUT"
  leg:
    needs: seed
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJSON(needs.seed.outputs.matrix) }}
    outputs:
      value: ${{ steps.o.outputs.value }}
    steps:
      - id: o
        run: echo "value=v${{ matrix.n }}" >> "$GITHUB_OUTPUT"
  last:
    needs: leg
    runs-on: ubuntu-latest
    steps:
      - run: test '${{ needs.leg.outputs.value }}' = v1
"#;

struct Host {
    area: tempfile::TempDir,
    ctx: ExecContext,
    store: ObjectStore,
    manifest: SourceManifest,
    plan: FrozenPlan,
    facts: PortableFacts,
    /// every committed transition, as the host would store it
    journal: Vec<String>,
    executed: Vec<String>,
}

impl Host {
    fn new() -> Host {
        let area = tempfile::Builder::new()
            .prefix("localci-driver-host-")
            .tempdir_in(std::fs::canonicalize(std::env::temp_dir()).unwrap())
            .unwrap();
        let root = area.path().join("source");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        std::fs::write(root.join(FILE), WORKFLOW).unwrap();
        let store = ObjectStore::open(area.path().join("objects")).unwrap();
        let ctx = ExecContext::new(
            [
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), area.path().display().to_string()),
            ]
            .into(),
        );
        let manifest = capture_directory(
            &ctx,
            &root,
            &CaptureOptions {
                store: Some(store.root().into()),
                ..Default::default()
            },
        )
        .unwrap()
        .manifest;
        let plan = build_frozen_plan_from_source(
            &store,
            &manifest,
            &PinnedPlanOptions {
                workflow_dirs_override: None,
                filters: Filters {
                    all: false,
                    include_disabled: true,
                    workflows_dirs: vec![],
                    workflows: vec![],
                    jobs: vec![],
                },
                disabled: vec![],
                base_branch: None,
                changed_files: None,
            },
        )
        .unwrap();
        let facts = PortableFacts {
            schema_version: DISPATCH_VERSION,
            github: Default::default(),
            vars: Default::default(),
            inputs: Default::default(),
        };
        Host {
            area,
            ctx,
            store,
            manifest,
            plan,
            facts,
            journal: vec![],
            executed: vec![],
        }
    }

    fn input(&self) -> PinnedExecution<'_> {
        PinnedExecution::new(&self.plan, &self.manifest, &self.store).unwrap()
    }

    fn driver(&self) -> Driver {
        Driver::with_evaluation(
            &self.plan,
            DriverConfig {
                schema_version: DRIVER_VERSION,
                max_parallel: 2,
            },
            &self.input().evaluation_source().unwrap(),
            self.facts.clone(),
        )
        .unwrap()
    }

    /// Propose, store, then apply: the order a host must keep.
    fn commit(&mut self, d: &mut Driver, input: Input) {
        let record = d.propose(input).unwrap();
        self.journal.push(serde_json::to_string(&record).unwrap());
        d.apply(&record).unwrap();
    }

    /// Run the task of a ready action with the existing Runner.
    fn execute(&mut self, action: &ActionKind) -> WireTaskResult {
        let name = format!("attempt-{}", self.executed.len());
        let root = self.area.path().join(name);
        let input = self.input();
        let ws = input
            .materialize_workspace(Workspace {
                root: root.join("root"),
                repo_root: root.join("no-live-repository"),
                run_dir: root.join("run"),
                tools: root.join("tools"),
                lock_file: root.join("lock"),
            })
            .unwrap();
        let opts = Opts {
            parallel: 1,
            traffic_controller: None,
            executor: None,
            portool: None,
            containers: 1,
            job_timeout_min: 1.,
            keep: false,
            trash: self.area.path().join("trash"),
            all_os_legs: false,
        };
        let result = match action {
            ActionKind::Ready { task, inputs } => execute_pinned_task(
                &self.ctx,
                &input,
                task,
                inputs,
                None,
                &ws,
                opts,
                &RefusePinnedForks,
            ),
            ActionKind::DerivedReady { dispatch, inputs } => execute_pinned_dispatch(
                &self.ctx,
                &input,
                dispatch,
                inputs,
                &self.facts,
                None,
                &ws,
                opts,
                &RefusePinnedForks,
            ),
            _ => panic!("not a ready action"),
        }
        .unwrap();
        let wire = WireTaskResult::from_task_result(&result, &ws.workspace().run_dir).unwrap();
        self.executed.push(task_of(action).unwrap().to_string());
        wire
    }

    /// Drive the run to its end. `lost` names the job whose worker never
    /// answers after dispatch: the host fences that claim.
    fn run(&mut self, d: &mut Driver, lost: Option<&str>) {
        self.commit(d, Input::Advance);
        for _ in 0..32 {
            let actions: Vec<Action> = d.pending_actions().to_vec();
            if actions.iter().any(|a| a.kind == ActionKind::Finished) {
                return;
            }
            assert!(!actions.is_empty(), "the run is stuck");
            for action in actions {
                let Some(task) = task_of(&action.kind).map(String::from) else {
                    // a cancellation request: delivered, then acknowledged
                    self.commit(d, Input::Ack { action: action.id });
                    continue;
                };
                let token = format!("attempt of {task}");
                self.commit(
                    d,
                    Input::Claim {
                        task: task.clone(),
                        token: token.clone(),
                    },
                );
                self.commit(
                    d,
                    Input::MarkDispatched {
                        task: task.clone(),
                        token: token.clone(),
                    },
                );
                if lost.is_some_and(|job| self.plan.task(&task).is_some_and(|t| t.jobs == [job])) {
                    self.commit(d, Input::Fence { task, token });
                    continue;
                }
                let wire = self.execute(&action.kind);
                self.commit(
                    d,
                    Input::Settle {
                        task,
                        token,
                        result_digest: wire.digest().unwrap(),
                        result: Box::new(wire),
                    },
                );
            }
        }
        panic!("the run did not finish");
    }

    /// A second driver fed the stored journal ends in the same place.
    fn replay(&self) -> Driver {
        let mut d = self.driver();
        for text in &self.journal {
            d.apply(&Transition::from_json(text).unwrap()).unwrap();
        }
        d
    }
}

fn task_of(action: &ActionKind) -> Option<&str> {
    match action {
        ActionKind::Ready { task, .. } => Some(task),
        ActionKind::DerivedReady { dispatch, .. } => Some(dispatch.task_id()),
        _ => None,
    }
}

#[test]
fn a_host_runs_a_dynamic_matrix_through_the_driver_and_the_runner() {
    let mut host = Host::new();
    let mut d = host.driver();
    host.run(&mut d, None);

    // seed, the two members its output named, and the job after them
    assert_eq!(host.executed.len(), 4);
    assert_eq!(d.admitted(), 0);
    assert!(!d.cancelled());
    assert!(d.infrastructure_failure().is_none());
    for job in ["seed", "leg", "last"] {
        assert_eq!(
            d.outcome(FILE, job).unwrap().status,
            JobStatus::Success,
            "{job}"
        );
    }
    // the joined matrix carries the output of its last member, as on GitHub
    assert_eq!(d.outcome(FILE, "leg").unwrap().outputs["value"], "v1");

    let replayed = host.replay();
    assert_eq!(replayed.revision(), d.revision());
    assert_eq!(replayed.pending_actions(), d.pending_actions());
    assert_eq!(replayed.outcome(FILE, "last"), d.outcome(FILE, "last"));
}

#[test]
fn a_host_fences_a_lost_worker_and_the_run_still_ends() {
    let mut host = Host::new();
    let mut d = host.driver();
    host.run(&mut d, Some("seed"));

    // the producer was lost, so its matrix was never expanded; the job after
    // it went to a worker, which reported it as not run
    let jobs: Vec<String> = host
        .executed
        .iter()
        .map(|t| host.plan.task(t).map_or(t.clone(), |t| t.jobs.join(",")))
        .collect();
    assert_eq!(
        jobs,
        ["last"],
        "{:?}",
        (d.outcome(FILE, "leg"), d.outcome(FILE, "last"))
    );
    assert_eq!(d.admitted(), 0);
    let seed = host
        .plan
        .tasks
        .iter()
        .find(|t| t.jobs == ["seed"])
        .unwrap()
        .id
        .clone();
    assert_eq!(d.core_decision(&seed).unwrap().0, CoreDecision::HostFailure);
    assert_eq!(
        d.host_failure_reason(&seed),
        Some(HostFailureReason::WorkerFenced)
    );
    assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Failure);
    assert!(d.outcome(FILE, "seed").unwrap().outputs.is_empty());
    for job in ["leg", "last"] {
        assert_ne!(
            d.outcome(FILE, job).unwrap().status,
            JobStatus::Success,
            "{job}"
        );
    }

    // the worker answers after all: its receipt is refused, nothing changes
    let ready = ActionKind::Ready {
        task: seed.clone(),
        inputs: local_ci::TaskInputs::new([], [], false),
    };
    let wire = host.execute(&ready);
    let revision = d.revision();
    assert!(d
        .propose(Input::Settle {
            task: seed.clone(),
            token: format!("attempt of {seed}"),
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        })
        .is_err());
    assert_eq!(d.revision(), revision);
    assert_eq!(host.replay().revision(), revision);
}
