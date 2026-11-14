//! Real captured source/shell results, not an executor recorder.
use super::*;
use crate::exec::{Opts, Outcome, Workspace};
use crate::source::{CaptureOptions, ObjectStore, SourceManifest};
use crate::source_execute::{
    execute_pinned_dispatch, execute_pinned_task, PinnedExecution, RefusePinnedForks,
};
use crate::{ExecContext, PinnedPlanOptions};
use serde_json::json;
use std::path::PathBuf;

const FILE: &str = ".github/workflows/ci.yml";
const WORKFLOW: &str = r#"on: push
jobs:
  seed:
    runs-on: [self-hosted, seed]
    outputs:
      matrix: ${{ steps.seed.outputs.matrix }}
      go: ${{ steps.seed.outputs.go }}
    steps:
      - id: seed
        run: |
          echo 'matrix={"include":[{"n":0,"os":"alpha"},{"n":1,"os":"beta"}]}' >> "$GITHUB_OUTPUT"
          echo 'go=yes' >> "$GITHUB_OUTPUT"
  dynamic:
    name: 'leg-${{ matrix.n }}'
    needs: seed
    if: needs.seed.outputs.go == 'yes'
    runs-on:
      group: ${{ inputs.group }}
      labels: [self-hosted, '${{ matrix.os }}']
    strategy:
      max-parallel: 2
      fail-fast: false
      matrix: ${{ fromJSON(needs.seed.outputs.matrix) }}
    env:
      LEG: ${{ matrix.n }}
      EXTRA: ${{ vars.tag }}
    outputs:
      value: ${{ steps.out.outputs.value }}
    steps:
      - id: out
        run: |
          test "$EXTRA" = actual-portable
          test "$LEG" = '${{ matrix.n }}'
          test '${{ strategy.job-index }}' = "$LEG"
          test '${{ strategy.job-total }}' = 2
          test '${{ github.workspace }}' = "$PWD"
          test -d '${{ runner.temp }}'
          test -n '${{ runner.os }}'
          test -z "${GITHUB_SHA:-}"
          test ! -e marker
          echo "$LEG" > marker
          echo "value=v$LEG" >> "$GITHUB_OUTPUT"
          echo "NEXT_ENV=e$LEG" >> "$GITHUB_ENV"
      - if: steps.out.outputs.value == format('v{0}', matrix.n)
        run: test "$NEXT_ENV" = "e$LEG"
      - if: false
        run: exit 99
  consumer:
    needs: dynamic
    runs-on: consumer
    steps:
      - run: test '${{ needs.dynamic.outputs.value }}' = v1
"#;
struct Fixture {
    area: tempfile::TempDir,
    ctx: ExecContext,
    store: ObjectStore,
    manifest: SourceManifest,
    plan: FrozenPlan,
    facts: PortableFacts,
}
impl Fixture {
    fn new(workflow: &str) -> Self {
        Self::new_files(workflow, &[])
    }
    fn new_files(workflow: &str, files: &[(&str, &str)]) -> Self {
        let area = tempfile::Builder::new()
            .prefix("localci-dynamic-driver-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let root = area.path().join("source");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        std::fs::write(root.join(FILE), workflow).unwrap();
        for (path, content) in files {
            std::fs::create_dir_all(root.join(path).parent().unwrap()).unwrap();
            std::fs::write(root.join(path), content).unwrap();
        }
        let store = ObjectStore::open(area.path().join("objects")).unwrap();
        let ctx = ExecContext::new(
            [
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), area.path().display().to_string()),
            ]
            .into(),
        );
        let manifest = crate::source::capture_directory(
            &ctx,
            &root,
            &CaptureOptions {
                store: Some(store.root().into()),
                ..Default::default()
            },
        )
        .unwrap()
        .manifest;
        let plan = crate::build_frozen_plan_from_source(
            &store,
            &manifest,
            &PinnedPlanOptions {
                workflow_dirs_override: None,
                filters: crate::frozen::Filters {
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
            github: Map::new(),
            vars: json!({"tag":"actual-portable"})
                .as_object()
                .unwrap()
                .clone(),
            inputs: json!({"group":"private-pool"}).as_object().unwrap().clone(),
        };
        Self {
            area,
            ctx,
            store,
            manifest,
            plan,
            facts,
        }
    }
    fn input(&self) -> PinnedExecution<'_> {
        PinnedExecution::new(&self.plan, &self.manifest, &self.store).unwrap()
    }
    fn driver(&self, limit: usize) -> Driver {
        Driver::with_evaluation(
            &self.plan,
            DriverConfig {
                schema_version: DRIVER_VERSION,
                max_parallel: limit,
            },
            &self.input().evaluation_source().unwrap(),
            self.facts.clone(),
        )
        .unwrap()
    }
    fn workspace(&self, name: &str) -> Workspace {
        let root = self.area.path().join(name);
        Workspace {
            root: root.join("root"),
            repo_root: root.join("no-live-repository"),
            run_dir: root.join("run"),
            tools: root.join("tools"),
            lock_file: root.join("lock"),
        }
    }
    fn options(&self) -> Opts {
        Opts {
            parallel: 1,
            traffic_controller: None,
            executor: None,
            portool: None,
            containers: 1,
            job_timeout_min: 1.,
            keep: false,
            trash: self.area.path().join("trash"),
        }
    }
    fn execute(&self, action: &ActionKind, name: &str) -> (WireTaskResult, PathBuf) {
        let input = self.input();
        let ws = input.materialize_workspace(self.workspace(name)).unwrap();
        let result = match action {
            ActionKind::Ready { task, inputs } => execute_pinned_task(
                &self.ctx,
                &input,
                task,
                inputs,
                None,
                &ws,
                self.options(),
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
                self.options(),
                &RefusePinnedForks,
            ),
            _ => panic!("not ready"),
        }
        .unwrap();
        (
            WireTaskResult::from_task_result(&result, &ws.workspace().run_dir).unwrap(),
            ws.workspace().root.clone(),
        )
    }
}
use serde_json::Map;
fn commit(d: &mut Driver, input: Input) -> Transition {
    let record = d.propose(input).unwrap();
    d.apply(&record).unwrap();
    record
}
fn ready(d: &Driver) -> Vec<ActionKind> {
    d.pending_actions()
        .iter()
        .filter(|a| ready_task(&a.kind).is_some())
        .map(|a| a.kind.clone())
        .collect()
}
fn claim(d: &mut Driver, action: &ActionKind) {
    let task = ready_task(action).unwrap().to_string();
    commit(
        d,
        Input::Claim {
            task: task.clone(),
            token: task,
        },
    );
}
fn settle(d: &mut Driver, action: &ActionKind, wire: WireTaskResult) -> Transition {
    let task = ready_task(action).unwrap().to_string();
    commit(
        d,
        Input::Settle {
            task: task.clone(),
            token: task,
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        },
    )
}
fn seed(f: &Fixture, d: &mut Driver) -> Vec<Transition> {
    let first = commit(d, Input::Advance);
    let action = ready(d)
        .into_iter()
        .find(|a| f.plan.task(ready_task(a).unwrap()).unwrap().jobs == ["seed"])
        .unwrap();
    assert_eq!(
        d.requirements(ready_task(&action).unwrap()).unwrap()[0].evaluated,
        Some(json!(["self-hosted", "seed"]))
    );
    let claimed = d
        .propose(Input::Claim {
            task: ready_task(&action).unwrap().into(),
            token: ready_task(&action).unwrap().into(),
        })
        .unwrap();
    d.apply(&claimed).unwrap();
    let (wire, _) = f.execute(&action, "seed");
    assert_eq!(
        wire.clone()
            .into_task_result(Path::new("."))
            .unwrap()
            .outcomes[0]
            .status,
        JobStatus::Success
    );
    let task = ready_task(&action).unwrap().to_string();
    let settled = d
        .propose(Input::Settle {
            task: task.clone(),
            token: task,
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        })
        .unwrap();
    assert_eq!(d.admitted(), 1);
    assert!(!ready(d)
        .iter()
        .any(|a| matches!(a, ActionKind::DerivedReady { .. })));
    d.apply(&settled).unwrap();
    vec![first, claimed, settled]
}

#[test]
fn real_needs_matrix_replay_reverse_workers_join_and_consumer() {
    let f = Fixture::new(WORKFLOW);
    let before = serde_json::to_vec(&f.plan).unwrap();
    let mut d = f.driver(2);
    let journal = seed(&f, &mut d);
    assert_eq!(d.admitted(), 0);
    let mut legs = ready(&d);
    legs.sort_by_key(|a| match a {
        ActionKind::DerivedReady { dispatch, .. } => dispatch.index(),
        _ => usize::MAX,
    });
    assert_eq!(legs.len(), 2);
    let mut replay = f.driver(2);
    for record in &journal {
        let decoded = Transition::from_json(&serde_json::to_string(record).unwrap()).unwrap();
        replay.apply(&decoded).unwrap();
    }
    assert_eq!(
        serde_json::to_value(d.pending_actions()).unwrap(),
        serde_json::to_value(replay.pending_actions()).unwrap()
    );
    assert!(replay.apply(journal.last().unwrap()).is_err());
    for (i, leg) in legs.iter().enumerate() {
        let requirement = &d.requirements(ready_task(leg).unwrap()).unwrap()[0];
        assert_eq!(
            requirement.original.as_ref().unwrap()["labels"][1],
            "${{ matrix.os }}"
        );
        assert_eq!(
            requirement.evaluated,
            Some(
                json!({"group":"private-pool","labels":["self-hosted",if i==0 {"alpha"} else {"beta"}]})
            )
        );
        claim(&mut d, leg);
    }
    assert_eq!(d.admitted(), 2);
    let mut roots = Vec::new();
    for (i, leg) in legs.iter().enumerate().rev() {
        let (wire, root) = f.execute(leg, &format!("worker-{i}"));
        let result = wire.clone().into_task_result(Path::new(".")).unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.outcomes[0].variant, Some(i));
        assert_eq!(
            result.outcomes[0].status,
            JobStatus::Success,
            "{:?}",
            result.jobs
        );
        assert_eq!(result.jobs[0].display, format!("leg-{i}"));
        assert_eq!(result.jobs[0].steps[1].outcome, Outcome::Pass);
        assert_eq!(
            result.jobs[0].steps[2].outcome,
            Outcome::Skip("if: false".into())
        );
        assert_eq!(
            std::fs::read_to_string(root.join("marker")).unwrap().trim(),
            i.to_string()
        );
        roots.push(root);
        settle(&mut d, leg, wire);
        if i == 1 {
            assert!(d.outcome(FILE, "dynamic").is_none());
            assert_eq!(d.admitted(), 1);
        }
    }
    assert_ne!(roots[0], roots[1]);
    assert_eq!(d.outcome(FILE, "dynamic").unwrap().outputs["value"], "v1");
    let consumer = ready(&d)
        .into_iter()
        .find(|a| matches!(a, ActionKind::Ready { .. }))
        .unwrap();
    claim(&mut d, &consumer);
    let (wire, _) = f.execute(&consumer, "consumer");
    assert_eq!(
        wire.clone()
            .into_task_result(Path::new("."))
            .unwrap()
            .outcomes[0]
            .status,
        JobStatus::Success
    );
    settle(&mut d, &consumer, wire);
    assert!(d
        .pending_actions()
        .iter()
        .any(|a| a.kind == ActionKind::Finished));
    assert_eq!(serde_json::to_vec(&f.plan).unwrap(), before);
    f.plan.verify().unwrap();
}

#[test]
fn worker_recomputes_membership_source_facts_and_inputs_before_execution() {
    let f = Fixture::new(WORKFLOW);
    let mut d = f.driver(2);
    let journal = seed(&f, &mut d);
    let leg = ready(&d).remove(0);
    let ActionKind::DerivedReady { dispatch, inputs } = leg else {
        panic!()
    };
    let input = f.input();
    let ws = input
        .materialize_workspace(f.workspace("rejected-worker"))
        .unwrap();
    let reject = |proof: &DerivedDispatch, values: &TaskInputs, facts: &PortableFacts| {
        assert!(execute_pinned_dispatch(
            &f.ctx,
            &input,
            proof,
            values,
            facts,
            None,
            &ws,
            f.options(),
            &RefusePinnedForks
        )
        .is_err());
        assert!(!ws.workspace().root.join("marker").exists());
        assert!(!ws.workspace().tools.exists());
    };
    for field in [
        "schema_version",
        "plan_digest",
        "origin",
        "task",
        "index",
        "inputs_digest",
        "evaluation_digest",
        "expansion_digest",
    ] {
        let mut forged = serde_json::to_value(&dispatch).unwrap();
        forged[field] = if field == "schema_version" || field == "index" {
            json!(999)
        } else {
            json!("forged")
        };
        let proof = DerivedDispatch::from_json(&forged.to_string()).unwrap();
        reject(&proof, &inputs, &f.facts);
    }
    let mut forged_inputs = inputs.clone();
    forged_inputs
        .needs
        .get_mut("seed")
        .unwrap()
        .outputs
        .insert("go".into(), "changed".into());
    reject(&dispatch, &forged_inputs, &f.facts);
    let mut forged_facts = f.facts.clone();
    forged_facts.vars.insert("tag".into(), json!("changed"));
    reject(&dispatch, &inputs, &forged_facts);
    let mut replay = Driver::with_evaluation(
        &f.plan,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: 2,
        },
        &input.evaluation_source().unwrap(),
        forged_facts,
    )
    .unwrap();
    assert!(replay.apply(&journal[0]).is_err());
    assert!(d
        .propose(Input::Claim {
            task: format!("{}#999", dispatch.origin()),
            token: "owner".into()
        })
        .is_err());
    let original = std::fs::read(
        f.store
            .object_path(input.evaluation_source().unwrap().record().workflows()[0].object_hash()),
    )
    .unwrap();
    let hash = input.evaluation_source().unwrap().record().workflows()[0]
        .object_hash()
        .to_string();
    std::fs::write(f.store.object_path(&hash), b"corrupt source").unwrap();
    reject(&dispatch, &inputs, &f.facts);
    std::fs::write(f.store.object_path(&hash), original).unwrap();
    let action = ActionKind::DerivedReady { dispatch, inputs };
    claim(&mut d, &action);
    let (wire, _) = f.execute(&action, "valid-worker");
    let task = ready_task(&action).unwrap().to_string();
    for mismatch in ["member", "variant", "owner"] {
        let mut forged = wire.clone();
        let mut owner = task.clone();
        if mismatch == "member" {
            forged.task = "nonmember".into();
        }
        if mismatch == "variant" {
            forged.outcomes[0].variant = Some(999);
        }
        if mismatch == "owner" {
            owner = "stale-owner".into();
        }
        assert!(d
            .propose(Input::Settle {
                task: task.clone(),
                token: owner,
                result_digest: forged.digest().unwrap(),
                result: Box::new(forged)
            })
            .is_err());
        assert_eq!(d.admitted(), 1);
    }
    settle(&mut d, &action, wire);
}

#[test]
fn early_skip_and_failures_preserve_producer_terminal_and_release() {
    for (matrix, cond, expected, diagnostic) in [
        (
            "not-json",
            "false",
            CoreDecision::UnstartedSkipped,
            "if: false",
        ),
        ("not-json", "true", CoreDecision::EarlyFailure, "matrix:"),
        (
            "{\"n\":[]}",
            "true",
            CoreDecision::EarlyFailure,
            "matrix has no combinations",
        ),
        (
            "not-json",
            "runner.os == 'Linux'",
            CoreDecision::EarlyFailure,
            "if:",
        ),
        (
            "not-json",
            "matrix.n == 0",
            CoreDecision::EarlyFailure,
            "if:",
        ),
        (
            "not-json",
            "github.actor == 'unknown'",
            CoreDecision::EarlyFailure,
            "github property unavailable",
        ),
    ] {
        let workflow = WORKFLOW
            .replace(
                "{\"include\":[{\"n\":0,\"os\":\"alpha\"},{\"n\":1,\"os\":\"beta\"}]}",
                matrix,
            )
            .replace("needs.seed.outputs.go == 'yes'", cond);
        let workflow = if expected == CoreDecision::UnstartedSkipped {
            workflow.replace("max-parallel: 2", "max-parallel: 0")
        } else {
            workflow
        };
        let f = Fixture::new(&workflow);
        let mut d = f.driver(2);
        let journal = seed(&f, &mut d);
        let origin = f.plan.tasks.iter().find(|t| t.dynamic_matrix).unwrap();
        let (decision, reason) = d.core_decision(&origin.id).unwrap();
        assert_eq!(decision, expected);
        assert!(reason.unwrap().contains(diagnostic), "{reason:?}");
        assert!(reason.unwrap().len() <= 4096);
        assert_eq!(d.admitted(), 0);
        assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Success);
        let seed_id = f
            .plan
            .tasks
            .iter()
            .find(|t| t.jobs == ["seed"])
            .unwrap()
            .id
            .clone();
        assert!(matches!(
            d.state.phases[&seed_id],
            Phase::Terminal(Terminal::Worker { .. })
        ));
        assert!(!ready(&d)
            .iter()
            .any(|a| matches!(a, ActionKind::DerivedReady { .. })));
        let mut replay = f.driver(2);
        for record in journal {
            replay.apply(&record).unwrap();
        }
        assert_eq!(
            replay.core_decision(&origin.id),
            d.core_decision(&origin.id)
        );
        let consumer = ready(&d).remove(0);
        claim(&mut d, &consumer);
        let (wire, _) = f.execute(&consumer, "skipped-consumer");
        assert_eq!(
            wire.clone()
                .into_task_result(Path::new("."))
                .unwrap()
                .outcomes[0]
                .status,
            JobStatus::Skipped
        );
        settle(&mut d, &consumer, wire);
        assert!(d
            .pending_actions()
            .iter()
            .any(|a| a.kind == ActionKind::Finished));
    }
}

#[test]
fn matrix_and_global_limits_gate_concrete_member_lifetimes() {
    for (matrix_limit, global_limit, reason) in
        [(1, 2, "matrix admission"), (2, 1, "global admission")]
    {
        let f = Fixture::new(
            &WORKFLOW.replace("max-parallel: 2", &format!("max-parallel: {matrix_limit}")),
        );
        let mut d = f.driver(global_limit);
        seed(&f, &mut d);
        let legs = ready(&d);
        claim(&mut d, &legs[0]);
        assert!(d
            .propose(Input::Claim {
                task: ready_task(&legs[1]).unwrap().into(),
                token: "second".into()
            })
            .unwrap_err()
            .to_string()
            .contains(reason));
        let (wire, _) = f.execute(&legs[0], "first");
        settle(&mut d, &legs[0], wire);
        assert_eq!(d.admitted(), 0);
        claim(&mut d, &legs[1]);
        assert_eq!(d.admitted(), 1);
        let (wire, _) = f.execute(&legs[1], "second");
        settle(&mut d, &legs[1], wire);
        assert_eq!(d.admitted(), 0);
        assert!(d.outcome(FILE, "dynamic").is_some());
    }
}

#[test]
fn dynamic_fail_fast_retires_queued_and_holds_cancelled_owner_until_real_result() {
    let workflow = WORKFLOW
        .replace("fail-fast: false", "fail-fast: true")
        .replace(
            "{\"n\":1,\"os\":\"beta\"}]}",
            "{\"n\":1,\"os\":\"beta\"},{\"n\":2,\"os\":\"gamma\"}]}",
        )
        .replace("strategy.job-total }}' = 2", "strategy.job-total }}' = 3")
        .replace(
            "          test ! -e marker",
            "          if [ \"$LEG\" = 0 ]; then exit 1; fi\n          test ! -e marker",
        );
    let f = Fixture::new(&workflow);
    let mut d = f.driver(3);
    seed(&f, &mut d);
    let legs = ready(&d);
    assert_eq!(legs.len(), 3);
    claim(&mut d, &legs[0]);
    claim(&mut d, &legs[1]);
    let (wire, _) = f.execute(&legs[0], "failing-worker");
    assert_eq!(
        wire.clone()
            .into_task_result(Path::new("."))
            .unwrap()
            .outcomes[0]
            .status,
        JobStatus::Failure
    );
    settle(&mut d, &legs[0], wire);
    assert_eq!(d.admitted(), 1);
    assert!(d.outcome(FILE, "dynamic").is_none());
    assert!(matches!(
        d.state.phases[ready_task(&legs[2]).unwrap()],
        Phase::Terminal(Terminal::Core {
            decision: CoreDecision::UnstartedSkipped,
            ..
        })
    ));
    assert!(!ready(&d)
        .iter()
        .any(|a| ready_task(a) == ready_task(&legs[2])));
    let cancel=d.pending_actions().iter().find(|a|matches!(&a.kind,ActionKind::Cancel{task,..} if task==ready_task(&legs[1]).unwrap())).unwrap().id.clone();
    commit(
        &mut d,
        Input::Ack {
            action: cancel.clone(),
        },
    );
    commit(&mut d, Input::Ack { action: cancel });
    assert_eq!(d.admitted(), 1);
    let (wire, _) = f.execute(&legs[1], "owned-worker");
    assert_eq!(
        wire.clone()
            .into_task_result(Path::new("."))
            .unwrap()
            .outcomes[0]
            .status,
        JobStatus::Success
    );
    settle(&mut d, &legs[1], wire.clone());
    assert_eq!(d.admitted(), 0);
    settle(&mut d, &legs[1], wire);
    assert_eq!(d.admitted(), 0);
    assert_eq!(
        d.outcome(FILE, "dynamic").unwrap().status,
        JobStatus::Failure
    );
}

#[test]
fn expansion_budgets_and_grouped_dynamic_are_explicit_before_dispatch() {
    let matrix = json!({"n": (0..100).collect::<Vec<_>>(), "os": ["alpha"]}).to_string();
    let workflow = WORKFLOW.replace(
        "{\"include\":[{\"n\":0,\"os\":\"alpha\"},{\"n\":1,\"os\":\"beta\"}]}",
        &matrix,
    );
    let mut f = Fixture::new(&workflow);
    f.facts
        .vars
        .insert("large_explicit_fact".into(), json!("x".repeat(512 * 1024)));
    let mut d = f.driver(2);
    seed(&f, &mut d);
    let origin = f.plan.tasks.iter().find(|t| t.dynamic_matrix).unwrap();
    assert_eq!(
        d.core_decision(&origin.id).unwrap().0,
        CoreDecision::EarlyFailure
    );
    assert!(d
        .core_decision(&origin.id)
        .unwrap()
        .1
        .unwrap()
        .contains("combination limit"));
    assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Success);
    assert_eq!(d.admitted(), 0);
    let mut grouped = f.plan.clone();
    grouped
        .tasks
        .iter_mut()
        .find(|t| t.dynamic_matrix)
        .unwrap()
        .jobs
        .push("seed".into());
    grouped.digest = grouped.compute_digest().unwrap();
    let source = PinnedExecution::new(&grouped, &f.manifest, &f.store)
        .unwrap()
        .evaluation_source()
        .unwrap();
    let error = Driver::with_evaluation(
        &grouped,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: 2,
        },
        &source,
        f.facts.clone(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("atomic shared-workspace"));
}

#[test]
fn source_identity_overrides_case_variants_and_fact_ambiguity_rejects() {
    let mut f = Fixture::new(WORKFLOW);
    f.facts.github = json!({"WORKFLOW":"forged", "JOB":"forged", "EVENT_NAME":"forged", "ACTOR":"explicit-host"}).as_object().unwrap().clone();
    let source = f.input().evaluation_source().unwrap();
    let evaluation = EvaluationContext::new(&f.plan, &source, f.facts.clone()).unwrap();
    let origin = f.plan.tasks.iter().find(|t| t.dynamic_matrix).unwrap();
    let contexts = evaluation.contexts(&f.plan, origin).unwrap();
    assert_eq!(contexts["github"]["workflow"], f.plan.workflows[0].name);
    assert_eq!(contexts["github"]["job"], "dynamic");
    assert_eq!(contexts["github"]["event_name"], "push");
    assert_eq!(contexts["github"]["actor"], "explicit-host");
    f.facts.github.insert("actor".into(), json!("ambiguous"));
    assert!(EvaluationContext::new(&f.plan, &source, f.facts.clone()).is_err());
}

#[test]
fn amplified_values_and_small_remaining_state_keep_real_producer_and_replay() {
    let repeated = "{0}".repeat(32);
    let expression = format!("format('{repeated}', vars.blob)");
    for target in ["matrix", "name", "remaining"] {
        let workflow = if target == "matrix" {
            // Real expression-valued include, before combo/variant/display copies.
            format!("on: push\njobs:\n  seed:\n    runs-on: [self-hosted, seed]\n    outputs:\n      matrix: ${{{{ steps.seed.outputs.matrix }}}}\n    steps:\n      - id: seed\n        run: echo 'matrix={{\"n\":[0]}}' >> \"$GITHUB_OUTPUT\"\n  dynamic:\n    needs: seed\n    runs-on: x\n    strategy:\n      matrix:\n        include:\n          - blob: ${{{{ {expression} }}}}\n    steps: []\n  consumer:\n    needs: dynamic\n    runs-on: x\n    steps:\n      - run: exit 99\n")
        } else if target == "name" {
            WORKFLOW.replace(
                "name: 'leg-${{ matrix.n }}'",
                &format!("name: ${{{{ {expression} }}}}"),
            )
        } else {
            WORKFLOW.to_string()
        };
        let mut f = Fixture::new(&workflow);
        if target != "remaining" {
            f.facts
                .vars
                .insert("blob".into(), json!("x".repeat(512 * 1024)));
        }
        let mut d = f.driver(2);
        if target == "remaining" {
            d.state_byte_limit = 16 * 1024;
        }
        let journal = seed(&f, &mut d);
        let origin = f.plan.tasks.iter().find(|t| t.dynamic_matrix).unwrap();
        let (decision, reason) = d.core_decision(&origin.id).unwrap();
        assert_eq!(decision, CoreDecision::EarlyFailure);
        assert!(reason.unwrap().contains("budget"), "{target}: {reason:?}");
        assert!(reason.unwrap().len() < 2048);
        assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Success);
        assert_eq!(d.admitted(), 0);
        assert!(!ready(&d)
            .iter()
            .any(|a| matches!(a, ActionKind::DerivedReady { .. })));
        let mut replay = f.driver(2);
        replay.state_byte_limit = d.state_byte_limit;
        for record in journal {
            replay.apply(&record).unwrap();
        }
        assert_eq!(
            replay.core_decision(&origin.id),
            d.core_decision(&origin.id)
        );
        assert_eq!(
            serde_json::to_value(replay.pending_actions()).unwrap(),
            serde_json::to_value(d.pending_actions()).unwrap()
        );
        let consumer = ready(&d).remove(0);
        claim(&mut d, &consumer);
        let (wire, _) = f.execute(&consumer, "budget-skipped-consumer");
        assert_eq!(
            wire.clone()
                .into_task_result(Path::new("."))
                .unwrap()
                .outcomes[0]
                .status,
            JobStatus::Skipped
        );
        settle(&mut d, &consumer, wire);
        assert!(d
            .pending_actions()
            .iter()
            .any(|a| a.kind == ActionKind::Finished));
        assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
    }
}

#[test]
fn fanout_fallbacks_do_not_spend_producer_terminal_capacity() {
    let mut workflow = WORKFLOW.replace(
        "name: 'leg-${{ matrix.n }}'",
        &format!("name: '{}'", "x".repeat(32 * 1024)),
    );
    for index in 0..80 {
        let condition = if index % 4 == 0 {
            "false".to_string()
        } else {
            format!("'{}' trailing", "x".repeat(1536))
        };
        workflow.push_str(&format!("  zz_fanout_{index:02}:\n    needs: seed\n    if: \"{condition}\"\n    runs-on: x\n    strategy:\n      matrix: ${{{{ fromJSON(needs.seed.outputs.matrix) }}}}\n    steps: []\n"));
    }
    let f = Fixture::new(&workflow);
    assert_eq!(f.plan.tasks.iter().filter(|t| t.dynamic_matrix).count(), 81);
    let mut d = f.driver(2);
    d.state_byte_limit = 180 * 1024;
    let journal = seed(&f, &mut d);
    assert_eq!(d.admitted(), 0);
    assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Success);
    assert!(!ready(&d)
        .iter()
        .any(|a| matches!(a, ActionKind::DerivedReady { .. })));
    for origin in f.plan.tasks.iter().filter(|t| t.dynamic_matrix) {
        let (decision, _) = d.core_decision(&origin.id).unwrap();
        if let Some(index) = origin.jobs[0].strip_prefix("zz_fanout_") {
            assert_eq!(
                decision,
                if index.parse::<usize>().unwrap() % 4 == 0 {
                    CoreDecision::UnstartedSkipped
                } else {
                    CoreDecision::EarlyFailure
                }
            );
        } else {
            assert_eq!(decision, CoreDecision::EarlyFailure);
        }
    }
    let mut replay = f.driver(2);
    replay.state_byte_limit = d.state_byte_limit;
    for record in journal {
        replay.apply(&record).unwrap();
    }
    assert_eq!(
        serde_json::to_value(&replay.state).unwrap(),
        serde_json::to_value(&d.state).unwrap()
    );
    let consumer = ready(&d).remove(0);
    claim(&mut d, &consumer);
    let (wire, _) = f.execute(&consumer, "fanout-skipped-consumer");
    assert_eq!(
        wire.clone()
            .into_task_result(Path::new("."))
            .unwrap()
            .outcomes[0]
            .status,
        JobStatus::Skipped
    );
    settle(&mut d, &consumer, wire);
    assert!(d
        .pending_actions()
        .iter()
        .any(|a| a.kind == ActionKind::Finished));
    assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
}

fn cancel_actions(d: &Driver) -> Vec<Action> {
    d.pending_actions()
        .iter()
        .filter(|a| matches!(a.kind, ActionKind::Cancel { .. }))
        .cloned()
        .collect()
}
fn finished(d: &Driver) -> bool {
    d.pending_actions()
        .iter()
        .any(|a| a.kind == ActionKind::Finished)
}

#[test]
fn cancellation_before_dispatch_and_after_finished_are_stable() {
    let f = Fixture::new(WORKFLOW);
    let mut d = f.driver(2);
    let proposed = d.propose(Input::Cancel).unwrap();
    assert!(!d.cancelled());
    assert!(!finished(&d));
    d.apply(&proposed).unwrap();
    assert!(d.cancelled());
    assert!(finished(&d));
    assert_eq!(d.admitted(), 0);
    assert!(ready(&d).is_empty());
    assert!(d.state.expansions.is_empty());
    for task in &f.plan.tasks {
        assert_eq!(
            d.core_decision(&task.id).unwrap().0,
            CoreDecision::UnstartedCancelled
        );
    }
    let before =
        serde_json::to_value((&d.state.phases, &d.state.outcomes, &d.state.outbox)).unwrap();
    commit(&mut d, Input::Cancel);
    commit(&mut d, Input::Advance);
    assert_eq!(
        serde_json::to_value((&d.state.phases, &d.state.outcomes, &d.state.outbox)).unwrap(),
        before
    );
    let completed=Fixture::new("on: push\njobs:\n  seed:\n    runs-on: [self-hosted, seed]\n    steps:\n      - run: echo completed\n");
    let mut done = completed.driver(1);
    seed(&completed, &mut done);
    assert!(finished(&done));
    assert!(!done.cancelled());
    let before =
        serde_json::to_value((&done.state.phases, &done.state.outcomes, &done.state.outbox))
            .unwrap();
    commit(&mut done, Input::Cancel);
    commit(&mut done, Input::Cancel);
    assert!(!done.cancelled());
    assert_eq!(
        serde_json::to_value((&done.state.phases, &done.state.outcomes, &done.state.outbox))
            .unwrap(),
        before
    );
}

#[test]
fn cancellation_owned_producer_waits_real_wire_through_ack_and_replay() {
    let f = Fixture::new(WORKFLOW);
    let mut d = f.driver(2);
    let mut journal = vec![commit(&mut d, Input::Advance)];
    let action = ready(&d).remove(0);
    let task = ready_task(&action).unwrap().to_string();
    journal.push(commit(
        &mut d,
        Input::Claim {
            task: task.clone(),
            token: "actual-owner".into(),
        },
    ));
    let (wire, _) = f.execute(&action, "owned-seed");
    journal.push(commit(&mut d, Input::Cancel));
    assert!(d.cancelled());
    assert_eq!(d.admitted(), 1);
    assert!(!finished(&d));
    assert!(d.state.expansions.is_empty());
    let actions = cancel_actions(&d);
    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0].kind,ActionKind::Cancel{task:id,token} if id==&task && token=="actual-owner")
    );
    journal.push(commit(&mut d, Input::Cancel));
    assert_eq!(cancel_actions(&d), actions);
    let mut replay = f.driver(2);
    for record in &journal {
        replay
            .apply(&Transition::from_json(&serde_json::to_string(record).unwrap()).unwrap())
            .unwrap();
    }
    assert_eq!(cancel_actions(&replay), actions);
    let ack = commit(
        &mut d,
        Input::Ack {
            action: actions[0].id.clone(),
        },
    );
    replay.apply(&ack).unwrap();
    let repeat = commit(&mut d, Input::Cancel);
    replay.apply(&repeat).unwrap();
    assert!(cancel_actions(&d).is_empty());
    assert_eq!(d.admitted(), 1);
    assert!(!finished(&d));
    assert!(d
        .propose(Input::Claim {
            task: task.clone(),
            token: "replacement".into()
        })
        .is_err());
    let receipt = commit(
        &mut d,
        Input::Settle {
            task: task.clone(),
            token: "actual-owner".into(),
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        },
    );
    replay.apply(&receipt).unwrap();
    assert_eq!(d.admitted(), 0);
    assert!(finished(&d));
    assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Success);
    assert_eq!(
        d.outcome(FILE, "dynamic").unwrap().status,
        JobStatus::Cancelled
    );
    assert_eq!(
        serde_json::to_value(&d.state).unwrap(),
        serde_json::to_value(&replay.state).unwrap()
    );
}

#[test]
fn cancellation_expanded_members_joins_only_after_owned_receipt() {
    let f = Fixture::new(WORKFLOW);
    let mut d = f.driver(2);
    seed(&f, &mut d);
    let legs = ready(&d);
    claim(&mut d, &legs[0]);
    let (wire, _) = f.execute(&legs[0], "owned-leg");
    commit(&mut d, Input::Cancel);
    assert_eq!(d.admitted(), 1);
    assert!(!finished(&d));
    assert!(ready(&d).is_empty());
    assert!(d.outcome(FILE, "dynamic").is_none());
    assert_eq!(
        d.core_decision(ready_task(&legs[1]).unwrap()).unwrap().0,
        CoreDecision::UnstartedCancelled
    );
    let cancellation = cancel_actions(&d).remove(0);
    commit(
        &mut d,
        Input::Ack {
            action: cancellation.id,
        },
    );
    commit(&mut d, Input::Cancel);
    assert_eq!(d.admitted(), 1);
    assert!(cancel_actions(&d).is_empty());
    settle(&mut d, &legs[0], wire);
    assert!(finished(&d));
    assert_eq!(d.admitted(), 0);
    let joined = d.outcome(FILE, "dynamic").unwrap();
    assert_eq!(joined.status, JobStatus::Cancelled);
    assert_eq!(joined.outputs["value"], "v0");
    let live = &d.state.phases[ready_task(&legs[0]).unwrap()];
    assert!(matches!(live, Phase::Terminal(Terminal::Worker { .. })));
}

#[test]
fn cancellation_and_failfast_share_once_only_requests_in_both_orders() {
    let workflow = WORKFLOW
        .replace("fail-fast: false", "fail-fast: true")
        .replace(
            "{\"n\":1,\"os\":\"beta\"}]}",
            "{\"n\":1,\"os\":\"beta\"},{\"n\":2,\"os\":\"gamma\"}]}",
        )
        .replace("strategy.job-total }}' = 2", "strategy.job-total }}' = 3")
        .replace(
            "          test ! -e marker",
            "          if [ \"$LEG\" = 0 ]; then exit 1; fi\n          test ! -e marker",
        );
    for cancel_first in [false, true] {
        let f = Fixture::new(&workflow);
        let mut d = f.driver(3);
        seed(&f, &mut d);
        let legs = ready(&d);
        claim(&mut d, &legs[0]);
        claim(&mut d, &legs[1]);
        let (failure, _) = f.execute(&legs[0], "failing-owner");
        let (success, _) = f.execute(&legs[1], "live-owner");
        if cancel_first {
            commit(&mut d, Input::Cancel);
        }
        if cancel_first {
            for action in cancel_actions(&d) {
                commit(&mut d, Input::Ack { action: action.id });
            }
        }
        settle(&mut d, &legs[0], failure);
        if !cancel_first {
            for action in cancel_actions(&d) {
                commit(&mut d, Input::Ack { action: action.id });
            }
            commit(&mut d, Input::Cancel);
        }
        commit(&mut d, Input::Cancel);
        assert!(cancel_actions(&d).is_empty());
        assert_eq!(d.admitted(), 1);
        assert!(!finished(&d));
        let Phase::Terminal(queued) = &d.state.phases[ready_task(&legs[2]).unwrap()] else {
            panic!()
        };
        assert_eq!(
            queued.outcomes()[0].status,
            if cancel_first {
                JobStatus::Cancelled
            } else {
                JobStatus::Skipped
            }
        );
        settle(&mut d, &legs[1], success);
        assert_eq!(d.admitted(), 0);
        assert!(finished(&d));
        assert_eq!(
            d.outcome(FILE, "dynamic").unwrap().status,
            JobStatus::Failure
        );
    }
}

#[test]
fn claim_capacity_accounts_escaped_tokens_and_keeps_cancel_admissible() {
    let f = Fixture::new(WORKFLOW);
    let mut d = f.driver(2);
    d.state_byte_limit = 12 * 1024;
    commit(&mut d, Input::Advance);
    let action = ready(&d).remove(0);
    let task = ready_task(&action).unwrap().to_string();
    let before = serde_json::to_value(&d.state).unwrap();
    let rejected = d
        .propose(Input::Claim {
            task: task.clone(),
            token: "\0".repeat(1024),
        })
        .unwrap_err();
    assert!(rejected.to_string().contains("cancellation capacity"));
    assert_eq!(serde_json::to_value(&d.state).unwrap(), before);
    let owner = "\0".repeat(256);
    commit(
        &mut d,
        Input::Claim {
            task: task.clone(),
            token: owner.clone(),
        },
    );
    assert_eq!(d.admitted(), 1);
    let (wire, _) = f.execute(&action, "capacity-owner");
    commit(&mut d, Input::Cancel);
    assert_eq!(d.admitted(), 1);
    assert!(!finished(&d));
    assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
    let requested = cancel_actions(&d);
    assert_eq!(requested.len(), 1);
    assert!(matches!(&requested[0].kind,ActionKind::Cancel{token,..} if token==&owner));
    commit(
        &mut d,
        Input::Settle {
            task,
            token: owner,
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        },
    );
    assert_eq!(d.admitted(), 0);
    assert!(finished(&d));
}

#[test]
fn cancel_projection_reserves_existing_matrix_outputs_with_json_escapes() {
    let payload = "\\001".repeat(1024);
    let workflow = WORKFLOW.replace(
        "          echo \"value=v$LEG\" >> \"$GITHUB_OUTPUT\"",
        &format!("          printf 'value=%b\\n' '{payload}' >> \"$GITHUB_OUTPUT\""),
    );
    let f = Fixture::new(&workflow);
    let mut d = f.driver(2);
    d.state_byte_limit = 32 * 1024;
    seed(&f, &mut d);
    let legs = ready(&d);
    assert_eq!(legs.len(), 2);
    claim(&mut d, &legs[0]);
    let (wire, _) = f.execute(&legs[0], "escaped-result-worker");
    assert_eq!(wire.outcomes[0].status, JobStatus::Success);
    assert_eq!(wire.outcomes[0].outputs["value"], "\u{1}".repeat(1024));
    settle(&mut d, &legs[0], wire);
    assert!(d.outcome(FILE, "dynamic").is_none());
    let bytes = crate::expr::serialized_size(&d.state, d.state_byte_limit).unwrap();
    assert!(bytes + d.cancel_reserve(&d.state).unwrap() <= d.state_byte_limit);
    commit(&mut d, Input::Cancel);
    assert!(finished(&d));
    assert_eq!(d.admitted(), 0);
    let joined = d.outcome(FILE, "dynamic").unwrap();
    assert_eq!(joined.status, JobStatus::Cancelled);
    assert_eq!(joined.outputs["value"], "\u{1}".repeat(1024));
    assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
}

#[test]
fn oversized_real_result_is_explicit_storage_error_and_keeps_owner() {
    let value = "z".repeat(6000);
    let workflow=format!("on: push\njobs:\n  seed:\n    runs-on: [self-hosted, seed]\n    outputs:\n      value: ${{{{ steps.seed.outputs.value }}}}\n    steps:\n      - id: seed\n        run: printf 'value=%s\\n' '{value}' >> \"$GITHUB_OUTPUT\"\n  consumer:\n    needs: seed\n    runs-on: x\n    steps:\n      - run: exit 99\n");
    let f = Fixture::new(&workflow);
    let mut d = f.driver(1);
    d.state_byte_limit = 12 * 1024;
    commit(&mut d, Input::Advance);
    let action = ready(&d).remove(0);
    claim(&mut d, &action);
    let (wire, _) = f.execute(&action, "oversized-real-worker");
    assert_eq!(wire.outcomes[0].status, JobStatus::Success);
    let task = ready_task(&action).unwrap().to_string();
    let before = serde_json::to_value(&d.state).unwrap();
    let error = d
        .propose(Input::Settle {
            task: task.clone(),
            token: task,
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        })
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("worker result exceeds remaining state capacity"));
    assert_eq!(serde_json::to_value(&d.state).unwrap(), before);
    assert_eq!(d.admitted(), 1);
    // The valid external terminal receipt cannot be stored in this tiny State;
    // requesting cancellation cannot forge a smaller receipt or release owner.
    commit(&mut d, Input::Cancel);
    assert_eq!(d.admitted(), 1);
    assert!(!finished(&d));
    assert_eq!(cancel_actions(&d).len(), 1);
}

const ATOMIC_WORKFLOW: &str = r#"on: push
jobs:
  producer:
    runs-on: x
    outputs:
      matrix: ${{ steps.out.outputs.matrix }}
    steps:
      - id: out
        run: echo 'matrix={"n":[0,1]}' >> "$GITHUB_OUTPUT"
  first:
    needs: producer
    runs-on: initial
    outputs:
      matrix: ${{ steps.out.outputs.matrix }}
    steps:
      - id: out
        run: |
          echo first > shared.txt
          echo 'matrix=${{ needs.producer.outputs.matrix }}' >> "$GITHUB_OUTPUT"
  matrix:
    needs: first
    runs-on: ${{ needs.first.outputs.label }}
    name: 'member-${{ matrix.n }}'
    strategy:
      fail-fast: false
      max-parallel: 1
      matrix: ${{ fromJSON(needs.first.outputs.matrix) }}
    outputs:
      value: ${{ steps.out.outputs.value }}
    steps:
      - id: out
        run: |
          test -f shared.txt
          echo '${{ matrix.n }}' >> shared.txt
          echo 'value=v${{ matrix.n }}' >> "$GITHUB_OUTPUT"
  last:
    needs: matrix
    runs-on: last
    steps:
      - run: |
          test '${{ needs.matrix.outputs.value }}' = v1
          echo last >> shared.txt
  consumer:
    needs: [matrix, last]
    runs-on: x
    steps:
      - run: test '${{ needs.matrix.outputs.value }}' = v1
"#;
fn atomic_action(d: &Driver) -> ActionKind {
    ready(d)
        .into_iter()
        .find(|a| match a {
            ActionKind::Ready { task, .. } => d.plan.task(task).unwrap().jobs.len() > 1,
            _ => false,
        })
        .expect("atomic group ready")
}
fn atomic_start(f: &Fixture, d: &mut Driver) -> Vec<Transition> {
    let mut records = vec![commit(d, Input::Advance)];
    let producer = ready(d)
        .into_iter()
        .find(|a| match a {
            ActionKind::Ready { task, .. } => d.plan.task(task).unwrap().jobs == ["producer"],
            _ => false,
        })
        .unwrap();
    let record = d
        .propose(Input::Claim {
            task: ready_task(&producer).unwrap().into(),
            token: ready_task(&producer).unwrap().into(),
        })
        .unwrap();
    d.apply(&record).unwrap();
    records.push(record);
    let (wire, _) = f.execute(&producer, "producer");
    records.push(settle(d, &producer, wire));
    records
}
#[test]
fn atomic_group_real_dynamic_shared_workspace_exact_settlement_replay() {
    let f = Fixture::new(ATOMIC_WORKFLOW);
    let mut d = f.driver(2);
    let mut records = atomic_start(&f, &mut d);
    let action = atomic_action(&d);
    let id = ready_task(&action).unwrap();
    assert_eq!(d.plan.task(id).unwrap().jobs, ["first", "matrix", "last"]);
    assert!(!d.plan.task(id).unwrap().dynamic_matrix); // immutable planner flag
    assert_eq!(
        d.execution_mode(id).unwrap(),
        TaskExecutionMode::AtomicSharedWorkspace {
            placement: GroupPlacement::Unknown
        }
    );
    let requirements = d.requirements(id).unwrap();
    assert_eq!(requirements.len(), 3);
    assert_eq!(
        requirements[1].original,
        Some(json!("${{ needs.first.outputs.label }}"))
    );
    assert!(requirements.iter().all(|r| r.evaluated.is_none()));
    assert!(d.outcome(FILE, "first").is_none());
    records.push(commit(
        &mut d,
        Input::Claim {
            task: id.into(),
            token: id.into(),
        },
    ));
    assert_eq!(d.admitted(), 1);
    let (wire, root) = f.execute(&action, "group");
    assert_eq!(
        std::fs::read_to_string(root.join("shared.txt")).unwrap(),
        "first\n0\n1\nlast\n"
    );
    assert_eq!(
        wire.clone()
            .into_task_result(Path::new("."))
            .unwrap()
            .jobs
            .len(),
        4
    );
    for change in ["missing", "order", "status", "identity", "duplicate"] {
        let mut result = wire.clone().into_task_result(Path::new(".")).unwrap();
        match change {
            "missing" => {
                result.jobs.remove(1);
            }
            "order" => result.jobs.swap(1, 2),
            "status" => result.outcomes[1].status = JobStatus::Failure,
            "identity" => result.task.push_str("-other"),
            _ => result.jobs.push(result.jobs[1].clone()),
        }
        let bad = WireTaskResult::from_task_result(&result, Path::new(".")).unwrap();
        let before = digest(&d.state).unwrap();
        assert!(
            d.propose(Input::Settle {
                task: id.into(),
                token: id.into(),
                result_digest: bad.digest().unwrap(),
                result: Box::new(bad)
            })
            .is_err(),
            "{change}"
        );
        assert_eq!(digest(&d.state).unwrap(), before);
        assert_eq!(d.admitted(), 1);
    }
    records.push(settle(&mut d, &action, wire));
    assert_eq!(d.outcome(FILE, "matrix").unwrap().outputs["value"], "v1");
    assert_eq!(d.admitted(), 0);
    assert!(d.state.expansions.is_empty());
    let consumer = ready(&d).pop().unwrap();
    records.push(commit(
        &mut d,
        Input::Claim {
            task: ready_task(&consumer).unwrap().into(),
            token: ready_task(&consumer).unwrap().into(),
        },
    ));
    let (wire, _) = f.execute(&consumer, "consumer");
    records.push(settle(&mut d, &consumer, wire));
    let mut replay = f.driver(2);
    for record in records {
        replay.apply(&record).unwrap();
    }
    assert_eq!(digest(&d.state).unwrap(), digest(&replay.state).unwrap());
    assert!(d.state.finished);
}

#[test]
fn atomic_group_real_static_and_failed_invalid_empty_internal_matrices() {
    for (name, workflow, expected) in [
        (
            "static",
            ATOMIC_WORKFLOW.replace(
                "matrix: ${{ fromJSON(needs.first.outputs.matrix) }}",
                "matrix: {n: [0,1]}",
            ),
            JobStatus::Success,
        ),
        (
            "failed",
            ATOMIC_WORKFLOW.replace(
                "echo 'value=v${{ matrix.n }}'",
                "test '${{ matrix.n }}' != 0\n          echo 'value=v${{ matrix.n }}'",
            ),
            JobStatus::Failure,
        ),
        (
            "invalid",
            ATOMIC_WORKFLOW.replace("matrix={\"n\":[0,1]}", "matrix=broken"),
            JobStatus::Failure,
        ),
        (
            "empty",
            ATOMIC_WORKFLOW.replace("matrix={\"n\":[0,1]}", "matrix={\"n\":[]}"),
            JobStatus::Failure,
        ),
    ] {
        let f = Fixture::new(&workflow);
        let mut d = f.driver(2);
        atomic_start(&f, &mut d);
        let action = atomic_action(&d);
        claim(&mut d, &action);
        let (wire, _) = f.execute(&action, name);
        settle(&mut d, &action, wire);
        assert_eq!(
            d.outcome(FILE, "matrix").unwrap().status,
            expected,
            "{name}"
        );
        if expected == JobStatus::Failure {
            assert!(d.outcome(FILE, "last").unwrap().ancestor_failed);
        }
        assert!(d.state.expansions.is_empty());
    }
}

#[test]
fn atomic_group_cancel_inflight_or_late_real_receipt_retains_ownership_and_replays() {
    use std::io::Read;
    for in_flight in [false, true] {
        let signal_area = tempfile::tempdir_in("/private/tmp").unwrap();
        let signal = signal_area.path().join("running.fifo");
        assert!(ExecContext::new(Default::default())
            .command("/usr/bin/mkfifo")
            .arg(&signal)
            .status()
            .unwrap()
            .success());
        let workflow = if in_flight {
            ATOMIC_WORKFLOW.replace("          test -f shared.txt",&format!("          test -f shared.txt\n          echo running > '{}'\n          sleep 60",signal.display()))
        } else {
            ATOMIC_WORKFLOW.into()
        };
        let f = Fixture::new(&workflow);
        let mut d = f.driver(2);
        let mut records = atomic_start(&f, &mut d);
        let action = atomic_action(&d);
        let id = ready_task(&action).unwrap();
        records.push(commit(
            &mut d,
            Input::Claim {
                task: id.into(),
                token: id.into(),
            },
        ));
        let wire = std::thread::scope(|scope| {
            let mut worker = Some(scope.spawn(|| f.execute(&action, "owned-group").0));
            let completed = if in_flight {
                let mut line = String::new();
                std::fs::File::open(&signal)
                    .unwrap()
                    .read_to_string(&mut line)
                    .unwrap();
                assert_eq!(line, "running\n");
                None
            } else {
                Some(worker.take().unwrap().join().unwrap())
            };
            records.push(commit(&mut d, Input::Cancel));
            assert!(d.outcome(FILE, "first").is_none());
            assert!(d.outcome(FILE, "matrix").is_none());
            assert_eq!(d.admitted(), 1);
            assert!(!finished(&d));
            assert!(ready(&d).is_empty());
            let cancel = cancel_actions(&d).remove(0);
            records.push(commit(&mut d, Input::Ack { action: cancel.id }));
            records.push(commit(&mut d, Input::Cancel));
            assert!(cancel_actions(&d).is_empty());
            if let Some(wire) = completed {
                wire
            } else {
                f.ctx.cancel.cancel(); // Deliver only to this fixture's real process group.
                worker.take().unwrap().join().unwrap()
            }
        });
        records.push(settle(&mut d, &action, wire));
        assert_eq!(d.admitted(), 0);
        assert!(finished(&d));
        assert_eq!(
            d.outcome(FILE, "matrix").unwrap().status,
            if in_flight {
                JobStatus::Cancelled
            } else {
                JobStatus::Success
            }
        );
        assert!(matches!(
            d.state.phases[id],
            Phase::Terminal(Terminal::Worker { .. })
        ));
        let mut replay = f.driver(2);
        for record in records {
            replay.apply(&record).unwrap();
        }
        assert_eq!(digest(&d.state).unwrap(), digest(&replay.state).unwrap());
    }
}

#[test]
fn atomic_group_rejects_unsafe_partial_readiness_membership_source_and_runtime_context() {
    let f = Fixture::new(ATOMIC_WORKFLOW);
    let mut d = f.driver(2);
    let records = atomic_start(&f, &mut d);
    let group = d.plan.tasks.iter().find(|t| t.jobs.len() > 1).unwrap();
    for mutation in ["nodes", "order", "external"] {
        let mut plan = f.plan.clone();
        let task = plan.tasks.iter_mut().find(|t| t.id == group.id).unwrap();
        match mutation {
            "nodes" => {
                task.nodes.pop();
            }
            "order" => {
                task.jobs.swap(0, 1);
                task.nodes.swap(0, 1);
            }
            _ => task.depends_on.clear(),
        }
        plan.digest = plan.compute_digest().unwrap();
        let source = PinnedExecution::new(&plan, &f.manifest, &f.store)
            .unwrap()
            .evaluation_source()
            .unwrap();
        assert!(
            Driver::with_evaluation(
                &plan,
                DriverConfig {
                    schema_version: DRIVER_VERSION,
                    max_parallel: 2
                },
                &source,
                f.facts.clone()
            )
            .is_err(),
            "{mutation}"
        );
    }
    // Forged task partition, but unchanged real job graph: first -> matrix
    // -> last. Grouping first+last would require first's partial publication
    // to start the external matrix. Never repair this by inventing edges.
    let mut unsafe_plan = f.plan.clone();
    let at = unsafe_plan
        .tasks
        .iter()
        .position(|t| t.jobs.len() > 1)
        .unwrap();
    let original = unsafe_plan.tasks[at].clone();
    let mut outer = original.clone();
    outer.jobs = vec!["first".into(), "last".into()];
    outer.nodes = outer
        .jobs
        .iter()
        .map(|j| crate::dag::node_id(FILE, j))
        .collect();
    outer.id = format!("{FILE}::first+last");
    let mut middle = original;
    middle.jobs = vec!["matrix".into()];
    middle.nodes = vec![crate::dag::node_id(FILE, "matrix")];
    middle.id = format!("{FILE}::matrix");
    middle.dynamic_matrix = true;
    middle.grouped_reason = None;
    middle.workspace.shared_by_jobs = false;
    middle.depends_on = vec![outer.id.clone()];
    unsafe_plan.tasks[at] = outer;
    unsafe_plan.tasks.insert(at + 1, middle);
    let owners = vec![
        unsafe_plan.tasks[at].id.clone(),
        unsafe_plan.tasks[at + 1].id.clone(),
    ];
    for task in unsafe_plan.tasks.iter_mut().skip(at + 2) {
        task.depends_on = owners.clone();
    }
    unsafe_plan.digest = unsafe_plan.compute_digest().unwrap();
    assert_eq!(unsafe_plan.graph, f.plan.graph);
    let source = PinnedExecution::new(&unsafe_plan, &f.manifest, &f.store)
        .unwrap()
        .evaluation_source()
        .unwrap();
    let error = Driver::with_evaluation(
        &unsafe_plan,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: 2,
        },
        &source,
        f.facts.clone(),
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("unsafe atomic/external readiness"));
    let other = Fixture::new(&ATOMIC_WORKFLOW.replace("runs-on: initial", "runs-on: different"));
    let source = other.input().evaluation_source().unwrap();
    assert!(Driver::with_evaluation(
        &f.plan,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: 2
        },
        &source,
        f.facts.clone()
    )
    .is_err());
    let mut other_driver = other.driver(2);
    assert!(other_driver.apply(&records[0]).is_err());
    let runtime = Fixture::new(&ATOMIC_WORKFLOW.replace(
        "fromJSON(needs.first.outputs.matrix)",
        "fromJSON(runner.temp)",
    ));
    let error = Driver::with_evaluation(
        &runtime.plan,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: 2,
        },
        &runtime.input().evaluation_source().unwrap(),
        runtime.facts.clone(),
    )
    .unwrap_err();
    assert!(error.downcast_ref::<AtomicGroupUnsupported>().is_some());
}

#[test]
fn atomic_group_bounded_membership_keeps_real_receipt_and_permit_on_unsupported_size() {
    // The real worker expands 4097 skipped entries. Core's replay bound cannot
    // authenticate that membership, so it retains ownership rather than
    // accepting a synthetic one-report matrix error.
    let matrix = json!({"n":(0..4097).collect::<Vec<_>>()}).to_string();
    let workflow = ATOMIC_WORKFLOW
        .replace("matrix={\"n\":[0,1]}", &format!("matrix={matrix}"))
        .replace(
            "    name: 'member-${{ matrix.n }}'",
            "    name: 'member-${{ matrix.n }}'\n    if: false",
        );
    let f = Fixture::new(&workflow);
    let mut d = f.driver(1);
    atomic_start(&f, &mut d);
    let action = atomic_action(&d);
    claim(&mut d, &action);
    let (wire, _) = f.execute(&action, "real-oversize-membership");
    let result = wire.clone().into_task_result(Path::new(".")).unwrap();
    assert_eq!(
        result.jobs.iter().filter(|r| r.job == "matrix").count(),
        4097
    );
    let before = digest(&d.state).unwrap();
    let error = d
        .propose(Input::Settle {
            task: ready_task(&action).unwrap().into(),
            token: ready_task(&action).unwrap().into(),
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        })
        .unwrap_err();
    assert!(error.downcast_ref::<AtomicGroupUnsupported>().is_some());
    assert_eq!(digest(&d.state).unwrap(), before);
    assert_eq!(d.admitted(), 1);
    commit(&mut d, Input::Cancel);
    assert_eq!(d.admitted(), 1);
    assert!(!finished(&d));
}

#[test]
fn atomic_group_shared_evaluator_bounds_cartesian_expression_names_and_context_copies() {
    let f = Fixture::new(ATOMIC_WORKFLOW);
    let mut spec = f.plan.workflows[0].job("matrix").unwrap().to_job();
    let mut needs = Map::new();
    needs.insert("first".into(),json!({"result":"success","outputs":{"matrix":json!({"n":(0..100).collect::<Vec<_>>(),"m":(0..100).collect::<Vec<_>>()} ).to_string()}}));
    assert!(crate::job_evaluate::evaluate_atomic(&spec, json!({}), &needs, false, false).is_err());
    needs["first"]["outputs"]["matrix"] = json!("{\"n\":[0,1]}");
    spec.name = Some("${{ format('{0}{0}', needs.first.outputs.big) }}".into());
    needs["first"]["outputs"]["big"] = json!("x".repeat(600_000));
    assert!(crate::job_evaluate::evaluate_atomic(&spec, json!({}), &needs, false, false).is_err());
    spec.name = None;
    needs["first"]["outputs"]["matrix"] = json!("{\"n\":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}");
    assert!(crate::job_evaluate::evaluate_atomic(&spec, json!({}), &needs, false, false).is_err());
    // Malformed expression data remains a genuine worker error, and a false
    // condition still preserves legacy expansion-before-if cardinality.
    needs["first"]["outputs"]
        .as_object_mut()
        .unwrap()
        .remove("big");
    needs["first"]["outputs"]["matrix"] = json!("broken");
    let evaluated =
        crate::job_evaluate::evaluate_atomic(&spec, json!({}), &needs, false, false).unwrap();
    assert!(matches!(
        evaluated.entries[0].decision,
        crate::job_evaluate::Decision::Error(_)
    ));
    spec.cond = Some("false".into());
    needs["first"]["outputs"]["matrix"] = json!("{\"n\":[0,1]}");
    let evaluated =
        crate::job_evaluate::evaluate_atomic(&spec, json!({}), &needs, false, false).unwrap();
    assert_eq!(evaluated.entries.len(), 2);
    assert!(evaluated
        .entries
        .iter()
        .all(|e| matches!(e.decision, crate::job_evaluate::Decision::Skip(_))));
}

#[test]
fn atomic_group_nonmatrix_worker_runtime_if_and_name_remain_supported() {
    let workflow=ATOMIC_WORKFLOW.replace("    strategy:\n      fail-fast: false\n      max-parallel: 1\n      matrix: ${{ fromJSON(needs.first.outputs.matrix) }}\n", "")
        .replace("name: 'member-${{ matrix.n }}'", "name: '${{ runner.os }}'\n    if: runner.os != ''")
        .replace("echo 'value=v${{ matrix.n }}'", "echo 'value=v1'");
    let f = Fixture::new(&workflow);
    let mut d = f.driver(2);
    atomic_start(&f, &mut d);
    let action = atomic_action(&d);
    claim(&mut d, &action);
    let (wire, _) = f.execute(&action, "nonmatrix-runtime");
    settle(&mut d, &action, wire);
    assert_eq!(
        d.outcome(FILE, "matrix").unwrap().status,
        JobStatus::Success
    );
}

fn abort_input(task: &str, reason: HostFailureReason) -> Input {
    Input::AbortBeforeExecution {
        task: task.into(),
        token: task.into(),
        reason,
    }
}
fn mark_input(task: &str) -> Input {
    Input::MarkDispatched {
        task: task.into(),
        token: task.into(),
    }
}
fn assert_rejected_unchanged(d: &Driver, input: Input) {
    let before = serde_json::to_value(&d.state).unwrap();
    assert!(d.propose(input).is_err());
    assert_eq!(serde_json::to_value(&d.state).unwrap(), before);
}

#[test]
fn host_abort_typed_identity_cancel_orders_and_replay() {
    for reason in [
        HostFailureReason::WorkspaceCopy,
        HostFailureReason::Lock,
        HostFailureReason::Setup,
        HostFailureReason::Provenance,
        HostFailureReason::ExecutorPreparation,
    ] {
        for cancel_first in [false, true] {
            let f = Fixture::new(WORKFLOW);
            let mut d = f.driver(2);
            let mut records = vec![commit(&mut d, Input::Advance)];
            let action = ready(&d).remove(0);
            let task = ready_task(&action).unwrap().to_string();
            records.push(commit(
                &mut d,
                Input::Claim {
                    task: task.clone(),
                    token: task.clone(),
                },
            ));
            assert_rejected_unchanged(
                &d,
                Input::AbortBeforeExecution {
                    task: task.clone(),
                    token: "wrong-owner".into(),
                    reason,
                },
            );
            assert_rejected_unchanged(&d, abort_input("unknown-task", reason));
            if cancel_first {
                records.push(commit(&mut d, Input::Cancel));
                let requested = cancel_actions(&d);
                assert_eq!(requested.len(), 1);
                records.push(commit(
                    &mut d,
                    Input::Ack {
                        action: requested[0].id.clone(),
                    },
                ));
                assert_eq!(d.admitted(), 1);
                assert_rejected_unchanged(&d, mark_input(&task));
            }
            let before = serde_json::to_value(&d.state).unwrap();
            let aborted = d.propose(abort_input(&task, reason)).unwrap();
            assert_eq!(serde_json::to_value(&d.state).unwrap(), before);
            let mut tampered = serde_json::to_value(&aborted).unwrap();
            tampered["state_digest"] = json!("forged");
            assert!(d
                .apply(&Transition::from_json(&tampered.to_string()).unwrap())
                .is_err());
            assert_eq!(serde_json::to_value(&d.state).unwrap(), before);
            d.apply(&aborted).unwrap();
            records.push(aborted.clone());
            assert!(d.apply(&aborted).is_err()); // exact record is not an extra transition
            assert_eq!(d.admitted(), 0);
            assert!(cancel_actions(&d).is_empty());
            let expected = if cancel_first {
                CoreDecision::UnstartedCancelled
            } else {
                CoreDecision::HostFailure
            };
            assert_eq!(d.core_decision(&task).unwrap().0, expected);
            assert_eq!(d.host_failure_reason(&task), Some(reason));
            assert!(matches!(
                &d.state.phases[&task],
                Phase::Terminal(Terminal::HostAbort { .. })
            ));
            assert_eq!(
                d.outcome(FILE, "seed").unwrap().status,
                if cancel_first {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failure
                }
            );
            records.push(commit(&mut d, abort_input(&task, reason))); // exact assertion idempotent
            let different = if reason == HostFailureReason::Lock {
                HostFailureReason::Setup
            } else {
                HostFailureReason::Lock
            };
            assert_rejected_unchanged(&d, abort_input(&task, different));
            assert_rejected_unchanged(
                &d,
                Input::AbortBeforeExecution {
                    task: task.clone(),
                    token: "different-owner".into(),
                    reason,
                },
            );
            assert_rejected_unchanged(&d, mark_input(&task));
            let (wire, _) = f.execute(&action, "contradictory-late-worker");
            assert_rejected_unchanged(
                &d,
                Input::Settle {
                    task: task.clone(),
                    token: task.clone(),
                    result_digest: wire.digest().unwrap(),
                    result: Box::new(wire),
                },
            );
            records.push(commit(&mut d, Input::Cancel));
            assert_eq!(d.core_decision(&task).unwrap().0, expected);
            let mut replay = f.driver(2);
            for record in records {
                let text = serde_json::to_string(&record).unwrap();
                replay
                    .apply(&Transition::from_json(&text).unwrap())
                    .unwrap();
            }
            assert_eq!(
                serde_json::to_value(&replay.state).unwrap(),
                serde_json::to_value(&d.state).unwrap()
            );
        }
    }
}

#[test]
fn dispatch_marker_never_releases_owner_and_actual_terminal_is_stronger() {
    for marked in [false, true] {
        let f = Fixture::new(WORKFLOW);
        let mut d = f.driver(2);
        let mut records = vec![commit(&mut d, Input::Advance)];
        let action = ready(&d).remove(0);
        let task = ready_task(&action).unwrap().to_string();
        records.push(commit(
            &mut d,
            Input::Claim {
                task: task.clone(),
                token: task.clone(),
            },
        ));
        let stale = d.propose(mark_input(&task)).unwrap();
        assert_rejected_unchanged(
            &d,
            Input::MarkDispatched {
                task: task.clone(),
                token: "wrong".into(),
            },
        );
        if marked {
            let before = serde_json::to_value(&d.state).unwrap();
            let record = d.propose(mark_input(&task)).unwrap();
            assert_eq!(serde_json::to_value(&d.state).unwrap(), before);
            d.apply(&record).unwrap();
            records.push(record);
            records.push(commit(&mut d, mark_input(&task)));
            assert_rejected_unchanged(
                &d,
                abort_input(&task, HostFailureReason::ExecutorPreparation),
            );
        }
        records.push(commit(&mut d, Input::Cancel));
        assert!(d.apply(&stale).is_err());
        if marked {
            records.push(commit(&mut d, mark_input(&task))); // idempotent marker, NOT resend authorization
        } else {
            assert_rejected_unchanged(&d, mark_input(&task));
        }
        let action_id = cancel_actions(&d)[0].id.clone();
        records.push(commit(&mut d, Input::Ack { action: action_id }));
        assert_eq!(d.admitted(), 1);
        let (wire, _) = f.execute(&action, "real-terminal");
        records.push(settle(&mut d, &action, wire));
        assert_eq!(d.outcome(FILE, "seed").unwrap().status, JobStatus::Success);
        assert_eq!(d.admitted(), 0);
        assert!(finished(&d));
        let mut replay = f.driver(2);
        for record in records {
            replay.apply(&record).unwrap();
        }
        assert_eq!(
            serde_json::to_value(&d.state).unwrap(),
            serde_json::to_value(&replay.state).unwrap()
        );
    }
}

#[test]
fn real_workspace_preparation_failure_drains_before_abort_and_preserves_source_and_receipt() {
    let f = Fixture::new(ATOMIC_WORKFLOW);
    let plan_digest = f.plan.digest.clone();
    let manifest = serde_json::to_value(&f.manifest).unwrap();
    let mut d = f.driver(2);
    let mut records = atomic_start(&f, &mut d);
    let producer = f
        .plan
        .tasks
        .iter()
        .find(|t| t.jobs == ["producer"])
        .unwrap();
    let producer_terminal = serde_json::to_value(&d.state.phases[&producer.id]).unwrap();
    let action = atomic_action(&d);
    let task = ready_task(&action).unwrap().to_string();
    records.push(commit(
        &mut d,
        Input::Claim {
            task: task.clone(),
            token: task.clone(),
        },
    ));
    let ws = f.workspace("failed-preparation");
    std::fs::create_dir_all(ws.root.parent().unwrap()).unwrap();
    std::fs::write(&ws.root, "root is a file, not a checkout directory").unwrap();
    // Real source materialization failure, joined before asserting no dispatch.
    std::thread::scope(|scope| {
        let preparing = scope.spawn(|| f.input().materialize_workspace(ws.clone()));
        assert!(preparing.join().unwrap().is_err());
    });
    assert!(!ws.root.join("shared.txt").exists()); // first actual command never ran
    assert!(!ws.root.join("marker").exists());
    records.push(commit(
        &mut d,
        abort_input(&task, HostFailureReason::WorkspaceCopy),
    ));
    assert_eq!(d.core_decision(&task).unwrap().0, CoreDecision::HostFailure);
    for job in ["first", "matrix", "last"] {
        let outcome = d.outcome(FILE, job).unwrap();
        assert_eq!(outcome.status, JobStatus::Failure);
        assert!(outcome.outputs.is_empty());
        assert_eq!(outcome.ancestor_failed, job != "first");
    }
    assert_eq!(
        serde_json::to_value(&d.state.phases[&producer.id]).unwrap(),
        producer_terminal
    );
    assert_eq!(f.plan.digest, plan_digest);
    assert_eq!(serde_json::to_value(&f.manifest).unwrap(), manifest);
    let consumer = ready(&d).remove(0);
    records.push(commit(
        &mut d,
        Input::Claim {
            task: ready_task(&consumer).unwrap().into(),
            token: ready_task(&consumer).unwrap().into(),
        },
    ));
    records.push(commit(&mut d, mark_input(ready_task(&consumer).unwrap())));
    let (wire, _) = f.execute(&consumer, "host-abort-consumer");
    assert_eq!(wire.outcomes[0].status, JobStatus::Skipped);
    records.push(settle(&mut d, &consumer, wire));
    assert!(finished(&d));
    let mut replay = f.driver(2);
    for record in records {
        replay.apply(&record).unwrap();
    }
    assert_eq!(
        serde_json::to_value(&d.state).unwrap(),
        serde_json::to_value(&replay.state).unwrap()
    );
}

#[test]
fn never_dispatched_group_abort_capacity_survives_cancel_and_ack() {
    let f = Fixture::new(ATOMIC_WORKFLOW);
    let mut d = f.driver(2);
    atomic_start(&f, &mut d);
    let action = atomic_action(&d);
    let task = ready_task(&action).unwrap().to_string();
    let token = "\0".repeat(256);
    let input = Input::Claim {
        task: task.clone(),
        token: token.clone(),
    };
    let candidate = d.compute(&input).unwrap();
    let required = crate::expr::serialized_size(&candidate, MAX_WIRE_BYTES).unwrap()
        + d.cancel_reserve(&candidate).unwrap();
    d.state_byte_limit = required - 1;
    assert_rejected_unchanged(&d, input.clone());
    d.state_byte_limit = required;
    commit(&mut d, input);
    let reserve = d.cancel_reserve(&d.state).unwrap();
    assert!(reserve > 0);
    commit(&mut d, Input::Cancel);
    let request = cancel_actions(&d).remove(0);
    commit(&mut d, Input::Ack { action: request.id });
    assert_eq!(d.admitted(), 1);
    assert!(d.cancel_reserve(&d.state).unwrap() > 0);
    commit(
        &mut d,
        Input::AbortBeforeExecution {
            task: task.clone(),
            token,
            reason: HostFailureReason::Provenance,
        },
    );
    assert_eq!(
        d.core_decision(&task).unwrap().0,
        CoreDecision::UnstartedCancelled
    );
    assert_eq!(d.admitted(), 0);
    assert!(finished(&d));
    assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
}

#[test]
fn host_abort_matrix_failfast_and_cancelled_sibling_join_exact_membership() {
    let workflow = WORKFLOW.replace("fail-fast: false", "fail-fast: true");
    let f = Fixture::new(&workflow);
    let mut d = f.driver(2);
    seed(&f, &mut d);
    let actions = ready(&d);
    assert_eq!(actions.len(), 2);
    for action in &actions {
        claim(&mut d, action);
    }
    let first = ready_task(&actions[0]).unwrap();
    let second = ready_task(&actions[1]).unwrap();
    commit(&mut d, abort_input(first, HostFailureReason::Setup));
    assert_eq!(d.admitted(), 1);
    assert!(d.outcome(FILE, "dynamic").is_none());
    assert_rejected_unchanged(&d, mark_input(second));
    let request = cancel_actions(&d).remove(0);
    commit(&mut d, Input::Ack { action: request.id });
    commit(
        &mut d,
        abort_input(second, HostFailureReason::ExecutorPreparation),
    );
    assert_eq!(
        d.core_decision(second).unwrap().0,
        CoreDecision::UnstartedCancelled
    );
    assert_eq!(
        d.outcome(FILE, "dynamic").unwrap().status,
        JobStatus::Failure
    );
    assert_eq!(d.admitted(), 0);
    let Phase::Terminal(Terminal::HostAbort { outcomes, .. }) = &d.state.phases[first] else {
        panic!()
    };
    assert_eq!(
        outcomes[0].variant,
        d.task(&d.state, first)
            .unwrap()
            .variant
            .as_ref()
            .map(|v| v.index)
    );
}

#[test]
fn never_dispatched_abort_after_cancel_preserves_real_matrix_outputs_at_capacity() {
    let payload = "\\001".repeat(1024);
    let workflow = WORKFLOW.replace(
        "          echo \"value=v$LEG\" >> \"$GITHUB_OUTPUT\"",
        &format!("          printf 'value=%b\\n' '{payload}' >> \"$GITHUB_OUTPUT\""),
    );
    let f = Fixture::new(&workflow);
    let mut d = f.driver(2);
    seed(&f, &mut d);
    let legs = ready(&d);
    claim(&mut d, &legs[0]);
    let (wire, _) = f.execute(&legs[0], "abort-retained-real-output");
    let real_receipt = wire.digest().unwrap();
    settle(&mut d, &legs[0], wire);
    let first = ready_task(&legs[0]).unwrap();
    let task = ready_task(&legs[1]).unwrap().to_string();
    let input = Input::Claim {
        task: task.clone(),
        token: task.clone(),
    };
    let candidate = d.compute(&input).unwrap();
    d.state_byte_limit = crate::expr::serialized_size(&candidate, MAX_WIRE_BYTES).unwrap()
        + d.cancel_reserve(&candidate).unwrap();
    commit(&mut d, input);
    commit(&mut d, Input::Cancel);
    let request = cancel_actions(&d).remove(0);
    commit(&mut d, Input::Ack { action: request.id });
    assert_eq!(d.admitted(), 1);
    commit(&mut d, abort_input(&task, HostFailureReason::Lock));
    assert!(finished(&d));
    assert_eq!(d.admitted(), 0);
    let joined = d.outcome(FILE, "dynamic").unwrap();
    assert_eq!(joined.status, JobStatus::Cancelled);
    assert_eq!(joined.outputs["value"], "\u{1}".repeat(1024));
    assert!(
        matches!(&d.state.phases[first], Phase::Terminal(Terminal::Worker { result_digest, .. }) if result_digest == &real_receipt)
    );
    assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
}

// Dependency-free real libtest processes. The discovery and execution commands
// use a fixture-owned offline target, never the repository's build directory.
fn shard_fixture(fail_fast: bool, preparation: &str, test_body: &str) -> Fixture {
    shard_fixture_output(fail_fast, preparation, test_body, "agreed")
}
fn shard_fixture_output(
    fail_fast: bool,
    preparation: &str,
    test_body: &str,
    output: &str,
) -> Fixture {
    use crate::shard::adapters::{run_invocation, CargoTest, Safety};
    use crate::shard::{partition_tests, History, PartitionOpts, RunnerAdapter};
    let workflow = format!(
        r#"on: push
jobs:
  test:
    runs-on: [shard-runner]
    outputs:
      value: ${{{{ steps.out.outputs.value }}}}
    steps:
      - id: out
        run: echo "value={output}" >> "$GITHUB_OUTPUT"
      - run: {preparation}
      - run: cargo test --offline --lib
  matrix:
    runs-on: unrelated
    strategy:
      max-parallel: 1
      matrix:
        n: [0, 1]
    steps:
      - run: 'true'
  consumer:
    runs-on: consumer
    needs: test
    steps:
      - run: test '${{{{ needs.test.outputs.value }}}}' = agreed
"#
    );
    let mut fixture = Fixture::new_files(&workflow, &[
        ("Cargo.toml", "[package]\nname='driver-shard-fixture'\nversion='0.1.0'\nedition='2021'\n[workspace]\n"),
        ("src/lib.rs", test_body),
        (".github/workflows/other.yml", "on: push\njobs:\n  test:\n    runs-on: other\n    steps:\n      - run: 'true'\n"),
    ]);
    fixture.ctx = ExecContext::new(
        [
            (
                "PATH".into(),
                format!(
                    "{}:/usr/bin:/bin",
                    Path::new(env!("CARGO")).parent().unwrap().display()
                ),
            ),
            ("HOME".into(), fixture.area.path().display().to_string()),
            (
                "CARGO_TARGET_DIR".into(),
                fixture
                    .area
                    .path()
                    .join("cargo-target")
                    .display()
                    .to_string(),
            ),
            (
                "CARGO_HOME".into(),
                fixture.area.path().join("cargo-home").display().to_string(),
            ),
            ("CARGO_BUILD_JOBS".into(), "2".into()),
            ("CARGO_INCREMENTAL".into(), "0".into()),
            ("CARGO_PROFILE_DEV_DEBUG".into(), "0".into()),
            ("CARGO_PROFILE_TEST_DEBUG".into(), "0".into()),
        ]
        .into(),
    );
    let adapter = CargoTest::new("1.100.0", &["--offline", "--lib"], Safety::default());
    let discovery = run_invocation(
        &fixture.ctx,
        &adapter.discover_command(),
        &fixture.area.path().join("source"),
    )
    .unwrap();
    let discovered = adapter.discover(&discovery).unwrap();
    assert_eq!(discovered.tests, ["alpha", "beta"]);
    assert!(discovered.complete);
    let partition = partition_tests(
        &adapter,
        &discovered,
        &History::default(),
        2,
        &PartitionOpts::default(),
    );
    fixture.plan = fixture
        .plan
        .with_shards(&[crate::dag::ShardRequest {
            workflow: FILE.into(),
            job: "test".into(),
            plan: partition,
            step: 2,
            dir: "".into(),
            discovered,
            fail_fast,
        }])
        .unwrap();
    assert!(fixture.plan.shard_groups.iter().any(|g| g.split));
    fixture
}
const SHARD_TESTS: &str = "#[test] fn alpha() {}\n#[test] fn beta() {}\n";
fn mark(d: &mut Driver, action: &ActionKind) {
    let task = ready_task(action).unwrap().to_string();
    commit(
        d,
        Input::MarkDispatched {
            task: task.clone(),
            token: task,
        },
    );
}
fn shard_actions(d: &Driver) -> Vec<ActionKind> {
    ready(d)
        .into_iter()
        .filter(|a| d.plan.task(ready_task(a).unwrap()).unwrap().shard.is_some())
        .collect()
}
fn replay_shards(f: &Fixture, d: &Driver, records: &[Transition], limit: usize) {
    let mut restored = f.driver(limit);
    for record in records {
        restored
            .apply(&Transition::from_json(&serde_json::to_string(record).unwrap()).unwrap())
            .unwrap();
    }
    assert_eq!(digest(&restored.state).unwrap(), digest(&d.state).unwrap());
}
#[test]
fn frozen_shards_real_reverse_join_and_static_matrix_coexist() {
    let f = shard_fixture(false, "'true'", SHARD_TESTS);
    let mut d = f.driver(2);
    let mut journal = vec![commit(&mut d, Input::Advance)];
    let actions = shard_actions(&d);
    assert_eq!(actions.len(), 2);
    assert!(ready(&d)
        .iter()
        .any(|a| ready_task(a) == Some(".github/workflows/other.yml::test")));
    assert!(d.outcome(".github/workflows/other.yml", "test").is_none());

    for action in &actions {
        let task = ready_task(action).unwrap().to_string();
        journal.push(commit(
            &mut d,
            Input::Claim {
                task: task.clone(),
                token: task.clone(),
            },
        ));
        journal.push(commit(
            &mut d,
            Input::MarkDispatched {
                task: task.clone(),
                token: task,
            },
        ));
    }
    let matrix = ready(&d)
        .into_iter()
        .find(|a| {
            d.plan
                .task(ready_task(a).unwrap())
                .unwrap()
                .variant
                .is_some()
        })
        .unwrap();
    assert!(d
        .propose(Input::Claim {
            task: ready_task(&matrix).unwrap().into(),
            token: "blocked".into()
        })
        .is_err());
    for (index, action) in actions.iter().rev().enumerate() {
        let (wire, _) = f.execute(action, &format!("shard-{index}"));
        assert!(wire.shard.as_ref().unwrap().parsed);
        journal.push(settle(&mut d, action, wire));
        if index == 0 {
            assert!(d.outcome(FILE, "test").is_none());
        }
    }
    let group = &f.plan.shard_groups.iter().find(|g| g.split).unwrap().id;
    let join = d.shard_join(group).unwrap();
    assert!(join.aggregate.ok(), "{:?}", join.aggregate.problems());
    assert_eq!(join.outcome.status, JobStatus::Success);
    assert!(join.core_members.is_empty());
    assert_eq!(join.outcome.outputs["value"], "agreed");
    // Matrix concurrency remains separate from the released shard lifetimes.
    claim(&mut d, &matrix);
    let matrix2 = ready(&d)
        .into_iter()
        .find(|a| {
            d.plan
                .task(ready_task(a).unwrap())
                .unwrap()
                .variant
                .is_some()
        })
        .unwrap();
    assert!(d
        .propose(Input::Claim {
            task: ready_task(&matrix2).unwrap().into(),
            token: "matrix-cap".into()
        })
        .is_err());
    // Only journaled shard transitions are compared before unrelated matrix work.
    let mut shard_only = f.driver(2);
    for record in &journal {
        shard_only.apply(record).unwrap();
    }
    replay_shards(&f, &shard_only, &journal, 2);
    let consumer = ready(&d)
        .into_iter()
        .find(|a| ready_task(a).unwrap().ends_with("::consumer"))
        .unwrap();
    claim(&mut d, &consumer);
    mark(&mut d, &consumer);
    let (wire, _) = f.execute(&consumer, "consumer");
    assert_eq!(wire.outcomes[0].status, JobStatus::Success);
    settle(&mut d, &consumer, wire);
}
#[test]
fn frozen_shards_genuine_missing_coverage_and_output_disagreement_fail_join() {
    // A real pre-test step changes the suite after its actual frozen discovery.
    let f = shard_fixture(false, "sed -i '' '/fn beta/d' src/lib.rs", SHARD_TESTS);
    let mut d = f.driver(2);
    commit(&mut d, Input::Advance);
    for (i, action) in shard_actions(&d).iter().enumerate() {
        claim(&mut d, action);
        mark(&mut d, action);
        let (wire, _) = f.execute(action, &format!("coverage-{i}"));
        settle(&mut d, action, wire);
    }
    let group = &f.plan.shard_groups.iter().find(|g| g.split).unwrap().id;
    let join = d.shard_join(group).unwrap();
    assert!(!join.aggregate.ok());
    assert!(join.aggregate.problems().iter().any(|p| p.contains("beta")));
    assert_eq!(join.outcome.status, JobStatus::Failure);
    assert_eq!(d.admitted(), 0);
    // Different genuine processes emit different values from the declared step.
    let f = shard_fixture_output(false, "'true'", SHARD_TESTS, "$$");
    let mut d = f.driver(2);
    commit(&mut d, Input::Advance);
    for (i, action) in shard_actions(&d).iter().enumerate() {
        claim(&mut d, action);
        mark(&mut d, action);
        let (wire, _) = f.execute(action, &format!("outputs-{i}"));
        settle(&mut d, action, wire);
    }
    let join = d
        .shard_join(&f.plan.shard_groups.iter().find(|g| g.split).unwrap().id)
        .unwrap();
    assert!(join.aggregate.ok());
    assert_eq!(join.outcome.status, JobStatus::Failure);
    assert!(join.diagnostic.unwrap().contains("different values"));
}
#[test]
fn frozen_shards_real_failure_core_skip_and_cancel_ack_keep_owner() {
    let f = shard_fixture(true, "'true'", "#[test] fn alpha() { panic!(\"real failure\"); }\n#[test] fn beta() { panic!(\"real failure\"); }\n");
    let mut d = f.driver(2);
    commit(&mut d, Input::Advance);
    let actions = shard_actions(&d);
    claim(&mut d, &actions[0]);
    mark(&mut d, &actions[0]);
    let (wire, _) = f.execute(&actions[0], "failed-shard");
    assert_eq!(wire.outcomes[0].status, JobStatus::Failure);
    settle(&mut d, &actions[0], wire);
    assert_eq!(
        d.core_decision(ready_task(&actions[1]).unwrap()).unwrap().0,
        CoreDecision::UnstartedSkipped
    );
    let group = &f.plan.shard_groups.iter().find(|g| g.split).unwrap().id;
    let join = d.shard_join(group).unwrap();
    assert_eq!(join.core_members.len(), 1);
    assert_eq!(join.aggregate.missing.len(), 1);
    assert_eq!(join.outcome.status, JobStatus::Failure);
    // Claimed but undispatched cancellation retains ownership through ACK.
    let f = shard_fixture(false, "'true'", SHARD_TESTS);
    let mut d = f.driver(2);
    let mut journal = vec![commit(&mut d, Input::Advance)];
    let action = shard_actions(&d)[0].clone();
    let task = ready_task(&action).unwrap().to_string();
    journal.push(commit(
        &mut d,
        Input::Claim {
            task: task.clone(),
            token: task.clone(),
        },
    ));
    journal.push(commit(&mut d, Input::Cancel));
    let cancel = d
        .pending_actions()
        .iter()
        .find(|a| matches!(a.kind, ActionKind::Cancel { .. }))
        .unwrap()
        .id
        .clone();
    journal.push(commit(&mut d, Input::Ack { action: cancel }));
    assert_eq!(d.admitted(), 1);
    assert!(d.outcome(FILE, "test").is_none());
    journal.push(commit(
        &mut d,
        Input::AbortBeforeExecution {
            task: task.clone(),
            token: task,
            reason: HostFailureReason::ExecutorPreparation,
        },
    ));
    assert_eq!(d.admitted(), 0);
    let join = d
        .shard_join(&f.plan.shard_groups.iter().find(|g| g.split).unwrap().id)
        .unwrap();
    assert_eq!(join.aggregate.missing.len(), 2);
    assert_eq!(join.core_members.len(), 2);
    assert_eq!(join.outcome.status, JobStatus::Cancelled);
    replay_shards(&f, &d, &journal, 2);
}

#[test]
fn frozen_shards_typed_membership_and_receipt_identity_reject_without_mutation() {
    let f = shard_fixture(false, "'true'", SHARD_TESTS);
    for shape in [
        "dynamic",
        "grouped",
        "variant",
        "missing",
        "index",
        "count",
        "assignment",
        "manifest",
        "runner",
        "step",
        "order",
        "producer",
    ] {
        let mut plan = f.plan.clone();
        let i = plan.tasks.iter().position(|t| t.shard.is_some()).unwrap();
        match shape {
            "dynamic" => plan.tasks[i].dynamic_matrix = true,
            "grouped" => plan.tasks[i].jobs.push("consumer".into()),
            "variant" => plan.tasks[i].variant = plan.tasks.iter().find_map(|t| t.variant.clone()),
            "missing" => {
                plan.tasks.remove(i);
            }
            "index" => plan.tasks[i].shard.as_mut().unwrap().index = 99,
            "count" => plan.tasks[i].shard.as_mut().unwrap().count = 99,
            "assignment" => plan
                .shard_groups
                .iter_mut()
                .find(|g| g.split)
                .unwrap()
                .assignments[0]
                .push("other".into()),
            "manifest" => plan
                .shard_groups
                .iter_mut()
                .find(|g| g.split)
                .unwrap()
                .manifest
                .push("other".into()),
            "runner" => plan.tasks[i].shard.as_mut().unwrap().runner.runner = "unknown".into(),
            "step" => plan.tasks[i].shard.as_mut().unwrap().step = 0,
            "order" => plan
                .shard_groups
                .iter_mut()
                .find(|g| g.split)
                .unwrap()
                .shards
                .reverse(),
            _ => {
                let mut t = plan.tasks[i].clone();
                t.shard = None;
                t.id = format!("{FILE}::test");
                plan.tasks.push(t);
            }
        }
        plan.digest = plan.compute_digest().unwrap();
        let error = Driver::new(
            &plan,
            DriverConfig {
                schema_version: DRIVER_VERSION,
                max_parallel: 2,
            },
        )
        .unwrap_err();
        assert!(
            error.downcast_ref::<FrozenShardUnsupported>().is_some(),
            "{shape}: {error:#}"
        );
    }
    let mut d = f.driver(2);
    commit(&mut d, Input::Advance);
    let action = shard_actions(&d)[0].clone();
    claim(&mut d, &action);
    mark(&mut d, &action);
    let (wire, _) = f.execute(&action, "identity");
    let id = ready_task(&action).unwrap().to_string();
    for wrong in ["job", "shard", "missing"] {
        let mut invalid = wire.clone();
        match wrong {
            "job" => invalid.shard.as_mut().unwrap().job_id = "consumer".into(),
            "shard" => invalid.shard.as_mut().unwrap().shard_id = "other-owner".into(),
            _ => invalid.shard = None,
        }
        assert_rejected_unchanged(
            &d,
            Input::Settle {
                task: id.clone(),
                token: id.clone(),
                result_digest: invalid.digest().unwrap(),
                result: Box::new(invalid),
            },
        );
    }
    settle(&mut d, &action, wire.clone());
    settle(&mut d, &action, wire);
    assert!(d.outcome(FILE, "test").is_none());
}

#[test]
fn frozen_shards_inflight_cancel_real_terminal_and_late_success_replay() {
    use std::io::Read;
    for inflight in [false, true] {
        let signal_area = tempfile::tempdir_in("/private/tmp").unwrap();
        let signal = signal_area.path().join("started.fifo");
        assert!(ExecContext::new(Default::default())
            .command("/usr/bin/mkfifo")
            .arg(&signal)
            .status()
            .unwrap()
            .success());
        let prep = if inflight {
            format!("echo running > '{}'; sleep 60", signal.display())
        } else {
            "'true'".into()
        };
        let f = shard_fixture(false, &prep, SHARD_TESTS);
        let mut d = f.driver(2);
        let mut journal = vec![commit(&mut d, Input::Advance)];
        let action = shard_actions(&d)[0].clone();
        let id = ready_task(&action).unwrap().to_string();
        journal.push(commit(
            &mut d,
            Input::Claim {
                task: id.clone(),
                token: id.clone(),
            },
        ));
        journal.push(commit(
            &mut d,
            Input::MarkDispatched {
                task: id.clone(),
                token: id.clone(),
            },
        ));
        let wire = std::thread::scope(|scope| {
            let mut worker = Some(scope.spawn(|| f.execute(&action, "cancelled-shard").0));
            let completed = if inflight {
                let mut line = String::new();
                std::fs::File::open(&signal)
                    .unwrap()
                    .read_to_string(&mut line)
                    .unwrap();
                assert_eq!(line, "running\n");
                None
            } else {
                Some(worker.take().unwrap().join().unwrap())
            };
            journal.push(commit(&mut d, Input::Cancel));
            let cancel = cancel_actions(&d).remove(0);
            journal.push(commit(&mut d, Input::Ack { action: cancel.id }));
            assert_eq!(d.admitted(), 1);
            assert!(d.outcome(FILE, "test").is_none());
            assert_rejected_unchanged(&d, abort_input(&id, HostFailureReason::ExecutorPreparation));
            if let Some(wire) = completed {
                wire
            } else {
                f.ctx.cancel.cancel();
                worker.take().unwrap().join().unwrap()
            }
        });
        let actual_status = wire.outcomes[0].status;
        let receipt_digest = wire.digest().unwrap();
        journal.push(settle(&mut d, &action, wire));
        assert_eq!(d.admitted(), 0);
        assert!(finished(&d));
        assert_eq!(
            actual_status,
            if inflight {
                JobStatus::Cancelled
            } else {
                JobStatus::Success
            }
        );
        assert!(
            matches!(&d.state.phases[&id], Phase::Terminal(Terminal::Worker { result_digest, .. }) if result_digest == &receipt_digest)
        );
        let join = d
            .shard_join(&f.plan.shard_groups.iter().find(|g| g.split).unwrap().id)
            .unwrap();
        assert_eq!(join.core_members.len(), 1);
        assert_eq!(join.aggregate.missing.len(), 1);
        assert_eq!(join.outcome.status, JobStatus::Cancelled);
        replay_shards(&f, &d, &journal, 2);
    }
}

#[test]
fn frozen_shards_reserved_escaped_outputs_cancel_abort_exact_capacity() {
    // Shell printf writes actual control characters, which expand sixfold in
    // JSON. Genuine Wire projections and all immutable output copies count.
    let f = shard_fixture_output(
        false,
        "'true'",
        SHARD_TESTS,
        "$(printf '\\001%.0s' {1..1024})",
    );
    let mut d = f.driver(2);
    commit(&mut d, Input::Advance);
    let actions = shard_actions(&d);
    claim(&mut d, &actions[0]);
    mark(&mut d, &actions[0]);
    let (wire, _) = f.execute(&actions[0], "reserved-real-shard");
    let real_digest = wire.digest().unwrap();
    settle(&mut d, &actions[0], wire);
    let task = ready_task(&actions[1]).unwrap().to_string();
    let owner = "\0".repeat(256);
    let input = Input::Claim {
        task: task.clone(),
        token: owner.clone(),
    };
    let candidate = d.compute(&input).unwrap();
    let required = crate::expr::serialized_size(&candidate, MAX_WIRE_BYTES).unwrap()
        + d.cancel_reserve(&candidate).unwrap();
    d.state_byte_limit = required - 1;
    assert_rejected_unchanged(&d, input.clone());
    d.state_byte_limit = required;
    commit(&mut d, input);
    commit(&mut d, Input::Cancel);
    let cancel = cancel_actions(&d).remove(0);
    commit(&mut d, Input::Ack { action: cancel.id });
    assert_eq!(d.admitted(), 1);
    commit(
        &mut d,
        Input::AbortBeforeExecution {
            task,
            token: owner,
            reason: HostFailureReason::Lock,
        },
    );
    let group = &f.plan.shard_groups.iter().find(|g| g.split).unwrap().id;
    let join = d.shard_join(group).unwrap();
    assert_eq!(join.outcome.outputs["value"], "\u{1}".repeat(1024));
    assert_eq!(join.outcome.status, JobStatus::Cancelled);
    assert_eq!(join.core_members.len(), 1);
    assert_eq!(d.admitted(), 0);
    assert!(finished(&d));
    assert!(crate::expr::serialized_size(&d.state, d.state_byte_limit).is_ok());
    assert!(
        matches!(&d.state.phases[ready_task(&actions[0]).unwrap()], Phase::Terminal(Terminal::Worker { result_digest, .. }) if result_digest == &real_digest)
    );
}

#[test]
fn frozen_shards_final_real_receipt_uses_join_reserve_and_storage_rejection_is_typed() {
    for large in [false, true] {
        let output = if large {
            "x".repeat(16 * 1024)
        } else {
            "agreed".into()
        };
        let f = shard_fixture_output(false, "'true'", SHARD_TESTS, &output);
        let mut d = f.driver(2);
        commit(&mut d, Input::Advance);
        let actions = shard_actions(&d);
        claim(&mut d, &actions[0]);
        mark(&mut d, &actions[0]);
        let (first_wire, _) = f.execute(&actions[0], "reserve-first");
        settle(&mut d, &actions[0], first_wire);
        claim(&mut d, &actions[1]);
        mark(&mut d, &actions[1]);
        let (wire, _) = f.execute(&actions[1], "reserve-last");
        let id = ready_task(&actions[1]).unwrap().to_string();
        let input = Input::Settle {
            task: id.clone(),
            token: id.clone(),
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        };
        // Capacity for a new real receipt is deliberately not promised by a
        // claim. For the small case count the exact verified pre-join state and
        // its obligations, then prove joining cannot discard that producer in
        // final checked(). A larger receipt with only old-state capacity is a
        // separate, typed storage rejection.
        if large {
            d.state_byte_limit = crate::expr::serialized_size(&d.state, MAX_WIRE_BYTES).unwrap()
                + d.cancel_reserve(&d.state).unwrap();
        } else {
            let Input::Settle {
                result,
                result_digest,
                ..
            } = &input
            else {
                unreachable!()
            };
            let mut prejoin = d.state.clone();
            prejoin.revision += 1;
            prejoin.phases.insert(
                id.clone(),
                Phase::Terminal(Terminal::Worker {
                    token: id.clone(),
                    result_digest: result_digest.clone(),
                    outcomes: result.outcomes.clone(),
                    shard: result.shard.clone(),
                }),
            );
            d.state_byte_limit = crate::expr::serialized_size(&prejoin, MAX_WIRE_BYTES).unwrap()
                + d.cancel_reserve(&prejoin).unwrap();
        }
        if large {
            let before = digest(&d.state).unwrap();
            let error = d.propose(input.clone()).unwrap_err();
            assert!(
                error.downcast_ref::<DriverStorageError>().is_some(),
                "{error:#}"
            );
            assert_eq!(before, digest(&d.state).unwrap());
            assert_eq!(d.admitted(), 1);
            assert!(d.outcome(FILE, "test").is_none());
            // The host still owns the immutable real receipt; a larger durable
            // storage policy is outside this stage. Restoring the test limit
            // demonstrates that the same real receipt can settle unchanged.
            d.state_byte_limit = MAX_WIRE_BYTES;
        }
        commit(&mut d, input);
        assert_eq!(d.admitted(), 0);
        let joined = d
            .shard_join(&f.plan.shard_groups.iter().find(|g| g.split).unwrap().id)
            .unwrap();
        assert!(joined.aggregate.ok());
        assert_eq!(joined.outcome.outputs["value"], output);
        assert_eq!(joined.outcome.status, JobStatus::Success);
    }
}
