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
        let area = tempfile::Builder::new()
            .prefix("localci-dynamic-driver-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let root = area.path().join("source");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        std::fs::write(root.join(FILE), workflow).unwrap();
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
        Phase::Terminal(Terminal::Unstarted { .. })
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
    assert!(error.to_string().contains("grouped dynamic"));
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
