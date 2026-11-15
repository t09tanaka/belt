//! Actual worker shell execution plus transactional static-driver progression.
use crate::driver::{ActionKind, Driver, DriverConfig, Input, DRIVER_VERSION};
use crate::exec::{Opts, Outcome, Runner, Workspace};
use crate::source::{CaptureOptions, ObjectStore};
use crate::source_execute::{execute_pinned_task, PinnedExecution, RefusePinnedForks};
use crate::task::{JobStatus, TaskInputs};
use crate::wire::Digested;
use crate::{ExecContext, PinnedPlanOptions};
use std::path::Path;

const WORKFLOW: &str = r#"on: push
jobs:
  seed:
    runs-on: x
    outputs:
      value: ${{ steps.seed.outputs.value }}
    steps:
      - id: seed
        run: echo 'value=from-seed' >> "$GITHUB_OUTPUT"
  matrix:
    name: 'leg-${{ matrix.n }}'
    needs: seed
    if: needs.seed.outputs.value == 'from-seed'
    runs-on: x
    strategy:
      matrix:
        n: [0, 1]
    env:
      FROM: ${{ needs.seed.outputs.value }}
      LEG: ${{ matrix.n }}
    outputs:
      value: ${{ steps.out.outputs.value }}
    steps:
      - id: out
        run: |
          test "$FROM" = from-seed
          test "$LEG" = '${{ matrix.n }}'
          test '${{ strategy.job-total }}' = 2
          test '${{ strategy.job-index }}' = "$LEG"
          test '${{ github.workspace }}' = "$PWD"
          test -d '${{ runner.temp }}'
          test -n '${{ runner.os }}'
          test -z "${NEXT_ENV:-}"
          test -z "${GITHUB_SHA:-}"
          test ! -e "marker-$LEG"
          printf '%s' "$LEG" > "marker-$LEG"
          echo "value=$FROM-$LEG" >> "$GITHUB_OUTPUT"
          echo "NEXT_ENV=env-$LEG" >> "$GITHUB_ENV"
      - if: steps.out.outputs.value == format('from-seed-{0}', matrix.n)
        env:
          FROM_PREVIOUS: ${{ env.NEXT_ENV }}
        run: test "$FROM_PREVIOUS" = "env-$LEG"
      - if: false
        run: exit 99
  downstream:
    needs: matrix
    runs-on: x
    steps:
      - run: test '${{ needs.matrix.outputs.value }}' = from-seed-1
  skipped:
    needs: seed
    if: false
    runs-on: x
    steps:
      - run: exit 99
"#;
fn options(area: &Path) -> Opts {
    Opts {
        parallel: 1,
        traffic_controller: None,
        executor: None,
        portool: None,
        containers: 1,
        job_timeout_min: 1.,
        keep: false,
        trash: area.join("trash"),
    }
}
fn workspace(area: &Path, name: &str) -> Workspace {
    let area = area.join(name);
    Workspace {
        root: area.join("root"),
        repo_root: area.join("no-live-repo"),
        run_dir: area.join("run"),
        tools: area.join("tools"),
        lock_file: area.join("lock"),
    }
}
fn commit(driver: &mut Driver, input: Input) {
    let record = driver.propose(input).unwrap();
    driver.apply(&record).unwrap();
}
fn ready(driver: &Driver) -> Vec<(String, TaskInputs)> {
    driver
        .pending_actions()
        .iter()
        .filter_map(|a| match &a.kind {
            ActionKind::Ready { task, inputs } => Some((task.clone(), inputs.clone())),
            _ => None,
        })
        .collect()
}
#[test]
fn actual_two_static_legs_settle_driver_and_match_legacy_whole_matrix() {
    let area = tempfile::Builder::new()
        .prefix("localci-static-driver-")
        .tempdir_in(std::fs::canonicalize(std::env::temp_dir()).unwrap())
        .unwrap();
    let root = area.path().join("source");
    std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
    std::fs::write(root.join(".github/workflows/ci.yml"), WORKFLOW).unwrap();
    let store = ObjectStore::open(area.path().join("objects")).unwrap();
    // Explicit private context: only system shell/core utilities are visible;
    // no host Docker/SSH/daemon/service/network is used by these scripts.
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
    let input = PinnedExecution::new(&plan, &manifest, &store).unwrap();
    let mut driver = Driver::new(
        &plan,
        DriverConfig {
            schema_version: DRIVER_VERSION,
            max_parallel: 2,
        },
    )
    .unwrap();
    commit(&mut driver, Input::Advance);
    let (seed, inputs) = ready(&driver)
        .into_iter()
        .find(|(id, _)| plan.task(id).unwrap().jobs == ["seed"])
        .unwrap();
    let ws = input
        .materialize_workspace(workspace(area.path(), "seed"))
        .unwrap();
    commit(
        &mut driver,
        Input::Claim {
            task: seed.clone(),
            token: seed.clone(),
        },
    );
    let result = execute_pinned_task(
        &ctx,
        &input,
        &seed,
        &inputs,
        None,
        &ws,
        options(area.path()),
        &RefusePinnedForks,
    )
    .unwrap();
    assert_eq!(result.outcomes[0].outputs["value"], "from-seed");
    let wire =
        crate::result_wire::WireTaskResult::from_task_result(&result, &ws.workspace().run_dir)
            .unwrap();
    commit(
        &mut driver,
        Input::Settle {
            task: seed.clone(),
            token: seed,
            result_digest: wire.digest().unwrap(),
            result: Box::new(wire),
        },
    );
    let mut legs: Vec<_> = ready(&driver)
        .into_iter()
        .filter(|(id, _)| plan.task(id).unwrap().variant.is_some())
        .collect();
    assert_eq!(legs.len(), 2);
    legs.sort_by_key(|(id, _)| plan.task(id).unwrap().variant.as_ref().unwrap().index);
    let original_inputs = legs[0].1.clone();
    assert_eq!(original_inputs.needs["seed"].outputs["value"], "from-seed");
    let mut reports = Vec::new();
    // Reverse settlement exercises the real existing static-variant route.
    for (id, inputs) in legs.iter().rev() {
        let variant = plan.task(id).unwrap().variant.as_ref().unwrap().index;
        let ws = input
            .materialize_workspace(workspace(area.path(), &format!("leg-{variant}")))
            .unwrap();
        commit(
            &mut driver,
            Input::Claim {
                task: id.clone(),
                token: id.clone(),
            },
        );
        let result = execute_pinned_task(
            &ctx,
            &input,
            id,
            inputs,
            None,
            &ws,
            options(area.path()),
            &RefusePinnedForks,
        )
        .unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.outcomes[0].variant, Some(variant));
        assert_eq!(
            result.outcomes[0].status,
            JobStatus::Success,
            "{:?}",
            result.jobs
        );
        assert_eq!(result.jobs[0].steps.len(), 3);
        assert_eq!(result.jobs[0].steps[0].outcome, Outcome::Pass);
        assert_eq!(result.jobs[0].steps[1].outcome, Outcome::Pass);
        assert_eq!(
            result.jobs[0].steps[2].outcome,
            Outcome::Skip("if: false".into())
        );
        assert!(ws
            .workspace()
            .root
            .join(format!("marker-{variant}"))
            .exists());
        assert!(!ws
            .workspace()
            .root
            .join(format!("marker-{}", 1 - variant))
            .exists());
        let wire =
            crate::result_wire::WireTaskResult::from_task_result(&result, &ws.workspace().run_dir)
                .unwrap();
        commit(
            &mut driver,
            Input::Settle {
                task: id.clone(),
                token: id.clone(),
                result_digest: wire.digest().unwrap(),
                result: Box::new(wire),
            },
        );
        reports.push((variant, result.jobs));
        if variant == 1 {
            assert!(driver
                .outcome(".github/workflows/ci.yml", "matrix")
                .is_none());
        }
    }
    let joined = driver
        .outcome(".github/workflows/ci.yml", "matrix")
        .unwrap()
        .clone();
    assert_eq!(joined.outputs["value"], "from-seed-1");
    // The unchanged local whole-matrix route shares the evaluator. With no
    // executor/TTC it retains its existing sequential workspace behavior.
    let legacy_ws = input
        .materialize_workspace(workspace(area.path(), "legacy-whole"))
        .unwrap();
    let runner = Runner::new_pinned(
        &ctx,
        &input,
        &legacy_ws,
        options(area.path()),
        &RefusePinnedForks,
    )
    .unwrap();
    runner.prepare_tools().unwrap();
    let legacy = runner
        .run_task(
            &legacy_ws.workspace().root,
            plan.task(&legs[0].0).unwrap(),
            &original_inputs,
            None,
            true,
            false,
            true,
        )
        .unwrap();
    assert_eq!(legacy.outcomes, vec![joined]);
    reports.sort_by_key(|(variant, _)| *variant);
    for ((_, actual), legacy) in reports.iter().zip(&legacy.jobs) {
        assert_eq!(actual[0].display, legacy.display);
        assert_eq!(actual[0].outcome, legacy.outcome);
        let outcomes = |r: &crate::exec::JobResult| {
            r.steps
                .iter()
                .map(|s| (s.label.clone(), s.outcome.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(outcomes(&actual[0]), outcomes(legacy));
    }
    let remaining = ready(&driver);
    assert_eq!(remaining.len(), 2);
    for (id, inputs) in remaining {
        let ws = input
            .materialize_workspace(workspace(
                area.path(),
                &format!("remaining-{}", plan.task(&id).unwrap().jobs[0]),
            ))
            .unwrap();
        commit(
            &mut driver,
            Input::Claim {
                task: id.clone(),
                token: id.clone(),
            },
        );
        let result = execute_pinned_task(
            &ctx,
            &input,
            &id,
            &inputs,
            None,
            &ws,
            options(area.path()),
            &RefusePinnedForks,
        )
        .unwrap();
        assert_ne!(
            result.outcomes[0].status,
            JobStatus::Failure,
            "{:?}",
            result.jobs
        );
        let wire =
            crate::result_wire::WireTaskResult::from_task_result(&result, &ws.workspace().run_dir)
                .unwrap();
        commit(
            &mut driver,
            Input::Settle {
                task: id.clone(),
                token: id,
                result_digest: wire.digest().unwrap(),
                result: Box::new(wire),
            },
        );
    }
    assert_eq!(
        driver
            .outcome(".github/workflows/ci.yml", "downstream")
            .unwrap()
            .status,
        JobStatus::Success
    );
    assert_eq!(
        driver
            .outcome(".github/workflows/ci.yml", "skipped")
            .unwrap()
            .status,
        JobStatus::Skipped
    );
    assert!(driver
        .pending_actions()
        .iter()
        .any(|a| a.kind == ActionKind::Finished));
}
