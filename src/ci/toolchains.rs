//! The version declarations one task of a frozen plan runs under (spec 22.1):
//! the project's version files and the `*-version` inputs of the `setup-*`
//! steps of the task's own jobs. The parent freezes from this and the worker
//! derives the same from the delivered plan, so neither trusts the other's
//! reading. Only steps written directly in a job are read: the inside of a
//! reusable workflow or a composite action is not.
use crate::toolchain::versions::{self, Declaration, Scope, Unresolved};
use local_ci::{dag::ExecutionTask, FrozenPlan};
use serde_json::Value;

/// The workflow files the tasks of `plan` run, each once, in plan order.
pub fn workflows(plan: &FrozenPlan) -> Vec<String> {
    let mut files: Vec<String> = Vec::new();
    for task in &plan.tasks {
        if !files.contains(&task.workflow) {
            files.push(task.workflow.clone());
        }
    }
    files
}

/// `declarations` (read with the task's workflow) as `task` runs under them.
/// A version written as `${{ matrix.<key> }}` is the value of the task's own
/// matrix variant, and `${{ env.<NAME> }}` is the literal the workflow gives
/// the step (`env:` of the step, the job, then the workflow). Nothing else is
/// evaluated here.
pub fn task_declarations(
    plan: &FrozenPlan,
    task: &ExecutionTask,
    project: &str,
    declarations: &[Declaration],
) -> Result<(Scope, Vec<Declaration>), Unresolved> {
    let lookup = |scope: &Scope, context: &str, key: &str| match context {
        "matrix" => scalar(task.variant.as_ref()?.values.get(key)?),
        "env" => env_literal(plan, scope, key),
        _ => None,
    };
    versions::task_declarations(declarations, project, &task.workflow, &task.jobs, &lookup)
}

/// A matrix value that is a version as written: a string or a number.
fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// `env.<key>` for the step `scope` names, when the nearest `env:` that
/// defines it holds a literal. A value that is itself an expression is not
/// known here, and an outer definition does not stand in for it.
fn env_literal(plan: &FrozenPlan, scope: &Scope, key: &str) -> Option<String> {
    let workflow = plan
        .workflows
        .iter()
        .find(|w| Some(&w.file) == scope.workflow.as_ref())?;
    let job = workflow
        .jobs
        .iter()
        .find(|j| Some(&j.id) == scope.job.as_ref())?;
    let step = job.steps.get(scope.step?)?;
    let value = [&step.env, &job.env, &workflow.env]
        .into_iter()
        .find_map(|env| env.iter().find(|(name, _)| name == key).map(|(_, v)| v))?;
    (!value.contains("${{")).then(|| value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::execute::tests::{fixture, fixture_with};
    use crate::toolchain::{snapshot::read_captured, versions::effective};

    const FILE: &str = ".github/workflows/ci.yml";

    /// What each task of `job` requires, as `tool=version` lines (or the
    /// expression that could not be resolved), with the scope it is frozen in.
    fn required(
        f: &crate::ci::execute::tests::Fixture,
        job: &str,
    ) -> Vec<Result<(Scope, Vec<String>), String>> {
        let files = workflows(&f.plan);
        assert_eq!(files, [FILE]);
        let read = read_captured(&f.store, &f.manifest, "", &files).unwrap();
        let tasks = f.plan.tasks.iter().filter(|t| t.jobs == [job]);
        tasks
            .map(|task| {
                task_declarations(&f.plan, task, "", &read)
                    .map(|(scope, own)| {
                        let lines = effective(&own, &scope).unwrap();
                        (scope, lines.iter().map(|r| r.describe()).collect())
                    })
                    .map_err(|u| u.expression)
            })
            .collect()
    }
    fn job_scope(job: &str) -> Scope {
        Scope {
            project: "".into(),
            workflow: Some(FILE.into()),
            job: Some(job.into()),
            step: None,
        }
    }

    const WORKFLOW: &str = r#"on: push
env:
  NODE: 20.0.0
  PY: 3.10.0
jobs:
  plain:
    runs-on: ubuntu-latest
    steps:
      - run: echo plain
  pinned:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: 18.0.0
      - run: echo pinned
  legs:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        node: ["18.0.0", 20]
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: ${{ matrix.node }}
      - run: echo leg
  fromenv:
    runs-on: ubuntu-latest
    env:
      PY: 3.12.1
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: ${{ env.NODE }}
      - uses: actions/setup-python@v5
        env:
          PY: 3.11.2
        with:
          python-version: ${{ env.PY }}
      - uses: actions/setup-go@v5
        with:
          go-version: ${{ env.PY }}
"#;

    #[test]
    fn each_task_requires_the_setup_inputs_of_its_own_job_only() {
        let f = fixture_with(WORKFLOW, |project| {
            std::fs::write(project.join("rust-toolchain"), "1.80.0\n").unwrap();
        });
        let lines = |all: &[&str]| all.iter().map(|l| l.to_string()).collect::<Vec<_>>();
        // No setup step: the project scope and the project's file, as before.
        assert_eq!(
            required(&f, "plain"),
            [Ok((Scope::project(""), lines(&["rust=1.80.0"])))]
        );
        assert_eq!(
            required(&f, "pinned"),
            [Ok((
                job_scope("pinned"),
                lines(&["node=18.0.0", "rust=1.80.0"])
            ))]
        );
        // Each leg of the matrix takes its own value: a string or a number.
        let mut legs = required(&f, "legs");
        legs.sort_by_key(|leg| format!("{leg:?}"));
        assert_eq!(legs.len(), 2);
        assert_eq!(
            legs[0],
            Ok((job_scope("legs"), lines(&["node=18.0.0", "rust=1.80.0"])))
        );
        let (scope, numbered) = legs[1].clone().unwrap();
        assert_eq!(scope, job_scope("legs"));
        assert!(numbered[0].starts_with("node=>=20.0.0"), "{numbered:?}");
        // `env`: the step's own, then the job's, then the workflow's.
        assert_eq!(
            required(&f, "fromenv"),
            [Ok((
                job_scope("fromenv"),
                lines(&["go=3.12.1", "node=20.0.0", "python=3.11.2", "rust=1.80.0"])
            ))]
        );
    }

    /// A task without a setup step carries, byte for byte, the bundle that
    /// was frozen before workflows were read: stored runs and their retries
    /// keep their identity.
    #[test]
    fn a_task_without_a_setup_step_freezes_to_the_same_bytes_as_before() {
        use crate::toolchain::{
            contract::FrozenBundle,
            versions::{FrozenToolchains, MatchPolicy},
        };
        let f = fixture_with(WORKFLOW, |project| {
            std::fs::write(project.join("rust-toolchain"), "1.80.0\n").unwrap();
        });
        let digest = crate::ids::Digest::of_bytes(b"source");
        let bytes = |scope: Scope, declarations: &[Declaration]| {
            let frozen = FrozenToolchains::freeze(
                digest.clone(),
                digest.clone(),
                scope,
                declarations,
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            )
            .unwrap();
            serde_json::to_vec(&FrozenBundle::seal_exact(frozen).unwrap()).unwrap()
        };
        let before = read_captured(&f.store, &f.manifest, "", &[]).unwrap();
        let read = read_captured(&f.store, &f.manifest, "", &workflows(&f.plan)).unwrap();
        assert!(read.len() > before.len());
        let task = |job: &str| f.plan.tasks.iter().find(|t| t.jobs == [job]).unwrap();
        let (scope, own) = task_declarations(&f.plan, task("plain"), "", &read).unwrap();
        assert_eq!(bytes(scope, &own), bytes(Scope::project(""), &before));
        // A job with a setup step is another bundle.
        let (scope, own) = task_declarations(&f.plan, task("pinned"), "", &read).unwrap();
        assert_ne!(bytes(scope, &own), bytes(Scope::project(""), &before));
    }

    #[test]
    fn what_the_plan_does_not_hold_as_a_literal_is_unresolved() {
        let job = |env: &str, version: &str, matrix: &str| {
            format!(
                "on: push\nenv:\n  OUTER: 20.0.0\n  DYN: ${{{{ github.sha }}}}\njobs:\n  build:\n    runs-on: ubuntu-latest\n{matrix}    steps:\n      - uses: actions/setup-node@v4\n{env}        with:\n          node-version: {version}\n"
            )
        };
        let unresolved = |workflow: String, expression: &str| {
            let got = required(&fixture(&workflow), "build");
            assert!(!got.is_empty(), "{workflow}");
            for task in got {
                assert_eq!(task, Err(expression.to_string()), "{workflow}");
            }
        };
        // An env value that is an expression, and a name no `env:` defines.
        unresolved(job("", "${{ env.DYN }}", ""), "${{ env.DYN }}");
        unresolved(job("", "${{ env.MISSING }}", ""), "${{ env.MISSING }}");
        // The step's own expression hides the literal of the workflow.
        unresolved(
            job(
                "        env:\n          OUTER: ${{ github.ref }}\n",
                "${{ env.OUTER }}",
                "",
            ),
            "${{ env.OUTER }}",
        );
        // Another context, and a job without a matrix.
        unresolved(job("", "${{ github.ref }}", ""), "${{ github.ref }}");
        unresolved(job("", "${{ matrix.node }}", ""), "${{ matrix.node }}");
        // A key the variant lacks, and a value that is not a scalar.
        let matrix = "    strategy:\n      matrix:\n        flag: [true]\n";
        unresolved(job("", "${{ matrix.node }}", matrix), "${{ matrix.node }}");
        unresolved(job("", "${{ matrix.flag }}", matrix), "${{ matrix.flag }}");
        // The literal of the workflow itself does resolve.
        assert_eq!(
            required(&fixture(&job("", "${{ env.OUTER }}", "")), "build"),
            [Ok((job_scope("build"), vec!["node=20.0.0".to_string()]))]
        );
    }
}
