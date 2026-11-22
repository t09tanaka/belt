//! The version declarations one task of a frozen plan runs under (spec 22.1):
//! the project's version files and the `*-version` inputs of the `setup-*`
//! steps of the task's own jobs. The parent freezes from this and the worker
//! derives the same from the delivered plan with the same function, so
//! neither trusts the other's reading and both read alike. Only steps written
//! directly in a job are read: the inside of a reusable workflow or a
//! composite action is not.
use crate::source::store::SourceStore;
use crate::toolchain::snapshot::read_captured_task;
use crate::toolchain::versions::{
    self, Constraint, Declaration, Diagnostic, EffectiveRequirement, FrozenToolchains, Scope,
    SetupRead, Skipped, ToolId,
};
use local_ci::{dag::ExecutionTask, source::SourceManifest, FrozenPlan};
use serde_json::Value;

/// What the captured source holds for the tasks of one workflow: the
/// project's declarations with the workflow's setup steps, and the code of
/// the reason when the workflow could not be read.
pub type TaskSource = (SetupRead, Option<String>);

/// Read the captured source once for the tasks of `workflow`. An error is one
/// of the project's own version files, as for a plain command.
pub fn read_task(
    store: &SourceStore,
    manifest: &SourceManifest,
    project: &str,
    workflow: &str,
) -> Result<TaskSource, Diagnostic> {
    read_captured_task(store, manifest, project, workflow)
}

/// An input of a setup step that is no requirement, or a workflow that was
/// not read: who it concerns, where it is written, and what it means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub task: String,
    /// The job of the step; `None` for a workflow as a whole.
    pub job: Option<String>,
    /// `file:line` of the step, or the workflow file.
    pub at: String,
    pub text: String,
}

/// What a task is frozen from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFreeze {
    pub scope: Scope,
    pub declarations: Vec<Declaration>,
    pub notices: Vec<Notice>,
}

impl TaskFreeze {
    /// The tools a setup step of the task requires by a channel or an alias
    /// (`stable`, `lts/*`): a requirement only while a concrete version is
    /// known for it.
    pub fn aliases(&self) -> Vec<ToolId> {
        let step = |d: &&Declaration| d.scope == self.scope && self.scope.job.is_some();
        let alias = |d: &&Declaration| matches!(d.constraint, Constraint::Resolve(_));
        let mut tools: Vec<ToolId> = Vec::new();
        for d in self.declarations.iter().filter(step).filter(alias) {
            if !tools.contains(&d.tool) {
                tools.push(d.tool.clone());
            }
        }
        tools
    }
}

/// What `task` is frozen from: the scope, the declarations in it, and one
/// notice for each input of its setup steps that is no requirement.
///
/// A version belt understands (a version, a range) is a requirement, and
/// where a setup step requires a tool the project's own version file of that
/// tool is not: the step installs its version for the job. A version written
/// as `${{ matrix.<key> }}` is the value of the task's own matrix variant,
/// and `${{ env.<NAME> }}` is the literal the workflow gives the step (`env:`
/// of the step, the job, then the workflow). A channel or an alias is a
/// requirement unless `unresolved` says no concrete version is known for it.
/// Anything else is not interpreted: it requires nothing, and the notice says
/// where it is written and why. A job the task does not run has no effect.
pub fn task_declarations(
    source: &TaskSource,
    plan: &FrozenPlan,
    task: &ExecutionTask,
    project: &str,
    unresolved: &dyn Fn(&ToolId, &str) -> bool,
) -> TaskFreeze {
    let (read, unread) = source;
    let lookup = |scope: &Scope, context: &str, key: &str| match context {
        "matrix" => scalar(task.variant.as_ref()?.values.get(key)?),
        "env" => env_literal(plan, scope, key),
        _ => None,
    };
    let (scope, declarations, skipped) = versions::task_declarations(
        read,
        project,
        &task.workflow,
        &task.jobs,
        &lookup,
        unresolved,
    );
    let mut notices: Vec<Notice> = Vec::new();
    if let Some(code) = unread {
        notices.push(Notice {
            task: task.id.clone(),
            job: None,
            at: task.workflow.clone(),
            text: format!(
                "the workflow could not be read for its setup steps ({code}); no version is required from them"
            ),
        });
    }
    for skipped in &skipped {
        let tool = skipped.tool.as_str();
        let notice = Notice {
            task: task.id.clone(),
            job: skipped.scope.job.clone(),
            at: format!("{}:{}", skipped.step.path, skipped.step.first_line),
            text: format!(
                "{tool}: {}; no {tool} version is required from this step",
                reason(skipped)
            ),
        };
        if !notices.contains(&notice) {
            notices.push(notice);
        }
    }
    TaskFreeze {
        scope,
        declarations,
        notices,
    }
}

/// [`task_declarations`] as the holder of a frozen bundle reads it: a channel
/// or an alias of a setup step is a requirement exactly when `requirements`
/// (the bundle's) hold it for that tool. The parent leaves such a requirement
/// out when nothing resolves it, and a worker reads the same from the bundle
/// it was given, so the two derive one bundle from one source.
pub fn task_declarations_as_frozen(
    source: &TaskSource,
    plan: &FrozenPlan,
    task: &ExecutionTask,
    project: &str,
    requirements: &[EffectiveRequirement],
) -> TaskFreeze {
    let absent = |tool: &ToolId, alias: &str| {
        let alias = Constraint::Resolve(alias.into());
        let held = |r: &EffectiveRequirement| &r.tool == tool && r.constraints.contains(&alias);
        !requirements.iter().any(held)
    };
    task_declarations(source, plan, task, project, &absent)
}

/// Whether `frozen` is what this source freezes `task` to: the scope, and the
/// requirements in it. The code of the difference otherwise.
pub fn check_frozen(
    source: &TaskSource,
    plan: &FrozenPlan,
    task: &ExecutionTask,
    project: &str,
    frozen: &FrozenToolchains,
) -> Result<(), String> {
    let own = task_declarations_as_frozen(source, plan, task, project, &frozen.requirements);
    if frozen.scope != own.scope {
        return Err("toolchain source scope mismatch".into());
    }
    let effective = versions::effective(&own.declarations, &frozen.scope).map_err(|e| e.code)?;
    if effective != frozen.requirements {
        return Err("toolchain source declarations mismatch".into());
    }
    Ok(())
}

/// The notices of a run as lines for a person. The legs of a matrix say the
/// same of the same step: that is one line, naming the job.
pub fn render(notices: &[Notice]) -> Vec<String> {
    let mut groups: Vec<(&Notice, Vec<&str>)> = Vec::new();
    for notice in notices {
        let same = |(first, _): &&mut (&Notice, Vec<&str>)| {
            first.at == notice.at && first.text == notice.text && first.job == notice.job
        };
        match groups.iter_mut().find(same) {
            Some((_, tasks)) => {
                if !tasks.contains(&notice.task.as_str()) {
                    tasks.push(&notice.task);
                }
            }
            None => groups.push((notice, vec![&notice.task])),
        }
    }
    let line = |(notice, tasks): &(&Notice, Vec<&str>)| {
        let (at, text) = (&notice.at, &notice.text);
        match (&notice.job, tasks.as_slice()) {
            (None, _) => format!("workflow {at}: {text}"),
            (Some(job), [task]) => format!("task `{task}` (job {job}, {at}): {text}"),
            (Some(job), tasks) => format!("job {job}, {} tasks ({at}): {text}", tasks.len()),
        }
    };
    groups.iter().map(line).collect()
}

/// Why an input of a setup step is no requirement, for a person.
fn reason(skipped: &Skipped) -> String {
    let value = &skipped.value;
    match skipped.code.as_str() {
        "conditional_step" => "the step has an `if:` condition".into(),
        "unresolved_alias" => format!("`{value}` names no concrete version"),
        "unresolved_expression" => {
            format!("the version is the expression {value}, which is not known until the run")
        }
        "version_file_expression" => {
            format!("the version file is the expression {value}, which is not known until the run")
        }
        "missing_version_file" => {
            format!("the version file {value} is not a readable file of the captured source")
        }
        "invalid_version_file" => {
            format!("the version file {value} is not a path inside the project")
        }
        "version_file_without_tool" => {
            format!("the version file {value} names no version of it")
        }
        crate::toolchain::declarations::STRUCTURED_VERSION_FILE => {
            format!("the version file {value} is not a plain version file")
        }
        code => format!("`{value}` is not a version belt reads ({code})"),
    }
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
    use crate::ci::execute::tests::{fixture, fixture_with, Fixture};
    use crate::toolchain::versions::effective;

    const FILE: &str = ".github/workflows/ci.yml";

    fn resolved(_: &ToolId, _: &str) -> bool {
        false
    }
    fn unresolved(_: &ToolId, _: &str) -> bool {
        true
    }
    fn source(f: &Fixture) -> TaskSource {
        read_task(&f.store, &f.manifest, "", FILE).unwrap()
    }
    fn tasks<'a>(f: &'a Fixture, job: &str) -> Vec<&'a ExecutionTask> {
        let tasks: Vec<_> = f.plan.tasks.iter().filter(|t| t.jobs == [job]).collect();
        assert!(!tasks.is_empty(), "{job}");
        tasks
    }
    fn frozen_with(
        f: &Fixture,
        job: &str,
        alias: &dyn Fn(&ToolId, &str) -> bool,
    ) -> Vec<TaskFreeze> {
        let source = source(f);
        tasks(f, job)
            .into_iter()
            .map(|task| task_declarations(&source, &f.plan, task, "", alias))
            .collect()
    }
    fn lines(freeze: &TaskFreeze) -> Vec<String> {
        let required = effective(&freeze.declarations, &freeze.scope).unwrap();
        required.iter().map(|r| r.describe()).collect()
    }
    /// What each task of `job` requires, as `tool=version` lines, with the
    /// scope it is frozen in and the notices as a person reads them.
    fn required(f: &Fixture, job: &str) -> Vec<(Scope, Vec<String>, Vec<String>)> {
        frozen_with(f, job, &resolved)
            .iter()
            .map(|freeze| (freeze.scope.clone(), lines(freeze), render(&freeze.notices)))
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
    fn strings(all: &[&str]) -> Vec<String> {
        all.iter().map(|l| l.to_string()).collect()
    }
    /// The one notice of the one task of `job`, without the task id.
    fn notice(f: &Fixture, job: &str) -> String {
        let all = required(f, job);
        assert_eq!(all.len(), 1, "{all:?}");
        let (_, _, notices) = &all[0];
        assert_eq!(notices.len(), 1, "{notices:?}");
        let id = &tasks(f, job)[0].id;
        notices[0].replace(&format!("task `{id}` "), "")
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
          node-version: ${{ Matrix.node }}
      - run: echo leg
  fromenv:
    runs-on: ubuntu-latest
    env:
      PY: 3.12.1
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: ${{ ENV.NODE }}
      - uses: actions/setup-python@v5
        env:
          PY: 3.11.2
        with:
          python-version: ${{ env.PY }}
      - uses: actions/setup-go@v5
        with:
          go-version: ${{ env.PY }}
  rusty:
    runs-on: ubuntu-latest
    steps:
      - uses: dtolnay/rust-toolchain@1.89.0
      - run: echo rusty
"#;

    #[test]
    fn each_task_requires_the_setup_inputs_of_its_own_job_only() {
        let f = fixture_with(WORKFLOW, |project| {
            std::fs::write(project.join("rust-toolchain"), "1.80.0\n").unwrap();
        });
        // No setup step: the project scope and the project's file, as before.
        assert_eq!(
            required(&f, "plain"),
            [(Scope::project(""), strings(&["rust=1.80.0"]), vec![])]
        );
        assert_eq!(
            required(&f, "pinned"),
            [(
                job_scope("pinned"),
                strings(&["node=18.0.0", "rust=1.80.0"]),
                vec![]
            )]
        );
        // Each leg of the matrix takes its own value: a string or a number.
        let mut legs = required(&f, "legs");
        legs.sort_by_key(|leg| format!("{leg:?}"));
        assert_eq!(legs.len(), 2);
        assert_eq!(
            legs[0],
            (
                job_scope("legs"),
                strings(&["node=18.0.0", "rust=1.80.0"]),
                vec![]
            )
        );
        let (scope, numbered, notices) = legs[1].clone();
        assert_eq!(scope, job_scope("legs"));
        assert!(numbered[0].starts_with("node=>=20.0.0"), "{numbered:?}");
        assert_eq!(notices, Vec::<String>::new());
        // `env`: the step's own, then the job's, then the workflow's.
        assert_eq!(
            required(&f, "fromenv"),
            [(
                job_scope("fromenv"),
                strings(&["go=3.12.1", "node=20.0.0", "python=3.11.2", "rust=1.80.0"]),
                vec![]
            )]
        );
        // A step that names the tool of a project file stands before the
        // file: no contradiction, and nothing is told.
        assert_eq!(
            required(&f, "rusty"),
            [(job_scope("rusty"), strings(&["rust=1.89.0"]), vec![])]
        );
    }

    /// A task without a setup step carries, byte for byte, the bundle that
    /// was frozen before workflows were read: stored runs and their retries
    /// keep their identity.
    #[test]
    fn a_task_without_a_setup_step_freezes_to_the_same_bytes_as_before() {
        use crate::toolchain::{
            contract::FrozenBundle, snapshot::read_captured, versions::MatchPolicy,
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
        let plain = frozen_with(&f, "plain", &resolved).remove(0);
        assert_eq!(
            bytes(plain.scope, &plain.declarations),
            bytes(Scope::project(""), &before)
        );
        // A job with a setup step is another bundle.
        let pinned = frozen_with(&f, "pinned", &resolved).remove(0);
        assert_ne!(
            bytes(pinned.scope, &pinned.declarations),
            bytes(Scope::project(""), &before)
        );
    }

    fn job(env: &str, version: &str, matrix: &str) -> String {
        format!(
            "on: push\nenv:\n  OUTER: 20.0.0\n  DYN: ${{{{ github.sha }}}}\njobs:\n  build:\n    runs-on: ubuntu-latest\n{matrix}    steps:\n      - uses: actions/setup-node@v4\n{env}        with:\n          node-version: {version}\n"
        )
    }

    #[test]
    fn what_the_plan_does_not_hold_as_a_literal_is_a_notice_and_no_requirement() {
        let told = |workflow: String, expression: &str| {
            let f = fixture(&workflow);
            let all = required(&f, "build");
            for (scope, lines, notices) in &all {
                assert_eq!(scope, &Scope::project(""), "{workflow}");
                assert_eq!(lines, &Vec::<String>::new(), "{workflow}");
                assert_eq!(notices.len(), 1, "{workflow}");
                let expected = format!(
                    "node: the version is the expression {expression}, which is not known until the run; no node version is required from this step"
                );
                assert!(notices[0].ends_with(&expected), "{notices:?}");
            }
        };
        // An env value that is an expression, and a name no `env:` defines.
        told(job("", "${{ env.DYN }}", ""), "${{ env.DYN }}");
        told(job("", "${{ env.MISSING }}", ""), "${{ env.MISSING }}");
        // The step's own expression hides the literal of the workflow.
        told(
            job(
                "        env:\n          OUTER: ${{ github.ref }}\n",
                "${{ env.OUTER }}",
                "",
            ),
            "${{ env.OUTER }}",
        );
        // Another context, and a job without a matrix.
        told(job("", "${{ github.ref }}", ""), "${{ github.ref }}");
        told(job("", "${{ matrix.node }}", ""), "${{ matrix.node }}");
        // A key the variant lacks, and a value that is not a scalar.
        let matrix = "    strategy:\n      matrix:\n        flag: [true]\n";
        told(job("", "${{ matrix.node }}", matrix), "${{ matrix.node }}");
        told(job("", "${{ matrix.flag }}", matrix), "${{ matrix.flag }}");
        // A matrix that is itself an expression: its values are not known.
        let dynamic = "    strategy:\n      matrix: ${{ fromJSON(vars.MATRIX) }}\n";
        let f = fixture(&job("", "${{ matrix.node }}", dynamic));
        assert!(tasks(&f, "build").iter().all(|task| task.dynamic_matrix));
        told(job("", "${{ matrix.node }}", dynamic), "${{ matrix.node }}");
        // The literal of the workflow itself does resolve.
        assert_eq!(
            required(&fixture(&job("", "${{ env.OUTER }}", "")), "build"),
            [(job_scope("build"), strings(&["node=20.0.0"]), vec![])]
        );
    }

    #[test]
    fn a_notice_names_the_task_its_job_and_the_step_in_the_workflow() {
        // The step is line 9 of the workflow `job` writes.
        let f = fixture(&job("", "pypy3.10", ""));
        assert_eq!(
            notice(&f, "build"),
            format!("(job build, {FILE}:9): node: `pypy3.10` is not a version belt reads (invalid_version); no node version is required from this step")
        );
        // The legs of a matrix say the same of the same step: one line.
        let matrix = "    strategy:\n      matrix:\n        os: [a, b, c]\n";
        let f = fixture(&job("", "${{ matrix.node }}", matrix));
        let source = source(&f);
        let mut all = Vec::new();
        for task in tasks(&f, "build") {
            all.extend(task_declarations(&source, &f.plan, task, "", &resolved).notices);
        }
        assert_eq!(all.len(), 3);
        assert_eq!(
            render(&all),
            [format!("job build, 3 tasks ({FILE}:12): node: the version is the expression ${{{{ matrix.node }}}}, which is not known until the run; no node version is required from this step")]
        );
        // The same notice of the same task twice is one task.
        let twice: Vec<Notice> = all[..1].iter().chain(&all[..1]).cloned().collect();
        assert!(render(&twice)[0].starts_with("task `"), "{twice:?}");
        // Another text, another step or another job is another line.
        let mut other = all[..2].to_vec();
        other[1].text = "other".into();
        assert_eq!(render(&other).len(), 2);
        let mut other = all[..2].to_vec();
        other[1].at = "elsewhere:1".into();
        assert_eq!(render(&other).len(), 2);
        let mut other = all[..2].to_vec();
        other[1].job = Some("other".into());
        assert_eq!(render(&other).len(), 2);
    }

    #[test]
    fn every_form_belt_does_not_interpret_is_told_with_its_reason() {
        let step = |uses: &str, lines: &str| {
            format!(
                "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: {uses}\n{lines}      - run: echo done\n"
            )
        };
        let at = format!("(job build, {FILE}:6): ");
        let told = |workflow: &str, files: &[(&str, &str)], tool: &str, reason: &str| {
            let f = fixture_with(workflow, |project| {
                for (name, content) in files {
                    let path = project.join(name);
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    std::fs::write(path, content).unwrap();
                }
            });
            assert_eq!(
                notice(&f, "build"),
                format!("{at}{tool}: {reason}; no {tool} version is required from this step"),
                "{workflow}"
            );
            let (scope, lines, _) = required(&f, "build").remove(0);
            assert_eq!((scope, lines), (Scope::project(""), vec![]), "{workflow}");
        };
        let go = step(
            "actions/setup-go@v5",
            "        with:\n          go-version-file: go.mod\n",
        );
        told(
            &go,
            &[("go.mod", "module x\n\ngo 1.22\n")],
            "go",
            "the version file go.mod is not a plain version file",
        );
        told(
            &go,
            &[],
            "go",
            "the version file go.mod is not a readable file of the captured source",
        );
        told(
            &step(
                "actions/setup-go@v5",
                "        with:\n          go-version-file: ../go.mod\n",
            ),
            &[],
            "go",
            "the version file ../go.mod is not a path inside the project",
        );
        told(
            &step("actions/setup-go@v5", "        with:\n          go-version-file: ${{ matrix.file }}\n"),
            &[],
            "go",
            "the version file is the expression ${{ matrix.file }}, which is not known until the run",
        );
        told(
            &step(
                "actions/setup-go@v5",
                "        with:\n          go-version-file: meta/.tool-versions\n",
            ),
            &[("meta/.tool-versions", "ruby 3.3.0\n")],
            "go",
            "the version file meta/.tool-versions names no version of it",
        );
        told(
            &step(
                "actions/setup-go@v5",
                "        with:\n          go-version: oldstable\n",
            ),
            &[],
            "go",
            "`oldstable` is not a version belt reads (invalid_version)",
        );
        // A step under a condition, also when its version is read from a
        // file: the notice points at the step in the workflow, not the file.
        told(
            &step(
                "actions/setup-python@v5",
                "        if: runner.os == 'Linux'\n        with:\n          python-version-file: .python-version\n",
            ),
            &[(".python-version", "\n\n3.12.1\n")],
            "python",
            "the step has an `if:` condition",
        );
    }

    #[test]
    fn steps_under_conditions_are_alternatives_and_a_mixed_job_keeps_what_is_read() {
        let workflow = r#"on: push
jobs:
  either:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-node@v4
        if: runner.os == 'Linux'
        with:
          node-version: 18.0.0
      - uses: actions/setup-node@v4
        if: runner.os != 'Linux'
        with:
          node-version: 20.0.0
      - run: echo either
  mixed:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-node@v4
        with:
          node-version: 20.0.0
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: echo mixed
  clean:
    runs-on: ubuntu-latest
    steps:
      - run: echo clean
"#;
        let f = fixture_with(workflow, |project| {
            std::fs::write(project.join(".node-version"), "22.0.0\n").unwrap();
            std::fs::write(project.join("go.mod"), "module x\n\ngo 1.22\n").unwrap();
        });
        // Two versions under conditions do not contradict: neither is
        // required, and the project's file is, as before.
        let (scope, lines, notices) = required(&f, "either").remove(0);
        assert_eq!(
            (scope, lines),
            (Scope::project(""), strings(&["node=22.0.0"]))
        );
        assert_eq!(notices.len(), 2, "{notices:?}");
        assert!(notices[0].contains(&format!("{FILE}:6): node: the step has an `if:`")));
        assert!(notices[1].contains(&format!("{FILE}:10): node: the step has an `if:`")));
        // What is read of a job is required; what is not is told.
        let (scope, lines, notices) = required(&f, "mixed").remove(0);
        assert_eq!(
            (scope, lines),
            (job_scope("mixed"), strings(&["node=20.0.0"]))
        );
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains(&format!("{FILE}:21): go: the version file go.mod")));
        // A job the task does not run has no effect on it, and no notice.
        assert_eq!(
            required(&f, "clean"),
            [(Scope::project(""), strings(&["node=22.0.0"]), vec![])]
        );
    }

    #[test]
    fn an_alias_is_required_while_it_resolves_and_parent_and_worker_read_it_alike() {
        use crate::toolchain::versions::MatchPolicy;
        let workflow = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: dtolnay/rust-toolchain@stable\n      - uses: actions/setup-node@v4\n        with:\n          node-version: lts/*\n      - run: echo build\n";
        let f = fixture_with(workflow, |project| {
            std::fs::write(project.join("rust-toolchain"), "1.80.0\n").unwrap();
        });
        let source = source(&f);
        let task = tasks(&f, "build")[0];
        let tool = |name: &str| ToolId::parse(name).unwrap();
        // Resolved: both aliases are requirements of the task.
        let kept = task_declarations(&source, &f.plan, task, "", &resolved);
        assert_eq!(kept.scope, job_scope("build"));
        assert_eq!(lines(&kept), ["node=lts/*", "rust=stable"]);
        assert_eq!(kept.notices, []);
        assert_eq!(kept.aliases(), [tool("rust"), tool("node")]);
        // `stable` stays unresolved: the step requires nothing, the notice
        // says so, and the project's file is the requirement again.
        let no_rust = |tool: &ToolId, alias: &str| tool.as_str() == "rust" && alias == "stable";
        let dropped = task_declarations(&source, &f.plan, task, "", &no_rust);
        assert_eq!(dropped.scope, job_scope("build"));
        assert_eq!(lines(&dropped), ["node=lts/*", "rust=1.80.0"]);
        assert_eq!(dropped.aliases(), [tool("node")]);
        assert_eq!(
            render(&dropped.notices),
            [format!("task `{}` (job build, {FILE}:6): rust: `stable` names no concrete version; no rust version is required from this step", task.id)]
        );
        // Nothing resolves: the task is frozen as the project is.
        let none = task_declarations(&source, &f.plan, task, "", &unresolved);
        assert_eq!(none.scope, Scope::project(""));
        assert_eq!(lines(&none), ["rust=1.80.0"]);
        assert_eq!(none.aliases(), []);
        assert_eq!(none.notices.len(), 2);
        // The holder of a bundle reads which aliases were left out from the
        // bundle's own requirements, and so derives the bundle it was given.
        let digest = crate::ids::Digest::of_bytes(b"source");
        let frozen = |freeze: &TaskFreeze| {
            FrozenToolchains::freeze(
                digest.clone(),
                digest.clone(),
                freeze.scope.clone(),
                &freeze.declarations,
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            )
            .unwrap()
        };
        for freeze in [&kept, &dropped, &none] {
            let bundle = frozen(freeze);
            let read =
                task_declarations_as_frozen(&source, &f.plan, task, "", &bundle.requirements);
            assert_eq!(&read, freeze);
            assert_eq!(check_frozen(&source, &f.plan, task, "", &bundle), Ok(()));
        }
        // A bundle of another scope, or of other requirements, is not this
        // task's.
        let mut other = frozen(&dropped);
        other.scope = Scope::project("");
        assert_eq!(
            check_frozen(&source, &f.plan, task, "", &other),
            Err("toolchain source scope mismatch".to_string())
        );
        let mut other = frozen(&dropped);
        other.requirements[1].constraints = frozen(&kept).requirements[0].constraints.clone();
        assert_eq!(
            check_frozen(&source, &f.plan, task, "", &other),
            Err("toolchain source declarations mismatch".to_string())
        );
        // An alias the bundle holds for another tool is not this tool's.
        let mut other = frozen(&kept);
        other.requirements.swap(0, 1);
        let (node, rust) = (
            other.requirements[1].tool.clone(),
            other.requirements[0].tool.clone(),
        );
        other.requirements[0].tool = node;
        other.requirements[1].tool = rust;
        let read = task_declarations_as_frozen(&source, &f.plan, task, "", &other.requirements);
        assert_eq!(read, none);
    }

    #[test]
    fn two_steps_of_one_task_that_contradict_are_refused_by_the_check() {
        let workflow = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version: 18.0.0\n      - uses: actions/setup-node@v4\n        with:\n          node-version: 20.0.0\n      - run: echo build\n";
        let f = fixture(workflow);
        let source = source(&f);
        let task = tasks(&f, "build")[0];
        let freeze = task_declarations(&source, &f.plan, task, "", &resolved);
        assert_eq!(freeze.notices, []);
        let conflict = effective(&freeze.declarations, &freeze.scope).unwrap_err();
        assert_eq!(conflict.code, "conflicting_versions");
        let bundle = FrozenToolchains {
            schema_version: 1,
            source_manifest: crate::ids::Digest::of_bytes(b"source"),
            profile_definition: crate::ids::Digest::of_bytes(b"profile"),
            scope: freeze.scope.clone(),
            requirements: vec![],
            baseline: vec![],
            selections: vec![],
            policy: Default::default(),
        };
        assert_eq!(
            check_frozen(&source, &f.plan, task, "", &bundle),
            Err("conflicting_versions".to_string())
        );
    }

    #[test]
    fn a_workflow_that_cannot_be_read_is_one_notice_and_no_requirement() {
        let f = fixture_with(WORKFLOW, |project| {
            std::fs::write(project.join("rust-toolchain"), "1.80.0\n").unwrap();
        });
        let (read, unread) = read_task(&f.store, &f.manifest, "", "absent.yml").unwrap();
        assert_eq!(unread.as_deref(), Some("missing_workflow"));
        let mut all = Vec::new();
        for task in tasks(&f, "legs") {
            let mut task = task.clone();
            task.workflow = "absent.yml".into();
            let source = (read.clone(), unread.clone());
            let freeze = task_declarations(&source, &f.plan, &task, "", &resolved);
            // The project's file is required, as for a plain command.
            assert_eq!(freeze.scope, Scope::project(""));
            assert_eq!(lines(&freeze), ["rust=1.80.0"]);
            all.extend(freeze.notices);
        }
        assert_eq!(all.len(), 2);
        assert_eq!(
            render(&all),
            ["workflow absent.yml: the workflow could not be read for its setup steps (missing_workflow); no version is required from them"]
        );
        // A project file that cannot be read is the error it is for a plain
        // command.
        let f = fixture_with(WORKFLOW, |project| {
            std::fs::write(project.join("rust-toolchain"), "not a version\n").unwrap();
        });
        let refused = read_task(&f.store, &f.manifest, "", FILE).unwrap_err();
        assert_eq!(refused.code, "invalid_version");
    }
}
