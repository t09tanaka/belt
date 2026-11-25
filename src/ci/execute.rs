//! Run one task of a frozen plan in a workspace the host made, through the
//! core's pinned execution. Every step goes to the caller's [`StepExecutor`]:
//! nothing here starts a process, and nothing reads the process environment.
//!
//! The core binds a workspace only while it is the captured source, byte for
//! byte ([`PinnedExecution::verify_workspace`]): before any secret file is
//! placed and before any setup result is copied in. The only exception is
//! what preparation wrote ([`TaskPaths::prepared`], spec 22.5): those paths
//! are left out of the comparison, and every other byte must still match. A
//! workspace that is not that is refused here, never run unverified.

use crate::source::SourceStore;
use local_ci::exec::{Opts, Workspace};
use local_ci::source::{RelPath, SourceManifest};
use local_ci::source_execute::{PinnedExecution, RefusePinnedForks, WorkspaceOverlay};
use local_ci::{
    CancelHandle, ExecContext, FrozenPlan, PinnedPlanOptions, StepExecutor, TaskInputs, TaskResult,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

/// GitHub's default `timeout-minutes` of a job.
const JOB_TIMEOUT_MINUTES: f64 = 360.0;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecuteError {
    #[error("the plan does not belong to the source of this attempt: {0}")]
    Binding(String),
    #[error("the workspace is not the captured source: {0}")]
    Workspace(String),
    #[error("the task could not be run: {0}")]
    Run(String),
}

/// The host side of one attempt: its paths (all absolute; `root` exists, the
/// others are the attempt's own and are made when needed) and the whole
/// environment a step starts from. `Debug` is not derived: the environment
/// may hold what must not be printed.
#[derive(Clone)]
pub struct TaskPaths {
    /// The attempt's workspace: the captured source, plus only what
    /// preparation wrote at `prepared`.
    pub root: PathBuf,
    /// Relative paths `fast-worktree add` wrote before the source was laid
    /// (dependencies, hook output), recorded when the workspace was made.
    /// Left out of the byte check against the captured source; nothing else
    /// is.
    pub prepared: Vec<String>,
    /// Logs of the steps and the run's own files.
    pub run_dir: PathBuf,
    /// Scripts and shims of the attempt.
    pub tools: PathBuf,
    pub lock_file: PathBuf,
    pub trash: PathBuf,
    pub env: BTreeMap<String, String>,
    /// Stops the task: once it is cancelled the core starts nothing more
    /// (no step, no helper process of its own) and takes down what it has
    /// up. The host keeps a clone and cancels through it.
    pub cancel: CancelHandle,
}

/// Freeze the plan of a captured source: the workflows `options` select,
/// read from the objects of the capture and from nothing else.
pub fn freeze_plan(
    store: &SourceStore,
    manifest: &SourceManifest,
    options: &PinnedPlanOptions,
) -> Result<FrozenPlan, ExecuteError> {
    let view = store
        .object_store()
        .map_err(|e| ExecuteError::Binding(e.to_string()))?;
    view.with_sdk(|objects| {
        Ok(local_ci::build_frozen_plan_from_source(
            objects, manifest, options,
        ))
    })
    .map_err(|e| ExecuteError::Binding(e.to_string()))?
    .map_err(|e| ExecuteError::Binding(format!("{e:#}")))
}

pub(crate) enum SelectedTask<'a> {
    Plan(&'a str),
    Derived(&'a local_ci::driver::DerivedDispatch),
}

pub(crate) fn decode_dispatch(
    value: &serde_json::Value,
    task: &str,
) -> Result<local_ci::driver::DerivedDispatch, String> {
    let dispatch = local_ci::driver::DerivedDispatch::from_json(&value.to_string())
        .map_err(|e| format!("invalid matrix dispatch: {e:#}"))?;
    if dispatch.task_id() != task {
        return Err("matrix dispatch names a different task".into());
    }
    Ok(dispatch)
}

/// The local CI event supplies no caller-provided GitHub, vars or inputs overrides.
/// Source-bound workflow scheduling still comes from the SDK evaluation source.
pub(crate) fn portable_facts() -> local_ci::driver::PortableFacts {
    local_ci::driver::PortableFacts {
        schema_version: local_ci::driver::DISPATCH_VERSION,
        github: Default::default(),
        vars: Default::default(),
        inputs: Default::default(),
    }
}

/// Run task `task_id` of `plan` in `paths.root` with `inputs` as the results
/// of the jobs it needs.
/// A matrix that would need a second workspace is refused by the core
/// ([`RefusePinnedForks`]).
pub fn execute_task(
    store: &SourceStore,
    manifest: &SourceManifest,
    plan: &FrozenPlan,
    task_id: &str,
    inputs: &TaskInputs,
    paths: TaskPaths,
    executor: Arc<dyn StepExecutor>,
) -> Result<TaskResult, ExecuteError> {
    execute_selected(
        store,
        manifest,
        plan,
        SelectedTask::Plan(task_id),
        inputs,
        paths,
        executor,
    )
}

pub(crate) fn execute_selected(
    store: &SourceStore,
    manifest: &SourceManifest,
    plan: &FrozenPlan,
    task: SelectedTask<'_>,
    inputs: &TaskInputs,
    paths: TaskPaths,
    executor: Arc<dyn StepExecutor>,
) -> Result<TaskResult, ExecuteError> {
    // (an attempt's task holds no lock for its life: spec 25.8 5b (a))
    let view = store.object_store_per_call();
    let ran = view.with_sdk(|objects| {
        let mut ctx = ExecContext::new(paths.env.clone());
        // The handle the host holds, not one of the context's own.
        ctx.cancel = paths.cancel.clone();
        let run = || -> Result<TaskResult, ExecuteError> {
            let input = PinnedExecution::new(plan, manifest, objects)
                .map_err(|e| ExecuteError::Binding(format!("{e:#}")))?;
            let workspace = Workspace {
                root: paths.root.clone(),
                // No second checkout is ever made from it: forks are refused.
                repo_root: paths.root.clone(),
                run_dir: paths.run_dir.clone(),
                tools: paths.tools.clone(),
                lock_file: paths.lock_file.clone(),
            };
            let ws = if paths.prepared.is_empty() {
                input.verify_workspace(&ctx, workspace)
            } else {
                let overlay = WorkspaceOverlay {
                    paths: paths.prepared.iter().map(RelPath::new).collect(),
                    secret_files: Vec::new(),
                };
                input.verify_workspace_with_overlay(&ctx, workspace, &overlay)
            }
            .map_err(|e| ExecuteError::Workspace(format!("{e:#}")))?;
            let opts = Opts {
                parallel: 1,
                traffic_controller: None,
                executor: Some(executor),
                portool: None,
                containers: 1,
                job_timeout_min: JOB_TIMEOUT_MINUTES,
                keep: true,
                trash: paths.trash.clone(),
                all_os_legs: false,
            };
            match task {
                SelectedTask::Plan(task_id) => local_ci::source_execute::execute_pinned_task(
                    &ctx,
                    &input,
                    task_id,
                    inputs,
                    None,
                    &ws,
                    opts,
                    &RefusePinnedForks,
                ),
                SelectedTask::Derived(dispatch) => {
                    local_ci::source_execute::execute_pinned_dispatch(
                        &ctx,
                        &input,
                        dispatch,
                        inputs,
                        &portable_facts(),
                        None,
                        &ws,
                        opts,
                        &RefusePinnedForks,
                    )
                }
            }
            .map_err(|e| ExecuteError::Run(format!("{e:#}")))
        };
        Ok(run())
    });
    ran.map_err(|e| ExecuteError::Binding(e.to_string()))?
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::source::{capture, CaptureParams, SourceRequest};
    use local_ci::exec::Exit;
    use local_ci::frozen::Filters;
    use local_ci::{JobStatus, PinnedPlanOptions, StepExit, StepRequest};
    use std::path::Path;
    use std::sync::Mutex;

    /// Starts the step as a plain child: the contract of the trait, without
    /// a queue. Records what it was asked.
    #[derive(Default)]
    struct Plain(Mutex<Vec<Vec<String>>>);
    impl StepExecutor for Plain {
        fn run(&self, req: &StepRequest) -> anyhow::Result<StepExit> {
            self.0.lock().unwrap().push(req.argv.clone());
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&req.log)?;
            let status = std::process::Command::new(&req.argv[0])
                .args(&req.argv[1..])
                .current_dir(&req.cwd)
                .env_clear()
                .envs(req.env.iter().cloned())
                .stdin(std::process::Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log)
                .status()?;
            Ok(StepExit {
                exit: Exit::Code(status.code().unwrap_or(1)),
                run_secs: 0.0,
                queue_wait_secs: None,
            })
        }
    }

    pub(crate) const WORKFLOW: &str = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    outputs:\n      version: ${{ steps.v.outputs.version }}\n    steps:\n      - id: v\n        run: echo \"version=1.2.3\" >> \"$GITHUB_OUTPUT\"\n  test:\n    needs: build\n    runs-on: ubuntu-latest\n    steps:\n      - run: test \"${{ needs.build.outputs.version }}\" = 1.2.3 && echo ran > seen.txt\n";

    pub(crate) struct Fixture {
        _root: tempfile::TempDir,
        pub(crate) store: SourceStore,
        pub(crate) manifest: SourceManifest,
        pub(crate) plan: FrozenPlan,
        pub(crate) area: PathBuf,
    }

    pub(crate) fn fixture(workflow: &str) -> Fixture {
        fixture_with(workflow, |_| {})
    }

    pub(crate) fn fixture_with(workflow: &str, add_files: impl FnOnce(&Path)) -> Fixture {
        let root = crate::test_tmp::tempdir().unwrap();
        let area = std::fs::canonicalize(root.path()).unwrap();
        let project = area.join("project");
        std::fs::create_dir_all(project.join(".github/workflows")).unwrap();
        std::fs::write(project.join(".github/workflows/ci.yml"), workflow).unwrap();
        add_files(&project);
        let store = SourceStore::open(&area.join("state")).unwrap();
        let captured = capture(
            &std::env::vars().collect(),
            &store,
            &SourceRequest::Directory { dir: project },
            &CaptureParams::default(),
        )
        .unwrap();
        let manifest = store
            .manifest(&captured.manifest.ci_source_id)
            .unwrap()
            .unwrap();
        let options = PinnedPlanOptions {
            workflow_dirs_override: None,
            filters: Filters {
                all: true,
                include_disabled: false,
                workflows_dirs: vec![],
                workflows: vec![],
                jobs: vec![],
            },
            disabled: vec![],
            base_branch: None,
            changed_files: None,
        };
        let plan = freeze_plan(&store, &manifest, &options).unwrap();
        Fixture {
            _root: root,
            store,
            manifest,
            plan,
            area,
        }
    }

    fn paths(area: &Path, name: &str) -> TaskPaths {
        let at = area.join(name);
        TaskPaths {
            root: at.join("ws"),
            prepared: Vec::new(),
            run_dir: at.join("run"),
            tools: at.join("tools"),
            lock_file: at.join("lock"),
            trash: at.join("trash"),
            env: env(),
            cancel: CancelHandle::new(),
        }
    }

    fn env() -> BTreeMap<String, String> {
        [
            ("PATH".to_string(), std::env::var("PATH").unwrap()),
            ("HOME".to_string(), "/nonexistent".to_string()),
        ]
        .into()
    }

    pub(crate) fn task_of<'a>(plan: &'a FrozenPlan, job: &str) -> &'a str {
        &plan
            .tasks
            .iter()
            .find(|t| t.jobs.iter().any(|j| j == job))
            .unwrap()
            .id
    }

    #[test]
    fn derived_dispatch_executes_only_its_source_bound_variant() {
        use local_ci::driver::{ActionKind, Driver, DriverConfig, Input, DRIVER_VERSION};
        let f = fixture(
            r#"on: push
jobs:
  leg:
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJSON('{"include":[{"n":0},{"n":1}]}') }}
    steps:
      - run: echo ${{ matrix.n }} > member
"#,
        );
        let mut driver = f
            .store
            .object_store()
            .unwrap()
            .with_sdk(|objects| {
                let pin = PinnedExecution::new(&f.plan, &f.manifest, objects)?;
                Driver::with_evaluation(
                    &f.plan,
                    DriverConfig {
                        schema_version: DRIVER_VERSION,
                        max_parallel: 2,
                    },
                    &pin.evaluation_source()?,
                    portable_facts(),
                )
            })
            .unwrap();
        let advance = driver.propose(Input::Advance).unwrap();
        driver.apply(&advance).unwrap();
        let exec = Arc::new(Plain::default());
        let members = driver
            .pending_actions()
            .iter()
            .filter_map(|a| match &a.kind {
                ActionKind::DerivedReady { dispatch, inputs } => Some((dispatch, inputs)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(members.len(), 2);
        for (dispatch, inputs) in members {
            let paths = paths(&f.area, &format!("leg-{}", dispatch.index()));
            f.store.materialize(&f.manifest, &paths.root).unwrap();
            let produced = paths.root.join("member");
            let json = serde_json::to_value(dispatch).unwrap();
            assert!(decode_dispatch(&json, "another-task").is_err());
            let decoded = decode_dispatch(&json, dispatch.task_id()).unwrap();
            let mut wrong = inputs.clone();
            wrong.run_cancelled = !wrong.run_cancelled;
            assert!(execute_selected(
                &f.store,
                &f.manifest,
                &f.plan,
                SelectedTask::Derived(&decoded),
                &wrong,
                paths.clone(),
                exec.clone()
            )
            .is_err());
            assert!(!produced.exists());
            let result = execute_selected(
                &f.store,
                &f.manifest,
                &f.plan,
                SelectedTask::Derived(&decoded),
                inputs,
                paths,
                exec.clone(),
            )
            .unwrap();
            assert_eq!(result.task, dispatch.task_id());
            assert_eq!(result.outcomes[0].status, JobStatus::Success);
            assert_eq!(
                std::fs::read_to_string(produced).unwrap().trim(),
                dispatch.index().to_string()
            );
        }
    }

    #[test]
    fn a_task_runs_in_a_materialized_workspace_and_hands_its_outputs_to_the_next() {
        let f = fixture(WORKFLOW);
        assert_eq!(f.plan.tasks.len(), 2, "needs does not merge the jobs");
        let exec = Arc::new(Plain::default());

        let up = paths(&f.area, "up");
        f.store.materialize(&f.manifest, &up.root).unwrap();
        let built = execute_task(
            &f.store,
            &f.manifest,
            &f.plan,
            task_of(&f.plan, "build"),
            &TaskInputs::default(),
            up,
            exec.clone(),
        )
        .unwrap();
        assert_eq!(built.outcomes.len(), 1);
        assert_eq!(built.outcomes[0].status, JobStatus::Success);
        assert_eq!(built.outcomes[0].outputs["version"], "1.2.3");

        // Another workspace, as on another host: only the outcome crosses.
        let down = paths(&f.area, "down");
        f.store.materialize(&f.manifest, &down.root).unwrap();
        let inputs = TaskInputs::new(&built.outcomes, &built.outcomes, false);
        let tested = execute_task(
            &f.store,
            &f.manifest,
            &f.plan,
            task_of(&f.plan, "test"),
            &inputs,
            down.clone(),
            exec.clone(),
        )
        .unwrap();
        assert_eq!(tested.outcomes[0].status, JobStatus::Success);
        assert_eq!(
            std::fs::read_to_string(down.root.join("seen.txt")).unwrap(),
            "ran\n"
        );
        assert_eq!(exec.0.lock().unwrap().len(), 2, "one step per task");
    }

    #[test]
    fn a_workspace_that_is_not_the_captured_source_is_refused_before_any_step() {
        let f = fixture(WORKFLOW);
        let exec = Arc::new(Plain::default());
        let p = paths(&f.area, "dirty");
        f.store.materialize(&f.manifest, &p.root).unwrap();
        std::fs::write(p.root.join(".env"), "TOKEN=x").unwrap();
        let refused = execute_task(
            &f.store,
            &f.manifest,
            &f.plan,
            task_of(&f.plan, "build"),
            &TaskInputs::default(),
            p,
            exec.clone(),
        );
        assert!(
            matches!(refused, Err(ExecuteError::Workspace(_))),
            "{refused:?}"
        );
        assert!(exec.0.lock().unwrap().is_empty());
    }

    #[test]
    fn a_plan_of_another_source_is_refused() {
        let f = fixture(WORKFLOW);
        let other = fixture(&WORKFLOW.replace("1.2.3", "9.9.9"));
        let exec = Arc::new(Plain::default());
        let p = paths(&f.area, "other");
        f.store.materialize(&f.manifest, &p.root).unwrap();
        let refused = execute_task(
            &f.store,
            &f.manifest,
            &other.plan,
            task_of(&other.plan, "build"),
            &TaskInputs::default(),
            p,
            exec.clone(),
        );
        assert!(
            matches!(refused, Err(ExecuteError::Binding(_))),
            "{refused:?}"
        );
        assert!(exec.0.lock().unwrap().is_empty());
    }

    fn git_env(home: &Path) -> BTreeMap<String, String> {
        [
            ("PATH", std::env::var("PATH").unwrap()),
            ("HOME", home.display().to_string()),
            ("GIT_CONFIG_GLOBAL", "/dev/null".to_string()),
            ("GIT_CONFIG_NOSYSTEM", "1".to_string()),
            ("GIT_AUTHOR_NAME", "t".to_string()),
            ("GIT_AUTHOR_EMAIL", "t@example.com".to_string()),
            ("GIT_COMMITTER_NAME", "t".to_string()),
            ("GIT_COMMITTER_EMAIL", "t@example.com".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    fn git(env: &BTreeMap<String, String>, dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env_clear()
            .envs(env)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim_end().to_string()
    }

    /// `fast-worktree` with Git alone, then `prepare` over the new worktree:
    /// what its dependency hooks and `[[clone]]` rules would write.
    struct GitOnly(BTreeMap<String, String>, fn(&Path));
    impl crate::workspace::FastWorktree for GitOnly {
        fn add(
            &self,
            repo: &Path,
            dest: &Path,
            branch: &str,
            start: &str,
        ) -> Result<(), crate::workspace::plan::FwFailure> {
            let dest = dest.to_str().unwrap();
            git(
                &self.0,
                repo,
                &["worktree", "add", "-q", "-b", branch, dest, start],
            );
            (self.1)(Path::new(dest));
            Ok(())
        }
        fn remove(
            &self,
            repo: &Path,
            path: &Path,
        ) -> Result<(), crate::workspace::plan::FwFailure> {
            git(
                &self.0,
                repo,
                &["worktree", "remove", "--force", path.to_str().unwrap()],
            );
            Ok(())
        }
    }

    /// The default input of `ci`: a Git worktree with staged, unstaged and
    /// untracked changes, in the workspace a worker makes of it (its clone,
    /// the bundle, HEAD and index put back to the original's) with
    /// `prepare` as the preparation, and `tamper` done to the workspace
    /// after it was made. The task `build` run there.
    fn run_in_worker_workspace(
        prepare: fn(&Path),
        tamper: fn(&Path),
    ) -> (
        crate::workspace::Workspace,
        Result<TaskResult, ExecuteError>,
    ) {
        let root = crate::test_tmp::tempdir().unwrap();
        let area = std::fs::canonicalize(root.path()).unwrap();
        std::fs::create_dir_all(area.join("home")).unwrap();
        let env = git_env(&area.join("home"));
        let work = area.join("work");
        std::fs::create_dir_all(work.join(".github/workflows")).unwrap();
        git(&env, &work, &["init", "-q", "-b", "main"]);
        std::fs::write(work.join(".github/workflows/ci.yml"), WORKFLOW).unwrap();
        std::fs::write(work.join("a.txt"), "a\n").unwrap();
        std::fs::write(work.join(".gitignore"), "node_modules\n").unwrap();
        git(&env, &work, &["add", "."]);
        git(&env, &work, &["commit", "-qm", "base"]);
        std::fs::write(work.join("a.txt"), "staged\n").unwrap();
        git(&env, &work, &["add", "a.txt"]);
        std::fs::write(work.join("a.txt"), "unstaged\n").unwrap();
        std::fs::write(work.join("new.txt"), "untracked\n").unwrap();
        std::fs::create_dir_all(work.join("gen")).unwrap();
        std::fs::write(work.join("gen/kept.txt"), "untracked\n").unwrap();

        let store = SourceStore::open(&area.join("state")).unwrap();
        let captured = capture(
            &env,
            &store,
            &SourceRequest::Worktree { dir: work.clone() },
            &CaptureParams::default(),
        )
        .unwrap();
        let manifest = store
            .manifest(&captured.manifest.ci_source_id)
            .unwrap()
            .unwrap();
        let options = PinnedPlanOptions {
            workflow_dirs_override: None,
            filters: Filters {
                all: true,
                include_disabled: false,
                workflows_dirs: vec![],
                workflows: vec![],
                jobs: vec![],
            },
            disabled: vec![],
            base_branch: None,
            changed_files: None,
        };
        let plan = store
            .object_store()
            .unwrap()
            .with_sdk(|objects| {
                Ok(local_ci::build_frozen_plan_from_source(
                    objects, &manifest, &options,
                ))
            })
            .unwrap()
            .unwrap();

        // What a worker does with the source: its clone, the bundle, the
        // attempt's worktree.
        let workspaces = crate::workspace::Workspaces::new(
            area.join("worker"),
            env.clone(),
            Arc::new(GitOnly(env.clone(), prepare)),
        );
        let repo = workspaces
            .ensure_clone(manifest.repository_id.as_deref().unwrap(), None)
            .unwrap();
        let bundle = store
            .export_bundle(&ExecContext::new(env.clone()), &manifest, &[])
            .unwrap();
        repo.receive_bundle(&bundle, &manifest, &[]).unwrap();
        let ws = workspaces
            .create(
                &crate::ids::AttemptId::generate(),
                &store,
                &manifest,
                Some(&repo),
            )
            .unwrap();
        assert!(ws.index_reproduced, "{:?}", ws.warnings);
        assert_eq!(
            std::fs::read_to_string(ws.root.join("a.txt")).unwrap(),
            "unstaged\n"
        );

        tamper(&ws.root);
        let exec = Arc::new(Plain::default());
        let at = area.join("attempt");
        let built = execute_task(
            &store,
            &manifest,
            &plan,
            task_of(&plan, "build"),
            &TaskInputs::default(),
            TaskPaths {
                root: ws.root.clone(),
                prepared: ws.prepared.clone(),
                run_dir: at.join("run"),
                tools: at.join("tools"),
                lock_file: at.join("lock"),
                trash: at.join("trash"),
                env,
                cancel: CancelHandle::new(),
            },
            exec.clone(),
        );
        if built.is_err() {
            assert!(exec.0.lock().unwrap().is_empty(), "a step ran");
        }
        (ws, built)
    }

    fn nothing(_: &Path) {}

    /// What `fast-worktree add` writes for a project with npm dependencies:
    /// an ignored `node_modules` and an untracked file of a hook.
    fn installs_dependencies(dest: &Path) {
        std::fs::create_dir_all(dest.join("node_modules/dep")).unwrap();
        std::fs::write(dest.join("node_modules/dep/index.js"), "x\n").unwrap();
        std::fs::write(dest.join("hook.log"), "installed\n").unwrap();
    }

    fn built(result: Result<TaskResult, ExecuteError>) {
        let built = result.unwrap();
        assert_eq!(built.outcomes[0].status, JobStatus::Success);
        assert_eq!(built.outcomes[0].outputs["version"], "1.2.3");
    }

    fn refused(result: Result<TaskResult, ExecuteError>) {
        assert!(
            matches!(result, Err(ExecuteError::Workspace(_))),
            "{:?}",
            result.map(|_| ())
        );
    }

    #[test]
    fn a_task_runs_in_the_workspace_a_worker_makes_of_a_dirty_git_worktree() {
        let (ws, result) = run_in_worker_workspace(nothing, nothing);
        assert!(ws.prepared.is_empty(), "{:?}", ws.prepared);
        built(result);
    }

    /// Run 4: `fast-worktree add` installed `node_modules` for the npm
    /// projects of the repository, and every task there was refused as "not
    /// the captured source". What preparation wrote is recorded and left
    /// out of the check (spec 22.5).
    #[test]
    fn what_preparation_wrote_is_not_taken_for_a_changed_source() {
        let (ws, result) = run_in_worker_workspace(installs_dependencies, nothing);
        assert_eq!(ws.prepared, ["hook.log", "node_modules"]);
        built(result);
    }

    #[test]
    fn a_file_added_after_preparation_is_still_refused() {
        let (_, result) = run_in_worker_workspace(installs_dependencies, |root| {
            std::fs::write(root.join("late.txt"), "late\n").unwrap();
        });
        refused(result);
    }

    #[test]
    fn a_changed_captured_file_is_still_refused_after_preparation() {
        let (_, result) = run_in_worker_workspace(installs_dependencies, |root| {
            std::fs::write(root.join("a.txt"), "changed\n").unwrap();
        });
        refused(result);
    }

    /// The worktree is made at the execution commit, which holds every
    /// captured file: what preparation writes next to captured files is
    /// recorded file by file, and a captured file it overwrote is never
    /// recorded (the source laid over it puts the captured bytes back).
    #[test]
    fn a_prepared_path_never_hides_a_captured_file() {
        let (ws, result) = run_in_worker_workspace(
            |dest| {
                std::fs::create_dir_all(dest.join("gen")).unwrap();
                std::fs::write(dest.join("gen/junk.txt"), "junk\n").unwrap();
            },
            nothing,
        );
        assert_eq!(ws.prepared, ["gen/junk.txt"]);
        built(result);
        let (ws, result) = run_in_worker_workspace(
            |dest| std::fs::write(dest.join("new.txt"), "junk\n").unwrap(),
            nothing,
        );
        assert!(ws.prepared.is_empty(), "{:?}", ws.prepared);
        built(result);
    }

    /// A listed path that is a captured entry, or a directory above one, is
    /// never recorded: leaving it out would leave captured bytes unchecked.
    #[test]
    fn a_path_that_holds_a_captured_entry_is_never_recorded() {
        let f = fixture(WORKFLOW);
        let listing = ".github/\0.github/workflows/ci.yml\0.git\0node_modules/\0";
        assert_eq!(
            crate::workspace::prepared_of(listing, &f.manifest),
            [".git", "node_modules"]
        );
    }
}
