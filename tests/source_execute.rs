//! Fixed inputs are planned without a Git checkout or an execution context.
#![allow(clippy::disallowed_methods)]
mod common;
use local_ci::frozen::Filters;
use local_ci::source::{self, CaptureOptions, ObjectStore, SourceManifest};
use local_ci::{build_frozen_plan_from_source as build, ExecContext, PinnedPlanOptions};
use std::path::{Path, PathBuf};

const WORKFLOW: &str = "name: fixed\non: push\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo captured\n";
fn options() -> PinnedPlanOptions {
    PinnedPlanOptions {
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
    }
}
struct Fixture {
    _tmp: common::Tmp,
    root: PathBuf,
    store: ObjectStore,
    ctx: ExecContext,
}
impl Fixture {
    fn new(workflow: &str) -> Self {
        let tmp = common::tempdir().unwrap();
        let root = tmp.path().join("input");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        std::fs::write(root.join(".github/workflows/ci.yml"), workflow).unwrap();
        let store = ObjectStore::open(tmp.path().join("objects")).unwrap();
        let ctx = ExecContext::new(
            [
                ("PATH".into(), String::new()),
                ("GITHUB_SHA".into(), "live-host-sha-must-not-leak".into()),
                ("HOME".into(), tmp.path().display().to_string()),
            ]
            .into(),
        );
        Self {
            _tmp: tmp,
            root,
            store,
            ctx,
        }
    }
    fn capture(&self) -> SourceManifest {
        source::capture_directory(
            &self.ctx,
            &self.root,
            &CaptureOptions {
                store: Some(self.store.root().into()),
                ..Default::default()
            },
        )
        .unwrap()
        .manifest
    }
}

use local_ci::exec::{Exit, Opts, Workspace};
use local_ci::source_execute::{
    execute_pinned_setup, execute_pinned_task, PinnedExecution, RefusePinnedForks,
};
use std::sync::{Arc, Mutex};
type RecordedStep = (PathBuf, Vec<(String, String)>);
#[derive(Default)]
struct Recorder(Mutex<Vec<RecordedStep>>);
impl local_ci::StepExecutor for Recorder {
    fn run(&self, req: &local_ci::StepRequest) -> anyhow::Result<local_ci::StepExit> {
        assert_eq!(
            std::fs::read_to_string(req.cwd.join("payload")).unwrap(),
            "captured"
        );
        if req.cwd.join("secret").exists() {
            assert_eq!(
                std::fs::read_to_string(req.cwd.join("secret")).unwrap(),
                "restored by host"
            );
            assert!(!req.cwd.join("mutable-marker").exists());
            std::fs::write(req.cwd.join("mutable-marker"), "independent").unwrap();
        }
        self.0
            .lock()
            .unwrap()
            .push((req.cwd.clone(), req.env.clone()));
        Ok(local_ci::StepExit {
            exit: Exit::Code(0),
            run_secs: 0.,
            queue_wait_secs: None,
        })
    }
}
fn ws(f: &Fixture, name: &str) -> Workspace {
    let area = f._tmp.path().join(name);
    Workspace {
        root: area.join("root"),
        repo_root: area.join("no-live-repo"),
        run_dir: area.join("run"),
        tools: area.join("tools"),
        lock_file: area.join("lock"),
    }
}
fn opts(f: &Fixture, recorder: Arc<Recorder>) -> Opts {
    Opts {
        parallel: 2,
        traffic_controller: None,
        executor: Some(recorder),
        portool: None,
        containers: 1,
        job_timeout_min: 1.,
        keep: false,
        trash: f._tmp.path().join("trash"),
    }
}
#[test]
fn directory_setup_and_task_use_only_materialized_bytes_without_fake_sha() {
    let f = Fixture::new(&WORKFLOW.replace("echo captured", "npm ci"));
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    std::fs::write(f.root.join("payload"), "changed live input").unwrap();
    let recorder = Arc::new(Recorder::default());
    let recipe = local_ci::setup::derive_setup_recipe(&p, &p.tasks[0].id).unwrap();
    let context = local_ci::setup::TemplateContext::local(&p, "attempt");
    let setup = execute_pinned_setup(
        &f.ctx,
        &input,
        &recipe,
        &recipe.template_key(&context),
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks,
    )
    .unwrap();
    assert_eq!(
        setup.receipt.source,
        format!("manifest:{}", m.manifest_digest)
    );
    let result = execute_pinned_task(
        &f.ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        Some((&setup.receipt, &context)),
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks,
    )
    .unwrap();
    assert!(
        result.receipt_ignored.is_none(),
        "{:?}",
        result.receipt_ignored
    );
    let calls = recorder.0.lock().unwrap();
    assert!(!calls.is_empty());
    assert!(calls
        .iter()
        .all(|(_, env)| !env.iter().any(|(k, _)| k == "GITHUB_SHA")));
    drop(calls);
    let mut wrong = setup.receipt.clone();
    wrong.source = "other".into();
    let result = execute_pinned_task(
        &f.ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        Some((&wrong, &context)),
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks,
    )
    .unwrap();
    assert!(result.receipt_ignored.is_some());
}
#[test]
fn forged_manifest_and_replaced_root_are_rejected_before_executor() {
    let f = Fixture::new(WORKFLOW);
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let mut bad = m.clone();
    bad.repository_id = Some("github:example/forged".into());
    assert!(PinnedExecution::new(&p, &bad, &f.store).is_err());
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    assert!(input
        .materialize_workspace(workspace.workspace().clone())
        .is_err());
    let recorder = Arc::new(Recorder::default());
    let mut recipe = local_ci::setup::derive_setup_recipe(&p, &p.tasks[0].id).unwrap();
    recipe.left_in_place.clear();
    recipe.task = "forged-task".into();
    let context = local_ci::setup::TemplateContext::local(&p, "attempt");
    assert!(execute_pinned_setup(
        &f.ctx,
        &input,
        &recipe,
        &recipe.template_key(&context),
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks
    )
    .is_err());
    assert!(recorder.0.lock().unwrap().is_empty());
    let root = &workspace.workspace().root;
    std::fs::rename(root, root.with_file_name("saved-root")).unwrap();
    std::fs::create_dir(root).unwrap();
    let recorder = Arc::new(Recorder::default());
    assert!(execute_pinned_task(
        &f.ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        None,
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks
    )
    .is_err());
    assert!(recorder.0.lock().unwrap().is_empty());
}
fn git(dir: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("/usr/bin/git")
        .args([
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}

#[test]
fn dirty_git_capture_executes_after_live_checkout_changes() {
    let f = Fixture::new(WORKFLOW);
    std::fs::write(f.root.join("payload"), "base").unwrap();
    git(&f.root, &["init", "-q", "-b", "main"]);
    git(&f.root, &["add", "."]);
    git(&f.root, &["commit", "-qm", "initial"]);
    let base = git(&f.root, &["rev-parse", "HEAD"]);
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let ctx = ExecContext::new(
        [
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), f._tmp.path().display().to_string()),
        ]
        .into(),
    );
    let m = source::capture_source(
        &ctx,
        &f.root,
        &CaptureOptions {
            store: Some(f.store.root().into()),
            base: Some(base),
            ..Default::default()
        },
    )
    .unwrap()
    .manifest;
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    std::fs::write(f.root.join("payload"), "live changed again").unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    let recorder = Arc::new(Recorder::default());
    execute_pinned_task(
        &f.ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        None,
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks,
    )
    .unwrap();
    let seen = recorder.0.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(seen[0]
        .1
        .iter()
        .any(|(k, v)| k == "GITHUB_SHA" && Some(v) == m.execution_commit.as_ref()));
}
const MATRIX: &str = "name: fixed\non: push\njobs:\n  check:\n    runs-on: ubuntu-latest\n    strategy:\n      fail-fast: false\n      max-parallel: 2\n      matrix: ${{ fromJSON('{\"leg\":[1,2]}') }}\n    steps:\n      - run: echo matrix\n";
struct Forks {
    store: ObjectStore,
    manifest: SourceManifest,
    removed: Mutex<Vec<PathBuf>>,
}
impl local_ci::source_execute::PinnedWorkspaceProvider for Forks {
    fn fork(&self, _: &Path, child: &Path) -> anyhow::Result<()> {
        source::materialize_from(&self.store, &self.manifest, child)?;
        // A host restores each child's secret after validating its source.
        std::fs::write(child.join("secret"), "restored by host")?;
        Ok(())
    }
    fn remove(&self, child: &Path) -> anyhow::Result<()> {
        self.removed.lock().unwrap().push(child.into());
        std::fs::remove_dir_all(child)?;
        Ok(())
    }
}
#[test]
fn internal_matrix_uses_independent_host_forks_and_cleans_them() {
    let f = Fixture::new(MATRIX);
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    assert!(p.tasks[0].dynamic_matrix);
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    let recorder = Arc::new(Recorder::default());
    let provider = Forks {
        store: f.store.clone(),
        manifest: m.clone(),
        removed: Mutex::new(vec![]),
    };
    execute_pinned_task(
        &f.ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        None,
        &workspace,
        opts(&f, recorder.clone()),
        &provider,
    )
    .unwrap();
    let seen = recorder.0.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_ne!(seen[0].0, seen[1].0);
    assert!(seen.iter().all(|(path, _)| !path.exists()));
    assert_eq!(provider.removed.lock().unwrap().len(), 2);
    assert!(!workspace.workspace().root.join("secret").exists());
}
#[test]
fn secret_source_is_not_restored_and_default_matrix_fork_is_refused() {
    let f = Fixture::new(MATRIX);
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    std::fs::write(f.root.join("secret"), "private").unwrap();
    let m = source::capture_directory(
        &f.ctx,
        &f.root,
        &CaptureOptions {
            store: Some(f.store.root().into()),
            secret_files: vec![source::SecretSpec {
                glob: "secret".into(),
                required: true,
            }],
            secret_key: vec![3; 32],
            ..Default::default()
        },
    )
    .unwrap()
    .manifest;
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    assert!(!workspace.workspace().root.join("secret").exists());
    let recorder = Arc::new(Recorder::default());
    let result = execute_pinned_task(
        &f.ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        None,
        &workspace,
        opts(&f, recorder.clone()),
        &RefusePinnedForks,
    )
    .unwrap();
    assert!(recorder.0.lock().unwrap().is_empty());
    assert!(result
        .jobs
        .iter()
        .all(|j| matches!(j.outcome, local_ci::exec::Outcome::Error(_))));
}
