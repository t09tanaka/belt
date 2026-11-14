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
    let owned = imported_workspace(&f, &m, "attempt", &ctx);
    let workspace = input.materialize_git_workspace(&ctx, owned).unwrap();
    assert_eq!(
        git(&workspace.workspace().root, &["rev-parse", "HEAD"]),
        m.execution_commit.as_deref().unwrap()
    );
    assert!(git(&workspace.workspace().root, &["status", "--porcelain"]).is_empty());
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

#[test]
fn materialized_capability_does_not_read_source_objects_on_execution_guards() {
    let f = Fixture::new(&WORKFLOW.replace("echo captured", "npm ci"));
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    std::fs::rename(
        f.store.root().join("objects"),
        f.store.root().join("unavailable-objects"),
    )
    .unwrap();
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
    assert!(result.receipt_ignored.is_none());
    assert!(!recorder.0.lock().unwrap().is_empty());
    assert!(input.materialize_workspace(ws(&f, "another-root")).is_err());
}

#[derive(Default)]
struct NamespaceRecorder(Mutex<Vec<(PathBuf, PathBuf, String)>>);
impl local_ci::StepExecutor for NamespaceRecorder {
    fn run(&self, req: &local_ci::StepRequest) -> anyhow::Result<local_ci::StepExit> {
        let script = PathBuf::from(req.argv.last().unwrap());
        assert!(script.is_file());
        let project = req
            .env
            .iter()
            .find(|(k, _)| k == "COMPOSE_PROJECT_NAME")
            .unwrap()
            .1
            .clone();
        self.0
            .lock()
            .unwrap()
            .push((req.cwd.clone(), script, project));
        Ok(local_ci::StepExit {
            exit: Exit::Code(0),
            run_secs: 0.,
            queue_wait_secs: None,
        })
    }
}

#[test]
fn simultaneous_captured_matrix_invocations_isolate_workspace_tools_and_compose_names() {
    for real_git in [false, true] {
        let f = Fixture::new(MATRIX);
        std::fs::write(f.root.join("payload"), "captured").unwrap();
        let m = if real_git {
            git(&f.root, &["init", "-q", "-b", "main"]);
            git(&f.root, &["add", "."]);
            git(&f.root, &["commit", "-qm", "initial"]);
            let ctx = ExecContext::new(
                [
                    ("PATH".into(), "/usr/bin:/bin".into()),
                    ("HOME".into(), f._tmp.path().display().to_string()),
                ]
                .into(),
            );
            source::capture_source(
                &ctx,
                &f.root,
                &CaptureOptions {
                    store: Some(f.store.root().into()),
                    ..Default::default()
                },
            )
            .unwrap()
            .manifest
        } else {
            f.capture()
        };
        let p = build(&f.store, &m, &options()).unwrap();
        let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
        let mut a = ws(&f, "attempt-a");
        let mut b = ws(&f, "attempt-b");
        a.root = f._tmp.path().join("siblings/a");
        b.root = f._tmp.path().join("siblings/b");
        a.tools = f._tmp.path().join("shared-tools");
        b.tools = a.tools.clone();
        let (a, b) = if real_git {
            let ctx = git_context(&f);
            let imported = imported_workspace(&f, &m, "matrix-repo", &ctx);
            a.repo_root = imported.repo_root.clone();
            b.repo_root = imported.repo_root;
            (
                input.materialize_git_workspace(&ctx, a).unwrap(),
                input.materialize_git_workspace(&ctx, b).unwrap(),
            )
        } else {
            (
                input.materialize_workspace(a).unwrap(),
                input.materialize_workspace(b).unwrap(),
            )
        };
        assert_ne!(a.workspace().tools, b.workspace().tools);
        let recorder = Arc::new(NamespaceRecorder::default());
        let provider = Forks {
            store: f.store.clone(),
            manifest: m.clone(),
            removed: Mutex::new(vec![]),
        };
        std::thread::scope(|scope| {
            let mut handles = vec![];
            for workspace in [&a, &b, &a] {
                let recorder = recorder.clone();
                let input = &input;
                let p = &p;
                let provider = &provider;
                let f = &f;
                handles.push(scope.spawn(move || {
                    let mut options = opts(f, Arc::new(Recorder::default()));
                    options.executor = Some(recorder);
                    execute_pinned_task(
                        &f.ctx.fresh_attempt(),
                        input,
                        &p.tasks[0].id,
                        &Default::default(),
                        None,
                        workspace,
                        options,
                        provider,
                    )
                    .unwrap()
                }));
            }
            for handle in handles {
                assert_eq!(handle.join().unwrap().jobs.len(), 2);
            }
        });
        let seen = recorder.0.lock().unwrap();
        assert_eq!(seen.len(), 6);
        for index in 0..3 {
            let values: std::collections::HashSet<_> = seen
                .iter()
                .map(|(cwd, script, project)| match index {
                    0 => cwd.display().to_string(),
                    1 => script.display().to_string(),
                    _ => project.clone(),
                })
                .collect();
            assert_eq!(
                values.len(),
                6,
                "namespace collision (kind {index}): {seen:?}"
            );
        }
        assert!(seen.iter().all(|(_, _, project)| project
            .bytes()
            .all(|byte| byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || b"-_".contains(&byte))));
        let removed = provider.removed.lock().unwrap();
        assert_eq!(removed.len(), 6);
        assert!(removed.iter().all(|path| !path.exists()));
        assert!(a.workspace().root.is_dir() && b.workspace().root.is_dir());
    }
}

#[test]
fn actual_materialization_still_verifies_every_used_object() {
    let f = Fixture::new(WORKFLOW);
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let entry = m
        .object_manifest
        .entries
        .iter()
        .find(|entry| entry.path.to_path() == Path::new("payload"))
        .unwrap();
    std::fs::write(f.store.object_path(&entry.hash), "modified").unwrap();
    assert!(input.materialize_workspace(ws(&f, "attempt")).is_err());
}

fn git_context(f: &Fixture) -> ExecContext {
    ExecContext::new(
        [
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), f._tmp.path().display().to_string()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ]
        .into(),
    )
}
fn imported_workspace(f: &Fixture, m: &SourceManifest, name: &str, ctx: &ExecContext) -> Workspace {
    let workspace = ws(f, name);
    std::fs::create_dir_all(&workspace.repo_root).unwrap();
    git(&workspace.repo_root, &["init", "-q"]);
    let bundle = source::export_with_basis(ctx, &f.store, m, &[]).unwrap();
    source::import_objects_for(ctx, &workspace.repo_root, &bundle, m, &[]).unwrap();
    workspace
}

#[test]
fn host_verified_git_workspace_preserves_original_head_staged_index_and_dirty_payload() {
    let f = Fixture::new(&WORKFLOW.replace(
        "echo captured",
        "git rev-parse HEAD; git status --porcelain",
    ));
    let ctx = git_context(&f);
    std::fs::write(f.root.join("payload"), "base").unwrap();
    git(&f.root, &["init", "-q", "-b", "main"]);
    git(&f.root, &["add", "."]);
    git(&f.root, &["commit", "-qm", "initial"]);
    let original = git(&f.root, &["rev-parse", "HEAD"]);
    std::fs::write(f.root.join("payload"), "staged").unwrap();
    git(&f.root, &["add", "payload"]);
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let expected_status = git(&f.root, &["status", "--porcelain"]);
    let m = source::capture_source(
        &ctx,
        &f.root,
        &CaptureOptions {
            store: Some(f.store.root().into()),
            ..Default::default()
        },
    )
    .unwrap()
    .manifest;
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = imported_workspace(&f, &m, "owned", &ctx);
    source::add_worktree(&ctx, &f.store, &workspace.repo_root, &workspace.root, &m).unwrap();
    // RTR owns this existing lifecycle: restore original HEAD and captured index.
    git(&workspace.root, &["reset", "--soft", &original]);
    git(
        &workspace.root,
        &["read-tree", m.index_tree.as_deref().unwrap()],
    );
    let root = workspace.root.clone();
    let index_before = git(&root, &["write-tree"]);
    let verified = input.verify_workspace(&ctx, workspace.clone()).unwrap();
    assert_eq!(git(&root, &["rev-parse", "HEAD"]), original);
    assert_eq!(git(&root, &["status", "--porcelain"]), expected_status);
    assert_eq!(git(&root, &["write-tree"]), index_before);
    let mut options = opts(&f, Arc::new(Recorder::default()));
    options.executor = None;
    let result = execute_pinned_task(
        &ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        None,
        &verified,
        options,
        &RefusePinnedForks,
    )
    .unwrap();
    assert!(matches!(
        result.jobs[0].outcome,
        local_ci::exec::Outcome::Pass
    ));
    let output = std::fs::read_to_string(result.jobs[0].steps[0].log.as_ref().unwrap()).unwrap();
    assert!(output.contains(&original), "{output}");
    assert!(output.contains("MM payload"), "{output}");
    git(&root, &["read-tree", "HEAD"]);
    assert!(input.verify_workspace(&ctx, workspace).is_err());
    assert_eq!(git(&f.root, &["rev-parse", "HEAD"]), original);
    assert_eq!(git(&f.root, &["status", "--porcelain"]), expected_status);
}

fn private_state(f: &Fixture) -> (std::fs::File, PathBuf) {
    use std::os::unix::fs::DirBuilderExt;
    let root = f._tmp.path().join("worker-state");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("attempts"))
        .unwrap();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("attempts/a"))
        .unwrap();
    (std::fs::File::open(&root).unwrap(), root)
}

#[test]
fn private_worker_receipt_restores_setup_marks_and_rejects_identity_or_path_retargeting() {
    use local_ci::source_execute::{AttemptBinding, PrivateReceiptDirectory};
    let f = Fixture::new(&WORKFLOW.replace("echo captured", "npm ci\n      - run: echo consumer"));
    std::fs::write(f.root.join("payload"), "captured").unwrap();
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    let (state, _) = private_state(&f);
    let directory = PrivateReceiptDirectory::open(&state, Path::new("attempts/a")).unwrap();
    let binding = AttemptBinding {
        attempt: "a".into(),
        generation: 3,
    };
    workspace
        .write_receipt(&input, &directory, &binding)
        .unwrap();
    workspace
        .write_receipt(&input, &directory, &binding)
        .unwrap();
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
    let marks = workspace
        .workspace()
        .tools
        .join("installed")
        .join(workspace.workspace().root.file_name().unwrap());
    std::fs::create_dir_all(&marks).unwrap();
    std::fs::write(marks.join("npm-verified-setup"), "preserved").unwrap();
    std::fs::write(workspace.workspace().root.join("setup-created"), "allowed").unwrap();
    std::fs::rename(
        f.store.root().join("objects"),
        f.store.root().join("unavailable-objects"),
    )
    .unwrap();
    let restored = input
        .restore_workspace(workspace.workspace().clone(), &directory, &binding)
        .unwrap();
    assert_eq!(restored.workspace().tools, workspace.workspace().tools);
    assert_eq!(
        std::fs::read_to_string(marks.join("npm-verified-setup")).unwrap(),
        "preserved"
    );
    let result = execute_pinned_task(
        &f.ctx.fresh_attempt(),
        &input,
        &p.tasks[0].id,
        &Default::default(),
        Some((&setup.receipt, &context)),
        &restored,
        opts(&f, recorder.clone()),
        &RefusePinnedForks,
    )
    .unwrap();
    assert!(result.receipt_ignored.is_none());
    let calls = recorder.0.lock().unwrap();
    assert!(calls.iter().all(|(_, env)| env
        .iter()
        .any(|(name, value)| name == "LOCAL_CI_MARKS" && Path::new(value) == marks)));
    drop(calls);
    assert!(input
        .restore_workspace(
            workspace.workspace().clone(),
            &directory,
            &AttemptBinding {
                attempt: "a".into(),
                generation: 4
            }
        )
        .is_err());
    let mut retargeted = workspace.workspace().clone();
    retargeted.tools = f._tmp.path().join("other-tools");
    assert!(input
        .restore_workspace(retargeted, &directory, &binding)
        .is_err());
    let root = &workspace.workspace().root;
    std::fs::rename(root, root.with_file_name("original-root")).unwrap();
    std::fs::create_dir(root).unwrap();
    assert!(input
        .restore_workspace(workspace.workspace().clone(), &directory, &binding)
        .is_err());
}

#[test]
fn worker_receipt_rejects_symlink_public_ancestor_and_linked_receipt() {
    use local_ci::source_execute::{AttemptBinding, PrivateReceiptDirectory};
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new(WORKFLOW);
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = input.materialize_workspace(ws(&f, "attempt")).unwrap();
    let (state, root) = private_state(&f);
    assert!(PrivateReceiptDirectory::open(&state, Path::new("../outside")).is_err());
    std::os::unix::fs::symlink("a", root.join("attempts/link")).unwrap();
    assert!(PrivateReceiptDirectory::open(&state, Path::new("attempts/link")).is_err());
    std::fs::set_permissions(
        root.join("attempts/a"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    assert!(PrivateReceiptDirectory::open(&state, Path::new("attempts/a")).is_err());
    std::fs::set_permissions(
        root.join("attempts/a"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let directory = PrivateReceiptDirectory::open(&state, Path::new("attempts/a")).unwrap();
    let binding = AttemptBinding {
        attempt: "a".into(),
        generation: 1,
    };
    workspace
        .write_receipt(&input, &directory, &binding)
        .unwrap();
    let receipt = root.join("attempts/a/.local-ci-captured/workspace.json");
    std::fs::hard_link(&receipt, root.join("linked")).unwrap();
    assert!(input
        .restore_workspace(workspace.workspace().clone(), &directory, &binding)
        .is_err());
    std::fs::remove_file(root.join("linked")).unwrap();
    std::fs::set_permissions(&receipt, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(input
        .restore_workspace(workspace.workspace().clone(), &directory, &binding)
        .is_err());
}

#[test]
fn verified_host_provider_runs_fixed_dirty_submodule_payload_and_rejects_nested_corruption() {
    let f = Fixture::new(&WORKFLOW.replace("echo captured", "cat deps/sub/payload"));
    let ctx = git_context(&f);
    let sub = f._tmp.path().join("sub-source");
    std::fs::create_dir(&sub).unwrap();
    std::fs::write(sub.join("payload"), "sub-base").unwrap();
    git(&sub, &["init", "-q", "-b", "main"]);
    git(&sub, &["add", "."]);
    git(&sub, &["commit", "-qm", "submodule initial"]);
    let revision = git(&sub, &["rev-parse", "HEAD"]);
    git(&f.root, &["init", "-q", "-b", "main"]);
    git(
        &f.root,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            sub.to_str().unwrap(),
            "deps/sub",
        ],
    );
    git(&f.root, &["add", "."]);
    git(&f.root, &["commit", "-qm", "parent initial"]);
    std::fs::write(f.root.join("deps/sub/payload"), "sub-dirty").unwrap();
    let m = source::capture_source(
        &ctx,
        &f.root,
        &CaptureOptions {
            store: Some(f.store.root().into()),
            ..Default::default()
        },
    )
    .unwrap()
    .manifest;
    assert_eq!(m.submodules.len(), 1);
    assert_eq!(m.submodules[0].revision, revision);
    let nested = f
        .store
        .load_manifest(m.submodules[0].source_id.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    let workspace = ws(&f, "owned");
    std::fs::create_dir_all(&workspace.repo_root).unwrap();
    git(&workspace.repo_root, &["init", "-q"]);
    let bundle = source::export_with_basis(&ctx, &f.store, &m, &[]).unwrap();
    source::import_objects_for(&ctx, &workspace.repo_root, &bundle, &m, &[nested]).unwrap();
    git(
        &workspace.repo_root,
        &[
            "worktree",
            "add",
            "-q",
            "--no-checkout",
            "--detach",
            workspace.root.to_str().unwrap(),
            m.execution_commit.as_deref().unwrap(),
        ],
    );
    source::materialize_from(&f.store, &m, &workspace.root).unwrap();
    git(
        &workspace.root,
        &["update-ref", "HEAD", m.original_head.as_deref().unwrap()],
    );
    git(
        &workspace.root,
        &["read-tree", m.index_tree.as_deref().unwrap()],
    );
    let verified = input.verify_workspace(&ctx, workspace.clone()).unwrap();
    let mut options = opts(&f, Arc::new(Recorder::default()));
    options.executor = None;
    let result = execute_pinned_task(
        &ctx,
        &input,
        &p.tasks[0].id,
        &Default::default(),
        None,
        &verified,
        options,
        &RefusePinnedForks,
    )
    .unwrap();
    assert!(matches!(
        result.jobs[0].outcome,
        local_ci::exec::Outcome::Pass
    ));
    let output = std::fs::read_to_string(result.jobs[0].steps[0].log.as_ref().unwrap()).unwrap();
    assert!(output.contains("sub-dirty"), "{output}");
    assert!(!workspace.root.join("deps/sub/.git").exists());
    std::fs::write(workspace.root.join("deps/sub/payload"), "corrupt").unwrap();
    assert!(input.verify_workspace(&ctx, workspace).is_err());
}

#[test]
fn plain_directory_provider_accepts_a_git_ancestor_but_rejects_its_own_git_metadata() {
    let f = Fixture::new(WORKFLOW);
    let m = f.capture();
    let p = build(&f.store, &m, &options()).unwrap();
    let input = PinnedExecution::new(&p, &m, &f.store).unwrap();
    git(f._tmp.path(), &["init", "-q"]);
    let workspace = ws(&f, "provided");
    source::materialize_from(&f.store, &m, &workspace.root).unwrap();
    input
        .verify_workspace(&git_context(&f), workspace.clone())
        .unwrap();
    std::fs::write(workspace.root.join(".git"), "gitdir: elsewhere").unwrap();
    assert!(input.verify_workspace(&git_context(&f), workspace).is_err());
}
