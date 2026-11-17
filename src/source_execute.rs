//! Execute an individual frozen task from immutable captured input.
use crate::{
    context::ExecContext,
    exec::{Opts, Runner, Workspace},
    frozen::FrozenPlan,
    setup::{PreparedReceipt, PreparedSetup, SetupRecipe, TemplateContext, TemplateKey},
    source::{ObjectStore, SourceManifest},
    task::{TaskInputs, TaskResult},
};
use anyhow::{bail, Context, Result};
mod evaluation;
pub(crate) use evaluation::GitWorkflowReader;
pub use evaluation::{
    git_evaluation_source, verify_git_evaluation_source, EvaluationBinding, EvaluationSource,
    JobScheduling, VerifiedEvaluationSource, WorkflowScheduling,
};
mod receipt;
pub use receipt::{AttemptBinding, PrivateReceiptDirectory};
use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
static WORKSPACE_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

fn new_namespace() -> Result<String> {
    let nonce = tempfile::Builder::new().prefix("pinned-").tempdir()?;
    Ok(format!(
        "{}-{}-{}",
        nonce
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_ascii_lowercase(),
        std::process::id(),
        WORKSPACE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Host-owned independent matrix workspaces. Implementations restore secrets
/// separately, preserve validated setup contents, and remove only owned children.
/// They must never clone a live caller checkout or share mutable installs.
pub trait PinnedWorkspaceProvider: Sync {
    fn fork(&self, parent: &Path, child: &Path) -> Result<()>;
    fn remove(&self, child: &Path) -> Result<()>;
}

/// An owned source materialization. Its root cannot be retargeted through this API.
/// Hosts may restore secrets only after materialization, before executing setup.
pub struct PinnedWorkspace {
    workspace: Workspace,
    plan_digest: String,
    root: std::path::PathBuf,
    device: u64,
    inode: u64,
    pub(crate) namespace: String,
    pub(crate) sequence: Arc<AtomicUsize>,
}
impl PinnedWorkspace {
    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }
    pub(crate) fn validate(&self, input: &PinnedExecution<'_>) -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let root = std::fs::canonicalize(&self.workspace.root)?;
        let metadata = std::fs::metadata(&root)?;
        if self.plan_digest != input.plan.digest
            || root != self.root
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
        {
            bail!("captured workspace binding mismatch");
        }
        Ok(())
    }
}

/// Capability binding a frozen plan to its verified captured source objects.
pub struct PinnedExecution<'a> {
    pub(crate) plan: &'a FrozenPlan,
    manifest: &'a SourceManifest,
    store: &'a ObjectStore,
}
impl<'a> PinnedExecution<'a> {
    pub fn new(
        plan: &'a FrozenPlan,
        manifest: &'a SourceManifest,
        store: &'a ObjectStore,
    ) -> Result<Self> {
        let input = Self {
            plan,
            manifest,
            store,
        };
        input.validate_binding(plan)?;
        crate::source::check_manifest_digest(manifest)?;
        crate::source::check_materialization_graph(store, manifest)?;
        Ok(input)
    }
    /// Materialize into a fresh host-owned root. No secret files are restored.
    /// Root ownership and lifecycle remain with the host; setup mutations are
    /// allowed, but replacing the root directory invalidates this capability.
    pub fn materialize_workspace(&self, workspace: Workspace) -> Result<PinnedWorkspace> {
        self.validate(self.plan)?;
        if !matches!(
            self.manifest.source_kind,
            crate::source::SourceKind::Directory
        ) {
            bail!("Git source requires source-aware Git materialization");
        }
        if workspace.root.exists() {
            bail!("captured workspace root must be fresh")
        }
        use std::os::unix::fs::DirBuilderExt;
        if let Some(parent) = workspace.root.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&workspace.root)?;
        crate::source::materialize_from(self.store, self.manifest, &workspace.root)?;
        self.bind_verified_workspace(workspace)
    }

    /// Bind a workspace provided by the trusted host lifecycle, before secrets
    /// or setup. HEAD/index semantics come from the source kind, not a caller
    /// assertion: worktree captures preserve original HEAD and captured index.
    pub fn verify_workspace(
        &self,
        ctx: &ExecContext,
        workspace: Workspace,
    ) -> Result<PinnedWorkspace> {
        self.validate(self.plan)?;
        crate::source::verify_materialized_from(self.store, self.manifest, &workspace.root)?;
        self.verify_git(ctx, &workspace.root, false)?;
        self.bind_verified_workspace(workspace)
    }

    /// SDK-owned legacy Git materialization: HEAD is the execution commit and
    /// the index is its tree, matching ordinary local-ci worktree execution.
    /// `workspace.repo_root` must already contain the imported captured commit.
    pub fn materialize_git_workspace(
        &self,
        ctx: &ExecContext,
        workspace: Workspace,
    ) -> Result<PinnedWorkspace> {
        self.validate(self.plan)?;
        if matches!(
            self.manifest.source_kind,
            crate::source::SourceKind::Directory
        ) {
            bail!("directory source has no Git materialization");
        }
        if std::fs::symlink_metadata(&workspace.root).is_ok() {
            bail!("captured workspace root must be fresh");
        }
        crate::source::add_worktree(
            ctx,
            self.store,
            &workspace.repo_root,
            &workspace.root,
            self.manifest,
        )?;
        crate::source::verify_materialized_from(self.store, self.manifest, &workspace.root)?;
        self.verify_git(ctx, &workspace.root, true)?;
        self.bind_verified_workspace(workspace)
    }

    fn verify_git(&self, ctx: &ExecContext, root: &Path, execution_snapshot: bool) -> Result<()> {
        use crate::source::SourceKind;
        let m = self.manifest;
        if matches!(m.source_kind, SourceKind::Directory) {
            if std::fs::symlink_metadata(root.join(".git")).is_ok() {
                bail!("directory input cannot claim a Git checkout");
            }
            return Ok(());
        }
        let oid = |value: Option<&str>| -> Result<String> {
            let value = value.context("captured Git source lacks required HEAD/index identity")?;
            if value.len() != 40 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                bail!("invalid captured Git object identity");
            }
            Ok(value.to_string())
        };
        let head = if execution_snapshot || matches!(m.source_kind, SourceKind::Commit) {
            oid(m.execution_commit.as_deref())?
        } else {
            oid(m.original_head.as_deref())?
        };
        let tree = if execution_snapshot || matches!(m.source_kind, SourceKind::Commit) {
            format!("{head}^{{tree}}")
        } else {
            oid(m.index_tree.as_deref())?
        };
        if std::fs::canonicalize(crate::git::toplevel(ctx, root)?)? != std::fs::canonicalize(root)?
            || crate::git::git(ctx, root, &["rev-parse", "--verify", "HEAD"])? != head
        {
            bail!("captured workspace Git HEAD/root mismatch");
        }
        crate::git::git(
            ctx,
            root,
            &["diff-index", "--cached", "--quiet", &tree, "--"],
        )?;
        Ok(())
    }

    fn bind_verified_workspace(&self, mut workspace: Workspace) -> Result<PinnedWorkspace> {
        use std::os::unix::fs::MetadataExt;
        let root = std::fs::canonicalize(&workspace.root)?;
        let metadata = std::fs::metadata(&root)?;
        if !metadata.is_dir() {
            bail!("captured workspace root is not a directory");
        }
        if [
            &workspace.root,
            &workspace.repo_root,
            &workspace.run_dir,
            &workspace.tools,
            &workspace.lock_file,
        ]
        .iter()
        .any(|path| !path.is_absolute())
        {
            bail!("captured workspace paths must be absolute");
        }
        let namespace = new_namespace()?;
        workspace.tools = workspace.tools.join(&namespace);
        Ok(PinnedWorkspace {
            namespace,
            sequence: Arc::new(AtomicUsize::new(0)),
            workspace,
            plan_digest: self.plan.digest.clone(),
            root,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(crate) fn validate(&self, plan: &FrozenPlan) -> Result<()> {
        plan.verify()?;
        if plan.digest != self.plan.digest {
            bail!("captured execution plan mismatch");
        }
        Ok(())
    }

    fn validate_binding(&self, plan: &FrozenPlan) -> Result<()> {
        self.validate(plan)?;
        let Some(p) = &plan.source.pinned else {
            bail!("captured execution requires a pinned plan")
        };
        let m = self.manifest;
        if plan.digest != self.plan.digest
            || plan.source.source_id != m.source_id
            || p.manifest_digest != m.manifest_digest
            || p.kind != m.source_kind
            || p.repository_id != m.repository_id
            || p.cwd_relative != m.cwd_relative
            || plan.source.execution_commit != m.execution_commit
            || plan.source.base != m.base
            || plan.branch != m.branch
            || m.secret_files
                .iter()
                .any(|s| m.object_manifest.entries.iter().any(|e| e.path == s.path))
        {
            bail!("captured execution identity mismatch");
        }
        // Blob/tree verification belongs to actual materialization, once per root.
        Ok(())
    }
}

/// Refuse nested workspaces unless a host supplies an explicit lifecycle.
/// This also prevents accidental secret-bearing recursive copies.
pub struct RefusePinnedForks;
impl PinnedWorkspaceProvider for RefusePinnedForks {
    fn fork(&self, _: &Path, _: &Path) -> Result<()> {
        bail!("captured matrix fork requires a host workspace and secret provider")
    }
    fn remove(&self, _: &Path) -> Result<()> {
        Ok(())
    }
}

pub fn execute_pinned_setup(
    ctx: &ExecContext,
    input: &PinnedExecution<'_>,
    recipe: &SetupRecipe,
    key: &TemplateKey,
    ws: &PinnedWorkspace,
    opts: Opts,
    workspaces: &dyn PinnedWorkspaceProvider,
) -> Result<PreparedSetup> {
    input.validate(input.plan)?;
    ws.validate(input)?;
    crate::wire::check_version("setup recipe", recipe.schema_version)?;
    if crate::setup::derive_setup_recipe(input.plan, &recipe.task)? != *recipe {
        bail!("setup recipe does not belong to the frozen plan");
    }
    let runner = Runner::new_pinned(ctx, input, ws, opts, workspaces)?;
    let result = (|| {
        runner.prepare_tools()?;
        runner.run_recipe(recipe, key.digest())
    })();
    ctx.cancel.remove_containers(ctx);
    result
}

#[allow(clippy::too_many_arguments)]
pub fn execute_pinned_task(
    ctx: &ExecContext,
    input: &PinnedExecution<'_>,
    task_id: &str,
    inputs: &TaskInputs,
    prepared: Option<(&PreparedReceipt, &TemplateContext)>,
    ws: &PinnedWorkspace,
    opts: Opts,
    workspaces: &dyn PinnedWorkspaceProvider,
) -> Result<TaskResult> {
    input.validate(input.plan)?;
    ws.validate(input)?;
    let runner = Runner::new_pinned(ctx, input, ws, opts, workspaces)?;
    crate::task::execute_with_runner(
        ctx,
        input.plan,
        task_id,
        inputs,
        prepared,
        ws.workspace(),
        runner,
    )
}

/// Execute only a recomputed member of the original captured dynamic task.
/// Host must bind dispatch/inputs/facts to its committed readiness/attempt.
#[allow(clippy::too_many_arguments)]
pub fn execute_pinned_dispatch(
    ctx: &ExecContext,
    input: &PinnedExecution<'_>,
    dispatch: &crate::driver::DerivedDispatch,
    inputs: &TaskInputs,
    facts: &crate::driver::PortableFacts,
    prepared: Option<(&PreparedReceipt, &TemplateContext)>,
    ws: &PinnedWorkspace,
    opts: Opts,
    workspaces: &dyn PinnedWorkspaceProvider,
) -> Result<TaskResult> {
    input.validate(input.plan)?;
    ws.validate(input)?;
    let source = input.evaluation_source()?;
    let selection =
        crate::driver::dispatch::validate(input.plan, &source, facts, dispatch, inputs)?;
    let task = selection.task.clone();
    let runner = Runner::new_pinned(ctx, input, ws, opts, workspaces)?
        .select_derived(selection, facts.clone());
    crate::task::execute_resolved_with_runner(ctx, &task, inputs, prepared, ws.workspace(), runner)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_proof_rejects_public_runner_plan_retargeting_at_every_entry() {
        let area = tempfile::tempdir().unwrap();
        let source = area.path().join("source");
        std::fs::create_dir_all(source.join(".github/workflows")).unwrap();
        std::fs::write(
            source.join(".github/workflows/ci.yml"),
            "on: push\njobs:\n  test:\n    runs-on: x\n    steps:\n      - run: npm ci\n",
        )
        .unwrap();
        let store = ObjectStore::open(area.path().join("store")).unwrap();
        let ctx = ExecContext::new(Default::default());
        let manifest = crate::source::capture_directory(
            &ctx,
            &source,
            &crate::source::CaptureOptions {
                store: Some(store.root().into()),
                ..Default::default()
            },
        )
        .unwrap()
        .manifest;
        let plan = crate::build_frozen_plan_from_source(
            &store,
            &manifest,
            &crate::PinnedPlanOptions {
                filters: crate::frozen::Filters {
                    all: false,
                    include_disabled: true,
                    workflows_dirs: vec![],
                    workflows: vec![],
                    jobs: vec![],
                },
                workflow_dirs_override: None,
                disabled: vec![],
                base_branch: None,
                changed_files: None,
            },
        )
        .unwrap();
        let input = PinnedExecution::new(&plan, &manifest, &store).unwrap();
        let workspace = input
            .materialize_workspace(Workspace {
                root: area.path().join("root"),
                repo_root: area.path().join("no-repo"),
                run_dir: area.path().join("run"),
                tools: area.path().join("tools"),
                lock_file: area.path().join("lock"),
            })
            .unwrap();
        let mut legacy = plan.clone();
        legacy.schema_version = 6;
        legacy.source.pinned = None;
        legacy.source.execution_commit = Some("a".repeat(40));
        legacy.source.base = Some("main".into());
        let opts = Opts {
            parallel: 1,
            traffic_controller: None,
            executor: None,
            portool: None,
            containers: 1,
            job_timeout_min: 1.,
            keep: false,
            trash: area.path().join("trash"),
            all_os_legs: false,
        };
        let provider = RefusePinnedForks;
        let mut runner = Runner::new_pinned(&ctx, &input, &workspace, opts, &provider).unwrap();
        runner.fp = &legacy;
        let recipe = crate::setup::derive_setup_recipe(&plan, &plan.tasks[0].id).unwrap();
        assert!(runner.prepare_tools().is_err());
        assert!(runner.run_recipe(&recipe, "key").is_err());
        assert!(runner.run_units().is_err());
        assert!(runner.release_ports(&workspace.workspace().root).is_err());
        assert!(runner
            .run_task(
                &workspace.workspace().root,
                &plan.tasks[0],
                &Default::default(),
                None,
                true,
                false,
                false
            )
            .is_err());
        assert!(!workspace.workspace().tools.exists());
    }
    #[test]
    fn replaced_workspace_at_final_cleanup_retains_actual_result_and_port_reference() {
        struct ReplaceRoot;
        impl crate::StepExecutor for ReplaceRoot {
            fn run(&self, req: &crate::StepRequest) -> Result<crate::StepExit> {
                std::fs::rename(&req.cwd, req.cwd.with_extension("original"))?;
                std::fs::create_dir(&req.cwd)?;
                Ok(crate::StepExit {
                    exit: crate::exec::Exit::Code(0),
                    run_secs: 0.,
                    queue_wait_secs: None,
                })
            }
        }
        let area = tempfile::tempdir().unwrap();
        let source = area.path().join("source");
        std::fs::create_dir_all(source.join(".github/workflows")).unwrap();
        std::fs::write(
            source.join(".github/workflows/ci.yml"),
            "on: push\njobs:\n  test:\n    runs-on: x\n    steps:\n      - run: echo completed\n",
        )
        .unwrap();
        let store = ObjectStore::open(area.path().join("store")).unwrap();
        let ctx = ExecContext::new(Default::default());
        let manifest = crate::source::capture_directory(
            &ctx,
            &source,
            &crate::source::CaptureOptions {
                store: Some(store.root().into()),
                ..Default::default()
            },
        )
        .unwrap()
        .manifest;
        let plan = crate::build_frozen_plan_from_source(
            &store,
            &manifest,
            &crate::PinnedPlanOptions {
                filters: crate::frozen::Filters {
                    all: false,
                    include_disabled: true,
                    workflows_dirs: vec![],
                    workflows: vec![],
                    jobs: vec![],
                },
                workflow_dirs_override: None,
                disabled: vec![],
                base_branch: None,
                changed_files: None,
            },
        )
        .unwrap();
        let input = PinnedExecution::new(&plan, &manifest, &store).unwrap();
        let workspace = input
            .materialize_workspace(Workspace {
                root: area.path().join("root"),
                repo_root: area.path().join("no-repo"),
                run_dir: area.path().join("run"),
                tools: area.path().join("tools"),
                lock_file: area.path().join("lock"),
            })
            .unwrap();
        let opts = Opts {
            parallel: 1,
            traffic_controller: None,
            executor: Some(Arc::new(ReplaceRoot)),
            portool: Some(area.path().join("never-invoked-portool")),
            containers: 1,
            job_timeout_min: 1.,
            keep: false,
            trash: area.path().join("trash"),
            all_os_legs: false,
        };
        let provider = RefusePinnedForks;
        let runner = Runner::new_pinned(&ctx, &input, &workspace, opts, &provider).unwrap();
        let outcome = crate::task::execute_resolved_outcome(
            &ctx,
            &plan.tasks[0],
            &TaskInputs::default(),
            None,
            workspace.workspace(),
            runner,
        );
        assert_eq!(
            outcome.execution.as_ref().unwrap().outcomes[0].status,
            crate::JobStatus::Success
        );
        assert!(outcome.final_error.is_some());
        assert_eq!(
            outcome.cleanup.pending.port_paths,
            vec![workspace.workspace().root.clone()]
        );
        assert!(
            crate::execution_outcome::retained_result(&outcome.legacy_result().unwrap_err())
                .is_some()
        );
    }
}
