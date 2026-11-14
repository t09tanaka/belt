//! Execute an individual frozen task from immutable captured input.
use crate::{
    context::ExecContext,
    exec::{Opts, Runner, Workspace},
    frozen::FrozenPlan,
    setup::{PreparedReceipt, PreparedSetup, SetupRecipe, TemplateContext, TemplateKey},
    source::{ObjectStore, SourceManifest},
    task::{TaskInputs, TaskResult},
};
use anyhow::{bail, Result};
use std::path::Path;

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
}
impl PinnedWorkspace {
    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }
    fn validate(&self, input: &PinnedExecution<'_>) -> Result<()> {
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
        input.validate(plan)?;
        Ok(input)
    }
    /// Materialize into a fresh host-owned root. No secret files are restored.
    /// Root ownership and lifecycle remain with the host; setup mutations are
    /// allowed, but replacing the root directory invalidates this capability.
    pub fn materialize_workspace(&self, workspace: Workspace) -> Result<PinnedWorkspace> {
        use std::os::unix::fs::MetadataExt;
        self.validate(self.plan)?;
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
        let root = std::fs::canonicalize(&workspace.root)?;
        let metadata = std::fs::metadata(&root)?;
        Ok(PinnedWorkspace {
            workspace,
            plan_digest: self.plan.digest.clone(),
            root,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(crate) fn validate(&self, plan: &FrozenPlan) -> Result<()> {
        plan.verify()?;
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
            || !m.submodules.is_empty()
            || m.secret_files
                .iter()
                .any(|s| m.object_manifest.entries.iter().any(|e| e.path == s.path))
        {
            bail!("captured execution identity mismatch");
        }
        // This verified view is deliberately secret-free and never consults Git.
        let view = tempfile::tempdir()?;
        crate::source::materialize_from(self.store, m, view.path())?;
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
    let runner = Runner::new_pinned(ctx, input, ws.workspace(), opts, workspaces)?;
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
    let runner = Runner::new_pinned(ctx, input, ws.workspace(), opts, workspaces)?;
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
        };
        let provider = RefusePinnedForks;
        let mut runner =
            Runner::new_pinned(&ctx, &input, workspace.workspace(), opts, &provider).unwrap();
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
}
