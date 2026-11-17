//! Safe structural projection of the core plan. No workflow values, step
//! labels, expression text, artifact names or recipe scripts are copied.

use local_ci::dag::{GroupedReason, NodeKind, NotSplit};
use local_ci::frozen::DisabledSource;
use local_ci::FrozenPlan;
use serde::Serialize;

use super::PlanError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanSummary {
    pub schema_version: u32,
    pub core_schema_version: u32,
    pub digest: String,
    pub remote_disabled_status_checked: bool,
    pub selected_workflows: usize,
    pub skipped_workflows: usize,
    pub unrunnable_workflows: usize,
    pub nodes: Vec<NodeSummary>,
    pub tasks: Vec<TaskSummary>,
    pub shard_groups: Vec<ShardSummary>,
    /// Largest dependency layer, ignoring conditions and host capabilities.
    /// This is a structural layer width, not a promised execution concurrency.
    pub dependency_layer_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeShape {
    Job,
    DynamicMatrix,
    MatrixVariant,
    MatrixGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeSummary {
    pub id: String,
    pub workflow: String,
    pub job: String,
    pub shape: NodeShape,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Grouping {
    SharedOutputs,
    OrderAroundSharedOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskSummary {
    pub id: String,
    /// Artifacts and recipes remain scoped to this workflow and task. Equal
    /// artifact names or recipe hashes never imply a cross-workflow edge.
    pub workflow: String,
    pub jobs: Vec<String>,
    pub nodes: Vec<String>,
    pub depends_on: Vec<String>,
    pub dynamic_matrix: bool,
    pub conditional_jobs: usize,
    pub conditional_steps: usize,
    pub command_steps: usize,
    pub conditional_command_steps: usize,
    pub needs_outputs: usize,
    pub artifact_inputs: usize,
    pub required_artifact_outputs: usize,
    pub own_checkout: bool,
    pub shared_by_jobs: bool,
    pub services: usize,
    pub grouping: Option<Grouping>,
    pub setup_steps: usize,
    pub setup_steps_left_in_job: usize,
    pub toolchain_requirements: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShardSummary {
    pub id: String,
    pub workflow: String,
    pub source_jobs: Vec<String>,
    pub shards: Vec<String>,
    /// Native partitions have no file assignments until the runner finishes.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub assigned_test_counts: Vec<usize>,
    pub split: bool,
    /// This unpartitioned candidate has to be listed on a worker.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub discovery_candidate: bool,
    /// Core's actual reasons. Planning does not invent test discovery facts.
    pub not_split: Vec<NotSplit>,
}

pub(crate) fn validate_path(path: &str) -> Result<(), PlanError> {
    if path.is_empty()
        || path.len() > 1024
        || path.starts_with('/')
        || path.contains('\\')
        || path.bytes().any(|b| b.is_ascii_control())
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(PlanError::InvalidMetadata);
    }
    Ok(())
}

fn identifier(id: &str) -> Result<(), PlanError> {
    if id.is_empty() || id.len() > 2048 || id.bytes().any(|b| b.is_ascii_control()) {
        return Err(PlanError::InvalidMetadata);
    }
    Ok(())
}

pub(crate) fn summarize(fp: &FrozenPlan) -> Result<PlanSummary, PlanError> {
    if fp.graph.nodes.len() > 4096
        || fp.tasks.len() > 4096
        || fp.workflows.len() > 512
        || fp.shard_groups.len() > 4096
    {
        return Err(PlanError::InputLimit);
    }
    let graph = &fp.graph;
    let mut nodes = vec![];
    for n in &graph.nodes {
        validate_path(&n.workflow)?;
        identifier(&n.id)?;
        identifier(&n.job)?;
        nodes.push(NodeSummary {
            id: n.id.clone(),
            workflow: n.workflow.clone(),
            job: n.job.clone(),
            shape: match &n.kind {
                NodeKind::Job => NodeShape::Job,
                NodeKind::DynamicMatrix { .. } => NodeShape::DynamicMatrix,
                NodeKind::MatrixVariant { .. } => NodeShape::MatrixVariant,
                NodeKind::MatrixGroup { .. } => NodeShape::MatrixGroup,
            },
            depends_on: n.depends_on.clone(),
        });
    }
    let mut tasks = vec![];
    let mut depths = std::collections::BTreeMap::new();
    let mut widths = std::collections::BTreeMap::<usize, usize>::new();
    for t in &fp.tasks {
        validate_path(&t.workflow)?;
        identifier(&t.id)?;
        for id in t.jobs.iter().chain(&t.nodes).chain(&t.depends_on) {
            identifier(id)?;
        }
        // Frozen task order is dependency order. Unknown/future references
        // must fail, never be presented as independent or an empty success.
        let mut depth = 0;
        for dependency in &t.depends_on {
            depth = depth.max(depths.get(dependency).ok_or(PlanError::InvalidCorePlan)? + 1);
        }
        if depths.insert(t.id.clone(), depth).is_some() {
            return Err(PlanError::InvalidCorePlan);
        }
        *widths.entry(depth).or_default() += 1;
        let recipe = local_ci::setup::derive_setup_recipe(fp, &t.id)
            .map_err(|_| PlanError::InvalidCorePlan)?;
        let demand =
            local_ci::plan_task_demand(fp, &t.id).map_err(|_| PlanError::UnsupportedDemand)?;
        let jobs: Vec<_> = fp
            .workflows
            .iter()
            .filter(|w| w.file == t.workflow)
            .flat_map(|w| &w.jobs)
            .filter(|j| t.jobs.contains(&j.id))
            .collect();
        tasks.push(TaskSummary {
            id: t.id.clone(),
            workflow: t.workflow.clone(),
            jobs: t.jobs.clone(),
            nodes: t.nodes.clone(),
            depends_on: t.depends_on.clone(),
            dynamic_matrix: t.dynamic_matrix,
            conditional_jobs: jobs.iter().filter(|j| j.cond.is_some()).count(),
            conditional_steps: jobs
                .iter()
                .flat_map(|j| &j.steps)
                .filter(|s| s.cond.is_some())
                .count(),
            command_steps: demand.nodes.len(),
            conditional_command_steps: demand.nodes.iter().filter(|n| n.conditional).count(),
            needs_outputs: t.needs_outputs.len(),
            artifact_inputs: t.artifacts.len(),
            required_artifact_outputs: t.verification.artifacts.len(),
            own_checkout: t.workspace.own_checkout,
            shared_by_jobs: t.workspace.shared_by_jobs,
            services: t.services.len(),
            grouping: t.grouped_reason.as_ref().map(|r| match r {
                GroupedReason::SharedOutputs { .. } => Grouping::SharedOutputs,
                GroupedReason::OrderAroundSharedOutputs => Grouping::OrderAroundSharedOutputs,
            }),
            setup_steps: recipe.steps.len(),
            setup_steps_left_in_job: recipe.left_in_place.len(),
            toolchain_requirements: recipe.toolchain.len(),
        });
    }
    let candidates: std::collections::BTreeSet<_> = super::discovery::candidates(fp)
        .into_iter()
        .map(|step| (step.workflow, step.job))
        .collect();
    let mut shard_groups = vec![];
    for g in &fp.shard_groups {
        validate_path(&g.workflow)?;
        for id in std::iter::once(&g.id)
            .chain(&g.source_jobs)
            .chain(&g.shards)
        {
            identifier(id)?;
        }
        shard_groups.push(ShardSummary {
            id: g.id.clone(),
            workflow: g.workflow.clone(),
            source_jobs: g.source_jobs.clone(),
            shards: g.shards.clone(),
            assigned_test_counts: if g.split && g.assignments.iter().any(|a| !a.is_empty()) {
                g.assignments.iter().map(|tests| tests.len()).collect()
            } else {
                Vec::new()
            },
            split: g.split,
            discovery_candidate: g
                .source_jobs
                .first()
                .is_some_and(|job| candidates.contains(&(g.workflow.clone(), job.clone()))),
            not_split: g.not_split.clone(),
        });
    }
    Ok(PlanSummary {
        schema_version: 1,
        core_schema_version: fp.schema_version,
        digest: fp.digest.clone(),
        remote_disabled_status_checked: matches!(fp.disabled.source, DisabledSource::Github),
        selected_workflows: fp.workflows.len(),
        skipped_workflows: fp.skipped.len(),
        unrunnable_workflows: fp.skipped.iter().filter(|s| s.unrunnable).count(),
        nodes,
        tasks,
        shard_groups,
        dependency_layer_width: widths.values().copied().max().unwrap_or(0),
    })
}

fn reason_name<T: Serialize>(reason: &T) -> String {
    // Core's own snake_case name, never a local paraphrase of it.
    serde_json::to_string(reason)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

/// The human view of [`PlanSummary`] (spec 7): how many tasks can be placed on
/// their own, what each waits for, the widest dependency layer, and why jobs
/// stay together or a task is not split. Structural facts only, as in JSON.
pub fn render_text(s: &PlanSummary) -> String {
    let mut lines = vec![
        format!(
            "{} workflows, {} tasks, {} nodes",
            s.selected_workflows,
            s.tasks.len(),
            s.nodes.len()
        ),
        format!("plan {}", s.digest),
        format!(
            "{} tasks can be placed independently, widest dependency layer {}",
            s.tasks.len(),
            s.dependency_layer_width
        ),
    ];
    for t in &s.tasks {
        if !t.depends_on.is_empty() {
            lines.push(format!(
                "task {} waits for {}",
                t.id,
                t.depends_on.join(", ")
            ));
        }
        if let Some(grouping) = &t.grouping {
            lines.push(format!(
                "task {} keeps {} jobs on one host: {}",
                t.id,
                t.jobs.len(),
                reason_name(grouping)
            ));
        }
        if t.dynamic_matrix {
            lines.push(format!(
                "task {} expands its matrix when it runs: variants unknown",
                t.id
            ));
        }
    }
    for g in s.shard_groups.iter().filter(|g| g.split) {
        for (id, count) in g.shards.iter().zip(&g.assigned_test_counts) {
            lines.push(format!("shard {id} has {count} assigned tests"));
        }
    }
    for g in s.shard_groups.iter().filter(|g| !g.split) {
        if g.discovery_candidate {
            lines.push(format!(
                "task {} can be partitioned after worker discovery",
                g.id
            ));
            continue;
        }
        let reasons: Vec<_> = g.not_split.iter().map(reason_name).collect();
        lines.push(format!(
            "task {} is not split: [{}]",
            g.id,
            reasons.join(", ")
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHARED: &str = "on: push\njobs:\n  first:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo one > shared.txt\n  second:\n    needs: first\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo two > shared.txt\n  list:\n    runs-on: ubuntu-latest\n    outputs:\n      v: ${{ steps.l.outputs.v }}\n    steps:\n      - id: l\n        run: echo PRIVATE_SCRIPT\n  each:\n    needs: list\n    runs-on: ubuntu-latest\n    strategy:\n      matrix: ${{ fromJSON(needs.list.outputs.v) }}\n    steps:\n      - run: echo PRIVATE_SCRIPT\n";

    #[test]
    fn text_shows_placement_dependencies_width_and_why_jobs_stay_together() {
        let fp = plan(&[(".github/workflows/ci.yml", SHARED)]);
        let mut summary = summarize(&fp).unwrap();
        let w = ".github/workflows/ci.yml";
        let head = format!("1 workflows, 3 tasks, 4 nodes\nplan {}\n3 tasks can be placed independently, widest dependency layer 2\ntask {w}::first+second keeps 2 jobs on one host: shared_outputs\ntask {w}::each waits for {w}::list\ntask {w}::each expands its matrix when it runs: variants unknown\n", fp.digest);
        let list = format!("task {w}::list is not split: [no_runner_adapter]\n");
        let rest = format!("task {w}::each is not split: [no_runner_adapter]");
        assert_eq!(
            render_text(&summary),
            format!("{head}task {w}::first+second is not split: [no_runner_adapter, shared_workspace]\n{list}{rest}")
        );
        assert!(!render_text(&summary).contains("PRIVATE_SCRIPT"));
        // A group core did split has no reason to show.
        summary.shard_groups[0].split = true;
        assert_eq!(render_text(&summary), format!("{head}{list}{rest}"));
    }
    use local_ci::frozen::{DisabledRecord, Filters, FrozenConfig, PlanParts, SourceRef};

    fn plan(workflows: &[(&str, &str)]) -> FrozenPlan {
        let workflows = workflows
            .iter()
            .map(|(path, text)| local_ci::workflow::parse(path, text).unwrap())
            .collect();
        let event = local_ci::filter::Event {
            base: "main".into(),
            head: Some("topic".into()),
            changed: vec![],
            all: true,
        };
        FrozenPlan::new(PlanParts {
            repository: None,
            source: SourceRef {
                source_id: "commit:fixture".into(),
                execution_commit: Some("a".repeat(40)),
                pinned: None,
                base: Some("main".into()),
            },
            branch: Some("topic".into()),
            changed_files: vec![],
            config: FrozenConfig {
                workflow_dirs: vec![".github/workflows".into()],
                default_workflows_dir: ".github/workflows".into(),
                no_cache: vec![],
                link_node_modules: false,
                ignore_env: vec![],
            },
            filters: Filters {
                all: true,
                include_disabled: false,
                workflows_dirs: vec![],
                workflows: vec![],
                jobs: vec![],
            },
            disabled: DisabledRecord::not_checked(),
            plan: local_ci::plan::build(workflows, &event, &[]),
        })
        .unwrap()
    }

    const WORKFLOW: &str = r#"
on: push
jobs:
  prepare:
    runs-on: ubuntu-latest
    outputs:
      variants: ${{ steps.list.outputs.variants }}
    steps:
      - run: echo PRIVATE_SCRIPT
      - uses: actions/upload-artifact@v4
        with:
          name: PRIVATE_ARTIFACT
          path: output.txt
  check:
    needs: prepare
    if: ${{ needs.prepare.result == 'success' }}
    runs-on: ubuntu-latest
    strategy:
      matrix:
        version: [PRIVATE_MATRIX_A, PRIVATE_MATRIX_B]
    env:
      VALUE: PRIVATE_ENV
    steps:
      - run: npm ci
      - run: echo PRIVATE_COMMAND
      - uses: actions/download-artifact@v4
        with:
          name: PRIVATE_ARTIFACT
  dynamic:
    needs: prepare
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJSON(needs.prepare.outputs.variants) }}
    steps:
      - if: ${{ needs.prepare.outputs.variants != '' }}
        run: echo PRIVATE_DYNAMIC
"#;

    #[test]
    fn preserves_core_dag_unknown_conditions_and_workflow_boundaries_without_values() {
        let other = WORKFLOW.replace("    if: ${{ needs.prepare.result == 'success' }}\n", "");
        let fp = plan(&[
            (".github/workflows/a.yml", WORKFLOW),
            (".github/workflows/b.yml", &other),
        ]);
        let adapter = crate::ci::CiPlan::from_frozen(fp.clone()).unwrap();
        assert_eq!(adapter.frozen(), &fp);
        let summary = adapter.summary();
        assert_eq!(summary.schema_version, 1);
        assert_eq!(summary.core_schema_version, fp.schema_version);
        assert_eq!(summary.digest, fp.digest);
        assert_eq!(summary.selected_workflows, 2);
        assert_eq!(summary.skipped_workflows, 0);
        assert_eq!(summary.unrunnable_workflows, 0);
        assert!(!summary.remote_disabled_status_checked);
        assert_eq!(summary.nodes.len(), fp.graph.nodes.len());
        assert_eq!(summary.tasks.len(), fp.tasks.len());
        assert_eq!(summary.dependency_layer_width, 6);
        for (actual, core) in summary.nodes.iter().zip(&fp.graph.nodes) {
            assert_eq!(actual.id, core.id);
            assert_eq!(actual.depends_on, core.depends_on);
            assert_eq!(actual.workflow, core.workflow);
            assert_eq!(actual.job, core.job);
        }
        assert!(summary.nodes.iter().any(|n| n.shape == NodeShape::Job));
        assert!(summary
            .nodes
            .iter()
            .any(|n| n.shape == NodeShape::DynamicMatrix));
        assert!(summary
            .nodes
            .iter()
            .any(|n| n.shape == NodeShape::MatrixVariant));
        assert!(summary
            .nodes
            .iter()
            .any(|n| n.shape == NodeShape::MatrixGroup));
        for (actual, core) in summary.tasks.iter().zip(&fp.tasks) {
            let demand = local_ci::plan_task_demand(&fp, &core.id).unwrap();
            let recipe = local_ci::setup::derive_setup_recipe(&fp, &core.id).unwrap();
            assert_eq!(actual.depends_on, core.depends_on);
            assert!(actual
                .depends_on
                .iter()
                .all(|dep| fp.task(dep).unwrap().workflow == actual.workflow));
            assert_eq!(actual.command_steps, demand.nodes.len());
            assert_eq!(
                actual.conditional_command_steps,
                demand.nodes.iter().filter(|d| d.conditional).count()
            );
            let workflow = fp
                .workflows
                .iter()
                .find(|w| w.file == core.workflow)
                .unwrap();
            let jobs: Vec<_> = workflow
                .jobs
                .iter()
                .filter(|j| core.jobs.contains(&j.id))
                .collect();
            assert_eq!(
                actual.conditional_jobs,
                jobs.iter().filter(|j| j.cond.is_some()).count()
            );
            assert_eq!(
                actual.conditional_steps,
                jobs.iter()
                    .flat_map(|j| &j.steps)
                    .filter(|s| s.cond.is_some())
                    .count()
            );
            assert_eq!(actual.setup_steps, recipe.steps.len());
            assert_eq!(actual.setup_steps_left_in_job, recipe.left_in_place.len());
            assert_eq!(actual.artifact_inputs, core.artifacts.len());
            assert_eq!(
                actual.required_artifact_outputs,
                core.verification.artifacts.len()
            );
        }
        assert!(summary
            .tasks
            .iter()
            .any(|t| t.dynamic_matrix && t.conditional_command_steps > 0));
        assert!(summary.tasks.iter().any(|t| t.artifact_inputs > 0));
        assert!(summary.tasks.iter().any(|t| t.conditional_jobs > 0));
        assert!(summary.tasks.iter().any(|t| t.conditional_steps > 0));
        for (actual, core) in summary.shard_groups.iter().zip(&fp.shard_groups) {
            assert_eq!(actual.not_split, core.not_split);
            assert_eq!(actual.shards, core.shards);
            assert!(!actual.split);
        }
        let json = serde_json::to_string(summary).unwrap();
        assert!(!json.contains("PRIVATE_"), "{json}");
        assert!(!json.contains("fromJSON"));
        assert!(!json.contains("needs.prepare.result"));
        assert_eq!(serde_json::to_string(summary).unwrap(), json);
        assert_eq!(adapter.into_frozen(), fp);
    }

    #[test]
    fn empty_and_unrunnable_plans_are_explicit() {
        let fp = plan(&[]);
        let summary = summarize(&fp).unwrap();
        assert!(summary.tasks.is_empty());
        assert_eq!(summary.dependency_layer_width, 0);
        let fp = plan(&[("bad.yml", "on: push\njobs:\n  check:\n    needs: missing\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n")]);
        let summary = summarize(&fp).unwrap();
        assert_eq!(summary.unrunnable_workflows, 1);
        assert_eq!(summary.skipped_workflows, 1);
    }

    #[test]
    fn validates_metadata_limits_and_core_failures() {
        for bad in ["", "/x", "../x", "a/./b", "a//b", "x\\y", "x\ny"] {
            assert_eq!(validate_path(bad), Err(PlanError::InvalidMetadata));
        }
        assert!(validate_path(&"x".repeat(1024)).is_ok());
        assert!(validate_path(&"x".repeat(1025)).is_err());
        assert!(validate_path(".github/workflows/a.yml").is_ok());
        for bad in ["", "x\ny", &"x".repeat(2049)] {
            assert!(identifier(bad).is_err());
        }
        assert!(identifier("build#0").is_ok());
        assert!(identifier(&"x".repeat(2048)).is_ok());
        let mut fp = plan(&[("a.yml", WORKFLOW)]);
        let base = fp.clone();
        // Counts exactly at each budget pass the size guard. Invalid first
        // task metadata then stops projection without thousands of core calls.
        fp.tasks = vec![fp.tasks[0].clone(); 4096];
        fp.tasks[0].workflow = "/invalid".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.graph.nodes = vec![fp.graph.nodes[0].clone(); 4096];
        fp.graph.nodes[0].workflow = "/invalid".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.workflows = vec![fp.workflows[0].clone(); 512];
        fp.tasks[0].workflow = "/invalid".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.tasks = vec![fp.tasks[0].clone(); 4097];
        assert_eq!(summarize(&fp), Err(PlanError::InputLimit));
        fp = base.clone();
        fp.graph.nodes = vec![fp.graph.nodes[0].clone(); 4097];
        assert_eq!(summarize(&fp), Err(PlanError::InputLimit));
        fp = base.clone();
        fp.workflows = vec![fp.workflows[0].clone(); 513];
        assert_eq!(summarize(&fp), Err(PlanError::InputLimit));
        fp = base.clone();
        fp.shard_groups = vec![fp.shard_groups[0].clone(); 4096];
        fp.tasks[0].workflow = "/invalid".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.shard_groups = vec![fp.shard_groups[0].clone(); 4097];
        assert_eq!(summarize(&fp), Err(PlanError::InputLimit));
        fp = base.clone();
        fp.schema_version = 0;
        assert!(crate::ci::CiPlan::from_frozen(fp).is_err());
        fp = base.clone();
        fp.graph.nodes[0].workflow = "../x".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.graph.nodes[0].id = "".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.graph.nodes[0].job = "".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.tasks[0].workflow = "../x".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.tasks[0].workflow = "missing.yml".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidCorePlan));
        fp = base.clone();
        fp.tasks[0].id = "".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.tasks[0].jobs = vec!["".into()];
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.tasks[0].depends_on = vec!["missing".into()];
        assert_eq!(summarize(&fp), Err(PlanError::InvalidCorePlan));
        fp = base.clone();
        fp.tasks.insert(1, fp.tasks[0].clone());
        assert_eq!(summarize(&fp), Err(PlanError::InvalidCorePlan));
        fp = base.clone();
        fp.workflows[0].jobs[0].steps[0].shell = Some("unsupported".into());
        // Re-sealing a locally generated plan preserves the graph but the
        // core demand adapter must still reject an unsupported command shell.
        fp.digest = fp.compute_digest().unwrap();
        assert_eq!(summarize(&fp), Err(PlanError::UnsupportedDemand));
        fp = base.clone();
        fp.shard_groups[0].workflow = "/x".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.shard_groups[0].id = "".into();
        assert_eq!(summarize(&fp), Err(PlanError::InvalidMetadata));
        fp = base.clone();
        fp.tasks[0].grouped_reason = Some(GroupedReason::SharedOutputs {
            files: vec!["PRIVATE_PATH".into()],
        });
        assert_eq!(
            summarize(&fp).unwrap().tasks[0].grouping,
            Some(Grouping::SharedOutputs)
        );
        fp.tasks[0].grouped_reason = Some(GroupedReason::OrderAroundSharedOutputs);
        assert_eq!(
            summarize(&fp).unwrap().tasks[0].grouping,
            Some(Grouping::OrderAroundSharedOutputs)
        );
        fp = base;
        fp.disabled.source = DisabledSource::Github;
        assert!(summarize(&fp).unwrap().remote_disabled_status_checked);
    }
}
