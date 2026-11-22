//! Freeze command input once and obtain an acceptance receipt from the existing owner.
use super::{
    context::Context,
    resident::{Operation, Reply},
};
use crate::{
    coordinator::TaskSpec,
    error::{ApiError, ErrorCode},
    ids::*,
    wire::CommandSpec,
};
use serde::Serialize;
#[derive(Debug, Serialize)]
pub struct Receipt {
    pub schema_version: u32,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub source_id: SourceId,
    pub state: &'static str,
    #[serde(skip)]
    pub warnings: Vec<String>,
}

/// Where a submitted command may run.
#[derive(Debug, Clone, Copy, Default)]
pub struct Mode {
    /// In the original directory, on the local worker (`--in-place`).
    pub in_place: bool,
    /// On the local worker, from a snapshot (`--local`).
    pub local: bool,
}

pub async fn command(
    context: Context,
    command: Vec<String>,
    revision: Option<String>,
) -> Result<Receipt, ApiError> {
    command_with_ids(
        context,
        command,
        revision,
        RunId::generate(),
        TaskId::generate(),
    )
    .await
}
pub async fn command_with_ids(
    context: Context,
    command: Vec<String>,
    revision: Option<String>,
    run_id: RunId,
    task_id: TaskId,
) -> Result<Receipt, ApiError> {
    command_with_mode(context, command, revision, run_id, task_id, Mode::default()).await
}

pub async fn command_with_mode(
    mut context: Context,
    command: Vec<String>,
    revision: Option<String>,
    run_id: RunId,
    task_id: TaskId,
    mode: Mode,
) -> Result<Receipt, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    let in_place = mode.in_place;
    if in_place && (!context.config.local_worker.value || revision.is_some()) {
        return Err(invalid("in-place requires the parent local worker".into()));
    }
    if mode.local && !context.config.local_worker.value {
        return Err(invalid("--local requires the parent local worker".into()));
    }
    if in_place {
        let root = context.repo_root.as_deref().unwrap_or(&context.cwd);
        let root = std::fs::canonicalize(root).map_err(|e| invalid(e.to_string()))?;
        let home = context
            .env
            .get("HOME")
            .and_then(|h| std::fs::canonicalize(h).ok());
        for state in [
            &context.bootstrap.state_dir,
            &context.bootstrap.worker_state_dir,
        ] {
            let state = std::fs::canonicalize(state).unwrap_or(state.clone());
            crate::workspace::plan::check_in_place(
                &root,
                std::path::Path::new(""),
                &state,
                home.as_deref(),
                true,
            )?;
        }
        context.config.source_mode.value = crate::config::model::SourceMode::Worktree;
    }
    let requirements = crate::toolchain::capability::ExecutionRequirements::parse(
        context.config.profile.value.clone(),
        &context.config.requires.value,
    )
    .map_err(invalid)?;
    if command.is_empty() {
        return Err(invalid("command cannot be empty".into()));
    }
    let owner = context
        .owner()
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?;
    let (context, captured, run_id) = tokio::task::spawn_blocking(move || {
        let captured = super::input::capture(&context, &run_id, revision.as_deref());
        (context, captured, run_id)
    })
    .await
    .map_err(|e| invalid(e.to_string()))?;
    let captured = captured?;
    let source_id = captured.manifest.source_id.clone();
    let digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .map_err(|e| invalid(e.to_string()))?;
    let cwd = String::from_utf8(captured.manifest.cwd_relative.0.clone())
        .map_err(|e| invalid(e.to_string()))?;
    let freeze_state = context.state_dir().to_owned();
    let resolution_guard = tokio::task::spawn_blocking(move || {
        crate::source::SourceStore::open_foreground(&freeze_state).map(|(_source, guard)| guard)
    })
    .await
    .map_err(|e| invalid(e.to_string()))?
    .map_err(|error| {
        if error.is::<crate::source::retention::ForegroundBusy>() {
            crate::source::classify::api_error_of(&error)
        } else {
            invalid(error.to_string())
        }
    })?;
    let toolchains = freeze_resolved(
        &context,
        &owner,
        &captured.manifest,
        &digest,
        &requirements,
        resolution_guard,
    )
    .await?;
    let env = context
        .config
        .pass_env
        .value
        .iter()
        .filter_map(|key| {
            context
                .env
                .get(key)
                .map(|value| (key.clone(), value.clone()))
        })
        .collect();
    let timeout = context
        .config
        .timeout_seconds
        .value
        .map(|s| {
            s.checked_mul(1000)
                .ok_or_else(|| invalid("command timeout overflow".into()))
        })
        .transpose()?;
    let has_fast_worktree = captured
        .manifest
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.0 == b".fast-worktree.toml");
    let setup = crate::setup::policy::prepare_spec(
        &context.config.prepare_commands,
        has_fast_worktree,
        false,
    );
    let outputs: Vec<crate::wire::OutputDecl> = context
        .config
        .outputs
        .iter()
        .map(|o| crate::wire::OutputDecl {
            name: o.name.clone(),
            paths: o.paths.clone(),
            if_missing: if o.required {
                crate::wire::MissingPolicy::Required
            } else {
                crate::wire::MissingPolicy::Optional
            },
            when: match o.when {
                crate::config::model::OutputWhen::Always => crate::wire::OutputWhen::Always,
                crate::config::model::OutputWhen::Success => crate::wire::OutputWhen::Success,
                crate::config::model::OutputWhen::Failure => crate::wire::OutputWhen::Failure,
            },
        })
        .collect();
    crate::outputs::validate(&[], &outputs).map_err(|e| invalid(e.into()))?;
    let mut spec = TaskSpec {
        ci: None,
        toolchains: toolchains.clone(),
        requirements: requirements.clone(),
        in_place,
        local: mode.local,
        outputs: outputs.clone(),
        setup,
        plan_digest: if let Some(bundle) = &toolchains {
            Digest::of_bytes(
                &serde_json::to_vec(&(&command, &outputs, &requirements, bundle.digest()))
                    .map_err(|e| invalid(e.to_string()))?,
            )
        } else if requirements.is_empty() {
            crate::outputs::command_plan_digest(&command, &outputs)
                .map_err(|e| invalid(e.to_string()))?
        } else {
            Digest::of_bytes(
                &serde_json::to_vec(&(&command, &outputs, &requirements))
                    .map_err(|e| invalid(e.to_string()))?,
            )
        },
        manifest_digest: digest,
        commands: vec![CommandSpec {
            node_id: NodeId::parse("command").map_err(|e| invalid(e.to_string()))?,
            argv: command,
            cwd,
            env,
            depends_on: vec![],
            timeout_ms: timeout,
            setup_step: None,
        }],
        command_timeout_ms: timeout,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: !in_place && !captured.manifest.secret_files.is_empty(),
        artifacts: Vec::new(),
    };
    if in_place {
        if spec.setup.take().is_some() {
            let mut prep = Vec::new();
            for (i, argv) in context.config.prepare_commands.iter().enumerate() {
                let node =
                    NodeId::parse(&format!("prepare-{i}")).map_err(|e| invalid(e.to_string()))?;
                let depends_on = prep
                    .last()
                    .map(|c: &CommandSpec| vec![c.node_id.clone()])
                    .unwrap_or_default();
                prep.push(CommandSpec {
                    node_id: node,
                    argv: argv.clone(),
                    cwd: String::new(),
                    env: spec.commands[0].env.clone(),
                    depends_on,
                    timeout_ms: timeout,
                    setup_step: None,
                });
            }
            if let Some(last) = prep.last() {
                spec.commands[0].depends_on.push(last.node_id.clone());
            }
            prep.append(&mut spec.commands);
            spec.commands = prep;
        }
        crate::workspace::in_place::bind(
            context.state_dir(),
            &run_id,
            &source_id,
            &spec.manifest_digest,
            &context.bootstrap.worker_id,
            context.repo_root.as_deref().unwrap_or(&context.cwd),
            context
                .config
                .secret_files
                .value
                .iter()
                .map(|s| (s.glob.clone(), s.required))
                .collect(),
        )?;
    }
    // The binding must exist before the owner can accept the run (the worker
    // reads it at dispatch), so it is written first and removed again when the
    // owner definitively refuses. An unconfirmed submit keeps it: the run may exist.
    let reply = owner
        .request(Operation::SubmitCaptured {
            run_id: run_id.clone(),
            source_id: source_id.clone(),
            tasks: vec![(task_id.clone(), spec)],
        })
        .await
        .map_err(|e| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!("submit unconfirmed for run {run_id}, task {task_id}: {e}"),
            )
        })?;
    if in_place && matches!(reply, Reply::ControlError { .. } | Reply::Error { .. }) {
        crate::workspace::in_place::unbind(context.state_dir(), &run_id);
    }
    match reply {
        Reply::Accepted { run_id: accepted } if accepted == run_id => Ok(Receipt {
            schema_version: 1,
            run_id,
            task_id,
            source_id,
            state: "coordinator_accepted",
            warnings: captured.warnings,
        }),
        Reply::ControlError { error } => Err(error),
        Reply::Error { message } => Err(invalid(message)),
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "unexpected submit receipt",
        )),
    }
}

/// The toolchain selection a task of this source carries: frozen from the
/// version declarations the capture holds, and, when the evidence is not
/// complete, resolved once by the owner's worker. `guard` keeps the captured
/// source alive until that is done. A plain command and a CI run use this one
/// path, so the same source freezes to the same bundle.
pub(super) async fn freeze_resolved(
    context: &Context,
    owner: &super::resident::Client,
    manifest: &crate::source::manifest::SourceManifest,
    digest: &Digest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
    guard: crate::source::retention::Guard,
) -> Result<Option<crate::toolchain::contract::FrozenBundle>, ApiError> {
    let (mut all, _) =
        freeze_resolved_each(context, owner, manifest, digest, requirements, None, guard).await?;
    Ok(all.pop().flatten())
}

/// [`freeze_resolved`] for the tasks of a CI plan, in plan order: each task
/// carries the project's declarations and those of the `setup-*` steps of its
/// own jobs (spec 22.1). Tasks that freeze to the same bundle are resolved
/// once. The second value is the notices of the freeze: one line for each
/// input of a setup step that is no requirement.
pub(super) async fn freeze_resolved_tasks(
    context: &Context,
    owner: &super::resident::Client,
    manifest: &crate::source::manifest::SourceManifest,
    digest: &Digest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
    plan: &local_ci::FrozenPlan,
    guard: crate::source::retention::Guard,
) -> Result<Frozen, ApiError> {
    let plan = Some(plan.clone());
    freeze_resolved_each(context, owner, manifest, digest, requirements, plan, guard).await
}

/// The bundle of each task, and the notices of the freeze.
type Frozen = (
    Vec<Option<crate::toolchain::contract::FrozenBundle>>,
    Vec<String>,
);

async fn freeze_resolved_each(
    context: &Context,
    owner: &super::resident::Client,
    manifest: &crate::source::manifest::SourceManifest,
    digest: &Digest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
    plan: Option<local_ci::FrozenPlan>,
    guard: crate::source::retention::Guard,
) -> Result<Frozen, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    let plan = std::sync::Arc::new(plan);
    let (prepared, guard) = {
        let (context, manifest) = (context.clone(), manifest.clone());
        let (requirements, plan) = (requirements.clone(), plan.clone());
        tokio::task::spawn_blocking(move || {
            // Blocking source IO retains ownership even when the submit future
            // is cancelled. Return the guard only after that IO has drained.
            let prepared = prepare(&context, &manifest, &requirements, plan.as_ref().as_ref());
            (prepared, guard)
        })
        .await
        .map_err(|e| invalid(e.to_string()))?
    };
    let mut prepared = prepared?;
    // The setup-step aliases of each task that nothing resolves: no
    // requirement, and a notice (spec 22.1).
    let tasks = plan.as_ref().as_ref().map_or(1, |plan| plan.tasks.len());
    let mut dropped: Vec<Vec<crate::toolchain::versions::ToolId>> = vec![Vec::new(); tasks];
    loop {
        let (frozen, back) = {
            let (context, digest) = (context.clone(), digest.clone());
            let (plan, dropped) = (plan.clone(), dropped.clone());
            tokio::task::spawn_blocking(move || {
                // Reads this host's inventory only: the source was read above.
                let plan = plan.as_ref().as_ref();
                let frozen = freeze_prepared(&context, &prepared, digest, plan, &dropped);
                (frozen, prepared)
            })
            .await
            .map_err(|e| invalid(e.to_string()))?
        };
        prepared = back;
        let (frozen, aliases, notices) = frozen?;
        let mut resolved: Vec<Option<crate::toolchain::contract::FrozenBundle>> = Vec::new();
        let mut again = false;
        for (index, bundle) in frozen.iter().enumerate() {
            // The same unresolved bundle is the same question to the owner.
            if let Some(earlier) = frozen[..index].iter().position(|b| b == bundle) {
                resolved.push(resolved[earlier].clone());
                continue;
            }
            match resolve_bundle(owner, digest, requirements, bundle.clone()).await {
                Ok(bundle) => resolved.push(bundle),
                Err(unresolved) => {
                    // An alias of a setup step that stays unresolved is no
                    // requirement. Anything else is refused as it was.
                    let open = unresolved_aliases(&unresolved, bundle.as_ref(), &aliases[index]);
                    if open.is_empty() {
                        return Err(unresolved);
                    }
                    for (other, drop) in frozen.iter().zip(dropped.iter_mut()) {
                        if other == bundle {
                            drop.extend(open.iter().cloned());
                        }
                    }
                    again = true;
                    resolved.push(None);
                }
            }
        }
        if !again {
            drop(guard);
            return Ok((resolved, crate::ci::toolchains::render(&notices)));
        }
    }
}

/// The tools of `aliases` (the setup-step aliases of the task `bundle` was
/// frozen for) that `bundle` holds no evidence for, when `refused` says the
/// bundle could not be resolved. An answer that never came says nothing of
/// the kind.
fn unresolved_aliases(
    refused: &ApiError,
    bundle: Option<&crate::toolchain::contract::FrozenBundle>,
    aliases: &[crate::toolchain::versions::ToolId],
) -> Vec<crate::toolchain::versions::ToolId> {
    let mismatch = matches!(
        refused.blocked_reason,
        Some(crate::error::BlockedReason::EnvironmentMismatch)
    );
    let Some(bundle) = bundle.filter(|_| mismatch) else {
        return Vec::new();
    };
    let open = |tool: &&crate::toolchain::versions::ToolId| {
        !bundle.evidence.iter().any(|e| &e.tool == *tool)
    };
    aliases.iter().filter(open).cloned().collect()
}

async fn resolve_bundle(
    owner: &super::resident::Client,
    digest: &Digest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
    frozen: Option<crate::toolchain::contract::FrozenBundle>,
) -> Result<Option<crate::toolchain::contract::FrozenBundle>, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    let toolchains = match frozen {
        Some(bundle) if bundle.validate(digest).is_err() => {
            let asked = bundle.frozen.describe_requirements();
            match owner
                .request(Operation::ResolveToolchains {
                    bundle,
                    requirements: requirements.clone(),
                })
                .await
                .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?
            {
                Reply::ResolvedToolchains { bundle } => {
                    bundle.validate(digest).map_err(|e| invalid(e.into()))?;
                    Some(bundle)
                }
                Reply::ControlError { error } => return Err(error),
                _ => {
                    return Err(ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        format!("toolchain resolve required ({asked})"),
                    ))
                }
            }
        }
        other => other,
    };
    Ok(toolchains)
}

/// `code at path:line and path:line`: a contradiction says where both sides
/// are written.
fn located(diagnostic: &crate::toolchain::versions::Diagnostic) -> String {
    let at: Vec<String> = diagnostic
        .locations
        .iter()
        .map(|l| format!("{}:{}", l.path, l.first_line))
        .collect();
    if at.is_empty() {
        return diagnostic.code.clone();
    }
    format!("{} at {}", diagnostic.code, at.join(" and "))
}

/// What the freeze reads once: the declarations of the captured source and
/// this host's profile.
struct Prepared {
    project: String,
    /// The declarations of a plain command.
    plain: Vec<crate::toolchain::versions::Declaration>,
    /// What each task of the plan reads of its workflow, in plan order.
    sources: Vec<crate::ci::toolchains::TaskSource>,
    profile: crate::toolchain::profile::Profile,
    required: Vec<(
        crate::toolchain::versions::ToolId,
        crate::toolchain::versions::Location,
    )>,
}

/// Read the captured source for a plain command (`plan` is `None`: no
/// workflow is read) or for each task of `plan`.
fn prepare(
    context: &Context,
    captured: &crate::source::manifest::SourceManifest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
    plan: Option<&local_ci::FrozenPlan>,
) -> Result<Prepared, ApiError> {
    use crate::toolchain::{
        contract,
        versions::{Location, ToolId},
    };
    let blocked =
        |e: String| ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, e);
    let store = crate::source::SourceStore::open(context.state_dir())
        .map_err(|e| blocked(e.to_string()))?;
    let manifest = store
        .manifest(&captured.ci_source_id)
        .map_err(|e| blocked(e.to_string()))?
        .ok_or_else(|| blocked("captured toolchain source unavailable".into()))?;
    if manifest.manifest_digest != captured.manifest_digest {
        return Err(blocked("captured toolchain source binding changed".into()));
    }
    let project = std::str::from_utf8(&manifest.cwd_relative.0)
        .map_err(|_| blocked("invalid toolchain project scope".into()))?;
    let mut plain = Vec::new();
    let mut sources: Vec<crate::ci::toolchains::TaskSource> = Vec::new();
    match plan {
        None => {
            plain = crate::toolchain::snapshot::read_captured(&store, &manifest, project, &[])
                .map_err(|e| blocked(e.code))?;
        }
        Some(plan) => {
            for (index, task) in plan.tasks.iter().enumerate() {
                // The tasks of one workflow read the same of it.
                let same = |t: &local_ci::dag::ExecutionTask| t.workflow == task.workflow;
                let source = match plan.tasks[..index].iter().position(same) {
                    Some(earlier) => sources[earlier].clone(),
                    None => {
                        crate::ci::toolchains::read_task(&store, &manifest, project, &task.workflow)
                            .map_err(|e| blocked(located(&e)))?
                    }
                };
                sources.push(source);
            }
        }
    }
    // Required profile tools are capture-time baseline inputs, not all optional probes.
    let profile = crate::toolchain::profile::Profile::load(
        &context.bootstrap.worker_state_dir,
        &context.bootstrap.env,
    )
    .map_err(blocked)?;
    let generic = requirements.requires.iter().filter_map(|r| match r {
        crate::toolchain::capability::Requirement::Tool(name) => Some(name.clone()),
        _ => None,
    });
    let required = profile
        .required_tools()
        .iter()
        .cloned()
        .chain(generic)
        .map(|name| {
            ToolId::parse(contract::canonical_tool(&name))
                .map(|tool| {
                    (
                        tool,
                        Location {
                            path: "profile:required_tools".into(),
                            first_line: 1,
                            last_line: 1,
                        },
                    )
                })
                .map_err(|e| blocked(e.into()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Prepared {
        project: project.into(),
        plain,
        sources,
        profile,
        required,
    })
}

/// The bundles of a freeze, the setup-step aliases each was frozen with, and
/// the notices of what is no requirement.
type Bundles = (
    Vec<Option<crate::toolchain::contract::FrozenBundle>>,
    Vec<Vec<crate::toolchain::versions::ToolId>>,
    Vec<crate::ci::toolchains::Notice>,
);

/// One bundle for a plain command, one per task of `plan` otherwise, in plan
/// order. `dropped` names, for each task, the tools whose setup-step alias
/// nothing resolved: no requirement comes from those.
fn freeze_prepared(
    context: &Context,
    prepared: &Prepared,
    digest: Digest,
    plan: Option<&local_ci::FrozenPlan>,
    dropped: &[Vec<crate::toolchain::versions::ToolId>],
) -> Result<Bundles, ApiError> {
    use crate::toolchain::{
        contract::{self, FrozenBundle},
        versions::{FrozenToolchains, MatchPolicy, Scope},
    };
    let blocked =
        |e: String| ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, e);
    let (project, profile) = (prepared.project.as_str(), &prepared.profile);
    // What each bundle is frozen from: the project as a whole for a plain
    // command, the declarations of its own jobs for a task of a plan. An
    // input of a setup step belt cannot interpret requires nothing and is
    // told as a notice; the worker reads the same with the same function.
    let mut sets = Vec::new();
    let mut aliases = Vec::new();
    let mut notices = Vec::new();
    match plan {
        None => {
            sets.push((
                String::new(),
                Scope::project(project),
                prepared.plain.clone(),
            ));
            aliases.push(Vec::new());
        }
        Some(plan) => {
            for (index, task) in plan.tasks.iter().enumerate() {
                let unresolved = |tool: &_, _: &str| dropped[index].contains(tool);
                let freeze = crate::ci::toolchains::task_declarations(
                    &prepared.sources[index],
                    plan,
                    task,
                    project,
                    &unresolved,
                );
                aliases.push(freeze.aliases());
                notices.extend(freeze.notices);
                let who = format!("task `{}` (job {}): ", task.id, task.jobs.join(", "));
                sets.push((who, freeze.scope, freeze.declarations));
            }
        }
    }
    let policy = match context.config.toolchain_match.value {
        crate::config::model::ToolchainMatch::Exact => MatchPolicy::Exact,
        crate::config::model::ToolchainMatch::Minor => MatchPolicy::Minor,
        crate::config::model::ToolchainMatch::Major => MatchPolicy::Major,
        crate::config::model::ToolchainMatch::Any => MatchPolicy::Any,
    };
    let observations = contract::observations(&profile.report());
    let mut bundles: Vec<Option<FrozenBundle>> = Vec::with_capacity(sets.len());
    for (index, (who, scope, declarations)) in sets.iter().enumerate() {
        // Tasks with the same declarations in the same scope share a bundle.
        let same = |(_, s, d): &(String, Scope, Vec<_>)| s == scope && d == declarations;
        if let Some(earlier) = sets[..index].iter().position(same) {
            bundles.push(bundles[earlier].clone());
            continue;
        }
        if declarations.is_empty() && prepared.required.is_empty() {
            bundles.push(None);
            continue;
        }
        let frozen = FrozenToolchains::freeze(
            digest.clone(),
            profile.definition_digest().clone(),
            scope.clone(),
            declarations,
            &prepared.required,
            &observations,
            policy,
        )
        .map_err(|e| {
            blocked(if plan.is_some() {
                format!("{who}{}", located(&e))
            } else {
                e.code
            })
        })?;
        bundles.push(Some(seal(context, profile, frozen)?));
    }
    // What a worker will check of each bundle, checked here first: a bundle
    // the source does not read back to is refused at the submit, not on a
    // worker.
    if let Some(plan) = plan {
        for (index, task) in plan.tasks.iter().enumerate() {
            let Some(bundle) = &bundles[index] else {
                continue;
            };
            let source = &prepared.sources[index];
            crate::ci::toolchains::check_frozen(source, plan, task, project, &bundle.frozen)
                .map_err(|e| blocked(format!("{}{e}", sets[index].0)))?;
        }
    }
    Ok((bundles, aliases, notices))
}

/// Seal what the source pins exactly, and take what the configured inventory
/// already answers.
fn seal(
    context: &Context,
    profile: &crate::toolchain::profile::Profile,
    frozen: crate::toolchain::versions::FrozenToolchains,
) -> Result<crate::toolchain::contract::FrozenBundle, ApiError> {
    use crate::toolchain::{contract::FrozenBundle, versions::VersionObservation};
    let blocked =
        |e: String| ApiError::blocked(crate::error::BlockedReason::EnvironmentMismatch, e);
    let unknown: Vec<&str> = frozen
        .baseline
        .iter()
        .filter(|b| b.version == VersionObservation::Unknown)
        .map(|b| b.tool.as_str())
        .collect();
    if !unknown.is_empty() {
        return Err(blocked(format!(
            "toolchain baseline unknown; capture requires an observed required tool ({})",
            unknown.join(", ")
        )));
    }
    let mut bundle = FrozenBundle::seal_exact(frozen).map_err(|e| blocked(e.into()))?;
    if bundle.validate(&bundle.frozen.source_manifest).is_ok() {
        return Ok(bundle);
    }
    // Loading a configured inventory is read-only and invokes no managers.
    if let Some(registry) = worker_inventory(&context.bootstrap.worker_state_dir, profile) {
        for requirement in bundle.frozen.requirements.clone() {
            if bundle.evidence.iter().any(|e| e.tool == requirement.tool) {
                continue;
            }
            if let Ok(request) = registry.request(&bundle.frozen, &requirement.tool, None) {
                bundle.frozen = request.frozen;
                bundle.evidence.push(request.evidence);
            }
        }
    }
    Ok(bundle)
}

/// The inventory of the local worker, if this host has one. The worker opens
/// its state through the canonical path and the inventory refuses any other
/// spelling, so a state directory reached through a symlink is resolved first;
/// otherwise its evidence would silently be left out of the capture.
fn worker_inventory(
    worker_state_dir: &std::path::Path,
    profile: &crate::toolchain::profile::Profile,
) -> Option<crate::toolchain::registry::Registry> {
    let state = std::fs::canonicalize(worker_state_dir).ok()?;
    crate::toolchain::registry::Registry::load(
        &state,
        profile,
        profile.report().profile_fingerprint,
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::worker_inventory;
    use crate::toolchain::{profile::Profile, registry::Registry};
    use std::os::unix::fs::{symlink, DirBuilderExt};

    #[test]
    fn the_worker_inventory_is_found_through_a_symlinked_state_directory() {
        let root = crate::test_tmp::tempdir().unwrap();
        let root = std::fs::canonicalize(root.path()).unwrap();
        let real = root.join("real");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(real.join("toolchains"))
            .unwrap();
        let link = root.join("link");
        symlink(&real, &link).unwrap();
        let env = [("PATH".to_string(), real.display().to_string())].into();
        let profile = Profile::load(&link, &env).unwrap();
        // The raw spelling is what the inventory itself refuses.
        assert_eq!(
            Registry::load(&link, &profile, profile.report().profile_fingerprint).err(),
            Some("noncanonical_toolchains_path".to_string())
        );
        assert!(worker_inventory(&link, &profile).is_some());
        assert!(worker_inventory(&real, &profile).is_some());
        // No local worker state at all is not an inventory.
        assert!(worker_inventory(&root.join("absent"), &profile).is_none());
    }
}
