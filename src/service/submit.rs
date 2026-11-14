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
    command_with_mode(context, command, revision, run_id, task_id, false).await
}

pub async fn command_with_mode(
    mut context: Context,
    command: Vec<String>,
    revision: Option<String>,
    run_id: RunId,
    task_id: TaskId,
    in_place: bool,
) -> Result<Receipt, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    if in_place && (!context.config.local_worker.value || revision.is_some()) {
        return Err(invalid("in-place requires the parent local worker".into()));
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
        crate::source::SourceStore::open(&freeze_state).and_then(|s| s.mutation_guard())
    })
    .await
    .map_err(|e| invalid(e.to_string()))?
    .map_err(|e| invalid(e.to_string()))?;
    let (toolchains, resolution_guard) = {
        let capture_context = context.clone();
        let manifest = captured.manifest.clone();
        let source_digest = digest.clone();
        let requirements = requirements.clone();
        tokio::task::spawn_blocking(move || {
            // Blocking source IO retains ownership even when the submit future
            // is cancelled. Return the guard only after that IO has drained.
            let frozen =
                freeze_toolchains(&capture_context, &manifest, source_digest, &requirements);
            (frozen, resolution_guard)
        })
        .await
        .map_err(|e| invalid(e.to_string()))?
    };
    let toolchains = match toolchains? {
        Some(bundle) if bundle.validate(&digest).is_err() => {
            match owner
                .request(Operation::ResolveToolchains {
                    bundle,
                    requirements: requirements.clone(),
                })
                .await
                .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?
            {
                Reply::ResolvedToolchains { bundle } => {
                    bundle.validate(&digest).map_err(|e| invalid(e.into()))?;
                    Some(bundle)
                }
                Reply::ControlError { error } => return Err(error),
                _ => {
                    return Err(ApiError::blocked(
                        crate::error::BlockedReason::EnvironmentMismatch,
                        "toolchain resolve required",
                    ))
                }
            }
        }
        other => other,
    };
    drop(resolution_guard);
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
        toolchains: toolchains.clone(),
        requirements: requirements.clone(),
        in_place,
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
    match owner
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
        })? {
        Reply::Accepted { run_id: accepted } if accepted == run_id => Ok(Receipt {
            schema_version: 1,
            run_id,
            task_id,
            source_id,
            state: "coordinator_accepted",
            warnings: captured.warnings,
        }),
        Reply::Error { message } => Err(invalid(message)),
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "unexpected submit receipt",
        )),
    }
}

fn freeze_toolchains(
    context: &Context,
    captured: &crate::source::manifest::SourceManifest,
    digest: Digest,
    requirements: &crate::toolchain::capability::ExecutionRequirements,
) -> Result<Option<crate::toolchain::contract::FrozenBundle>, ApiError> {
    use crate::toolchain::{
        contract::{self, FrozenBundle},
        versions::{FrozenToolchains, Location, MatchPolicy, Scope, ToolId, VersionObservation},
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
    let declarations = crate::toolchain::snapshot::read_captured(&store, &manifest, project, &[])
        .map_err(|e| blocked(e.code))?;
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
    if declarations.is_empty() && required.is_empty() {
        return Ok(None);
    }
    let policy = match context.config.toolchain_match.value {
        crate::config::model::ToolchainMatch::Exact => MatchPolicy::Exact,
        crate::config::model::ToolchainMatch::Minor => MatchPolicy::Minor,
        crate::config::model::ToolchainMatch::Major => MatchPolicy::Major,
        crate::config::model::ToolchainMatch::Any => MatchPolicy::Any,
    };
    let observations = contract::observations(&profile.report());
    let frozen = FrozenToolchains::freeze(
        digest,
        profile.definition_digest().clone(),
        Scope::project(project),
        &declarations,
        &required,
        &observations,
        policy,
    )
    .map_err(|e| blocked(e.code))?;
    if frozen
        .baseline
        .iter()
        .any(|b| b.version == VersionObservation::Unknown)
    {
        return Err(blocked(
            "toolchain baseline unknown; capture requires an observed required tool".into(),
        ));
    }
    let mut bundle = FrozenBundle::seal_exact(frozen).map_err(|e| blocked(e.into()))?;
    if bundle.validate(&bundle.frozen.source_manifest).is_ok() {
        return Ok(Some(bundle));
    }
    // Loading a configured inventory is read-only and invokes no managers.
    if let Ok(registry) = crate::toolchain::registry::Registry::load(
        &context.bootstrap.worker_state_dir,
        &profile,
        profile.report().profile_fingerprint,
    ) {
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
    Ok(Some(bundle))
}
