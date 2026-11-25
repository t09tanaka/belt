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
    context: Context,
    command: Vec<String>,
    revision: Option<String>,
    run_id: RunId,
    task_id: TaskId,
    mode: Mode,
) -> Result<Receipt, ApiError> {
    let (state, run) = (context.state_dir().to_owned(), run_id.clone());
    let result = submit_command(context, command, revision, run_id, task_id, mode).await;
    if let Err(error) = &result {
        // A submission that failed or was refused will not be sent again.
        super::input::give_up_after(state, run, error).await;
    }
    result
}

async fn submit_command(
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
    let mut requirements = crate::toolchain::capability::ExecutionRequirements::parse(
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
    super::input::capture_begins(&run_id);
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
    let prepare_manifest = (!in_place).then(|| (context.clone(), run_id.clone()));
    let (resolution_guard, prepare) = tokio::task::spawn_blocking(move || {
        crate::source::SourceStore::open_foreground(&freeze_state).map(|(source, guard)| {
            let prepare = prepare_manifest.map(|(context, run_id)| {
                let manifest =
                    super::input::source_manifest(&context, &run_id).map_err(|e| e.message)?;
                crate::toolchain::snapshot::read_captured_prepare_tools(&source, &manifest)
            });
            (guard, prepare)
        })
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
    // An `--in-place` workspace is the caller's own and is not prepared.
    if let Some(prepare) = prepare {
        let prepare = prepare
            .map_err(|e| invalid(format!("cannot read how the workspace is prepared: {e}")))?;
        requirements = super::ci_run::run_requirements(requirements, &prepare)?;
    }
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
    // A plain command is refused as a whole when nothing resolves it.
    all.pop().transpose().map(Option::flatten)
}

/// [`freeze_resolved`] for the tasks of a CI plan, in plan order: each task
/// carries the project's declarations and those of the `setup-*` steps of its
/// own jobs (spec 22.1). Tasks that freeze to the same bundle are resolved
/// once. The second value is the notices of the freeze: one line for each
/// input of a setup step that is no requirement. A task whose requirements
/// no worker resolves is that refusal (spec 22.1): the run goes on without
/// it. When that is every task of the plan, nothing would run, and the
/// refusal of the first is the error of the submit.
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
    let (tasks, notices) =
        freeze_resolved_each(context, owner, manifest, digest, requirements, plan, guard).await?;
    Ok((all_unplaceable(tasks)?, notices))
}

/// The selections of the tasks of a run, or the refusal of the first task
/// when no task of it can be placed.
fn all_unplaceable(tasks: Vec<Selection>) -> Result<Vec<Selection>, ApiError> {
    match tasks.iter().position(Result::is_ok) {
        None if !tasks.is_empty() => Err(tasks.into_iter().find_map(Result::err).expect("a task")),
        _ => Ok(tasks),
    }
}

/// The toolchain selection of one task: its bundle (`None` when it requires
/// nothing), or the refusal that no worker resolves its requirements. A
/// refused task is never one that requires nothing.
pub(crate) type Selection = Result<Option<crate::toolchain::contract::FrozenBundle>, ApiError>;

/// The selection of each task, and the notices of the freeze.
type Frozen = (Vec<Selection>, Vec<String>);

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
    let prepared = std::sync::Arc::new(prepared?);
    // One freeze of every task, with the setup-step aliases `dropped` names
    // left out.
    let freeze = |dropped: Vec<Vec<crate::toolchain::versions::ToolId>>| {
        let (context, digest) = (context.clone(), digest.clone());
        let (plan, prepared) = (plan.clone(), prepared.clone());
        async move {
            tokio::task::spawn_blocking(move || {
                // Reads this host's inventory only: the source was read above.
                let plan = plan.as_ref().as_ref();
                freeze_prepared(&context, &prepared, digest, plan, &dropped)
            })
            .await
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?
        }
    };
    let tasks = plan.as_ref().as_ref().map_or(1, |plan| plan.tasks.len());
    let ask = |bundle, observed: Observed| async move {
        resolve_bundle(owner, digest, requirements, bundle, &observed).await
    };
    let (resolved, notices) = resolve_each(tasks, freeze, ask).await?;
    drop(guard);
    Ok((resolved, crate::ci::toolchains::render(&notices)))
}

/// The resolved bundle of each of `tasks` tasks, and the notices of the
/// freeze they come from. `freeze` freezes every task with the setup-step
/// aliases it is given left out, and `ask` resolves one bundle.
async fn resolve_each<Freeze, Freezing, Ask, Asking>(
    tasks: usize,
    freeze: Freeze,
    ask: Ask,
) -> Result<(Vec<Selection>, Vec<crate::ci::toolchains::Notice>), ApiError>
where
    Freeze: Fn(Vec<Vec<crate::toolchain::versions::ToolId>>) -> Freezing,
    Freezing: std::future::Future<Output = Result<Bundles, ApiError>>,
    Ask: Fn(crate::toolchain::contract::FrozenBundle, Observed) -> Asking,
    Asking: std::future::Future<Output = Result<crate::toolchain::contract::FrozenBundle, Refusal>>,
{
    // The setup-step aliases of each task that nothing resolves: no
    // requirement, and a notice (spec 22.1).
    let mut dropped: Vec<Vec<crate::toolchain::versions::ToolId>> = vec![Vec::new(); tasks];
    // What was already resolved: a bundle that comes back unchanged in a
    // later pass is not asked for again.
    let mut asked = Asked::default();
    // The bundles nothing resolves, with the refusal: the task is refused by
    // itself, and the next pass does not ask again.
    let mut refused: Vec<(crate::toolchain::contract::FrozenBundle, ApiError)> = Vec::new();
    loop {
        // A freeze that fails after an alias was left out fails for a reason
        // of its own, and that is the error.
        let (frozen, aliases, notices, observed) = freeze(dropped.clone()).await?;
        let mut resolved: Vec<Selection> = Vec::new();
        let mut again = false;
        for (index, bundle) in frozen.iter().enumerate() {
            // The same unresolved bundle is the same question to the owner.
            if let Some(earlier) = frozen[..index].iter().position(|b| b == bundle) {
                resolved.push(resolved[earlier].clone());
                continue;
            }
            if let Some((_, error)) = refused.iter().find(|(b, _)| Some(b) == bundle.as_ref()) {
                resolved.push(Err(error.clone()));
                continue;
            }
            let unresolved = match asked.resolve(&ask, bundle.clone(), &observed).await {
                Ok(bundle) => {
                    resolved.push(Ok(bundle));
                    continue;
                }
                // No answer about the requirements leaves nothing out.
                Err(Refusal::Other(error)) => return Err(error),
                Err(Refusal::Unresolved(unresolved)) => unresolved,
            };
            // A range the tool of this host satisfies was sealed to that
            // version first, and the bundle is still open. An alias of a
            // setup step that stays unresolved is no requirement. Anything
            // else is refused as it was.
            let open = unresolved_aliases(bundle.as_ref(), &aliases[index]);
            // Several aliases are open: each is asked for with the others
            // left out, and only one that is refused by itself is left out.
            let mut alone = vec![false; open.len()];
            for (tool, alone) in open.iter().zip(alone.iter_mut()) {
                if open.len() == 1 {
                    break;
                }
                let mut without = dropped.clone();
                for (other, drop) in frozen.iter().zip(without.iter_mut()) {
                    if other == bundle {
                        drop.extend(open.iter().filter(|other| *other != tool).cloned());
                    }
                }
                let (probe, _, _, observed) = freeze(without).await?;
                match asked.resolve(&ask, probe[index].clone(), &observed).await {
                    Ok(_) => *alone = true,
                    Err(Refusal::Other(error)) => return Err(error),
                    Err(Refusal::Unresolved(_)) => {}
                }
            }
            let open = left_out(open, &alone);
            if open.is_empty() {
                // Nothing can be left out: this task is refused, by itself
                // and with what it requires; the other tasks go on.
                let unresolved = with_requirements(unresolved, bundle.as_ref());
                if let Some(bundle) = bundle {
                    refused.push((bundle.clone(), unresolved.clone()));
                }
                resolved.push(Err(unresolved));
                continue;
            }
            for (other, drop) in frozen.iter().zip(dropped.iter_mut()) {
                if other == bundle {
                    drop.extend(open.iter().cloned());
                }
            }
            again = true;
            resolved.push(Ok(None));
        }
        if !again {
            return Ok((resolved, notices));
        }
    }
}

/// The refusal `error` of `bundle`, saying what the bundle requires.
fn with_requirements(
    mut error: ApiError,
    bundle: Option<&crate::toolchain::contract::FrozenBundle>,
) -> ApiError {
    if let Some(bundle) = bundle {
        let required = bundle.frozen.describe_requirements();
        if !error.message.contains(&required) {
            error.message = format!("{} ({required})", error.message);
        }
    }
    error
}

/// Why a bundle was not resolved.
#[derive(Debug, PartialEq)]
enum Refusal {
    /// The owner answered that nothing resolves the requirements, and what
    /// this host observed does not answer them either.
    Unresolved(ApiError),
    /// No answer about the requirements: the owner is stopping, fenced or
    /// overloaded, the request failed, or the answer is no bundle of this
    /// source. Nothing is sealed or left out after it.
    Other(ApiError),
}

/// The bundles the owner resolved in this submit, by the bundle asked for.
#[derive(Default)]
struct Asked(
    Vec<(
        crate::toolchain::contract::FrozenBundle,
        crate::toolchain::contract::FrozenBundle,
    )>,
);

impl Asked {
    /// `ask` for `bundle`, once for each bundle of this submit.
    async fn resolve<Ask, Asking>(
        &mut self,
        ask: &Ask,
        bundle: Option<crate::toolchain::contract::FrozenBundle>,
        observed: &Observed,
    ) -> Result<Option<crate::toolchain::contract::FrozenBundle>, Refusal>
    where
        Ask: Fn(crate::toolchain::contract::FrozenBundle, Observed) -> Asking,
        Asking:
            std::future::Future<Output = Result<crate::toolchain::contract::FrozenBundle, Refusal>>,
    {
        let Some(bundle) = bundle else {
            return Ok(None);
        };
        let same = |(asked, _): &&(_, _)| asked == &bundle;
        if let Some((_, answer)) = self.0.iter().find(same) {
            return Ok(Some(answer.clone()));
        }
        let answer = ask(bundle.clone(), observed.clone()).await?;
        self.0.push((bundle, answer.clone()));
        Ok(Some(answer))
    }
}

/// The aliases of `open` that are left out: those the owner refused when
/// asked for each by itself (`alone` says which were resolved).
fn left_out(
    open: Vec<crate::toolchain::versions::ToolId>,
    alone: &[bool],
) -> Vec<crate::toolchain::versions::ToolId> {
    let refused = open.into_iter().zip(alone).filter(|(_, alone)| !**alone);
    refused.map(|(tool, _)| tool).collect()
}

/// The tools of `aliases` (the setup-step aliases of the task `bundle` was
/// frozen for) that `bundle` holds no evidence for, after the owner answered
/// that the bundle could not be resolved.
fn unresolved_aliases(
    bundle: Option<&crate::toolchain::contract::FrozenBundle>,
    aliases: &[crate::toolchain::versions::ToolId],
) -> Vec<crate::toolchain::versions::ToolId> {
    let Some(bundle) = bundle else {
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
    bundle: crate::toolchain::contract::FrozenBundle,
    observed: &Observed,
) -> Result<crate::toolchain::contract::FrozenBundle, Refusal> {
    if bundle.validate(digest).is_ok() {
        return Ok(bundle);
    }
    let reply = owner
        .request(Operation::ResolveToolchains {
            bundle: bundle.clone(),
            requirements: requirements.clone(),
        })
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))
        .map_err(Refusal::Other)?;
    resolved_reply(reply, bundle, observed, digest)
}

/// What the owner answered to the request to resolve `bundle`. Only its
/// answer that the requirements could not be resolved is that refusal; an
/// owner that is stopping, or any other reply, says nothing about them.
fn resolved_reply(
    reply: Reply,
    bundle: crate::toolchain::contract::FrozenBundle,
    observed: &Observed,
    digest: &Digest,
) -> Result<crate::toolchain::contract::FrozenBundle, Refusal> {
    match reply {
        Reply::ResolvedToolchains { bundle } => match bundle.validate(digest) {
            Ok(()) => Ok(bundle),
            Err(e) => Err(Refusal::Other(ApiError::new(ErrorCode::InvalidInput, e))),
        },
        Reply::ControlError { error } => seal_after_refusal(bundle, observed, digest, error),
        _ => Err(Refusal::Other(ApiError::blocked(
            crate::error::BlockedReason::EnvironmentMismatch,
            format!(
                "toolchain resolve required ({})",
                bundle.frozen.describe_requirements()
            ),
        ))),
    }
}

/// What the owner's refusal to resolve leaves the submit with. Only the
/// answer that no registry could resolve the requirements (`blocked:
/// environment_mismatch`) lets a range the tool this host observed satisfies
/// be sealed to that version. Every other error — the owner stopping, fenced
/// or overloaded, a storage or protocol failure — says nothing about the
/// requirements, and is returned as it came, as is a refusal the observation
/// cannot answer.
fn seal_after_refusal(
    bundle: crate::toolchain::contract::FrozenBundle,
    observed: &Observed,
    digest: &Digest,
    error: ApiError,
) -> Result<crate::toolchain::contract::FrozenBundle, Refusal> {
    if error.code != ErrorCode::Blocked
        || error.blocked_reason != Some(crate::error::BlockedReason::EnvironmentMismatch)
    {
        return Err(Refusal::Other(error));
    }
    let sealed = bundle.seal_observed(observed);
    if sealed.validate(digest).is_err() {
        return Err(Refusal::Unresolved(error));
    }
    Ok(sealed)
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
        crate::toolchain::capability::Requirement::Tool(name)
        | crate::toolchain::capability::Requirement::Prepare(name) => Some(name.clone()),
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

/// The bundles of a freeze, the setup-step aliases each was frozen with, the
/// notices of what is no requirement, and what this host observed of each
/// tool.
type Bundles = (
    Vec<Option<crate::toolchain::contract::FrozenBundle>>,
    Vec<Vec<crate::toolchain::versions::ToolId>>,
    Vec<crate::ci::toolchains::Notice>,
    Observed,
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
                aliases.push(freeze.aliases(&prepared.sources[index]));
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
    Ok((bundles, aliases, notices, observations))
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

/// What the profile of this host reports for each tool at capture.
type Observed = std::collections::BTreeMap<
    crate::toolchain::versions::ToolId,
    crate::toolchain::versions::VersionObservation,
>;

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

    /// A range is sealed to the observed version only after the refusal that
    /// says the requirements could not be resolved. An error that says
    /// nothing about them leaves the bundle unsealed and comes back unchanged.
    #[test]
    fn only_the_unresolved_requirements_refusal_seals_to_the_observed_version() {
        use super::{seal_after_refusal, Refusal};
        use crate::error::{ApiError, BlockedReason, ErrorCode};
        use crate::ids::Digest;
        use crate::toolchain::{contract::FrozenBundle, versions::*};
        let source = Digest::of_bytes(b"source");
        let scope = Scope::project("");
        let node = ToolId::parse("node").unwrap();
        let bundle = FrozenBundle::seal_exact(
            FrozenToolchains::freeze(
                source.clone(),
                Digest::of_bytes(b"parent"),
                scope.clone(),
                &[Declaration {
                    tool: node.clone(),
                    constraint: Constraint::parse("20").unwrap(),
                    scope,
                    location: Location {
                        path: ".nvmrc".into(),
                        first_line: 1,
                        last_line: 1,
                    },
                }],
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            )
            .unwrap(),
        )
        .unwrap();
        assert!(bundle.validate(&source).is_err());
        let seen = |version: &str| [(node.clone(), VersionObservation::parse(version))].into();
        let unresolved = || {
            ApiError::blocked(
                BlockedReason::EnvironmentMismatch,
                "toolchain resolve required: no coherent worker proposal",
            )
        };
        let sealed =
            seal_after_refusal(bundle.clone(), &seen("20.11.1"), &source, unresolved()).unwrap();
        sealed.validate(&source).unwrap();
        assert_eq!(sealed.evidence[0].concrete.to_string(), "20.11.1");
        // The observation does not answer the refusal: it stays the refusal.
        assert_eq!(
            seal_after_refusal(bundle.clone(), &seen("18.0.0"), &source, unresolved()),
            Err(Refusal::Unresolved(unresolved()))
        );
        for transient in [
            ApiError::new(ErrorCode::CommunicationUnknown, "resident is stopping"),
            ApiError::new(ErrorCode::StaleEpoch, "fenced"),
            ApiError::new(ErrorCode::Overloaded, "send again"),
            ApiError::new(ErrorCode::Internal, "storage"),
            ApiError::blocked(BlockedReason::SecretUnavailable, "secret"),
        ] {
            assert_eq!(
                seal_after_refusal(bundle.clone(), &seen("20.11.1"), &source, transient.clone()),
                Err(Refusal::Other(transient))
            );
        }
    }
    use crate::toolchain::{profile::Profile, registry::Registry};
    use std::os::unix::fs::{symlink, DirBuilderExt};

    /// A refused bundle is first sealed from what this host observed, and
    /// only then are the aliases of setup steps that are still open left
    /// out: a range beside such an alias is not lost with it, the alias is
    /// never sealed from an observation, and an alias no setup step wrote
    /// (the project file names it) keeps the refusal.
    #[test]
    fn a_refused_bundle_is_sealed_from_the_observation_before_step_aliases_are_left_out() {
        use super::{seal_after_refusal, unresolved_aliases, Refusal};
        use crate::error::{ApiError, BlockedReason};
        use crate::ids::Digest;
        use crate::toolchain::{contract::FrozenBundle, versions::*};
        let source = Digest::of_bytes(b"source");
        let (node, rust) = (
            ToolId::parse("node").unwrap(),
            ToolId::parse("rust").unwrap(),
        );
        let bundle = |declared: &[(&ToolId, &str)]| {
            let scope = Scope::project("");
            let declarations: Vec<Declaration> = declared
                .iter()
                .map(|(tool, constraint)| Declaration {
                    tool: (*tool).clone(),
                    constraint: Constraint::parse(constraint).unwrap(),
                    scope: scope.clone(),
                    location: Location {
                        path: ".github/workflows/ci.yml".into(),
                        first_line: 1,
                        last_line: 1,
                    },
                })
                .collect();
            let frozen = FrozenToolchains::freeze(
                source.clone(),
                Digest::of_bytes(b"parent"),
                scope,
                &declarations,
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            );
            FrozenBundle::seal_exact(frozen.unwrap()).unwrap()
        };
        // Both tools are observed: only the range is answered by that.
        let seen: super::Observed = [
            (node.clone(), VersionObservation::parse("20.11.1")),
            (rust.clone(), VersionObservation::parse("1.89.0")),
        ]
        .into();
        let refusal = || ApiError::blocked(BlockedReason::EnvironmentMismatch, "resolve required");
        let mixed = bundle(&[(&node, "20"), (&rust, "stable")]);
        // The alias keeps the sealed bundle open: the refusal comes back, and
        // the alias of the step, not the range, is what is left out.
        assert_eq!(
            seal_after_refusal(mixed.clone(), &seen, &source, refusal()),
            Err(Refusal::Unresolved(refusal()))
        );
        let steps = [rust.clone()];
        assert_eq!(unresolved_aliases(Some(&mixed), &steps), steps);
        // Asked again without the alias, the range is sealed.
        let sealed =
            seal_after_refusal(bundle(&[(&node, "20")]), &seen, &source, refusal()).unwrap();
        sealed.validate(&source).unwrap();
        assert_eq!(sealed.evidence[0].concrete.to_string(), "20.11.1");
        // A range the observation is outside of is not an alias to leave out.
        let unmet = bundle(&[(&node, "18")]);
        assert_eq!(
            seal_after_refusal(unmet.clone(), &seen, &source, refusal()),
            Err(Refusal::Unresolved(refusal()))
        );
        assert!(unresolved_aliases(Some(&unmet), &[]).is_empty());
        // An alias the project file wrote is no alias of a step.
        assert!(unresolved_aliases(Some(&mixed), &[]).is_empty());
        // A task that requires nothing has no alias to leave out.
        assert!(unresolved_aliases(None, &steps).is_empty());
    }

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

    /// A bundle of the step aliases `declared` (tool and alias), unsealed.
    fn alias_bundle(declared: &[(&str, &str)]) -> crate::toolchain::contract::FrozenBundle {
        use crate::toolchain::{contract::FrozenBundle, versions::*};
        let source = crate::ids::Digest::of_bytes(b"source");
        let scope = Scope::project("");
        let declarations: Vec<Declaration> = declared
            .iter()
            .map(|(tool, constraint)| Declaration {
                tool: ToolId::parse(tool).unwrap(),
                constraint: Constraint::parse(constraint).unwrap(),
                scope: scope.clone(),
                location: Location {
                    path: ".github/workflows/ci.yml".into(),
                    first_line: 1,
                    last_line: 1,
                },
            })
            .collect();
        let frozen = FrozenToolchains::freeze(
            source.clone(),
            crate::ids::Digest::of_bytes(b"parent"),
            scope,
            &declarations,
            &[],
            &Default::default(),
            MatchPolicy::Exact,
        );
        FrozenBundle::seal_exact(frozen.unwrap()).unwrap()
    }

    fn refusal() -> crate::error::ApiError {
        crate::error::ApiError::blocked(
            crate::error::BlockedReason::EnvironmentMismatch,
            "toolchain resolve required: no coherent worker proposal",
        )
    }

    /// What a task was resolved to: its requirements, or the refusal.
    type Described = Result<Option<String>, crate::error::ApiError>;

    /// What the owner says of a bundle that holds exactly these tools.
    type Verdict<'a> = dyn Fn(&[&str]) -> Option<super::Refusal> + 'a;

    /// [`super::resolve_each`] for two tasks: the first has the setup-step
    /// aliases `steps`, the second requires nothing. The owner answers by
    /// `verdict`, and a freeze with something left out fails with `unfrozen`.
    /// The result (the requirements of each task and the tools told as left
    /// out), and what the owner was asked, in order.
    #[allow(clippy::type_complexity)]
    fn resolve_steps(
        steps: &[(&str, &str)],
        verdict: &Verdict<'_>,
        unfrozen: Option<crate::error::ApiError>,
    ) -> (
        Result<(Vec<Described>, Vec<String>), crate::error::ApiError>,
        Vec<String>,
    ) {
        use crate::toolchain::versions::ToolId;
        let asks = std::cell::RefCell::new(Vec::new());
        let freeze = |dropped: Vec<Vec<ToolId>>| {
            let unfrozen = unfrozen.clone();
            async move {
                assert_eq!(dropped.len(), 2);
                assert!(dropped[1].is_empty());
                if let Some(error) = unfrozen.filter(|_| !dropped[0].is_empty()) {
                    return Err(error);
                }
                let left = |name: &str| dropped[0].iter().any(|tool| tool.as_str() == name);
                let kept: Vec<(&str, &str)> = steps
                    .iter()
                    .filter(|(tool, _)| !left(tool))
                    .cloned()
                    .collect();
                let aliases = kept.iter().map(|(tool, _)| ToolId::parse(tool).unwrap());
                let notices = dropped[0].iter().map(|tool| crate::ci::toolchains::Notice {
                    task: "build".into(),
                    job: Some("build".into()),
                    at: "ci.yml:1".into(),
                    text: tool.as_str().into(),
                });
                let bundle = (!kept.is_empty()).then(|| alias_bundle(&kept));
                let bundles: super::Bundles = (
                    vec![bundle, None],
                    vec![aliases.collect(), vec![]],
                    notices.collect(),
                    Default::default(),
                );
                Ok(bundles)
            }
        };
        let ask = |bundle: crate::toolchain::contract::FrozenBundle, _: super::Observed| {
            let asked = bundle.frozen.describe_requirements();
            asks.borrow_mut().push(asked);
            let tools: Vec<&str> = bundle
                .frozen
                .requirements
                .iter()
                .map(|r| r.tool.as_str())
                .collect();
            let answer = match verdict(&tools) {
                Some(refused) => Err(refused),
                None => Ok(bundle.clone()),
            };
            async move { answer }
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = runtime.block_on(super::resolve_each(2, freeze, ask));
        let result = result.map(|(resolved, notices)| {
            let described = |selection: &super::Selection| {
                selection.clone().map(|bundle| {
                    bundle
                        .as_ref()
                        .map(|bundle| bundle.frozen.describe_requirements())
                })
            };
            (
                resolved.iter().map(described).collect(),
                notices.into_iter().map(|notice| notice.text).collect(),
            )
        });
        (result, asks.into_inner())
    }

    /// Of several setup-step aliases in one refused bundle, only those the
    /// owner refuses by themselves are left out: the one it resolves stays a
    /// requirement of the task. A bundle the owner already resolved is not
    /// asked for again in the next pass.
    #[test]
    fn only_the_aliases_refused_alone_are_left_out_and_no_bundle_is_asked_twice() {
        use super::Refusal;
        let strings = |all: &[&str]| -> Vec<String> { all.iter().map(|s| s.to_string()).collect() };
        let no_rust: &Verdict<'_> = &|tools| {
            tools
                .contains(&"rust")
                .then(|| Refusal::Unresolved(refusal()))
        };
        let both = [("node", "lts/*"), ("rust", "stable")];
        let (result, asks) = resolve_steps(&both, no_rust, None);
        assert_eq!(
            result,
            Ok((
                vec![Ok(Some("node=lts/*".to_string())), Ok(None)],
                strings(&["rust"])
            ))
        );
        // The bundle, then each alias without the other; the pass after that
        // freezes to the bundle of the first probe and asks nothing.
        assert_eq!(
            asks,
            ["node=lts/*, rust=stable", "node=lts/*", "rust=stable"]
        );
        // One alias: it is left out with no question about it alone.
        let (result, asks) = resolve_steps(&[("rust", "stable")], no_rust, None);
        assert_eq!(result, Ok((vec![Ok(None), Ok(None)], strings(&["rust"]))));
        assert_eq!(asks, ["rust=stable"]);
        // Nothing resolves either alias: both are left out.
        let nothing: &Verdict<'_> = &|_| Some(Refusal::Unresolved(refusal()));
        let (result, asks) = resolve_steps(&both, nothing, None);
        assert_eq!(
            result,
            Ok((vec![Ok(None), Ok(None)], strings(&["node", "rust"])))
        );
        assert_eq!(asks.len(), 3);
        // Each resolves alone and the two do not together: nothing can be
        // left out, and the refusal stands for that task alone, with what it
        // requires. The other task is resolved as before (spec 22.1).
        let apart: &Verdict<'_> =
            &|tools| (tools.len() > 1).then(|| Refusal::Unresolved(refusal()));
        let (result, asks) = resolve_steps(&both, apart, None);
        let mut refused = refusal();
        refused.message = format!("{} (node=lts/*, rust=stable)", refused.message);
        assert_eq!(result, Ok((vec![Err(refused), Ok(None)], vec![])));
        assert_eq!(asks.len(), 3);
        // Three aliases, one of them refused alone: the other two stay, and
        // are asked for together once.
        let three = [("go", "stable"), ("node", "lts/*"), ("rust", "stable")];
        let (result, asks) = resolve_steps(&three, no_rust, None);
        assert_eq!(
            result,
            Ok((
                vec![Ok(Some("go=stable, node=lts/*".to_string())), Ok(None)],
                strings(&["rust"])
            ))
        );
        assert_eq!(
            asks,
            [
                "go=stable, node=lts/*, rust=stable",
                "go=stable",
                "node=lts/*",
                "rust=stable",
                "go=stable, node=lts/*"
            ]
        );
    }

    /// Only the answer that the requirements could not be resolved leaves an
    /// alias out. An owner that did not answer the question is the error of
    /// the submit, for the bundle and for an alias asked for alone; so is a
    /// freeze that fails once an alias is left out, in place of the refusal
    /// that came before it.
    #[test]
    fn no_alias_is_left_out_without_the_refusal_and_a_failed_freeze_is_its_own_error() {
        use super::Refusal;
        use crate::error::{ApiError, BlockedReason, ErrorCode};
        let both = [("node", "lts/*"), ("rust", "stable")];
        // The words of a refusal, from an owner that is stopping.
        let stopping = || {
            ApiError::blocked(
                BlockedReason::EnvironmentMismatch,
                "toolchain resolve required",
            )
        };
        let unanswered: &Verdict<'_> = &|_| Some(Refusal::Other(stopping()));
        let (result, asks) = resolve_steps(&both, unanswered, None);
        assert_eq!(result, Err(stopping()));
        assert_eq!(asks.len(), 1);
        let lost = || ApiError::new(ErrorCode::CommunicationUnknown, "resident is stopping");
        let alone: &Verdict<'_> = &|tools| {
            Some(match tools.len() {
                1 => Refusal::Other(lost()),
                _ => Refusal::Unresolved(refusal()),
            })
        };
        let (result, asks) = resolve_steps(&both, alone, None);
        assert_eq!(result, Err(lost()));
        assert_eq!(asks.len(), 2);
        // The freeze without the alias contradicts itself: that is told, not
        // the refusal of the alias.
        let conflict =
            || ApiError::blocked(BlockedReason::EnvironmentMismatch, "conflicting_versions");
        let nothing: &Verdict<'_> = &|_| Some(Refusal::Unresolved(refusal()));
        for steps in [&both[..], &both[1..]] {
            let (result, _) = resolve_steps(steps, nothing, Some(conflict()));
            assert_eq!(result, Err(conflict()));
        }
    }

    /// What the owner's reply to the resolve request is to the submit: the
    /// resolved bundle, the refusal that nothing resolves the requirements,
    /// or no answer about them.
    #[test]
    fn only_the_owner_saying_so_is_the_refusal_that_nothing_resolves_the_bundle() {
        use super::{resolved_reply, Refusal, Reply};
        use crate::error::{ApiError, BlockedReason, ErrorCode};
        let source = crate::ids::Digest::of_bytes(b"source");
        let open = alias_bundle(&[("rust", "stable")]);
        let pinned = alias_bundle(&[("rust", "1.89.0")]);
        pinned.validate(&source).unwrap();
        let seen = super::Observed::new();
        let reply = |reply: Reply| resolved_reply(reply, open.clone(), &seen, &source);
        assert_eq!(
            reply(Reply::ResolvedToolchains {
                bundle: pinned.clone()
            }),
            Ok(pinned)
        );
        // A bundle that is still open is no answer.
        let unresolved = reply(Reply::ResolvedToolchains {
            bundle: open.clone(),
        });
        assert!(
            matches!(&unresolved, Err(Refusal::Other(e)) if e.code == ErrorCode::InvalidInput),
            "{unresolved:?}"
        );
        assert_eq!(
            reply(Reply::ControlError { error: refusal() }),
            Err(Refusal::Unresolved(refusal()))
        );
        let internal = ApiError::new(ErrorCode::Internal, "storage");
        assert_eq!(
            reply(Reply::ControlError {
                error: internal.clone()
            }),
            Err(Refusal::Other(internal))
        );
        // The owner is stopping: the same words as a refusal, and no answer.
        assert_eq!(
            reply(Reply::Stopping),
            Err(Refusal::Other(ApiError::blocked(
                BlockedReason::EnvironmentMismatch,
                "toolchain resolve required (rust=stable)"
            )))
        );
    }

    /// A run in which some task can be placed goes on without those that
    /// cannot; a run in which none can is refused as a whole, with the
    /// refusal of its first task (spec 22.1).
    #[test]
    fn only_a_run_of_which_no_task_can_be_placed_is_refused_as_a_whole() {
        use super::all_unplaceable;
        use crate::error::{ApiError, BlockedReason};
        let refused = |n: &str| ApiError::blocked(BlockedReason::EnvironmentMismatch, n);
        let some = vec![Err(refused("first")), Ok(None), Err(refused("third"))];
        assert_eq!(all_unplaceable(some.clone()), Ok(some));
        let none = vec![Err(refused("first")), Err(refused("second"))];
        assert_eq!(all_unplaceable(none), Err(refused("first")));
        assert_eq!(all_unplaceable(vec![Ok(None)]), Ok(vec![Ok(None)]));
    }

    #[test]
    fn the_aliases_left_out_are_those_not_resolved_alone() {
        use super::left_out;
        let tool = |name: &str| crate::toolchain::versions::ToolId::parse(name).unwrap();
        let open = || vec![tool("go"), tool("node"), tool("rust")];
        assert_eq!(left_out(open(), &[true, false, true]), [tool("node")]);
        assert_eq!(left_out(open(), &[false, false, false]), open());
        assert_eq!(left_out(open(), &[true, true, true]), []);
        assert_eq!(left_out(vec![tool("rust")], &[false]), [tool("rust")]);
    }
}
