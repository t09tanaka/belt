//! Read-only preview: never starts an owner or captures execution inputs.
use crate::{
    error::{ApiError, ErrorCode},
    scheduler::decide::Decision,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub version: u32,
    pub argv: Vec<String>,
    pub repository_id: Option<String>,
    pub cwd_relative: String,
    pub requirements: crate::toolchain::capability::ExecutionRequirements,
}
impl Query {
    pub fn check(&self) -> Result<(), ApiError> {
        if self.version != 1
            || self.argv.is_empty()
            || self.argv.len() > 256
            || self.argv.iter().map(String::len).sum::<usize>() > 65536
            || self.argv.iter().any(|s| s.contains('\0'))
            || self.repository_id.as_ref().is_some_and(|s| s.len() > 4096)
        {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "invalid estimate command",
            ));
        }
        crate::workspace::plan::check_cwd_relative(std::path::Path::new(&self.cwd_relative))?;
        self.requirements
            .validate()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Estimate {
    pub version: u32,
    pub online: bool,
    pub source_kind: String,
    pub conditional: bool,
    pub decision: Option<Decision>,
    pub forecasts: Vec<crate::wire::ForecastResponse>,
    pub assumptions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<WorkerCostEvidence>,
}
pub type WorkerCostEvidence = crate::scheduler::measured::WorkerCostEvidence;
impl Estimate {
    pub fn offline(source_kind: String) -> Self {
        Self {
            version: 1,
            online: false,
            source_kind,
            conditional: true,
            decision: None,
            forecasts: vec![],
            components: vec![],
            assumptions: vec![
                "owner unavailable; runtime, queue, transfer, setup and collection are unknown"
                    .into(),
            ],
        }
    }
    pub fn human(&self) -> String {
        let mut text = format!(
            "estimate: {} (conditional)\n",
            if self.online {
                "online"
            } else {
                "offline; unknown"
            }
        );
        if let Some(decision) = &self.decision {
            text.push_str(&format!("{}\n", decision.summary()));
            for worker in &decision.compared {
                let time = |value: Option<u64>| {
                    value
                        .map(|ms| format!("{ms} ms"))
                        .unwrap_or_else(|| "unknown".into())
                };
                let label = match worker.label {
                    crate::scheduler::decide::Label::Measured => "measured",
                    crate::scheduler::decide::Label::Fallback => "fallback (ranking only)",
                    crate::scheduler::decide::Label::Unknown => "unknown",
                };
                text.push_str(&format!(
                    "{}: {}; execution {}; queue {}; conditional={}\n",
                    worker.worker_id,
                    label,
                    time(worker.outlook.execution_ms),
                    time(worker.outlook.wait_ms),
                    worker.outlook.conditional
                ));
            }
        }
        for note in &self.assumptions {
            text.push_str(&format!("{note}\n"));
        }
        text
    }
}
/// Mirror local-ci source identity lookup without capture or object-store creation.
fn metadata(context: &super::context::Context) -> (Option<String>, String, String) {
    let Some(root) = &context.repo_root else {
        let directory = std::process::Command::new("git")
            .args(["rev-parse", "--show-toplevel"])
            .current_dir(&context.cwd)
            .env_clear()
            .envs(&context.env)
            .env("LC_ALL", "C")
            .output()
            .ok()
            .is_some_and(|o| {
                String::from_utf8_lossy(&o.stderr)
                    .starts_with("fatal: not a git repository (or any of the parent directories)")
            });
        return (
            None,
            String::new(),
            if directory { "directory" } else { "unknown" }.into(),
        );
    };
    let ctx = local_ci::ExecContext::new(context.env.clone());
    let id = local_ci::git::repo_slug(&ctx, root)
        .map(|slug| format!("github:{slug}"))
        .or_else(|| {
            let out = std::process::Command::new("git")
                .args(["rev-list", "--max-parents=0", "HEAD"])
                .current_dir(root)
                .env_clear()
                .envs(&context.env)
                .output()
                .ok()?;
            if !out.status.success() {
                return None;
            }
            let text = String::from_utf8(out.stdout).ok()?;
            let first = text.lines().min()?;
            crate::workspace::plan::is_commit_id(first).then(|| format!("root:{first}"))
        });
    let cwd = context
        .cwd
        .strip_prefix(root)
        .ok()
        .and_then(|p| p.to_str())
        .unwrap_or("")
        .to_string();
    (id, cwd, "git".into())
}
pub async fn command(
    context: super::context::Context,
    argv: Vec<String>,
) -> Result<Estimate, ApiError> {
    let (repository_id, cwd_relative, source_kind) = metadata(&context);
    let query = Query {
        version: 1,
        argv,
        repository_id,
        cwd_relative,
        requirements: crate::toolchain::capability::ExecutionRequirements::parse(
            context.config.profile.value.clone(),
            &context.config.requires.value,
        )
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?,
    };
    query.check()?;
    let store = super::bootstrap::read_only_store(&context.bootstrap)
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
    let client = super::resident::Client::load(context.state_dir(), store.cluster_id().clone());
    drop(store);
    if let Ok(client) = client {
        match client
            .request(super::resident::Operation::Estimate { query })
            .await
        {
            Ok(super::resident::Reply::Estimate { mut estimate }) => {
                estimate.source_kind = source_kind;
                return Ok(estimate);
            }
            Ok(super::resident::Reply::ControlError { error }) => return Err(error),
            _ => {}
        }
    }
    Ok(Estimate::offline(source_kind))
}
/// Exact private input, deliberately never exposed through public metadata queries.
pub(crate) fn frozen_input(
    store: &crate::storage::CoordinatorStore,
    task_id: &crate::ids::TaskId,
) -> Result<crate::storage::QueuedTask, ApiError> {
    let input = store
        .estimate_input(task_id)
        .map_err(|e| ApiError::new(e.code(), e.to_string()))?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, format!("task {task_id} not found")))?;
    let spec: crate::coordinator::TaskSpec = serde_json::from_value(input.spec.clone())
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, format!("stored task spec: {e}")))?;
    spec.requirements
        .validate()
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?;
    if let Some(bundle) = &spec.toolchains {
        bundle
            .validate(&spec.manifest_digest)
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e))?;
    }
    Ok(input)
}

/// No checkout discovery, project config, capture, owner startup or source-store opening.
pub async fn task(
    bootstrap: super::bootstrap::Spec,
    task_id: crate::ids::TaskId,
) -> Result<Estimate, ApiError> {
    let store = super::bootstrap::read_only_store(&bootstrap)
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
    frozen_input(&store, &task_id)?;
    let client = super::resident::Client::load(&bootstrap.state_dir, store.cluster_id().clone());
    drop(store);
    if let Ok(client) = client {
        match client
            .request(super::resident::Operation::EstimateTask { task_id })
            .await
        {
            Ok(super::resident::Reply::Estimate { estimate }) => return Ok(estimate),
            Ok(super::resident::Reply::ControlError { error }) => return Err(error),
            _ => {}
        }
    }
    let mut estimate = Estimate::offline("captured".into());
    estimate.assumptions.push("hypothetical rerun using stored task inputs; not remaining runtime; captured source kind is unknown".into());
    estimate
        .assumptions
        .push("stored specifications do not freeze mutable in-place workspace files".into());
    Ok(estimate)
}

pub fn cli(args: &crate::cli::EstimateArgs, json: bool) -> i32 {
    let result = (|| {
        let env = std::env::vars().collect();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        if let Some(id) = &args.task {
            let id = crate::ids::TaskId::parse(id)
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
            let config = crate::config::paths::user_config_path(&env)
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
            let input = config
                .parent()
                .expect("config parent")
                .join("coordinator.json");
            let bootstrap = super::bootstrap::read_input(&input)
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
            return runtime.block_on(task(bootstrap, id));
        }
        let cwd = std::env::current_dir()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        let context = args
            .execution
            .apply(super::context::Context::discover_estimate(env, cwd)?);
        runtime.block_on(command(context, args.command.clone()))
    })();
    match result {
        Ok(estimate) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":true,"result":estimate})
                );
            } else {
                print!("{}", estimate.human());
            }
            0
        }
        Err(error) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":error})
                );
            } else {
                eprintln!("{}", error.message);
            }
            error.code.exit_code()
        }
    }
}
