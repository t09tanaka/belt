//! Command-line definition and dispatch.

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

pub use crate::error::exit;
use crate::error::ErrorCode;

/// Version of the `--json` envelope.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Parser)]
#[command(name = "remote-task-runner", version, about)]
pub struct Cli {
    /// Print a structured result on stdout (progress and diagnostics go to stderr).
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Register the root-owned service definition for a non-root worker.
    RegisterSystemWorker {
        #[arg(long)]
        account: String,
        #[arg(long)]
        config_dir: std::path::PathBuf,
    },
    /// Run one admitted private tool installer (internal).
    #[command(hide = true)]
    ToolchainInstall {
        #[arg(long)]
        request: std::path::PathBuf,
        #[arg(long)]
        binding: String,
    },
    /// Run the parent owner process (internal).
    #[command(hide = true)]
    Daemon(DaemonArgs),
    /// Configure the coordinator and the local worker.
    Init,
    /// Run CI for the project.
    Ci(CiArgs),
    /// Run one command and wait for its result.
    Exec(ExecArgs),
    /// Submit a command without waiting.
    Submit(SubmitArgs),
    /// List runs and tasks.
    Ls(ListArgs),
    /// Show one run or task.
    Show(ShowArgs),
    /// Show logs.
    Logs(LogsArgs),
    /// Wait for completion.
    Wait(IdArgs),
    /// Request cancellation.
    Cancel(IdArgs),
    /// Retry a terminal attempt.
    Retry(IdArgs),
    /// Fetch outputs.
    Outputs(OutputsArgs),
    /// Preview placement if a command or a captured task were run again.
    Estimate(EstimateArgs),
    /// Check the setup.
    Doctor,
    /// Manage workers.
    Workers(WorkersArgs),
    /// Run as a worker (internal).
    #[command(hide = true)]
    Worker(WorkerArgs),
    /// Install or control the worker service on this host (internal; run over ssh).
    #[command(hide = true)]
    WorkerService(WorkerServiceArgs),
    /// Supervise one attempt as its own process (internal).
    #[command(hide = true)]
    Supervise(SuperviseArgs),
}

#[derive(Debug, Args)]
pub struct DaemonArgs {
    /// Retained private service bootstrap input; absent means bounded stdin.
    #[arg(long)]
    pub input: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct SuperviseArgs {
    /// JSON spec file written by the agent.
    #[arg(long)]
    pub spec: std::path::PathBuf,
}

#[derive(Debug, Args)]
pub struct CiArgs {
    #[command(subcommand)]
    pub sub: Option<CiSub>,
    /// Snapshot kind (for example `commit`).
    #[arg(long)]
    pub snapshot: Option<String>,
    /// Git ref to run.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum CiSub {
    /// Print the plan without running.
    Plan,
}

#[derive(Debug, Args)]
pub struct ExecArgs {
    #[command(flatten)]
    pub execution: ExecutionArgs,
    /// Run in the current directory instead of a snapshot.
    #[arg(long)]
    pub in_place: bool,
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct SubmitArgs {
    #[command(flatten)]
    pub execution: ExecutionArgs,
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

/// A task preview reuses its stored inputs; it does not predict remaining runtime.
#[derive(Debug, Args)]
pub struct EstimateArgs {
    #[command(flatten)]
    pub execution: ExecutionArgs,
    /// Task whose frozen commands, environment, setup and output plan would run again.
    /// Does not capture the current checkout or apply current project settings.
    /// This previews a hypothetical rerun, not remaining runtime.
    #[arg(long, conflicts_with_all = ["profile", "requires", "command"])]
    pub task: Option<String>,
    #[arg(last = true, required_unless_present = "task")]
    pub command: Vec<String>,
}

/// Placement selectors never change the worker's own active environment.
#[derive(Debug, Args)]
pub struct ExecutionArgs {
    #[arg(long)]
    pub profile: Option<String>,
    #[arg(long, value_delimiter = ',')]
    pub requires: Vec<String>,
}
impl ExecutionArgs {
    pub(crate) fn apply(
        &self,
        mut context: crate::service::context::Context,
    ) -> crate::service::context::Context {
        if let Some(profile) = &self.profile {
            context.config.profile = crate::config::Value {
                value: Some(profile.clone()),
                origin: crate::config::Origin::Cli,
            };
        }
        if !self.requires.is_empty() {
            context.config.requires = crate::config::Value {
                value: self.requires.clone(),
                origin: crate::config::Origin::Cli,
            };
        }
        context
    }
}

/// Metadata listing, ordered by canonical identifier. Cursors are exclusive.
#[derive(Debug, Args)]
pub struct ListArgs {
    #[arg(long, default_value_t = 32)]
    pub limit: u32,
    #[arg(long)]
    pub cursor: Option<String>,
}
#[derive(Debug, Args)]
pub struct ShowArgs {
    pub id: String,
    #[arg(long, default_value_t = 32)]
    pub limit: u32,
    #[arg(long)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub struct IdArgs {
    pub id: String,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    pub id: String,
    /// Keep streaming.
    #[arg(long)]
    pub follow: bool,
    /// Read a specific historical attempt belonging to the task.
    #[arg(long)]
    pub attempt: Option<String>,
}

#[derive(Debug, Args)]
pub struct OutputsArgs {
    pub id: String,
    /// Read an exact historical attempt belonging to the task.
    #[arg(long)]
    pub attempt: Option<String>,
    /// Destination directory.
    #[arg(long)]
    pub dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct WorkersArgs {
    #[command(subcommand)]
    pub sub: Option<WorkersSub>,
}

#[derive(Debug, Subcommand)]
pub enum WorkersSub {
    /// Register a worker.
    Add {
        name: String,
        /// SSH destination, for example `user@host`.
        #[arg(long)]
        ssh: Option<String>,
        /// Install the worker over SSH.
        #[arg(long)]
        install: bool,
        /// With `--install`: the build to send, for a worker whose OS or
        /// architecture differs from this machine's.
        #[arg(long, requires = "install", value_name = "PATH")]
        binary: Option<std::path::PathBuf>,
        /// Register an ephemeral worker: started on its own host with
        /// `worker --ephemeral`, reached over SSH, never installed.
        #[arg(long, conflicts_with_all = ["install", "binary"])]
        ephemeral: bool,
    },
    /// Start a worker.
    Start { name: String },
    /// Stop new placement; finish accepted work.
    Drain { name: String },
    /// Resume placement.
    Resume { name: String },
    /// Stop a worker.
    Stop {
        name: String,
        /// Stop immediately and cancel running work.
        #[arg(long)]
        cancel_running: bool,
    },
    /// Upgrade a worker.
    Upgrade {
        name: String,
        /// The build to send, for a worker whose OS or architecture differs
        /// from this machine's.
        #[arg(long, value_name = "PATH")]
        binary: Option<std::path::PathBuf>,
    },
    /// Show differences from the project requirements.
    Diff {
        /// A registered worker; every registered worker when omitted.
        name: Option<String>,
    },
    /// Sync a worker with the project requirements.
    Sync {
        name: String,
        /// Project to sync.
        #[arg(long)]
        project: Option<String>,
    },
    /// Declare an ephemeral worker gone: its attempts end as interrupted.
    Forget { name: String },
}

#[derive(Debug, Args)]
pub struct WorkerServiceArgs {
    #[arg(value_enum)]
    pub action: crate::service::worker_service::Action,
    /// With `stop`: cancel running work instead of waiting for it.
    #[arg(long)]
    pub cancel_running: bool,
    /// With `stop`: seconds to wait for accepted work to finish.
    #[arg(long, default_value_t = 600)]
    pub wait_secs: u64,
}

#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// Start from the administrator-registered, version-independent definition.
    #[arg(long, requires = "input")]
    pub system_service: bool,
    /// Speak the protocol over stdin/stdout.
    #[arg(long)]
    pub stdio: bool,
    /// Retained private input for a foreground persistent worker.
    #[arg(long, conflicts_with_all = ["stdio", "ephemeral"])]
    pub input: Option<std::path::PathBuf>,
    /// A worker that exists only while this process runs: no service is
    /// installed, it starts clean with a fresh identity and removes its state
    /// on exit. Needs `--cluster`, `--name` and `--key-file`.
    #[arg(long, conflicts_with = "stdio", requires_all = ["cluster", "name", "key_file"])]
    pub ephemeral: bool,
    /// With `--ephemeral`: the cluster the worker belongs to.
    #[arg(long, requires = "ephemeral")]
    pub cluster: Option<String>,
    /// With `--ephemeral`: the worker name the parent registered.
    #[arg(long, requires = "ephemeral")]
    pub name: Option<String>,
    /// With `--ephemeral`: the cluster key file (absolute path, owner-only).
    #[arg(long, requires = "ephemeral")]
    pub key_file: Option<std::path::PathBuf>,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
}

/// The `--json` envelope written to stdout.
#[derive(Debug, Serialize)]
pub struct Envelope {
    pub schema_version: u32,
    pub ok: bool,
    pub error: ErrorBody,
}

impl Envelope {
    pub fn not_implemented() -> Self {
        Envelope {
            schema_version: SCHEMA_VERSION,
            ok: false,
            error: ErrorBody {
                code: ErrorCode::NotImplemented,
                message: "this command is not implemented yet".to_string(),
            },
        }
    }
}

/// Run a parsed command and return the process exit code.
pub fn run(cli: Cli) -> i32 {
    if let Command::ToolchainInstall { request, binding } = &cli.command {
        return crate::toolchain::preparation::helper_main(request, binding);
    }
    if matches!(cli.command, Command::Init) {
        return match crate::service::init::command() {
            Ok(receipt) => {
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"schema_version":1,"ok":true,"result":receipt})
                    );
                } else {
                    println!("initialized {} {}", receipt.cluster_id, receipt.worker_id);
                }
                exit::SUCCESS
            }
            Err(error) => {
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"schema_version":1,"ok":false,"error":error})
                    );
                } else {
                    eprintln!("error: {}", error.message);
                }
                error.code.exit_code()
            }
        };
    }

    if matches!(&cli.command, Command::Ci(args) if matches!(args.sub, Some(CiSub::Plan))) {
        return run_ci_plan(&cli);
    }
    if matches!(&cli.command, Command::Ci(_)) {
        return run_ci(&cli);
    }

    if matches!(&cli.command, Command::Exec(_))
        || matches!(
            &cli.command,
            Command::Wait(_) | Command::Cancel(_) | Command::Retry(_)
        )
    {
        return run_control(&cli);
    }

    if matches!(cli.command, Command::Doctor) {
        return crate::service::doctor::command(cli.json);
    }
    if let Command::Estimate(args) = &cli.command {
        return crate::service::estimate::cli(args, cli.json);
    }
    if let Command::Outputs(args) = &cli.command {
        return crate::service::outputs::command(args, cli.json);
    }
    if let Command::Logs(args) = &cli.command {
        return crate::service::logs::command(args, cli.json);
    }

    if matches!(cli.command, Command::Ls(_) | Command::Show(_)) {
        return run_query(&cli);
    }

    if let Command::Daemon(args) = &cli.command {
        return crate::service::bootstrap::daemon_main(args.input.as_deref());
    }
    if let Command::RegisterSystemWorker {
        account,
        config_dir,
    } = &cli.command
    {
        return crate::service::worker_service::system_user::command(account, config_dir, cli.json);
    }
    if let Command::Worker(args) = &cli.command {
        if let Some(input) = &args.input {
            if args.system_service {
                return match crate::service::worker_service::system_user::start(input) {
                    Ok(()) => 0,
                    Err(error) => {
                        eprintln!("{error:#}");
                        1
                    }
                };
            }
            return crate::service::worker::main(input);
        }
        if let (true, Some(cluster), Some(name), Some(key_file)) =
            (args.ephemeral, &args.cluster, &args.name, &args.key_file)
        {
            return crate::service::worker::ephemeral::main(
                &crate::service::worker::ephemeral::Args {
                    cluster: cluster.clone(),
                    name: name.clone(),
                    key_file: key_file.clone(),
                },
            );
        }
        if args.stdio && !args.ephemeral {
            return run_worker_stdio();
        }
    }
    if let Command::Supervise(args) = &cli.command {
        return crate::supervisor::process::main(&args.spec);
    }
    if let Command::WorkerService(args) = &cli.command {
        return crate::service::worker_service::main(
            args.action,
            args.cancel_running,
            args.wait_secs,
        );
    }
    if matches!(&cli.command, Command::Workers(WorkersArgs { sub: None })) {
        return crate::service::workers::list_command(cli.json);
    }
    if let Command::Workers(WorkersArgs { sub: Some(sub) }) = &cli.command {
        if let Some(code) = crate::service::workers::command(sub, cli.json) {
            return code;
        }
    }
    if let Command::Submit(args) = &cli.command {
        let result = (|| -> Result<crate::service::submit::Receipt, crate::error::ApiError> {
            let env = std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                .collect();
            let cwd = std::env::current_dir()
                .map_err(|e| crate::error::ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
            let context = args
                .execution
                .apply(crate::service::context::Context::discover(env, cwd)?);
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| crate::error::ApiError::new(ErrorCode::Internal, e.to_string()))?;
            runtime.block_on(crate::service::submit::command(
                context,
                args.command.clone(),
                None,
            ))
        })();
        return match result {
            Ok(receipt) => {
                for warning in &receipt.warnings {
                    eprintln!("warning: {warning}");
                }
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"schema_version":1,"ok":true,"result":receipt})
                    );
                } else {
                    println!("{} {}", receipt.run_id, receipt.task_id);
                }
                exit::SUCCESS
            }
            Err(error) => {
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"schema_version":1,"ok":false,"error":error})
                    );
                } else {
                    eprintln!("error: {}", error.message);
                }
                error.code.exit_code()
            }
        };
    }
    let envelope = Envelope::not_implemented();
    if cli.json {
        let line = serde_json::to_string(&envelope).expect("envelope serializes");
        println!("{line}");
    } else {
        eprintln!("error: {}", envelope.error.message);
    }
    ErrorCode::NotImplemented.exit_code()
}

/// The context `ci` and `ci plan` capture and plan in: the snapshot kind and
/// revision of the command line over the configuration.
fn ci_context(args: &CiArgs) -> Result<crate::service::context::Context, crate::error::ApiError> {
    use crate::{
        config::{model::SourceMode, Overlay},
        service::context::{runtime_spec, Context},
    };
    let invalid = |message| crate::error::ApiError::new(ErrorCode::InvalidInput, message);
    let source_mode = match args.snapshot.as_deref() {
        None => None,
        Some("worktree") => Some(SourceMode::Worktree),
        Some("commit") => Some(SourceMode::Commit),
        Some(_) => return Err(invalid("unsupported snapshot kind")),
    };
    if source_mode == Some(SourceMode::Worktree) && args.git_ref.is_some() {
        return Err(invalid("worktree snapshot does not accept a revision"));
    }
    let env = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let cwd = std::env::current_dir().map_err(|_| invalid("command directory is unavailable"))?;
    let spec = runtime_spec(&env)
        .map_err(|_| invalid("coordinator configuration is unavailable; initialize first"))?;
    let overlay = Overlay {
        source_mode,
        ..Default::default()
    };
    Context::load_with_overlay(env, cwd, spec, &overlay)
        .map_err(|_| invalid("planning configuration is invalid or unavailable"))
}

/// `ci`: run the frozen plan of the captured input as one run and wait for
/// it. The exit code is the run's, as with `exec`.
fn run_ci(cli: &Cli) -> i32 {
    let result = (|| -> Result<crate::service::ci_run::CiReport, crate::error::ApiError> {
        let Command::Ci(args) = &cli.command else {
            unreachable!()
        };
        let context = ci_context(args)?;
        let internal =
            |e: std::io::Error| crate::error::ApiError::new(ErrorCode::Internal, e.to_string());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(internal)?;
        let result = runtime.block_on(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .map_err(internal)?;
            let watcher = tokio::spawn(async move {
                while interrupt.recv().await.is_some() {
                    if sender.send(()).is_err() {
                        break;
                    }
                }
            });
            let result =
                crate::service::ci_run::run(context, args.git_ref.clone(), &mut receiver).await;
            watcher.abort();
            let _ = watcher.await;
            result
        });
        runtime.shutdown_background();
        result
    })();
    match result {
        Ok(ci) => {
            let code = ci.report.cli_exit_code;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":code==0,"result":ci})
                );
            } else {
                print!("{}", crate::service::ci_run::render_jobs(&ci.jobs));
                let crate::service::completion::Target::Run(run) = &ci.report.target else {
                    unreachable!("ci waits for a run")
                };
                println!("{run} {}", ci.report.state);
                if ci.report.collection_pending == Some(true) {
                    eprintln!("collection is still pending");
                }
                if let Some(error) = &ci.report.error {
                    eprintln!("error: {}", error.message);
                }
            }
            code
        }
        Err(error) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":error})
                );
            } else {
                eprintln!("error: {}", error.message);
            }
            error.code.exit_code()
        }
    }
}

fn run_ci_plan(cli: &Cli) -> i32 {
    let result = (|| -> Result<_, crate::error::ApiError> {
        let Command::Ci(args) = &cli.command else {
            unreachable!()
        };
        let context = ci_context(args)?;
        let prepared = crate::service::planning::prepare(context, args.git_ref.as_deref())?;
        let summary = prepared.plan().summary().clone();
        prepared.close()?;
        Ok(summary)
    })();
    match result {
        Ok(summary) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":true,"result":summary})
                );
            } else {
                println!("{}", crate::ci::summary::render_text(&summary));
            }
            exit::SUCCESS
        }
        Err(error) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":{"code":error.code,"message":error.message}})
                );
            } else {
                eprintln!("{}", error.message);
            }
            error.code.exit_code()
        }
    }
}

fn run_control(cli: &Cli) -> i32 {
    let result = (|| -> Result<crate::service::control::Report, crate::error::ApiError> {
        if let Command::Wait(args) | Command::Cancel(args) | Command::Retry(args) = &cli.command {
            crate::service::completion::Target::parse(&args.id)?;
        }
        let env = std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect();
        let cwd = std::env::current_dir()
            .map_err(|e| crate::error::ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        let spec = crate::service::context::runtime_spec(&env)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| crate::error::ApiError::new(ErrorCode::Internal, e.to_string()))?;
        let result = runtime.block_on(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .map_err(|e| crate::error::ApiError::new(ErrorCode::Internal, e.to_string()))?;
            let watcher = tokio::spawn(async move {
                while interrupt.recv().await.is_some() {
                    if sender.send(()).is_err() {
                        break;
                    }
                }
            });
            let result = match &cli.command {
                Command::Exec(args) => {
                    crate::service::control::exec_mode(
                        args.execution.apply(crate::service::context::Context::load(
                            env,
                            cwd,
                            spec.clone(),
                        )?),
                        args.command.clone(),
                        args.in_place,
                        &mut receiver,
                    )
                    .await
                }
                Command::Wait(args) => {
                    crate::service::control::wait(
                        &spec,
                        crate::service::completion::Target::parse(&args.id)?,
                        &mut receiver,
                    )
                    .await
                }
                Command::Retry(args) => {
                    crate::service::control::retry(
                        &spec,
                        crate::service::completion::Target::parse(&args.id)?,
                    )
                    .await
                }
                Command::Cancel(args) => {
                    let target = crate::service::completion::Target::parse(&args.id)?;
                    let snapshot = crate::service::control::cancel(&spec, target.clone()).await?;
                    Ok(crate::service::control::Report {
                        schema_version: 1,
                        retry_receipt: None,
                        target,
                        task_id: None,
                        state: "cancel_requested",
                        stop_confirmed: snapshot.stop_confirmed(),
                        collection_pending: Some(snapshot.collection_pending_tasks > 0),
                        cancel_requested: true,
                        cancel_acknowledged: true,
                        snapshot: Some(snapshot),
                        cli_exit_code: 0,
                        error: None,
                    })
                }
                _ => unreachable!(),
            };
            watcher.abort();
            let _ = watcher.await;
            result
        });
        runtime.shutdown_background();
        result
    })();
    match result {
        Ok(report) => {
            let code = report.cli_exit_code;
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":code==0,"result":report})
                );
            } else {
                let id = match &report.target {
                    crate::service::completion::Target::Run(id) => id.to_string(),
                    crate::service::completion::Target::Task(id) => id.to_string(),
                };
                if report.snapshot.as_ref().is_some_and(|s| s.in_place) {
                    eprintln!(
                        "in-place: mutable original workspace; snapshot is a comparison baseline"
                    );
                }
                if let Some(task) = &report.task_id {
                    println!("{id} {task} {}", report.state);
                } else {
                    println!("{id} {}", report.state);
                }
                if report.collection_pending == Some(true) {
                    eprintln!("collection is still pending");
                }
                if let Some(error) = &report.error {
                    eprintln!("error: {}", error.message);
                }
            }
            code
        }
        Err(error) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":error})
                );
            } else {
                eprintln!("error: {}", error.message);
            }
            error.code.exit_code()
        }
    }
}

/// `worker --stdio`: relay stdin and stdout to the worker agent's Unix socket.
/// stdout carries protocol bytes only, so every diagnostic goes to stderr and
/// `--json` is not honoured here. This is the one place the process environment
/// is read (to find the socket); the rest of the crate takes a map.
fn run_worker_stdio() -> i32 {
    use crate::protocol::transport::{bridge, default_agent_socket, BridgeError};
    // Only HOME is read, with `var_os`: `std::env::vars()` panics on any
    // non-UTF-8 variable, and a bridge started under an odd environment must
    // still work. A HOME that is not UTF-8 cannot be a worker socket path here.
    let mut env = std::collections::BTreeMap::new();
    if let Some(home) = std::env::var_os("HOME").and_then(|h| h.into_string().ok()) {
        env.insert("HOME".to_string(), home);
    }
    let Some(socket) = default_agent_socket(&env) else {
        eprintln!("error: cannot locate the worker agent socket: HOME is not set");
        return ErrorCode::InvalidInput.exit_code();
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: cannot start the runtime: {e}");
            return exit::FAILURE;
        }
    };
    let result = runtime.block_on(bridge(&socket, tokio::io::stdin(), tokio::io::stdout()));
    // `tokio::io::stdin` reads on a blocking thread. When the agent closes
    // first, that read may be parked on a stdin the peer keeps open, and a plain
    // drop of the runtime would wait for it forever.
    runtime.shutdown_background();
    match result {
        Ok(()) => exit::SUCCESS,
        Err(e @ BridgeError::AgentNotRunning { .. }) => {
            eprintln!("error: {e}");
            ErrorCode::NotFound.exit_code()
        }
        Err(e) => {
            eprintln!("error: {e}");
            exit::FAILURE
        }
    }
}

/// Entry point used by the binary: parse arguments, run, return the exit code.
pub fn main() -> i32 {
    match Cli::try_parse() {
        Ok(cli) => run(cli),
        Err(e) => {
            let code = if e.use_stderr() {
                exit::INPUT_ERROR
            } else {
                exit::SUCCESS
            };
            let _ = e.print();
            code
        }
    }
}

fn run_query(cli: &Cli) -> i32 {
    use crate::error::ApiError;
    use crate::service::query::Query;
    let result = (|| -> Result<serde_json::Value, ApiError> {
        // Validate limits and identifier syntax before touching runtime state.
        let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
        let (limit, cursor) = match &cli.command {
            Command::Ls(a) => (a.limit, a.cursor.as_deref()),
            Command::Show(a) => (a.limit, a.cursor.as_deref()),
            _ => unreachable!(),
        };
        if limit == 0 || limit > 64 {
            return Err(invalid("limit must be 1..64".into()));
        }
        match &cli.command {
            Command::Ls(_) => {
                cursor
                    .map(crate::ids::RunId::parse)
                    .transpose()
                    .map_err(|e| invalid(e.to_string()))?;
            }
            Command::Show(a) => {
                if a.id.starts_with("run_") {
                    crate::ids::RunId::parse(&a.id).map_err(|e| invalid(e.to_string()))?;
                    cursor
                        .map(crate::ids::TaskId::parse)
                        .transpose()
                        .map_err(|e| invalid(e.to_string()))?;
                } else if a.id.starts_with("task_") {
                    crate::ids::TaskId::parse(&a.id).map_err(|e| invalid(e.to_string()))?;
                    cursor
                        .map(str::parse::<u64>)
                        .transpose()
                        .map_err(|e| invalid(e.to_string()))?;
                } else {
                    return Err(invalid("expected run_ or task_ identifier".into()));
                }
            }
            _ => unreachable!(),
        }
        let env = std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect();
        let spec = crate::service::context::runtime_spec(&env)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        runtime.block_on(async {
            let client = crate::service::query::Reader::open(spec)
                .await
                .map_err(|e| ApiError::new(e.code, e.to_string()))?;
            let value = match &cli.command {
                Command::Ls(_) => serde_json::to_value(
                    client
                        .query(Query::Runs {
                            limit,
                            after: cursor
                                .map(crate::ids::RunId::parse)
                                .transpose()
                                .map_err(|e| invalid(e.to_string()))?,
                        })
                        .await
                        .map_err(|e| ApiError::new(e.code, e.to_string()))?,
                ),
                Command::Show(a) => serde_json::to_value(
                    client
                        .show(&a.id, limit, cursor)
                        .await
                        .map_err(|e| ApiError::new(e.code, e.to_string()))?,
                ),
                _ => unreachable!(),
            }
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
            Ok(value)
        })
    })();
    match result {
        Ok(value) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":true,"data":value})
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&value).expect("query serializes")
                );
            }
            0
        }
        Err(error) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":error})
                );
            } else {
                eprintln!("error: {}", error.message);
            }
            error.code.exit_code()
        }
    }
}

#[cfg(test)]
mod estimate_args_tests {
    use super::*;
    #[test]
    fn frozen_task_conflicts_with_execution_overrides_and_raw_command_stays_required() {
        assert!(
            Cli::try_parse_from(["rtr", "estimate", "--task", "task_example", "--json"]).is_ok()
        );
        assert!(Cli::try_parse_from(["rtr", "estimate", "--", "echo", "hello"]).is_ok());
        assert!(Cli::try_parse_from(["rtr", "estimate"]).is_err());
        for extra in [
            vec!["--profile", "p"],
            vec!["--requires", "node"],
            vec!["--", "echo"],
        ] {
            let mut args = vec!["rtr", "estimate", "--task", "task_example"];
            args.extend(extra);
            assert!(Cli::try_parse_from(args).is_err());
        }
        let query = crate::service::estimate::Query {
            version: 1,
            argv: vec!["echo".into()],
            repository_id: None,
            cwd_relative: String::new(),
            requirements: Default::default(),
        };
        assert_eq!(
            serde_json::to_value(query).unwrap(),
            serde_json::json!({
                "version":1,"argv":["echo"],"repository_id":null,"cwd_relative":"",
                "requirements": {}
            })
        );
    }
}
