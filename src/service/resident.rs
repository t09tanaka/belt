//! Single-owner runtime with durable acceptance and independent local/remote delivery.
use crate::config::runtime::{WorkerInputs, WorkerTarget};
use crate::config::ResolvedConfig;
use crate::coordinator::{Coordinator, TaskSpec};
use crate::ids::*;
use crate::platform::wall_ms;
use crate::protocol::{connect, ClientConfig, Io};
use crate::results::{CollectConfig, ResultStore};
use crate::scheduler::measured::MeasuredCosts;
use crate::state::{AttemptState, Observation, Receipts, Termination};
use crate::storage::CoordinatorStore;
use crate::worker::{agent::Agent, serve::serve_connection};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, watch, Mutex, Semaphore};
use tokio::task::JoinSet;

mod ci_runs;
mod delivery;
mod input_retention;
mod remote;
mod result_retention;

pub const MAX_REQUEST_BYTES: usize = 262_144;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// All paths and worker identity/environment come from the bootstrap caller.
pub struct Inputs {
    pub state_dir: PathBuf,
    pub worker: WorkerInputs,
    pub collection_interval: Duration,
    pub collection: CollectConfig,
}

/// Private, versioned IPC. Clients capture into the configured private parent stores.
/// SubmitCaptured durably queues that immutable input; legacy Submit accepts prepositioned
/// local sources only and explicitly refuses secret-bearing tasks.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema_version: u32,
    pub cluster_id: ClusterId,
    pub owner_token: String,
    pub operation: Operation,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    ResolveToolchains {
        bundle: crate::toolchain::contract::FrozenBundle,
        requirements: crate::toolchain::capability::ExecutionRequirements,
    },
    /// One look at a warm-up on one worker (`workers sync`); the first look
    /// starts it. Asked again with the same `sync_id` until nothing is pending.
    EnvSync {
        worker: WorkerId,
        sync_id: crate::ids::RequestId,
        tools: Vec<crate::wire::ToolRequirement>,
    },
    Estimate {
        query: super::estimate::Query,
    },
    EstimateTask {
        task_id: TaskId,
    },
    Cancel {
        target: super::completion::Target,
        expected_control_revision: u64,
        request_id: String,
    },
    Retry {
        target: super::completion::Target,
        expected_control_revision: u64,
        request_id: String,
    },
    Ping,
    Health,
    /// Re-read the private worker registration and dial workers added since
    /// startup. Never disconnects or changes a worker that is already dialed.
    ReloadWorkers,
    /// Which binary this resident runs and what it offers and requires of a
    /// worker in `hello`. A resident older than this operation cannot read
    /// the request and closes the connection.
    Build,
    Submit {
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
    },
    SubmitCaptured {
        run_id: RunId,
        source_id: SourceId,
        tasks: Vec<(TaskId, TaskSpec)>,
    },
    Task {
        task_id: TaskId,
    },
    Query {
        query: super::query::Query,
    },
    Shutdown,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub attempt_id: AttemptId,
    pub state: AttemptState,
    pub observation: Observation,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Reply {
    ResolvedToolchains {
        bundle: crate::toolchain::contract::FrozenBundle,
    },
    EnvSync {
        results: Vec<crate::wire::ToolSyncResult>,
    },
    Estimate {
        estimate: super::estimate::Estimate,
    },
    RetryAccepted {
        receipt: super::completion::RetryReceipt,
    },
    Cancelled {
        snapshot: super::completion::Snapshot,
    },
    ControlError {
        error: crate::error::ApiError,
    },
    Ready {
        epoch: CoordinatorEpoch,
    },
    WorkersReloaded {
        /// The workers this request started dialing.
        connecting: Vec<String>,
    },
    Build {
        /// SHA-256 of the executable, read when the resident started.
        binary_sha256: String,
        features: Vec<String>,
        required_features: Vec<String>,
    },
    Health {
        cluster_id: ClusterId,
        coordinator_id: CoordinatorId,
        worker_id: WorkerId,
        epoch: CoordinatorEpoch,
        ready: bool,
        process: super::macos::RunningIdentity,
    },
    Accepted {
        run_id: RunId,
    },
    Task {
        attempts: Vec<Attempt>,
    },
    Query {
        response: super::query::Response,
    },
    QueryError {
        code: crate::error::ErrorCode,
        message: String,
    },
    Stopping,
    Error {
        message: String,
    },
}

/// The digest of this process's executable, read once. The resident begins
/// reading it when it starts (on a thread of its own: startup does not wait
/// for it), so a binary replaced on disk later does not change it.
pub(crate) fn own_build() -> Option<&'static str> {
    static BUILD: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    BUILD
        .get_or_init(|| {
            let executable = std::env::current_exe().ok()?;
            super::worker_service::sha256_file(&executable).ok()
        })
        .as_deref()
}

fn build_reply(build: Option<&str>) -> Reply {
    match build {
        Some(digest) => Reply::Build {
            binary_sha256: digest.to_string(),
            features: crate::protocol::session::offered_features(),
            required_features: crate::protocol::session::required_features(),
        },
        None => Reply::Error {
            message: "the resident could not read its own executable".into(),
        },
    }
}

/// No store is exposed: callers only receive socket credentials.
#[derive(Clone)]
pub struct Client {
    socket: PathBuf,
    cluster_id: ClusterId,
    owner_token: String,
}
impl Operation {
    fn requires_toolchains(&self) -> bool {
        match self {
            Self::ResolveToolchains { .. } => true,
            Self::Submit { tasks, .. } | Self::SubmitCaptured { tasks, .. } => {
                tasks.iter().any(|(_, spec)| spec.toolchains.is_some())
            }
            _ => false,
        }
    }
    /// A run of CI tasks: a resident that does not know them must refuse the
    /// request, not read the tasks as tasks without commands.
    fn requires_discovery(&self) -> bool {
        match self {
            Self::Submit { tasks, .. } | Self::SubmitCaptured { tasks, .. } => {
                tasks.iter().any(|(_, spec)| {
                    spec.ci
                        .as_ref()
                        .is_some_and(|ci| crate::wire::is_discovery_task(&ci.task))
                })
            }
            _ => false,
        }
    }
    fn requires_ci(&self) -> bool {
        match self {
            Self::Submit { tasks, .. } | Self::SubmitCaptured { tasks, .. } => {
                tasks.iter().any(|(_, spec)| spec.ci.is_some())
            }
            _ => false,
        }
    }
    fn requires_capabilities(&self) -> bool {
        match self {
            Self::Submit { tasks, .. } | Self::SubmitCaptured { tasks, .. } => {
                tasks.iter().any(|(_, spec)| !spec.requirements.is_empty())
            }
            _ => false,
        }
    }
}
impl Client {
    /// Load credentials without taking writable coordinator ownership.
    /// The expected cluster is supplied by the caller, never inferred from a token.
    pub fn load(state: &Path, cluster_id: ClusterId) -> anyhow::Result<Self> {
        check_private_dir(state)?;
        let ipc = state.join("parent");
        check_private_dir(&ipc)?;
        let mut file = secure_file(&ipc.join("owner.token"), false)?;
        let mut token = String::new();
        Read::by_ref(&mut file)
            .take(65)
            .read_to_string(&mut token)?;
        anyhow::ensure!(
            token.len() == 64
                && token
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "invalid owner token"
        );
        let socket = ipc.join("request.sock");
        let meta = std::fs::symlink_metadata(&socket)?;
        use std::os::unix::fs::FileTypeExt;
        anyhow::ensure!(
            meta.file_type().is_socket() && meta.uid() == uid() && meta.mode() & 0o077 == 0,
            "unsafe owner socket"
        );
        Ok(Self {
            socket,
            cluster_id,
            owner_token: token,
        })
    }

    pub async fn query(
        &self,
        query: super::query::Query,
    ) -> Result<super::query::Response, super::query::Error> {
        use super::query::Error;
        use crate::error::ErrorCode;
        match self
            .request(Operation::Query { query })
            .await
            .map_err(|e| Error::new(ErrorCode::CommunicationUnknown, e))?
        {
            Reply::Query { response } => Ok(response),
            Reply::QueryError { code, message } => Err(Error::new(code, message)),
            Reply::Error { message } => Err(Error::new(ErrorCode::CommunicationUnknown, message)),
            _ => Err(Error::new(ErrorCode::Internal, "unexpected query response")),
        }
    }
    pub fn socket_path(&self) -> &Path {
        &self.socket
    }
    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
    pub async fn request(&self, operation: Operation) -> anyhow::Result<Reply> {
        let mut stream = tokio::time::timeout(IO_TIMEOUT, UnixStream::connect(&self.socket))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "parent connect timeout"))??;
        let response_timeout = if matches!(&operation, Operation::SubmitCaptured { .. }) {
            Duration::from_secs(300)
        } else {
            IO_TIMEOUT
        };
        let request = Request {
            schema_version: if operation.requires_discovery() {
                6
            } else if operation.requires_ci() {
                5
            } else if operation.requires_toolchains() {
                4
            } else if operation.requires_capabilities() {
                3
            } else {
                2
            },
            cluster_id: self.cluster_id.clone(),
            owner_token: self.owner_token.clone(),
            operation,
        };
        write_message(&mut stream, &request).await?;
        Ok(read_message_with_timeout(&mut stream, response_timeout).await?)
    }
}

struct CapturedInputs {
    #[cfg(test)]
    retention_test: input_retention::TestControl,
    transfer: Arc<Mutex<()>>,
    sources: crate::source::SourceStore,
    vault: crate::secrets::vault::Vault,
    key: crate::secrets::key::ClusterKey,
    env: std::collections::BTreeMap<String, String>,
    parent_state: std::path::PathBuf,
    local_worker: WorkerId,
}

impl CapturedInputs {
    /// Validate the original run binding and retained bundle without capture.
    fn validate_retry(
        &self,
        prepared: &crate::storage::PreparedRetry,
    ) -> crate::storage::Result<bool> {
        use crate::storage::StorageError;
        let run = &prepared.run_id;
        let id = crate::source::classify::ci_id_of(&prepared.source_id)
            .ok_or_else(|| StorageError::Invalid("invalid retained source".into()))?;
        let unavailable =
            || StorageError::Immutable("retained source unavailable; submit a new run".into());
        if self
            .sources
            .source_of_run(run)
            .map_err(|_| unavailable())?
            .as_deref()
            != Some(&id)
        {
            return Err(unavailable());
        }
        let manifest = self
            .sources
            .manifest(&id)
            .map_err(|_| unavailable())?
            .ok_or_else(unavailable)?;
        for retained in self.sources.closure(&manifest).map_err(|_| unavailable())? {
            crate::source::store::verify_manifest(&retained).map_err(|_| unavailable())?;
            for entry in &retained.object_manifest.entries {
                if !self.sources.has_blob(&entry.hash)
                    || self.sources.blob_size(&entry.hash) != Some(entry.size)
                {
                    return Err(unavailable());
                }
            }
        }
        let digest = Digest::parse(&manifest.manifest_digest)
            .or_else(|_| Digest::parse(&format!("sha256:{}", manifest.manifest_digest)))
            .map_err(|_| unavailable())?;
        if prepared.manifest_digest != digest {
            return Err(unavailable());
        }
        if prepared.in_place {
            let b = crate::workspace::in_place::load(
                &self.parent_state,
                run,
                &prepared.source_id,
                &digest,
                &self.local_worker,
            )
            .map_err(|_| unavailable())?;
            let paths = manifest
                .secret_files
                .iter()
                .map(|s| s.path.display())
                .collect::<Vec<_>>();
            crate::workspace::in_place::read_secrets(&b, &self.env, &paths)
                .map_err(|_| StorageError::SecretUnavailable)?;
            return Ok(false);
        }
        self.vault
            .load(run, &manifest.secret_files, &self.key)
            .map_err(|_| StorageError::SecretUnavailable)?;
        Ok(!manifest.secret_files.is_empty())
    }
}

pub struct Resident {
    remote_config: Arc<ResolvedConfig>,
    /// Where the private registration is re-read from on a reload.
    remote_env: BTreeMap<String, String>,
    dial: remote::Dial,
    coordinator: Arc<Coordinator>,
    files: Arc<ResultStore>,
    agent: Arc<Agent>,
    sources: crate::source::SourceStore,
    captured: Arc<CapturedInputs>,
    worker_client: crate::protocol::Client,
    input_gate: Arc<crate::coordinator::InputGate>,
    worker_id: WorkerId,
    listener: UnixListener,
    worker_connection: tokio::task::JoinHandle<()>,
    client: Client,
    heartbeat: Duration,
    collection_interval: Duration,
    collection: CollectConfig,
    retention_days: u32,
    state_root: Arc<StateRoot>,
    _token_file: File,
}

/// The state directory this owner opened at startup. The open handle keeps
/// its inode from being reused, so device and inode identify it for as long
/// as the owner runs.
struct StateRoot {
    path: PathBuf,
    dir: File,
}
impl StateRoot {
    fn open(path: &Path) -> io::Result<Self> {
        let dir = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            dir,
        })
    }
    /// False once the path no longer names the directory opened at startup:
    /// it was removed, or something else took its name. An owner without its
    /// directory can reach neither its clients nor its durable work. A read
    /// that fails for another reason proves nothing and keeps the owner.
    fn present(&self) -> bool {
        let Ok(held) = self.dir.metadata() else {
            return true;
        };
        match std::fs::symlink_metadata(&self.path) {
            Ok(now) => now.dev() == held.dev() && now.ino() == held.ino(),
            Err(e) => !matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ),
        }
    }
}

struct StartupParts {
    coordinator: Arc<Coordinator>,
    files: Arc<ResultStore>,
    sources: crate::source::SourceStore,
    captured: Arc<CapturedInputs>,
    agent: Arc<Agent>,
    token_file: File,
    state_root: StateRoot,
    socket: PathBuf,
    token: String,
    worker_id: WorkerId,
    cluster: ClusterId,
    heartbeat: Duration,
    worker_timeout: Duration,
    collection_interval: Duration,
    collection: CollectConfig,
    retention_days: u32,
}
impl StartupParts {
    fn prepare(
        config: ResolvedConfig,
        inputs: Inputs,
        binding: Option<(ClusterId, CoordinatorId)>,
    ) -> anyhow::Result<Self> {
        private_dir(&inputs.state_dir)?;
        // Refuse symlinked existing state entries before storage follows them.
        reject_symlinks(&inputs.state_dir)?;
        let state_root = StateRoot::open(&inputs.state_dir)?;
        let settings = config.runtime_settings()?;
        let store = CoordinatorStore::open_bound(&inputs.state_dir, wall_ms(), binding)?;
        private_dir(&inputs.state_dir.join("parent"))?;
        let costs: Arc<dyn crate::scheduler::CostModel> =
            MeasuredCosts::open(&inputs.state_dir.join("parent/stats"), inputs.collection)
                .map(|costs| Arc::new(costs) as Arc<dyn crate::scheduler::CostModel>)
                .unwrap_or_else(|_| Arc::new(crate::scheduler::UnknownCosts));
        let coordinator = Arc::new(settings.start_coordinator(store, costs)?);
        let files = Arc::new(ResultStore::open(&inputs.state_dir.join("results"))?);
        let ipc = inputs.state_dir.join("parent");
        private_dir(&ipc)?;
        let token_path = ipc.join("owner.token");
        let mut token_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(token_path)?;
        anyhow::ensure!(
            token_file.metadata()?.is_file()
                && token_file.metadata()?.nlink() == 1
                && token_file.metadata()?.uid() == uid()
                && token_file.metadata()?.permissions().mode() & 0o077 == 0,
            "unsafe owner token"
        );
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("owner token: {e}"))?;
        let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        token_file.set_len(0)?;
        token_file.write_all(token.as_bytes())?;
        token_file.sync_all()?;
        let socket = ipc.join("request.sock");
        let worker_id = inputs.worker.worker_id.clone();
        private_dir(&inputs.worker.state_dir)?;
        reject_symlinks(&inputs.worker.state_dir)?;
        let sources = crate::source::SourceStore::open(&inputs.worker.state_dir)?;
        let captured = Arc::new(CapturedInputs {
            #[cfg(test)]
            retention_test: input_retention::TestControl::default(),
            transfer: Arc::new(Mutex::new(())),
            sources: crate::source::SourceStore::open(&inputs.state_dir)?,
            vault: crate::secrets::vault::Vault::open(&inputs.state_dir)?,
            key: crate::secrets::key::load(&inputs.worker.cluster_key_path)?,
            env: inputs.worker.env.clone(),
            parent_state: inputs.state_dir.clone(),
            local_worker: worker_id.clone(),
        });
        coordinator.set_parent_local(worker_id.clone());
        let runtime = config.worker_runtime(inputs.worker)?;
        let host = runtime
            .open_host_in_place(inputs.state_dir.clone())
            .map_err(|e| anyhow::anyhow!(e.message))?;
        let agent = runtime
            .start_with_host(host)
            .map_err(|e| anyhow::anyhow!(e.message))?;
        let cluster = coordinator.store().cluster_id().clone();
        Ok(StartupParts {
            cluster,
            coordinator,
            files,
            sources,
            captured,
            agent,
            token_file,
            state_root,
            socket,
            token,
            worker_id,
            heartbeat: Duration::from_millis(settings.heartbeat_ms),
            worker_timeout: Duration::from_millis(settings.coordinator.worker_budget_ms),
            collection_interval: inputs.collection_interval,
            collection: inputs.collection,
            retention_days: config.retention_days.value,
        })
    }
}
struct Startup {
    parts: Option<StartupParts>,
}
impl Startup {
    fn cleanup(mut self) {
        if let Some(parts) = self.parts.take() {
            parts.agent.shutdown();
        }
    }
}
impl Drop for Startup {
    fn drop(&mut self) {
        if let Some(parts) = self.parts.take() {
            // Cancellation must also retain ownership until agent watchers stop.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || {
                    parts.agent.shutdown();
                    drop(parts);
                });
            } else {
                parts.agent.shutdown();
            }
        }
    }
}
async fn connect_local(
    startup: &Startup,
    config: &ResolvedConfig,
) -> anyhow::Result<(
    UnixListener,
    tokio::task::JoinHandle<()>,
    crate::protocol::Client,
    Arc<crate::coordinator::InputGate>,
    bool,
    bool,
    bool,
)> {
    let parts = startup.parts.as_ref().expect("prepared startup");
    let listener = crate::protocol::transport::listen_unix(&parts.socket)?;
    let (parent, worker) = UnixStream::pair()?;
    let worker_agent = parts.agent.clone();
    let cluster = parts.cluster.clone();
    let server_cluster = cluster.clone();
    let worker_connection = tokio::spawn(async move {
        let _ = serve_connection(
            worker_agent,
            Io::from_stream(worker),
            server_cluster,
            Arc::new(Semaphore::new(64)),
        )
        .await;
    });
    let connected = match connect(
        Io::from_stream(parent),
        ClientConfig::new(
            cluster.clone(),
            parts.coordinator.store().coordinator_id().clone(),
            parts.coordinator.epoch(),
        ),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            worker_connection.abort();
            return Err(e.into());
        }
    };
    let probe = connected
        .negotiated
        .features
        .iter()
        .any(|feature| feature == crate::wire::PREPARATION_PROBE_FEATURE);
    let observations = connected
        .negotiated
        .features
        .iter()
        .any(|feature| feature == crate::setup::observation_transport::FEATURE);
    let env_sync = connected
        .negotiated
        .features
        .iter()
        .any(|feature| feature == crate::wire::ENV_SYNC_FEATURE);
    let worker_client = connected.client.clone();
    let provenance = Digest::of_bytes(&serde_json::to_vec(
        &serde_json::json!({"transport":"local-private-rpc-v1","protocol":connected.negotiated.version,"features":connected.negotiated.features,"schema":connected.worker.schema_version,"profile":connected.worker.profile_fingerprint,"timeout_ms":parts.worker_timeout.as_millis()}),
    )?);
    let input_gate = Arc::new(crate::coordinator::InputGate::with_transport_policy(
        provenance.to_string(),
    ));
    input_gate.set_ci_discovery(
        connected
            .negotiated
            .features
            .iter()
            .any(|f| f == crate::wire::CI_DISCOVERY_FEATURE),
    );
    input_gate.set_ci_task(
        connected
            .negotiated
            .features
            .iter()
            .any(|feature| feature == crate::wire::CI_TASK_FEATURE),
    );
    input_gate
        .observe(
            connected.worker.worker_boot_id.clone(),
            connected.worker.agent_epoch,
        )
        .await;
    if let Err(e) = config
        .attach_worker(
            &parts.coordinator,
            WorkerTarget::Local(&parts.worker_id),
            connected,
        )
        .await
    {
        worker_connection.abort();
        anyhow::bail!(e.message);
    }
    Ok((
        listener,
        worker_connection,
        worker_client,
        input_gate,
        observations,
        probe,
        env_sync,
    ))
}

impl Resident {
    /// Take the process lock before starting a new epoch, then construct exactly
    /// one coordinator and one ResultStore. A second owner fails before mutation.
    pub async fn start(config: &ResolvedConfig, inputs: Inputs) -> anyhow::Result<Self> {
        Self::start_bound(config, inputs, None).await
    }
    pub(crate) async fn start_bound(
        config: &ResolvedConfig,
        inputs: Inputs,
        binding: Option<(ClusterId, CoordinatorId)>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !inputs.collection_interval.is_zero(),
            "zero collection interval"
        );
        std::thread::spawn(own_build);
        anyhow::ensure!(
            config.local_worker.value,
            "local resident requires local worker"
        );
        let inputs_env = inputs.worker.env.clone();
        let remote_config = Arc::new(remote::snapshot(
            config,
            &inputs.worker.env,
            &inputs.worker.worker_id,
        )?);
        // The local worker's commands that still run in another scope: wait
        // for them instead of failing into a restart loop.
        anyhow::ensure!(
            super::traffic_hold::wait_until_clear(&inputs.worker.state_dir, &inputs.worker.env)
                .await?,
            "stopped while waiting for another task-traffic-controller scope to drain"
        );
        let config_owned = config.clone();
        let mut startup = tokio::task::spawn_blocking(move || {
            StartupParts::prepare(config_owned, inputs, binding)
                .map(|parts| Startup { parts: Some(parts) })
        })
        .await??;
        let connected = connect_local(&startup, config).await;
        let (listener, worker_connection, worker_client, input_gate, observations, probe, env_sync) =
            match connected {
                Ok(value) => value,
                Err(e) => {
                    tokio::task::spawn_blocking(move || startup.cleanup()).await?;
                    return Err(e);
                }
            };
        let parts = startup.parts.take().expect("prepared startup");

        parts
            .coordinator
            .attach(
                parts.worker_id.clone(),
                Arc::new(
                    crate::coordinator::ClientLink::gated(
                        worker_client.clone(),
                        parts.worker_timeout,
                        input_gate.clone(),
                    )
                    .with_preparation_observations(observations)
                    .with_preparation_probe(probe)
                    .with_env_sync(env_sync)
                    .with_ci_task(input_gate.ci_task())
                    .with_ci_discovery(input_gate.ci_discovery()),
                ),
            )
            .await;
        Ok(Self {
            remote_config,
            remote_env: inputs_env,
            dial: remote::dial(),
            coordinator: parts.coordinator,
            files: parts.files,
            agent: parts.agent,
            sources: parts.sources,
            captured: parts.captured,
            worker_client,
            input_gate,
            worker_id: parts.worker_id,
            listener,
            worker_connection,
            client: Client {
                socket: parts.socket,
                cluster_id: parts.cluster,
                owner_token: parts.token,
            },
            heartbeat: parts.heartbeat,
            collection_interval: parts.collection_interval,
            collection: parts.collection,
            retention_days: parts.retention_days,
            state_root: Arc::new(parts.state_root),
            _token_file: parts.token_file,
        })
    }
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Run until owner-authenticated shutdown, fencing, or the loss of the
    /// state directory (checked on the heartbeat tick; a clean exit).
    /// Disconnecting a request never cancels accepted work. All work loops
    /// have separate clocks.
    pub async fn run(mut self) -> anyhow::Result<()> {
        let (stop, rx) = watch::channel(false);
        let mut loops = JoinSet::new();
        // Connection failures stay inside each owner and never terminate the resident.
        let mut remotes = JoinSet::new();
        let mut dialed = BTreeSet::new();
        for worker in self.remote_config.workers.clone() {
            dialed.insert(worker.id.clone());
            remotes.spawn((self.dial)(
                self.remote_config.clone(),
                worker,
                self.coordinator.clone(),
                self.captured.clone(),
                rx.clone(),
            ));
        }
        let (reload_tx, mut reload_rx) = mpsc::channel::<oneshot::Sender<Reply>>(4);
        let (base, env, local) = (
            self.remote_config.clone(),
            self.remote_env.clone(),
            self.worker_id.clone(),
        );
        let coordinator = self.coordinator.clone();
        let stop_tick = stop.clone();
        let mut tick_stop = rx.clone();
        let heartbeat = self.heartbeat;
        let state_root = self.state_root.clone();
        let tick_root = state_root.clone();
        loops.spawn(async move {
            let mut timer = tokio::time::interval(heartbeat);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = tick_stop.changed() => return Ok::<(), anyhow::Error>(()), _ = timer.tick() => {} }
                if !tick_root.present() { let _ = stop_tick.send(true); return Ok(()); }
                if let Err(e) = coordinator.tick().await { let _ = stop_tick.send(true); return Err(e.into()); }
            }
        });
        let coordinator = self.coordinator.clone();
        let files = self.files.clone();
        let collection = self.collection;
        let cadence = self.collection_interval;
        let mut collect_stop = rx.clone();
        let stop_collect = stop.clone();
        loops.spawn(async move {
            let mut timer = tokio::time::interval(cadence);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = collect_stop.changed() => return Ok(()), _ = timer.tick() => {} }
                if let Err(e) = coordinator.collect_stopping(&files, collection, Some(collect_stop.clone())).await { let _ = stop_collect.send(true); return Err(e.into()); }
            }
        });
        loops.spawn(ci_runs::run(
            self.coordinator.clone(),
            self.captured.parent_state.clone(),
            heartbeat,
            self.retention_days,
            rx.clone(),
        ));
        #[cfg(not(test))]
        let gc_cadence = Duration::from_secs(60);
        #[cfg(test)]
        let gc_cadence = self.captured.retention_test.cadence();
        loops.spawn(result_retention::run(
            self.coordinator.clone(),
            self.files.clone(),
            self.retention_days,
            gc_cadence,
            rx.clone(),
        ));
        loops.spawn(input_retention::run(
            self.coordinator.clone(),
            self.captured.clone(),
            self.retention_days,
            rx.clone(),
        ));
        let agent = self.agent.clone();
        let mut maintain_stop = rx.clone();
        loops.spawn(async move {
            let mut timer = tokio::time::interval(heartbeat);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = maintain_stop.changed() => return Ok(()), _ = timer.tick() => {} }
                let agent = agent.clone();
                tokio::task::spawn_blocking(move || agent.supervise()).await?;
            }
        });
        loops.spawn(delivery::sources(
            self.coordinator.clone(),
            self.captured.clone(),
            self.worker_client.clone(),
            self.worker_id.clone(),
            self.input_gate.clone(),
            rx.clone(),
        ));
        loops.spawn(delivery::secrets(
            self.coordinator.clone(),
            self.captured.clone(),
            self.worker_client.clone(),
            self.worker_id.clone(),
            self.input_gate.clone(),
            rx.clone(),
        ));
        let permits = Arc::new(Semaphore::new(32));
        // Serializes acceptance with shutdown, not with placement/collection.
        let admission = Arc::new(Mutex::new(false));
        let mut requests = JoinSet::new();
        let mut stopped = rx;
        let mut failure = None;
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                result = &mut self.worker_connection => {
                    failure = Some(anyhow::anyhow!("local worker connection ended: {result:?}"));
                    break;
                },
                result = loops.join_next(), if !loops.is_empty() => {
                    match result { Some(Ok(Err(e))) => failure = Some(e), Some(Err(e)) => failure = Some(e.into()), _ => {} }
                    break;
                },
                Some(answer) = reload_rx.recv() => {
                    let (base, env, local) = (base.clone(), env.clone(), local.clone());
                    let fresh = tokio::task::spawn_blocking(move || remote::reload(&base, &env, &local)).await;
                    let reply = match fresh {
                        Ok(Ok(fresh)) => {
                            let fresh = Arc::new(fresh);
                            let mut connecting = Vec::new();
                            for worker in fresh.workers.clone() {
                                if dialed.insert(worker.id.clone()) {
                                    connecting.push(worker.id.clone());
                                    remotes.spawn((self.dial)(fresh.clone(), worker, self.coordinator.clone(), self.captured.clone(), stop.subscribe()));
                                }
                            }
                            Reply::WorkersReloaded { connecting }
                        }
                        Ok(Err(e)) => Reply::ControlError { error: crate::error::ApiError::new(crate::error::ErrorCode::InvalidInput, format!("worker registration unusable: {e}")) },
                        Err(e) => Reply::ControlError { error: crate::error::ApiError::new(crate::error::ErrorCode::Internal, e.to_string()) },
                    };
                    let _ = answer.send(reply);
                },
                ended = requests.join_next(), if !requests.is_empty() => { if let Some(Err(e)) = ended { failure = Some(anyhow::Error::from(e)); break; } },
                conn = self.listener.accept() => {
                    let (stream, _) = match conn { Ok(c) => c, Err(e) => { failure = Some(e.into()); break; } };
                    if stream.peer_cred().map(|peer| peer.uid()).ok() != Some(uid()) { continue; }
                    let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                    let coordinator = self.coordinator.clone();
                    let sources = self.sources.clone();
                    let captured = self.captured.clone();
                    let worker_client = self.worker_client.clone();
                    let client = self.client.clone();
                    let worker_id = self.worker_id.clone();
                    let stop = stop.clone();
                    let admission = admission.clone();
                    let reload = reload_tx.clone();
                    requests.spawn(async move {
                        let _permit = permit;
                        let _ = handle(stream, coordinator, sources, captured, worker_client, worker_id, client, admission, stop, reload).await;
                    });
                }
            }
        }
        *admission.lock().await = true;
        let _ = stop.send(true);
        while let Some(result) = requests.join_next().await {
            if let Err(e) = result {
                failure.get_or_insert(e.into());
            }
        }
        while let Some(result) = loops.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(e) => {
                    failure.get_or_insert(e.into());
                }
            }
        }
        while let Some(result) = remotes.join_next().await {
            if let Err(e) = result {
                failure.get_or_insert(e.into());
            }
        }
        let costs = self.coordinator.scheduler().model();
        // All telemetry producers are drained, outside actor and admission locks.
        let _ = tokio::task::spawn_blocking(move || costs.drain()).await;
        // No cancellation or terminal write: supervisors keep running, and a
        // restarted owner adopts the same durable attempts.
        let agent = self.agent.clone();
        tokio::task::spawn_blocking(move || agent.shutdown()).await?;
        self.worker_connection.abort();
        match failure {
            // Another loop may meet the missing files before the heartbeat
            // does; without the directory that is the same clean exit.
            Some(e) if state_root.present() => Err(e),
            _ => Ok(()),
        }
    }
}
impl Drop for Resident {
    fn drop(&mut self) {
        self.worker_connection.abort();
        self.agent.shutdown();
        // Only this lock-holding owner's private socket is removed: when the
        // directory was replaced, the name belongs to someone else.
        if self.state_root.present() {
            let _ = std::fs::remove_file(&self.client.socket);
        }
    }
}

/// Admission stays open while input IO runs. A prepared token is rechecked at commit.
struct RetryCommand {
    target: super::completion::Target,
    expected: u64,
    request_id: String,
}
async fn retry_control<F, L>(
    coordinator: Arc<Coordinator>,
    admission: Arc<Mutex<bool>>,
    command: RetryCommand,
    lease: Arc<L>,
    mut stop: watch::Receiver<bool>,
    validate: F,
) -> anyhow::Result<Reply>
where
    F: FnOnce(&crate::storage::PreparedRetry) -> crate::storage::Result<bool> + Send + 'static,
    L: Send + Sync + 'static,
{
    let error_reply = |error: crate::storage::StorageError| Reply::ControlError {
        error: if matches!(error, crate::storage::StorageError::SecretUnavailable) {
            crate::error::ApiError::blocked(
                crate::error::BlockedReason::SecretUnavailable,
                error.to_string(),
            )
        } else {
            crate::error::ApiError::new(error.code(), error.to_string())
        },
    };
    let stopping_reply = || Reply::ControlError {
        error: crate::error::ApiError::new(
            crate::error::ErrorCode::CommunicationUnknown,
            "resident is stopping",
        ),
    };
    if *stop.borrow() {
        return Ok(stopping_reply());
    }
    let owner = coordinator.clone();
    let prepared = tokio::task::spawn_blocking(move || {
        owner
            .store()
            .prepare_retry(command.target, command.expected, command.request_id)
    })
    .await?;
    let prepared = match prepared {
        Ok(crate::storage::RetryPreparation::Replay(receipt)) => {
            return Ok(Reply::RetryAccepted { receipt })
        }
        Ok(crate::storage::RetryPreparation::Prepared(prepared)) => prepared,
        Err(error) => return Ok(error_reply(error)),
    };
    let input_lease = lease.clone();
    // This detached work owns only metadata, captured inputs and the input lease.
    let inputs = coordinator.clone();
    let preflight = tokio::task::spawn_blocking(move || {
        let source = inputs.store().source_mutation_guard_foreground();
        drop(inputs); // Detached input IO retains only its source guard, never owner metadata.
        let validated =
            source.and_then(|source| validate(&prepared).map(|secret| (secret, source)));
        // The lease leaves with the source guard and behind it. When a stopped
        // caller has dropped the handle, the runtime drops this output in field
        // order, so the source lock is released before the lease's ownership.
        (prepared, validated, input_lease)
    });
    let (prepared, validated, _input_lease) = tokio::select! {
        biased;
        _=stop.wait_for(|stopped| *stopped)=>return Ok(stopping_reply()),
        result=preflight=>result?,
    };
    let (verified_secret, source) = match validated {
        Ok(secret) => secret,
        Err(error) => return Ok(error_reply(error)),
    };
    let gate = tokio::select! {
        biased;
        _=stop.wait_for(|stopped| *stopped)=>return Ok(stopping_reply()),
        gate=admission.lock()=>gate,
    };
    if *gate || coordinator.is_fenced() {
        return Ok(Reply::ControlError {
            error: crate::error::ApiError::new(
                if coordinator.is_fenced() {
                    crate::error::ErrorCode::StaleEpoch
                } else {
                    crate::error::ErrorCode::CommunicationUnknown
                },
                "resident is stopping",
            ),
        });
    }
    let result = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        coordinator
            .store()
            .commit_retry_guarded(prepared, verified_secret, wall_ms(), source)
    })
    .await?;
    drop(gate);
    Ok(match result {
        Ok(receipt) => Reply::RetryAccepted { receipt },
        Err(error) => error_reply(error),
    })
}
#[allow(clippy::too_many_arguments)]
async fn handle(
    mut stream: UnixStream,
    coordinator: Arc<Coordinator>,
    sources: crate::source::SourceStore,
    captured: Arc<CapturedInputs>,
    worker_client: crate::protocol::Client,
    worker_id: WorkerId,
    client: Client,
    admission: Arc<Mutex<bool>>,
    stop: watch::Sender<bool>,
    reload: mpsc::Sender<oneshot::Sender<Reply>>,
) -> anyhow::Result<()> {
    let request: Request = read_message(&mut stream).await?;
    anyhow::ensure!(
        matches!(request.schema_version, 2..=6)
            && (request.schema_version >= 6 || !request.operation.requires_discovery())
            && (request.schema_version >= 5 || !request.operation.requires_ci())
            && (request.schema_version >= 3 || !request.operation.requires_capabilities())
            && (request.schema_version >= 4 || !request.operation.requires_toolchains())
            && request.cluster_id == client.cluster_id
            && request.owner_token == client.owner_token,
        "unauthorized parent request"
    );
    // Metadata reads use their own read-only SQLite thread, never the owner's
    // admission lock or update connection. Dropping the response cannot write state.
    if let Operation::Query { query } = request.operation {
        let state = coordinator
            .store()
            .db
            .path()
            .parent()
            .expect("database parent")
            .to_owned();
        let reply = tokio::task::spawn_blocking(move || {
            match crate::storage::CoordinatorStore::open_read_only(&state)
                .and_then(|s| s.query(query))
            {
                Ok(response) => Reply::Query { response },
                Err(e) => Reply::QueryError {
                    code: e.code(),
                    message: e.to_string(),
                },
            }
        })
        .await?;
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    if matches!(request.operation, Operation::Build) {
        // The digest may still be being read; the runtime does not wait for it.
        let build = tokio::task::spawn_blocking(own_build).await?;
        return write_message(&mut stream, &build_reply(build))
            .await
            .map_err(Into::into);
    }
    if matches!(request.operation, Operation::ReloadWorkers) {
        let (answer, wait) = oneshot::channel();
        let reply = if reload.send(answer).await.is_err() {
            Reply::Stopping
        } else {
            match tokio::time::timeout(Duration::from_secs(4), wait).await {
                Ok(Ok(reply)) => reply,
                _ => Reply::ControlError {
                    error: crate::error::ApiError::new(
                        crate::error::ErrorCode::CommunicationUnknown,
                        "the resident did not finish the reload in time",
                    ),
                },
            }
        };
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    if let Operation::EstimateTask { task_id } = request.operation {
        let reply = match coordinator.estimate_task(task_id).await {
            Ok(estimate) => Reply::Estimate { estimate },
            Err(error) => Reply::ControlError { error },
        };
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    if let Operation::Estimate { query } = request.operation {
        let reply = match coordinator.estimate(query).await {
            Ok(estimate) => Reply::Estimate { estimate },
            Err(error) => Reply::ControlError { error },
        };
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    if let Operation::EnvSync {
        worker,
        sync_id,
        tools,
    } = request.operation
    {
        // Like the resolution below: one bounded call to the worker, no
        // admission lock and no writable SQLite scope.
        let mut stopping = stop.subscribe();
        let reply = tokio::select! {
            _ = stopping.changed() => Reply::Stopping,
            result = coordinator.env_sync(&worker, sync_id, tools) => match result {
                Ok(results) => Reply::EnvSync { results },
                Err(error) => Reply::ControlError { error },
            }
        };
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    if let Operation::ResolveToolchains {
        bundle,
        requirements,
    } = request.operation
    {
        // No admission lock or writable SQLite scope is held during remote IO.
        let mut stopping = stop.subscribe();
        let reply = tokio::select! {
            _ = stopping.changed() => Reply::Stopping,
            result = coordinator.resolve_toolchains(bundle, requirements) => match result {
                Ok(bundle) if !*stop.borrow() && !coordinator.is_fenced() => Reply::ResolvedToolchains { bundle },
                Ok(_) => Reply::Stopping,
                Err(error) => Reply::ControlError { error },
            }
        };
        return write_message(&mut stream, &reply).await.map_err(Into::into);
    }
    let mut gate = admission.lock().await;
    let reply = if *gate || coordinator.is_fenced() {
        if matches!(
            &request.operation,
            Operation::Cancel { .. } | Operation::Retry { .. }
        ) {
            Reply::ControlError {
                error: crate::error::ApiError::new(
                    if coordinator.is_fenced() {
                        crate::error::ErrorCode::StaleEpoch
                    } else {
                        crate::error::ErrorCode::CommunicationUnknown
                    },
                    "resident is stopping",
                ),
            }
        } else {
            Reply::Error {
                message: "resident is stopping".into(),
            }
        }
    } else {
        match request.operation {
            Operation::Retry {
                target,
                expected_control_revision,
                request_id,
            } => {
                drop(gate);
                let mut stopping = stop.subscribe();
                let retained = async {
                    let ownership = tokio::select! {
                        biased;
                        _=stopping.wait_for(|stopped| *stopped)=>anyhow::bail!("resident is stopping"),
                        ownership=captured.transfer.clone().lock_owned()=>ownership,
                    };
                    super::forward::foreground_lease(&captured.sources, &mut stopping, ownership).await
                }.await;
                let lease = match retained {
                    Ok(lease) => lease,
                    Err(error) => {
                        return write_message(
                            &mut stream,
                            &Reply::ControlError {
                                error: if error.is::<crate::source::retention::ForegroundBusy>() {
                                    crate::source::classify::api_error_of(&error)
                                } else {
                                    crate::error::ApiError::new(
                                        crate::error::ErrorCode::CommunicationUnknown,
                                        error.to_string(),
                                    )
                                },
                            },
                        )
                        .await
                        .map_err(Into::into)
                    }
                };
                let answer = retry_control(
                    coordinator,
                    admission,
                    RetryCommand {
                        target,
                        expected: expected_control_revision,
                        request_id,
                    },
                    lease,
                    stopping,
                    move |prepared| captured.validate_retry(prepared),
                )
                .await?;
                return write_message(&mut stream, &answer)
                    .await
                    .map_err(Into::into);
            }
            Operation::Cancel {
                target,
                expected_control_revision,
                request_id,
            } => {
                let clear_target = target.clone();
                let owner = coordinator.clone();
                match tokio::task::spawn_blocking(move || {
                    owner.store().cancel_control(
                        target,
                        expected_control_revision,
                        request_id,
                        wall_ms(),
                    )
                })
                .await?
                {
                    Ok(snapshot) => {
                        coordinator.clear_prefetch(&clear_target);
                        Reply::Cancelled { snapshot }
                    }
                    Err(error) => Reply::ControlError {
                        error: crate::error::ApiError::new(error.code(), error.to_string()),
                    },
                }
            }
            Operation::Health => match super::macos::RunningIdentity::observe(std::process::id()) {
                Ok(process) => Reply::Health {
                    cluster_id: coordinator.store().cluster_id().clone(),
                    coordinator_id: coordinator.store().coordinator_id().clone(),
                    worker_id,
                    epoch: coordinator.epoch(),
                    ready: worker_client.ping(Duration::from_secs(1)).await.is_ok(),
                    process,
                },
                Err(_) => Reply::Error {
                    message: "resident process identity unknown".into(),
                },
            },
            // Answered before admission; never reaches this match.
            Operation::ReloadWorkers | Operation::Build => Reply::Error {
                message: "handled outside admission".into(),
            },
            Operation::Ping => Reply::Ready {
                epoch: coordinator.store().epoch(),
            },
            Operation::Submit {
                run_id,
                source_id,
                tasks,
            } => {
                drop(gate);
                let answer = if tasks.is_empty() || tasks.len() > 64 {
                    Reply::Error {
                        message: "submit requires 1..64 tasks".into(),
                    }
                } else {
                    let source = source_id.clone();
                    let expected = tasks[0].1.manifest_digest.clone();
                    if tasks
                        .iter()
                        .any(|(_, task)| task.manifest_digest != expected || task.has_secret_bundle)
                    {
                        return write_message(
                            &mut stream,
                            &Reply::Error {
                                message:
                                    "source digests must match; secret forwarding is unavailable"
                                        .into(),
                            },
                        )
                        .await
                        .map_err(Into::into);
                    }
                    let owner = coordinator.clone();
                    let checked = tokio::task::spawn_blocking(
                        move || -> anyhow::Result<crate::source::retention::Guard> {
                            let mutation = owner.store().source_mutation_guard_foreground()?;
                            let ci =
                                crate::source::classify::ci_id_of(&source).ok_or_else(|| {
                                    anyhow::anyhow!("source is not a captured source")
                                })?;
                            let manifest = sources.manifest(&ci)?.ok_or_else(|| {
                                anyhow::anyhow!("source has not reached the local worker")
                            })?;
                            crate::source::store::verify_manifest(&manifest)?;
                            let digest =
                                Digest::parse(&manifest.manifest_digest).or_else(|_| {
                                    Digest::parse(&format!("sha256:{}", manifest.manifest_digest))
                                })?;
                            anyhow::ensure!(digest == expected, "source manifest digest mismatch");
                            owner
                                .store()
                                .put_source_ref(&source, &expected, wall_ms())?;
                            Ok(mutation)
                        },
                    )
                    .await?;
                    let source_guard = match checked {
                        Ok(guard) => guard,
                        Err(e)
                            if e.downcast_ref::<crate::storage::StorageError>()
                                .is_some_and(|e| {
                                    matches!(e, crate::storage::StorageError::SourceBusy)
                                }) =>
                        {
                            return write_message(
                                &mut stream,
                                &Reply::ControlError {
                                    error: crate::error::ApiError::new(
                                        crate::error::ErrorCode::SourceBusy,
                                        "captured inputs are temporarily busy",
                                    ),
                                },
                            )
                            .await
                            .map_err(Into::into);
                        }
                        Err(e) => {
                            return write_message(
                                &mut stream,
                                &Reply::Error {
                                    message: e.to_string(),
                                },
                            )
                            .await
                            .map_err(Into::into);
                        }
                    };
                    let gate = admission.lock().await;
                    anyhow::ensure!(!*gate && !coordinator.is_fenced(), "resident is stopping");
                    match coordinator
                        .enqueue_guarded(run_id.clone(), source_id, tasks, source_guard)
                        .await
                    {
                        Ok(()) => Reply::Accepted { run_id },
                        Err(e) => Reply::Error {
                            message: e.to_string(),
                        },
                    }
                };
                return write_message(&mut stream, &answer)
                    .await
                    .map_err(Into::into);
            }
            Operation::SubmitCaptured {
                run_id,
                source_id,
                tasks,
            } => {
                // Capture validation runs outside admission; delivery is a background job.
                drop(gate);
                let source = source_id.clone();
                let run = run_id.clone();
                let capture = captured.clone();
                let mut stopping = stop.subscribe();
                let checked = tokio::task::spawn_blocking(
                    move || -> anyhow::Result<(Digest, bool, crate::source::retention::Guard)> {
                        let mutation = capture.sources.foreground_guard()?;
                        let id = crate::source::classify::ci_id_of(&source)
                            .ok_or_else(|| anyhow::anyhow!("invalid source"))?;
                        anyhow::ensure!(
                            capture.sources.source_of_run(&run)?.as_deref() == Some(&id),
                            "run source binding mismatch"
                        );
                        let manifest = capture
                            .sources
                            .manifest(&id)?
                            .ok_or_else(|| anyhow::anyhow!("source unavailable"))?;
                        crate::source::store::verify_manifest(&manifest)?;
                        capture
                            .vault
                            .load(&run, &manifest.secret_files, &capture.key)?;
                        Ok((
                            Digest::parse(&manifest.manifest_digest).or_else(|_| {
                                Digest::parse(&format!("sha256:{}", manifest.manifest_digest))
                            })?,
                            !manifest.secret_files.is_empty(),
                            mutation,
                        ))
                    },
                );
                let result = async {
                    let (digest, has_secrets, source_guard) = tokio::select! {
                        biased;
                        _=stopping.wait_for(|stopped| *stopped)=>anyhow::bail!("resident is stopping"),
                        checked=checked=>checked??,
                    };
                    anyhow::ensure!(
                        !tasks.is_empty() && tasks.len() <= 64,
                        "submit requires 1..64 tasks"
                    );
                    anyhow::ensure!(
                        tasks.iter().all(|(_, task)| task.manifest_digest == digest
                            && task.has_secret_bundle == (has_secrets && !task.in_place)),
                        "source digest mismatch"
                    );
                    let guard = admission.lock().await;
                    anyhow::ensure!(!*guard && !coordinator.is_fenced(), "resident is stopping");
                    let owner = coordinator.clone();
                    let source = source_id.clone();
                    tokio::task::spawn_blocking(move || {
                        owner.store().put_source_ref(&source, &digest, wall_ms())
                    })
                    .await??;
                    coordinator
                        .enqueue_guarded(run_id.clone(), source_id, tasks, source_guard)
                        .await?;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                let answer = match result {
                    Ok(()) => Reply::Accepted { run_id },
                    Err(e) if e.is::<crate::source::retention::ForegroundBusy>() => {
                        Reply::ControlError {
                            error: crate::source::classify::api_error_of(&e),
                        }
                    }
                    Err(e) => Reply::Error {
                        message: e.to_string(),
                    },
                };
                return write_message(&mut stream, &answer)
                    .await
                    .map_err(Into::into);
            }
            Operation::Task { task_id } => {
                let coordinator = coordinator.clone();
                match tokio::task::spawn_blocking(move || {
                    coordinator.store().latest_attempt_of_task(&task_id)
                })
                .await?
                {
                    Ok(records) => Reply::Task {
                        attempts: records
                            .into_iter()
                            .map(|a| Attempt {
                                attempt_id: a.attempt_id,
                                state: a.state,
                                observation: a.observation,
                                termination: a.termination,
                                receipts: a.receipts,
                            })
                            .collect(),
                    },
                    Err(e) => Reply::Error {
                        message: e.to_string(),
                    },
                }
            }
            Operation::Query { .. }
            | Operation::Estimate { .. }
            | Operation::EstimateTask { .. }
            | Operation::EnvSync { .. }
            | Operation::ResolveToolchains { .. } => {
                unreachable!("read handled before admission")
            }
            Operation::Shutdown => {
                *gate = true;
                let _ = stop.send(true);
                Reply::Stopping
            }
        }
    };
    drop(gate);
    write_message(&mut stream, &reply).await?;
    Ok(())
}
async fn read_message<T: DeserializeOwned>(stream: &mut UnixStream) -> io::Result<T> {
    read_message_with_timeout(stream, IO_TIMEOUT).await
}
async fn read_message_with_timeout<T: DeserializeOwned>(
    stream: &mut UnixStream,
    timeout: Duration,
) -> io::Result<T> {
    tokio::time::timeout(timeout, async {
        let length = stream.read_u32().await? as usize;
        if length == 0 || length > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "parent frame size",
            ));
        }
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "parent request timeout"))?
}
async fn write_message<T: Serialize>(stream: &mut UnixStream, message: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "parent frame size",
        ));
    }
    tokio::time::timeout(IO_TIMEOUT, async {
        stream.write_u32(bytes.len() as u32).await?;
        stream.write_all(&bytes).await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "parent response timeout"))?
}
fn uid() -> u32 {
    unsafe { libc::geteuid() }
}
pub(super) fn private_dir(path: &Path) -> io::Result<()> {
    validate_components(path)?;
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() && m.uid() == uid() && m.permissions().mode() & 0o077 == 0 => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe resident directory",
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            std::fs::DirBuilder::new().mode(0o700).create(path)
        }
        Err(e) => Err(e),
    }
}
pub(crate) fn validate_components(path: &Path) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "resident path must be absolute",
        ));
    }
    let mut current = PathBuf::new();
    for c in path.components() {
        match c {
            Component::RootDir | Component::Normal(_) => current.push(c),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "resident path component",
                ))
            }
        }
        match std::fs::symlink_metadata(&current) {
            Ok(m) if m.file_type().is_symlink() => {
                // Name the refused component and the way out: nothing here is
                // resolved for the caller, so the real path has to be given.
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "symlinked resident path: {} is a symbolic link; give the real path instead (for the state directory, set HOME or XDG_STATE_HOME to the real path)",
                        current.display()
                    ),
                ));
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
pub(super) fn reject_symlinks(path: &Path) -> io::Result<()> {
    for e in std::fs::read_dir(path)? {
        let e = e?;
        if e.file_type()?.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "symlinked resident state",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Open private regular files without following a link or blocking on a FIFO.
pub(super) fn secure_file(path: &Path, writable: bool) -> io::Result<File> {
    validate_components(path)?;
    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe resident file",
        ));
    }
    Ok(file)
}
pub(super) fn check_private_dir(path: &Path) -> io::Result<()> {
    validate_components(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != uid() || meta.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe resident directory",
        ));
    }
    Ok(())
}
