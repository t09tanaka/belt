//! Construction shared by resident parent and worker entry points.
//! Inputs are explicit: these factories never mutate the process environment.
use super::resolve::{ResolvedConfig, RuntimeSettings};
use crate::coordinator::{ClientLink, Coordinator, CoordinatorError};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{WorkerBootId, WorkerId};
use crate::protocol::session::Connected;
use crate::scheduler::{CostModel, Scheduler};
use crate::setup::builder::SetupConfig;
use crate::storage::CoordinatorStore;
use crate::worker::agent::{Agent, AgentConfig};
use crate::worker::machine::Machine;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

impl RuntimeSettings {
    pub fn start_coordinator(
        &self,
        store: CoordinatorStore,
        costs: Arc<dyn CostModel>,
    ) -> Result<Coordinator, CoordinatorError> {
        let scheduler = Scheduler::new(self.scheduler, costs).map_err(CoordinatorError::Invalid)?;
        Coordinator::start_with(store, self.coordinator, scheduler)
    }
}

/// A local identity is explicit; remote names must match their verified hello.
pub enum WorkerTarget<'a> {
    Local(&'a WorkerId),
    Remote(&'a str),
}

impl ResolvedConfig {
    /// Only a completed protocol handshake can enter this construction path.
    /// Identity mismatches leave both the scheduler and attached links untouched.
    pub async fn attach_worker(
        &self,
        coordinator: &Coordinator,
        target: WorkerTarget<'_>,
        connected: Connected,
    ) -> Result<(), ApiError> {
        self.attach_worker_link(coordinator, target, connected, None)
            .await
            .map(|_| ())
    }

    /// Verify and install a source-gated session in one placement-fenced operation.
    pub async fn attach_gated_worker(
        &self,
        coordinator: &Coordinator,
        target: WorkerTarget<'_>,
        connected: Connected,
        gate: Arc<crate::coordinator::InputGate>,
    ) -> Result<Arc<dyn crate::coordinator::WorkerLink>, ApiError> {
        self.attach_worker_link(coordinator, target, connected, Some(gate))
            .await
    }

    async fn attach_worker_link(
        &self,
        coordinator: &Coordinator,
        target: WorkerTarget<'_>,
        connected: Connected,
        gate: Option<Arc<crate::coordinator::InputGate>>,
    ) -> Result<Arc<dyn crate::coordinator::WorkerLink>, ApiError> {
        let invalid = |message| ApiError::new(ErrorCode::InvalidInput, message);
        let settings = self
            .runtime_settings()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        if &connected.cluster_id != coordinator.store().cluster_id() {
            return Err(invalid("worker hello cluster does not match coordinator"));
        }
        let id = &connected.worker.worker_id;
        let (priority, ephemeral) = match target {
            WorkerTarget::Local(expected) => {
                if !self.local_worker.value || expected != id {
                    return Err(invalid(
                        "local worker identity does not match configuration",
                    ));
                }
                (self.local_worker_priority.value, false)
            }
            WorkerTarget::Remote(name) => {
                let worker = self
                    .workers
                    .iter()
                    .find(|w| w.id == name)
                    .ok_or_else(|| invalid("worker is not configured"))?;
                if worker.id != id.as_str() {
                    return Err(invalid(
                        "worker hello identity does not match configured name",
                    ));
                }
                (worker.priority.value, worker.ephemeral)
            }
        };
        coordinator.scheduler().set_priority(id, priority);
        let timeout = Duration::from_millis(settings.coordinator.worker_budget_ms);
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
        let workflow_artifacts = connected
            .negotiated
            .features
            .iter()
            .any(|f| f == crate::wire::WORKFLOW_ARTIFACTS_FEATURE);
        let matrix_dispatch = connected
            .negotiated
            .features
            .iter()
            .any(|f| f == crate::wire::MATRIX_DISPATCH_FEATURE);
        let discovery = connected
            .negotiated
            .features
            .iter()
            .any(|f| f == crate::wire::CI_DISCOVERY_FEATURE);
        let ci_task = connected
            .negotiated
            .features
            .iter()
            .any(|feature| feature == crate::wire::CI_TASK_FEATURE);
        let attempt_release = connected
            .negotiated
            .features
            .iter()
            .any(|feature| feature == crate::wire::ATTEMPT_RELEASE_FEATURE);
        if let Some(gate) = &gate {
            gate.set_ci_task(ci_task);
            gate.set_ci_discovery(discovery);
        }
        let notice_client = connected.client.clone();
        let link: Arc<dyn crate::coordinator::WorkerLink> = Arc::new(
            (match gate {
                Some(gate) => ClientLink::gated(connected.client, timeout, gate),
                None => ClientLink::new(connected.client, timeout),
            })
            .with_preparation_observations(observations)
            .with_preparation_probe(probe)
            .with_env_sync(env_sync)
            .with_ci_task(ci_task)
            .with_matrix_dispatch(matrix_dispatch)
            .with_ci_discovery(discovery)
            .with_workflow_artifacts(workflow_artifacts)
            .with_attempt_release(attempt_release),
        );
        {
            use crate::coordinator::identity::Admission;
            match coordinator.attach_identity(id.clone(), &connected.worker.worker_boot_id, link.clone(), ephemeral)
                .await.map_err(|error| ApiError::new(ErrorCode::Internal, error.to_string()))? {
                Admission::First | Admission::Same => {}
                Admission::Retired => {
                    // Not attached. The worker is told once more before the
                    // connection goes, so that it stops this member's
                    // attempts now instead of at the end of their lease
                    // (spec 24.3). That it was sent is recorded; that it
                    // arrived is not assumed.
                    crate::coordinator::identity::notify_retired(
                        coordinator,
                        &notice_client,
                        id,
                        &connected.worker.worker_boot_id,
                    )
                    .await;
                    return Err(ApiError::new(ErrorCode::Conflict, "the worker identity was retired"));
                }
                Admission::Conflict => return Err(ApiError::new(ErrorCode::Conflict,
                    "another identity holds this worker name; use workers forget once the previous worker is gone")),
            }
        }
        Ok(link)
    }
}

/// Machine identity, paths and command environment supplied by the caller.
#[derive(Debug, Clone)]
pub struct WorkerInputs {
    pub state_dir: PathBuf,
    pub worker_id: WorkerId,
    pub boot_id: WorkerBootId,
    pub supervisor_exe: PathBuf,
    pub cluster_key_path: PathBuf,
    pub env: BTreeMap<String, String>,
}

/// Settings for both consumers and setup builders, derived together.
pub struct WorkerRuntime {
    agent: AgentConfig,
    setup: SetupConfig,
    env: BTreeMap<String, String>,
    cluster_key_path: PathBuf,
}

impl ResolvedConfig {
    pub fn worker_runtime(
        &self,
        inputs: WorkerInputs,
    ) -> Result<WorkerRuntime, super::ConfigError> {
        let settings = self.runtime_settings()?;
        // The scope of the task-traffic-controller CLI in this environment,
        // never one of the worker's own: one ledger and one budget per user.
        // A role waits for whatever still runs in another scope before it
        // comes here (`service::traffic_hold`); this is the last look, and
        // the only one of a caller that does not wait.
        let traffic = super::traffic::resolve(&inputs.env)
            .and_then(|scope| {
                match super::traffic::hold(&inputs.state_dir, &scope.state_dir).message() {
                    Some(held) => Err(held.to_string()),
                    None => Ok(scope),
                }
            })
            .map_err(|e| super::ConfigError::Invalid { problems: vec![e] })?;
        let mut agent = AgentConfig::new(
            inputs.state_dir.clone(),
            inputs.worker_id.clone(),
            inputs.boot_id,
            inputs.supervisor_exe.clone(),
        );
        agent.group_key_path = inputs.cluster_key_path.clone();
        agent.traffic_state_dir = traffic.state_dir;
        agent.traffic_config = Some(traffic.config_file);
        agent.max_active_task = settings.own_limits.max_active_task;
        agent.prefetch_per_worker = settings.own_limits.prefetch_per_worker;
        agent.forecast_ttl_ms = settings.scheduler.rules.forecast_ttl_ms;
        agent.term_grace_ms = settings.term_grace_ms;
        agent.warmup_idle_ms = settings.warmup_idle_ms;
        agent.log_limit_bytes = settings.log_limit_bytes;
        agent.retention_ms = settings.retention_ms;
        let embedded_bin =
            crate::workspace::embedded::bin(&inputs.state_dir, &inputs.supervisor_exe);
        let profile = crate::toolchain::profile::Profile::load(&inputs.state_dir, &inputs.env)
            .and_then(|profile| profile.with_embedded_bin(embedded_bin.clone()))
            .map_err(|e| super::ConfigError::Invalid { problems: vec![e] })?;
        agent.profile = profile.name().into();
        agent.profile_fingerprint = profile.startup_report().profile_fingerprint;
        let profile_definition = profile.definition_digest().clone();
        let env = profile.environment().clone();
        let scope_env = profile.scope_environment().clone();
        // git and the workspace tools read `XDG_CONFIG_HOME` as a step does.
        let host_env = profile.host_environment();
        agent.capability_profile = Some(Arc::new(profile));
        let mut setup = SetupConfig::new(inputs.state_dir, inputs.worker_id, inputs.supervisor_exe);
        setup.env = env;
        setup.embedded_bin = Some(embedded_bin);
        setup.scope_env = Some(scope_env);
        setup.profile_definition = Some(profile_definition);
        setup.profile_fingerprint = Some(agent.profile_fingerprint.clone());
        setup.profile = format!("{}:{}", agent.profile, agent.profile_fingerprint);
        setup.traffic_state_dir = agent.traffic_state_dir.clone();
        setup.traffic_config = agent.traffic_config.clone();
        setup.lease_ms = settings.coordinator.lease_ms;
        setup.term_grace_ms = agent.term_grace_ms;
        setup.log_limit_bytes = agent.log_limit_bytes;
        setup.kill_wait_ms = agent.kill_wait_ms;
        setup.poll_ms = agent.poll_ms;
        setup.inhibit_sleep = agent.inhibit_sleep;
        Ok(WorkerRuntime {
            agent,
            setup,
            env: host_env,
            cluster_key_path: inputs.cluster_key_path,
        })
    }
}

impl WorkerRuntime {
    pub(crate) fn with_coordinator(mut self, state: PathBuf) -> Self {
        self.agent.coordinator_state = Some(state);
        self
    }

    pub(crate) fn with_instance(mut self, instance: Option<WorkerBootId>) -> Self {
        self.agent.instance_id = instance;
        self
    }

    pub fn agent_config(&self) -> &AgentConfig {
        &self.agent
    }
    pub fn setup_config(&self) -> &SetupConfig {
        &self.setup
    }
    /// Preserve Machine's Git preflight and typed BLOCKED errors.
    pub fn open_host(&self) -> Result<Arc<Machine>, ApiError> {
        self.machine().map(Arc::new)
    }

    fn machine(&self) -> Result<Machine, ApiError> {
        let machine = Machine::open(
            &self.agent.state_dir,
            self.env.clone(),
            self.cluster_key_path.clone(),
        )?;
        crate::workspace::embedded::install(&self.agent.state_dir, &self.agent.supervisor_exe)
            .map_err(|e| {
                ApiError::new(
                    crate::error::ErrorCode::Internal,
                    format!("worker tool links: {e}"),
                )
            })?;
        machine.with_setup(self.setup.clone())
    }

    pub(crate) fn open_host_in_place(
        &self,
        parent_state: PathBuf,
    ) -> Result<Arc<Machine>, ApiError> {
        self.machine().map(|machine| {
            Arc::new(machine.with_in_place(parent_state, self.agent.worker_id.clone()))
        })
    }
    pub(crate) fn start_with_host(self, host: Arc<Machine>) -> Result<Arc<Agent>, ApiError> {
        Agent::start(self.agent, host)
    }

    pub fn start(self) -> Result<Arc<Agent>, ApiError> {
        let host = self.open_host()?;
        Agent::start(self.agent, host)
    }

    /// Keep the enrollment gate continuously held through host preflight and DB open.
    pub(crate) fn start_owned(self, file: std::fs::File) -> Result<Arc<Agent>, ApiError> {
        let owner = crate::worker::agent::OwnedAgentLock::new(self.agent.clone(), file)?;
        let host = self.open_host()?;
        Agent::start_owned(owner, host)
    }
}
