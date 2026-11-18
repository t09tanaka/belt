//! One serial SSH lifecycle per explicitly registered private user-config worker.
use super::*;
use crate::config::resolve::WorkerSpec;
use crate::protocol::ssh::{ssh_argv, RemoteCommand, SshTarget};
use crate::protocol::transport::spawn_child_piped_stderr;
use std::collections::BTreeMap;

/// Re-read only private user registration at startup. Project files cannot
/// redirect a remote peer. Registrations added later reach a running resident
/// only through an explicit, authenticated reload request ([`reload`]).
pub(super) fn snapshot(
    config: &ResolvedConfig,
    env: &BTreeMap<String, String>,
    local_id: &WorkerId,
) -> anyhow::Result<ResolvedConfig> {
    let mut snapshot = config.clone();
    snapshot.workers.clear();
    // A local-only configuration does not enroll SSH peers. In particular,
    // ordinary readable user config need not satisfy private registration's
    // stricter file permissions when no remote worker was configured.
    if config.workers.is_empty() {
        return Ok(snapshot);
    }
    snapshot.workers = registered(env, local_id)?;
    Ok(snapshot)
}

/// The registration as it is now, for a worker added while this resident runs.
/// Unlike startup, an empty startup set does not skip the read.
pub(super) fn reload(
    config: &ResolvedConfig,
    env: &BTreeMap<String, String>,
    local_id: &WorkerId,
) -> anyhow::Result<ResolvedConfig> {
    let mut fresh = config.clone();
    fresh.workers = registered(env, local_id)?;
    Ok(fresh)
}

fn registered(
    env: &BTreeMap<String, String>,
    local_id: &WorkerId,
) -> anyhow::Result<Vec<WorkerSpec>> {
    let path = crate::config::paths::user_config_path(env)?;
    let file = match secure_file(&path, false) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut text = String::new();
    file.take(262_145).read_to_string(&mut text)?;
    anyhow::ensure!(text.len() <= 262_144, "worker registration too large");
    let user = crate::config::resolve::Loaded {
        value: crate::config::parse::parse_user("private worker registration", &text)?,
        path: "private worker registration".into(),
    };
    let registered = crate::config::resolve::resolve(&crate::config::resolve::Inputs {
        overlay: &crate::config::Overlay::default(),
        project: None,
        legacy: None,
        user: Some(&user),
    })?;
    let mut workers = Vec::new();
    for worker in registered.workers {
        let id = WorkerId::parse(&worker.id)?;
        anyhow::ensure!(
            &id != local_id,
            "remote worker identity conflicts with local worker"
        );
        SshTarget::parse(&worker.ssh)?;
        bridge_command(&worker)?;
        workers.push(worker);
    }
    Ok(workers)
}

/// `workers forget`, answered by the resident: the registration says whether
/// the worker is ephemeral, the coordinator whether it is connected.
pub(super) async fn forget(
    coordinator: &Coordinator,
    config: &ResolvedConfig,
    env: &BTreeMap<String, String>,
    local_id: &WorkerId,
    worker: &WorkerId,
) -> Reply {
    use crate::coordinator::identity::ForgetRefusal;
    use crate::error::{ApiError, ErrorCode};
    let (config, env, local_id) = (config.clone(), env.clone(), local_id.clone());
    let fresh = tokio::task::spawn_blocking(move || reload(&config, &env, &local_id)).await;
    let ephemeral = match fresh {
        Ok(Ok(fresh)) => fresh
            .workers
            .iter()
            .find(|registered| registered.id == worker.as_str())
            .map(|registered| registered.ephemeral),
        Ok(Err(e)) => {
            return Reply::ControlError {
                error: ApiError::new(
                    ErrorCode::InvalidInput,
                    format!("worker registration unusable: {e}"),
                ),
            }
        }
        Err(e) => {
            return Reply::ControlError {
                error: ApiError::new(ErrorCode::Internal, e.to_string()),
            }
        }
    };
    match coordinator.forget_worker(worker, ephemeral).await {
        Ok(Ok(forgotten)) => Reply::WorkerForgotten {
            identity_retired: forgotten.identity.is_some(),
            ended: forgotten.ended,
        },
        Ok(Err(refusal)) => Reply::ControlError {
            error: ApiError::new(
                match refusal {
                    ForgetRefusal::NotRegistered => ErrorCode::NotFound,
                    ForgetRefusal::NotEphemeral => ErrorCode::InvalidInput,
                    ForgetRefusal::Connected => ErrorCode::Conflict,
                },
                refusal.message(),
            ),
        },
        Err(e) => Reply::ControlError {
            error: ApiError::new(ErrorCode::Internal, e.to_string()),
        },
    }
}

/// The command run on the worker host: the registered absolute binary, or the
/// bare name for a registration made before the path was recorded.
fn bridge_command(worker: &WorkerSpec) -> anyhow::Result<RemoteCommand> {
    Ok(RemoteCommand::worker_stdio(
        worker.binary.as_deref().unwrap_or("remote-task-runner"),
    )?)
}

/// Owned Child plus a captured process-start identity. The fallback cleanup
/// task owns no coordinator/store and cannot affect any other peer or process.
struct OwnedSsh {
    child: Option<tokio::process::Child>,
    identity: Option<(u32, String)>,
}
impl OwnedSsh {
    fn new(child: tokio::process::Child) -> Self {
        let identity = child.id().filter(|pid| *pid > 1).and_then(|pid| {
            crate::platform::process_start_identity(pid)
                .identity()
                .map(|start| (pid, start.to_owned()))
        });
        Self {
            child: Some(child),
            identity,
        }
    }
    fn signal(&self) {
        if let Some((pid, start)) = &self.identity {
            if crate::platform::process_start_identity(*pid).identity() == Some(start.as_str()) {
                crate::platform::signal_pid(*pid, libc::SIGKILL);
            }
        }
    }
    async fn reap(mut self) {
        self.signal();
        if let Some(mut child) = self.child.take() {
            if tokio::time::timeout(IO_TIMEOUT, child.wait())
                .await
                .is_err()
            {
                tokio::spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}
impl Drop for OwnedSsh {
    fn drop(&mut self) {
        self.signal();
        if let Some(mut child) = self.child.take() {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

/// How a worker is dialed. A resident test replaces it so no real `ssh` runs.
pub(super) type Dial = Arc<
    dyn Fn(
            Arc<ResolvedConfig>,
            WorkerSpec,
            Arc<Coordinator>,
            Arc<CapturedInputs>,
            watch::Receiver<bool>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

pub(super) fn dial() -> Dial {
    Arc::new(|config, worker, coordinator, captured, stop| {
        Box::pin(run(config, worker, coordinator, captured, stop))
    })
}

pub(super) async fn run(
    config: Arc<ResolvedConfig>,
    worker: WorkerSpec,
    coordinator: Arc<Coordinator>,
    captured: Arc<CapturedInputs>,
    stop: watch::Receiver<bool>,
) {
    // Registration was validated before startup. Neither identity nor destination
    // is learned from hello. There is no implicit enrollment fallback.
    let target = SshTarget::parse(&worker.ssh).expect("validated registration");
    let command = bridge_command(&worker).expect("validated registration");
    let argv = ssh_argv(&target, &command);
    run_with_argv(config, worker, argv, coordinator, captured, stop).await;
}

async fn run_with_argv(
    config: Arc<ResolvedConfig>,
    worker: WorkerSpec,
    argv: Vec<String>,
    coordinator: Arc<Coordinator>,
    captured: Arc<CapturedInputs>,
    mut stop: watch::Receiver<bool>,
) {
    let mut delay = Duration::from_millis(200);
    while !*stop.borrow() {
        if session(
            &config,
            &worker,
            &argv,
            coordinator.clone(),
            captured.clone(),
            stop.clone(),
        )
        .await
        .is_err()
        {
            // Neither destination nor peer-controlled diagnostics enter logs.
            eprintln!(
                "remote worker {} unavailable or identity verification failed",
                worker.id
            );
        }
        if *stop.borrow() {
            break;
        }
        tokio::select! { _=stop.changed()=>break, _=tokio::time::sleep(delay)=>{} }
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

async fn session(
    config: &ResolvedConfig,
    worker: &WorkerSpec,
    argv: &[String],
    coordinator: Arc<Coordinator>,
    captured: Arc<CapturedInputs>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let transport = spawn_child_piped_stderr(argv)?;
    let mut child = OwnedSsh::new(transport.child);
    // Constant memory drain, never protocol stdout and never unbounded logs.
    let stderr = child.child.as_mut().and_then(|child| child.stderr.take());
    let drain = tokio::spawn(async move {
        if let Some(mut stderr) = stderr {
            let mut buffer = [0u8; 4096];
            loop {
                if !matches!(stderr.read(&mut buffer).await, Ok(n) if n > 0) {
                    break;
                }
            }
        }
    });
    let connected = tokio::select! {
        _=stop.changed()=>None,
        connected=connect(transport.io, ClientConfig::new(
            coordinator.store().cluster_id().clone(), coordinator.store().coordinator_id().clone(), coordinator.epoch()
        ))=>Some(connected),
    };
    let outcome = async {
        let Some(connected) = connected else { return Ok(()); };
        let connected = connected?;
        let client = connected.client.clone();
        let worker_id = connected.worker.worker_id.clone();
        let gate = Arc::new(crate::coordinator::InputGate::with_transport_policy(format!("ssh-private-rpc-v1:{}",Digest::of_bytes(&serde_json::to_vec(&serde_json::json!({"argv":argv,"protocol":connected.negotiated.version,"features":connected.negotiated.features,"schema":connected.worker.schema_version,"profile":connected.worker.profile_fingerprint,"policy":"source-64k-v1"}))?))));
        gate.observe(connected.worker.worker_boot_id.clone(),connected.worker.agent_epoch).await;
        let link = config.attach_gated_worker(&coordinator, WorkerTarget::Remote(&worker.id), connected, gate.clone())
            .await.map_err(|e| anyhow::anyhow!(e.message))?;
        let (cancel, cancelled) = watch::channel(false);
        let mut jobs = JoinSet::new();
        jobs.spawn(delivery::sources(coordinator.clone(), captured.clone(), client.clone(), worker_id.clone(), gate.clone(), cancelled.clone()));
        jobs.spawn(delivery::secrets(coordinator.clone(), captured, client.clone(), worker_id.clone(), gate.clone(), cancelled));
        let mut probe = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                _=child.child.as_mut().expect("owned child").wait()=>break,
                _=jobs.join_next()=>break,
                _=probe.tick()=> {
                    let alive = tokio::select! { _=stop.changed()=>false, result=client.ping(IO_TIMEOUT)=>result.is_ok() };
                    if !alive { break; }
                }
            }
        }
        // Stop publishers before invalidating their captured generation. A late
        // verify/inspect response cannot repopulate the disconnected session.
        let _ = cancel.send(true);
        gate.invalidate().await;
        coordinator.detach_link(&worker_id, &link).await?;
        while jobs.join_next().await.is_some() {}
        Ok::<(), anyhow::Error>(())
    }.await;
    child.reap().await;
    drain.abort();
    let _ = drain.await;
    outcome
}

#[cfg(test)]
mod tests;
