//! Explicit enrollment. Secrets are read from the install request, never journaled.
use super::*;
use crate::storage::WorkerStore;

pub(super) fn pin(state: &Path, request: &InstallRequest) -> anyhow::Result<()> {
    let member = request.membership.as_ref().context("membership missing")?;
    WorkerStore::open(state, &request.worker_id)?
        .for_member(&member.caller_id)
        .replace_member_pin(member.caller_name.as_str(), crate::platform::wall_ms())?;
    Ok(())
}

fn work(state: &Path) -> anyhow::Result<u64> {
    if !state.join("worker.db").try_exists()? {
        return Ok(0);
    }
    Ok(WorkerStore::open_read_only(state)?.count_group_work()?)
}

pub(super) fn existing<L: Launcher>(
    paths: &Paths,
    journal: &Journal,
    request: &InstallRequest,
    mut launcher: L,
    ready_timeout: Duration,
) -> anyhow::Result<Status> {
    let member = request.membership.as_ref().context("membership missing")?;
    let moving = journal.cluster != request.cluster;
    check_same_group_key(&paths.key, request, moving)?;
    ensure!(
        !moving || member.move_group,
        "target belongs to group {}; use --move to move an idle member",
        journal.cluster
    );
    ensure!(
        !moving || work(&paths.state)? == 0,
        "target belongs to group {} and has live work or uncollected results; cannot move",
        journal.cluster
    );
    ensure!(
        journal.worker_id == request.worker_id,
        "target member is named {}; use that name",
        journal.worker_id
    );
    ensure!(
        journal.system_user.is_some() == request.system_user,
        "installed service mode differs from the worker registration"
    );
    ensure!(
        journal.switch.is_none(),
        "finish the version switch before changing membership"
    );
    let plan = plan(paths, journal)?;
    let receipt = journal
        .receipt
        .as_ref()
        .context("service receipt missing")?;
    let mut files = services(paths, journal)?;
    let _lease = files.lifecycle_lock()?;
    files.verify(receipt)?;
    let loaded = launcher.loaded_state(&plan, receipt);
    ensure!(
        loaded != LoadedState::Unknown,
        "loaded service identity unknown"
    );
    let previous_control = control::read(&paths.state)?;
    let mut manager = Manager {
        identity: identity(journal),
        files,
        launcher,
        worker: marker(paths, Duration::from_secs(1)),
    };
    // Set only while this handover has stopped a running service, so a rerun
    // after a failure starts it again; a service the user stopped stays down.
    let restart_marker = paths.state.join("membership-restart");
    let resuming = restart_marker.try_exists()?;
    let mut stopped_here = false;
    let result = (|| -> anyhow::Result<_> {
        if moving && loaded == LoadedState::Match {
            publish_exact(&restart_marker, b"1", 0o600, true)?;
            stopped_here = true;
            manager.stop(&plan, receipt, false)?;
            ensure!(
                manager.launcher.loaded_state(&plan, receipt) == LoadedState::Absent,
                "service did not stop; group unchanged"
            );
        }
        // The stopped agent cannot accept an old-group submit during the move.
        let _agent = if moving {
            Some(crate::storage::ProcessLock::acquire(
                &paths.state,
                "agent.lock",
            )?)
        } else {
            None
        };
        if moving && work(&paths.state)? != 0 {
            control::write(&paths.state, previous_control)?;
            drop(_agent);
            if loaded == LoadedState::Match {
                ensure_started(&mut manager.files, &mut manager.launcher, &plan, receipt)?;
            }
            anyhow::bail!("live work appeared before stopping; group unchanged");
        }
        let store = WorkerStore::open(&paths.state, &request.worker_id)?;
        if moving {
            store.clear_group_membership()?;
            match fs::remove_file(crate::group::next_key_path(&paths.key)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        publish_exact(&paths.key, request.key_file_text.as_bytes(), 0o600, true)?;
        let key = crate::secrets::key::load(&paths.key)?;
        store.observe_group_key(&crate::group::fingerprint(&key))?;
        pin(&paths.state, request)?;
        // Enrollment is the sole explicit replacement of the durable binding.
        publish_exact(
            &paths.state.join("worker-binding.json"),
            &serde_json::to_vec(&(
                1,
                &request.cluster,
                &request.worker_id,
                format!("{:x}", Sha256::digest(request.key_file_text.as_bytes())),
            ))?,
            0o600,
            true,
        )?;
        let mut journal = read_journal(paths)?.context("service journal missing")?;
        if moving {
            let mut spec: super::super::worker::WorkerSpec =
                serde_json::from_reader(resident::secure_file(&paths.input, false)?)?;
            spec.expected_cluster = request.cluster.clone();
            publish_exact(&paths.input, &serde_json::to_vec(&spec)?, 0o600, true)?;
            journal.cluster = request.cluster.clone();
            save_journal(paths, &journal)?;
            control::write(&paths.state, previous_control)?;
        }
        drop(_agent);
        let ready = if stopped_here || resuming {
            ensure_started(&mut manager.files, &mut manager.launcher, &plan, receipt)?;
            manager.launcher.verify_loaded(&plan, receipt)?;
            let ready = wait_ready(&paths.state, ready_timeout);
            ensure!(
                ready,
                "worker service readiness unconfirmed; retry membership handover"
            );
            remove_marker(&restart_marker)?;
            ready
        } else {
            agent_reachable(&paths.state)
        };
        let mut status = status_with(
            paths,
            &journal,
            &plan,
            receipt,
            &mut manager.launcher,
            ready,
        )?;
        status.membership_only = true;
        Ok(status)
    })();
    if let Err(error) = result {
        let restore = control::write(&paths.state, previous_control);
        let restart = if stopped_here || resuming {
            ensure_started(&mut manager.files, &mut manager.launcher, &plan, receipt)
        } else {
            Ok(())
        };
        return match restore.map_err(anyhow::Error::from).and(restart) {
            Ok(()) => Err(error),
            Err(rollback) => Err(anyhow::anyhow!(
                "{error}; membership restart failed: {rollback}"
            )),
        };
    }
    result
}

/// Same-group enrollment must not replace a newer key with the caller's key.
pub(crate) fn check_same_group_key(
    path: &Path,
    request: &InstallRequest,
    moving: bool,
) -> anyhow::Result<()> {
    if !moving {
        let bytes = hex_decode(request.key_file_text.trim_end_matches('\n'))
            .context("cluster key is not lower-case hex")?;
        let incoming = crate::secrets::key::ClusterKey::from_bytes(bytes)?;
        let current = crate::secrets::key::load(path)?;
        ensure!(crate::group::fingerprint(&current) == crate::group::fingerprint(&incoming),
            "same-group key differs; run `belt group send-key <member>` from a member that holds the current key");
    }
    Ok(())
}

fn remove_marker(path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
