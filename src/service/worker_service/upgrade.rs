//! Switching the installed version on the worker host (`upgrade`, `rollback`).
//!
//! Both are the same resumable sequence towards a target version. The order
//! is: place the binary in its own version directory (an existing file is
//! never overwritten), check that the target can read what is installed,
//! stop the running service once no attempt is live, record the switch, then
//! move the service file, the worker input and `bin/current` to the target
//! and start it. The switch is recorded in the journal before the first
//! change that cannot be undone by doing nothing, so a run that died anywhere
//! is finished by running `upgrade` again or undone by `rollback`.
//!
//! The drain marker is left as it is: the caller resumes the worker after it
//! has checked the new service. Nothing here signals a process.
use super::*;

/// What the parent sends on stdin. `binary_sha256` is the target version:
/// for `upgrade` the binary that runs the action, for `rollback` a version
/// that is already installed.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwitchRequest {
    pub schema: u32,
    pub cluster: ClusterId,
    pub worker_id: WorkerId,
    pub binary_sha256: String,
}

/// An unfinished switch, as the journal records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Switch {
    /// The version that was installed when the switch began.
    pub from: String,
    /// The previous version's service file, until it has been removed.
    pub from_receipt: Option<macos::Receipt>,
}

/// The points of the sequence, in order. A test fails or stops at any of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Place,
    Compat,
    Stop,
    Record,
    RemoveOld,
    Input,
    Link,
    Publish,
    Start,
    Settle,
}
pub const STEPS: [Step; 10] = [
    Step::Place,
    Step::Compat,
    Step::Stop,
    Step::Record,
    Step::RemoveOld,
    Step::Input,
    Step::Link,
    Step::Publish,
    Step::Start,
    Step::Settle,
];
/// Called before each step; an error stops the sequence there.
pub type Hook<'a> = &'a mut dyn FnMut(Step) -> anyhow::Result<()>;

/// 64 lower-case hex digits: the only shape a version directory name has.
pub fn is_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn check_request(journal: &Journal, request: &SwitchRequest) -> anyhow::Result<()> {
    ensure!(request.schema == 1, "unsupported switch request schema");
    ensure!(
        journal.cluster == request.cluster && journal.worker_id == request.worker_id,
        "a different worker is installed here"
    );
    ensure!(
        is_digest(&request.binary_sha256),
        "binary digest is not lower-case hex"
    );
    Ok(())
}

fn absent(path: &Path) -> anyhow::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}

fn read_private(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    resident::secure_file(path, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= LIMIT, "file too large");
    Ok(bytes)
}

/// The worker input a version ran with, kept beside its binary.
fn version_input(paths: &Paths, digest: &str) -> PathBuf {
    paths.config.join("bin").join(digest).join("worker.json")
}

/// The target version must read everything the installed one left: the
/// worker input, the identity binding and the worker database. The database
/// is opened read-only through the schema compatibility table; nothing is
/// migrated or rewritten here.
fn compatible(paths: &Paths, journal: &Journal) -> anyhow::Result<()> {
    let input = super::super::worker::read_input(&paths.input)
        .context("this version cannot read the installed worker input")?;
    ensure!(
        input.schema == 1
            && input.expected_cluster == journal.cluster
            && input.worker_id == journal.worker_id
            && input.state_dir == paths.state,
        "the installed worker input belongs to another worker"
    );
    marker(paths, Duration::ZERO).verify_binding(&identity(journal))?;
    if !absent(&paths.state.join("worker.db"))? {
        let store = crate::storage::WorkerStore::open_read_only(&paths.state)
            .context("this version cannot read the worker database")?;
        ensure!(
            store.worker_id() == &journal.worker_id,
            "the worker database belongs to another worker"
        );
    }
    Ok(())
}

/// A version directory whose binary is still the bytes its name says.
fn installed_version(paths: &Paths, digest: &str) -> anyhow::Result<()> {
    let target = paths.executable(digest);
    ensure!(
        !absent(&target)?,
        "version {digest} is not installed on this host"
    );
    ensure!(
        fs::symlink_metadata(&target)?.is_file() && sha256_file(&target)? == digest,
        "the installed version {digest} differs from its name"
    );
    Ok(())
}

/// `worker-service upgrade`: run by the new binary itself.
pub fn upgrade<L: Launcher>(
    env: &BTreeMap<String, String>,
    request: &SwitchRequest,
    current_exe: &Path,
    launcher: L,
    options: Options,
    hook: Hook,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve(env)?;
    let journal = require_journal(&paths)?;
    check_request(&journal, request)?;
    let digest = sha256_file(current_exe)?;
    ensure!(
        digest == request.binary_sha256,
        "the upgrading binary is not the one the parent sent"
    );
    hook(Step::Place)?;
    place_binary(&paths, &digest, current_exe)?;
    hook(Step::Compat)?;
    compatible(&paths, &journal)?;
    switch_to(&paths, journal, &digest, launcher, options, hook)
}

/// `worker-service rollback`: back to a version that is still installed.
pub fn rollback<L: Launcher>(
    env: &BTreeMap<String, String>,
    request: &SwitchRequest,
    launcher: L,
    options: Options,
    hook: Hook,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve(env)?;
    let journal = require_journal(&paths)?;
    check_request(&journal, request)?;
    installed_version(&paths, &request.binary_sha256)?;
    switch_to(
        &paths,
        journal,
        &request.binary_sha256,
        launcher,
        options,
        hook,
    )
}

/// Stop the service of the version the journal names. It is booted out only
/// after no attempt is live; a state that cannot be told stops nothing.
fn retire<L: Launcher>(
    paths: &Paths,
    journal: &Journal,
    receipt: &macos::Receipt,
    files: DirectoryFiles,
    mut launcher: L,
    wait: Duration,
) -> anyhow::Result<(DirectoryFiles, L)> {
    let plan = plan(paths, journal)?;
    match launcher.loaded_state(&plan, receipt) {
        LoadedState::Unknown => anyhow::bail!(
            "loaded service identity unknown; reconciliation required (nothing stopped)"
        ),
        LoadedState::Absent => {
            marker(paths, wait).request(
                &identity(journal),
                WorkerRequest::Stop {
                    cancel_running: false,
                },
            )?;
            Ok((files, launcher))
        }
        LoadedState::Match => {
            let mut manager = Manager {
                identity: identity(journal),
                files,
                launcher,
                worker: marker(paths, wait),
            };
            manager.stop(&plan, receipt, false)?;
            let Manager {
                files,
                mut launcher,
                ..
            } = manager;
            ensure!(
                launcher.loaded_state(&plan, receipt) == LoadedState::Absent,
                "the service is not confirmed stopped; nothing was switched"
            );
            Ok((files, launcher))
        }
    }
}

/// The input the target version runs with: the bytes it ran with before, or
/// the current input with only the executable changed.
fn input_for(paths: &Paths, digest: &str) -> anyhow::Result<Vec<u8>> {
    let saved = version_input(paths, digest);
    let bytes = if absent(&saved)? {
        let mut spec = super::super::worker::read_input(&paths.input)?;
        spec.executable = paths.executable(digest);
        serde_json::to_vec(&spec)?
    } else {
        read_private(&saved)?
    };
    let spec: super::super::worker::WorkerSpec =
        serde_json::from_slice(&bytes).context("worker input unreadable")?;
    ensure!(
        spec.executable == paths.executable(digest),
        "the kept worker input names another version"
    );
    Ok(bytes)
}

/// Point `bin/current` at `digest` by renaming a new link over the old one.
fn point_current(paths: &Paths, digest: &str) -> anyhow::Result<()> {
    let link = paths.current_link();
    if fs::read_link(&link).ok().as_deref() == Some(Path::new(digest)) {
        return Ok(());
    }
    let directory = link.parent().context("link parent")?;
    let temporary = directory.join(format!(".current.{}", std::process::id()));
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    std::os::unix::fs::symlink(digest, &temporary)?;
    fs::rename(&temporary, &link)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn switch_to<L: Launcher>(
    paths: &Paths,
    mut journal: Journal,
    to: &str,
    mut launcher: L,
    options: Options,
    hook: Hook,
) -> anyhow::Result<Status> {
    let mut files = services(paths)?;
    // A switch that died between recording a new service file and publishing
    // it left a receipt for a file that was never there.
    if journal.switch.is_some() {
        if let Some(receipt) = &journal.receipt {
            if absent(receipt.path())? {
                journal.receipt = None;
                save_journal(paths, &journal)?;
            }
        }
    }
    if journal.binary_sha256 != to {
        if let Some(receipt) = journal.receipt.clone() {
            hook(Step::Stop)?;
            (files, launcher) = retire(paths, &journal, &receipt, files, launcher, options.wait)?;
            // Keep the exact input this version ran with, for a later return.
            publish_exact(
                &version_input(paths, &journal.binary_sha256),
                &read_private(&paths.input)?,
                0o600,
                false,
            )?;
            hook(Step::Record)?;
            let from = match journal.switch.take() {
                Some(switch) => switch.from,
                None => journal.binary_sha256.clone(),
            };
            journal.switch = Some(Switch {
                from,
                from_receipt: Some(receipt),
            });
            journal.receipt = None;
        } else {
            // Only an unfinished switch has no service file; nothing runs.
            ensure!(journal.switch.is_some(), "service was never installed");
            hook(Step::Record)?;
        }
        journal.binary_sha256 = to.to_string();
        save_journal(paths, &journal)?;
    }
    if let Some(old) = journal.switch.as_ref().and_then(|s| s.from_receipt.clone()) {
        hook(Step::RemoveOld)?;
        if !absent(old.path())? {
            let _lease = files.lifecycle_lock()?;
            files.remove(&old)?;
        }
        if let Some(switch) = journal.switch.as_mut() {
            switch.from_receipt = None;
        }
        save_journal(paths, &journal)?;
    }
    hook(Step::Input)?;
    let input = input_for(paths, to)?;
    if read_private(&paths.input)? != input {
        publish_exact(&paths.input, &input, 0o600, true)?;
    }
    hook(Step::Link)?;
    point_current(paths, to)?;
    let plan = plan(paths, &journal)?;
    if journal.receipt.is_none() {
        hook(Step::Publish)?;
        let _lease = files.lifecycle_lock()?;
        files
            .install_journaled(&plan, |receipt, _stage| {
                journal.receipt = Some(receipt.clone());
                save_journal(paths, &journal)
            })
            .map_err(|e| anyhow::anyhow!("service file publication failed ({e})"))?;
    }
    let receipt = journal.receipt.clone().context("service receipt missing")?;
    hook(Step::Start)?;
    clear_cancel(&paths.state)?;
    ensure_started(&mut files, &mut launcher, &plan, &receipt)?;
    let ready = wait_ready(&paths.state, options.ready_timeout);
    hook(Step::Settle)?;
    if journal.switch.take().is_some() {
        save_journal(paths, &journal)?;
    }
    status_with(paths, &journal, &plan, &receipt, &mut launcher, ready)
}

/// The status of a host whose switch has no service file yet: nothing of the
/// target version is registered, so nothing is claimed about a service.
pub(super) fn unsettled_status(paths: &Paths, journal: &Journal) -> anyhow::Result<Status> {
    let switch = journal
        .switch
        .as_ref()
        .context("service was never installed")?;
    let current = Control::effective(&paths.state);
    Ok(Status {
        worker_id: journal.worker_id.clone(),
        cluster: journal.cluster.clone(),
        binary_sha256: journal.binary_sha256.clone(),
        loaded: None,
        ready: false,
        draining: current.drain,
        cancel_running: current.cancel_running,
        live_attempts: live_attempts(&paths.state).ok(),
        bridge_binary: None,
        switching_from: Some(switch.from.clone()),
    })
}

#[cfg(test)]
mod tests;
