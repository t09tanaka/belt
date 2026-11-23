//! Switching the installed version on the worker host (`upgrade`, `rollback`).
//!
//! Both are the same resumable sequence towards a target version. The order
//! is: drain (recording that this upgrade did it), place the binary in its
//! own version directory (an existing file is never overwritten), check that
//! the target can read what is installed, stop the running service once no
//! attempt is live, save the worker database, record the switch, then move
//! the service file, the worker input and `bin/current` to the target and
//! start it. The switch is recorded in the journal before the first change
//! that cannot be undone by doing nothing, so a run that died anywhere is
//! finished by running `upgrade` again or undone by `rollback`.
//!
//! The record of the switch outlives the start of the new version. Only
//! `commit`, which the parent sends after it has checked the new version,
//! forgets the previous one, deletes the saved database and resumes a drain
//! this upgrade made. Until then `rollback` returns to the previous version
//! and puts the saved database back before it starts, so a migration by the
//! new version does not strand the old one. After `commit` there is nothing
//! to roll back to.
//!
//! Every action here holds the host's switch lock from before it reads the
//! journal until it has answered. Nothing here signals a process.
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
    /// The worker database as `from` left it is kept beside that version.
    /// False when no database existed then.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub database_saved: bool,
}

/// The points of the sequence, in order. A test fails or stops at any of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Drain,
    Place,
    Compat,
    Stop,
    Backup,
    Record,
    RemoveOld,
    Restore,
    Input,
    Link,
    Publish,
    Start,
    Settle,
}
pub const STEPS: [Step; 13] = [
    Step::Drain,
    Step::Place,
    Step::Compat,
    Step::Stop,
    Step::Backup,
    Step::Record,
    Step::RemoveOld,
    Step::Restore,
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
    let paths = Paths::resolve_for(env, launcher.backend())?;
    let _lock = exclusive_lock(&paths)?;
    let mut journal = require_journal(&paths)?;
    check_request(&journal, request)?;
    let digest = sha256_file(current_exe)?;
    ensure!(
        digest == request.binary_sha256,
        "the upgrading binary is not the one the parent sent"
    );
    hook(Step::Drain)?;
    own_drain(&paths, &mut journal)?;
    hook(Step::Place)?;
    let created = absent(&paths.config.join("bin").join(&digest))?;
    place_binary(&paths, &digest, current_exe)?;
    hook(Step::Compat)?;
    if let Err(e) = compatible(&paths, &journal) {
        // Spec 25.7 (a): a copy the version check refused goes at once.
        if let Ok(places) = crate::service::old_binaries::Places::of(env, launcher.backend()) {
            crate::service::old_binaries::remove_refused(
                &places,
                &digest,
                created,
                crate::service::old_binaries::Holds::Switch,
            );
        }
        return Err(e);
    }
    switch_to(&paths, journal, &digest, launcher, options, hook)
}

/// Drain for the upgrade and say so in the journal, unless the worker is
/// draining already: that drain is the operator's and stays theirs. The
/// journal is written first, so a drain is never left without its owner.
fn own_drain(paths: &Paths, journal: &mut Journal) -> anyhow::Result<()> {
    marker(paths, Duration::ZERO).verify_binding(&identity(journal))?;
    let draining = Control::effective(&paths.state).drain;
    if !journal.drained_by_upgrade {
        if draining {
            return Ok(());
        }
        journal.drained_by_upgrade = true;
        save_journal(paths, journal)?;
    }
    if !draining {
        control::write(&paths.state, Control::draining())?;
    }
    Ok(())
}

/// Where the worker database of `digest` is kept while a switch away from
/// it is not committed.
fn saved_database(paths: &Paths, digest: &str) -> PathBuf {
    paths
        .config
        .join("bin")
        .join(digest)
        .join("worker.db.saved")
}

/// Move the write-ahead log into the database file. The service is stopped,
/// so nothing else has the database open; if something does, say so rather
/// than copy or replace a file that is in use.
pub(crate) fn checkpoint(database: &Path) -> anyhow::Result<()> {
    ensure!(
        fs::symlink_metadata(database)?.is_file(),
        "the worker database is not a regular file"
    );
    let connection = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let busy: i64 =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    ensure!(
        busy == 0,
        "the worker database is in use by another process"
    );
    Ok(())
}

pub(crate) fn remove_if_present(path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Copy `from` to `to` through a private temporary file and a rename.
pub(crate) fn copy_private(from: &Path, to: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let directory = to.parent().context("file parent")?;
    let temporary = crate::platform::owned_temp::file_in(directory)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    let mut source = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(from)?;
    std::io::copy(&mut source, &mut temporary.as_file())?;
    temporary.as_file().sync_all()?;
    temporary.persist(to).map_err(|e| e.error)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

/// Keep the stopped version's database beside its binary. Returns whether
/// there was one. A copy left by an earlier, unrecorded attempt is replaced.
fn save_database(paths: &Paths, digest: &str) -> anyhow::Result<bool> {
    let database = paths.state.join("worker.db");
    let saved = saved_database(paths, digest);
    if absent(&database)? {
        remove_if_present(&saved)?;
        return Ok(false);
    }
    // No deletion (a `belt group forget`, the agent's stage 3 passes) runs
    // between this copy and the trial that keeps them out until the switch
    // is committed or undone: the copy may be put back (spec 25.1 rule 8).
    let _forget =
        crate::storage::ProcessLock::acquire(&paths.state, crate::service::group::FORGET_LOCK)?;
    checkpoint(&database)?;
    copy_private(&database, &saved)?;
    crate::worker::control::begin_trial(&paths.state)?;
    Ok(true)
}

/// Put the database `digest` left back in place. The copy is kept until the
/// switch is over, so running this again gives the same result.
fn restore_database(paths: &Paths, digest: &str) -> anyhow::Result<()> {
    let database = paths.state.join("worker.db");
    let saved = saved_database(paths, digest);
    ensure!(
        !absent(&saved)?,
        "the worker database saved for version {digest} is gone, so that version cannot be restored; run upgrade again to finish the switch"
    );
    if !absent(&database)? {
        checkpoint(&database)?;
    }
    // A log of the replaced database must never be replayed into this one.
    remove_if_present(&paths.state.join("worker.db-wal"))?;
    remove_if_present(&paths.state.join("worker.db-shm"))?;
    let _forget =
        crate::storage::ProcessLock::acquire(&paths.state, crate::service::group::FORGET_LOCK)?;
    copy_private(&saved, &database)
}

/// `worker-service rollback`: back to the version an uncommitted switch
/// began from. Without such a switch only the installed version itself can
/// be named (it is started again); a committed switch cannot be undone.
pub fn rollback<L: Launcher>(
    env: &BTreeMap<String, String>,
    request: &SwitchRequest,
    launcher: L,
    options: Options,
    hook: Hook,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve_for(env, launcher.backend())?;
    let _lock = exclusive_lock(&paths)?;
    let journal = require_journal(&paths)?;
    check_request(&journal, request)?;
    let origin = journal
        .switch
        .as_ref()
        .map_or(journal.binary_sha256.as_str(), |switch| {
            switch.from.as_str()
        });
    ensure!(
        origin == request.binary_sha256,
        "no uncommitted switch from version {} is recorded; a committed upgrade cannot be rolled back",
        request.binary_sha256
    );
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

/// The reason a stop did not finish, in the switch's own words: while
/// attempts are live nothing was switched (the caller decides about the
/// drain marker, so the stop command's advice does not apply).
fn stop_failure(live: Option<u32>, error: anyhow::Error) -> anyhow::Error {
    match live {
        Some(live) if live > 0 => {
            anyhow::anyhow!("{live} attempt(s) are still live after waiting; nothing was switched")
        }
        _ => error,
    }
}

/// Stop the service of the version the journal names. It is booted out only
/// after no attempt is live; a state that cannot be told stops nothing.
fn retire<L: Launcher>(
    paths: &Paths,
    journal: &Journal,
    receipt: &macos::Receipt,
    files: system_user::ServiceFiles,
    mut launcher: L,
    wait: Duration,
) -> anyhow::Result<(system_user::ServiceFiles, L)> {
    let plan = plan(paths, journal)?;
    match launcher.loaded_state(&plan, receipt) {
        LoadedState::Unknown => anyhow::bail!(
            "loaded service identity unknown; reconciliation required (nothing stopped)"
        ),
        LoadedState::Absent => {
            marker(paths, wait)
                .request(
                    &identity(journal),
                    WorkerRequest::Stop {
                        cancel_running: false,
                    },
                )
                .map_err(|e| stop_failure(live_attempts(&paths.state).ok(), e))?;
            Ok((files, launcher))
        }
        LoadedState::Match => {
            let mut manager = Manager {
                identity: identity(journal),
                files,
                launcher,
                worker: marker(paths, wait),
            };
            manager
                .stop(&plan, receipt, false)
                .map_err(|e| stop_failure(live_attempts(&paths.state).ok(), e))?;
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
    // A name that proves its owner (spec 25.4 (a)).
    crate::platform::owned_temp::sweep_files(directory);
    let temporary = directory.join(crate::platform::owned_temp::new_file_name());
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
    let mut files = services(paths, &journal)?;
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
            // The database is saved once, as the version the switch began
            // from left it. A recorded switch keeps that copy: what is in
            // place now may already be the newer version's.
            let (from, database_saved) = match journal.switch.take() {
                Some(switch) => (switch.from, switch.database_saved),
                None => {
                    hook(Step::Backup)?;
                    let saved = save_database(paths, &journal.binary_sha256)?;
                    (journal.binary_sha256.clone(), saved)
                }
            };
            hook(Step::Record)?;
            journal.switch = Some(Switch {
                from,
                from_receipt: Some(receipt),
                database_saved,
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
        if journal.system_user.is_none() && !absent(old.path())? {
            let _lease = files.lifecycle_lock()?;
            files.remove(&old)?;
        }
        if let Some(switch) = journal.switch.as_mut() {
            switch.from_receipt = None;
        }
        save_journal(paths, &journal)?;
    }
    // Returning to the version the switch began from: its database goes back
    // before it starts. Without a service file nothing of either version runs.
    if journal.receipt.is_none()
        && journal
            .switch
            .as_ref()
            .is_some_and(|switch| switch.from == to && switch.database_saved)
    {
        hook(Step::Restore)?;
        restore_database(paths, to)?;
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
    // Back where the switch began, nothing is left to return to. A switch to
    // any other version stays recorded until the parent commits it.
    if journal.switch.as_ref().is_some_and(|s| s.from == to) {
        remove_if_present(&saved_database(paths, to))?;
        journal.switch = None;
        save_journal(paths, &journal)?;
        crate::worker::control::end_trial(&paths.state)?;
    }
    status_with(paths, &journal, &plan, &receipt, &mut launcher, ready)
}

/// `worker-service commit`: the parent has checked the installed version.
/// Forget the version the switch began from, delete its saved database and
/// resume a drain the upgrade made. Running it again changes nothing more.
pub fn commit<L: Launcher>(
    env: &BTreeMap<String, String>,
    request: &SwitchRequest,
    mut launcher: L,
) -> anyhow::Result<Status> {
    let paths = Paths::resolve_for(env, launcher.backend())?;
    let _lock = exclusive_lock(&paths)?;
    let mut journal = require_journal(&paths)?;
    check_request(&journal, request)?;
    ensure!(
        journal.binary_sha256 == request.binary_sha256,
        "the host is on version {}, not the one to commit",
        journal.binary_sha256
    );
    let receipt = journal
        .receipt
        .clone()
        .context("the version switch is unfinished; nothing was committed")?;
    if let Some(switch) = journal.switch.take() {
        // The copy goes first: a switch that is still recorded without it
        // refuses to roll back instead of starting on a missing database.
        remove_if_present(&saved_database(&paths, &switch.from))?;
        save_journal(&paths, &journal)?;
        crate::worker::control::end_trial(&paths.state)?;
    }
    if journal.drained_by_upgrade {
        // The marker first: a crash in between leaves the ownership recorded
        // and the next commit finishes, never a drain nobody owns.
        marker(&paths, Duration::ZERO).request(&identity(&journal), WorkerRequest::Resume)?;
        journal.drained_by_upgrade = false;
        save_journal(&paths, &journal)?;
    }
    // Spec 25.7 (a): this host's commit removes the versions nothing names.
    // The upgrade is committed: a failure here is not the commit's.
    match crate::service::old_binaries::Places::of(env, launcher.backend()) {
        Ok(places) => {
            let config = paths.config.clone();
            crate::service::old_binaries::sweep(
                &places,
                &mut || crate::service::init::upgrade::guard_present(&config, &mut launcher),
                true,
            );
        }
        Err(e) => eprintln!("belt: the old versions were not looked at: {e:#}"),
    }
    let plan = plan(&paths, &journal)?;
    let ready = agent_reachable(&paths.state);
    status_with(&paths, &journal, &plan, &receipt, &mut launcher, ready)
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
        membership_only: false,
        coordinator_id: None,
        system_user: journal.system_user.is_some(),
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
        drained_by_upgrade: journal.drained_by_upgrade,
        switch_running: false,
        // Nothing of the target version is registered: no scope is claimed.
        traffic_scope: None,
        traffic_hold: None,
    })
}

#[cfg(test)]
mod tests;
