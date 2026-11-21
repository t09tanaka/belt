//! `belt upgrade`: this machine's resident (one binary, both roles) moves to
//! a new version, and goes back to the previous one with both of its
//! databases unless the CLI that ran the upgrade commits in time (§24.8).
//!
//! The order is: place the upgrade hold on the worker (new attempts are
//! refused as in a drain, and `workers resume` does not lift it), wait until
//! the worker has no live attempt, place the new binary in its own version
//! directory (the running one is never overwritten), check that this version
//! reads both databases, keep the managed journal and input, record the trial
//! with its deadline, register the guard, stop the resident, keep both
//! databases as they are at that one moment, switch the managed files and
//! `bin/resident` to the new version and start it. The CLI then checks its
//! health and commits, which lifts the hold.
//!
//! The guard is the service entry that watches the deadline: a job of the
//! same service manager that runs the previous version's binary, which the
//! upgrade never changes, registered before anything is switched. When the
//! trial is not committed by the deadline (the CLI failed or died, or the new
//! version does not answer), the guard stops whatever runs under the
//! resident's name, puts both databases back together, restores the managed
//! files and `bin/resident`, and starts the previous version. Commit and
//! rollback decide under one lock on one record, so exactly one of them
//! happens; after the commit nothing is rolled back.
use super::*;
use crate::service::worker_service::{self as ws, upgrade as wsu};
use crate::worker::control;
#[cfg(test)]
use crate::worker::control::Control;
use std::os::unix::fs::PermissionsExt;

/// How long a trial waits for its commit. Shorter than one lease period (60 s
/// by default), so an attempt the new version adopted is still leased when
/// the previous version adopts it again.
pub const COMMIT_DEADLINE: Duration = Duration::from_secs(45);
/// The health check of the new version never waits past this.
const READY: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// How long accepted work may take to finish before nothing is switched.
    pub wait: Duration,
    /// From the record of the trial to the guard's rollback.
    pub deadline: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    /// The new version may be running; it is not committed.
    Trial,
    /// A rollback has begun: nothing commits any more.
    Restoring,
    /// Both databases and the managed files are back; the previous version
    /// is started and the record removed.
    Restored,
}

/// The one record commit and rollback decide on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema: u32,
    from: String,
    to: String,
    deadline_ms: u64,
    /// Both databases were kept after the resident stopped.
    backed_up: bool,
    phase: Phase,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Upgraded {
    pub from: String,
    pub to: String,
}

/// A switched, uncommitted version, as the CLI holds it until it commits.
pub struct Trial {
    paths: Paths,
    /// The journal of the version the trial began from.
    from: Journal,
    /// The input the new version runs with.
    pub spec: Spec,
}

fn upgrade_dir(paths: &Paths) -> PathBuf {
    paths.config.join("upgrade")
}
fn record_path(paths: &Paths) -> PathBuf {
    upgrade_dir(paths).join("record.json")
}
fn kept(paths: &Paths, name: &str) -> PathBuf {
    upgrade_dir(paths).join(name)
}
const KEPT_JOURNAL: &str = "init.json";
const KEPT_INPUT: &str = "coordinator.json";
const KEPT_PARENT_DB: &str = "coordinator.db";
const KEPT_WORKER_DB: &str = "worker.db";

/// Commit and rollback both hold this while they read and change the record.
/// It is never held while the new version is checked, so a CLI that waits on
/// an unresponsive version cannot keep the guard from rolling back.
fn upgrade_lock(paths: &Paths) -> anyhow::Result<File> {
    let path = paths.config.join("upgrade.lock");
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
    {
        Ok(f) => f.sync_all()?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let lock = resident::secure_file(&path, true)?;
    lock.lock()?;
    Ok(lock)
}

fn read_record(paths: &Paths) -> anyhow::Result<Option<Record>> {
    let path = record_path(paths);
    if !path.try_exists()? {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    resident::secure_file(&path, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= LIMIT, "upgrade record too large");
    let record: Record = serde_json::from_slice(&bytes).context("upgrade record unreadable")?;
    ensure!(record.schema == 1, "unsupported upgrade record schema");
    Ok(Some(record))
}

fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let directory = path.parent().context("file parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|e| e.error)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn write_record(paths: &Paths, record: &Record) -> anyhow::Result<()> {
    write_private(&record_path(paths), &serde_json::to_vec(record)?)
}

/// The same journal, naming version `to`.
fn retargeted(paths: &Paths, j: &Journal, to: &str) -> Journal {
    let mut j = j.clone();
    j.binary_hash = to.to_string();
    j.spec.executable = paths.config.join("bin").join(to).join("belt");
    j
}

/// Read again the proofs of files that were just replaced.
fn refresh(j: &mut Journal, replaced: &[&Path]) -> anyhow::Result<()> {
    for proof in j.files.iter_mut() {
        if replaced.contains(&proof.path.as_path()) {
            *proof = Proof::read(&proof.path)?;
        }
    }
    Ok(())
}

/// Replace the journal in place; the copy kept for the trial is not touched.
fn replace_journal(paths: &Paths, j: &Journal) -> anyhow::Result<()> {
    let mut file = JournalFile {
        path: paths.journal.clone(),
        proof: Some(Proof::read(&paths.journal)?),
    };
    file.save(j)
}

fn put_input(paths: &Paths, spec: &Spec) -> anyhow::Result<()> {
    let stage = stage_path(&paths.input);
    bootstrap::write_input(&stage, spec)?;
    fs::rename(&stage, &paths.input)?;
    File::open(&paths.config)?.sync_all()?;
    Ok(())
}

/// Replace the resident's service file with the one `plan` renders. Nothing
/// may be loaded under its name while this runs.
fn put_service(paths: &Paths, plan: &macos::policy::Plan) -> anyhow::Result<macos::Receipt> {
    let mut files = DirectoryFiles::open(&paths.services)?;
    let _lease = files.lifecycle_lock()?;
    wsu::remove_if_present(&paths.services.join(plan.filename()))?;
    File::open(&paths.services)?.sync_all()?;
    files
        .install_journaled(plan, |_, _| Ok(()))
        .map_err(|e| anyhow::anyhow!("service file publication failed ({e})"))
}

/// The guard's job: the previous version's binary, under the resident's name
/// with `.upgrade` added.
fn guard_plan(paths: &Paths, from: &Journal) -> anyhow::Result<macos::policy::Plan> {
    let mut definition = definition(paths, from)?;
    definition.label.push_str(".upgrade");
    definition.arguments = vec![
        "upgrade-guard".into(),
        "--config".into(),
        paths
            .config
            .to_str()
            .context("configuration path must be UTF-8")?
            .into(),
    ];
    render(paths, definition)
}

fn install_guard<L: Launcher>(
    paths: &Paths,
    from: &Journal,
    receipt: &macos::Receipt,
    launcher: &mut L,
) -> anyhow::Result<()> {
    let plan = guard_plan(paths, from)?;
    // A guard left by an earlier, finished upgrade goes first.
    remove_guard(paths, &plan, receipt, launcher)?;
    let mut files = DirectoryFiles::open(&paths.services)?;
    let guard = {
        let _lease = files.lifecycle_lock()?;
        files
            .install_journaled(&plan, |_, _| Ok(()))
            .map_err(|e| anyhow::anyhow!("upgrade guard publication failed ({e})"))?
    };
    launcher
        .launchctl(
            &plan
                .start_arguments(guard.path())
                .map_err(anyhow::Error::msg)?,
        )
        .map_err(|e| {
            anyhow::anyhow!("the upgrade guard did not start ({e}); nothing was switched")
        })?;
    ensure!(
        launcher.loaded_state(&plan, &guard) == LoadedState::Match,
        "the upgrade guard is not confirmed running; nothing was switched"
    );
    Ok(())
}

/// The file goes first and the job is stopped by name last, so a guard that
/// removes itself is stopped as its final step.
fn remove_guard<L: Launcher>(
    paths: &Paths,
    plan: &macos::policy::Plan,
    receipt: &macos::Receipt,
    launcher: &mut L,
) -> anyhow::Result<()> {
    {
        let mut files = DirectoryFiles::open(&paths.services)?;
        let _lease = files.lifecycle_lock()?;
        wsu::remove_if_present(&paths.services.join(plan.filename()))?;
        File::open(&paths.services)?.sync_all()?;
    }
    if launcher.loaded_state(plan, receipt) != LoadedState::Absent {
        launcher.launchctl(&plan.stop_by_name_arguments())?;
    }
    Ok(())
}

/// Stop whatever version runs under the resident's name and confirm it.
fn stop_resident<L: Launcher>(
    plan: &macos::policy::Plan,
    receipt: &macos::Receipt,
    launcher: &mut L,
) -> anyhow::Result<()> {
    if launcher.loaded_state(plan, receipt) != LoadedState::Absent {
        launcher.launchctl(&plan.stop_by_name_arguments())?;
    }
    ensure!(
        launcher.loaded_state(plan, receipt) == LoadedState::Absent,
        "the resident is not confirmed stopped; nothing was restored"
    );
    Ok(())
}

/// Keep a stopped database as it is now. Returns whether there was one.
fn keep_database(live: &Path, copy: &Path) -> anyhow::Result<bool> {
    wsu::remove_if_present(copy)?;
    if !live.try_exists()? {
        return Ok(false);
    }
    wsu::checkpoint(live)?;
    wsu::copy_private(live, copy)?;
    Ok(true)
}

/// Put back the database kept before the switch; a database that did not
/// exist then does not exist after.
fn restore_database(live: &Path, copy: &Path) -> anyhow::Result<()> {
    if live.try_exists()? {
        wsu::checkpoint(live)?;
    }
    // A log of the replaced database must never be replayed into this one.
    let name = live.file_name().context("database name")?.to_owned();
    let mut wal = name.clone();
    wal.push("-wal");
    let mut shm = name;
    shm.push("-shm");
    let directory = live.parent().context("database parent")?;
    wsu::remove_if_present(&directory.join(wal))?;
    wsu::remove_if_present(&directory.join(shm))?;
    if copy.try_exists()? {
        wsu::copy_private(copy, live)
    } else {
        wsu::remove_if_present(live)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

/// The record goes first: from then on the trial is over, and what is left
/// is only kept copies. Then the hold is lifted. Running it again, or with
/// the record already gone, finishes what a crash left.
fn finish(paths: &Paths) -> anyhow::Result<()> {
    wsu::remove_if_present(&record_path(paths))?;
    if upgrade_dir(paths).try_exists()? {
        File::open(upgrade_dir(paths))?.sync_all()?;
    }
    for name in [KEPT_JOURNAL, KEPT_INPUT, KEPT_PARENT_DB, KEPT_WORKER_DB] {
        wsu::remove_if_present(&kept(paths, name))?;
    }
    control::end_trial(&paths.worker)?;
    control::lift_hold(&paths.worker)?;
    Ok(())
}

/// Back to the version the trial began from, with both databases as they
/// were kept. Returns false when there is no trial (it was committed or is
/// already rolled back). Every step can run again after a crash.
fn rollback<L: Launcher>(paths: &Paths, launcher: &mut L) -> anyhow::Result<bool> {
    let _lock = upgrade_lock(paths)?;
    let Some(mut record) = read_record(paths)? else {
        return Ok(false);
    };
    let from = read_journal(&kept(paths, KEPT_JOURNAL))?;
    ensure!(
        from.binary_hash == record.from,
        "the kept journal names another version"
    );
    let plan = plan(paths, &from)?;
    let named = from.plist.clone().context("service receipt missing")?;
    if record.phase == Phase::Trial {
        record.phase = Phase::Restoring;
        write_record(paths, &record)?;
    }
    if record.phase == Phase::Restoring {
        stop_resident(&plan, &named, launcher)?;
        let mut j = from.clone();
        if record.backed_up {
            // Never under a coordinator that still runs.
            let _owner = crate::storage::ProcessLock::acquire(&paths.state, "coordinator.lock")?;
            restore_database(
                &paths.state.join("coordinator.db"),
                &kept(paths, KEPT_PARENT_DB),
            )?;
            restore_database(
                &paths.worker.join("worker.db"),
                &kept(paths, KEPT_WORKER_DB),
            )?;
            if let Some(database) = j.database.as_mut() {
                *database = Proof::read(&database.path)?;
            }
        }
        wsu::copy_private(&kept(paths, KEPT_INPUT), &paths.input)?;
        let receipt = put_service(paths, &plan)?;
        refresh(&mut j, &[&paths.input, receipt.path()])?;
        j.plist = Some(receipt);
        replace_journal(paths, &j)?;
        link_resident(&j.spec.executable, &mut NativeDurability)?;
        record.phase = Phase::Restored;
        write_record(paths, &record)?;
    }
    let j = read_journal(&paths.journal)?;
    let receipt = j.plist.clone().context("service receipt missing")?;
    if launcher.loaded_state(&plan, &receipt) != LoadedState::Match {
        launcher.launchctl(
            &plan
                .start_arguments(receipt.path())
                .map_err(anyhow::Error::msg)?,
        )?;
    }
    finish(paths)?;
    Ok(true)
}

/// Undo a trial that failed in the CLI, then remove the guard.
fn abandon<L: Launcher>(
    paths: &Paths,
    from: &Journal,
    launcher: &mut L,
    error: anyhow::Error,
) -> anyhow::Error {
    let named = from.plist.clone();
    let undone = rollback(paths, launcher).and_then(|_| {
        let guard = guard_plan(paths, from)?;
        remove_guard(
            paths,
            &guard,
            &named.context("service receipt missing")?,
            launcher,
        )
    });
    match undone {
        Ok(()) => anyhow::anyhow!(
            "{error:#}; version {} and both databases were restored",
            from.binary_hash
        ),
        Err(undo) => anyhow::anyhow!(
            "{error:#}; the rollback did not finish ({undo:#}); the upgrade guard rolls back by the deadline"
        ),
    }
}

fn identity_checked(paths: &Paths, j: &Journal) -> anyhow::Result<()> {
    let store = crate::storage::CoordinatorStore::open_read_only(&paths.state)
        .context("this version cannot read the parent database")?;
    ensure!(
        Some(store.cluster_id()) == j.cluster.as_ref()
            && Some(store.coordinator_id()) == j.coordinator.as_ref(),
        "the parent database belongs to another member"
    );
    if paths.worker.join("worker.db").try_exists()? {
        let store = crate::storage::WorkerStore::open_read_only(&paths.worker)
            .context("this version cannot read the worker database")?;
        ensure!(
            store.worker_id() == &j.spec.worker_id,
            "the worker database belongs to another worker"
        );
    }
    Ok(())
}

/// Everything up to and including the start of the new version. On success
/// the trial runs and the guard watches its deadline.
pub fn begin<L: Launcher>(
    env: &BTreeMap<String, String>,
    current_exe: &Path,
    launcher: &mut L,
    options: Options,
    now_ms: u64,
) -> anyhow::Result<Trial> {
    let initial = Paths::resolve_for(env, launcher.backend())?;
    let init_lock = resident::secure_file(&initial.config.join("init.lock"), true)
        .context("this machine has no initialized resident; run belt init first")?;
    // One upgrade, init or membership change at a time on this machine.
    init_lock.lock()?;
    membership_undo::recover(&initial, launcher)?;
    ensure!(
        !initial.config.join("membership-undo.json").try_exists()?,
        "membership undo is still pending"
    );
    let spec = bootstrap::read_input(&initial.input).context("resident input unavailable")?;
    let (from, paths) = managed_journal(&spec)?;
    ensure!(
        launcher.backend() == paths.backend,
        "service manager differs"
    );
    ensure!(
        read_record(&paths)?.is_none(),
        "an earlier upgrade is still in trial; it is committed or rolled back by its deadline, then run upgrade again"
    );
    let to = ws::sha256_file(current_exe)?;
    ensure!(
        to != from.binary_hash,
        "this machine already runs version {to}"
    );
    control::hold(&paths.worker)?;
    let prepared = (|| -> anyhow::Result<()> {
        let live = ws::live_attempts(&paths.worker)?;
        if ws::wait_until_idle(
            || ws::live_attempts(&paths.worker),
            options.wait,
            Duration::from_millis(250),
        )
        .is_err()
        {
            anyhow::bail!("{live} attempt(s) are still live after waiting; nothing was switched");
        }
        let placed = ws::place_binary_with(
            &ws::Paths::resolve_for(env, paths.backend)?,
            &to,
            current_exe,
            0o700,
        )?;
        ensure!(
            placed == retargeted(&paths, &from, &to).spec.executable,
            "the version directory is not where the resident's binaries are"
        );
        identity_checked(&paths, &from)
    })();
    if let Err(error) = prepared {
        control::lift_hold(&paths.worker)?;
        return Err(error.context("the upgrade hold is lifted"));
    }
    let lock = upgrade_lock(&paths)?;
    private_dir_with(&upgrade_dir(&paths), &mut NativeDurability)?;
    wsu::copy_private(&paths.journal, &kept(&paths, KEPT_JOURNAL))?;
    wsu::copy_private(&paths.input, &kept(&paths, KEPT_INPUT))?;
    let mut record = Record {
        schema: 1,
        from: from.binary_hash.clone(),
        to: to.clone(),
        deadline_ms: now_ms.saturating_add(options.deadline.as_millis() as u64),
        backed_up: false,
        phase: Phase::Trial,
    };
    write_record(&paths, &record)?;
    let switched = (|| -> anyhow::Result<Spec> {
        let named = from.plist.clone().context("service receipt missing")?;
        install_guard(&paths, &from, &named, launcher)?;
        let plan = plan(&paths, &from)?;
        match launcher.loaded_state(&plan, &named) {
            LoadedState::Absent => {}
            LoadedState::Unknown => {
                anyhow::bail!("loaded service identity unknown; nothing was switched")
            }
            LoadedState::Match => {
                let previous = control::read(&paths.worker)?;
                let mut manager = macos::Manager {
                    identity: macos::WorkerIdentity {
                        cluster_id: from.cluster.clone().context("group missing")?,
                        worker_id: from.spec.worker_id.clone(),
                    },
                    files: DirectoryFiles::open(&paths.services)?,
                    launcher: &mut *launcher,
                    worker: ws::MarkerControl {
                        state: paths.worker.clone(),
                        wait: options.wait,
                        interval: Duration::from_millis(250),
                    },
                };
                let stopped = manager.stop(&plan, &named, false);
                // The stop drains on its way; the user's own drain is what stays.
                control::write(&paths.worker, previous)?;
                stopped?;
            }
        }
        ensure!(
            launcher.loaded_state(&plan, &named) == LoadedState::Absent,
            "the resident is not confirmed stopped; nothing was switched"
        );
        {
            // Both databases at one moment, with nothing running on them.
            let _owner = crate::storage::ProcessLock::acquire(&paths.state, "coordinator.lock")?;
            keep_database(
                &paths.state.join("coordinator.db"),
                &kept(&paths, KEPT_PARENT_DB),
            )?;
            keep_database(
                &paths.worker.join("worker.db"),
                &kept(&paths, KEPT_WORKER_DB),
            )?;
        }
        record.backed_up = true;
        write_record(&paths, &record)?;
        // From here the version that starts promises nothing until the commit.
        control::begin_trial(&paths.worker)?;
        let mut j = retargeted(&paths, &from, &to);
        j.files.retain(|p| p.path != from.spec.executable);
        j.files.push(Proof::read(&j.spec.executable)?);
        put_input(&paths, &j.spec)?;
        let next = super::plan(&paths, &j)?;
        let receipt = put_service(&paths, &next)?;
        refresh(&mut j, &[&paths.input, receipt.path()])?;
        j.plist = Some(receipt.clone());
        replace_journal(&paths, &j)?;
        link_resident(&j.spec.executable, &mut NativeDurability)?;
        launcher
            .launchctl(
                &next
                    .start_arguments(receipt.path())
                    .map_err(anyhow::Error::msg)?,
            )
            .map_err(|e| anyhow::anyhow!("the new version did not start ({e})"))?;
        Ok(j.spec)
    })();
    drop(lock);
    match switched {
        Ok(spec) => Ok(Trial { paths, from, spec }),
        Err(error) => Err(abandon(&paths, &from, launcher, error)),
    }
}

/// The CLI has checked the new version: the trial ends, unless the guard
/// rolled it back first. The kept copies go and the hold is lifted.
pub fn commit<L: Launcher>(trial: &Trial, launcher: &mut L) -> anyhow::Result<Upgraded> {
    let paths = &trial.paths;
    {
        let _lock = upgrade_lock(paths)?;
        match read_record(paths)? {
            Some(record) if record.phase == Phase::Trial && record.to == trial.versions().1 => {
                finish(paths)?;
            }
            _ => anyhow::bail!(
                "the upgrade was rolled back before its commit (deadline passed); version {} runs with both databases restored",
                trial.from.binary_hash
            ),
        }
    }
    let named = trial
        .from
        .plist
        .clone()
        .context("service receipt missing")?;
    remove_guard(paths, &guard_plan(paths, &trial.from)?, &named, launcher)?;
    let (from, to) = trial.versions();
    Ok(Upgraded {
        from: from.to_string(),
        to: to.to_string(),
    })
}

impl Trial {
    fn versions(&self) -> (&str, &str) {
        let to = self
            .spec
            .executable
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        (&self.from.binary_hash, to)
    }
    /// The CLI's own health check failed: roll back now, not at the deadline.
    pub fn abandon<L: Launcher>(self, launcher: &mut L, error: anyhow::Error) -> anyhow::Error {
        abandon(&self.paths, &self.from, launcher, error)
    }
}

/// The whole upgrade. `health` checks the new version; it is never run while
/// the upgrade lock is held.
pub fn upgrade<L: Launcher>(
    env: &BTreeMap<String, String>,
    current_exe: &Path,
    launcher: &mut L,
    options: Options,
    health: &mut dyn FnMut(&Spec, &mut L) -> anyhow::Result<()>,
) -> anyhow::Result<Upgraded> {
    let trial = begin(
        env,
        current_exe,
        launcher,
        options,
        crate::platform::wall_ms(),
    )?;
    if let Err(error) = health(&trial.spec, launcher) {
        return Err(trial.abandon(
            launcher,
            error.context("the new version failed its health check"),
        ));
    }
    commit(&trial, launcher)
}

/// One look by the guard. Returns true when the guard has nothing more to
/// watch (and has removed itself).
pub fn guard_tick<L: Launcher>(
    config: &Path,
    launcher: &mut L,
    now_ms: u64,
) -> anyhow::Result<bool> {
    let input = bootstrap::read_input(&config.join("coordinator.json"))?;
    let journal = read_journal(&config.join(KEPT_JOURNAL))?;
    let mut paths = Paths::resolve_for(&input.env, journal.backend())?;
    ensure!(
        paths.config == config,
        "the guard belongs to another member"
    );
    paths.key = journal.spec.cluster_key_path.clone();
    let from = match read_journal(&kept(&paths, KEPT_JOURNAL)) {
        Ok(kept) => kept,
        Err(_) => journal,
    };
    let named = from.plist.clone().context("service receipt missing")?;
    let guard = guard_plan(&paths, &from)?;
    let record = {
        let _lock = upgrade_lock(&paths)?;
        read_record(&paths)?
    };
    match record {
        Some(record) if record.phase == Phase::Trial && now_ms < record.deadline_ms => Ok(false),
        Some(_) => {
            rollback(&paths, launcher)?;
            remove_guard(&paths, &guard, &named, launcher)?;
            Ok(true)
        }
        None => {
            // Committed, or rolled back, by someone who may have died before
            // the hold was lifted.
            {
                let _lock = upgrade_lock(&paths)?;
                if read_record(&paths)?.is_none() {
                    finish(&paths)?;
                }
            }
            remove_guard(&paths, &guard, &named, launcher)?;
            Ok(true)
        }
    }
}

/// `upgrade-guard --config <dir>`: run by the service manager until the
/// trial is committed or rolled back.
pub fn guard_main(config: &Path) -> i32 {
    let mut launcher = ws::HostLauncher::native();
    loop {
        match guard_tick(config, &mut launcher, crate::platform::wall_ms()) {
            Ok(true) => return 0,
            Ok(false) => {}
            Err(error) => {
                let _ = writeln!(std::io::stderr(), "error: {error:#}");
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The new version answers its health check as the registered service, and
/// this version reads both databases it left.
fn checked<L: Launcher>(spec: &Spec, launcher: &mut L) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(READY, async {
            loop {
                if managed_health(spec, launcher).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("the new version did not answer as the registered service"))
    })?;
    let (j, paths) = managed_journal(spec)?;
    identity_checked(&paths, &j)
}

/// `belt upgrade [--binary <path>]`. A binary other than this one runs the
/// upgrade itself: the version that is installed is the one that checks it
/// can read what is there.
pub enum Ran {
    Here(Upgraded),
    /// The named binary ran the upgrade and printed its own answer.
    Delegated(i32),
}
pub fn command(binary: Option<&Path>, json: bool) -> Result<Ran, ApiError> {
    let current = std::env::current_exe()
        .map_err(|_| ApiError::new(ErrorCode::InvalidInput, "cannot locate executable"))?;
    let failure = |e: anyhow::Error| ApiError::new(ErrorCode::Conflict, format!("upgrade: {e:#}"));
    if let Some(binary) = binary {
        let other = ws::sha256_file(binary).map_err(|e| {
            ApiError::new(ErrorCode::InvalidInput, format!("upgrade: --binary: {e:#}"))
        })?;
        if other != ws::sha256_file(&current).map_err(failure)? {
            let mut command = std::process::Command::new(binary);
            if json {
                command.arg("--json");
            }
            let status = command
                .arg("upgrade")
                .status()
                .map_err(|e| ApiError::new(ErrorCode::InvalidInput, format!("upgrade: {e}")))?;
            return Ok(Ran::Delegated(status.code().unwrap_or(1)));
        }
    }
    let env: BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    upgrade(
        &env,
        &current,
        &mut ws::HostLauncher::native(),
        Options {
            wait: Duration::from_secs(600),
            deadline: COMMIT_DEADLINE,
        },
        &mut checked,
    )
    .map(Ran::Here)
    .map_err(failure)
}

#[cfg(test)]
mod tests;
