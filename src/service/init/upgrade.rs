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
    /// An interrupt (SIGINT) while accepted work finishes lifts the hold and
    /// switches nothing. Only the CLI, which installs the handler, sets it.
    pub interruptible: bool,
    /// Whether an interrupt came: the handler's flag, or what stands for it
    /// in a test.
    pub interrupted: fn() -> bool,
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

/// The one record commit and rollback decide on. The guard of an older
/// version reads what a newer one wrote, so fields it does not know are
/// left alone rather than refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
/// exist then does not exist after. The database it replaces is never
/// opened: the new version may have left it in a form this version cannot
/// read, or broken. Its log goes first, then the kept copy takes its name in
/// one rename, so running this again after a crash ends the same way.
fn restore_database(live: &Path, copy: &Path) -> anyhow::Result<()> {
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
    // The kept journal stays until the guard is gone: it is what the guard
    // reads, and the live journal may be one the guard's version cannot.
    for name in [KEPT_INPUT, KEPT_PARENT_DB, KEPT_WORKER_DB] {
        wsu::remove_if_present(&kept(paths, name))?;
    }
    control::end_trial(&paths.worker)?;
    control::lift_hold(&paths.worker)?;
    Ok(())
}

/// The last kept copy, once the guard is removed.
fn forget(paths: &Paths) -> anyhow::Result<()> {
    wsu::remove_if_present(&kept(paths, KEPT_JOURNAL))
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
        )?;
        forget(paths)
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
    clock: &dyn Fn() -> u64,
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
    // Before the live files are read: they may be the new version's.
    let unwatched = recover_unwatched(&initial, launcher)?;
    let spec = bootstrap::read_input(&initial.input).context("resident input unavailable")?;
    let (mut from, paths) = managed_journal(&spec)?;
    // Before the journal is copied for a rollback or retargeted: a leftover
    // key switch would reach the new version, which may not know the field.
    super::key_change::settle_held(&paths.journal, &mut from)?;
    ensure!(
        launcher.backend() == paths.backend,
        "service manager differs"
    );
    ensure!(
        read_record(&paths)?.is_none(),
        "an earlier upgrade is still in trial; it is committed or rolled back by its deadline, then run upgrade again"
    );
    let recovered = match unwatched {
        Some(note) => Some(note),
        None => lift_orphaned_hold(&paths)?,
    };
    let to = ws::sha256_file(current_exe)?;
    if to == from.binary_hash {
        match recovered {
            Some(note) => anyhow::bail!("this machine already runs version {to}; {note}"),
            None => anyhow::bail!("this machine already runs version {to}"),
        }
    }
    if let Some(note) = recovered {
        let _ = writeln!(std::io::stderr(), "note: {note}");
    }
    control::hold(&paths.worker)?;
    // From the hold to the record an interrupt is only noted, so that the
    // hold is lifted on the way out rather than left behind.
    let held = options
        .interruptible
        .then(crate::service::workers::warmup::interrupt::Held::new);
    let interrupted = || held.is_some() && (options.interrupted)();
    let prepared = (|| -> anyhow::Result<()> {
        let live = ws::live_attempts(&paths.worker)?;
        if ws::wait_until_idle(
            || {
                ensure!(!interrupted(), "interrupted");
                ws::live_attempts(&paths.worker)
            },
            options.wait,
            Duration::from_millis(250),
        )
        .is_err()
        {
            ensure!(
                !interrupted(),
                "interrupted while accepted work finished; nothing was switched"
            );
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
        identity_checked(&paths, &from)?;
        ensure!(!interrupted(), "interrupted; nothing was switched");
        Ok(())
    })();
    if let Err(error) = prepared {
        control::lift_hold(&paths.worker)?;
        return Err(error.context("the upgrade hold is lifted"));
    }
    let mut record = Record {
        schema: 1,
        from: from.binary_hash.clone(),
        to: to.clone(),
        deadline_ms: 0,
        backed_up: false,
        phase: Phase::Trial,
    };
    let recorded = (|| -> anyhow::Result<File> {
        let lock = upgrade_lock(&paths)?;
        private_dir_with(&upgrade_dir(&paths), &mut NativeDurability)?;
        wsu::copy_private(&paths.journal, &kept(&paths, KEPT_JOURNAL))?;
        wsu::copy_private(&paths.input, &kept(&paths, KEPT_INPUT))?;
        // The trial's window opens here, after the wait for accepted work,
        // however long that took.
        record.deadline_ms = clock().saturating_add(options.deadline.as_millis() as u64);
        write_record(&paths, &record)?;
        Ok(lock)
    })();
    drop(held);
    // Without a record there is no trial and no guard: nothing else would
    // ever lift the hold.
    let lock = match recorded {
        Ok(lock) => lock,
        Err(error) => {
            control::lift_hold(&paths.worker)?;
            return Err(error.context("nothing was switched and the upgrade hold is lifted"));
        }
    };
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
    forget(paths)?;
    let (from, to) = trial.versions();
    Ok(Upgraded {
        from: from.to_string(),
        to: to.to_string(),
    })
}

/// The version an executable in its version directory is.
fn version_of(executable: &Path) -> &str {
    executable
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or_default()
}

impl Trial {
    fn versions(&self) -> (&str, &str) {
        (&self.from.binary_hash, version_of(&self.spec.executable))
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
    clock: &dyn Fn() -> u64,
    health: &mut dyn FnMut(&Spec, &mut L) -> anyhow::Result<()>,
) -> anyhow::Result<Upgraded> {
    let trial = begin(env, current_exe, launcher, options, clock)?;
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
    let Watch {
        paths,
        named,
        guard,
    } = watch(config)?;
    let record = {
        let _lock = upgrade_lock(&paths)?;
        read_record(&paths)?
    };
    match record {
        Some(record) if record.phase == Phase::Trial && now_ms < record.deadline_ms => Ok(false),
        Some(_) => {
            rollback(&paths, launcher)?;
            remove_guard(&paths, &guard, &named, launcher)?;
            forget(&paths)?;
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
            forget(&paths)?;
            Ok(true)
        }
    }
}

/// What the guard watches with: the paths and service names of the version
/// the trial began from.
struct Watch {
    paths: Paths,
    named: macos::Receipt,
    guard: macos::policy::Plan,
}

fn watch(config: &Path) -> anyhow::Result<Watch> {
    // The kept journal is what this (the previous) version wrote; the live
    // files may be the new version's, which this one need not read.
    let kept_journal = config.join("upgrade").join(KEPT_JOURNAL);
    let from = if kept_journal.try_exists()? {
        read_journal(&kept_journal)?
    } else {
        read_journal(&config.join("init.json"))?
    };
    let mut paths = Paths::resolve_for(&from.spec.env, from.backend())?;
    ensure!(
        paths.config == config,
        "the guard belongs to another member"
    );
    paths.key = from.spec.cluster_key_path.clone();
    let named = from.plist.clone().context("service receipt missing")?;
    let guard = guard_plan(&paths, &from)?;
    Ok(Watch {
        paths,
        named,
        guard,
    })
}

/// A trial record that no guard watches: the CLI died after the record and
/// before its guard was confirmed. The caller holds `init.lock`, so no
/// upgrade is between those two steps now, and nothing else would ever roll
/// it back; it is rolled back here, the way the guard does.
fn recover_unwatched<L: Launcher>(
    initial: &Paths,
    launcher: &mut L,
) -> anyhow::Result<Option<&'static str>> {
    if read_record(initial)?.is_none() {
        return Ok(None);
    }
    let Watch {
        paths,
        named,
        guard,
    } = watch(&initial.config)?;
    if launcher.loaded_state(&guard, &named) != LoadedState::Absent {
        // Watched (or not known): the guard decides by the deadline.
        return Ok(None);
    }
    rollback(&paths, launcher)?;
    remove_guard(&paths, &guard, &named, launcher)?;
    forget(&paths)?;
    Ok(Some(
        "an earlier upgrade was left in trial with no guard; it is rolled back with both databases",
    ))
}

/// A hold, or a trial marker, with no record: an upgrade was cut short
/// before its record (or after its commit or rollback removed it). The
/// caller holds `init.lock`, so no upgrade owns it; it is lifted here.
fn lift_orphaned_hold(paths: &Paths) -> anyhow::Result<Option<&'static str>> {
    if !control::held(&paths.worker) && !control::in_trial(&paths.worker) {
        return Ok(None);
    }
    let _lock = upgrade_lock(paths)?;
    if read_record(paths)?.is_some() {
        return Ok(None);
    }
    finish(paths)?;
    Ok(Some(
        "the upgrade hold an interrupted upgrade left is lifted",
    ))
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

/// What the CLI saw of the new version before the commit (§24.8).
#[derive(Debug)]
pub(crate) struct Seen {
    /// The resident's own answer: its build, and what it offers and
    /// requires in `hello`.
    pub build: crate::service::workers::ResidentBuild,
    /// The features its worker agreed to in a `hello`, or why it refused.
    pub hello: Result<Vec<String>, String>,
    /// The attempts it adopted: the live ones of the parent database.
    pub adopted: Vec<String>,
    /// What it noted it did with them, on trial.
    pub proof: control::TrialProof,
}

/// Whether the new version `to` may be committed on what was seen: it is
/// that build and negotiates what this binary does, its worker agrees in
/// `hello` to every feature this build requires, and, when it adopted
/// attempts, it renewed the lease of one of them (or a worker answered for
/// each of them that it has no lease left to renew: ended, retired,
/// cancelling or never arrived).
pub(crate) fn judged(to: &str, seen: &Seen) -> anyhow::Result<()> {
    use crate::service::workers::ResidentBuild;
    let required = crate::protocol::session::required_features();
    match &seen.build {
        ResidentBuild::Running {
            binary_sha256,
            features,
            required_features,
        } => {
            ensure!(
                binary_sha256 == to,
                "the new version answers as build {binary_sha256}, not {to}"
            );
            ensure!(
                *features == crate::protocol::session::offered_features()
                    && *required_features == required,
                "the new version negotiates other features than this binary"
            );
        }
        ResidentBuild::NotRunning => {
            anyhow::bail!("the new version did not say which build it is (no answer)")
        }
        ResidentBuild::Unknown { reason } => {
            anyhow::bail!("the new version did not say which build it is ({reason})")
        }
    }
    let agreed = seen
        .hello
        .as_ref()
        .map_err(|e| anyhow::anyhow!("the new version's worker did not answer hello: {e}"))?;
    let missing: Vec<&str> = required
        .iter()
        .filter(|f| !agreed.contains(f))
        .map(String::as_str)
        .collect();
    ensure!(
        missing.is_empty(),
        "the new version's worker did not agree to {}",
        missing.join(", ")
    );
    let renewed = seen.adopted.iter().any(|a| seen.proof.renewed.contains(a));
    let none_to_renew = seen
        .adopted
        .iter()
        .all(|a| seen.proof.unrenewed.contains(a));
    ensure!(
        renewed || none_to_renew,
        "the new version renewed the lease of none of the {} attempt(s) it adopted",
        seen.adopted.len()
    );
    Ok(())
}

/// A `hello` to the worker of the new version, as a probe: it proves the
/// group key, and the worker records nothing. Only this build's own
/// required features are agreed to.
async fn local_hello(spec: &Spec, worker_state: &Path) -> Result<Vec<String>, String> {
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or_else(|| "the resident names no group".to_string())?;
    let key = crate::protocol::Membership::load(spec.worker_id.clone(), &spec.cluster_key_path)
        .map_err(|e| e.to_string())?
        .key;
    let io = crate::protocol::transport::connect_unix(&worker_state.join("agent.sock"))
        .await
        .map_err(|e| format!("the worker socket: {e}"))?;
    let answer = crate::service::workers::hello_on(io, &cluster, key, None).await?;
    if answer.worker != spec.worker_id || answer.cluster != cluster {
        return Err("another worker answered".into());
    }
    Ok(answer.features)
}

/// The attempts of the parent database that are live: the ones a starting
/// version adopts.
fn adopted(paths: &Paths) -> anyhow::Result<Vec<String>> {
    Ok(
        crate::storage::CoordinatorStore::open_read_only(&paths.state)?
            .live_attempts(None)?
            .into_iter()
            .map(|live| live.record.attempt_id.to_string())
            .collect(),
    )
}

/// The new version answers its health check as the registered service, this
/// version reads both databases it left, and what is seen of it is judged
/// ([`judged`]). The coordinator renews at its ticks once it reaches the
/// workers, so the proof of a renewal is waited for, never past [`READY`].
fn checked<L: Launcher>(spec: &Spec, launcher: &mut L) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
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
    identity_checked(&paths, &j)?;
    let to = version_of(&spec.executable);
    let mut seen = Seen {
        build: crate::service::workers::resident_build(spec),
        hello: runtime.block_on(local_hello(spec, &paths.worker)),
        adopted: Vec::new(),
        proof: control::TrialProof::default(),
    };
    // The build and the hello first: they do not change by waiting.
    judged(to, &seen)?;
    seen.adopted = adopted(&paths)?;
    loop {
        seen.proof = control::trial_proof(&paths.worker).unwrap_or_default();
        let judgement = judged(to, &seen);
        if judgement.is_ok() || started.elapsed() >= READY {
            return judgement;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
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
    // Ours while this upgrade runs; an interrupt outside the wait still ends
    // the command at once.
    let interrupt = crate::service::workers::warmup::interrupt::Guard::new();
    upgrade(
        &env,
        &current,
        &mut ws::HostLauncher::native(),
        Options {
            wait: Duration::from_secs(600),
            deadline: COMMIT_DEADLINE,
            interruptible: interrupt.is_some(),
            interrupted: crate::service::workers::warmup::interrupt::requested,
        },
        &crate::platform::wall_ms,
        &mut checked,
    )
    .map(Ran::Here)
    .map_err(failure)
}

#[cfg(test)]
mod tests;
