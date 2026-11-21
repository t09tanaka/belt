//! Worker-only enrollment becomes the combined resident under the worker switch lock.
use super::*;
use crate::service::worker_service::{self, Enrollment};
use std::{collections::BTreeSet, os::unix::fs::PermissionsExt};

fn drained(worker: &worker_service::Paths, enrollment: &Enrollment) -> anyhow::Result<()> {
    if !worker.state.join("worker.db").try_exists()? {
        return Ok(());
    }
    let store = crate::storage::WorkerStore::open_read_only(&worker.state)?;
    ensure!(
        store.worker_id() == &enrollment.worker_id,
        "worker database identity differs"
    );
    let count = store.count_group_work()?;
    ensure!(count == 0,
        "{count} live attempts or uncollected results prevent init; drain with workers stop from the members, or wait, then run init again");
    Ok(())
}

// Only these init-owned paths can be removed on rollback. Existing binaries,
// worker state, enrollment, keys and user configuration are never cleanup roots.
struct Undo {
    roots: Vec<PathBuf>,
    stages: BTreeSet<PathBuf>,
    stage_dirs: Vec<PathBuf>,
    binary_permissions: Option<(PathBuf, fs::Permissions)>,
    created_parents: Vec<PathBuf>,
    resident_target: Option<PathBuf>,
}
impl Undo {
    fn prepare(paths: &Paths, digest: &str) -> anyhow::Result<Self> {
        let binary_dir = paths.config.join("bin").join(digest);
        let mut roots = vec![
            paths.state.clone(),
            paths.input.clone(),
            paths.journal.clone(),
        ];
        for path in &roots {
            ensure!(
                !path.try_exists()?,
                "existing coordinator state requires reconciliation before worker takeover"
            );
        }
        let binary = binary_dir.join("belt");
        let binary_permissions = if binary.try_exists()? {
            let (file, hash) = binary_source(&binary)?;
            let metadata = file.metadata()?;
            ensure!(
                hash == digest
                    && metadata.uid() == rustix::process::geteuid().as_raw()
                    && metadata.nlink() == 1
                    && metadata.mode() & 0o022 == 0,
                "unsafe installed worker binary"
            );
            Some((binary.clone(), metadata.permissions()))
        } else {
            None
        };
        let mut created_parents = Vec::new();
        for parent in paths.state.ancestors().skip(1) {
            if parent.try_exists()? {
                break;
            }
            created_parents.push(parent.to_owned());
        }
        let stage_dirs = vec![
            paths.config.clone(),
            paths.services.clone(),
            binary_dir.clone(),
        ];
        for path in [binary, binary_dir, paths.config.join("init.lock")] {
            if !path.try_exists()? {
                roots.push(path);
            }
        }
        let mut stage_dirs = stage_dirs;
        stage_dirs.push(paths.config.join("bin"));
        let resident_target = match fs::read_link(paths.config.join("bin/resident")) {
            Ok(target) => Some(target),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let stages = stages(&stage_dirs)?;
        Ok(Self {
            roots,
            stages,
            stage_dirs,
            binary_permissions,
            created_parents,
            resident_target,
        })
    }
    fn restore(&self, paths: &Paths) -> anyhow::Result<()> {
        if paths.journal.try_exists()? {
            let j = read_journal(&paths.journal)?;
            if let Some(receipt) = &j.plist {
                if receipt.path().try_exists()? {
                    DirectoryFiles::open(&paths.services)?.remove(receipt)?;
                }
            }
        }
        for stage in stages(&self.stage_dirs)?.difference(&self.stages) {
            fs::remove_file(stage)?;
        }
        let bin = paths.config.join("bin");
        if bin.try_exists()? {
            for entry in fs::read_dir(&bin)? {
                let entry = entry?;
                if entry.file_type()?.is_symlink()
                    && entry.file_name().to_str().is_some_and(|name| {
                        name.strip_prefix(".resident.").is_some_and(|pid| {
                            !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit())
                        })
                    })
                {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        let link = paths.config.join("bin/resident");
        if fs::read_link(&link).ok() != self.resident_target {
            match fs::remove_file(&link) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            if let Some(target) = &self.resident_target {
                std::os::unix::fs::symlink(target, &link)?;
            }
        }
        if bin.try_exists()? {
            File::open(&bin)?.sync_all()?;
        }
        for path in &self.roots {
            if path.try_exists()? {
                if path.is_dir() {
                    fs::remove_dir_all(path)?;
                } else {
                    fs::remove_file(path)?;
                }
            }
        }
        if let Some((path, permissions)) = &self.binary_permissions {
            fs::set_permissions(path, permissions.clone())?;
        }
        for path in &self.created_parents {
            if path.try_exists()? {
                fs::remove_dir(path)?;
            }
        }
        File::open(&paths.config)?.sync_all()?;
        Ok(())
    }
}
fn stages(dirs: &[PathBuf]) -> anyhow::Result<BTreeSet<PathBuf>> {
    let mut paths = BTreeSet::new();
    for dir in dirs {
        if dir.try_exists()? {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with(".init-"))
                {
                    paths.insert(entry.path());
                }
            }
        }
    }
    Ok(paths)
}

pub(super) async fn initialize<L: Launcher>(
    mut context: ContextInput,
    worker: worker_service::Paths,
    name: Option<&WorkerId>,
    launcher: &mut L,
    durability: &mut impl Durability,
) -> anyhow::Result<ResultReceipt> {
    let _switch = worker_service::switch_lock(&worker, true, Duration::from_secs(2))?
        .context("worker service switch is running; retry init when it finishes")?;
    let enrollment = worker_service::enrollment(&worker).context("invalid worker enrollment")?;
    check_name(name, &enrollment.worker_id)?;
    drained(&worker, &enrollment)?;
    // Keep the installed execution environment, including its resource scope.
    context.env = enrollment.env.clone();
    let mut paths = Paths::resolve_for(&context.env, launcher.backend())?;
    ensure!(
        paths.config == worker.config && paths.worker == worker.state,
        "worker environment paths differ"
    );
    paths.key = worker.key.clone();
    service_inputs_observable(&paths, &context.env)?;
    let (_, digest) = binary_source(&context.executable)?;
    let mut undo = Undo::prepare(&paths, &digest)?;
    let agent_lock = worker.state.join("agent.lock");
    if !agent_lock.try_exists()? {
        undo.roots.push(agent_lock);
    }
    let retired = [
        worker.journal.with_extension("json.retired-by-init"),
        enrollment.receipt.path().with_extension("retired-by-init"),
    ];
    for path in &retired {
        ensure!(!path.try_exists()?, "retired worker receipt already exists");
    }
    let loaded = launcher.loaded_state(&enrollment.plan, &enrollment.receipt);
    ensure!(
        loaded != LoadedState::Unknown,
        "worker service identity unknown; nothing changed"
    );
    // Keep init's lock across the post-start retirement and any rollback;
    // another init must not restart a resident while its files are undone.
    let init_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(paths.config.join("init.lock"))?;
    init_lock.lock()?;
    let result = async {
        if loaded == LoadedState::Match {
            launcher.verify_loaded(&enrollment.plan, &enrollment.receipt)?;
            launcher.launchctl(
                &enrollment
                    .plan
                    .stop_arguments(enrollment.receipt.path())
                    .map_err(anyhow::Error::msg)?,
            )?;
        }
        ensure!(
            launcher.loaded_state(&enrollment.plan, &enrollment.receipt) == LoadedState::Absent,
            "worker service did not stop"
        );
        // Recheck after stop: an admission could have raced the first count.
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(worker.state.join("agent.lock"))?;
        owner.try_lock()?;
        drained(&worker, &enrollment)?;
        // `worker --stdio` only connects to agent.sock (run_worker_stdio);
        // it never starts an agent. The switch lock excludes service helpers
        // throughout this gap until the combined resident acquires agent.lock.
        drop(owner);
        // Init receipts require private files; the worker installer uses 0755.
        // Preserve the inode and bytes, and restore the mode on rollback.
        if let Some((path, _)) = &undo.binary_permissions {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        let receipt = initialize_prepared(
            context,
            paths.clone(),
            Some(&enrollment),
            name,
            launcher,
            durability,
        )
        .await?;
        // Renames preserve the receipts' inodes and make retirement reversible.
        rename_new(enrollment.receipt.path(), &retired[1])?;
        rename_new(&worker.journal, &retired[0])?;
        Ok(receipt)
    }
    .await;
    match result {
        Ok(receipt) => Ok(receipt),
        Err(error) => {
            let rollback = (|| -> anyhow::Result<()> {
                // Never restart the worker while a replacement may still be running.
                if paths.journal.try_exists()? {
                    let j = read_journal(&paths.journal)?;
                    if let Some(receipt) = &j.plist {
                        let plan = plan(&paths, &j)?;
                        match launcher.loaded_state(&plan, receipt) {
                            LoadedState::Match => {
                                launcher.verify_loaded(&plan, receipt)?;
                                launcher.launchctl(
                                    &plan
                                        .stop_arguments(receipt.path())
                                        .map_err(anyhow::Error::msg)?,
                                )?;
                                ensure!(
                                    launcher.loaded_state(&plan, receipt) == LoadedState::Absent,
                                    "init resident did not stop during rollback"
                                );
                            }
                            LoadedState::Unknown => {
                                anyhow::bail!("init resident state unknown during rollback")
                            }
                            LoadedState::Absent => {}
                        }
                    }
                }
                for (from, to) in retired
                    .iter()
                    .zip([&worker.journal, &enrollment.receipt.path().to_path_buf()])
                {
                    if from.try_exists()? {
                        rename_new(from, to)?;
                    }
                }
                undo.restore(&paths)?;
                if loaded == LoadedState::Match
                    && launcher.loaded_state(&enrollment.plan, &enrollment.receipt)
                        == LoadedState::Absent
                {
                    launcher.launchctl(
                        &enrollment
                            .plan
                            .start_arguments(enrollment.receipt.path())
                            .map_err(anyhow::Error::msg)?,
                    )?;
                }
                if loaded == LoadedState::Match {
                    launcher.verify_loaded(&enrollment.plan, &enrollment.receipt)?;
                }
                Ok(())
            })();
            match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(anyhow::anyhow!(
                    "{error}; worker takeover rollback failed: {rollback}"
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_restores_resident_link_and_removes_its_stage() {
        for old in [None, Some("previous-digest")] {
            let dir = crate::test_tmp::tempdir().unwrap();
            let env = BTreeMap::from([("HOME".into(), dir.path().to_str().unwrap().into())]);
            let paths = Paths::resolve(&env).unwrap();
            private_dir(&paths.config).unwrap();
            let bin = paths.config.join("bin");
            private_dir(&bin).unwrap();
            let link = bin.join("resident");
            if let Some(target) = old {
                std::os::unix::fs::symlink(target, &link).unwrap();
            }
            let undo = Undo::prepare(&paths, "new-digest").unwrap();
            if old.is_some() {
                fs::remove_file(&link).unwrap();
            }
            std::os::unix::fs::symlink("new-digest", &link).unwrap();
            let stage = bin.join(format!(".resident.{}", std::process::id()));
            std::os::unix::fs::symlink("new-digest", &stage).unwrap();
            let stale = bin.join(".resident.987654");
            std::os::unix::fs::symlink("missing-target", &stale).unwrap();
            let regular = bin.join(".resident.123456");
            fs::write(&regular, "keep").unwrap();
            let unrelated = bin.join(".resident.123.extra");
            std::os::unix::fs::symlink("missing-target", &unrelated).unwrap();
            undo.restore(&paths).unwrap();
            assert!(fs::symlink_metadata(stale).is_err());
            assert!(regular.is_file());
            assert!(fs::symlink_metadata(unrelated).unwrap().is_symlink());
            assert_eq!(fs::read_link(&link).ok(), old.map(PathBuf::from));
            assert!(fs::symlink_metadata(stage).is_err());
        }
    }
}
