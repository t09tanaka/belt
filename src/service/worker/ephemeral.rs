//! `worker --ephemeral`: a worker that exists only while the user keeps it
//! running (a notebook cell, a Colab session).
//!
//! It registers no service and is never installed or upgraded by the parent.
//! It starts clean: a persistent worker's journal, or any state at the
//! canonical worker directory, makes it refuse. Each start has its own
//! identity (the boot identity carries a per-start instance), so a restarted
//! worker is never mistaken for the one before it. The parent reaches it
//! through `worker --stdio` over the tunnel the user prepared; no port is
//! opened here. On exit it removes the state directory it created, unless
//! work is still recorded in it.
use super::WorkerSpec;
use crate::service::worker_service::{live_attempts, Paths};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};

mod plan;

/// The state directory this process created, and how to know it is still that one.
struct Created {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

fn create_state(path: &Path) -> anyhow::Result<Created> {
    std::fs::DirBuilder::new().mode(0o700).create(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(meta.is_dir(), "worker state is not a directory");
    Ok(Created {
        path: path.to_owned(),
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

/// Remove what was created, if nothing runs in it and it is still the same
/// directory (never a replacement that happens to sit at the path).
fn clean_up(created: &Created) -> anyhow::Result<bool> {
    if !plan::removable(live_attempts(&created.path).ok()) {
        return Ok(false);
    }
    let meta = std::fs::symlink_metadata(&created.path)?;
    anyhow::ensure!(
        meta.is_dir() && meta.dev() == created.dev && meta.ino() == created.ino,
        "worker state was replaced; left in place"
    );
    std::fs::remove_dir_all(&created.path)?;
    Ok(true)
}

pub struct Args {
    pub cluster: String,
    pub name: String,
    pub key_file: PathBuf,
}

fn start(args: &Args) -> anyhow::Result<()> {
    let env: std::collections::BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let executable = std::env::current_exe()?;
    let os_boot = crate::platform::boot_id()
        .ok_or_else(|| anyhow::anyhow!("OS boot identity unavailable"))?;
    let instance = ulid::Ulid::new().to_string();
    // Only the existence of a resident's journal matters; reading it is not
    // this role's business.
    let resident = std::fs::symlink_metadata(Paths::journal_path(&env)?).is_ok();
    let state_exists = env
        .get("HOME")
        .map(|h| Path::new(h).join(".belt"))
        .is_some_and(|p| std::fs::symlink_metadata(p).is_ok());
    let planned = plan::plan(
        &plan::Request {
            cluster: &args.cluster,
            name: &args.name,
            key_file: &args.key_file,
        },
        &plan::Host {
            env: &env,
            executable: &executable,
            os_boot: &os_boot,
            instance: &instance,
            resident_journal: resident,
            state_exists,
        },
    )
    .map_err(anyhow::Error::msg)?;
    let spec: WorkerSpec = planned.spec;
    let home = std::path::PathBuf::from(&spec.env["HOME"]);
    let created = create_state(&spec.state_dir)?;
    let result = (|| {
        let config = spec.validate(&home)?;
        eprintln!(
            "ephemeral worker {} started as instance {instance}",
            spec.worker_id
        );
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(super::run(spec, config, Some(planned.boot_id)))
    })();
    match clean_up(&created) {
        Ok(true) => {}
        Ok(false) => eprintln!("work is still recorded in the worker state; it was kept"),
        Err(e) => eprintln!("worker state was not removed: {e}"),
    }
    result
}

/// Foreground until stopped (SIGTERM or interrupt). Never installs anything.
pub fn main(args: &Args) -> i32 {
    match start(args) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: ephemeral worker failed: {e}");
            1
        }
    }
}
