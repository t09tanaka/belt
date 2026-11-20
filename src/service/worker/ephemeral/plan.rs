//! Pure decisions of `worker --ephemeral`: what the one-shot worker is, whether
//! this host may run it, and whether its state may be removed afterwards.
use super::super::{WorkerSpec, ENV};
use crate::ids::{ClusterId, WorkerBootId, WorkerId};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// What the user gave on the command line.
pub struct Request<'a> {
    pub cluster: &'a str,
    /// The worker name the parent registered (`id` of its `[[workers]]`).
    pub name: &'a str,
    pub key_file: &'a Path,
}

/// What was observed on this host before anything was created.
pub struct Host<'a> {
    pub env: &'a BTreeMap<String, String>,
    pub executable: &'a Path,
    pub os_boot: &'a str,
    /// Fresh per start; never reused.
    pub instance: &'a str,
    /// A persistent worker's service journal exists on this host.
    pub resident_journal: bool,
    /// The canonical worker state directory already exists.
    pub state_exists: bool,
}

pub struct Plan {
    pub spec: WorkerSpec,
    /// The identity of this start: the OS boot with the instance appended.
    pub boot_id: WorkerBootId,
}

/// The start's own identity. The parent registered the worker by name, so the
/// name stays; what differs between two starts is this value, which no earlier
/// start can have had.
pub fn boot_id(os_boot: &str, instance: &str) -> Result<WorkerBootId, String> {
    if instance.is_empty() {
        return Err("an ephemeral start needs its own instance identity".into());
    }
    WorkerBootId::parse(&format!("{os_boot}.{instance}"))
        .map_err(|e| format!("ephemeral start identity: {e}"))
}

pub fn plan(request: &Request<'_>, host: &Host<'_>) -> Result<Plan, String> {
    let cluster = ClusterId::parse(request.cluster).map_err(|e| format!("--cluster: {e}"))?;
    let worker_id = WorkerId::parse(request.name).map_err(|e| format!("--name: {e}"))?;
    if !request.key_file.is_absolute() || request.key_file.to_string_lossy().contains('\0') {
        return Err("--key-file must be an absolute path".into());
    }
    // An ephemeral worker never takes over a resident's place. The persistent
    // worker owns this state directory and its identity.
    if host.resident_journal {
        return Err(
            "a persistent worker is installed on this host; an ephemeral worker would share its state".into(),
        );
    }
    if host.state_exists {
        return Err(
            "worker state already exists on this host; an ephemeral worker starts clean and never resumes it".into(),
        );
    }
    let home = host
        .env
        .get("HOME")
        .filter(|h| Path::new(h).is_absolute())
        .ok_or("HOME must be an absolute path")?;
    let env: BTreeMap<String, String> = ENV
        .iter()
        .filter_map(|k| Some((k.to_string(), host.env.get(*k)?.clone())))
        .collect();
    let state_dir: PathBuf = Path::new(home).join(".belt");
    Ok(Plan {
        boot_id: boot_id(host.os_boot, host.instance)?,
        spec: WorkerSpec {
            schema: 1,
            expected_cluster: cluster,
            worker_id,
            state_dir,
            cluster_key_path: request.key_file.to_owned(),
            executable: host.executable.to_owned(),
            env,
            config: "schema_version = 1\n".into(),
        },
    })
}

/// State goes away only when nothing is left running in it. A read that
/// failed, or an attempt that is still live, keeps it.
pub fn removable(live_attempts: Option<u32>) -> bool {
    live_attempts == Some(0)
}

/// An ephemeral worker ends itself once a member told it that its identity
/// is retired and nothing runs on it any more (spec chapter 15, 24.3): no
/// member that retired the identity sends it work again, and an identity
/// lasts for one start. A count that could not be read keeps it running.
pub fn finished(told_retired: bool, live_attempts: Option<u64>) -> bool {
    told_retired && live_attempts == Some(0)
}

#[cfg(test)]
mod tests;
