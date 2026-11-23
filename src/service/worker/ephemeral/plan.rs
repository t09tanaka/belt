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
    /// A persistent worker's journal exists on this host: the worker
    /// service's, or the combined `init` resident's.
    pub resident_journal: bool,
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
        return Err(RESIDENT.into());
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

const RESIDENT: &str =
    "a persistent worker is installed on this host; an ephemeral worker would share its state";

/// The `kind` an ephemeral worker writes into `ephemeral.json`.
pub const MARKER_KIND: &str = "ephemeral_worker";

/// What `ephemeral.json` of a state left behind says (spec 25.4 (d), proof 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Marker {
    /// Readable, and written by an ephemeral worker.
    Ephemeral,
    /// There is no marker: the state was not made by an ephemeral worker.
    Absent,
    /// It exists but could not be read safely or parsed.
    Unreadable,
    /// Readable, of another kind.
    OtherKind,
}

/// The filesystem a state directory is on, as `statfs` names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filesystem {
    /// Linux: `f_type`, the superblock magic.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Magic(u64),
    /// macOS: `f_fstypename`.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    Name(String),
    /// It could not be asked.
    Unknown,
}

/// Local filesystems, where a `flock` excludes every process that can reach
/// the state: ext2/3/4, xfs, btrfs, zfs, tmpfs, overlayfs (the magic numbers
/// of linux/magic.h and OpenZFS) and APFS, HFS+. Network filesystems, the
/// ones shared between a host and its guest, and every kind not listed here
/// are not: a lock there says nothing about another kernel's processes.
pub fn local_filesystem(filesystem: &Filesystem) -> bool {
    const LOCAL_MAGIC: [u64; 6] = [
        0xEF53,      // ext2, ext3, ext4
        0x5846_5342, // xfs
        0x9123_683E, // btrfs
        0x2FC1_2FC1, // zfs
        0x0102_1994, // tmpfs
        0x794C_7630, // overlayfs
    ];
    match filesystem {
        Filesystem::Magic(magic) => LOCAL_MAGIC.contains(magic),
        Filesystem::Name(name) => name == "apfs" || name == "hfs",
        Filesystem::Unknown => false,
    }
}

/// What the worker that created a state saw of the filesystem under it,
/// as it wrote it into `ephemeral.json`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Recorded {
    /// The writer's own `local_filesystem` verdict.
    pub local: bool,
    /// The kind it saw, for the person reading a refusal.
    pub kind: String,
}

/// The record a worker creating a state on `filesystem` writes.
pub fn recorded(filesystem: &Filesystem) -> Recorded {
    Recorded {
        local: local_filesystem(filesystem),
        kind: match filesystem {
            Filesystem::Magic(magic) => format!("{magic:#x}"),
            Filesystem::Name(name) => name.clone(),
            Filesystem::Unknown => "unknown".into(),
        },
    }
}

/// Proofs 1 to 3 that a state left behind may be cleared by the next
/// ephemeral start: what can be read without writing anything. `Err` names
/// the condition that failed; then nothing is opened for writing, nothing is
/// locked and nothing is removed.
///
/// Proof 2 has two halves. The same directory can be a local filesystem to
/// the start that clears it and a shared one to the worker that lives in it
/// (a container's bind mount of a directory of its host; an NFS server and
/// its client), and a lock taken on one side need not reach the other. So
/// both the creator's recorded verdict and the one seen now must be local; a
/// marker that records none proves nothing.
pub fn examined(
    marker: &Marker,
    recorded: Option<&Recorded>,
    filesystem: &Filesystem,
    resident_journal: bool,
) -> Result<(), String> {
    match marker {
        Marker::Ephemeral => {}
        Marker::Absent => {
            return Err(
                "it has no ephemeral.json, so it was not created by an ephemeral worker; an ephemeral worker starts clean and never resumes it".into(),
            )
        }
        Marker::Unreadable => {
            return Err("its ephemeral.json cannot be read as an ephemeral worker's marker".into())
        }
        Marker::OtherKind => {
            return Err("its ephemeral.json is not of the ephemeral worker kind".into())
        }
    }
    match recorded {
        Some(recorded) if recorded.local => {}
        Some(recorded) => {
            return Err(format!(
                "the worker that created it did not see it on a local filesystem (it saw {}; a lock taken here does not show that no process on that side uses it)",
                recorded.kind
            ))
        }
        None => {
            return Err(
                "its ephemeral.json does not record the filesystem its creator saw it on".into(),
            )
        }
    }
    if !local_filesystem(filesystem) {
        return Err(
            "it is not on a local filesystem (a lock on a network or shared filesystem does not show that no other host uses it)".into(),
        );
    }
    if resident_journal {
        return Err(RESIDENT.into());
    }
    Ok(())
}

/// Proofs 4 and 5: both locks were taken, without waiting. `ephemeral.lock`
/// held by someone means a process the earlier start launched still runs;
/// `agent.lock` means an agent or an operation on this state does.
pub fn unused(ephemeral_lock_taken: bool, agent_lock_taken: bool) -> Result<(), String> {
    if !ephemeral_lock_taken {
        return Err(
            "its ephemeral.lock is held: a process started by the earlier ephemeral worker is still running".into(),
        );
    }
    if !agent_lock_taken {
        return Err("its agent.lock is held: a worker still uses this state".into());
    }
    Ok(())
}

/// State goes away only when nothing is left running in it. A read that
/// failed, or an attempt that is still live, keeps it.
pub fn removable(live_attempts: Option<u32>) -> bool {
    live_attempts == Some(0)
}

/// An ephemeral worker ends itself once a member told it that its identity
/// is retired, nothing runs on it any more, and no member that still uses
/// it has a result to collect from it (spec chapter 15, 24.3). Until then it
/// keeps serving the other members. A count that could not be read keeps it
/// running.
pub fn finished(
    told_retired: bool,
    live_attempts: Option<u64>,
    uncollected_results: Option<u64>,
) -> bool {
    told_retired && live_attempts == Some(0) && uncollected_results == Some(0)
}

/// How long a graceful end waits: the TERM grace the supervisors use, the
/// wait after their KILL (the agent's default, 5 s) and a little for the
/// records to settle. Capped at a day so that a huge configured grace never
/// overflows a deadline.
pub fn stop_bound_ms(term_grace_ms: u64) -> u64 {
    term_grace_ms
        .saturating_add(5_000)
        .saturating_add(5_000)
        .min(86_400_000)
}

/// Nothing of this worker runs: no live attempt, and no supervisor or setup
/// builder process. A count that could not be read is never quiet.
pub fn quiet(live_attempts: Option<u64>, running_processes: Option<u64>) -> bool {
    live_attempts == Some(0) && running_processes == Some(0)
}

/// What a graceful end does next.
#[derive(Debug, PartialEq, Eq)]
pub enum StopStep {
    /// Keep waiting for the supervisors.
    Wait,
    /// Nothing runs any more: the state may be erased.
    Settled,
    /// Something is still alive at the bound: keep the state, fail.
    GiveUp,
}

/// One look at the worker, and the decision it leads to. `after_begin`: the
/// look was started after the stop marker was written (a look started before
/// it can miss an attempt accepted in between, so it says nothing).
/// `quiet_before`: the previous look was quiet and after the begin; two quiet
/// looks in a row are idle, as `wait_until_idle` requires. Returns the step
/// and whether this look counts as a quiet one for the next.
pub fn stop_step(
    after_begin: bool,
    quiet: bool,
    quiet_before: bool,
    bound_passed: bool,
) -> (StopStep, bool) {
    let quiet_now = after_begin && quiet;
    let step = if quiet_now && quiet_before {
        StopStep::Settled
    } else if bound_passed {
        StopStep::GiveUp
    } else {
        StopStep::Wait
    };
    (step, quiet_now)
}

#[cfg(test)]
mod tests;
