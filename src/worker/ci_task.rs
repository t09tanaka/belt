//! The worker's side of a CI task (payload [`CiTask`], feature
//! `ci_task_v1`): where the frozen plans a coordinator delivered are kept,
//! and what the agent checks before it hands the task to a supervisor.
//!
//! A plan is kept under the SHA-256 of its bytes and is used only when the
//! bytes still hash to that name. The worker reads the task out of it and
//! never plans again.
//!
//! What cannot be run yet is refused before anything starts, with the
//! reason: the core of the plan takes a workspace only while it is the
//! captured source byte for byte, so an attempt with placed secret files, a
//! prepared setup or an in-place workspace has no CI task. A task with
//! service containers is refused too: the core starts them itself, and a
//! supervisor that is lost would leave them up (recovery does not stop them
//! yet).
//!
//! The plans are few and bounded ([`MAX_PLANS`], [`MAX_PLANS_BYTES`]); one
//! that no unfinished attempt names goes with the worker's GC once it has
//! been there for the retention time ([`sweep_plans`]).

use crate::error::{ApiError, ErrorCode};
use crate::ids::Digest;
use crate::source::classify::ci_id_of;
pub use crate::supervisor::ci_task::MAX_INPUT_BYTES;
use crate::supervisor::ci_task::{CiTaskSpec, MAX_PLAN_BYTES, RESULT_ARTIFACT};
use crate::toolchain::contract::ExecutionBinding;
use crate::toolchain::profile::Profile;
use crate::wire::{AttemptPayload, BlobPutRequest, BlobPutResponse, CiTask, SchemaVersion};
use crate::workspace::{Workspace, WorkspaceKind};
use local_ci::{FrozenPlan, TaskInputs};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// A worker keeps at most this many plans, of at most this many bytes
/// together. One more is refused until the GC made room.
pub const MAX_PLANS: usize = 256;
pub const MAX_PLANS_BYTES: u64 = 1 << 30;
const MAX_TASK_ID_BYTES: usize = 1024;

fn invalid(m: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, m)
}

fn hex_of(digest: &Digest) -> String {
    let text = digest.to_string();
    text.strip_prefix("sha256:").unwrap_or(&text).to_string()
}

/// `<state_dir>/ci-plans`, private to the worker's user.
pub fn plans_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("ci-plans")
}

/// Where the plan with this digest is kept, when it was delivered.
pub fn plan_path(state_dir: &Path, digest: &Digest) -> PathBuf {
    plans_dir(state_dir).join(format!("{}.json", hex_of(digest)))
}

/// Keep the bytes of a frozen plan under their digest. The same bytes again
/// change nothing.
pub fn store_plan(state_dir: &Path, bytes: &[u8]) -> std::io::Result<Digest> {
    let limits = PlanLimits {
        one: MAX_PLAN_BYTES,
        count: MAX_PLANS,
        total: MAX_PLANS_BYTES,
    };
    store_plan_within(state_dir, bytes, limits)
}

#[derive(Debug, Clone, Copy)]
struct PlanLimits {
    one: u64,
    count: usize,
    total: u64,
}

/// May a plan of `len` bytes be added to `held` plans of `held_bytes`?
fn room_for(len: u64, held: usize, held_bytes: u64, limits: PlanLimits) -> bool {
    held < limits.count && held_bytes.saturating_add(len) <= limits.total
}

/// The plans kept in `dir` (files named `<64 hex>.json`), with their sizes.
fn kept_plans(dir: &Path) -> Vec<(PathBuf, std::fs::Metadata)> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path).ok()?;
            (meta.is_file() && digest_of_name(&path).is_some()).then_some((path, meta))
        })
        .collect()
}

/// The digest a file of the plans directory is named after.
fn digest_of_name(path: &Path) -> Option<Digest> {
    let hex = path.file_name()?.to_str()?.strip_suffix(".json")?;
    Digest::try_from(format!("sha256:{hex}")).ok()
}

/// The directory of the plans, made if it is not there, private either way.
fn private_plans_dir(state_dir: &Path) -> std::io::Result<PathBuf> {
    let dir = plans_dir(state_dir);
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)?;
    // A directory that was there keeps the mode it had: make it private.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(dir)
}

fn store_plan_within(
    state_dir: &Path,
    bytes: &[u8],
    limits: PlanLimits,
) -> std::io::Result<Digest> {
    if bytes.len() as u64 > limits.one {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the frozen plan is too large",
        ));
    }
    let digest = Digest::of_bytes(bytes);
    let dir = private_plans_dir(state_dir)?;
    let target = plan_path(state_dir, &digest);
    if !target.is_file() {
        let held = kept_plans(&dir);
        let held_bytes = held.iter().map(|(_, m)| m.len()).sum();
        if !room_for(bytes.len() as u64, held.len(), held_bytes, limits) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "this worker holds too many frozen plans",
            ));
        }
    }
    let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(target).map_err(|e| e.error)?;
    Ok(digest)
}

/// A worker holds at most this many plans part-way.
const MAX_PLAN_PARTS: usize = 8;

/// Where the bytes of a plan wait until the last of them arrived. Not a
/// name of a plan ([`digest_of_name`]): it is never read as one, and
/// [`sweep_plans`] removes it when the delivery was cut.
fn part_path(dir: &Path, digest: &Digest, size: u64) -> PathBuf {
    dir.join(format!("incoming-{}-{size}.part", hex_of(digest)))
}

fn parts_in(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "part"))
        .count()
}

/// May another plan start to arrive while `parts` are part-way?
fn may_start_part(parts: usize) -> bool {
    parts < MAX_PLAN_PARTS
}

fn put_fault(e: std::io::Error) -> ApiError {
    match e.kind() {
        std::io::ErrorKind::StorageFull => ApiError::new(ErrorCode::Overloaded, e.to_string()),
        std::io::ErrorKind::InvalidInput => invalid(e.to_string()),
        _ => ApiError::new(ErrorCode::Internal, format!("frozen plan: {e}")),
    }
}

/// `blob_put` of a frozen plan: one chunk of its bytes (the caller checked
/// the chunk against the announced size and the largest plan). The bytes are
/// kept under their digest only when all of them arrived and hash to it. A
/// plan that is held already is answered as complete, and counts as
/// delivered now (the GC keeps it for the retention time from here).
pub fn put_plan(state_dir: &Path, r: &BlobPutRequest) -> Result<BlobPutResponse, ApiError> {
    use crate::worker::blob_policy::{plan_chunk, ChunkStep};
    let answer = |received, complete| BlobPutResponse {
        schema_version: SchemaVersion::CURRENT,
        digest: r.digest.clone(),
        received,
        complete,
    };
    let target = plan_path(state_dir, &r.digest);
    if target.is_file() {
        if let Ok(file) = std::fs::OpenOptions::new().append(true).open(&target) {
            let _ = file.set_modified(std::time::SystemTime::now());
        }
        return Ok(answer(r.total_size, true));
    }
    let dir = private_plans_dir(state_dir).map_err(put_fault)?;
    let part = part_path(&dir, &r.digest, r.total_size);
    let held = match std::fs::symlink_metadata(&part) {
        Ok(meta) if meta.is_file() => Some(meta.len()),
        Ok(_) => return Err(invalid("the staged plan is not a file")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(put_fault(e)),
    };
    let data = &r.data.0;
    let at = held.unwrap_or(0);
    let (skip, complete) = match plan_chunk(at, r.total_size, r.offset, data.len()) {
        ChunkStep::Behind => return Ok(answer(at, false)),
        ChunkStep::Overlong => {
            let _ = std::fs::remove_file(&part);
            return Err(invalid(
                "the staged bytes are not this plan's; send it again from the start",
            ));
        }
        ChunkStep::Append { skip, complete } => (skip, complete),
    };
    let mut open = std::fs::OpenOptions::new();
    open.append(true).custom_flags(libc::O_NOFOLLOW);
    if held.is_none() {
        if !may_start_part(parts_in(&dir)) {
            return Err(ApiError::new(
                ErrorCode::Overloaded,
                "too many frozen plans are part-way",
            ));
        }
        open.create_new(true).mode(0o600);
    }
    let mut file = open.open(&part).map_err(put_fault)?;
    file.write_all(&data[skip..]).map_err(put_fault)?;
    if !complete {
        return Ok(answer(at.max(r.offset + data.len() as u64), false));
    }
    file.sync_all().map_err(put_fault)?;
    drop(file);
    let bytes = std::fs::read(&part).map_err(put_fault)?;
    let _ = std::fs::remove_file(&part);
    if Digest::of_bytes(&bytes) != r.digest {
        return Err(invalid(format!(
            "the bytes of {} do not have that digest; send it again from the start",
            r.digest
        )));
    }
    store_plan(state_dir, &bytes).map_err(put_fault)?;
    Ok(answer(r.total_size, true))
}

/// May the file of a plan go? Not while an unfinished attempt names it, and
/// not before it has been there for `keep_ms`: a plan is delivered before
/// the attempt that names it is.
fn plan_is_garbage(in_use: bool, age_ms: Option<u64>, keep_ms: u64) -> bool {
    !in_use && age_ms.is_some_and(|age| age >= keep_ms)
}

/// Remove the plans no attempt in `in_use` names and that were delivered at
/// least `keep_ms` ago, and what a delivery that was cut left behind (after
/// the same time). Returns how many files went. Best effort: a file that
/// cannot be removed stays for the next pass.
pub fn sweep_plans(
    state_dir: &Path,
    in_use: &std::collections::BTreeSet<Digest>,
    keep_ms: u64,
) -> usize {
    let dir = plans_dir(state_dir);
    let now = std::time::SystemTime::now();
    let mut removed = 0;
    for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let age_ms = meta
            .modified()
            .ok()
            .and_then(|at| now.duration_since(at).ok())
            .map(|age| u64::try_from(age.as_millis()).unwrap_or(u64::MAX));
        let used = digest_of_name(&path).is_some_and(|d| in_use.contains(&d));
        if plan_is_garbage(used, age_ms, keep_ms) && std::fs::remove_file(&path).is_ok() {
            removed += 1;
        }
    }
    removed
}

/// The plans the unfinished attempts of `attempts` name.
pub fn plans_in_use<'a>(
    specs: impl IntoIterator<Item = &'a serde_json::Value>,
) -> std::collections::BTreeSet<Digest> {
    specs
        .into_iter()
        .filter_map(|spec| {
            let task = spec.get("ci_task")?;
            serde_json::from_value::<CiTask>(task.clone()).ok()
        })
        .map(|task| task.plan_digest)
        .collect()
}

/// One GC pass over the plans, with the attempts of `store` that are not
/// finished. When they cannot be listed nothing is removed.
pub fn gc_plans(state_dir: &Path, store: &crate::storage::WorkerStore, keep_ms: u64) -> usize {
    let Ok(live) = store.list_nonterminal() else {
        return 0;
    };
    sweep_plans(
        state_dir,
        &plans_in_use(live.iter().map(|a| &a.spec)),
        keep_ms,
    )
}

/// Is `name` the name the result of a CI task is kept under, or below it?
fn is_reserved_name(name: &str) -> bool {
    name.strip_prefix(RESULT_ARTIFACT)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// Are the inputs small enough to be accepted?
fn inputs_fit(inputs: &serde_json::Value, max: usize) -> bool {
    serde_json::to_vec(inputs).is_ok_and(|json| json.len() <= max)
}

/// What a payload with a CI task may not have, from the payload alone.
/// Checked when the attempt is accepted, and again before it is prepared.
pub fn check_payload(p: &AttemptPayload) -> Result<(), &'static str> {
    let Some(task) = &p.ci_task else {
        return Ok(());
    };
    if !p.commands.is_empty() {
        return Err("a CI task carries no commands: its steps are in the plan");
    }
    if task.task_id.is_empty() || task.task_id.len() > MAX_TASK_ID_BYTES {
        return Err("the task id of a CI task is empty or too long");
    }
    if task.task_id.chars().any(char::is_control) {
        return Err("the task id of a CI task has a control character");
    }
    if !task.inputs.is_object() {
        return Err("the inputs of a CI task are not an object");
    }
    if !inputs_fit(&task.inputs, MAX_INPUT_BYTES) {
        return Err("the inputs of the CI task are too large");
    }
    let names = p.artifacts.iter().map(|a| &a.name);
    if names
        .chain(p.outputs.iter().map(|o| &o.name))
        .any(|name| is_reserved_name(name))
    {
        return Err(
            "an artifact or output of a CI task is named like its result \
             (rtr-ci-task-result.json is reserved)",
        );
    }
    if p.in_place {
        return Err("a CI task never runs in place");
    }
    if p.has_secret_bundle {
        return Err(
            "a CI task of a source with secret files cannot be run yet: its workspace must \
             be the captured source alone (waiting for local-ci's workspace binding to \
             accept declared secret files)",
        );
    }
    if p.setup.is_some() {
        return Err(
            "a CI task with a prepared setup cannot be run yet: its workspace must be the \
             captured source alone (waiting for local-ci's workspace binding to accept \
             prepared setup output)",
        );
    }
    Ok(())
}

/// The environment every step of a CI task starts from: what a command of
/// `exec` with no variables of its own gets. Without a profile the worker
/// has no measured environment, and the steps get the `PATH` and `HOME` of
/// the agent's own.
pub fn base_env(
    profile: Option<&Profile>,
    binding: Option<&ExecutionBinding>,
) -> Result<BTreeMap<String, String>, String> {
    let none = BTreeMap::new();
    match (profile, binding) {
        (Some(_), Some(binding)) => binding.environment(&none).map_err(str::to_string),
        (Some(profile), None) => profile.execution_environment(&none),
        (None, _) => Ok(["PATH", "HOME"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok().map(|v| (key.to_string(), v)))
            .collect()),
    }
}

/// The plan `digest` names, when it was delivered and is what it says:
/// its bytes hash to the digest and are a sealed plan.
fn delivered_plan(state_dir: &Path, digest: &Digest) -> Result<FrozenPlan, ApiError> {
    let plan_path = plan_path(state_dir, digest);
    let bytes = match std::fs::metadata(&plan_path) {
        Ok(meta) if meta.len() <= MAX_PLAN_BYTES => std::fs::read(&plan_path).ok(),
        Ok(_) => return Err(invalid("the frozen plan is too large")),
        Err(_) => None,
    }
    .ok_or_else(|| {
        ApiError::new(
            ErrorCode::NotFound,
            format!("the frozen plan {digest} has not been delivered to this worker"),
        )
    })?;
    if Digest::of_bytes(&bytes) != *digest {
        return Err(invalid(
            "the frozen plan this worker holds does not hash to the digest of the attempt",
        ));
    }
    let plan: FrozenPlan = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("the frozen plan is not a plan this worker can read"))?;
    plan.verify()
        .map_err(|_| invalid("the frozen plan is not sealed"))?;
    Ok(plan)
}

/// What a task of the plan may not have, from the plan.
pub fn check_task(plan: &FrozenPlan, task_id: &str) -> Result<(), &'static str> {
    let Some(task) = plan.task(task_id) else {
        return Err("the frozen plan has no such task");
    };
    if !task.services.is_empty() {
        return Err(
            "a CI task with service containers cannot be run yet: the plan's core starts \
             them itself, and a supervisor that is lost would leave them up (stopping \
             them on recovery is not built yet)",
        );
    }
    Ok(())
}

/// What is checked of a CI task when its attempt is accepted, beyond the
/// payload: the task in the plan, when the plan is here already. A plan
/// that has not been delivered is no reason to refuse here (the attempt
/// then fails when it is prepared).
pub fn check_accepted(state_dir: &Path, payload: &AttemptPayload) -> Result<(), ApiError> {
    let Some(task) = &payload.ci_task else {
        return Ok(());
    };
    match delivered_plan(state_dir, &task.plan_digest) {
        Ok(plan) => check_task(&plan, &task.task_id).map_err(invalid),
        Err(e) if e.code == ErrorCode::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// How many commands the task starts, as far as its plan tells: the steps
/// the supervisor will run as `step-0`, `step-1`, ... 0 when the plan is
/// not here or does not tell.
pub fn planned_steps(state_dir: &Path, task: &CiTask) -> usize {
    delivered_plan(state_dir, &task.plan_digest)
        .ok()
        .and_then(|plan| local_ci::plan_task_demand(&plan, &task.task_id).ok())
        .map_or(0, |demand| demand.nodes.len())
}

/// Check the task against the plan that was delivered and say what the
/// supervisor runs. Nothing is started here.
pub fn prepare(
    state_dir: &Path,
    attempt_dir: &Path,
    payload: &AttemptPayload,
    task: &CiTask,
    workspace: &Workspace,
    env: BTreeMap<String, String>,
) -> Result<CiTaskSpec, ApiError> {
    check_payload(payload).map_err(invalid)?;
    if workspace.kind == WorkspaceKind::InPlace {
        return Err(invalid("a CI task never runs in place"));
    }
    let plan_path = plan_path(state_dir, &task.plan_digest);
    let plan = delivered_plan(state_dir, &task.plan_digest)?;
    check_task(&plan, &task.task_id).map_err(invalid)?;
    let inputs = serde_json::to_vec(&task.inputs)
        .ok()
        .filter(|json| json.len() <= MAX_INPUT_BYTES)
        .ok_or_else(|| invalid("the inputs of the CI task are too large"))?;
    serde_json::from_slice::<TaskInputs>(&inputs)
        .map_err(|_| invalid("the inputs of the CI task cannot be read"))?;
    let ci_source_id = ci_id_of(&payload.source_id)
        .ok_or_else(|| invalid("the source of the attempt has no captured id"))?;
    Ok(CiTaskSpec {
        plan_path,
        plan_digest: task.plan_digest.clone(),
        task_id: task.task_id.clone(),
        inputs: task.inputs.clone(),
        ci_source_id,
        workspace: workspace.root.clone(),
        scratch: attempt_dir.join("ci"),
        env,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci::execute::tests::{fixture, task_of, Fixture, WORKFLOW};

    #[test]
    fn a_plan_is_kept_under_the_digest_of_its_bytes_and_only_once() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let digest = store_plan(dir.path(), b"{\"plan\":1}").unwrap();
        assert_eq!(digest, Digest::of_bytes(b"{\"plan\":1}"));
        let path = plan_path(dir.path(), &digest);
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"plan\":1}");
        assert!(path.starts_with(plans_dir(dir.path())));
        assert!(!path.to_string_lossy().contains(':'));
        assert_eq!(store_plan(dir.path(), b"{\"plan\":1}").unwrap(), digest);
        let other = store_plan(dir.path(), b"{}").unwrap();
        assert_ne!(plan_path(dir.path(), &other), path);
        assert_eq!(std::fs::read_dir(plans_dir(dir.path())).unwrap().count(), 2);
    }

    fn chunk(bytes: &[u8], digest: &Digest, offset: usize, len: usize) -> BlobPutRequest {
        BlobPutRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: crate::ids::RequestId::generate(),
            coordinator_epoch: crate::ids::CoordinatorEpoch(1),
            source_id: crate::ids::SourceId::parse("src_01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            kind: crate::wire::SourceBlobKind::Plan,
            digest: digest.clone(),
            total_size: bytes.len() as u64,
            offset: offset as u64,
            data: crate::wire::Bytes(bytes[offset..offset + len].to_vec()),
        }
    }

    #[test]
    fn a_delivered_plan_is_kept_only_when_every_byte_arrived_and_hashes_to_its_name() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let bytes = b"{\"a\":\"frozen plan\"}";
        let digest = Digest::of_bytes(bytes);
        let first = put_plan(dir.path(), &chunk(bytes, &digest, 0, 5)).unwrap();
        assert_eq!((first.received, first.complete), (5, false));
        assert!(!plan_path(dir.path(), &digest).exists());
        // A chunk ahead of what is held writes nothing and says where to
        // go on; a repeated one is harmless.
        let ahead = put_plan(dir.path(), &chunk(bytes, &digest, 9, 3)).unwrap();
        assert_eq!((ahead.received, ahead.complete), (5, false));
        let again = put_plan(dir.path(), &chunk(bytes, &digest, 0, 5)).unwrap();
        assert_eq!((again.received, again.complete), (5, false));
        let last = put_plan(dir.path(), &chunk(bytes, &digest, 5, bytes.len() - 5)).unwrap();
        assert_eq!((last.received, last.complete), (bytes.len() as u64, true));
        assert_eq!(std::fs::read(plan_path(dir.path(), &digest)).unwrap(), bytes);
        assert_eq!(parts_in(&plans_dir(dir.path())), 0, "nothing stays staged");
        let mode = std::fs::metadata(plans_dir(dir.path())).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);

        // Held already: complete at once, whatever the chunk.
        let held = put_plan(dir.path(), &chunk(bytes, &digest, 0, 1)).unwrap();
        assert_eq!((held.received, held.complete), (bytes.len() as u64, true));

        // Bytes that do not hash to the name are not kept, and nothing of
        // them stays.
        let wrong = Digest::of_bytes(b"another plan");
        let e = put_plan(dir.path(), &chunk(bytes, &wrong, 0, bytes.len())).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("digest"), "{}", e.message);
        assert!(!plan_path(dir.path(), &wrong).exists());
        assert_eq!(parts_in(&plans_dir(dir.path())), 0);
    }

    #[test]
    fn a_plan_that_is_held_counts_as_delivered_again_when_it_is_sent_again() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let bytes = b"{}";
        let digest = store_plan(dir.path(), bytes).unwrap();
        let path = plan_path(dir.path(), &digest);
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        std::fs::File::options()
            .append(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        // An hour old: the GC with a minute of retention would take it.
        put_plan(dir.path(), &chunk(bytes, &digest, 0, 2)).unwrap();
        let none = std::collections::BTreeSet::new();
        assert_eq!(sweep_plans(dir.path(), &none, 60_000), 0);
        assert!(path.is_file());
    }

    #[test]
    fn only_a_few_plans_are_part_way_at_once_and_a_cut_delivery_goes_with_the_gc() {
        assert!(may_start_part(MAX_PLAN_PARTS - 1));
        assert!(!may_start_part(MAX_PLAN_PARTS));
        let dir = crate::test_tmp::tempdir().unwrap();
        for n in 0..MAX_PLAN_PARTS {
            let bytes = format!("plan {n}").into_bytes();
            let digest = Digest::of_bytes(&bytes);
            put_plan(dir.path(), &chunk(&bytes, &digest, 0, 1)).unwrap();
        }
        let bytes = b"one more";
        let digest = Digest::of_bytes(bytes);
        let e = put_plan(dir.path(), &chunk(bytes, &digest, 0, 1)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Overloaded);
        // One that is part-way already goes on.
        let bytes = b"plan 0";
        let digest = Digest::of_bytes(bytes);
        let done = put_plan(dir.path(), &chunk(bytes, &digest, 1, 5)).unwrap();
        assert!(done.complete);
        // What a cut delivery left goes with the GC; the plan stays while it
        // is young.
        let none = std::collections::BTreeSet::new();
        assert_eq!(sweep_plans(dir.path(), &none, 0), MAX_PLAN_PARTS);
        assert_eq!(parts_in(&plans_dir(dir.path())), 0);
    }

    #[test]
    fn a_worker_that_holds_too_many_plans_refuses_one_more_as_overloaded() {
        let full = std::io::Error::new(std::io::ErrorKind::StorageFull, "full");
        assert_eq!(put_fault(full).code, ErrorCode::Overloaded);
        let large = std::io::Error::new(std::io::ErrorKind::InvalidInput, "large");
        assert_eq!(put_fault(large).code, ErrorCode::InvalidInput);
        let other = std::io::Error::other("disk");
        assert_eq!(put_fault(other).code, ErrorCode::Internal);
    }

    fn payload_for(f: &Fixture, digest: &Digest, job: &str) -> AttemptPayload {
        let d = Digest::of_bytes(b"x");
        AttemptPayload {
            ci_task: Some(CiTask {
                plan_digest: digest.clone(),
                task_id: task_of(&f.plan, job).to_string(),
                inputs: serde_json::to_value(TaskInputs::default()).unwrap(),
            }),
            toolchains: None,
            outputs: Vec::new(),
            requirements: Default::default(),
            in_place: false,
            setup: None,
            run_id: crate::ids::RunId::generate(),
            task_id: crate::ids::TaskId::generate(),
            generation: crate::ids::Generation(1),
            source_id: crate::source::classify::source_id_of_ci(&f.manifest.source_id).unwrap(),
            plan_digest: d.clone(),
            manifest_digest: d,
            permit_id: None,
            commands: vec![],
            lease_ms: 60_000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: Vec::new(),
        }
    }

    fn prepared(
        f: &Fixture,
        p: &AttemptPayload,
        kind: WorkspaceKind,
    ) -> Result<CiTaskSpec, ApiError> {
        let root = f.area.join("ws");
        let ws = Workspace {
            root: root.clone(),
            cwd: root,
            kind,
            index_reproduced: true,
            warnings: Vec::new(),
        };
        let env = [("PATH".to_string(), "/usr/bin:/bin".to_string())].into();
        prepare(
            &f.area.join("state"),
            &f.area.join("attempt"),
            p,
            p.ci_task.as_ref().unwrap(),
            &ws,
            env,
        )
    }

    #[test]
    fn a_task_is_prepared_only_from_a_delivered_plan_that_holds_it() {
        let f = fixture(WORKFLOW);
        let state = f.area.join("state");
        let bytes = serde_json::to_vec(&f.plan).unwrap();
        let digest = Digest::of_bytes(&bytes);
        let p = payload_for(&f, &digest, "build");

        // Not delivered: the parent is told so, and nothing is made up.
        let missing = prepared(&f, &p, WorkspaceKind::Directory).unwrap_err();
        assert_eq!(missing.code, ErrorCode::NotFound);
        assert!(missing.message.contains("has not been delivered"));

        assert_eq!(store_plan(&state, &bytes).unwrap(), digest);
        let spec = prepared(&f, &p, WorkspaceKind::Directory).unwrap();
        assert_eq!(spec.plan_path, plan_path(&state, &digest));
        assert_eq!(spec.plan_digest, digest);
        assert_eq!(spec.task_id, task_of(&f.plan, "build"));
        assert_eq!(spec.ci_source_id, f.manifest.source_id);
        assert_eq!(spec.workspace, f.area.join("ws"));
        assert_eq!(spec.scratch, f.area.join("attempt").join("ci"));
        assert_eq!(spec.env["PATH"], "/usr/bin:/bin");
        assert_eq!(spec.validate(), Ok(()));

        // A task the plan does not have, inputs the core cannot read.
        let mut other = p.clone();
        other.ci_task.as_mut().unwrap().task_id = "no such task".into();
        let e = prepared(&f, &other, WorkspaceKind::Directory).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("no such task"));
        let mut other = p.clone();
        other.ci_task.as_mut().unwrap().inputs = serde_json::json!({ "needs": 3 });
        let e = prepared(&f, &other, WorkspaceKind::Directory).unwrap_err();
        assert!(e.message.contains("cannot be read"), "{}", e.message);

        // Bytes that no longer hash to their name are not a plan.
        std::fs::write(plan_path(&state, &digest), b"{}").unwrap();
        let e = prepared(&f, &p, WorkspaceKind::Directory).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        assert!(e.message.contains("does not hash"));
    }

    #[test]
    fn what_would_change_the_workspace_before_the_task_is_refused() {
        let f = fixture(WORKFLOW);
        let bytes = serde_json::to_vec(&f.plan).unwrap();
        let digest = store_plan(&f.area.join("state"), &bytes).unwrap();
        let base = payload_for(&f, &digest, "build");
        assert_eq!(check_payload(&base), Ok(()));
        let e = prepared(&f, &base, WorkspaceKind::InPlace).unwrap_err();
        assert!(e.message.contains("in place"));

        let refused = |change: fn(&mut AttemptPayload), word: &str| {
            let mut p = base.clone();
            change(&mut p);
            let why = check_payload(&p).unwrap_err();
            assert!(why.contains(word), "{why}");
            assert_eq!(
                prepared(&f, &p, WorkspaceKind::Directory).unwrap_err().code,
                ErrorCode::InvalidInput
            );
        };
        refused(|p| p.has_secret_bundle = true, "secret files");
        refused(
            |p| p.has_secret_bundle = true,
            "waiting for local-ci's workspace binding",
        );
        refused(|p| p.in_place = true, "in place");
        refused(
            |p| {
                p.setup = Some(crate::wire::SetupSpec {
                    steps: vec![],
                    toolchain: vec![],
                    scope: crate::wire::ReuseScope::Run,
                    freshness_secs: None,
                    secrets: vec![],
                    no_cache: false,
                    step_timeout_ms: None,
                })
            },
            "waiting for local-ci's workspace binding to accept prepared setup",
        );
        refused(|p| p.ci_task.as_mut().unwrap().task_id.clear(), "task id");
        refused(
            |p| p.ci_task.as_mut().unwrap().task_id = "a\nb".into(),
            "control character",
        );
        refused(
            |p| p.ci_task.as_mut().unwrap().task_id = "x".repeat(MAX_TASK_ID_BYTES + 1),
            "too long",
        );
        refused(
            |p| p.ci_task.as_mut().unwrap().inputs = serde_json::json!("x"),
            "not an object",
        );
        refused(
            |p| {
                p.commands = vec![crate::wire::CommandSpec {
                    node_id: crate::ids::NodeId::parse("main").unwrap(),
                    argv: vec!["true".into()],
                    cwd: String::new(),
                    env: BTreeMap::new(),
                    depends_on: vec![],
                    timeout_ms: None,
                    setup_step: None,
                }]
            },
            "no commands",
        );
        // A payload without a CI task is not this module's to judge.
        let mut plain = base.clone();
        plain.ci_task = None;
        plain.has_secret_bundle = true;
        assert_eq!(check_payload(&plain), Ok(()));
        // The longest task id still passes.
        let mut long = base.clone();
        long.ci_task.as_mut().unwrap().task_id = "x".repeat(MAX_TASK_ID_BYTES);
        assert_eq!(check_payload(&long), Ok(()));
    }

    #[test]
    fn a_plan_over_the_limit_is_not_kept() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let limits = PlanLimits {
            one: 5,
            count: 8,
            total: 64,
        };
        assert!(store_plan_within(dir.path(), b"12345", limits).is_ok());
        let refused = store_plan_within(dir.path(), b"123456", limits).unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read_dir(plans_dir(dir.path())).unwrap().count(), 1);
    }

    const WITH_SERVICE: &str = "on: push\njobs:\n  db:\n    runs-on: ubuntu-latest\n    services:\n      postgres:\n        image: postgres:16\n    steps:\n      - run: echo hi\n";

    #[test]
    fn a_task_with_service_containers_is_refused_when_it_is_accepted_and_when_it_is_prepared() {
        let f = fixture(WITH_SERVICE);
        let state = f.area.join("state");
        let task = task_of(&f.plan, "db").to_string();
        let why = check_task(&f.plan, &task).unwrap_err();
        assert!(why.contains("service containers"), "{why}");
        assert!(why.contains("recovery"), "{why}");
        assert_eq!(
            check_task(&f.plan, "no such task"),
            Err("the frozen plan has no such task")
        );

        let digest = Digest::of_bytes(&serde_json::to_vec(&f.plan).unwrap());
        let p = payload_for(&f, &digest, "db");
        // Not delivered yet: nothing to judge the task by (prepare fails).
        assert!(check_accepted(&state, &p).is_ok());
        store_plan(&state, &serde_json::to_vec(&f.plan).unwrap()).unwrap();
        let refused = check_accepted(&state, &p).unwrap_err();
        assert_eq!(refused.code, ErrorCode::InvalidInput);
        assert!(refused.message.contains("service containers"));
        let refused = prepared(&f, &p, WorkspaceKind::Directory).unwrap_err();
        assert_eq!(refused.code, ErrorCode::InvalidInput);
        assert!(refused.message.contains("service containers"));
        // A payload without a CI task is never read against a plan.
        let mut plain = p.clone();
        plain.ci_task = None;
        assert!(check_accepted(&state, &plain).is_ok());
        // A plan that is here and is not what its name says is refused at
        // once, not only when the attempt is prepared.
        std::fs::write(plan_path(&state, &digest), b"{}").unwrap();
        let refused = check_accepted(&state, &p).unwrap_err();
        assert_eq!(refused.code, ErrorCode::InvalidInput);
        assert!(refused.message.contains("does not hash"));
    }

    #[test]
    fn a_task_without_services_is_accepted_and_its_steps_are_counted_from_the_plan() {
        let f = fixture(WORKFLOW);
        let state = f.area.join("state");
        let bytes = serde_json::to_vec(&f.plan).unwrap();
        let digest = Digest::of_bytes(&bytes);
        let p = payload_for(&f, &digest, "build");
        let task = p.ci_task.clone().unwrap();
        assert_eq!(planned_steps(&state, &task), 0, "the plan is not here");
        store_plan(&state, &bytes).unwrap();
        assert!(check_accepted(&state, &p).is_ok());
        assert_eq!(check_task(&f.plan, &task.task_id), Ok(()));
        // One `run:` step: the supervisor runs it as `step-0`.
        assert_eq!(planned_steps(&state, &task), 1);
        let mut unknown = task.clone();
        unknown.task_id = "no such task".into();
        assert_eq!(planned_steps(&state, &unknown), 0);

        let two = fixture(
            "on: push\njobs:\n  job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo one\n      - run: echo two\n",
        );
        let bytes = serde_json::to_vec(&two.plan).unwrap();
        let state = two.area.join("state");
        let digest = store_plan(&state, &bytes).unwrap();
        let task = payload_for(&two, &digest, "job").ci_task.unwrap();
        assert_eq!(planned_steps(&state, &task), 2);
    }

    #[test]
    fn the_name_of_the_result_is_reserved_in_a_ci_payload_only() {
        assert!(is_reserved_name("rtr-ci-task-result.json"));
        assert!(is_reserved_name("rtr-ci-task-result.json/part"));
        assert!(!is_reserved_name("rtr-ci-task-result.json.bak"));
        assert!(!is_reserved_name("rtr-ci-task-result"));
        assert!(!is_reserved_name("dir/rtr-ci-task-result.json"));
        assert!(!is_reserved_name(""));
        assert_eq!(RESULT_ARTIFACT, "rtr-ci-task-result.json");

        let f = fixture(WORKFLOW);
        let digest = Digest::of_bytes(b"plan");
        let base = payload_for(&f, &digest, "build");
        let artifact = |name: &str| crate::wire::ArtifactDecl {
            name: name.into(),
            path: "out.txt".into(),
            if_missing: crate::wire::MissingPolicy::Optional,
        };
        let output = |name: &str| crate::wire::OutputDecl {
            name: name.into(),
            paths: vec!["out".into()],
            if_missing: crate::wire::MissingPolicy::Optional,
            when: crate::wire::OutputWhen::Always,
        };
        let mut p = base.clone();
        p.artifacts = vec![artifact("report"), artifact("rtr-ci-task-result.json.bak")];
        p.outputs = vec![output("dist")];
        assert_eq!(check_payload(&p), Ok(()));
        for change in [
            (|p: &mut AttemptPayload, a: crate::wire::ArtifactDecl, _| p.artifacts.push(a))
                as fn(&mut AttemptPayload, crate::wire::ArtifactDecl, crate::wire::OutputDecl),
            |p, _, o| p.outputs.push(o),
        ] {
            for name in ["rtr-ci-task-result.json", "rtr-ci-task-result.json/x"] {
                let mut clash = p.clone();
                change(&mut clash, artifact(name), output(name));
                let why = check_payload(&clash).unwrap_err();
                assert!(why.contains("reserved"), "{why}");
                // Not this module's rule for an attempt of commands.
                clash.ci_task = None;
                assert_eq!(check_payload(&clash), Ok(()));
            }
        }
    }

    #[test]
    fn inputs_over_the_limit_are_refused_when_the_attempt_is_accepted() {
        let small = serde_json::json!({ "needs": {} });
        let len = serde_json::to_vec(&small).unwrap().len();
        assert!(inputs_fit(&small, len));
        assert!(!inputs_fit(&small, len - 1));

        let f = fixture(WORKFLOW);
        let mut p = payload_for(&f, &Digest::of_bytes(b"plan"), "build");
        assert_eq!(check_payload(&p), Ok(()));
        p.ci_task.as_mut().unwrap().inputs =
            serde_json::json!({ "pad": "x".repeat(MAX_INPUT_BYTES) });
        assert_eq!(
            check_payload(&p),
            Err("the inputs of the CI task are too large")
        );
    }

    #[test]
    fn the_plans_a_worker_keeps_are_bounded_in_number_and_in_bytes() {
        let limits = PlanLimits {
            one: 100,
            count: 2,
            total: 10,
        };
        assert!(room_for(10, 0, 0, limits));
        assert!(room_for(4, 1, 6, limits));
        assert!(!room_for(5, 1, 6, limits), "over the total");
        assert!(!room_for(1, 2, 0, limits), "already as many as allowed");
        assert!(!room_for(u64::MAX, 0, 1, limits), "no overflow");

        let dir = crate::test_tmp::tempdir().unwrap();
        let first = store_plan_within(dir.path(), b"aaaa", limits).unwrap();
        // Over the total with what is held.
        let full = store_plan_within(dir.path(), b"bbbbbbb", limits).unwrap_err();
        assert_eq!(full.kind(), std::io::ErrorKind::StorageFull);
        store_plan_within(dir.path(), b"cccc", limits).unwrap();
        // As many as allowed.
        let full = store_plan_within(dir.path(), b"d", limits).unwrap_err();
        assert_eq!(full.kind(), std::io::ErrorKind::StorageFull);
        // A plan that is held already is no new plan.
        assert_eq!(
            store_plan_within(dir.path(), b"aaaa", limits).unwrap(),
            first
        );
        assert_eq!(kept_plans(&plans_dir(dir.path())).len(), 2);
        assert_eq!(
            std::fs::read_dir(plans_dir(dir.path())).unwrap().count(),
            2,
            "a refused plan leaves nothing"
        );
        assert_eq!((MAX_PLANS, MAX_PLANS_BYTES), (256, 1 << 30));
    }

    #[test]
    fn a_plans_directory_that_was_there_is_made_private() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let plans = plans_dir(dir.path());
        std::fs::create_dir(&plans).unwrap();
        std::fs::set_permissions(&plans, std::fs::Permissions::from_mode(0o755)).unwrap();
        store_plan(dir.path(), b"{}").unwrap();
        let mode = std::fs::metadata(&plans).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn a_plan_goes_only_when_no_unfinished_attempt_names_it_and_it_has_been_kept_long_enough() {
        assert!(plan_is_garbage(false, Some(10), 10));
        assert!(plan_is_garbage(false, Some(11), 10));
        assert!(!plan_is_garbage(false, Some(9), 10), "delivered just now");
        assert!(!plan_is_garbage(true, Some(1_000), 10), "in use");
        assert!(
            !plan_is_garbage(false, None, 0),
            "an age that cannot be told"
        );

        let dir = crate::test_tmp::tempdir().unwrap();
        let used = store_plan(dir.path(), b"{\"used\":1}").unwrap();
        let idle = store_plan(dir.path(), b"{\"idle\":1}").unwrap();
        let plans = plans_dir(dir.path());
        // What a delivery that was cut leaves, and a directory nobody made.
        std::fs::write(plans.join(".tmpAbCd"), b"half").unwrap();
        std::fs::create_dir(plans.join("sub")).unwrap();
        let in_use = [used.clone()].into();
        // Within the time a delivered plan is kept: nothing goes.
        assert_eq!(sweep_plans(dir.path(), &in_use, u64::MAX), 0);
        assert!(plan_path(dir.path(), &idle).exists());
        // After it: the plan nobody names and the leftover go.
        assert_eq!(sweep_plans(dir.path(), &in_use, 0), 2);
        assert!(plan_path(dir.path(), &used).exists());
        assert!(!plan_path(dir.path(), &idle).exists());
        assert!(!plans.join(".tmpAbCd").exists());
        assert!(plans.join("sub").is_dir(), "only files are removed");
        // Without the directory there is nothing to do.
        let none = crate::test_tmp::tempdir().unwrap();
        assert_eq!(sweep_plans(none.path(), &in_use, 0), 0);
    }

    #[test]
    fn the_plans_in_use_are_those_the_unfinished_attempts_name() {
        let a = Digest::of_bytes(b"a");
        let b = Digest::of_bytes(b"b");
        let ci = |d: &Digest| serde_json::json!({ "ci_task": { "plan_digest": d, "task_id": "t", "inputs": {} } });
        let specs = [
            ci(&a),
            serde_json::json!({ "commands": [] }),
            serde_json::json!({ "ci_task": "not a task" }),
            ci(&b),
            ci(&a),
        ];
        assert_eq!(plans_in_use(&specs), [a.clone(), b].into());

        // Through the store: the plan of an attempt that has not ended stays.
        let dir = crate::test_tmp::tempdir().unwrap();
        let worker = crate::ids::WorkerId::parse("w-gc").unwrap();
        let store = crate::storage::WorkerStore::open(dir.path(), &worker).unwrap();
        let named = store_plan(dir.path(), b"{\"named\":1}").unwrap();
        let other = store_plan(dir.path(), b"{\"other\":1}").unwrap();
        store
            .submit(
                crate::storage::NewAcceptance {
                    attempt_id: crate::ids::AttemptId::generate(),
                    run_id: crate::ids::RunId::generate(),
                    task_id: crate::ids::TaskId::generate(),
                    generation: crate::ids::Generation(1),
                    payload_hash: crate::ids::PayloadHash::parse(&format!(
                        "sha256:{}",
                        "a".repeat(64)
                    ))
                    .unwrap(),
                    coordinator_epoch: crate::ids::CoordinatorEpoch(1),
                    spec: ci(&named),
                },
                1,
            )
            .unwrap();
        assert_eq!(gc_plans(dir.path(), &store, 0), 1);
        assert!(plan_path(dir.path(), &named).exists());
        assert!(!plan_path(dir.path(), &other).exists());
    }

    #[test]
    fn with_a_profile_the_steps_get_its_environment_and_with_a_binding_the_activated_one() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let home = dir.path().to_str().unwrap().to_string();
        let measured: BTreeMap<String, String> = [
            ("PATH".to_string(), home.clone()),
            ("HOME".to_string(), home.clone()),
        ]
        .into();
        let profile = Profile::load(dir.path(), &measured).unwrap();
        let env = base_env(Some(&profile), None).unwrap();
        assert_eq!(
            env,
            profile.execution_environment(&BTreeMap::new()).unwrap()
        );
        assert_eq!(env.get("PATH"), Some(&home), "not the PATH of the agent");

        let d = Digest::of_bytes(b"x");
        let activated: BTreeMap<String, String> =
            [("PATH".to_string(), "/activated/bin".to_string())].into();
        let binding = ExecutionBinding {
            activation: crate::toolchain::adapters::ActivationReceipt {
                base_profile: d.clone(),
                frozen: d.clone(),
                setup_policy: d.clone(),
                installation: d.clone(),
                environment: activated.clone(),
                shell: crate::toolchain::adapters::FileIdentity {
                    path: "/bin/sh".into(),
                    digest: d.clone(),
                    size: 1,
                    executable: true,
                },
                binding_digest: d.clone(),
                execution_fingerprint: d,
            },
        };
        // The activation wins over the profile it was made from.
        assert_eq!(base_env(Some(&profile), Some(&binding)).unwrap(), activated);
        // Without a profile a binding is not looked at.
        let plain = base_env(None, Some(&binding)).unwrap();
        assert_ne!(
            plain.get("PATH").map(String::as_str),
            Some("/activated/bin")
        );
    }

    #[test]
    fn without_a_profile_the_steps_get_only_path_and_home() {
        let env = base_env(None, None).unwrap();
        assert!(env.keys().all(|k| k == "PATH" || k == "HOME"));
        assert_eq!(env.get("PATH").cloned(), std::env::var("PATH").ok());
    }
}
