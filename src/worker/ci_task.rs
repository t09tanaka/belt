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
//! prepared setup or an in-place workspace has no CI task.

use crate::error::{ApiError, ErrorCode};
use crate::ids::Digest;
use crate::source::classify::ci_id_of;
use crate::supervisor::ci_task::{CiTaskSpec, MAX_PLAN_BYTES};
use crate::toolchain::contract::ExecutionBinding;
use crate::toolchain::profile::Profile;
use crate::wire::{AttemptPayload, CiTask};
use crate::workspace::{Workspace, WorkspaceKind};
use local_ci::{FrozenPlan, TaskInputs};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

/// The inputs of a task (the outcomes of the jobs it needs) are JSON of at
/// most this many bytes.
pub const MAX_INPUT_BYTES: usize = 4 << 20;
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
    store_plan_within(state_dir, bytes, MAX_PLAN_BYTES)
}

fn store_plan_within(state_dir: &Path, bytes: &[u8], max: u64) -> std::io::Result<Digest> {
    if bytes.len() as u64 > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the frozen plan is too large",
        ));
    }
    let digest = Digest::of_bytes(bytes);
    let dir = plans_dir(state_dir);
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)?;
    let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(plan_path(state_dir, &digest))
        .map_err(|e| e.error)?;
    Ok(digest)
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
    let bytes = match std::fs::metadata(&plan_path) {
        Ok(meta) if meta.len() <= MAX_PLAN_BYTES => std::fs::read(&plan_path).ok(),
        Ok(_) => return Err(invalid("the frozen plan is too large")),
        Err(_) => None,
    }
    .ok_or_else(|| {
        ApiError::new(
            ErrorCode::NotFound,
            format!(
                "the frozen plan {} has not been delivered to this worker",
                task.plan_digest
            ),
        )
    })?;
    if Digest::of_bytes(&bytes) != task.plan_digest {
        return Err(invalid(
            "the frozen plan this worker holds does not hash to the digest of the attempt",
        ));
    }
    let plan: FrozenPlan = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("the frozen plan is not a plan this worker can read"))?;
    plan.verify()
        .map_err(|_| invalid("the frozen plan is not sealed"))?;
    if plan.task(&task.task_id).is_none() {
        return Err(invalid("the frozen plan has no such task"));
    }
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
        assert!(store_plan_within(dir.path(), b"12345", 5).is_ok());
        let refused = store_plan_within(dir.path(), b"123456", 5).unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read_dir(plans_dir(dir.path())).unwrap().count(), 1);
    }

    #[test]
    fn without_a_profile_the_steps_get_only_path_and_home() {
        let env = base_env(None, None).unwrap();
        assert!(env.keys().all(|k| k == "PATH" || k == "HOME"));
        assert_eq!(env.get("PATH").cloned(), std::env::var("PATH").ok());
    }
}
