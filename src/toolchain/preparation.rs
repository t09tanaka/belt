//! Private tool preparation. Only the managed helper can execute adapter commands.
use super::adapters::{
    self, AdapterKind, HostTools, InstallPlan, InstallationReceipt, ResolutionEvidence,
};
use super::versions::{FrozenToolchains, ToolId};
use crate::ids::Digest;
use crate::supervisor::spec::{command_id, spec_digest, NodeSpec, SuperviseSpec};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use task_traffic_controller::{managed, process, Context};

const MAX_REQUEST: u64 = 1024 * 1024;
const MAX_OUTPUT: usize = 8192;
pub(crate) const REQUEST_FILE: &str = "tool-request.json";
/// The exit of a helper whose install failed by itself (`install_exit`).
/// Every other failure of the helper exits with 1. Not a code of `error.rs`,
/// of sysexits, or of a shell.
pub(crate) const INSTALL_FAILED_EXIT: i32 = 3;

/// Trusted worker input, never a wire/project installer specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationRequest {
    pub kind: AdapterKind,
    pub frozen: FrozenToolchains,
    pub tool: ToolId,
    pub evidence: ResolutionEvidence,
    pub private_root: PathBuf,
    pub host: HostTools,
    pub expected_plan: Digest,
    /// Trusted local worker definition, derived from its own configuration.
    /// `frozen.profile_definition` remains the parent's capture provenance.
    pub profile_definition: Digest,
    /// Actual operator execution identity, distinct from profile definition.
    pub base_profile: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparationFailure(pub String);
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolPreparationPoll {
    Pending,
    Installed(Box<InstallationReceipt>),
    Failed(PreparationFailure),
}
impl PreparationRequest {
    pub fn digest(&self) -> Digest {
        Digest::of_bytes(&serde_json::to_vec(self).expect("trusted request serializes"))
    }
    pub(crate) fn plan(&self, root: &Path) -> Result<InstallPlan, String> {
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() as u64 > MAX_REQUEST {
            return Err("preparation_request_too_large".into());
        }
        if self.private_root != root {
            return Err("preparation_private_root_mismatch".into());
        }
        let plan = adapters::plan(
            self.kind,
            &self.frozen,
            &self.tool,
            &self.evidence,
            root,
            &self.host,
        )
        .map_err(str::to_string)?;
        if plan.key() != self.expected_plan {
            return Err("preparation_plan_key_mismatch".into());
        }
        Ok(plan)
    }
}

pub(crate) fn helper_node(
    exe: &Path,
    dir: &Path,
    request: &PreparationRequest,
) -> Result<NodeSpec, String> {
    Ok(NodeSpec {
        node_id: crate::ids::NodeId::parse("tool-install").map_err(|e| e.to_string())?,
        argv: vec![
            exe.to_str().ok_or("helper_path_utf8")?.into(),
            "toolchain-install".into(),
            "--request".into(),
            dir.join(REQUEST_FILE)
                .to_str()
                .ok_or("request_path_utf8")?
                .into(),
            "--binding".into(),
            request.digest().to_string(),
        ],
        cwd: dir.into(),
        env: BTreeMap::new(),
        weight: None,
        timeout_ms: None,
    })
}

/// Read the checked, bounded private file through the same descriptor.
fn private_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|e| e.to_string())?;
    let m = file.metadata().map_err(|e| e.to_string())?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != rustix::process::geteuid().as_raw()
        || m.mode() & 0o077 != 0
        || m.len() > MAX_REQUEST
    {
        return Err("unsafe_preparation_file".into());
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_REQUEST + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_REQUEST {
        return Err("preparation_file_limit".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

fn admitted(ctx: &Context, spec: &SuperviseSpec, node: &NodeSpec) -> bool {
    let id = command_id(&spec.attempt_id, 0);
    let assessment = managed::assess(ctx, &id);
    if assessment.verdict
        != task_traffic_controller::reap::Verdict::Keep(
            task_traffic_controller::reap::Keep::LeaseHeld,
        )
    {
        return false;
    }
    let managed::Loaded::Ok(sidecar) = assessment.sidecar else {
        return false;
    };
    let pid = std::process::id();
    let Some(child) = &sidecar.child else {
        return false;
    };
    let ledger = task_traffic_controller::ledger::Ledger::load(&ctx.paths.ledger_file());
    let running = ledger.ok().is_some_and(|l| {
        l.entries.iter().any(|e| {
            e.id == id
                && e.state == task_traffic_controller::admit::State::Running
                && e.pid == sidecar.owner_pid
        })
    });
    let worker = crate::storage::WorkerStore::open(&spec.state_dir, &spec.worker_id);
    let accepted = worker
        .as_ref()
        .ok()
        .and_then(|store| store.get(&spec.attempt_id).ok().flatten())
        .is_some_and(|a| {
            a.spec == serde_json::json!({"setup_builder": true})
                && a.payload_hash == Digest::of_bytes(spec.attempt_id.as_str().as_bytes())
                && matches!(
                    a.state,
                    crate::state::AttemptState::Starting | crate::state::AttemptState::Active
                )
                && a.termination.is_none()
                && a.cancel_requested_at_ms.is_none()
        });
    let read = crate::platform::read_process(pid);
    let launch = worker.ok().and_then(|store| store.launches(&spec.attempt_id).ok())
        .is_some_and(|launches| launches.iter().any(|(node_id, _, identity)| {
            identity.as_ref().is_some_and(|identity| node_id == &node.node_id && identity.pid == pid
                && matches!(&read.start, crate::platform::StartIdentity::Identity(start) if start == &identity.start_identity)
                && Some(identity.boot_id.as_str()) == sidecar.boot_id.as_deref())
        }));
    running
        && accepted
        && launch
        && sidecar.command_id == id
        && sidecar.phase == task_traffic_controller::receipt::Phase::ChildStarted
        && sidecar.terminal.is_none()
        && !sidecar.tracked_to_exit
        && sidecar.spec_digest == spec_digest(node)
        && sidecar.attempt
            == Some(serde_json::json!({"attempt_id": spec.attempt_id, "node_id": node.node_id}))
        && sidecar.boot_id.is_some()
        && sidecar.boot_id == process::system_boot_id()
        && child.pid == pid
        && child.start_time.is_some()
        && child.start_time == process::start_time(pid)
        && child.pgid == Some(pid as i32)
        && child.sid == Some(pid as i32)
        && sidecar.owner_pid > 1
        && sidecar.owner_start_time.is_some()
        && sidecar.owner_start_time == process::start_time(sidecar.owner_pid)
        && crate::platform::read_process(pid).ppid == Some(sidecar.owner_pid)
}

/// How long a helper waits for the records of its own admission (the
/// ledger entry, the sidecar, the launch in the worker store). Its
/// supervisor writes them after it started the helper, and on a busy machine
/// that takes seconds: a short wait here failed installs that were admitted.
/// A helper that is never admitted is stopped by its supervisor's deadline
/// before this, or leaves here.
const ADMISSION_WAIT: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Eq)]
enum AdmissionStep {
    Go,
    Wait,
    GiveUp,
}

/// What a helper does next. It runs the manager only once it is admitted.
/// When the admission of this command names another process as its child,
/// this helper was started beside it and never will be: it leaves at once.
fn admission_step(admitted: bool, another_is_the_child: bool, waited: Duration) -> AdmissionStep {
    if admitted {
        AdmissionStep::Go
    } else if another_is_the_child || waited >= ADMISSION_WAIT {
        AdmissionStep::GiveUp
    } else {
        AdmissionStep::Wait
    }
}

/// The admission of this builder's command records a child, and it is not
/// this process.
fn another_is_the_child(ctx: &Context, spec: &SuperviseSpec) -> bool {
    let id = command_id(&spec.attempt_id, 0);
    match managed::assess(ctx, &id).sidecar {
        managed::Loaded::Ok(sidecar) => sidecar
            .child
            .is_some_and(|child| child.pid != std::process::id()),
        _ => false,
    }
}

/// The exit of a helper whose install ended with `error`:
/// `INSTALL_FAILED_EXIT` when the install failed by itself, in a way the same
/// version would meet again (a command of the manager failed, a probe did
/// not answer the version, another device is mounted in the root), and 1
/// for everything else (a lock, a cleanup, a receipt that could not be
/// written, an executor that could not start).
pub(crate) fn install_exit(error: &str) -> i32 {
    match error {
        adapters::COMMAND_FAILED | adapters::VERSION_MISMATCH | adapters::OTHER_DEVICE => {
            INSTALL_FAILED_EXIT
        }
        _ => 1,
    }
}

/// Why a helper failed, and what it exits with.
struct HelperFailure {
    exit: i32,
    why: String,
}
impl From<String> for HelperFailure {
    fn from(why: String) -> Self {
        HelperFailure { exit: 1, why }
    }
}
impl From<&str> for HelperFailure {
    fn from(why: &str) -> Self {
        why.to_string().into()
    }
}

fn run_helper(path: &Path, binding: &str) -> Result<(), HelperFailure> {
    let dir = path.parent().ok_or("missing_builder_directory")?;
    if path != dir.join(REQUEST_FILE)
        || std::fs::canonicalize(dir).map_err(|e| e.to_string())? != dir
    {
        return Err("noncanonical_builder_directory".into());
    }
    let m = std::fs::symlink_metadata(dir).map_err(|e| e.to_string())?;
    if !m.is_dir() || m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o077 != 0 {
        return Err("unsafe_builder_directory".into());
    }
    let request: PreparationRequest = private_json(path)?;
    if request.digest().as_str() != binding {
        return Err("preparation_request_binding".into());
    }
    let spec: SuperviseSpec = private_json(&dir.join("spec.json"))?;
    spec.validate().map_err(|e| e.to_string())?;
    let meta: serde_json::Value = private_json(&dir.join("meta.json"))?;
    let builder = dir
        .file_name()
        .and_then(|v| v.to_str())
        .ok_or("builder_name")?;
    let builders = dir.parent().ok_or("builders_root")?;
    let setup = builders.parent().ok_or("setup_root")?;
    let state = setup.parent().ok_or("state_root")?;
    if builders.file_name().is_none_or(|v| v != "builders")
        || setup.file_name().is_none_or(|v| v != "setup")
        || spec.state_dir != dir
        || meta["builder_id"] != builder
        || meta["attempt_id"] != spec.attempt_id.as_str()
        || meta["kind"] != "ToolInstall"
        || meta["key"] != format!("tool:{}", request.expected_plan)
        || spec.profile_fingerprint.as_ref() != Some(&request.base_profile)
        || spec.nodes.len() != 1
    {
        return Err("preparation_builder_binding".into());
    }
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let node = helper_node(&exe, dir, &request)?;
    if spec.nodes[0] != node {
        return Err("preparation_spec_binding".into());
    }
    if !dir.join("worker.db").is_file() {
        return Err("preparation_acceptance_missing".into());
    }
    let toolchains = state.join("toolchains");
    let plan = request.plan(&toolchains)?;
    let ctx = Context::new(task_traffic_controller::paths::Paths::new(
        &spec.traffic_state_dir,
        spec.traffic_config_path(),
    ));
    let began = Instant::now();
    loop {
        let admitted = admitted(&ctx, &spec, &node);
        match admission_step(
            admitted,
            !admitted && another_is_the_child(&ctx, &spec),
            began.elapsed(),
        ) {
            AdmissionStep::Go => break,
            AdmissionStep::GiveUp => return Err("installer_not_admitted".into()),
            AdmissionStep::Wait => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    // Only the install itself can fail "by itself".
    adapters::install_admitted(&plan, &toolchains, &mut HelperExecutor).map_err(|e| {
        HelperFailure {
            exit: install_exit(e),
            why: e.into(),
        }
    })?;
    Ok(())
}

pub(crate) fn helper_main(path: &Path, binding: &str) -> i32 {
    match run_helper(path, binding) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("private tool preparation: {}", e.why);
            e.exit
        }
    }
}

// No public production executor: creation is possible only after helper admission.
struct HelperExecutor;
impl adapters::AdmittedExecutor for HelperExecutor {
    fn execute(
        &mut self,
        command: &adapters::CommandSpec,
    ) -> Result<adapters::CommandOutput, &'static str> {
        let mut child = Command::new(&command.program)
            .args(&command.argv)
            .env_clear()
            .envs(&command.env)
            .current_dir(&command.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| "installer_spawn")?;
        let stdout = child.stdout.take().ok_or("installer_stdout")?;
        let stderr = child.stderr.take().ok_or("installer_stderr")?;
        // cargo install writes a line per dependency to stderr: only its
        // last MAX_OUTPUT bytes are kept, as a diagnostic tail (spec 22.1).
        let tail = cargo_install(command);
        // Drain both pipes concurrently, retaining at most the contract's limit.
        let (out, err, status) = std::thread::scope(|scope| {
            let out = scope.spawn(|| drain(stdout));
            let err = scope.spawn(|| {
                if tail {
                    drain_tail(stderr)
                } else {
                    drain(stderr)
                }
            });
            let status = child.wait();
            (out.join(), err.join(), status)
        });
        let stdout = out.map_err(|_| "installer_drain_panicked")??;
        err.map_err(|_| "installer_drain_panicked")??;
        Ok(adapters::CommandOutput {
            success: status.map_err(|_| "installer_wait")?.success(),
            stdout,
        })
    }
}
fn cargo_install(command: &adapters::CommandSpec) -> bool {
    command.program.file_name().is_some_and(|n| n == "cargo")
        && command.argv.first().is_some_and(|a| a == "install")
}
/// Read to the end, keeping only the last MAX_OUTPUT bytes.
fn drain_tail(mut stream: impl Read) -> Result<Vec<u8>, &'static str> {
    let mut kept = std::collections::VecDeque::new();
    let mut buf = [0; 4096];
    loop {
        let n = stream.read(&mut buf).map_err(|_| "installer_output_read")?;
        if n == 0 {
            break;
        }
        kept.extend(&buf[..n]);
        while kept.len() > MAX_OUTPUT {
            kept.pop_front();
        }
    }
    Ok(kept.into())
}
fn drain(mut stream: impl Read) -> Result<Vec<u8>, &'static str> {
    let mut kept = Vec::new();
    let mut total = 0usize;
    let mut buf = [0; 4096];
    loop {
        let n = stream.read(&mut buf).map_err(|_| "installer_output_read")?;
        if n == 0 {
            break;
        }
        total = total.saturating_add(n);
        if total <= MAX_OUTPUT {
            kept.extend_from_slice(&buf[..n]);
        }
    }
    if total > MAX_OUTPUT {
        Err("installer_output_limit")
    } else {
        Ok(kept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    /// #220 H1: a cargo install that writes more than MAX_OUTPUT to stderr
    /// (a line per dependency) succeeds; another installer's still fails.
    #[test]
    fn a_cargo_install_with_a_long_stderr_succeeds() {
        use adapters::AdmittedExecutor;
        let temp = crate::test_tmp::tempdir().unwrap();
        let script = b"#!/bin/sh\ni=0\nwhile [ $i -lt 400 ]; do echo \"   Compiling dependency-$i v1.0.0\" >&2; i=$((i+1)); done\n";
        let spec = |name: &str| {
            let program = temp.path().join(name);
            std::fs::write(&program, script).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
            adapters::CommandSpec {
                program,
                argv: vec!["install".into()],
                env: Default::default(),
                cwd: temp.path().to_path_buf(),
            }
        };
        let out = HelperExecutor.execute(&spec("cargo")).unwrap();
        assert!(out.success);
        assert_eq!(
            HelperExecutor.execute(&spec("mise")).err(),
            Some("installer_output_limit")
        );
    }
    #[test]
    fn preparation_files_are_bounded_owned_private_regular_and_not_links() {
        let temp = crate::test_tmp::tempdir().unwrap();
        let file = temp.path().join("request");
        std::fs::write(&file, b"{}").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(private_json::<serde_json::Value>(&file).is_ok());
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert!(private_json::<serde_json::Value>(&link).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(private_json::<serde_json::Value>(&file).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let hard = temp.path().join("hard");
        std::fs::hard_link(&file, &hard).unwrap();
        assert!(private_json::<serde_json::Value>(&file).is_err());
        std::fs::remove_file(hard).unwrap();
        std::fs::write(&file, vec![b' '; MAX_REQUEST as usize + 1]).unwrap();
        assert!(private_json::<serde_json::Value>(&file).is_err());
        assert!(private_json::<serde_json::Value>(temp.path()).is_err());
    }
}

#[cfg(test)]
mod admission_tests {
    use super::{admission_step, AdmissionStep, ADMISSION_WAIT};
    use std::time::Duration;

    #[test]
    fn only_an_install_that_failed_by_itself_has_its_own_exit() {
        use super::{adapters, install_exit, HelperFailure, INSTALL_FAILED_EXIT};
        assert_eq!(INSTALL_FAILED_EXIT, 3);
        for by_itself in [
            adapters::COMMAND_FAILED,
            adapters::VERSION_MISMATCH,
            adapters::OTHER_DEVICE,
        ] {
            assert_eq!(install_exit(by_itself), INSTALL_FAILED_EXIT, "{by_itself}");
        }
        assert_eq!(adapters::COMMAND_FAILED, "installation_command_failed");
        assert_eq!(adapters::VERSION_MISMATCH, "installed_version_mismatch");
        assert_eq!(adapters::OTHER_DEVICE, "installation_cleanup_other_device");
        for other in [
            "installation_partial_cleanup",
            "install_lock_wait",
            "receipt_write",
            "installer_not_admitted",
            "installer_spawn",
            "installer_output_limit",
            "preparation_request_binding",
            "",
        ] {
            assert_eq!(install_exit(other), 1, "{other}");
        }
        // What fails before or around the install exits with 1, whatever
        // it says.
        let before: HelperFailure = adapters::COMMAND_FAILED.into();
        assert_eq!(
            (before.exit, before.why.as_str()),
            (1, adapters::COMMAND_FAILED)
        );
        let before: HelperFailure = String::from("x").into();
        assert_eq!((before.exit, before.why.as_str()), (1, "x"));
    }

    #[test]
    fn a_helper_runs_only_admitted_waits_for_its_records_and_leaves_when_another_is_the_child() {
        let just_under = ADMISSION_WAIT - Duration::from_millis(1);
        // Admitted: it goes, whatever else.
        assert_eq!(
            admission_step(true, false, Duration::ZERO),
            AdmissionStep::Go
        );
        assert_eq!(
            admission_step(true, false, ADMISSION_WAIT),
            AdmissionStep::Go
        );
        // Not yet: it waits well beyond the few seconds a busy machine
        // takes to write the records, then gives up.
        assert_eq!(
            admission_step(false, false, Duration::ZERO),
            AdmissionStep::Wait
        );
        assert_eq!(
            admission_step(false, false, Duration::from_secs(30)),
            AdmissionStep::Wait
        );
        assert_eq!(
            admission_step(false, false, just_under),
            AdmissionStep::Wait
        );
        assert_eq!(
            admission_step(false, false, ADMISSION_WAIT),
            AdmissionStep::GiveUp
        );
        // Another process is the admitted child: never this one.
        assert_eq!(
            admission_step(false, true, Duration::ZERO),
            AdmissionStep::GiveUp
        );
    }
}
