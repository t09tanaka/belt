//! Real private fixture managers, through the setup supervisor and TTC.
use belt::error::ApiError;
use belt::ids::{AttemptId, Digest, WorkerId};
use belt::setup::builder::{BuilderWorkspaces, SetupConfig, Setups};
use belt::toolchain::adapters::{self, AdapterKind, FileIdentity, HostTools, ResolutionEvidence};
use belt::toolchain::preparation::{PreparationRequest, ToolPreparationPoll};
use belt::toolchain::versions::{
    Constraint, Declaration, FrozenToolchains, Location, MatchPolicy, Scope, ToolId,
};
use belt::wire::AttemptPayload;
use belt::workspace::Workspace;
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
const BIN: &str = env!("CARGO_BIN_EXE_belt");
struct NoWorkspace;
impl BuilderWorkspaces for NoWorkspace {
    fn create(&self, _: &AttemptId, _: &AttemptPayload) -> Result<Workspace, ApiError> {
        panic!("tool preparation created workspace")
    }
    fn remove(&self, _: &Workspace) -> Result<(), ApiError> {
        panic!("tool preparation removed workspace")
    }
}
fn executable(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
struct Fx {
    dir: tempfile::TempDir,
    cfg: SetupConfig,
    setups: Arc<Setups>,
    request: PreparationRequest,
    consumers: Vec<AttemptId>,
}
impl Fx {
    fn new(kind: AdapterKind, tool: &str, mode: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("belt-tool-preparation-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .unwrap();
        let base = std::fs::canonicalize(dir.path()).unwrap();
        let mut cfg = SetupConfig::new(
            base.join("state"),
            WorkerId::parse("w-tool").unwrap(),
            PathBuf::from(BIN),
        );
        cfg.profile_definition = Some(Digest::of_bytes(b"definition"));
        cfg.profile_fingerprint = Some(Digest::of_bytes(b"actual execution"));
        cfg.poll_ms = 20;
        cfg.term_grace_ms = 500;
        cfg.kill_wait_ms = 5000;
        cfg.deadline_ms = 60_000;
        let setups = Setups::open(cfg.clone(), Arc::new(NoWorkspace)).unwrap();
        super::common::fixture_traffic_config(&cfg.state_dir);
        let root = setups.private_tool_root().unwrap();
        let host = base.join("host");
        std::fs::create_dir(&host).unwrap();
        // All writes and probes stay under the fixture's private state. No host manager runs.
        let script = format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> '{host}/calls'
printf '%s\n' "$HOME" "$PATH" >> '{host}/environment'
{mode}
[ -d '{host}' ] || exit 0
if [ -n "${{NODENV_ROOT:-}}" ]; then
  version="$1"; bin="$NODENV_ROOT/versions/$version/bin"; names=node
elif [ -n "${{RUSTUP_HOME:-}}" ]; then
  version="${{RUSTUP_TOOLCHAIN%%-*}}"; bin="$RUSTUP_HOME/toolchains/$RUSTUP_TOOLCHAIN/bin"; names='rustc cargo'
elif [ -n "${{MISE_DATA_DIR:-}}" ]; then
  version="${{2#*@}}"; bin="$3/bin"; names=python3
elif [ -n "${{FVM_CACHE_PATH:-}}" ]; then
  version="$2"; bin="$FVM_CACHE_PATH/versions/$version/bin"; names=flutter
else
  /bin/mkdir -p "$COREPACK_HOME"
  if [ "$1" = install ]; then exit 0; fi
  version=1.2.3; bin="$3"; names="$4"
fi
/bin/mkdir -p "$bin"
for name in $names; do
  case "$name" in
    rustc|cargo) output="$name $version (fixture)";;
    python3) output="Python $version";;
    flutter) output='{{"frameworkVersion":"1.2.3"}}';;
    *) output="$version";;
  esac
  /usr/bin/printf '#!/bin/sh\nprintf '\''%%s\\n'\'' '\''%s'\''\n' "$output" > "$bin/$name"
  /bin/chmod 700 "$bin/$name"
done
"#,
            host = host.display(),
            mode = match mode {
                "fail" => "exit 19".to_string(),
                "flaky" => format!("if [ -e '{}/fail' ]; then exit 19; fi", host.display()),
                "wrong" => "# wrong probe is injected below".to_string(),
                "long" => "/usr/bin/yes x | /usr/bin/head -c 9000; exit 0".to_string(),
                "hang" => crate::fixture_process::script(&format!(
                    "while [ -d '{0}' ]; do /bin/sleep 0.05; done &\nprintf '%s\\n' \"$!\" > '{0}/child.pending'\n/bin/mv '{0}/child.pending' '{0}/child'\nwait",
                    host.display()
                )),
                "gate" => format!(
                    "while [ ! -f '{0}/release' ] && [ -d '{0}' ]; do /bin/sleep 0.05; done",
                    host.display()
                ),
                _ => String::new(),
            }
        );
        let script = if mode == "wrong" {
            script.replace("output=\"$version\"", "output=9.9.9")
        } else {
            script
        };
        for name in ["manager", "node-build", "node"] {
            executable(&host.join(name), &script);
        }
        let tools = HostTools {
            manager: FileIdentity::capture(&host.join("manager")).unwrap(),
            dependencies: ["node-build", "node"]
                .iter()
                .map(|n| FileIdentity::capture(&host.join(n)).unwrap())
                .collect(),
            search_path: std::iter::once(host)
                .chain(crate::common::system_search_path())
                .collect(),
            rust_host: "fixture-host".into(),
        };
        let tool = ToolId::parse(tool).unwrap();
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(b"source"),
            Digest::of_bytes(b"parent capture definition"),
            scope.clone(),
            &[Declaration {
                tool: tool.clone(),
                constraint: Constraint::parse("1.2.3").unwrap(),
                scope,
                location: Location {
                    path: "captured".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let evidence = ResolutionEvidence {
            contract: frozen.digest(),
            tool: tool.clone(),
            concrete: node_semver::Version::parse("1.2.3").unwrap(),
            catalog: Digest::of_bytes(b"fixture authority"),
            resolved_inputs: BTreeMap::new(),
        };
        let frozen = adapters::resolve(&frozen, &evidence).unwrap();
        let plan = adapters::plan(kind, &frozen, &tool, &evidence, &root, &tools).unwrap();
        let request = PreparationRequest {
            kind,
            frozen,
            tool,
            evidence,
            private_root: root,
            host: tools,
            expected_plan: plan.key(),
            profile_definition: cfg.profile_definition.clone().unwrap(),
            base_profile: cfg.profile_fingerprint.clone().unwrap(),
        };
        Self {
            dir,
            cfg,
            setups,
            request,
            consumers: vec![],
        }
    }
    fn consumer(&mut self) -> AttemptId {
        let id = AttemptId::generate();
        self.consumers.push(id.clone());
        id
    }
    fn poll(&self, id: &AttemptId) -> ToolPreparationPoll {
        self.setups.poll_tool(id, &self.request).unwrap()
    }
    fn done(&self, id: &AttemptId) -> ToolPreparationPoll {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let p = self.poll(id);
            if p != ToolPreparationPoll::Pending {
                return p;
            }
            assert!(Instant::now() < deadline, "tool builder timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    fn request_for(&self, version: &str, source: &[u8]) -> PreparationRequest {
        let tool = self.request.tool.clone();
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(source),
            self.request.frozen.profile_definition.clone(),
            scope.clone(),
            &[Declaration {
                tool: tool.clone(),
                constraint: Constraint::parse(version).unwrap(),
                scope,
                location: Location {
                    path: "captured".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let evidence = ResolutionEvidence {
            contract: frozen.digest(),
            tool: tool.clone(),
            concrete: node_semver::Version::parse(version).unwrap(),
            catalog: Digest::of_bytes(b"fixture authority"),
            resolved_inputs: BTreeMap::new(),
        };
        let frozen = adapters::resolve(&frozen, &evidence).unwrap();
        let plan = adapters::plan(
            self.request.kind,
            &frozen,
            &tool,
            &evidence,
            &self.request.private_root,
            &self.request.host,
        )
        .unwrap();
        PreparationRequest {
            frozen,
            evidence,
            expected_plan: plan.key(),
            ..self.request.clone()
        }
    }
    fn calls(&self) -> usize {
        std::fs::read_to_string(self.dir.path().join("host/calls"))
            .unwrap_or_default()
            .lines()
            .count()
    }
    fn plan(&self) -> adapters::InstallPlan {
        adapters::plan(
            self.request.kind,
            &self.request.frozen,
            &self.request.tool,
            &self.request.evidence,
            &self.request.private_root,
            &self.request.host,
        )
        .unwrap()
    }
}
impl Drop for Fx {
    fn drop(&mut self) {
        for id in &self.consumers {
            self.setups.release(id);
        }
        // Keep fixture files until all builders have settled; no unowned process signalling.
        let deadline = Instant::now() + Duration::from_secs(15);
        while self
            .dir
            .path()
            .join("state/setup/builders")
            .read_dir()
            .unwrap()
            .next()
            .is_some()
        {
            if Instant::now() >= deadline {
                let preserved = self.dir.path().to_path_buf();
                eprintln!("unsettled tool fixture retained: {}", preserved.display());
                let old = std::mem::replace(&mut self.dir, crate::test_tmp::tempdir().unwrap());
                let _ = old.keep();
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn all_five_adapters_execute_under_managed_supervisor_and_reuse_verified_receipts() {
    for (kind, tool, calls) in [
        (AdapterKind::Nodenv, "node", 1),
        (AdapterKind::Rustup, "rust", 1),
        (AdapterKind::Mise, "python", 1),
        (AdapterKind::Fvm, "flutter", 1),
        (AdapterKind::Corepack, "pnpm", 2),
    ] {
        let mut f = Fx::new(kind, tool, "");
        let a = f.consumer();
        let b = f.consumer();
        assert_eq!(f.poll(&a), ToolPreparationPoll::Pending);
        assert_eq!(f.poll(&b), ToolPreparationPoll::Pending);
        assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
        assert!(matches!(f.done(&b), ToolPreparationPoll::Installed(_)));
        assert_eq!(f.setups.builds_started(), 1);
        assert_eq!(f.calls(), calls);
        let original = f.setups.preparation_observations(64);
        assert_eq!(
            original.len(),
            1,
            "two consumers share one producer observation"
        );
        assert_eq!(original[0].policy, "sum-ttc-admitted-run-wall-v1");
        assert_eq!(original[0].node_count, 1);
        assert_eq!(original[0].boot, belt::platform::boot_id().unwrap());
        for _ in 0..100 {
            assert_eq!(f.setups.preparation_observations(usize::MAX), original);
        }
        assert!(f.setups.preparation_observations(0).is_empty());
        let reopened = Setups::open(f.cfg.clone(), Arc::new(NoWorkspace)).unwrap();
        assert!(matches!(
            reopened.poll_tool(&a, &f.request).unwrap(),
            ToolPreparationPoll::Installed(_)
        ));
        assert_eq!(reopened.builds_started(), 0);
        assert!(
            reopened.preparation_observations(64).is_empty(),
            "warm receipt is not a duration observation"
        );
        assert_eq!(f.calls(), calls);
        let env = std::fs::read_to_string(f.dir.path().join("host/environment")).unwrap();
        let plan = f.plan();
        // Private roots and explicitly selected manager paths may live below
        // the harness HOME. Compare environment values, not path substrings.
        let expected: Vec<_> = plan
            .commands()
            .iter()
            .flat_map(|command| {
                assert_eq!(Path::new(&command.env["HOME"]), plan.root().join("home"));
                [command.env["HOME"].as_str(), command.env["PATH"].as_str()]
            })
            .collect();
        assert_eq!(env.lines().collect::<Vec<_>>(), expected);
        let traffic = std::fs::read_dir(f.cfg.traffic_state_dir.join("managed")).unwrap();
        let sidecars: Vec<task_traffic_controller::managed::Sidecar> = traffic
            .map(|e| serde_json::from_slice(&std::fs::read(e.unwrap().path()).unwrap()).unwrap())
            .collect();
        assert_eq!(
            sidecars.len(),
            1,
            "one supervised admitted operation per install"
        );
        assert!(sidecars[0].child.is_some());
        assert_eq!(
            sidecars[0].phase,
            task_traffic_controller::receipt::Phase::Terminal
        );
        let paths = task_traffic_controller::paths::Paths::new(
            &f.cfg.traffic_state_dir,
            f.cfg.traffic_state_dir.join("config.toml"),
        );
        assert!(
            task_traffic_controller::ledger::Ledger::load(&paths.ledger_file())
                .unwrap()
                .entries
                .is_empty(),
            "TTC permit released after drain"
        );
    }
}

#[test]
fn manager_failures_probe_mismatch_and_output_limit_fail_closed_for_each_consumer() {
    for mode in ["fail", "wrong", "long"] {
        let mut f = Fx::new(AdapterKind::Nodenv, "node", mode);
        let a = f.consumer();
        assert!(
            matches!(f.done(&a), ToolPreparationPoll::Failed(_)),
            "{mode}"
        );
        assert!(!f.plan().root().join("receipt.json").exists());
        assert!(f.setups.preparation_observations(64).is_empty());
        // The consumer that saw the failure keeps it; nothing is run again
        // for it.
        let ToolPreparationPoll::Failed(why) = f.poll(&a) else {
            panic!("{mode}")
        };
        assert_eq!(f.setups.builds_started(), 1);
        // The helper has its own exit for a manager that failed and for a
        // probe that does not answer the version. Too much output is any
        // other failure of the helper (exit 1).
        assert_eq!(
            why.0,
            if mode == "long" {
                "a setup step exited with 1"
            } else {
                "a setup step exited with 3"
            },
            "{mode}"
        );
        if mode == "long" {
            // Not a failure of the install itself: nothing is remembered,
            // and the next consumer starts the install again, with no word
            // about a later try.
            let calls = f.calls();
            let other = f.consumer();
            let ToolPreparationPoll::Failed(again) = f.done(&other) else {
                panic!("{mode}")
            };
            assert_eq!(again, why, "{mode}");
            assert!(!again.0.contains("tried again"), "{again:?}");
            assert_eq!(f.setups.builds_started(), 2, "{mode}");
            assert!(f.calls() > calls, "{mode}");
            assert!(!f.plan().root().join("receipt.json").exists());
            assert!(f.setups.preparation_observations(64).is_empty());
            continue;
        }
        // A later consumer is told the same failure at once, with when the
        // version is tried again: the install that just failed by itself is
        // not run again for everybody who asks.
        let calls = f.calls();
        let other = f.consumer();
        let ToolPreparationPoll::Failed(held) = f.poll(&other) else {
            panic!("{mode}: a failed install was started again")
        };
        assert!(held.0.contains("it is tried again in 600 s"), "{held:?}");
        let ToolPreparationPoll::Failed(first) = f.poll(&a) else {
            panic!("{mode}")
        };
        assert!(held.0.starts_with(&first.0), "{held:?} / {first:?}");
        assert_eq!(f.poll(&other), ToolPreparationPoll::Failed(held));
        assert_eq!(f.setups.builds_started(), 1, "{mode}");
        assert_eq!(f.calls(), calls, "{mode}");
        // An explicit request (a warm-up) lifts it: the install runs again
        // from a cleared root, and this manager fails again.
        f.setups.retry_tool(&f.request);
        let third = f.consumer();
        assert!(matches!(f.done(&third), ToolPreparationPoll::Failed(_)));
        assert_eq!(f.setups.builds_started(), 2, "{mode}");
        assert!(f.calls() > calls, "{mode}");
        assert!(!f.plan().root().join("receipt.json").exists());
        assert!(f.setups.preparation_observations(64).is_empty());
    }
}

/// The failure of an install is held for a time, not for ever: afterwards
/// the next consumer starts the install again.
#[test]
fn a_failed_install_is_tried_again_for_a_new_consumer_once_the_hold_is_over() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "flaky");
    f.cfg.tool_failure_hold_ms = 1500;
    f.setups = Setups::open(f.cfg.clone(), Arc::new(NoWorkspace)).unwrap();
    let fail = f.dir.path().join("host/fail");
    std::fs::write(&fail, b"").unwrap();
    let a = f.consumer();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Failed(_)));
    let failed = Instant::now();
    std::fs::remove_file(&fail).unwrap();
    let b = f.consumer();
    let first = f.poll(&b);
    if failed.elapsed() < Duration::from_millis(1000) {
        let ToolPreparationPoll::Failed(held) = first else {
            panic!("the hold was not kept: {first:?}")
        };
        assert!(
            held.0.contains("tried again in 1 s") || held.0.contains("tried again in 2 s"),
            "{held:?}"
        );
        assert_eq!(f.setups.builds_started(), 1);
    }
    // The same consumer asks again after the hold: the manager, repaired
    // by now, installs the version.
    assert!(
        matches!(
            {
                let deadline = Instant::now() + Duration::from_secs(90);
                loop {
                    match f.poll(&b) {
                        ToolPreparationPoll::Failed(held) if held.0.contains("tried again in") => {
                            assert!(Instant::now() < deadline, "the hold never ended");
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        other => break other,
                    }
                }
            },
            ToolPreparationPoll::Pending | ToolPreparationPoll::Installed(_)
        ),
        "after the hold a new installer is started"
    );
    assert!(matches!(f.done(&b), ToolPreparationPoll::Installed(_)));
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(f.calls(), 2);
}

fn builders_settled(f: &Fx) -> bool {
    f.dir
        .path()
        .join("state/setup/builders")
        .read_dir()
        .unwrap()
        .next()
        .is_none()
}

/// H1: a failed install, and one stopped half-way, leave a private root
/// without a receipt. The next consumer of the same version installs it.
#[test]
fn a_failed_or_stopped_install_does_not_block_the_same_version_afterwards() {
    // Failed by the manager, which is then repaired.
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "flaky");
    let fail = f.dir.path().join("host/fail");
    std::fs::write(&fail, b"").unwrap();
    let a = f.consumer();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Failed(_)));
    let root = f.plan().root().to_path_buf();
    assert!(root.join("home").exists(), "the failed install left a root");
    assert!(!root.join("receipt.json").exists());
    std::fs::write(root.join("home/stale"), b"left by the failed install").unwrap();
    std::fs::remove_file(&fail).unwrap();
    // The failure is held for a while; the explicit request of a warm-up
    // lifts it, and the repaired manager is run at once.
    f.setups.retry_tool(&f.request);
    let b = f.consumer();
    assert!(
        matches!(f.done(&b), ToolPreparationPoll::Installed(_)),
        "the same version must install after a failed install"
    );
    assert!(!root.join("home/stale").exists(), "the root was reused");
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(f.calls(), 2);
    drop(f);

    // Stopped while the manager ran (the last consumer went away).
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "gate");
    let a = f.consumer();
    assert_eq!(f.poll(&a), ToolPreparationPoll::Pending);
    wait_until(&f, "manager called exactly once", || f.calls() == 1);
    let root = f.plan().root().to_path_buf();
    f.setups.release(&a);
    wait_until(&f, "builder directories settled", || builders_settled(&f));
    assert!(
        root.join("home").exists(),
        "the stopped install left a root"
    );
    assert!(!root.join("receipt.json").exists());
    std::fs::write(root.join("home/stale"), b"left by the stopped install").unwrap();
    std::fs::write(f.dir.path().join("host/release"), b"").unwrap();
    let b = f.consumer();
    assert!(matches!(f.done(&b), ToolPreparationPoll::Installed(_)));
    assert!(!root.join("home/stale").exists(), "the root was reused");
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(f.calls(), 2);
}

/// A worker that restarts in the middle of an install: the old installer is
/// stopped and settled first, then the version is installed for whoever asks.
#[test]
fn a_restart_in_the_middle_of_an_install_ends_with_the_version_installed() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "gate");
    let a = f.consumer();
    assert_eq!(f.poll(&a), ToolPreparationPoll::Pending);
    wait_until(&f, "manager called exactly once", || f.calls() == 1);
    let reopened = Setups::open(f.cfg.clone(), Arc::new(NoWorkspace)).unwrap();
    let b = AttemptId::generate();
    // The old writer still works in the root: no second one beside it.
    assert_eq!(
        reopened.poll_tool(&b, &f.request).unwrap(),
        ToolPreparationPoll::Pending
    );
    assert_eq!(reopened.builds_started(), 0);
    wait_until(&f, "reopened builders settled", || {
        reopened.unsettled().is_empty()
    });
    assert!(!f.plan().root().join("receipt.json").exists());
    std::fs::write(f.dir.path().join("host/release"), b"").unwrap();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(90);
    let done = loop {
        let poll = reopened.poll_tool(&b, &f.request).unwrap();
        if poll != ToolPreparationPoll::Pending {
            break poll;
        }
        assert!(
            Instant::now() < deadline,
            "tool builder timed out: restarted install completed; waited {:?} (deadline 90s)\n{}",
            started.elapsed(),
            fixture_diagnostics(&f),
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        matches!(done, ToolPreparationPoll::Installed(_)),
        "{done:?}"
    );
    assert_eq!(reopened.builds_started(), 1);
    assert_eq!(f.calls(), 2);
    reopened.release(&b);
}

#[test]
fn receipt_tree_external_identity_and_consumer_binding_are_revalidated() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "");
    let a = f.consumer();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
    let mut changed = f.request.clone();
    changed.frozen.source_manifest = Digest::of_bytes(b"changed source");
    // A changed frozen source requires new resolution evidence; even malformed rebinding is rejected.
    assert!(f.setups.poll_tool(&a, &changed).is_err());
    std::fs::write(f.plan().probes()[0].1.program.clone(), b"changed").unwrap();
    assert!(matches!(f.poll(&a), ToolPreparationPoll::Failed(_)));
    let b = f.consumer();
    assert!(matches!(f.poll(&b), ToolPreparationPoll::Failed(_)));
    let mut bad = f.request.clone();
    bad.expected_plan = Digest::of_bytes(b"wrong key");
    assert!(f.setups.poll_tool(&b, &bad).is_err());
    executable(&f.request.host.manager.path, "#!/bin/sh\nexit 0\n");
    assert!(f.setups.poll_tool(&a, &f.request).is_err());
}

#[test]
fn direct_hidden_helper_never_executes_manager_without_admission() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "gate");
    let a = f.consumer();
    f.poll(&a);
    let deadline = Instant::now() + Duration::from_secs(30);
    let request = loop {
        let path = std::fs::read_dir(f.dir.path().join("state/setup/builders"))
            .unwrap()
            .flatten()
            .map(|e| e.path().join("tool-request.json"))
            .find(|p| p.exists());
        if let Some(p) = path {
            if p.parent().unwrap().join("spec.json").exists() {
                break p;
            }
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    };
    let result = std::process::Command::new(BIN)
        .args(["toolchain-install", "--request"])
        .arg(request)
        .arg("--binding")
        .arg(f.request.digest().as_str())
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("installer_not_admitted"));
    std::fs::write(f.dir.path().join("host/release"), b"").unwrap();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
    assert_eq!(f.calls(), 1);
}

// Best-effort snapshots only: never recover, open a writable store, or acquire
// admission locks while reporting a timeout. Cleanup may race these reads.
fn diagnostic_tail(path: &Path) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let read = || -> std::io::Result<String> {
        let mut file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        let offset = len.saturating_sub(32 * 1024);
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::new();
        file.take(32 * 1024).read_to_end(&mut bytes)?;
        let text = String::from_utf8_lossy(&bytes);
        let lines: Vec<_> = text.lines().collect();
        Ok(format!(
            "{}{}",
            if offset > 0 || lines.len() > 50 {
                "[truncated]\n"
            } else {
                ""
            },
            lines[lines.len().saturating_sub(50)..].join("\n")
        ))
    };
    read().unwrap_or_else(|e| format!("[unavailable: {e}]"))
}

fn diagnostic_query(db: &rusqlite::Connection, sql: &str, attempt: &str) -> String {
    let read = || -> rusqlite::Result<Vec<String>> {
        let mut stmt = db.prepare(sql)?;
        let columns = stmt.column_count();
        let rows = stmt.query_map([attempt], |row| {
            (0..columns)
                .map(|i| {
                    row.get::<_, rusqlite::types::Value>(i)
                        .map(|v| format!("{v:?}"))
                })
                .collect::<rusqlite::Result<Vec<_>>>()
                .map(|values| values.join(" | "))
        })?;
        rows.collect()
    };
    match read() {
        Ok(rows) if rows.is_empty() => "[no records]".into(),
        Ok(rows) => rows.join("\n"),
        Err(e) => format!("[unavailable: {e}]"),
    }
}

fn fixture_diagnostics(f: &Fx) -> String {
    let mut out = format!(
        "fixture: {}\nhost/calls (last 50 lines, at most 32 KiB):\n{}\n",
        f.dir.path().display(),
        diagnostic_tail(&f.dir.path().join("host/calls")),
    );
    let entries = match std::fs::read_dir(f.cfg.state_dir.join("setup/builders")) {
        Ok(entries) => entries,
        Err(e) => return format!("{out}builders unavailable: {e}"),
    };
    let mut found = false;
    for entry in entries {
        let dir = match entry {
            Ok(entry) => entry.path(),
            Err(e) => {
                out.push_str(&format!("builder entry unavailable: {e}\n"));
                continue;
            }
        };
        found = true;
        out.push_str(&format!(
            "builder: {}\nmeta.json: {}\n",
            dir.display(),
            diagnostic_tail(&dir.join("meta.json"))
        ));
        let spec = std::fs::read(dir.join("spec.json"))
            .map_err(|e| e.to_string())
            .and_then(|bytes| {
                serde_json::from_slice::<belt::supervisor::spec::SuperviseSpec>(&bytes)
                    .map_err(|e| e.to_string())
            });
        let spec = match spec {
            Ok(spec) => spec,
            Err(e) => {
                out.push_str(&format!("spec unavailable: {e}\n"));
                continue;
            }
        };
        let attempt = spec.attempt_id.as_str();
        out.push_str(&format!("attempt: {attempt}\n"));
        match rusqlite::Connection::open_with_flags(
            dir.join("worker.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ) {
            Ok(db) => {
                // Do not let lock contention obscure the original timeout.
                if let Err(e) = db.busy_timeout(Duration::ZERO) {
                    out.push_str(&format!("diagnostic busy timeout unavailable: {e}\n"));
                }
                for (label, sql) in [
                    ("events (last 50, newest first)", "SELECT seq, at_ms, kind, payload_json FROM event WHERE attempt_id = ?1 ORDER BY seq DESC LIMIT 50"),
                    ("supervisor_unresolved reasons (last 50, newest first)", "SELECT seq, at_ms, payload_json FROM event WHERE attempt_id = ?1 AND kind = 'supervisor_unresolved' ORDER BY seq DESC LIMIT 50"),
                    ("admission worker attempt", "SELECT state, termination_json, cancel_requested_at_ms FROM worker_attempt WHERE attempt_id = ?1 LIMIT 50"),
                    ("admission command launches", "SELECT node_id, intent_at_ms, boot_id, pid, start_identity, identity_at_ms FROM command_launch WHERE attempt_id = ?1 LIMIT 50"),
                ] {
                    out.push_str(&format!("{label}:\n{}\n", diagnostic_query(&db, sql, attempt)));
                }
            }
            Err(e) => out.push_str(&format!("worker.db unavailable: {e}\n")),
        }
        // Use only this fixture's configured paths; never resolve host defaults.
        let paths = task_traffic_controller::paths::Paths::new(
            &spec.traffic_state_dir,
            spec.traffic_config_path(),
        );
        let command = belt::supervisor::spec::command_id(&spec.attempt_id, 0);
        out.push_str(&format!(
            "admission {command} sidecar:\n{}\nadmission fixture ledger:\n{}\n",
            diagnostic_tail(&paths.managed_file(&command)),
            diagnostic_tail(&paths.ledger_file()),
        ));
    }
    if !found {
        out.push_str("[no builder directories]\n");
    }
    out
}

#[track_caller]
fn wait_until(f: &Fx, label: &str, mut ready: impl FnMut() -> bool) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "fixture condition timed out: {label}; waited {:?} (deadline 30s) at {}\n{}",
            started.elapsed(),
            std::panic::Location::caller(),
            fixture_diagnostics(f),
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn fixture_child(f: &Fx) -> (u32, u64) {
    let path = f.dir.path().join("host/child");
    wait_until(f, "fixture child PID file exists", || path.exists());
    let pid = std::fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let start = task_traffic_controller::process::start_time(pid).unwrap();
    (pid, start)
}
fn same_live(pid: u32, start: u64) -> bool {
    matches!(
        belt::platform::read_process(pid).start,
        belt::platform::process_read::StartIdentity::Identity(_)
    ) && task_traffic_controller::process::start_time(pid) == Some(start)
}

#[test]
fn releasing_one_consumer_preserves_install_and_final_release_drains_owned_children() {
    {
        let mut f = Fx::new(AdapterKind::Nodenv, "node", "gate");
        let a = f.consumer();
        let b = f.consumer();
        f.poll(&a);
        f.poll(&b);
        wait_until(&f, "manager called exactly once", || f.calls() == 1);
        f.setups.release(&a);
        std::fs::write(f.dir.path().join("host/release"), b"").unwrap();
        assert!(matches!(f.done(&b), ToolPreparationPoll::Installed(_)));
        assert_eq!(f.setups.builds_started(), 1);
    }
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "hang");
    let a = f.consumer();
    f.poll(&a);
    let (pid, start) = fixture_child(&f);
    f.setups.release(&a);
    let b = f.consumer();
    assert_eq!(f.poll(&b), ToolPreparationPoll::Pending);
    assert_eq!(f.setups.builds_started(), 1);
    wait_until(&f, "recorded child no longer live", || {
        !same_live(pid, start)
    });
    wait_until(&f, "builder directories settled", || builders_settled(&f));
    assert_eq!(f.calls(), 1);
    assert_eq!(f.setups.builds_started(), 1);
    assert!(
        f.plan().root().exists(),
        "the stopped installer does not remove its own root"
    );
    assert!(!f.plan().root().join("receipt.json").exists());
    assert!(f.setups.preparation_observations(64).is_empty());
}

#[test]
fn restart_keeps_same_key_barrier_until_old_writer_settles_then_installs_again() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "hang");
    let a = f.consumer();
    f.poll(&a);
    let (pid, start) = fixture_child(&f);
    let reopened = Setups::open(f.cfg.clone(), Arc::new(NoWorkspace)).unwrap();
    assert!(!reopened.unsettled().is_empty());
    let b = f.consumer();
    assert_eq!(
        reopened.poll_tool(&b, &f.request).unwrap(),
        ToolPreparationPoll::Pending
    );
    assert_eq!(reopened.builds_started(), 0);
    wait_until(&f, "reopened builders settled", || {
        reopened.unsettled().is_empty()
    });
    // The barrier goes only after the old writer is seen gone: at the moment
    // nothing is unsettled any more, its child is not alive.
    assert!(
        !same_live(pid, start),
        "the old installer was settled while its child was alive"
    );
    assert_eq!(f.calls(), 1);
    assert!(f.plan().root().exists());
    assert!(!f.plan().root().join("receipt.json").exists());
    // Only now a new installer is started for the consumer that waited.
    assert_eq!(
        reopened.poll_tool(&b, &f.request).unwrap(),
        ToolPreparationPoll::Pending
    );
    assert_eq!(reopened.builds_started(), 1);
    reopened.release(&b);
    wait_until(&f, "builder directories settled", || builders_settled(&f));
    assert!(f.setups.preparation_observations(64).is_empty());
    assert!(reopened.preparation_observations(64).is_empty());
}

#[test]
fn supervisor_deadline_drains_installer_descendants_without_publishing_receipt() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "hang");
    // The supervisor's deadline counts from its own start, so it also covers
    // launching the supervisor process and the manager script that starts the
    // child. A deadline shorter than that startup (seen on a loaded machine or
    // for a freshly built binary) fires before the child exists, and the
    // fixture then never sees it. This one is far longer than any startup, so
    // the child is always recorded first, and still ends the run well inside
    // `done`'s own limit.
    f.cfg.deadline_ms = 10_000;
    f.setups = Setups::open(f.cfg.clone(), Arc::new(NoWorkspace)).unwrap();
    let a = f.consumer();
    f.poll(&a);
    let (pid, start) = fixture_child(&f);
    assert!(matches!(f.done(&a), ToolPreparationPoll::Failed(_)));
    wait_until(&f, "recorded child no longer live", || {
        !same_live(pid, start)
    });
    assert!(!f.plan().root().join("receipt.json").exists());
}

#[test]
fn source_contracts_share_artifact_but_attempt_bindings_and_concrete_versions_stay_separate() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "gate");
    let other_source = f.request_for("1.2.3", b"another source");
    assert_eq!(f.request.expected_plan, other_source.expected_plan);
    let a = f.consumer();
    let b = f.consumer();
    f.poll(&a);
    assert!(
        f.setups.poll_tool(&a, &other_source).is_err(),
        "same consumer cannot change source contract"
    );
    assert_eq!(
        f.setups.poll_tool(&b, &other_source).unwrap(),
        ToolPreparationPoll::Pending
    );
    wait_until(&f, "manager called exactly once", || f.calls() == 1);
    std::fs::write(f.dir.path().join("host/release"), b"").unwrap();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
    wait_until(&f, "other source installed", || {
        matches!(
            f.setups.poll_tool(&b, &other_source).unwrap(),
            ToolPreparationPoll::Installed(_)
        )
    });
    assert_eq!(f.setups.builds_started(), 1);
    let node20 = f.request_for("20.0.0", b"node20 source");
    let node22 = f.request_for("22.0.0", b"node22 source");
    assert_ne!(node20.expected_plan, node22.expected_plan);
    let c = f.consumer();
    let d = f.consumer();
    f.setups.poll_tool(&c, &node20).unwrap();
    f.setups.poll_tool(&d, &node22).unwrap();
    wait_until(&f, "node 20 installed", || {
        matches!(
            f.setups.poll_tool(&c, &node20).unwrap(),
            ToolPreparationPoll::Installed(_)
        )
    });
    wait_until(&f, "node 22 installed", || {
        matches!(
            f.setups.poll_tool(&d, &node22).unwrap(),
            ToolPreparationPoll::Installed(_)
        )
    });
    assert_eq!(f.setups.builds_started(), 3);
    assert_eq!(f.calls(), 3);
    assert_eq!(node20.base_profile, node22.base_profile);
    assert_eq!(node20.base_profile, f.request.base_profile);
}

#[test]
fn helper_death_still_drains_recorded_owned_manager_children() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "hang");
    let a = f.consumer();
    f.poll(&a);
    let (pid, start) = fixture_child(&f);
    let managed = f.cfg.traffic_state_dir.join("managed");
    let deadline = Instant::now() + Duration::from_secs(30);
    let sidecar = loop {
        assert!(
            Instant::now() < deadline,
            "owned descendant was not recorded"
        );
        let sidecar = std::fs::read_dir(&managed)
            .unwrap()
            .flatten()
            .filter_map(|e| std::fs::read(e.path()).ok())
            .filter_map(|b| {
                serde_json::from_slice::<task_traffic_controller::managed::Sidecar>(&b).ok()
            })
            .find(|s| {
                s.descendants
                    .iter()
                    .any(|d| d.pid == pid && d.start_time == Some(start))
            });
        if let Some(s) = sidecar {
            break s;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let helper = sidecar.child.unwrap();
    assert!(helper.pid > 1);
    let identity = crate::common::identity_of(helper.pid).unwrap();
    crate::common::kill_recorded(helper.pid, &identity);
    assert!(matches!(f.done(&a), ToolPreparationPoll::Failed(_)));
    wait_until(&f, "recorded child no longer live", || {
        !same_live(pid, start)
    });
    assert_eq!(f.calls(), 1);
    assert!(!f.plan().root().join("receipt.json").exists());
    // A later consumer is not failed by the root the dead helper left: a
    // new installer is started, clears it and runs the manager again.
    let b = f.consumer();
    let second = f.poll(&b);
    assert_eq!(second, ToolPreparationPoll::Pending, "{second:?}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while f.calls() < 2 {
        let now = f.poll(&b);
        assert_eq!(
            now,
            ToolPreparationPoll::Pending,
            "the second installer ended before the manager ran: {now:?}"
        );
        assert!(Instant::now() < deadline, "the manager did not run again");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(f.setups.builds_started(), 2);
    assert_eq!(f.calls(), 2);
    f.setups.release(&b);
    wait_until(&f, "builder directories settled", || builders_settled(&f));
}

#[test]
fn parent_capture_definition_and_local_worker_definition_remain_separate() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "");
    let parent = f.request.frozen.clone();
    let parent_digest = parent.digest();
    assert_ne!(parent.profile_definition, f.request.profile_definition);
    assert_eq!(
        f.cfg.profile_definition.as_ref(),
        Some(&f.request.profile_definition)
    );
    assert_eq!(
        f.cfg.profile_fingerprint.as_ref(),
        Some(&f.request.base_profile)
    );
    let a = f.consumer();
    let mut wrong_local = f.request.clone();
    wrong_local.profile_definition = parent.profile_definition.clone();
    assert_ne!(
        wrong_local.digest(),
        f.request.digest(),
        "helper request binding includes the local definition"
    );
    assert_eq!(
        wrong_local.expected_plan, f.request.expected_plan,
        "local preparation identity does not namespace the reusable artifact"
    );
    assert!(f.setups.poll_tool(&a, &wrong_local).is_err());
    assert_eq!(f.setups.builds_started(), 0);
    assert_eq!(
        f.calls(),
        0,
        "wrong local definition must fail before manager execution"
    );
    assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
    assert_eq!(f.calls(), 1);
    assert_eq!(f.request.frozen, parent);
    assert_eq!(f.request.frozen.digest(), parent_digest);
    let b = f.consumer();
    assert!(matches!(f.done(&b), ToolPreparationPoll::Installed(_)));
    assert_eq!(f.setups.builds_started(), 1);
    assert_eq!(
        f.calls(),
        1,
        "the unchanged parent contract reuses the verified receipt"
    );
    assert!(f.setups.poll_tool(&b, &wrong_local).is_err());
    assert_eq!(f.calls(), 1);
    assert_eq!(f.request.frozen, parent);
    assert_eq!(wrong_local.frozen, parent);
}

#[test]
fn worker_private_registry_builds_request_for_real_setup_preparation() {
    use belt::toolchain::profile::Profile;
    use belt::toolchain::registry::Registry;
    for (kind, tool) in [
        (AdapterKind::Nodenv, "node"),
        (AdapterKind::Rustup, "rust"),
        (AdapterKind::Mise, "python"),
        (AdapterKind::Fvm, "flutter"),
        (AdapterKind::Corepack, "pnpm"),
    ] {
        let mut f = Fx::new(kind, tool, "");
        let state = &f.cfg.state_dir;
        let host = f.request.host.manager.path.parent().unwrap();
        let profiles = format!(
        "schema_version=1\nactive='fixture'\n[profiles.fixture]\npath=[{:?}]\nshell='/bin/sh'\ncache_namespace='fixture'\n",
        host.to_str().unwrap()
    );
        std::fs::write(state.join("profiles.toml"), profiles).unwrap();
        std::fs::set_permissions(
            state.join("profiles.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let profile = Profile::load(state, &BTreeMap::new()).unwrap();
        let config = format!(
        "schema_version=1\nactive_profile='fixture'\nprofile_definition={:?}\n[[recipes]]\ntool={:?}\nkind={:?}\nmanager={:?}\ndependencies=[{:?},{:?}]\nsearch_path=[{:?}{}]\ninventory=['1.2.3']\nrust_host='fixture-host'\n",
        profile.definition_digest().as_str(), tool, format!("{kind:?}"), f.request.host.manager.path.to_str().unwrap(), host.join("node-build").to_str().unwrap(), host.join("node").to_str().unwrap(), host.to_str().unwrap(), crate::common::system_search_path_toml()
    );
        std::fs::write(state.join("toolchains.toml"), config).unwrap();
        std::fs::set_permissions(
            state.join("toolchains.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        // The fixture supplies the same trusted measured identity to both checked factories.
        let registry =
            Registry::load(state, &profile, f.cfg.profile_fingerprint.clone().unwrap()).unwrap();
        f.cfg.profile_definition = Some(profile.definition_digest().clone());
        f.setups = Setups::open(f.cfg.clone(), Arc::new(NoWorkspace)).unwrap();
        let frozen = f.request.frozen.clone();
        let parent_evidence = f.request.evidence.clone();
        f.request = registry
            .request(&frozen, &f.request.tool, Some(&parent_evidence))
            .unwrap();
        assert_eq!(f.request.frozen, frozen);
        assert_ne!(
            f.request.profile_definition,
            f.request.frozen.profile_definition
        );
        assert!(matches!(
            registry.detect(&f.request).unwrap(),
            adapters::Availability::Missing
        ));
        assert_eq!(f.calls(), 0);
        let consumer = f.consumer();
        assert!(matches!(
            f.done(&consumer),
            ToolPreparationPoll::Installed(_)
        ));
        assert!(matches!(
            registry.detect(&f.request).unwrap(),
            adapters::Availability::Installed(_)
        ));
        let second = f.consumer();
        assert!(matches!(f.done(&second), ToolPreparationPoll::Installed(_)));
        assert_eq!(f.calls(), if kind == AdapterKind::Corepack { 2 } else { 1 });
    }
}

#[test]
fn preparation_observation_excludes_actual_ttc_admission_wait() {
    use task_traffic_controller::config::Config;
    use task_traffic_controller::{managed, Context};
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "");
    let cfg = Config {
        budget: 1,
        cpu_threshold_percent: 100.0,
        memory_reserve_mib: 0,
        ..Config::default()
    };
    std::fs::create_dir_all(&f.cfg.traffic_state_dir).unwrap();
    std::fs::write(
        f.cfg.traffic_state_dir.join("config.toml"),
        toml::to_string(&cfg).unwrap(),
    )
    .unwrap();
    let ctx = Context::new(task_traffic_controller::paths::Paths::new(
        &f.cfg.traffic_state_dir,
        f.cfg.traffic_state_dir.join("config.toml"),
    ));
    let id = "preparation-admission-blocker".to_string();
    let command = managed::CommandSpec {
        command_id: id.clone(),
        spec_digest: "private-fixture".into(),
        opts: task_traffic_controller::exec::Opts {
            label: None,
            weight: Some(1),
            parent: None,
            front: false,
            key: None,
            argv: vec!["/usr/bin/true".into()],
            cwd: f.dir.path().into(),
            env: None,
            stdout_log: None,
        },
        attempt: None,
        services: vec![],
    };
    let managed::Registration::Registered(_, guard) =
        managed::register(&ctx, &cfg, &command).unwrap()
    else {
        panic!("fresh blocker")
    };
    assert!(matches!(
        managed::try_admit(&ctx, cfg.limits(), &id, 0.0).unwrap(),
        managed::TryAdmit::Admitted
    ));
    let a = f.consumer();
    let start = Instant::now();
    assert_eq!(f.poll(&a), ToolPreparationPoll::Pending);
    wait_until(&f, "installer queued alongside admission blocker", || {
        task_traffic_controller::ledger::Ledger::load(&ctx.paths.ledger_file())
            .unwrap()
            .entries
            .len()
            == 2
    });
    // Hold the fixture's own permit; the manager cannot start during this wait.
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(f.calls(), 0);
    assert!(f.setups.preparation_observations(64).is_empty());
    managed::finish(&ctx, guard, managed::TerminalKind::Cancelled).unwrap();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
    let observations = f.setups.preparation_observations(64);
    assert_eq!(observations.len(), 1);
    let elapsed = u64::try_from(start.elapsed().as_millis()).unwrap();
    assert!(
        elapsed >= observations[0].duration_ms + 1400,
        "admission wait must not enter admitted run duration: {elapsed} vs {:?}",
        observations[0]
    );
}

#[test]
fn preparation_telemetry_publish_failure_preserves_successful_installation() {
    let mut f = Fx::new(AdapterKind::Nodenv, "node", "gate");
    let a = f.consumer();
    assert_eq!(f.poll(&a), ToolPreparationPoll::Pending);
    wait_until(&f, "manager called exactly once", || f.calls() == 1);
    let dir = std::fs::read_dir(f.cfg.state_dir.join("setup/builders"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let node_digest = Digest::of_bytes(b"tool-install").as_str().replace(':', "-");
    // Occupy the immutable telemetry destination only, leaving computation files untouched.
    std::fs::create_dir(dir.join(format!("preparation-{node_digest}.json"))).unwrap();
    std::fs::write(f.dir.path().join("host/release"), b"release").unwrap();
    assert!(matches!(f.done(&a), ToolPreparationPoll::Installed(_)));
    assert!(matches!(
        adapters::detect(&f.plan()),
        adapters::Availability::Installed(_)
    ));
    assert!(f.setups.preparation_observations(64).is_empty());
}
