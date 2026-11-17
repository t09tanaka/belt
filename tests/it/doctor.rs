//! The real binary's `doctor` in a private HOME with a private PATH. A fake
//! `task-traffic-controller` and a fake `ssh` stand in for the real ones; no
//! service, no ssh and no state outside the temporary directory is touched.
use super::workers_lifecycle::{json, Fx};
use remote_task_runner::error::ErrorCode;
use std::os::unix::fs::PermissionsExt;

fn tool(fx: &Fx, name: &str, body: &str) {
    let path = fx.path(&format!("bin/{name}"));
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A docker client that reaches no engine, first on PATH, so a real one on
/// the machine that runs the test never decides the result.
fn no_engine(fx: &Fx) {
    tool(fx, "docker", "exit 1");
}

fn check<'a>(report: &'a serde_json::Value, host: &str, id: &str) -> &'a serde_json::Value {
    report["result"]["checks"]
        .as_array()
        .expect("checks")
        .iter()
        .find(|c| c["host"] == host && c["id"] == id)
        .unwrap_or_else(|| panic!("no {host} {id} in {report}"))
}

#[test]
fn an_uninitialized_parent_is_told_to_run_init() {
    let fx = Fx::new();
    let out = fx.run(&["doctor"]);
    assert_eq!(out.status.code(), Some(ErrorCode::NotFound.exit_code()));
    assert_eq!(json(&out)["error"]["code"], "NOT_FOUND");
}

#[test]
fn a_parent_without_a_separate_command_is_healthy() {
    let fx = Fx::new();
    fx.init();
    no_engine(&fx);
    let out = fx.run(&["doctor"]);
    let report = json(&out);
    assert_eq!(out.status.code(), Some(0), "{report}");
    assert_eq!(
        check(&report, "parent", "docker_resource_domain")["level"],
        "notice"
    );
    assert_eq!(report["ok"], true);
    assert_eq!(report["result"]["healthy"], true);
    assert_eq!(check(&report, "parent", "traffic_runtime")["level"], "ok");
    assert_eq!(check(&report, "parent", "service_profile")["level"], "ok");
    // The worker shares the command's scope.
    assert_eq!(
        check(&report, "parent", "traffic_state_path")["level"],
        "ok"
    );
}

#[test]
fn an_unpatched_task_traffic_controller_fails_doctor() {
    // A05: the binary that does not know managed entries is not compatible.
    // A36: its `status --json` is still read as the old contract prints it.
    let fx = Fx::new();
    fx.init();
    tool(
        &fx,
        "task-traffic-controller",
        "[ \"$1 $2\" = 'status --json' ] || exit 2\nprintf '%s\\n' '{\"budget\":8,\"entries\":[]}'",
    );
    let out = fx.run(&["doctor"]);
    let report = json(&out);
    assert_eq!(out.status.code(), Some(ErrorCode::Blocked.exit_code()));
    assert_eq!(report["ok"], false);
    assert_eq!(report["error"]["code"], "BLOCKED");
    assert_eq!(report["error"]["blocked_reason"], "environment_mismatch");
    assert_eq!(report["result"]["healthy"], false);
    let runtime = check(&report, "parent", "traffic_runtime");
    assert_eq!(runtime["level"], "fail");
    assert!(runtime["detail"]
        .as_str()
        .unwrap()
        .contains("does not know managed entries"));
    // The scope check passes: the worker uses the command's scope.
    assert_eq!(
        check(&report, "parent", "traffic_state_path")["level"],
        "ok"
    );
}

#[test]
fn an_updated_task_traffic_controller_passes_the_runtime_check() {
    let fx = Fx::new();
    fx.init();
    tool(
        &fx,
        "task-traffic-controller",
        "printf '%s\\n' '{\"ledger_format\":1,\"entries\":[]}'",
    );
    let report = json(&fx.run(&["doctor"]));
    assert_eq!(check(&report, "parent", "traffic_runtime")["level"], "ok");
}

#[test]
fn an_unreachable_worker_fails_and_the_diff_slot_is_listed_as_not_checked() {
    let fx = Fx::new();
    fx.init();
    fx.config(
        "schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\nbinary = \"/w/bin/current/remote-task-runner\"\n",
    );
    fx.ssh("exit 255");
    let out = fx.run(&["doctor"]);
    let report = json(&out);
    assert_eq!(out.status.code(), Some(ErrorCode::Blocked.exit_code()));
    assert_eq!(check(&report, "mini-a", "reachable")["level"], "fail");
    assert_eq!(
        check(&report, "mini-a", "environment_diff")["level"],
        "not_checked"
    );
    assert_eq!(report["result"]["not_checked"], 1);
    // One fixed command through the registered binary.
    assert_eq!(
        fx.argv().last().unwrap(),
        "exec '/w/bin/current/remote-task-runner' 'worker-service' 'doctor'"
    );
}

#[test]
fn text_mode_lists_every_check_and_exits_by_the_result() {
    let fx = Fx::new();
    fx.init();
    no_engine(&fx);
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
            .arg("doctor")
            // Outside any project: nothing about a project is reported.
            .current_dir(fx.path("home"))
            .env_clear()
            .env("HOME", fx.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", fx.path("bin").display()),
            )
            .output()
            .unwrap()
    };
    let out = run();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    assert!(stdout.contains("[ok] parent traffic_runtime: "));
    assert!(stdout.contains("0 failed, 0 not checked, "));
    tool(&fx, "task-traffic-controller", "exit 1");
    let out = run();
    assert_eq!(out.status.code(), Some(ErrorCode::Blocked.exit_code()));
    assert!(String::from_utf8_lossy(&out.stdout).contains("[FAIL] parent traffic_runtime: "));
    assert!(String::from_utf8_lossy(&out.stderr).contains("check(s) failed"));
}

#[test]
fn the_host_helper_without_an_installed_worker_says_so() {
    let fx = Fx::new();
    let out = fx.run(&["worker-service", "doctor"]);
    assert_eq!(out.status.code(), Some(1));
    let answer = json(&out);
    assert_eq!(answer["ok"], false);
    assert!(answer["error"]
        .as_str()
        .unwrap()
        .contains("no worker is installed here"));
}

#[test]
fn a_legacy_key_of_the_project_is_noticed() {
    // Spec: `link-node-modules` is shown by doctor only when it has a value.
    let fx = Fx::new();
    fx.init();
    let project = fx.path("project");
    std::fs::create_dir(&project).unwrap();
    let run = || {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
            .args(["--json", "doctor"])
            .current_dir(&project)
            .env_clear()
            .env("HOME", fx.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", fx.path("bin").display()),
            )
            .output()
            .unwrap();
        json(&out)
    };
    let legacy = |report: &serde_json::Value| {
        report["result"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["id"] == "legacy_config")
            .cloned()
            .collect::<Vec<_>>()
    };
    assert!(legacy(&run()).is_empty());
    std::fs::write(project.join(".local-ci.toml"), "link-node-modules = true\n").unwrap();
    let found = legacy(&run());
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0]["level"], "notice");
    assert!(found[0]["detail"]
        .as_str()
        .unwrap()
        .contains("link-node-modules"));
}

#[test]
fn a_check_that_was_not_made_is_not_healthy_and_exits_apart_from_a_failure() {
    let fx = Fx::new();
    fx.init();
    // An engine answers, but the client does not say where it connects.
    tool(
        &fx,
        "docker",
        "case \"$1\" in info) echo ENGINE1;; *) exit 1;; esac",
    );
    let out = fx.run(&["doctor"]);
    let report = json(&out);
    let domain = check(&report, "parent", "docker_resource_domain");
    assert_eq!(domain["level"], "not_checked", "{report}");
    assert!(domain["detail"]
        .as_str()
        .unwrap()
        .contains("could not be determined"));
    assert_eq!(report["ok"], false);
    assert_eq!(report["result"]["healthy"], false);
    assert_eq!(report["result"]["failed"], 0);
    assert_eq!(report["result"]["not_checked"], 1);
    assert_eq!(report["error"]["code"], "COMMAND_FAILED");
    assert!(report["error"].get("blocked_reason").is_none());
    assert_eq!(out.status.code(), Some(1));
    assert_ne!(ErrorCode::Blocked.exit_code(), 1);
}

/// The service environment a coordinator retains: what `workers diff` measures
/// the parent in.
fn retained_env(fx: &Fx) -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([
        ("HOME".to_string(), fx.path("home").display().to_string()),
        (
            "PATH".to_string(),
            format!("{}:/usr/bin:/bin", fx.path("bin").display()),
        ),
    ])
}

fn answering_worker(retained: bool) -> (Option<i32>, serde_json::Value) {
    use remote_task_runner::service::doctor::{Docker, HostFacts, ProfileFacts, Scope};
    use remote_task_runner::toolchain::envdiff::{self, EnvReport};
    let fx = Fx::new();
    let cluster = fx.init_with_env(if retained {
        retained_env(&fx)
    } else {
        Default::default()
    });
    no_engine(&fx);
    fx.config(
        "schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\nbinary = \"/w/bin/current/remote-task-runner\"\n",
    );
    let scope = Scope {
        state_dir: "/s".into(),
        config_file: "/s/config.toml".into(),
        in_use: false,
        policy_digest: Some("sha256:p".into()),
        ledger_entries: Some(0),
    };
    let doctor = serde_json::json!({"ok": true, "doctor": {
        "status": {
            "worker_id": "mini-a", "cluster": cluster,
            "binary_sha256": "0".repeat(64),
            "loaded": true, "ready": true, "draining": false,
            "cancel_running": false, "live_attempts": 0
        },
        "facts": HostFacts {
            schema: 1,
            worker_scope: scope.clone(),
            cli_scope: scope,
            cli: None,
            docker: Docker::default(),
            profile: ProfileFacts {
                name: Some("default".into()),
                fingerprint: Some("sha256:f".into()),
                error: None,
            },
        },
    }});
    // No machine that runs this test is this platform.
    let env = serde_json::json!({"ok": true, "env": EnvReport {
        schema_version: envdiff::SCHEMA,
        os: "plan9".into(),
        arch: "mips".into(),
        tools: Default::default(),
        helpers: Default::default(),
        docker: envdiff::Docker::NotRunnable,
        disk_free_bytes: Some(envdiff::MIN_DISK_BYTES),
        profile: "default".into(),
        required_tools: vec![],
        profile_digest: None,
        policy_digest: None,
    }});
    fx.ssh(&format!(
        "case \"$*\" in\n*\"'worker-service' 'doctor'\") printf '%s\\n' '{doctor}';;\n*\"'worker-service' 'env'\") printf '%s\\n' '{env}';;\n*) exit 1;;\nesac"
    ));
    let out = fx.run(&["doctor"]);
    (out.status.code(), json(&out))
}

#[test]
fn a_worker_that_answers_has_its_environment_compared_with_the_parent_s() {
    let (code, report) = answering_worker(true);
    assert_eq!(code, Some(ErrorCode::Blocked.exit_code()), "{report}");
    assert_eq!(check(&report, "mini-a", "reachable")["level"], "ok");
    let compared: Vec<_> = report["result"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["host"] == "mini-a" && c["id"] == "environment_diff")
        .collect();
    let platform: Vec<_> = compared
        .iter()
        .filter(|c| c["detail"].as_str().unwrap().starts_with("platform: "))
        .collect();
    assert_eq!(platform.len(), 1, "{report}");
    assert_eq!(platform[0]["level"], "fail");
    assert!(
        platform[0]["detail"]
            .as_str()
            .unwrap()
            .contains("worker=plan9/mips"),
        "{report}"
    );
    // The comparison was made: nothing says it was left out.
    assert!(
        compared
            .iter()
            .all(|c| !c["detail"].as_str().unwrap().contains("left out")),
        "{report}"
    );
    // The policy digest is not measured by either side yet: listed as a
    // notice, neither a pass nor a reason to exit 1.
    assert!(compared.iter().any(|c| c["level"] == "notice"
        && c["detail"].as_str().unwrap().starts_with("policy_digest: ")
        && c["detail"].as_str().unwrap().contains("not measured yet")));
    assert_eq!(report["result"]["not_checked"], 0, "{report}");
}

#[test]
fn without_a_retained_service_environment_the_parent_is_not_measured_in_the_shell_s() {
    let (code, report) = answering_worker(false);
    // Not compared at all, and said so: one check that was not made.
    let compared: Vec<_> = report["result"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["host"] == "mini-a" && c["id"] == "environment_diff")
        .collect();
    assert_eq!(compared.len(), 1, "{report}");
    assert_eq!(compared[0]["level"], "not_checked");
    assert!(
        compared[0]["detail"]
            .as_str()
            .unwrap()
            .contains("the parent's environment could not be measured"),
        "{report}"
    );
    assert_ne!(code, Some(0));
}
