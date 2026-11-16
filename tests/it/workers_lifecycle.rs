//! The real binary's `workers` commands against a fake `ssh` placed first on
//! PATH. The fake never starts a remote helper, so no launchctl, LaunchAgent,
//! ~/.ssh or sudo is involved. Remote install/start/stop with a fake launcher
//! is covered in the library tests of the service modules.
use remote_task_runner::error::ErrorCode;
use remote_task_runner::ids::{ClusterId, WorkerId};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Output;

const BIN: &str = env!("CARGO_BIN_EXE_remote-task-runner");

struct Fx {
    dir: super::common::Tmp,
}
impl Fx {
    fn new() -> Self {
        let dir = super::common::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("home")).unwrap();
        std::fs::create_dir(dir.path().join("bin")).unwrap();
        Fx { dir }
    }
    fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.path().join(name)
    }
    fn config(&self, text: &str) {
        let dir = self.path("home/.config/remote-task-runner");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("config.toml"), text).unwrap();
    }
    /// A fake ssh: records its argv (one per line) and runs `body`.
    fn ssh(&self, body: &str) {
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\n{body}\n",
            self.path("argv").display()
        );
        let path = self.path("bin/ssh");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    fn run(&self, args: &[&str]) -> Output {
        std::process::Command::new(BIN)
            .arg("--json")
            .args(args)
            .env_clear()
            .env("HOME", self.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.path("bin").display()),
            )
            .output()
            .unwrap()
    }
    fn argv(&self) -> Vec<String> {
        std::fs::read_to_string(self.path("argv"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

fn answer(worker: &str, draining: bool) -> String {
    format!(
        "printf '%s\\n' '{}'",
        serde_json::json!({"ok": true, "status": {
            "worker_id": WorkerId::parse(worker).unwrap(),
            "cluster": ClusterId::generate(),
            "binary_sha256": "0".repeat(64),
            "loaded": true, "ready": true, "draining": draining,
            "cancel_running": false, "live_attempts": 0
        }})
    )
}

fn json(out: &Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout).expect("stdout is one JSON document")
}

#[test]
fn an_unregistered_worker_is_not_found_and_no_ssh_runs() {
    let fx = Fx::new();
    fx.ssh("exit 0");
    for args in [
        &["workers", "drain", "ghost"][..],
        &["workers", "resume", "ghost"],
        &["workers", "start", "ghost"],
        &["workers", "stop", "ghost", "--cancel-running"],
    ] {
        let out = fx.run(args);
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::NotFound.exit_code()),
            "{args:?}"
        );
        assert_eq!(json(&out)["error"]["code"], "NOT_FOUND");
    }
    assert!(!fx.path("argv").exists());
}

#[test]
fn drain_runs_one_fixed_ssh_command_and_reports_the_helper_answer() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\n");
    fx.ssh(&answer("mini-a", true));
    let out = fx.run(&["workers", "drain", "mini-a"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = json(&out);
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"]["draining"], true);
    let argv = fx.argv();
    let at = argv
        .iter()
        .position(|a| a == "--")
        .expect("destination follows --");
    assert_eq!(argv[at + 1], "ci@mini-a.local");
    assert_eq!(argv.len(), at + 3, "one remote command word: {argv:?}");
    assert!(argv[at + 2].contains("'worker-service' 'drain'"));
    assert!(!argv[at + 2].contains("mini-a"));
    assert!(argv.iter().any(|a| a == "BatchMode=yes"));
}

#[test]
fn stop_with_cancel_running_is_requested_explicitly() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    fx.ssh(&answer("mini-a", false));
    assert_eq!(
        fx.run(&["workers", "stop", "mini-a"]).status.code(),
        Some(0)
    );
    assert!(!fx.argv().last().unwrap().contains("--cancel-running"));
    assert_eq!(
        fx.run(&["workers", "stop", "mini-a", "--cancel-running"])
            .status
            .code(),
        Some(0)
    );
    assert!(fx.argv().last().unwrap().contains("'--cancel-running'"));
}

#[test]
fn an_unreachable_worker_is_never_reported_as_stopped() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    fx.ssh("exit 255");
    for args in [
        &["workers", "stop", "mini-a"][..],
        &["workers", "drain", "mini-a"],
        &["workers", "start", "mini-a"],
        &["workers", "resume", "mini-a"],
    ] {
        let out = fx.run(args);
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::CommunicationUnknown.exit_code()),
            "{args:?}"
        );
        let value = json(&out);
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "COMMUNICATION_UNKNOWN");
    }
}

#[test]
fn a_silent_ssh_success_is_not_a_success() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    fx.ssh("exit 0");
    let out = fx.run(&["workers", "stop", "mini-a"]);
    assert_eq!(
        out.status.code(),
        Some(ErrorCode::CommunicationUnknown.exit_code())
    );
}

#[test]
fn add_needs_an_initialized_coordinator_and_changes_nothing_without_it() {
    let fx = Fx::new();
    fx.ssh("exit 0");
    let out = fx.run(&["workers", "add", "mini-a", "--ssh", "ci@h", "--install"]);
    assert_eq!(out.status.code(), Some(ErrorCode::NotFound.exit_code()));
    assert!(!fx.path("argv").exists());
    assert!(!Path::new(&fx.path("home/.config/remote-task-runner/config.toml")).exists());
}

#[test]
fn text_output_goes_to_stdout_on_success_and_stderr_on_failure() {
    let fx = Fx::new();
    fx.config("schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@h\"\n");
    fx.ssh(&answer("mini-a", true));
    let run = |args: &[&str]| {
        std::process::Command::new(BIN)
            .args(args)
            .env_clear()
            .env("HOME", fx.path("home"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", fx.path("bin").display()),
            )
            .output()
            .unwrap()
    };
    let ok = run(&["workers", "drain", "mini-a"]);
    assert_eq!(
        String::from_utf8_lossy(&ok.stdout).trim(),
        "worker mini-a draining"
    );
    let bad = run(&["workers", "drain", "ghost"]);
    assert!(bad.stdout.is_empty() && !bad.stderr.is_empty());
}
