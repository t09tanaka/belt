//! The real binary's `workers upgrade` against a fake `ssh` placed first on
//! PATH. The fake never starts a remote helper. The whole sequence, with a
//! fake launcher and failures at every step, is covered in the library tests
//! (`service::workers::upgrade`, `service::worker_service::upgrade`).
use super::workers_lifecycle::{json, Fx};
use remote_task_runner::error::ErrorCode;

const REGISTERED: &str =
    "schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\n";

#[test]
fn an_unregistered_worker_is_not_found_and_no_ssh_runs() {
    let fx = Fx::new();
    fx.init();
    fx.ssh("exit 0");
    let out = fx.run(&["workers", "upgrade", "ghost"]);
    assert_eq!(out.status.code(), Some(ErrorCode::NotFound.exit_code()));
    assert_eq!(json(&out)["error"]["code"], "NOT_FOUND");
    assert!(!fx.path("argv").exists());
}

#[test]
fn an_unreachable_worker_is_never_reported_as_upgraded() {
    let fx = Fx::new();
    fx.init();
    fx.config(REGISTERED);
    fx.ssh("exit 255");
    let out = fx.run(&["workers", "upgrade", "mini-a"]);
    let value = json(&out);
    assert_eq!(value["ok"], false);
    if cfg!(target_os = "macos") {
        assert_eq!(
            out.status.code(),
            Some(ErrorCode::CommunicationUnknown.exit_code())
        );
        assert_eq!(value["error"]["code"], "COMMUNICATION_UNKNOWN");
        // Only the read-only probe ran: one fixed command, no helper action.
        let argv = fx.argv();
        assert!(argv.last().unwrap().starts_with("uname -s; uname -m;"));
        assert!(!argv.iter().any(|a| a.contains("worker-service")));
    } else {
        // Only macOS parents upgrade workers; nothing is sent from elsewhere.
        assert_eq!(out.status.code(), Some(ErrorCode::InvalidInput.exit_code()));
        assert!(!fx.path("argv").exists());
    }
}

#[test]
fn a_host_that_answers_garbage_to_the_first_question_changes_nothing() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let fx = Fx::new();
    fx.init();
    fx.config(REGISTERED);
    // Staged already, then every helper action prints something unreadable.
    let arch = if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        std::env::consts::ARCH
    };
    fx.ssh(&format!(
        "cat >/dev/null; for a in \"$@\"; do last=$a; done\n\
         printf '%s\\n' \"$last\" >> {}\n\
         case \"$last\" in uname*) printf 'Darwin\\n{arch}\\nstaged\\n';; *) echo 'not json';; esac",
        fx.path("commands").display()
    ));
    let out = fx.run(&["workers", "upgrade", "mini-a"]);
    assert_eq!(
        out.status.code(),
        Some(ErrorCode::CommunicationUnknown.exit_code())
    );
    let commands = std::fs::read_to_string(fx.path("commands")).unwrap();
    let commands: Vec<&str> = commands.lines().collect();
    assert_eq!(commands.len(), 2, "{commands:?}");
    assert!(commands[1].ends_with(" worker-service status --wait-secs 600"));
    assert!(commands[1].starts_with("exec \"$HOME/.remote-task-runner-bootstrap/rtr-"));
}

#[test]
fn text_mode_prints_the_error_on_stderr_only() {
    let fx = Fx::new();
    fx.init();
    fx.ssh("exit 0");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_remote-task-runner"))
        .args(["workers", "upgrade", "ghost"])
        .env_clear()
        .env("HOME", fx.path("home"))
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", fx.path("bin").display()),
        )
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(ErrorCode::NotFound.exit_code()));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).starts_with("error: "));
}
