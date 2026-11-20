//! CLI parsing and bounded helper input; private homes, no service manager or SSH.
use super::workers_lifecycle::{json, Fx};
use belt::cli::{Cli, Command, GroupSub};
use clap::Parser;
use std::{
    io::Write,
    process::{Command as Process, Stdio},
};

#[test]
fn join_and_init_arguments() {
    let parsed = Cli::try_parse_from([
        "belt", "group", "join", "user@a", "--ssh", "user@b", "--move",
    ])
    .unwrap();
    assert!(
        matches!(parsed.command, Command::Group { command: Some(GroupSub::Join { existing_member_ssh, ssh: Some(ssh), move_group: true }) } if existing_member_ssh == "user@a" && ssh == "user@b")
    );
    let parsed = Cli::try_parse_from(["belt", "group", "join", "user@a"]).unwrap();
    assert!(matches!(
        parsed.command,
        Command::Group {
            command: Some(GroupSub::Join {
                ssh: None,
                move_group: false,
                ..
            })
        }
    ));
    assert!(Cli::try_parse_from(["belt", "group", "join", "user@a", "--name", "b"]).is_err());
    assert!(
        matches!(Cli::try_parse_from(["belt", "init", "--name", "b"]).unwrap().command, Command::Init { name: Some(name) } if name == "b")
    );
    assert!(matches!(
        Cli::try_parse_from(["belt", "group"]).unwrap().command,
        Command::Group { command: None }
    ));
}

#[test]
fn missing_init_refuses_before_ssh() {
    let f = Fx::new();
    f.ssh("exit 99");
    let out = f.run(&["group", "join", "user@a"]);
    assert!(!out.status.success());
    assert!(json(&out)["error"]["message"]
        .as_str()
        .unwrap()
        .contains("belt init first"));
    assert!(!f.path("argv").exists());
    assert!(!f.path("home/.config/belt/coordinator.json").exists());
}

#[test]
fn helper_rejects_oversized_stdin() {
    let f = Fx::new();
    let mut child = Process::new(env!("CARGO_BIN_EXE_belt"))
        .arg("group-join-helper")
        .env_clear()
        .env("HOME", f.path("home"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&vec![b' '; 8193])
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    assert_eq!(
        json(&out)["error"]["message"],
        "group join request too large"
    );
    assert!(!f.path("home/.config/belt/coordinator.json").exists());
}
