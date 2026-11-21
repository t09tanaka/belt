use super::workers_lifecycle::{json, Fx};
use belt::cli::{Cli, Command, GroupSub};
use clap::Parser;
use std::{
    io::Write,
    process::{Command as Process, Stdio},
};

#[test]
fn repeated_remove_and_skip_flags() {
    let parsed = Cli::try_parse_from([
        "belt",
        "group",
        "rotate-key",
        "--remove",
        "a",
        "--remove",
        "b",
        "--skip",
        "c",
        "--skip",
        "d",
    ])
    .unwrap();
    assert!(
        matches!(parsed.command, Command::Group { command: Some(GroupSub::RotateKey { remove, skip }) } if remove == ["a", "b"] && skip == ["c", "d"])
    );
}

#[test]
fn helper_bounds_input_and_never_echoes_invalid_secret_input() {
    let f = Fx::new();
    for input in [vec![b' '; 65537], b"{\"secret-do-not-print\": 1}".to_vec()] {
        let mut child = Process::new(env!("CARGO_BIN_EXE_belt"))
            .arg("group-rotate-helper")
            .env_clear()
            .env("HOME", f.path("home"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&input).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(!out.status.success());
        assert_eq!(json(&out)["ok"], false);
        assert!(!String::from_utf8_lossy(&out.stdout).contains("secret-do-not-print"));
        assert!(!String::from_utf8_lossy(&out.stderr).contains("secret-do-not-print"));
    }
    assert!(!f.path("home/.config/belt/coordinator.json").exists());
}

#[test]
fn send_key_takes_one_member() {
    let parsed = Cli::try_parse_from(["belt", "group", "send-key", "mini-b"]).unwrap();
    assert!(
        matches!(parsed.command, Command::Group { command: Some(GroupSub::SendKey { member }) } if member == "mini-b")
    );
    assert!(Cli::try_parse_from(["belt", "group", "send-key"]).is_err());
}
