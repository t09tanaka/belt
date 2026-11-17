use assert_cmd::Command;

const CASES: &[&[&str]] = &[
    &["ci"],
    &["ci", "--snapshot", "commit", "--ref", "HEAD"],
    &["workers"],
    &["workers", "diff", "w"],
    &["workers", "sync", "w", "--project", "p"],
    &["workers", "forget", "w"],
    &["worker", "--ephemeral"],
];

#[test]
fn every_command_reports_not_implemented_as_json() {
    for args in CASES {
        let out = Command::cargo_bin("remote-task-runner")
            .unwrap()
            .arg("--json")
            .args(*args)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(70), "{args:?}");
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("stdout is JSON");
        assert_eq!(v["schema_version"], 1, "{args:?}");
        assert_eq!(v["ok"], false, "{args:?}");
        assert_eq!(v["error"]["code"], "NOT_IMPLEMENTED", "{args:?}");
        assert!(v["error"]["message"].is_string(), "{args:?}");
    }
}

#[test]
fn without_json_stdout_is_empty_and_error_goes_to_stderr() {
    let out = Command::cargo_bin("remote-task-runner")
        .unwrap()
        .args(["workers", "forget", "w"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(70));
    assert!(out.stdout.is_empty());
    assert!(!out.stderr.is_empty());
}

#[test]
fn bad_input_exits_with_input_error() {
    let out = Command::cargo_bin("remote-task-runner")
        .unwrap()
        .arg("exec")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
