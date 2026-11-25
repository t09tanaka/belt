use assert_cmd::Command;

#[test]
fn workers_lists_registrations_without_starting_a_resident() {
    let root = crate::test_tmp::tempdir().unwrap();
    let config = root.path().join(".config/belt");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("config.toml"),
        "schema_version = 1\n[[workers]]\nid = 'mini-a'\nssh = 'user@mini-a'\nephemeral = true\n",
    )
    .unwrap();
    let out = Command::cargo_bin("belt")
        .unwrap()
        .env_clear()
        .env("HOME", root.path())
        .args(["workers", "--json"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{:?}", out);
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        value["result"]["workers"],
        serde_json::json!([{
            "id":"mini-a","ssh":"user@mini-a","ephemeral":true,"connection":"unknown","identity":null,"cpu_cores":null,"memory_bytes":null
        }])
    );
}

#[test]
fn bad_input_exits_with_input_error() {
    let out = Command::cargo_bin("belt")
        .unwrap()
        .arg("exec")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
