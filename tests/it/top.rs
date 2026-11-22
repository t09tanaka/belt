//! Exercise the installed CLI with a real terminal, resident and worker.
#[tokio::test]
async fn top_refreshes_the_live_task_and_restores_the_terminal_on_each_quit_key() {
    let (_root, spec, input) = super::bootstrap::control_fixture_with(|spec| {
        // An initialized client pins its cluster before observing its resident.
        let store =
            belt::storage::CoordinatorStore::open(&spec.state_dir, belt::platform::wall_ms())
                .unwrap();
        spec.expected_cluster = Some(store.cluster_id().clone());
    });
    super::bootstrap::start_resident(&spec).await;
    let harness = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/it/top_pty.py");
    let out = std::process::Command::new("python3")
        .args([harness.as_os_str(), spec.executable.as_os_str()])
        .current_dir(input)
        .env_clear()
        .envs(&spec.env)
        .env("TERM", "xterm-256color")
        .output()
        .expect("start the PTY harness");
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
