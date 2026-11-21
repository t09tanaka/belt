//! Processes spawned by integration fixtures use this test binary itself.
#[test]
#[ignore]
fn fixture_sleeper() {
    let Ok(mode) = std::env::var("BELT_IT_FIXTURE") else {
        return;
    };
    if mode == "cat" {
        std::io::copy(&mut std::io::stdin().lock(), &mut std::io::stdout().lock()).unwrap();
    } else {
        let seconds: f64 = mode.parse().unwrap();
        std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
    }
    std::process::exit(0);
}

pub(crate) fn sleep_command(secs: &str) -> String {
    command(secs, false)
}

fn command(mode: &str, exec: bool) -> String {
    let exe = std::env::current_exe().unwrap();
    let exe = belt::protocol::ssh::quote(exe.to_str().unwrap());
    // Sleeping fixtures must not add libtest banners to command logs.
    let output = if mode == "cat" { "" } else { " > /dev/null" };
    let mode = belt::protocol::ssh::quote(mode);
    let exec = if exec { "exec " } else { "" };
    format!(
        "BELT_IT_FIXTURE={mode} {exec}{exe} --exact fixture_process::fixture_sleeper --ignored --quiet{output}"
    )
}

/// Replace numeric sleeps, preserving exec, shell syntax and durations.
pub(crate) fn script(script: &str) -> String {
    let normalized = script.replace("/bin/sleep ", "sleep ");
    let mut rest = normalized.as_str();
    let mut result = String::new();
    while let Some(start) = rest.find("sleep ") {
        let (prefix, tail) = rest.split_at(start);
        let tail = &tail[6..];
        let end = tail
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(tail.len());
        assert!(end > 0, "fixture sleep needs a numeric duration");
        if let Some(prefix) = prefix.strip_suffix("exec ") {
            result.push_str(prefix);
            result.push_str(&command(&tail[..end], true));
        } else {
            result.push_str(prefix);
            result.push_str(&sleep_command(&tail[..end]));
        }
        rest = &tail[end..];
    }
    result.push_str(rest);
    result
}

#[test]
fn fixture_environment_is_readable_by_the_product_marker_scan() {
    use task_traffic_controller::{marker, process};
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            if matches!(self.0.try_wait(), Ok(None)) {
                assert!(self.0.id() > 1);
                self.0.kill().unwrap();
            }
            self.0.wait().unwrap();
        }
    }
    let id = format!("fixture-{}", std::process::id());
    let scope = "fixture-environment-test";
    let child = Child(
        std::process::Command::new("sh")
            .arg("-c")
            .arg(script("exec sleep 30"))
            .env(marker::ENV_VAR, &id)
            .env(marker::SCOPE_VAR, scope)
            .spawn()
            .unwrap(),
    );
    let scan = process::scan_system(&id, scope);
    assert!(
        scan.alive.iter().any(|p| p.pid == child.0.id()),
        "fixture marker was not readable: {scan:?}"
    );
    let foreign = process::scan_system("foreign-fixture", scope);
    assert!(!foreign.alive.iter().any(|p| p.pid == child.0.id()));
    assert!(!foreign.hidden.iter().any(|p| p.proc.pid == child.0.id()));
}
