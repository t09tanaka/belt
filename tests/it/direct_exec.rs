//! A50: `belt exec` on a host where no resident runs (direct execution,
//! spec chapter 4 and 16). The real binary runs in a private HOME whose
//! coordinator is initialized (so workers are configured) but not started.
use super::workers_lifecycle::{json, Fx};
use belt::service::direct::NOTICE;
use std::collections::BTreeMap;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use task_traffic_controller::admit::State;
use task_traffic_controller::ledger::Ledger;

const WORKERS: &str =
    "schema_version = 1\n[[workers]]\nid = \"mini-a\"\nssh = \"ci@mini-a.local\"\n";

fn env(fx: &Fx) -> BTreeMap<String, String> {
    BTreeMap::from([("HOME".to_string(), fx.path("home").display().to_string())])
}

/// The scope the standalone CLI uses in this HOME.
fn ledger_file(fx: &Fx) -> PathBuf {
    let scope = belt::config::traffic::resolve(&env(fx)).unwrap();
    task_traffic_controller::Paths::new(&scope.state_dir, &scope.config_file).ledger_file()
}

fn entries(fx: &Fx) -> Vec<task_traffic_controller::ledger::Entry> {
    Ledger::load(&ledger_file(fx)).unwrap().entries
}

/// An initialized host with a worker configured and no resident, and a
/// task-traffic-controller budget of `budget` in this HOME's scope.
fn host(budget: u32) -> Fx {
    let fx = Fx::new();
    fx.config(WORKERS);
    fx.init();
    crate::common::fixture_worker_traffic_budget(&env(&fx), budget);
    std::fs::create_dir_all(fx.path("project/sub")).unwrap();
    fx
}

fn belt(fx: &Fx, cwd: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_belt"));
    command
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(std::env::var_os("LLVM_PROFILE_FILE").map(|p| ("LLVM_PROFILE_FILE", p)))
        .env("HOME", fx.path("home"))
        .env("PATH", "/usr/bin:/bin");
    command
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn notices(out: &Output) -> usize {
    String::from_utf8_lossy(&out.stderr)
        .lines()
        .filter(|line| *line == NOTICE)
        .count()
}

fn last_json(out: &Output) -> serde_json::Value {
    let text = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(text.lines().last().expect("a JSON line")).unwrap()
}

#[test]
fn a50_without_a_resident_exec_runs_here_in_the_cwd_and_makes_no_run() {
    let fx = host(2);
    let cwd = fx.path("project/sub");
    // `--local` is accepted; direct execution is already local.
    let out = belt(
        &fx,
        &cwd,
        &[
            "--json",
            "exec",
            "--local",
            "--",
            "/bin/sh",
            "-c",
            "pwd -P > where.txt; echo child-output; exit 23",
        ],
    )
    .output()
    .unwrap();
    assert_eq!(out.status.code(), Some(23), "{out:?}");
    assert_eq!(notices(&out), 1, "{out:?}");
    assert_eq!(
        std::fs::read_to_string(cwd.join("where.txt"))
            .unwrap()
            .trim(),
        std::fs::canonicalize(&cwd).unwrap().to_str().unwrap()
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("child-output"));
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(result["ok"], false);
    assert_eq!(
        result["result"],
        serde_json::json!({"mode":"direct","exit_code":23,"started":true})
    );
    // No run, no snapshot, no attempt: the configured worker is not used and
    // the coordinator and the local worker are untouched.
    let ls = json(&fx.run(&["ls"]));
    assert_eq!(ls["data"]["page"]["items"], serde_json::json!([]), "{ls}");
    for state in ["coordinator", "local-worker"] {
        let names: Vec<_> = std::fs::read_dir(fx.path(state))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(
            !names.iter().any(|n| n == "attempts" || n == "sources"),
            "{state}: {names:?}"
        );
    }
    assert!(entries(&fx).is_empty());
    // Text mode: the command's own output and exit code, the one line.
    let out = belt(
        &fx,
        &cwd,
        &["exec", "--", "/bin/sh", "-c", "echo hello; exit 0"],
    )
    .output()
    .unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "hello\n");
    assert_eq!(notices(&out), 1, "{out:?}");
}

#[test]
fn a50_without_a_resident_options_that_need_a_run_and_submit_are_refused() {
    let fx = host(2);
    let cwd = fx.path("project");
    for args in [
        &[
            "--json",
            "exec",
            "--snapshot",
            "commit",
            "--",
            "touch",
            "ran",
        ][..],
        &["--json", "exec", "--in-place", "--", "touch", "ran"],
        &["--json", "submit", "--", "touch", "ran"],
    ] {
        let out = belt(&fx, &cwd, args).output().unwrap();
        assert_eq!(
            out.status.code(),
            Some(belt::error::ErrorCode::Blocked.exit_code()),
            "{args:?} {out:?}"
        );
        let error = &last_json(&out)["error"];
        assert_eq!(error["code"], "BLOCKED");
        let message = error["message"].as_str().unwrap();
        assert!(
            message.contains("no resident is running") && message.contains("belt init"),
            "{message}"
        );
        assert!(!cwd.join("ran").exists(), "{args:?}");
    }
}

/// The standalone CLI's library, as another program on the host would use
/// it: holds the budget until `release` exists.
fn hold_with_library(fx: &Fx, held: &Path, release: &Path) -> std::thread::JoinHandle<()> {
    use task_traffic_controller::exec::{self, Hooks, Opts};
    let scope = belt::config::traffic::resolve(&env(fx)).unwrap();
    let script = holder_script(held, release);
    let home = fx.path("home");
    std::thread::spawn(move || {
        let ctx = task_traffic_controller::Context::new(task_traffic_controller::Paths::new(
            &scope.state_dir,
            &scope.config_file,
        ));
        let cfg = task_traffic_controller::config::Config::load(&scope.config_file).unwrap();
        let opts = Opts {
            argv: vec!["/bin/sh".into(), "-c".into(), script],
            cwd: home.clone(),
            env: Some(BTreeMap::from([
                ("HOME".to_string(), home.display().to_string()),
                ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ])),
            ..Opts::default()
        };
        exec::run_gated(
            &ctx,
            &cfg,
            &opts,
            &exec::new_id(),
            &Hooks {
                cancelled: &|| false,
                progress: &|_| {},
                progress_tty: false,
            },
        )
        .unwrap();
    })
}

fn holder_script(held: &Path, release: &Path) -> String {
    crate::fixture_process::script(&format!(
        "touch '{}'; while [ ! -e '{}' ]; do sleep 0.05; done",
        held.display(),
        release.display()
    ))
}

/// With a budget of one, a command the standalone task-traffic-controller
/// runs keeps `belt exec` waiting in the same ledger until it ends: one
/// budget, never two commands admitted at once.
#[test]
fn a50_direct_exec_and_the_standalone_task_traffic_controller_share_one_budget() {
    let cli = crate::supervisor::ttc_cli();
    for standalone in [false, true] {
        if standalone && cli.is_none() {
            continue;
        }
        let fx = host(1);
        let held = fx.path("held");
        let release = fx.path("release");
        let cwd = fx.path("project");
        let holder: Box<dyn FnOnce()> = if standalone {
            let mut child = Command::new(cli.as_ref().unwrap())
                .args([
                    "exec",
                    "--",
                    "/bin/sh",
                    "-c",
                    &holder_script(&held, &release),
                ])
                .env_clear()
                .env("HOME", fx.path("home"))
                .env("PATH", "/usr/bin:/bin")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            Box::new(move || assert!(child.wait().unwrap().success()))
        } else {
            let thread = hold_with_library(&fx, &held, &release);
            Box::new(move || thread.join().unwrap())
        };
        // Released whatever happens, so nothing is left waiting.
        struct Release(PathBuf);
        impl Drop for Release {
            fn drop(&mut self) {
                let _ = std::fs::write(&self.0, "");
            }
        }
        let guard = Release(release.clone());
        wait_for("the standalone command to start", || held.exists());
        let child = belt(&fx, &cwd, &["exec", "--", "/bin/sh", "-c", "touch ran"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        wait_for("belt exec to queue behind it", || entries(&fx).len() == 2);
        let ledger = entries(&fx);
        assert_eq!(
            ledger.iter().filter(|e| e.state == State::Running).count(),
            1,
            "{ledger:?}"
        );
        assert!(!cwd.join("ran").exists(), "standalone={standalone}");
        drop(guard);
        let out = child.wait_with_output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{out:?}");
        assert_eq!(notices(&out), 1);
        assert!(cwd.join("ran").exists());
        holder();
        assert!(entries(&fx).is_empty());
    }
}

/// Ctrl-C reaches the command's process group (here: belt's own group, as a
/// terminal sends it); the command stops, its exit is 128 + SIGINT and the
/// ledger entry is gone.
#[test]
fn a50_ctrl_c_stops_the_command_and_removes_its_ledger_entry() {
    let fx = host(2);
    let cwd = fx.path("project");
    let started = cwd.join("started");
    // Both commands are shell builtins. Once the marker exists the final
    // process is already running, with no child spawn or exec left to race.
    let mut command = belt(
        &fx,
        &cwd,
        &[
            "exec",
            "--",
            "/bin/sh",
            "-c",
            ": > started; read -r line; exit 37",
        ],
    );
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().unwrap();
    let input = child.stdin.take().unwrap();
    let group = i32::try_from(child.id()).unwrap();
    assert!(group > 1);
    wait_for("the command to start", || started.exists());
    assert_eq!(entries(&fx).len(), 1);
    assert!(matches!(
        belt::platform::process::signal_process_group(group, libc::SIGINT),
        belt::platform::process::GroupSignal::Delivered
    ));
    wait_for("Ctrl-C to terminate belt while stdin stays open", || {
        child.try_wait().unwrap().is_some()
    });
    drop(input);
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(128 + libc::SIGINT), "{out:?}");
    assert!(entries(&fx).is_empty());
}

/// A ledger the embedded core cannot read (a newer standalone CLI wrote it)
/// is refused and left byte for byte; `doctor` says why.
#[test]
fn a50_an_unreadable_ledger_is_refused_unchanged_and_doctor_says_why() {
    let fx = host(2);
    let cwd = fx.path("project");
    let ledger = ledger_file(&fx);
    std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    let newer = br#"{"version":9,"next_seq":4,"entries_v9":[]}"#;
    std::fs::write(&ledger, newer).unwrap();
    let out = belt(&fx, &cwd, &["--json", "exec", "--", "touch", "ran"])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(belt::error::ErrorCode::Blocked.exit_code()),
        "{out:?}"
    );
    assert!(last_json(&out)["error"]["message"]
        .as_str()
        .unwrap()
        .contains("belt doctor"));
    assert!(!cwd.join("ran").exists());
    assert_eq!(std::fs::read(&ledger).unwrap(), newer);
    let report = json(&fx.run(&["doctor"]));
    let check = doctor_check(&report, "direct_exec_ledger");
    assert_eq!(check["level"], "fail", "{report}");
    assert!(check["detail"].as_str().unwrap().contains("cannot read"));
    assert_eq!(std::fs::read(&ledger).unwrap(), newer);
}

fn doctor_check<'a>(report: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    report["result"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["host"] == "parent" && c["id"] == id)
        .unwrap_or_else(|| panic!("no {id} in {report}"))
}

/// One budget only when the shell's scope is the resident's: `doctor` shows
/// the difference otherwise.
#[test]
fn a50_doctor_compares_the_direct_scope_with_the_resident_s() {
    let fx = Fx::new();
    fx.init();
    let report = json(&fx.run(&["doctor"]));
    let check = doctor_check(&report, "direct_exec_scope");
    assert_eq!(check["level"], "ok", "{report}");
    assert_eq!(doctor_check(&report, "direct_exec_ledger")["level"], "ok");

    let fx = Fx::new();
    let service_home = fx.path("service-home");
    std::fs::create_dir(&service_home).unwrap();
    fx.init_with_env(BTreeMap::from([
        ("HOME".to_string(), service_home.display().to_string()),
        ("PATH".to_string(), "/usr/bin:/bin".to_string()),
    ]));
    let report = json(&fx.run(&["doctor"]));
    let check = doctor_check(&report, "direct_exec_scope");
    assert_eq!(check["level"], "notice", "{report}");
    let detail = check["detail"].as_str().unwrap();
    assert!(detail.contains("two budgets"), "{detail}");
    assert!(detail.contains(service_home.to_str().unwrap()), "{detail}");
}

#[test]
fn a50_a_worker_without_a_coordinator_prevents_direct_execution() {
    let fx = host(2);
    let state = fx.path("home/.belt");
    std::fs::create_dir_all(&state).unwrap();
    let _socket = std::os::unix::net::UnixListener::bind(state.join("agent.sock")).unwrap();
    let cwd = fx.path("project/sub");
    let out = belt(&fx, &cwd, &["--json", "exec", "--", "/bin/touch", "ran"])
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(!out.status.success());
    assert!(report["error"]["message"]
        .as_str()
        .unwrap()
        .contains("worker agent"));
    assert!(!cwd.join("ran").exists());
}

#[test]
fn a50_uninitialized_submit_explains_how_to_start_a_resident_before_project_preflight() {
    let fx = Fx::new();
    let home = fx.path("home");
    let out = belt(&fx, &home, &["--json", "submit", "--", "/bin/true"])
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(report["error"]["message"]
        .as_str()
        .unwrap()
        .contains("belt init"));
    assert_eq!(std::fs::read_dir(home).unwrap().count(), 0);
}

/// Every file and directory under `dir` with its kind and size, so a run can
/// be compared with what was there before it.
fn tree(dir: &Path) -> Vec<(PathBuf, bool, u64)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, bool, u64)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let meta = std::fs::symlink_metadata(entry.path()).unwrap();
            let relative = entry.path().strip_prefix(root).unwrap().to_path_buf();
            out.push((
                relative,
                meta.is_dir(),
                if meta.is_dir() { 0 } else { meta.len() },
            ));
            if meta.is_dir() {
                walk(root, &entry.path(), out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn state_dir(fx: &Fx) -> PathBuf {
    fx.path("coordinator")
}

fn run_json(fx: &Fx, cwd: &Path, command: &[&str]) -> Output {
    let mut args = vec!["--json", "exec", "--"];
    args.extend_from_slice(command);
    belt(fx, cwd, &args).output().unwrap()
}

/// Hold an exclusive `flock` on `name` in the coordinator's state directory,
/// as a resident does while it starts or runs. The file is created here.
fn hold(fx: &Fx, name: &str) -> std::fs::File {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(state_dir(fx).join(name))
        .unwrap();
    file.lock().unwrap();
    file
}

/// A resident that is starting (or still holds ownership) but does not
/// answer is not "no resident": the command is not run outside the queue,
/// and nothing is created.
#[test]
fn a_starting_or_unresponsive_resident_prevents_direct_execution() {
    for name in ["startup.lock", "coordinator.lock"] {
        let fx = host(2);
        let cwd = fx.path("project");
        let held = hold(&fx, name);
        let before = tree(&state_dir(&fx));
        let out = run_json(&fx, &cwd, &["/usr/bin/touch", "ran"]);
        assert_eq!(
            out.status.code(),
            Some(belt::error::ErrorCode::CommunicationUnknown.exit_code()),
            "{name}: {out:?}"
        );
        let error = &last_json(&out)["error"];
        assert_eq!(error["code"], "COMMUNICATION_UNKNOWN", "{name}");
        let message = error["message"].as_str().unwrap();
        assert!(
            message.contains("starting or not answering"),
            "{name}: {message}"
        );
        assert_eq!(notices(&out), 0, "{name}");
        assert!(!cwd.join("ran").exists(), "{name}");
        assert_eq!(tree(&state_dir(&fx)), before, "{name}");
        // Once the holder is gone nothing keeps the command from running.
        drop(held);
        let out = run_json(&fx, &cwd, &["/usr/bin/touch", "ran"]);
        assert_eq!(out.status.code(), Some(0), "{name}: {out:?}");
        assert!(cwd.join("ran").exists(), "{name}");
    }
}

/// A socket file nobody listens on (left by a resident that died) is not a
/// resident: the command runs here, and nothing is created or removed.
#[test]
fn a_stale_socket_file_means_no_resident_and_nothing_is_created() {
    let fx = host(2);
    let cwd = fx.path("project");
    let parent = state_dir(&fx).join("parent");
    std::fs::create_dir_all(&parent).unwrap();
    drop(std::os::unix::net::UnixListener::bind(parent.join("request.sock")).unwrap());
    assert!(parent.join("request.sock").exists());
    let before = tree(&state_dir(&fx));
    let before_worker = tree(&fx.path("local-worker"));
    let out = run_json(&fx, &cwd, &["/usr/bin/touch", "ran"]);
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert_eq!(notices(&out), 1, "{out:?}");
    assert!(cwd.join("ran").exists());
    assert_eq!(tree(&state_dir(&fx)), before);
    assert_eq!(tree(&fx.path("local-worker")), before_worker);
}

/// Direct execution creates nothing in the state directory, whatever the
/// command's outcome.
#[test]
fn direct_execution_creates_nothing_in_the_state_directory() {
    let fx = host(2);
    let cwd = fx.path("project");
    let before = tree(&state_dir(&fx));
    let before_worker = tree(&fx.path("local-worker"));
    for code in ["0", "5"] {
        let out = run_json(&fx, &cwd, &["/bin/sh", "-c", &format!("exit {code}")]);
        assert_eq!(out.status.code(), code.parse().ok(), "{out:?}");
        assert_eq!(tree(&state_dir(&fx)), before);
        assert_eq!(tree(&fx.path("local-worker")), before_worker);
    }
}

/// No `init` yet: the real binary still runs the command, and does not
/// create the coordinator's configuration or state.
#[test]
fn an_uninitialized_home_runs_the_command_here() {
    let fx = Fx::new();
    let home = fx.path("home");
    let out = belt(
        &fx,
        &home,
        &["exec", "--", "/bin/sh", "-c", "echo hello; exit 3"],
    )
    .output()
    .unwrap();
    assert_eq!(out.status.code(), Some(3), "{out:?}");
    assert_eq!(String::from_utf8_lossy(&out.stdout), "hello\n");
    assert_eq!(notices(&out), 1, "{out:?}");
    assert!(!home.join(".config/belt").exists());
    assert!(!home.join(".belt").exists());
}

#[test]
fn every_option_that_needs_a_run_is_refused_by_name() {
    let fx = host(2);
    let cwd = fx.path("project");
    let before = tree(&state_dir(&fx));
    for (args, flag) in [
        (&["--ref", "main"][..], "--ref"),
        (&["--snapshot", "commit"], "--snapshot"),
        (&["--in-place"], "--in-place"),
        (&["--profile", "p"], "--profile"),
        (&["--requires", "node"], "--requires"),
    ] {
        let mut all = vec!["--json", "exec"];
        all.extend_from_slice(args);
        all.extend(["--", "/usr/bin/touch", "ran"]);
        let out = belt(&fx, &cwd, &all).output().unwrap();
        assert_eq!(
            out.status.code(),
            Some(belt::error::ErrorCode::Blocked.exit_code()),
            "{args:?} {out:?}"
        );
        let message = last_json(&out)["error"]["message"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(message.starts_with(flag), "{args:?}: {message}");
        assert!(message.contains("belt init"), "{message}");
        assert!(!cwd.join("ran").exists(), "{args:?}");
    }
    assert_eq!(tree(&state_dir(&fx)), before);
}

/// The exit of a belt the test started, or a kill of that same child and a
/// failure when it does not end.
fn finish(child: &mut std::process::Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("belt did not end after the signal");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A command that records the signal it gets and exits with a code that
/// names it, once the marker says its handlers are installed.
fn signal_script() -> &'static str {
    "trap 'echo TERM > got; exit 41' TERM; trap 'echo INT > got; exit 42' INT; \
     trap 'echo HUP > got; exit 43' HUP; : > started; while :; do sleep 0.05; done"
}

/// A signal sent to belt alone (not to a process group) is forwarded to the
/// command, which ends the way it chooses; belt exits with that code and the
/// ledger entry is gone.
#[test]
fn sigint_sigterm_and_sighup_sent_to_belt_reach_the_command() {
    for (signal, name, code) in [
        (libc::SIGTERM, "TERM", 41),
        (libc::SIGINT, "INT", 42),
        (libc::SIGHUP, "HUP", 43),
    ] {
        let fx = host(2);
        let cwd = fx.path("project");
        let mut child = belt(&fx, &cwd, &["exec", "--", "/bin/sh", "-c", signal_script()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_for("the command to start", || cwd.join("started").exists());
        // Only the process this test started, never a group.
        assert!(child.id() > 1);
        assert!(belt::platform::process::signal_pid(child.id(), signal));
        let status = finish(&mut child);
        assert_eq!(status.code(), Some(code), "{name}: {status:?}");
        assert_eq!(
            std::fs::read_to_string(cwd.join("got")).unwrap().trim(),
            name
        );
        assert!(entries(&fx).is_empty(), "{name}");
    }
}

/// A stop signal while belt waits for admission ends it cleanly: the command
/// never starts, the entry is removed, and the exit code is 128 + N.
#[test]
fn a_signal_while_waiting_for_admission_ends_belt_without_starting_the_command() {
    for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        let fx = host(1);
        let held = fx.path("held");
        let release = fx.path("release");
        let cwd = fx.path("project");
        struct Release(PathBuf);
        impl Drop for Release {
            fn drop(&mut self) {
                let _ = std::fs::write(&self.0, "");
            }
        }
        let _guard = Release(release.clone());
        let holder = hold_with_library(&fx, &held, &release);
        wait_for("the holder to start", || held.exists());
        let mut child = belt(&fx, &cwd, &["exec", "--", "/usr/bin/touch", "ran"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        wait_for("belt exec to queue behind it", || entries(&fx).len() == 2);
        assert!(child.id() > 1);
        assert!(belt::platform::process::signal_pid(child.id(), signal));
        let status = finish(&mut child);
        assert_eq!(status.code(), Some(128 + signal), "{status:?}");
        assert!(!cwd.join("ran").exists());
        wait_for("belt's entry to go", || entries(&fx).len() == 1);
        std::fs::write(&release, "").unwrap();
        holder.join().unwrap();
        assert!(entries(&fx).is_empty());
    }
}
