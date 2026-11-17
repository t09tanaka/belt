//! End-to-end runs against `examples/demo`, copied into a fresh git repository.

#![allow(clippy::disallowed_methods)] // the tests build the binary's environment

mod common;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dest);
        } else {
            std::fs::copy(e.path(), &dest).unwrap();
        }
    }
}

fn git(dir: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .args([
            "-c",
            "user.email=demo@example.com",
            "-c",
            "user.name=demo",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap()
        .status
        .success();
    assert!(ok, "git {args:?}");
}

struct Demo {
    _tmp: common::Tmp,
    repo: PathBuf,
    home: PathBuf,
}

/// The demo on `main`, plus a `feature` branch that changes `app/`.
fn demo(change: &str) -> Demo {
    let tmp = common::tempdir().unwrap();
    let repo = tmp.path().join("demo");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/demo"),
        &repo,
    );
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    git(&repo, &["checkout", "-qb", "feature"]);
    let f = repo.join(change);
    let mut text = std::fs::read_to_string(&f).unwrap();
    text.push_str("# changed\n");
    std::fs::write(&f, text).unwrap();
    git(&repo, &["commit", "-qam", "change"]);
    let home = tmp.path().join("home");
    Demo {
        _tmp: tmp,
        repo,
        home,
    }
}

fn local_ci(d: &Demo, args: &[&str]) -> Output {
    local_ci_path(d, &std::env::var("PATH").unwrap(), args)
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

// The unreaped leader pins the identity of the process group until cleanup.
struct ProbeChild(Child);
impl Drop for ProbeChild {
    fn drop(&mut self) {
        let pid = self.0.id() as i32;
        if pid > 1 {
            // SAFETY: this Child was spawned into its own group and is unreaped.
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
        let _ = self.0.wait();
    }
}

fn probe_status(mut command: Command, timeout: Duration) -> (bool, Option<u32>) {
    let Ok(child) = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
    else {
        return (false, None);
    };
    let child = ProbeChild(child);
    let pid = child.0.id();
    let start = Instant::now();
    let success = loop {
        let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: inspect only our owned child without reaping its group leader.
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                child.0.id(),
                &mut status,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc != 0 {
            break false;
        }
        if unsafe { status.si_pid() } != 0 {
            break status.si_code == libc::CLD_EXITED && unsafe { status.si_status() } == 0;
        }
        if start.elapsed() >= timeout {
            break false;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    drop(child);
    (success, Some(pid))
}

fn probe_success(command: Command, timeout: Duration) -> bool {
    probe_status(command, timeout).0
}

fn docker_available() -> bool {
    let mut command = Command::new("docker");
    command.arg("info");
    probe_success(command, Duration::from_secs(5))
}

#[test]
fn availability_probe_is_bounded_and_reaps_its_owned_child() {
    let mut command = Command::new("/bin/sh");
    command.args(["-c", "exec sleep 20"]);
    let (success, pid) = probe_status(command, Duration::from_secs(2));
    assert!(!success);
    // Use the parent's spawn receipt: the child need not get CPU time before
    // its deadline, and need not write a file to prove which process was reaped.
    let pid = pid.expect("the owned child was spawned");
    assert!(pid > 1 && pid <= i32::MAX as u32);
    let pid = pid as i32;
    // The helper already reaped this owned child.
    assert_eq!(
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
    for (script, expected) in [("exit 0", true), ("exit 2", false)] {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]);
        assert_eq!(probe_success(command, Duration::from_secs(2)), expected);
    }
}

#[test]
fn plan_lists_units_and_setup() {
    let d = demo("app/lib.sh");
    let o = local_ci(&d, &["plan"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert!(
        out.contains("(1 file changed, triggered by the changes)"),
        "{out}"
    );
    assert!(out.contains("setup (once): app$ ./install.sh"), "{out}");
    assert!(
        out.contains("unit 1  ci.yml: lint → discover → test → deploy"),
        "{out}"
    );
    assert!(
        out.contains("integration (services: cache)  [exclusive]"),
        "{out}"
    );
}

#[test]
fn nothing_runs_when_paths_do_not_match() {
    let d = demo("README.md");
    let o = local_ci(&d, &["run", "--no-status", "--no-traffic-controller"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert!(
        out.contains("not triggered (paths not matched: ci.yml)"),
        "{out}"
    );
    // nothing to run is not a success of any check, but exits 0 as it always did
    assert!(out.contains("result: no_work · pass 0"), "{out}");
    assert!(out.contains("NO WORK"), "{out}");
    // --all runs it anyway
    let o = local_ci(&d, &["plan", "--all"]);
    let out = stdout(&o);
    assert!(out.contains("all checks)"), "{out}");
    assert!(out.contains("unit 1  ci.yml: lint"), "{out}");
}

#[test]
fn runs_jobs_with_dynamic_matrix() {
    let d = demo("app/lib.sh");
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "-j",
            "test",
            "-j",
            "deploy",
        ],
    );
    let out = stdout(&o);
    // the report-upload step is run for real (one artifact per shard), so
    // nothing is left out of the run
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("result: success"), "{out}");
    for line in [
        "PASS   ci.yml › lint",
        "PASS   ci.yml › discover",
        "PASS   ci.yml › test (1)",
        "PASS   ci.yml › test (2)",
        "SKIP   deploy (if: false)",
    ] {
        assert!(out.contains(line), "missing `{line}` in\n{out}");
    }
    // the run left nothing behind in the repository
    let wt = Command::new("git")
        .args(["worktree", "list"])
        .current_dir(&d.repo)
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&wt.stdout).lines().count(), 1);
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(&d.repo)
        .output()
        .unwrap();
    assert!(status.stdout.is_empty());
}

#[test]
fn reports_the_failing_step_with_its_log() {
    let d = demo("app/lib.sh");
    std::fs::write(d.repo.join("app/lib.sh"), "add() { echo 0; }\n").unwrap();
    git(&d.repo, &["commit", "-qam", "break add"]);
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "-j",
            "test",
        ],
    );
    let out = stdout(&o);
    assert_eq!(o.status.code(), Some(1), "{out}");
    assert!(
        out.contains("FAIL   ci.yml › test (1) › Unit tests (shard 1) (exit 1)"),
        "{out}"
    );
    assert!(
        out.contains("── ci.yml › test (1) › Unit tests (shard 1) (exit 1)"),
        "{out}"
    );
    assert!(out.contains("log: "), "{out}");
    assert!(out.contains("result: failure"), "{out}");
    // fail-fast: false keeps the other shard running
    assert!(out.contains("ci.yml › test (2)"), "{out}");
}

#[test]
fn parallel_units_do_not_see_each_others_writes() {
    let d = demo("app/lib.sh");
    // `tamper` edits a tracked file for a while and puts it back; `observe`
    // reads that file meanwhile. On GitHub each job has its own checkout.
    std::fs::write(
        d.repo.join(".github/workflows/isolation.yml"),
        r#"name: Isolation
on: pull_request
jobs:
  tamper:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo 'echo tampered' >> app/lib.sh
          sleep 4
          git checkout -- app/lib.sh
  observe:
    runs-on: ubuntu-latest
    steps:
      - run: |
          sleep 2
          git diff --exit-code -- app/lib.sh
          ! grep -q tampered app/lib.sh
"#,
    )
    .unwrap();
    git(&d.repo, &["add", "."]);
    git(&d.repo, &["commit", "-qm", "isolation"]);
    let o = local_ci(&d, &["plan", "-j", "tamper", "-j", "observe"]);
    let out = stdout(&o);
    assert!(out.contains("unit 1  isolation.yml: tamper"), "{out}");
    assert!(out.contains("unit 2  isolation.yml: observe"), "{out}");

    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "--parallel",
            "2",
            "-j",
            "tamper",
            "-j",
            "observe",
        ],
    );
    let out = stdout(&o);
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("PASS   isolation.yml › observe"), "{out}");
    let wt = Command::new("git")
        .args(["worktree", "list"])
        .current_dir(&d.repo)
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&wt.stdout).lines().count(), 1);
}

#[test]
fn json_result_is_saved() {
    let d = demo("app/lib.sh");
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "--json",
            "-j",
            "lint",
        ],
    );
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["state"], "success");
    assert_eq!(v["units"][0]["jobs"][0]["result"], "pass");
    let saved = Path::new(v["run_dir"].as_str().unwrap()).join("result.json");
    assert!(saved.is_file());
}

fn add_workflow(d: &Demo, file: &str, yaml: &str) {
    std::fs::write(d.repo.join(".github/workflows").join(file), yaml).unwrap();
    git(&d.repo, &["add", "."]);
    git(&d.repo, &["commit", "-qm", file]);
}

/// The `time` of each job of a `local-ci run --json`, by job id.
fn job_times(o: &Output) -> serde_json::Value {
    let v: serde_json::Value = serde_json::from_slice(&o.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&o.stdout)));
    let mut times = serde_json::Map::new();
    for u in v["units"].as_array().unwrap() {
        for j in u["jobs"].as_array().unwrap() {
            times.insert(j["job"].as_str().unwrap().into(), j["time"].clone());
        }
    }
    times.into()
}

const SLOW_E2E: &str = r#"name: Slow
on: pull_request
jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - run: echo ran
"#;

#[test]
fn job_time_counts_the_running_and_no_waits_on_an_idle_machine() {
    let d = demo("app/lib.sh");
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-cache",
            "--no-traffic-controller",
            "--json",
            "-j",
            "lint",
        ],
    );
    let t = &job_times(&o)["lint"];
    assert!(t["work_secs"].as_f64().unwrap() > 0.0, "{t}");
    for k in [
        "wait_lock_secs",
        "wait_slot_secs",
        "wait_queue_secs",
        "wait_parallel_secs",
    ] {
        assert!(t[k].as_f64().unwrap() < 1.0, "{k}: {t}");
    }
    // the fields that were there keep their place
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v["units"][0]["jobs"][0]["secs"].is_number());
    assert!(v["units"][0]["waited_secs"].is_number());
}

#[test]
fn traffic_controller_queue_is_counted_apart_from_the_running() {
    let d = demo("app/lib.sh");
    add_workflow(
        &d,
        "queue.yml",
        r#"name: Queue
on: pull_request
jobs:
  waiter:
    runs-on: ubuntu-latest
    steps:
      - run: echo one
      - run: echo two
"#,
    );
    // a task-traffic-controller that lets every command start 2 seconds late
    let bin = d.home.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake = bin.join("task-traffic-controller");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\necho \"$*\" >> {}/argv\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nsleep 2\nexec \"$@\"\n",
            d.home.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let o = local_ci_path(
        &d,
        &path,
        &[
            "run",
            "--no-status",
            "--no-cache",
            "--json",
            "-w",
            "queue.yml",
        ],
    );
    let t = &job_times(&o)["waiter"];
    let queue = t["wait_queue_secs"].as_f64().unwrap();
    assert!((3.5..15.0).contains(&queue), "two steps, 2s each: {t}");
    assert!(t["work_secs"].as_f64().unwrap() < queue, "{t}");
    // task-traffic-controller is given the step's own command, as before: its history shows it
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert!(
        argv.lines()
            .all(|l| l.contains("-- bash --noprofile --norc -eo pipefail ")),
        "{argv}"
    );
    // the summary names the wait in the job's own line
    let o = local_ci_path(
        &d,
        &path,
        &["run", "--no-status", "--no-cache", "-w", "queue.yml"],
    );
    let out = stdout(&o);
    assert!(
        out.contains("PASS   queue.yml › waiter") && out.contains("waited queue "),
        "{out}"
    );
}

/// The steps of the one job of a `local-ci run --json`.
fn job_steps(o: &Output) -> Vec<serde_json::Value> {
    let v: serde_json::Value = serde_json::from_slice(&o.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&o.stdout)));
    v["units"][0]["jobs"][0]["steps"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn waiting_for_the_traffic_controller_does_not_use_up_the_jobs_timeout() {
    let d = demo("app/lib.sh");
    // 6 seconds for the job; its steps run for about one
    add_workflow(
        &d,
        "queue.yml",
        r#"name: Queue
on: pull_request
jobs:
  waiter:
    runs-on: ubuntu-latest
    timeout-minutes: 0.1
    steps:
      - run: echo one
      - run: sleep 1
"#,
    );
    // a task-traffic-controller that lets every command start 7 seconds late:
    // the first step alone waits longer than the job may run
    let bin = d.home.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake = bin.join("task-traffic-controller");
    std::fs::write(
        &fake,
        "#!/bin/sh\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nsleep 7\nexec \"$@\"\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let o = local_ci_path(
        &d,
        &path,
        &[
            "run",
            "--no-status",
            "--no-cache",
            "--json",
            "-w",
            "queue.yml",
        ],
    );
    let steps = job_steps(&o);
    assert_eq!(steps.len(), 2, "{steps:?}");
    for s in &steps {
        assert_eq!(s["result"], "pass", "{steps:?}");
    }
    let t = &job_times(&o)["waiter"];
    assert!(t["wait_queue_secs"].as_f64().unwrap() > 12.0, "{t}");
    assert!(t["work_secs"].as_f64().unwrap() < 6.0, "{t}");
}

#[test]
fn the_running_of_the_steps_still_uses_up_the_jobs_timeout() {
    let d = demo("app/lib.sh");
    // 6 seconds for the job: the second step is stopped when they are over,
    // and the third, which would run after a failure, is not started
    add_workflow(
        &d,
        "over.yml",
        r#"name: Over
on: pull_request
jobs:
  slow:
    runs-on: ubuntu-latest
    timeout-minutes: 0.1
    steps:
      - run: sleep 2
      - run: sleep 30
      - if: always()
        run: echo cleanup
"#,
    );
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-cache",
            "--no-traffic-controller",
            "--json",
            "-w",
            "over.yml",
        ],
    );
    let steps = job_steps(&o);
    assert_eq!(steps.len(), 3, "{steps:?}");
    assert_eq!(steps[0]["result"], "pass", "{steps:?}");
    let second = steps[1]["reason"].to_string();
    assert!(
        second.contains("timed out after ")
            && second.contains("s (what was left of the job's 0.1 min)"),
        "{steps:?}"
    );
    assert!(steps[1]["secs"].as_f64().unwrap() < 20.0, "{steps:?}");
    let third = steps[2]["reason"].to_string();
    assert!(
        third.contains("not started: the job timed out (0.1 min)"),
        "{steps:?}"
    );
}

#[test]
fn container_slot_and_repository_lock_waits_are_counted() {
    use fs2::FileExt;
    let d = demo("app/lib.sh");
    add_workflow(&d, "slow.yml", SLOW_E2E);
    // a first run creates the repository's lock file
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-cache",
            "--no-traffic-controller",
            "--json",
            "-w",
            "slow.yml",
        ],
    );
    assert_eq!(o.status.code(), Some(0));
    let locks = d.home.join("locks");
    let repo_lock = std::fs::read_dir(&locks)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            !p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("containers-")
        })
        .expect("the run took the repository lock");
    let open = |p: &Path| {
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(p)
            .unwrap()
    };
    let repo_hold = open(&repo_lock);
    let slot_hold = open(&locks.join("containers-0.lock"));
    repo_hold.lock_exclusive().unwrap();
    slot_hold.lock_exclusive().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_local-ci"))
        .args([
            "run",
            "--no-status",
            "--no-cache",
            "--no-traffic-controller",
            "--json",
            "--containers",
            "1",
            "-w",
            "slow.yml",
        ])
        .current_dir(&d.repo)
        .env("LOCAL_CI_HOME", &d.home)
        .env(
            "PATH",
            common::private_docker_path(&d.home, &std::env::var("PATH").unwrap()),
        )
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // The run needs a moment to get as far as the lock, longer on a busy machine.
    std::thread::sleep(std::time::Duration::from_secs(4));
    // the lock comes first, then the slot
    repo_hold.unlock().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));
    slot_hold.unlock().unwrap();
    let o = child.wait_with_output().unwrap();
    let t = &job_times(&o)["e2e"];
    assert!(t["wait_lock_secs"].as_f64().unwrap() >= 1.0, "{t}");
    assert!(t["wait_slot_secs"].as_f64().unwrap() >= 2.0, "{t}");
    assert!(t["work_secs"].as_f64().unwrap() < 6.0, "{t}");
}

#[test]
fn a_unit_waiting_for_a_parallel_place_is_counted() {
    let d = demo("app/lib.sh");
    add_workflow(
        &d,
        "two.yml",
        r#"name: Two
on: pull_request
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 2; echo first
  second:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 2; echo second
"#,
    );
    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-cache",
            "--no-traffic-controller",
            "--json",
            "--parallel",
            "1",
            "-w",
            "two.yml",
        ],
    );
    let t = job_times(&o);
    let waits: Vec<f64> = ["first", "second"]
        .iter()
        .map(|j| t[*j]["wait_parallel_secs"].as_f64().unwrap())
        .collect();
    // one of them went first
    assert!(
        waits.iter().cloned().fold(0.0, f64::max) >= 1.5,
        "{waits:?}"
    );
    assert!(
        waits.iter().cloned().fold(f64::MAX, f64::min) < 1.0,
        "{waits:?}"
    );
}

#[test]
fn services_get_their_own_port() {
    if !docker_available() {
        eprintln!(
            "real Docker acceptance unavailable (missing, failed, or timed out); not verified"
        );
        return;
    }
    let d = demo("app/lib.sh");
    let o = local_ci_real_path(
        &d,
        &std::env::var("PATH").unwrap(),
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "-j",
            "integration",
        ],
    );
    let out = stdout(&o);
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("PASS   ci.yml › integration"), "{out}");
}

/// `gh` that reports ci.yml as disabled on GitHub.
fn fake_gh_disabled(d: &Demo) -> String {
    let bin = d.home.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let gh = bin.join("gh");
    std::fs::write(
        &gh,
        "#!/bin/sh\nprintf '.github/workflows/ci.yml\\tdisabled_manually\\n'\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    git(
        &d.repo,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/demo/demo.git",
        ],
    );
    format!("{}:{}", bin.display(), std::env::var("PATH").unwrap())
}

fn local_ci_path(d: &Demo, path: &str, args: &[&str]) -> Output {
    local_ci_real_path(d, &common::private_docker_path(&d.home, path), args)
}

fn local_ci_real_path(d: &Demo, path: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_local-ci"))
        .args(args)
        .current_dir(&d.repo)
        .env("LOCAL_CI_HOME", &d.home)
        .env("PATH", path)
        .output()
        .unwrap()
}

#[test]
fn include_disabled_runs_disabled_workflows_with_paths() {
    let d = demo("app/lib.sh");
    let path = fake_gh_disabled(&d);
    let base = ["plan", "--base", "main"];
    let o = local_ci_path(&d, &path, &base);
    let out = stdout(&o);
    assert!(
        out.contains("not triggered (disabled manually on GitHub: ci.yml)"),
        "{out}"
    );
    assert!(!out.contains("unit 1"), "{out}");
    let o = local_ci_path(&d, &path, &[&base[..], &["--include-disabled"]].concat());
    let out = stdout(&o);
    assert!(out.contains("triggered by the changes)"), "{out}");
    assert!(out.contains("unit 1  ci.yml: lint"), "{out}");

    // paths still apply
    let d = demo("README.md");
    let path = fake_gh_disabled(&d);
    let o = local_ci_path(&d, &path, &[&base[..], &["--include-disabled"]].concat());
    let out = stdout(&o);
    assert!(
        out.contains("not triggered (paths not matched: ci.yml)"),
        "{out}"
    );
    assert!(!out.contains("unit 1"), "{out}");
}

#[test]
fn workflows_dir_adds_workflows_github_does_not_run() {
    let d = demo("app/lib.sh");
    // Same file name as .github/workflows/ci.yml: the output names it by path.
    std::fs::create_dir_all(d.repo.join("ci/local")).unwrap();
    std::fs::write(
        d.repo.join("ci/local/ci.yml"),
        r#"on: pull_request
jobs:
  extra:
    runs-on: ubuntu-latest
    steps:
      - run: echo extra
"#,
    )
    .unwrap();
    git(&d.repo, &["add", "."]);
    git(&d.repo, &["commit", "-qm", "local-only workflow"]);

    let out = stdout(&local_ci(&d, &["plan"]));
    assert!(!out.contains("ci/local"), "{out}");

    let o = local_ci(&d, &["plan", "--workflows-dir", "./ci/local/"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert!(out.contains("  ci.yml: lint"), "{out}");
    assert!(out.contains("  ci/local/ci.yml: extra"), "{out}");

    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "--workflows-dir",
            "ci/local",
            "-w",
            "ci/local/ci.yml",
        ],
    );
    let out = stdout(&o);
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("PASS   ci/local/ci.yml › extra"), "{out}");
}

#[test]
fn workflows_dir_must_be_committed() {
    let d = demo("app/lib.sh");
    std::fs::create_dir_all(d.repo.join("ci/local")).unwrap();
    std::fs::write(d.repo.join("ci/local/ci.yml"), "on: pull_request\n").unwrap();
    let o = local_ci(&d, &["plan", "--workflows-dir", "ci/local"]);
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(err.contains("commit them first"), "{err}");
}

/// The demo with its workflow moved from `.github/workflows` to `.ci/workflows`
/// and a `.local-ci.toml` pointing there, committed on `feature`.
fn moved_to_ci_workflows(config: &str) -> Demo {
    let d = demo("app/lib.sh");
    std::fs::create_dir_all(d.repo.join(".ci")).unwrap();
    git(&d.repo, &["mv", ".github/workflows", ".ci/workflows"]);
    std::fs::write(d.repo.join(".local-ci.toml"), config).unwrap();
    git(&d.repo, &["add", ".local-ci.toml"]);
    git(&d.repo, &["commit", "-qm", "move the workflows"]);
    d
}

#[test]
fn config_replaces_the_workflows_dir() {
    let d = moved_to_ci_workflows("workflows-dirs = [\".ci/workflows\"]\n");
    assert!(!d.repo.join(".github/workflows").exists());
    let o = local_ci(&d, &["plan"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    // named without the configured directory, as `.github/workflows/` was
    assert!(
        out.contains("unit 1  ci.yml: lint → discover → test → deploy"),
        "{out}"
    );
    assert!(!out.contains(".ci/workflows"), "{out}");

    let o = local_ci(
        &d,
        &[
            "run",
            "--no-status",
            "--no-traffic-controller",
            "-j",
            "lint",
        ],
    );
    let out = stdout(&o);
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("PASS   ci.yml › lint"), "{out}");
}

#[test]
fn config_replaces_github_workflows_instead_of_adding_to_it() {
    let d = demo("app/lib.sh");
    std::fs::create_dir_all(d.repo.join(".ci/workflows")).unwrap();
    std::fs::write(
        d.repo.join(".ci/workflows/other.yml"),
        "on: pull_request\njobs:\n  other:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo other\n",
    )
    .unwrap();
    std::fs::write(
        d.repo.join(".local-ci.toml"),
        "workflows-dirs = [\".ci/workflows\"]\n",
    )
    .unwrap();
    git(&d.repo, &["add", "."]);
    git(&d.repo, &["commit", "-qm", "configure"]);
    let o = local_ci(&d, &["plan"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert!(out.contains("unit 1  other.yml: other"), "{out}");
    assert!(!out.contains("ci.yml"), "{out}");
}

#[test]
fn config_is_read_from_the_commit() {
    // Written but not committed: the commit has no `.local-ci.toml`, so the
    // workflows still come from `.github/workflows`.
    let d = demo("app/lib.sh");
    std::fs::write(
        d.repo.join(".local-ci.toml"),
        "workflows-dirs = [\".ci/workflows\"]\n",
    )
    .unwrap();
    let o = local_ci(&d, &["plan"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert!(out.contains("unit 1  ci.yml: lint"), "{out}");
}

#[test]
fn config_and_workflows_dir_add_up() {
    let d = moved_to_ci_workflows("workflows-dirs = [\".ci/workflows\"]\n");
    std::fs::create_dir_all(d.repo.join("ci/local")).unwrap();
    std::fs::write(
        d.repo.join("ci/local/ci.yml"),
        "on: pull_request\njobs:\n  extra:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo extra\n",
    )
    .unwrap();
    git(&d.repo, &["add", "."]);
    git(&d.repo, &["commit", "-qm", "local-only workflow"]);
    let o = local_ci(&d, &["plan", "--workflows-dir", "ci/local"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert!(out.contains("  ci.yml: lint"), "{out}");
    assert!(out.contains("  ci/local/ci.yml: extra"), "{out}");
    // naming a configured directory again reads it once
    let o = local_ci(&d, &["plan", "--workflows-dir", ".ci/workflows/"]);
    let out = stdout(&o);
    assert!(o.status.success(), "{out}");
    assert_eq!(out.matches("ci.yml: lint").count(), 1, "{out}");
}

#[test]
fn bad_config_is_an_error() {
    for (config, msg) in [
        (
            "workflows-dirs = [\"/etc/workflows\"]\n",
            "relative to the repository root",
        ),
        ("workflows-dir = [\".ci/workflows\"]\n", "unknown field"),
        (
            "workflows-dirs = [\".ci/workflows\", \"ci/none\"]\n",
            ".local-ci.toml: workflows-dirs ci/none: no .yml / .yaml files there",
        ),
    ] {
        let d = moved_to_ci_workflows(config);
        let o = local_ci(&d, &["plan"]);
        assert_eq!(o.status.code(), Some(64), "{config}");
        let err = String::from_utf8_lossy(&o.stderr);
        assert!(err.contains(".local-ci.toml"), "{config}: {err}");
        assert!(err.contains(msg), "{config}: {err}");
    }
}

/// A stand-in controller named `name` that records its argv in `<home>/<name>.argv`.
fn fake_controller(d: &Demo, bin: &Path, name: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(bin).unwrap();
    let fake = bin.join(name);
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\necho \"$*\" >> {}/{name}.argv\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n",
            d.home.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn the_controller_is_found_by_its_new_name_only() {
    for (name, used) in [("task-traffic-controller", true), ("canirun", false)] {
        let d = demo("app/lib.sh");
        let bin = d.home.join("bin");
        fake_controller(&d, &bin, name);
        let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
        let o = local_ci_path(
            &d,
            &path,
            &["run", "--no-status", "--no-cache", "-j", "lint"],
        );
        assert!(o.status.success(), "{name}: {}", stdout(&o));
        assert_eq!(d.home.join(format!("{name}.argv")).exists(), used, "{name}");
    }
}

#[test]
fn only_the_new_no_controller_flag_exists() {
    let d = demo("app/lib.sh");
    let bin = d.home.join("bin");
    fake_controller(&d, &bin, "task-traffic-controller");
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let args = |f| ["run", "--no-status", "--no-cache", f, "-j", "lint"];
    let o = local_ci_path(&d, &path, &args("--no-traffic-controller"));
    assert!(o.status.success(), "{}", stdout(&o));
    assert!(!d.home.join("task-traffic-controller.argv").exists());
    let o = local_ci_path(&d, &path, &args("--no-canirun"));
    assert!(!o.status.success());
    let help = Command::new(env!("CARGO_BIN_EXE_local-ci"))
        .args(["run", "--help"])
        .output()
        .unwrap();
    let help = stdout(&help);
    assert!(
        help.contains("--no-traffic-controller") && !help.contains("canirun"),
        "{help}"
    );
}

/// Two jobs that say what threads they were given, and a controller with a
/// budget of 8 that records how each step was handed to it.
fn thread_share_demo() -> (Demo, String) {
    let d = demo("app/lib.sh");
    let out = d.home.join("threads");
    std::fs::create_dir_all(&out).unwrap();
    let say = |job: &str| {
        format!(
            "echo \"$CARGO_BUILD_JOBS $NEXTEST_TEST_THREADS $RUST_TEST_THREADS\" > {}/{job}",
            out.display()
        )
    };
    add_workflow(
        &d,
        "threads.yml",
        &format!(
            r#"name: Threads
on: pull_request
jobs:
  plain:
    runs-on: ubuntu-latest
    steps:
      - run: {}
  own:
    runs-on: ubuntu-latest
    env:
      CARGO_BUILD_JOBS: 1
    steps:
      - run: {}
        env:
          NEXTEST_TEST_THREADS: 7
"#,
            say("plain"),
            say("own")
        ),
    );
    let path = budget_controller(&d);
    (d, path)
}

/// A `PATH` with a controller whose budget is 8 and which starts every
/// command at once, after writing how it was called to `argv`.
fn budget_controller(d: &Demo) -> String {
    let bin = d.home.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake = bin.join("task-traffic-controller");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nif [ \"$1\" = inspect ]; then echo '{{\"limits\":{{\"budget\":8}}}}'; exit 0; fi\necho \"$*\" >> {}/argv\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n",
            d.home.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    format!("{}:{}", bin.display(), std::env::var("PATH").unwrap())
}

/// local-ci with `env` added and the thread variables of the caller removed.
fn local_ci_threads(d: &Demo, path: &str, env: &[(&str, &str)], args: &[&str]) -> Output {
    let args = [&["-w", "threads.yml"], args].concat();
    local_ci_share(d, path, env, &args)
}

fn local_ci_share(d: &Demo, path: &str, env: &[(&str, &str)], args: &[&str]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_local-ci"));
    c.args(["run", "--no-status", "--no-cache"])
        .args(args)
        .current_dir(&d.repo)
        .env("LOCAL_CI_HOME", &d.home)
        .env("PATH", common::private_docker_path(&d.home, path));
    for k in local_ci::cpu::JOB_THREAD_ENV {
        c.env_remove(k);
    }
    c.envs(env.iter().copied()).output().unwrap()
}

fn threads_of(d: &Demo, job: &str) -> String {
    std::fs::read_to_string(d.home.join("threads").join(job))
        .unwrap()
        .trim()
        .to_string()
}

#[test]
fn jobs_are_given_a_share_of_the_controller_budget() {
    let (d, path) = thread_share_demo();
    let o = local_ci_threads(&d, &path, &[], &["--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    // two jobs on a budget of 8: four threads each
    assert_eq!(threads_of(&d, "plain"), "4 4 4");
    // what the job and the step set stays
    assert_eq!(threads_of(&d, "own"), "1 7 4");
    // each step asks the controller for the job's share
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert_eq!(argv.lines().count(), 2, "{argv}");
    assert!(
        argv.lines()
            .all(|l| l.starts_with("exec --weight 4 --key ")),
        "{argv}"
    );
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(
        v["cpu"],
        serde_json::json!({"budget": 8, "jobs_at_once": 2, "threads_per_job": 4})
    );
    // and the summary says so
    let o = local_ci_threads(&d, &path, &[], &[]);
    let out = stdout(&o);
    assert!(
        out.contains("cpu    budget 8 · 2 jobs at once · 4 threads each"),
        "{out}"
    );
}

#[test]
fn the_callers_thread_variables_are_kept() {
    let (d, path) = thread_share_demo();
    let o = local_ci_threads(&d, &path, &[("RUST_TEST_THREADS", "3")], &[]);
    assert!(o.status.success(), "{}", stdout(&o));
    assert_eq!(threads_of(&d, "plain"), "4 4 3");
    assert_eq!(threads_of(&d, "own"), "1 7 3");
}

#[test]
fn parallel_decides_the_share_and_one_job_at_a_time_has_no_limit() {
    let (d, path) = thread_share_demo();
    // eight places for two jobs: still four threads each
    let o = local_ci_threads(&d, &path, &[], &["--parallel", "8"]);
    assert!(o.status.success(), "{}", stdout(&o));
    assert_eq!(threads_of(&d, "plain"), "4 4 4");
    std::fs::remove_file(d.home.join("argv")).unwrap();
    // one at a time: nothing is set, and the controller works the weight out
    let o = local_ci_threads(&d, &path, &[], &["--parallel", "1", "--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    assert_eq!(threads_of(&d, "plain"), "");
    assert_eq!(threads_of(&d, "own"), "1 7");
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert!(argv.lines().all(|l| l.starts_with("exec --key ")), "{argv}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v.get("cpu").is_none(), "{v}");
}

#[test]
fn the_entries_of_a_matrix_count_as_jobs_that_run_at_once() {
    let d = demo("app/lib.sh");
    let out = d.home.join("threads");
    std::fs::create_dir_all(&out).unwrap();
    let matrix = |max_parallel: &str| {
        format!(
            r#"name: Matrix
on: pull_request
jobs:
  entry:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        n: [1, 2, 3, 4]{max_parallel}
    steps:
      - run: echo "$CARGO_BUILD_JOBS $NEXTEST_TEST_THREADS $RUST_TEST_THREADS" > {}/${{{{ matrix.n }}}}
"#,
            out.display()
        )
    };
    add_workflow(&d, "matrix.yml", &matrix(""));
    let path = budget_controller(&d);
    let o = local_ci_share(&d, &path, &[], &["-w", "matrix.yml", "--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    // one unit, but its four entries run together: two of them fit a budget of 8
    for n in 1..=4 {
        assert_eq!(threads_of(&d, &n.to_string()), "4 4 4", "entry {n}");
    }
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert_eq!(argv.lines().count(), 4, "{argv}");
    assert!(
        argv.lines()
            .all(|l| l.starts_with("exec --weight 4 --key ")),
        "{argv}"
    );
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(
        v["cpu"],
        serde_json::json!({"budget": 8, "jobs_at_once": 2, "threads_per_job": 4})
    );
    // `max-parallel: 1` runs them one after another: each has the whole budget
    add_workflow(&d, "matrix.yml", &matrix("\n      max-parallel: 1"));
    let o = local_ci_share(&d, &path, &[], &["-w", "matrix.yml", "--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    for n in 1..=4 {
        assert_eq!(threads_of(&d, &n.to_string()), "", "entry {n}");
    }
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v.get("cpu").is_none(), "{v}");
}

/// A workflow of one job with a matrix of four entries, each saying its threads.
fn matrix_workflow(d: &Demo, job: &str) {
    let out = d.home.join("threads");
    std::fs::create_dir_all(&out).unwrap();
    add_workflow(
        d,
        "matrix.yml",
        &format!(
            r#"name: Matrix
on: pull_request
jobs:
  {job}:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        n: [1, 2, 3, 4]
    steps:
      - run: echo "$CARGO_BUILD_JOBS $NEXTEST_TEST_THREADS $RUST_TEST_THREADS" > {}/${{{{ matrix.n }}}}
"#,
            out.display()
        ),
    );
}

#[test]
fn an_empty_thread_variable_of_the_caller_counts_as_not_set() {
    let (d, path) = thread_share_demo();
    let o = local_ci_threads(&d, &path, &[("CARGO_BUILD_JOBS", "")], &[]);
    assert!(o.status.success(), "{}", stdout(&o));
    assert_eq!(threads_of(&d, "plain"), "4 4 4");
}

#[test]
fn a_single_job_run_is_not_limited() {
    let d = demo("app/lib.sh");
    let out = d.home.join("threads");
    std::fs::create_dir_all(&out).unwrap();
    add_workflow(
        &d,
        "single.yml",
        &format!(
            "name: Single\non: pull_request\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"$CARGO_BUILD_JOBS $NEXTEST_TEST_THREADS $RUST_TEST_THREADS\" > {}/only\n",
            out.display()
        ),
    );
    let path = budget_controller(&d);
    let o = local_ci_share(&d, &path, &[], &["-w", "single.yml", "--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    assert_eq!(threads_of(&d, "only"), "");
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert!(!argv.contains("--weight"), "{argv}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v.get("cpu").is_none(), "{v}");
}

#[test]
fn matrix_entries_get_a_share_under_parallel_one() {
    let d = demo("app/lib.sh");
    matrix_workflow(&d, "entry");
    let path = budget_controller(&d);
    let o = local_ci_share(
        &d,
        &path,
        &[],
        &["-w", "matrix.yml", "--parallel", "1", "--json"],
    );
    assert!(o.status.success(), "{}", stdout(&o));
    // one unit at a time, but its four entries run together
    for n in 1..=4 {
        assert_eq!(threads_of(&d, &n.to_string()), "2 2 2", "entry {n}");
    }
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(
        v["cpu"],
        serde_json::json!({"budget": 8, "jobs_at_once": 4, "threads_per_job": 2})
    );
}

#[test]
fn the_entries_of_an_exclusive_job_do_not_count_as_running_together() {
    let d = demo("app/lib.sh");
    // an e2e job holds the repository lock, so its entries run one after another
    matrix_workflow(&d, "e2e");
    let path = budget_controller(&d);
    let o = local_ci_share(&d, &path, &[], &["-w", "matrix.yml", "--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    for n in 1..=4 {
        assert_eq!(threads_of(&d, &n.to_string()), "", "entry {n}");
    }
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert!(!argv.contains("--weight"), "{argv}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v.get("cpu").is_none(), "{v}");
}

#[test]
fn no_share_is_given_when_the_controller_cannot_say_its_budget() {
    let (d, _) = thread_share_demo();
    let bin = d.home.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake = bin.join("task-traffic-controller");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\nif [ \"$1\" = inspect ]; then exit 1; fi\necho \"$*\" >> {}/argv\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n",
            d.home.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let o = local_ci_threads(&d, &path, &[], &["--json"]);
    assert!(o.status.success(), "{}", stdout(&o));
    assert_eq!(threads_of(&d, "plain"), "");
    assert_eq!(threads_of(&d, "own"), "1 7");
    let argv = std::fs::read_to_string(d.home.join("argv")).unwrap();
    assert_eq!(argv.lines().count(), 2, "{argv}");
    assert!(!argv.contains("--weight"), "{argv}");
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert!(v.get("cpu").is_none(), "{v}");
    let err = String::from_utf8_lossy(&o.stderr);
    assert_eq!(
        err.matches("the CPU share of the jobs is off").count(),
        1,
        "{err}"
    );
    assert!(err.contains("`inspect` failed"), "{err}");
}
