//! End-to-end tests for `submit` / `wait` / `jobs` / `cancel` and estimates.

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct TestEnv {
    dir: TempDir,
}

impl TestEnv {
    fn new(config: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let p = dir
            .path()
            .join("config/task-traffic-controller/config.toml");
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, config).unwrap();
        TestEnv { dir }
    }

    fn state(&self) -> PathBuf {
        self.dir.path().join("state/task-traffic-controller")
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_task-traffic-controller"));
        c.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.dir.path())
            .env("XDG_STATE_HOME", self.dir.path().join("state"))
            .env("XDG_CONFIG_HOME", self.dir.path().join("config"))
            .current_dir(self.dir.path())
            .stdin(Stdio::null());
        c
    }

    fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().unwrap()
    }

    fn submit(&self, extra: &[&str], script: &str) -> (String, String) {
        let out = self
            .cmd()
            .arg("submit")
            .args(extra)
            .args(["--", "sh", "-c", script])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        (
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    fn jobs_json(&self) -> Vec<serde_json::Value> {
        let out = self.run(&["jobs", "--json"]);
        assert!(out.status.success());
        serde_json::from_slice(&out.stdout).unwrap()
    }

    /// Job `id` finished long ago, as far as its descriptor says.
    fn finished_long_ago(&self, id: &str) {
        let p = self.state().join(format!("jobs/{id}.json"));
        let mut job: serde_json::Value = serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
        assert!(job["finished_at"].is_string(), "{job}");
        job["finished_at"] = "2020-01-01T00:00:00Z".into();
        fs::write(&p, serde_json::to_vec(&job).unwrap()).unwrap();
    }

    fn state_of(&self, id: &str) -> String {
        self.jobs_json()
            .iter()
            .find(|j| j["id"] == id)
            .map(|j| j["state"].as_str().unwrap().to_string())
            .unwrap_or_else(|| "missing".into())
    }
}

fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn submit_returns_immediately_and_wait_collects_output_and_code() {
    let env = TestEnv::new("budget = 4\n");
    let t = Instant::now();
    let (id, _) = env.submit(&[], "echo hello; sleep 1; echo bye >&2; exit 5");
    assert!(t.elapsed() < Duration::from_secs(5), "submit blocked");
    assert_eq!(id.len(), 32);
    let out = env.run(&["wait", &id]);
    assert_eq!(out.status.code(), Some(5));
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains(&format!("=== task-traffic-controller job {id} exit 5")),
        "{text}"
    );
    assert!(text.contains("hello\n") && text.contains("bye\n"), "{text}");
    assert_eq!(env.state_of(&id), "done 5");
    let out = env.run(&["wait", "--no-output", &id]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(
        text.starts_with("=== task-traffic-controller job"),
        "{text}"
    );
}

#[test]
fn wait_reports_in_given_order_and_first_failure_wins() {
    let env = TestEnv::new("budget = 4\n");
    let (a, _) = env.submit(&[], "sleep 0.5; echo A; exit 3");
    let (b, _) = env.submit(&[], "echo B; exit 0");
    let (c, _) = env.submit(&[], "echo C; exit 9");
    let out = env.run(&["wait", &b, &a, &c]);
    assert_eq!(out.status.code(), Some(3));
    let text = String::from_utf8_lossy(&out.stdout);
    let pa = text.find("B\n").unwrap();
    let pb = text.find("A\n").unwrap();
    let pc = text.find("C\n").unwrap();
    assert!(pa < pb && pb < pc, "{text}");
}

#[test]
fn jobs_are_gated_by_the_budget() {
    let env = TestEnv::new("budget = 1\n");
    let (a, _) = env.submit(&[], "sleep 1");
    let (b, _) = env.submit(&[], "true");
    assert!(wait_until(Duration::from_secs(5), || env.state_of(&a) == "running"));
    assert_eq!(env.state_of(&b), "queued");
    assert!(wait_until(Duration::from_secs(5), || {
        String::from_utf8_lossy(&env.run(&["status"]).stdout).contains("waiting (1)")
    }));
    assert!(env.run(&["wait", &a, &b]).status.success());
    assert_eq!(env.state_of(&b), "done 0");
}

#[test]
fn cancel_queued_and_running_jobs() {
    let env = TestEnv::new("budget = 1\n");
    let pidfile = env.dir.path().join("child.pid");
    let (running, _) = env.submit(
        &[],
        &format!("echo $$ > {}; exec sleep 30", pidfile.display()),
    );
    let (queued, _) = env.submit(&[], "echo never");
    assert!(wait_until(Duration::from_secs(5), || pidfile.exists()));
    assert!(wait_until(Duration::from_secs(5), || env.state_of(&queued) == "queued"));
    let out = env.run(&["cancel", &queued, &running]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = env.run(&["wait", "--no-output", &queued, &running]);
    assert_eq!(out.status.code(), Some(143));
    assert_eq!(env.state_of(&queued), "done 143");
    assert_eq!(env.state_of(&running), "done 143");
    let child_pid: i32 = fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || unsafe {
            libc::kill(child_pid, 0) != 0
        }),
        "sleep child survived cancel"
    );
    let out = env.run(&["cancel", &queued]);
    assert!(String::from_utf8_lossy(&out.stdout).contains("already finished"));
}

#[test]
fn job_survives_submitter_and_records_history() {
    let env = TestEnv::new("budget = 4\n");
    // The submitter process has already exited by the time submit() returns.
    let (id, _) = env.submit(&[], "sleep 1; echo done");
    let out = env.run(&["wait", &id]);
    assert!(out.status.success());
    // The first `wait` prints the header and the output...
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (header, body) = text.split_once('\n').unwrap();
    assert!(
        header.starts_with(&format!("=== task-traffic-controller job {id} exit 0")),
        "{header}"
    );
    assert_eq!(body, "done\n");
    // ...and the output, handed over, is gone: a second `wait` still knows
    // how the job ended, and has nothing more to print.
    assert!(!env.state().join(format!("jobs/{id}.log")).exists());
    let again = env.run(&["wait", &id]);
    assert!(again.status.success());
    let text = String::from_utf8_lossy(&again.stdout).into_owned();
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(text.contains("exit 0"), "{text}");
    // The runner records the run right after it has written the exit file.
    let hist = env.state().join("history.jsonl");
    assert!(wait_until(Duration::from_secs(10), || {
        fs::read_to_string(&hist).is_ok_and(|h| h.lines().count() == 1)
    }));
}

#[test]
fn wait_on_unknown_id_is_an_error() {
    let env = TestEnv::new("budget = 4\n");
    let out = env.run(&["wait", "nope"]);
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn submit_prints_an_estimate_from_history() {
    let env = TestEnv::new("budget = 1\n");
    // The command only holds while `hold` exists, so learning its key is
    // instant; the recorded duration is then replaced by a fixed one, so the
    // figures below come from that history and not from how long a real run
    // took on a loaded machine.
    let script = "if [ -e hold ]; then sleep 30; fi";
    let out = env.run(&["exec", "--", "sh", "-c", script]);
    assert!(out.status.success());
    let path = env.state().join("history.jsonl");
    let hist = fs::read_to_string(&path).unwrap();
    let mut rec: serde_json::Value = serde_json::from_str(hist.lines().next().unwrap()).unwrap();
    rec["wall_secs"] = 100.0.into();
    rec["cpu_secs"] = 0.1.into();
    fs::write(&path, format!("{rec}\n")).unwrap();
    fs::write(env.dir.path().join("hold"), "").unwrap();
    // Seconds are truncated and the first job has already run for a moment,
    // so check the minute only (100s is 1:39 or 1:40, a few seconds later
    // 1:3x).
    let (a, est_a) = env.submit(&[], script);
    assert!(est_a.contains("done in ~1:"), "{est_a}");
    assert!(!est_a.contains('?'), "{est_a}");
    // Second one waits behind the first: start in ~1:xx, done in ~3:xx.
    let (b, est_b) = env.submit(&[], script);
    assert!(est_b.contains("start in ~1:"), "{est_b}");
    assert!(est_b.contains("done in ~3:"), "{est_b}");
    // Unknown command: rough estimate.
    let (c, est_c) = env.submit(&[], "true # never seen");
    assert!(est_c.contains('?'), "{est_c}");
    let jobs = String::from_utf8_lossy(&env.run(&["jobs"]).stdout).into_owned();
    assert!(jobs.contains("start in ~"), "{jobs}");
    for id in [&a, &b, &c] {
        assert!(env.run(&["cancel", id]).status.success());
    }
}

#[test]
fn gc_removes_old_finished_jobs_only() {
    let env = TestEnv::new("budget = 4\n");
    let (id, _) = env.submit(&[], "true");
    let (fresh, _) = env.submit(&[], "true");
    assert!(env.run(&["wait", &id, &fresh]).status.success());
    // One finished long ago, one just now: only the first is past
    // `jobs_keep_days` (a day).
    env.finished_long_ago(&id);
    let out = env.run(&["gc"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("removed 1 old jobs"), "{text}");
    let left = env.jobs_json();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0]["id"], fresh.as_str());
}

/// With 0 a job's own runner, sweeping right after the job, would remove
/// the result before `wait` reads it (`submit A; submit B; wait A B`).
#[test]
fn jobs_keep_days_zero_is_refused() {
    let env = TestEnv::new("budget = 4\njobs_keep_days = 0\n");
    let out = env.run(&["jobs"]);
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("jobs_keep_days must be between 1 and 3650"),
        "{err}"
    );
}

/// The runner writes the exit code while its entry is still in the queue
/// (it cannot leave the queue here: the test holds the ledger lock). With
/// the order the other way round a `wait` could find a job with no entry
/// and no exit code, and call it lost.
#[test]
fn the_runner_records_the_exit_before_it_leaves_the_queue() {
    use task_traffic_controller::{ledger::Ledger, lock};
    let env = TestEnv::new("budget = 4\n");
    let go = env.dir.path().join("go");
    let (id, _) = env.submit(
        &[],
        &format!(
            "while [ ! -e '{}' ]; do sleep 0.05; done; exit 4",
            go.display()
        ),
    );
    assert!(wait_until(Duration::from_secs(10), || env.state_of(&id) == "running"));
    let held = lock::acquire(&env.state().join("ledger.lock"), Duration::from_secs(10)).unwrap();
    fs::write(&go, "").unwrap();
    let exit = env.state().join(format!("jobs/{id}.exit"));
    assert!(
        wait_until(Duration::from_secs(8), || exit.exists()),
        "no exit code while the runner still had its entry"
    );
    assert_eq!(fs::read_to_string(&exit).unwrap().trim(), "4");
    let ledger = Ledger::load(&env.state().join("ledger.json")).unwrap();
    assert!(ledger.find(&id).is_some(), "the entry was released first");
    drop(held);
    assert_eq!(env.run(&["wait", &id]).status.code(), Some(4));
}

#[test]
fn a_job_log_over_the_limit_is_cut_before_wait_can_see_it() {
    let env = TestEnv::new("budget = 4\njobs_log_max_mib = 1\n");
    let (id, _) = env.submit(&[], "yes 0123456789abcdef | head -c 3000000; echo the-end");
    let out = env.run(&["wait", &id]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.len() < (1 << 20) + 1000, "{}", text.len());
    assert!(text.contains("were cut"), "{}", &text[..300]);
    assert!(text.trim_end().ends_with("the-end"));
}

#[test]
fn wait_removes_the_output_once_it_has_handed_it_over() {
    let env = TestEnv::new("budget = 4\n");
    let (id, _) = env.submit(&[], "echo hi");
    let out = env.run(&["wait", &id]);
    assert!(String::from_utf8_lossy(&out.stdout).contains("hi"));
    let mut left: Vec<String> = fs::read_dir(env.state().join("jobs"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort();
    // The descriptor and exit code stay for `jobs` and a repeated `wait`.
    assert_eq!(left, [format!("{id}.exit"), format!("{id}.json")]);
    assert_eq!(env.state_of(&id), "done 0");
    // `--no-output` hands nothing over, so it leaves the log alone.
    let (id2, _) = env.submit(&[], "echo keep");
    assert!(env.run(&["wait", "--no-output", &id2]).status.success());
    assert!(env.state().join(format!("jobs/{id2}.log")).exists());
}

#[test]
fn a_job_log_under_the_limit_is_printed_whole() {
    let env = TestEnv::new("budget = 4\n");
    let (id, _) = env.submit(&[], "echo first; echo second");
    let out = env.run(&["wait", &id]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.ends_with("first\nsecond\n"), "{text}");
    assert!(!text.contains("were cut"));
}

#[test]
fn exec_and_submit_clean_up_by_themselves_when_the_sweep_is_due() {
    let env = TestEnv::new("budget = 4\n");
    let state = env.state();
    fs::create_dir_all(state.join("jobs")).unwrap();
    // A job file with no descriptor, a day old, and a sweep last run long ago.
    let orphan = state.join("jobs/gone.log");
    fs::write(&orphan, "x").unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&orphan)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 86_400))
        .unwrap();
    // The first run only starts the clock.
    assert!(env.run(&["exec", "--", "true"]).status.success());
    assert!(orphan.exists());
    let stale = r#"{"complete":"2000-01-01T00:00:00Z"}"#;
    fs::write(state.join("last-sweep"), stale).unwrap();
    // The sweep runs after the command, before `exec` returns.
    let out = env.run(&["exec", "--", "sh", "-c", "test -e jobs-marker || echo ran"]);
    assert!(out.status.success());
    assert!(!orphan.exists());
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // A job does the same from its runner, once the job is over: `submit`
    // itself does nothing of the kind, and `wait` does not wait for it.
    fs::write(&orphan, "x").unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(&orphan)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 86_400))
        .unwrap();
    fs::write(state.join("last-sweep"), stale).unwrap();
    let (id, _) = env.submit(&[], "true");
    let out = env.run(&["wait", &id]);
    assert!(out.status.success());
    assert!(wait_until(Duration::from_secs(10), || !orphan.exists()));
}

#[test]
fn gc_exits_2_after_doing_the_rest_when_a_step_failed() {
    let env = TestEnv::new("budget = 4\n");
    let (id, _) = env.submit(&[], "true");
    assert!(env.run(&["wait", &id]).status.success());
    env.finished_long_ago(&id);
    // The stamp can neither be read nor written.
    let stamp = env.state().join("last-sweep");
    let _ = fs::remove_file(&stamp);
    fs::create_dir_all(&stamp).unwrap();
    let out = env.run(&["gc"]);
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(err.lines().count(), 1, "{err}");
    assert!(err.starts_with("task-traffic-controller: gc: "), "{err}");
    assert!(err.contains("last-sweep"), "{err}");
    // The sweep itself ran.
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("removed 1 old jobs"), "{text}");
}

/// A job `wait` calls lost keeps its log: only output that was printed for
/// a job with an exit record is removed.
#[test]
fn a_job_reported_as_lost_keeps_its_log() {
    let env = TestEnv::new("budget = 4\n");
    let jobs = env.state().join("jobs");
    fs::create_dir_all(&jobs).unwrap();
    // A descriptor whose runner is gone: no exit file, no ledger entry.
    fs::write(
        jobs.join("lostjob.json"),
        r#"{"id":"lostjob","argv":["true"],"cwd":"/","weight":null,"front":false,"key":null,
            "submitted_at":"2020-01-01T00:00:00Z","submitter_pid":1}"#,
    )
    .unwrap();
    fs::write(jobs.join("lostjob.log"), "what it wrote before it died\n").unwrap();
    let out = env.run(&["wait", "lostjob"]);
    assert_eq!(out.status.code(), Some(2));
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("lost (runner died before finishing)"),
        "{text}"
    );
    assert!(text.ends_with("what it wrote before it died\n"), "{text}");
    assert_eq!(
        fs::read_to_string(jobs.join("lostjob.log")).unwrap(),
        "what it wrote before it died\n"
    );
    assert!(!jobs.join("lostjob.exit").exists());
}

/// The log of a running job is cut while it runs. Between two looks of the
/// runner (200 ms) it can pass the limit by what the job writes meanwhile;
/// this job writes about 1 MiB a second, so twice the limit is far above
/// that.
#[test]
fn the_log_of_a_running_job_stays_near_the_limit_while_it_runs() {
    let env = TestEnv::new("budget = 4\njobs_log_max_mib = 1\n");
    let script = "i=0; while [ $i -lt 80 ]; do \
                  head -c 50000 /dev/zero | tr '\\0' x; echo; sleep 0.05; i=$((i+1)); done; \
                  echo the-end";
    let (id, _) = env.submit(&["--detach"], script);
    let log = env.state().join(format!("jobs/{id}.log"));
    let exit = env.state().join(format!("jobs/{id}.exit"));
    let mut largest = 0;
    let mut shrank = false;
    let mut last = 0;
    assert!(wait_until(Duration::from_secs(60), || {
        let len = fs::metadata(&log).map_or(0, |m| m.len());
        largest = largest.max(len);
        shrank |= len < last;
        last = len;
        exit.exists()
    }));
    // 4 MB were written; the file never held anything like that.
    assert!(shrank, "the log was never cut while the job ran");
    assert!(largest < 2 << 20, "the log reached {largest} bytes");
    let out = env.run(&["wait", &id]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("were cut"),
        "{}",
        &text[..300.min(text.len())]
    );
    assert!(text.trim_end().ends_with("the-end"));
    assert!(text.len() < (1 << 20) + 1000, "{}", text.len());
}

#[test]
fn job_without_a_waiter_is_cancelled_after_grace() {
    let env = TestEnv::new("budget = 4\njobs_orphan_grace_secs = 1\n");
    let pidfile = env.dir.path().join("orphan.pid");
    let (id, _) = env.submit(
        &[],
        &format!("echo $$ > {}; exec sleep 30", pidfile.display()),
    );
    assert!(wait_until(Duration::from_secs(5), || pidfile.exists()));
    assert!(
        wait_until(Duration::from_secs(6), || {
            env.state_of(&id).starts_with("orphaned")
        }),
        "{}",
        env.state_of(&id)
    );
    let child_pid: i32 = fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(3), || unsafe {
            libc::kill(child_pid, 0) != 0
        }),
        "orphaned child kept running"
    );
    let out = env.run(&["wait", "--no-output", &id]);
    assert_eq!(out.status.code(), Some(143));
    assert!(String::from_utf8_lossy(&out.stdout).contains("orphaned"));
}

#[test]
fn waiter_keeps_the_job_alive_and_detach_opts_out() {
    // Grace of 3s: on a loaded machine spawning `wait` can take a while.
    let env = TestEnv::new("budget = 4\njobs_orphan_grace_secs = 3\n");
    // A waiter present from the start: the job runs to completion.
    let (a, _) = env.submit(&[], "sleep 3; echo kept");
    let out = env.run(&["wait", &a]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("kept"));
    // --detach: no waiter, still finishes.
    let (b, _) = env.submit(&["--detach"], "sleep 3; echo detached");
    assert!(wait_until(Duration::from_secs(10), || env.state_of(&b) == "done 0"));
    let log = fs::read_to_string(env.state().join(format!("jobs/{b}.log"))).unwrap();
    assert_eq!(log, "detached\n");
}

/// A `git` on PATH whose first call from a job runner sleeps `secs` (the
/// runner calls git to derive the key before it registers), and the PATH
/// to use it.
fn slow_first_git(env: &TestEnv, secs: u32) -> String {
    let bin = env.dir.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let marker = env.dir.path().join("git-slowed");
    let git = bin.join("git");
    fs::write(
        &git,
        format!(
            "#!/bin/sh\n\
             if ps -o args= -p $PPID | grep -q __run-job && [ ! -e {m} ]; then\n\
             touch {m}; sleep {secs}; fi\nexit 128\n",
            m = marker.display()
        ),
    )
    .unwrap();
    let mut perms = fs::metadata(&git).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
    fs::set_permissions(&git, perms).unwrap();
    format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

fn submit_with_path(env: &TestEnv, path: &str, script: &str) -> Output {
    env.cmd()
        .env("PATH", path)
        .args(["submit", "--", "sh", "-c", script])
        .output()
        .unwrap()
}

#[test]
fn back_to_back_submits_start_in_submit_order() {
    let env = TestEnv::new("budget = 1\n");
    // The first runner is 2 s slow to register; the second submit must
    // still queue behind it.
    let path = slow_first_git(&env, 2);
    let order = env.dir.path().join("order");
    let ids: Vec<String> = (1..=2)
        .map(|n| {
            let out = submit_with_path(&env, &path, &format!("echo {n} >> {}", order.display()));
            assert!(out.status.success());
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        })
        .collect();
    assert!(env
        .run(&["wait", "--no-output", &ids[0], &ids[1]])
        .status
        .success());
    assert_eq!(fs::read_to_string(&order).unwrap(), "1\n2\n");
}

#[test]
fn submit_warns_when_the_runner_is_slow_to_register() {
    let env = TestEnv::new("budget = 4\n");
    // The runner's first `git` call hangs for 7 s; submit gives up after
    // 5 s with a warning.
    let path = slow_first_git(&env, 7);
    let t = Instant::now();
    let out = submit_with_path(&env, &path, "exit 3");
    let took = t.elapsed();
    assert!(out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("has not joined the queue within 5s"), "{err}");
    assert!(
        took >= Duration::from_secs(5) && took < Duration::from_secs(7),
        "{took:?}"
    );
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert_eq!(id.len(), 32);
    // Past the starting grace, a job with no ledger entry is reported
    // lost, as the warning says.
    let out = env.run(&["wait", &id]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stdout).contains("lost"));
}

#[test]
fn wait_does_not_call_a_job_lost_while_the_ledger_lock_is_busy() {
    use fs2::FileExt;
    let env = TestEnv::new("budget = 4\n");
    // Held as a holder stalled on a saturated machine would, so the
    // runner cannot register until it is released.
    fs::create_dir_all(env.state()).unwrap();
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(env.state().join("ledger.lock"))
        .unwrap();
    lock.lock_exclusive().unwrap();
    let out = env
        .cmd()
        .env("TASK_TRAFFIC_CONTROLLER_LEDGER_LOCK_TIMEOUT_MS", "200")
        .args(["submit", "--", "echo", "ran"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    // submit gave up waiting after 5 s; the wait below runs past the
    // starting grace with the runner still unregistered.
    let waiter = env
        .cmd()
        .env("TASK_TRAFFIC_CONTROLLER_LEDGER_LOCK_TIMEOUT_MS", "200")
        .args(["wait", &id])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(2));
    drop(lock);
    let out = waiter.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}");
    assert!(stdout.contains("ran"), "{stdout}");
    assert!(!stdout.contains("lost"), "{stdout}");
}
