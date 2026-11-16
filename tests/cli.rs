//! End-to-end tests for the `task-traffic-controller` binary.
//!
//! Every test gets its own [`TestEnv`]: a temp directory supplying
//! `HOME` / `XDG_STATE_HOME` / `XDG_CONFIG_HOME` to the spawned binary via
//! `Command::env`, so the real `~/.local/state` is never touched and tests
//! can run in parallel.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct TestEnv {
    dir: TempDir,
}

impl TestEnv {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        TestEnv { dir }
    }

    fn with_config(self, toml: &str) -> Self {
        let p = self
            .dir
            .path()
            .join("config/task-traffic-controller/config.toml");
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, toml).unwrap();
        self
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
            .current_dir(self.dir.path());
        c
    }

    fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().unwrap()
    }

    fn marker_dir(&self) -> PathBuf {
        let p = self.dir.path().join("markers");
        fs::create_dir_all(&p).unwrap();
        p
    }

    /// Spawn `task-traffic-controller exec <extra> -- sh -c '<script>'` detached from our
    /// stdio so tests can wait on it.
    fn spawn_exec(&self, extra: &[&str], script: &str) -> Child {
        self.cmd()
            .arg("exec")
            .args(extra)
            .args(["--", "sh", "-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn ledger_json(&self) -> serde_json::Value {
        let out = self.run(&["status", "--json"]);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn entries(&self) -> Vec<serde_json::Value> {
        self.ledger_json()["entries"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
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

/// A script that records start/end timestamps (ns since epoch) in a marker
/// file and sleeps `secs`.
fn marker_script(dir: &Path, name: &str, secs: &str) -> String {
    let p = dir.join(name);
    let now = "perl -MTime::HiRes=time -e 'printf \"%.0f\\n\", time*1e9'";
    format!(
        "{now} > {p}.start; sleep {secs}; {now} > {p}.end",
        p = p.display()
    )
}

/// A script that runs until the test creates `<dir>/<name>.release` (giving
/// up after 60 s so a failed test cannot leave it behind), so the job holds
/// its slot for as long as the test needs rather than for a fixed time.
fn gated_script(dir: &Path, name: &str) -> String {
    let p = dir.join(name);
    format!(
        "i=0; while [ -d {dir} ] && [ ! -e {p}.release ] && [ $i -lt 1200 ]; do sleep 0.05; i=$((i+1)); done",
        p = p.display(),
        dir = dir.display()
    )
}

/// Releases only this test's marker when unwinding, including before a child
/// has started. The bounded gated script then exits without a lingering job.
struct Gate(PathBuf);

impl Gate {
    fn new(dir: &Path, name: &str) -> Self {
        Self(dir.join(format!("{name}.release")))
    }

    fn release(&self) {
        fs::write(&self.0, "").unwrap();
    }
}

impl Drop for Gate {
    fn drop(&mut self) {
        let _ = fs::write(&self.0, "");
    }
}

fn marked_gate(dir: &Path, name: &str) -> String {
    format!(
        "{}; {}; {}",
        marker_script(dir, name, "0"),
        gated_script(dir, name),
        marker_script(dir, &format!("{name}.released"), "0")
    )
}

fn read_ns(p: &Path) -> u128 {
    fs::read_to_string(p).unwrap().trim().parse().unwrap()
}

#[test]
fn exec_passes_through_exit_code_and_stdout() {
    let env = TestEnv::new();
    let out = env.run(&["exec", "--", "sh", "-c", "echo hi; exit 3"]);
    assert_eq!(out.status.code(), Some(3));
    assert_eq!(stdout(&out), "hi\n");
    assert!(env.entries().is_empty(), "entry must be removed after run");
}

#[test]
fn exec_without_command_is_a_usage_error() {
    let env = TestEnv::new();
    let out = env.run(&["exec", "--"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("Usage"));
}

#[test]
fn exec_missing_program_fails_cleanly() {
    let env = TestEnv::new();
    let out = env.run(&["exec", "--", "definitely-not-a-program-xyz"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(env.entries().is_empty());
}

#[test]
fn exec_records_history_and_learns_weight() {
    let env = TestEnv::new().with_config("budget = 8\n");
    // Two processes in parallel, each burning 1.2 s of its own CPU time
    // (user + sys, as cpu_secs counts it). Bounded by CPU time rather than
    // the clock, so a loaded machine only stretches the wall time. A single
    // process cannot burn 1.2 s of CPU in less wall time, so the run is
    // always long enough to learn from (weight::MIN_USABLE_WALL_SECS, 1.0).
    // Counting user time alone left the wall time to the user/sys split of
    // the `times` loop: about 1.3 s here, but under 1.0 s elsewhere.
    let script =
        "for i in 1 2; do perl -e '1 while do { my @t = times; $t[0] + $t[1] } < 1.2' & done; wait";
    let out = env.run(&["exec", "--", "sh", "-c", script]);
    assert!(out.status.success());
    let hist = fs::read_to_string(env.state().join("history.jsonl")).unwrap();
    let rec: serde_json::Value = serde_json::from_str(hist.lines().next().unwrap()).unwrap();
    // The learning floor itself, so a short run fails here and not as a
    // missing "(Learned)" below.
    assert!(rec["wall_secs"].as_f64().unwrap() >= 1.0, "{rec}");
    assert!(rec["cpu_secs"].as_f64().unwrap() >= 2.0, "{rec}");
    assert_eq!(rec["exit_code"], 0);
    assert_eq!(rec["weight_used"], 2, "default weight on first run");

    let out = env.run(&["history", "--", "sh", "-c", script]);
    let text = stdout(&out);
    assert!(text.contains("next: weight"), "{text}");
    // On a loaded machine the run is recorded as contended; with no calm
    // run to prefer, it is still what the weight is learned from.
    assert!(text.contains("(Learned)"), "{text}");
}

#[test]
fn concurrency_never_exceeds_budget() {
    let env = TestEnv::new().with_config("budget = 2\n");
    let dir = env.marker_dir();
    let n = 5;
    let children: Vec<Child> = (0..n)
        .map(|i| {
            env.spawn_exec(
                &["--weight", "1"],
                &marker_script(&dir, &format!("m{i}"), "0.6"),
            )
        })
        .collect();
    for mut c in children {
        assert!(c.wait().unwrap().success());
    }
    let mut spans: Vec<(u128, u128)> = (0..n)
        .map(|i| {
            (
                read_ns(&dir.join(format!("m{i}.start"))),
                read_ns(&dir.join(format!("m{i}.end"))),
            )
        })
        .collect();
    spans.sort();
    // Max overlap at any start instant must be <= budget.
    for (s, _) in &spans {
        let overlapping = spans.iter().filter(|(a, b)| a <= s && s < b).count();
        assert!(overlapping <= 2, "{overlapping} ran at once: {spans:?}");
    }
    assert!(env.entries().is_empty());
}

#[test]
fn head_reserves_and_light_entry_does_not_overtake() {
    let env = TestEnv::new().with_config("budget = 3\n");
    let dir = env.marker_dir();
    let mut big = env.spawn_exec(&["--weight", "2"], &marker_script(&dir, "big", "1"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let mut heavy = env.spawn_exec(&["--weight", "2"], &marker_script(&dir, "heavy", "0.3"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries()
            .iter()
            .any(|e| e["weight"] == 2 && e["state"] == "waiting")
    }));
    // Nothing has been learned about `light`, so it is not short and may not
    // backfill (see short_command_backfills_behind_a_head_that_does_not_fit).
    let mut light = env.spawn_exec(&["--weight", "1"], &marker_script(&dir, "light", "0.3"));
    assert!(big.wait().unwrap().success());
    assert!(heavy.wait().unwrap().success());
    assert!(light.wait().unwrap().success());
    let heavy_start = read_ns(&dir.join("heavy.start"));
    let light_start = read_ns(&dir.join("light.start"));
    assert!(
        light_start >= heavy_start,
        "light overtook the reserving head"
    );
}

/// The shape that held a queue for 50 minutes: a resident command (a
/// development server) holds part of the budget, a `--front` head does not
/// fit next to it, and a short command behind the head would. The short
/// command runs while the resident still holds its slot, and the head's
/// progress line names who holds the budget.
#[test]
fn short_command_backfills_behind_a_head_that_does_not_fit() {
    let env = TestEnv::new()
        .with_config("budget = 4\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    let resident_gate = Gate::new(&dir, "resident");
    // Teach task-traffic-controller that `quick` is short (a run of at least 1 s counts).
    let learn = env.run(&["exec", "--key", "quick", "--", "sh", "-c", "sleep 1.1"]);
    assert!(learn.status.success());
    let mut resident = env.spawn_exec(&["--weight", "2"], &gated_script(&dir, "resident"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let head_log = dir.join("head.log");
    let head = env
        .cmd()
        .arg("exec")
        .args(["--front", "--weight", "4"])
        .args(["--", "sh", "-c", &marker_script(&dir, "head", "0")])
        .stderr(fs::File::create(&head_log).unwrap())
        .spawn()
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().len() == 2
            && fs::read_to_string(&head_log)
                .unwrap()
                .contains("; held by ")
    }));
    let mut quick = env.spawn_exec(
        &["--weight", "1", "--key", "quick"],
        &marker_script(&dir, "quick", "0"),
    );
    // Finished and gone from the ledger, so `status` sees only the
    // resident and the head.
    let quick_done = wait_until(Duration::from_secs(10), || {
        dir.join("quick.end").exists() && env.entries().len() == 2
    });
    let status = stdout(&env.run(&["status"]));
    resident_gate.release();
    assert!(quick_done, "quick never ran next to the resident: {status}");
    assert!(
        status.contains("the head needs w4, 2 of 4 free"),
        "{status}"
    );
    assert!(quick.wait().unwrap().success());
    assert!(resident.wait().unwrap().success());
    let out = head.wait_with_output().unwrap();
    assert!(out.status.success());
    let err = fs::read_to_string(&head_log).unwrap();
    assert!(
        err.contains("(budget)") && err.contains("; held by "),
        "{err}"
    );
    assert!(read_ns(&dir.join("quick.start")) < read_ns(&dir.join("head.start")));
}

#[test]
fn measured_use_beyond_the_declared_weight_holds_the_next_start() {
    let env = TestEnv::new()
        .with_config("budget = 2\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    // Declared weight 1, but six workers each burning 4 s of their own
    // CPU time. Bounded by CPU time, so load only stretches the wall time.
    let now = "perl -MTime::HiRes=time -e 'printf \"%.0f\\n\", time*1e9'";
    let burn = format!(
        "for i in 1 2 3 4 5 6; do perl -e '1 while (times)[0] < 4' & done; wait; {now} > {}",
        dir.join("hog.end").display()
    );
    let mut hog = env.spawn_exec(&["--weight", "1"], &burn);
    assert!(
        wait_until(Duration::from_secs(20), || {
            env.entries()
                .iter()
                .any(|e| e["using"].as_f64().unwrap_or(0.0) >= 2.5)
        }),
        "the running tree was never measured: {:?}",
        env.entries()
    );
    let status = stdout(&env.run(&["status"]));
    assert!(status.contains("w1  (using "), "{status}");
    // By declared weight (1 + 1 <= 2) this would start at once.
    let mut next = env.spawn_exec(&["--weight", "1"], &marker_script(&dir, "next", "0"));
    assert!(hog.wait().unwrap().success());
    assert!(next.wait().unwrap().success());
    assert!(
        read_ns(&dir.join("next.start")) >= read_ns(&dir.join("hog.end")),
        "started while the measured tree filled the budget"
    );
}

/// The shape that ran every job of a CI runner at once: a gated command
/// that wraps each of its own steps in `exec`. The steps queue against the
/// same budget as children of the outer entry.
#[test]
fn nested_execs_that_do_not_fit_together_run_one_after_another() {
    let env = TestEnv::new()
        .with_config("budget = 4\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    let (gate_a, gate_b) = (Gate::new(&dir, "a"), Gate::new(&dir, "b"));
    let step = |name: &str| {
        format!(
            "{BIN} exec --weight 3 -- {} &\n",
            script_file(&dir, name, &marked_gate(&dir, name))
        )
    };
    let outer = format!("{}{}wait\n", step("a"), step("b"));
    let mut parent = env.spawn_exec(&["--weight", "1"], &script_file(&dir, "outer", &outer));
    // One step runs, the other waits; both are entries nested in the outer.
    assert!(
        wait_until(Duration::from_secs(10), || {
            let nested = nested_entries(&env);
            nested.len() == 2 && nested.iter().filter(|e| e["state"] == "running").count() == 1
        }),
        "{:?}",
        env.entries()
    );
    let entries = env.entries();
    let outer_entry = entries.iter().find(|e| e["parent"].is_null()).unwrap();
    for e in nested_entries(&env) {
        assert_eq!(e["parent"], outer_entry["id"], "{entries:?}");
    }
    let status = stdout(&env.run(&["status"]));
    assert!(
        status.contains(&format!("[in {}] ", outer_entry["pid"])),
        "{status}"
    );
    // The outer's weight of 1 is inside the step's 3, not on top of it.
    assert!(status.contains("budget 3/4 used"), "{status}");
    let inspect: serde_json::Value =
        serde_json::from_slice(&env.run(&["inspect", "--json"]).stdout).unwrap();
    assert_eq!(
        inspect["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["parent"] == outer_entry["id"])
            .count(),
        2,
        "{inspect}"
    );
    // The second stays queued for as long as the first holds its slot.
    // (The marker is written after the entry turns running: wait for it.)
    assert!(wait_until(Duration::from_secs(10), || {
        dir.join("a.start").exists() || dir.join("b.start").exists()
    }));
    let (first, second) = if dir.join("a.start").exists() {
        ("a", "b")
    } else {
        ("b", "a")
    };
    assert!(!dir.join(format!("{second}.start")).exists());
    if first == "a" {
        gate_a.release();
    } else {
        gate_b.release();
    }
    assert!(wait_until(Duration::from_secs(10), || dir
        .join(format!("{second}.start"))
        .exists()));
    gate_a.release();
    gate_b.release();
    assert!(parent.wait().unwrap().success());
    assert!(
        read_ns(&dir.join(format!("{second}.start")))
            >= read_ns(&dir.join(format!("{first}.released.end"))),
        "the steps overlapped"
    );
    assert_eq!(history_lines(&env), 3);
    assert!(env.entries().is_empty());
}

/// A command that waits for its nested command never gets its budget back
/// by waiting: the nested one starts although nothing is free, and ahead
/// of a top-level entry that queued before it.
#[test]
fn a_nested_exec_starts_when_the_budget_is_full_and_someone_queued_first() {
    let env = TestEnv::new()
        .with_config("budget = 2\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    let (hog_gate, go) = (Gate::new(&dir, "hog"), Gate::new(&dir, "go"));
    let mut hog = env.spawn_exec(&["--weight", "1"], &gated_script(&dir, "hog"));
    let outer = format!(
        "{}\n{BIN} exec --weight 2 -- {}\n",
        gated_script(&dir, "go"),
        script_file(&dir, "step", &marker_script(&dir, "step", "0"))
    );
    let mut parent = env.spawn_exec(&["--weight", "1"], &script_file(&dir, "outer", &outer));
    assert!(wait_until(Duration::from_secs(10), || {
        env.entries()
            .iter()
            .filter(|e| e["state"] == "running")
            .count()
            == 2
    }));
    // Queued before the nested step exists, and needing the whole budget.
    let mut big = env.spawn_exec(&["--weight", "2"], &marker_script(&dir, "big", "0"));
    assert!(wait_until(Duration::from_secs(10), || env.entries().len() == 3));
    go.release();
    // The outer command finishes while the hog still holds its slot.
    assert!(parent.wait().unwrap().success());
    assert!(dir.join("step.end").exists());
    assert!(!dir.join("big.start").exists());
    hog_gate.release();
    assert!(hog.wait().unwrap().success());
    assert!(big.wait().unwrap().success());
    assert!(read_ns(&dir.join("big.start")) >= read_ns(&dir.join("step.end")));
}

/// What a nested entry runs is measured on that entry, not also on the
/// one it is nested in.
#[test]
fn what_a_nested_exec_uses_is_not_measured_on_its_parent_too() {
    let env = TestEnv::new()
        .with_config("budget = 2\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    // One worker burning 6 s of its own CPU time: bounded by CPU time, so
    // load only stretches the wall time.
    let burn = "perl -e '1 while (times)[0] < 6'\n";
    let outer = format!(
        "{BIN} exec --weight 1 -- {}\n",
        script_file(&dir, "burn", burn)
    );
    let mut parent = env.spawn_exec(&["--weight", "1"], &script_file(&dir, "outer", &outer));
    let using = |e: &serde_json::Value| e["using"].as_f64().unwrap_or(0.0);
    assert!(
        wait_until(Duration::from_secs(30), || {
            nested_entries(&env).iter().any(|e| using(e) >= 0.5)
        }),
        "the nested tree was never measured: {:?}",
        env.entries()
    );
    // The outer's own sampler has had time to see the same processes.
    let on_parent = wait_until(Duration::from_millis(2500), || {
        env.entries()
            .iter()
            .any(|e| e["parent"].is_null() && using(e) >= 0.3)
    });
    let entries = env.entries();
    assert!(parent.wait().unwrap().success());
    assert!(!on_parent, "charged twice: {entries:?}");
}

/// A command that only starts its steps and waits for them does not learn
/// their weight: what they used is recorded on them, and taken off what
/// was measured for the whole tree.
#[test]
fn a_command_does_not_learn_the_weight_of_what_its_nested_execs_used() {
    let env = TestEnv::new()
        .with_config("budget = 4\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    // Two workers each burning 2 s of their own CPU time.
    let burn = "for i in 1 2; do perl -e '1 while (times)[0] < 2' & done; wait\n";
    let outer = script_file(
        &dir,
        "outer",
        &format!(
            "{BIN} exec --weight 1 -- {}\n",
            script_file(&dir, "burn", burn)
        ),
    );
    let out = env.run(&["exec", "--", "sh", "-c", &outer]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let records: Vec<serde_json::Value> = fs::read_to_string(env.state().join("history.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let cpu_of = |script: &str| {
        records
            .iter()
            .find(|r| r["argv"].as_str().unwrap().contains(script))
            .unwrap_or_else(|| panic!("no record for {script}: {records:?}"))["cpu_secs"]
            .as_f64()
            .unwrap()
    };
    assert!(cpu_of("burn.sh") >= 3.5, "{records:?}");
    assert!(cpu_of("outer.sh") < 1.0, "{records:?}");
    // The second run queues at weight 1, not at the two cores its step
    // used.
    let mut again = env.spawn_exec(&[], &outer);
    assert!(wait_until(Duration::from_secs(10), || !nested_entries(
        &env
    )
    .is_empty()));
    let entries = env.entries();
    assert!(again.wait().unwrap().success());
    let outer_entry = entries.iter().find(|e| e["parent"].is_null()).unwrap();
    assert_eq!(outer_entry["weight"], 1, "{entries:?}");
}

/// Stopping a command also drops the nested commands of it that have not
/// started: nobody is left to want them.
#[test]
fn stopping_a_command_drops_its_nested_execs_that_are_still_waiting() {
    let env = TestEnv::new()
        .with_config("budget = 2\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let dir = env.marker_dir();
    let (gate_a, gate_b) = (Gate::new(&dir, "a"), Gate::new(&dir, "b"));
    let step = |name: &str| {
        format!(
            "{BIN} exec --weight 2 -- {} &\n",
            script_file(&dir, name, &marked_gate(&dir, name))
        )
    };
    let outer = format!("{}{}wait\n", step("a"), step("b"));
    let mut parent = env.spawn_exec(&["--weight", "1"], &script_file(&dir, "outer", &outer));
    let started = || {
        ["a", "b"]
            .iter()
            .filter(|n| dir.join(format!("{n}.start")).exists())
            .count()
    };
    assert!(
        wait_until(Duration::from_secs(10), || {
            let nested = nested_entries(&env);
            nested.len() == 2
                && nested.iter().filter(|e| e["state"] == "waiting").count() == 1
                && started() == 1
        }),
        "{:?}",
        env.entries()
    );
    // SIGTERM to the outer `exec`, which passes it on to its command.
    assert_eq!(
        unsafe { libc::kill(parent.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    assert!(!parent.wait().unwrap().success());
    // The step that was waiting leaves the queue without starting; the
    // one that runs is not touched.
    assert!(
        wait_until(Duration::from_secs(10), || {
            let entries = env.entries();
            entries.len() == 1 && entries[0]["state"] == "running"
        }),
        "{:?}",
        env.entries()
    );
    gate_a.release();
    gate_b.release();
    assert!(
        wait_until(Duration::from_secs(10), || env.entries().is_empty()),
        "{:?}",
        env.entries()
    );
    assert_eq!(started(), 1, "the waiting step ran after its parent ended");
}

/// A nested command that waits for one of its siblings (a server and its
/// client) is not held for ever: after the configured time the next one
/// starts although the budget is full.
#[test]
fn a_nested_exec_that_does_not_fit_starts_after_the_sibling_limit() {
    let env = TestEnv::new().with_config(
        "budget = 2\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\nnested_sibling_max_wait_secs = 2\n",
    );
    let dir = env.marker_dir();
    let server = Gate::new(&dir, "server");
    // The client is started only once the server runs.
    let outer = format!(
        "{BIN} exec --weight 2 -- {} &\nwhile [ ! -e {} ]; do sleep 0.05; done\n{BIN} exec --weight 2 -- {}\n",
        script_file(&dir, "server", &marked_gate(&dir, "server")),
        dir.join("server.start").display(),
        script_file(&dir, "client", &marker_script(&dir, "client", "0"))
    );
    let mut parent = env.spawn_exec(
        &["--weight", "1"],
        &script_file(&dir, "outer", &format!("{outer}wait\n")),
    );
    // The client is queued behind the server for the budget.
    assert!(
        wait_until(Duration::from_secs(10), || {
            nested_entries(&env).iter().any(|e| e["state"] == "waiting")
        }),
        "{:?}",
        env.entries()
    );
    assert!(!dir.join("client.start").exists());
    // It runs while the server still holds its slot.
    assert!(
        wait_until(Duration::from_secs(20), || dir.join("client.end").exists()),
        "{:?}",
        env.entries()
    );
    assert!(!dir.join("server.released.end").exists());
    server.release();
    assert!(parent.wait().unwrap().success());
    assert!(
        read_ns(&dir.join("client.start")) - read_ns(&dir.join("server.start")) >= 2_000_000_000,
        "started before the limit"
    );
}

/// `--nested inherit` (or the environment variable, for a whole tree) is
/// what every nested `exec` did before: it runs at once under the outer
/// entry and is not a run of its own.
#[test]
fn nested_inherit_runs_at_once_under_the_outer_entry() {
    let env = TestEnv::new()
        .with_config("budget = 1\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    let nested = format!("{BIN} exec --weight 1 -- echo nested-ok");
    let by_flag = format!("{BIN} exec --nested inherit --weight 1 -- echo nested-ok");
    for (var, inner) in [(None, &by_flag), (Some("inherit"), &nested)] {
        let mut cmd = env.cmd();
        if let Some(v) = var {
            cmd.env("TASK_TRAFFIC_CONTROLLER_NESTED", v);
        }
        let before = history_lines(&env);
        let out = cmd
            .args(["exec", "--weight", "1", "--", "sh", "-c", inner])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(stdout(&out), "nested-ok\n");
        assert_eq!(history_lines(&env), before + 1, "{var:?}");
    }
    // The flag wins over the variable.
    let out = env
        .cmd()
        .env("TASK_TRAFFIC_CONTROLLER_NESTED", "inherit")
        .args(["exec", "--weight", "1", "--", "sh", "-c"])
        .arg(format!(
            "{BIN} exec --nested admit --weight 1 -- echo nested-ok"
        ))
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(history_lines(&env), 4);
    // Anything else is refused before the command runs.
    let out = env.run(&["exec", "--nested", "off", "--", "echo", "no"]);
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(stdout(&out), "");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("`admit` or `inherit`"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn front_jumps_the_queue() {
    let env = TestEnv::new().with_config("budget = 1\n");
    let dir = env.marker_dir();
    let first_gate = Gate::new(&dir, "first");
    let front_gate = Gate::new(&dir, "front");
    // Keep the resident and front entry alive until status observes both.
    // Front may already be running beyond the budget; normal must stay queued.
    let mut first = env.spawn_exec(&[], &gated_script(&dir, "first"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let mut normal = env.spawn_exec(&[], &marker_script(&dir, "normal", "0.2"));
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 2));
    let mut front = env.spawn_exec(&["--front"], &marked_gate(&dir, "front"));
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 3));
    let status = stdout(&env.run(&["status"]));
    first_gate.release();
    assert!(status.contains("[front]"), "{status}");
    assert!(wait_until(Duration::from_secs(5), || dir
        .join("front.start")
        .exists()));
    assert!(!dir.join("normal.start").exists());
    front_gate.release();
    for c in [&mut first, &mut normal, &mut front] {
        assert!(c.wait().unwrap().success());
    }
    assert!(read_ns(&dir.join("front.start")) < read_ns(&dir.join("normal.start")));
}

#[test]
fn front_waits_briefly_on_an_overloaded_machine() {
    // A factor this small makes any non-zero load average "overloaded".
    let env = TestEnv::new().with_config(
        "budget = 2\nmemory_reserve_mib = 0\nfront_overload_factor = 0.0001\nfront_overload_max_wait_secs = 2\n",
    );
    let dir = env.marker_dir();
    let first_gate = Gate::new(&dir, "first");
    let mut first = env.spawn_exec(&["--weight", "1"], &marked_gate(&dir, "first"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let queued = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let front = env.spawn_exec(
        &["--front", "--weight", "1"],
        &marker_script(&dir, "front", "0"),
    );
    let out = front.wait_with_output().unwrap();
    assert!(out.status.success());
    let waited = Duration::from_nanos(
        (read_ns(&dir.join("front.start")) - queued)
            .try_into()
            .unwrap(),
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("overloaded: load"), "{err}");
    assert!(waited >= Duration::from_millis(1900), "{waited:?}");
    assert!(!dir.join("first.released.start").exists());
    first_gate.release();
    assert!(first.wait().unwrap().success());
    // It gave up on the guard rather than waiting for `first` to finish.
    assert!(read_ns(&dir.join("front.start")) < read_ns(&dir.join("first.released.start")));
}

#[test]
fn killed_waiting_head_is_reaped_and_queue_moves_on() {
    let env = TestEnv::new().with_config("budget = 1\n");
    let dir = env.marker_dir();
    let run_gate = Gate::new(&dir, "run");
    let mut running = env.spawn_exec(&[], &gated_script(&dir, "run"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let mut doomed = env.spawn_exec(&[], &marker_script(&dir, "doomed", "5"));
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 2));
    let mut survivor = env.spawn_exec(&[], &marker_script(&dir, "survivor", "0.1"));
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 3));
    // SIGKILL the waiting head: no handler can run, only the flock drops.
    unsafe {
        libc::kill(doomed.id() as i32, libc::SIGKILL);
    }
    doomed.wait().unwrap();
    run_gate.release();
    assert!(running.wait().unwrap().success());
    // Measure queue recovery after the holder has stopped, separately from
    // the release marker polling and holder shutdown latency.
    let t = Instant::now();
    assert!(
        wait_until(Duration::from_secs(4), || survivor
            .try_wait()
            .unwrap()
            .is_some()),
        "survivor blocked by dead head"
    );
    assert!(survivor.wait().unwrap().success());
    assert!(
        t.elapsed() < Duration::from_secs(4),
        "survivor blocked by dead head"
    );
    assert!(!dir.join("doomed.start").exists());
    assert!(env.entries().is_empty());
    assert!(!env.state().join("leases").read_dir().unwrap().any(|_| true));
}

const BIN: &str = env!("CARGO_BIN_EXE_task-traffic-controller");

/// Write `body` as a shell script in `dir` and return the command line
/// that runs it, so nested command lines need no quoting.
fn script_file(dir: &Path, name: &str, body: &str) -> String {
    let p = dir.join(format!("{name}.sh"));
    fs::write(&p, body).unwrap();
    format!("sh {}", p.display())
}

fn history_lines(env: &TestEnv) -> usize {
    fs::read_to_string(env.state().join("history.jsonl"))
        .map(|h| h.lines().count())
        .unwrap_or(0)
}

/// The entries nested in another, as `status --json` shows them.
fn nested_entries(env: &TestEnv) -> Vec<serde_json::Value> {
    env.entries()
        .into_iter()
        .filter(|e| e["parent"].is_string())
        .collect()
}

#[test]
fn nested_exec_is_admitted_in_its_parent_and_bogus_lease_is_gated() {
    let env = TestEnv::new()
        .with_config("budget = 1\ncpu_threshold_percent = 100\nmemory_reserve_mib = 0\n");
    // Outer takes the whole budget; inner still runs, in the outer's share,
    // and so does one nested in the inner.
    let out = env.run(&[
        "exec",
        "--weight",
        "1",
        "--",
        "sh",
        "-c",
        &format!("{BIN} exec --weight 1 -- {BIN} exec --weight 1 -- echo nested-ok"),
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(stdout(&out), "nested-ok\n");
    // Each was a run of its own.
    assert_eq!(history_lines(&env), 3);
    assert!(env.entries().is_empty());

    // A stale lease id gates normally: with budget 1 busy, it waits.
    let dir = env.marker_dir();
    let mut hog = env.spawn_exec(&[], &marker_script(&dir, "hog", "1"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let mut stale = env
        .cmd()
        .env("TASK_TRAFFIC_CONTROLLER_LEASE", "deadbeef")
        .args([
            "exec",
            "--",
            "sh",
            "-c",
            &marker_script(&dir, "stale", "0.1"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 2));
    assert!(hog.wait().unwrap().success());
    assert!(stale.wait().unwrap().success());
    assert!(read_ns(&dir.join("stale.start")) >= read_ns(&dir.join("hog.end")));
}

#[test]
fn sigterm_is_forwarded_and_exit_mirrors_child() {
    let env = TestEnv::new();
    let dir = env.marker_dir();
    let pidfile = dir.join("child.pid");
    let mut child = env.spawn_exec(
        &[],
        &format!("echo $$ > {}; exec sleep 30", pidfile.display()),
    );
    assert!(wait_until(Duration::from_secs(5), || pidfile.exists()));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let child_pid: i32 = fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let status = child.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(libc::SIGTERM), "{status:?}");
    assert!(
        wait_until(Duration::from_secs(3), || unsafe {
            libc::kill(child_pid, 0) != 0
        }),
        "child sleep still alive"
    );
    assert!(env.entries().is_empty());
}

/// Hold the ledger lock from the test, as a holder stalled on a saturated
/// machine would, until the returned file is dropped.
fn hold_ledger_lock(env: &TestEnv) -> fs::File {
    use fs2::FileExt;
    fs::create_dir_all(env.state()).unwrap();
    let f = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(env.state().join("ledger.lock"))
        .unwrap();
    f.lock_exclusive().unwrap();
    f
}

/// `task-traffic-controller exec -- echo ran` giving up on the ledger lock after 200 ms.
fn spawn_impatient_exec(env: &TestEnv) -> Child {
    env.cmd()
        .env("TASK_TRAFFIC_CONTROLLER_LEDGER_LOCK_TIMEOUT_MS", "200")
        .args(["exec", "--", "echo", "ran"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn assert_ran_without_timeout(child: Child) {
    let out = child.wait_with_output().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{err}");
    assert!(!err.contains("timed out"), "{err}");
    assert_eq!(stdout(&out).trim(), "ran");
}

#[test]
fn waiting_exec_survives_a_busy_ledger_lock() {
    let env = TestEnv::new().with_config("budget = 1\n");
    let dir = env.marker_dir();
    let mut hog = env.spawn_exec(&[], &gated_script(&dir, "hog"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let waiter = spawn_impatient_exec(&env);
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 2));
    let lock = hold_ledger_lock(&env);
    thread::sleep(Duration::from_millis(1500));
    drop(lock);
    fs::write(dir.join("hog.release"), "").unwrap();
    assert!(hog.wait().unwrap().success());
    assert_ran_without_timeout(waiter);
}

#[test]
fn exec_registers_once_a_busy_ledger_lock_frees() {
    let env = TestEnv::new();
    let lock = hold_ledger_lock(&env);
    let exec = spawn_impatient_exec(&env);
    thread::sleep(Duration::from_millis(1000));
    drop(lock);
    assert_ran_without_timeout(exec);
}

#[test]
fn status_reads_the_ledger_while_the_lock_is_busy() {
    let env = TestEnv::new().with_config("budget = 1\n");
    let dir = env.marker_dir();
    let mut hog = env.spawn_exec(&[], &gated_script(&dir, "hog"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let lock = hold_ledger_lock(&env);
    let out = env
        .cmd()
        .env("TASK_TRAFFIC_CONTROLLER_LEDGER_LOCK_TIMEOUT_MS", "200")
        .args(["status", "--json"])
        .output()
        .unwrap();
    drop(lock);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let ledger: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(ledger["entries"].as_array().unwrap().len(), 1);
    fs::write(dir.join("hog.release"), "").unwrap();
    assert!(hog.wait().unwrap().success());
}

#[test]
fn sigint_while_waiting_removes_entry() {
    let env = TestEnv::new().with_config("budget = 1\n");
    let dir = env.marker_dir();
    let hog_gate = Gate::new(&dir, "hog");
    let mut hog = env.spawn_exec(&[], &gated_script(&dir, "hog"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let mut waiter = env.spawn_exec(&[], "echo never");
    assert!(wait_until(Duration::from_secs(5), || env.entries().len() == 2));
    unsafe {
        libc::kill(waiter.id() as i32, libc::SIGINT);
    }
    let status = waiter.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(libc::SIGINT));
    assert!(wait_until(Duration::from_secs(2), || env.entries().len() == 1));
    hog_gate.release();
    assert!(hog.wait().unwrap().success());
}

#[test]
fn progress_is_printed_while_waiting() {
    let env = TestEnv::new().with_config("budget = 1\nprogress_interval_secs = 1\n");
    let dir = env.marker_dir();
    let mut hog = env.spawn_exec(&[], &marker_script(&dir, "hog", "1.5"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let waiter = env.spawn_exec(&[], "true");
    let out = waiter.wait_with_output().unwrap();
    assert!(out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("task-traffic-controller: waiting"), "{err}");
    assert!(err.contains("running 1 (1/1 used)"), "{err}");
    assert!(hog.wait().unwrap().success());
}

/// Keep the budget occupied until the first progress line, then wait for a
/// repeated line or observe two one-second intervals with an unchanged reason.
fn progress_lines_behind_hog(config: &str, repeats: bool) -> usize {
    let env = TestEnv::new().with_config(config);
    let dir = env.marker_dir();
    let gate = Gate::new(&dir, "hog");
    let mut hog = env.spawn_exec(&[], &gated_script(&dir, "hog"));
    assert!(wait_until(Duration::from_secs(5), || {
        env.entries().iter().any(|e| e["state"] == "running")
    }));
    let log = dir.join("progress.log");
    let mut waiter = env
        .cmd()
        .args(["exec", "--", "true"])
        .stderr(fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let lines = || {
        fs::read_to_string(&log)
            .unwrap()
            .lines()
            .filter(|l| l.starts_with("task-traffic-controller: waiting"))
            .count()
    };
    assert!(wait_until(Duration::from_secs(5), || lines() >= 1));
    let observed = Instant::now();
    assert!(wait_until(Duration::from_secs(5), || {
        if repeats {
            lines() >= 2
        } else {
            observed.elapsed() >= Duration::from_secs(2)
        }
    }));
    let n = lines();
    gate.release();
    assert!(waiter.wait().unwrap().success());
    assert!(hog.wait().unwrap().success());
    n
}

#[test]
fn non_tty_progress_prints_only_on_change() {
    // The reason stays "budget" across two intervals: one line, not one per second.
    let n = progress_lines_behind_hog("budget = 1\nprogress_interval_secs = 1\n", false);
    assert_eq!(n, 1);
}

#[test]
fn non_tty_progress_interval_is_configurable() {
    let n = progress_lines_behind_hog(
        "budget = 1\nprogress_interval_secs = 60\nprogress_interval_secs_non_tty = 1\n",
        true,
    );
    assert!(n >= 2, "{n}");
}

#[test]
fn exec_one_quoted_argument_gets_a_hint() {
    let env = TestEnv::new();
    let out = env.run(&["exec", "--", "no-such-program-xyz run x"]);
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("hint:"), "{err}");
    assert!(err.contains("`-- no-such-program-xyz run x`"), "{err}");
    // A plain missing program gets no hint.
    let out = env.run(&["exec", "--", "no-such-program-xyz"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("hint:"));
}

#[test]
fn submit_one_quoted_argument_gets_a_hint() {
    let env = TestEnv::new();
    let out = env.run(&["submit", "--", "no-such-program-xyz run x"]);
    assert!(out.status.success());
    let id = stdout(&out).trim().to_string();
    let out = env.run(&["wait", &id]);
    assert_eq!(out.status.code(), Some(2));
    let log = stdout(&out);
    assert!(log.contains("hint:"), "{log}");
    assert!(log.contains("`-- no-such-program-xyz run x`"), "{log}");
}

#[test]
fn fixed_weight_pattern_overrides_history() {
    let env = TestEnv::new().with_config(
        "budget = 4\n[[pattern]]\nmatch = \"*lint*\"\nweight = 1\n[[pattern]]\nmatch = \"*heavy*\"\nmin_weight = 3\n",
    );
    env.run(&["exec", "--", "sh", "-c", "true # lint"]);
    env.run(&["exec", "--", "sh", "-c", "true # heavy"]);
    let hist = fs::read_to_string(env.state().join("history.jsonl")).unwrap();
    let recs: Vec<serde_json::Value> = hist
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(recs[0]["weight_used"], 1);
    assert_eq!(recs[1]["weight_used"], 3);
}

#[test]
fn weight_flag_is_recorded() {
    let env = TestEnv::new();
    env.run(&["exec", "--weight", "3", "--", "true"]);
    let hist = fs::read_to_string(env.state().join("history.jsonl")).unwrap();
    let rec: serde_json::Value = serde_json::from_str(hist.lines().next().unwrap()).unwrap();
    assert_eq!(rec["weight_used"], 3);
}

#[test]
fn key_override_groups_history() {
    let env = TestEnv::new();
    env.run(&["exec", "--key", "e2e", "--", "sh", "-c", "true 1"]);
    env.run(&["exec", "--key", "e2e", "--", "sh", "-c", "true 2"]);
    let out = env.run(&["history", "--key", "e2e", "--json"]);
    let v: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0]["key"], v[1]["key"]);
}

/// A stand-in `npx` on PATH so the heavy-command rules can be exercised
/// without Node.
fn with_fake_npx(env: &TestEnv, c: &mut Command) {
    use std::os::unix::fs::PermissionsExt;
    let bin = env.dir.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let npx = bin.join("npx");
    fs::write(&npx, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&npx, fs::Permissions::from_mode(0o755)).unwrap();
    let path = std::env::var("PATH").unwrap_or_default();
    c.env("PATH", format!("{}:{path}", bin.display()));
}

#[test]
fn wrapped_commands_share_the_key_of_their_heavy_command() {
    let env = TestEnv::new();
    let run = |args: &[&str]| {
        let mut c = env.cmd();
        with_fake_npx(&env, &mut c);
        let out = c.args(args).output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["exec", "--", "npx", "tool"]);
    run(&[
        "exec",
        "--",
        "sh",
        "-c",
        "npx tool > out.log 2>&1; echo done",
    ]);
    run(&["exec", "--", "bash", "-c", "cd . && npx tool | cat"]);
    // Scripts in a temporary directory (the test's own temp dir) are read.
    for name in ["one.sh", "two.sh"] {
        let p = env.dir.path().join(name);
        fs::write(&p, "#!/bin/sh\nset -e\nnpx tool\n").unwrap();
        run(&["exec", "--", "sh", p.to_str().unwrap()]);
    }

    let out = env.run(&["history", "--json", "--", "npx", "tool"]);
    let v: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v.len(), 5, "{v:?}");
    assert!(v.iter().all(|r| r["key"] == v[0]["key"]));
    assert!(v[0].get("key_argv").is_none(), "{}", v[0]);
    assert_eq!(v[1]["key_argv"], "npx tool");
    assert_eq!(v[1]["argv"], "sh -c npx tool > out.log 2>&1; echo done");

    let text = stdout(&env.run(&["history"]));
    assert!(text.contains("key: npx tool"), "{text}");
}

#[test]
fn test_runners_and_mutation_tools_start_heavier() {
    let env = TestEnv::new()
        .with_config("budget = 8\n[[pattern]]\nmatch = \"*jest*\"\ninitial_weight = 3\n");
    let first_weight = |args: &[&str]| {
        let mut c = env.cmd();
        with_fake_npx(&env, &mut c);
        let out = c.arg("exec").arg("--").args(args).output().unwrap();
        assert!(out.status.success());
        let hist = fs::read_to_string(env.state().join("history.jsonl")).unwrap();
        let last: serde_json::Value = serde_json::from_str(hist.lines().last().unwrap()).unwrap();
        last["weight_used"].as_u64().unwrap()
    };
    assert_eq!(first_weight(&["npx", "vitest", "run"]), 4);
    assert_eq!(
        first_weight(&["sh", "-c", "npx stryker run > /dev/null"]),
        6
    );
    assert_eq!(first_weight(&["npx", "jest"]), 3, "a pattern overrides");
    assert_eq!(first_weight(&["npx", "tool"]), 2, "default_weight");
}

#[test]
fn shell_string_without_a_heavy_command_keeps_its_own_key() {
    let env = TestEnv::new();
    env.run(&["exec", "--", "sh", "-c", "true 1"]);
    env.run(&["exec", "--", "sh", "-c", "true 2"]);
    let out = env.run(&["history", "--json"]);
    let v: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v.len(), 2);
    assert_ne!(v[0]["key"], v[1]["key"]);
}

#[test]
fn worktrees_share_a_key() {
    let env = TestEnv::new();
    let repo = env.dir.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str], cwd: &Path| {
        let o = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@example.com")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@example.com")
            .output()
            .unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    };
    git(&["init", "-q", "-b", "main"], &repo);
    git(&["commit", "-q", "--allow-empty", "-m", "init"], &repo);
    let wt = env.dir.path().join("wt");
    git(&["worktree", "add", "-q", wt.to_str().unwrap()], &repo);
    env.cmd()
        .current_dir(&repo)
        .args(["exec", "--", "true"])
        .output()
        .unwrap();
    env.cmd()
        .current_dir(&wt)
        .args(["exec", "--", "true"])
        .output()
        .unwrap();
    let hist = fs::read_to_string(env.state().join("history.jsonl")).unwrap();
    let recs: Vec<serde_json::Value> = hist
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0]["key"], recs[1]["key"]);
    assert_eq!(recs[0]["repo"], recs[1]["repo"]);
}

#[test]
fn an_entry_records_the_worktree_and_branch_it_was_queued_from() {
    let env = TestEnv::new();
    let repo = env.dir.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str], cwd: &Path| {
        let o = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@example.com")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@example.com")
            .output()
            .unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    };
    git(&["init", "-q", "-b", "main"], &repo);
    git(&["commit", "-q", "--allow-empty", "-m", "init"], &repo);
    let wt = env.dir.path().join("wt");
    git(
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "feat/x",
            wt.to_str().unwrap(),
        ],
        &repo,
    );
    let sub = wt.join("sub");
    fs::create_dir_all(&sub).unwrap();
    let spawn = |cwd: &Path| {
        env.cmd()
            .current_dir(cwd)
            .args(["exec", "--", "sleep", "30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let mut a = spawn(&repo);
    let mut b = spawn(&sub);
    assert!(wait_until(Duration::from_secs(20), || env.entries().len() == 2));
    let entries = env.entries();
    let _ = a.kill();
    let _ = b.kill();
    let _ = a.wait();
    let _ = b.wait();
    let canon = |p: &Path| fs::canonicalize(p).unwrap().to_string_lossy().into_owned();
    let of = |branch: &str| {
        entries
            .iter()
            .find(|e| e["branch"] == branch)
            .unwrap_or_else(|| panic!("no entry for {branch}: {entries:?}"))
            .clone()
    };
    let (main_e, wt_e) = (of("main"), of("feat/x"));
    assert_eq!(main_e["worktree"], canon(&repo));
    assert_eq!(wt_e["worktree"], canon(&wt));
    // Both belong to the same repository: the history key still collapses.
    assert_eq!(main_e["repo"], wt_e["repo"]);
    assert_eq!(main_e["repo"], canon(&repo));
}

#[test]
fn subprojects_of_one_repo_learn_separately() {
    let env = TestEnv::new();
    let repo = env.dir.path().join("mono");
    fs::create_dir_all(repo.join("api")).unwrap();
    fs::create_dir_all(repo.join("client")).unwrap();
    let git = |args: &[&str]| {
        let o = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .output()
            .unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    };
    git(&["init", "-q", "-b", "main"]);
    env.cmd()
        .current_dir(repo.join("api"))
        .args(["exec", "--", "true"])
        .output()
        .unwrap();
    env.cmd()
        .current_dir(repo.join("client"))
        .args(["exec", "--", "true"])
        .output()
        .unwrap();
    env.cmd()
        .current_dir(&repo)
        .args(["exec", "--", "true"])
        .output()
        .unwrap();
    let hist = fs::read_to_string(env.state().join("history.jsonl")).unwrap();
    let recs: Vec<serde_json::Value> = hist
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(recs.len(), 3);
    assert_ne!(recs[0]["key"], recs[1]["key"]);
    assert_ne!(recs[1]["key"], recs[2]["key"]);
    assert_eq!(recs[0]["subdir"], "api");
    assert_eq!(recs[2]["subdir"], "");
    let out = env
        .cmd()
        .current_dir(repo.join("api"))
        .args(["history"])
        .output()
        .unwrap();
    assert!(stdout(&out).contains("mono/api: true"), "{}", stdout(&out));
}

#[test]
fn config_and_gc_commands() {
    let env = TestEnv::new().with_config("budget = 3\nhistory_keep = 1\n");
    let out = env.run(&["config"]);
    assert!(stdout(&out).contains("budget = 3"));
    let out = env.run(&["config", "--path"]);
    assert!(stdout(&out)
        .trim()
        .ends_with("config/task-traffic-controller/config.toml"));
    env.run(&["exec", "--", "true"]);
    env.run(&["exec", "--", "true"]);
    let out = env.run(&["gc"]);
    assert!(
        stdout(&out).contains("trimmed 1 history records"),
        "{}",
        stdout(&out)
    );
}

#[test]
fn invalid_config_is_reported() {
    let env = TestEnv::new().with_config("budget = 0\n");
    let out = env.run(&["status"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("budget"));
}
