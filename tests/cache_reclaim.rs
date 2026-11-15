//! What a run leaves in the cache directory on each way out: a normal end, a
//! failing job, a signal it handles, and a kill it cannot handle (the next
//! run reclaims that). Everything is under a temporary `LOCAL_CI_HOME`.

#![allow(clippy::disallowed_methods)] // the tests build the binary's environment

mod common;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(["-c", "user.email=demo@example.com", "-c", "user.name=demo"])
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?}");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

struct Fixture {
    tmp: common::Tmp,
    repo: PathBuf,
    home: PathBuf,
}

/// A repository whose `feature` branch triggers two workflows, each running
/// `step` (one in a matrix): two units, so each runs in a copy of the
/// worktree (`<sha>-u<n>-<pid>`). The step comes after one that differs
/// between the jobs, so it is not taken for their common setup, which runs
/// once in the worktree itself.
fn fixture(step: &str) -> Fixture {
    let tmp = common::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(repo.join(".github/workflows")).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(
        repo.join(".github/workflows/ci.yml"),
        format!(
            "name: CI\non: pull_request\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: echo a\n      - run: |\n          {step}\n"
        ),
    )
    .unwrap();
    std::fs::write(
        repo.join(".github/workflows/other.yml"),
        format!(
            "name: Other\non: pull_request\njobs:\n  b:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        n: [1, 2]\n    steps:\n      - uses: actions/checkout@v4\n      - run: echo b\n      - run: |\n          {step}\n"
        ),
    )
    .unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    git(&repo, &["checkout", "-qb", "feature"]);
    std::fs::write(repo.join("f"), "x").unwrap();
    git(&repo, &["add", "f"]);
    git(&repo, &["commit", "-qm", "change"]);
    let home = tmp.path().join("home");
    Fixture { tmp, repo, home }
}

impl Fixture {
    fn command(&self, args: &[&str]) -> Command {
        let path = common::private_docker_path(&self.home, &std::env::var("PATH").unwrap());
        let mut c = Command::new(env!("CARGO_BIN_EXE_local-ci"));
        c.args(args)
            .current_dir(&self.repo)
            .env("LOCAL_CI_HOME", &self.home)
            .env("PATH", path)
            .stdin(Stdio::null());
        c
    }

    fn run(&self) -> Command {
        self.command(&[
            "run",
            "--base",
            "main",
            "--no-status",
            "--no-traffic-controller",
            "--no-portool",
        ])
    }

    fn output(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    /// The worktrees, copies and tools directories in the cache.
    fn entries(&self) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(self.home.join("wt"))
            .into_iter()
            .flatten()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| !n.starts_with('.'))
            .collect();
        v.sort();
        v
    }

    fn owner_files(&self) -> usize {
        std::fs::read_dir(self.home.join("wt"))
            .into_iter()
            .flatten()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".owner-")
            })
            .count()
    }

    fn registered_worktrees(&self) -> usize {
        git(&self.repo, &["worktree", "list"]).lines().count()
    }

    fn assert_nothing_left(&self) {
        assert_eq!(self.entries(), Vec::<String>::new());
        assert_eq!(self.owner_files(), 0);
        assert_eq!(self.registered_worktrees(), 1);
    }

    fn gate(&self) -> PathBuf {
        self.tmp.path().join("gate")
    }

    /// Start a run whose steps wait at the gate, and return once one waits.
    fn start_blocked(&self) -> Child {
        std::fs::write(self.gate(), "").unwrap();
        let child = self
            .run()
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(120);
        // every unit is in its step: no copy is still being made
        while !["a", "b"]
            .iter()
            .all(|j| self.tmp.path().join(format!("started-{j}")).exists())
        {
            assert!(Instant::now() < until, "no step started");
            std::thread::sleep(Duration::from_millis(20));
        }
        // a step runs in the copy of its unit
        let during = self.entries();
        assert!(during.iter().any(|n| n.contains("-u")), "{during:?}");
        assert!(during.iter().any(|n| n.ends_with(".tools")), "{during:?}");
        assert!(self.registered_worktrees() >= 3, "{during:?}");
        child
    }

    /// Let the steps that wait at the gate go on.
    fn open_gate(&self) {
        std::fs::remove_file(self.gate()).unwrap();
    }
}

/// A step that waits for as long as the gate file is there (a minute at
/// most: a step orphaned by a killed run ends by itself).
fn gated(f: &Path) -> String {
    format!(
        "touch '{0}/started-'$GITHUB_JOB; n=0; while [ -e '{0}/gate' ] && [ $n -lt 600 ]; do sleep 0.1; n=$((n+1)); done",
        f.display()
    )
}

fn gated_fixture() -> Fixture {
    // the step needs the directory of the fixture, known only once it is made
    let f = fixture("STEP");
    for wf in ["ci.yml", "other.yml"] {
        let wf = f.repo.join(".github/workflows").join(wf);
        let text = std::fs::read_to_string(&wf)
            .unwrap()
            .replace("STEP", &gated(f.tmp.path()));
        std::fs::write(&wf, text).unwrap();
    }
    git(&f.repo, &["commit", "-qam", "gated steps"]);
    f
}

/// Signal a run this test started. Only ever the pid of that child.
fn signal(child: &Child, sig: i32) {
    let pid = child.id();
    assert!(pid > 1 && pid <= i32::MAX as u32);
    // SAFETY: the child is ours and not reaped yet, so the pid is its own.
    unsafe {
        libc::kill(pid as i32, sig);
    }
}

#[test]
fn a_run_that_passes_leaves_nothing() {
    let f = fixture("true");
    let o = f.run().output().unwrap();
    assert_eq!(o.status.code(), Some(0), "{o:?}");
    f.assert_nothing_left();
}

#[test]
fn a_run_with_a_failing_job_leaves_nothing() {
    let f = fixture("exit 1");
    let o = f.run().output().unwrap();
    assert_eq!(o.status.code(), Some(1), "{o:?}");
    f.assert_nothing_left();
}

#[test]
fn an_interrupted_run_leaves_nothing() {
    for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP] {
        let f = gated_fixture();
        let mut child = f.start_blocked();
        assert!(!f.entries().is_empty());
        signal(&child, sig);
        let status = child.wait().unwrap();
        assert_eq!(status.code(), Some(130), "signal {sig}");
        f.assert_nothing_left();
    }
}

#[test]
fn what_a_killed_run_left_is_reclaimed_by_the_next_run_and_not_while_it_lives() {
    let f = gated_fixture();
    let mut child = f.start_blocked();
    let during = f.entries();
    assert!(!during.is_empty());

    // another terminal, while the run is alive: nothing of it is touched
    let o = f.output(&["cache", "prune", "--kept"]);
    assert!(o.status.success(), "{o:?}");
    assert!(
        String::from_utf8_lossy(&o.stdout).starts_with("removed 0 worktree entries"),
        "{o:?}"
    );
    assert_eq!(f.entries(), during);

    // SIGKILL: the run cannot clean up
    child.kill().unwrap();
    child.wait().unwrap();
    f.open_gate();
    let left = f.entries();
    assert!(left.len() >= during.len(), "{left:?}");
    assert!(f.registered_worktrees() > 1);
    let o = f.output(&["cache", "status"]);
    let out = String::from_utf8_lossy(&o.stdout).into_owned();
    assert!(
        out.contains(&format!("{} left by runs that are gone", left.len())),
        "{out}"
    );

    // the next run of the same commit removes them before it starts
    let o = f.run().output().unwrap();
    assert_eq!(o.status.code(), Some(0), "{o:?}");
    f.assert_nothing_left();
}

#[test]
fn keep_keeps_the_worktrees_until_they_are_pruned() {
    let f = fixture("true");
    let o = f.run().arg("--keep").output().unwrap();
    assert_eq!(o.status.code(), Some(0), "{o:?}");
    let kept = f.entries();
    assert!(!kept.is_empty());

    // neither a later run nor a plain prune removes them
    let o = f.run().arg("--no-cache").output().unwrap();
    assert_eq!(o.status.code(), Some(0), "{o:?}");
    assert!(f.output(&["cache", "prune"]).status.success());
    assert_eq!(f.entries(), kept);

    assert!(f.output(&["cache", "prune", "--kept"]).status.success());
    f.assert_nothing_left();
}
