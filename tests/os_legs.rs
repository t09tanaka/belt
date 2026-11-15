//! A matrix over `runs-on` OS labels, on a host that is one of them: the leg of
//! the host's OS runs, the others are reported as not covered.

#![allow(clippy::disallowed_methods)] // the tests build the binary's environment

mod common;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

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

struct Repo {
    _tmp: common::Tmp,
    repo: PathBuf,
    home: PathBuf,
}

/// A repository with one workflow, `ci.yml`, on `main`, and a `feature` branch
/// that changes a file.
fn repo(workflow: &str) -> Repo {
    let tmp = common::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(repo.join(".github/workflows")).unwrap();
    std::fs::write(repo.join(".github/workflows/ci.yml"), workflow).unwrap();
    std::fs::write(repo.join("a.txt"), "a\n").unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    git(&repo, &["checkout", "-qb", "feature"]);
    std::fs::write(repo.join("a.txt"), "b\n").unwrap();
    git(&repo, &["commit", "-qam", "change"]);
    let home = tmp.path().join("home");
    Repo {
        _tmp: tmp,
        repo,
        home,
    }
}

fn run(r: &Repo, extra: &[&str], env: &[(&str, &str)]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_local-ci"));
    c.args([
        "run",
        "--no-status",
        "--no-traffic-controller",
        "--base",
        "main",
    ])
    .args(extra)
    .current_dir(&r.repo)
    .env("LOCAL_CI_HOME", &r.home);
    for (k, v) in env {
        c.env(k, v);
    }
    c.output().unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

/// `(label of the host's OS, name of the host's OS, label of another OS, name of it)`
fn labels() -> (&'static str, &'static str, &'static str, &'static str) {
    if cfg!(target_os = "macos") {
        ("macos-latest", "macOS", "ubuntu-latest", "Linux")
    } else {
        ("ubuntu-latest", "Linux", "macos-latest", "macOS")
    }
}

fn matrix_workflow(steps: &str) -> String {
    let (host, _, other, _) = labels();
    format!(
        "name: CI\non: push\njobs:\n  test:\n    runs-on: ${{{{ matrix.os }}}}\n    strategy:\n      matrix:\n        os: [{host}, {other}]\n    steps:\n{steps}"
    )
}

fn json(o: &Output) -> serde_json::Value {
    serde_json::from_slice(&o.stdout).unwrap_or_else(|e| panic!("{e}: {}", stdout(o)))
}

fn job<'a>(v: &'a serde_json::Value, display: &str) -> &'a serde_json::Value {
    v["units"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|u| u["jobs"].as_array().unwrap())
        .find(|j| j["display"] == display)
        .unwrap_or_else(|| panic!("no job {display} in {v}"))
}

#[test]
fn the_leg_of_the_other_os_is_not_run_and_not_a_pass() {
    let (host, host_name, other, other_name) = labels();
    let r = repo(&matrix_workflow("      - run: echo hi\n"));
    let o = run(&r, &[], &[]);
    let out = stdout(&o);
    assert!(
        out.contains(&format!("PASS   ci.yml › test ({host})")),
        "{out}"
    );
    let why = format!(
        "runs-on {other}: {other_name} is not reproduced on this {host_name} host; the same steps ran as test ({host})"
    );
    assert!(out.contains(&format!("test ({other}) ({why})")), "{out}");
    assert!(
        out.contains(&format!(
            "not covered (so this is not a pass): ci.yml › test ({other}): {why}"
        )),
        "{out}"
    );
    assert!(out.contains("result: incomplete"), "{out}");
    // not a failure either
    assert!(out.contains("fail 0 / error 0"), "{out}");
    assert_eq!(o.status.code(), Some(3), "{out}");

    let o = run(&r, &["--json"], &[]);
    let v = json(&o);
    assert_eq!(v["state"], "incomplete");
    assert_eq!(job(&v, &format!("test ({host})"))["result"], "pass");
    let skipped = job(&v, &format!("test ({other})"));
    assert_eq!(skipped["result"], "skip");
    assert_eq!(skipped["reason"], why);
    assert!(v["not_covered"][0].as_str().unwrap().contains(&why));
    // the host's own leg has nothing to note
    assert!(job(&v, &format!("test ({host})")).get("os_note").is_none());
}

#[test]
fn all_os_legs_runs_every_leg_with_a_note() {
    let (host, _, other, other_name) = labels();
    let host_name = labels().1;
    let r = repo(&matrix_workflow("      - run: echo hi\n"));
    for (args, env) in [
        (vec!["--all-os-legs"], vec![]),
        (vec![], vec![("LOCAL_CI_ALL_OS_LEGS", "1")]),
    ] {
        let o = run(&r, &args, &env);
        let out = stdout(&o);
        assert_eq!(o.status.code(), Some(0), "{out}");
        assert!(
            out.contains(&format!("PASS   ci.yml › test ({host})")),
            "{out}"
        );
        let note = format!("ran on {host_name}, not {other_name} (runs-on {other})");
        assert!(
            out.contains(&format!("PASS   ci.yml › test ({other})"))
                && out.contains(&format!("[{note}]")),
            "{out}"
        );
        assert!(out.contains("result: success"), "{out}");
    }
    // a result of the other kind of run is not reused
    let o = run(&r, &[], &[]);
    assert_eq!(o.status.code(), Some(3), "{}", stdout(&o));
}

#[test]
fn a_lone_job_of_another_os_runs_with_a_note() {
    let (_, host_name, other, other_name) = labels();
    let wf = format!(
        "name: CI\non: push\njobs:\n  lone:\n    runs-on: {other}\n    steps:\n      - run: echo hi\n"
    );
    let r = repo(&wf);
    let o = run(&r, &[], &[]);
    let out = stdout(&o);
    let note = format!("ran on {host_name}, not {other_name} (runs-on {other})");
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("PASS   ci.yml › lone"), "{out}");
    assert!(out.contains(&format!("[{note}]")), "{out}");
    let v = json(&run(&r, &["--json", "--no-cache"], &[]));
    assert_eq!(job(&v, "lone")["os_note"], note);
    assert_eq!(v["state"], "success");
}

#[test]
fn legs_that_read_the_os_are_all_run() {
    let (host, _, other, _) = labels();
    let r = repo(&matrix_workflow("      - run: echo ${{ matrix.os }}\n"));
    let o = run(&r, &[], &[]);
    let out = stdout(&o);
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(
        out.contains(&format!("PASS   ci.yml › test ({host})")),
        "{out}"
    );
    assert!(
        out.contains(&format!("PASS   ci.yml › test ({other})")),
        "{out}"
    );
    // the leg of the other OS ran on the host, and says so
    let (_, host_name, _, other_name) = labels();
    assert!(
        out.contains(&format!(
            "[ran on {host_name}, not {other_name} (runs-on {other})]"
        )),
        "{out}"
    );
}

#[test]
fn a_job_that_needs_a_matrix_gets_the_outputs_of_the_leg_that_ran() {
    let (host, _, other, _) = labels();
    let wf = format!(
        "name: CI\non: push\njobs:\n  build:\n    runs-on: ${{{{ matrix.os }}}}\n    strategy:\n      matrix:\n        os: [{other}, {host}]\n    outputs:\n      v: ${{{{ steps.s.outputs.v }}}}\n    steps:\n      - id: s\n        run: echo \"v=42\" >> \"$GITHUB_OUTPUT\"\n  use:\n    needs: build\n    runs-on: {host}\n    steps:\n      - run: test \"${{{{ needs.build.outputs.v }}}}\" = 42\n      - run: test \"${{{{ needs.build.result }}}}\" = success\n"
    );
    let r = repo(&wf);
    let o = run(&r, &[], &[]);
    let out = stdout(&o);
    assert!(
        out.contains(&format!("PASS   ci.yml › build ({host})")),
        "{out}"
    );
    assert!(out.contains("PASS   ci.yml › use"), "{out}");
    assert!(out.contains("fail 0 / error 0"), "{out}");
    // the left-out leg keeps the run from being a pass, and nothing else does
    assert!(out.contains(&format!("build ({other})")), "{out}");
    assert_eq!(o.status.code(), Some(3), "{out}");
}
