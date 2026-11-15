//! The build cache, end to end: a Rust job runs twice, and the second run
//! gets the dependencies it built the first time without building them again.

#![allow(clippy::disallowed_methods)] // the tests build their own commands

mod common;
use local_ci::{ExecContext, Quiet, RunOptions, Target};
use std::path::{Path, PathBuf};
use std::process::Command;

fn git(dir: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .args([
            "-c",
            "user.email=t@example.com",
            "-c",
            "user.name=t",
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

/// The variables a job needs to find cargo, as the test process has them.
fn ctx(log: &Path) -> ExecContext {
    let mut env: std::collections::BTreeMap<String, String> =
        ["PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME"]
            .into_iter()
            .filter_map(|k| Some((k.to_string(), std::env::var(k).ok()?)))
            .collect();
    env.insert("FX_LOG".into(), log.to_string_lossy().into_owned());
    ExecContext::new(env)
}

struct Fixture {
    tmp: common::Tmp,
    repo: PathBuf,
    log: PathBuf,
}

/// A repository with a crate that depends on a crate outside it (as a
/// dependency from the registry is: the same path in every worktree), and a
/// job that builds it, writing what cargo says to `$FX_LOG`.
fn fixture() -> Fixture {
    let tmp = common::tempdir().unwrap();
    let dep = tmp.path().join("dep");
    std::fs::create_dir_all(dep.join("src")).unwrap();
    std::fs::write(
        dep.join("Cargo.toml"),
        "[package]\nname = \"fxdep\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    std::fs::write(dep.join("src/lib.rs"), "pub fn n() -> u32 { 1 }\n").unwrap();
    let repo = tmp.path().join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::create_dir_all(repo.join(".github/workflows")).unwrap();
    std::fs::write(
        repo.join("Cargo.toml"),
        format!(
            "[package]\nname = \"fx\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nfxdep = {{ path = \"{}\" }}\n",
            dep.display()
        ),
    )
    .unwrap();
    std::fs::write(
        repo.join("src/main.rs"),
        "fn main() { println!(\"{}\", fxdep::n()); }\n",
    )
    .unwrap();
    std::fs::write(repo.join(".gitignore"), "target\n").unwrap();
    std::fs::write(
        repo.join(".github/workflows/ci.yml"),
        "name: ci\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: cargo build -v --offline 2> \"$FX_LOG\"\n",
    )
    .unwrap();
    std::fs::write(repo.join("a.txt"), "a\n").unwrap();
    let lock = Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .current_dir(&repo)
        .output()
        .expect("cargo is needed for this test");
    assert!(lock.status.success(), "{lock:?}");
    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "base"]);
    git(&repo, &["checkout", "-qb", "feature"]);
    std::fs::write(repo.join("a.txt"), "b\n").unwrap();
    git(&repo, &["commit", "-qam", "change"]);
    let log = tmp.path().join("cargo.log");
    Fixture { tmp, repo, log }
}

impl Fixture {
    /// One run of the job with `cache` as the cache directory; what cargo said.
    fn run(
        &self,
        cache: &Path,
        no_build_cache: bool,
    ) -> (String, Option<local_ci::build_cache::JobCache>) {
        let _ = std::fs::remove_file(&self.log);
        let rep = local_ci::run(
            &ctx(&self.log),
            RunOptions {
                target: Target {
                    base: Some("main".into()),
                    sha: "HEAD".into(),
                    all: false,
                    include_disabled: true,
                    workflows_dirs: vec![],
                    workflows: vec![],
                    jobs: vec![],
                    dir: self.repo.clone(),
                },
                parallel: Some(1),
                no_traffic_controller: true,
                no_portool: true,
                containers: 1,
                keep: false,
                job_timeout: 5.0,
                // the result of the last run must not stand in for this one
                no_cache: true,
                no_build_cache,
                cache_dir: cache.to_path_buf(),
                source: None,
            },
            &mut Quiet,
        )
        .unwrap();
        let jobs: Vec<_> = rep.result.units.iter().flat_map(|u| &u.jobs).collect();
        assert_eq!(jobs.len(), 1);
        assert!(
            matches!(jobs[0].outcome, local_ci::exec::Outcome::Pass),
            "{:?}",
            jobs[0]
        );
        let said = std::fs::read_to_string(&self.log).unwrap_or_default();
        (said, jobs[0].build_cache.clone())
    }
}

#[test]
fn the_second_run_restores_what_the_first_built() {
    let f = fixture();
    let cache = f.tmp.path().join("cache");
    let (first, stat) = f.run(&cache, false);
    assert!(first.contains("Compiling fxdep"), "{first}");
    let stat = stat.expect("a Rust job has a build cache record");
    assert!(stat.saved && !stat.restored, "{stat:?}");
    let entries = local_ci::build_cache::entries(&cache.join("build"));
    assert_eq!(entries.len(), 1);
    // nothing the entry should not hold
    assert!(!entries[0].key.contains(".tmp-"));

    let (second, stat) = f.run(&cache, false);
    assert!(second.contains("Fresh fxdep"), "{second}");
    assert!(!second.contains("Compiling fxdep"), "{second}");
    let stat = stat.unwrap();
    assert!(stat.restored && !stat.saved, "{stat:?}");
    assert_eq!(
        local_ci::build_cache::entries(&cache.join("build")).len(),
        1
    );
    // incremental data is not kept
    let entry = cache.join("build").join(&entries[0].key).join("target");
    assert!(entry.is_dir());
    assert!(!entry.join("debug/incremental").exists());
}

#[test]
fn without_the_build_cache_nothing_is_kept_or_restored() {
    let f = fixture();
    let cache = f.tmp.path().join("cache");
    let (first, stat) = f.run(&cache, true);
    assert!(first.contains("Compiling fxdep"), "{first}");
    assert!(stat.is_none());
    let (second, stat) = f.run(&cache, true);
    assert!(second.contains("Compiling fxdep"), "{second}");
    assert!(stat.is_none());
    assert!(!cache.join("build").exists(), "left behind");
}
