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
    fixture_with(BUILD_WORKFLOW)
}

/// The default job: a step that says whether a leftover of the last job is
/// there, the build, and a step that leaves a file that is not cargo's.
const BUILD_WORKFLOW: &str = "name: ci\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: if [ -e target/tmp/x ]; then echo leftover > \"$FX_LOG.left\"; fi\n      - run: cargo build -v --offline 2> \"$FX_LOG\"\n      - run: mkdir -p target/tmp target/nextest && echo x > target/tmp/x && echo y > target/nextest/y\n";

fn fixture_with(workflow: &str) -> Fixture {
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
    std::fs::write(repo.join(".github/workflows/ci.yml"), workflow).unwrap();
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
    /// Commit `content` as `path` of the repository.
    fn commit(&self, path: &str, content: &str) {
        let p = self.repo.join(path);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, content).unwrap();
        git(&self.repo, &["add", path]);
        git(&self.repo, &["commit", "-qm", path]);
    }

    /// One run of the job; its result.
    fn exec(
        &self,
        ctx: &ExecContext,
        cache: &Path,
        no_build_cache: bool,
    ) -> local_ci::exec::JobResult {
        let _ = std::fs::remove_file(&self.log);
        let _ = std::fs::remove_file(self.log.with_extension("log.left"));
        let rep = local_ci::run(
            ctx,
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
        let jobs: Vec<_> = rep.result.units.into_iter().flat_map(|u| u.jobs).collect();
        assert_eq!(jobs.len(), 1);
        let job = jobs.into_iter().next().unwrap();
        assert!(
            matches!(job.outcome, local_ci::exec::Outcome::Pass),
            "{job:?}"
        );
        job
    }

    /// One run of the job with `cache` as the cache directory; what cargo said.
    fn run(
        &self,
        cache: &Path,
        no_build_cache: bool,
    ) -> (String, Option<local_ci::build_cache::JobCache>) {
        let job = self.exec(&ctx(&self.log), cache, no_build_cache);
        let said = std::fs::read_to_string(&self.log).unwrap_or_default();
        (said, job.build_cache)
    }

    /// Whether the last run found target/tmp/x of an earlier job.
    fn found_leftover(&self) -> bool {
        self.log.with_extension("log.left").exists()
    }
}

fn entry_keys(cache: &Path) -> Vec<String> {
    local_ci::build_cache::entries(&cache.join("build"))
        .into_iter()
        .map(|e| e.key)
        .collect()
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

/// Run twice with `change` between them: the key is new the second time.
fn changing_misses(f: &Fixture, change: impl FnOnce(&Fixture)) {
    let cache = f.tmp.path().join("cache");
    let (_, first) = f.run(&cache, false);
    let first = first.unwrap();
    assert!(first.saved, "{first:?}");
    change(f);
    let (said, second) = f.run(&cache, false);
    let second = second.unwrap();
    assert_ne!(first.key, second.key);
    assert!(!second.restored && second.saved, "{second:?}");
    assert!(said.contains("Compiling fxdep"), "{said}");
    assert_eq!(entry_keys(&cache).len(), 2);
}

#[test]
fn a_change_of_the_cargo_config_is_a_miss() {
    let f = fixture();
    changing_misses(&f, |f| {
        f.commit(".cargo/config.toml", "[term]\nverbose = false\n")
    });
}

#[test]
fn a_change_of_the_toolchain_file_is_a_miss() {
    let f = fixture();
    // the toolchain that is in use, so that the file changes nothing else
    let active = Command::new("rustup")
        .args(["show", "active-toolchain"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|t| t.split_whitespace().next().map(str::to_string))
        .unwrap_or_else(|| "stable".into());
    changing_misses(&f, |f| {
        f.commit(
            "rust-toolchain.toml",
            &format!("[toolchain]\nchannel = \"{active}\"\n"),
        )
    });
}

const MATRIX_WORKFLOW: &str = "name: ci\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        v: [VALUE]\n    steps:\n      - uses: actions/checkout@v4\n      - run: cargo build -v --offline 2> \"$FX_LOG\"\n";

#[test]
fn a_change_of_the_matrix_values_is_a_miss() {
    // nothing but the matrix changes: no variable, no step reads it
    let f = fixture_with(&MATRIX_WORKFLOW.replace("VALUE", "1"));
    changing_misses(&f, |f| {
        f.commit(
            ".github/workflows/ci.yml",
            &MATRIX_WORKFLOW.replace("VALUE", "2"),
        )
    });
}

#[test]
fn the_evaluated_environment_is_in_the_key_not_the_text() {
    let wf = BUILD_WORKFLOW.replace(
        "    steps:",
        "    env:\n      CARGO_TERM_PROGRESS_WHEN: never\n    steps:",
    );
    // a step's variable whose text is the same and whose value is not
    let wf = wf.replace(
        "      - run: cargo build -v --offline",
        "      - env:\n          CARGO_INCREMENTAL: ${{ github.sha }}\n        run: cargo build -v --offline",
    );
    let f = fixture_with(&wf);
    // the commit changes github.sha, nothing else the key holds
    changing_misses(&f, |f| f.commit("b.txt", "b\n"));
}

#[test]
fn what_is_not_cargos_is_not_kept_or_restored() {
    let f = fixture();
    let cache = f.tmp.path().join("cache");
    let (_, first) = f.run(&cache, false);
    assert!(first.unwrap().saved);
    assert!(!f.found_leftover());
    let key = entry_keys(&cache).remove(0);
    let target = cache.join("build").join(key).join("target");
    // `build` is in every layout of cargo's
    assert!(target.join("debug/build").is_dir());
    assert!(!target.join("tmp").exists());
    assert!(!target.join("nextest").exists());
    // the second job gets the entry and finds no tmp/x of the first
    let (said, second) = f.run(&cache, false);
    assert!(second.unwrap().restored);
    assert!(said.contains("Fresh fxdep"), "{said}");
    assert!(!f.found_leftover());
}

#[test]
fn a_probe_that_takes_too_long_goes_without_the_cache() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture_with(
        "name: ci\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: echo hi\n",
    );
    let bin = f.tmp.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let rustc = bin.join("rustc");
    std::fs::write(&rustc, "#!/bin/sh\nsleep 60\n").unwrap();
    std::fs::set_permissions(&rustc, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut env = ctx(&f.log).env().clone();
    let path = format!(
        "{}:{}",
        bin.display(),
        env.get("PATH").cloned().unwrap_or_default()
    );
    env.insert("PATH".into(), path);
    env.insert(
        "LOCAL_CI_BUILD_CACHE_PROBE_TIMEOUT_SECS".into(),
        "0.5".into(),
    );
    let cache = f.tmp.path().join("cache");
    let started = std::time::Instant::now();
    let job = f.exec(&ExecContext::new(env), &cache, false);
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
    assert!(job.build_cache.is_none(), "{job:?}");
    let note = job
        .steps
        .last()
        .and_then(|s| s.note.clone())
        .unwrap_or_default();
    assert!(note.contains("build cache: skipped (toolchain"), "{note}");
    assert!(!cache.join("build").exists());
}

#[test]
fn a_save_that_fails_leaves_the_outcome_as_it_is() {
    let f = fixture();
    let cache = f.tmp.path().join("cache");
    // the directory of the entries is a file
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(cache.join("build"), "not a directory").unwrap();
    let job = f.exec(&ctx(&f.log), &cache, false);
    let stat = job.build_cache.clone().unwrap();
    assert!(!stat.saved && !stat.restored, "{stat:?}");
    let note = job
        .steps
        .last()
        .and_then(|s| s.note.clone())
        .unwrap_or_default();
    assert!(note.contains("build cache: not saved"), "{note}");
}

#[test]
fn a_restore_that_fails_leaves_the_outcome_as_it_is() {
    use std::os::unix::fs::PermissionsExt;
    if unsafe { libc::geteuid() } == 0 {
        return; // root reads everything
    }
    let f = fixture();
    let cache = f.tmp.path().join("cache");
    let (_, first) = f.run(&cache, false);
    assert!(first.unwrap().saved);
    let key = entry_keys(&cache).remove(0);
    // a directory of the entry that cannot be read: cp fails half way
    let hidden = cache.join("build").join(&key).join("target/debug/build");
    std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o000)).unwrap();
    let (said, second) = f.run(&cache, false);
    std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o755)).unwrap();
    let stat = second.unwrap();
    assert!(!stat.restored, "{stat:?}");
    // the job built as if there were no cache
    assert!(said.contains("Compiling fxdep"), "{said}");
}

#[test]
fn saves_of_one_key_at_once_leave_one_entry() {
    let tmp = common::tempdir().unwrap();
    let ws = tmp.path().join("ws");
    for f in [
        "target/debug/.fingerprint/a/x",
        "target/debug/deps/lib.rlib",
        "target/debug/build/b/out",
    ] {
        let p = ws.join(f);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, vec![1u8; 4096]).unwrap();
    }
    let c = std::sync::Arc::new(ExecContext::new(
        [(
            "PATH".to_string(),
            std::env::var("PATH").unwrap_or_default(),
        )]
        .into_iter()
        .collect(),
    ));
    let s = std::sync::Arc::new(settings(tmp.path()));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(6));
    let handles: Vec<_> = (0..6)
        .map(|n| {
            let (c, s, b, ws) = (c.clone(), s.clone(), barrier.clone(), ws.clone());
            std::thread::spawn(move || {
                b.wait();
                local_ci::build_cache::save(&c, &s, "k", &ws, n)
            })
        })
        .collect();
    let saves: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    use local_ci::build_cache::Save;
    assert_eq!(
        saves.iter().filter(|r| **r == Save::Saved).count(),
        1,
        "{saves:?}"
    );
    assert!(
        saves
            .iter()
            .all(|r| matches!(r, Save::Saved | Save::Exists)),
        "{saves:?}"
    );
    // one entry, no temporary entry, no lock file
    let names: Vec<String> = std::fs::read_dir(&s.dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, ["k"]);
    assert_eq!(local_ci::build_cache::entries(&s.dir).len(), 1);
}

fn settings(root: &Path) -> local_ci::build_cache::Settings {
    local_ci::build_cache::Settings {
        dir: root.join("cache/build"),
        trash: root.join("cache/trash"),
        max_bytes: u64::MAX,
        copy_limit: u64::MAX,
        probe_timeout: std::time::Duration::from_secs(20),
    }
}

#[test]
fn prune_leaves_the_temporary_entry_of_a_writer_and_reaps_a_dead_one() {
    use fs2::FileExt;
    let tmp = common::tempdir().unwrap();
    let s = settings(tmp.path());
    std::fs::create_dir_all(&s.dir).unwrap();
    let c = ExecContext::new(Default::default());
    let tmp_entry = s.dir.join("k.tmp-1-0");
    std::fs::create_dir_all(&tmp_entry).unwrap();
    std::fs::write(tmp_entry.join("f"), "x").unwrap();
    let lock_file = s.dir.join("k.tmp-1-0.lock");
    let held = std::fs::File::create(&lock_file).unwrap();
    held.lock_exclusive().unwrap();
    // a writer holds it
    let p = local_ci::build_cache::prune(&c, &s, None);
    assert_eq!(p.removed, 0, "{p:?}");
    assert!(tmp_entry.join("f").exists() && lock_file.exists());
    let p = local_ci::build_cache::clear(&c, &s);
    assert_eq!(p.removed, 0, "{p:?}");
    assert!(tmp_entry.join("f").exists());
    // the writer died: its lock is gone with it, the files are left
    held.unlock().unwrap();
    drop(held);
    let p = local_ci::build_cache::prune(&c, &s, None);
    assert_eq!(p.removed, 1, "{p:?}");
    assert!(!tmp_entry.exists() && !lock_file.exists());
}

#[test]
fn a_step_variable_that_reads_an_earlier_step_does_not_fail_the_job() {
    // not known when the job starts: the job passes, with or without the cache
    let wf = BUILD_WORKFLOW.replace(
        "      - run: cargo build -v --offline",
        "      - env:\n          CARGO_INCREMENTAL: ${{ steps.nope.outputs.v }}\n        run: cargo build -v --offline",
    );
    let f = fixture_with(&wf);
    let cache = f.tmp.path().join("cache");
    let job = f.exec(&ctx(&f.log), &cache, false);
    let note = job
        .steps
        .last()
        .and_then(|s| s.note.clone())
        .unwrap_or_default();
    assert!(
        job.build_cache.is_some() || note.contains("build cache: skipped"),
        "{note}"
    );
}
