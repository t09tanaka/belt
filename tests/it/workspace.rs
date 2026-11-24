//! Spec 22.5 end to end, over throwaway repositories: attempt workspaces made
//! and removed (with a stand-in for `fast-worktree`, and with the real one
//! where it is installed), the original never written, concurrent attempts of
//! one repository, directory snapshots, `--in-place`, and a missing tool.
//!
//! The real `fast-worktree` is used when it is on the PATH; set
//! `BELT_REQUIRE_FAST_WORKTREE=1` (CI does) to make its absence a failure.

use crate::common;

use belt::error::{BlockedReason, ErrorCode};
use belt::ids::AttemptId;
use belt::source::retention::CloneGc;
use belt::source::{capture, CaptureParams, ObjectsRepo, SourceRequest, SourceStore};
use belt::workspace::plan::FwFailure;
use belt::workspace::{CliFastWorktree, FastWorktree, Workspace, WorkspaceKind, Workspaces};
use local_ci::source::SourceManifest as CiManifest;
use local_ci::ExecContext;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

const BASE_PATH: &str = "/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin";

fn path_var() -> String {
    // the real tool lives in cargo's bin directory
    match std::env::var("HOME") {
        Ok(h) => format!("{BASE_PATH}:{h}/.cargo/bin"),
        Err(_) => BASE_PATH.to_string(),
    }
}

fn env(home: &Path) -> BTreeMap<String, String> {
    [
        ("PATH".to_string(), path_var()),
        ("HOME".to_string(), home.to_str().unwrap().to_string()),
        ("GIT_CONFIG_GLOBAL".to_string(), "/dev/null".to_string()),
        ("GIT_CONFIG_NOSYSTEM".to_string(), "1".to_string()),
        ("GIT_AUTHOR_NAME".to_string(), "t".to_string()),
        ("GIT_AUTHOR_EMAIL".to_string(), "t@example.com".to_string()),
        ("GIT_COMMITTER_NAME".to_string(), "t".to_string()),
        (
            "GIT_COMMITTER_EMAIL".to_string(),
            "t@example.com".to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

fn git_out(env: &BTreeMap<String, String>, dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(["-c", "protocol.file.allow=always"])
        .args(["-c", "gc.auto=0", "-c", "core.fsmonitor=false"])
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(env)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim_end().to_string()
}

/// A stand-in for `fast-worktree` that needs only Git: it refuses a dirty
/// worktree on remove, as the real one does, and runs the project's hooks not
/// at all.
struct GitFw {
    env: BTreeMap<String, String>,
}

/// `git worktree add` and `remove` race with each other in one repository
/// (Git lists `.git/worktrees` while another call deletes an entry); the real
/// tool unregisters only its own entry, so the stand-in is serialized.
static GIT_WORKTREE: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl FastWorktree for GitFw {
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure> {
        let _g = GIT_WORKTREE.lock().unwrap_or_else(|e| e.into_inner());
        let out = Command::new("git")
            .args(["worktree", "add", "-q", "-b", branch])
            .arg(dest)
            .arg(start)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .output()
            .unwrap();
        if out.status.success() {
            Ok(())
        } else {
            Err(FwFailure::Failed {
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            })
        }
    }

    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        let _g = GIT_WORKTREE.lock().unwrap_or_else(|e| e.into_inner());
        let st = Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=all"])
            .current_dir(path)
            .env_clear()
            .envs(&self.env)
            .output()
            .unwrap();
        if !st.stdout.is_empty() {
            return Err(FwFailure::Failed {
                code: Some(1),
                stderr: "uncommitted or untracked changes".into(),
            });
        }
        let out = Command::new("git")
            .args(["worktree", "remove"])
            .arg(path)
            .current_dir(repo)
            .env_clear()
            .envs(&self.env)
            .output()
            .unwrap();
        if out.status.success() {
            Ok(())
        } else {
            Err(FwFailure::Failed {
                code: out.status.code(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            })
        }
    }
}

struct Backend {
    name: &'static str,
    real: bool,
    make: fn(BTreeMap<String, String>) -> Arc<dyn FastWorktree>,
}

impl Backend {
    /// The tool for a fixture: its own HOME, so no cache is shared.
    fn fw(&self, f: &Fixture) -> Arc<dyn FastWorktree> {
        (self.make)(f.env())
    }
}

/// Runs `f` against every backend at once: the backends share nothing (each
/// body makes its own fixture and its own HOME), so their checks overlap
/// instead of queueing behind one another.
fn each_backend(f: impl Fn(&Backend) + Sync) {
    let all = backends(&common_home());
    std::thread::scope(|s| {
        for b in &all {
            let f = &f;
            s.spawn(move || f(b));
        }
    });
}

fn real_available(home: &Path) -> bool {
    Command::new("fast-worktree")
        .arg("--version")
        .env_clear()
        .envs(env(home))
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn backends(home: &Path) -> Vec<Backend> {
    let mut v = vec![Backend {
        name: "git",
        real: false,
        make: |env| Arc::new(GitFw { env }),
    }];
    if real_available(home) {
        v.push(Backend {
            name: "fast-worktree",
            real: true,
            make: |env| Arc::new(CliFastWorktree::new(env)),
        });
    } else if std::env::var("BELT_REQUIRE_FAST_WORKTREE").is_ok() {
        panic!("fast-worktree is required here and is not installed");
    } else {
        eprintln!("fast-worktree is not installed: only the stand-in is tested");
    }
    v
}

struct Fixture {
    _tmp: common::Tmp,
    root: PathBuf,
    /// the repository the user works in
    dir: PathBuf,
    /// the worker's state directory
    state: PathBuf,
}

impl Fixture {
    fn env(&self) -> BTreeMap<String, String> {
        env(&self.root.join("home"))
    }
    fn git(&self, dir: &Path, args: &[&str]) -> String {
        git_out(&self.env(), dir, args)
    }
    fn store(&self) -> SourceStore {
        SourceStore::open(&self.root.join("capture-state")).unwrap()
    }
    fn workspaces(&self, fw: &Arc<dyn FastWorktree>) -> Workspaces {
        Workspaces::new(self.state.clone(), self.env(), fw.clone())
    }
    fn clone_of(&self, ws: &Workspaces) -> ObjectsRepo {
        ws.ensure_clone("github:example/repo", Some(self.dir.to_str().unwrap()))
            .unwrap()
    }
    /// The bundle of `m` unpacked into the clone, as a transfer would.
    fn deliver(&self, store: &SourceStore, repo: &ObjectsRepo, m: &CiManifest) {
        if repo.has_commit(m.execution_commit.as_deref().unwrap()) {
            return;
        }
        let bundle = store
            .export_bundle(&ExecContext::new(self.env()), m, &[])
            .unwrap();
        repo.receive_bundle(&bundle, m, &store.dirty_below(m).unwrap())
            .unwrap();
    }
    fn commit(&self, file: &str, content: &str) -> String {
        std::fs::write(self.dir.join(file), content).unwrap();
        self.git(&self.dir, &["add", "."]);
        self.git(&self.dir, &["commit", "-qm", file]);
        self.git(&self.dir, &["rev-parse", "HEAD"])
    }
    fn capture_commit(&self, store: &SourceStore, rev: &str) -> CiManifest {
        let c = capture(
            &self.env(),
            store,
            &SourceRequest::Commit {
                dir: self.dir.clone(),
                rev: rev.to_string(),
            },
            &CaptureParams::default(),
        )
        .unwrap();
        store.manifest(&c.manifest.ci_source_id).unwrap().unwrap()
    }
}

fn fixture() -> Fixture {
    let tmp = common::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let dir = root.join("work");
    std::fs::create_dir_all(root.join("home")).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    let f = Fixture {
        state: root.join("worker-state"),
        _tmp: tmp,
        root,
        dir,
    };
    f.git(&f.dir, &["init", "-q", "-b", "main"]);
    std::fs::write(f.dir.join(".gitignore"), "deps/\nlocal.env\n").unwrap();
    std::fs::write(
        f.dir.join(".fast-worktree.toml"),
        "[[post_add]]\nrun = \"mkdir -p deps && echo ready > deps/ready\"\n\n\
         [[pre_remove]]\nrun = \"echo gone > \\\"$FAST_WORKTREE_MAIN/.git/pre-remove-ran\\\"\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(f.dir.join("pkg")).unwrap();
    std::fs::write(f.dir.join("pkg/lib.txt"), "lib\n").unwrap();
    std::fs::write(f.dir.join("a.txt"), "a\n").unwrap();
    std::fs::write(f.dir.join("run.sh"), "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(
        f.dir.join("run.sh"),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    f.git(&f.dir, &["add", "."]);
    f.git(&f.dir, &["commit", "-qm", "base"]);
    f
}

/// Everything under `dir`, `.git` included: relative path, type, mode, and
/// the bytes (or link target), one digest per path.
fn tree_snapshot(dir: &Path) -> std::collections::BTreeMap<String, String> {
    use std::os::unix::fs::PermissionsExt;
    fn walk(root: &Path, rel: &Path, out: &mut std::collections::BTreeMap<String, String>) {
        let mut names: Vec<_> = std::fs::read_dir(root.join(rel))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        names.sort();
        for n in names {
            let p = rel.join(&n);
            let full = root.join(&p);
            let meta = std::fs::symlink_metadata(&full).unwrap();
            let mut h = Sha256::new();
            h.update(format!("{:o}:", meta.permissions().mode()).as_bytes());
            if meta.is_dir() {
                h.update(b"d");
                out.insert(p.to_string_lossy().into_owned(), hex(&h.finalize()));
                walk(root, &p, out);
                continue;
            }
            if meta.file_type().is_symlink() {
                h.update(
                    std::fs::read_link(&full)
                        .unwrap()
                        .to_string_lossy()
                        .as_bytes(),
                );
            } else {
                h.update(std::fs::read(&full).unwrap());
            }
            out.insert(p.to_string_lossy().into_owned(), hex(&h.finalize()));
        }
    }
    fn hex(d: &[u8]) -> String {
        d.iter().map(|b| format!("{b:02x}")).collect()
    }
    let mut out = std::collections::BTreeMap::new();
    walk(dir, Path::new(""), &mut out);
    out
}

/// Fails naming the paths that were added, removed or changed since `before`.
fn assert_tree_unchanged(
    dir: &Path,
    before: &std::collections::BTreeMap<String, String>,
    what: &str,
) {
    let after = tree_snapshot(dir);
    let mut diff = Vec::new();
    for (p, h) in before {
        match after.get(p) {
            None => diff.push(format!("removed {p}")),
            Some(a) if a != h => diff.push(format!("changed {p}")),
            _ => {}
        }
    }
    for p in after.keys().filter(|p| !before.contains_key(*p)) {
        diff.push(format!("added {p}"));
    }
    assert!(diff.is_empty(), "{what}: the original changed: {diff:?}");
}

fn worktrees(f: &Fixture, clone: &Path) -> Vec<String> {
    f.git(clone, &["worktree", "list", "--porcelain"])
        .lines()
        .filter_map(|l| l.strip_prefix("worktree ").map(str::to_string))
        .collect()
}

fn branches(f: &Fixture, clone: &Path) -> Vec<String> {
    f.git(
        clone,
        &["branch", "--list", "belt/*", "--format=%(refname:short)"],
    )
    .lines()
    .map(str::to_string)
    .collect()
}

#[test]
fn add_and_remove_a_commit_workspace() {
    each_backend(|b| {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        assert!(w.root.ends_with(att.as_str()), "{}", b.name);
        assert_eq!(w.cwd, w.root);
        assert_eq!(
            std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
            "a\n"
        );
        assert_eq!(
            std::fs::read_to_string(w.root.join("pkg/lib.txt")).unwrap(),
            "lib\n"
        );
        assert!(w.root.join(".git").exists());
        if b.real {
            // the project's own preparation ran
            assert_eq!(
                std::fs::read_to_string(w.root.join("deps/ready")).unwrap(),
                "ready\n"
            );
        }
        assert_eq!(branches(&f, repo.dir()).len(), 1);
        // the run leaves tracked changes and new files behind
        std::fs::write(w.root.join("a.txt"), "changed\n").unwrap();
        std::fs::write(w.root.join("out.log"), "x\n").unwrap();
        ws.remove(&w).unwrap();
        assert!(!w.root.exists(), "{}", b.name);
        assert!(branches(&f, repo.dir()).is_empty());
        assert_eq!(worktrees(&f, repo.dir()).len(), 1);
        if b.real {
            assert!(repo.dir().join(".git/pre-remove-ran").exists());
        }
        // removing again is fine
        ws.remove(&w).unwrap();
    });
}

/// A48: terminal cleanup runs the project's release hook for this attempt.
#[test]
fn removing_an_attempt_releases_only_its_portool_allocation() {
    let f = fixture();
    let bin = f.root.join("embedded-bin");
    std::fs::create_dir(&bin).unwrap();
    for tool in ["portool", "fast-worktree"] {
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_belt"), bin.join(tool)).unwrap();
    }
    let mut embedded_env = f.env();
    embedded_env.insert("PATH".into(), format!("{}:/usr/bin:/bin", bin.display()));
    // The attempts belong to the worker clone. The capture repository has
    // a different portool project, but both use this fixture's private HOME.
    let allocations = || {
        let out = Command::new("portool")
            .args(["ls", "--all", "--json"])
            .env_clear()
            .envs(&embedded_env)
            .current_dir(&f.dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        json["allocations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| PathBuf::from(entry["path"].as_str().unwrap()))
            .collect::<std::collections::BTreeSet<_>>()
    };
    f.commit(".portool.toml", "[ports]\nweb = 0\n");
    // The release hook is part of the uncommitted snapshot used at admission.
    std::fs::write(f.dir.join(".fast-worktree.toml"), "[[post_add]]\nrun = \"portool sync --quiet && portool check\"\n[[pre_remove]]\nrun = \"portool release\"\n").unwrap();
    assert!(allocations().is_empty());
    let store = f.store();
    let manifest = dirty_capture(&f, &store);
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(embedded_env.clone()));
    let ws = f.workspaces(&fw);
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &manifest);
    let create = || {
        ws.create(&AttemptId::generate(), &store, &manifest, Some(&repo))
            .unwrap()
    };
    let first = create();
    let second = create();
    let roots = [
        first.root.canonicalize().unwrap(),
        second.root.canonicalize().unwrap(),
    ];
    assert_eq!(allocations(), roots.clone().into_iter().collect());
    let command = Command::new("portool")
        .args(["exec", "--", "/bin/sh", "-c", "test -n \"$WEB_PORT\""])
        .current_dir(&first.root)
        .env_clear()
        .envs(&embedded_env)
        .status()
        .unwrap();
    assert!(command.success());
    ws.remove(&first).unwrap();
    assert!(!first.root.exists());
    assert_eq!(allocations(), [roots[1].clone()].into_iter().collect());
    ws.remove(&second).unwrap();
    assert!(allocations().is_empty());

    // A standalone installation, when available, uses the very same
    // registry. It must receive a disjoint block and survive attempt cleanup.
    let standalone = |args: &[&str]| {
        Command::new("portool")
            .args(args)
            .env_clear()
            .envs(f.env())
            .current_dir(&f.dir)
            .output()
    };
    let has_standalone = standalone(&["--version"]).is_ok_and(|out| out.status.success());
    // The final release belongs to the worker even without a pre_remove hook.
    std::fs::write(
        f.dir.join(".fast-worktree.toml"),
        "[[post_add]]\nrun = \"portool sync --quiet\"\n",
    )
    .unwrap();
    let manifest = dirty_capture(&f, &store);
    f.deliver(&store, &repo, &manifest);
    let no_hook = std::thread::scope(|scope| {
        let side = scope.spawn(|| {
            if has_standalone {
                assert!(standalone(&["sync", "--quiet"]).unwrap().status.success());
            }
        });
        let attempt = ws
            .create(&AttemptId::generate(), &store, &manifest, Some(&repo))
            .unwrap();
        side.join().unwrap();
        attempt
    });
    assert!(allocations().contains(&no_hook.root));
    if has_standalone {
        assert_eq!(allocations().len(), 2);
        assert!(standalone(&["check"]).unwrap().status.success());
        let read = |dir: &Path| {
            std::fs::read_to_string(dir.join(".env.portool"))
                .unwrap()
                .lines()
                .find(|line| line.starts_with("WEB_PORT="))
                .unwrap()
                .to_owned()
        };
        assert_ne!(read(&no_hook.root), read(&f.dir));
    }
    ws.remove(&no_hook).unwrap();
    if has_standalone {
        assert_eq!(
            allocations(),
            [f.dir.canonicalize().unwrap()].into_iter().collect()
        );
        assert!(standalone(&["release"]).unwrap().status.success());
    }
    assert!(allocations().is_empty());
}

fn common_home() -> PathBuf {
    // a home for the stand-in checks that never exists: only the PATH matters
    PathBuf::from("/nonexistent-home")
}

#[test]
fn a_subdirectory_is_the_working_directory() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let c = capture(
        &f.env(),
        &store,
        &SourceRequest::Commit {
            dir: f.dir.join("pkg"),
            rev: "HEAD".into(),
        },
        &CaptureParams::default(),
    )
    .unwrap();
    let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    let w = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    assert_eq!(w.cwd, w.root.join("pkg"));
    assert!(w.cwd.join("lib.txt").exists());
    ws.remove(&w).unwrap();
}

#[test]
fn the_original_is_never_written() {
    each_backend(|b| {
        let f = fixture();
        // dirty: a tracked change, an untracked file, an ignored file to take
        std::fs::write(f.dir.join("a.txt"), "dirty\n").unwrap();
        std::fs::write(f.dir.join("new.txt"), "new\n").unwrap();
        std::fs::write(f.dir.join("local.env"), "SECRET_FREE=1\n").unwrap();
        let store = f.store();
        let c = capture(
            &f.env(),
            &store,
            &SourceRequest::Worktree { dir: f.dir.clone() },
            &CaptureParams {
                include: vec!["local.env".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        // the synthetic commit reaches the worker as a bundle
        f.deliver(&store, &repo, &m);
        let before = tree_snapshot(&f.dir);
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        // the dirty state, and the ignored file that was asked for, arrived
        assert_eq!(
            std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
            "dirty\n"
        );
        assert_eq!(
            std::fs::read_to_string(w.root.join("new.txt")).unwrap(),
            "new\n"
        );
        assert_eq!(
            std::fs::read_to_string(w.root.join("local.env")).unwrap(),
            "SECRET_FREE=1\n"
        );
        // and the run wrecks the workspace
        std::fs::write(w.root.join("a.txt"), "wrecked\n").unwrap();
        std::fs::remove_file(w.root.join("pkg/lib.txt")).unwrap();
        ws.remove(&w).unwrap();
        assert_tree_unchanged(&f.dir, &before, b.name);
    });
}

#[test]
fn a_failed_unpack_leaves_nothing_behind() {
    each_backend(|b| {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        // a store that does not hold the blobs
        let empty = SourceStore::open(&f.root.join("empty-state")).unwrap();
        let att = AttemptId::generate();
        let e = ws.create(&att, &empty, &m, Some(&repo)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal, "{}: {e:?}", b.name);
        assert!(!f.state.join("workspaces").join(att.as_str()).exists());
        assert!(branches(&f, repo.dir()).is_empty());
        assert_eq!(worktrees(&f, repo.dir()).len(), 1);
        let scratch: Vec<_> = std::fs::read_dir(f.state.join("workspaces"))
            .unwrap()
            .collect();
        assert!(scratch.is_empty(), "scratch directories were left");
        // the same attempt can be tried again with the right store
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        ws.remove(&w).unwrap();
    });
}

#[test]
fn a_directory_snapshot_gets_a_plain_directory() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let plain = f.root.join("plain");
    std::fs::create_dir_all(plain.join("sub")).unwrap();
    std::fs::write(plain.join("sub/x.txt"), "x\n").unwrap();
    std::fs::write(plain.join("top.txt"), "t\n").unwrap();
    let store = f.store();
    let c = capture(
        &f.env(),
        &store,
        &SourceRequest::Directory {
            dir: plain.join("sub"),
        },
        &CaptureParams::default(),
    )
    .unwrap();
    let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
    let ws = f.workspaces(&b.fw(&f));
    let att = AttemptId::generate();
    let w = ws.create(&att, &store, &m, None).unwrap();
    assert_eq!(w.kind, WorkspaceKind::Directory);
    assert_eq!(std::fs::read_to_string(w.cwd.join("x.txt")).unwrap(), "x\n");
    assert!(!w.root.join(".git").exists());
    // a second create of the same attempt does not touch the first
    let e = ws.create(&att, &store, &m, None).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert!(w.cwd.join("x.txt").exists());
    ws.remove(&w).unwrap();
    assert!(!w.root.exists());
    // a failed unpack removes its directory
    let empty = SourceStore::open(&f.root.join("empty-state")).unwrap();
    let att2 = AttemptId::generate();
    assert!(ws.create(&att2, &empty, &m, None).is_err());
    assert!(!f.state.join("workspaces").join(att2.as_str()).exists());
}

#[test]
fn a_git_source_needs_its_clone_with_the_commit() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    let ws = f.workspaces(&b.fw(&f));
    let e = ws
        .create(&AttemptId::generate(), &store, &m, None)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // an empty clone does not hold the commit
    let empty = ws.ensure_clone("github:example/empty", None).unwrap();
    let e = ws
        .create(&AttemptId::generate(), &store, &m, Some(&empty))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
}

#[test]
fn concurrent_attempts_of_one_repository_do_not_collide() {
    each_backend(|b| {
        let f = fixture();
        let store = Arc::new(f.store());
        let first = f.git(&f.dir, &["rev-parse", "HEAD"]);
        let second = f.commit("second.txt", "two\n");
        let m1 = f.capture_commit(&store, &first);
        let m2 = f.capture_commit(&store, &second);
        // receiving is the transfer layer's, one at a time; the workspaces are ours
        {
            let ws = f.workspaces(&b.fw(&f));
            let repo = f.clone_of(&ws);
            f.deliver(&store, &repo, &m1);
            f.deliver(&store, &repo, &m2);
        }
        // every thread asks for the clone itself, as separate processes would
        let results: Vec<_> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..16)
                .map(|i| {
                    let (f, store, b) = (&f, &store, &b);
                    let (m1, m2) = (&m1, &m2);
                    s.spawn(move || {
                        let ws = f.workspaces(&b.fw(f));
                        let repo = f.clone_of(&ws);
                        let m = if i % 2 == 0 { m1 } else { m2 };
                        let att = AttemptId::generate();
                        let w = ws.create(&att, store, m, Some(&repo)).unwrap();
                        (i, w, ws)
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let clones: std::collections::BTreeSet<_> = results
            .iter()
            .map(|(_, w, _)| match &w.kind {
                WorkspaceKind::Worktree { clone, .. } => clone.clone(),
                k => panic!("{k:?}"),
            })
            .collect();
        assert_eq!(clones.len(), 1, "{}: one canonical clone", b.name);
        let roots: std::collections::BTreeSet<_> =
            results.iter().map(|(_, w, _)| w.root.clone()).collect();
        assert_eq!(roots.len(), 16);
        for (i, w, _) in &results {
            let has_second = w.root.join("second.txt").exists();
            assert_eq!(has_second, i % 2 == 1, "{}: attempt {i}", b.name);
            assert_eq!(
                std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
                "a\n"
            );
        }
        let clone = clones.into_iter().next().unwrap();
        assert_eq!(branches(&f, &clone).len(), 16);
        // and they go away together
        std::thread::scope(|s| {
            for (_, w, ws) in &results {
                s.spawn(move || ws.remove(w).unwrap());
            }
        });
        assert!(branches(&f, &clone).is_empty(), "{}", b.name);
        assert_eq!(worktrees(&f, &clone).len(), 1);
    });
}

#[test]
fn a_missing_or_failing_fast_worktree_is_an_environment_mismatch() {
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    // not on the PATH it is given
    let mut none = env(&f.root.join("home"));
    none.insert("PATH".into(), "/nonexistent-bin".into());
    let missing: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(none));
    // a stand-in that fails
    struct Broken;
    impl FastWorktree for Broken {
        fn add(&self, _: &Path, _: &Path, _: &str, _: &str) -> Result<(), FwFailure> {
            Err(FwFailure::Failed {
                code: Some(1),
                stderr: "FAILED post_add".into(),
            })
        }
        fn remove(&self, _: &Path, _: &Path) -> Result<(), FwFailure> {
            Err(FwFailure::Failed {
                code: Some(1),
                stderr: "stuck".into(),
            })
        }
    }
    let broken: Arc<dyn FastWorktree> = Arc::new(Broken);
    for fw in [missing, broken] {
        let ws = f.workspaces(&fw);
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        let e = ws.create(&att, &store, &m, Some(&repo)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Blocked, "{e:?}");
        assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(!f.state.join("workspaces").join(att.as_str()).exists());
        assert!(branches(&f, repo.dir()).is_empty());
    }
}

#[test]
fn in_place_uses_the_original_and_never_removes_it() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    let before = tree_snapshot(&f.dir);
    // only the parent's local worker
    let e = ws.open_in_place(&f.dir, Path::new(""), false).unwrap_err();
    assert_eq!(e.blocked_reason, Some(BlockedReason::NoCompatibleWorker));
    let w = ws.open_in_place(&f.dir, Path::new("pkg"), true).unwrap();
    assert_eq!(w.kind, WorkspaceKind::InPlace);
    assert_eq!(w.root, std::fs::canonicalize(&f.dir).unwrap());
    assert_eq!(w.cwd, w.root.join("pkg"));
    ws.remove(&w).unwrap();
    assert_tree_unchanged(&f.dir, &before, "in place");
    assert!(f.dir.join(".git").exists());
    // the runner's own state is not an in-place directory
    std::fs::create_dir_all(&f.state).unwrap();
    for d in [f.state.clone(), f.state.join("workspaces"), f.root.clone()] {
        std::fs::create_dir_all(&d).unwrap();
        let e = ws.open_in_place(&d, Path::new(""), true).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput, "{d:?}");
    }
    let e = ws
        .open_in_place(&f.root.join("nope"), Path::new(""), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    let e = ws
        .open_in_place(&f.dir, Path::new("missing"), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    let e = ws
        .open_in_place(&f.dir.join("a.txt"), Path::new(""), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
}

#[test]
fn only_attempt_workspaces_can_be_removed() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    let before = tree_snapshot(&f.dir);
    for root in [f.dir.clone(), f.state.join("workspaces"), f.root.clone()] {
        let w = Workspace {
            cwd: root.clone(),
            root,
            kind: WorkspaceKind::Directory,
            index_reproduced: true,
            warnings: vec![],
        };
        let e = ws.remove(&w).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
    }
    assert_tree_unchanged(&f.dir, &before, "removal");
}

#[test]
fn the_clone_is_made_once_and_updated_from_origin() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    let again = f.clone_of(&ws);
    assert_eq!(repo.dir(), again.dir());
    let head = f.commit("later.txt", "later\n");
    assert!(!repo.has_commit(&head));
    ws.update_clone(&repo).unwrap();
    assert!(repo.has_commit(&head));
    // an empty clone has no origin to fetch from: nothing to do
    let empty = ws.ensure_clone("github:example/empty", None).unwrap();
    ws.update_clone(&empty).unwrap();
}

#[test]
fn a_create_after_a_crash_clears_the_stale_branch_and_registration() {
    each_backend(|b| {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        // a worktree made, then its directory lost (a crash, a cleaned disk)
        let dir = f.state.join("workspaces").join(att.as_str());
        std::fs::create_dir_all(f.state.join("workspaces")).unwrap();
        let commit = m.execution_commit.clone().unwrap();
        f.git(
            repo.dir(),
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                &format!("belt/{att}"),
                dir.to_str().unwrap(),
                &commit,
            ],
        );
        std::fs::remove_dir_all(&dir).unwrap();
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        assert!(w.root.join("a.txt").exists(), "{}", b.name);
        ws.remove(&w).unwrap();
        assert!(branches(&f, repo.dir()).is_empty());
    });
}

// ---- review fixes -----------------------------------------------------

/// A worktree capture of a repository with a staged change, an unstaged one
/// and an untracked file.
fn dirty_capture(f: &Fixture, store: &SourceStore) -> CiManifest {
    std::fs::write(f.dir.join("a.txt"), "staged\n").unwrap();
    f.git(&f.dir, &["add", "a.txt"]);
    std::fs::write(f.dir.join("pkg/lib.txt"), "unstaged\n").unwrap();
    std::fs::write(f.dir.join("new.txt"), "untracked\n").unwrap();
    let c = capture(
        &f.env(),
        store,
        &SourceRequest::Worktree { dir: f.dir.clone() },
        &CaptureParams::default(),
    )
    .unwrap();
    store.manifest(&c.manifest.ci_source_id).unwrap().unwrap()
}

#[test]
fn head_and_index_are_the_originals() {
    each_backend(|b| {
        let f = fixture();
        let store = f.store();
        let m = dirty_capture(&f, &store);
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        // the bundle carries the index tree (local-ci 0.10)
        assert!(m.index_tree.is_some());
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        assert!(w.index_reproduced, "{}: {:?}", b.name, w.warnings);
        assert!(w.warnings.is_empty());
        let head = f.git(&f.dir, &["rev-parse", "HEAD"]);
        assert_eq!(f.git(&w.root, &["rev-parse", "HEAD"]), head, "{}", b.name);
        for args in [
            vec!["diff"],
            vec!["diff", "--cached"],
            vec!["status", "--porcelain"],
        ] {
            assert_eq!(
                f.git(&w.root, &args),
                f.git(&f.dir, &args),
                "{}: git {args:?}",
                b.name
            );
        }
        assert!(f
            .git(&w.root, &["diff", "--cached", "--name-only"])
            .contains("a.txt"));
        ws.remove(&w).unwrap();
        assert!(branches(&f, repo.dir()).is_empty());
    });
}

#[test]
fn without_the_index_tree_the_index_is_the_heads_and_it_is_said() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let m = dirty_capture(&f, &store);
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    // a clone that has not got the index tree (a bundle made before local-ci 0.10)
    let index_ref = f.git(
        repo.dir(),
        &["for-each-ref", "--format=%(refname)", "refs/local-ci/index"],
    );
    f.git(repo.dir(), &["update-ref", "-d", index_ref.trim()]);
    f.git(repo.dir(), &["reflog", "expire", "--expire=now", "--all"]);
    f.git(repo.dir(), &["repack", "-a", "-d", "-q"]);
    f.git(repo.dir(), &["prune", "--expire=now"]);
    let head = f.git(&f.dir, &["rev-parse", "HEAD"]);
    let w = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    assert!(!w.index_reproduced);
    assert_eq!(w.warnings.len(), 1, "{:?}", w.warnings);
    assert!(w.warnings[0].contains("index tree"), "{:?}", w.warnings);
    assert_eq!(f.git(&w.root, &["rev-parse", "HEAD"]), head);
    // the index is HEAD's tree: nothing staged, all changes unstaged
    assert_eq!(f.git(&w.root, &["diff", "--cached"]), "");
    assert!(f.git(&w.root, &["diff", "--name-only"]).contains("a.txt"));
    assert_eq!(
        std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
        "staged\n"
    );
    ws.remove(&w).unwrap();
}

/// A `fast-worktree` that does a little and fails, leaving `dest` behind.
struct Half {
    gitfile: Option<String>,
}

impl FastWorktree for Half {
    fn add(&self, _: &Path, dest: &Path, _: &str, _: &str) -> Result<(), FwFailure> {
        std::fs::create_dir_all(dest).unwrap();
        if let Some(g) = &self.gitfile {
            std::fs::write(dest.join(".git"), g).unwrap();
        }
        std::fs::write(dest.join("junk"), "x").unwrap();
        Err(FwFailure::Failed {
            code: Some(1),
            stderr: "half".into(),
        })
    }
    fn remove(&self, _: &Path, _: &Path) -> Result<(), FwFailure> {
        panic!("a directory that is not a worktree of the clone is not handed to the tool")
    }
}

#[test]
fn a_failed_add_inside_another_repository_does_not_touch_it() {
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    // the worker's state lives inside someone's repository, which is dirty
    let outer = f.root.join("outer");
    std::fs::create_dir_all(&outer).unwrap();
    f.git(&outer, &["init", "-q", "-b", "main"]);
    std::fs::write(outer.join("t.txt"), "committed\n").unwrap();
    f.git(&outer, &["add", "."]);
    f.git(&outer, &["commit", "-qm", "outer"]);
    std::fs::write(outer.join("t.txt"), "my uncommitted work\n").unwrap();
    std::fs::write(outer.join("untracked.txt"), "mine\n").unwrap();
    let want = (
        f.git(&outer, &["status", "--porcelain"]),
        f.git(&outer, &["rev-parse", "HEAD"]),
    );
    for gitfile in [
        None,
        Some("gitdir: /nonexistent/.git/worktrees/x\n".to_string()),
        Some(format!("gitdir: {}\n", outer.join(".git").display())),
    ] {
        let fw: Arc<dyn FastWorktree> = Arc::new(Half { gitfile });
        let ws = Workspaces::new(outer.join("state"), f.env(), fw);
        let repo = ws
            .ensure_clone("github:example/repo", Some(f.dir.to_str().unwrap()))
            .unwrap();
        f.deliver(&f.store(), &repo, &m);
        let att = AttemptId::generate();
        let e = ws.create(&att, &store, &m, Some(&repo)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Blocked, "{e:?}");
        assert!(!outer.join("state/workspaces").join(att.as_str()).exists());
        assert_eq!(
            std::fs::read_to_string(outer.join("t.txt")).unwrap(),
            "my uncommitted work\n"
        );
        assert!(outer.join("untracked.txt").exists());
        let status = f.git(&outer, &["status", "--porcelain"]);
        let status: Vec<_> = status.lines().filter(|l| !l.contains("state/")).collect();
        assert_eq!(status.join("\n"), want.0, "{status:?}");
        assert_eq!(f.git(&outer, &["rev-parse", "HEAD"]), want.1);
    }
}

#[test]
fn a_symlink_at_the_workspaces_place_is_not_followed() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    // a repository of someone else, dirty, that the link points at
    let victim = f.root.join("victim");
    std::fs::create_dir_all(&victim).unwrap();
    f.git(&victim, &["init", "-q", "-b", "main"]);
    std::fs::write(victim.join("v.txt"), "v\n").unwrap();
    f.git(&victim, &["add", "."]);
    f.git(&victim, &["commit", "-qm", "v"]);
    std::fs::write(victim.join("v.txt"), "precious\n").unwrap();
    let att = AttemptId::generate();
    let place = f.state.join("workspaces").join(att.as_str());
    std::fs::create_dir_all(f.state.join("workspaces")).unwrap();
    std::os::unix::fs::symlink(&victim, &place).unwrap();
    // a create finds something there that is not its leftover
    let e = ws.create(&att, &store, &m, Some(&repo)).unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
    assert_eq!(
        std::fs::read_to_string(victim.join("v.txt")).unwrap(),
        "precious\n"
    );
    // a removal removes the link, not what it points at
    let w = Workspace {
        root: place.clone(),
        cwd: place.clone(),
        kind: WorkspaceKind::Worktree {
            clone: repo.dir().to_path_buf(),
            branch: format!("belt/{att}"),
        },
        index_reproduced: true,
        warnings: vec![],
    };
    ws.remove(&w).unwrap();
    assert!(std::fs::symlink_metadata(&place).is_err());
    assert_eq!(
        std::fs::read_to_string(victim.join("v.txt")).unwrap(),
        "precious\n"
    );
    assert_eq!(f.git(&victim, &["status", "--porcelain"]), " M v.txt");
}

#[test]
fn a_create_over_a_dead_creates_directory_starts_over() {
    each_backend(|b| {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let att = AttemptId::generate();
        let dir = f.state.join("workspaces").join(att.as_str());
        std::fs::create_dir_all(f.state.join("workspaces")).unwrap();
        f.git(
            repo.dir(),
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                &format!("belt/{att}"),
                dir.to_str().unwrap(),
                m.execution_commit.as_deref().unwrap(),
            ],
        );
        // it died with the directory half used
        std::fs::write(dir.join("a.txt"), "half\n").unwrap();
        std::fs::write(dir.join("left-over"), "x\n").unwrap();
        let w = ws.create(&att, &store, &m, Some(&repo)).unwrap();
        assert_eq!(
            std::fs::read_to_string(w.root.join("a.txt")).unwrap(),
            "a\n",
            "{}",
            b.name
        );
        assert!(!w.root.join("left-over").exists());
        ws.remove(&w).unwrap();
        assert!(branches(&f, repo.dir()).is_empty());
        assert_eq!(worktrees(&f, repo.dir()).len(), 1);
        // a directory with no branch and no registration is not ours
        let att2 = AttemptId::generate();
        let other = f.state.join("workspaces").join(att2.as_str());
        std::fs::create_dir_all(&other).unwrap();
        let e = ws.create(&att2, &store, &m, Some(&repo)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Conflict);
        assert!(other.exists());
    });
}

#[test]
fn remove_after_the_directory_is_gone_clears_the_branch() {
    each_backend(|b| {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        std::fs::remove_dir_all(&w.root).unwrap();
        ws.remove(&w).unwrap();
        assert!(branches(&f, repo.dir()).is_empty(), "{}", b.name);
        assert_eq!(worktrees(&f, repo.dir()).len(), 1);
    });
}

#[test]
fn a_forged_record_is_refused() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    let w = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    let before = tree_snapshot(&f.dir);
    let good = w.kind.clone();
    let WorkspaceKind::Worktree { clone, branch } = good else {
        panic!()
    };
    for kind in [
        // a repository of the user's, not a clone made here
        WorkspaceKind::Worktree {
            clone: f.dir.clone(),
            branch: branch.clone(),
        },
        // a branch that is not the attempt's
        WorkspaceKind::Worktree {
            clone: clone.clone(),
            branch: "main".into(),
        },
        WorkspaceKind::Worktree {
            clone: f.state.join("clones"),
            branch: branch.clone(),
        },
    ] {
        let forged = Workspace { kind, ..w.clone() };
        let e = ws.remove(&forged).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput, "{forged:?}");
    }
    assert_tree_unchanged(&f.dir, &before, "removal");
    assert!(w.root.exists());
    ws.remove(&w).unwrap();
}

#[test]
fn the_sweeper_clears_what_no_record_names() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    let ws = f.workspaces(&b.fw(&f));
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    // nothing there yet: nothing to do
    assert_eq!(ws.sweep_orphans(&|_| false).unwrap().removed.len(), 0);
    let kept = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    let lost = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    let plain = f
        .state
        .join("workspaces")
        .join(AttemptId::generate().as_str());
    std::fs::create_dir_all(plain.join("sub")).unwrap();
    // scratch directories carry a marker: only those of a gone owner go
    let mut ended = std::process::Command::new("/bin/sh")
        .args(["-c", "exit 0"])
        .spawn()
        .unwrap();
    let ended_start = match belt::platform::process_start_identity(ended.id()) {
        belt::platform::StartIdentity::Identity(s) | belt::platform::StartIdentity::Zombie(s) => s,
        other => panic!("{other:?}"),
    };
    let ended_pid = ended.id();
    ended.wait().unwrap();
    let marked = |dir: &std::path::Path, pid: u32, start: &str| {
        std::fs::create_dir_all(dir).unwrap();
        belt::platform::owned_temp::OwnedTemp::write_marker_of(dir, pid, start).unwrap();
    };
    let scratch = f.state.join("workspaces/.staging.abc");
    marked(&scratch, ended_pid, &ended_start);
    let half_clone = f.state.join("clones/.clone.xyz");
    marked(&half_clone, ended_pid, &ended_start);
    let me = std::process::id();
    let my_start = match belt::platform::process_start_identity(me) {
        belt::platform::StartIdentity::Identity(s) => s,
        other => panic!("{other:?}"),
    };
    let live_scratch = f.state.join("workspaces/.staging.live");
    marked(&live_scratch, me, &my_start);
    // without a marker (an older version, or a maker that could not say who
    // it was): removed, because the sweep runs where no writer can exist
    let unmarked_scratch = f.state.join("clones/.clone.unmarked");
    std::fs::create_dir_all(unmarked_scratch.join("c/.git")).unwrap();
    let unmarked_staging = f.state.join("workspaces/.staging.unmarked");
    std::fs::create_dir_all(&unmarked_staging).unwrap();
    // a marker that is there but says nothing readable: kept
    let garbled_scratch = f.state.join("clones/.clone.garbled");
    std::fs::create_dir_all(&garbled_scratch).unwrap();
    std::fs::write(garbled_scratch.join("owner"), "not a marker").unwrap();
    // a scratch directory that its maker was still building when it was
    // killed is under a name that says who made it: gone with a gone maker
    let building = |root: &str, pid: u32, start: &str| {
        let name = belt::platform::owned_temp::OwnedTemp::building_name_of(pid, start).unwrap();
        let dir = f.state.join(root).join(name);
        std::fs::create_dir_all(dir.join("d")).unwrap();
        dir
    };
    let half_built = [
        building("clones", ended_pid, &ended_start),
        building("workspaces", ended_pid, &ended_start),
    ];
    let being_built = building("clones", me, &my_start);
    let foreign = f.state.join("workspaces/notes.txt");
    std::fs::write(&foreign, "mine").unwrap();
    // a leftover that holds a directory it cannot read
    std::fs::create_dir_all(plain.join("locked")).unwrap();
    std::fs::set_permissions(
        plain.join("locked"),
        std::os::unix::fs::PermissionsExt::from_mode(0o000),
    )
    .unwrap();
    let kept_root = kept.root.clone();
    let report = ws
        .sweep_orphans(&|a| kept_root.ends_with(a.as_str()))
        .unwrap();
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    assert_eq!(report.removed.len(), 6, "{:?}", report.removed);
    assert!(kept.root.exists());
    assert!(foreign.exists() && live_scratch.exists() && garbled_scratch.exists());
    assert!(being_built.exists());
    for gone in &half_built {
        assert!(!gone.exists(), "{gone:?}");
    }
    for gone in [
        &lost.root,
        &plain,
        &scratch,
        &half_clone,
        &unmarked_scratch,
        &unmarked_staging,
    ] {
        assert!(!gone.exists(), "{gone:?}");
    }
    // the dead attempt's branch went with its worktree
    assert_eq!(branches(&f, repo.dir()).len(), 1);
    assert_eq!(worktrees(&f, repo.dir()).len(), 2);
    ws.remove(&kept).unwrap();
}

#[test]
fn a_directory_workspace_with_an_unreadable_directory_is_removed() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let plain = f.root.join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    std::fs::write(plain.join("x.txt"), "x\n").unwrap();
    let store = f.store();
    let c = capture(
        &f.env(),
        &store,
        &SourceRequest::Directory { dir: plain },
        &CaptureParams::default(),
    )
    .unwrap();
    let m = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
    let ws = f.workspaces(&b.fw(&f));
    let w = ws.create(&AttemptId::generate(), &store, &m, None).unwrap();
    let locked = w.root.join("run/locked");
    std::fs::create_dir_all(&locked).unwrap();
    std::fs::write(locked.join("f"), "f").unwrap();
    std::fs::set_permissions(&locked, std::os::unix::fs::PermissionsExt::from_mode(0o000)).unwrap();
    std::fs::set_permissions(
        w.root.join("run"),
        std::os::unix::fs::PermissionsExt::from_mode(0o500),
    )
    .unwrap();
    ws.remove(&w).unwrap();
    assert!(!w.root.exists());
}

#[test]
fn an_existing_clone_must_come_from_the_same_seed() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    ws.ensure_clone("github:example/repo", Some(f.dir.to_str().unwrap()))
        .unwrap();
    // the same place by another spelling is the same
    let spelled = format!("{}/./", f.dir.display());
    ws.ensure_clone("github:example/repo", Some(&spelled))
        .unwrap();
    let other = f.root.join("other");
    std::fs::create_dir_all(&other).unwrap();
    f.git(&other, &["init", "-q", "-b", "main"]);
    for seed in [Some(other.to_str().unwrap()), None] {
        let e = ws.ensure_clone("github:example/repo", seed).unwrap_err();
        assert_eq!(e.code, ErrorCode::Conflict, "{seed:?}");
    }
    // an empty clone is not a clone of a seed either
    ws.ensure_clone("github:example/empty", None).unwrap();
    let e = ws
        .ensure_clone("github:example/empty", Some(f.dir.to_str().unwrap()))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Conflict);
}

#[test]
fn in_place_wants_the_top_of_a_worktree_inside_which_the_cwd_stays() {
    let b = &backends(&common_home())[0];
    let f = fixture();
    let ws = f.workspaces(&b.fw(&f));
    // not a Git worktree
    let plain = f.root.join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let original = ws.open_in_place(&plain, Path::new(""), true).unwrap();
    ws.remove(&original).unwrap();
    assert!(plain.is_dir());
    // inside one, not its top
    let e = ws
        .open_in_place(&f.dir.join("pkg"), Path::new(""), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // the home directory (from the env map) is not a project
    let home = f.root.join("home");
    f.git(&home, &["init", "-q", "-b", "main"]);
    let e = ws.open_in_place(&home, Path::new(""), true).unwrap_err();
    assert!(e.message.contains("project directory"), "{}", e.message);
    // a working directory that is a link out of the worktree
    let outside = f.root.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, f.dir.join("escape")).unwrap();
    let e = ws
        .open_in_place(&f.dir, Path::new("escape"), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput);
    // a link that stays inside resolves
    std::os::unix::fs::symlink("pkg", f.dir.join("inside")).unwrap();
    let w = ws.open_in_place(&f.dir, Path::new("inside"), true).unwrap();
    assert_eq!(w.cwd, w.root.join("pkg"));
    // a file is not a working directory
    let e = ws
        .open_in_place(&f.dir, Path::new("a.txt"), true)
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
}

/// `fast-worktree add` for the fairness test: slow, as an `npm ci` is.
struct Slow(Arc<dyn FastWorktree>);

impl FastWorktree for Slow {
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure> {
        std::thread::sleep(std::time::Duration::from_millis(150));
        self.0.add(repo, dest, branch, start)
    }
    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        self.0.remove(repo, path)
    }
}

#[test]
fn adds_of_one_commit_do_not_starve_an_add_of_another() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let b = &backends(&common_home())[0];
    let f = fixture();
    let store = Arc::new(f.store());
    let first = f.git(&f.dir, &["rev-parse", "HEAD"]);
    let second = f.commit("second.txt", "two\n");
    let m1 = f.capture_commit(&store, &first);
    let m2 = f.capture_commit(&store, &second);
    {
        let ws = f.workspaces(&b.fw(&f));
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m1);
        f.deliver(&store, &repo, &m2);
        // the checkout starts at the first commit
        let w = ws
            .create(&AttemptId::generate(), &store, &m1, Some(&repo))
            .unwrap();
        ws.remove(&w).unwrap();
    }
    let slow: Arc<dyn FastWorktree> = Arc::new(Slow(b.fw(&f)));
    let stop = AtomicBool::new(false);
    let waited = std::thread::scope(|s| {
        for _ in 0..4 {
            let (f, store, slow, m1, stop) = (&f, &store, &slow, &m1, &stop);
            s.spawn(move || {
                let started = std::time::Instant::now();
                // The adds of the first commit go on until the other add is done. The
                // cap only ends them when it is starved; it is just above the 15 s the
                // assertion allows, so a starved add fails at once, not after 40 s.
                while !stop.load(Ordering::SeqCst) && started.elapsed().as_secs() < 17 {
                    let ws = Workspaces::new(f.state.clone(), f.env(), slow.clone());
                    let repo = f.clone_of(&ws);
                    let w = ws
                        .create(&AttemptId::generate(), store, m1, Some(&repo))
                        .unwrap();
                    ws.remove(&w).unwrap();
                }
            });
        }
        std::thread::sleep(std::time::Duration::from_millis(400));
        let ws = Workspaces::new(f.state.clone(), f.env(), slow.clone());
        let repo = f.clone_of(&ws);
        let t = std::time::Instant::now();
        let w = ws
            .create(&AttemptId::generate(), &store, &m2, Some(&repo))
            .unwrap();
        let waited = t.elapsed();
        stop.store(true, Ordering::SeqCst);
        assert!(w.root.join("second.txt").exists());
        ws.remove(&w).unwrap();
        waited
    });
    assert!(
        waited.as_secs() < 15,
        "an add of another commit waited {waited:?} behind adds of the first"
    );
}

// ---- the real command's wrapper, with scripts in its place -------------

fn script_env(f: &Fixture, bin: &Path) -> BTreeMap<String, String> {
    let mut e = f.env();
    e.insert("PATH".into(), format!("{}:/usr/bin:/bin", bin.display()));
    e
}

fn fake_tool(f: &Fixture, body: &str) -> (PathBuf, BTreeMap<String, String>) {
    let bin = f.root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let path = bin.join("fast-worktree");
    common::write_executable(&path, &format!("#!/bin/sh\n{body}\n"));
    let env = script_env(f, &bin);
    (bin, env)
}

#[test]
fn a_flood_of_output_is_kept_to_its_tail() {
    let f = fixture();
    let (_bin, env) = fake_tool(
        &f,
        "yes 0123456789 | head -c 30000000 >&2\necho THE-END-MARKER >&2\nexit 3",
    );
    let fw = CliFastWorktree::new(env);
    let started = std::time::Instant::now();
    let e = fw.add(&f.dir, &f.root.join("d"), "b", "c").unwrap_err();
    let FwFailure::Failed { code, stderr } = e else {
        panic!("{e:?}")
    };
    assert_eq!(code, Some(3));
    assert!(stderr.len() < 200_000, "{} bytes kept", stderr.len());
    assert!(stderr.contains("THE-END-MARKER"));
    assert!(stderr.contains("earlier output dropped"));
    assert!(started.elapsed().as_secs() < 60);
}

fn alive(pid: &str) -> bool {
    Command::new("kill")
        .args(["-0", pid])
        .output()
        .unwrap()
        .status
        .success()
}

#[test]
fn a_hook_that_never_ends_is_killed_with_its_children() {
    let f = fixture();
    let pidfile = f.root.join("child.pid");
    let (_bin, env) = fake_tool(
        &f,
        &crate::fixture_process::script(&format!(
            "echo starting >&2\nwhile [ -d '{guard}' ]; do sleep 0.05; done &\necho $! > '{}'\nwait",
            pidfile.display(),
            guard = f.root.display()
        )),
    );
    // Allow the real shell and descendant to start under instrumented CI.
    // The hook still cannot complete, so timeout and group cleanup are required.
    let fw = CliFastWorktree::with_timeout(env, std::time::Duration::from_secs(10));
    let started = std::time::Instant::now();
    let e = fw.add(&f.dir, &f.root.join("d"), "b", "c").unwrap_err();
    assert!(started.elapsed().as_secs() < 30);
    let FwFailure::TimedOut { secs, stderr } = e else {
        panic!("{e:?}")
    };
    assert_eq!(secs, 10);
    assert!(stderr.contains("starting"));
    let pid = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .to_string();
    for _ in 0..100 {
        if !alive(&pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("the hook's child {pid} is still running");
}

#[test]
fn an_environment_without_home_or_path_is_named() {
    let f = fixture();
    for drop_key in ["HOME", "PATH"] {
        let mut env = f.env();
        env.remove(drop_key);
        let fw = CliFastWorktree::new(env);
        let e = fw.remove(&f.dir, &f.root.join("d")).unwrap_err();
        assert_eq!(
            e,
            FwFailure::NoEnv {
                missing: vec![drop_key.to_string()]
            }
        );
    }
}

#[test]
fn a_tool_that_succeeds_and_one_that_writes_only_to_stdout() {
    let f = fixture();
    let (_bin, env) = fake_tool(&f, "echo fine");
    assert!(CliFastWorktree::new(env).remove(&f.dir, &f.root).is_ok());
    let (_bin, env) = fake_tool(&f, "echo only-on-stdout\nexit 2");
    let e = CliFastWorktree::new(env)
        .remove(&f.dir, &f.root)
        .unwrap_err();
    assert_eq!(
        e,
        FwFailure::Failed {
            code: Some(2),
            stderr: "only-on-stdout\n".into()
        }
    );
    // a directory that does not exist is not "not installed"
    let (_bin, env) = fake_tool(&f, "exit 0");
    let e = CliFastWorktree::new(env)
        .remove(&f.root.join("nope"), &f.root)
        .unwrap_err();
    assert!(matches!(e, FwFailure::Failed { code: None, .. }), "{e:?}");
}

/// The stand-in `GitFw` with a remove that can be made to fail and a record of
/// the port releases asked for.
struct PortSpy {
    inner: GitFw,
    remove_fails: bool,
    release: Result<(), FwFailure>,
    released: std::sync::Mutex<Vec<PathBuf>>,
}

impl FastWorktree for PortSpy {
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure> {
        self.inner.add(repo, dest, branch, start)
    }
    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        if self.remove_fails {
            return Err(FwFailure::Failed {
                code: Some(1),
                stderr: "stuck".into(),
            });
        }
        self.inner.remove(repo, path)
    }
    fn release_ports(&self, _: &Path, path: &Path) -> Result<(), FwFailure> {
        self.released.lock().unwrap().push(path.to_path_buf());
        self.release.clone()
    }
}

#[test]
fn ports_are_released_only_when_the_worktree_is_really_gone() {
    for remove_fails in [true, false] {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let spy = Arc::new(PortSpy {
            inner: GitFw { env: f.env() },
            remove_fails,
            release: Ok(()),
            released: Default::default(),
        });
        let fw: Arc<dyn FastWorktree> = spy.clone();
        let ws = f.workspaces(&fw);
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        let result = ws.remove(&w);
        let released = spy.released.lock().unwrap().clone();
        if remove_fails {
            assert!(result.is_err());
            assert!(w.root.exists(), "the worktree is still there");
            assert!(released.is_empty(), "ports kept: {released:?}");
        } else {
            result.unwrap();
            assert!(!w.root.exists());
            assert_eq!(released.len(), 1);
        }
    }
}

#[test]
fn a_registry_the_embedded_portool_cannot_read_blocks_the_release_and_other_failures_keep_their_cause(
) {
    for (release, reason) in [
        (
            FwFailure::IncompatibleRegistry {
                message: "newer registry".into(),
            },
            "newer registry",
        ),
        (
            FwFailure::Failed {
                code: None,
                stderr: "disk gone".into(),
            },
            "disk gone",
        ),
    ] {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let fw: Arc<dyn FastWorktree> = Arc::new(PortSpy {
            inner: GitFw { env: f.env() },
            remove_fails: false,
            release: Err(release),
            released: Default::default(),
        });
        let ws = f.workspaces(&fw);
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        let e = ws.remove(&w).unwrap_err();
        assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
        assert!(e.message.contains(reason), "{}", e.message);
    }
}

#[test]
fn releasing_ports_refuses_a_newer_registry_without_rewriting_it() {
    let f = fixture();
    let state = f.root.join("xdg-state");
    let ledger = state.join("portool/registry.json");
    std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    let body = r#"{"version":999}"#;
    std::fs::write(&ledger, body).unwrap();
    let mut env = f.env();
    env.insert("XDG_STATE_HOME".into(), state.to_str().unwrap().into());
    let fw = CliFastWorktree::new(env);
    let err = fw
        .release_ports(&f.dir, &f.root.join("gone-worktree"))
        .unwrap_err();
    assert!(
        matches!(&err, FwFailure::IncompatibleRegistry { message } if message.contains("v999")),
        "{err:?}"
    );
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), body);
}

#[test]
fn ports_are_released_when_someone_else_removed_the_directory_or_a_link_is_there() {
    for link in [false, true] {
        let f = fixture();
        let store = f.store();
        let m = f.capture_commit(&store, "HEAD");
        let spy = Arc::new(PortSpy {
            inner: GitFw { env: f.env() },
            remove_fails: false,
            release: Ok(()),
            released: Default::default(),
        });
        let fw: Arc<dyn FastWorktree> = spy.clone();
        let ws = f.workspaces(&fw);
        let repo = f.clone_of(&ws);
        f.deliver(&store, &repo, &m);
        let w = ws
            .create(&AttemptId::generate(), &store, &m, Some(&repo))
            .unwrap();
        std::fs::remove_dir_all(&w.root).unwrap();
        if link {
            std::os::unix::fs::symlink(&f.dir, &w.root).unwrap();
        }
        ws.remove(&w).unwrap();
        assert!(std::fs::symlink_metadata(&w.root).is_err());
        assert_eq!(
            spy.released.lock().unwrap().clone(),
            vec![w.root.clone()],
            "link: {link}"
        );
        assert!(branches(&f, repo.dir()).is_empty());
    }
}

/// The ports go through the embedded portool, the worktree through Git.
struct NewerRegistry {
    inner: GitFw,
    ports: CliFastWorktree,
}

impl FastWorktree for NewerRegistry {
    fn add(&self, repo: &Path, dest: &Path, branch: &str, start: &str) -> Result<(), FwFailure> {
        self.inner.add(repo, dest, branch, start)
    }
    fn remove(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        self.inner.remove(repo, path)
    }
    fn release_ports(&self, repo: &Path, path: &Path) -> Result<(), FwFailure> {
        self.ports.release_ports(repo, path)
    }
}

#[test]
fn a_registry_that_cannot_be_read_still_lets_the_gone_worktrees_branch_be_cleared() {
    let f = fixture();
    let state = f.root.join("xdg-state");
    let ledger = state.join("portool/registry.json");
    std::fs::create_dir_all(ledger.parent().unwrap()).unwrap();
    let body = r#"{"version":999}"#;
    std::fs::write(&ledger, body).unwrap();
    let mut env = f.env();
    env.insert("XDG_STATE_HOME".into(), state.to_str().unwrap().into());
    let store = f.store();
    let m = f.capture_commit(&store, "HEAD");
    let fw: Arc<dyn FastWorktree> = Arc::new(NewerRegistry {
        inner: GitFw { env: f.env() },
        ports: CliFastWorktree::new(env),
    });
    let ws = f.workspaces(&fw);
    let repo = f.clone_of(&ws);
    f.deliver(&store, &repo, &m);
    let w = ws
        .create(&AttemptId::generate(), &store, &m, Some(&repo))
        .unwrap();
    std::fs::remove_dir_all(&w.root).unwrap();
    let e = ws.remove(&w).unwrap_err();
    assert_eq!(e.blocked_reason, Some(BlockedReason::EnvironmentMismatch));
    assert!(e.message.contains("v999"), "{}", e.message);
    assert_eq!(std::fs::read_to_string(&ledger).unwrap(), body);
    assert!(branches(&f, repo.dir()).is_empty());
    assert_eq!(worktrees(&f, repo.dir()).len(), 1);
}

/// Spec 25.8 5b (e) step 4, (g)8: the refs of a detached source go from
/// the clone only under its import lock taken without waiting; while an
/// import holds it, nothing is removed. The clone directory stays ((g)19).
#[test]
fn imported_refs_go_only_under_a_free_import_lock() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let head = f.commit("a", "a");
    let repo = f.clone_of(&ws);
    f.git(repo.dir(), &["fetch", "-q", f.dir.to_str().unwrap(), &head]);
    let source = format!("src-{}", "a".repeat(32));
    let snapshot = format!("refs/local-ci/snapshots/{source}");
    f.git(repo.dir(), &["update-ref", &snapshot, &head]);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let held = ws.import_lock("github:example/repo").unwrap();
    held.lock().unwrap();
    // (released by `LOCK_UN` on drop, not by the last close: a child a
    // parallel test forks meanwhile does not keep it held)
    let held = belt::platform::flock::HeldLock::new(held);
    let done = std::cell::Cell::new(0);
    let drop_refs = |clone: &str| {
        ws.drop_imported_refs(
            clone,
            &source,
            &|| Ok(false),
            &|| {
                done.set(done.get() + 1);
                Ok(())
            },
            &|| false,
        )
    };
    assert!(!drop_refs(&clone).unwrap());
    assert_eq!(done.get(), 0);
    assert_eq!(f.git(repo.dir(), &["rev-parse", &snapshot]), head);
    drop(held);
    assert!(drop_refs(&clone).unwrap(), "the import lock stayed busy");
    assert_eq!(done.get(), 1);
    assert!(f
        .git(repo.dir(), &["for-each-ref", "refs/local-ci/"])
        .is_empty());
    assert!(repo.dir().is_dir());
    // a name that is not a clone's is refused
    assert!(drop_refs("../x").is_err());
}

/// Review round 1 (M2): step 4's git runs with hooks off: a hook the clone
/// has (`reference-transaction`, which `update-ref -d` runs) does not run;
/// a source received again keeps its refs and only its row goes.
#[test]
fn imported_refs_go_without_the_clones_hooks() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let head = f.commit("a", "a");
    let repo = f.clone_of(&ws);
    f.git(repo.dir(), &["fetch", "-q", f.dir.to_str().unwrap(), &head]);
    let source = format!("src-{}", "b".repeat(32));
    let snapshot = format!("refs/local-ci/snapshots/{source}");
    f.git(repo.dir(), &["update-ref", &snapshot, &head]);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let hooks = repo.dir().join(".git").join("hooks");
    std::fs::create_dir_all(&hooks).unwrap();
    let ran = repo.dir().join("hook-ran");
    let hook = hooks.join("reference-transaction");
    std::fs::write(&hook, format!("#!/bin/sh\ntouch '{}'\n", ran.display())).unwrap();
    let mut perm = std::fs::metadata(&hook).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
    std::fs::set_permissions(&hook, perm).unwrap();
    // (the hook does run for git as usual)
    f.git(repo.dir(), &["update-ref", "refs/heads/probe", &head]);
    assert!(ran.exists());
    std::fs::remove_file(&ran).unwrap();
    // received again: the refs stay, the row goes
    let done = std::cell::Cell::new(0);
    let run = |held: bool| {
        let ok = ws
            .drop_imported_refs(
                &clone,
                &source,
                &|| Ok(held),
                &|| {
                    done.set(done.get() + 1);
                    Ok(())
                },
                &|| false,
            )
            .unwrap();
        assert!(ok, "the import lock stayed busy");
    };
    run(true);
    assert_eq!(done.get(), 1);
    assert_eq!(f.git(repo.dir(), &["rev-parse", &snapshot]), head);
    run(false);
    assert_eq!(done.get(), 2);
    assert!(f
        .git(repo.dir(), &["for-each-ref", "refs/local-ci/"])
        .is_empty());
    assert!(!ran.exists(), "a hook of the clone ran");
}

/// Review round 1 (L1): a clone directory without its own `.git` directory
/// is an error (git never looks above it), and the row stays.
#[test]
fn imported_refs_need_the_clones_own_git_directory() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let head = f.commit("a", "a");
    let repo = f.clone_of(&ws);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let source = format!("src-{}", "c".repeat(32));
    // the clone's .git replaced by a file: refused, nothing done
    let git = repo.dir().join(".git");
    let kept = repo.dir().join("git-kept");
    std::fs::rename(&git, &kept).unwrap();
    std::fs::write(&git, "gitdir: elsewhere\n").unwrap();
    let done = std::cell::Cell::new(0);
    let r = ws.drop_imported_refs(
        &clone,
        &source,
        &|| Ok(false),
        &|| {
            done.set(done.get() + 1);
            Ok(())
        },
        &|| false,
    );
    assert!(r.is_err());
    assert_eq!(done.get(), 0);
    // a clone that cannot be looked at (not "absent") is an error too
    std::fs::remove_file(&git).unwrap();
    std::fs::rename(&kept, &git).unwrap();
    let clones = repo.dir().parent().unwrap().to_path_buf();
    let mode = |m: u32| {
        let mut perm = std::fs::metadata(&clones).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perm, m);
        std::fs::set_permissions(&clones, perm).unwrap();
    };
    mode(0o000);
    let r = ws.drop_imported_refs(
        &clone,
        &source,
        &|| Ok(false),
        &|| {
            done.set(done.get() + 1);
            Ok(())
        },
        &|| false,
    );
    mode(0o700);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(done.get(), 0);
    let _ = head;
}

/// Review round 2 (M-b): the pass stops while a step 4 git runs slowly:
/// the git is ended at once and the row stays (`false`, no `done`).
#[test]
fn imported_refs_stop_ends_a_slow_git_and_keeps_the_row() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let repo = f.clone_of(&ws);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let bin = f.root.join("slow-bin");
    std::fs::create_dir_all(&bin).unwrap();
    let git = bin.join("git");
    std::fs::write(&git, "#!/bin/sh\nsleep 30 &\nwait\n").unwrap();
    let mut perm = std::fs::metadata(&git).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
    std::fs::set_permissions(&git, perm).unwrap();
    let mut env = f.env();
    let path = format!("{}:{}", bin.display(), env["PATH"]);
    env.insert("PATH".into(), path);
    let slow = Workspaces::new(f.state.clone(), env, fw.clone());
    let source = format!("src-{}", "d".repeat(32));
    let start = std::time::Instant::now();
    let stop = || start.elapsed() > std::time::Duration::from_millis(200);
    let done = std::cell::Cell::new(0);
    let r = slow
        .drop_imported_refs(
            &clone,
            &source,
            &|| Ok(false),
            &|| {
                done.set(done.get() + 1);
                Ok(())
            },
            &stop,
        )
        .unwrap();
    assert!(!r);
    assert_eq!(done.get(), 0);
    assert!(
        start.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
    // (review round 3, M-1: nothing the stopped row leaves blocks a later
    // `clone_ref` writer: the import lock is free again)
    assert!(
        ws.try_import_lock(&clone).unwrap().is_some(),
        "the import lock stayed held"
    );
}

/// Review round 3 (M-1): a publication of a source whose `ref_drop:` row
/// exists races step 4. The publication writes its `clone_ref` row under
/// the clone's import lock, only when the refs it counts on are there; it
/// comes right after step 4 has checked `clone_ref` (inside `held`). Under
/// the lock it waits for step 4 to end: the refs go, the row goes, and the
/// publication, finding no refs, writes no `clone_ref` row (it is
/// delivered again). Never a `clone_ref` row whose refs are gone.
#[test]
fn a_publication_racing_step_4_never_keeps_a_row_without_its_refs() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = Arc::new(f.workspaces(&fw));
    let head = f.commit("a", "a");
    let repo = f.clone_of(&ws);
    f.git(repo.dir(), &["fetch", "-q", f.dir.to_str().unwrap(), &head]);
    let source = format!("src-{}", "f".repeat(32));
    let snapshot = format!("refs/local-ci/snapshots/{source}");
    f.git(repo.dir(), &["update-ref", &snapshot, &head]);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let clone_ref = Arc::new(AtomicBool::new(false));
    let ref_drop = std::cell::Cell::new(true);
    let dir = repo.dir().to_path_buf();
    let publish = {
        let (ws, clone, clone_ref, dir, snapshot) = (
            ws.clone(),
            clone.clone(),
            clone_ref.clone(),
            dir.clone(),
            snapshot.clone(),
        );
        move || {
            // (the publication's bounded wait for the import lock)
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let _lock = loop {
                if let Some(lock) = ws.try_import_lock(&clone).unwrap() {
                    break lock;
                }
                assert!(std::time::Instant::now() < deadline, "lock never free");
                std::thread::sleep(std::time::Duration::from_millis(5));
            };
            let there = Command::new("git")
                .args([
                    "-C",
                    dir.to_str().unwrap(),
                    "rev-parse",
                    "-q",
                    "--verify",
                    &snapshot,
                ])
                .output()
                .unwrap()
                .status
                .success();
            if there {
                clone_ref.store(true, Ordering::SeqCst);
            }
        }
    };
    let publisher = std::cell::RefCell::new(None);
    let held = || -> anyhow::Result<bool> {
        let seen = clone_ref.load(Ordering::SeqCst);
        // the publication starts right after the check, and has 300 ms
        // to finish before step 4 goes on
        let t = std::thread::spawn(publish.clone());
        let until = std::time::Instant::now() + std::time::Duration::from_millis(300);
        while !t.is_finished() && std::time::Instant::now() < until {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        *publisher.borrow_mut() = Some(t);
        Ok(seen)
    };
    let ok = ws
        .drop_imported_refs(
            &clone,
            &source,
            &held,
            &|| {
                ref_drop.set(false);
                Ok(())
            },
            &|| false,
        )
        .unwrap();
    assert!(ok);
    publisher.borrow_mut().take().unwrap().join().unwrap();
    let refs_there = !f.git(repo.dir(), &["for-each-ref", &snapshot]).is_empty();
    let row = clone_ref.load(Ordering::SeqCst);
    assert!(
        (row && refs_there) || (!row && (ref_drop.get() || !refs_there)),
        "clone_ref {row}, refs {refs_there}, ref_drop {}",
        ref_drop.get()
    );
    assert!(!row || refs_there, "a clone_ref row whose refs are gone");
}

/// Review round 2 (L-c): a clone name that is a symlink (to a real clone)
/// is refused, and the row stays.
#[test]
fn imported_refs_refuse_a_clone_that_is_a_symlink() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let repo = f.clone_of(&ws);
    let clones = repo.dir().parent().unwrap().to_path_buf();
    let link = "r-0123456789abcdef";
    std::os::unix::fs::symlink(repo.dir(), clones.join(link)).unwrap();
    let source = format!("src-{}", "e".repeat(32));
    let done = std::cell::Cell::new(0);
    let r = ws.drop_imported_refs(
        link,
        &source,
        &|| Ok(false),
        &|| {
            done.set(done.get() + 1);
            Ok(())
        },
        &|| false,
    );
    assert!(r.is_err(), "{r:?}");
    assert_eq!(done.get(), 0);
}

/// Spec 25.8 5b (e): the gc of a clone runs `git gc --prune=14.days.ago`
/// without packing refs: a loose object no ref reaches that is older than
/// 14 days goes, a newer one stays (a workspace may be about to point a ref
/// at it), the refs stay loose, a stale `.lock` file is left alone and the
/// clone directory stays ((g)19). A busy import lock skips it, the clone's
/// own lock and its main lock do not (review round 1, M2); a stop ends it;
/// a clone that is not there has nothing to do.
#[test]
fn the_clone_gc_prunes_only_old_unreachable_objects_under_free_locks() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let head = f.commit("a", "a");
    let repo = f.clone_of(&ws);
    f.git(repo.dir(), &["fetch", "-q", f.dir.to_str().unwrap(), &head]);
    let snapshot = format!("refs/local-ci/snapshots/src-{}", "d".repeat(32));
    f.git(repo.dir(), &["update-ref", &snapshot, &head]);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let git_dir = repo.dir().join(".git");
    let object = |text: &str, old: bool| {
        let file = repo.dir().join("blob-input");
        std::fs::write(&file, text).unwrap();
        let id = f.git(repo.dir(), &["hash-object", "-w", file.to_str().unwrap()]);
        std::fs::remove_file(&file).unwrap();
        let path = git_dir.join("objects").join(&id[..2]).join(&id[2..]);
        if old {
            let month = std::time::SystemTime::now() - std::time::Duration::from_secs(30 * 86_400);
            std::fs::File::open(&path)
                .unwrap()
                .set_modified(month)
                .unwrap();
        }
        (id, path)
    };
    let present = |id: &str| {
        std::process::Command::new("git")
            .args(["cat-file", "-e", id])
            .current_dir(repo.dir())
            .envs(f.env())
            .status()
            .unwrap()
            .success()
    };
    let old = object("old and unreachable", true);
    let fresh = object("fresh and unreachable", false);
    let stale = git_dir.join("refs/local-ci/snapshots/stale.lock");
    std::fs::write(&stale, "").unwrap();
    let gc = |stop: &dyn Fn() -> bool| ws.gc_clone(&clone, stop);
    // busy: the import lock skips it (review round 1, M2: it is the only
    // lock the gc takes)
    let clones = repo.dir().parent().unwrap().to_path_buf();
    let held = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(clones.join(format!("{clone}.import.lock")))
        .unwrap();
    held.lock().unwrap();
    assert_eq!(gc(&|| false).unwrap(), CloneGc::Busy);
    held.unlock().unwrap();
    assert!(old.1.exists());
    // stopped at once: nothing of it is left running
    assert_eq!(gc(&|| true).unwrap(), CloneGc::Stopped);
    // (the clone's own lock and its main lock, held shared as
    // fast-worktree does, do not keep it from running)
    let own = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(clones.join(format!("{clone}.lock")))
        .unwrap();
    own.lock().unwrap();
    let shared = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(git_dir.join("belt-workspace.lock"))
        .unwrap();
    shared.lock_shared().unwrap();
    assert_eq!(gc(&|| false).unwrap(), CloneGc::Ran);
    drop((own, shared));
    // (a newer one may have moved into a cruft pack: asked by id)
    assert!(!present(&old.0), "an object older than 14 days stayed");
    assert!(present(&fresh.0), "an object newer than 14 days went");
    assert_eq!(f.git(repo.dir(), &["rev-parse", &snapshot]), head);
    assert!(git_dir.join(&snapshot).is_file(), "the refs were packed");
    assert!(stale.exists(), "a lock file was removed");
    assert!(repo.dir().is_dir());
    // a clone that is not there: nothing to do; a bad name is refused
    assert_eq!(
        ws.gc_clone(&format!("r-{}", "0".repeat(16)), &|| false)
            .unwrap(),
        CloneGc::Ran
    );
    assert!(ws.gc_clone("../x", &|| false).is_err());
}

/// Whether `id` is in the repository `dir`.
fn has_object(f: &Fixture, dir: &Path, id: &str) -> bool {
    Command::new("git")
        .args(["cat-file", "-e", id])
        .current_dir(dir)
        .env_clear()
        .envs(f.env())
        .status()
        .unwrap()
        .success()
}

/// The loose object `id` of the repository whose Git directory is
/// `git_dir`, made 30 days old.
fn age_object(git_dir: &Path, id: &str) {
    let path = git_dir.join("objects").join(&id[..2]).join(&id[2..]);
    let month = std::time::SystemTime::now() - std::time::Duration::from_secs(30 * 86_400);
    std::fs::File::open(&path)
        .unwrap()
        .set_modified(month)
        .unwrap();
}

/// Review round 1, M1 and L8: belt's clone writes no reflog of the main
/// checkout, and the gc expires that reflog first, so a commit only an old
/// checkout reached goes; the reflog of a linked worktree is not touched.
/// An old commit and tree a ref reaches, and a blob only a linked
/// worktree's index holds, stay. A clone with `extensions.preciousObjects`
/// or a `gc.recentObjectsHook` is not collected.
#[test]
fn the_clone_gc_takes_what_only_the_main_reflog_reached_and_keeps_the_rest() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let ws = f.workspaces(&fw);
    let head = f.commit("a", "a");
    let repo = f.clone_of(&ws);
    let dir = repo.dir().to_path_buf();
    let git_dir = dir.join(".git");
    assert!(
        !git_dir.join("logs/HEAD").exists(),
        "the clone wrote a reflog of its HEAD"
    );
    let clone = dir.file_name().unwrap().to_str().unwrap().to_string();
    let tree = f.git(&dir, &["rev-parse", &format!("{head}^{{tree}}")]);
    // a commit an old checkout reached (the user's git keeps a reflog)
    f.git(&dir, &["config", "core.logAllRefUpdates", "true"]);
    let lost = f.git(&dir, &["commit-tree", "-m", "lost", &tree]);
    f.git(&dir, &["checkout", "-q", "--detach", &lost]);
    f.git(&dir, &["checkout", "-q", "--detach", &head]);
    age_object(&git_dir, &lost);
    // an old commit, tree and blob a ref reaches
    std::fs::write(dir.join("kept-input"), "kept by a ref").unwrap();
    let blob = f.git(&dir, &["hash-object", "-w", "kept-input"]);
    std::fs::remove_file(dir.join("kept-input")).unwrap();
    let kept_tree = {
        let out = Command::new("git")
            .args(["mktree"])
            .current_dir(&dir)
            .env_clear()
            .envs(f.env())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write as _;
        let mut out = out;
        writeln!(out.stdin.take().unwrap(), "100644 blob {blob}\tkept").unwrap();
        let o = out.wait_with_output().unwrap();
        String::from_utf8(o.stdout).unwrap().trim().to_string()
    };
    let kept = f.git(&dir, &["commit-tree", "-m", "kept", &kept_tree]);
    let snapshot = format!("refs/local-ci/snapshots/src-{}", "e".repeat(32));
    f.git(&dir, &["update-ref", &snapshot, &kept]);
    for id in [&blob, &kept_tree, &kept] {
        age_object(&git_dir, id);
    }
    // a linked worktree: its own reflog, and a blob only its index holds
    let wt = f.root.join("linked");
    f.git(
        &dir,
        &[
            "worktree",
            "add",
            "-q",
            "--detach",
            wt.to_str().unwrap(),
            &head,
        ],
    );
    let wt_lost = f.git(&wt, &["commit-tree", "-m", "wt", &tree]);
    f.git(&wt, &["checkout", "-q", "--detach", &wt_lost]);
    f.git(&wt, &["checkout", "-q", "--detach", &head]);
    std::fs::write(wt.join("staged"), "only in the index").unwrap();
    f.git(&wt, &["add", "staged"]);
    let staged = f.git(&wt, &["rev-parse", ":staged"]);
    age_object(&git_dir, &staged);
    let wt_log = git_dir.join("worktrees/linked/logs/HEAD");
    let wt_before = std::fs::read_to_string(&wt_log).unwrap();
    assert!(wt_before.contains(&wt_lost));
    // precious: nothing goes
    f.git(&dir, &["config", "extensions.preciousObjects", "true"]);
    assert_eq!(ws.gc_clone(&clone, &|| false).unwrap(), CloneGc::Refused);
    assert!(has_object(&f, &dir, &lost));
    f.git(&dir, &["config", "--unset", "extensions.preciousObjects"]);
    // nor one with a hook that would keep objects (it is not run)
    f.git(&dir, &["config", "gc.recentObjectsHook", "false"]);
    assert_eq!(ws.gc_clone(&clone, &|| false).unwrap(), CloneGc::Refused);
    f.git(&dir, &["config", "--unset", "gc.recentObjectsHook"]);
    assert_eq!(ws.gc_clone(&clone, &|| false).unwrap(), CloneGc::Ran);
    assert!(
        !has_object(&f, &dir, &lost),
        "a commit only the main HEAD's reflog reached stayed"
    );
    for id in [&blob, &kept_tree, &kept, &staged, &head] {
        assert!(has_object(&f, &dir, id), "{id} went");
    }
    assert!(
        has_object(&f, &dir, &wt_lost),
        "the linked reflog was expired"
    );
    assert_eq!(std::fs::read_to_string(&wt_log).unwrap(), wt_before);
    // the main lock busy (an attempt's setup holds it): the gc runs, the
    // main reflog is left for the next one
    let later = f.git(&dir, &["commit-tree", "-m", "later", &tree]);
    f.git(&dir, &["checkout", "-q", "--detach", &later]);
    f.git(&dir, &["checkout", "-q", "--detach", &head]);
    let shared = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(git_dir.join("belt-workspace.lock"))
        .unwrap();
    shared.lock_shared().unwrap();
    assert_eq!(ws.gc_clone(&clone, &|| false).unwrap(), CloneGc::Ran);
    let log = std::fs::read_to_string(git_dir.join("logs/HEAD")).unwrap();
    assert!(
        log.contains(&later),
        "the reflog was expired under a busy lock"
    );
}

/// Review round 1, M2: while the gc runs (a slow git through `PATH`), the
/// same repository's `ensure_clone` and an attempt's workspace, which moves
/// the clone's checkout, do not wait for it (the slow git waits, bounded,
/// for the test to let it go). That checkout writes no reflog (M1).
#[test]
fn the_clone_gc_stalls_no_setup() {
    let f = fixture();
    let fw: Arc<dyn FastWorktree> = Arc::new(CliFastWorktree::new(f.env()));
    let marks = f.root.join("slow-git");
    std::fs::create_dir_all(&marks).unwrap();
    let real = String::from_utf8(
        Command::new("sh")
            .args(["-c", "command -v git"])
            .env_clear()
            .envs(f.env())
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    let m = marks.to_str().unwrap();
    std::fs::write(
        marks.join("git"),
        format!(
            "#!/bin/sh\nfor a in \"$@\"; do\n  if [ \"$a\" = gc ]; then\n    : > '{m}/started'\n    i=0\n    while [ ! -e '{m}/release' ] && [ $i -lt 600 ]; do sleep 0.1; i=$((i+1)); done\n    '{real}' \"$@\"\n    s=$?\n    : > '{m}/done'\n    exit $s\n  fi\ndone\nexec '{real}' \"$@\"\n"
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(marks.join("git"), std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut env = f.env();
    let path = format!("{m}:{}", env["PATH"]);
    env.insert("PATH".into(), path);
    let ws = Workspaces::new(f.state.clone(), env, fw.clone());
    let store = f.store();
    f.commit("a", "a");
    let repo = f.clone_of(&ws);
    // a later commit: the attempt moves the clone's checkout to it
    f.commit("b", "b");
    let c = capture(
        &f.env(),
        &store,
        &SourceRequest::Commit {
            dir: f.dir.clone(),
            rev: "HEAD".into(),
        },
        &CaptureParams::default(),
    )
    .unwrap();
    let manifest = store.manifest(&c.manifest.ci_source_id).unwrap().unwrap();
    f.deliver(&store, &repo, &manifest);
    let clone = repo
        .dir()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    std::thread::scope(|s| {
        let gc = s.spawn(|| ws.gc_clone(&clone, &|| false));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !marks.join("started").exists() {
            if gc.is_finished() {
                panic!("the gc ended before its git started: {:?}", gc.join());
            }
            assert!(std::time::Instant::now() < deadline, "the gc did not start");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        ws.ensure_clone("github:example/repo", Some(f.dir.to_str().unwrap()))
            .unwrap();
        let w = ws
            .create(&AttemptId::generate(), &store, &manifest, Some(&repo))
            .unwrap();
        assert!(!marks.join("done").exists(), "the setup waited for the gc");
        std::fs::write(marks.join("release"), "").unwrap();
        // (M1: belt's checkout writes no reflog of the main HEAD)
        assert!(!repo.dir().join(".git/logs/HEAD").exists());
        assert_eq!(
            f.git(repo.dir(), &["rev-parse", "HEAD"]),
            manifest.execution_commit.clone().unwrap()
        );
        assert_eq!(gc.join().unwrap().unwrap(), CloneGc::Ran);
        ws.remove(&w).unwrap();
    });
}
