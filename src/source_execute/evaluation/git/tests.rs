use super::*;
use crate::{Quiet, Target};
use std::{cell::RefCell, os::unix::fs::PermissionsExt, process::Command};

type SpawnObserver = Box<dyn FnMut(&Child)>;
thread_local! { static SPAWN: RefCell<Option<SpawnObserver>> = RefCell::new(None); }
pub(super) fn observe_spawn(child: &Child) {
    SPAWN.with(|slot| {
        if let Some(f) = &mut *slot.borrow_mut() {
            f(child);
        }
    });
}
const WORKFLOW: &str = "on: push\njobs:\n  check:\n    runs-on: [self-hosted, linux]\n    steps:\n      - run: echo fixture\n\n# raw trailing bytes  \n\n";

#[allow(clippy::disallowed_methods)]
fn git(root: &Path, args: &[&str]) -> String {
    let out = Command::new("/usr/bin/git")
        .args([
            "--literal-pathspecs",
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim_end().into()
}
struct Fixture {
    area: tempfile::TempDir,
    ctx: ExecContext,
    plan: FrozenPlan,
}
impl Fixture {
    fn new() -> Self {
        let area = tempfile::Builder::new()
            .prefix("localci-git-evaluation-")
            .tempdir_in(std::fs::canonicalize(std::env::temp_dir()).unwrap())
            .unwrap();
        std::fs::create_dir_all(area.path().join(".github/workflows")).unwrap();
        std::fs::write(area.path().join(".github/workflows/ci.yml"), WORKFLOW).unwrap();
        git(area.path(), &["init", "-q", "-b", "main"]);
        git(area.path(), &["add", "."]);
        git(area.path(), &["commit", "-qm", "fixture"]);
        let ctx = ExecContext::new(
            [
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), area.path().display().to_string()),
            ]
            .into(),
        );
        let plan = crate::build_frozen_plan(
            &ctx,
            &Target {
                dir: area.path().into(),
                sha: "HEAD".into(),
                base: Some("HEAD".into()),
                all: true,
                include_disabled: true,
                workflows_dirs: vec![],
                workflows: vec![],
                jobs: vec![],
            },
            &mut Quiet,
        )
        .unwrap();
        Self { area, ctx, plan }
    }
    fn root(&self) -> &Path {
        self.area.path()
    }
    fn source(&self) -> VerifiedEvaluationSource {
        git_evaluation_source(&self.ctx, self.root(), &self.plan).unwrap()
    }
    fn retarget(&mut self, commit: String) {
        self.plan.source.execution_commit = Some(commit.clone());
        self.plan.source.source_id = format!("commit:{commit}");
        self.plan.digest = self.plan.compute_digest().unwrap();
    }
}

#[test]
fn fixed_git_commit_preserves_raw_bytes_facts_and_rejects_forged_records() {
    let f = Fixture::new();
    let commit = f.plan.source.execution_commit.clone().unwrap();
    let first = f.source();
    let record = first.record();
    assert_eq!(
        record.binding(),
        &EvaluationBinding::Git {
            execution_commit: commit.clone()
        }
    );
    assert_eq!(
        record.workflows()[0].object_hash(),
        format!("{:x}", Sha256::digest(WORKFLOW.as_bytes()))
    );
    assert_eq!(
        record.workflows()[0].jobs()[0].runs_on(),
        Some(&serde_json::json!(["self-hosted", "linux"]))
    );
    let text = serde_json::to_string(record).unwrap();
    let decoded = EvaluationSource::from_json(&text).unwrap();
    assert_eq!(
        verify_git_evaluation_source(&f.ctx, f.root(), &f.plan, &decoded)
            .unwrap()
            .record(),
        record
    );
    std::fs::write(
        f.root().join(".github/workflows/ci.yml"),
        WORKFLOW.replace("linux", "macos"),
    )
    .unwrap();
    assert_eq!(f.source().record(), record);
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "other"]);
    let other = git(f.root(), &["rev-parse", "HEAD"]);
    git(f.root(), &["replace", &commit, &other]);
    assert_eq!(
        f.source().record(),
        record,
        "replace refs must not retarget the original commit"
    );
    for (pointer, value) in [
        ("/schema_version", serde_json::json!(999)),
        ("/binding/execution_commit", serde_json::json!(other)),
        (
            "/workflows/0/object_hash",
            serde_json::json!("0".repeat(64)),
        ),
        ("/workflows/0/jobs/0/runs_on", serde_json::json!("macos")),
    ] {
        let mut bad = serde_json::to_value(record).unwrap();
        *bad.pointer_mut(pointer).unwrap() = value;
        let bad: EvaluationSource = serde_json::from_value(bad).unwrap();
        assert!(
            verify_git_evaluation_source(&f.ctx, f.root(), &f.plan, &bad).is_err(),
            "{pointer}"
        );
    }
}

#[test]
fn git_projection_commit_identity_and_path_mode_are_checked() {
    let mut f = Fixture::new();
    let original = f.plan.clone();
    f.retarget("HEAD".into());
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan).is_err());
    f.plan = original.clone();
    f.plan.source.source_id = "commit:wrong".into();
    f.plan.digest = f.plan.compute_digest().unwrap();
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan).is_err());
    f.plan = original.clone();
    std::fs::write(
        f.root().join(".github/workflows/ci.yml"),
        WORKFLOW.replace("echo fixture", "echo different"),
    )
    .unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "different projection"]);
    f.retarget(git(f.root(), &["rev-parse", "HEAD"]));
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan).is_err());
    // Keep the frozen projection but bind it to commits with unsupported modes.
    let blob = git(f.root(), &["rev-parse", "HEAD:.github/workflows/ci.yml"]);
    for mode in ["120000", "160000"] {
        let object = if mode == "160000" {
            git(f.root(), &["rev-parse", "HEAD"])
        } else {
            blob.clone()
        };
        git(
            f.root(),
            &[
                "update-index",
                "--cacheinfo",
                &format!("{mode},{object},.github/workflows/ci.yml"),
            ],
        );
        git(f.root(), &["commit", "-qm", "unsupported mode"]);
        f.retarget(git(f.root(), &["rev-parse", "HEAD"]));
        assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan).is_err());
    }
    assert!(tree_entry(
        b"100644 blob 0000000000000000000000000000000000000000 1\tother\0",
        "expected",
        40
    )
    .is_err());
    assert!(tree_entry(
        b"100644 blob 0000000000000000000000000000000000000000 1\tx\0x\0",
        "x",
        40
    )
    .is_err());
    assert!(tree_entry(
        b"040000 tree 0000000000000000000000000000000000000000 -\tx\0",
        "x",
        40
    )
    .is_err());
    let literal = command(
        &f.ctx,
        f.root(),
        &["ls-tree", "-z", "HEAD", "--", ":(glob)**"],
        MAX_PATH + 256,
        Instant::now(),
    )
    .unwrap();
    assert!(literal.is_empty(), "pathspec magic must be disabled");
}

#[test]
fn git_blob_budget_and_missing_local_objects_fail_without_fetching() {
    let mut f = Fixture::new();
    let mut large = WORKFLOW.as_bytes().to_vec();
    large.extend(vec![b' '; MAX_SOURCE_BYTES]);
    std::fs::write(f.root().join(".github/workflows/ci.yml"), large).unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "oversized"]);
    f.retarget(git(f.root(), &["rev-parse", "HEAD"]));
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan)
        .unwrap_err()
        .to_string()
        .contains("byte bound"));
    let f = Fixture::new();
    let blob = git(f.root(), &["rev-parse", "HEAD:.github/workflows/ci.yml"]);
    git(
        f.root(),
        &[
            "config",
            "remote.origin.url",
            "https://example.invalid/must-not-fetch",
        ],
    );
    git(f.root(), &["config", "remote.origin.promisor", "true"]);
    git(f.root(), &["config", "extensions.partialClone", "origin"]);
    std::fs::remove_file(
        f.root()
            .join(".git/objects")
            .join(&blob[..2])
            .join(&blob[2..]),
    )
    .unwrap();
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan).is_err());
    assert!(!f.root().join(".git/FETCH_HEAD").exists());
}

#[test]
fn sha256_git_object_format_is_supported_without_changing_source_binding() {
    let f = Fixture::new();
    let other = f.root().join("sha256");
    std::fs::create_dir(&other).unwrap();
    git(
        &other,
        &["init", "-q", "-b", "main", "--object-format=sha256"],
    );
    std::fs::create_dir_all(other.join(".github/workflows")).unwrap();
    std::fs::write(other.join(".github/workflows/ci.yml"), WORKFLOW).unwrap();
    git(&other, &["add", "."]);
    git(&other, &["commit", "-qm", "sha256 fixture"]);
    let mut plan = f.plan.clone();
    let commit = git(&other, &["rev-parse", "HEAD"]);
    assert_eq!(commit.len(), 64);
    plan.source.source_id = format!("commit:{commit}");
    plan.source.execution_commit = Some(commit.clone());
    plan.digest = plan.compute_digest().unwrap();
    let result = git_evaluation_source(&f.ctx, &other, &plan).unwrap();
    assert_eq!(
        result.record().binding(),
        &EvaluationBinding::Git {
            execution_commit: commit
        }
    );
}

fn fake_context(script: &str) -> (tempfile::TempDir, ExecContext) {
    let area = tempfile::Builder::new()
        .prefix("localci-git-child-")
        .tempdir_in(std::fs::canonicalize(std::env::temp_dir()).unwrap())
        .unwrap();
    let path = area.path().join("git");
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let ctx = ExecContext::new(
        [
            (
                "PATH".into(),
                format!("{}:/usr/bin:/bin", area.path().display()),
            ),
            ("HOME".into(), area.path().display().to_string()),
        ]
        .into(),
    );
    (area, ctx)
}
fn not_running(pid: u32) {
    // Our leader has been synchronously reaped, not merely signalled.
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}
#[test]
fn bounded_git_child_cancel_deadline_and_output_errors_reap_the_owned_leader() {
    let (area, ctx) = fake_context("#!/bin/sh\nexec /bin/sleep 60\n");
    let pid = std::rc::Rc::new(RefCell::new(0));
    let seen = pid.clone();
    let cancel = ctx.cancel.clone();
    SPAWN.with(|s| {
        *s.borrow_mut() = Some(Box::new(move |child| {
            *seen.borrow_mut() = child.id();
            cancel.cancel();
        }))
    });
    assert!(command(&ctx, area.path(), &["unused"], 16, Instant::now())
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    SPAWN.with(|s| s.borrow_mut().take());
    not_running(*pid.borrow());
    for (script, limit, expected, aged) in [
        ("#!/bin/sh\nexec /bin/sleep 60\n", 16, "deadline", true),
        (
            "#!/bin/sh\nexec /usr/bin/yes x\n",
            16,
            "output limit",
            false,
        ),
        (
            "#!/bin/sh\nexec /usr/bin/yes x >&2\n",
            16,
            "output limit",
            false,
        ),
        (
            "#!/bin/sh\nprintf 'private error' >&2\nexit 1\n",
            16,
            "command failed",
            false,
        ),
    ] {
        let (area, ctx) = fake_context(script);
        let seen = pid.clone();
        SPAWN.with(|s| {
            *s.borrow_mut() = Some(Box::new(move |child| *seen.borrow_mut() = child.id()))
        });
        let started = if aged {
            Instant::now() - (REQUEST_TIME - Duration::from_millis(100))
        } else {
            Instant::now()
        };
        let error = command(&ctx, area.path(), &["unused"], limit, started)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("private error"));
        SPAWN.with(|s| s.borrow_mut().take());
        not_running(*pid.borrow());
    }
}

#[test]
fn exact_byte_limit_and_combined_workflow_limit_are_independent_of_projection() {
    let mut f = Fixture::new();
    let mut exact = WORKFLOW.as_bytes().to_vec();
    exact.push(b'#');
    exact.resize(MAX_SOURCE_BYTES, b' ');
    std::fs::write(f.root().join(".github/workflows/ci.yml"), &exact).unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "exact byte bound"]);
    f.retarget(git(f.root(), &["rev-parse", "HEAD"]));
    assert_eq!(
        f.source().record().workflows()[0].object_hash(),
        format!("{:x}", Sha256::digest(&exact))
    );
    let mut first = WORKFLOW.as_bytes().to_vec();
    first.push(b'#');
    first.resize(MAX_SOURCE_BYTES / 2, b' ');
    let mut second = first.clone();
    second.push(b' ');
    std::fs::write(f.root().join(".github/workflows/ci.yml"), &first).unwrap();
    std::fs::write(f.root().join(".github/workflows/other.yml"), &first).unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "combined byte bound"]);
    f.plan = crate::build_frozen_plan(
        &f.ctx,
        &Target {
            dir: f.root().into(),
            sha: "HEAD".into(),
            base: Some("HEAD".into()),
            all: true,
            include_disabled: true,
            workflows_dirs: vec![],
            workflows: vec![],
            jobs: vec![],
        },
        &mut Quiet,
    )
    .unwrap();
    assert_eq!(f.plan.workflows.len(), 2);
    git_evaluation_source(&f.ctx, f.root(), &f.plan).unwrap();
    std::fs::write(f.root().join(".github/workflows/other.yml"), &second).unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "combined bound plus one"]);
    f.retarget(git(f.root(), &["rev-parse", "HEAD"]));
    assert!(
        crate::git::workflows_at(&f.ctx, f.root(), "HEAD", None, &[])
            .unwrap_err()
            .to_string()
            .contains("byte bound")
    );
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan)
        .unwrap_err()
        .to_string()
        .contains("byte bound"));
}

#[test]
fn context_git_redirection_cannot_retarget_the_explicit_repository_and_blob_oid_is_checked() {
    let f = Fixture::new();
    let expected = f.source();
    let ctx = ExecContext::new(
        [
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), f.root().display().to_string()),
            ("GIT_DIR".into(), "/does-not-exist".into()),
            ("GIT_OBJECT_DIRECTORY".into(), "/does-not-exist".into()),
            ("GIT_CONFIG_COUNT".into(), "1".into()),
            (
                "GIT_CONFIG_KEY_0".into(),
                "core.repositoryformatversion".into(),
            ),
            ("GIT_CONFIG_VALUE_0".into(), "999".into()),
        ]
        .into(),
    );
    assert_eq!(
        git_evaluation_source(&ctx, f.root(), &f.plan)
            .unwrap()
            .record(),
        expected.record()
    );
    let original_blob = git(f.root(), &["rev-parse", "HEAD:.github/workflows/ci.yml"]);
    std::fs::write(
        f.root().join(".github/workflows/ci.yml"),
        WORKFLOW.replace("linux", "macos"),
    )
    .unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "other raw blob"]);
    let other_blob = git(f.root(), &["rev-parse", "HEAD:.github/workflows/ci.yml"]);
    let object = |id: &str| f.root().join(".git/objects").join(&id[..2]).join(&id[2..]);
    // Real Git writes a valid compressed object; transplanting it under the old
    // object name must not change the old commit's omitted runs-on facts.
    std::fs::remove_file(object(&original_blob)).unwrap();
    std::fs::copy(object(&other_blob), object(&original_blob)).unwrap();
    assert!(git_evaluation_source(&f.ctx, f.root(), &f.plan).is_err());
}

#[test]
fn ordinary_git_planning_preserves_trailing_workflow_scalar_bytes() {
    let f = Fixture::new();
    let text = "on: push\njobs:\n  check:\n    runs-on: fixture\n    steps:\n      - run: |\n          printf fixture\n\n";
    std::fs::write(f.root().join(".github/workflows/ci.yml"), text).unwrap();
    git(f.root(), &["add", "."]);
    git(f.root(), &["commit", "-qm", "trailing block scalar"]);
    let files = crate::git::workflows_at(&f.ctx, f.root(), "HEAD", None, &[]).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].1.as_bytes(), text.as_bytes());
    let plan = crate::build_frozen_plan(
        &f.ctx,
        &Target {
            dir: f.root().into(),
            sha: "HEAD".into(),
            base: Some("HEAD".into()),
            all: true,
            include_disabled: true,
            workflows_dirs: vec![],
            workflows: vec![],
            jobs: vec![],
        },
        &mut Quiet,
    )
    .unwrap();
    git_evaluation_source(&f.ctx, f.root(), &plan).unwrap();
}
