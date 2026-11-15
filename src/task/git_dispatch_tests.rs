use super::*;
use crate::driver::{
    ActionKind, Driver, DriverConfig, Input, PortableFacts, Transition, DISPATCH_VERSION,
    DRIVER_VERSION,
};
use crate::result_wire::WireTaskResult;
use crate::wire::Digested;
use crate::{Quiet, Target};
use std::path::{Path, PathBuf};
use std::process::Command;

const FILE: &str = ".github/workflows/ci.yml";
const WORKFLOW: &str = r#"on: push
jobs:
  seed:
    runs-on: seed
    outputs:
      matrix: ${{ steps.out.outputs.matrix }}
    steps:
      - id: out
        run: echo 'matrix={"n":[0,1]}' >> "$GITHUB_OUTPUT"
  dynamic:
    needs: seed
    name: 'leg-${{ matrix.n }}'
    runs-on: [self-hosted, '${{ inputs.pool }}']
    strategy:
      fail-fast: false
      matrix: ${{ fromJSON(needs.seed.outputs.matrix) }}
    env:
      TAG: ${{ vars.tag }}
    outputs:
      value: ${{ steps.out.outputs.value }}
    steps:
      - id: out
        run: |
          test "$TAG" = verified
          test '${{ strategy.job-index }}' = '${{ matrix.n }}'
          printf '%s' '${{ matrix.n }}' > marker
          echo "value=$TAG-${{ matrix.n }}" >> "$GITHUB_OUTPUT"
"#;
#[allow(clippy::disallowed_methods)]
fn git(root: &Path, args: &[&str]) {
    let out = Command::new("/usr/bin/git")
        .args([
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
}
struct Fixture {
    area: tempfile::TempDir,
    ctx: ExecContext,
    plan: FrozenPlan,
    facts: PortableFacts,
}
impl Fixture {
    fn new() -> Self {
        Self::with_workflow(WORKFLOW)
    }
    fn with_workflow(workflow: &str) -> Self {
        let area = tempfile::Builder::new()
            .prefix("localci-git-dispatch-")
            .tempdir_in(std::fs::canonicalize(std::env::temp_dir()).unwrap())
            .unwrap();
        let root = area.path().join("repo");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        std::fs::write(root.join(FILE), workflow).unwrap();
        git(&root, &["init", "-q", "-b", "main"]);
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "fixture"]);
        let bin = area.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        let docker = bin.join("docker");
        std::fs::write(
            &docker,
            "#!/bin/sh\nprintf cleanup >> \"$HOME/cleanup-called\"\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o700)).unwrap();
        let portool = bin.join("portool");
        std::fs::write(
            &portool,
            "#!/bin/sh\nprintf release >> \"$HOME/port-release-called\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&portool, std::fs::Permissions::from_mode(0o700)).unwrap();
        let ctx = ExecContext::new(
            [
                ("PATH".into(), format!("{}:/usr/bin:/bin", bin.display())),
                ("HOME".into(), area.path().display().to_string()),
            ]
            .into(),
        );
        let plan = crate::build_frozen_plan(
            &ctx,
            &Target {
                dir: root.clone(),
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
        let facts = PortableFacts {
            schema_version: DISPATCH_VERSION,
            github: Map::new(),
            vars: json!({"tag":"verified"}).as_object().unwrap().clone(),
            inputs: json!({"pool":"private"}).as_object().unwrap().clone(),
        };
        Self {
            area,
            ctx,
            plan,
            facts,
        }
    }
    fn repo(&self) -> PathBuf {
        self.area.path().join("repo")
    }
    fn driver(&self) -> Driver {
        let source =
            crate::source_execute::git_evaluation_source(&self.ctx, &self.repo(), &self.plan)
                .unwrap();
        Driver::with_evaluation(
            &self.plan,
            DriverConfig {
                schema_version: DRIVER_VERSION,
                max_parallel: 2,
            },
            &source,
            self.facts.clone(),
        )
        .unwrap()
    }
    fn ws(&self, name: &str) -> Workspace {
        let base = self.area.path().join(name);
        let root = base.join("root");
        std::fs::create_dir_all(&base).unwrap();
        git(
            self.area.path(),
            &[
                "clone",
                "-q",
                "--no-hardlinks",
                self.repo().to_str().unwrap(),
                root.to_str().unwrap(),
            ],
        );
        git(
            &root,
            &[
                "checkout",
                "-q",
                "--detach",
                self.plan.source.execution_commit.as_deref().unwrap(),
            ],
        );
        Workspace {
            root,
            repo_root: self.repo(),
            run_dir: base.join("run"),
            tools: base.join("tools"),
            lock_file: base.join("lock"),
        }
    }
    fn opts(&self) -> Opts {
        Opts {
            parallel: 1,
            traffic_controller: None,
            executor: None,
            portool: None,
            containers: 1,
            job_timeout_min: 1.,
            keep: false,
            trash: self.area.path().join("trash"),
        }
    }
    fn refusal_opts(&self) -> Opts {
        let mut opts = self.opts();
        opts.portool = Some(self.area.path().join("bin/portool"));
        opts
    }
    fn seed(&self, d: &mut Driver) -> Vec<Transition> {
        let mut journal = vec![commit(d, Input::Advance)];
        let action = d
            .pending_actions()
            .iter()
            .find_map(|a| match &a.kind {
                ActionKind::Ready { task, inputs } => Some((task.clone(), inputs.clone())),
                _ => None,
            })
            .unwrap();
        journal.push(commit(
            d,
            Input::Claim {
                task: action.0.clone(),
                token: action.0.clone(),
            },
        ));
        journal.push(commit(
            d,
            Input::MarkDispatched {
                task: action.0.clone(),
                token: action.0.clone(),
            },
        ));
        let ws = self.ws("seed");
        let result = execute_task(
            &self.ctx,
            &self.plan,
            &action.0,
            &action.1,
            None,
            &ws,
            self.opts(),
        )
        .unwrap();
        assert_eq!(result.outcomes[0].status, JobStatus::Success);
        let wire = WireTaskResult::from_task_result(&result, &ws.run_dir).unwrap();
        journal.push(commit(
            d,
            Input::Settle {
                task: action.0.clone(),
                token: action.0,
                result_digest: wire.digest().unwrap(),
                result: Box::new(wire),
            },
        ));
        journal
    }
}
fn commit(d: &mut Driver, input: Input) -> Transition {
    let t = d.propose(input).unwrap();
    d.apply(&t).unwrap();
    t
}
fn legs(d: &Driver) -> Vec<(crate::driver::DerivedDispatch, TaskInputs)> {
    d.pending_actions()
        .iter()
        .filter_map(|a| match &a.kind {
            ActionKind::DerivedReady { dispatch, inputs } => {
                Some((*dispatch.clone(), inputs.clone()))
            }
            _ => None,
        })
        .collect()
}
#[test]
fn git_derived_real_shell_receipts_replay_and_join() {
    let f = Fixture::new();
    let mut d = f.driver();
    let mut journal = f.seed(&mut d);
    let mut variants = legs(&d);
    variants.sort_by_key(|v| v.0.index());
    assert_eq!(variants.len(), 2);
    for (dispatch, inputs) in variants.iter().rev() {
        let requirement = &d.requirements(dispatch.task_id()).unwrap()[0];
        assert_eq!(
            requirement.original,
            Some(json!(["self-hosted", "${{ inputs.pool }}"]))
        );
        assert_eq!(
            requirement.evaluated,
            Some(json!(["self-hosted", "private"]))
        );
        let task = dispatch.task_id().to_string();
        journal.push(commit(
            &mut d,
            Input::Claim {
                task: task.clone(),
                token: task.clone(),
            },
        ));
        journal.push(commit(
            &mut d,
            Input::MarkDispatched {
                task: task.clone(),
                token: task.clone(),
            },
        ));
        let ws = f.ws(&format!("leg-{}", dispatch.index()));
        let ctx = f.ctx.fresh_attempt();
        let result = crate::execute_git_dispatch(
            &ctx,
            &f.plan,
            dispatch,
            inputs,
            &f.facts,
            None,
            &ws,
            f.opts(),
        )
        .unwrap();
        assert_eq!(result.outcomes[0].status, JobStatus::Success);
        assert_eq!(result.outcomes[0].variant, Some(dispatch.index()));
        assert_eq!(result.jobs[0].display, format!("leg-{}", dispatch.index()));
        assert_eq!(
            std::fs::read_to_string(ws.root.join("marker")).unwrap(),
            dispatch.index().to_string()
        );
        let wire = WireTaskResult::from_task_result(&result, &ws.run_dir).unwrap();
        journal.push(commit(
            &mut d,
            Input::Settle {
                task: task.clone(),
                token: task,
                result_digest: wire.digest().unwrap(),
                result: Box::new(wire),
            },
        ));
    }
    assert_eq!(
        d.outcome(FILE, "dynamic").unwrap().status,
        JobStatus::Success
    );
    let mut replay = f.driver();
    for t in journal {
        replay
            .apply(&Transition::from_json(&serde_json::to_string(&t).unwrap()).unwrap())
            .unwrap();
    }
    assert_eq!(
        serde_json::to_value(replay.pending_actions()).unwrap(),
        serde_json::to_value(d.pending_actions()).unwrap()
    );
}
#[test]
fn git_derived_rejects_forged_dispatch_inputs_facts_before_runner_side_effects() {
    let f = Fixture::new();
    let mut d = f.driver();
    f.seed(&mut d);
    let (dispatch, inputs) = legs(&d).remove(0);
    let ws = f.ws("refuse");
    let ctx = f.ctx.fresh_attempt();
    ctx.cancel
        .track_container("another-attempt-sentinel".into());
    let cleanup_before = std::fs::read(f.area.path().join("cleanup-called")).unwrap_or_default();
    let ports_before = std::fs::read(f.area.path().join("port-release-called")).unwrap_or_default();
    for (key, value) in [
        ("schema_version", json!(999)),
        ("plan_digest", json!("foreign")),
        ("origin", json!("foreign")),
        ("task", json!("foreign")),
        ("index", json!(999)),
        ("inputs_digest", json!("foreign")),
        ("evaluation_digest", json!("foreign")),
        ("expansion_digest", json!("foreign")),
    ] {
        let mut v = serde_json::to_value(&dispatch).unwrap();
        v[key] = value;
        let bad = crate::driver::DerivedDispatch::from_json(&v.to_string()).unwrap();
        assert!(
            execute_git_dispatch(
                &ctx,
                &f.plan,
                &bad,
                &inputs,
                &f.facts,
                None,
                &ws,
                f.refusal_opts()
            )
            .is_err(),
            "{key}"
        );
    }
    let mut bad_inputs = inputs.clone();
    bad_inputs.needs.get_mut("seed").unwrap().status = JobStatus::Failure;
    assert!(execute_git_dispatch(
        &ctx,
        &f.plan,
        &dispatch,
        &bad_inputs,
        &f.facts,
        None,
        &ws,
        f.refusal_opts()
    )
    .is_err());
    bad_inputs = inputs.clone();
    bad_inputs.run_cancelled = true;
    assert!(execute_git_dispatch(
        &ctx,
        &f.plan,
        &dispatch,
        &bad_inputs,
        &f.facts,
        None,
        &ws,
        f.refusal_opts()
    )
    .is_err());
    let mut bad_facts = f.facts.clone();
    bad_facts.vars.insert("tag".into(), json!("changed"));
    assert!(execute_git_dispatch(
        &ctx,
        &f.plan,
        &dispatch,
        &inputs,
        &bad_facts,
        None,
        &ws,
        f.refusal_opts()
    )
    .is_err());
    assert!(!ws.tools.exists());
    assert!(!ws.run_dir.exists());
    assert!(!ws.root.join("marker").exists());
    assert_eq!(
        std::fs::read(f.area.path().join("cleanup-called")).unwrap_or_default(),
        cleanup_before
    );
    assert_eq!(
        std::fs::read(f.area.path().join("port-release-called")).unwrap_or_default(),
        ports_before
    );
}
#[test]
fn git_derived_uses_original_commit_not_live_workflow_and_rejects_other_commit() {
    let f = Fixture::new();
    let mut d = f.driver();
    f.seed(&mut d);
    let (dispatch, inputs) = legs(&d).remove(0);
    let ws = f.ws("original");
    std::fs::write(
        f.repo().join(FILE),
        WORKFLOW
            .replace("private", "other")
            .replace("self-hosted", "foreign"),
    )
    .unwrap();
    let result = execute_git_dispatch(
        &f.ctx.fresh_attempt(),
        &f.plan,
        &dispatch,
        &inputs,
        &f.facts,
        None,
        &ws,
        f.opts(),
    )
    .unwrap();
    assert_eq!(result.outcomes[0].status, JobStatus::Success);
    git(&f.repo(), &["add", "."]);
    git(&f.repo(), &["commit", "-qm", "changed scheduling"]);
    let mut changed = f.plan.clone();
    let out = f
        .ctx
        .command("git")
        .arg("-C")
        .arg(f.repo())
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let sha = String::from_utf8(out.stdout).unwrap().trim().to_string();
    changed.source.execution_commit = Some(sha.clone());
    changed.source.source_id = format!("commit:{sha}");
    changed.digest = changed.compute_digest().unwrap();
    assert!(
        verify_git_dispatch(&f.ctx, &changed, &f.repo(), &dispatch, &inputs, &f.facts).is_err()
    );
}

#[test]
fn git_derived_native_background_remains_for_next_step_and_drains_with_entry() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;
    let old = "      - id: out\n        run: |\n          test \"$TAG\" = verified";
    let new = r#"      - name: start background
        run: |
          (exec 9>"$GITHUB_WORKSPACE/witness"; printf r >&9; while [ ! -f "$GITHUB_WORKSPACE/proceed" ]; do sleep 0.01; done; printf u >&9; touch "$GITHUB_WORKSPACE/used"; while :; do sleep 1; done) &
      - id: out
        run: |
          touch "$GITHUB_WORKSPACE/proceed"
          for n in $(seq 1 200); do test ! -f "$GITHUB_WORKSPACE/used" || break; sleep 0.01; done
          test -f "$GITHUB_WORKSPACE/used"
          test "$TAG" = verified"#;
    assert_eq!(WORKFLOW.matches(old).count(), 1);
    let f = Fixture::with_workflow(&WORKFLOW.replace(old, new));
    let mut d = f.driver();
    f.seed(&mut d);
    let (dispatch, inputs) = legs(&d).remove(0);
    let ws = f.ws("native-background");
    let fifo = ws.root.join("witness");
    let c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    let reader = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo)
        .unwrap();
    let ctx = f.ctx.fresh_attempt();
    let result = observe_drained_witness("resource cleanup", ctx.cancel.clone(), reader, || {
        crate::execute_git_dispatch(
            &ctx,
            &f.plan,
            &dispatch,
            &inputs,
            &f.facts,
            None,
            &ws,
            f.opts(),
        )
    })
    .unwrap();
    assert_eq!(result.outcomes[0].status, JobStatus::Success);
    assert_eq!(ctx.cancel.native_group_count(), 0);
}

#[test]
fn git_attempt_cleanup_failure_keeps_the_actual_result_and_pending_port() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new();
    let mut d = f.driver();
    f.seed(&mut d);
    let (dispatch, inputs) = legs(&d).remove(0);
    let ws = f.ws("port-cleanup-failure");
    std::fs::write(ws.root.join(".portool.toml"), b"# private fixture").unwrap();
    let portool = f.area.path().join("bin/portool");
    std::fs::write(&portool,"#!/bin/sh\ncase \"$1\" in\nsync) : > .env.portool;;\nrelease) printf release >> \"$HOME/port-release-called\"; exit 1;;\nesac\nexit 0\n").unwrap();
    std::fs::set_permissions(&portool, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut opts = f.opts();
    opts.portool = Some(portool);
    let ctx = f.ctx.fresh_attempt();
    let selected =
        verify_git_dispatch(&ctx, &f.plan, &ws.repo_root, &dispatch, &inputs, &f.facts).unwrap();
    let task = selected.task.clone();
    let runner = Runner::new(&ctx, &f.plan, &ws, opts)
        .unwrap()
        .select_derived(selected, f.facts.clone());
    let outcome = execute_resolved_outcome(&ctx, &task, &inputs, None, &ws, runner);
    assert_eq!(
        outcome.execution.as_ref().unwrap().outcomes[0].status,
        JobStatus::Success
    );
    assert!(outcome.final_error.is_some());
    assert_eq!(outcome.cleanup.pending.port_paths, vec![ws.root.clone()]);
    let error = outcome.legacy_result().unwrap_err();
    let original = crate::execution_outcome::retained_result(&error)
        .expect("actual result retained by legacy error");
    assert_eq!(original.outcomes[0].status, JobStatus::Success);
    assert_eq!(
        std::fs::read_to_string(ws.root.join("marker")).unwrap(),
        dispatch.index().to_string()
    );
}

/// Observe actual resource/receipt boundaries with a kernel pipe witness.
/// The producer reaches the second-step handshake before it can be drained.
fn observe_drained_witness<T>(
    boundary: &'static str,
    cancel: crate::CancelHandle,
    mut reader: std::fs::File,
    action: impl FnOnce() -> T,
) -> T {
    use std::io::Read;
    use std::time::{Duration, Instant};
    let seen = std::rc::Rc::new(std::cell::Cell::new(false));
    let observed = seen.clone();
    let result = crate::exec::finalization_test::observe(
        move |phase| {
            if phase != boundary || observed.replace(true) {
                return;
            }
            assert_eq!(
                cancel.native_group_count(),
                0,
                "live native group at {boundary}"
            );
            let until = Instant::now() + Duration::from_secs(3);
            let mut bytes = Vec::new();
            loop {
                let mut buf = [0; 16];
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => bytes.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < until,
                            "live background producer at {boundary}"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => panic!("owned witness read: {e}"),
                }
            }
            assert_eq!(bytes, b"ru", "both step handshakes must precede {boundary}");
        },
        action,
    );
    assert!(seen.get(), "actual {boundary} boundary was not reached");
    result
}

#[test]
fn recipe_drains_background_before_observing_artifact_digests() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;
    let f = Fixture::with_workflow(
        "on: push\njobs:\n  prepare:\n    runs-on: fixture\n    steps:\n      - run: npm ci\n",
    );
    let ws = f.ws("recipe-producer");
    let fifo = ws.root.join("witness");
    let c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    let reader = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(fifo)
        .unwrap();
    let mut recipe = crate::setup::derive_setup_recipe(&f.plan, &f.plan.tasks[0].id).unwrap();
    assert_eq!(recipe.steps.len(), 1);
    recipe.steps[0].run = r#"(exec 9>"$GITHUB_WORKSPACE/witness"; printf r >&9; while [ ! -f "$GITHUB_WORKSPACE/proceed" ]; do sleep 0.01; done; printf u >&9; touch "$GITHUB_WORKSPACE/used"; while :; do sleep 1; done) &"#.into();
    let mut next = recipe.steps[0].clone();
    next.id = "handshake-and-artifact".into();
    next.run = r#"touch "$GITHUB_WORKSPACE/proceed"; for n in $(seq 1 200); do test ! -f "$GITHUB_WORKSPACE/used" || break; sleep 0.01; done; test -f "$GITHUB_WORKSPACE/used"; mkdir prepared-artifact; echo done > prepared-artifact/content"#.into();
    recipe.steps.push(next);
    let runner = Runner::new(&f.ctx, &f.plan, &ws, f.opts()).unwrap();
    runner.prepare_tools().unwrap();
    let result = observe_drained_witness("artifact capture", f.ctx.cancel.clone(), reader, || {
        runner.run_recipe(&recipe, "fixture-key")
    })
    .unwrap();
    assert!(result
        .steps
        .iter()
        .all(|step| step.outcome == crate::exec::Outcome::Pass));
    assert!(result
        .receipt
        .artifacts
        .iter()
        .any(|artifact| artifact.path == "prepared-artifact"));
}
