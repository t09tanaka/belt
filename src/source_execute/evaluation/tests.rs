use super::*;
use crate::source::{self, CaptureOptions, ObjectStore};
use crate::{ExecContext, PinnedPlanOptions};
use std::path::Path;

const WORKFLOW: &str = "on: push\njobs:\n  seed:\n    runs-on: [self-hosted, linux]\n    steps:\n      - run: echo seed\n  dynamic:\n    needs: seed\n    runs-on: ${{ matrix.os }}\n    strategy:\n      matrix: ${{ fromJSON(needs.seed.outputs.matrix) }}\n    steps:\n      - run: echo dynamic\n";
#[allow(clippy::disallowed_methods)]
fn git(root: &Path, args: &[&str]) -> String {
    let result = std::process::Command::new("/usr/bin/git")
        .args(args)
        .current_dir(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().into()
}

#[test]
fn captured_directory_and_git_recompute_scheduling_provenance() {
    for is_git in [false, true] {
        let area = tempfile::Builder::new()
            .prefix("localci-evaluation-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let root = area.path().join("source");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        let path = root.join(".github/workflows/ci.yml");
        std::fs::write(&path, WORKFLOW).unwrap();
        let base = if is_git {
            git(&root, &["init", "-q", "-b", "main"]);
            git(&root, &["add", "."]);
            git(&root, &["commit", "-qm", "fixture"]);
            Some(git(&root, &["rev-parse", "HEAD"]))
        } else {
            None
        };
        let ctx = ExecContext::new(
            [
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), area.path().display().to_string()),
            ]
            .into(),
        );
        let store = ObjectStore::open(area.path().join("objects")).unwrap();
        let opts = CaptureOptions {
            store: Some(store.root().into()),
            base,
            ..Default::default()
        };
        let manifest = if is_git {
            source::capture_source(&ctx, &root, &opts)
        } else {
            source::capture_directory(&ctx, &root, &opts)
        }
        .unwrap()
        .manifest;
        let plan = crate::build_frozen_plan_from_source(
            &store,
            &manifest,
            &PinnedPlanOptions {
                filters: crate::frozen::Filters {
                    include_disabled: true,
                    jobs: vec!["dynamic".into()],
                    all: false,
                    workflows_dirs: vec![],
                    workflows: vec![],
                },
                workflow_dirs_override: None,
                disabled: vec![],
                base_branch: None,
                changed_files: None,
            },
        )
        .unwrap();
        let before = serde_json::to_vec(&plan).unwrap();
        let input = PinnedExecution::new(&plan, &manifest, &store).unwrap();
        let verified = input.evaluation_source().unwrap();
        let record = verified.record();
        assert_eq!(record.plan_digest(), plan.digest);
        assert_eq!(record.source_id(), manifest.source_id);
        assert_eq!(record.manifest_digest(), manifest.manifest_digest);
        let workflow = &record.workflows()[0];
        assert_eq!(workflow.workflow(), ".github/workflows/ci.yml");
        assert_eq!(workflow.jobs()[0].job(), "seed");
        assert_eq!(
            workflow.jobs()[0].runs_on(),
            Some(&serde_json::json!(["self-hosted", "linux"]))
        );
        assert_eq!(
            workflow.jobs()[1].runs_on(),
            Some(&Value::String("${{ matrix.os }}".into()))
        );
        let text = serde_json::to_string(record).unwrap();
        let decoded = EvaluationSource::from_json(&text).unwrap();
        assert_eq!(
            input.verify_evaluation_source(&decoded).unwrap().record(),
            record
        );
        // Live checkout edits cannot retarget the captured requirement.
        std::fs::write(&path, WORKFLOW.replace("matrix.os", "runner.os")).unwrap();
        assert_eq!(input.evaluation_source().unwrap().record(), record);
        for key in [
            "schema_version",
            "plan_digest",
            "source_id",
            "manifest_digest",
            "workflows",
        ] {
            let mut forged = serde_json::to_value(record).unwrap();
            forged[key] = if key == "schema_version" {
                serde_json::json!(999)
            } else if key == "workflows" {
                serde_json::json!([])
            } else {
                Value::String("forged".into())
            };
            let forged: EvaluationSource = serde_json::from_value(forged).unwrap();
            assert!(input.verify_evaluation_source(&forged).is_err(), "{key}");
        }
        for field in ["workflow", "object_hash"] {
            let mut forged = serde_json::to_value(record).unwrap();
            forged["workflows"][0][field] = Value::String("forged".into());
            assert!(input
                .verify_evaluation_source(&serde_json::from_value(forged).unwrap())
                .is_err());
        }
        for field in ["job", "runs_on"] {
            let mut forged = serde_json::to_value(record).unwrap();
            forged["workflows"][0]["jobs"][0][field] = Value::String("forged".into());
            assert!(input
                .verify_evaluation_source(&serde_json::from_value(forged).unwrap())
                .is_err());
        }
        let mut unknown = serde_json::to_value(record).unwrap();
        unknown["unchecked_verified"] = Value::Bool(true);
        assert!(EvaluationSource::from_json(&unknown.to_string()).is_err());
        assert!(EvaluationSource::from_json(&" ".repeat(MAX_SOURCE_BYTES + 1)).is_err());
        // Frozen wire projection and original canonical bytes are unchanged.
        assert_eq!(serde_json::to_vec(&plan).unwrap(), before);
        assert!(!String::from_utf8(before).unwrap().contains("runs_on"));
        plan.verify().unwrap();
        let parsed = crate::workflow::parse(".github/workflows/ci.yml", WORKFLOW).unwrap();
        let mut stripped = parsed.clone();
        for job in &mut stripped.jobs {
            job.runs_on = None;
        }
        assert_eq!(
            serde_json::to_value(parsed).unwrap(),
            serde_json::to_value(stripped).unwrap()
        );
        // A validly rehashed plan cannot silently reinterpret original bytes.
        let mut retargeted = plan.clone();
        retargeted.workflows[0].jobs[0].name = Some("changed projection".into());
        retargeted.digest = retargeted.compute_digest().unwrap();
        let retargeted_input = PinnedExecution::new(&retargeted, &manifest, &store).unwrap();
        assert!(retargeted_input.evaluation_source().is_err());
        // Corrupting the object after capability construction is still caught.
        std::fs::write(store.object_path(workflow.object_hash()), b"corrupt").unwrap();
        assert!(input.evaluation_source().is_err());
    }
}
