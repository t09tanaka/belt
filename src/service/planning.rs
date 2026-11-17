//! Private, owned planning drafts. No resident, coordinator or demand is opened.
use super::context::Context;
use crate::{
    ci::CiPlan,
    error::{ApiError, ErrorCode},
    ids::RunId,
    source::SourceStore,
};
use local_ci::{frozen::Filters, source::SourceManifest, PinnedPlanOptions};

/// One capture and one exact core plan, retained together for a future executor.
/// Dropping the object removes only its owned private draft, including vault data.
/// There is deliberately no Debug/Serialize implementation for captured inputs.
pub struct Prepared {
    plan: CiPlan,
    manifest: SourceManifest,
    store: SourceStore,
    run: RunId,
    draft: tempfile::TempDir,
}
impl Prepared {
    pub fn plan(&self) -> &CiPlan {
        &self.plan
    }
    pub fn manifest(&self) -> &SourceManifest {
        &self.manifest
    }
    pub fn source_store(&self) -> &SourceStore {
        &self.store
    }
    pub fn run_id(&self) -> &RunId {
        &self.run
    }
    /// Explicit cleanup permits the CLI to report cleanup failure safely.
    pub fn close(self) -> Result<(), ApiError> {
        self.draft
            .close()
            .map_err(|_| failure("cannot remove private planning draft"))
    }
}
fn failure(message: &'static str) -> ApiError {
    ApiError::new(ErrorCode::InvalidInput, message)
}

// Read only manifest-listed regular configuration blobs. Never reopen the
// original project, follow links or parse a declared secret as configuration.
fn captured_text(
    store: &SourceStore,
    manifest: &SourceManifest,
    path: &str,
) -> Result<Option<String>, ApiError> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    crate::ci::summary::validate_path(path)
        .map_err(|_| failure("captured planning configuration path is invalid"))?;
    if manifest
        .secret_files
        .iter()
        .any(|e| e.path.0 == path.as_bytes())
    {
        return Err(failure(
            "captured planning configuration is unavailable or secret",
        ));
    }
    let Some(entry) = manifest
        .object_manifest
        .entries
        .iter()
        .find(|e| e.path.0 == path.as_bytes())
    else {
        return Ok(None);
    };
    if !matches!(entry.mode, 0o100644 | 0o100755) || entry.size > 1_048_576 {
        return Err(failure(
            "captured planning configuration is unavailable or too large",
        ));
    }
    let verified = store
        .open_blob_verified(&entry.hash)
        .map_err(|_| failure("captured planning configuration is invalid"))?;
    let mut text = String::new();
    verified
        .file
        .take(1_048_577)
        .read_to_string(&mut text)
        .map_err(|_| failure("captured planning configuration is invalid"))?;
    if text.len() > 1_048_576
        || text.len() as u64 != entry.size
        || format!("{:x}", Sha256::digest(text.as_bytes())) != entry.hash
    {
        return Err(failure("captured planning configuration is too large"));
    }
    Ok(Some(text))
}
fn captured_ci_config(
    store: &SourceStore,
    manifest: &SourceManifest,
) -> Result<(Vec<String>, Vec<crate::config::model::ShardPolicy>), ApiError> {
    use crate::config::{self, resolve::Loaded, Inputs, Overlay};
    let project = captured_text(store, manifest, ".remote-task-runner.toml")?
        .map(|text| {
            config::parse::parse_project(".remote-task-runner.toml", &text).map(|value| Loaded {
                path: ".remote-task-runner.toml".into(),
                value,
            })
        })
        .transpose()
        .map_err(|_| failure("captured project configuration is invalid"))?;
    let legacy = captured_text(store, manifest, ".local-ci.toml")?
        .map(|text| config::legacy::from_text(".local-ci.toml", &text))
        .transpose()
        .map_err(|_| failure("captured legacy configuration is invalid"))?;
    let policies = project
        .as_ref()
        .and_then(|p| p.value.ci.as_ref())
        .map(|ci| ci.sharding.clone())
        .unwrap_or_default();
    config::resolve(&Inputs {
        overlay: &Overlay::default(),
        project: project.as_ref(),
        legacy: legacy.as_ref(),
        user: None,
    })
    .map(|resolved| (resolved.workflows_dirs.value, policies))
    .map_err(|_| failure("captured planning configuration is invalid"))
}

/// Read the exact plan currently bound to this run. A run still discovering
/// its tests shows its initial plan; once applied it shows the partition.
pub fn stored(context: &Context, run: &RunId) -> Result<crate::ci::summary::PlanSummary, ApiError> {
    let store = super::bootstrap::read_only_store(&context.bootstrap)
        .map_err(|_| failure("coordinator state is unavailable"))?;
    let rows = store
        .ci_run_tasks(run)
        .map_err(|_| failure("cannot read the stored run"))?;
    let digest = rows
        .iter()
        .filter(|r| r.spec.pointer("/ci/task/inputs/rtr_discovery").is_none())
        .find_map(|r| {
            r.spec
                .pointer("/ci/task/plan_digest")
                .and_then(|v| v.as_str())
        })
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no stored CI plan for this run"))?;
    let digest =
        crate::ids::Digest::parse(digest).map_err(|_| failure("invalid stored plan digest"))?;
    let bytes = crate::ci::advance::read_plan(context.state_dir(), &digest)
        .map_err(|_| failure("stored CI plan is unavailable"))?;
    let plan = serde_json::from_slice(&bytes).map_err(|_| failure("stored CI plan is invalid"))?;
    let ci = CiPlan::from_frozen(plan)
        .map_err(|_| failure("stored CI plan cannot be summarized safely"))?;
    Ok(ci.summary().clone())
}

/// Capture once. The core reads only verified objects and owns its secret-free view.
/// Missing filter metadata is rejected by core rather than inferred from live Git.
pub fn prepare(context: Context, revision: Option<&str>) -> Result<Prepared, ApiError> {
    let draft = tempfile::Builder::new()
        .prefix("ci-plan-")
        .tempdir_in(context.state_dir())
        .map_err(|_| failure("cannot create private planning draft"))?;
    let run = RunId::generate();
    super::input::capture_into(&context, &run, revision, draft.path())
        .map_err(|error| ApiError::new(error.code, "cannot capture planning source"))?;
    let store = SourceStore::open(draft.path())
        .map_err(|_| failure("captured planning input is unavailable"))?;
    let manifest = super::input::source_manifest_in(draft.path(), &run)
        .map_err(|_| failure("captured planning input is unavailable"))?;
    let frozen = freeze(&store, &manifest)?;
    let plan = CiPlan::from_frozen(frozen)
        .map_err(|_| failure("captured plan cannot be summarized safely"))?;
    Ok(Prepared {
        plan,
        manifest,
        store,
        run,
        draft,
    })
}

/// Freeze the plan of a captured source, once: the workflows its own
/// captured configuration selects, read from verified objects only. `ci
/// plan` shows this plan and `ci` runs it.
pub(super) fn freeze(
    store: &SourceStore,
    manifest: &SourceManifest,
) -> Result<local_ci::FrozenPlan, ApiError> {
    freeze_with_policies(store, manifest).map(|(plan, _)| plan)
}

pub(super) fn freeze_with_policies(
    store: &SourceStore,
    manifest: &SourceManifest,
) -> Result<(local_ci::FrozenPlan, Vec<crate::config::model::ShardPolicy>), ApiError> {
    let (workflow_dirs, policies) = captured_ci_config(store, manifest)?;

    let options = PinnedPlanOptions {
        workflow_dirs_override: Some(workflow_dirs),
        filters: Filters {
            all: false,
            include_disabled: false,
            workflows_dirs: vec![],
            workflows: vec![],
            jobs: vec![],
        },
        disabled: vec![],
        base_branch: None,
        changed_files: None,
    };
    store
        .object_store()
        .and_then(|view| {
            view.with_sdk(|objects| {
                Ok(local_ci::build_frozen_plan_from_source(
                    objects, manifest, &options,
                ))
            })
        })
        .map_err(|_| failure("captured planning input is unavailable"))?
        .map(|plan| (plan, policies))
        .map_err(|error| ApiError::new(ErrorCode::InvalidInput, error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{capture, CaptureParams, SourceRequest};

    #[test]
    fn captured_configuration_verifies_used_bytes_and_never_reads_live_source() {
        let root = crate::test_tmp::tempdir().unwrap();
        let project = root.path().join("project");
        std::fs::create_dir(&project).unwrap();
        let path = ".local-ci.toml";
        std::fs::write(project.join(path), "captured").unwrap();
        let store = SourceStore::open(&root.path().join("state")).unwrap();
        let env = std::env::vars().collect();
        let captured = capture(
            &env,
            &store,
            &SourceRequest::Directory {
                dir: project.clone(),
            },
            &CaptureParams::default(),
        )
        .unwrap();
        let manifest = store
            .manifest(&captured.manifest.ci_source_id)
            .unwrap()
            .unwrap();
        std::fs::write(project.join(path), "live private value").unwrap();
        assert_eq!(
            captured_text(&store, &manifest, path).unwrap().as_deref(),
            Some("captured")
        );
        assert!(captured_text(&store, &manifest, "../.local-ci.toml").is_err());
        assert_eq!(captured_text(&store, &manifest, "absent").unwrap(), None);
        let mut bad = manifest.clone();
        bad.object_manifest.entries[0].size += 1;
        assert!(captured_text(&store, &bad, path).is_err());
        bad = manifest.clone();
        bad.object_manifest.entries[0].mode = 0o120000;
        assert!(captured_text(&store, &bad, path).is_err());
        bad = manifest.clone();
        bad.secret_files.push(local_ci::source::SecretEntry {
            path: local_ci::source::RelPath::new(path),
            hmac: "private hmac".into(),
        });
        assert!(captured_text(&store, &bad, path).is_err());
        let hash = &manifest.object_manifest.entries[0].hash;
        std::fs::write(store.object_store().unwrap().object_path(hash), "corrupt!").unwrap();
        assert!(captured_text(&store, &manifest, path).is_err());
    }
    #[test]
    fn captured_plan_keeps_missing_filter_metadata_typed() {
        let root = crate::test_tmp::tempdir().unwrap();
        let project = root.path().join("project");
        std::fs::create_dir_all(project.join(".github/workflows")).unwrap();
        std::fs::write(project.join(".github/workflows/ci.yml"),
            "on:\n  push:\n    paths: ['src/**']\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo private\n").unwrap();
        let store = SourceStore::open(&root.path().join("state")).unwrap();
        let captured = capture(
            &std::env::vars().collect(),
            &store,
            &SourceRequest::Directory { dir: project },
            &CaptureParams::default(),
        )
        .unwrap();
        let manifest = store
            .manifest(&captured.manifest.ci_source_id)
            .unwrap()
            .unwrap();
        let options = PinnedPlanOptions {
            workflow_dirs_override: None,
            filters: Filters {
                all: false,
                include_disabled: false,
                workflows_dirs: vec![],
                workflows: vec![],
                jobs: vec![],
            },
            disabled: vec![],
            base_branch: None,
            changed_files: None,
        };
        assert!(matches!(
            store
                .object_store()
                .unwrap()
                .with_sdk(|objects| {
                    Ok(local_ci::build_frozen_plan_from_source(
                        objects, &manifest, &options,
                    ))
                })
                .unwrap(),
            Err(local_ci::PinnedPlanError::MissingChangedFiles)
        ));
    }
}
