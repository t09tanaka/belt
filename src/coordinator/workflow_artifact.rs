//! Live exchange for a step on a worker. The placement fixes the run and
//! workflow; no command from the worker supplies either scope.
use super::*;
use crate::ci::artifact_exchange::CHUNK;
use crate::ids::Digest;
use crate::results::{
    store::{ArtifactState, Publish},
    ResultStore,
};
use crate::storage::WorkflowUpload;
use crate::wire::{
    ArtifactAction, ArtifactAnswer, ArtifactExchangeResult, ArtifactIntent, Bytes, CiTask,
    WorkflowArtifactCommand, WorkflowArtifactRequest,
};
use local_ci::artifact_io::Receipt;
use std::io::{Read, Seek, SeekFrom};

impl Coordinator {
    async fn artifact_call(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        command: WorkflowArtifactCommand,
    ) -> Result<Option<ArtifactExchangeResult>> {
        let req = WorkflowArtifactRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            coordinator_epoch: self.epoch(),
            attempt_id: a.attempt_id.clone(),
            generation: a.generation,
            command,
        };
        match link.call(Request::WorkflowArtifact(req)).await {
            Ok(Response::WorkflowArtifact(r)) => Ok(Some(r.result)),
            Err(LinkError::Remote(e)) if e.code == ErrorCode::StaleEpoch => Err(self.fence()),
            _ => Ok(None),
        }
    }
    pub(super) async fn follow_artifacts(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
    ) -> Result<()> {
        if !link.workflow_artifacts_negotiated() {
            return Ok(());
        }
        let Some(raw) = live.placement.as_ref().map(|p| &p.demand["payload"]) else {
            return Ok(());
        };
        if !raw["ci_task"]["artifact_workflow"].is_string() {
            return Ok(());
        }
        let payload: AttemptPayload = serde_json::from_value(raw.clone())
            .map_err(|e| CoordinatorError::Invalid(e.to_string()))?;
        if payload_hash(&payload).ok().as_ref() != Some(&live.record.payload_hash) {
            return Err(CoordinatorError::Invalid(
                "artifact placement payload mismatch".into(),
            ));
        }
        let Some(ci) = payload.ci_task else {
            return Ok(());
        };
        let Some(workflow) = ci.artifact_workflow.clone() else {
            return Ok(());
        };
        let Some(ArtifactExchangeResult::Pending {
            action: Some(action),
        }) = self
            .artifact_call(link, &live.record, WorkflowArtifactCommand::Poll)
            .await?
        else {
            return Ok(());
        };
        let key = action.key.clone();
        let result = self
            .process_artifact(link, &live.record, payload.run_id, workflow, ci, action)
            .await;
        let answer = match result {
            Ok(Some(ok)) => ok,
            Ok(None) => return Ok(()),
            Err(
                CoordinatorError::Invalid(reason)
                | CoordinatorError::Storage(StorageError::Invalid(reason)),
            ) => ArtifactAnswer::Failed { reason },
            Err(e) => return Err(e),
        };
        self.artifact_call(
            link,
            &live.record,
            WorkflowArtifactCommand::Answer { key, answer },
        )
        .await?;
        Ok(())
    }
    async fn process_artifact(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        run: RunId,
        workflow: String,
        ci: CiTask,
        action: ArtifactAction,
    ) -> Result<Option<ArtifactAnswer>> {
        let attempt = a.attempt_id.clone();
        match action.intent {
            ArtifactIntent::Catalog { name, pattern } => {
                let entries = self
                    .db(move |s| {
                        s.workflow_download(&run, &workflow, &attempt, &action.key, name, pattern)
                    })
                    .await?;
                Ok(Some(ArtifactAnswer::Catalog { artifacts: entries }))
            }
            ArtifactIntent::Fetch {
                producer,
                name,
                version,
                digest,
                offset,
            } => {
                let data = self
                    .db(move |s| {
                        let receipt =
                            s.workflow_version(&run, &workflow, &name, version, &producer)?;
                        let f = receipt
                            .files
                            .iter()
                            .find(|f| digest.as_str().strip_prefix("sha256:") == Some(&f.sha256))
                            .ok_or_else(|| {
                                StorageError::Invalid("digest not in workflow receipt".into())
                            })?;
                        if offset >= f.size {
                            return Err(StorageError::Invalid(
                                "invalid artifact read offset".into(),
                            ));
                        }
                        let files = ResultStore::open_read_only(&s.state_dir().join("results"))
                            .map_err(store_error)?;
                        let mut src = files
                            .open_artifact(&producer, &digest, f.size)
                            .map_err(store_error)?;
                        src.seek(SeekFrom::Start(offset))?;
                        let mut data = vec![0; (f.size - offset).min(CHUNK as u64) as usize];
                        src.read_exact(&mut data)?;
                        Ok(Bytes(data))
                    })
                    .await?;
                Ok(Some(ArtifactAnswer::Chunk { data }))
            }
            ArtifactIntent::Upload { receipt, overwrite } => {
                let receipt: Receipt = serde_json::from_value(receipt)
                    .map_err(|_| CoordinatorError::Invalid("invalid artifact receipt".into()))?;
                let state = self.store.state_dir().to_owned();
                let verified = receipt.clone();
                let wf = workflow.clone();
                let files = tokio::task::spawn_blocking(move || {
                    verify_manifest(&state, &ci, &wf, &verified)?;
                    ResultStore::open(&state.join("results"))
                        .map_err(|e| CoordinatorError::Results(e.to_string()))
                })
                .await
                .map_err(|_| CoordinatorError::Aborted)??;
                let digests = receipt
                    .files
                    .iter()
                    .map(|f| Digest::parse(&format!("sha256:{}", f.sha256)))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| CoordinatorError::Invalid(e.to_string()))?;
                let producer = attempt.clone();
                self.db(move |s| s.reserve_workflow_files(&producer, digests, wall_ms()))
                    .await?;
                // Take at most one chunk this pass so lease renewal of other
                // attempts always gets a turn. The synced part determines the
                // next offset after a lost reply or parent restart.
                for f in &receipt.files {
                    let digest = Digest::parse(&format!("sha256:{}", f.sha256))
                        .map_err(|e| CoordinatorError::Invalid(e.to_string()))?;
                    let size = f.size;
                    let p = files.clone();
                    let d = digest.clone();
                    let producer = attempt.clone();
                    let status =
                        tokio::task::spawn_blocking(move || p.artifact_state(&producer, &d, size))
                            .await
                            .map_err(|_| CoordinatorError::Aborted)?
                            .map_err(|e| CoordinatorError::Results(e.to_string()))?;
                    let ArtifactState::Partial(offset) = status else {
                        continue;
                    };
                    let data = if offset < size {
                        let cmd = WorkflowArtifactCommand::Read {
                            key: action.key.clone(),
                            digest: digest.clone(),
                            offset,
                        };
                        let Some(ArtifactExchangeResult::Chunk { data }) =
                            self.artifact_call(link, a, cmd).await?
                        else {
                            return Ok(None);
                        };
                        if data.0.len() as u64 != (size - offset).min(CHUNK as u64) {
                            return Err(CoordinatorError::Invalid(
                                "worker returned incorrect artifact chunk length".into(),
                            ));
                        }
                        data.0
                    } else {
                        Vec::new()
                    };
                    let p = files.clone();
                    let producer = attempt.clone();
                    tokio::task::spawn_blocking(move || {
                        let end = p
                            .append_part(&producer, &digest, offset, &data)
                            .map_err(store_error)?;
                        if end == size
                            && !matches!(
                                p.publish_artifact(&producer, &digest, size)
                                    .map_err(store_error)?,
                                Publish::Published
                            )
                        {
                            return Err(StorageError::Invalid(
                                "uploaded artifact digest mismatch".into(),
                            ));
                        }
                        Ok(())
                    })
                    .await
                    .map_err(|_| CoordinatorError::Aborted)??;
                    return Ok(None);
                }
                let version = self
                    .db(move |s| {
                        s.publish_workflow_artifact(
                            WorkflowUpload {
                                run: &run,
                                workflow: &workflow,
                                producer: &attempt,
                                key: &action.key,
                                overwrite,
                                receipt,
                            },
                            wall_ms(),
                        )
                    })
                    .await?
                    .receipt
                    .version;
                Ok(Some(ArtifactAnswer::Uploaded { version }))
            }
        }
    }
}

fn store_error(e: impl std::fmt::Display) -> StorageError {
    StorageError::Corrupt(e.to_string())
}

fn verify_manifest(
    state: &std::path::Path,
    ci: &CiTask,
    workflow: &str,
    receipt: &Receipt,
) -> Result<()> {
    let invalid = |s: &str| CoordinatorError::Invalid(s.into());
    local_ci::artifact::validate_name(&receipt.name).map_err(CoordinatorError::Invalid)?;
    if receipt.files.is_empty() || receipt.files.len() > crate::outputs::MAX_FILES {
        return Err(invalid("invalid artifact file count"));
    }
    let mut total = 0u64;
    let mut paths = BTreeSet::new();
    for f in &receipt.files {
        if !crate::outputs::valid_path(&f.path) || !paths.insert(&f.path) {
            return Err(invalid("invalid artifact path"));
        }
        Digest::parse(&format!("sha256:{}", f.sha256))
            .map_err(|_| invalid("invalid file digest"))?;
        total = total
            .checked_add(f.size)
            .filter(|v| *v <= crate::outputs::MAX_BUNDLE_BYTES)
            .ok_or_else(|| invalid("artifact exceeds size limit"))?;
    }
    let bytes = crate::ci::advance::plan_bytes(state, &ci.plan_digest)
        .map_err(CoordinatorError::Results)?;
    let plan: local_ci::FrozenPlan =
        serde_json::from_slice(&bytes).map_err(|e| CoordinatorError::Results(e.to_string()))?;
    let origin = ci
        .dispatch
        .as_ref()
        .map(|d| {
            crate::ci::execute::decode_dispatch(d, &ci.task_id).map(|d| d.origin().to_string())
        })
        .transpose()
        .map_err(CoordinatorError::Invalid)?;
    let task = plan
        .task(origin.as_deref().unwrap_or(&ci.task_id))
        .ok_or_else(|| invalid("missing artifact producer task"))?;
    if plan.digest != receipt.plan_digest
        || task.workflow != workflow
        || !task
            .jobs
            .iter()
            .any(|j| format!("{workflow}::{j}") == receipt.job)
    {
        return Err(invalid(
            "artifact receipt does not belong to the placed job",
        ));
    }
    Ok(())
}
