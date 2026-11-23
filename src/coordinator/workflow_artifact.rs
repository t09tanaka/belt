//! Live exchange for a step on a worker. The placement fixes the run and
//! workflow; no command from the worker supplies either scope.
use super::*;
use crate::ci::artifact_exchange::CHUNK;
use crate::ids::Digest;
use crate::results::{
    store::{ArtifactState, Publish, StoreError},
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
        let reading = matches!(&command, WorkflowArtifactCommand::Read { .. });
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
            Err(LinkError::Remote(e)) if reading && e.code == ErrorCode::InvalidInput => Err(
                CoordinatorError::Invalid("worker could not read the staged artifact".into()),
            ),
            _ => Ok(None),
        }
    }
    /// What the worker asks of the parent for a step, answered. Like every
    /// other answer of a worker, what it asks is applied (written here) only
    /// under the actor and while `asked` is still the worker's connection.
    pub(super) async fn follow_artifacts(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
        asked: &Attached,
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
        let Some(ArtifactExchangeResult::Pending {
            action: Some(action),
        }) = self
            .artifact_call(link, &live.record, WorkflowArtifactCommand::Poll)
            .await?
        else {
            return Ok(());
        };
        let key = action.key.clone();
        let answer = match self.placed_artifact(link, live, raw, action, asked).await {
            Ok(Some(ok)) => ok,
            Ok(None) => return Ok(()),
            // What went wrong with this one action is the answer to it: the
            // step that asked fails, and the walk goes on to the next attempt.
            Err(e) => match action_failure(e)? {
                Failure::Answer(reason) => {
                    eprintln!(
                        "workflow artifact action of attempt {} failed: {reason}",
                        live.record.attempt_id
                    );
                    ArtifactAnswer::Failed { reason }
                }
                // Nothing is answered: the next poll finds the same action.
                Failure::Retry(why) => {
                    eprintln!(
                        "workflow artifact action of attempt {} is tried again: {why}",
                        live.record.attempt_id
                    );
                    return Ok(());
                }
            },
        };
        self.artifact_call(
            link,
            &live.record,
            WorkflowArtifactCommand::Answer { key, answer },
        )
        .await?;
        Ok(())
    }
    /// The action of a step, in the run and workflow its placement fixed.
    async fn placed_artifact(
        &self,
        link: &dyn WorkerLink,
        live: &LiveAttempt,
        raw: &serde_json::Value,
        action: ArtifactAction,
        asked: &Attached,
    ) -> Result<Option<ArtifactAnswer>> {
        let invalid = |s: &str| CoordinatorError::Invalid(s.into());
        let payload: AttemptPayload = serde_json::from_value(raw.clone())
            .map_err(|e| CoordinatorError::Invalid(e.to_string()))?;
        if payload_hash(&payload).ok().as_ref() != Some(&live.record.payload_hash) {
            return Err(invalid("artifact placement payload mismatch"));
        }
        let ci = payload
            .ci_task
            .ok_or_else(|| invalid("artifact placement is not a CI task"))?;
        let workflow = ci
            .artifact_workflow
            .clone()
            .ok_or_else(|| invalid("artifact placement has no workflow"))?;
        self.process_artifact(
            link,
            &live.record,
            payload.run_id,
            workflow,
            ci,
            action,
            asked,
        )
        .await
    }
    /// `None` when nothing is answered: also when the connection that asked
    /// is gone, and then nothing was written either.
    #[allow(clippy::too_many_arguments)]
    async fn process_artifact(
        &self,
        link: &dyn WorkerLink,
        a: &AttemptRecord,
        run: RunId,
        workflow: String,
        ci: CiTask,
        action: ArtifactAction,
        asked: &Attached,
    ) -> Result<Option<ArtifactAnswer>> {
        let attempt = a.attempt_id.clone();
        match action.intent {
            ArtifactIntent::Catalog { name, pattern } => {
                let Some(_actor) = self.answered(&a.worker_id, asked).await? else {
                    return Ok(None);
                };
                let entries = self
                    .db(move |s| {
                        s.workflow_download(&run, &workflow, &attempt, &action.key, name, pattern)
                    })
                    .await?;
                Ok(entries.map(|artifacts| ArtifactAnswer::Catalog { artifacts }))
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
                            .map_err(published_error)?;
                        let mut src = files
                            .open_artifact(&producer, &digest, f.size)
                            .map_err(published_error)?;
                        src.seek(SeekFrom::Start(offset))?;
                        let mut data = vec![0; (f.size - offset).min(CHUNK as u64) as usize];
                        src.read_exact(&mut data)?;
                        Ok(Bytes(data))
                    })
                    .await?;
                Ok(Some(ArtifactAnswer::Chunk { data }))
            }
            ArtifactIntent::Upload { receipt, overwrite } => {
                // Held while this writes, and let go while the worker is
                // asked for a chunk.
                let Some(actor) = self.answered(&a.worker_id, asked).await? else {
                    return Ok(None);
                };
                let mut actor = Some(actor);
                let receipt: Receipt = serde_json::from_value(receipt)
                    .map_err(|_| CoordinatorError::Invalid("invalid artifact receipt".into()))?;
                let state = self.store.state_dir().to_owned();
                let verified = receipt.clone();
                let wf = workflow.clone();
                let declaring = attempt.clone();
                let files = tokio::task::spawn_blocking(move || {
                    let files = ResultStore::open(&state.join("results"))
                        .map_err(|e| CoordinatorError::Results(e.to_string()))?;
                    verify_manifest(&state, &files, &declaring, &ci, &wf, &verified)?;
                    Ok::<_, CoordinatorError>(files)
                })
                .await
                .map_err(|_| CoordinatorError::Aborted)??;
                let declarations = receipt.clone();
                let scope = workflow.clone();
                let run_id = run.clone();
                let key = action.key.clone();
                let producer = attempt.clone();
                self.db(move |s| {
                    s.reserve_workflow_files(
                        WorkflowUpload {
                            run: &run_id,
                            workflow: &scope,
                            producer: &producer,
                            key: &key,
                            overwrite,
                            receipt: declarations,
                        },
                        wall_ms(),
                    )
                })
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
                    // The actor while the part is written.
                    let (data, _actor) = if offset < size {
                        let cmd = WorkflowArtifactCommand::Read {
                            key: action.key.clone(),
                            digest: digest.clone(),
                            offset,
                        };
                        drop(actor.take());
                        let Some(ArtifactExchangeResult::Chunk { data }) =
                            self.artifact_call(link, a, cmd).await?
                        else {
                            return Ok(None);
                        };
                        let Some(again) = self.answered(&a.worker_id, asked).await? else {
                            return Ok(None);
                        };
                        if data.0.len() as u64 != (size - offset).min(CHUNK as u64) {
                            return Err(CoordinatorError::Invalid(
                                "worker returned incorrect artifact chunk length".into(),
                            ));
                        }
                        (data.0, Some(again))
                    } else {
                        (Vec::new(), actor.take())
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

/// A failure of the result store is a fault of this machine, not a verdict
/// on the bytes: the same call may succeed at the next poll. A length the
/// store did not expect is one too, as the next poll reads it again.
fn store_error(e: StoreError) -> StorageError {
    match e {
        StoreError::Io(e) => StorageError::Io(e),
        StoreError::BadName(_) => StorageError::Invalid(e.to_string()),
        StoreError::Offset { .. } | StoreError::Crash(_) => {
            StorageError::Io(std::io::Error::other(e.to_string()))
        }
    }
}

/// Opening a published file for a reader also gives two verdicts: the file
/// is not there, or it is not the one its receipt names.
fn published_error(e: StoreError) -> StorageError {
    match e {
        StoreError::Io(e) if e.kind() == std::io::ErrorKind::NotFound => StorageError::NotFound {
            what: "published artifact file".into(),
        },
        StoreError::Io(e) if e.kind() == std::io::ErrorKind::InvalidData => {
            StorageError::Corrupt(e.to_string())
        }
        e => store_error(e),
    }
}

/// What one failed action comes to when the walk goes on.
#[derive(Debug, PartialEq, Eq)]
enum Failure {
    /// The verdict on the action: its step fails with this reason.
    Answer(String),
    /// A fault that may be gone at the next poll. Nothing is answered.
    Retry(String),
}

/// What one failed action answers its step, or the error that stops the
/// walk: this process is not the coordinator any more, or its database (not
/// one row or one file of it) cannot be used now. Such an error says nothing
/// about the action, which is asked again by the next poll. So is one that
/// came from reading or writing a file, which is only not answered.
fn action_failure(e: CoordinatorError) -> Result<Failure> {
    match e {
        CoordinatorError::Fenced(_)
        | CoordinatorError::Aborted
        | CoordinatorError::Storage(
            StorageError::Sqlite(_)
            | StorageError::Busy
            | StorageError::Incompatible(_)
            | StorageError::MigrationRefused(_)
            | StorageError::KindMismatch { .. }
            | StorageError::Locked(_)
            | StorageError::StaleEpoch { .. }
            | StorageError::Closed
            | StorageError::Panicked,
        ) => Err(e),
        CoordinatorError::Results(_)
        | CoordinatorError::Storage(
            StorageError::Io(_)
            | StorageError::SourceBusy
            | StorageError::Overloaded(_)
            | StorageError::AttemptIdInUse(_)
            | StorageError::Capacity { .. }
            | StorageError::AttemptRecords(_),
        ) => Ok(Failure::Retry(e.to_string())),
        CoordinatorError::Invalid(reason)
        | CoordinatorError::Storage(StorageError::Invalid(reason)) => Ok(Failure::Answer(reason)),
        CoordinatorError::Storage(
            StorageError::Corrupt(_)
            | StorageError::NotFound { .. }
            | StorageError::Immutable(_)
            | StorageError::PayloadConflict(_)
            | StorageError::LocatorPendingDelete(_)
            | StorageError::Retired(_)
            | StorageError::LiveAttemptExists(_)
            | StorageError::LeaseNotRenewable(_)
            | StorageError::Transition(_)
            | StorageError::IllegalState { .. }
            | StorageError::SecretUnavailable,
        ) => Ok(Failure::Answer(e.to_string())),
    }
}

/// A digest names bytes of one length. A declaration that gives it another
/// length than the file this producer already published under it, or two
/// lengths at once, is refused here: the published file is left as it is.
/// A published file of another length that does not hash to its digest was
/// damaged where it lay and refuses nothing: the upload replaces it.
fn declared_sizes(files: &ResultStore, producer: &AttemptId, receipt: &Receipt) -> Result<()> {
    let invalid = |s: &str| CoordinatorError::Invalid(s.into());
    let mut sizes = BTreeMap::new();
    for f in &receipt.files {
        let digest = Digest::parse(&format!("sha256:{}", f.sha256))
            .map_err(|_| invalid("invalid file digest"))?;
        if *sizes.entry(f.sha256.as_str()).or_insert(f.size) != f.size {
            return Err(invalid("artifact declares one digest with two sizes"));
        }
        let results = |e: StoreError| CoordinatorError::Results(e.to_string());
        let Some(len) = files.published_len(producer, &digest).map_err(results)? else {
            continue;
        };
        if len != f.size
            && files
                .verify_artifact(producer, &digest, len)
                .map_err(results)?
        {
            return Err(invalid(
                "artifact size conflicts with the published file of its digest",
            ));
        }
    }
    Ok(())
}

fn verify_manifest(
    state: &std::path::Path,
    files: &ResultStore,
    producer: &AttemptId,
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
    declared_sizes(files, producer, receipt)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    fn storage(e: StorageError) -> Result<Failure> {
        action_failure(CoordinatorError::Storage(e))
    }

    #[test]
    fn a_fault_of_the_store_is_tried_again_and_a_verdict_is_answered() {
        let retried = |r: Result<Failure>| matches!(r, Ok(Failure::Retry(_)));
        let answered = |r: Result<Failure>| matches!(r, Ok(Failure::Answer(_)));
        let io = |kind: ErrorKind| StoreError::Io(Error::new(kind, "file"));
        // The walk stops.
        assert!(action_failure(CoordinatorError::Aborted).is_err());
        assert!(action_failure(CoordinatorError::Fenced(CoordinatorEpoch(1))).is_err());
        assert!(storage(StorageError::Busy).is_err());
        assert!(storage(StorageError::Closed).is_err());
        // The action is left for the next poll.
        assert!(retried(storage(StorageError::SourceBusy)));
        assert!(retried(storage(StorageError::Io(Error::other("disk")))));
        assert!(retried(action_failure(CoordinatorError::Results(
            "disk".into()
        ))));
        assert!(retried(storage(store_error(io(ErrorKind::NotFound)))));
        assert!(retried(storage(store_error(StoreError::Offset {
            want: 1,
            have: 2
        }))));
        assert!(retried(storage(published_error(io(
            ErrorKind::PermissionDenied
        )))));
        // The step fails.
        assert_eq!(
            action_failure(CoordinatorError::Invalid("bad".into())).unwrap(),
            Failure::Answer("bad".into())
        );
        assert_eq!(
            storage(StorageError::Invalid("bad".into())).unwrap(),
            Failure::Answer("bad".into())
        );
        assert!(answered(storage(StorageError::Corrupt("digest".into()))));
        assert!(answered(storage(StorageError::Immutable("v1".into()))));
        assert!(answered(storage(StorageError::NotFound {
            what: "blob".into()
        })));
        assert!(answered(storage(store_error(StoreError::BadName(
            "..".into()
        )))));
        assert!(answered(storage(published_error(io(ErrorKind::NotFound)))));
        assert!(answered(storage(published_error(io(
            ErrorKind::InvalidData
        )))));
    }
}
