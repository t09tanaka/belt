//! Live CI artifact receipts, independent of terminal attempt collection.
use super::{collect::add_ref, CoordinatorStore, ResultRef};
use crate::ids::{AttemptId, Digest, RunId};
use crate::results::ResultStore;
use crate::storage::{BlobKind, Result, StorageError};
use local_ci::artifact_io::Receipt;
use rusqlite::{params, OptionalExtension};

/// Parent-owned identity of a version and the worker's verified manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkflowArtifact {
    pub producer: AttemptId,
    pub receipt: Receipt,
}

pub struct WorkflowUpload<'a> {
    pub run: &'a RunId,
    pub workflow: &'a str,
    pub producer: &'a AttemptId,
    pub key: &'a Digest,
    pub overwrite: bool,
    pub receipt: Receipt,
}

impl CoordinatorStore {
    /// Called only after the collector has sealed every file in the result
    /// store. The version, action replay identity and GC references are one
    /// transaction, including across coordinator restarts.
    pub fn publish_workflow_artifact(
        &self,
        upload: WorkflowUpload<'_>,
        at_ms: u64,
    ) -> Result<WorkflowArtifact> {
        let WorkflowUpload {
            run,
            workflow,
            producer,
            key,
            overwrite,
            mut receipt,
        } = upload;
        let files = ResultStore::open(&self.state_dir.join("results"))
            .map_err(|e| StorageError::Corrupt(e.to_string()))?;
        let mut paths = std::collections::BTreeSet::new();
        let mut total = 0u64;
        if receipt.files.is_empty() || receipt.files.len() > crate::outputs::MAX_FILES {
            return Err(StorageError::Invalid("invalid artifact file count".into()));
        }
        for file in &receipt.files {
            if !crate::outputs::valid_path(&file.path) || !paths.insert(&file.path) {
                return Err(StorageError::Invalid(
                    "invalid or duplicate artifact path".into(),
                ));
            }
            total = total
                .checked_add(file.size)
                .filter(|s| *s <= crate::outputs::MAX_BUNDLE_BYTES)
                .ok_or_else(|| StorageError::Invalid("artifact exceeds size limit".into()))?;
            let digest = Digest::parse(&format!("sha256:{}", file.sha256))
                .map_err(|e| StorageError::Invalid(e.to_string()))?;
            if !files
                .verify_artifact(producer, &digest, file.size)
                .map_err(|e| StorageError::Corrupt(e.to_string()))?
            {
                return Err(StorageError::Invalid(
                    "artifact bytes failed verification".into(),
                ));
            }
        }
        files
            .seal(producer)
            .map_err(|e| StorageError::Corrupt(e.to_string()))?;
        let (run, workflow, producer, key) = (
            run.to_string(),
            workflow.to_owned(),
            producer.clone(),
            key.to_string(),
        );
        local_ci::artifact::validate_name(&receipt.name).map_err(StorageError::Invalid)?;
        let encoded = serde_json::to_vec(&(overwrite, &receipt))
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        let fingerprint = Digest::of_bytes(&encoded).to_string();
        self.db.write(move |tx| {
            let active: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempt a JOIN task t USING(task_id)
                 WHERE a.attempt_id=?1 AND t.run_id=?2 AND a.retired=0 AND a.state<>'terminal'
                 AND a.generation>=t.min_generation AND t.cancel_requested_at_ms IS NULL)",
                params![producer.as_str(),run], |r| r.get(0))?;
            if !active {
                return Err(StorageError::Invalid("artifact producer is no longer active".into()));
            }
            let prior: Option<(String,String,String)> = tx.query_row(
                "SELECT workflow,fingerprint,receipt_json FROM workflow_artifact WHERE producer=?1 AND action_key=?2",
                params![producer.as_str(),key], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            if let Some((prior_workflow, prior_fingerprint, json)) = prior {
                if prior_workflow != workflow || prior_fingerprint != fingerprint {
                    return Err(StorageError::Invalid("artifact action replay changed its request".into()));
                }
                return Ok(WorkflowArtifact { producer, receipt: serde_json::from_str(&json)
                    .map_err(|e| StorageError::Corrupt(e.to_string()))? });
            }
            let last: u32 = tx.query_row(
                "SELECT coalesce(max(version),0) FROM workflow_artifact WHERE run_id=?1 AND workflow=?2 AND name=?3",
                params![run,workflow,receipt.name], |r| r.get(0))?;
            if last != 0 && !overwrite {
                return Err(StorageError::Invalid(format!("artifact '{}' already exists", receipt.name)));
            }
            receipt.version = last.checked_add(1).ok_or_else(|| StorageError::Invalid("too many artifact versions".into()))?;
            for file in &receipt.files {
                let digest = Digest::parse(&format!("sha256:{}", file.sha256))
                    .map_err(|e| StorageError::Invalid(e.to_string()))?;
                let locator = ResultStore::artifact_locator(&producer, &digest);
                add_ref(tx,producer.as_str(),&ResultRef {
                    ref_id: format!("ci-artifact/{producer}/{digest}"), kind: BlobKind::Result, locator,
                },at_ms)?;
            }
            let json = serde_json::to_string(&receipt).map_err(|e| StorageError::Invalid(e.to_string()))?;
            tx.execute("INSERT INTO workflow_artifact(run_id,workflow,name,version,producer,action_key,fingerprint,receipt_json)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![run, workflow, receipt.name, receipt.version, producer.as_str(),key,fingerprint,json])?;
            Ok(WorkflowArtifact { producer, receipt })
        })
    }

    /// A download takes the newest immutable version of each visible name.
    pub fn workflow_artifacts(&self, run: &RunId, workflow: &str) -> Result<Vec<WorkflowArtifact>> {
        let (run, workflow) = (run.to_string(), workflow.to_owned());
        self.db.call(move |c| {
            let mut stmt = c.prepare("SELECT producer,receipt_json FROM workflow_artifact w
                WHERE run_id=?1 AND workflow=?2 AND version=(SELECT max(version) FROM workflow_artifact v
                    WHERE v.run_id=w.run_id AND v.workflow=w.workflow AND v.name=w.name) ORDER BY name")?;
            let rows = stmt.query_map(params![run,workflow], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
            rows.map(|row| {
                let (producer,json) = row?;
                Ok(WorkflowArtifact { producer: super::id("producer",AttemptId::parse(&producer))?,
                    receipt:serde_json::from_str(&json).map_err(|e| StorageError::Corrupt(e.to_string()))? })
            }).collect()
        })
    }
}
