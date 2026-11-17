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

impl WorkflowUpload<'_> {
    fn fingerprint(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&(self.overwrite, &self.receipt))
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        Ok(Digest::of_bytes(&bytes).to_string())
    }
}

impl CoordinatorStore {
    pub(crate) fn reserve_workflow_files(
        &self,
        upload: WorkflowUpload<'_>,
        at_ms: u64,
    ) -> Result<()> {
        let producer = upload.producer.clone();
        let key = upload.key.to_string();
        let workflow = upload.workflow.to_owned();
        let fingerprint = upload.fingerprint()?;
        let receipt = upload.receipt;
        self.db.write(move |tx| {
            let inserted=tx.execute(
                "INSERT INTO workflow_upload_inflight(producer,action_key,workflow,name,fingerprint)
                 VALUES(?1,?2,?3,?4,?5) ON CONFLICT(producer,action_key) DO NOTHING",
                params![producer.as_str(),key,workflow,receipt.name,fingerprint])?;
            if inserted==0 {
                let same:bool=tx.query_row("SELECT workflow=?3 AND fingerprint=?4 FROM workflow_upload_inflight WHERE producer=?1 AND action_key=?2",
                    params![producer.as_str(),key,workflow,fingerprint],|r|r.get(0))?;
                if !same {return Err(StorageError::Invalid("artifact proposal changed during transfer".into()));}
                return Ok(());
            }
            for f in receipt.files {
                let digest=Digest::parse(&format!("sha256:{}",f.sha256)).map_err(|e|StorageError::Invalid(e.to_string()))?;
                let locator = ResultStore::artifact_locator(&producer, &digest);
                add_ref(
                    tx,
                    producer.as_str(),
                    &ResultRef {
                        ref_id: format!("ci-artifact/{producer}/{digest}"),
                        kind: BlobKind::Result,
                        locator,
                    },
                    at_ms,
                )?;
            }
            Ok(())
        })
    }

    pub(crate) fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }

    pub(crate) fn workflow_download(
        &self,
        run: &RunId,
        workflow: &str,
        consumer: &AttemptId,
        key: &Digest,
        name: Option<String>,
        pattern: Option<String>,
    ) -> Result<Option<Vec<serde_json::Value>>> {
        let (run, workflow, consumer, key) = (
            run.to_string(),
            workflow.to_owned(),
            consumer.to_string(),
            key.to_string(),
        );
        self.db.write(move|tx| {
            let fp=serde_json::to_string(&(&run,&workflow,&name,&pattern)).map_err(|e|StorageError::Invalid(e.to_string()))?;
            if let Some((prior,selection))=tx.query_row(
                "SELECT fingerprint,selection_json FROM workflow_download WHERE consumer=?1 AND action_key=?2",
                params![consumer,key],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()? {
                if prior!=fp {return Err(StorageError::Invalid("artifact download replay changed".into()));}
                return serde_json::from_str(&selection).map(Some).map_err(|e|StorageError::Corrupt(e.to_string()));
            }
            let mut q=tx.prepare("SELECT name,producer,receipt_json FROM workflow_artifact a
                WHERE run_id=?1 AND workflow=?2 AND version=(SELECT max(version) FROM workflow_artifact v
                WHERE v.run_id=a.run_id AND v.workflow=a.workflow AND v.name=a.name) ORDER BY name LIMIT 1025")?;
            let mut all=std::collections::BTreeMap::new();
            for row in q.query_map(params![run,workflow],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?)))? {
                let (name,producer,receipt)=row?;
                all.insert(name,(producer,receipt));
            }
            if all.len()>1024 {return Err(StorageError::Invalid("too many artifacts in workflow".into()));}
            let names=all.keys().cloned().collect::<Vec<_>>();
            let selected=match local_ci::artifact::select_artifacts(&names,name.as_deref(),pattern.as_deref()) {
                Ok(selected)=>selected,
                Err(missing)=>{
                    let mut pending=tx.prepare("SELECT DISTINCT u.name FROM workflow_upload_inflight u JOIN attempt a ON a.attempt_id=u.producer
                        JOIN task t USING(task_id) WHERE t.run_id=?1 AND u.workflow=?2 AND u.producer<>?3 AND a.retired=0 AND a.state<>'terminal'
                        AND a.cancel_requested_at_ms IS NULL AND t.cancel_requested_at_ms IS NULL AND NOT EXISTS(
                        SELECT 1 FROM workflow_artifact w WHERE w.producer=u.producer AND w.action_key=u.action_key) LIMIT 1025")?;
                    let names=pending.query_map(params![run,workflow,consumer],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
                    if local_ci::artifact::select_artifacts(&names,name.as_deref(),pattern.as_deref()).is_ok(){return Ok(None);}
                    return Err(StorageError::Invalid(missing));
                }
            };
            let mut entries=Vec::new();
            let mut bytes=0usize;
            for n in selected {
                let (producer,receipt)=&all[&n];
                bytes+=receipt.len()+producer.len()+64;
                if bytes>crate::ci::artifact_exchange::MAX_MESSAGE/2 {return Err(StorageError::Invalid("selected artifact catalog exceeds transfer limit".into()));}
                let receipt:serde_json::Value=serde_json::from_str(receipt).map_err(|e|StorageError::Corrupt(e.to_string()))?;
                entries.push(serde_json::json!({"producer":producer,"receipt":receipt}));
            }
            let json=serde_json::to_string(&entries).map_err(|e|StorageError::Corrupt(e.to_string()))?;
            tx.execute("INSERT INTO workflow_download VALUES(?1,?2,?3,?4)",params![consumer,key,fp,json])?;
            Ok(Some(entries))
        })
    }

    pub(crate) fn workflow_version(
        &self,
        run: &RunId,
        workflow: &str,
        name: &str,
        version: u32,
        producer: &AttemptId,
    ) -> Result<Receipt> {
        let (run, workflow, name, producer) = (
            run.to_string(),
            workflow.to_owned(),
            name.to_owned(),
            producer.to_string(),
        );
        self.db.call(move |c| {
            let raw: Option<String> = c
                .query_row(
                    "SELECT receipt_json FROM workflow_artifact
                WHERE run_id=?1 AND workflow=?2 AND name=?3 AND version=?4 AND producer=?5",
                    params![run, workflow, name, version, producer],
                    |r| r.get(0),
                )
                .optional()?;
            serde_json::from_str(
                &raw.ok_or_else(|| {
                    StorageError::Invalid("no such workflow artifact version".into())
                })?,
            )
            .map_err(|e| StorageError::Corrupt(e.to_string()))
        })
    }

    /// Called only after the collector has sealed every file in the result
    /// store. The version, action replay identity and GC references are one
    /// transaction, including across coordinator restarts.
    pub fn publish_workflow_artifact(
        &self,
        upload: WorkflowUpload<'_>,
        at_ms: u64,
    ) -> Result<WorkflowArtifact> {
        let fingerprint = upload.fingerprint()?;
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
            let mut names = tx.prepare("SELECT DISTINCT name FROM workflow_artifact WHERE run_id=?1 AND workflow=?2 ORDER BY name LIMIT 1025")?;
            let existing=names.query_map(params![run,workflow], |r|r.get::<_,String>(0))?
                .collect::<std::result::Result<Vec<_>,_>>()?;
            if existing.len() >= crate::outputs::MAX_OUTPUTS && !existing.contains(&receipt.name) {
                return Err(StorageError::Invalid("too many artifacts in this workflow".into()));
            }
            local_ci::artifact::name_collision(&existing, &receipt.name).map_err(StorageError::Invalid)?;
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
