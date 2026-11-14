//! Source-derived scheduling facts omitted by the legacy frozen wire format.
//! A decoded record is untrusted until recomputed from PinnedExecution.
use super::PinnedExecution;
use crate::plan::Selected;
use crate::source::RelPath;
use crate::wire::WorkflowSpec;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Read;

const VERSION: u32 = 1;
const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_WORKFLOWS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobScheduling {
    job: String,
    runs_on: Option<Value>,
}
impl JobScheduling {
    pub fn job(&self) -> &str {
        &self.job
    }
    /// Original expression/string/labels/group mapping, without placement guesses.
    pub fn runs_on(&self) -> Option<&Value> {
        self.runs_on.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowScheduling {
    workflow: String,
    object_hash: String,
    jobs: Vec<JobScheduling>,
}
impl WorkflowScheduling {
    pub fn workflow(&self) -> &str {
        &self.workflow
    }
    pub fn object_hash(&self) -> &str {
        &self.object_hash
    }
    pub fn jobs(&self) -> &[JobScheduling] {
        &self.jobs
    }
}

/// Portable transport record; deserialization does not verify its provenance.
/// Hosts must authenticate their separately supplied github/vars/inputs facts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationSource {
    schema_version: u32,
    plan_digest: String,
    source_id: String,
    manifest_digest: String,
    workflows: Vec<WorkflowScheduling>,
}
impl EvaluationSource {
    pub fn from_json(text: &str) -> Result<Self> {
        ensure!(
            text.len() <= MAX_SOURCE_BYTES,
            "evaluation source exceeds byte bound"
        );
        Ok(serde_json::from_str(text)?)
    }
    pub fn plan_digest(&self) -> &str {
        &self.plan_digest
    }
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }
    pub fn workflows(&self) -> &[WorkflowScheduling] {
        &self.workflows
    }
}

/// Capability obtained only by reading verified captured workflow objects.
/// This deliberately has no Deserialize implementation or unchecked constructor.
#[derive(Debug, Clone)]
pub struct VerifiedEvaluationSource(EvaluationSource);
impl VerifiedEvaluationSource {
    pub fn record(&self) -> &EvaluationSource {
        &self.0
    }
}

impl PinnedExecution<'_> {
    pub fn evaluation_source(&self) -> Result<VerifiedEvaluationSource> {
        self.validate_binding(self.plan)?;
        crate::source::check_manifest_digest(self.manifest)?;
        ensure!(
            self.plan.workflows.len() <= MAX_WORKFLOWS,
            "too many evaluation workflows"
        );
        let mut workflows = Vec::new();
        let mut remaining = MAX_SOURCE_BYTES;
        for frozen in &self.plan.workflows {
            let path = RelPath::new(&frozen.file);
            let entry = self
                .manifest
                .object_manifest
                .entries
                .iter()
                .find(|e| e.path == path)
                .context("evaluation workflow is absent from captured source")?;
            ensure!(
                matches!(entry.mode, 0o100644 | 0o100755),
                "evaluation workflow is not a regular object"
            );
            ensure!(
                entry.size <= remaining as u64,
                "evaluation workflow exceeds byte bound"
            );
            ensure!(
                entry.hash.len() == 64 && entry.hash.bytes().all(|c| c.is_ascii_hexdigit()),
                "invalid evaluation object hash"
            );
            // Bound the actual read too: a corrupted store cannot force an
            // allocation based only on the smaller manifest size.
            let mut bytes = Vec::new();
            std::fs::File::open(self.store.object_path(&entry.hash))?
                .take(remaining as u64 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= remaining && bytes.len() as u64 == entry.size,
                "evaluation object size mismatch"
            );
            ensure!(
                format!("{:x}", Sha256::digest(&bytes)) == entry.hash,
                "evaluation object digest mismatch"
            );
            remaining -= bytes.len();
            let parsed = crate::workflow::parse(&frozen.file, std::str::from_utf8(&bytes)?)?;
            let projected = WorkflowSpec::from_selected(&Selected {
                wf: parsed.clone(),
                event: frozen.event.clone(),
            });
            ensure!(
                &projected == frozen,
                "evaluation workflow differs from frozen projection"
            );
            let jobs = parsed
                .jobs
                .into_iter()
                .map(|job| {
                    Ok(JobScheduling {
                        job: job.id,
                        runs_on: job.runs_on.map(serde_json::to_value).transpose()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            workflows.push(WorkflowScheduling {
                workflow: frozen.file.clone(),
                object_hash: entry.hash.clone(),
                jobs,
            });
        }
        let record = EvaluationSource {
            schema_version: VERSION,
            plan_digest: self.plan.digest.clone(),
            source_id: self.manifest.source_id.clone(),
            manifest_digest: self.manifest.manifest_digest.clone(),
            workflows,
        };
        ensure!(
            serde_json::to_vec(&record)?.len() <= MAX_SOURCE_BYTES,
            "evaluation source exceeds byte bound"
        );
        Ok(VerifiedEvaluationSource(record))
    }

    /// Recompute at the worker's source-aware boundary before accepting a
    /// transported record. Hash equality alone is not authentication.
    pub fn verify_evaluation_source(
        &self,
        record: &EvaluationSource,
    ) -> Result<VerifiedEvaluationSource> {
        let verified = self.evaluation_source()?;
        ensure!(
            &verified.0 == record,
            "evaluation source provenance mismatch"
        );
        Ok(verified)
    }
}

#[cfg(test)]
mod tests;
