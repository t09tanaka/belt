//! The executor waits at the artifact step while the parent verifies the
//! uploaded bytes or supplies a workflow-scoped download.
use super::*;
use crate::secrets::{mask::Masker, place::FileIdentity};
use crate::storage::WorkflowArtifact;
use local_ci::{
    artifact,
    artifact_io::{Area, Receipt},
    step_executor::ArtifactRequest,
};
use sha2::{Digest as _, Sha256};
use std::sync::Mutex;
use std::time::Duration;

pub(crate) struct Host {
    pub root: PathBuf,
    pub workflow: Option<String>,
    pub secret_files: Vec<FileIdentity>,
    pub secret_paths: Vec<String>,
    pub masker: Masker,
    pub serial: Mutex<()>,
}
impl Host {
    pub fn run(&self, req: &ArtifactRequest<'_>) -> Result<String> {
        let _guard = self.serial.lock().unwrap_or_else(|e| e.into_inner());
        ensure!(
            self.workflow.as_deref() == Some(req.workflow),
            "workflow artifact exchange is not authorized for this task"
        );
        fs::create_dir_all(&self.root)?;
        let key = Digest::of_bytes(&serde_json::to_vec(&(
            req.workflow,
            &req.who.job,
            req.variant,
            req.step_index,
        ))?);
        let option = |n: &str| {
            req.with
                .iter()
                .find(|(k, _)| k == n)
                .map(|(_, v)| v.as_str())
                .filter(|v| !v.trim().is_empty())
        };
        let scratch = tempfile::tempdir_in(&self.root)?;
        let area = Area {
            root: scratch.path().join("artifacts"),
            plan_digest: req.area.plan_digest.clone(),
            attempt: req.area.attempt.clone(),
        };
        match req.kind {
            artifact::Kind::Upload => {
                let name = artifact::runtime_name(option("name").unwrap_or("artifact"))
                    .map_err(anyhow::Error::msg)?;
                let overwrite = artifact::parse_bool("overwrite", option("overwrite"))
                    .map_err(anyhow::Error::msg)?;
                let mut total = 0u64;
                let mut count = 0usize;
                let note = local_ci::artifact_io::upload_checked(
                    &area,
                    req.who,
                    req.workspace,
                    req.with,
                    |path, meta, bytes| {
                        count += 1;
                        total = total.saturating_add(bytes.len() as u64);
                        if count > crate::outputs::MAX_FILES
                            || total > crate::outputs::MAX_BUNDLE_BYTES
                        {
                            return Err("artifact exceeds transfer limits".into());
                        }
                        if self.secret_paths.iter().any(|p| p == path)
                            || self.secret_files.contains(&FileIdentity::of(meta))
                            || self.masker.contains(bytes)
                        {
                            return Err("artifact contains secret material".into());
                        }
                        Ok(())
                    },
                )
                .map_err(anyhow::Error::msg)?;
                let version = area.root.join(&name).join("v1");
                let Some(receipt) = read_json::<Receipt>(&version.join("receipt.json"))? else {
                    return Ok(note);
                };
                let blobs = self.root.join(hex(&key));
                fs::create_dir_all(&blobs)?;
                for f in &receipt.files {
                    let digest = Digest::parse(&format!("sha256:{}", f.sha256))?;
                    ensure!(crate::outputs::valid_path(&f.path), "invalid upload path");
                    let dest = blobs.join(hex(&digest));
                    match fs::hard_link(version.join("files").join(&f.path), dest) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                let answer = self.ask(
                    req,
                    &key,
                    ArtifactIntent::Upload {
                        receipt: serde_json::to_value(&receipt)?,
                        overwrite,
                    },
                )?;
                let ArtifactAnswer::Uploaded { version: published } = answer else {
                    bail!("invalid upload acknowledgement")
                };
                let mut receipt = receipt;
                receipt.version = published;
                atomic_json(&version.join("receipt.json"), &receipt)?;
                let names = req.area.root.join(&name);
                fs::create_dir_all(&names)?;
                fs::rename(&version, names.join(format!("v{published}")))?;
                Ok(format!(
                    "uploaded `{name}` v{published}: {count} files, {total} bytes"
                ))
            }
            artifact::Kind::Download => {
                let answer = self.ask(
                    req,
                    &key,
                    ArtifactIntent::Catalog {
                        name: option("name").map(str::to_owned),
                        pattern: option("pattern").map(str::to_owned),
                    },
                )?;
                let ArtifactAnswer::Catalog { artifacts } = answer else {
                    bail!("invalid artifact catalog")
                };
                for item in artifacts {
                    let item: WorkflowArtifact = serde_json::from_value(item)?;
                    let r = &item.receipt;
                    artifact::validate_name(&r.name).map_err(anyhow::Error::msg)?;
                    ensure!(
                        r.plan_digest == area.plan_digest,
                        "artifact is from another plan"
                    );
                    let vdir = area.root.join(&r.name).join(format!("v{}", r.version));
                    for f in &r.files {
                        ensure!(
                            crate::outputs::valid_path(&f.path)
                                && f.size <= crate::outputs::MAX_BUNDLE_BYTES,
                            "invalid download file"
                        );
                        let digest = Digest::parse(&format!("sha256:{}", f.sha256))?;
                        let path = vdir.join("files").join(&f.path);
                        fs::create_dir_all(path.parent().context("file parent")?)?;
                        let mut file = File::create_new(path)?;
                        let mut offset = 0;
                        let mut hash = Sha256::new();
                        while offset < f.size {
                            let part_key = Digest::of_bytes(&serde_json::to_vec(&(
                                &key,
                                &item.producer,
                                &r.name,
                                r.version,
                                &digest,
                                offset,
                            ))?);
                            let reply = self.ask(
                                req,
                                &part_key,
                                ArtifactIntent::Fetch {
                                    producer: item.producer.clone(),
                                    name: r.name.clone(),
                                    version: r.version,
                                    digest: digest.clone(),
                                    offset,
                                },
                            )?;
                            let ArtifactAnswer::Chunk { data } = reply else {
                                bail!("invalid artifact download chunk")
                            };
                            ensure!(
                                !data.0.is_empty()
                                    && data.0.len() as u64 <= (f.size - offset).min(CHUNK as u64),
                                "invalid artifact chunk length"
                            );
                            file.write_all(&data.0)?;
                            hash.update(&data.0);
                            offset += data.0.len() as u64;
                        }
                        ensure!(
                            format!("{:x}", hash.finalize()) == f.sha256,
                            "downloaded artifact failed verification"
                        );
                    }
                    atomic_json(&vdir.join("receipt.json"), r)?;
                }
                local_ci::artifact_io::download(&area, req.workspace, req.with)
                    .map_err(anyhow::Error::msg)
            }
        }
    }

    fn ask(
        &self,
        req: &ArtifactRequest<'_>,
        key: &Digest,
        intent: ArtifactIntent,
    ) -> Result<ArtifactAnswer> {
        let pending = self.root.join("pending.json");
        let reply = response_path(&self.root, key);
        atomic_json(
            &pending,
            &ArtifactAction {
                key: key.clone(),
                intent,
            },
        )?;
        let outcome = loop {
            if req.cancel.is_cancelled() {
                break Err(anyhow::anyhow!("artifact action cancelled"));
            }
            if let Some(answer) = read_json(&reply)? {
                break match answer {
                    ArtifactAnswer::Failed { reason } => Err(anyhow::anyhow!(reason)),
                    ok => Ok(ok),
                };
            }
            std::thread::sleep(Duration::from_millis(40));
        };
        fs::remove_file(pending)?;
        if reply.exists() {
            fs::remove_file(reply)?;
        }
        outcome
    }
}
