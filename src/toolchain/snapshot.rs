//! Verified captured-source bridge. Never reads the caller's checkout, config,
//! HOME, secrets store, or manager executables.
use super::declarations::{self, Document, MAX_DOCUMENTS, MAX_DOCUMENT_BYTES, VERSION_FILES};
use super::versions::{Declaration, Diagnostic, Location};
use crate::source::store::{verify_manifest, SourceStore};
use local_ci::source::SourceManifest;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

pub fn read_captured(
    store: &SourceStore,
    manifest: &SourceManifest,
    project: &str,
    workflows: &[String],
) -> Result<Vec<Declaration>, Diagnostic> {
    let error = |code: &str, path: &str| Diagnostic {
        code: code.into(),
        locations: vec![Location {
            path: path.chars().take(1024).collect(),
            first_line: 1,
            last_line: 1,
        }],
    };
    if workflows.len() > MAX_DOCUMENTS {
        return Err(error("too_many_workflows", ""));
    }
    verify_manifest(manifest).map_err(|_| error("invalid_source_manifest", ""))?;
    if !project.is_empty() && !declarations::safe_path(project) {
        return Err(error("invalid_project_path", project));
    }
    let mut paths: BTreeSet<_> = VERSION_FILES
        .iter()
        .map(|name| {
            if project.is_empty() {
                name.to_string()
            } else {
                format!("{project}/{name}")
            }
        })
        .collect();
    paths.extend(workflows.iter().cloned());
    let mut documents = BTreeMap::new();
    let mut visited = BTreeSet::new();
    while let Some(path) = paths.pop_first() {
        if !visited.insert(path.clone()) {
            continue;
        }
        if visited.len() > MAX_DOCUMENTS || !declarations::safe_path(&path) {
            return Err(error("invalid_document_set", &path));
        }
        if manifest
            .secret_files
            .iter()
            .any(|e| e.path.0 == path.as_bytes())
        {
            return Err(error("secret_version_document", &path));
        }
        let Some(entry) = manifest
            .object_manifest
            .entries
            .iter()
            .find(|e| e.path.0 == path.as_bytes())
        else {
            continue;
        };
        if entry.mode != 0o100644 && entry.mode != 0o100755 {
            return Err(error("version_document_not_regular", &path));
        }
        if entry.size > MAX_DOCUMENT_BYTES as u64
            || store.blob_size(&entry.hash) != Some(entry.size)
        {
            return Err(error("invalid_document_size", &path));
        }
        let upload = store
            .open_blob_verified(&entry.hash)
            .map_err(|_| error("invalid_document_blob", &path))?;
        let mut bytes = vec![];
        upload
            .file
            .take(MAX_DOCUMENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| error("unreadable_document_blob", &path))?;
        if bytes.len() as u64 != entry.size || format!("{:x}", Sha256::digest(&bytes)) != entry.hash
        {
            return Err(error("invalid_document_blob", &path));
        }
        let doc = Document::new(&path, &bytes)?;
        if workflows.contains(&path) {
            for value in declarations::workflow_version_files(&doc)? {
                paths.insert(value);
            }
        }
        documents.insert(path, doc);
    }
    declarations::read_project(project, &documents, workflows)
}
