//! The owner of a worker's import token (spec 25.8, stage 5a).
//!
//! A worker takes one token per publication and records its id in the
//! receiving directory (`incoming/<source>/`). The token's rows go when the
//! publication is done or the directory is thrown away
//! ([`SourceStore::release_import`]); rows no directory records are removed
//! when the agent starts ([`SourceStore::forget_unowned_imports`]).
//!
//! Only tokens of the worker's shape are touched: `state='pending'`, no
//! source, no run. A worker never publishes with a token
//! (`publish_received_one`), so its tokens never leave `pending`; any other
//! row is kept.
use super::{ImportToken, Index};
use crate::source::SourceStore;
use anyhow::{ensure, Result};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::collections::BTreeSet;
use std::os::unix::fs::MetadataExt;

/// Rows one transaction deletes (spec 25.8, 4; 25.11).
const PAGE: usize = 1000;

/// What a startup sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ForgetReport {
    /// Tokens whose rows were removed.
    pub removed: usize,
    /// Rows of another shape (published, bound, with a source or a run):
    /// kept, for `doctor`.
    pub kept: usize,
}

/// Is `id` the id of a token ([`SourceStore::begin_import`] makes ULIDs)?
pub fn is_import_token_id(id: &str) -> bool {
    id.len() == 26 && ulid::Ulid::from_string(id).is_ok()
}

const WORKER_SHAPE: &str = "state='pending' AND source IS NULL AND run_id IS NULL";

impl Index {
    /// Remove the token `id`'s members (a page per transaction), then its
    /// root and its row together. `false` when the row is not of the
    /// worker's shape (it is kept).
    fn forget_import(&self, id: &str) -> Result<bool> {
        let root = format!("import:{id}");
        loop {
            let mut c = self.connect_for_import()?;
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let ours: bool = tx.query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM import_session WHERE token=?1 AND {WORKER_SHAPE})"
                ),
                [id],
                |r| r.get(0),
            )?;
            if !ours {
                return Ok(false);
            }
            let n = tx.execute(
                "DELETE FROM member WHERE rowid IN (SELECT rowid FROM member WHERE root=?1 LIMIT ?2)",
                params![root, PAGE as i64],
            )?;
            if n == PAGE {
                tx.commit()?;
                continue;
            }
            tx.execute("DELETE FROM graph WHERE root=?1", [&root])?;
            tx.execute("DELETE FROM root WHERE id=?1 AND kind='pending'", [&root])?;
            tx.execute("DELETE FROM import_session WHERE token=?1", [id])?;
            tx.commit()?;
            return Ok(true);
        }
    }
}

impl SourceStore {
    /// The token `id` again (after a restart), while its row is still a
    /// pending one of the worker's shape. `None` when it is gone.
    pub fn resume_import(&self, id: &str) -> Result<Option<ImportToken>> {
        if !is_import_token_id(id) {
            return Ok(None);
        }
        let _guard = self.mutation_guard()?;
        let metadata = self.retention.root.metadata()?;
        let pending = self
            .retention
            .connect()?
            .query_row(
                &format!("SELECT 1 FROM import_session WHERE token=?1 AND {WORKER_SHAPE}"),
                [id],
                |_| Ok(()),
            )
            .optional()?;
        Ok(pending.map(|()| ImportToken {
            id: id.to_string(),
            store: (metadata.dev(), metadata.ino()),
        }))
    }

    /// The token's id, for its owner to record.
    pub fn import_token_id(token: &ImportToken) -> &str {
        &token.id
    }

    /// The owner of the token `id` ended: its row, its root (`import:<id>`)
    /// and the root's members go. Nothing else: the objects stay, and a
    /// published source keeps its `receive:<source>` root. A token that is
    /// gone already is fine.
    pub fn release_import(&self, id: &str) -> Result<()> {
        ensure!(is_import_token_id(id), "not an import token id");
        let _guard = self.mutation_guard()?;
        self.retention.forget_import(id)?;
        Ok(())
    }

    /// At the worker's start, under `agent.lock` and before anything is
    /// received: remove the tokens no receiving directory records (`owned`
    /// holds the ids that are recorded). At most [`PAGE`] rows per
    /// transaction.
    pub fn forget_unowned_imports(&self, owned: &BTreeSet<String>) -> Result<ForgetReport> {
        let _guard = self.mutation_guard()?;
        let mut report = ForgetReport::default();
        let mut after = String::new();
        loop {
            let tokens: Vec<(String, bool)> = {
                let c = self.retention.connect()?;
                let mut s = c.prepare(&format!(
                    "SELECT token,({WORKER_SHAPE}) FROM import_session WHERE token>?1 ORDER BY token LIMIT ?2"
                ))?;
                let rows =
                    s.query_map(params![after, PAGE as i64], |r| Ok((r.get(0)?, r.get(1)?)))?;
                rows.collect::<std::result::Result<_, _>>()?
            };
            let Some((last, _)) = tokens.last() else {
                return Ok(report);
            };
            after = last.clone();
            for (token, ours) in &tokens {
                if owned.contains(token) {
                    continue;
                }
                if *ours && self.retention.forget_import(token)? {
                    report.removed += 1;
                } else {
                    report.kept += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::diff::BlobWant;
    use sha2::Digest as _;

    fn want(bytes: &[u8]) -> BlobWant {
        BlobWant {
            hash: format!("{:x}", sha2::Sha256::digest(bytes)),
            size: bytes.len() as u64,
        }
    }
    fn count(store: &SourceStore, sql: &str) -> i64 {
        store
            .retention
            .connect()
            .unwrap()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    }
    fn rows(store: &SourceStore) -> (i64, i64, i64) {
        (
            count(store, "SELECT count(*) FROM import_session"),
            count(store, "SELECT count(*) FROM root WHERE id LIKE 'import:%'"),
            count(
                store,
                "SELECT count(*) FROM member WHERE root LIKE 'import:%'",
            ),
        )
    }

    #[test]
    fn a_released_token_leaves_no_row_and_keeps_the_object() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let token = store.begin_import().unwrap();
        for i in 0..3u8 {
            let b = [i; 9];
            store
                .ingest_blob_with_token(&token, &want(&b), &mut &b[..])
                .unwrap();
        }
        assert_eq!(rows(&store), (1, 1, 3));
        let id = SourceStore::import_token_id(&token).to_string();
        assert!(store.resume_import(&id).unwrap().is_some());
        store.release_import(&id).unwrap();
        assert_eq!(rows(&store), (0, 0, 0));
        assert!(store.has_blob(&want(&[0u8; 9]).hash));
        assert!(store.resume_import(&id).unwrap().is_none());
        // again: nothing to do
        store.release_import(&id).unwrap();
    }

    #[test]
    fn a_release_spans_pages_of_members() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let token = store.begin_import().unwrap();
        let id = SourceStore::import_token_id(&token).to_string();
        {
            let mut c = store.retention.connect().unwrap();
            let tx = c.transaction().unwrap();
            for i in 0..(PAGE * 2 + 5) {
                tx.execute(
                    "INSERT INTO member VALUES(?1,?2)",
                    params![format!("import:{id}"), format!("b:{i:064}")],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        store.release_import(&id).unwrap();
        assert_eq!(rows(&store), (0, 0, 0));
    }

    #[test]
    fn startup_forgets_only_unrecorded_tokens_of_the_worker_shape() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let b = [7u8; 5];
        let kept = store.begin_import().unwrap();
        store
            .ingest_blob_with_token(&kept, &want(&b), &mut &b[..])
            .unwrap();
        // the blob-per-token rows of the old intake
        for _ in 0..3 {
            store.ingest_blob(&want(&b), &mut &b[..]).unwrap();
        }
        // a row of another shape (a parent's publication)
        let other = store.begin_import().unwrap();
        let other_id = SourceStore::import_token_id(&other).to_string();
        store
            .retention
            .connect()
            .unwrap()
            .execute(
                "UPDATE import_session SET state='published',source='x' WHERE token=?1",
                [&other_id],
            )
            .unwrap();
        let owned = BTreeSet::from([SourceStore::import_token_id(&kept).to_string()]);
        let report = store.forget_unowned_imports(&owned).unwrap();
        assert_eq!(
            report,
            ForgetReport {
                removed: 3,
                kept: 1
            }
        );
        assert_eq!(rows(&store), (2, 2, 1));
        assert!(store
            .resume_import(SourceStore::import_token_id(&kept))
            .unwrap()
            .is_some());
        assert!(store.resume_import(&other_id).unwrap().is_none());
        assert!(store.has_blob(&want(&b).hash));
    }

    #[test]
    fn only_a_ulid_names_a_token() {
        assert!(is_import_token_id(&ulid::Ulid::new().to_string()));
        assert!(!is_import_token_id("../x"));
        assert!(!is_import_token_id(""));
    }
}
