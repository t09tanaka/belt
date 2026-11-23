//! The owner of a worker's import token (spec 25.8, stage 5a).
//!
//! A worker takes one token per publication and records its id in the
//! receiving directory (`incoming/<source>/`). When the publication is done
//! or the directory is thrown away, the token is released
//! ([`SourceStore::release_import`]); tokens no directory records are
//! released when the agent starts ([`SourceStore::forget_unowned_imports`]).
//!
//! Releasing is two steps. The token's root becomes `abandoned` (it no
//! longer counts as a root of unknown membership, and still protects its
//! objects), under the shared mutation lock. Then, under the exclusive lock,
//! its memberships go through `release_member`: an object another root
//! holds only loses this membership; a file only this token held (what a
//! failed receipt installed) is claimed and unlinked, but only when the
//! claim can drain in the same section. What waits is finished by a later
//! pass ([`SourceStore::release_abandoned_imports`], every agent tick).
//!
//! Only tokens of the worker's shape are touched: `state='pending'`, no
//! source, no run. A worker never publishes with a token
//! (`publish_received_one`), so its tokens never leave `pending`; any other
//! row is kept.
use super::{ImportReleaseReport, ImportToken, Index};
use crate::source::SourceStore;
use anyhow::{ensure, Result};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::collections::BTreeSet;
use std::io;
use std::os::unix::fs::MetadataExt;

/// Rows one transaction changes (spec 25.8, 4; 25.11).
const PAGE: usize = 1000;

/// What a startup sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ForgetReport {
    /// Unrecorded tokens that were abandoned.
    pub abandoned: usize,
    /// Rows of another shape (published, bound, with a source or a run):
    /// kept, for `doctor`.
    pub kept: usize,
    /// What the release pass after it did.
    pub release: ImportReleaseReport,
}

/// Is `id` the id of a token ([`SourceStore::begin_import`] makes ULIDs)?
pub fn is_import_token_id(id: &str) -> bool {
    id.len() == 26 && ulid::Ulid::from_string(id).is_ok()
}

const WORKER_SHAPE: &str = "state='pending' AND source IS NULL AND run_id IS NULL";

impl Index {
    /// The token's root stops being a pending one: `abandoned`. `false`
    /// when the token is not of the worker's shape (it is kept).
    fn abandon_import(&self, c: &rusqlite::Transaction<'_>, id: &str) -> Result<bool> {
        let ours: bool = c.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM import_session WHERE token=?1 AND {WORKER_SHAPE})"
            ),
            [id],
            |r| r.get(0),
        )?;
        if ours {
            c.execute(
                "UPDATE root SET kind='abandoned' WHERE id=?1 AND kind='receipt'",
                [format!("import:{id}")],
            )?;
            // a session whose root is gone already goes with it
            c.execute(
                "DELETE FROM import_session WHERE token=?1 AND NOT EXISTS(SELECT 1 FROM root WHERE id=?2)",
                params![id, format!("import:{id}")],
            )?;
        }
        Ok(ours)
    }
}

fn would_block(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|e| e.kind() == io::ErrorKind::WouldBlock)
    })
}

impl SourceStore {
    /// The token `id` again (after a restart), while it is a pending one of
    /// the worker's shape. `None` when it is gone or released.
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
                &format!("SELECT 1 FROM import_session WHERE token=?1 AND {WORKER_SHAPE} AND EXISTS(SELECT 1 FROM root WHERE id='import:'||?1 AND kind='receipt')"),
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

    /// The owner of the token `id` ended. Its root is abandoned at once
    /// (the owner's record may go after this returns); its memberships are
    /// released now when the store is free, else by a later pass.
    pub fn release_import(&self, id: &str) -> Result<ImportReleaseReport> {
        ensure!(is_import_token_id(id), "not an import token id");
        {
            let _guard = self.mutation_guard()?;
            let mut c = self.retention.connect_for_import()?;
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            self.retention.abandon_import(&tx, id)?;
            tx.commit()?;
        }
        self.release_abandoned_imports()
    }

    /// One bounded pass over the abandoned tokens, under the exclusive
    /// mutation lock. A store that is busy is left for the next pass.
    pub fn release_abandoned_imports(&self) -> Result<ImportReleaseReport> {
        let _guard = match self.retention.guard(true) {
            Ok(g) => g,
            Err(e) if would_block(&e) => {
                return Ok(ImportReleaseReport {
                    stopped: true,
                    ..Default::default()
                })
            }
            Err(e) => return Err(e),
        };
        self.retention.release_abandoned_imports()
    }

    /// At the worker's start, under `agent.lock` and before anything is
    /// received: abandon the tokens no receiving directory records (`owned`
    /// holds the ids that are recorded), at most [`PAGE`] rows per
    /// transaction, then release what the store lets go of.
    pub fn forget_unowned_imports(&self, owned: &BTreeSet<String>) -> Result<ForgetReport> {
        let mut report = ForgetReport::default();
        {
            let _guard = self.mutation_guard()?;
            let mut after = String::new();
            loop {
                let mut c = self.retention.connect_for_import()?;
                let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let tokens: Vec<String> = {
                    let mut s = tx.prepare(
                        "SELECT token FROM import_session WHERE token>?1 ORDER BY token LIMIT ?2",
                    )?;
                    let rows = s.query_map(params![after, (PAGE / 4) as i64], |r| r.get(0))?;
                    rows.collect::<std::result::Result<_, _>>()?
                };
                let Some(last) = tokens.last() else {
                    break;
                };
                after = last.clone();
                for token in tokens.iter().filter(|t| !owned.contains(*t)) {
                    if self.retention.abandon_import(&tx, token)? {
                        report.abandoned += 1;
                    } else {
                        report.kept += 1;
                    }
                }
                tx.commit()?;
            }
        }
        // (until a pass makes no progress: the old intake left a token for
        // every blob it took)
        loop {
            let pass = self.release_abandoned_imports()?;
            report.release.released += pass.released;
            report.release.unlinked += pass.unlinked;
            report.release.kept = pass.kept;
            report.release.stopped = pass.stopped;
            if pass.released == 0 && pass.unlinked == 0 {
                return Ok(report);
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
    fn claims(store: &SourceStore) -> i64 {
        count(
            store,
            "SELECT count(*) FROM source_delete_claim WHERE done=0",
        ) + count(
            store,
            "SELECT count(*) FROM publication WHERE state='claimed'",
        )
    }
    /// A store whose legacy manifests are all indexed (what the parent's
    /// registration pass reaches; a worker's store never runs it).
    fn store(dir: &std::path::Path) -> SourceStore {
        let store = SourceStore::open(dir).unwrap();
        store
            .retention
            .connect()
            .unwrap()
            .execute(
                "UPDATE meta SET value='done' WHERE key='legacy_registration'",
                [],
            )
            .unwrap();
        store
    }
    fn ingest(store: &SourceStore, token: &ImportToken, b: &[u8]) {
        store
            .ingest_blob_with_token(token, &want(b), &mut &b[..])
            .unwrap();
    }
    /// `b` held by a published source's root as well.
    fn publish(store: &SourceStore, b: &[u8]) {
        let c = store.retention.connect().unwrap();
        c.execute(
            "INSERT OR IGNORE INTO root(id,kind,source) VALUES('receive:x','received','x')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO member VALUES('receive:x',?1)",
            [format!("b:{}", want(b).hash)],
        )
        .unwrap();
    }

    #[test]
    fn a_released_token_unlinks_only_what_no_other_root_holds() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = store(dir.path());
        let token = store.begin_import().unwrap();
        let (lone, shared) = ([1u8; 9], [2u8; 9]);
        ingest(&store, &token, &lone);
        ingest(&store, &token, &shared);
        publish(&store, &shared);
        assert_eq!(rows(&store), (1, 1, 2));
        let id = SourceStore::import_token_id(&token).to_string();
        assert!(store.resume_import(&id).unwrap().is_some());
        let r = store.release_import(&id).unwrap();
        assert_eq!((r.released, r.unlinked, r.kept), (1, 1, 0));
        assert_eq!(rows(&store), (0, 0, 0));
        assert_eq!(claims(&store), 0);
        assert!(!store.has_blob(&want(&lone).hash));
        assert!(store.has_blob(&want(&shared).hash));
        assert!(store.resume_import(&id).unwrap().is_none());
        // again: nothing to do
        store.release_import(&id).unwrap();
        store.begin_import().unwrap();
    }

    #[test]
    fn a_release_while_another_receipt_is_in_flight_leaves_no_claim() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = store(dir.path());
        let token = store.begin_import().unwrap();
        let other = store.begin_import().unwrap();
        let (lone, shared, theirs) = ([3u8; 9], [4u8; 9], [5u8; 9]);
        ingest(&store, &token, &lone);
        ingest(&store, &token, &shared);
        publish(&store, &shared);
        ingest(&store, &other, &theirs);
        let id = SourceStore::import_token_id(&token).to_string();
        let r = store.release_import(&id).unwrap();
        // a receipt in flight has an exact membership: it holds back
        // nothing but its own files
        assert_eq!((r.released, r.unlinked, r.kept), (1, 1, 0));
        assert_eq!(claims(&store), 0);
        assert!(!store.has_blob(&want(&lone).hash));
        assert!(store.has_blob(&want(&theirs).hash));
        // receipts go on
        let next = store.begin_import().unwrap();
        ingest(&store, &next, &[6u8; 9]);
        ingest(&store, &other, &[7u8; 9]);
        for t in [&other, &next] {
            store
                .release_import(SourceStore::import_token_id(t))
                .unwrap();
        }
        assert_eq!(rows(&store), (0, 0, 0));
        assert_eq!(claims(&store), 0);
        assert!(!store.has_blob(&want(&theirs).hash));
        assert!(store.has_blob(&want(&shared).hash));
    }

    #[test]
    fn a_capture_root_of_unknown_membership_still_holds_every_claim() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = store(dir.path());
        let token = store.begin_import().unwrap();
        let lone = [10u8; 9];
        ingest(&store, &token, &lone);
        // a parent's capture in progress: a pending root with no source
        store.capture_pending_root(None).unwrap();
        let id = SourceStore::import_token_id(&token).to_string();
        let r = store.release_import(&id).unwrap();
        assert_eq!((r.released, r.unlinked, r.kept), (0, 0, 1));
        assert_eq!(claims(&store), 0);
        assert!(store.has_blob(&want(&lone).hash));
        store.begin_import().unwrap();
    }

    #[test]
    fn without_indexed_legacy_manifests_no_file_is_unlinked() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = SourceStore::open(dir.path()).unwrap();
        let token = store.begin_import().unwrap();
        ingest(&store, &token, &[8u8; 9]);
        let id = SourceStore::import_token_id(&token).to_string();
        let r = store.release_import(&id).unwrap();
        assert_eq!((r.unlinked, r.kept), (0, 1));
        assert_eq!(claims(&store), 0);
        assert!(store.has_blob(&want(&[8u8; 9]).hash));
    }

    #[test]
    fn a_member_whose_file_never_came_goes_without_a_claim() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = store(dir.path());
        let token = store.begin_import().unwrap();
        let b = [9u8; 9];
        store.retention.import_blob(&token, &want(&b).hash).unwrap();
        let id = SourceStore::import_token_id(&token).to_string();
        let r = store.release_import(&id).unwrap();
        assert_eq!((r.released, r.unlinked), (1, 0));
        assert_eq!(rows(&store), (0, 0, 0));
        assert_eq!(claims(&store), 0);
    }

    #[test]
    fn startup_forgets_only_unrecorded_tokens_of_the_worker_shape() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = store(dir.path());
        let b = [7u8; 5];
        let kept = store.begin_import().unwrap();
        ingest(&store, &kept, &b);
        publish(&store, &b);
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
        assert_eq!((report.abandoned, report.kept), (3, 1));
        assert_eq!(report.release.released, 3);
        assert_eq!(rows(&store), (2, 2, 1));
        assert!(store
            .resume_import(SourceStore::import_token_id(&kept))
            .unwrap()
            .is_some());
        assert!(store.has_blob(&want(&b).hash));
    }

    #[test]
    fn only_a_ulid_names_a_token() {
        assert!(is_import_token_id(&ulid::Ulid::new().to_string()));
        assert!(!is_import_token_id("../x"));
        assert!(!is_import_token_id(""));
    }
}
