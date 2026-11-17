//! Removing collected results that are past their retention (spec chapter
//! 13: inputs, logs and results are kept for 7 days after the end by
//! default; what runs, waits for collection or is referenced is never
//! removed).
//!
//! Which references go is decided by the parent database
//! (the guarded bounded claim transaction); this deletes the files of the result store
//! whose last reference went, and only then forgets their locators.

use super::collect::CollectError;
use super::plan::retention_ms;
use super::store::ResultStore;
use crate::storage::CoordinatorStore;

/// In-memory bounded keyset position. Restart safely repeats from the beginning.
#[derive(Debug, Default)]
pub struct Cursor {
    run_after: Option<crate::ids::RunId>,
    current: Option<(crate::ids::RunId, String)>,
}
#[derive(Debug, Default)]
pub struct PageReport {
    pub examined: u32,
    pub removed: u32,
    pub deleted: u32,
    pub foreign: u32,
    /// Claims whose entry could not be removed (an I/O error on that entry):
    /// they stay pending with their data and are tried again by a later page.
    pub failed: u32,
    pub skipped: bool,
    pub clock_held: bool,
}

/// At most one run/ref page and one deletion page. The caller must retain the
/// process owner through this synchronous call (including aborted async waits).
pub fn page(
    store: &CoordinatorStore,
    files: &ResultStore,
    cursor: &mut Cursor,
    retention_days: u32,
) -> Result<PageReport, CollectError> {
    let (run, after) = if let Some(current) = cursor.current.take() {
        current
    } else {
        let Some(run) = store.result_gc_next_run(cursor.run_after.clone())? else {
            cursor.run_after = None;
            return Ok(PageReport::default());
        };
        (run, String::new())
    };
    cursor.run_after = Some(run.clone());
    let guard = match store.result_gc_guard(&run) {
        Ok(guard) => guard,
        Err(crate::storage::StorageError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
            return Ok(PageReport {
                skipped: true,
                ..Default::default()
            })
        }
        Err(e) => return Err(e.into()),
    };
    let claim = store.claim_result_page(
        &guard,
        &run,
        &after,
        32,
        crate::platform::clock::wall_ms(),
        crate::platform::clock::monotonic_ms(),
        crate::platform::boot_id(),
        retention_ms(retention_days),
    )?;
    let mut report = PageReport {
        examined: claim.examined,
        removed: claim.removed,
        foreign: claim.foreign,
        clock_held: claim.clock_held,
        ..Default::default()
    };
    // Durable prior claims are replayed even while the clock is quarantined.
    // Only a known claim makes a missing file an idempotent success.
    for pending in store.pending_result_page(&guard, &run, 32)? {
        if !ResultStore::owns(&pending.locator) {
            return Err(crate::storage::StorageError::Invalid(
                "foreign result deletion claim".into(),
            )
            .into());
        }
        match files.remove(&pending.locator) {
            Ok(()) => {}
            // One entry that cannot be removed (permissions, a replaced
            // ancestor) must not hold back the claims behind it. It stays
            // pending and is never recorded as done.
            Err(super::store::StoreError::Io(_)) => {
                report.failed += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        }
        store.finish_result_delete(&guard, &pending)?;
        report.deleted += 1;
    }
    if let Some(next) = claim.next {
        cursor.current = Some((run, next));
    }
    Ok(report)
}

/// Cancellation of an async waiter never drops the process owner while its
/// blocking deletion page still owns a run guard or mutates the result store.
pub(crate) async fn owned_blocking<T, R, F>(
    owner: std::sync::Arc<T>,
    work: F,
) -> Result<R, tokio::task::JoinError>
where
    T: Send + Sync + 'static,
    R: Send + 'static,
    F: FnOnce(&T) -> R + Send + 'static,
{
    tokio::task::spawn_blocking(move || work(&owner)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_gc_wait_keeps_owner_and_exclusive_pin_until_drain() {
        let root = crate::test_tmp::tempdir().unwrap();
        let store = Arc::new(CoordinatorStore::open(root.path(), 1).unwrap());
        let run = crate::ids::RunId::generate();
        store
            .create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        let (entered, receive_entered) = mpsc::channel();
        let (resume, receive_resume) = mpsc::channel();
        let (drained, receive_drained) = mpsc::channel();
        let id = run.clone();
        let task = tokio::spawn(owned_blocking(store, move |owner| {
            let guard = owner.result_gc_guard(&id).unwrap();
            entered.send(()).unwrap();
            receive_resume
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            drop(guard);
            drained.send(()).unwrap();
        }));
        tokio::task::spawn_blocking(move || receive_entered.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let owner_held = matches!(
            CoordinatorStore::open(root.path(), 2),
            Err(crate::storage::StorageError::Locked(_))
        );
        let pin_held = crate::results::pin::RunPin::shared(root.path(), &run).is_err();
        resume.send(()).unwrap();
        tokio::task::spawn_blocking(move || receive_drained.recv_timeout(Duration::from_secs(2)))
            .await
            .unwrap()
            .unwrap();
        assert!(owner_held);
        assert!(pin_held);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if CoordinatorStore::open(root.path(), 2).is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("GC owner did not drain");
    }
}
