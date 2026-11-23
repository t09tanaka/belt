//! The coordinator's path after an attempt ended: collecting its outputs.
//!
//! [`Coordinator::collect`] is a pass of its own, beside [`Coordinator::tick`]:
//! fetching a large artifact must not keep the tick from renewing leases.
//! One pass runs at a time; the workers are worked on side by side, each
//! within the budget of the collector.

use super::link::Budgeted;
use super::{join_all, Coordinator, CoordinatorError, Result, WorkerLink};
use crate::ids::WorkerId;
use crate::results::{CollectConfig, CollectError, CollectReport, Collector, ResultStore};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

type Collected = std::result::Result<CollectReport, CollectError>;

impl Coordinator {
    /// One pass of result collection: continue the collection of every
    /// ended attempt, on every worker, from what is durable. A worker that
    /// is not attached or does not answer is not an error; its attempts
    /// stay `collecting` and nothing is run again. `files` is the one
    /// result store of this process, the same for every pass (see
    /// [`ResultStore::open`]).
    pub async fn collect(
        &self,
        files: &Arc<ResultStore>,
        cfg: CollectConfig,
    ) -> Result<CollectReport> {
        self.collect_stopping(files, cfg, None).await
    }

    pub(crate) async fn collect_stopping(
        &self,
        files: &Arc<ResultStore>,
        cfg: CollectConfig,
        stop: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<CollectReport> {
        let one = Arc::new(Arc::clone(&self.collecting).lock_owned().await);
        self.check_fenced()?;
        if self.in_trial() {
            // Nothing is acknowledged or adopted until the commit.
            return Ok(CollectReport::default());
        }
        let collector = Collector::new(Arc::clone(&self.store), Arc::clone(files), cfg)
            .with_costs(self.sched.model(), self.cost_tails.clone())
            .with_waits(Arc::clone(&self.collect_waits))
            .with_live_waits(Arc::clone(&self.live_log_waits))
            .with_stop(stop)
            .with_collecting(one)
            .with_submits(self.submits_in_flight());
        // Spec 25.6 (a): the workers attached since the last pass list what
        // they hold of this parent that nobody releases any more.
        self.reconcile_unreleased(
            tokio::time::Instant::now() + Duration::from_millis(cfg.budget_ms / 4),
        )
        .await?;
        let mut attached: Vec<(WorkerId, Arc<dyn WorkerLink>)> = {
            let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
            // Use every attached worker before rotating/truncating the page.
            // Otherwise temporarily unselected peers lose their fair cursors.
            self.live_log_cursors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|worker, _| workers.contains_key(worker));
            workers
                .iter()
                .map(|(w, l)| (w.clone(), Arc::clone(l)))
                .collect()
        };
        // Bound concurrent peers and rotate the starting worker, so a large
        // attachment set cannot create unbounded RPCs or favor the first IDs.
        {
            let mut cursor = self
                .live_worker_cursor
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(after) = cursor.as_ref() {
                let start = attached.partition_point(|(w, _)| w <= after);
                let len = attached.len();
                if len > 0 {
                    attached.rotate_left(start % len);
                }
            }
            attached.truncate(8);
            *cursor = attached.last().map(|(w, _)| w.clone());
        }
        let scan = cfg.scan;
        let open = self.db(move |s| s.collectable(None, scan)).await?;
        let mut others: BTreeSet<WorkerId> = open.into_iter().map(|c| c.record.worker_id).collect();
        let deadline = Instant::now() + Duration::from_millis(cfg.budget_ms);
        let live_deadline = Instant::now() + Duration::from_millis(cfg.budget_ms / 2);
        let mut passes: Vec<Pin<Box<dyn Future<Output = Collected> + Send + '_>>> = Vec::new();
        let collector = &collector;
        for (worker, link) in attached {
            others.remove(&worker);
            let after = self
                .live_log_cursors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&worker)
                .cloned();
            passes.push(Box::pin(async move {
                let live_link = Budgeted {
                    inner: Arc::clone(&link),
                    deadline: live_deadline,
                };
                let (cursor, mut live) = collector
                    .collect_live_worker(&worker, &live_link, after, live_deadline)
                    .await?;
                {
                    let mut cursors = self
                        .live_log_cursors
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    match cursor {
                        Some(a) => {
                            cursors.insert(worker.clone(), a);
                        }
                        None => {
                            cursors.remove(&worker);
                        }
                    }
                }
                let link = Budgeted {
                    inner: link,
                    deadline,
                };
                live.merge(
                    collector
                        .collect_worker(&worker, Some(&link), deadline)
                        .await?,
                );
                Ok(live)
            }));
        }
        // What no worker is needed for is closed also without a link.
        for worker in others {
            passes.push(Box::pin(async move {
                collector.collect_worker(&worker, None, deadline).await
            }));
        }
        let mut report = CollectReport::default();
        let mut failed = None;
        for pass in join_all(passes).await {
            match pass {
                Ok(part) => report.merge(part),
                Err(e) => failed = failed.or(Some(e)),
            }
        }
        match failed {
            None => Ok(report),
            Some(CollectError::Fenced) => Err(self.fence()),
            Some(CollectError::Storage(e)) => Err(e.into()),
            Some(CollectError::Aborted) => Err(CoordinatorError::Aborted),
            Some(CollectError::Files(e)) => Err(CoordinatorError::Results(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{CoordinatorConfig, LinkError};
    use crate::ids::AttemptId;
    use crate::storage::CoordinatorStore;
    use crate::wire::{Request, Response};

    struct Disconnected;
    impl WorkerLink for Disconnected {
        fn call(
            &self,
            _: Request,
        ) -> Pin<Box<dyn Future<Output = std::result::Result<Response, LinkError>> + Send + '_>>
        {
            Box::pin(async { Err(LinkError::Unconfirmed) })
        }
    }

    #[tokio::test]
    async fn detached_worker_cursors_are_pruned_before_the_attached_page_is_truncated() {
        let root = crate::test_tmp::tempdir().unwrap();
        let store = CoordinatorStore::open(root.path(), 1).unwrap();
        let coordinator = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
        let files = Arc::new(ResultStore::open(&root.path().join("results")).unwrap());
        let gone = WorkerId::parse("gone").unwrap();
        coordinator
            .live_log_cursors
            .lock()
            .unwrap()
            .insert(gone.clone(), AttemptId::generate());
        for n in 0..10 {
            let worker = WorkerId::parse(&format!("worker-{n:02}")).unwrap();
            coordinator
                .try_attach(worker.clone(), Arc::new(Disconnected))
                .unwrap();
            coordinator
                .live_log_cursors
                .lock()
                .unwrap()
                .insert(worker, AttemptId::generate());
        }
        coordinator
            .collect(&files, CollectConfig::default())
            .await
            .unwrap();
        let cursors = coordinator.live_log_cursors.lock().unwrap();
        assert!(!cursors.contains_key(&gone));
        // The first eight empty workers clear their cursors during polling;
        // attached workers outside that page must retain theirs for next time.
        assert_eq!(cursors.len(), 2);
        assert!(cursors.contains_key(&WorkerId::parse("worker-08").unwrap()));
        assert!(cursors.contains_key(&WorkerId::parse("worker-09").unwrap()));
    }
}
