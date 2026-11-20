//! `workers sync`: one look at a warm-up on one worker, over the link the
//! coordinator already holds. Outside the placement actor, the admission lock
//! and SQLite: only a bounded, authenticated call.
use super::*;
use crate::wire::{EnvSyncRequest, Request, Response, SchemaVersion, ToolRequirement};

impl Coordinator {
    fn environment_link(
        &self,
        worker: &WorkerId,
    ) -> std::result::Result<std::sync::Arc<dyn WorkerLink>, ApiError> {
        self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(worker)
            .cloned()
            .ok_or_else(|| {
                ApiError::new(
                    ErrorCode::NotFound,
                    format!(
                        "worker {worker} is not connected to the resident: it is not registered (workers add), or the resident has not reached it yet"
                    ),
                )
            })
    }

    pub async fn env_report(
        &self,
        worker: &WorkerId,
    ) -> std::result::Result<crate::toolchain::envdiff::EnvReport, ApiError> {
        let link = self.environment_link(worker)?;
        let request = Request::EnvReport(crate::wire::EnvReportRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            required: Vec::new(),
        });
        let incompatible = || {
            ApiError::new(
                ErrorCode::ProtocolIncompatible,
                format!("worker {worker} did not supply a compatible environment report"),
            )
        };
        match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            link.read_only_call(request),
        )
        .await
        {
            Ok(Ok(Response::EnvReport(answer))) => answer
                .environment
                .filter(|r| r.schema_version == crate::toolchain::envdiff::SCHEMA)
                .ok_or_else(incompatible),
            Ok(Ok(_)) => Err(incompatible()),
            Ok(Err(LinkError::Remote(error))) => Err(error),
            Ok(Err(LinkError::Unconfirmed)) | Err(_) => Err(ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!("worker {worker} did not answer the environment request"),
            )),
        }
    }

    /// Ask `worker` about the warm-up `sync_id` (the first ask starts it).
    /// A worker whose hello did not agree on `env_sync_v1` is never sent the
    /// request. No answer in time is `COMMUNICATION_UNKNOWN`: the warm-up may
    /// go on, and the same `sync_id` may be asked about again.
    pub async fn env_sync(
        &self,
        worker: &WorkerId,
        sync_id: RequestId,
        tools: Vec<ToolRequirement>,
    ) -> std::result::Result<Vec<crate::wire::ToolSyncResult>, ApiError> {
        let link = self.environment_link(worker)?;
        if !link.env_sync_negotiated() {
            return Err(ApiError::new(
                ErrorCode::ProtocolIncompatible,
                format!(
                    "worker {worker} runs a belt that cannot install ahead of a task; run belt workers upgrade {worker}, then workers sync again"
                ),
            ));
        }
        let wanted = tools.len();
        let request = Request::EnvSync(EnvSyncRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            sync_id,
            coordinator_epoch: self.epoch(),
            tools,
            dry_run: false,
        });
        // Under the reply timeout of the control socket that asks.
        let budget = std::time::Duration::from_millis(self.cfg.worker_budget_ms)
            .min(std::time::Duration::from_secs(4));
        match tokio::time::timeout(budget, link.read_only_call(request)).await {
            Ok(Ok(Response::EnvSync(response))) if response.results.len() == wanted => {
                Ok(response.results)
            }
            Ok(Ok(_)) => Err(ApiError::new(
                ErrorCode::ProtocolIncompatible,
                format!("worker {worker} answered env_sync with something else"),
            )),
            Ok(Err(LinkError::Remote(error))) => Err(error),
            Ok(Err(LinkError::Unconfirmed)) | Err(_) => Err(ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!("worker {worker} did not answer in time"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{EnvSyncResponse, SyncOutcome, ToolSyncResult};
    use std::sync::atomic::AtomicUsize;

    /// Counts what it is sent; answers as told.
    struct Link {
        negotiated: bool,
        calls: AtomicUsize,
        answer: fn(EnvSyncRequest) -> std::result::Result<Response, LinkError>,
    }
    impl WorkerLink for Link {
        fn env_sync_negotiated(&self) -> bool {
            self.negotiated
        }
        fn call(&self, request: Request) -> link::LinkFuture<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let Request::EnvSync(r) = request else {
                panic!("only env_sync is sent")
            };
            let answer = (self.answer)(r);
            Box::pin(async move { answer })
        }
    }
    fn echo(r: EnvSyncRequest) -> std::result::Result<Response, LinkError> {
        assert!(!r.dry_run);
        Ok(Response::EnvSync(EnvSyncResponse {
            schema_version: SchemaVersion::CURRENT,
            results: r
                .tools
                .iter()
                .map(|t| ToolSyncResult {
                    name: t.name.clone(),
                    outcome: SyncOutcome::Pending,
                    message: Some(r.sync_id.to_string()),
                })
                .collect(),
        }))
    }
    fn tools() -> Vec<ToolRequirement> {
        vec![ToolRequirement {
            name: "node".into(),
            version: "20.0.0".into(),
        }]
    }
    async fn fx(
        negotiated: bool,
        answer: fn(EnvSyncRequest) -> std::result::Result<Response, LinkError>,
    ) -> (tempfile::TempDir, Coordinator, WorkerId, Arc<Link>) {
        let dir = crate::test_tmp::tempdir().unwrap();
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        let c = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
        let worker = WorkerId::parse("w-a").unwrap();
        let link = Arc::new(Link {
            negotiated,
            calls: AtomicUsize::new(0),
            answer,
        });
        c.attach(worker.clone(), link.clone()).await;
        (dir, c, worker, link)
    }

    #[tokio::test]
    async fn an_old_peer_is_never_sent_the_request_and_is_told_to_upgrade() {
        let (_dir, c, worker, link) = fx(false, echo).await;
        let error = c
            .env_sync(&worker, RequestId::generate(), tools())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::ProtocolIncompatible);
        assert!(
            error.message.contains("workers upgrade w-a"),
            "{}",
            error.message
        );
        assert_eq!(link.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn the_warm_up_goes_to_the_named_worker_with_its_sync_id() {
        let (_dir, c, worker, link) = fx(true, echo).await;
        let sync = RequestId::generate();
        let results = c.env_sync(&worker, sync.clone(), tools()).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "node");
        assert_eq!(results[0].message, Some(sync.to_string()));
        assert_eq!(link.calls.load(Ordering::SeqCst), 1);
        // A worker the resident does not hold is said, not guessed at.
        let other = WorkerId::parse("w-b").unwrap();
        let error = c
            .env_sync(&other, RequestId::generate(), tools())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::NotFound);
        assert!(error.message.contains("w-b is not connected"));
        assert_eq!(link.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_answer_a_refusal_and_a_short_answer_are_three_different_errors() {
        let (_dir, c, worker, _link) = fx(true, |_| Err(LinkError::Unconfirmed)).await;
        let error = c
            .env_sync(&worker, RequestId::generate(), tools())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::CommunicationUnknown);
        let (_dir, c, worker, _link) = fx(true, |_| {
            Err(LinkError::Remote(ApiError::new(
                ErrorCode::Overloaded,
                "busy",
            )))
        })
        .await;
        let error = c
            .env_sync(&worker, RequestId::generate(), tools())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Overloaded);
        let (_dir, c, worker, _link) = fx(true, |_| {
            Ok(Response::EnvSync(EnvSyncResponse {
                schema_version: SchemaVersion::CURRENT,
                results: vec![],
            }))
        })
        .await;
        let error = c
            .env_sync(&worker, RequestId::generate(), tools())
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::ProtocolIncompatible);
    }
}
