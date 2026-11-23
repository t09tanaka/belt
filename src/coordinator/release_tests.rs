//! The parent's side of stage 3 (spec 25.6 (iv), (xii), (xvi)) against a
//! scripted worker: what the reconciliation releases, what it keeps, that
//! nothing is sent to a peer without `attempt_release_v1`, and that no
//! release goes out while a submit of the attempt waits for its answer.

use super::link::LinkFuture;
use super::*;
use crate::wire::{ListUnreleasedResponse, ReleaseAttemptResponse, SchemaVersion};

/// A worker that lists `unreleased` and accepts every release.
struct Scripted {
    feature: bool,
    unreleased: Vec<AttemptId>,
    sent: Mutex<Vec<Request>>,
}

impl Scripted {
    fn new(feature: bool, unreleased: Vec<AttemptId>) -> Arc<Self> {
        Arc::new(Scripted {
            feature,
            unreleased,
            sent: Mutex::new(Vec::new()),
        })
    }

    fn released(&self) -> Vec<AttemptId> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| match r {
                Request::ReleaseAttempt(r) => Some(r.attempt_id.clone()),
                _ => None,
            })
            .collect()
    }

    fn listed(&self) -> usize {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|r| matches!(r, Request::ListUnreleased(_)))
            .count()
    }
}

impl WorkerLink for Scripted {
    fn call(&self, request: Request) -> LinkFuture<'_> {
        self.sent.lock().unwrap().push(request.clone());
        let answer = match request {
            Request::ListUnreleased(_) => Ok(Response::ListUnreleased(ListUnreleasedResponse {
                schema_version: SchemaVersion::CURRENT,
                attempts: self.unreleased.clone(),
                more: false,
            })),
            Request::ReleaseAttempt(r) => Ok(Response::ReleaseAttempt(ReleaseAttemptResponse {
                schema_version: SchemaVersion::CURRENT,
                attempt_id: r.attempt_id,
            })),
            _ => Err(LinkError::Unconfirmed),
        };
        Box::pin(async move { answer })
    }
    fn attempt_release_negotiated(&self) -> bool {
        self.feature
    }
}

struct Fx {
    _dir: tempfile::TempDir,
    c: Coordinator,
}

fn fx() -> Fx {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let c = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
    Fx { _dir: dir, c }
}

fn worker() -> WorkerId {
    WorkerId::parse("w-a").unwrap()
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

impl Fx {
    /// An attempt this parent holds a row of, its collection not closed.
    async fn placed(&self) -> AttemptId {
        let source = SourceId::generate();
        self.c
            .store()
            .put_source_ref(&source, &crate::ids::Digest::of_bytes(b"manifest"), 1)
            .unwrap();
        let task = TaskId::generate();
        self.c
            .enqueue(
                RunId::generate(),
                source,
                vec![(task.clone(), super::paging_tests::spec())],
            )
            .await
            .unwrap();
        let attempt = AttemptId::generate();
        let outcome = self
            .c
            .store()
            .place(
                &task,
                &worker(),
                &attempt,
                |_| true,
                |input| demand_of(input, 60_000),
                2,
            )
            .unwrap();
        assert!(matches!(outcome, PlaceOutcome::Placed(_)), "{outcome:?}");
        attempt
    }
}

// (iv) A worker absent at the pruning gets nothing then; its next connection
// lists the pruned attempt and the reconciliation releases it. An attempt
// with a row here and an open collection is not released.
#[tokio::test]
async fn a_pruned_attempt_is_released_at_the_next_connection_and_an_open_one_is_kept() {
    let f = fx();
    let open = f.placed().await;
    let pruned = AttemptId::generate();
    // Not attached: the release of the pruning has nowhere to go.
    f.c.release_on(&worker(), std::slice::from_ref(&pruned))
        .await;
    let link = Scripted::new(true, vec![open.clone(), pruned.clone()]);
    f.c.attach(worker(), link.clone()).await;
    f.c.reconcile_unreleased(soon()).await.unwrap();
    assert_eq!(link.listed(), 1);
    assert_eq!(link.released(), vec![pruned]);
    // Done: the next pass asks nothing until the worker is due again.
    f.c.reconcile_unreleased(soon()).await.unwrap();
    assert_eq!(link.listed(), 1);
}

// (xii) A peer without the feature is sent neither operation.
#[tokio::test]
async fn a_worker_without_the_feature_is_sent_no_release_and_no_listing() {
    let f = fx();
    let pruned = AttemptId::generate();
    let link = Scripted::new(false, vec![pruned.clone()]);
    f.c.attach(worker(), link.clone()).await;
    f.c.release_on(&worker(), std::slice::from_ref(&pruned))
        .await;
    f.c.reconcile_unreleased(soon()).await.unwrap();
    assert!(link.sent.lock().unwrap().is_empty());
}

// (xvi) While a submit of the attempt waits for its answer no release is
// sent; once it is answered the release goes out.
#[tokio::test]
async fn no_release_is_sent_while_a_submit_of_the_attempt_waits_for_its_answer() {
    let f = fx();
    let a = AttemptId::generate();
    let link = Scripted::new(true, vec![a.clone()]);
    f.c.attach(worker(), link.clone()).await;
    let submits = f.c.submits_in_flight();
    {
        let _sending = submits.sending(&a);
        f.c.release_on(&worker(), std::slice::from_ref(&a)).await;
        f.c.reconcile_unreleased(soon()).await.unwrap();
        assert!(link.released().is_empty());
    }
    f.c.release_on(&worker(), std::slice::from_ref(&a)).await;
    assert_eq!(link.released(), vec![a]);
}
