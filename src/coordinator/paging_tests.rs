//! The coordinator walks a worker's live attempts and its unacknowledged
//! placements a page at a time: with more of them than one page holds, every
//! one is still reached, once.

use super::link::LinkFuture;
use super::*;
use crate::ids::{Digest, NodeId};
use crate::storage::PAGE_MAX;
use std::sync::atomic::AtomicUsize;

/// More than one page, and not a multiple of it.
const MANY: usize = PAGE_MAX as usize + 6;

/// A worker that counts what it is sent and answers nothing.
#[derive(Default)]
struct Silent {
    calls: AtomicUsize,
}

impl WorkerLink for Silent {
    fn call(&self, _request: Request) -> LinkFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(LinkError::Unconfirmed) })
    }
}

fn spec() -> TaskSpec {
    TaskSpec {
        ci: None,
        toolchains: None,
        outputs: Vec::new(),
        requirements: Default::default(),
        in_place: false,
        local: false,
        setup: None,
        plan_digest: Digest::of_bytes(b"plan"),
        manifest_digest: Digest::of_bytes(b"manifest"),
        commands: vec![CommandSpec {
            setup_step: None,
            node_id: NodeId::parse("main").unwrap(),
            argv: vec!["/bin/true".into()],
            cwd: String::new(),
            env: BTreeMap::new(),
            depends_on: vec![],
            timeout_ms: None,
        }],
        command_timeout_ms: None,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: false,
        artifacts: vec![],
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

fn worker(name: &str) -> WorkerId {
    WorkerId::parse(name).unwrap()
}

impl Fx {
    async fn tasks(&self, count: usize) -> Vec<TaskId> {
        let source = SourceId::generate();
        self.c
            .store()
            .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
            .unwrap();
        let tasks: Vec<TaskId> = (0..count).map(|_| TaskId::generate()).collect();
        let specs = tasks.iter().map(|t| (t.clone(), spec())).collect();
        self.c
            .enqueue(RunId::generate(), source, specs)
            .await
            .unwrap();
        tasks
    }

    /// Commit the placement of a queued task, as `dispatch` does before it
    /// sends anything.
    fn place(&self, task: &TaskId, on: &str) -> AttemptId {
        let attempt = AttemptId::generate();
        let outcome = self
            .c
            .store()
            .place(
                task,
                &worker(on),
                &attempt,
                |_| true,
                |input| demand_of(input, 60_000),
                2,
            )
            .unwrap();
        assert!(matches!(outcome, PlaceOutcome::Placed(_)), "{outcome:?}");
        attempt
    }

    async fn placed(&self, count: usize, on: &str) -> Vec<AttemptId> {
        let tasks = self.tasks(count).await;
        tasks.iter().map(|t| self.place(t, on)).collect()
    }

    fn observations(&self, on: &str) -> Vec<(AttemptId, Observation)> {
        let live = self.c.store().live_attempts(Some(&worker(on))).unwrap();
        live.into_iter()
            .map(|l| (l.record.attempt_id, l.record.observation))
            .collect()
    }
}

fn all(ids: &[AttemptId], observation: Observation) -> Vec<(AttemptId, Observation)> {
    ids.iter().map(|id| (id.clone(), observation)).collect()
}

#[tokio::test]
async fn a_detach_reaches_the_attempts_beyond_the_first_page_and_no_other_worker() {
    let f = fx();
    let on_a = f.placed(MANY, "w-a").await;
    let on_b = f.placed(2, "w-b").await;
    f.c.detach(&worker("w-a")).await.unwrap();
    assert_eq!(f.observations("w-a"), all(&on_a, Observation::Unknown));
    assert_eq!(f.observations("w-b"), all(&on_b, Observation::Known));
}

#[tokio::test]
async fn a_detach_of_the_installed_link_reaches_the_attempts_beyond_the_first_page() {
    let f = fx();
    let on_a = f.placed(MANY, "w-a").await;
    let link: Arc<dyn WorkerLink> = Arc::new(Silent::default());
    f.c.try_attach(worker("w-a"), Arc::clone(&link)).unwrap();
    assert!(f.c.detach_link(&worker("w-a"), &link).await.unwrap());
    assert_eq!(f.observations("w-a"), all(&on_a, Observation::Unknown));
}

#[tokio::test]
async fn a_tick_looks_at_every_live_attempt_of_a_worker_once_beyond_the_first_page() {
    let f = fx();
    let on_a = f.placed(MANY, "w-a").await;
    f.c.try_attach(worker("w-a"), Arc::new(Silent::default()))
        .unwrap();
    // The worker gives no answer: each of its attempts is `unknown`, once.
    let report = f.c.tick().await.unwrap();
    assert_eq!(report.unknown, on_a);
    assert_eq!(f.observations("w-a"), all(&on_a, Observation::Unknown));
}

#[tokio::test]
async fn a_placed_attempt_is_found_only_on_its_own_worker() {
    let f = fx();
    let on_a = f.placed(1, "w-a").await;
    let link = Arc::new(Silent::default());
    let attached: Attached = link.clone();
    f.c.attach(worker("w-a"), attached.clone()).await;
    let mut report = TickReport::default();
    let sent =
        f.c.send_placed(&worker("w-b"), &*link, &attached, &on_a[0], &mut report)
            .await
            .unwrap();
    assert_eq!(sent, Sent::NotTaken);
    assert_eq!(link.calls.load(Ordering::SeqCst), 0, "nothing was sent");
    let sent =
        f.c.send_placed(&worker("w-a"), &*link, &attached, &on_a[0], &mut report)
            .await
            .unwrap();
    assert_eq!(sent, Sent::NotTaken);
    assert_eq!(link.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_pending_plans_of_a_worker_are_its_oldest_readable_ones_up_to_the_cap() {
    let f = fx();
    let store = f.c.store();
    let ids = |plans: Vec<WorkloadPlan>| -> Vec<AttemptId> {
        plans.into_iter().map(|p| p.attempt_id).collect()
    };
    let pending = |on: &str| ids(scheduler::pending_plans(store, &worker(on)).unwrap());
    assert!(pending("w-a").is_empty());

    // Placements no plan can be read from come first: they are passed over,
    // and do not take the place of one that can.
    let tasks = f.tasks(MANY + 3).await;
    let first = f.place(&tasks[0], "w-a");
    let no_attempt = store
        .record_placement(&tasks[0], &worker("w-a"), None, &json!({}), 3)
        .unwrap();
    for demand in [json!({}), json!({"payload": 1})] {
        store
            .record_placement(&tasks[0], &worker("w-a"), Some(&first), &demand, 3)
            .unwrap();
    }
    let elsewhere = f.place(&tasks[1], "w-b");
    let mut on_a = vec![first];
    for task in &tasks[2..] {
        on_a.push(f.place(task, "w-a"));
    }
    assert_eq!(on_a.len(), MANY + 2);
    let cap = crate::forecast::plan::MAX_PENDING_PLANS;
    assert_eq!(cap, 64);
    assert_eq!(pending("w-a"), on_a[..cap]);
    assert_eq!(pending("w-b"), vec![elsewhere]);

    // An acknowledged one is no longer pending; the next oldest takes its
    // place, also from the page after.
    let acked = store.unacked_demand().unwrap();
    assert_eq!(acked[0].attempt_id.as_ref(), Some(&on_a[0]));
    store.ack_placement(acked[0].placement_id, 4).unwrap();
    store.ack_placement(no_attempt, 4).unwrap();
    assert_eq!(pending("w-a"), on_a[1..cap + 1]);

    // Fewer than the cap: all of them, to the end of the last page. (Before
    // the second of this worker lie the four above and the one elsewhere.)
    assert_eq!(acked[5].attempt_id.as_ref(), Some(&on_a[1]));
    for p in &acked[6..] {
        store.ack_placement(p.placement_id, 4).unwrap();
    }
    assert_eq!(pending("w-a"), vec![on_a[1].clone()]);
}
