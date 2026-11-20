//! The steps of the coordinator that cannot be reached from outside at the
//! moment they matter: what happens between a placement and its submit.

use super::link::LinkFuture;
use super::*;
use crate::ids::{Digest, NodeId};
use std::sync::atomic::AtomicUsize;

/// A worker that counts what it is sent and answers nothing.
#[derive(Default)]
pub(super) struct Silent {
    calls: AtomicUsize,
    pub(super) features: Vec<&'static str>,
}

impl Silent {
    pub(super) fn with_features(features: Vec<&'static str>) -> Self {
        Self {
            features,
            ..Self::default()
        }
    }
}

impl WorkerLink for Silent {
    fn ci_task_negotiated(&self) -> bool {
        self.features.contains(&crate::wire::CI_TASK_FEATURE)
    }

    fn ci_discovery_negotiated(&self) -> bool {
        self.features.contains(&crate::wire::CI_DISCOVERY_FEATURE)
    }

    fn matrix_dispatch_negotiated(&self) -> bool {
        self.features
            .contains(&crate::wire::MATRIX_DISPATCH_FEATURE)
    }

    fn workflow_artifacts_negotiated(&self) -> bool {
        self.features
            .contains(&crate::wire::WORKFLOW_ARTIFACTS_FEATURE)
    }

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

pub(super) struct Fx {
    _dir: tempfile::TempDir,
    pub(super) c: Coordinator,
    pub(super) worker: WorkerId,
}

pub(super) fn fx() -> Fx {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let c = Coordinator::start(store, CoordinatorConfig::default()).unwrap();
    Fx {
        _dir: dir,
        c,
        worker: WorkerId::parse("w-a").unwrap(),
    }
}

impl Fx {
    /// Queue one task and commit its placement, as `dispatch` does before it
    /// sends anything.
    pub(super) async fn placed(&self) -> (TaskId, AttemptId) {
        let source = SourceId::generate();
        self.c
            .store()
            .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
            .unwrap();
        let task = TaskId::generate();
        self.c
            .enqueue(RunId::generate(), source, vec![(task.clone(), spec())])
            .await
            .unwrap();
        let attempt = AttemptId::generate();
        let outcome = self
            .c
            .store()
            .place(
                &task,
                &self.worker,
                &attempt,
                |_| true,
                |input| demand_of(input, 60_000),
                2,
            )
            .unwrap();
        assert!(matches!(outcome, PlaceOutcome::Placed(_)), "{outcome:?}");
        (task, attempt)
    }

    async fn send(&self, link: &Silent, attempt: &AttemptId) -> Sent {
        let mut report = TickReport::default();
        self.c
            .send_placed(&self.worker, link, attempt, &mut report)
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn a_placed_attempt_is_sent_as_it_was_committed() {
    let f = fx();
    let link = Silent::default();
    let (_task, attempt) = f.placed().await;
    // Sent; this worker gives no answer, so it did not take it.
    assert_eq!(f.send(&link, &attempt).await, Sent::NotTaken);
    assert_eq!(link.calls.load(Ordering::SeqCst), 1);
    // An attempt that is not there (any more) is not sent.
    assert_eq!(f.send(&link, &AttemptId::generate()).await, Sent::NotTaken);
    assert_eq!(link.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_cancel_recorded_after_the_placement_holds_the_submit_back() {
    let f = fx();
    let link = Silent::default();
    let (task, attempt) = f.placed().await;
    f.c.cancel_task(&task).await.unwrap();
    assert_eq!(f.send(&link, &attempt).await, Sent::Withheld);
    assert_eq!(link.calls.load(Ordering::SeqCst), 0, "nothing was sent");
}

#[tokio::test]
async fn a_retire_recorded_after_the_placement_holds_the_submit_back() {
    let f = fx();
    let link = Silent::default();
    let (_task, attempt) = f.placed().await;
    f.c.request_retire(&attempt, "move").await.unwrap();
    assert_eq!(f.send(&link, &attempt).await, Sent::Withheld);
    assert_eq!(link.calls.load(Ordering::SeqCst), 0, "nothing was sent");
}

#[test]
fn a_spec_is_checked_with_the_construction_every_placement_uses() {
    let (run, source, task) = (RunId::generate(), SourceId::generate(), TaskId::generate());
    let good = serde_json::to_value(spec()).unwrap();
    check_spec(&run, &source, &task, &good, 60_000).unwrap();
    // Not a task spec at all, and one whose command is not a command.
    let mut broken = good.clone();
    broken["commands"][0]["node_id"] = json!("");
    for bad in [json!({"nope": 1}), json!(7), broken] {
        let e = check_spec(&run, &source, &task, &bad, 60_000).unwrap_err();
        assert!(matches!(e, CoordinatorError::Invalid(_)), "{e:?}");
        assert!(e.to_string().contains(&task.to_string()), "{e}");
    }
    // The demand is the payload itself, under the hash that is recorded.
    let input = PlaceInput {
        permit_id: None,
        run_id: run.clone(),
        task_id: task.clone(),
        generation: Generation(3),
        source_id: Some(source.clone()),
        spec: good,
    };
    let (hash, demand) = demand_of(&input, 60_000).unwrap();
    let payload: AttemptPayload = serde_json::from_value(demand["payload"].clone()).unwrap();
    assert_eq!(payload.generation, Generation(3));
    assert_eq!(payload.lease_ms, 60_000);
    assert_eq!(payload_hash(&payload).unwrap(), hash);
    // A run without a source has no payload.
    let no_source = PlaceInput {
        source_id: None,
        ..input
    };
    assert!(matches!(
        demand_of(&no_source, 60_000),
        Err(StorageError::Invalid(_))
    ));
}

#[tokio::test]
async fn futures_are_joined_in_their_order_whichever_ends_first() {
    let slow = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        1u32
    }) as Pin<Box<dyn Future<Output = u32> + Send>>;
    let fast = Box::pin(async { 2u32 }) as Pin<Box<dyn Future<Output = u32> + Send>>;
    assert_eq!(join_all(vec![slow, fast]).await, vec![1, 2]);
    assert_eq!(join_all::<u32>(vec![]).await, Vec::<u32>::new());
}

#[test]
fn declared_artifacts_reach_the_payload_and_its_hash() {
    use crate::wire::MissingPolicy;
    let input = |spec: &TaskSpec| PlaceInput {
        permit_id: None,
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: Some(SourceId::generate()),
        spec: serde_json::to_value(spec).unwrap(),
    };
    // A spec that declares none is stored as before the field existed.
    let none = spec();
    assert!(serde_json::to_value(&none)
        .unwrap()
        .get("artifacts")
        .is_none());
    let mut declared = spec();
    declared.artifacts = vec![ArtifactDecl {
        name: "report".into(),
        path: "out/report.txt".into(),
        if_missing: MissingPolicy::Required,
    }];
    let at = input(&declared);
    let read: TaskSpec = serde_json::from_value(at.spec.clone()).unwrap();
    let payload = build_payload(&at, &read, 60_000).unwrap();
    assert_eq!(payload.artifacts, declared.artifacts);
    // The same attempt with another list is another payload.
    let without = build_payload(&at, &none, 60_000).unwrap();
    assert!(without.artifacts.is_empty());
    assert_ne!(
        payload_hash(&payload).unwrap(),
        payload_hash(&without).unwrap()
    );
}

#[test]
fn setup_survives_queue_serialization_and_payload_construction() {
    let mut task = spec();
    // Old persisted queue records have no setup field.
    let mut legacy = serde_json::to_value(&task).unwrap();
    legacy.as_object_mut().unwrap().remove("setup");
    assert!(serde_json::from_value::<TaskSpec>(legacy)
        .unwrap()
        .setup
        .is_none());
    let input = PlaceInput {
        permit_id: None,
        run_id: RunId::generate(),
        task_id: TaskId::generate(),
        generation: Generation(1),
        source_id: Some(SourceId::generate()),
        spec: serde_json::Value::Null,
    };
    assert!(build_payload(&input, &task, 1234).unwrap().setup.is_none());
    task.setup = Some(crate::wire::SetupSpec {
        steps: vec![],
        toolchain: vec![],
        scope: crate::wire::ReuseScope::Run,
        freshness_secs: Some(90),
        secrets: vec![],
        no_cache: true,
        step_timeout_ms: Some(123),
    });
    let read: TaskSpec = serde_json::from_slice(&serde_json::to_vec(&task).unwrap()).unwrap();
    let payload = build_payload(&input, &read, 1234).unwrap();
    assert_eq!(payload.setup, task.setup);
    let roundtrip: AttemptPayload =
        serde_json::from_slice(&serde_json::to_vec(&payload).unwrap()).unwrap();
    assert_eq!(roundtrip.setup, task.setup);
}

// ------------------------------------------------------------- CI tasks

/// A task of a CI plan, with `steps` commands in the plan.
fn ci_spec(hold: bool, steps: u32) -> TaskSpec {
    TaskSpec {
        ci: Some(CiSpec {
            history_keys: Vec::new(),
            dynamic_origin: false,
            core_outcome: None,
            matrix: None,
            shard_retry_forbidden: false,
            shard_preferred_worker: None,
            task: crate::wire::CiTask {
                verify_shard_version: false,
                artifact_workflow: None,
                dispatch: None,
                plan_digest: Digest::of_bytes(b"plan"),
                task_id: "ci.yml/build".into(),
                inputs: serde_json::json!({"needs": {}}),
            },
            hold,
            steps,
            unrunnable: None,
            shard_group: None,
            joined: false,
            join_failure: None,
            rerun: false,
        }),
        commands: vec![],
        ..spec()
    }
}

#[test]
fn a_worker_without_ci_tasks_is_left_out_of_ci_tasks_only() {
    assert!(policy::lacks_ci_task(&ci_spec(false, 1), false));
    assert!(!policy::lacks_ci_task(&ci_spec(false, 1), true));
    assert!(!policy::lacks_ci_task(&spec(), false));
    assert!(!policy::lacks_ci_task(&spec(), true));
    assert_eq!(policy::CI_TASK_UNMET, "protocol feature ci_task_v1");
}

#[test]
fn the_declarations_of_a_ci_task_leave_room_for_its_result() {
    let max = crate::outputs::MAX_OUTPUTS;
    assert!(policy::ci_declarations_fit(0, 0));
    assert!(policy::ci_declarations_fit(max - 1, 0));
    assert!(policy::ci_declarations_fit(1, max - 2));
    assert!(!policy::ci_declarations_fit(max, 0));
    assert!(!policy::ci_declarations_fit(1, max - 1));
    assert!(!policy::ci_declarations_fit(usize::MAX, 1));
}

#[test]
fn the_forecast_of_a_ci_task_is_asked_for_every_step_of_the_plan() {
    let a = AttemptId::generate();
    let plan = policy::workload_of_spec(&a, &ci_spec(false, 3), Some(40));
    let names: Vec<&str> = plan.nodes.iter().map(|n| n.node_id.as_str()).collect();
    assert_eq!(names, ["step-0", "step-1", "step-2"]);
    assert_eq!(plan.plan_digest, Digest::of_bytes(b"plan"));
    assert_eq!(plan.attempt_id, a);
    assert!(plan.nodes[0].depends_on.is_empty());
    assert_eq!(plan.nodes[1].depends_on, [plan.nodes[0].node_id.clone()]);
    assert_eq!(plan.nodes[2].depends_on, [plan.nodes[1].node_id.clone()]);
    // Only the first step waits for the workspace.
    let delays: Vec<_> = plan.nodes.iter().map(|n| n.external_delay_ms).collect();
    assert_eq!(delays, [Some(40), None, None]);
    assert!(plan.nodes.iter().all(|n| n.correlation.attempt_id == a));
    assert_eq!(plan.nodes[2].correlation.node_id, plan.nodes[2].node_id);
    assert!(policy::workload_of_spec(&a, &ci_spec(false, 0), None)
        .nodes
        .is_empty());
    // A task of commands is asked for its commands, as before.
    let commands = policy::workload_of_spec(&a, &spec(), Some(40));
    assert_eq!(commands.nodes.len(), 1);
    assert_eq!(commands.nodes[0].node_id.as_str(), "main");
    assert_eq!(commands.nodes[0].external_delay_ms, Some(40));
}

#[tokio::test]
async fn a_ci_task_is_queued_without_commands_and_what_no_worker_takes_is_refused() {
    let f = fx();
    let source = SourceId::generate();
    let store = f.c.store();
    store
        .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    let enqueue = |spec: TaskSpec| {
        let (c, source) = (&f.c, source.clone());
        async move {
            let task = TaskId::generate();
            let run = RunId::generate();
            let result = c.enqueue(run, source, vec![(task.clone(), spec)]).await;
            result.map(|()| task).map_err(|e| e.to_string())
        }
    };
    // A task of commands still needs one.
    let mut bare = spec();
    bare.commands.clear();
    assert!(enqueue(bare).await.unwrap_err().contains("no command"));
    let mut both = ci_spec(false, 1);
    both.commands = spec().commands;
    assert!(enqueue(both).await.unwrap_err().contains("no commands"));
    let mut secrets = ci_spec(false, 1);
    secrets.has_secret_bundle = true;
    assert!(enqueue(secrets).await.unwrap_err().contains("secret files"));
    let mut reserved = ci_spec(false, 1);
    reserved.artifacts = vec![ArtifactDecl {
        name: "rtr-ci-task-result.json".into(),
        path: "x".into(),
        if_missing: crate::wire::MissingPolicy::Optional,
    }];
    assert!(enqueue(reserved).await.unwrap_err().contains("reserved"));
    let mut many = ci_spec(false, 1);
    many.artifacts = (0..crate::outputs::MAX_OUTPUTS)
        .map(|n| ArtifactDecl {
            name: format!("a{n}"),
            path: format!("a{n}"),
            if_missing: crate::wire::MissingPolicy::Optional,
        })
        .collect();
    assert!(enqueue(many).await.unwrap_err().contains("too many"));

    // A task that waits is a task of its run, and is not in the queue.
    let ready = enqueue(ci_spec(false, 2)).await.unwrap();
    let held = enqueue(ci_spec(true, 2)).await.unwrap();
    let queued: Vec<TaskId> = store
        .queued_tasks(16)
        .unwrap()
        .into_iter()
        .map(|t| t.task_id)
        .collect();
    assert_eq!(queued, std::slice::from_ref(&ready));
    let waiting = store.ci_waiting_runs(16).unwrap();
    assert_eq!(waiting.len(), 1);
    let rows = store.ci_run_tasks(&waiting[0]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].task_id, held);
    assert!(rows[0].attempt.is_none() && !rows[0].cancelled);
    // It cannot be placed while it waits.
    let attempt = AttemptId::generate();
    let place = |task: &TaskId, attempt: &AttemptId| {
        store
            .place(
                task,
                &f.worker,
                attempt,
                |_| true,
                |input| demand_of(input, 60_000),
                2,
            )
            .unwrap()
    };
    assert!(!matches!(place(&held, &attempt), PlaceOutcome::Placed(_)));
    // Released with its inputs, it is queued like any other.
    let released = serde_json::to_value(ci_spec(false, 2)).unwrap();
    assert!(store.release_ci_task(&held, &released, 5).unwrap());
    assert!(!store.release_ci_task(&held, &released, 6).unwrap(), "once");
    assert_eq!(store.queued_tasks(16).unwrap().len(), 2);
    assert!(store.ci_waiting_runs(16).unwrap().is_empty());

    // The placement keeps the number of steps: the payload does not say
    // it, and the next forecast question carries the work that was placed.
    assert!(matches!(place(&held, &attempt), PlaceOutcome::Placed(_)));
    let demand = store.placement_demand(&attempt).unwrap().unwrap();
    assert_eq!(demand["ci_steps"], 2);
    assert!(demand["payload"]["ci_task"].is_object());
    let pending = crate::scheduler::pending_plans(store, &f.worker).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].attempt_id, attempt);
    let names: Vec<&str> = pending[0]
        .nodes
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    assert_eq!(names, ["step-0", "step-1"]);
    // A task of commands has no such number.
    let (_task, other) = f.placed().await;
    let demand = store.placement_demand(&other).unwrap().unwrap();
    assert!(demand.get("ci_steps").is_none());
    let pending = crate::scheduler::pending_plans(store, &f.worker).unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[1].nodes[0].node_id.as_str(), "main");
}

/// A run of three tasks of one plan: `first` is queued, `placed` has an
/// attempt, `held` waits for the jobs it needs.
async fn ci_run_of_three(f: &Fx) -> (RunId, TaskId, TaskId, TaskId) {
    let source = SourceId::generate();
    f.c.store()
        .put_source_ref(&source, &Digest::of_bytes(b"manifest"), 1)
        .unwrap();
    let named = |name: &str, hold: bool| {
        let mut spec = ci_spec(hold, 1);
        spec.ci.as_mut().unwrap().task.task_id = name.into();
        spec
    };
    let (run, first, placed, held) = (
        RunId::generate(),
        TaskId::generate(),
        TaskId::generate(),
        TaskId::generate(),
    );
    let tasks = vec![
        (first.clone(), named("ci.yml/first", false)),
        (placed.clone(), named("ci.yml/placed", false)),
        (held.clone(), named("ci.yml/held", true)),
    ];
    f.c.enqueue(run.clone(), source, tasks).await.unwrap();
    let outcome =
        f.c.store()
            .place(
                &placed,
                &f.worker,
                &AttemptId::generate(),
                |_| true,
                |input| demand_of(input, 60_000),
                2,
            )
            .unwrap();
    assert!(matches!(outcome, PlaceOutcome::Placed(_)));
    (run, first, placed, held)
}

fn unrunnable_events(store: &CoordinatorStore) -> Vec<(String, String)> {
    store
        .events_after(0, 1000)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "ci_task_unrunnable")
        .map(|e| {
            (
                e.payload["task"].as_str().unwrap().to_string(),
                e.payload["reason"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_ci_run_whose_plan_is_gone_ends_with_the_reason_instead_of_waiting() {
    let f = fx();
    let store = f.c.store();
    let (run, first, placed, held) = ci_run_of_three(&f).await;
    let state = f._dir.path().join("parent");
    let results = state.join("results");

    // The parent has no file of the plan: the run can never go on.
    let pass = crate::ci::advance::advance_runs(store, &state, &results, 7).unwrap();
    assert!(
        pass.faults.is_empty() && pass.released.is_empty(),
        "{pass:?}"
    );
    assert_eq!(pass.failed.len(), 1);
    assert_eq!(pass.failed[0].0, run);
    let why = pass.failed[0].1.clone();
    assert!(why.contains("the frozen plan"), "{why}");
    // What was not placed yet is cancelled, in the order it was made; the
    // task that has an attempt is left to end.
    let cancelled: Vec<&TaskId> = pass.unrunnable.iter().map(|(task, _)| task).collect();
    assert_eq!(cancelled, [&first, &held]);
    assert!(pass.unrunnable.iter().all(|(_, reason)| reason == &why));
    let rows = store.ci_run_tasks(&run).unwrap();
    let row = |task: &TaskId| rows.iter().find(|row| &row.task_id == task).unwrap();
    assert!(row(&first).cancelled && row(&held).cancelled);
    assert!(!row(&placed).cancelled);
    assert_eq!(row(&held).spec["ci"]["unrunnable"], why.as_str());
    assert_eq!(row(&first).spec["ci"]["unrunnable"], why.as_str());
    assert!(row(&placed).spec["ci"].get("unrunnable").is_none());
    assert!(store.queued_tasks(16).unwrap().is_empty());
    // The reason is an event of the run, once per task, and is shown where
    // the task is shown.
    assert_eq!(
        unrunnable_events(store),
        [
            (first.to_string(), why.clone()),
            (held.to_string(), why.clone())
        ]
    );
    let shown = store.query_task(held.clone()).unwrap().unwrap();
    assert_eq!(shown.unrunnable.as_deref(), Some(why.as_str()));
    assert!(shown.cancel_requested_at_ms.is_some());
    let shown = store.query_task(placed.clone()).unwrap().unwrap();
    assert_eq!(shown.unrunnable, None);
    let page = store.query_tasks(run.clone(), 16, None).unwrap();
    let reasons = page.items.iter().filter(|t| t.unrunnable.is_some()).count();
    assert_eq!(reasons, 2);
    let lines = crate::service::ci_run::job_lines(store, &state, &results, &run).unwrap();
    let line = |task: &TaskId| lines.iter().find(|line| &line.task_id == task).unwrap();
    assert_eq!(line(&held).reason.as_deref(), Some(why.as_str()));
    assert_eq!(line(&placed).reason, None);
    let text = crate::service::ci_run::render_jobs(&lines);
    assert!(
        text.contains(&format!("ci.yml/held: no_result ({why})")),
        "{text}"
    );

    // Nothing waits any more: the next pass does not see the run, and
    // nothing is written twice.
    assert!(store.ci_waiting_runs(16).unwrap().is_empty());
    let again = crate::ci::advance::advance_runs(store, &state, &results, 8).unwrap();
    assert_eq!(again, crate::ci::advance::Pass::default());
    assert!(crate::ci::advance::fail_run(store, &run, "again", 9)
        .unwrap()
        .is_empty());
    assert_eq!(unrunnable_events(store).len(), 2);
}

#[tokio::test]
async fn a_ci_run_that_only_could_not_be_read_now_is_left_as_it_is_and_tried_again() {
    let f = fx();
    let store = f.c.store();
    let (run, first, _placed, held) = ci_run_of_three(&f).await;
    let state = f._dir.path().join("parent");
    let results = state.join("results");
    // Something that is not a file is where the plan is kept: the read
    // fails, but not because the plan is gone.
    let path = crate::worker::ci_task::plan_path(&state, &Digest::of_bytes(b"plan"));
    std::fs::create_dir_all(&path).unwrap();
    for _ in 0..2 {
        let pass = crate::ci::advance::advance_runs(store, &state, &results, 7).unwrap();
        assert!(pass.failed.is_empty() && pass.unrunnable.is_empty());
        assert_eq!(pass.faults.len(), 1);
        assert_eq!(pass.faults[0].0, run);
        assert!(pass.faults[0].1.contains("the frozen plan"));
        // The plan of a run that waits is kept.
        assert!(pass.plans.contains(&Digest::of_bytes(b"plan")));
    }
    let rows = store.ci_run_tasks(&run).unwrap();
    assert!(rows.iter().all(|row| !row.cancelled));
    assert!(unrunnable_events(store).is_empty());
    assert_eq!(
        store.ci_waiting_runs(16).unwrap(),
        std::slice::from_ref(&run)
    );

    // A file that is there but is not the plan: that will not pass.
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, b"{}").unwrap();
    let pass = crate::ci::advance::advance_runs(store, &state, &results, 8).unwrap();
    assert_eq!(pass.failed.len(), 1);
    assert!(pass.failed[0].1.contains("digest"), "{}", pass.failed[0].1);
    assert!(store.ci_waiting_runs(16).unwrap().is_empty());

    // Only one task, when one is named; one that is cancelled is not
    // cancelled again, and a task of commands is never touched.
    let (other, first_2, _placed_2, held_2) = ci_run_of_three(&f).await;
    let one = store
        .fail_ci_tasks(&other, Some(&held_2), "too large", 9)
        .unwrap();
    assert_eq!(one, std::slice::from_ref(&held_2));
    assert!(store
        .fail_ci_tasks(&other, Some(&held_2), "too large", 9)
        .unwrap()
        .is_empty());
    let rows = store.ci_run_tasks(&other).unwrap();
    let cancelled: Vec<&TaskId> = rows
        .iter()
        .filter(|r| r.cancelled)
        .map(|r| &r.task_id)
        .collect();
    assert_eq!(cancelled, [&held_2]);
    assert_eq!(
        store.fail_ci_tasks(&other, None, "gone", 10).unwrap(),
        [first_2]
    );
    let (plain, _attempt) = f.placed().await;
    let plain_run = store.query_task(plain.clone()).unwrap().unwrap().run_id;
    assert!(store
        .fail_ci_tasks(&plain_run, None, "gone", 11)
        .unwrap()
        .is_empty());
    let _ = (first, held);
}
