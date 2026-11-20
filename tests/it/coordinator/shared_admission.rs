//! A52 on the coordinator's side (spec 24.4 and 24.5): what a member does
//! when a worker's admission window is full with the work of every member.
//! The workers are the in-memory ones of the parent module; `foreign` is the
//! number of attempts other members hold there.
//!
//! No process is started here.

use super::*;
use belt::coordinator::{
    EVENT_SUBMIT_REFUSED_ATTEMPT_ID, EVENT_SUBMIT_REFUSED_CAPACITY, REASON_CAPACITY_REFUSED,
};
use belt::error::OverloadedReason;
use belt::scheduler::decide::{Decision, Exclusion};
use belt::scheduler::{reason_of, EVENT_TASK_WAITING, EVENT_WORKER_EXCLUDED};

fn full() -> ApiError {
    ApiError::overloaded(OverloadedReason::Capacity, "the window is full")
}

fn events(c: &Coordinator, kind: &str) -> Vec<serde_json::Value> {
    let all = c.store().events_after(0, 10_000).unwrap();
    let of_kind = all.into_iter().filter(|e| e.kind == kind);
    of_kind.map(|e| e.payload).collect()
}

fn noted(c: &Coordinator, a: &AttemptId, kind: &str) -> Vec<serde_json::Value> {
    let all = c.store().events_of(a, kind).unwrap();
    all.into_iter().map(|e| e.payload).collect()
}

fn excluded(d: &Decision) -> Vec<(&str, Exclusion)> {
    let all = d.excluded.iter();
    all.map(|e| (e.worker_id.as_str(), e.why)).collect()
}

fn reason(c: &Coordinator, a: &AttemptId) -> Decision {
    reason_of(c.store(), a).unwrap().unwrap()
}

/// The submit of an attempt as it was recorded: what a copy that is still on
/// its way carries.
fn copy_of(c: &Coordinator, a: &AttemptId) -> SubmitAttemptRequest {
    let demand = c.store().placement_demand(a).unwrap().unwrap();
    SubmitAttemptRequest::new(
        RequestId::generate(),
        c.store().epoch(),
        a.clone(),
        serde_json::from_value(demand["payload"].clone()).unwrap(),
    )
    .unwrap()
}

/// The window filled between the `inspect` and the submit (another member
/// was faster). The submit was sent once and answered: nothing of it exists
/// anywhere, so the attempt is closed without a retire and the task goes to
/// another worker as its next generation.
#[tokio::test]
async fn a52_an_attempt_sent_once_and_refused_for_capacity_is_closed_without_a_retire() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let b = f.worker("w-b", (1, 0));
    let c = f.open(&[&a, &b]);
    let task = enqueue(&c, 1).await.remove(0);
    a.set(|s| s.fault = Some(full()));
    let report = c.tick().await.unwrap();
    let first = only(&c, &task);
    assert_eq!(first.worker_id, a.id);
    // Closed at once: the task is free with this tick, and it did not fail.
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    assert!(report.settled.is_empty() && report.unknown.is_empty());
    assert!(first.retired && first.termination.is_none());
    assert_eq!(queued(&c), vec![task.clone()]);
    assert!(c.store().unacked_demand().unwrap().is_empty());
    // One submit, and no retire, went to the worker; it holds nothing, no
    // tombstone either.
    assert_eq!(a.received(Op::Submit), 1);
    assert_eq!(a.received(Op::Retire), 0);
    assert!(a.holds().is_empty());
    assert!(a.state.lock().unwrap().tombstones.is_empty());
    // Why is in this member's database only, written with the tombstone,
    // which is confirmed as it is written.
    let (asked, confirmed) = c
        .store()
        .retire_tombstone(&first.attempt_id)
        .unwrap()
        .unwrap();
    assert_eq!(confirmed, Some(asked));
    let why = noted(&c, &first.attempt_id, EVENT_SUBMIT_REFUSED_CAPACITY);
    assert_eq!(why.len(), 1);
    assert_eq!(why[0]["worker"], "w-a");
    assert_eq!(why[0]["code"], "OVERLOADED");
    assert_eq!(why[0]["reason"], "capacity");
    assert_eq!(why[0]["retire"], false);
    assert_eq!(REASON_CAPACITY_REFUSED, "capacity_refused");
    assert_eq!(EVENT_SUBMIT_REFUSED_CAPACITY, "submit_refused_capacity");
    for kind in ["retire_requested", "retire_confirmed", "submit_deferred"] {
        assert!(noted(&c, &first.attempt_id, kind).is_empty(), "{kind}");
    }
    // The worker is not left out for the task for good.
    assert!(events(&c, EVENT_WORKER_EXCLUDED).is_empty());
    assert!(c
        .store()
        .excluded_workers(&task, belt::scheduler::REASON_WORKER_CANNOT_RUN)
        .unwrap()
        .is_empty());

    // The other member's attempt is what fills the window. The next
    // generation goes to the other worker, and why the first one is out is
    // in the reason.
    a.set(|s| {
        s.fault = None;
        s.foreign = 1;
    });
    let report = c.tick().await.unwrap();
    let all = attempts(&c, &task);
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].generation, Generation(2));
    assert_eq!(all[1].worker_id, b.id);
    assert_eq!(report.placed, vec![all[1].attempt_id.clone()]);
    assert_eq!(
        excluded(&reason(&c, &all[1].attempt_id)),
        [("w-a", Exclusion::Capacity)]
    );
    assert_eq!(a.received(Op::Submit), 1, "nothing more was sent there");
    // A copy of the refused submit there is none; were one to arrive now
    // that the window is full, it would be refused again and leave nothing.
    let e = a
        .handle(Request::SubmitAttempt(copy_of(&c, &first.attempt_id)))
        .unwrap_err();
    assert_eq!(e.overloaded_reason, Some(OverloadedReason::Capacity));
    assert!(a.holds().is_empty());
    // The command starts once, on the worker that took it.
    b.start(&all[1].attempt_id);
    assert_eq!(f.cluster.starts_of(&task), vec!["w-b"]);
    assert!(c.tick().await.unwrap().placed.is_empty());
    assert_eq!(attempts(&c, &task).len(), 2);
}

/// A submit whose first send was never answered may still arrive. When the
/// resend is refused for capacity, also by the next parent process, the
/// attempt is retired on the worker first; the task moves only once the
/// worker confirmed that nothing started, and the late copy of the first
/// submit is refused as retired.
#[tokio::test]
async fn a52_an_attempt_whose_send_is_uncertain_is_retired_before_the_task_moves() {
    for restart in [false, true] {
        let f = fx();
        let a = f.worker("w-a", (1, 0));
        let b = f.worker("w-b", (1, 0));
        let mut c = f.open(&[&a, &b]);
        let task = enqueue(&c, 1).await.remove(0);
        // The first send is on its way for a long time.
        a.set(|s| s.lose.push((Op::Submit, Lost::Request)));
        let report = c.tick().await.unwrap();
        let first = only(&c, &task);
        assert_eq!(first.worker_id, a.id);
        assert_eq!(report.unknown, vec![first.attempt_id.clone()]);
        let late = copy_of(&c, &first.attempt_id);
        if restart {
            // How often it was sent is in the database, not in the process.
            drop(c);
            c = f.open(&[&a, &b]);
        }
        // The window fills; the resend is answered "nothing stored".
        a.set(|s| s.fault = Some(full()));
        let report = c.tick().await.unwrap();
        assert_eq!(a.received(Op::Submit), 2);
        assert!(report.retired.is_empty() && report.placed.is_empty());
        // Not closed: a retire is asked for, and the task stays.
        let now = only(&c, &task);
        assert!(!now.retired && now.termination.is_none());
        let (_, confirmed) = c
            .store()
            .retire_tombstone(&first.attempt_id)
            .unwrap()
            .unwrap();
        assert_eq!(confirmed, None);
        assert!(queued(&c).is_empty());
        assert!(b.holds().is_empty());
        let why = noted(&c, &first.attempt_id, EVENT_SUBMIT_REFUSED_CAPACITY);
        assert_eq!(why.len(), 1);
        assert_eq!(why[0]["reason"], "capacity");
        assert_eq!(why[0]["retire"], true);
        assert_eq!(noted(&c, &first.attempt_id, "retire_requested").len(), 1);

        // While the worker cannot confirm, the task is kept, unknown.
        a.set(|s| s.offline = true);
        for _ in 0..3 {
            let report = c.tick().await.unwrap();
            assert!(report.placed.is_empty() && report.retired.is_empty());
        }
        assert_eq!(only(&c, &task).observation, Observation::Unknown);
        assert!(b.holds().is_empty());

        // It answers: tombstone written, never started. Only now is the
        // next generation placed, on the worker that has room.
        a.set(|s| {
            s.offline = false;
            s.fault = None;
            s.foreign = 1;
        });
        let report = c.tick().await.unwrap();
        assert_eq!(report.retired, vec![first.attempt_id.clone()]);
        assert_eq!(
            a.received(Op::Submit),
            2,
            "the refused attempt is not sent again"
        );
        let all = attempts(&c, &task);
        assert_eq!(all.len(), 2);
        assert!(all[0].retired && all[0].termination.is_none());
        assert_eq!(all[1].worker_id, b.id);
        assert_eq!(report.placed, vec![all[1].attempt_id.clone()]);
        // The first submit arrives at last, when the window has room again:
        // refused as retired, and never started. (The copy a parent process
        // before this one sent is of an older epoch, and refused for that.)
        a.set(|s| s.foreign = 0);
        let refused = if restart {
            ErrorCode::StaleEpoch
        } else {
            ErrorCode::Retired
        };
        for _ in 0..3 {
            let e = a.handle(Request::SubmitAttempt(late.clone())).unwrap_err();
            assert_eq!(e.code, refused);
            let again = copy_of(&c, &first.attempt_id);
            let e = a.handle(Request::SubmitAttempt(again)).unwrap_err();
            assert_eq!(e.code, ErrorCode::Retired);
        }
        assert!(a.holds().is_empty());
        b.start(&all[1].attempt_id);
        assert_eq!(f.cluster.starts_of(&task), vec!["w-b"]);
        assert!(c.tick().await.unwrap().placed.is_empty());
        assert_eq!(attempts(&c, &task).len(), 2);
    }
}

/// A worker that says in `inspect` that its window is full gets nothing, on
/// the member's own machine too: there is no place kept for its own runs.
/// With nowhere else to go the task waits, says why, and does not fail; it
/// is placed when the window has room again.
#[tokio::test]
async fn a52_a_task_with_nowhere_to_go_waits_for_capacity_and_is_placed_when_there_is_room() {
    let f = fx();
    let local = f.worker("w-local", (2, 1));
    let c = f.open(&[&local]);
    c.set_parent_local(local.id.clone());
    local.set(|s| s.foreign = 3);
    let task = enqueue(&c, 1).await.remove(0);
    for _ in 0..3 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.settled.is_empty());
    }
    // Nothing was placed, sent or failed.
    assert!(attempts(&c, &task).is_empty());
    assert_eq!(queued(&c), vec![task.clone()]);
    assert_eq!(local.received(Op::Submit), 0);
    // Why it waits is recorded once, while nothing changes.
    let waiting = events(&c, EVENT_TASK_WAITING);
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0]["task"], task.to_string());
    assert_eq!(waiting[0]["blocked_reason"], "capacity");
    assert_eq!(
        waiting[0]["decision"]["excluded"][0]["worker_id"],
        "w-local"
    );
    assert_eq!(waiting[0]["decision"]["excluded"][0]["why"], "capacity");
    // An estimate says the same and changes nothing, however often (A06).
    for _ in 0..100 {
        let preview = c.estimate_task(task.clone()).await.unwrap();
        let decision = preview.decision.unwrap();
        assert_eq!(decision.winner, None);
        assert_eq!(excluded(&decision), [("w-local", Exclusion::Capacity)]);
    }
    assert!(attempts(&c, &task).is_empty());
    assert_eq!(events(&c, EVENT_TASK_WAITING).len(), 1);
    assert_eq!(local.received(Op::Submit), 0);
    // One of the other members' attempts ends.
    local.set(|s| s.foreign = 2);
    let report = c.tick().await.unwrap();
    let placed = only(&c, &task);
    assert_eq!(report.placed, vec![placed.attempt_id.clone()]);
    assert_eq!(local.holds(), vec![placed.attempt_id.clone()]);
    // The reason says that the forecast it was placed by counts the work of
    // other members, and how much of it.
    let decision = reason(&c, &placed.attempt_id);
    assert_eq!(decision.compared[0].outlook.foreign_demand, 2);
    assert!(
        decision
            .summary()
            .contains("(includes 2 attempts of other members)"),
        "{}",
        decision.summary()
    );
}

/// The window is counted from the other members' attempts and what this
/// member holds there itself: its own limits leave room, the worker's
/// window does not.
#[tokio::test]
async fn a52_the_members_own_tasks_and_the_others_fill_the_window_together() {
    let f = fx();
    let a = f.worker("w-a", (2, 1));
    let c = f.open(&[&a]);
    a.set(|s| s.foreign = 1);
    let tasks = enqueue(&c, 3).await;
    let report = c.tick().await.unwrap();
    assert_eq!(report.placed.len(), 2);
    assert_eq!(a.holds().len(), 2);
    assert_eq!(a.received(Op::Submit), 2, "the third was not sent");
    assert!(attempts(&c, &tasks[2]).is_empty());
    let waiting = events(&c, EVENT_TASK_WAITING);
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0]["task"], tasks[2].to_string());
    assert_eq!(waiting[0]["blocked_reason"], "capacity");
    // With nobody else there, the same three fill the member's own limit:
    // that is `full`, and no reason of the group.
    let g = fx();
    let alone = g.worker("w-a", (2, 1));
    let d = g.open(&[&alone]);
    let tasks = enqueue(&d, 4).await;
    assert_eq!(d.tick().await.unwrap().placed.len(), 3);
    let waiting = events(&d, EVENT_TASK_WAITING);
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0]["task"], tasks[3].to_string());
    assert_eq!(waiting[0]["blocked_reason"], serde_json::Value::Null);
    assert_eq!(waiting[0]["decision"]["excluded"][0]["why"], "full");
}

/// An in-place task is never placed again by itself, so a full window is
/// waited out like any other `OVERLOADED`: the same attempt is sent again
/// until it is taken, and is neither closed, retired nor failed.
#[tokio::test]
async fn a52_an_in_place_task_refused_for_capacity_waits_and_is_neither_moved_nor_failed() {
    let f = fx();
    let local = f.worker("z-local", (1, 0));
    let c = f.open(&[&local]);
    c.set_parent_local(local.id.clone());
    local.set(|s| s.fault = Some(full()));
    let task = TaskId::generate();
    let mut input = spec();
    input.in_place = true;
    c.enqueue(RunId::generate(), source(&c), vec![(task.clone(), input)])
        .await
        .unwrap();
    for sent in 1..=4 {
        let report = c.tick().await.unwrap();
        assert!(report.settled.is_empty() && report.retired.is_empty());
        assert_eq!(local.received(Op::Submit), sent);
    }
    let first = only(&c, &task);
    assert!(!first.retired && first.termination.is_none());
    assert_eq!(c.store().retire_tombstone(&first.attempt_id).unwrap(), None);
    assert_eq!(local.received(Op::Retire), 0);
    assert!(noted(&c, &first.attempt_id, EVENT_SUBMIT_REFUSED_CAPACITY).is_empty());
    assert_eq!(noted(&c, &first.attempt_id, "submit_deferred").len(), 1);
    local.set(|s| s.fault = None);
    c.tick().await.unwrap();
    assert_eq!(local.holds(), vec![first.attempt_id.clone()]);
    assert_eq!(attempts(&c, &task).len(), 1);
}

/// The same rule for an attempt id another member holds (spec 11): sent
/// once and answered, the attempt is closed without a retire; with a send
/// that was never answered before it, it is retired first.
#[tokio::test]
async fn an_attempt_id_in_use_is_closed_when_sent_once_and_retired_when_the_send_is_uncertain() {
    let in_use = || ApiError::overloaded(OverloadedReason::AttemptIdInUse, "in use");
    // Sent once.
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    a.set(|s| s.fault = Some(in_use()));
    let task = enqueue(&c, 1).await.remove(0);
    let report = c.tick().await.unwrap();
    let first = only(&c, &task);
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    assert!(first.retired && first.termination.is_none());
    assert_eq!(a.received(Op::Retire), 0);
    let why = noted(&c, &first.attempt_id, EVENT_SUBMIT_REFUSED_ATTEMPT_ID);
    assert_eq!(why.len(), 1);
    assert_eq!(why[0]["retire"], false);
    // Uncertain: the first send was lost.
    let g = fx();
    let w = g.worker("w-a", (1, 0));
    let d = g.open(&[&w]);
    w.set(|s| s.lose.push((Op::Submit, Lost::Request)));
    let task = enqueue(&d, 1).await.remove(0);
    d.tick().await.unwrap();
    w.set(|s| s.fault = Some(in_use()));
    let report = d.tick().await.unwrap();
    let first = only(&d, &task);
    assert!(report.retired.is_empty());
    assert!(!first.retired);
    let (_, confirmed) = d
        .store()
        .retire_tombstone(&first.attempt_id)
        .unwrap()
        .unwrap();
    assert_eq!(confirmed, None);
    let why = noted(&d, &first.attempt_id, EVENT_SUBMIT_REFUSED_ATTEMPT_ID);
    assert_eq!(why[0]["retire"], true);
    w.set(|s| s.fault = None);
    let report = d.tick().await.unwrap();
    assert_eq!(report.retired, vec![first.attempt_id.clone()]);
    assert_eq!(w.received(Op::Retire), 1);
    let all = attempts(&d, &task);
    assert_eq!(all.len(), 2);
    assert_eq!(w.holds(), vec![all[1].attempt_id.clone()]);
    // The late copy of the first submit: this member's own tombstone.
    let e = w
        .handle(Request::SubmitAttempt(copy_of(&d, &first.attempt_id)))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Retired);
}

/// A cancel that was recorded while the submit was on its way is not lost
/// to the close: the attempt takes the retire path and is settled cancelled.
#[tokio::test]
async fn a_cancel_recorded_before_the_refusal_settles_the_attempt_cancelled() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    // The first send is lost, the cancel is recorded, and the resend the
    // cancel does not prevent (the retire goes first from then on) is not
    // made: the cancel is delivered instead.
    a.set(|s| s.lose.push((Op::Submit, Lost::Request)));
    let task = enqueue(&c, 1).await.remove(0);
    c.tick().await.unwrap();
    let first = only(&c, &task);
    c.cancel_task(&task).await.unwrap();
    a.set(|s| s.fault = Some(full()));
    for _ in 0..3 {
        c.tick().await.unwrap();
    }
    let now = only(&c, &task);
    assert_eq!(now.attempt_id, first.attempt_id);
    assert!(!now.retired);
    assert_eq!(
        now.termination.map(|t| t.outcome()),
        Some(Outcome::Cancelled)
    );
    assert!(noted(&c, &first.attempt_id, EVENT_SUBMIT_REFUSED_CAPACITY).is_empty());
    assert_eq!(attempts(&c, &task).len(), 1);
}

/// A worker that refused for capacity is left out until its `inspect`
/// answers room, also when this member's own count says there is room (the
/// worker holds an attempt of this member that the member no longer
/// counts): the worker is the one that knows. One generation is closed,
/// and no further submit goes out tick after tick.
#[tokio::test]
async fn a52_a_worker_that_refused_for_capacity_gets_nothing_until_it_answers_room() {
    let f = fx();
    let a = f.worker("w-a", (1, 0));
    let c = f.open(&[&a]);
    let tasks = enqueue(&c, 1).await;
    c.tick().await.unwrap();
    let first = only(&c, &tasks[0]);
    // The attempt ends for the member, and the worker still holds another
    // attempt of it that the member does not count.
    let stray = AttemptId::generate();
    a.set(|s| {
        let mut left = s.held[&first.attempt_id].clone();
        left.payload.task_id = TaskId::generate();
        s.held.insert(stray.clone(), left);
        let h = s.held.get_mut(&first.attempt_id).unwrap();
        h.state = AttemptState::Terminal;
        h.termination = Some(ended(Outcome::Succeeded));
    });
    let report = c.tick().await.unwrap();
    assert_eq!(report.settled, vec![first.attempt_id.clone()]);
    assert_eq!(a.received(Op::Submit), 1);
    // By the member's count the worker is free and nobody else is there, so
    // the next task is placed: refused, sent once, closed.
    let task = enqueue(&c, 1).await.remove(0);
    let report = c.tick().await.unwrap();
    let refused = only(&c, &task);
    assert_eq!(report.retired, vec![refused.attempt_id.clone()]);
    assert_eq!(a.received(Op::Submit), 2);
    assert_eq!(
        noted(&c, &refused.attempt_id, EVENT_SUBMIT_REFUSED_CAPACITY).len(),
        1
    );
    // From here on nothing is placed or sent there, however often it is
    // looked at, and an estimate says the same and changes nothing.
    for _ in 0..5 {
        let report = c.tick().await.unwrap();
        assert!(report.placed.is_empty() && report.retired.is_empty());
        let preview = c.estimate_task(task.clone()).await.unwrap();
        assert_eq!(
            excluded(&preview.decision.unwrap()),
            [("w-a", Exclusion::Capacity)]
        );
    }
    assert_eq!(a.received(Op::Submit), 2);
    assert_eq!(attempts(&c, &task).len(), 1);
    assert_eq!(queued(&c), vec![task.clone()]);
    let waiting = events(&c, EVENT_TASK_WAITING);
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0]["blocked_reason"], "capacity");
    let before_events = c.store().events_after(0, 10_000).unwrap();
    let before_attempts = attempts(&c, &task);
    for _ in 0..100 {
        let preview = c.estimate_task(task.clone()).await.unwrap();
        assert_eq!(
            excluded(&preview.decision.unwrap()),
            [("w-a", Exclusion::Capacity)]
        );
    }
    assert_eq!(before_events, c.store().events_after(0, 10_000).unwrap());
    assert_eq!(before_attempts, attempts(&c, &task));
    assert_eq!(queued(&c), vec![task.clone()]);
    let report = c.tick().await.unwrap();
    assert!(report.placed.is_empty());
    assert_eq!(a.received(Op::Submit), 2);
    // The worker answers room: it is a candidate again, and the task goes
    // there as its next generation.
    a.set(|s| {
        s.held.remove(&stray);
    });
    let report = c.tick().await.unwrap();
    let all = attempts(&c, &task);
    assert_eq!(all.len(), 2);
    assert_eq!(report.placed, vec![all[1].attempt_id.clone()]);
    assert_eq!(all[1].generation, Generation(2));
    assert!(a.holds().contains(&all[1].attempt_id));
    assert_eq!(a.received(Op::Submit), 3);
}

// ------------------------------------------- two members, one real worker

/// A member's own connection to a real agent, under its own name.
async fn member_link(
    cluster: &ClusterId,
    c: &Coordinator,
    name: &str,
    agent: &Arc<Agent>,
) -> Arc<dyn WorkerLink> {
    let (near, far) = belt::protocol::transport::duplex_pair(64 * 1024);
    tokio::spawn(belt::worker::serve::serve_connection(
        agent.clone(),
        far,
        cluster.clone(),
        Arc::new(tokio::sync::Semaphore::new(64)),
    ));
    let membership = belt::protocol::Membership::load(
        WorkerId::parse(name).unwrap(),
        &agent.config().group_key_path,
    )
    .unwrap();
    let cfg = belt::protocol::ClientConfig::new(
        cluster.clone(),
        c.store().coordinator_id().clone(),
        c.epoch(),
        membership,
    );
    let client = belt::protocol::connect(near, cfg).await.unwrap().client;
    Arc::new(belt::coordinator::ClientLink::new(
        client,
        Duration::from_secs(30),
    ))
}

/// A task whose command writes a line to `marker` when it starts, and stays.
async fn staying_task(c: &Coordinator, marker: &std::path::Path) -> TaskId {
    let mut task_spec = spec();
    let script = format!("echo start >> {}; exec sleep 300", marker.display());
    task_spec.commands[0].argv = vec!["/bin/sh".into(), "-c".into(), script];
    task_spec.commands[0]
        .env
        .insert("PATH".into(), "/usr/bin:/bin".into());
    let task = TaskId::generate();
    c.enqueue(
        RunId::generate(),
        source(c),
        vec![(task.clone(), task_spec)],
    )
    .await
    .unwrap();
    task
}

/// Tick both members until `done`, for at most 30 seconds.
async fn tick_until(members: [&Coordinator; 2], what: &str, done: impl Fn() -> bool) {
    let end = std::time::Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(
            std::time::Instant::now() < end,
            "timed out waiting for {what}"
        );
        for c in members {
            c.tick().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A52 with two real coordinators and one real worker whose window holds
/// one attempt: both place at once, the worker holds one attempt and no
/// trace of the other, the member that lost waits with the reason and does
/// not fail, and each command starts once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a52_two_members_place_on_one_real_worker_at_once_and_its_window_holds() {
    let mut w = worker_agent::fx();
    w.admit_commands(2);
    let dirs = [
        crate::test_tmp::tempdir().unwrap(),
        crate::test_tmp::tempdir().unwrap(),
    ];
    let open = |dir: &TempDir| {
        let store = CoordinatorStore::open(dir.path(), 1).unwrap();
        Coordinator::start(store, CoordinatorConfig::default()).unwrap()
    };
    let (m, n) = (open(&dirs[0]), open(&dirs[1]));
    w.cluster = m.store().cluster_id().clone();
    let agent = w.start_holding("boot-a", 1);
    let id = agent.config().worker_id.clone();
    m.attach(
        id.clone(),
        member_link(&w.cluster, &m, "main", &agent).await,
    )
    .await;
    n.attach(id.clone(), member_link(&w.cluster, &n, "sub", &agent).await)
        .await;
    let markers = [w.path("main-starts"), w.path("sub-starts")];
    let tasks = [
        staying_task(&m, &markers[0]).await,
        staying_task(&n, &markers[1]).await,
    ];
    let (first, second) = tokio::join!(m.tick(), n.tick());
    first.unwrap();
    second.unwrap();

    // The worker holds one attempt, of whoever came first.
    let store = w.store();
    let held = store.list_nonterminal().unwrap();
    assert_eq!(held.len(), 1, "{held:?}");
    let won = usize::from(held[0].coordinator_id != *m.store().coordinator_id());
    let members = [&m, &n];
    let (winner, loser) = (members[won], members[1 - won]);
    let (kept, waiting) = (&tasks[won], &tasks[1 - won]);
    assert_eq!(only(winner, kept).attempt_id, held[0].attempt_id);
    tick_until([winner, loser], "the first command", || {
        count_lines(&markers[won]) == 1
    })
    .await;
    for _ in 0..3 {
        loser.tick().await.unwrap();
    }
    // The other member's task did not fail and is still queued. It was
    // either never placed (the `inspect` already said the window is full)
    // or placed once, sent once, refused and closed without a retire.
    assert_eq!(queued(loser), vec![waiting.clone()]);
    let lost = attempts(loser, waiting);
    assert!(lost.len() <= 1, "{lost:?}");
    let own = store.for_member(loser.store().coordinator_id());
    for a in &lost {
        assert!(a.retired && a.termination.is_none(), "{a:?}");
        let (asked, confirmed) = loser
            .store()
            .retire_tombstone(&a.attempt_id)
            .unwrap()
            .unwrap();
        assert_eq!(confirmed, Some(asked));
        let why = noted(loser, &a.attempt_id, EVENT_SUBMIT_REFUSED_CAPACITY);
        assert_eq!(why.len(), 1);
        assert_eq!(why[0]["retire"], false);
        // Nothing of it is on the worker, no tombstone either.
        assert!(own.get(&a.attempt_id).unwrap().is_none());
        assert!(own.tombstone(&a.attempt_id).unwrap().is_none());
    }
    assert_eq!(own.count_undelivered_tombstones().unwrap(), 0);
    assert_eq!(store.list_nonterminal().unwrap().len(), 1);
    let waits = events(loser, EVENT_TASK_WAITING);
    let last = waits.last().expect("why the task waits is recorded");
    assert_eq!(last["task"], waiting.to_string());
    assert_eq!(last["blocked_reason"], "capacity");
    assert_eq!(count_lines(&markers[1 - won]), 0);
    assert!(events(winner, EVENT_TASK_WAITING).is_empty());

    // The first member's attempt ends: the window has room, and the task
    // that waited is placed and started, once.
    winner.cancel_task(kept).await.unwrap();
    tick_until([winner, loser], "the second command", || {
        count_lines(&markers[1 - won]) == 1
    })
    .await;
    let placed = attempts(loser, waiting).pop().unwrap();
    assert!(!placed.retired);
    assert_eq!(placed.worker_id, id);
    assert_eq!(
        only(winner, kept).termination.map(|t| t.outcome()),
        Some(Outcome::Cancelled)
    );
    assert_eq!(count_lines(&markers[won]), 1);
    assert_eq!(count_lines(&markers[1 - won]), 1);
    loser.cancel_task(waiting).await.unwrap();
    tick_until([winner, loser], "the second command to stop", || {
        attempts(loser, waiting)
            .pop()
            .is_some_and(|a| a.state == AttemptState::Terminal)
    })
    .await;
}
