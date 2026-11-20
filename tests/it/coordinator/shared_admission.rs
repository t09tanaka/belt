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
