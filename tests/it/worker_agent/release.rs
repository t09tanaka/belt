//! Stage 3 of the retention on a running agent (spec 25.6): what a
//! release deletes, what a peer without `attempt_release_v1` keeps, the
//! start that converts the rows an older version acknowledged, and a member
//! forgotten for good.

use super::*;
use belt::error::GroupRefusal;
use belt::protocol::ConnectError;
use belt::wire::{ListUnreleasedRequest, ReleaseAttemptRequest, ATTEMPT_RELEASE_FEATURE};

/// A connection of a parent without `attempt_release_v1`.
async fn connect_old(f: &Fx, agent: &Arc<Agent>) -> Client {
    let (a, b) = duplex_pair(64 * 1024);
    tokio::spawn(serve_connection(
        agent.clone(),
        b,
        f.cluster.clone(),
        Arc::new(Semaphore::new(64)),
    ));
    let mut cfg = client_cfg(&f.cluster);
    cfg.features.retain(|v| v != ATTEMPT_RELEASE_FEATURE);
    connect(a, cfg).await.unwrap().client
}

fn release_req(a: &AttemptId) -> Request {
    Request::ReleaseAttempt(ReleaseAttemptRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: a.clone(),
    })
}

fn unreleased_req() -> Request {
    Request::ListUnreleased(ListUnreleasedRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        after: None,
        limit: 500,
    })
}

/// The dedup record of the member the fixture's parent is.
fn recorded(f: &Fx, a: &AttemptId) -> Option<belt::storage::DedupRecord> {
    let (_, member) = f.store().members().unwrap().remove(0);
    let member = belt::ids::CoordinatorId::parse(&member).unwrap();
    f.store().for_member(&member).attempt_record(a).unwrap()
}

fn lock_of(f: &Fx, a: &AttemptId) -> PathBuf {
    f.state().join("supervisors").join(format!("{a}.lock"))
}

async fn digest_of(c: &Client, a: &AttemptId) -> Digest {
    let Response::GetResult(r) = call(
        c,
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: a.clone(),
            generation: Generation(1),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    r.result_digest
}

fn ack_req(a: &AttemptId, digest: Digest) -> Request {
    Request::AckCollected(AckCollectedRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: a.clone(),
        generation: Generation(1),
        result_digest: digest,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_deletes_a_terminal_attempt_and_a_parent_without_the_feature_sees_todays_worker()
{
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("echo x")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    wait_for("the supervisor exits", || f.supervisors(&a).is_empty());
    assert!(f.state().join("attempts").join(a.as_str()).exists());
    let Response::ListUnreleased(page) = call(&c, unreleased_req()).await.unwrap() else {
        panic!()
    };
    assert_eq!(page.attempts, vec![a.clone()]);
    let Response::ReleaseAttempt(r) = call(&c, release_req(&a)).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.attempt_id, a);
    assert!(!f.state().join("attempts").join(a.as_str()).exists());
    assert!(
        !lock_of(&f, &a).exists(),
        "the lock goes after the directory"
    );
    assert!(f.store().get(&a).unwrap().is_none());
    assert!(f.store().dropped_attempts(10).unwrap().is_empty());
    // Released and nothing in flight: the record went too; again is NOT_FOUND.
    assert_eq!(
        call(&c, release_req(&a)).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    let Response::ListUnreleased(page) = call(&c, unreleased_req()).await.unwrap() else {
        panic!()
    };
    assert!(page.attempts.is_empty());

    // A parent that did not agree the feature is sent neither operation's
    // answer, and its acknowledgement keeps the row as before.
    let old = connect_old(&f, &agent).await;
    for r in [release_req(&a), unreleased_req()] {
        assert_eq!(
            call(&old, r).await.unwrap_err().code,
            ErrorCode::ProtocolIncompatible
        );
    }
    let b = AttemptId::generate();
    submit(&old, &b, &payload("echo y")).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
    let digest = digest_of(&old, &b).await;
    call(&old, ack_req(&b, digest)).await.unwrap();
    assert!(f.rec(&b).acked_at_ms.is_some());
    // A tick does not take a row this agent's life acknowledged.
    agent.tick();
    assert!(f.store().get(&b).unwrap().is_some());

    // A live attempt is not released.
    let live = AttemptId::generate();
    submit(&c, &live, &payload("sleep 30")).await;
    assert_eq!(
        call(&c, release_req(&live)).await.unwrap_err().code,
        ErrorCode::Overloaded
    );
    assert!(f.store().get(&live).unwrap().is_some());
    agent.shutdown();

    // (xv) The next start deletes the row the old parent acknowledged
    // before it accepts a connection, with no record.
    let agent = f.start("boot-a");
    assert!(f.store().get(&b).unwrap().is_none());
    assert!(!f.state().join("attempts").join(b.as_str()).exists());
    assert!(!lock_of(&f, &b).exists());
    let c = f.connect(&agent).await;
    let Response::ListUnreleased(page) = call(&c, unreleased_req()).await.unwrap() else {
        panic!()
    };
    assert!(!page.attempts.contains(&b));
    agent.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forgotten_member_is_refused_at_its_proof() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("echo x")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    let (name, _) = f.store().members().unwrap().remove(0);
    let attempts = WorkerStore::open(&f.state(), &f.worker)
        .unwrap()
        .forget_member(&name, 1)
        .unwrap();
    assert_eq!(attempts, vec![a.to_string()]);
    // The connection that stayed open has nothing accepted any more: with
    // its records gone, a resend would otherwise run again.
    let e = call(&c, submit_req(&a, &payload("echo x")))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidInput, "{}", e.message);
    assert!(f.store().get(&a).unwrap().is_none());
    let (x, y) = duplex_pair(64 * 1024);
    tokio::spawn(serve_connection(
        agent.clone(),
        y,
        f.cluster.clone(),
        Arc::new(Semaphore::new(64)),
    ));
    match connect(x, client_cfg(&f.cluster)).await {
        Err(ConnectError::Refused(e)) => {
            assert_eq!(e.group_refusal, Some(GroupRefusal::MemberForgotten))
        }
        Err(e) => panic!("refused otherwise: {e}"),
        Ok(_) => panic!("a forgotten member was let in"),
    }
    agent.shutdown();
}

// (viii) Past `ack_wait` an attempt has no row and an unreleased record: a
// restart keeps that record, a late submit of it does not run, and only a
// release frees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_the_ack_wait_a_restart_keeps_the_unreleased_record_until_a_release() {
    let f = fx();
    let mut cfg = f.cfg("boot-a");
    cfg.ack_wait_ms = 0;
    let agent = f.start_with(cfg);
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    let marker = f.path("ran-again");
    submit(&c, &a, &payload("echo x")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    wait_for("the supervisor exits", || f.supervisors(&a).is_empty());
    wait_for("the ack wait turns the row into a record", || {
        agent.tick();
        f.store().get(&a).unwrap().is_none()
    });
    agent.shutdown();

    let agent = f.start("boot-b");
    let c = f.connect(&agent).await;
    let Response::ListUnreleased(page) = call(&c, unreleased_req()).await.unwrap() else {
        panic!()
    };
    assert_eq!(page.attempts, vec![a.clone()], "the restart kept it");
    let late = payload(&format!("touch {}", marker.display()));
    let first = call(&c, submit_req(&a, &late)).await.unwrap_err().code;
    assert!(f.store().get(&a).unwrap().is_none());
    assert!(!marker.exists(), "a late submit ran");
    // A late copy is answered by its record, not by the checks of a new
    // acceptance: RETIRED even while the worker drains.
    belt::worker::control::write(&f.state(), belt::worker::control::Control::draining()).unwrap();
    let e = call(&c, submit_req(&a, &late)).await.unwrap_err();
    assert_eq!(e.code, first, "{e:?}");
    assert_ne!(e.code, ErrorCode::Overloaded);
    belt::worker::control::write(&f.state(), belt::worker::control::Control::default()).unwrap();
    call(&c, release_req(&a)).await.unwrap();
    let Response::ListUnreleased(page) = call(&c, unreleased_req()).await.unwrap() else {
        panic!()
    };
    assert!(page.attempts.is_empty());
    agent.shutdown();
}

// (vii) A connection of the member that was open at the release still reads
// a copy of the submit after it: the late submit does not run, and the
// record stays until that connection is done, however the release came.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_open_at_the_release_keeps_the_record_until_it_is_done() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    let marker = f.path("ran-again");
    submit(&c, &a, &payload("echo x")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    wait_for("the supervisor exits", || f.supervisors(&a).is_empty());

    // A second connection of the same member; no permit is free on it, so
    // the late submit it takes waits before it is handled.
    let permits = Arc::new(Semaphore::new(0));
    let (x, y) = duplex_pair(64 * 1024);
    tokio::spawn(serve_connection(
        agent.clone(),
        y,
        f.cluster.clone(),
        permits.clone(),
    ));
    let d = Arc::new(connect(x, client_cfg(&f.cluster)).await.unwrap().client);
    let late = payload(&format!("touch {}", marker.display()));
    let waiting = tokio::spawn({
        let d = d.clone();
        let request = submit_req(&a, &late);
        async move { call(&d, request).await }
    });

    call(&c, release_req(&a)).await.unwrap();
    assert!(f.store().get(&a).unwrap().is_none());
    assert!(
        recorded(&f, &a).is_some(),
        "the other connection may still read a copy"
    );
    permits.add_permits(1);
    let _ = waiting.await.unwrap();
    assert!(!marker.exists(), "a late submit ran");
    assert!(f.store().get(&a).unwrap().is_none());
    assert!(recorded(&f, &a).is_some(), "the connection is still open");
    drop(d);
    wait_for("the record goes with the connection", || {
        recorded(&f, &a).is_none()
    });
    assert!(!marker.exists());
    agent.shutdown();
}

// (xiii) The checks of the agent do not make the lock of a released attempt
// again, in a tick or at the next start, and the directory is gone with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_agent_never_makes_the_lock_of_a_released_attempt_again() {
    let f = fx();
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("echo x")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    wait_for("the supervisor exits", || f.supervisors(&a).is_empty());
    call(&c, release_req(&a)).await.unwrap();
    for _ in 0..3 {
        agent.tick();
    }
    assert!(!lock_of(&f, &a).exists());
    assert!(!f.state().join("attempts").join(a.as_str()).exists());
    agent.shutdown();

    let agent = f.start("boot-b");
    agent.tick();
    assert!(!lock_of(&f, &a).exists());
    assert!(!f.state().join("attempts").join(a.as_str()).exists());
    agent.shutdown();
}

// (xv) A start on trial deletes nothing the older version acknowledged (a
// rollback restores the database, not the directories); the next start that
// is not on trial does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_on_trial_keeps_the_rows_an_older_version_acknowledged() {
    let f = fx();
    let agent = f.start("boot-a");
    let old = connect_old(&f, &agent).await;
    let b = AttemptId::generate();
    submit(&old, &b, &payload("echo y")).await;
    assert_eq!(f.wait_terminal(&b), Outcome::Succeeded);
    wait_for("the supervisor exits", || f.supervisors(&b).is_empty());
    let digest = digest_of(&old, &b).await;
    call(&old, ack_req(&b, digest)).await.unwrap();
    agent.shutdown();

    belt::worker::control::begin_trial(&f.state()).unwrap();
    let agent = f.start("boot-b");
    agent.tick();
    assert!(f.store().get(&b).unwrap().is_some());
    assert!(f.state().join("attempts").join(b.as_str()).exists());
    agent.shutdown();

    belt::worker::control::end_trial(&f.state()).unwrap();
    let agent = f.start("boot-c");
    assert!(f.store().get(&b).unwrap().is_none());
    assert!(!f.state().join("attempts").join(b.as_str()).exists());
    assert!(recorded(&f, &b).is_none());
    agent.shutdown();
}

// A release whose teardown failed (a busy workspace) keeps the directory and
// the `dropped_attempt` entry; a later tick finishes it. No event of the
// attempt is written after its row went.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_whose_cleanup_failed_is_finished_by_a_later_tick() {
    let f = fx();
    f.host.fail_cleanup.store(true, Ordering::SeqCst);
    let agent = f.start("boot-a");
    let c = f.connect(&agent).await;
    let a = AttemptId::generate();
    submit(&c, &a, &payload("echo x")).await;
    assert_eq!(f.wait_terminal(&a), Outcome::Succeeded);
    wait_for("the supervisor exits", || f.supervisors(&a).is_empty());
    call(&c, release_req(&a)).await.unwrap();
    let dir = f.state().join("attempts").join(a.as_str());
    assert!(f.store().get(&a).unwrap().is_none());
    assert!(dir.join("cleanup-pending").exists(), "the teardown is kept");
    assert_eq!(f.store().dropped_attempts(10).unwrap(), vec![a.to_string()]);
    agent.tick();
    assert!(dir.exists());
    assert!(f.store().events_of_attempt(&a, 10).unwrap().is_empty());
    f.host.fail_cleanup.store(false, Ordering::SeqCst);
    agent.tick();
    assert!(!dir.exists());
    assert!(!lock_of(&f, &a).exists());
    assert!(f.store().dropped_attempts(10).unwrap().is_empty());
    agent.shutdown();
}
