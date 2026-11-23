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
    let attempts = f.store().forget_member(&name, 1).unwrap();
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
