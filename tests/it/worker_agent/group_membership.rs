//! The group on a real agent (spec chapter 24): who is let in (A53: the
//! group, the key, the fixed name), what a member can reach (24.4), the
//! notice of a retired identity (24.3), and the switch of the key.

use super::*;
use belt::error::GroupRefusal;
use belt::group;
use belt::protocol::frame::{Body, Frame, MAX_FRAME_LEN};
use belt::protocol::io::{read_frame, write_frame};
use belt::protocol::{ClientConfig, ConnectError, Connected, Membership};
use belt::secrets::key::ClusterKey;

fn other_key() -> ClusterKey {
    ClusterKey::from_bytes(vec![9; 32]).unwrap()
}

/// A member of the group: its coordinator id, its name, its epoch, its key.
#[derive(Clone)]
struct Member {
    coordinator: CoordinatorId,
    name: &'static str,
    epoch: u64,
    key: ClusterKey,
}

fn member(name: &'static str) -> Member {
    Member {
        coordinator: CoordinatorId::generate(),
        name,
        epoch: 1,
        key: proto_common::group_key(),
    }
}

impl Member {
    fn cfg(&self, cluster: &ClusterId) -> ClientConfig {
        ClientConfig::new(
            cluster.clone(),
            self.coordinator.clone(),
            CoordinatorEpoch(self.epoch),
            Membership {
                member: WorkerId::parse(self.name).unwrap(),
                key: self.key.clone(),
            },
        )
    }
}

async fn dial_as(f: &Fx, agent: &Arc<Agent>, cfg: ClientConfig) -> Result<Connected, ConnectError> {
    let (a, b) = duplex_pair(64 * 1024);
    tokio::spawn(serve_connection(
        agent.clone(),
        b,
        f.cluster.clone(),
        Arc::new(Semaphore::new(64)),
    ));
    connect(a, cfg).await
}

async fn dial(f: &Fx, agent: &Arc<Agent>, m: &Member) -> Result<Connected, ConnectError> {
    dial_as(f, agent, m.cfg(&f.cluster)).await
}

fn refusal(r: Result<Connected, ConnectError>) -> GroupRefusal {
    match r {
        Err(ConnectError::Refused(e)) => {
            assert_eq!(e.code, ErrorCode::ProtocolIncompatible, "{}", e.message);
            e.group_refusal.expect("a reason")
        }
        Err(e) => panic!("refused otherwise: {e}"),
        Ok(_) => panic!("the peer was let in"),
    }
}

fn request_of(m: &Member, a: &AttemptId, p: &AttemptPayload) -> Request {
    Request::SubmitAttempt(
        SubmitAttemptRequest::new(
            RequestId::generate(),
            CoordinatorEpoch(m.epoch),
            a.clone(),
            p.clone(),
        )
        .unwrap(),
    )
}

/// A command that says it started and stays until it is released.
fn until_released(f: &Fx, tag: &str) -> (String, PathBuf, PathBuf) {
    let (started, release) = (
        f.path(&format!("{tag}-started")),
        f.path(&format!("{tag}-release")),
    );
    let script = format!(
        "touch {}; while [ ! -e {} ]; do sleep 0.1; done",
        started.display(),
        release.display()
    );
    (script, started, release)
}

fn get_req(a: &AttemptId) -> Request {
    Request::GetAttempt(GetAttemptRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: a.clone(),
    })
}

async fn status_of(c: &Client, a: &AttemptId) -> AttemptStatus {
    match call(c, get_req(a)).await.unwrap() {
        Response::GetAttempt(r) => r,
        other => panic!("{other:?}"),
    }
}

async fn code_of(c: &Client, r: Request) -> ErrorCode {
    call(c, r).await.unwrap_err().code
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_of_another_group_or_with_another_key_is_refused_and_nothing_is_recorded() {
    let f = fx();
    let agent = f.start("boot-1");
    // Another group, with the right key.
    let stranger = member("main");
    let elsewhere = stranger.cfg(&ClusterId::generate());
    assert_eq!(
        refusal(dial_as(&f, &agent, elsewhere).await),
        GroupRefusal::GroupMismatch
    );
    // The right group, with another key. The refusal shows the fingerprint
    // of the key this worker signs with, and never the key.
    let mut wrong = member("main");
    wrong.key = other_key();
    assert_eq!(
        refusal(dial(&f, &agent, &wrong).await),
        GroupRefusal::GroupKeyMismatch {
            fingerprint: group::fingerprint(&proto_common::group_key())
        }
    );
    // Neither left anything: no member recorded, no epoch, no attempt.
    let store = f.store();
    assert!(store.members().unwrap().is_empty());
    assert!(store.list_nonterminal().unwrap().is_empty());
    for m in [&stranger, &wrong] {
        assert_eq!(
            store
                .for_member(&m.coordinator)
                .max_coordinator_epoch()
                .unwrap(),
            CoordinatorEpoch(0)
        );
    }
    // A member is let in, is told who answers, and is recorded.
    let main = member("main");
    let connected = dial(&f, &agent, &main).await.unwrap();
    assert_eq!(connected.worker.worker_id, f.worker);
    assert_eq!(connected.cluster_id, f.cluster);
    assert_eq!(
        f.store().members().unwrap(),
        vec![("main".to_string(), main.coordinator.to_string())]
    );
    // The single key of before the group is generation 1.
    let (current, next) = f.store().group_keys().unwrap();
    let current = current.unwrap();
    assert_eq!(current.generation, 1);
    assert_eq!(
        current.fingerprint,
        group::fingerprint(&proto_common::group_key())
    );
    assert!(next.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn before_the_proof_the_worker_says_nothing_about_itself_and_serves_nothing() {
    let f = fx();
    let agent = f.start("boot-1");
    let hello = |cluster: &ClusterId| HelloRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        cluster_id: cluster.clone(),
        coordinator_id: CoordinatorId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        protocol_versions: vec![1],
        features: belt::protocol::session::offered_features(),
    };
    // Whatever the group in `hello`, the answer is the same kind of thing:
    // version, features, a nonce. No identity, epoch, profile or capability.
    for cluster in [f.cluster.clone(), ClusterId::generate()] {
        let (mut raw, b) = duplex_pair(64 * 1024);
        tokio::spawn(serve_connection(
            agent.clone(),
            b,
            f.cluster.clone(),
            Arc::new(Semaphore::new(4)),
        ));
        let h = hello(&cluster);
        let frame = Frame::message(0, Message::Request(Request::Hello(h)));
        write_frame(&mut raw.writer, &frame, MAX_FRAME_LEN)
            .await
            .unwrap();
        let answer = read_frame(&mut raw.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .unwrap();
        let Body::Message(message) = answer.body else {
            panic!("a message")
        };
        let said = serde_json::to_value(&message).unwrap();
        let text = said.to_string();
        assert!(matches!(
            message,
            Message::Response {
                response: Response::Hello(_),
                ..
            }
        ));
        for secret in [
            f.worker.as_str(),
            "boot-1",
            "worker_id",
            "worker_boot_id",
            "agent_epoch",
            "coordinator_epoch",
            "profile",
            "\"capabilities\"",
            "cluster_id",
        ] {
            assert!(!text.contains(secret), "{secret} in {text}");
        }
        // Anything but the proof is refused, and the connection is over.
        let inspect = Request::Inspect(InspectRequest {
            preparation_observations: None,
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
        });
        let frame = Frame::message(1, Message::Request(inspect));
        write_frame(&mut raw.writer, &frame, MAX_FRAME_LEN)
            .await
            .unwrap();
        let refused = read_frame(&mut raw.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .unwrap();
        let Body::Message(Message::Error(e)) = refused.body else {
            panic!("an error")
        };
        assert_eq!(e.error.code, ErrorCode::InvalidInput);
        assert!(!e.error.message.contains(f.worker.as_str()));
        assert!(read_frame(&mut raw.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .is_none());
    }
    assert!(f.store().members().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_is_sent_to_a_worker_that_does_not_prove_the_key() {
    // A worker that answers `hello` like any other and then answers the
    // proof with one made under another key.
    let cluster = ClusterId::generate();
    let (near, mut raw) = duplex_pair(64 * 1024);
    let impostor = tokio::spawn(async move {
        let first = read_frame(&mut raw.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .unwrap();
        let Body::Message(Message::Request(Request::Hello(h))) = first.body else {
            panic!("hello")
        };
        let negotiated = belt::protocol::hello::Negotiated {
            version: 1,
            features: h.features.clone(),
        };
        let answer = belt::protocol::hello::hello_response(&negotiated, group::nonce().unwrap());
        let frame = Frame::message(
            0,
            Message::Response {
                request_id: h.request_id,
                response: Response::Hello(answer),
            },
        );
        write_frame(&mut raw.writer, &frame, MAX_FRAME_LEN)
            .await
            .unwrap();
        let second = read_frame(&mut raw.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .unwrap();
        let Body::Message(Message::Request(Request::GroupProof(p))) = second.body else {
            panic!("the proof")
        };
        let me = proto_common::identity("w1");
        let forged = group::receiver_proof(&other_key(), &me.worker_id, &p.nonce);
        let frame = Frame::message(
            0,
            Message::Response {
                request_id: p.request_id,
                response: Response::GroupProof(me.response(&negotiated, forged)),
            },
        );
        write_frame(&mut raw.writer, &frame, MAX_FRAME_LEN)
            .await
            .unwrap();
        // What the connecting side sends after that: nothing at all.
        let mut later = 0;
        while let Ok(Some(_)) = read_frame(&mut raw.reader, MAX_FRAME_LEN).await {
            later += 1;
        }
        later
    });
    let e = match connect(near, member("main").cfg(&cluster)).await {
        Err(ConnectError::Incompatible(e)) => e,
        Err(e) => panic!("{e}"),
        Ok(_) => panic!("a client was made for a worker that proved nothing"),
    };
    assert!(matches!(
        e.group_refusal,
        Some(GroupRefusal::GroupKeyMismatch { .. })
    ));
    assert_eq!(impostor.await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_members_name_and_coordinator_id_are_fixed_together() {
    let f = fx();
    let agent = f.start("boot-1");
    let main = member("main");
    dial(&f, &agent, &main).await.unwrap();
    // A machine that was rebuilt (another coordinator id) does not take
    // over the name, and a copy does not join under another name.
    let mut rebuilt = member("main");
    rebuilt.epoch = 4;
    assert_eq!(
        refusal(dial(&f, &agent, &rebuilt).await),
        GroupRefusal::MemberIdentityMismatch
    );
    let mut copy = main.clone();
    copy.name = "sub";
    assert_eq!(
        refusal(dial(&f, &agent, &copy).await),
        GroupRefusal::MemberIdentityMismatch
    );
    // The refusals recorded nothing, not even an epoch.
    assert_eq!(
        f.store().members().unwrap(),
        vec![("main".to_string(), main.coordinator.to_string())]
    );
    assert_eq!(
        f.store()
            .for_member(&rebuilt.coordinator)
            .max_coordinator_epoch()
            .unwrap(),
        CoordinatorEpoch(0)
    );
    // The recorded pair is let in again, and another member under its own name.
    dial(&f, &agent, &main).await.unwrap();
    dial(&f, &agent, &member("sub")).await.unwrap();
    assert_eq!(f.store().members().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_reaches_its_own_attempts_and_another_members_are_not_found() {
    let f = fx();
    // Two members run a command each at once; the default budget of a small
    // machine admits one, and the second would wait for the first.
    f.admit_commands(2);
    let agent = f.start("boot-1");
    let (main, mut sub) = (member("main"), member("sub"));
    // The other member is on a much older epoch than the first: an epoch is
    // each member's own, so that makes nothing of it stale.
    let mut main = main;
    main.epoch = 9;
    sub.epoch = 2;
    let a = dial(&f, &agent, &main).await.unwrap().client;
    let b = dial(&f, &agent, &sub).await.unwrap().client;
    let (script, started, release) = until_released(&f, "main");
    let x = AttemptId::generate();
    let first = payload(&script);
    call(&a, request_of(&main, &x, &first)).await.unwrap();
    wait_for("the command to start", || started.exists());

    // By every way of naming it, the other member's attempt is not there.
    assert_eq!(code_of(&b, get_req(&x)).await, ErrorCode::NotFound);
    assert!(listed(&b, None, true).await.is_empty());
    assert_eq!(listed(&a, None, true).await, vec![x.clone()]);
    let result = Request::GetResult(GetResultRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: x.clone(),
        generation: Generation(1),
    });
    assert_eq!(code_of(&b, result).await, ErrorCode::NotFound);
    let watch = Request::WatchEvents(WatchEventsRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        attempt_id: Some(x.clone()),
        after_seq: 0,
        limit: 10,
    });
    assert_eq!(code_of(&b, watch).await, ErrorCode::NotFound);
    let mut renew = renew_req(&x, 1, 60_000);
    if let Request::RenewLease(r) = &mut renew {
        r.coordinator_epoch = CoordinatorEpoch(sub.epoch);
    }
    assert_eq!(code_of(&b, renew).await, ErrorCode::NotFound);

    // Its cancel and its retire are about an attempt of its own that never
    // arrived. The first member's command keeps running.
    let mut cancel = cancel_req(&x);
    if let Request::CancelAttempt(r) = &mut cancel {
        r.coordinator_epoch = CoordinatorEpoch(sub.epoch);
    }
    let Response::CancelAttempt(cancelled) = call(&b, cancel).await.unwrap() else {
        panic!()
    };
    assert!(cancelled.tombstone && cancelled.state.is_none());
    let mut retire = retire_req(&x);
    if let Request::RetireAttempt(r) = &mut retire {
        r.coordinator_epoch = CoordinatorEpoch(sub.epoch);
    }
    call(&b, retire).await.unwrap();
    let mine = status_of(&a, &x).await;
    assert!(
        !mine.retired,
        "another member's retire reached this attempt"
    );
    assert_ne!(mine.state, AttemptState::Terminal);
    let rec = f.store().get(&x).unwrap().unwrap();
    assert_eq!(rec.coordinator_id, main.coordinator);
    assert_eq!(rec.cancel_requested_at_ms, None);

    // Each member's epoch is the highest that member showed.
    let again = dial(&f, &agent, &main).await.unwrap();
    assert_eq!(again.worker.coordinator_epoch, CoordinatorEpoch(9));
    let again = dial(&f, &agent, &sub).await.unwrap();
    assert_eq!(again.worker.coordinator_epoch, CoordinatorEpoch(2));

    // The second member's own work under the same task id is its own.
    let y = AttemptId::generate();
    let mut second = payload("exit 0");
    second.task_id = first.task_id.clone();
    call(&b, request_of(&sub, &y, &second)).await.unwrap();
    wait_for("the second member's attempt to end", || {
        f.store().get(&y).unwrap().unwrap().state == AttemptState::Terminal
    });
    assert_eq!(code_of(&a, get_req(&y)).await, ErrorCode::NotFound);

    std::fs::write(&release, "").unwrap();
    wait_for("the first member's attempt to end", || {
        f.store().get(&x).unwrap().unwrap().state == AttemptState::Terminal
    });
    assert_eq!(
        status_of(&a, &x).await.termination.unwrap().outcome(),
        Outcome::Succeeded
    );
}

/// An attempt id is held by one member per worker (the attempt's directory,
/// its supervisor and its commands are named by the id alone). A second
/// member that submits the same id is refused with `OVERLOADED` and the
/// reason `attempt_id_in_use`: nothing of it is stored, and neither that
/// submit nor the retire that follows it touches the first member's attempt,
/// its directory or its running command.
#[tokio::test(flavor = "multi_thread")]
async fn an_attempt_id_another_member_holds_is_refused_without_touching_that_attempt() {
    let f = fx();
    // Two members run a command each at once; the default budget of a small
    // machine admits one, and the second would wait for the first.
    f.admit_commands(2);
    let agent = f.start("boot-1");
    let (main, sub) = (member("main"), member("sub"));
    let a = dial(&f, &agent, &main).await.unwrap().client;
    let b = dial(&f, &agent, &sub).await.unwrap().client;
    let (script, started, release) = until_released(&f, "held");
    let x = AttemptId::generate();
    let first = payload(&script);
    call(&a, request_of(&main, &x, &first)).await.unwrap();
    wait_for("the command to start", || started.exists());
    let dir = agent.config().attempts_root().join(x.as_str());
    let spec = std::fs::read(dir.join("supervise.json")).unwrap();
    let held = f.store().get(&x).unwrap().unwrap();

    // The same id from the other member, with its own payload and a newer
    // epoch of its own.
    let mut newer = member("sub");
    newer.coordinator = sub.coordinator.clone();
    newer.epoch = sub.epoch + 5;
    let second = payload("exit 0");
    for _ in 0..2 {
        let e = call(&b, request_of(&newer, &x, &second)).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Overloaded);
        assert_eq!(
            e.overloaded_reason,
            Some(belt::error::OverloadedReason::AttemptIdInUse)
        );
        assert!(
            !e.message.contains(held.run_id.as_str()),
            "nothing of the other member's attempt is shown"
        );
    }
    // Nothing was stored for it: no attempt, and not the epoch it carried.
    assert_eq!(code_of(&b, get_req(&x)).await, ErrorCode::NotFound);
    assert!(listed(&b, None, true).await.is_empty());
    let again = dial(&f, &agent, &sub).await.unwrap();
    assert_eq!(
        again.worker.coordinator_epoch,
        CoordinatorEpoch(0),
        "the refused submit left its epoch"
    );
    // It takes the attempt off the worker: only a tombstone of its own.
    let mut retire = retire_req(&x);
    if let Request::RetireAttempt(r) = &mut retire {
        r.coordinator_epoch = CoordinatorEpoch(sub.epoch);
    }
    let Response::RetireAttempt(retired) = call(&b, retire).await.unwrap() else {
        panic!()
    };
    assert!(retired.tombstone);
    assert_eq!(retired.status, RetireStatus::NotStarted);
    assert_eq!(
        code_of(&b, request_of(&sub, &x, &second)).await,
        ErrorCode::Retired
    );
    // The first member's attempt: the same row, the same files, and the
    // command still runs.
    let now = f.store().get(&x).unwrap().unwrap();
    assert_eq!(now.coordinator_id, main.coordinator);
    assert_eq!(now.payload_hash, held.payload_hash);
    assert!(!now.retired && now.cancel_requested_at_ms.is_none());
    assert_ne!(now.state, AttemptState::Terminal);
    assert_eq!(std::fs::read(dir.join("supervise.json")).unwrap(), spec);
    assert!(!status_of(&a, &x).await.retired);
    // The second member goes on under a new attempt id.
    let y = AttemptId::generate();
    call(&b, request_of(&sub, &y, &second)).await.unwrap();
    wait_for("the second member's attempt to end", || {
        f.store().get(&y).unwrap().unwrap().state == AttemptState::Terminal
    });
    std::fs::write(&release, "").unwrap();
    wait_for("the first member's attempt to end", || {
        f.store().get(&x).unwrap().unwrap().state == AttemptState::Terminal
    });
    assert_eq!(
        status_of(&a, &x).await.termination.unwrap().outcome(),
        Outcome::Succeeded
    );
}

fn retired_req(m: &Member, worker: &WorkerId, boot: &str) -> Request {
    Request::IdentityRetired(IdentityRetiredRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_id: m.coordinator.clone(),
        worker_id: worker.clone(),
        worker_boot_id: WorkerBootId::parse(boot).unwrap(),
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retired_identity_stops_that_members_attempts_now_and_refuses_it_from_then_on() {
    let f = fx();
    f.admit_commands(2);
    let agent = f.start("boot-1");
    let (main, sub) = (member("main"), member("sub"));
    let a = dial(&f, &agent, &main).await.unwrap().client;
    let b = dial(&f, &agent, &sub).await.unwrap().client;
    // A lease far longer than the test: what ends the attempt is the notice.
    let long = |script: &str| {
        let mut p = payload(script);
        p.lease_ms = 600_000;
        p
    };
    let (script, started, _release) = until_released(&f, "main");
    let (mine, theirs) = (AttemptId::generate(), AttemptId::generate());
    call(&a, request_of(&main, &mine, &long(&script)))
        .await
        .unwrap();
    let other_started = f.path("other-started");
    let other_script = format!("touch {}; sleep 600", other_started.display());
    call(&b, request_of(&sub, &theirs, &long(&other_script)))
        .await
        .unwrap();
    wait_for("both commands to start", || {
        started.exists() && other_started.exists()
    });

    // A notice that names another worker, another identity, or another
    // member than the one that sends it changes nothing.
    let elsewhere = WorkerId::parse("w-other").unwrap();
    for wrong in [
        retired_req(&main, &elsewhere, "boot-1"),
        retired_req(&main, &f.worker, "boot-0"),
        retired_req(&sub, &f.worker, "boot-1"),
    ] {
        assert_eq!(code_of(&a, wrong).await, ErrorCode::NotFound);
    }
    assert_ne!(status_of(&a, &mine).await.state, AttemptState::Terminal);
    dial(&f, &agent, &main).await.unwrap();

    // The notice: this member's attempt ends `interrupted`, without waiting
    // for the lease and without a cancel.
    let first = call(&a, retired_req(&main, &f.worker, "boot-1"))
        .await
        .unwrap();
    wait_for("the retired member's attempt to end", || {
        f.store().get(&mine).unwrap().unwrap().state == AttemptState::Terminal
    });
    let ended = f.store().get(&mine).unwrap().unwrap();
    assert_eq!(ended.termination.unwrap().outcome(), Outcome::Interrupted);
    assert_eq!(ended.cancel_requested_at_ms, None);
    assert!(!ended.retired);
    assert_eq!(events(&f, &mine, "lease_expired"), 1);
    // The same notice again: the same answer, nothing more.
    let again = call(&a, retired_req(&main, &f.worker, "boot-1"))
        .await
        .unwrap();
    let strip = |r: Response| match r {
        Response::IdentityRetired(r) => (r.worker_id, r.worker_boot_id),
        other => panic!("{other:?}"),
    };
    assert_eq!(strip(first), strip(again));
    assert_eq!(events(&f, &mine, "lease_expired"), 1);

    // The other member's attempt, lease and connection are untouched.
    assert_ne!(status_of(&b, &theirs).await.state, AttemptState::Terminal);
    assert_eq!(events(&f, &theirs, "lease_expired"), 0);
    dial(&f, &agent, &sub).await.unwrap();
    // The member that retired the identity is refused for as long as the
    // worker has it, also after the agent starts again...
    assert_eq!(
        refusal(dial(&f, &agent, &main).await),
        GroupRefusal::IdentityRetired
    );
    let Response::CancelAttempt(_) = call(&b, {
        let mut c = cancel_req(&theirs);
        if let Request::CancelAttempt(r) = &mut c {
            r.coordinator_epoch = CoordinatorEpoch(sub.epoch);
        }
        c
    })
    .await
    .unwrap() else {
        panic!()
    };
    wait_for("the other member's attempt to end", || {
        f.store().get(&theirs).unwrap().unwrap().state == AttemptState::Terminal
    });
    assert_eq!(
        f.store()
            .get(&theirs)
            .unwrap()
            .unwrap()
            .termination
            .unwrap()
            .outcome(),
        Outcome::Cancelled
    );
    drop((a, b));
    agent.shutdown();
    let same = f.start("boot-1");
    assert_eq!(
        refusal(dial(&f, &same, &main).await),
        GroupRefusal::IdentityRetired
    );
    same.shutdown();
    // ...and let in again by the worker's next identity.
    let next = f.start("boot-2");
    dial(&f, &next, &main).await.unwrap();
}

/// The start of a prepared attempt looks at its lease first. A lease that
/// was ended by the retirement notice (flagged, long before its time is up)
/// ends the attempt there, `interrupted`, and its command never starts.
/// That is for the retiring member's attempt that had not started: one of
/// its attempts that already runs is stopped by its supervisor, and another
/// member's attempt in the same place starts as if nothing had happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_ends_an_unstarted_attempt_whose_lease_was_ended_and_no_other() {
    let f = fx();
    f.admit_commands(3);
    // Three attempts of two members are held at once.
    let agent = f.start_holding("boot-1", 3);
    let (main, sub) = (member("main"), member("sub"));
    let a = dial(&f, &agent, &main).await.unwrap().client;
    let b = dial(&f, &agent, &sub).await.unwrap().client;
    // Leases far longer than the test: no lease runs out by time here.
    let long = |script: &str| {
        let mut p = payload(script);
        p.lease_ms = 600_000;
        p
    };
    let marked = |tag: &str| {
        let marker = f.path(&format!("{tag}-started"));
        (format!("touch {}; sleep 600", marker.display()), marker)
    };
    // One attempt of the retiring member runs already.
    let (running_script, running_marker) = marked("running");
    let running = AttemptId::generate();
    call(&a, request_of(&main, &running, &long(&running_script)))
        .await
        .unwrap();
    wait_for("the first command to start", || running_marker.exists());
    // Two more are accepted and held before their start: one of the
    // retiring member, one of the other member.
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (unstarted_script, unstarted_marker) = marked("unstarted");
    let other_marker = f.path("other-started");
    let other_script = format!("touch {}", other_marker.display());
    let (unstarted, other) = (AttemptId::generate(), AttemptId::generate());
    call(&a, request_of(&main, &unstarted, &long(&unstarted_script)))
        .await
        .unwrap();
    call(&b, request_of(&sub, &other, &long(&other_script)))
        .await
        .unwrap();
    call(&a, retired_req(&main, &f.worker, "boot-1"))
        .await
        .unwrap();
    // The notice flags the lease of both attempts of that member, and of
    // no other.
    let flagged = |x: &AttemptId| f.store().lease(x).unwrap().is_some_and(|(_, ended)| ended);
    assert!(flagged(&unstarted) && flagged(&running));
    assert!(!flagged(&other));
    assert_ne!(
        f.store().get(&unstarted).unwrap().unwrap().state,
        AttemptState::Terminal,
        "held before its start"
    );
    f.host.block_prepare.store(false, Ordering::SeqCst);

    // The unstarted one ends at its start, without a command.
    assert_eq!(f.wait_terminal(&unstarted), Outcome::Interrupted);
    assert!(!unstarted_marker.exists(), "its command started");
    assert!(f.store().launches(&unstarted).unwrap().is_empty());
    assert_eq!(events(&f, &unstarted, "lease_expired"), 1);
    let ended = f.store().get(&unstarted).unwrap().unwrap();
    assert_eq!(ended.cancel_requested_at_ms, None);
    assert!(!ended.retired);
    // The one that ran was stopped by its supervisor: it had a command.
    assert_eq!(f.wait_terminal(&running), Outcome::Interrupted);
    assert_eq!(f.store().launches(&running).unwrap().len(), 1);
    // The other member's attempt starts and runs to its own end.
    assert_eq!(f.wait_terminal(&other), Outcome::Succeeded);
    assert!(other_marker.exists());
    assert_eq!(events(&f, &other, "lease_expired"), 0);
    assert_eq!(f.store().launches(&other).unwrap().len(), 1);
    drop((a, b));
    agent.shutdown();
}

/// What an ephemeral worker ends itself on (spec 24.3): the notice, no
/// live attempt of any member, and no result that a member which still uses
/// the worker has not collected. The other member's uncollected result
/// holds it until that member collects; the result of the member that
/// retired the identity is never collected and does not hold it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_members_uncollected_result_holds_a_retired_worker_until_it_is_collected() {
    let f = fx();
    f.admit_commands(2);
    let agent = f.start("boot-1");
    let (main, sub) = (member("main"), member("sub"));
    let a = dial(&f, &agent, &main).await.unwrap().client;
    let b = dial(&f, &agent, &sub).await.unwrap().client;
    assert_eq!(agent.retirement(), (false, Some(0), Some(0)));
    let mut long = payload("sleep 600");
    long.lease_ms = 600_000;
    let (mine, theirs) = (AttemptId::generate(), AttemptId::generate());
    call(&a, request_of(&main, &mine, &long)).await.unwrap();
    call(&b, request_of(&sub, &theirs, &payload("exit 0")))
        .await
        .unwrap();
    assert_eq!(f.wait_terminal(&theirs), Outcome::Succeeded);
    // Nobody retired the identity yet: nothing ends the worker.
    let (told, live, uncollected) = agent.retirement();
    assert_eq!((told, live, uncollected), (false, Some(1), Some(1)));

    call(&a, retired_req(&main, &f.worker, "boot-1"))
        .await
        .unwrap();
    assert_eq!(f.wait_terminal(&mine), Outcome::Interrupted);
    // Told and idle, but the other member has not collected its result.
    // The retiring member's own result (never collected) is not counted.
    assert!(f.store().get(&mine).unwrap().unwrap().acked_at_ms.is_none());
    assert_eq!(agent.retirement(), (true, Some(0), Some(1)));
    // The other member is still served: it reads its result and collects.
    let Response::GetResult(result) = call(
        &b,
        Request::GetResult(GetResultRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: theirs.clone(),
            generation: Generation(1),
        }),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(agent.retirement(), (true, Some(0), Some(1)));
    call(
        &b,
        Request::AckCollected(AckCollectedRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            attempt_id: theirs.clone(),
            generation: Generation(1),
            result_digest: result.result_digest,
        }),
    )
    .await
    .unwrap();
    assert_eq!(agent.retirement(), (true, Some(0), Some(0)));
    drop((a, b));
    agent.shutdown();
}

async fn alive_on(c: &Client) -> bool {
    let inspect = Request::Inspect(InspectRequest {
        preparation_observations: None,
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
    });
    // A closed connection says so at once: no answer at all is neither.
    tokio::time::timeout(T, c.call(inspect, T))
        .await
        .expect("a connection answers or is closed")
        .is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_switch_of_the_key_closes_what_was_proven_with_the_old_one() {
    let f = fx();
    let agent = f.start("boot-1");
    let key_path = agent.config().group_key_path.clone();
    let old = member("main");
    let mut new = member("sub");
    new.key = other_key();
    let on_old = dial(&f, &agent, &old).await.unwrap().client;
    // Only the current key is accepted before a rotation.
    assert!(matches!(
        refusal(dial(&f, &agent, &new).await),
        GroupRefusal::GroupKeyMismatch { .. }
    ));

    // The first stage of a rotation: the next key lies beside the current
    // one. Both are accepted, and nothing that runs is touched.
    proto_common::place_key(&group::next_key_path(&key_path), &other_key());
    agent.tick();
    let on_new = dial(&f, &agent, &new).await.unwrap().client;
    assert!(alive_on(&on_old).await);
    dial(&f, &agent, &old).await.unwrap();
    let (current, next) = f.store().group_keys().unwrap();
    assert_eq!(current.unwrap().generation, 1);
    assert_eq!(next.unwrap().fingerprint, group::fingerprint(&other_key()));

    // The second stage: the next key becomes the current one. The
    // connection proven with the old key is closed; the one proven with the
    // new key stays; the old key is refused with the new fingerprint.
    assert!(group::switch_to_next(&key_path).unwrap());
    agent.tick();
    wait_for("the old connection to be closed", || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { !alive_on(&on_old).await })
        })
    });
    assert!(alive_on(&on_new).await);
    assert_eq!(
        refusal(dial(&f, &agent, &old).await),
        GroupRefusal::GroupKeyMismatch {
            fingerprint: group::fingerprint(&other_key())
        }
    );
    let (current, next) = f.store().group_keys().unwrap();
    let current = current.unwrap();
    assert_eq!(
        (current.generation, current.fingerprint),
        (2, group::fingerprint(&other_key()))
    );
    assert!(next.is_none());

    // A member that still holds the new key as its next one, and is refused
    // like that, makes the switch by itself and is let in.
    let dir = tempfile::tempdir().unwrap();
    let late_path = dir.path().join("cluster.key");
    proto_common::place_key(&late_path, &proto_common::group_key());
    proto_common::place_key(&group::next_key_path(&late_path), &other_key());
    let late = |path: &Path| Member {
        key: belt::secrets::key::load(path).unwrap(),
        ..old.clone()
    };
    let refused = dial(&f, &agent, &late(&late_path)).await.err().unwrap();
    let shown = refused.peer_key_fingerprint().unwrap().to_string();
    assert!(group::switch_if_peer_is_on_next(&late_path, &shown).unwrap());
    dial(&f, &agent, &late(&late_path)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_probe_proves_the_key_is_told_who_answers_and_leaves_nothing() {
    let f = fx();
    let agent = f.start("boot-1");
    let probe = ClientConfig::probe(f.cluster.clone(), proto_common::group_key());
    let connected = dial_as(&f, &agent, probe).await.unwrap();
    assert_eq!(connected.worker.worker_id, f.worker);
    assert_eq!(connected.cluster_id, f.cluster);
    // Nothing is served on it, and nothing was recorded for it.
    assert!(!alive_on(&connected.client).await);
    assert!(f.store().members().unwrap().is_empty());
    // It proves the key like any connection.
    let wrong = ClientConfig::probe(f.cluster.clone(), other_key());
    assert!(matches!(
        refusal(dial_as(&f, &agent, wrong).await),
        GroupRefusal::GroupKeyMismatch { .. }
    ));
}

fn inspect_req() -> Request {
    Request::Inspect(InspectRequest {
        preparation_observations: None,
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
    })
}

async fn window_of(c: &Client) -> AdmissionWindow {
    match call(c, inspect_req()).await.unwrap() {
        Response::Inspect(i) => i
            .admission
            .expect("a worker of this build answers its window"),
        other => panic!("{other:?}"),
    }
}

fn forecast_req(m: &Member) -> Request {
    let (a, n) = (AttemptId::generate(), NodeId::parse("main").unwrap());
    Request::Forecast(ForecastRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(m.epoch),
        workload: WorkloadPlan {
            attempt_id: a.clone(),
            plan_digest: Digest::of_bytes(b"plan"),
            expansion_seq: 0,
            nodes: vec![WorkloadNode {
                ci_history_key: None,
                node_id: n.clone(),
                depends_on: vec![],
                priority: 0,
                weight: Some(1),
                predicted_rss_bytes: Some(1),
                predicted_runtime_ms: Some(5_000),
                external_delay_ms: None,
                ready_in_ms: Some(0),
                concurrency: ConcurrencyLimits::default(),
                correlation: Correlation {
                    attempt_id: a,
                    node_id: n,
                    ledger_entry: None,
                },
                shared_builder: None,
            }],
        },
        pending: vec![],
    })
}

/// A52 on a real agent (spec 24.4 and 24.5): the admission window is the
/// worker's own and one budget over every member. A submit beyond it is
/// refused for capacity and leaves nothing, no tombstone and no epoch;
/// `inspect` and a forecast tell a member how much of the window is the
/// others', and nothing else about their attempts.
#[tokio::test(flavor = "multi_thread")]
async fn a52_the_window_is_one_budget_over_every_member_and_a_refused_submit_leaves_nothing() {
    let f = fx();
    // At the end two commands of one member run at once.
    f.admit_commands(2);
    // The window of this worker: one active task and one waiting behind it.
    let agent = f.start("boot-1");
    let (main, sub) = (member("main"), member("sub"));
    let a = dial(&f, &agent, &main).await.unwrap().client;
    let b = dial(&f, &agent, &sub).await.unwrap().client;
    let empty = window_of(&a).await;
    assert_eq!(
        (empty.limit, empty.used, empty.own, empty.foreign),
        (2, 0, 0, 0)
    );
    assert!(empty.has_room() && !empty.shared());
    // One attempt of each member, held before its start: no command runs,
    // so the test does not turn on how many the machine admits at once.
    f.host.block_prepare.store(true, Ordering::SeqCst);
    let (x, y) = (AttemptId::generate(), AttemptId::generate());
    let (script, started, release) = until_released(&f, "sub");
    let (first, second) = (payload("exit 0"), payload(&script));
    call(&a, request_of(&main, &x, &first)).await.unwrap();
    call(&b, request_of(&sub, &y, &second)).await.unwrap();
    for c in [&a, &b] {
        let w = window_of(c).await;
        assert_eq!((w.limit, w.used, w.own, w.foreign), (2, 2, 1, 1));
        assert!(!w.has_room() && w.shared());
    }

    // A third, from either member and also under a newer epoch of its own.
    let z = AttemptId::generate();
    let third = payload("exit 0");
    let mut newer = sub.clone();
    newer.epoch = sub.epoch + 5;
    for (c, m) in [(&a, &main), (&b, &newer), (&b, &newer)] {
        let e = call(c, request_of(m, &z, &third)).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Overloaded);
        assert_eq!(
            e.overloaded_reason,
            Some(belt::error::OverloadedReason::Capacity)
        );
        for other in [&first, &second] {
            assert!(
                !e.message.contains(other.run_id.as_str())
                    && !e.message.contains(other.task_id.as_str()),
                "nothing of an attempt is shown"
            );
        }
    }
    // Nothing was stored: no attempt, no tombstone, no event of it, and
    // not the epoch the refused submit carried.
    let store = f.store();
    assert_eq!(store.count_nonterminal().unwrap(), 2);
    for (c, m) in [(&a, &main), (&b, &sub)] {
        assert_eq!(code_of(c, get_req(&z)).await, ErrorCode::NotFound);
        let own = store.for_member(&m.coordinator);
        assert!(own.get(&z).unwrap().is_none());
        assert!(own.tombstone(&z).unwrap().is_none());
        assert_eq!(own.count_undelivered_tombstones().unwrap(), 0);
        assert!(own.events_of_attempt(&z, 10).unwrap().is_empty());
        assert_eq!(listed(c, None, true).await.len(), 1);
    }
    let again = dial(&f, &agent, &sub).await.unwrap();
    assert_eq!(
        again.worker.coordinator_epoch,
        CoordinatorEpoch(sub.epoch),
        "the refused submit left its epoch"
    );
    // A resend of an accepted attempt adds nothing and is answered.
    let Response::SubmitAttempt(resent) = call(&a, request_of(&main, &x, &first)).await.unwrap()
    else {
        panic!()
    };
    assert!(resent.duplicate);

    // A forecast counts the other member's attempt as demand, says how
    // many there are, and names none of them.
    for (c, m, own, other, theirs) in [(&b, &sub, &y, &x, &first), (&a, &main, &x, &y, &second)] {
        let Response::Forecast(r) = call(c, forecast_req(m)).await.unwrap() else {
            panic!("a forecast answer")
        };
        assert_eq!(r.foreign_demand, 1);
        assert!(r.coverage.iter().any(|c| &c.attempt_id == own));
        assert!(r.coverage.iter().all(|c| &c.attempt_id != other));
        let text = serde_json::to_string(&r).unwrap();
        for hidden in [
            other.as_str(),
            theirs.run_id.as_str(),
            theirs.task_id.as_str(),
        ] {
            assert!(!text.contains(hidden), "{text}");
        }
    }

    // An attempt that ended gives its place back, to whoever comes first:
    // the first member's is cancelled, the other member's goes on running.
    call(&a, cancel_req(&x)).await.unwrap();
    f.host.block_prepare.store(false, Ordering::SeqCst);
    assert_eq!(f.wait_terminal(&x), Outcome::Cancelled);
    wait_for("the other member's command to start", || started.exists());
    let w = window_of(&b).await;
    assert_eq!((w.limit, w.used, w.own, w.foreign), (2, 1, 1, 0));
    assert!(w.has_room() && !w.shared());
    let w = window_of(&a).await;
    assert_eq!((w.used, w.own, w.foreign), (1, 0, 1));
    assert!(w.has_room() && w.shared());
    // The attempt that was refused is taken now, as it was sent before.
    call(&b, request_of(&sub, &z, &third)).await.unwrap();
    assert_eq!(f.wait_terminal(&z), Outcome::Succeeded);
    std::fs::write(&release, "").unwrap();
    assert_eq!(f.wait_terminal(&y), Outcome::Succeeded);
    let w = window_of(&a).await;
    assert_eq!((w.limit, w.used, w.own, w.foreign), (2, 0, 0, 0));
}
