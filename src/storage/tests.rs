//! Repository-level tests: the invariants storage enforces.

use super::*;
use crate::error::ErrorCode;
use crate::ids::*;
use crate::state::{Exit, Observation, Outcome, Receipt, Receipts};
use crate::wire::LeaseKey;
use rusqlite::OptionalExtension;
use serde_json::json;
use tempfile::TempDir;

const DAY: u64 = 86_400_000;

fn hash(c: char) -> PayloadHash {
    PayloadHash::parse(&format!("sha256:{}", c.to_string().repeat(64))).unwrap()
}

fn done(outcome: Outcome) -> Termination {
    match outcome {
        Outcome::Succeeded => Termination::new(outcome, Some(Exit::Code(0)), None).unwrap(),
        Outcome::Failed => Termination::new(outcome, Some(Exit::Code(1)), None).unwrap(),
        _ => Termination::new(outcome, None, None).unwrap(),
    }
}

fn key(epoch: u64, seq: u64) -> LeaseKey {
    LeaseKey {
        coordinator_epoch: CoordinatorEpoch(epoch),
        lease_seq: seq,
    }
}

fn path_state(dir: &TempDir) -> &std::path::Path {
    dir.path()
}

// ============================================================ coordinator

struct Fx {
    _dir: TempDir,
    store: CoordinatorStore,
    run: RunId,
    task: TaskId,
    worker: WorkerId,
}

fn fx() -> Fx {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(path_state(&dir), 1).unwrap();
    let run = RunId::generate();
    let task = TaskId::generate();
    store.create_run(&run, None, None, &json!({}), 1).unwrap();
    store.create_task(&task, &run, &json!({}), 1).unwrap();
    Fx {
        _dir: dir,
        store,
        run,
        task,
        worker: WorkerId::parse("mini-a").unwrap(),
    }
}

impl Fx {
    fn attempt(&self, h: char) -> Result<AttemptRecord> {
        self.store.create_attempt(
            NewAttempt {
                attempt_id: AttemptId::generate(),
                task_id: self.task.clone(),
                worker_id: self.worker.clone(),
                payload_hash: hash(h),
                permit_id: None,
            },
            10,
        )
    }

    fn to_finalizing(&self, id: &AttemptId) {
        for s in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            self.store.transition_attempt(id, s, 20).unwrap();
        }
    }
}

#[test]
fn each_open_starts_a_new_epoch_and_a_second_open_is_locked_out() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let first = CoordinatorStore::open(dir.path(), 1).unwrap();
    assert_eq!(first.epoch(), CoordinatorEpoch(1));
    let err = CoordinatorStore::open(dir.path(), 2).err().unwrap();
    assert!(matches!(err, StorageError::Locked(_)), "{err:?}");
    // Reading needs no lock and does not bump the epoch.
    let ro = CoordinatorStore::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.epoch(), CoordinatorEpoch(1));
    assert!(ro
        .create_run(&RunId::generate(), None, None, &json!({}), 1)
        .is_err());
    let (cluster, coordinator) = (first.cluster_id().clone(), first.coordinator_id().clone());
    drop(first);
    drop(ro);
    let second = CoordinatorStore::open(dir.path(), 3).unwrap();
    assert_eq!(second.epoch(), CoordinatorEpoch(2));
    assert_eq!(second.cluster_id(), &cluster);
    assert_eq!(second.coordinator_id(), &coordinator);
    drop(second);
    assert_eq!(
        CoordinatorStore::open(dir.path(), 4).unwrap().epoch(),
        CoordinatorEpoch(3)
    );
}

#[test]
fn a_task_never_has_two_live_attempts() {
    let f = fx();
    let a1 = f.attempt('a').unwrap();
    assert_eq!(a1.generation, Generation(1));
    assert_eq!(a1.state, AttemptState::Accepted);
    assert!(a1.receipts.reached(Receipt::CoordinatorAccepted));
    assert_eq!(a1.coordinator_epoch, f.store.epoch());
    let err = f.attempt('a').err().unwrap();
    assert!(matches!(err, StorageError::LiveAttemptExists(_)), "{err:?}");
    assert_eq!(err.code(), ErrorCode::Conflict);
    // Even the unique index alone refuses it, bypassing the repository check.
    let raw = f.store.db.call(|c| {
        Ok(c.execute(
            "INSERT INTO attempt (attempt_id, task_id, worker_id, generation, payload_hash,
                coordinator_epoch, state, receipts_json, created_at_ms)
             SELECT 'x', task_id, 'w', 99, 'h', 1, 'accepted', '{}', 1 FROM attempt LIMIT 1",
            [],
        )?)
    });
    assert!(raw.is_err());

    // A terminal attempt frees the task; the next generation counts up.
    f.to_finalizing(&a1.attempt_id);
    f.store
        .finalize_attempt(&a1.attempt_id, done(Outcome::Failed), 30)
        .unwrap();
    let a2 = f.attempt('a').unwrap();
    assert_eq!(a2.generation, Generation(2));
}

#[test]
fn a_new_attempt_waits_for_a_confirmed_retire_with_a_persisted_tombstone() {
    let f = fx();
    let a1 = f.attempt('a').unwrap();
    // Confirming without the tombstone is refused.
    assert!(matches!(
        f.store.confirm_retire(&a1.attempt_id, 5).err().unwrap(),
        StorageError::NotFound { .. }
    ));
    f.store.begin_retire(&a1.attempt_id, "ack lost", 5).unwrap();
    f.store.begin_retire(&a1.attempt_id, "again", 6).unwrap();
    assert_eq!(
        f.store.retire_tombstone(&a1.attempt_id).unwrap(),
        Some((5, None))
    );
    // The tombstone alone does not free the task (the old one may still run).
    assert!(f.attempt('a').is_err());
    f.store.confirm_retire(&a1.attempt_id, 7).unwrap();
    assert_eq!(
        f.store.retire_tombstone(&a1.attempt_id).unwrap(),
        Some((5, Some(7)))
    );
    assert!(
        f.store
            .get_attempt(&a1.attempt_id)
            .unwrap()
            .unwrap()
            .retired
    );
    f.attempt('a').unwrap();
    // The tombstone cannot be deleted, not even by SQL.
    let del = f
        .store
        .db
        .call(|c| Ok(c.execute("DELETE FROM retire_tombstone", [])?));
    assert!(del.is_err());
    assert!(f.store.retire_tombstone(&a1.attempt_id).unwrap().is_some());
}

#[test]
fn terminal_is_fixed_once_whichever_finalize_comes_first() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    f.to_finalizing(&a.attempt_id);
    let results: Vec<Finalized> = std::thread::scope(|s| {
        let hs: Vec<_> = [
            Outcome::Succeeded,
            Outcome::Cancelled,
            Outcome::Failed,
            Outcome::Interrupted,
        ]
        .into_iter()
        .map(|o| {
            let (st, id) = (&f.store, &a.attempt_id);
            s.spawn(move || st.finalize_attempt(id, done(o), 40).unwrap())
        })
        .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|r| r.won()).count(), 1);
    let winner = results.iter().find(|r| r.won()).unwrap().termination();
    assert!(results.iter().all(|r| r.termination() == winner));
    let rec = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert_eq!(rec.termination, Some(winner));
    assert_eq!(rec.state, AttemptState::Terminal);
    assert!(rec.receipts.reached(Receipt::Terminal));
    // A later call with a different result still gets the first one.
    let again = f
        .store
        .finalize_attempt(&a.attempt_id, done(Outcome::Succeeded), 50)
        .unwrap();
    assert_eq!(again, Finalized::AlreadyTerminal(winner));
    // Exactly one terminal event.
    let events = f.store.events_after(0, 1000).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == "attempt_terminal")
            .count(),
        1
    );
}

#[test]
fn only_the_state_machine_moves_attempts() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    let id = &a.attempt_id;
    // Accepted -> Active skips stages.
    let e = f
        .store
        .transition_attempt(id, AttemptState::Active, 1)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::Transition(_)), "{e:?}");
    // Terminal is never reached by a plain transition, nor finalize from Accepted.
    assert!(f
        .store
        .transition_attempt(id, AttemptState::Terminal, 1)
        .is_err());
    assert!(f
        .store
        .finalize_attempt(id, done(Outcome::Succeeded), 1)
        .is_err());
    f.to_finalizing(id);
    assert!(f
        .store
        .transition_attempt(id, AttemptState::Terminal, 1)
        .is_err());
    let rec = f.store.get_attempt(id).unwrap().unwrap();
    assert_eq!(rec.state, AttemptState::Finalizing);
    assert_eq!(rec.termination, None);
    // The database itself refuses a terminal row without a termination.
    let bad = f
        .store
        .db
        .call(|c| Ok(c.execute("UPDATE attempt SET state = 'terminal'", [])?));
    assert!(bad.is_err());
}

#[test]
fn cancel_is_persisted_and_receipts_are_first_write_wins() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    assert!(f.store.request_cancel(&a.attempt_id, 5).unwrap());
    assert!(!f.store.request_cancel(&a.attempt_id, 9).unwrap());
    let rec = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert_eq!(rec.cancel_requested_at_ms, Some(5));
    assert!(f.store.request_cancel(&AttemptId::generate(), 1).is_err());
    assert!(f
        .store
        .mark_receipt(&a.attempt_id, Receipt::WorkerAccepted, 6)
        .unwrap());
    assert!(!f
        .store
        .mark_receipt(&a.attempt_id, Receipt::WorkerAccepted, 7)
        .unwrap());
    for r in [Receipt::Terminal, Receipt::Collected] {
        assert!(f.store.mark_receipt(&a.attempt_id, r, 8).is_err());
    }
    f.store
        .set_observation(&a.attempt_id, Observation::Unknown)
        .unwrap();
    let rec = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert_eq!(rec.observation, Observation::Unknown);
    assert_eq!(rec.receipts.at_ms(Receipt::WorkerAccepted), Some(6));
}

#[test]
fn the_event_journal_is_append_only_with_increasing_sequence() {
    let f = fx();
    let s1 = f
        .store
        .append_event(Some(&f.run), None, "a", &json!(1), 1)
        .unwrap();
    let s2 = f
        .store
        .append_event(Some(&f.run), None, "b", &json!(2), 2)
        .unwrap();
    assert!(s2 > s1);
    let all = f.store.events_after(0, 1000).unwrap();
    assert!(all.windows(2).all(|w| w[0].seq < w[1].seq));
    let tail = f.store.events_after(s1, 1000).unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].payload, json!(2));
    assert!(f.store.events_after(0, 1).unwrap().len() == 1);
    assert!(f
        .store
        .db
        .call(|c| Ok(c.execute("UPDATE event SET kind='x'", [])?))
        .is_err());
    assert!(f
        .store
        .db
        .call(|c| Ok(c.execute("DELETE FROM event", [])?))
        .is_err());
    // A sequence number is never reused, even after the newest row would go.
    let s3 = f.store.append_event(None, None, "c", &json!(3), 3).unwrap();
    assert!(s3 > s2);
}

#[test]
fn events_are_numbered_per_attempt_without_holes() {
    let f = fx();
    let (a, b) = (AttemptId::generate(), AttemptId::generate());
    let add = |att: Option<&AttemptId>, k: &str| {
        f.store
            .append_event(Some(&f.run), att, k, &json!({"k": k}), 1)
            .unwrap()
    };
    add(Some(&a), "x");
    add(None, "none");
    add(Some(&b), "x");
    add(Some(&a), "y");
    add(Some(&a), "x");
    add(Some(&b), "y");
    let all = f.store.events_after(0, 1000).unwrap();
    let of = |att: &AttemptId| -> Vec<u64> {
        all.iter()
            .filter(|e| e.attempt_id.as_deref() == Some(att.as_str()))
            .map(|e| e.attempt_seq.unwrap())
            .collect()
    };
    assert_eq!(of(&a), [1, 2, 3]);
    assert_eq!(of(&b), [1, 2]);
    let none: Vec<_> = all.iter().filter(|e| e.attempt_id.is_none()).collect();
    assert!(none.iter().all(|e| e.attempt_seq.is_none()));
    // By attempt and kind, oldest first.
    let xs = f.store.events_of(&a, "x").unwrap();
    assert_eq!(
        xs.iter()
            .map(|e| e.attempt_seq.unwrap())
            .collect::<Vec<_>>(),
        [1, 3]
    );
    assert!(f.store.events_of(&a, "nope").unwrap().is_empty());
    // The pair is unique, and an event has an attempt_seq exactly when it has an attempt.
    let dup = f.store.db.call(|c| {
        Ok(c.execute(
            "INSERT INTO event (run_id, attempt_id, attempt_seq, kind, payload_json, at_ms)
             SELECT run_id, attempt_id, attempt_seq, kind, payload_json, at_ms FROM event
             WHERE attempt_seq = 1 AND attempt_id IS NOT NULL LIMIT 1",
            [],
        )?)
    });
    assert!(dup.is_err());
    let lone = f.store.db.call(|c| {
        Ok(c.execute(
            "INSERT INTO event (attempt_id, attempt_seq, kind, payload_json, at_ms)
             VALUES (NULL, 5, 'k', '1', 1)",
            [],
        )?)
    });
    assert!(lone.is_err());
}

#[test]
fn unseen_events_are_appended_once_per_key() {
    let f = fx();
    let a = AttemptId::generate();
    let want = |keys: &[&str]| -> Vec<(String, serde_json::Value)> {
        keys.iter()
            .map(|k| (k.to_string(), json!({"stream": k})))
            .collect()
    };
    let go = |keys: &[&str]| {
        f.store
            .append_events_unseen(Some(&f.run), &a, "t", "stream", &want(keys), 5)
            .unwrap()
    };
    assert_eq!(go(&["o", "e"]), 2);
    assert_eq!(go(&["o", "e"]), 0);
    assert_eq!(go(&["e", "z"]), 1);
    let ev = f.store.events_of(&a, "t").unwrap();
    assert_eq!(ev.len(), 3);
    assert_eq!(
        ev.iter()
            .map(|e| e.attempt_seq.unwrap())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(ev
        .iter()
        .all(|e| e.run_id.as_deref() == Some(f.run.as_str())));
    // Other kinds and other attempts do not count as seen.
    let b = AttemptId::generate();
    assert_eq!(
        f.store
            .append_events_unseen(None, &b, "t", "stream", &want(&["o"]), 6)
            .unwrap(),
        1
    );
}

#[test]
fn matrix_permits_hold_until_terminal_or_confirmed_retire() {
    let f = fx();
    f.store.create_matrix_group("g", &f.run, 2, 1).unwrap();
    let (p1, p2, p3) = (
        PermitId::generate(),
        PermitId::generate(),
        PermitId::generate(),
    );
    assert!(f.store.acquire_permit("g", &p1, 1).unwrap().is_some());
    assert!(f.store.acquire_permit("g", &p2, 1).unwrap().is_some());
    assert!(f.store.acquire_permit("g", &p3, 1).unwrap().is_none());
    // Asking again with an existing id returns it without taking another slot.
    assert!(f.store.acquire_permit("g", &p1, 2).unwrap().is_some());
    assert_eq!(f.store.held_permits("g").unwrap(), 2);

    // Bind p1 to an attempt in the same commit as the attempt.
    let aid = AttemptId::generate();
    let a = f
        .store
        .create_attempt(
            NewAttempt {
                attempt_id: aid.clone(),
                task_id: f.task.clone(),
                worker_id: f.worker.clone(),
                payload_hash: hash('a'),
                permit_id: Some(p1.clone()),
            },
            3,
        )
        .unwrap();
    assert_eq!(
        f.store.get_permit(&p1).unwrap().unwrap().attempt_id,
        Some(aid.clone())
    );
    assert_eq!(a.permit_id, Some(p1.clone()));
    // Unknown, running, even finalizing: the slot stays.
    let e = f.store.release_permit(&p1, 4).err().unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    f.to_finalizing(&aid);
    assert!(f.store.release_permit(&p1, 5).is_err());
    f.store
        .finalize_attempt(&aid, done(Outcome::Succeeded), 6)
        .unwrap();
    f.store.release_permit(&p1, 7).unwrap();
    f.store.release_permit(&p1, 8).unwrap();
    assert_eq!(f.store.held_permits("g").unwrap(), 1);
    assert!(f.store.acquire_permit("g", &p3, 9).unwrap().is_some());
    // An unbound permit can be given back, and a bound one frees on retire.
    f.store.release_permit(&p2, 9).unwrap();
    assert!(f.store.get_permit(&PermitId::generate()).unwrap().is_none());
    assert!(f.store.release_permit(&PermitId::generate(), 1).is_err());
    assert!(f
        .store
        .acquire_permit("nope", &PermitId::generate(), 1)
        .is_err());
}

#[test]
fn creating_an_attempt_with_a_bad_permit_leaves_no_attempt() {
    let f = fx();
    let r = f.store.create_attempt(
        NewAttempt {
            attempt_id: AttemptId::generate(),
            task_id: f.task.clone(),
            worker_id: f.worker.clone(),
            payload_hash: hash('a'),
            permit_id: Some(PermitId::generate()),
        },
        1,
    );
    assert!(r.is_err());
    assert!(f.store.list_nonterminal_attempts().unwrap().is_empty());
    // The failed commit did not use up the task.
    f.attempt('a').unwrap();
}

#[test]
fn placements_stay_in_the_unacked_demand_until_acknowledged() {
    let f = fx();
    let p1 = f
        .store
        .record_placement(&f.task, &f.worker, None, &json!({"cpu": 2}), 1)
        .unwrap();
    let p2 = f
        .store
        .record_placement(&f.task, &f.worker, None, &json!({"cpu": 1}), 2)
        .unwrap();
    assert!(p2 > p1);
    let open = f.store.unacked_demand().unwrap();
    assert_eq!(open.len(), 2);
    assert_eq!(open[0].demand, json!({"cpu": 2}));
    f.store.ack_placement(p1, 3).unwrap();
    f.store.ack_placement(p1, 4).unwrap();
    let open = f.store.unacked_demand().unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].placement_id, p2);
    assert!(f.store.ack_placement(999, 1).is_err());
}

#[test]
fn source_and_secret_refs_are_idempotent_and_immutable() {
    let f = fx();
    let src = SourceId::generate();
    let d1 = Digest::of_bytes(b"one");
    let d2 = Digest::of_bytes(b"two");
    f.store.put_source_ref(&src, &d1, 1).unwrap();
    f.store.put_source_ref(&src, &d1, 2).unwrap();
    let e = f.store.put_source_ref(&src, &d2, 3).err().unwrap();
    assert!(matches!(e, StorageError::Immutable(_)));
    assert_eq!(e.code(), ErrorCode::Conflict);
    f.store.put_secret_bundle_ref("b1", &d1, 1).unwrap();
    f.store.put_secret_bundle_ref("b1", &d1, 1).unwrap();
    assert!(f.store.put_secret_bundle_ref("b1", &d2, 1).is_err());
    let run = RunId::generate();
    f.store
        .create_run(&run, Some(&src), Some("b1"), &json!({"k": 1}), 4)
        .unwrap();
    f.store.close_run(&run, 5).unwrap();
    assert!(f
        .store
        .create_run(
            &RunId::generate(),
            Some(&SourceId::generate()),
            None,
            &json!({}),
            1
        )
        .is_err());
}

fn receipt(a: &AttemptId, name: &str) -> ArtifactReceipt {
    ArtifactReceipt {
        attempt_id: a.clone(),
        name: name.into(),
        rel_path: format!("out/{name}"),
        size: 3,
        digest: Digest::of_bytes(name.as_bytes()),
        source_id: SourceId::generate(),
        plan_digest: Digest::of_bytes(b"plan"),
        profile_fingerprint: "fp".into(),
    }
}

#[test]
fn collected_needs_the_terminal_and_every_required_artifact() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    let id = &a.attempt_id;
    f.store.declare_artifact(id, "bin", true).unwrap();
    f.store.declare_artifact(id, "extra", false).unwrap();
    f.store.declare_artifact(id, "bin", true).unwrap();
    // Not terminal yet.
    assert!(f.store.mark_collected(id, 1).is_err());
    f.to_finalizing(id);
    f.store
        .finalize_attempt(id, done(Outcome::Succeeded), 2)
        .unwrap();
    // Terminal, but the required artifact has no receipt.
    let e = f.store.mark_collected(id, 3).err().unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    let r = receipt(id, "bin");
    f.store.record_artifact(&r, 4).unwrap();
    f.store.record_artifact(&r, 5).unwrap();
    let mut changed = r.clone();
    changed.size = 4;
    assert!(matches!(
        f.store.record_artifact(&changed, 6).err().unwrap(),
        StorageError::Immutable(_)
    ));
    assert!(f.store.mark_collected(id, 7).unwrap());
    assert!(!f.store.mark_collected(id, 8).unwrap());
    let rec = f.store.get_attempt(id).unwrap().unwrap();
    assert_eq!(rec.receipts.at_ms(Receipt::Collected), Some(7));
    // An undeclared artifact is recorded as optional.
    f.store.record_artifact(&receipt(id, "late"), 9).unwrap();
}

#[test]
fn an_attempt_without_required_artifacts_is_collected_at_terminal() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    f.to_finalizing(&a.attempt_id);
    f.store
        .finalize_attempt(&a.attempt_id, done(Outcome::Failed), 2)
        .unwrap();
    assert!(f.store.mark_collected(&a.attempt_id, 3).unwrap());
}

fn blob(r: &str, a: &AttemptId, kind: BlobKind, loc: &str) -> NewBlobRef {
    NewBlobRef {
        ref_id: r.into(),
        attempt_id: a.to_string(),
        kind,
        locator: loc.into(),
    }
}

#[test]
fn coordinator_gc_keeps_everything_that_is_not_safely_finished() {
    let f = fx();
    let run = &f.run;
    let mk = |task: &TaskId| -> AttemptRecord {
        f.store.create_task(task, run, &json!({}), 1).unwrap();
        f.store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: task.clone(),
                    worker_id: f.worker.clone(),
                    payload_hash: hash('a'),
                    permit_id: None,
                },
                1,
            )
            .unwrap()
    };
    let end = |a: &AttemptRecord, at: u64| {
        for s in [
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ] {
            f.store.transition_attempt(&a.attempt_id, s, 1).unwrap();
        }
        f.store
            .finalize_attempt(&a.attempt_id, done(Outcome::Succeeded), at)
            .unwrap();
    };
    let old = mk(&TaskId::generate()); // terminal, collected, old: collectable
    let running = mk(&TaskId::generate()); // not terminal
    let uncollected = mk(&TaskId::generate()); // terminal, required artifact missing
    let fresh = mk(&TaskId::generate()); // terminal, collected, recent
    let pinned = mk(&TaskId::generate()); // terminal, collected, old, pinned
    end(&old, 1_000);
    end(&uncollected, 1_000);
    end(&fresh, 1_000 + 6 * DAY);
    end(&pinned, 1_000);
    f.store
        .declare_artifact(&uncollected.attempt_id, "bin", true)
        .unwrap();
    for a in [&old, &fresh, &pinned] {
        assert!(f.store.mark_collected(&a.attempt_id, 2_000).unwrap());
    }
    for (name, a) in [
        ("old", &old),
        ("run", &running),
        ("unc", &uncollected),
        ("fresh", &fresh),
        ("pin", &pinned),
    ] {
        f.store
            .add_blob_ref(
                &blob(
                    &format!("{name}-in"),
                    &a.attempt_id,
                    BlobKind::Input,
                    &format!("blob/{name}"),
                ),
                1,
            )
            .unwrap();
        f.store
            .add_blob_ref(
                &blob(
                    &format!("{name}-log"),
                    &a.attempt_id,
                    BlobKind::Log,
                    &format!("log/{name}"),
                ),
                1,
            )
            .unwrap();
    }
    f.store.pin_blob("pin-in").unwrap();
    assert!(
        f.store.unpin_blob("run-in").is_err(),
        "pins never go negative"
    );
    // Several attempts may share one file: the locator stays in use.
    f.store
        .add_blob_ref(
            &blob("shared", &old.attempt_id, BlobKind::Result, "blob/fresh"),
            1,
        )
        .unwrap();

    let now = 1_000 + 7 * DAY;
    let report = f.store.gc(now, 7 * DAY).unwrap();
    let removed: Vec<&str> = report.removed.iter().map(|r| r.ref_id.as_str()).collect();
    assert_eq!(removed, vec!["old-in", "old-log", "pin-log", "shared"]);
    let kept = |id: &str| report.kept.iter().find(|(r, _)| r == id).map(|(_, k)| *k);
    use policy::KeepReason::*;
    assert_eq!(kept("run-in"), Some(NotTerminal));
    assert_eq!(kept("unc-in"), Some(AwaitingCollection));
    assert_eq!(kept("fresh-in"), Some(WithinRetention));
    assert_eq!(kept("pin-in"), Some(Pinned));
    // `blob/fresh` is still named by fresh-in, so its file must stay: it is
    // not queued. The files nobody names any more are queued, atomically.
    let mut queued = report.queued.clone();
    queued.sort();
    assert_eq!(queued, vec!["blob/old", "log/old", "log/pin"]);
    let mut pending = f.store.pending_deletes().unwrap();
    pending.sort();
    assert_eq!(pending, queued);
    // After the pin is released the rest follows once old enough.
    assert_eq!(f.store.unpin_blob("pin-in").unwrap(), 0);
    let report = f.store.gc(now, 7 * DAY).unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].ref_id, "pin-in");
    // Dedup records, tombstones and the journal are never GC'd.
    assert!(!f.store.events_after(0, 1000).unwrap().is_empty());
    assert!(f.store.get_attempt(&old.attempt_id).unwrap().is_some());
}

#[test]
fn a_restart_keeps_what_was_committed_and_drops_what_was_not() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let (run, task, aid);
    {
        let s = CoordinatorStore::open(dir.path(), 1).unwrap();
        run = RunId::generate();
        task = TaskId::generate();
        aid = AttemptId::generate();
        s.create_run(&run, None, None, &json!({}), 1).unwrap();
        s.create_task(&task, &run, &json!({}), 1).unwrap();
        s.create_attempt(
            NewAttempt {
                attempt_id: aid.clone(),
                task_id: task.clone(),
                worker_id: WorkerId::parse("w").unwrap(),
                payload_hash: hash('a'),
                permit_id: None,
            },
            1,
        )
        .unwrap();
        s.transition_attempt(&aid, AttemptState::Preparing, 2)
            .unwrap();
        // A write that "crashes" before commit: it is gone after the restart.
        s.db.call(|c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute("UPDATE attempt SET state = 'ready'", [])?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            s.get_attempt(&aid).unwrap().unwrap().state,
            AttemptState::Preparing
        );
    }
    let s = CoordinatorStore::open(dir.path(), 9).unwrap();
    assert_eq!(s.epoch(), CoordinatorEpoch(2));
    let live = s.list_nonterminal_attempts().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].attempt_id, aid);
    assert_eq!(live[0].state, AttemptState::Preparing);
    assert!(!live[0].state.recovery_must_not_respawn());
}

// ================================================================== worker

struct Wx {
    dir: TempDir,
    store: WorkerStore,
    id: WorkerId,
}

fn wx() -> Wx {
    let dir = crate::test_tmp::tempdir().unwrap();
    let id = WorkerId::parse("mini-a").unwrap();
    let store = WorkerStore::open(dir.path(), &id).unwrap();
    store.start_agent(1).unwrap();
    Wx { dir, store, id }
}

/// The run of a task in these tests: the same task always names the
/// same run, so a resend is a true replay.
fn run_of_task(task: &TaskId) -> RunId {
    RunId::parse(&format!("run_{}", &task.as_str()["task_".len()..])).unwrap()
}

fn acceptance(a: &AttemptId, task: &TaskId, h: char, epoch: u64) -> NewAcceptance {
    NewAcceptance {
        attempt_id: a.clone(),
        run_id: run_of_task(task),
        task_id: task.clone(),
        generation: Generation(1),
        payload_hash: hash(h),
        coordinator_epoch: CoordinatorEpoch(epoch),
        spec: json!({"cmd": "true"}),
    }
}

/// The submit that produced `a`, byte for byte.
fn resend_of(a: &AcceptedAttempt) -> NewAcceptance {
    NewAcceptance {
        attempt_id: a.attempt_id.clone(),
        run_id: a.run_id.clone(),
        task_id: a.task_id.clone(),
        generation: a.generation,
        payload_hash: a.payload_hash.clone(),
        coordinator_epoch: a.coordinator_epoch,
        spec: a.spec.clone(),
    }
}

fn accepted(w: &Wx) -> AcceptedAttempt {
    match w
        .store
        .submit(
            acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1),
            10,
        )
        .unwrap()
    {
        SubmitOutcome::Accepted(a) => a,
        other => panic!("{other:?}"),
    }
}

fn drive(w: &Wx, id: &AttemptId, to: &[AttemptState]) {
    for s in to {
        w.store.transition(id, *s, 20).unwrap();
    }
}

const TO_READY: [AttemptState; 2] = [AttemptState::Preparing, AttemptState::Ready];

fn node(n: &str) -> NodeId {
    NodeId::parse(n).unwrap()
}

#[test]
fn submit_is_idempotent_for_the_same_payload_and_conflicts_otherwise() {
    let w = wx();
    let (a, task) = (AttemptId::generate(), TaskId::generate());
    let first = match w.store.submit(acceptance(&a, &task, 'a', 1), 10).unwrap() {
        SubmitOutcome::Accepted(r) => r,
        other => panic!("{other:?}"),
    };
    assert_eq!(first.state, AttemptState::Accepted);
    assert!(first.receipts.reached(Receipt::WorkerAccepted));
    // 100 resends return the same record and change nothing.
    for i in 0..100 {
        let again = w
            .store
            .submit(acceptance(&a, &task, 'a', 1), 1_000 + i)
            .unwrap();
        assert_eq!(again, SubmitOutcome::Existing(first.clone()));
    }
    let e = w
        .store
        .submit(acceptance(&a, &task, 'b', 1), 11)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::PayloadConflict(_)), "{e:?}");
    assert_eq!(e.code(), ErrorCode::Conflict);
    assert_eq!(w.store.get(&a).unwrap().unwrap(), first);
    let accepted_events = w.store.events_after(0, 1000).unwrap();
    assert_eq!(
        accepted_events
            .iter()
            .filter(|e| e.kind == "attempt_accepted")
            .count(),
        1
    );
}

#[test]
fn a_second_live_attempt_for_a_task_is_refused() {
    let w = wx();
    let task = TaskId::generate();
    let a1 = AttemptId::generate();
    w.store.submit(acceptance(&a1, &task, 'a', 1), 1).unwrap();
    let e = w
        .store
        .submit(acceptance(&AttemptId::generate(), &task, 'a', 1), 2)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(_)), "{e:?}");
    // A retired attempt still counts until it is confirmed stopped: a new
    // submit for the task is refused, not just delayed by the coordinator.
    w.store.retire(&a1, CoordinatorEpoch(1), 3).unwrap();
    let e = w
        .store
        .submit(acceptance(&AttemptId::generate(), &task, 'a', 1), 4)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(ref l) if *l == a1.to_string()));
    // The stop is confirmed by the terminal write; then the next one may come.
    drive(
        &w,
        &a1,
        &[
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ],
    );
    w.store.finalize(&a1, done(Outcome::Cancelled), 5).unwrap();
    w.store
        .submit(acceptance(&AttemptId::generate(), &task, 'a', 1), 6)
        .unwrap();
}

#[test]
fn a_resend_that_differs_in_run_task_or_generation_conflicts() {
    let w = wx();
    let first = accepted(&w);
    let mut other_gen = resend_of(&first);
    other_gen.generation = Generation(2);
    let mut other_run = resend_of(&first);
    other_run.run_id = RunId::generate();
    let mut other_task = resend_of(&first);
    other_task.task_id = TaskId::generate();
    for bad in [other_gen, other_run, other_task] {
        let e = w.store.submit(bad, 11).err().unwrap();
        assert!(matches!(e, StorageError::PayloadConflict(_)), "{e:?}");
        assert_eq!(e.code(), ErrorCode::Conflict);
    }
    // The true replay still answers, and nothing changed.
    assert_eq!(
        w.store.submit(resend_of(&first), 12).unwrap(),
        SubmitOutcome::Existing(first.clone())
    );
    assert_eq!(w.store.get(&first.attempt_id).unwrap().unwrap(), first);
}

#[test]
fn a_cancel_that_arrives_first_leaves_a_tombstone_and_the_late_submit_is_retired() {
    let w = wx();
    let (a, task) = (AttemptId::generate(), TaskId::generate());
    assert_eq!(
        w.store.cancel(&a, CoordinatorEpoch(1), 5).unwrap(),
        CancelOutcome::Tombstoned
    );
    assert_eq!(w.store.tombstone(&a).unwrap().as_deref(), Some("cancel"));
    let e = w
        .store
        .submit(acceptance(&a, &task, 'a', 1), 6)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::Retired(_)), "{e:?}");
    assert_eq!(e.code(), ErrorCode::Retired);
    assert!(w.store.get(&a).unwrap().is_none(), "nothing was accepted");
    // The tombstone survives a restart and cannot be deleted.
    let Wx { dir, store, id } = w;
    drop(store);
    let store = WorkerStore::open(dir.path(), &id).unwrap();
    assert!(store.submit(acceptance(&a, &task, 'a', 1), 7).is_err());
    assert!(store
        .db
        .call(|c| Ok(c.execute("DELETE FROM tombstone", [])?))
        .is_err());
    // Retire upgrades the same tombstone.
    assert_eq!(store.retire(&a, CoordinatorEpoch(1), 8).unwrap(), None);
    assert_eq!(store.tombstone(&a).unwrap().as_deref(), Some("retire"));
}

#[test]
fn cancelling_a_known_attempt_is_recorded_once_and_stops_new_spawns() {
    let w = wx();
    let a = accepted(&w);
    drive(&w, &a.attempt_id, &TO_READY);
    let CancelOutcome::Requested(rec) = w
        .store
        .cancel(&a.attempt_id, CoordinatorEpoch(1), 30)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(rec.cancel_requested_at_ms, Some(30));
    assert!(matches!(
        w.store.cancel(&a.attempt_id, CoordinatorEpoch(1), 31).unwrap(),
        CancelOutcome::AlreadyRequested(r) if r.cancel_requested_at_ms == Some(30)
    ));
    let e = w
        .store
        .claim_launch(&a.attempt_id, &node("build"), 32)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    assert_eq!(
        w.store.get(&a.attempt_id).unwrap().unwrap().state,
        AttemptState::Ready
    );
    // The same submit still answers with the existing state (not Retired).
    let again = w
        .store
        .submit(
            NewAcceptance {
                attempt_id: a.attempt_id.clone(),
                run_id: a.run_id.clone(),
                task_id: a.task_id.clone(),
                generation: a.generation,
                payload_hash: a.payload_hash.clone(),
                coordinator_epoch: CoordinatorEpoch(1),
                spec: a.spec.clone(),
            },
            40,
        )
        .unwrap();
    assert!(matches!(again, SubmitOutcome::Existing(_)));
}

#[test]
fn a_retired_attempt_refuses_submit_launch_and_lease() {
    let w = wx();
    let a = accepted(&w);
    drive(&w, &a.attempt_id, &TO_READY);
    let rec = w
        .store
        .retire(&a.attempt_id, CoordinatorEpoch(1), 30)
        .unwrap()
        .unwrap();
    assert!(rec.retired);
    let resend = resend_of(&a);
    assert!(matches!(
        w.store.submit(resend, 31).err().unwrap(),
        StorageError::Retired(_)
    ));
    assert!(matches!(
        w.store
            .claim_launch(&a.attempt_id, &node("n"), 32)
            .err()
            .unwrap(),
        StorageError::Retired(_)
    ));
    let e = w
        .store
        .renew_lease(&a.attempt_id, key(1, 1), 33)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LeaseNotRenewable(_)));
    assert_eq!(e.code(), ErrorCode::LeaseNotRenewable);
    // Retire twice is fine and keeps one event.
    w.store
        .retire(&a.attempt_id, CoordinatorEpoch(1), 34)
        .unwrap();
    let ev = w.store.events_after(0, 1000).unwrap();
    assert_eq!(ev.iter().filter(|e| e.kind == "retired").count(), 1);
}

#[test]
fn the_first_claim_grants_the_spawn_and_a_replay_never_does() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    // Too early.
    assert!(w.store.claim_launch(id, &node("build"), 1).is_err());
    drive(&w, id, &TO_READY);
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 30).unwrap(),
        LaunchClaim::Granted
    );
    assert_eq!(
        w.store.get(id).unwrap().unwrap().state,
        AttemptState::Starting
    );
    // Crash before the identity was recorded: recovery sees the claim, and
    // the state forbids a respawn.
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 31).unwrap(),
        LaunchClaim::AlreadyClaimed { identity: None }
    );
    assert!(w
        .store
        .get(id)
        .unwrap()
        .unwrap()
        .state
        .recovery_must_not_respawn());
    let ident = ProcessIdentity {
        boot_id: WorkerBootId::parse("boot-1").unwrap(),
        pid: 4242,
        start_identity: "Mon Nov 10 10:00:00 2026".into(),
    };
    w.store
        .record_process_identity(id, &node("build"), &ident, 32)
        .unwrap();
    w.store
        .record_process_identity(id, &node("build"), &ident, 33)
        .unwrap();
    let other = ProcessIdentity {
        pid: 1,
        ..ident.clone()
    };
    assert!(matches!(
        w.store
            .record_process_identity(id, &node("build"), &other, 34)
            .err()
            .unwrap(),
        StorageError::Immutable(_)
    ));
    assert!(w
        .store
        .record_process_identity(id, &node("nope"), &ident, 34)
        .is_err());
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 35).unwrap(),
        LaunchClaim::AlreadyClaimed {
            identity: Some(ident.clone())
        }
    );
    // A further node of the same attempt is a new claim, in Starting or Active.
    assert_eq!(
        w.store.claim_launch(id, &node("test"), 36).unwrap(),
        LaunchClaim::Granted
    );
    w.store.transition(id, AttemptState::Active, 37).unwrap();
    assert_eq!(
        w.store.claim_launch(id, &node("lint"), 38).unwrap(),
        LaunchClaim::Granted
    );
    let launches = w.store.launches(id).unwrap();
    assert_eq!(launches.len(), 3);
    assert_eq!(launches[0].0, node("build"));
    assert_eq!(launches[0].2, Some(ident));
    // Finalizing / Terminal: no more commands.
    w.store
        .transition(id, AttemptState::Finalizing, 40)
        .unwrap();
    assert!(w.store.claim_launch(id, &node("late"), 41).is_err());
}

#[test]
fn lease_renewal_is_ordered_idempotent_and_never_revives() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(&w, id, &TO_READY);
    let renew = |k| w.store.renew_lease(id, k, 50);
    assert_eq!(renew(key(1, 1)).unwrap(), LeaseOutcome::Extended);
    assert_eq!(renew(key(1, 1)).unwrap(), LeaseOutcome::Stale);
    assert_eq!(renew(key(1, 3)).unwrap(), LeaseOutcome::Extended);
    assert_eq!(renew(key(1, 2)).unwrap(), LeaseOutcome::Stale);
    // A new coordinator epoch restarts the sequence and still wins.
    assert_eq!(renew(key(2, 1)).unwrap(), LeaseOutcome::Extended);
    assert_eq!(w.store.lease(id).unwrap(), Some((key(2, 1), false)));
    // The old coordinator is now rejected outright.
    let e = renew(key(1, 99)).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    assert_eq!(e.code(), ErrorCode::StaleEpoch);
    assert_eq!(w.store.lease(id).unwrap(), Some((key(2, 1), false)));
    // An expiry is remembered and a late renewal does not undo it.
    assert!(w.store.events_of(id, "lease_expired").unwrap().is_empty());
    assert!(w.store.expire_lease(id, 60).unwrap());
    assert!(!w.store.expire_lease(id, 61).unwrap());
    assert_eq!(w.store.lease(id).unwrap(), Some((key(2, 1), true)));
    // Written once, by the first, with its time and the attempt's run.
    let ev = w.store.events_of(id, "lease_expired").unwrap();
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].at_ms, 60);
    assert_eq!(ev[0].run_id.as_deref(), Some(a.run_id.as_str()));
    assert!(renew(key(2, 5)).is_err());
    assert!(w
        .store
        .renew_lease(&AttemptId::generate(), key(2, 1), 1)
        .is_err());
}

#[test]
fn a_lease_is_not_renewed_while_cancelling_or_after_the_end() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(&w, id, &TO_READY);
    w.store.renew_lease(id, key(1, 1), 1).unwrap();
    drive(&w, id, &[AttemptState::Cancelling]);
    assert!(w.store.renew_lease(id, key(1, 2), 2).is_err());
    drive(&w, id, &[AttemptState::Finalizing]);
    assert!(w.store.renew_lease(id, key(1, 3), 3).is_err());
    w.store.finalize(id, done(Outcome::Cancelled), 4).unwrap();
    assert!(w.store.renew_lease(id, key(1, 4), 5).is_err());
    // A never-started cancel request alone also blocks renewal.
    let b = accepted(&w);
    w.store
        .cancel(&b.attempt_id, CoordinatorEpoch(1), 6)
        .unwrap();
    assert!(w.store.renew_lease(&b.attempt_id, key(1, 1), 7).is_err());
}

#[test]
fn worker_terminal_is_fixed_once() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(
        &w,
        id,
        &[
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ],
    );
    let wins: Vec<bool> = std::thread::scope(|s| {
        let hs: Vec<_> = [Outcome::Succeeded, Outcome::Cancelled, Outcome::Failed]
            .into_iter()
            .map(|o| {
                let st = &w.store;
                s.spawn(move || st.finalize(id, done(o), 50).unwrap().won())
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(wins.iter().filter(|w| **w).count(), 1);
    let rec = w.store.get(id).unwrap().unwrap();
    assert!(rec.state.is_terminal() && rec.termination.is_some());
    assert_eq!(rec.terminal_at_ms, Some(50));
    // A receipt cannot be set around finalize.
    for r in [
        Receipt::Terminal,
        Receipt::Collected,
        Receipt::WorkerAccepted,
        Receipt::CoordinatorAccepted,
    ] {
        assert!(w.store.mark_receipt(id, r, 1).is_err());
    }
}

#[test]
fn receipts_log_positions_and_results() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    assert!(w
        .store
        .mark_receipt(id, Receipt::CommandRegistered, 5)
        .unwrap());
    assert!(!w
        .store
        .mark_receipt(id, Receipt::CommandRegistered, 6)
        .unwrap());
    assert!(w.store.mark_receipt(id, Receipt::Running, 7).unwrap());
    // Each receipt is in the journal once, at the time it was first reached.
    let ev = w.store.events_of(id, "attempt_receipt").unwrap();
    let seen: Vec<(String, u64)> = ev
        .iter()
        .map(|e| (e.payload["receipt"].as_str().unwrap().to_string(), e.at_ms))
        .collect();
    assert_eq!(
        seen,
        vec![
            ("command_registered".to_string(), 5),
            ("running".to_string(), 7)
        ]
    );
    assert!(ev
        .iter()
        .all(|e| e.run_id.as_deref() == Some(a.run_id.as_str())));
    let p = |w, d, s| LogPosition {
        written_bytes: w,
        dropped_bytes: d,
        last_seq: s,
    };
    assert_eq!(
        w.store.log_position(id, "stdout").unwrap(),
        LogPosition::default()
    );
    assert_eq!(
        w.store
            .update_log_position(id, "stdout", p(100, 0, 3))
            .unwrap(),
        p(100, 0, 3)
    );
    // Never backwards, per column.
    assert_eq!(
        w.store
            .update_log_position(id, "stdout", p(50, 0, 2))
            .unwrap(),
        p(100, 0, 3)
    );
    let pos = w
        .store
        .update_log_position(id, "stdout", p(120, 900, 4))
        .unwrap();
    assert_eq!(pos, p(120, 900, 4));
    assert!(pos.truncated() && !LogPosition::default().truncated());
    assert_eq!(
        w.store.log_position(id, "stderr").unwrap(),
        LogPosition::default()
    );
    assert!(w
        .store
        .update_log_position(&AttemptId::generate(), "stdout", p(1, 0, 1))
        .is_err());
    // Results only at terminal, once.
    assert!(w.store.record_result(id, &json!({"ok": 1})).is_err());
    drive(
        &w,
        id,
        &[
            AttemptState::Preparing,
            AttemptState::Ready,
            AttemptState::Starting,
            AttemptState::Active,
            AttemptState::Finalizing,
        ],
    );
    w.store.finalize(id, done(Outcome::Succeeded), 9).unwrap();
    assert_eq!(w.store.get_result(id).unwrap(), None);
    w.store.record_result(id, &json!({"ok": 1})).unwrap();
    w.store.record_result(id, &json!({"ok": 1})).unwrap();
    assert!(matches!(
        w.store.record_result(id, &json!({"ok": 2})).err().unwrap(),
        StorageError::Immutable(_)
    ));
    assert_eq!(w.store.get_result(id).unwrap(), Some(json!({"ok": 1})));
    assert_eq!(w.store.get_result(&AttemptId::generate()).unwrap(), None);
    // The result that was acknowledged is stored again as often as it is
    // asked for; only a first result after the acknowledgement is refused.
    assert!(w
        .store
        .ack_collected_as_read(id, CoordinatorEpoch(1), 10, true)
        .unwrap());
    w.store.record_result(id, &json!({"ok": 1})).unwrap();
    assert_eq!(w.store.get_result(id).unwrap(), Some(json!({"ok": 1})));
}

#[test]
fn worker_gc_waits_for_the_ack_and_the_retention_and_skips_pins() {
    let w = wx();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let a = accepted(&w);
        drive(
            &w,
            &a.attempt_id,
            &[
                AttemptState::Preparing,
                AttemptState::Ready,
                AttemptState::Starting,
                AttemptState::Active,
                AttemptState::Finalizing,
            ],
        );
        w.store
            .finalize(&a.attempt_id, done(Outcome::Succeeded), 1_000)
            .unwrap();
        ids.push(a.attempt_id);
    }
    let running = accepted(&w).attempt_id;
    for (i, id) in ids.iter().chain([&running]).enumerate() {
        w.store
            .add_blob_ref(
                &blob(&format!("r{i}"), id, BlobKind::Result, &format!("res/{i}")),
                1,
            )
            .unwrap();
    }
    // r0: acked and old. r1: not acked. r2: acked but pinned. r3: acked, recent.
    for i in [0, 2, 3] {
        assert!(w
            .store
            .ack_collected(&ids[i], CoordinatorEpoch(1), 2_000)
            .unwrap());
    }
    assert!(!w
        .store
        .ack_collected(&ids[0], CoordinatorEpoch(1), 2_001)
        .unwrap());
    // An acknowledgement of a read that found no result is refused once a
    // result is there (and the other way round), and changes nothing.
    assert_eq!(w.store.get_result(&ids[1]).unwrap(), None);
    assert!(matches!(
        w.store
            .ack_collected_as_read(&ids[1], CoordinatorEpoch(1), 2_002, true),
        Err(StorageError::IllegalState { .. })
    ));
    assert!(w
        .store
        .events_of(&ids[1], "ack_collected")
        .unwrap()
        .is_empty());
    w.store
        .finalize_with_result(
            &ids[1],
            done(Outcome::Succeeded),
            2_003,
            |t| serde_json::json!({ "termination": t, "artifacts": [] }),
        )
        .unwrap();
    assert!(matches!(
        w.store
            .ack_collected_as_read(&ids[1], CoordinatorEpoch(1), 2_004, false),
        Err(StorageError::IllegalState { .. })
    ));
    assert!(w
        .store
        .events_of(&ids[1], "ack_collected")
        .unwrap()
        .is_empty());
    assert!(w
        .store
        .ack_collected(&running, CoordinatorEpoch(1), 1)
        .is_err());
    // An attempt that was acknowledged without a result takes none any
    // more: storing one fails, loudly, and nothing is stored.
    assert_eq!(w.store.get_result(&ids[3]).unwrap(), None);
    assert!(matches!(
        w.store.finalize_with_result(
            &ids[3],
            done(Outcome::Succeeded),
            2_005,
            |t| serde_json::json!({ "termination": t, "artifacts": [] }),
        ),
        Err(StorageError::IllegalState { why, .. })
            if why == "the attempt was acknowledged without a result"
    ));
    assert!(matches!(
        w.store
            .record_result(&ids[3], &serde_json::json!({"ok": 1})),
        Err(StorageError::IllegalState { .. })
    ));
    assert_eq!(w.store.get_result(&ids[3]).unwrap(), None);
    w.store.pin_blob("r2").unwrap();
    let now = 1_000 + 7 * DAY;
    // r3 would be old enough too; make it recent by giving it a later terminal.
    let report = w.store.gc(now - 1, 7 * DAY, 30 * DAY).unwrap();
    assert!(
        report.removed.is_empty(),
        "one ms short of retention: {report:?}"
    );
    let report = w.store.gc(now, 7 * DAY, 30 * DAY).unwrap();
    let removed: Vec<_> = report.removed.iter().map(|r| r.ref_id.as_str()).collect();
    assert_eq!(removed, vec!["r0", "r3"]);
    assert!(report
        .kept
        .contains(&("r1".to_string(), policy::KeepReason::AwaitingCollection)));
    assert!(report
        .kept
        .contains(&("r2".to_string(), policy::KeepReason::Pinned)));
    assert!(report
        .kept
        .contains(&("r4".to_string(), policy::KeepReason::NotTerminal)));
    // The dedup record of a GC'd attempt stays: an old submit is not new work.
    let old = w.store.get(&ids[0]).unwrap().unwrap();
    let again = resend_of(&old);
    assert!(matches!(
        w.store.submit(again, now).unwrap(),
        SubmitOutcome::Existing(_)
    ));
}

#[test]
fn stale_coordinator_epochs_are_rejected_for_every_control_request() {
    let w = wx();
    let (a, task) = (AttemptId::generate(), TaskId::generate());
    w.store.submit(acceptance(&a, &task, 'a', 5), 1).unwrap();
    let old = CoordinatorEpoch(4);
    let e = w.store.cancel(&a, old, 2).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }));
    assert!(w.store.retire(&a, old, 2).is_err());
    assert!(w
        .store
        .submit(
            acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 4),
            2
        )
        .is_err());
    assert!(w
        .store
        .get(&a)
        .unwrap()
        .unwrap()
        .cancel_requested_at_ms
        .is_none());
    assert!(w.store.tombstone(&a).unwrap().is_none());
}

#[test]
fn a_worker_restart_keeps_the_launch_claim_and_the_agent_epoch_grows() {
    let w = wx();
    let a = accepted(&w);
    drive(&w, &a.attempt_id, &TO_READY);
    w.store
        .claim_launch(&a.attempt_id, &node("build"), 30)
        .unwrap();
    // A claim made in an uncommitted transaction is lost, the committed one is not.
    w.store
        .db
        .call(|c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute("DELETE FROM command_launch", [])?;
            Ok(())
        })
        .unwrap();
    let Wx { dir, store, id } = w;
    drop(store);
    let store = WorkerStore::open(dir.path(), &id).unwrap();
    assert_eq!(store.start_agent(100).unwrap(), AgentEpoch(2));
    assert_eq!(store.start_agent(101).unwrap(), AgentEpoch(3));
    let live = store.list_nonterminal().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].state, AttemptState::Starting);
    assert!(live[0].state.recovery_must_not_respawn());
    assert_eq!(
        store
            .claim_launch(&a.attempt_id, &node("build"), 102)
            .unwrap(),
        LaunchClaim::AlreadyClaimed { identity: None }
    );
    // Another worker's database is not ours.
    let other = WorkerId::parse("mini-b").unwrap();
    assert!(WorkerStore::open(dir.path(), &other).is_err());
    let ro = WorkerStore::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.worker_id(), &id);
    assert!(ro.start_agent(1).is_err());
}

#[test]
fn database_files_of_the_wrong_kind_are_not_opened() {
    let dir = crate::test_tmp::tempdir().unwrap();
    drop(WorkerStore::open(dir.path(), &WorkerId::parse("w").unwrap()).unwrap());
    std::fs::rename(
        dir.path().join("worker.db"),
        dir.path().join("coordinator.db"),
    )
    .unwrap();
    let e = CoordinatorStore::open(dir.path(), 1).err().unwrap();
    assert!(matches!(e, StorageError::KindMismatch { .. }), "{e:?}");
    assert_eq!(e.code(), ErrorCode::ProtocolIncompatible);
}

#[test]
fn error_codes_map_to_the_wire_codes() {
    use StorageError::*;
    let a = AttemptId::generate();
    let cases: Vec<(StorageError, ErrorCode)> = vec![
        (
            Incompatible(policy::Incompat::Foreign),
            ErrorCode::ProtocolIncompatible,
        ),
        (Locked("x".into()), ErrorCode::Conflict),
        (PayloadConflict(a.clone()), ErrorCode::Conflict),
        (NotFound { what: "x".into() }, ErrorCode::NotFound),
        (Retired(a.clone()), ErrorCode::Retired),
        (LeaseNotRenewable(a), ErrorCode::LeaseNotRenewable),
        (Invalid("x".into()), ErrorCode::InvalidInput),
        (Corrupt("x".into()), ErrorCode::Internal),
        (Closed, ErrorCode::Internal),
        (Panicked, ErrorCode::Internal),
    ];
    for (e, c) in cases {
        assert_eq!(e.code(), c, "{e}");
    }
}

// ======================================================= review hardening

fn to_finalizing_w(w: &WorkerStore, id: &AttemptId) {
    for s in [
        AttemptState::Preparing,
        AttemptState::Ready,
        AttemptState::Starting,
        AttemptState::Active,
        AttemptState::Finalizing,
    ] {
        w.transition(id, s, 20).unwrap();
    }
}

/// `n` independent handles on one worker database, opened at the same moment
/// by `n` threads: each has its own connection and thread, as the agent and
/// the supervisors do.
fn open_together(dir: &std::path::Path, id: &WorkerId, n: usize) -> Vec<Result<WorkerStore>> {
    let barrier = std::sync::Barrier::new(n);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n)
            .map(|_| {
                let b = &barrier;
                s.spawn(move || {
                    b.wait();
                    WorkerStore::open(dir, id)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn open_many(n: usize) -> (TempDir, WorkerId, Vec<WorkerStore>) {
    let dir = crate::test_tmp::tempdir().unwrap();
    let id = WorkerId::parse("mini-a").unwrap();
    let stores = open_together(dir.path(), &id, n)
        .into_iter()
        .map(|r| r.expect("a concurrent open must not fail"))
        .collect();
    (dir, id, stores)
}

/// Run `f(i, store)` on every handle at the same moment.
fn together<T: Send>(
    stores: &[WorkerStore],
    f: impl Fn(usize, &WorkerStore) -> T + Sync,
) -> Vec<T> {
    let barrier = std::sync::Barrier::new(stores.len());
    std::thread::scope(|s| {
        let hs: Vec<_> = stores
            .iter()
            .enumerate()
            .map(|(i, st)| {
                let (b, f) = (&barrier, &f);
                s.spawn(move || {
                    b.wait();
                    f(i, st)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn no_busy<T>(rs: &[Result<T>]) {
    for r in rs {
        if let Err(e) = r {
            assert!(!e.is_retryable(), "a busy error leaked: {e:?}");
        }
    }
}

#[test]
fn many_connections_opening_a_fresh_worker_database_at_once_all_succeed() {
    for _ in 0..5 {
        let dir = crate::test_tmp::tempdir().unwrap();
        let id = WorkerId::parse("mini-a").unwrap();
        let opened = open_together(dir.path(), &id, 8);
        no_busy(&opened);
        let stores: Vec<_> = opened.into_iter().map(|r| r.unwrap()).collect();
        let rows: i64 = stores[0]
            .db
            .call(|c| Ok(c.query_row("SELECT count(*) FROM worker_state", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(rows, 1);
        let version: String = stores[0]
            .db
            .call(|c| {
                Ok(c.query_row(
                    "SELECT value FROM schema_meta WHERE key='schema_version'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(version, "3");
    }
}

#[test]
fn concurrent_finalize_from_separate_connections_has_one_winner() {
    let (_dir, _id, stores) = open_many(6);
    let acc = acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1);
    let a = acc.attempt_id.clone();
    stores[0].submit(acc, 1).unwrap();
    to_finalizing_w(&stores[0], &a);
    let outcomes = [
        Outcome::Succeeded,
        Outcome::Cancelled,
        Outcome::Failed,
        Outcome::Interrupted,
        Outcome::Succeeded,
        Outcome::Failed,
    ];
    let results = together(&stores, |i, st| st.finalize(&a, done(outcomes[i]), 40));
    no_busy(&results);
    let results: Vec<Finalized> = results.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.won()).count(), 1);
    let winner = results.iter().find(|r| r.won()).unwrap().termination();
    assert!(results.iter().all(|r| r.termination() == winner));
    assert_eq!(
        stores[0].get(&a).unwrap().unwrap().termination,
        Some(winner)
    );
    let terminal_events = stores[0]
        .events_after(0, 1000)
        .unwrap()
        .into_iter()
        .filter(|e| e.kind == "attempt_terminal")
        .count();
    assert_eq!(terminal_events, 1);
}

#[test]
fn concurrent_claim_launch_grants_each_node_once() {
    let (_dir, _id, stores) = open_many(6);
    let acc = acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1);
    let a = acc.attempt_id.clone();
    stores[0].submit(acc, 1).unwrap();
    for s in TO_READY {
        stores[0].transition(&a, s, 2).unwrap();
    }
    // The same node from six connections: one grant, five already-claimed.
    let results = together(&stores, |_, st| st.claim_launch(&a, &node("build"), 30));
    no_busy(&results);
    let results: Vec<LaunchClaim> = results.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|c| **c == LaunchClaim::Granted)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|c| matches!(c, LaunchClaim::AlreadyClaimed { .. }))
            .count(),
        5
    );
    assert_eq!(
        stores[0].get(&a).unwrap().unwrap().state,
        AttemptState::Starting
    );
    // Six different nodes at once: every one is granted, the attempt moves to
    // Starting only once.
    let acc = acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1);
    let b = acc.attempt_id.clone();
    stores[0].submit(acc, 3).unwrap();
    for s in TO_READY {
        stores[0].transition(&b, s, 4).unwrap();
    }
    let results = together(&stores, |i, st| {
        st.claim_launch(&b, &node(&format!("n{i}")), 31)
    });
    no_busy(&results);
    assert!(results
        .into_iter()
        .all(|r| r.unwrap() == LaunchClaim::Granted));
    assert_eq!(stores[0].launches(&b).unwrap().len(), 6);
    let starts = stores[0]
        .events_after(0, 1000)
        .unwrap()
        .into_iter()
        .filter(|e| {
            e.attempt_id.as_deref() == Some(b.as_str())
                && e.kind == "attempt_state"
                && e.payload["to"] == "starting"
        })
        .count();
    assert_eq!(starts, 1);
}

#[test]
fn concurrent_submits_have_one_winner_and_no_busy_errors() {
    let (_dir, _id, stores) = open_many(6);
    // The same attempt six times: one Accepted, five Existing.
    let acc = acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1);
    let results = together(&stores, |_, st| st.submit(acc.clone(), 5));
    no_busy(&results);
    let results: Vec<SubmitOutcome> = results.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, SubmitOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, SubmitOutcome::Existing(_)))
            .count(),
        5
    );
    // Six different attempts for one task: exactly one gets in.
    let task = TaskId::generate();
    let results = together(&stores, |_, st| {
        st.submit(acceptance(&AttemptId::generate(), &task, 'a', 1), 6)
    });
    no_busy(&results);
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .all(|e| matches!(e, StorageError::LiveAttemptExists(_))));
}

#[test]
fn a_busy_database_is_a_retryable_error_and_changes_nothing() {
    let (_dir, id, stores) = open_many(2);
    let (holder, other) = (&stores[0], &stores[1]);
    // A short wait on the second connection, so the test is quick.
    other
        .db
        .call(|c| {
            c.busy_timeout(std::time::Duration::from_millis(20))?;
            Ok(())
        })
        .unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let acc = acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1);
    std::thread::scope(|s| {
        s.spawn(|| {
            holder
                .db
                .write(move |_tx| {
                    locked_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        });
        locked_rx.recv().unwrap();
        let e = other.submit(acc.clone(), 1).err().unwrap();
        assert!(matches!(e, StorageError::Busy), "{e:?}");
        assert!(e.is_retryable());
        assert_eq!(e.code(), ErrorCode::Internal);
        assert!(!StorageError::Closed.is_retryable());
        assert!(StorageError::SourceBusy.is_retryable());
        assert_eq!(StorageError::SourceBusy.code(), ErrorCode::SourceBusy);
        assert_eq!(
            StorageError::Io(std::io::Error::from(std::io::ErrorKind::WouldBlock)).code(),
            ErrorCode::Internal
        );
        let e = other.gc(1, 1, 1).err().unwrap();
        assert!(e.is_retryable());
        release_tx.send(()).unwrap();
    });
    // The same call, retried, goes through; the failed one left nothing.
    assert!(other.get(&acc.attempt_id).unwrap().is_none());
    assert!(matches!(
        other.submit(acc, 2).unwrap(),
        SubmitOutcome::Accepted(_)
    ));
    assert_eq!(other.worker_id(), &id);
}

#[test]
fn a_failed_request_still_remembers_the_newer_coordinator_epoch() {
    let w = wx();
    // submit: a payload conflict at epoch 5 must not forget epoch 5.
    let (a, task) = (AttemptId::generate(), TaskId::generate());
    w.store.submit(acceptance(&a, &task, 'a', 1), 1).unwrap();
    let e = w
        .store
        .submit(acceptance(&a, &task, 'b', 5), 2)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::PayloadConflict(_)));
    let e = w
        .store
        .submit(acceptance(&a, &task, 'a', 4), 3)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    // A live-attempt refusal rolls back its own writes and keeps the epoch.
    let e = w
        .store
        .submit(acceptance(&AttemptId::generate(), &task, 'a', 7), 4)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LiveAttemptExists(_)));
    assert_eq!(w.store.list_nonterminal().unwrap().len(), 1);
    let e = w.store.cancel(&a, CoordinatorEpoch(6), 5).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    // renew_lease on a retired attempt fails at epoch 9 and still raises it.
    w.store.retire(&a, CoordinatorEpoch(7), 6).unwrap();
    let e = w.store.renew_lease(&a, key(9, 1), 7).err().unwrap();
    assert!(matches!(e, StorageError::LeaseNotRenewable(_)));
    let e = w.store.retire(&a, CoordinatorEpoch(8), 8).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    // ack_collected on a non-terminal attempt, same.
    let e = w
        .store
        .ack_collected(&a, CoordinatorEpoch(12), 9)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }));
    let e = w.store.cancel(&a, CoordinatorEpoch(11), 10).err().unwrap();
    assert!(matches!(e, StorageError::StaleEpoch { .. }), "{e:?}");
    // And the same epoch is still fine.
    w.store.cancel(&a, CoordinatorEpoch(12), 11).unwrap();
}

#[test]
fn an_expired_lease_refuses_a_new_launch_claim_but_not_a_replay() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(&w, id, &TO_READY);
    w.store.renew_lease(id, key(1, 1), 25).unwrap();
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 30).unwrap(),
        LaunchClaim::Granted
    );
    w.store.expire_lease(id, 40).unwrap();
    let e = w.store.claim_launch(id, &node("test"), 41).err().unwrap();
    assert!(
        matches!(e, StorageError::IllegalState { why, .. } if why.contains("lease expired")),
        "{e:?}"
    );
    assert_eq!(w.store.launches(id).unwrap().len(), 1);
    // The earlier claim is still reported, so recovery can check the process.
    assert_eq!(
        w.store.claim_launch(id, &node("build"), 42).unwrap(),
        LaunchClaim::AlreadyClaimed { identity: None }
    );
    // The first claim of a ready attempt is refused too once it expired.
    let b = accepted(&w);
    drive(&w, &b.attempt_id, &TO_READY);
    w.store.expire_lease(&b.attempt_id, 50).unwrap();
    assert!(w.store.claim_launch(&b.attempt_id, &node("n"), 51).is_err());
    assert_eq!(
        w.store.get(&b.attempt_id).unwrap().unwrap().state,
        AttemptState::Ready
    );
}

#[test]
fn a_result_is_the_same_when_its_canonical_form_is_the_same() {
    let w = wx();
    let a = accepted(&w);
    to_finalizing_w(&w.store, &a.attempt_id);
    w.store
        .finalize(&a.attempt_id, done(Outcome::Succeeded), 9)
        .unwrap();
    let one: serde_json::Value =
        serde_json::from_str(r#"{"b": 2, "a": {"y": 1, "x": [1, 2]}}"#).unwrap();
    let same: serde_json::Value =
        serde_json::from_str(r#"{ "a":{"x":[1,2],"y":1},"b":2 }"#).unwrap();
    w.store.record_result(&a.attempt_id, &one).unwrap();
    w.store.record_result(&a.attempt_id, &same).unwrap();
    let different: serde_json::Value =
        serde_json::from_str(r#"{"a":{"x":[2,1],"y":1},"b":2}"#).unwrap();
    assert!(matches!(
        w.store
            .record_result(&a.attempt_id, &different)
            .err()
            .unwrap(),
        StorageError::Immutable(_)
    ));
    // What is stored is the canonical text.
    let text: String = w
        .store
        .db
        .call(|c| {
            Ok(c.query_row(
                "SELECT result_json FROM worker_attempt WHERE result_json IS NOT NULL",
                [],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(text, r#"{"a":{"x":[1,2],"y":1},"b":2}"#);
    // Floats are not wire JSON.
    assert!(matches!(
        w.store
            .record_result(&a.attempt_id, &json!({"x": 1.5}))
            .err()
            .unwrap(),
        StorageError::Invalid(_)
    ));
}

#[test]
fn tombstones_keep_their_identity_even_against_sql() {
    let w = wx();
    let (a, task) = (AttemptId::generate(), TaskId::generate());
    w.store.cancel(&a, CoordinatorEpoch(3), 5).unwrap();
    let exec = |sql: &'static str| {
        let a = a.to_string();
        w.store.db.call(move |c| Ok(c.execute(sql, [&a])?))
    };
    assert!(exec("UPDATE tombstone SET attempt_id = 'att_other' WHERE attempt_id = ?1").is_err());
    assert!(exec("UPDATE tombstone SET coordinator_epoch = 9 WHERE attempt_id = ?1").is_err());
    assert!(exec("UPDATE tombstone SET recorded_at_ms = 1 WHERE attempt_id = ?1").is_err());
    assert!(exec("DELETE FROM tombstone WHERE attempt_id = ?1").is_err());
    // A cancel may grow into a retire (what `retire` does), and no further back.
    assert_eq!(
        exec("UPDATE tombstone SET kind = 'retire' WHERE attempt_id = ?1").unwrap(),
        1
    );
    assert!(exec("UPDATE tombstone SET kind = 'cancel' WHERE attempt_id = ?1").is_err());
    w.store.retire(&a, CoordinatorEpoch(3), 6).unwrap();
    assert_eq!(w.store.tombstone(&a).unwrap().as_deref(), Some("retire"));
    let _ = task;
}

#[test]
fn a_retire_tombstone_only_gains_its_confirmation() {
    let f = fx();
    let a = f.attempt('a').unwrap();
    f.store.begin_retire(&a.attempt_id, "lost", 5).unwrap();
    let exec = |sql: &'static str| {
        let id = a.attempt_id.to_string();
        f.store.db.call(move |c| Ok(c.execute(sql, [&id])?))
    };
    assert!(exec("UPDATE retire_tombstone SET reason = 'x' WHERE attempt_id = ?1").is_err());
    assert!(exec("UPDATE retire_tombstone SET requested_at_ms = 1 WHERE attempt_id = ?1").is_err());
    assert!(exec("UPDATE retire_tombstone SET worker_id = 'w' WHERE attempt_id = ?1").is_err());
    f.store.confirm_retire(&a.attempt_id, 7).unwrap();
    // A second confirmation is a no-op through the API, and SQL cannot move it.
    f.store.confirm_retire(&a.attempt_id, 9).unwrap();
    assert_eq!(
        f.store.retire_tombstone(&a.attempt_id).unwrap(),
        Some((5, Some(7)))
    );
    assert!(
        exec("UPDATE retire_tombstone SET confirmed_at_ms = 99 WHERE attempt_id = ?1").is_err()
    );
    assert_eq!(
        f.store
            .get_attempt(&a.attempt_id)
            .unwrap()
            .unwrap()
            .retired_at_ms,
        Some(7)
    );
}

#[test]
fn a_released_permit_asked_for_again_is_returned_as_released() {
    let f = fx();
    f.store.create_matrix_group("g", &f.run, 1, 1).unwrap();
    let p = PermitId::generate();
    assert!(f.store.acquire_permit("g", &p, 2).unwrap().unwrap().held);
    f.store.release_permit(&p, 3).unwrap();
    let again = f.store.acquire_permit("g", &p, 4).unwrap().unwrap();
    assert!(!again.held, "a released permit is not given back");
    assert_eq!(f.store.held_permits("g").unwrap(), 0);
    // It took no slot: a new id still gets the only one.
    assert!(f
        .store
        .acquire_permit("g", &PermitId::generate(), 5)
        .unwrap()
        .is_some());
}

// ------------------------------------------------------------ gc: files

#[test]
fn the_last_reference_queues_the_file_and_a_new_reference_to_it_is_refused() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    to_finalizing_w(&w.store, id);
    w.store
        .finalize(id, done(Outcome::Succeeded), 1_000)
        .unwrap();
    w.store
        .ack_collected(id, CoordinatorEpoch(1), 1_001)
        .unwrap();
    let live = accepted(&w);
    w.store
        .add_blob_ref(&blob("r1", id, BlobKind::Log, "log/one"), 1)
        .unwrap();
    // A second reference to the same file, held by an attempt that is still running.
    w.store
        .add_blob_ref(&blob("r2", &live.attempt_id, BlobKind::Input, "log/one"), 1)
        .unwrap();
    w.store
        .add_blob_ref(&blob("r3", id, BlobKind::Result, "res/two"), 1)
        .unwrap();
    let now = 1_000 + 7 * DAY;
    let report = w.store.gc(now, 7 * DAY, 30 * DAY).unwrap();
    // log/one is still named by r2; only res/two is queued.
    assert_eq!(report.queued, vec!["res/two"]);
    assert_eq!(w.store.pending_deletes().unwrap(), vec!["res/two"]);
    // The file's name cannot be taken again before the deletion is confirmed.
    let e = w
        .store
        .add_blob_ref(&blob("r4", &live.attempt_id, BlobKind::Log, "res/two"), now)
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::LocatorPendingDelete(ref l) if l == "res/two"));
    assert_eq!(e.code(), ErrorCode::Conflict);
    // Another name is fine.
    w.store
        .add_blob_ref(
            &blob("r5", &live.attempt_id, BlobKind::Log, "res/three"),
            now,
        )
        .unwrap();
    // After the file is deleted and confirmed the name is free; confirming twice is harmless.
    assert!(w.store.complete_delete("res/two").unwrap());
    assert!(!w.store.complete_delete("res/two").unwrap());
    assert!(w.store.pending_deletes().unwrap().is_empty());
    w.store
        .add_blob_ref(&blob("r4", &live.attempt_id, BlobKind::Log, "res/two"), now)
        .unwrap();
    // A second pass does not queue anything that still has a reference.
    assert!(w
        .store
        .gc(now, 7 * DAY, 30 * DAY)
        .unwrap()
        .queued
        .is_empty());
}

#[test]
fn gc_and_a_new_reference_from_another_connection_never_leave_a_dangling_file() {
    let (_dir, _id, stores) = open_many(2);
    for round in 0..30 {
        let acc = acceptance(&AttemptId::generate(), &TaskId::generate(), 'a', 1);
        let a = acc.attempt_id.clone();
        stores[0].submit(acc, 1).unwrap();
        to_finalizing_w(&stores[0], &a);
        stores[0]
            .finalize(&a, done(Outcome::Succeeded), 1_000)
            .unwrap();
        stores[0]
            .ack_collected(&a, CoordinatorEpoch(1), 1_001)
            .unwrap();
        let loc = format!("file/{round}");
        stores[0]
            .add_blob_ref(&blob(&format!("old{round}"), &a, BlobKind::Log, &loc), 1)
            .unwrap();
        let now = 1_000 + 7 * DAY;
        let new_ref = blob(&format!("new{round}"), &a, BlobKind::Log, &loc);
        let barrier = std::sync::Barrier::new(2);
        let (gc_result, add_result) = std::thread::scope(|s| {
            let g = s.spawn(|| {
                barrier.wait();
                stores[0].gc(now, 7 * DAY, 30 * DAY)
            });
            let ad = s.spawn(|| {
                barrier.wait();
                stores[1].add_blob_ref(&new_ref, now)
            });
            (g.join().unwrap(), ad.join().unwrap())
        });
        gc_result.unwrap();
        // Either the new reference got in first (then the file is not queued
        // by anyone who saw it) or it was refused. Never both a live
        // reference and a queued deletion for the same file.
        let (refs, queued): (i64, i64) = stores[0]
            .db
            .call({
                let loc = loc.clone();
                move |c| {
                    Ok((
                        c.query_row(
                            "SELECT count(*) FROM blob_ref WHERE locator = ?1",
                            [&loc],
                            |r| r.get(0),
                        )?,
                        c.query_row(
                            "SELECT count(*) FROM pending_delete WHERE locator = ?1",
                            [&loc],
                            |r| r.get(0),
                        )?,
                    ))
                }
            })
            .unwrap();
        assert!(
            !(refs > 0 && queued > 0),
            "round {round}: {refs} refs, {queued} queued"
        );
        match add_result {
            Ok(()) => {}
            Err(StorageError::LocatorPendingDelete(_)) => assert_eq!(queued, 1),
            Err(e) => panic!("round {round}: {e:?}"),
        }
    }
}

#[test]
fn gc_runs_in_batches_and_a_failed_batch_keeps_what_the_earlier_ones_did() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    to_finalizing_w(&w.store, id);
    w.store
        .finalize(id, done(Outcome::Succeeded), 1_000)
        .unwrap();
    w.store
        .ack_collected(id, CoordinatorEpoch(1), 1_001)
        .unwrap();
    for i in 0..25 {
        w.store
            .add_blob_ref(
                &blob(&format!("r{i:02}"), id, BlobKind::Log, &format!("f/{i:02}")),
                1,
            )
            .unwrap();
    }
    w.store.pin_blob("r03").unwrap();
    let now = 1_000 + 7 * DAY;
    // Batches of 10: 10 + 10 + 5 references examined, one transaction each.
    let report = w.store.gc_batched(now, 7 * DAY, 30 * DAY, 10).unwrap();
    assert_eq!(report.removed.len(), 24);
    assert_eq!(report.kept.len(), 1);
    assert_eq!(report.queued.len(), 24);
    assert_eq!(w.store.pending_deletes().unwrap().len(), 24);
    assert!(w.store.gc_batched(now, 7 * DAY, 30 * DAY, 0).is_err());

    // A pass that fails in its second batch: the first batch is committed and
    // its files are queued, so nothing is lost and nothing is half-done.
    let w2 = wx();
    let a2 = accepted(&w2);
    to_finalizing_w(&w2.store, &a2.attempt_id);
    w2.store
        .finalize(&a2.attempt_id, done(Outcome::Succeeded), 1_000)
        .unwrap();
    for i in 0..6 {
        w2.store
            .add_blob_ref(
                &blob(
                    &format!("r{i}"),
                    &a2.attempt_id,
                    BlobKind::Log,
                    &format!("g/{i}"),
                ),
                1,
            )
            .unwrap();
    }
    let calls = std::sync::atomic::AtomicU32::new(0);
    let calls = std::sync::Arc::new(calls);
    let c2 = calls.clone();
    let failing = super::common::gc_all(&w2.store.db, now, 7 * DAY, None, 3, move |_, _| {
        if c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 3 {
            Err(StorageError::Busy)
        } else {
            Ok(policy::GcCandidate {
                terminal_at_ms: Some(1_000),
                retired_at_ms: None,
                collected: true,
                collection_failed_at_ms: None,
                missing_required_artifacts: 0,
                pins: 0,
            })
        }
    });
    assert!(failing.unwrap_err().is_retryable());
    assert_eq!(
        w2.store.pending_deletes().unwrap().len(),
        3,
        "first batch kept"
    );
    let rest: i64 = w2
        .store
        .db
        .call(|c| Ok(c.query_row("SELECT count(*) FROM blob_ref", [], |r| r.get(0))?))
        .unwrap();
    assert_eq!(rest, 3, "the failed batch rolled back");
}

// -------------------------------------------- gc: every end has a deadline

#[test]
fn coordinator_gc_reaches_a_deadline_for_every_way_an_attempt_can_end() {
    let f = fx();
    let mk = || -> AttemptRecord {
        let task = TaskId::generate();
        f.store.create_task(&task, &f.run, &json!({}), 1).unwrap();
        f.store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: task,
                    worker_id: f.worker.clone(),
                    payload_hash: hash('a'),
                    permit_id: None,
                },
                1,
            )
            .unwrap()
    };
    let end = |a: &AttemptRecord, at: u64| {
        f.to_finalizing(&a.attempt_id);
        f.store
            .finalize_attempt(&a.attempt_id, done(Outcome::Failed), at)
            .unwrap();
    };
    let add = |r: &str, a: &AttemptRecord| {
        f.store
            .add_blob_ref(&blob(r, &a.attempt_id, BlobKind::Log, &format!("l/{r}")), 1)
            .unwrap();
    };
    // 1. terminal, outputs never recovered, collection given up.
    let given_up = mk();
    end(&given_up, 1_000);
    f.store
        .declare_artifact(&given_up.attempt_id, "bin", true)
        .unwrap();
    add("given-up", &given_up);
    // 2. terminal, nobody has given up yet: still waited for.
    let waiting = mk();
    end(&waiting, 1_000);
    f.store
        .declare_artifact(&waiting.attempt_id, "bin", true)
        .unwrap();
    add("waiting", &waiting);
    // 3. never terminal, retire confirmed.
    let retired = mk();
    f.store
        .begin_retire(&retired.attempt_id, "unknown", 2)
        .unwrap();
    f.store.confirm_retire(&retired.attempt_id, 3_000).unwrap();
    add("retired", &retired);
    // 4. never terminal, retire only requested: it may still run.
    let unconfirmed = mk();
    f.store
        .begin_retire(&unconfirmed.attempt_id, "unknown", 2)
        .unwrap();
    add("unconfirmed", &unconfirmed);
    // 5. running.
    let running = mk();
    add("running", &running);
    // 6. retired and pinned.
    let pinned = mk();
    f.store.begin_retire(&pinned.attempt_id, "x", 2).unwrap();
    f.store.confirm_retire(&pinned.attempt_id, 3_000).unwrap();
    add("pinned", &pinned);
    f.store.pin_blob("pinned").unwrap();

    // The recording API: terminal only, first time wins, never for collected.
    assert!(matches!(
        f.store
            .record_collection_failed(&running.attempt_id, 5)
            .err()
            .unwrap(),
        StorageError::IllegalState { .. }
    ));
    assert!(f
        .store
        .record_collection_failed(&given_up.attempt_id, 5_000)
        .unwrap());
    assert!(!f
        .store
        .record_collection_failed(&given_up.attempt_id, 6_000)
        .unwrap());
    let rec = f.store.get_attempt(&given_up.attempt_id).unwrap().unwrap();
    assert_eq!(rec.collection_failed_at_ms, Some(5_000));
    let collected = mk();
    end(&collected, 1_000);
    f.store
        .mark_collected(&collected.attempt_id, 2_000)
        .unwrap();
    assert!(f
        .store
        .record_collection_failed(&collected.attempt_id, 3)
        .is_err());
    assert!(f
        .store
        .record_collection_failed(&AttemptId::generate(), 3)
        .is_err());

    // Within the retention measured from the later of the end events: nothing.
    let r = 7 * DAY;
    let report = f.store.gc(3_000 + r - 1, r).unwrap();
    assert!(report.removed.is_empty(), "{report:?}");
    // Retired (3_000) is old enough before given-up (5_000) is.
    let report = f.store.gc(3_000 + r, r).unwrap();
    let removed: Vec<_> = report.removed.iter().map(|x| x.ref_id.as_str()).collect();
    assert_eq!(removed, vec!["retired"]);
    let report = f.store.gc(5_000 + r, r).unwrap();
    let removed: Vec<_> = report.removed.iter().map(|x| x.ref_id.as_str()).collect();
    assert_eq!(removed, vec!["given-up"]);
    // Much later still: only what may legitimately still be needed is left.
    let report = f.store.gc(u64::MAX / 2, r).unwrap();
    assert!(report.removed.is_empty(), "{report:?}");
    let kept = |id: &str| report.kept.iter().find(|(r, _)| r == id).map(|(_, k)| *k);
    use policy::KeepReason::*;
    assert_eq!(kept("waiting"), Some(AwaitingCollection));
    assert_eq!(kept("unconfirmed"), Some(NotTerminal));
    assert_eq!(kept("running"), Some(NotTerminal));
    assert_eq!(kept("pinned"), Some(Pinned));
    // The pin goes, the retired attempt follows.
    f.store.unpin_blob("pinned").unwrap();
    assert_eq!(f.store.gc(u64::MAX / 2, r).unwrap().removed.len(), 1);
    // Once the waiting one is given up it follows as well.
    f.store
        .record_collection_failed(&waiting.attempt_id, 9_000)
        .unwrap();
    assert_eq!(f.store.gc(9_000 + r - 1, r).unwrap().removed.len(), 0);
    assert_eq!(f.store.gc(9_000 + r, r).unwrap().removed.len(), 1);
}

#[test]
fn worker_gc_gives_up_waiting_for_an_ack_that_never_comes() {
    let w = wx();
    let finish = |outcome: Outcome, retire: bool| -> AttemptId {
        let a = accepted(&w);
        to_finalizing_w(&w.store, &a.attempt_id);
        if retire {
            w.store
                .retire(&a.attempt_id, CoordinatorEpoch(1), 500)
                .unwrap();
        }
        w.store
            .finalize(&a.attempt_id, done(outcome), 1_000)
            .unwrap();
        a.attempt_id
    };
    let plain = finish(Outcome::Succeeded, false);
    let retired = finish(Outcome::Cancelled, true);
    let acked = finish(Outcome::Succeeded, false);
    w.store
        .ack_collected(&acked, CoordinatorEpoch(1), 1_001)
        .unwrap();
    let running = accepted(&w).attempt_id;
    let pinned = finish(Outcome::Failed, false);
    for (i, a) in [&plain, &retired, &acked, &running, &pinned]
        .into_iter()
        .enumerate()
    {
        w.store
            .add_blob_ref(
                &blob(&format!("r{i}"), a, BlobKind::Result, &format!("res/{i}")),
                1,
            )
            .unwrap();
    }
    w.store.pin_blob("r4").unwrap();
    let (retention, wait) = (7 * DAY, 7 * DAY);
    // The acked one is collectable at retention; the others are still awaited.
    let rep = w.store.gc(1_000 + retention, retention, wait + 1).unwrap();
    assert_eq!(
        rep.removed
            .iter()
            .map(|r| r.ref_id.as_str())
            .collect::<Vec<_>>(),
        vec!["r2"]
    );
    // Once the wait has passed too, the unacked and the retired ones go; the
    // running one and the pinned one never.
    let rep = w.store.gc(1_000 + wait - 1, retention, wait).unwrap();
    assert!(rep.removed.is_empty());
    let rep = w.store.gc(1_000 + wait, retention, wait).unwrap();
    let mut removed: Vec<_> = rep.removed.iter().map(|r| r.ref_id.as_str()).collect();
    removed.sort();
    assert_eq!(removed, vec!["r0", "r1"]);
    let rep = w.store.gc(u64::MAX / 2, retention, wait).unwrap();
    assert!(rep.removed.is_empty());
    assert_eq!(rep.kept.len(), 2);
    assert_eq!(DEFAULT_ACK_WAIT_MS, 7 * DAY);
}

#[test]
fn a_retired_attempt_blocks_a_new_submit_until_it_is_terminal_even_on_the_database() {
    let w = wx();
    let a = accepted(&w);
    w.store
        .retire(&a.attempt_id, CoordinatorEpoch(1), 5)
        .unwrap();
    // The index itself says so, not only the check in `submit`.
    let task = a.task_id.to_string();
    let second = w.store.db.write(move |tx| {
        Ok(tx.execute(
            "INSERT INTO worker_attempt (attempt_id, run_id, task_id, generation, payload_hash,
                coordinator_epoch, spec_json, state, receipts_json, accepted_at_ms)
             VALUES ('att_x','run_x',?1,2,'h',1,'{}','accepted','{}',1)",
            [&task],
        )?)
    });
    assert!(second.is_err());
}

// ================================================================== queue

fn queue_fx(tasks: usize) -> (TempDir, CoordinatorStore, RunId, Vec<TaskId>) {
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let run = RunId::generate();
    let ids: Vec<TaskId> = (0..tasks).map(|_| TaskId::generate()).collect();
    let specs: Vec<(TaskId, serde_json::Value)> = ids
        .iter()
        .enumerate()
        .map(|(i, t)| (t.clone(), json!({ "n": i })))
        .collect();
    store.enqueue(&run, None, &json!({}), &specs, 5).unwrap();
    (dir, store, run, ids)
}

fn worker(name: &str) -> WorkerId {
    WorkerId::parse(name).unwrap()
}

fn place_on(store: &CoordinatorStore, task: &TaskId, w: &str, cap: u32) -> PlaceOutcome {
    store
        .place(
            task,
            &worker(w),
            &AttemptId::generate(),
            move |l| l.active + l.pending < cap,
            |i| Ok((hash('a'), json!({ "generation": i.generation.0 }))),
            10,
        )
        .unwrap()
}

fn placed(o: PlaceOutcome) -> AttemptRecord {
    match o {
        PlaceOutcome::Placed(r) => *r,
        other => panic!("{other:?}"),
    }
}

fn queued(store: &CoordinatorStore) -> Vec<TaskId> {
    let q = store.queued_tasks(100).unwrap();
    q.into_iter().map(|t| t.task_id).collect()
}

fn seen(state: AttemptState, termination: Option<Termination>) -> Option<Seen> {
    Some(Seen {
        state,
        termination,
        receipts: Receipts::default(),
    })
}

#[test]
fn a_run_is_queued_whole_or_not_at_all_and_in_the_order_given() {
    let (_d, store, run, ids) = queue_fx(3);
    assert_eq!(queued(&store), ids);
    assert_eq!(store.queued_tasks(2).unwrap().len(), 2);
    assert_eq!(store.queued_tasks(1).unwrap()[0].spec, json!({"n": 0}));
    assert_eq!(store.queued_tasks(1).unwrap()[0].run_id, run);
    // A run without tasks, and a run one of whose tasks already exists.
    let empty = store.enqueue(&RunId::generate(), None, &json!({}), &[], 6);
    assert!(matches!(empty, Err(StorageError::Invalid(_))), "{empty:?}");
    let other = RunId::generate();
    let clash = vec![(TaskId::generate(), json!({})), (ids[0].clone(), json!({}))];
    assert!(store.enqueue(&other, None, &json!({}), &clash, 6).is_err());
    assert_eq!(queued(&store), ids, "nothing of the refused run is queued");
    let runs: i64 = store
        .db
        .call(|c| Ok(c.query_row("SELECT count(*) FROM run", [], |r| r.get(0))?))
        .unwrap();
    assert_eq!(runs, 1);
}

#[test]
fn placing_commits_the_attempt_and_its_unacked_demand_together() {
    let (_d, store, _run, ids) = queue_fx(2);
    let a = placed(place_on(&store, &ids[0], "w-a", 1));
    assert_eq!(a.generation, Generation(1));
    assert_eq!(a.state, AttemptState::Accepted);
    assert_eq!(a.worker_id, worker("w-a"));
    assert_eq!(a.coordinator_epoch, store.epoch());
    assert!(a.receipts.reached(Receipt::CoordinatorAccepted));
    let demand = store.unacked_demand().unwrap();
    assert_eq!(demand.len(), 1);
    assert_eq!(demand[0].attempt_id, Some(a.attempt_id.clone()));
    assert_eq!(demand[0].demand, json!({"generation": 1}));
    assert_eq!(queued(&store), vec![ids[1].clone()]);
    assert_eq!(
        store.worker_load(&worker("w-a")).unwrap(),
        WorkerLoad {
            active: 0,
            pending: 1
        }
    );

    // A full worker: nothing is written.
    let full = place_on(&store, &ids[1], "w-a", 1);
    assert_eq!(
        full,
        PlaceOutcome::WorkerFull(WorkerLoad {
            active: 0,
            pending: 1
        })
    );
    // A placed task is not placed twice, on this worker or another.
    assert_eq!(place_on(&store, &ids[0], "w-b", 9), PlaceOutcome::NotQueued);
    // A payload that cannot be built leaves nothing behind either.
    let failed = store.place(
        &ids[1],
        &worker("w-b"),
        &AttemptId::generate(),
        |_| true,
        |_| Err(StorageError::Invalid("no payload".into())),
        10,
    );
    assert!(
        matches!(failed, Ok(PlaceOutcome::Unbuildable(_))),
        "{failed:?}"
    );
    assert_eq!(store.unacked_demand().unwrap().len(), 1);
    assert_eq!(store.live_attempts(None).unwrap().len(), 1);
    assert_eq!(queued(&store), vec![ids[1].clone()]);
}

#[test]
fn a_task_is_queued_again_only_once_its_attempt_is_confirmed_retired() {
    let (_d, store, run, ids) = queue_fx(5);
    // 0: live. 1: terminal. 2: retire asked. 3: retire confirmed. 4: cancelled.
    let live = placed(place_on(&store, &ids[0], "w-a", 9));
    let ended = placed(place_on(&store, &ids[1], "w-a", 9));
    store
        .record_seen(
            &ended.attempt_id,
            true,
            seen(AttemptState::Terminal, Some(done(Outcome::Failed))),
            Observation::Known,
            20,
        )
        .unwrap();
    let asked = placed(place_on(&store, &ids[2], "w-a", 9));
    store.begin_retire(&asked.attempt_id, "move", 20).unwrap();
    let gone = placed(place_on(&store, &ids[3], "w-a", 9));
    store.begin_retire(&gone.attempt_id, "move", 20).unwrap();
    store.confirm_retire(&gone.attempt_id, 21).unwrap();
    assert_eq!(store.cancel_task(&ids[4], 22).unwrap(), None);
    assert_eq!(store.task_cancel_requested(&ids[4]).unwrap(), Some(22));
    assert_eq!(store.task_cancel_requested(&ids[0]).unwrap(), None);

    assert_eq!(queued(&store), vec![ids[3].clone()]);
    for o in [Observation::Unknown, Observation::Reconciling] {
        store
            .record_seen(&live.attempt_id, false, None, o, 23)
            .unwrap();
        assert_eq!(queued(&store), vec![ids[3].clone()], "{o:?} keeps its task");
    }
    assert_eq!(place_on(&store, &ids[4], "w-b", 9), PlaceOutcome::NotQueued);
    assert_eq!(place_on(&store, &ids[2], "w-b", 9), PlaceOutcome::NotQueued);

    // The slots: live and retire-asked hold one each, the others none.
    let live_now = store.live_attempts(Some(&worker("w-a"))).unwrap();
    let ids_now: Vec<_> = live_now
        .iter()
        .map(|l| l.record.attempt_id.clone())
        .collect();
    assert_eq!(
        ids_now,
        vec![live.attempt_id.clone(), asked.attempt_id.clone()]
    );
    assert_eq!(live_now[0].retire_reason, None);
    assert_eq!(live_now[1].retire_reason.as_deref(), Some("move"));
    assert!(live_now[0]
        .placement
        .as_ref()
        .unwrap()
        .acked_at_ms
        .is_none());
    assert_eq!(
        store.worker_load(&worker("w-a")).unwrap(),
        WorkerLoad {
            active: 0,
            pending: 2
        }
    );
    assert!(store
        .live_attempts(Some(&worker("w-b")))
        .unwrap()
        .is_empty());

    // The retired one goes elsewhere as the next generation.
    let next = placed(place_on(&store, &ids[3], "w-b", 9));
    assert_eq!(next.generation, Generation(2));
    let all = store.attempts_of_task(&ids[3]).unwrap();
    assert_eq!(all.len(), 2);
    assert!(all[0].retired && !all[1].retired);
    assert!(queued(&store).is_empty());

    // A closed run places nothing more.
    let (_d2, store2, run2, ids2) = queue_fx(1);
    store2.close_run(&run2, 30).unwrap();
    assert!(queued(&store2).is_empty());
    assert_eq!(
        place_on(&store2, &ids2[0], "w-a", 9),
        PlaceOutcome::NotQueued
    );
    let _ = run;
}

#[test]
fn one_task_placed_from_many_threads_gets_one_attempt() {
    let (_d, store, _run, ids) = queue_fx(1);
    let outcomes: Vec<PlaceOutcome> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let (store, task) = (&store, &ids[0]);
                s.spawn(move || place_on(store, task, &format!("w-{i}"), 9))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let won = outcomes
        .iter()
        .filter(|o| matches!(o, PlaceOutcome::Placed(_)))
        .count();
    assert_eq!(won, 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| **o == PlaceOutcome::NotQueued)
            .count(),
        7
    );
    assert_eq!(store.unacked_demand().unwrap().len(), 1);
}

#[test]
fn what_a_worker_reported_is_mirrored_edge_by_edge_and_a_result_is_fixed_once() {
    let (_d, store, _run, ids) = queue_fx(1);
    let a = placed(place_on(&store, &ids[0], "w-a", 9)).attempt_id;
    let mut receipts = Receipts::default();
    receipts.mark(Receipt::CommandRegistered, 1);
    let rec = store
        .record_seen(
            &a,
            true,
            Some(Seen {
                state: AttemptState::Active,
                termination: None,
                receipts,
            }),
            Observation::Known,
            30,
        )
        .unwrap();
    assert_eq!(rec.state, AttemptState::Active);
    assert_eq!(rec.receipts.at_ms(Receipt::WorkerAccepted), Some(30));
    assert!(rec.receipts.reached(Receipt::CommandRegistered));
    assert!(!rec.receipts.reached(Receipt::Running));
    assert!(store.unacked_demand().unwrap().is_empty());
    assert_eq!(
        store.worker_load(&worker("w-a")).unwrap(),
        WorkerLoad {
            active: 1,
            pending: 0
        }
    );
    let steps = store.events_of(&a, "attempt_state").unwrap();
    assert_eq!(steps.len(), 4, "accepted to active is four edges");

    // A state behind the mirror, and an observation alone.
    let rec = store
        .record_seen(
            &a,
            false,
            seen(AttemptState::Ready, None),
            Observation::Unknown,
            31,
        )
        .unwrap();
    assert_eq!(rec.state, AttemptState::Active);
    assert_eq!(rec.observation, Observation::Unknown);
    assert_eq!(store.events_of(&a, "attempt_observation").unwrap().len(), 1);
    store
        .record_seen(&a, false, None, Observation::Unknown, 32)
        .unwrap();
    assert_eq!(store.events_of(&a, "attempt_observation").unwrap().len(), 1);

    // A terminal state without a result is refused whole.
    let bad = store.record_seen(
        &a,
        false,
        seen(AttemptState::Terminal, None),
        Observation::Known,
        33,
    );
    assert!(matches!(bad, Err(StorageError::Invalid(_))), "{bad:?}");
    assert_eq!(
        store.get_attempt(&a).unwrap().unwrap().observation,
        Observation::Unknown
    );

    let end = seen(AttemptState::Terminal, Some(done(Outcome::Interrupted)));
    let rec = store
        .record_seen(&a, false, end, Observation::Known, 34)
        .unwrap();
    assert_eq!(rec.termination, Some(done(Outcome::Interrupted)));
    assert_eq!(rec.terminal_at_ms, Some(34));
    let again = seen(AttemptState::Terminal, Some(done(Outcome::Succeeded)));
    let rec = store
        .record_seen(&a, false, again, Observation::Known, 35)
        .unwrap();
    assert_eq!(rec.termination, Some(done(Outcome::Interrupted)));
    assert_eq!(store.events_of(&a, "attempt_terminal").unwrap().len(), 1);
    assert_eq!(
        store.worker_load(&worker("w-a")).unwrap(),
        WorkerLoad::default()
    );
    assert!(store.live_attempts(None).unwrap().is_empty());
}

#[test]
fn a_cancel_of_a_task_reaches_its_live_attempt_once_and_a_restart_marks_it_reconciling() {
    let (dir, store, _run, ids) = queue_fx(2);
    let a = placed(place_on(&store, &ids[0], "w-a", 9)).attempt_id;
    let hit = store.cancel_task(&ids[0], 40).unwrap().unwrap();
    assert_eq!(hit.attempt_id, a);
    assert_eq!(hit.cancel_requested_at_ms, Some(40));
    let again = store.cancel_task(&ids[0], 41).unwrap().unwrap();
    assert_eq!(again.cancel_requested_at_ms, Some(40));
    assert_eq!(store.task_cancel_requested(&ids[0]).unwrap(), Some(40));
    assert_eq!(store.events_of(&a, "cancel_requested").unwrap().len(), 1);
    assert!(matches!(
        store.cancel_task(&TaskId::generate(), 42),
        Err(StorageError::NotFound { .. })
    ));
    assert!(matches!(
        store.task_cancel_requested(&TaskId::generate()),
        Err(StorageError::NotFound { .. })
    ));

    drop(store);
    let store = CoordinatorStore::open(dir.path(), 50).unwrap();
    assert_eq!(store.epoch(), CoordinatorEpoch(2));
    assert_eq!(store.mark_reconciling().unwrap(), 1);
    assert_eq!(store.mark_reconciling().unwrap(), 0);
    let live = store.live_attempts(None).unwrap();
    assert_eq!(live[0].record.observation, Observation::Reconciling);
    assert_eq!(live[0].record.coordinator_epoch, CoordinatorEpoch(1));
    assert_eq!(
        store.unacked_demand().unwrap().len(),
        1,
        "the demand survives"
    );
    assert_eq!(queued(&store), vec![ids[1].clone()]);
}

#[test]
fn a_terminal_attempt_is_not_retired_by_a_late_confirmation() {
    let (_dir, store, _run, ids) = queue_fx(1);
    let a = placed(place_on(&store, &ids[0], "w-a", 9)).attempt_id;
    store.begin_retire(&a, "move", 20).unwrap();
    let end = seen(AttemptState::Terminal, Some(done(Outcome::Succeeded)));
    store
        .record_seen(&a, true, end, Observation::Known, 21)
        .unwrap();
    let e = store.confirm_retire(&a, 22).err().unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    assert_eq!(e.code(), ErrorCode::Conflict);
    // Nothing of the refused confirmation stays.
    let rec = store.get_attempt(&a).unwrap().unwrap();
    assert!(!rec.retired);
    assert_eq!(rec.termination, Some(done(Outcome::Succeeded)));
    assert_eq!(store.retire_tombstone(&a).unwrap(), Some((20, None)));
    assert!(store.events_of(&a, "retire_confirmed").unwrap().is_empty());
    assert!(queued(&store).is_empty(), "the task is not placed again");
    let e = store.finish_retire(&a, "refused", 23).err().unwrap();
    assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
}

#[test]
fn a_confirmed_retire_frees_the_task_or_settles_the_attempt_by_what_is_recorded() {
    let (_dir, store, _run, ids) = queue_fx(4);
    let at = |i: usize| placed(place_on(&store, &ids[i], "w-a", 9)).attempt_id;
    // Without a tombstone there is nothing to confirm.
    let plain = at(0);
    assert!(matches!(
        store.finish_retire(&plain, "refused", 20),
        Err(StorageError::NotFound { .. })
    ));

    // An ordinary retire: the task is free again.
    store.begin_retire(&plain, "move", 20).unwrap();
    assert_eq!(
        store.finish_retire(&plain, "refused", 21).unwrap(),
        RetireEnd::Freed
    );
    let rec = store.get_attempt(&plain).unwrap().unwrap();
    assert!(rec.retired && rec.termination.is_none());
    assert_eq!(rec.retired_at_ms, Some(21));
    assert_eq!(
        store.retire_tombstone(&plain).unwrap(),
        Some((20, Some(21)))
    );
    assert!(queued(&store).contains(&ids[0]));

    // The task was cancelled after the retire was asked for: the attempt
    // ends `cancelled` and the task is not placed again.
    let cancelled = at(1);
    store.begin_retire(&cancelled, "move", 30).unwrap();
    store.cancel_task(&ids[1], 31).unwrap();
    let RetireEnd::Settled(rec) = store.finish_retire(&cancelled, "refused", 32).unwrap() else {
        panic!("a cancelled task is settled")
    };
    assert_eq!(rec.state, AttemptState::Terminal);
    assert_eq!(rec.termination, Some(done(Outcome::Cancelled)));
    assert_eq!(rec.terminal_at_ms, Some(32));
    assert!(!rec.retired);
    assert_eq!(rec.observation, Observation::Known);
    assert_eq!(
        store.retire_tombstone(&cancelled).unwrap(),
        Some((30, Some(32)))
    );
    let confirmed = store.events_of(&cancelled, "retire_confirmed").unwrap();
    assert_eq!(confirmed.len(), 1);
    assert_eq!(confirmed[0].payload["settled"], "cancelled");
    assert!(!queued(&store).contains(&ids[1]));

    // The retire was asked for because the worker will never take it.
    let refused = at(2);
    store.begin_retire(&refused, "refused", 40).unwrap();
    store
        .record_seen(&refused, false, None, Observation::Unknown, 41)
        .unwrap();
    let RetireEnd::Settled(rec) = store.finish_retire(&refused, "refused", 42).unwrap() else {
        panic!("a refused attempt is settled")
    };
    assert_eq!(rec.termination.unwrap().outcome(), Outcome::Failed);
    assert_eq!(rec.observation, Observation::Known);
    assert!(!rec.retired);
    let confirmed = store.events_of(&refused, "retire_confirmed").unwrap();
    assert_eq!(confirmed[0].payload["settled"], "failed");
    assert!(!queued(&store).contains(&ids[2]));
    // A cancel wins over the reason.
    let both = at(3);
    store.begin_retire(&both, "refused", 50).unwrap();
    store.cancel_task(&ids[3], 51).unwrap();
    let RetireEnd::Settled(rec) = store.finish_retire(&both, "refused", 52).unwrap() else {
        panic!()
    };
    assert_eq!(rec.termination, Some(done(Outcome::Cancelled)));

    // Nothing settled holds a slot or demand; once concluded, it stays.
    assert_eq!(
        store.worker_load(&worker("w-a")).unwrap(),
        WorkerLoad::default()
    );
    assert!(store.unacked_demand().unwrap().is_empty());
    assert!(store.live_attempts(None).unwrap().is_empty());
    for a in [&cancelled, &refused] {
        let e = store.finish_retire(a, "refused", 60).err().unwrap();
        assert!(matches!(e, StorageError::IllegalState { .. }), "{e:?}");
    }
    // Confirming a freed one again changes nothing.
    assert_eq!(
        store.finish_retire(&plain, "refused", 60).unwrap(),
        RetireEnd::Freed
    );
    assert_eq!(
        store.get_attempt(&plain).unwrap().unwrap().retired_at_ms,
        Some(21)
    );
    assert_eq!(queued(&store), vec![ids[0].clone()]);
}

#[test]
fn a_task_whose_payload_cannot_be_built_is_refused_alone_and_a_fault_is_a_fault() {
    let (_dir, store, _run, ids) = queue_fx(1);
    let put = |r: Result<(PayloadHash, serde_json::Value)>| {
        store.place(
            &ids[0],
            &worker("w-a"),
            &AttemptId::generate(),
            |_| true,
            move |_| r,
            10,
        )
    };
    assert_eq!(
        put(Err(StorageError::Invalid("no payload".into()))).unwrap(),
        PlaceOutcome::Unbuildable("no payload".into())
    );
    let e = put(Err(StorageError::Corrupt("disk".into())))
        .err()
        .unwrap();
    assert!(matches!(e, StorageError::Corrupt(_)), "{e:?}");
    // Neither wrote anything: the task is as it was, and can be placed.
    assert!(store.attempts_of_task(&ids[0]).unwrap().is_empty());
    assert_eq!(queued(&store), ids);
    assert!(matches!(
        put(Ok((hash('a'), json!({})))).unwrap(),
        PlaceOutcome::Placed(_)
    ));
}

#[test]
fn a_step_1_coordinator_database_with_rows_is_read_as_it_is_and_upgraded_once() {
    use super::policy::Access;
    let dir = crate::test_tmp::tempdir().unwrap();
    let path = dir.path().join("coordinator.db");
    let all = schema::steps(DbKind::Coordinator);
    assert_eq!(
        all.len(),
        11,
        "this test is about step 1 and the steps after it"
    );
    let (cluster, coordinator) = (ClusterId::generate(), CoordinatorId::generate());
    let (run, queued_task, placed_task) =
        (RunId::generate(), TaskId::generate(), TaskId::generate());
    let attempt = AttemptId::generate();
    {
        // What the first step's binary wrote: a run with a task that waits
        // and one that was placed and is running.
        let v1 = db::open_with(
            &path,
            "coordinator",
            &all[..1],
            schema::compat(1),
            Access::ReadWrite,
        )
        .unwrap();
        let rows = [
            format!("INSERT INTO cluster VALUES ('{cluster}', '{coordinator}', 1)"),
            format!("INSERT INTO coordinator_state VALUES (1, '{cluster}', 3)"),
            format!(
                "INSERT INTO run (run_id, spec_json, created_at_ms) VALUES ('{run}', '{{}}', 2)"
            ),
            format!("INSERT INTO task VALUES ('{queued_task}', '{run}', '{{\"n\":1}}', 3)"),
            format!("INSERT INTO task VALUES ('{placed_task}', '{run}', '{{\"n\":2}}', 4)"),
            format!(
                "INSERT INTO attempt (attempt_id, task_id, worker_id, generation, payload_hash,
                    coordinator_epoch, state, receipts_json, created_at_ms)
                 VALUES ('{attempt}', '{placed_task}', 'w-a', 1, '{}', 3, 'active', '{}', 5)",
                hash('a').as_str(),
                serde_json::to_string(&Receipts::default()).unwrap()
            ),
            format!(
                "INSERT INTO placement (task_id, worker_id, attempt_id, demand_json, created_at_ms)
                 VALUES ('{placed_task}', 'w-a', '{attempt}', '{{}}', 5)"
            ),
        ];
        v1.write(move |tx| {
            for sql in &rows {
                tx.execute(sql, [])?;
            }
            Ok(())
        })
        .unwrap();
    }
    let schema_of = |store: &CoordinatorStore| -> (String, i64, i64) {
        store
            .db
            .call(|c| {
                let version = c.query_row(
                    "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                    [],
                    |r| r.get(0),
                )?;
                let column = c.query_row(
                    "SELECT count(*) FROM pragma_table_info('task')
                     WHERE name = 'cancel_requested_at_ms'",
                    [],
                    |r| r.get(0),
                )?;
                let index = c.query_row(
                    "SELECT count(*) FROM sqlite_master
                     WHERE type = 'index' AND name = 'placement_attempt'",
                    [],
                    |r| r.get(0),
                )?;
                Ok((version, column, index))
            })
            .unwrap()
    };
    let check_rows = |store: &CoordinatorStore| {
        assert_eq!(queued(store), vec![queued_task.clone()]);
        let live = store.live_attempts(None).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].record.attempt_id, attempt);
        assert_eq!(live[0].record.state, AttemptState::Active);
        assert!(live[0].placement.is_some());
        assert_eq!(store.task_cancel_requested(&queued_task).unwrap(), None);
        assert_eq!(
            store.worker_load(&worker("w-a")).unwrap(),
            WorkerLoad {
                active: 1,
                pending: 0
            }
        );
    };

    // Read-only, the newer binary reads the old file without changing it.
    {
        let ro = CoordinatorStore::open_read_only(dir.path()).unwrap();
        assert_eq!(schema_of(&ro), ("1".to_string(), 0, 0));
        assert_eq!(ro.epoch(), CoordinatorEpoch(3));
        check_rows(&ro);
        assert!(matches!(
            ro.task_cancel_requested(&TaskId::generate()),
            Err(StorageError::NotFound { .. })
        ));
        // It has no collections: none is open, none is closed, and the run
        // stands as its attempts do.
        assert_eq!(ro.collection(&attempt).unwrap(), None);
        assert!(ro.collectable(None, 10).unwrap().is_empty());
        assert!(ro.persisted_artifacts(&attempt).unwrap().is_empty());
        let standing = ro.run_standing(&run, None).unwrap();
        assert_eq!(
            standing.verdict,
            crate::results::plan::RunVerdict::Collecting
        );
        assert_eq!(standing.tasks.len(), 2);
    }

    // Opened for writing, it is migrated, and every row is still there.
    let store = CoordinatorStore::open(dir.path(), 10).unwrap();
    assert_eq!(schema_of(&store), ("11".to_string(), 1, 1));
    assert_eq!(store.epoch(), CoordinatorEpoch(4));
    assert_eq!(store.cluster_id(), &cluster);
    check_rows(&store);
    // What the third step added is there, and empty.
    assert!(store.collectable(None, 10).unwrap().is_empty());
    assert_eq!(store.collection(&attempt).unwrap(), None);
    // What the second step added works on the old rows.
    assert_eq!(store.cancel_task(&queued_task, 11).unwrap(), None);
    assert_eq!(store.task_cancel_requested(&queued_task).unwrap(), Some(11));
    assert!(queued(&store).is_empty());
    let hit = store.cancel_task(&placed_task, 12).unwrap().unwrap();
    assert_eq!(hit.attempt_id, attempt);
    assert_eq!(hit.cancel_requested_at_ms, Some(12));
    drop(store);

    // Opening again migrates nothing and loses nothing.
    let store = CoordinatorStore::open(dir.path(), 20).unwrap();
    assert_eq!(schema_of(&store), ("11".to_string(), 1, 1));
    assert_eq!(store.epoch(), CoordinatorEpoch(5));
    assert_eq!(store.task_cancel_requested(&queued_task).unwrap(), Some(11));
    assert_eq!(store.live_attempts(None).unwrap().len(), 1);
    let ro = CoordinatorStore::open_read_only(dir.path()).unwrap();
    assert_eq!(ro.task_cancel_requested(&queued_task).unwrap(), Some(11));
    assert!(queued(&ro).is_empty());
}

#[test]
fn the_termination_and_the_result_are_one_commit() {
    let w = wx();
    let a = accepted(&w);
    let id = &a.attempt_id;
    drive(&w, id, &[AttemptState::Preparing, AttemptState::Finalizing]);
    let of = |t: &Termination| json!({ "termination": t });
    // A result that cannot be stored leaves the attempt as it was.
    let e = w
        .store
        .finalize_with_result(id, done(Outcome::Succeeded), 9, |_| json!({"x": 1.5}))
        .unwrap_err();
    assert!(matches!(e, StorageError::Invalid(_)), "{e:?}");
    let rec = w.store.get(id).unwrap().unwrap();
    assert_eq!(
        (rec.state, rec.termination),
        (AttemptState::Finalizing, None)
    );
    assert_eq!(w.store.get_result(id).unwrap(), None);
    // Both at once.
    let (fin, result, fresh) = w
        .store
        .finalize_with_result(id, done(Outcome::Succeeded), 9, of)
        .unwrap();
    assert_eq!(
        (fin, fresh),
        (Finalized::Won(done(Outcome::Succeeded)), true)
    );
    assert_eq!(result, of(&done(Outcome::Succeeded)));
    assert_eq!(w.store.get_result(id).unwrap(), Some(result.clone()));
    // Again, with another termination: the first holds, and so does its result.
    let (fin, again, fresh) = w
        .store
        .finalize_with_result(id, done(Outcome::Failed), 10, of)
        .unwrap();
    assert_eq!(
        (fin, fresh),
        (Finalized::AlreadyTerminal(done(Outcome::Succeeded)), false)
    );
    assert_eq!(again, result);
    // An attempt whose termination was fixed alone gets its result built
    // from that termination.
    let b = accepted(&w);
    let id = &b.attempt_id;
    drive(&w, id, &[AttemptState::Preparing, AttemptState::Finalizing]);
    w.store.finalize(id, done(Outcome::Failed), 9).unwrap();
    let (fin, result, fresh) = w
        .store
        .finalize_with_result(id, done(Outcome::Succeeded), 10, of)
        .unwrap();
    assert_eq!((fin.won(), fresh), (false, true));
    assert_eq!(result, of(&done(Outcome::Failed)));
}

/// Take the task's attempt off `w`: placed there, retire asked for with
/// `reason`, and confirmed.
fn taken_off(store: &CoordinatorStore, task: &TaskId, w: &str, reason: &str) -> AttemptId {
    let a = placed(place_on(store, task, w, 1000)).attempt_id;
    store.begin_retire(&a, reason, 20).unwrap();
    store.confirm_retire(&a, 21).unwrap();
    a
}

#[test]
fn only_a_confirmed_retire_with_the_given_reason_excludes_its_worker() {
    let (_d, store, _run, ids) = queue_fx(3);
    let of = |task: &TaskId, reason: &str| -> Vec<String> {
        let all = store.excluded_workers(task, reason).unwrap();
        all.iter().map(|w| w.to_string()).collect()
    };
    taken_off(&store, &ids[0], "w-b", "cannot");
    taken_off(&store, &ids[0], "w-a", "cannot");
    taken_off(&store, &ids[0], "w-c", "move");
    assert_eq!(of(&ids[0], "cannot"), ["w-a", "w-b"]);
    assert_eq!(of(&ids[0], "move"), ["w-c"]);
    assert_eq!(of(&ids[0], "other"), Vec::<String>::new());
    // Asked for and not confirmed: the attempt is still there.
    let asked = placed(place_on(&store, &ids[1], "w-a", 1000)).attempt_id;
    store.begin_retire(&asked, "cannot", 20).unwrap();
    assert_eq!(of(&ids[1], "cannot"), Vec::<String>::new());
    store.confirm_retire(&asked, 21).unwrap();
    assert_eq!(of(&ids[1], "cannot"), ["w-a"]);
    // Another task's attempts say nothing about this one.
    assert_eq!(of(&ids[2], "cannot"), Vec::<String>::new());
}

#[test]
fn the_scan_puts_the_tasks_none_of_the_given_workers_may_take_after_the_others() {
    let (_d, store, _run, ids) = queue_fx(261);
    // 0..258: taken off w-a for the reason. 258: taken off w-a for another
    // reason. 259: taken off w-b for the reason. 260: never placed.
    for task in &ids[..258] {
        taken_off(&store, task, "w-a", "cannot");
    }
    taken_off(&store, &ids[258], "w-a", "move");
    taken_off(&store, &ids[259], "w-b", "cannot");
    let scan = |workers: &[&str], reason: &str| -> Vec<TaskId> {
        let workers: Vec<WorkerId> = workers.iter().map(|w| worker(w)).collect();
        let q = store.queued_tasks_for(256, &workers, reason).unwrap();
        assert_eq!(q.len(), 256);
        q.into_iter().map(|t| t.task_id).collect()
    };
    let by_age = ids[..256].to_vec();
    // Nobody is asked about: by age, as the plain scan.
    assert_eq!(scan(&[], "cannot"), by_age);
    let plain = store.queued_tasks(256).unwrap();
    assert_eq!(
        plain.into_iter().map(|t| t.task_id).collect::<Vec<_>>(),
        by_age
    );
    // For w-a alone the 258 wait behind the three it may take.
    let mut behind = ids[258..].to_vec();
    behind.extend_from_slice(&ids[..253]);
    assert_eq!(scan(&["w-a"], "cannot"), behind);
    // Another reason excludes nobody.
    assert_eq!(scan(&["w-a"], "other"), by_age);
    // With a worker that may take them they are back in their place: only a
    // task every one of the workers is excluded for goes behind.
    assert_eq!(scan(&["w-a", "w-b"], "cannot"), by_age);
    assert_eq!(scan(&["w-b", "w-c"], "cannot"), by_age);
    taken_off(&store, &ids[0], "w-b", "cannot");
    let without_first = ids[1..257].to_vec();
    assert_eq!(scan(&["w-a", "w-b"], "cannot"), without_first);
    assert_eq!(scan(&["w-b", "w-a"], "cannot"), without_first);
    // w-b alone: the first and 259 go behind everything, which is beyond
    // the scan.
    assert_eq!(scan(&["w-b"], "cannot"), without_first);
    // The last place is theirs when there is room for it.
    let all = store
        .queued_tasks_for(1000, &[worker("w-b")], "cannot")
        .unwrap();
    let tail: Vec<TaskId> = all[259..].iter().map(|t| t.task_id.clone()).collect();
    assert_eq!(tail, vec![ids[0].clone(), ids[259].clone()]);
}

#[test]
fn a_retire_and_its_note_are_written_together_or_not_at_all() {
    let (_d, store, _run, ids) = queue_fx(2);
    let a = placed(place_on(&store, &ids[0], "w-a", 9)).attempt_id;
    let note = json!({"worker": "w-a"});
    let noted = |a: &AttemptId| store.events_of(a, "noted").unwrap().len();
    assert!(store
        .begin_retire_noting(&a, "cannot", Some(("noted", &note)), 20)
        .unwrap());
    assert_eq!(noted(&a), 1);
    assert_eq!(store.events_of(&a, "noted").unwrap()[0].payload, note);
    assert_eq!(store.events_of(&a, "retire_requested").unwrap().len(), 1);
    // Asked again: nothing more is written.
    assert!(!store
        .begin_retire_noting(&a, "cannot", Some(("noted", &note)), 21)
        .unwrap());
    assert_eq!(noted(&a), 1);
    assert_eq!(store.events_of(&a, "retire_requested").unwrap().len(), 1);
    assert_eq!(store.retire_tombstone(&a).unwrap(), Some((20, None)));
    // A retire that was asked for with another reason keeps its reason, and
    // the note is not written: the worker is not excluded.
    let b = placed(place_on(&store, &ids[1], "w-a", 9)).attempt_id;
    store.begin_retire(&b, "move", 20).unwrap();
    assert!(!store
        .begin_retire_noting(&b, "cannot", Some(("noted", &note)), 21)
        .unwrap());
    assert_eq!(noted(&b), 0);
    store.confirm_retire(&b, 22).unwrap();
    assert!(store
        .excluded_workers(&ids[1], "cannot")
        .unwrap()
        .is_empty());
    // Without a note it is the plain retire.
    assert!(!store.begin_retire_noting(&b, "move", None, 23).unwrap());
    // An attempt nobody knows.
    let e = store
        .begin_retire_noting(&AttemptId::generate(), "cannot", None, 1)
        .unwrap_err();
    assert_eq!(e.code(), ErrorCode::NotFound);
}

#[test]
fn bounded_query_index_migrates_only_with_owner_and_preserves_rows() {
    use super::policy::Access;
    let dir = crate::test_tmp::tempdir().unwrap();
    let path = dir.path().join("coordinator.db");
    let all = schema::steps(DbKind::Coordinator);
    assert_eq!(all.len(), 11);
    let cluster = ClusterId::generate();
    let co = CoordinatorId::generate();
    let run = RunId::generate();
    let tasks = (0..3).map(|_| TaskId::generate()).collect::<Vec<_>>();
    {
        let old = db::open_with(
            &path,
            "coordinator",
            &all[..3],
            schema::compat(3),
            Access::ReadWrite,
        )
        .unwrap();
        let (cluster, co, run, tasks) = (cluster.clone(), co.clone(), run.clone(), tasks.clone());
        old.write(move |tx|{
            tx.execute("INSERT INTO cluster VALUES (?1,?2,1)",rusqlite::params![cluster.as_str(),co.as_str()])?;
            tx.execute("INSERT INTO coordinator_state VALUES (1,?1,1)",[cluster.as_str()])?;
            tx.execute("INSERT INTO run (run_id,spec_json,created_at_ms) VALUES (?1,'{}',1)",[run.as_str()])?;
            for task in tasks {tx.execute("INSERT INTO task (task_id,run_id,spec_json,created_at_ms) VALUES (?1,?2,'{}',1)",rusqlite::params![task.as_str(),run.as_str()])?;}
            Ok(())
        }).unwrap();
    }
    let schema_info = |store: &CoordinatorStore| {
        store
            .db
            .call(|c| {
                Ok((
                    c.query_row(
                        "SELECT value FROM schema_meta WHERE key='schema_version'",
                        [],
                        |r| r.get::<_, String>(0),
                    )?,
                    c.query_row(
                        "SELECT sql FROM sqlite_master WHERE name='task_run_key'",
                        [],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?,
                    c.query_row("SELECT count(*) FROM task", [], |r| r.get::<_, i64>(0))?,
                ))
            })
            .unwrap()
    };
    {
        let ro = CoordinatorStore::open_read_only(dir.path()).unwrap();
        assert_eq!(schema_info(&ro), ("3".into(), None, 3));
        assert_eq!(
            ro.query_tasks(run.clone(), 64, None).unwrap().items.len(),
            3
        );
        assert_eq!(schema_info(&ro), ("3".into(), None, 3));
    }
    let owner = CoordinatorStore::open(dir.path(), 2).unwrap();
    let migrated = schema_info(&owner);
    assert_eq!(migrated.0, "11");
    assert_eq!(migrated.2, 3);
    assert!(migrated.1.is_some());
    let target_run = run.to_string();
    let plan=owner.db.call(move |c|{
        let mut q=c.prepare("EXPLAIN QUERY PLAN SELECT task_id FROM task WHERE run_id=?1 AND task_id>coalesce(NULL,'') ORDER BY task_id LIMIT 64")?;
        let rows=q.query_map([target_run],|r|r.get::<_,String>(3))?.collect::<std::result::Result<Vec<_>,_>>()?;Ok(rows)
    }).unwrap();
    assert!(
        plan.iter().any(|line| line.contains("task_run_key")),
        "{plan:?}"
    );
    let fresh = crate::test_tmp::tempdir().unwrap();
    let new = CoordinatorStore::open(fresh.path(), 1).unwrap();
    assert_eq!(schema_info(&new).1, migrated.1);
    assert_eq!(
        owner.query_tasks(run, 64, None).unwrap().items.len(),
        tasks.len()
    );
}

#[test]
fn run_cancel_intent_precedes_enqueue_and_every_creation_and_place_gate() {
    use crate::service::completion::Target;
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let run = RunId::generate();
    let task = TaskId::generate();
    store.cancel_run(&run, 2).unwrap();
    store.cancel_run(&run, 3).unwrap();
    let before = store.completion(Target::Run(run.clone())).unwrap();
    assert!(!before.accepted);
    assert!(before.cancel_requested);
    assert!(before.stop_confirmed());
    assert_eq!(before.exit_code(), Some(130));
    store
        .enqueue(&run, None, &json!({}), &[(task.clone(), json!({}))], 4)
        .unwrap();
    let next = TaskId::generate();
    store.create_task(&next, &run, &json!({}), 5).unwrap();
    assert!(store.queued_tasks(10).unwrap().is_empty());
    for task in [&task, &next] {
        assert!(matches!(
            store
                .place(
                    task,
                    &WorkerId::parse("local").unwrap(),
                    &AttemptId::generate(),
                    |_| true,
                    |_| panic!("cancelled task reached payload construction"),
                    6
                )
                .unwrap(),
            PlaceOutcome::NotQueued
        ));
        assert!(store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: task.clone(),
                    worker_id: WorkerId::parse("local").unwrap(),
                    payload_hash: hash('a'),
                    permit_id: None
                },
                6
            )
            .is_err());
    }
    let task_id = task.to_string();
    store
        .db
        .write(move |tx| {
            tx.execute(
                "UPDATE task SET cancel_requested_at_ms=NULL WHERE task_id=?1",
                [task_id],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(store.task_cancel_requested(&task).unwrap().is_some());
    let after = store.completion(Target::Run(run)).unwrap();
    assert_eq!(after.task_count, 2);
    assert_eq!(after.computed_tasks, 2);
    assert_eq!(after.exit_code(), Some(130));
}

#[test]
fn run_cancel_is_atomic_idempotent_and_preserves_unknown_and_natural_completion() {
    use crate::service::completion::Target;
    let f = fx();
    let attempt = f.attempt('a').unwrap();
    f.store
        .set_observation(&attempt.attempt_id, Observation::Unknown)
        .unwrap();
    f.store.cancel_run(&f.run, 20).unwrap();
    f.store.cancel_run(&f.run, 21).unwrap();
    let snapshot = f.store.completion(Target::Run(f.run.clone())).unwrap();
    assert!(snapshot.cancel_requested);
    assert_eq!(snapshot.uncertain_tasks, 1);
    assert!(!snapshot.stop_confirmed());
    assert_eq!(snapshot.exit_code(), None);
    assert_eq!(
        f.store
            .get_attempt(&attempt.attempt_id)
            .unwrap()
            .unwrap()
            .cancel_requested_at_ms,
        Some(20)
    );
    let events = f
        .store
        .events_of(&attempt.attempt_id, "cancel_requested")
        .unwrap();
    assert_eq!(events.len(), 1);
    f.to_finalizing(&attempt.attempt_id);
    f.store
        .finalize_attempt(&attempt.attempt_id, done(Outcome::Succeeded), 30)
        .unwrap();
    let snapshot = f.store.completion(Target::Task(f.task.clone())).unwrap();
    assert!(snapshot.stop_confirmed());
    assert_eq!(snapshot.collection_pending_tasks, 1);
    assert_eq!(snapshot.exit_code(), None);
    let b = TaskId::generate();
    f.store.create_task(&b, &f.run, &json!({}), 31).unwrap();
    let snapshot = f.store.completion(Target::Run(f.run.clone())).unwrap();
    assert_eq!(snapshot.task_count, 2);
    assert!(snapshot.stop_confirmed());
    assert_eq!(snapshot.first_compute_failure.unwrap().termination, None);
}

#[test]
fn run_cancel_races_enqueue_and_place_without_losing_the_intent() {
    use crate::service::completion::Target;
    use std::sync::{Arc, Barrier};
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = Arc::new(CoordinatorStore::open(dir.path(), 1).unwrap());
    for _ in 0..8 {
        let run = RunId::generate();
        let task = TaskId::generate();
        let barrier = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                store.cancel_run(&run, 5).unwrap();
            });
            scope.spawn(|| {
                barrier.wait();
                store
                    .enqueue(&run, None, &json!({}), &[(task.clone(), json!({}))], 4)
                    .unwrap();
            });
        });
        assert_eq!(
            store.completion(Target::Run(run)).unwrap().exit_code(),
            Some(130)
        );
        assert!(store.task_cancel_requested(&task).unwrap().is_some());
        let run = RunId::generate();
        let task = TaskId::generate();
        store
            .enqueue(&run, None, &json!({}), &[(task.clone(), json!({}))], 6)
            .unwrap();
        let barrier = Barrier::new(2);
        let attempt = AttemptId::generate();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                store.cancel_run(&run, 8).unwrap();
            });
            scope.spawn(|| {
                barrier.wait();
                store
                    .place(
                        &task,
                        &WorkerId::parse("local").unwrap(),
                        &attempt,
                        |_| true,
                        |_| Ok((hash('a'), json!({}))),
                        7,
                    )
                    .unwrap();
            });
        });
        assert!(store.task_cancel_requested(&task).unwrap().is_some());
        if let Some(attempt) = store.get_attempt(&attempt).unwrap() {
            assert_eq!(attempt.cancel_requested_at_ms, Some(8));
            assert!(attempt.termination.is_none());
        }
        assert!(store.queued_tasks(1000).unwrap().is_empty());
    }
}
#[test]
fn completion_waits_for_closed_collection_and_preserves_compute_error_precedence() {
    use crate::results::plan::Verdict;
    use crate::service::completion::Target;
    for outcome in [Outcome::Succeeded, Outcome::Failed] {
        for verdict in [Verdict::Complete, Verdict::RequiredMissing, Verdict::GaveUp] {
            let f = fx();
            let attempt = f.attempt('a').unwrap();
            f.to_finalizing(&attempt.attempt_id);
            f.store
                .finalize_attempt(&attempt.attempt_id, done(outcome), 30)
                .unwrap();
            let target = Target::Task(f.task.clone());
            assert_eq!(
                f.store.completion(target.clone()).unwrap().exit_code(),
                None
            );
            let digest = Digest::of_bytes(b"result");
            f.store
                .begin_collection(&attempt.attempt_id, &digest, &json!({}), &[], &[], 31)
                .unwrap();
            assert_eq!(
                f.store.completion(target.clone()).unwrap().exit_code(),
                None,
                "result receipt alone isn't closed collection"
            );
            f.store
                .close_collection(&attempt.attempt_id, Some(&digest), verdict, false, 32)
                .unwrap();
            let snapshot = f.store.completion(target).unwrap();
            assert!(snapshot.settled());
            assert_eq!(snapshot.collection_pending_tasks, 0);
            assert_eq!(
                snapshot.exit_code(),
                Some(if outcome == Outcome::Failed {
                    1
                } else if verdict == Verdict::Complete {
                    0
                } else {
                    78
                })
            );
        }
    }
}

#[test]
fn eligible_queue_filter_accepts_large_worker_and_run_unions_without_sql_variable_growth() {
    let (_dir, store, run, tasks) = queue_fx(1);
    let workers: Vec<_> = (0..2000).map(|i| worker(&format!("worker-{i}"))).collect();
    let mut runs: Vec<_> = (0..2000).map(|_| RunId::generate()).collect();
    assert!(store
        .queued_tasks_eligible(10, &workers, "cannot", Some(&runs))
        .unwrap()
        .is_empty());
    runs.push(run);
    let queued = store
        .queued_tasks_eligible(10, &workers, "cannot", Some(&runs))
        .unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].task_id, tasks[0]);
    assert!(store
        .queued_tasks_eligible(10, &workers, "cannot", Some(&[]))
        .unwrap()
        .is_empty());
}

#[test]
fn retry_same_run_receipt_fences_cancel_and_preserves_history() {
    use crate::service::completion::Target;
    let f = fx();
    let source = SourceId::generate();
    f.store
        .put_source_ref(
            &source,
            &ManifestDigest::parse(&format!("sha256:{}", "a".repeat(64))).unwrap(),
            1,
        )
        .unwrap();
    let source_text = source.to_string();
    let run_text = f.run.to_string();
    f.store
        .db
        .write(move |tx| {
            tx.execute(
                "UPDATE run SET source_id=?1 WHERE run_id=?2",
                rusqlite::params![source_text, run_text],
            )?;
            Ok(())
        })
        .unwrap();
    let task = f.task.to_string();
    f.store.db.write(move |tx| {
        tx.execute("UPDATE task SET spec_json=?1 WHERE task_id=?2",rusqlite::params![json!({"manifest_digest":format!("sha256:{}","a".repeat(64)),"has_secret_bundle":false}).to_string(),task])?;
        Ok(())
    }).unwrap();
    let attempt = f.attempt('a').unwrap();
    f.to_finalizing(&attempt.attempt_id);
    f.store
        .finalize_attempt(&attempt.attempt_id, done(Outcome::Failed), 30)
        .unwrap();
    f.store
        .cancel_control(
            Target::Run(f.run.clone()),
            0,
            RunId::generate().to_string(),
            11,
        )
        .unwrap();
    let target = Target::Run(f.run.clone());
    let receipt = f
        .store
        .retry(target.clone(), 1, "request-one".into(), 12, |_, _, _| {
            Ok(false)
        })
        .unwrap();
    assert_eq!(receipt.control_revision, 2);
    assert_eq!(receipt.task_count, 1);
    let snapshot = f.store.completion(target.clone()).unwrap();
    assert_eq!(snapshot.control_revision, 2);
    assert!(!snapshot.cancel_requested);
    assert!(!snapshot.stop_confirmed());
    assert!(f.store.latest_attempt_of_task(&f.task).unwrap().is_none());
    assert!(f
        .store
        .query_task(f.task.clone())
        .unwrap()
        .unwrap()
        .current_attempt
        .is_none());
    assert_eq!(f.store.queued_tasks(10).unwrap().len(), 1);
    let replay = f
        .store
        .retry(target.clone(), 1, "request-one".into(), 13, |_, _, _| {
            panic!("receipt must precede validation")
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(receipt).unwrap(),
        serde_json::to_value(replay).unwrap()
    );
    assert!(f
        .store
        .retry(target.clone(), 2, "request-one".into(), 14, |_, _, _| Ok(
            false
        ))
        .is_err());
    assert!(f
        .store
        .cancel_control(target.clone(), 0, RunId::generate().to_string(), 15)
        .is_err());
    assert!(f
        .store
        .cancel_control(
            Target::Task(f.task.clone()),
            0,
            RunId::generate().to_string(),
            15
        )
        .is_err());
    assert!(!f.store.completion(target.clone()).unwrap().cancel_requested);
    assert_eq!(f.store.attempts_of_task(&f.task).unwrap().len(), 1);
    assert_eq!(
        f.store
            .get_attempt(&attempt.attempt_id)
            .unwrap()
            .unwrap()
            .termination,
        Some(done(Outcome::Failed))
    );
    f.store
        .cancel_control(target, 2, RunId::generate().to_string(), 16)
        .unwrap();
    assert!(f.store.task_cancel_requested(&f.task).unwrap().is_some());
}

fn retry_fixture(outcome: Outcome) -> (Fx, AttemptRecord) {
    let f = fx();
    let source = SourceId::generate();
    f.store
        .put_source_ref(
            &source,
            &ManifestDigest::parse(&format!("sha256:{}", "a".repeat(64))).unwrap(),
            1,
        )
        .unwrap();
    let run = f.run.to_string();
    let source = source.to_string();
    f.store
        .db
        .write(move |tx| {
            tx.execute(
                "UPDATE run SET source_id=?1 WHERE run_id=?2",
                rusqlite::params![source, run],
            )?;
            Ok(())
        })
        .unwrap();
    let task = f.task.to_string();
    f.store.db.write(move |tx| {
        tx.execute("UPDATE task SET spec_json=?1 WHERE task_id=?2",rusqlite::params![json!({"manifest_digest":format!("sha256:{}","a".repeat(64)),"has_secret_bundle":false}).to_string(),task])?;
        Ok(())
    }).unwrap();
    let attempt = f.attempt('a').unwrap();
    f.to_finalizing(&attempt.attempt_id);
    let termination = if outcome == Outcome::TimedOut {
        Termination::new(
            outcome,
            None,
            Some(crate::state::TimeoutInfo {
                kind: crate::state::TimeoutKind::Command,
                phase: crate::state::TimeoutPhase::Running,
            }),
        )
        .unwrap()
    } else {
        done(outcome)
    };
    f.store
        .finalize_attempt(&attempt.attempt_id, termination, 30)
        .unwrap();
    (f, attempt)
}
#[test]
fn retry_same_run_rejects_unsafe_attempts_and_unavailable_inputs_atomically() {
    use crate::service::completion::Target;
    for observation in [Observation::Unknown, Observation::Reconciling] {
        let (f, a) = retry_fixture(Outcome::Failed);
        f.store.set_observation(&a.attempt_id, observation).unwrap();
        assert!(f
            .store
            .retry(
                Target::Task(f.task.clone()),
                0,
                "unsafe".into(),
                40,
                |_, _, _| panic!("unsafe attempt reached inputs")
            )
            .is_err());
        assert_eq!(
            f.store
                .completion(Target::Run(f.run.clone()))
                .unwrap()
                .control_revision,
            0
        );
    }
    let (f, a) = retry_fixture(Outcome::Failed);
    let before = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    assert!(matches!(
        f.store.retry(
            Target::Task(f.task.clone()),
            0,
            "missing-secret".into(),
            40,
            |_, _, _| Err(StorageError::SecretUnavailable)
        ),
        Err(StorageError::SecretUnavailable)
    ));
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .control_revision,
        0
    );
    assert!(f.store.queued_tasks(10).unwrap().is_empty());
    assert_eq!(f.store.get_attempt(&a.attempt_id).unwrap().unwrap(), before);
    let receipt = f
        .store
        .retry(
            Target::Task(f.task.clone()),
            0,
            "retained".into(),
            41,
            |_, _, _| Ok(false),
        )
        .unwrap();
    assert_eq!(receipt.task_count, 1);
    assert!(f
        .store
        .retry(
            Target::Task(f.task.clone()),
            1,
            "queued".into(),
            42,
            |_, _, _| panic!("queued retry cannot validate")
        )
        .is_err());
    let placed = f
        .store
        .place(
            &f.task,
            &f.worker,
            &AttemptId::generate(),
            |_| true,
            |input| {
                assert_eq!(input.generation, Generation(2));
                Ok((hash('b'), json!({})))
            },
            43,
        )
        .unwrap();
    assert!(matches!(placed, PlaceOutcome::Placed(_)));
    assert_eq!(f.store.attempts_of_task(&f.task).unwrap().len(), 2);
    assert!(f
        .store
        .retry(
            Target::Task(f.task.clone()),
            1,
            "live".into(),
            44,
            |_, _, _| panic!("live retry cannot validate")
        )
        .is_err());
    assert!(f
        .store
        .query_task(f.task.clone())
        .unwrap()
        .unwrap()
        .current_attempt
        .is_some());
}
#[test]
fn retry_same_run_run_selection_preserves_success_and_task_selection_retries_success() {
    use crate::service::completion::Target;
    let (f, _) = retry_fixture(Outcome::Failed);
    let success = TaskId::generate();
    f.store
        .create_task(&success, &f.run, &json!({"manifest_digest":format!("sha256:{}","a".repeat(64)),"has_secret_bundle":false}), 31)
        .unwrap();
    let a = f
        .store
        .create_attempt(
            NewAttempt {
                attempt_id: AttemptId::generate(),
                task_id: success.clone(),
                worker_id: f.worker.clone(),
                payload_hash: hash('b'),
                permit_id: None,
            },
            32,
        )
        .unwrap();
    f.to_finalizing(&a.attempt_id);
    f.store
        .finalize_attempt(&a.attempt_id, done(Outcome::Succeeded), 33)
        .unwrap();
    let before = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
    let receipt = f
        .store
        .retry(
            Target::Run(f.run.clone()),
            0,
            "mixed".into(),
            40,
            |_, _, _| Ok(false),
        )
        .unwrap();
    assert_eq!(receipt.task_count, 1);
    assert_eq!(f.store.queued_tasks(10).unwrap()[0].task_id, f.task);
    assert_eq!(f.store.get_attempt(&a.attempt_id).unwrap().unwrap(), before);
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .computed_tasks,
        1
    );
    let receipt = f
        .store
        .retry(Target::Task(success), 1, "success".into(), 41, |_, _, _| {
            Ok(false)
        })
        .unwrap();
    assert_eq!(receipt.control_revision, 2);
    assert_eq!(receipt.min_generation, Some(2));
}
#[test]
fn retry_same_run_concurrent_requests_and_owner_restart_keep_one_boundary() {
    use crate::service::completion::Target;
    use std::sync::Barrier;
    let (f, _) = retry_fixture(Outcome::Failed);
    let barrier = Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let join = scope.spawn(|| {
            barrier.wait();
            f.store.retry(
                Target::Task(f.task.clone()),
                0,
                "first".into(),
                40,
                |_, _, _| Ok(false),
            )
        });
        barrier.wait();
        let other = f.store.retry(
            Target::Task(f.task.clone()),
            0,
            "second".into(),
            40,
            |_, _, _| Ok(false),
        );
        [join.join().unwrap(), other]
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    let receipt = results.into_iter().find_map(Result::ok).unwrap();
    let Fx {
        _dir: dir,
        store,
        run,
        task,
        ..
    } = f;
    drop(store);
    let owner = CoordinatorStore::open(dir.path(), 50).unwrap();
    let replay = owner
        .retry(
            Target::Task(task),
            0,
            receipt.request_id.clone(),
            51,
            |_, _, _| panic!("restart replay must not inspect inputs"),
        )
        .unwrap();
    assert_eq!(
        serde_json::to_value(receipt).unwrap(),
        serde_json::to_value(replay).unwrap()
    );
    assert_eq!(
        owner.completion(Target::Run(run)).unwrap().control_revision,
        1
    );
}
#[test]
fn retry_same_run_rejects_closed_and_unconfirmed_retirement() {
    use crate::service::completion::Target;
    let (f, a) = retry_fixture(Outcome::Failed);
    f.store
        .begin_retire(&a.attempt_id, "retry test", 31)
        .unwrap();
    assert!(f
        .store
        .retry(
            Target::Task(f.task.clone()),
            0,
            "retiring".into(),
            40,
            |_, _, _| panic!("retirement is not stop proof")
        )
        .is_err());
    let (f, _) = retry_fixture(Outcome::Failed);
    f.store.close_run(&f.run, 35).unwrap();
    assert!(f
        .store
        .retry(
            Target::Run(f.run.clone()),
            0,
            "closed".into(),
            40,
            |_, _, _| panic!("closed run inputs must not be inspected")
        )
        .is_err());
}

#[test]
fn retry_same_run_migration_preserves_v5_cancellation_tombstones() {
    use crate::service::completion::Target;
    let dir = crate::test_tmp::tempdir().unwrap();
    let run = RunId::generate();
    let steps = schema::steps(DbKind::Coordinator);
    let old = db::open_with(
        &dir.path().join("coordinator.db"),
        "coordinator",
        &steps[..5],
        schema::compat(5),
        policy::Access::ReadWrite,
    )
    .unwrap();
    let old_run = run.to_string();
    old.write(move |tx| {
        tx.execute(
            "INSERT INTO run_cancel_intent(run_id,requested_at_ms) VALUES(?1,7)",
            [old_run],
        )?;
        Ok(())
    })
    .unwrap();
    drop(old);
    let owner = CoordinatorStore::open(dir.path(), 8).unwrap();
    let before = owner.completion(Target::Run(run.clone())).unwrap();
    assert_eq!(before.control_revision, 0);
    assert!(before.cancel_requested);
    let task = TaskId::generate();
    owner
        .enqueue(&run, None, &json!({}), &[(task.clone(), json!({}))], 9)
        .unwrap();
    assert_eq!(owner.task_cancel_requested(&task).unwrap(), Some(7));
    assert!(owner.queued_tasks(10).unwrap().is_empty());
}
#[test]
fn retry_same_run_keeps_old_collection_and_atomic_run_selection() {
    use crate::results::plan::Verdict;
    use crate::service::completion::Target;
    let (f, old) = retry_fixture(Outcome::Failed);
    let digest = Digest::of_bytes(b"result");
    f.store
        .begin_collection(&old.attempt_id, &digest, &json!({}), &[], &[], 31)
        .unwrap();
    let next = TaskId::generate();
    f.store.create_task(&next, &f.run, &json!({}), 32).unwrap();
    assert!(f
        .store
        .retry(
            Target::Run(f.run.clone()),
            0,
            "atomic".into(),
            40,
            |_, _, _| Ok(false)
        )
        .is_err());
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .control_revision,
        0
    );
    let receipt = f
        .store
        .retry(
            Target::Task(f.task.clone()),
            0,
            "one".into(),
            41,
            |_, _, _| Ok(false),
        )
        .unwrap();
    assert_eq!(receipt.control_revision, 1);
    f.store
        .close_collection(&old.attempt_id, Some(&digest), Verdict::Complete, false, 42)
        .unwrap();
    let completion = f.store.completion(Target::Task(f.task.clone())).unwrap();
    assert_eq!(completion.computed_tasks, 0);
    assert_eq!(completion.collection_pending_tasks, 0);
    assert!(!completion.settled());
    assert_eq!(
        f.store
            .get_attempt(&old.attempt_id)
            .unwrap()
            .unwrap()
            .termination,
        Some(done(Outcome::Failed))
    );
}

#[test]
fn retry_same_run_rejects_integer_exhaustion_and_delayed_initial_submit() {
    use crate::service::completion::Target;
    for revision_exhausted in [false, true] {
        let (f, a) = retry_fixture(Outcome::Failed);
        let run = f.run.to_string();
        let attempt = a.attempt_id.to_string();
        f.store
            .db
            .write(move |tx| {
                if revision_exhausted {
                    tx.execute(
                        "UPDATE run SET control_revision=?1 WHERE run_id=?2",
                        rusqlite::params![i64::MAX, run],
                    )?;
                } else {
                    tx.execute(
                        "UPDATE attempt SET generation=?1 WHERE attempt_id=?2",
                        rusqlite::params![i64::MAX, attempt],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        let expected = if revision_exhausted {
            i64::MAX as u64
        } else {
            0
        };
        assert!(f
            .store
            .retry(
                Target::Run(f.run.clone()),
                expected,
                "exhausted".into(),
                40,
                |_, _, _| panic!("exhausted retry reached inputs")
            )
            .is_err());
        assert_eq!(
            f.store
                .completion(Target::Run(f.run.clone()))
                .unwrap()
                .control_revision,
            expected
        );
    }
    let (f, _) = retry_fixture(Outcome::Failed);
    f.store
        .retry(
            Target::Run(f.run.clone()),
            0,
            "retry".into(),
            40,
            |_, _, _| Ok(false),
        )
        .unwrap();
    let extra = TaskId::generate();
    assert!(f
        .store
        .enqueue(&f.run, None, &json!({}), &[(extra.clone(), json!({}))], 41)
        .is_err());
    assert!(f.store.query_task(extra).unwrap().is_none());
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .control_revision,
        1
    );
    assert_eq!(f.store.queued_tasks(10).unwrap().len(), 1);
}

#[test]
fn retry_same_run_fresh_cancel_races_placement_without_missing_the_attempt() {
    use crate::service::completion::Target;
    use std::sync::Barrier;
    for _ in 0..8 {
        let (f, _) = retry_fixture(Outcome::Failed);
        f.store
            .retry(
                Target::Run(f.run.clone()),
                0,
                "retry".into(),
                40,
                |_, _, _| Ok(false),
            )
            .unwrap();
        let barrier = Barrier::new(2);
        std::thread::scope(|scope| {
            let placed = scope.spawn(|| {
                barrier.wait();
                f.store
                    .place(
                        &f.task,
                        &f.worker,
                        &AttemptId::generate(),
                        |_| true,
                        |_| Ok((hash('b'), json!({}))),
                        41,
                    )
                    .unwrap()
            });
            barrier.wait();
            f.store
                .cancel_control(
                    Target::Run(f.run.clone()),
                    1,
                    RunId::generate().to_string(),
                    42,
                )
                .unwrap();
            let _ = placed.join().unwrap();
        });
        assert!(f.store.task_cancel_requested(&f.task).unwrap().is_some());
        assert!(f.store.queued_tasks(10).unwrap().is_empty());
        for current in f
            .store
            .attempts_of_task(&f.task)
            .unwrap()
            .into_iter()
            .filter(|a| a.generation.0 >= 2)
        {
            assert_eq!(current.cancel_requested_at_ms, Some(42));
        }
    }
}

#[test]
fn retry_same_run_pages_large_runs_and_keeps_receipts_constant_size() {
    use crate::service::completion::Target;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (f, _) = retry_fixture(Outcome::Failed);
    let run = f.run.to_string();
    let spec =
        json!({"manifest_digest":format!("sha256:{}","a".repeat(64)),"has_secret_bundle":false})
            .to_string();
    let termination = serde_json::to_string(&done(Outcome::Failed)).unwrap();
    let receipts = serde_json::to_string(&Receipts::default()).unwrap();
    f.store.db.write(move |tx| {
        for _ in 1..300 {
            let task=TaskId::generate().to_string();
            tx.execute("INSERT INTO task(task_id,run_id,spec_json,created_at_ms) VALUES(?1,?2,?3,1)",rusqlite::params![task,run,spec])?;
            tx.execute("INSERT INTO attempt(attempt_id,task_id,worker_id,generation,payload_hash,coordinator_epoch,state,termination_json,receipts_json,created_at_ms,terminal_at_ms) VALUES(?1,?2,'local',1,?3,1,'terminal',?4,?5,1,2)",rusqlite::params![AttemptId::generate().as_str(),task,hash('a').as_str(),termination,receipts])?;
        }
        Ok(())
    }).unwrap();
    let RetryPreparation::Prepared(prepared) = f
        .store
        .prepare_retry(Target::Run(f.run.clone()), 0, "changing-input".into())
        .unwrap()
    else {
        panic!("new request")
    };
    let run = f.run.to_string();
    let last=f.store.db.write(move |tx| {
        let last:String=tx.query_row("SELECT MAX(task_id) FROM task WHERE run_id=?1",[run],|r|r.get(0))?;
        tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.has_secret_bundle',1) WHERE task_id=?1",[&last])?;
        Ok(last)
    }).unwrap();
    assert!(f.store.commit_retry(prepared, false, 40).is_err());
    let run = f.run.to_string();
    let changed: i64 = f
        .store
        .db
        .call(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM task WHERE run_id=?1 AND min_generation<>1",
                [run],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(
        changed, 0,
        "a late-page failure must roll back earlier page updates"
    );
    f.store.db.write(move |tx|{tx.execute("UPDATE task SET spec_json=json_set(spec_json,'$.has_secret_bundle',0) WHERE task_id=?1",[last])?;Ok(())}).unwrap();
    let calls = AtomicUsize::new(0);
    let receipt = f
        .store
        .retry(
            Target::Run(f.run.clone()),
            0,
            "large-run".into(),
            41,
            |_, _, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(false)
            },
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(receipt.task_count, 300);
    assert!(receipt.task_id.is_none());
    assert!(receipt.min_generation.is_none());
    assert!(serde_json::to_vec(&receipt).unwrap().len() < 512);
    let run = f.run.to_string();
    let authorized:i64=f.store.db.call(move |c|Ok(c.query_row("SELECT count(*) FROM task WHERE run_id=?1 AND min_generation=2 AND authorization_revision=1",[run],|r|r.get(0))?)).unwrap();
    assert_eq!(authorized, 300);
}

#[test]
fn retry_same_run_accepts_each_unsuccessful_terminal_outcome_but_not_unplaced_cancel() {
    use crate::service::completion::Target;
    for outcome in [Outcome::Cancelled, Outcome::TimedOut, Outcome::Interrupted] {
        let (f, a) = retry_fixture(outcome);
        let before = f.store.get_attempt(&a.attempt_id).unwrap().unwrap();
        f.store
            .cancel_control(
                Target::Run(f.run.clone()),
                0,
                RunId::generate().to_string(),
                31,
            )
            .unwrap();
        let receipt = f
            .store
            .retry(
                Target::Run(f.run.clone()),
                1,
                "terminal".into(),
                40,
                |_, _, _| Ok(false),
            )
            .unwrap();
        assert_eq!(receipt.task_count, 1);
        assert!(
            !f.store
                .completion(Target::Run(f.run.clone()))
                .unwrap()
                .cancel_requested
        );
        assert_eq!(f.store.get_attempt(&a.attempt_id).unwrap().unwrap(), before);
    }
    let dir = crate::test_tmp::tempdir().unwrap();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let run = RunId::generate();
    let task = TaskId::generate();
    let source = SourceId::generate();
    let digest = Digest::of_bytes(b"retained");
    store.put_source_ref(&source, &digest, 1).unwrap();
    store
        .enqueue(
            &run,
            Some(&source),
            &json!({}),
            &[(
                task.clone(),
                json!({"manifest_digest":digest,"has_secret_bundle":false}),
            )],
            1,
        )
        .unwrap();
    store
        .cancel_control(
            Target::Run(run.clone()),
            0,
            RunId::generate().to_string(),
            2,
        )
        .unwrap();
    assert!(store
        .retry(
            Target::Task(task.clone()),
            1,
            "unplaced".into(),
            3,
            |_, _, _| panic!("unplaced cancellation is not stopping proof")
        )
        .is_err());
    assert_eq!(
        store.completion(Target::Run(run)).unwrap().control_revision,
        1
    );
    assert!(store.attempts_of_task(&task).unwrap().is_empty());
}

#[test]
fn control_fence_rejects_old_retry_after_cancel_and_replays_original_cancel_ack() {
    use crate::service::completion::Target;
    for task_target in [false, true] {
        let (f, _) = retry_fixture(Outcome::Failed);
        let target = if task_target {
            Target::Task(f.task.clone())
        } else {
            Target::Run(f.run.clone())
        };
        let RetryPreparation::Prepared(old_retry) = f
            .store
            .prepare_retry(target.clone(), 0, "delayed-retry".into())
            .unwrap()
        else {
            panic!("new request")
        };
        let ack = f
            .store
            .cancel_control(target.clone(), 0, "cancel-ack".into(), 40)
            .unwrap();
        assert_eq!(ack.control_revision, 1);
        assert!(f.store.commit_retry(old_retry, false, 41).is_err());
        assert!(f
            .store
            .retry(
                target.clone(),
                0,
                "delayed-transport".into(),
                41,
                |_, _, _| panic!("stale retry must not reach inputs")
            )
            .is_err());
        assert!(f.store.completion(target.clone()).unwrap().cancel_requested);
        let accepted = f
            .store
            .retry(target.clone(), 1, "fresh-retry".into(), 42, |_, _, _| {
                Ok(false)
            })
            .unwrap();
        assert_eq!(accepted.control_revision, 2);
        let replay = f
            .store
            .cancel_control(target.clone(), 0, "cancel-ack".into(), 43)
            .unwrap();
        assert_eq!(
            serde_json::to_value(ack).unwrap(),
            serde_json::to_value(replay).unwrap()
        );
        let current = f.store.completion(target.clone()).unwrap();
        assert_eq!(current.control_revision, 2);
        assert!(!current.cancel_requested);
        assert!(f
            .store
            .cancel_control(target.clone(), 0, "new-stale-cancel".into(), 44)
            .is_err());
        assert!(f
            .store
            .cancel_control(target.clone(), 1, "cancel-ack".into(), 44)
            .is_err());
        let other = if task_target {
            Target::Run(f.run.clone())
        } else {
            Target::Task(f.task.clone())
        };
        assert!(f
            .store
            .cancel_control(other, 0, "cancel-ack".into(), 44)
            .is_err());
        assert!(f
            .store
            .prepare_retry(target, 0, "cancel-ack".into())
            .is_err());
        assert_eq!(f.store.queued_tasks(10).unwrap().len(), 1);
    }
}
#[test]
fn control_fence_pre_enqueue_revision_is_inherited_and_cancel_receipt_survives_restart() {
    use crate::service::completion::Target;
    let dir = crate::test_tmp::tempdir().unwrap();
    let run = RunId::generate();
    let task = TaskId::generate();
    let store = CoordinatorStore::open(dir.path(), 1).unwrap();
    let ack = store
        .cancel_control(Target::Run(run.clone()), 0, "before-submit".into(), 2)
        .unwrap();
    assert_eq!(ack.control_revision, 1);
    assert!(!ack.accepted);
    assert!(ack.cancel_requested);
    let again = store
        .cancel_control(Target::Run(run.clone()), 0, "before-submit".into(), 3)
        .unwrap();
    assert_eq!(
        serde_json::to_value(&ack).unwrap(),
        serde_json::to_value(again).unwrap()
    );
    store
        .enqueue(&run, None, &json!({}), &[(task.clone(), json!({}))], 4)
        .unwrap();
    assert_eq!(
        store
            .completion(Target::Run(run.clone()))
            .unwrap()
            .control_revision,
        1
    );
    assert!(store.task_cancel_requested(&task).unwrap().is_some());
    assert!(store.queued_tasks(10).unwrap().is_empty());
    drop(store);
    let restarted = CoordinatorStore::open(dir.path(), 5).unwrap();
    let again = restarted
        .cancel_control(Target::Run(run), 0, "before-submit".into(), 6)
        .unwrap();
    assert_eq!(
        serde_json::to_value(ack).unwrap(),
        serde_json::to_value(again).unwrap()
    );
}
#[test]
fn control_fence_internal_fresh_cancel_advances_once_and_delivery_never_advances() {
    use crate::service::completion::Target;
    for task_target in [false, true] {
        let (f, attempt) = retry_fixture(Outcome::Failed);
        let RetryPreparation::Prepared(prepared) = f
            .store
            .prepare_retry(Target::Run(f.run.clone()), 0, "old-retry".into())
            .unwrap()
        else {
            panic!("new request")
        };
        if task_target {
            f.store.cancel_task(&f.task, 40).unwrap();
        } else {
            f.store.cancel_run(&f.run, 40).unwrap();
        }
        assert_eq!(
            f.store
                .completion(Target::Run(f.run.clone()))
                .unwrap()
                .control_revision,
            1
        );
        assert!(f.store.commit_retry(prepared, false, 41).is_err());
        f.store.request_cancel(&attempt.attempt_id, 42).unwrap();
        f.store.request_cancel(&attempt.attempt_id, 43).unwrap();
        assert_eq!(
            f.store
                .completion(Target::Run(f.run.clone()))
                .unwrap()
                .control_revision,
            1
        );
    }
}
#[test]
fn control_fence_concurrent_cancel_and_retry_have_one_winner() {
    use crate::service::completion::Target;
    use std::sync::Barrier;
    for _ in 0..8 {
        let (f, _) = retry_fixture(Outcome::Failed);
        let barrier = Barrier::new(2);
        let (cancel, retry) = std::thread::scope(|scope| {
            let pending = scope.spawn(|| {
                barrier.wait();
                f.store
                    .cancel_control(Target::Run(f.run.clone()), 0, "cancel".into(), 40)
            });
            barrier.wait();
            let retry = f.store.retry(
                Target::Run(f.run.clone()),
                0,
                "retry".into(),
                40,
                |_, _, _| Ok(false),
            );
            (pending.join().unwrap(), retry)
        });
        assert_ne!(cancel.is_ok(), retry.is_ok());
        let snapshot = f.store.completion(Target::Run(f.run.clone())).unwrap();
        assert_eq!(snapshot.control_revision, 1);
        assert_eq!(snapshot.cancel_requested, cancel.is_ok());
    }
}

#[test]
fn control_fence_cancel_overflow_and_conflicting_retry_identity_change_nothing() {
    use crate::service::completion::Target;
    let (f, _) = retry_fixture(Outcome::Failed);
    let run = f.run.to_string();
    f.store
        .db
        .write(move |tx| {
            tx.execute(
                "UPDATE run SET control_revision=?1 WHERE run_id=?2",
                rusqlite::params![i64::MAX, run],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(f
        .store
        .cancel_control(
            Target::Run(f.run.clone()),
            i64::MAX as u64,
            "overflow".into(),
            40
        )
        .is_err());
    let snapshot = f.store.completion(Target::Run(f.run.clone())).unwrap();
    assert_eq!(snapshot.control_revision, i64::MAX as u64);
    assert!(!snapshot.cancel_requested);
    let (f, _) = retry_fixture(Outcome::Failed);
    f.store
        .retry(
            Target::Run(f.run.clone()),
            0,
            "accepted-retry".into(),
            40,
            |_, _, _| Ok(false),
        )
        .unwrap();
    assert!(f
        .store
        .cancel_control(Target::Run(f.run.clone()), 0, "accepted-retry".into(), 41)
        .is_err());
    assert_eq!(
        f.store
            .completion(Target::Run(f.run.clone()))
            .unwrap()
            .control_revision,
        1
    );
    assert!(f.store.queued_tasks(10).unwrap().len() == 1);
}

#[test]
fn live_log_progress_registers_ref_cas_metadata_and_survives_final_receipt_reset() {
    use crate::wire::LogStream::{Stderr, Stdout};
    let f = fx();
    let a = f.attempt('a').unwrap().attempt_id;
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 0, 3, false, false, 0, 2)
        .is_err());
    let progress = f.store.prepare_log_progress(&a, Stdout, 2).unwrap();
    assert_eq!(progress.committed_bytes, 0);
    assert!(f.store.collection(&a).unwrap().is_none());
    let aid = a.to_string();
    f.store
        .db
        .call(move |c| {
            let row: (String, String, String) = c.query_row(
                "SELECT attempt_id,kind,locator FROM blob_ref WHERE ref_id=?1",
                [format!("{aid}:logs")],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            assert_eq!(
                row,
                (aid.clone(), "log".into(), format!("results/{aid}/logs"))
            );
            Ok(())
        })
        .unwrap();
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 0, 3, false, false, 0, 3)
        .unwrap());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 0, 3, false, false, 0, 4)
        .unwrap());
    assert_eq!(
        f.store
            .log_progress(&a, Stdout)
            .unwrap()
            .unwrap()
            .updated_at_ms,
        3
    );
    assert!(!f
        .store
        .commit_log_progress(&a, Stdout, 0, 4, false, false, 0, 4)
        .unwrap());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 3, 2, false, false, 0, 4)
        .is_err());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 3, 3, true, true, 7, 4)
        .unwrap());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 3, 3, false, true, 7, 5)
        .is_err());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 3, 4, true, true, 7, 5)
        .is_err());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 3, 3, true, false, 7, 5)
        .is_err());
    assert!(f
        .store
        .commit_log_progress(&a, Stdout, 3, 3, true, true, 6, 5)
        .is_err());
    assert_eq!(
        f.store
            .prepare_log_progress(&a, Stderr, 5)
            .unwrap()
            .committed_bytes,
        0
    );
    f.to_finalizing(&a);
    f.store
        .finalize_attempt(&a, done(Outcome::Succeeded), 6)
        .unwrap();
    f.store
        .begin_collection(&a, &Digest::of_bytes(b"one"), &json!({}), &[], &[], 7)
        .unwrap();
    f.store
        .set_collection_logs(&a, &Digest::of_bytes(b"one"), &json!({}), 8)
        .unwrap();
    f.store
        .begin_collection(&a, &Digest::of_bytes(b"two"), &json!({}), &[], &[], 9)
        .unwrap();
    assert!(f.store.collection(&a).unwrap().unwrap().logs.is_none());
    let progress = f.store.log_progress(&a, Stdout).unwrap().unwrap();
    assert_eq!(progress.committed_bytes, 3);
    assert!(progress.eof);
}

#[test]
fn live_log_prepare_rolls_back_on_pending_delete_or_mismatched_reference() {
    use crate::wire::LogStream::Stdout;
    let f = fx();
    let a = f.attempt('a').unwrap().attempt_id;
    let locator = crate::results::ResultStore::logs_locator(&a);
    let l = locator.clone();
    f.store
        .db
        .write(move |tx| {
            tx.execute(
                "INSERT INTO pending_delete(locator,queued_at_ms) VALUES(?1,1)",
                [l],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        f.store.prepare_log_progress(&a, Stdout, 2),
        Err(StorageError::LocatorPendingDelete(_))
    ));
    assert!(f.store.log_progress(&a, Stdout).unwrap().is_none());
    let aid = a.to_string();
    f.store.db.write(move |tx| {tx.execute("DELETE FROM pending_delete WHERE locator=?1",[locator])?;tx.execute("INSERT INTO blob_ref(ref_id,attempt_id,kind,locator,created_at_ms) VALUES(?1,?2,'result','other',1)",rusqlite::params![format!("{aid}:logs"),aid])?;Ok(())}).unwrap();
    assert!(matches!(
        f.store.prepare_log_progress(&a, Stdout, 3),
        Err(StorageError::Immutable(_))
    ));
    assert!(f.store.log_progress(&a, Stdout).unwrap().is_none());
}

#[test]
fn live_log_target_pages_are_bounded_current_and_payload_backed() {
    let (_dir, store, _run, tasks) = queue_fx(68);
    let worker = worker("w-a");
    let mut attempts = Vec::new();
    for (n, task) in tasks.iter().enumerate() {
        let a = AttemptId::generate();
        let record = placed(
            store
                .place(
                    task,
                    &worker,
                    &a,
                    |_| true,
                    move |_| {
                        Ok((
                            hash('a'),
                            if n == 67 {
                                json!({})
                            } else {
                                json!({"payload":{"nodes":[]}})
                            },
                        ))
                    },
                    10,
                )
                .unwrap(),
        );
        if n != 66 {
            store
                .mark_receipt(&record.attempt_id, Receipt::WorkerAccepted, 11)
                .unwrap();
        }
        attempts.push(record.attempt_id);
    }
    let first = store.query_live_log_targets(&worker, None, 1000).unwrap();
    assert_eq!(first.len(), 64);
    assert!(first
        .windows(2)
        .all(|w| w[0].record.attempt_id < w[1].record.attempt_id));
    let second = store
        .query_live_log_targets(&worker, Some(&first[63].record.attempt_id), 1000)
        .unwrap();
    assert_eq!(second.len(), 2);
    assert!(store
        .query_live_log_targets(&worker, Some(&second[1].record.attempt_id), 64)
        .unwrap()
        .is_empty());
    assert!(store
        .query_live_log_targets(&worker, None, 0)
        .unwrap()
        .is_empty());
    assert!(store
        .query_live_log_targets(&WorkerId::parse("other").unwrap(), None, 64)
        .unwrap()
        .is_empty());
    store.begin_retire(&attempts[0], "replaced", 12).unwrap();
    store.confirm_retire(&attempts[0], 13).unwrap();
    let all = store.query_live_log_targets(&worker, None, 64).unwrap();
    assert!(!all.iter().any(|t| t.record.attempt_id == attempts[0]));
}

#[test]
fn live_log_legacy_bootstrap_needs_matching_final_receipt_and_keeps_stream_metadata() {
    use crate::wire::LogStream::Stdout;
    let f = fx();
    let a = f.attempt('a').unwrap().attempt_id;
    f.to_finalizing(&a);
    f.store
        .finalize_attempt(&a, done(Outcome::Succeeded), 21)
        .unwrap();
    let files = crate::results::ResultStore::open(&f._dir.path().join("results")).unwrap();
    files.append_log(&a, Stdout, 0, b"bytes").unwrap();
    let digest = Digest::of_bytes(b"bytes");
    let verified = || {
        files
            .verify_legacy_log_prefix(&a, Stdout, &digest, 5)
            .unwrap()
            .unwrap()
    };
    assert!(f
        .store
        .bootstrap_log_progress(verified(), 22)
        .unwrap()
        .is_none());
    let result = Digest::of_bytes(b"result");
    f.store
        .begin_collection(&a, &result, &json!({}), &[], &[], 23)
        .unwrap();
    f.store.set_collection_logs(&a,&result,&json!({"stdout":{"sha256":digest.as_str(),"bytes":4,"complete":true,"log_truncated":true,"dropped_bytes":8}}),24).unwrap();
    assert!(f
        .store
        .bootstrap_log_progress(verified(), 25)
        .unwrap()
        .is_none());
    f.store.set_collection_logs(&a,&result,&json!({"stdout":{"sha256":digest.as_str(),"bytes":5,"complete":false,"log_truncated":true,"dropped_bytes":8}}),26).unwrap();
    let p = f
        .store
        .bootstrap_log_progress(verified(), 27)
        .unwrap()
        .unwrap();
    assert_eq!(p.committed_bytes, 5);
    assert!(!p.eof);
    assert!(p.log_truncated);
    assert_eq!(p.dropped_bytes, 8);
    assert_eq!(
        f.store
            .bootstrap_log_progress(verified(), 28)
            .unwrap()
            .unwrap(),
        p
    );
    assert!(f
        .store
        .collection(&a)
        .unwrap()
        .unwrap()
        .closed_at_ms
        .is_none());
}

#[test]
fn live_log_v6_migration_preserves_final_receipt_and_bootstraps_verified_bytes() {
    use crate::wire::LogStream::Stdout;
    let f = fx();
    let a = f.attempt('a').unwrap().attempt_id;
    f.to_finalizing(&a);
    f.store
        .finalize_attempt(&a, done(Outcome::Succeeded), 21)
        .unwrap();
    let files = crate::results::ResultStore::open(&f._dir.path().join("results")).unwrap();
    files.append_log(&a, Stdout, 0, b"old").unwrap();
    let result = Digest::of_bytes(b"result");
    let digest = Digest::of_bytes(b"old");
    let receipt = json!({"stdout":{"sha256":digest.as_str(),"bytes":3,"complete":true,"log_truncated":false,"dropped_bytes":0}});
    f.store
        .begin_collection(&a, &result, &json!({"old":true}), &[], &[], 22)
        .unwrap();
    f.store
        .set_collection_logs(&a, &result, &receipt, 23)
        .unwrap();
    // Exactly the pre-progress schema: no rows or constraints are skipped.
    f.store.db.write(|tx|{tx.execute_batch("DROP TRIGGER result_gc_ref_insert; DROP TABLE result_gc_ref; DROP TABLE result_delete_claim; DROP TABLE result_gc_clock; DROP INDEX attempt_task_key; DROP TABLE collection_log; DROP INDEX event_task_waiting; DROP INDEX placement_unacked; DROP INDEX attempt_worker_live; DELETE FROM schema_meta WHERE key IN ('execution_requirements_v1','frozen_toolchains_v1'); UPDATE schema_meta SET value='6' WHERE key='schema_version';")?;Ok(())}).unwrap();
    drop(f.store);
    let owner = CoordinatorStore::open(f._dir.path(), 24).unwrap();
    assert_eq!(owner.collection(&a).unwrap().unwrap().logs, Some(receipt));
    assert!(owner.log_progress(&a, Stdout).unwrap().is_none());
    let token = files
        .verify_legacy_log_prefix(&a, Stdout, &digest, 3)
        .unwrap()
        .unwrap();
    let progress = owner.bootstrap_log_progress(token, 25).unwrap().unwrap();
    assert_eq!(progress.committed_bytes, 3);
    assert!(progress.eof);
    let reader = CoordinatorStore::open_read_only(f._dir.path()).unwrap();
    assert_eq!(reader.epoch(), owner.epoch());
    assert_eq!(reader.log_progress(&a, Stdout).unwrap(), Some(progress));
}

#[test]
fn live_log_append_crash_replays_from_committed_offset_without_publishing_tail() {
    use crate::results::store::{Crash, StoreError};
    use crate::wire::LogStream::Stdout;
    let f = fx();
    let a = f.attempt('a').unwrap().attempt_id;
    let files = crate::results::ResultStore::open(&f._dir.path().join("results")).unwrap();
    let reader = crate::results::ResultStore::open_read_only(files.root()).unwrap();
    assert_eq!(
        f.store
            .prepare_log_progress(&a, Stdout, 21)
            .unwrap()
            .committed_bytes,
        0
    );
    files.append_log(&a, Stdout, 0, b"first").unwrap();
    f.store
        .commit_log_progress(&a, Stdout, 0, 5, false, false, 0, 22)
        .unwrap();
    files.crash_once_at(Crash::LogAppended);
    assert!(matches!(
        files.append_log(&a, Stdout, 5, b"second"),
        Err(StoreError::Crash(Crash::LogAppended))
    ));
    let progress = f.store.log_progress(&a, Stdout).unwrap().unwrap();
    assert_eq!(progress.committed_bytes, 5);
    assert_eq!(
        reader
            .read_range(&a, Stdout, progress.committed_bytes, 0, 100)
            .unwrap(),
        b"first"
    );
    let resumed = crate::results::ResultStore::open(files.root()).unwrap();
    resumed
        .recover_log_prefix(&a, Stdout, progress.committed_bytes)
        .unwrap();
    resumed.append_log(&a, Stdout, 5, b"second").unwrap();
    f.store
        .commit_log_progress(&a, Stdout, 5, 11, false, false, 0, 23)
        .unwrap();
    assert_eq!(
        reader.read_range(&a, Stdout, 11, 0, 100).unwrap(),
        b"firstsecond"
    );
}

// ================================================= guarded result retention
fn retention_ready(f: &Fx) -> AttemptId {
    let a = f.attempt('a').unwrap().attempt_id;
    f.to_finalizing(&a);
    f.store
        .finalize_attempt(&a, done(Outcome::Failed), 100)
        .unwrap();
    f.store.mark_collected(&a, 101).unwrap();
    let id = a.to_string();
    f.store.db.write(move |tx| {tx.execute("INSERT INTO collection(attempt_id,verdict,acked_at_ms,closed_at_ms,updated_at_ms) VALUES(?1,'complete',102,101,102)",[id])?;Ok(())}).unwrap();
    a
}
#[test]
fn result_retention_claims_bounded_pages_and_replays_only_known_claims() {
    use crate::results::store::ResultStore;
    let f = fx();
    let a = retention_ready(&f);
    let files = ResultStore::open(&f._dir.path().join("results")).unwrap();
    files.write_result(&a, b"{}").unwrap();
    let locator = ResultStore::result_locator(&a);
    for n in 0..70 {
        f.store
            .add_blob_ref(
                &blob(&format!("result-{n:03}"), &a, BlobKind::Result, &locator),
                102,
            )
            .unwrap();
    }
    let run = f.run.to_string();
    let plan=f.store.db.call(move |c| {let mut s=c.prepare("EXPLAIN QUERY PLAN SELECT b.ref_id,b.attempt_id,b.locator,b.pins FROM result_gc_ref g JOIN blob_ref b USING(ref_id) WHERE g.run_id=?1 AND g.ref_id>?2 ORDER BY g.ref_id LIMIT 32")?;let rows=s.query_map(rusqlite::params![run,""],|r|r.get::<_,String>(3))?.collect::<std::result::Result<Vec<_>,_>>()?;Ok(rows)}).unwrap();
    assert!(
        plan.iter().any(|line| line.contains("result_gc_ref_run")),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("TEMP B-TREE")),
        "{plan:?}"
    );
    let guard = crate::test_tmp::gc_guard(&f.store, &f.run);
    assert!(f
        .store
        .claim_result_page(&guard, &f.run, "", 0, 200, 10, Some("boot".into()), 10)
        .is_err());
    assert!(f
        .store
        .claim_result_page(&guard, &f.run, "", 65, 200, 10, Some("boot".into()), 10)
        .is_err());
    assert!(
        f.store
            .claim_result_page(&guard, &f.run, "", 32, 200, 10, Some("boot".into()), 10)
            .unwrap()
            .clock_held
    );
    let first = f
        .store
        .claim_result_page(&guard, &f.run, "", 32, 210, 20, Some("boot".into()), 10)
        .unwrap();
    assert_eq!((first.examined, first.removed, first.queued), (32, 32, 0));
    let second = f
        .store
        .claim_result_page(
            &guard,
            &f.run,
            first.next.as_ref().unwrap(),
            32,
            211,
            21,
            Some("boot".into()),
            10,
        )
        .unwrap();
    assert_eq!(
        (second.examined, second.removed, second.queued),
        (32, 32, 0)
    );
    let third = f
        .store
        .claim_result_page(
            &guard,
            &f.run,
            second.next.as_ref().unwrap(),
            32,
            212,
            22,
            Some("boot".into()),
            10,
        )
        .unwrap();
    assert_eq!((third.examined, third.removed, third.queued), (6, 6, 1));
    assert!(third.next.is_none());
    assert!(f
        .store
        .add_blob_ref(&blob("late", &a, BlobKind::Result, &locator), 212)
        .is_err());
    assert!(f.store.pending_result_page(&guard, &f.run, 0).is_err());
    let claim = f
        .store
        .pending_result_page(&guard, &f.run, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claim.attempt, a);
    assert_eq!(claim.run, f.run);
    files.remove(&claim.locator).unwrap(); // crash after FS, before completion
    drop(guard);
    drop(f.store);
    let restarted = CoordinatorStore::open(f._dir.path(), 300).unwrap();
    let guard = crate::test_tmp::gc_guard(&restarted, &f.run);
    let replay = restarted
        .pending_result_page(&guard, &f.run, 1)
        .unwrap()
        .pop()
        .unwrap();
    files.remove(&replay.locator).unwrap();
    restarted.finish_result_delete(&guard, &replay).unwrap();
    assert!(restarted
        .pending_result_page(&guard, &f.run, 1)
        .unwrap()
        .is_empty());
    assert!(restarted.finish_result_delete(&guard, &replay).is_err());
    // Verdict/audit survive physical deletion, including failed compute.
    assert_eq!(
        restarted
            .get_attempt(&a)
            .unwrap()
            .unwrap()
            .termination
            .unwrap()
            .outcome(),
        Outcome::Failed
    );
    assert!(restarted
        .collection(&a)
        .unwrap()
        .unwrap()
        .closed_at_ms
        .is_some());
    restarted
        .db
        .write(|tx| {
            tx.execute("INSERT INTO pending_delete VALUES('results/legacy',1)", [])?;
            Ok(())
        })
        .unwrap();
    assert!(restarted
        .pending_result_page(&guard, &f.run, 32)
        .unwrap()
        .is_empty());
    assert_eq!(restarted.pending_deletes().unwrap(), vec!["results/legacy"]);
}
#[test]
fn result_retention_reader_writer_and_legacy_backfill_are_fail_closed() {
    use crate::service::completion::Target;
    let f = fx();
    let a = retention_ready(&f);
    let reader = CoordinatorStore::open_read_only(f._dir.path()).unwrap();
    let target = Target::Task(f.task.clone());
    let pin = reader.result_read_pin(&target, Some(&a)).unwrap();
    assert!(f
        .store
        .claim_result_page(&pin, &f.run, "", 1, 200, 10, Some("boot".into()), 0)
        .is_err());
    assert!(f.store.result_gc_guard(&f.run).is_err());
    assert!(reader.result_gc_guard(&f.run).is_err());
    assert!(reader
        .result_read_pin(&target, Some(&AttemptId::generate()))
        .is_err());
    assert!(reader
        .result_read_pin(&Target::Run(f.run.clone()), Some(&a))
        .is_err());
    drop(pin);
    let writer = f.store.result_writer_pin(&a).unwrap();
    assert!(f.store.result_gc_guard(&f.run).is_err());
    drop(writer);
    let guard = crate::test_tmp::gc_guard(&f.store, &f.run);
    assert_eq!(
        reader.result_read_pin(&target, None).unwrap_err().code(),
        ErrorCode::Conflict
    );
    drop(guard);
    let path = f._dir.path().join(format!("reader-pins/{}.lock", f.run));
    // Legacy fixture only: production never unlinks a pin.
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        reader.result_read_pin(&target, None).unwrap_err().code(),
        ErrorCode::NotFound
    );
    assert!(!path.exists());
    let guard = crate::test_tmp::gc_guard(&f.store, &f.run);
    drop(guard);
    reader.result_read_pin(&target, None).unwrap();
}
#[test]
fn result_retention_keeps_unknown_unacked_open_and_pinned_references() {
    let f = fx();
    let a = retention_ready(&f);
    let locator = crate::results::store::ResultStore::result_locator(&a);
    f.store
        .add_blob_ref(&blob("ref", &a, BlobKind::Result, &locator), 102)
        .unwrap();
    let guard = crate::test_tmp::gc_guard(&f.store, &f.run);
    f.store
        .claim_result_page(&guard, &f.run, "", 1, 200, 10, Some("boot".into()), 10)
        .unwrap();
    for mutation in ["UPDATE attempt SET observation='unknown'", "UPDATE attempt SET observation='known'; UPDATE collection SET acked_at_ms=NULL", "UPDATE collection SET acked_at_ms=NULL,closed_at_ms=NULL,verdict=NULL", "UPDATE collection SET acked_at_ms=102,closed_at_ms=101,verdict='complete'; UPDATE blob_ref SET pins=1"] {
        f.store.db.write(move |tx| {tx.execute_batch(mutation)?;Ok(())}).unwrap();
        let p=f.store.claim_result_page(&guard,&f.run,"",1,210,20,Some("boot".into()),10).unwrap();assert_eq!(p.removed,0);
    }
    f.store
        .db
        .write(|tx| {
            tx.execute_batch(
                "UPDATE blob_ref SET pins=0; UPDATE attempt SET terminal_at_ms=1000;",
            )?;
            Ok(())
        })
        .unwrap();
    let future = f
        .store
        .claim_result_page(&guard, &f.run, "", 1, 210, 20, Some("boot".into()), 0)
        .unwrap();
    assert_eq!(future.removed, 0);
    // Invalid clock must Hold even retention zero.
    let p = f
        .store
        .claim_result_page(&guard, &f.run, "", 1, 500, u64::MAX, Some("boot".into()), 0)
        .unwrap();
    assert!(p.clock_held);
    assert_eq!(p.removed, 0);
}

#[test]
fn result_gc_run_selection_is_indexed_and_independent_of_task_facts() {
    let f = fx();
    let run = f.run.to_string();
    f.store
        .db
        .write(move |tx| {
            for _ in 0..2000 {
                tx.execute(
                    "INSERT INTO task(task_id,run_id,spec_json,created_at_ms) VALUES(?1,?2,'{}',1)",
                    rusqlite::params![TaskId::generate().to_string(), run],
                )?;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(
        f.store.result_gc_next_run(None).unwrap(),
        Some(f.run.clone())
    );
    assert_eq!(
        f.store.result_gc_next_run(Some(f.run.clone())).unwrap(),
        None
    );
    let plan = f
        .store
        .db
        .call(|c| {
            let mut q = c.prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                coordinator::query::RESULT_GC_NEXT_RUN
            ))?;
            let rows = q
                .query_map([""], |r| r.get::<_, String>(3))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .unwrap();
    assert_eq!(plan.len(), 1, "{plan:?}");
    assert!(
        plan[0].contains("SEARCH run USING COVERING INDEX") && plan[0].contains("run_id>?"),
        "{plan:?}"
    );
    // A facts query would fail; the GC path must never consult task metadata.
    f.store
        .db
        .write(|tx| {
            tx.execute_batch("ALTER TABLE task RENAME TO unrelated_tasks;")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(f.store.result_gc_next_run(None).unwrap(), Some(f.run));
}

#[test]
fn result_retention_claim_is_retained_when_ancestor_points_outside_root() {
    use crate::results::ResultStore;
    use std::os::unix::fs::symlink;
    let f = fx();
    let a = retention_ready(&f);
    let files = ResultStore::open(&f._dir.path().join("results")).unwrap();
    let outside = crate::test_tmp::tempdir().unwrap();
    std::fs::create_dir(outside.path().join("logs")).unwrap();
    std::fs::write(outside.path().join("logs/sentinel"), b"safe").unwrap();
    let locator = ResultStore::logs_locator(&a);
    f.store
        .add_blob_ref(&blob("claimed", &a, BlobKind::Log, &locator), 102)
        .unwrap();
    let guard = crate::test_tmp::gc_guard(&f.store, &f.run);
    f.store
        .claim_result_page(&guard, &f.run, "", 32, 200, 10, Some("boot".into()), 10)
        .unwrap();
    f.store
        .claim_result_page(&guard, &f.run, "", 32, 210, 20, Some("boot".into()), 10)
        .unwrap();
    drop(guard);
    symlink(outside.path(), files.root().join(a.as_str())).unwrap();
    // The entry cannot be removed through the link: it is counted, stays
    // pending and is never recorded as done.
    let report = crate::test_tmp::settled_page(&f.store, &files, 7);
    assert_eq!((report.deleted, report.failed), (0, 1));
    assert_eq!(
        std::fs::read(outside.path().join("logs/sentinel")).unwrap(),
        b"safe"
    );
    let guard = crate::test_tmp::gc_guard(&f.store, &f.run);
    assert_eq!(
        f.store
            .pending_result_page(&guard, &f.run, 32)
            .unwrap()
            .len(),
        1
    );
    drop(guard);
    std::fs::remove_file(files.root().join(a.as_str())).unwrap();
    // The known claim alone permits a missing object to finish successfully.
    assert_eq!(
        crate::test_tmp::settled_page(&f.store, &files, 7).deleted,
        1
    );
}

#[test]
fn enqueue_does_not_publish_run_when_reader_pin_cannot_be_secured() {
    use std::os::unix::fs::PermissionsExt;
    let f = fx();
    let run = RunId::generate();
    let task = TaskId::generate();
    let pins = f._dir.path().join("reader-pins");
    std::fs::set_permissions(&pins, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(f
        .store
        .enqueue(&run, None, &json!({}), &[(task.clone(), json!({}))], 2)
        .is_err());
    assert!(f.store.query_run(run.clone()).unwrap().is_none());
    assert!(f.store.query_task(task.clone()).unwrap().is_none());
    std::fs::set_permissions(&pins, std::fs::Permissions::from_mode(0o700)).unwrap();
    f.store
        .enqueue(&run, None, &json!({}), &[(task, json!({}))], 2)
        .unwrap();
    let reader = CoordinatorStore::open_read_only(f._dir.path()).unwrap();
    let pin = reader
        .result_read_pin(&crate::service::completion::Target::Run(run.clone()), None)
        .unwrap();
    assert!(f.store.result_gc_guard(&run).is_err());
    drop(pin);
}

#[test]
fn capability_durable_schema_fence_rejects_old_binaries_and_indexes_placement() {
    use super::policy::{Access, Incompat};
    for kind in [DbKind::Coordinator, DbKind::Worker] {
        let root = crate::test_tmp::tempdir().unwrap();
        let path = root.path().join("contract.db");
        let steps = schema::steps(kind);
        let previous = steps.len() - 1;
        drop(
            db::open_with(
                &path,
                kind.as_str(),
                &steps[..previous],
                schema::compat(previous),
                Access::ReadWrite,
            )
            .unwrap(),
        );
        let current = Db::open(&path, kind, Access::ReadWrite).unwrap();
        assert_eq!(
            current
                .call(|c| Ok(c.query_row(
                    "SELECT value FROM schema_meta WHERE key='execution_requirements_v1'",
                    [],
                    |r| r.get::<_, String>(0)
                )?))
                .unwrap(),
            "1"
        );
        assert_eq!(
            current
                .call(|c| Ok(c.query_row(
                    "SELECT value FROM schema_meta WHERE key='frozen_toolchains_v1'",
                    [],
                    |r| r.get::<_, String>(0)
                )?))
                .unwrap(),
            "1"
        );
        if kind == DbKind::Coordinator {
            let plan=current.call(|c| {let mut q=c.prepare("EXPLAIN QUERY PLAN SELECT substr(payload_json,1,65537) FROM event WHERE kind='task_waiting' AND json_extract(payload_json,'$.task')=?1 ORDER BY seq DESC LIMIT 1")?;let rows=q.query_map(["task_example"],|r|r.get::<_,String>(3))?.collect::<std::result::Result<Vec<_>,_>>()?;Ok(rows)}).unwrap();
            assert!(
                plan.iter().any(|line| line.contains("event_task_waiting")),
                "{plan:?}"
            );
            assert!(
                !plan.iter().any(|line| line.contains("SCAN event")),
                "{plan:?}"
            );
        }
        drop(current);
        for access in [Access::ReadOnly, Access::ReadWrite] {
            let result = db::open_with(
                &path,
                kind.as_str(),
                &steps[..previous],
                schema::compat(previous),
                access,
            );
            assert!(matches!(
                result,
                Err(StorageError::Incompatible(Incompat::Newer { .. }))
            ));
        }
    }
}

#[test]
fn source_preview_guard_fences_retry_commit_and_foreign_guards_without_mutation() {
    use crate::service::completion::Target;
    let (f, _) = retry_fixture(Outcome::Failed);
    let target = Target::Run(f.run.clone());
    let prepare = || match f
        .store
        .prepare_retry(target.clone(), 0, "source-fence".into())
        .unwrap()
    {
        RetryPreparation::Prepared(p) => p,
        _ => panic!("expected prepare"),
    };
    let source = crate::source::SourceStore::open(f._dir.path()).unwrap();
    let prepared = prepare();
    let exclusive = source.retention.guard(true).unwrap();
    assert!(f
        .store
        .prepare_retry(target.clone(), 0, "blocked-preflight".into())
        .is_err());
    assert!(f.store.commit_retry(prepared, false, 40).is_err());
    assert_eq!(
        f.store.completion(target.clone()).unwrap().control_revision,
        0
    );
    drop(exclusive);
    let other = crate::test_tmp::tempdir().unwrap();
    let foreign = crate::source::SourceStore::open(other.path())
        .unwrap()
        .mutation_guard()
        .unwrap();
    assert!(f
        .store
        .commit_retry_guarded(prepare(), false, 40, foreign)
        .is_err());
    assert_eq!(
        f.store.completion(target.clone()).unwrap().control_revision,
        0
    );
    let shared = source.mutation_guard().unwrap();
    assert!(source.retention.guard(true).is_err());
    f.store
        .commit_retry_guarded(prepare(), false, 40, shared)
        .unwrap();
    assert_eq!(f.store.completion(target).unwrap().control_revision, 1);
    drop(source.retention.guard(true).unwrap());
}

#[test]
fn future_run_expiry_fences_retry_preflight_commit_and_enqueue_without_mutation() {
    use crate::service::completion::Target;
    let (f, _) = retry_fixture(Outcome::Failed);
    let target = Target::Run(f.run.clone());
    let prepared = match f
        .store
        .prepare_retry(target.clone(), 0, "before-expiry".into())
        .unwrap()
    {
        RetryPreparation::Prepared(p) => p,
        _ => panic!("expected prepared retry"),
    };
    let sources = crate::source::SourceStore::open(f._dir.path()).unwrap();
    {
        let _exclusive = sources.retention.guard(true).unwrap();
        // Fixture-only insertion: production A exposes no expiry mutator.
        rusqlite::Connection::open(sources.root().join("retention.db"))
            .unwrap()
            .execute(
                "INSERT INTO run_expiry VALUES(?1,?2,?3)",
                rusqlite::params![
                    f.run.as_str(),
                    prepared.source_id.as_str(),
                    prepared.manifest_digest.as_str()
                ],
            )
            .unwrap();
    }
    match f
        .store
        .prepare_retry(target.clone(), 0, "after-expiry".into())
    {
        Err(error) => assert!(error.to_string().contains("run expired")),
        Ok(_) => panic!("expired preflight accepted"),
    }
    assert!(f
        .store
        .commit_retry(prepared, false, 40)
        .unwrap_err()
        .to_string()
        .contains("run expired"));
    assert!(f
        .store
        .enqueue(
            &f.run,
            None,
            &json!({}),
            &[(TaskId::generate(), json!({}))],
            40
        )
        .unwrap_err()
        .to_string()
        .contains("run expired"));
    assert_eq!(f.store.completion(target).unwrap().control_revision, 0);
    assert_eq!(f.store.attempts_of_task(&f.task).unwrap().len(), 1);
}

/// Secret delivery walks one worker's live attempts by this keyset page, so a
/// worker with more live attempts than one page is still covered completely.
#[test]
fn live_attempts_of_a_worker_are_paged_without_gaps_repeats_or_other_rows() {
    let f = fx();
    let on = |name: &str, at_ms: u64| {
        let task = TaskId::generate();
        f.store.create_task(&task, &f.run, &json!({}), 1).unwrap();
        f.store
            .create_attempt(
                NewAttempt {
                    attempt_id: AttemptId::generate(),
                    task_id: task,
                    worker_id: worker(name),
                    payload_hash: hash('a'),
                    permit_id: None,
                },
                at_ms,
            )
            .unwrap()
            .attempt_id
    };
    // Rows that never belong to the walk are interleaved with the live ones.
    let ended = on("mini-a", 10);
    f.to_finalizing(&ended);
    f.store
        .finalize_attempt(&ended, done(Outcome::Succeeded), 30)
        .unwrap();
    let retired = on("mini-a", 10);
    f.store.begin_retire(&retired, "ack lost", 11).unwrap();
    f.store.confirm_retire(&retired, 12).unwrap();
    let mut expected = Vec::new();
    for i in 0..70u64 {
        // Several attempts share one creation time; the row order breaks the tie.
        expected.push(on("mini-a", 10 + i / 30));
        if i % 20 == 0 {
            on("mini-b", 10 + i / 30);
        }
    }

    let mut walked = Vec::new();
    let mut sizes = Vec::new();
    let mut cursor = None;
    loop {
        let page = f.store.live_attempt_page(&f.worker, cursor, 16).unwrap();
        sizes.push(page.len());
        let Some((last, _)) = page.last() else { break };
        cursor = Some(*last);
        walked.extend(page.into_iter().map(|(_, live)| live.record.attempt_id));
    }
    assert_eq!(sizes, [16, 16, 16, 16, 6, 0]);
    assert_eq!(walked, expected);

    // A caller cannot ask for an unbounded page.
    let capped = f
        .store
        .live_attempt_page(&f.worker, None, u32::MAX)
        .unwrap();
    assert_eq!(capped.len(), 64);
    assert_eq!(
        capped
            .iter()
            .map(|(_, live)| live.record.attempt_id.clone())
            .collect::<Vec<_>>(),
        expected[..64]
    );

    // The cache is pruned against the live set without loading that set.
    let mut asked = expected[..62].to_vec();
    asked.push(ended);
    asked.push(retired);
    let live = f.store.live_attempt_subset(&f.worker, &asked).unwrap();
    assert_eq!(live, expected[..62].iter().cloned().collect());
    asked.push(expected[62].clone());
    assert!(matches!(
        f.store.live_attempt_subset(&f.worker, &asked),
        Err(StorageError::Invalid(_))
    ));
    let other = worker("mini-b");
    assert!(f
        .store
        .live_attempt_subset(&other, &expected[..8])
        .unwrap()
        .is_empty());
}
