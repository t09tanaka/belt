//! Committed preparation history, deliberately not consumed as scalar setup cost.
use super::*;
use crate::ids::BuilderId;
use crate::setup::observation::{PreparationObservation, POLICY};

#[derive(Debug, Clone)]
pub struct PreparationSample {
    pub(super) sample: Sample,
    namespace: String,
    sequence: u64,
    payload: String,
}
impl PreparationSample {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn verified(
        worker: &WorkerId,
        transport: &Transport,
        owner: &BuilderId,
        native_boot: &str,
        sequence: u64,
        value: &PreparationObservation,
        parent_boot: &str,
        sent: u64,
        received: u64,
        remote_now: u64,
    ) -> Option<Self> {
        if native_boot.is_empty()
            || native_boot.len() > 128
            || parent_boot.is_empty()
            || sequence == 0
            || sequence > i64::MAX as u64
            || value.policy != POLICY
            || value.binding.owner_generation != *owner
            || value.boot != native_boot
            || !(1..=256).contains(&value.node_count)
            || [
                sent,
                received,
                remote_now,
                value.finished_monotonic_ms,
                value.duration_ms,
            ]
            .contains(&u64::MAX)
            || received < sent
            || value.duration_ms > i64::MAX as u64
        {
            return None;
        }
        let age = remote_now.checked_sub(value.finished_monotonic_ms)?;
        let observed = sent.checked_sub(age)?;
        if received.checked_sub(observed)? > TTL_MS {
            return None;
        }
        let namespace = hash(&serde_json::json!({"version":2,"worker":worker,
            "transport":transport,"native_boot":native_boot,"owner":owner,"parent_boot":parent_boot}));
        let key =
            hash(&serde_json::json!({"preparation":&namespace,"builder":value.binding.builder_id}));
        let payload = serde_json::to_string(value).ok()?;
        let receipt =
            hash(&serde_json::json!({"namespace":&namespace,"sequence":sequence,"original":value}));
        Some(Self {
            sample: Sample {
                key,
                receipt,
                boot: parent_boot.into(),
                observed,
                duration: value.duration_ms,
            },
            namespace,
            sequence,
            payload,
        })
    }
}
impl MeasuredCosts {
    pub fn record_preparation(&self, sample: PreparationSample) {
        if let Some(tx) = self
            .writer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            let _ = tx.try_send(Write::Preparation(Box::new(sample)));
        }
    }
}
pub(super) fn initialize(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS preparation_namespaces(namespace TEXT PRIMARY KEY,boot TEXT NOT NULL,cutoff INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS preparation_receipts(key TEXT PRIMARY KEY,namespace TEXT NOT NULL,sequence INTEGER NOT NULL,payload TEXT NOT NULL,UNIQUE(namespace,sequence));")
}
pub(super) fn record(db: &mut Connection, value: &PreparationSample) -> rusqlite::Result<bool> {
    let sample = &value.sample;
    if platform::boot_id().as_deref() != Some(sample.boot.as_str()) {
        return Ok(false);
    }
    let (Ok(observed), Ok(duration), Ok(sequence)) = (
        i64::try_from(sample.observed),
        i64::try_from(sample.duration),
        i64::try_from(value.sequence),
    ) else {
        return Ok(false);
    };
    let tx = db.transaction()?;
    // Previous native-parent-boot history is never reused; reclaim only its
    // namespace slots. At 4096 current-boot namespaces, new history is Unknown.
    tx.execute(
        "DELETE FROM preparation_namespaces WHERE boot!=?1",
        [&sample.boot],
    )?;
    let cutoff: Option<i64> = tx
        .query_row(
            "SELECT cutoff FROM preparation_namespaces WHERE namespace=?1",
            [&value.namespace],
            |r| r.get(0),
        )
        .optional()?;
    if cutoff.is_some_and(|cutoff| sequence <= cutoff) {
        return Ok(false);
    }
    if cutoff.is_none() {
        let count: i64 = tx.query_row("SELECT COUNT(*) FROM preparation_namespaces", [], |r| {
            r.get(0)
        })?;
        if count >= CAP as i64 {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO preparation_namespaces VALUES(?1,?2,0)",
            params![value.namespace, sample.boot],
        )?;
    }
    let inserted=tx.execute("INSERT OR IGNORE INTO preparation_receipts(key,namespace,sequence,payload) VALUES(?1,?2,?3,?4)",params![sample.key,value.namespace,sequence,value.payload])?;
    if inserted == 0 {
        return Ok(false);
    }
    let inserted=tx.execute("INSERT OR IGNORE INTO costs(version,key,receipt,boot,observed,duration) VALUES(2,?1,?2,?3,?4,?5)",params![sample.key,sample.receipt,sample.boot,observed,duration])?;
    if inserted == 0 {
        return Ok(false);
    }
    prune(&tx, &sample.boot)?;
    tx.commit()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AttemptId, BuilderId};
    use crate::setup::observation::{PreparationBinding, PreparationKind};
    fn fixture() -> (
        WorkerId,
        Transport,
        BuilderId,
        PreparationObservation,
        String,
    ) {
        let owner = BuilderId::generate();
        (
            WorkerId::parse("worker-a").unwrap(),
            Transport {
                worker_boot: WorkerBootId::parse("worker-boot").unwrap(),
                agent_epoch: AgentEpoch(1),
                policy: "opaque-policy".into(),
            },
            owner.clone(),
            PreparationObservation {
                binding: PreparationBinding {
                    builder_id: BuilderId::generate(),
                    attempt_id: AttemptId::generate(),
                    owner_generation: owner,
                    kind: PreparationKind::ToolInstall,
                    key: Digest::of_bytes(b"recipe"),
                },
                policy: POLICY.into(),
                nodes_digest: Digest::of_bytes(b"nodes"),
                node_count: 1,
                boot: "remote-native".into(),
                finished_monotonic_ms: 450,
                duration_ms: 10,
            },
            platform::boot_id().unwrap(),
        )
    }
    #[test]
    fn conservative_age_original_receipt_and_conflicts_survive_parent_restart() {
        let dir = private_test_root();
        let model = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        let (w, t, o, v, b) = fixture();
        let sample =
            PreparationSample::verified(&w, &t, &o, "remote-native", 1, &v, &b, 1000, 1005, 500)
                .unwrap();
        assert_eq!(sample.sample.observed, 950);
        model.record_preparation(sample.clone());
        model.drain();
        drop(model);
        let model = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        let replay =
            PreparationSample::verified(&w, &t, &o, "remote-native", 1, &v, &b, 1100, 1105, 590)
                .unwrap();
        assert_eq!(replay.sample.observed, 960);
        model.record_preparation(replay);
        let mut conflict = v.clone();
        conflict.duration_ms = 999;
        model.record_preparation(
            PreparationSample::verified(
                &w,
                &t,
                &o,
                "remote-native",
                1,
                &conflict,
                &b,
                1100,
                1105,
                590,
            )
            .unwrap(),
        );
        model.drain();
        let cache = model.cache.read().unwrap();
        let retained = &cache[&sample.sample.key];
        assert_eq!((retained.observed, retained.duration), (950, 10));
        let db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        let payload: String = db
            .query_row("SELECT payload FROM preparation_receipts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            serde_json::from_str::<PreparationObservation>(&payload).unwrap(),
            v
        );
        assert_eq!(model.lookup_at(&w, &CostQuery::default(), 1006).ms[1], None);
    }
    #[test]
    fn age_boot_overflow_backward_and_policy_fail_closed_without_fake_started_interval() {
        let (w, t, o, mut v, b) = fixture();
        let make = |v: &PreparationObservation, sent, received, now| {
            PreparationSample::verified(&w, &t, &o, "remote-native", 1, v, &b, sent, received, now)
        };
        assert!(make(&v, 1000, 999, 500).is_none());
        assert!(make(&v, 10, 11, 500).is_none());
        assert!(make(&v, 1000, 1001, 449).is_none());
        assert!(make(&v, 1000, 1000 + TTL_MS, 500).is_none());
        assert!(make(&v, u64::MAX, u64::MAX, 500).is_none());
        assert!(make(&v, 1000, 1001, u64::MAX).is_none());
        let mut invalid = v.clone();
        invalid.boot = "different".into();
        assert!(make(&invalid, 1000, 1001, 500).is_none());
        invalid = v.clone();
        invalid.policy = "untrusted-policy".into();
        assert!(make(&invalid, 1000, 1001, 500).is_none());
        invalid = v.clone();
        invalid.binding.owner_generation = BuilderId::generate();
        assert!(make(&invalid, 1000, 1001, 500).is_none());
        v.duration_ms = 10000;
        // The duration is a sum of admitted work, not an interval starting at
        // mapped_finished. It must not be checked as finished+duration<=now.
        assert!(make(&v, 1000, 1001, 500).is_some());
    }
    #[test]
    fn sequence_eviction_watermark_blocks_fresher_replay_and_namespace_capacity_is_bounded() {
        let dir = private_test_root();
        let model = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        model.drain();
        let (w, t, o, v, b) = fixture();
        let mut sample =
            PreparationSample::verified(&w, &t, &o, "remote-native", 1, &v, &b, 1000, 1001, 500)
                .unwrap();
        let mut db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        assert!(record(&mut db, &sample).unwrap());
        {
            let tx = db.transaction().unwrap();
            for n in 0..CAP {
                tx.execute(
                    "INSERT INTO costs VALUES(1,?1,?1,?2,?3,1)",
                    params![format!("phase-{n}"), b, 1000 + n as i64],
                )
                .unwrap();
            }
            prune(&tx, &b).unwrap();
            tx.commit().unwrap();
        }
        sample.sample.observed = 99999;
        assert!(!record(&mut db, &sample).unwrap());
        let count: i64 = db
            .query_row("SELECT COUNT(*) FROM preparation_receipts", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
        let floor: i64 = db
            .query_row("SELECT cutoff FROM preparation_namespaces", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(floor, 1);
        {
            let tx = db.transaction().unwrap();
            for n in 1..CAP {
                tx.execute(
                    "INSERT INTO preparation_namespaces VALUES(?1,?2,0)",
                    params![format!("namespace-{n}"), b],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        let mut fresh = sample.clone();
        fresh.namespace = "new-owner".into();
        fresh.sequence = 2;
        assert!(!record(&mut db, &fresh).unwrap());
        db.execute(
            "UPDATE preparation_namespaces SET boot='previous-native-parent-boot'",
            [],
        )
        .unwrap();
        assert!(record(&mut db, &fresh).unwrap());
        let mut foreign = fresh;
        foreign.sample.boot = "previous-native-parent-boot".into();
        assert!(!record(&mut db, &foreign).unwrap());
    }
    #[test]
    fn full_queue_and_writer_fault_publish_no_uncommitted_history() {
        let (w, t, o, v, b) = fixture();
        let value =
            PreparationSample::verified(&w, &t, &o, "remote-native", 1, &v, &b, 1000, 1001, 500)
                .unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let bounded = MeasuredCosts {
            boot: Some(b),
            policy: String::new(),
            cache: Arc::default(),
            writer: Mutex::new(Some(tx)),
            join: Mutex::new(None),
        };
        bounded.record_preparation(value.clone());
        bounded.record_preparation(value.clone());
        assert!(bounded.cache.read().unwrap().is_empty());
        drop(rx);
        bounded.drain();
        let dir = private_test_root();
        let model = MeasuredCosts::open(dir.path(), Default::default()).unwrap();
        let db = Connection::open(dir.path().join("costs-v1.sqlite")).unwrap();
        db.execute("DROP TABLE preparation_receipts", []).unwrap();
        model.record_preparation(value);
        model.drain();
        assert!(model.cache.read().unwrap().is_empty());
    }
}
