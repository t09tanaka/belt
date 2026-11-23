//! Acceptance tests of the deleting pass of received sources (spec 25.8 5b
//! (d), (g)).
use super::deletion::set_fault;
use super::*;
use crate::source::{capture, CaptureParams, SourceRequest};
use std::cell::RefCell;
use std::collections::BTreeMap;

/// The age limit in these tests (14 days in production).
const AGE: u64 = 1_000;
/// "Now" of the trusted clock in these tests.
const NOW: u64 = 1_000_000;

struct Env {
    root: tempfile::TempDir,
    parent: SourceStore,
    receiver: SourceStore,
    n: usize,
}

impl Env {
    fn new() -> Self {
        let root = crate::test_tmp::tempdir().unwrap();
        let parent = SourceStore::open(&root.path().join("parent")).unwrap();
        let receiver = SourceStore::open(&root.path().join("receiver")).unwrap();
        let env = Env {
            root,
            parent,
            receiver,
            n: 0,
        };
        env.sql(
            "UPDATE meta SET value='done' WHERE key IN ('legacy_registration','bytes_fill','clone_ref_fill')",
        );
        env
    }
    fn sql(&self, s: &str) {
        self.receiver
            .retention
            .connect()
            .unwrap()
            .execute_batch(s)
            .unwrap();
    }
    fn count(&self, s: &str) -> i64 {
        self.receiver
            .retention
            .connect()
            .unwrap()
            .query_row(s, [], |r| r.get(0))
            .unwrap()
    }
    /// A received source made of `files` (name, content), used at `used`.
    fn source(&mut self, files: &[(&str, &str)], used: i64) -> String {
        self.n += 1;
        let dir = self.root.path().join(format!("in-{}", self.n));
        std::fs::create_dir(&dir).unwrap();
        for (name, content) in files {
            std::fs::write(dir.join(name), content).unwrap();
        }
        let m = capture(
            &BTreeMap::new(),
            &self.parent,
            &SourceRequest::Directory { dir },
            &CaptureParams::default(),
        )
        .unwrap()
        .manifest;
        let ci = crate::source::classify::ci_id_of(&m.source_id).unwrap();
        let m = self.parent.manifest(&ci).unwrap().unwrap();
        for e in &m.object_manifest.entries {
            let want = crate::source::diff::BlobWant {
                hash: e.hash.clone(),
                size: e.size,
            };
            let bytes = self.parent.read_blob(&e.hash).unwrap();
            self.receiver.ingest_blob(&want, &mut &bytes[..]).unwrap();
        }
        self.receiver.publish_received(&m, None).unwrap();
        // (the receipt's token is released, as the receiving path does)
        self.sql("DELETE FROM member WHERE root LIKE 'import:%'; DELETE FROM root WHERE id LIKE 'import:%'; DELETE FROM import_session;");
        self.sql(&format!(
            "UPDATE root SET used_ms={used} WHERE id='receive:{}'",
            m.source_id
        ));
        m.source_id.clone()
    }
    fn received(&self, id: &str) -> bool {
        let held = self.receiver.mutation_guard().unwrap();
        self.receiver.received(&held, id).unwrap()
    }
    /// Seed the source clock so that the next pass trusts `NOW`.
    fn trust_clock(&self) {
        let state = crate::results::clock_policy::step(None, Some("b"), NOW - AGE, 0, AGE)
            .state
            .unwrap();
        self.receiver
            .retention
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO meta(key,value) VALUES('received_clock',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [serde_json::to_string(&state).unwrap()],
            )
            .unwrap();
    }
    fn pass_with(
        &self,
        cap: u64,
        uses: &dyn Fn(&str) -> Result<SourceUse>,
    ) -> Result<ReceivedPassReport> {
        self.receiver.received_pass(&ReceivedPassInputs {
            wall: NOW,
            mono: AGE,
            boot: Some("b".into()),
            max_age_ms: AGE,
            cap_bytes: cap,
            uses,
        })
    }
    fn pass(&self) -> ReceivedPassReport {
        self.pass_with(u64::MAX, &|_| Ok(SourceUse::Ended(None)))
            .unwrap()
    }
    /// The sizes of the object files the index has a row for.
    fn files_on_disk(&self) -> u64 {
        let c = self.receiver.retention.connect().unwrap();
        let mut s = c.prepare("SELECT path FROM object").unwrap();
        let paths: Vec<String> = s
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        paths
            .iter()
            .filter_map(|p| std::fs::symlink_metadata(self.receiver.root().join(p)).ok())
            .map(|m| m.len())
            .sum()
    }
    fn store_bytes(&self) -> u64 {
        self.receiver.store_bytes().unwrap().unwrap()
    }
}

const OLD: i64 = (NOW - AGE - 1) as i64;
const YOUNG: i64 = (NOW - AGE + 1) as i64;

/// (g)10, (g)13: an unreferenced source unused for longer than the age goes
/// (its manifest, its own blobs and its rows); a younger one stays; a blob
/// another source holds stays; `store_bytes` matches the files.
#[test]
fn an_expired_unreferenced_source_goes_and_a_younger_one_stays() {
    let mut env = Env::new();
    let old = env.source(&[("a", "only old"), ("s", "shared")], OLD);
    let young = env.source(&[("b", "only young"), ("s", "shared")], YOUNG);
    env.trust_clock();
    let report = env.pass();
    assert_eq!(report.detached, 1, "{report:?}");
    assert!(!report.stuck && report.gate.is_none(), "{report:?}");
    assert!(!env.received(&old));
    assert!(env.received(&young));
    assert!(!env
        .receiver
        .root()
        .join(format!("manifests/{old}.json"))
        .exists());
    // every row of the old source is gone, the young one's are all there
    {
        let c = env.receiver.retention.connect().unwrap();
        let mut s = c.prepare("SELECT r.id||'|'||r.kind||'|'||ifnull(m.object,'-') FROM root r LEFT JOIN member m ON m.root=r.id").unwrap();
        let rows: Vec<String> = s
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            env.count("SELECT count(*) FROM root WHERE id LIKE 'drop:%'"),
            0,
            "{rows:#?} {report:?}"
        );
    }
    assert_eq!(env.count("SELECT count(*) FROM source_delete_claim"), 0);
    let m = env.receiver.manifest(&young).unwrap().unwrap();
    for e in &m.object_manifest.entries {
        assert!(env.receiver.read_blob(&e.hash).is_ok());
    }
    assert_eq!(env.store_bytes(), env.files_on_disk());
    // a new import is not refused afterwards
    drop(env.receiver.begin_import().unwrap());
}

/// (g)10: while the clock holds, nothing goes by age.
#[test]
fn nothing_goes_by_age_while_the_clock_holds() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    let report = env.pass();
    assert!(report.clock_held, "{report:?}");
    assert_eq!(report.detached, 0);
    assert!(env.received(&old));
}

/// (g)2, (g)3, (h)7: a live attempt (of any member) or a pin keeps the
/// source; a failed lookup keeps it too.
#[test]
fn a_referenced_source_stays() {
    let mut env = Env::new();
    let live = env.source(&[("a", "live")], OLD);
    let broken = env.source(&[("b", "broken")], OLD);
    env.trust_clock();
    let report = env
        .pass_with(u64::MAX, &|s| {
            if s == live {
                Ok(SourceUse::Live)
            } else if s == broken {
                anyhow::bail!("worker.db is busy")
            } else {
                Ok(SourceUse::Ended(None))
            }
        })
        .unwrap();
    assert_eq!(report.detached, 0, "{report:?}");
    assert!(env.received(&live));
    assert!(env.received(&broken));
}

/// L6: a source whose attempt ended after its recorded time is treated as
/// used at the attempt's terminal time.
#[test]
fn an_attempt_that_ended_later_counts_as_the_last_use() {
    let mut env = Env::new();
    let id = env.source(&[("a", "ended")], OLD);
    env.trust_clock();
    let report = env
        .pass_with(u64::MAX, &|_| Ok(SourceUse::Ended(Some(YOUNG))))
        .unwrap();
    assert_eq!(report.detached, 0, "{report:?}");
    assert!(env.received(&id));
    assert_eq!(
        env.count(&format!("SELECT used_ms FROM root WHERE id='receive:{id}'")),
        YOUNG
    );
}

/// (g)4: a source another received source's graph names stays however old,
/// and goes once that parent is gone.
#[test]
fn a_submodule_source_stays_while_its_parent_is_there() {
    let mut env = Env::new();
    let child = env.source(&[("c", "child")], OLD);
    let parent = env.source(&[("p", "parent")], YOUNG);
    env.sql(&format!(
        "INSERT INTO graph(root,source,done) VALUES('receive:{parent}','{child}',1)"
    ));
    env.trust_clock();
    assert_eq!(env.pass().detached, 0);
    assert!(env.received(&child));
    env.sql(&format!("DELETE FROM graph WHERE root='receive:{parent}'"));
    assert_eq!(env.pass().detached, 1);
    assert!(!env.received(&child));
    assert!(env.received(&parent));
}

/// (g)11, (g)13: over the cap, the oldest unreferenced sources go until the
/// total is under it, also while the clock holds; nothing goes by size
/// before every size is known.
#[test]
fn over_the_cap_the_oldest_go_until_it_is_met() {
    let mut env = Env::new();
    let a = env.source(&[("a", &"a".repeat(4000))], 10);
    let b = env.source(&[("b", &"b".repeat(4000))], 20);
    let c = env.source(&[("c", &"c".repeat(4000))], 30);
    let total = env.store_bytes();
    // room for two of the three
    let cap = total - 3000;
    env.sql("UPDATE meta SET value='' WHERE key='bytes_fill'");
    assert_eq!(
        env.pass_with(cap, &|_| Ok(SourceUse::Ended(None)))
            .unwrap()
            .detached,
        0
    );
    env.sql("UPDATE meta SET value='done' WHERE key='bytes_fill'");
    let report = env.pass_with(cap, &|_| Ok(SourceUse::Ended(None))).unwrap();
    assert!(report.clock_held, "{report:?}");
    assert_eq!(report.detached, 1, "{report:?}");
    assert!(!env.received(&a));
    assert!(env.received(&b) && env.received(&c));
    assert!(env.store_bytes() <= cap);
    assert_eq!(env.store_bytes(), env.files_on_disk());
}

/// (g)22: a store with a parent's root is left alone; (g) gate: nothing
/// goes while the legacy registration is pending.
#[test]
fn a_parent_root_or_a_closed_gate_stops_the_pass() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.trust_clock();
    env.sql("INSERT INTO root(id,kind,source) VALUES('run:x','run',NULL)");
    assert!(env.pass().parent);
    env.sql("DELETE FROM root WHERE id='run:x'");
    env.sql("UPDATE meta SET value='pending' WHERE key='legacy_registration'");
    assert_eq!(env.pass().gate, Some(Gate::LegacyRegistration));
    assert!(env.received(&old));
}

/// (e), step 4: the clone ref a detached source leaves is recorded in a
/// `ref_drop:` row; one another root still has is not.
#[test]
fn a_detach_records_the_clone_refs_it_orphans() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.sql(&format!(
        "INSERT INTO clone_ref(clone,source,root) VALUES('c1','{old}','receive:{old}'),('c2','{old}','receive:{old}'),('c2','{old}','other')"
    ));
    env.trust_clock();
    assert_eq!(env.pass().detached, 1);
    assert_eq!(
        env.count(&format!(
            "SELECT count(*) FROM meta WHERE key='ref_drop:c1:{old}'"
        )),
        1
    );
    assert_eq!(
        env.count("SELECT count(*) FROM meta WHERE key LIKE 'ref_drop:c2:%'"),
        0
    );
    assert_eq!(
        env.count(&format!(
            "SELECT count(*) FROM clone_ref WHERE root='receive:{old}'"
        )),
        0
    );
}

/// (g)21: a pass killed after a claim leaves it; the next pass finishes it
/// first, and then imports are not refused.
#[test]
fn a_claim_left_by_a_crash_is_finished_by_the_next_pass() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.trust_clock();
    set_fault(Some("after_claim"));
    let failed = env.pass_with(u64::MAX, &|_| Ok(SourceUse::Ended(None)));
    set_fault(None);
    assert!(failed.is_err());
    assert!(env.count("SELECT count(*) FROM source_delete_claim WHERE done=0") > 0);
    assert!(env.receiver.begin_import().is_err());
    let report = env.pass();
    assert!(!report.stuck, "{report:?}");
    assert_eq!(env.count("SELECT count(*) FROM source_delete_claim"), 0);
    assert!(!env.received(&old));
    assert_eq!(env.store_bytes(), env.files_on_disk());
    drop(env.receiver.begin_import().unwrap());
}

/// (g)37: a member that cannot be proven does not stop the members after
/// it; the cursor moves past it.
#[test]
fn an_unproven_member_does_not_stop_the_ones_after_it() {
    let mut env = Env::new();
    let a = env.source(&[("a", "first")], OLD);
    let b = env.source(&[("b", "second")], OLD);
    // the first source's blob is changed by hand: it cannot be proven
    let ma = env.receiver.manifest(&a).unwrap().unwrap();
    let hash = ma.object_manifest.entries[0].hash.clone();
    let path: String = env
        .receiver
        .retention
        .connect()
        .unwrap()
        .query_row("SELECT path FROM object WHERE id='b:'||?1", [&hash], |r| {
            r.get(0)
        })
        .unwrap();
    let file = env.receiver.root().join(&path);
    let mut perm = std::fs::metadata(&file).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o600);
    std::fs::set_permissions(&file, perm).unwrap();
    std::fs::write(&file, b"changed").unwrap();
    env.trust_clock();
    let report = env.pass();
    assert_eq!(report.detached, 2, "{report:?}");
    assert!(report.kept >= 1, "{report:?}");
    // the changed file stays with its membership; the other source's files went
    assert!(file.exists());
    let mb_left = env.count(&format!(
        "SELECT count(*) FROM member WHERE root LIKE 'drop:{b}:%'"
    ));
    assert_eq!(mb_left, 0);
    assert_eq!(
        env.count(&format!(
            "SELECT count(*) FROM member WHERE root LIKE 'drop:{a}:%'"
        )),
        1
    );
}

/// (g)14 (the section bound): one section checks at most 32 candidates.
#[test]
fn a_section_checks_at_most_32_candidates() {
    let mut env = Env::new();
    for i in 0..33 {
        env.source(&[("f", &format!("source {i}"))], OLD);
    }
    env.trust_clock();
    let asked = RefCell::new(0usize);
    let report = env
        .pass_with(u64::MAX, &|_| {
            *asked.borrow_mut() += 1;
            Ok(SourceUse::Live)
        })
        .unwrap();
    assert_eq!(report.detached, 0);
    // every candidate is asked once, over two sections (a third finds no
    // candidate, a fourth only that step 0 has nothing)
    assert_eq!(*asked.borrow(), 33);
    assert_eq!(report.sections, 4, "{report:?}");
}
