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
        // (this process has started its recount, as the caller ensures)
        *env.receiver.retention.recounted.lock().unwrap() = Some(std::time::Instant::now());
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
        uses: &dyn Fn(&str, Option<&crate::ids::SourceId>) -> Result<SourceUse>,
    ) -> Result<ReceivedPassReport> {
        self.receiver.received_pass(&ReceivedPassInputs {
            wall: NOW,
            mono: AGE,
            boot: Some("b".into()),
            max_age_ms: AGE,
            cap_bytes: cap,
            uses,
            stop: &|| false,
        })
    }
    fn pass(&self) -> ReceivedPassReport {
        self.pass_with(u64::MAX, &|_, _| Ok(SourceUse::Ended(None)))
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
        .pass_with(u64::MAX, &|s, _| {
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
        .pass_with(u64::MAX, &|_, _| Ok(SourceUse::Ended(Some(YOUNG))))
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
        env.pass_with(cap, &|_, _| Ok(SourceUse::Ended(None)))
            .unwrap()
            .detached,
        0
    );
    env.sql("UPDATE meta SET value='done' WHERE key='bytes_fill'");
    let report = env
        .pass_with(cap, &|_, _| Ok(SourceUse::Ended(None)))
        .unwrap();
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
    let failed = env.pass_with(u64::MAX, &|_, _| Ok(SourceUse::Ended(None)));
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
        .pass_with(u64::MAX, &|_, _| {
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

/// Review M1: nothing is detached while a root waits for its `clone_ref`
/// retry (its time is not known: a missing one would sort it oldest), and
/// nothing by size while a size waits for its retry.
#[test]
fn a_pending_clone_ref_or_bytes_retry_holds_the_rules() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.sql(&format!(
        "UPDATE root SET used_ms=NULL WHERE id='receive:{old}'; INSERT INTO clone_ref_retry VALUES('receive:{old}',1,0)"
    ));
    env.trust_clock();
    assert_eq!(env.pass().detached, 0);
    assert!(env.received(&old));
    env.sql("DELETE FROM clone_ref_retry; INSERT INTO bytes_retry VALUES('b:x',1,0)");
    env.sql(&format!(
        "UPDATE root SET used_ms={YOUNG} WHERE id='receive:{old}'"
    ));
    let ended = |_: &str, _: Option<&crate::ids::SourceId>| Ok(SourceUse::Ended(None));
    assert_eq!(env.pass_with(0, &ended).unwrap().detached, 0);
    assert!(env.received(&old));
    env.sql("DELETE FROM bytes_retry");
    assert_eq!(env.pass_with(0, &ended).unwrap().detached, 1);
}

/// Review M2: nothing goes by size while a recount runs or waits.
#[test]
fn nothing_goes_by_size_during_a_recount() {
    let mut env = Env::new();
    let a = env.source(&[("a", "a")], YOUNG);
    let ended = |_: &str, _: Option<&crate::ids::SourceId>| Ok(SourceUse::Ended(None));
    for key in ["bytes_recount", "bytes_recount_wait"] {
        env.sql(&format!("INSERT INTO meta VALUES('{key}','')"));
        assert_eq!(env.pass_with(0, &ended).unwrap().detached, 0, "{key}");
        assert!(env.received(&a));
        env.sql(&format!("DELETE FROM meta WHERE key='{key}'"));
    }
    assert_eq!(env.pass_with(0, &ended).unwrap().detached, 1);
}

/// Review M3: a blob shared only with a source detached earlier in the
/// section counts as freed: the cap does not take one source too many.
#[test]
fn a_blob_shared_with_a_detached_source_counts_as_freed() {
    let mut env = Env::new();
    let shared = "s".repeat(20_000);
    let a = env.source(&[("a", &"a".repeat(4000)), ("s", &shared)], 10);
    let b = env.source(&[("b", &"b".repeat(4000)), ("s", &shared)], 20);
    let c = env.source(&[("c", &"c".repeat(4000))], 30);
    let cap = env.store_bytes() - 16_000;
    let report = env
        .pass_with(cap, &|_, _| Ok(SourceUse::Ended(None)))
        .unwrap();
    assert_eq!(report.detached, 2, "{report:?}");
    assert!(!env.received(&a) && !env.received(&b));
    assert!(env.received(&c));
    assert!(env.store_bytes() <= cap);
}

/// Review M4: the clone refs of the children a root carried get their
/// `ref_drop:` rows as well.
#[test]
fn a_detach_records_the_carried_children_clone_refs() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.sql(&format!(
        "INSERT INTO clone_ref(clone,source,root) VALUES('c1','{old}','receive:{old}'),('c3','child','receive:{old}')"
    ));
    env.trust_clock();
    assert_eq!(env.pass().detached, 1);
    assert_eq!(
        env.count(&format!(
            "SELECT count(*) FROM meta WHERE key IN ('ref_drop:c1:{old}','ref_drop:c3:child')"
        )),
        2
    );
}

/// Review M5: the pass asks with the CI id (the pins) and the converted
/// `SourceId` (`worker.db`): a pinned source and one a live attempt names
/// stay, the third goes.
#[test]
fn the_pins_and_the_attempts_are_asked_with_their_own_ids() {
    let mut env = Env::new();
    let pinned = env.source(&[("a", "pinned")], OLD);
    let attempted = env.source(&[("b", "attempted")], OLD);
    let free = env.source(&[("c", "free")], OLD);
    let pins = crate::worker::pins::SourcePins::default();
    let _pin = pins.pin(&env.receiver, &pinned).unwrap();
    // (worker.db names sources as `src_<ULID>`)
    let live = crate::source::classify::source_id_of_ci(&attempted)
        .unwrap()
        .to_string();
    assert!(live.starts_with("src_"));
    env.trust_clock();
    let report = env
        .pass_with(u64::MAX, &|ci, id| {
            if pins.pinned(ci) || id.is_some_and(|id| id.as_str() == live) {
                Ok(SourceUse::Live)
            } else {
                Ok(SourceUse::Ended(None))
            }
        })
        .unwrap();
    assert_eq!(report.detached, 1, "{report:?}");
    assert!(env.received(&pinned) && env.received(&attempted));
    assert!(!env.received(&free));
}

/// Review L1: an empty abandoned root goes with its graph rows.
#[test]
fn an_empty_root_goes_with_its_graph_rows() {
    let env = Env::new();
    env.sql("INSERT INTO root(id,kind,source) VALUES('drop:x:1','abandoned',NULL); INSERT INTO graph(root,source,done) VALUES('drop:x:1','y',1)");
    let (what, _) = env.receiver.retention.leftovers_with(1 << 20).unwrap();
    assert_eq!(what, "progress");
    assert_eq!(
        env.count("SELECT count(*) FROM root WHERE id='drop:x:1'"),
        0
    );
    assert_eq!(
        env.count("SELECT count(*) FROM graph WHERE root='drop:x:1'"),
        0
    );
}

/// Review L2: the reads of files that cannot be proven count against the
/// section budget, and the section ends once they took half of it.
#[test]
fn unproven_reads_spend_the_section_budget() {
    let mut env = Env::new();
    let mut files = Vec::new();
    for i in 0..3 {
        let content = format!("{i}").repeat(30);
        let id = env.source(&[("f", &content)], YOUNG);
        let hash = env
            .receiver
            .manifest(&id)
            .unwrap()
            .unwrap()
            .object_manifest
            .entries[0]
            .hash
            .clone();
        // the source becomes a `drop:` root holding only its blob, whose
        // size is not counted and whose content changed
        env.sql(&format!(
            "INSERT INTO root(id,kind,source) VALUES('drop:{id}:1','abandoned',NULL);
             DELETE FROM member WHERE root='receive:{id}' AND object<>'b:{hash}';
             UPDATE member SET root='drop:{id}:1' WHERE root='receive:{id}';
             DELETE FROM root WHERE id='receive:{id}';
             DELETE FROM object_bytes WHERE id='b:{hash}'"
        ));
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
        std::fs::write(&file, "x".repeat(30)).unwrap();
        files.push(file);
    }
    // 100 bytes: the first two reads (30 each) reach half of it
    let (what, report) = env.receiver.retention.leftovers_with(100).unwrap();
    assert_eq!(what, "stuck");
    assert_eq!(report.kept, 2, "{report:?}");
    assert!(files.iter().all(|f| f.exists()));
}

/// Review L3: a claim the section cannot verify is not drained inside it;
/// the pass does not stop on it.
#[test]
fn a_claim_larger_than_the_rest_of_the_budget_waits() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.trust_clock();
    set_fault(Some("after_claim"));
    let failed = env.pass_with(u64::MAX, &|_, _| Ok(SourceUse::Ended(None)));
    set_fault(None);
    assert!(failed.is_err());
    let pending = env.count("SELECT count(*) FROM source_delete_claim WHERE done=0");
    assert!(pending > 0);
    // (a claim the whole section cannot hash is hashed with the lock let
    // go, spec (d) 「claimの再実行」)
    let (what, _) = env.receiver.retention.leftovers_with(1).unwrap();
    assert_eq!(what, "large");
    assert_eq!(
        env.count("SELECT count(*) FROM source_delete_claim WHERE done=0"),
        pending
    );
    let report = env.pass();
    assert!(!report.stuck, "{report:?}");
    assert!(!env.received(&old));
}

/// Review L4: step 0's page and the candidates follow an index: no scan of
/// a whole table and no sort of every received root.
#[test]
fn the_pass_queries_follow_an_index() {
    let env = Env::new();
    let c = env.receiver.retention.connect().unwrap();
    for sql in super::deletion::PLANNED {
        let mut s = c.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let args: Vec<i64> = vec![0; s.parameter_count()];
        let plan: Vec<String> = s
            .query_map(rusqlite::params_from_iter(args), |r| r.get::<_, String>(3))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let text = plan.join("\n");
        eprintln!("{text}");
        assert!(!text.contains("TEMP B-TREE"), "{sql}\n{text}");
        assert!(!plan.iter().any(|l| l.starts_with("SCAN")), "{sql}\n{text}");
    }
}

/// Moves source `id`'s blob alone into an abandoned root `root`; `counted`
/// keeps its `object_bytes` row. Returns the blob's path.
fn abandon_blob(env: &Env, id: &str, root: &str, counted: bool) -> std::path::PathBuf {
    let hash = env
        .receiver
        .manifest(id)
        .unwrap()
        .unwrap()
        .object_manifest
        .entries[0]
        .hash
        .clone();
    env.sql(&format!(
        "INSERT INTO root(id,kind,source) VALUES('{root}','abandoned',NULL);
         DELETE FROM member WHERE root='receive:{id}' AND object<>'b:{hash}';
         UPDATE member SET root='{root}' WHERE root='receive:{id}';
         DELETE FROM root WHERE id='receive:{id}'"
    ));
    if !counted {
        env.sql(&format!("DELETE FROM object_bytes WHERE id='b:{hash}'"));
    }
    let path: String = env
        .receiver
        .retention
        .connect()
        .unwrap()
        .query_row("SELECT path FROM object WHERE id='b:'||?1", [&hash], |r| {
            r.get(0)
        })
        .unwrap();
    env.receiver.root().join(path)
}

/// Review round 2 (b), as changed by the large-blob rule: a member whose
/// size is not counted is looked at (`lstat` through its checked
/// ancestors) before it is read, so it is read only when it fits.
#[test]
fn an_unknown_size_is_looked_at_before_it_is_read() {
    let mut env = Env::new();
    let a = env.source(&[("f", &"a".repeat(200))], YOUNG);
    let b = env.source(&[("f", &"b".repeat(100))], YOUNG);
    let c = env.source(&[("f", &"c".repeat(300))], YOUNG);
    let fa = abandon_blob(&env, &a, "drop:a:1", true);
    let fb = abandon_blob(&env, &b, "drop:b:1", true);
    let fc = abandon_blob(&env, &c, "drop:c:1", false);
    // 1,000: the two known files (200, then 100; each read twice) leave
    // 400; the uncounted one is 300, more than half of that: it waits
    let (what, report) = env.receiver.retention.leftovers_with(1000).unwrap();
    assert_eq!(what, "progress");
    assert_eq!(report.unlinked, 2, "{report:?}");
    assert!(!fa.exists() && !fb.exists());
    assert!(fc.exists());
    // the next section takes it
    let (_, report) = env.receiver.retention.leftovers_with(1000).unwrap();
    assert_eq!(report.unlinked, 1, "{report:?}");
    assert!(!fc.exists());
}

/// Review round 2 (a): step 0 pages only roots with members; an empty
/// abandoned `import:` root still goes, with its token's unused session.
#[test]
fn an_empty_import_root_goes_with_its_session() {
    let env = Env::new();
    env.sql(
        "INSERT INTO root(id,kind,source) VALUES('import:t1','abandoned',NULL);
         INSERT INTO import_session(token,state) VALUES('t1','pending')",
    );
    let (what, report) = env.receiver.retention.leftovers_with(1 << 20).unwrap();
    assert_eq!(what, "progress", "{report:?}");
    assert_eq!(
        env.count("SELECT count(*) FROM root WHERE id='import:t1'"),
        0
    );
    assert_eq!(
        env.count("SELECT count(*) FROM import_session WHERE token='t1'"),
        0
    );
}

/// Caller condition L5: nothing is detached before this process started
/// its recount (the rewind of the fills).
#[test]
fn nothing_is_detached_before_the_process_recounts() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.trust_clock();
    *env.receiver.retention.recounted.lock().unwrap() = None;
    let report = env.pass();
    assert_eq!(report.detached, 0, "{report:?}");
    assert!(env.received(&old));
    // the recount starts (and, the fills done again, ends)
    while !env.receiver.retention.recount_bytes(1000).unwrap() {
        env.sql("UPDATE meta SET value='done' WHERE key IN ('bytes_fill','clone_ref_fill')");
    }
    assert!(env.receiver.retention.recounted.lock().unwrap().is_some());
    env.trust_clock();
    let report = env.pass();
    assert_eq!(report.detached, 1, "{report:?}");
    assert!(!env.received(&old));
}

/// Review round 1 (M1, L1): a pass whose `stop` is true (the agent is
/// stopping, or a version is on trial) runs no section and detaches nothing.
#[test]
fn a_stopped_pass_runs_no_section() {
    let mut env = Env::new();
    let old = env.source(&[("a", "old")], OLD);
    env.trust_clock();
    let report = env
        .receiver
        .received_pass(&ReceivedPassInputs {
            wall: NOW,
            mono: AGE,
            boot: Some("b".into()),
            max_age_ms: AGE,
            cap_bytes: u64::MAX,
            uses: &|_, _| Ok(SourceUse::Ended(None)),
            stop: &|| true,
        })
        .unwrap();
    assert_eq!(report.sections, 0, "{report:?}");
    assert_eq!(report.detached, 0, "{report:?}");
    assert!(env.received(&old));
    // the same pass, not stopped, detaches it
    assert_eq!(env.pass().detached, 1);
}

/// Review round 1 (L2): a section removes at most 64 empty roots.
#[test]
fn empty_roots_go_a_bounded_number_a_section() {
    let env = Env::new();
    let rows: String = (0..100)
        .map(|i| format!("INSERT INTO root(id,kind,source) VALUES('drop:e:{i}','abandoned',NULL);"))
        .collect();
    env.sql(&rows);
    let (what, _) = env.receiver.retention.leftovers_with(1 << 20).unwrap();
    assert_eq!(what, "progress");
    assert_eq!(
        env.count("SELECT count(*) FROM root WHERE id LIKE 'drop:e:%'"),
        36
    );
    let (_, _) = env.receiver.retention.leftovers_with(1 << 20).unwrap();
    assert_eq!(
        env.count("SELECT count(*) FROM root WHERE id LIKE 'drop:e:%'"),
        0
    );
}

/// A pass whose section budget is `bytes` (64 MiB in production).
fn pass_budget(env: &Env, bytes: u64) -> ReceivedPassReport {
    super::deletion::set_budget(bytes);
    let report = env.pass();
    super::deletion::set_budget(64 * 1024 * 1024);
    report
}

/// (g)14, (g)41: a blob larger than the in-lock proof may read (half a
/// section) is claimed, hashed with the lock let go and unlinked, one per
/// pass; no claim is left between passes.
#[test]
fn a_large_blob_goes_one_per_pass() {
    let mut env = Env::new();
    let a = env.source(&[("f", &"a".repeat(600))], YOUNG);
    let b = env.source(&[("f", &"b".repeat(600))], YOUNG);
    let fa = abandon_blob(&env, &a, "drop:a:1", true);
    let fb = abandon_blob(&env, &b, "drop:b:1", true);
    let report = pass_budget(&env, 1000);
    assert_eq!(report.unlinked, 1, "{report:?}");
    assert_eq!(env.count("SELECT count(*) FROM source_delete_claim"), 0);
    assert!(!fa.exists() && fb.exists());
    let report = pass_budget(&env, 1000);
    assert_eq!(report.unlinked, 1, "{report:?}");
    assert!(!fb.exists());
    assert_eq!(
        env.count("SELECT count(*) FROM root WHERE id LIKE 'drop:%'"),
        0
    );
    assert_eq!(env.count("SELECT count(*) FROM source_delete_claim"), 0);
    assert_eq!(env.store_bytes(), env.files_on_disk());
}

/// A large blob whose content is not its name withdraws its claim: the
/// file and its membership stay, no claim is left, the root waits for its
/// next try, and `begin_import` is not refused.
#[test]
fn a_large_blob_that_is_not_its_content_stays() {
    let mut env = Env::new();
    let a = env.source(&[("f", &"a".repeat(600))], YOUNG);
    let fa = abandon_blob(&env, &a, "drop:a:1", true);
    let mut perm = std::fs::metadata(&fa).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o600);
    std::fs::set_permissions(&fa, perm).unwrap();
    std::fs::write(&fa, "x".repeat(600)).unwrap();
    let report = pass_budget(&env, 1000);
    assert_eq!((report.unlinked, report.kept), (0, 1), "{report:?}");
    assert!(fa.exists());
    assert_eq!(
        env.count("SELECT count(*) FROM member WHERE root='drop:a:1'"),
        1
    );
    assert_eq!(env.count("SELECT count(*) FROM source_delete_claim"), 0);
    assert_eq!(
        env.count("SELECT count(*) FROM publication WHERE state='claimed'"),
        0
    );
    assert_eq!(
        env.count("SELECT count(*) FROM drop_retry WHERE root='drop:a:1'"),
        1
    );
    drop(env.receiver.begin_import().unwrap());
}

/// (g)39: a large claim left by a crash is finished by the next pass, and
/// `begin_import` is not refused after it.
#[test]
fn a_large_claim_left_by_a_crash_is_finished_by_the_next_pass() {
    let mut env = Env::new();
    let a = env.source(&[("f", &"a".repeat(600))], YOUNG);
    let fa = abandon_blob(&env, &a, "drop:a:1", true);
    set_fault(Some("after_large_claim"));
    super::deletion::set_budget(1000);
    let failed = env.pass_with(u64::MAX, &|_, _| Ok(SourceUse::Ended(None)));
    set_fault(None);
    assert!(failed.is_err());
    assert_eq!(
        env.count("SELECT count(*) FROM source_delete_claim WHERE done=0"),
        1
    );
    // (the membership stays until the claim completes)
    assert_eq!(
        env.count("SELECT count(*) FROM member WHERE root='drop:a:1'"),
        1
    );
    let report = pass_budget(&env, 1000);
    assert!(!report.stuck, "{report:?}");
    assert_eq!(report.unlinked, 1, "{report:?}");
    assert!(!fa.exists());
    assert_eq!(env.count("SELECT count(*) FROM source_delete_claim"), 0);
    assert_eq!(
        env.count("SELECT count(*) FROM member WHERE root='drop:a:1'"),
        0
    );
    drop(env.receiver.begin_import().unwrap());
}

/// A root with an unproven member is read again only when its next try is
/// due (spec 25.8 5b, rootごとの次に試す時刻).
#[test]
fn an_unproven_root_waits_for_its_next_try() {
    let mut env = Env::new();
    let a = env.source(&[("f", &"a".repeat(30))], YOUNG);
    let fa = abandon_blob(&env, &a, "drop:a:1", true);
    let mut perm = std::fs::metadata(&fa).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o600);
    std::fs::set_permissions(&fa, perm).unwrap();
    std::fs::write(&fa, "x".repeat(30)).unwrap();
    let (what, report) = env.receiver.retention.leftovers_with(1000).unwrap();
    assert_eq!((what, report.kept), ("stuck", 1));
    assert_eq!(
        env.count("SELECT count(*) FROM drop_retry WHERE root='drop:a:1'"),
        1
    );
    // not due: nothing is read
    let (what, report) = env.receiver.retention.leftovers_with(1000).unwrap();
    assert_eq!((what, report.kept), ("none", 0));
    // due: read again
    env.sql("UPDATE drop_retry SET next_ms=0");
    let (what, report) = env.receiver.retention.leftovers_with(1000).unwrap();
    assert_eq!((what, report.kept), ("stuck", 1));
    // the row goes with its root
    env.sql("DELETE FROM member WHERE root='drop:a:1'");
    env.receiver.retention.leftovers_with(1000).unwrap();
    assert_eq!(env.count("SELECT count(*) FROM drop_retry"), 0);
}

/// Spec 25.8 5b: the agent tick's token patrol takes no exclusive lock: a
/// file only an abandoned token holds is left to step 0 of the pass.
#[test]
fn the_tick_leaves_unshared_token_files_to_the_pass() {
    let mut env = Env::new();
    let a = env.source(&[("f", "token")], YOUNG);
    let fa = abandon_blob(&env, &a, "import:01ARZ3NDEKTSV4RRFFQ69G5FAV", true);
    let report = env.receiver.release_abandoned_imports().unwrap();
    assert_eq!(report.unlinked, 0, "{report:?}");
    assert!(fa.exists());
    let report = env.pass();
    assert_eq!(report.unlinked, 1, "{report:?}");
    assert!(!fa.exists());
}
