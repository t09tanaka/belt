use super::*;
use crate::ids::CoordinatorId;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const DAY: u64 = 24 * 3600 * 1000;

struct NoWorkspace;
impl BuilderWorkspaces for NoWorkspace {
    fn create(&self, _: &AttemptId, _: &AttemptPayload) -> Result<Workspace, ApiError> {
        panic!("unexpected workspace")
    }
    fn remove(&self, _: &Workspace) -> Result<(), ApiError> {
        panic!("unexpected workspace cleanup")
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    state: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = crate::test_tmp::tempdir().unwrap();
        let state = std::fs::canonicalize(temp.path()).unwrap().join("state");
        private_dir(&state).unwrap();
        private_dir(&state.join("toolchains")).unwrap();
        Self { _temp: temp, state }
    }
    /// A root installed with its receipt (as an install leaves it).
    fn root(&self, identity: &str) -> PathBuf {
        let root = self.state.join("toolchains").join(identity);
        private_dir(&root).unwrap();
        private_dir(&root.join("bin")).unwrap();
        std::fs::write(root.join("bin/node"), b"tool").unwrap();
        std::fs::write(root.join("receipt.json"), b"{}").unwrap();
        root
    }
    fn open(&self) -> Arc<Setups> {
        let cfg = SetupConfig::new(
            self.state.clone(),
            crate::ids::WorkerId::parse("w-roots").unwrap(),
            "/bin/sh".into(),
        );
        let s = Setups::open(cfg, Arc::new(NoWorkspace)).unwrap();
        // What the agent's first tick does (on its own thread there).
        s.db();
        s.roots_pass(platform::wall_ms());
        s
    }
    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.state.join("worker.db")).unwrap()
    }
    /// The root was last used `days` days before `now`.
    fn used(&self, identity: &str, at_ms: u64) {
        self.db()
            .execute(
                "UPDATE toolchain_root SET last_used_ms = ?2 WHERE identity = ?1",
                rusqlite::params![identity, at_ms as i64],
            )
            .unwrap();
    }
    fn rows(&self) -> Vec<String> {
        let db = self.db();
        let mut stmt = db
            .prepare("SELECT identity FROM toolchain_root ORDER BY identity")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
    fn attempt(&self, state: &str) -> AttemptId {
        let db = self.db();
        crate::service::worker_service::tests::put_attempt(
            &db,
            &CoordinatorId::generate(),
            1,
            state,
        );
        let id: String = db
            .query_row(
                "SELECT attempt_id FROM worker_attempt ORDER BY rowid DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        AttemptId::parse(&id).unwrap()
    }
}

fn plan_of(s: &Setups, identity: &str) -> PathBuf {
    s.toolchains().join(identity)
}

/// Spec 25.7 (b) acceptance (vii): a root installed before these tables (a
/// receipt and no row) gets a `live` row at the first start, is bound
/// without being installed again, and goes when unused for 14 days; a root
/// without a receipt gets no row.
#[test]
fn a_root_from_before_gets_a_row_at_the_first_start_and_goes_when_unused() {
    let f = Fixture::new();
    let root = f.root(A);
    let partial = f.state.join("toolchains").join(B);
    private_dir(&partial).unwrap();
    let s = f.open();
    assert_eq!(f.rows(), vec![A.to_string()]);
    let warm = AttemptId::generate();
    assert!(
        s.db()
            .as_ref()
            .unwrap()
            .toolchain_bind(A, warm.as_str(), 1)
            .unwrap()
            == crate::storage::Bind::Bound
    );
    s.release_warmup_roots(&warm);
    let now = platform::wall_ms();
    assert!(s.roots_pass(now).removed.is_empty(), "used just now");
    f.used(A, now - 15 * DAY);
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
    assert!(!root.exists());
    assert!(f.rows().is_empty());
    assert!(
        partial.exists(),
        "not a root with a receipt: not ours to judge"
    );
}

/// Spec 25.7 (b) acceptance (ii): a root an attempt that is not terminal,
/// a running builder, a warm-up or a template references stays, however
/// old; each alone.
#[test]
fn a_referenced_root_stays_for_each_kind_of_holder() {
    let f = Fixture::new();
    let root = f.root(A);
    let s = f.open();
    let db = s.db().unwrap();
    let now = platform::wall_ms();
    let old = now - 30 * DAY;
    let check = |what: &str| {
        f.used(A, old);
        assert!(s.roots_pass(now).removed.is_empty(), "{what}");
        assert!(root.exists(), "{what}");
    };
    // An attempt: until it is terminal.
    let a = f.attempt("active");
    // Accepted after the first start: held by its reference alone.
    f.db()
        .execute(
            "UPDATE worker_attempt SET accepted_at_ms = ?1",
            [now as i64],
        )
        .unwrap();
    assert_eq!(
        db.toolchain_bind(A, a.as_str(), now).unwrap(),
        crate::storage::Bind::Bound
    );
    check("attempt");
    f.db()
        .execute(
            "UPDATE worker_attempt SET state = 'terminal', termination_json = '{}', terminal_at_ms = 1 WHERE attempt_id = ?1",
            [a.as_str()],
        )
        .unwrap();
    // A builder: while its directory is there.
    let builder = BuilderId::generate();
    private_dir(&s.cfg.builders().join(builder.as_str())).unwrap();
    assert!(s.add_root_refs(vec![A.into()], "builder", builder.as_str()));
    check("builder");
    std::fs::remove_dir(s.cfg.builders().join(builder.as_str())).unwrap();
    // A warm-up: until it is released.
    let warm = AttemptId::generate();
    assert_eq!(
        db.toolchain_bind(A, warm.as_str(), now).unwrap(),
        crate::storage::Bind::Bound
    );
    check("warm-up");
    s.release(&warm);
    // A template: while it is in the store.
    let template = s.store.root_for_test().join("tpl");
    private_dir(&template).unwrap();
    std::fs::write(template.join("receipt.json"), b"{}").unwrap();
    assert!(s.add_root_refs(vec![A.into()], "template", "tpl"));
    check("template");
    std::fs::remove_file(template.join("receipt.json")).unwrap();
    f.used(A, old);
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
    assert!(!root.exists());
}

/// Spec 25.7 (b) acceptance (iii): a removal claimed and then cut short
/// (the root still there, the row `evicting`) is finished at the next start.
#[test]
fn a_removal_cut_short_is_finished_at_the_next_start() {
    let f = Fixture::new();
    let root = f.root(A);
    drop(f.open());
    assert!(crate::storage::WorkerStore::open(
        &f.state,
        &crate::ids::WorkerId::parse("w-roots").unwrap()
    )
    .unwrap()
    .toolchain_claim(A, None)
    .unwrap());
    // A bind meanwhile treats the root as not there.
    let s = f.open();
    assert!(!root.exists());
    assert!(f.rows().is_empty());
    assert_eq!(
        s.bind_root(&AttemptId::generate(), &plan_of(&s, A)),
        RootBind::Absent
    );
}

/// Spec 25.7 (b) acceptance (iv) and (vi): a builder directory with no
/// reference row (a version that records none, or a builder whose helper
/// may still run: its directory is removed only after its supervisor
/// confirmed every process it started stopped) stops every removal; once
/// it is gone, the next pass goes on.
#[test]
fn a_builder_directory_without_references_holds_every_removal() {
    let f = Fixture::new();
    let root = f.root(A);
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 30 * DAY);
    let old_builder = s.cfg.builders().join("01OLDBUILDER");
    private_dir(&old_builder).unwrap();
    assert_eq!(s.roots_pass(now).held, Some("builder_without_references"));
    assert!(root.exists());
    std::fs::remove_dir(&old_builder).unwrap();
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
}

/// Spec 25.7 (b) acceptance (vi): the reference of a builder stays while
/// its directory does (its helper may outlive a killed supervisor until the
/// recovery confirms it stopped) and goes at the first pass after.
#[test]
fn a_builder_reference_goes_only_after_its_directory() {
    let f = Fixture::new();
    let root = f.root(A);
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 30 * DAY);
    let builder = BuilderId::generate();
    private_dir(&s.cfg.builders().join(builder.as_str())).unwrap();
    assert!(s.hold_root_for_install(&builder, &plan_of(&s, A)).is_ok());
    for _ in 0..2 {
        assert!(s.roots_pass(now).removed.is_empty());
        assert!(root.exists());
    }
    std::fs::remove_dir(s.cfg.builders().join(builder.as_str())).unwrap();
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
}

/// Spec 25.7 (b) acceptance (v): unused for 14 days goes, used within them
/// stays; over 20 GiB of measured roots, the least recently used
/// unreferenced one goes, one per pass.
#[test]
fn fourteen_days_and_twenty_gib() {
    let f = Fixture::new();
    f.root(A);
    f.root(B);
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 13 * DAY);
    f.used(B, now - 12 * DAY);
    assert!(s.roots_pass(now).removed.is_empty());
    assert_eq!(f.rows().len(), 2);
    // Measured: small, nothing goes.
    assert!(s.roots_pass(now).removed.is_empty());
    let db = s.db().unwrap();
    db.toolchain_set_bytes(A, Some(11 << 30), now).unwrap();
    db.toolchain_set_bytes(B, Some(10 << 30), now).unwrap();
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
    assert_eq!(f.rows(), vec![B.to_string()]);
    assert!(s.roots_pass(now).removed.is_empty(), "10 GiB is within");
    f.used(B, now - 15 * DAY);
    assert_eq!(s.roots_pass(now).removed, vec![B.to_string()]);
}

/// 25.1 principle 8: nothing goes while an upgrade is on trial.
#[test]
fn nothing_goes_on_trial() {
    let f = Fixture::new();
    let root = f.root(A);
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 30 * DAY);
    crate::worker::control::begin_trial(&f.state).unwrap();
    assert_eq!(s.roots_pass(now).held, Some("trial"));
    assert!(root.exists());
    crate::worker::control::end_trial(&f.state).unwrap();
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
}

/// The roots a setup builder or template runs from are read off the
/// environment of its execution binding.
#[test]
fn the_roots_of_a_binding_are_those_on_its_paths() {
    let toolchains = Path::new("/state/toolchains");
    let env = BTreeMap::from([
        (
            "PATH".to_string(),
            format!("/state/toolchains/{A}/bin:/usr/bin:/state/toolchains/{B}/x/bin"),
        ),
        (
            "OTHER".to_string(),
            "/state/toolchains/short/bin".to_string(),
        ),
    ]);
    assert_eq!(
        roots_in(&env, toolchains),
        vec![A.to_string(), B.to_string()]
    );
}

/// An attempt accepted before the first start with these tables bound its
/// roots without a reference: nothing goes until it is terminal.
#[test]
fn an_attempt_from_before_the_tables_holds_every_removal() {
    let f = Fixture::new();
    let root = f.root(A);
    let a = {
        drop(
            crate::storage::WorkerStore::open(
                &f.state,
                &crate::ids::WorkerId::parse("w-roots").unwrap(),
            )
            .unwrap(),
        );
        f.attempt("active")
    };
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 30 * DAY);
    assert_eq!(s.roots_pass(now).held, Some("attempt_from_before"));
    assert!(root.exists());
    f.db()
        .execute(
            "UPDATE worker_attempt SET state = 'terminal', termination_json = '{}', terminal_at_ms = 1 WHERE attempt_id = ?1",
            [a.as_str()],
        )
        .unwrap();
    assert_eq!(s.roots_pass(now).removed, vec![A.to_string()]);
}

/// Review M1: a worker DB that cannot be opened is not remembered as none:
/// meanwhile nothing is bound, installed into or removed, and the next use
/// opens it.
#[test]
fn a_database_that_cannot_be_opened_is_opened_again_later() {
    let f = Fixture::new();
    f.root(A);
    let blocker = f.state.join("worker.db");
    private_dir(&blocker).unwrap();
    let cfg = SetupConfig::new(
        f.state.clone(),
        crate::ids::WorkerId::parse("w-roots").unwrap(),
        "/bin/sh".into(),
    );
    let s = Setups::open(cfg, Arc::new(NoWorkspace)).unwrap();
    assert!(s.db().is_none());
    let a = AttemptId::generate();
    assert_eq!(s.bind_root(&a, &plan_of(&s, A)), RootBind::NoDatabase);
    assert!(s
        .hold_root_for_install(&BuilderId::generate(), &plan_of(&s, A))
        .is_err());
    assert!(!s.add_root_refs(vec![A.into()], "builder", "b"));
    assert_eq!(s.roots_pass(platform::wall_ms()).held, Some("no_database"));
    std::fs::remove_dir(&blocker).unwrap();
    assert!(s.db().is_some());
    assert_eq!(s.bind_root(&a, &plan_of(&s, A)), RootBind::Bound);
}

/// Review M3: a removal that fails in a way waiting does not cure puts the
/// root back (used now, so it is not picked again at once), is counted for
/// `doctor`, and the passes go on to remove the others.
#[test]
fn a_removal_that_fails_puts_the_root_back_and_the_passes_go_on() {
    let f = Fixture::new();
    let a = f.root(A);
    let b = f.root(B);
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 31 * DAY);
    f.used(B, now - 30 * DAY);
    // Group-writable: `unsafe_private_directory`, every time.
    std::fs::set_permissions(&a, std::fs::Permissions::from_mode(0o770)).unwrap();
    assert!(s.roots_pass(now).removed.is_empty());
    let db = s.db().unwrap();
    let rows = db.toolchain_roots().unwrap();
    let row = rows.iter().find(|r| r.identity == A).unwrap();
    assert!(!row.evicting, "back to live");
    assert!(row.last_used_ms >= now);
    assert_eq!(db.toolchain_status().unwrap().evict_failures, 1);
    assert_eq!(
        doctor_notices(&f.state, now).len(),
        1,
        "{:?}",
        doctor_notices(&f.state, now)
    );
    assert_eq!(s.roots_pass(now).removed, vec![B.to_string()]);
    assert!(a.exists() && !b.exists());
    assert_eq!(db.toolchain_status().unwrap().evict_failures, 0);
    std::fs::set_permissions(&a, std::fs::Permissions::from_mode(0o700)).unwrap();
}

/// Review M4: a builder or template references only roots that are `live`;
/// one being removed fails the reference and adds none.
#[test]
fn references_are_refused_on_a_root_being_removed() {
    let f = Fixture::new();
    f.root(A);
    f.root(B);
    let s = f.open();
    let db = s.db().unwrap();
    assert!(db.toolchain_claim(A, None).unwrap());
    assert!(!s.add_root_refs(vec![B.into(), A.into()], "builder", "b1"));
    assert!(!db.toolchain_referenced(B).unwrap(), "none of them");
    assert!(s.add_root_refs(vec![B.into()], "builder", "b1"));
    assert!(db.toolchain_referenced(B).unwrap());
}

/// Review L1: a state directory reached through a link (macOS `/var`) still
/// finds the roots on a binding's paths, which are canonical.
#[test]
fn a_linked_state_directory_finds_the_roots_of_a_binding() {
    let temp = crate::test_tmp::tempdir().unwrap();
    let base = std::fs::canonicalize(temp.path()).unwrap();
    let real = base.join("real");
    private_dir(&real.join("toolchains")).unwrap();
    std::os::unix::fs::symlink(&real, base.join("link")).unwrap();
    let cfg = SetupConfig::new(
        base.join("link"),
        crate::ids::WorkerId::parse("w-roots").unwrap(),
        "/bin/sh".into(),
    );
    let s = Setups::open(cfg, Arc::new(NoWorkspace)).unwrap();
    let canonical = s.private_tool_root().unwrap();
    let env = BTreeMap::from([(
        "PATH".to_string(),
        format!("{}/{A}/bin:/usr/bin", canonical.display()),
    )]);
    assert_eq!(roots_in(&env, &s.toolchains()), vec![A.to_string()]);
}

/// Review L4: a root that cannot be measured is stamped too, so the next
/// pass measures another; a root an installer holds is not measured.
#[test]
fn an_unmeasurable_root_does_not_keep_the_others_unmeasured() {
    let f = Fixture::new();
    let a = f.root(A);
    f.root(B);
    let s = f.open();
    let now = platform::wall_ms();
    f.used(A, now - 2 * DAY);
    f.used(B, now - DAY);
    let db = s.db().unwrap();
    let bytes = |id: &str| {
        db.toolchain_roots()
            .unwrap()
            .into_iter()
            .find(|r| r.identity == id)
            .unwrap()
    };
    // Measured by the first pass of `open`: not any more.
    f.db()
        .execute(
            "UPDATE toolchain_root SET bytes = NULL, measured_ms = NULL",
            [],
        )
        .unwrap();
    std::fs::set_permissions(a.join("bin"), std::fs::Permissions::from_mode(0o000)).unwrap();
    s.roots_pass(now);
    s.roots_pass(now + 1);
    std::fs::set_permissions(a.join("bin"), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(bytes(A).bytes, None);
    assert!(bytes(A).measured_ms.is_some());
    assert!(bytes(B).bytes.is_some(), "B was measured at the next pass");
}

/// Review L3: a pass that keeps stopping for the same reason for an hour is
/// told by `doctor`, naming the reason; a missing builders directory is no
/// builder.
#[test]
fn a_pass_that_keeps_stopping_is_told_by_doctor() {
    let f = Fixture::new();
    f.root(A);
    let s = f.open();
    let now = platform::wall_ms();
    let _ = std::fs::remove_dir(s.cfg.builders());
    assert_eq!(s.roots_pass(now).held, None, "no builders directory");
    private_dir(&s.cfg.builders().join("01OLDBUILDER")).unwrap();
    let then = now - 2 * 3600 * 1000;
    assert_eq!(s.roots_pass(then).held, Some("builder_without_references"));
    assert_eq!(s.roots_pass(now).held, Some("builder_without_references"));
    let notices = doctor_notices(&f.state, now);
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert!(
        notices[0].contains("setup builder directory"),
        "{notices:?}"
    );
    assert!(!notices[0].contains(f.state.to_str().unwrap()), "no paths");
    std::fs::remove_dir(s.cfg.builders().join("01OLDBUILDER")).unwrap();
    s.roots_pass(now);
    assert!(doctor_notices(&f.state, now).is_empty());
}
