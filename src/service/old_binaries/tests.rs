use super::*;
use crate::ids::{CoordinatorId, WorkerId};
use std::os::unix::fs::{symlink, PermissionsExt};

const OLD: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const NEW: &str = "2222222222222222222222222222222222222222222222222222222222222222";

/// A host whose `bin/current` points at the new version, with the old one
/// placed long ago and a tool link of the old one. Nothing names the old
/// version until a test adds exactly one thing that does.
struct Fixture {
    _dir: tempfile::TempDir,
    places: Places,
}

impl Fixture {
    fn new() -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path();
        let places = Places {
            config: root.join("config"),
            worker_state: root.join("state"),
            parent_state: Some(root.join("parent")),
            services: vec![root.join("services")],
        };
        for d in [
            places.bin(),
            places.worker_state.clone(),
            places.parent_state.clone().unwrap(),
            places.services[0].clone(),
        ] {
            fs::create_dir_all(d).unwrap();
        }
        let f = Self { _dir: dir, places };
        for v in [OLD, NEW] {
            fs::create_dir(f.version(v)).unwrap();
            fs::write(f.version(v).join("belt"), v).unwrap();
            f.age(v);
        }
        symlink(NEW, f.places.bin().join("current")).unwrap();
        let tool = f.tool_bin();
        fs::create_dir_all(&tool).unwrap();
        symlink(f.version(OLD).join("belt"), tool.join("portool")).unwrap();
        f
    }
    fn version(&self, v: &str) -> PathBuf {
        self.places.bin().join(v)
    }
    fn tool_bin(&self) -> PathBuf {
        self.places.worker_state.join("tool-bin").join("abc")
    }
    /// Placed an hour ago.
    fn age(&self, v: &str) {
        let then = SystemTime::now() - Duration::from_secs(3600);
        File::open(self.version(v))
            .unwrap()
            .set_modified(then)
            .unwrap();
    }
    fn sweep(&self) -> Outcome {
        sweep(&self.places, &mut || Ok(false), false, SystemTime::now())
    }
    /// Swept, and the old version is still there with its tool link.
    fn assert_kept(&self) -> Outcome {
        let outcome = self.sweep();
        assert!(self.version(OLD).join("belt").exists(), "{outcome:?}");
        assert!(self.tool_bin().exists());
        assert!(self.version(NEW).exists());
        outcome
    }
    fn assert_removed(&self) {
        let outcome = self.sweep();
        assert_eq!(outcome.removed, vec![OLD.to_string()], "{outcome:?}");
        assert!(!self.version(OLD).exists());
        assert!(!self.tool_bin().exists());
        assert!(self.version(NEW).join("belt").exists());
    }
}

/// Acceptance (i): a version nothing names goes, with its tool link; the
/// one `bin/current` points at stays.
#[test]
fn an_unnamed_version_goes_with_its_tool_link() {
    Fixture::new().assert_removed();
}

/// Acceptance (ii), `bin/resident` (it may point elsewhere than `current`).
#[test]
fn a_version_bin_resident_points_at_is_kept() {
    let f = Fixture::new();
    symlink(OLD, f.places.bin().join("resident")).unwrap();
    f.assert_kept();
}

/// Acceptance (ii), `bin/current`.
#[test]
fn a_version_bin_current_points_at_is_kept() {
    let f = Fixture::new();
    fs::remove_file(f.places.bin().join("current")).unwrap();
    symlink(OLD, f.places.bin().join("current")).unwrap();
    symlink(NEW, f.places.bin().join("resident")).unwrap();
    f.assert_kept();
}

/// Acceptance (ii), a registered service definition (LaunchAgent or unit)
/// that runs the version's path.
#[test]
fn a_version_a_service_definition_runs_is_kept() {
    let f = Fixture::new();
    let exe = f.version(OLD).join("belt");
    fs::write(
        f.places.services[0].join("dev.belt.worker.plist"),
        format!("<string>{}</string>", exe.display()),
    )
    .unwrap();
    f.assert_kept();
}

/// Acceptance (ii), the resident's journal (`init.json`, which also holds
/// the version a trial goes back to).
#[test]
fn a_version_the_resident_journal_names_is_kept() {
    let f = Fixture::new();
    fs::write(
        f.places.config.join("init.json"),
        format!("{{\"binary_hash\":\"{OLD}\"}}"),
    )
    .unwrap();
    f.assert_kept();
}

/// Acceptance (ii), the worker's journal: the version it runs, which is the
/// path a system user's worker pins (`system_user::check_boot` runs
/// `bin/<binary_sha256>/belt`), and the version an unfinished switch goes
/// back to.
#[test]
fn a_version_the_worker_journal_pins_is_kept() {
    let f = Fixture::new();
    fs::write(
        f.places.config.join("worker-service.json"),
        format!("{{\"binary_sha256\":\"{NEW}\",\"switch\":{{\"from\":\"{OLD}\"}}}}"),
    )
    .unwrap();
    f.assert_kept();
}

/// Acceptance (ii) and (iv): while an upgrade is on trial nothing goes:
/// the record of the trial, or the trial marker of the worker or of the
/// parent.
#[test]
fn nothing_goes_while_an_upgrade_is_on_trial() {
    let f = Fixture::new();
    let markers = [
        f.places.config.join("upgrade").join("record.json"),
        f.places.worker_state.join("upgrade-trial.json"),
        f.places
            .parent_state
            .clone()
            .unwrap()
            .join("upgrade-trial.json"),
    ];
    for marker in &markers {
        fs::create_dir_all(marker.parent().unwrap()).unwrap();
        fs::write(marker, b"{}").unwrap();
        assert_eq!(f.assert_kept().held, Some(Held::Trial));
        fs::remove_file(marker).unwrap();
    }
    f.assert_removed();
}

/// Acceptance (ii), the upgrade guard: when it is there by any account,
/// nothing goes (the two ways it can be there are in `init::upgrade`'s tests).
#[test]
fn nothing_goes_while_the_guard_is_there() {
    let f = Fixture::new();
    let outcome = sweep(&f.places, &mut || Ok(true), false, SystemTime::now());
    assert_eq!(outcome.held, Some(Held::Guard));
    let outcome = sweep(
        &f.places,
        &mut || Err(anyhow::anyhow!("journal unreadable")),
        false,
        SystemTime::now(),
    );
    assert_eq!(outcome.held, Some(Held::Unreadable));
    assert!(f.version(OLD).exists());
    f.assert_removed();
}

/// Acceptance (ii) and (iii), a setup builder: while its directory is there
/// nothing goes; after it is gone, the next start removes the version.
#[test]
fn a_running_builder_keeps_every_version_until_it_is_gone() {
    let f = Fixture::new();
    let builder = f.places.worker_state.join("setup/builders/b1");
    fs::create_dir_all(&builder).unwrap();
    assert_eq!(f.assert_kept().held, Some(Held::Builder));
    fs::remove_dir(&builder).unwrap();
    f.assert_removed();
}

/// Acceptance (ii), a supervisor: an attempt that is not terminal keeps
/// every version (its supervisor runs a version path and starts again by it).
#[test]
fn a_live_attempt_keeps_every_version() {
    let f = Fixture::new();
    drop(
        crate::storage::WorkerStore::open(&f.places.worker_state, &WorkerId::parse("w").unwrap())
            .unwrap(),
    );
    let db = rusqlite::Connection::open(f.places.worker_state.join("worker.db")).unwrap();
    crate::service::worker_service::tests::put_attempt(
        &db,
        &CoordinatorId::generate(),
        1,
        "active",
    );
    drop(db);
    assert_eq!(f.assert_kept().held, Some(Held::Supervisor));
}

/// A command that places a version holds `init.lock` or
/// `worker-service.lock` until a journal names it: while either is held by
/// someone else, nothing goes.
#[test]
fn nothing_goes_while_a_version_is_being_placed() {
    let f = Fixture::new();
    for name in ["init.lock", "worker-service.lock"] {
        let lock = try_lock(&f.places.config.join(name), true)
            .unwrap()
            .unwrap();
        assert_eq!(f.assert_kept().held, Some(Held::Placing));
        drop(lock);
    }
    f.assert_removed();
}

/// A version directory placed moments ago may be one a first `init` is
/// placing before `init.lock` exists: it is kept.
#[test]
fn a_version_placed_moments_ago_is_kept() {
    let f = Fixture::new();
    File::open(f.version(OLD))
        .unwrap()
        .set_modified(SystemTime::now())
        .unwrap();
    assert_eq!(f.assert_kept().held, None);
}

/// A journal that cannot be read may name the version: nothing goes.
#[test]
fn an_unreadable_journal_keeps_every_version() {
    let f = Fixture::new();
    let journal = f.places.config.join("init.json");
    fs::write(&journal, b"{}").unwrap();
    fs::set_permissions(&journal, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&journal).is_ok() {
        // Run as root: the mode does not stop a read.
        return;
    }
    assert_eq!(f.assert_kept().held, Some(Held::Unreadable));
}

/// A tool link that points anywhere but a gone version of this
/// configuration (a build elsewhere, a version that is still there) stays.
#[test]
fn a_tool_link_to_anything_else_is_kept() {
    let f = Fixture::new();
    let other = f.places.worker_state.join("tool-bin").join("dev");
    fs::create_dir_all(&other).unwrap();
    symlink("/somewhere/target/debug/belt", other.join("portool")).unwrap();
    let new = f.places.worker_state.join("tool-bin").join("new");
    fs::create_dir_all(&new).unwrap();
    symlink(f.version(NEW).join("belt"), new.join("portool")).unwrap();
    f.assert_removed();
    assert!(other.exists());
    assert!(new.exists());
}
