use super::*;
use std::os::unix::fs::PermissionsExt;

const INSTANCE: &str = "01J0000000000000000000000";

struct Fx {
    root: tempfile::TempDir,
}

impl Fx {
    fn new() -> Self {
        let root = crate::test_tmp::prefixed("belt-eph-unit-").unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        Fx { root }
    }
    fn state(&self) -> PathBuf {
        self.root.path().join(".belt")
    }
    /// A state the way a killed ephemeral worker leaves it: marker, locks,
    /// a database and a workspace tree. The lock files are never opened
    /// here, so no descriptor of this test process can hold them.
    fn left_behind(&self) -> PathBuf {
        let state = self.state();
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&state)
            .unwrap();
        private(&state.join(MARKER), &serde_json::to_vec(&marker()).unwrap());
        private(&state.join(LOCK), b"");
        private(&state.join(AGENT_LOCK), b"");
        private(&state.join("worker.db"), b"rows");
        std::fs::create_dir_all(state.join("attempts/a1/workspace")).unwrap();
        std::fs::write(state.join("attempts/a1/workspace/file"), "x").unwrap();
        state
    }
}

fn private(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn marker() -> MarkerFile {
    MarkerFile {
        schema: 1,
        kind: plan::MARKER_KIND.into(),
        boot: Some("boot-1".into()),
        pid: Some(4242),
        start: Some("start-1".into()),
        instance: Some(INSTANCE.into()),
    }
}

fn local(_: &File) -> plan::Filesystem {
    plan::Filesystem::Name("apfs".into())
}

/// Every entry below `dir` with its size, so "nothing changed" also means
/// "nothing was created".
fn tree(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_owned()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            found.push((entry.path(), if meta.is_dir() { 0 } else { meta.len() }));
            if meta.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    found.sort();
    found
}

fn refused(state: &Path, resident: bool, filesystem: fn(&File) -> plan::Filesystem) -> String {
    let before = tree(state);
    let error = reclaim(state, resident, filesystem)
        .unwrap_err()
        .to_string();
    assert_eq!(tree(state), before, "a refusal changed the state: {error}");
    assert!(error.contains("is kept"), "{error}");
    error
}

#[test]
fn a_state_nobody_uses_is_cleared_whole() {
    let fx = Fx::new();
    let state = fx.left_behind();
    reclaim(&state, false, local).unwrap();
    assert!(std::fs::symlink_metadata(&state).is_err());
    assert_eq!(tree(fx.root.path()), []);
}

#[test]
fn the_real_filesystem_of_a_test_directory_is_a_local_one() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let dir = File::open(&state).unwrap();
    let kind = filesystem_of(&dir);
    assert!(plan::local_filesystem(&kind), "{kind:?}");
    reclaim(&state, false, filesystem_of).unwrap();
    assert!(!state.exists());
}

#[test]
fn a_start_killed_before_its_locks_existed_is_cleared_too() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(LOCK)).unwrap();
    std::fs::remove_file(state.join(AGENT_LOCK)).unwrap();
    reclaim(&state, false, local).unwrap();
    assert!(!state.exists());
}

#[test]
fn a_state_without_the_marker_or_with_another_kind_is_kept() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(MARKER)).unwrap();
    // No lock file is created in a state that is not this role's.
    std::fs::remove_file(state.join(LOCK)).unwrap();
    let error = refused(&state, false, local);
    assert!(error.contains("no ephemeral.json"), "{error}");

    let mut other = marker();
    other.kind = "resident_worker".into();
    private(&state.join(MARKER), &serde_json::to_vec(&other).unwrap());
    let error = refused(&state, false, local);
    assert!(
        error.contains("not of the ephemeral worker kind"),
        "{error}"
    );

    for bytes in [&b"{"[..], b"", b"[]", br#"{"schema":1}"#] {
        private(&state.join(MARKER), bytes);
        let error = refused(&state, false, local);
        assert!(error.contains("cannot be read"), "{error}");
    }
    // A marker others could have written, and one that is a link.
    private(&state.join(MARKER), &serde_json::to_vec(&marker()).unwrap());
    std::fs::set_permissions(state.join(MARKER), std::fs::Permissions::from_mode(0o644)).unwrap();
    let error = refused(&state, false, local);
    assert!(error.contains("cannot be read"), "{error}");
    std::fs::remove_file(state.join(MARKER)).unwrap();
    private(
        &fx.root.path().join("elsewhere"),
        &serde_json::to_vec(&marker()).unwrap(),
    );
    std::os::unix::fs::symlink(fx.root.path().join("elsewhere"), state.join(MARKER)).unwrap();
    let error = refused(&state, false, local);
    assert!(error.contains("cannot be read"), "{error}");
}

#[test]
fn a_state_on_a_filesystem_that_is_not_listed_is_kept() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(LOCK)).unwrap();
    for filesystem in [
        (|_| plan::Filesystem::Name("nfs".into())) as fn(&File) -> plan::Filesystem,
        |_| plan::Filesystem::Magic(0x6573_5546),
        |_| plan::Filesystem::Unknown,
    ] {
        let error = refused(&state, false, filesystem);
        assert!(error.contains("not on a local filesystem"), "{error}");
    }
}

#[test]
fn a_state_beside_a_resident_worker_is_kept() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let error = refused(&state, true, local);
    assert!(error.contains("persistent worker is installed"), "{error}");
}

#[test]
fn a_state_whose_ephemeral_lock_is_held_is_kept_and_its_agent_lock_untouched() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(AGENT_LOCK)).unwrap();
    // What a supervisor or a command that inherited the descriptor holds.
    let inherited = File::open(state.join(LOCK)).unwrap();
    inherited.try_lock_shared().unwrap();
    let error = refused(&state, false, local);
    assert!(error.contains("ephemeral.lock is held"), "{error}");
    assert!(error.contains("pid 4242"), "{error}");
    assert!(error.contains("boot boot-1"), "{error}");
    assert!(error.contains(INSTANCE), "{error}");
}

#[test]
fn a_state_whose_agent_lock_is_held_is_kept() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let agent = File::open(state.join(AGENT_LOCK)).unwrap();
    agent.try_lock().unwrap();
    let error = refused(&state, false, local);
    assert!(error.contains("agent.lock is held"), "{error}");
}

#[test]
fn a_state_that_is_a_link_or_open_to_others_is_kept() {
    let fx = Fx::new();
    let real = fx.left_behind();
    let elsewhere = fx.root.path().join("real");
    std::fs::rename(&real, &elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, fx.state()).unwrap();
    let before = tree(&elsewhere);
    let error = reclaim(&fx.state(), false, local).unwrap_err().to_string();
    assert!(error.contains("not a private directory"), "{error}");
    assert_eq!(tree(&elsewhere), before);
    assert!(fx.state().is_symlink());

    std::fs::remove_file(fx.state()).unwrap();
    std::fs::rename(&elsewhere, &real).unwrap();
    std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = refused(&real, false, local);
    assert!(error.contains("not a private directory"), "{error}");
}

#[test]
fn a_clearing_that_fails_midway_leaves_the_marker_for_the_next_start() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let stuck = state.join("attempts/a1");
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o500)).unwrap();
    if std::fs::write(stuck.join("probe"), "").is_ok() {
        // Running as a user that file modes do not stop: nothing to show.
        std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }
    assert!(reclaim(&state, false, local).is_err());
    assert_eq!(read_marker(&state).0, plan::Marker::Ephemeral);
    assert!(state.join(LOCK).exists());
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
    reclaim(&state, false, local).unwrap();
    assert!(!state.exists());
}

#[test]
fn a_new_state_starts_with_the_marker_and_holds_an_inheritable_shared_lock() {
    let fx = Fx::new();
    let state = fx.state();
    let (created, alive) = create_state(&state, &marker()).unwrap();
    assert_eq!(created.path, state);
    let (kind, written) = read_marker(&state);
    assert_eq!(kind, plan::Marker::Ephemeral);
    let written = written.unwrap();
    assert_eq!(written.schema, 1);
    assert_eq!(written.kind, "ephemeral_worker");
    assert_eq!(written.boot.as_deref(), Some("boot-1"));
    assert_eq!(written.pid, Some(4242));
    assert_eq!(written.start.as_deref(), Some("start-1"));
    assert_eq!(written.instance.as_deref(), Some(INSTANCE));
    assert_eq!(
        tree(&state)
            .into_iter()
            .map(|(path, _)| path.file_name().unwrap().to_owned())
            .collect::<Vec<_>>(),
        [MARKER, LOCK]
    );
    // Not close-on-exec: every process started from here keeps the lock held.
    // SAFETY: a query on a descriptor this test owns.
    let flags = unsafe { libc::fcntl(alive.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(flags & libc::FD_CLOEXEC, 0);
    // Shared: a second holder is fine, an exclusive taker is not.
    let second = File::open(state.join(LOCK)).unwrap();
    second.try_lock_shared().unwrap();
    drop(second);
    let error = refused(&state, false, local);
    assert!(error.contains("ephemeral.lock is held"), "{error}");
    // The directory is never adopted: a second creation fails.
    assert!(create_state(&state, &marker()).is_err());
    assert_eq!(read_marker(&state).0, plan::Marker::Ephemeral);
    drop(alive);
}

#[test]
fn a_new_start_never_writes_into_a_directory_that_already_exists() {
    let fx = Fx::new();
    let state = fx.state();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&state)
        .unwrap();
    private(&state.join(LOCK), b"");
    let clearing = File::open(state.join(LOCK)).unwrap();
    clearing.try_lock().unwrap();
    // The directory exists, so creation fails before anything is written.
    assert!(create_state(&state, &marker()).is_err());
    assert!(!state.join(MARKER).exists());
}

#[test]
fn only_a_state_with_the_marker_is_marked() {
    let fx = Fx::new();
    assert!(!marked(&fx.state()));
    let state = fx.left_behind();
    assert!(marked(&state));
    std::fs::remove_file(state.join(MARKER)).unwrap();
    assert!(!marked(&state));
    // A marker that cannot be read still marks the state.
    std::os::unix::fs::symlink("nowhere", state.join(MARKER)).unwrap();
    assert!(marked(&state));
}
