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
        filesystem: Some(plan::recorded(&plan::Filesystem::Name("apfs".into()))),
    }
}

fn marker_kind(state: &Path) -> plan::Marker {
    read_marker(&File::open(state).unwrap()).kind
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

/// A clearing on a host the test answers for.
fn reclaim(
    state: &Path,
    resident: bool,
    filesystem: fn(&File) -> plan::Filesystem,
    at: &dyn Fn(Step),
) -> anyhow::Result<Cleared> {
    clear(
        state,
        &Host {
            resident: &|| Ok(resident),
            filesystem_of: &filesystem,
            device_of: &same_device,
            at,
        },
    )
}

fn refused(state: &Path, resident: bool, filesystem: fn(&File) -> plan::Filesystem) -> String {
    let before = tree(state);
    let error = reclaim(state, resident, filesystem, &nowhere)
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
    reclaim(&state, false, local, &nowhere).unwrap();
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
    reclaim(&state, false, filesystem_of, &nowhere).unwrap();
    assert!(!state.exists());
}

#[test]
fn a_start_killed_before_its_locks_existed_is_cleared_too() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(LOCK)).unwrap();
    std::fs::remove_file(state.join(AGENT_LOCK)).unwrap();
    reclaim(&state, false, local, &nowhere).unwrap();
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
    let error = reclaim(&fx.state(), false, local, &nowhere)
        .unwrap_err()
        .to_string();
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
    assert!(reclaim(&state, false, local, &nowhere).is_err());
    assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
    assert!(state.join(LOCK).exists());
    std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
    reclaim(&state, false, local, &nowhere).unwrap();
    assert!(!state.exists());
}

#[test]
fn a_new_state_starts_with_the_marker_and_holds_an_inheritable_shared_lock() {
    let fx = Fx::new();
    let state = fx.state();
    let (created, alive) = create_state(&state, &marker(), local, &nowhere).unwrap();
    assert_eq!(created.path, state);
    let read = read_marker(&File::open(&state).unwrap());
    assert_eq!(read.kind, plan::Marker::Ephemeral);
    let written = read.file.unwrap();
    assert_eq!(written.schema, 1);
    assert_eq!(written.kind, "ephemeral_worker");
    assert_eq!(written.boot.as_deref(), Some("boot-1"));
    assert_eq!(written.pid, Some(4242));
    assert_eq!(written.start.as_deref(), Some("start-1"));
    assert_eq!(written.instance.as_deref(), Some(INSTANCE));
    // What this process saw of the directory it made.
    assert_eq!(
        written.filesystem,
        Some(plan::Recorded {
            local: true,
            kind: "apfs".into()
        })
    );
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
    assert!(create_state(&state, &marker(), local, &nowhere).is_err());
    assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
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
    assert!(create_state(&state, &marker(), local, &nowhere).is_err());
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

/// Spec 25.4 (d), proof 2: the worker that lives in a state and the start
/// that clears it can see the same directory through different filesystems
/// (a container's bind mount of a directory of its host). What the creator
/// saw is in the marker, and it must be local too.
#[test]
fn a_state_its_creator_saw_on_a_shared_filesystem_is_kept_even_where_it_is_local_now() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(LOCK)).unwrap();
    for seen in [
        plan::Filesystem::Name("virtiofs".into()),
        plan::Filesystem::Magic(0x6573_5546),
        plan::Filesystem::Unknown,
    ] {
        let mut shared = marker();
        shared.filesystem = Some(plan::recorded(&seen));
        private(&state.join(MARKER), &serde_json::to_vec(&shared).unwrap());
        let error = refused(&state, false, local);
        assert!(
            error.contains("the worker that created it did not see it on a local filesystem"),
            "{error}"
        );
        // The real verdict of this directory, local as well, changes nothing.
        refused(&state, false, filesystem_of);
    }
    // A marker that records nothing proves nothing.
    let mut silent = marker();
    silent.filesystem = None;
    private(&state.join(MARKER), &serde_json::to_vec(&silent).unwrap());
    let error = refused(&state, false, local);
    assert!(error.contains("does not record the filesystem"), "{error}");
    private(
        &state.join(MARKER),
        br#"{"schema":1,"kind":"ephemeral_worker"}"#,
    );
    let error = refused(&state, false, local);
    assert!(error.contains("does not record the filesystem"), "{error}");
}

/// Creating a state on a filesystem that is not listed stays allowed; the
/// marker says what was seen, and that alone keeps every later start from
/// clearing it.
#[test]
fn a_state_created_on_a_shared_filesystem_records_it_and_is_never_cleared() {
    let fx = Fx::new();
    let state = fx.state();
    let shared = |_: &File| plan::Filesystem::Name("virtiofs".into());
    let (_, alive) = create_state(&state, &marker(), shared, &nowhere).unwrap();
    let written = read_marker(&File::open(&state).unwrap()).file.unwrap();
    assert_eq!(
        written.filesystem,
        Some(plan::Recorded {
            local: false,
            kind: "virtiofs".into()
        })
    );
    // Its owner is gone and both locks are free: still kept.
    drop(alive);
    let error = refused(&state, false, local);
    assert!(
        error.contains("did not see it on a local filesystem (it saw virtiofs"),
        "{error}"
    );
}

/// The marker exists only once its writer holds `ephemeral.lock`: at every
/// moment of a creation there is no marker, and from the moment the lock is
/// held a start that clears finds it held and changes nothing. (What a
/// clearing does before that moment is in the tests of the creation below.)
#[test]
fn a_clearing_start_never_meets_a_marked_state_whose_owner_does_not_hold_the_lock() {
    let fx = Fx::new();
    let state = fx.state();
    let seen = std::cell::RefCell::new(Vec::new());
    let (_, alive) = create_state(&state, &marker(), local, &|step| {
        seen.borrow_mut().push(step);
        assert_eq!(marker_kind(&state), plan::Marker::Absent, "{step:?}");
        assert_eq!(
            state.join(LOCK).exists(),
            step != Step::Directory,
            "{step:?}: the lock comes before the marker"
        );
        if step == Step::Locked {
            let error = refused(&state, false, local);
            assert!(error.contains("ephemeral.lock is held"), "{error}");
        }
    })
    .unwrap();
    assert_eq!(*seen.borrow(), [Step::Directory, Step::Made, Step::Locked]);
    // Complete: the marker is there, and so is the hold on the lock.
    assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
    assert!(!state.join(MARKER_PART).exists());
    let error = refused(&state, false, local);
    assert!(error.contains("ephemeral.lock is held"), "{error}");
    drop(alive);
    assert_eq!(
        reclaim(&state, false, local, &nowhere).unwrap(),
        Cleared::Left
    );
    assert!(!state.exists());
}

/// "There is a marker" counts only for the marker read again under the
/// exclusive lock: the file that was examined, with the bytes it had.
#[test]
fn a_marker_that_changes_while_the_state_is_examined_is_not_a_proof() {
    for same_bytes in [true, false] {
        let fx = Fx::new();
        let state = fx.left_behind();
        let mut other = marker();
        if !same_bytes {
            other.instance = Some("another-start".into());
        }
        let error = reclaim(&state, false, local, &|step| {
            if step == Step::Examined {
                // Another file under the same name, as a start that came in
                // between would have written it.
                // (Made while the examined one still exists, so it is
                // certainly not the same inode.)
                private(&state.join("new"), &serde_json::to_vec(&other).unwrap());
                std::fs::rename(state.join("new"), state.join(MARKER)).unwrap();
            }
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("is kept"), "{error}");
        assert!(
            error.contains("ephemeral.json changed while it was examined"),
            "{error}"
        );
        assert!(state.join("worker.db").exists());
        assert!(state.join("attempts/a1/workspace/file").exists());
        assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
    }
    // And one that is gone by then.
    let fx = Fx::new();
    let state = fx.left_behind();
    let error = reclaim(&state, false, local, &|step| {
        if step == Step::Examined {
            std::fs::remove_file(state.join(MARKER)).unwrap();
        }
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("changed while it was examined"), "{error}");
    assert!(state.join("worker.db").exists());
}

/// One start clears at a time: the directory itself is locked for it.
#[test]
fn a_state_another_start_is_clearing_is_left_to_it() {
    let fx = Fx::new();
    let state = fx.left_behind();
    std::fs::remove_file(state.join(LOCK)).unwrap();
    let clearing = File::open(&state).unwrap();
    clearing.try_lock().unwrap();
    // Nothing is created either: no lock file of a second clearing.
    let error = refused(&state, false, local);
    assert!(
        error.contains("another ephemeral start is clearing it"),
        "{error}"
    );
    drop(clearing);
    reclaim(&state, false, local, &nowhere).unwrap();
    assert!(!state.exists());
}

/// What is removed is reached from the descriptor of the directory that was
/// examined. A path that is something else by then costs that something
/// nothing.
#[test]
fn a_state_swapped_for_a_link_after_the_proof_costs_nothing_outside_it() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let victim = fx.root.path().join("victim");
    std::fs::create_dir_all(victim.join("attempts/keep")).unwrap();
    std::fs::write(victim.join("attempts/keep/file"), "precious").unwrap();
    std::fs::write(victim.join("worker.db"), "precious").unwrap();
    std::fs::write(victim.join(MARKER), "precious").unwrap();
    let before = tree(&victim);
    let aside = fx.root.path().join("aside");
    let error = reclaim(&state, false, local, &|step| {
        if step == Step::Proven {
            std::fs::rename(&state, &aside).unwrap();
            std::os::unix::fs::symlink(&victim, &state).unwrap();
        }
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("replaced while it was cleared"), "{error}");
    assert_eq!(tree(&victim), before);
    assert!(state.is_symlink());
    // The directory that was proven unused is the one that was emptied; its
    // name was no longer the state's, so it stays.
    assert_eq!(tree(&aside), []);
}

/// A link inside the state is removed as a link, whatever it points at.
#[test]
fn links_inside_a_state_are_removed_and_never_followed() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let victim = fx.root.path().join("victim");
    std::fs::create_dir_all(victim.join("deep")).unwrap();
    std::fs::write(victim.join("deep/file"), "precious").unwrap();
    let before = tree(&victim);
    std::os::unix::fs::symlink(&victim, state.join("attempts/a1/out")).unwrap();
    std::os::unix::fs::symlink(victim.join("deep/file"), state.join("linked-file")).unwrap();
    reclaim(&state, false, local, &nowhere).unwrap();
    assert!(!state.exists());
    assert_eq!(tree(&victim), before);
}

/// A creation that fails takes back the names it made, and only those: a
/// name it did not make is not its to remove.
#[test]
fn a_creation_that_fails_takes_back_what_it_made_and_only_that() {
    // Everything in the directory is its own: all of it goes, the marker
    // included. (The directory was moved away under it, which is what makes
    // the creation fail after the marker; its name is then not removed.)
    let fx = Fx::new();
    let state = fx.state();
    let aside = fx.root.path().join("aside");
    let result = create_state(&state, &marker(), local, &|step| {
        if step == Step::Locked {
            std::fs::rename(&state, &aside).unwrap();
        }
    });
    assert!(result.is_err());
    assert_eq!(tree(fx.root.path()), [(aside.clone(), 0)]);
    std::fs::remove_dir(&aside).unwrap();
    // The marker cannot be written because its place is taken by a file
    // this creation did not make: that file stays, alone.
    let result = create_state(&state, &marker(), local, &|step| {
        if step == Step::Locked {
            private(&state.join(MARKER_PART), b"in the way");
        }
    });
    assert!(result.is_err());
    assert_eq!(tree(&state), [(state.join(MARKER_PART), 10)]);
    // Which is a leftover the next start removes.
    assert_eq!(
        reclaim(&state, false, local, &nowhere).unwrap(),
        Cleared::Unfinished
    );
    assert_eq!(tree(fx.root.path()), []);
}

/// Spec 25.4 (d), proof 3: the worker service's journal or the `init`
/// resident's, and only "not found" means that neither is there.
#[test]
fn only_journals_that_are_clearly_absent_mean_that_no_persistent_worker_is_installed() {
    let fx = Fx::new();
    let config = fx.root.path().join("config");
    std::fs::create_dir(&config).unwrap();
    let journals = [config.join("worker-service.json"), config.join("init.json")];
    assert!(!resident_installed(&journals).unwrap());
    for journal in &journals {
        std::fs::write(journal, "{}").unwrap();
        assert!(resident_installed(&journals).unwrap(), "{journal:?}");
        std::fs::remove_file(journal).unwrap();
        // Whatever sits at the name counts, readable or not.
        std::os::unix::fs::symlink("nowhere", journal).unwrap();
        assert!(resident_installed(&journals).unwrap(), "{journal:?}");
        std::fs::remove_file(journal).unwrap();
    }
    // ENOTDIR: the configuration directory is not a directory.
    let file = fx.root.path().join("file");
    std::fs::write(&file, "").unwrap();
    for at in [0, 1] {
        let mut odd = journals.clone();
        odd[at] = file.join("journal.json");
        let error = resident_installed(&odd).unwrap_err().to_string();
        assert!(error.contains("cannot be examined"), "{error}");
        assert!(error.contains("journal.json"), "{error}");
        assert!(
            error.contains("cannot tell whether a persistent worker is installed"),
            "{error}"
        );
    }
    // EACCES: the directory cannot be searched.
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o000)).unwrap();
    let denied = std::fs::symlink_metadata(&journals[0])
        .is_err_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
    let result = resident_installed(&journals);
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o700)).unwrap();
    // A user that file modes do not stop sees a plain "not found" instead.
    if denied {
        let error = result.unwrap_err().to_string();
        assert!(error.contains("cannot be examined"), "{error}");
        assert!(error.contains("worker-service.json"), "{error}");
    }
}

#[test]
fn the_refusal_of_a_marked_state_names_it_and_the_way_out() {
    let fx = Fx::new();
    refuse_marked(&fx.state()).unwrap();
    let state = fx.left_behind();
    let error = refuse_marked(&state).unwrap_err().to_string();
    assert!(error.contains("belongs to an ephemeral worker"), "{error}");
    assert!(error.contains("belt worker --ephemeral"), "{error}");
    assert!(error.contains(&state.display().to_string()), "{error}");
}

/// The locks prove something about the directory that was examined, not
/// about whatever its path names by the time they are taken.
#[test]
fn a_state_replaced_while_it_is_examined_is_kept_and_so_is_its_replacement() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let aside = fx.root.path().join("aside");
    let before = tree(&state);
    let error = reclaim(&state, false, local, &|step| {
        if step == Step::Examined {
            // Another start cleared the path and made its own state there.
            std::fs::rename(&state, &aside).unwrap();
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&state)
                .unwrap();
            private(&state.join("worker.db"), b"new");
        }
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("replaced while it was examined"), "{error}");
    assert!(error.contains("is kept"), "{error}");
    let moved: Vec<_> = before
        .iter()
        .map(|(path, size)| (aside.join(path.strip_prefix(&state).unwrap()), *size))
        .collect();
    assert_eq!(tree(&aside), moved);
    assert_eq!(std::fs::read(state.join("worker.db")).unwrap(), b"new");
}

/// The lock that is held must be the file `ephemeral.lock` names now, not
/// only a file that once had that name (the second half of the check after
/// the locks; the first half is the test above).
#[test]
fn a_hold_on_a_lock_file_that_is_no_longer_the_one_in_the_state_is_not_a_proof() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let error = reclaim(&state, false, local, &|step| {
        if step == Step::Held {
            // Another file under the name, made while the held one exists.
            private(&state.join("new"), b"");
            std::fs::rename(state.join("new"), state.join(LOCK)).unwrap();
        }
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("replaced while it was examined"), "{error}");
    assert!(error.contains("is kept"), "{error}");
    assert!(state.join("attempts/a1/workspace/file").exists());
    assert!(state.join("worker.db").exists());
    assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
}

/// Spec 25.4 (d), proof 5 through inheritance: a process the worker started
/// holds `ephemeral.lock` by the descriptor it inherited, after the worker
/// itself is gone.
#[test]
fn a_child_that_inherited_the_lock_keeps_the_state_from_being_cleared() {
    let fx = Fx::new();
    let state = fx.state();
    let (_, alive) = create_state(&state, &marker(), local, &nowhere).unwrap();
    let mut child = std::process::Command::new("/bin/sleep")
        .arg("600")
        .stdin(std::process::Stdio::null())
        .spawn()
        .unwrap();
    // The worker is gone; only the child holds the lock now.
    drop(alive);
    // Nothing is asserted before the child is gone again.
    let before = tree(&state);
    let outcome = reclaim(&state, false, local, &nowhere);
    let after = tree(&state);
    let pid = child.id();
    // Only the child this test started.
    let killed = pid > 1 && crate::platform::signal_pid(pid, libc::SIGKILL);
    child.wait().unwrap();
    assert!(killed);
    let error = outcome.unwrap_err().to_string();
    assert!(error.contains("ephemeral.lock is held"), "{error}");
    assert_eq!(after, before, "a refusal changed the state");
    assert_eq!(
        reclaim(&state, false, local, &nowhere).unwrap(),
        Cleared::Left
    );
    assert!(!state.exists());
}

/// An empty private directory at the state's place.
fn bare(fx: &Fx) -> PathBuf {
    let state = fx.state();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&state)
        .unwrap();
    state
}

/// The names directly in a directory.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// Spec 25.4 (d): what a start killed between `mkdir` and the marker leaves
/// (and a failed creation killed while taking its files back, and a
/// clearing killed before its `rmdir`) is removed by the next start.
#[test]
fn what_a_start_killed_before_its_marker_left_is_removed() {
    for shape in [&[][..], &[LOCK], &[LOCK, MARKER_PART], &[MARKER_PART]] {
        let fx = Fx::new();
        let state = bare(&fx);
        for name in shape {
            private(&state.join(name), b"{");
        }
        assert_eq!(
            reclaim(&state, false, local, &nowhere).unwrap(),
            Cleared::Unfinished,
            "{shape:?}"
        );
        assert_eq!(tree(fx.root.path()), [], "{shape:?}");
    }
}

/// Without every condition nothing is written, locked or removed: not even
/// a lock file is made in a directory that may be somebody else's.
#[test]
fn an_unmarked_state_is_kept_unless_it_is_provably_an_unfinished_creation() {
    let fx = Fx::new();
    let state = bare(&fx);
    // A persistent worker is installed: the directory may be its own.
    let error = refused(&state, true, local);
    assert!(error.contains("persistent worker is installed"), "{error}");
    // Decided from what can be read: the directory was not even locked.
    let locked = std::cell::Cell::new(false);
    assert!(reclaim(&state, true, local, &|_| locked.set(true)).is_err());
    assert!(!locked.get());
    // Not a local filesystem.
    let error = refused(&state, false, |_| plan::Filesystem::Name("nfs".into()));
    assert!(error.contains("not on a local filesystem"), "{error}");
    // Another start is at it.
    let clearing = File::open(&state).unwrap();
    clearing.try_lock().unwrap();
    let error = refused(&state, false, local);
    assert!(
        error.contains("another ephemeral start is clearing it"),
        "{error}"
    );
    drop(clearing);
    // A start that is creating it holds the lock.
    private(&state.join(LOCK), b"");
    let creating = File::open(state.join(LOCK)).unwrap();
    creating.try_lock_shared().unwrap();
    let error = refused(&state, false, local);
    assert!(error.contains("ephemeral.lock is held"), "{error}");
    assert!(error.contains("creating it right now"), "{error}");
    drop(creating);
    // A lock file that others can touch proves nothing.
    std::fs::set_permissions(state.join(LOCK), std::fs::Permissions::from_mode(0o644)).unwrap();
    let error = refused(&state, false, local);
    assert!(
        error.contains("ephemeral.lock cannot be examined"),
        "{error}"
    );
    std::fs::set_permissions(state.join(LOCK), std::fs::Permissions::from_mode(0o600)).unwrap();
    // Any other name: not what a creation leaves.
    private(&state.join(AGENT_LOCK), b"");
    let error = refused(&state, false, local);
    assert!(error.contains("no ephemeral.json"), "{error}");
    std::fs::remove_file(state.join(AGENT_LOCK)).unwrap();
    assert_eq!(
        reclaim(&state, false, local, &nowhere).unwrap(),
        Cleared::Unfinished
    );
    assert!(!state.exists());
}

/// What is true before the lock is taken is read again under it. A file
/// that appeared, a marker, or a persistent worker's journal keeps the
/// directory; the refusal takes back the lock file it made and nothing else.
#[test]
fn an_unfinished_creation_is_judged_again_under_the_lock() {
    for lock_was_there in [false, true] {
        let fx = Fx::new();
        let state = bare(&fx);
        if lock_was_there {
            private(&state.join(LOCK), b"");
        }
        let error = reclaim(&state, false, local, &|step| {
            if step == Step::Held {
                // A worker took its first lock in the directory.
                private(&state.join(AGENT_LOCK), b"");
            }
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("changed while it was examined"), "{error}");
        assert!(error.contains("is kept"), "{error}");
        let left: &[&str] = if lock_was_there {
            &[AGENT_LOCK, LOCK]
        } else {
            &[AGENT_LOCK]
        };
        assert_eq!(names(&state), left);
    }
    // A journal that appeared since the first look.
    let fx = Fx::new();
    let state = bare(&fx);
    let installed = std::cell::Cell::new(false);
    let error = clear(
        &state,
        &Host {
            resident: &|| Ok(installed.get()),
            filesystem_of: &local,
            device_of: &same_device,
            at: &|step| {
                if step == Step::Held {
                    installed.set(true);
                }
            },
        },
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("changed while it was examined"), "{error}");
    assert!(error.contains("persistent worker is installed"), "{error}");
    assert_eq!(names(&state), [] as [&str; 0]);
    // The lock file under the name is no longer the one that is held: it
    // is somebody else's, and stays.
    let error = reclaim(&state, false, local, &|step| {
        if step == Step::Held {
            private(&state.join("new"), b"other");
            std::fs::rename(state.join("new"), state.join(LOCK)).unwrap();
        }
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("replaced while it was examined"), "{error}");
    assert_eq!(std::fs::read(state.join(LOCK)).unwrap(), b"other");
}

/// Only the two names are unlinked and the directory goes by a plain
/// `rmdir`: what appears after the proof stays, with the directory.
#[test]
fn what_appears_in_an_unfinished_creation_after_the_proof_is_never_removed() {
    let fx = Fx::new();
    let state = bare(&fx);
    private(&state.join(MARKER_PART), b"{");
    let error = reclaim(&state, false, local, &|step| {
        if step == Step::Proven {
            std::fs::create_dir(state.join("attempts")).unwrap();
            std::fs::write(state.join("attempts/file"), "new").unwrap();
        }
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("could not be removed"), "{error}");
    assert_eq!(names(&state), ["attempts"]);
    assert_eq!(std::fs::read(state.join("attempts/file")).unwrap(), b"new");
}

/// The creator's side of that race. A start may take the directory of a
/// creation for a leftover at any moment before the creation holds its
/// lock; the creation then steps back and touches nothing.
#[test]
fn a_creation_whose_directory_is_taken_for_a_leftover_steps_back() {
    // The directory is removed before the lock file is made.
    let fx = Fx::new();
    let state = fx.state();
    let result = create_state(&state, &marker(), local, &|step| {
        if step == Step::Directory {
            assert_eq!(
                reclaim(&state, false, local, &nowhere).unwrap(),
                Cleared::Unfinished
            );
        }
    });
    assert!(result.is_err());
    assert_eq!(tree(fx.root.path()), []);
    // The other start has made its lock file: `ephemeral.lock` is only
    // ever a new file of the creation, never one that was there.
    let result = create_state(&state, &marker(), local, &|step| {
        if step == Step::Directory {
            private(&state.join(LOCK), b"theirs");
        }
    });
    let error = result.err().unwrap().to_string();
    assert!(error.contains("start again"), "{error}");
    assert_eq!(tree(&state), [(state.join(LOCK), 6)]);
    std::fs::remove_dir_all(&state).unwrap();
    // The other start took the lock file the creation had just made: the
    // file is left to it.
    let theirs = std::cell::RefCell::new(None);
    let result = create_state(&state, &marker(), local, &|step| {
        if step == Step::Made {
            let lock = File::open(state.join(LOCK)).unwrap();
            lock.try_lock().unwrap();
            *theirs.borrow_mut() = Some(lock);
        }
    });
    let error = result.err().unwrap().to_string();
    assert!(error.contains("start again"), "{error}");
    assert_eq!(tree(&state), [(state.join(LOCK), 0)]);
    // And that start, or the next, removes it.
    drop(theirs.take());
    assert_eq!(
        reclaim(&state, false, local, &nowhere).unwrap(),
        Cleared::Unfinished
    );
    assert_eq!(tree(fx.root.path()), []);
}

/// A device answer that makes every entry called `mount` another filesystem.
fn mounted(name: &CStr, device: u64) -> u64 {
    if name == c"mount" {
        device.wrapping_add(1)
    } else {
        device
    }
}

fn clear_across(state: &Path, device_of: &dyn Fn(&CStr, u64) -> u64) -> anyhow::Result<Cleared> {
    clear(
        state,
        &Host {
            resident: &|| Ok(false),
            filesystem_of: &local,
            device_of,
            at: &nowhere,
        },
    )
}

/// A filesystem mounted inside the state (a FUSE or bind mount alive in a
/// workspace) is neither entered nor unlinked. The directories above it
/// stay, and so do the locks and the marker, so that the next start can
/// prove the state again once the mount is gone.
#[test]
fn a_filesystem_mounted_inside_the_state_is_never_entered_or_removed() {
    let fx = Fx::new();
    let state = fx.left_behind();
    let workspace = state.join("attempts/a1/workspace");
    std::fs::create_dir_all(workspace.join("mount/deep")).unwrap();
    std::fs::write(workspace.join("mount/deep/file"), "theirs").unwrap();
    // A file can be a mount point too.
    std::fs::create_dir(state.join("single")).unwrap();
    std::fs::write(state.join("single/mount"), "theirs").unwrap();
    let kept = [
        (state.join(AGENT_LOCK), 0),
        (state.join("attempts"), 0),
        (state.join("attempts/a1"), 0),
        (workspace.clone(), 0),
        (workspace.join("mount"), 0),
        (workspace.join("mount/deep"), 0),
        (workspace.join("mount/deep/file"), 6),
        (
            state.join(MARKER),
            std::fs::metadata(state.join(MARKER)).unwrap().len(),
        ),
        (state.join(LOCK), 0),
        (state.join("single"), 0),
        (state.join("single/mount"), 6),
    ];
    let error = clear_across(&state, &mounted).unwrap_err().to_string();
    assert!(
        error.contains("another filesystem is mounted in it"),
        "{error}"
    );
    assert!(error.contains("attempts/a1/workspace/mount"), "{error}");
    assert!(error.contains("single/mount"), "{error}");
    let mut kept = kept.to_vec();
    kept.sort();
    // The rest of the data went: worker.db and the workspace's own file.
    assert_eq!(tree(&state), kept);
    assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
    std::fs::remove_file(state.join("single/mount")).unwrap();
    // What a directory is on is asked of the descriptor that was opened,
    // too: a filesystem mounted between the look and the open is seen.
    let looks = std::cell::Cell::new(0);
    let error = clear_across(&state, &|name, device| {
        if name == c"mount" {
            looks.set(looks.get() + 1);
            // The look at the name says "same device"; the descriptor
            // opened next is on another.
            if looks.get() % 2 == 0 {
                return device.wrapping_add(1);
            }
        }
        device
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("attempts/a1/workspace/mount"), "{error}");
    assert!(workspace.join("mount/deep/file").exists());
    // Unmounted: the next start clears the rest.
    assert_eq!(clear_across(&state, &same_device).unwrap(), Cleared::Left);
    assert!(!state.exists());
}

/// A state as a worker that ran leaves it at its exit: created here, with
/// the two locks a worker takes and some data, and no database.
fn ran(fx: &Fx) -> (Created, File) {
    let (created, alive) = create_state(&fx.state(), &marker(), local, &nowhere).unwrap();
    private(&created.path.join(OWNER_LOCK), b"");
    private(&created.path.join(AGENT_LOCK), b"");
    std::fs::create_dir_all(created.path.join("attempts/a1/workspace")).unwrap();
    std::fs::write(created.path.join("attempts/a1/workspace/file"), "x").unwrap();
    (created, alive)
}

fn exit_host<'a>(resident: &'a dyn Fn() -> anyhow::Result<bool>, at: &'a dyn Fn(Step)) -> Host<'a> {
    Host {
        resident,
        filesystem_of: &local,
        device_of: &same_device,
        at,
    }
}

/// The exit of a worker removes its state as a clearing does: under the
/// directory lock, the data first, the marker last.
#[test]
fn the_exit_removes_the_state_under_the_directory_lock_with_the_marker_last() {
    let fx = Fx::new();
    let (created, alive) = ran(&fx);
    let state = created.path.clone();
    let seen = std::cell::RefCell::new(Vec::new());
    let erased = clean_up(
        &created,
        &alive,
        &exit_host(&|| Ok(false), &|step| {
            if step == Step::Proven {
                // No other start can examine it meanwhile.
                let other = File::open(&state).unwrap();
                assert!(matches!(other.try_lock(), Err(TryLockError::WouldBlock)));
                let error = refused(&state, false, local);
                assert!(
                    error.contains("another ephemeral start is clearing it"),
                    "{error}"
                );
            }
            if let Step::Last(name) = step {
                assert_eq!(marker_kind(&state), plan::Marker::Ephemeral, "{name}");
                assert!(!state.join("attempts").exists(), "{name}: data goes first");
                seen.borrow_mut().push((name, names(&state)));
            }
        }),
    )
    .unwrap();
    assert!(erased);
    let all = [AGENT_LOCK, MARKER, LOCK, OWNER_LOCK];
    assert_eq!(
        *seen.borrow(),
        [
            (OWNER_LOCK, all.map(String::from).to_vec()),
            (AGENT_LOCK, all[..3].iter().map(|n| n.to_string()).collect()),
            (LOCK, all[1..3].iter().map(|n| n.to_string()).collect()),
            (MARKER, vec![MARKER.to_string()]),
        ]
    );
    assert_eq!(tree(fx.root.path()), []);
}

/// A persistent worker installed while this one ran may have put its own
/// files into the directory: its journal keeps everything.
#[test]
fn the_exit_keeps_everything_when_a_persistent_worker_appeared_or_the_state_is_in_use() {
    let fx = Fx::new();
    let (created, alive) = ran(&fx);
    let state = created.path.clone();
    let before = tree(&state);
    let error = clean_up(&created, &alive, &exit_host(&|| Ok(true), &nowhere))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("a persistent worker was installed"),
        "{error}"
    );
    assert_eq!(tree(&state), before);
    // A journal that cannot be looked at may be one.
    let error = clean_up(
        &created,
        &alive,
        &exit_host(&|| Err(anyhow::anyhow!("cannot tell")), &nowhere),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("cannot tell"), "{error}");
    assert_eq!(tree(&state), before);
    // A worker holds one of its locks.
    for name in [OWNER_LOCK, AGENT_LOCK] {
        let worker = File::open(state.join(name)).unwrap();
        worker.try_lock().unwrap();
        let error = clean_up(&created, &alive, &exit_host(&|| Ok(false), &nowhere))
            .unwrap_err()
            .to_string();
        assert!(error.contains(&format!("{name} is held")), "{error}");
        assert_eq!(tree(&state), before);
    }
    // Another start is examining it.
    let other = File::open(&state).unwrap();
    other.try_lock().unwrap();
    let error = clean_up(&created, &alive, &exit_host(&|| Ok(false), &nowhere))
        .unwrap_err()
        .to_string();
    assert!(error.contains("examining the worker state"), "{error}");
    assert_eq!(tree(&state), before);
    drop(other);
    // The lock file under the name is not the one this worker holds.
    let error = clean_up(
        &created,
        &alive,
        &exit_host(&|| Ok(false), &|step| {
            if step == Step::Held {
                private(&state.join("new"), b"");
                std::fs::rename(state.join("new"), state.join(LOCK)).unwrap();
            }
        }),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("was replaced"), "{error}");
    assert_eq!(tree(&state), before);
}

/// The exit stays on the state's filesystem too, and keeps the marker
/// with what it could not remove.
#[test]
fn the_exit_does_not_enter_a_filesystem_mounted_inside_the_state() {
    let fx = Fx::new();
    let (created, alive) = ran(&fx);
    let state = created.path.clone();
    std::fs::create_dir(state.join("attempts/a1/workspace/mount")).unwrap();
    std::fs::write(state.join("attempts/a1/workspace/mount/file"), "theirs").unwrap();
    let host = Host {
        resident: &|| Ok(false),
        filesystem_of: &local,
        device_of: &mounted,
        at: &nowhere,
    };
    let error = clean_up(&created, &alive, &host).unwrap_err().to_string();
    assert!(error.contains("attempts/a1/workspace/mount"), "{error}");
    assert!(state.join("attempts/a1/workspace/mount/file").exists());
    assert!(!state.join("attempts/a1/workspace/file").exists());
    assert_eq!(marker_kind(&state), plan::Marker::Ephemeral);
    drop(alive);
    assert_eq!(clear_across(&state, &same_device).unwrap(), Cleared::Left);
    assert!(!state.exists());
}
