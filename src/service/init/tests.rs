use super::*;
use macos::{policy::Plan, LaunchError, Receipt};
use std::os::unix::fs::PermissionsExt;
struct Unknown;
impl Launcher for Unknown {
    fn launchctl(&mut self, _: &[String]) -> Result<(), LaunchError> {
        panic!("unknown service must not start")
    }
    fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
        Err(LaunchError::NotApplied("unknown".into()))
    }
}
fn fixture() -> (tempfile::TempDir, ContextInput) {
    let t = crate::test_tmp::tempdir().unwrap();
    let executable = t.path().join("source");
    fs::write(&executable, b"test executable").unwrap();
    let env = BTreeMap::from([
        ("HOME".into(), t.path().to_str().unwrap().into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
        (
            "XDG_CONFIG_HOME".into(),
            t.path().join("config").to_str().unwrap().into(),
        ),
        ("SECRET_TOKEN".into(), "never persist".into()),
    ]);
    (t, ContextInput { env, executable })
}
#[tokio::test]
async fn unknown_job_preserves_receipts_and_reruns_without_replacing() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c.clone(), &mut Unknown)
        .await
        .unwrap_err()
        .to_string()
        .contains("unknown"));
    let input = Proof::read(&p.input).unwrap();
    let key = Proof::read(&p.key).unwrap();
    let spec = bootstrap::read_input(&p.input).unwrap();
    assert!(!spec.env.contains_key("SECRET_TOKEN"));
    assert!(spec.env.contains_key("XDG_CONFIG_HOME"));
    assert_eq!(spec.repo_root, Some(p.config.clone()));
    assert_eq!(
        crate::storage::CoordinatorStore::open_read_only(&p.state)
            .unwrap()
            .epoch(),
        CoordinatorEpoch(0)
    );
    assert!(initialize(c, &mut Unknown).await.is_err());
    input.verify().unwrap();
    key.verify().unwrap();
}
#[tokio::test]
async fn upgrade_and_foreign_changes_refuse_without_overwrite() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c.clone(), &mut Unknown).await.is_err());
    let input = Proof::read(&p.input).unwrap();
    fs::write(&c.executable, b"new binary").unwrap();
    assert!(initialize(c.clone(), &mut Unknown)
        .await
        .unwrap_err()
        .to_string()
        .contains("upgrade required"));
    input.verify().unwrap();
    fs::write(&c.executable, b"test executable").unwrap();
    fs::write(&p.key, b"foreign").unwrap();
    assert!(initialize(c, &mut Unknown)
        .await
        .unwrap_err()
        .to_string()
        .contains("changed"));
    assert_eq!(fs::read(&p.key).unwrap(), b"foreign");
}
#[tokio::test]
async fn foreign_input_and_relative_config_are_preflight_errors() {
    let (_t, mut c) = fixture();
    c.env.insert("XDG_CONFIG_HOME".into(), "relative".into());
    assert!(initialize(c.clone(), &mut Unknown).await.is_err());
    c.env.remove("XDG_CONFIG_HOME");
    // The worker would refuse a relative state home at start: not after install.
    c.env.insert("XDG_STATE_HOME".into(), "relative".into());
    let e = Paths::resolve(&c.env).err().unwrap().to_string();
    assert!(e.contains("XDG_STATE_HOME"), "{e}");
    assert!(initialize(c.clone(), &mut Unknown).await.is_err());
    c.env.remove("XDG_STATE_HOME");
    let p = Paths::resolve(&c.env).unwrap();
    private_dir(&p.config).unwrap();
    fs::write(&p.input, b"foreign").unwrap();
    assert!(initialize(c, &mut Unknown).await.is_err());
    assert!(!p.journal.exists());
    assert_eq!(fs::read(&p.input).unwrap(), b"foreign");
}
#[tokio::test]
async fn unobservable_service_values_are_refused_before_any_write() {
    for bad in ["o'brien", "a{b}", "back\\slash", "say\"hi"] {
        let (t, mut c) = fixture();
        let config = t.path().join(bad);
        c.env
            .insert("XDG_CONFIG_HOME".into(), config.to_str().unwrap().into());
        let error = initialize(c, &mut Unknown).await.unwrap_err();
        assert!(
            error.downcast_ref::<InvalidServiceInput>().is_some(),
            "{bad}"
        );
        assert!(!config.exists(), "{bad}");
    }
    let (t, mut c) = fixture();
    c.env.insert("TMPDIR".into(), "/tmp/it's".into());
    assert!(initialize(c, &mut Unknown).await.is_err());
    assert!(!t.path().join("config").exists());
}
#[test]
fn pending_publication_is_bound_to_inode_not_matching_bytes() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    private_dir(&p.config).unwrap();
    let stage = stage_path(&p.input);
    fs::write(&stage, b"private").unwrap();
    fs::set_permissions(&stage, fs::Permissions::from_mode(0o600)).unwrap();
    let proof = Proof::read(&stage).unwrap();
    fs::rename(&stage, p.config.join("old")).unwrap();
    fs::write(&stage, b"private").unwrap();
    fs::set_permissions(&stage, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(proof.verify().is_err());
}
#[test]
fn destination_links_and_unsafe_permissions_refuse() {
    let t = crate::test_tmp::tempdir().unwrap();
    let a = t.path().join("a");
    fs::write(&a, b"x").unwrap();
    fs::set_permissions(&a, fs::Permissions::from_mode(0o600)).unwrap();
    fs::hard_link(&a, t.path().join("b")).unwrap();
    assert!(Proof::read(&a).is_err());
    fs::remove_file(t.path().join("b")).unwrap();
    fs::set_permissions(&a, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Proof::read(&a).is_err());
    std::os::unix::fs::symlink(&a, t.path().join("link")).unwrap();
    assert!(Proof::read(&t.path().join("link")).is_err());
}

#[test]
fn directories_are_never_created_through_a_symlinked_ancestor() {
    let t = crate::test_tmp::tempdir().unwrap();
    let real = t.path().join("real");
    fs::create_dir(&real).unwrap();
    let alias = t.path().join("alias");
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    assert!(private_dir(&alias.join("state")).is_err());
    assert!(private_dir(&alias.join("nested/state")).is_err());
    assert_eq!(fs::read_dir(&real).unwrap().count(), 0);
    private_dir(&real.join("nested/state")).unwrap();
    assert!(private_dir(&alias.join("nested/state")).is_err());
}

/// `real` and a link `alias` to it, both private directories of one root.
fn aliased() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let t = crate::test_tmp::tempdir().unwrap();
    let (real, alias) = (t.path().join("real"), t.path().join("alias"));
    fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    (t, real, alias)
}
fn env_of(pairs: &[(&str, &Path)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
        .collect()
}
fn refusal(env: &BTreeMap<String, String>) -> String {
    match Paths::resolve(env) {
        Ok(_) => panic!("accepted {env:?}"),
        Err(error) => format!("{error:#}"),
    }
}

#[test]
fn a_symlinked_path_is_refused_by_name_with_the_way_out() {
    let (_t, real, alias) = aliased();
    // HOME itself is a link: LaunchAgents and the default state go through it.
    let error = refusal(&env_of(&[("HOME", &alias)]));
    assert!(
        error.contains(&format!("{} is a symbolic link", alias.display())),
        "{error}"
    );
    assert!(
        error.contains("set HOME or XDG_STATE_HOME to the real path"),
        "{error}"
    );
    // The link is named, not the path below it and not what it points at.
    assert!(!error.contains("Library/LaunchAgents"), "{error}");
    assert!(!error.contains(real.to_str().unwrap()), "{error}");
    // A link further down is named as that component.
    let home = real.join("home");
    fs::create_dir_all(home.join("target")).unwrap();
    std::os::unix::fs::symlink(home.join("target"), home.join(".local")).unwrap();
    let error = refusal(&env_of(&[("HOME", &home)]));
    assert!(
        error.contains(&format!(
            "{} is a symbolic link",
            home.join(".local").display()
        )),
        "{error}"
    );
    assert_eq!(fs::read_dir(home.join("target")).unwrap().count(), 0);
}

#[test]
fn home_is_checked_only_through_the_paths_made_from_it() {
    let (_t, real, alias) = aliased();
    let home = real.join("home");
    fs::create_dir_all(&home).unwrap();
    let elsewhere = real.join("elsewhere");
    fs::create_dir_all(elsewhere.join("target")).unwrap();
    // The default state and configuration directories of this HOME are links.
    for name in [".local", ".config"] {
        std::os::unix::fs::symlink(elsewhere.join("target"), home.join(name)).unwrap();
    }
    assert!(refusal(&env_of(&[("HOME", &home)])).contains("is a symbolic link"));
    let (state, config) = (elsewhere.join("state"), elsewhere.join("config"));
    // One of the two given as a real path is not enough: the other default is used.
    assert!(
        refusal(&env_of(&[("HOME", &home), ("XDG_STATE_HOME", &state)])).contains(&format!(
            "{} is a symbolic link",
            home.join(".config").display()
        ))
    );
    assert!(
        refusal(&env_of(&[("HOME", &home), ("XDG_CONFIG_HOME", &config)])).contains(&format!(
            "{} is a symbolic link",
            home.join(".local").display()
        ))
    );
    // Both given as real paths: the unused defaults under HOME do not matter.
    let paths = Paths::resolve(&env_of(&[
        ("HOME", &home),
        ("XDG_STATE_HOME", &state),
        ("XDG_CONFIG_HOME", &config),
    ]))
    .unwrap();
    assert_eq!(paths.state, state.join("belt/coordinator"));
    assert_eq!(paths.worker, state.join("belt/local-worker"));
    assert!(paths.config.starts_with(&config));
    assert_eq!(paths.services, home.join("Library/LaunchAgents"));
    // XDG_* paths are held to the same rule on their own components.
    for (variable, through) in [
        ("XDG_STATE_HOME", alias.join("elsewhere/state")),
        ("XDG_CONFIG_HOME", alias.join("elsewhere/config")),
    ] {
        let mut env = env_of(&[
            ("HOME", &home),
            ("XDG_STATE_HOME", &state),
            ("XDG_CONFIG_HOME", &config),
        ]);
        env.insert(variable.into(), through.to_str().unwrap().into());
        assert!(
            refusal(&env).contains(&format!("{} is a symbolic link", alias.display())),
            "{variable}"
        );
    }
    // LaunchAgents is always under HOME, so a HOME that is a link is refused
    // whatever XDG_* say; the service directory is never reached through one.
    let linked_home = alias.join("home");
    let error = refusal(&env_of(&[
        ("HOME", &linked_home),
        ("XDG_STATE_HOME", &state),
        ("XDG_CONFIG_HOME", &config),
    ]));
    assert!(
        error.contains(&format!("{} is a symbolic link", alias.display())),
        "{error}"
    );
}

#[tokio::test]
async fn initialize_through_a_symlinked_home_creates_nothing() {
    let (t, real, alias) = aliased();
    let executable = t.path().join("source");
    fs::write(&executable, b"test executable").unwrap();
    let mut env = env_of(&[("HOME", &alias)]);
    env.insert("PATH".into(), "/usr/bin:/bin".into());
    let error = initialize(ContextInput { env, executable }, &mut Unknown)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains(&format!("{} is a symbolic link", alias.display())),
        "{error:#}"
    );
    assert_eq!(fs::read_dir(&real).unwrap().count(), 0);
}

#[tokio::test]
async fn journal_recovers_before_and_after_no_replace_publication() {
    for already_published in [false, true] {
        let (_t, c) = fixture();
        let p = Paths::resolve(&c.env).unwrap();
        assert!(initialize(c.clone(), &mut Unknown).await.is_err());
        let mut j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
        let mut jf = JournalFile {
            path: p.journal.clone(),
            proof: Some(Proof::read(&p.journal).unwrap()),
        };
        let stage = stage_path(&p.input);
        fs::rename(&p.input, &stage).unwrap();
        j.files.retain(|f| f.path != p.input);
        j.pending = Some(Pending {
            staged: Proof::read(&stage).unwrap(),
            target: p.input.clone(),
        });
        jf.save(&j).unwrap();
        if already_published {
            rename_new(&stage, &p.input).unwrap();
        }
        assert!(initialize(c, &mut Unknown)
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown"));
        let j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
        assert!(j.pending.is_none());
        assert!(j.files.iter().any(|f| f.path == p.input));
    }
}

#[test]
fn source_executable_can_have_cargo_hardlinks() {
    let (_t, c) = fixture();
    fs::hard_link(&c.executable, c.executable.with_extension("linked")).unwrap();
    let (_f, digest) = binary_source(&c.executable).unwrap();
    assert_eq!(digest, format!("{:x}", Sha256::digest(b"test executable")));
}
#[tokio::test]
async fn uncertain_bootstrap_keeps_exact_managed_receipts() {
    struct Uncertain;
    impl Launcher for Uncertain {
        fn loaded_state(&mut self, _: &Plan, _: &Receipt) -> LoadedState {
            LoadedState::Absent
        }
        fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
            panic!("no stop verification")
        }
        fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
            assert_eq!(args[0], "bootstrap");
            Err(LaunchError::OutcomeUnknown("owned test uncertainty".into()))
        }
    }
    let (t, mut c) = fixture();
    let state_home = t.path().join("state-home");
    c.env
        .insert("XDG_STATE_HOME".into(), state_home.to_str().unwrap().into());
    let p = Paths::resolve(&c.env).unwrap();
    let err = initialize(c, &mut Uncertain).await.unwrap_err();
    assert!(err.downcast_ref::<Unconfirmed>().is_some());
    // The user's state home reaches the worker through the input, so the
    // worker shares the scope of the user's task-traffic-controller; the
    // service definition itself does not carry it (it rendered).
    let spec = bootstrap::read_input(&p.input).unwrap();
    assert_eq!(spec.env["XDG_STATE_HOME"], state_home.to_str().unwrap());
    let j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    assert!(j.plist.is_some());
    for f in &j.files {
        f.verify().unwrap();
    }
    assert!(p.input.exists());
}

#[tokio::test]
async fn binary_stage_directory_barrier_precedes_pending_and_os_actions() {
    struct FailStage {
        events: Vec<PathBuf>,
        journal: PathBuf,
    }
    impl Durability for FailStage {
        fn sync_directory(&mut self, path: &Path) -> std::io::Result<()> {
            self.events.push(path.into());
            if path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|s| s == "bin")
                && fs::read_dir(path)?.any(|e| {
                    e.unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".init-")
                })
            {
                let j: Journal = serde_json::from_slice(&fs::read(&self.journal)?).unwrap();
                assert!(j.pending.is_none());
                assert!(j.plist.is_none());
                return Err(std::io::Error::other(
                    "injected binary-stage directory fsync failure",
                ));
            }
            File::open(path)?.sync_all()
        }
    }
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    let mut io = FailStage {
        events: vec![],
        journal: p.journal.clone(),
    };
    assert!(initialize_with_durability(c, &mut Unknown, &mut io)
        .await
        .unwrap_err()
        .to_string()
        .contains("fsync failure"));
    let j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    assert!(!j.spec.executable.exists());
    assert!(j.pending.is_none());
    assert!(!p.input.exists());
    let directory = j.spec.executable.parent().unwrap();
    let first = io.events.iter().position(|e| e == directory).unwrap();
    let last = io.events.iter().rposition(|e| e == directory).unwrap();
    assert!(first < last);
    assert_eq!(io.events[first + 1], p.config.join("bin"));
    assert!(io.events[..first].contains(&p.config.join("bin")));
}
#[tokio::test]
async fn managed_ensure_never_spawns_during_init_stopped_or_missing_journal() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c, &mut Unknown).await.is_err());
    let spec = bootstrap::read_input(&p.input).unwrap();
    let start = p.state.join("startup.lock");
    assert!(bootstrap::connect_managed(&spec, &mut Unknown)
        .await
        .is_err());
    assert!(!start.exists());
    let original = fs::read(&p.journal).unwrap();
    let mut j: Journal = serde_json::from_slice(&original).unwrap();
    j.plist = None;
    fs::write(&p.journal, serde_json::to_vec(&j).unwrap()).unwrap();
    assert!(bootstrap::ensure(spec.clone()).await.is_err());
    assert!(!start.exists());
    assert_eq!(
        super::super::context::runtime_spec(&spec.env)
            .err()
            .unwrap()
            .code,
        ErrorCode::CommunicationUnknown
    );
    fs::write(&p.journal, &original).unwrap();
    fs::rename(&p.journal, p.config.join("saved-journal")).unwrap();
    assert!(bootstrap::ensure(spec).await.is_err());
    assert!(!start.exists());
    assert_eq!(
        crate::storage::CoordinatorStore::open_read_only(&p.state)
            .unwrap()
            .epoch(),
        CoordinatorEpoch(0)
    );
}
#[tokio::test]
async fn planned_database_publication_recovers_without_epoch_claim() {
    for published in [false, true] {
        let (_t, c) = fixture();
        let p = Paths::resolve(&c.env).unwrap();
        assert!(initialize(c.clone(), &mut Unknown).await.is_err());
        let mut j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
        let mut jf = JournalFile {
            path: p.journal.clone(),
            proof: Some(Proof::read(&p.journal).unwrap()),
        };
        let db = p.state.join("coordinator.db");
        let stage = stage_path(&db);
        fs::rename(&db, &stage).unwrap();
        NativeDurability.sync_directory(&p.state).unwrap();
        j.database = None;
        j.pending = Some(Pending {
            staged: Proof::read(&stage).unwrap(),
            target: db.clone(),
        });
        jf.save(&j).unwrap();
        if published {
            rename_new(&stage, &db).unwrap();
        }
        assert!(initialize(c, &mut Unknown)
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown"));
        let j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
        assert!(j.database.is_some());
        assert!(j.pending.is_none());
        assert_eq!(
            crate::storage::CoordinatorStore::open_read_only(&p.state)
                .unwrap()
                .epoch(),
            CoordinatorEpoch(0)
        );
    }
}
#[tokio::test]
async fn identity_initializer_rejects_active_owner_and_foreign_metadata_before_writes() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c, &mut Unknown).await.is_err());
    let j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    let cluster = j.cluster.unwrap();
    let coordinator = j.coordinator.unwrap();
    let owner = crate::storage::CoordinatorStore::open(&p.state, 1).unwrap();
    assert!(crate::storage::CoordinatorStore::initialize_identity(
        &p.state,
        &cluster,
        &coordinator,
        |_| panic!("must not publish")
    )
    .is_err());
    drop(owner);
    let db = Proof::read(&p.state.join("coordinator.db")).unwrap();
    assert!(crate::storage::CoordinatorStore::initialize_identity(
        &p.state,
        &ClusterId::generate(),
        &coordinator,
        |_| panic!("must not publish")
    )
    .is_err());
    assert!(crate::storage::CoordinatorStore::open_bound(
        &p.state,
        2,
        Some((ClusterId::generate(), coordinator))
    )
    .is_err());
    db.verify().unwrap();
    assert_eq!(
        crate::storage::CoordinatorStore::open_read_only(&p.state)
            .unwrap()
            .epoch(),
        CoordinatorEpoch(1)
    );
}

#[tokio::test]
async fn uncertain_journal_commit_retains_exact_native_stage_for_reinit() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c.clone(), &mut Unknown).await.is_err());
    let mut j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    let plan = plan(&p, &j).unwrap();
    let old = j.plist.take().unwrap();
    let mut native = DirectoryFiles::open(&p.services).unwrap();
    native.verify(&old).unwrap();
    fs::remove_file(old.path()).unwrap();
    j.files.retain(|proof| proof.path != old.path());
    let mut jf = JournalFile {
        path: p.journal.clone(),
        proof: Some(Proof::read(&p.journal).unwrap()),
    };
    jf.save(&j).unwrap();
    let error = native
        .install_journaled(&plan, |receipt, stage| {
            j.plist = Some(receipt.clone());
            j.pending = Some(Pending {
                staged: Proof::read(stage)?,
                target: receipt.path().into(),
            });
            jf.save_with(&j, |_| {
                Err(std::io::Error::other(
                    "injected journal directory fsync failure after rename",
                ))
            })
        })
        .unwrap_err();
    assert!(error.receipt.is_some());
    drop(native);
    let persisted: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    let staged = persisted.pending.unwrap().staged;
    staged.verify().unwrap();
    assert!(initialize(c, &mut Unknown)
        .await
        .unwrap_err()
        .to_string()
        .contains("unknown"));
    let after: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    assert!(after.pending.is_none());
    let mut published = staged;
    published.path = after.plist.unwrap().path().into();
    published.verify().unwrap();
}
#[tokio::test]
async fn uncertain_publication_directory_sync_preserves_recoverable_target() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c.clone(), &mut Unknown).await.is_err());
    let mut j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    let mut jf = JournalFile {
        path: p.journal.clone(),
        proof: Some(Proof::read(&p.journal).unwrap()),
    };
    let stage = stage_path(&p.input);
    fs::rename(&p.input, &stage).unwrap();
    NativeDurability.sync_directory(&p.config).unwrap();
    j.files.retain(|proof| proof.path != p.input);
    j.pending = Some(Pending {
        staged: Proof::read(&stage).unwrap(),
        target: p.input.clone(),
    });
    jf.save(&j).unwrap();
    assert!(
        rename_new_with(&stage, &p.input, |_| Err(std::io::Error::other(
            "injected post-rename fsync failure"
        )))
        .unwrap_err()
        .downcast_ref::<Unconfirmed>()
        .is_some()
    );
    assert!(initialize(c, &mut Unknown)
        .await
        .unwrap_err()
        .to_string()
        .contains("unknown"));
    let after: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    assert!(after.pending.is_none());
    assert!(after.files.iter().any(|proof| proof.path == p.input));
}

#[test]
fn concurrent_directory_winner_is_revalidated_and_synced() {
    struct Record(Vec<PathBuf>);
    impl Durability for Record {
        fn sync_directory(&mut self, path: &Path) -> std::io::Result<()> {
            self.0.push(path.to_owned());
            Ok(())
        }
    }
    let root = crate::test_tmp::tempdir().unwrap();
    let directory = root.path().join("winner");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let mut durability = Record(Vec::new());
    // Simulate the entry created after the caller's NotFound observation.
    create_directory_with(&directory, &mut durability).unwrap();
    assert_eq!(durability.0, [directory.clone(), root.path().to_owned()]);
    private_dir_with(&directory, &mut durability).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(private_dir_with(&directory, &mut durability).is_err());

    let file = root.path().join("foreign-file");
    fs::write(&file, b"foreign").unwrap();
    let link = root.path().join("foreign-link");
    std::os::unix::fs::symlink(&directory, &link).unwrap();
    for foreign in [file, link] {
        let mut durability = Record(Vec::new());
        assert!(create_directory_with(&foreign, &mut durability).is_err());
        assert!(durability.0.is_empty());
    }
}

/// A systemd user manager that takes the unit and never says whether it
/// started: the files of the Linux form are in place and retained.
struct SilentSystemd(Vec<Vec<String>>);
impl Launcher for SilentSystemd {
    fn backend(&self) -> Backend {
        Backend::Systemd
    }
    fn loaded_state(&mut self, _: &Plan, _: &Receipt) -> LoadedState {
        LoadedState::Absent
    }
    fn verify_loaded(&mut self, _: &Plan, _: &Receipt) -> Result<(), LaunchError> {
        panic!("no stop verification")
    }
    fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
        self.0.push(args.to_vec());
        Err(LaunchError::OutcomeUnknown("owned test uncertainty".into()))
    }
}

#[tokio::test]
async fn init_with_a_systemd_manager_writes_a_user_unit_and_no_launch_agent() {
    let (t, c) = fixture();
    let p = Paths::resolve_for(&c.env, Backend::Systemd).unwrap();
    assert_eq!(p.services, t.path().join("config/systemd/user"));
    assert_eq!(
        Paths::resolve(&c.env).unwrap().services,
        t.path().join("Library/LaunchAgents")
    );
    let mut manager = SilentSystemd(Vec::new());
    let err = initialize(c.clone(), &mut manager).await.unwrap_err();
    assert!(err.downcast_ref::<Unconfirmed>().is_some());
    let j: Journal = serde_json::from_slice(&fs::read(&p.journal).unwrap()).unwrap();
    assert!(j.systemd && j.backend() == Backend::Systemd);
    let receipt = j.plist.as_ref().unwrap();
    let unit = receipt.path();
    assert_eq!(unit.parent().unwrap(), p.services);
    assert_eq!(unit.extension().unwrap(), "service");
    let text = fs::read_to_string(unit).unwrap();
    assert!(text.contains("[Service]") && text.contains("Restart=always"));
    assert!(text.contains("WantedBy=default.target"));
    assert!(text.contains(j.spec.executable.to_str().unwrap()));
    assert!(text.contains(p.input.to_str().unwrap()));
    assert!(!text.contains("SECRET_TOKEN") && !text.contains("never persist"));
    // The manager was asked once, in its own words, for this unit.
    let name = unit.file_name().unwrap().to_str().unwrap().to_string();
    assert_eq!(
        manager.0,
        [vec!["--user".to_string(), "start".into(), name]]
    );
    assert!(!t.path().join("Library").exists());
    // The journal says which manager it was written for: launchd does not
    // take it over, and nothing is written for it.
    let error = initialize(c.clone(), &mut Unknown).await.unwrap_err();
    assert!(
        error.to_string().contains("another service manager"),
        "{error}"
    );
    assert!(!t.path().join("Library").exists());
    // A process that checks the managed service finds it where init put it,
    // whatever machine it runs on.
    let (journal, paths) = managed_journal(&j.spec).unwrap();
    assert_eq!(paths.backend, Backend::Systemd);
    assert_eq!(paths.services, p.services);
    assert!(journal.systemd);
    let health = managed_health(&j.spec, &mut Unknown).await.unwrap_err();
    assert!(
        health.to_string().contains("another service manager"),
        "{health}"
    );
}

#[tokio::test]
async fn a_launchd_journal_does_not_say_systemd_and_reads_as_before() {
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    assert!(initialize(c.clone(), &mut Unknown).await.is_err());
    let bytes = fs::read(&p.journal).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("systemd"));
    let j: Journal = serde_json::from_slice(&bytes).unwrap();
    assert!(!j.systemd && j.backend() == Backend::Launchd);
    // A systemd manager does not take over a journal written for launchd.
    let error = initialize(c, &mut SilentSystemd(Vec::new()))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("another service manager"),
        "{error}"
    );
    assert!(!p.config.parent().unwrap().join("systemd").exists());
}
