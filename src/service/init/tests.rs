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
    // The SSH helpers find the resident binary at bin/resident without PATH.
    assert_eq!(
        fs::canonicalize(p.config.join("bin").join("resident").join("belt")).unwrap(),
        fs::canonicalize(&spec.executable).unwrap()
    );
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
    assert_eq!(paths.worker, home.join(".belt"));
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

#[tokio::test]
async fn managed_journal_rejects_a_second_worker_path_even_when_input_agrees() {
    let (_root, context) = fixture();
    let paths = Paths::resolve(&context.env).unwrap();
    assert!(initialize(context, &mut Unknown).await.is_err());
    let mut journal: Journal = serde_json::from_slice(&fs::read(&paths.journal).unwrap()).unwrap();
    journal.spec.worker_state_dir = paths.state.parent().unwrap().join("local-worker");
    fs::write(&paths.journal, serde_json::to_vec(&journal).unwrap()).unwrap();
    assert!(managed_journal(&journal.spec)
        .err()
        .unwrap()
        .to_string()
        .contains("init paths changed"));
}

#[tokio::test]
async fn malformed_worker_enrollment_is_refused_without_init_writes() {
    let (_root, context) = fixture();
    let paths = Paths::resolve(&context.env).unwrap();
    private_dir(&paths.config).unwrap();
    let worker_journal = super::super::worker_service::Paths::journal_path(&context.env).unwrap();
    fs::write(&worker_journal, b"existing worker-only installation").unwrap();
    let before = fs::read(&worker_journal).unwrap();
    let error = initialize(context, &mut Unknown).await.unwrap_err();
    assert!(error.to_string().contains("invalid worker enrollment"));
    assert_eq!(fs::read(worker_journal).unwrap(), before);
    assert!(!paths.journal.exists());
    assert!(!paths.state.exists());
    assert!(!paths.worker.exists());
}

#[test]
fn default_member_name_reduces_short_hostname_and_requires_valid_result() {
    assert_eq!(hostname_name("Mini_A.local").unwrap().as_str(), "Mini_A");
    assert_eq!(
        hostname_name("my mac🦀.example").unwrap().as_str(),
        "my-mac-"
    );
    for name in ["", ".local", &"a".repeat(129)] {
        assert!(hostname_name(name)
            .unwrap_err()
            .to_string()
            .contains("init --name"));
    }
}

#[tokio::test]
async fn init_name_cannot_rename_on_rerun() {
    let (_t, c) = fixture();
    let paths = Paths::resolve(&c.env).unwrap();
    let error = initialize_named(c.clone(), Some("chosen"), &mut Unknown)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unknown"));
    let before = fs::read(&paths.journal).unwrap();
    assert_eq!(
        bootstrap::read_input(&paths.input)
            .unwrap()
            .worker_id
            .as_str(),
        "chosen"
    );
    let error = initialize_named(c.clone(), Some("different"), &mut Unknown)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot rename"));
    assert_eq!(fs::read(&paths.journal).unwrap(), before);
    for name in [None, Some("chosen")] {
        assert!(initialize_named(c.clone(), name, &mut Unknown)
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown"));
    }
}

#[tokio::test]
async fn membership_rollback_restores_rows_and_preserves_service_state() {
    use crate::service::worker_service::{tests::Fake, InstallRequest, Membership};
    use crate::storage::{CoordinatorStore, WorkerStore};
    for running in [false, true] {
        for crash_step in 0..5 {
            let (_t, context) = fixture();
            let paths = Paths::resolve(&context.env).unwrap();
            assert!(initialize(context.clone(), &mut Unknown).await.is_err());
            let spec = bootstrap::read_input(&paths.input).unwrap();
            let worker = WorkerStore::open(&paths.worker, &spec.worker_id).unwrap();
            worker
                .for_member(&CoordinatorId::generate())
                .replace_member_pin("old-peer", 1234)
                .unwrap();
            worker.observe_group_key("old-first").unwrap();
            worker.observe_group_key("old-current").unwrap();
            worker
                .set_next_group_key(&crate::storage::KeyGeneration {
                    generation: 3,
                    fingerprint: "old-next".into(),
                })
                .unwrap();
            let image = worker.membership_image().unwrap();
            let original = CoordinatorStore::open_read_only(&paths.state).unwrap();
            let coordinator = original.coordinator_id().clone();
            let request = InstallRequest {
                schema: 1,
                system_user: false,
                cluster: ClusterId::generate(),
                worker_id: spec.worker_id.clone(),
                key_file_text: "ab".repeat(32) + "\n",
                binary_sha256: String::new(),
                membership: Some(Membership {
                    caller_name: WorkerId::parse("peer").unwrap(),
                    caller_id: CoordinatorId::generate(),
                    move_group: true,
                    join_singleton: false,
                    install: false,
                }),
            };
            let fake = Fake::default();
            {
                let mut w = fake.0.borrow_mut();
                w.loaded = running;
                w.omit_socket = true;
                w.state = paths.worker.clone();
            }
            let previous = crate::worker::control::Control::draining();
            crate::worker::control::write(&paths.worker, previous).unwrap();
            if crash_step == 0 {
                // This removal fails immediately after move_group commits, before key publication.
                let next = crate::group::next_key_path(&paths.key);
                fs::create_dir(&next).unwrap();
                assert!(handover(&context.env, &request, fake.clone()).is_err());
                assert_eq!(fake.0.borrow().loaded, running);
                assert_eq!(
                    crate::worker::control::read(&paths.worker).unwrap(),
                    previous
                );
                assert_eq!(fake.0.borrow().calls.len(), if running { 2 } else { 0 });
                fs::remove_dir(next).unwrap();
            } else {
                // Simulate termination at each publication boundary: recovery must
                // run before runtime_spec / managed receipt checks on the next call.
                membership_undo::prepare(&paths, previous, running, true).unwrap();
                worker.clear_group_membership().unwrap();
                worker.observe_group_key("new-current").unwrap();
                worker
                    .for_member(&request.membership.as_ref().unwrap().caller_id)
                    .replace_member_pin("peer", 5678)
                    .unwrap();
                CoordinatorStore::move_group(&paths.state, &request.cluster).unwrap();
                if crash_step >= 2 {
                    fs::write(&paths.key, &request.key_file_text).unwrap();
                }
                if crash_step >= 3 {
                    let mut moved = spec.clone();
                    moved.expected_cluster = Some(request.cluster.clone());
                    fs::write(&paths.input, serde_json::to_vec(&moved).unwrap()).unwrap();
                }
                if crash_step == 4 {
                    let mut journal = read_journal(&paths.journal).unwrap();
                    journal.cluster = Some(request.cluster.clone());
                    journal.spec.expected_cluster = Some(request.cluster.clone());
                    for path in [&paths.key, &paths.input] {
                        *journal.files.iter_mut().find(|p| &p.path == path).unwrap() =
                            Proof::read(path).unwrap();
                    }
                    JournalFile {
                        path: paths.journal.clone(),
                        proof: Some(Proof::read(&paths.journal).unwrap()),
                    }
                    .save(&journal)
                    .unwrap();
                }
                fake.0.borrow_mut().loaded = false;
            }
            membership_undo::recover(&paths, &mut fake.clone()).unwrap();
            assert_eq!(
                worker.membership_image().unwrap(),
                image,
                "step {crash_step}"
            );
            assert_eq!(
                CoordinatorStore::open_read_only(&paths.state)
                    .unwrap()
                    .cluster_id(),
                original.cluster_id()
            );
            let status = handover(&context.env, &request, fake.clone()).unwrap();
            assert_eq!(status.ready, running);
            assert_eq!(fake.0.borrow().loaded, running);
            assert_eq!(status.cluster, request.cluster);
            let db = CoordinatorStore::open_read_only(&paths.state).unwrap();
            assert_eq!(db.cluster_id(), &request.cluster);
            assert_eq!(db.coordinator_id(), &coordinator);
            managed_journal(&bootstrap::read_input(&paths.input).unwrap()).unwrap();
            assert!(!paths.config.join("membership-undo.json").exists());
            CoordinatorStore::move_group(&paths.state, &request.cluster).unwrap();
        }
    }
}

#[tokio::test]
async fn membership_failure_after_db_commit_restarts_and_reruns() {
    use crate::service::worker_service::{tests::Fake, InstallRequest, Membership};
    use crate::storage::{CoordinatorStore, WorkerStore};
    for crash_step in 0..5 {
        let (_t, context) = fixture();
        let paths = Paths::resolve(&context.env).unwrap();
        assert!(initialize(context.clone(), &mut Unknown).await.is_err());
        let spec = bootstrap::read_input(&paths.input).unwrap();
        WorkerStore::open(&paths.worker, &spec.worker_id).unwrap();
        let original = CoordinatorStore::open_read_only(&paths.state).unwrap();
        let coordinator = original.coordinator_id().clone();
        let request = InstallRequest {
            schema: 1,
            system_user: false,
            cluster: ClusterId::generate(),
            worker_id: spec.worker_id.clone(),
            key_file_text: "ab".repeat(32) + "\n",
            binary_sha256: String::new(),
            membership: Some(Membership {
                caller_name: WorkerId::parse("peer").unwrap(),
                caller_id: CoordinatorId::generate(),
                move_group: true,
                join_singleton: false,
                install: false,
            }),
        };
        let fake = Fake::default();
        {
            let mut w = fake.0.borrow_mut();
            w.loaded = true;
            w.omit_socket = true;
            w.state = paths.worker.clone();
        }
        let previous = crate::worker::control::Control::draining();
        crate::worker::control::write(&paths.worker, previous).unwrap();
        if crash_step == 0 {
            // This removal fails immediately after move_group commits, before key publication.
            let next = crate::group::next_key_path(&paths.key);
            fs::create_dir(&next).unwrap();
            assert!(handover(&context.env, &request, fake.clone()).is_err());
            assert!(fake.0.borrow().loaded);
            assert_eq!(
                crate::worker::control::read(&paths.worker).unwrap(),
                previous
            );
            assert_eq!(fake.0.borrow().calls.len(), 2);
            fs::remove_dir(next).unwrap();
        } else {
            // Simulate termination at each publication boundary: recovery must
            // run before runtime_spec / managed receipt checks on the next call.
            membership_undo::prepare(&paths, previous, true, true).unwrap();
            CoordinatorStore::move_group(&paths.state, &request.cluster).unwrap();
            if crash_step >= 2 {
                fs::write(&paths.key, &request.key_file_text).unwrap();
            }
            if crash_step >= 3 {
                let mut moved = spec.clone();
                moved.expected_cluster = Some(request.cluster.clone());
                fs::write(&paths.input, serde_json::to_vec(&moved).unwrap()).unwrap();
            }
            if crash_step == 4 {
                let mut journal = read_journal(&paths.journal).unwrap();
                journal.cluster = Some(request.cluster.clone());
                journal.spec.expected_cluster = Some(request.cluster.clone());
                for path in [&paths.key, &paths.input] {
                    *journal.files.iter_mut().find(|p| &p.path == path).unwrap() =
                        Proof::read(path).unwrap();
                }
                JournalFile {
                    path: paths.journal.clone(),
                    proof: Some(Proof::read(&paths.journal).unwrap()),
                }
                .save(&journal)
                .unwrap();
            }
            fake.0.borrow_mut().loaded = false;
        }
        let status = handover(&context.env, &request, fake.clone()).unwrap();
        assert!(status.ready && fake.0.borrow().loaded);
        assert_eq!(status.cluster, request.cluster);
        let db = CoordinatorStore::open_read_only(&paths.state).unwrap();
        assert_eq!(db.cluster_id(), &request.cluster);
        assert_eq!(db.coordinator_id(), &coordinator);
        managed_journal(&bootstrap::read_input(&paths.input).unwrap()).unwrap();
        assert!(!paths.config.join("membership-undo.json").exists());
        CoordinatorStore::move_group(&paths.state, &request.cluster).unwrap();
    }
}

#[tokio::test]
async fn membership_restart_crash_and_live_work_bail_preserve_state() {
    use crate::service::worker_service::{
        tests::{put_attempt, Fake},
        InstallRequest, Membership,
    };
    use crate::storage::WorkerStore;
    for live_work in [false, true] {
        let (_t, context) = fixture();
        let paths = Paths::resolve(&context.env).unwrap();
        assert!(initialize(context.clone(), &mut Unknown).await.is_err());
        let spec = bootstrap::read_input(&paths.input).unwrap();
        let worker = WorkerStore::open(&paths.worker, &spec.worker_id).unwrap();
        let peer = CoordinatorId::generate();
        let request = InstallRequest {
            schema: 1,
            system_user: false,
            cluster: ClusterId::generate(),
            worker_id: spec.worker_id.clone(),
            key_file_text: "ab".repeat(32) + "\n",
            binary_sha256: String::new(),
            membership: Some(Membership {
                caller_name: WorkerId::parse("peer").unwrap(),
                caller_id: peer.clone(),
                move_group: true,
                join_singleton: false,
                install: false,
            }),
        };
        let fake = Fake::default();
        {
            let mut w = fake.0.borrow_mut();
            w.loaded = true;
            w.omit_socket = true;
            w.state = paths.worker.clone();
            if live_work {
                let state = paths.worker.clone();
                let id = spec.worker_id.clone();
                let peer = peer.clone();
                w.after_stop = Some(Box::new(move || {
                    let worker = WorkerStore::open(&state, &id).unwrap();
                    worker
                        .for_member(&peer)
                        .replace_member_pin("late-peer", 42)
                        .unwrap();
                    worker.observe_group_key("late-key").unwrap();
                    let db = rusqlite::Connection::open(state.join("worker.db")).unwrap();
                    put_attempt(&db, &peer, 1, "active");
                }));
            } else {
                w.before_start = Some(Box::new(|| panic!("simulated crash before restart")));
            }
        }
        if live_work {
            let error = handover(&context.env, &request, fake.clone()).unwrap_err();
            assert!(error.to_string().contains("live work appeared"));
            assert!(worker
                .members()
                .unwrap()
                .iter()
                .any(|(name, id)| name == "late-peer" && id == peer.as_str()));
            assert_eq!(
                worker.group_keys().unwrap().0.unwrap().fingerprint,
                "late-key"
            );
            assert_eq!(worker.count_group_work().unwrap(), 1);
            assert!(fake.0.borrow().loaded);
        } else {
            let crash = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handover(&context.env, &request, fake.clone())
            }));
            assert!(crash.is_err());
            assert!(!fake.0.borrow().loaded);
            assert!(paths.config.join("membership-undo.json").exists());
            let status = handover(&context.env, &request, fake.clone()).unwrap();
            assert!(status.ready && fake.0.borrow().loaded);
            assert!(!paths.config.join("membership-undo.json").exists());
        }
    }
}

#[tokio::test]
async fn workers_stop_start_use_init_receipt_and_preserve_refusals() {
    use super::super::worker_service::{self as ws, tests::Fake, Submitters};
    use crate::storage::{CoordinatorStore, WorkerStore};
    use crate::worker::control::{self, Control};
    for cancel in [false, true] {
        let (_tmp, context) = fixture();
        let paths = Paths::resolve(&context.env).unwrap();
        assert!(initialize(context.clone(), &mut Unknown).await.is_err());
        let spec = bootstrap::read_input(&paths.input).unwrap();
        let worker = WorkerStore::open(&paths.worker, &spec.worker_id).unwrap();
        let store = std::rc::Rc::new(CoordinatorStore::open(&paths.state, 1).unwrap());
        let binding = paths.worker.join("worker-binding.json");
        fs::write(
            &binding,
            serde_json::to_vec(&(1, store.cluster_id(), &spec.worker_id, "key")).unwrap(),
        )
        .unwrap();
        fs::set_permissions(&binding, fs::Permissions::from_mode(0o600)).unwrap();
        let run = RunId::generate();
        store
            .create_run(&run, None, None, &serde_json::json!({}), 1)
            .unwrap();
        let task = crate::ids::TaskId::generate();
        store
            .create_task(&task, &run, &serde_json::json!({}), 1)
            .unwrap();
        let fake = Fake::default();
        {
            let mut world = fake.0.borrow_mut();
            world.loaded = true;
            world.omit_socket = true;
            world.state = paths.worker.clone();
            let state = paths.worker.clone();
            world.after_stop = Some(Box::new(move || {
                let seen = control::read(&state).unwrap();
                assert!(seen.drain);
                assert_eq!(seen.cancel_running, cancel);
            }));
        }
        let guard = Submitters {
            all: false,
            caller: Some(store.coordinator_id().clone()),
        };
        let error =
            ws::stop_for(&context.env, fake.clone(), cancel, Duration::ZERO, &guard).unwrap_err();
        assert!(
            error.to_string().contains("own non-terminal runs"),
            "{error:#}"
        );
        assert!(fake.0.borrow().calls.is_empty());
        assert_eq!(control::read(&paths.worker).unwrap(), Control::default());
        control::write(&paths.worker, Control::draining()).unwrap();
        // --all-submitters uses the same drain-and-stop manager, even for
        // cancel-running, and stops the init service, never a worker service.
        let status = ws::stop_for(
            &context.env,
            fake.clone(),
            cancel,
            Duration::ZERO,
            &Submitters::all(),
        )
        .unwrap();
        assert_eq!(status.loaded, Some(false));
        let journal = read_journal(&paths.journal).unwrap();
        let plan = plan(&paths, &journal).unwrap();
        assert_eq!(
            fake.0.borrow().calls[0],
            plan.stop_arguments(journal.plist.as_ref().unwrap().path())
                .unwrap()
        );
        let status = ws::start(&context.env, fake.clone(), Duration::ZERO).unwrap();
        assert_eq!(status.loaded, Some(true));
        assert_eq!(
            fake.0.borrow().calls[1],
            plan.start_arguments(journal.plist.as_ref().unwrap().path())
                .unwrap()
        );
        assert_eq!(control::read(&paths.worker).unwrap(), Control::draining());
        assert!(!paths.config.join("worker-service.lock").exists());
        control::write(&paths.worker, Control::default()).unwrap();
        assert!(!paths.config.join("worker-service.json").exists());
        // A cancelled task is no longer active; an unreadable worker DB
        // still refuses before touching the service or leaving a drain.
        store.cancel_task(&task, 2).unwrap();
        store.close_run(&run, 2).unwrap();
        assert!(!store.has_nonterminal_runs().unwrap());
        // A task arriving during the drain is caught by the last check.
        let late_run = RunId::generate();
        store
            .create_run(&late_run, None, None, &serde_json::json!({}), 3)
            .unwrap();
        let late_task = crate::ids::TaskId::generate();
        let inserted = late_task.clone();
        let writer = store.clone();
        let mut checks = 0;
        fake.0.borrow_mut().before_verify = Some(Box::new(move || {
            checks += 1;
            if checks == 2 {
                writer
                    .create_task(&inserted, &late_run, &serde_json::json!({}), 4)
                    .unwrap();
            }
        }));
        control::write(&paths.worker, Control::draining()).unwrap();
        let error =
            ws::stop_for(&context.env, fake.clone(), cancel, Duration::ZERO, &guard).unwrap_err();
        assert!(
            error.to_string().contains("own non-terminal runs"),
            "{error:#}"
        );
        assert_eq!(
            fake.0.borrow().calls.len(),
            2,
            "no bootout after the second check"
        );
        assert_eq!(control::read(&paths.worker).unwrap(), Control::draining());
        fake.0.borrow_mut().before_verify = None;
        store.cancel_task(&late_task, 5).unwrap();
        control::write(&paths.worker, Control::default()).unwrap();
        let db = rusqlite::Connection::open(paths.worker.join("worker.db")).unwrap();
        ws::tests::put_attempt(&db, &CoordinatorId::generate(), 1, "active");
        let error =
            ws::stop_for(&context.env, fake.clone(), cancel, Duration::ZERO, &guard).unwrap_err();
        assert!(error.to_string().contains("other members"), "{error:#}");
        assert_eq!(control::read(&paths.worker).unwrap(), Control::default());
        // Override permits draining, not bootout before accepted work ends.
        assert!(ws::stop_for(
            &context.env,
            fake.clone(),
            cancel,
            Duration::ZERO,
            &Submitters::all()
        )
        .is_err());
        assert_eq!(fake.0.borrow().calls.len(), 2);
        assert!(control::read(&paths.worker).unwrap().drain);
        drop(db);
        drop(worker);
        fs::write(paths.worker.join("worker.db"), b"unreadable").unwrap();
        assert!(ws::stop_for(&context.env, fake.clone(), cancel, Duration::ZERO, &guard).is_err());
        assert_eq!(fake.0.borrow().calls.len(), 2);
    }
}

#[tokio::test]
async fn worker_start_refuses_pending_membership_undo() {
    use super::super::worker_service::{self as ws, tests::Fake};
    let (_tmp, context) = fixture();
    let paths = Paths::resolve(&context.env).unwrap();
    assert!(initialize(context.clone(), &mut Unknown).await.is_err());
    fs::write(paths.config.join("membership-undo.json"), b"invalid undo").unwrap();
    let fake = Fake::default();
    assert!(ws::start(&context.env, fake.clone(), Duration::ZERO).is_err());
    assert!(fake.0.borrow().calls.is_empty());
    assert!(paths.config.join("membership-undo.json").exists());
}

#[tokio::test]
async fn a_rotated_managed_key_still_proves_the_runtime_binding() {
    let (_root, context) = fixture();
    let paths = Paths::resolve(&context.env).unwrap();
    assert!(initialize(context, &mut Unknown).await.is_err());
    let spec = read_journal(&paths.journal).unwrap().spec;
    managed_journal(&spec).unwrap();
    let staged = crate::group::next_key_path(&paths.key);
    crate::secrets::key::init(&staged).unwrap();
    assert!(crate::group::switch_to_next(&paths.key).unwrap());
    managed_journal(&spec).unwrap();
    assert!(read_journal(&paths.journal)
        .unwrap()
        .switching_key
        .is_none());
}

#[tokio::test]
async fn a_crash_during_managed_key_rotation_accepts_only_the_recorded_files() {
    let (_root, context) = fixture();
    let paths = Paths::resolve(&context.env).unwrap();
    assert!(initialize(context, &mut Unknown).await.is_err());
    let mut j = read_journal(&paths.journal).unwrap();
    let staged = crate::group::next_key_path(&paths.key);
    crate::secrets::key::init(&staged).unwrap();
    let mut next = Proof::read(&staged).unwrap();
    next.path = paths.key.clone();
    j.switching_key = Some(next);
    fs::write(&paths.journal, serde_json::to_vec(&j).unwrap()).unwrap();
    managed_journal(&j.spec).unwrap();
    fs::rename(staged, &paths.key).unwrap();
    managed_journal(&j.spec).unwrap();
    finish_key_change(&j.spec).unwrap();
    let disk = fs::read_to_string(&paths.journal).unwrap();
    assert!(!disk.contains("switching_key"));
    read_raw_journal(&paths.journal)
        .unwrap()
        .files
        .iter()
        .for_each(|p| p.verify().unwrap());
    let bytes = fs::read(&paths.key).unwrap();
    let mut replacement = tempfile::NamedTempFile::new_in(&paths.config).unwrap();
    replacement.write_all(&bytes).unwrap();
    replacement.persist(&paths.key).unwrap();
    assert!(managed_journal(&j.spec).is_err());
}

/// A journal that still names a `switching_key`, as a crash between the two
/// journal writes of a key rotation leaves it. Returns the spec, the paths and
/// the receipt of the staged key.
async fn journal_with_leftover(context: ContextInput) -> (Spec, Paths, Proof) {
    let paths = Paths::resolve(&context.env).unwrap();
    assert!(initialize(context, &mut Unknown).await.is_err());
    let mut j = read_journal(&paths.journal).unwrap();
    let staged = crate::group::next_key_path(&paths.key);
    crate::secrets::key::init(&staged).unwrap();
    let mut next = Proof::read(&staged).unwrap();
    next.path = paths.key.clone();
    j.switching_key = Some(next.clone());
    fs::write(&paths.journal, serde_json::to_vec(&j).unwrap()).unwrap();
    (j.spec, paths, next)
}

#[tokio::test]
async fn a_start_clears_a_leftover_key_switch_before_the_rename() {
    let (_root, context) = fixture();
    let (spec, paths, _) = journal_with_leftover(context).await;
    let before = read_raw_journal(&paths.journal).unwrap();
    let old_receipt = before.files.iter().find(|p| p.path == paths.key).unwrap();
    let old_key = fs::read(&paths.key).unwrap();
    settle_key_change(&spec).unwrap();
    let disk = fs::read_to_string(&paths.journal).unwrap();
    assert!(!disk.contains("switching_key"));
    let after = read_raw_journal(&paths.journal).unwrap();
    let kept = after.files.iter().find(|p| p.path == paths.key).unwrap();
    assert_eq!(
        serde_json::to_value(kept).unwrap(),
        serde_json::to_value(old_receipt).unwrap()
    );
    kept.verify().unwrap();
    assert_eq!(fs::read(&paths.key).unwrap(), old_key);
    assert!(crate::group::next_key_path(&paths.key).exists());
}

#[tokio::test]
async fn a_start_clears_a_leftover_key_switch_after_the_rename() {
    let (_root, context) = fixture();
    let (spec, paths, next) = journal_with_leftover(context).await;
    fs::rename(crate::group::next_key_path(&paths.key), &paths.key).unwrap();
    settle_key_change(&spec).unwrap();
    let disk = fs::read_to_string(&paths.journal).unwrap();
    assert!(!disk.contains("switching_key"));
    let after = read_raw_journal(&paths.journal).unwrap();
    let kept = after.files.iter().find(|p| p.path == paths.key).unwrap();
    assert_eq!(
        serde_json::to_value(kept).unwrap(),
        serde_json::to_value(&next).unwrap()
    );
    kept.verify().unwrap();
}

#[tokio::test]
async fn a_start_refuses_a_leftover_key_switch_it_cannot_resolve() {
    let (_root, context) = fixture();
    let (spec, paths, _) = journal_with_leftover(context).await;
    let bytes = fs::read(&paths.key).unwrap();
    let mut replacement = tempfile::NamedTempFile::new_in(&paths.config).unwrap();
    replacement.write_all(&bytes).unwrap();
    replacement.persist(&paths.key).unwrap();
    assert!(settle_key_change(&spec).is_err());
    assert!(fs::read_to_string(&paths.journal)
        .unwrap()
        .contains("switching_key"));
}

#[tokio::test]
async fn a_start_leaves_a_leftover_to_a_change_that_holds_the_lock() {
    let (_root, context) = fixture();
    let (spec, paths, _) = journal_with_leftover(context).await;
    let lock = resident::secure_file(&paths.config.join("init.lock"), true).unwrap();
    lock.lock().unwrap();
    settle_key_change(&spec).unwrap();
    assert!(fs::read_to_string(&paths.journal)
        .unwrap()
        .contains("switching_key"));
    assert!(finish_key_change(&spec).is_err());
}

#[tokio::test]
async fn a_start_without_a_leftover_changes_nothing() {
    let (_root, context) = fixture();
    let paths = Paths::resolve(&context.env).unwrap();
    assert!(initialize(context, &mut Unknown).await.is_err());
    let spec = read_journal(&paths.journal).unwrap().spec;
    let before = fs::read(&paths.journal).unwrap();
    settle_key_change(&spec).unwrap();
    assert_eq!(fs::read(&paths.journal).unwrap(), before);
}
