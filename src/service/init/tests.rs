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
    let t = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let p = Paths::resolve(&c.env).unwrap();
    private_dir(&p.config).unwrap();
    fs::write(&p.input, b"foreign").unwrap();
    assert!(initialize(c, &mut Unknown).await.is_err());
    assert!(!p.journal.exists());
    assert_eq!(fs::read(&p.input).unwrap(), b"foreign");
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
    let t = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let (_t, c) = fixture();
    let p = Paths::resolve(&c.env).unwrap();
    let err = initialize(c, &mut Uncertain).await.unwrap_err();
    assert!(err.downcast_ref::<Unconfirmed>().is_some());
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
    let root = tempfile::tempdir_in("/private/tmp").unwrap();
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
