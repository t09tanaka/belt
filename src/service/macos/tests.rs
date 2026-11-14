use super::*;
use policy::{render, Definition, Mode};
use std::collections::BTreeMap;
use std::{cell::RefCell, rc::Rc};
type Loaded = Option<(String, PathBuf, [u8; 32])>;
type SharedLoaded = Rc<RefCell<Loaded>>;
#[derive(Default)]
struct FakeLaunch {
    calls: Vec<Vec<String>>,
    fail: Vec<usize>,
    unknown: bool,
    loaded: SharedLoaded,
    locked_root: Option<PathBuf>,
}
impl Launcher for FakeLaunch {
    fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
        if args[0] == "bootout" {
            if let Some(root) = &self.locked_root {
                assert!(lifecycle_is_locked(root));
            }
        }
        self.calls.push(args.to_vec());
        if self.fail.contains(&self.calls.len()) {
            return Err(if self.unknown {
                LaunchError::OutcomeUnknown("injected".into())
            } else {
                LaunchError::NotApplied("injected".into())
            });
        }
        if args[0] == "bootstrap" {
            *self.loaded.borrow_mut() = Some((
                args[1].clone(),
                args[2].clone().into(),
                Sha256::digest(std::fs::read(&args[2]).unwrap()).into(),
            ));
        } else if args[0] == "bootout" {
            *self.loaded.borrow_mut() = None;
        }
        Ok(())
    }
    fn verify_loaded(&mut self, p: &Plan, r: &Receipt) -> Result<(), LaunchError> {
        let args = p.start_arguments(r.path()).unwrap();
        if *self.loaded.borrow()
            == Some((
                args[1].clone(),
                r.path().into(),
                Sha256::digest(p.plist().as_bytes()).into(),
            ))
        {
            Ok(())
        } else {
            Err(LaunchError::NotApplied("foreign/unconfirmed job".into()))
        }
    }
}
#[derive(Default)]
struct FakeWorker {
    calls: Vec<WorkerRequest>,
    fail: bool,
    identities: Vec<WorkerIdentity>,
    replace_on_stop: Option<(SharedLoaded, Loaded)>,
    mutate_receipt_on_stop: Option<PathBuf>,
    locked_root: Option<PathBuf>,
}
impl WorkerControl for FakeWorker {
    fn request(&mut self, identity: &WorkerIdentity, request: WorkerRequest) -> anyhow::Result<()> {
        self.identities.push(identity.clone());
        self.calls.push(request);
        anyhow::ensure!(!self.fail, "worker unavailable");
        if matches!(request, WorkerRequest::Stop { .. }) {
            if let Some(root) = &self.locked_root {
                assert!(lifecycle_is_locked(root));
            }
            if let Some(path) = &self.mutate_receipt_on_stop {
                std::fs::write(path, b"foreign replacement").unwrap();
            }
            if let Some((loaded, replacement)) = &self.replace_on_stop {
                *loaded.borrow_mut() = replacement.clone();
            }
        }
        Ok(())
    }
}
fn plan() -> Plan {
    render(&Definition {
        label: "org.example.worker".into(),
        mode: Mode::User { uid: 501 },
        executable: "/opt/example/runner".into(),
        arguments: vec![
            "--config-file".into(),
            "/var/example/private/config.json".into(),
        ],
        environment: BTreeMap::new(),
        working_directory: "/var/example".into(),
    })
    .unwrap()
}
fn identity() -> WorkerIdentity {
    WorkerIdentity {
        cluster_id: crate::ids::ClusterId::generate(),
        worker_id: crate::ids::WorkerId::parse("example").unwrap(),
    }
}
fn manager(root: &Path) -> Manager<DirectoryFiles, FakeLaunch, FakeWorker> {
    Manager {
        identity: identity(),
        files: DirectoryFiles::open(root).unwrap(),
        launcher: FakeLaunch::default(),
        worker: FakeWorker::default(),
    }
}
#[test]
fn lifecycle_is_separate_from_worker_control() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let receipt = m.install_and_start(&p).unwrap();
    assert_eq!(std::fs::read_to_string(receipt.path()).unwrap(), p.plist());
    assert_eq!(
        std::fs::metadata(receipt.path()).unwrap().mode() & 0o777,
        0o600
    );
    assert_eq!(
        m.launcher.calls,
        [vec![
            "bootstrap".to_owned(),
            "gui/501".to_owned(),
            receipt.path().to_str().unwrap().to_owned()
        ]]
    );
    assert!(m.worker.calls.is_empty());
    m.ensure_installed(&p, &receipt).unwrap();
    assert!(m.install(&p).is_err());
    m.drain().unwrap();
    m.resume().unwrap();
    m.stop(&p, &receipt, false).unwrap();
    m.start(&p, &receipt).unwrap();
    m.stop(&p, &receipt, true).unwrap();
    assert!(m.worker.identities.iter().all(|i| i == &m.identity));
    assert_eq!(
        m.worker.calls,
        [
            WorkerRequest::Drain,
            WorkerRequest::Resume,
            WorkerRequest::Stop {
                cancel_running: false
            },
            WorkerRequest::Stop {
                cancel_running: true
            }
        ]
    );
    assert_eq!(
        m.launcher.calls[1],
        ["bootout", "gui/501", receipt.path().to_str().unwrap()]
    );
    m.remove(&receipt).unwrap();
    assert!(!receipt.path().exists());
    assert!(m.remove(&receipt).is_err());
}
#[test]
fn proven_start_rejection_rolls_back_but_unknown_preserves_evidence() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    m.launcher.fail = vec![1];
    assert!(m.install_and_start(&plan()).is_err());
    assert!(!tmp.path().join(plan().filename()).exists());
    assert_eq!(m.launcher.calls.len(), 1);
    m.launcher.calls.clear();
    m.launcher.fail = vec![1];
    m.launcher.unknown = true;
    let foreign = Some(("gui/501".into(), tmp.path().join("foreign.plist"), [7; 32]));
    *m.launcher.loaded.borrow_mut() = foreign.clone();
    let e = m.install_and_start(&plan()).unwrap_err();
    assert!(e.to_string().contains("retaining"));
    let receipt = e.receipt.unwrap();
    m.files.verify(&receipt).unwrap();
    assert_eq!(m.launcher.calls.len(), 1);
    assert!(m.stop(&plan(), &receipt, true).is_err());
    assert!(m.worker.calls.is_empty());
    assert_eq!(m.launcher.calls.len(), 1);
    assert_eq!(*m.launcher.loaded.borrow(), foreign);
    assert!(tmp.path().join(plan().filename()).exists());
}
#[test]
fn worker_failure_prevents_bootout() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let r = m.install(&p).unwrap();
    m.start(&p, &r).unwrap();
    m.launcher.calls.clear();
    m.worker.fail = true;
    assert!(m.stop(&p, &r, true).is_err());
    assert!(m.drain().is_err());
    assert!(m.resume().is_err());
    assert!(m.launcher.calls.is_empty());
}
#[test]
fn foreign_modified_or_replaced_files_are_never_adopted() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let r = m.install(&p).unwrap();
    std::fs::write(r.path(), "foreign").unwrap();
    assert!(m.start(&p, &r).is_err());
    assert!(m.remove(&r).is_err());
    assert!(m.launcher.calls.is_empty());
    std::fs::write(r.path(), p.plist()).unwrap();
    m.ensure_installed(&p, &r).unwrap();
    let foreign = tmp.path().join("foreign");
    std::fs::write(&foreign, p.plist()).unwrap();
    std::fs::set_permissions(&foreign, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::rename(&foreign, r.path()).unwrap();
    assert!(m.remove(&r).is_err());
    assert_eq!(std::fs::read_to_string(r.path()).unwrap(), p.plist());
    assert!(m.install(&p).is_err());
}
#[test]
fn roots_symlinks_permissions_hardlinks_and_foreign_receipts_rejected() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let root = tmp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    assert!(DirectoryFiles::open(Path::new("relative")).is_err());
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(DirectoryFiles::open(&root).is_err());
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let link = tmp.path().join("link");
    std::os::unix::fs::symlink(&root, &link).unwrap();
    assert!(DirectoryFiles::open(&link).is_err());
    let mut m = manager(&root);
    let p = plan();
    let r = m.install(&p).unwrap();
    let other = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    assert!(manager(other.path()).remove(&r).is_err());
    std::fs::set_permissions(r.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(m.remove(&r).is_err());
    std::fs::set_permissions(r.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
    let hard = root.join("hard");
    std::fs::hard_link(r.path(), &hard).unwrap();
    assert!(m.remove(&r).is_err());
    std::fs::remove_file(hard).unwrap();
    std::fs::remove_file(r.path()).unwrap();
    std::os::unix::fs::symlink("/dev/null", r.path()).unwrap();
    assert!(m.remove(&r).is_err());
    assert!(m.install(&p).is_err());
    let saved = tmp.path().join("saved");
    std::fs::rename(&root, &saved).unwrap();
    std::fs::create_dir(&root).unwrap();
    assert!(m.install(&p).is_err());
}
#[test]
fn a_receipt_cannot_start_another_plan() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let r = m.install(&p).unwrap();
    let different = render(&Definition {
        label: "org.example.other".into(),
        mode: Mode::User { uid: 501 },
        executable: "/opt/example/runner".into(),
        arguments: vec![],
        environment: BTreeMap::new(),
        working_directory: "/var/example".into(),
    })
    .unwrap();
    assert!(m.start(&different, &r).is_err());
    assert!(m.launcher.calls.is_empty());
}

struct FailingFiles {
    inner: DirectoryFiles,
    fail_install: bool,
    fail_remove: bool,
}
impl ManagedFiles for FailingFiles {
    type LifecycleLease = Arc<DirectoryLock>;
    fn lifecycle_lock(&mut self) -> anyhow::Result<Self::LifecycleLease> {
        self.inner.lifecycle_lock()
    }
    fn install(&mut self, p: &Plan) -> Result<Receipt, InstallStartError> {
        if self.fail_install {
            return Err(InstallStartError::without_receipt(anyhow::anyhow!(
                "injected publish failure"
            )));
        }
        self.inner.install(p)
    }
    fn verify(&mut self, r: &Receipt) -> anyhow::Result<()> {
        self.inner.verify(r)
    }
    fn remove(&mut self, r: &Receipt) -> anyhow::Result<()> {
        anyhow::ensure!(!self.fail_remove, "injected rollback failure");
        self.inner.remove(r)
    }
}
#[test]
fn publish_and_rollback_failures_remain_errors() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = Manager {
        identity: identity(),
        files: FailingFiles {
            inner: DirectoryFiles::open(tmp.path()).unwrap(),
            fail_install: true,
            fail_remove: false,
        },
        launcher: FakeLaunch::default(),
        worker: FakeWorker::default(),
    };
    assert!(m.install_and_start(&plan()).unwrap_err().receipt.is_none());
    assert!(m.launcher.calls.is_empty());
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    m.files.fail_install = false;
    m.files.fail_remove = true;
    m.launcher.fail = vec![1];
    let error = m.install_and_start(&plan()).unwrap_err();
    assert!(error.to_string().contains("file rollback failed"));
    m.files.verify(&error.receipt.unwrap()).unwrap();
    assert!(tmp.path().join(plan().filename()).exists());
}

#[test]
fn post_publish_sync_and_rollback_failures_preserve_exact_evidence() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    for fail_verification in [true, false] {
        let error = files
            .install_with(
                &plan(),
                |_| Err(io::Error::other("sync injected")),
                |files, receipt| {
                    if fail_verification {
                        anyhow::bail!("verification injected");
                    }
                    anyhow::ensure!(
                        files.read(receipt.path())?.0 == *receipt,
                        "identity changed"
                    );
                    anyhow::bail!("remove injected");
                },
            )
            .unwrap_err();
        let receipt = error.receipt.expect("published file evidence retained");
        files.verify(&receipt).unwrap();
        let encoded = serde_json::to_vec(&receipt).unwrap();
        let restored: Receipt = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored, receipt);
        files.remove(&restored).unwrap();
    }
}
#[test]
fn worker_ack_cannot_authorize_bootout_after_loaded_job_replacement() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let r = m.install_and_start(&p).unwrap();
    let foreign = Some(("gui/501".into(), tmp.path().join("foreign.plist"), [9; 32]));
    m.worker.replace_on_stop = Some((m.launcher.loaded.clone(), foreign.clone()));
    assert!(m.stop(&p, &r, true).is_err());
    assert_eq!(
        m.worker.calls,
        [WorkerRequest::Stop {
            cancel_running: true
        }]
    );
    assert_eq!(m.launcher.calls.len(), 1);
    assert_eq!(*m.launcher.loaded.borrow(), foreign);
    m.files.verify(&r).unwrap();
}

fn lifecycle_is_locked(root: &Path) -> bool {
    use std::os::fd::AsRawFd;
    let file = File::open(root).unwrap();
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        unsafe {
            libc::flock(file.as_raw_fd(), libc::LOCK_UN);
        }
        false
    } else {
        assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
        true
    }
}
#[test]
fn lifecycle_lease_covers_nested_checks_worker_ack_and_path_bootout() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let receipt = m.install_and_start(&p).unwrap();
    m.worker.locked_root = Some(tmp.path().into());
    m.launcher.locked_root = Some(tmp.path().into());
    m.stop(&p, &receipt, false).unwrap();
    assert!(!lifecycle_is_locked(tmp.path()));
    assert_eq!(
        m.launcher.calls[1],
        ["bootout", "gui/501", receipt.path().to_str().unwrap()]
    );
}
#[test]
fn receipt_change_during_ack_prevents_bootout() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let mut m = manager(tmp.path());
    let p = plan();
    let receipt = m.install_and_start(&p).unwrap();
    m.worker.mutate_receipt_on_stop = Some(receipt.path().into());
    assert!(m
        .stop(&p, &receipt, false)
        .unwrap_err()
        .to_string()
        .contains("reconciliation required"));
    assert_eq!(m.worker.calls.len(), 1);
    assert_eq!(m.launcher.calls.len(), 1);
    assert!(receipt.path().exists());
    assert!(!lifecycle_is_locked(tmp.path()));
}
#[derive(Default)]
struct FakeRunner {
    calls: Rc<RefCell<Vec<Vec<String>>>>,
    outputs: std::collections::VecDeque<Result<CommandOutput, String>>,
}
impl CommandRunner for FakeRunner {
    fn run(&mut self, arguments: &[String]) -> Result<CommandOutput, String> {
        self.calls.borrow_mut().push(arguments.to_vec());
        self.outputs.pop_front().expect("unexpected command")
    }
}
struct FakeAccounts(Option<Account>);
impl AccountLookup for FakeAccounts {
    fn lookup(&mut self, _: &str) -> Result<Option<Account>, String> {
        Ok(self.0.clone())
    }
}
fn native_output(plan: &Plan, receipt: &Receipt) -> CommandOutput {
    let target = plan.service_target();
    let domain = target.rsplit_once('/').unwrap().0;
    let mut fields = format!(
        "{target} = {{\n\tpath = {}\n\tprogram = {}\n\targuments = {{\n",
        receipt.path().display(),
        plan.executable().display()
    );
    for arg in plan.arguments() {
        fields.push_str(&format!("\t\t{arg}\n"));
    }
    fields.push_str("\t}\n");
    match plan.mode() {
        policy::Mode::User { .. } => {
            fields.push_str(&format!("\tdomain = {domain} [100001]\n\tasid = 100001\n"))
        }
        policy::Mode::SystemUser { username, .. } => {
            fields.push_str(&format!("\tdomain = system\n\tusername = {username}\n"))
        }
    }
    fields.push_str("}\n");
    CommandOutput {
        success: true,
        exit_code: Some(0),
        stdout: fields.into_bytes(),
        stderr: Vec::new(),
    }
}
#[test]
fn native_verifier_queries_exact_target_and_refuses_unconfirmed_observations() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let receipt = files.install(&p).unwrap();
    let mut native = NativeLauncher::with_accounts(
        FakeRunner {
            outputs: [Ok(native_output(&p, &receipt))].into(),
            ..Default::default()
        },
        FakeAccounts(None),
    );
    native.verify_loaded(&p, &receipt).unwrap();
    // Fake runner asserts exact target and bounded error behavior, never runs OS commands.
    let mut bads = vec![
        Err("deadline".into()),
        Ok(CommandOutput {
            success: false,
            exit_code: Some(1),
            ..native_output(&p, &receipt)
        }),
        Ok(CommandOutput {
            stderr: b"diagnostic".to_vec(),
            ..native_output(&p, &receipt)
        }),
        Ok(CommandOutput {
            stdout: vec![b'x'; native::OUTPUT_LIMIT + 1],
            ..native_output(&p, &receipt)
        }),
        Ok(CommandOutput {
            stdout: b"unknown format\n".to_vec(),
            ..native_output(&p, &receipt)
        }),
    ];
    for bad in bads.drain(..) {
        let mut native = NativeLauncher::with_accounts(
            FakeRunner {
                outputs: [bad].into(),
                ..Default::default()
            },
            FakeAccounts(None),
        );
        assert!(matches!(
            native.verify_loaded(&p, &receipt),
            Err(LaunchError::NotApplied(_))
        ));
    }
    let mut system = policy::Definition {
        label: "org.example.system".into(),
        mode: policy::Mode::User { uid: 501 },
        executable: "/opt/example/runner".into(),
        arguments: vec![
            "daemon".into(),
            "--input".into(),
            "/var/example/private/input.json".into(),
        ],
        environment: BTreeMap::new(),
        working_directory: "/var/example".into(),
    };
    system.mode = policy::Mode::SystemUser {
        uid: 502,
        username: "worker_user".into(),
    };
    system.environment.clear();
    let system = policy::render(&system).unwrap();
    for account in [
        None,
        Some(Account {
            name: "worker_user".into(),
            uid: 501,
        }),
        Some(Account {
            name: "other".into(),
            uid: 502,
        }),
    ] {
        let mut native = NativeLauncher::with_accounts(
            FakeRunner {
                outputs: [Ok(native_output(&system, &receipt))].into(),
                ..Default::default()
            },
            FakeAccounts(account),
        );
        assert!(native.verify_loaded(&system, &receipt).is_err());
    }
    let mut native = NativeLauncher::with_accounts(
        FakeRunner {
            outputs: [Ok(native_output(&system, &receipt))].into(),
            ..Default::default()
        },
        FakeAccounts(Some(Account {
            name: "worker_user".into(),
            uid: 502,
        })),
    );
    native.verify_loaded(&system, &receipt).unwrap();
}

#[test]
fn native_stop_observes_before_and_after_ack_and_has_no_label_fallback() {
    for scenario in 0..3 {
        let changed = scenario == 1;
        let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
        let p = plan();
        let mut files = DirectoryFiles::open(tmp.path()).unwrap();
        let receipt = files.install(&p).unwrap();
        let mut after = native_output(&p, &receipt);
        if changed {
            after.stdout = String::from_utf8(after.stdout)
                .unwrap()
                .replace("program = /opt/example/runner", "program = /foreign")
                .into_bytes();
        }
        let calls = Rc::new(RefCell::new(Vec::new()));
        let runner = FakeRunner {
            calls: calls.clone(),
            outputs: [
                Ok(native_output(&p, &receipt)),
                Ok(after),
                if scenario == 2 {
                    Err("bootout failed".into())
                } else {
                    Ok(CommandOutput {
                        success: true,
                        exit_code: Some(0),
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    })
                },
            ]
            .into(),
        };
        let mut manager = Manager {
            identity: identity(),
            files,
            launcher: NativeLauncher::with_accounts(runner, FakeAccounts(None)),
            worker: FakeWorker::default(),
        };
        assert_eq!(manager.stop(&p, &receipt, false).is_err(), scenario != 0);
        let commands = calls.borrow();
        assert_eq!(commands[0], ["print", "gui/501/org.example.worker"]);
        assert_eq!(commands[1], commands[0]);
        assert_eq!(commands.len(), if changed { 2 } else { 3 });
        if !changed {
            assert_eq!(
                commands[2],
                ["bootout", "gui/501", receipt.path().to_str().unwrap()]
            );
        }
        assert_eq!(manager.worker.calls.len(), 1);
    }
}
#[test]
fn native_execution_failure_is_uncertain_and_account_lookup_failure_refuses_stop() {
    struct Unavailable;
    impl AccountLookup for Unavailable {
        fn lookup(&mut self, _: &str) -> Result<Option<Account>, String> {
            Err("unavailable".into())
        }
    }
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let receipt = files.install(&p).unwrap();
    let mut output = native_output(&p, &receipt);
    output.stdout = String::from_utf8(output.stdout)
        .unwrap()
        .replace("\tdomain =", "\tusername = worker_user\n\tdomain =")
        .into_bytes();
    let mut native = NativeLauncher::with_accounts(
        FakeRunner {
            outputs: [Ok(output)].into(),
            ..Default::default()
        },
        Unavailable,
    );
    assert!(native.verify_loaded(&p, &receipt).is_err());
    for output in [
        Err("spawn".into()),
        Ok(CommandOutput {
            success: false,
            exit_code: Some(1),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }),
        Ok(CommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: Vec::new(),
            stderr: vec![0; native::OUTPUT_LIMIT + 1],
        }),
        Ok(CommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: vec![0; native::OUTPUT_LIMIT + 1],
            stderr: Vec::new(),
        }),
    ] {
        let mut native = NativeLauncher::new(FakeRunner {
            outputs: [output].into(),
            ..Default::default()
        });
        assert!(matches!(
            native.launchctl(&["bootstrap".into()]),
            Err(LaunchError::OutcomeUnknown(_))
        ));
    }
}

#[test]
fn unparseable_native_observation_prevents_worker_stop() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let receipt = files.install(&p).unwrap();
    let calls = Rc::new(RefCell::new(Vec::new()));
    let runner = FakeRunner {
        calls: calls.clone(),
        outputs: [Ok(CommandOutput {
            success: true,
            exit_code: Some(0),
            stdout: b"unknown layout\n".to_vec(),
            stderr: Vec::new(),
        })]
        .into(),
    };
    let mut manager = Manager {
        identity: identity(),
        files,
        launcher: NativeLauncher::with_accounts(runner, FakeAccounts(None)),
        worker: FakeWorker::default(),
    };
    assert!(manager.stop(&p, &receipt, false).is_err());
    assert!(manager.worker.calls.is_empty());
    assert_eq!(
        calls.borrow().as_slice(),
        [vec![
            "print".to_owned(),
            "gui/501/org.example.worker".to_owned()
        ]]
    );
}

#[test]
fn native_absence_requires_exact_known_exit_and_domain_diagnostic() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let receipt = files.install(&p).unwrap();
    let domain = match p.mode() {
        policy::Mode::User { uid } => format!("user gui: {uid}"),
        policy::Mode::SystemUser { .. } => "system".into(),
    };
    let diagnostic = format!(
        "Bad request.\nCould not find service \"{}\" in domain for {domain}\n",
        p.label()
    );
    for (code, stdout, stderr, expected) in [
        (
            Some(113),
            vec![],
            diagnostic.as_bytes().to_vec(),
            LoadedState::Absent,
        ),
        (
            Some(1),
            vec![],
            diagnostic.as_bytes().to_vec(),
            LoadedState::Unknown,
        ),
        (
            Some(113),
            b"extra".to_vec(),
            diagnostic.as_bytes().to_vec(),
            LoadedState::Unknown,
        ),
        (
            Some(113),
            vec![],
            b"permission denied".to_vec(),
            LoadedState::Unknown,
        ),
        (
            None,
            vec![],
            diagnostic.as_bytes().to_vec(),
            LoadedState::Unknown,
        ),
    ] {
        let mut native = NativeLauncher::with_accounts(
            FakeRunner {
                outputs: [Ok(CommandOutput {
                    success: false,
                    exit_code: code,
                    stdout,
                    stderr,
                })]
                .into(),
                ..Default::default()
            },
            FakeAccounts(None),
        );
        assert_eq!(native.loaded_state(&p, &receipt), expected);
    }
}

#[test]
fn running_identity_is_fresh_and_refuses_nonrunning_or_changed_jobs() {
    assert!(RunningIdentity::observe(0).is_err());
    assert!(RunningIdentity::observe(1).is_err());
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let receipt = files.install(&p).unwrap();
    fn running(plan: &Plan, receipt: &Receipt, pid: u32) -> CommandOutput {
        let mut o = native_output(plan, receipt);
        let text = String::from_utf8(o.stdout).unwrap();
        o.stdout = text
            .replacen(
                " = {\n",
                &format!(" = {{\n\tstate = running\n\tpid = {pid}\n"),
                1,
            )
            .into_bytes();
        o
    }
    let pid = std::process::id();
    let expected = RunningIdentity::observe(pid).unwrap();
    let mut native = NativeLauncher::with_accounts(
        FakeRunner {
            outputs: [
                Ok(running(&p, &receipt, pid)),
                Ok(running(&p, &receipt, pid)),
            ]
            .into(),
            ..Default::default()
        },
        FakeAccounts(None),
    );
    assert_eq!(native.running_identity(&p, &receipt).unwrap(), expected);
    for output in [
        native_output(&p, &receipt),
        running(&p, &receipt, 1),
        CommandOutput {
            success: false,
            ..running(&p, &receipt, pid)
        },
    ] {
        let mut native = NativeLauncher::with_accounts(
            FakeRunner {
                outputs: [Ok(output)].into(),
                ..Default::default()
            },
            FakeAccounts(None),
        );
        assert!(native.running_identity(&p, &receipt).is_err());
    }
    let mut changed = running(&p, &receipt, pid);
    changed.stdout = String::from_utf8(changed.stdout)
        .unwrap()
        .replace("state = running", "state = waiting")
        .into_bytes();
    let mut native = NativeLauncher::with_accounts(
        FakeRunner {
            outputs: [Ok(running(&p, &receipt, pid)), Ok(changed)].into(),
            ..Default::default()
        },
        FakeAccounts(None),
    );
    assert!(native.running_identity(&p, &receipt).is_err());
    let mut stale = expected;
    stale.start.push_str("-reused");
    assert!(stale.verify_live().is_err());
}
#[test]
fn journal_callback_waits_for_durable_stage_directory() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let called = std::cell::Cell::new(false);
    let e = files
        .install_journaled_with_sync(
            &p,
            |_, _| {
                called.set(true);
                Ok(())
            },
            |_| {
                Err(std::io::Error::other(
                    "injected stage directory fsync failure",
                ))
            },
        )
        .unwrap_err();
    assert!(!called.get());
    assert!(e.receipt.is_some());
    assert!(!tmp.path().join(p.filename()).exists());
    let synced = std::cell::Cell::new(false);
    files
        .install_journaled_with_sync(
            &p,
            |receipt, stage| {
                assert!(synced.get());
                assert!(stage.exists());
                assert!(!receipt.path().exists());
                Ok(())
            },
            |path| {
                assert_eq!(path, tmp.path());
                File::open(path)?.sync_all()?;
                synced.set(true);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn failed_native_publication_retains_staged_evidence_after_journal_callback() {
    let tmp = tempfile::tempdir_in(std::fs::canonicalize("/tmp").unwrap()).unwrap();
    let p = plan();
    let mut files = DirectoryFiles::open(tmp.path()).unwrap();
    let stage = std::cell::RefCell::new(None);
    let error = files
        .install_journaled(&p, |receipt, path| {
            *stage.borrow_mut() = Some(path.to_owned());
            std::fs::write(receipt.path(), b"foreign target appeared")?;
            Ok(())
        })
        .unwrap_err();
    assert!(error.receipt.is_some());
    let staged = stage.borrow().clone().unwrap();
    assert_eq!(std::fs::read(&staged).unwrap(), p.plist().as_bytes());
    assert_eq!(
        std::fs::read(tmp.path().join(p.filename())).unwrap(),
        b"foreign target appeared"
    );
}
