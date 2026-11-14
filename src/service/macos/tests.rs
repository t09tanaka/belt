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
}
impl Launcher for FakeLaunch {
    fn launchctl(&mut self, args: &[String]) -> Result<(), LaunchError> {
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
}
impl WorkerControl for FakeWorker {
    fn request(&mut self, identity: &WorkerIdentity, request: WorkerRequest) -> anyhow::Result<()> {
        self.identities.push(identity.clone());
        self.calls.push(request);
        anyhow::ensure!(!self.fail, "worker unavailable");
        if matches!(request, WorkerRequest::Stop { .. }) {
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
        ["bootout", "gui/501/org.example.worker"]
    );
    m.remove(&receipt).unwrap();
    assert!(!receipt.path().exists());
    assert!(m.remove(&receipt).is_err());
}
#[test]
fn proven_start_rejection_rolls_back_but_unknown_preserves_evidence() {
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let other = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
    let tmp = tempfile::tempdir_in("/private/tmp").unwrap();
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
