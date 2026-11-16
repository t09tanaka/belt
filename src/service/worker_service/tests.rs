//! A fake launcher and a private temporary HOME; nothing here reaches launchctl.
use super::*;
use crate::service::macos::{LaunchError, Receipt};
use std::cell::RefCell;
use std::os::unix::net::UnixListener;
use std::rc::Rc;

#[derive(Default)]
pub(crate) struct World {
    pub loaded: bool,
    pub unknown: bool,
    pub fail_start: bool,
    pub calls: Vec<Vec<String>>,
    /// The operator marker as the service was started (before the agent runs).
    pub control_at_bootstrap: Vec<Control>,
    pub state: PathBuf,
    listener: Option<UnixListener>,
}
#[derive(Clone, Default)]
pub(crate) struct Fake(pub Rc<RefCell<World>>);
impl Launcher for Fake {
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError> {
        let mut w = self.0.borrow_mut();
        w.calls.push(arguments.to_vec());
        match arguments[0].as_str() {
            "bootstrap" => {
                if w.fail_start {
                    return Err(LaunchError::NotApplied("rejected".into()));
                }
                let seen = Control::effective(&w.state);
                w.control_at_bootstrap.push(seen);
                w.loaded = true;
                let socket = w.state.join("agent.sock");
                let _ = fs::remove_file(&socket);
                w.listener = Some(UnixListener::bind(&socket).unwrap());
                Ok(())
            }
            "bootout" => {
                w.loaded = false;
                w.listener = None;
                let _ = fs::remove_file(w.state.join("agent.sock"));
                Ok(())
            }
            _ => Err(LaunchError::NotApplied("unexpected".into())),
        }
    }
    fn verify_loaded(&mut self, _: &policy::Plan, _: &Receipt) -> Result<(), LaunchError> {
        if self.0.borrow().loaded {
            Ok(())
        } else {
            Err(LaunchError::NotApplied("not loaded".into()))
        }
    }
    fn loaded_state(&mut self, _: &policy::Plan, _: &Receipt) -> LoadedState {
        let w = self.0.borrow();
        if w.unknown {
            LoadedState::Unknown
        } else if w.loaded {
            LoadedState::Match
        } else {
            LoadedState::Absent
        }
    }
}

struct Host {
    _dir: tempfile::TempDir,
    root: PathBuf,
    env: BTreeMap<String, String>,
    exe: PathBuf,
    digest: String,
    fake: Fake,
    request: InstallRequest,
}
impl Host {
    fn new() -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path().to_owned();
        let home = root.join("h");
        fs::create_dir(&home).unwrap();
        let exe = root.join("rtr");
        fs::write(&exe, b"pretend binary").unwrap();
        let digest = sha256_file(&exe).unwrap();
        let env = BTreeMap::from([
            ("HOME".to_string(), home.to_str().unwrap().to_string()),
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ]);
        let fake = Fake::default();
        fake.0.borrow_mut().state = home.join(".remote-task-runner");
        Host {
            request: InstallRequest {
                schema: 1,
                cluster: ClusterId::generate(),
                worker_id: WorkerId::parse("mini-a").unwrap(),
                key_file_text: format!("{}\n", "ab".repeat(64)),
                binary_sha256: digest.clone(),
            },
            _dir: dir,
            root,
            env,
            exe,
            digest,
            fake,
        }
    }
    fn paths(&self) -> Paths {
        Paths::resolve(&self.env).unwrap()
    }
    fn install(&self) -> anyhow::Result<Status> {
        install(
            &self.env,
            &self.request,
            &self.exe,
            self.fake.clone(),
            Duration::from_secs(2),
        )
    }
    /// What the running agent writes on its first start.
    fn bind(&self) {
        let bytes = serde_json::to_vec(&(
            1,
            &self.request.cluster,
            &self.request.worker_id,
            "key-digest",
        ))
        .unwrap();
        let path = self.paths().state.join("worker-binding.json");
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn launchctl_calls(&self) -> Vec<String> {
        self.fake
            .0
            .borrow()
            .calls
            .iter()
            .map(|c| c[0].clone())
            .collect()
    }
}

#[test]
fn install_places_the_binary_key_input_and_starts_the_service() {
    let h = Host::new();
    let status = h.install().unwrap();
    assert_eq!(status.loaded, Some(true));
    assert!(status.ready && !status.draining);
    assert_eq!(status.live_attempts, Some(0));
    let paths = h.paths();
    let binary = paths.executable(&h.digest);
    assert_eq!(fs::read(&binary).unwrap(), b"pretend binary");
    assert_eq!(
        fs::metadata(&binary).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert_eq!(
        fs::metadata(&paths.key).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let input = crate::service::worker::read_input(&paths.input).unwrap();
    assert_eq!(input.worker_id, h.request.worker_id);
    assert_eq!(input.expected_cluster, h.request.cluster);
    assert_eq!(input.executable, binary);
    assert_eq!(input.state_dir, paths.state);
    assert_eq!(h.launchctl_calls(), ["bootstrap"]);
    let plist = fs::read_dir(&paths.services)
        .unwrap()
        .map(|e| fs::read_to_string(e.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(plist.len(), 1);
    // The worker foreground role with a retained input, never the combined daemon.
    assert!(plist[0].contains("<string>worker</string>") && plist[0].contains("--input"));
    assert!(!plist[0].contains("daemon"));
    assert!(!plist[0].contains("ab".repeat(64).as_str()));
}

#[test]
fn install_is_idempotent_and_starts_once() {
    let h = Host::new();
    let first = h.install().unwrap();
    assert_eq!(h.install().unwrap(), first);
    assert_eq!(h.launchctl_calls(), ["bootstrap"]);
}

#[test]
fn a_rejected_start_keeps_the_receipt_and_a_retry_finishes() {
    let h = Host::new();
    h.fake.0.borrow_mut().fail_start = true;
    assert!(h.install().is_err());
    assert!(!h.fake.0.borrow().loaded);
    h.fake.0.borrow_mut().fail_start = false;
    assert_eq!(h.install().unwrap().loaded, Some(true));
    assert_eq!(fs::read_dir(h.paths().services).unwrap().count(), 1);
}

#[test]
fn install_refuses_a_wrong_digest_a_changed_key_and_another_identity() {
    let mut h = Host::new();
    h.request.binary_sha256 = "0".repeat(64);
    assert!(h.install().unwrap_err().to_string().contains("not the one"));
    assert!(!h.paths().journal.exists());
    h.request.binary_sha256 = h.digest.clone();
    h.install().unwrap();
    let mut changed_key = Host::new();
    changed_key.env = h.env.clone();
    changed_key.exe = h.exe.clone();
    changed_key.request = InstallRequest {
        key_file_text: format!("{}\n", "cd".repeat(64)),
        cluster: h.request.cluster.clone(),
        worker_id: h.request.worker_id.clone(),
        schema: 1,
        binary_sha256: h.digest.clone(),
    };
    assert!(changed_key
        .install()
        .unwrap_err()
        .to_string()
        .contains("refusing to replace"));
    h.request.worker_id = WorkerId::parse("other").unwrap();
    assert!(h.install().is_err());
}

#[test]
fn install_refuses_a_weak_or_unsupported_request() {
    let mut h = Host::new();
    h.request.schema = 2;
    assert!(h.install().unwrap_err().to_string().contains("schema"));
    h.request.schema = 1;
    h.request.key_file_text = "ab\n".into();
    assert!(h.install().is_err());
}

#[test]
fn drain_and_resume_change_only_the_marker() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let drained = drain(&h.env, h.fake.clone()).unwrap();
    assert!(drained.draining && !drained.cancel_running);
    assert_eq!(
        control::read(&h.paths().state).unwrap(),
        Control::draining()
    );
    let resumed = resume(&h.env, h.fake.clone()).unwrap();
    assert!(!resumed.draining);
    assert_eq!(h.launchctl_calls(), ["bootstrap"]);
}

#[test]
fn a_foreign_binding_refuses_every_request() {
    let h = Host::new();
    h.install().unwrap();
    let other = serde_json::to_vec(&(1, ClusterId::generate(), &h.request.worker_id, "k")).unwrap();
    let path = h.paths().state.join("worker-binding.json");
    fs::write(&path, other).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(drain(&h.env, h.fake.clone()).is_err());
    assert!(!control::path(&h.paths().state).exists());
    assert!(stop(&h.env, h.fake.clone(), false, Duration::ZERO).is_err());
    assert!(h.fake.0.borrow().loaded);
}

#[test]
fn drain_without_an_install_is_refused() {
    let h = Host::new();
    assert!(drain(&h.env, h.fake.clone())
        .unwrap_err()
        .to_string()
        .contains("no worker is installed"));
}

#[test]
fn stop_drains_boots_the_service_out_and_resets_the_marker() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let status = stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    assert_eq!(status.loaded, Some(false));
    assert!(!status.ready);
    assert_eq!(h.launchctl_calls(), ["bootstrap", "bootout"]);
    assert_eq!(control::read(&h.paths().state).unwrap(), Control::default());
    // Already stopped: reported, and launchctl is not touched again.
    stop(&h.env, h.fake.clone(), true, Duration::ZERO).unwrap();
    assert_eq!(h.launchctl_calls(), ["bootstrap", "bootout"]);
}

#[test]
fn an_unknown_loaded_state_stops_nothing_and_starts_nothing() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    h.fake.0.borrow_mut().unknown = true;
    assert!(stop(&h.env, h.fake.clone(), false, Duration::ZERO).is_err());
    assert!(start(&h.env, h.fake.clone(), Duration::ZERO).is_err());
    assert_eq!(h.launchctl_calls(), ["bootstrap"]);
}

#[test]
fn start_boots_a_stopped_service_and_drops_a_stale_cancel_request() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    control::write(&h.paths().state, Control::cancelling()).unwrap();
    let status = start(&h.env, h.fake.clone(), Duration::from_secs(2)).unwrap();
    assert_eq!(status.loaded, Some(true));
    assert!(status.ready);
    assert!(status.draining && !status.cancel_running);
    assert_eq!(h.launchctl_calls(), ["bootstrap", "bootout", "bootstrap"]);
    // Starting a running service only reports it.
    start(&h.env, h.fake.clone(), Duration::ZERO).unwrap();
    assert_eq!(h.launchctl_calls().len(), 3);
}

#[test]
fn a_changed_service_file_blocks_start() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    let plist = fs::read_dir(h.paths().services)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    fs::write(&plist, b"foreign").unwrap();
    assert!(start(&h.env, h.fake.clone(), Duration::ZERO).is_err());
    assert_eq!(h.launchctl_calls(), ["bootstrap", "bootout"]);
}

#[test]
fn stop_requests_ask_for_cancellation_only_when_told() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let state = h.paths().state;
    let mut control_plane = marker(&h.paths(), Duration::ZERO);
    let id = identity(&read_journal(&h.paths()).unwrap().unwrap());
    control_plane
        .request(
            &id,
            WorkerRequest::Stop {
                cancel_running: true,
            },
        )
        .unwrap();
    assert_eq!(control::read(&state).unwrap(), Control::cancelling());
    control_plane
        .request(
            &id,
            WorkerRequest::Stop {
                cancel_running: false,
            },
        )
        .unwrap();
    assert_eq!(control::read(&state).unwrap(), Control::draining());
}

#[test]
fn status_reports_a_stopped_service_without_claiming_readiness() {
    let h = Host::new();
    h.install().unwrap();
    let up = status(&h.env, h.fake.clone()).unwrap();
    assert!(up.ready && up.loaded == Some(true));
    h.fake.0.borrow_mut().unknown = true;
    let unknown = status(&h.env, h.fake.clone()).unwrap();
    assert_eq!(unknown.loaded, None);
    let _ = &h.root;
}

#[test]
fn waiting_for_idle_never_treats_a_failed_read_as_idle() {
    let interval = Duration::from_millis(1);
    assert!(wait_until_idle(|| Ok(0), Duration::ZERO, interval).is_ok());
    let mut remaining = 3;
    assert!(wait_until_idle(
        || {
            remaining = u32::saturating_sub(remaining, 1);
            Ok(remaining)
        },
        Duration::from_secs(5),
        interval
    )
    .is_ok());
    assert!(wait_until_idle(
        || anyhow::bail!("db unreadable"),
        Duration::from_secs(5),
        interval
    )
    .unwrap_err()
    .to_string()
    .contains("db unreadable"));
    let timeout = wait_until_idle(|| Ok(2), Duration::ZERO, interval).unwrap_err();
    assert!(timeout.to_string().contains("2 attempt(s) are still live"));
}

#[test]
fn live_attempts_is_zero_without_a_database() {
    let dir = crate::test_tmp::tempdir().unwrap();
    assert_eq!(live_attempts(dir.path()).unwrap(), 0);
}

#[test]
fn hex_decoding_accepts_only_lower_case_pairs() {
    assert_eq!(hex_decode("00ff1a"), Some(vec![0, 255, 26]));
    assert_eq!(hex_decode(""), None);
    assert_eq!(hex_decode("abc"), None);
    assert_eq!(hex_decode("AB"), None);
    assert_eq!(hex_decode("zz"), None);
}

#[test]
fn install_gives_the_parent_one_absolute_path_that_survives_a_version_change() {
    let h = Host::new();
    let status = h.install().unwrap();
    let paths = h.paths();
    let bridge = status.bridge_binary.clone().expect("bridge path");
    assert_eq!(Path::new(&bridge), paths.bridge_binary());
    assert!(Path::new(&bridge).is_absolute());
    assert!(!bridge.contains(&h.digest), "the path names no version");
    // It resolves to the installed binary.
    assert_eq!(fs::read(&bridge).unwrap(), b"pretend binary");
    assert_eq!(
        fs::read_link(paths.current_link()).unwrap(),
        Path::new(&h.digest)
    );
    // Re-running keeps it; status reports it too.
    assert_eq!(h.install().unwrap().bridge_binary, Some(bridge.clone()));
    assert_eq!(
        status_of(&h).bridge_binary,
        Some(bridge),
        "status carries the path"
    );
    // A link naming another version is never moved by an install.
    fs::remove_file(paths.current_link()).unwrap();
    std::os::unix::fs::symlink("0".repeat(64), paths.current_link()).unwrap();
    assert!(h
        .install()
        .unwrap_err()
        .to_string()
        .contains("another version"));
}

fn status_of(h: &Host) -> Status {
    status(&h.env, h.fake.clone()).unwrap()
}

#[test]
fn a_rerun_with_a_different_shell_environment_still_succeeds() {
    let h = Host::new();
    h.install().unwrap();
    let mut other = h.env.clone();
    other.insert("LANG".into(), "ja_JP.UTF-8".into());
    other.insert("PATH".into(), "/usr/bin:/bin:/opt/other".into());
    let again = install(
        &other,
        &h.request,
        &h.exe,
        h.fake.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    assert!(again.ready);
    // The installed environment is the first one; the plan did not change.
    let journal = read_journal(&h.paths()).unwrap().unwrap();
    assert_eq!(journal.env.get("PATH").unwrap(), "/usr/bin:/bin");
    assert!(!journal.env.contains_key("LANG"));
    assert_eq!(h.launchctl_calls(), ["bootstrap"]);
}

#[test]
fn an_install_that_never_registered_the_service_can_be_replaced_by_a_rerun() {
    let h = Host::new();
    // The services directory cannot be created: the run stops after the key,
    // the input and the journal, before any receipt.
    fs::create_dir_all(h.paths().services.parent().unwrap()).unwrap();
    fs::write(h.paths().services, b"in the way").unwrap();
    assert!(h.install().is_err());
    assert!(read_journal(&h.paths()).unwrap().unwrap().receipt.is_none());
    fs::remove_file(h.paths().services).unwrap();
    let mut changed = Host::new();
    changed.env = h.env.clone();
    changed.exe = h.exe.clone();
    changed.request = InstallRequest {
        schema: 1,
        cluster: h.request.cluster.clone(),
        worker_id: h.request.worker_id.clone(),
        key_file_text: format!("{}\n", "cd".repeat(64)),
        binary_sha256: h.digest.clone(),
    };
    changed.env.insert("LANG".into(), "C".into());
    changed.fake = h.fake.clone();
    let status = changed.install().unwrap();
    assert!(status.ready);
    assert_eq!(
        fs::read_to_string(h.paths().key).unwrap(),
        changed.request.key_file_text
    );
    assert_eq!(
        read_journal(&h.paths()).unwrap().unwrap().env.get("LANG"),
        Some(&"C".to_string())
    );
}

#[test]
fn publishing_replaces_only_when_told_to() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let path = dir.path().join("f");
    publish_exact(&path, b"one", 0o600, false).unwrap();
    publish_exact(&path, b"one", 0o600, false).unwrap();
    assert!(publish_exact(&path, b"two", 0o600, false)
        .unwrap_err()
        .to_string()
        .contains("refusing to replace"));
    publish_exact(&path, b"two", 0o600, true).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"two");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn start_clears_a_stale_cancel_request_before_the_service_runs() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    control::write(&h.paths().state, Control::cancelling()).unwrap();
    start(&h.env, h.fake.clone(), Duration::from_secs(2)).unwrap();
    let seen = h.fake.0.borrow().control_at_bootstrap.clone();
    assert_eq!(seen.len(), 2);
    assert!(
        !seen[1].cancel_running,
        "the restarted agent must never see the old cancel request"
    );
}

#[test]
fn an_unreadable_state_directory_is_an_error_not_zero_attempts() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let file = dir.path().join("not-a-directory");
    fs::write(&file, b"x").unwrap();
    assert!(live_attempts(&file).is_err());
}

#[test]
fn idle_must_hold_across_one_more_look() {
    let interval = Duration::from_millis(1);
    // Zero, then an acceptance that was still committing, then zero twice.
    let mut calls = 0;
    wait_until_idle(
        || {
            calls += 1;
            Ok(if calls == 2 { 1 } else { 0 })
        },
        Duration::from_secs(5),
        interval,
    )
    .unwrap();
    assert_eq!(calls, 4);
    let mut calls = 0;
    wait_until_idle(
        || {
            calls += 1;
            Ok(0)
        },
        Duration::ZERO,
        interval,
    )
    .unwrap();
    assert_eq!(calls, 2);
}
