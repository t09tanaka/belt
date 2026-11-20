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
    pub backend: policy::Backend,
    pub state: PathBuf,
    listener: Option<UnixListener>,
}
#[derive(Clone, Default)]
pub(crate) struct Fake(pub Rc<RefCell<World>>);
impl Launcher for Fake {
    fn backend(&self) -> policy::Backend {
        self.0.borrow().backend
    }
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError> {
        let mut w = self.0.borrow_mut();
        w.calls.push(arguments.to_vec());
        // `systemctl` words are `<scope> <verb> <unit>`: start and stop do
        // what bootstrap and bootout do to the fake world.
        let verb = match (w.backend, arguments[0].as_str()) {
            (policy::Backend::Systemd, _) => match arguments[1].as_str() {
                "start" => "bootstrap",
                "stop" => "bootout",
                _ => "unexpected",
            },
            (_, verb) => verb,
        };
        match verb {
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
        Self::on(policy::Backend::Launchd)
    }
    fn on(backend: policy::Backend) -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let root = dir.path().to_owned();
        let home = root.join("h");
        fs::create_dir(&home).unwrap();
        let exe = root.join("belt");
        fs::write(&exe, b"pretend binary").unwrap();
        let digest = sha256_file(&exe).unwrap();
        let env = BTreeMap::from([
            ("HOME".to_string(), home.to_str().unwrap().to_string()),
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ]);
        let fake = Fake::default();
        fake.0.borrow_mut().state = home.join(".belt");
        fake.0.borrow_mut().backend = backend;
        Host {
            request: InstallRequest {
                membership: None,
                system_user: false,
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
        Paths::resolve_for(&self.env, self.fake.backend()).unwrap()
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
fn a_worker_only_member_can_read_its_group_without_a_coordinator_identity() {
    let h = Host::new();
    h.install().unwrap();
    let paths = h.paths();
    let worker = crate::storage::WorkerStore::open(&paths.state, &h.request.worker_id).unwrap();
    let key = crate::secrets::key::load(&paths.key).unwrap();
    worker
        .observe_group_key(&crate::group::fingerprint(&key))
        .unwrap();
    let spec = crate::service::group::load_spec(&h.env).unwrap();
    let report = crate::service::group::report(&h.env, &spec).unwrap();
    assert_eq!(report["group_id"], h.request.cluster.to_string());
    assert_eq!(report["name"], h.request.worker_id.to_string());
    assert_eq!(report["current"]["number"], 1);
    assert!(!report.to_string().contains(h.request.key_file_text.trim()));
    assert!(!paths.config.join("coordinator.json").exists());
    assert!(!paths.state.join("coordinator.db").exists());
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
        membership: None,
        system_user: false,
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
        membership: None,
        system_user: false,
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

fn with(env: &BTreeMap<String, String>, key: &str, value: &str) -> BTreeMap<String, String> {
    let mut env = env.clone();
    env.insert(key.into(), value.into());
    env
}

#[test]
fn status_reports_the_scope_the_installed_worker_resolves() {
    let h = Host::new();
    let home = PathBuf::from(&h.env["HOME"]);
    let state_home = h.root.join("xdg-state");
    let env = with(&h.env, "XDG_STATE_HOME", state_home.to_str().unwrap());
    install(
        &env,
        &h.request,
        &h.exe,
        h.fake.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    // From the recorded environment, whatever shell asks: a doctor compares
    // this with what its own shell resolves.
    let scope = status(&h.env, h.fake.clone())
        .unwrap()
        .traffic_scope
        .unwrap();
    assert_eq!(
        scope,
        TrafficScope {
            state_dir: state_home.join("task-traffic-controller"),
            config_file: home.join(".config/task-traffic-controller/config.toml"),
        }
    );
    let json = serde_json::to_value(status(&h.env, h.fake.clone()).unwrap()).unwrap();
    assert_eq!(
        json["traffic_scope"]["state_dir"],
        serde_json::json!(state_home.join("task-traffic-controller"))
    );
    assert!(json.get("traffic_hold").is_none());
    // An answer of a helper that does not know the fields still reads.
    let mut old = json.clone();
    old.as_object_mut().unwrap().remove("traffic_scope");
    let old: Status = serde_json::from_value(old).unwrap();
    assert_eq!(old.traffic_scope, None);
    assert_eq!(old.traffic_hold, None);
}

/// Only `install` decides the scope. A caller whose own shell has a relative
/// XDG_STATE_HOME can still stop, drain, resume and look at the worker.
#[test]
fn only_install_checks_the_scope_of_the_calling_shell() {
    let h = Host::new();
    let relative = with(&h.env, "XDG_STATE_HOME", "relative/state");
    let refused = install(
        &relative,
        &h.request,
        &h.exe,
        h.fake.clone(),
        Duration::from_secs(2),
    )
    .unwrap_err()
    .to_string();
    assert!(refused.contains("XDG_STATE_HOME must be"), "{refused}");
    assert!(read_journal(&h.paths()).unwrap().is_none());

    h.install().unwrap();
    h.bind();
    let seen = status(&relative, h.fake.clone()).unwrap();
    assert_eq!(seen.loaded, Some(true));
    // The scope of the worker, not of the caller.
    assert_eq!(
        seen.traffic_scope.unwrap().state_dir,
        PathBuf::from(&h.env["HOME"]).join(".local/state/task-traffic-controller")
    );
    assert!(drain(&relative, h.fake.clone()).unwrap().draining);
    assert!(!resume(&relative, h.fake.clone()).unwrap().draining);
    let stopped = stop(&relative, h.fake.clone(), false, Duration::ZERO).unwrap();
    assert_eq!(stopped.loaded, Some(false));
    assert_eq!(
        start(&relative, h.fake.clone(), Duration::from_secs(2))
            .unwrap()
            .loaded,
        Some(true)
    );
    // The same binary installed again from that shell is still refused.
    assert!(install(
        &relative,
        &h.request,
        &h.exe,
        h.fake.clone(),
        Duration::from_secs(2),
    )
    .is_err());
}

#[test]
fn a_stopped_idle_worker_takes_the_scope_of_a_new_install_and_nothing_else() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let state_home = h.root.join("moved-state");
    let mut moved = with(&h.env, "XDG_STATE_HOME", state_home.to_str().unwrap());
    moved.insert("PATH".into(), "/usr/bin:/bin:/opt/other".into());
    let reinstall = |env: &BTreeMap<String, String>| {
        install(
            env,
            &h.request,
            &h.exe,
            h.fake.clone(),
            Duration::from_secs(2),
        )
        .unwrap()
    };
    let recorded = || read_journal(&h.paths()).unwrap().unwrap().env;
    let input = || -> super::super::worker::WorkerSpec {
        serde_json::from_slice(&fs::read(h.paths().input).unwrap()).unwrap()
    };
    let home_scope = PathBuf::from(&h.env["HOME"]).join(".local/state/task-traffic-controller");

    // Running: it keeps what it started with, and status says so.
    let running = reinstall(&moved);
    assert_eq!(running.traffic_scope.unwrap().state_dir, home_scope);
    assert_eq!(recorded(), h.env);
    assert_eq!(input().env, h.env);

    // Stopped and idle: the scope follows the caller; PATH does not, and
    // the service file is the one that was registered.
    stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    let after = reinstall(&moved);
    assert_eq!(
        after.traffic_scope.unwrap().state_dir,
        state_home.join("task-traffic-controller")
    );
    assert_eq!(after.loaded, Some(true));
    let expected = with(&h.env, "XDG_STATE_HOME", state_home.to_str().unwrap());
    assert_eq!(recorded(), expected);
    // What the worker reads at its start says the same.
    assert_eq!(input().env, expected);
    assert_eq!(h.launchctl_calls(), ["bootstrap", "bootout", "bootstrap"]);

    // And back, when the caller has none any more.
    stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    let back = reinstall(&h.env);
    assert_eq!(back.traffic_scope.unwrap().state_dir, home_scope);
    assert_eq!(recorded(), h.env);
    assert_eq!(input().env, h.env);
}

#[test]
fn the_scope_refresh_touches_only_the_two_scope_variables_of_an_idle_worker() {
    let env = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let recorded = env(&[("HOME", "/h"), ("PATH", "/bin"), ("XDG_STATE_HOME", "/old")]);
    let caller = env(&[
        ("HOME", "/other"),
        ("PATH", "/other/bin"),
        ("LANG", "C"),
        ("XDG_CONFIG_HOME", "/c"),
    ]);
    // An unfinished attempt (or a running worker) keeps the recorded scope.
    assert_eq!(scope_refreshed(&recorded, &caller, false), recorded);
    assert_eq!(
        scope_refreshed(&recorded, &caller, true),
        env(&[("HOME", "/h"), ("PATH", "/bin"), ("XDG_CONFIG_HOME", "/c")])
    );
    let caller = env(&[("XDG_STATE_HOME", "/new")]);
    assert_eq!(
        scope_refreshed(&recorded, &caller, true),
        env(&[("HOME", "/h"), ("PATH", "/bin"), ("XDG_STATE_HOME", "/new")])
    );
}

#[test]
fn status_says_when_the_worker_waits_for_its_previous_scope() {
    use crate::config::traffic::{record_hold, Hold, HOLD_STALE_MS};
    let h = Host::new();
    h.install().unwrap();
    let state = h.paths().state;
    assert_eq!(status_of(&h).traffic_hold, None);
    let now = crate::platform::wall_ms();
    record_hold(&state, &Hold::Draining("old commands".into()), now).unwrap();
    let held = status_of(&h).traffic_hold.unwrap();
    assert_eq!(held.reason, "waiting_for_previous_scope");
    assert_eq!(held.message, "old commands");
    record_hold(&state, &Hold::Unreadable("broken".into()), now).unwrap();
    assert_eq!(
        status_of(&h).traffic_hold.unwrap().reason,
        "previous_scope_unreadable"
    );
    // A record nobody renewed is from a worker that is gone.
    record_hold(
        &state,
        &Hold::Draining("left behind".into()),
        now - HOLD_STALE_MS - 60_000,
    )
    .unwrap();
    assert_eq!(status_of(&h).traffic_hold, None);
    // So is the record of a service that is not loaded.
    record_hold(&state, &Hold::Draining("old commands".into()), now).unwrap();
    h.bind();
    let stopped = stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
    assert_eq!(stopped.traffic_hold, None);
}

/// The Linux backend: the same helper, a systemd user unit and `systemctl`
/// words. The fake world answers them as it answers launchctl.
mod systemd {
    use super::*;

    #[test]
    fn the_host_launcher_is_the_hosts_own_manager() {
        assert_eq!(
            HostLauncher::native().backend(),
            if cfg!(target_os = "linux") {
                policy::Backend::Systemd
            } else {
                policy::Backend::Launchd
            }
        );
    }

    const UNIT_START: usize = 0;

    fn unit_calls(h: &Host) -> Vec<Vec<String>> {
        h.fake.0.borrow().calls.clone()
    }

    #[test]
    fn install_writes_a_user_unit_and_starts_it() {
        let h = Host::on(policy::Backend::Systemd);
        let status = h.install().unwrap();
        assert_eq!(status.loaded, Some(true));
        assert!(status.ready && !status.draining);
        let paths = h.paths();
        assert_eq!(
            paths.services,
            PathBuf::from(&h.env["HOME"]).join(".config/systemd/user")
        );
        let units: Vec<_> = fs::read_dir(&paths.services)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(units.len(), 1);
        let name = units[0].file_name().unwrap().to_str().unwrap().to_owned();
        assert!(name.starts_with("io.github.t09tanaka.belt.worker.") && name.ends_with(".service"));
        let unit = fs::read_to_string(&units[0]).unwrap();
        assert!(unit.contains(&format!(
            "ExecStart=\"{}\" \"worker\" \"--input\" \"{}\"\n",
            paths.executable(&h.digest).display(),
            paths.input.display()
        )));
        assert!(!unit.contains("daemon") && !unit.contains(&"ab".repeat(64)));
        assert_eq!(
            unit_calls(&h)[UNIT_START],
            ["--user", "start", name.as_str()]
        );
        assert_eq!(unit_calls(&h).len(), 1);
        // Installing again changes nothing.
        assert_eq!(h.install().unwrap(), status);
        assert_eq!(unit_calls(&h).len(), 1);
    }

    #[test]
    fn stop_then_start_use_the_unit_name_and_the_same_state_rules() {
        let h = Host::on(policy::Backend::Systemd);
        h.install().unwrap();
        h.bind();
        let status = stop(&h.env, h.fake.clone(), false, Duration::ZERO).unwrap();
        assert_eq!(status.loaded, Some(false));
        control::write(&h.paths().state, Control::cancelling()).unwrap();
        let status = start(&h.env, h.fake.clone(), Duration::from_secs(2)).unwrap();
        assert_eq!(status.loaded, Some(true));
        assert!(status.ready && status.draining && !status.cancel_running);
        let verbs: Vec<_> = unit_calls(&h).iter().map(|c| c[1].clone()).collect();
        assert_eq!(verbs, ["start", "stop", "start"]);
        assert!(unit_calls(&h).iter().all(|c| c[0] == "--user"));
    }

    #[test]
    fn the_unit_directory_follows_xdg_config_home() {
        let h = Host::on(policy::Backend::Systemd);
        let env = with(
            &h.env,
            "XDG_CONFIG_HOME",
            h.root.join("xdg").to_str().unwrap(),
        );
        assert_eq!(
            Paths::resolve_for(&env, policy::Backend::Systemd)
                .unwrap()
                .services,
            h.root.join("xdg/systemd/user")
        );
        assert_eq!(
            Paths::resolve_for(&env, policy::Backend::Launchd)
                .unwrap()
                .services,
            PathBuf::from(&h.env["HOME"]).join("Library/LaunchAgents")
        );
    }
}

#[test]
fn the_journal_is_where_every_service_manager_keeps_it() {
    let env = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let e = env(&[("HOME", "/h")]);
    let want = PathBuf::from("/h/.config/belt/worker-service.json");
    assert_eq!(Paths::journal_path(&e).unwrap(), want);
    for backend in [policy::Backend::Launchd, policy::Backend::Systemd] {
        assert_eq!(Paths::resolve_for(&e, backend).unwrap().journal, want);
    }
    let x = env(&[("HOME", "/h"), ("XDG_CONFIG_HOME", "/x")]);
    let want = PathBuf::from("/x/belt/worker-service.json");
    assert_eq!(Paths::journal_path(&x).unwrap(), want);
    assert_eq!(
        Paths::resolve_for(&x, policy::Backend::Systemd)
            .unwrap()
            .journal,
        want
    );
    // An unusable home is an error, not "no journal".
    assert!(Paths::journal_path(&env(&[])).is_err());
    assert!(Paths::journal_path(&env(&[("HOME", "relative")])).is_err());
}

#[test]
fn a_system_service_installs_stops_restores_and_upgrades_without_replacing_the_admin_file() {
    for backend in [policy::Backend::Launchd, policy::Backend::Systemd] {
        let mut host = Host::on(backend);
        host.request.system_user = true;
        let system_dir = host.root.join("system");
        fs::create_dir(&system_dir).unwrap();
        system_user::with_fixture_directory(&system_dir, || {
            let error = host.install().unwrap_err(); // the administrator hasn't registered it
            assert!(!host.fake.0.borrow().loaded);
            if euid() == 0 {
                assert!(error.to_string().contains("non-root"));
                return;
            }
            let paths = host.paths();
            let journal = require_journal(&paths).unwrap();
            let system_plan = plan(&paths, &journal).unwrap();
            let file = system_user::publish_definition(&system_dir, &system_plan, euid()).unwrap();
            let bytes = fs::read(&file).unwrap();
            let account = journal.system_user.as_deref().unwrap();
            let version = fs::canonicalize(paths.executable(&host.digest)).unwrap();
            // Automatic startup is legitimate as soon as the administrator
            // publishes the first definition, before the parent retries.
            assert_eq!(
                system_user::check_boot(&paths, account, &paths.bridge_binary()).unwrap(),
                version
            );
            assert!(system_user::check_boot(&paths, "another-account", &version).is_err());
            assert!(system_user::check_boot(&paths, account, &host.exe).is_err());
            let up = host.install().unwrap();
            assert!(up.system_user && up.ready && up.loaded == Some(true));
            let first = require_journal(&paths).unwrap().receipt.unwrap();
            host.bind();
            let stopped = stop(&host.env, host.fake.clone(), false, Duration::ZERO).unwrap();
            assert_eq!(stopped.loaded, Some(false));
            let started = start(&host.env, host.fake.clone(), Duration::ZERO).unwrap();
            assert!(started.ready && started.system_user);
            let new_binary = host.root.join("new");
            fs::write(&new_binary, b"another build").unwrap();
            let update = upgrade::SwitchRequest {
                schema: 1,
                cluster: host.request.cluster.clone(),
                worker_id: host.request.worker_id.clone(),
                binary_sha256: sha256_file(&new_binary).unwrap(),
            };
            let options = Options {
                cancel_running: false,
                all_submitters: false,
                submitter: None,
                wait: Duration::ZERO,
                ready_timeout: Duration::ZERO,
            };
            let switched = upgrade::upgrade(
                &host.env,
                &update,
                &new_binary,
                host.fake.clone(),
                options,
                &mut |step| {
                    if step == upgrade::Step::Input {
                        assert!(system_user::check_boot(&paths, account, &version)
                            .unwrap_err()
                            .to_string()
                            .contains("switch has not published"));
                    }
                    Ok(())
                },
            )
            .unwrap();
            let new = fs::canonicalize(paths.executable(&update.binary_sha256)).unwrap();
            assert_eq!(
                system_user::check_boot(&paths, account, &paths.bridge_binary()).unwrap(),
                new
            );
            assert!(system_user::check_boot(&paths, account, &version).is_err());
            assert!(switched.system_user && switched.ready);
            assert_eq!(switched.binary_sha256, update.binary_sha256);
            let installed = upgrade::commit(&host.env, &update, host.fake.clone()).unwrap();
            assert!(installed.switching_from.is_none() && installed.ready);
            assert_eq!(require_journal(&paths).unwrap().receipt.unwrap(), first);
            assert_eq!(fs::read(file).unwrap(), bytes);
            assert!(!paths.services.exists());
            let (_, _, env, _) = service_view(&host.env, host.fake.clone()).unwrap();
            assert_eq!(&env, system_plan.environment());
            for call in &host.fake.0.borrow().calls {
                match backend {
                    policy::Backend::Launchd => assert_eq!(call[1], "system"),
                    policy::Backend::Systemd => assert_eq!(call[0], "--system"),
                }
            }
        });
    }
}

#[test]
fn membership_moves_an_initialized_idle_member_without_adding_a_worker() {
    use crate::service::init::{initialize, ContextInput};
    use crate::storage::{CoordinatorStore, WorkerStore};
    struct Unobserved;
    impl Launcher for Unobserved {
        fn launchctl(&mut self, _: &[String]) -> Result<(), LaunchError> {
            panic!("no service may start")
        }
        fn verify_loaded(&mut self, _: &policy::Plan, _: &Receipt) -> Result<(), LaunchError> {
            Err(LaunchError::NotApplied("unobserved".into()))
        }
    }
    let h = Host::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(initialize(
            ContextInput {
                env: h.env.clone(),
                executable: h.exe.clone()
            },
            &mut Unobserved
        ))
        .is_err());
    let input = h.paths().config.join("coordinator.json");
    let spec = super::super::bootstrap::read_input(&input).unwrap();
    let store = CoordinatorStore::open_read_only(&spec.state_dir).unwrap();
    let original_id = store.coordinator_id().clone();
    let worker = WorkerStore::open(&spec.worker_state_dir, &spec.worker_id).unwrap();
    let old_member = CoordinatorId::generate();
    worker
        .for_member(&old_member)
        .pin_member("old-member", 1)
        .unwrap();
    worker.observe_group_key("old").unwrap();
    worker
        .set_next_group_key(&crate::storage::KeyGeneration {
            generation: 2,
            fingerprint: "old-next".into(),
        })
        .unwrap();
    fs::write(
        crate::group::next_key_path(&spec.cluster_key_path),
        b"old key",
    )
    .unwrap();
    let journal_path = h.paths().journal;
    let mut request = h.request;
    request.worker_id = spec.worker_id.clone();
    request.membership = Some(Membership {
        caller_name: WorkerId::parse("caller").unwrap(),
        caller_id: CoordinatorId::generate(),
        move_group: false,
        install: false,
    });
    let before = fs::read(&input).unwrap();
    let error = install(&h.env, &request, &h.exe, h.fake.clone(), Duration::ZERO).unwrap_err();
    assert!(error.to_string().contains(store.cluster_id().as_str()));
    assert_eq!(fs::read(&input).unwrap(), before);
    assert_eq!(
        worker.members().unwrap(),
        vec![("old-member".into(), old_member.to_string())]
    );
    request.membership.as_mut().unwrap().move_group = true;
    let status = install(&h.env, &request, &h.exe, h.fake.clone(), Duration::ZERO).unwrap();
    assert_eq!(status.coordinator_id, Some(original_id.clone()));
    assert_eq!(status.cluster, request.cluster);
    assert!(!status.ready && status.loaded == Some(false));
    assert!(h.fake.0.borrow().calls.is_empty());
    assert!(!journal_path.exists());
    let changed = super::super::bootstrap::read_input(&input).unwrap();
    assert_eq!(changed.expected_cluster, Some(request.cluster.clone()));
    let changed_store = CoordinatorStore::open_read_only(&changed.state_dir).unwrap();
    assert_eq!(changed_store.coordinator_id(), &original_id);
    assert_eq!(changed_store.cluster_id(), &request.cluster);
    assert_eq!(
        worker.members().unwrap(),
        vec![(
            "caller".into(),
            request.membership.as_ref().unwrap().caller_id.to_string()
        )]
    );
    let (current, next) = worker.group_keys().unwrap();
    assert_eq!(current.unwrap().generation, 1);
    assert!(next.is_none());
    assert!(!crate::group::next_key_path(&spec.cluster_key_path).exists());
    assert_eq!(
        fs::read_to_string(&spec.cluster_key_path).unwrap(),
        request.key_file_text
    );
}

fn put_attempt(db: &rusqlite::Connection, member: &CoordinatorId, _n: u32, state: &str) {
    db.execute(
        "INSERT INTO worker_attempt (coordinator_id, attempt_id, run_id, task_id, generation, payload_hash, coordinator_epoch, spec_json, state, termination_json, receipts_json, accepted_at_ms, terminal_at_ms)
         VALUES (?1, ?2, ?6, ?7, 1, 'sha256:0000000000000000000000000000000000000000000000000000000000000000', 1, '{}', ?3, ?4, '{}', 1, ?5)",
        rusqlite::params![
            member.as_str(),
            crate::ids::AttemptId::generate().to_string(),
            state,
            (state == "terminal").then_some("{}"),
            (state == "terminal").then_some(2),
            crate::ids::RunId::generate().to_string(),
            crate::ids::TaskId::generate().to_string()
        ],
    )
    .unwrap();
}

#[test]
fn stop_refuses_other_members_live_work_unless_all_submitters() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let state = h.paths().state;
    let store = crate::storage::WorkerStore::open(&state, &h.request.worker_id).unwrap();
    let (alice, bob, me) = (
        CoordinatorId::generate(),
        CoordinatorId::generate(),
        CoordinatorId::generate(),
    );
    store.for_member(&alice).pin_member("alice", 1).unwrap();
    let db = rusqlite::Connection::open(state.join("worker.db")).unwrap();
    put_attempt(&db, &alice, 1, "active");
    put_attempt(&db, &alice, 2, "active");
    put_attempt(&db, &alice, 3, "terminal");
    put_attempt(&db, &bob, 4, "active");
    put_attempt(&db, &me, 5, "active");
    let calls = h.launchctl_calls();
    let mut options = Options {
        cancel_running: false,
        all_submitters: false,
        submitter: Some(me.clone()),
        wait: Duration::ZERO,
        ready_timeout: Duration::ZERO,
    };
    for cancel_running in [false, true] {
        options.cancel_running = cancel_running;
        let error = handle(
            Action::Stop,
            &h.env,
            &[],
            &h.exe,
            h.fake.clone(),
            options.clone(),
        )
        .unwrap_err();
        let error = format!("{error:#}");
        assert!(
            error.contains("3 live attempt(s) of other members"),
            "{error}"
        );
        assert!(error.contains("alice (2)"), "{error}");
        assert!(error.contains(&format!("{} (1)", bob.as_str())), "{error}");
        assert!(error.contains("--all-submitters"), "{error}");
        assert!(!error.contains(me.as_str()));
        // Nothing changed: no drain marker, no service change.
        assert_eq!(control::read(&state).unwrap(), Control::default());
        assert_eq!(h.launchctl_calls(), calls);
    }
    // Only the caller's own work: not refused by the guard.
    db.execute(
        "UPDATE worker_attempt SET state = 'terminal', termination_json = '{}', terminal_at_ms = 2 WHERE coordinator_id <> ?1",
        [me.as_str()],
    )
    .unwrap();
    refuse_other_submitters(
        &state,
        &Submitters {
            all: false,
            caller: Some(me.clone()),
        },
    )
    .unwrap();
    // Without a named caller, the caller's attempt is another member's too.
    assert!(refuse_other_submitters(
        &state,
        &Submitters {
            all: false,
            caller: None
        }
    )
    .is_err());
}

#[test]
fn stop_with_all_submitters_goes_ahead_and_waits_for_everyone() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let state = h.paths().state;
    crate::storage::WorkerStore::open(&state, &h.request.worker_id).unwrap();
    let other = CoordinatorId::generate();
    let db = rusqlite::Connection::open(state.join("worker.db")).unwrap();
    put_attempt(&db, &other, 1, "active");
    let finisher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        db.execute(
            "UPDATE worker_attempt SET state = 'terminal', termination_json = '{}', terminal_at_ms = 2",
            [],
        )
        .unwrap();
    });
    let options = Options {
        cancel_running: false,
        all_submitters: true,
        submitter: None,
        wait: Duration::from_secs(10),
        ready_timeout: Duration::ZERO,
    };
    let status = handle(Action::Stop, &h.env, &[], &h.exe, h.fake.clone(), options).unwrap();
    finisher.join().unwrap();
    assert_eq!(status.loaded, Some(false));
    assert_eq!(h.launchctl_calls(), ["bootstrap", "bootout"]);
}

#[test]
fn an_unreadable_worker_database_refuses_a_stop_and_no_database_does_not() {
    let h = Host::new();
    h.install().unwrap();
    h.bind();
    let state = h.paths().state;
    let guard = Submitters {
        all: false,
        caller: None,
    };
    refuse_other_submitters(&state, &guard).unwrap();
    fs::write(state.join("worker.db"), b"not a database").unwrap();
    let error = refuse_other_submitters(&state, &guard).unwrap_err();
    assert!(
        format!("{error:#}").contains("could not read which members"),
        "{error:#}"
    );
}
