use super::*;
use crate::service::{
    bootstrap::Spec,
    group::join,
    init::{initialize_named, ContextInput},
    workers::{Remote, RemoteOutput},
};
use crate::{
    protocol::ssh::SshTarget,
    storage::{CoordinatorStore, WorkerStore},
};

fn initialized(name: &str) -> Host {
    struct Unknown;
    impl Launcher for Unknown {
        fn launchctl(&mut self, _: &[String]) -> Result<(), LaunchError> {
            panic!("no native services")
        }
        fn verify_loaded(&mut self, _: &policy::Plan, _: &Receipt) -> Result<(), LaunchError> {
            Err(LaunchError::NotApplied("unobserved".into()))
        }
    }
    let host = Host::new();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = rt
        .block_on(initialize_named(
            ContextInput {
                env: host.env.clone(),
                executable: host.exe.clone(),
            },
            Some(name),
            &mut Unknown,
        ))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("loaded service identity unknown"),
        "{error}"
    );
    let spec = spec(&host);
    WorkerStore::open(&spec.worker_state_dir, &spec.worker_id).unwrap();
    host
}
fn worker_only(host: &Host) {
    host.fake.0.borrow_mut().omit_socket = true;
    install(
        &host.env,
        &host.request,
        &host.exe,
        host.fake.clone(),
        Duration::ZERO,
    )
    .unwrap();
}
fn spec(h: &Host) -> Spec {
    crate::service::context::runtime_spec(&h.env).unwrap()
}

// The same actual install helper and fake launcher as the workers add tests.
struct Pair<'a> {
    a: &'a Host,
    b: &'a Host,
    fail_second: bool,
    calls: usize,
}
impl Remote for Pair<'_> {
    fn bridge_hello(
        &mut self,
        _: &SshTarget,
        _: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        panic!("initialized members use the shared resident")
    }
    fn run(
        &mut self,
        target: &SshTarget,
        shell: &str,
        stdin: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        self.calls += 1;
        if shell == crate::service::group::join::HELPER {
            assert_eq!(target, &SshTarget::parse("user@a").unwrap());
            let request = serde_json::from_slice(stdin).unwrap();
            let result = join::accept(self, &self.a.env.clone(), &self.a.exe.clone(), request);
            let (code, reply) = match result {
                Ok(name) => (0, serde_json::json!({"ok":true,"result":{"name":name}})),
                Err(error) => (1, serde_json::json!({"ok":false,"error":error})),
            };
            return Ok(RemoteOutput {
                code: Some(code),
                stdout: serde_json::to_vec(&reply).unwrap(),
                stderr: vec![],
            });
        }
        assert!(shell.contains("'worker-service' 'install'"));
        let request: InstallRequest = serde_json::from_slice(stdin).unwrap();
        assert!(!request.membership.as_ref().unwrap().install);
        assert!(!shell.contains(request.key_file_text.trim()));
        let host = if target == &SshTarget::parse("user@b").unwrap() {
            self.b
        } else {
            assert_eq!(target, &SshTarget::parse("user@a").unwrap());
            if self.fail_second {
                return Err("injected step two failure".into());
            }
            self.a
        };
        let (code, reply) = match install(
            &host.env,
            &request,
            &host.exe,
            host.fake.clone(),
            Duration::ZERO,
        ) {
            Ok(status) => (0, serde_json::json!({"ok":true,"status":status})),
            Err(error) => (1, serde_json::json!({"ok":false,"error":error.to_string()})),
        };
        Ok(RemoteOutput {
            code: Some(code),
            stdout: serde_json::to_vec(&reply).unwrap(),
            stderr: vec![],
        })
    }
}
fn run(pair: &mut Pair<'_>, moving: bool) -> Result<serde_json::Value, crate::error::ApiError> {
    join::join(
        pair,
        &pair.b.env.clone(),
        &pair.b.exe.clone(),
        "user@a",
        "user@b",
        moving,
    )
}
fn snapshot(h: &Host) -> Vec<Vec<u8>> {
    let s = spec(h);
    let mut paths = vec![
        s.cluster_key_path,
        h.paths().config.join("coordinator.json"),
        h.paths().config.join("init.json"),
        crate::config::paths::user_config_path(&h.env).unwrap(),
    ];
    let mut result: Vec<_> = paths
        .drain(..)
        .map(|p| fs::read(p).unwrap_or_default())
        .collect();
    result.push(
        serde_json::to_vec(
            &WorkerStore::open_read_only(&s.worker_state_dir)
                .unwrap()
                .members()
                .unwrap(),
        )
        .unwrap(),
    );
    result
}
fn assert_joined(a: &Host, b: &Host) {
    let (sa, sb) = (spec(a), spec(b));
    assert_eq!(sa.expected_cluster, sb.expected_cluster);
    assert_eq!(
        fs::read(&sa.cluster_key_path).unwrap(),
        fs::read(&sb.cluster_key_path).unwrap()
    );
    for (host, own, peer) in [(a, &sa, &sb), (b, &sb, &sa)] {
        let entries = crate::service::workers::registrations(&host.env).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, peer.worker_id.as_str());
        let coordinator = CoordinatorStore::open_read_only(&peer.state_dir).unwrap();
        assert!(WorkerStore::open_read_only(&own.worker_state_dir)
            .unwrap()
            .members()
            .unwrap()
            .contains(&(
                peer.worker_id.to_string(),
                coordinator.coordinator_id().to_string()
            )));
    }
}
#[test]
fn singleton_join_registers_both_directions_and_repeats() {
    let (a, b) = (initialized("a"), initialized("b"));
    assert_ne!(spec(&a).expected_cluster, spec(&b).expected_cluster);
    let mut pair = Pair {
        a: &a,
        b: &b,
        fail_second: false,
        calls: 0,
    };
    run(&mut pair, false).unwrap();
    assert_joined(&a, &b);
    let stable = || {
        let mut a = snapshot(&a);
        let mut b = snapshot(&b);
        // Same-group enrollment republishes the key receipt, as workers add does.
        a.remove(2);
        b.remove(2);
        (a, b)
    };
    let before = stable();
    run(&mut pair, false).unwrap();
    assert_joined(&a, &b);
    assert_eq!(before, stable());
}
#[test]
fn join_resumes_after_second_step_fails() {
    let (a, b) = (initialized("a"), initialized("b"));
    let mut pair = Pair {
        a: &a,
        b: &b,
        fail_second: true,
        calls: 0,
    };
    assert!(run(&mut pair, false)
        .unwrap_err()
        .message
        .contains("now in the existing member's group"));
    assert_eq!(spec(&a).expected_cluster, spec(&b).expected_cluster);
    assert_eq!(
        crate::service::workers::registrations(&a.env)
            .unwrap()
            .len(),
        1
    );
    assert!(crate::service::workers::registrations(&b.env)
        .unwrap()
        .is_empty());
    pair.fail_second = false;
    run(&mut pair, false).unwrap();
    assert_joined(&a, &b);
}
#[test]
fn other_members_require_move_whether_listed_or_only_pinned() {
    for pinned in [false, true] {
        let (a, b) = (initialized("a"), initialized("b"));
        if pinned {
            WorkerStore::open(&spec(&b).worker_state_dir, &spec(&b).worker_id)
                .unwrap()
                .for_member(&CoordinatorId::generate())
                .pin_member("other", 1)
                .unwrap();
        } else {
            fs::write(
                crate::config::paths::user_config_path(&b.env).unwrap(),
                "schema_version = 1\n[[workers]]\nid = 'other'\nssh = 'user@other'\n",
            )
            .unwrap();
        }
        let before = (snapshot(&a), snapshot(&b));
        let mut pair = Pair {
            a: &a,
            b: &b,
            fail_second: false,
            calls: 0,
        };
        assert!(run(&mut pair, false)
            .unwrap_err()
            .message
            .contains("--move"));
        assert_eq!(before, (snapshot(&a), snapshot(&b)));
        run(&mut pair, true).unwrap();
        assert_eq!(spec(&a).expected_cluster, spec(&b).expected_cluster);
    }
}
#[test]
fn live_work_refuses_even_move_without_changes() {
    let (a, b) = (initialized("a"), initialized("b"));
    let db = rusqlite::Connection::open(spec(&b).worker_state_dir.join("worker.db")).unwrap();
    put_attempt(&db, &CoordinatorId::generate(), 1, "active");
    let before = (snapshot(&a), snapshot(&b));
    let mut pair = Pair {
        a: &a,
        b: &b,
        fail_second: false,
        calls: 0,
    };
    assert!(run(&mut pair, true)
        .unwrap_err()
        .message
        .contains("live work"));
    assert_eq!(before, (snapshot(&a), snapshot(&b)));
    assert!(a.fake.0.borrow().calls.is_empty());
    assert!(b.fake.0.borrow().calls.is_empty());
}
#[test]
fn join_requires_init_before_any_ssh() {
    for installed in [false, true] {
        let (a, b) = (initialized("a"), Host::new());
        if installed {
            worker_only(&b);
        }
        let mut pair = Pair {
            a: &a,
            b: &b,
            fail_second: false,
            calls: 0,
        };
        assert!(run(&mut pair, false)
            .unwrap_err()
            .message
            .contains("belt init first"));
        assert_eq!(pair.calls, 0);
    }
}
#[test]
fn same_name_refuses_with_init_name_guidance() {
    let (a, b) = (initialized("same"), initialized("same"));
    let before = (snapshot(&a), snapshot(&b));
    let mut pair = Pair {
        a: &a,
        b: &b,
        fail_second: false,
        calls: 0,
    };
    let e = run(&mut pair, false).unwrap_err();
    assert!(e.message.contains("member_identity_mismatch") && e.message.contains("init --name"));
    assert_eq!(before, (snapshot(&a), snapshot(&b)));
}
#[test]
fn takeover_name_mismatch_refuses_before_stopping() {
    let h = Host::new();
    worker_only(&h);
    let before = fs::read(h.paths().journal).unwrap();
    let calls = h.fake.0.borrow().calls.len();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = rt
        .block_on(initialize_named(
            ContextInput {
                env: h.env.clone(),
                executable: h.exe.clone(),
            },
            Some("different"),
            &mut h.fake.clone(),
        ))
        .unwrap_err();
    assert!(error.to_string().contains("cannot rename"));
    assert_eq!(fs::read(h.paths().journal).unwrap(), before);
    assert_eq!(h.fake.0.borrow().calls.len(), calls);
}

#[test]
fn initialized_same_group_enrollment_keeps_current_next_and_records_on_mismatch() {
    let mut h = initialized("target");
    let spec = spec(&h);
    let store = WorkerStore::open(&spec.worker_state_dir, &spec.worker_id).unwrap();
    let current = crate::secrets::key::load(&spec.cluster_key_path).unwrap();
    store
        .observe_group_key(&crate::group::fingerprint(&current))
        .unwrap();
    let next_path = crate::group::next_key_path(&spec.cluster_key_path);
    let next = crate::secrets::key::init(&next_path).unwrap();
    store
        .set_next_group_key(&crate::storage::KeyGeneration {
            generation: 2,
            fingerprint: crate::group::fingerprint(&next),
        })
        .unwrap();
    let records = store.group_keys().unwrap();
    let before = snapshot(&h);
    let next_before = fs::read(&next_path).unwrap();
    h.request.cluster = spec.expected_cluster.unwrap();
    h.request.worker_id = spec.worker_id;
    h.request.key_file_text = "ab".repeat(32) + "\n";
    h.request.membership = Some(Membership {
        caller_name: WorkerId::parse("caller").unwrap(),
        caller_id: CoordinatorId::generate(),
        move_group: false,
        join_singleton: false,
        install: false,
    });
    let error = h.install().unwrap_err();
    assert!(
        error.to_string().contains("belt group send-key <member>"),
        "{error}"
    );
    assert_eq!(snapshot(&h), before);
    assert_eq!(fs::read(next_path).unwrap(), next_before);
    assert_eq!(store.group_keys().unwrap(), records);
}
