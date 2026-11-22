//! Tests for behaviour that already existed and had none: how
//! retention treats retired and half-written records, a second warm-up after
//! release, and the complete-content proof of a large setup tree.

/// A second `workers sync` after the worker let go of an idle warm-up
/// (`warmup_idle_seconds`, one second here: the smallest the configuration
/// allows, so the test waits for real time and no hook is needed).
mod warm_up_after_release {
    use belt::ids::RequestId;
    use belt::service::bootstrap::{ensure, Spec};
    use belt::service::resident::{Client, Operation, Reply};
    use belt::wire::{SyncOutcome, ToolRequirement};
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    /// A local worker whose registry may install node 20.0.0 with a fixture
    /// manager that logs each run to `calls` and waits while `hold` exists.
    fn fixture() -> (crate::common::DaemonRoot, Spec, PathBuf) {
        use belt::toolchain::profile::Profile;
        let (root, spec, _input) = crate::bootstrap::control_fixture();
        std::fs::write(
            root.path().join(".config/belt/config.toml"),
            "schema_version = 1\n[coordinator]\nheartbeat_seconds = 1\nstale_seconds = 3\nwarmup_idle_seconds = 1\n",
        )
        .unwrap();
        let host = root.path().join("private-managers");
        let stubs = root.path().join("tool-stubs");
        std::fs::create_dir(&host).unwrap();
        crate::common::write_executable(&host.join("node"), "#!/bin/sh\nprintf 'v18.0.0\\n'\n");
        let traffic_config = belt::config::traffic::resolve(&spec.env)
            .unwrap()
            .config_file;
        let traffic = traffic_config.parent().unwrap();
        std::fs::create_dir_all(traffic).unwrap();
        std::fs::set_permissions(traffic, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(
            &traffic_config,
            "budget = 8\ncpu_threshold_percent = 100.0\nmemory_reserve_mib = 0\n",
        )
        .unwrap();
        let profile_text = format!("schema_version=1\nactive='base18'\n[profiles.base18]\npath=[{:?},{stubs:?},'/usr/bin','/bin']\nshell='/bin/sh'\ncache_namespace='base18'\nrequired_tools=['node']\nallowed_env=['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','LLVM_PROFILE_FILE']\n", host.to_str().unwrap());
        let profile_file = spec.worker_state_dir.join("profiles.toml");
        std::fs::write(&profile_file, &profile_text).unwrap();
        std::fs::set_permissions(&profile_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let profile = Profile::load(&spec.worker_state_dir, &spec.env).unwrap();
        let manager = format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$1" >> '{host}/calls'
while [ -e '{host}/hold' ]; do /bin/sleep 0.05; done
version="$1"
bin="$NODENV_ROOT/versions/$version/bin"
/bin/mkdir -p "$bin"
/usr/bin/printf '#!/bin/sh\necho v%s\n' "$version" > "$bin/node"
/bin/chmod 700 "$bin/node"
"#,
            host = host.display()
        );
        crate::common::write_executable(&host.join("manager"), &manager);
        crate::common::write_executable(&host.join("node-build"), &manager);
        let registry = format!("schema_version=1\nactive_profile='base18'\nprofile_definition={:?}\n[[recipes]]\ntool='node'\nkind='Nodenv'\nmanager={:?}\ndependencies=[{:?}]\nsearch_path=[{:?}{}]\ninventory=['20.0.0','22.0.0']\n", profile.definition_digest().as_str(), host.join("manager").to_str().unwrap(), host.join("node-build").to_str().unwrap(), host.to_str().unwrap(), crate::common::system_search_path_toml());
        let registry_file = spec.worker_state_dir.join("toolchains.toml");
        std::fs::write(&registry_file, registry).unwrap();
        std::fs::set_permissions(&registry_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        (root, spec, host)
    }

    fn calls(host: &Path, version: &str) -> usize {
        std::fs::read_to_string(host.join("calls"))
            .unwrap_or_default()
            .lines()
            .filter(|l| *l == version)
            .count()
    }

    fn installed(spec: &Spec) -> Vec<String> {
        let mut report =
            belt::toolchain::envreport::collect(&spec.worker_state_dir, &spec.env).unwrap();
        report.tools.retain(|name, _| name == "node");
        report.tools["node"]
            .managed
            .as_ref()
            .expect("the registry manages node")
            .installed
            .clone()
    }

    async fn warm(client: &Client, spec: &Spec, sync: &RequestId) -> SyncOutcome {
        let Reply::EnvSync { results } = client
            .request(Operation::EnvSync {
                worker: spec.worker_id.clone(),
                sync_id: sync.clone(),
                tools: vec![ToolRequirement {
                    name: "node".into(),
                    version: "20.0.0".into(),
                }],
            })
            .await
            .unwrap()
        else {
            panic!("the resident must answer the warm-up")
        };
        results[0].outcome
    }

    async fn until(what: &str, mut f: impl FnMut() -> bool) {
        let end = Instant::now() + Duration::from_secs(20);
        while !f() {
            assert!(Instant::now() < end, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(client: &Client) {
        assert!(matches!(
            client.request(Operation::Shutdown).await.unwrap(),
            Reply::Stopping
        ));
        until("the resident to stop", || !client.socket_path().exists()).await;
    }

    fn managers(host: &Path) -> Vec<i32> {
        let out = std::process::Command::new("pgrep")
            .args(["-f", &format!("{}/node-build", host.display())])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .collect()
    }

    /// Ask until the warm-up is decided, never seeing two managers at once.
    async fn decided(client: &Client, spec: &Spec, host: &Path, sync: &RequestId) -> SyncOutcome {
        let end = Instant::now() + Duration::from_secs(20);
        loop {
            let outcome = warm(client, spec, sync).await;
            assert!(managers(host).len() <= 1, "two installs run at once");
            if outcome != SyncOutcome::Pending {
                return outcome;
            }
            assert!(Instant::now() < end, "the install did not end");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// An install nobody asks about any more loses its warm-up after the idle
    /// time. A sync after that reaches the installed state through one
    /// install at a time and never a second one beside the first, and a
    /// third sync finds nothing to do.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_sync_after_the_idle_release_ends_installed_with_no_second_install_beside_the_first()
    {
        let (_root, spec, host) = fixture();
        let client = ensure(spec.clone()).await.unwrap();
        std::fs::write(host.join("hold"), "").unwrap();
        let first = RequestId::generate();
        assert_eq!(warm(&client, &spec, &first).await, SyncOutcome::Pending);
        until("the first install to begin", || calls(&host, "20.0.0") == 1).await;
        // Nobody asks again for longer than the idle time (one second).
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(installed(&spec).is_empty());
        std::fs::remove_file(host.join("hold")).unwrap();
        // Asked again with the same id, as a parent that was cut off does.
        assert_eq!(
            decided(&client, &spec, &host, &first).await,
            SyncOutcome::Installed
        );
        assert_eq!(installed(&spec), ["20.0.0"]);
        // The first install may have been stopped by the release and run
        // again, or been adopted by the new warm-up; never more than one
        // install more than the one that was let go.
        let runs = calls(&host, "20.0.0");
        assert!((1..=2).contains(&runs), "{runs} manager runs");
        // Nothing is left to do, and nothing runs.
        let third = RequestId::generate();
        assert_eq!(warm(&client, &spec, &third).await, SyncOutcome::Unchanged);
        assert_eq!(calls(&host, "20.0.0"), runs);
        assert!(managers(&host).is_empty());
        stop(&client).await;
    }

    /// A finished install whose warm-up was let go after the idle time: the
    /// next sync (new id) is `unchanged` and runs no manager, however the
    /// warm-up bookkeeping was released.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_sync_after_the_idle_release_of_a_finished_install_runs_nothing() {
        let (_root, spec, host) = fixture();
        let client = ensure(spec.clone()).await.unwrap();
        let first = RequestId::generate();
        assert_eq!(
            decided(&client, &spec, &host, &first).await,
            SyncOutcome::Installed
        );
        assert_eq!(calls(&host, "20.0.0"), 1);
        tokio::time::sleep(Duration::from_millis(2500)).await;
        // The same id: it is forgotten, so it is a new question with the same
        // answer; a new id likewise.
        for sync in [first, RequestId::generate()] {
            assert_eq!(
                decided(&client, &spec, &host, &sync).await,
                SyncOutcome::Unchanged
            );
        }
        assert_eq!(calls(&host, "20.0.0"), 1);
        assert_eq!(installed(&spec), ["20.0.0"]);
        stop(&client).await;
    }
}

/// The template's complete-content proof on a tree of thousands of files and
/// a few MiB: a consumer copies the template only after its whole tree digest
/// matches the receipt, so a change anywhere in it (a byte of a deep file
/// with its size and time kept, one file more, one file less) means the
/// template is not used and is rebuilt.
mod large_template_proof {
    use belt::error::ApiError;
    use belt::ids::{AttemptId, Digest, Generation, PlanDigest, RunId, SourceId, TaskId, WorkerId};
    use belt::setup::builder::{BuilderWorkspaces, SetupConfig, SetupPoll, Setups};
    use belt::setup::store::remove_tree;
    use belt::wire::{AttemptPayload, ReuseScope, SetupSpec, SetupStep};
    use belt::workspace::{Workspace, WorkspaceKind};
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const BIN: &str = env!("CARGO_BIN_EXE_belt");
    const LIMIT: Duration = Duration::from_secs(60);
    const DIRS: usize = 20;
    const PER_DIR: usize = 50;

    struct DirWs {
        base: PathBuf,
    }
    impl DirWs {
        fn make(&self, attempt: &AttemptId) -> Workspace {
            let root = self.base.join("ws").join(attempt.as_str());
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("package.json"), "{}\n").unwrap();
            Workspace {
                cwd: root.clone(),
                root,
                kind: WorkspaceKind::Directory,
                index_reproduced: true,
                warnings: Vec::new(),
            }
        }
    }
    impl BuilderWorkspaces for DirWs {
        fn create(&self, attempt: &AttemptId, _: &AttemptPayload) -> Result<Workspace, ApiError> {
            Ok(self.make(attempt))
        }
        fn remove(&self, ws: &Workspace) -> Result<(), ApiError> {
            remove_tree(&ws.root).unwrap();
            Ok(())
        }
    }

    struct Fx {
        dir: tempfile::TempDir,
        ws: Arc<DirWs>,
        setups: Arc<Setups>,
        run: RunId,
    }
    impl Drop for Fx {
        fn drop(&mut self) {
            // Only what this fixture's own directory names, and only while it
            // still is that process.
            let pattern = self.dir.path().to_string_lossy().into_owned();
            let out = std::process::Command::new("pgrep")
                .args(["-f", &pattern])
                .output()
                .unwrap();
            for pid in String::from_utf8_lossy(&out.stdout)
                .lines()
                .filter_map(|l| l.trim().parse::<i32>().ok())
            {
                crate::common::kill_if_command(pid, &pattern);
            }
            let _ = remove_tree(self.dir.path());
        }
    }

    fn fixture() -> Fx {
        let dir = tempfile::Builder::new()
            .prefix("belt-gaps-setup-")
            .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
            .unwrap();
        let ws = Arc::new(DirWs {
            base: dir.path().to_path_buf(),
        });
        let mut c = SetupConfig::new(
            dir.path().join("state"),
            WorkerId::parse("w-test").unwrap(),
            PathBuf::from(BIN),
        );
        c.poll_ms = 25;
        c.term_grace_ms = 1_000;
        c.kill_wait_ms = 2_000;
        c.deadline_ms = 120_000;
        c.env = BTreeMap::from([
            (
                "PATH".to_string(),
                "/usr/bin:/bin:/usr/sbin:/sbin".to_string(),
            ),
            ("HOME".to_string(), dir.path().display().to_string()),
        ]);
        let setups = Setups::open(c, ws.clone()).unwrap();
        Fx {
            dir,
            ws,
            setups,
            run: RunId::generate(),
        }
    }

    /// The bytes of file `n` of the generated tree: about 1 KiB each, one of
    /// them 1 MiB, and no two alike.
    fn content(n: usize) -> Vec<u8> {
        let size = if n == 7 { 1 << 20 } else { 700 + n % 512 };
        (0..size)
            .map(|i| (i.wrapping_mul(31).wrapping_add(n * 17) % 251) as u8)
            .collect()
    }

    /// `DIRS` directories of `PER_DIR` files each, one level deeper than the
    /// first: returns how many files there are.
    fn generate(root: &Path) -> usize {
        let mut n = 0;
        for d in 0..DIRS {
            let dir = root.join(format!("pkg{d:02}/lib/inner{}", d % 5));
            std::fs::create_dir_all(&dir).unwrap();
            for i in 0..PER_DIR {
                std::fs::write(dir.join(format!("f{i:02}.js")), content(n)).unwrap();
                n += 1;
            }
        }
        n
    }

    fn files_under(root: &Path) -> usize {
        std::fs::read_dir(root)
            .unwrap()
            .map(|e| {
                let p = e.unwrap().path();
                if p.is_dir() {
                    files_under(&p)
                } else {
                    1
                }
            })
            .sum()
    }

    fn payload(f: &Fx, run: &str) -> AttemptPayload {
        let step = SetupStep {
            id: "i1".into(),
            dir: String::new(),
            run: run.into(),
            shell: None,
            env: BTreeMap::new(),
            unresolved_env: vec![],
        };
        AttemptPayload {
            ci_task: None,
            toolchains: None,
            outputs: Vec::new(),
            requirements: Default::default(),
            in_place: false,
            run_id: f.run.clone(),
            task_id: TaskId::generate(),
            generation: Generation(1),
            source_id: SourceId::generate(),
            plan_digest: PlanDigest::of_bytes(b"plan"),
            manifest_digest: Digest::of_bytes(b"the source"),
            permit_id: None,
            commands: vec![],
            lease_ms: 60_000,
            command_timeout_ms: None,
            deadline_ms: None,
            resource_wait_timeout_ms: None,
            has_secret_bundle: false,
            artifacts: vec![],
            setup: Some(SetupSpec {
                steps: vec![step],
                toolchain: vec![],
                scope: ReuseScope::Persistent,
                freshness_secs: None,
                secrets: vec![],
                no_cache: false,
                step_timeout_ms: None,
            }),
        }
    }

    /// A consumer asks until the setup is decided: (skipped steps, note).
    fn done(f: &Fx, p: &AttemptPayload) -> (Vec<String>, Option<String>, Workspace) {
        let a = AttemptId::generate();
        let ws = f.ws.make(&a);
        let end = Instant::now() + LIMIT;
        loop {
            if let SetupPoll::Done { skip, note } = f.setups.poll(&a, p, &ws).unwrap() {
                return (skip.into_iter().collect(), note, ws);
            }
            assert!(Instant::now() < end, "the setup did not end");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn templates(f: &Fx) -> Vec<PathBuf> {
        let Ok(rd) = std::fs::read_dir(f.dir.path().join("state/setup/templates")) else {
            return vec![];
        };
        rd.flatten()
            .filter(|e| !e.file_name().to_string_lossy().starts_with(".tmp-"))
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect()
    }

    fn open(path: &Path) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Build the template, check it is used whole, change `mutate` in it, and
    /// check that the next consumer is refused it and the one after is given a
    /// rebuilt one.
    fn proof_refuses(what: &str, mutate: fn(&Path)) {
        let f = fixture();
        let generated = f.dir.path().join("generated");
        let total = generate(&generated);
        assert_eq!(total, DIRS * PER_DIR);
        let p = payload(&f, &format!("cp -R '{}' node_modules", generated.display()));
        let deep = format!(
            "node_modules/pkg{:02}/lib/inner{}/f49.js",
            DIRS - 1,
            (DIRS - 1) % 5
        );
        // Untouched first: the proof does not refuse a whole template.
        let (skip, note, ws) = done(&f, &p);
        assert_eq!(skip, ["i1"], "{note:?}");
        assert_eq!(files_under(&ws.root.join("node_modules")), total);
        let original = std::fs::read(ws.root.join(&deep)).unwrap();
        assert_eq!(original, content(total - 1));
        let (skip, _, ws) = done(&f, &p);
        assert_eq!(skip, ["i1"]);
        assert_eq!(files_under(&ws.root.join("node_modules")), total);
        assert_eq!(f.setups.builds_started(), 1);

        let templates_now = templates(&f);
        assert_eq!(templates_now.len(), 1);
        let tdir = templates_now[0].clone();
        open(&tdir);
        open(&tdir.join("tree"));
        open(&tdir.join("tree/node_modules"));
        mutate(&tdir.join("tree"));
        // The next consumer is told, takes nothing of it and runs the steps
        // itself; the template is gone.
        let (skip, note, ws) = done(&f, &p);
        assert!(skip.is_empty(), "{what}: {skip:?}");
        assert!(note.is_some(), "{what}");
        assert!(!ws.root.join("node_modules").exists(), "{what}");
        assert!(templates(&f).is_empty(), "{what}");
        // The one after that gets a template rebuilt from nothing.
        let (skip, _, ws) = done(&f, &p);
        assert_eq!(skip, ["i1"], "{what}");
        assert_eq!(f.setups.builds_started(), 2, "{what}");
        assert_eq!(files_under(&ws.root.join("node_modules")), total, "{what}");
        assert_eq!(
            std::fs::read(ws.root.join(&deep)).unwrap(),
            original,
            "{what}"
        );
    }

    /// The same size and the same modification time: only a byte differs.
    #[test]
    fn one_changed_byte_of_a_deep_file_of_a_large_template_means_a_rebuild() {
        proof_refuses("one byte of a deep file", |tree| {
            let file = tree.join(format!(
                "node_modules/pkg{:02}/lib/inner{}/f49.js",
                DIRS - 1,
                (DIRS - 1) % 5
            ));
            let before = std::fs::metadata(&file).unwrap().modified().unwrap();
            open(file.parent().unwrap());
            open(&file);
            let mut bytes = std::fs::read(&file).unwrap();
            let at = bytes.len() / 2;
            bytes[at] ^= 0x01;
            std::fs::write(&file, bytes).unwrap();
            std::fs::File::options()
                .write(true)
                .open(&file)
                .unwrap()
                .set_modified(before)
                .unwrap();
        });
    }

    #[test]
    fn one_added_file_deep_in_a_large_template_means_a_rebuild() {
        proof_refuses("one file more", |tree| {
            let dir = tree.join("node_modules/pkg03/lib/inner3");
            open(&dir);
            std::fs::write(dir.join("extra.js"), b"x").unwrap();
        });
    }

    #[test]
    fn one_removed_file_of_a_large_template_means_a_rebuild() {
        proof_refuses("one file less", |tree| {
            let dir = tree.join("node_modules/pkg03/lib/inner3");
            open(&dir);
            std::fs::remove_file(dir.join("f00.js")).unwrap();
        });
    }
}
