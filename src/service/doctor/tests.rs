//! Facts from temporary directories and a scripted command runner; checks
//! from hand-built facts; the whole run against a scripted stand-in for ssh.
//! No real task-traffic-controller, docker, ssh or service is touched.
use super::*;
use std::fs;

#[derive(Default)]
struct FakeRunner {
    /// `(program file name, first argument)` -> `(exit code, stdout)`.
    answers: BTreeMap<(String, String), (i32, String)>,
    asked: Vec<(String, Vec<String>)>,
}
impl FakeRunner {
    fn answer(&mut self, program: &str, first: &str, code: i32, stdout: &str) {
        self.answers
            .insert((program.into(), first.into()), (code, stdout.into()));
    }
}
impl Runner for FakeRunner {
    fn run(
        &mut self,
        program: &Path,
        arguments: &[&str],
        _: &BTreeMap<String, String>,
    ) -> Option<(i32, String)> {
        let name = program.file_name().unwrap().to_string_lossy().into_owned();
        self.asked.push((
            program.to_string_lossy().into_owned(),
            arguments.iter().map(|a| a.to_string()).collect(),
        ));
        self.answers.get(&(name, arguments[0].to_string())).cloned()
    }
}

struct Host {
    _dir: tempfile::TempDir,
    home: PathBuf,
    bin: PathBuf,
    state: PathBuf,
    env: BTreeMap<String, String>,
}
impl Host {
    fn new() -> Self {
        let dir = crate::test_tmp::tempdir().unwrap();
        let home = dir.path().join("home");
        let bin = dir.path().join("bin");
        let state = home.join(".belt");
        for path in [&home, &bin, &state] {
            fs::create_dir(path).unwrap();
        }
        fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
        let env = BTreeMap::from([
            ("HOME".to_string(), home.to_str().unwrap().to_string()),
            (
                "PATH".to_string(),
                format!("{}:/nonexistent", bin.display()),
            ),
        ]);
        Host {
            _dir: dir,
            home,
            bin,
            state,
            env,
        }
    }
    fn tool(&self, name: &str, mode: u32) -> PathBuf {
        let path = self.bin.join(name);
        fs::write(&path, "#!/bin/sh\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        path
    }
}

#[test]
fn a_tool_is_found_only_as_an_executable_file_in_an_absolute_directory() {
    let h = Host::new();
    assert_eq!(find_on_path(&h.env, "tool"), None);
    h.tool("tool", 0o644);
    assert_eq!(find_on_path(&h.env, "tool"), None);
    let path = h.tool("tool", 0o755);
    assert_eq!(find_on_path(&h.env, "tool"), Some(path));
    fs::create_dir(h.bin.join("directory")).unwrap();
    assert_eq!(find_on_path(&h.env, "directory"), None);
    assert_eq!(find_on_path(&BTreeMap::new(), "tool"), None);
    // A relative PATH entry is never searched.
    let relative = BTreeMap::from([("PATH".to_string(), ".".to_string())]);
    assert_eq!(find_on_path(&relative, "Cargo.toml"), None);
}

#[test]
fn the_command_scope_follows_xdg_and_falls_back_to_home() {
    let env = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        cli_scope_paths(&env(&[("HOME", "/h")])),
        (
            PathBuf::from("/h/.local/state/task-traffic-controller"),
            PathBuf::from("/h/.config/task-traffic-controller/config.toml")
        )
    );
    assert_eq!(
        cli_scope_paths(&env(&[
            ("HOME", "/h"),
            ("XDG_STATE_HOME", "/s"),
            ("XDG_CONFIG_HOME", "/c")
        ])),
        (
            PathBuf::from("/s/task-traffic-controller"),
            PathBuf::from("/c/task-traffic-controller/config.toml")
        )
    );
    // Empty values are unset, as the command itself reads them.
    assert_eq!(
        cli_scope_paths(&env(&[
            ("HOME", "/h"),
            ("XDG_STATE_HOME", ""),
            ("XDG_CONFIG_HOME", "")
        ])),
        cli_scope_paths(&env(&[("HOME", "/h")]))
    );
    assert_eq!(
        cli_scope_paths(&env(&[])).0,
        PathBuf::from("/.local/state/task-traffic-controller")
    );
    assert_eq!(
        worker_scope_paths(&env(&[("HOME", "/h"), ("XDG_STATE_HOME", "/s")])),
        (
            PathBuf::from("/s/task-traffic-controller"),
            PathBuf::from("/h/.config/task-traffic-controller/config.toml")
        )
    );
}

#[test]
fn the_worker_scope_is_the_one_the_agent_gives_its_supervisors() {
    // The environment the journal records at install decides the scope: what
    // doctor looks at as the worker's is what the agent hands its supervisors
    // and setup steps, whatever the variables are.
    let host = Host::new();
    let state_home = host.home.join("xdg-state");
    let config_home = host.home.join("xdg-config");
    let variants: [Vec<(&str, String)>; 4] = [
        vec![],
        vec![("XDG_STATE_HOME", state_home.display().to_string())],
        vec![("XDG_CONFIG_HOME", config_home.display().to_string())],
        vec![
            ("XDG_STATE_HOME", state_home.display().to_string()),
            ("XDG_CONFIG_HOME", config_home.display().to_string()),
        ],
    ];
    for extra in variants {
        let mut recorded = host.env.clone();
        recorded.extend(extra.into_iter().map(|(k, v)| (k.to_string(), v)));
        let config = crate::config::resolve(&crate::config::Inputs {
            overlay: &crate::config::Overlay::default(),
            project: None,
            legacy: None,
            user: None,
        })
        .unwrap();
        let runtime = config
            .worker_runtime(crate::config::runtime::WorkerInputs {
                state_dir: host.state.clone(),
                worker_id: WorkerId::parse("mini-a").unwrap(),
                boot_id: crate::ids::WorkerBootId::parse("boot").unwrap(),
                supervisor_exe: PathBuf::from("/bin/belt"),
                cluster_key_path: host.home.join("cluster.key"),
                env: recorded.clone(),
            })
            .unwrap();
        let (state, config_file) = worker_scope_paths(&recorded);
        let agent = runtime.agent_config();
        assert_eq!(state, agent.traffic_state_dir, "{recorded:?}");
        assert_eq!(Some(&config_file), agent.traffic_config.as_ref());
        let setup = runtime.setup_config();
        assert_eq!(state, setup.traffic_state_dir);
        assert_eq!(setup.traffic_config, agent.traffic_config);
    }
}

#[test]
fn the_policy_digest_is_the_one_task_traffic_controller_reports() {
    let h = Host::new();
    let state = h.state.join("traffic");
    let config = state.join("config.toml");
    fs::create_dir(&state).unwrap();
    // A budget equal to the logical CPU count is left out of our digest (it
    // is the default), so pick one that is not the default here.
    let budget = task_traffic_controller::config::logical_cpus() + 1;
    fs::write(&config, format!("budget = {budget}\n")).unwrap();
    let ours = scope(&state, &config).policy_digest.unwrap();
    let context = task_traffic_controller::Context::new(Paths::new(&state, &config));
    let theirs = task_traffic_controller::inspect::inspect(&context).unwrap();
    assert_eq!(ours, theirs.policy_digest);
    // Another budget is another policy; a broken file has no digest.
    fs::write(&config, format!("budget = {}\n", budget + 1)).unwrap();
    assert_ne!(scope(&state, &config).policy_digest.unwrap(), ours);
    fs::write(&config, "budget = \"many\"\n").unwrap();
    assert_eq!(scope(&state, &config).policy_digest, None);
}

#[test]
fn a_scope_says_whether_it_is_in_use_and_how_many_entries_it_holds() {
    let h = Host::new();
    let (state, config) = cli_scope_paths(&h.env);
    let empty = scope(&state, &config);
    assert!(!empty.in_use);
    assert_eq!(empty.ledger_entries, Some(0));
    assert!(empty.policy_digest.is_some());
    assert_eq!(empty.state_dir, state.to_str().unwrap());
    assert_eq!(empty.config_file, config.to_str().unwrap());
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    fs::write(&config, "").unwrap();
    assert!(scope(&state, &config).in_use);
    fs::remove_file(&config).unwrap();
    fs::create_dir_all(&state).unwrap();
    assert!(scope(&state, &config).in_use);
    let ledger = Paths::new(&state, &config).ledger_file();
    fs::write(&ledger, "").unwrap();
    assert_eq!(scope(&state, &config).ledger_entries, Some(0));
    fs::write(&ledger, "not a ledger").unwrap();
    assert_eq!(scope(&state, &config).ledger_entries, None);
}

#[test]
fn gathering_asks_the_tools_the_service_environment_resolves() {
    let h = Host::new();
    let mut runner = FakeRunner::default();
    // Nothing installed: nothing is asked.
    let facts = gather(&h.state, &h.env, &h.env, &mut runner);
    assert_eq!(facts.schema, 1);
    assert_eq!(facts.cli, None);
    assert_eq!(facts.docker, Docker::default());
    assert!(runner.asked.is_empty());
    assert_eq!(
        facts.worker_scope.state_dir,
        h.home
            .join(".local/state/task-traffic-controller")
            .to_str()
            .unwrap()
    );
    assert_eq!(
        facts.cli_scope.state_dir,
        h.home
            .join(".local/state/task-traffic-controller")
            .to_str()
            .unwrap()
    );
    assert_eq!(facts.profile.error, None);
    assert!(facts.profile.name.is_some() && facts.profile.fingerprint.is_some());

    let cli = h.tool("task-traffic-controller", 0o755);
    let docker = h.tool("docker", 0o755);
    runner.answer(
        "task-traffic-controller",
        "status",
        0,
        r#"{"ledger_format":1,"entries":[]}"#,
    );
    runner.answer("docker", "info", 0, "ENGINE-1\n");
    runner.answer("docker", "context", 0, "unix:///var/run/docker.sock\n");
    let facts = gather(&h.state, &h.env, &h.env, &mut runner);
    assert_eq!(
        facts.cli,
        Some(TrafficCli {
            path: cli.to_str().unwrap().into(),
            status_readable: true,
            knows_managed: true
        })
    );
    assert_eq!(
        facts.docker,
        Docker {
            cli: Some(docker.to_str().unwrap().into()),
            endpoint: Some("unix:///var/run/docker.sock".into()),
            engine_id: Some("ENGINE-1".into())
        }
    );
    assert_eq!(
        runner.asked[0],
        (
            cli.to_str().unwrap().to_string(),
            vec!["status".to_string(), "--json".to_string()]
        )
    );
    assert!(runner
        .asked
        .iter()
        .any(|(_, a)| a == &["info", "--format", "{{.ID}}"]));
}

#[test]
fn an_old_or_silent_command_is_seen_for_what_it_is() {
    let h = Host::new();
    h.tool("task-traffic-controller", 0o755);
    let cli = |code: i32, stdout: &str| {
        let mut runner = FakeRunner::default();
        runner.answer("task-traffic-controller", "status", code, stdout);
        let cli = traffic_cli(&h.env, &mut runner).unwrap();
        (cli.status_readable, cli.knows_managed)
    };
    // The status JSON of a binary from before managed entries.
    assert_eq!(cli(0, r#"{"budget":8,"entries":[]}"#), (true, false));
    assert_eq!(cli(0, r#"{"ledger_format":2}"#), (true, true));
    assert_eq!(cli(2, r#"{"ledger_format":2}"#), (false, false));
    assert_eq!(cli(0, "not json"), (false, false));
    assert_eq!(cli(0, "[]"), (false, false));
    // It did not run at all.
    let silent = traffic_cli(&h.env, &mut FakeRunner::default()).unwrap();
    assert!(!silent.status_readable && !silent.knows_managed);
}

#[test]
fn docker_facts_prefer_the_environment_and_ignore_failed_answers() {
    let h = Host::new();
    h.tool("docker", 0o755);
    let mut runner = FakeRunner::default();
    runner.answer("docker", "info", 1, "ENGINE\n");
    runner.answer("docker", "context", 0, "two\nlines\n");
    let facts = docker(&h.env, &mut runner);
    assert!(facts.cli.is_some());
    assert_eq!((facts.endpoint, facts.engine_id), (None, None));
    let mut env = h.env.clone();
    env.insert("DOCKER_HOST".into(), "tcp://10.0.0.1:2375".into());
    let mut runner = FakeRunner::default();
    runner.answer("docker", "info", 0, "  \n");
    let facts = docker(&env, &mut runner);
    assert_eq!(facts.endpoint.as_deref(), Some("tcp://10.0.0.1:2375"));
    assert_eq!(facts.engine_id, None);
    // The context is not asked when the environment already says.
    assert_eq!(runner.asked.len(), 1);
    env.insert("DOCKER_HOST".into(), String::new());
    let mut runner = FakeRunner::default();
    runner.answer("docker", "context", 0, "unix:///s\n");
    assert_eq!(
        docker(&env, &mut runner).endpoint.as_deref(),
        Some("unix:///s")
    );
}

#[test]
fn a_profile_that_does_not_load_is_a_fact_not_a_default() {
    let h = Host::new();
    fs::write(h.state.join("profiles.toml"), "this is not = [toml").unwrap();
    fs::set_permissions(
        h.state.join("profiles.toml"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let facts = profile(&h.state, &h.env);
    assert!(facts.error.is_some());
    assert_eq!((facts.name, facts.fingerprint), (None, None));
}

fn healthy() -> HostFacts {
    let one = Scope {
        state_dir: "/s".into(),
        config_file: "/s/config.toml".into(),
        in_use: true,
        policy_digest: Some("sha256:p".into()),
        ledger_entries: Some(0),
    };
    HostFacts {
        schema: 1,
        worker_scope: one.clone(),
        cli_scope: one,
        cli: Some(TrafficCli {
            path: "/bin/task-traffic-controller".into(),
            status_readable: true,
            knows_managed: true,
        }),
        docker: Docker {
            cli: Some("/bin/docker".into()),
            endpoint: Some("unix:///var/run/docker.sock".into()),
            engine_id: Some("E1".into()),
        },
        profile: ProfileFacts {
            name: Some("default".into()),
            fingerprint: Some("sha256:f".into()),
            error: None,
        },
        cli_jobs: Some(CliJobs::default()),
        cli_jobs_error: None,
        linger: None,
        abandoned_imports: None,
        source_lock_problem: None,
        service_path: None,
        macos_linker: None,
    }
}
fn level(checks: &[Check], id: &str) -> Level {
    let found: Vec<_> = checks.iter().filter(|c| c.id == id).collect();
    assert_eq!(found.len(), 1, "{id}");
    found[0].level
}
fn detail<'a>(checks: &'a [Check], id: &str) -> &'a str {
    &checks.iter().find(|c| c.id == id).unwrap().detail
}

#[test]
fn a_host_that_fits_together_passes_every_check() {
    let checks = evaluate("mini-a", &healthy());
    assert_eq!(
        checks.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        [
            "traffic_runtime",
            "traffic_processes",
            "traffic_state_path",
            "traffic_policy",
            "docker_resource_domain",
            "service_profile"
        ]
    );
    assert!(checks
        .iter()
        .all(|c| c.host == "mini-a" && c.level == Level::Ok));
    assert!(detail(&checks, "service_profile").contains("default (sha256:f)"));
    assert!(detail(&checks, "docker_resource_domain").contains("E1"));
}

#[test]
fn an_unpatched_task_traffic_controller_is_not_compatible() {
    // A05: the binary that does not know managed entries fails.
    let mut facts = healthy();
    facts.cli.as_mut().unwrap().knows_managed = false;
    facts.cli_scope.ledger_entries = Some(3);
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_runtime"), Level::Fail);
    let text = detail(&checks, "traffic_runtime");
    assert!(text.contains("/bin/task-traffic-controller") && text.contains("3 in its ledger"));
    facts.cli_scope.ledger_entries = None;
    assert!(detail(&evaluate("parent", &facts), "traffic_runtime").contains("ledger is unreadable"));
    // One that cannot be asked is not assumed compatible either.
    facts.cli.as_mut().unwrap().status_readable = false;
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_runtime"), Level::Fail);
    assert!(detail(&checks, "traffic_runtime").contains("compatibility is unknown"));
    facts.cli = None;
    assert_eq!(
        level(&evaluate("parent", &facts), "traffic_runtime"),
        Level::Ok
    );
}

#[test]
fn two_scopes_fail_when_the_command_is_there_and_are_noted_when_it_is_not() {
    // A05: the same budget and policy only when the scope is one.
    let mut facts = healthy();
    facts.worker_scope.state_dir = "/w/traffic".into();
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_state_path"), Level::Fail);
    assert!(
        detail(&checks, "traffic_state_path").contains("/w/traffic")
            && detail(&checks, "traffic_state_path").contains("two budgets")
    );
    // The command is gone but left state: still two budgets in use.
    facts.cli = None;
    assert_eq!(
        level(&evaluate("parent", &facts), "traffic_state_path"),
        Level::Fail
    );
    facts.cli_scope.in_use = false;
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_state_path"), Level::Notice);
    assert!(detail(&checks, "traffic_state_path").contains("not installed"));
    // Only the configuration file differs: still not one scope.
    let mut facts = healthy();
    facts.worker_scope.config_file = "/other.toml".into();
    assert_eq!(
        level(&evaluate("parent", &facts), "traffic_state_path"),
        Level::Fail
    );
}

#[test]
fn policies_must_match_and_an_unreadable_one_is_a_failure() {
    let mut facts = healthy();
    facts.worker_scope.policy_digest = Some("sha256:other".into());
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_policy"), Level::Fail);
    assert!(
        detail(&checks, "traffic_policy").contains("sha256:other")
            && detail(&checks, "traffic_policy").contains("sha256:p")
    );
    facts.cli = None;
    facts.cli_scope.in_use = false;
    assert_eq!(
        level(&evaluate("parent", &facts), "traffic_policy"),
        Level::Notice
    );
    facts.worker_scope.policy_digest = None;
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_policy"), Level::Fail);
    assert!(detail(&checks, "traffic_policy").contains("worker scope's"));
    let mut facts = healthy();
    facts.cli_scope.policy_digest = None;
    let checks = evaluate("parent", &facts);
    assert_eq!(level(&checks, "traffic_policy"), Level::Fail);
    assert!(detail(&checks, "traffic_policy").contains("command's"));
}

#[test]
fn a_docker_engine_on_another_host_is_a_domain_this_version_does_not_manage() {
    let mut facts = healthy();
    for remote in ["tcp://10.0.0.1:2375", "ssh://ci@engine", "https://engine"] {
        facts.docker.endpoint = Some(remote.into());
        let checks = evaluate("mini-a", &facts);
        assert_eq!(level(&checks, "docker_resource_domain"), Level::Fail);
        assert!(detail(&checks, "docker_resource_domain").contains(remote));
    }
    // Remote even when it does not answer right now.
    facts.docker.engine_id = None;
    assert_eq!(
        level(&evaluate("mini-a", &facts), "docker_resource_domain"),
        Level::Fail
    );
    for local in ["unix:///var/run/docker.sock", "npipe:////./pipe/docker"] {
        facts.docker.endpoint = Some(local.into());
        assert_eq!(
            level(&evaluate("mini-a", &facts), "docker_resource_domain"),
            Level::Notice
        );
    }
    facts.docker.endpoint = None;
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "docker_resource_domain"), Level::Notice);
    assert!(detail(&checks, "docker_resource_domain").contains("no engine answered"));
    // An engine that answers through an endpoint nobody could name is not
    // "on this host": the current context may point anywhere.
    facts.docker.engine_id = Some("E".into());
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "docker_resource_domain"), Level::NotChecked);
    assert!(
        detail(&checks, "docker_resource_domain").contains("engine E answered")
            && detail(&checks, "docker_resource_domain").contains("could not be determined")
    );
    facts.docker.endpoint = Some("unix:///var/run/docker.sock".into());
    assert_eq!(
        level(&evaluate("mini-a", &facts), "docker_resource_domain"),
        Level::Ok
    );
    facts.docker = Docker::default();
    assert_eq!(
        level(&evaluate("mini-a", &facts), "docker_resource_domain"),
        Level::Ok
    );
}

#[test]
fn a_profile_error_fails_the_service_profile_check() {
    let mut facts = healthy();
    facts.profile = ProfileFacts {
        name: None,
        fingerprint: None,
        error: Some("profiles.toml: bad".into()),
    };
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "service_profile"), Level::Fail);
    assert!(detail(&checks, "service_profile").contains("profiles.toml: bad"));
}

#[test]
fn one_engine_reached_from_two_hosts_fails_on_both() {
    let hosts = |list: &[(&str, Option<&str>)]| {
        list.iter()
            .map(|(h, e)| (h.to_string(), e.map(str::to_owned)))
            .collect::<Vec<_>>()
    };
    assert!(shared_engines(&hosts(&[
        ("parent", Some("E1")),
        ("a", Some("E2")),
        ("b", None)
    ]))
    .is_empty());
    assert!(shared_engines(&hosts(&[("a", None), ("b", None)])).is_empty());
    let checks = shared_engines(&hosts(&[
        ("parent", Some("E1")),
        ("a", Some("E2")),
        ("b", Some("E1")),
    ]));
    assert_eq!(
        checks.iter().map(|c| c.host.as_str()).collect::<Vec<_>>(),
        ["parent", "b"]
    );
    assert!(checks.iter().all(|c| c.level == Level::Fail
        && c.id == "docker_shared_engine"
        && c.detail.contains("E1")
        && c.detail.contains("parent, b")));
}

fn status(loaded: Option<bool>, ready: bool, draining: bool) -> Status {
    Status {
        membership_only: false,
        coordinator_id: None,
        system_user: false,
        worker_id: WorkerId::parse("mini-a").unwrap(),
        cluster: ClusterId::generate(),
        binary_sha256: "a".repeat(64),
        loaded,
        ready,
        draining,
        cancel_running: false,
        live_attempts: Some(0),
        bridge_binary: Some("/w/bin/current/belt".into()),
        switching_from: None,
        drained_by_upgrade: false,
        switch_running: false,
        traffic_scope: None,
        traffic_hold: None,
    }
}

#[test]
fn the_service_check_says_what_the_host_showed() {
    let me = "a".repeat(64);
    let of = |s: &Status| {
        let checks = service_checks("mini-a", s, &me);
        (level(&checks, "service"), level(&checks, "version"))
    };
    assert_eq!(of(&status(Some(true), true, false)), (Level::Ok, Level::Ok));
    assert_eq!(of(&status(Some(true), true, true)).0, Level::Notice);
    assert_eq!(of(&status(Some(true), false, false)).0, Level::Fail);
    assert_eq!(of(&status(Some(false), false, false)).0, Level::Fail);
    assert_eq!(of(&status(Some(false), true, false)).0, Level::Fail);
    assert_eq!(of(&status(None, true, false)).0, Level::Fail);
    let mut switching = status(Some(true), true, false);
    switching.switching_from = Some("b".repeat(64));
    let checks = service_checks("mini-a", &switching, &me);
    assert_eq!(level(&checks, "service"), Level::Fail);
    assert!(detail(&checks, "service").contains("unfinished"));
    // While a switch runs, what the status shows is about to change.
    switching.switch_running = true;
    let checks = service_checks("mini-a", &switching, &me);
    assert_eq!(level(&checks, "service"), Level::NotChecked);
    assert!(detail(&checks, "service").contains("a version switch is running"));
    let mut running = status(Some(true), true, false);
    running.switch_running = true;
    assert_eq!(
        level(&service_checks("mini-a", &running, &me), "service"),
        Level::NotChecked
    );
    // Another build is worth saying, not a failure.
    let checks = service_checks("mini-a", &status(Some(true), true, false), &"b".repeat(64));
    assert_eq!(level(&checks, "version"), Level::Notice);
    assert!(
        detail(&checks, "version").contains("aaaaaaaaaaaa")
            && detail(&checks, "version").contains("bbbbbbbbbbbb")
    );
    assert!(detail(
        &service_checks("m", &status(Some(true), true, false), "short"),
        "version"
    )
    .contains("short"));
}

fn out(code: i32, stdout: &str) -> RemoteOutput {
    RemoteOutput {
        code: Some(code),
        stdout: stdout.as_bytes().to_vec(),
        stderr: vec![],
    }
}
fn answer_line(status: &Status, facts: &HostFacts) -> String {
    format!(
        "{}\n",
        serde_json::json!({"ok": true, "doctor": {"status": status, "facts": facts}})
    )
}

#[test]
fn every_way_of_not_getting_an_answer_is_a_reason() {
    let good = answer_line(&status(Some(true), true, false), &healthy());
    assert_eq!(parse_answer(&out(0, &good)).unwrap().facts, healthy());
    assert!(parse_answer(&out(255, &good))
        .unwrap_err()
        .contains("could not be reached"));
    // A helper from before `doctor` prints a usage error on stderr only.
    assert!(parse_answer(&out(2, ""))
        .unwrap_err()
        .contains("predates doctor"));
    assert!(parse_answer(&out(0, "noise\n"))
        .unwrap_err()
        .contains("unreadable"));
    assert!(parse_answer(&out(1, &good))
        .unwrap_err()
        .contains("refused"));
    assert!(
        parse_answer(&out(1, "{\"ok\":false,\"error\":\"no worker\"}\n"))
            .unwrap_err()
            .contains("no worker")
    );
    assert!(parse_answer(&out(0, "{\"ok\":true,\"doctor\":{}}\n"))
        .unwrap_err()
        .contains("facts were unreadable"));
}

struct FakeRemote {
    cluster: ClusterId,
    /// Per ssh destination: what the helper prints, or `None` when unreachable.
    hosts: BTreeMap<String, Option<(String, HostFacts, Status)>>,
    bridge_fails: bool,
    bridge_worker: Option<String>,
    /// What a reachable host prints for `worker-service env`.
    env_answer: String,
    shells: Vec<String>,
}
impl Remote for FakeRemote {
    fn run(
        &mut self,
        target: &SshTarget,
        shell: &str,
        _: &[u8],
        _: Duration,
    ) -> Result<RemoteOutput, String> {
        self.shells.push(shell.to_string());
        let key = format!("{target:?}");
        let host = self
            .hosts
            .iter()
            .find(|(ssh, _)| key.contains(ssh.split('@').next_back().unwrap()))
            .map(|(_, host)| host)
            .expect("a known destination");
        Ok(match host {
            None => out(255, ""),
            Some(_) if shell.ends_with("'worker-service' 'env'") => out(0, &self.env_answer),
            Some((_, facts, status)) => out(0, &answer_line(status, facts)),
        })
    }
    fn bridge_hello(
        &mut self,
        target: &SshTarget,
        _: &str,
        _: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        if self.bridge_fails {
            return Err("exit 127".into());
        }
        let key = format!("{target:?}");
        let name = self
            .hosts
            .iter()
            .find(|(ssh, _)| key.contains(ssh.split('@').next_back().unwrap()))
            .and_then(|(_, host)| host.as_ref().map(|(name, _, _)| name.clone()))
            .unwrap();
        let name = self.bridge_worker.clone().unwrap_or(name);
        Ok((self.cluster.clone(), WorkerId::parse(&name).unwrap()))
    }
}

struct World {
    host: Host,
    spec: Spec,
    remote: FakeRemote,
    runner: FakeRunner,
    version: String,
}
impl World {
    fn new(workers: &[(&str, &str, bool)]) -> Self {
        let host = Host::new();
        let cluster = ClusterId::generate();
        let mut config = String::from("schema_version = 1\n");
        let mut hosts = BTreeMap::new();
        let version = "a".repeat(64);
        for (name, ssh, with_binary) in workers {
            config.push_str(&format!("[[workers]]\nid = \"{name}\"\nssh = \"{ssh}\"\n"));
            if *with_binary {
                config.push_str("binary = \"/w/bin/current/belt\"\n");
            }
            let mut answer = status(Some(true), true, false);
            answer.worker_id = WorkerId::parse(name).unwrap();
            answer.cluster = cluster.clone();
            let mut facts = healthy();
            facts.docker.engine_id = Some(format!("engine-of-{name}"));
            hosts.insert(ssh.to_string(), Some((name.to_string(), facts, answer)));
        }
        let directory = host.home.join(".config/belt");
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("config.toml"), config).unwrap();
        let local_state = host.home.join("local-worker");
        fs::create_dir(&local_state).unwrap();
        fs::set_permissions(&local_state, fs::Permissions::from_mode(0o700)).unwrap();
        World {
            spec: Spec {
                state_dir: host.home.join("coordinator"),
                worker_state_dir: local_state,
                worker_id: WorkerId::parse("local").unwrap(),
                cluster_key_path: host.home.join("cluster.key"),
                executable: host.home.join("belt"),
                env: BTreeMap::new(),
                repo_root: None,
                expected_cluster: Some(cluster.clone()),
                managed_service: None,
            },
            remote: FakeRemote {
                cluster,
                hosts,
                bridge_fails: false,
                bridge_worker: None,
                env_answer: String::new(),
                shells: vec![],
            },
            runner: FakeRunner::default(),
            version,
            host,
        }
    }
    fn run(&mut self) -> Report {
        run(
            &mut self.remote,
            &mut self.runner,
            &self.host.env,
            &self.spec,
            &self.version,
            Backend::Launchd,
            None,
        )
        .unwrap()
    }
}
fn of<'a>(report: &'a Report, host: &str, id: &str) -> &'a Check {
    let found: Vec<_> = report
        .checks
        .iter()
        .filter(|c| c.host == host && c.id == id)
        .collect();
    assert_eq!(found.len(), 1, "{host} {id}: {:?}", report.checks);
    found[0]
}

#[test]
fn a_parent_alone_reports_its_own_worker_and_nothing_about_others() {
    let mut w = World::new(&[]);
    let report = w.run();
    assert!(report.checks.iter().all(|c| c.host == "parent"));
    // Nothing is installed in the temporary HOME: one scope in use, no CLI.
    assert_eq!(of(&report, "parent", "traffic_runtime").level, Level::Ok);
    assert_eq!(of(&report, "parent", "traffic_state_path").level, Level::Ok);
    assert_eq!(of(&report, "parent", "service_profile").level, Level::Ok);
    // The coordinator retains no service environment in this fixture.
    assert_eq!(
        of(&report, "parent", "service_environment").level,
        Level::Notice
    );
    assert!(report.healthy);
    assert_eq!((report.failed, report.not_checked), (0, 0));
    assert!(w.remote.shells.is_empty());
}

#[test]
fn the_retained_service_environment_is_the_one_that_is_checked() {
    let mut w = World::new(&[]);
    // The shell has the command on PATH; the service environment does not.
    w.host.tool("task-traffic-controller", 0o755);
    assert_eq!(of(&w.run(), "parent", "traffic_runtime").level, Level::Fail);
    w.spec.env = BTreeMap::from([
        ("HOME".to_string(), w.host.env["HOME"].clone()),
        ("PATH".to_string(), "/nonexistent".to_string()),
    ]);
    let report = w.run();
    assert_eq!(of(&report, "parent", "traffic_runtime").level, Level::Ok);
    assert_eq!(
        of(&report, "parent", "service_environment").level,
        Level::Ok
    );
}

#[test]
fn an_unpatched_command_on_the_parent_makes_the_report_unhealthy() {
    // A05 end to end on the parent: found on PATH, asked, graded.
    let mut w = World::new(&[]);
    w.host.tool("task-traffic-controller", 0o755);
    w.runner
        .answer("task-traffic-controller", "status", 0, r#"{"entries":[]}"#);
    let report = w.run();
    assert!(!report.healthy);
    let check = of(&report, "parent", "traffic_runtime");
    assert_eq!(check.level, Level::Fail);
    assert!(check.detail.contains("does not know managed entries"));
    // The worker shares the command's scope, so the scope check passes.
    assert_eq!(of(&report, "parent", "traffic_state_path").level, Level::Ok);
    assert_eq!(report.failed, 1);
    // The updated command passes the runtime check.
    w.runner.answer(
        "task-traffic-controller",
        "status",
        0,
        r#"{"ledger_format":1,"entries":[]}"#,
    );
    let report = w.run();
    assert_eq!(of(&report, "parent", "traffic_runtime").level, Level::Ok);
    assert_eq!(report.failed, 0);
}

#[test]
fn every_registered_worker_is_asked_through_its_registered_binary() {
    let mut w = World::new(&[
        ("mini-a", "ci@mini-a.local", true),
        ("mini-b", "ci@mini-b.local", true),
    ]);
    let report = w.run();
    assert_eq!(
        w.remote.shells,
        ["exec '/w/bin/current/belt' 'worker-service' 'doctor'"; 2]
    );
    for host in ["mini-a", "mini-b"] {
        for id in [
            "reachable",
            "service",
            "version",
            "bridge",
            "traffic_runtime",
        ] {
            assert_eq!(of(&report, host, id).level, Level::Ok, "{host} {id}");
        }
        // The slot for `workers diff` is listed, never passed silently.
        let slot = of(&report, host, "environment_diff");
        assert_eq!(slot.level, Level::NotChecked);
    }
    // Nothing failed, and the report is still not healthy: checks are missing.
    assert!(!report.healthy);
    let left_out = |id: &str| {
        report
            .checks
            .iter()
            .filter(|c| c.level == Level::NotChecked && c.id == id)
            .count()
    };
    assert_eq!(left_out("environment_diff"), 2);
    assert_eq!(
        (report.failed, report.not_checked),
        (0, left_out("environment_diff"))
    );
    let (code, message) = report.problem().unwrap();
    assert_eq!(code, ErrorCode::CommandFailed);
    assert!(message.starts_with(&format!("{} check(s) were not made", report.not_checked)));
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["healthy"], false);
    assert_eq!(json["checks"][0]["level"], "notice");
    assert!(json["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["level"] == "not_checked" && c["id"] == "environment_diff"));
    let printed = text(&report);
    assert!(printed.contains("[not checked] mini-a environment_diff: "));
    assert!(printed.contains("[ok] mini-b bridge: "));
    assert!(printed.contains("[notice] parent service_environment: "));
    assert!(printed.ends_with(&format!(
        "0 failed, {} not checked, {} checks",
        report.not_checked,
        report.checks.len()
    )));
}

#[test]
fn a_worker_that_cannot_be_looked_at_fails_and_is_not_graded() {
    let mut w = World::new(&[
        ("mini-a", "ci@mini-a.local", true),
        ("mini-b", "ci@mini-b.local", false),
    ]);
    w.remote.hosts.insert("ci@mini-a.local".into(), None);
    let report = w.run();
    assert!(!report.healthy);
    assert_eq!(report.failed, 2);
    let a = of(&report, "mini-a", "reachable");
    assert_eq!(a.level, Level::Fail);
    assert!(a.detail.contains("could not be reached"));
    let b = of(&report, "mini-b", "reachable");
    assert!(b.detail.contains("no binary path"));
    // No check about a host that gave no facts.
    assert!(!report
        .checks
        .iter()
        .any(|c| c.host != "parent" && !["reachable", "environment_diff"].contains(&&*c.id)));
    assert!(text(&report).contains("[FAIL] mini-a reachable: "));
    assert_eq!(w.remote.shells.len(), 1);
}

#[test]
fn an_answer_for_another_worker_or_cluster_is_refused() {
    let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
    if let Some(Some((_, _, status))) = w.remote.hosts.get_mut("ci@mini-a.local") {
        status.worker_id = WorkerId::parse("mini-z").unwrap();
    }
    let report = w.run();
    assert!(of(&report, "mini-a", "reachable")
        .detail
        .contains("different worker identity"));
    let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
    w.spec.expected_cluster = Some(ClusterId::generate());
    assert_eq!(of(&w.run(), "mini-a", "reachable").level, Level::Fail);
    w.spec.expected_cluster = None;
    let error = run(
        &mut w.remote,
        &mut w.runner,
        &w.host.env,
        &w.spec,
        &w.version,
        Backend::Launchd,
        None,
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
}

#[test]
fn a_bridge_that_fails_or_answers_as_someone_else_fails_the_bridge_check() {
    let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
    w.remote.bridge_fails = true;
    let report = w.run();
    let check = of(&report, "mini-a", "bridge");
    assert_eq!(check.level, Level::Fail);
    assert!(check.detail.contains("did not complete hello: exit 127"));
    // The rest of the host is still graded.
    assert_eq!(of(&report, "mini-a", "service").level, Level::Ok);
    w.remote.bridge_fails = false;
    w.remote.bridge_worker = Some("mini-z".into());
    let report = w.run();
    assert!(of(&report, "mini-a", "bridge")
        .detail
        .contains("different worker or cluster"));
    w.remote.bridge_worker = None;
    w.remote.cluster = ClusterId::generate();
    assert_eq!(of(&w.run(), "mini-a", "bridge").level, Level::Fail);
}

#[test]
fn workers_that_share_an_engine_with_each_other_or_the_parent_fail() {
    let mut w = World::new(&[
        ("mini-a", "ci@mini-a.local", true),
        ("mini-b", "ci@mini-b.local", true),
    ]);
    for host in w.remote.hosts.values_mut() {
        host.as_mut().unwrap().1.docker.engine_id = Some("shared".into());
    }
    let report = w.run();
    assert_eq!(report.failed, 2);
    for host in ["mini-a", "mini-b"] {
        assert!(of(&report, host, "docker_shared_engine")
            .detail
            .contains("mini-a, mini-b"));
    }
}

#[test]
fn another_line_can_put_its_checks_where_the_slot_is() {
    let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
    let asked = std::sync::Mutex::new(Vec::new());
    let source = |_: &mut dyn Remote, host: &str| {
        asked.lock().unwrap().push(host.to_string());
        vec![Check::new(
            host,
            "environment_diff",
            Level::Fail,
            "node differs",
        )]
    };
    let report = run(
        &mut w.remote,
        &mut w.runner,
        &w.host.env,
        &w.spec,
        &w.version,
        Backend::Launchd,
        Some(&source),
    )
    .unwrap();
    assert_eq!(*asked.lock().unwrap(), ["mini-a"]);
    let check = of(&report, "mini-a", "environment_diff");
    assert_eq!(
        (check.level, check.detail.as_str()),
        (Level::Fail, "node differs")
    );
    assert_eq!((report.failed, report.healthy), (1, false));
    assert!(!report
        .checks
        .iter()
        .any(|c| c.id == "environment_diff" && c.level == Level::NotChecked));
    assert_eq!(report.problem().unwrap().0, ErrorCode::Blocked);
}

#[test]
fn an_endpoint_is_local_only_as_a_socket_or_a_pipe() {
    assert!(local_endpoint("unix:///var/run/docker.sock"));
    assert!(local_endpoint("npipe:////./pipe/docker_engine"));
    assert!(!local_endpoint("tcp://127.0.0.1:2375"));
    assert!(!local_endpoint("ssh://host"));
    assert!(!local_endpoint(""));
}

#[test]
fn the_system_runner_returns_the_exit_code_and_output_in_the_given_environment() {
    let env = BTreeMap::from([("DOCTOR_TEST".to_string(), "seen".to_string())]);
    let mut runner = SystemRunner::default();
    assert_eq!(runner.timeout, Duration::from_secs(10));
    assert_eq!(
        runner.run(
            Path::new("/bin/sh"),
            &["-c", "echo $DOCTOR_TEST$HOME; exit 3"],
            &env
        ),
        Some((3, "seen\n".into()))
    );
    assert_eq!(runner.run(Path::new("/nonexistent/tool"), &[], &env), None);
}

#[test]
fn a_worker_host_is_viewed_through_its_recorded_service_environment() {
    use crate::service::worker_service::{install, service_view, tests::Fake, InstallRequest};
    let h = Host::new();
    let exe = h.bin.join("belt");
    fs::write(&exe, b"pretend binary").unwrap();
    let fake = Fake::default();
    fake.0.borrow_mut().state = h.state.clone();
    let mut env = h.env.clone();
    env.insert("NOT_FOR_THE_SERVICE".into(), "x".into());
    // Nothing installed: there is no service to look at.
    assert!(service_view(&env, fake.clone()).is_err());
    let request = InstallRequest {
        membership: None,
        system_user: false,
        schema: 1,
        cluster: ClusterId::generate(),
        worker_id: WorkerId::parse("mini-a").unwrap(),
        key_file_text: format!("{}\n", "ab".repeat(64)),
        binary_sha256: crate::service::worker_service::sha256_file(&exe).unwrap(),
    };
    install(&env, &request, &exe, fake.clone(), Duration::from_secs(2)).unwrap();
    let (status, state, service_env, recorded) = service_view(&env, fake).unwrap();
    assert_eq!(status.worker_id.as_str(), "mini-a");
    assert_eq!(status.loaded, Some(true));
    assert_eq!(state, h.state);
    // What the LaunchAgent was given: nothing else from the calling shell.
    assert_eq!(service_env, h.env);
    let facts = gather(&state, &service_env, &recorded, &mut FakeRunner::default());
    assert_eq!(facts.profile.error, None);
    let line = answer_line(&status, &facts);
    let parsed = parse_answer(&out(0, &line)).unwrap();
    assert_eq!((parsed.status, parsed.facts), (status, facts));
}

#[test]
fn unsupported_legacy_keys_are_noticed_only_when_they_carry_a_value() {
    use crate::config::resolve::Notice;
    assert!(project_checks(Ok(&[])).is_empty());
    let notices = [Notice {
        key: "link-node-modules",
        file: "/r/.local-ci.toml".into(),
        reason: "not supported".into(),
    }];
    let checks = project_checks(Ok(&notices));
    assert_eq!(checks.len(), 1);
    assert_eq!(
        (
            checks[0].host.as_str(),
            checks[0].id.as_str(),
            checks[0].level
        ),
        ("project", "legacy_config", Level::Notice)
    );
    assert_eq!(
        checks[0].detail,
        "`link-node-modules` in /r/.local-ci.toml: not supported"
    );
    let checks = project_checks(Err("bad toml".into()));
    assert_eq!(checks[0].level, Level::NotChecked);
    assert!(checks[0].detail.contains("bad toml"));
}

#[test]
fn a_command_that_does_not_finish_is_stopped_and_counts_as_unanswered() {
    let mut runner = SystemRunner {
        timeout: Duration::from_millis(200),
    };
    let started = Instant::now();
    assert_eq!(
        runner.run(
            Path::new("/bin/sh"),
            &["-c", "exec sleep 30"],
            &BTreeMap::new()
        ),
        None
    );
    assert!(started.elapsed() < Duration::from_secs(10));
    // One that finishes inside the limit is answered.
    assert_eq!(
        runner.run(Path::new("/bin/sh"), &["-c", "exit 0"], &BTreeMap::new()),
        Some((0, String::new()))
    );
}

#[test]
fn a_report_is_healthy_only_when_nothing_failed_and_nothing_was_left_out() {
    let check = |level| Check::new("h", "c", level, "d");
    let report = Report::of(vec![check(Level::Ok), check(Level::Notice)]);
    assert!(report.healthy);
    assert_eq!(report.problem(), None);
    // One check that was not made is enough, and it is not called a failure.
    let report = Report::of(vec![check(Level::Ok), check(Level::NotChecked)]);
    assert!(!report.healthy);
    assert_eq!((report.failed, report.not_checked), (0, 1));
    assert_eq!(
        report.problem(),
        Some((
            ErrorCode::CommandFailed,
            "1 check(s) were not made; nothing failed, but the report is incomplete".into()
        ))
    );
    // A failure is the answer whatever else is missing.
    let report = Report::of(vec![
        check(Level::Fail),
        check(Level::NotChecked),
        check(Level::Fail),
    ]);
    assert!(!report.healthy);
    assert_eq!(
        report.problem(),
        Some((ErrorCode::Blocked, "2 check(s) failed".into()))
    );
    assert_ne!(
        ErrorCode::Blocked.exit_code(),
        ErrorCode::CommandFailed.exit_code()
    );
    let report = Report::of(vec![check(Level::Fail)]);
    assert!(!report.healthy && report.not_checked == 0);
}

#[test]
fn the_jobs_of_the_command_are_judged_from_the_facts() {
    let jobs = |live, stale| Some(CliJobs { live, stale });
    let judged = |facts: &HostFacts| {
        let checks = evaluate("mini-a", facts);
        (
            level(&checks, "traffic_processes"),
            detail(&checks, "traffic_processes").to_string(),
        )
    };
    // Nothing runs, nothing is left over.
    let mut facts = healthy();
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::Ok);
    assert!(d.contains("no job of the command is running"));
    // Jobs run under a command that knows managed entries: noted.
    facts.cli_jobs = jobs(2, 1);
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::Notice);
    assert!(d.contains("2 job(s)") && d.contains("1 left-over"));
    // The same under a command that does not know them, or cannot be asked.
    facts.cli.as_mut().unwrap().knows_managed = false;
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::Fail);
    assert!(d.contains("2 job(s)") && d.contains("update the command"));
    facts.cli.as_mut().unwrap().knows_managed = true;
    facts.cli.as_mut().unwrap().status_readable = false;
    assert_eq!(judged(&facts).0, Level::Fail);
    // The command is gone but its jobs still run: noted, not failed.
    facts.cli = None;
    assert_eq!(judged(&facts).0, Level::Notice);
    // Only left-over entries: noted.
    facts.cli_jobs = jobs(0, 3);
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::Notice);
    assert!(d.contains("3 entries") && d.contains("no live owner"));
    // Not read: said, with the reason.
    facts.cli_jobs = None;
    facts.cli_scope.ledger_entries = None;
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::NotChecked);
    assert!(d.contains("ledger is unreadable"));
    facts.cli_scope.ledger_entries = Some(0);
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::NotChecked);
    assert!(d.contains("predates the check"));
    facts.cli_jobs_error = Some("permission denied".into());
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::NotChecked);
    assert!(d.contains("could not be read (permission denied)") && !d.contains("predates"));
    // Neither a command nor state: nothing could be running.
    facts.cli_scope.in_use = false;
    let (l, d) = judged(&facts);
    assert_eq!(l, Level::Ok);
    assert!(d.contains("none of its jobs can be running"));
}

fn listing(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut todo = vec![root.to_path_buf()];
    while let Some(dir) = todo.pop() {
        let Ok(read) = fs::read_dir(&dir) else {
            continue;
        };
        for e in read.flatten() {
            out.push(e.path());
            if e.path().is_dir() {
                todo.push(e.path());
            }
        }
    }
    out.sort();
    out
}

fn ledger_entry(id: &str, parent: Option<&str>) -> task_traffic_controller::ledger::Entry {
    let mut v = serde_json::json!({
        "id": id, "pid": 1, "state": "running", "front": false, "weight": 1,
        "predicted_rss": 0, "repo": "r", "argv": "a", "key": "k", "seq": 1,
        "enqueued_at": "2026-01-01T00:00:00Z"
    });
    if let Some(p) = parent {
        v["parent"] = p.into();
    }
    serde_json::from_value(v).unwrap()
}

#[test]
fn the_jobs_of_a_scope_are_read_from_leases_and_sidecars_without_writing() {
    let h = Host::new();
    let (state, config) = cli_scope_paths(&h.env);
    // No ledger: nothing runs, and nothing is created.
    assert_eq!(cli_jobs(&state, &config), Ok(CliJobs::default()));
    assert!(!state.exists());
    let paths = Paths::new(&state, &config);
    let context = Context::new(Paths::new(&state, &config));
    fs::create_dir_all(paths.managed_dir()).unwrap();
    let ledger = Ledger {
        version: 3,
        entries: vec![
            ledger_entry("held", None),
            ledger_entry("gone", None),
            ledger_entry("managed", None),
            ledger_entry("nested", Some("managed")),
            ledger_entry("deeper", Some("nested")),
        ],
        ..Ledger::default()
    };
    ledger.save(&paths.ledger_file()).unwrap();
    // `held` has an owner, `gone` has none, `managed` is belt's, and what
    // runs inside it is belt's too.
    let _lease = lock::take_lease(&context, "held").unwrap();
    let _nested = lock::take_lease(&context, "nested").unwrap();
    fs::write(paths.managed_file("managed"), "{}").unwrap();
    let before = listing(&state);
    assert_eq!(cli_jobs(&state, &config), Ok(CliJobs { live: 1, stale: 1 }));
    assert_eq!(listing(&state), before);
    // A ledger nobody can read is no answer, and says why.
    fs::write(paths.ledger_file(), "not a ledger").unwrap();
    assert!(cli_jobs(&state, &config).is_err());
}

#[test]
fn a_scope_with_a_ledger_but_no_lock_or_lease_files_gets_none_created() {
    let h = Host::new();
    let (state, config) = cli_scope_paths(&h.env);
    let paths = Paths::new(&state, &config);
    let ledger = Ledger {
        entries: vec![ledger_entry("plain", None)],
        ..Ledger::default()
    };
    ledger.save(&paths.ledger_file()).unwrap();
    let before = listing(&state);
    // A live, sidecar-less entry is a foreign job; with no lease it is stale.
    assert_eq!(cli_jobs(&state, &config), Ok(CliJobs { live: 0, stale: 1 }));
    assert_eq!(listing(&state), before);
    assert!(!paths.ledger_lock().exists());
    assert!(!paths.lease_file("plain").exists());
    // With the owner's lease it is live, under a command that knows managed entries.
    let context = Context::new(Paths::new(&state, &config));
    let _lease = lock::take_lease(&context, "plain").unwrap();
    assert_eq!(cli_jobs(&state, &config), Ok(CliJobs { live: 1, stale: 0 }));
}

mod environment_diff {
    use super::*;
    use crate::toolchain::envdiff::{Class, Docker, EnvReport, Item, Observed, ToolFact};

    fn item(subject: &str, class: Class) -> Item {
        Item {
            subject: subject.into(),
            class,
            parent: Some("22.1.0".into()),
            worker: None,
            steps: (class != Class::Same).then(|| "do this".to_string()),
        }
    }

    fn environment(node: &str) -> EnvReport {
        EnvReport {
            schema_version: envdiff::SCHEMA,
            os: "macos".into(),
            arch: "aarch64".into(),
            tools: BTreeMap::from([(
                "node".to_string(),
                ToolFact {
                    observed: Observed::Available,
                    version: Some(node.into()),
                    managed: None,
                },
            )]),
            helpers: BTreeMap::new(),
            docker: Docker::NotRunnable,
            disk_free_bytes: Some(envdiff::MIN_DISK_BYTES),
            profile: "default".into(),
            required_tools: vec![],
            profile_digest: None,
            policy_digest: Some("sha256:t".into()),
            rustup: Vec::new(),
        }
    }

    fn answer(report: &EnvReport) -> String {
        serde_json::json!({"ok": true, "env": report}).to_string()
    }

    #[test]
    fn every_class_becomes_one_check_and_only_what_a_person_must_fix_fails() {
        let items = [
            item("platform", Class::Same),
            item("node", Class::Differs),
            item("git", Class::Missing),
            item("flutter", Class::AutoInstallable),
            item("xcode", Class::ManualRequired),
            item("policy_digest", Class::NotMeasured),
            item("belt", Class::Differs),
            item("disk", Class::Missing),
            item("profile", Class::Differs),
            item("policy_digest", Class::Differs),
            item("platform", Class::Differs),
        ];
        let checks = diff_checks("mini-a", Ok(&items));
        assert!(checks
            .iter()
            .all(|c| c.host == "mini-a" && c.id == "environment_diff"));
        let levels: Vec<_> = checks.iter().map(|c| c.level).collect();
        assert_eq!(
            levels,
            [
                Level::Ok,
                Level::Notice,
                Level::Notice,
                Level::Notice,
                Level::Notice,
                Level::Notice,
                Level::Fail,
                Level::Fail,
                Level::Fail,
                Level::Fail,
                Level::Fail,
            ]
        );
        assert!(checks[1]
            .detail
            .ends_with("(a notice: tasks that need it are placed on a member that has it)"));
        assert_eq!(checks[0].detail, "platform: same parent=22.1.0 worker=-");
        assert_eq!(
            checks[3].detail,
            "flutter: auto_installable parent=22.1.0 worker=- -> do this (an exec installs it; `belt workers sync mini-a` installs it now)"
        );
        assert_eq!(
            checks[5].detail,
            "policy_digest: not_measured parent=22.1.0 worker=- -> do this (not measured yet)"
        );
    }

    /// The two Macs of a working group (doctor-run6): other tool versions,
    /// tools neither side could measure, Xcode with only the Command Line
    /// Tools and docker with no engine on the worker. Nothing fails; every
    /// row is still shown.
    #[test]
    fn a_healthy_two_mac_group_has_notices_and_no_failure() {
        let fact = |observed, version: Option<&str>| ToolFact {
            observed,
            version: version.map(str::to_string),
            managed: None,
        };
        let mut parent = environment("22.23.5");
        let mut worker = environment("-");
        let tools = |r: &mut EnvReport, rows: &[(&str, Observed, Option<&str>)]| {
            r.tools = rows
                .iter()
                .map(|(n, o, v)| (n.to_string(), fact(*o, *v)))
                .collect();
        };
        use Observed::{Available as A, Unavailable as U, Unknown as K};
        tools(
            &mut parent,
            &[
                ("cargo", A, Some("1.92.0")),
                ("rustc", A, Some("1.92.0")),
                ("git", A, Some("2.56.0")),
                ("node", A, Some("22.23.5")),
                ("npm", A, Some("10.9.9")),
                ("xcode", A, Some("27.1")),
                ("dart", K, None),
                ("flutter", K, None),
                ("pnpm", K, None),
                ("yarn", K, None),
            ],
        );
        tools(
            &mut worker,
            &[
                ("cargo", A, Some("1.100.0")),
                ("rustc", A, Some("1.100.0")),
                ("git", A, Some("2.54.0")),
                ("node", U, None),
                ("npm", U, None),
                ("xcode", K, None),
                ("dart", K, None),
                ("flutter", K, None),
                ("pnpm", K, None),
                ("yarn", K, None),
            ],
        );
        parent.docker = Docker::Runnable;
        worker.docker = Docker::Unknown;
        parent.helpers.insert("mise".into(), Some("present".into()));
        worker.helpers.insert("mise".into(), None);
        let items = envdiff::diff(&parent, &worker);
        let checks = diff_checks("mini", Ok(&items));
        assert_eq!(checks.len(), items.len());
        let failed: Vec<_> = checks.iter().filter(|c| c.level == Level::Fail).collect();
        assert!(failed.is_empty(), "{failed:#?}");
        let row = |subject: &str| {
            checks
                .iter()
                .find(|c| c.detail.starts_with(&format!("{subject}: ")))
                .unwrap()
                .detail
                .clone()
        };
        assert!(row("dart").starts_with("dart: not_measured parent=unknown worker=unknown"));
        assert!(row("node").starts_with("node: missing parent=22.23.5 worker=-"));
        assert!(
            row("xcode").contains("only the Command Line Tools"),
            "{}",
            row("xcode")
        );
        assert!(
            row("docker").contains("no Docker engine is running"),
            "{}",
            row("docker")
        );
        assert!(row("mise").starts_with("mise: missing parent=present worker=absent"));
    }

    #[test]
    fn a_registered_worker_with_nothing_to_fix_keeps_the_exit_at_zero() {
        let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
        // No separate task-traffic-controller on the worker: nothing else is left out.
        let (_, facts, _) = w
            .remote
            .hosts
            .get_mut("ci@mini-a.local")
            .unwrap()
            .as_mut()
            .unwrap();
        facts.cli = None;
        facts.cli_scope.in_use = false;
        let source = |_: &mut dyn Remote, host: &str| {
            diff_checks(
                host,
                Ok(&[
                    item("platform", Class::Same),
                    item("flutter", Class::AutoInstallable),
                    item("policy_digest", Class::NotMeasured),
                ]),
            )
        };
        let report = run(
            &mut w.remote,
            &mut w.runner,
            &w.host.env,
            &w.spec,
            &w.version,
            Backend::Launchd,
            Some(&source),
        )
        .unwrap();
        let left: Vec<_> = report
            .checks
            .iter()
            .filter(|c| c.level == Level::NotChecked)
            .collect();
        assert!(left.is_empty(), "{left:?}");
        assert!(report.problem().is_none(), "{:?}", report.problem());
        // The same list with something a person must fix does fail.
        let source = |_: &mut dyn Remote, host: &str| {
            diff_checks(host, Ok(&[item("profile", Class::Differs)]))
        };
        let report = run(
            &mut w.remote,
            &mut w.runner,
            &w.host.env,
            &w.spec,
            &w.version,
            Backend::Launchd,
            Some(&source),
        )
        .unwrap();
        assert!(report.problem().is_some());
    }

    #[test]
    fn the_parent_is_measured_in_the_retained_environment_as_workers_diff_does() {
        let w = World::new(&[]);
        let mut seen = None;
        let result = measure_parent(&w.spec, |state, env| {
            seen = Some((state.to_path_buf(), env.clone()));
            Err("no profile".into())
        });
        assert_eq!(result.unwrap_err(), "no profile");
        assert_eq!(
            seen,
            Some((w.spec.worker_state_dir.clone(), w.spec.env.clone()))
        );
    }

    #[test]
    fn what_the_worker_reported_cannot_rewrite_the_terminal() {
        let mut one = item("node", Class::Differs);
        one.worker = Some("20\u{1b}[2J\n[ok] parent".into());
        let checks = diff_checks("mini-a", Ok(&[one]));
        assert_eq!(
            checks[0].detail,
            "node: differs parent=22.1.0 worker=20[2J[ok] parent -> do this (a notice: tasks that need it are placed on a member that has it)"
        );
        let refused = diff_checks("mini-a", Err("refused\u{7}\nnow"));
        assert_eq!(
            refused[0].detail,
            "the environments were not compared: refusednow"
        );
    }

    #[test]
    fn a_difference_that_was_not_taken_is_never_an_empty_list_or_a_pass() {
        for measured in [Err("no answer"), Ok(&[][..])] {
            let checks = diff_checks("mini-a", measured);
            assert_eq!(checks.len(), 1, "{measured:?}");
            assert_eq!(checks[0].level, Level::NotChecked);
            assert_eq!(
                (checks[0].host.as_str(), checks[0].id.as_str()),
                ("mini-a", "environment_diff")
            );
        }
        assert_eq!(
            diff_checks("mini-a", Err("no answer"))[0].detail,
            "the environments were not compared: no answer"
        );
        assert_eq!(
            diff_checks("mini-a", Ok(&[]))[0].detail,
            "the environments were not compared: the comparison listed nothing"
        );
    }

    #[test]
    fn the_worker_is_asked_for_its_environment_and_the_difference_is_listed() {
        let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
        let exe = w.host.home.join("belt");
        fs::write(&exe, "binary").unwrap();
        let parent = environment("22.1.0");
        w.remote.env_answer = answer(&environment("20.0.0"));
        let checks = worker_diff(&mut w.remote, &w.host.env, &exe, Ok(&parent), "mini-a");
        assert_eq!(
            w.remote.shells,
            ["exec '/w/bin/current/belt' 'worker-service' 'env'"]
        );
        let subject = |name: &str| {
            let found: Vec<_> = checks
                .iter()
                .filter(|c| c.detail.starts_with(&format!("{name}: ")))
                .collect();
            assert_eq!(found.len(), 1, "{name}: {checks:?}");
            found[0]
        };
        assert_eq!(subject("platform").level, Level::Ok);
        let node = subject("node");
        assert_eq!(node.level, Level::Notice);
        assert!(
            node.detail
                .starts_with("node: differs parent=22.1.0 worker=20.0.0 -> "),
            "{}",
            node.detail
        );
        // The same environment on both sides: nothing fails.
        w.remote.env_answer = answer(&parent);
        let same = worker_diff(&mut w.remote, &w.host.env, &exe, Ok(&parent), "mini-a");
        assert!(same.len() > 1);
        assert!(same.iter().all(|c| c.level == Level::Ok), "{same:?}");
    }

    #[test]
    fn a_worker_that_gives_no_report_is_listed_as_not_compared() {
        let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
        let exe = w.host.home.join("belt");
        fs::write(&exe, "binary").unwrap();
        let parent = environment("22.1.0");
        // A binary that predates the action prints nothing.
        let checks = worker_diff(&mut w.remote, &w.host.env, &exe, Ok(&parent), "mini-a");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].level, Level::NotChecked);
        assert!(
            checks[0].detail.starts_with(
                "the environments were not compared: the worker's binary gave no environment report"
            ),
            "{}",
            checks[0].detail
        );
    }

    #[test]
    fn without_the_parent_s_measurement_no_worker_is_asked() {
        let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
        let exe = w.host.home.join("belt");
        let checks = worker_diff(
            &mut w.remote,
            &w.host.env,
            &exe,
            Err("profile unreadable"),
            "mini-a",
        );
        assert!(w.remote.shells.is_empty());
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].level, Level::NotChecked);
        assert_eq!(
            checks[0].detail,
            "the environments were not compared: the parent's environment could not be measured (profile unreadable)"
        );
    }

    #[test]
    fn a_worker_that_did_not_answer_is_not_asked_for_its_environment_again() {
        let mut w = World::new(&[
            ("mini-a", "ci@mini-a.local", true),
            ("mini-b", "ci@mini-b.local", true),
        ]);
        *w.remote.hosts.get_mut("ci@mini-b.local").unwrap() = None;
        let asked = std::sync::Mutex::new(Vec::new());
        let source = |_: &mut dyn Remote, host: &str| {
            asked.lock().unwrap().push(host.to_string());
            diff_checks(host, Ok(&[item("platform", Class::Same)]))
        };
        let report = run(
            &mut w.remote,
            &mut w.runner,
            &w.host.env,
            &w.spec,
            &w.version,
            Backend::Launchd,
            Some(&source),
        )
        .unwrap();
        assert_eq!(*asked.lock().unwrap(), ["mini-a"]);
        assert_eq!(of(&report, "mini-a", "environment_diff").level, Level::Ok);
        let unreachable = of(&report, "mini-b", "environment_diff");
        assert_eq!(unreachable.level, Level::NotChecked);
        assert!(unreachable.detail.starts_with("the worker did not answer"));
        assert_eq!(of(&report, "mini-b", "reachable").level, Level::Fail);
    }
}

#[test]
fn only_a_systemd_host_is_asked_about_lingering_and_no_answer_is_not_a_pass() {
    let h = Host::new();
    // launchd has no lingering: nothing is run, nothing is reported.
    let mut runner = FakeRunner::default();
    assert_eq!(
        linger_fact(Backend::Launchd, 501, &h.env, &mut runner),
        None
    );
    assert!(runner.asked.is_empty());
    // systemd: one question, in the service environment, about this user.
    let mut runner = FakeRunner::default();
    runner.answer("loginctl", "show-user", 0, "Linger=no\n");
    assert_eq!(
        linger_fact(Backend::Systemd, 1000, &h.env, &mut runner),
        Some(Linger {
            uid: 1000,
            enabled: Some(false)
        })
    );
    assert_eq!(
        runner.asked,
        [(
            "/usr/bin/loginctl".to_string(),
            vec![
                "show-user".to_string(),
                "1000".to_string(),
                "--property=Linger".to_string()
            ]
        )]
    );
    runner.answer("loginctl", "show-user", 0, "Linger=yes\n");
    assert_eq!(
        linger_fact(Backend::Systemd, 1000, &h.env, &mut runner)
            .unwrap()
            .enabled,
        Some(true)
    );
    // The one on the service PATH is the one that is asked.
    h.tool("loginctl", 0o755);
    let mut runner = FakeRunner::default();
    runner.answer("loginctl", "show-user", 1, "Linger=yes\n");
    let failed = linger_fact(Backend::Systemd, 7, &h.env, &mut runner).unwrap();
    assert_eq!((failed.uid, failed.enabled), (7, None));
    assert_eq!(
        runner.asked[0].0,
        h.bin.join("loginctl").to_string_lossy().into_owned()
    );
    // A loginctl that cannot be run at all.
    let silent = linger_fact(Backend::Systemd, 7, &h.env, &mut FakeRunner::default()).unwrap();
    assert_eq!(silent.enabled, None);
}

#[test]
fn lingering_is_one_more_check_of_a_host_that_reported_it() {
    assert!(evaluate("mini-l", &healthy())
        .iter()
        .all(|c| c.id != "linger"));
    let mut facts = healthy();
    facts.linger = Some(Linger {
        uid: 1000,
        enabled: Some(false),
    });
    let checks = evaluate("mini-l", &facts);
    assert_eq!(level(&checks, "linger"), Level::Notice);
    assert!(detail(&checks, "linger").contains("loginctl enable-linger 1000"));
    // A warning changes neither count of the report; an unanswered
    // question is one more check that was not made.
    let base = Report::of(evaluate("mini-l", &healthy()));
    let warned = Report::of(checks);
    assert_eq!(
        (warned.failed, warned.not_checked, warned.healthy),
        (base.failed, base.not_checked, base.healthy)
    );
    facts.linger = Some(Linger {
        uid: 1000,
        enabled: None,
    });
    let report = Report::of(evaluate("mini-l", &facts));
    assert_eq!(
        (report.failed, report.not_checked),
        (base.failed, base.not_checked + 1)
    );
    assert!(!report.healthy);
    // A helper that predates the check sends no such field, and a host
    // without the fact sends none either.
    let line = answer_line(&status(Some(true), true, false), &healthy());
    assert!(!line.contains("linger"));
    assert_eq!(parse_answer(&out(0, &line)).unwrap().facts.linger, None);
    let line = answer_line(&status(Some(true), true, false), &facts);
    assert_eq!(
        parse_answer(&out(0, &line)).unwrap().facts.linger,
        facts.linger
    );
}

#[test]
fn the_parent_is_asked_about_lingering_only_when_init_registered_its_service_with_systemd() {
    let run_as = |backend: Backend, managed: bool| {
        let mut w = World::new(&[]);
        w.runner.answer("loginctl", "show-user", 0, "Linger=no\n");
        if managed {
            w.spec.managed_service = Some(w.host.home.join("init.json"));
        }
        let report = run(
            &mut w.remote,
            &mut w.runner,
            &w.host.env,
            &w.spec,
            &w.version,
            backend,
            None,
        )
        .unwrap();
        let asked = w.runner.asked.iter().any(|(_, a)| a[0] == "show-user");
        (report, asked)
    };
    let (report, asked) = run_as(Backend::Systemd, true);
    assert!(asked);
    let linger = of(&report, "parent", "linger");
    assert_eq!(linger.level, Level::Notice);
    assert!(
        linger.detail.contains("loginctl enable-linger"),
        "{}",
        linger.detail
    );
    assert!(report.healthy);
    for (backend, managed) in [
        (Backend::Systemd, false),
        (Backend::Launchd, true),
        (Backend::Launchd, false),
    ] {
        let (report, asked) = run_as(backend, managed);
        assert!(!asked, "{backend:?} {managed}");
        assert!(report.checks.iter().all(|c| c.id != "linger"));
    }
}

#[test]
fn a_linux_worker_whose_lingering_is_off_is_warned_about_with_the_fix() {
    let mut w = World::new(&[("mini-l", "ci@mini-l.local", true)]);
    if let Some(Some((_, facts, _))) = w.remote.hosts.get_mut("ci@mini-l.local") {
        facts.linger = Some(Linger {
            uid: 1001,
            enabled: Some(false),
        });
    }
    let report = w.run();
    let linger = of(&report, "mini-l", "linger");
    assert_eq!(linger.level, Level::Notice);
    assert!(
        linger.detail.ends_with("run: loginctl enable-linger 1001"),
        "{}",
        linger.detail
    );
    assert!(report
        .checks
        .iter()
        .all(|c| c.host != "parent" || c.id != "linger"));
    assert_eq!(report.failed, 0);
}

fn ephemeral_world() -> World {
    let w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
    let config = w.host.home.join(".config/belt/config.toml");
    let mut text = fs::read_to_string(&config).unwrap();
    text.push_str("[[workers]]\nid = \"colab-1\"\nssh = \"colab-tunnel\"\nephemeral = true\n");
    fs::write(&config, text).unwrap();
    w
}

#[test]
fn an_ephemeral_worker_is_only_asked_for_hello_and_never_for_its_service() {
    let mut w = ephemeral_world();
    w.remote.hosts.insert(
        "colab-tunnel".into(),
        Some(("colab-1".into(), healthy(), status(Some(true), true, false))),
    );
    let report = w.run();
    assert_eq!(of(&report, "colab-1", "service").level, Level::Notice);
    assert!(of(&report, "colab-1", "service")
        .detail
        .contains("no service"));
    assert_eq!(of(&report, "colab-1", "bridge").level, Level::Ok);
    // Nothing else is said about it: no reachability, version or host facts.
    let said: Vec<&str> = report
        .checks
        .iter()
        .filter(|c| c.host == "colab-1")
        .map(|c| c.id.as_str())
        .collect();
    assert_eq!(said, ["service", "bridge"]);
    // The helper of the persistent worker ran; none ran on the ephemeral one.
    assert_eq!(w.remote.shells.len(), 1);
    assert_eq!(report.failed, 0);
    assert_eq!(of(&report, "mini-a", "service").level, Level::Ok);

    // It answers as somebody else: that is wrong, not merely absent.
    let mut w = ephemeral_world();
    w.remote.hosts.insert(
        "colab-tunnel".into(),
        Some(("other".into(), healthy(), status(Some(true), true, false))),
    );
    let report = w.run();
    assert_eq!(of(&report, "colab-1", "bridge").level, Level::Fail);
}

#[test]
fn an_ephemeral_worker_that_is_not_running_is_a_notice_and_its_environment_is_not_compared() {
    let mut w = ephemeral_world();
    w.remote.hosts.insert("colab-tunnel".into(), None);
    w.remote.bridge_fails = true;
    let asked = std::sync::Mutex::new(Vec::new());
    let source = |_: &mut dyn Remote, host: &str| {
        asked.lock().unwrap().push(host.to_string());
        Vec::new()
    };
    let report = run(
        &mut w.remote,
        &mut w.runner,
        &w.host.env,
        &w.spec,
        &w.version,
        Backend::Launchd,
        Some(&source),
    )
    .unwrap();
    let bridge = of(&report, "colab-1", "bridge");
    assert_eq!(bridge.level, Level::Notice);
    assert!(
        bridge.detail.contains("workers forget colab-1"),
        "{}",
        bridge.detail
    );
    assert!(report
        .checks
        .iter()
        .all(|c| c.host != "colab-1" || c.id != "environment_diff"));
    // The persistent worker's bridge fails the report; the ephemeral one adds nothing to it.
    assert_eq!(report.failed, 1);
    assert_eq!(of(&report, "mini-a", "bridge").level, Level::Fail);
    assert_eq!(*asked.lock().unwrap(), ["mini-a"]);

    // Nor is one that answers: it has no installed helper to report with.
    let mut w = ephemeral_world();
    w.remote.hosts.insert(
        "colab-tunnel".into(),
        Some(("colab-1".into(), healthy(), status(Some(true), true, false))),
    );
    let asked = std::sync::Mutex::new(Vec::new());
    let source = |_: &mut dyn Remote, host: &str| {
        asked.lock().unwrap().push(host.to_string());
        Vec::new()
    };
    run(
        &mut w.remote,
        &mut w.runner,
        &w.host.env,
        &w.spec,
        &w.version,
        Backend::Launchd,
        Some(&source),
    )
    .unwrap();
    assert_eq!(*asked.lock().unwrap(), ["mini-a"]);
}

/// Hands out copies of itself so workers are asked together, and holds each
/// kind of ask until all of that kind are in flight: a run that asks one
/// at a time fails the meeting instead of passing.
struct Together {
    doctor: std::sync::Arc<crate::service::workers::meeting::Gate>,
    after: std::sync::Arc<crate::service::workers::meeting::Gate>,
    inner: std::sync::Arc<std::sync::Mutex<FakeRemote>>,
}
impl Remote for Together {
    fn split(&self) -> Option<Box<dyn Remote + Send>> {
        Some(Box::new(Together {
            doctor: self.doctor.clone(),
            after: self.after.clone(),
            inner: self.inner.clone(),
        }))
    }
    fn run(
        &mut self,
        target: &SshTarget,
        shell: &str,
        stdin: &[u8],
        timeout: Duration,
    ) -> Result<RemoteOutput, String> {
        if shell.ends_with("'worker-service' 'doctor'") {
            self.doctor.meet()?;
        } else if shell == "env-probe" {
            self.after.meet()?;
            return Ok(out(0, ""));
        }
        self.inner
            .lock()
            .unwrap()
            .run(target, shell, stdin, timeout)
    }
    fn bridge_hello(
        &mut self,
        target: &SshTarget,
        binary: &str,
        cluster: &ClusterId,
    ) -> Result<(ClusterId, WorkerId), String> {
        self.after.meet()?;
        self.inner
            .lock()
            .unwrap()
            .bridge_hello(target, binary, cluster)
    }
}

#[test]
fn workers_and_the_questions_after_the_first_one_are_asked_at_the_same_time() {
    use crate::service::workers::meeting::Gate;
    let mut w = World::new(&[
        ("mini-a", "ci@mini-a.local", true),
        ("mini-b", "ci@mini-b.local", true),
        ("mini-c", "ci@mini-c.local", true),
    ]);
    let mut together = Together {
        // Three doctor asks, then three hello asks and three comparisons.
        doctor: Gate::new(3),
        after: Gate::new(6),
        inner: std::sync::Arc::new(std::sync::Mutex::new(std::mem::replace(
            &mut w.remote,
            FakeRemote {
                cluster: ClusterId::generate(),
                hosts: BTreeMap::new(),
                bridge_fails: false,
                bridge_worker: None,
                env_answer: String::new(),
                shells: vec![],
            },
        ))),
    };
    let target = SshTarget::parse("ci@x").unwrap();
    let source = |remote: &mut dyn Remote, host: &str| {
        let met = remote.run(&target, "env-probe", &[], Duration::from_secs(1));
        vec![Check::new(
            host,
            "environment_diff",
            if met.is_ok() { Level::Ok } else { Level::Fail },
            "compared",
        )]
    };
    let report = run(
        &mut together,
        &mut w.runner,
        &w.host.env,
        &w.spec,
        &w.version,
        Backend::Launchd,
        Some(&source),
    )
    .unwrap();
    for name in ["mini-a", "mini-b", "mini-c"] {
        assert_eq!(of(&report, name, "reachable").level, Level::Ok, "{name}");
        assert_eq!(of(&report, name, "bridge").level, Level::Ok, "{name}");
        assert_eq!(
            of(&report, name, "environment_diff").level,
            Level::Ok,
            "{name}"
        );
    }
    // The order of the report is the order of the registrations.
    let hosts: Vec<_> =
        report
            .checks
            .iter()
            .map(|c| c.host.as_str())
            .fold(Vec::new(), |mut seen, h| {
                if seen.last() != Some(&h) {
                    seen.push(h);
                }
                seen
            });
    assert_eq!(hosts[..4], ["parent", "mini-a", "mini-b", "mini-c"][..]);
}

#[test]
fn doctor_reports_a_mismatch_between_registered_and_installed_service_modes() {
    let mut w = World::new(&[("mini-a", "ci@mini-a.local", true)]);
    w.remote
        .hosts
        .get_mut("ci@mini-a.local")
        .unwrap()
        .as_mut()
        .unwrap()
        .2
        .system_user = true;
    let report = w.run();
    let reachable = of(&report, "mini-a", "reachable");
    assert_eq!(reachable.level, Level::Fail);
    assert!(reachable.detail.contains("installed service mode differs"));
}

#[test]
fn removed_members_warn_without_registration_and_remain_after_failed_probe() {
    let mut w = World::new(&[("gone", "ci@gone.local", true)]);
    fs::write(
        w.host.home.join(".config/belt/config.toml"),
        "schema_version=1\n",
    )
    .unwrap();
    let path = w.spec.worker_state_dir.join("removed-members.json");
    fs::write(
        &path,
        r#"{"gone":{"removed_at_ms":1,"ssh":"ci@gone.local"},"legacy":2}"#,
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let report = w.run();
    let warning = of(&report, "gone", "removed_member_ssh_reachable");
    assert_eq!(warning.level, Level::Notice);
    assert!(text(&report).contains("authorized_keys"));
    assert!(serde_json::to_string(&report)
        .unwrap()
        .contains(&warning.id));
    assert_eq!(w.remote.shells, ["true"]);
    w.remote.hosts.insert("ci@gone.local".into(), None);
    let report = w.run();
    assert!(!report.checks.iter().any(|c| c.host == "gone"));
    assert!(of(&report, "legacy", "removed_member_ssh_unknown")
        .detail
        .contains("cannot be checked"));
    assert!(path.exists());
}

/// Spec 25.5: a run whose pruning began and did not reach its end is kept
/// as it is and counted; no such run is no line at all.
#[test]
fn runs_whose_pruning_is_not_finished_are_counted() {
    use crate::source::retention::Gate;
    assert_eq!(pruning_report(Some(0), Some(Gate::Capture)), None);
    let (level, detail) = pruning_report(Some(3), None).unwrap();
    assert_eq!(level, Level::Notice);
    assert!(
        detail.starts_with("3 run(s) past their retention"),
        "{detail}"
    );
    assert!(!detail.contains("gate="), "{detail}");
    // The gate of the source store that holds their inputs is named.
    let (_, detail) = pruning_report(Some(3), Some(Gate::LegacyRegistration)).unwrap();
    assert!(detail.ends_with("(gate=legacy_registration)"), "{detail}");
    assert_eq!(pruning_report(None, None).unwrap().0, Level::NotChecked);
}

#[test]
fn wrong_content_files_kept_because_another_root_holds_them_are_shown() {
    let none = UnjudgedCaptures::default();
    assert_eq!(capture_detail(&none).0, Level::Ok);
    let held = UnjudgedCaptures {
        corrupt_held: 2,
        ..Default::default()
    };
    let (level, detail) = capture_detail(&held);
    assert_eq!(level, Level::Notice);
    assert!(
        detail.contains("2 file(s)") && detail.contains("cannot remove them while they are held"),
        "{detail}"
    );
    assert_eq!(
        capture_detail(&UnjudgedCaptures {
            elsewhere: 1,
            ..none
        })
        .0,
        Level::Notice
    );
}

#[test]
fn what_released_import_tokens_still_hold_is_counted() {
    let mut facts = healthy();
    facts.abandoned_imports = Some(AbandonedImports {
        roots: 0,
        members: 0,
        bytes: 0,
        bytes_truncated: false,
        unreadable_tokens: 0,
        large_old_claims: 0,
        gc_failing: 0,
    });
    assert!(evaluate("mini-a", &facts)
        .iter()
        .all(|c| c.id != "abandoned_imports"));
    facts.abandoned_imports = Some(AbandonedImports {
        roots: 2,
        members: 5,
        bytes: 900,
        bytes_truncated: false,
        unreadable_tokens: 0,
        large_old_claims: 0,
        gc_failing: 0,
    });
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "abandoned_imports"), Level::Notice);
    let text = detail(&checks, "abandoned_imports");
    assert!(
        text.contains("2 released") && text.contains("5 membership") && text.contains("900 byte")
    );
    assert!(!text.contains("at least") && !text.contains("cannot be read"));
    // a truncated total says it is a lower bound; unreadable token records
    // are counted, and alone they raise the notice
    facts.abandoned_imports = Some(AbandonedImports {
        roots: 0,
        members: 0,
        bytes: 0,
        bytes_truncated: true,
        unreadable_tokens: 3,
        large_old_claims: 0,
        gc_failing: 0,
    });
    let checks = evaluate("mini-a", &facts);
    let text = detail(&checks, "abandoned_imports");
    assert!(text.contains("at least 0 byte") && text.contains("3 incoming source(s)"));
    assert!(checks.iter().all(|c| c.id != "source_old_claims"));
}

/// Review round 1 (L3): a large undone claim of the older shape, which may
/// block imports for good, is shown.
#[test]
fn a_large_old_claim_is_shown() {
    let mut facts = healthy();
    facts.abandoned_imports = Some(AbandonedImports {
        roots: 0,
        members: 0,
        bytes: 0,
        bytes_truncated: false,
        unreadable_tokens: 0,
        large_old_claims: 2,
        gc_failing: 0,
    });
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "source_old_claims"), Level::Notice);
    assert!(detail(&checks, "source_old_claims").contains("2 deletion claim"));
    assert!(checks.iter().all(|c| c.id != "source_clone_gc"));
}

/// Review round 1, L6: clones whose gc keeps failing are shown.
#[test]
fn a_failing_clone_gc_is_shown() {
    let mut facts = healthy();
    facts.abandoned_imports = Some(AbandonedImports {
        roots: 0,
        members: 0,
        bytes: 0,
        bytes_truncated: false,
        unreadable_tokens: 0,
        large_old_claims: 0,
        gc_failing: 2,
    });
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "source_clone_gc"), Level::Notice);
    assert!(detail(&checks, "source_clone_gc").contains("gc of 2 clone(s)"));
}

#[test]
fn a_source_lock_that_cannot_be_taken_fails_the_worker() {
    let mut facts = healthy();
    assert!(evaluate("mini-a", &facts)
        .iter()
        .all(|c| c.id != "source_lock"));
    facts.source_lock_problem = Some("is not a plain file".into());
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "source_lock"), Level::Fail);
    let text = detail(&checks, "source_lock");
    assert!(text.contains("is not a plain file") && text.contains("INTERNAL"));
}

#[test]
fn a_path_gap_names_the_command_that_recaptures_it_for_the_parent_and_a_worker() {
    let mut facts = healthy();
    facts.service_path = Some(env_gaps::PathFacts {
        login: Some("/opt/homebrew/bin:/usr/bin".into()),
        gaps: vec![env_gaps::PathGap {
            dir: "/opt/homebrew/bin".into(),
            tools: vec!["node".into()],
        }],
    });
    let parent = evaluate("parent", &facts);
    assert!(
        detail(&parent, "service_path").contains("run `belt upgrade --refresh-env`"),
        "{parent:?}"
    );
    let worker = evaluate("mini", &facts);
    assert!(
        detail(&worker, "service_path").contains("run `belt workers refresh-env mini`"),
        "{worker:?}"
    );
}
