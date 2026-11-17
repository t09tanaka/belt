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
        let state = home.join(".remote-task-runner");
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
                supervisor_exe: PathBuf::from("/bin/rtr"),
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
    fs::write(&config, "budget = 3\n").unwrap();
    let ours = scope(&state, &config).policy_digest.unwrap();
    let context = task_traffic_controller::Context::new(Paths::new(&state, &config));
    let theirs = task_traffic_controller::inspect::inspect(&context).unwrap();
    assert_eq!(ours, theirs.policy_digest);
    // Another budget is another policy; a broken file has no digest.
    fs::write(&config, "budget = 4\n").unwrap();
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
    // Everything that was looked at passes; what was not is said, not passed.
    assert!(checks.iter().all(|c| c.host == "mini-a"
        && c.level
            == if c.id == "traffic_processes" {
                Level::NotChecked
            } else {
                Level::Ok
            }));
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
        worker_id: WorkerId::parse("mini-a").unwrap(),
        cluster: ClusterId::generate(),
        binary_sha256: "a".repeat(64),
        loaded,
        ready,
        draining,
        cancel_running: false,
        live_attempts: Some(0),
        bridge_binary: Some("/w/bin/current/remote-task-runner".into()),
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
                config.push_str("binary = \"/w/bin/current/remote-task-runner\"\n");
            }
            let mut answer = status(Some(true), true, false);
            answer.worker_id = WorkerId::parse(name).unwrap();
            answer.cluster = cluster.clone();
            let mut facts = healthy();
            facts.docker.engine_id = Some(format!("engine-of-{name}"));
            hosts.insert(ssh.to_string(), Some((name.to_string(), facts, answer)));
        }
        let directory = host.home.join(".config/remote-task-runner");
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
                executable: host.home.join("rtr"),
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
        ["exec '/w/bin/current/remote-task-runner' 'worker-service' 'doctor'"; 2]
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
        (
            0,
            left_out("environment_diff") + left_out("traffic_processes")
        )
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
    let exe = h.bin.join("rtr");
    fs::write(&exe, b"pretend binary").unwrap();
    let fake = Fake::default();
    fake.0.borrow_mut().state = h.state.clone();
    let mut env = h.env.clone();
    env.insert("NOT_FOR_THE_SERVICE".into(), "x".into());
    // Nothing installed: there is no service to look at.
    assert!(service_view(&env, fake.clone()).is_err());
    let request = InstallRequest {
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
fn running_processes_of_the_command_are_said_to_be_unexamined() {
    // The command is installed: its jobs may run, and nobody looked.
    let mut facts = healthy();
    facts.cli_scope.ledger_entries = Some(3);
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "traffic_processes"), Level::NotChecked);
    assert!(detail(&checks, "traffic_processes").contains("are not examined (3 entries"));
    facts.cli_scope.ledger_entries = None;
    assert!(detail(&evaluate("mini-a", &facts), "traffic_processes").contains("unreadable"));
    // No command, but it left state behind: the same.
    facts.cli = None;
    assert_eq!(
        level(&evaluate("mini-a", &facts), "traffic_processes"),
        Level::NotChecked
    );
    // Neither a command nor state: there is nothing that could be running.
    facts.cli_scope.in_use = false;
    let checks = evaluate("mini-a", &facts);
    assert_eq!(level(&checks, "traffic_processes"), Level::Ok);
    assert!(detail(&checks, "traffic_processes").contains("none of its jobs can be running"));
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
            item("ruby", Class::Missing),
            item("flutter", Class::AutoInstallable),
            item("xcode", Class::ManualRequired),
            item("policy_digest", Class::NotMeasured),
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
                Level::Fail,
                Level::Fail,
                Level::Notice,
                Level::Fail,
                Level::Notice
            ]
        );
        assert_eq!(checks[0].detail, "platform: same parent=22.1.0 worker=-");
        assert_eq!(
            checks[3].detail,
            "flutter: auto_installable parent=22.1.0 worker=- -> do this (an exec installs it; `remote-task-runner workers sync mini-a` installs it now)"
        );
        assert_eq!(
            checks[5].detail,
            "policy_digest: not_measured parent=22.1.0 worker=- -> do this (not measured yet)"
        );
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
        let source =
            |_: &mut dyn Remote, host: &str| diff_checks(host, Ok(&[item("node", Class::Missing)]));
        let report = run(
            &mut w.remote,
            &mut w.runner,
            &w.host.env,
            &w.spec,
            &w.version,
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
            "node: differs parent=22.1.0 worker=20[2J[ok] parent -> do this"
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
        let exe = w.host.home.join("rtr");
        fs::write(&exe, "binary").unwrap();
        let parent = environment("22.1.0");
        w.remote.env_answer = answer(&environment("20.0.0"));
        let checks = worker_diff(&mut w.remote, &w.host.env, &exe, Ok(&parent), "mini-a");
        assert_eq!(
            w.remote.shells,
            ["exec '/w/bin/current/remote-task-runner' 'worker-service' 'env'"]
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
        assert_eq!(node.level, Level::Fail);
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
        let exe = w.host.home.join("rtr");
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
        let exe = w.host.home.join("rtr");
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
