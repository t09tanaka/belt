use super::*;

fn definition() -> Definition {
    Definition {
        label: "org.example.worker".into(),
        mode: Mode::User { uid: 1000 },
        executable: "/opt/example/bin/runner".into(),
        arguments: vec!["worker".into(), "--input".into(), "/cfg/in.json".into()],
        environment: BTreeMap::from([
            ("HOME".into(), "/home/u".into()),
            ("PATH".into(), "/usr/bin".into()),
        ]),
        working_directory: "/cfg".into(),
    }
}

fn words(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

#[test]
fn a_user_unit_is_rendered_exactly() {
    let plan = render(&definition()).unwrap();
    assert_eq!(
        plan.plist(),
        "[Unit]\nDescription=remote-task-runner worker\n\n[Service]\nType=simple\n\
ExecStart=\"/opt/example/bin/runner\" \"worker\" \"--input\" \"/cfg/in.json\"\n\
WorkingDirectory=/cfg\nEnvironment=\"HOME=/home/u\"\nEnvironment=\"PATH=/usr/bin\"\n\
Restart=always\nRestartSec=5\nTimeoutStopSec=60\n\n[Install]\nWantedBy=default.target\n"
    );
    assert_eq!(plan.filename(), "org.example.worker.service");
    assert_eq!(plan.label(), "org.example.worker");
    assert_eq!(plan.domain(), "--user");
    assert_eq!(plan.service_target(), "org.example.worker.service");
    assert_eq!(
        plan.arguments(),
        [
            "/opt/example/bin/runner",
            "worker",
            "--input",
            "/cfg/in.json"
        ]
    );
    assert_eq!(plan.executable(), Path::new("/opt/example/bin/runner"));
    assert_eq!(plan.working_directory(), Path::new("/cfg"));
    assert_eq!(plan.environment().len(), 2);
}

#[test]
fn a_system_user_unit_names_a_non_root_user() {
    let mut d = definition();
    d.mode = Mode::SystemUser {
        uid: 1001,
        username: "worker_user-2".into(),
    };
    let plan = render(&d).unwrap();
    assert!(plan
        .plist()
        .contains("\nUser=worker_user-2\nRestart=always\n"));
    assert!(plan.plist().ends_with("WantedBy=multi-user.target\n"));
    assert_eq!(plan.domain(), "--system");
    // Not a user unit: no `User=` there, and it follows the login.
    assert!(!render(&definition()).unwrap().plist().contains("User="));
}

#[test]
fn start_and_stop_arguments_name_the_scope_and_the_unit() {
    let plan = render(&definition()).unwrap();
    let file = Path::new("/home/u/.config/systemd/user/org.example.worker.service");
    assert_eq!(
        plan.start_arguments(file).unwrap(),
        ["--user", "start", "org.example.worker.service"]
    );
    assert_eq!(
        plan.stop_arguments(file).unwrap(),
        ["--user", "stop", "org.example.worker.service"]
    );
    assert!(plan.start_arguments(Path::new("relative")).is_err());
    assert!(plan.stop_arguments(Path::new("relative")).is_err());
}

#[test]
fn specifiers_variables_quotes_and_backslashes_are_escaped() {
    let mut d = definition();
    d.arguments = vec!["a%b$c\"d\\e f".into()];
    d.environment = BTreeMap::from([("PATH".into(), "/a%b:/c$d\"e\\".into())]);
    d.working_directory = "/work dir/100%".into();
    let unit = render(&d).unwrap().plist().to_owned();
    assert!(unit.contains("\"a%%b$$c\\\"d\\\\e f\"\n"), "{unit}");
    assert!(
        unit.contains("Environment=\"PATH=/a%%b:/c$d\\\"e\\\\\"\n"),
        "{unit}"
    );
    assert!(
        unit.contains("WorkingDirectory=/work dir/100%%\n"),
        "{unit}"
    );
}

#[test]
fn invalid_inputs_are_refused() {
    for label in ["", ".x", "x/y", "x y", &"x".repeat(129)] {
        let mut d = definition();
        d.label = label.into();
        assert!(render(&d).is_err(), "{label}");
    }
    for label in ["x", "a_b-2.c", &"x".repeat(128)] {
        let mut d = definition();
        d.label = label.into();
        assert!(render(&d).is_ok(), "{label}");
    }
    for mode in [
        Mode::User { uid: 0 },
        Mode::SystemUser {
            uid: 0,
            username: "worker".into(),
        },
        Mode::SystemUser {
            uid: 1,
            username: "root".into(),
        },
        Mode::SystemUser {
            uid: 1,
            username: "".into(),
        },
        Mode::SystemUser {
            uid: 1,
            username: "bad name\nUser=root".into(),
        },
    ] {
        let mut d = definition();
        d.mode = mode;
        assert!(render(&d).is_err());
    }
    for path in ["relative", "/x/../y", "/x/\n", "/x/\0"] {
        let mut d = definition();
        d.executable = path.into();
        assert!(render(&d).is_err(), "{path}");
        let mut d = definition();
        d.working_directory = path.into();
        assert!(render(&d).is_err(), "{path}");
    }
    use std::os::unix::ffi::OsStringExt;
    let mut d = definition();
    d.executable = PathBuf::from(std::ffi::OsString::from_vec(vec![b'/', 255]));
    assert!(render(&d).is_err());
    for key in ["", "1A", "A-B", "XDG_RUNTIME_DIR", "TOKEN"] {
        let mut d = definition();
        d.environment.insert(key.into(), "v".into());
        assert!(render(&d).is_err(), "{key}");
    }
    let mut d = definition();
    d.arguments.push("a\nb".into());
    assert!(render(&d).is_err());
    let mut d = definition();
    d.environment.insert("LANG".into(), "x\ty".into());
    assert!(render(&d).is_err());
}

#[test]
fn units_live_below_the_config_home() {
    assert_eq!(
        user_unit_dir(Path::new("/home/u/.config")),
        PathBuf::from("/home/u/.config/systemd/user")
    );
}

#[test]
fn a_start_reloads_enables_then_starts_and_a_stop_only_stops() {
    let unit = "rtr.worker.service";
    assert_eq!(
        steps(&words(&["--user", "start", unit])).unwrap(),
        [
            words(&["--user", "daemon-reload"]),
            words(&["--user", "enable", unit]),
            words(&["--user", "start", unit]),
        ]
    );
    assert_eq!(
        steps(&words(&["--system", "start", unit])).unwrap(),
        [words(&["--system", "start", unit])]
    );
    assert_eq!(
        steps(&words(&["--system", "stop", unit])).unwrap(),
        [words(&["--system", "stop", unit])]
    );
    for bad in [
        words(&[]),
        words(&["--user", "start"]),
        words(&["--user", "start", unit, "extra"]),
        words(&["--global", "start", unit]),
        words(&["--user", "restart", unit]),
        words(&["--user", "start", "x.timer"]),
        words(&["--user", "start", ".service"]),
        words(&["--user", "start", "a b.service"]),
        words(&["--user", "start", "../x.service"]),
    ] {
        assert!(steps(&bad).is_err(), "{bad:?}");
    }
}

#[test]
fn show_is_asked_for_exactly_the_properties_it_accepts() {
    assert_eq!(
        show_arguments("--user", "u.service"),
        [
            "--user",
            "show",
            "u.service",
            "--property=LoadState,ActiveState,FragmentPath,MainPID,NeedDaemonReload"
        ]
    );
}

#[test]
fn the_runtime_directory_and_bus_are_given_or_derived() {
    let env = |r: Option<&str>, b: Option<&str>| systemctl_env(r, b, 1000);
    assert_eq!(
        env(None, None),
        [
            ("XDG_RUNTIME_DIR".to_string(), "/run/user/1000".to_string()),
            (
                "DBUS_SESSION_BUS_ADDRESS".to_string(),
                "unix:path=/run/user/1000/bus".to_string()
            )
        ]
    );
    assert_eq!(
        env(Some("/run/x"), Some("unix:path=/b")),
        [
            ("XDG_RUNTIME_DIR".to_string(), "/run/x".to_string()),
            (
                "DBUS_SESSION_BUS_ADDRESS".to_string(),
                "unix:path=/b".to_string()
            )
        ]
    );
    // A relative or odd value is not trusted; the derived one is used.
    assert_eq!(env(Some("run/x"), None)[0].1, "/run/user/1000");
    assert_eq!(env(Some("/run/\nx"), None)[0].1, "/run/user/1000");
    assert_eq!(env(None, Some(""))[1].1, "unix:path=/run/user/1000/bus");
    assert_eq!(env(None, Some("a\nb"))[1].1, "unix:path=/run/user/1000/bus");
    assert_eq!(env(Some("/run/x"), None)[1].1, "unix:path=/run/x/bus");
}

fn show(load: &str, active: &str, fragment: &str, pid: u32, reload: &str) -> Vec<u8> {
    format!(
        "LoadState={load}\nActiveState={active}\nFragmentPath={fragment}\nMainPID={pid}\nNeedDaemonReload={reload}\n"
    )
    .into_bytes()
}

const FILE: &str = "/home/u/.config/systemd/user/u.service";

#[test]
fn show_output_is_read_strictly() {
    let ok = parse_show(&show("loaded", "active", FILE, 42, "no")).unwrap();
    assert_eq!(
        ok,
        Show {
            system: false,
            load: "loaded".into(),
            active: "active".into(),
            fragment: FILE.into(),
            main_pid: 42,
            need_reload: false,
        }
    );
    assert!(
        parse_show(&show("loaded", "active", FILE, 42, "yes"))
            .unwrap()
            .need_reload
    );
    // Property order does not matter.
    assert!(parse_show(
        b"NeedDaemonReload=no\nMainPID=0\nFragmentPath=\nActiveState=inactive\nLoadState=not-found\n"
    )
    .is_ok());
    for bad in [
        &b""[..],
        b"LoadState=loaded\n",
        b"garbage\n",
        b"\xff\xfe",
        b"Extra=1\nLoadState=loaded\nActiveState=active\nFragmentPath=x\nMainPID=1\nNeedDaemonReload=no\n",
        b"LoadState=loaded\nLoadState=loaded\nActiveState=active\nFragmentPath=x\nMainPID=1\nNeedDaemonReload=no\n",
        b"LoadState=loaded\nActiveState=active\nFragmentPath=x\nMainPID=-1\nNeedDaemonReload=no\n",
        b"LoadState=loaded\nActiveState=active\nFragmentPath=x\nMainPID=1\nNeedDaemonReload=maybe\n",
    ] {
        assert!(parse_show(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
    }
}

#[test]
fn only_this_file_running_without_a_pending_reload_matches() {
    let file = Path::new(FILE);
    let state = |l: &str, a: &str, f: &str, r: &str| {
        parse_show(&show(l, a, f, 7, r)).unwrap().classify(file)
    };
    for active in ["active", "activating", "reloading"] {
        assert_eq!(state("loaded", active, FILE, "no"), LoadedState::Match);
        // Another file, or a file changed since systemd read it.
        assert_eq!(
            state("loaded", active, "/other.service", "no"),
            LoadedState::Unknown
        );
        assert_eq!(state("loaded", active, FILE, "yes"), LoadedState::Unknown);
    }
    for active in ["inactive", "failed"] {
        assert_eq!(state("loaded", active, FILE, "no"), LoadedState::Absent);
        assert_eq!(state("loaded", active, "", "yes"), LoadedState::Absent);
    }
    assert_eq!(
        state("not-found", "inactive", "", "no"),
        LoadedState::Absent
    );
    assert_eq!(state("not-found", "active", "", "no"), LoadedState::Unknown);
    assert_eq!(
        state("loaded", "deactivating", FILE, "no"),
        LoadedState::Unknown
    );
    assert_eq!(state("masked", "inactive", "", "no"), LoadedState::Unknown);
    assert_eq!(state("error", "failed", "", "no"), LoadedState::Unknown);
}

#[test]
fn a_running_pid_is_named_only_for_an_active_unit_with_a_real_process() {
    let pid = |a: &str, p: u32| {
        parse_show(&show("loaded", a, FILE, p, "no"))
            .unwrap()
            .running_pid()
    };
    assert_eq!(pid("active", 2), Some(2));
    assert_eq!(pid("active", 1), None);
    assert_eq!(pid("active", 0), None);
    assert_eq!(pid("activating", 99), None);
    assert_eq!(pid("inactive", 99), None);
}

#[test]
fn a_system_worker_requires_the_effective_account_and_persistent_activation() {
    let show = show("loaded", "active", FILE, 3, "no");
    let mut reply = String::from_utf8(show).unwrap();
    reply.push_str("User=build\nDropInPaths=\nUnitFileState=enabled\n");
    let read = |s: &str| parse_show_for(s.as_bytes(), Some("build"));
    assert_eq!(
        read(&reply).unwrap().classify(Path::new(FILE)),
        LoadedState::Match
    );
    for foreign in [
        reply.replace("User=build", "User=root"),
        reply.replace("DropInPaths=", "DropInPaths=/run/override.conf"),
        reply.replace("UnitFileState=enabled", "UnitFileState=enabled-runtime"),
        reply.replace("User=build\n", ""),
        reply.replace("DropInPaths=\n", ""),
        reply.replace("UnitFileState=enabled\n", ""),
    ] {
        assert!(read(&foreign).is_err(), "{foreign}");
    }
    let inactive = reply.replace("ActiveState=active", "ActiveState=inactive");
    assert_eq!(
        read(&inactive).unwrap().classify(Path::new(FILE)),
        LoadedState::Absent
    );
    for stale in [
        inactive.replace(FILE, "/run/other"),
        inactive.replace("NeedDaemonReload=no", "NeedDaemonReload=yes"),
    ] {
        assert_eq!(
            read(&stale).unwrap().classify(Path::new(FILE)),
            LoadedState::Unknown
        );
    }
    let args = show_arguments("--system", "a.service");
    assert_eq!(
        args.last().unwrap(),
        &format!("--property={PROPERTIES},User,DropInPaths,UnitFileState")
    );
}
