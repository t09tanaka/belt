use super::*;
use crate::service::macos::policy::{render, Definition};
// Layout provenance: parent read-only observations of Apple standard GUI and
// UserName daemon jobs on 2026-11-14. All paths/names/counts below are generic.
// This tests the observed print grammar; actual managed install/stop is untested.
fn definition() -> Definition {
    Definition {
        label: "org.example.worker".into(),
        mode: Mode::User { uid: 501 },
        executable: "/opt/example/runner".into(),
        arguments: vec![
            "daemon".into(),
            "--input".into(),
            "/var/example/private/input.json".into(),
        ],
        working_directory: "/var/example".into(),
        environment: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
    }
}
fn fixture() -> String {
    concat!("gui/501/org.example.worker = {\n",
        "\tpath = /opt/example/org.example.worker.plist\n\ttype = LaunchAgent\n\tstate = running\n",
        "\tprogram = /opt/example/runner\n\targuments = {\n\t\t/opt/example/runner\n\t\tdaemon\n\t\t--input\n\t\t/var/example/private/input.json\n\t}\n",
        "\tinherited environment = {\n\t\tSSH_AUTH_SOCK => /var/run/com.apple.launchd.example/Listeners\n\t}\n",
        "\tdefault environment = {\n\t\tPATH => /usr/bin\n\t}\n",
        "\tenvironment = {\n\t\tXPC_SERVICE_NAME => org.example.worker\n\t}\n",
        "\tdomain = gui/501 [100001]\n\tasid = 100001\n\n",
        "\tsemaphores = {\n\t\tsuccessful exit => 0\n\t\tprovides events => 1\n\t}\n",
        "\tendpoints = {\n\t\t\"org.example.endpoint\" = {\n\t\t\tport = 0x100\n\t\t\tactive = 1\n\t\t\tmanaged = 1\n\t\t\treset = 0\n\t\t\thide = 0\n\t\t\twatching = 0\n\t\t}\n\t}\n",
        "\tpid-local endpoints = {\n\t}\n",
        "\tresource coalition = {\n\t\tID = 100\n\t\ttype = resource\n\t\tstate = active\n\t\tactive count = 1\n\t\tname = org.example.worker\n\t}\n",
        "\tjetsam coalition = {\n\t}\n",
        "\tresource limits = {\n\t\tmaxfiles (soft) => 512\n\t\tmaxfiles (hard) => 512\n\t}\n",
        "}\n").into()
}
fn parsed(s: &str) -> Observed {
    parse(s.as_bytes()).unwrap()
}
fn path() -> &'static Path {
    Path::new("/opt/example/org.example.worker.plist")
}
#[test]
fn observed_layout_and_identity() {
    let p = render(&definition()).unwrap();
    let f = fixture();
    let o = parsed(&f);
    assert_eq!(o.username(), None);
    assert_eq!(o.verify(&p, path(), None), Ok(()));
    assert_eq!(
        p.arguments(),
        [
            "/opt/example/runner",
            "daemon",
            "--input",
            "/var/example/private/input.json"
        ]
    );
    assert_eq!(p.environment(), &definition().environment);
    assert_eq!(p.mode(), &Mode::User { uid: 501 });
    assert_eq!(p.working_directory(), Path::new("/var/example"));
    assert_eq!(p.service_target(), "gui/501/org.example.worker");
    assert!(p.stop_arguments(Path::new("relative")).is_err());
    assert_eq!(o.arguments.len(), 4);
    assert_eq!(o.fields["path"], path().to_str().unwrap());
    assert_eq!(o.environment["XPC_SERVICE_NAME"], "org.example.worker");
    assert_eq!(o.defaults["PATH"], "/usr/bin");
    assert_eq!(
        o.inherited["SSH_AUTH_SOCK"],
        "/var/run/com.apple.launchd.example/Listeners"
    );
    let mut missing_domain = parsed(&f);
    missing_domain.fields.remove("domain");
    assert!(missing_domain.verify(&p, path(), None).is_err());
    let cwd = f.replace(
        "\tdomain =",
        "\tworking directory = /var/example\n\tdomain =",
    );
    assert_eq!(parsed(&cwd).verify(&p, path(), None), Ok(()));
    assert!(
        parsed(&cwd.replace("directory = /var/example", "directory = /foreign"))
            .verify(&p, path(), None)
            .is_err()
    );
    for (old, new) in [
        ("gui/501/org.example.worker", "gui/502/org.example.worker"),
        (
            "path = /opt/example/org.example.worker.plist",
            "path = /foreign.plist",
        ),
        ("program = /opt/example/runner", "program = /foreign"),
        ("\t\tdaemon\n", "\t\tforeign\n"),
        ("\t\t--input\n", "\t\t/var/example/private/input.json\n"),
        ("domain = gui/501 [100001]", "domain = gui/502 [100001]"),
        ("domain = gui/501 [100001]", "domain = gui/501"),
        ("[100001]", "[]"),
        ("[100001]", "[abc]"),
        ("asid = 100001", "asid = 100002"),
        ("PATH => /usr/bin", "PATH => /foreign"),
    ] {
        assert!(
            parsed(&f.replace(old, new))
                .verify(&p, path(), None)
                .is_err(),
            "{old}"
        );
    }
    for session in ["0", "0001", "4294967296"] {
        assert!(parsed(&f.replace("100001", session))
            .verify(&p, path(), None)
            .is_err());
    }
    for prefix in ["DYLD_", "LD_", "__XPC_DYLD_"] {
        for block in [
            "environment",
            "default environment",
            "inherited environment",
        ] {
            let bad = f.replace(
                &format!("\t{block} = {{\n"),
                &format!("\t{block} = {{\n\t\t{prefix}INJECT => x\n"),
            );
            assert!(parsed(&bad).verify(&p, path(), None).is_err());
        }
    }
    for block in ["environment", "inherited environment"] {
        let good = f.replace("\t\tPATH => /usr/bin\n", "").replace(
            &format!("\t{block} = {{\n"),
            &format!("\t{block} = {{\n\t\tPATH => /usr/bin\n"),
        );
        assert_eq!(parsed(&good).verify(&p, path(), None), Ok(()));
    }
    assert!(parsed(&f.replace("\t\tPATH => /usr/bin\n", ""))
        .verify(&p, path(), None)
        .is_err());
}
#[test]
fn user_identity_is_not_installer_identity() {
    let f = fixture().replace("\tdomain =", "\tusername = worker_user\n\tdomain =");
    let gui = render(&definition()).unwrap();
    let o = parsed(&f);
    assert_eq!(o.username(), Some("worker_user"));
    assert_eq!(o.verify(&gui, path(), Some(("worker_user", 501))), Ok(()));
    for user in [None, Some(("other", 501)), Some(("worker_user", 502))] {
        assert!(o.verify(&gui, path(), user).is_err());
    }
    let mut d = definition();
    d.mode = Mode::SystemUser {
        uid: 502,
        username: "worker_user".into(),
    };
    let system = render(&d).unwrap();
    let f = f
        .replace("gui/501/org.example.worker", "system/org.example.worker")
        .replace("domain = gui/501 [100001]", "domain = system")
        .replace("\tinherited environment = {\n\t\tSSH_AUTH_SOCK => /var/run/com.apple.launchd.example/Listeners\n\t}\n", "");
    let o = parsed(&f);
    assert_eq!(
        o.verify(&system, path(), Some(("worker_user", 502))),
        Ok(())
    );
    for user in [None, Some(("other", 502)), Some(("worker_user", 501))] {
        assert!(o.verify(&system, path(), user).is_err());
    }
    assert!(parsed(&f.replace("\tusername = worker_user\n", ""))
        .verify(&system, path(), Some(("worker_user", 502)))
        .is_err());
    assert!(
        parsed(&f.replace("domain = system", "domain = gui/502 [100001]"))
            .verify(&system, path(), Some(("worker_user", 502)))
            .is_err()
    );
    assert!(
        parsed(&f.replace("username = worker_user", "username = other"))
            .verify(&system, path(), Some(("worker_user", 502)))
            .is_err()
    );
}
#[test]
fn rejects_unknown_duplicate_truncated_and_ambiguous_data() {
    let f = fixture();
    for bad in [
        vec![b'x'; 65_537],
        vec![255, b'\n'],
        b"".to_vec(),
        b"\n".to_vec(),
        b"}\n".to_vec(),
        b" = {\n}\n".to_vec(),
        f.trim_end().as_bytes().to_vec(),
        f.replace("\n}", "\n").into_bytes(),
        format!("{f}}}\n").into_bytes(),
    ] {
        assert!(parse(&bad).is_err());
    }
    for (old, new) in [
        ("\tpath =", "path ="),
        ("\tpath =", "\tpath:"),
        ("\tstate = running", "\tstate = running\n\tstate = running"),
        ("\tstate = running", "\tunknown = value"),
        ("\tstate = running", "\tunknown = {\n\t}"),
        ("\t\tdaemon\n", "\tdaemon\n"),
        ("\t\tPATH => /usr/bin", "\t\tPATH = /usr/bin"),
        ("\t\tPATH => /usr/bin", "\t\tBAD KEY => /usr/bin"),
        (
            "\t\tPATH => /usr/bin",
            "\t\tPATH => /usr/bin\n\t\tPATH => /usr/bin",
        ),
        ("\t\tdaemon\n", "\t\t\n"),
        ("\t\tdaemon\n", "\t\t daemon\n"),
        ("\t\tdaemon\n", "\t\tdaemon \n"),
        ("\t\tdaemon\n", "\t\t\"daemon\"\n"),
        ("\t\tdaemon\n", "\t\t\\daemon\n"),
        ("\t\tdaemon\n", "\t\tdaemon\r\n"),
        ("\t\tdaemon\n", "\t\tpath = {\n"),
        (
            "\t\t\"org.example.endpoint\" = {",
            "\t\torg.example.endpoint = {",
        ),
        ("\t\t\tport = 0x100", "\t\t\tunknown = 1"),
        (
            "\t\t\tport = 0x100",
            "\t\t\tport = 0x100\n\t\t\tport = 0x100",
        ),
        ("\t\t\tport = 0x100", "\t\tport = 0x100"),
        ("\t\t\tport = 0x100", "\t\t\tport: 0x100"),
        ("\t\tID = 100", "\tID = 100"),
        ("\t\tID = 100", "\t\tID: 100"),
        ("\t\tID = 100", "\t\tunknown = 100"),
        ("\t\tID = 100", "\t\tID = 100\n\t\tID = 100"),
        ("\t\tsuccessful exit => 0", "\t\tunknown => 0"),
        ("\t\tmaxfiles (soft) => 512", "\t\tunknown => 512"),
    ] {
        assert!(
            parse(f.replace(old, new).as_bytes()).is_err(),
            "{old} -> {new}"
        );
    }
    for key in ["path", "program", "domain"] {
        let missing = f
            .lines()
            .filter(|line| !line.starts_with(&format!("\t{key} =")))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        assert!(parse(missing.as_bytes()).is_err());
    }
    for tail in [
        "\targuments = {\n",
        "\tendpoints = {\n",
        "\tendpoints = {\n\t\t\"name\" = {\n",
    ] {
        assert!(parse(format!("target = {{\n{tail}").as_bytes()).is_err());
    }
    let no_args = "target = {\n\tpath = /x\n\tprogram = /x\n\tdomain = system\n}\n";
    assert!(parse(no_args.as_bytes()).is_err());
    let duplicated_endpoint = f.replace(
        "\tendpoints = {\n",
        "\tendpoints = {\n\t\t\"org.example.endpoint\" = {\n\t\t}\n",
    );
    assert!(parse(duplicated_endpoint.as_bytes()).is_err());
    let mut bound = f.clone();
    bound.insert_str(bound.len() - 2, &"\n".repeat(65_536 - f.len()));
    assert_eq!(bound.len(), 65_536);
    assert!(parse(bound.as_bytes()).is_ok());
    bound.insert(bound.len() - 2, '\n');
    assert_eq!(bound.len(), 65_537);
    assert!(parse(bound.as_bytes()).is_err());
}
#[test]
fn accepts_only_enumerated_diagnostics() {
    for key in [
        "active count",
        "minimum runtime",
        "base minimum runtime",
        "exit timeout",
        "runs",
        "pid",
        "immediate reason",
        "forks",
        "execs",
        "initialized",
        "trampolined",
        "started suspended",
        "proxy started suspended",
        "checked allocations",
        "checked allocations reason",
        "checked allocations flags",
        "last exit code",
        "spawn type",
        "jetsam priority",
        "jetsam memory limit (active, soft)",
        "jetsam memory limit (inactive, soft)",
        "jetsamproperties category",
        "jetsam thread limit",
        "cpumon",
        "exponential throttling grace limit",
        "sanitizer flags",
        "properties",
        "group",
        "spawn role",
        "job state",
    ] {
        let f = fixture().replace(
            "\tstate = running",
            &format!("\tstate = running\n\t{key} = observed"),
        );
        let o = parsed(&f);
        assert_eq!(o.fields[key], "observed");
    }
}

#[test]
fn environment_extras_require_observed_os_provenance_and_exact_values() {
    let mut d = definition();
    d.environment.clear();
    let plan = render(&d).unwrap();
    let f = fixture().replace(
        "PATH => /usr/bin\n",
        "PATH => /usr/bin:/bin:/usr/sbin:/sbin\n",
    );
    assert_eq!(parsed(&f).verify(&plan, path(), None), Ok(()));
    let add = |text: &str, block: &str, key: &str, value: &str| {
        text.replace(
            &format!("\t{block} = {{\n"),
            &format!("\t{block} = {{\n\t\t{key} => {value}\n"),
        )
    };
    for key in ["BASH_ENV", "PYTHONPATH", "NODE_OPTIONS", "UNKNOWN_KEY"] {
        for block in [
            "environment",
            "default environment",
            "inherited environment",
        ] {
            assert!(
                parsed(&add(&f, block, key, "unapproved"))
                    .verify(&plan, path(), None)
                    .is_err(),
                "{block}/{key}"
            );
        }
    }
    for (key, value) in [
        ("OSLogRateLimit", "64"),
        ("MallocSpaceEfficient", "1"),
        ("MallocMaxMagazines", "1"),
    ] {
        assert_eq!(
            parsed(&add(&f, "environment", key, value)).verify(&plan, path(), None),
            Ok(())
        );
        for block in ["default environment", "inherited environment"] {
            assert!(parsed(&add(&f, block, key, value))
                .verify(&plan, path(), None)
                .is_err());
        }
        assert!(parsed(&add(&f, "environment", key, "2"))
            .verify(&plan, path(), None)
            .is_err());
    }
    for block in ["default environment", "inherited environment"] {
        assert!(
            parsed(&add(&f, block, "XPC_SERVICE_NAME", "org.example.worker"))
                .verify(&plan, path(), None)
                .is_err()
        );
    }
    assert!(parsed(&f.replace(
        "XPC_SERVICE_NAME => org.example.worker",
        "XPC_SERVICE_NAME => foreign"
    ))
    .verify(&plan, path(), None)
    .is_err());
    assert!(
        parsed(&f.replace("PATH => /usr/bin:/bin:/usr/sbin:/sbin", "PATH => /foreign"))
            .verify(&plan, path(), None)
            .is_err()
    );
    for block in ["environment", "default environment"] {
        assert!(parsed(&add(
            &f,
            block,
            "SSH_AUTH_SOCK",
            "/var/run/com.apple.launchd.example/Listeners"
        ))
        .verify(&plan, path(), None)
        .is_err());
    }
    for socket in [
        "/foreign/example/Listeners",
        "/var/run/com.apple.launchd.example/Other",
        "/var/run/com.apple.launchd./Listeners",
        "/var/run/com.apple.launchd.bad-token/Listeners",
    ] {
        assert!(
            parsed(&f.replace("/var/run/com.apple.launchd.example/Listeners", socket))
                .verify(&plan, path(), None)
                .is_err()
        );
    }
    let mut system_definition = d.clone();
    system_definition.mode = Mode::SystemUser {
        uid: 502,
        username: "worker_user".into(),
    };
    let system = render(&system_definition).unwrap();
    let system_observation = f
        .replace("gui/501/org.example.worker", "system/org.example.worker")
        .replace(
            "domain = gui/501 [100001]",
            "domain = system\n\tusername = worker_user",
        );
    assert!(parsed(&system_observation)
        .verify(&system, path(), Some(("worker_user", 502)))
        .is_err());
    // A lower OS default may differ, but the job's configured effective value
    // must be exact. Every unconfigured key above is checked in every layer.
    d.environment.insert("PATH".into(), "/configured".into());
    let plan = render(&d).unwrap();
    let f = add(&f, "environment", "PATH", "/configured");
    assert_eq!(parsed(&f).verify(&plan, path(), None), Ok(()));
    assert!(
        parsed(&f.replace("PATH => /configured", "PATH => /changed"))
            .verify(&plan, path(), None)
            .is_err()
    );
}
