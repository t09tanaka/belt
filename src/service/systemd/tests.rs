//! The launcher against a scripted runner; no `systemctl` is run.
use super::*;
use crate::service::macos::policy::{Definition, Mode};
use crate::service::macos::{DirectoryFiles, ManagedFiles};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

#[derive(Default)]
struct Script {
    calls: Vec<Vec<String>>,
    answers: VecDeque<Result<CommandOutput, String>>,
}
#[derive(Clone, Default)]
struct Fake(Rc<RefCell<Script>>);
impl CommandRunner for Fake {
    fn run(&mut self, arguments: &[String]) -> Result<CommandOutput, String> {
        let mut s = self.0.borrow_mut();
        s.calls.push(arguments.to_vec());
        s.answers
            .pop_front()
            .unwrap_or_else(|| Err("no answer scripted".into()))
    }
}
impl Fake {
    fn push(&self, a: Result<CommandOutput, String>) {
        self.0.borrow_mut().answers.push_back(a);
    }
    fn calls(&self) -> Vec<Vec<String>> {
        self.0.borrow().calls.clone()
    }
}
fn out(success: bool, stdout: &str, stderr: &str) -> Result<CommandOutput, String> {
    Ok(CommandOutput {
        success,
        exit_code: Some(if success { 0 } else { 1 }),
        stdout: stdout.as_bytes().to_vec(),
        stderr: stderr.as_bytes().to_vec(),
    })
}
fn ok() -> Result<CommandOutput, String> {
    out(true, "", "")
}

struct Unit {
    _dir: tempfile::TempDir,
    plan: Plan,
    receipt: Receipt,
}
fn unit() -> Unit {
    let dir = crate::test_tmp::tempdir().unwrap();
    let plan = policy::render(&Definition {
        label: "org.example.worker".into(),
        mode: Mode::User { uid: 1000 },
        executable: "/opt/rtr".into(),
        arguments: vec!["worker".into()],
        environment: BTreeMap::new(),
        working_directory: "/cfg".into(),
    })
    .unwrap();
    let mut files = DirectoryFiles::open(dir.path()).unwrap();
    let receipt = files.install(&plan).unwrap();
    Unit {
        _dir: dir,
        plan,
        receipt,
    }
}
fn shown(
    u: &Unit,
    load: &str,
    active: &str,
    pid: u32,
    reload: &str,
) -> Result<CommandOutput, String> {
    out(
        true,
        &format!(
            "LoadState={load}\nActiveState={active}\nFragmentPath={}\nMainPID={pid}\nNeedDaemonReload={reload}\n",
            u.receipt.path().display()
        ),
        "",
    )
}
fn launcher(f: &Fake) -> SystemdLauncher<Fake> {
    SystemdLauncher::new(f.clone())
}
fn words(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}
const START: [&str; 3] = ["--user", "start", "org.example.worker.service"];

#[test]
fn it_drives_systemd_and_says_so() {
    assert_eq!(launcher(&Fake::default()).backend(), Backend::Systemd);
}

#[test]
fn a_start_runs_reload_enable_start_in_order() {
    let f = Fake::default();
    (0..3).for_each(|_| f.push(ok()));
    launcher(&f).launchctl(&words(&START)).unwrap();
    assert_eq!(
        f.calls(),
        [
            words(&["--user", "daemon-reload"]),
            words(&["--user", "enable", "org.example.worker.service"]),
            words(&["--user", "start", "org.example.worker.service"]),
        ]
    );
}

#[test]
fn a_failed_preparation_stops_the_start_as_not_applied() {
    for failing in 0..2 {
        let f = Fake::default();
        (0..failing).for_each(|_| f.push(ok()));
        f.push(out(false, "", "denied"));
        let error = launcher(&f).launchctl(&words(&START)).unwrap_err();
        assert!(matches!(error, LaunchError::NotApplied(_)), "{failing}");
        assert_eq!(f.calls().len(), failing + 1);
    }
}

#[test]
fn an_unconfirmed_last_step_is_uncertain() {
    for last in [
        out(false, "", ""),
        out(true, &"x".repeat(OUTPUT_LIMIT + 1), ""),
        out(true, "", &"x".repeat(OUTPUT_LIMIT + 1)),
        Err("deadline".into()),
    ] {
        let f = Fake::default();
        f.push(ok());
        f.push(ok());
        f.push(last);
        assert!(matches!(
            launcher(&f).launchctl(&words(&START)),
            Err(LaunchError::OutcomeUnknown(_))
        ));
    }
    // A runner that gave no answer for a preparatory step is uncertain too.
    let f = Fake::default();
    f.push(Err("deadline".into()));
    assert!(matches!(
        launcher(&f).launchctl(&words(&START)),
        Err(LaunchError::OutcomeUnknown(_))
    ));
}

#[test]
fn a_stop_is_one_command_and_bad_arguments_run_nothing() {
    let f = Fake::default();
    f.push(ok());
    launcher(&f)
        .launchctl(&words(&["--user", "stop", "org.example.worker.service"]))
        .unwrap();
    assert_eq!(
        f.calls(),
        [words(&["--user", "stop", "org.example.worker.service"])]
    );
    let f = Fake::default();
    assert!(matches!(
        launcher(&f).launchctl(&words(&["--user", "kill", "x.service"])),
        Err(LaunchError::NotApplied(_))
    ));
    assert!(f.calls().is_empty());
}

#[test]
fn the_loaded_state_follows_the_unit() {
    let u = unit();
    let f = Fake::default();
    f.push(shown(&u, "loaded", "active", 5, "no"));
    f.push(shown(&u, "loaded", "inactive", 0, "no"));
    f.push(shown(&u, "not-found", "inactive", 0, "no"));
    f.push(shown(&u, "loaded", "deactivating", 5, "no"));
    f.push(out(false, "", ""));
    f.push(out(true, "", "warning"));
    f.push(out(true, "garbage", ""));
    f.push(out(true, &"x".repeat(OUTPUT_LIMIT + 1), ""));
    f.push(Err("deadline".into()));
    let mut l = launcher(&f);
    let states: Vec<_> = (0..9)
        .map(|_| l.loaded_state(&u.plan, &u.receipt))
        .collect();
    assert_eq!(
        states,
        [
            LoadedState::Match,
            LoadedState::Absent,
            LoadedState::Absent,
            LoadedState::Unknown,
            LoadedState::Unknown,
            LoadedState::Unknown,
            LoadedState::Unknown,
            LoadedState::Unknown,
            LoadedState::Unknown,
        ]
    );
    assert_eq!(
        f.calls()[0],
        [
            "--user",
            "show",
            "org.example.worker.service",
            "--property=LoadState,ActiveState,FragmentPath,MainPID,NeedDaemonReload"
        ]
    );
}

#[test]
fn verify_loaded_accepts_only_a_match() {
    let u = unit();
    let f = Fake::default();
    f.push(shown(&u, "loaded", "active", 5, "no"));
    f.push(shown(&u, "loaded", "inactive", 0, "no"));
    f.push(shown(&u, "loaded", "active", 5, "yes"));
    let mut l = launcher(&f);
    assert!(l.verify_loaded(&u.plan, &u.receipt).is_ok());
    assert!(matches!(
        l.verify_loaded(&u.plan, &u.receipt),
        Err(LaunchError::NotApplied(_))
    ));
    assert!(l.verify_loaded(&u.plan, &u.receipt).is_err());
    // A query that failed is no answer.
    f.push(Err("deadline".into()));
    assert!(l.verify_loaded(&u.plan, &u.receipt).is_err());
}

#[test]
fn the_running_identity_is_the_units_own_stable_process() {
    let u = unit();
    let me = std::process::id();
    let f = Fake::default();
    f.push(shown(&u, "loaded", "active", me, "no"));
    f.push(shown(&u, "loaded", "active", me, "no"));
    let identity = launcher(&f).running_identity(&u.plan, &u.receipt).unwrap();
    assert_eq!(identity.pid, me);
    assert_eq!(f.calls().len(), 2);
}

#[test]
fn an_unconfirmed_running_identity_is_refused() {
    let u = unit();
    let me = std::process::id();
    let parent = std::os::unix::process::parent_id();
    let cases: Vec<Vec<Result<CommandOutput, String>>> = vec![
        // not this unit's active process
        vec![shown(&u, "loaded", "inactive", 0, "no")],
        // active but nothing to name
        vec![shown(&u, "loaded", "active", 0, "no")],
        // a pid that is not a live process
        vec![shown(&u, "loaded", "active", 4_000_000, "no")],
        // the main process changed between the two observations
        vec![
            shown(&u, "loaded", "active", me, "no"),
            shown(&u, "loaded", "active", parent, "no"),
        ],
        // the second observation failed
        vec![shown(&u, "loaded", "active", me, "no"), Err("x".into())],
        vec![Err("x".into())],
    ];
    for answers in cases {
        let f = Fake::default();
        answers.into_iter().for_each(|a| f.push(a));
        assert!(launcher(&f).running_identity(&u.plan, &u.receipt).is_err());
    }
}
