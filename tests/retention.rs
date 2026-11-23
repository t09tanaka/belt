//! The retention sweep: what it removes, what it keeps, and that the
//! library and the CLI run it by themselves.

use chrono::{DateTime, Duration as Span, Utc};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use task_traffic_controller::config::Config;
use task_traffic_controller::exec::{self, Gated, Hooks, Opts};
use task_traffic_controller::history::{self, Record};
use task_traffic_controller::jobs::{self, Job};
use task_traffic_controller::managed::{self, CommandSpec};
use task_traffic_controller::retention::{
    self, DEAD_JOB_AFTER_SECS, STRAY_AFTER_SECS, SWEEP_EVERY_SECS,
};
use task_traffic_controller::{lock, Clock, Context, FakeClock, FixedBoot, FixedLive, Paths};

fn ctx_at(dir: &Path, clock: Arc<FakeClock>) -> Context {
    let mut ctx = Context::new(Paths::new(dir.join("state"), dir.join("config.toml")))
        .with_live(Arc::new(FixedLive(None)))
        .with_boot(Arc::new(FixedBoot(Some("boot-1".into()))))
        .with_clock(clock);
    // These tests are about what a sweep does when it has the time; the
    // budget has tests of its own.
    ctx.sweep_budget = Duration::from_secs(3600);
    ctx
}

/// The configuration file of the state directory says `text`: what the
/// `task-traffic-controller` command goes by, and so every sweep.
fn config(ctx: &Context, text: &str) -> Config {
    std::fs::write(ctx.paths.config_file(), text).unwrap();
    Config::load(ctx.paths.config_file()).unwrap()
}

fn ctx(dir: &Path) -> (Context, Arc<FakeClock>) {
    let clock = Arc::new(FakeClock::new(Utc::now()));
    (ctx_at(dir, clock.clone()), clock)
}

fn record(key: &str, at: DateTime<Utc>) -> Record {
    Record {
        key: key.into(),
        repo: String::new(),
        subdir: String::new(),
        argv: "x".into(),
        key_argv: String::new(),
        started_at: at,
        wall_secs: 1.0,
        cpu_secs: 1.0,
        max_rss_bytes: 1,
        exit_code: 0,
        weight_used: 1,
        contended: false,
        measurement_id: None,
        extra: Default::default(),
    }
}

fn job(id: &str, submitted_at: DateTime<Utc>) -> Job {
    Job {
        id: id.into(),
        argv: vec!["true".into()],
        cwd: "/".into(),
        weight: None,
        front: false,
        key: None,
        label: None,
        submitted_at,
        submitter_pid: 1,
        detach: false,
        orphaned: false,
        finished_at: None,
        extra: Default::default(),
    }
}

fn age(path: &Path, secs: i64) {
    let t = SystemTime::now() - Duration::from_secs(secs as u64);
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(t)
        .unwrap();
}

fn count_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |rd| rd.count())
}

#[test]
fn the_first_call_only_arms_the_clock_and_a_sweep_is_due_every_six_hours() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let cfg = Config::default();
    let old = clock.now() - Span::days(200);
    history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();

    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
    assert!(ctx.paths.sweep_stamp().exists());
    assert_eq!(
        history::read_all(ctx.paths.history_file()).unwrap().len(),
        1
    );

    clock.advance(Duration::from_secs(SWEEP_EVERY_SECS as u64 - 1));
    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
    clock.advance(Duration::from_secs(1));
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
    assert_eq!(report.history_dropped, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    // The sweep restarted the interval.
    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
}

#[test]
fn history_keys_not_seen_for_history_keep_days_are_dropped_and_active_ones_stay() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = Config {
        history_keep_days: 30,
        ..Config::default()
    };
    let p = ctx.paths.history_file();
    history::append(p, &record("gone", now - Span::days(31))).unwrap();
    history::append(p, &record("active", now - Span::days(100))).unwrap();
    history::append(p, &record("active", now - Span::days(29))).unwrap();
    let report = retention::sweep(&ctx, &cfg);
    assert_eq!(report.history_dropped, 1);
    assert!(history::for_key(p, "gone").unwrap().is_empty());
    assert_eq!(history::for_key(p, "active").unwrap().len(), 2);
}

#[test]
fn run_gated_sweeps_by_itself_when_due() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let old = clock.now() - Span::days(400);
    history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();
    std::fs::write(ctx.paths.sweep_stamp(), "2000-01-01T00:00:00Z").unwrap();
    let opts = Opts {
        weight: Some(1),
        key: Some("k".into()),
        argv: vec!["true".into()],
        cwd: dir.path().to_path_buf(),
        ..Opts::default()
    };
    let got = exec::run_gated(&ctx, &Config::default(), &opts, "r", &Hooks::quiet()).unwrap();
    assert!(matches!(got, Gated::Finished(_)));
    assert!(history::for_key(ctx.paths.history_file(), "stale")
        .unwrap()
        .is_empty());
    // The run itself was recorded before the sweep, which ran after it.
    assert_eq!(
        history::read_all(ctx.paths.history_file()).unwrap().len(),
        1
    );
}

#[test]
fn a_managed_command_sweeps_when_it_finishes_and_not_when_it_registers() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let old = clock.now() - Span::days(400);
    history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();
    // A stamp that cannot be understood means "due", not "start the clock".
    std::fs::write(ctx.paths.sweep_stamp(), "not a time").unwrap();
    let spec = CommandSpec {
        command_id: "c".into(),
        spec_digest: "d".into(),
        opts: Opts {
            weight: Some(1),
            key: Some("k".into()),
            argv: vec!["true".into()],
            cwd: dir.path().to_path_buf(),
            ..Opts::default()
        },
        attempt: None,
        services: vec![],
    };
    let cfg = Config::default();
    let register = |id: &str| {
        let spec = CommandSpec {
            command_id: id.into(),
            ..spec.clone()
        };
        match managed::register(&ctx, &cfg, &spec).unwrap() {
            managed::Registration::Registered(_, guard) => guard,
            _ => panic!("not registered"),
        }
    };
    let left = || history::read_all(ctx.paths.history_file()).unwrap().len();
    let guard = register("c");
    // Nothing before the command: registering costs no sweep.
    assert_eq!(left(), 1);
    // Nothing after a command that did not end well either: whoever
    // cancels, or has a failure to report, gets the answer at once.
    use managed::TerminalKind as Kind;
    assert!(!managed::sweeps_after(&Kind::Cancelled));
    assert!(!managed::sweeps_after(&Kind::Exited { code: 1 }));
    assert!(!managed::sweeps_after(&Kind::Signaled { signal: 15 }));
    assert!(!managed::sweeps_after(&Kind::SpawnFailed));
    assert!(!managed::sweeps_after(&Kind::Interrupted));
    assert!(!managed::sweeps_after(&Kind::Abandoned));
    assert!(managed::sweeps_after(&Kind::Exited { code: 0 }));
    managed::finish(&ctx, guard, Kind::Cancelled).unwrap();
    assert_eq!(left(), 1);
    managed::finish(&ctx, register("e"), Kind::Exited { code: 3 }).unwrap();
    assert_eq!(left(), 1);
    managed::finish(&ctx, register("d"), Kind::Exited { code: 0 }).unwrap();
    assert_eq!(left(), 0);
}

#[test]
fn a_job_whose_runner_died_gets_a_finish_time_and_is_then_removed() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let cfg = Config::default();
    let now = clock.now();
    jobs::save(&ctx, &job("dead", now - Span::seconds(DEAD_JOB_AFTER_SECS))).unwrap();
    std::fs::write(ctx.paths.job_log("dead"), "output\n").unwrap();
    jobs::save(
        &ctx,
        &job("young", now - Span::seconds(DEAD_JOB_AFTER_SECS - 5)),
    )
    .unwrap();
    // Its runner is alive: it holds the lease.
    jobs::save(&ctx, &job("alive", now - Span::days(3))).unwrap();
    let _lease = lock::take_lease(&ctx, "alive").unwrap();

    let report = retention::sweep(&ctx, &cfg);
    assert_eq!(report.jobs_found_dead, 1, "{report}");
    assert_eq!(report.jobs_removed, 0);
    assert_eq!(jobs::load(&ctx, "dead").unwrap().finished_at, Some(now));
    assert_eq!(jobs::load(&ctx, "young").unwrap().finished_at, None);
    assert_eq!(jobs::load(&ctx, "alive").unwrap().finished_at, None);

    // keep_days later: the dead one goes with its log; the others stay.
    clock.advance(Duration::from_secs(7 * 86_400 + 1));
    let report = retention::sweep(&ctx, &cfg);
    assert_eq!(report.jobs_removed, 1, "{report}");
    assert!(jobs::load(&ctx, "dead").is_err());
    assert!(!ctx.paths.job_log("dead").exists());
    assert!(jobs::load(&ctx, "young").is_ok());
    assert!(jobs::load(&ctx, "alive").is_ok());
}

#[test]
fn a_dead_jobs_log_is_cut_to_the_limit_and_keeps_its_tail() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let cfg = config(&ctx, "jobs_log_max_mib = 1\n");
    jobs::save(&ctx, &job("big", clock.now() - Span::hours(5))).unwrap();
    let mut text = String::new();
    for i in 0..60_000 {
        text.push_str(&format!("line {i:08} ................\n"));
    }
    assert!(text.len() > 1 << 20);
    std::fs::write(ctx.paths.job_log("big"), &text).unwrap();
    let report = retention::sweep(&ctx, &cfg);
    assert_eq!(report.logs_cut, 1, "{report}");
    let cut = std::fs::read_to_string(ctx.paths.job_log("big")).unwrap();
    assert!(cut.len() <= (1 << 20) + 200, "{}", cut.len());
    assert!(
        cut.starts_with("[task-traffic-controller:"),
        "{}",
        &cut[..80]
    );
    assert!(cut.ends_with("line 00059999 ................\n"));
    // Every kept line is whole.
    assert!(cut.lines().nth(1).unwrap().starts_with("line "));
    // Cut once: a second sweep finds it within the limit.
    assert_eq!(retention::sweep(&ctx, &cfg).logs_cut, 0);
}

#[test]
fn a_live_jobs_log_is_not_cut() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let cfg = config(&ctx, "jobs_log_max_mib = 1\n");
    jobs::save(&ctx, &job("run", clock.now() - Span::hours(5))).unwrap();
    let _lease = lock::take_lease(&ctx, "run").unwrap();
    std::fs::write(ctx.paths.job_log("run"), vec![b'a'; 2 << 20]).unwrap();
    assert_eq!(retention::sweep(&ctx, &cfg).logs_cut, 0);
    assert_eq!(
        std::fs::metadata(ctx.paths.job_log("run")).unwrap().len(),
        2 << 20
    );
}

#[test]
fn job_files_without_a_descriptor_and_old_temp_files_are_swept() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    let jobs_dir = ctx.paths.jobs_dir();
    std::fs::create_dir_all(&jobs_dir).unwrap();
    let state = ctx.paths.state_dir().to_path_buf();
    let put = |p: std::path::PathBuf, secs: i64| {
        std::fs::write(&p, "x").unwrap();
        age(&p, secs);
    };
    let old = STRAY_AFTER_SECS + 60;
    put(jobs_dir.join("orphan.log"), old);
    put(jobs_dir.join("orphan.exit"), old);
    put(jobs_dir.join("orphan.waiter"), old);
    put(jobs_dir.join(".tmpOLD"), old);
    put(state.join(".tmpOLD"), old);
    // Kept: young, unknown names, the file of a job that has a descriptor.
    put(jobs_dir.join("fresh.log"), 60);
    put(jobs_dir.join(".tmpNEW"), 60);
    put(jobs_dir.join("notes.txt"), old);
    put(state.join("other.dat"), old);
    jobs::save(&ctx, &job("has", Utc::now())).unwrap();
    put(jobs_dir.join("has.log"), old);

    let report = retention::sweep(&ctx, &Config::default());
    // Two temporary files and one job without a descriptor.
    assert_eq!(report.strays_removed, 3, "{report}");
    let mut left: Vec<String> = std::fs::read_dir(&jobs_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    left.sort();
    assert_eq!(
        left,
        [".tmpNEW", "fresh.log", "has.json", "has.log", "notes.txt"]
    );
    assert!(state.join("other.dat").exists());
    assert!(!state.join(".tmpOLD").exists());
}

#[test]
fn an_attempt_that_died_between_its_markers_is_settled() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let mdir = ctx.paths.measurements_dir();
    std::fs::create_dir_all(&mdir).unwrap();
    // "m1": appended, then died before .applied. "m2": died before the
    // append. A third attempt would append "m2" again, never "m1".
    let mut r = record("k", clock.now());
    r.measurement_id = Some("m1".into());
    history::append(ctx.paths.history_file(), &r).unwrap();
    std::fs::write(mdir.join("m1.pending"), b"").unwrap();
    std::fs::write(mdir.join("m2.pending"), b"").unwrap();

    let report = retention::sweep(&ctx, &Config::default());
    assert_eq!(report.markers_removed, 1, "{report}");
    assert!(mdir.join("m1.applied").exists());
    assert!(!mdir.join("m1.pending").exists());
    assert!(!mdir.join("m2.pending").exists());
    let again = |id: &str| {
        let mut r = record("k", clock.now());
        r.measurement_id = Some(id.into());
        managed::record_measurement(&ctx, id, &r).unwrap()
    };
    assert_eq!(again("m1"), managed::Measured::Duplicate);
    assert_eq!(again("m2"), managed::Measured::Applied);
    assert_eq!(count_in(&mdir), 2);
}

#[test]
fn a_pending_marker_is_left_alone_while_an_attempt_holds_the_history_lock() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    let ctx = ctx.with_ledger_lock_timeout(Duration::from_millis(200));
    let mdir = ctx.paths.measurements_dir();
    std::fs::create_dir_all(&mdir).unwrap();
    std::fs::write(mdir.join("live.pending"), b"").unwrap();
    let held = lock::acquire(&ctx.paths.history_lock(), Duration::from_secs(1)).unwrap();
    let report = retention::sweep(&ctx, &Config::default());
    assert!(!report.errors.is_empty());
    assert!(mdir.join("live.pending").exists());
    drop(held);
}

#[test]
fn a_failing_step_does_not_stop_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let ctx = ctx.with_ledger_lock_timeout(Duration::from_millis(200));
    jobs::save(&ctx, &job("dead", clock.now() - Span::hours(2))).unwrap();
    // History is busy, jobs are not.
    let _held = lock::acquire(&ctx.paths.history_lock(), Duration::from_secs(1)).unwrap();
    let report = retention::sweep(&ctx, &Config::default());
    assert!(report.errors.iter().any(|e| e.starts_with("history")));
    assert_eq!(report.jobs_found_dead, 1);
}

#[test]
fn trimming_removes_the_markers_of_the_records_it_drops_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    for (i, id) in ["m0", "m1", "m2"].iter().enumerate() {
        let mut r = record("k", clock.now() + Span::seconds(i as i64));
        r.measurement_id = Some((*id).into());
        managed::record_measurement(&ctx, id, &r).unwrap();
    }
    assert_eq!(managed::trim_history(&ctx, 2).unwrap(), 1);
    let mdir = ctx.paths.measurements_dir();
    assert!(!mdir.join("m0.applied").exists());
    assert!(mdir.join("m1.applied").exists() && mdir.join("m2.applied").exists());
}

#[test]
fn a_history_with_nothing_left_and_empty_directories_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let mut r = record("k", clock.now() - Span::days(400));
    r.measurement_id = Some("m".into());
    managed::record_measurement(&ctx, "m", &r).unwrap();
    drop(lock::take_lease(&ctx, "x").unwrap());
    std::fs::remove_file(ctx.paths.lease_file("x")).unwrap();
    std::fs::create_dir_all(ctx.paths.jobs_dir()).unwrap();
    retention::sweep(&ctx, &Config::default());
    assert!(!ctx.paths.history_file().exists());
    assert!(!ctx.paths.measurements_dir().exists());
    // `leases/` and `jobs/` stay: a released version makes the directory
    // and opens a file in it in two steps, with nothing to stop a removal
    // in between.
    assert!(ctx.paths.leases_dir().exists());
    assert!(ctx.paths.jobs_dir().exists());
    // Every writer makes its directory again.
    jobs::save(&ctx, &job("j", clock.now())).unwrap();
    drop(lock::take_lease(&ctx, "y").unwrap());
    let mut r = record("k", clock.now());
    r.measurement_id = Some("n".into());
    managed::record_measurement(&ctx, "n", &r).unwrap();
}

// ---- what the review found ----

fn old_stamp(ctx: &Context) {
    std::fs::create_dir_all(ctx.paths.state_dir()).unwrap();
    std::fs::write(
        ctx.paths.sweep_stamp(),
        r#"{"complete":"2000-01-01T00:00:00Z"}"#,
    )
    .unwrap();
}

fn finished_job(ctx: &Context, id: &str, submitted: DateTime<Utc>, finished: DateTime<Utc>) {
    let mut j = job(id, submitted);
    j.finished_at = Some(finished);
    jobs::save(ctx, &j).unwrap();
}

#[test]
fn a_job_that_keeps_failing_does_not_stop_the_jobs_after_it() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = config(&ctx, "jobs_log_max_mib = 1\n");
    // Looked at first (newest): its log is over the limit and cannot be
    // opened. Looked at after it: an old job that must still go.
    finished_job(&ctx, "bad", now - Span::hours(2), now - Span::hours(1));
    let log = ctx.paths.job_log("bad");
    std::fs::write(&log, vec![b'a'; 2 << 20]).unwrap();
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&log).is_ok() {
        return; // root reads anything: the failure cannot be made here
    }
    finished_job(&ctx, "old", now - Span::days(4), now - Span::days(3));

    let report = retention::sweep(&ctx, &cfg);
    assert_eq!(report.jobs_removed, 1, "{report}");
    assert!(jobs::load(&ctx, "old").is_err());
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(
        report.errors[0].starts_with("job bad:"),
        "{:?}",
        report.errors
    );
    assert!(report.complete);
}

#[test]
fn an_empty_or_unreadable_stamp_means_due_and_only_an_absent_one_arms_the_clock() {
    for text in ["", "garbage", "2000-01-01T00:00:00Z", "{\"complete\":"] {
        let dir = tempfile::tempdir().unwrap();
        let (ctx, clock) = ctx(dir.path());
        let old = clock.now() - Span::days(200);
        history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();
        std::fs::write(ctx.paths.sweep_stamp(), text).unwrap();
        let report = retention::sweep_if_due(&ctx, &Config::default())
            .unwrap_or_else(|| panic!("not due with stamp {text:?}"));
        assert_eq!(report.history_dropped, 1);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        // It left a stamp that says so.
        assert!(retention::sweep_if_due(&ctx, &Config::default()).is_none());
    }
}

#[test]
fn the_sweep_runs_when_the_stamp_cannot_be_written_and_says_so_once() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let old = clock.now() - Span::days(200);
    history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();
    // Neither readable nor writable as a file.
    std::fs::create_dir_all(ctx.paths.sweep_stamp()).unwrap();
    for dropped in [1, 0] {
        let report = retention::sweep_if_due(&ctx, &Config::default()).expect("due");
        assert_eq!(report.history_dropped, dropped);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(
            report.errors[0].contains("last-sweep"),
            "{:?}",
            report.errors
        );
    }
}

#[test]
fn a_stamp_that_cannot_be_created_does_not_mean_never() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    let state = ctx.paths.state_dir().to_path_buf();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o555)).unwrap();
    let writable = std::fs::write(state.join("probe"), "x").is_ok();
    let got = retention::sweep_if_due(&ctx, &Config::default());
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    if writable {
        return; // root writes anywhere
    }
    // Absent and impossible to write: the sweep ran instead of arming.
    let report = got.expect("the sweep must run");
    assert!(!report.errors.is_empty());
}

#[test]
fn no_configured_number_can_make_a_sweep_panic() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = Config {
        jobs_keep_days: u64::MAX,
        history_keep_days: u64::MAX,
        managed_keep_days: u64::MAX,
        jobs_log_max_mib: u64::MAX,
        history_keep: usize::MAX,
        ..Config::default()
    };
    finished_job(&ctx, "old", now - Span::days(4000), now - Span::days(3999));
    std::fs::write(ctx.paths.job_log("old"), "x").unwrap();
    history::append(
        ctx.paths.history_file(),
        &record("k", now - Span::days(4000)),
    )
    .unwrap();
    let report = retention::sweep(&ctx, &cfg);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!((report.jobs_removed, report.history_dropped), (0, 0));
    assert!(jobs::load(&ctx, "old").is_ok());
}

#[test]
fn a_job_another_wait_holds_keeps_its_files_however_old() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    finished_job(&ctx, "a", now - Span::days(9), now - Span::days(8));
    std::fs::write(ctx.paths.job_exit("a"), "7\n").unwrap();
    let waiter = jobs::take_waiter(&ctx, "a").unwrap();
    let report = retention::sweep(&ctx, &Config::default());
    assert_eq!(report.jobs_removed, 0, "{report}");
    assert_eq!(jobs::read_exit(&ctx, "a"), Some(7));
    drop(waiter);
    let report = retention::sweep(&ctx, &Config::default());
    assert_eq!(report.jobs_removed, 1, "{report}");
    assert_eq!(count_in(&ctx.paths.jobs_dir()), 0);
}

#[test]
fn a_dead_job_goes_exactly_one_day_after_it_was_found_dead_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let cfg = Config::default();
    assert_eq!(cfg.jobs_keep_days, 1);
    jobs::save(
        &ctx,
        &job("dead", clock.now() - Span::seconds(DEAD_JOB_AFTER_SECS)),
    )
    .unwrap();
    assert_eq!(retention::sweep(&ctx, &cfg).jobs_found_dead, 1);
    clock.advance(Duration::from_secs(86_400));
    assert_eq!(retention::sweep(&ctx, &cfg).jobs_removed, 0);
    assert!(jobs::load(&ctx, "dead").is_ok());
    clock.advance(Duration::from_secs(1));
    assert_eq!(retention::sweep(&ctx, &cfg).jobs_removed, 1);
    assert!(jobs::load(&ctx, "dead").is_err());
}

#[test]
fn the_automatic_sweep_skips_a_busy_lock_and_comes_back_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = Config::default();
    finished_job(&ctx, "old", now - Span::days(4), now - Span::days(3));
    history::append(
        ctx.paths.history_file(),
        &record("stale", now - Span::days(200)),
    )
    .unwrap();
    // A temporary file whose writer is gone, and a sidecar that was never
    // launched: an hour old both.
    let stray = ctx.paths.state_dir().join(".ttc-tmp.2147483646.x");
    std::fs::write(&stray, "x").unwrap();
    age(&stray, STRAY_AFTER_SECS + 5);
    std::fs::create_dir_all(ctx.paths.managed_dir()).unwrap();
    std::fs::write(ctx.paths.managed_file("gone"), "not json").unwrap();
    age(&ctx.paths.managed_file("gone"), STRAY_AFTER_SECS + 5);
    old_stamp(&ctx);
    // Somebody holds the ledger lock for as long as it likes.
    let held = lock::ledger(&ctx).unwrap();
    let started = std::time::Instant::now();
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
    // It did not wait for the lock (10 s by default)...
    assert!(started.elapsed() < Duration::from_secs(5));
    // ...did every stage that does not need it (the jobs and the stray
    // files only read the ledger), and says the rest is left.
    assert!(!report.complete);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        (
            report.history_dropped,
            report.jobs_removed,
            report.strays_removed,
            report.receipts_removed
        ),
        (1, 1, 1, 0)
    );
    assert!(ctx.paths.managed_file("gone").exists());
    // Not again at once, and not six hours later either.
    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    drop(held);
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due again");
    assert!(report.complete);
    assert_eq!(report.receipts_removed, 1);
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
}

#[test]
fn one_automatic_sweep_does_a_bounded_amount_and_the_next_does_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = Config::default();
    let n = retention::AUTO_MAX_ACTIONS + 7;
    for i in 0..n {
        finished_job(
            &ctx,
            &format!("j{i}"),
            now - Span::days(4),
            now - Span::days(3),
        );
    }
    old_stamp(&ctx);
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
    assert_eq!(report.jobs_removed, retention::AUTO_MAX_ACTIONS);
    assert!(!report.complete);
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due again");
    assert_eq!(report.jobs_removed, 7);
    assert!(report.complete);
    // `gc` has no such limit.
    for i in 0..n {
        finished_job(
            &ctx,
            &format!("k{i}"),
            now - Span::days(4),
            now - Span::days(3),
        );
    }
    assert_eq!(retention::sweep(&ctx, &cfg).jobs_removed, n);
}

/// A hundred records of one key, the newest now.
fn hundred_records(ctx: &Context, now: DateTime<Utc>) {
    for i in 0..100 {
        history::append(
            ctx.paths.history_file(),
            &record("k", now - Span::seconds(i)),
        )
        .unwrap();
    }
}

/// Programs with different numbers share a state directory: the larger of
/// the caller's and the configuration file's counts, nothing is recorded,
/// and a number lowered in the file counts from the next sweep.
#[test]
fn the_larger_of_the_callers_number_and_the_config_files_wins_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    hundred_records(&ctx, clock.now());
    let kept = || history::read_all(ctx.paths.history_file()).unwrap().len();
    let embedder = Config::default();
    assert_eq!(embedder.history_keep, 50);
    // The command's file keeps more than the embedding program: its sweep
    // removes nothing of it.
    config(&ctx, "history_keep = 100\n");
    assert_eq!(retention::sweep(&ctx, &embedder).history_dropped, 0);
    // Lowered in the file: at once.
    config(&ctx, "history_keep = 70\n");
    assert_eq!(retention::sweep(&ctx, &embedder).history_dropped, 30);
    // The caller's own number is the larger one: the file does not lower it.
    let command = config(&ctx, "history_keep = 10\n");
    assert_eq!(retention::sweep(&ctx, &embedder).history_dropped, 20);
    assert_eq!(kept(), 50);
    // The command itself goes by its file.
    assert_eq!(retention::sweep(&ctx, &command).history_dropped, 40);
    // No file: the defaults stand in for it, so a caller's smaller number
    // alone removes nothing the command would keep.
    std::fs::remove_file(ctx.paths.config_file()).unwrap();
    hundred_records(&ctx, clock.now());
    let small = Config {
        history_keep: 5,
        ..Config::default()
    };
    assert_eq!(retention::sweep(&ctx, &small).history_dropped, 60);
    assert_eq!(kept(), 50);
    // The stamp holds what the throttle needs and nothing else.
    let stamp = std::fs::read_to_string(ctx.paths.sweep_stamp()).unwrap();
    let stamp: serde_json::Value = serde_json::from_str(&stamp).unwrap();
    assert_eq!(stamp.as_object().unwrap().len(), 1, "{stamp}");
    assert!(stamp["complete"].is_string(), "{stamp}");
}

#[test]
fn a_config_file_that_is_not_valid_is_said_once_and_the_defaults_stand_in() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    hundred_records(&ctx, clock.now());
    std::fs::write(ctx.paths.config_file(), "history_keep = \"many\"\n").unwrap();
    let small = Config {
        history_keep: 5,
        ..Config::default()
    };
    let report = retention::sweep(&ctx, &small);
    assert_eq!(report.history_dropped, 50);
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(
        report.errors[0].contains("config.toml"),
        "{:?}",
        report.errors
    );
    // Once in a process.
    hundred_records(&ctx, clock.now());
    let report = retention::sweep(&ctx, &small);
    assert_eq!(report.history_dropped, 100);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

/// Many processes find the sweep due at the same moment: one sweeps. The
/// others either find the lock busy or, with the lock, read the stamp
/// again and find it was done.
#[test]
fn only_one_of_many_that_find_the_sweep_due_sweeps() {
    // The window is a few instructions wide: many rounds, to meet it.
    for round in 0..25 {
        let dir = tempfile::tempdir().unwrap();
        let (ctx, clock) = ctx(dir.path());
        let now = clock.now();
        for i in 0..10 {
            finished_job(
                &ctx,
                &format!("j{i}"),
                now - Span::days(4),
                now - Span::days(3),
            );
        }
        old_stamp(&ctx);
        let gate = Arc::new(std::sync::Barrier::new(8));
        let sweepers: Vec<_> = (0..8)
            .map(|_| {
                let (ctx, gate) = (ctx.clone(), gate.clone());
                std::thread::spawn(move || {
                    gate.wait();
                    retention::sweep_if_due(&ctx, &Config::default())
                })
            })
            .collect();
        let reports: Vec<_> = sweepers
            .into_iter()
            .filter_map(|t| t.join().unwrap())
            .collect();
        assert_eq!(reports.len(), 1, "round {round}: {reports:?}");
        assert_eq!(reports[0].jobs_removed, 10);
        assert!(reports[0].complete);
    }
}

/// The stamp is replaced whole: whoever reads it, at any moment, finds a
/// stamp (the in-place write this replaces could be read empty, which
/// means "due").
#[test]
fn the_stamp_is_never_read_half_written() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    old_stamp(&ctx);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let (path, stop) = (ctx.paths.sweep_stamp(), stop.clone());
        std::thread::spawn(move || {
            let mut reads = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let text = std::fs::read_to_string(&path).unwrap();
                assert!(retention::parse_stamp(&text).is_some(), "{text:?}");
                reads += 1;
            }
            reads
        })
    };
    for _ in 0..300 {
        assert!(retention::sweep(&ctx, &Config::default()).complete);
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(reader.join().unwrap() > 0);
    // And no temporary file is left beside it.
    let left: Vec<_> = std::fs::read_dir(ctx.paths.state_dir())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with(".ttc-tmp."))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

/// A sweep that has no time left does nothing, says so, and is tried again
/// ten minutes later, not six hours.
#[test]
fn a_sweep_that_is_out_of_time_stops_and_is_tried_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut ctx, clock) = ctx(dir.path());
    let now = clock.now();
    finished_job(&ctx, "old", now - Span::days(4), now - Span::days(3));
    history::append(
        ctx.paths.history_file(),
        &record("stale", now - Span::days(200)),
    )
    .unwrap();
    old_stamp(&ctx);
    let cfg = Config::default();
    ctx.sweep_budget = Duration::ZERO;
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
    assert!(!report.complete);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!((report.history_dropped, report.jobs_removed), (0, 0));
    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    ctx.sweep_budget = Duration::from_secs(3600);
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due again");
    assert!(report.complete);
    assert_eq!((report.history_dropped, report.jobs_removed), (1, 1));
    // `gc` has no budget.
    finished_job(&ctx, "old", now - Span::days(4), now - Span::days(3));
    ctx.sweep_budget = Duration::ZERO;
    assert_eq!(retention::sweep(&ctx, &cfg).jobs_removed, 1);
}

/// However little time a sweep has and however much there is to look at,
/// every sweep that finds something due removes some of it, the oldest
/// first: looking and removing have their own time in each stage, and a
/// stage's time starts with its first look.
#[test]
fn a_sweep_with_almost_no_time_still_gets_everything_done_in_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let (mut ctx, clock) = ctx(dir.path());
    let now = clock.now();
    const N: usize = 30;
    std::fs::create_dir_all(ctx.paths.managed_dir()).unwrap();
    for i in 0..N {
        finished_job(
            &ctx,
            &format!("j{i}"),
            now - Span::days(4),
            now - Span::days(3),
        );
        let cut_short = ctx.paths.managed_file(&format!("m{i}"));
        std::fs::write(&cut_short, "{").unwrap();
        age(&cut_short, STRAY_AFTER_SECS + 5);
    }
    old_stamp(&ctx);
    // Eight nanoseconds: one for each stage to look, one to remove.
    ctx.sweep_budget = Duration::from_nanos(8);
    let cfg = Config::default();
    let left = |ctx: &Context| jobs::list(ctx).unwrap().len() + count_in(&ctx.paths.managed_dir());
    let mut rounds = 0;
    while left(&ctx) > 0 {
        let before = left(&ctx);
        let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            report.jobs_removed > 0 && report.receipts_removed > 0 || left(&ctx) == 0,
            "round {rounds}: {report:?}"
        );
        assert!(left(&ctx) < before);
        rounds += 1;
        assert!(rounds <= N, "not done after {rounds} sweeps");
        clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    }
    // It did take more than one: the budget is real.
    assert!(rounds > 1, "{rounds}");
}

/// What a caller's supervisor says about a stop: asked for after `after`
/// looks, or never; and one signal that arrives when the command has just
/// exited, which is the caller's to take.
#[derive(Default)]
struct Stops {
    looks: std::sync::atomic::AtomicUsize,
    after: Option<usize>,
    panics: bool,
    late_signal: bool,
    pending: std::sync::atomic::AtomicI32,
}

impl task_traffic_controller::Supervisor for Stops {
    fn take_pending(&self) -> i32 {
        self.pending.swap(0, std::sync::atomic::Ordering::SeqCst)
    }
    fn child_exited(&self) {
        if self.late_signal {
            self.pending
                .store(libc::SIGTERM, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn stop_pending(&self) -> bool {
        assert!(!self.panics, "the supervisor fails");
        let n = self.looks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.pending.load(std::sync::atomic::Ordering::SeqCst) != 0
            || self.after.is_some_and(|after| n >= after)
    }
}

#[test]
fn a_stop_keeps_the_sweep_from_starting_and_ends_one_that_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    for i in 0..100 {
        finished_job(
            &ctx,
            &format!("j{i}"),
            now - Span::days(4),
            now - Span::days(3),
        );
    }
    old_stamp(&ctx);
    let cfg = Config::default();
    let stopped = ctx.clone().with_supervisor(Arc::new(Stops {
        after: Some(0),
        ..Stops::default()
    }));
    assert!(retention::sweep_if_due(&stopped, &cfg).is_none());
    assert_eq!(jobs::list(&ctx).unwrap().len(), 100);
    // Asked for while it runs: it ends at the next item, and what is left
    // is for the next sweep.
    let later = ctx.clone().with_supervisor(Arc::new(Stops {
        after: Some(60),
        ..Stops::default()
    }));
    let report = retention::sweep_if_due(&later, &cfg).expect("due");
    assert!(!report.complete);
    assert!(
        report.jobs_removed > 0 && report.jobs_removed < 100,
        "{report:?}"
    );
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due again");
    assert!(report.complete);
    assert!(jobs::list(&ctx).unwrap().is_empty());
}

fn stale_history_and_a_due_stamp(ctx: &Context) {
    let old = ctx.clock.now() - Span::days(400);
    history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();
    old_stamp(ctx);
}

fn gated(ctx: &Context, dir: &Path, script: &str, hooks: &Hooks) -> Gated {
    let opts = Opts {
        weight: Some(1),
        key: Some("k".into()),
        argv: vec!["sh".into(), "-c".into(), script.into()],
        cwd: dir.to_path_buf(),
        ..Opts::default()
    };
    exec::run_gated(ctx, &Config::default(), &opts, "r", hooks).unwrap()
}

fn stale_left(ctx: &Context) -> usize {
    history::for_key(ctx.paths.history_file(), "stale")
        .unwrap()
        .len()
}

/// No tidying up after a command that a signal ended: whoever stopped it
/// wants the process gone.
#[test]
fn run_gated_does_not_sweep_after_a_command_that_a_signal_ended() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    stale_history_and_a_due_stamp(&ctx);
    let got = gated(&ctx, dir.path(), "kill -TERM $$", &Hooks::quiet());
    assert!(matches!(
        got,
        Gated::Finished(task_traffic_controller::runner::Exit::Signal(libc::SIGTERM))
    ));
    assert_eq!(stale_left(&ctx), 1);
    // The same command ending by itself: swept.
    let got = gated(&ctx, dir.path(), "exit 3", &Hooks::quiet());
    assert!(matches!(got, Gated::Finished(_)));
    assert_eq!(stale_left(&ctx), 0);
}

/// A stop signal that arrives when the command has just exited belongs to
/// the caller: `run_gated` does not sweep, and does not take the signal.
#[test]
fn run_gated_leaves_a_late_stop_signal_to_its_caller_and_does_not_sweep() {
    use task_traffic_controller::Supervisor;
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    stale_history_and_a_due_stamp(&ctx);
    let stops = Arc::new(Stops {
        late_signal: true,
        ..Stops::default()
    });
    let ctx = ctx.with_supervisor(stops.clone());
    let got = gated(&ctx, dir.path(), "true", &Hooks::quiet());
    assert!(matches!(got, Gated::Finished(_)));
    assert_eq!(stale_left(&ctx), 1);
    assert_eq!(stops.take_pending(), libc::SIGTERM);
}

/// A panic in the cleanup stays in the cleanup: the command's result is
/// returned as it was, by `run_gated` and by `managed::finish`, and the
/// failure is said once.
#[test]
fn a_panic_in_the_sweep_changes_nothing_for_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    stale_history_and_a_due_stamp(&ctx);
    let ctx = ctx.with_supervisor(Arc::new(Stops {
        panics: true,
        ..Stops::default()
    }));
    let said = std::sync::Mutex::new(Vec::new());
    let hooks = Hooks {
        progress: &|line| said.lock().unwrap().push(line.to_string()),
        ..Hooks::quiet()
    };
    let got = gated(&ctx, dir.path(), "exit 7", &hooks);
    assert!(matches!(
        got,
        Gated::Finished(task_traffic_controller::runner::Exit::Code(7))
    ));
    let cleanup: Vec<String> = said
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.contains("cleanup:"))
        .cloned()
        .collect();
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    assert!(cleanup[0].contains("internal error"), "{cleanup:?}");
    // Not said a second time in the same process.
    let again = gated(&ctx, dir.path(), "exit 7", &hooks);
    assert!(matches!(again, Gated::Finished(_)));
    let lines = said.lock().unwrap();
    assert_eq!(lines.iter().filter(|l| l.contains("cleanup:")).count(), 1);
    drop(lines);

    let spec = CommandSpec {
        command_id: "c".into(),
        spec_digest: "d".into(),
        opts: Opts {
            weight: Some(1),
            key: Some("k".into()),
            argv: vec!["true".into()],
            cwd: dir.path().to_path_buf(),
            ..Opts::default()
        },
        attempt: None,
        services: vec![],
    };
    let managed::Registration::Registered(_, guard) =
        managed::register(&ctx, &Config::default(), &spec).unwrap()
    else {
        panic!("not registered");
    };
    managed::finish(&ctx, guard, managed::TerminalKind::Exited { code: 0 }).unwrap();
    assert_eq!(stale_left(&ctx), 1);
}

#[test]
fn an_automatic_sweep_cuts_a_few_logs_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = config(&ctx, "jobs_log_max_mib = 1\n");
    let n = retention::AUTO_MAX_CUTS + 2;
    for i in 0..n {
        let id = format!("j{i}");
        finished_job(&ctx, &id, now - Span::hours(2), now - Span::hours(1));
        std::fs::write(ctx.paths.job_log(&id), vec![b'a'; (1 << 20) + 1]).unwrap();
    }
    old_stamp(&ctx);
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
    assert_eq!(report.logs_cut, retention::AUTO_MAX_CUTS);
    assert!(!report.complete);
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due again");
    assert_eq!(report.logs_cut, 2);
    assert!(report.complete);
}

#[test]
fn a_job_whose_descriptor_cannot_be_read_goes_after_an_hour_unless_it_is_live() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    let jobs_dir = ctx.paths.jobs_dir();
    std::fs::create_dir_all(&jobs_dir).unwrap();
    let old = STRAY_AFTER_SECS + 60;
    let put = |name: &str, secs: i64| {
        let p = jobs_dir.join(name);
        std::fs::write(&p, "{").unwrap();
        age(&p, secs);
    };
    for id in ["damaged", "leased", "waited"] {
        put(&format!("{id}.json"), old);
        put(&format!("{id}.log"), old);
        put(&format!("{id}.exit"), old);
    }
    put("fresh.json", 60);
    put("fresh.log", old);
    let _lease = lock::take_lease(&ctx, "leased").unwrap();
    let waiter = jobs::take_waiter(&ctx, "waited").unwrap();
    let report = retention::sweep(&ctx, &Config::default());
    assert_eq!(report.strays_removed, 1, "{report}");
    let names = || {
        let mut v: Vec<String> = std::fs::read_dir(&jobs_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| !n.ends_with(".waiter"))
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        names(),
        [
            "fresh.json",
            "fresh.log",
            "leased.exit",
            "leased.json",
            "leased.log",
            "waited.exit",
            "waited.json",
            "waited.log"
        ]
    );
    drop(waiter);
    age(&jobs_dir.join("waited.waiter"), old);
    assert_eq!(retention::sweep(&ctx, &Config::default()).strays_removed, 1);
    assert!(!jobs_dir.join("waited.json").exists());
    assert!(!jobs_dir.join("waited.waiter").exists());
}

#[test]
fn temp_files_stay_while_their_writer_lives_and_unknown_names_in_managed_stay() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, _clock) = ctx(dir.path());
    let m = ctx.paths.managed_dir();
    std::fs::create_dir_all(&m).unwrap();
    let elsewhere = dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let ctx = Context::new(
        ctx.paths
            .clone()
            .with_history_file(elsewhere.join("history.jsonl")),
    )
    .with_live(Arc::new(FixedLive(None)))
    .with_boot(Arc::new(FixedBoot(Some("boot-1".into()))));
    let mut child = std::process::Command::new("true").spawn().unwrap();
    let gone = child.id();
    child.wait().unwrap();
    let me = std::process::id();
    let old = STRAY_AFTER_SECS + 60;
    let put = |p: std::path::PathBuf, secs: i64| {
        std::fs::write(&p, "x").unwrap();
        age(&p, secs);
    };
    // A writer stopped for longer than the hour keeps its file.
    put(m.join(format!(".ttc-tmp.{me}.stalled")), old);
    put(m.join(format!(".ttc-tmp.{gone}.crashed")), old);
    put(m.join(format!(".ttc-tmp.{gone}.young")), 60);
    put(m.join(".ttc-tmp.1.init"), old);
    put(m.join(".ttc-tmp.nopid"), old);
    put(m.join(".tmpAbC123"), old);
    put(m.join("notes.txt"), old);
    put(m.join("backup.json.bak"), old);
    // Next to a history file kept elsewhere: only this version's own names.
    put(elsewhere.join(format!(".ttc-tmp.{gone}.crashed")), old);
    put(elsewhere.join(".tmpAbC123"), old);
    put(elsewhere.join("theirs.txt"), old);

    let report = retention::sweep(&ctx, &Config::default());
    assert_eq!(report.strays_removed, 3, "{report}");
    let names = |d: &Path| {
        let mut v: Vec<String> = std::fs::read_dir(d)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    };
    let mut want = vec![
        ".ttc-tmp.1.init".to_string(),
        format!(".ttc-tmp.{gone}.young"),
        format!(".ttc-tmp.{me}.stalled"),
        ".ttc-tmp.nopid".to_string(),
        "backup.json.bak".to_string(),
        "notes.txt".to_string(),
    ];
    want.sort();
    assert_eq!(names(&m), want);
    assert_eq!(names(&elsewhere), [".tmpAbC123", "theirs.txt"]);
}

#[test]
fn a_measurement_recorded_twice_keeps_its_marker_while_one_copy_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let mut r = record("k", clock.now());
    r.measurement_id = Some("dup".into());
    assert_eq!(
        managed::record_measurement(&ctx, "dup", &r).unwrap(),
        managed::Measured::Applied
    );
    // A second copy with the same id got into the file (a resend an older
    // version let through).
    history::append(ctx.paths.history_file(), &r).unwrap();
    assert_eq!(managed::trim_history(&ctx, 1).unwrap(), 1);
    assert!(ctx.paths.measurements_dir().join("dup.applied").exists());
    assert_eq!(
        managed::record_measurement(&ctx, "dup", &r).unwrap(),
        managed::Measured::Duplicate
    );
    assert_eq!(
        history::read_all(ctx.paths.history_file()).unwrap().len(),
        1
    );
}

/// Many appenders against a trim that rewrites the file again and again:
/// every record of the key that is never trimmed must still be there.
#[test]
fn appends_during_trims_are_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    const WRITERS: usize = 4;
    const EACH: usize = 150;
    let trimmer = {
        let (ctx, stop) = (ctx.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut trims = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                // Something to drop each time, so the file is rewritten.
                managed::append_record(&ctx, &record("stale", now - Span::days(400))).unwrap();
                trims += managed::trim_history_aged(&ctx, 1_000_000, Some(now - Span::days(30)))
                    .unwrap();
            }
            trims
        })
    };
    let writers: Vec<_> = (0..WRITERS)
        .map(|_| {
            let ctx = ctx.clone();
            std::thread::spawn(move || {
                for _ in 0..EACH {
                    managed::append_record(&ctx, &record("live", now)).unwrap();
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(trimmer.join().unwrap() > 0, "no trim rewrote the file");
    assert_eq!(
        history::for_key(ctx.paths.history_file(), "live")
            .unwrap()
            .len(),
        WRITERS * EACH
    );
}

/// The same against appenders that do not take the history lock, as a
/// released version does not. A record appended in the instant between the
/// trim's last look and its rename is lost (the documented rest); what
/// must hold is that the file never gets half a line, and no record twice.
#[test]
fn unlocked_appends_during_trims_leave_whole_lines_and_no_record_twice() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    const WRITERS: usize = 4;
    const EACH: usize = 150;
    let trimmer = {
        let (ctx, stop) = (ctx.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut trims = 0;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                managed::append_record(&ctx, &record("stale", now - Span::days(400))).unwrap();
                trims += managed::trim_history_aged(&ctx, 1_000_000, Some(now - Span::days(30)))
                    .unwrap();
            }
            trims
        })
    };
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let path = ctx.paths.history_file().to_path_buf();
            std::thread::spawn(move || {
                for i in 0..EACH {
                    let mut r = record("live", now);
                    r.measurement_id = Some(format!("w{w}-{i}"));
                    history::append(&path, &r).unwrap();
                }
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(trimmer.join().unwrap() > 0, "no trim rewrote the file");
    let text = std::fs::read_to_string(ctx.paths.history_file()).unwrap();
    assert!(
        text.is_empty() || text.ends_with('\n'),
        "ends in half a line"
    );
    let mut ids = Vec::new();
    for line in text.lines() {
        let r: Record =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("half a line: {line:?}: {e}"));
        if r.key == "live" {
            ids.push(r.measurement_id.unwrap());
        }
    }
    let n = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), n, "a record is there twice");
    assert!(n <= WRITERS * EACH && n > 0);
}

/// A lease taken while a sweep removes lease files nobody holds must be a
/// lock on the file the name points to: otherwise the next look finds the
/// lease free and the entry is reaped while its command runs.
#[test]
fn a_lease_taken_during_an_orphan_sweep_is_held_on_the_file_the_name_points_to() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    // Every lease file looks old to the sweeper, so only the locks protect.
    clock.advance(Duration::from_secs(
        2 * lock::ORPHAN_LEASE_AFTER_SECS as u64,
    ));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sweeper = {
        let (ctx, stop) = (ctx.clone(), stop.clone());
        std::thread::spawn(move || {
            let empty = task_traffic_controller::ledger::Ledger::default();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                lock::sweep_orphan_leases(&ctx, &empty);
            }
        })
    };
    for i in 0..400 {
        let id = format!("l{}", i % 3);
        let lease = lock::take_lease(&ctx, &id).unwrap();
        assert!(lock::lease_is_held(&ctx, &id), "round {i}: lease not held");
        assert!(ctx.paths.lease_file(&id).exists(), "round {i}: file gone");
        drop(lease);
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    sweeper.join().unwrap();
    // And an old file nobody holds does go, a young one does not.
    let empty = task_traffic_controller::ledger::Ledger::default();
    lock::sweep_orphan_leases(&ctx, &empty);
    let old = ctx.paths.lease_file("old");
    drop(lock::take_lease(&ctx, "old").unwrap());
    let held = lock::take_lease(&ctx, "held").unwrap();
    assert_eq!(lock::sweep_orphan_leases(&ctx, &empty), 1);
    assert!(!old.exists());
    assert!(ctx.paths.lease_file("held").exists());
    drop(held);
    let (young_ctx, _) = self::ctx(dir.path());
    drop(lock::take_lease(&young_ctx, "young").unwrap());
    assert_eq!(lock::sweep_orphan_leases(&young_ctx, &empty), 0);
    assert!(young_ctx.paths.lease_file("young").exists());
}
