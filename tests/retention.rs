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
    Context::new(Paths::new(dir.join("state"), dir.join("config.toml")))
        .with_live(Arc::new(FixedLive(None)))
        .with_boot(Arc::new(FixedBoot(Some("boot-1".into()))))
        .with_clock(clock)
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
    let managed::Registration::Registered(_, guard) = managed::register(&ctx, &cfg, &spec).unwrap()
    else {
        panic!("not registered");
    };
    // Nothing before the command: registering costs no sweep.
    assert_eq!(
        history::read_all(ctx.paths.history_file()).unwrap().len(),
        1
    );
    managed::finish(&ctx, guard, managed::TerminalKind::Cancelled).unwrap();
    assert!(history::read_all(ctx.paths.history_file())
        .unwrap()
        .is_empty());
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
    let cfg = Config {
        jobs_log_max_mib: 1,
        ..Config::default()
    };
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
    let cfg = Config {
        jobs_log_max_mib: 1,
        ..Config::default()
    };
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
    let cfg = Config {
        jobs_log_max_mib: 1,
        ..Config::default()
    };
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
    old_stamp(&ctx);
    // Somebody holds the ledger lock for as long as it likes.
    let held = lock::ledger(&ctx).unwrap();
    let started = std::time::Instant::now();
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due");
    // It did not wait for the lock (10 s by default)...
    assert!(started.elapsed() < Duration::from_secs(5));
    // ...did the stage that was free, and says the rest is left.
    assert!(!report.complete);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!((report.history_dropped, report.jobs_removed), (1, 0));
    // Not again at once, and not six hours later either.
    assert!(retention::sweep_if_due(&ctx, &cfg).is_none());
    clock.advance(Duration::from_secs(retention::RETRY_AFTER_SECS as u64));
    drop(held);
    let report = retention::sweep_if_due(&ctx, &cfg).expect("due again");
    assert!(report.complete);
    assert_eq!(report.jobs_removed, 1);
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

#[test]
fn a_program_with_shorter_numbers_does_not_remove_what_another_asked_to_keep() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    for i in 0..100 {
        history::append(
            ctx.paths.history_file(),
            &record("k", now - Span::seconds(i)),
        )
        .unwrap();
    }
    let long = Config {
        history_keep: 100,
        ..Config::default()
    };
    let short = Config {
        history_keep: 50,
        ..Config::default()
    };
    assert_eq!(retention::sweep(&ctx, &long).history_dropped, 0);
    // The other program's sweep, with its own shorter number: nothing goes.
    assert_eq!(retention::sweep(&ctx, &short).history_dropped, 0);
    // Until the first one has not asked for 30 days.
    clock.advance(Duration::from_secs(retention::ASK_TTL_SECS as u64 - 1));
    history::append(ctx.paths.history_file(), &record("k", clock.now())).unwrap();
    assert_eq!(retention::sweep(&ctx, &short).history_dropped, 1);
    clock.advance(Duration::from_secs(1));
    assert_eq!(retention::sweep(&ctx, &short).history_dropped, 50);
}

/// The program with the shorter numbers does every sweep; the other only
/// ever runs commands in between. Its numbers still count, from its first
/// command on and for as long as it is in use.
#[test]
fn a_program_that_never_does_the_sweep_itself_is_still_heard() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    // (Age plays no part here: the key is kept however old.)
    let long = Config {
        history_keep: 100,
        history_keep_days: 3650,
        ..Config::default()
    };
    let short = Config {
        history_keep_days: 3650,
        ..Config::default()
    };
    assert_eq!(short.history_keep, 50);
    // The short one is first in the state directory: it arms the clock.
    assert!(retention::sweep_if_due(&ctx, &short).is_none());
    for i in 0..100 {
        let at = clock.now() - Span::seconds(i);
        history::append(ctx.paths.history_file(), &record("k", at)).unwrap();
    }
    let kept = || history::read_all(ctx.paths.history_file()).unwrap().len();
    // Six hours a step for 40 days: the long one is never the sweeper.
    for step in 0..160 {
        assert!(
            retention::sweep_if_due(&ctx, &long).is_none(),
            "step {step}: the long one swept"
        );
        clock.advance(Duration::from_secs(SWEEP_EVERY_SECS as u64));
        let report = retention::sweep_if_due(&ctx, &short).expect("due");
        assert_eq!(report.history_dropped, 0, "step {step}");
    }
    assert_eq!(kept(), 100);
    // The long one is gone for 30 days: the short numbers apply.
    let mut dropped = 0;
    for _ in 0..121 {
        clock.advance(Duration::from_secs(SWEEP_EVERY_SECS as u64));
        dropped += retention::sweep_if_due(&ctx, &short).map_or(0, |r| r.history_dropped);
    }
    assert_eq!(dropped, 50);
    assert_eq!(kept(), 50);
}

#[test]
fn an_automatic_sweep_cuts_a_few_logs_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let now = clock.now();
    let cfg = Config {
        jobs_log_max_mib: 1,
        ..Config::default()
    };
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
