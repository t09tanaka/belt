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
    // And the run itself was recorded after the sweep.
    assert_eq!(
        history::read_all(ctx.paths.history_file()).unwrap().len(),
        1
    );
}

#[test]
fn register_sweeps_by_itself_when_due() {
    let dir = tempfile::tempdir().unwrap();
    let (ctx, clock) = ctx(dir.path());
    let old = clock.now() - Span::days(400);
    history::append(ctx.paths.history_file(), &record("stale", old)).unwrap();
    std::fs::write(ctx.paths.sweep_stamp(), "not a time").unwrap();
    // An unreadable stamp only re-arms the clock.
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
    let reg = managed::register(&ctx, &cfg, &spec).unwrap();
    assert_eq!(
        history::read_all(ctx.paths.history_file()).unwrap().len(),
        1
    );
    drop(reg);
    std::fs::write(ctx.paths.sweep_stamp(), "2000-01-01T00:00:00Z").unwrap();
    let spec = CommandSpec {
        command_id: "c2".into(),
        ..spec
    };
    let _reg = managed::register(&ctx, &cfg, &spec).unwrap();
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
    assert_eq!(report.strays_removed, 5, "{report}");
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
    assert!(!ctx.paths.leases_dir().exists());
    assert!(!ctx.paths.jobs_dir().exists());
    // Every writer makes its directory again.
    jobs::save(&ctx, &job("j", clock.now())).unwrap();
    drop(lock::take_lease(&ctx, "y").unwrap());
    let mut r = record("k", clock.now());
    r.measurement_id = Some("n".into());
    managed::record_measurement(&ctx, "n", &r).unwrap();
}
