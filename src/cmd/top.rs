//! `top`: a full-screen view of the queue (alternate screen, raw mode),
//! refreshed every `--interval` seconds and on resize.
//!
//! [`View`] is a plain description of what is on screen, built from the
//! ledger by [`View::build`] (pure) and drawn by [`draw`]. Everything that
//! decides what a row says (order, numbers, the shared time axis, the bar
//! strings, worktree colors, column widths) is a pure function below, so it
//! is tested without a terminal.

use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};
use ratatui::Frame;
use task_traffic_controller::admit::{self, Decision, Limits, LiveSnapshot, Reason, State};
use task_traffic_controller::config::Config;
use task_traffic_controller::display::{fmt_bytes, fmt_duration, is_idle, running_for, truncate};
use task_traffic_controller::error::{Error, Result};
use task_traffic_controller::history::{self, Record};
use task_traffic_controller::ledger::{Entry, Ledger};
use task_traffic_controller::{eta, exec, gitctx, key, lock, managed, Context};

/// One running entry's share of the cores bar.
#[derive(Debug, Clone, PartialEq)]
pub struct Slot {
    /// The row number in the table.
    pub num: usize,
    pub slots: u32,
    /// Index into [`View::worktrees`].
    pub wt: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Progress {
    /// No learned wall time.
    Unknown,
    /// `pct` is capped at 99 while the command is still running. `left` is
    /// the time to the expected end, or the overrun when `over`.
    Known { pct: u8, left: Duration, over: bool },
}

/// A worktree with something queued or running in it.
#[derive(Debug, Clone, PartialEq)]
pub struct Wt {
    pub path: String,
    /// Branch, or the short commit hash when detached; empty when unknown.
    pub branch: String,
    /// Where `top` was started.
    pub mine: bool,
    pub color: Color,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunRow {
    /// Position in the table, from 1.
    pub num: usize,
    pub project: String,
    pub job: String,
    pub weight: u32,
    pub using: Option<f64>,
    pub elapsed: Duration,
    pub expected_secs: Option<f64>,
    pub progress: Progress,
    pub idle: bool,
    pub backfilled: bool,
    pub wt: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WaitRow {
    pub num: usize,
    pub project: String,
    pub job: String,
    pub weight: u32,
    pub waited: Duration,
    pub front: bool,
    pub why: String,
    /// Seconds until the estimated start; None when there is no estimate.
    pub start_in: Option<f64>,
    /// The learned wall time; None when the command has no history.
    pub expected_secs: Option<f64>,
    pub rough: bool,
    pub wt: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeldRow {
    pub id: String,
    pub reason: String,
    pub hint: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Machine {
    pub cpu_busy: f64,
    pub available_memory: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct View {
    pub now: DateTime<Utc>,
    pub budget: u32,
    pub used: u32,
    /// One per running entry, in table order. Slots sum to at most
    /// `budget`; `slots + free == budget`.
    pub slots: Vec<Slot>,
    pub free: u32,
    /// Slots charged beyond the budget (measured use above the weight).
    pub over: u32,
    pub machine: Option<Machine>,
    /// In order of first appearance in the table.
    pub worktrees: Vec<Wt>,
    /// Soonest to finish first.
    pub running: Vec<RunRow>,
    /// In queue order; numbered after the running rows.
    pub waiting: Vec<WaitRow>,
    pub held: Vec<HeldRow>,
    /// The latest finished runs, newest first. Only filled when nothing is
    /// running or waiting.
    pub recent: Vec<RecentRow>,
}

/// A finished run, from `history.jsonl`.
#[derive(Debug, Clone, PartialEq)]
pub struct RecentRow {
    pub project: String,
    pub job: String,
    pub took: Duration,
    pub ago: Duration,
    /// The exit code when it was not 0.
    pub failed: Option<i32>,
    /// The worktree it ran from, when recorded.
    pub wt: Option<Wt>,
}

impl View {
    /// Read the ledger and the machine, then [`View::from_parts`]. `me` is
    /// the worktree `top` was started in.
    pub fn build(ctx: &Context, cfg: &Config, me: Option<&str>) -> Result<View> {
        let ledger = lock::load_reaped(ctx)?;
        let held = managed::views(ctx, &ledger.entries)
            .into_iter()
            .filter(|v| v.quarantined)
            .map(|v| HeldRow {
                id: v.command_id,
                reason: v.reason,
                hint: v.hint,
            })
            .collect();
        // The history is only needed (and only read) for an empty queue.
        let recent = if ledger.entries.is_empty() {
            history::read_all(ctx.paths.history_file()).unwrap_or_default()
        } else {
            Vec::new()
        };
        Ok(View::from_parts(
            &ledger,
            held,
            cfg.limits(),
            ctx.live.snapshot(),
            ctx.clock.now(),
            me,
            &recent,
        ))
    }

    pub fn from_parts(
        ledger: &Ledger,
        held: Vec<HeldRow>,
        limits: Limits,
        live: Option<LiveSnapshot>,
        now: DateTime<Utc>,
        me: Option<&str>,
        history: &[Record],
    ) -> View {
        let queue = ledger.queue_at(now);
        let used = admit::used_budget(&queue);
        let timed: Vec<eta::Timed> = queue
            .iter()
            .zip(&ledger.entries)
            .map(|(entry, e)| eta::Timed {
                entry: entry.clone(),
                elapsed_secs: e
                    .started_at
                    .map(|s| (now - s).num_milliseconds().max(0) as f64 / 1000.0)
                    .unwrap_or(0.0),
            })
            .collect();

        // Budget slots are handed out in start order (the oldest run keeps
        // its share when the total overflows); the table is ordered by
        // finish. Remember each run's slots by id.
        let mut by_start: Vec<&Entry> = ledger
            .entries
            .iter()
            .filter(|e| e.state == State::Running)
            .collect();
        by_start.sort_by_key(|e| e.started_at);
        let mut fits: HashMap<&str, u32> = HashMap::new();
        let mut taken = 0u32;
        let mut over = 0u32;
        for e in &by_start {
            let charged = queue
                .iter()
                .find(|q| q.id == e.id)
                .map(admit::charged)
                .unwrap_or(e.weight);
            let fit = charged.min(limits.budget.saturating_sub(taken));
            over += charged - fit;
            taken += fit;
            fits.insert(e.id.as_str(), fit);
        }

        let mut running = by_start;
        order_running(&mut running, now);
        let order = admit::queue_order(&queue);
        let waiting: Vec<&Entry> = order.iter().filter_map(|q| ledger.find(&q.id)).collect();

        let table: Vec<&Entry> = running.iter().chain(&waiting).copied().collect();
        let worktrees = worktrees_of(&table, me);
        let wt_of = |e: &Entry| -> Option<usize> {
            let p = e.worktree.as_deref()?;
            worktrees.iter().position(|w| w.path == p)
        };

        let mut slots = Vec::new();
        let mut run_rows = Vec::new();
        for (i, e) in running.iter().enumerate() {
            let num = i + 1;
            slots.push(Slot {
                num,
                slots: fits.get(e.id.as_str()).copied().unwrap_or(0),
                wt: wt_of(e),
            });
            let elapsed = running_for(e, now);
            run_rows.push(RunRow {
                num,
                project: project(e),
                job: job_name(e),
                weight: e.weight,
                using: e.using,
                elapsed,
                expected_secs: e.expected_secs.filter(|x| *x > 0.0),
                progress: progress(elapsed, e.expected_secs),
                idle: is_idle(e.using, elapsed),
                backfilled: e.backfilled,
                wt: wt_of(e),
            });
        }

        let mut wait_rows = Vec::new();
        for (i, e) in waiting.iter().enumerate() {
            let Some(q) = queue.iter().find(|q| q.id == e.id) else {
                continue;
            };
            let est = eta::estimate(&timed, limits, &e.id);
            wait_rows.push(WaitRow {
                num: run_rows.len() + i + 1,
                project: project(e),
                job: job_name(e),
                weight: e.weight,
                waited: (now - e.enqueued_at).to_std().unwrap_or_default(),
                front: e.front,
                why: why(&queue, q, limits, live),
                start_in: est.as_ref().map(|x| x.start_in),
                expected_secs: e.expected_secs.filter(|x| *x > 0.0),
                rough: est.as_ref().is_some_and(|x| x.rough),
                wt: wt_of(e),
            });
        }

        View {
            now,
            budget: limits.budget,
            used,
            slots,
            free: limits.budget.saturating_sub(taken),
            over,
            machine: live.map(|s| Machine {
                cpu_busy: s.cpu_busy,
                available_memory: s.available_memory,
            }),
            worktrees,
            recent: if run_rows.is_empty() && wait_rows.is_empty() {
                recent_rows(history, now, me, RECENT_MAX)
            } else {
                Vec::new()
            },
            running: run_rows,
            waiting: wait_rows,
            held,
        }
    }
}

/// Seconds a running entry is still expected to run, or `None` without a
/// learned wall time. An overrun counts as 0 (it may end any moment).
fn remaining_secs(e: &Entry, now: DateTime<Utc>) -> Option<f64> {
    let exp = e.expected_secs.filter(|x| *x > 0.0)?;
    Some((exp - running_for(e, now).as_secs_f64()).max(0.0))
}

/// Soonest to finish first; runs with no learned time last, oldest first.
fn order_running(running: &mut [&Entry], now: DateTime<Utc>) {
    running.sort_by(|a, b| {
        let (x, y) = (remaining_secs(a, now), remaining_secs(b, now));
        match (x, y) {
            (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then(a.started_at.cmp(&b.started_at))
    });
}

/// Repo basename, plus `/subdir` when there is one.
fn project(e: &Entry) -> String {
    if e.repo.is_empty() {
        "-".into()
    } else {
        key::project_label(&e.repo, &e.subdir)
    }
}

/// The `--label`; otherwise the script's file name for `bash ... step.sh`
/// style commands; otherwise the whole command line.
fn job_name(e: &Entry) -> String {
    if let Some(l) = e.label.as_deref().filter(|l| !l.is_empty()) {
        return l.to_string();
    }
    job_of_argv(&e.argv)
}

fn job_of_argv(argv: &str) -> String {
    script_name(argv).unwrap_or_else(|| argv.to_string())
}

fn script_name(argv: &str) -> Option<String> {
    let mut words = argv.split_whitespace();
    let shell = Path::new(words.next()?).file_name()?.to_str()?;
    if !matches!(shell, "sh" | "bash" | "zsh" | "dash") {
        return None;
    }
    words.filter(|w| !w.starts_with('-')).find_map(|w| {
        let name = Path::new(w).file_name()?.to_str()?;
        name.ends_with(".sh").then(|| name.to_string())
    })
}

fn progress(elapsed: Duration, expected_secs: Option<f64>) -> Progress {
    let Some(exp) = expected_secs.filter(|x| *x > 0.0) else {
        return Progress::Unknown;
    };
    let el = elapsed.as_secs_f64();
    let pct = ((el / exp) * 100.0).floor().clamp(0.0, 99.0) as u8;
    Progress::Known {
        pct,
        left: Duration::from_secs_f64((exp - el).abs()),
        over: el > exp,
    }
}

/// Why the waiting entry `q` is not running yet, from the admit decision.
fn why(
    queue: &[admit::QueueEntry],
    q: &admit::QueueEntry,
    limits: Limits,
    live: Option<LiveSnapshot>,
) -> String {
    match admit::admit(queue, &q.id, 0, q.waited_secs, limits, live) {
        Decision::Admitted => "starting".into(),
        Decision::Wait(Reason::Budget { used, budget }) => format!(
            "needs {}, {} free",
            q.weight.min(budget).max(1),
            budget.saturating_sub(used)
        ),
        Decision::Wait(Reason::NotHead { ahead }) => format!("behind {ahead}"),
        Decision::Wait(Reason::Reserved { ahead }) => format!("behind {ahead}, reserved"),
        Decision::Wait(r) => exec::describe(&r),
    }
}

// ------------------------------------------------------------- worktrees

/// Colors a worktree can get. No red (overruns), yellow (idle) or gray
/// (a repository with a single worktree).
const WT_COLORS: [Color; 8] = [
    Color::Cyan,
    Color::Green,
    Color::Magenta,
    Color::Blue,
    Color::LightCyan,
    Color::LightGreen,
    Color::LightMagenta,
    Color::LightBlue,
];

/// FNV-1a: stable across runs and platforms, unlike `DefaultHasher`.
fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The colors of the worktrees of one repository, in the order of `paths`.
///
/// Each path starts at the palette slot its hash picks; if a worktree
/// before it (in sorted path order, so the result does not depend on the
/// order given) already took that color, it moves on to the next free slot.
/// Colors repeat only when there are more worktrees than colors. Gray when
/// there is a single worktree: there is nothing to tell apart.
fn repo_colors(paths: &[&str]) -> Vec<Color> {
    if paths.len() <= 1 {
        return vec![Color::Gray; paths.len()];
    }
    let mut sorted: Vec<&str> = paths.to_vec();
    sorted.sort_unstable();
    let n = WT_COLORS.len();
    let mut taken = vec![false; n];
    let mut slot_of: HashMap<&str, usize> = HashMap::new();
    for p in sorted {
        let start = (fnv1a(p) % n as u64) as usize;
        let slot = (0..n)
            .map(|k| (start + k) % n)
            .find(|&c| !taken[c])
            .unwrap_or(start);
        taken[slot] = true;
        // Past the palette size every slot is taken: start over, so the
        // repeats are spread as evenly as the palette allows.
        if taken.iter().all(|t| *t) {
            taken.fill(false);
        }
        slot_of.insert(p, slot);
    }
    paths.iter().map(|p| WT_COLORS[slot_of[p]]).collect()
}

/// The distinct worktrees of `table` (running first, then waiting), in
/// order of first appearance. The branch is the newest entry's.
fn worktrees_of(table: &[&Entry], me: Option<&str>) -> Vec<Wt> {
    let mut out: Vec<(Wt, u64, String)> = Vec::new();
    for e in table {
        let Some(path) = e.worktree.as_deref() else {
            continue;
        };
        let branch = e.branch.clone().unwrap_or_default();
        match out.iter_mut().find(|(w, _, _)| w.path == path) {
            Some((w, seq, _)) => {
                if e.seq > *seq {
                    *seq = e.seq;
                    w.branch = branch;
                }
            }
            None => out.push((
                Wt {
                    path: path.to_string(),
                    branch,
                    mine: me == Some(path),
                    color: Color::Gray,
                },
                e.seq,
                e.repo.clone(),
            )),
        }
    }
    let repos: Vec<String> = out.iter().map(|(_, _, r)| r.clone()).collect();
    let mut result: Vec<Wt> = out.into_iter().map(|(w, _, _)| w).collect();
    for repo in repos.iter().collect::<std::collections::BTreeSet<_>>() {
        let idx: Vec<usize> = (0..result.len()).filter(|&i| repos[i] == *repo).collect();
        let paths: Vec<&str> = idx.iter().map(|&i| result[i].path.as_str()).collect();
        let colors = repo_colors(&paths);
        for (&i, c) in idx.iter().zip(colors) {
            result[i].color = c;
        }
    }
    result
}

/// `s` cut to `max` characters with the middle replaced by `…`.
fn ellipsize_middle(s: &str, max: usize) -> String {
    let n = s.chars().count();
    if n <= max {
        return s.to_string();
    }
    if max <= 1 {
        return "…".repeat(max);
    }
    let keep = max - 1;
    let head = keep.div_ceil(2);
    let tail = keep - head;
    let h: String = s.chars().take(head).collect();
    let t: String = s.chars().skip(n - tail).collect();
    format!("{h}…{t}")
}

// ------------------------------------------------------- the time axis

/// Seconds per column the axis can use, finest first.
const SCALES: [f64; 24] = [
    1.0, 2.0, 3.0, 5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 180.0, 300.0, 450.0,
    600.0, 900.0, 1200.0, 1800.0, 3600.0, 7200.0, 14400.0, 28800.0, 86400.0,
];

/// Tick spacings for the labels on top.
const TICKS: [f64; 14] = [
    10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0, 1800.0, 3600.0, 7200.0, 14400.0, 28800.0,
    43200.0, 86400.0,
];

/// What one bar needs to know. Times in seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BarSpec {
    Running {
        elapsed: f64,
        expected: Option<f64>,
    },
    Waiting {
        start_in: Option<f64>,
        expected: Option<f64>,
        rough: bool,
    },
}

impl BarSpec {
    fn of_run(r: &RunRow) -> BarSpec {
        BarSpec::Running {
            elapsed: r.elapsed.as_secs_f64(),
            expected: r.expected_secs,
        }
    }

    fn of_wait(w: &WaitRow) -> BarSpec {
        BarSpec::Waiting {
            start_in: w.start_in,
            expected: w.expected_secs,
            rough: w.rough,
        }
    }

    /// Seconds to the right of "now" this bar needs.
    fn ahead(&self) -> f64 {
        match *self {
            BarSpec::Running { elapsed, expected } => {
                expected.map_or(0.0, |x| (x - elapsed).max(0.0))
            }
            BarSpec::Waiting {
                start_in, expected, ..
            } => start_in.unwrap_or(0.0).max(0.0) + expected.unwrap_or(0.0),
        }
    }

    /// Seconds to the left of "now".
    fn behind(&self) -> f64 {
        match *self {
            BarSpec::Running { elapsed, .. } => elapsed,
            BarSpec::Waiting { .. } => 0.0,
        }
    }
}

/// One scale for every row: `now` sits at column `now_col`, a column is
/// `secs_per_col` seconds wide.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scale {
    pub secs_per_col: f64,
    pub now_col: usize,
    pub width: usize,
}

/// Columns needed to show `secs` (at least one when there is any).
fn cols_for(secs: f64, per_col: f64) -> usize {
    if secs <= 0.0 {
        0
    } else {
        ((secs / per_col).ceil() as usize).max(1)
    }
}

/// The finest scale at which the row that ends last still fits in `width`
/// columns. What has already run takes at most a third of the width, so an
/// old run cannot squeeze out the future; the rest is cut with `←`. When
/// even the coarsest scale is too small, the bar is cut with `→`.
pub fn compute_scale(specs: &[BarSpec], width: usize) -> Scale {
    let behind = specs.iter().map(BarSpec::behind).fold(0.0, f64::max);
    let ahead = specs.iter().map(BarSpec::ahead).fold(0.0, f64::max);
    let now_col = |per: f64| cols_for(behind, per).min(width / 3);
    let per_col = SCALES
        .iter()
        .copied()
        .find(|&s| now_col(s) + 1 + cols_for(ahead, s) <= width)
        .unwrap_or(SCALES[SCALES.len() - 1]);
    Scale {
        secs_per_col: per_col,
        now_col: now_col(per_col),
        width,
    }
}

/// A bar `scale.width` characters wide.
///
/// - `█` ran, `▓` ran past its expected time, `▒` expected to run, `┄` the
///   same when the estimate is rough, `│` now.
/// - `→` the duration is unknown (just a start), or the bar goes on past
///   the right edge; `←` a run older than the left edge.
pub fn bar_string(spec: BarSpec, scale: Scale) -> String {
    let w = scale.width;
    if w == 0 {
        return String::new();
    }
    let now = scale.now_col.min(w - 1);
    let per = scale.secs_per_col;
    let mut cells = vec![' '; w];
    cells[now] = '│';
    let fill = |cells: &mut Vec<char>, from: usize, len: usize, c: char| {
        let to = (from + len).min(w);
        for cell in cells.iter_mut().take(to).skip(from) {
            *cell = c;
        }
        if from + len > w && w > 0 {
            cells[w - 1] = '→';
        }
    };
    match spec {
        BarSpec::Running { elapsed, expected } => {
            let want = cols_for(elapsed, per);
            let drawn = want.min(now);
            let start = now - drawn;
            for cell in cells.iter_mut().take(now).skip(start) {
                *cell = '█';
            }
            if let Some(x) = expected.filter(|x| elapsed > *x) {
                let over = cols_for(elapsed - x, per).min(drawn);
                for cell in cells.iter_mut().take(now).skip(now - over) {
                    *cell = '▓';
                }
            }
            if want > now && now > 0 {
                cells[0] = '←';
            }
            match expected {
                Some(x) if x > elapsed => {
                    fill(&mut cells, now + 1, cols_for(x - elapsed, per), '▒');
                }
                Some(_) => {}
                None => fill(&mut cells, now + 1, 1, '→'),
            }
        }
        BarSpec::Waiting {
            start_in,
            expected,
            rough,
        } => {
            let from = now + 1 + (start_in.unwrap_or(0.0).max(0.0) / per).floor() as usize;
            if from >= w {
                cells[w - 1] = '→';
            } else {
                match expected {
                    Some(x) => {
                        let c = if rough { '┄' } else { '▒' };
                        fill(&mut cells, from, cols_for(x, per).max(1), c);
                    }
                    None => cells[from] = '→',
                }
            }
        }
    }
    cells.into_iter().collect()
}

/// `+45s`, `+2m`, `+1h30m`.
fn fmt_tick(secs: f64) -> String {
    let s = secs.round() as u64;
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    let mut out = String::from("+");
    if h > 0 {
        out.push_str(&format!("{h}h"));
    }
    if m > 0 {
        out.push_str(&format!("{m}m"));
    }
    if sec > 0 || (h == 0 && m == 0) {
        out.push_str(&format!("{sec}s"));
    }
    out
}

/// The ruler above the bars: `now` at the vertical line, then `+2m`-style
/// labels to the right, at least 7 columns apart (closer, down to 3, when
/// nothing would fit otherwise).
pub fn axis_line(scale: Scale) -> String {
    let w = scale.width;
    let put = |cells: &mut [char], at: usize, text: &str| -> bool {
        let n = text.chars().count();
        if at + n > w {
            return false;
        }
        for (i, c) in text.chars().enumerate() {
            cells[at + i] = c;
        }
        true
    };
    let mut cells = vec![' '; w];
    let mut next_free = 0;
    if put(&mut cells, scale.now_col.min(w), "now") {
        next_free = scale.now_col + 4;
    }
    for min_cols in [7.0, 5.0, 3.0] {
        let tick = TICKS
            .iter()
            .copied()
            .find(|t| t / scale.secs_per_col >= min_cols)
            .unwrap_or(TICKS[TICKS.len() - 1]);
        let mut placed = false;
        let mut free = next_free;
        let mut line = cells.clone();
        for k in 1.. {
            let t = tick * k as f64;
            let col = scale.now_col + 1 + (t / scale.secs_per_col).floor() as usize;
            if col >= w {
                break;
            }
            let label = fmt_tick(t);
            if col >= free && put(&mut line, col, &label) {
                free = col + label.chars().count() + 1;
                placed = true;
            }
        }
        if placed || min_cols == 3.0 {
            cells = line;
            break;
        }
    }
    cells.into_iter().collect()
}

// -------------------------------------------------------------- the table

/// Column widths for a terminal `width` wide.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cols {
    /// `None` when the project column is dropped.
    pub project: Option<usize>,
    pub worktree: usize,
    pub job: usize,
    pub cores: Option<usize>,
    pub bar: usize,
    pub right: usize,
    /// The text after the bar names why a waiting entry waits.
    pub why: bool,
    /// `idle` and `[backfill]` are shown as `z` and `b`.
    pub short_flags: bool,
}

const BAR_MIN: usize = 12;
/// On a terminal this wide the bar is never narrower than [`BAR_WIDE_MIN`].
const WIDE: usize = 80;
const BAR_WIDE_MIN: usize = 20;
const BAR_FLOOR: usize = 6;
const RIGHT_MAX: usize = 34;
const RIGHT_FLOOR: usize = 8;
const PROJECT_CAP: usize = 24;
const WT_CAP: usize = 32;
const JOB_CAP: usize = 60;
const CORES_HEADER: usize = 5;

/// What each column would like, before it is fitted to the screen.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Wants {
    /// The color band, the number and the gap after them.
    pub prefix: usize,
    pub project: usize,
    pub worktree: usize,
    /// The worktree column when the project column is gone: a lone
    /// worktree's `-` is replaced by its project name.
    pub worktree_without_project: usize,
    /// The longest job name with its flags written out, and abbreviated.
    pub job: usize,
    pub job_short_flags: usize,
    pub cores: usize,
    /// The text after the bar with and without the reason for waiting.
    pub right: usize,
    pub right_short: usize,
}

/// Fit the columns in `width`, keeping the bar readable.
///
/// On a terminal at least [`WIDE`] columns wide the bar is never narrower
/// than [`BAR_WIDE_MIN`]. To get there, in this order: the reason for
/// waiting after the bar goes, `idle` and `[backfill]` shrink to `z` and
/// `b`, the project column goes, the cores column goes, and only then the
/// worktree and job names are cut (the branch in the middle). The text
/// after the bar, the bar, the worktree and the job are never dropped.
pub fn layout(width: usize, w: Wants) -> Cols {
    let bar_min = if width >= WIDE { BAR_WIDE_MIN } else { BAR_MIN };
    let pw = w.project.clamp(7, PROJECT_CAP);
    // (why, short flags, project, cores), most to least.
    let states = [
        (true, false, true, true),
        (false, false, true, true),
        (false, true, true, true),
        (false, true, false, true),
        (false, true, false, false),
    ];
    let build = |(why, short, project, cores): (bool, bool, bool, bool)| {
        let right = if why { w.right } else { w.right_short };
        let job = if short { w.job_short_flags } else { w.job };
        let wt = if project {
            w.worktree
        } else {
            w.worktree_without_project
        };
        Cols {
            project: project.then_some(pw),
            worktree: wt.clamp(1, WT_CAP),
            job: job.clamp(1, JOB_CAP),
            cores: cores.then_some(w.cores),
            bar: bar_min,
            right: right.clamp(RIGHT_FLOOR, RIGHT_MAX),
            why,
            short_flags: short,
        }
    };
    let need = |c: &Cols| {
        w.prefix
            + c.project.map_or(0, |p| p + 1)
            + c.worktree
            + 1
            + c.job
            + 1
            + c.cores.map_or(0, |n| n + 1)
            + c.bar
            + 1
            + c.right
    };
    for st in states {
        let mut c = build(st);
        let n = need(&c);
        if n <= width {
            c.bar += width - n;
            return c;
        }
    }
    // Nothing fits at the bar's minimum: cut the names, then the bar, then
    // the text after it.
    let mut c = build(states[states.len() - 1]);
    let mut short = need(&c) - width;
    for (which, floor) in [
        (1, 10),
        (2, 12),
        (1, 6),
        (2, 4),
        (0, BAR_FLOOR),
        (3, RIGHT_FLOOR),
    ] {
        let cur = match which {
            0 => &mut c.bar,
            1 => &mut c.worktree,
            2 => &mut c.job,
            _ => &mut c.right,
        };
        let cut = short.min(cur.saturating_sub(floor));
        *cur -= cut;
        short -= cut;
    }
    c
}

/// The text of the worktree column: the branch (cut in the middle when
/// `max` is short) and `(current)` for where `top` runs; `-` for a row with
/// no worktree record or a repository with a single worktree.
fn worktree_text(w: Option<&Wt>, max: usize) -> String {
    let Some(w) = w.filter(|w| w.color != Color::Gray) else {
        return "-".into();
    };
    let branch = if w.branch.is_empty() {
        Path::new(&w.path)
            .file_name()
            .map_or(w.path.clone(), |n| n.to_string_lossy().into_owned())
    } else {
        w.branch.clone()
    };
    let suffix = if w.mine { " (current)" } else { "" };
    let room = max.saturating_sub(suffix.chars().count()).max(1);
    format!("{}{suffix}", ellipsize_middle(&branch, room))
}

/// The worktree column of a row. A `-` (lone worktree, or none recorded)
/// shows the project name instead when the project column is gone, so such
/// rows can still be told apart; the bool is true then (drawn gray).
fn worktree_cell(wt: Option<&Wt>, project: &str, project_col: bool, max: usize) -> (String, bool) {
    let text = worktree_text(wt, max);
    if text == "-" && !project_col && project != "-" {
        return (ellipsize_middle(project, max), true);
    }
    (text, false)
}

/// The job name cut so that its flags fit in `width`: (name, flags).
fn job_parts(job: &str, flags: &[&str], width: usize) -> (String, String) {
    let tail: String = flags.iter().map(|f| format!(" {f}")).collect();
    let tail_w = tail.chars().count();
    if tail_w + 4 > width {
        return (truncate(job, width), String::new());
    }
    (truncate(job, width - tail_w), tail)
}

fn right_text_run(r: &RunRow) -> String {
    match &r.progress {
        Progress::Unknown => format!("ran {}", fmt_duration(r.elapsed)),
        Progress::Known { left, over, .. } => format!(
            "{} {}",
            if *over { "over" } else { "left" },
            fmt_duration(*left)
        ),
    }
}

fn right_text_wait(w: &WaitRow) -> String {
    format!("{} ({})", starts_text(w), w.why)
}

/// `starts ~1:35?`: when a waiting entry starts, without the reason.
fn starts_text(w: &WaitRow) -> String {
    let start = match w.start_in {
        None => "?".to_string(),
        Some(s) if s <= 0.0 => "now".to_string(),
        Some(s) => format!(
            "~{}{}",
            fmt_duration(Duration::from_secs_f64(s)),
            if w.rough { "?" } else { "" }
        ),
    };
    format!("starts {start}")
}

/// A flag after the job name: written out, and abbreviated for narrow
/// terminals.
type Flag = (&'static str, &'static str);
const IDLE: Flag = ("idle", "z");
const BACKFILL: Flag = ("[backfill]", "b");
const FRONT: Flag = ("[front]", "f");

/// A row, ready to lay out.
#[derive(Debug, Clone, PartialEq)]
struct TableRow {
    num: usize,
    project: String,
    job: String,
    flags: Vec<Flag>,
    cores: String,
    spec: BarSpec,
    right: String,
    /// `right` without the reason for waiting.
    right_short: String,
    wt: Option<usize>,
    over: bool,
}

fn table_rows(v: &View) -> Vec<TableRow> {
    let run = v.running.iter().map(|r| TableRow {
        num: r.num,
        project: r.project.clone(),
        job: r.job.clone(),
        flags: [(r.idle, IDLE), (r.backfilled, BACKFILL)]
            .into_iter()
            .filter_map(|(on, f)| on.then_some(f))
            .collect(),
        cores: r.weight.to_string(),
        spec: BarSpec::of_run(r),
        right: right_text_run(r),
        right_short: right_text_run(r),
        wt: r.wt,
        over: matches!(r.progress, Progress::Known { over: true, .. }),
    });
    let wait = v.waiting.iter().map(|w| TableRow {
        num: w.num,
        project: w.project.clone(),
        job: w.job.clone(),
        flags: if w.front { vec![FRONT] } else { vec![] },
        cores: w.weight.to_string(),
        spec: BarSpec::of_wait(w),
        right: right_text_wait(w),
        right_short: starts_text(w),
        wt: w.wt,
        over: false,
    });
    run.chain(wait).collect()
}

// ------------------------------------------------------------ empty state

/// How many finished runs the empty screen lists.
const RECENT_MAX: usize = 5;

fn cores(n: u32) -> String {
    format!("{n} core{}", if n == 1 { "" } else { "s" })
}

/// The two lines shown when nothing runs or waits.
fn empty_lines(free: u32) -> (String, String) {
    (
        "Nothing running or waiting".into(),
        format!("A new command starts right away · {} free", cores(free)),
    )
}

const RECENT_TITLE: &str = "recently finished";
const RUN_HINT: &str = "Run one with: task-traffic-controller exec -- <command>";

/// The line under the table when something runs but nobody waits.
fn nobody_waiting(free: u32) -> String {
    if free > 0 {
        format!(
            "No one waiting · a command needing ≤{} starts right away",
            cores(free)
        )
    } else {
        "No one waiting · the next command waits for cores".into()
    }
}

/// `45s ago`, `2m ago`, `3h ago`, `2d ago`.
fn ago_text(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0..=59 => format!("{s}s ago"),
        60..=3599 => format!("{}m ago", s / 60),
        3600..=86399 => format!("{}h ago", s / 3600),
        _ => format!("{}d ago", s / 86400),
    }
}

/// The newest `max` finished runs, newest first. Worktrees are colored the
/// way the queue colors them: per repository, gray when only one worktree
/// of it is among these runs.
fn recent_rows(
    history: &[Record],
    now: DateTime<Utc>,
    me: Option<&str>,
    max: usize,
) -> Vec<RecentRow> {
    let finished =
        |r: &Record| r.started_at + chrono::Duration::milliseconds((r.wall_secs * 1000.0) as i64);
    let mut recs: Vec<&Record> = history.iter().collect();
    recs.sort_by_key(|r| std::cmp::Reverse(finished(r)));
    recs.truncate(max);
    let mut colors: HashMap<(&str, &str), Color> = HashMap::new();
    let mut repos: Vec<&str> = recs.iter().map(|r| r.repo.as_str()).collect();
    repos.sort_unstable();
    repos.dedup();
    for repo in repos {
        let mut paths: Vec<&str> = recs
            .iter()
            .filter(|r| r.repo == repo)
            .filter_map(|r| r.worktree.as_deref())
            .collect();
        paths.sort_unstable();
        paths.dedup();
        for (p, c) in paths.iter().zip(repo_colors(&paths)) {
            colors.insert((repo, p), c);
        }
    }
    recs.into_iter()
        .map(|r| RecentRow {
            project: if r.repo.is_empty() {
                "-".into()
            } else {
                key::project_label(&r.repo, &r.subdir)
            },
            job: job_of_argv(&r.argv),
            took: Duration::from_secs_f64(r.wall_secs.max(0.0)),
            ago: (now - finished(r)).to_std().unwrap_or_default(),
            failed: (r.exit_code != 0).then_some(r.exit_code),
            wt: r.worktree.as_deref().map(|path| Wt {
                path: path.to_string(),
                branch: r.branch.clone().unwrap_or_default(),
                mine: me == Some(path),
                color: colors
                    .get(&(r.repo.as_str(), path))
                    .copied()
                    .unwrap_or(Color::Gray),
            }),
        })
        .collect()
}

/// The text after the job on a finished run: `3:20 · 2m ago`.
fn recent_time_text(r: &RecentRow) -> String {
    format!("{} · {}", fmt_duration(r.took), ago_text(r.ago))
}

// ---------------------------------------------------------------- drawing

fn wt_color(v: &View, wt: Option<usize>) -> Color {
    wt.and_then(|i| v.worktrees.get(i))
        .map_or(Color::Gray, |w| w.color)
}

pub fn draw(f: &mut Frame, v: &View, interval: Duration, error: Option<&str>) {
    let area = f.area();
    let held_h = u16::from(!v.held.is_empty());
    let [head, budget, queue, held] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Fill(1),
        Constraint::Length(held_h),
    ])
    .areas(area);
    draw_header(f, head, v, interval, error);
    draw_budget(f, budget, v);
    draw_queue(f, queue, v);
    if held_h > 0 {
        draw_held(f, held, v);
    }
}

fn draw_header(f: &mut Frame, area: Rect, v: &View, interval: Duration, error: Option<&str>) {
    let clock = v.now.with_timezone(&Local).format("%H:%M:%S").to_string();
    let mut spans = vec![
        Span::styled(
            "task-traffic-controller",
            Style::new().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {clock}  every {}s", interval.as_secs_f64())),
    ];
    if let Some(e) = error {
        spans.push(Span::styled(
            format!("  {}", truncate(e, 60)),
            Style::new().fg(Color::Red),
        ));
    }
    let hint = "q: quit";
    f.render_widget(Paragraph::new(Line::from(spans)), area);
    f.render_widget(
        Paragraph::new(hint)
            .right_aligned()
            .style(Style::new().add_modifier(Modifier::DIM)),
        area,
    );
}

fn draw_budget(f: &mut Frame, area: Rect, v: &View) {
    let mut title = vec![Span::raw(format!(
        " cores {}/{} in use · {} free",
        v.used, v.budget, v.free
    ))];
    if v.over > 0 {
        title.push(Span::styled(
            format!(" · {} over budget", v.over),
            Style::new().fg(Color::Red),
        ));
    }
    title.push(Span::raw(" "));
    let mut block = Block::bordered().title(Line::from(title));
    if let Some(m) = &v.machine {
        block = block.title(
            Line::from(format!(
                " cpu {:.0}% · mem {} free ",
                m.cpu_busy * 100.0,
                fmt_bytes(m.available_memory)
            ))
            .right_aligned(),
        );
    }
    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(bar_line(v, inner.width)), inner);
}

/// One cell group per slot; each running entry's slots in its worktree's
/// color with its row number first, free slots dim.
fn bar_line(v: &View, width: u16) -> Line<'static> {
    let w = (width as u32 / v.budget.max(1)).max(1) as usize;
    let mut spans = Vec::new();
    for s in &v.slots {
        if s.slots == 0 {
            continue;
        }
        let c = wt_color(v, s.wt);
        let cells = s.slots as usize * w;
        let num: String = format!("#{}", s.num).chars().take(cells).collect();
        let n = num.chars().count();
        spans.push(Span::styled(
            num,
            Style::new()
                .fg(Color::Black)
                .bg(c)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled("█".repeat(cells - n), Style::new().fg(c)));
    }
    spans.push(Span::styled(
        "░".repeat(v.free as usize * w),
        Style::new().fg(Color::DarkGray),
    ));
    Line::from(spans)
}

/// Pad or cut `s` to exactly `w` characters.
fn fit(s: &str, w: usize) -> String {
    let t = truncate(s, w);
    let n = t.chars().count();
    format!("{t}{}", " ".repeat(w - n))
}

/// The bar's characters as spans: runs of one character class share a style.
fn bar_spans(bar: &str, base: Style) -> Vec<Span<'static>> {
    let style = |c: char| match c {
        '▓' => base.fg(Color::Red),
        '▒' | '┄' | '→' | '←' => base.add_modifier(Modifier::DIM),
        '│' => base.fg(Color::DarkGray),
        _ => base,
    };
    let mut out: Vec<Span<'static>> = Vec::new();
    let mut run = String::new();
    let mut cur: Option<Style> = None;
    for c in bar.chars() {
        let s = style(c);
        if cur.is_some_and(|x| x != s) {
            out.push(Span::styled(std::mem::take(&mut run), cur.unwrap()));
        }
        cur = Some(s);
        run.push(c);
    }
    if let Some(s) = cur {
        out.push(Span::styled(run, s));
    }
    out
}

/// What the rows of the table would like in the way of columns.
fn table_wants(v: &View, rows: &[TableRow], prefix: usize) -> Wants {
    let widest = |len: &dyn Fn(&TableRow) -> usize| rows.iter().map(len).max().unwrap_or(0);
    let wt_text = |r: &TableRow, project_col: bool| {
        let wt = r.wt.and_then(|i| v.worktrees.get(i));
        worktree_cell(wt, &r.project, project_col, usize::MAX)
            .0
            .chars()
            .count()
    };
    let job = |r: &TableRow, short: bool| {
        r.job.chars().count()
            + r.flags
                .iter()
                .map(|f| 1 + if short { f.1 } else { f.0 }.len())
                .sum::<usize>()
    };
    Wants {
        prefix,
        project: widest(&|r| r.project.chars().count()),
        worktree: widest(&|r| wt_text(r, true)).max("worktree".len()),
        worktree_without_project: widest(&|r| wt_text(r, false)).max("worktree".len()),
        job: widest(&|r| job(r, false)),
        job_short_flags: widest(&|r| job(r, true)),
        cores: widest(&|r| r.cores.chars().count()).max(CORES_HEADER),
        right: widest(&|r| r.right.chars().count()),
        right_short: widest(&|r| r.right_short.chars().count()),
    }
}

fn draw_queue(f: &mut Frame, area: Rect, v: &View) {
    let block = Block::bordered().title(" queue ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let rows = table_rows(v);
    if rows.is_empty() {
        draw_empty(f, inner, v);
        return;
    }
    let num_w = format!("#{}", rows.len()).len();
    let prefix = 1 + num_w + 1;
    let wt_of = |r: &TableRow| r.wt.and_then(|i| v.worktrees.get(i));
    let cols = layout(inner.width as usize, table_wants(v, &rows, prefix));
    let specs: Vec<BarSpec> = rows.iter().map(|r| r.spec).collect();
    let scale = compute_scale(&specs, cols.bar);

    let dim = Style::new().add_modifier(Modifier::DIM);
    let mut lines = Vec::new();
    // Header: column names, then the ruler.
    let mut head = vec![Span::raw(" ".repeat(prefix))];
    if let Some(pw) = cols.project {
        head.push(Span::styled(fit("project", pw + 1), dim));
    }
    head.push(Span::styled(fit("worktree", cols.worktree + 1), dim));
    head.push(Span::styled(fit("job", cols.job + 1), dim));
    if let Some(cw) = cols.cores {
        head.push(Span::styled(fit("cores", cw + 1), dim));
    }
    head.push(Span::styled(axis_line(scale), dim));
    lines.push(Line::from(head));

    for r in &rows {
        let wt = wt_of(r);
        let mine = wt.is_some_and(|w| w.mine);
        let base = if mine {
            Style::new().add_modifier(Modifier::BOLD)
        } else {
            Style::new()
        };
        let mut spans = vec![
            Span::styled("▌", Style::new().fg(wt_color(v, r.wt))),
            Span::styled(format!("{:>num_w$} ", format!("#{}", r.num)), base),
        ];
        if let Some(pw) = cols.project {
            spans.push(Span::styled(fit(&r.project, pw + 1), base));
        }
        let (cell, is_project) =
            worktree_cell(wt, &r.project, cols.project.is_some(), cols.worktree);
        spans.push(Span::styled(
            fit(&cell, cols.worktree + 1),
            if is_project {
                dim
            } else {
                base.fg(wt_color(v, r.wt))
            },
        ));
        let flag_text = |f: &Flag| if cols.short_flags { f.1 } else { f.0 };
        let flag_strs: Vec<&str> = r.flags.iter().map(flag_text).collect();
        let (name, flags) = job_parts(&r.job, &flag_strs, cols.job);
        let used = name.chars().count() + flags.chars().count();
        spans.push(Span::styled(name, base));
        for flag in &r.flags {
            if flags.contains(flag_text(flag)) {
                spans.push(Span::styled(
                    format!(" {}", flag_text(flag)),
                    if *flag == IDLE {
                        Style::new().fg(Color::Yellow)
                    } else {
                        dim
                    },
                ));
            }
        }
        spans.push(Span::raw(" ".repeat((cols.job + 1).saturating_sub(used))));
        if let Some(cw) = cols.cores {
            spans.push(Span::styled(fit(&r.cores, cw + 1), base));
        }
        spans.extend(bar_spans(&bar_string(r.spec, scale), base));
        spans.push(Span::styled(
            format!(
                " {}",
                truncate(if cols.why { &r.right } else { &r.right_short }, cols.right)
            ),
            if r.over {
                Style::new().fg(Color::Red)
            } else {
                base
            },
        ));
        lines.push(Line::from(spans));
    }
    if v.waiting.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::styled(format!(" {}", nobody_waiting(v.free)), dim));
    }
    f.render_widget(Paragraph::new(lines), inner);
}

/// The inside of the queue box when nothing runs or waits: what that
/// means, and what finished lately (or how to start something).
fn draw_empty(f: &mut Frame, area: Rect, v: &View) {
    let dim = Style::new().add_modifier(Modifier::DIM);
    let (first, second) = empty_lines(v.free);
    let mut lines = vec![
        Line::raw(""),
        Line::raw(format!(" {first}")),
        Line::styled(format!(" {second}"), dim),
        Line::raw(""),
    ];
    if v.recent.is_empty() {
        lines.push(Line::styled(format!(" {RUN_HINT}"), dim));
    } else {
        lines.push(Line::styled(format!(" {RECENT_TITLE}"), dim));
        let wt_text = |r: &RecentRow, max| worktree_text(r.wt.as_ref(), max);
        let widest =
            |len: &dyn Fn(&RecentRow) -> usize| v.recent.iter().map(len).max().unwrap_or(0);
        let cols = layout(
            area.width as usize,
            Wants {
                prefix: 2,
                project: widest(&|r| r.project.chars().count()),
                worktree: widest(&|r| wt_text(r, usize::MAX).chars().count()),
                worktree_without_project: widest(&|r| wt_text(r, usize::MAX).chars().count()),
                job: widest(&|r| r.job.chars().count()),
                job_short_flags: widest(&|r| r.job.chars().count()),
                cores: usize::MAX / 2,
                right: widest(&|r| recent_time_text(r).chars().count()),
                right_short: widest(&|r| recent_time_text(r).chars().count()),
            },
        );
        for r in &v.recent {
            let color = r.wt.as_ref().map_or(Color::Gray, |w| w.color);
            let mut spans = vec![Span::styled("▌ ", Style::new().fg(color))];
            if let Some(pw) = cols.project {
                spans.push(Span::raw(fit(&r.project, pw + 1)));
            }
            spans.push(Span::styled(
                fit(&wt_text(r, cols.worktree), cols.worktree + 1),
                Style::new().fg(color),
            ));
            spans.push(Span::raw(fit(&r.job, cols.job + 1)));
            spans.push(Span::styled(recent_time_text(r), dim));
            if let Some(code) = r.failed {
                spans.push(Span::styled(
                    format!("  exit {code}"),
                    Style::new().fg(Color::Red),
                ));
            }
            lines.push(Line::from(spans));
        }
    }
    f.render_widget(Paragraph::new(lines), area);
}

/// One line: `held, owner gone (2): m1 owner gone -> resolve m1; m2 ...`.
fn held_text(held: &[HeldRow]) -> String {
    let items: Vec<String> = held
        .iter()
        .map(|h| format!("{} {} -> {}", h.id, h.reason, h.hint))
        .collect();
    format!("held, owner gone ({}): {}", held.len(), items.join("; "))
}

fn draw_held(f: &mut Frame, area: Rect, v: &View) {
    f.render_widget(
        Paragraph::new(truncate(&held_text(&v.held), area.width as usize))
            .style(Style::new().fg(Color::Yellow)),
        area,
    );
}

// ------------------------------------------------------------- event loop

/// Puts the terminal back on every exit path, including a panic unwinding
/// through [`run`] (`ratatui::init` also installs a panic hook that does it
/// before the message prints).
struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

fn is_quit(key: &event::KeyEvent) -> bool {
    key.kind == KeyEventKind::Press
        && match key.code {
            KeyCode::Char('q') | KeyCode::Esc => true,
            KeyCode::Char('c') => key.modifiers.contains(KeyModifiers::CONTROL),
            _ => false,
        }
}

pub fn run(ctx: &Context, cfg: &Config, interval: f64) -> Result<i32> {
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return Err(Error::Spawn(
            "top needs a terminal; use `status` for a one-shot, pipeable view".into(),
        ));
    }
    let interval = Duration::from_secs_f64(interval.clamp(0.2, 3600.0));
    // Build the first view before touching the terminal, so a ledger that
    // cannot be read is reported on the normal screen.
    let me = std::env::current_dir()
        .ok()
        .and_then(|d| gitctx::worktree(&d))
        .map(|w| w.path);
    let mut view = View::build(ctx, cfg, me.as_deref())?;
    let mut error: Option<String> = None;
    let mut terminal = ratatui::init();
    let _restore = Restore;
    let mut next = Instant::now() + interval;
    loop {
        terminal.draw(|f| draw(f, &view, interval, error.as_deref()))?;
        let wait = next.saturating_duration_since(Instant::now());
        if event::poll(wait)? {
            match event::read()? {
                Event::Key(k) if is_quit(&k) => return Ok(0),
                // A resize (or any other event) redraws with the same data.
                _ => continue,
            }
        }
        match View::build(ctx, cfg, me.as_deref()) {
            Ok(v) => {
                view = v;
                error = None;
            }
            Err(e) => error = Some(e.to_string()),
        }
        next = Instant::now() + interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as Cd;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    const ME: &str = "/src/remote-task-runner";
    const FIX: &str = "/src/remote-task-runner/.worktrees/fix-queue-order";
    const FIX_BRANCH: &str = "fix/queue-order-after-reap-of-the-waiting-head";
    const LCI: &str = "/src/local-ci";

    fn limits(budget: u32) -> Limits {
        let mut l = Config::parse("").unwrap().limits();
        l.budget = budget;
        l
    }

    #[allow(clippy::too_many_arguments)]
    fn entry(
        id: &str,
        state: State,
        weight: u32,
        seq: u64,
        repo: &str,
        subdir: &str,
        argv: &str,
        now: DateTime<Utc>,
        age_secs: i64,
    ) -> Entry {
        Entry {
            id: id.into(),
            pid: 1000 + seq as u32,
            state,
            front: false,
            weight,
            predicted_rss: 0,
            expected_secs: None,
            repo: repo.into(),
            subdir: subdir.into(),
            argv: argv.into(),
            key: format!("k{seq}"),
            seq,
            enqueued_at: now - Cd::seconds(age_secs),
            started_at: (state == State::Running).then(|| now - Cd::seconds(age_secs)),
            using: None,
            backfilled: false,
            label: None,
            worktree: None,
            branch: None,
            parent: None,
            nested_cpu_secs: 0.0,
        }
    }

    fn in_wt(mut e: Entry, path: &str, branch: &str) -> Entry {
        e.worktree = Some(path.into());
        e.branch = Some(branch.into());
        e
    }

    fn ledger(entries: Vec<Entry>) -> Ledger {
        Ledger {
            entries,
            ..Ledger::default()
        }
    }

    /// A repository with two worktrees (main, where `top` runs, and a fix
    /// branch) and `local-ci run` three times from the lone worktree of
    /// another one: two running, one waiting.
    fn sample(now: DateTime<Utc>) -> Ledger {
        let rtr = "/src/remote-task-runner";
        let mut a = entry(
            "a",
            State::Running,
            4,
            1,
            rtr,
            "",
            "cargo test --workspace",
            now,
            95,
        );
        a.expected_secs = Some(190.0);
        a.using = Some(3.4);
        let a = in_wt(a, FIX, FIX_BRANCH);
        let mut b = entry(
            "b",
            State::Running,
            6,
            2,
            "/src/local-ci",
            "",
            "local-ci run",
            now,
            400,
        );
        b.expected_secs = Some(600.0);
        b.using = Some(0.1);
        b.backfilled = true;
        let b = in_wt(b, LCI, "main");
        let c = entry(
            "c",
            State::Running,
            2,
            3,
            "/src/local-ci",
            "",
            "local-ci run",
            now,
            12,
        );
        let c = in_wt(c, LCI, "main");
        let mut d = entry(
            "d",
            State::Waiting,
            6,
            4,
            "/src/local-ci",
            "",
            "local-ci run",
            now,
            45,
        );
        d.expected_secs = Some(240.0);
        let d = in_wt(d, LCI, "main");
        let e = entry(
            "e",
            State::Waiting,
            2,
            5,
            rtr,
            "",
            "cargo clippy --all-targets",
            now,
            20,
        );
        let e = in_wt(e, ME, "main");
        ledger(vec![a, b, c, d, e])
    }

    fn live() -> LiveSnapshot {
        LiveSnapshot {
            available_memory: 9 << 30,
            cpu_busy: 0.5,
            load_avg: 1.0,
        }
    }

    fn sample_view(now: DateTime<Utc>) -> View {
        View::from_parts(
            &sample(now),
            vec![],
            limits(12),
            Some(live()),
            now,
            Some(ME),
            &[],
        )
    }

    #[test]
    fn progress_is_capped_while_running_and_counts_the_overrun() {
        let s = Duration::from_secs;
        assert_eq!(
            progress(s(50), Some(200.0)),
            Progress::Known {
                pct: 25,
                left: s(150),
                over: false
            }
        );
        assert_eq!(
            progress(s(199), Some(200.0)),
            Progress::Known {
                pct: 99,
                left: s(1),
                over: false
            }
        );
        assert_eq!(
            progress(s(200), Some(200.0)),
            Progress::Known {
                pct: 99,
                left: s(0),
                over: false
            }
        );
        assert_eq!(
            progress(s(260), Some(200.0)),
            Progress::Known {
                pct: 99,
                left: s(60),
                over: true
            }
        );
        assert_eq!(progress(s(5), None), Progress::Unknown);
        assert_eq!(progress(s(5), Some(0.0)), Progress::Unknown);
    }

    #[test]
    fn jobs_are_named_by_label_then_script_then_argv() {
        let now = Utc::now();
        let mut e = entry("a", State::Running, 1, 1, "/r/web", "api", "x", now, 1);
        e.argv = "bash -e /ci/steps/step-8.sh --fast".into();
        assert_eq!(job_name(&e), "step-8.sh");
        e.argv = "/bin/zsh ./run.sh".into();
        assert_eq!(job_name(&e), "run.sh");
        e.argv = "bash -c make test".into();
        assert_eq!(job_name(&e), "bash -c make test");
        e.argv = "cargo test --workspace".into();
        assert_eq!(job_name(&e), "cargo test --workspace");
        e.label = Some("unit tests".into());
        assert_eq!(job_name(&e), "unit tests");
        assert_eq!(project(&e), "web/api");
        e.subdir.clear();
        assert_eq!(project(&e), "web");
        e.repo.clear();
        assert_eq!(project(&e), "-");
    }

    #[test]
    fn rows_are_numbered_soonest_finish_first_then_the_queue() {
        let now = Utc::now();
        let v = sample_view(now);
        assert_eq!(v.budget, 12);
        assert_eq!(v.used, 12);
        // Left: cargo test 95 s, the first local-ci 200 s, the second unknown.
        let nums: Vec<_> = v.running.iter().map(|r| (r.num, r.job.as_str())).collect();
        assert_eq!(
            nums,
            [
                (1, "cargo test --workspace"),
                (2, "local-ci run"),
                (3, "local-ci run")
            ]
        );
        assert_eq!(v.running[0].project, "remote-task-runner");
        assert_eq!(v.running[1].project, "local-ci");
        assert_eq!(v.waiting.iter().map(|w| w.num).collect::<Vec<_>>(), [4, 5]);
        assert_eq!(v.waiting[0].why, "needs 6, 0 free");
        assert_eq!(v.waiting[1].job, "cargo clippy --all-targets");
        assert_eq!(v.waiting[1].why, "behind 1");
        assert_eq!(v.waiting[0].expected_secs, Some(240.0));
        assert_eq!(v.waiting[1].expected_secs, None);
        // Slots follow the table order and still add up.
        assert_eq!(
            v.slots.iter().map(|s| (s.num, s.slots)).collect::<Vec<_>>(),
            [(1, 4), (2, 6), (3, 2)]
        );
        assert_eq!(v.free, 0);
        assert_eq!(
            v.slots.iter().map(|s| s.slots).sum::<u32>() + v.free,
            v.budget
        );
        assert_eq!(v.over, 0);
        assert_eq!(
            v.running[1].progress,
            Progress::Known {
                pct: 66,
                left: Duration::from_secs(200),
                over: false
            }
        );
        assert!(v.running[1].idle && v.running[1].backfilled);
        assert_eq!(v.running[2].progress, Progress::Unknown);
        assert!(v.machine.is_some());
        assert!(v.recent.is_empty());
    }
    #[test]
    fn equal_finish_times_keep_the_older_run_first() {
        let now = Utc::now();
        let mut x = entry("x", State::Running, 1, 1, "/r/a", "", "x", now, 30);
        let mut y = entry("y", State::Running, 1, 2, "/r/b", "", "y", now, 50);
        x.expected_secs = Some(100.0);
        y.expected_secs = Some(120.0); // 70 left for both
        let v = View::from_parts(&ledger(vec![x, y]), vec![], limits(4), None, now, None, &[]);
        assert_eq!(
            v.running.iter().map(|r| r.job.as_str()).collect::<Vec<_>>(),
            ["y", "x"]
        );
    }

    #[test]
    fn slots_never_exceed_the_budget() {
        let now = Utc::now();
        let mut x = entry("x", State::Running, 3, 1, "/r/a", "", "a", now, 30);
        let mut y = entry("y", State::Running, 3, 2, "/r/b", "", "b", now, 20);
        y.using = Some(7.0); // measured above its weight
        x.using = None;
        let v = View::from_parts(&ledger(vec![x, y]), vec![], limits(6), None, now, None, &[]);
        let total: u32 = v.slots.iter().map(|s| s.slots).sum();
        assert_eq!(total + v.free, 6);
        assert_eq!(v.free, 0);
        assert_eq!(v.over, 4); // charged 3 + 7 = 10 against 6
        assert!(v.machine.is_none());
    }

    #[test]
    fn worktrees_come_in_table_order_with_mine_and_color() {
        let v = sample_view(Utc::now());
        let w: Vec<_> = v
            .worktrees
            .iter()
            .map(|w| (w.path.as_str(), w.mine))
            .collect();
        assert_eq!(w, [(FIX, false), (LCI, false), (ME, true)]);
        assert_eq!(v.worktrees[0].branch, FIX_BRANCH);
        // remote-task-runner has two worktrees queued: colored, and
        // different from each other. local-ci has one: gray.
        assert_ne!(v.worktrees[0].color, Color::Gray);
        assert_ne!(v.worktrees[2].color, Color::Gray);
        assert_ne!(v.worktrees[0].color, v.worktrees[2].color);
        assert_eq!(v.worktrees[1].color, Color::Gray);
        // Rows point at their worktree; the cores slots too.
        assert_eq!(v.running[0].wt, Some(0));
        assert_eq!(v.running[1].wt, Some(1));
        assert_eq!(v.waiting[1].wt, Some(2));
        assert_eq!(v.slots[0].wt, Some(0));
    }
    #[test]
    fn an_entry_without_a_worktree_has_none_and_nobody_is_mine() {
        let now = Utc::now();
        let a = entry("a", State::Running, 1, 1, "/r/a", "", "a", now, 5);
        let v = View::from_parts(
            &ledger(vec![a]),
            vec![],
            limits(4),
            None,
            now,
            Some(ME),
            &[],
        );
        assert!(v.worktrees.is_empty());
        assert_eq!(v.running[0].wt, None);
    }

    #[test]
    fn the_newest_entry_names_the_branch() {
        let now = Utc::now();
        let old = in_wt(
            entry("a", State::Running, 1, 1, "/r", "", "a", now, 50),
            "/r/wt",
            "old-branch",
        );
        let new = in_wt(
            entry("b", State::Waiting, 1, 2, "/r", "", "b", now, 5),
            "/r/wt",
            "new-branch",
        );
        let v = View::from_parts(
            &ledger(vec![old, new]),
            vec![],
            limits(4),
            None,
            now,
            None,
            &[],
        );
        assert_eq!(v.worktrees.len(), 1);
        assert_eq!(v.worktrees[0].branch, "new-branch");
    }

    /// Two paths whose hashes pick the same palette slot.
    fn colliding_paths() -> (String, String) {
        let n = WT_COLORS.len() as u64;
        let first = "/w/wt-0".to_string();
        let want = fnv1a(&first) % n;
        let other = (1..1000)
            .map(|i| format!("/w/wt-{i}"))
            .find(|p| fnv1a(p) % n == want)
            .unwrap();
        (first, other)
    }

    #[test]
    fn worktree_colors_are_stable_and_never_the_warning_colors() {
        assert_eq!(fnv1a(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(repo_colors(&["/w/a"]), [Color::Gray]);
        assert!(repo_colors(&[]).is_empty());
        let paths: Vec<String> = (0..6).map(|i| format!("/w/wt-{i}")).collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let c = repo_colors(&refs);
        assert_eq!(c, repo_colors(&refs));
        assert!(c.iter().all(|c| WT_COLORS.contains(c)));
        for bad in [Color::Red, Color::Yellow, Color::Gray, Color::DarkGray] {
            assert!(!WT_COLORS.contains(&bad));
        }
    }

    #[test]
    fn worktrees_of_one_repository_never_share_a_color_until_the_palette_runs_out() {
        let (a, b) = colliding_paths();
        let n = WT_COLORS.len() as u64;
        assert_eq!(fnv1a(&a) % n, fnv1a(&b) % n);
        let c = repo_colors(&[&a, &b]);
        assert_ne!(c[0], c[1]);
        // Order of the input does not change who gets what.
        let r = repo_colors(&[&b, &a]);
        assert_eq!((r[0], r[1]), (c[1], c[0]));
        // The same for a bigger set, in any rotation.
        let paths: Vec<String> = (0..WT_COLORS.len()).map(|i| format!("/w/wt-{i}")).collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let base = repo_colors(&refs);
        let distinct: std::collections::HashSet<_> =
            base.iter().map(|c| format!("{c:?}")).collect();
        assert_eq!(distinct.len(), WT_COLORS.len());
        for k in 1..refs.len() {
            let mut rot = refs.clone();
            rot.rotate_left(k);
            let got = repo_colors(&rot);
            for (p, col) in rot.iter().zip(got) {
                let at = refs.iter().position(|x| x == p).unwrap();
                assert_eq!(col, base[at]);
            }
        }
        // More worktrees than colors: repeats are allowed, nothing breaks.
        let many: Vec<String> = (0..20).map(|i| format!("/m/wt-{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert_eq!(repo_colors(&many).len(), 20);
    }

    #[test]
    fn colliding_worktrees_in_the_view_get_different_colors() {
        let (a, b) = colliding_paths();
        let now = Utc::now();
        let ea = in_wt(
            entry("a", State::Running, 1, 1, "/r", "", "a", now, 9),
            &a,
            "x",
        );
        let eb = in_wt(
            entry("b", State::Running, 1, 2, "/r", "", "b", now, 8),
            &b,
            "y",
        );
        let v = View::from_parts(
            &ledger(vec![ea, eb]),
            vec![],
            limits(4),
            None,
            now,
            None,
            &[],
        );
        assert_ne!(v.worktrees[0].color, v.worktrees[1].color);
        assert!(v.worktrees.iter().all(|w| w.color != Color::Gray));
    }

    #[test]
    fn long_names_lose_their_middle() {
        assert_eq!(ellipsize_middle("main", 10), "main");
        assert_eq!(ellipsize_middle("abcdefghij", 10), "abcdefghij");
        assert_eq!(ellipsize_middle("abcdefghijk", 10), "abcde…hijk");
        assert_eq!(
            ellipsize_middle("fix/login-redirect-loop-on-expired-session", 20),
            "fix/login-…d-session"
        );
        assert_eq!(ellipsize_middle("abcdef", 2), "a…");
        assert_eq!(ellipsize_middle("abcdef", 1), "…");
        assert_eq!(ellipsize_middle("abcdef", 0), "");
        assert_eq!(ellipsize_middle("日本語のブランチ名", 5), "日本…チ名");
    }

    fn scale(per: f64, now: usize, width: usize) -> Scale {
        Scale {
            secs_per_col: per,
            now_col: now,
            width,
        }
    }

    #[test]
    fn the_scale_is_the_finest_one_that_fits_the_row_ending_last() {
        let run = BarSpec::Running {
            elapsed: 60.0,
            expected: Some(180.0),
        };
        // 12 columns behind, 24 ahead at 5 s per column; 3 s would not fit.
        assert_eq!(compute_scale(&[run], 40), scale(5.0, 12, 40));
        // A waiting row that starts in 2 m and runs 5 m pushes it out.
        let wait = BarSpec::Waiting {
            start_in: Some(120.0),
            expected: Some(300.0),
            rough: false,
        };
        let s = compute_scale(&[run, wait], 40);
        assert_eq!(s.secs_per_col, 15.0);
        assert!(s.now_col + 1 + cols_for(420.0, 15.0) <= 40);
        // Nothing to show: the finest scale, now at the left edge.
        assert_eq!(compute_scale(&[], 40), scale(1.0, 0, 40));
        // An old run takes at most a third of the width.
        let old = BarSpec::Running {
            elapsed: 36000.0,
            expected: Some(40000.0),
        };
        assert_eq!(compute_scale(&[old], 30).now_col, 10);
        // Even the coarsest scale cannot hold it: cut, not a panic.
        let huge = BarSpec::Waiting {
            start_in: Some(1e9),
            expected: Some(1.0),
            rough: false,
        };
        assert_eq!(compute_scale(&[huge], 20).secs_per_col, 86400.0);
    }

    #[test]
    fn a_running_bar_has_what_ran_left_of_now_and_the_rest_right() {
        let sc = scale(5.0, 12, 40);
        let bar = |elapsed, expected| bar_string(BarSpec::Running { elapsed, expected }, sc);
        assert_eq!(
            bar(60.0, Some(180.0)),
            format!("{}│{}{}", "█".repeat(12), "▒".repeat(24), " ".repeat(3))
        );
        // Past its expected time: the overrun is its own mark, nothing ahead.
        assert_eq!(
            bar(100.0, Some(60.0)),
            format!("←{}{}│{}", "█".repeat(3), "▓".repeat(8), " ".repeat(27))
        );
        // Not long enough to reach the left edge.
        assert_eq!(
            bar(30.0, Some(60.0)),
            format!(
                "{}{}│{}{}",
                " ".repeat(6),
                "█".repeat(6),
                "▒".repeat(6),
                " ".repeat(21)
            )
        );
        // No learned time: just an arrow after now.
        assert_eq!(
            bar(30.0, None),
            format!("{}{}│→{}", " ".repeat(6), "█".repeat(6), " ".repeat(26))
        );
        // Ends exactly on its expected time: nothing ahead.
        assert_eq!(
            bar(60.0, Some(60.0)),
            format!("{}│{}", "█".repeat(12), " ".repeat(27))
        );
        // Too long for the right edge: cut with an arrow.
        let s = bar(60.0, Some(2000.0));
        assert_eq!(s.chars().count(), 40);
        assert!(s.ends_with('→'));
    }

    #[test]
    fn a_waiting_bar_starts_where_it_is_expected_to_and_is_as_long_as_it_runs() {
        let sc = scale(5.0, 12, 40);
        let wait = |start_in, expected, rough| {
            bar_string(
                BarSpec::Waiting {
                    start_in,
                    expected,
                    rough,
                },
                sc,
            )
        };
        let pad = |n: usize| " ".repeat(n);
        // Starts in 50 s (10 columns after the first one past now).
        assert_eq!(
            wait(Some(50.0), Some(60.0), false),
            format!("{}│{}{}{}", pad(12), pad(10), "▒".repeat(12), pad(5))
        );
        // A rough estimate is dotted.
        assert_eq!(
            wait(Some(50.0), Some(60.0), true),
            format!("{}│{}{}{}", pad(12), pad(10), "┄".repeat(12), pad(5))
        );
        // Unknown duration: only an arrow at the start.
        assert_eq!(
            wait(Some(50.0), None, false),
            format!("{}│{}→{}", pad(12), pad(10), pad(16))
        );
        // Unknown start: begins at now.
        assert_eq!(
            wait(None, Some(10.0), false),
            format!("{}│▒▒{}", pad(12), pad(25))
        );
        // Starts after the right edge: just the arrow at the edge.
        let far = wait(Some(5000.0), Some(10.0), false);
        assert_eq!(far, format!("{}│{}→", pad(12), pad(26)));
        // Starts in view but runs past the edge.
        let cut = wait(Some(50.0), Some(600.0), false);
        assert!(cut.ends_with('→') && cut.contains('▒'));
        assert_eq!(cut.chars().count(), 40);
        // A zero-width bar draws nothing.
        assert_eq!(
            bar_string(
                BarSpec::Waiting {
                    start_in: None,
                    expected: None,
                    rough: false
                },
                scale(1.0, 0, 0)
            ),
            ""
        );
    }

    #[test]
    fn the_axis_labels_the_future_every_so_many_columns() {
        assert_eq!(
            axis_line(scale(5.0, 12, 40)),
            format!(
                "{}now{}+1m{}+2m",
                " ".repeat(12),
                " ".repeat(10),
                " ".repeat(9)
            )
        );
        // No room for "now" at the right edge: no label, no panic.
        assert_eq!(axis_line(scale(1.0, 39, 40)).chars().count(), 40);
        assert_eq!(axis_line(scale(86400.0, 0, 3)).chars().count(), 3);
        assert_eq!(fmt_tick(45.0), "+45s");
        assert_eq!(fmt_tick(120.0), "+2m");
        assert_eq!(fmt_tick(90.0), "+1m30s");
        assert_eq!(fmt_tick(3600.0), "+1h");
        assert_eq!(fmt_tick(5400.0), "+1h30m");
    }

    fn wants() -> Wants {
        Wants {
            prefix: 4,
            project: 18,
            worktree: 32,
            worktree_without_project: 32,
            job: 28,
            job_short_flags: 16,
            cores: 5,
            right: 31,
            right_short: 13,
        }
    }

    #[test]
    fn narrow_terminals_keep_the_bar_and_give_up_other_things_in_order() {
        let cols = |project, cores, job, bar, right, why, short_flags| Cols {
            project,
            worktree: 32,
            job,
            cores,
            bar,
            right,
            why,
            short_flags,
        };
        // Room for everything; the rest goes to the bar.
        assert_eq!(
            layout(150, wants()),
            cols(Some(18), Some(5), 28, 27, 31, true, false)
        );
        // 1. The reason for waiting goes.
        assert_eq!(
            layout(130, wants()),
            cols(Some(18), Some(5), 28, 25, 13, false, false)
        );
        // 2. idle and [backfill] shrink to z and b.
        assert_eq!(
            layout(113, wants()),
            cols(Some(18), Some(5), 16, 20, 13, false, true)
        );
        // 3. The project column goes, 4. then cores.
        assert_eq!(
            layout(100, wants()),
            cols(None, Some(5), 16, 26, 13, false, true)
        );
        assert_eq!(
            layout(90, wants()),
            cols(None, None, 16, 22, 13, false, true)
        );
        // 5. Only then the names are cut; the bar keeps its 20.
        let c = layout(85, wants());
        assert_eq!((c.worktree, c.job, c.bar), (29, 16, 20));
        // Under 80 columns the bar may be narrower.
        let c = layout(60, wants());
        assert_eq!((c.worktree, c.job, c.bar, c.right), (12, 16, 12, 13));
        // Whatever the width, nothing exceeds it, the bar of a wide
        // terminal is at least 20 and the time columns stay.
        for w in 60..220 {
            let c = layout(w, wants());
            let total = 4
                + c.project.map_or(0, |p| p + 1)
                + c.worktree
                + 1
                + c.job
                + 1
                + c.cores.map_or(0, |n| n + 1)
                + c.bar
                + 1
                + c.right;
            assert!(total <= w, "{w}: {c:?} = {total}");
            assert!(c.bar >= BAR_FLOOR && c.right >= RIGHT_FLOOR && c.job >= 4 && c.worktree >= 6);
            if w >= 80 {
                assert!(c.bar >= 20, "{w}: {c:?}");
            }
        }
        let _ = layout(5, wants());
        let _ = layout(
            80,
            Wants {
                cores: usize::MAX / 2,
                ..wants()
            },
        );
    }

    #[test]
    fn the_worktree_column_shows_the_branch_current_and_dashes() {
        let v = sample_view(Utc::now());
        let fix = &v.worktrees[0];
        let me = &v.worktrees[2];
        assert_eq!(worktree_text(Some(me), 40), "main (current)");
        assert_eq!(worktree_text(Some(fix), 100), FIX_BRANCH);
        // Cut in the middle; "(current)" always stays whole.
        assert_eq!(worktree_text(Some(fix), 20), "fix/queue-…ting-head");
        assert_eq!(worktree_text(Some(me), 12), "m… (current)");
        // A lone worktree (gray) and a row with no record are `-`.
        assert_eq!(worktree_text(Some(&v.worktrees[1]), 40), "-");
        assert_eq!(worktree_text(None, 40), "-");
        // No branch recorded: the directory name.
        let mut nameless = fix.clone();
        nameless.branch.clear();
        assert_eq!(worktree_text(Some(&nameless), 40), "fix-queue-order");
    }
    #[test]
    fn a_job_name_gives_up_characters_to_its_flags() {
        assert_eq!(
            job_parts("cargo test", &["idle", "[backfill]"], 30),
            ("cargo test".to_string(), " idle [backfill]".to_string())
        );
        assert_eq!(
            job_parts("0123456789abcdefghij", &["idle"], 12),
            ("012345…".to_string(), " idle".to_string())
        );
        // No room for the flags: the name wins.
        assert_eq!(
            job_parts("0123456789", &["idle", "[backfill]"], 12),
            ("0123456789".to_string(), String::new())
        );
        assert_eq!(job_parts("x", &[], 5), ("x".to_string(), String::new()));
    }

    #[test]
    fn the_text_after_the_bar_says_time_left_or_when_it_starts_and_why() {
        let v = sample_view(Utc::now());
        assert_eq!(right_text_run(&v.running[0]), "left 1:35");
        assert_eq!(right_text_run(&v.running[2]), "ran 0:12");
        let w = &v.waiting[0];
        let t = right_text_wait(w);
        assert!(
            t.starts_with("starts ~") && t.ends_with("(needs 6, 0 free)"),
            "{t}"
        );
        let mut w = w.clone();
        w.start_in = None;
        assert_eq!(right_text_wait(&w), "starts ? (needs 6, 0 free)");
        w.start_in = Some(0.0);
        assert_eq!(right_text_wait(&w), "starts now (needs 6, 0 free)");
        w.start_in = Some(130.0);
        w.rough = true;
        assert_eq!(right_text_wait(&w), "starts ~2:10? (needs 6, 0 free)");
        let mut over = v.running[0].clone();
        over.progress = Progress::Known {
            pct: 99,
            left: Duration::from_secs(60),
            over: true,
        };
        assert_eq!(right_text_run(&over), "over 1:00");
    }

    #[test]
    fn table_rows_carry_the_flags_and_the_overrun() {
        let v = sample_view(Utc::now());
        let rows = table_rows(&v);
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[1].flags, [IDLE, BACKFILL]);
        assert!(rows[0].flags.is_empty());
        assert_eq!(rows[3].cores, "6");
        assert_eq!(rows[0].cores, "4");
        assert!(rows.iter().all(|r| !r.over));
        let mut v = v;
        v.waiting[0].front = true;
        assert_eq!(table_rows(&v)[3].flags, [FRONT]);
        v.running[0].progress = Progress::Known {
            pct: 99,
            left: Duration::from_secs(1),
            over: true,
        };
        assert!(table_rows(&v)[0].over);
    }

    fn render(v: &View, w: u16, h: u16) -> Terminal<TestBackend> {
        let mut t = Terminal::new(TestBackend::new(w, h)).unwrap();
        t.draw(|f| draw(f, v, Duration::from_secs(1), None))
            .unwrap();
        t
    }

    fn frame(v: &View, w: u16, h: u16) -> String {
        let t = render(v, w, h);
        let buf = t.backend().buffer().clone();
        (0..h)
            .map(|y| {
                (0..w)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn a_frame_is_one_table_on_a_shared_axis() {
        let now = Utc::now();
        let mut v = sample_view(now);
        v.held.push(HeldRow {
            id: "m1".into(),
            reason: "owner gone".into(),
            hint: "resolve m1".into(),
        });
        let out = frame(&v, 170, 16);
        println!("{out}");
        assert!(out.contains("cores 12/12 in use · 0 free"));
        assert!(out.contains("cpu 50% · mem 9.0 GiB free"));
        assert!(out.contains(" queue "));
        assert!(!out.contains("running 3, waiting 2"));
        // No legend; the worktree is a column.
        assert!(!out.contains("worktrees"));
        assert!(!out.contains("(root)"));
        assert!(out.contains("worktree") && out.contains("main (current)"));
        assert!(out.contains("fix/queue-") && out.contains("…"));
        assert!(out.contains("held, owner gone (1): m1 owner gone -> resolve m1"));
        assert!(out.contains("q: quit"));
        assert!(out.contains("now") && out.contains("│"));
        assert!(out.contains("starts ~") && out.contains("(needs 6, 0 free)"));
        assert!(out.contains("idle") && out.contains("[backfill]"));
        // The reserved cores are plain numbers: no `w6`, no `4/3.4`.
        assert!(!out.contains("w6") && !out.contains("4/3.4"));
        // Rows are numbered #1 to #5, top to bottom.
        let order: Vec<String> = out
            .lines()
            .filter(|l| l.contains('▌'))
            .map(|l| {
                let at = l.find('#').unwrap();
                l[at..].chars().take(2).collect()
            })
            .collect();
        assert_eq!(order, ["#1", "#2", "#3", "#4", "#5"]);
        assert!(!out.contains("A "));
        // Somebody is waiting: no "No one waiting" line.
        assert!(!out.contains("No one waiting"));
        // Tiny and narrow terminals must not panic; the time columns stay.
        let _ = frame(&v, 20, 5);
        let _ = frame(&v, 1, 1);
        let narrow = frame(&v, 62, 16);
        assert!(
            narrow.contains("left 1:35") && narrow.contains("starts"),
            "{narrow}"
        );
    }

    #[test]
    fn local_ci_run_is_never_cut_at_90_or_120_columns() {
        let v = sample_view(Utc::now());
        for w in [90u16, 120] {
            let out = frame(&v, w, 16);
            println!("{out}");
            assert_eq!(out.matches("local-ci run").count(), 3, "{w}\n{out}");
            assert!(!out.contains("local-ci …") && !out.contains("local-ci r…"));
            // The time columns are all there.
            assert!(out.contains("left 3:20") && out.contains("ran 0:12"), "{w}");
            assert!(out.contains("starts ~"), "{w}");
            // The axis has a label, and the bar is at least 20 wide.
            let head = out.lines().find(|l| l.contains("worktree")).unwrap();
            assert!(head.contains("+"), "{w}\n{out}");
            let rows = table_rows(&v);
            let cols = layout(w as usize - 2, table_wants(&v, &rows, 4));
            assert!(cols.bar >= 20, "{w}: {cols:?}");
        }
        // At 90 the reason for waiting, the long flags, the project and the
        // cores column are what gave way.
        let narrow = frame(&v, 90, 16);
        assert!(!narrow.contains("project") && !narrow.contains("cores  "));
        assert!(!narrow.contains("(needs 6") && !narrow.contains("(behind 1)"));
        assert!(narrow.contains("local-ci run z b"), "{narrow}");
        // The lone worktrees show their project (gray) instead of `-`.
        let t = render(&v, 90, 16);
        let buf = t.backend().buffer().clone();
        let y = row_y(&buf, 90, 16, "local-ci run z b");
        let line: String = (0..90u16)
            .map(|x| buf[(x, y)].symbol().to_string())
            .collect();
        assert!(line.contains("local-ci   "), "{line}");
        let x = line
            .find("local-ci")
            .map(|b| line[..b].chars().count() as u16)
            .unwrap();
        assert!(buf[(x, y)].modifier.contains(Modifier::DIM));
        // With the project column the cell stays `-`.
        let wide = frame(&v, 170, 16);
        assert!(wide
            .lines()
            .any(|l| l.contains("local-ci") && l.contains(" - ")));
    }

    fn row_y(buf: &ratatui::buffer::Buffer, w: u16, h: u16, needle: &str) -> u16 {
        (0..h)
            .find(|&y| {
                (0..w)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
                    .contains(needle)
            })
            .unwrap_or_else(|| panic!("no row with {needle}"))
    }

    #[test]
    fn the_band_is_the_worktree_color_and_my_row_is_bold() {
        let v = sample_view(Utc::now());
        let t = render(&v, 120, 16);
        let buf = t.backend().buffer().clone();
        let at = |needle: &str| {
            let y = row_y(&buf, 120, 16, needle);
            // Column 0 is the block border; the band is the next column.
            (buf[(1, y)].clone(), buf[(2, y)].clone())
        };
        let (band, num) = at("cargo test --workspace");
        assert_eq!(band.symbol(), "▌");
        assert_eq!(band.fg, v.worktrees[0].color);
        assert_eq!(num.symbol(), "#");
        assert!(!num.modifier.contains(Modifier::BOLD));
        let (band, _) = at("local-ci run");
        assert_eq!(band.fg, Color::Gray);
        // The row of the worktree top runs in is bold, the others are not.
        let (band, num) = at("cargo clippy");
        assert_eq!(band.fg, v.worktrees[2].color);
        assert!(num.modifier.contains(Modifier::BOLD));
        // The worktree name has the worktree's color.
        let y = row_y(&buf, 120, 16, "cargo clippy");
        let x = (0..120u16)
            .find(|&x| buf[(x, y)].symbol() == "m" && buf[(x + 1, y)].symbol() == "a")
            .unwrap();
        assert_eq!(buf[(x, y)].fg, v.worktrees[2].color);
        // The cores bar uses the same colors, led by `#1`.
        let by = row_y(&buf, 120, 16, "cores 12/12") + 1;
        assert_eq!(buf[(1, by)].symbol(), "#");
        assert_eq!(buf[(2, by)].symbol(), "1");
        assert_eq!(buf[(1, by)].bg, v.worktrees[0].color);
    }

    #[test]
    fn only_the_overrun_is_drawn_red() {
        let now = Utc::now();
        let mut a = entry("a", State::Running, 1, 1, "/r/a", "", "a", now, 100);
        a.expected_secs = Some(60.0);
        let mut b = entry("b", State::Running, 1, 2, "/r/b", "", "b", now, 20);
        b.expected_secs = Some(60.0);
        let v = View::from_parts(&ledger(vec![a, b]), vec![], limits(4), None, now, None, &[]);
        let t = render(&v, 80, 12);
        let buf = t.backend().buffer().clone();
        let red = (0..12u16)
            .flat_map(|y| (0..80u16).map(move |x| (x, y)))
            .filter(|&(x, y)| buf[(x, y)].symbol() == "▓")
            .collect::<Vec<_>>();
        assert!(!red.is_empty());
        assert!(red.iter().all(|&(x, y)| buf[(x, y)].fg == Color::Red));
        // The overrun is on one row only: the one that is over.
        let y0 = red[0].1;
        assert!(red.iter().all(|&(_, y)| y == y0));
        // And its text says so, in red.
        let line: String = (0..80u16)
            .map(|x| buf[(x, y0)].symbol().to_string())
            .collect();
        let x = line.find("over").unwrap();
        let col = line[..x].chars().count() as u16;
        assert_eq!(buf[(col, y0)].fg, Color::Red);
    }

    fn rec(
        repo: &str,
        argv: &str,
        wt: Option<(&str, &str)>,
        ended_ago: i64,
        wall: f64,
        exit: i32,
        now: DateTime<Utc>,
    ) -> Record {
        Record {
            key: "k".into(),
            repo: repo.into(),
            subdir: String::new(),
            argv: argv.into(),
            key_argv: String::new(),
            started_at: now - Cd::seconds(ended_ago) - Cd::milliseconds((wall * 1000.0) as i64),
            wall_secs: wall,
            cpu_secs: 1.0,
            max_rss_bytes: 1,
            exit_code: exit,
            weight_used: 1,
            contended: false,
            measurement_id: None,
            worktree: wt.map(|w| w.0.into()),
            branch: wt.map(|w| w.1.into()),
        }
    }

    /// Finished runs, oldest first in the file (as `history.jsonl` is).
    fn history(now: DateTime<Utc>) -> Vec<Record> {
        let rtr = "/src/remote-task-runner";
        vec![
            rec(
                "/src/local-ci",
                "local-ci run",
                Some((LCI, "main")),
                3000,
                250.0,
                0,
                now,
            ),
            rec(rtr, "cargo test --workspace", None, 2500, 180.0, 0, now),
            rec(
                "/src/local-ci",
                "local-ci run",
                Some((LCI, "main")),
                1500,
                240.0,
                0,
                now,
            ),
            rec(
                rtr,
                "cargo clippy --all-targets",
                Some((ME, "main")),
                540,
                48.0,
                0,
                now,
            ),
            rec(
                rtr,
                "cargo test --workspace",
                Some((FIX, FIX_BRANCH)),
                420,
                190.0,
                1,
                now,
            ),
            rec(
                "/src/local-ci",
                "bash /ci/steps/step-8.sh --fast",
                Some((LCI, "main")),
                120,
                245.0,
                0,
                now,
            ),
        ]
    }

    fn empty_view(now: DateTime<Utc>, hist: &[Record]) -> View {
        View::from_parts(
            &Ledger::default(),
            vec![],
            limits(12),
            Some(live()),
            now,
            Some(ME),
            hist,
        )
    }

    #[test]
    fn the_empty_state_texts_say_what_is_free() {
        assert_eq!(
            empty_lines(12),
            (
                "Nothing running or waiting".to_string(),
                "A new command starts right away · 12 cores free".to_string()
            )
        );
        assert_eq!(
            empty_lines(1).1,
            "A new command starts right away · 1 core free"
        );
        assert_eq!(
            empty_lines(0).1,
            "A new command starts right away · 0 cores free"
        );
        assert_eq!(
            nobody_waiting(5),
            "No one waiting · a command needing ≤5 cores starts right away"
        );
        assert_eq!(
            nobody_waiting(1),
            "No one waiting · a command needing ≤1 core starts right away"
        );
        assert_eq!(
            nobody_waiting(0),
            "No one waiting · the next command waits for cores"
        );
        let s = Duration::from_secs;
        assert_eq!(ago_text(s(0)), "0s ago");
        assert_eq!(ago_text(s(59)), "59s ago");
        assert_eq!(ago_text(s(120)), "2m ago");
        assert_eq!(ago_text(s(3599)), "59m ago");
        assert_eq!(ago_text(s(3600)), "1h ago");
        assert_eq!(ago_text(s(86399)), "23h ago");
        assert_eq!(ago_text(s(86400 * 3)), "3d ago");
    }

    #[test]
    fn recent_runs_are_the_newest_five_with_their_worktree_and_failure() {
        let now = Utc::now();
        let rows = recent_rows(&history(now), now, Some(ME), RECENT_MAX);
        assert_eq!(rows.len(), 5);
        // Newest first: the step script, the failed test run, clippy, ...
        let jobs: Vec<_> = rows.iter().map(|r| r.job.as_str()).collect();
        assert_eq!(
            jobs,
            [
                "step-8.sh",
                "cargo test --workspace",
                "cargo clippy --all-targets",
                "local-ci run",
                "cargo test --workspace"
            ]
        );
        assert_eq!(rows[0].project, "local-ci");
        assert_eq!(rows[0].took, Duration::from_secs(245));
        assert_eq!(ago_text(rows[0].ago), "2m ago");
        assert_eq!(ago_text(rows[1].ago), "7m ago");
        assert_eq!(rows[1].failed, Some(1));
        assert_eq!(rows[0].failed, None);
        // Worktrees: remote-task-runner has two among these, so both are
        // colored; local-ci has one (gray); an old record has none.
        let wt = |i: usize| rows[i].wt.as_ref();
        assert_eq!(worktree_text(wt(1), 100), FIX_BRANCH);
        assert_eq!(worktree_text(wt(2), 40), "main (current)");
        assert_ne!(wt(1).unwrap().color, wt(2).unwrap().color);
        assert_eq!(worktree_text(wt(0), 40), "-");
        assert_eq!(wt(0).unwrap().color, Color::Gray);
        assert_eq!(rows[4].project, "remote-task-runner");
        // An unrecorded worktree (old history) is `-`.
        let old = recent_rows(&history(now)[1..2], now, None, 5);
        assert!(old[0].wt.is_none());
        assert_eq!(worktree_text(old[0].wt.as_ref(), 40), "-");
        assert!(recent_rows(&[], now, None, 5).is_empty());
        assert_eq!(recent_time_text(&rows[0]), "4:05 · 2m ago");
    }

    #[test]
    fn an_empty_queue_lists_what_finished_lately() {
        let now = Utc::now();
        let v = empty_view(now, &history(now));
        assert_eq!(v.recent.len(), 5);
        let t = render(&v, 90, 16);
        let buf = t.backend().buffer().clone();
        let out = frame(&v, 90, 16);
        println!("{out}");
        assert!(out.contains(" queue "));
        assert!(out.contains("cores 0/12 in use · 12 free"));
        assert!(out.contains("Nothing running or waiting"));
        assert!(out.contains("A new command starts right away · 12 cores free"));
        assert!(out.contains("recently finished"));
        assert!(!out.contains("Run one with"));
        assert!(!out.contains("No one waiting"));
        // Five rows under the heading, newest first, with the exit code of
        // the failed one.
        let lines: Vec<&str> = out.lines().collect();
        let h = lines
            .iter()
            .position(|l| l.contains("recently finished"))
            .unwrap();
        let rows = &lines[h + 1..h + 6];
        assert!(rows[0].contains("step-8.sh") && rows[0].contains("4:05 · 2m ago"));
        assert!(rows[1].contains("exit 1") && rows[1].contains("7m ago"));
        assert!(rows[1].contains("fix/queue-"));
        assert!(rows[2].contains("main (current)"));
        assert_eq!(out.matches("exit ").count(), 1);
        assert!(rows.iter().all(|l| l.contains("▌")));
        // In the upper half of the box; the first line is plain, the second
        // dim.
        let y1 = row_y(&buf, 90, 16, "Nothing running");
        let y2 = row_y(&buf, 90, 16, "A new command");
        assert!(y1 < 8 && y2 == y1 + 1);
        let x1 = (0..90u16).find(|&x| buf[(x, y1)].symbol() == "N").unwrap();
        assert!(!buf[(x1, y1)].modifier.contains(Modifier::BOLD));
        assert!(!buf[(x1, y1)].modifier.contains(Modifier::DIM));
        assert!(buf[(x1, y2)].modifier.contains(Modifier::DIM));
        let yh = row_y(&buf, 90, 16, "recently finished");
        assert!(buf[(x1, yh)].modifier.contains(Modifier::DIM));
        // The failure is red.
        let yf = row_y(&buf, 90, 16, "exit 1");
        let xf = (0..90u16)
            .find(|&x| buf[(x, yf)].symbol() == "e" && buf[(x + 1, yf)].symbol() == "x")
            .unwrap();
        assert_eq!(buf[(xf, yf)].fg, Color::Red);
        // Narrow and tiny terminals do not panic.
        let _ = frame(&v, 40, 10);
        let _ = frame(&v, 10, 4);
    }

    #[test]
    fn an_empty_queue_with_no_history_says_how_to_start_one() {
        let now = Utc::now();
        let v = empty_view(now, &[]);
        let out = frame(&v, 90, 12);
        println!("{out}");
        assert!(out.contains("Nothing running or waiting"));
        assert!(out.contains("Run one with: task-traffic-controller exec -- <command>"));
        assert!(!out.contains("recently finished"));
        let t = render(&v, 90, 12);
        let buf = t.backend().buffer().clone();
        let y = row_y(&buf, 90, 12, "Run one with");
        let x = (0..90u16).find(|&x| buf[(x, y)].symbol() == "R").unwrap();
        assert!(buf[(x, y)].modifier.contains(Modifier::DIM));
    }

    #[test]
    fn running_without_waiting_says_what_would_start_right_away() {
        let now = Utc::now();
        let mut a = entry("a", State::Running, 4, 1, "/r/a", "", "cargo test", now, 30);
        a.expected_secs = Some(100.0);
        let v = View::from_parts(&ledger(vec![a]), vec![], limits(12), None, now, None, &[]);
        let out = frame(&v, 90, 12);
        println!("{out}");
        assert!(out.contains("No one waiting · a command needing ≤8 cores starts right away"));
        assert!(!out.contains("Nothing running"));
        let full = {
            let a = entry(
                "a",
                State::Running,
                12,
                1,
                "/r/a",
                "",
                "cargo test",
                now,
                30,
            );
            View::from_parts(&ledger(vec![a]), vec![], limits(12), None, now, None, &[])
        };
        assert!(frame(&full, 90, 12).contains("No one waiting · the next command waits for cores"));
        // Someone waiting: the line is not there.
        let out = frame(&sample_view(now), 90, 16);
        assert!(!out.contains("No one waiting"));
    }

    /// The screens drawn for the pull request description. Run with
    /// `--nocapture` to see them.
    #[test]
    fn the_screens_for_the_description() {
        let now = Utc::now();
        for w in [90u16, 120] {
            println!("== width {w}\n{}", frame(&sample_view(now), w, 16));
        }
        println!(
            "== empty, with history\n{}",
            frame(&empty_view(now, &history(now)), 90, 16)
        );
        println!(
            "== empty, no history\n{}",
            frame(&empty_view(now, &[]), 90, 12)
        );
        let mut a = entry(
            "a",
            State::Running,
            4,
            1,
            "/src/local-ci",
            "",
            "local-ci run",
            now,
            30,
        );
        a.expected_secs = Some(100.0);
        let a = in_wt(a, LCI, "main");
        let v = View::from_parts(
            &ledger(vec![a]),
            vec![],
            limits(12),
            Some(live()),
            now,
            Some(ME),
            &[],
        );
        println!("== running, nobody waiting\n{}", frame(&v, 90, 12));
    }
}
