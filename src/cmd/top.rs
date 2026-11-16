//! `top`: a full-screen view of the queue (alternate screen, raw mode),
//! refreshed every `--interval` seconds and on resize.
//!
//! [`View`] is a plain description of what is on screen, built from the
//! ledger by [`View::build`] (pure) and drawn by [`draw`]. Everything that
//! decides what a row says (order, numbers, the progress bar, the bar
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
    /// No colors (`NO_COLOR` set, or a dumb terminal): bars use `█` and `░`.
    pub plain: bool,
    /// Which rows get a color: only mine (`Focus`), or one per worktree.
    pub mode: ColorMode,
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
        let mut view = View::from_parts(
            &ledger,
            held,
            cfg.limits(),
            ctx.live.snapshot(),
            ctx.clock.now(),
            me,
        );
        view.plain = no_color(
            std::env::var("NO_COLOR").ok().as_deref(),
            std::env::var("TERM").ok().as_deref(),
        );
        Ok(view)
    }

    pub fn from_parts(
        ledger: &Ledger,
        held: Vec<HeldRow>,
        limits: Limits,
        live: Option<LiveSnapshot>,
        now: DateTime<Utc>,
        me: Option<&str>,
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
            running: run_rows,
            waiting: wait_rows,
            held,
            plain: false,
            mode: default_mode(me),
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

/// Colors a worktree can get in the overview. No red or anything that
/// reads as red (overruns), no yellow (idle), no gray (a repository with a
/// single worktree), nothing too dark to read on a dark background.
const WT_COLORS: [Color; 8] = [
    Color::LightBlue,
    Color::LightCyan,
    Color::LightGreen,
    Color::LightMagenta,
    Color::Cyan,
    Color::Green,
    Color::Magenta,
    Color::Indexed(141),
];

/// The color of the current worktree's rows in `Focus`.
const FOCUS_COLOR: Color = Color::LightBlue;
/// Text, band and cores of the rows that get no color in `Focus`.
const MUTED: Color = Color::Indexed(243);

/// How rows are colored: `Focus` colors only the worktree `top` was
/// started in and mutes the rest; `Overview` gives every worktree (of a
/// repository with several) its own color. `c` switches between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorMode {
    Focus,
    Overview,
}

impl ColorMode {
    pub fn toggled(self) -> ColorMode {
        match self {
            ColorMode::Focus => ColorMode::Overview,
            ColorMode::Overview => ColorMode::Focus,
        }
    }

    fn label(self) -> &'static str {
        match self {
            ColorMode::Focus => "focus",
            ColorMode::Overview => "overview",
        }
    }
}

/// `Focus` when `top` runs in a git worktree (it knows which is its own),
/// `Overview` otherwise.
pub fn default_mode(me: Option<&str>) -> ColorMode {
    if me.is_some() {
        ColorMode::Focus
    } else {
        ColorMode::Overview
    }
}

/// The color of a row of worktree `w`, or `None` when the row has none:
/// in `Focus` every row but mine, in `Overview` a lone worktree's.
pub fn tint(mode: ColorMode, w: Option<&Wt>) -> Option<Color> {
    let w = w?;
    match mode {
        ColorMode::Focus => w.mine.then_some(FOCUS_COLOR),
        ColorMode::Overview => (w.color != Color::Gray).then_some(w.color),
    }
}

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

// ------------------------------------------------------------ the bar

const FILLED: char = '█';
const EMPTY: char = '░';

/// How full a running row's bar is: time run over the expected time, at
/// most 1. `None` when the command has no learned wall time (no bar).
fn run_fraction(elapsed: Duration, expected_secs: Option<f64>) -> Option<f64> {
    let exp = expected_secs.filter(|x| *x > 0.0)?;
    Some((elapsed.as_secs_f64() / exp).clamp(0.0, 1.0))
}

/// Filled cells of a bar `width` wide at `frac` (0 to 1); `None` when the
/// row has no bar. Only a full fraction fills every cell.
pub fn bar_fill(frac: Option<f64>, width: usize) -> Option<usize> {
    frac.map(|f| {
        if f >= 1.0 {
            width
        } else {
            ((f.max(0.0) * width as f64).floor() as usize).min(width.saturating_sub(1))
        }
    })
}

/// A bar `width` characters wide: `█` for what is done, `░` for the rest;
/// blank when the row has no bar.
#[cfg(test)]
pub fn bar_string(frac: Option<f64>, width: usize) -> String {
    match bar_fill(frac, width) {
        Some(n) => format!(
            "{}{}",
            FILLED.to_string().repeat(n),
            EMPTY.to_string().repeat(width - n)
        ),
        None => " ".repeat(width),
    }
}

/// Colors cannot be used: `NO_COLOR` is set to anything but nothing, or the
/// terminal is `dumb`.
fn no_color(no_color: Option<&str>, term: Option<&str>) -> bool {
    no_color.is_some_and(|v| !v.is_empty()) || term == Some("dumb")
}

/// The bar's track: a faint gray background, close to a dark terminal's own.
const TRACK: Color = Color::Indexed(236);
/// What a row without a color fills the track with: lighter than it.
const LONE_FILL: Color = Color::Indexed(245);
/// Left blocks of 1/8 to 7/8 of a cell.
const EIGHTHS: [char; 7] = ['▏', '▎', '▍', '▌', '▋', '▊', '▉'];

/// A bar in colors: `full` cells in the task's color, then a `part` cell
/// (a left block, foreground the task's color on the track), then `rest`
/// cells of track.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BarCells {
    pub full: usize,
    pub part: Option<char>,
    pub rest: usize,
}

pub fn bar_cells(frac: f64, width: usize) -> BarCells {
    let max = width * 8;
    let eighths = if frac >= 1.0 {
        max
    } else {
        ((frac.max(0.0) * max as f64).floor() as usize).min(max.saturating_sub(1))
    };
    let (full, rem) = (eighths / 8, eighths % 8);
    let part = (rem > 0).then(|| EIGHTHS[rem - 1]);
    BarCells {
        full,
        part,
        rest: width - full - usize::from(part.is_some()),
    }
}

// -------------------------------------------------------------- the table

/// Column widths for a terminal `width` wide.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cols {
    pub project: usize,
    pub worktree: usize,
    pub job: usize,
    pub bar: usize,
    pub right: usize,
    /// The text after the bar names why a waiting entry waits.
    pub why: bool,
    /// `idle` and `[backfill]` are shown as `z` and `b`.
    pub short_flags: bool,
}

const BAR_MIN: usize = 10;
/// On a terminal this wide the bar is never narrower than [`BAR_WIDE_MIN`].
const WIDE: usize = 80;
const BAR_WIDE_MIN: usize = 20;
const BAR_FLOOR: usize = 6;
const RIGHT_MAX: usize = 60;
const RIGHT_FLOOR: usize = 8;
const PROJECT_CAP: usize = 24;
const WT_CAP: usize = 32;
const JOB_CAP: usize = 60;
/// Before a column is dropped, the worktree and job columns are squeezed
/// to this much (the middle of the name goes).
const WT_SQUEEZED: usize = 20;
const PROJECT_SQUEEZED: usize = 20;
const JOB_SQUEEZED: usize = 24;
/// The project, worktree and job columns are never cut below this.
const NAME_FLOOR: usize = 8;

/// What each column would like, before it is fitted to the screen.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Wants {
    /// The color band, the number and the gap after them.
    pub prefix: usize,
    pub project: usize,
    pub worktree: usize,
    /// The longest job name with its flags written out, and abbreviated.
    pub job: usize,
    pub job_short_flags: usize,
    /// The least the worktree column may get: room for the branch (4
    /// characters) and ` (current)` when a row has it.
    pub worktree_floor: usize,
    /// The text after the bar with and without the reason for waiting.
    pub right: usize,
    pub right_short: usize,
}

/// Fit the columns in `width`, keeping the bar readable.
///
/// On a terminal at least [`WIDE`] columns wide the bar is never narrower
/// than [`BAR_WIDE_MIN`]. To get there, in this order: the reason for
/// waiting after the bar goes, the project, worktree and job columns are
/// squeezed to 20, 20 and 24 characters (the middle of the name goes),
/// `idle` and `[backfill]` shrink to `z` and `b`, and only then the three
/// names are cut further, the widest first. No column is dropped.
pub fn layout(width: usize, w: Wants) -> Cols {
    let bar_min = if width >= WIDE { BAR_WIDE_MIN } else { BAR_MIN };
    let bar_gap = 1;
    let wt_floor = w.worktree_floor.max(NAME_FLOOR);
    // (why, squeeze, short flags), most to least.
    let states = [
        (true, false, false),
        (false, false, false),
        (false, true, false),
        (false, true, true),
    ];
    let build = |(why, squeeze, short): (bool, bool, bool)| {
        let right = if why { w.right } else { w.right_short };
        let job = if short { w.job_short_flags } else { w.job };
        let (pr_cap, wt_cap, job_cap) = if squeeze {
            (PROJECT_SQUEEZED, WT_SQUEEZED, JOB_SQUEEZED)
        } else {
            (PROJECT_CAP, WT_CAP, JOB_CAP)
        };
        Cols {
            project: w.project.clamp(7, pr_cap),
            worktree: w
                .worktree
                .clamp(1, wt_cap.max(wt_floor))
                .max(wt_floor.min(w.worktree)),
            job: job.clamp(1, job_cap),
            bar: bar_min,
            right: right.clamp(RIGHT_FLOOR, RIGHT_MAX),
            why,
            short_flags: short,
        }
    };
    let need = |c: &Cols| {
        w.prefix + c.project + 1 + c.worktree + 1 + c.job + 1 + c.bar + bar_gap + c.right
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
    // The three names give up one character at a time, the widest first.
    while short > 0 {
        let widest = [
            (c.worktree, wt_floor),
            (c.job, NAME_FLOOR),
            (c.project, NAME_FLOOR),
        ]
        .iter()
        .enumerate()
        .filter(|(_, (cur, floor))| cur > floor)
        .max_by_key(|(_, (cur, _))| *cur)
        .map(|(i, _)| i);
        match widest {
            Some(0) => c.worktree -= 1,
            Some(1) => c.job -= 1,
            Some(_) => c.project -= 1,
            None => break,
        }
        short -= 1;
    }
    for (cur, floor) in [(&mut c.bar, BAR_FLOOR), (&mut c.right, RIGHT_FLOOR)] {
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
    let room = max.saturating_sub(suffix.chars().count()).max(4);
    format!("{}{suffix}", ellipsize_middle(&branch, room))
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
        Progress::Known {
            left, over: true, ..
        } => format!("over {}", fmt_duration(*left)),
        Progress::Known { pct, left, .. } => {
            format!("{pct}%  left {}", fmt_duration(*left))
        }
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
    /// Share of the bar filled (0 for a waiting row).
    frac: Option<f64>,
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
        frac: Some(run_fraction(r.elapsed, r.expected_secs).unwrap_or(0.0)),
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
        frac: Some(0.0),
        right: right_text_wait(w),
        right_short: starts_text(w),
        wt: w.wt,
        over: false,
    });
    run.chain(wait).collect()
}

// ------------------------------------------------------------ empty state

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

// ---------------------------------------------------------------- drawing

fn row_tint(v: &View, wt: Option<usize>) -> Option<Color> {
    tint(v.mode, wt.and_then(|i| v.worktrees.get(i)))
}

/// The color of a row's band and cores slots: its tint, else muted (`Focus`)
/// or plain gray (`Overview`).
fn band_color(v: &View, wt: Option<usize>) -> Color {
    row_tint(v, wt).unwrap_or(match v.mode {
        ColorMode::Focus => MUTED,
        ColorMode::Overview => Color::Gray,
    })
}

/// `base` with the row's color on its text: its tint, muted in `Focus` when
/// it has none, left alone in `Overview`.
fn tinted(v: &View, tint: Option<Color>, base: Style) -> Style {
    match (tint, v.mode) {
        (Some(c), _) => base.fg(c),
        (None, ColorMode::Focus) => base.fg(MUTED),
        (None, ColorMode::Overview) => base,
    }
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
        Span::styled(
            format!("  {}", v.mode.label()),
            Style::new().add_modifier(Modifier::DIM),
        ),
    ];
    if let Some(e) = error {
        spans.push(Span::styled(
            format!("  {}", truncate(e, 60)),
            Style::new().fg(Color::Red),
        ));
    }
    let hint = "c: colors  q: quit";
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
        let c = band_color(v, s.wt);
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

/// A column: `s` cut to `w` characters, then padded to `w`, plus one space
/// that separates it from the next column even when `s` fills the width.
fn col(s: &str, w: usize) -> String {
    format!("{} ", fit(s, w))
}

/// Pad or cut `s` to exactly `w` characters.
fn fit(s: &str, w: usize) -> String {
    let t = truncate(s, w);
    let n = t.chars().count();
    format!("{t}{}", " ".repeat(w - n))
}

/// What the rows of the table would like in the way of columns.
fn table_wants(v: &View, rows: &[TableRow], prefix: usize) -> Wants {
    let widest = |len: &dyn Fn(&TableRow) -> usize| rows.iter().map(len).max().unwrap_or(0);
    let wt_text = |r: &TableRow| {
        let wt = r.wt.and_then(|i| v.worktrees.get(i));
        worktree_text(wt, usize::MAX).chars().count()
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
        worktree: widest(&wt_text).max("worktree".len()),
        job: widest(&|r| job(r, false)),
        job_short_flags: widest(&|r| job(r, true)),
        worktree_floor: 4 + if v.worktrees.iter().any(|w| w.mine) {
            " (current)".len()
        } else {
            0
        },
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

    let dim = Style::new().add_modifier(Modifier::DIM);
    let mut lines = Vec::new();
    // Header: column names, then the ruler.
    let mut head = vec![Span::raw(" ".repeat(prefix))];
    head.push(Span::styled(col("project", cols.project), dim));
    head.push(Span::styled(col("worktree", cols.worktree), dim));
    head.push(Span::styled(col("job", cols.job), dim));
    head.push(Span::styled(col("progress", cols.bar), dim));
    lines.push(Line::from(head));

    for r in &rows {
        let wt = wt_of(r);
        let mine = wt.is_some_and(|w| w.mine);
        let base = if mine {
            Style::new().add_modifier(Modifier::BOLD)
        } else {
            Style::new()
        };
        let tint = row_tint(v, r.wt);
        let text = tinted(v, tint, base);
        let mut spans = vec![
            Span::styled("▌", Style::new().fg(band_color(v, r.wt))),
            Span::styled(format!("{:>num_w$} ", format!("#{}", r.num)), text),
        ];
        spans.push(Span::styled(col(&r.project, cols.project), text));
        let cell = worktree_text(wt, cols.worktree);
        spans.push(Span::styled(col(&cell, cols.worktree), text));
        let flag_text = |f: &Flag| if cols.short_flags { f.1 } else { f.0 };
        let shown = &r.flags;
        let flag_strs: Vec<&str> = shown.iter().map(flag_text).collect();
        let (name, flags) = job_parts(&r.job, &flag_strs, cols.job);
        let used = name.chars().count() + flags.chars().count();
        spans.push(Span::styled(name, text));
        for flag in shown {
            if flags.contains(flag_text(flag)) {
                spans.push(Span::styled(
                    format!(" {}", flag_text(flag)),
                    if *flag == IDLE {
                        Style::new().fg(Color::Yellow)
                    } else {
                        tinted(v, tint, dim)
                    },
                ));
            }
        }
        spans.push(Span::raw(" ".repeat((cols.job + 1).saturating_sub(used))));
        match r.frac {
            None => spans.push(Span::raw(" ".repeat(cols.bar))),
            Some(frac) if v.plain => {
                let n = bar_fill(Some(frac), cols.bar).unwrap_or(0);
                spans.push(Span::styled(
                    FILLED.to_string().repeat(n),
                    tinted(v, tint, base),
                ));
                spans.push(Span::styled(EMPTY.to_string().repeat(cols.bar - n), dim));
            }
            Some(frac) => {
                let fill = tint.unwrap_or(LONE_FILL);
                let cells = bar_cells(frac, cols.bar);
                spans.push(Span::styled(" ".repeat(cells.full), Style::new().bg(fill)));
                if let Some(ch) = cells.part {
                    spans.push(Span::styled(
                        ch.to_string(),
                        Style::new().fg(fill).bg(TRACK),
                    ));
                }
                spans.push(Span::styled(" ".repeat(cells.rest), Style::new().bg(TRACK)));
            }
        }
        spans.push(Span::styled(
            format!(
                " {}",
                truncate(if cols.why { &r.right } else { &r.right_short }, cols.right)
            ),
            if r.over {
                Style::new().fg(Color::Red)
            } else {
                text
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

/// The inside of the queue box when nothing runs or waits.
fn draw_empty(f: &mut Frame, area: Rect, v: &View) {
    let dim = Style::new().add_modifier(Modifier::DIM);
    let (first, second) = empty_lines(v.free);
    let lines = vec![
        Line::raw(""),
        Line::raw(format!(" {first}")),
        Line::styled(format!(" {second}"), dim),
    ];
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

/// `c` (without Ctrl, which quits) switches the color mode.
fn is_toggle(key: &event::KeyEvent) -> bool {
    key.kind == KeyEventKind::Press
        && key.code == KeyCode::Char('c')
        && !key.modifiers.contains(KeyModifiers::CONTROL)
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
    let mut mode = view.mode;
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
                Event::Key(k) if is_toggle(&k) => {
                    mode = mode.toggled();
                    view.mode = mode;
                    continue;
                }
                // A resize (or any other event) redraws with the same data.
                _ => continue,
            }
        }
        match View::build(ctx, cfg, me.as_deref()) {
            Ok(v) => {
                view = v;
                view.mode = mode;
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
    }
    #[test]
    fn equal_finish_times_keep_the_older_run_first() {
        let now = Utc::now();
        let mut x = entry("x", State::Running, 1, 1, "/r/a", "", "x", now, 30);
        let mut y = entry("y", State::Running, 1, 2, "/r/b", "", "y", now, 50);
        x.expected_secs = Some(100.0);
        y.expected_secs = Some(120.0); // 70 left for both
        let v = View::from_parts(&ledger(vec![x, y]), vec![], limits(4), None, now, None);
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
        let v = View::from_parts(&ledger(vec![x, y]), vec![], limits(6), None, now, None);
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
        let v = View::from_parts(&ledger(vec![a]), vec![], limits(4), None, now, Some(ME));
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
        let v = View::from_parts(&ledger(vec![old, new]), vec![], limits(4), None, now, None);
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
    fn no_palette_color_reads_as_red_or_is_too_dark() {
        // Named reds, the ANSI red slots, and the red-dominant cube/ramp
        // entries of the 256-color palette.
        for c in WT_COLORS.iter().chain([&FOCUS_COLOR]) {
            match *c {
                Color::Red | Color::LightRed | Color::Black | Color::DarkGray | Color::Gray => {
                    panic!("{c:?}")
                }
                Color::Indexed(i) => {
                    assert!(![1, 9, 52, 88, 124, 160, 196, 202, 203, 204].contains(&i));
                    assert!(i >= 16 && !(232..=243).contains(&i), "{i}");
                    if (16..232).contains(&i) {
                        let (r, g, b) = ((i - 16) / 36, (i - 16) / 6 % 6, (i - 16) % 6);
                        assert!(r <= g.max(b), "{i} is red-dominant");
                    }
                }
                _ => {}
            }
        }
        assert!(WT_COLORS.contains(&FOCUS_COLOR));
    }

    #[test]
    fn the_mode_follows_where_top_runs_and_c_switches_it() {
        assert_eq!(default_mode(Some(ME)), ColorMode::Focus);
        assert_eq!(default_mode(None), ColorMode::Overview);
        assert_eq!(ColorMode::Focus.toggled(), ColorMode::Overview);
        assert_eq!(ColorMode::Focus.toggled().toggled(), ColorMode::Focus);
        let key = |code, m| event::KeyEvent::new(code, m);
        assert!(is_toggle(&key(KeyCode::Char('c'), KeyModifiers::NONE)));
        assert!(!is_toggle(&key(KeyCode::Char('c'), KeyModifiers::CONTROL)));
        assert!(!is_toggle(&key(KeyCode::Char('q'), KeyModifiers::NONE)));
        assert!(is_quit(&key(KeyCode::Char('c'), KeyModifiers::CONTROL)));
        assert!(!is_quit(&key(KeyCode::Char('c'), KeyModifiers::NONE)));
    }

    #[test]
    fn a_row_has_a_color_only_when_the_mode_gives_it_one() {
        let wt = |mine, color| Wt {
            path: "/w".into(),
            branch: "b".into(),
            mine,
            color,
        };
        let (me, other, lone) = (
            wt(true, Color::Cyan),
            wt(false, Color::Green),
            wt(false, Color::Gray),
        );
        // Focus: mine in the fixed color, everything else none.
        assert_eq!(tint(ColorMode::Focus, Some(&me)), Some(FOCUS_COLOR));
        assert_eq!(tint(ColorMode::Focus, Some(&other)), None);
        assert_eq!(tint(ColorMode::Focus, Some(&lone)), None);
        assert_eq!(tint(ColorMode::Focus, None), None);
        // Overview: each worktree's own, none for a lone one.
        assert_eq!(tint(ColorMode::Overview, Some(&me)), Some(Color::Cyan));
        assert_eq!(tint(ColorMode::Overview, Some(&other)), Some(Color::Green));
        assert_eq!(tint(ColorMode::Overview, Some(&lone)), None);
        assert_eq!(tint(ColorMode::Overview, None), None);
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
        let v = View::from_parts(&ledger(vec![ea, eb]), vec![], limits(4), None, now, None);
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

    #[test]
    fn a_running_bar_fills_with_the_time_run_over_the_expected_time() {
        let d = Duration::from_secs;
        // 12 of 20 s: 60% of 10 cells.
        assert_eq!(run_fraction(d(12), Some(20.0)), Some(0.6));
        assert_eq!(
            bar_string(run_fraction(d(12), Some(20.0)), 10),
            "██████░░░░"
        );
        assert_eq!(bar_string(Some(0.0), 10), "░░░░░░░░░░");
        // Nearly done is not full: the last cell is for the finish.
        assert_eq!(
            bar_string(run_fraction(d(199), Some(200.0)), 10),
            "█████████░"
        );
        // Over the expected time the bar stays full.
        assert_eq!(
            bar_string(run_fraction(d(50), Some(30.0)), 10),
            "██████████"
        );
        // No learned wall time (or a useless one): no bar.
        assert_eq!(run_fraction(d(12), None), None);
        assert_eq!(run_fraction(d(12), Some(0.0)), None);
        assert_eq!(bar_string(None, 10), "          ");
        assert_eq!(bar_fill(Some(0.5), 0), Some(0));
    }

    fn wait_row(start_in: Option<f64>, rough: bool) -> WaitRow {
        WaitRow {
            num: 4,
            project: "p".into(),
            job: "j".into(),
            weight: 6,
            front: false,
            why: "needs 6, 0 free".into(),
            start_in,
            expected_secs: Some(60.0),
            rough,
            wt: None,
        }
    }

    #[test]
    fn a_waiting_row_has_an_empty_bar_and_says_when_it_starts() {
        let w = wait_row(Some(47.0), false);
        assert_eq!(right_text_wait(&w), "starts ~0:47 (needs 6, 0 free)");
        assert_eq!(starts_text(&w), "starts ~0:47");
        let w = wait_row(Some(47.0), true);
        assert_eq!(starts_text(&w), "starts ~0:47?");
        let w = wait_row(None, false);
        assert_eq!(starts_text(&w), "starts ?");
        let w = wait_row(Some(0.0), false);
        assert_eq!(starts_text(&w), "starts now");
        // A waiting row has an empty bar (0%).
        let v = View {
            waiting: vec![wait_row(Some(47.0), false), wait_row(None, true)],
            ..sample_view(Utc::now())
        };
        let rows = table_rows(&v);
        for r in rows.iter().rev().take(2) {
            assert_eq!(r.frac, Some(0.0));
            assert_eq!(bar_string(r.frac, 4), "░░░░");
        }
    }

    #[test]
    fn the_text_after_the_bar_says_time_left_or_when_it_starts_and_why() {
        let v = sample_view(Utc::now());
        assert_eq!(right_text_run(&v.running[0]), "50%  left 1:35");
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

    #[test]
    fn the_bar_grows_with_the_width_and_every_row_has_the_same_one() {
        let v = sample_view(Utc::now());
        let rows = table_rows(&v);
        let cols = |w: usize| layout(w - 2, table_wants(&v, &rows, 4));
        // The bar is never under 20, and grows with the width while the
        // same columns are shown (it gives way when a column comes back).
        assert!(cols(90).bar >= 20 && cols(120).bar >= 20 && cols(150).bar >= 20);
        assert!(
            cols(90).bar < cols(120).bar,
            "{:?} {:?}",
            cols(90),
            cols(120)
        );
        assert_eq!(cols(121).bar, cols(120).bar + 1);
        assert_eq!(cols(151).bar, cols(150).bar + 1);
        // At 150 every column is there (but the waiting reason).
        let c = cols(150);
        assert!(c.project >= 18 && c.worktree == 32);
        let bars = [cols(90).bar, cols(120).bar, cols(150).bar];
        let _ = bars;
    }

    #[test]
    fn project_worktree_and_job_always_show_and_current_stays_whole() {
        let v = sample_view(Utc::now());
        let rows = table_rows(&v);
        for w in [90u16, 120] {
            let out = frame(&v, w, 16);
            println!("{out}");
            let head = out.lines().find(|l| l.contains("worktree")).unwrap();
            assert!(
                head.contains("project") && head.contains("job"),
                "{w}\n{out}"
            );
            assert!(head.contains("progress"), "{w}");
            assert!(!head.contains("need") && !head.contains(" use"), "{w}");
            let c = layout(w as usize - 2, table_wants(&v, &rows, 4));
            assert!(
                c.bar >= 20 && c.project >= 8 && c.worktree >= 14 && c.job >= 8,
                "{c:?}"
            );
            // The row of the current worktree keeps ` (current)` whole.
            assert!(out.contains("main (current)"), "{w}\n{out}");
        }
        // At 120 the names are squeezed but the flags are written out.
        let c = layout(118, table_wants(&v, &rows, 4));
        assert!(!c.short_flags, "{c:?}");
        assert_eq!((c.project, c.worktree, c.job), (18, 20, 24));
        let out = frame(&v, 120, 16);
        assert!(out.contains(" idle") && out.contains("[backfill]"), "{out}");
        assert!(!out.contains(" z b"), "{out}");
        // Every width keeps ` (current)`.
        for w in [70u16, 90, 120, 130, 170] {
            assert!(frame(&v, w, 16).contains(" (current)"), "{w}");
        }
    }

    /// The cells of row `i`'s bar at terminal width `w`.
    fn bar_row(v: &View, w: u16, i: usize) -> Vec<ratatui::buffer::Cell> {
        let rows = table_rows(v);
        let num_w = format!("#{}", rows.len()).len();
        let c = layout(w as usize - 2, table_wants(v, &rows, 1 + num_w + 1));
        let x0 = (1 + 1 + num_w + 1 + c.project + 1 + c.worktree + 1 + c.job + 1) as u16;
        let t = render(v, w, 16);
        let buf = t.backend().buffer().clone();
        // The table starts on screen row 5 (header 0, cores box 1-3, box top,
        // column titles), then one row per entry.
        let y = row_y(&buf, w, 16, "progress") + 1 + i as u16;
        (x0..x0 + c.bar as u16)
            .map(|x| buf[(x, y)].clone())
            .collect()
    }

    #[test]
    fn bar_cells_are_colored_by_row_with_an_eighth_cell_and_a_gray_track() {
        let mut v = sample_view(Utc::now());
        v.mode = ColorMode::Overview;
        let rows = table_rows(&v);
        let fix = v.worktrees[0].color;
        // #1 runs in a worktree with a color: that color fills from the left.
        for w in [90u16, 120, 150] {
            let cells = bar_row(&v, w, 0);
            let want = bar_cells(rows[0].frac.unwrap(), cells.len());
            assert!(cells[..want.full]
                .iter()
                .all(|c| c.bg == fix && c.symbol() == " "));
            let mut at = want.full;
            if let Some(ch) = want.part {
                let c = &cells[at];
                assert_eq!(
                    (c.symbol(), c.fg, c.bg),
                    (ch.to_string().as_str(), fix, TRACK)
                );
                at += 1;
            }
            assert!(cells[at..]
                .iter()
                .all(|c| c.bg == TRACK && c.symbol() == " "));
            assert!(want.full > 0 && at < cells.len());
        }
        // #2 is in a lone worktree: a darker gray fill than the track.
        let cells = bar_row(&v, 120, 1);
        let want = bar_cells(rows[1].frac.unwrap(), cells.len());
        assert!(cells[..want.full].iter().all(|c| c.bg == LONE_FILL));
        // #3 has no learned time: the track only.
        assert!(bar_row(&v, 120, 2).iter().all(|c| c.bg == TRACK));
        // Waiting rows: the track only (0%), as wide as the others.
        for i in [3, 4] {
            let cells = bar_row(&v, 120, i);
            assert_eq!(cells.len(), bar_row(&v, 120, 0).len());
            assert!(cells.iter().all(|c| c.bg == TRACK && c.symbol() == " "));
        }
    }

    #[test]
    fn a_bar_is_full_at_100_percent_and_an_eighth_cell_is_a_left_block() {
        assert_eq!(
            bar_cells(1.0, 10),
            BarCells {
                full: 10,
                part: None,
                rest: 0
            }
        );
        assert_eq!(
            bar_cells(0.0, 10),
            BarCells {
                full: 0,
                part: None,
                rest: 10
            }
        );
        // 0.55 of 10 cells = 5.5 cells: five full and a half block.
        assert_eq!(
            bar_cells(0.55, 10),
            BarCells {
                full: 5,
                part: Some('▌'),
                rest: 4
            }
        );
        assert_eq!(
            bar_cells(0.02, 10),
            BarCells {
                full: 0,
                part: Some('▏'),
                rest: 9
            }
        );
        // Never full before it is done, whatever the rounding.
        let c = bar_cells(0.9999, 10);
        assert!(c.full < 10 && c.part.is_some());
        // Over the expected time the fraction is capped at 1: full.
        assert_eq!(bar_cells(3.0, 10).full, 10);
    }

    #[test]
    fn without_color_the_bar_uses_block_characters() {
        let mut v = sample_view(Utc::now());
        assert!(!v.plain);
        v.plain = true;
        let out = frame(&v, 120, 16);
        assert!(out.contains('█') && out.contains('░'), "{out}");
        let t = render(&v, 120, 16);
        let buf = t.backend().buffer().clone();
        let y = row_y(&buf, 120, 16, "50%");
        assert!((0..120u16).all(|x| buf[(x, y)].bg != TRACK));
        assert!(no_color(Some("1"), None) && no_color(None, Some("dumb")));
        assert!(
            !no_color(None, Some("xterm")) && !no_color(Some(""), None) && !no_color(None, None)
        );
    }

    #[test]
    fn columns_are_always_separated_even_when_a_name_fills_its_column() {
        let v = sample_view(Utc::now());
        let rows = table_rows(&v);
        let longest = |f: &dyn Fn(&TableRow) -> usize| rows.iter().map(f).max().unwrap();
        let (mut full_project, mut full_wt, mut full_job) = (false, false, false);
        for w in 60u16..180 {
            let c = layout(w as usize - 2, table_wants(&v, &rows, 4));
            full_project |= c.project == longest(&|r| r.project.chars().count());
            full_wt |= c.worktree == "main (current)".len();
            full_job |= c.job == table_wants(&v, &rows, 4).job.min(JOB_SQUEEZED);
            let t = render(&v, w, 16);
            let buf = t.backend().buffer().clone();
            let x_project = 1 + 4;
            let x_wt = x_project + c.project + 1;
            let x_job = x_wt + c.worktree + 1;
            let x_bar = x_job + c.job + 1;
            for i in 0..rows.len() as u16 {
                let y = row_y(&buf, w, 16, "worktree") + 1 + i;
                for x in [x_wt, x_job, x_bar] {
                    // The cell before each column's start is a space.
                    assert_eq!(buf[(x as u16 - 1, y)].symbol(), " ", "{w} row {i} x {x}");
                }
            }
        }
        // The cases above really happened: a name exactly as wide as its
        // column, for each of the three.
        assert!(
            full_project && full_wt && full_job,
            "{full_project} {full_wt} {full_job}"
        );
        assert_eq!(col("remote-task-runner", 18), "remote-task-runner ");
        assert_eq!(col("remote-task-runner", 17), "remote-task-runn… ");
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
    fn a_frame_is_one_table_with_a_progress_bar_per_row() {
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
        assert!(out.contains("progress"));
        assert!(out.contains("50%  left 1:35") && out.contains("starts ~"));
        assert!(out.contains("starts ~") && out.contains("(needs 6, 0 free)"));
        assert!(out.contains("[backfill]") && out.contains("idle"));
        // The cores are in the box on top, not in the table.
        assert!(!out.contains(" need ") && !out.contains(" use  "));
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
        let mut v = sample_view(Utc::now());
        v.mode = ColorMode::Overview;
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

    /// The foreground of every non-blank cell of the row with `needle`,
    /// inside the box.
    fn row_fgs(buf: &ratatui::buffer::Buffer, needle: &str) -> Vec<Color> {
        let y = row_y(buf, 120, 16, needle);
        (1..119u16)
            .filter(|&x| buf[(x, y)].symbol() != " ")
            .map(|x| buf[(x, y)].fg)
            .collect()
    }

    #[test]
    fn focus_colors_only_my_rows_in_every_part_and_mutes_the_rest() {
        let mut v = sample_view(Utc::now());
        assert_eq!(v.mode, ColorMode::Focus);
        let t = render(&v, 120, 16);
        let buf = t.backend().buffer().clone();
        // Mine (#5): band, number, project, worktree, job and the text after
        // the bar are all the one fixed color.
        let mine = row_fgs(&buf, "cargo clippy");
        assert!(
            mine.len() > 20 && mine.iter().all(|c| *c == FOCUS_COLOR),
            "{mine:?}"
        );
        // The others (another worktree, a lone worktree): all muted.
        for needle in ["cargo test --workspace", "local-ci run"] {
            let fgs = row_fgs(&buf, needle);
            assert!(
                fgs.len() > 20 && fgs.iter().all(|c| *c == MUTED),
                "{needle} {fgs:?}"
            );
        }
        // Their bars fill with the light gray, not a color.
        let cells = bar_row(&v, 120, 0);
        let want = bar_cells(table_rows(&v)[0].frac.unwrap(), cells.len());
        assert!(want.full > 0 && cells[..want.full].iter().all(|c| c.bg == LONE_FILL));
        let cells = bar_row(&v, 120, 1);
        assert!(cells[0].bg == LONE_FILL);
        // A waiting row is the track only.
        assert!(bar_row(&v, 120, 4).iter().all(|c| c.bg == TRACK));
        // The cores bar: #1 (not mine) is muted, never a worktree color.
        let by = row_y(&buf, 120, 16, "cores 12/12") + 1;
        assert_eq!((buf[(1, by)].symbol(), buf[(1, by)].bg), ("#", MUTED));
        // The header names the mode and the key.
        let head: String = (0..120u16)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect();
        assert!(
            head.contains("focus") && head.contains("c: colors  q: quit"),
            "{head}"
        );

        // Overview: the first row is its worktree's color, text included.
        v.mode = v.mode.toggled();
        let t = render(&v, 120, 16);
        let buf = t.backend().buffer().clone();
        let fgs = row_fgs(&buf, "cargo test --workspace");
        assert!(fgs.iter().all(|c| *c == v.worktrees[0].color), "{fgs:?}");
        let head: String = (0..120u16)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect();
        assert!(head.contains("overview"), "{head}");
    }

    #[test]
    fn only_the_overrun_is_drawn_red() {
        let now = Utc::now();
        let mut a = entry("a", State::Running, 1, 1, "/r/a", "", "a", now, 100);
        a.expected_secs = Some(60.0);
        let mut b = entry("b", State::Running, 1, 2, "/r/b", "", "b", now, 20);
        b.expected_secs = Some(60.0);
        let v = View::from_parts(&ledger(vec![a, b]), vec![], limits(4), None, now, None);
        let t = render(&v, 80, 12);
        let buf = t.backend().buffer().clone();
        // Only the text of the row that is over is red; its bar is full.
        let y0 = row_y(&buf, 80, 12, "over 0");
        let line: String = (0..80u16)
            .map(|x| buf[(x, y0)].symbol().to_string())
            .collect();
        let count = |y: u16, bg: Color| (0..80u16).filter(|&x| buf[(x, y)].bg == bg).count();
        // Past its time the bar is full (the lone worktree's darker gray)
        // with no track left; the other row still has some.
        assert!(count(y0, LONE_FILL) >= 10 && count(y0, TRACK) == 0);
        let y1 = row_y(&buf, 80, 12, "left");
        assert!(count(y1, TRACK) > 0 && count(y1, LONE_FILL) > 0);
        let x = line.find("over 0").unwrap();
        let col = line[..x].chars().count() as u16;
        assert_eq!(buf[(col, y0)].fg, Color::Red);
    }

    fn empty_view(now: DateTime<Utc>) -> View {
        View::from_parts(
            &Ledger::default(),
            vec![],
            limits(12),
            Some(live()),
            now,
            Some("/src/remote-task-runner"),
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
    }

    #[test]
    fn an_empty_queue_says_only_that_and_what_is_free() {
        let v = empty_view(Utc::now());
        let out = frame(&v, 90, 12);
        println!("{out}");
        assert!(out.contains("Nothing running or waiting"));
        assert!(out.contains("A new command starts right away · 12 cores free"));
        assert!(!out.contains("recently finished") && !out.contains("Run one with"));
        let t = render(&v, 90, 12);
        let buf = t.backend().buffer().clone();
        let y = row_y(&buf, 90, 12, "A new command");
        let x = (0..90u16).find(|&x| buf[(x, y)].symbol() == "A").unwrap();
        assert!(buf[(x, y)].modifier.contains(Modifier::DIM));
    }

    #[test]
    fn running_without_waiting_says_what_would_start_right_away() {
        let now = Utc::now();
        let mut a = entry("a", State::Running, 4, 1, "/r/a", "", "cargo test", now, 30);
        a.expected_secs = Some(100.0);
        let v = View::from_parts(&ledger(vec![a]), vec![], limits(12), None, now, None);
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
            View::from_parts(&ledger(vec![a]), vec![], limits(12), None, now, None)
        };
        assert!(frame(&full, 90, 12).contains("No one waiting · the next command waits for cores"));
        // Someone waiting: the line is not there.
        let out = frame(&sample_view(now), 90, 16);
        assert!(!out.contains("No one waiting"));
    }
}
