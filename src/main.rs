//! Command-line adapter of `local-ci`: parses arguments, prints, picks the exit code.
//! The one place that reads the process environment.

#![allow(clippy::disallowed_methods)]

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use local_ci::{
    plan, report, workflow, CancelHandle, ExecContext, Observer, Repo, RunOptions, Target,
};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Once;

#[derive(Parser)]
#[command(
    version,
    about = "Run the GitHub Actions jobs a branch would trigger, locally, and print a short report"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct TargetArgs {
    /// Compare against this ref (default: origin's default branch)
    #[arg(long)]
    base: Option<String>,
    /// Commit to test (default: HEAD)
    #[arg(long, default_value = "HEAD")]
    sha: String,
    /// Run every workflow a pull request can trigger, ignoring `paths` / `paths-ignore`
    /// (`branches` and `types` still apply). Default: only what these changes trigger
    #[arg(long)]
    all: bool,
    /// Also run workflows that are disabled on GitHub (`paths` still apply; `--all` implies this)
    #[arg(long)]
    include_disabled: bool,
    /// Also read workflows from this directory (relative to the repository root), repeatable,
    /// besides `.github/workflows` (or the `workflows-dirs` of `.local-ci.toml`).
    /// Their triggers are applied as for the others
    #[arg(long = "workflows-dir", value_name = "DIR")]
    workflows_dirs: Vec<String>,
    /// Only these workflows (file name or `name:`), repeatable
    #[arg(long = "workflow", short = 'w')]
    workflows: Vec<String>,
    /// Only these jobs (id), plus the jobs they need; repeatable
    #[arg(long = "job", short = 'j')]
    jobs: Vec<String>,
    /// Repository directory (default: current directory)
    #[arg(long, short = 'C')]
    dir: Option<PathBuf>,
}

impl TargetArgs {
    fn into_target(self) -> Result<Target> {
        let dir = match self.dir {
            Some(d) => d,
            None => std::env::current_dir()?,
        };
        Ok(Target {
            base: self.base,
            sha: self.sha,
            all: self.all,
            include_disabled: self.include_disabled,
            workflows_dirs: self.workflows_dirs,
            workflows: self.workflows,
            jobs: self.jobs,
            dir,
        })
    }
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    target: TargetArgs,
    /// Units to run at once (default: all of them with task-traffic-controller, else 4)
    #[arg(long)]
    parallel: Option<usize>,
    /// Do not wrap steps in `task-traffic-controller exec`
    #[arg(long)]
    no_traffic_controller: bool,
    /// Do not give jobs that start containers or test servers a port block from portool
    #[arg(long)]
    no_portool: bool,
    /// Jobs with containers or test servers that may run at once, across all local-ci runs
    #[arg(long, default_value_t = 2)]
    containers: usize,
    /// Do not record the result as the `local-ci` commit status
    #[arg(long)]
    no_status: bool,
    /// Keep the worktrees afterwards (their paths are printed)
    #[arg(long)]
    keep: bool,
    /// Lines of a failing step's log to print
    #[arg(long, default_value_t = 30)]
    tail: usize,
    /// Job timeout in minutes when the workflow sets none
    #[arg(long, default_value_t = 60.0)]
    job_timeout: f64,
    /// Print the result as JSON instead of the text report
    #[arg(long)]
    json: bool,
    /// Run every unit even when a result from the last day covers it
    /// (results of this run are still kept)
    #[arg(long)]
    no_cache: bool,
    /// Do not restore or save the `target/` of Rust jobs (LOCAL_CI_NO_BUILD_CACHE=1 does the same)
    #[arg(long)]
    no_build_cache: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show which workflows and jobs would run, without running anything
    Plan {
        #[command(flatten)]
        target: TargetArgs,
    },
    /// Run them in a throwaway worktree of the commit and report
    Run(RunArgs),
    /// Record the last run's result as the `local-ci` commit status (after pushing)
    Status {
        /// Repository directory (default: current directory)
        #[arg(long, short = 'C')]
        dir: Option<PathBuf>,
        /// Only post if the last run tested this commit (default: HEAD)
        #[arg(long, default_value = "HEAD")]
        sha: String,
    },
    /// Show what the cache directory holds, or remove what no run needs
    Cache {
        #[command(subcommand)]
        cmd: CacheCmd,
    },
}

#[derive(Subcommand)]
enum CacheCmd {
    /// The disk space each part of the cache takes, and whose the worktrees are
    Status,
    /// Remove the worktrees of runs that are gone and empty the trash
    Prune {
        /// Also remove the worktrees kept with `run --keep`
        #[arg(long)]
        kept: bool,
        /// Also remove the worktrees left by an earlier version, which recorded
        /// no owner, once no process has the pid in their name
        #[arg(long)]
        legacy: bool,
        /// Remove the build cache (the `target/` directories kept for Rust jobs)
        #[arg(long)]
        build: bool,
    },
}

/// The first signal that cancelled the run, or 0.
static CANCELLED_BY: AtomicI32 = AtomicI32::new(0);

/// Whether the process was started with `sig` ignored (`nohup`, a shell's
/// `trap '' TERM`).
fn inherited_ignored(sig: i32) -> bool {
    // SAFETY: a null new action only reads the current one into `old`.
    unsafe {
        let mut old: libc::sigaction = std::mem::zeroed();
        libc::sigaction(sig, std::ptr::null(), &mut old) == 0 && old.sa_sigaction == libc::SIG_IGN
    }
}

/// Make Ctrl-C, `SIGTERM` and `SIGHUP` cancel the run, so that it ends in
/// order and removes its worktrees. `SIGTERM` and `SIGHUP` stay ignored when
/// the process was started with them ignored; Ctrl-C is always handled, as
/// it always was.
fn cancel_on_signals(cancel: CancelHandle) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let mut handled = vec![libc::SIGINT];
        handled.extend(
            [libc::SIGTERM, libc::SIGHUP]
                .into_iter()
                .filter(|sig| !inherited_ignored(*sig)),
        );
        let Ok(mut signals) = signal_hook::iterator::Signals::new(&handled) else {
            return;
        };
        // cancelling takes locks: in a thread, not in the signal handler
        std::thread::spawn(move || {
            for sig in signals.forever() {
                let _ = CANCELLED_BY.compare_exchange(0, sig, Ordering::SeqCst, Ordering::SeqCst);
                cancel.cancel();
            }
        });
    });
}

/// End the way the signal that cancelled the run would have ended the
/// process, now that the run has cleaned up: whoever waits for local-ci
/// (a supervisor, a shell) sees that `SIGTERM` or `SIGHUP` ended it. Ctrl-C
/// keeps its exit code, 130.
fn die_of_the_signal_that_cancelled() {
    let sig = CANCELLED_BY.load(Ordering::SeqCst);
    if sig != libc::SIGTERM && sig != libc::SIGHUP {
        return;
    }
    let _ = std::io::stdout().flush();
    // SAFETY: the default action is restored for one of two fixed signals,
    // and the signal is raised in this process only (`raise`, no pid).
    unsafe {
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

/// Prints notices the way `local-ci` always has, and installs the signal handlers.
struct Stderr {
    cancel: CancelHandle,
}
impl Observer for Stderr {
    fn notice(&mut self, msg: &str) {
        eprintln!("local-ci: {msg}");
    }
    fn executing(&mut self) {
        // Ctrl-C, SIGTERM and SIGHUP stop the run this process is making
        cancel_on_signals(self.cancel.clone());
    }
}

/// The context of this process: its environment, and a cancel handle for the one run it makes.
fn context() -> ExecContext {
    ExecContext::new(
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
    )
}

fn run(ctx: &ExecContext, a: RunArgs) -> Result<i32> {
    let (json, tail, no_status) = (a.json, a.tail, a.no_status);
    let opts = RunOptions {
        target: a.target.into_target()?,
        parallel: a.parallel,
        no_traffic_controller: a.no_traffic_controller,
        no_portool: a.no_portool,
        containers: a.containers,
        keep: a.keep,
        job_timeout: a.job_timeout,
        no_cache: a.no_cache,
        no_build_cache: a.no_build_cache,
        cache_dir: ctx.cache_dir()?,
        source: None,
    };
    let rep = local_ci::run(
        ctx,
        opts,
        &mut Stderr {
            cancel: ctx.cancel.clone(),
        },
    )?;
    let result = &rep.result;
    if json {
        println!("{}", serde_json::to_string_pretty(result)?);
    } else {
        report::print(result, tail, &rep.style);
    }
    let cancelled = rep.cancelled;
    if !json {
        if no_status || cancelled {
            println!(
                "status: not posted ({})",
                if cancelled {
                    "cancelled"
                } else {
                    "--no-status"
                }
            );
        } else {
            // a run over a captured source posts only to a commit whose
            // content it tested
            let posted =
                local_ci::publish_target(ctx, &rep.repo_root, &result.sha, rep.source.as_ref())
                    .and_then(|target| {
                        local_ci::post_status(
                            ctx,
                            &rep.repo_root,
                            &target,
                            &result.state,
                            &result.description,
                        )
                        .map(|()| target)
                        .map_err(|e| format!("{e:#}"))
                    });
            match posted {
                Ok(target) => println!(
                    "status: local-ci={} posted to {}",
                    report::github_state(&result.state),
                    &target[..7]
                ),
                Err(e) => println!("status: not posted ({e})"),
            }
        }
    }
    Ok(if cancelled {
        130
    } else {
        match result.state.as_str() {
            // `no_work` (the changes trigger no workflow) keeps the exit code a
            // run with nothing to do always had
            "success" | "no_work" => 0,
            "failure" => 1,
            // checked what it could, and left out what local-ci cannot reproduce
            "incomplete" => 3,
            _ => 2,
        }
    })
}

fn status(ctx: &ExecContext, dir: Option<PathBuf>, sha: &str) -> Result<i32> {
    let dir = match dir {
        Some(d) => d,
        None => std::env::current_dir()?,
    };
    let (state, short) = local_ci::status(ctx, &dir, sha, &ctx.cache_dir()?)?;
    println!("status: local-ci={state} posted to {short}");
    Ok(0)
}

fn size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut n = bytes as f64;
    let mut unit = 0;
    while n >= 1024.0 && unit + 1 < UNITS.len() {
        n /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{bytes} B"),
        _ => format!("{n:.1} {}", UNITS[unit]),
    }
}

fn cache(ctx: &ExecContext, cmd: CacheCmd) -> Result<i32> {
    use local_ci::reclaim::{self, State};
    let dir = ctx.cache_dir()?;
    match cmd {
        CacheCmd::Status => {
            println!("cache: {}", dir.display());
            let mut parts: Vec<_> = std::fs::read_dir(&dir)
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.file_name())
                .collect();
            parts.sort();
            let mut total = 0;
            for p in parts {
                let bytes = reclaim::disk_usage(&dir.join(&p));
                total += bytes;
                println!("  {:<10} {:>10}", p.to_string_lossy(), size(bytes));
            }
            println!("  {:<10} {:>10}", "total", size(total));
            let entries = reclaim::entries(&dir)?;
            let count = |s: State| entries.iter().filter(|e| e.state == s).count();
            let build = dir.join(local_ci::build_cache::DIR);
            let es = local_ci::build_cache::entries(&build);
            println!(
                "build cache: {} entries, {} (the least recently used go above {}; LOCAL_CI_BUILD_CACHE_MAX_GB)",
                es.len(),
                size(es.iter().map(|e| e.bytes).sum()),
                size(
                    local_ci::build_cache::Settings::at(ctx, &dir).max_bytes
                )
            );
            println!(
                "worktrees: {} in use, {} kept (--keep), {} left by runs that are gone, {} from an older version, {} not local-ci's",
                count(State::InUse),
                count(State::Kept),
                count(State::Stale),
                count(State::Legacy),
                count(State::Unknown)
            );
            if count(State::Stale) > 0 {
                println!("`local-ci cache prune` removes the ones left by runs that are gone (so does the next run)");
            }
            if count(State::Legacy) > 0 {
                println!("`local-ci cache prune --legacy` removes the ones from an older version (no run does)");
            }
        }
        CacheCmd::Prune {
            kept,
            legacy,
            build,
        } => {
            let before =
                reclaim::disk_usage(&dir.join("wt")) + reclaim::disk_usage(&dir.join("trash"));
            let swept = reclaim::prune_all(ctx, &dir, kept, legacy)?;
            let after =
                reclaim::disk_usage(&dir.join("wt")) + reclaim::disk_usage(&dir.join("trash"));
            println!(
                "removed {} worktree entries, freed {}; {} in use, {} kept (--keep), {} from an older version, {} not local-ci's",
                swept.removed.len(),
                size(before.saturating_sub(after)),
                swept.in_use,
                swept.kept,
                swept.legacy,
                swept.unknown
            );
            if build {
                let s = local_ci::build_cache::Settings::at(ctx, &dir);
                let before = reclaim::disk_usage(&s.dir);
                let cleared = local_ci::build_cache::clear(ctx, &s);
                for w in &cleared.warnings {
                    eprintln!("warning: build cache: {w}");
                }
                println!(
                    "removed {} build cache entries, freed {}",
                    cleared.removed,
                    size(before)
                );
            }
        }
    }
    Ok(0)
}

fn job_note(j: &workflow::Job) -> String {
    let mut n = Vec::new();
    if j.container {
        n.push("container: unsupported".to_string());
    }
    if let Some(u) = &j.reusable {
        n.push(format!("reusable workflow {u}: unsupported"));
    }
    match &j.matrix {
        Some(serde_yaml::Value::String(_)) => n.push("matrix from expression".into()),
        Some(m) => {
            let c = matrix_size(m);
            if c != 1 {
                n.push(format!("matrix ×{c}"));
            }
        }
        None => {}
    }
    if !j.services.is_empty() {
        n.push(format!(
            "services: {}",
            j.services
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if n.is_empty() {
        String::new()
    } else {
        format!(" ({})", n.join("; "))
    }
}

fn matrix_size(m: &serde_yaml::Value) -> usize {
    let Some(map) = m.as_mapping() else { return 1 };
    let mut n = 1usize;
    let mut base = false;
    for (k, v) in map {
        let key = k.as_str().unwrap_or("");
        if key == "include" || key == "exclude" {
            continue;
        }
        if let Some(seq) = v.as_sequence() {
            n *= seq.len().max(1);
            base = true;
        }
    }
    let inc = map
        .get(serde_yaml::Value::String("include".into()))
        .and_then(|v| v.as_sequence())
        .map(|s| s.len())
        .unwrap_or(0);
    if base {
        n
    } else {
        inc.max(1)
    }
}

fn print_plan(r: &Repo, p: &plan::Plan, fp: &local_ci::FrozenPlan) {
    let dir = r.default_workflows_dir.as_str();
    println!(
        "local-ci plan  {} vs {} ({} file{} changed, {}){}",
        &r.sha[..7],
        r.base,
        r.changed.len(),
        if r.changed.len() == 1 { "" } else { "s" },
        if r.all {
            "all checks"
        } else {
            "triggered by the changes"
        },
        if r.dirty {
            "  [uncommitted changes are not tested]"
        } else {
            ""
        }
    );
    let mut recipes: Vec<local_ci::setup::SetupRecipe> = Vec::new();
    let mut failed: Vec<(String, String)> = Vec::new();
    for t in &fp.tasks {
        match local_ci::setup::derive_setup_recipe(fp, &t.id) {
            Ok(r) => recipes.push(r),
            Err(e) => failed.push((t.id.clone(), format!("{e:#}"))),
        }
    }
    for (task, why) in &failed {
        println!("setup of {task} could not be worked out: {why}");
    }
    let all = local_ci::setup::SetupRecipe::union(&recipes.iter().collect::<Vec<_>>());
    let covered = local_ci::setup::covered_by(&all);
    if !all.steps.is_empty() {
        let s: Vec<String> = all
            .steps
            .iter()
            .filter(|s| !covered.contains_key(&s.id))
            .map(|s| {
                format!(
                    "{}$ {}",
                    if s.dir.is_empty() { "." } else { &s.dir },
                    s.label
                )
            })
            .collect();
        println!("setup (once): {}", s.join(" | "));
    }
    for l in &all.left_in_place {
        println!(
            "setup stays in {}: {} ({})",
            l.job,
            l.label,
            l.reason.describe()
        );
    }
    for (i, u) in p.units.iter().enumerate() {
        let w = &p.selected[u.wf].wf;
        let jobs: Vec<String> = u
            .jobs
            .iter()
            .map(|id| format!("{id}{}", job_note(p.job(u.wf, id))))
            .collect();
        println!(
            "unit {:<2} {}: {}{}",
            i + 1,
            report::short(dir, &w.file),
            jobs.join(" → "),
            if u.exclusive { "  [exclusive]" } else { "" }
        );
    }
    if !p.skipped.is_empty() {
        let skipped: Vec<report::Skipped> = p
            .skipped
            .iter()
            .map(|(w, r)| report::Skipped {
                workflow: w.clone(),
                reason: r.clone(),
            })
            .collect();
        println!("not triggered ({})", report::group_skipped(&skipped, dir));
    }
}

fn main() {
    let cli = Cli::parse();
    let ctx = context();
    let res = match cli.cmd {
        Cmd::Plan { target } => target.into_target().and_then(|t| {
            let (r, p) = local_ci::resolve(
                &ctx,
                &t,
                &mut Stderr {
                    cancel: ctx.cancel.clone(),
                },
            )?;
            let fp = local_ci::freeze(&ctx, &r, p.clone(), &t)?;
            print_plan(&r, &p, &fp);
            Ok(0)
        }),
        Cmd::Run(a) => run(&ctx, a),
        Cmd::Status { dir, sha } => status(&ctx, dir, &sha),
        Cmd::Cache { cmd } => cache(&ctx, cmd),
    };
    let code = res.unwrap_or_else(|e| {
        eprintln!("local-ci: {e:#}");
        64
    });
    die_of_the_signal_that_cancelled();
    std::process::exit(code);
}
