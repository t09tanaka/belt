//! `task-traffic-controller exec`: CLI adapter over [`task_traffic_controller::exec`]. Turns how a run
//! ended into an exit code, re-raising signals as the shell expects.

use crate::signals::reraise;
use std::io::IsTerminal;
use task_traffic_controller::config::Config;
use task_traffic_controller::error::{Error, Result};
use task_traffic_controller::exec::{self, Gated, Hooks, NestedMode, Nesting, Opts};
use task_traffic_controller::runner::{self, Exit};
use task_traffic_controller::Context;

/// Overrides how a nested `exec` is treated (`admit` or `inherit`) for
/// everything started with it in the environment; `--nested` wins.
pub const NESTED_ENV: &str = "TASK_TRAFFIC_CONTROLLER_NESTED";

/// `--nested`, else [`NESTED_ENV`], else the default.
pub fn nested_mode(flag: Option<&str>, env: Option<&str>) -> Result<NestedMode> {
    let (Some(text), from) = (match flag {
        Some(f) => (Some(f), "--nested"),
        None => (env, NESTED_ENV),
    }) else {
        return Ok(NestedMode::default());
    };
    NestedMode::parse(text)
        .ok_or_else(|| Error::Spawn(format!("{from} is `admit` or `inherit`, not {text:?}")))
}

pub fn run(ctx: &Context, cfg: &Config, mut opts: Opts, nested: Option<&str>) -> Result<i32> {
    if opts.argv.is_empty() {
        return Err(Error::Spawn(
            "usage: task-traffic-controller exec [--weight N] [--front] [--key K] [--nested admit|inherit] -- <command> [args...]".into(),
        ));
    }
    let var = |name: &str| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned());
    let mode = nested_mode(nested, var(NESTED_ENV).as_deref())?;
    match exec::nesting(
        ctx,
        mode,
        var("TASK_TRAFFIC_CONTROLLER_LEASE").as_deref(),
        var(task_traffic_controller::marker::ENV_VAR).as_deref(),
        var(task_traffic_controller::marker::SCOPE_VAR).as_deref(),
    )? {
        Nesting::Inherit(lease) => {
            let out = runner::run(ctx, &opts.argv, &opts.cwd, &lease, false)?;
            return Ok(finish(out.exit));
        }
        Nesting::Child(parent) => opts.parent = Some(parent),
        Nesting::Top => {}
    }
    sweep_if_due(ctx, cfg);
    let id = exec::new_id();
    match run_gated(ctx, cfg, &opts, &id, &|| false)? {
        Gated::Finished(exit) => Ok(finish(exit)),
        Gated::Interrupted(sig) => reraise(sig),
    }
}

/// [`exec::run_gated`] with progress lines on stderr.
pub fn run_gated(
    ctx: &Context,
    cfg: &Config,
    opts: &Opts,
    id: &str,
    cancelled: &dyn Fn() -> bool,
) -> Result<Gated> {
    exec::run_gated(
        ctx,
        cfg,
        opts,
        id,
        &Hooks {
            cancelled,
            progress: &|line| eprintln!("{line}"),
            progress_tty: std::io::stderr().is_terminal(),
        },
    )
}

pub fn finish(exit: Exit) -> i32 {
    match exit {
        Exit::Code(c) => {
            crate::signals::restore();
            c
        }
        Exit::Signal(s) => reraise(s),
    }
}

/// The retention sweep, when it is due ([`task_traffic_controller::retention`]).
/// Run here as well as inside the library so a failure is reported, on
/// stderr, by the one run that did the sweep; the library's own call then
/// finds it fresh.
pub fn sweep_if_due(ctx: &Context, cfg: &Config) {
    if let Some(report) = task_traffic_controller::retention::sweep_if_due(ctx, cfg) {
        for e in &report.errors {
            eprintln!("task-traffic-controller: cleanup: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_wins_over_the_environment_and_admit_is_the_default() {
        assert_eq!(nested_mode(None, None).unwrap(), NestedMode::Admit);
        assert_eq!(
            nested_mode(None, Some("inherit")).unwrap(),
            NestedMode::Inherit
        );
        assert_eq!(
            nested_mode(Some("admit"), Some("inherit")).unwrap(),
            NestedMode::Admit
        );
        assert_eq!(
            nested_mode(Some("inherit"), None).unwrap(),
            NestedMode::Inherit
        );
        let err = nested_mode(None, Some("off")).unwrap_err().to_string();
        assert!(err.contains(NESTED_ENV) && err.contains("off"), "{err}");
        let err = nested_mode(Some("no"), Some("admit"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("--nested"), "{err}");
    }
}
