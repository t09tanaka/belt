//! Linux service backend: a systemd user service driving the same worker-host
//! helper as the macOS LaunchAgent (`worker_service`). The plan, the managed
//! file store, the journal and the upgrade order are shared; only the unit
//! text and the manager's commands differ (`policy`).
//!
//! The launcher is generic over a command runner so tests never reach a real
//! `systemctl`. The native runner clears the environment and passes only what
//! `systemctl --user` needs to find the user's manager. Nothing here signals
//! a process: stopping is the manager's job, after the worker acknowledged
//! the stop.
pub mod policy;

use super::macos::policy::{Backend, Plan};
use super::macos::{LaunchError, Launcher, LoadedState, Receipt, RunningIdentity};
use super::manager_command::{run_bounded, CommandOutput, CommandRunner, OUTPUT_LIMIT};
use std::time::Duration;

const SYSTEMCTL: &str = "/usr/bin/systemctl";
const QUICK: Duration = Duration::from_secs(30);
/// Longer than the unit's own stop timeout, so systemd answers first.
const STOP: Duration = Duration::from_secs(policy::STOP_TIMEOUT_SECS as u64 + 60);

/// The real `systemctl`. The scope flag leads every argument list.
pub struct SystemctlRunner {
    env: Vec<(String, String)>,
}
impl SystemctlRunner {
    /// `uid` names the runtime directory when the caller's environment has none.
    pub fn from_environment(uid: u32) -> Self {
        let var = |k: &str| std::env::var(k).ok();
        Self {
            env: policy::systemctl_env(
                var("XDG_RUNTIME_DIR").as_deref(),
                var("DBUS_SESSION_BUS_ADDRESS").as_deref(),
                uid,
            ),
        }
    }
}
impl CommandRunner for SystemctlRunner {
    fn run(&mut self, arguments: &[String]) -> Result<CommandOutput, String> {
        let deadline = if arguments.iter().any(|a| a == "stop") {
            STOP
        } else {
            QUICK
        };
        run_bounded(SYSTEMCTL, arguments, &self.env, deadline)
    }
}

pub struct SystemdLauncher<R = SystemctlRunner> {
    runner: R,
}
impl SystemdLauncher {
    pub fn native() -> Self {
        Self::new(SystemctlRunner::from_environment(
            rustix::process::geteuid().as_raw(),
        ))
    }
}
impl<R: CommandRunner> SystemdLauncher<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }
    fn show(&mut self, plan: &Plan) -> Result<policy::Show, LaunchError> {
        let output = self
            .runner
            .run(&policy::show_arguments(plan.domain(), &plan.filename()))
            .map_err(|_| LaunchError::NotApplied("unit query failed".into()))?;
        if !output.success || !output.stderr.is_empty() || output.stdout.len() > OUTPUT_LIMIT {
            return Err(LaunchError::NotApplied("unit query unconfirmed".into()));
        }
        policy::parse_show(&output.stdout).map_err(|why| LaunchError::NotApplied(why.into()))
    }
}
impl<R: CommandRunner> Launcher for SystemdLauncher<R> {
    fn backend(&self) -> Backend {
        Backend::Systemd
    }
    fn running_identity(
        &mut self,
        plan: &Plan,
        receipt: &Receipt,
    ) -> Result<RunningIdentity, LaunchError> {
        let mut original = None;
        for _ in 0..2 {
            let show = self.show(plan)?;
            if show.classify(receipt.path()) != LoadedState::Match {
                return Err(LaunchError::NotApplied("unit is not this service".into()));
            }
            let pid = show
                .running_pid()
                .ok_or_else(|| LaunchError::NotApplied("unit has no running process".into()))?;
            let identity = RunningIdentity::observe(pid).map_err(|_| {
                LaunchError::NotApplied("running process identity unconfirmed".into())
            })?;
            if original.as_ref().is_some_and(|first| first != &identity) {
                return Err(LaunchError::NotApplied("running service changed".into()));
            }
            original = Some(identity);
        }
        let identity = original.expect("two observations");
        identity
            .verify_live()
            .map_err(|_| LaunchError::NotApplied("running service stopped or changed".into()))?;
        Ok(identity)
    }
    fn loaded_state(&mut self, plan: &Plan, receipt: &Receipt) -> LoadedState {
        match self.show(plan) {
            Ok(show) => show.classify(receipt.path()),
            Err(_) => LoadedState::Unknown,
        }
    }
    fn verify_loaded(&mut self, plan: &Plan, receipt: &Receipt) -> Result<(), LaunchError> {
        match self.show(plan)?.classify(receipt.path()) {
            LoadedState::Match => Ok(()),
            _ => Err(LaunchError::NotApplied(
                "unit is not loaded from the managed file".into(),
            )),
        }
    }
    /// A failure before the last step changed nothing that runs; the last
    /// step's failure, or a runner that gave no answer, is uncertain.
    fn launchctl(&mut self, arguments: &[String]) -> Result<(), LaunchError> {
        let steps = policy::steps(arguments).map_err(LaunchError::NotApplied)?;
        let last = steps.len() - 1;
        for (index, step) in steps.iter().enumerate() {
            let output = self.runner.run(step).map_err(LaunchError::OutcomeUnknown)?;
            if output.success
                && output.stdout.len() <= OUTPUT_LIMIT
                && output.stderr.len() <= OUTPUT_LIMIT
            {
                continue;
            }
            return Err(if index < last {
                LaunchError::NotApplied("systemctl did not confirm a preparatory step".into())
            } else {
                LaunchError::OutcomeUnknown("systemctl did not confirm success".into())
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
