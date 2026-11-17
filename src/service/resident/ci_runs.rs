//! Lets the CI runs of this parent go on: one pass of
//! [`crate::ci::advance`] per heartbeat, off the executor threads. A pass
//! holds nothing between two looks, so stopping and starting the resident
//! loses nothing of a run.
use super::*;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// How long the parent keeps a frozen plan no waiting task names: as long as
/// the results of its run, and a day at least (a task that was let into the
/// queue is delivered its plan when it is placed, which may be later).
pub(super) fn plan_retention_ms(retention_days: u32) -> u64 {
    u64::from(retention_days.max(1)).saturating_mul(DAY_MS)
}

pub(super) async fn run(
    coordinator: Arc<Coordinator>,
    state: std::path::PathBuf,
    cadence: Duration,
    retention_days: u32,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(cadence);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = stop.changed() => return Ok(()), _ = timer.tick() => {} }
        if coordinator.is_fenced() {
            continue;
        }
        let (owner, dir) = (coordinator.clone(), state.clone());
        // A pass that fails is tried again at the next heartbeat: a run that
        // cannot go on never stops the resident.
        let _ = tokio::task::spawn_blocking(move || {
            let pass =
                crate::ci::advance::advance_runs(owner.store(), &dir, &dir.join("results"), wall_ms());
            if let Ok(pass) = &pass {
                let keep_ms = plan_retention_ms(retention_days);
                crate::worker::ci_task::sweep_plans(&dir, &pass.plans, keep_ms);
            }
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plan_is_kept_for_the_retention_of_the_results_and_a_day_at_least() {
        assert_eq!(plan_retention_ms(0), DAY_MS);
        assert_eq!(plan_retention_ms(1), DAY_MS);
        assert_eq!(plan_retention_ms(14), 14 * DAY_MS);
        assert_eq!(plan_retention_ms(u32::MAX), u64::from(u32::MAX) * DAY_MS);
    }
}
