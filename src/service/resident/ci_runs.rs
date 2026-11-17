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

/// The runs a pass could not look at, as news: a fault is told once, when it
/// starts or its reason changes. `known` keeps what was told while it lasts
/// (a run that went on is forgotten, so the same fault later is news again).
pub(super) fn stalled_news(
    known: &mut std::collections::BTreeMap<RunId, String>,
    faults: &[(RunId, String)],
) -> Vec<(RunId, String)> {
    known.retain(|run, _| faults.iter().any(|(faulty, _)| faulty == run));
    faults
        .iter()
        .filter(|(run, why)| known.insert(run.clone(), why.clone()).as_ref() != Some(why))
        .cloned()
        .collect()
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
    let mut stalled = std::collections::BTreeMap::new();
    loop {
        tokio::select! { _ = stop.changed() => return Ok(()), _ = timer.tick() => {} }
        if coordinator.is_fenced() {
            continue;
        }
        let (owner, dir) = (coordinator.clone(), state.clone());
        // A pass that fails is tried again at the next heartbeat: a run that
        // cannot go on never stops the resident. A run that can never go on
        // was ended by the pass itself; one that only could not be looked at
        // now is tried again, and the run is told why once (`ci_run_stalled`).
        let known = std::mem::take(&mut stalled);
        let kept = tokio::task::spawn_blocking(move || {
            let mut known = known;
            let pass = crate::ci::advance::advance_runs(
                owner.store(),
                &dir,
                &dir.join("results"),
                wall_ms(),
            );
            if let Ok(pass) = &pass {
                let keep_ms = plan_retention_ms(retention_days);
                crate::worker::ci_task::sweep_plans(&dir, &pass.plans, keep_ms);
                for (run, why) in stalled_news(&mut known, &pass.faults) {
                    let payload = serde_json::json!({ "reason": why });
                    let told = owner.store().append_event(
                        Some(&run),
                        None,
                        "ci_run_stalled",
                        &payload,
                        wall_ms(),
                    );
                    if told.is_err() {
                        // Not told: it is news again at the next pass.
                        known.remove(&run);
                    }
                }
            }
            known
        })
        .await;
        stalled = kept.unwrap_or_default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_that_could_not_be_looked_at_is_told_once_while_the_fault_lasts() {
        let (a, b) = (RunId::generate(), RunId::generate());
        let fault = |run: &RunId, why: &str| (run.clone(), why.to_string());
        let mut known = std::collections::BTreeMap::new();
        assert!(stalled_news(&mut known, &[]).is_empty());
        let both = [fault(&a, "busy"), fault(&b, "busy")];
        assert_eq!(stalled_news(&mut known, &both), both);
        assert!(stalled_news(&mut known, &both).is_empty());
        // Another reason is news; the one that did not change is not.
        let changed = [fault(&a, "io"), fault(&b, "busy")];
        assert_eq!(stalled_news(&mut known, &changed), [fault(&a, "io")]);
        // A run that went on is forgotten: the same fault later is news.
        assert!(stalled_news(&mut known, &[fault(&b, "busy")]).is_empty());
        assert_eq!(known.len(), 1);
        assert_eq!(stalled_news(&mut known, &changed), [fault(&a, "io")]);
        assert!(stalled_news(&mut known, &[]).is_empty());
        assert!(known.is_empty());
    }

    #[test]
    fn a_plan_that_was_not_delivered_is_a_refusal_only_when_the_worker_said_so() {
        use crate::ci::advance::Fault;
        use crate::error::{ApiError, ErrorCode};
        use crate::protocol::transfer::TransferError;
        use crate::protocol::CallError;
        use crate::service::forward::{plan_read_fault, plan_transfer_fault, PlanFault};
        let full = ApiError::new(ErrorCode::Overloaded, "the worker holds 256 frozen plans");
        assert_eq!(
            plan_transfer_fault(&TransferError::Call(CallError::Remote(full))),
            PlanFault::Refused(
                "frozen plan not taken: the worker holds 256 frozen plans (OVERLOADED)".into()
            )
        );
        for other in [
            TransferError::Call(CallError::NotSent),
            TransferError::Stalled,
            TransferError::Unexpected("answer"),
        ] {
            let words = other.to_string();
            assert_eq!(plan_transfer_fault(&other), PlanFault::Retry(words));
        }
        assert_eq!(
            plan_read_fault(Fault::Unrunnable("gone".into())),
            PlanFault::Unrunnable("gone".into())
        );
        assert_eq!(
            plan_read_fault(Fault::Transient("busy".into())),
            PlanFault::Retry("busy".into())
        );
        let worker = WorkerId::parse("w-a").unwrap();
        assert_eq!(
            super::super::delivery::plan_refused_event(&worker, "full"),
            serde_json::json!({ "worker": "w-a", "reason": "full" })
        );
    }

    #[test]
    fn a_plan_is_kept_for_the_retention_of_the_results_and_a_day_at_least() {
        assert_eq!(plan_retention_ms(0), DAY_MS);
        assert_eq!(plan_retention_ms(1), DAY_MS);
        assert_eq!(plan_retention_ms(14), 14 * DAY_MS);
        assert_eq!(plan_retention_ms(u32::MAX), u64::from(u32::MAX) * DAY_MS);
    }
}
