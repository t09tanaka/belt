//! What a preview (`estimate`) shares with a placement, and how a preview is
//! read by a person. No clock, no store, no worker: values in, values out.
//!
//! A placement and a preview compare workers by the same [`Costs`]: the
//! parent's own measurements first, then what the configured cost model says
//! for the parts nobody measured ([`merge_costs`]). The text shows, for every
//! worker, which kind of number each part is: a measured time, a figure from
//! the cost model, a fallback that only orders workers, or unknown. A fallback
//! is never printed as a time.

use super::decide::{word, Compared, Costs, Decision, Excluded, Label};
use super::decide::{COLLECTION, SETUP, TRANSFER};
use super::measured::{ComponentEvidence, EvidenceKind, WorkerCostEvidence};

/// The costs a worker is compared by: every part the parent measured, and
/// the cost model's answer for the parts it did not.
pub fn merge_costs(measured: Costs, model: Costs) -> Costs {
    let mut costs = measured;
    for part in [TRANSFER, SETUP, COLLECTION] {
        if costs.ms[part].is_none() {
            costs.ms[part] = model.ms[part];
            costs.assumed[part] = model.assumed[part];
        }
    }
    costs.transfer_bytes = costs.transfer_bytes.or(model.transfer_bytes);
    costs.cache_warm = costs.cache_warm.or(model.cache_warm);
    costs.affinity |= model.affinity;
    costs
}

/// How a worker stands for a task that waits in the queue, by the input of
/// its run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Gate {
    /// It is asked for a forecast: the source is ready there, or the parent
    /// measured what sending it there takes.
    Asked,
    /// It did not take the input of the run; left out by name, with why.
    InputRefused(String),
    /// It does not hold the source and nothing is measured: not asked, and
    /// not named in the decision.
    NotReady,
}

/// The gate a placement puts before the forecast of a queued task.
/// `refusal` is the reason of a refusal that still holds (none for a worker
/// where the input is ready).
pub fn source_gate(ready: bool, refusal: Option<String>, measured_cold: bool) -> Gate {
    match refusal {
        Some(why) => Gate::InputRefused(why),
        None if ready || measured_cold => Gate::Asked,
        None => Gate::NotReady,
    }
}

/// A placement makes no decision for a queued task while workers are
/// attached and none of them holds the source or refused the input:
/// `(ready, refused the input)` of every worker the tick may place on.
pub fn waits_for_input(open: &[(bool, bool)]) -> bool {
    !open.is_empty() && !open.iter().any(|(ready, refused)| *ready || *refused)
}

/// Whether a forecast makes its worker a candidate. A worker that holds the
/// source is compared whatever it said (without a number, as unknown); one
/// that does not is compared only by an answer that is usable and not yet
/// expired.
pub fn forecast_counts(ready: bool, unusable: bool, ttl_left_ms: u64) -> bool {
    ready || (!unusable && ttl_left_ms > 0)
}

fn time(ms: Option<u64>) -> String {
    match ms {
        Some(ms) => format!("{ms} ms"),
        None => "unknown".to_string(),
    }
}

/// One of transfer, setup and collection, as the comparison used it.
fn part(costs: &Costs, index: usize, evidence: Option<&ComponentEvidence>) -> String {
    let kind = evidence.map(|e| &e.kind);
    match (costs.ms[index], costs.assumed[index]) {
        (Some(rank), true) => {
            format!("rank {rank} fallback (worst known candidate; orders workers, not a time)")
        }
        (Some(ms), false) => match (kind, evidence) {
            (Some(EvidenceKind::Measured), Some(e)) => format!(
                "{ms} ms measured ({} samples; valid for {})",
                e.sample_count
                    .map_or_else(|| "unknown".to_string(), |n| n.to_string()),
                time(e.remaining_ttl_ms)
            ),
            (Some(EvidenceKind::VerifiedReady), _) => format!("{ms} ms verified ready"),
            _ => format!("{ms} ms from the cost model"),
        },
        (None, _) => match evidence.and_then(|e| e.reason.as_deref()) {
            Some(reason) => format!("unknown ({reason})"),
            None => "unknown".to_string(),
        },
    }
}

fn compared(worker: &Compared, evidence: Option<&WorkerCostEvidence>) -> String {
    let head = match (worker.label, worker.predicted_ms) {
        (Label::Measured, Some(ms)) => format!("predicted completion in {ms} ms (measured)"),
        (Label::Fallback, Some(rank)) => {
            format!("rank {rank} (fallback; orders workers, not a time)")
        }
        _ => "unknown (not counted as free)".to_string(),
    };
    let unusable = match worker.outlook.unusable {
        Some(why) => format!("; forecast not used: {}", word(&why)),
        None => String::new(),
    };
    let mut text = format!(
        "{}: {head}{unusable}; priority {} (+{} ms); load {}; queue {}; execution {}; conditional={}\n",
        worker.worker_id,
        worker.priority,
        worker.penalty_ms,
        worker.load,
        time(worker.outlook.wait_ms),
        time(worker.outlook.execution_ms),
        worker.outlook.conditional
    );
    let costs = evidence.map(|e| &e.costs);
    for (name, index, evidence) in [
        ("transfer", TRANSFER, costs.map(|c| &c.transfer)),
        ("setup", SETUP, costs.map(|c| &c.setup)),
        ("collection", COLLECTION, costs.map(|c| &c.collection)),
    ] {
        text.push_str(&format!(
            "  {name}: {}\n",
            part(&worker.costs, index, evidence)
        ));
    }
    text
}

fn excluded(worker: &Excluded) -> String {
    let unmet = if worker.unmet.is_empty() {
        String::new()
    } else {
        format!("; unmet: {}", worker.unmet.join(", "))
    };
    format!(
        "{}: excluded: {}{unmet}\n",
        worker.worker_id,
        word(&worker.why)
    )
}

/// The preview as text: the chosen worker and the rule that chose it, then
/// every compared worker with the numbers behind it, the workers left out
/// with why, and the assumptions the preview stands on.
pub fn human(
    online: bool,
    decision: Option<&Decision>,
    components: &[WorkerCostEvidence],
    assumptions: &[String],
) -> String {
    let state = if online { "online" } else { "offline; unknown" };
    let mut text = format!("estimate: {state} (conditional)\n");
    if let Some(decision) = decision {
        text.push_str(&format!("{}\n", decision.summary()));
        text.push_str(&format!(
            "rules: one step of priority is worth {} ms; within {} ms is about the same; a forecast is used for {} ms\n",
            decision.rules.priority_step_ms,
            decision.rules.tie_window_ms,
            decision.rules.forecast_ttl_ms
        ));
        for worker in &decision.compared {
            let evidence = components.iter().find(|c| c.worker_id == worker.worker_id);
            text.push_str(&compared(worker, evidence));
        }
        for worker in &decision.excluded {
            text.push_str(&excluded(worker));
        }
    }
    for note in assumptions {
        text.push_str(&format!("{note}\n"));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::WorkerId;
    use crate::scheduler::decide::{decide, Candidate, Exclusion, Outlook, Rules, Unusable};
    use crate::scheduler::measured::CostEvidence;
    use crate::wire::RuntimeSource;

    fn id(name: &str) -> WorkerId {
        WorkerId::parse(name).unwrap()
    }

    fn rules() -> Rules {
        Rules {
            priority_step_ms: 60_000,
            tie_window_ms: 1000,
            forecast_ttl_ms: 2000,
        }
    }

    fn outlook(finish: Option<u64>, ranking: Option<u64>, source: RuntimeSource) -> Outlook {
        Outlook {
            finish_ms: finish,
            ranking_ms: ranking,
            ready_ms: Some(0),
            wait_ms: finish.map(|ms| ms / 2),
            execution_ms: finish.or(ranking),
            source,
            conditional: finish.is_none(),
            null_reason: None,
            unusable: None,
        }
    }

    fn candidate(name: &str, priority: i64, costs: Costs, outlook: Outlook) -> Candidate {
        Candidate {
            worker_id: id(name),
            priority,
            load: 3,
            costs,
            outlook,
        }
    }

    fn evidence(kind: EvidenceKind, reason: Option<&str>) -> ComponentEvidence {
        ComponentEvidence {
            kind,
            ms: None,
            sample_count: None,
            observed_monotonic_ms: None,
            remaining_ttl_ms: None,
            provenance: None,
            reason: reason.map(str::to_string),
        }
    }

    #[test]
    fn a_measured_part_is_kept_and_an_unmeasured_one_is_what_the_model_says() {
        let measured = Costs {
            ms: [None, Some(5), None],
            assumed: [false, false, false],
            transfer_bytes: None,
            cache_warm: None,
            affinity: false,
        };
        let model = Costs {
            ms: [Some(1), Some(2), None],
            assumed: [true, true, true],
            transfer_bytes: Some(7),
            cache_warm: Some(false),
            affinity: true,
        };
        assert_eq!(
            merge_costs(measured, model),
            Costs {
                ms: [Some(1), Some(5), None],
                assumed: [true, false, true],
                transfer_bytes: Some(7),
                cache_warm: Some(false),
                affinity: true,
            }
        );
        // The last part is filled like the first two.
        let last = Costs {
            ms: [None, None, Some(9)],
            ..Costs::default()
        };
        let got = merge_costs(Costs::default(), last);
        assert_eq!(got.ms, [None, None, Some(9)]);
        let got = merge_costs(last, model);
        assert_eq!(got.ms, [Some(1), Some(2), Some(9)]);
        assert_eq!(got.assumed, [true, true, false]);
    }

    #[test]
    fn what_the_parent_measured_wins_over_the_model_for_bytes_cache_and_affinity() {
        let known = Costs {
            transfer_bytes: Some(3),
            cache_warm: Some(true),
            affinity: true,
            ..Costs::default()
        };
        let model = Costs {
            transfer_bytes: Some(7),
            cache_warm: Some(false),
            affinity: true,
            ..Costs::default()
        };
        let got = merge_costs(known, model);
        assert_eq!(
            (got.transfer_bytes, got.cache_warm, got.affinity),
            (Some(3), Some(true), true)
        );
        let got = merge_costs(known, Costs::default());
        assert_eq!(
            (got.transfer_bytes, got.cache_warm, got.affinity),
            (Some(3), Some(true), true)
        );
        assert_eq!(
            merge_costs(Costs::default(), Costs::default()),
            Costs::default()
        );
    }

    #[test]
    fn a_refusal_names_the_worker_and_only_a_ready_or_measured_worker_is_asked() {
        let why = || Some("no room".to_string());
        assert_eq!(
            source_gate(false, why(), false),
            Gate::InputRefused("no room".to_string())
        );
        assert_eq!(
            source_gate(false, why(), true),
            Gate::InputRefused("no room".to_string())
        );
        assert_eq!(source_gate(true, None, false), Gate::Asked);
        assert_eq!(source_gate(false, None, true), Gate::Asked);
        assert_eq!(source_gate(true, None, true), Gate::Asked);
        assert_eq!(source_gate(false, None, false), Gate::NotReady);
    }

    #[test]
    fn no_decision_is_made_only_while_workers_are_there_and_none_holds_or_refused_the_input() {
        assert!(!waits_for_input(&[]));
        assert!(waits_for_input(&[(false, false)]));
        assert!(waits_for_input(&[(false, false), (false, false)]));
        assert!(!waits_for_input(&[(false, false), (true, false)]));
        assert!(!waits_for_input(&[(false, true), (false, false)]));
        assert!(!waits_for_input(&[(true, true)]));
    }

    #[test]
    fn a_worker_without_the_source_counts_only_by_a_usable_answer_that_has_not_expired() {
        assert!(forecast_counts(true, true, 0));
        assert!(forecast_counts(true, false, 0));
        assert!(forecast_counts(false, false, 1));
        assert!(!forecast_counts(false, false, 0));
        assert!(!forecast_counts(false, true, 1));
        assert!(!forecast_counts(false, true, 0));
    }

    #[test]
    fn an_offline_preview_says_so_and_lists_its_assumptions() {
        assert_eq!(
            human(false, None, &[], &["owner unavailable".to_string()]),
            "estimate: offline; unknown (conditional)\nowner unavailable\n"
        );
        assert_eq!(
            human(true, None, &[], &[]),
            "estimate: online (conditional)\n"
        );
    }

    #[test]
    fn every_compared_worker_shows_the_kind_of_each_number_and_the_excluded_show_why() {
        let fast = Costs {
            ms: [Some(1000), Some(0), Some(500)],
            ..Costs::default()
        };
        // Nothing known for transfer and setup; the worst known is assumed.
        let assumed = Costs {
            ms: [Some(1000), None, Some(500)],
            assumed: [true, false, true],
            ..Costs::default()
        };
        let decision = decide(
            vec![
                candidate(
                    "w-a",
                    2,
                    fast,
                    outlook(Some(4000), None, RuntimeSource::Measured),
                ),
                candidate(
                    "w-b",
                    0,
                    assumed,
                    outlook(None, Some(9000), RuntimeSource::Fallback),
                ),
            ],
            vec![
                Excluded {
                    worker_id: id("w-c"),
                    why: Exclusion::CapabilityMismatch,
                    unmet: vec!["node 22".to_string(), "profile ci".to_string()],
                },
                Excluded {
                    worker_id: id("w-d"),
                    why: Exclusion::Full,
                    unmet: vec![],
                },
            ],
            rules(),
        );
        let mut measured = evidence(EvidenceKind::Measured, None);
        measured.sample_count = Some(4);
        measured.remaining_ttl_ms = Some(86_000);
        let components = vec![
            WorkerCostEvidence {
                worker_id: id("w-a"),
                costs: CostEvidence {
                    transfer: measured,
                    setup: evidence(EvidenceKind::VerifiedReady, None),
                    collection: evidence(EvidenceKind::Measured, None),
                },
            },
            WorkerCostEvidence {
                worker_id: id("w-b"),
                costs: CostEvidence {
                    transfer: evidence(EvidenceKind::Fallback, Some("worst known")),
                    setup: evidence(EvidenceKind::Unknown, Some("no setup model")),
                    collection: evidence(EvidenceKind::Fallback, None),
                },
            },
        ];
        let text = human(true, Some(&decision), &components, &["a note".to_string()]);
        assert_eq!(
            text,
            "estimate: online (conditional)\n\
             w-a: earliest predicted completion; compared: [w-a=4500ms measured, w-b=rank 129500 fallback]; excluded: [w-c capability_mismatch, w-d full]\n\
             rules: one step of priority is worth 60000 ms; within 1000 ms is about the same; a forecast is used for 2000 ms\n\
             w-a: predicted completion in 4500 ms (measured); priority 2 (+0 ms); load 3; queue 2000 ms; execution 4000 ms; conditional=false\n\
             \x20 transfer: 1000 ms measured (4 samples; valid for 86000 ms)\n\
             \x20 setup: 0 ms verified ready\n\
             \x20 collection: 500 ms measured (unknown samples; valid for unknown)\n\
             w-b: rank 9500 (fallback; orders workers, not a time); priority 0 (+120000 ms); load 3; queue unknown; execution 9000 ms; conditional=true\n\
             \x20 transfer: rank 1000 fallback (worst known candidate; orders workers, not a time)\n\
             \x20 setup: unknown (no setup model)\n\
             \x20 collection: rank 500 fallback (worst known candidate; orders workers, not a time)\n\
             w-c: excluded: capability_mismatch; unmet: node 22, profile ci\n\
             w-d: excluded: full\n\
             a note\n"
        );
    }

    #[test]
    fn a_worker_without_evidence_or_a_number_is_shown_as_unknown_never_as_free() {
        let model = Costs {
            ms: [Some(700), None, None],
            ..Costs::default()
        };
        let mut refused = outlook(None, None, RuntimeSource::Fallback);
        refused.wait_ms = None;
        refused.execution_ms = None;
        refused.unusable = Some(Unusable::Refused);
        let decision = decide(vec![candidate("w-a", 0, model, refused)], vec![], rules());
        // The evidence of another worker is not this worker's.
        let other = vec![WorkerCostEvidence {
            worker_id: id("w-z"),
            costs: CostEvidence {
                transfer: evidence(EvidenceKind::Measured, None),
                setup: evidence(EvidenceKind::Unknown, Some("of another worker")),
                collection: evidence(EvidenceKind::Unknown, None),
            },
        }];
        assert_eq!(
            human(true, Some(&decision), &other, &[]),
            "estimate: online (conditional)\n\
             w-a: the only candidate; compared: [w-a=unknown (not counted as free)]; excluded: []\n\
             rules: one step of priority is worth 60000 ms; within 1000 ms is about the same; a forecast is used for 2000 ms\n\
             w-a: unknown (not counted as free); forecast not used: refused; priority 0 (+0 ms); load 3; queue unknown; execution unknown; conditional=true\n\
             \x20 transfer: 700 ms from the cost model\n\
             \x20 setup: unknown\n\
             \x20 collection: unknown\n"
        );
        // Evidence that is unknown without a reason, and a known figure the
        // evidence does not call measured.
        let own = vec![WorkerCostEvidence {
            worker_id: id("w-a"),
            costs: CostEvidence {
                transfer: evidence(EvidenceKind::Unknown, Some("not measured here")),
                setup: evidence(EvidenceKind::Unknown, None),
                collection: evidence(EvidenceKind::Unknown, Some("no result size")),
            },
        }];
        let text = human(true, Some(&decision), &own, &[]);
        assert!(
            text.ends_with(
                "  transfer: 700 ms from the cost model\n  setup: unknown\n  collection: unknown (no result size)\n"
            ),
            "{text}"
        );
    }
}
