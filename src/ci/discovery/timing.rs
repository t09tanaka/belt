//! Convert runner milliseconds to one reference worker before partitioning.
//! Ratios use only the same test measured on both workers under one history key.
use super::{History, PartitionOpts};
use std::collections::BTreeMap;

#[derive(Default)]
pub(crate) struct TimingModel {
    pub history: History,
    pub costs: PartitionOpts,
    pub slots: Vec<String>,
}

pub(crate) fn calibrate(
    samples: &BTreeMap<String, History>,
    workers: &[String],
    capacity: usize,
    transfer_ms: u64,
) -> TimingModel {
    let mut model = TimingModel::default();
    model.costs.fixed_cost_ms = transfer_ms;
    let Some((_, reference)) = samples
        .iter()
        .max_by_key(|(name, h)| (h.millis.len(), std::cmp::Reverse(*name)))
    else {
        return model;
    };
    let mut speeds = BTreeMap::new();
    let mut weights = BTreeMap::<&String, Vec<u64>>::new();
    for (worker, history) in samples {
        let mut ratios: Vec<_> = history
            .millis
            .iter()
            .filter_map(|(id, &ms)| {
                let baseline = *reference.millis.get(id)?;
                if ms == 0 || baseline == 0 {
                    return None;
                }
                Some(
                    ((u128::from(baseline) * 1000 / u128::from(ms)).clamp(1, u128::from(u32::MAX)))
                        as u32,
                )
            })
            .collect();
        ratios.sort_unstable();
        let speed = ratios.get(ratios.len() / 2).copied().unwrap_or(1000);
        speeds.insert(worker, speed);
        for (identity, &ms) in &history.millis {
            let normalized = u128::from(ms) * u128::from(speed) / 1000;
            if normalized
                <= u128::from(u64::MAX / crate::supervisor::ci_task::MAX_PLAN_BYTES / 1000)
            {
                weights.entry(identity).or_default().push(normalized as u64);
            }
        }
    }
    for (id, mut measured) in weights {
        measured.sort_unstable();
        model.history.millis.insert(
            id.clone(),
            reference
                .millis
                .get(id)
                .copied()
                .unwrap_or(measured[measured.len() / 2]),
        );
    }
    if workers.len() > 1
        && workers
            .iter()
            .any(|w| speeds.get(w).is_some_and(|s| *s != 1000))
    {
        let speed = |w: &String| speeds.get(w).copied().unwrap_or(1000);
        model.slots = workers.iter().cycle().take(capacity).cloned().collect();
        // The SDK omits empty shards. With faster slots first, only a suffix
        // can be empty and every emitted shard index still names its worker.
        model.slots.sort_by_key(|w| std::cmp::Reverse(speed(w)));
        model.costs.speed_permille = model.slots.iter().map(speed).collect();
    }
    model
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_identical_tests_and_scales_each_observation_once() {
        let hist = |v: &[(&str, u64)]| History {
            millis: v.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        };
        let samples = BTreeMap::from([
            ("a".into(), hist(&[("same", 100), ("one", 70), ("two", 70)])),
            ("b".into(), hist(&[("same", 50), ("other", 60)])),
        ]);
        let model = calibrate(&samples, &["a".into(), "b".into()], 4, 25);
        assert_eq!(model.history.millis["other"], 120);
        assert_eq!(model.history.millis["same"], 100);
        assert_eq!(model.costs.speed_permille, [2000, 2000, 1000, 1000]);
        assert_eq!(model.slots, ["b", "b", "a", "a"]);
        assert_eq!(model.costs.fixed_cost_ms, 25);
        assert!(calibrate(&samples, &["new".into()], 2, 0).slots.is_empty());
    }
}
