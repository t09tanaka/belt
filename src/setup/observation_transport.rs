//! Bounded immutable preparation telemetry. Missing/unavailable data is Unknown.
use super::observation::PreparationObservation;
use crate::ids::BuilderId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

pub const FEATURE: &str = "preparation_observations_v1";
pub const PAGE_LIMIT: usize = 64;
const CAPACITY: usize = 4096;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationPageRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ObservationCursor>,
}
/// A completed pass starts the next pass at its frozen upper watermark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationCursor {
    pub owner_generation: BuilderId,
    pub native_boot: String,
    pub after_sequence: u64,
    pub upper_sequence: u64,
    pub monotonic_now_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SequencedObservation {
    pub sequence: u64,
    pub observation: PreparationObservation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    InvalidCursor,
    OwnerChanged,
    NativeBootChanged,
    ClockAnomaly,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObservationPage {
    Available {
        owner_generation: BuilderId,
        native_boot: String,
        monotonic_now_ms: u64,
        entries: Vec<SequencedObservation>,
        cursor: ObservationCursor,
        pass_complete: bool,
        /// The owner dropped new samples at its cap; their values remain Unknown.
        overflow: bool,
    },
    Unavailable {
        reason: UnavailableReason,
    },
}
#[derive(Default)]
pub(crate) struct ObservationLog {
    values: BTreeMap<u64, PreparationObservation>,
    builders: HashSet<BuilderId>,
    last_sequence: u64,
    overflow: bool,
}
impl ObservationLog {
    pub(crate) fn insert(&mut self, value: PreparationObservation) {
        if self.builders.contains(&value.binding.builder_id) {
            return;
        }
        let Some(sequence) = self.last_sequence.checked_add(1) else {
            self.overflow = true;
            return;
        };
        if self.values.len() == CAPACITY {
            self.overflow = true;
            return;
        }
        self.builders.insert(value.binding.builder_id.clone());
        self.values.insert(sequence, value);
        self.last_sequence = sequence;
    }
    pub(crate) fn snapshot(&self, limit: usize) -> Vec<PreparationObservation> {
        self.values
            .values()
            .take(limit.min(PAGE_LIMIT))
            .cloned()
            .collect()
    }
    pub(crate) fn page(
        &self,
        owner: &BuilderId,
        request: &ObservationPageRequest,
        boot: Option<&str>,
        now: u64,
    ) -> ObservationPage {
        use UnavailableReason::*;
        let unavailable = |reason| ObservationPage::Unavailable { reason };
        let Some(boot) = boot.filter(|v| !v.is_empty() && v.len() <= 128) else {
            return unavailable(ClockAnomaly);
        };
        if now == u64::MAX {
            return unavailable(ClockAnomaly);
        }
        let (after, upper) = if let Some(cursor) = &request.cursor {
            if cursor.owner_generation != *owner {
                return unavailable(OwnerChanged);
            }
            if cursor.native_boot != boot {
                return unavailable(NativeBootChanged);
            }
            if cursor.after_sequence > cursor.upper_sequence
                || cursor.upper_sequence > self.last_sequence
            {
                return unavailable(InvalidCursor);
            }
            if now < cursor.monotonic_now_ms {
                return unavailable(ClockAnomaly);
            }
            (
                cursor.after_sequence,
                if cursor.after_sequence == cursor.upper_sequence {
                    self.last_sequence
                } else {
                    cursor.upper_sequence
                },
            )
        } else {
            (0, self.last_sequence)
        };
        let entries: Vec<_> = self
            .values
            .range((
                std::ops::Bound::Excluded(after),
                std::ops::Bound::Included(upper),
            ))
            .take(PAGE_LIMIT)
            .map(|(sequence, observation)| SequencedObservation {
                sequence: *sequence,
                observation: observation.clone(),
            })
            .collect();
        if entries.iter().any(|entry| {
            let value = &entry.observation;
            value.binding.owner_generation != *owner
                || value.boot != boot
                || value.finished_monotonic_ms > now
                || value.duration_ms > value.finished_monotonic_ms
        }) {
            return unavailable(ClockAnomaly);
        }
        let after = entries.last().map_or(upper, |v| v.sequence);
        ObservationPage::Available {
            owner_generation: owner.clone(),
            native_boot: boot.into(),
            monotonic_now_ms: now,
            entries,
            cursor: ObservationCursor {
                owner_generation: owner.clone(),
                native_boot: boot.into(),
                after_sequence: after,
                upper_sequence: upper,
                monotonic_now_ms: now,
            },
            pass_complete: after == upper,
            overflow: self.overflow,
        }
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }
    #[cfg(test)]
    pub(crate) fn contains_key(&self, id: &BuilderId) -> bool {
        self.builders.contains(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AttemptId, Digest};
    use crate::setup::observation::{PreparationBinding, PreparationKind, POLICY};
    fn sample(owner: &BuilderId) -> PreparationObservation {
        PreparationObservation {
            binding: PreparationBinding {
                builder_id: BuilderId::generate(),
                attempt_id: AttemptId::generate(),
                owner_generation: owner.clone(),
                kind: PreparationKind::ToolInstall,
                key: Digest::of_bytes(b"private recipe"),
            },
            policy: POLICY.into(),
            nodes_digest: Digest::of_bytes(b"private commands"),
            node_count: 1,
            boot: "native-boot".into(),
            finished_monotonic_ms: 100,
            duration_ms: 10,
        }
    }
    fn read(
        log: &ObservationLog,
        owner: &BuilderId,
        cursor: Option<ObservationCursor>,
        now: u64,
    ) -> ObservationPage {
        log.page(
            owner,
            &ObservationPageRequest { cursor },
            Some("native-boot"),
            now,
        )
    }
    type Batch = (Vec<SequencedObservation>, ObservationCursor, bool, bool);
    fn available(page: ObservationPage) -> Result<Batch, UnavailableReason> {
        match page {
            ObservationPage::Available {
                entries,
                cursor,
                pass_complete,
                overflow,
                ..
            } => Ok((entries, cursor, pass_complete, overflow)),
            ObservationPage::Unavailable { reason } => Err(reason),
        }
    }
    #[test]
    fn bounded_passes_cover_insertions_without_sort_order_or_stamp_renewal() {
        let owner = BuilderId::generate();
        let mut log = ObservationLog::default();
        let mut samples: Vec<_> = (0..130).map(|_| sample(&owner)).collect();
        samples.sort_by(|a, b| b.binding.builder_id.cmp(&a.binding.builder_id));
        for value in &samples {
            log.insert(value.clone());
        }
        assert!(log.snapshot(0).is_empty());
        assert_eq!(log.snapshot(usize::MAX), samples[..PAGE_LIMIT]);
        assert!(log.contains_key(&samples[0].binding.builder_id));
        assert!(!log.contains_key(&BuilderId::generate()));
        let (first, cursor, complete, overflow) = available(read(&log, &owner, None, 200)).unwrap();
        assert_eq!(first.len(), 64);
        assert!(!complete && !overflow);
        assert_eq!(cursor.upper_sequence, 130);
        let mut late = sample(&owner);
        // Even an insertion whose builder sorts before every existing key is next.
        late.binding.builder_id = BuilderId::parse("bld_00000000000000000000000002").unwrap();
        log.insert(late.clone());
        let (second, cursor, complete, _) =
            available(read(&log, &owner, Some(cursor), 210)).unwrap();
        assert_eq!(second.len(), 64);
        assert!(!complete);
        assert_eq!(cursor.upper_sequence, 130);
        let (third, cursor, complete, _) =
            available(read(&log, &owner, Some(cursor), 220)).unwrap();
        assert_eq!(third.len(), 2);
        assert!(complete);
        let joined: Vec<_> = first.into_iter().chain(second).chain(third).collect();
        assert_eq!(
            joined.iter().map(|v| v.sequence).collect::<Vec<_>>(),
            (1..=130).collect::<Vec<_>>()
        );
        assert_eq!(
            joined
                .into_iter()
                .map(|v| v.observation)
                .collect::<Vec<_>>(),
            samples
        );
        let (next, cursor, complete, _) = available(read(&log, &owner, Some(cursor), 230)).unwrap();
        assert_eq!(
            next,
            vec![SequencedObservation {
                sequence: 131,
                observation: late.clone()
            }]
        );
        assert!(complete);
        let (empty, cursor, complete, _) =
            available(read(&log, &owner, Some(cursor), 240)).unwrap();
        assert!(empty.is_empty() && complete);
        let mut after_pass = sample(&owner);
        after_pass.binding.builder_id = BuilderId::parse("bld_00000000000000000000000001").unwrap();
        log.insert(after_pass.clone());
        let (next, _, complete, _) = available(read(&log, &owner, Some(cursor), 250)).unwrap();
        assert_eq!(
            next,
            vec![SequencedObservation {
                sequence: 132,
                observation: after_pass
            }]
        );
        assert!(complete);
        let mut replay = late.clone();
        replay.finished_monotonic_ms = 239;
        log.insert(replay);
        let (_, _, _, _) = available(read(&log, &owner, None, 250)).unwrap();
        let retained = log.values.get(&131).unwrap();
        assert_eq!(retained, &late);
        assert_eq!(log.last_sequence, 132);
        assert_eq!(250 - retained.finished_monotonic_ms, 150);
    }
    #[test]
    fn cursors_reject_restart_invalid_watermarks_boot_and_clock_changes() {
        let owner = BuilderId::generate();
        let mut log = ObservationLog::default();
        log.insert(sample(&owner));
        let (_, cursor, _, _) = available(read(&log, &owner, None, 200)).unwrap();
        assert_eq!(
            available(read(
                &log,
                &BuilderId::generate(),
                Some(cursor.clone()),
                201
            ))
            .unwrap_err(),
            UnavailableReason::OwnerChanged
        );
        let mut invalid = cursor.clone();
        invalid.after_sequence = 2;
        assert_eq!(
            available(read(&log, &owner, Some(invalid), 201)).unwrap_err(),
            UnavailableReason::InvalidCursor
        );
        let mut invalid = cursor.clone();
        invalid.upper_sequence = 2;
        assert_eq!(
            available(read(&log, &owner, Some(invalid), 201)).unwrap_err(),
            UnavailableReason::InvalidCursor
        );
        assert_eq!(
            available(read(&log, &owner, Some(cursor.clone()), 199)).unwrap_err(),
            UnavailableReason::ClockAnomaly
        );
        // Repeating an exchange at the same native tick is valid, not regression.
        let (entries, same_tick, complete, _) =
            available(read(&log, &owner, Some(cursor.clone()), 200)).unwrap();
        assert!(entries.is_empty() && complete);
        assert_eq!(same_tick.monotonic_now_ms, cursor.monotonic_now_ms);
        assert_eq!(
            available(log.page(
                &owner,
                &ObservationPageRequest {
                    cursor: Some(cursor)
                },
                Some("new-boot"),
                201
            ))
            .unwrap_err(),
            UnavailableReason::NativeBootChanged
        );
        for boot in [None, Some("")] {
            assert_eq!(
                available(log.page(&owner, &ObservationPageRequest::default(), boot, 201))
                    .unwrap_err(),
                UnavailableReason::ClockAnomaly
            );
        }
        let empty = ObservationLog::default();
        for length in [1, 128, 129] {
            let boot = "b".repeat(length);
            let page =
                available(empty.page(&owner, &ObservationPageRequest::default(), Some(&boot), 201));
            if length <= 128 {
                assert!(page.unwrap().0.is_empty());
            } else {
                assert_eq!(page.unwrap_err(), UnavailableReason::ClockAnomaly);
            }
        }
        assert_eq!(
            available(read(&log, &owner, None, u64::MAX)).unwrap_err(),
            UnavailableReason::ClockAnomaly
        );
        assert_eq!(
            available(read(&log, &owner, None, 99)).unwrap_err(),
            UnavailableReason::ClockAnomaly
        );
        let (entries, cursor, complete, overflow) =
            available(read(&empty, &owner, None, 201)).unwrap();
        assert!(entries.is_empty() && complete && !overflow);
        assert_eq!((cursor.after_sequence, cursor.upper_sequence), (0, 0));
    }
    #[test]
    fn each_sample_requires_its_own_owner_boot_and_checked_duration() {
        let owner = BuilderId::generate();
        for variant in 0..4 {
            let mut log = ObservationLog::default();
            let mut value = sample(&owner);
            match variant {
                0 => value.binding.owner_generation = BuilderId::generate(),
                1 => value.boot = "other-native-boot".into(),
                2 => value.finished_monotonic_ms = 201,
                _ => value.duration_ms = value.finished_monotonic_ms + 1,
            }
            log.insert(value);
            assert_eq!(
                available(read(&log, &owner, None, 200)).unwrap_err(),
                UnavailableReason::ClockAnomaly
            );
        }
        for duration in [0, 1, 100] {
            let mut log = ObservationLog::default();
            let mut value = sample(&owner);
            value.duration_ms = duration;
            log.insert(value.clone());
            // A completion in the current millisecond and duration == finish
            // are both valid; neither may be rounded into an anomaly.
            let (entries, _, complete, _) = available(read(&log, &owner, None, 100)).unwrap();
            assert!(complete);
            assert_eq!(entries[0].observation, value);
        }
    }
    #[test]
    fn full_log_and_sequence_overflow_preserve_known_samples_and_expose_unknown() {
        let owner = BuilderId::generate();
        let mut log = ObservationLog::default();
        let original = sample(&owner);
        log.insert(original.clone());
        for _ in 1..CAPACITY {
            log.insert(sample(&owner));
        }
        log.insert(original.clone());
        assert!(!log.overflow);
        log.insert(sample(&owner));
        assert_eq!(log.len(), CAPACITY);
        assert_eq!(log.values[&1], original);
        assert!(available(read(&log, &owner, None, 200)).unwrap().3);
        let mut exhausted = ObservationLog {
            last_sequence: u64::MAX,
            ..Default::default()
        };
        exhausted.insert(sample(&owner));
        assert!(exhausted.overflow);
        assert_eq!(exhausted.len(), 0);
    }
    #[test]
    fn optional_wire_fields_roundtrip_without_private_commands_or_paths() {
        use crate::{
            ids::RequestId,
            wire::{InspectRequest, SchemaVersion},
        };
        let request = InspectRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: RequestId::generate(),
            preparation_observations: None,
        };
        let legacy = serde_json::to_value(&request).unwrap();
        assert!(legacy.get("preparation_observations").is_none());
        assert_eq!(
            serde_json::from_value::<InspectRequest>(legacy).unwrap(),
            request
        );
        let owner = BuilderId::generate();
        let mut log = ObservationLog::default();
        log.insert(sample(&owner));
        let page = read(&log, &owner, None, 200);
        let encoded = serde_json::to_string(&page).unwrap();
        assert_eq!(
            serde_json::from_str::<ObservationPage>(&encoded).unwrap(),
            page
        );
        for private in ["argv", "env", "path", "private recipe", "private commands"] {
            assert!(!encoded.contains(private));
        }
        let (_, cursor, _, _) = available(page).unwrap();
        let request = InspectRequest {
            preparation_observations: Some(ObservationPageRequest {
                cursor: Some(cursor),
            }),
            ..request
        };
        assert_eq!(
            serde_json::from_str::<InspectRequest>(&serde_json::to_string(&request).unwrap())
                .unwrap(),
            request
        );
    }
}
