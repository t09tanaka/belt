//! Immutable sample provenance and conservative cross-host lifetime conversion.
use serde::{Deserialize, Serialize};
/// Original normal-report sample; queries never renew its identity or stamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSample {
    pub native_boot: String,
    pub sequence: u64,
    pub completed_monotonic_ms: u64,
    pub ttl_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeProvenance {
    pub profile: ProfileSample,
    pub observed_monotonic_ms: u64,
    pub observation_ttl_ms: u64,
    /// Earliest content verification in this reply; never reset by the second pass.
    pub verification_started_monotonic_ms: Option<u64>,
}
impl ProbeProvenance {
    /// Conservative cross-host deadline: parent send time, never receive time.
    pub(crate) fn deadline(&self, sent: u64, received: u64) -> Option<u64> {
        let p = &self.profile;
        if p.native_boot.is_empty()
            || p.native_boot.len() > 128
            || p.sequence == 0
            || p.ttl_ms != 5000
            || self.observation_ttl_ms == 0
            || self.observation_ttl_ms > 5000
            || received < sent
            || [
                sent,
                received,
                self.observed_monotonic_ms,
                p.completed_monotonic_ms,
            ]
            .contains(&u64::MAX)
        {
            return None;
        }
        let age = self
            .observed_monotonic_ms
            .checked_sub(p.completed_monotonic_ms)?;
        let remaining = p.ttl_ms.checked_sub(age)?.min(self.observation_ttl_ms);
        if let Some(started) = self.verification_started_monotonic_ms {
            if started < p.completed_monotonic_ms || started > self.observed_monotonic_ms {
                return None;
            }
            // Verification starts after the original profile sample. Its age
            // is therefore no greater, so the same 5000ms lifetime is already
            // bounded by the original profile deadline above.
        }
        let deadline = sent.checked_add(remaining)?;
        (received < deadline).then_some(deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> ProbeProvenance {
        ProbeProvenance {
            profile: ProfileSample {
                native_boot: "boot".into(),
                sequence: 1,
                completed_monotonic_ms: 100,
                ttl_ms: 5000,
            },
            observed_monotonic_ms: 5090,
            observation_ttl_ms: 5000,
            verification_started_monotonic_ms: None,
        }
    }
    #[test]
    fn preparation_freshness_uses_original_age_and_parent_send() {
        let mut p = sample();
        assert_eq!(p.deadline(1000, 1009), Some(1010));
        assert_eq!(p.deadline(1000, 1010), None);
        assert_eq!(p.deadline(1000, 999), None);
        p.observation_ttl_ms = 3;
        assert_eq!(p.deadline(1000, 1002), Some(1003));
        p.observed_monotonic_ms = 5100;
        assert_eq!(p.deadline(1000, 1000), None);
        p.observed_monotonic_ms = 5101;
        assert_eq!(p.deadline(1000, 1000), None);
        p.observed_monotonic_ms = 99;
        assert_eq!(p.deadline(1000, 1000), None);
    }
    #[test]
    fn preparation_freshness_rejects_malformed_and_overflow() {
        for index in 0..10 {
            let mut p = sample();
            let mut sent = 1000;
            let mut received = 1000;
            match index {
                0 => p.profile.native_boot.clear(),
                1 => p.profile.native_boot = "x".repeat(129),
                2 => p.profile.sequence = 0,
                3 => p.profile.ttl_ms = 5001,
                4 => p.observation_ttl_ms = 0,
                5 => p.observation_ttl_ms = 5001,
                6 => sent = u64::MAX,
                7 => received = u64::MAX,
                8 => p.observed_monotonic_ms = u64::MAX,
                _ => p.profile.completed_monotonic_ms = u64::MAX,
            }
            assert_eq!(p.deadline(sent, received), None, "case {index}");
        }
        assert_eq!(sample().deadline(u64::MAX - 5, u64::MAX - 5), None);
        let mut p = sample();
        p.profile.native_boot = "x".repeat(128);
        assert_eq!(p.deadline(1000, 1000), Some(1010));
    }
    #[test]
    fn preparation_freshness_limits_complete_verification_from_its_start() {
        let mut p = sample();
        p.verification_started_monotonic_ms = Some(100);
        assert_eq!(p.deadline(1000, 1009), Some(1010));
        assert_eq!(p.deadline(1000, 1010), None);
        p.verification_started_monotonic_ms = Some(99);
        assert_eq!(p.deadline(1000, 1000), None);
        p.verification_started_monotonic_ms = Some(5091);
        assert_eq!(p.deadline(1000, 1000), None);
        p.verification_started_monotonic_ms = Some(u64::MAX);
        assert_eq!(p.deadline(1000, 1000), None);
        p.verification_started_monotonic_ms = Some(5000);
        assert_eq!(p.deadline(1000, 1009), Some(1010));
        // Millisecond timestamps may put a complete tiny verification and
        // the final observation at exactly the same instant.
        p.verification_started_monotonic_ms = Some(p.observed_monotonic_ms);
        assert_eq!(p.deadline(1000, 1009), Some(1010));
    }
}
