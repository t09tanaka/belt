//! Connection-local prefetch preference, not an admission or reservation.
//! A repeated forecast cannot extend the first bounded wait for a cold worker.
use crate::ids::{RunId, TaskId};
use crate::storage::QueuedTask;
use std::collections::BTreeMap;
const CAP: usize = 64;
const WAIT_CEILING_MS: u64 = 30_000;
struct Choice {
    task: QueuedTask,
    generation: u64,
    first_deadline: u64,
    first_observed: u64,
    hint_deadline: u64,
    failed: bool,
}
#[derive(Default)]
pub(super) struct Preferences {
    choices: BTreeMap<TaskId, Choice>,
}
impl Preferences {
    pub fn eligible(&self, task: &TaskId, generation: u64, now: u64) -> bool {
        if now == u64::MAX {
            return false;
        }
        match self.choices.get(task) {
            Some(c) => {
                c.generation != generation
                    || (!c.failed && now >= c.first_observed && now < c.first_deadline)
            }
            None => self.choices.len() < CAP,
        }
    }
    pub fn choose(
        &mut self,
        task: &QueuedTask,
        generation: u64,
        expected: u64,
        ttl: u64,
        now: u64,
    ) -> bool {
        if ttl == 0 || !self.eligible(&task.task_id, generation, now) {
            return false;
        }
        let budget = expected.saturating_add(ttl).min(WAIT_CEILING_MS);
        let entry = self
            .choices
            .entry(task.task_id.clone())
            .or_insert_with(|| Choice {
                task: task.clone(),
                generation,
                first_deadline: now.saturating_add(budget),
                first_observed: now,
                hint_deadline: 0,
                failed: false,
            });
        if entry.generation != generation {
            *entry = Choice {
                task: task.clone(),
                generation,
                first_deadline: now.saturating_add(budget),
                first_observed: now,
                hint_deadline: 0,
                failed: false,
            };
        }
        if entry.task.run_id != task.run_id
            || entry.task.source_id != task.source_id
            || entry.task.spec != task.spec
        {
            entry.failed = true;
            return false;
        }
        entry.hint_deadline = now.saturating_add(ttl).min(entry.first_deadline);
        true
    }
    pub fn preferred(&self, generation: u64, now: u64) -> Option<QueuedTask> {
        self.choices
            .values()
            .find(|c| {
                c.generation == generation
                    && !c.failed
                    && now >= c.first_observed
                    && now < c.hint_deadline
            })
            .map(|c| c.task.clone())
    }
    pub fn fail(&mut self, run: &RunId, generation: u64) {
        for c in self.choices.values_mut() {
            if &c.task.run_id == run && c.generation == generation {
                c.failed = true;
            }
        }
    }
    pub fn clear(&mut self, task: Option<&TaskId>, run: Option<&RunId>) {
        self.choices.retain(|id, c| {
            !task.is_some_and(|t| t == id) && !run.is_some_and(|r| r == &c.task.run_id)
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn task() -> QueuedTask {
        QueuedTask {
            task_id: TaskId::generate(),
            run_id: RunId::generate(),
            source_id: Some(crate::ids::SourceId::generate()),
            spec: serde_json::json!({"manifest_digest":"exact"}),
            created_at_ms: 0,
            queue_rowid: 1,
        }
    }
    #[test]
    fn repeated_hints_never_extend_first_wait_and_cancel_retry_resets() {
        let mut p = Preferences::default();
        let task = task();
        assert!(p.choose(&task, 1, 1000, 2000, 10));
        assert!(p.choose(&task, 1, 1000, 2000, 2010));
        assert!(!p.eligible(&task.task_id, 1, 3010));
        assert!(!p.choose(&task, 1, 1000, 2000, 3011));
        p.clear(Some(&task.task_id), None);
        assert!(p.choose(&task, 1, 1000, 2000, 3011));
    }
    #[test]
    fn failures_suppress_until_new_connection_and_ttl_cannot_exceed_ceiling() {
        let mut p = Preferences::default();
        let task = task();
        assert!(p.choose(&task, 1, u64::MAX, 60_000, 10));
        assert!(!p.eligible(&task.task_id, 1, 30_010));
        p.clear(None, Some(&task.run_id));
        assert!(p.choose(&task, 1, 1, 100, 20));
        p.fail(&task.run_id, 1);
        assert!(!p.eligible(&task.task_id, 1, 21));
        assert!(p.choose(&task, 2, 1, 100, 21));
        assert!(p.preferred(1, 22).is_none());
        assert!(p.preferred(2, 122).is_none());
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    fn task() -> QueuedTask {
        QueuedTask {
            task_id: TaskId::generate(),
            run_id: RunId::generate(),
            source_id: Some(crate::ids::SourceId::generate()),
            spec: serde_json::json!({"manifest_digest":"source-proof"}),
            created_at_ms: 0,
            queue_rowid: 1,
        }
    }
    #[test]
    fn full_capacity_refuses_new_task_but_new_generation_reuses_existing_slot() {
        let mut p = Preferences::default();
        let mut tasks = vec![];
        for _ in 0..64 {
            let t = task();
            assert!(p.choose(&t, 1, 100, 10, 1));
            tasks.push(t);
        }
        assert_eq!(p.choices.len(), 64);
        let extra = task();
        assert!(!p.eligible(&extra.task_id, 1, 2));
        assert!(!p.choose(&extra, 1, 100, 10, 2));
        p.fail(&tasks[0].run_id, 1);
        assert!(!p.eligible(&tasks[0].task_id, 1, 2));
        assert!(p.eligible(&tasks[0].task_id, 2, 2));
        assert!(p.choose(&tasks[0], 2, 100, 10, 2));
        assert_eq!(p.choices.len(), 64);
        assert_eq!(p.choices[&tasks[0].task_id].first_deadline, 112);
        assert!(!p.choices[&tasks[0].task_id].failed);
    }
    #[test]
    fn ttl_and_first_wait_have_strict_boundaries_and_clock_unknown_or_regression_never_prefers() {
        let t = task();
        let mut p = Preferences::default();
        assert!(!p.choose(&t, 1, 100, 0, 5));
        assert_eq!(p.choices.len(), 0);
        assert!(!p.eligible(&t.task_id, 1, u64::MAX));
        assert!(!p.choose(&t, 1, 100, 10, u64::MAX));
        assert!(p.choose(&t, 1, 3, 2, 10));
        assert_eq!(p.choices[&t.task_id].first_deadline, 15);
        assert!(p.preferred(1, 10).is_some());
        assert!(p.preferred(1, 11).is_some());
        assert!(p.preferred(1, 12).is_none());
        assert!(p.eligible(&t.task_id, 1, 14));
        assert!(!p.eligible(&t.task_id, 1, 15));
        assert!(p.choose(&t, 1, 100, 10, 11));
        assert!(p.preferred(1, 14).is_some());
        assert!(p.preferred(1, 15).is_none());
        assert!(p.preferred(1, 9).is_none());
        assert!(!p.eligible(&t.task_id, 1, 9));
        assert!(!p.choose(&t, 1, 3, 2, 9));
        assert!(p.preferred(1, u64::MAX).is_none());
        assert!(!p.choose(&t, 1, 3, 2, 15));
        p.clear(Some(&t.task_id), None);
        assert!(p.choose(&t, 1, u64::MAX, u64::MAX, 10));
        assert_eq!(p.choices[&t.task_id].first_deadline, 30_010);
        assert!(p.eligible(&t.task_id, 1, 30_009));
        assert!(!p.eligible(&t.task_id, 1, 30_010));
        p.clear(Some(&t.task_id), None);
        assert!(p.choose(&t, 1, 0, 10, u64::MAX - 2));
        assert!(p.preferred(1, u64::MAX - 1).is_some());
        assert!(p.preferred(1, u64::MAX).is_none());
    }
    #[test]
    fn changed_run_source_or_manifest_spec_cannot_refresh_exact_hint() {
        for field in 0..3 {
            let mut p = Preferences::default();
            let original = task();
            assert!(p.choose(&original, 1, 100, 10, 1));
            let mut changed = original.clone();
            match field {
                0 => changed.run_id = RunId::generate(),
                1 => changed.source_id = None,
                _ => changed.spec = serde_json::json!({"manifest_digest":"changed"}),
            };
            assert!(!p.choose(&changed, 1, 100, 10, 2));
            assert!(p.preferred(1, 2).is_none());
            assert!(!p.eligible(&original.task_id, 1, 2));
            assert!(p.choose(&changed, 2, 100, 10, 2));
            assert_eq!(p.preferred(2, 2).unwrap(), changed);
        }
    }
    #[test]
    fn failures_and_clear_apply_only_to_exact_run_task_and_generation() {
        let mut p = Preferences::default();
        let first = task();
        let mut same_run = task();
        same_run.run_id = first.run_id.clone();
        let other = task();
        for t in [&first, &same_run, &other] {
            assert!(p.choose(t, 1, 100, 10, 1));
        }
        p.fail(&first.run_id, 2);
        assert!(p.eligible(&first.task_id, 1, 2));
        p.fail(&RunId::generate(), 1);
        assert!(p.eligible(&first.task_id, 1, 2));
        p.fail(&first.run_id, 1);
        assert!(!p.eligible(&first.task_id, 1, 2));
        assert!(!p.eligible(&same_run.task_id, 1, 2));
        assert!(p.eligible(&other.task_id, 1, 2));
        p.clear(None, None);
        assert_eq!(p.choices.len(), 3);
        p.clear(Some(&first.task_id), None);
        assert!(!p.choices.contains_key(&first.task_id));
        assert!(p.choices.contains_key(&same_run.task_id));
        assert!(p.choose(&first, 1, 100, 10, 2));
        p.clear(None, Some(&first.run_id));
        assert_eq!(p.choices.len(), 1);
        assert!(p.choices.contains_key(&other.task_id));
        p.clear(Some(&TaskId::generate()), Some(&RunId::generate()));
        assert_eq!(p.choices.len(), 1);
        p.clear(Some(&other.task_id), None);
        assert!(p.preferred(1, 2).is_none());
    }
}
