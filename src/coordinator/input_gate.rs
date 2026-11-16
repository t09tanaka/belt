//! Connection-local source readiness metadata. No IO or synchronization lives here.
use crate::ids::{AgentEpoch, Digest, RunId, SourceId, WorkerBootId};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyInput {
    pub run_id: RunId,
    pub source_id: SourceId,
    pub manifest_digest: Digest,
}
#[derive(Debug, Clone)]
pub struct PlacementInputs {
    pub generation: u64,
    pub ready: Vec<ReadyInput>,
    pub identity: Option<(WorkerBootId, AgentEpoch)>,
}

pub(super) struct State {
    ready: Vec<ReadyInput>,
    evaluated: BTreeSet<RunId>,
    identity: Option<(WorkerBootId, AgentEpoch)>,
    generation: u64,
}
impl State {
    pub fn new(generation: u64) -> Self {
        Self {
            ready: Vec::new(),
            evaluated: BTreeSet::new(),
            identity: None,
            generation,
        }
    }
    pub fn snapshot(&self) -> PlacementInputs {
        PlacementInputs {
            generation: self.generation,
            ready: self.ready.clone(),
            identity: self.identity.clone(),
        }
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn evaluated(&mut self, generation: u64, run: &RunId) {
        if self.generation == generation && self.ready.iter().any(|i| &i.run_id == run) {
            self.evaluated.insert(run.clone());
        }
    }
    pub fn evict_evaluated(&mut self) -> bool {
        let Some(index) = self
            .ready
            .iter()
            .position(|i| self.evaluated.contains(&i.run_id))
        else {
            return false;
        };
        let removed = self.ready.remove(index);
        self.evaluated.remove(&removed.run_id);
        true
    }
    pub fn observe(&mut self, boot: WorkerBootId, epoch: AgentEpoch, replacement_generation: u64) {
        if self.identity.as_ref() != Some(&(boot.clone(), epoch)) {
            self.ready.clear();
            self.evaluated.clear();
            self.generation = replacement_generation;
            self.identity = Some((boot, epoch));
        }
    }
    pub fn publish(&mut self, generation: u64, input: ReadyInput) {
        if generation == self.generation && self.ready.len() < 64 && !self.ready.contains(&input) {
            self.ready.push(input);
        }
    }
    pub fn retain(&mut self, keep: impl FnMut(&ReadyInput) -> bool) {
        self.ready.retain(keep);
        let current: BTreeSet<_> = self.ready.iter().map(|i| i.run_id.clone()).collect();
        self.evaluated.retain(|id| current.contains(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> ReadyInput {
        ReadyInput {
            run_id: RunId::generate(),
            source_id: SourceId::generate(),
            manifest_digest: Digest::of_bytes(b"manifest"),
        }
    }
    fn boot(name: &str) -> WorkerBootId {
        WorkerBootId::parse(name).unwrap()
    }
    #[test]
    fn exact_generation_duplicate_and_capacity_checks() {
        let mut state = State::new(10);
        assert_eq!(state.generation(), 10);
        assert_eq!(state.snapshot().generation, 10);
        assert!(state.snapshot().ready.is_empty());
        let first = input();
        state.publish(9, first.clone());
        assert!(state.snapshot().ready.is_empty());
        state.publish(10, first.clone());
        state.publish(10, first.clone());
        assert_eq!(state.snapshot().ready, vec![first.clone()]);
        for _ in 1..64 {
            state.publish(10, input());
        }
        assert_eq!(state.snapshot().ready.len(), 64);
        state.publish(10, input());
        assert_eq!(state.snapshot().ready.len(), 64);
        assert_eq!(state.snapshot().ready[0], first);
    }
    #[test]
    fn only_current_known_evaluations_allow_fifo_eviction() {
        let mut state = State::new(10);
        let a = input();
        let b = input();
        state.publish(10, a.clone());
        state.publish(10, b.clone());
        state.evaluated(9, &a.run_id);
        state.evaluated(10, &RunId::generate());
        assert!(!state.evict_evaluated());
        assert!(state.evaluated.is_empty());
        state.evaluated(10, &b.run_id);
        state.evaluated(10, &a.run_id);
        assert!(state.evict_evaluated());
        assert_eq!(state.snapshot().ready, vec![b.clone()]);
        assert!(!state.evaluated.contains(&a.run_id));
        assert!(state.evict_evaluated());
        assert!(state.snapshot().ready.is_empty());
        assert!(state.evaluated.is_empty());
        assert!(!state.evict_evaluated());
    }
    #[test]
    fn boot_and_epoch_changes_clear_both_metadata_sets() {
        let mut state = State::new(10);
        let a = input();
        state.publish(10, a.clone());
        state.evaluated(10, &a.run_id);
        state.observe(boot("boot-a"), AgentEpoch(1), 20);
        assert_eq!(state.generation(), 20);
        assert!(state.snapshot().ready.is_empty());
        assert!(state.evaluated.is_empty());
        state.publish(10, a.clone());
        assert!(state.snapshot().ready.is_empty());
        state.publish(20, a.clone());
        state.evaluated(20, &a.run_id);
        state.observe(boot("boot-a"), AgentEpoch(1), 30);
        assert_eq!(state.generation(), 20);
        assert_eq!(state.snapshot().ready, vec![a.clone()]);
        assert!(state.evict_evaluated());
        state.publish(20, a.clone());
        state.evaluated(20, &a.run_id);
        state.observe(boot("boot-b"), AgentEpoch(1), 30);
        assert_eq!(state.generation(), 30);
        assert!(state.snapshot().ready.is_empty());
        assert!(state.evaluated.is_empty());
        state.publish(30, a.clone());
        state.evaluated(30, &a.run_id);
        state.observe(boot("boot-b"), AgentEpoch(2), 40);
        assert_eq!(state.generation(), 40);
        assert!(state.snapshot().ready.is_empty());
        assert!(state.evaluated.is_empty());
        state.publish(40, a.clone());
        state.evaluated(30, &a.run_id);
        assert!(!state.evict_evaluated());
        assert_eq!(state.snapshot().ready, vec![a]);
    }
    #[test]
    fn pruning_removes_only_dead_readiness_and_its_evaluation_metadata() {
        let mut state = State::new(10);
        let a = input();
        let b = input();
        state.publish(10, a.clone());
        state.publish(10, b.clone());
        state.evaluated(10, &a.run_id);
        state.evaluated(10, &b.run_id);
        state.retain(|i| i == &b);
        assert_eq!(state.snapshot().ready, vec![b.clone()]);
        assert_eq!(state.evaluated, BTreeSet::from([b.run_id.clone()]));
        assert_eq!(state.generation(), 10);
        state.retain(|_| true);
        assert_eq!(state.snapshot().ready, vec![b]);
        state.retain(|_| false);
        assert!(state.snapshot().ready.is_empty());
        assert!(state.evaluated.is_empty());
    }
}
