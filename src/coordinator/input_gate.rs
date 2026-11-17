//! Connection-local source readiness metadata. No IO or synchronization lives here.
use crate::ids::{AgentEpoch, Digest, RunId, SourceId, WorkerBootId};
use std::collections::{BTreeMap, BTreeSet};

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

/// How many refusals a connection remembers. One more puts the oldest run
/// away (its refusal is recorded again if the worker still refuses).
const REFUSALS: usize = 64;

pub(super) struct State {
    /// The runs whose input this worker refused to take (a frozen plan it
    /// has no room for), with the reason: the worker is left out of their
    /// placement, by name, until the input arrives or the worker is another.
    refused: BTreeMap<RunId, String>,
    ready: Vec<ReadyInput>,
    evaluated: BTreeSet<RunId>,
    identity: Option<(WorkerBootId, AgentEpoch)>,
    generation: u64,
}
impl State {
    pub fn new(generation: u64) -> Self {
        Self {
            refused: BTreeMap::new(),
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
            self.refused.clear();
            self.generation = replacement_generation;
            self.identity = Some((boot, epoch));
        }
    }
    pub fn publish(&mut self, generation: u64, input: ReadyInput) {
        if generation == self.generation {
            self.refused.remove(&input.run_id);
        }
        if generation == self.generation && self.ready.len() < 64 && !self.ready.contains(&input) {
            self.ready.push(input);
        }
    }
    /// The worker refused the input of `run`. `true` when that is news (the
    /// first refusal of the run on this connection, or another reason): the
    /// caller records it once.
    pub fn refuse(&mut self, generation: u64, run: &RunId, why: &str) -> bool {
        if generation != self.generation || self.refused.get(run).is_some_and(|known| known == why)
        {
            return false;
        }
        if !self.refused.contains_key(run) && self.refused.len() >= REFUSALS {
            self.refused.pop_first();
        }
        self.refused.insert(run.clone(), why.to_string());
        true
    }
    /// The runs whose input the worker did not take, with the reason, while
    /// that holds.
    pub fn refusals(&self) -> Vec<(RunId, String)> {
        self.refused
            .iter()
            .map(|(run, why)| (run.clone(), why.clone()))
            .collect()
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
    fn refusal(state: &State, run: &RunId) -> Option<String> {
        let all = state.refusals();
        let mut of_run = all.into_iter().filter(|(refused, _)| refused == run);
        let found = of_run.next().map(|(_, why)| why);
        assert!(of_run.next().is_none());
        found
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
    fn a_refusal_is_news_once_and_holds_until_the_input_arrives_or_the_worker_changes() {
        let mut state = State::new(10);
        let a = input();
        let b = input();
        assert_eq!(refusal(&state, &a.run_id), None);
        // Of another connection: not kept.
        assert!(!state.refuse(9, &a.run_id, "full"));
        assert!(!state.refuse(11, &a.run_id, "full"));
        assert_eq!(refusal(&state, &a.run_id), None);
        assert!(state.refuse(10, &a.run_id, "full"));
        assert!(!state.refuse(10, &a.run_id, "full"));
        assert_eq!(refusal(&state, &a.run_id).as_deref(), Some("full"));
        assert_eq!(refusal(&state, &b.run_id), None);
        // Another reason is news, and replaces the first.
        assert!(state.refuse(10, &a.run_id, "part-way"));
        assert_eq!(refusal(&state, &a.run_id).as_deref(), Some("part-way"));
        assert!(state.refuse(10, &b.run_id, "full"));
        // An input published for another connection changes nothing.
        state.publish(9, a.clone());
        assert_eq!(refusal(&state, &a.run_id).as_deref(), Some("part-way"));
        assert!(state.snapshot().ready.is_empty());
        // The input arrived: the refusal of that run is over, the other holds.
        state.publish(10, a.clone());
        assert_eq!(refusal(&state, &a.run_id), None);
        assert_eq!(refusal(&state, &b.run_id).as_deref(), Some("full"));
        assert_eq!(state.snapshot().ready, vec![a.clone()]);
        // It arrives even when the ready list has no room to say so.
        let mut crowded = State::new(10);
        for _ in 0..64 {
            crowded.publish(10, input());
        }
        assert!(crowded.refuse(10, &a.run_id, "full"));
        crowded.publish(10, a.clone());
        assert_eq!(refusal(&crowded, &a.run_id), None);
        assert_eq!(crowded.snapshot().ready.len(), 64);
        // The same worker again keeps it; another worker starts clean.
        state.observe(boot("boot-a"), AgentEpoch(1), 20);
        assert_eq!(refusal(&state, &b.run_id), None);
        assert!(state.refuse(20, &b.run_id, "full"));
        state.observe(boot("boot-a"), AgentEpoch(1), 30);
        assert_eq!(refusal(&state, &b.run_id).as_deref(), Some("full"));
        state.observe(boot("boot-a"), AgentEpoch(2), 30);
        assert_eq!(refusal(&state, &b.run_id), None);
    }
    #[test]
    fn the_refusals_of_a_connection_are_bounded_and_the_oldest_run_goes_first() {
        let mut state = State::new(10);
        let mut runs: Vec<RunId> = (0..REFUSALS).map(|_| RunId::generate()).collect();
        runs.sort();
        for run in &runs {
            assert!(state.refuse(10, run, "full"));
        }
        assert_eq!(REFUSALS, 64);
        assert_eq!(state.refused.len(), REFUSALS);
        // A run that is known changes its reason in place: nothing goes.
        assert!(state.refuse(10, &runs[5], "other"));
        assert_eq!(state.refused.len(), REFUSALS);
        assert_eq!(refusal(&state, &runs[0]).as_deref(), Some("full"));
        // One more: the first goes.
        let more = RunId::generate();
        assert!(state.refuse(10, &more, "full"));
        assert_eq!(state.refused.len(), REFUSALS);
        assert_eq!(refusal(&state, &more).as_deref(), Some("full"));
        let gone = runs
            .iter()
            .filter(|run| refusal(&state, run).is_none())
            .count();
        assert_eq!(gone, 1);
        // With room, nothing goes.
        let mut small = State::new(10);
        for run in &runs[..REFUSALS - 1] {
            assert!(small.refuse(10, run, "full"));
        }
        assert!(small.refuse(10, &more, "full"));
        assert_eq!(small.refused.len(), REFUSALS);
        assert!(runs[..REFUSALS - 1]
            .iter()
            .all(|run| refusal(&small, run).is_some()));
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
