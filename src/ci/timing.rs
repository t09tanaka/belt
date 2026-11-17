//! The SDK gives both planning and execution the same logical step key.
//! Scope it to this repository (or exact directory snapshot), matrix task,
//! working directory and service profile. Only digests reach the wire/ledger.
use crate::ids::Digest;
use local_ci::FrozenPlan;

pub(crate) fn task_scope(plan: &FrozenPlan, task: &str) -> Digest {
    let repository = plan
        .source
        .pinned
        .as_ref()
        .and_then(|p| p.repository_id.as_deref());
    let identity = repository.map_or(("source", plan.source.source_id.as_str()), |id| {
        ("repository", id)
    });
    Digest::of_bytes(&serde_json::to_vec(&(identity, task)).expect("strings serialize"))
}

pub(crate) fn step(scope: &Digest, sdk_key: &str, cwd: &str) -> Digest {
    Digest::of_bytes(&serde_json::to_vec(&(scope, sdk_key, cwd)).expect("strings serialize"))
}

pub(crate) fn keys(demand: &local_ci::TaskDemand, scope: &Digest) -> Vec<Option<Digest>> {
    demand
        .nodes
        .iter()
        .map(|node| node.key.as_deref().map(|key| step(scope, key, &node.cwd)))
        .collect()
}

pub(crate) fn profile_key(step: &Digest, profile: &Digest) -> String {
    format!("rtr-ci-step:{profile}:{step}")
}
