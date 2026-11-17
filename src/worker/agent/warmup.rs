//! `env_sync`: install versions ahead of the attempts that need them (the
//! warm-up of `workers sync`).
//!
//! The coordinator names tools and exact versions; everything else comes from
//! this worker's own registry (`toolchains.toml`): a version the operator did
//! not authorize is never installed. The install itself is the one a placed
//! attempt would start: `Host::toolchain_poll` with a consumer made for the
//! warm-up, so it passes the same setup supervisor and traffic gate, and an
//! attempt that needs the same version meanwhile joins the same flight.
//!
//! A request never waits for an install. It answers `pending`, and the
//! coordinator asks again with the same `sync_id`. A warm-up nobody asks
//! about any more is released by the agent's tick.

use super::*;
use crate::toolchain::preparation::{PreparationRequest, ToolPreparationPoll};
use crate::toolchain::registry::{Registry, Unavailable};
use crate::toolchain::versions::ToolId;

/// Warm-ups remembered at once; a new one beyond that is refused.
const MAX_WARMUPS: usize = 16;

pub(super) struct Warmup {
    /// What holds the reference to the installs of this warm-up.
    consumer: AttemptId,
    /// What was asked for; the same id never changes its question.
    tools: Vec<ToolRequirement>,
    begun_ms: u64,
    asked_ms: u64,
    /// Tools seen `pending`: when they turn up installed, this warm-up (or a
    /// flight it joined) installed them.
    waited: BTreeSet<String>,
    /// Decided outcomes, repeated to a later poll.
    done: BTreeMap<String, ToolSyncResult>,
    released: bool,
}

/// Why this worker will not install the version, for a person.
pub(super) fn why(tool: &str, version: &str, reason: &Unavailable) -> String {
    match reason {
        Unavailable::Unconfigured => {
            "no version manager is configured on this worker (toolchains.toml)".into()
        }
        Unavailable::Unsupported => {
            format!("the worker's toolchains.toml has no recipe for {tool}")
        }
        Unavailable::NotInInventory => {
            format!("{tool} {version} is not in the inventory of the worker's toolchains.toml")
        }
        Unavailable::ManualRequired(what) => format!("{tool} needs a person: {what}"),
        Unavailable::ResolveRequired | Unavailable::BaselineCheckRequired => {
            format!("{tool} {version} is not an exact installable version")
        }
        Unavailable::Invalid(what) => format!("{tool} {version} cannot be planned: {what}"),
    }
}

/// The trusted request for one named version, or why there is none.
pub(super) fn request(
    registry: Option<&Registry>,
    tool: &ToolRequirement,
) -> Result<PreparationRequest, String> {
    let registry =
        registry.ok_or_else(|| why(&tool.name, &tool.version, &Unavailable::Unconfigured))?;
    let id = ToolId::parse(&tool.name).map_err(|_| "not a tool name".to_string())?;
    let version = node_semver::Version::parse(&tool.version)
        .map_err(|_| format!("{} is not an exact version", tool.version))?;
    registry
        .inventory_request(&id, &version)
        .map_err(|e| why(&tool.name, &tool.version, &e))
}

/// One look at one tool. `waited` says whether an earlier look saw it pending.
pub(super) fn outcome(
    poll: Result<ToolPreparationPoll, String>,
    waited: bool,
    timed_out: bool,
) -> (SyncOutcome, Option<String>) {
    match poll {
        Ok(ToolPreparationPoll::Installed(_)) if waited => (SyncOutcome::Installed, None),
        Ok(ToolPreparationPoll::Installed(_)) => (SyncOutcome::Unchanged, None),
        Ok(ToolPreparationPoll::Failed(reason)) => (SyncOutcome::Failed, Some(reason.0)),
        Ok(ToolPreparationPoll::Pending) if timed_out => (
            SyncOutcome::Failed,
            Some("the install did not finish in time".into()),
        ),
        Ok(ToolPreparationPoll::Pending) => (SyncOutcome::Pending, None),
        Err(reason) => (SyncOutcome::Failed, Some(reason)),
    }
}

fn result(name: &str, (outcome, message): (SyncOutcome, Option<String>)) -> ToolSyncResult {
    ToolSyncResult {
        name: name.into(),
        outcome,
        message,
    }
}

impl Agent {
    pub(super) fn env_sync(&self, r: EnvSyncRequest) -> Result<EnvSyncResponse, ApiError> {
        if r.tools.len() > MAX_ENV_SYNC_TOOLS {
            return Err(invalid("env_sync names too many tools"));
        }
        let names: BTreeSet<&str> = r.tools.iter().map(|t| t.name.as_str()).collect();
        if names.len() != r.tools.len() {
            return Err(invalid("env_sync names a tool twice"));
        }
        let seen = self.store.max_coordinator_epoch().map_err(api)?;
        if r.coordinator_epoch < seen {
            return Err(ApiError::new(
                ErrorCode::StaleEpoch,
                format!(
                    "coordinator epoch {} is older than {}",
                    r.coordinator_epoch.0, seen.0
                ),
            ));
        }
        let registry = self.toolchains.as_ref();
        let results = if r.dry_run {
            r.tools
                .iter()
                .map(|tool| {
                    let poll = request(registry, tool).and_then(|request| {
                        use crate::toolchain::adapters::Availability;
                        match registry.expect("a request has a registry").detect(&request) {
                            Ok(Availability::Installed(receipt)) => {
                                Ok(ToolPreparationPoll::Installed(receipt))
                            }
                            Ok(Availability::Missing) => Ok(ToolPreparationPoll::Pending),
                            Ok(Availability::Unknown(e) | Availability::ManualRequired(e)) => {
                                Err(e.to_string())
                            }
                            Err(e) => Err(why(&tool.name, &tool.version, &e)),
                        }
                    });
                    let (outcome, message) = outcome(poll, false, false);
                    let outcome = match outcome {
                        SyncOutcome::Pending => SyncOutcome::WouldInstall,
                        other => other,
                    };
                    result(&tool.name, (outcome, message))
                })
                .collect()
        } else {
            self.warm_up(&r)?
        };
        Ok(EnvSyncResponse {
            schema_version: SchemaVersion::CURRENT,
            results,
        })
    }

    fn warm_up(&self, r: &EnvSyncRequest) -> Result<Vec<ToolSyncResult>, ApiError> {
        let now = platform::monotonic_ms();
        // One warm-up request at a time: a look reads receipts, it never
        // waits for an install.
        let mut warmups = lock(&self.warmups);
        self.reap_warmups_locked(&mut warmups, now);
        let id = r.sync_id.to_string();
        if !warmups.contains_key(&id) {
            if self.stopping() {
                return Err(ApiError::new(
                    ErrorCode::Overloaded,
                    "the worker is stopping; no install is started",
                ));
            }
            if warmups.len() >= MAX_WARMUPS {
                return Err(ApiError::new(
                    ErrorCode::Overloaded,
                    "too many warm-ups are in progress on this worker",
                ));
            }
            warmups.insert(
                id.clone(),
                Warmup {
                    consumer: AttemptId::generate(),
                    tools: r.tools.clone(),
                    begun_ms: now,
                    asked_ms: now,
                    waited: BTreeSet::new(),
                    done: BTreeMap::new(),
                    released: false,
                },
            );
        }
        let warmup = warmups.get_mut(&id).expect("inserted above");
        if warmup.tools != r.tools {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "this sync_id was used for other tools",
            ));
        }
        warmup.asked_ms = now;
        let timed_out = now.saturating_sub(warmup.begun_ms) >= self.cfg.prepare_timeout_ms;
        let mut results = Vec::new();
        for tool in &r.tools {
            if let Some(done) = warmup.done.get(&tool.name) {
                results.push(done.clone());
                continue;
            }
            let poll = request(self.toolchains.as_ref(), tool).and_then(|request| {
                self.host
                    .toolchain_poll(&warmup.consumer, &request)
                    .map_err(|e| e.message)
            });
            let waited = warmup.waited.contains(&tool.name);
            let one = result(&tool.name, outcome(poll, waited, timed_out));
            if one.outcome == SyncOutcome::Pending {
                warmup.waited.insert(tool.name.clone());
            } else {
                warmup.done.insert(tool.name.clone(), one.clone());
            }
            results.push(one);
        }
        if warmup.done.len() == r.tools.len() && !warmup.released {
            // Nothing is waited for any more (a timed-out install loses this
            // reference, and stops if it was the last one).
            self.host.setup_release(&warmup.consumer);
            warmup.released = true;
        }
        Ok(results)
    }

    fn reap_warmups_locked(&self, warmups: &mut BTreeMap<String, Warmup>, now: u64) {
        let idle = self.cfg.warmup_idle_ms;
        warmups.retain(|_, w| {
            let keep = now.saturating_sub(w.asked_ms) < idle;
            if !keep && !w.released {
                self.host.setup_release(&w.consumer);
            }
            keep
        });
    }

    /// Release the warm-ups nobody asked about for `warmup_idle_ms`: their
    /// reference to an install goes, and the last one stops the install.
    pub(super) fn reap_warmups(&self) {
        let mut warmups = lock(&self.warmups);
        self.reap_warmups_locked(&mut warmups, platform::monotonic_ms());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolchain::preparation::PreparationFailure;

    #[test]
    fn a_pending_look_times_out_and_a_failure_keeps_its_reason() {
        let pending = || Ok(ToolPreparationPoll::Pending);
        assert_eq!(
            outcome(pending(), false, false),
            (SyncOutcome::Pending, None)
        );
        assert_eq!(
            outcome(pending(), true, false),
            (SyncOutcome::Pending, None)
        );
        let (timed, message) = outcome(pending(), true, true);
        assert_eq!(timed, SyncOutcome::Failed);
        assert!(message.unwrap().contains("in time"));
        assert_eq!(
            outcome(
                Ok(ToolPreparationPoll::Failed(PreparationFailure(
                    "exit 19".into()
                ))),
                true,
                true
            ),
            (SyncOutcome::Failed, Some("exit 19".into()))
        );
        assert_eq!(
            outcome(Err("no recipe".into()), false, false),
            (SyncOutcome::Failed, Some("no recipe".into()))
        );
    }

    #[test]
    fn every_refusal_names_the_tool_or_the_file_to_change() {
        let say = |reason| why("node", "20.0.0", &reason);
        assert!(say(Unavailable::Unconfigured).contains("toolchains.toml"));
        assert!(say(Unavailable::Unsupported).contains("no recipe for node"));
        assert!(say(Unavailable::NotInInventory).contains("node 20.0.0 is not in the inventory"));
        assert!(say(Unavailable::ManualRequired("license")).contains("needs a person: license"));
        assert!(say(Unavailable::ResolveRequired).contains("not an exact installable version"));
        assert!(say(Unavailable::BaselineCheckRequired).contains("not an exact"));
        assert!(say(Unavailable::Invalid("probe")).contains("cannot be planned: probe"));
    }

    #[test]
    fn a_request_needs_a_registry_a_tool_name_and_an_exact_version() {
        let tool = |name: &str, version: &str| ToolRequirement {
            name: name.into(),
            version: version.into(),
        };
        assert!(request(None, &tool("node", "20.0.0"))
            .unwrap_err()
            .contains("no version manager is configured"));
    }
}
