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
//!
//! A request does not wait long for the look either. The look (the registry,
//! receipts, the setup owner) runs on its own thread, one per `sync_id` at a
//! time and outside the lock of the warm-ups. The request that started it
//! waits `LOOK_WAIT` for it; a request that finds one under way answers at
//! once from what is known. A coordinator that gave up on a slow answer and
//! asks again therefore queues nothing here, and what the slow look found is
//! kept for the next request.

use super::*;
use crate::toolchain::adapters::{self, Availability};
use crate::toolchain::preparation::{PreparationRequest, ToolPreparationPoll};
use crate::toolchain::registry::{Registry, Unavailable};
use crate::toolchain::versions::ToolId;

/// Warm-ups remembered at once; a new one beyond that is refused.
const MAX_WARMUPS: usize = 16;
/// How long a request waits for the look it started. Well under the time the
/// coordinator waits for the answer.
const LOOK_WAIT: std::time::Duration = std::time::Duration::from_millis(1_500);

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
    /// A look is under way on its own thread. No second one is started, and
    /// the warm-up is not forgotten under it.
    looking: bool,
    /// The first look was taken: only that one lifts a remembered install
    /// failure of the versions asked for.
    looked: bool,
    /// The parent ended the sync while a look was under way: the look that is
    /// running lets go of the install and forgets the warm-up when it ends.
    cancelled: bool,
}

/// What a request is answered from: what is decided, the rest `pending`.
fn known(warmup: &Warmup) -> Vec<ToolSyncResult> {
    warmup
        .tools
        .iter()
        .map(|tool| {
            warmup
                .done
                .get(&tool.name)
                .cloned()
                .unwrap_or_else(|| result(&tool.name, (SyncOutcome::Pending, None)))
        })
        .collect()
}

/// Whether a warm-up last asked about at `asked_ms` is kept at `now`.
fn kept(looking: bool, asked_ms: u64, now: u64, idle: u64) -> bool {
    looking || now.saturating_sub(asked_ms) < idle
}

/// Ends the look when its thread ends, however it ends. The idle time starts
/// again then: the coordinator can only ask again once it has the answer, and
/// a look that took longer than the idle time (a loaded host) must not leave
/// the warm-up already idle, released before the next ask arrives.
struct Looking {
    agent: Arc<Agent>,
    id: String,
}
impl Drop for Looking {
    fn drop(&mut self) {
        if let Some(warmup) = lock(&self.agent.warmups).get_mut(&self.id) {
            warmup.looking = false;
            warmup.asked_ms = warmup.asked_ms.max(platform::monotonic_ms());
        }
    }
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

/// What a sync would do about what a look without an install found
/// (`Pending`: it would install). A root without a receipt is what a failed
/// or stopped install left: the sync clears it and installs again, so the
/// dry run says the same as for a version that is not there.
pub(super) fn would(found: Availability) -> Result<ToolPreparationPoll, String> {
    match found {
        Availability::Installed(receipt) => Ok(ToolPreparationPoll::Installed(receipt)),
        Availability::Missing | Availability::Unknown(adapters::UNRECEIPTED) => {
            Ok(ToolPreparationPoll::Pending)
        }
        Availability::Unknown(e) | Availability::ManualRequired(e) => Err(e.to_string()),
    }
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
    pub(super) fn env_sync(
        self: &Arc<Self>,
        store: &WorkerStore,
        r: EnvSyncRequest,
    ) -> Result<EnvSyncResponse, ApiError> {
        if r.tools.len() > MAX_ENV_SYNC_TOOLS {
            return Err(invalid("env_sync names too many tools"));
        }
        let names: BTreeSet<&str> = r.tools.iter().map(|t| t.name.as_str()).collect();
        if names.len() != r.tools.len() {
            return Err(invalid("env_sync names a tool twice"));
        }
        let seen = store.max_coordinator_epoch().map_err(api)?;
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
                        registry
                            .expect("a request has a registry")
                            .detect(&request)
                            .map_err(|e| why(&tool.name, &tool.version, &e))
                            .and_then(would)
                    });
                    let (outcome, message) = outcome(poll, false, false);
                    let outcome = match outcome {
                        SyncOutcome::Pending => SyncOutcome::WouldInstall,
                        other => other,
                    };
                    result(&tool.name, (outcome, message))
                })
                .collect()
        } else if r.tools.is_empty() {
            // Asking about no tool at all ends the warm-up of that id: the
            // parent is not waiting for it any more. (A worker that does not
            // know this answers a conflict, and lets the idle time end it.)
            self.end_warm_up(&r.sync_id.to_string());
            Vec::new()
        } else {
            self.warm_up(&r)?
        };
        Ok(EnvSyncResponse {
            schema_version: SchemaVersion::CURRENT,
            results,
        })
    }

    /// Let go of the reference of the warm-up `id` now, as the idle time would
    /// later: the last reference stops the install. A look that is under way
    /// ends it when it comes back.
    fn end_warm_up(&self, id: &str) {
        let mut warmups = lock(&self.warmups);
        let Some(warmup) = warmups.get_mut(id) else {
            return;
        };
        if warmup.looking {
            warmup.cancelled = true;
            return;
        }
        if !warmup.released {
            self.host.setup_release(&warmup.consumer);
        }
        warmups.remove(id);
    }

    fn warm_up(self: &Arc<Self>, r: &EnvSyncRequest) -> Result<Vec<ToolSyncResult>, ApiError> {
        let now = platform::monotonic_ms();
        let id = r.sync_id.to_string();
        {
            let mut warmups = lock(&self.warmups);
            self.reap_warmups_locked(&mut warmups, now);
            if !warmups.contains_key(&id) {
                if self.stopping() {
                    // Not `OVERLOADED`: asking again does not help, and the
                    // coordinator must say so instead of waiting.
                    return Err(ApiError::new(
                        ErrorCode::Conflict,
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
                        looking: false,
                        looked: false,
                        cancelled: false,
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
            if warmup.looking || warmup.done.len() == warmup.tools.len() {
                // A look is under way, or nothing is left to look at.
                return Ok(known(warmup));
            }
            warmup.looking = true;
        }
        // Outside the lock: the guard takes it when it is dropped, also when
        // the thread could not be started.
        let (tx, rx) = std::sync::mpsc::channel();
        let looking = Looking {
            agent: self.clone(),
            id: id.clone(),
        };
        std::thread::Builder::new()
            .name("warm-up".into())
            .spawn(move || {
                looking.agent.look(&looking.id);
                drop(looking);
                let _ = tx.send(());
            })
            .map_err(|e| internal("warm-up thread", e))?;
        // Bounded: a slow look is answered `pending`, and goes on.
        let _ = rx.recv_timeout(LOOK_WAIT);
        match lock(&self.warmups).get(&id) {
            Some(warmup) => Ok(known(warmup)),
            None => Err(ApiError::new(
                ErrorCode::CommunicationUnknown,
                "the warm-up was forgotten while it was looked at",
            )),
        }
    }

    /// One look at what is undecided of the warm-up `id`, outside the lock.
    fn look(&self, id: &str) {
        let (consumer, todo, waited, begun_ms, first) = {
            let mut warmups = lock(&self.warmups);
            let Some(warmup) = warmups.get_mut(id) else {
                return;
            };
            let first = !std::mem::replace(&mut warmup.looked, true);
            let todo: Vec<ToolRequirement> = warmup
                .tools
                .iter()
                .filter(|tool| !warmup.done.contains_key(&tool.name))
                .cloned()
                .collect();
            (
                warmup.consumer.clone(),
                todo,
                warmup.waited.clone(),
                warmup.begun_ms,
                first,
            )
        };
        let mut seen = Vec::with_capacity(todo.len());
        for tool in &todo {
            let poll = request(self.toolchains.as_ref(), tool).and_then(|request| {
                if first {
                    // A sync is an explicit request to install: a failure
                    // remembered from an earlier install does not answer it.
                    self.host.toolchain_retry(&request);
                }
                self.host
                    .toolchain_poll(&consumer, &request)
                    .map_err(|e| e.message)
            });
            let timed_out =
                platform::monotonic_ms().saturating_sub(begun_ms) >= self.cfg.prepare_timeout_ms;
            seen.push(result(
                &tool.name,
                outcome(poll, waited.contains(&tool.name), timed_out),
            ));
        }
        let release = {
            let mut warmups = lock(&self.warmups);
            let Some(warmup) = warmups.get_mut(id) else {
                return;
            };
            for one in seen {
                if one.outcome == SyncOutcome::Pending {
                    warmup.waited.insert(one.name.clone());
                } else {
                    warmup.done.insert(one.name.clone(), one);
                }
            }
            // Nothing is waited for any more (a timed-out install loses this
            // reference, and stops if it was the last one).
            let cancelled = warmup.cancelled;
            let release =
                (warmup.done.len() == warmup.tools.len() || cancelled) && !warmup.released;
            warmup.released |= release;
            if cancelled {
                warmups.remove(id);
            }
            release
        };
        if release {
            self.host.setup_release(&consumer);
        }
    }

    fn reap_warmups_locked(&self, warmups: &mut BTreeMap<String, Warmup>, now: u64) {
        let idle = self.cfg.warmup_idle_ms;
        warmups.retain(|_, w| {
            let keep = kept(w.looking, w.asked_ms, now, idle);
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
    fn a_dry_run_says_would_install_for_what_a_sync_installs_again() {
        assert_eq!(
            would(Availability::Missing),
            Ok(ToolPreparationPoll::Pending)
        );
        assert_eq!(
            would(Availability::Unknown(adapters::UNRECEIPTED)),
            Ok(ToolPreparationPoll::Pending)
        );
        // Anything else a sync fails on, the dry run fails on.
        assert_eq!(
            would(Availability::Unknown("unsafe_receipt")),
            Err("unsafe_receipt".into())
        );
        assert_eq!(
            would(Availability::ManualRequired("needs a person")),
            Err("needs a person".into())
        );
        assert_eq!(
            outcome(
                would(Availability::Unknown(adapters::UNRECEIPTED)),
                false,
                false
            ),
            (SyncOutcome::Pending, None)
        );
    }

    #[test]
    fn an_answer_is_what_is_decided_and_pending_for_the_rest_in_the_order_asked() {
        let tool = |name: &str| ToolRequirement {
            name: name.into(),
            version: "1.0.0".into(),
        };
        let failed = result("b", (SyncOutcome::Failed, Some("exit 19".into())));
        let warmup = Warmup {
            consumer: AttemptId::generate(),
            tools: vec![tool("c"), tool("b"), tool("a")],
            begun_ms: 0,
            asked_ms: 0,
            waited: BTreeSet::new(),
            done: BTreeMap::from([("b".to_string(), failed.clone())]),
            released: false,
            looking: true,
            looked: true,
            cancelled: false,
        };
        assert_eq!(
            known(&warmup),
            vec![
                result("c", (SyncOutcome::Pending, None)),
                failed,
                result("a", (SyncOutcome::Pending, None)),
            ]
        );
    }

    #[test]
    fn a_warm_up_is_kept_while_it_is_asked_about_or_looked_at() {
        assert!(kept(false, 100, 100, 60));
        assert!(kept(false, 100, 159, 60));
        assert!(!kept(false, 100, 160, 60));
        assert!(!kept(false, 100, 1_000, 60));
        // A clock that went back is not "long ago".
        assert!(kept(false, 100, 50, 60));
        // Never forgotten under a look, however long nobody asked.
        assert!(kept(true, 100, 1_000, 60));
        assert!(!kept(false, 0, 0, 0));
        assert!(kept(true, 0, 0, 0));
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
