//! The installer of `workers sync`: a warm-up asked of the running resident.
//!
//! The resident passes each look to the worker over the link it already
//! holds (`env_sync`), and the worker installs through its own registry and
//! setup gate. Nothing here starts a resident, opens ssh or reads a file the
//! worker wrote: no resident means no install, and that is said.
use super::{plain, InstallError, Installed, Installer};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{RequestId, WorkerId};
use crate::service::bootstrap::Spec;
use crate::service::resident::{Client, Operation, Reply};
use crate::toolchain::envsync::InstallStep;
use crate::wire::{SyncOutcome, ToolRequirement};
use std::time::{Duration, Instant};

/// Between two looks at a running install.
const POLL: Duration = Duration::from_millis(1000);
/// How long one version may take in all while the worker says it is still
/// installing. The worker gives up earlier (its preparation timeout).
const LIMIT: Duration = Duration::from_secs(3600);
/// How long nobody may answer (the resident, or the worker behind it) before
/// the sync ends: a dead link or a resident that does not know the request
/// is not waited for like an install.
const QUIET: Duration = Duration::from_secs(120);

/// Why the resident gave no reply.
#[derive(Debug, PartialEq, Eq)]
pub enum NoReply {
    /// Nothing listens: no resident runs.
    NotRunning,
    /// It did not answer this time.
    Other(String),
}

/// What one look at the warm-up means for the step.
#[derive(Debug)]
pub enum Look {
    Done(Installed),
    /// The worker answered: the install is running. Look again.
    Pending,
    /// The worker (or the resident) answered that it is busy for now: it is
    /// there, and nothing of the request was lost. Look again.
    Busy(String),
    /// Nobody answered this time (the reason is said if that goes on).
    Again(String),
    Failed(String),
    Stop(ApiError),
}

fn not_running() -> ApiError {
    ApiError::new(
        ErrorCode::CommunicationUnknown,
        "the resident is not running, and workers sync installs through it; start it (any exec or submit does), then run workers sync again",
    )
}

/// Pure: the reply (or its absence) to what the installer does next. `begun`
/// says that an earlier look saw the install running on the worker: from then
/// on a resident that is away, or back without its link to the worker yet, is
/// a missing answer and not the end of the sync.
pub fn look(step: &InstallStep, reply: Result<Reply, NoReply>, begun: bool) -> Look {
    match reply {
        Err(NoReply::NotRunning) if begun => {
            Look::Again("the resident is not running; it may be restarting".into())
        }
        Err(NoReply::NotRunning) => Look::Stop(not_running()),
        Err(NoReply::Other(why)) => Look::Again(format!("the resident did not answer: {why}")),
        Ok(Reply::EnvSync { results }) => {
            match results.iter().find(|r| r.name == step.tool) {
                Some(r) => match r.outcome {
                    SyncOutcome::Installed => Look::Done(Installed::Now),
                    // A worker that restarted meanwhile no longer knows that
                    // it was this warm-up that waited for the install.
                    SyncOutcome::Unchanged if begun => Look::Done(Installed::Now),
                    SyncOutcome::Unchanged => Look::Done(Installed::Already),
                    SyncOutcome::Pending => Look::Pending,
                    SyncOutcome::Failed => Look::Failed(plain(
                        r.message.as_deref().unwrap_or("the worker gave no reason"),
                    )),
                    SyncOutcome::WouldInstall => Look::Stop(ApiError::new(
                        ErrorCode::Internal,
                        "the worker answered a dry run to an install",
                    )),
                },
                None => Look::Stop(ApiError::new(
                    ErrorCode::Internal,
                    format!("the worker did not answer for {}", step.tool),
                )),
            }
        }
        // The link gave no answer in time: the warm-up may well go on, so
        // the same one is asked about again.
        Ok(Reply::ControlError { error }) if error.code == ErrorCode::CommunicationUnknown => {
            Look::Again(plain(&error.message))
        }
        // Too many warm-ups, or too many requests: an answer, ask again.
        Ok(Reply::ControlError { error }) if error.code == ErrorCode::Overloaded => {
            Look::Busy(plain(&error.message))
        }
        // The resident is back, and has not reached the worker again yet.
        Ok(Reply::ControlError { error }) if begun && error.code == ErrorCode::NotFound => {
            Look::Again(plain(&error.message))
        }
        // Everything else ends the sync with what was said: a worker that is
        // stopping, one that must be upgraded, one the resident does not hold.
        Ok(Reply::ControlError { error }) => Look::Stop(ApiError {
            message: plain(&error.message),
            ..error
        }),
        // Stopping after the install was seen running: it may come back.
        Ok(Reply::Stopping) if begun => {
            Look::Again("the resident is stopping; it may be restarting".into())
        }
        Ok(Reply::Stopping) => Look::Stop(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "the resident is stopping; run workers sync again once it is back",
        )),
        Ok(Reply::Error { message }) => Look::Stop(ApiError::new(
            ErrorCode::ProtocolIncompatible,
            format!(
                "the running resident refused the request ({}); if it is older than this command, restart it and run workers sync again",
                plain(&message)
            ),
        )),
        Ok(_) => Look::Stop(ApiError::new(
            ErrorCode::Internal,
            "the resident answered something else",
        )),
    }
}

/// Pure: whether a look is an answer of the worker (it is there), and whether
/// it shows the install running.
pub fn answer_of(look: &Look) -> (bool, bool) {
    match look {
        Look::Pending => (true, true),
        Look::Busy(_) => (true, false),
        _ => (false, false),
    }
}

/// What is added to a sync that ends after its install was seen running.
const MAY_GO_ON: &str = "An install may still be running on the worker, so look with workers diff before running workers sync again";

/// Pure: `error` ends a sync; `begun` says its install was seen running.
pub fn ended(mut error: ApiError, begun: bool) -> ApiError {
    if begun {
        error.message = format!("{}. {MAY_GO_ON}", error.message.trim_end_matches('.'));
    }
    error
}

/// Pure: whether the resident itself understood a request (it answered the
/// operation, with a result or with an error of its own). One that never
/// does may be older than this command.
pub fn understood(reply: &Result<Reply, NoReply>) -> bool {
    matches!(
        reply,
        Ok(Reply::EnvSync { .. } | Reply::ControlError { .. } | Reply::Stopping)
    )
}

/// Asks the running resident. Connects at the first install, so a sync that
/// has nothing to install needs no resident.
pub struct ResidentWarmup {
    spec: Spec,
    poll: Duration,
    limit: Duration,
    quiet: Duration,
}

/// Pure: whether a wait that began `waited` ago and was last answered
/// `unanswered` ago ends now, and why. `understood` says that the resident
/// answered the operation at least once: only one that never did is said to
/// be possibly older than this command.
pub fn give_up(
    waited: Duration,
    unanswered: Duration,
    limit: Duration,
    quiet: Duration,
    last: &str,
    understood: bool,
) -> Option<String> {
    if unanswered >= quiet {
        let older = if understood {
            ""
        } else {
            "; if the running resident is older than this command, restart it"
        };
        return Some(format!(
            "no answer for {} s ({last}){older}. {MAY_GO_ON}",
            quiet.as_secs()
        ));
    }
    (waited >= limit).then(|| {
        format!(
            "the install is still running after {} s; look with workers diff before running workers sync again",
            limit.as_secs()
        )
    })
}

impl ResidentWarmup {
    pub fn new(spec: Spec) -> Self {
        Self {
            spec,
            poll: POLL,
            limit: LIMIT,
            quiet: QUIET,
        }
    }
    /// For the tests: how often to look, and for how long in all and
    /// without an answer.
    #[doc(hidden)]
    pub fn paced(mut self, poll: Duration, limit: Duration) -> Self {
        self.poll = poll;
        self.limit = limit;
        self.quiet = limit;
        self
    }
}

fn no_reply(error: &anyhow::Error) -> NoReply {
    use std::io::ErrorKind;
    match error.downcast_ref::<std::io::Error>().map(|e| e.kind()) {
        Some(ErrorKind::ConnectionRefused | ErrorKind::NotFound) => NoReply::NotRunning,
        _ => NoReply::Other(error.to_string()),
    }
}

/// Pure: why no client of the resident could be made. No token or no socket
/// is no resident (none is started here); anything else is said as it is.
fn no_client(error: &anyhow::Error) -> ApiError {
    match no_reply(error) {
        NoReply::NotRunning => not_running(),
        NoReply::Other(why) => ApiError::new(
            ErrorCode::Internal,
            format!("the resident cannot be asked: {}", plain(&why)),
        ),
    }
}

impl Installer for ResidentWarmup {
    fn install(&mut self, worker: &str, step: &InstallStep) -> Result<Installed, InstallError> {
        let stop = |e: ApiError| InstallError::Stop(e);
        let worker_id = WorkerId::parse(worker)
            .map_err(|e| stop(ApiError::new(ErrorCode::InvalidInput, e.to_string())))?;
        let mut client: Client =
            crate::service::bootstrap::load_client(&self.spec).map_err(|e| stop(no_client(&e)))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| stop(ApiError::new(ErrorCode::Internal, e.to_string())))?;
        // One warm-up for this step, asked about until it is decided.
        let sync_id = RequestId::generate();
        let start = Instant::now();
        // When the worker last answered, whether it ever said the install is
        // running, and whether the resident ever answered the operation.
        let mut answered = start;
        let mut begun = false;
        let mut heard = false;
        eprintln!(
            "installing {} {} on worker {worker}",
            step.tool, step.version
        );
        loop {
            let reply = runtime
                .block_on(client.request(Operation::EnvSync {
                    worker: worker_id.clone(),
                    sync_id: sync_id.clone(),
                    tools: vec![ToolRequirement {
                        name: step.tool.clone(),
                        version: step.version.clone(),
                    }],
                }))
                .map_err(|e| no_reply(&e));
            if understood(&reply) {
                heard = true;
            } else if let Ok(fresh) = crate::service::bootstrap::load_client(&self.spec) {
                // A resident that restarted has a new token and socket: the
                // next look is asked of the one that runs now.
                client = fresh;
            }
            let what = look(step, reply, begun);
            let (is_answer, running) = answer_of(&what);
            if is_answer {
                answered = Instant::now();
            }
            let last = match what {
                Look::Done(how) => return Ok(how),
                Look::Failed(reason) => return Err(InstallError::Failed(reason)),
                Look::Stop(error) => return Err(stop(ended(error, begun))),
                Look::Pending => String::new(),
                Look::Busy(why) | Look::Again(why) => why,
            };
            begun |= running;
            if let Some(why) = give_up(
                start.elapsed(),
                answered.elapsed(),
                self.limit,
                self.quiet,
                &last,
                heard,
            ) {
                return Err(stop(ApiError::new(ErrorCode::CommunicationUnknown, why)));
            }
            std::thread::sleep(self.poll);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::ToolSyncResult;

    fn step() -> InstallStep {
        InstallStep {
            tool: "node".into(),
            version: "20.0.0".into(),
        }
    }
    fn answered(outcome: SyncOutcome, message: Option<&str>) -> Result<Reply, NoReply> {
        Ok(Reply::EnvSync {
            results: vec![ToolSyncResult {
                name: "node".into(),
                outcome,
                message: message.map(Into::into),
            }],
        })
    }
    fn control(code: ErrorCode, message: &str) -> Result<Reply, NoReply> {
        Ok(Reply::ControlError {
            error: ApiError::new(code, message),
        })
    }

    #[test]
    fn installed_and_already_there_are_told_apart_and_pending_is_asked_again() {
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Installed, None), false),
            Look::Done(Installed::Now)
        ));
        // Already there: done, and not something this sync installed.
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Unchanged, None), false),
            Look::Done(Installed::Already)
        ));
        // Seen running before: a worker that restarted says `unchanged`,
        // and it was still this sync that had it installed.
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Unchanged, None), true),
            Look::Done(Installed::Now)
        ));
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Installed, None), true),
            Look::Done(Installed::Now)
        ));
        for begun in [false, true] {
            assert!(matches!(
                look(&step(), answered(SyncOutcome::Pending, None), begun),
                Look::Pending
            ));
        }
    }

    #[test]
    fn a_wait_ends_when_nobody_answers_or_the_install_runs_too_long() {
        let s = Duration::from_secs;
        assert_eq!(give_up(s(10), s(5), s(3600), s(120), "x", false), None);
        // Answered all along: only the long limit ends it.
        assert_eq!(give_up(s(3599), s(0), s(3600), s(120), "", true), None);
        let long = give_up(s(3600), s(0), s(3600), s(120), "", true).unwrap();
        assert!(long.contains("still running after 3600 s"), "{long}");
        // Not answered: ended early, with the last reason.
        assert_eq!(give_up(s(119), s(119), s(3600), s(120), "x", false), None);
        let quiet = give_up(s(120), s(120), s(3600), s(120), "connection closed", false).unwrap();
        assert_eq!(
            quiet,
            format!("no answer for 120 s (connection closed); if the running resident is older than this command, restart it. {MAY_GO_ON}")
        );
        // A resident that answered the operation is not an older one.
        let known = give_up(
            s(120),
            s(120),
            s(3600),
            s(120),
            "worker w did not answer",
            true,
        )
        .unwrap();
        assert_eq!(
            known,
            format!("no answer for 120 s (worker w did not answer). {MAY_GO_ON}")
        );
        assert!(!known.contains("restart"), "{known}");
        // Both at once: the missing answer is what is said.
        assert!(give_up(s(4000), s(200), s(3600), s(120), "x", true)
            .unwrap()
            .contains("no answer"));
    }

    #[test]
    fn pending_and_busy_are_answers_of_the_worker_and_only_pending_shows_the_install() {
        assert_eq!(answer_of(&Look::Pending), (true, true));
        assert_eq!(answer_of(&Look::Busy("crowded".into())), (true, false));
        assert_eq!(answer_of(&Look::Again("quiet".into())), (false, false));
        assert_eq!(answer_of(&Look::Failed("no".into())), (false, false));
        assert_eq!(answer_of(&Look::Done(Installed::Now)), (false, false));
        assert_eq!(
            answer_of(&Look::Stop(ApiError::new(ErrorCode::Internal, "x"))),
            (false, false)
        );
    }

    #[test]
    fn a_sync_that_ends_after_its_install_was_seen_says_it_may_go_on() {
        let error = || ApiError::new(ErrorCode::NotFound, "worker w is not connected.");
        assert_eq!(ended(error(), false).message, "worker w is not connected.");
        let after = ended(error(), true);
        assert_eq!(after.code, ErrorCode::NotFound);
        assert_eq!(
            after.message,
            format!("worker w is not connected. {MAY_GO_ON}")
        );
        assert!(after.message.contains("workers diff"));
    }

    #[test]
    fn only_an_answer_to_the_operation_shows_that_the_resident_knows_it() {
        assert!(understood(&answered(SyncOutcome::Pending, None)));
        assert!(understood(&control(ErrorCode::CommunicationUnknown, "x")));
        assert!(understood(&Ok(Reply::Stopping)));
        assert!(!understood(&Err(NoReply::NotRunning)));
        assert!(!understood(&Err(NoReply::Other("closed".into()))));
        assert!(!understood(&Ok(Reply::Error {
            message: "unknown variant".into()
        })));
    }

    #[test]
    fn a_client_that_cannot_be_made_is_no_resident_only_when_nothing_is_there() {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::ConnectionRefused,
        ] {
            let error = anyhow::Error::from(std::io::Error::from(kind));
            assert!(no_client(&error)
                .message
                .contains("the resident is not running"));
        }
        let denied = no_client(&anyhow::Error::from(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )));
        assert_eq!(denied.code, ErrorCode::Internal);
        assert!(
            !denied.message.contains("not running"),
            "{}",
            denied.message
        );
        assert!(denied.message.contains("cannot be asked"));
        let broken = no_client(&anyhow::anyhow!("invalid owner token\u{1b}[2J"));
        assert_eq!(
            broken.message,
            "the resident cannot be asked: invalid owner token[2J"
        );
    }

    #[test]
    fn a_failure_is_never_done_and_its_text_cannot_rewrite_the_terminal() {
        let Look::Failed(reason) = look(
            &step(),
            answered(SyncOutcome::Failed, Some("exit 19\u{1b}[2J\nnext")),
            true,
        ) else {
            panic!("a failed install must be reported as failed")
        };
        assert_eq!(reason, "exit 19[2Jnext");
        let Look::Failed(reason) = look(&step(), answered(SyncOutcome::Failed, None), false) else {
            panic!()
        };
        assert!(reason.contains("no reason"));
    }

    #[test]
    fn an_answer_for_another_tool_or_a_dry_run_ends_the_sync() {
        let other = Ok(Reply::EnvSync {
            results: vec![ToolSyncResult {
                name: "rust".into(),
                outcome: SyncOutcome::Installed,
                message: None,
            }],
        });
        let Look::Stop(e) = look(&step(), other, false) else {
            panic!()
        };
        assert!(e.message.contains("did not answer for node"));
        assert!(matches!(
            look(&step(), answered(SyncOutcome::WouldInstall, None), true),
            Look::Stop(_)
        ));
    }

    #[test]
    fn no_resident_stops_and_says_so_unless_the_install_was_seen_running() {
        let Look::Stop(e) = look(&step(), Err(NoReply::NotRunning), false) else {
            panic!()
        };
        assert!(e.message.contains("the resident is not running"));
        // After a look saw the install running, a resident that is away may
        // be restarting: asked again, within the time nobody may answer.
        let Look::Again(why) = look(&step(), Err(NoReply::NotRunning), true) else {
            panic!("a restarting resident must be asked again")
        };
        assert!(why.contains("may be restarting"), "{why}");
        for begun in [false, true] {
            assert!(matches!(
                look(&step(), Err(NoReply::Other("timeout".into())), begun),
                Look::Again(_)
            ));
        }
        let refused =
            anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));
        assert_eq!(no_reply(&refused), NoReply::NotRunning);
        let gone = anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::NotFound));
        assert_eq!(no_reply(&gone), NoReply::NotRunning);
        let slow = anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert!(matches!(no_reply(&slow), NoReply::Other(_)));
        assert!(matches!(
            no_reply(&anyhow::anyhow!("bad frame")),
            NoReply::Other(_)
        ));
    }

    #[test]
    fn busy_and_unanswered_are_asked_again_and_an_old_peer_stops_with_the_upgrade_hint() {
        for begun in [false, true] {
            // No answer from the worker in time: nobody answered.
            assert!(matches!(
                look(
                    &step(),
                    control(ErrorCode::CommunicationUnknown, "later"),
                    begun
                ),
                Look::Again(_)
            ));
            // Busy is an answer: the worker is there.
            let Look::Busy(why) = look(
                &step(),
                control(ErrorCode::Overloaded, "too many warm-ups\n"),
                begun,
            ) else {
                panic!("busy must be told apart from no answer")
            };
            assert_eq!(why, "too many warm-ups");
            let Look::Stop(e) = look(
                &step(),
                control(
                    ErrorCode::ProtocolIncompatible,
                    "run remote-task-runner workers upgrade w\n",
                ),
                begun,
            ) else {
                panic!()
            };
            assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
            assert_eq!(e.message, "run remote-task-runner workers upgrade w");
            assert!(matches!(
                look(&step(), control(ErrorCode::StaleEpoch, "x"), begun),
                Look::Stop(_)
            ));
        }
    }

    #[test]
    fn a_stopping_worker_ends_the_sync_at_once_with_what_it_said() {
        for begun in [false, true] {
            let Look::Stop(e) = look(
                &step(),
                control(
                    ErrorCode::Conflict,
                    "the worker is stopping; no install is started",
                ),
                begun,
            ) else {
                panic!("a stopping worker is not asked again")
            };
            assert_eq!(e.code, ErrorCode::Conflict);
            assert_eq!(e.message, "the worker is stopping; no install is started");
        }
    }

    #[test]
    fn a_worker_the_resident_does_not_hold_stops_at_first_and_is_asked_again_later() {
        let unknown = || control(ErrorCode::NotFound, "worker w is not connected\n");
        // At the first look it is a worker that is not registered or not
        // reached: said at once.
        let Look::Stop(e) = look(&step(), unknown(), false) else {
            panic!()
        };
        assert_eq!(e.code, ErrorCode::NotFound);
        // After the install was seen running, the resident restarted and is
        // attaching the worker again.
        let Look::Again(why) = look(&step(), unknown(), true) else {
            panic!("a link that is being attached again must be asked again")
        };
        assert_eq!(why, "worker w is not connected");
    }

    #[test]
    fn a_stopping_or_older_resident_stops_the_sync() {
        let Look::Stop(e) = look(&step(), Ok(Reply::Stopping), false) else {
            panic!()
        };
        assert!(e.message.contains("stopping"));
        // After the install was seen running, a resident that stops may be
        // restarting: asked again, like one that is away.
        let Look::Again(why) = look(&step(), Ok(Reply::Stopping), true) else {
            panic!("a resident that stops in the middle of an install is asked again")
        };
        assert!(why.contains("may be restarting"), "{why}");
        let Look::Stop(e) = look(
            &step(),
            Ok(Reply::Error {
                message: "unknown variant `env_sync`".into(),
            }),
            false,
        ) else {
            panic!()
        };
        assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
        assert!(e.message.contains("restart it"));
        assert!(matches!(
            look(
                &step(),
                Ok(Reply::Accepted {
                    run_id: crate::ids::RunId::generate()
                }),
                false
            ),
            Look::Stop(_)
        ));
    }
}
