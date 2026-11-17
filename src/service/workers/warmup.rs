//! The installer of `workers sync`: a warm-up asked of the running resident.
//!
//! The resident passes each look to the worker over the link it already
//! holds (`env_sync`), and the worker installs through its own registry and
//! setup gate. Nothing here starts a resident, opens ssh or reads a file the
//! worker wrote: no resident means no install, and that is said.
use super::{plain, InstallError, Installer};
use crate::error::{ApiError, ErrorCode};
use crate::ids::{RequestId, WorkerId};
use crate::service::bootstrap::Spec;
use crate::service::resident::{Client, Operation, Reply};
use crate::toolchain::envsync::InstallStep;
use crate::wire::{SyncOutcome, ToolRequirement};
use std::time::{Duration, Instant};

/// Between two looks at a running install.
const POLL: Duration = Duration::from_millis(1000);
/// How long one version may take in all. The worker gives up earlier (its
/// preparation timeout); this only ends a wait nobody answers.
const LIMIT: Duration = Duration::from_secs(3600);

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
    Done,
    /// Not decided yet: look again (the reason is said if time runs out).
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

/// Pure: the reply (or its absence) to what the installer does next.
pub fn look(step: &InstallStep, reply: Result<Reply, NoReply>) -> Look {
    match reply {
        Err(NoReply::NotRunning) => Look::Stop(not_running()),
        Err(NoReply::Other(why)) => Look::Again(format!("the resident did not answer: {why}")),
        Ok(Reply::EnvSync { results }) => {
            match results.iter().find(|r| r.name == step.tool) {
                Some(r) => match r.outcome {
                    SyncOutcome::Installed | SyncOutcome::Unchanged => Look::Done,
                    SyncOutcome::Pending => Look::Again("the install is still running".into()),
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
        // The worker or the resident is busy, or the link gave no answer:
        // the warm-up may well go on, so the same one is asked about again.
        Ok(Reply::ControlError { error })
            if matches!(
                error.code,
                ErrorCode::CommunicationUnknown | ErrorCode::Overloaded
            ) =>
        {
            Look::Again(plain(&error.message))
        }
        Ok(Reply::ControlError { error }) => Look::Stop(ApiError {
            message: plain(&error.message),
            ..error
        }),
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

/// Asks the running resident. Connects at the first install, so a sync that
/// has nothing to install needs no resident.
pub struct ResidentWarmup {
    spec: Spec,
    poll: Duration,
    limit: Duration,
}

impl ResidentWarmup {
    pub fn new(spec: Spec) -> Self {
        Self {
            spec,
            poll: POLL,
            limit: LIMIT,
        }
    }
    /// For the tests: how often to look and for how long.
    #[doc(hidden)]
    pub fn paced(mut self, poll: Duration, limit: Duration) -> Self {
        self.poll = poll;
        self.limit = limit;
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

impl Installer for ResidentWarmup {
    fn install(&mut self, worker: &str, step: &InstallStep) -> Result<(), InstallError> {
        let stop = |e: ApiError| InstallError::Stop(e);
        let worker_id = WorkerId::parse(worker)
            .map_err(|e| stop(ApiError::new(ErrorCode::InvalidInput, e.to_string())))?;
        // No credentials or no socket: nothing runs (never started here).
        let client: Client =
            crate::service::bootstrap::load_client(&self.spec).map_err(|_| stop(not_running()))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| stop(ApiError::new(ErrorCode::Internal, e.to_string())))?;
        // One warm-up for this step, asked about until it is decided.
        let sync_id = RequestId::generate();
        let begun = Instant::now();
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
            match look(step, reply) {
                Look::Done => return Ok(()),
                Look::Failed(reason) => return Err(InstallError::Failed(reason)),
                Look::Stop(error) => return Err(stop(error)),
                Look::Again(why) => {
                    if begun.elapsed() >= self.limit {
                        return Err(stop(ApiError::new(
                            ErrorCode::CommunicationUnknown,
                            format!(
                                "no outcome after {} s ({why}); the install may still be running on the worker, so look with workers diff before running workers sync again",
                                self.limit.as_secs()
                            ),
                        )));
                    }
                    std::thread::sleep(self.poll);
                }
            }
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
    fn installed_and_already_there_are_done_and_pending_is_asked_again() {
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Installed, None)),
            Look::Done
        ));
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Unchanged, None)),
            Look::Done
        ));
        assert!(matches!(
            look(&step(), answered(SyncOutcome::Pending, None)),
            Look::Again(_)
        ));
    }

    #[test]
    fn a_failure_is_never_done_and_its_text_cannot_rewrite_the_terminal() {
        let Look::Failed(reason) = look(
            &step(),
            answered(SyncOutcome::Failed, Some("exit 19\u{1b}[2J\nnext")),
        ) else {
            panic!("a failed install must be reported as failed")
        };
        assert_eq!(reason, "exit 19[2Jnext");
        let Look::Failed(reason) = look(&step(), answered(SyncOutcome::Failed, None)) else {
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
        let Look::Stop(e) = look(&step(), other) else {
            panic!()
        };
        assert!(e.message.contains("did not answer for node"));
        assert!(matches!(
            look(&step(), answered(SyncOutcome::WouldInstall, None)),
            Look::Stop(_)
        ));
    }

    #[test]
    fn no_resident_stops_and_says_so() {
        let Look::Stop(e) = look(&step(), Err(NoReply::NotRunning)) else {
            panic!()
        };
        assert!(e.message.contains("the resident is not running"));
        assert!(matches!(
            look(&step(), Err(NoReply::Other("timeout".into()))),
            Look::Again(_)
        ));
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
        for code in [ErrorCode::CommunicationUnknown, ErrorCode::Overloaded] {
            assert!(matches!(
                look(&step(), control(code, "later")),
                Look::Again(_)
            ));
        }
        let Look::Stop(e) = look(
            &step(),
            control(
                ErrorCode::ProtocolIncompatible,
                "run remote-task-runner workers upgrade w\n",
            ),
        ) else {
            panic!()
        };
        assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
        assert_eq!(e.message, "run remote-task-runner workers upgrade w");
        for code in [ErrorCode::NotFound, ErrorCode::StaleEpoch] {
            assert!(matches!(look(&step(), control(code, "x")), Look::Stop(_)));
        }
    }

    #[test]
    fn a_stopping_or_older_resident_stops_the_sync() {
        let Look::Stop(e) = look(&step(), Ok(Reply::Stopping)) else {
            panic!()
        };
        assert!(e.message.contains("stopping"));
        let Look::Stop(e) = look(
            &step(),
            Ok(Reply::Error {
                message: "unknown variant `env_sync`".into(),
            }),
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
                })
            ),
            Look::Stop(_)
        ));
    }
}
