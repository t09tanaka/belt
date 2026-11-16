//! Parent-side `workers upgrade`: drain, place the new version, check it can
//! read what is installed, switch the service, check the bridge the resident
//! uses, resume. The worker-host helper does the switch; this side decides
//! what a failure means and never reports a state the host did not show:
//!
//! - a failure before anything was switched leaves the previous version
//!   serving and says so;
//! - a failure after the switch began rolls back to the previous version and
//!   checks it the same way;
//! - when the rollback fails too, or the host cannot be observed, the error
//!   says what was last seen and claims neither "upgraded" nor "stopped".
//!
//! The health check is one `hello` from this binary through the registered
//! bridge path. A resident that is already running reconnects on its own and
//! negotiates again; its link is not checked here.
use super::*;
use crate::service::worker_service::upgrade::SwitchRequest;

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Upgraded,
    AlreadyCurrent,
}

#[derive(Debug, Serialize)]
pub struct UpgradeResult {
    pub id: String,
    pub outcome: Outcome,
    /// The version that was serving before, and the one that serves now.
    pub from: String,
    pub to: String,
    /// The worker is draining now (it was before the upgrade and was left so).
    pub draining: bool,
    pub status: Status,
}

/// A helper action run by the staged binary of this exact build.
fn staged_command(digest: &str, action: &'static str) -> String {
    format!("exec \"{STAGING}/rtr-{digest}\" worker-service {action} --wait-secs {STOP_WAIT_SECS}")
}

fn short(digest: &str) -> &str {
    digest.get(..12).unwrap_or(digest)
}

/// The version to return to: what an unfinished switch started from, else
/// what is installed, whichever is not the target.
fn previous_version(before: &Status, target: &str) -> Option<String> {
    [
        before.switching_from.as_deref(),
        Some(&before.binary_sha256),
    ]
    .into_iter()
    .flatten()
    .find(|version| *version != target)
    .map(str::to_owned)
}

/// Why `status` does not show `digest` serving, if it does not.
fn not_serving(status: &Status, digest: &str) -> Option<String> {
    if status.switching_from.is_some() {
        return Some("the version switch is unfinished".into());
    }
    if status.binary_sha256 != digest {
        return Some(format!(
            "the host reports version {}",
            short(&status.binary_sha256)
        ));
    }
    match status.loaded {
        Some(true) => {}
        Some(false) => return Some("the service is not loaded".into()),
        None => return Some("the loaded service could not be identified".into()),
    }
    (!status.ready).then(|| "the agent does not accept connections".into())
}

fn observed(status: &Status) -> String {
    let loaded = match status.loaded {
        Some(true) => "loaded",
        Some(false) => "not loaded",
        None => "unknown",
    };
    format!(
        "last observed: version {}{}, service {loaded}, agent {}, {}",
        short(&status.binary_sha256),
        if status.switching_from.is_some() {
            " (switch unfinished)"
        } else {
            ""
        },
        if status.ready { "ready" } else { "not ready" },
        if status.draining {
            "draining"
        } else {
            "not draining"
        },
    )
}

struct Session<'a, R: Remote> {
    remote: &'a mut R,
    target: SshTarget,
    name: &'a str,
    cluster: ClusterId,
    /// The staged binary that runs every helper action.
    digest: String,
    /// The registered bridge path, when the registration has one.
    binary: Option<String>,
}
impl<R: Remote> Session<'_, R> {
    fn helper(
        &mut self,
        action: &'static str,
        stdin: &[u8],
        timeout: Duration,
    ) -> Result<Status, ApiError> {
        let out = self
            .remote
            .run(
                &self.target,
                &staged_command(&self.digest, action),
                stdin,
                timeout,
            )
            .map_err(|m| ApiError::new(ErrorCode::CommunicationUnknown, m))?;
        let status = parse_reply(&out)?;
        if status.worker_id.as_str() != self.name || status.cluster != self.cluster {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                "the host answered for a different worker identity or cluster",
            ));
        }
        Ok(status)
    }
    fn status(&mut self) -> Result<Status, ApiError> {
        self.helper("status", &[], Duration::from_secs(120))
    }
    fn switch(&mut self, action: &'static str, digest: &str) -> Result<Status, ApiError> {
        let request = SwitchRequest {
            schema: 1,
            cluster: self.cluster.clone(),
            worker_id: WorkerId::parse(self.name).map_err(|e| invalid(e.to_string()))?,
            binary_sha256: digest.to_string(),
        };
        let body = serde_json::to_vec(&request)
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        self.helper(action, &body, Duration::from_secs(STOP_WAIT_SECS + 300))
    }
    /// `digest` is serving: the host says so and the bridge the resident
    /// uses answers `hello` as this worker in this cluster.
    fn serving(&mut self, status: &Status, digest: &str) -> Result<(), String> {
        if let Some(why) = not_serving(status, digest) {
            return Err(why);
        }
        let binary = self
            .binary
            .clone()
            .or_else(|| status.bridge_binary.clone())
            .ok_or("the host did not report its bridge binary path")?;
        let (cluster, worker) = self
            .remote
            .bridge_hello(&self.target, &binary, &self.cluster)
            .map_err(|m| format!("the worker bridge did not complete hello: {m}"))?;
        if worker.as_str() != self.name || cluster != self.cluster {
            return Err("the worker bridge answered for a different worker or cluster".into());
        }
        Ok(())
    }
    /// Resume when this run is the one that drained. The text says what is
    /// left for the operator, if anything.
    fn resume(&mut self, wanted: bool) -> &'static str {
        if !wanted {
            return "; it was draining before and still is";
        }
        match self.helper("resume", &[], Duration::from_secs(120)) {
            Ok(status) if !status.draining => "",
            _ => "; it could not be resumed and is draining, run workers resume",
        }
    }
    fn last_observed(&mut self) -> (ErrorCode, String) {
        match self.status() {
            Ok(status) => (ErrorCode::Conflict, observed(&status)),
            Err(_) => (
                ErrorCode::CommunicationUnknown,
                "its state could not be observed".into(),
            ),
        }
    }
    /// Return to `previous` after the switch began. Always an error: either
    /// "rolled back and serving again", or what was last seen.
    fn roll_back(&mut self, previous: Option<String>, reason: &str, resume: bool) -> ApiError {
        let name = self.name;
        let Some(previous) = previous else {
            let (code, seen) = self.last_observed();
            let left = self.resume(resume);
            return ApiError::new(
                code,
                format!("upgrade of worker {name} failed ({reason}); no previous version is known, so nothing was rolled back; {seen}{left}"),
            );
        };
        let failure = match self.switch("rollback", &previous) {
            Ok(status) => match self.serving(&status, &previous) {
                Ok(()) => {
                    let left = self.resume(resume);
                    return ApiError::new(
                        ErrorCode::Conflict,
                        format!(
                            "upgrade of worker {name} failed ({reason}); rolled back to version {}, which is serving again{left}",
                            short(&previous)
                        ),
                    );
                }
                Err(why) => format!("the previous version did not pass its check: {why}"),
            },
            Err(error) => error.message,
        };
        let (code, seen) = self.last_observed();
        ApiError::new(
            code,
            format!("upgrade of worker {name} failed ({reason}) and the rollback failed ({failure}); {seen}; the worker is not confirmed serving either version"),
        )
    }
}

/// `workers upgrade <name>`: bring a registered worker to this binary.
pub fn upgrade(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    exe: &Path,
    name: &str,
) -> Result<UpgradeResult, ApiError> {
    check_name(name)?;
    let registration = registered(env, name)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no such worker is registered"))?;
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or_else(|| invalid("the coordinator has no cluster identity; run init"))?;
    if remote.parent_os() != "macos" {
        return Err(invalid(
            "upgrading a worker is implemented for macOS parents and workers only",
        ));
    }
    let target = target_of(&registration.ssh)?;
    let new = worker_service::sha256_file(exe)
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    // Staging writes only the bootstrap directory; the installed worker is untouched.
    ensure_staged(remote, &target, exe, &new)?;
    let mut session = Session {
        remote,
        target,
        name,
        cluster,
        digest: new.clone(),
        binary: registration.binary.clone(),
    };
    let before = session.status()?;
    let unfinished = before.switching_from.is_some();
    if !unfinished && before.binary_sha256 == new && session.serving(&before, &new).is_ok() {
        return Ok(UpgradeResult {
            id: name.into(),
            outcome: Outcome::AlreadyCurrent,
            from: new.clone(),
            to: new,
            draining: before.draining,
            status: before,
        });
    }
    // The version is installed but not serving: the switch below starts it
    // (it changes nothing else) and checks it like a new one.
    let previous = previous_version(&before, &new);
    // An unfinished switch was drained by the run that began it.
    let resume = !before.draining || unfinished;
    if !before.draining && !unfinished {
        match session.helper("drain", &[], Duration::from_secs(120)) {
            Ok(status) if status.draining => {}
            Ok(_) => {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!("worker {name} did not start draining; nothing was changed"),
                ))
            }
            Err(error) => {
                return Err(ApiError::new(
                    error.code,
                    format!(
                        "worker {name} was not upgraded: draining failed ({}); the previous version is installed and may be draining, run workers resume",
                        error.message
                    ),
                ))
            }
        }
    }
    let switched = match session.switch("upgrade", &new) {
        Ok(status) => status,
        Err(error) => {
            // What does the host show now?
            let after = match session.status() {
                Ok(after) => after,
                Err(_) => {
                    return Err(ApiError::new(
                        ErrorCode::CommunicationUnknown,
                        format!(
                            "upgrade of worker {name} failed ({}) and the worker could not be observed afterwards; it may be on either version and may be draining",
                            error.message
                        ),
                    ))
                }
            };
            if let Some(previous) = previous
                .as_ref()
                .filter(|previous| not_serving(&after, previous).is_none())
            {
                let left = session.resume(resume);
                return Err(ApiError::new(
                    error.code,
                    format!(
                        "worker {name} was not upgraded: {}; the previous version {} is still serving{left}",
                        error.message,
                        short(previous)
                    ),
                ));
            }
            if not_serving(&after, &new).is_some() {
                return Err(session.roll_back(previous, &error.message, resume));
            }
            // Only the answer was lost: the host shows the new version.
            after
        }
    };
    let reason = match session.serving(&switched, &new) {
        Ok(()) => {
            if session.binary.is_none() {
                if let Some(binary) = &switched.bridge_binary {
                    set_binary(env, name, binary)?;
                }
            }
            let left = session.resume(resume);
            if resume && !left.is_empty() {
                return Err(ApiError::new(
                    ErrorCode::Conflict,
                    format!(
                        "worker {name} was upgraded to version {} and passed its check{left}",
                        short(&new)
                    ),
                ));
            }
            return Ok(UpgradeResult {
                id: name.into(),
                outcome: Outcome::Upgraded,
                from: previous.unwrap_or_else(|| new.clone()),
                to: new,
                draining: !resume,
                status: Status {
                    draining: !resume,
                    ..switched
                },
            });
        }
        Err(why) => format!("the new version did not pass its check: {why}"),
    };
    Err(session.roll_back(previous, &reason, resume))
}

pub(super) fn command(
    name: &str,
    json: bool,
    env: &BTreeMap<String, String>,
    exe: std::io::Result<std::path::PathBuf>,
) -> i32 {
    let result = exe
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
        .and_then(|exe| {
            let spec = crate::service::context::runtime_spec(env)?;
            upgrade(&mut SshRemote, env, &spec, &exe, name)
        });
    let text = match &result {
        Ok(r) if r.outcome == Outcome::AlreadyCurrent => {
            format!("worker {name} already has version {}", short(&r.to))
        }
        Ok(r) => format!(
            "worker {name} upgraded from version {} to {}{}",
            short(&r.from),
            short(&r.to),
            if r.draining {
                "; it was draining before and still is"
            } else {
                ""
            }
        ),
        Err(_) => String::new(),
    };
    emit(
        json,
        result.map(|r| serde_json::to_value(r).expect("serializes")),
        || text,
    )
}

#[cfg(test)]
mod tests;
