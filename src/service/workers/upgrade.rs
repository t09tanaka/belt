//! Parent-side `workers upgrade`: check the resident, then let the host
//! drain, place the new version, check it can read what is installed and
//! switch the service; check the bridge the resident uses; commit. The
//! worker-host helper does the switch; this side decides what a failure
//! means and never reports a state the host did not show:
//!
//! - a failure before anything was switched leaves the previous version
//!   serving (checked with a `hello`, not assumed) and says so;
//! - a failure after the switch began rolls back to the previous version and
//!   checks it the same way;
//! - when the rollback fails too, or the host cannot be observed, the error
//!   says what was last seen and claims neither "upgraded" nor "stopped".
//!
//! The host keeps two records until this side commits: the version the
//! switch began from, and whether the upgrade made the drain. A run that
//! died anywhere is therefore continued by running `upgrade` again: it can
//! still roll back, and it resumes exactly the drain an earlier run made.
//! After the commit there is no rollback.
//!
//! The health check is one `hello` through the registered bridge path with
//! the features the coordinator resident requires. The upgrade is refused
//! while a resident of another build runs: its link to the new worker would
//! go unchecked. The previous version is checked with the features it agreed
//! to before the upgrade, not with what this build requires.
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
    /// The worker is draining now. Only a drain that was there before the
    /// upgrade is left; one the upgrade made has been resumed.
    pub draining: bool,
    pub status: Status,
}

/// A helper action run by the staged binary of this exact build.
fn staged_command(digest: &str, action: &'static str) -> String {
    format!("exec \"{STAGING}/belt-{digest}\" worker-service {action} --wait-secs {STOP_WAIT_SECS}")
}

fn short(digest: &str) -> &str {
    digest.get(..12).unwrap_or(digest)
}

/// The version to return to: what an uncommitted switch started from, else
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

/// Why `status` does not show `digest` serving, if it does not. A switch
/// that awaits its commit is not a reason: the version runs.
fn not_serving(status: &Status, digest: &str) -> Option<String> {
    if status.binary_sha256 != digest {
        return Some(format!(
            "the host reports version {}",
            short(&status.binary_sha256)
        ));
    }
    match status.loaded {
        Some(true) => {}
        Some(false) => return Some("the service is not loaded".into()),
        None if status.switching_from.is_some() => {
            return Some("the version switch is unfinished".into())
        }
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
        match &status.switching_from {
            Some(from) => format!(" (switch from {} not committed)", short(from)),
            None => String::new(),
        },
        if status.ready { "ready" } else { "not ready" },
        match (status.draining, status.drained_by_upgrade) {
            (true, true) => "drained by the upgrade",
            (true, false) => "draining",
            (false, _) => "not draining",
        },
    )
}

/// The resident decides what a worker must offer. `Ok`: the features the
/// health check requires. `Err`: why the upgrade must not start.
fn resident_requirement(new: &str, resident: ResidentBuild) -> Result<Vec<String>, String> {
    let offered = crate::protocol::session::offered_features();
    let required = crate::protocol::session::required_features();
    match resident {
        ResidentBuild::NotRunning => Ok(required),
        ResidentBuild::Unknown { reason } => Err(format!(
            "the running resident did not say which build it is ({reason})"
        )),
        ResidentBuild::Running {
            binary_sha256,
            features,
            required_features,
        } => {
            if binary_sha256 != new {
                return Err(format!(
                    "the running resident is build {}, this binary is {}",
                    short(&binary_sha256),
                    short(new)
                ));
            }
            if features != offered || required_features != required {
                return Err(
                    "the running resident negotiates other features than this binary".into(),
                );
            }
            Ok(required_features)
        }
    }
}

/// What the previous version must agree to when it is checked: of the
/// features the resident requires, those the version agreed to before the
/// upgrade. A version that was not seen serving is asked for none.
fn previous_requirement(required: &[String], agreed: Option<&[String]>) -> Vec<String> {
    required
        .iter()
        .filter(|feature| agreed.is_some_and(|agreed| agreed.contains(feature)))
        .cloned()
        .collect()
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
    fn request(
        &mut self,
        action: &'static str,
        digest: &str,
        timeout: Duration,
    ) -> Result<Status, ApiError> {
        let request = SwitchRequest {
            schema: 1,
            cluster: self.cluster.clone(),
            worker_id: WorkerId::parse(self.name).map_err(|e| invalid(e.to_string()))?,
            binary_sha256: digest.to_string(),
        };
        let body = serde_json::to_vec(&request)
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        self.helper(action, &body, timeout)
    }
    fn switch(&mut self, action: &'static str, digest: &str) -> Result<Status, ApiError> {
        self.request(action, digest, Duration::from_secs(STOP_WAIT_SECS + 300))
    }
    fn hello(&mut self, status: &Status, required: &[String]) -> Result<Vec<String>, String> {
        let binary = self
            .binary
            .clone()
            .or_else(|| status.bridge_binary.clone())
            .ok_or("the host did not report its bridge binary path")?;
        let hello = self
            .remote
            .bridge_hello_with(&self.target, &binary, &self.cluster, required)
            .map_err(|m| format!("the worker bridge did not complete hello: {m}"))?;
        if hello.worker.as_str() != self.name || hello.cluster != self.cluster {
            return Err("the worker bridge answered for a different worker or cluster".into());
        }
        Ok(hello.features)
    }
    /// `digest` is serving: the host says so and the bridge the resident
    /// uses answers `hello` as this worker in this cluster, agreeing to
    /// `required`.
    fn serving(
        &mut self,
        status: &Status,
        digest: &str,
        required: &[String],
    ) -> Result<(), String> {
        if let Some(why) = not_serving(status, digest) {
            return Err(why);
        }
        self.hello(status, required).map(|_| ())
    }
    /// Tell the host that `digest` has been checked. It forgets the version
    /// the switch began from and resumes a drain the upgrade made. `Ok`: the
    /// host's state after it, and what is left to say about the drain.
    /// `Err`: what is left for the operator.
    fn commit(&mut self, digest: &str) -> Result<(Status, &'static str), &'static str> {
        match self.request("commit", digest, Duration::from_secs(120)) {
            Ok(status) if !status.draining => Ok((status, "")),
            Ok(status) if !status.drained_by_upgrade => {
                Ok((status, "; it was draining before the upgrade and still is"))
            }
            _ => Err("; the commit did not finish, so it may still be drained by the upgrade: run workers upgrade again"),
        }
    }
    fn left(&mut self, digest: &str) -> &'static str {
        match self.commit(digest) {
            Ok((_, left)) | Err(left) => left,
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
    /// "rolled back and serving again", or what was last seen. The drain is
    /// released only with a version that passed its check.
    fn roll_back(
        &mut self,
        previous: Option<String>,
        reason: &str,
        required: &[String],
    ) -> ApiError {
        let name = self.name;
        let Some(previous) = previous else {
            let (code, seen) = self.last_observed();
            return ApiError::new(
                code,
                format!("upgrade of worker {name} failed ({reason}); no previous version is known, so nothing was rolled back; {seen}; run workers upgrade again"),
            );
        };
        let failure = match self.switch("rollback", &previous) {
            Ok(status) => match self.serving(&status, &previous, required) {
                Ok(()) => {
                    let left = self.left(&previous);
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
    binary: Option<&Path>,
    name: &str,
    resident: impl FnOnce(&Spec) -> ResidentBuild,
) -> Result<UpgradeResult, ApiError> {
    check_name(name)?;
    let registration = registered(env, name)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no such worker is registered"))?;
    if registration.ephemeral {
        return Err(invalid(EPHEMERAL_NO_SERVICE));
    }
    let cluster = spec
        .expected_cluster
        .clone()
        .ok_or_else(|| invalid("the coordinator has no cluster identity; run init"))?;
    let target = target_of(&registration.ssh)?;
    let digest_of = |path: &Path| {
        worker_service::sha256_file(path)
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
    };
    // What is sent: the build named with `--binary` (for a worker whose OS
    // or architecture is not the parent's), else this binary.
    let ship = binary.unwrap_or(exe);
    let new = digest_of(ship)?;
    // Before anything is sent to the host. The resident is compared with
    // this binary, whatever build goes to the worker.
    let required = resident_requirement(&digest_of(exe)?, resident(spec)).map_err(|why| {
        ApiError::new(
            ErrorCode::Conflict,
            format!("worker {name} was not upgraded: {why}; restart the resident so that it runs this binary, then run workers upgrade again (nothing was changed)"),
        )
    })?;
    // Staging writes only the bootstrap directory; the installed worker is untouched.
    ensure_staged(remote, &target, ship, &new, binary.is_some())?;
    let mut session = Session {
        remote,
        target,
        name,
        cluster,
        digest: new.clone(),
        binary: registration.binary.clone(),
    };
    let before = session.status()?;
    if before.switch_running {
        return Err(ApiError::new(
            ErrorCode::Conflict,
            format!("worker {name} was not upgraded: a version switch is running on its host; nothing was changed, run workers upgrade again when it has finished"),
        ));
    }
    let uncommitted = before.switching_from.is_some();
    if !uncommitted
        && before.binary_sha256 == new
        && session.serving(&before, &new, &required).is_ok()
    {
        if !before.drained_by_upgrade {
            return Ok(UpgradeResult {
                id: name.into(),
                outcome: Outcome::AlreadyCurrent,
                from: new.clone(),
                to: new,
                draining: before.draining,
                status: before,
            });
        }
        // An earlier run was stopped inside its commit: finish it.
        return match session.commit(&new) {
            Ok((status, _)) => Ok(UpgradeResult {
                id: name.into(),
                outcome: Outcome::AlreadyCurrent,
                from: new.clone(),
                to: new,
                draining: status.draining,
                status,
            }),
            Err(left) => Err(ApiError::new(
                ErrorCode::Conflict,
                format!(
                    "worker {name} has version {} and passed its check{left}",
                    short(&new)
                ),
            )),
        };
    }
    // The version is installed but not serving: the switch below starts it
    // (it changes nothing else) and checks it like a new one.
    let previous = previous_version(&before, &new);
    // What the previous version agrees to, while it can still be asked.
    let agreed = (before.binary_sha256 != new
        && not_serving(&before, &before.binary_sha256).is_none())
    .then(|| session.hello(&before, &[]).ok())
    .flatten();
    let previous_required = previous_requirement(&required, agreed.as_deref());
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
            if after.switch_running {
                // The helper is still at work: undoing it now would run
                // two switches against each other.
                return Err(ApiError::new(
                    ErrorCode::CommunicationUnknown,
                    format!(
                        "upgrade of worker {name}: the host did not answer ({}) and a version switch is still running there; nothing was rolled back or resumed, run workers upgrade again when it has finished; {}",
                        error.message,
                        observed(&after)
                    ),
                ));
            }
            if let Some(previous) = previous.as_ref().filter(|previous| {
                after.switching_from.is_none()
                    && session
                        .serving(&after, previous, &previous_required)
                        .is_ok()
            }) {
                let left = session.left(previous);
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
                return Err(session.roll_back(previous, &error.message, &previous_required));
            }
            // Only the answer was lost: the host shows the new version.
            after
        }
    };
    let reason = match session.serving(&switched, &new, &required) {
        Ok(()) => {
            // Commit, and with it resume, before anything else can fail.
            let (status, _) = session.commit(&new).map_err(|left| {
                ApiError::new(
                    ErrorCode::Conflict,
                    format!(
                        "worker {name} was upgraded to version {} and passed its check{left}",
                        short(&new)
                    ),
                )
            })?;
            if session.binary.is_none() {
                if let Some(binary) = &status.bridge_binary {
                    set_binary(env, name, binary).map_err(|error| {
                        ApiError::new(
                            error.code,
                            format!(
                                "worker {name} was upgraded to version {} and {}, but its registration could not be updated: {}",
                                short(&new),
                                if status.draining {
                                    "is still draining as it was before"
                                } else {
                                    "is not draining"
                                },
                                error.message
                            ),
                        )
                    })?;
                }
            }
            return Ok(UpgradeResult {
                id: name.into(),
                outcome: Outcome::Upgraded,
                from: previous.unwrap_or_else(|| new.clone()),
                to: new,
                draining: status.draining,
                status,
            });
        }
        Err(why) => format!("the new version did not pass its check: {why}"),
    };
    Err(session.roll_back(previous, &reason, &previous_required))
}

fn describe(name: &str, result: &UpgradeResult) -> String {
    let done = match result.outcome {
        Outcome::AlreadyCurrent => {
            format!("worker {name} already has version {}", short(&result.to))
        }
        Outcome::Upgraded => format!(
            "worker {name} upgraded from version {} to {}",
            short(&result.from),
            short(&result.to)
        ),
    };
    if result.draining {
        format!("{done}; it was draining before and still is")
    } else {
        done
    }
}

pub(super) fn command(
    name: &str,
    binary: Option<&Path>,
    json: bool,
    env: &BTreeMap<String, String>,
    exe: std::io::Result<std::path::PathBuf>,
) -> i32 {
    let result = exe
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))
        .and_then(|exe| {
            let spec = crate::service::context::runtime_spec(env)?;
            upgrade(
                &mut SshRemote::new(&spec.cluster_key_path),
                env,
                &spec,
                &exe,
                binary,
                name,
                resident_build,
            )
        });
    let text = result
        .as_ref()
        .map(|r| describe(name, r))
        .unwrap_or_default();
    emit(
        json,
        result.map(|r| serde_json::to_value(r).expect("serializes")),
        || text,
    )
}

#[cfg(test)]
mod tests;
