//! Join is two ordinary enrollments; no resident protocol or key transport of its own.
use super::{internal, workers};
use crate::{
    error::{ApiError, ErrorCode},
    ids::WorkerId,
    protocol::ssh::SshTarget,
    service::{
        bootstrap::Spec,
        workers::{AddOptions, Remote, SshRemote},
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Read, path::Path, time::Duration};

pub(crate) const HELPER: &str = r#"case "${XDG_CONFIG_HOME:-}" in /*) belt_config=$XDG_CONFIG_HOME ;; *) belt_config="$HOME/.config" ;; esac; if [ -x "$belt_config/belt/bin/resident/belt" ]; then exec "$belt_config/belt/bin/resident/belt" group-join-helper; elif [ -x "$belt_config/belt/bin/current/belt" ]; then exec "$belt_config/belt/bin/current/belt" group-join-helper; else exec belt group-join-helper; fi"#;
const LIMIT: u64 = 8192;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub name: WorkerId,
    pub ssh: String,
    pub move_group: bool,
}

fn initialized(env: &BTreeMap<String, String>) -> Result<Spec, ApiError> {
    let spec = crate::service::context::runtime_spec(env).map_err(|_| {
        ApiError::new(
            ErrorCode::Conflict,
            "group join requires an initialized host; run belt init first",
        )
    })?;
    crate::service::init::validate_managed(&spec).map_err(|e| {
        ApiError::new(
            ErrorCode::Conflict,
            format!("group join requires a completed belt init: {e}"),
        )
    })?;
    Ok(spec)
}

fn default_ssh(user: &str, hostname: &str) -> Result<String, ApiError> {
    let destination = format!("{user}@{hostname}");
    SshTarget::parse(&destination)
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
    Ok(destination)
}

pub(crate) fn accept(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    exe: &Path,
    request: Request,
) -> Result<WorkerId, ApiError> {
    let spec = initialized(env)?;
    workers::add(
        remote,
        env,
        &spec,
        exe,
        request.name.as_str(),
        Some(&request.ssh),
        AddOptions {
            move_group: request.move_group,
            join_singleton: true,
            ..Default::default()
        },
        workers::reload_resident,
    )?;
    Ok(spec.worker_id)
}

/// The first enrollment may already have succeeded even if its SSH reply is lost.
/// Never roll it back; retrying repeats the same-group enrollment.
pub fn join(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    exe: &Path,
    existing: &str,
    ssh: &str,
    move_group: bool,
) -> Result<serde_json::Value, ApiError> {
    let spec = initialized(env)?;
    let target = SshTarget::parse(existing)
        .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
    SshTarget::parse(ssh).map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
    let request = Request {
        name: spec.worker_id,
        ssh: ssh.into(),
        move_group,
    };
    let body = serde_json::to_vec(&request).map_err(internal)?;
    if body.len() as u64 > LIMIT {
        return Err(ApiError::new(
            ErrorCode::InvalidInput,
            "group join request too large",
        ));
    }
    let mut step_one = || -> Result<WorkerId, ApiError> {
        let output = remote
            .run(&target, HELPER, &body, Duration::from_secs(360))
            .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e))?;
        let reply: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|_| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                "join helper reply unreadable; enrollment outcome unknown",
            )
        })?;
        if output.code != Some(0) || reply["ok"] != true {
            return Err(
                serde_json::from_value(reply["error"].clone()).unwrap_or_else(|_| {
                    ApiError::new(
                        ErrorCode::CommunicationUnknown,
                        "join helper failed; enrollment outcome unknown",
                    )
                }),
            );
        }
        serde_json::from_value(reply["result"]["name"].clone()).map_err(internal)
    };
    let name = step_one().map_err(|e| ApiError::new(e.code, format!("group join step 1: {}; other members must be able to reach this host at --ssh {ssh}; rerun group join to finish an interrupted enrollment", e.message)))?;
    let mut step_two = || {
        // Step one changed the retained group and key. Never use the old spec here.
        let spec = initialized(env)?;
        workers::add(
            remote,
            env,
            &spec,
            exe,
            name.as_str(),
            Some(existing),
            AddOptions::default(),
            workers::reload_resident,
        )
    };
    step_two().map_err(|e| ApiError::new(e.code, format!("this host is now in the existing member's group, but step 2 could not finish registering {name} locally: {}; rerun belt group join to finish; step 1 was not undone", e.message)))?;
    Ok(serde_json::json!({"member":name,"joined":true}))
}

fn environment() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect()
}

pub fn command(existing: &str, ssh: Option<&str>, move_group: bool, json: bool) -> i32 {
    let env = environment();
    let result = (|| {
        let spec = initialized(&env)?;
        let ssh = match ssh {
            Some(ssh) => ssh.to_owned(),
            None => default_ssh(
                &crate::platform::current_username().map_err(internal)?,
                &crate::platform::hostname().map_err(internal)?,
            )?,
        };
        join(
            &mut SshRemote::new(&spec.cluster_key_path),
            &env,
            &std::env::current_exe().map_err(internal)?,
            existing,
            &ssh,
            move_group,
        )
    })();
    workers::emit(json, result, || {
        "joined group; both members are registered".into()
    })
}

pub fn helper_command() -> i32 {
    let result = (|| {
        let mut body = Vec::new();
        std::io::stdin()
            .take(LIMIT + 1)
            .read_to_end(&mut body)
            .map_err(internal)?;
        if body.len() as u64 > LIMIT {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "group join request too large",
            ));
        }
        let request: Request = serde_json::from_slice(&body).map_err(internal)?;
        let env = environment();
        let spec = initialized(&env)?;
        let name = accept(
            &mut SshRemote::new(&spec.cluster_key_path),
            &env,
            &std::env::current_exe().map_err(internal)?,
            request,
        )?;
        Ok(serde_json::json!({"name":name}))
    })();
    workers::emit(true, result, String::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ssh_default_keeps_full_hostname() {
        assert_eq!(
            default_ssh("alice.dev", "mini.example.org").unwrap(),
            "alice.dev@mini.example.org"
        );
        assert!(default_ssh("alice", "bad host").is_err());
    }
}
