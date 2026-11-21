//! Sequential SSH rotation. Only key metadata is kept in the resume record.
use super::{internal, load_spec, workers};
use crate::{
    error::{ApiError, ErrorCode},
    group,
    ids::{ClusterId, WorkerId},
    protocol::ssh::SshTarget,
    secrets::key::{self, ClusterKey},
    service::{
        bootstrap::Spec,
        resident,
        workers::{Remote, SshRemote},
    },
    storage::{CoordinatorStore, KeyGeneration, ProcessLock, WorkerStore},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) const HELPER: &str = "exec belt group-rotate-helper";
const LIMIT: u64 = 65536;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Generation {
    number: u64,
    fingerprint: String,
}

// Deliberately no Debug: the distribution request contains the key.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    cluster: ClusterId,
    member: WorkerId,
    action: Action,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
enum Action {
    Collect,
    SendKey {
        key: Vec<u8>,
    },
    Distribute {
        generation: Generation,
        key: Vec<u8>,
    },
    Switch {
        generation: Generation,
    },
}

fn conflict(message: impl Into<String>) -> ApiError {
    ApiError::new(ErrorCode::Conflict, message)
}
fn earlier() -> ApiError {
    conflict("a different next fingerprint is present; finish the earlier rotation")
}
fn publish(path: &Path, bytes: &[u8]) -> Result<(), ApiError> {
    let parent = path.parent().ok_or_else(|| internal("missing parent"))?;
    let mut file = tempfile::NamedTempFile::new_in(parent).map_err(internal)?;
    file.write_all(bytes).map_err(internal)?;
    file.as_file().sync_all().map_err(internal)?;
    file.persist_noclobber(path)
        .map_err(|e| internal(e.error))?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(internal)
}
fn store(spec: &Spec) -> Result<WorkerStore, ApiError> {
    WorkerStore::open(&spec.worker_state_dir, &spec.worker_id).map_err(internal)
}
fn stage(spec: &Spec, generation: &Generation, incoming: &ClusterKey) -> Result<(), ApiError> {
    if crate::worker::rekey::pending(&spec.worker_state_dir) {
        return Err(conflict("send-key is pending; wait for its identity reset"));
    }
    if group::fingerprint(incoming) != generation.fingerprint || generation.number == 0 {
        return Err(conflict("invalid rotation generation"));
    }
    let keys = group::load_keys(&spec.cluster_key_path).map_err(internal)?;
    let store = store(spec)?;
    store
        .observe_group_key(&group::fingerprint(&keys.current))
        .map_err(internal)?;
    // Refuse foreign next keys even if the requested key is already current.
    if keys
        .next
        .as_ref()
        .is_some_and(|k| group::fingerprint(k) != generation.fingerprint)
        || store
            .group_keys()
            .map_err(internal)?
            .1
            .is_some_and(|g| g.fingerprint != generation.fingerprint)
    {
        return Err(earlier());
    }
    if group::fingerprint(&keys.current) == generation.fingerprint {
        return Ok(());
    }
    store
        .set_next_group_key(&KeyGeneration {
            generation: generation.number,
            fingerprint: generation.fingerprint.clone(),
        })
        .map_err(|_| earlier())?;
    if keys.next.is_none() {
        let text: String = incoming
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        publish(
            &group::next_key_path(&spec.cluster_key_path),
            format!("{text}\n").as_bytes(),
        )?;
    }
    Ok(())
}
fn switch(spec: &Spec, generation: &Generation) -> Result<(), ApiError> {
    if crate::worker::rekey::pending(&spec.worker_state_dir) {
        return Err(conflict("send-key is pending; wait for its identity reset"));
    }
    let keys = group::load_keys(&spec.cluster_key_path).map_err(internal)?;
    if let Some(next) = keys.next {
        if group::fingerprint(&next) != generation.fingerprint {
            return Err(earlier());
        }
        group::switch_to_next(&spec.cluster_key_path).map_err(internal)?;
        std::fs::File::open(spec.cluster_key_path.parent().unwrap())
            .and_then(|f| f.sync_all())
            .map_err(internal)?;
    } else if group::fingerprint(&keys.current) != generation.fingerprint {
        return Err(conflict(
            "requested next key is missing; rerun distribution",
        ));
    }
    store(spec)?
        .observe_group_key(&generation.fingerprint)
        .map_err(internal)?;
    // Existing agent ticks and outbound sessions observe the key file, close
    // connections proven with the discarded key, and reconnect without stopping work.
    Ok(())
}

pub(crate) fn accept(
    env: &BTreeMap<String, String>,
    spec: &Spec,
    request: Request,
) -> Result<Value, ApiError> {
    if spec.expected_cluster.as_ref() != Some(&request.cluster) || spec.worker_id != request.member
    {
        return Err(conflict("rotation target group or member differs"));
    }
    let _lock = ProcessLock::acquire(&spec.worker_state_dir, "group-rotation-key.lock")
        .map_err(internal)?;
    match request.action {
        Action::SendKey { key } => {
            let _command =
                ProcessLock::acquire(&spec.worker_state_dir, "group-rotation-command.lock")
                    .map_err(internal)?;
            let key = ClusterKey::from_bytes(key).map_err(internal)?;
            let state = &spec.worker_state_dir;
            let database = store(spec)?;
            // If no agent owns the state, the helper drives supervisor recovery.
            let offline = match ProcessLock::acquire(state, "agent.lock") {
                Ok(lock) => Some(lock),
                Err(crate::storage::StorageError::Locked(_)) => None,
                Err(error) => return Err(internal(error)),
            };
            // Resume an interrupted handoff first. Once done, this invocation
            // still creates its own fresh identity, even for the same key.
            let wait = || -> Result<(), ApiError> {
                let deadline = std::time::Instant::now() + Duration::from_secs(50);
                while crate::worker::rekey::pending(state) {
                    if offline.is_some() {
                        crate::worker::agent::interrupt_offline(
                            state,
                            &spec.worker_id,
                            &database,
                            deadline,
                        )?;
                        if std::time::Instant::now() >= deadline {
                            return Err(conflict("send-key is still stopping old attempts; rerun send-key to continue"));
                        }
                        if database.list_nonterminal().map_err(internal)?.is_empty() {
                            crate::worker::rekey::finish(state, &spec.cluster_key_path, &database)
                                .map_err(internal)?;
                            break;
                        }
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(conflict("send-key is still stopping old attempts; resume with send-key or start the resident"));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Ok(())
            };
            wait()?;
            let instance = crate::worker::rekey::stage(state, &key).map_err(internal)?;
            wait()?;
            let coordinator_id = if spec.state_dir.join("coordinator.db").exists() {
                Some(
                    CoordinatorStore::membership_writer(&spec.state_dir)
                        .map_err(internal)?
                        .coordinator_id()
                        .clone(),
                )
            } else {
                None
            };
            Ok(
                json!({"fingerprint":group::fingerprint(&key), "instance":instance, "identity_regenerated":true, "coordinator_id":coordinator_id}),
            )
        }
        Action::Collect => Ok(
            json!({"members":workers::registrations(env)?.into_iter().map(|w| w.id).collect::<Vec<_>>()}),
        ),
        Action::Distribute { generation, key } => {
            stage(
                spec,
                &generation,
                &ClusterKey::from_bytes(key).map_err(internal)?,
            )?;
            Ok(json!({"fingerprint":generation.fingerprint}))
        }
        Action::Switch { generation } => {
            switch(spec, &generation)?;
            Ok(json!({"fingerprint":generation.fingerprint}))
        }
    }
}
fn call(
    remote: &mut impl Remote,
    entry: &workers::Registered,
    cluster: &ClusterId,
    action: Action,
) -> Result<Value, ApiError> {
    let result = (|| {
        let request = Request {
            cluster: cluster.clone(),
            member: WorkerId::parse(&entry.id).map_err(internal)?,
            action,
        };
        let body = serde_json::to_vec(&request).map_err(internal)?;
        let target = SshTarget::parse(&entry.ssh).map_err(internal)?;
        let shell = match &entry.binary {
            Some(binary) => {
                crate::protocol::ssh::RemoteCommand::worker_stdio(binary).map_err(internal)?;
                format!(
                    "exec {} group-rotate-helper",
                    crate::protocol::ssh::quote(binary)
                )
            }
            None => HELPER.into(),
        };
        let out = remote
            .run(&target, &shell, &body, Duration::from_secs(60))
            .map_err(|_| conflict("SSH unavailable; rerun or use --skip"))?;
        let reply: Value = serde_json::from_slice(&out.stdout)
            .map_err(|_| conflict("SSH helper reply unavailable; rerun or use --skip"))?;
        if out.code != Some(0) || reply["ok"] != true {
            return Err(serde_json::from_value::<ApiError>(reply["error"].clone())
                .unwrap_or_else(|_| conflict("rotation helper failed")));
        }
        Ok(reply["result"].clone())
    })();
    result
        .map_err(|e: ApiError| ApiError::new(e.code, format!("member {}: {}", entry.id, e.message)))
}

pub(crate) fn send_key(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    member: &str,
) -> Result<Value, ApiError> {
    let _lock = ProcessLock::acquire(&spec.worker_state_dir, "group-rotation-command.lock")
        .map_err(internal)?;
    let entry = workers::registrations(env)?
        .into_iter()
        .find(|w| w.id == member)
        .ok_or_else(|| {
            conflict(format!(
                "member {member} is not in this host's [[workers]] list"
            ))
        })?;
    let cluster = spec
        .expected_cluster
        .as_ref()
        .ok_or_else(|| conflict("group missing"))?;
    let key = group::load_keys(&spec.cluster_key_path)
        .map_err(internal)?
        .current;
    let reply = call(
        remote,
        &entry,
        cluster,
        Action::SendKey {
            key: key.as_bytes().to_vec(),
        },
    )?;
    if let Some(id) = reply.get("coordinator_id").filter(|v| !v.is_null()) {
        let id = serde_json::from_value(id.clone()).map_err(internal)?;
        store(spec)?
            .for_member(&id)
            .replace_member_pin(member, crate::platform::wall_ms())
            .map_err(internal)?;
    }
    Ok(reply)
}

pub fn send_key_command(member: &str, json_output: bool) -> i32 {
    let env = environment();
    let result = load_spec(&env).and_then(|spec| {
        send_key(
            &mut SshRemote::new(&spec.cluster_key_path),
            &env,
            &spec,
            member,
        )
    });
    workers::emit(json_output, result, || {
        format!("sent current group key to {member}; worker identity regenerated (old live attempts stopped)")
    })
}

pub(crate) fn rotate(
    remote: &mut impl Remote,
    env: &BTreeMap<String, String>,
    spec: &Spec,
    remove: &[String],
    skip: &[String],
) -> Result<Value, ApiError> {
    let _lock = ProcessLock::acquire(&spec.worker_state_dir, "group-rotation-command.lock")
        .map_err(internal)?;
    let entries = workers::registrations(env)?;
    let cluster = spec
        .expected_cluster
        .as_ref()
        .ok_or_else(|| conflict("group missing"))?;
    for name in remove.iter().chain(skip) {
        WorkerId::parse(name).map_err(internal)?;
        if name == spec.worker_id.as_str() {
            return Err(conflict(format!(
                "cannot remove or skip local member {name}"
            )));
        }
        if skip.contains(name) && !remove.contains(name) && !entries.iter().any(|w| &w.id == name) {
            return Err(conflict(format!("unknown member {name}")));
        }
    }
    let peers: Vec<_> = entries
        .iter()
        .filter(|w| {
            w.id != spec.worker_id.as_str() && !remove.contains(&w.id) && !skip.contains(&w.id)
        })
        .collect();
    for peer in &peers {
        let reply = call(remote, peer, cluster, Action::Collect)?;
        let names: Vec<String> =
            serde_json::from_value(reply["members"].clone()).map_err(internal)?;
        for name in names {
            if name != spec.worker_id.as_str()
                && !entries.iter().any(|w| w.id == name)
                && !remove.contains(&name)
            {
                return Err(conflict(format!(
                    "unknown member {name} in {}'s workers list; nothing distributed",
                    peer.id
                )));
            }
        }
    }
    let record = spec.worker_state_dir.join("group-rotation.json");
    let (generation, next) = {
        let _key_lock = ProcessLock::acquire(&spec.worker_state_dir, "group-rotation-key.lock")
            .map_err(internal)?;
        let keys = group::load_keys(&spec.cluster_key_path).map_err(internal)?;
        let generation: Generation = match resident::secure_file(&record, false) {
            Ok(file) => serde_json::from_reader(file.take(LIMIT)).map_err(internal)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let store = store(spec)?;
                let current = store
                    .observe_group_key(&group::fingerprint(&keys.current))
                    .map_err(internal)?;
                let next = match keys.next {
                    Some(k) => k,
                    None => key::init(&group::next_key_path(&spec.cluster_key_path))
                        .map_err(internal)?,
                };
                let recorded = store.group_keys().map_err(internal)?.1;
                let generation = Generation {
                    number: recorded.map_or(current.generation + 1, |g| g.generation),
                    fingerprint: group::fingerprint(&next),
                };
                publish(&record, &serde_json::to_vec(&generation).map_err(internal)?)?;
                generation
            }
            Err(e) => return Err(internal(e)),
        };
        let keys = group::load_keys(&spec.cluster_key_path).map_err(internal)?;
        let next = keys
            .by_fingerprint(&generation.fingerprint)
            .ok_or_else(|| conflict("rotation key missing; restore it before resuming"))?
            .clone();
        stage(spec, &generation, &next)?;
        (generation, next)
    };
    let guidance = guidance(spec, &entries, remove);
    if !remove.is_empty() {
        record_removals(spec, remove)?;
        workers::remove_registrations(env, remove)?;
        workers::reload_resident(spec);
    }
    let run = (|| {
        for peer in &peers {
            call(
                remote,
                peer,
                cluster,
                Action::Distribute {
                    generation: generation.clone(),
                    key: next.as_bytes().to_vec(),
                },
            )?;
        }
        for peer in &peers {
            call(
                remote,
                peer,
                cluster,
                Action::Switch {
                    generation: generation.clone(),
                },
            )?;
        }
        let _key_lock = ProcessLock::acquire(&spec.worker_state_dir, "group-rotation-key.lock")
            .map_err(internal)?;
        switch(spec, &generation)?;
        std::fs::remove_file(&record).map_err(internal)?;
        Ok(
            json!({"fingerprint":generation.fingerprint,"generation":generation.number,"guidance":guidance}),
        )
    })();
    run.map_err(|e: ApiError| {
        ApiError::new(e.code, format!("{}\n{}", e.message, guidance.join("\n")))
    })
}
/// Receiving-only members have no coordinator database yet. Keep their small
/// removal record beside worker state; never create a coordinator identity as
/// a side effect of key rotation. Initialized members use the existing table.
fn record_removals(spec: &Spec, names: &[String]) -> Result<(), ApiError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(internal)?
        .as_millis() as u64;
    if spec
        .state_dir
        .join("coordinator.db")
        .try_exists()
        .map_err(internal)?
    {
        let store = CoordinatorStore::membership_writer(&spec.state_dir).map_err(internal)?;
        for name in names {
            store
                .record_removed_member(&WorkerId::parse(name).map_err(internal)?, now)
                .map_err(internal)?;
        }
        return Ok(());
    }
    let path = spec.worker_state_dir.join("removed-members.json");
    let mut removed: BTreeMap<String, u64> = match resident::secure_file(&path, false) {
        Ok(file) => serde_json::from_reader(file.take(LIMIT)).map_err(internal)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
        Err(e) => return Err(internal(e)),
    };
    for name in names {
        removed.entry(name.clone()).or_insert(now);
    }
    let mut temporary =
        tempfile::NamedTempFile::new_in(&spec.worker_state_dir).map_err(internal)?;
    serde_json::to_writer(&mut temporary, &removed).map_err(internal)?;
    temporary.as_file().sync_all().map_err(internal)?;
    temporary.persist(&path).map_err(|e| internal(e.error))?;
    std::fs::File::open(&spec.worker_state_dir)
        .and_then(|f| f.sync_all())
        .map_err(internal)
}

fn guidance(spec: &Spec, entries: &[workers::Registered], remove: &[String]) -> Vec<String> {
    let mut lines = Vec::new();
    for name in remove {
        for member in std::iter::once(spec.worker_id.as_str()).chain(
            entries
                .iter()
                .filter(|w| !remove.contains(&w.id) && w.id != spec.worker_id.as_str())
                .map(|w| w.id.as_str()),
        ) {
            lines.push(format!("On {member}, identify {name}'s SSH public keys, then manually delete their matching lines from the belt account's ~/.ssh/authorized_keys; belt does not edit authorized_keys."));
        }
        lines.push(format!("After one lease period (lease_seconds) plus the TERM stop grace since last contact, run belt workers forget {name} on this member; other members with work there must do the same."));
    }
    lines
}
fn environment() -> BTreeMap<String, String> {
    std::env::vars().collect()
}
pub fn command(remove: &[String], skip: &[String], json_output: bool) -> i32 {
    let env = environment();
    let result = load_spec(&env).and_then(|spec| {
        rotate(
            &mut SshRemote::new(&spec.cluster_key_path),
            &env,
            &spec,
            remove,
            skip,
        )
    });
    let text = result
        .as_ref()
        .map(|v| {
            format!(
                "group key rotated: {}\n{}",
                v["fingerprint"].as_str().unwrap_or_default(),
                v["guidance"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        })
        .unwrap_or_default();
    workers::emit(json_output, result, || text)
}
pub fn helper_command() -> i32 {
    let result = (|| {
        let mut body = Vec::new();
        std::io::stdin()
            .take(LIMIT + 1)
            .read_to_end(&mut body)
            .map_err(internal)?;
        if body.len() as u64 > LIMIT {
            return Err(conflict("rotation request too large"));
        }
        // Do not echo serde errors: they can contain secret input.
        let request =
            serde_json::from_slice(&body).map_err(|_| conflict("invalid rotation request"))?;
        let env = environment();
        accept(&env, &load_spec(&env)?, request)
    })();
    workers::emit(true, result, String::new)
}
