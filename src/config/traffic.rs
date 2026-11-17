//! The task-traffic-controller scope a worker uses: the one the
//! `task-traffic-controller` CLI of the same OS user resolves, so both share
//! one ledger, one lock, one set of leases and one configuration (spec §14).
//!
//! The rule itself is the library's own (`Paths::from_env`, the function the
//! CLI calls), so it is not written a second time here. What this module adds
//! is a check before the call. The environment arrives as a map argument;
//! this module never reads the process environment.

use crate::ids::AttemptId;
use crate::supervisor::lock::SupervisorLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use task_traffic_controller::ledger::Ledger;
use task_traffic_controller::{managed, Context, Paths};

/// What the CLI calls its directory under the state and config homes.
pub const DIR: &str = "task-traffic-controller";
/// The variable that moves the CLI's state directory. A step that changed it
/// would run its nested `task-traffic-controller` against another ledger.
pub const STATE_HOME_ENV: &str = "XDG_STATE_HOME";
/// The variable that moves the CLI's configuration directory.
pub const CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";
/// How the CLI treats a `task-traffic-controller exec` called from inside a
/// command that is admitted already.
pub const NESTED_ENV: &str = "TASK_TRAFFIC_CONTROLLER_NESTED";
/// Run under the command's own entry, with no entry and no admission of its
/// own (spec chapter 14: the nested call inherits the lease of its step and
/// is not charged a second time). The CLI's default since 0.9 is `admit`.
pub const NESTED_INHERIT: &str = "inherit";
/// Where a worker kept its own ledger before it shared the CLI's.
pub const LEGACY_DIR: &str = "traffic";
/// The file a worker that waits for another scope to drain keeps in its
/// state directory, for `worker-service status`.
pub const HOLD_FILE: &str = "traffic-hold.json";
/// The first and the longest pause between two looks at a scope that is not
/// drained.
pub const HOLD_FIRST_MS: u64 = 1_000;
pub const HOLD_MAX_MS: u64 = 30_000;
/// A hold record older than this was left by a worker that is gone: a
/// waiting worker writes it again at every look.
pub const HOLD_STALE_MS: u64 = 3 * HOLD_MAX_MS;

/// The variables that tell a command which scope its own
/// `task-traffic-controller` uses and how it nests. They are the worker's:
/// neither a profile nor a step sets them, and they are no part of the
/// toolchain environment (profile fingerprint, execution binding).
pub fn is_step_env(key: &str) -> bool {
    key == STATE_HOME_ENV || key == CONFIG_HOME_ENV || key == NESTED_ENV
}

/// What every command of a worker started with `worker_env` gets of them:
/// the worker's own `XDG_STATE_HOME` and `XDG_CONFIG_HOME`, each only when
/// the worker has it, and the nesting mode.
pub fn step_env(worker_env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut env: BTreeMap<String, String> = [STATE_HOME_ENV, CONFIG_HOME_ENV]
        .into_iter()
        .filter_map(|key| Some((key.to_string(), worker_env.get(key)?.clone())))
        .collect();
    env.insert(NESTED_ENV.into(), NESTED_INHERIT.into());
    env
}

/// Make `env` carry exactly `scope`'s values of the scope variables.
pub fn apply_step_env(env: &mut BTreeMap<String, String>, scope: &BTreeMap<String, String>) {
    env.retain(|key, _| !is_step_env(key));
    env.extend(scope.iter().map(|(k, v)| (k.clone(), v.clone())));
}

/// The digest of an effective configuration (the policy of its scope).
/// `None` only when the configuration cannot be serialized.
pub fn policy_digest(config: &task_traffic_controller::config::Config) -> Option<String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(config).ok()?;
    Some(format!("sha256:{:x}", Sha256::digest(bytes)))
}

/// The state directory and the configuration file of one scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub state_dir: PathBuf,
    pub config_file: PathBuf,
}

/// The CLI's rule on `env` (`Paths::from_env`): `$XDG_STATE_HOME/` else
/// `$HOME/.local/state/`, then `task-traffic-controller`; the configuration
/// is `$XDG_CONFIG_HOME/task-traffic-controller/config.toml`, else under
/// `$HOME/.config`. As in the CLI an empty variable counts as unset.
///
/// Stricter than the CLI in two places, checked before the call, because a
/// worker and the commands it starts must resolve the same directory whatever
/// their working directory: a relative value is refused (the CLI would
/// resolve it against its own working directory), and a missing `HOME` is
/// refused (the CLI falls back to `/`).
pub fn resolve(env: &BTreeMap<String, String>) -> Result<Scope, String> {
    let get = |var: &str| env.get(var).filter(|v| !v.is_empty());
    let state = get(STATE_HOME_ENV);
    let config = get(CONFIG_HOME_ENV);
    let home = get("HOME");
    // HOME decides only what an XDG variable leaves open.
    for (var, value, needs_home) in [
        (STATE_HOME_ENV, state, state.is_none()),
        (CONFIG_HOME_ENV, config, config.is_none()),
    ] {
        if needs_home && home.is_none() {
            return Err(format!(
                "cannot locate the task-traffic-controller scope: set {var} or HOME"
            ));
        }
        if let Some(value) = value {
            require_absolute(var, value)?;
        }
    }
    if let Some(home) = home {
        if state.is_none() || config.is_none() {
            require_absolute("HOME", home)?;
        }
    }
    let paths = Paths::from_env(
        home.map(OsStr::new),
        state.map(OsStr::new),
        config.map(OsStr::new),
    );
    Ok(Scope {
        state_dir: paths.state_dir().to_path_buf(),
        config_file: paths.config_file().to_path_buf(),
    })
}

fn require_absolute(name: &str, value: &str) -> Result<(), String> {
    if Path::new(value).is_absolute() {
        return Ok(());
    }
    Err(format!(
        "{name} must be an absolute path to share one task-traffic-controller scope, not {value:?}"
    ))
}

/// Whether a worker may start serving in `scope`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hold {
    /// Nothing of this worker runs in another scope.
    Clear,
    /// Something still does, and ends without anybody's help: the worker
    /// waits. The text says what and where.
    Draining(String),
    /// The former scope cannot be read. Waiting does not change that: the
    /// text says what a person has to do.
    Unreadable(String),
}

impl Hold {
    pub fn message(&self) -> Option<&str> {
        match self {
            Hold::Clear => None,
            Hold::Draining(m) | Hold::Unreadable(m) => Some(m.as_str()),
        }
    }
    /// The word `worker-service status` reports.
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            Hold::Clear => None,
            Hold::Draining(_) => Some("waiting_for_previous_scope"),
            Hold::Unreadable(_) => Some("previous_scope_unreadable"),
        }
    }
}

/// What keeps the worker of `state_dir` from serving in `scope`: commands
/// that are still live in its former scope (`<state>/traffic`), or a living
/// supervisor whose stored spec names another scope than `scope` (the former
/// one, or the one of an environment that has changed since). Their budget
/// is counted in a ledger the new commands would not see.
///
/// Nothing is moved or removed. Only living supervisors count: an attempt
/// that was accepted and not started is given the new scope when it is
/// started (`SuperviseSpec::rescope`), and one whose supervisor is gone is
/// recovered from its stored spec by the agent, which never starts a command
/// for it again.
pub fn hold(state_dir: &Path, scope: &Path) -> Hold {
    match legacy_hold(&state_dir.join(LEGACY_DIR), scope) {
        Hold::Clear => {}
        held => return held,
    }
    let running = foreign_supervisors(state_dir, scope);
    if running.is_empty() {
        return Hold::Clear;
    }
    Hold::Draining(format!(
        "attempts started in another task-traffic-controller scope are still running ({}). \
         The worker now uses {}; it starts serving when they have ended",
        running.join(", "),
        scope.display()
    ))
}

/// The former scope of a worker (`<state>/traffic`), while it still holds a
/// live command. The history is not carried over (a cold start).
///
/// Live is whatever the reaper would keep: a held lease, or a managed
/// command whose processes may still run. A ledger that cannot be read is
/// never taken as empty.
pub fn legacy_hold(legacy: &Path, scope: &Path) -> Hold {
    match std::fs::symlink_metadata(legacy) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Hold::Clear,
        Err(e) => return unreadable(legacy, scope, &format!("it cannot be inspected ({e})")),
    }
    let ctx = Context::new(Paths::new(legacy, legacy.join("config.toml")));
    let ledger = match Ledger::load(&ctx.paths.ledger_file()) {
        Ok(ledger) => ledger,
        Err(e) => return unreadable(legacy, scope, &format!("its ledger cannot be read ({e})")),
    };
    let live: Vec<&str> = ledger
        .entries
        .iter()
        .filter(|e| !managed::assess(&ctx, &e.id).verdict.is_release())
        .map(|e| e.id.as_str())
        .collect();
    if live.is_empty() {
        return Hold::Clear;
    }
    Hold::Draining(format!(
        "the worker's previous task-traffic-controller scope {} is not drained: \
         it still holds live commands ({}). The worker now shares {} with the \
         task-traffic-controller CLI; it starts serving when those commands have finished",
        legacy.display(),
        live.join(", "),
        scope.display()
    ))
}

fn unreadable(legacy: &Path, scope: &Path, why: &str) -> Hold {
    Hold::Unreadable(format!(
        "the worker's previous task-traffic-controller scope {} is not drained: {why}. \
         The worker now shares {} with the task-traffic-controller CLI and does not serve \
         while it cannot tell whether a command of the previous scope still runs. Waiting \
         does not repair this: make sure no command the worker started is still running, \
         move {} aside (for example to {}.unreadable), and the worker starts serving at \
         its next look",
        legacy.display(),
        scope.display(),
        legacy.display(),
        legacy.display()
    ))
}

/// The two fields of a stored supervisor spec that say whose it is and where
/// its commands are admitted.
#[derive(Deserialize)]
struct StoredScope {
    attempt_id: AttemptId,
    traffic_state_dir: PathBuf,
}

/// The attempts and setup builders of `state_dir` whose supervisor lives and
/// was started for another scope than `scope`. The supervisor lock is taken
/// for a moment, as the agent does: the caller is the only owner of the
/// state directory (it holds the agent lock), so no supervisor is being
/// started meanwhile.
fn foreign_supervisors(state_dir: &Path, scope: &Path) -> Vec<String> {
    let subdirs = |dir: PathBuf| -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|rd| rd.flatten().map(|e| e.path()).collect())
            .unwrap_or_default()
    };
    // (the directory the supervisor lock is kept under, the stored spec)
    let attempts = subdirs(state_dir.join("attempts"))
        .into_iter()
        .map(|dir| (state_dir.to_path_buf(), dir.join("supervise.json")));
    let builders = subdirs(state_dir.join("setup").join("builders"))
        .into_iter()
        .map(|dir| (dir.clone(), dir.join("spec.json")));
    let mut found: Vec<String> = attempts
        .chain(builders)
        .filter_map(|(lock_dir, file)| {
            let stored: StoredScope = serde_json::from_slice(&std::fs::read(file).ok()?).ok()?;
            if stored.traffic_state_dir == scope {
                return None;
            }
            // A lock that can be taken proves the supervisor is gone.
            if let Ok(Some(_free)) = SupervisorLock::try_acquire(&lock_dir, &stored.attempt_id) {
                return None;
            }
            Some(format!(
                "{} in {}",
                stored.attempt_id,
                stored.traffic_state_dir.display()
            ))
        })
        .collect();
    found.sort();
    found
}

/// The pause before look number `looks` (0 for the first) at a scope that
/// is not drained: doubled from [`HOLD_FIRST_MS`] up to [`HOLD_MAX_MS`].
pub fn hold_backoff_ms(looks: u32) -> u64 {
    let factor = 1u64.checked_shl(looks).unwrap_or(u64::MAX);
    HOLD_FIRST_MS.saturating_mul(factor).min(HOLD_MAX_MS)
}

/// What a waiting worker says about itself to `worker-service status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoldRecord {
    pub reason: String,
    pub message: String,
    /// The wall clock at the worker's last look.
    pub checked_at_ms: u64,
}

/// Record `hold` in `state_dir`, or remove the record when it is clear.
pub fn record_hold(state_dir: &Path, hold: &Hold, now_ms: u64) -> std::io::Result<()> {
    let path = state_dir.join(HOLD_FILE);
    let (Some(reason), Some(message)) = (hold.reason(), hold.message()) else {
        return match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
    };
    let record = HoldRecord {
        reason: reason.into(),
        message: message.into(),
        checked_at_ms: now_ms,
    };
    let mut tmp = tempfile::NamedTempFile::new_in(state_dir)?;
    std::io::Write::write_all(&mut tmp, &serde_json::to_vec(&record)?)?;
    tmp.persist(&path).map_err(|e| e.error)?;
    Ok(())
}

/// The record of a worker that is waiting now. One that was not renewed for
/// [`HOLD_STALE_MS`] (or is dated in the future) is from a worker that is
/// gone, and is not reported.
pub fn recorded_hold(state_dir: &Path, now_ms: u64) -> Option<HoldRecord> {
    let record: HoldRecord =
        serde_json::from_slice(&std::fs::read(state_dir.join(HOLD_FILE)).ok()?).ok()?;
    let age = now_ms.checked_sub(record.checked_at_ms)?;
    (age <= HOLD_STALE_MS).then_some(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use task_traffic_controller::lock;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_scope_follows_the_cli_rule() {
        // Both XDG homes win over HOME.
        assert_eq!(
            resolve(&env(&[
                ("HOME", "/h"),
                ("XDG_STATE_HOME", "/s"),
                ("XDG_CONFIG_HOME", "/c"),
            ]))
            .unwrap(),
            Scope {
                state_dir: "/s/task-traffic-controller".into(),
                config_file: "/c/task-traffic-controller/config.toml".into(),
            }
        );
        // Without them, HOME decides both.
        assert_eq!(
            resolve(&env(&[("HOME", "/h")])).unwrap(),
            Scope {
                state_dir: "/h/.local/state/task-traffic-controller".into(),
                config_file: "/h/.config/task-traffic-controller/config.toml".into(),
            }
        );
        // An empty variable counts as unset, each on its own.
        assert_eq!(
            resolve(&env(&[
                ("HOME", "/h"),
                ("XDG_STATE_HOME", ""),
                ("XDG_CONFIG_HOME", "/c"),
            ]))
            .unwrap(),
            Scope {
                state_dir: "/h/.local/state/task-traffic-controller".into(),
                config_file: "/c/task-traffic-controller/config.toml".into(),
            }
        );
        // HOME is not needed when both homes are given.
        assert_eq!(
            resolve(&env(&[("XDG_STATE_HOME", "/s"), ("XDG_CONFIG_HOME", "/c")]))
                .unwrap()
                .state_dir,
            Path::new("/s/task-traffic-controller")
        );
    }

    #[test]
    fn a_scope_that_depends_on_the_working_directory_is_refused() {
        let e = resolve(&env(&[("HOME", "/h"), ("XDG_STATE_HOME", "rel")])).unwrap_err();
        assert!(e.contains("XDG_STATE_HOME") && e.contains("\"rel\""), "{e}");
        let e = resolve(&env(&[("HOME", "/h"), ("XDG_CONFIG_HOME", "rel")])).unwrap_err();
        assert!(e.contains("XDG_CONFIG_HOME"), "{e}");
        let e = resolve(&env(&[("HOME", "home")])).unwrap_err();
        assert!(e.contains("HOME must be") && e.contains("\"home\""), "{e}");
        // No HOME at all (or an empty one) is not the CLI's `/`.
        let e = resolve(&env(&[("XDG_CONFIG_HOME", "/c")])).unwrap_err();
        assert!(e.contains("set XDG_STATE_HOME or HOME"), "{e}");
        let e = resolve(&env(&[("XDG_STATE_HOME", "/s"), ("HOME", "")])).unwrap_err();
        assert!(e.contains("set XDG_CONFIG_HOME or HOME"), "{e}");
    }

    fn entry(id: &str) -> task_traffic_controller::ledger::Entry {
        serde_json::from_value(serde_json::json!({
            "id": id, "pid": std::process::id(), "state": "running", "front": false,
            "weight": 1, "predicted_rss": 0, "repo": "", "argv": "x", "key": "k",
            "seq": 1, "enqueued_at": "2030-01-01T00:00:00Z",
        }))
        .unwrap()
    }

    fn legacy_with(dir: &Path, ids: &[&str]) -> (PathBuf, Context) {
        let legacy = dir.join(LEGACY_DIR);
        let ctx = Context::new(Paths::new(&legacy, legacy.join("config.toml")));
        let ledger = Ledger {
            entries: ids.iter().map(|id| entry(id)).collect(),
            ..Default::default()
        };
        ledger.save(&ctx.paths.ledger_file()).unwrap();
        (legacy, ctx)
    }

    #[test]
    fn a_missing_or_drained_former_scope_does_not_stop_the_worker() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let scope = dir.path().join("new");
        // Never used.
        assert_eq!(
            legacy_hold(&dir.path().join(LEGACY_DIR), &scope),
            Hold::Clear
        );
        // Used, and nothing left in the ledger.
        let (legacy, _) = legacy_with(dir.path(), &[]);
        assert_eq!(legacy_hold(&legacy, &scope), Hold::Clear);
        // An entry whose owner is gone (no lease, not managed) is not live.
        let (legacy, _) = legacy_with(dir.path(), &["dead"]);
        assert_eq!(legacy_hold(&legacy, &scope), Hold::Clear);
        // The directory without a ledger file.
        std::fs::remove_file(legacy.join("ledger.json")).unwrap();
        assert_eq!(legacy_hold(&legacy, &scope), Hold::Clear);
    }

    #[test]
    fn a_former_scope_with_a_live_command_is_refused_until_it_ends() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let scope = dir.path().join("new");
        let (legacy, ctx) = legacy_with(dir.path(), &["dead", "held"]);
        let lease = lock::take_lease(&ctx, "held").unwrap();
        let held = legacy_hold(&legacy, &scope);
        // It ends by itself: the worker waits.
        assert_eq!(held.reason(), Some("waiting_for_previous_scope"));
        let Hold::Draining(e) = held else {
            panic!("{held:?}")
        };
        assert!(e.contains("live commands (held)"), "{e}");
        assert!(e.contains(&legacy.display().to_string()), "{e}");
        assert!(e.contains(&scope.display().to_string()), "{e}");
        drop(lease);
        assert_eq!(legacy_hold(&legacy, &scope), Hold::Clear);
    }

    #[test]
    fn a_former_scope_that_cannot_be_read_is_refused() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let scope = dir.path().join("new");
        let (legacy, _) = legacy_with(dir.path(), &[]);
        std::fs::write(legacy.join("ledger.json"), "{not json").unwrap();
        let held = legacy_hold(&legacy, &scope);
        // Not something that ends: another word, and what to do by hand.
        assert_eq!(held.reason(), Some("previous_scope_unreadable"));
        let Hold::Unreadable(e) = held else {
            panic!("{held:?}")
        };
        assert!(e.contains("its ledger cannot be read"), "{e}");
        assert!(e.contains("Waiting does not repair this"), "{e}");
        assert!(
            e.contains(&format!("move {} aside", legacy.display())),
            "{e}"
        );
        assert!(!e.contains("when those commands have finished"), "{e}");
        // Moved aside as the text says, the worker goes on.
        std::fs::rename(&legacy, dir.path().join("traffic.unreadable")).unwrap();
        assert_eq!(hold(dir.path(), &scope), Hold::Clear);
        // Not a directory that can be looked into at all.
        let file = dir.path().join("file");
        std::fs::write(&file, "").unwrap();
        let Hold::Unreadable(e) = legacy_hold(&file.join(LEGACY_DIR), &scope) else {
            panic!("not a directory is not a clear scope")
        };
        assert!(e.contains("it cannot be inspected"), "{e}");
    }

    #[test]
    fn a_command_gets_the_scope_variables_of_the_worker_and_inherit() {
        let worker = env(&[
            ("HOME", "/h"),
            ("XDG_STATE_HOME", "/s"),
            ("PATH", "/bin"),
            ("TASK_TRAFFIC_CONTROLLER_NESTED", "admit"),
        ]);
        let scope = step_env(&worker);
        assert_eq!(
            scope,
            env(&[
                ("XDG_STATE_HOME", "/s"),
                ("TASK_TRAFFIC_CONTROLLER_NESTED", "inherit"),
            ])
        );
        assert_eq!(
            step_env(&env(&[("XDG_CONFIG_HOME", "/c")])),
            env(&[
                ("XDG_CONFIG_HOME", "/c"),
                ("TASK_TRAFFIC_CONTROLLER_NESTED", "inherit"),
            ])
        );
        for key in [
            "XDG_STATE_HOME",
            "XDG_CONFIG_HOME",
            "TASK_TRAFFIC_CONTROLLER_NESTED",
        ] {
            assert!(is_step_env(key), "{key}");
        }
        for key in [
            "HOME",
            "PATH",
            "XDG_CACHE_HOME",
            "TASK_TRAFFIC_CONTROLLER_LEASE",
        ] {
            assert!(!is_step_env(key), "{key}");
        }
        // A stored environment of another scope: what the worker does not
        // have goes, what it has replaces, everything else stays.
        let mut stored = env(&[
            ("PATH", "/bin"),
            ("XDG_STATE_HOME", "/old"),
            ("XDG_CONFIG_HOME", "/old-config"),
        ]);
        apply_step_env(&mut stored, &scope);
        assert_eq!(
            stored,
            env(&[
                ("PATH", "/bin"),
                ("XDG_STATE_HOME", "/s"),
                ("TASK_TRAFFIC_CONTROLLER_NESTED", "inherit"),
            ])
        );
    }

    fn stored_spec(file: &Path, attempt: &AttemptId, traffic: &Path) {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let spec = serde_json::json!({ "attempt_id": attempt, "traffic_state_dir": traffic });
        std::fs::write(file, spec.to_string()).unwrap();
    }

    #[test]
    fn a_living_supervisor_of_another_scope_holds_the_worker_until_it_ends() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let state = dir.path();
        let scope = state.join("new");
        let old = state.join("old");
        let (attempt, builder, same, unstarted) = (
            AttemptId::generate(),
            AttemptId::generate(),
            AttemptId::generate(),
            AttemptId::generate(),
        );
        let attempts = state.join("attempts");
        stored_spec(
            &attempts.join(attempt.as_str()).join("supervise.json"),
            &attempt,
            &old,
        );
        // In this scope already, however alive.
        stored_spec(
            &attempts.join(same.as_str()).join("supervise.json"),
            &same,
            &scope,
        );
        let _same = SupervisorLock::try_acquire(state, &same).unwrap().unwrap();
        // Of the other scope, but no supervisor: started with the new one.
        stored_spec(
            &attempts.join(unstarted.as_str()).join("supervise.json"),
            &unstarted,
            &old,
        );
        // Not a spec at all.
        std::fs::create_dir_all(attempts.join("junk")).unwrap();
        std::fs::write(attempts.join("junk").join("supervise.json"), "{").unwrap();
        let builder_dir = state.join("setup").join("builders").join("b1");
        stored_spec(&builder_dir.join("spec.json"), &builder, &old);
        assert_eq!(hold(state, &scope), Hold::Clear);

        let supervisor = SupervisorLock::try_acquire(state, &attempt)
            .unwrap()
            .unwrap();
        let building = SupervisorLock::try_acquire(&builder_dir, &builder)
            .unwrap()
            .unwrap();
        let held = hold(state, &scope);
        assert_eq!(held.reason(), Some("waiting_for_previous_scope"));
        let text = held.message().unwrap();
        for id in [&attempt, &builder] {
            assert!(
                text.contains(&format!("{id} in {}", old.display())),
                "{text}"
            );
        }
        for id in [&same, &unstarted] {
            assert!(!text.contains(id.as_str()), "{text}");
        }
        assert!(text.contains(&scope.display().to_string()), "{text}");
        drop(supervisor);
        let text = hold(state, &scope).message().unwrap().to_string();
        assert!(
            !text.contains(attempt.as_str()) && text.contains(builder.as_str()),
            "{text}"
        );
        drop(building);
        assert_eq!(hold(state, &scope), Hold::Clear);

        // The former scope is looked at first.
        let (_legacy, ctx) = legacy_with(state, &["held"]);
        let _lease = lock::take_lease(&ctx, "held").unwrap();
        let text = hold(state, &scope).message().unwrap().to_string();
        assert!(text.contains("live commands (held)"), "{text}");
    }

    #[test]
    fn the_pause_between_looks_doubles_up_to_its_cap() {
        assert_eq!(hold_backoff_ms(0), 1_000);
        assert_eq!(hold_backoff_ms(1), 2_000);
        assert_eq!(hold_backoff_ms(4), 16_000);
        assert_eq!(hold_backoff_ms(5), 30_000);
        assert_eq!(hold_backoff_ms(63), 30_000);
        assert_eq!(hold_backoff_ms(64), 30_000);
        assert_eq!(hold_backoff_ms(u32::MAX), 30_000);
    }

    #[test]
    fn a_hold_is_recorded_while_it_lasts_and_not_reported_once_stale() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let state = dir.path();
        assert_eq!(recorded_hold(state, 5_000), None);
        // Clearing what was never recorded is fine.
        record_hold(state, &Hold::Clear, 5_000).unwrap();
        record_hold(state, &Hold::Draining("old commands".into()), 5_000).unwrap();
        let record = recorded_hold(state, 5_000).unwrap();
        assert_eq!(
            record,
            HoldRecord {
                reason: "waiting_for_previous_scope".into(),
                message: "old commands".into(),
                checked_at_ms: 5_000,
            }
        );
        // Renewed at every look, so one that is old was left behind.
        assert!(recorded_hold(state, 5_000 + HOLD_STALE_MS).is_some());
        assert_eq!(recorded_hold(state, 5_001 + HOLD_STALE_MS), None);
        // A clock that went back does not make it current.
        assert_eq!(recorded_hold(state, 4_999), None);
        record_hold(state, &Hold::Unreadable("broken".into()), 6_000).unwrap();
        assert_eq!(
            recorded_hold(state, 6_000).unwrap().reason,
            "previous_scope_unreadable"
        );
        record_hold(state, &Hold::Clear, 7_000).unwrap();
        assert_eq!(recorded_hold(state, 7_000), None);
        assert!(!state.join(HOLD_FILE).exists());
        // Not a record: nothing to report.
        std::fs::write(state.join(HOLD_FILE), "{").unwrap();
        assert_eq!(recorded_hold(state, 7_000), None);
    }
}
