//! The task-traffic-controller scope a worker uses: the one the
//! `task-traffic-controller` CLI of the same OS user resolves, so both share
//! one ledger, one lock, one set of leases and one configuration (spec §14).
//!
//! The library has no public function for this (it never looks at `HOME` or
//! the XDG variables; the rule lives in the CLI binary), so the rule is
//! written once here. The environment arrives as a map argument; this module
//! never reads the process environment.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use task_traffic_controller::ledger::Ledger;
use task_traffic_controller::{managed, Context, Paths};

/// What the CLI calls its directory under the state and config homes.
pub const DIR: &str = "task-traffic-controller";
/// The variable that moves the CLI's state directory. A step that changed it
/// would run its nested `task-traffic-controller` against another ledger.
pub const STATE_HOME_ENV: &str = "XDG_STATE_HOME";
const CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";
/// Where a worker kept its own ledger before it shared the CLI's.
pub const LEGACY_DIR: &str = "traffic";

/// The state directory and the configuration file of one scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub state_dir: PathBuf,
    pub config_file: PathBuf,
}

/// The CLI's rule on `env`: `$XDG_STATE_HOME/task-traffic-controller`, else
/// `$HOME/.local/state/task-traffic-controller`; the configuration is
/// `$XDG_CONFIG_HOME/task-traffic-controller/config.toml`, else under
/// `$HOME/.config`. As in the CLI an empty variable counts as unset.
///
/// Stricter than the CLI in two places, because a worker and the commands it
/// starts must resolve the same directory whatever their working directory:
/// a relative value is refused (the CLI would resolve it against its own
/// working directory), and a missing `HOME` is refused (the CLI falls back to
/// `/`).
pub fn resolve(env: &BTreeMap<String, String>) -> Result<Scope, String> {
    let state = base(env, STATE_HOME_ENV, ".local/state")?;
    let config = base(env, CONFIG_HOME_ENV, ".config")?;
    Ok(Scope {
        state_dir: state.join(DIR),
        config_file: config.join(DIR).join("config.toml"),
    })
}

fn base(env: &BTreeMap<String, String>, var: &str, under_home: &str) -> Result<PathBuf, String> {
    let (name, value, tail) = match env.get(var).filter(|v| !v.is_empty()) {
        Some(value) => (var, value, None),
        None => match env.get("HOME").filter(|v| !v.is_empty()) {
            Some(home) => ("HOME", home, Some(under_home)),
            None => {
                return Err(format!(
                    "cannot locate the task-traffic-controller scope: set {var} or HOME"
                ))
            }
        },
    };
    let path = Path::new(value);
    if !path.is_absolute() {
        return Err(format!(
            "{name} must be an absolute path to share one task-traffic-controller scope, not {value:?}"
        ));
    }
    Ok(match tail {
        Some(tail) => path.join(tail),
        None => path.to_path_buf(),
    })
}

/// Refuse to leave the worker's former scope (`<state>/traffic`) while it
/// still holds a live command: its budget would be counted in neither
/// ledger. Nothing is moved or removed; attempts supervised from the old
/// scope keep the path in their stored spec. The history is not carried
/// over (a cold start).
///
/// Live is whatever the reaper would keep: a held lease, or a managed
/// command whose processes may still run. A ledger that cannot be read is
/// refused too, never taken as empty.
pub fn refuse_live_legacy(legacy: &Path, scope: &Path) -> Result<(), String> {
    match std::fs::symlink_metadata(legacy) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(refusal(
                legacy,
                scope,
                &format!("it cannot be inspected ({e})"),
            ))
        }
    }
    let ctx = Context::new(Paths::new(legacy, legacy.join("config.toml")));
    let ledger = Ledger::load(&ctx.paths.ledger_file())
        .map_err(|e| refusal(legacy, scope, &format!("its ledger cannot be read ({e})")))?;
    let live: Vec<&str> = ledger
        .entries
        .iter()
        .filter(|e| !managed::assess(&ctx, &e.id).verdict.is_release())
        .map(|e| e.id.as_str())
        .collect();
    if live.is_empty() {
        return Ok(());
    }
    Err(refusal(
        legacy,
        scope,
        &format!("it still holds live commands ({})", live.join(", ")),
    ))
}

fn refusal(legacy: &Path, scope: &Path, why: &str) -> String {
    format!(
        "the worker's previous task-traffic-controller scope {} is not drained: {why}. \
         The worker now shares {} with the task-traffic-controller CLI; let those commands \
         finish (or resolve them), then start the worker again",
        legacy.display(),
        scope.display()
    )
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
        refuse_live_legacy(&dir.path().join(LEGACY_DIR), &scope).unwrap();
        // Used, and nothing left in the ledger.
        let (legacy, _) = legacy_with(dir.path(), &[]);
        refuse_live_legacy(&legacy, &scope).unwrap();
        // An entry whose owner is gone (no lease, not managed) is not live.
        let (legacy, _) = legacy_with(dir.path(), &["dead"]);
        refuse_live_legacy(&legacy, &scope).unwrap();
        // The directory without a ledger file.
        std::fs::remove_file(legacy.join("ledger.json")).unwrap();
        refuse_live_legacy(&legacy, &scope).unwrap();
    }

    #[test]
    fn a_former_scope_with_a_live_command_is_refused_until_it_ends() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let scope = dir.path().join("new");
        let (legacy, ctx) = legacy_with(dir.path(), &["dead", "held"]);
        let lease = lock::take_lease(&ctx, "held").unwrap();
        let e = refuse_live_legacy(&legacy, &scope).unwrap_err();
        assert!(e.contains("live commands (held)"), "{e}");
        assert!(e.contains(&legacy.display().to_string()), "{e}");
        assert!(e.contains(&scope.display().to_string()), "{e}");
        drop(lease);
        refuse_live_legacy(&legacy, &scope).unwrap();
    }

    #[test]
    fn a_former_scope_that_cannot_be_read_is_refused() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let scope = dir.path().join("new");
        let (legacy, _) = legacy_with(dir.path(), &[]);
        std::fs::write(legacy.join("ledger.json"), "{not json").unwrap();
        let e = refuse_live_legacy(&legacy, &scope).unwrap_err();
        assert!(e.contains("its ledger cannot be read"), "{e}");
        // Not a directory that can be looked into at all.
        let file = dir.path().join("file");
        std::fs::write(&file, "").unwrap();
        let e = refuse_live_legacy(&file.join(LEGACY_DIR), &scope).unwrap_err();
        assert!(e.contains("it cannot be inspected"), "{e}");
    }
}
