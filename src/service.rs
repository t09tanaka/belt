//! Resident processes and authenticated local command composition.
pub mod bootstrap;
pub mod ci_run;
pub mod completion;
pub mod context;
pub mod control;
pub mod direct;
pub mod doctor;
pub mod estimate;
mod forward;
pub mod github_status;
pub mod group;
pub mod input;
pub mod logs;
pub mod macos;
mod manager_command;
pub mod output_export;
pub mod outputs;
pub mod query;
pub mod resident;
pub mod status_post;
pub mod submit;
pub mod systemd;

pub mod init;

/// A worker waits here while its commands still run in another scope.
mod traffic_hold;
/// Standalone persistent worker role, independent of SSH bridge lifetime.
pub mod worker;

/// Worker-host install and lifecycle helper (macOS launchd, Linux systemd).
pub mod worker_service;
/// Parent-side `workers` commands.
pub mod workers;

pub mod planning;

pub(crate) mod top;

/// The directories that [`sweep_start_temporaries`] looks at.
fn temporary_dirs(env: &std::collections::BTreeMap<String, String>) -> Vec<std::path::PathBuf> {
    use worker_service::Paths;
    let mut dirs = Vec::new();
    for backend in [
        macos::policy::Backend::Launchd,
        macos::policy::Backend::Systemd,
    ] {
        let Ok(paths) = Paths::resolve_for(env, backend) else {
            continue;
        };
        for dir in [
            paths.config.clone(),
            paths.config.join("bin"),
            paths.state.clone(),
            paths.state.join("upgrade"),
            paths.services.clone(),
        ] {
            if !dirs.contains(&dir) {
                dirs.push(dir);
            }
        }
    }
    dirs
}

/// Spec 25.4 (a): at the start of `init`, `upgrade`, `group`, `workers`, the
/// resident and the worker, clear the temporary files that a process killed
/// earlier left in the places those commands write: the configuration
/// directory and its `bin/`, the state directory and its `upgrade/`, and the
/// directory of the service definitions (LaunchAgents or the user's systemd
/// units, shared with other products: only names that start with
/// [`crate::platform::owned_temp::FILE_PREFIX`] are looked at there). One
/// `readdir` of each; a path that cannot be resolved is skipped.
pub fn sweep_start_temporaries(env: &std::collections::BTreeMap<String, String>) {
    for dir in temporary_dirs(env) {
        crate::platform::owned_temp::sweep_files(&dir);
    }
}

/// How many entries named like belt's temporary files are in those
/// directories (what `doctor` shows: they are left when their owner is not
/// proven gone).
pub(crate) fn leftover_temporaries(env: &std::collections::BTreeMap<String, String>) -> usize {
    temporary_dirs(env)
        .iter()
        .map(|dir| crate::platform::owned_temp::count_files(dir))
        .sum()
}

#[cfg(test)]
mod start_sweep_tests {
    use super::*;
    use crate::platform::owned_temp::testing::{dead_name, live_name};
    use std::collections::BTreeMap;

    /// Spec 25.4 (a) acceptance (i), (ii) and (v) for the places the start of
    /// a command sweeps: the configuration directory and `bin/`, the state
    /// directory and `upgrade/`, and the service definitions.
    #[test]
    fn start_sweep_clears_dead_temporaries_and_leaves_other_products_alone() {
        let home = crate::test_tmp::tempdir().unwrap();
        let env = BTreeMap::from([(
            "HOME".to_string(),
            home.path().to_string_lossy().into_owned(),
        )]);
        let paths = worker_service::Paths::resolve(&env).unwrap();
        let dirs = [
            paths.config.clone(),
            paths.config.join("bin"),
            paths.state.clone(),
            paths.state.join("upgrade"),
            paths.services.clone(),
        ];
        let mut gone = Vec::new();
        let mut kept = Vec::new();
        for (n, dir) in dirs.iter().enumerate() {
            std::fs::create_dir_all(dir).unwrap();
            let gone_one = dir.join(dead_name(&format!("eeeeeeeeee{n:02}")));
            let live_one = dir.join(live_name(&format!("ffffffffff{n:02}")));
            let other = dir.join("com.other-product.agent.plist");
            let unit = dir.join("other.service");
            let hidden = dir.join(".other-products-temporary");
            for f in [&gone_one, &live_one, &other, &unit, &hidden] {
                std::fs::write(f, b"x").unwrap();
            }
            gone.push(gone_one);
            kept.extend([live_one, other, unit, hidden]);
        }
        assert_eq!(leftover_temporaries(&env), dirs.len() * 2);
        sweep_start_temporaries(&env);
        for f in &gone {
            assert!(!f.exists(), "{f:?}");
        }
        for f in &kept {
            assert!(f.exists(), "{f:?}");
        }
        assert_eq!(leftover_temporaries(&env), dirs.len());
    }
}
