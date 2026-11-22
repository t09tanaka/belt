//! The per-version tool commands used by a worker's hooks and tasks.
use crate::ids::Digest;
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

/// Version of the belt binary and the four tools linked into this build.
pub const BUILD_ID: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (task-traffic-controller ",
    env!("BELT_TTC_VERSION"),
    ", local-ci ",
    env!("BELT_LOCAL_CI_VERSION"),
    ", fast-worktree ",
    env!("BELT_FAST_WORKTREE_VERSION"),
    ", portool ",
    env!("BELT_PORTOOL_VERSION"),
    ")"
);

pub const NAMES: [&str; 2] = ["fast-worktree", "portool"];

pub fn version(name: &str) -> Option<&'static str> {
    match name {
        "fast-worktree" => Some(env!("BELT_FAST_WORKTREE_VERSION")),
        "portool" => Some(env!("BELT_PORTOOL_VERSION")),
        _ => None,
    }
}

/// Portool's reader is also the authority on registry compatibility. Only a
/// schema mismatch is a version diagnostic; it never repairs the registry.
pub(crate) fn incompatible_registry(
    env: &std::collections::BTreeMap<String, String>,
) -> Option<String> {
    match portool::check_registry_in(&portool::Context::from_env(env)) {
        Err(portool::Error::UnsupportedRegistryVersion {
            found, supported, ..
        }) => Some(registry_diagnostic(found, supported)),
        _ => None,
    }
}

/// The refusal for a registry the embedded portool cannot read (written by a
/// newer standalone one); it is never rewritten.
pub(crate) fn registry_diagnostic(found: u32, supported: u32) -> String {
    format!("portool {} reads registry through v{supported}; registry written by standalone portool is v{found}", env!("BELT_PORTOOL_VERSION"))
}

pub fn dispatch() -> Option<i32> {
    let mut args = std::env::args_os();
    let invoked = PathBuf::from(args.next()?);
    match invoked.file_name()?.to_str()? {
        "portool" => Some(portool::run_cli(std::env::args_os())),
        "fast-worktree" => {
            // execvp may preserve a bare argv[0]. Keep the link's name when
            // launching pool helpers, even on Linux where current_exe follows it.
            let running = std::env::current_exe().and_then(std::fs::canonicalize).ok();
            let same = |p: &Path| {
                std::fs::canonicalize(p).is_ok_and(|target| Some(&target) == running.as_ref())
            };
            let helper = if invoked.components().count() == 1 {
                std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                    .map(|dir| dir.join(&invoked))
                    .find(|path| same(path))
            } else {
                same(&invoked).then_some(invoked)
            };
            let helper = helper.and_then(|p| std::path::absolute(p).ok());
            match helper {
                Some(link) => Some(fast_worktree::cli::run_with_helper(
                    std::env::args_os(),
                    Some(link),
                )),
                None => {
                    eprintln!("fast-worktree: cannot locate the invoked helper link");
                    Some(1)
                }
            }
        }
        _ => None,
    }
}

pub(crate) fn bin(state: &Path, executable: &Path) -> PathBuf {
    // Service executables live at immutable version-specific paths. Older
    // supervisors keep the links to the executable they started with.
    let id = Digest::of_bytes(executable.as_os_str().as_encoded_bytes());
    state
        .join("tool-bin")
        .join(&id.as_str()[Digest::PREFIX.len()..])
}

pub(crate) fn install(state: &Path, executable: &Path) -> io::Result<()> {
    let bin = bin(state, executable);
    for path in [state.join("tool-bin"), bin.clone()] {
        match crate::service::resident::private_dir(&path) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                crate::service::resident::check_private_dir(&path)?
            }
            result => result?,
        }
    }
    for name in NAMES {
        let link = bin.join(name);
        match symlink(executable, &link) {
            Err(e)
                if e.kind() == io::ErrorKind::AlreadyExists
                    && std::fs::read_link(&link)? == executable => {}
            result => result?,
        }
    }
    Ok(())
}

pub(crate) fn prepend(bin: &Path, path: &str) -> Result<String, String> {
    std::env::join_paths(
        std::iter::once(bin.to_owned())
            .chain(std::env::split_paths(OsStr::new(path)).filter(|entry| entry != bin)),
    )
    .map_err(|e| e.to_string())?
    .into_string()
    .map_err(|_| "worker PATH must be UTF-8".into())
}

/// The environment of a command run under an execution binding: the
/// activation and `own`, with the tool-bin directory (when there is one) first
/// on its `PATH` as it is for every other worker command.
pub(crate) fn activated(
    bin: Option<&Path>,
    binding: &crate::toolchain::contract::ExecutionBinding,
    own: &std::collections::BTreeMap<String, String>,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    let mut env = binding.environment(own).map_err(str::to_string)?;
    if let Some(bin) = bin {
        let path = prepend(bin, env.get("PATH").ok_or("activation has no PATH")?)?;
        env.insert("PATH".into(), path);
    }
    Ok(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_service_binary_keeps_the_previous_links_and_refuses_a_replaced_link() {
        let state = crate::test_tmp::tempdir().unwrap();
        let state = state.path().canonicalize().unwrap();
        let old = state.join("versions/old/belt");
        let new = state.join("versions/new/belt");
        install(&state, &old).unwrap();
        install(&state, &new).unwrap();
        install(&state, &new).unwrap();
        for exe in [&old, &new] {
            for name in NAMES {
                assert_eq!(
                    std::fs::read_link(bin(&state, exe).join(name)).unwrap(),
                    *exe
                );
            }
        }
        let replaced = bin(&state, &old).join("portool");
        std::fs::remove_file(&replaced).unwrap();
        std::fs::write(&replaced, "preserve").unwrap();
        assert!(install(&state, &old).is_err());
        assert_eq!(std::fs::read(&replaced).unwrap(), b"preserve");
    }

    fn bound(
        environment: std::collections::BTreeMap<String, String>,
    ) -> crate::toolchain::contract::ExecutionBinding {
        let activated = environment;
        let d = Digest::of_bytes(b"x");
        crate::toolchain::contract::ExecutionBinding {
            activation: crate::toolchain::adapters::ActivationReceipt {
                base_profile: d.clone(),
                frozen: d.clone(),
                setup_policy: d.clone(),
                installation: d.clone(),
                environment: activated,
                shell: crate::toolchain::adapters::FileIdentity {
                    path: "/bin/sh".into(),
                    digest: d.clone(),
                    size: 1,
                    executable: true,
                },
                binding_digest: d.clone(),
                execution_fingerprint: d,
            },
        }
    }

    #[test]
    fn a_bound_command_gets_the_tool_bin_first_and_is_refused_without_a_path() {
        let bin = Path::new("/state/tool-bin/abc");
        let with_path = bound([("PATH".to_string(), "/activated/bin".to_string())].into());
        let env = activated(Some(bin), &with_path, &Default::default()).unwrap();
        assert_eq!(env["PATH"], "/state/tool-bin/abc:/activated/bin");
        let env = activated(None, &with_path, &Default::default()).unwrap();
        assert_eq!(env["PATH"], "/activated/bin");
        let no_path = bound([("HOME".to_string(), "/h".to_string())].into());
        assert_eq!(
            activated(Some(bin), &no_path, &Default::default()),
            Err("activation has no PATH".to_string())
        );
        assert!(activated(None, &no_path, &Default::default()).is_ok());
        let own = [("FLAG".to_string(), "1".to_string())].into();
        assert_eq!(activated(None, &no_path, &own).unwrap()["FLAG"], "1");
    }
}
