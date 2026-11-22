//! The per-version tool commands used by a worker's hooks and tasks.
use crate::ids::Digest;
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

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
        Err(portool::Error::UnsupportedRegistryVersion { found, supported, .. }) =>
            Some(format!("portool {} reads registry through v{supported}; registry written by standalone portool is v{found}", env!("BELT_PORTOOL_VERSION"))),
        _ => None,
    }
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
}
