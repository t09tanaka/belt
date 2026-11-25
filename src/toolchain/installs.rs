//! Where the user's version managers keep installed tool versions (spec
//! 22.1). Read only: directory listings, nothing runs a manager, installs or
//! writes. The versions found here are candidates; a profile probes each
//! candidate's own executable before it counts.
use super::capability::{valid_version, DETECTED_TOOLS, MAX_DETECTED_INSTALLS};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Entries of one versions directory read at most.
const MAX_ENTRIES: usize = 1024;

/// One installed version a manager has, not yet probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub tool: &'static str,
    pub exe: &'static str,
    pub arg: &'static str,
    pub source: &'static str,
    /// The directory that holds `exe`.
    pub bin: PathBuf,
}

fn absolute(path: PathBuf) -> Option<PathBuf> {
    path.is_absolute().then_some(path)
}

/// The mise data directory of `env`: `MISE_DATA_DIR`, else
/// `$XDG_DATA_HOME/mise`, else `~/.local/share/mise`.
pub(crate) fn mise_data_dir(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    if let Some(dir) = env.get("MISE_DATA_DIR") {
        return absolute(dir.into());
    }
    if let Some(dir) = env.get("XDG_DATA_HOME") {
        return absolute(PathBuf::from(dir).join("mise"));
    }
    absolute(PathBuf::from(env.get("HOME")?).join(".local/share/mise"))
}

/// The real subdirectories of `dir` with the version their name gives,
/// after `strip` removed a prefix. A link, a file or a name that is not
/// UTF-8 or not a version is skipped: a manager's alias links (`20`,
/// `lts`) point at a version that is listed by its own name.
fn versions(dir: &Path, strip: &str) -> Vec<(node_semver::Version, PathBuf)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in entries.take(MAX_ENTRIES).flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let path = entry.path();
        if !fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir()) {
            continue;
        }
        let Some(name) = name.strip_prefix(strip) else {
            continue;
        };
        // Homebrew's revision suffix (`20.11.1_1`) is not part of the version.
        let name = name.split('_').next().unwrap_or(name);
        if let Ok(version) = node_semver::Version::parse(name) {
            found.push((version, path));
        }
    }
    found
}

/// The parents of the profile PATH directories named `bin` that hold an
/// executable `brew`, in PATH order, each once.
fn brew_prefixes(env: &BTreeMap<String, String>) -> Vec<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let mut found: Vec<PathBuf> = Vec::new();
    let Some(path) = env.get("PATH") else {
        return found;
    };
    for dir in std::env::split_paths(path).take(MAX_ENTRIES) {
        let brew = dir.join("brew");
        let usable =
            fs::metadata(&brew).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0);
        if !dir.is_absolute() || !dir.ends_with("bin") || !usable {
            continue;
        }
        if let Some(prefix) = dir.parent() {
            if !found.iter().any(|p| p == prefix) {
                found.push(prefix.to_path_buf());
            }
        }
    }
    found
}

/// Every installed version the managers of `env` have, highest first, at
/// most [`MAX_DETECTED_INSTALLS`]: mise (`<data>/installs/<tool>/<v>`),
/// nodenv (`$NODENV_ROOT` or `~/.nodenv`, `versions/<v>`), nvm (`$NVM_DIR`
/// or `~/.nvm`, `versions/node/v<v>`) and Homebrew
/// (`$HOMEBREW_PREFIX/Cellar/node[@N]/<v>`, else the prefix of each `brew`
/// on the profile PATH).
pub(crate) fn candidates(env: &BTreeMap<String, String>) -> Vec<Candidate> {
    let home = env.get("HOME").map(PathBuf::from);
    let mut found: Vec<(node_semver::Version, Candidate)> = Vec::new();
    let mut add = |tool: &'static str, source: &'static str, dir: &Path, strip: &str| {
        let Some((_, exe, arg)) = DETECTED_TOOLS.iter().find(|(t, _, _)| *t == tool) else {
            return;
        };
        for (version, path) in versions(dir, strip) {
            found.push((
                version,
                Candidate {
                    tool,
                    exe,
                    arg,
                    source,
                    bin: path.join("bin"),
                },
            ));
        }
    };
    if let Some(data) = mise_data_dir(env) {
        for (tool, _, _) in DETECTED_TOOLS {
            add(tool, "mise", &data.join("installs").join(tool), "");
        }
    }
    let nodenv = match env.get("NODENV_ROOT") {
        Some(root) => absolute(root.into()),
        None => home.as_ref().and_then(|h| absolute(h.join(".nodenv"))),
    };
    if let Some(root) = nodenv {
        add("node", "nodenv", &root.join("versions"), "");
    }
    let nvm = match env.get("NVM_DIR") {
        Some(root) => absolute(root.into()),
        None => home.as_ref().and_then(|h| absolute(h.join(".nvm"))),
    };
    if let Some(root) = nvm {
        add("node", "nvm", &root.join("versions/node"), "v");
    }
    let prefixes: Vec<PathBuf> = match env.get("HOMEBREW_PREFIX") {
        Some(prefix) => absolute(prefix.into()).into_iter().collect(),
        // Without it, the prefix of a `brew` on the profile PATH: a machine
        // whose profile does not use Homebrew is not read.
        None => brew_prefixes(env),
    };
    for prefix in prefixes {
        let cellar = prefix.join("Cellar");
        let Ok(entries) = fs::read_dir(&cellar) else {
            continue;
        };
        for entry in entries.take(MAX_ENTRIES).flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name == "node" || name.strip_prefix("node@").is_some() {
                add("node", "homebrew", &entry.path(), "");
            }
        }
    }
    found.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.1.tool.cmp(b.1.tool))
            .then_with(|| a.1.bin.cmp(&b.1.bin))
    });
    found
        .into_iter()
        .take(MAX_DETECTED_INSTALLS)
        .map(|(_, c)| c)
        .collect()
}

/// The version `tool`'s executable printed, as a plain release or
/// prerelease (`20.11.1`).
pub(crate) fn reported_version(tool: &str, output: &str) -> Option<String> {
    let text = output.trim();
    let token = match tool {
        "node" => text.strip_prefix('v')?.split_whitespace().next()?,
        "python" => text.strip_prefix("Python ")?.split_whitespace().next()?,
        "go" => text
            .strip_prefix("go version go")?
            .split_whitespace()
            .next()?,
        "ruby" => text.strip_prefix("ruby ")?.split_whitespace().next()?,
        _ => return None,
    };
    let base = token.split(['-', '+']).next()?;
    (valid_version(token)
        && base.contains('.')
        && base
            .split('.')
            .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit())))
    .then(|| token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(path: &Path) {
        fs::create_dir_all(path).unwrap();
    }

    #[test]
    fn every_manager_is_read_and_links_are_skipped() {
        let t = tempfile::tempdir().unwrap();
        let home = t.path().join("home");
        dir(&home.join(".local/share/mise/installs/node/22.1.0"));
        dir(&home.join(".local/share/mise/installs/python/3.12.1"));
        std::os::unix::fs::symlink(
            home.join(".local/share/mise/installs/node/22.1.0"),
            home.join(".local/share/mise/installs/node/22"),
        )
        .unwrap();
        dir(&home.join(".nodenv/versions/20.11.1"));
        dir(&home.join(".nvm/versions/node/v18.19.0"));
        let brew = t.path().join("brew");
        dir(&brew.join("Cellar/node@16/16.20.2_1"));
        dir(&brew.join("Cellar/git/2.40.0"));
        let env = BTreeMap::from([
            ("HOME".to_string(), home.display().to_string()),
            ("HOMEBREW_PREFIX".to_string(), brew.display().to_string()),
        ]);
        let found: Vec<_> = candidates(&env)
            .into_iter()
            .map(|c| (c.tool, c.source, c.bin))
            .collect();
        assert_eq!(
            found,
            [
                (
                    "node",
                    "mise",
                    home.join(".local/share/mise/installs/node/22.1.0/bin")
                ),
                ("node", "nodenv", home.join(".nodenv/versions/20.11.1/bin")),
                ("node", "nvm", home.join(".nvm/versions/node/v18.19.0/bin")),
                (
                    "node",
                    "homebrew",
                    brew.join("Cellar/node@16/16.20.2_1/bin")
                ),
                (
                    "python",
                    "mise",
                    home.join(".local/share/mise/installs/python/3.12.1/bin")
                ),
            ]
        );
    }

    #[test]
    fn homebrew_without_its_prefix_variable_is_found_through_brew_on_path() {
        let t = tempfile::tempdir().unwrap();
        let brew = t.path().join("brew");
        dir(&brew.join("Cellar/node/22.1.0"));
        dir(&brew.join("bin"));
        let other = t.path().join("other");
        dir(&other.join("Cellar/node/20.1.0"));
        dir(&other.join("bin"));
        let exe = brew.join("bin/brew");
        fs::write(&exe, "#!/bin/sh\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&exe, fs::Permissions::from_mode(0o755)).unwrap();
        let path = format!(
            "{}:{}",
            other.join("bin").display(),
            brew.join("bin").display()
        );
        let env = BTreeMap::from([
            (
                "HOME".to_string(),
                t.path().join("home").display().to_string(),
            ),
            ("PATH".to_string(), path),
        ]);
        let found: Vec<_> = candidates(&env).into_iter().map(|c| c.bin).collect();
        assert_eq!(found, [brew.join("Cellar/node/22.1.0/bin")]);
        // No brew on PATH: Homebrew is not read.
        let env = BTreeMap::from([("PATH".to_string(), other.join("bin").display().to_string())]);
        assert!(candidates(&env).is_empty());
    }
    #[test]
    fn candidates_are_capped_highest_first() {
        let t = tempfile::tempdir().unwrap();
        for minor in 0..(MAX_DETECTED_INSTALLS + 5) {
            dir(&t.path().join(format!(".nodenv/versions/20.{minor}.0")));
        }
        let env = BTreeMap::from([
            ("HOME".to_string(), t.path().display().to_string()),
            ("HOMEBREW_PREFIX".to_string(), "/nonexistent".to_string()),
        ]);
        let found = candidates(&env);
        assert_eq!(found.len(), MAX_DETECTED_INSTALLS);
        assert!(found[0]
            .bin
            .ends_with(format!("20.{}.0/bin", MAX_DETECTED_INSTALLS + 4)));
    }

    #[test]
    fn versions_are_read_from_each_tools_own_output() {
        assert_eq!(
            reported_version("node", "v20.11.1\n").as_deref(),
            Some("20.11.1")
        );
        assert_eq!(
            reported_version("python", "Python 3.12.1").as_deref(),
            Some("3.12.1")
        );
        assert_eq!(
            reported_version("go", "go version go1.22.0 darwin/arm64").as_deref(),
            Some("1.22.0")
        );
        assert_eq!(
            reported_version(
                "ruby",
                "ruby 3.3.0 (2023-12-25 revision 5124f9ac75) [arm64-darwin23]"
            )
            .as_deref(),
            Some("3.3.0")
        );
        assert_eq!(reported_version("node", "20.11.1"), None);
        assert_eq!(reported_version("node", "vgarbage"), None);
    }
}
