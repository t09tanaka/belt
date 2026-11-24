//! The tools a Git workspace's preparation runs (spec 22.1): `fast-worktree
//! add` prepares the projects of the captured source's `.fast-worktree.toml`
//! (its `[[clone]]` entries, or without any the tracked lockfiles of its
//! presets less its `[[ignore]]`) and runs its `[[post_add]]` hooks, on the
//! worker's own environment and before the task's toolchains are activated.
//! A worker that lacks one of these tools cannot prepare the workspace, so
//! each is a presence requirement of the task. No I/O.
//!
//! This mirrors `fast-worktree` `=0.7.0` (see `Cargo.toml`), whose target
//! rules are private: when the pin moves, check the preset table below.

use crate::toolchain::capability::TOOLS;
use std::collections::BTreeSet;

/// The presets of `fast-worktree` 0.7.0: its name, the lockfile that marks a
/// project, and the tools its default key commands and install run.
const PRESETS: &[(&str, &str, &[&str])] = &[
    ("npm", "package-lock.json", &["node", "npm"]),
    ("dart", "pubspec.lock", &["dart"]),
    ("bundler", "Gemfile.lock", &["ruby", "bundle"]),
];

/// What preparation runs: the tools a worker reports (`capability::TOOLS`),
/// which become requirements, and the other programs, which no worker report
/// can show and so are not.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PrepareTools {
    pub tools: BTreeSet<String>,
    pub unobservable: BTreeSet<String>,
}

impl PrepareTools {
    fn add(&mut self, program: &str) {
        if TOOLS.contains(&program) {
            self.tools.insert(program.to_string());
        } else if !program.is_empty() {
            self.unobservable.insert(program.chars().take(64).collect());
        }
    }
    fn add_command(&mut self, command: &str) {
        if let Some(program) = program_of(command) {
            self.add(program);
        }
    }
}

/// The program a shell command runs first: the first word after any
/// `NAME=value` assignments. `None` for a command that starts with anything
/// a plain word is not (a quote, a subshell, a variable).
fn program_of(command: &str) -> Option<&str> {
    let word = command
        .split_whitespace()
        .find(|w| !w.split_once('=').is_some_and(|(n, _)| is_name(n)))?;
    let plain = word
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"_.+-/".contains(&b));
    plain.then(|| word.rsplit('/').next().unwrap_or(word))
}

fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !s.as_bytes()[0].is_ascii_digit()
}

/// The tools preparing a workspace of a source with `paths` (its files,
/// relative to the root) and `config` (the text of its root
/// `.fast-worktree.toml`, if any) runs. An error when the file cannot be
/// read: `fast-worktree add` refuses it on every worker as well.
pub fn prepare_tools<'a>(
    paths: impl IntoIterator<Item = &'a str>,
    config: Option<&str>,
) -> Result<PrepareTools, String> {
    let config: fast_worktree::Config = match config {
        Some(text) => toml::from_str(text).map_err(|e| format!(".fast-worktree.toml: {e}"))?,
        None => fast_worktree::Config::default(),
    };
    let mut out = PrepareTools::default();
    let preset_tools = |name: &str| PRESETS.iter().find(|p| p.0 == name).map(|p| p.2);
    if config.clone.is_empty() {
        for path in paths {
            let (dir, base) = path.rsplit_once('/').unwrap_or((".", path));
            let Some((name, _, tools)) = PRESETS.iter().find(|p| p.1 == base) else {
                continue;
            };
            if config.ignore.iter().any(|i| i.covers(name, dir)) {
                continue;
            }
            for tool in *tools {
                out.add(tool);
            }
        }
    } else {
        for entry in &config.clone {
            if let Some(name) = &entry.preset {
                let tools = preset_tools(name)
                    .ok_or_else(|| format!(".fast-worktree.toml: unknown preset {name:?}"))?;
                for tool in tools {
                    out.add(tool);
                }
            }
            let commands = entry.key_commands.iter().flatten();
            let commands = commands.chain(entry.install.iter());
            let commands = commands.chain(entry.verify.iter());
            for command in commands.chain(entry.after.iter().flatten()) {
                out.add_command(command);
            }
        }
    }
    // A hook with `requires` is skipped by fast-worktree when that tool is
    // missing: it never fails preparation.
    for hook in config.post_add.iter().filter(|h| h.requires.is_none()) {
        out.add_command(&hook.run);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tools(paths: &[&str], config: Option<&str>) -> Vec<String> {
        let found = prepare_tools(paths.iter().copied(), config).unwrap();
        found.tools.into_iter().collect()
    }

    /// Without `[[clone]]`, every tracked lockfile of a preset is prepared:
    /// a Rust project with npm examples needs node and npm to be prepared.
    #[test]
    fn tracked_lockfiles_require_their_preset_tools() {
        let paths = [
            "Cargo.toml",
            "Cargo.lock",
            "examples/sharding/node/package-lock.json",
            "examples/sharding/node/package.json",
        ];
        assert_eq!(tools(&paths, None), ["node", "npm"]);
        assert_eq!(tools(&["app/pubspec.lock"], None), ["dart"]);
        assert!(tools(&["Cargo.lock", "src/package-lock.json.bak"], None).is_empty());
        let found = prepare_tools(["Gemfile.lock"], None).unwrap();
        assert!(found.tools.is_empty());
        assert_eq!(
            found.unobservable.into_iter().collect::<Vec<_>>(),
            ["bundle", "ruby"]
        );
    }

    /// `[[ignore]]` leaves a project out, as fast-worktree does.
    #[test]
    fn ignored_projects_require_nothing() {
        let config = "[[ignore]]\ndir = \"web\"\nreason = \"not built here\"\n";
        assert!(tools(&["web/package-lock.json"], Some(config)).is_empty());
        assert_eq!(
            tools(
                &["web/package-lock.json", "api/package-lock.json"],
                Some(config)
            ),
            ["node", "npm"]
        );
    }

    /// With `[[clone]]`, only its entries count: their preset and the first
    /// program of each command, after assignments. `post_add` hooks count
    /// unless they name a `requires` tool.
    #[test]
    fn clone_entries_and_hooks_require_what_they_run() {
        let config = r#"
[[clone]]
path = "ios/Pods"
key = ["ios/Podfile.lock"]
key_commands = ["FOO=1 /usr/local/bin/flutter --version"]
install = "pod install"

[[post_add]]
run = "portool sync"

[[post_add]]
run = "docker compose up"
requires = "docker"
"#;
        let paths = ["web/package-lock.json"];
        let found = prepare_tools(paths, Some(config)).unwrap();
        assert_eq!(
            found.tools.into_iter().collect::<Vec<_>>(),
            ["flutter", "portool"]
        );
        assert_eq!(found.unobservable.into_iter().collect::<Vec<_>>(), ["pod"]);
        let preset = "[[clone]]\npreset = \"npm\"\ndir = \"api\"\n";
        assert_eq!(tools(&[], Some(preset)), ["node", "npm"]);
        assert!(prepare_tools([], Some("[[clone]]\npreset = \"x\"\ndir = \".\"\n")).is_err());
        assert!(prepare_tools([], Some("[[clone]\n")).is_err());
    }
}
