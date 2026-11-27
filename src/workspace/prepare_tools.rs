//! The tools a Git workspace's preparation runs (spec 22.1): `fast-worktree
//! add` prepares the projects of the captured source's `.fast-worktree.toml`
//! (its `[[clone]]` entries, or without any the tracked lockfiles of its
//! presets less its `[[ignore]]`) and runs its `[[post_add]]` hooks, on the
//! task's activated toolchains (the worker's own environment without any).
//! A task prepares only the presets it needs, as a GitHub Actions job runs
//! `npm ci` only after it sets up node: [`PrepareTools::for_task`] gives the
//! tools that are requirements of the task and the presets it leaves out
//! (`fast-worktree add --skip-preset`). No I/O.
//!
//! This mirrors `fast-worktree` `=0.9.0` (see `Cargo.toml`), whose target
//! rules are private: when the pin moves, check the preset table below.

use crate::toolchain::capability::TOOLS;
use std::collections::{BTreeMap, BTreeSet};

/// The presets of `fast-worktree` 0.9.0: its name, the lockfile that marks a
/// project, and the tools its default key commands and install run.
const PRESETS: &[(&str, &str, &[&str])] = &[
    ("npm", "package-lock.json", &["node", "npm"]),
    ("dart", "pubspec.lock", &["dart"]),
    ("bundler", "Gemfile.lock", &["ruby", "bundle"]),
];

/// The tools whose setup (a setup step, or a declared toolchain) makes a
/// task prepare the projects of each preset: on GitHub Actions `npm ci`
/// follows `actions/setup-node`, `dart pub get` a dart or flutter setup, and
/// `bundle install` `ruby/setup-ruby`.
const PRESET_SETUP: &[(&str, &[&str])] = &[
    ("npm", &["node", "npm"]),
    ("dart", &["dart", "flutter"]),
    ("bundler", &["ruby", "bundle"]),
];

/// What preparation runs: the tools a worker reports (`capability::TOOLS`),
/// which become requirements, and the other programs, which no worker report
/// can show and so are not. `tools` is every tool; `presets` holds, for each
/// preset with a project, the tools a task that skips it does not run, and
/// `always` those of the rest (`[[clone]]` entries without a preset,
/// `post_add` hooks), which every task runs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PrepareTools {
    pub tools: BTreeSet<String>,
    pub presets: BTreeMap<String, BTreeSet<String>>,
    pub always: BTreeSet<String>,
    pub unobservable: BTreeSet<String>,
    /// The source declares a `[[post_add]]` hook. `fast-worktree` 0.9 runs
    /// hooks with `--skip-preset` too, and a hook may need what a preset
    /// installs, so such a source skips no preset (spec 22.1).
    pub post_add: bool,
}

/// The preparation of one task: the tools it runs, each a requirement of the
/// task, and the presets it leaves out, sorted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TaskPreparation {
    pub tools: BTreeSet<String>,
    pub skip: Vec<String>,
}

impl PrepareTools {
    fn add(&mut self, preset: Option<&str>, program: &str) {
        if let Some(name) = preset {
            self.presets.entry(name.to_string()).or_default();
        }
        if TOOLS.contains(&program) {
            self.tools.insert(program.to_string());
            match preset {
                Some(name) => self.presets.entry(name.to_string()).or_default(),
                None => &mut self.always,
            }
            .insert(program.to_string());
        } else if !program.is_empty() {
            self.unobservable.insert(program.chars().take(64).collect());
        }
    }
    fn add_command(&mut self, preset: Option<&str>, command: &str) {
        if let Some(program) = program_of(command) {
            self.add(preset, program);
        }
    }

    /// The preparation of a task whose setup steps or declared toolchains
    /// set up `set_up` (tool names: `node`, `dart`, `ruby`, ...): it prepares
    /// a preset only when it sets up one of that preset's tools, and skips
    /// every other preset with a project. The one rule both the parent's
    /// requirements and the worker's `fast-worktree add` follow (spec 22.1).
    pub fn for_task(&self, set_up: &BTreeSet<String>) -> TaskPreparation {
        if self.post_add {
            return self.whole();
        }
        let mut out = TaskPreparation {
            tools: self.always.clone(),
            skip: Vec::new(),
        };
        for (preset, tools) in &self.presets {
            if needs_preset(preset, set_up) {
                out.tools.extend(tools.iter().cloned());
            } else {
                out.skip.push(preset.clone());
            }
        }
        out
    }

    /// The preparation of a plain command, which has no setup steps: every
    /// preset (spec 22.1).
    pub fn whole(&self) -> TaskPreparation {
        TaskPreparation {
            tools: self.tools.clone(),
            skip: Vec::new(),
        }
    }
}

/// Whether `name` is a preset of `fast-worktree` (one a task can skip).
pub fn is_preset(name: &str) -> bool {
    PRESETS.iter().any(|p| p.0 == name)
}

/// `program` as the preset tool it names (`npm`, `dart`, ...), if it is one.
pub(crate) fn preset_program(program: &str) -> Option<&'static str> {
    // `npx` runs the packages `npm ci` installs.
    let program = if program == "npx" { "npm" } else { program };
    PRESET_SETUP
        .iter()
        .flat_map(|(_, tools)| tools.iter())
        .find(|t| **t == program)
        .copied()
}

/// Whether setting up `set_up` makes a task prepare the projects of `preset`.
fn needs_preset(preset: &str, set_up: &BTreeSet<String>) -> bool {
    PRESET_SETUP
        .iter()
        .find(|(name, _)| *name == preset)
        .is_none_or(|(_, tools)| tools.iter().any(|t| set_up.contains(*t)))
}

/// The program a shell command runs first: the first word after any
/// `NAME=value` assignments. `None` for a command that starts with anything
/// a plain word is not (a quote, a subshell, a variable).
pub(crate) fn program_of(command: &str) -> Option<&str> {
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
                out.add(Some(name), tool);
            }
        }
    } else {
        for entry in &config.clone {
            let preset = entry.preset.as_deref();
            if let Some(name) = preset {
                let tools = preset_tools(name)
                    .ok_or_else(|| format!(".fast-worktree.toml: unknown preset {name:?}"))?;
                for tool in tools {
                    out.add(preset, tool);
                }
            }
            // A skipped preset runs none of its entry's commands either.
            let commands = entry.key_commands.iter().flatten();
            let commands = commands.chain(entry.install.iter());
            let commands = commands.chain(entry.verify.iter());
            for command in commands.chain(entry.after.iter().flatten()) {
                out.add_command(preset, command);
            }
        }
    }
    out.post_add = !config.post_add.is_empty();
    // A hook with `requires` is skipped by fast-worktree when that tool is
    // missing: it never fails preparation. A hook runs whatever is skipped.
    for hook in config.post_add.iter().filter(|h| h.requires.is_none()) {
        out.add_command(None, &hook.run);
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

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// Without `[[clone]]`, every tracked lockfile of a preset is a project
    /// of that preset; a task prepares it only when it sets up the preset's
    /// tool. A Rust-only task of a Rust project with npm examples skips npm
    /// and requires neither node nor npm; a task that sets up node requires
    /// both and skips nothing.
    #[test]
    fn tracked_lockfiles_require_their_preset_tools_of_the_tasks_that_set_them_up() {
        let paths = [
            "Cargo.toml",
            "Cargo.lock",
            "examples/sharding/node/package-lock.json",
            "examples/sharding/node/package.json",
        ];
        let found = prepare_tools(paths, None).unwrap();
        assert_eq!(found.tools, set(&["node", "npm"]));
        let rust = found.for_task(&set(&["rust"]));
        assert!(rust.tools.is_empty(), "{rust:?}");
        assert_eq!(rust.skip, ["npm"]);
        let node = found.for_task(&set(&["node", "rust"]));
        assert_eq!(node.tools, set(&["node", "npm"]));
        assert!(node.skip.is_empty());
        assert_eq!(found.whole(), node);
        let dart = prepare_tools(["app/pubspec.lock"], None).unwrap();
        assert_eq!(dart.tools, set(&["dart"]));
        assert_eq!(dart.for_task(&set(&["flutter"])).tools, set(&["dart"]));
        assert_eq!(dart.for_task(&set(&["node"])).skip, ["dart"]);
        assert!(tools(&["Cargo.lock", "src/package-lock.json.bak"], None).is_empty());
        let found = prepare_tools(["Gemfile.lock"], None).unwrap();
        assert!(found.tools.is_empty());
        assert_eq!(
            found.unobservable.iter().cloned().collect::<Vec<_>>(),
            ["bundle", "ruby"]
        );
        assert_eq!(found.for_task(&set(&[])).skip, ["bundler"]);
        assert!(found.for_task(&set(&["ruby"])).skip.is_empty());
    }

    /// A `[[clone]]` entry of a preset is skipped with it, commands
    /// included; an entry without a preset runs for every task.
    #[test]
    fn a_skipped_preset_leaves_out_its_clone_entries_but_not_the_rest() {
        let config = r#"
[[clone]]
preset = "npm"
dir = "web"
key_commands = ["flutter --version"]

[[clone]]
path = "vendor"
key = ["vendor.lock"]
install = "cargo vendor"
"#;
        let found = prepare_tools([], Some(config)).unwrap();
        let rust = found.for_task(&set(&["rust"]));
        assert_eq!(rust.tools, set(&["cargo"]));
        assert_eq!(rust.skip, ["npm"]);
        let node = found.for_task(&set(&["node"]));
        assert_eq!(node.tools, set(&["cargo", "flutter", "node", "npm"]));
        assert!(node.skip.is_empty());
    }

    /// fast-worktree 0.9 runs `[[post_add]]` hooks with `--skip-preset` as
    /// well, and a hook may use what a preset installs: a source with any
    /// hook skips no preset, even one whose hook has `requires`.
    #[test]
    fn a_post_add_hook_makes_every_task_prepare_every_preset() {
        let config = "[[post_add]]\nrun = \"npm run codegen\"\nrequires = \"npm\"\n";
        let found = prepare_tools(["web/package-lock.json"], Some(config)).unwrap();
        let rust = found.for_task(&set(&["rust"]));
        assert!(rust.skip.is_empty());
        assert_eq!(rust.tools, set(&["node", "npm"]));
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
