//! Environment gaps a worker's service has against its user's own shell
//! (spec 15.x "環境の差"): the directories of the login shell's `PATH` the
//! service does not have, and a macOS linker older than the SDK it links
//! against. The host gathers facts with bounded commands; the parent turns
//! them into checks. No command builds anything.

use super::{Check, Level, Runner};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// The line the login shell prints its `PATH` on. Anything a profile prints
/// before it is ignored.
const MARKER: &str = "__BELT_LOGIN_PATH__=";
/// A longer answer is not a `PATH`.
const MAX_PATH_BYTES: usize = 16 * 1024;
/// At most this many directories are reported.
const MAX_GAPS: usize = 32;
/// Tools a missing directory is named by, when it has them.
const NOTABLE: &[&str] = &[
    "brew",
    "mise",
    "node",
    "npm",
    "pnpm",
    "yarn",
    "git",
    "gh",
    "python3",
    "cargo",
    "rustup",
    "go",
    "java",
    "ruby",
    "docker",
    "task-traffic-controller",
    "local-ci",
    "fast-worktree",
    "portool",
];

/// The variables the login shell starts with. Nothing else of the caller's
/// environment reaches it, so it builds `PATH` from its own profile.
pub fn login_env(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    ["HOME", "USER", "SHELL"]
        .into_iter()
        .filter_map(|k| Some((k.to_string(), env.get(k)?.clone())))
        .collect()
}

/// The user's login shell: `SHELL` when it is an absolute path, else `/bin/sh`.
pub fn shell(env: &BTreeMap<String, String>) -> PathBuf {
    env.get("SHELL")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| PathBuf::from("/bin/sh"))
}

/// The arguments that make `shell` a non-interactive login shell printing
/// its `PATH` after the marker.
pub fn arguments(shell: &Path) -> [String; 3] {
    let script = if shell.file_name().is_some_and(|n| n == "fish") {
        format!("printf '\\n%s%s\\n' '{MARKER}' (string join : $PATH)")
    } else {
        format!("printf '\\n%s%s\\n' '{MARKER}' \"$PATH\"")
    };
    ["-l".into(), "-c".into(), script]
}

/// The `PATH` in the answer: exit 0, the last marker line, a bounded value
/// with at least one absolute entry. Relative and empty entries are dropped.
pub fn parse(code: i32, stdout: &str) -> Option<String> {
    if code != 0 {
        return None;
    }
    let value = stdout.lines().rev().find_map(|l| l.strip_prefix(MARKER))?;
    if value.len() > MAX_PATH_BYTES {
        return None;
    }
    let entries = absolute_entries(value);
    (!entries.is_empty()).then(|| entries.join(":"))
}

fn absolute_entries(path: &str) -> Vec<&str> {
    let mut seen = Vec::new();
    for entry in path.split(':') {
        if entry.starts_with('/') && !seen.contains(&entry) {
            seen.push(entry);
        }
    }
    seen
}

/// The login shell's `PATH`, asked with a bound in an environment of only
/// `HOME`, `USER` and `SHELL`. `None`: it could not be read.
pub fn login_path(env: &BTreeMap<String, String>, runner: &mut dyn Runner) -> Option<String> {
    let shell = shell(env);
    let arguments = arguments(&shell);
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    let (code, stdout) = runner.run(&shell, &arguments, &login_env(env))?;
    parse(code, &stdout)
}

/// The entries of `login` that `service` lacks, in the login shell's order.
pub fn missing(service: &str, login: &str) -> Vec<String> {
    let have = absolute_entries(service);
    absolute_entries(login)
        .into_iter()
        .filter(|e| !have.contains(e))
        .take(MAX_GAPS)
        .map(str::to_string)
        .collect()
}

/// One directory of the login shell's `PATH` the service lacks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathGap {
    pub dir: String,
    /// The notable tools in it.
    pub tools: Vec<String>,
}

/// What a host found out about its service's `PATH`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathFacts {
    /// `None`: the login shell's `PATH` could not be read.
    pub login: Option<String>,
    pub gaps: Vec<PathGap>,
}

fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

/// Compare the service's `PATH` with the login shell's.
pub fn path_facts(
    caller: &BTreeMap<String, String>,
    service: &BTreeMap<String, String>,
    runner: &mut dyn Runner,
) -> PathFacts {
    let Some(login) = login_path(caller, runner) else {
        return PathFacts {
            login: None,
            gaps: Vec::new(),
        };
    };
    let service_path = service.get("PATH").map(String::as_str).unwrap_or("");
    let gaps = missing(service_path, &login)
        .into_iter()
        .map(|dir| PathGap {
            tools: NOTABLE
                .iter()
                .filter(|t| executable(&Path::new(&dir).join(t)))
                .map(|t| t.to_string())
                .collect(),
            dir,
        })
        .collect();
    PathFacts {
        login: Some(login),
        gaps,
    }
}

/// The checks of a host's `PATH` facts. `fix` is the sentence saying how
/// the service's `PATH` is recaptured.
pub fn path_checks(host: &str, facts: &PathFacts, fix: &str) -> Vec<Check> {
    let check = |level, detail: String| Check::new(host, "service_path", level, detail);
    if facts.login.is_none() {
        return vec![check(
            Level::Notice,
            "the login shell's PATH could not be read, so the service's PATH was not compared with it".into(),
        )];
    }
    if facts.gaps.is_empty() {
        return vec![check(
            Level::Ok,
            "the service's PATH has every directory of the login shell's PATH".into(),
        )];
    }
    facts
        .gaps
        .iter()
        .map(|gap| {
            let tools = if gap.tools.is_empty() {
                String::new()
            } else {
                format!(" (it has {})", gap.tools.join(", "))
            };
            check(
                Level::Notice,
                format!(
                    "the service's PATH lacks {}{tools}, which the login shell's PATH has, so jobs here cannot find what is in it; {fix}",
                    gap.dir
                ),
            )
        })
        .collect()
}

/// What a macOS host found out about its linker and SDK.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolchainFacts {
    /// `xcode-select -p`.
    pub developer_dir: Option<String>,
    /// The version of the SDK `xcrun` links against.
    pub sdk_version: Option<String>,
    /// The newest SDK the selected developer directory ships with, which is
    /// the newest its linker can read.
    pub toolchain_sdk: Option<String>,
    /// `pkgutil --pkg-info=com.apple.pkg.CLTools_Executables`.
    pub clt_version: Option<String>,
}

/// `27.2` of `MacOSX27.2.sdk`.
pub fn sdk_version_of(name: &str) -> Option<String> {
    let v = name.strip_prefix("MacOSX")?.strip_suffix(".sdk")?;
    version(v).map(|_| v.to_string())
}

fn version(text: &str) -> Option<Vec<u64>> {
    text.split('.').map(|p| p.parse().ok()).collect()
}

/// The `version:` line of `pkgutil --pkg-info`.
pub fn parse_pkgutil(code: i32, stdout: &str) -> Option<String> {
    if code != 0 {
        return None;
    }
    stdout
        .lines()
        .find_map(|l| l.strip_prefix("version:"))
        .map(|v| v.trim().to_string())
        .filter(|v| version(v).is_some())
}

/// The version of the SDK at `path`: its directory name after links, else
/// its `SDKSettings.json`.
fn sdk_version_at(path: &Path) -> Option<String> {
    let real = std::fs::canonicalize(path).ok()?;
    if let Some(v) = real
        .file_name()
        .and_then(|n| sdk_version_of(&n.to_string_lossy()))
    {
        return Some(v);
    }
    let text = std::fs::read_to_string(real.join("SDKSettings.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    v.get("Version")?.as_str().map(str::to_string)
}

/// The newest `MacOSX<v>.sdk` in the SDK directory of `developer_dir`.
fn newest_sdk(developer_dir: &Path) -> Option<String> {
    let dirs = [
        developer_dir.join("Platforms/MacOSX.platform/Developer/SDKs"),
        developer_dir.join("SDKs"),
    ];
    dirs.iter()
        .filter_map(|d| std::fs::read_dir(d).ok())
        .flatten()
        .filter_map(|e| sdk_version_of(&e.ok()?.file_name().to_string_lossy()))
        .max_by_key(|v| version(v))
}

fn first_line(answer: Option<(i32, String)>) -> Option<String> {
    let (_, stdout) = answer.filter(|(c, _)| *c == 0)?;
    let line = stdout.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_string())
}

/// Ask the selected developer directory, the SDK `xcrun` uses and the CLT
/// package. Nothing is compiled or linked.
pub fn toolchain_facts(env: &BTreeMap<String, String>, runner: &mut dyn Runner) -> ToolchainFacts {
    let developer_dir = first_line(runner.run(Path::new("/usr/bin/xcode-select"), &["-p"], env));
    let sdk_version =
        first_line(runner.run(Path::new("/usr/bin/xcrun"), &["--show-sdk-path"], env))
            .and_then(|p| sdk_version_at(Path::new(&p)));
    let toolchain_sdk = developer_dir
        .as_deref()
        .and_then(|d| newest_sdk(Path::new(d)));
    let clt_version = runner
        .run(
            Path::new("/usr/sbin/pkgutil"),
            &["--pkg-info=com.apple.pkg.CLTools_Executables"],
            env,
        )
        .and_then(|(c, s)| parse_pkgutil(c, &s));
    ToolchainFacts {
        developer_dir,
        sdk_version,
        toolchain_sdk,
        clt_version,
    }
}

fn major_minor(text: &str) -> Option<(u64, u64)> {
    let v = version(text)?;
    Some((*v.first()?, v.get(1).copied().unwrap_or(0)))
}

const CLT_DIR: &str = "/Library/Developer/CommandLineTools";

/// The checks of a host's linker facts: a linker whose own SDKs are older
/// than the SDK it links against cannot read that SDK's `.tbd` files, so
/// every C or Objective-C link fails.
pub fn toolchain_checks(host: &str, facts: &ToolchainFacts) -> Vec<Check> {
    let check = |level, detail: String| Check::new(host, "macos_linker", level, detail);
    let (Some(dir), Some(sdk)) = (&facts.developer_dir, &facts.sdk_version) else {
        return vec![check(
            Level::Notice,
            "the selected developer directory or its SDK could not be read, so the linker was not compared with the SDK".into(),
        )];
    };
    let clt = dir.trim_end_matches('/') == CLT_DIR;
    let behind = |own: &str| match (major_minor(own), major_minor(sdk)) {
        (Some(own), Some(sdk)) => own < sdk,
        _ => false,
    };
    let toolchain_behind = facts.toolchain_sdk.as_deref().is_some_and(behind);
    let clt_behind = clt
        && facts
            .clt_version
            .as_deref()
            .is_some_and(|v| major_minor(v).map(|m| m.0) < major_minor(sdk).map(|m| m.0));
    if !toolchain_behind && !clt_behind {
        return vec![check(
            Level::Ok,
            format!("the linker of {dir} can read the SDK it links against ({sdk})"),
        )];
    }
    let own = if clt_behind {
        format!(
            "Command Line Tools {}",
            facts.clt_version.as_deref().unwrap_or("?")
        )
    } else {
        format!(
            "{dir}, whose newest SDK is {}",
            facts.toolchain_sdk.as_deref().unwrap_or("?")
        )
    };
    let fix = if clt {
        "update the Command Line Tools: `softwareupdate --list`, then `softwareupdate --install \"<the Command Line Tools label it lists>\"` (or `xcode-select --install`)".to_string()
    } else {
        format!("update Xcode, or select the current Command Line Tools with `sudo xcode-select --switch {CLT_DIR}` (update them with `softwareupdate --list` and `softwareupdate --install \"<label>\"`)")
    };
    vec![check(
        Level::Notice,
        format!("the linker of {own} is older than the SDK it links against ({sdk}) and cannot read its .tbd files, so every C or Objective-C link here fails; {fix}"),
    )]
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake(
        Vec<(PathBuf, Vec<String>, BTreeMap<String, String>)>,
        Option<(i32, String)>,
    );
    impl Runner for Fake {
        fn run(
            &mut self,
            program: &Path,
            arguments: &[&str],
            env: &BTreeMap<String, String>,
        ) -> Option<(i32, String)> {
            self.0.push((
                program.into(),
                arguments.iter().map(|a| a.to_string()).collect(),
                env.clone(),
            ));
            self.1.clone()
        }
    }

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn the_login_shell_runs_as_a_login_shell_with_only_home_user_and_shell() {
        let caller = env(&[
            ("HOME", "/h"),
            ("USER", "u"),
            ("SHELL", "/bin/zsh"),
            ("PATH", "/usr/bin"),
            ("SECRET", "x"),
        ]);
        let mut fake = Fake(
            Vec::new(),
            Some((0, format!("motd\n{MARKER}/opt/homebrew/bin:/usr/bin\n"))),
        );
        assert_eq!(
            login_path(&caller, &mut fake).as_deref(),
            Some("/opt/homebrew/bin:/usr/bin")
        );
        let (program, arguments, seen) = &fake.0[0];
        assert_eq!(program, Path::new("/bin/zsh"));
        assert_eq!(&arguments[..2], ["-l", "-c"]);
        assert_eq!(
            seen,
            &env(&[("HOME", "/h"), ("USER", "u"), ("SHELL", "/bin/zsh")])
        );
    }

    #[test]
    fn a_failed_or_unmarked_or_relative_answer_is_no_path() {
        assert_eq!(parse(1, &format!("{MARKER}/usr/bin\n")), None);
        assert_eq!(parse(0, "/usr/bin\n"), None);
        assert_eq!(parse(0, &format!("{MARKER}bin:.\n")), None);
        assert_eq!(
            parse(0, &format!("{MARKER}{}\n", "/a".repeat(MAX_PATH_BYTES))),
            None
        );
        assert_eq!(
            parse(0, &format!("{MARKER}/a::rel:/a:/b\n")).as_deref(),
            Some("/a:/b")
        );
        assert_eq!(shell(&env(&[("SHELL", "zsh")])), PathBuf::from("/bin/sh"));
    }

    #[test]
    fn every_login_directory_the_service_lacks_is_a_gap_with_its_tools() {
        let root = crate::test_tmp::tempdir().unwrap();
        let brew = root.path().join("brew");
        std::fs::create_dir(&brew).unwrap();
        for (name, mode) in [
            ("brew", 0o755),
            ("node", 0o755),
            ("notes", 0o755),
            ("git", 0o644),
        ] {
            let p = brew.join(name);
            std::fs::write(&p, "").unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let brew = brew.to_str().unwrap().to_string();
        let mut fake = Fake(
            Vec::new(),
            Some((0, format!("{MARKER}{brew}:/usr/bin:/bin\n"))),
        );
        let service = env(&[("PATH", "/h/.cargo/bin:/usr/bin:/bin")]);
        let facts = path_facts(&env(&[("SHELL", "/bin/zsh")]), &service, &mut fake);
        assert_eq!(
            facts.gaps,
            [PathGap {
                dir: brew.clone(),
                tools: vec!["brew".into(), "node".into()]
            }]
        );
        let checks = path_checks("mini", &facts, "run `belt workers refresh-env mini`");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].level, Level::Notice);
        assert!(checks[0].detail.contains(&brew));
        assert!(checks[0].detail.contains("brew, node"));
        assert!(checks[0].detail.contains("`belt workers refresh-env mini`"));
        // Nothing missing is a pass; an unreadable login shell is said.
        let same = path_facts(
            &env(&[("SHELL", "/bin/zsh")]),
            &env(&[("PATH", &format!("/usr/bin:{brew}:/bin:/x"))]),
            &mut fake,
        );
        assert_eq!(path_checks("mini", &same, "f")[0].level, Level::Ok);
        let mut silent = Fake(Vec::new(), None);
        let unread = path_facts(&env(&[]), &service, &mut silent);
        assert_eq!(unread.login, None);
        assert_eq!(path_checks("mini", &unread, "f")[0].level, Level::Notice);
    }

    fn linker(dir: &str, sdk: &str, own: &str, clt: &str) -> ToolchainFacts {
        ToolchainFacts {
            developer_dir: Some(dir.into()),
            sdk_version: Some(sdk.into()),
            toolchain_sdk: Some(own.into()),
            clt_version: Some(clt.into()),
        }
    }

    #[test]
    fn a_linker_older_than_the_sdk_it_links_against_is_warned_with_the_fix() {
        // The mini: Xcode 15.4 selected, xcrun links against the CLT's 27.2 SDK.
        let xcode = "/Applications/Xcode.app/Contents/Developer";
        let old = toolchain_checks("mini", &linker(xcode, "27.2", "14.5", "27.2.0.0.1"));
        assert_eq!(old[0].level, Level::Notice);
        assert!(old[0].detail.contains("14.5") && old[0].detail.contains("27.2"));
        assert!(old[0].detail.contains("xcode-select --switch"));
        // Outdated Command Line Tools linking against their newer SDK.
        let clt = toolchain_checks("mini", &linker(CLT_DIR, "27.2", "27.2", "26.4.0.0.1"));
        assert_eq!(clt[0].level, Level::Notice);
        assert!(clt[0].detail.contains("softwareupdate --list"));
        // Current tools pass; unknown facts are said, not passed.
        let fine = toolchain_checks("mini", &linker(xcode, "27.2", "27.2", "26.0"));
        assert_eq!(fine[0].level, Level::Ok);
        let unknown = ToolchainFacts {
            developer_dir: None,
            sdk_version: None,
            toolchain_sdk: None,
            clt_version: None,
        };
        assert_eq!(toolchain_checks("mini", &unknown)[0].level, Level::Notice);
    }

    #[test]
    fn versions_are_read_from_sdk_names_and_pkgutil() {
        assert_eq!(sdk_version_of("MacOSX27.2.sdk").as_deref(), Some("27.2"));
        assert_eq!(sdk_version_of("MacOSX.sdk"), None);
        let out = "package-id: com.apple.pkg.CLTools_Executables\nversion: 27.2.0.0.1792721180\nvolume: /\n";
        assert_eq!(
            parse_pkgutil(0, out).as_deref(),
            Some("27.2.0.0.1792721180")
        );
        assert_eq!(parse_pkgutil(1, out), None);
        let root = crate::test_tmp::tempdir().unwrap();
        let sdks = root.path().join("SDKs");
        for name in ["MacOSX13.1.sdk", "MacOSX26.5.sdk", "MacOSX9.sdk", "Other"] {
            std::fs::create_dir_all(sdks.join(name)).unwrap();
        }
        std::os::unix::fs::symlink(sdks.join("MacOSX26.5.sdk"), sdks.join("MacOSX.sdk")).unwrap();
        assert_eq!(newest_sdk(root.path()).as_deref(), Some("26.5"));
        assert_eq!(
            sdk_version_at(&sdks.join("MacOSX.sdk")).as_deref(),
            Some("26.5")
        );
    }
}
