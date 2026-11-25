//! Environment difference between the parent and a worker (`workers diff`).
//! Pure: two serializable reports in, classified items out. No process, file or
//! network access, so `doctor` can call `diff` on reports it already holds.
use super::capability::{self, Requirement, ToolObservation, WorkerCapabilities};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA: u32 = 1;
/// Below this much free disk a worker is reported as `differs`.
pub const MIN_DISK_BYTES: u64 = 2 << 30;
/// Tools that are never installed automatically (administrator rights, a
/// license acceptance or a GUI is involved).
const MANUAL_TOOLS: &[&str] = &["xcode", "docker"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Observed {
    Available,
    Unavailable,
    Unknown,
}

/// What a version manager on the worker may install and has installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Managed {
    pub inventory: Vec<String>,
    pub installed: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolFact {
    pub observed: Observed,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed: Option<Managed>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Docker {
    Runnable,
    NotRunnable,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvReport {
    pub schema_version: u32,
    pub os: String,
    pub arch: String,
    pub tools: BTreeMap<String, ToolFact>,
    /// Build identity of what is built into this binary. The
    /// belt row names the embedded dependencies and their resolved versions.
    pub helpers: BTreeMap<String, Option<String>>,
    pub docker: Docker,
    pub disk_free_bytes: Option<u64>,
    /// The active profile's name and required tools are compared; its digest
    /// covers absolute paths that differ between hosts, so it is shown only.
    pub profile: String,
    pub required_tools: Vec<String>,
    pub profile_digest: Option<String>,
    /// `None` until a real digest of the task-traffic-controller policy
    /// exists: then it is `not_measured`, never `same`.
    pub policy_digest: Option<String>,
    /// The toolchains the user's rustup has for this host, as
    /// `<version> (<toolchain>)` (spec 22.1). Shown only: an attempt that
    /// needs one of these versions uses it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rustup: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Same,
    Differs,
    Missing,
    AutoInstallable,
    ManualRequired,
    /// Nothing could be compared; there is nothing to act on either.
    NotMeasured,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub subject: String,
    pub class: Class,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// What to do, for every class except `same`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<String>,
}

/// The manual procedure for a tool that is never installed automatically
/// (without the closing "then rerun" clause).
pub fn manual_steps(tool: &str) -> Option<&'static str> {
    match tool {
        "xcode" => Some("on the worker, install Xcode (App Store or developer.apple.com), then run `sudo xcodebuild -license accept`"),
        "docker" => Some("on the worker, start Docker (Docker Desktop, OrbStack or Colima) so that `docker info` succeeds for the worker user"),
        _ => None,
    }
}

const RERUN: &str = ", then rerun workers diff";

/// The procedure for a tool a person has to put right. `differs`: it is on
/// the worker in another version; otherwise it is not there.
fn install_steps(tool: &str, version: Option<&str>, differs: bool) -> String {
    if let Some(manual) = manual_steps(tool) {
        return format!("{manual}{RERUN}");
    }
    let spec = version.map_or_else(|| tool.to_string(), |v| format!("{tool} {v}"));
    if differs {
        format!(
            "install {spec} on the worker (or authorize it in the worker's toolchains.toml){RERUN}"
        )
    } else {
        format!("install {spec} on the worker so that it is on the profile PATH{RERUN}")
    }
}

fn unmeasured_steps(name: &str, side: &str) -> String {
    format!("{name} could not be measured on {side}; check its profile PATH and rerun workers diff")
}

fn item(
    subject: &str,
    class: Class,
    parent: Option<&str>,
    worker: Option<&str>,
    steps: Option<String>,
) -> Item {
    Item {
        subject: subject.into(),
        class,
        parent: parent.map(str::to_string),
        worker: worker.map(str::to_string),
        steps,
    }
}

fn tool_item(name: &str, want: &ToolFact, have: Option<&ToolFact>) -> Option<Item> {
    // The parent does not use a tool it lacks.
    if want.observed == Observed::Unavailable {
        return None;
    }
    let wanted = want.version.as_deref();
    let have_version = have.and_then(|h| h.version.as_deref());
    let unmeasured = |side: &str| {
        item(
            name,
            Class::Differs,
            wanted,
            have_version,
            Some(unmeasured_steps(name, side)),
        )
    };
    if want.observed == Observed::Unknown {
        return Some(unmeasured("this machine"));
    }
    // A worker that did not report the tool at all was not measured either.
    let Some(have) = have else {
        return Some(unmeasured("the worker"));
    };
    if have.observed == Observed::Available && have.version.as_deref() == wanted {
        return Some(item(name, Class::Same, wanted, wanted, None));
    }
    let managed = have.managed.as_ref();
    let listed = |pick: fn(&Managed) -> &Vec<String>| {
        wanted
            .zip(managed)
            .is_some_and(|(v, m)| pick(m).iter().any(|i| i == v))
    };
    // Installed under the version manager: an attempt activates it, whether
    // or not it is the default on the profile PATH (or measurable there).
    if listed(|m| &m.installed) {
        return Some(item(name, Class::Same, wanted, have_version, None));
    }
    if have.observed == Observed::Unknown {
        return Some(unmeasured("the worker"));
    }
    let present = have.observed == Observed::Available;
    let class = if MANUAL_TOOLS.contains(&name) {
        Class::ManualRequired
    } else if listed(|m| &m.inventory) {
        Class::AutoInstallable
    } else if present {
        Class::Differs
    } else {
        Class::Missing
    };
    let steps = match class {
        Class::AutoInstallable => steps_for_auto(),
        _ => install_steps(name, wanted, present),
    };
    Some(item(name, class, wanted, have_version, Some(steps)))
}

fn steps_for_auto() -> String {
    "run workers sync to install it with the version manager".into()
}

/// A built-in identity compared by value.
fn value_item(subject: &str, want: Option<&str>, have: Option<&str>) -> Option<Item> {
    let want = want?;
    Some(match have {
        None => item(
            subject,
            Class::Missing,
            Some(want),
            None,
            Some(format!("install {subject} {want} on the worker")),
        ),
        Some(h) if h == want => item(subject, Class::Same, Some(want), Some(h), None),
        Some(h) => item(
            subject,
            Class::Differs,
            Some(want),
            Some(h),
            Some(format!(
                "install {subject} {want} on the worker, or run workers upgrade for belt"
            )),
        ),
    })
}

fn profile_text(r: &EnvReport) -> String {
    let digest = r
        .profile_digest
        .as_deref()
        .map(|d| d.chars().take(19).collect::<String>());
    format!(
        "{} requires [{}]{}",
        r.profile,
        r.required_tools.join(", "),
        digest.map_or_else(String::new, |d| format!(" ({d})"))
    )
}

fn profile_item(want: &EnvReport, have: &EnvReport) -> Item {
    let set = |r: &EnvReport| {
        r.required_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<String>>()
    };
    let (pw, ph) = (profile_text(want), profile_text(have));
    if want.profile == have.profile && set(want) == set(have) {
        return item("profile", Class::Same, Some(&pw), Some(&ph), None);
    }
    item(
        "profile",
        Class::Differs,
        Some(&pw),
        Some(&ph),
        Some(format!(
            "make the worker's active profile name and required_tools match the parent's{RERUN}"
        )),
    )
}

fn policy_item(want: &EnvReport, have: &EnvReport) -> Item {
    match (&want.policy_digest, &have.policy_digest) {
        (Some(w), Some(h)) if w == h => item("policy_digest", Class::Same, Some(w), Some(h), None),
        (Some(w), Some(h)) => item(
            "policy_digest",
            Class::Differs,
            Some(w),
            Some(h),
            Some(format!(
                "align the task-traffic-controller policy on the worker{RERUN}"
            )),
        ),
        _ => item(
            "policy_digest",
            Class::NotMeasured,
            None,
            None,
            Some("the task-traffic-controller policy digest is not measured yet".into()),
        ),
    }
}

/// Difference between the parent (`want`) and a worker (`have`), in a fixed
/// order: platform, tools by name, helpers by name, docker, disk, profile,
/// policy digest. Every item carries one class; `Unknown` is never `Same`.
pub fn diff(want: &EnvReport, have: &EnvReport) -> Vec<Item> {
    let mut items = Vec::new();
    let platform = |r: &EnvReport| format!("{}/{}", r.os, r.arch);
    let (pw, ph) = (platform(want), platform(have));
    items.push(if pw == ph {
        item("platform", Class::Same, Some(&pw), Some(&ph), None)
    } else {
        item(
            "platform",
            Class::Differs,
            Some(&pw),
            Some(&ph),
            Some("use a worker with the same OS and architecture (a different architecture needs its own profile)".into()),
        )
    });
    for (name, fact) in &want.tools {
        if name == "docker" || crate::workspace::embedded::version(name).is_some() {
            continue;
        }
        items.extend(tool_item(name, fact, have.tools.get(name)));
    }
    if want.helpers.contains_key("portool_registry")
        || have.helpers.contains_key("portool_registry")
    {
        let version = |report: &EnvReport| {
            report
                .helpers
                .get("portool_registry")
                .and_then(|s| s.clone())
        };
        items.push(item(
            "portool_registry",
            Class::ManualRequired,
            version(want).as_deref(),
            version(have).as_deref(),
            Some("upgrade belt to read the shared portool registry".into()),
        ));
    }
    for (name, version) in &want.helpers {
        if name == "portool_registry" {
            continue;
        }
        let Some(version) = version else { continue };
        let found = have.helpers.get(name).and_then(|v| v.as_deref());
        items.extend(value_item(name, Some(version), found));
    }
    if want.docker == Docker::Runnable {
        items.push(match have.docker {
            Docker::Runnable => item(
                "docker",
                Class::Same,
                Some("runnable"),
                Some("runnable"),
                None,
            ),
            Docker::NotRunnable => item(
                "docker",
                Class::ManualRequired,
                Some("runnable"),
                Some("not runnable"),
                Some(install_steps("docker", None, false)),
            ),
            Docker::Unknown => item(
                "docker",
                Class::Differs,
                Some("runnable"),
                Some("unknown"),
                Some(unmeasured_steps("docker", "the worker")),
            ),
        });
    }
    items.push(match have.disk_free_bytes {
        None => item(
            "disk",
            Class::Missing,
            None,
            None,
            Some("free disk could not be measured on the worker; rerun workers diff".into()),
        ),
        Some(b) if b < MIN_DISK_BYTES => item(
            "disk",
            Class::Differs,
            None,
            Some(&b.to_string()),
            Some(format!(
                "free at least {} GiB on the worker's state volume",
                MIN_DISK_BYTES >> 30
            )),
        ),
        Some(b) => item("disk", Class::Same, None, Some(&b.to_string()), None),
    });
    if !want.rustup.is_empty() || !have.rustup.is_empty() {
        // Shown, never a failure: which versions a machine already has is
        // what the rustc row and placement use.
        let list = |r: &EnvReport| (!r.rustup.is_empty()).then(|| r.rustup.join(", "));
        items.push(Item {
            subject: "rustup".into(),
            class: Class::Same,
            parent: list(want),
            worker: list(have),
            steps: None,
        });
    }
    items.push(profile_item(want, have));
    items.push(policy_item(want, have));
    items
}

/// True when nothing needs attention. A `not_measured` item is no
/// difference: it says only that nothing could be compared.
pub fn all_same(items: &[Item]) -> bool {
    items
        .iter()
        .all(|i| matches!(i.class, Class::Same | Class::NotMeasured))
}

/// One short line (at most 128 characters) saying how to resolve an unmet
/// tool requirement a worker cannot install by itself, for the waiting reason
/// of a task. `unknown`: the worker could not measure the tool, which is no
/// reason to install it.
pub fn waiting_hint(tool: &str, unknown: bool) -> String {
    let steps = if unknown {
        format!("{tool} could not be measured on the worker; rerun workers diff, or check its profile PATH")
    } else {
        install_steps(tool, None, false)
            .strip_suffix(RERUN)
            .map(str::to_string)
            .unwrap_or_default()
    };
    let line = format!("how: {steps}");
    line.chars().take(128).collect()
}

/// `unmet` as `capability::missing` produced it, with one hint inserted after
/// the first line naming an unmet tool requirement (so the display cap of four
/// lines keeps it). Requirements other than tools carry no procedure. The
/// lines are matched through `capability::unmet_line`, the function `missing`
/// writes them with.
pub fn with_steps(
    mut unmet: Vec<String>,
    requires: &[Requirement],
    report: Option<&WorkerCapabilities>,
) -> Vec<String> {
    let named = requires.iter().find_map(|r| match r {
        Requirement::Tool(name) => {
            let line = capability::unmet_line(r);
            unmet
                .iter()
                .position(|u| *u == line)
                .map(|at| (at, name.as_str()))
        }
        _ => None,
    });
    if let Some((at, tool)) = named {
        let unknown = report.is_some_and(|r| r.tools.get(tool) == Some(&ToolObservation::Unknown));
        unmet.insert(at + 1, waiting_hint(tool, unknown));
    }
    unmet
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fact(observed: Observed, version: Option<&str>) -> ToolFact {
        ToolFact {
            observed,
            version: version.map(str::to_string),
            managed: None,
        }
    }
    fn managed(inventory: &[&str], installed: &[&str]) -> Option<Managed> {
        Some(Managed {
            inventory: inventory.iter().map(|v| v.to_string()).collect(),
            installed: installed.iter().map(|v| v.to_string()).collect(),
        })
    }
    fn report() -> EnvReport {
        EnvReport {
            schema_version: SCHEMA,
            os: "macos".into(),
            arch: "aarch64".into(),
            tools: BTreeMap::new(),
            helpers: BTreeMap::new(),
            docker: Docker::Runnable,
            disk_free_bytes: Some(MIN_DISK_BYTES),
            profile: "default".into(),
            required_tools: vec!["node".into()],
            profile_digest: Some("sha256:p".into()),
            policy_digest: Some("sha256:t".into()),
            rustup: Vec::new(),
        }
    }
    /// The item for one tool when the parent has `want` and the worker `have`.
    fn tool(want: ToolFact, have: Option<ToolFact>) -> Option<Item> {
        let mut parent = report();
        parent.tools.insert("node".into(), want);
        let mut worker = report();
        if let Some(have) = have {
            worker.tools.insert("node".into(), have);
        }
        diff(&parent, &worker)
            .into_iter()
            .find(|i| i.subject == "node")
    }
    fn class(want: ToolFact, have: Option<ToolFact>) -> Class {
        tool(want, have).expect("an item").class
    }
    fn avail(v: &str) -> ToolFact {
        fact(Observed::Available, Some(v))
    }

    #[test]
    fn identical_reports_are_all_same() {
        let items = diff(&report(), &report());
        assert!(all_same(&items));
        let mut worker = report();
        worker.docker = Docker::Unknown;
        assert!(!all_same(&diff(&report(), &worker)));
        assert_eq!(
            items.iter().map(|i| i.subject.as_str()).collect::<Vec<_>>(),
            ["platform", "docker", "disk", "profile", "policy_digest"]
        );
        assert!(items.iter().all(|i| i.steps.is_none()));
    }

    #[test]
    fn a_tool_the_parent_lacks_is_not_reported() {
        assert_eq!(
            tool(fact(Observed::Unavailable, None), Some(avail("1"))),
            None
        );
        assert_eq!(tool(fact(Observed::Unavailable, None), None), None);
    }

    #[test]
    fn unknown_is_never_same() {
        let unknown = fact(Observed::Unknown, None);
        assert_eq!(
            class(unknown.clone(), Some(unknown.clone())),
            Class::Differs
        );
        assert_eq!(class(unknown.clone(), None), Class::Differs);
        assert_eq!(class(avail("1.0.0"), Some(unknown)), Class::Differs);
        // A worker that did not report the tool was not measured either.
        assert_eq!(class(avail("1.0.0"), None), Class::Differs);
        // Said as "could not be measured", never as an install instruction.
        for (want, have, side) in [
            (
                avail("1.0.0"),
                Some(fact(Observed::Unknown, None)),
                "the worker",
            ),
            (avail("1.0.0"), None, "the worker"),
            (fact(Observed::Unknown, None), None, "this machine"),
        ] {
            let steps = tool(want, have).unwrap().steps.unwrap();
            assert!(
                steps.contains(&format!("node could not be measured on {side}")),
                "{steps}"
            );
            assert!(!steps.contains("install"), "{steps}");
        }
    }

    #[test]
    fn equal_versions_are_same_and_other_versions_differ() {
        assert_eq!(class(avail("20.1.0"), Some(avail("20.1.0"))), Class::Same);
        assert_eq!(
            class(avail("20.1.0"), Some(avail("18.0.0"))),
            Class::Differs
        );
        let item = tool(avail("20.1.0"), Some(avail("18.0.0"))).unwrap();
        assert_eq!(item.parent.as_deref(), Some("20.1.0"));
        assert_eq!(item.worker.as_deref(), Some("18.0.0"));
        let steps = item.steps.unwrap();
        assert!(
            steps.contains("install node 20.1.0 on the worker"),
            "{steps}"
        );
        assert!(steps.contains("toolchains.toml"), "{steps}");
    }

    #[test]
    fn a_missing_tool_is_missing_auto_installable_or_manual() {
        let unavailable = fact(Observed::Unavailable, None);
        assert_eq!(
            class(avail("20.1.0"), Some(unavailable.clone())),
            Class::Missing
        );
        // A manager that may install it, but not this version.
        let mut other = unavailable.clone();
        other.managed = managed(&["18.0.0"], &[]);
        let item = tool(avail("20.1.0"), Some(other)).unwrap();
        assert_eq!(item.class, Class::Missing);
        assert_eq!(
            item.steps.as_deref(),
            Some("install node 20.1.0 on the worker so that it is on the profile PATH, then rerun workers diff")
        );
        let mut capable = unavailable;
        capable.managed = managed(&["18.0.0", "20.1.0"], &[]);
        let item = tool(avail("20.1.0"), Some(capable)).unwrap();
        assert_eq!(item.class, Class::AutoInstallable);
        assert_eq!(item.worker, None);
        assert!(item.steps.unwrap().contains("workers sync"));
    }

    #[test]
    fn an_installed_non_default_version_is_same() {
        let mut have = avail("18.0.0");
        have.managed = managed(&["18.0.0", "20.1.0"], &["18.0.0", "20.1.0"]);
        let item = tool(avail("20.1.0"), Some(have)).unwrap();
        assert_eq!(item.class, Class::Same);
        assert_eq!(item.worker.as_deref(), Some("18.0.0"));
        assert_eq!(item.steps, None);
    }

    #[test]
    fn a_version_installed_under_the_manager_is_same_even_off_the_profile_path() {
        // The normal case when adapters install privately: no default tool.
        for observed in [Observed::Unavailable, Observed::Unknown] {
            let mut have = fact(observed, None);
            have.managed = managed(&["20.1.0"], &["20.1.0"]);
            let item = tool(avail("20.1.0"), Some(have)).unwrap();
            assert_eq!(item.class, Class::Same, "{observed:?}");
            assert_eq!(item.worker, None);
            assert_eq!(item.steps, None);
        }
        // Another installed version does not count.
        let mut have = fact(Observed::Unavailable, None);
        have.managed = managed(&["18.0.0", "20.1.0"], &["18.0.0"]);
        assert_eq!(class(avail("20.1.0"), Some(have)), Class::AutoInstallable);
    }

    #[test]
    fn a_version_the_manager_may_install_is_auto_installable() {
        let mut have = avail("18.0.0");
        have.managed = managed(&["18.0.0", "20.1.0"], &["18.0.0"]);
        let item = tool(avail("20.1.0"), Some(have.clone())).unwrap();
        assert_eq!(item.class, Class::AutoInstallable);
        assert!(item.steps.unwrap().contains("workers sync"));
        // Not in the inventory and not installed: nothing can fix it.
        have.managed = managed(&["18.0.0"], &["18.0.0"]);
        let item = tool(avail("20.1.0"), Some(have)).unwrap();
        assert_eq!(item.class, Class::Differs);
        assert!(item.steps.unwrap().contains("install node 20.1.0"));
    }

    #[test]
    fn manual_tools_are_manual_whatever_the_version_manager_says() {
        let mut parent = report();
        parent.tools.insert("xcode".into(), avail("16.0"));
        for have in [avail("15.0"), fact(Observed::Unavailable, None)] {
            let mut worker = report();
            worker.tools.insert("xcode".into(), have);
            let item = diff(&parent, &worker)
                .into_iter()
                .find(|i| i.subject == "xcode")
                .unwrap();
            assert_eq!(item.class, Class::ManualRequired);
            assert_eq!(
                item.steps,
                Some(format!("{}{RERUN}", manual_steps("xcode").unwrap()))
            );
            let steps = item.steps.unwrap();
            assert!(steps.contains("sudo xcodebuild -license accept"), "{steps}");
        }
        // Even an offered version cannot make it automatic.
        let mut worker = report();
        let mut have = avail("15.0");
        have.managed = managed(&["16.0"], &[]);
        worker.tools.insert("xcode".into(), have);
        let item = diff(&parent, &worker)
            .into_iter()
            .find(|i| i.subject == "xcode")
            .unwrap();
        assert_eq!(item.class, Class::ManualRequired);
        let mut worker = report();
        let mut absent = fact(Observed::Unavailable, None);
        absent.managed = managed(&["16.0"], &[]);
        worker.tools.insert("xcode".into(), absent);
        let item = diff(&parent, &worker)
            .into_iter()
            .find(|i| i.subject == "xcode")
            .unwrap();
        assert_eq!(item.class, Class::ManualRequired);
        // Same version is same.
        let mut worker = report();
        worker.tools.insert("xcode".into(), avail("16.0"));
        assert!(diff(&parent, &worker)
            .iter()
            .all(|i| i.class == Class::Same));
    }

    #[test]
    fn standalone_helpers_do_not_contribute_a_diff() {
        let mut parent = report();
        let mut worker = report();
        for name in crate::workspace::embedded::NAMES {
            parent.tools.insert(name.into(), avail("0.5.0"));
            worker.tools.insert(name.into(), avail("8.9.0"));
        }
        assert!(diff(&parent, &worker)
            .iter()
            .all(|row| !crate::workspace::embedded::NAMES.contains(&row.subject.as_str())));
    }

    #[test]
    fn either_incompatible_registry_requires_a_manual_upgrade() {
        for (local, remote) in [
            (None, None),
            (Some("a"), None),
            (None, Some("b")),
            (Some("a"), Some("b")),
        ] {
            let mut parent = report();
            let mut worker = report();
            for (report, why) in [(&mut parent, local), (&mut worker, remote)] {
                if let Some(why) = why {
                    report
                        .helpers
                        .insert("portool_registry".into(), Some(why.into()));
                }
            }
            let rows: Vec<_> = diff(&parent, &worker)
                .into_iter()
                .filter(|i| i.subject == "portool_registry")
                .collect();
            if local.is_none() && remote.is_none() {
                assert!(rows.is_empty());
            } else {
                assert_eq!(rows.len(), 1);
                let row = &rows[0];
                assert_eq!(row.class, Class::ManualRequired);
                assert_eq!(row.parent.as_deref(), local);
                assert_eq!(row.worker.as_deref(), remote);
                assert!(row.steps.as_ref().unwrap().contains("upgrade belt"));
            }
        }
    }

    #[test]
    fn the_docker_tool_entry_is_covered_by_the_docker_field() {
        let mut parent = report();
        parent.tools.insert("docker".into(), avail("27.0.0"));
        let items = diff(&parent, &report());
        assert_eq!(items.iter().filter(|i| i.subject == "docker").count(), 1);
    }

    #[test]
    fn docker_runnability() {
        let mut worker = report();
        worker.docker = Docker::NotRunnable;
        let items = diff(&report(), &worker);
        let item = items.iter().find(|i| i.subject == "docker").unwrap();
        assert_eq!(item.class, Class::ManualRequired);
        let steps = item.steps.as_deref().unwrap();
        assert!(steps.contains("OrbStack or Colima"), "{steps}");
        assert!(steps.contains("`docker info` succeeds"), "{steps}");
        worker.docker = Docker::Unknown;
        let items = diff(&report(), &worker);
        let item = items.iter().find(|i| i.subject == "docker").unwrap();
        assert_eq!(item.class, Class::Differs);
        let steps = item.steps.as_deref().unwrap();
        assert!(
            steps.contains("docker could not be measured on the worker"),
            "{steps}"
        );
        assert!(!steps.contains("install"), "{steps}");
        // The parent does not run docker: nothing to compare.
        let mut parent = report();
        parent.docker = Docker::NotRunnable;
        assert!(diff(&parent, &worker).iter().all(|i| i.subject != "docker"));
        parent.docker = Docker::Unknown;
        assert!(diff(&parent, &worker).iter().all(|i| i.subject != "docker"));
    }

    #[test]
    fn the_built_in_build_identity_compares_by_version() {
        let mut parent = report();
        parent.helpers.insert("belt".into(), Some("0.13.0".into()));
        parent.helpers.insert("absent-here".into(), None);
        let mut worker = report();
        let find = |parent: &EnvReport, worker: &EnvReport| {
            diff(parent, worker)
                .into_iter()
                .find(|i| i.subject == "belt")
                .unwrap()
        };
        assert_eq!(find(&parent, &worker).class, Class::Missing);
        worker.helpers.insert("belt".into(), Some("0.12.0".into()));
        let item = find(&parent, &worker);
        assert_eq!(item.class, Class::Differs);
        assert!(item
            .steps
            .unwrap()
            .contains("install belt 0.13.0 on the worker"));
        assert!(diff(&parent, &worker)
            .iter()
            .all(|i| i.subject != "absent-here"));
        worker.helpers.insert("belt".into(), Some("0.13.0".into()));
        assert_eq!(find(&parent, &worker).class, Class::Same);
    }

    #[test]
    fn platform_and_disk() {
        let mut worker = report();
        worker.arch = "x86_64".into();
        worker.disk_free_bytes = Some(MIN_DISK_BYTES - 1);
        let items = diff(&report(), &worker);
        let get = |name: &str| items.iter().find(|i| i.subject == name).unwrap();
        assert_eq!(get("platform").class, Class::Differs);
        assert_eq!(get("platform").worker.as_deref(), Some("macos/x86_64"));
        assert!(get("platform")
            .steps
            .as_deref()
            .unwrap()
            .contains("needs its own profile"));
        assert_eq!(get("disk").class, Class::Differs);
        assert_eq!(
            get("disk").steps.as_deref(),
            Some("free at least 2 GiB on the worker's state volume")
        );
        worker.disk_free_bytes = None;
        let items = diff(&report(), &worker);
        let item = items.iter().find(|i| i.subject == "disk").unwrap();
        assert_eq!(item.class, Class::Missing);
        assert!(!item.steps.as_deref().unwrap().contains("install"));
    }

    #[test]
    fn the_profile_is_compared_by_name_and_required_tools_and_its_digest_is_shown_only() {
        let mut worker = report();
        // Hosts never share absolute paths, so the digest alone is no difference.
        worker.profile_digest = Some("sha256:other".into());
        let items = diff(&report(), &worker);
        let item = items.iter().find(|i| i.subject == "profile").unwrap();
        assert_eq!(item.class, Class::Same);
        assert!(item.parent.as_deref().unwrap().contains("sha256:p"));
        assert!(item.worker.as_deref().unwrap().contains("sha256:other"));
        assert!(all_same(&items));
        // The same tools in another order are the same set.
        let mut parent = report();
        parent.required_tools = vec!["git".into(), "node".into()];
        worker.required_tools = vec!["node".into(), "git".into()];
        assert!(all_same(&diff(&parent, &worker)));
        // A different name or tool set differs, with a step.
        worker.required_tools = vec!["node".into()];
        let items = diff(&parent, &worker);
        let item = items.iter().find(|i| i.subject == "profile").unwrap();
        assert_eq!(item.class, Class::Differs);
        assert!(item.steps.is_some());
        worker.required_tools = parent.required_tools.clone();
        worker.profile = "other".into();
        assert!(!all_same(&diff(&parent, &worker)));
        // A report without a digest still compares.
        worker.profile = "default".into();
        worker.profile_digest = None;
        assert!(all_same(&diff(&parent, &worker)));
    }

    #[test]
    fn the_policy_digest_is_not_measured_until_a_real_digest_exists() {
        let policy = |parent: Option<&str>, worker: Option<&str>| {
            let mut p = report();
            p.policy_digest = parent.map(str::to_string);
            let mut w = report();
            w.policy_digest = worker.map(str::to_string);
            let items = diff(&p, &w);
            let found = items.iter().find(|i| i.subject == "policy_digest").unwrap();
            (found.clone(), all_same(&items))
        };
        // Neither side has one: never `same`, but nothing to act on.
        let (item, green) = policy(None, None);
        assert_eq!(item.class, Class::NotMeasured);
        assert!(item.steps.is_some());
        assert!(green);
        assert_eq!(policy(Some("a"), None).0.class, Class::NotMeasured);
        assert_eq!(policy(None, Some("a")).0.class, Class::NotMeasured);
        // Real digests compare.
        assert_eq!(policy(Some("a"), Some("a")).0.class, Class::Same);
        let (item, green) = policy(Some("a"), Some("b"));
        assert_eq!(item.class, Class::Differs);
        assert!(!green);
        assert_eq!(
            serde_json::to_value(Class::NotMeasured).unwrap(),
            "not_measured"
        );
    }

    #[test]
    fn the_report_round_trips_and_rejects_unknown_fields() {
        let mut r = report();
        r.tools.insert("node".into(), {
            let mut f = avail("20.1.0");
            f.managed = managed(&["20.1.0"], &[]);
            f
        });
        let text = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<EnvReport>(&text).unwrap(), r);
        let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
        value["extra"] = 1.into();
        assert!(serde_json::from_value::<EnvReport>(value).is_err());
        assert_eq!(
            serde_json::to_value(Class::ManualRequired).unwrap(),
            "manual_required"
        );
        assert_eq!(
            serde_json::to_value(Class::AutoInstallable).unwrap(),
            "auto_installable"
        );
    }

    #[test]
    fn waiting_hints_name_a_procedure_within_128_characters() {
        for tool in crate::toolchain::capability::TOOLS {
            for unknown in [false, true] {
                let hint = waiting_hint(tool, unknown);
                assert!(hint.starts_with("how: "), "{hint}");
                assert!(hint.chars().count() <= 128, "{hint}");
            }
        }
        assert!(waiting_hint("xcode", false).contains("sudo xcodebuild -license accept"));
        assert!(waiting_hint("docker", false).contains("Colima"));
        assert_eq!(
            waiting_hint("node", false),
            "how: install node on the worker so that it is on the profile PATH"
        );
        // Not measured is no reason to install.
        assert_eq!(
            waiting_hint("node", true),
            "how: node could not be measured on the worker; rerun workers diff, or check its profile PATH"
        );
        // Counted in characters, not bytes.
        let long = "é".repeat(200);
        let hint = waiting_hint(&long, false);
        assert_eq!(hint.chars().count(), 128);
        let fixed = "how: install  on the worker so that it is on the profile PATH"
            .chars()
            .count();
        let exact = "é".repeat(128 - fixed);
        assert_eq!(waiting_hint(&exact, false).chars().count(), 128);
        let short = "é".repeat(127 - fixed);
        assert_eq!(waiting_hint(&short, false).chars().count(), 127);
    }

    fn capabilities(tools: &[(&str, ToolObservation)]) -> WorkerCapabilities {
        use crate::ids::Digest;
        WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            cpu_cores: None,
            memory_bytes: None,
            schema_version: 1,
            os: "macos".into(),
            arch: "aarch64".into(),
            profile: "default".into(),
            profile_digest: Digest::of_bytes(b"definition"),
            execution_uid: 1,
            protocol_versions: vec![1],
            profile_fingerprint: Digest::of_bytes(b"p"),
            tools: tools
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            features: [capability::FEATURE.to_string()].into(),
            rustup: Vec::new(),
        }
    }

    #[test]
    fn with_steps_adds_one_hint_after_the_unmet_tool() {
        use capability::ExecutionRequirements;
        let requires = vec![
            Requirement::Os("windows".into()),
            Requirement::Tool("xcode".into()),
            Requirement::Tool("docker".into()),
        ];
        let req = ExecutionRequirements {
            requires: requires.clone(),
            ..Default::default()
        };
        let caps = capabilities(&[]);
        // The lines of the real `missing`, not a copy of its format.
        let unmet = capability::missing(&req, Some(&caps));
        assert_eq!(unmet.len(), 3);
        let out = with_steps(unmet.clone(), &requires, Some(&caps));
        assert_eq!(out.len(), 4);
        assert_eq!(out[..2], unmet[..2]);
        assert_eq!(out[2], waiting_hint("xcode", false));
        assert_eq!(out[3], unmet[2]);
        // A tool that is not in the unmet lines gets no hint.
        let out = with_steps(unmet[..1].to_vec(), &requires, Some(&caps));
        assert_eq!(out.len(), 1);
        assert_eq!(
            with_steps(Vec::new(), &requires, Some(&caps)),
            Vec::<String>::new()
        );
        // The hint follows the first requirement that is unmet, in order.
        let out = with_steps(unmet[2..].to_vec(), &requires, Some(&caps));
        assert_eq!(out[1], waiting_hint("docker", false));
        // No report: the single "report unavailable" line has no tool to name.
        let out = with_steps(capability::missing(&req, None), &requires, None);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn with_steps_says_not_measured_for_an_unknown_observation() {
        use capability::ExecutionRequirements;
        let requires = vec![Requirement::Tool("node".into())];
        let req = ExecutionRequirements {
            requires: requires.clone(),
            ..Default::default()
        };
        for (observation, unknown) in [
            (ToolObservation::Unknown, true),
            (ToolObservation::Unavailable, false),
        ] {
            let caps = capabilities(&[("node", observation)]);
            let out = with_steps(
                capability::missing(&req, Some(&caps)),
                &requires,
                Some(&caps),
            );
            assert_eq!(out.len(), 2);
            assert_eq!(out[1], waiting_hint("node", unknown));
        }
        // A tool the report lacks entirely is not "unknown".
        let caps = capabilities(&[]);
        let out = with_steps(
            capability::missing(&req, Some(&caps)),
            &requires,
            Some(&caps),
        );
        assert_eq!(out[1], waiting_hint("node", false));
    }
    #[test]
    fn detected_rustup_toolchains_are_shown_and_never_fail() {
        let parent = report();
        let mut worker = report();
        assert!(diff(&parent, &worker).iter().all(|i| i.subject != "rustup"));
        worker.rustup = vec!["1.89.0 (1.89.0-aarch64-apple-darwin)".into()];
        let items = diff(&parent, &worker);
        let row = items.iter().find(|i| i.subject == "rustup").unwrap();
        assert_eq!(row.class, Class::Same);
        assert_eq!(row.parent, None);
        assert_eq!(
            row.worker.as_deref(),
            Some("1.89.0 (1.89.0-aarch64-apple-darwin)")
        );
    }
}
