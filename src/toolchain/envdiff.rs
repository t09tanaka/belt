//! Environment difference between the parent and a worker (`workers diff`).
//! Pure: two serializable reports in, classified items out. No process, file or
//! network access, so `doctor` can call `diff` on reports it already holds.
use super::capability::Requirement;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Versions of portool-adjacent helpers: task-traffic-controller,
    /// local-ci and remote-task-runner itself. `None`: not found.
    pub helpers: BTreeMap<String, Option<String>>,
    pub docker: Docker,
    pub disk_free_bytes: Option<u64>,
    pub profile_digest: Option<String>,
    pub policy_digest: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Same,
    Differs,
    Missing,
    AutoInstallable,
    ManualRequired,
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

/// The manual procedure for a tool that is never installed automatically.
pub fn manual_steps(tool: &str) -> Option<&'static str> {
    match tool {
        "xcode" => Some("install Xcode and accept its license on the worker"),
        "docker" => Some("install and start Docker Desktop on the worker"),
        _ => None,
    }
}

fn steps_for(tool: &str) -> String {
    manual_steps(tool).map_or_else(
        || format!("install {tool} on the worker, then rerun workers diff"),
        str::to_string,
    )
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
    let manual = MANUAL_TOOLS.contains(&name);
    let auto = steps_for_auto();
    let unmeasured = |side: &str| {
        item(
            name,
            Class::Differs,
            wanted,
            have.and_then(|h| h.version.as_deref()),
            Some(format!(
                "{side} could not measure {name}; rerun workers diff"
            )),
        )
    };
    if want.observed == Observed::Unknown {
        return Some(unmeasured("the parent"));
    }
    let Some(have) = have.filter(|h| h.observed != Observed::Unavailable) else {
        // Not on the worker (or not reported at all).
        let managed = have.and_then(|h| h.managed.as_ref());
        let class = if manual {
            Class::ManualRequired
        } else if wanted
            .is_some_and(|v| managed.is_some_and(|m| m.inventory.iter().any(|i| i == v)))
        {
            Class::AutoInstallable
        } else {
            Class::Missing
        };
        let steps = if class == Class::AutoInstallable {
            auto
        } else {
            steps_for(name)
        };
        return Some(item(name, class, wanted, None, Some(steps)));
    };
    if have.observed == Observed::Unknown {
        return Some(unmeasured("the worker"));
    }
    if have.version.as_deref() == wanted {
        return Some(item(name, Class::Same, wanted, wanted, None));
    }
    let managed = have.managed.as_ref();
    if wanted.is_some_and(|v| managed.is_some_and(|m| m.installed.iter().any(|i| i == v))) {
        // Installed under the version manager: an attempt activates it.
        return Some(item(
            name,
            Class::Same,
            wanted,
            have.version.as_deref(),
            None,
        ));
    }
    let class = if manual {
        Class::ManualRequired
    } else if wanted.is_some_and(|v| managed.is_some_and(|m| m.inventory.iter().any(|i| i == v))) {
        Class::AutoInstallable
    } else {
        Class::Differs
    };
    let steps = match class {
        Class::AutoInstallable => auto,
        _ => steps_for(name),
    };
    Some(item(
        name,
        class,
        wanted,
        have.version.as_deref(),
        Some(steps),
    ))
}

fn steps_for_auto() -> String {
    "run workers sync to install it with the version manager".into()
}

fn value_item(subject: &str, want: Option<&str>, have: Option<&str>) -> Option<Item> {
    let want = want?;
    Some(match have {
        None => item(
            subject,
            Class::Missing,
            Some(want),
            None,
            Some(format!("the worker did not report {subject}")),
        ),
        Some(h) if h == want => item(subject, Class::Same, Some(want), Some(h), None),
        Some(h) => item(
            subject,
            Class::Differs,
            Some(want),
            Some(h),
            Some(format!("align {subject} on the worker")),
        ),
    })
}

/// Difference between the parent (`want`) and a worker (`have`), in a fixed
/// order: platform, tools by name, helpers by name, docker, disk, profile
/// digest, policy digest. Every item carries one class; `Unknown` is never
/// `Same`.
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
            Some("use a worker with the same OS and architecture".into()),
        )
    });
    for (name, fact) in &want.tools {
        if name == "docker" {
            continue;
        }
        items.extend(tool_item(name, fact, have.tools.get(name)));
    }
    for (name, version) in &want.helpers {
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
                manual_steps("docker").map(str::to_string),
            ),
            Docker::Unknown => item(
                "docker",
                Class::Differs,
                Some("runnable"),
                Some("unknown"),
                Some("the worker could not tell; rerun workers diff".into()),
            ),
        });
    }
    items.push(match have.disk_free_bytes {
        None => item(
            "disk",
            Class::Missing,
            None,
            None,
            Some("the worker did not report free disk".into()),
        ),
        Some(b) if b < MIN_DISK_BYTES => item(
            "disk",
            Class::Differs,
            None,
            Some(&b.to_string()),
            Some("free disk on the worker".into()),
        ),
        Some(b) => item("disk", Class::Same, None, Some(&b.to_string()), None),
    });
    items.extend(value_item(
        "profile_digest",
        want.profile_digest.as_deref(),
        have.profile_digest.as_deref(),
    ));
    items.extend(value_item(
        "policy_digest",
        want.policy_digest.as_deref(),
        have.policy_digest.as_deref(),
    ));
    items
}

/// True when nothing needs attention.
pub fn all_same(items: &[Item]) -> bool {
    items.iter().all(|i| i.class == Class::Same)
}

/// One short line (at most 128 characters) saying how to resolve an unmet
/// tool requirement a worker cannot install by itself, for the waiting reason
/// of a task.
pub fn waiting_hint(tool: &str) -> String {
    let steps = steps_for(tool);
    let line = format!("how: {steps}");
    line.chars().take(128).collect()
}

/// `unmet` as `capability::missing` produced it, with one hint inserted after
/// the first line naming an unmet tool requirement (so the display cap of four
/// lines keeps it). Requirements other than tools carry no procedure.
pub fn with_steps(mut unmet: Vec<String>, requires: &[Requirement]) -> Vec<String> {
    let named = requires.iter().find_map(|r| match r {
        Requirement::Tool(name) => {
            let line = format!("{r:?} unavailable");
            unmet
                .iter()
                .position(|u| *u == line)
                .map(|at| (at, name.as_str()))
        }
        _ => None,
    });
    if let Some((at, tool)) = named {
        let hint = waiting_hint(tool);
        unmet.insert(at + 1, hint);
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
            profile_digest: Some("sha256:p".into()),
            policy_digest: Some("sha256:t".into()),
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
        assert_eq!(
            items.iter().map(|i| i.subject.as_str()).collect::<Vec<_>>(),
            [
                "platform",
                "docker",
                "disk",
                "profile_digest",
                "policy_digest"
            ]
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
        let item = tool(avail("1.0.0"), Some(fact(Observed::Unknown, None))).unwrap();
        assert!(item.steps.unwrap().contains("the worker could not measure"));
        let item = tool(fact(Observed::Unknown, None), None).unwrap();
        assert!(item.steps.unwrap().contains("the parent could not measure"));
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
        assert!(item.steps.unwrap().contains("install node"));
    }

    #[test]
    fn a_missing_tool_is_missing_auto_installable_or_manual() {
        assert_eq!(class(avail("20.1.0"), None), Class::Missing);
        let unavailable = fact(Observed::Unavailable, None);
        assert_eq!(
            class(avail("20.1.0"), Some(unavailable.clone())),
            Class::Missing
        );
        // A manager that may install it, but not this version.
        let mut other = unavailable.clone();
        other.managed = managed(&["18.0.0"], &[]);
        assert_eq!(class(avail("20.1.0"), Some(other)), Class::Missing);
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
    fn a_version_the_manager_may_install_is_auto_installable() {
        let mut have = avail("18.0.0");
        have.managed = managed(&["18.0.0", "20.1.0"], &["18.0.0"]);
        assert_eq!(
            class(avail("20.1.0"), Some(have.clone())),
            Class::AutoInstallable
        );
        // Not in the inventory and not installed: nothing can fix it.
        have.managed = managed(&["18.0.0"], &["18.0.0"]);
        assert_eq!(class(avail("20.1.0"), Some(have)), Class::Differs);
    }

    #[test]
    fn manual_tools_are_manual_whatever_the_version_manager_says() {
        let mut parent = report();
        parent.tools.insert("xcode".into(), avail("16.0"));
        for have in [
            None,
            Some(avail("15.0")),
            Some(fact(Observed::Unavailable, None)),
        ] {
            let mut worker = report();
            if let Some(h) = have {
                worker.tools.insert("xcode".into(), h);
            }
            let item = diff(&parent, &worker)
                .into_iter()
                .find(|i| i.subject == "xcode")
                .unwrap();
            assert_eq!(item.class, Class::ManualRequired);
            assert_eq!(item.steps.as_deref(), manual_steps("xcode"));
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
        assert_eq!(item.steps.as_deref(), manual_steps("docker"));
        worker.docker = Docker::Unknown;
        let items = diff(&report(), &worker);
        let item = items.iter().find(|i| i.subject == "docker").unwrap();
        assert_eq!(item.class, Class::Differs);
        // The parent does not run docker: nothing to compare.
        let mut parent = report();
        parent.docker = Docker::NotRunnable;
        assert!(diff(&parent, &worker).iter().all(|i| i.subject != "docker"));
        parent.docker = Docker::Unknown;
        assert!(diff(&parent, &worker).iter().all(|i| i.subject != "docker"));
    }

    #[test]
    fn helpers_compare_by_version() {
        let mut parent = report();
        parent
            .helpers
            .insert("local-ci".into(), Some("0.13.0".into()));
        parent
            .helpers
            .insert("task-traffic-controller".into(), Some("0.8.2".into()));
        parent.helpers.insert("absent-here".into(), None);
        let mut worker = report();
        worker
            .helpers
            .insert("local-ci".into(), Some("0.12.0".into()));
        let items = diff(&parent, &worker);
        let get = |name: &str| items.iter().find(|i| i.subject == name).unwrap();
        assert_eq!(get("local-ci").class, Class::Differs);
        assert_eq!(get("task-traffic-controller").class, Class::Missing);
        assert!(items.iter().all(|i| i.subject != "absent-here"));
        worker
            .helpers
            .insert("local-ci".into(), Some("0.13.0".into()));
        worker
            .helpers
            .insert("task-traffic-controller".into(), None);
        let items = diff(&parent, &worker);
        let get = |name: &str| items.iter().find(|i| i.subject == name).unwrap();
        assert_eq!(get("local-ci").class, Class::Same);
        assert_eq!(get("task-traffic-controller").class, Class::Missing);
    }

    #[test]
    fn platform_disk_and_digests() {
        let mut worker = report();
        worker.arch = "x86_64".into();
        worker.disk_free_bytes = Some(MIN_DISK_BYTES - 1);
        worker.profile_digest = Some("sha256:other".into());
        worker.policy_digest = None;
        let items = diff(&report(), &worker);
        let get = |name: &str| items.iter().find(|i| i.subject == name).unwrap();
        assert_eq!(get("platform").class, Class::Differs);
        assert_eq!(get("platform").worker.as_deref(), Some("macos/x86_64"));
        assert_eq!(get("disk").class, Class::Differs);
        assert_eq!(get("profile_digest").class, Class::Differs);
        assert_eq!(get("policy_digest").class, Class::Missing);
        worker.disk_free_bytes = None;
        assert_eq!(
            diff(&report(), &worker)
                .iter()
                .find(|i| i.subject == "disk")
                .unwrap()
                .class,
            Class::Missing
        );
        // A parent without a digest has nothing to compare.
        let mut parent = report();
        parent.profile_digest = None;
        parent.policy_digest = None;
        assert!(diff(&parent, &worker)
            .iter()
            .all(|i| i.subject != "profile_digest" && i.subject != "policy_digest"));
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
            let hint = waiting_hint(tool);
            assert!(hint.starts_with("how: "), "{hint}");
            assert!(hint.chars().count() <= 128, "{hint}");
        }
        assert!(waiting_hint("xcode").contains("accept its license"));
        assert!(waiting_hint("docker").contains("Docker Desktop"));
        assert!(waiting_hint("node").contains("install node"));
        // Counted in characters, not bytes.
        let long = "é".repeat(200);
        let hint = waiting_hint(&long);
        assert_eq!(hint.chars().count(), 128);
        let exact = "é".repeat(
            128 - "how: install  on the worker, then rerun workers diff"
                .chars()
                .count(),
        );
        assert_eq!(waiting_hint(&exact).chars().count(), 128);
        let short = "é".repeat(
            127 - "how: install  on the worker, then rerun workers diff"
                .chars()
                .count(),
        );
        assert_eq!(waiting_hint(&short).chars().count(), 127);
    }

    #[test]
    fn with_steps_adds_one_hint_after_the_unmet_tool() {
        let requires = vec![
            Requirement::Os("macos".into()),
            Requirement::Tool("xcode".into()),
            Requirement::Tool("docker".into()),
        ];
        let unmet = vec![
            "Os(\"macos\") unavailable".to_string(),
            "Tool(\"xcode\") unavailable".to_string(),
            "Tool(\"docker\") unavailable".to_string(),
        ];
        let out = with_steps(unmet.clone(), &requires);
        assert_eq!(out.len(), 4);
        assert_eq!(out[..2], unmet[..2]);
        assert_eq!(out[2], waiting_hint("xcode"));
        assert_eq!(out[3], unmet[2]);
        // A tool that is not in the unmet lines gets no hint.
        let out = with_steps(vec!["Os(\"macos\") unavailable".into()], &requires);
        assert_eq!(out.len(), 1);
        assert_eq!(with_steps(Vec::new(), &requires), Vec::<String>::new());
        // The hint follows the first requirement that is unmet, in order.
        let out = with_steps(vec!["Tool(\"docker\") unavailable".into()], &requires);
        assert_eq!(out[1], waiting_hint("docker"));
    }
}
