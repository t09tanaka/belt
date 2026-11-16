//! What `workers sync` does about a difference: install what a worker's
//! version manager may install, and say exactly what a person must do for the
//! rest. Pure: reports in, a plan out; the installing itself is injected.
use super::contract::canonical_tool;
use super::envdiff::{self, Class, EnvReport, ToolFact};
use super::versions::{EffectiveRequirement, Match, ToolId, VersionObservation};
use node_semver::Version;
use serde::{Deserialize, Serialize};

/// Reported by `diff`, but nothing `sync` could change.
const NOT_SYNCED: &[&str] = &["platform", "disk", "profile_digest", "policy_digest"];

/// One version to install through the worker's version manager.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallStep {
    /// The manager's tool id (`node`, `rust`, ...).
    pub tool: String,
    pub version: String,
}

/// A difference `sync` leaves alone, with what to do about it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocked {
    pub subject: String,
    pub missing: String,
    pub steps: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPlan {
    pub install: Vec<InstallStep>,
    pub blocked: Vec<Blocked>,
}

/// The worker's fact for a manager tool id: the first entry (by name) that
/// has a version manager, else the first entry at all.
fn worker_fact<'a>(worker: &'a EnvReport, tool: &str) -> Option<&'a ToolFact> {
    let mut entries = worker
        .tools
        .iter()
        .filter(|(name, _)| canonical_tool(name) == tool)
        .map(|(_, fact)| fact);
    let all: Vec<&ToolFact> = entries.by_ref().collect();
    all.iter()
        .find(|f| f.managed.is_some())
        .or_else(|| all.first())
        .copied()
}

fn satisfies(tool: &ToolId, constraints: &[super::versions::Constraint], version: &str) -> bool {
    let observed = VersionObservation::parse_for(tool, version);
    constraints
        .iter()
        .all(|c| c.matches(&observed) == Match::Match)
}

fn blocked(subject: &str, missing: String, steps: String) -> Blocked {
    Blocked {
        subject: subject.into(),
        missing,
        steps,
    }
}

/// A project requirement against what the worker has and may install.
fn project_step(worker: &EnvReport, req: &EffectiveRequirement, plan: &mut SyncPlan) {
    let tool = &req.tool;
    let name = tool.as_str();
    let fact = worker_fact(worker, name);
    if let Some(fact) = fact {
        let present = fact
            .version
            .iter()
            .chain(fact.managed.iter().flat_map(|m| m.installed.iter()));
        if fact.observed == envdiff::Observed::Available
            && present
                .into_iter()
                .any(|v| satisfies(tool, &req.constraints, v))
        {
            return;
        }
    }
    let candidates = fact
        .and_then(|f| f.managed.as_ref())
        .map(|m| m.inventory.as_slice());
    match candidates {
        Some(inventory) => {
            let best = inventory
                .iter()
                .filter(|v| satisfies(tool, &req.constraints, v))
                .filter_map(|v| Version::parse(v).ok().map(|parsed| (parsed, v)))
                .max_by(|a, b| a.0.cmp(&b.0));
            match best {
                Some((_, version)) => plan.install.push(InstallStep {
                    tool: name.into(),
                    version: version.clone(),
                }),
                None => plan.blocked.push(blocked(
                    name,
                    format!("no version of {name} the worker may install satisfies the project requirement"),
                    format!("authorize a matching {name} version in the worker's toolchains.toml, then rerun workers sync"),
                )),
            }
        }
        None => plan.blocked.push(blocked(
            name,
            format!("the worker has no {name} for the project requirement and no version manager for it"),
            envdiff::manual_steps(name).map_or_else(
                || format!("install {name} on the worker, then rerun workers sync"),
                str::to_string,
            ),
        )),
    }
}

fn add_install(plan: &mut SyncPlan, step: InstallStep) {
    if !plan.install.contains(&step) {
        plan.install.push(step);
    }
}

/// The plan for bringing `worker` to `parent`, or, with `project`, to the
/// project's requirements for each tool it declares. Tools the project does
/// not declare, helpers and docker still follow the parent.
pub fn plan(
    parent: &EnvReport,
    worker: &EnvReport,
    project: Option<&[EffectiveRequirement]>,
) -> SyncPlan {
    let mut plan = SyncPlan::default();
    let declared = |subject: &str| {
        project.is_some_and(|reqs| {
            reqs.iter()
                .any(|r| r.tool.as_str() == canonical_tool(subject))
        })
    };
    for item in envdiff::diff(parent, worker) {
        if NOT_SYNCED.contains(&item.subject.as_str()) || declared(&item.subject) {
            continue;
        }
        let steps = item.steps.clone().unwrap_or_default();
        match item.class {
            Class::Same => {}
            Class::AutoInstallable => add_install(
                &mut plan,
                InstallStep {
                    tool: canonical_tool(&item.subject).into(),
                    version: item.parent.clone().unwrap_or_default(),
                },
            ),
            Class::Differs | Class::Missing | Class::ManualRequired => plan.blocked.push(blocked(
                &item.subject,
                format!(
                    "{}: parent {}, worker {}",
                    item.subject,
                    item.parent.as_deref().unwrap_or("-"),
                    item.worker.as_deref().unwrap_or("-")
                ),
                steps,
            )),
        }
    }
    for req in project.unwrap_or_default() {
        project_step(worker, req, &mut plan);
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolchain::envdiff::{Docker, Managed, Observed, SCHEMA};
    use crate::toolchain::versions::Constraint;
    use std::collections::BTreeMap;

    fn report() -> EnvReport {
        EnvReport {
            schema_version: SCHEMA,
            os: "macos".into(),
            arch: "aarch64".into(),
            tools: BTreeMap::new(),
            helpers: BTreeMap::new(),
            docker: Docker::Runnable,
            disk_free_bytes: Some(envdiff::MIN_DISK_BYTES),
            profile_digest: Some("sha256:p".into()),
            policy_digest: Some("sha256:t".into()),
        }
    }
    fn fact(
        observed: Observed,
        version: Option<&str>,
        managed: Option<(&[&str], &[&str])>,
    ) -> ToolFact {
        ToolFact {
            observed,
            version: version.map(str::to_string),
            managed: managed.map(|(inventory, installed)| Managed {
                inventory: inventory.iter().map(|v| v.to_string()).collect(),
                installed: installed.iter().map(|v| v.to_string()).collect(),
            }),
        }
    }
    fn req(tool: &str, range: &str) -> EffectiveRequirement {
        EffectiveRequirement {
            tool: ToolId::parse(tool).unwrap(),
            constraints: vec![Constraint::parse(range).unwrap()],
            provenance: vec![],
        }
    }
    fn step(tool: &str, version: &str) -> InstallStep {
        InstallStep {
            tool: tool.into(),
            version: version.into(),
        }
    }

    #[test]
    fn nothing_to_do_when_the_reports_match() {
        assert_eq!(plan(&report(), &report(), None), SyncPlan::default());
    }

    #[test]
    fn the_parent_version_is_installed_when_the_manager_may() {
        let mut parent = report();
        parent.tools.insert(
            "node".into(),
            fact(Observed::Available, Some("20.1.0"), None),
        );
        let mut worker = report();
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("18.0.0"),
                Some((&["18.0.0", "20.1.0"], &["18.0.0"])),
            ),
        );
        let got = plan(&parent, &worker, None);
        assert_eq!(got.install, [step("node", "20.1.0")]);
        assert!(got.blocked.is_empty());
    }

    #[test]
    fn rustc_and_cargo_make_one_rust_install() {
        let mut parent = report();
        let mut worker = report();
        for name in ["rustc", "cargo"] {
            parent
                .tools
                .insert(name.into(), fact(Observed::Available, Some("1.80.0"), None));
            worker.tools.insert(
                name.into(),
                fact(Observed::Unavailable, None, Some((&["1.80.0"], &[]))),
            );
        }
        assert_eq!(
            plan(&parent, &worker, None).install,
            [step("rust", "1.80.0")]
        );
    }

    #[test]
    fn what_cannot_be_installed_is_blocked_with_its_steps() {
        let mut parent = report();
        parent.tools.insert(
            "xcode".into(),
            fact(Observed::Available, Some("16.0.0"), None),
        );
        parent.tools.insert(
            "git".into(),
            fact(Observed::Available, Some("2.40.0"), None),
        );
        parent
            .helpers
            .insert("local-ci".into(), Some("0.13.0".into()));
        let mut worker = report();
        worker.docker = Docker::NotRunnable;
        parent.docker = Docker::Runnable;
        let got = plan(&parent, &worker, None);
        assert!(got.install.is_empty());
        let subjects: Vec<&str> = got.blocked.iter().map(|b| b.subject.as_str()).collect();
        assert_eq!(subjects, ["git", "xcode", "local-ci", "docker"]);
        let xcode = got.blocked.iter().find(|b| b.subject == "xcode").unwrap();
        assert!(xcode.steps.contains("accept its license"));
        assert!(xcode.missing.contains("parent 16.0.0, worker -"));
        let docker = got.blocked.iter().find(|b| b.subject == "docker").unwrap();
        assert!(docker.steps.contains("Docker Desktop"));
    }

    #[test]
    fn platform_disk_and_digests_are_not_synced() {
        let mut worker = report();
        worker.arch = "x86_64".into();
        worker.disk_free_bytes = Some(0);
        worker.profile_digest = Some("sha256:other".into());
        worker.policy_digest = None;
        assert_eq!(plan(&report(), &worker, None), SyncPlan::default());
    }

    #[test]
    fn an_auto_installable_helper_is_not_installed_by_a_version_manager() {
        // Only tools go through an adapter; a helper that differs is blocked.
        let mut parent = report();
        parent
            .helpers
            .insert("local-ci".into(), Some("0.13.0".into()));
        let mut worker = report();
        worker
            .helpers
            .insert("local-ci".into(), Some("0.12.0".into()));
        let got = plan(&parent, &worker, None);
        assert!(got.install.is_empty());
        assert_eq!(got.blocked.len(), 1);
        assert_eq!(
            got.blocked[0].missing,
            "local-ci: parent 0.13.0, worker 0.12.0"
        );
    }

    #[test]
    fn a_project_requirement_picks_the_best_authorized_version() {
        let mut worker = report();
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("18.0.0"),
                Some((&["18.0.0", "20.1.0", "20.9.0", "21.0.0"], &["18.0.0"])),
            ),
        );
        let reqs = [req("node", "^20")];
        let got = plan(&report(), &worker, Some(&reqs));
        assert_eq!(got.install, [step("node", "20.9.0")]);
        assert!(got.blocked.is_empty());
    }

    #[test]
    fn a_project_requirement_already_met_installs_nothing() {
        let mut worker = report();
        // The active version.
        worker.tools.insert(
            "node".into(),
            fact(Observed::Available, Some("20.1.0"), None),
        );
        assert_eq!(
            plan(&report(), &worker, Some(&[req("node", "^20")])),
            SyncPlan::default()
        );
        // An installed, non-default version.
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("18.0.0"),
                Some((&["18.0.0", "20.1.0"], &["18.0.0", "20.1.0"])),
            ),
        );
        assert_eq!(
            plan(&report(), &worker, Some(&[req("node", "^20")])),
            SyncPlan::default()
        );
        // A version that is only offered is not present.
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("18.0.0"),
                Some((&["18.0.0", "20.1.0"], &["18.0.0"])),
            ),
        );
        assert_eq!(
            plan(&report(), &worker, Some(&[req("node", "^20")])).install,
            [step("node", "20.1.0")]
        );
        // A tool that is not observed does not count as present.
        worker.tools.insert(
            "node".into(),
            fact(Observed::Unknown, Some("20.1.0"), Some((&["20.2.0"], &[]))),
        );
        assert_eq!(
            plan(&report(), &worker, Some(&[req("node", "^20")])).install,
            [step("node", "20.2.0")]
        );
    }

    #[test]
    fn a_project_requirement_nothing_can_satisfy_is_blocked() {
        let mut worker = report();
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("18.0.0"),
                Some((&["18.0.0"], &["18.0.0"])),
            ),
        );
        let got = plan(&report(), &worker, Some(&[req("node", "^20")]));
        assert!(got.install.is_empty());
        assert_eq!(got.blocked.len(), 1);
        assert!(got.blocked[0].missing.contains("may install satisfies"));
        assert!(got.blocked[0].steps.contains("toolchains.toml"));
        // No version manager at all.
        let got = plan(&report(), &report(), Some(&[req("node", "^20")]));
        assert!(got.blocked[0].missing.contains("no version manager"));
        assert!(got.blocked[0].steps.contains("install node on the worker"));
        let got = plan(&report(), &report(), Some(&[req("xcode", "^16")]));
        assert!(got.blocked[0].steps.contains("accept its license"));
    }

    #[test]
    fn an_alias_is_not_a_version_and_is_never_installed() {
        let mut worker = report();
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("18.0.0"),
                Some((&["18.0.0", "20.1.0"], &["18.0.0"])),
            ),
        );
        let alias = EffectiveRequirement {
            tool: ToolId::parse("node").unwrap(),
            constraints: vec![Constraint::Resolve("lts".into())],
            provenance: vec![],
        };
        let got = plan(&report(), &worker, Some(&[alias]));
        assert!(got.install.is_empty());
        assert_eq!(got.blocked.len(), 1);
    }

    #[test]
    fn a_declared_tool_follows_the_project_not_the_parent() {
        let mut parent = report();
        parent.tools.insert(
            "node".into(),
            fact(Observed::Available, Some("22.0.0"), None),
        );
        parent.tools.insert(
            "git".into(),
            fact(Observed::Available, Some("2.40.0"), None),
        );
        let mut worker = report();
        worker.tools.insert(
            "node".into(),
            fact(
                Observed::Available,
                Some("20.1.0"),
                Some((&["20.1.0", "22.0.0"], &["20.1.0"])),
            ),
        );
        worker.tools.insert(
            "git".into(),
            fact(Observed::Available, Some("2.40.0"), None),
        );
        // The parent runs 22, the project wants 20: nothing to do for node.
        assert_eq!(
            plan(&parent, &worker, Some(&[req("node", "^20")])),
            SyncPlan::default()
        );
        // Without the project the parent decides.
        assert_eq!(
            plan(&parent, &worker, None).install,
            [step("node", "22.0.0")]
        );
        // `rustc` in the diff is the project's `rust`.
        parent.tools.insert(
            "rustc".into(),
            fact(Observed::Available, Some("1.80.0"), None),
        );
        worker.tools.insert(
            "rustc".into(),
            fact(Observed::Available, Some("1.70.0"), None),
        );
        let got = plan(&parent, &worker, Some(&[req("rust", "^1.70")]));
        // Only node, which the project does not declare, follows the parent.
        assert_eq!(got.install, [step("node", "22.0.0")]);
        assert!(got.blocked.is_empty());
    }
}
