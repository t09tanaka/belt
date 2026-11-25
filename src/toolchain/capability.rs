//! Bounded requirements shared by placement and worker admission.
use crate::ids::Digest;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub const FEATURE: &str = "worker_capabilities_v1";
pub const FEATURES: &[&str] = &[
    crate::wire::PREPARATION_PROBE_FEATURE,
    crate::setup::observation_transport::FEATURE,
    FEATURE,
    crate::toolchain::contract::FEATURE,
    "blob_resume",
    "log_resume",
    crate::forecast::COMMAND_FEATURE,
];
pub const TOOLS: &[&str] = &[
    "node",
    "npm",
    "pnpm",
    "yarn",
    "rustc",
    "cargo",
    "flutter",
    "dart",
    "xcode",
    "git",
    "portool",
    "fast-worktree",
    "docker",
];
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum Requirement {
    Os(String),
    Arch(String),
    Tool(String),
    Feature(String),
}
fn os(v: &str) -> Option<&str> {
    match v {
        "macos" | "linux" | "windows" => Some(v),
        _ => None,
    }
}
fn arch(v: &str) -> Option<&str> {
    match v {
        "arm64" | "aarch64" => Some("aarch64"),
        "x86_64" | "amd64" => Some("x86_64"),
        _ => None,
    }
}
impl Requirement {
    pub fn parse(value: &str) -> Result<Self, String> {
        let (prefix, name) = value.split_once(':').unwrap_or(("", value));
        let parsed = match prefix {
            "" => os(name)
                .map(|v| Self::Os(v.into()))
                .or_else(|| arch(name).map(|v| Self::Arch(v.into())))
                .or_else(|| TOOLS.contains(&name).then(|| Self::Tool(name.into()))),
            "os" => os(name).map(|v| Self::Os(v.into())),
            "arch" => arch(name).map(|v| Self::Arch(v.into())),
            "tool" => TOOLS.contains(&name).then(|| Self::Tool(name.into())),
            "feature" => FEATURES.contains(&name).then(|| Self::Feature(name.into())),
            _ => None,
        };
        parsed.ok_or_else(|| format!("unknown execution requirement {value:?}"))
    }
    pub fn validate(&self) -> bool {
        match self {
            Self::Os(v) => os(v) == Some(v.as_str()),
            Self::Arch(v) => arch(v) == Some(v.as_str()),
            Self::Tool(v) => TOOLS.contains(&v.as_str()),
            Self::Feature(v) => FEATURES.contains(&v.as_str()),
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRequirements {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<Requirement>,
}
impl ExecutionRequirements {
    pub fn parse(profile: Option<String>, requires: &[String]) -> Result<Self, String> {
        let requires = requires
            .iter()
            .map(|v| Requirement::parse(v))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .collect();
        let result = Self { profile, requires };
        result.validate()?;
        Ok(result)
    }
    pub fn is_empty(&self) -> bool {
        self.profile.is_none() && self.requires.is_empty()
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.profile.as_ref().is_some_and(|v| !valid_name(v)) {
            return Err("invalid worker profile selector".into());
        }
        if self.requires.len() > 32 || self.requires.iter().any(|v| !v.validate()) {
            return Err("invalid or excessive execution requirements".into());
        }
        Ok(())
    }
}
pub fn valid_name(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
pub fn valid_version(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 192
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-+".contains(&b))
        && v.bytes().any(|b| b.is_ascii_digit())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolObservation {
    Available { version: String, identity: Digest },
    Unavailable,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_free_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// TTC admission budget; None means not observed.
    pub resource_budget: Option<u32>,
    /// Logical CPU cores of the worker's host; None when not observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    /// Physical memory of the worker's host in bytes; None when not observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    pub schema_version: u32,
    pub os: String,
    pub arch: String,
    pub profile: String,
    pub profile_digest: Digest,
    pub execution_uid: u32,
    pub protocol_versions: Vec<u32>,
    pub profile_fingerprint: Digest,
    pub tools: BTreeMap<String, ToolObservation>,
    pub features: BTreeSet<String>,
    /// Rust toolchains the user's own rustup already has for this host,
    /// found read-only (spec 22.1). None when there are none, so a report
    /// without them serializes as before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rustup: Vec<DetectedRustup>,
    /// Versions of other tools the user's version managers already have
    /// (mise, nodenv, nvm, Homebrew), found read-only (spec 22.1).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub installs: Vec<DetectedInstall>,
}
/// Most installed tool versions a worker probes, over every manager
/// (spec 22.1).
pub const MAX_DETECTED_INSTALLS: usize = 32;
/// The tools detection looks for, with the executable that reports the
/// version and its argument.
pub const DETECTED_TOOLS: &[(&str, &str, &str)] = &[
    ("node", "node", "-v"),
    ("python", "python3", "--version"),
    ("go", "go", "version"),
    ("ruby", "ruby", "-v"),
];
/// The version managers detection reads.
pub const DETECTED_SOURCES: &[&str] = &["mise", "nodenv", "nvm", "homebrew"];
/// One installed version of a tool, as its own executable reports it.
/// `bin` is the directory that holds the executable on this worker.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DetectedInstall {
    pub tool: String,
    pub version: String,
    pub source: String,
    pub bin: std::path::PathBuf,
    pub identity: Digest,
}
/// Most installed rustup toolchains a worker probes (spec 22.1).
pub const MAX_DETECTED_RUSTUP: usize = 16;
/// One toolchain of the user's rustup, as its own `rustc -V` and `cargo -V`
/// report it. `bin` is the toolchain's `bin` directory on this worker.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DetectedRustup {
    pub name: String,
    pub version: String,
    pub bin: std::path::PathBuf,
    pub rustc: Digest,
    pub cargo: Digest,
}
impl WorkerCapabilities {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && self.protocol_versions == [1]
            && os(&self.os).is_some()
            && arch(&self.arch) == Some(self.arch.as_str())
            && valid_name(&self.profile)
            && self.tools.len() <= TOOLS.len()
            && self.tools.iter().all(|(k, v)| {
                TOOLS.contains(&k.as_str())
                    && match v {
                        ToolObservation::Available { version, .. } => valid_version(version),
                        _ => true,
                    }
            })
            && self.features.len() <= FEATURES.len()
            && self.features.iter().all(|v| FEATURES.contains(&v.as_str()))
            && self.rustup.len() <= MAX_DETECTED_RUSTUP
            && self.rustup.iter().all(|r| {
                valid_version(&r.version)
                    && !r.name.is_empty()
                    && r.name.len() <= 128
                    && r.name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                    && r.bin.is_absolute()
            })
            && self.installs.len() <= MAX_DETECTED_INSTALLS
            && self.installs.iter().all(|i| {
                DETECTED_TOOLS.iter().any(|(t, _, _)| *t == i.tool)
                    && DETECTED_SOURCES.contains(&i.source.as_str())
                    && valid_version(&i.version)
                    && i.bin.is_absolute()
            })
    }
}
/// The line `missing` writes for an unmet requirement.
pub fn unmet_line(requirement: &Requirement) -> String {
    format!("{requirement:?} unavailable")
}
/// Missing reports and unknown probes never satisfy a constrained task.
pub fn missing(req: &ExecutionRequirements, report: Option<&WorkerCapabilities>) -> Vec<String> {
    if let Err(e) = req.validate() {
        return vec![e];
    }
    if req.is_empty() {
        return Vec::new();
    }
    let Some(r) = report.filter(|r| r.validate() && r.features.contains(FEATURE)) else {
        return vec!["worker_capabilities_v1 report unavailable".into()];
    };
    let mut missing = Vec::new();
    if let Some(p) = &req.profile {
        if p != &r.profile {
            missing.push(format!("profile {p} unavailable"));
        }
    }
    for v in &req.requires {
        let available = match v {
            Requirement::Os(v) => v == &r.os,
            Requirement::Arch(v) => v == &r.arch,
            Requirement::Tool(v) => {
                matches!(r.tools.get(v), Some(ToolObservation::Available { .. }))
            }
            Requirement::Feature(v) => r.features.contains(v),
        };
        if !available {
            missing.push(unmet_line(v));
        }
    }
    missing
}

/// Eligibility is distinct from a native observation. Call only after validating
/// the complete bundle against this worker's trusted registry.
pub fn missing_with_installable(
    req: &ExecutionRequirements,
    report: Option<&WorkerCapabilities>,
    bundle: Option<&crate::toolchain::contract::FrozenBundle>,
) -> Vec<String> {
    let mut native = req.clone();
    if let Some(bundle) = bundle {
        native.requires.retain(|r| match r {
            Requirement::Tool(name) => !bundle
                .frozen
                .selections
                .iter()
                .any(|s| s.tool.as_str() == crate::toolchain::contract::canonical_tool(name)),
            _ => true,
        });
    }
    missing(&native, report)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn report() -> WorkerCapabilities {
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
            tools: BTreeMap::new(),
            features: [FEATURE.into()].into(),
            rustup: Vec::new(),
            installs: Vec::new(),
        }
    }
    /// A report's detected rustup toolchains are bounded and well formed
    /// (spec 22.1); each rule refuses alone.
    #[test]
    fn detected_rustup_toolchains_are_validated() {
        let one = |name: &str, version: &str, bin: &str| DetectedRustup {
            name: name.into(),
            version: version.into(),
            bin: bin.into(),
            rustc: Digest::of_bytes(b"rustc"),
            cargo: Digest::of_bytes(b"cargo"),
        };
        let with = |rustup: Vec<DetectedRustup>| WorkerCapabilities { rustup, ..report() };
        let good = one("1.89.0-aarch64-apple-darwin", "1.89.0", "/r/bin");
        assert!(with(vec![good.clone()]).validate());
        assert!(with(vec![good.clone(); MAX_DETECTED_RUSTUP]).validate());
        assert!(!with(vec![good.clone(); MAX_DETECTED_RUSTUP + 1]).validate());
        assert!(!with(vec![one("1.89.0", "not a version", "/r/bin")]).validate());
        assert!(!with(vec![one("", "1.89.0", "/r/bin")]).validate());
        assert!(with(vec![one(&"a".repeat(128), "1.89.0", "/r/bin")]).validate());
        assert!(!with(vec![one(&"a".repeat(129), "1.89.0", "/r/bin")]).validate());
        assert!(!with(vec![one("1.89.0/x", "1.89.0", "/r/bin")]).validate());
        assert!(!with(vec![one("1.89.0", "1.89.0", "r/bin")]).validate());
        // A bad entry after a good one still refuses the report.
        assert!(!with(vec![good, one("", "1.89.0", "/r/bin")]).validate());
    }
    #[test]
    fn eligible_selected_tool_is_distinct_from_its_native_unavailable_observation() {
        use crate::toolchain::{contract::FrozenBundle, versions::*};
        let report = report();
        let before = report.clone();
        let required = ExecutionRequirements::parse(None, &["node".into()]).unwrap();
        assert!(!missing(&required, Some(&report)).is_empty());
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(b"source"),
            Digest::of_bytes(b"capture"),
            scope.clone(),
            &[Declaration {
                tool: ToolId::parse("node").unwrap(),
                constraint: Constraint::parse("20.0.0").unwrap(),
                scope,
                location: Location {
                    path: ".node-version".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let selected = FrozenBundle::seal_exact(frozen).unwrap();
        selected.validate(&selected.frozen.source_manifest).unwrap();
        assert!(missing_with_installable(&required, Some(&report), Some(&selected)).is_empty());
        assert_eq!(
            missing_with_installable(&required, Some(&report), None),
            vec!["Tool(\"node\") unavailable"],
            "without a verified installable bundle, unavailable native node remains required",
        );
        let constrained = ExecutionRequirements::parse(
            Some("other".into()),
            &[
                "node".into(),
                "os:linux".into(),
                "arch:x86_64".into(),
                "feature:blob_resume".into(),
            ],
        )
        .unwrap();
        assert_eq!(
            missing_with_installable(&constrained, Some(&report), Some(&selected)),
            vec![
                "profile other unavailable",
                "Os(\"linux\") unavailable",
                "Arch(\"x86_64\") unavailable",
                "Feature(\"blob_resume\") unavailable",
            ],
            "installable node never waives profile, OS, architecture or feature constraints",
        );
        assert_eq!(report, before);
        assert!(!missing(&required, Some(&report)).is_empty());
    }

    #[test]
    fn grammar_is_strict_and_aliases_are_canonical() {
        for v in [
            "macos",
            "linux",
            "windows",
            "os:macos",
            "arm64",
            "aarch64",
            "x86_64",
            "amd64",
            "arch:amd64",
            "node",
            "tool:node",
            "feature:worker_capabilities_v1",
        ] {
            assert!(Requirement::parse(v).unwrap().validate());
        }
        for v in [
            "darwin",
            "os:darwin",
            "arch:i386",
            "label:node",
            "tool:nope",
            "feature:magic",
            "",
            "node:22",
        ] {
            assert!(Requirement::parse(v).is_err(), "{v}");
        }
        assert_eq!(Requirement::parse("arm64"), Requirement::parse("aarch64"));
        let req = ExecutionRequirements::parse(
            Some("node22".into()),
            &["arm64".into(), "arch:aarch64".into()],
        )
        .unwrap();
        assert_eq!(req.requires.len(), 1);
        assert!(ExecutionRequirements::parse(Some("../bad".into()), &[]).is_err());
        assert!(ExecutionRequirements::parse(None, &["unknown-tool".into()]).is_err());
        assert!(!valid_name(""));
        assert!(!valid_name(&"x".repeat(65)));
        assert!(valid_name(&"x".repeat(64)));
        assert!(valid_version(&"2".repeat(192)));
        assert!(valid_name("a_B-9"));
        for v in [
            Requirement::Os("alien".into()),
            Requirement::Arch("arm64".into()),
            Requirement::Tool("magic".into()),
            Requirement::Feature("magic".into()),
        ] {
            assert!(!v.validate());
            assert!(ExecutionRequirements {
                profile: None,
                requires: vec![v]
            }
            .validate()
            .is_err());
        }
        assert!(ExecutionRequirements {
            profile: None,
            requires: vec![Requirement::Tool("node".into()); 33]
        }
        .validate()
        .is_err());
        assert!(ExecutionRequirements {
            profile: None,
            requires: vec![Requirement::Tool("node".into()); 32]
        }
        .validate()
        .is_ok());
        assert!(!ExecutionRequirements {
            profile: Some("default".into()),
            requires: vec![]
        }
        .is_empty());
        assert!(ExecutionRequirements::default().is_empty());
        assert!(!req.is_empty());
        assert!(!ExecutionRequirements {
            profile: None,
            requires: vec![Requirement::Tool("node".into())]
        }
        .is_empty());
    }
    #[test]
    fn compatibility_never_infers_a_tool_from_a_label_or_unknown() {
        let mut r = report();
        assert!(r.validate());
        let mut full = report();
        full.tools = TOOLS
            .iter()
            .map(|v| (v.to_string(), ToolObservation::Unknown))
            .collect();
        full.features = FEATURES.iter().map(|v| v.to_string()).collect();
        assert!(full.validate());

        assert!(missing(&ExecutionRequirements::default(), None).is_empty());
        let req = ExecutionRequirements::parse(
            Some("node22".into()),
            &[
                "node".into(),
                "linux".into(),
                "amd64".into(),
                format!("feature:{FEATURE}"),
            ],
        )
        .unwrap();
        assert_eq!(
            missing(&req, None),
            vec!["worker_capabilities_v1 report unavailable"]
        );
        assert_eq!(missing(&req, Some(&r)).len(), 4);
        r.profile = "node22".into();
        r.os = "linux".into();
        r.arch = "x86_64".into();
        for status in [ToolObservation::Unknown, ToolObservation::Unavailable] {
            r.tools.insert("node".into(), status);
            assert_eq!(missing(&req, Some(&r)).len(), 1);
        }
        r.tools.insert(
            "node".into(),
            ToolObservation::Available {
                version: "v22.1.0".into(),
                identity: Digest::of_bytes(b"22"),
            },
        );
        assert!(missing(&req, Some(&r)).is_empty());
        r.features.clear();
        assert_eq!(
            missing(&req, Some(&r)),
            vec!["worker_capabilities_v1 report unavailable"]
        );
        let mut malformed = report();
        malformed.schema_version = 2;
        let only_profile = ExecutionRequirements::parse(Some("default".into()), &[]).unwrap();
        assert_eq!(
            missing(&only_profile, Some(&malformed)),
            vec!["worker_capabilities_v1 report unavailable"]
        );
        assert!(!missing(
            &ExecutionRequirements {
                profile: Some("?".into()),
                requires: vec![]
            },
            None
        )
        .is_empty());
        let feature_req =
            ExecutionRequirements::parse(None, &["feature:blob_resume".into()]).unwrap();
        assert_eq!(missing(&feature_req, Some(&report())).len(), 1);
        let no_profile = ExecutionRequirements::parse(None, &["macos".into()]).unwrap();
        assert!(missing(&no_profile, Some(&report())).is_empty());
    }
    #[test]
    fn malformed_public_reports_fail_closed_and_versions_are_bounded() {
        let mut r = report();
        r.features = (0..=FEATURES.len())
            .map(|i| format!("invalid_{i}"))
            .collect();
        assert!(!r.validate());
        r = report();
        r.features.insert("magic".into());
        assert!(!r.validate());
        r = report();
        r.features = ["magic".into()].into();
        assert!(!r.validate());
        r = report();
        r.protocol_versions = vec![2];
        assert!(!r.validate());
        r = report();
        r.schema_version = 2;
        assert!(!r.validate());
        r = report();
        r.os = "alien".into();
        assert!(!r.validate());
        r = report();
        r.arch = "arm64".into();
        assert!(!r.validate());
        r = report();
        r.profile.clear();
        assert!(!r.validate());
        r = report();
        r.tools.insert("magic".into(), ToolObservation::Unknown);
        assert!(!r.validate());
        r = report();
        r.tools.insert(
            "node".into(),
            ToolObservation::Available {
                version: "bad\n22".into(),
                identity: Digest::of_bytes(b"22"),
            },
        );
        assert!(!r.validate());
        for v in [
            "",
            "garbage",
            "\u{1b}22",
            "22.0.0-/private/path",
            "22.0.0 with space",
            &"2".repeat(193),
        ] {
            assert!(!valid_version(v));
        }
        assert!(valid_version("22.0.0-prerelease+build22"));
        r = report();
        r.tools = (0..14)
            .map(|n| (n.to_string(), ToolObservation::Unknown))
            .collect();
        assert!(!r.validate());
    }
    #[test]
    fn durable_roundtrip_defaults_and_normalized_serialization() {
        let req =
            ExecutionRequirements::parse(Some("test".into()), &["arm64".into(), "node".into()])
                .unwrap();
        assert_eq!(
            req,
            serde_json::from_str(&serde_json::to_string(&req).unwrap()).unwrap()
        );
        assert_eq!(
            ExecutionRequirements::default(),
            serde_json::from_str("{}").unwrap()
        );
        assert!(serde_json::from_str::<ExecutionRequirements>("{\"labels\":[]}").is_err());
    }
}
