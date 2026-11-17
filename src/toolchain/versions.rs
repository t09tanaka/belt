//! Typed, side-effect-free version contracts. Manager aliases need resolution;
//! absence and unparseable observations are never implicit wildcards.
use crate::ids::Digest;
use node_semver::{Range, Version};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolId(String);
impl ToolId {
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        let value = match value {
            "nodejs" => "node",
            "rustc" | "cargo" => "rust",
            "golang" => "go",
            other => other,
        };
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c))
        {
            return Err("invalid_tool");
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub path: String,
    pub first_line: usize,
    pub last_line: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub locations: Vec<Location>,
}
impl Diagnostic {
    pub(crate) fn at(code: &str, location: Location) -> Self {
        Self {
            code: code.into(),
            locations: vec![location],
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Constraint {
    /// npm-compatible syntax, not Rust's different caret-default VersionReq.
    Range(Range),
    Pub(PubRange),
    /// A channel or alias is not a concrete version, even when it looks familiar.
    Resolve(String),
    Expression(String),
    VersionFileExpression(String),
    Distribution {
        version: Version,
        integrity: String,
    },
}
impl Constraint {
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        let value = value.trim();
        if value.len() > 512 || value.contains(['\n', '\r', '\0']) {
            return Err("invalid_version");
        }
        if value.contains("${{") {
            if !value.contains("}}") {
                return Err("invalid_expression");
            }
            return Ok(Self::Expression(value.into()));
        }
        if let Some((version, integrity)) = value.split_once("+sha") {
            let (algorithm, hash) = integrity.split_once('.').ok_or("invalid_integrity")?;
            let length = match algorithm {
                "224" => 56,
                "256" => 64,
                "384" => 96,
                "512" => 128,
                _ => return Err("invalid_integrity"),
            };
            if hash.len() != length || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("invalid_integrity");
            }
            return Ok(Self::Distribution {
                version: Version::parse(version).map_err(|_| "invalid_version")?,
                integrity: format!("sha{integrity}"),
            });
        }
        if let Ok(range) = Range::parse(value) {
            return Ok(Self::Range(range));
        }
        if value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-/*._".contains(&c))
            && (matches!(
                value,
                "stable" | "beta" | "nightly" | "latest" | "current" | "node" | "system"
            ) || value.starts_with("lts/")
                || value.starts_with("nightly-")
                || value.starts_with("beta-"))
        {
            return Ok(Self::Resolve(value.into()));
        }
        Err("invalid_version")
    }
    pub fn matches(&self, observed: &VersionObservation) -> Match {
        match (self, observed) {
            (Self::Pub(range), VersionObservation::Known(version)) => {
                if range.matches(version) {
                    Match::Match
                } else {
                    Match::Mismatch
                }
            }
            (Self::Range(range), VersionObservation::Known(version)) => {
                if range.satisfies(version) {
                    Match::Match
                } else {
                    Match::Mismatch
                }
            }
            _ => Match::Unknown,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "version", rename_all = "snake_case")]
pub enum VersionObservation {
    Known(Version),
    Unknown,
    Identity(String),
}
impl VersionObservation {
    /// Public probe tokens only; command output is parsed by the probe adapter.
    pub fn parse_for(tool: &ToolId, value: &str) -> Self {
        if tool.as_str() == "xcode" && value.split('.').count() == 2 {
            return Self::parse(&format!("{value}.0"));
        }
        Self::parse(value)
    }
    pub fn identity(value: &str) -> Self {
        if !value.is_empty()
            && value.len() <= 192
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-+_".contains(&b))
            && value.bytes().any(|b| b.is_ascii_digit())
        {
            Self::Identity(value.into())
        } else {
            Self::Unknown
        }
    }
    pub fn parse(value: &str) -> Self {
        Version::parse(value)
            .map(Self::Known)
            .unwrap_or(Self::Unknown)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Match {
    Match,
    Mismatch,
    Unknown,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchPolicy {
    #[default]
    Exact,
    Minor,
    Major,
    Any,
}
impl MatchPolicy {
    pub fn compare(self, baseline: &VersionObservation, observed: &VersionObservation) -> Match {
        if matches!(baseline, VersionObservation::Unknown)
            || matches!(observed, VersionObservation::Unknown)
        {
            return Match::Unknown;
        }
        if let (VersionObservation::Identity(a), VersionObservation::Identity(b)) =
            (baseline, observed)
        {
            return if self == Self::Any {
                Match::Match
            } else if self != Self::Exact {
                Match::Unknown
            } else if a == b {
                Match::Match
            } else {
                Match::Mismatch
            };
        }
        let (VersionObservation::Known(a), VersionObservation::Known(b)) = (baseline, observed)
        else {
            return if self == Self::Any {
                Match::Match
            } else {
                Match::Unknown
            };
        };
        let same = match self {
            Self::Any => true,
            Self::Exact => a == b && a.build == b.build,
            Self::Minor => {
                a.major == b.major && a.minor == b.minor && a.pre_release == b.pre_release
            }
            Self::Major => a.major == b.major && a.pre_release == b.pre_release,
        };
        if same {
            Match::Match
        } else {
            Match::Mismatch
        }
    }
}
/// Scope is explicit: unrelated packages/jobs and sequential setup steps are
/// never flattened into an impossible process-wide version requirement.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Scope {
    pub project: String,
    pub workflow: Option<String>,
    pub job: Option<String>,
    pub step: Option<usize>,
}
impl Scope {
    pub fn project(path: &str) -> Self {
        Self {
            project: path.into(),
            workflow: None,
            job: None,
            step: None,
        }
    }
    fn applies(&self, target: &Self) -> bool {
        self.project == target.project
            && self
                .workflow
                .as_ref()
                .is_none_or(|v| target.workflow.as_ref() == Some(v))
            && self
                .job
                .as_ref()
                .is_none_or(|v| target.job.as_ref() == Some(v))
            && self.step.is_none_or(|v| target.step == Some(v))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Declaration {
    pub tool: ToolId,
    pub constraint: Constraint,
    pub scope: Scope,
    pub location: Location,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveRequirement {
    pub tool: ToolId,
    pub constraints: Vec<Constraint>,
    pub provenance: Vec<Location>,
}
/// The scopes a worker has to serve for `declarations`: each project as a
/// whole, and every narrower scope (workflow, job, step) a declaration names.
/// A requirement of a narrow scope is taken together with the project's own.
pub fn requirement_scopes(declarations: &[Declaration]) -> Vec<Scope> {
    let mut scopes = std::collections::BTreeSet::new();
    for d in declarations {
        scopes.insert(Scope::project(&d.scope.project));
        scopes.insert(d.scope.clone());
    }
    scopes.into_iter().collect()
}
/// Reject contradictory constraints in the selected scope. No file wins by
/// accidental read order. Ranges intersect; unresolved aliases stay unresolved.
pub fn effective(
    declarations: &[Declaration],
    scope: &Scope,
) -> Result<Vec<EffectiveRequirement>, Diagnostic> {
    if declarations.len() > 256 {
        return Err(Diagnostic {
            code: "too_many_declarations".into(),
            locations: vec![],
        });
    }
    let mut map: BTreeMap<ToolId, EffectiveRequirement> = BTreeMap::new();
    for d in declarations.iter().filter(|d| d.scope.applies(scope)) {
        let r = map
            .entry(d.tool.clone())
            .or_insert_with(|| EffectiveRequirement {
                tool: d.tool.clone(),
                constraints: vec![],
                provenance: vec![],
            });
        r.constraints.push(d.constraint.clone());
        r.provenance.push(d.location.clone());
    }
    for r in map.values() {
        let mut intersection = Range::any();
        let mut alias: Option<&str> = None;
        let mut distribution: Option<(&Version, &str)> = None;
        for c in &r.constraints {
            match c {
                Constraint::Range(range) => {
                    let Some(next) = intersection.intersect(range) else {
                        return Err(Diagnostic {
                            code: "conflicting_versions".into(),
                            locations: r.provenance.clone(),
                        });
                    };
                    intersection = next;
                }
                Constraint::Resolve(value) => {
                    if alias.is_some_and(|a| a != value) {
                        return Err(Diagnostic {
                            code: "conflicting_versions".into(),
                            locations: r.provenance.clone(),
                        });
                    }
                    alias = Some(value);
                }
                Constraint::Pub(range) => {
                    let range = range.as_range();
                    let Some(next) = intersection.intersect(&range) else {
                        return Err(Diagnostic {
                            code: "conflicting_versions".into(),
                            locations: r.provenance.clone(),
                        });
                    };
                    intersection = next;
                }
                Constraint::Distribution { version, integrity } => {
                    if distribution.is_some_and(|(v, i)| v != version || i != integrity) {
                        return Err(Diagnostic {
                            code: "conflicting_versions".into(),
                            locations: r.provenance.clone(),
                        });
                    }
                    distribution = Some((version, integrity));
                    let exact =
                        Range::parse(format!("={version}")).expect("concrete version is a range");
                    let Some(next) = intersection.intersect(&exact) else {
                        return Err(Diagnostic {
                            code: "conflicting_versions".into(),
                            locations: r.provenance.clone(),
                        });
                    };
                    intersection = next;
                }
                Constraint::Expression(_) | Constraint::VersionFileExpression(_) => {}
            }
        }
    }
    Ok(map.into_values().collect())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Baseline {
    pub tool: ToolId,
    pub version: VersionObservation,
    pub provenance: Location,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSelection {
    pub tool: ToolId,
    pub concrete: Version,
    pub requirement_digest: Digest,
    /// Digest of the trusted adapter/parent resolution evidence, not a script.
    pub authority: Digest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenToolchains {
    pub schema_version: u32,
    pub source_manifest: Digest,
    pub profile_definition: Digest,
    pub scope: Scope,
    pub requirements: Vec<EffectiveRequirement>,
    pub baseline: Vec<Baseline>,
    pub selections: Vec<ResolvedSelection>,
    pub policy: MatchPolicy,
}
impl FrozenToolchains {
    /// Caller supplies already captured observations. No env/files/probes are
    /// consulted here; retry serializes/reuses this exact contract.
    #[allow(clippy::too_many_arguments)]
    pub fn freeze(
        source_manifest: Digest,
        profile_definition: Digest,
        scope: Scope,
        declarations: &[Declaration],
        required: &[(ToolId, Location)],
        observed: &BTreeMap<ToolId, VersionObservation>,
        policy: MatchPolicy,
    ) -> Result<Self, Diagnostic> {
        let requirements = effective(declarations, &scope)?;
        let mut baseline = BTreeMap::new();
        for (tool, provenance) in required {
            if !requirements.iter().any(|r| &r.tool == tool) {
                baseline.entry(tool.clone()).or_insert_with(|| Baseline {
                    tool: tool.clone(),
                    version: observed
                        .get(tool)
                        .cloned()
                        .unwrap_or(VersionObservation::Unknown),
                    provenance: provenance.clone(),
                });
            }
        }
        Ok(Self {
            schema_version: 1,
            source_manifest,
            profile_definition,
            scope,
            requirements,
            baseline: baseline.into_values().collect(),
            selections: vec![],
            policy,
        })
    }
    /// Freeze an explicit trusted resolution without modifying the original
    /// contract. Alias/expression resolution is supplied by the next adapter
    /// layer; it must bind to this exact requirement/source/scope digest.
    pub fn with_selection(
        &self,
        tool: &ToolId,
        concrete: Version,
        authority: Digest,
    ) -> Result<Self, Diagnostic> {
        let r = self
            .requirements
            .iter()
            .find(|r| &r.tool == tool)
            .ok_or_else(|| Diagnostic {
                code: "unknown_selection_tool".into(),
                locations: vec![],
            })?;
        if r.constraints.iter().any(|c| match c {
            Constraint::Range(range) => !range.satisfies(&concrete),
            Constraint::Pub(range) => !range.matches(&concrete),
            Constraint::Distribution { version, .. } => version != &concrete,
            _ => false,
        }) {
            return Err(Diagnostic {
                code: "selection_mismatch".into(),
                locations: r.provenance.clone(),
            });
        }
        let mut frozen = self.clone();
        frozen.selections.retain(|s| &s.tool != tool);
        let requirement_digest = Digest::of_bytes(
            &serde_json::to_vec(&(
                &self.source_manifest,
                &self.profile_definition,
                &self.scope,
                r,
            ))
            .expect("selection identity"),
        );
        frozen.selections.push(ResolvedSelection {
            tool: tool.clone(),
            concrete,
            requirement_digest,
            authority,
        });
        frozen.selections.sort_by(|a, b| a.tool.cmp(&b.tool));
        Ok(frozen)
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || self.requirements.len() > 256
            || self.baseline.len() > 256
            || self.selections.len() > 256
        {
            return Err("invalid_frozen_toolchains");
        }
        for r in &self.requirements {
            if r.constraints.is_empty()
                || self
                    .requirements
                    .iter()
                    .filter(|other| other.tool == r.tool)
                    .count()
                    != 1
            {
                return Err("invalid_requirement");
            }
        }
        for b in &self.baseline {
            if self.requirements.iter().any(|r| r.tool == b.tool)
                || self
                    .baseline
                    .iter()
                    .filter(|other| other.tool == b.tool)
                    .count()
                    != 1
            {
                return Err("invalid_baseline");
            }
        }
        for selection in &self.selections {
            let r = self
                .requirements
                .iter()
                .find(|r| r.tool == selection.tool)
                .ok_or("invalid_selection")?;
            let digest = Digest::of_bytes(
                &serde_json::to_vec(&(
                    &self.source_manifest,
                    &self.profile_definition,
                    &self.scope,
                    r,
                ))
                .expect("selection identity"),
            );
            if digest != selection.requirement_digest
                || self
                    .selections
                    .iter()
                    .filter(|s| s.tool == selection.tool)
                    .count()
                    != 1
            {
                return Err("invalid_selection");
            }
            if r.constraints.iter().any(|c| match c {
                Constraint::Range(range) => !range.satisfies(&selection.concrete),
                Constraint::Pub(range) => !range.matches(&selection.concrete),
                Constraint::Distribution { version, .. } => version != &selection.concrete,
                _ => false,
            }) {
                return Err("invalid_selection");
            }
        }
        Ok(())
    }
    pub fn is_resolved(&self) -> bool {
        self.validate().is_ok()
            && self
                .requirements
                .iter()
                .all(|r| self.selections.iter().any(|s| s.tool == r.tool))
            && self
                .baseline
                .iter()
                .all(|b| !matches!(b.version, VersionObservation::Unknown))
    }
    pub fn check(
        &self,
        observed: &BTreeMap<ToolId, VersionObservation>,
    ) -> BTreeMap<ToolId, Match> {
        let missing = VersionObservation::Unknown;
        if self.validate().is_err() {
            return self
                .requirements
                .iter()
                .map(|r| (r.tool.clone(), Match::Unknown))
                .chain(
                    self.baseline
                        .iter()
                        .map(|b| (b.tool.clone(), Match::Unknown)),
                )
                .collect();
        }
        let mut result = BTreeMap::new();
        for r in &self.requirements {
            let selection = self.selections.iter().find(|s| s.tool == r.tool);
            let checks: Vec<_> = r
                .constraints
                .iter()
                .map(|c| match (c, selection) {
                    (Constraint::Range(_) | Constraint::Pub(_), _) | (_, None) => {
                        c.matches(observed.get(&r.tool).unwrap_or(&missing))
                    }
                    (_, Some(_)) => Match::Match,
                })
                .collect();
            let status = if checks.contains(&Match::Mismatch) {
                Match::Mismatch
            } else if checks.contains(&Match::Unknown) {
                Match::Unknown
            } else {
                Match::Match
            };
            let selected = selection
                .map(|s| {
                    MatchPolicy::Exact.compare(
                        &VersionObservation::Known(s.concrete.clone()),
                        observed.get(&r.tool).unwrap_or(&missing),
                    )
                })
                .unwrap_or(Match::Unknown);
            let status = if status == Match::Mismatch || selected == Match::Mismatch {
                Match::Mismatch
            } else if status == Match::Unknown || selected == Match::Unknown {
                Match::Unknown
            } else {
                Match::Match
            };
            result.insert(r.tool.clone(), status);
        }
        for b in &self.baseline {
            result.insert(
                b.tool.clone(),
                self.policy
                    .compare(&b.version, observed.get(&b.tool).unwrap_or(&missing)),
            );
        }
        result
    }
    pub fn digest(&self) -> Digest {
        Digest::of_bytes(&serde_json::to_vec(self).expect("frozen toolchain serialization"))
    }
}

/// Pub syntax is not npm syntax: notably ^0.0.3 allows <0.1.0 and
/// exclusive stable upper bounds normally exclude their prereleases.
/// Rules: dart.dev/tools/pub/dependencies#version-constraints and the official
/// pub_semver VersionRange constructor / Version.nextBreaking API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PubRange {
    bounds: Vec<(PubOp, Version)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PubOp {
    Eq,
    Ge,
    Gt,
    Le,
    Lt,
}
impl PubRange {
    pub fn parse(raw: &str) -> Result<Self, &'static str> {
        let raw = raw.trim();
        if raw.is_empty() || raw.len() > 512 {
            return Err("invalid_pub_range");
        }
        if raw == "any" {
            return Ok(Self { bounds: vec![] });
        }
        if let Some(version) = raw.strip_prefix('^') {
            let min = Self::version(version)?;
            let max = if min.major == 0 {
                Version::new(0, min.minor + 1, 0)
            } else {
                Version::new(min.major + 1, 0, 0)
            };
            return Self::parse(&format!(">={min} <{max}"));
        }
        let mut words = raw.split_whitespace();
        let mut bounds = vec![];
        while let Some(word) = words.next() {
            let (op, version) = if let Some(v) = word.strip_prefix(">=") {
                (PubOp::Ge, v)
            } else if let Some(v) = word.strip_prefix("<=") {
                (PubOp::Le, v)
            } else if let Some(v) = word.strip_prefix('>') {
                (PubOp::Gt, v)
            } else if let Some(v) = word.strip_prefix('<') {
                (PubOp::Lt, v)
            } else {
                (PubOp::Eq, word)
            };
            let version = if version.is_empty() {
                words.next().ok_or("invalid_pub_range")?
            } else {
                version
            };
            bounds.push((op, Self::version(version)?));
        }
        let min = bounds
            .iter()
            .filter(|(op, _)| matches!(op, PubOp::Ge | PubOp::Gt | PubOp::Eq))
            .map(|(_, v)| v)
            .max()
            .cloned();
        for (op, version) in &mut bounds {
            if *op == PubOp::Lt
                && !version.is_prerelease()
                && !min.as_ref().is_some_and(|min| {
                    min.is_prerelease()
                        && min.major == version.major
                        && min.minor == version.minor
                        && min.patch == version.patch
                })
            {
                *version = Version::parse(format!("{version}-0"))
                    .expect("validated version with first prerelease");
            }
        }
        Ok(Self { bounds })
    }
    fn version(raw: &str) -> Result<Version, &'static str> {
        let version = Version::parse(raw).map_err(|_| "invalid_pub_range")?;
        if !version.build.is_empty() {
            return Err("pub_build_requires_resolution");
        }
        Ok(version)
    }
    pub fn matches(&self, version: &Version) -> bool {
        self.bounds.iter().all(|(op, bound)| {
            // Pub compares build components after ordinary semver precedence;
            // missing components sort first, numbers before strings. The parsed
            // Identifier ordering has those exact list/component rules.
            let ordering = version
                .cmp(bound)
                .then_with(|| version.build.cmp(&bound.build));
            match op {
                PubOp::Eq => ordering.is_eq(),
                PubOp::Ge => !ordering.is_lt(),
                PubOp::Gt => ordering.is_gt(),
                PubOp::Le => !ordering.is_gt(),
                PubOp::Lt => ordering.is_lt(),
            }
        })
    }
    fn as_range(&self) -> Range {
        if self.bounds.is_empty() {
            return Range::any();
        }
        let value = self
            .bounds
            .iter()
            .map(|(op, v)| {
                format!(
                    "{}{v}",
                    match op {
                        PubOp::Eq => "=",
                        PubOp::Ge => ">=",
                        PubOp::Gt => ">",
                        PubOp::Le => "<=",
                        PubOp::Lt => "<",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        Range::parse(value).expect("typed Pub bounds are valid semver comparators")
    }
}
