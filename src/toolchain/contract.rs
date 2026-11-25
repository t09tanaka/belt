//! Source-bound, complete frozen resolution carried unchanged across retries.
use super::{
    adapters::{self, ResolutionEvidence},
    versions::{Constraint, FrozenToolchains},
};
use crate::ids::Digest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
pub const FEATURE: &str = "frozen_toolchains_v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenBundle {
    pub schema_version: u32,
    pub frozen: FrozenToolchains,
    pub evidence: Vec<ResolutionEvidence>,
}
impl FrozenBundle {
    pub fn validate(&self, source: &Digest) -> Result<(), &'static str> {
        self.validate_partial(source)?;
        if !self.frozen.is_resolved() || self.evidence.len() != self.frozen.requirements.len() {
            return Err("toolchain_resolve_required");
        }
        Ok(())
    }
    pub fn validate_partial(&self, source: &Digest) -> Result<(), &'static str> {
        if self.schema_version != 1
            || &self.frozen.source_manifest != source
            || self.evidence.len() > 256
        {
            return Err("toolchain_bundle_binding");
        }
        self.frozen.validate()?;
        let mut base = self.frozen.clone();
        base.selections.clear();
        let mut seen = BTreeSet::new();
        for evidence in &self.evidence {
            if !seen.insert(evidence.tool.clone()) {
                return Err("duplicate_resolution_evidence");
            }
            base = adapters::resolve(&base, evidence)?;
        }
        if base != self.frozen {
            return Err("incomplete_resolution_evidence");
        }
        Ok(())
    }
    pub fn digest(&self) -> Digest {
        Digest::of_bytes(&serde_json::to_vec(self).expect("bundle serializes"))
    }
    /// Only a typed range proven equal to one exact version is an offline pin.
    pub fn seal_exact(mut frozen: FrozenToolchains) -> Result<Self, &'static str> {
        let mut evidence = Vec::new();
        for requirement in frozen.requirements.clone() {
            let concrete = requirement.constraints.iter().find_map(|c| match c {
                Constraint::Distribution { version, .. } => Some(version.clone()),
                Constraint::Range(range) => range.min_version().filter(|version| {
                    node_semver::Range::parse(version.to_string())
                        .is_ok_and(|exact| range.allows_all(&exact) && exact.allows_all(range))
                }),
                _ => None,
            });
            if let Some(concrete) = concrete {
                let proof = proof(&frozen, &requirement, concrete, "source_exact_pin_v1");
                if let Ok(resolved) = adapters::resolve(&frozen, &proof) {
                    frozen = resolved;
                    evidence.push(proof);
                }
            }
        }
        Ok(Self {
            schema_version: 1,
            frozen,
            evidence,
        })
    }
    /// Seals each requirement that still has no evidence to the version the
    /// parent observed for that tool, when every constraint of the requirement
    /// accepts that version. An alias, a channel or an expression never
    /// matches an observation, so it stays unresolved.
    pub fn seal_observed(
        mut self,
        observed: &std::collections::BTreeMap<
            super::versions::ToolId,
            super::versions::VersionObservation,
        >,
    ) -> Self {
        use super::versions::{Match, VersionObservation};
        for requirement in self.frozen.requirements.clone() {
            if self.evidence.iter().any(|e| e.tool == requirement.tool) {
                continue;
            }
            let Some(VersionObservation::Known(concrete)) = observed.get(&requirement.tool) else {
                continue;
            };
            let seen = VersionObservation::Known(concrete.clone());
            if requirement
                .constraints
                .iter()
                .any(|c| c.matches(&seen) != Match::Match)
            {
                continue;
            }
            let proof = proof(
                &self.frozen,
                &requirement,
                concrete.clone(),
                "parent_observed_version_v1",
            );
            if let Ok(resolved) = adapters::resolve(&self.frozen, &proof) {
                self.frozen = resolved;
                self.evidence.push(proof);
            }
        }
        self
    }
}
/// Evidence for a version the source itself fixes (`domain` says how), bound
/// to the contract without its selections.
fn proof(
    frozen: &FrozenToolchains,
    requirement: &super::versions::EffectiveRequirement,
    concrete: node_semver::Version,
    domain: &str,
) -> ResolutionEvidence {
    let mut base = frozen.clone();
    base.selections.clear();
    ResolutionEvidence {
        contract: base.digest(),
        tool: requirement.tool.clone(),
        concrete,
        catalog: Digest::of_bytes(
            &serde_json::to_vec(&(domain, &base.source_manifest, &base.scope, requirement))
                .expect("proof serializes"),
        ),
        resolved_inputs: Default::default(),
    }
}

/// A frozen selection the worker serves from its own profile, with no managed
/// install: the exact version its probe reports, and the identity of that
/// probe in place of an installation receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTool {
    pub tool: super::versions::ToolId,
    pub version: node_semver::Version,
    pub identity: Digest,
    /// A toolchain of the user's rustup (spec 22.1): its `bin` goes first on
    /// the attempt's PATH and `RUSTUP_TOOLCHAIN` names it. `None` for a tool
    /// on the profile path.
    pub rustup: Option<(std::path::PathBuf, String)>,
}
/// The native tool that serves the selection of `tool`, if the profile probe
/// reports exactly the selected version.
pub fn native_tool(
    frozen: &FrozenToolchains,
    tool: &super::versions::ToolId,
    report: &super::capability::WorkerCapabilities,
) -> Option<NativeTool> {
    use super::versions::{Match, MatchPolicy, VersionObservation};
    let selection = frozen.selections.iter().find(|s| &s.tool == tool)?;
    let observed = observations(report);
    if MatchPolicy::Exact.compare(
        &VersionObservation::Known(selection.concrete.clone()),
        observed.get(tool).unwrap_or(&VersionObservation::Unknown),
    ) != Match::Match
    {
        return detected_rustup(&selection.concrete, tool, report);
    }
    let probes: Vec<_> = report
        .tools
        .iter()
        .filter(|(name, _)| canonical_tool(name) == tool.as_str())
        .collect();
    Some(NativeTool {
        tool: tool.clone(),
        version: selection.concrete.clone(),
        identity: Digest::of_bytes(
            &serde_json::to_vec(&("native_observed_v1", &selection.concrete, &probes))
                .expect("native identity serializes"),
        ),
        rustup: None,
    })
}
/// The rustup toolchain of `report` that reports exactly `version`, as the
/// native tool for `rust`. A channel's prerelease (nightly, beta) never
/// equals a release.
fn detected_rustup(
    version: &node_semver::Version,
    tool: &super::versions::ToolId,
    report: &super::capability::WorkerCapabilities,
) -> Option<NativeTool> {
    if tool.as_str() != "rust" {
        return None;
    }
    let found = report
        .rustup
        .iter()
        .find(|r| node_semver::Version::parse(&r.version).ok().as_ref() == Some(version))?;
    Some(NativeTool {
        tool: tool.clone(),
        version: version.clone(),
        identity: Digest::of_bytes(
            &serde_json::to_vec(&("rustup_detected_v1", version, found))
                .expect("detected identity serializes"),
        ),
        rustup: Some((found.bin.clone(), found.name.clone())),
    })
}
/// The highest version of the user's rustup toolchains in `report` that
/// every constraint of `requirement` accepts, as resolution evidence
/// (domain `worker_detected_rustup_v1`). Only `rust` is detected.
pub fn detected_rustup_evidence(
    frozen: &FrozenToolchains,
    requirement: &super::versions::EffectiveRequirement,
    report: &super::capability::WorkerCapabilities,
) -> Option<ResolutionEvidence> {
    use super::versions::{Match, VersionObservation};
    if requirement.tool.as_str() != "rust" {
        return None;
    }
    let best = report
        .rustup
        .iter()
        .filter_map(|r| node_semver::Version::parse(&r.version).ok())
        .filter(|v| {
            let seen = VersionObservation::Known(v.clone());
            requirement
                .constraints
                .iter()
                .all(|c| c.matches(&seen) == Match::Match)
        })
        .max()?;
    let evidence = proof(frozen, requirement, best, "worker_detected_rustup_v1");
    adapters::resolve(frozen, &evidence).ok()?;
    Some(evidence)
}

/// Trusted worker-local execution context; never accepted from the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBinding {
    pub activation: super::adapters::ActivationReceipt,
}
impl ExecutionBinding {
    pub fn environment(
        &self,
        extra: &std::collections::BTreeMap<String, String>,
    ) -> Result<std::collections::BTreeMap<String, String>, &'static str> {
        let mut env = self.activation.environment.clone();
        for (key, value) in extra {
            if protected(key) && env.get(key) != Some(value) {
                return Err("protected_activation_environment");
            }
            env.insert(key.clone(), value.clone());
        }
        Ok(env)
    }
    pub fn fingerprint(&self) -> &Digest {
        &self.activation.execution_fingerprint
    }
}
pub fn protected(key: &str) -> bool {
    matches!(key, "PATH" | "HOME" | "SHELL" | "TMPDIR" | "CI")
        || key.starts_with("XDG_")
        || key.starts_with("NODENV_")
        || key.starts_with("RUSTUP_")
        || key.starts_with("CARGO_")
        || key.starts_with("MISE_")
        || key.starts_with("FVM_")
        || key.starts_with("COREPACK_")
        || key == "GOTOOLCHAIN"
}

pub fn observations(
    report: &super::capability::WorkerCapabilities,
) -> std::collections::BTreeMap<super::versions::ToolId, super::versions::VersionObservation> {
    use super::{
        capability::ToolObservation,
        versions::{ToolId, VersionObservation},
    };
    let mut observations = std::collections::BTreeMap::new();
    for (name, observation) in &report.tools {
        let canonical = canonical_tool(name);
        let Ok(tool) = ToolId::parse(canonical) else {
            continue;
        };
        let ToolObservation::Available { version, .. } = observation else {
            continue;
        };
        let version = VersionObservation::parse_for(&tool, version);
        observations
            .entry(tool)
            .and_modify(|prior| {
                if prior != &version {
                    *prior = VersionObservation::Unknown;
                }
            })
            .or_insert(version);
    }
    observations
}
pub fn check_baselines(
    frozen: &FrozenToolchains,
    report: Option<&super::capability::WorkerCapabilities>,
) -> Result<(), &'static str> {
    let observed = report.map(observations).unwrap_or_default();
    if frozen.baseline.iter().any(|baseline| {
        frozen.policy.compare(
            &baseline.version,
            observed
                .get(&baseline.tool)
                .unwrap_or(&super::versions::VersionObservation::Unknown),
        ) != super::versions::Match::Match
    }) {
        return Err("toolchain_baseline_mismatch");
    }
    Ok(())
}
pub fn canonical_tool(name: &str) -> &str {
    match name {
        "rustc" | "cargo" => "rust",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolchain::versions::*;
    use std::collections::BTreeMap;
    fn bundle(value: &str) -> FrozenBundle {
        bundle_for("node", value)
    }
    fn bundle_for(tool: &str, value: &str) -> FrozenBundle {
        let scope = Scope::project("");
        FrozenBundle::seal_exact(
            FrozenToolchains::freeze(
                Digest::of_bytes(b"source"),
                Digest::of_bytes(b"parent"),
                scope.clone(),
                &[Declaration {
                    tool: ToolId::parse(tool).unwrap(),
                    constraint: Constraint::parse(value).unwrap(),
                    scope,
                    location: Location {
                        path: ".node-version".into(),
                        first_line: 1,
                        last_line: 1,
                    },
                }],
                &[],
                &Default::default(),
                MatchPolicy::Exact,
            )
            .unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn exact_source_pin_is_offline_complete_ranges_and_aliases_need_authority() {
        for exact in ["20.0.0", "=20.0.0", ">=20.0.0 <=20.0.0"] {
            let b = bundle(exact);
            b.validate(&b.frozen.source_manifest).unwrap();
            assert_eq!(b.evidence[0].concrete.to_string(), "20.0.0");
            assert!(b.evidence[0].resolved_inputs.is_empty());
        }
        for floating in ["20", "^20.0.0", "lts/*", "${{ matrix.node }}"] {
            let b = bundle(floating);
            assert_eq!(
                b.validate(&b.frozen.source_manifest),
                Err("toolchain_resolve_required")
            );
        }
        let distribution = bundle(&format!("20.0.0+sha256.{}", "a".repeat(64)));
        distribution
            .validate(&distribution.frozen.source_manifest)
            .unwrap();
        assert_eq!(distribution.evidence[0].concrete.to_string(), "20.0.0");
    }
    #[test]
    fn full_evidence_and_source_binding_are_mandatory_and_immutable() {
        let mut b = bundle("20.0.0");
        assert!(b.validate(&Digest::of_bytes(b"other")).is_err());
        let original = b.clone();
        b.evidence.clear();
        assert!(b.validate(&b.frozen.source_manifest).is_err());
        let mut b = original.clone();
        b.evidence[0].catalog = Digest::of_bytes(b"later catalog");
        assert!(b.validate(&b.frozen.source_manifest).is_err());
        let mut b = original.clone();
        b.evidence.push(b.evidence[0].clone());
        assert!(b.validate(&b.frozen.source_manifest).is_err());
        let restored: FrozenBundle =
            serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
        assert_eq!(original, restored);
    }
    #[test]
    fn exact_pin_does_not_replace_an_unproven_alias_constraint() {
        let mut frozen = bundle("20.0.0").frozen;
        frozen.selections.clear();
        frozen.requirements[0]
            .constraints
            .push(Constraint::parse("lts/*").unwrap());
        frozen.requirements[0].provenance.push(Location {
            path: ".nvmrc".into(),
            first_line: 1,
            last_line: 1,
        });
        frozen.validate().unwrap();
        let unresolved = FrozenBundle::seal_exact(frozen).unwrap();
        assert!(unresolved.evidence.is_empty());
        assert!(unresolved.frozen.selections.is_empty());
        unresolved
            .validate_partial(&unresolved.frozen.source_manifest)
            .unwrap();
        assert_eq!(
            unresolved.validate(&unresolved.frozen.source_manifest),
            Err("toolchain_resolve_required")
        );
    }
    #[test]
    fn complete_resolution_still_requires_a_known_native_baseline() {
        let mut frozen = bundle("20.0.0").frozen;
        frozen.selections.clear();
        frozen.baseline.push(Baseline {
            tool: ToolId::parse("rust").unwrap(),
            version: VersionObservation::Unknown,
            provenance: Location {
                path: "profile".into(),
                first_line: 1,
                last_line: 1,
            },
        });
        let unresolved = FrozenBundle::seal_exact(frozen).unwrap();
        assert_eq!(unresolved.evidence.len(), 1);
        unresolved
            .validate_partial(&unresolved.frozen.source_manifest)
            .unwrap();
        assert_eq!(
            unresolved.validate(&unresolved.frozen.source_manifest),
            Err("toolchain_resolve_required")
        );
    }
    #[test]
    fn maximum_complete_evidence_bundle_is_accepted() {
        let scope = Scope::project("");
        let declarations: Vec<_> = (0..256)
            .map(|index| Declaration {
                tool: ToolId::parse(&format!("tool{index}")).unwrap(),
                constraint: Constraint::parse("1.0.0").unwrap(),
                scope: scope.clone(),
                location: Location {
                    path: "toolchain-manifest".into(),
                    first_line: index + 1,
                    last_line: index + 1,
                },
            })
            .collect();
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(b"source"),
            Digest::of_bytes(b"parent"),
            scope,
            &declarations,
            &[],
            &Default::default(),
            MatchPolicy::Exact,
        )
        .unwrap();
        let complete = FrozenBundle::seal_exact(frozen).unwrap();
        assert_eq!(complete.evidence.len(), 256);
        complete.validate(&complete.frozen.source_manifest).unwrap();
    }
    #[test]
    fn malformed_bundle_and_changed_contract_cannot_keep_resolution_authority() {
        let original = bundle("20.0.0");
        let source = original.frozen.source_manifest.clone();
        for schema in [0, 2, u32::MAX] {
            let mut changed = original.clone();
            changed.schema_version = schema;
            assert_eq!(changed.validate(&source), Err("toolchain_bundle_binding"));
        }
        let mut oversized = original.clone();
        oversized.evidence = vec![original.evidence[0].clone(); 257];
        assert_eq!(oversized.validate(&source), Err("toolchain_bundle_binding"));
        let mut duplicate = original.clone();
        duplicate.evidence.push(duplicate.evidence[0].clone());
        assert_eq!(
            duplicate.validate(&source),
            Err("duplicate_resolution_evidence")
        );
        let mut changed = original.clone();
        changed.frozen.schema_version = 0;
        assert!(changed.validate(&source).is_err());
        let mut changed = original.clone();
        changed.frozen.profile_definition = Digest::of_bytes(b"different profile");
        assert!(changed.validate(&source).is_err());
        assert_ne!(changed.digest(), original.digest());
    }
    #[test]
    fn baseline_requires_positive_consistent_native_observations() {
        use crate::toolchain::capability::{ToolObservation, WorkerCapabilities};
        let id = Digest::of_bytes(b"native");
        let available = |version: &str| ToolObservation::Available {
            version: version.into(),
            identity: id.clone(),
        };
        let mut report = WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            cpu_cores: None,
            memory_bytes: None,
            schema_version: 1,
            os: "linux".into(),
            arch: "aarch64".into(),
            profile: "default".into(),
            profile_digest: id.clone(),
            execution_uid: 1000,
            protocol_versions: vec![1],
            profile_fingerprint: id.clone(),
            tools: Default::default(),
            features: Default::default(),
            rustup: Vec::new(),
        };
        let mut frozen = bundle("20.0.0").frozen;
        assert_eq!(check_baselines(&frozen, None), Ok(()));
        frozen.baseline.push(Baseline {
            tool: ToolId::parse("rust").unwrap(),
            version: VersionObservation::parse("1.89.0"),
            provenance: Location {
                path: "baseline".into(),
                first_line: 1,
                last_line: 1,
            },
        });
        assert_eq!(
            check_baselines(&frozen, None),
            Err("toolchain_baseline_mismatch")
        );
        report.tools.insert("rustc".into(), available("1.89.0"));
        report.tools.insert("cargo".into(), available("1.89.0"));
        report.tools.insert("node".into(), ToolObservation::Unknown);
        report
            .tools
            .insert("go".into(), ToolObservation::Unavailable);
        report
            .tools
            .insert("invalid tool".into(), available("1.0.0"));
        assert_eq!(observations(&report).len(), 1);
        assert_eq!(check_baselines(&frozen, Some(&report)), Ok(()));
        report.tools.insert("cargo".into(), available("1.88.0"));
        assert_eq!(
            observations(&report)[&ToolId::parse("rust").unwrap()],
            VersionObservation::Unknown
        );
        assert_eq!(
            check_baselines(&frozen, Some(&report)),
            Err("toolchain_baseline_mismatch")
        );
        report.tools.remove("rustc");
        assert_eq!(
            check_baselines(&frozen, Some(&report)),
            Err("toolchain_baseline_mismatch")
        );
    }
    #[test]
    fn submitted_environment_cannot_replace_activation_but_allowed_values_merge() {
        let id = Digest::of_bytes(b"fixture");
        let binding = ExecutionBinding {
            activation: super::super::adapters::ActivationReceipt {
                base_profile: id.clone(),
                frozen: id.clone(),
                setup_policy: id.clone(),
                installation: id.clone(),
                environment: std::collections::BTreeMap::from([
                    ("PATH".into(), "/private/tool/bin:/usr/bin".into()),
                    ("HOME".into(), "/private/home".into()),
                    ("GOTOOLCHAIN".into(), "local".into()),
                ]),
                shell: super::super::adapters::FileIdentity {
                    path: "/bin/sh".into(),
                    digest: id.clone(),
                    size: 0,
                    executable: true,
                },
                binding_digest: id.clone(),
                execution_fingerprint: id,
            },
        };
        assert_eq!(binding.fingerprint(), &Digest::of_bytes(b"fixture"));
        for key in [
            "PATH",
            "HOME",
            "SHELL",
            "GOTOOLCHAIN",
            "RUSTUP_HOME",
            "MISE_DATA_DIR",
            "NODENV_ROOT",
            "COREPACK_HOME",
            "TMPDIR",
            "CI",
            "XDG_CACHE_HOME",
            "CARGO_HOME",
            "FVM_CACHE_PATH",
        ] {
            assert_eq!(
                binding.environment(&std::collections::BTreeMap::from([(
                    key.into(),
                    "override".into()
                )])),
                Err("protected_activation_environment")
            );
        }
        let env = binding
            .environment(&std::collections::BTreeMap::from([(
                "PROJECT_FLAG".into(),
                "yes".into(),
            )]))
            .unwrap();
        assert_eq!(env["PROJECT_FLAG"], "yes");
        assert_eq!(env["PATH"], binding.activation.environment["PATH"]);
        assert_eq!(env["HOME"], "/private/home");
        assert_eq!(env["GOTOOLCHAIN"], "local");
        assert_eq!(
            binding
                .environment(&std::collections::BTreeMap::from([(
                    "PATH".into(),
                    env["PATH"].clone()
                )]))
                .unwrap()["PATH"],
            env["PATH"]
        );
    }
    fn seen(tool: &str, version: VersionObservation) -> BTreeMap<ToolId, VersionObservation> {
        BTreeMap::from([(ToolId::parse(tool).unwrap(), version)])
    }
    #[test]
    fn a_range_is_sealed_to_the_version_the_parent_observed() {
        let open = bundle("20");
        let sealed = open
            .clone()
            .seal_observed(&seen("node", VersionObservation::parse("20.11.1")));
        sealed.validate(&sealed.frozen.source_manifest).unwrap();
        assert_eq!(sealed.evidence.len(), 1);
        assert_eq!(sealed.evidence[0].concrete.to_string(), "20.11.1");
        assert_eq!(sealed.frozen.selections[0].concrete.to_string(), "20.11.1");
        assert!(sealed.evidence[0].resolved_inputs.is_empty());
        // The evidence says it came from an observation, not from a source pin.
        let tagged = |domain: &str| {
            Digest::of_bytes(
                &serde_json::to_vec(&(
                    domain,
                    &open.frozen.source_manifest,
                    &open.frozen.scope,
                    &open.frozen.requirements[0],
                ))
                .unwrap(),
            )
        };
        assert_eq!(
            sealed.evidence[0].catalog,
            tagged("parent_observed_version_v1")
        );
        assert_ne!(sealed.evidence[0].catalog, tagged("source_exact_pin_v1"));
        assert_eq!(sealed.evidence[0].contract, open.frozen.digest());
    }
    #[test]
    fn an_observation_seals_nothing_it_does_not_prove() {
        let node = |version| seen("node", version);
        let known = || VersionObservation::parse("20.11.1");
        // No observation of that tool, or none that is a version in the range.
        for observed in [
            BTreeMap::new(),
            seen("go", known()),
            node(VersionObservation::Unknown),
            node(VersionObservation::identity("20.11.1-custom")),
            node(VersionObservation::parse("21.0.0")),
        ] {
            let open = bundle("20");
            assert_eq!(open.clone().seal_observed(&observed), open);
        }
        // A channel, an alias or an expression is never an observed version.
        for floating in ["lts/*", "latest", "stable", "${{ matrix.node }}"] {
            let open = bundle(floating);
            assert_eq!(open.clone().seal_observed(&node(known())), open);
        }
        // Every constraint of the requirement has to accept the version.
        for extra in [">=20.11.2", "lts/*"] {
            let mut frozen = bundle("20").frozen;
            frozen.requirements[0]
                .constraints
                .push(Constraint::parse(extra).unwrap());
            frozen.requirements[0].provenance.push(Location {
                path: ".nvmrc".into(),
                first_line: 1,
                last_line: 1,
            });
            frozen.validate().unwrap();
            let open = FrozenBundle::seal_exact(frozen).unwrap();
            assert!(open.evidence.is_empty());
            assert_eq!(open.clone().seal_observed(&node(known())), open);
        }
        // What already has evidence keeps it.
        let pinned = bundle("20.0.0");
        assert_eq!(
            pinned
                .clone()
                .seal_observed(&node(VersionObservation::parse("20.0.0"))),
            pinned
        );
        // A contract that cannot be resolved stays as it was.
        let mut broken = bundle("20");
        broken.frozen.schema_version = 2;
        assert_eq!(broken.clone().seal_observed(&node(known())), broken);
    }
    #[test]
    fn a_pub_range_is_sealed_by_the_same_rule() {
        let pub_bundle = || {
            let mut frozen = bundle_for("dart", "3.4.0").frozen;
            frozen.selections.clear();
            frozen.requirements[0].constraints =
                vec![Constraint::Pub(PubRange::parse("^3.4.0").unwrap())];
            FrozenBundle::seal_exact(frozen).unwrap()
        };
        let open = pub_bundle();
        assert!(open.evidence.is_empty());
        let sealed = open
            .clone()
            .seal_observed(&seen("dart", VersionObservation::parse("3.5.2")));
        sealed.validate(&sealed.frozen.source_manifest).unwrap();
        assert_eq!(sealed.evidence[0].concrete.to_string(), "3.5.2");
        assert_eq!(
            open.clone()
                .seal_observed(&seen("dart", VersionObservation::parse("4.0.0"))),
            open
        );
    }
    /// How a probe may spell what it saw: a leading `v` is the same version,
    /// a prerelease is another one. Neither a range nor a pin takes the
    /// prerelease for the release.
    #[test]
    fn a_prefixed_or_prerelease_observation_is_exactly_the_version_it_names() {
        let node = ToolId::parse("node").unwrap();
        let observed = |version| seen("node", VersionObservation::parse(version));
        let report = |version| native_report(&[("node", version, b"a")]);
        // The range is sealed to the version the prefixed spelling names.
        let sealed = bundle("20").seal_observed(&observed("v20.11.1"));
        assert_eq!(sealed.evidence[0].concrete.to_string(), "20.11.1");
        let pinned = bundle("20.11.1");
        assert_eq!(pinned.clone().seal_observed(&observed("v20.11.1")), pinned);
        assert_eq!(
            native_tool(&pinned.frozen, &node, &report("v20.11.1"))
                .unwrap()
                .version
                .to_string(),
            "20.11.1"
        );
        // A nightly of 20.11.1 is not in the range and is not the pin.
        let open = bundle("20");
        assert_eq!(
            open.clone().seal_observed(&observed("20.11.1-nightly")),
            open
        );
        assert_eq!(
            pinned.clone().seal_observed(&observed("20.11.1-nightly")),
            pinned
        );
        assert_eq!(pinned.evidence[0].concrete.to_string(), "20.11.1");
        assert_eq!(
            native_tool(&pinned.frozen, &node, &report("20.11.1-nightly")),
            None
        );
    }
    /// `rustc` and `cargo` are one tool; `dart` and `flutter` are two, though
    /// a Flutter SDK ships a `dart`: neither observation stands for the other.
    #[test]
    fn dart_and_flutter_are_observed_as_separate_tools() {
        let dart = ToolId::parse("dart").unwrap();
        let flutter = ToolId::parse("flutter").unwrap();
        let both = native_report(&[("dart", "3.5.2", b"d"), ("flutter", "3.24.0", b"f")]);
        assert_eq!(
            observations(&both),
            BTreeMap::from([
                (dart.clone(), VersionObservation::parse("3.5.2")),
                (flutter.clone(), VersionObservation::parse("3.24.0")),
            ])
        );
        let only_flutter = native_report(&[("flutter", "3.5.2", b"f")]);
        assert_eq!(observations(&only_flutter).get(&dart), None);
        let pinned = bundle_for("dart", "3.5.2");
        assert_eq!(native_tool(&pinned.frozen, &dart, &only_flutter), None);
        let native = native_tool(&pinned.frozen, &dart, &both).unwrap();
        assert_eq!(native.tool, dart);
        // The flutter beside it is no part of the identity of the dart.
        assert_eq!(
            native_tool(
                &pinned.frozen,
                &dart,
                &native_report(&[("dart", "3.5.2", b"d"), ("flutter", "3.19.0", b"other")]),
            ),
            Some(native)
        );
    }
    fn native_report(
        tools: &[(&str, &str, &[u8])],
    ) -> crate::toolchain::capability::WorkerCapabilities {
        use crate::toolchain::capability::{ToolObservation, WorkerCapabilities};
        let id = Digest::of_bytes(b"profile");
        WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            cpu_cores: None,
            memory_bytes: None,
            schema_version: 1,
            os: "linux".into(),
            arch: "aarch64".into(),
            profile: "default".into(),
            profile_digest: id.clone(),
            execution_uid: 1000,
            protocol_versions: vec![1],
            profile_fingerprint: id,
            tools: tools
                .iter()
                .map(|(name, version, identity)| {
                    (
                        name.to_string(),
                        ToolObservation::Available {
                            version: version.to_string(),
                            identity: Digest::of_bytes(identity),
                        },
                    )
                })
                .collect(),
            features: Default::default(),
            rustup: Vec::new(),
        }
    }
    #[test]
    fn a_selection_is_native_only_at_exactly_the_observed_version() {
        let node = ToolId::parse("node").unwrap();
        let frozen = bundle("20.0.0").frozen;
        let native = native_tool(
            &frozen,
            &node,
            &native_report(&[("node", "20.0.0", b"a"), ("go", "1.22.0", b"g")]),
        )
        .unwrap();
        assert_eq!(native.tool, node);
        assert_eq!(native.version.to_string(), "20.0.0");
        // Another tool is not part of the identity; the probe of this one is.
        assert_eq!(
            native_tool(
                &frozen,
                &node,
                &native_report(&[("node", "20.0.0", b"a"), ("go", "1.23.0", b"other")]),
            ),
            Some(native.clone())
        );
        let replaced =
            native_tool(&frozen, &node, &native_report(&[("node", "20.0.0", b"b")])).unwrap();
        assert_ne!(replaced.identity, native.identity);
        assert_eq!(replaced.version, native.version);
        // A different, unknown or absent version does not serve the selection.
        for report in [
            native_report(&[("node", "20.0.1", b"a")]),
            native_report(&[("node", "not a version", b"a")]),
            native_report(&[("go", "20.0.0", b"a")]),
        ] {
            assert_eq!(native_tool(&frozen, &node, &report), None);
        }
        // Nothing is native for a tool the contract did not select.
        let mut open = frozen.clone();
        open.selections.clear();
        let same = native_report(&[("node", "20.0.0", b"a")]);
        assert_eq!(native_tool(&open, &node, &same), None);
        assert_eq!(
            native_tool(&frozen, &ToolId::parse("go").unwrap(), &same),
            None
        );
    }
    #[test]
    fn a_native_tool_of_several_programs_needs_all_of_them_to_agree() {
        let rust = ToolId::parse("rust").unwrap();
        let frozen = bundle_for("rust", "1.89.0").frozen;
        let both = |cargo: &str, identity: &[u8]| {
            native_tool(
                &frozen,
                &rust,
                &native_report(&[("rustc", "1.89.0", b"r"), ("cargo", cargo, identity)]),
            )
        };
        let native = both("1.89.0", b"c").unwrap();
        assert_eq!(native.version.to_string(), "1.89.0");
        assert_ne!(both("1.89.0", b"d").unwrap().identity, native.identity);
        assert_eq!(both("1.88.0", b"c"), None);
    }
    fn rustup_report(
        versions: &[(&str, &str)],
    ) -> crate::toolchain::capability::WorkerCapabilities {
        let mut report = native_report(&[("rustc", "1.92.0", b"r"), ("cargo", "1.92.0", b"c")]);
        report.rustup = versions
            .iter()
            .map(
                |(name, version)| crate::toolchain::capability::DetectedRustup {
                    name: (*name).into(),
                    version: (*version).into(),
                    bin: format!("/rustup/toolchains/{name}/bin").into(),
                    rustc: Digest::of_bytes(name.as_bytes()),
                    cargo: Digest::of_bytes(version.as_bytes()),
                },
            )
            .collect();
        report
    }
    #[test]
    fn an_installed_rustup_toolchain_resolves_and_serves_a_range_the_profile_rust_does_not() {
        let rust = ToolId::parse("rust").unwrap();
        let open = bundle_for("rust", "1.89");
        assert!(open.evidence.is_empty());
        let requirement = open.frozen.requirements[0].clone();
        let report = rustup_report(&[
            ("1.89.0-host", "1.89.0"),
            ("1.89.1-host", "1.89.1"),
            ("1.90.0-host", "1.90.0"),
            ("nightly-host", "1.89.2-nightly"),
        ]);
        // The highest installed release the range accepts; never a nightly.
        let evidence = detected_rustup_evidence(&open.frozen, &requirement, &report).unwrap();
        assert_eq!(evidence.concrete.to_string(), "1.89.1");
        let mut sealed = open.clone();
        sealed.frozen = adapters::resolve(&open.frozen, &evidence).unwrap();
        sealed.evidence.push(evidence);
        sealed.validate(&sealed.frozen.source_manifest).unwrap();
        // The profile's own rust is 1.92.0: the toolchain serves it, by its bin.
        let native = native_tool(&sealed.frozen, &rust, &report).unwrap();
        assert_eq!(
            native.rustup,
            Some((
                "/rustup/toolchains/1.89.1-host/bin".into(),
                "1.89.1-host".into()
            ))
        );
        // Nothing installed in range: no evidence, no native tool.
        let none = rustup_report(&[
            ("1.90.0-host", "1.90.0"),
            ("nightly-host", "1.89.2-nightly"),
        ]);
        assert!(detected_rustup_evidence(&open.frozen, &requirement, &none).is_none());
        assert!(native_tool(&sealed.frozen, &rust, &none).is_none());
        // Only rust is detected.
        let node = bundle_for("node", "20");
        let report = rustup_report(&[("20.0.0-host", "20.0.0")]);
        assert!(
            detected_rustup_evidence(&node.frozen, &node.frozen.requirements[0], &report).is_none()
        );
    }
}
