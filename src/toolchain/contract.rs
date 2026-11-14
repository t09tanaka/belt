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
                let mut base = frozen.clone();
                base.selections.clear();
                let proof = ResolutionEvidence {
                    contract: base.digest(),
                    tool: requirement.tool.clone(),
                    concrete,
                    catalog: Digest::of_bytes(
                        &serde_json::to_vec(&(
                            "source_exact_pin_v1",
                            &base.source_manifest,
                            &base.scope,
                            &requirement,
                        ))
                        .expect("proof serializes"),
                    ),
                    resolved_inputs: Default::default(),
                };
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
    fn bundle(value: &str) -> FrozenBundle {
        let scope = Scope::project("");
        FrozenBundle::seal_exact(
            FrozenToolchains::freeze(
                Digest::of_bytes(b"source"),
                Digest::of_bytes(b"parent"),
                scope.clone(),
                &[Declaration {
                    tool: ToolId::parse("node").unwrap(),
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
}
