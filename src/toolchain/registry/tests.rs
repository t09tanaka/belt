use super::*;
use crate::toolchain::versions::{Declaration, Location, MatchPolicy, Scope};
use std::os::unix::fs::PermissionsExt;
struct Fixture {
    _temp: tempfile::TempDir,
    state: PathBuf,
    profile: Profile,
    config: Configuration,
}
impl Fixture {
    fn new(kind: AdapterKind, tool: &str) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let state = std::fs::canonicalize(temp.path()).unwrap();
        std::fs::create_dir(state.join("toolchains")).unwrap();
        std::fs::set_permissions(
            state.join("toolchains"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let manager = state.join("manager");
        // Never run. A visible marker would detect accidental manager discovery.
        std::fs::write(
            &manager,
            format!(
                "#!/bin/sh\necho ran > '{}/manager-ran'\nexit 91\n",
                state.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&manager, std::fs::Permissions::from_mode(0o700)).unwrap();
        let node = state.join("node");
        std::fs::copy(&manager, &node).unwrap();
        let node_build = state.join("node-build");
        std::fs::copy(&manager, &node_build).unwrap();
        let profiles = format!("schema_version=1\nactive='test'\n[profiles.test]\npath=[{:?}]\nshell='/bin/sh'\ncache_namespace='test'\n",state.to_str().unwrap());
        std::fs::write(state.join("profiles.toml"), profiles).unwrap();
        std::fs::set_permissions(
            state.join("profiles.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let profile = Profile::load(&state, &BTreeMap::new()).unwrap();
        let config = Configuration {
            schema_version: 1,
            active_profile: profile.name().into(),
            profile_definition: profile.definition_digest().clone(),
            recipes: vec![Recipe {
                tool: ToolId::parse(tool).unwrap(),
                kind,
                manager,
                dependencies: vec![node, node_build],
                search_path: vec![state.clone()],
                rust_host: "fixture-host".into(),
                inventory: vec!["1.2.3".parse().unwrap(), "1.3.0".parse().unwrap()],
                resolved_inputs: BTreeMap::new(),
            }],
        };
        let fixture = Self {
            _temp: temp,
            state,
            profile,
            config,
        };
        fixture.write();
        fixture
    }
    fn write(&self) {
        std::fs::write(
            self.state.join("toolchains.toml"),
            toml::to_string(&self.config).unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(
            self.state.join("toolchains.toml"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    fn load(&self) -> Result<Registry, String> {
        Registry::load(
            &self.state,
            &self.profile,
            Digest::of_bytes(b"checked actual worker"),
        )
    }
    fn frozen(&self, constraint: &str, source: &[u8]) -> FrozenToolchains {
        let scope = Scope::project("");
        FrozenToolchains::freeze(
            Digest::of_bytes(source),
            Digest::of_bytes(b"different parent profile"),
            scope.clone(),
            &[Declaration {
                tool: self.config.recipes[0].tool.clone(),
                constraint: Constraint::parse(constraint).unwrap(),
                scope,
                location: Location {
                    path: "captured".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .unwrap()
    }
}
#[test]
fn closed_recipes_use_private_exact_plans_without_discovery() {
    for (kind, tool) in [
        (AdapterKind::Nodenv, "node"),
        (AdapterKind::Rustup, "rust"),
        (AdapterKind::Mise, "python"),
        (AdapterKind::Fvm, "flutter"),
        (AdapterKind::Corepack, "pnpm"),
    ] {
        let f = Fixture::new(kind, tool);
        let registry = f.load().unwrap();
        let frozen = f.frozen("^1.2", b"source");
        let request = registry
            .request(&frozen, &f.config.recipes[0].tool, None)
            .unwrap();
        assert_eq!(request.evidence.concrete.to_string(), "1.3.0");
        assert_eq!(request.profile_definition, *f.profile.definition_digest());
        assert_ne!(
            request.profile_definition,
            request.frozen.profile_definition
        );
        assert_eq!(request.frozen.source_manifest, frozen.source_manifest);
        assert_eq!(
            request.base_profile,
            Digest::of_bytes(b"checked actual worker")
        );
        assert_eq!(registry.detect(&request).unwrap(), Availability::Missing);
        assert!(!f.state.join("manager-ran").exists());
        assert_eq!(
            std::fs::read_dir(f.state.join("toolchains"))
                .unwrap()
                .count(),
            0
        );
        let other = registry
            .request(&f.frozen("^1.2", b"another source"), &request.tool, None)
            .unwrap();
        assert_eq!(request.expected_plan, other.expected_plan);
        assert_ne!(request.evidence.contract, other.evidence.contract);
    }
}
#[test]
fn aliases_need_explicit_authority_and_parent_selection_is_preserved() {
    let mut f = Fixture::new(AdapterKind::Nodenv, "node");
    let tool = f.config.recipes[0].tool.clone();
    let frozen = f.frozen("lts/*", b"source");
    assert_eq!(
        f.load().unwrap().request(&frozen, &tool, None).unwrap_err(),
        Unavailable::ResolveRequired
    );
    f.config.recipes[0]
        .resolved_inputs
        .insert("lts/*".into(), "1.2.3".parse().unwrap());
    f.write();
    let registry = f.load().unwrap();
    let local = registry.request(&frozen, &tool, None).unwrap();
    let parent = ResolutionEvidence {
        contract: frozen.digest(),
        tool: tool.clone(),
        concrete: "1.3.0".parse().unwrap(),
        catalog: Digest::of_bytes(b"parent catalog"),
        resolved_inputs: [("lts/*".into(), "1.3.0".parse().unwrap())].into(),
    };
    let selected = adapters::resolve(&frozen, &parent).unwrap();
    let request = registry.request(&selected, &tool, Some(&parent)).unwrap();
    assert_eq!(request.frozen, selected);
    assert_eq!(request.evidence.digest(), parent.digest());
    assert_ne!(local.evidence.digest(), parent.digest());
    assert_eq!(
        registry.request(&selected, &tool, None).unwrap_err(),
        Unavailable::ResolveRequired
    );
    assert!(registry
        .request(&f.frozen("lts/*", b"other source"), &tool, Some(&parent))
        .is_err());
    let mut broken = request.clone();
    broken.base_profile = Digest::of_bytes(b"wire supplied");
    assert!(registry.detect(&broken).is_err());
    let mut outside = parent.clone();
    outside.concrete = "2.0.0".parse().unwrap();
    outside
        .resolved_inputs
        .insert("lts/*".into(), outside.concrete.clone());
    let selected = adapters::resolve(&frozen, &outside).unwrap();
    assert_eq!(
        registry
            .request(&selected, &tool, Some(&outside))
            .unwrap_err(),
        Unavailable::NotInInventory
    );
}
#[test]
fn snapshot_does_not_reread_config_but_host_drift_fails_closed() {
    let mut f = Fixture::new(AdapterKind::Nodenv, "node");
    let registry = f.load().unwrap();
    let original = registry.configuration_digest().unwrap().clone();
    f.config.recipes[0].inventory = vec!["2.0.0".parse().unwrap()];
    f.write();
    let frozen = f.frozen("1.2.3", b"source");
    let tool = &f.config.recipes[0].tool;
    assert!(registry.request(&frozen, tool, None).is_ok());
    let replacement = f.load().unwrap();
    assert_ne!(&original, replacement.configuration_digest().unwrap());
    assert_eq!(
        replacement.request(&frozen, tool, None).unwrap_err(),
        Unavailable::NotInInventory
    );
    std::fs::write(&f.config.recipes[0].manager, b"changed").unwrap();
    assert!(registry.request(&frozen, tool, None).is_err());
}
#[test]
fn configuration_is_strict_private_bounded_and_profile_bound() {
    let mut f = Fixture::new(AdapterKind::Nodenv, "node");
    let path = f.state.join("toolchains.toml");
    let original = std::fs::read(&path).unwrap();
    for text in [
        String::from_utf8(original.clone()).unwrap() + "\nextra='shell'\n",
        "schema_version=2".into(),
    ] {
        std::fs::write(&path, text).unwrap();
        assert!(f.load().is_err());
    }
    f.write();
    f.config.active_profile = "other".into();
    f.write();
    assert!(f.load().is_err());
    f.config.active_profile = f.profile.name().into();
    f.config.profile_definition = Digest::of_bytes(b"parent definition");
    f.write();
    assert!(f.load().is_err());
    f.config.profile_definition = f.profile.definition_digest().clone();
    f.config.recipes.push(f.config.recipes[0].clone());
    f.write();
    assert!(f.load().is_err());
    f.config.recipes.pop();
    f.write();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    assert!(f.load().is_err());
    f.write();
    let hard = f.state.join("hard");
    std::fs::hard_link(&path, &hard).unwrap();
    assert!(f.load().is_err());
    std::fs::remove_file(hard).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(f.state.join("profiles.toml"), &path).unwrap();
    assert!(f.load().is_err());
    std::fs::remove_file(&path).unwrap();
    let registry = f.load().unwrap();
    assert_eq!(
        registry
            .request(
                &f.frozen("1.2.3", b"source"),
                &f.config.recipes[0].tool,
                None
            )
            .unwrap_err(),
        Unavailable::Unconfigured
    );
    f.write();
    std::fs::write(&path, vec![b' '; MAX_CONFIG as usize + 1]).unwrap();
    assert!(f.load().is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(f.load().is_err());
}
#[test]
fn invalid_host_paths_limits_and_recipe_pairs_are_rejected() {
    let mut f = Fixture::new(AdapterKind::Rustup, "rust");
    let initial = f.config.recipes[0].clone();
    let mut variants = vec![];
    let link = f.state.join("manager-link");
    std::os::unix::fs::symlink(&initial.manager, &link).unwrap();
    let mut r = initial.clone();
    r.manager = link;
    variants.push(r);
    let mut r = initial.clone();
    r.manager = f.state.join("toolchains/../manager");
    variants.push(r);
    let mut r = initial.clone();
    r.manager = f.state.join("toolchains");
    variants.push(r);
    let mut r = initial.clone();
    r.rust_host = "".into();
    variants.push(r);
    let mut r = initial.clone();
    r.manager = PathBuf::from("relative");
    variants.push(r);
    let mut r = initial.clone();
    r.search_path = vec![f.state.join("manager")];
    variants.push(r);
    let mut r = initial.clone();
    r.inventory = vec!["1.2.3".parse().unwrap(); 257];
    variants.push(r);
    let mut r = initial.clone();
    r.kind = AdapterKind::Fvm;
    variants.push(r);
    let mut r = initial.clone();
    r.dependencies = vec![r.manager.clone(); 33];
    variants.push(r);
    let mut r = initial.clone();
    r.resolved_inputs
        .insert("lts/*".into(), "9.0.0".parse().unwrap());
    variants.push(r);
    for recipe in variants {
        f.config.recipes = vec![recipe];
        f.write();
        assert!(f.load().is_err());
    }
    f.config.recipes = vec![initial; 33];
    f.write();
    assert!(f.load().is_err());
}

#[test]
fn negative_outcomes_and_frozen_authority_are_distinct() {
    let f = Fixture::new(AdapterKind::Nodenv, "node");
    let registry = f.load().unwrap();
    let tool = f.config.recipes[0].tool.clone();
    let frozen = f.frozen("1.2.3", b"source");
    assert_eq!(
        registry
            .request(&frozen, &ToolId::parse("python").unwrap(), None)
            .unwrap_err(),
        Unavailable::Unsupported
    );
    assert_eq!(
        registry
            .request(&frozen, &ToolId::parse("xcode").unwrap(), None)
            .unwrap_err(),
        Unavailable::ManualRequired("install_xcode_and_accept_license_manually")
    );
    for expression in ["${{ matrix.node }}", "lts/*"] {
        assert_eq!(
            registry
                .request(&f.frozen(expression, b"source"), &tool, None)
                .unwrap_err(),
            Unavailable::ResolveRequired
        );
    }
    let request = registry.request(&frozen, &tool, None).unwrap();
    let mut evidence = request.evidence.clone();
    evidence.catalog = Digest::of_bytes(b"different authority");
    assert!(registry
        .request(&request.frozen, &tool, Some(&evidence))
        .is_err());
    let mut changed = request.frozen.clone();
    changed.source_manifest = Digest::of_bytes(b"different source");
    assert!(registry
        .request(&changed, &tool, Some(&request.evidence))
        .is_err());
    assert!(registry
        .request(&frozen, &tool, Some(&request.evidence))
        .is_err());
}

#[test]
fn static_expression_mapping_cannot_select_a_matrix_leg() {
    let mut f = Fixture::new(AdapterKind::Nodenv, "node");
    let expression = "${{ matrix.node }}";
    f.config.recipes[0]
        .resolved_inputs
        .insert(expression.into(), "1.2.3".parse().unwrap());
    f.write();
    let registry = f.load().unwrap();
    let tool = f.config.recipes[0].tool.clone();
    let frozen = f.frozen(expression, b"same captured workflow");
    assert_eq!(
        registry.request(&frozen, &tool, None).unwrap_err(),
        Unavailable::ResolveRequired
    );
    let mut plans = vec![];
    for version in ["1.2.3", "1.3.0"] {
        let concrete: Version = version.parse().unwrap();
        let evidence = ResolutionEvidence {
            contract: frozen.digest(),
            tool: tool.clone(),
            concrete: concrete.clone(),
            catalog: Digest::of_bytes(version.as_bytes()),
            resolved_inputs: [(expression.into(), concrete)].into(),
        };
        let selected = adapters::resolve(&frozen, &evidence).unwrap();
        let request = registry.request(&selected, &tool, Some(&evidence)).unwrap();
        assert_eq!(request.evidence.digest(), evidence.digest());
        plans.push(request.expected_plan);
    }
    assert_ne!(plans[0], plans[1]);
    let mut version_file = frozen.clone();
    version_file.requirements[0].constraints =
        vec![Constraint::VersionFileExpression(expression.into())];
    assert_eq!(
        registry.request(&version_file, &tool, None).unwrap_err(),
        Unavailable::ResolveRequired
    );
}

#[test]
fn baseline_only_contract_requires_base_observation_check_not_installation() {
    let f = Fixture::new(AdapterKind::Nodenv, "node");
    let registry = f.load().unwrap();
    let tool = f.config.recipes[0].tool.clone();
    let frozen = FrozenToolchains::freeze(
        Digest::of_bytes(b"source"),
        Digest::of_bytes(b"parent"),
        Scope::project(""),
        &[],
        &[(
            tool.clone(),
            Location {
                path: "workflow".into(),
                first_line: 1,
                last_line: 1,
            },
        )],
        &[(
            tool.clone(),
            crate::toolchain::versions::VersionObservation::parse("1.2.3"),
        )]
        .into(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(frozen.validate().is_ok());
    assert_eq!(
        registry.request(&frozen, &tool, None).unwrap_err(),
        Unavailable::BaselineCheckRequired
    );
}

#[test]
fn multi_tool_callers_complete_resolution_before_building_install_requests() {
    let mut f = Fixture::new(AdapterKind::Nodenv, "node");
    let node = f.config.recipes[0].tool.clone();
    let rust = ToolId::parse("rust").unwrap();
    let mut recipe = f.config.recipes[0].clone();
    recipe.kind = AdapterKind::Rustup;
    recipe.tool = rust.clone();
    f.config.recipes.push(recipe);
    f.write();
    let registry = f.load().unwrap();
    let scope = Scope::project("");
    let declarations: Vec<_> = [node.clone(), rust.clone()]
        .into_iter()
        .map(|tool| Declaration {
            tool,
            constraint: Constraint::parse("^1.2").unwrap(),
            scope: scope.clone(),
            location: Location {
                path: "captured".into(),
                first_line: 1,
                last_line: 1,
            },
        })
        .collect();
    let initial = FrozenToolchains::freeze(
        Digest::of_bytes(b"source"),
        Digest::of_bytes(b"parent"),
        scope,
        &declarations,
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    // These are resolution intermediates, never submitted to Setups.
    let node_intermediate = registry.request(&initial, &node, None).unwrap();
    let rust_intermediate = registry
        .request(&node_intermediate.frozen, &rust, None)
        .unwrap();
    let complete = &rust_intermediate.frozen;
    assert!(complete.is_resolved());
    let node_final = registry
        .request(complete, &node, Some(&node_intermediate.evidence))
        .unwrap();
    let rust_final = registry
        .request(complete, &rust, Some(&rust_intermediate.evidence))
        .unwrap();
    assert_eq!(node_final.frozen, rust_final.frozen);
    assert_eq!(&node_final.frozen, complete);
    assert_eq!(
        node_final.evidence.digest(),
        node_intermediate.evidence.digest()
    );
    assert_eq!(node_final.expected_plan, node_intermediate.expected_plan);
    assert_ne!(node_final.digest(), node_intermediate.digest());
}
