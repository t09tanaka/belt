use super::*;
use crate::toolchain::versions::{Declaration, Location, MatchPolicy, Scope};
use std::os::unix::fs::{symlink, PermissionsExt};
fn base_env(plan: &InstallPlan) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("HOME".into(), text(plan.root.parent().unwrap()).unwrap()),
        (
            "PATH".into(),
            text(plan.external[0].path.parent().unwrap()).unwrap(),
        ),
        ("XDG_CONFIG_HOME".into(), "/private/worker-config".into()),
        ("TMPDIR".into(), "/private/worker-tmp".into()),
    ])
}
fn executable(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
struct Fixture {
    _temp: tempfile::TempDir,
    plan: InstallPlan,
    shell: FileIdentity,
}
fn fixture(kind: AdapterKind, tool: &str) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let state = fs::canonicalize(temp.path()).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    let hostdir = state.join("host");
    fs::create_dir(&hostdir).unwrap();
    for name in ["manager", "node", "node-build", "shell", "shim.js"] {
        executable(&hostdir.join(name), name.as_bytes());
    }
    let tool = ToolId::parse(tool).unwrap();
    let scope = Scope::project("");
    let decl = Declaration {
        tool: tool.clone(),
        constraint: Constraint::parse("1.2.3").unwrap(),
        scope: scope.clone(),
        location: Location {
            path: "captured".into(),
            first_line: 1,
            last_line: 1,
        },
    };
    let frozen = FrozenToolchains::freeze(
        Digest::of_bytes(b"source"),
        Digest::of_bytes(b"profile"),
        scope,
        &[decl],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    let evidence = ResolutionEvidence {
        contract: frozen.digest(),
        tool: tool.clone(),
        concrete: Version::parse("1.2.3").unwrap(),
        catalog: Digest::of_bytes(b"authenticated-catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    let frozen = resolve(&frozen, &evidence).unwrap();
    let host = HostTools {
        manager: FileIdentity::capture(&hostdir.join("manager")).unwrap(),
        dependencies: ["node", "node-build", "shim.js"]
            .iter()
            .map(|p| FileIdentity::capture(&hostdir.join(p)).unwrap())
            .collect(),
        search_path: vec![hostdir.clone()],
        rust_host: "aarch64-apple-darwin".into(),
    };
    let plan = plan(kind, &frozen, &tool, &evidence, &state, &host).unwrap();
    let shell = FileIdentity::capture(&hostdir.join("shell")).unwrap();
    Fixture {
        _temp: temp,
        plan,
        shell,
    }
}
#[derive(Default)]
struct Fake {
    expected: Vec<CommandSpec>,
    calls: Vec<CommandSpec>,
    populate: Option<InstallPlan>,
    wrong: bool,
    fail: bool,
}
impl Fake {
    fn for_plan(plan: &InstallPlan) -> Self {
        Self {
            expected: plan
                .commands
                .iter()
                .cloned()
                .chain(plan.probes.iter().map(|(_, p)| p.clone()))
                .collect(),
            populate: Some(plan.clone()),
            ..Self::default()
        }
    }
}
impl AdmittedExecutor for Fake {
    fn execute(&mut self, command: &CommandSpec) -> Result<CommandOutput> {
        assert_eq!(Some(command), self.expected.get(self.calls.len()));
        self.calls.push(command.clone());
        if self.fail {
            return Err("fake_cancelled");
        }
        let p = self.populate.as_ref().unwrap();
        if self.calls.len() == 1 {
            fs::create_dir_all(&p.payload).unwrap();
            for (_, probe) in &p.probes {
                executable(&probe.program, b"private-concrete-tool");
            }
        }
        let name = p
            .probes
            .iter()
            .find(|(_, probe)| probe == command)
            .map(|(name, _)| name.as_str());
        let version = if self.wrong { "9.9.9" } else { "1.2.3" };
        let stdout = match name {
            Some("flutter") => format!("{{\"frameworkVersion\":\"{version}\"}}"),
            Some("rustc") => format!("rustc {version} (fixture)"),
            Some("cargo") => format!("cargo {version} (fixture)"),
            Some("python3") => format!("Python {version}"),
            Some("go") => format!("go version go{version} fixture"),
            Some("ruby") => format!("ruby {version} (fixture)"),
            Some(_) => version.into(),
            None => String::new(),
        };
        Ok(CommandOutput {
            success: true,
            stdout: stdout.into_bytes(),
        })
    }
}
#[test]
fn all_five_closed_lifecycles_and_identical_setup_consumer_environment() {
    for (kind, tool) in [
        (AdapterKind::Nodenv, "node"),
        (AdapterKind::Rustup, "rust"),
        (AdapterKind::Mise, "python"),
        (AdapterKind::Fvm, "flutter"),
        (AdapterKind::Corepack, "pnpm"),
    ] {
        let f = fixture(kind, tool);
        let p = &f.plan;
        assert_eq!(detect(p), Availability::Missing);
        for c in p.commands() {
            assert_eq!(c.cwd, p.root);
            assert!(c.env["HOME"].starts_with(p.root.to_str().unwrap()));
            assert!(!c
                .argv
                .iter()
                .any(|a| matches!(a.as_str(), "use" | "global" | "default")));
        }
        let mut fake = Fake::for_plan(p);
        let receipt = install_admitted(p, &mut fake).unwrap();
        assert_eq!(fake.calls, fake.expected);
        assert_eq!(
            detect(p),
            Availability::Installed(Box::new(receipt.clone()))
        );
        let base = Digest::of_bytes(b"base");
        let setup = Digest::of_bytes(b"setup");
        let activation = activate(
            p,
            &receipt,
            base.clone(),
            &base_env(p),
            setup.clone(),
            &f.shell,
        )
        .unwrap();
        assert_eq!(activation.environment["HOME"], base_env(p)["HOME"]);
        assert_eq!(
            activation.environment["XDG_CONFIG_HOME"],
            base_env(p)["XDG_CONFIG_HOME"]
        );
        assert_eq!(activation.environment["TMPDIR"], base_env(p)["TMPDIR"]);
        assert_eq!(activation.base_profile, base);
        assert_ne!(activation.execution_fingerprint, base);
        assert_eq!(
            activation,
            activate(
                p,
                &receipt,
                base.clone(),
                &base_env(p),
                setup.clone(),
                &f.shell
            )
            .unwrap()
        );
        let changed = activate(
            p,
            &receipt,
            base,
            &base_env(p),
            Digest::of_bytes(b"new-policy"),
            &f.shell,
        )
        .unwrap();
        assert_ne!(
            changed.execution_fingerprint,
            activation.execution_fingerprint
        );
        let mut never = Fake::default();
        assert_eq!(install_admitted(p, &mut never).unwrap(), receipt);
        assert!(never.calls.is_empty());
        executable(&p.probes[0].1.program, b"drift");
        assert!(matches!(
            detect(p),
            Availability::Unknown("installation_tree_drift")
        ));
        assert!(activate(
            p,
            &receipt,
            Digest::of_bytes(b"base"),
            &base_env(p),
            setup,
            &f.shell
        )
        .is_err());
    }
}
#[test]
fn crashes_wrong_versions_and_concurrency_never_publish_success() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let mut fake = Fake::for_plan(&f.plan);
    fake.wrong = true;
    assert_eq!(
        install_admitted(&f.plan, &mut fake),
        Err("installed_version_mismatch")
    );
    assert!(!f.plan.root.join("receipt.json").exists());
    assert_eq!(
        install_admitted(&f.plan, &mut fake),
        Err("installation_partial_requires_cleanup")
    );
    let f = fixture(AdapterKind::Fvm, "flutter");
    let mut fake = Fake::for_plan(&f.plan);
    fake.fail = true;
    assert_eq!(install_admitted(&f.plan, &mut fake), Err("fake_cancelled"));
    assert!(matches!(detect(&f.plan), Availability::Unknown(_)));
    let f = fixture(AdapterKind::Rustup, "rust");
    mkdir(&f.plan.root).unwrap();
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(f.plan.root.join("install.lock"))
        .unwrap();
    lock.lock().unwrap();
    assert_eq!(
        install_admitted(&f.plan, &mut Fake::default()),
        Err("installation_busy")
    );
    drop(lock);
}
#[test]
fn symlink_escape_receipt_corruption_external_drift_and_manual_reasons() {
    let f = fixture(AdapterKind::Corepack, "pnpm");
    let mut fake = Fake::for_plan(&f.plan);
    let receipt = install_admitted(&f.plan, &mut fake).unwrap();
    fs::write(f.plan.payload.join("secret"), b"data").unwrap();
    assert!(receipt.validate(&f.plan).is_err());
    let f = fixture(AdapterKind::Mise, "node");
    let mut fake = Fake::for_plan(&f.plan);
    install_admitted(&f.plan, &mut fake).unwrap();
    fs::write(f.plan.root.join("receipt.json"), b"{}").unwrap();
    assert_eq!(detect(&f.plan), Availability::Unknown("receipt_invalid"));
    let f = fixture(AdapterKind::Nodenv, "node");
    mkdir(&f.plan.root).unwrap();
    symlink(&f.plan.external[0].path, &f.plan.payload).unwrap();
    assert_eq!(tree(&f.plan), Err("installation_link_escape"));
    let f = fixture(AdapterKind::Nodenv, "node");
    executable(&f.plan.external[0].path, b"manager-updated");
    assert_eq!(
        install_admitted(&f.plan, &mut Fake::default()),
        Err("tool_identity_drift")
    );
    for tool in ["xcode", "docker-desktop", "system-package"] {
        assert!(manual_requirement(tool).is_some());
    }
    assert!(manual_requirement("node").is_none());
}
#[test]
fn corepack_hash_descriptor_and_input_version_validation() {
    assert_eq!(
        output_version("node", b"v1.2.3\n").unwrap(),
        Version::parse("1.2.3").unwrap()
    );
    assert!(output_version("rustc", b"cargo 1.2.3").is_err());
    assert!(output_version("node", &vec![b'1'; 8193]).is_err());
    assert!(output_version("flutter", b"{}").is_err());
    let f = fixture(AdapterKind::Corepack, "pnpm");
    assert_eq!(
        f.plan.commands[0].argv,
        vec!["install", "--global", "pnpm@1.2.3"]
    );
    assert_eq!(f.plan.activation_env["COREPACK_ENABLE_PROJECT_SPEC"], "0");
    assert_eq!(f.plan.activation_env["COREPACK_ENABLE_NETWORK"], "0");
    assert_eq!(f.plan.commands[1].argv[0], "enable");
    let f = fixture(AdapterKind::Nodenv, "node");
    assert_eq!(
        f.plan.commands[0].program.file_name().unwrap(),
        "node-build"
    );
    assert_eq!(f.plan.commands[0].argv[0], "1.2.3");
}
#[test]
fn complete_multi_tool_activation_fails_closed_for_missing_selection_and_baseline() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap();
    let scope = Scope::project("");
    let declarations = ["node", "pnpm"].map(|tool| Declaration {
        tool: ToolId::parse(tool).unwrap(),
        constraint: Constraint::parse("1.2.3").unwrap(),
        scope: scope.clone(),
        location: Location {
            path: "captured".into(),
            first_line: 1,
            last_line: 1,
        },
    });
    let initial = FrozenToolchains::freeze(
        Digest::of_bytes(b"source"),
        Digest::of_bytes(b"profile"),
        scope,
        &declarations,
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    let evidence = |tool: &str| ResolutionEvidence {
        contract: initial.digest(),
        tool: ToolId::parse(tool).unwrap(),
        concrete: Version::parse("1.2.3").unwrap(),
        catalog: Digest::of_bytes(b"catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    let node_evidence = evidence("node");
    let pm_evidence = evidence("pnpm");
    let frozen = resolve(&resolve(&initial, &node_evidence).unwrap(), &pm_evidence).unwrap();
    let host = HostTools {
        manager: f.plan.external[0].clone(),
        dependencies: f.plan.external[1..].to_vec(),
        search_path: vec![f.plan.external[0].path.parent().unwrap().into()],
        rust_host: "aarch64-apple-darwin".into(),
    };
    let node = plan(
        AdapterKind::Nodenv,
        &frozen,
        &node_evidence.tool,
        &node_evidence,
        state,
        &host,
    )
    .unwrap();
    let pm = plan(
        AdapterKind::Corepack,
        &frozen,
        &pm_evidence.tool,
        &pm_evidence,
        state,
        &host,
    )
    .unwrap();
    let nr = install_admitted(&node, &mut Fake::for_plan(&node)).unwrap();
    let pr = install_admitted(&pm, &mut Fake::for_plan(&pm)).unwrap();
    let base = Digest::of_bytes(b"base");
    let policy = Digest::of_bytes(b"policy");
    assert_eq!(
        activate(
            &node,
            &nr,
            base.clone(),
            &base_env(&node),
            policy.clone(),
            &f.shell
        ),
        Err("complete_activation_set_required")
    );
    let home = state.join("attempt-home");
    mkdir(&home).unwrap();
    let env = BTreeMap::from([
        ("PATH".into(), text(&host.search_path[0]).unwrap()),
        ("APP_ENV".into(), "test".into()),
        ("HOME".into(), text(&home).unwrap()),
    ]);
    let activate_pair = |pairs: &[(&InstallPlan, &InstallationReceipt)]| {
        activate_set(
            &frozen,
            pairs,
            base.clone(),
            &BTreeMap::new(),
            &env,
            policy.clone(),
            &f.shell,
        )
    };
    assert_eq!(
        activate_pair(&[(&node, &nr)]),
        Err("complete_activation_set_required")
    );
    let result = activate_pair(&[(&node, &nr), (&pm, &pr)]).unwrap();
    assert_eq!(result.environment["APP_ENV"], "test");
    assert_eq!(result.environment["HOME"], text(&home).unwrap());
    assert_eq!(
        result.environment["COREPACK_HOME"],
        text(&pm.payload).unwrap()
    );
    assert_eq!(result, activate_pair(&[(&pm, &pr), (&node, &nr)]).unwrap());
    assert_eq!(
        activate_pair(&[(&node, &nr), (&node, &nr)]),
        Err("activation_contract_mismatch")
    );
}
#[test]
fn corepack_external_shim_is_explicitly_bound_and_internal_links_are_validated() {
    let f = fixture(AdapterKind::Corepack, "pnpm");
    let mut fake = Fake::for_plan(&f.plan);
    let receipt = install_admitted(&f.plan, &mut fake).unwrap();
    let shim = &f.plan.probes[0].1.program;
    fs::remove_file(shim).unwrap();
    let trusted = &f
        .plan
        .external
        .iter()
        .find(|d| d.path.file_name().unwrap() == "shim.js")
        .unwrap()
        .path;
    symlink(trusted, shim).unwrap();
    let mut updated = receipt.clone();
    updated.tree = tree(&f.plan).unwrap();
    updated.validate(&f.plan).unwrap();
    executable(trusted, b"external-updated");
    assert_eq!(updated.validate(&f.plan), Err("tool_identity_drift"));
    let f = fixture(AdapterKind::Mise, "node");
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    let target = &f.plan.probes[0].1.program;
    symlink(target, f.plan.payload.join("tool-link")).unwrap();
    assert_eq!(tree(&f.plan).unwrap().links.len(), 1);
    fs::set_permissions(target, fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(tree(&f.plan), Err("unsafe_installation_owner_or_mode"));
}
#[test]
fn cross_source_artifact_reuse_preserves_actual_identity_and_changes_binding() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let p = &f.plan;
    let receipt = install_admitted(p, &mut Fake::for_plan(p)).unwrap();
    let mut second = p.clone();
    second.frozen.source_manifest = Digest::of_bytes(b"other-source");
    second.frozen.selections.clear();
    let evidence = ResolutionEvidence {
        contract: second.frozen.digest(),
        tool: p.tool.clone(),
        concrete: p.version.clone(),
        catalog: Digest::of_bytes(b"catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    second.frozen = resolve(&second.frozen, &evidence).unwrap();
    assert_eq!(second.key(), p.key());
    receipt.validate(&second).unwrap();
    let mut never = Fake::default();
    assert_eq!(install_admitted(&second, &mut never).unwrap(), receipt);
    assert!(never.calls.is_empty());
    let base = Digest::of_bytes(b"base");
    let setup = Digest::of_bytes(b"setup");
    let first = activate(
        p,
        &receipt,
        base.clone(),
        &base_env(p),
        setup.clone(),
        &f.shell,
    )
    .unwrap();
    let next = activate(&second, &receipt, base, &base_env(p), setup, &f.shell).unwrap();
    assert_eq!(first.execution_fingerprint, next.execution_fingerprint);
    assert_ne!(first.binding_digest, next.binding_digest);
}

#[test]
fn frozen_distribution_integrity_and_resolution_authority_are_enforced() {
    let f = fixture(AdapterKind::Corepack, "pnpm");
    let state = f.plan.root.parent().unwrap();
    let host = HostTools {
        manager: f.plan.external[0].clone(),
        dependencies: f.plan.external[1..].to_vec(),
        search_path: vec![f.plan.external[0].path.parent().unwrap().into()],
        rust_host: "aarch64-apple-darwin".into(),
    };
    let mut frozen = f.plan.frozen.clone();
    frozen.selections.clear();
    let integrity = format!("sha512.{}", "a".repeat(128));
    frozen.requirements[0].constraints = vec![Constraint::Distribution {
        version: Version::parse("1.2.3").unwrap(),
        integrity: integrity.clone(),
    }];
    let mut evidence = ResolutionEvidence {
        contract: frozen.digest(),
        tool: f.plan.tool.clone(),
        concrete: Version::parse("1.2.3").unwrap(),
        catalog: Digest::of_bytes(b"catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    let selected = resolve(&frozen, &evidence).unwrap();
    let p = plan(
        AdapterKind::Corepack,
        &selected,
        &evidence.tool,
        &evidence,
        state,
        &host,
    )
    .unwrap();
    assert_eq!(
        p.commands[0].argv,
        vec!["install", "--global", &format!("pnpm@1.2.3+{integrity}")]
    );
    evidence.catalog = Digest::of_bytes(b"different-catalog");
    assert_eq!(
        plan(
            AdapterKind::Corepack,
            &selected,
            &evidence.tool,
            &evidence,
            state,
            &host
        )
        .unwrap_err(),
        "resolution_authority_mismatch"
    );
    let mut alias = f.plan.frozen.clone();
    alias.selections.clear();
    alias.requirements[0].constraints = vec![Constraint::Resolve("latest".into())];
    let mut evidence = ResolutionEvidence {
        contract: alias.digest(),
        tool: f.plan.tool.clone(),
        concrete: Version::parse("1.2.3").unwrap(),
        catalog: Digest::of_bytes(b"catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    assert_eq!(
        resolve(&alias, &evidence).unwrap_err(),
        "resolution_input_unproven"
    );
    evidence
        .resolved_inputs
        .insert("latest".into(), evidence.concrete.clone());
    assert!(resolve(&alias, &evidence).is_ok());
    evidence.contract = Digest::of_bytes(b"other-contract");
    assert_eq!(
        resolve(&alias, &evidence).unwrap_err(),
        "resolution_contract_mismatch"
    );
    let f = fixture(AdapterKind::Nodenv, "node");
    let mut frozen = f.plan.frozen.clone();
    frozen.selections.clear();
    frozen.requirements[0].constraints = vec![Constraint::Distribution {
        version: Version::parse("1.2.3").unwrap(),
        integrity,
    }];
    let evidence = ResolutionEvidence {
        contract: frozen.digest(),
        tool: f.plan.tool.clone(),
        concrete: Version::parse("1.2.3").unwrap(),
        catalog: Digest::of_bytes(b"catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    let selected = resolve(&frozen, &evidence).unwrap();
    let host = HostTools {
        manager: f.plan.external[0].clone(),
        dependencies: f.plan.external[1..].to_vec(),
        search_path: vec![],
        rust_host: String::new(),
    };
    assert_eq!(
        plan(
            AdapterKind::Nodenv,
            &selected,
            &evidence.tool,
            &evidence,
            f.plan.root.parent().unwrap(),
            &host
        )
        .unwrap_err(),
        "distribution_integrity_not_supported"
    );
}

#[test]
fn manager_recipes_are_exact_and_base_identity_policy_stays_separate() {
    let f = fixture(AdapterKind::Rustup, "rust");
    assert_eq!(
        f.plan.commands[0].argv,
        vec![
            "toolchain",
            "install",
            "1.2.3-aarch64-apple-darwin",
            "--profile",
            "minimal",
            "--no-self-update"
        ]
    );
    let f = fixture(AdapterKind::Mise, "node");
    assert_eq!(
        f.plan.commands[0].argv,
        vec![
            "install-into",
            "node@1.2.3",
            f.plan.payload.to_str().unwrap()
        ]
    );
    assert_eq!(f.plan.commands[0].env["MISE_NO_CONFIG"], "1");
    assert_eq!(f.plan.commands[0].env["MISE_NO_HOOKS"], "1");
    let f = fixture(AdapterKind::Fvm, "flutter");
    assert_eq!(
        f.plan.commands[0].argv,
        vec!["install", "1.2.3", "--skip-pub-get"]
    );
    assert_eq!(f.plan.probes[0].1.argv, vec!["--version", "--machine"]);
    let f = fixture(AdapterKind::Nodenv, "node");
    let receipt = install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    let env = base_env(&f.plan);
    let base = Digest::of_bytes(b"base");
    let policy = Digest::of_bytes(b"policy");
    let original = activate(
        &f.plan,
        &receipt,
        base.clone(),
        &env,
        policy.clone(),
        &f.shell,
    )
    .unwrap();
    let mut changed_env = env.clone();
    changed_env.insert("APP_ENV".into(), "changed".into());
    let changed = activate(
        &f.plan,
        &receipt,
        base.clone(),
        &changed_env,
        policy.clone(),
        &f.shell,
    )
    .unwrap();
    assert_ne!(
        original.execution_fingerprint,
        changed.execution_fingerprint
    );
    let mut bad_env = env;
    bad_env.insert("PATH".into(), "relative".into());
    assert_eq!(
        activate(
            &f.plan,
            &receipt,
            base.clone(),
            &bad_env,
            policy.clone(),
            &f.shell
        ),
        Err("invalid_base_path")
    );
    let mut with_baseline = f.plan.clone();
    with_baseline
        .frozen
        .baseline
        .push(crate::toolchain::versions::Baseline {
            tool: ToolId::parse("git").unwrap(),
            version: crate::toolchain::versions::VersionObservation::Known(
                Version::parse("1.2.3").unwrap(),
            ),
            provenance: Location {
                path: "profile".into(),
                first_line: 1,
                last_line: 1,
            },
        });
    assert_eq!(
        activate_set(
            &with_baseline.frozen,
            &[(&with_baseline, &receipt)],
            base,
            &BTreeMap::new(),
            &base_env(&with_baseline),
            policy,
            &f.shell
        ),
        Err("activation_baseline_mismatch")
    );
}

#[test]
fn restored_invalid_integrity_never_reaches_installer() {
    let f = fixture(AdapterKind::Corepack, "pnpm");
    let mut frozen = f.plan.frozen.clone();
    frozen.selections.clear();
    frozen.requirements[0].constraints = vec![Constraint::Distribution {
        version: f.plan.version.clone(),
        integrity: "sha512.invalid".into(),
    }];
    let evidence = ResolutionEvidence {
        contract: frozen.digest(),
        tool: f.plan.tool.clone(),
        concrete: f.plan.version.clone(),
        catalog: Digest::of_bytes(b"catalog"),
        resolved_inputs: BTreeMap::new(),
    };
    let frozen = resolve(&frozen, &evidence).unwrap();
    let host = HostTools {
        manager: f.plan.external[0].clone(),
        dependencies: f.plan.external[1..].to_vec(),
        search_path: vec![],
        rust_host: String::new(),
    };
    assert_eq!(
        plan(
            AdapterKind::Corepack,
            &frozen,
            &evidence.tool,
            &evidence,
            f.plan.root.parent().unwrap(),
            &host
        )
        .unwrap_err(),
        "invalid_distribution_integrity"
    );
}
