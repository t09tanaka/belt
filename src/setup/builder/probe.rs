//! Read-only worker-local preparation probe and its ownership boundary tests.
use super::*;

impl Setups {
    pub(super) fn invalidate_probe(inner: &mut Inner, key: &str) {
        if let Some(version) = inner
            .probe_versions
            .get(key)
            .and_then(std::sync::Weak::upgrade)
        {
            let next = version.load(Ordering::SeqCst).saturating_add(1);
            version.store(next, Ordering::SeqCst);
        }
    }

    /// Read-only identity/status probe. All filesystem work occurs outside `inner`.
    pub(crate) fn probe_preparation(
        &self,
        input: crate::setup::preparation_probe::PreparationProbeInput<'_>,
        profile: Option<&crate::toolchain::profile::Profile>,
        fingerprint: Option<&Digest>,
        registry: Option<&crate::toolchain::registry::Registry>,
    ) -> crate::setup::preparation_probe::PreparationProbe {
        self.probe_preparation_checked(input, profile, fingerprint, registry, || {})
    }

    fn probe_preparation_checked(
        &self,
        input: crate::setup::preparation_probe::PreparationProbeInput<'_>,
        profile: Option<&crate::toolchain::profile::Profile>,
        fingerprint: Option<&Digest>,
        registry: Option<&crate::toolchain::registry::Registry>,
        io_gate: impl FnOnce(),
    ) -> crate::setup::preparation_probe::PreparationProbe {
        use crate::setup::preparation_probe::{
            PreparationProbe as Probe, PreparationProbeEntry as Entry,
            PreparationProbeState as State, ProbeUnknown as Unknown,
        };
        let Ok(_probe_guard) = self.probe_gate.try_lock() else {
            return Probe::Unknown(Unknown::BudgetExceeded);
        };
        if let Err(e) = crate::setup::preparation_probe::input_budget(&input) {
            return Probe::Unknown(e);
        }
        if input
            .setup
            .as_ref()
            .is_some_and(|s| policy::validate(s).is_err())
            || input
                .frozen
                .as_ref()
                .is_some_and(|b| b.validate(input.manifest_digest).is_err())
        {
            return Probe::Unknown(Unknown::InvalidBinding);
        }
        let began = platform::monotonic_ms();
        let workspace = !input.in_place
            && input
                .setup
                .as_ref()
                .is_some_and(|s| !policy::hoisted(s).is_empty());
        let tool_count = input.frozen.as_ref().map_or(0, |b| b.evidence.len());
        if !workspace && tool_count == 0 {
            // A frozen requirement without matching evidence is not an absence proof.
            if input
                .frozen
                .as_ref()
                .is_some_and(|b| !b.frozen.requirements.is_empty())
            {
                return Probe::Unknown(Unknown::InvalidBinding);
            }
            return Probe::NoManagedPreparation;
        }
        type ResolvedProbeEntry = (PreparationKind, Option<String>, Option<Unknown>);
        let byte_budget = adapters::IdentityReadBudget::probe();
        // Real plans are retained only in the first pass; the second must derive
        // exactly the same keys from the same normal profile sample.
        let verified = std::cell::RefCell::new(Vec::<adapters::VerifiedInstallation<'_>>::new());
        let proofs = std::cell::RefCell::new(BTreeMap::<String, Digest>::new());
        let first_pass = std::cell::Cell::new(true);
        let verification_started = std::cell::Cell::new(None);
        let resolve = || -> Result<(Vec<ResolvedProbeEntry>, crate::setup::preparation_probe::ProfileSample), Unknown> {
            let profile = profile.ok_or(Unknown::ProfileUnavailable)?;
            let fingerprint = fingerprint.ok_or(Unknown::ProfileUnavailable)?;
            if self.cfg.profile_fingerprint.as_ref() != Some(fingerprint)
                || self.cfg.profile_definition.as_ref() != Some(profile.definition_digest())
            {
                return Err(Unknown::InvalidBinding);
            }
            let (snapshot, sample) = profile
                .preparation_snapshot(fingerprint)
                .ok_or(Unknown::ProfileUnavailable)?;
            if profile.required_tools().iter().any(|tool| {
                !matches!(
                    snapshot.tools.get(tool),
                    Some(crate::toolchain::capability::ToolObservation::Available { .. })
                )
            }) {
                return Err(Unknown::ProfileUnavailable);
            }
            let mut entries = Vec::new();
            if first_pass.get() && input.frozen.is_some() {
                verification_started.set(Some(platform::monotonic_ms()));
            }
            if let Some(bundle) = input.frozen {
                crate::toolchain::contract::check_baselines(&bundle.frozen, Some(&snapshot))
                    .map_err(|_| Unknown::ProfileUnavailable)?;
                for evidence in &bundle.evidence {
                    let registry = registry.ok_or(Unknown::MissingContext)?;
                    let (request, plan) = registry
                        .probe_plan_request(&bundle.frozen, &evidence.tool, evidence, &byte_budget)
                        .map_err(|_| Unknown::IoUnavailable)?;
                    if self.cfg.profile_definition.as_ref() != Some(&request.profile_definition)
                        || self.cfg.profile_fingerprint.as_ref() != Some(&request.base_profile)
                    {
                        return Err(Unknown::InvalidBinding);
                    }
                    if first_pass.get() {
                        if let Ok(installation) = adapters::inspect_installation(plan, &byte_budget) {
                            proofs.borrow_mut().insert(format!("tool:{}", request.expected_plan), installation.digest());
                            verified.borrow_mut().push(installation);
                        }
                    }
                    // expected_plan was produced by the shared actual adapter plan path.
                    entries.push((
                        PreparationKind::ToolInstall,
                        Some(format!("tool:{}", request.expected_plan)),
                        None,
                    ));
                }
            }
            if workspace {
                if input.frozen.is_some() {
                    let bundle = input.frozen.as_ref().expect("frozen checked");
                    let activation = adapters::prospective_activation(
                        &bundle.frozen, &verified.borrow(), fingerprint.clone(),
                        &crate::toolchain::contract::observations(&snapshot), profile.environment(),
                        Digest::of_bytes(&serde_json::to_vec(&input.setup).map_err(|_| Unknown::InvalidBinding)?),
                        profile.shell(), &byte_budget,
                    );
                    if let Ok(activation) = activation {
                        let spec = input.setup.as_ref().expect("workspace checked");
                        crate::setup::preparation_probe::environment_budget(spec, &activation.environment)?;
                        let ctx = workspace_key_context(spec, input.manifest_digest, input.run_id,
                            activation.execution_fingerprint.to_string(), activation.environment);
                        entries.push((PreparationKind::WorkspaceSetup, Some(policy::template_key(spec, &ctx)), None));
                    } else {
                        entries.push((PreparationKind::WorkspaceSetup, None, Some(Unknown::ActivationUnverified)));
                    }
                } else {
                    let spec = input.setup.as_ref().expect("workspace checked");
                    crate::setup::preparation_probe::environment_budget(spec, &self.cfg.env)?;
                    let ctx = workspace_key_context(
                        spec,
                        input.manifest_digest,
                        input.run_id,
                        self.cfg.profile.clone(),
                        self.cfg.env.clone(),
                    );
                    entries.push((
                        PreparationKind::WorkspaceSetup,
                        Some(policy::template_key(spec, &ctx)),
                        None,
                    ));
                }
            }
            Ok((entries, sample))
        };
        let (first, sample) = match resolve() {
            Ok(v) => v,
            Err(e) => return Probe::Unknown(e),
        };
        // Only live probes retain revisions. Prune dead weak entries on every probe.
        // A conservative cap keeps concurrent callers from growing the owner map.
        let watches = {
            let mut inner = lock(&self.inner);
            inner.probe_versions.retain(|_, v| v.strong_count() > 0);
            if inner.probe_versions.len().saturating_add(first.len()) > 256 {
                return Probe::Unknown(Unknown::BudgetExceeded);
            }
            first
                .iter()
                .filter_map(|(_, key, _)| key.as_ref())
                .map(|key| {
                    let revision = inner
                        .probe_versions
                        .get(key)
                        .and_then(std::sync::Weak::upgrade)
                        .unwrap_or_else(|| {
                            let v = Arc::new(AtomicU64::new(0));
                            inner.probe_versions.insert(key.clone(), Arc::downgrade(&v));
                            v
                        });
                    let value = revision.load(Ordering::SeqCst);
                    (key.clone(), revision, value)
                })
                .collect::<Vec<_>>()
        };
        io_gate();
        first_pass.set(false);
        let (second, second_sample) = match resolve() {
            Ok(v) => v,
            Err(e) => return Probe::Unknown(e),
        };
        if first != second || sample != second_sample {
            return Probe::Unknown(Unknown::OwnershipChanged);
        }
        if byte_budget.finish().is_err() {
            return Probe::Unknown(Unknown::OwnershipChanged);
        }
        // Native boot discovery may read procfs on Linux; keep it outside ownership.
        let final_boot = platform::boot_id();
        let checked_at = platform::monotonic_ms();
        let inner = lock(&self.inner);
        if watches
            .iter()
            .any(|(_, version, old)| *old == u64::MAX || version.load(Ordering::SeqCst) != *old)
        {
            return Probe::Unknown(Unknown::OwnershipChanged);
        }
        let entries = first
            .into_iter()
            .map(|(kind, key, unknown)| {
                let state = if let Some(e) = unknown {
                    State::Unknown(e)
                } else if inner.unknown_builder {
                    State::Unknown(Unknown::OwnershipChanged)
                } else if key
                    .as_ref()
                    .is_some_and(|key| inner.barriers.contains_key(key))
                {
                    State::Unknown(Unknown::Barrier)
                } else if let Some(id) = key.as_ref().and_then(|key| inner.by_key.get(key)) {
                    match inner.flights.get(id) {
                        Some(f)
                            if key.as_deref() == Some(f.key.as_str())
                                && !f.stop
                                && f.end.is_none()
                                && !f.refs.is_empty()
                                && matches!(
                                    (kind, f.kind),
                                    (PreparationKind::ToolInstall, BuilderKind::ToolInstall)
                                        | (
                                            PreparationKind::WorkspaceSetup,
                                            BuilderKind::WorkspaceSetup
                                        )
                                ) =>
                        {
                            State::Active {
                                builder_id: id.clone(),
                                builder_attempt_id: f.attempt_id.clone(),
                            }
                        }
                        _ => State::Unknown(Unknown::OwnershipChanged),
                    }
                } else if let Some(proof) =
                    key.as_ref().and_then(|k| proofs.borrow().get(k).cloned())
                {
                    State::VerifiedAvailable { proof }
                } else {
                    State::ConcretePlan
                };
                Entry {
                    kind,
                    key: key.map(|key| Digest::of_bytes(key.as_bytes())),
                    state,
                }
            })
            .collect();
        let observed = platform::monotonic_ms();
        let provenance = crate::setup::preparation_probe::ProbeProvenance {
            profile: sample,
            observed_monotonic_ms: observed,
            observation_ttl_ms: 5000,
            verification_started_monotonic_ms: verification_started.get(),
        };
        if observed < began
            || observed < checked_at
            || final_boot.as_deref() != Some(provenance.profile.native_boot.as_str())
            || provenance.deadline(0, 0).is_none()
        {
            return Probe::Unknown(Unknown::ProfileUnavailable);
        }
        Probe::Entries {
            provenance,
            owner_generation: self.owner_generation.clone(),
            entries,
        }
    }
}

#[cfg(test)]
mod preparation_probe_tests {
    use super::*;
    use crate::setup::preparation_probe::{
        PreparationProbe as Probe, PreparationProbeInput as Input, PreparationProbeState as State,
        ProbeUnknown as Unknown,
    };
    use crate::toolchain::profile::Profile;

    struct NoWorkspace;
    impl BuilderWorkspaces for NoWorkspace {
        fn create(&self, _: &AttemptId, _: &AttemptPayload) -> Result<Workspace, ApiError> {
            panic!("probe started workspace")
        }
        fn remove(&self, _: &Workspace) -> Result<(), ApiError> {
            panic!("probe removed workspace")
        }
    }
    struct Fixture {
        _temp: tempfile::TempDir,
        owner: Arc<Setups>,
        profile: Profile,
        fingerprint: Digest,
        manifest: Digest,
        source: crate::ids::SourceId,
        run: RunId,
        setup: Option<SetupSpec>,
        frozen: Option<Box<crate::toolchain::contract::FrozenBundle>>,
    }
    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = std::fs::canonicalize(temp.path()).unwrap();
            // A real fixed shell path, but no tool executable to discover/probe.
            let shell = root.join("sh");
            std::fs::copy("/bin/sh", &shell).unwrap();
            let env = BTreeMap::from([
                ("PATH".into(), root.to_str().unwrap().into()),
                ("SHELL".into(), shell.to_str().unwrap().into()),
                ("HOME".into(), root.to_str().unwrap().into()),
            ]);
            let profile = Profile::load(&root, &env).unwrap();
            let fingerprint = profile.report().profile_fingerprint;
            let mut cfg = SetupConfig::new(
                root.join("state"),
                crate::ids::WorkerId::parse("w-probe").unwrap(),
                root.join("never-supervise"),
            );
            cfg.profile_fingerprint = Some(fingerprint.clone());
            cfg.profile_definition = Some(profile.definition_digest().clone());
            cfg.profile = fingerprint.to_string();
            cfg.env = profile.environment().clone();
            let owner = Setups::open(cfg, Arc::new(NoWorkspace)).unwrap();
            let setup = Some(serde_json::from_value(serde_json::json!({"steps":[{"id":"prepare", "run":"private-script", "env":{"PRIVATE_VALUE":"private-value"}}], "scope":"run"})).unwrap());
            Self {
                _temp: temp,
                owner,
                profile,
                fingerprint,
                manifest: Digest::of_bytes(b"source"),
                source: crate::ids::SourceId::generate(),
                run: RunId::generate(),
                setup,
                frozen: None,
            }
        }
        fn input(&self) -> Input<'_> {
            Input {
                manifest_digest: &self.manifest,
                source_id: &self.source,
                run_id: &self.run,
                setup: &self.setup,
                frozen: &self.frozen,
                in_place: false,
            }
        }
        fn probe(&self) -> Probe {
            self.owner.probe_preparation(
                self.input(),
                Some(&self.profile),
                Some(&self.fingerprint),
                None,
            )
        }
        fn key(&self) -> String {
            let spec = self.setup.as_ref().unwrap();
            policy::template_key(
                spec,
                &workspace_key_context(
                    spec,
                    &self.manifest,
                    &self.run,
                    self.owner.cfg.profile.clone(),
                    self.owner.cfg.env.clone(),
                ),
            )
        }
        fn seed(&self, key: &str) -> (BuilderId, AttemptId) {
            let id = BuilderId::generate();
            let attempt = AttemptId::generate();
            let mut inner = lock(&self.owner.inner);
            inner.by_key.insert(key.into(), id.clone());
            inner.flights.insert(
                id.clone(),
                Flight {
                    key: key.into(),
                    attempt_id: attempt.clone(),
                    refs: BTreeSet::from(["consumer".into()]),
                    end: None,
                    kind: BuilderKind::WorkspaceSetup,
                    stop: false,
                },
            );
            (id, attempt)
        }
    }
    fn entry(probe: Probe) -> crate::setup::preparation_probe::PreparationProbeEntry {
        match probe {
            Probe::Entries { mut entries, .. } => {
                assert_eq!(entries.len(), 1);
                entries.remove(0)
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    #[test]
    fn preparation_probe_rejects_report_sample_change_between_resolutions() {
        let f = Fixture::new();
        let result = f.owner.probe_preparation_checked(
            f.input(),
            Some(&f.profile),
            Some(&f.fingerprint),
            None,
            || {
                f.profile.report();
            },
        );
        assert!(matches!(result, Probe::Unknown(Unknown::OwnershipChanged)));
        let result = f.probe();
        let Probe::Entries { provenance, .. } = result else {
            panic!("expected identity");
        };
        let (_, sample) = f.profile.preparation_snapshot(&f.fingerprint).unwrap();
        assert_eq!(provenance.profile, sample);
        assert!(provenance.observed_monotonic_ms >= sample.completed_monotonic_ms);
    }
    #[test]
    fn preparation_probe_shared_producer_key_and_private_read_only_output() {
        let mut f = Fixture::new();
        let before = f.key();
        assert_eq!(
            entry(f.probe()).key,
            Some(Digest::of_bytes(before.as_bytes()))
        );
        assert_eq!(entry(f.probe()).state, State::ConcretePlan);
        let output = format!("{:?}", f.probe());
        for private in [
            "private-script",
            "PRIVATE_VALUE",
            "private-value",
            f.owner.cfg.state_dir.to_str().unwrap(),
        ] {
            assert!(!output.contains(private));
        }
        assert!(!f.owner.cfg.state_dir.join("toolchains").exists());
        assert_eq!(f.owner.started.load(Ordering::SeqCst), 0);
        assert!(lock(&f.owner.inner).consumers.is_empty());
        assert!(lock(&f.owner.inner).flights.is_empty());
        f.run = RunId::generate();
        assert_ne!(f.key(), before);
        let run_key = f.key();
        f.manifest = Digest::of_bytes(b"other-source");
        assert_ne!(f.key(), run_key);
        let key = f.key();
        f.setup.as_mut().unwrap().steps[0]
            .env
            .insert("PRIVATE_VALUE".into(), "changed".into());
        assert_ne!(f.key(), key);
        let spec = f.setup.as_ref().unwrap();
        let original = workspace_key_context(
            spec,
            &f.manifest,
            &f.run,
            f.owner.cfg.profile.clone(),
            f.owner.cfg.env.clone(),
        );
        let mut context = original.clone();
        context.env.insert("OTHER".into(), "changed".into());
        assert_ne!(
            policy::template_key(spec, &context),
            policy::template_key(spec, &original)
        );
        context = original.clone();
        context.profile = "changed".into();
        assert_ne!(
            policy::template_key(spec, &context),
            policy::template_key(spec, &original)
        );
        let mut secret = spec.clone();
        secret.secrets.push(crate::wire::SecretVersion {
            name: "secret".into(),
            digest: "hmac-a".into(),
        });
        let a = policy::template_key(&secret, &original);
        secret.secrets[0].digest = "hmac-b".into();
        assert_ne!(a, policy::template_key(&secret, &original));
        // Secret-bearing setup is not hoisted by the actual producer either.
        f.setup = Some(secret);
        assert_eq!(f.probe(), Probe::NoManagedPreparation);
    }
    #[test]
    fn preparation_probe_active_requires_current_exact_live_flight() {
        let f = Fixture::new();
        let key = f.key();
        let (id, attempt) = f.seed(&key);
        assert_eq!(
            entry(f.probe()).state,
            State::Active {
                builder_id: id.clone(),
                builder_attempt_id: attempt.clone()
            }
        );
        for fault in 0..6 {
            {
                let mut inner = lock(&f.owner.inner);
                match fault {
                    0 => {
                        inner.barriers.insert(key.clone(), attempt.clone());
                    }
                    1 => inner.unknown_builder = true,
                    2 => inner.flights.get_mut(&id).unwrap().stop = true,
                    3 => {
                        inner.flights.get_mut(&id).unwrap().end =
                            Some(BuilderResult::Workspace(BuildEnd::Crashed("x".into())))
                    }
                    4 => inner.flights.get_mut(&id).unwrap().key = "other".into(),
                    _ => {
                        inner.by_key.insert(key.clone(), BuilderId::generate());
                    }
                }
            }
            assert!(matches!(entry(f.probe()).state, State::Unknown(_)));
            let mut inner = lock(&f.owner.inner);
            inner.barriers.clear();
            inner.unknown_builder = false;
            let flight = inner.flights.get_mut(&id).unwrap();
            flight.stop = false;
            flight.end = None;
            flight.key = key.clone();
            inner.by_key.insert(key.clone(), id.clone());
        }
    }
    #[test]
    fn preparation_probe_io_gate_does_not_hold_owner_lock_and_writer_invalidates() {
        let f = Fixture::new();
        let key = f.key();
        let (id, _) = f.seed(&key);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let f = &f;
            let probe = scope.spawn(move || {
                f.owner.probe_preparation_checked(
                    f.input(),
                    Some(&f.profile),
                    Some(&f.fingerprint),
                    None,
                    || {
                        entered_tx.send(()).unwrap();
                        resume_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                    },
                )
            });
            entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(f.owner.wanted(&id), (1, false));
            // Renewal and unrelated ownership/release can progress while IO stalls.
            assert!(policy::renew_lease(f.owner.wanted(&id).0, false));
            let unrelated = AttemptId::generate();
            let (other_id, _) = f.seed("unrelated-key");
            {
                let mut inner = lock(&f.owner.inner);
                inner.flights.get_mut(&other_id).unwrap().refs =
                    BTreeSet::from([unrelated.to_string()]);
                inner.consumers.insert(
                    unrelated.to_string(),
                    Consumer {
                        builder: Some(other_id.clone()),
                        ..Consumer::default()
                    },
                );
            }
            f.owner.release(&unrelated);
            assert_eq!(f.owner.wanted(&other_id), (0, true));
            f.owner.finish(
                &id,
                BuilderResult::Workspace(BuildEnd::Crashed("test".into())),
            );
            resume_tx.send(()).unwrap();
            assert_eq!(
                probe.join().unwrap(),
                Probe::Unknown(Unknown::OwnershipChanged)
            );
        });
    }
    #[test]
    fn preparation_probe_complete_absence_budget_profile_and_unverified_frozen() {
        let mut f = Fixture::new();
        assert_eq!(
            f.owner
                .probe_preparation(f.input(), None, Some(&f.fingerprint), None),
            Probe::Unknown(Unknown::ProfileUnavailable)
        );
        assert_eq!(
            f.owner.probe_preparation(
                f.input(),
                Some(&f.profile),
                Some(&Digest::of_bytes(b"wrong")),
                None
            ),
            Probe::Unknown(Unknown::InvalidBinding)
        );
        let frozen = crate::toolchain::versions::FrozenToolchains::freeze(
            f.manifest.clone(),
            f.profile.definition_digest().clone(),
            crate::toolchain::versions::Scope::project(""),
            &[],
            &[],
            &BTreeMap::new(),
            crate::toolchain::versions::MatchPolicy::Exact,
        )
        .unwrap();
        f.frozen = Some(Box::new(crate::toolchain::contract::FrozenBundle {
            schema_version: 1,
            frozen,
            evidence: vec![],
        }));
        // An empty complete frozen set can now compose its exact shell-bound
        // activation; the workspace is still identity-only, never available.
        assert_eq!(entry(f.probe()).state, State::ConcretePlan);
        f.frozen = None;
        f.setup.as_mut().unwrap().steps[0].run = "x".repeat(300 * 1024);
        assert_eq!(f.probe(), Probe::Unknown(Unknown::BudgetExceeded));
        f.setup = None;
        assert_eq!(f.probe(), Probe::NoManagedPreparation);
    }
    #[test]
    fn preparation_probe_output_budget_counts_workspace_with_tool_entries() {
        let mut f = Fixture::new();
        let frozen = crate::toolchain::versions::FrozenToolchains::freeze(
            f.manifest.clone(),
            f.profile.definition_digest().clone(),
            crate::toolchain::versions::Scope::project(""),
            &[],
            &[],
            &BTreeMap::new(),
            crate::toolchain::versions::MatchPolicy::Exact,
        )
        .unwrap();
        // Count validation precedes semantic contract validation and any IO.
        let evidence = (0..16)
            .map(|i| crate::toolchain::adapters::ResolutionEvidence {
                contract: frozen.digest(),
                tool: crate::toolchain::versions::ToolId::parse(&format!("tool{i}")).unwrap(),
                concrete: node_semver::Version::parse("1.2.3").unwrap(),
                catalog: Digest::of_bytes(b"catalog"),
                resolved_inputs: BTreeMap::new(),
            })
            .collect();
        f.frozen = Some(Box::new(crate::toolchain::contract::FrozenBundle {
            schema_version: 1,
            frozen,
            evidence,
        }));
        assert_eq!(
            crate::setup::preparation_probe::input_budget(&f.input()),
            Err(Unknown::BudgetExceeded)
        );
        assert_eq!(f.probe(), Probe::Unknown(Unknown::BudgetExceeded));
        let mut in_place = f.input();
        in_place.in_place = true;
        assert_eq!(
            crate::setup::preparation_probe::input_budget(&in_place),
            Ok(())
        ); // 16 tools, no managed workspace
        f.frozen.as_mut().unwrap().evidence.pop();
        assert_eq!(
            crate::setup::preparation_probe::input_budget(&f.input()),
            Ok(())
        ); // 15 + 1 = 16
        f.setup = None;
        assert_eq!(
            crate::setup::preparation_probe::input_budget(&f.input()),
            Ok(())
        ); // 15 tools alone
    }
    struct SmallTool {
        plan: adapters::InstallPlan,
    }
    impl adapters::AdmittedExecutor for SmallTool {
        fn execute(
            &mut self,
            command: &adapters::CommandSpec,
        ) -> Result<adapters::CommandOutput, &'static str> {
            for (_, probe) in self.plan.probes() {
                std::fs::create_dir_all(probe.program.parent().unwrap()).unwrap();
                std::fs::write(&probe.program, b"tiny-complete-tool").unwrap();
                std::fs::set_permissions(&probe.program, std::fs::Permissions::from_mode(0o700))
                    .unwrap();
            }
            let name = self
                .plan
                .probes()
                .iter()
                .find(|(_, p)| p == command)
                .map(|(n, _)| n.as_str());
            let output = match name {
                Some("rustc") => "rustc 1.2.3 (fixture)",
                Some("cargo") => "cargo 1.2.3 (fixture)",
                _ => "",
            };
            Ok(adapters::CommandOutput {
                success: true,
                stdout: output.as_bytes().to_vec(),
            })
        }
    }
    fn small_install(
        f: &mut Fixture,
        manager: &std::path::Path,
    ) -> (
        crate::toolchain::registry::Registry,
        adapters::InstallPlan,
        adapters::InstallationReceipt,
    ) {
        use crate::toolchain::versions::*;
        let state = &f.owner.cfg.state_dir;
        private_dir(&state.join("toolchains")).unwrap();
        let config = serde_json::json!({"schema_version":1,"active_profile":f.profile.name(),"profile_definition":f.profile.definition_digest(),"recipes":[{"tool":"rust","kind":"Rustup","manager":manager,"dependencies":[],"search_path":[manager.parent().unwrap()],"rust_host":"fixture-host","inventory":["1.2.3"],"resolved_inputs":{}}]});
        let path = state.join("toolchains.toml");
        std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let scope = Scope::project("");
        let frozen = FrozenToolchains::freeze(
            f.manifest.clone(),
            f.profile.definition_digest().clone(),
            scope.clone(),
            &[Declaration {
                tool: ToolId::parse("rust").unwrap(),
                constraint: Constraint::parse("1.2.3").unwrap(),
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
        .unwrap();
        let bundle = crate::toolchain::contract::FrozenBundle::seal_exact(frozen).unwrap();
        let registry =
            crate::toolchain::registry::Registry::load(state, &f.profile, f.fingerprint.clone())
                .unwrap();
        let evidence = &bundle.evidence[0];
        let request = registry
            .request(&bundle.frozen, &evidence.tool, Some(evidence))
            .unwrap();
        let plan = request.plan(&request.private_root).unwrap();
        let receipt =
            adapters::install_admitted(&plan, &mut SmallTool { plan: plan.clone() }).unwrap();
        f.frozen = Some(Box::new(bundle));
        // Installation writes preceded the normal report sample; a query must
        // never refresh that sample itself.
        assert_eq!(f.profile.report().profile_fingerprint, f.fingerprint);
        (registry, plan, receipt)
    }
    #[test]
    fn preparation_probe_small_complete_tool_and_exact_frozen_workspace_key() {
        let mut f = Fixture::new();
        let manager = std::fs::canonicalize(f._temp.path())
            .unwrap()
            .join("manager");
        std::fs::write(&manager, b"private-manager-never-executed").unwrap();
        std::fs::set_permissions(&manager, std::fs::Permissions::from_mode(0o700)).unwrap();
        let (registry, plan, receipt) = small_install(&mut f, &manager);
        let probe = f.owner.probe_preparation(
            f.input(),
            Some(&f.profile),
            Some(&f.fingerprint),
            Some(&registry),
        );
        let Probe::Entries {
            entries,
            provenance,
            ..
        } = probe
        else {
            panic!("expected bounded sampled entries");
        };
        assert_eq!(entries.len(), 2);
        assert!(matches!(entries[0].state, State::VerifiedAvailable { .. }));
        assert_eq!(entries[1].state, State::ConcretePlan);
        assert!(provenance.verification_started_monotonic_ms.is_some());
        let (snapshot, _) = f.profile.preparation_snapshot(&f.fingerprint).unwrap();
        let activation = adapters::activate_set(
            &f.frozen.as_ref().unwrap().frozen,
            &[(&plan, &receipt)],
            f.fingerprint.clone(),
            &crate::toolchain::contract::observations(&snapshot),
            f.profile.environment(),
            Digest::of_bytes(&serde_json::to_vec(&f.setup).unwrap()),
            &adapters::FileIdentity::capture(f.profile.shell()).unwrap(),
        )
        .unwrap();
        let expected = policy::template_key(
            f.setup.as_ref().unwrap(),
            &workspace_key_context(
                f.setup.as_ref().unwrap(),
                &f.manifest,
                &f.run,
                activation.execution_fingerprint.to_string(),
                activation.environment,
            ),
        );
        assert_eq!(entries[1].key, Some(Digest::of_bytes(expected.as_bytes())));
        // Real mutation is rejected even at identical version/length.
        let executable = &plan.probes()[0].1.program;
        let size = std::fs::metadata(executable).unwrap().len() as usize;
        std::fs::write(executable, vec![b'z'; size]).unwrap();
        let changed = f.owner.probe_preparation(
            f.input(),
            Some(&f.profile),
            Some(&f.fingerprint),
            Some(&registry),
        );
        let Probe::Entries { entries, .. } = changed else {
            panic!("expected conservative entries");
        };
        assert_eq!(entries[0].state, State::ConcretePlan);
        assert_eq!(
            entries[1].state,
            State::Unknown(Unknown::ActivationUnverified)
        );
    }
    #[test]
    fn preparation_probe_global_gate_rejects_second_query_while_io_is_paused() {
        let f = Fixture::new();
        let first = f.owner.probe_preparation_checked(
            f.input(),
            Some(&f.profile),
            Some(&f.fingerprint),
            None,
            || {
                // The first query owns the gate while outside the ownership mutex.
                assert!(f.owner.inner.try_lock().is_ok());
                assert_eq!(f.probe(), Probe::Unknown(Unknown::BudgetExceeded));
            },
        );
        assert!(matches!(first, Probe::Entries { .. }));
        assert!(matches!(f.probe(), Probe::Entries { .. }));
    }
    #[test]
    #[ignore = "explicit read-only manager calibration; requires owned fixture and manager path"]
    fn preparation_probe_calibration_real_manager_six_samples() {
        let manager = std::fs::canonicalize(
            std::env::var_os("RTR_PROBE_CALIBRATION_MANAGER").expect("explicit manager required"),
        )
        .unwrap();
        let mut f = Fixture::new();
        let (registry, _, _) = small_install(&mut f, &manager);
        let manager_size = std::fs::metadata(&manager).unwrap().len();
        let mut samples = vec![];
        for i in 0..6 {
            // Normal report is a separate lifecycle action; the queried profile
            // contains only the owned shell, never the real manager on PATH.
            assert_eq!(f.profile.report().profile_fingerprint, f.fingerprint);
            let started = platform::monotonic_ms();
            let probe = f.owner.probe_preparation(
                f.input(),
                Some(&f.profile),
                Some(&f.fingerprint),
                Some(&registry),
            );
            let ended = platform::monotonic_ms();
            let (bytes, reads, paths, visits) = adapters::probe_statistics();
            let Probe::Entries {
                provenance,
                entries,
                ..
            } = probe
            else {
                panic!("calibration query was unknown");
            };
            assert!(matches!(entries[0].state, State::VerifiedAvailable { .. }));
            let start = provenance.verification_started_monotonic_ms.unwrap();
            let profile_age_entry = started - provenance.profile.completed_monotonic_ms;
            let profile_age_exit = ended - provenance.profile.completed_monotonic_ms;
            assert!(profile_age_exit < 5000);
            samples.push(serde_json::json!({"sample":i,"manager_bytes":manager_size,"charged_bytes_both_passes":bytes,"read_calls":reads,"held_paths":paths,"visited_entries":visits,"in_process_roundtrip_ms":ended-started,"profile_age_entry_ms":profile_age_entry,"profile_age_exit_ms":profile_age_exit,"verification_age_ms":ended-start,"unknown":false}));
        }
        println!(
            "PROBE_CALIBRATION {}",
            serde_json::to_string(&samples).unwrap()
        );
    }
    #[test]
    fn preparation_probe_expired_io_keeps_owner_until_real_drain_and_releases_gate() {
        let mut fixture = Fixture::new();
        let manager = std::fs::canonicalize(fixture._temp.path())
            .unwrap()
            .join("manager");
        std::fs::write(&manager, b"private-manager-never-executed").unwrap();
        std::fs::set_permissions(&manager, std::fs::Permissions::from_mode(0o700)).unwrap();
        let (registry, _, _) = small_install(&mut fixture, &manager);
        let f = Arc::new(fixture);
        let weak = Arc::downgrade(&f.owner);
        let (entered, waiting) = std::sync::mpsc::sync_channel(0);
        let (release, resume) = std::sync::mpsc::sync_channel(0);
        let child = f.clone();
        let task = std::thread::spawn(move || {
            let probe = child.owner.probe_preparation_checked(
                child.input(),
                Some(&child.profile),
                Some(&child.fingerprint),
                Some(&registry),
                || {
                    entered.send(()).unwrap();
                    resume.recv().unwrap();
                },
            );
            let released = child.owner.probe_gate.try_lock().is_ok();
            let retained = adapters::probe_statistics().2;
            (probe, released, retained)
        });
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(f.owner.inner.try_lock().is_ok());
        assert_eq!(f.probe(), Probe::Unknown(Unknown::BudgetExceeded));
        // One real elapsed interval crosses the immutable profile deadline while
        // the IO phase is paused. Dropping the caller does not detach another
        // verifier or release ownership beneath this still-running query.
        std::thread::sleep(Duration::from_millis(5010));
        drop(f);
        assert!(weak.upgrade().is_some());
        release.send(()).unwrap();
        let (probe, released, retained) = task.join().unwrap();
        assert_eq!(probe, Probe::Unknown(Unknown::ProfileUnavailable));
        assert!(released);
        assert!(
            retained > 0,
            "paused query retained actual artifact descriptor identities"
        );
        assert!(weak.upgrade().is_none());
    }
}
