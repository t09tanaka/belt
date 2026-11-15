use super::declarations::*;
use super::versions::*;
use super::*;
use std::collections::BTreeMap;
fn loc() -> Location {
    Location {
        path: ".node-version".into(),
        first_line: 1,
        last_line: 1,
    }
}
fn decl(raw: &str) -> Declaration {
    Declaration {
        tool: ToolId::parse("node").unwrap(),
        constraint: Constraint::parse(raw).unwrap(),
        scope: Scope::project(""),
        location: loc(),
    }
}
fn docs(items: &[(&str, &str)]) -> BTreeMap<String, Document> {
    items
        .iter()
        .map(|(p, t)| (p.to_string(), Document::new(p, t.as_bytes()).unwrap()))
        .collect()
}
#[test]
fn all_version_documents_and_source_scopes() {
    for (path, text, tool) in [
        (".node-version", "22.1.0\n", "node"),
        (".nvmrc", "# frozen\nv22.1.0\n", "node"),
        (
            "package.json",
            r#"{"engines":{"node":">=20 <23","npm":"^10"},"packageManager":"pnpm@9.1.0"}"#,
            "pnpm",
        ),
        (
            ".tool-versions",
            "# versions\nnodejs 22.1.0 22.2.0\nrust 1.82.0\n",
            "node",
        ),
        (
            "mise.toml",
            "[tools]\nnode = { version = \"22\" }\npython = [\"3.11\", \"3.12\"]\n",
            "python",
        ),
        ("rust-toolchain", "stable\n", "rust"),
        (
            "rust-toolchain.toml",
            "[toolchain]\nchannel = \"nightly-2026-01-01\"\n",
            "rust",
        ),
        (
            "rust-toolchain",
            "[toolchain]\nchannel = \"1.82.0\"",
            "rust",
        ),
        (".fvmrc", r#"{"flutter":"3.24.0"}"#, "flutter"),
        (
            ".fvm/fvm_config.json",
            r#"{"flutterSdkVersion":"3.24.0"}"#,
            "flutter",
        ),
        (
            "pubspec.yaml",
            "environment:\n  sdk: '>=3.0.0 <4.0.0'\n  flutter: 'any'\n",
            "dart",
        ),
        (".xcode-version", "16.2\n", "xcode"),
    ] {
        let got = read_project("", &docs(&[(path, text)]), &[]).unwrap();
        assert!(got.iter().any(|d| d.tool.as_str() == tool), "{path}");
        assert!(got
            .iter()
            .all(|d| d.location.path == path && d.location.first_line > 0));
    }
    let got = read_project(
        "app",
        &docs(&[("app/.node-version", "22"), (".node-version", "20")]),
        &[],
    )
    .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].scope.project, "app");
    assert!(read_project("", &BTreeMap::new(), &[]).unwrap().is_empty());
    assert!(read_project(
        "",
        &docs(&[
            ("package.json", "{}"),
            ("mise.toml", "[settings]"),
            ("pubspec.yaml", "name: app")
        ]),
        &[]
    )
    .unwrap()
    .is_empty());
}
#[test]
fn workflows_keep_jobs_steps_and_unresolved_matrices_separate() {
    let workflow="jobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '20'\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '22'\n  b:\n    runs-on: x\n    steps:\n      - run: ignored\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '${{ matrix.node }}'\n";
    let got = read_project(
        "",
        &docs(&[(".github/workflows/ci.yml", workflow)]),
        &[".github/workflows/ci.yml".into()],
    )
    .unwrap();
    assert_eq!(got.len(), 3);
    assert_ne!(got[0].scope, got[1].scope);
    assert_ne!(got[1].scope.job, got[2].scope.job);
    assert!(matches!(&got[2].constraint, Constraint::Expression(_)));
    for (action, key, value, tool) in [
        (
            "actions/setup-python@v5",
            "python-version",
            "3.12",
            "python",
        ),
        ("actions/setup-go@v5", "go-version", "1.23", "go"),
        ("actions/setup-java@v4", "java-version", "21", "java"),
        ("ruby/setup-ruby@v1", "ruby-version", "3.3", "ruby"),
        (
            "subosito/flutter-action@v2",
            "flutter-version",
            "3.24",
            "flutter",
        ),
        ("dart-lang/setup-dart@v1", "sdk", "stable", "dart"),
        (
            "maxim-lobanov/setup-xcode@v1",
            "xcode-version",
            "16.2",
            "xcode",
        ),
        (
            "actions-rust-lang/setup-rust-toolchain@v1",
            "toolchain",
            "stable",
            "rust",
        ),
        ("dtolnay/rust-toolchain@stable", "irrelevant", "x", "rust"),
        ("pnpm/action-setup@v4", "version", "9", "pnpm"),
    ] {
        let wf=format!("jobs:\n  test:\n    steps:\n      - uses: {action}\n        with:\n          {key}: '{value}'\n");
        let got = read_project("", &docs(&[("ci.yml", &wf)]), &["ci.yml".into()]).unwrap();
        assert_eq!(got[0].tool.as_str(), tool);
    }
    let wf="jobs:\n  test:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: custom.txt\n      - uses: unknown/setup-thing@v1\n      - run: true\n";
    let got = read_project(
        "app",
        &docs(&[("ci.yml", wf), ("custom.txt", "22")]),
        &["ci.yml".into()],
    )
    .unwrap();
    assert_eq!(got[0].location.path, "custom.txt");
}
#[test]
fn malformed_inputs_fail_without_printing_source_values() {
    for (path, text) in [
        ("package.json", "{ SECRET"),
        ("mise.toml", "[SECRET"),
        ("pubspec.yaml", "[: SECRET"),
        ("package.json", r#"{"engines":1}"#),
        ("package.json", r#"{"engines":{"node":1}}"#),
        ("package.json", r#"{"packageManager":1}"#),
        ("package.json", r#"{"packageManager":"SECRET"}"#),
        ("package.json", r#"{"packageManager":"bad@1"}"#),
        ("mise.toml", "tools = 1"),
        ("mise.toml", "[tools]\nnode = 1"),
        ("mise.toml", "[tools]\nnode = [1]"),
        ("rust-toolchain.toml", "[toolchain]"),
        ("rust-toolchain.toml", "[toolchain]\nchannel = 1"),
        (".fvmrc", "{}"),
        (".fvm/fvm_config.json", r#"{"flutterSdkVersion":1}"#),
        ("pubspec.yaml", "environment: 1"),
        ("pubspec.yaml", "environment:\n  sdk: 1"),
        (".tool-versions", "node"),
        (".node-version", "SECRET"),
    ] {
        let e = read_project("", &docs(&[(path, text)]), &[]).unwrap_err();
        assert!(!format!("{e:?}").contains("SECRET"), "{path}");
    }
    assert!(Document::new("../secret", b"22").is_err());
    assert!(Document::new("file", &[255]).is_err());
    assert!(Document::new("file", &vec![0; MAX_DOCUMENT_BYTES + 1]).is_err());
    for p in ["", "/a", "a//b", "a/./b", "a/../b", "a\\b", "a\nb"] {
        assert!(!safe_path(p));
    }
    assert!(!safe_path(&"a".repeat(1025)));
    assert!(safe_path("a/b"));
    let many = (0..MAX_DOCUMENTS + 1)
        .map(|i| (i.to_string(), Document::new("a", b"").unwrap()))
        .collect();
    assert!(read_project("", &many, &[]).is_err());
    assert!(read_project("", &BTreeMap::new(), &["missing.yml".into()]).is_err());
    for wf in ["[bad", "jobs:\n  a:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: ../bad", "jobs:\n  a:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: absent"] {assert!(read_project("",&docs(&[("ci.yml",wf)]),&["ci.yml".into()]).is_err());}
}
#[test]
fn typed_ranges_conflicts_baselines_and_immutable_retry_contract() {
    assert_eq!(ToolId::parse("nodejs").unwrap().as_str(), "node");
    assert_eq!(ToolId::parse("cargo").unwrap().as_str(), "rust");
    assert_eq!(ToolId::parse("golang").unwrap().as_str(), "go");
    for t in ["", "a/b", &"a".repeat(65)] {
        assert!(ToolId::parse(t).is_err());
    }
    for raw in [
        "",
        &"1".repeat(513),
        "22\n23",
        "${{ broken",
        "not a version",
    ] {
        assert!(Constraint::parse(raw).is_err());
    }
    let have = VersionObservation::parse("22.1.0");
    let old = VersionObservation::parse("20.1.0");
    assert_eq!(
        Constraint::parse(">=20 <23 || ^24").unwrap().matches(&have),
        Match::Match
    );
    assert_eq!(
        Constraint::parse("^20").unwrap().matches(&have),
        Match::Mismatch
    );
    assert_eq!(
        Constraint::parse("stable").unwrap().matches(&have),
        Match::Unknown
    );
    assert_eq!(
        Constraint::parse("22")
            .unwrap()
            .matches(&VersionObservation::Unknown),
        Match::Unknown
    );
    assert_eq!(
        VersionObservation::parse("invalid"),
        VersionObservation::Unknown
    );
    let scope = Scope::project("");
    assert!(effective(&[decl("20"), decl("22")], &scope).is_err());
    assert!(effective(&[decl("stable"), decl("beta")], &scope).is_err());
    assert!(effective(&[decl("^22"), decl(">=22.1")], &scope).is_ok());
    assert!(effective(
        &[decl("${{ matrix.node }}"), decl("stable"), decl("stable")],
        &scope
    )
    .is_ok());
    let tool = ToolId::parse("node").unwrap();
    let observed = [(tool.clone(), have.clone())].into();
    for (policy, value, expected) in [
        (MatchPolicy::Exact, "22.1.0", Match::Match),
        (MatchPolicy::Exact, "22.1.1", Match::Mismatch),
        (MatchPolicy::Minor, "22.1.2", Match::Match),
        (MatchPolicy::Minor, "22.2.0", Match::Mismatch),
        (MatchPolicy::Major, "22.2.0", Match::Match),
        (MatchPolicy::Major, "23.0.0", Match::Mismatch),
        (MatchPolicy::Any, "20.0.0", Match::Match),
    ] {
        assert_eq!(
            policy.compare(&have, &VersionObservation::parse(value)),
            expected
        );
        assert_eq!(
            policy.compare(&VersionObservation::Unknown, &have),
            Match::Unknown
        );
    }
    let frozen = FrozenToolchains::freeze(
        crate::ids::Digest::of_bytes(b"source"),
        crate::ids::Digest::of_bytes(b"profile"),
        scope.clone(),
        &[],
        &[(tool.clone(), loc()), (tool.clone(), loc())],
        &observed,
        MatchPolicy::Exact,
    )
    .unwrap();
    assert_eq!(frozen.baseline.len(), 1);
    assert_eq!(frozen.check(&observed)[&tool], Match::Match);
    let bytes = serde_json::to_vec(&frozen).unwrap();
    let restored: FrozenToolchains = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored.digest(), frozen.digest());
    assert_eq!(
        restored.check(&[(tool.clone(), old)].into())[&tool],
        Match::Mismatch
    );
    assert_eq!(restored.check(&BTreeMap::new())[&tool], Match::Unknown);
    let unknown = FrozenToolchains::freeze(
        frozen.source_manifest.clone(),
        frozen.profile_definition.clone(),
        scope.clone(),
        &[],
        &[(tool.clone(), loc())],
        &BTreeMap::new(),
        MatchPolicy::Any,
    )
    .unwrap();
    assert_eq!(unknown.check(&observed)[&tool], Match::Unknown);
    for (raw, status) in [
        ("^22", Match::Unknown),
        ("^20", Match::Mismatch),
        ("stable", Match::Unknown),
    ] {
        let declared = FrozenToolchains::freeze(
            frozen.source_manifest.clone(),
            frozen.profile_definition.clone(),
            scope.clone(),
            &[decl(raw)],
            &[(tool.clone(), loc())],
            &observed,
            MatchPolicy::Any,
        )
        .unwrap();
        assert!(declared.baseline.is_empty());
        assert_eq!(declared.check(&observed)[&tool], status);
    }
    assert!(FrozenToolchains::freeze(
        frozen.source_manifest,
        frozen.profile_definition,
        scope,
        &[decl("20"), decl("22")],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact
    )
    .is_err());
}

#[test]
fn explicit_resolution_and_distribution_integrity_are_frozen() {
    let mut d = decl("stable");
    let tool = d.tool.clone();
    let authority = crate::ids::Digest::of_bytes(b"resolution");
    let frozen = FrozenToolchains::freeze(
        authority.clone(),
        authority.clone(),
        Scope::project(""),
        &[d.clone()],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    let selected = frozen
        .with_selection(&tool, "22.1.0".parse().unwrap(), authority.clone())
        .unwrap();
    assert_ne!(selected.digest(), frozen.digest());
    assert!(frozen.selections.is_empty());
    assert_eq!(
        selected.check(&[(tool.clone(), VersionObservation::parse("22.1.0"))].into())[&tool],
        Match::Match
    );
    assert_eq!(
        selected.check(&[(tool.clone(), VersionObservation::parse("20.1.0"))].into())[&tool],
        Match::Mismatch
    );
    assert_eq!(selected.check(&BTreeMap::new())[&tool], Match::Unknown);
    assert!(selected
        .with_selection(
            &ToolId::parse("absent").unwrap(),
            "22.1.0".parse().unwrap(),
            authority.clone()
        )
        .is_err());
    d.constraint = Constraint::parse("^20").unwrap();
    let range = FrozenToolchains::freeze(
        authority.clone(),
        authority.clone(),
        Scope::project(""),
        &[d],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(range
        .with_selection(&tool, "22.1.0".parse().unwrap(), authority.clone())
        .is_err());
    assert!(range
        .with_selection(&tool, "20.1.0".parse().unwrap(), authority.clone())
        .is_ok());
    let got = read_project(
        "",
        &docs(&[(
            "package.json",
            r#"{"packageManager":"pnpm@9.1.0+sha224.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        )]),
        &[],
    )
    .unwrap();
    assert!(matches!(got[0].constraint, Constraint::Distribution { .. }));
    let distribution = FrozenToolchains::freeze(
        authority.clone(),
        authority.clone(),
        Scope::project(""),
        &got,
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(distribution
        .with_selection(&got[0].tool, "9.1.0".parse().unwrap(), authority.clone())
        .is_ok());
    assert!(distribution
        .with_selection(&got[0].tool, "9.2.0".parse().unwrap(), authority)
        .is_err());
    for raw in [
        "1.2.3+sha224",
        "1.2.3+sha000.aa",
        "1.2.3+sha224.",
        "1.2.3+sha224.zz",
        "1.2.3+sha224.aaaa",
        "bad+sha224.aa",
    ] {
        assert!(Constraint::parse(raw).is_err());
    }
}

#[test]
fn readers_use_verified_captured_directory_bytes_and_reject_corruption() {
    use crate::source::store::SourceStore;
    use local_ci::source::{capture_directory, seal_manifest, CaptureOptions, RelPath};
    let tmp = crate::test_tmp::tempdir().unwrap();
    let checkout = tmp.path().join("checkout");
    std::fs::create_dir(&checkout).unwrap();
    std::fs::write(checkout.join(".node-version"), "22.1.0").unwrap();
    let store = SourceStore::open(&tmp.path().join("state")).unwrap();
    let ctx = local_ci::ExecContext::new(Default::default());
    let capture = capture_directory(
        &ctx,
        &checkout,
        &CaptureOptions {
            store: Some(store.root().into()),
            ..Default::default()
        },
    )
    .unwrap();
    std::fs::write(checkout.join(".node-version"), "20.1.0").unwrap();
    let got = snapshot::read_captured(&store, &capture.manifest, "", &[]).unwrap();
    assert_eq!(
        got[0]
            .constraint
            .matches(&VersionObservation::parse("22.1.0")),
        Match::Match
    );
    assert_eq!(
        got[0]
            .constraint
            .matches(&VersionObservation::parse("20.1.0")),
        Match::Mismatch
    );
    assert!(snapshot::read_captured(&store, &capture.manifest, "../bad", &[]).is_err());
    let mut bad = capture.manifest.clone();
    bad.manifest_digest.clear();
    assert!(snapshot::read_captured(&store, &bad, "", &[]).is_err());
    let mut secret = capture.manifest.clone();
    secret.secret_files.push(local_ci::source::SecretEntry {
        path: RelPath(b".node-version".to_vec()),
        hmac: "hmac-sha256:private".into(),
    });
    let secret = seal_manifest(secret);
    assert!(snapshot::read_captured(&store, &secret, "", &[]).is_err());
    let mut symlink = capture.manifest.clone();
    symlink.object_manifest.entries[0].mode = 0o120000;
    let symlink = seal_manifest(symlink);
    assert!(snapshot::read_captured(&store, &symlink, "", &[]).is_err());
    let entry = &capture.manifest.object_manifest.entries[0];
    std::fs::write(
        store.object_store().unwrap().object_path(&entry.hash),
        "20.1.0",
    )
    .unwrap();
    assert!(snapshot::read_captured(&store, &capture.manifest, "", &[]).is_err());
}

#[test]
fn frozen_validation_unknown_identity_and_location_boundaries() {
    let tool = ToolId::parse("xcode").unwrap();
    assert_eq!(
        VersionObservation::parse_for(&tool, "16.2"),
        VersionObservation::parse("16.2.0")
    );
    assert_eq!(
        VersionObservation::parse_for(&tool, "16.bad"),
        VersionObservation::Unknown
    );
    assert_eq!(
        VersionObservation::parse_for(&ToolId::parse("node").unwrap(), "22.1.0"),
        VersionObservation::parse("22.1.0")
    );
    let opaque = VersionObservation::identity("vendor-2026.1");
    let other = VersionObservation::identity("vendor-2026.2");
    assert_eq!(MatchPolicy::Exact.compare(&opaque, &opaque), Match::Match);
    assert_eq!(MatchPolicy::Exact.compare(&opaque, &other), Match::Mismatch);
    assert_eq!(MatchPolicy::Minor.compare(&opaque, &other), Match::Unknown);
    assert_eq!(MatchPolicy::Any.compare(&opaque, &other), Match::Match);
    assert_eq!(
        MatchPolicy::Exact.compare(&opaque, &VersionObservation::parse("22.1.0")),
        Match::Unknown
    );
    for raw in ["", "no-digits", "bad/value", &"1".repeat(193)] {
        assert_eq!(
            VersionObservation::identity(raw),
            VersionObservation::Unknown
        );
    }
    let a = VersionObservation::parse("1.2.3+first");
    let b = VersionObservation::parse("1.2.3+second");
    assert_eq!(MatchPolicy::Exact.compare(&a, &b), Match::Mismatch);
    assert_eq!(
        MatchPolicy::Major.compare(
            &VersionObservation::parse("1.2.3-alpha"),
            &VersionObservation::parse("1.3.0-beta")
        ),
        Match::Mismatch
    );
    assert_eq!(
        MatchPolicy::Minor.compare(
            &VersionObservation::parse("1.2.3-alpha"),
            &VersionObservation::parse("1.2.4-beta")
        ),
        Match::Mismatch
    );
    for (path, text) in [("package.json", "[]"), ("pubspec.yaml", "[]")] {
        assert!(read_project("", &docs(&[(path, text)]), &[]).is_err());
    }
    assert!(read_project("../bad", &BTreeMap::new(), &[]).is_err());
    let tool = ToolId::parse("node").unwrap();
    let digest = crate::ids::Digest::of_bytes(b"binding");
    let frozen = FrozenToolchains::freeze(
        digest.clone(),
        digest.clone(),
        Scope::project(""),
        &[decl("22")],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(frozen.validate().is_ok());
    let selected = frozen
        .with_selection(&tool, "22.1.0".parse().unwrap(), digest.clone())
        .unwrap();
    assert!(selected.validate().is_ok());
    let mut profile_changed = selected.clone();
    profile_changed.profile_definition = crate::ids::Digest::of_bytes(b"other-profile");
    assert!(profile_changed.validate().is_err());
    let mut corrupt = selected.clone();
    corrupt.source_manifest = crate::ids::Digest::of_bytes(b"other");
    assert!(corrupt.validate().is_err());
    assert_eq!(corrupt.check(&BTreeMap::new())[&tool], Match::Unknown);
    let mut corrupt = selected.clone();
    corrupt.selections.push(corrupt.selections[0].clone());
    assert!(corrupt.validate().is_err());
    let mut corrupt = selected.clone();
    corrupt.selections[0].tool = ToolId::parse("absent").unwrap();
    assert!(corrupt.validate().is_err());
    let mut corrupt = selected.clone();
    corrupt.selections[0].concrete = "20.1.0".parse().unwrap();
    assert!(corrupt.validate().is_err());
    let mut corrupt = selected;
    corrupt.schema_version = 2;
    assert!(corrupt.validate().is_err());
    let mut corrupt = frozen;
    corrupt.requirements = vec![corrupt.requirements[0].clone(); 257];
    assert!(corrupt.validate().is_err());
    let wf="jobs:\n  a:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '20'\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '22'\n".replace("\\n","\n");
    let got = read_project("", &docs(&[("ci.yml", &wf)]), &["ci.yml".into()]).unwrap();
    assert_eq!(got[0].location.first_line, 6);
    assert_eq!(got[1].location.first_line, 9);
}

#[test]
fn pub_ranges_follow_dart_not_npm_prerelease_or_caret_rules() {
    for (range, version, want) in [
        ("^0.0.3", "0.0.9", true),
        ("^0.0.3", "0.1.0", false),
        ("^3.1.0", "3.5.0-beta", true),
        ("^3.1.0", "4.0.0-beta", false),
        (">=3.0.0 <4.0.0", "3.1.0-beta", true),
        (">=4.0.0-beta <4.0.0", "4.0.0-rc.1", true),
        ("any", "9.0.0-dev", true),
        ("1.2.3", "1.2.3", true),
        (">1.2.3", "1.2.3", false),
        ("<=1.2.3", "1.2.3", true),
        ("<1.2.3", "1.2.3-beta", false),
        (">= 1.2.3 < 2.0.0", "1.2.4", true),
    ] {
        assert_eq!(
            PubRange::parse(range)
                .unwrap()
                .matches(&version.parse().unwrap()),
            want,
            "{range} {version}"
        );
    }
    for raw in [
        "",
        &"1".repeat(513),
        "^bad",
        "3.x",
        ">",
        ">=bad",
        "1.2.3+build",
        ">=1.2.3 || <2.0.0",
    ] {
        assert!(PubRange::parse(raw).is_err());
    }
    let declarations = read_project(
        "",
        &docs(&[("pubspec.yaml", "environment:\n  sdk: ^3.1.0\n")]),
        &[],
    )
    .unwrap();
    assert_eq!(
        declarations[0]
            .constraint
            .matches(&VersionObservation::parse("3.5.0-beta")),
        Match::Match
    );
    let mut d = declarations[0].clone();
    d.constraint = Constraint::parse("<3").unwrap();
    assert!(effective(&[declarations[0].clone(), d], &Scope::project("")).is_err());
    let digest = crate::ids::Digest::of_bytes(b"pub");
    let frozen = FrozenToolchains::freeze(
        digest.clone(),
        digest.clone(),
        Scope::project(""),
        &declarations,
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(frozen
        .with_selection(
            &declarations[0].tool,
            "3.5.0-beta".parse().unwrap(),
            digest.clone()
        )
        .is_ok());
    assert!(frozen
        .with_selection(&declarations[0].tool, "4.0.0".parse().unwrap(), digest)
        .is_err());
}

#[test]
fn public_document_and_declaration_boundaries_are_checked() {
    assert!(effective(&vec![decl("22"); 257], &Scope::project("")).is_err());
    assert!(read_project("", &BTreeMap::new(), &vec!["ci.yml".into(); 257]).is_err());
    let mismatched = [("a".into(), Document::new("b", b"22").unwrap())].into();
    assert!(read_project("", &mismatched, &[]).is_err());
    let oversized = [(
        "a".into(),
        Document {
            path: "a".into(),
            text: "x".repeat(MAX_DOCUMENT_BYTES + 1),
        },
    )]
    .into();
    assert!(read_project("", &oversized, &[]).is_err());
    let wf="jobs:\n  a:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: 'custom.version'\n      - uses: unknown/action@v1\n        with:\n          node-version-file: ignored\n";
    assert_eq!(
        workflow_version_files(&Document::new("ci.yml", wf.as_bytes()).unwrap()).unwrap(),
        vec!["custom.version"]
    );
    let wf = wf.replace("custom.version", "../bad");
    assert!(workflow_version_files(&Document::new("ci.yml", wf.as_bytes()).unwrap()).is_err());
    assert!(workflow_version_files(&Document::new("ci.yml", b"[").unwrap()).is_err());
}

#[test]
fn distribution_conflicts_keep_both_locations() {
    let a = Declaration {
        tool: ToolId::parse("pnpm").unwrap(),
        constraint: Constraint::parse(&format!("9.1.0+sha224.{}", "a".repeat(56))).unwrap(),
        scope: Scope::project(""),
        location: loc(),
    };
    let mut b = a.clone();
    b.constraint = Constraint::parse(&format!("9.1.0+sha224.{}", "b".repeat(56))).unwrap();
    assert_eq!(
        effective(&[a.clone(), b], &Scope::project(""))
            .unwrap_err()
            .locations
            .len(),
        2
    );
    let mut b = a.clone();
    b.constraint = Constraint::parse("8").unwrap();
    assert!(effective(&[b, a.clone()], &Scope::project("")).is_err());
    assert!(effective(&[a.clone(), a], &Scope::project("")).is_ok());
}

#[test]
fn untrusted_frozen_baseline_cannot_override_constraints() {
    let tool = ToolId::parse("node").unwrap();
    let digest = crate::ids::Digest::of_bytes(b"frozen");
    let frozen = FrozenToolchains::freeze(
        digest.clone(),
        digest,
        Scope::project(""),
        &[decl("22")],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    let mut invalid = frozen.clone();
    invalid.baseline.push(Baseline {
        tool: tool.clone(),
        version: VersionObservation::parse("20.1.0"),
        provenance: loc(),
    });
    assert!(invalid.validate().is_err());
    assert_eq!(
        invalid.check(&[(tool.clone(), VersionObservation::parse("20.1.0"))].into())[&tool],
        Match::Unknown
    );
    let mut invalid = frozen.clone();
    invalid.requirements.push(invalid.requirements[0].clone());
    assert!(invalid.validate().is_err());
    let mut invalid = frozen;
    invalid.requirements[0].constraints.clear();
    assert!(invalid.validate().is_err());
    invalid.requirements.clear();
    let baseline = Baseline {
        tool,
        version: VersionObservation::Unknown,
        provenance: loc(),
    };
    invalid.baseline = vec![baseline; 2];
    assert!(invalid.validate().is_err());
}

#[test]
fn frozen_ranges_cannot_float_before_explicit_resolution() {
    let tool = ToolId::parse("node").unwrap();
    let digest = crate::ids::Digest::of_bytes(b"resolution");
    let frozen = FrozenToolchains::freeze(
        digest.clone(),
        digest.clone(),
        Scope::project(""),
        &[decl(">=20")],
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(!frozen.is_resolved());
    assert_eq!(
        frozen.check(&[(tool.clone(), VersionObservation::parse("22.1.0"))].into())[&tool],
        Match::Unknown
    );
    let frozen = frozen
        .with_selection(&tool, "22.1.0".parse().unwrap(), digest.clone())
        .unwrap();
    assert!(frozen.is_resolved());
    assert_eq!(
        frozen.check(&[(tool.clone(), VersionObservation::parse("24.1.0"))].into())[&tool],
        Match::Mismatch
    );
    let unknown = FrozenToolchains::freeze(
        digest.clone(),
        digest.clone(),
        Scope::project(""),
        &[],
        &[(tool.clone(), loc())],
        &BTreeMap::new(),
        MatchPolicy::Any,
    )
    .unwrap();
    assert!(!unknown.is_resolved());
    let known = FrozenToolchains::freeze(
        digest.clone(),
        digest,
        Scope::project(""),
        &[],
        &[(tool.clone(), loc())],
        &[(tool, VersionObservation::parse("22.1.0"))].into(),
        MatchPolicy::Any,
    )
    .unwrap();
    assert!(known.is_resolved());
    let mut invalid = known;
    invalid.schema_version = 2;
    assert!(!invalid.is_resolved());
}

#[test]
fn exact_public_limits_and_scoped_effective_inputs() {
    assert_eq!(MAX_DOCUMENT_BYTES, 262144);
    assert_eq!(MAX_DOCUMENTS, 256);
    assert!(ToolId::parse(&"a".repeat(64)).is_ok());
    let expression = format!("${{{{ {} }}}}", "x".repeat(505));
    assert_eq!(expression.len(), 512);
    assert!(Constraint::parse(&expression).is_ok());
    assert!(Constraint::parse(&expression.replace('x', "xx")).is_err());
    assert!(Constraint::parse(">=20\n<23").is_err());
    assert!(Constraint::parse("").is_err());
    assert!(effective(&vec![decl("22"); 256], &Scope::project("")).is_ok());
    let many: BTreeMap<_, _> = (0..256)
        .map(|i| {
            let path = format!("unused{i}");
            (path.clone(), Document::new(&path, b"").unwrap())
        })
        .collect();
    assert!(read_project("", &many, &[]).is_ok());
    let mut too_many = many;
    too_many.insert("extra".into(), Document::new("extra", b"").unwrap());
    assert_eq!(
        read_project("", &too_many, &[]).unwrap_err().code,
        "too_many_documents"
    );
    let workflows = docs(&[("ci.yml", "jobs: {}")]);
    assert!(read_project("", &workflows, &vec!["ci.yml".into(); 256]).is_ok());
    assert_eq!(
        read_project("", &workflows, &vec!["ci.yml".into(); 258])
            .unwrap_err()
            .code,
        "too_many_documents"
    );
    let mut d = decl("22");
    d.scope = Scope {
        project: "app".into(),
        workflow: Some("a.yml".into()),
        job: Some("a".into()),
        step: Some(1),
    };
    assert_eq!(effective(&[d.clone()], &d.scope).unwrap().len(), 1);
    for field in 0..4 {
        let mut other = d.scope.clone();
        match field {
            0 => other.project = "other".into(),
            1 => other.workflow = Some("b.yml".into()),
            2 => other.job = Some("b".into()),
            _ => other.step = Some(2),
        };
        assert!(effective(&[d.clone()], &other).unwrap().is_empty());
    }
    assert_eq!(
        VersionObservation::parse_for(&ToolId::parse("node").unwrap(), "16.2"),
        VersionObservation::Unknown
    );
    assert_eq!(
        VersionObservation::parse_for(&ToolId::parse("xcode").unwrap(), "16.2.1"),
        VersionObservation::parse("16.2.1")
    );
    let got = read_project(
        "",
        &docs(&[(".tool-versions", "# tools\nnode 22\npython 3.12\n")]),
        &[],
    )
    .unwrap();
    assert_eq!(got[0].location.first_line, 2);
    assert_eq!(got[0].location.last_line, 2);
    assert_eq!(got[1].location.first_line, 3);
    assert_eq!(got[1].location.last_line, 3);
}
#[test]
fn format_errors_and_workflow_context_are_not_hidden_by_other_parsers() {
    for (path, text) in [
        ("package.json", "{'engines': {'node': '22'}}"),
        ("package.json", r#"{"engines":{"bad/tool":"22"}}"#),
        ("mise.toml", "[tools]\nnode = 'bad'"),
        (".tool-versions", "node bad"),
        (".fvmrc", r#"{"flutter":"bad"}"#),
        ("pubspec.yaml", "environment:\n  sdk: '^bad'"),
    ] {
        assert!(read_project("", &docs(&[(path, text)]), &[]).is_err());
    }
    assert!(read_project("", &docs(&[(".node-version", "20"), (".nvmrc", "22")]), &[]).is_err());
    let wf="name: actions/setup-node\njobs:\n  a:\n    steps:\n      - uses: unknown/action@v1\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '22'\n";
    let got = read_project("", &docs(&[("ci.yml", wf)]), &["ci.yml".into()]).unwrap();
    assert_eq!(got[0].location.first_line, 8);
    assert!(read_project(
        "",
        &docs(&[("ci.yml", wf), (".node-version", "20")]),
        &["ci.yml".into()]
    )
    .is_err());
    assert!(read_project(
        "",
        &docs(&[("ci.yml", &wf.replace("'22'", "'bad'"))]),
        &["ci.yml".into()]
    )
    .is_err());
    for (action, inputs, count) in [
        ("actions/setup-node@stable", "", 0),
        ("dtolnay/rust-toolchain@012345", "", 0),
        (
            "dtolnay/rust-toolchain@stable",
            "        with:\n          toolchain: '1.82.0'\n",
            1,
        ),
    ] {
        let wf = format!("jobs:\n  a:\n    steps:\n      - uses: {action}\n{inputs}");
        let got = read_project("", &docs(&[("ci.yml", &wf)]), &["ci.yml".into()]).unwrap();
        assert_eq!(got.len(), count);
    }
    for bits in [224, 256, 384, 512] {
        assert!(Constraint::parse(&format!("1.2.3+sha{bits}.{}", "a".repeat(bits / 4))).is_ok());
    }
    assert!(Constraint::parse(&format!("bad+sha224.{}", "a".repeat(56))).is_err());
    assert!(Constraint::parse(&format!("1.2.3+sha224.{}", "z".repeat(56))).is_err());
}
#[test]
fn frozen_limit_boundaries_reselection_and_all_constraint_validators() {
    let digest = crate::ids::Digest::of_bytes(b"limits");
    let declarations: Vec<_> = (0..256)
        .map(|i| {
            let mut d = decl("22");
            d.tool = ToolId::parse(&format!("tool{i}")).unwrap();
            d
        })
        .collect();
    let full = FrozenToolchains::freeze(
        digest.clone(),
        digest.clone(),
        Scope::project(""),
        &declarations,
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(full.validate().is_ok());
    // One entry past each bound is refused for its size, before any
    // per-entry rule (the extra entry is also a duplicate) is consulted.
    let mut over = full.clone();
    over.requirements.push(over.requirements[0].clone());
    assert_eq!(over.validate(), Err("invalid_frozen_toolchains"));
    let mut baselines = full.clone();
    baselines.requirements.clear();
    baselines.baseline = declarations
        .iter()
        .map(|d| Baseline {
            tool: d.tool.clone(),
            version: VersionObservation::Unknown,
            provenance: loc(),
        })
        .collect();
    assert!(baselines.validate().is_ok());
    let mut over = baselines;
    over.baseline.push(over.baseline[0].clone());
    assert_eq!(over.validate(), Err("invalid_frozen_toolchains"));
    let mut selected = full;
    for d in &declarations {
        selected = selected
            .with_selection(&d.tool, "22.1.0".parse().unwrap(), digest.clone())
            .unwrap();
    }
    assert!(selected.validate().is_ok());
    let mut over = selected.clone();
    over.selections.push(over.selections[0].clone());
    assert_eq!(over.validate(), Err("invalid_frozen_toolchains"));
    let updated = selected
        .with_selection(
            &declarations[0].tool,
            "22.2.0".parse().unwrap(),
            digest.clone(),
        )
        .unwrap();
    assert_eq!(updated.selections.len(), 256);
    assert_eq!(
        updated
            .selections
            .iter()
            .find(|s| s.tool == declarations[0].tool)
            .unwrap()
            .concrete
            .to_string(),
        "22.2.0"
    );
    for constraint in [
        Constraint::Pub(PubRange::parse("^22.0.0").unwrap()),
        Constraint::parse(&format!("22.1.0+sha224.{}", "a".repeat(56))).unwrap(),
    ] {
        let mut declaration = decl("22");
        declaration.constraint = constraint;
        let frozen = FrozenToolchains::freeze(
            digest.clone(),
            digest.clone(),
            Scope::project(""),
            &[declaration.clone()],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .unwrap()
        .with_selection(&declaration.tool, "22.1.0".parse().unwrap(), digest.clone())
        .unwrap();
        assert!(frozen.validate().is_ok());
        let mut corrupt = frozen;
        corrupt.selections[0].concrete = "23.0.0".parse().unwrap();
        assert!(corrupt.validate().is_err());
    }
    assert_eq!(
        MatchPolicy::Any.compare(
            &VersionObservation::identity("vendor1"),
            &VersionObservation::parse("1.0.0")
        ),
        Match::Match
    );
}
#[test]
fn pub_range_boundaries_and_pre_release_contexts() {
    let raw = format!("{}{}1.2.3", "10.2.3 ".repeat(3), "1.2.3 ".repeat(81));
    assert_eq!(raw.len(), 512);
    assert!(PubRange::parse(&raw).is_ok());
    assert!(PubRange::parse(&raw.replacen("10.2.3", "100.2.3", 1)).is_err());
    for (range, value, want) in [
        (">1.2.3", "1.2.4", true),
        ("<1.2.3", "1.2.3-0", false),
        (">=3.0.0 <4.0.0", "4.0.0-beta", false),
        (">=3.0.0-beta <4.0.0", "4.0.0-beta", false),
        (">=4.1.0-beta <4.2.0", "4.2.0-beta", false),
        (">=4.2.1-beta <4.2.2", "4.2.2-beta", false),
    ] {
        assert_eq!(
            PubRange::parse(range)
                .unwrap()
                .matches(&value.parse().unwrap()),
            want
        );
    }
    for raw in ["1.2.3", ">1.2.3", "<=1.2.3", "<1.2.3", "<1.2.3-beta"] {
        let mut d = decl("22");
        d.constraint = Constraint::Pub(PubRange::parse(raw).unwrap());
        assert!(effective(&[d], &Scope::project("")).is_ok());
    }
    let mut low = decl("<3");
    low.tool = ToolId::parse("dart").unwrap();
    let mut high = low.clone();
    high.constraint = Constraint::Pub(PubRange::parse("^4.0.0").unwrap());
    assert!(effective(&[low, high], &Scope::project("")).is_err());
    assert_eq!(
        Constraint::Pub(PubRange::parse("^3.0.0").unwrap())
            .matches(&VersionObservation::parse("4.0.0")),
        Match::Mismatch
    );
}

#[test]
fn version_locations_ignore_action_names_on_non_action_steps() {
    let workflow="jobs:\n  a:\n    steps:\n      - name: actions/setup-node\n        run: true\n      - uses: actions/setup-node@v4\n        with:\n          node-version: '22'\n";
    let document = Document::new("ci.yml", workflow.as_bytes()).unwrap();
    let got = read_project(
        "",
        &[("ci.yml".into(), document.clone())].into(),
        &["ci.yml".into()],
    )
    .unwrap();
    assert_eq!(got[0].location.first_line, 8);
    assert!(workflow_version_files(&document).unwrap().is_empty());
    assert!(PubRange::parse("=1.2.3").is_err());
}

#[test]
fn version_file_expressions_wait_for_resolution_and_errors_keep_locations() {
    assert!(read_project(
        "",
        &docs(&[("rust-toolchain.toml", "[toolchain]\nchannel = 'bad'\n")]),
        &[]
    )
    .is_err());
    let workflow="jobs:\n  a:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: '${{ inputs.version_file }}'\n";
    let document = Document::new("ci.yml", workflow.as_bytes()).unwrap();
    assert!(workflow_version_files(&document).unwrap().is_empty());
    let got = read_project(
        "",
        &[("ci.yml".into(), document)].into(),
        &["ci.yml".into()],
    )
    .unwrap();
    assert!(matches!(
        got[0].constraint,
        Constraint::VersionFileExpression(_)
    ));
    assert_eq!(
        got[0]
            .constraint
            .matches(&VersionObservation::parse("22.1.0")),
        Match::Unknown
    );
    let digest = crate::ids::Digest::of_bytes(b"file-expression");
    let frozen = FrozenToolchains::freeze(
        digest.clone(),
        digest.clone(),
        Scope {
            project: "".into(),
            workflow: Some("ci.yml".into()),
            job: Some("a".into()),
            step: Some(0),
        },
        &got,
        &[],
        &BTreeMap::new(),
        MatchPolicy::Exact,
    )
    .unwrap();
    assert!(!frozen.is_resolved());
    assert!(frozen
        .with_selection(&got[0].tool, "22.1.0".parse().unwrap(), digest)
        .unwrap()
        .is_resolved());
    let invalid = workflow.replace("${{ inputs.version_file }}", "${{ broken");
    let document = Document::new("ci.yml", invalid.as_bytes()).unwrap();
    assert!(workflow_version_files(&document).is_err());
    assert!(read_project(
        "",
        &[("ci.yml".into(), document)].into(),
        &["ci.yml".into()]
    )
    .is_err());
    let workflow = workflow.replace("${{ inputs.version_file }}", "custom.txt");
    let input = docs(&[("ci.yml", &workflow), ("custom.txt", "# capture\n22.1.0\n")]);
    let got = read_project("", &input, &["ci.yml".into()]).unwrap();
    assert_eq!(got[0].location.path, "custom.txt");
    let invalid = docs(&[("ci.yml", &workflow), ("custom.txt", "bad")]);
    assert!(read_project("", &invalid, &["ci.yml".into()]).is_err());
}

#[test]
fn declared_selections_stay_exact_while_undeclared_baselines_use_policy() {
    let tool = ToolId::parse("node").unwrap();
    let digest = crate::ids::Digest::of_bytes(b"exact-selection");
    let captured = [(tool.clone(), VersionObservation::parse("20.1.0"))].into();
    for policy in [
        MatchPolicy::Exact,
        MatchPolicy::Minor,
        MatchPolicy::Major,
        MatchPolicy::Any,
    ] {
        let selected = FrozenToolchains::freeze(
            digest.clone(),
            digest.clone(),
            Scope::project(""),
            &[decl(">=20 <23")],
            &[],
            &captured,
            policy,
        )
        .unwrap()
        .with_selection(&tool, "20.1.0".parse().unwrap(), digest.clone())
        .unwrap();
        assert_eq!(selected.check(&captured)[&tool], Match::Match);
        for observed in ["20.1.1", "20.5.0", "22.0.0"] {
            assert_eq!(
                selected.check(&[(tool.clone(), VersionObservation::parse(observed))].into())
                    [&tool],
                Match::Mismatch,
                "{policy:?}: {observed}"
            );
        }
        assert_eq!(selected.check(&BTreeMap::new())[&tool], Match::Unknown);
        let baseline = FrozenToolchains::freeze(
            digest.clone(),
            digest.clone(),
            Scope::project(""),
            &[],
            &[(tool.clone(), loc())],
            &captured,
            policy,
        )
        .unwrap();
        for (observed, allowed) in [
            ("20.1.1", policy != MatchPolicy::Exact),
            (
                "20.5.0",
                matches!(policy, MatchPolicy::Major | MatchPolicy::Any),
            ),
            ("22.0.0", policy == MatchPolicy::Any),
        ] {
            assert_eq!(
                baseline.check(&[(tool.clone(), VersionObservation::parse(observed))].into())
                    [&tool],
                if allowed {
                    Match::Match
                } else {
                    Match::Mismatch
                },
                "baseline {policy:?}: {observed}"
            );
        }
        assert_eq!(baseline.check(&BTreeMap::new())[&tool], Match::Unknown);
    }
}

fn node_file_workflow(input: &str) -> String {
    format!(
        "jobs:\n  node:\n    steps:\n      - uses: actions/setup-node@v4\n        with:\n{input}"
    )
}
#[test]
fn setup_node_structured_custom_files_follow_upstream_selectors() {
    for (path, content) in [
        ("meta/package.json", r#"{"engines":{"node":"22.1.0"}}"#),
        (
            "meta/custom.txt",
            r#"{"volta":{"node":"22.1.0"},"engines":{"node":"20"}}"#,
        ),
        (
            "meta/custom.txt",
            r#"{"volta":{"node":false},"devEngines":{"runtime":{"name":"Node","version":"22.1.0"}},"engines":{"node":"20"}}"#,
        ),
        (
            "meta/custom.txt",
            r#"{"devEngines":{"runtime":[{"name":"other","version":"20"},{"name":"node","version":"22.1.0"}]},"engines":{"node":"20"}}"#,
        ),
        (
            "meta/custom.txt",
            r#"{"volta":{"node":null},"devEngines":{"runtime":{"name":"node","version":0}},"engines":{"node":"22.1.0"}}"#,
        ),
        (
            "meta/custom.txt",
            r#"{"volta":{"node":""},"engines":{"node":"22.1.0"}}"#,
        ),
        ("meta/.tool-versions", "python 3.12\nnodejs 22.1.0\n"),
        ("meta/custom.txt", "node 22.1.0\n"),
        ("meta/mise.toml", "[tools]\nnode = '22.1.0'\n"),
        (
            "meta/custom.txt",
            "[tools]\nnode = { version = '22.1.0' }\n",
        ),
        ("meta/custom.txt", "# captured\nv22.1.0\n"),
    ] {
        let workflow = node_file_workflow(&format!("          node-version-file: '{path}'\n"));
        let got = read_project(
            "app",
            &docs(&[("ci.yml", &workflow), (path, content)]),
            &["ci.yml".into()],
        )
        .unwrap();
        assert_eq!(got.len(), 1, "{path}: {content}");
        assert_eq!(got[0].location.path, path);
        assert_eq!(
            got[0]
                .constraint
                .matches(&VersionObservation::parse("22.1.0")),
            Match::Match
        );
        assert_eq!(
            got[0]
                .constraint
                .matches(&VersionObservation::parse("20.1.0")),
            Match::Mismatch
        );
    }
    for content in [
        "{bad",
        r#"{"volta":{"extends":"../package.json"}}"#,
        r#"{"engines":{"node":[]}}"#,
        r#"{"volta":{"node":{}}}"#,
        r#"{"engines":{"node":true}}"#,
        r#"{"engines":{"node":22}}"#,
        r#"{"engines":{"node":"bad"}}"#,
        "[tools]\nnode = ['22']",
        "nodejs bad",
        "bad",
    ] {
        let workflow = node_file_workflow("          node-version-file: 'meta/custom.json'\n");
        assert!(read_project(
            "app",
            &docs(&[("ci.yml", &workflow), ("meta/custom.json", content)]),
            &["ci.yml".into()]
        )
        .is_err());
    }
    // Independent project declarations remain constraints even though a
    // setup-node direct input takes precedence over its own file input.
    let workflow =
        node_file_workflow("          node-version: '20'\n          node-version-file: ignored\n");
    assert!(read_project(
        "app",
        &docs(&[("app/.node-version", "22"), ("ci.yml", &workflow)]),
        &["ci.yml".into()]
    )
    .is_err());
}
#[test]
fn setup_node_direct_input_precedes_ignored_files_in_discovery_and_reading() {
    for direct in ["22.1.0", "${{ matrix.node }}"] {
        for file in ["missing.json", "bad.json", "../not-read"] {
            let workflow = node_file_workflow(&format!(
                "          node-version: '{direct}'\n          node-version-file: '{file}'\n"
            ));
            let document = Document::new("ci.yml", workflow.as_bytes()).unwrap();
            assert!(workflow_version_files(&document).unwrap().is_empty());
            let got = read_project(
                "app",
                &docs(&[("ci.yml", &workflow), ("bad.json", "{malformed")]),
                &["ci.yml".into()],
            )
            .unwrap();
            assert_eq!(got.len(), 1);
            if direct.contains("${{") {
                assert!(matches!(got[0].constraint, Constraint::Expression(_)));
            } else {
                assert_eq!(
                    got[0]
                        .constraint
                        .matches(&VersionObservation::parse("22.1.0")),
                    Match::Match
                );
            }
        }
    }
    let workflow = node_file_workflow(
        "          node-version: ''\n          node-version-file: 'meta/package.json'\n",
    );
    let document = Document::new("ci.yml", workflow.as_bytes()).unwrap();
    assert_eq!(
        workflow_version_files(&document).unwrap(),
        vec!["meta/package.json"]
    );
    assert_eq!(
        read_project(
            "app",
            &docs(&[
                ("ci.yml", &workflow),
                ("meta/package.json", r#"{"engines":{"node":"22.1.0"}}"#)
            ]),
            &["ci.yml".into()]
        )
        .unwrap()
        .len(),
        1
    );
}
#[test]
fn captured_monorepo_node_files_are_workspace_relative_and_snapshot_bound() {
    use crate::source::store::SourceStore;
    use local_ci::source::{capture_directory, CaptureOptions};
    for (input,path,content,expected) in [
        ("          node-version-file: 'app/.node-version'\n","app/.node-version","22.1.0",2),
        ("          node-version-file: 'meta/package.json'\n","meta/package.json",r#"{"engines":{"node":"22.1.0"}}"#,1),
        ("          node-version-file: 'meta/mise.toml'\n","meta/mise.toml","[tools]\nnode = { version = '22.1.0' }\n",1),
        ("          node-version-file: 'meta/.tool-versions'\n","meta/.tool-versions","python 3.12\nnodejs 22.1.0\n",1),
        ("          node-version: '22.1.0'\n          node-version-file: missing.json\n","unused.txt","unused",1),
        ("          node-version: '${{ matrix.node }}'\n          node-version-file: missing.json\n","unused.txt","unused",1),
        ("          node-version: '22.1.0'\n          node-version-file: bad.json\n","bad.json","{malformed",1),
    ] {
        let tmp=crate::test_tmp::tempdir().unwrap();let source=tmp.path().join("source");std::fs::create_dir(&source).unwrap();let file=source.join(path);std::fs::create_dir_all(file.parent().unwrap()).unwrap();std::fs::write(&file,content).unwrap();
        let workflow=node_file_workflow(input);std::fs::write(source.join("ci.yml"),workflow).unwrap();
        let store=SourceStore::open(&tmp.path().join("state")).unwrap();let ctx=local_ci::ExecContext::new(Default::default());let captured=capture_directory(&ctx,&source,&CaptureOptions{store:Some(store.root().into()),..Default::default()}).unwrap();
        std::fs::write(file,"20.1.0").unwrap();
        let got=snapshot::read_captured(&store,&captured.manifest,"app",&["ci.yml".into()]).unwrap();assert_eq!(got.len(),expected,"{path}");
        for declaration in got {if !matches!(declaration.constraint,Constraint::Expression(_)) {assert_eq!(declaration.constraint.matches(&VersionObservation::parse("22.1.0")),Match::Match);}}
    }
}
#[test]
fn pub_observed_and_selected_build_metadata_uses_pub_ordering() {
    for (range, version, expected) in [
        ("1.2.3", "1.2.3+1", false),
        ("<=1.2.3", "1.2.3+1", false),
        (">1.2.3", "1.2.3+1", true),
        (">=1.2.3", "1.2.3+1", true),
        ("<1.2.3", "1.2.3+1", false),
        ("^1.2.3", "1.2.3+2.10", true),
        ("<2.0.0", "2.0.0-0+1", false),
        ("<=1.2.3-beta", "1.2.3-beta+1", false),
        (">1.2.3-beta", "1.2.3-beta+1", true),
    ] {
        assert_eq!(
            PubRange::parse(range)
                .unwrap()
                .matches(&version.parse().unwrap()),
            expected,
            "{range}: {version}"
        );
    }
    let digest = crate::ids::Digest::of_bytes(b"pub-build");
    for (range, allowed) in [("1.2.3", false), ("<=1.2.3", false), (">1.2.3", true)] {
        let mut declaration = decl("1.2.3");
        declaration.tool = ToolId::parse("dart").unwrap();
        declaration.constraint = Constraint::Pub(PubRange::parse(range).unwrap());
        let frozen = FrozenToolchains::freeze(
            digest.clone(),
            digest.clone(),
            Scope::project(""),
            &[declaration.clone()],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Any,
        )
        .unwrap();
        let selected = frozen.with_selection(
            &declaration.tool,
            "1.2.3+1".parse().unwrap(),
            digest.clone(),
        );
        assert_eq!(selected.is_ok(), allowed);
        if let Ok(selected) = selected {
            assert!(selected.validate().is_ok());
            assert_eq!(
                selected.check(
                    &[(
                        declaration.tool.clone(),
                        VersionObservation::parse("1.2.3+1")
                    )]
                    .into()
                )[&declaration.tool],
                Match::Match
            );
            assert_eq!(
                selected.check(
                    &[(
                        declaration.tool.clone(),
                        VersionObservation::parse("1.2.3+2")
                    )]
                    .into()
                )[&declaration.tool],
                Match::Mismatch
            );
        }
    }
    assert!(
        PubRange::parse("1.2.3+1").is_err(),
        "build-bearing bounds remain explicitly resolution-required"
    );
}

#[test]
fn non_node_workspace_files_and_node_toml_without_node_remain_explicit() {
    let workflow="jobs:\n  python:\n    steps:\n      - uses: actions/setup-python@v5\n        with:\n          python-version-file: meta/python.txt\n";
    let got = read_project(
        "app",
        &docs(&[
            ("ci.yml", workflow),
            ("meta/python.txt", "# captured\n3.12\n"),
        ]),
        &["ci.yml".into()],
    )
    .unwrap();
    assert_eq!(got[0].tool.as_str(), "python");
    assert_eq!(got[0].location.path, "meta/python.txt");
    assert!(read_project(
        "app",
        &docs(&[("ci.yml", workflow), ("meta/python.txt", "bad")]),
        &["ci.yml".into()]
    )
    .is_err());
    let workflow = node_file_workflow("          node-version-file: meta/mise.toml\n");
    assert!(read_project(
        "app",
        &docs(&[
            ("ci.yml", &workflow),
            ("meta/mise.toml", "[tools]\npython = '3.12'\n")
        ]),
        &["ci.yml".into()]
    )
    .is_err());
}
