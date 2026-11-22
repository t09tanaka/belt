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

#[test]
fn a_requirement_is_made_for_the_project_and_for_each_narrower_scope() {
    let step = |job: &str, n: usize, raw: &str| Declaration {
        scope: Scope {
            project: "".into(),
            workflow: Some("w.yml".into()),
            job: Some(job.into()),
            step: Some(n),
        },
        ..decl(raw)
    };
    assert!(requirement_scopes(&[]).is_empty());
    let found = [
        decl(">=18"),
        step("a", 0, "^20"),
        step("a", 0, "^20.1"),
        step("b", 1, "^22"),
    ];
    let scopes = requirement_scopes(&found);
    assert_eq!(scopes.len(), 3, "{scopes:?}");
    assert!(scopes.contains(&Scope::project("")));
    // The step of the same scope counts once; each scope sees the project's
    // own declarations as well.
    let a = effective(&found, &scopes[1]).unwrap();
    assert_eq!(a[0].constraints.len(), 3);
    let project = effective(&found, &Scope::project("")).unwrap();
    assert_eq!(project[0].constraints.len(), 1);
    // Another project is its own scope.
    let other = Declaration {
        scope: Scope::project("web"),
        ..decl("^16")
    };
    assert_eq!(requirement_scopes(&[other]).len(), 1);
}

#[test]
fn every_constraint_describes_itself_the_way_a_message_names_it() {
    let describe = |raw: &str| Constraint::parse(raw).unwrap().describe();
    assert_eq!(describe("20.0.0"), "20.0.0");
    assert_eq!(describe("lts/iron"), "lts/iron");
    assert_eq!(describe("stable"), "stable");
    assert_eq!(describe("${{ matrix.node }}"), "${{ matrix.node }}");
    let hash = "a".repeat(64);
    assert_eq!(describe(&format!("20.1.2+sha256.{hash}")), "20.1.2");
    assert_eq!(
        Constraint::VersionFileExpression("${{ env.FILE }}".into()).describe(),
        "${{ env.FILE }}"
    );
    let pubs = |raw: &str| Constraint::Pub(PubRange::parse(raw).unwrap()).describe();
    assert_eq!(pubs("any"), "any");
    assert_eq!(pubs("1.2.3"), "1.2.3");
    assert_eq!(pubs(">=1.0.0 <2.0.0"), ">=1.0.0 <2.0.0-0");
    assert_eq!(pubs(">1.0.0 <=2.0.0"), ">1.0.0 <=2.0.0");
}

#[test]
fn requirements_name_tool_and_version_in_order() {
    let requirement = |tool: &str, raws: &[&str]| EffectiveRequirement {
        tool: ToolId::parse(tool).unwrap(),
        constraints: raws.iter().map(|r| Constraint::parse(r).unwrap()).collect(),
        provenance: vec![loc()],
    };
    assert_eq!(requirement("node", &["20.0.0"]).describe(), "node=20.0.0");
    assert_eq!(
        requirement("node", &["20.0.0", "stable"]).describe(),
        "node=20.0.0,stable"
    );
    assert_eq!(requirement("node", &[]).describe(), "node=");
    let frozen = |requirements: Vec<EffectiveRequirement>| FrozenToolchains {
        schema_version: 1,
        source_manifest: crate::ids::Digest::of_bytes(b"source"),
        profile_definition: crate::ids::Digest::of_bytes(b"profile"),
        scope: Scope::project(""),
        requirements,
        baseline: vec![],
        selections: vec![],
        policy: MatchPolicy::Exact,
    };
    assert_eq!(frozen(vec![]).describe_requirements(), "");
    assert_eq!(
        frozen(vec![requirement("node", &["20.0.0"])]).describe_requirements(),
        "node=20.0.0"
    );
    assert_eq!(
        frozen(vec![
            requirement("node", &["20.0.0"]),
            requirement("rust", &["1.80.0"])
        ])
        .describe_requirements(),
        "node=20.0.0, rust=1.80.0"
    );
}

#[test]
fn one_context_reference_is_read_and_nothing_else_is() {
    let reference = |text: &str| context_reference(text).map(|(c, k)| (c, k.to_string()));
    let pair = |context: &str, key: &str| Some((context.to_string(), key.to_string()));
    assert_eq!(reference("${{ matrix.node }}"), pair("matrix", "node"));
    // Spaces do not matter; a key may hold letters, digits, `_` and `-`.
    assert_eq!(reference("  ${{env.NODE_v-1}} "), pair("env", "NODE_v-1"));
    assert_eq!(reference("${{ my-ctx_2.k }}"), pair("my-ctx_2", "k"));
    // A context name is not case sensitive; the key is kept as written.
    assert_eq!(reference("${{ Matrix.Node }}"), pair("matrix", "Node"));
    assert_eq!(reference("${{ ENV.NODE }}"), pair("env", "NODE"));
    for other in [
        "matrix.node",
        "${{ matrix.node",
        "matrix.node }}",
        "${{ matrix }}",
        "${{ .node }}",
        "${{ matrix. }}",
        "${{ matrix.a.b }}",
        "${{ matrix.node || 20 }}",
        "${{ ma trix.node }}",
        "${{ matrix.no de }}",
        "v${{ matrix.node }}",
        "${{ matrix.node }}.x",
        "${{ fromJSON(matrix.node) }}",
    ] {
        assert_eq!(context_reference(other), None, "{other}");
    }
}

fn step_scope(workflow: &str, job: &str, step: usize) -> Scope {
    Scope {
        project: "".into(),
        workflow: Some(workflow.into()),
        job: Some(job.into()),
        step: Some(step),
    }
}
fn step_decl(tool: &str, raw: &str, workflow: &str, job: &str, step: usize) -> Declaration {
    Declaration {
        tool: ToolId::parse(tool).unwrap(),
        constraint: Constraint::parse(raw).unwrap(),
        scope: step_scope(workflow, job, step),
        location: Location {
            path: workflow.into(),
            first_line: step + 5,
            last_line: step + 5,
        },
    }
}
fn task_scope(workflow: &str, job: &str) -> Scope {
    Scope {
        project: "".into(),
        workflow: Some(workflow.into()),
        job: Some(job.into()),
        step: None,
    }
}
fn read_of(declarations: &[Declaration]) -> SetupRead {
    SetupRead {
        declarations: declarations.to_vec(),
        ..Default::default()
    }
}
fn no_value(_: &Scope, _: &str, _: &str) -> Option<String> {
    None
}
fn resolved(_: &ToolId, _: &str) -> bool {
    false
}
/// `task_declarations` of the root project with no alias left unresolved.
fn task_of(
    read: &SetupRead,
    workflow: &str,
    jobs: &[&str],
    lookup: &dyn Fn(&Scope, &str, &str) -> Option<String>,
) -> (Scope, Vec<Declaration>, Vec<Skipped>) {
    let jobs: Vec<String> = jobs.iter().map(|job| job.to_string()).collect();
    task_declarations(read, "", workflow, &jobs, lookup, &resolved)
}
fn narrowed(d: &Declaration, workflow: &str, job: &str) -> Declaration {
    Declaration {
        scope: task_scope(workflow, job),
        ..d.clone()
    }
}

#[test]
fn a_task_takes_the_project_declarations_and_those_of_its_own_jobs() {
    let mut elsewhere = decl("18.0.0");
    elsewhere.scope = Scope::project("sub");
    let all = [
        decl("20.0.0"),
        elsewhere,
        step_decl("python", "3.12.0", "ci.yml", "build", 0),
        step_decl("go", "1.22.0", "ci.yml", "other", 2),
        step_decl("ruby", "3.3.0", "other.yml", "build", 0),
    ];
    let read = read_of(&all);
    // One job: the project file and that job only, in the order they were read.
    let (scope, got, skipped) = task_of(&read, "ci.yml", &["build"], &no_value);
    assert_eq!(scope, task_scope("ci.yml", "build"));
    assert_eq!(got, [all[0].clone(), narrowed(&all[2], "ci.yml", "build")]);
    assert_eq!(skipped, []);
    let required = effective(&got, &scope).unwrap();
    let tools: Vec<&str> = required.iter().map(|r| r.tool.as_str()).collect();
    assert_eq!(tools, ["node", "python"]);
    // Jobs grouped into one task: the union, in one scope that names them all.
    let (scope, got, _) = task_of(&read, "ci.yml", &["build", "other"], &no_value);
    assert_eq!(scope, task_scope("ci.yml", "build,other"));
    assert_eq!(
        got,
        [
            all[0].clone(),
            narrowed(&all[2], "ci.yml", "build,other"),
            narrowed(&all[3], "ci.yml", "build,other")
        ]
    );
    // A task without a setup declaration is frozen exactly as the project is.
    let (scope, got, skipped) = task_of(&read, "ci.yml", &["lint"], &no_value);
    assert_eq!(scope, Scope::project(""));
    assert_eq!(got, [all[0].clone()]);
    assert_eq!(skipped, []);
    assert_eq!(
        effective(&got, &scope).unwrap(),
        effective(&all, &Scope::project("")).unwrap()
    );
    // The same job id in another workflow is another job.
    let (scope, got, _) = task_of(&read, "other.yml", &["build"], &no_value);
    assert_eq!(scope, task_scope("other.yml", "build"));
    assert_eq!(
        got,
        [all[0].clone(), narrowed(&all[4], "other.yml", "build")]
    );
    // Another project is another scope: its own files, and the same steps.
    let jobs = ["build".to_string()];
    let (scope, got, _) = task_declarations(&read, "sub", "ci.yml", &jobs, &no_value, &resolved);
    assert_eq!(scope.project, "sub");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], all[1]);
    assert_eq!(got[1].scope, scope);
}

#[test]
fn two_steps_of_one_task_contradict_and_a_step_stands_before_the_project_file() {
    let all = [
        step_decl("node", "18.0.0", "ci.yml", "a", 0),
        step_decl("node", "20.0.0", "ci.yml", "b", 3),
    ];
    // Each step alone is consistent, as `workers sync` reads them.
    for d in &all {
        assert!(effective(&all, &d.scope).is_ok());
    }
    let (scope, got, skipped) = task_of(&read_of(&all), "ci.yml", &["a", "b"], &no_value);
    assert_eq!(skipped, []);
    let conflict = effective(&got, &scope).unwrap_err();
    assert_eq!(conflict.code, "conflicting_versions");
    assert_eq!(
        conflict.locations,
        [all[0].location.clone(), all[1].location.clone()]
    );
    // A project file and a step that disagree: the step installs its version
    // for the job, so the file is no requirement of this task. The file of
    // another tool stays one.
    let mut rust = decl("1.80.0");
    rust.tool = ToolId::parse("rust").unwrap();
    let all = [
        decl("22.0.0"),
        rust.clone(),
        step_decl("node", "20.0.0", "ci.yml", "a", 0),
    ];
    let read = read_of(&all);
    let (scope, got, skipped) = task_of(&read, "ci.yml", &["a"], &no_value);
    assert_eq!(scope, task_scope("ci.yml", "a"));
    assert_eq!(got, [rust.clone(), narrowed(&all[2], "ci.yml", "a")]);
    assert_eq!(skipped, []);
    let lines: Vec<String> = effective(&got, &scope)
        .unwrap()
        .iter()
        .map(|r| r.describe())
        .collect();
    assert_eq!(lines, ["node=20.0.0", "rust=1.80.0"]);
    // A task that does not run the step keeps the file.
    let (scope, got, _) = task_of(&read, "ci.yml", &["b"], &no_value);
    assert_eq!(scope, Scope::project(""));
    assert_eq!(got, [all[0].clone(), rust]);
}

fn skipped_of(d: &Declaration, code: &str, value: &str, step: &Location) -> Skipped {
    Skipped {
        tool: d.tool.clone(),
        scope: d.scope.clone(),
        code: code.into(),
        value: value.into(),
        location: d.location.clone(),
        step: step.clone(),
    }
}

#[test]
fn a_reference_takes_the_value_of_its_step_and_anything_else_is_skipped() {
    let reference = step_decl("node", "${{ matrix.node }}", "ci.yml", "build", 1);
    let read = read_of(std::slice::from_ref(&reference));
    // The lookup is asked with the step the input is written in.
    let variant = |scope: &Scope, context: &str, key: &str| {
        (scope.step == Some(1) && scope.job.as_deref() == Some("build"))
            .then(|| format!("{context}.{key}"))
            .filter(|asked| asked == "matrix.node")
            .map(|_| "18.0.0".to_string())
    };
    let (scope, got, skipped) = task_of(&read, "ci.yml", &["build"], &variant);
    assert_eq!(scope, task_scope("ci.yml", "build"));
    assert_eq!(skipped, []);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].constraint, Constraint::parse("18.0.0").unwrap());
    assert_eq!(got[0].location, reference.location);
    assert_eq!(
        effective(&got, &scope).unwrap()[0].describe(),
        "node=18.0.0"
    );
    // The context arrives in lower case, however it is written.
    let shouted = step_decl("node", "${{ MATRIX.node }}", "ci.yml", "build", 1);
    let (_, got, skipped) = task_of(
        &read_of(std::slice::from_ref(&shouted)),
        "ci.yml",
        &["build"],
        &variant,
    );
    assert_eq!((got.len(), skipped.len()), (1, 0));
    // What is no requirement: nothing is declared, the project scope stays,
    // and the notice names the input. Without a known step it points at the
    // declaration itself.
    let unresolved = |d: &Declaration, code: &str, value: &str| {
        (
            Scope::project(""),
            vec![],
            vec![skipped_of(d, code, value, &d.location)],
        )
    };
    // A key the variant lacks.
    assert_eq!(
        task_of(&read, "ci.yml", &["build"], &no_value),
        unresolved(&reference, "unresolved_expression", "${{ matrix.node }}")
    );
    // A value that is no version, or is an expression again.
    for value in ["a\nb", "${{ matrix.other }}"] {
        let bad = |_: &Scope, _: &str, _: &str| Some(value.to_string());
        assert_eq!(
            task_of(&read, "ci.yml", &["build"], &bad),
            unresolved(&reference, "unresolved_expression", "${{ matrix.node }}"),
            "{value}"
        );
    }
    // Any other expression is not looked up at all.
    let any = |_: &Scope, _: &str, _: &str| Some("18.0.0".to_string());
    let other = step_decl("node", "${{ matrix.node || 20 }}", "ci.yml", "build", 0);
    assert_eq!(
        task_of(
            &read_of(std::slice::from_ref(&other)),
            "ci.yml",
            &["build"],
            &any
        ),
        unresolved(&other, "unresolved_expression", "${{ matrix.node || 20 }}")
    );
    // A version file whose path is an expression names no file to read.
    let mut file = reference.clone();
    file.constraint = Constraint::VersionFileExpression("${{ matrix.file }}".into());
    assert_eq!(
        task_of(
            &read_of(std::slice::from_ref(&file)),
            "ci.yml",
            &["build"],
            &any
        ),
        unresolved(&file, "version_file_expression", "${{ matrix.file }}")
    );
    // An expression of a job the task does not run does not concern it.
    assert_eq!(
        task_of(&read, "ci.yml", &["lint"], &no_value),
        (Scope::project(""), vec![], vec![])
    );
}

#[test]
fn a_step_under_a_condition_and_a_skipped_input_require_nothing() {
    let project = decl("22.0.0");
    let pinned = step_decl("node", "20.0.0", "ci.yml", "build", 0);
    // The version of the second step was read from a file: the notice still
    // points at the step in its workflow.
    let mut filed = step_decl("python", "3.12.0", "ci.yml", "build", 1);
    filed.location.path = ".python-version".into();
    let at = |line: usize| Location {
        path: "ci.yml".into(),
        first_line: line,
        last_line: line,
    };
    let reader_skipped = |workflow: &str, job: &str| Skipped {
        tool: ToolId::parse("go").unwrap(),
        scope: step_scope(workflow, job, 2),
        code: "structured_version_file".into(),
        value: "go.mod".into(),
        location: at(31),
        step: at(30),
    };
    let read = SetupRead {
        declarations: vec![project.clone(), pinned.clone(), filed.clone()],
        skipped: vec![
            reader_skipped("ci.yml", "build"),
            reader_skipped("ci.yml", "other"),
            reader_skipped("other.yml", "build"),
        ],
        conditional: vec![pinned.scope.clone(), filed.scope.clone()],
        steps: vec![
            (step_scope("ci.yml", "other", 0), at(3)),
            (pinned.scope.clone(), at(10)),
            (filed.scope.clone(), at(20)),
        ],
    };
    let (scope, got, skipped) = task_of(&read, "ci.yml", &["build"], &no_value);
    // Nothing of the steps is required: the project's file is, as before.
    assert_eq!(scope, Scope::project(""));
    assert_eq!(got, [project.clone()]);
    assert_eq!(
        skipped,
        [
            reader_skipped("ci.yml", "build"),
            skipped_of(&pinned, "conditional_step", "20.0.0", &at(10)),
            skipped_of(&filed, "conditional_step", "3.12.0", &at(20)),
        ]
    );
    // The condition is the reason whatever the input is.
    let mut expression = pinned.clone();
    expression.constraint = Constraint::VersionFileExpression("${{ x }}".into());
    let conditional = SetupRead {
        declarations: vec![expression.clone()],
        conditional: vec![pinned.scope.clone()],
        ..Default::default()
    };
    let (_, _, skipped) = task_of(&conditional, "ci.yml", &["build"], &no_value);
    assert_eq!(skipped[0].code, "conditional_step");
    // The same step without the condition is a requirement, and stands before
    // the project's file.
    let unconditional = SetupRead {
        conditional: vec![filed.scope.clone()],
        ..read.clone()
    };
    let (scope, got, skipped) = task_of(&unconditional, "ci.yml", &["build"], &no_value);
    assert_eq!(scope, task_scope("ci.yml", "build"));
    assert_eq!(got, [narrowed(&pinned, "ci.yml", "build")]);
    assert_eq!(skipped.len(), 2);
    // The other job and the other workflow have their own skipped input only.
    let (_, _, skipped) = task_of(&read, "ci.yml", &["other"], &no_value);
    assert_eq!(skipped, [reader_skipped("ci.yml", "other")]);
    let (_, _, skipped) = task_of(&read, "other.yml", &["build"], &no_value);
    assert_eq!(skipped, [reader_skipped("other.yml", "build")]);
    let (_, _, skipped) = task_of(&read, "ci.yml", &["lint"], &no_value);
    assert_eq!(skipped, []);
}

#[test]
fn an_alias_of_a_setup_step_is_a_requirement_only_while_it_resolves() {
    let mut project = decl("1.80.0");
    project.tool = ToolId::parse("rust").unwrap();
    let rust = step_decl("rust", "stable", "ci.yml", "build", 0);
    let node = step_decl("node", "lts/*", "ci.yml", "build", 1);
    let step = Location {
        path: "ci.yml".into(),
        first_line: 40,
        last_line: 40,
    };
    let read = SetupRead {
        declarations: vec![project.clone(), rust.clone(), node.clone()],
        steps: vec![(rust.scope.clone(), step.clone())],
        ..Default::default()
    };
    let jobs = ["build".to_string()];
    let task = |unresolved: &dyn Fn(&ToolId, &str) -> bool| {
        task_declarations(&read, "", "ci.yml", &jobs, &no_value, unresolved)
    };
    // Resolved: both are requirements, and the step stands before the file.
    let (scope, got, skipped) = task(&resolved);
    assert_eq!(scope, task_scope("ci.yml", "build"));
    assert_eq!(
        got,
        [
            narrowed(&rust, "ci.yml", "build"),
            narrowed(&node, "ci.yml", "build")
        ]
    );
    assert_eq!(skipped, []);
    // `stable` of rust stays unresolved: the step requires nothing, the
    // project's file is the requirement again, and the other alias stays.
    let stable = |tool: &ToolId, alias: &str| tool.as_str() == "rust" && alias == "stable";
    let (scope, got, skipped) = task(&stable);
    assert_eq!(scope, task_scope("ci.yml", "build"));
    assert_eq!(got, [project.clone(), narrowed(&node, "ci.yml", "build")]);
    assert_eq!(
        skipped,
        [skipped_of(&rust, "unresolved_alias", "stable", &step)]
    );
    // Every alias unresolved: the task is frozen as the project is.
    let all = |_: &ToolId, _: &str| true;
    let (scope, got, skipped) = task(&all);
    assert_eq!(scope, Scope::project(""));
    assert_eq!(got, [project.clone()]);
    assert_eq!(skipped.len(), 2);
    assert_eq!(skipped[1].code, "unresolved_alias");
    assert_eq!(skipped[1].value, "lts/*");
    // A version is never asked about, and neither is a skipped input.
    let pinned = step_decl("node", "20.0.0", "ci.yml", "build", 0);
    let (_, got, skipped) = task_declarations(
        &read_of(std::slice::from_ref(&pinned)),
        "",
        "ci.yml",
        &jobs,
        &no_value,
        &all,
    );
    assert_eq!((got.len(), skipped.len()), (1, 0));
    // A reference whose value is an alias is asked about as the alias.
    let reference = step_decl("rust", "${{ matrix.rust }}", "ci.yml", "build", 0);
    let nightly = |_: &Scope, _: &str, _: &str| Some("nightly".to_string());
    let asked = |_: &ToolId, alias: &str| alias == "nightly";
    let (_, got, skipped) = task_declarations(
        &read_of(std::slice::from_ref(&reference)),
        "",
        "ci.yml",
        &jobs,
        &nightly,
        &asked,
    );
    assert_eq!(got, []);
    assert_eq!(skipped[0].code, "unresolved_alias");
    assert_eq!(skipped[0].value, "${{ matrix.rust }}");
}

/// The setup steps of one workflow `ci.yml`, read as the freeze reads them.
fn setup_read(workflow: &str, files: &[(&str, &str)]) -> SetupRead {
    let mut all = vec![("ci.yml", workflow)];
    all.extend_from_slice(files);
    let documents = docs(&all);
    let mut read = SetupRead::default();
    read_workflow(
        &documents["ci.yml"],
        &Scope::project(""),
        &documents,
        &mut read,
    )
    .unwrap();
    read
}
/// One job `a` with one setup step: `uses:` is line 4, its input line 6.
fn one_step(uses: &str, input: &str) -> String {
    let with = if input.is_empty() {
        String::new()
    } else {
        format!("        with:\n          {input}\n")
    };
    format!("jobs:\n  a:\n    steps:\n      - uses: {uses}\n{with}")
}
fn line(path: &str, line: usize) -> Location {
    Location {
        path: path.into(),
        first_line: line,
        last_line: line,
    }
}

#[test]
fn the_reader_records_each_setup_step_and_what_it_could_not_read() {
    let workflow = "jobs:\n  zeta:\n    steps:\n      - uses: actions/checkout@v4\n      - name: node\n        uses: actions/setup-node@v4\n        with:\n          node-version: 20.0.0\n  alpha:\n    steps:\n      - uses: actions/setup-python@v5\n        if: runner.os == 'Linux'\n        with:\n          python-version: pypy3.10\n      - uses: actions/setup-go@v5\n        with:\n          go-version-file: go.mod\n";
    let read = setup_read(workflow, &[("go.mod", "module x\n\ngo 1.22\n")]);
    // Jobs are read in the order they are written, so each step is found on
    // its own line: the step is its `uses:` line.
    let zeta = step_scope("ci.yml", "zeta", 1);
    let python = step_scope("ci.yml", "alpha", 0);
    let go = step_scope("ci.yml", "alpha", 1);
    assert_eq!(
        read.steps,
        [
            (zeta.clone(), line("ci.yml", 6)),
            (python.clone(), line("ci.yml", 11)),
            (go.clone(), line("ci.yml", 15)),
        ]
    );
    assert_eq!(read.conditional, [python.clone()]);
    assert_eq!(read.declarations.len(), 1);
    assert_eq!(read.declarations[0].scope, zeta);
    assert_eq!(read.declarations[0].location, line("ci.yml", 8));
    // A version belt does not read, and a file in a format of its own: each
    // with its own step and the line of its input.
    assert_eq!(
        read.skipped,
        [
            Skipped {
                tool: ToolId::parse("python").unwrap(),
                scope: python,
                code: "invalid_version".into(),
                value: "pypy3.10".into(),
                location: line("ci.yml", 14),
                step: line("ci.yml", 11),
            },
            Skipped {
                tool: ToolId::parse("go").unwrap(),
                scope: go,
                code: "structured_version_file".into(),
                value: "go.mod".into(),
                location: line("ci.yml", 17),
                step: line("ci.yml", 15),
            },
        ]
    );
    // A workflow that does not parse is the only error.
    let documents = docs(&[("ci.yml", "[bad")]);
    let mut read = SetupRead::default();
    let unread = read_workflow(
        &documents["ci.yml"],
        &Scope::project(""),
        &documents,
        &mut read,
    )
    .unwrap_err();
    assert_eq!(unread.code, "invalid_workflow");
    assert_eq!(read, SetupRead::default());
}

#[test]
fn a_version_belt_does_not_read_is_skipped_with_what_was_written() {
    for raw in ["pypy3.10", "3.13t", "oldstable", "graalpy-24"] {
        let workflow = one_step("actions/setup-python@v5", &format!("python-version: {raw}"));
        let read = setup_read(&workflow, &[]);
        assert_eq!(read.declarations, [], "{raw}");
        assert_eq!(read.skipped.len(), 1, "{raw}");
        assert_eq!(read.skipped[0].code, "invalid_version", "{raw}");
        assert_eq!(read.skipped[0].value, raw);
        assert_eq!(read.skipped[0].location, line("ci.yml", 6));
        assert_eq!(read.skipped[0].step, line("ci.yml", 4));
    }
    // What was written is cut at 80 characters.
    let long = "x".repeat(81);
    let workflow = one_step(
        "actions/setup-python@v5",
        &format!("python-version: {long}"),
    );
    assert_eq!(setup_read(&workflow, &[]).skipped[0].value, "x".repeat(80));
    let exact = "x".repeat(80);
    let workflow = one_step(
        "actions/setup-python@v5",
        &format!("python-version: {exact}"),
    );
    assert_eq!(setup_read(&workflow, &[]).skipped[0].value, exact);
}

#[test]
fn a_version_file_is_read_by_its_format_or_skipped() {
    let python = |file: &str| {
        one_step(
            "actions/setup-python@v5",
            &format!("python-version-file: {file}"),
        )
    };
    let one = |read: &SetupRead| {
        assert_eq!(read.skipped, [], "{read:?}");
        assert_eq!(read.declarations.len(), 1, "{read:?}");
        read.declarations[0].clone()
    };
    let skipped = |read: &SetupRead| {
        assert_eq!(read.declarations, [], "{read:?}");
        assert_eq!(read.skipped.len(), 1, "{read:?}");
        assert_eq!(read.skipped[0].step, line("ci.yml", 4));
        (
            read.skipped[0].code.clone(),
            read.skipped[0].location.clone(),
        )
    };
    let at_step = |code: &str| (code.to_string(), line("ci.yml", 6));
    // A plain version file: its content, at the file.
    let got = one(&setup_read(
        &python(".python-version"),
        &[(".python-version", "# pin\n3.12.1\n")],
    ));
    assert_eq!(got.tool.as_str(), "python");
    assert_eq!(got.constraint, Constraint::parse("3.12.1").unwrap());
    assert_eq!(got.location, line(".python-version", 1));
    assert_eq!(got.scope, step_scope("ci.yml", "a", 0));
    // Its content is no version: skipped, at the file.
    assert_eq!(
        skipped(&setup_read(
            &python(".python-version"),
            &[(".python-version", "pypy3.10\n")]
        )),
        ("invalid_version".to_string(), line(".python-version", 1))
    );
    // A file the source does not hold, and a path outside the project.
    assert_eq!(
        skipped(&setup_read(&python(".python-version"), &[])),
        at_step("missing_version_file")
    );
    assert_eq!(
        skipped(&setup_read(&python("../outside"), &[])),
        at_step("invalid_version_file")
    );
    // A path that is an expression waits for the run; half an expression is
    // nothing belt reads.
    let got = one(&setup_read(&python("${{ matrix.file }}"), &[]));
    assert_eq!(
        got.constraint,
        Constraint::VersionFileExpression("${{ matrix.file }}".into())
    );
    assert_eq!(got.location, line("ci.yml", 6));
    assert_eq!(
        skipped(&setup_read(&python("'${{ matrix.file'"), &[])),
        at_step("invalid_expression")
    );
    // A file in a format of its own is never read as a plain version.
    for file in [
        "go.mod",
        "go.work",
        "pyproject.toml",
        "global.json",
        "versions.yaml",
        "versions.yml",
        "poetry.lock",
        "pom.xml",
        "build.gradle",
        "build.gradle.kts",
    ] {
        let read = setup_read(&python(file), &[(file, "3.12.1\n")]);
        assert_eq!(skipped(&read), at_step("structured_version_file"), "{file}");
        assert_eq!(read.skipped[0].value, file);
    }
    // A format the project's own reader knows is read by it for the tool of
    // the step, at the root or in a directory.
    for file in [".tool-versions", "meta/.tool-versions"] {
        let content = "nodejs 20.1.0\npython 3.12.1\n";
        let got = one(&setup_read(&python(file), &[(file, content)]));
        assert_eq!(got.tool.as_str(), "python", "{file}");
        assert_eq!(got.constraint, Constraint::parse("3.12.1").unwrap());
        assert_eq!(got.location, line(file, 2));
        assert_eq!(got.scope, step_scope("ci.yml", "a", 0));
    }
    let got = one(&setup_read(
        &python("mise.toml"),
        &[("mise.toml", "[tools]\npython = '3.11.2'\n")],
    ));
    assert_eq!(got.constraint, Constraint::parse("3.11.2").unwrap());
    let rust = one_step(
        "actions-rust-lang/setup-rust-toolchain@v1",
        "toolchain-file: rust-toolchain.toml",
    );
    let got = one(&setup_read(
        &rust,
        &[("rust-toolchain.toml", "[toolchain]\nchannel = '1.80.0'\n")],
    ));
    assert_eq!(got.tool.as_str(), "rust");
    assert_eq!(got.constraint, Constraint::parse("1.80.0").unwrap());
    // It names no version of the tool: skipped at the step.
    assert_eq!(
        skipped(&setup_read(
            &python(".tool-versions"),
            &[(".tool-versions", "nodejs 20.1.0\n")]
        )),
        at_step("version_file_without_tool")
    );
    // It does not read: skipped where the reader says.
    assert_eq!(
        skipped(&setup_read(
            &python("meta/.tool-versions"),
            &[("meta/.tool-versions", "nodejs 20.1.0\npython\n")]
        )),
        (
            "missing_version".to_string(),
            line("meta/.tool-versions", 2)
        )
    );
    // Only the whole name is the format: another file ending the same way is
    // a plain version file.
    let got = one(&setup_read(
        &python("my.nvmrc"),
        &[("my.nvmrc", "3.12.1\n")],
    ));
    assert_eq!(got.tool.as_str(), "python");
    assert_eq!(got.location, line("my.nvmrc", 1));
    // setup-node keeps its own reading of a file, with its own errors.
    let node = one_step("actions/setup-node@v4", "node-version-file: package.json");
    let got = one(&setup_read(
        &node,
        &[("package.json", r#"{"engines":{"node":"20.1.0"}}"#)],
    ));
    assert_eq!(got.constraint, Constraint::parse("20.1.0").unwrap());
    assert_eq!(
        skipped(&setup_read(&node, &[("package.json", "{}")])),
        (
            "node_version_file_requires_resolution".to_string(),
            line("package.json", 1)
        )
    );
}

#[test]
fn the_reference_of_the_rust_toolchain_action_is_the_version_when_it_is_one() {
    let declared = |uses: &str, input: &str| {
        let read = setup_read(&one_step(uses, input), &[]);
        assert_eq!(read.skipped, [], "{uses}");
        assert_eq!(read.steps.len(), 1, "{uses}");
        let lines: Vec<String> = read
            .declarations
            .iter()
            .map(|d| {
                format!(
                    "{}={}@{}",
                    d.tool.as_str(),
                    d.constraint.describe(),
                    d.location.first_line
                )
            })
            .collect();
        lines.join(" ")
    };
    let range = |raw: &str| Constraint::parse(raw).unwrap().describe();
    assert_eq!(
        declared("dtolnay/rust-toolchain@1.89", ""),
        format!("rust={}@4", range("1.89"))
    );
    assert_eq!(
        declared("dtolnay/rust-toolchain@1.89.0", ""),
        "rust=1.89.0@4"
    );
    for channel in ["stable", "beta", "nightly"] {
        assert_eq!(
            declared(&format!("dtolnay/rust-toolchain@{channel}"), ""),
            format!("rust={channel}@4")
        );
    }
    // A branch, a tag of the action, a commit, and what is no version.
    for reference in ["master", "1", "v1.89", "1.2.3.4", "012345", "1.x9", "v1"] {
        assert_eq!(
            declared(&format!("dtolnay/rust-toolchain@{reference}"), ""),
            "",
            "{reference}"
        );
    }
    assert_eq!(declared("dtolnay/rust-toolchain", ""), "");
    // The `toolchain` input is the version when it is given (its line is
    // found by the input's name, which the action's own name holds too);
    // another input does not stand in for it.
    assert_eq!(
        declared("dtolnay/rust-toolchain@1.89.0", "toolchain: 1.70.0"),
        "rust=1.70.0@4"
    );
    assert_eq!(
        declared("dtolnay/rust-toolchain@1.89.0", "components: clippy"),
        "rust=1.89.0@4"
    );
    // The reference of another action is never a version.
    assert_eq!(declared("actions/setup-node@1.89.0", ""), "");
    assert_eq!(
        declared("actions-rust-lang/setup-rust-toolchain@1.89.0", ""),
        ""
    );
}

#[test]
fn sync_reads_every_step_and_only_skips_a_structured_version_file() {
    let go = one_step("actions/setup-go@v5", "go-version-file: go.mod");
    let bad = one_step("actions/setup-python@v5", "python-version: pypy3.10");
    let missing = one_step(
        "actions/setup-python@v5",
        "python-version-file: .python-version",
    );
    let names = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    // A structured file is skipped, and says so; nothing is declared from it.
    let read = read_setup(
        "",
        &docs(&[
            ("a.yml", &go),
            ("go.mod", "go 1.22\n"),
            (".node-version", "20.1.0"),
        ]),
        &names(&["a.yml"]),
    )
    .unwrap();
    assert_eq!(read.declarations, [decl("20.1.0")]);
    assert_eq!(read.skipped.len(), 1);
    assert_eq!(read.skipped[0].code, "structured_version_file");
    assert_eq!(read.skipped[0].step, line("a.yml", 4));
    assert_eq!(
        read_project(
            "",
            &docs(&[("a.yml", &go), ("go.mod", "go 1.22\n")]),
            &names(&["a.yml"])
        ),
        Ok(vec![])
    );
    // Anything else that is not read is the error it was, where it is written.
    let error = |workflows: &[(&str, &str)], order: &[&str]| {
        let mut all = workflows.to_vec();
        all.push(("go.mod", "go 1.22\n"));
        read_setup("", &docs(&all), &names(order)).unwrap_err()
    };
    let invalid = Diagnostic::at("invalid_version", line("b.yml", 6));
    assert_eq!(error(&[("b.yml", &bad)], &["b.yml"]), invalid);
    assert_eq!(
        error(&[("c.yml", &missing)], &["c.yml"]),
        Diagnostic::at("missing_version_file", line("c.yml", 6))
    );
    // The first workflow that is not read is the error, in the order given,
    // and a skipped structured file before it is none.
    let all = [
        ("a.yml", go.as_str()),
        ("b.yml", bad.as_str()),
        ("c.yml", missing.as_str()),
    ];
    assert_eq!(error(&all, &["a.yml", "b.yml", "c.yml"]), invalid);
    assert_eq!(
        error(&all, &["a.yml", "c.yml", "b.yml"]).code,
        "missing_version_file"
    );
    // A step under a condition is read as any other.
    let conditional = "jobs:\n  a:\n    steps:\n      - uses: actions/setup-node@v4\n        if: always()\n        with:\n          node-version: 20.1.0\n";
    let read = read_setup("", &docs(&[("a.yml", conditional)]), &names(&["a.yml"])).unwrap();
    assert_eq!(read.declarations.len(), 1);
    assert_eq!(read.conditional.len(), 1);
    // A workflow that is not among the documents, and one that does not parse.
    assert_eq!(
        read_setup("", &docs(&[("a.yml", &go)]), &names(&["z.yml"])).unwrap_err(),
        Diagnostic::at("missing_workflow", line("z.yml", 1))
    );
    assert_eq!(
        read_setup("", &docs(&[("a.yml", "[bad")]), &names(&["a.yml"]))
            .unwrap_err()
            .code,
        "invalid_workflow"
    );
    // Project files that contradict are an error once every workflow is read.
    let files = docs(&[
        (".node-version", "20.1.0"),
        (".nvmrc", "22.1.0"),
        ("b.yml", &bad),
    ]);
    assert_eq!(
        read_setup("", &files, &names(&["b.yml"])).unwrap_err().code,
        "invalid_version"
    );
    assert_eq!(
        read_setup("", &files, &[]).unwrap_err().code,
        "conflicting_versions"
    );
    assert_eq!(read_files("", &files).unwrap().len(), 2);
}

#[test]
fn the_freeze_asks_for_the_version_files_a_workflow_can_name() {
    let workflow = "jobs:\n  a:\n    steps:\n      - uses: actions/setup-python@v5\n        with:\n          python-version-file: meta/.python-version\n      - uses: actions/setup-go@v5\n        with:\n          go-version-file: ../outside\n      - uses: actions/setup-node@v4\n        with:\n          node-version-file: ${{ matrix.file }}\n      - uses: unknown/setup@v1\n        with:\n          node-version-file: ignored\n";
    let documents = docs(&[("ci.yml", workflow), ("bad.yml", "[bad")]);
    assert_eq!(
        setup_version_files(&documents["ci.yml"]),
        ["meta/.python-version"]
    );
    // A workflow that does not parse names none.
    assert!(setup_version_files(&documents["bad.yml"]).is_empty());
}
