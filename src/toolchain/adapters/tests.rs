use super::*;
use crate::toolchain::versions::{Declaration, Location, MatchPolicy, Scope};
use std::os::unix::fs::{symlink, PermissionsExt};
/// Install where the fixture put the root: its parent is the toolchain
/// directory. The tests of a root elsewhere name the directory themselves.
fn install_admitted(
    plan: &InstallPlan,
    executor: &mut impl AdmittedExecutor,
) -> Result<InstallationReceipt> {
    super::install_admitted(plan, plan.root.parent().unwrap(), executor)
}
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
    let temp = crate::test_tmp::tempdir().unwrap();
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
fn crashes_and_wrong_versions_never_publish_success() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let mut fake = Fake::for_plan(&f.plan);
    fake.wrong = true;
    assert_eq!(
        install_admitted(&f.plan, &mut fake),
        Err("installed_version_mismatch")
    );
    assert!(!f.plan.root.join("receipt.json").exists());
    assert_eq!(detect(&f.plan), Availability::Unknown(UNRECEIPTED));
    let f = fixture(AdapterKind::Fvm, "flutter");
    let mut fake = Fake::for_plan(&f.plan);
    fake.fail = true;
    assert_eq!(install_admitted(&f.plan, &mut fake), Err("fake_cancelled"));
    assert_eq!(detect(&f.plan), Availability::Unknown(UNRECEIPTED));
}
/// Everything below `root` except the lock, as sorted relative names. Links
/// are named, never entered.
fn listing(root: &Path) -> Vec<String> {
    let mut todo = vec![root.to_path_buf()];
    let mut out = vec![];
    while let Some(dir) = todo.pop() {
        for e in fs::read_dir(&dir).unwrap() {
            let e = e.unwrap();
            let m = fs::symlink_metadata(e.path()).unwrap();
            out.push(
                e.path()
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
            if m.is_dir() {
                todo.push(e.path());
            }
        }
    }
    out.retain(|n| n != "install.lock");
    out.sort();
    out
}
fn held_lock(plan: &InstallPlan) -> File {
    mkdir(&plan.root).unwrap();
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(plan.root.join("install.lock"))
        .unwrap();
    lock.lock().unwrap();
    lock
}
/// H1: an install that failed or was stopped leaves a root without a
/// receipt. The next install of the same version clears it and succeeds.
#[test]
fn a_failed_or_stopped_install_is_cleared_and_the_same_version_installs_again() {
    for stopped in [false, true] {
        let f = fixture(AdapterKind::Nodenv, "node");
        let mut fake = Fake::for_plan(&f.plan);
        if stopped {
            // Stopped at the first command: only the skeleton is there.
            fake.fail = true;
            assert_eq!(install_admitted(&f.plan, &mut fake), Err("fake_cancelled"));
        } else {
            // Ran to the end with the wrong version: a payload is there.
            fake.wrong = true;
            assert_eq!(
                install_admitted(&f.plan, &mut fake),
                Err("installed_version_mismatch")
            );
            assert!(f.plan.payload.exists());
        }
        // What a killed installer may leave: a half-written receipt, a
        // sealed directory, a file nobody may write.
        fs::write(f.plan.root.join("receipt.pending"), b"{").unwrap();
        let sealed = f.plan.root.join("cache/sealed/deep");
        fs::create_dir_all(&sealed).unwrap();
        fs::write(sealed.join("left"), b"partial").unwrap();
        fs::set_permissions(sealed.join("left"), fs::Permissions::from_mode(0o400)).unwrap();
        // Sealed as a read-only tree is (a module cache): it can be read,
        // not written. One nobody may read is another test.
        for dir in [&sealed, sealed.parent().unwrap()] {
            fs::set_permissions(dir, fs::Permissions::from_mode(0o500)).unwrap();
        }
        fs::write(f.plan.root.join("home/stale"), b"old").unwrap();
        let lock_inode = fs::metadata(f.plan.root.join("install.lock"))
            .unwrap()
            .ino();
        let receipt = install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
        assert_eq!(detect(&f.plan), Availability::Installed(Box::new(receipt)));
        // Nothing of the earlier installer is reused.
        assert!(!f.plan.root.join("receipt.pending").exists());
        assert!(!f.plan.root.join("cache/sealed").exists());
        assert!(!f.plan.root.join("home/stale").exists());
        assert!(trash(f.plan.root.parent().unwrap()).is_empty());
        // The lock is the same file: its inode is the exclusion.
        assert_eq!(
            fs::metadata(f.plan.root.join("install.lock"))
                .unwrap()
                .ino(),
            lock_inode
        );
    }
}
#[test]
fn clearing_a_partial_root_unlinks_links_and_never_follows_them() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap().to_path_buf();
    // Outside the root: a directory and files that must survive.
    let outside = state.join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("sentinel"), b"keep").unwrap();
    fs::create_dir(outside.join("sub")).unwrap();
    fs::write(outside.join("sub/sentinel"), b"keep").unwrap();
    let mut fake = Fake::for_plan(&f.plan);
    fake.fail = true;
    assert_eq!(install_admitted(&f.plan, &mut fake), Err("fake_cancelled"));
    // Links at the top of the root and deep inside it: to a directory, to a
    // file, to the root's own parent, to nothing.
    symlink(&outside, f.plan.root.join("link-to-dir")).unwrap();
    symlink(outside.join("sentinel"), f.plan.root.join("link-to-file")).unwrap();
    symlink(&state, f.plan.root.join("cache/up")).unwrap();
    fs::create_dir_all(f.plan.root.join("data/a/b")).unwrap();
    symlink(outside.join("sub"), f.plan.root.join("data/a/b/deep")).unwrap();
    symlink("/nonexistent/target", f.plan.root.join("dangling")).unwrap();
    // Hard links to files outside, one of them in a sealed directory: the
    // names go, the files and their modes stay.
    for (name, mode) in [("locked", 0o400), ("closed", 0o000)] {
        let file = outside.join(name);
        fs::write(&file, b"keep").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(mode)).unwrap();
    }
    fs::hard_link(outside.join("locked"), f.plan.root.join("cache/hard")).unwrap();
    let sealed = f.plan.root.join("data/sealed");
    fs::create_dir(&sealed).unwrap();
    fs::hard_link(outside.join("closed"), sealed.join("hard")).unwrap();
    fs::hard_link(outside.join("sentinel"), sealed.join("plain")).unwrap();
    fs::set_permissions(&sealed, fs::Permissions::from_mode(0o500)).unwrap();
    let before = listing(&outside);
    let host = listing(&state.join("host"));
    let modes = |p: &Path| fs::metadata(p).unwrap().mode();
    let (dir_mode, file_mode) = (modes(&outside), modes(&outside.join("sentinel")));
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    assert_eq!(listing(&outside), before);
    assert_eq!(listing(&state.join("host")), host);
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
    assert_eq!(fs::read(outside.join("sub/sentinel")).unwrap(), b"keep");
    assert_eq!(modes(&outside), dir_mode);
    assert_eq!(modes(&outside.join("sentinel")), file_mode);
    assert_eq!(modes(&outside.join("locked")) & 0o7777, 0o400);
    assert_eq!(modes(&outside.join("closed")) & 0o7777, 0o000);
    assert_eq!(fs::metadata(outside.join("sentinel")).unwrap().nlink(), 1);
    assert_eq!(fs::metadata(outside.join("locked")).unwrap().nlink(), 1);
    assert_eq!(fs::metadata(outside.join("closed")).unwrap().nlink(), 1);
    for gone in [
        "link-to-dir",
        "link-to-file",
        "cache/up",
        "data/a",
        "dangling",
        "cache/hard",
        "data/sealed",
    ] {
        assert!(
            fs::symlink_metadata(f.plan.root.join(gone)).is_err(),
            "{gone}"
        );
    }
}
#[test]
fn a_root_with_a_receipt_is_never_cleared_valid_or_not() {
    // A valid receipt: the answer as it is, nothing runs, nothing is removed.
    let f = fixture(AdapterKind::Nodenv, "node");
    let receipt = install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    let before = listing(&f.plan.root);
    assert!(before.len() > 2);
    let mut never = Fake::default();
    assert_eq!(install_admitted(&f.plan, &mut never).unwrap(), receipt);
    assert!(never.calls.is_empty());
    assert_eq!(listing(&f.plan.root), before);
    // A receipt that cannot be trusted (invalid, open to others, a link, a
    // directory): an error, and nothing is removed either.
    type Spoil = fn(&Path);
    let cases: [(&str, Spoil); 4] = [
        ("receipt_invalid", |r| fs::write(r, b"{}").unwrap()),
        ("unsafe_receipt", |r| {
            fs::set_permissions(r, fs::Permissions::from_mode(0o644)).unwrap()
        }),
        ("receipt_open", |r| {
            let kept = r.with_extension("kept");
            fs::rename(r, &kept).unwrap();
            symlink(&kept, r).unwrap()
        }),
        ("unsafe_receipt", |r| {
            fs::remove_file(r).unwrap();
            fs::create_dir(r).unwrap();
            fs::set_permissions(r, fs::Permissions::from_mode(0o700)).unwrap()
        }),
    ];
    for (error, spoil) in cases {
        let f = fixture(AdapterKind::Nodenv, "node");
        install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
        spoil(&f.plan.root.join("receipt.json"));
        let before = listing(&f.plan.root);
        let mut never = Fake::default();
        assert_eq!(install_admitted(&f.plan, &mut never), Err(error));
        assert!(never.calls.is_empty(), "{error}");
        assert_eq!(listing(&f.plan.root), before, "{error}");
    }
}
/// What the check of a root says for the toolchain directory `toolchains`.
fn checked(plan: &InstallPlan, toolchains: &Path) -> Result<()> {
    managed_root(plan, toolchains).map(|_| ())
}
/// Check the root, then clear it, as an installer holding the lock does.
fn cleared(plan: &InstallPlan, toolchains: &Path) -> Result<()> {
    clear_unreceipted(&managed_root(plan, toolchains)?)
}
/// The trash directories in the toolchain directory.
fn trash(state: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(state)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with(".trash-"))
        .collect();
    names.sort();
    names
}
#[test]
fn a_root_that_is_not_where_a_managed_root_must_be_is_refused_untouched() {
    // The root is a link to a directory elsewhere.
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap().to_path_buf();
    let elsewhere = state.join("elsewhere");
    mkdir(&elsewhere).unwrap();
    fs::write(elsewhere.join("sentinel"), b"keep").unwrap();
    symlink(&elsewhere, &f.plan.root).unwrap();
    let mut never = Fake::default();
    assert_eq!(
        install_admitted(&f.plan, &mut never),
        Err("unsafe_private_directory")
    );
    assert!(never.calls.is_empty());
    assert_eq!(cleared(&f.plan, &state), Err("unsafe_private_directory"));
    assert_eq!(listing(&elsewhere), ["sentinel"]);
    fs::remove_file(&f.plan.root).unwrap();
    // The check itself. Each root below holds something; none of it goes.
    let placed = |root: PathBuf| InstallPlan {
        root,
        ..f.plan.clone()
    };
    mkdir(&f.plan.root).unwrap();
    assert_eq!(checked(&f.plan, &state), Ok(()));
    fs::write(f.plan.root.join("left"), b"partial").unwrap();
    // Not named by a plan identity: too short, too long, not lower hex.
    let name = f
        .plan
        .root
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(name.len(), 64);
    for bad in [
        name[1..].to_owned(),
        format!("{name}0"),
        format!("g{}", &name[1..]),
        format!("A{}", &name[1..]),
        format!("/{}", &name[1..]).replace('/', ":"),
    ] {
        let other = state.join(&bad);
        mkdir(&other).unwrap();
        fs::write(other.join("left"), b"partial").unwrap();
        assert_eq!(
            checked(&placed(other.clone()), &state),
            Err("private_root_misplaced"),
            "{bad}"
        );
        assert_eq!(
            cleared(&placed(other.clone()), &state),
            Err("private_root_misplaced")
        );
        assert!(other.join("left").exists(), "{bad}");
    }
    // Reached through a link: the same directory by another path. It is not
    // in the expected directory, and a link is no toolchain directory either.
    let alias = state.join("alias");
    symlink(&state, &alias).unwrap();
    let through = placed(alias.join(&name));
    assert_eq!(checked(&through, &state), Err("private_root_misplaced"));
    assert_eq!(checked(&through, &alias), Err("unsafe_private_directory"));
    assert_eq!(cleared(&through, &alias), Err("unsafe_private_directory"));
    assert!(f.plan.root.join("left").exists());
    // Outside the expected toolchain directory. Every directory looks right
    // where it is named (owned, private, a plan identity), and the root is
    // still refused: only the directory the caller names holds roots.
    let deeper = state.join("deeper");
    mkdir(&deeper).unwrap();
    let outside = placed(deeper.join(&name));
    mkdir(&outside.root).unwrap();
    fs::write(outside.root.join("left"), b"partial").unwrap();
    assert_eq!(checked(&outside, &state), Err("private_root_misplaced"));
    assert_eq!(cleared(&outside, &state), Err("private_root_misplaced"));
    let mut never = Fake::default();
    assert_eq!(
        super::install_admitted(&outside, &state, &mut never),
        Err("private_root_misplaced")
    );
    assert!(never.calls.is_empty());
    assert_eq!(listing(&outside.root), ["left"]);
    assert!(!outside.root.join("install.lock").exists());
    // Nothing is made for a root outside that is not there yet.
    let first = if name.starts_with("0") { "1" } else { "0" };
    let absent = placed(deeper.join(format!("{first}{}", &name[1..])));
    assert_eq!(
        super::install_admitted(&absent, &state, &mut never),
        Err("private_root_misplaced")
    );
    assert!(!absent.root.exists());
    // The expected directory is a parent of the parent, or a sibling.
    assert_eq!(
        checked(&outside, state.parent().unwrap()),
        Err("private_root_misplaced")
    );
    assert_eq!(checked(&f.plan, &deeper), Err("private_root_misplaced"));
    // The link is further up: the path is not the one the root really has.
    let far = placed(alias.join("deeper").join(&name));
    assert_eq!(
        checked(&far, &alias.join("deeper")),
        Err("private_root_misplaced")
    );
    assert_eq!(
        cleared(&far, &alias.join("deeper")),
        Err("private_root_misplaced")
    );
    assert!(outside.root.join("left").exists());
    // Not a clean absolute path, or no parent and no name.
    for bad in [
        PathBuf::from(&name),
        state.join("x").join("..").join(&name),
        PathBuf::from("/"),
    ] {
        let parent = bad.parent().unwrap_or(&state).to_path_buf();
        assert_eq!(
            checked(&placed(bad.clone()), &parent),
            Err("private_root_misplaced"),
            "{}",
            bad.display()
        );
    }
    // A parent others may enter, then a root others may enter.
    fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(checked(&f.plan, &state), Err("unsafe_private_directory"));
    assert_eq!(cleared(&f.plan, &state), Err("unsafe_private_directory"));
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&f.plan.root, fs::Permissions::from_mode(0o750)).unwrap();
    assert_eq!(cleared(&f.plan, &state), Err("unsafe_private_directory"));
    fs::set_permissions(&f.plan.root, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(f.plan.root.join("left").exists());
    // A receipt that is there is never removed, and stops the removal
    // before anything else moves.
    fs::write(f.plan.root.join("receipt.json"), b"{}").unwrap();
    assert_eq!(
        cleared(&f.plan, &state),
        Err("installation_receipt_present")
    );
    assert_eq!(listing(&f.plan.root), ["left", "receipt.json"]);
    assert!(trash(&state).is_empty());
    fs::remove_file(f.plan.root.join("receipt.json")).unwrap();
    // And the plain case: everything but the lock goes, trash included.
    fs::write(f.plan.root.join("install.lock"), b"").unwrap();
    assert_eq!(cleared(&f.plan, &state), Ok(()));
    assert!(listing(&f.plan.root).is_empty());
    assert!(f.plan.root.join("install.lock").exists());
    assert!(trash(&state).is_empty());
    // A root that holds only the lock: nothing to do, no trash is made.
    assert_eq!(cleared(&f.plan, &state), Ok(()));
    assert!(trash(&state).is_empty());
}
/// The check and the removal are tied by descriptors: a root (or its parent
/// entry) that is replaced by a link after the check sends the removal
/// nowhere else.
#[test]
fn a_root_replaced_by_a_link_after_the_check_sends_the_removal_nowhere_else() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap().to_path_buf();
    let victim = state.join("victim");
    fs::create_dir_all(victim.join("sub")).unwrap();
    fs::write(victim.join("sub/sentinel"), b"keep").unwrap();
    fs::write(victim.join("sentinel"), b"keep").unwrap();
    mkdir(&f.plan.root).unwrap();
    fs::create_dir(f.plan.root.join("home")).unwrap();
    fs::write(f.plan.root.join("home/partial"), b"partial").unwrap();
    let managed = managed_root(&f.plan, &state).unwrap();
    // After the check: the checked directory is moved aside and its name is
    // a link to a directory that must survive.
    let aside = state.join("aside");
    fs::rename(&f.plan.root, &aside).unwrap();
    symlink(&victim, &f.plan.root).unwrap();
    let before = listing(&victim);
    assert_eq!(clear_unreceipted(&managed), Ok(()));
    assert_eq!(listing(&victim), before);
    assert_eq!(fs::read(victim.join("sub/sentinel")).unwrap(), b"keep");
    // What was checked is what was emptied.
    assert!(listing(&aside).is_empty());
    assert!(trash(&state).is_empty());
    // The descriptor and the path must name the same directory: one that
    // was opened and then replaced at its path is not the checked one.
    let moved = state.join("moved");
    fs::create_dir(&moved).unwrap();
    fs::set_permissions(&moved, fs::Permissions::from_mode(0o700)).unwrap();
    let opened = open_dir_at(CWD, &moved).unwrap();
    assert!(owned_open_dir(&opened, &moved).is_ok());
    fs::rename(&moved, state.join("moved-aside")).unwrap();
    assert_eq!(
        owned_open_dir(&opened, &moved).map(|_| ()),
        Err("private_directory_missing")
    );
    fs::create_dir(&moved).unwrap();
    fs::set_permissions(&moved, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        owned_open_dir(&opened, &moved).map(|_| ()),
        Err("private_root_misplaced")
    );
    assert!(owned_open_dir(&opened, &state.join("moved-aside")).is_ok());
    fs::set_permissions(&moved, fs::Permissions::from_mode(0o750)).unwrap();
    let open_to_others = open_dir_at(CWD, &moved).unwrap();
    assert_eq!(
        owned_open_dir(&open_to_others, &moved).map(|_| ()),
        Err("unsafe_private_directory")
    );
    // The walk itself opens a directory without following: a name that was a
    // directory when it was looked at and is a link when it is opened is
    // refused, not entered.
    let dir = open_dir_at(CWD, &state).unwrap();
    assert!(open_dir_at(&dir, "victim").is_ok());
    let name = f.plan.root.file_name().unwrap();
    assert!(open_dir_at(&dir, name).is_err());
    symlink("victim/sentinel", state.join("to-file")).unwrap();
    assert!(open_dir_at(&dir, "to-file").is_err());
    // Removing the link by name unlinks it; the directory behind it stays.
    let dev = fstat(&dir).unwrap().st_dev;
    let link = CString::new(name.as_encoded_bytes()).unwrap();
    assert_eq!(remove_at(dir.as_fd(), &link, dev), Ok(()));
    assert!(fs::symlink_metadata(&f.plan.root).is_err());
    assert_eq!(listing(&victim), before);
}
/// Nothing on another device is entered. A mount cannot be made here, so the
/// walk is told the root is on another device than the tree really is.
#[test]
fn a_directory_on_another_device_stops_the_removal_before_anything_in_it_goes() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap().to_path_buf();
    let tree = state.join("tree");
    fs::create_dir_all(tree.join("a/b")).unwrap();
    fs::write(tree.join("a/b/file"), b"x").unwrap();
    fs::write(tree.join("file"), b"x").unwrap();
    let before = listing(&tree);
    let dir = open_dir_at(CWD, &state).unwrap();
    let dev = fstat(&dir).unwrap().st_dev;
    let name = CString::new("tree").unwrap();
    assert_eq!(remove_at(dir.as_fd(), &name, dev + 1), Err(Errno::XDEV));
    assert_eq!(listing(&tree), before);
    // A file is not entered: it is unlinked whatever device is expected.
    let file = CString::new("file").unwrap();
    let inner = open_dir_at(&dir, "tree").unwrap();
    assert_eq!(remove_at(inner.as_fd(), &file, dev + 1), Ok(()));
    assert!(!tree.join("file").exists());
    // On the device of the root everything goes.
    assert_eq!(remove_at(dir.as_fd(), &name, dev), Ok(()));
    assert!(!tree.exists());
    // Something that is not there is an error, not a success.
    assert_eq!(remove_at(dir.as_fd(), &name, dev), Err(Errno::NOENT));

    // The same through a clear: the entries are renamed aside first, so the
    // root is empty; the removal of the trash stops, the clear fails, and
    // the trash stays as it is for the next clear.
    mkdir(&f.plan.root).unwrap();
    fs::create_dir_all(f.plan.root.join("home/deep")).unwrap();
    fs::write(f.plan.root.join("home/deep/partial"), b"partial").unwrap();
    fs::write(f.plan.root.join("install.lock"), b"").unwrap();
    let mut managed = managed_root(&f.plan, &state).unwrap();
    managed.dev += 1;
    assert_eq!(clear_unreceipted(&managed), Err(OTHER_DEVICE));
    assert!(listing(&f.plan.root).is_empty());
    assert!(f.plan.root.join("install.lock").exists());
    let left = trash(&state);
    assert_eq!(left.len(), 1);
    assert_eq!(
        listing(&state.join(&left[0])),
        ["home", "home/deep", "home/deep/partial"]
    );
    // The next clear of the same root removes what was left.
    assert_eq!(cleared(&f.plan, &state), Ok(()));
    assert!(trash(&state).is_empty());
}
#[test]
fn trash_names_belong_to_one_root() {
    let root = "0123456789abcdef".repeat(4);
    assert_eq!(trash_prefix(&root), format!(".trash-{root}-"));
    assert_eq!(trash_owner(&format!(".trash-{root}-01J")), Some(&*root));
    assert_eq!(trash_owner(&format!(".trash-{root}-x")), Some(&*root));
    for bad in [
        format!(".trash-{root}-"),
        format!(".trash-{root}"),
        format!(".trash-{root}x1"),
        format!(".trash-{}-1", &root[1..]),
        format!(".trash-G{}-1", &root[1..]),
        format!("trash-{root}-1"),
        format!("{root}-1"),
        root.clone(),
        ".trash-".into(),
        String::new(),
    ] {
        assert_eq!(trash_owner(&bad), None, "{bad}");
    }
}
/// Trash that an earlier clear left (the installer died between the rename
/// and the removal) goes with the next clear of the same root, and at the
/// start of the worker. Trash of a root an installer works in is left alone.
#[test]
fn trash_left_behind_goes_with_the_next_clear_and_at_the_start() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap().to_path_buf();
    let name = f
        .plan
        .root
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let first = if name.starts_with("0") { "1" } else { "0" };
    let other = format!("{first}{}", &name[1..]);
    let fill = |dir: &Path| {
        fs::create_dir_all(dir.join("sealed/deep")).unwrap();
        fs::write(dir.join("sealed/deep/left"), b"partial").unwrap();
        fs::set_permissions(dir.join("sealed"), fs::Permissions::from_mode(0o500)).unwrap();
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
    };
    let mine = state.join(format!(".trash-{name}-01A"));
    let theirs = state.join(format!(".trash-{other}-01B"));
    let unrelated = state.join(".trash-not-a-root");
    for dir in [&mine, &theirs, &unrelated] {
        fill(dir);
    }
    // A clear of this root removes its own trash and no other, also when the
    // root itself holds nothing.
    mkdir(&f.plan.root).unwrap();
    assert_eq!(cleared(&f.plan, &state), Ok(()));
    assert!(!mine.exists());
    assert!(theirs.exists() && unrelated.exists());
    // An install does the same.
    fill(&mine);
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    assert!(!mine.exists());
    assert!(theirs.exists() && unrelated.exists());

    // The start of the worker. `theirs` has no root: nobody clears it now.
    fill(&mine);
    let held = held_lock(&f.plan);
    sweep_trash(&state);
    assert!(mine.exists(), "an installer holds the lock of this root");
    assert!(!theirs.exists());
    assert!(unrelated.exists(), "not a trash name: never touched");
    assert!(f.plan.root.join("receipt.json").exists());
    drop(held);
    sweep_trash(&state);
    assert!(!mine.exists());
    assert!(unrelated.exists());
    assert!(f.plan.root.join("receipt.json").exists());
    // A root without a lock file, or with one that cannot be trusted, is
    // not swept at the start: the lock is what says nobody works there.
    let bare = state.join(&other);
    mkdir(&bare).unwrap();
    fill(&theirs);
    sweep_trash(&state);
    assert!(theirs.exists());
    assert!(!bare.join("install.lock").exists(), "nothing is created");
    fs::write(bare.join("install.lock"), b"").unwrap();
    fs::set_permissions(bare.join("install.lock"), fs::Permissions::from_mode(0o644)).unwrap();
    sweep_trash(&state);
    assert!(theirs.exists());
    fs::set_permissions(bare.join("install.lock"), fs::Permissions::from_mode(0o600)).unwrap();
    sweep_trash(&state);
    assert!(!theirs.exists());
    // A toolchain directory that is not there, or not private: nothing.
    sweep_trash(&state.join("absent"));
    fill(&mine);
    fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();
    sweep_trash(&state);
    assert!(mine.exists());
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
}
/// A rename that fails is the failure, and nothing is removed. A directory
/// that cannot be read is never opened up by its path: empty it goes, with
/// something in it the removal stops.
#[test]
fn a_clear_that_cannot_rename_removes_nothing_and_an_unreadable_directory_is_not_forced() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let state = f.plan.root.parent().unwrap().to_path_buf();
    mkdir(&f.plan.root).unwrap();
    fs::create_dir_all(f.plan.root.join("home/deep")).unwrap();
    fs::write(f.plan.root.join("home/deep/partial"), b"partial").unwrap();
    let before = listing(&f.plan.root);
    // No trash directory can be made: nothing in the root moves.
    fs::set_permissions(&state, fs::Permissions::from_mode(0o500)).unwrap();
    assert_eq!(
        cleared(&f.plan, &state),
        Err("installation_partial_cleanup")
    );
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(listing(&f.plan.root), before);
    assert!(trash(&state).is_empty());

    // An empty directory nobody may read goes as it is.
    let empty = f.plan.root.join("home/deep/empty");
    fs::create_dir(&empty).unwrap();
    fs::set_permissions(&empty, fs::Permissions::from_mode(0o000)).unwrap();
    assert_eq!(cleared(&f.plan, &state), Ok(()));
    assert!(listing(&f.plan.root).is_empty());
    assert!(trash(&state).is_empty());

    // One with something in it: the root is empty afterwards (the rename
    // came first), so the clear is done; the trash keeps the directory with
    // its mode, and no later clear or install is held up by it.
    let closed = f.plan.root.join("closed");
    fs::create_dir(&closed).unwrap();
    fs::write(closed.join("left"), b"partial").unwrap();
    fs::set_permissions(&closed, fs::Permissions::from_mode(0o300)).unwrap();
    assert_eq!(cleared(&f.plan, &state), Ok(()));
    assert!(listing(&f.plan.root).is_empty());
    let left = trash(&state);
    assert_eq!(left.len(), 1);
    let kept = state.join(&left[0]).join("closed");
    assert_eq!(fs::metadata(&kept).unwrap().mode() & 0o7777, 0o300);
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    assert_eq!(trash(&state), left);
    // So that the fixture can be removed.
    fs::set_permissions(&kept, fs::Permissions::from_mode(0o700)).unwrap();
}
#[test]
fn while_another_installer_holds_the_lock_nothing_is_removed_and_this_one_waits() {
    // The other installer holds the lock over a root it is filling.
    let f = fixture(AdapterKind::Rustup, "rust");
    let lock = held_lock(&f.plan);
    mkdir(&f.plan.root.join("home")).unwrap();
    fs::write(f.plan.root.join("home/in-progress"), b"theirs").unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let plan = f.plan.clone();
    let waiter = std::thread::spawn(move || {
        let done = install_admitted(&plan, &mut Fake::for_plan(&plan));
        let _ = tx.send(());
        done
    });
    // It does not come back and removes nothing while the lock is held.
    assert!(rx
        .recv_timeout(std::time::Duration::from_millis(300))
        .is_err());
    assert_eq!(
        fs::read(f.plan.root.join("home/in-progress")).unwrap(),
        b"theirs"
    );
    // The other one ends without a receipt: now this one clears and installs.
    drop(lock);
    let receipt = waiter.join().unwrap().unwrap();
    assert!(!f.plan.root.join("home/in-progress").exists());
    assert_eq!(detect(&f.plan), Availability::Installed(Box::new(receipt)));
    // The other one publishes while this one waits: its receipt is the
    // answer, and nothing runs here.
    let g = fixture(AdapterKind::Rustup, "rust");
    let lock = held_lock(&g.plan);
    let plan = g.plan.clone();
    let waiter = std::thread::spawn(move || {
        let mut never = Fake::default();
        let done = install_admitted(&plan, &mut never);
        (done, never.calls.len())
    });
    let held = managed_root(&g.plan, g.plan.root.parent().unwrap()).unwrap();
    let published = install_holding(&g.plan, &held, &mut Fake::for_plan(&g.plan)).unwrap();
    drop(lock);
    let (done, calls) = waiter.join().unwrap();
    assert_eq!(done.unwrap(), published);
    assert_eq!(calls, 0);
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
fn ruby_patchlevel_and_go_two_component_probe_versions_normalize() {
    let v = |s: &str| Version::parse(s).unwrap();
    assert_eq!(
        output_version("ruby", b"ruby 3.1.2p20 (2022-04-12 revision 4491bb740a)").unwrap(),
        v("3.1.2")
    );
    assert_eq!(
        output_version("ruby", b"ruby 3.3.0 (2023-12-25)").unwrap(),
        v("3.3.0")
    );
    assert_eq!(
        output_version("go", b"go version go1.20 darwin/arm64").unwrap(),
        v("1.20.0")
    );
    assert_eq!(
        output_version("go", b"go version go1.21.5 darwin/arm64").unwrap(),
        v("1.21.5")
    );
    for (tool, bytes) in [
        ("ruby", &b"ruby 3.1p20 x"[..]),
        ("go", b"go version go1 x"),
        ("go", b"go version go1.x x"),
        ("node", b"v1.2"),
    ] {
        assert!(output_version(tool, bytes).is_err(), "{tool} {bytes:?}");
    }
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

#[test]
fn go_probe_and_consumer_cannot_switch_the_frozen_toolchain() {
    let f = fixture(AdapterKind::Mise, "go");
    let (_, probe) = &f.plan.probes[0];
    assert_eq!(probe.program.file_name().unwrap(), "go");
    assert_eq!(probe.argv, vec!["version"]);
    assert_eq!(probe.env["GOTOOLCHAIN"], "local");
    let mut fake = Fake::for_plan(&f.plan);
    let receipt = install_admitted(&f.plan, &mut fake).unwrap();
    assert_eq!(fake.calls.last().unwrap().argv, vec!["version"]);
    assert_eq!(fake.calls.last().unwrap().env["GOTOOLCHAIN"], "local");
    let mut expected = None;
    for setting in [None, Some("auto"), Some("go1.99.0+auto"), Some("go1.99.0")] {
        let mut env = base_env(&f.plan);
        if let Some(value) = setting {
            env.insert("GOTOOLCHAIN".into(), value.into());
        }
        let activation = activate(
            &f.plan,
            &receipt,
            Digest::of_bytes(b"base"),
            &env,
            Digest::of_bytes(b"policy"),
            &f.shell,
        )
        .unwrap();
        assert_eq!(activation.environment["GOTOOLCHAIN"], "local");
        assert_eq!(activation.environment["HOME"], env["HOME"]);
        if let Some(previous) = &expected {
            assert_eq!(previous, &activation.execution_fingerprint);
        } else {
            expected = Some(activation.execution_fingerprint);
        }
    }
    // A different observed Go version still cannot publish a valid receipt.
    let wrong = fixture(AdapterKind::Mise, "go");
    let mut fake = Fake::for_plan(&wrong.plan);
    fake.wrong = true;
    assert_eq!(
        install_admitted(&wrong.plan, &mut fake),
        Err("installed_version_mismatch")
    );
    assert!(!wrong.plan.root.join("receipt.json").exists());
    assert!(matches!(detect(&wrong.plan), Availability::Unknown(_)));
}

#[test]
fn preparation_probe_bounded_identity_detects_growth_race_and_never_follows_links() {
    let temp = crate::test_tmp::tempdir().unwrap();
    let path = temp.path().join("manager");
    fs::write(&path, b"small").unwrap();
    let before = FileIdentity::capture(&path).unwrap();
    assert_eq!(FileIdentity::capture_bounded(&path, 5).unwrap(), before);
    assert_eq!(
        FileIdentity::capture_bounded(&path, 4),
        Err("identity_not_bounded_regular")
    );
    assert_eq!(
        FileIdentity::capture_bounded_checked(&path, 8, || {
            fs::write(&path, vec![b'x'; 1024 * 1024]).unwrap();
        }),
        Err("identity_too_large")
    );
    let link = temp.path().join("link");
    symlink(&path, &link).unwrap();
    assert_eq!(
        FileIdentity::capture_bounded(&link, 8),
        Err("identity_open")
    );
    assert!(FileIdentity::capture_bounded(&temp.path().join("absent"), 8).is_err());
}

#[test]
fn preparation_probe_total_fd_byte_budget_is_shared_across_files() {
    let temp = crate::test_tmp::tempdir().unwrap();
    let path = fs::canonicalize(temp.path()).unwrap().join("manager");
    fs::write(&path, vec![b'x'; 1024 * 1024]).unwrap();
    let budget = IdentityReadBudget::limited(1024 * 1024, 8 * 1024 * 1024);
    for _ in 0..7 {
        assert!(budget.capture(&path).is_ok());
    }
    // Reserve a growth sentinel; never begin an eighth full read at the boundary.
    assert!(budget.capture(&path).is_err());
    assert_eq!(budget.remaining.get(), 1024 * 1024);
}

#[test]
fn preparation_probe_complete_installation_matches_real_activation_and_detects_drift() {
    for (kind, tool) in [
        (AdapterKind::Nodenv, "node"),
        (AdapterKind::Rustup, "rust"),
        (AdapterKind::Mise, "python"),
        (AdapterKind::Fvm, "flutter"),
        (AdapterKind::Corepack, "pnpm"),
    ] {
        let f = fixture(kind, tool);
        let receipt = install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
        let budget = IdentityReadBudget::probe();
        let verified = inspect_installation(f.plan.clone(), &budget).unwrap();
        assert_eq!(verified.digest(), hash(&(&f.plan.key(), &receipt.digest())));
        let base = Digest::of_bytes(b"actual-base");
        let policy = Digest::of_bytes(b"actual-setup");
        let actual = activate_set(
            &f.plan.frozen,
            &[(&f.plan, &receipt)],
            base.clone(),
            &BTreeMap::new(),
            &base_env(&f.plan),
            policy.clone(),
            &f.shell,
        )
        .unwrap();
        let sampled = prospective_activation(
            &f.plan.frozen,
            &[verified],
            &[],
            base,
            &BTreeMap::new(),
            &base_env(&f.plan),
            policy,
            &f.shell.path,
            &budget,
        )
        .unwrap();
        assert_eq!(sampled, actual);
        budget.finish().unwrap();
        // Same-size bytes and restored mtime still fail complete content checks.
        let file = &f.plan.probes[0].1.program;
        let prior = fs::metadata(file).unwrap();
        fs::write(file, vec![b'x'; prior.len() as usize]).unwrap();
        File::options()
            .write(true)
            .open(file)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(prior.modified().unwrap()))
            .unwrap();
        assert_eq!(
            fs::metadata(file).unwrap().modified().unwrap(),
            prior.modified().unwrap()
        );
        let changed = IdentityReadBudget::probe();
        assert!(inspect_installation(f.plan.clone(), &changed).is_err());
        assert!(budget.finish().is_err());
    }
}
#[test]
fn preparation_probe_complete_installation_never_accepts_partial_unsafe_or_replaced_tree() {
    for change in 0..9 {
        let f = fixture(AdapterKind::Nodenv, "node");
        install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
        let path = &f.plan.probes[0].1.program;
        match change {
            0 => fs::remove_file(path).unwrap(),
            1 => fs::write(f.plan.payload.join("unexpected"), b"extra").unwrap(),
            2 => {
                fs::set_permissions(path, fs::Permissions::from_mode(0o722)).unwrap();
            }
            3 => {
                fs::hard_link(path, f.plan.payload.join("hardlink")).unwrap();
            }
            4 => {
                fs::remove_file(path).unwrap();
                symlink(&f.shell.path, path).unwrap();
            }
            5 => {
                fs::write(f.plan.root.join("receipt.json"), b"{}").unwrap();
            }
            6 => {
                fs::set_permissions(
                    f.plan.root.join("receipt.json"),
                    fs::Permissions::from_mode(0o644),
                )
                .unwrap();
            }
            7 => {
                fs::set_permissions(&f.plan.root, fs::Permissions::from_mode(0o755)).unwrap();
            }
            _ => {
                fs::remove_dir_all(&f.plan.payload).unwrap();
                fs::create_dir_all(&f.plan.payload).unwrap();
            }
        }
        assert!(
            inspect_installation(f.plan, &IdentityReadBudget::probe()).is_err(),
            "change {change}"
        );
    }
    let f = fixture(AdapterKind::Nodenv, "node");
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    let b = IdentityReadBudget::probe();
    inspect_installation(f.plan.clone(), &b).unwrap();
    let old = f.plan.root.with_extension("old");
    fs::rename(&f.plan.root, &old).unwrap();
    fs::rename(&old, &f.plan.root).unwrap();
    assert!(b.finish().is_err());
}
#[test]
fn preparation_probe_complete_budgets_receipt_directory_and_content() {
    let f = fixture(AdapterKind::Nodenv, "node");
    let receipt = install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    assert!(inspect_installation(f.plan.clone(), &IdentityReadBudget::limited(4, 10)).is_err());
    fs::write(
        f.plan.root.join("receipt.json"),
        vec![b' '; 1024 * 1024 + 1],
    )
    .unwrap();
    assert!(inspect_installation(f.plan.clone(), &IdentityReadBudget::probe()).is_err());
    // Compact tree vector flooding is refused while deserializing, before a
    // complete candidate can allocate an unbounded inventory.
    let entries = vec!["/small"; 1025];
    let mut value = serde_json::to_value(receipt).unwrap();
    value["tree"]["directories"] = serde_json::to_value(entries).unwrap();
    fs::write(
        f.plan.root.join("receipt.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    assert!(inspect_installation(f.plan, &IdentityReadBudget::probe()).is_err());
}

#[test]
fn preparation_probe_directory_flood_and_receipt_replacement_never_publish_partial_proof() {
    let f = fixture(AdapterKind::Nodenv, "node");
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    let sampled = IdentityReadBudget::probe();
    inspect_installation(f.plan.clone(), &sampled).unwrap();
    let receipt = f.plan.root.join("receipt.json");
    let old = f.plan.root.join("receipt.old");
    fs::rename(&receipt, &old).unwrap();
    fs::copy(&old, &receipt).unwrap();
    fs::set_permissions(&receipt, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(sampled.finish().is_err());
    for i in 0..1025 {
        fs::write(f.plan.payload.join(format!("extra-{i}")), b"x").unwrap();
    }
    let flooded = IdentityReadBudget::probe();
    assert!(inspect_installation(f.plan, &flooded).is_err());
    assert!(flooded.snapshot.borrow().paths_len() <= 256);
    assert!(flooded.snapshot.borrow().visits() <= 1024);
}

#[test]
fn preparation_probe_activation_cannot_mix_verification_queries() {
    let f = fixture(AdapterKind::Nodenv, "node");
    install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    let first = IdentityReadBudget::probe();
    let verified = inspect_installation(f.plan.clone(), &first).unwrap();
    let second = IdentityReadBudget::probe();
    assert_eq!(
        prospective_activation(
            &f.plan.frozen,
            &[verified],
            &[],
            Digest::of_bytes(b"base"),
            &BTreeMap::new(),
            &base_env(&f.plan),
            Digest::of_bytes(b"policy"),
            &f.shell.path,
            &second
        ),
        Err("probe_verification_query_mismatch")
    );
}
/// An adapter cannot move the worker's task-traffic-controller scope for the
/// commands that run under its activation.
#[test]
fn an_activation_never_replaces_the_xdg_scope_of_the_worker() {
    let mut f = fixture(AdapterKind::Corepack, "pnpm");
    for var in ["XDG_STATE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME"] {
        f.plan.activation_env.insert(var.into(), "/adapter".into());
    }
    let mut fake = Fake::for_plan(&f.plan);
    let receipt = install_admitted(&f.plan, &mut fake).unwrap();
    let mut base = base_env(&f.plan);
    base.insert("XDG_STATE_HOME".into(), "/private/worker-state".into());
    let activation = activate(
        &f.plan,
        &receipt,
        Digest::of_bytes(b"base"),
        &base,
        Digest::of_bytes(b"setup"),
        &f.shell,
    )
    .unwrap();
    assert_eq!(
        activation.environment["XDG_STATE_HOME"],
        base["XDG_STATE_HOME"]
    );
    assert_eq!(
        activation.environment["XDG_CONFIG_HOME"],
        base["XDG_CONFIG_HOME"]
    );
    assert!(!activation.environment.contains_key("XDG_DATA_HOME"));
}
/// The comparison a launch and a recovery make (`current != binding.activation`,
/// "accepted execution binding changed"), across the change that made a
/// worker carry `XDG_STATE_HOME` and hand both scope variables to every
/// command: the activation is made from the worker's toolchain environment
/// and its fingerprint, neither of which holds a scope variable, so a binding
/// accepted before is the one computed after. The command still gets the
/// worker's present scope.
#[test]
fn an_accepted_binding_stays_valid_when_the_worker_gains_scope_variables() {
    use crate::toolchain::contract::ExecutionBinding;
    use crate::toolchain::profile::Profile;
    let f = fixture(AdapterKind::Corepack, "pnpm");
    let mut fake = Fake::for_plan(&f.plan);
    let receipt = install_admitted(&f.plan, &mut fake).unwrap();
    let state = crate::test_tmp::tempdir().unwrap();
    let mut before = base_env(&f.plan);
    before.remove("XDG_CONFIG_HOME");
    let mut after = before.clone();
    after.insert("XDG_STATE_HOME".into(), "/private/worker-state".into());
    after.insert("XDG_CONFIG_HOME".into(), "/private/worker-config".into());
    let activation_of = |worker: &BTreeMap<String, String>| {
        let profile = Profile::load(state.path(), worker).unwrap();
        let activation = activate(
            &f.plan,
            &receipt,
            profile.startup_report().profile_fingerprint,
            profile.environment(),
            Digest::of_bytes(b"setup"),
            &f.shell,
        )
        .unwrap();
        (profile, activation)
    };
    let (_, accepted) = activation_of(&before);
    let (profile, current) = activation_of(&after);
    assert_eq!(current, accepted);
    assert!(!accepted.environment.contains_key("XDG_STATE_HOME"));
    assert!(!accepted.environment.contains_key("XDG_CONFIG_HOME"));
    // What a command of the accepted binding is started with now.
    let binding = ExecutionBinding {
        activation: accepted,
    };
    let env = profile
        .command_environment(Some(&binding), &BTreeMap::new())
        .unwrap();
    assert_eq!(env["XDG_STATE_HOME"], "/private/worker-state");
    assert_eq!(env["XDG_CONFIG_HOME"], "/private/worker-config");
    assert_eq!(env["PATH"], binding.activation.environment["PATH"]);
    // An ordinary variable is part of the binding as before.
    let mut other = before.clone();
    other.insert("ORDINARY".into(), "x".into());
    assert_ne!(activation_of(&other).1, binding.activation);
}

#[test]
fn a_native_selection_adds_no_private_path_and_its_identity_is_the_receipt() {
    use crate::toolchain::contract::NativeTool;
    use crate::toolchain::versions::VersionObservation;
    let f = fixture(AdapterKind::Nodenv, "node");
    let frozen = &f.plan.frozen;
    let env = base_env(&f.plan);
    let base = Digest::of_bytes(b"base");
    let policy = Digest::of_bytes(b"policy");
    let native = |tool: &ToolId, version: &str, identity: &[u8]| NativeTool {
        tool: tool.clone(),
        version: Version::parse(version).unwrap(),
        identity: Digest::of_bytes(identity),
    };
    let seen =
        |version: &str| BTreeMap::from([(f.plan.tool.clone(), VersionObservation::parse(version))]);
    let activate = |native: &[NativeTool], observed: &BTreeMap<_, _>| {
        activate_with_native(
            frozen,
            &[],
            native,
            base.clone(),
            observed,
            &env,
            policy.clone(),
            &f.shell,
        )
    };
    let node = &f.plan.tool;
    let first = activate(&[native(node, "1.2.3", b"a")], &seen("1.2.3")).unwrap();
    // The attempt uses the tool of the profile path; nothing is put before it.
    assert_eq!(first.environment, env);
    assert_eq!(
        first.installation,
        hash(&vec![(node.clone(), Digest::of_bytes(b"a"))])
    );
    // The same version from another executable is another execution identity.
    let replaced = activate(&[native(node, "1.2.3", b"b")], &seen("1.2.3")).unwrap();
    assert_ne!(replaced.installation, first.installation);
    assert_ne!(replaced.binding_digest, first.binding_digest);
    assert_ne!(replaced.execution_fingerprint, first.execution_fingerprint);
    // The profile has to report the selected version when it is activated.
    for observed in [seen("1.2.4"), BTreeMap::new()] {
        assert_eq!(
            activate(&[native(node, "1.2.3", b"a")], &observed),
            Err("activation_native_mismatch")
        );
    }
    // A native tool stands only for its own selection, at its version, once.
    let other = ToolId::parse("go").unwrap();
    for (tools, observed) in [
        (vec![native(node, "1.2.4", b"a")], seen("1.2.4")),
        (
            vec![native(&other, "1.2.3", b"a")],
            BTreeMap::from([(other.clone(), VersionObservation::parse("1.2.3"))]),
        ),
    ] {
        assert_eq!(
            activate(&tools, &observed),
            Err("activation_native_mismatch")
        );
    }
    for tools in [
        vec![],
        vec![native(node, "1.2.3", b"a"), native(node, "1.2.3", b"a")],
    ] {
        assert_eq!(
            activate(&tools, &seen("1.2.3")),
            Err("complete_activation_set_required")
        );
    }
    // A managed install and a native tool cannot both claim one selection.
    let receipt = install_admitted(&f.plan, &mut Fake::for_plan(&f.plan)).unwrap();
    assert_eq!(
        activate_with_native(
            frozen,
            &[(&f.plan, &receipt)],
            &[native(node, "1.2.3", b"a")],
            base.clone(),
            &seen("1.2.3"),
            &env,
            policy.clone(),
            &f.shell,
        ),
        Err("complete_activation_set_required")
    );
}

/// An install whose directory also gets `ships`, as a real one arrives with
/// more programs than the tool itself (the receipt covers them).
struct Shipping<'a> {
    inner: Fake,
    bin: PathBuf,
    ships: &'a [&'a str],
}
impl AdmittedExecutor for Shipping<'_> {
    fn execute(&mut self, command: &CommandSpec) -> Result<CommandOutput> {
        let output = self.inner.execute(command)?;
        for program in self.ships {
            executable(&self.bin.join(program), b"shipped with the managed tool");
        }
        Ok(output)
    }
}
/// One contract of two pins: `managed` installed by its adapter (with `ships`
/// beside it), `native` served by the profile. Returns the contract, the
/// managed plan and its receipt.
fn managed_beside_native(
    f: &Fixture,
    kind: AdapterKind,
    managed: &str,
    native: &str,
    ships: &[&str],
) -> (FrozenToolchains, InstallPlan, InstallationReceipt) {
    let state = f.plan.root.parent().unwrap();
    let scope = Scope::project("");
    let declarations = [managed, native].map(|tool| Declaration {
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
    let managed = evidence(managed);
    let frozen = resolve(&resolve(&initial, &managed).unwrap(), &evidence(native)).unwrap();
    let host = HostTools {
        manager: f.plan.external[0].clone(),
        dependencies: f.plan.external[1..].to_vec(),
        search_path: vec![f.plan.external[0].path.parent().unwrap().into()],
        rust_host: "aarch64-apple-darwin".into(),
    };
    let plan = plan(kind, &frozen, &managed.tool, &managed, state, &host).unwrap();
    let receipt = install_admitted(
        &plan,
        &mut Shipping {
            inner: Fake::for_plan(&plan),
            bin: plan.activation_bin.clone(),
            ships,
        },
    )
    .unwrap();
    (frozen, plan, receipt)
}

/// Every managed directory goes before the profile path, where the native
/// tool was measured. A managed install that ships a program of a native tool
/// would therefore run in its place: the activation is refused, and accepted
/// only while no managed directory holds such a program.
#[test]
fn a_managed_install_that_would_run_in_place_of_a_native_tool_is_refused() {
    use crate::toolchain::contract::NativeTool;
    use crate::toolchain::versions::VersionObservation;
    for (kind, managed, native, programs) in [
        // npm, npx and corepack come in the directory of a node install.
        (AdapterKind::Nodenv, "node", "npm", vec!["npm"]),
        // The flutter SDK has its own dart.
        (AdapterKind::Fvm, "flutter", "dart", vec!["dart"]),
        (AdapterKind::Nodenv, "node", "rust", vec!["rustc", "cargo"]),
        (AdapterKind::Nodenv, "node", "xcode", vec!["xcodebuild"]),
        // A tool no profile probe stands for is started by its own name.
        (AdapterKind::Nodenv, "node", "python", vec!["python"]),
    ] {
        let tool = ToolId::parse(native).unwrap();
        let served = [NativeTool {
            tool: tool.clone(),
            version: Version::parse("1.2.3").unwrap(),
            identity: Digest::of_bytes(b"measured on the profile path"),
        }];
        let activate = |f: &Fixture,
                        frozen: &FrozenToolchains,
                        plan: &InstallPlan,
                        receipt: &InstallationReceipt| {
            activate_with_native(
                frozen,
                &[(plan, receipt)],
                &served,
                Digest::of_bytes(b"base"),
                &BTreeMap::from([(tool.clone(), VersionObservation::parse("1.2.3"))]),
                &base_env(plan),
                Digest::of_bytes(b"policy"),
                &f.shell,
            )
        };
        // No overlap (npx and corepack are not npm): the managed directory
        // first, then the profile path, on which the native tool is the first
        // of its name.
        let f = fixture(kind, managed);
        let (frozen, plan, receipt) =
            managed_beside_native(&f, kind, managed, native, &["npx", "corepack"]);
        let accepted = activate(&f, &frozen, &plan, &receipt)
            .unwrap_or_else(|e| panic!("{managed}+{native}: {e}"));
        assert_eq!(
            accepted.environment["PATH"],
            format!(
                "{}:{}",
                text(&plan.activation_bin).unwrap(),
                base_env(&plan)["PATH"]
            ),
            "{managed}+{native}"
        );
        // Both stand in the identity, by tool, whichever way they are served.
        let mut receipts = vec![
            (plan.tool.clone(), receipt.digest()),
            (tool.clone(), served[0].identity.clone()),
        ];
        receipts.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(accepted.installation, hash(&receipts));
        assert_eq!(shadowed_native(&served, &[&plan]), None);
        // The read-only forecast of this activation is this activation.
        let budget = IdentityReadBudget::probe();
        let verified = inspect_installation(plan.clone(), &budget).unwrap();
        let forecast = |native: &[NativeTool]| {
            prospective_activation(
                &frozen,
                std::slice::from_ref(&verified),
                native,
                Digest::of_bytes(b"base"),
                &BTreeMap::from([(tool.clone(), VersionObservation::parse("1.2.3"))]),
                &base_env(&plan),
                Digest::of_bytes(b"policy"),
                &f.shell.path,
                &budget,
            )
        };
        assert_eq!(forecast(&served).as_ref(), Ok(&accepted));
        assert_eq!(forecast(&[]), Err("complete_activation_set_required"));
        for program in programs {
            // What a shell would not start does not stand in the way.
            let shipped = plan.activation_bin.join(program);
            fs::create_dir(&shipped).unwrap();
            assert_eq!(shadowed_native(&served, &[&plan]), None, "{program}");
            fs::remove_dir(&shipped).unwrap();
            fs::write(&shipped, b"data").unwrap();
            fs::set_permissions(&shipped, fs::Permissions::from_mode(0o600)).unwrap();
            assert_eq!(shadowed_native(&served, &[&plan]), None, "{program}");
            fs::remove_file(&shipped).unwrap();
            // An install that came with the program: it would be the one run.
            let f = fixture(kind, managed);
            let (frozen, plan, receipt) =
                managed_beside_native(&f, kind, managed, native, &[program]);
            assert_eq!(
                activate(&f, &frozen, &plan, &receipt),
                Err("activation_native_mismatch"),
                "{managed} ships {program}"
            );
            let said = shadowed_native(&served, &[&plan]).unwrap();
            assert!(
                said.contains(&format!("native {native} 1.2.3"))
                    && said.contains(&format!("managed {managed} install has its own {program}")),
                "{said}"
            );
            // Under the managed tool's own selection the program is that
            // tool: with nothing native, nothing is refused.
            assert_eq!(shadowed_native(&[], &[&plan]), None);
        }
    }
}
