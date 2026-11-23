use super::*;

const INSTANCE: &str = "01J0000000000000000000000";

fn env() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("HOME".into(), "/home/u".into()),
        ("PATH".into(), "/usr/bin".into()),
        ("TOKEN".into(), "secret".into()),
    ])
}

fn plan_with(
    env: &BTreeMap<String, String>,
    adjust: impl FnOnce(&mut Request<'_>, &mut Host<'_>),
) -> Result<Plan, String> {
    let cluster = ClusterId::generate().to_string();
    let key = PathBuf::from("/secrets/cluster.key");
    let exe = PathBuf::from("/opt/belt");
    let mut request = Request {
        cluster: &cluster,
        name: "colab",
        key_file: &key,
    };
    let mut host = Host {
        env,
        executable: &exe,
        os_boot: "0f0f0f0f-aaaa-bbbb-cccc-000000000000",
        instance: INSTANCE,
        resident_journal: false,
    };
    adjust(&mut request, &mut host);
    plan(&request, &host)
}

#[test]
fn a_clean_host_gets_a_worker_with_a_fresh_identity() {
    let env = env();
    let p = plan_with(&env, |_, _| {}).unwrap();
    assert_eq!(p.spec.worker_id.as_str(), "colab");
    assert_eq!(p.spec.schema, 1);
    assert_eq!(p.spec.state_dir, PathBuf::from("/home/u/.belt"));
    assert_eq!(
        p.spec.cluster_key_path,
        PathBuf::from("/secrets/cluster.key")
    );
    assert_eq!(p.spec.executable, PathBuf::from("/opt/belt"));
    assert_eq!(p.spec.config, "schema_version = 1\n");
    assert_eq!(
        p.boot_id.as_str(),
        format!("0f0f0f0f-aaaa-bbbb-cccc-000000000000.{INSTANCE}")
    );
    // Only the allowlisted environment is carried over.
    assert_eq!(
        p.spec.env.keys().map(String::as_str).collect::<Vec<_>>(),
        ["HOME", "PATH"]
    );
}

#[test]
fn two_starts_never_share_an_identity() {
    let env = env();
    let a = plan_with(&env, |_, h| h.instance = "a1").unwrap();
    let b = plan_with(&env, |_, h| h.instance = "b2").unwrap();
    assert_ne!(a.boot_id, b.boot_id);
    assert_eq!(a.spec.worker_id, b.spec.worker_id);
}

#[test]
fn the_start_identity_needs_an_instance_and_a_valid_name() {
    assert!(boot_id("boot", "").is_err());
    assert!(boot_id("boot", "i").is_ok());
    assert!(boot_id("bad boot", "i").is_err());
    assert!(boot_id("boot", &"x".repeat(200)).is_err());
}

#[test]
fn a_resident_is_never_taken_over() {
    let env = env();
    let resident = plan_with(&env, |_, h| h.resident_journal = true)
        .err()
        .unwrap();
    assert!(
        resident.contains("persistent worker is installed"),
        "{resident}"
    );
}

fn apfs() -> Filesystem {
    Filesystem::Name("apfs".into())
}

#[test]
fn only_the_listed_local_filesystems_are_local() {
    for magic in [
        0xEF53u64,
        0x5846_5342,
        0x9123_683E,
        0x2FC1_2FC1,
        0x0102_1994,
        0x794C_7630,
    ] {
        assert!(local_filesystem(&Filesystem::Magic(magic)), "{magic:#x}");
    }
    // NFS, SMB, CIFS, 9p, FUSE (virtiofs reports the same), and unknown ones.
    for magic in [
        0x6969u64,
        0x517B,
        0xFF53_4D42,
        0x0102_1997,
        0x6573_5546,
        0,
        0xEF54,
    ] {
        assert!(!local_filesystem(&Filesystem::Magic(magic)), "{magic:#x}");
    }
    for name in ["apfs", "hfs"] {
        assert!(local_filesystem(&Filesystem::Name(name.into())), "{name}");
    }
    for name in [
        "nfs", "smbfs", "webdav", "macfuse", "virtiofs", "exfat", "", "APFS", "apfs2",
    ] {
        assert!(!local_filesystem(&Filesystem::Name(name.into())), "{name}");
    }
    assert!(!local_filesystem(&Filesystem::Unknown));
}

#[test]
fn a_state_left_behind_is_examined_by_marker_then_filesystem_then_resident() {
    assert_eq!(examined(&Marker::Ephemeral, &apfs(), false), Ok(()));
    assert_eq!(
        examined(&Marker::Ephemeral, &Filesystem::Magic(0xEF53), false),
        Ok(())
    );
    let absent = examined(&Marker::Absent, &apfs(), false).unwrap_err();
    assert!(absent.contains("no ephemeral.json"), "{absent}");
    assert!(absent.contains("starts clean"), "{absent}");
    let unreadable = examined(&Marker::Unreadable, &apfs(), false).unwrap_err();
    assert!(unreadable.contains("cannot be read"), "{unreadable}");
    let other = examined(&Marker::OtherKind, &apfs(), false).unwrap_err();
    assert!(
        other.contains("not of the ephemeral worker kind"),
        "{other}"
    );
    for filesystem in [
        Filesystem::Magic(0x6969),
        Filesystem::Name("nfs".into()),
        Filesystem::Unknown,
    ] {
        let remote = examined(&Marker::Ephemeral, &filesystem, false).unwrap_err();
        assert!(remote.contains("not on a local filesystem"), "{remote}");
    }
    let resident = examined(&Marker::Ephemeral, &apfs(), true).unwrap_err();
    assert!(
        resident.contains("persistent worker is installed"),
        "{resident}"
    );
    // The first condition that fails is the one named.
    let first = examined(&Marker::Absent, &Filesystem::Unknown, true).unwrap_err();
    assert!(first.contains("no ephemeral.json"), "{first}");
    let second = examined(&Marker::Ephemeral, &Filesystem::Unknown, true).unwrap_err();
    assert!(second.contains("not on a local filesystem"), "{second}");
}

#[test]
fn a_state_is_unused_only_with_both_locks_taken() {
    assert_eq!(unused(true, true), Ok(()));
    let running = unused(false, true).unwrap_err();
    assert!(running.contains("ephemeral.lock is held"), "{running}");
    let agent = unused(true, false).unwrap_err();
    assert!(agent.contains("agent.lock is held"), "{agent}");
    let both = unused(false, false).unwrap_err();
    assert!(both.contains("ephemeral.lock is held"), "{both}");
}

#[test]
fn bad_input_is_refused() {
    let env = env();
    assert!(plan_with(&env, |r, _| r.cluster = "nope").is_err());
    assert!(plan_with(&env, |r, _| r.name = "bad name").is_err());
    assert!(plan_with(&env, |r, _| r.name = "").is_err());
    assert!(plan_with(&env, |r, _| r.key_file = Path::new("key")).is_err());
    assert!(plan_with(&env, |r, _| r.key_file = Path::new("/k\0")).is_err());
    let mut relative_home = env.clone();
    relative_home.insert("HOME".into(), "home".into());
    assert!(plan_with(&relative_home, |_, _| {}).is_err());
    let mut no_home = env.clone();
    no_home.remove("HOME");
    assert!(plan_with(&no_home, |_, _| {}).is_err());
    assert!(plan_with(&env, |_, h| h.instance = "").is_err());
}

#[test]
fn state_is_removed_only_when_nothing_is_live() {
    assert!(removable(Some(0)));
    assert!(!removable(Some(1)));
    assert!(!removable(None));
}

#[test]
fn the_worker_ends_itself_only_after_the_notice_with_nothing_live_and_nothing_to_collect() {
    assert!(finished(true, Some(0), Some(0)));
    assert!(
        !finished(true, Some(1), Some(0)),
        "an attempt of any member is live"
    );
    assert!(
        !finished(true, Some(0), Some(1)),
        "a member has a result to collect"
    );
    assert!(!finished(true, Some(1), Some(1)));
    assert!(
        !finished(true, None, Some(0)),
        "the count could not be read"
    );
    assert!(
        !finished(true, Some(0), None),
        "the count could not be read"
    );
    assert!(
        !finished(false, Some(0), Some(0)),
        "nobody retired this identity"
    );
    assert!(!finished(false, Some(2), Some(0)));
    assert!(!finished(false, None, None));
}

#[test]
fn the_stop_bound_is_the_grace_plus_the_kill_wait_and_a_margin_capped_at_a_day() {
    assert_eq!(stop_bound_ms(10_000), 20_000);
    assert_eq!(stop_bound_ms(0), 10_000);
    assert_eq!(stop_bound_ms(86_400_000 - 10_000), 86_400_000);
    assert_eq!(stop_bound_ms(86_400_000 - 9_999), 86_400_000);
    assert_eq!(stop_bound_ms(u64::MAX), 86_400_000);
}

#[test]
fn only_a_fully_read_empty_worker_is_quiet() {
    assert!(quiet(Some(0), Some(0)));
    assert!(!quiet(Some(1), Some(0)));
    assert!(!quiet(Some(0), Some(1)));
    assert!(!quiet(None, Some(0)));
    assert!(!quiet(Some(0), None));
}

#[test]
fn two_quiet_looks_after_the_begin_settle_and_nothing_else_does() {
    use StopStep::*;
    assert_eq!(stop_step(true, true, true, false), (Settled, true));
    assert_eq!(stop_step(true, true, true, true), (Settled, true));
    // The first quiet look waits for a second.
    assert_eq!(stop_step(true, true, false, false), (Wait, true));
    // Something alive resets the pair.
    assert_eq!(stop_step(true, false, true, false), (Wait, false));
    // A look begun before the stop says nothing, even quiet, even after one.
    assert_eq!(stop_step(false, true, true, false), (Wait, false));
    assert_eq!(stop_step(false, true, false, false), (Wait, false));
    // The bound gives up unless the pair is complete.
    assert_eq!(stop_step(true, false, false, true), (GiveUp, false));
    assert_eq!(stop_step(true, true, false, true), (GiveUp, true));
    assert_eq!(stop_step(false, true, true, true), (GiveUp, false));
}

#[test]
fn a_count_taken_before_the_stop_cannot_settle_an_attempt_accepted_after_it() {
    use StopStep::*;
    // The interleaving: a look spawned before the marker returns zero, an
    // attempt is accepted, the looks after the marker see it.
    let mut before = false;
    let mut next = |after_begin, quiet, bound| {
        let (step, q) = stop_step(after_begin, quiet, before, bound);
        before = q;
        step
    };
    assert_eq!(next(false, true, false), Wait);
    assert_eq!(next(false, true, false), Wait);
    assert_eq!(next(true, false, false), Wait);
    assert_eq!(next(true, true, false), Wait);
    assert_eq!(next(true, true, false), Settled);
}
