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
        state_exists: false,
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
fn a_resident_or_existing_state_is_never_taken_over() {
    let env = env();
    let resident = plan_with(&env, |_, h| h.resident_journal = true)
        .err()
        .unwrap();
    assert!(
        resident.contains("persistent worker is installed"),
        "{resident}"
    );
    let leftover = plan_with(&env, |_, h| h.state_exists = true).err().unwrap();
    assert!(leftover.contains("starts clean"), "{leftover}");
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
