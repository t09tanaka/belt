//! The system service definition belongs to the administrator. The SSH
//! helper owns the worker input, version link and journal as the service user.
use super::*;
use macos::{AccountLookup, InstallStartError, Receipt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::sync::Arc;

fn passwd(uid: u32) -> anyhow::Result<(String, String)> {
    ensure!(uid != 0, "the worker execution account must be non-root");
    let mut buffer = vec![0u8; 65536];
    let mut record = std::mem::MaybeUninit::uninit();
    let mut result = std::ptr::null_mut();
    // The strings are copied before the caller-owned buffer is dropped.
    let rc = unsafe {
        libc::getpwuid_r(
            uid,
            record.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    ensure!(
        rc == 0 && !result.is_null(),
        "cannot resolve service UID {uid}"
    );
    let record = unsafe { record.assume_init() };
    let name = unsafe { std::ffi::CStr::from_ptr(record.pw_name) }
        .to_str()?
        .to_owned();
    let home = unsafe { std::ffi::CStr::from_ptr(record.pw_dir) }
        .to_str()?
        .to_owned();
    ensure!(
        policy::valid_system_account(uid, &name),
        "unsupported non-root service account name"
    );
    Ok((name, home))
}

pub(super) fn current_account() -> anyhow::Result<String> {
    Ok(passwd(euid())?.0)
}

pub(super) fn plan_for(
    backend: policy::Backend,
    config: &Path,
    username: &str,
    uid: u32,
) -> anyhow::Result<policy::Plan> {
    let (name, home) = passwd(uid)?;
    ensure!(name == username, "system service account changed");
    let path = format!("{home}/.cargo/bin:{home}/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin");
    let env = BTreeMap::from([("HOME".into(), home), ("PATH".into(), path)]);
    service_plan(
        backend,
        config,
        policy::Mode::SystemUser {
            uid,
            username: name,
        },
        config.join("bin/current").join(BINARY),
        env,
    )
}

fn directory(backend: policy::Backend) -> &'static Path {
    Path::new(match backend {
        policy::Backend::Launchd => "/Library/LaunchDaemons",
        policy::Backend::Systemd => "/etc/systemd/system",
    })
}

fn verify_directory(directory: &Path, owner: u32) -> anyhow::Result<()> {
    for path in directory.ancestors() {
        let m = fs::symlink_metadata(path)?;
        ensure!(
            m.is_dir() && m.uid() == owner && m.mode() & 0o022 == 0,
            "unsafe system service directory: {}",
            path.display()
        );
    }
    Ok(())
}

fn read_definition(path: &Path, owner: u32, plan: &policy::Plan) -> anyhow::Result<Receipt> {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let m = file.metadata()?;
    ensure!(
        m.is_file()
            && m.nlink() == 1
            && m.uid() == owner
            && m.mode() & 0o022 == 0
            && m.len() <= LIMIT,
        "unsafe system service definition {}",
        path.display()
    );
    let mut bytes = Vec::new();
    (&mut file).take(LIMIT + 1).read_to_end(&mut bytes)?;
    ensure!(
        plan.installed_as(&bytes),
        "administrator-managed system service differs from the requested worker plan"
    );
    Ok(Receipt::system(
        path.to_owned(),
        &m,
        Sha256::digest(&bytes).into(),
    ))
}

#[cfg(test)]
thread_local! {
    static FIXTURE_DIRECTORY: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn with_fixture_directory<T>(root: &Path, run: impl FnOnce() -> T) -> T {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            FIXTURE_DIRECTORY.with_borrow_mut(|slot| *slot = None);
        }
    }
    FIXTURE_DIRECTORY.with_borrow_mut(|slot| {
        assert!(slot.is_none());
        *slot = Some(root.to_owned());
    });
    let _restore = Restore;
    run()
}

fn receipt(plan: &policy::Plan) -> anyhow::Result<Receipt> {
    #[cfg(test)]
    if let Some(root) = FIXTURE_DIRECTORY.with_borrow(Clone::clone) {
        return read_definition(&root.join(plan.filename()), euid(), plan);
    }
    let root = directory(plan.backend());
    let path = root.join(plan.filename());
    verify_directory(root, 0).and_then(|()| read_definition(&path, 0, plan)).with_context(|| format!("check the administrator registration at {}; on the worker host use register-system-worker with --account and --config-dir {}", path.display(), plan.working_directory().display()))
}

fn config_under_home(config: &Path, home: &Path) -> anyhow::Result<PathBuf> {
    let config = fs::canonicalize(config).with_context(|| {
        format!(
            "cannot resolve worker configuration directory {}; it must exist",
            config.display()
        )
    })?;
    let home = fs::canonicalize(home)
        .with_context(|| format!("cannot resolve execution account's home {}", home.display()))?;
    ensure!(
        config.starts_with(&home)
            && config
                .file_name()
                .is_some_and(|name| name == crate::config::paths::USER_DIR),
        "give the worker configuration directory under the execution account's home"
    );
    Ok(config)
}

/// Only an administrator-installed copy of the CLI should be used for this
/// operation. It renders the unit from explicit inputs and the OS account
/// database. It never reads a service definition from the worker account.
fn register(username: &str, config: &Path) -> anyhow::Result<PathBuf> {
    ensure!(
        euid() == 0,
        "system registration requires an administrator running a trusted copy of belt"
    );
    let account = macos::NativeAccounts
        .lookup(username)
        .map_err(anyhow::Error::msg)?
        .context("unknown execution account")?;
    let backend = HostLauncher::native().backend();
    let (_, home) = passwd(account.uid)?;
    let config = config_under_home(config, Path::new(&home))?;
    let plan = plan_for(backend, &config, username, account.uid)?;
    let root = directory(backend);
    verify_directory(root, 0)?;
    publish_definition(root, &plan, 0)
}

pub(crate) fn command(username: &str, config: &Path, json: bool) -> i32 {
    let result = register(username, config).map_err(|e| {
        crate::error::ApiError::new(crate::error::ErrorCode::Conflict, format!("{e:#}"))
    });
    let path = result
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    super::super::workers::emit(
        json,
        result.map(|path| serde_json::json!({"path": path})),
        || path,
    )
}

pub(super) fn publish_definition(
    root: &Path,
    plan: &policy::Plan,
    owner: u32,
) -> anyhow::Result<PathBuf> {
    let path = root.join(plan.filename());
    let mut temp = crate::platform::owned_temp::file_in(root)?;
    temp.write_all(plan.plist().as_bytes())?;
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o644))?;
    temp.as_file().sync_all()?;
    match temp.persist_noclobber(&path) {
        Ok(_) => File::open(root)?.sync_all()?,
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.error.into()),
    }
    read_definition(&path, owner, plan)?;
    Ok(path)
}

pub(super) enum ServiceFiles {
    User(DirectoryFiles),
    System(Box<policy::Plan>),
}

impl ServiceFiles {
    pub(super) fn install_journaled(
        &mut self,
        plan: &policy::Plan,
        record: impl FnOnce(&Receipt, &Path) -> anyhow::Result<()>,
    ) -> anyhow::Result<Receipt> {
        match self {
            Self::User(files) => Ok(files.install_journaled(plan, record)?),
            Self::System(_) => {
                let receipt = receipt(plan)?;
                record(&receipt, receipt.path())?;
                Ok(receipt)
            }
        }
    }
}

impl ManagedFiles for ServiceFiles {
    type LifecycleLease = Option<Arc<macos::DirectoryLock>>;
    fn lifecycle_lock(&mut self) -> anyhow::Result<Self::LifecycleLease> {
        match self {
            Self::User(files) => files.lifecycle_lock().map(Some),
            // The worker journal has its own switch lock. The helper never
            // changes the system directory, and re-verifies the unit on use.
            Self::System(_) => Ok(None),
        }
    }
    fn install(&mut self, plan: &policy::Plan) -> Result<Receipt, InstallStartError> {
        match self {
            Self::User(files) => files.install(plan),
            Self::System(_) => receipt(plan).map_err(InstallStartError::without_receipt),
        }
    }
    fn verify(&mut self, expected: &Receipt) -> anyhow::Result<()> {
        match self {
            Self::User(files) => files.verify(expected),
            Self::System(plan) => {
                ensure!(
                    receipt(plan)? == *expected,
                    "system definition was replaced"
                );
                Ok(())
            }
        }
    }
    fn remove(&mut self, receipt: &Receipt) -> anyhow::Result<()> {
        match self {
            Self::User(files) => files.remove(receipt),
            Self::System(_) => anyhow::bail!("the system service belongs to the administrator"),
        }
    }
}

/// The system definition remains enabled across upgrades. After a power
/// loss it may run only a version for which input and registration have
/// already been committed. During an unfinished switch the drain is kept.
pub(crate) fn start(input: &Path) -> anyhow::Result<()> {
    use std::os::unix::process::CommandExt;
    let (name, home) = passwd(euid())?;
    let paths = input_paths(input, home, HostLauncher::native().backend())?;
    let pinned = check_boot(&paths, &name, &std::env::current_exe()?)?;
    // macOS current_exe preserves the invocation symlink. Enter the pinned
    // version directly, so the foreground worker and its supervisor retain
    // an immutable path even across a later replacement of bin/current.
    Err(std::process::Command::new(&pinned)
        .args(["worker", "--input"])
        .arg(input)
        .exec())
    .with_context(|| format!("start pinned worker {}", pinned.display()))
}

/// The paths of the worker whose input is `input`, for the account whose
/// home is `home`. The configuration directory is the one the input is in
/// (the administrator chose it; the environment of a system service does not
/// name it). Spec 25.4 (a): this is the start of that worker, so the
/// temporary files a killed command left in its directories are cleared
/// here.
fn input_paths(input: &Path, home: String, backend: policy::Backend) -> anyhow::Result<Paths> {
    let config = input.parent().context("worker input directory missing")?;
    let env = BTreeMap::from([
        ("HOME".into(), home),
        (
            "XDG_CONFIG_HOME".into(),
            config
                .parent()
                .context("config home missing")?
                .to_str()
                .context("config home encoding")?
                .to_owned(),
        ),
    ]);
    let paths = Paths::resolve_for(&env, backend)?;
    ensure!(paths.input == input, "unexpected system worker input path");
    sweep_written_directories(&paths);
    Ok(paths)
}

pub(super) fn check_boot(
    paths: &Paths,
    username: &str,
    executable: &Path,
) -> anyhow::Result<PathBuf> {
    let journal = require_journal(paths)?;
    ensure!(
        journal.system_user.as_deref() == Some(username),
        "installed worker is not bound to this service account"
    );
    let plan = plan(paths, &journal)?;
    let observed = receipt(&plan)?;
    if let Some(expected) = &journal.receipt {
        ensure!(&observed == expected, "system definition was replaced");
    } else {
        // Initial administrator activation can precede the first receipt.
        // A switch, in contrast, may still be replacing inputs or restoring
        // the database. Its published receipt is the durable start boundary.
        ensure!(
            journal.switch.is_none(),
            "worker version switch has not published its receipt"
        );
    }
    let pinned = fs::canonicalize(paths.executable(&journal.binary_sha256))?;
    ensure!(
        pinned == fs::canonicalize(executable)?,
        "worker binary differs from the installed version"
    );
    Ok(pinned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The start of a system user's worker clears the temporary files of a
    /// gone owner in the directory its input is in, which is not the default
    /// configuration directory, and refuses an input that is somewhere else.
    #[test]
    fn the_start_sweeps_the_directory_of_the_input_it_was_given() {
        use crate::platform::owned_temp::testing::{dead_name, live_name};
        let root = crate::test_tmp::tempdir().unwrap();
        let home = fs::canonicalize(root.path()).unwrap();
        let config = home.join("chosen/belt");
        let default_config = home.join(".config/belt");
        for dir in [&config, &config.join("bin"), &default_config] {
            fs::create_dir_all(dir).unwrap();
        }
        let gone = [
            config.join(dead_name("jjjjjjjjjjj1")),
            config.join("bin").join(dead_name("jjjjjjjjjjj2")),
        ];
        let kept = [
            config.join(live_name("kkkkkkkkkkk1")),
            default_config.join(dead_name("kkkkkkkkkkk2")),
        ];
        for file in gone.iter().chain(&kept) {
            fs::write(file, b"x").unwrap();
        }
        let home_text = home.to_str().unwrap().to_string();
        let paths = input_paths(
            &config.join("worker.json"),
            home_text.clone(),
            policy::Backend::Launchd,
        )
        .unwrap();
        assert_eq!(paths.config, config);
        for file in &gone {
            assert!(!file.exists(), "{file:?}");
        }
        for file in &kept {
            assert!(file.exists(), "{file:?}");
        }
        assert!(input_paths(
            &config.join("other.json"),
            home_text,
            policy::Backend::Launchd
        )
        .is_err());
    }

    #[test]
    fn config_under_home_accepts_valid_path() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let config = home.join(".config").join(crate::config::paths::USER_DIR);
        fs::create_dir_all(&config).unwrap();
        let home_link = dir.path().join("home-link");
        std::os::unix::fs::symlink(&home, &home_link).unwrap();
        assert_eq!(
            config_under_home(&config, &home_link).unwrap(),
            fs::canonicalize(&config).unwrap()
        );
    }

    #[test]
    fn config_under_home_rejects_parent_escape() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        fs::create_dir(&home).unwrap();
        let config = home
            .join("../other/.config")
            .join(crate::config::paths::USER_DIR);
        fs::create_dir_all(&config).unwrap();
        assert_eq!(
            config_under_home(&config, &home).unwrap_err().to_string(),
            "give the worker configuration directory under the execution account's home"
        );
    }

    #[test]
    fn config_under_home_rejects_symlink_escape() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        fs::create_dir(&home).unwrap();
        let outside = dir
            .path()
            .join("other")
            .join(crate::config::paths::USER_DIR);
        fs::create_dir_all(&outside).unwrap();
        let config = home.join(crate::config::paths::USER_DIR);
        std::os::unix::fs::symlink(&outside, &config).unwrap();
        assert_eq!(
            config_under_home(&config, &home).unwrap_err().to_string(),
            "give the worker configuration directory under the execution account's home"
        );
    }

    #[test]
    fn config_under_home_rejects_wrong_last_component() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("wrong-name");
        fs::create_dir(&config).unwrap();
        assert!(config_under_home(&config, dir.path()).is_err());
    }

    #[test]
    fn config_under_home_rejects_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(crate::config::paths::USER_DIR);
        let error = config_under_home(&config, dir.path()).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "cannot resolve worker configuration directory {}; it must exist",
                config.display()
            )
        );
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    fn fixture_plan(backend: policy::Backend, config: &Path) -> policy::Plan {
        service_plan(
            backend,
            config,
            policy::Mode::SystemUser {
                uid: 501,
                username: "build".into(),
            },
            config.join("bin/current").join(BINARY),
            Environment::new(),
        )
        .unwrap()
    }

    #[test]
    fn a_system_definition_preserves_a_literal_specifier_and_refuses_an_injected_line() {
        let dir = Path::new("/home/build/100%/belt");
        for backend in [policy::Backend::Launchd, policy::Backend::Systemd] {
            let plan = fixture_plan(backend, dir);
            let cli = <crate::cli::Cli as clap::Parser>::try_parse_from(plan.arguments()).unwrap();
            let crate::cli::Command::Worker(args) = cli.command else {
                panic!("expected worker service command")
            };
            assert!(args.system_service);
            assert_eq!(args.input, Some(dir.join("worker.json")));
            let represented = match backend {
                policy::Backend::Launchd => "100%",
                policy::Backend::Systemd => "100%%",
            };
            assert!(plan
                .plist()
                .contains(&format!("/{represented}/belt/worker.json")));
            let injected = Path::new("/home/build/injected\nUser=root/belt");
            assert!(service_plan(
                backend,
                injected,
                policy::Mode::SystemUser {
                    uid: 501,
                    username: "build".into()
                },
                injected.join("bin/current").join(BINARY),
                Environment::new()
            )
            .is_err());
        }
    }

    #[test]
    fn system_registration_is_stable_across_worker_versions_and_checks_the_owned_definition() {
        for backend in [policy::Backend::Launchd, policy::Backend::Systemd] {
            let directory = crate::test_tmp::tempdir().unwrap();
            let config = Path::new("/home/build/.config/belt");
            let plan = fixture_plan(backend, config);
            let path = publish_definition(directory.path(), &plan, euid()).unwrap();
            assert_eq!(path.file_name().unwrap().to_str().unwrap(), plan.filename());
            assert_eq!(plan.executable(), config.join("bin/current").join(BINARY));
            assert!(plan.arguments().iter().any(|arg| arg == "--system-service"));
            let text = fs::read_to_string(&path).unwrap();
            match backend {
                policy::Backend::Launchd => {
                    assert!(text.contains("<key>UserName</key><string>build</string>"))
                }
                policy::Backend::Systemd => assert!(text.contains("User=build\n")),
            }
            let first = read_definition(&path, euid(), &plan).unwrap();
            publish_definition(directory.path(), &plan, euid()).unwrap();
            assert_eq!(read_definition(&path, euid(), &plan).unwrap(), first);
            assert!(read_definition(&path, euid().wrapping_add(1), &plan).is_err());
            fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
            assert!(read_definition(&path, euid(), &plan).is_err());
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            fs::write(&path, "another definition").unwrap();
            assert!(publish_definition(directory.path(), &plan, euid()).is_err());
            assert_eq!(fs::read_to_string(&path).unwrap(), "another definition");
            let original = directory.path().join("original");
            fs::rename(&path, &original).unwrap();
            std::os::unix::fs::symlink(&original, &path).unwrap();
            assert!(read_definition(&path, euid(), &plan).is_err());
        }
        assert!(passwd(0).is_err());
    }
}
