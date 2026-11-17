//! Explicit, receipt-owned initialization of the combined local resident.
//! No detached fallback, native stop, PID signalling or automatic upgrade.
use super::{
    bootstrap::{self, Spec},
    macos::{self, policy::Backend, DirectoryFiles, Launcher, LoadedState, ManagedFiles},
    resident,
};
use crate::{
    error::{ApiError, ErrorCode},
    ids::*,
};
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Unconfirmed(&'static str);
/// A path or environment value the macOS service observer could not parse back.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct InvalidServiceInput(String);
pub(crate) fn is_unconfirmed(error: &anyhow::Error) -> bool {
    error.downcast_ref::<Unconfirmed>().is_some()
}
pub(crate) fn binding_unconfirmed() -> anyhow::Error {
    Unconfirmed("managed runtime binding unconfirmed; no detached fallback").into()
}

const LIMIT: u64 = 262_144;
const ENV: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "XDG_CONFIG_HOME",
    // The scope of the task-traffic-controller CLI follows these two, so the
    // worker takes the values of the user's shell at install or init.
    "XDG_STATE_HOME",
];

#[derive(Clone)]
pub struct ContextInput {
    pub env: BTreeMap<String, String>,
    pub executable: PathBuf,
}
#[derive(Clone)]
struct Paths {
    config: PathBuf,
    state: PathBuf,
    worker: PathBuf,
    key: PathBuf,
    input: PathBuf,
    services: PathBuf,
    journal: PathBuf,
    /// The service manager the coordinator is registered with. The managed
    /// file's place and form follow it.
    backend: Backend,
}
/// Where the service manager reads the coordinator's managed file: the
/// user's LaunchAgents, or the user's systemd units beside the
/// configuration directory.
fn services_dir(backend: Backend, home: &Path, config: &Path) -> anyhow::Result<PathBuf> {
    Ok(match backend {
        Backend::Launchd => home.join("Library/LaunchAgents"),
        Backend::Systemd => {
            super::systemd::policy::user_unit_dir(config.parent().context("config home")?)
        }
    })
}
impl Paths {
    #[cfg(test)]
    fn resolve(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        Self::resolve_for(env, Backend::Launchd)
    }
    fn resolve_for(env: &BTreeMap<String, String>, backend: Backend) -> anyhow::Result<Self> {
        let home = PathBuf::from(env.get("HOME").context("HOME is required")?);
        // A relative XDG_CONFIG_HOME is refused here, not silently ignored.
        if let Some(x) = env.get("XDG_CONFIG_HOME") {
            resident::validate_components(Path::new(x))?;
        }
        // The worker will share the scope of the user's CLI: refuse here, at
        // install, an environment it would refuse at start.
        crate::config::traffic::resolve(env).map_err(anyhow::Error::msg)?;
        let user = crate::config::paths::user_config_path(env)?;
        let config = user.parent().context("configuration parent")?.to_owned();
        let root = env
            .get("XDG_STATE_HOME")
            .filter(|x| Path::new(x).is_absolute())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/state"))
            .join("rtr");
        let services = services_dir(backend, &home, &config)?;
        // Every path that is used is checked, before anything is created. HOME
        // is judged only through the paths made from it: a state or
        // configuration directory named by XDG_* stands on its own components.
        for used in [&services, &config, &root] {
            resident::validate_components(used)?;
        }
        Ok(Self {
            state: root.join("coordinator"),
            worker: root.join("local-worker"),
            key: config.join("cluster.key"),
            input: config.join("coordinator.json"),
            services,
            journal: config.join("init.json"),
            config,
            backend,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Proof {
    path: PathBuf,
    dev: u64,
    ino: u64,
    uid: u32,
    mode: u32,
    digest: String,
}
impl Proof {
    fn read(path: &Path) -> anyhow::Result<Self> {
        let mut f = resident::secure_file(path, false)?;
        let m = f.metadata()?;
        let digest = hash_reader(&mut f)?;
        let after = f.metadata()?;
        ensure!(same_file(&m, &after), "file changed while reading");
        Ok(Self {
            path: path.into(),
            dev: m.dev(),
            ino: m.ino(),
            uid: m.uid(),
            mode: m.mode(),
            digest,
        })
    }
    fn verify_inode(&self) -> anyhow::Result<()> {
        let f = resident::secure_file(&self.path, false)?;
        let m = f.metadata()?;
        ensure!(
            m.dev() == self.dev
                && m.ino() == self.ino
                && m.uid() == self.uid
                && m.mode() == self.mode,
            "managed database inode changed"
        );
        Ok(())
    }
    fn verify(&self) -> anyhow::Result<()> {
        let now = Self::read(&self.path)?;
        ensure!(
            now.dev == self.dev
                && now.ino == self.ino
                && now.uid == self.uid
                && now.mode == self.mode
                && now.digest == self.digest,
            "managed file changed; reconciliation required"
        );
        Ok(())
    }
}
fn same_file(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.is_file()
        && b.is_file()
        && a.dev() == b.dev()
        && a.ino() == b.ino()
        && a.len() == b.len()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}
fn hash_reader(r: &mut impl Read) -> anyhow::Result<String> {
    let mut h = Sha256::new();
    let mut b = [0; 65536];
    loop {
        let n = r.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    staged: Proof,
    target: PathBuf,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    uid: u32,
    binary_hash: String,
    spec: Spec,
    files: Vec<Proof>,
    pending: Option<Pending>,
    plist: Option<macos::Receipt>,
    database: Option<Proof>,
    cluster: Option<ClusterId>,
    coordinator: Option<CoordinatorId>,
    /// The service is a systemd user unit (Linux), not a LaunchAgent. Left
    /// out for launchd, so a journal written on macOS reads as before. The
    /// journal says it, not the machine that reads it: a process that checks
    /// a managed service looks for the file where init put it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    systemd: bool,
}
impl Journal {
    fn backend(&self) -> Backend {
        if self.systemd {
            Backend::Systemd
        } else {
            Backend::Launchd
        }
    }
}
struct JournalFile {
    path: PathBuf,
    proof: Option<Proof>,
}
impl JournalFile {
    fn save(&mut self, journal: &Journal) -> anyhow::Result<()> {
        self.save_with(journal, |path| File::open(path)?.sync_all())
    }
    fn save_with(
        &mut self,
        journal: &Journal,
        sync_directory: impl FnOnce(&Path) -> std::io::Result<()>,
    ) -> anyhow::Result<()> {
        if let Some(p) = &self.proof {
            p.verify()?;
        }
        let bytes = serde_json::to_vec(journal)?;
        ensure!(bytes.len() as u64 <= LIMIT, "init journal too large");
        let mut tmp =
            tempfile::NamedTempFile::new_in(self.path.parent().context("journal parent")?)?;
        tmp.write_all(&bytes)?;
        tmp.as_file().sync_all()?;
        if self.proof.is_some() {
            tmp.persist(&self.path).map_err(|e| e.error)?;
        } else {
            rustix::fs::renameat_with(
                rustix::fs::CWD,
                tmp.path(),
                rustix::fs::CWD,
                &self.path,
                rustix::fs::RenameFlags::NOREPLACE,
            )?;
        }
        sync_directory(self.path.parent().context("journal parent")?).map_err(|_| {
            Unconfirmed(
                "init journal durability outcome unknown; retain prepared files for reconciliation",
            )
        })?;
        self.proof = Some(Proof::read(&self.path)?);
        Ok(())
    }
}
trait Durability {
    fn sync_directory(&mut self, path: &Path) -> std::io::Result<()>;
}
struct NativeDurability;
impl Durability for NativeDurability {
    fn sync_directory(&mut self, path: &Path) -> std::io::Result<()> {
        File::open(path)?.sync_all()
    }
}
#[cfg(test)]
fn private_dir(path: &Path) -> anyhow::Result<()> {
    private_dir_with(path, &mut NativeDurability)
}
fn private_dir_with(path: &Path, durability: &mut impl Durability) -> anyhow::Result<()> {
    directory_with(path, durability)?;
    resident::check_private_dir(path)?;
    Ok(())
}
fn directory_with(path: &Path, durability: &mut impl Durability) -> anyhow::Result<()> {
    resident::validate_components(path)?;
    let mut prefix = PathBuf::new();
    for component in path.components() {
        prefix.push(component);
        match fs::symlink_metadata(&prefix) {
            Ok(m) => ensure!(
                m.is_dir() && !m.file_type().is_symlink(),
                "unsafe directory ancestor"
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                create_directory_with(&prefix, durability)?;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
// Another initializer can publish this directory before init.lock exists.
// Revalidate the winning entry, and sync it even when this caller lost creation.
fn create_directory_with(path: &Path, durability: &mut impl Durability) -> anyhow::Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "unsafe directory ancestor"
    );
    durability.sync_directory(path)?;
    durability.sync_directory(path.parent().context("new directory parent")?)?;
    Ok(())
}
fn rename_new(from: &Path, to: &Path) -> anyhow::Result<()> {
    rename_new_with(from, to, |path| File::open(path)?.sync_all())
}
fn rename_new_with(
    from: &Path,
    to: &Path,
    sync_directory: impl FnOnce(&Path) -> std::io::Result<()>,
) -> anyhow::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        from,
        rustix::fs::CWD,
        to,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    sync_directory(to.parent().context("publication parent")?).map_err(|_| {
        Unconfirmed("publication directory durability unknown; retained ownership receipt")
    })?;
    Ok(())
}
fn recover(
    j: &mut Journal,
    file: &mut JournalFile,
    paths: &Paths,
    plan: &macos::policy::Plan,
) -> anyhow::Result<()> {
    if let Some(p) = j.pending.clone() {
        let _database_owner = if p.target == paths.state.join("coordinator.db") {
            Some(crate::storage::ProcessLock::acquire(
                &paths.state,
                "coordinator.lock",
            )?)
        } else {
            None
        };
        ensure!(
            [
                &paths.key,
                &paths.input,
                &paths.state.join("coordinator.db"),
                &j.spec.executable,
                &paths.services.join(plan.filename())
            ]
            .contains(&&p.target),
            "invalid pending target"
        );
        ensure!(
            p.staged.path.parent() == p.target.parent()
                && p.staged
                    .path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s.starts_with(".init-")),
            "invalid pending stage"
        );
        if p.target.try_exists()? {
            let mut published = p.staged.clone();
            published.path = p.target.clone();
            published.verify()?;
            ensure!(
                !p.staged.path.try_exists()?,
                "ambiguous pending publication"
            );
        } else {
            p.staged.verify()?;
            rename_new(&p.staged.path, &p.target)?;
        }
        let mut proof = p.staged;
        proof.path = p.target;
        proof.verify()?;
        j.files.push(proof);
        j.pending = None;
        file.save(j)?;
    }
    Ok(())
}
fn publish(
    j: &mut Journal,
    file: &mut JournalFile,
    paths: &Paths,
    plan: &macos::policy::Plan,
    stage: &Path,
    target: &Path,
) -> anyhow::Result<()> {
    NativeDurability.sync_directory(stage.parent().context("stage parent")?)?;
    j.pending = Some(Pending {
        staged: Proof::read(stage)?,
        target: target.into(),
    });
    file.save(j)?;
    recover(j, file, paths, plan)
}
fn owned(j: &Journal, path: &Path) -> anyhow::Result<bool> {
    if let Some(p) = j.files.iter().find(|p| p.path == path) {
        p.verify()?;
        Ok(true)
    } else {
        ensure!(
            !path.try_exists()?,
            "existing unmanaged file; reconciliation required"
        );
        Ok(false)
    }
}
fn stage_path(target: &Path) -> PathBuf {
    target
        .parent()
        .expect("absolute target")
        .join(format!(".init-{}", ulid::Ulid::new()))
}
fn binary_source(path: &Path) -> anyhow::Result<(File, String)> {
    resident::validate_components(path)?;
    let mut f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let m = f.metadata()?;
    ensure!(m.is_file(), "executable source must be regular");
    let digest = hash_reader(&mut f)?;
    ensure!(same_file(&m, &f.metadata()?), "executable source changed");
    use std::io::{Seek, SeekFrom};
    f.seek(SeekFrom::Start(0))?;
    Ok((f, digest))
}
fn service_inputs_observable(paths: &Paths, env: &BTreeMap<String, String>) -> anyhow::Result<()> {
    let strings = [&paths.config, &paths.input].map(|p| p.to_str());
    let env_values = ENV.iter().filter_map(|k| env.get(*k).map(String::as_str));
    for value in strings.into_iter().flatten().chain(env_values) {
        if !macos::policy::observable(value) {
            return Err(InvalidServiceInput(format!(
                "cannot install the service: {value:?} contains characters the service observer cannot parse (control characters, braces, backslash, quotes, or edge whitespace); no files were changed"
            ))
            .into());
        }
    }
    Ok(())
}
fn plan(paths: &Paths, j: &Journal) -> anyhow::Result<macos::policy::Plan> {
    let label = format!(
        "io.github.t09tanaka.rtr.local.{}.{}",
        j.uid,
        &format!(
            "{:x}",
            Sha256::digest(paths.config.as_os_str().as_encoded_bytes())
        )[..12]
    );
    let env = j
        .spec
        .env
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "XDG_CONFIG_HOME" | "XDG_STATE_HOME"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let render = match paths.backend {
        Backend::Launchd => macos::policy::render,
        Backend::Systemd => super::systemd::policy::render,
    };
    render(&macos::policy::Definition {
        label,
        mode: macos::policy::Mode::User { uid: j.uid },
        executable: j.spec.executable.clone(),
        arguments: bootstrap::daemon_argv(&paths.input)?,
        environment: env,
        working_directory: paths.config.clone(),
    })
    .map_err(anyhow::Error::msg)
}
fn validate_journal(j: &Journal, p: &Paths, uid: u32, digest: &str) -> anyhow::Result<()> {
    ensure!(
        j.schema_version == 1 && j.uid == uid,
        "foreign init journal"
    );
    ensure!(
        j.backend() == p.backend,
        "the init journal was written for another service manager; reconciliation required"
    );
    ensure!(
        j.binary_hash == digest,
        "installed binary differs; upgrade required (installed version retained)"
    );
    ensure!(
        j.spec.executable == p.config.join("bin").join(digest).join("remote-task-runner")
            && j.spec.state_dir == p.state
            && j.spec.worker_state_dir == p.worker
            && j.spec.cluster_key_path == p.key
            && j.spec.repo_root.as_ref() == Some(&p.config)
            && j.spec.expected_cluster == j.cluster
            && j.cluster.is_some()
            && j.coordinator.is_some()
            && j.spec.managed_service.as_ref() == Some(&p.journal),
        "init paths changed; reconciliation required"
    );
    ensure!(
        j.spec.env.keys().all(|k| ENV.contains(&k.as_str())),
        "unexpected service environment"
    );
    let plist_path = p.services.join(plan(p, j)?.filename());
    ensure!(
        j.files.iter().all(|f| [
            &p.key,
            &p.input,
            &p.state.join("coordinator.db"),
            &j.spec.executable,
            &plist_path
        ]
        .contains(&&f.path)),
        "unexpected managed file"
    );
    Ok(())
}
#[derive(Debug, Serialize)]
pub struct ResultReceipt {
    pub schema_version: u32,
    pub cluster_id: ClusterId,
    pub coordinator_id: CoordinatorId,
    pub worker_id: WorkerId,
    pub epoch: CoordinatorEpoch,
    pub initialized: bool,
}

/// Native registration transport is injectable; health remains authenticated runtime IPC.
/// Callers must opt into this macOS-specific function; public CLI checks its platform first.
pub async fn initialize<L: Launcher>(
    context: ContextInput,
    launcher: &mut L,
) -> anyhow::Result<ResultReceipt> {
    initialize_with_durability(context, launcher, &mut NativeDurability).await
}
async fn initialize_with_durability<L: Launcher>(
    context: ContextInput,
    launcher: &mut L,
    durability: &mut impl Durability,
) -> anyhow::Result<ResultReceipt> {
    let uid = rustix::process::geteuid().as_raw();
    ensure!(uid > 0, "init requires a non-root user");
    let paths = Paths::resolve_for(&context.env, launcher.backend())?;
    // Values rendered into the service must round-trip through `launchctl print`;
    // refuse before any file or directory is created.
    service_inputs_observable(&paths, &context.env)?;
    // Resolve environment and source before writing anything.
    let (mut source, digest) = binary_source(&context.executable)?;
    private_dir_with(&paths.config, durability)?;
    let lockpath = paths.config.join("init.lock");
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&lockpath)
    {
        Ok(f) => f.sync_all()?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    };
    let lock = resident::secure_file(&lockpath, true)?;
    // This operation is blocking filesystem/service IO, including the cooperative lock.
    // CLI runs it in its dedicated runtime; no detached process is created here.
    lock.lock()?;
    let mut jf = JournalFile {
        path: paths.journal.clone(),
        proof: None,
    };
    let mut j = if paths.journal.try_exists()? {
        let mut bytes = Vec::new();
        resident::secure_file(&paths.journal, false)?
            .take(LIMIT + 1)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() as u64 <= LIMIT, "init journal too large");
        let j: Journal = serde_json::from_slice(&bytes)?;
        validate_journal(&j, &paths, uid, &digest)?;
        jf.proof = Some(Proof::read(&paths.journal)?);
        j
    } else {
        ensure!(
            !paths.input.try_exists()?,
            "existing coordinator input has no init receipt; reconciliation required"
        );
        let env = context
            .env
            .iter()
            .filter(|(k, _)| ENV.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let cluster = ClusterId::generate();
        let coordinator = CoordinatorId::generate();
        let j = Journal {
            schema_version: 1,
            uid,
            binary_hash: digest.clone(),
            spec: Spec {
                state_dir: paths.state.clone(),
                worker_state_dir: paths.worker.clone(),
                worker_id: WorkerId::parse(&format!("local-{}", ulid::Ulid::new()))?,
                cluster_key_path: paths.key.clone(),
                executable: paths
                    .config
                    .join("bin")
                    .join(&digest)
                    .join("remote-task-runner"),
                env,
                repo_root: Some(paths.config.clone()),
                expected_cluster: Some(cluster.clone()),
                managed_service: Some(paths.journal.clone()),
            },
            files: vec![],
            pending: None,
            systemd: paths.backend == Backend::Systemd,
            plist: None,
            database: None,
            cluster: Some(cluster),
            coordinator: Some(coordinator),
        };
        jf.save(&j)?;
        j
    };
    let plan = plan(&paths, &j)?;
    // Validate owned files first; missing or changed files are never silently repaired.
    for p in &j.files {
        p.verify()?;
    }
    recover(&mut j, &mut jf, &paths, &plan)?;
    private_dir_with(&paths.state, durability)?;
    private_dir_with(&paths.worker, durability)?;
    if !owned(&j, &paths.key)? {
        ensure!(
            fs::read_dir(&paths.state)?.next().is_none()
                && fs::read_dir(&paths.worker)?.next().is_none(),
            "missing cluster key with existing state; restore original key"
        );
        let stage = stage_path(&paths.key);
        crate::secrets::key::init(&stage)?;
        durability.sync_directory(stage.parent().context("stage parent")?)?;
        publish(&mut j, &mut jf, &paths, &plan, &stage, &paths.key)?;
    }
    crate::secrets::key::load(&paths.key)?;
    let executable = j.spec.executable.clone();
    if !owned(&j, &executable)? {
        private_dir_with(&paths.config.join("bin"), durability)?;
        private_dir_with(executable.parent().context("binary parent")?, durability)?;
        let stage = stage_path(&executable);
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(&stage)?;
        let before = source.metadata()?;
        std::io::copy(&mut source, &mut out)?;
        out.sync_all()?;
        ensure!(
            same_file(&before, &source.metadata()?) && Proof::read(&stage)?.digest == digest,
            "executable changed during copy"
        );
        durability.sync_directory(stage.parent().context("stage parent")?)?;
        publish(&mut j, &mut jf, &paths, &plan, &stage, &executable)?;
    }
    let database_path = paths.state.join("coordinator.db");
    if j.database.is_none() {
        if let Some(proof) = j.files.iter().find(|p| p.path == database_path).cloned() {
            proof.verify()?;
            j.database = Some(proof);
            j.files.retain(|p| p.path != database_path);
            jf.save(&j)?;
        } else {
            ensure!(
                !database_path.try_exists()?,
                "unmanaged coordinator database"
            );
        }
    }
    if let Some(proof) = &j.database {
        proof.verify_inode()?;
    }
    let cluster = j.cluster.clone().context("planned cluster missing")?;
    let coordinator = j
        .coordinator
        .clone()
        .context("planned coordinator missing")?;
    if j.database.is_some() {
        let store = crate::storage::CoordinatorStore::open_read_only(&paths.state)?;
        ensure!(
            store.cluster_id() == &cluster && store.coordinator_id() == &coordinator,
            "foreign coordinator metadata"
        );
    } else {
        crate::storage::CoordinatorStore::initialize_identity(
            &paths.state,
            &cluster,
            &coordinator,
            |stage| {
                durability.sync_directory(stage.parent().context("database stage parent")?)?;
                // The initializer owns coordinator.lock through this callback.
                j.pending = Some(Pending {
                    staged: Proof::read(stage)?,
                    target: database_path.clone(),
                });
                jf.save(&j)?;
                let proof = j.pending.as_ref().expect("set pending").staged.clone();
                proof.verify()?;
                rename_new(stage, &database_path)?;
                let mut published = proof;
                published.path = database_path.clone();
                published.verify()?;
                j.database = Some(published);
                j.pending = None;
                jf.save(&j)
            },
        )?;
    }
    if !owned(&j, &paths.input)? {
        let stage = stage_path(&paths.input);
        bootstrap::write_input(&stage, &j.spec)?;
        durability.sync_directory(stage.parent().context("input stage parent")?)?;
        publish(&mut j, &mut jf, &paths, &plan, &stage, &paths.input)?;
    }
    // Full managed validation follows plist publication, before native bootstrap.
    directory_with(&paths.services, durability)?;
    let mut files = DirectoryFiles::open(&paths.services)?;
    let _service_lease = files.lifecycle_lock()?;
    if j.plist.is_none() {
        let receipt = files
            .install_journaled(&plan, |receipt, stage| {
                durability.sync_directory(stage.parent().context("plist stage parent")?)?;
                j.plist = Some(receipt.clone());
                j.pending = Some(Pending {
                    staged: Proof::read(stage)?,
                    target: receipt.path().into(),
                });
                jf.save(&j)
            })
            .map_err(|_|Unconfirmed("service file/journal publication unconfirmed; prepared receipt retained for reconciliation"))?;
        j.plist = Some(receipt);
        recover(&mut j, &mut jf, &paths, &plan)?;
    }
    let receipt = j.plist.as_ref().context("missing service receipt")?;
    ensure!(
        receipt.path() == paths.services.join(plan.filename()),
        "service receipt path differs"
    );
    files.verify(receipt)?;
    bootstrap::read_input(&paths.input)?;
    match launcher.loaded_state(&plan, receipt) {
        LoadedState::Absent => {
            for p in &j.files {
                p.verify()?;
            }
            launcher
                .launchctl(
                    &plan
                        .start_arguments(receipt.path())
                        .map_err(anyhow::Error::msg)?,
                )
                .map_err(|error| match error {
                    macos::LaunchError::OutcomeUnknown(_) => anyhow::Error::new(Unconfirmed(
                        "service bootstrap outcome unknown; retained receipts for reconciliation",
                    )),
                    macos::LaunchError::NotApplied(_) => {
                        anyhow::anyhow!("service bootstrap rejected; retained receipts for retry")
                    }
                })?;
        }
        LoadedState::Match => {}
        LoadedState::Unknown => return Err(Unconfirmed(
            "loaded service identity unknown; reconciliation required (nothing started or stopped)",
        )
        .into()),
    }
    let health = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(reply) = bootstrap::health(&j.spec).await {
                break reply;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| {
        Unconfirmed("service readiness unconfirmed; retained init receipts for reconciliation")
    })?;
    let resident::Reply::Health {
        cluster_id,
        coordinator_id,
        worker_id,
        epoch,
        ready: true,
        process,
    } = health
    else {
        anyhow::bail!("resident readiness unconfirmed");
    };
    ensure!(
        j.cluster.as_ref().is_none_or(|c| c == &cluster_id)
            && j.coordinator.as_ref().is_none_or(|c| c == &coordinator_id),
        "resident identity changed; reconciliation required"
    );
    for p in &j.files {
        p.verify()?;
    }
    files.verify(receipt)?;
    if launcher.loaded_state(&plan, receipt) != LoadedState::Match
        || launcher.running_identity(&plan, receipt).ok().as_ref() != Some(&process)
        || process.verify_live().is_err()
    {
        return Err(
            Unconfirmed("service identity changed after health; reconciliation required").into(),
        );
    }
    validate_managed(&j.spec)?;
    j.cluster = Some(cluster_id.clone());
    j.coordinator = Some(coordinator_id.clone());
    jf.save(&j)?;
    Ok(ResultReceipt {
        schema_version: 1,
        cluster_id,
        coordinator_id,
        worker_id,
        epoch,
        initialized: true,
    })
}

fn managed_journal(spec: &Spec) -> anyhow::Result<(Journal, Paths)> {
    let journal = spec
        .managed_service
        .as_ref()
        .context("managed service reference missing")?;
    resident::validate_components(journal)?;
    let config = crate::config::paths::user_config_path(&spec.env)?
        .parent()
        .context("config parent")?
        .to_owned();
    ensure!(
        *journal == config.join("init.json"),
        "managed journal locator mismatch"
    );
    let mut bytes = Vec::new();
    resident::secure_file(journal, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= LIMIT, "managed journal too large");
    let j: Journal = serde_json::from_slice(&bytes)?;
    let home = PathBuf::from(spec.env.get("HOME").context("managed HOME missing")?);
    let paths = Paths {
        config: config.clone(),
        state: spec.state_dir.clone(),
        worker: spec.worker_state_dir.clone(),
        key: spec.cluster_key_path.clone(),
        input: config.join("coordinator.json"),
        services: services_dir(j.backend(), &home, &config)?,
        journal: journal.clone(),
        backend: j.backend(),
    };
    validate_journal(
        &j,
        &paths,
        rustix::process::geteuid().as_raw(),
        &j.binary_hash,
    )?;
    ensure!(
        j.spec == *spec && j.pending.is_none() && j.plist.is_some(),
        "managed initialization incomplete or input differs"
    );
    let db = j
        .database
        .as_ref()
        .context("managed database receipt missing")?;
    ensure!(
        db.path == paths.state.join("coordinator.db"),
        "managed database path differs"
    );
    db.verify_inode()?;
    for proof in &j.files {
        proof.verify()?;
    }
    ensure!(
        [&paths.input, &paths.key, &spec.executable]
            .iter()
            .all(|path| j.files.iter().any(|p| &p.path == *path)),
        "managed input receipts incomplete"
    );
    let store = crate::storage::CoordinatorStore::open_read_only(&paths.state)?;
    ensure!(
        Some(store.cluster_id()) == j.cluster.as_ref()
            && Some(store.coordinator_id()) == j.coordinator.as_ref(),
        "managed database identity mismatch"
    );
    let receipt = j.plist.as_ref().expect("checked receipt");
    let expected = plan(&paths, &j)?;
    ensure!(
        receipt.path() == paths.services.join(expected.filename()),
        "managed plist path mismatch"
    );
    DirectoryFiles::open(&paths.services)?.verify_read_only(receipt)?;
    Ok((j, paths))
}
pub(crate) fn managed_identity(spec: &Spec) -> anyhow::Result<(ClusterId, CoordinatorId)> {
    let (j, _) = managed_journal(spec)?;
    Ok((
        j.cluster.context("cluster binding missing")?,
        j.coordinator.context("coordinator binding missing")?,
    ))
}
pub(crate) fn validate_managed(spec: &Spec) -> anyhow::Result<()> {
    managed_journal(spec).map(|_| ())
}
pub(crate) async fn managed_health<L: Launcher>(
    spec: &Spec,
    launcher: &mut L,
) -> anyhow::Result<()> {
    let (j, paths) = managed_journal(spec)?;
    ensure!(
        launcher.backend() == paths.backend,
        "the managed service belongs to another service manager"
    );
    let plan = plan(&paths, &j)?;
    let receipt = j.plist.as_ref().expect("validated");
    let before = launcher.running_identity(&plan, receipt).map_err(|_| {
        Unconfirmed("managed service is not confirmed running; no detached fallback")
    })?;
    let reply = bootstrap::health(spec).await?;
    let resident::Reply::Health {
        process,
        coordinator_id,
        cluster_id,
        ready: true,
        ..
    } = reply
    else {
        return Err(Unconfirmed("managed runtime health unknown").into());
    };
    ensure!(
        Some(&cluster_id) == j.cluster.as_ref() && Some(&coordinator_id) == j.coordinator.as_ref(),
        "managed health binding differs"
    );
    let after = launcher
        .running_identity(&plan, receipt)
        .map_err(|_| Unconfirmed("managed service running identity changed"))?;
    ensure!(
        before == process && after == process,
        "health is not owned by the registered service"
    );
    process.verify_live()?;
    validate_managed(spec)?;
    Ok(())
}

/// `init`. The coordinator and its local worker run as one service of the
/// host's service manager: a LaunchAgent on macOS, a systemd user unit on
/// Linux (`HostLauncher::native`).
pub fn command() -> Result<ResultReceipt, ApiError> {
    let context = ContextInput {
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        executable: std::env::current_exe()
            .map_err(|_| ApiError::new(ErrorCode::InvalidInput, "cannot locate executable"))?,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
    runtime
        .block_on(initialize(
            context,
            &mut super::worker_service::HostLauncher::native(),
        ))
        .map_err(|e| {
            ApiError::new(
                if e.downcast_ref::<Unconfirmed>().is_some() {
                    ErrorCode::CommunicationUnknown
                } else if e.downcast_ref::<InvalidServiceInput>().is_some() {
                    ErrorCode::InvalidInput
                } else {
                    ErrorCode::Conflict
                },
                format!("init: {e}"),
            )
        })
}
#[cfg(test)]
mod tests;
