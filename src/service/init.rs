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
            .join("belt");
        let services = services_dir(backend, &home, &config)?;
        // Every path that is used is checked, before anything is created. HOME
        // is judged only through the paths made from it: a state or
        // configuration directory named by XDG_* stands on its own components.
        for used in [&services, &config, &root] {
            resident::validate_components(used)?;
        }
        Ok(Self {
            state: root.join("coordinator"),
            worker: super::worker_service::Paths::resolve_for(env, backend)?.state,
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
fn read_journal(path: &Path) -> anyhow::Result<Journal> {
    let mut bytes = Vec::new();
    resident::secure_file(path, false)?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= LIMIT, "init journal too large");
    Ok(serde_json::from_slice(&bytes)?)
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
        "io.github.t09tanaka.belt.local.{}.{}",
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
        j.spec.executable == p.config.join("bin").join(digest).join("belt")
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
    initialize_named_with_durability(context, None, launcher, durability).await
}

/// Initialize with an explicit member name; existing members cannot be renamed.
pub async fn initialize_named<L: Launcher>(
    context: ContextInput,
    name: Option<&str>,
    launcher: &mut L,
) -> anyhow::Result<ResultReceipt> {
    initialize_named_with_durability(context, name, launcher, &mut NativeDurability).await
}

async fn initialize_named_with_durability<L: Launcher>(
    context: ContextInput,
    name: Option<&str>,
    launcher: &mut L,
    durability: &mut impl Durability,
) -> anyhow::Result<ResultReceipt> {
    let name = name.map(WorkerId::parse).transpose()?;
    let paths = Paths::resolve_for(&context.env, launcher.backend())?;
    let worker = super::worker_service::Paths::resolve_for(&context.env, launcher.backend())?;
    if worker.journal.try_exists()? {
        return takeover::initialize(context, worker, name.as_ref(), launcher, durability).await;
    }
    initialize_prepared(context, paths, None, name.as_ref(), launcher, durability).await
}

async fn initialize_prepared<L: Launcher>(
    context: ContextInput,
    mut paths: Paths,
    enrollment: Option<&super::worker_service::Enrollment>,
    name: Option<&WorkerId>,
    launcher: &mut L,
    durability: &mut impl Durability,
) -> anyhow::Result<ResultReceipt> {
    let uid = rustix::process::geteuid().as_raw();
    ensure!(uid > 0, "init requires a non-root user");
    if let Some(enrollment) = enrollment {
        paths.key = paths.config.join("worker-cluster.key");
        ensure!(
            enrollment.env
                == context
                    .env
                    .iter()
                    .filter(|(k, _)| ENV.contains(&k.as_str()))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            "worker environment changed"
        );
    } else if paths.journal.try_exists()? {
        let journal = read_journal(&paths.journal)?;
        if journal.spec.cluster_key_path == paths.config.join("worker-cluster.key") {
            paths.key = journal.spec.cluster_key_path;
        }
    }
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
    // Takeover holds this lock through retirement and rollback as well.
    if enrollment.is_none() {
        lock.lock()?;
    }
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
        check_name(name, &j.spec.worker_id)?;
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
        let cluster = enrollment
            .map(|e| e.cluster.clone())
            .unwrap_or_else(ClusterId::generate);
        let coordinator = CoordinatorId::generate();
        let j = Journal {
            schema_version: 1,
            uid,
            binary_hash: digest.clone(),
            spec: Spec {
                state_dir: paths.state.clone(),
                worker_state_dir: paths.worker.clone(),
                worker_id: match enrollment {
                    Some(e) => e.worker_id.clone(),
                    None => match name {
                        Some(name) => name.clone(),
                        None => hostname_name(&crate::platform::hostname()?)?,
                    },
                },
                cluster_key_path: paths.key.clone(),
                executable: paths.config.join("bin").join(&digest).join("belt"),
                env,
                repo_root: Some(paths.config.clone()),
                expected_cluster: Some(cluster.clone()),
                managed_service: Some(paths.journal.clone()),
            },
            files: if enrollment.is_some() {
                let mut proofs = vec![Proof::read(&paths.key)?];
                let binary = paths.config.join("bin").join(&digest).join("belt");
                if binary.try_exists()? {
                    let proof = Proof::read(&binary)?;
                    ensure!(proof.digest == digest, "installed binary changed");
                    proofs.push(proof);
                }
                proofs
            } else {
                vec![]
            },
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
    link_resident(&executable, durability)?;
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

/// `bin/resident` -> `<digest>`: the binary this host's init resident runs,
/// at a fixed path the SSH helpers can find without PATH. Unlike the worker
/// service's `bin/current`, it follows every init, so it is replaced
/// atomically (a new link renamed over the old one).
fn link_resident(executable: &Path, durability: &mut impl Durability) -> anyhow::Result<()> {
    let dir = executable.parent().context("binary parent")?;
    let digest = dir.file_name().context("binary digest")?;
    let bin = dir.parent().context("bin directory")?;
    let link = bin.join("resident");
    if fs::read_link(&link).ok().as_deref() == Some(Path::new(digest)) {
        return Ok(());
    }
    let staged = bin.join(format!(".resident.{}", std::process::id()));
    match fs::remove_file(&staged) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    std::os::unix::fs::symlink(digest, &staged)?;
    fs::rename(&staged, &link)?;
    durability.sync_directory(bin)?;
    Ok(())
}

/// Start/stop the single resident through its existing managed receipt.
/// Serialized with membership handover by the init lock.
pub(super) fn worker_lifecycle<L: Launcher>(
    env: &BTreeMap<String, String>,
    mut launcher: L,
    stop: Option<(bool, Duration, &super::worker_service::Submitters)>,
    ready_timeout: Duration,
) -> anyhow::Result<super::worker_service::Status> {
    use super::worker_service as ws;
    use crate::worker::control::{self, Control};
    let initial =
        Paths::resolve_for(env, launcher.backend()).context("init resident paths unavailable")?;
    let lock = resident::secure_file(&initial.config.join("init.lock"), true)
        .context("init resident lock unavailable")?;
    lock.lock()?;
    membership_undo::recover(&initial, &mut launcher)?;
    ensure!(
        !initial.config.join("membership-undo.json").try_exists()?,
        "membership undo is still pending"
    );
    let spec = bootstrap::read_input(&initial.input).context("init resident input unavailable")?;
    let (journal, paths) = managed_journal(&spec)?;
    ensure!(
        launcher.backend() == paths.backend,
        "service manager differs"
    );
    let plan = plan(&paths, &journal)?;
    let receipt = journal.plist.as_ref().context("service receipt missing")?;
    let identity = macos::WorkerIdentity {
        cluster_id: journal.cluster.clone().context("group missing")?,
        worker_id: spec.worker_id.clone(),
    };
    let mut manager = macos::Manager {
        identity,
        files: DirectoryFiles::open(&paths.services)?,
        launcher,
        worker: ws::MarkerControl {
            state: paths.worker.clone(),
            wait: stop.map(|(_, wait, _)| wait).unwrap_or(Duration::ZERO),
            interval: Duration::from_millis(250),
        },
    };
    match (manager.launcher.loaded_state(&plan, receipt), stop) {
        (LoadedState::Unknown, _) => {
            anyhow::bail!("loaded service identity unknown; nothing started or stopped")
        }
        (LoadedState::Match, Some((cancel, _, submitters))) => {
            manager.worker.verify_binding(&manager.identity)?;
            let previous = control::read(&paths.worker)?;
            let store = if submitters.all {
                None
            } else {
                Some(crate::storage::CoordinatorStore::open_read_only(&paths.state)
                    .context("cannot tell whether target has its own non-terminal runs; use --all-submitters")?)
            };
            let check = || -> anyhow::Result<()> {
                if let Some(store) = &store {
                    ensure!(!store.has_nonterminal_runs()
                        .context("cannot tell whether target has its own non-terminal runs; use --all-submitters")?,
                        "target {} has its own non-terminal runs; nothing was stopped (use --all-submitters)", spec.worker_id);
                }
                Ok(())
            };
            ws::drain_before_check(&paths.worker, || {
                ws::refuse_other_submitters(&paths.worker, submitters)?;
                check()
            })?;
            manager.stop_checked(&plan, receipt, cancel, || {
                if let Err(error) = check() {
                    control::write(&paths.worker, previous)?;
                    return Err(error);
                }
                Ok(())
            })?;
            control::write(&paths.worker, previous)?;
        }
        (loaded, None) => {
            manager.worker.verify_binding(&manager.identity)?;
            ws::clear_cancel(&paths.worker)?;
            if loaded == LoadedState::Absent {
                manager.start(&plan, receipt)?;
            }
        }
        _ => {}
    }
    let loaded = match manager.launcher.loaded_state(&plan, receipt) {
        LoadedState::Match => Some(true),
        LoadedState::Absent => Some(false),
        LoadedState::Unknown => None,
    };
    let ready =
        stop.is_none() && loaded == Some(true) && ws::wait_ready(&paths.worker, ready_timeout);
    let control = Control::effective(&paths.worker);
    Ok(ws::Status {
        membership_only: false,
        coordinator_id: journal.coordinator,
        system_user: false,
        worker_id: spec.worker_id,
        cluster: manager.identity.cluster_id,
        binary_sha256: journal.binary_hash,
        loaded,
        ready,
        draining: control.drain,
        cancel_running: control.cancel_running,
        live_attempts: ws::live_attempts(&paths.worker).ok(),
        bridge_binary: Some(spec.executable.to_string_lossy().into_owned()),
        traffic_scope: None,
        traffic_hold: crate::config::traffic::recorded_hold(
            &paths.worker,
            crate::platform::wall_ms(),
        )
        .filter(|_| loaded != Some(false)),
        switching_from: None,
        drained_by_upgrade: false,
        switch_running: false,
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
    let mut paths = Paths::resolve_for(&spec.env, j.backend())?;
    if j.spec.cluster_key_path == paths.config.join("worker-cluster.key") {
        paths.key = j.spec.cluster_key_path.clone();
    }
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
/// The SSH helper changes membership without creating a second resident.
pub(crate) fn handover<L: Launcher>(
    env: &BTreeMap<String, String>,
    request: &super::worker_service::InstallRequest,
    mut launcher: L,
) -> anyhow::Result<super::worker_service::Status> {
    use crate::storage::{CoordinatorStore, WorkerStore};
    use crate::worker::control;
    let member = request.membership.as_ref().context("membership missing")?;
    let initial_paths = Paths::resolve_for(env, launcher.backend())?;
    let lock = resident::secure_file(&initial_paths.config.join("init.lock"), true)?;
    lock.lock()?;
    membership_undo::recover(&initial_paths, &mut launcher)?;
    let spec = super::context::runtime_spec(env).map_err(|e| anyhow::anyhow!(e.message))?;
    let (mut journal, paths) = managed_journal(&spec)?;
    let old = journal.cluster.as_ref().context("group missing")?;
    let moving = old != &request.cluster;
    super::worker_service::membership::check_same_group_key(&paths.key, request, moving)?;
    let worker = WorkerStore::open_read_only(&paths.worker)?;
    let is_singleton = || -> anyhow::Result<bool> {
        Ok(super::workers::registrations(env)
            .map_err(|e| anyhow::anyhow!(e.message))?
            .iter()
            .all(|entry| entry.id == spec.worker_id.as_str())
            && worker
                .members()?
                .iter()
                .all(|(name, _)| name == spec.worker_id.as_str()))
    };
    let singleton = member.join_singleton && is_singleton()?;
    ensure!(
        !moving || member.move_group || singleton,
        "target belongs to group {old}; use --move to move an idle member"
    );
    let coordinator = CoordinatorStore::open_read_only(&paths.state)?;
    ensure!(
        !moving || (worker.count_group_work()? == 0 && !coordinator.has_group_work()?),
        "target belongs to group {old} and has live work or uncollected results; cannot move"
    );
    ensure!(
        spec.worker_id == request.worker_id,
        "target member is named {}; use that name",
        spec.worker_id
    );
    ensure!(
        !request.system_user,
        "service mode differs from the worker registration"
    );
    let plan = plan(&paths, &journal)?;
    let receipt = journal.plist.clone().context("service receipt missing")?;
    let mut files = DirectoryFiles::open(&paths.services)?;
    let _lease = files.lifecycle_lock()?;
    files.verify(&receipt)?;
    let loaded = launcher.loaded_state(&plan, &receipt);
    ensure!(
        loaded != LoadedState::Unknown,
        "loaded service identity unknown"
    );
    let previous = control::read(&paths.worker)?;
    if moving {
        // Record restart intent before stopping; worker rows are still changing.
        membership_undo::prepare(&paths, previous, loaded == LoadedState::Match, false)?;
    }
    let result = (|| -> anyhow::Result<_> {
        if moving && loaded == LoadedState::Match {
            control::write(&paths.worker, control::Control::draining())?;
            launcher.verify_loaded(&plan, &receipt)?;
            launcher.launchctl(
                &plan
                    .stop_arguments(receipt.path())
                    .map_err(anyhow::Error::msg)?,
            )?;
            ensure!(
                launcher.loaded_state(&plan, &receipt) == LoadedState::Absent,
                "service did not stop; group unchanged"
            );
        }
        let _owner = if moving {
            Some(crate::storage::ProcessLock::acquire(
                &paths.state,
                "coordinator.lock",
            )?)
        } else {
            None
        };
        let _agent = if moving {
            Some(crate::storage::ProcessLock::acquire(
                &paths.worker,
                "agent.lock",
            )?)
        } else {
            None
        };
        let no_longer_singleton = moving && !member.move_group && !is_singleton()?;
        if moving
            && (worker.count_group_work()? != 0
                || coordinator.has_group_work()?
                || no_longer_singleton)
        {
            control::write(&paths.worker, previous)?;
            drop(_agent);
            drop(_owner);
            if loaded == LoadedState::Match {
                launcher.launchctl(
                    &plan
                        .start_arguments(receipt.path())
                        .map_err(anyhow::Error::msg)?,
                )?;
            }
            if no_longer_singleton {
                anyhow::bail!(
                    "another member appeared before stopping; group unchanged; use --move"
                );
            }
            anyhow::bail!("live work appeared before stopping; group unchanged");
        }
        let worker = WorkerStore::open(&paths.worker, &spec.worker_id)?;
        if moving {
            // Snapshot worker rows only after both owners stopped, including final pins.
            membership_undo::prepare(&paths, previous, loaded == LoadedState::Match, true)?;
            worker.clear_group_membership()?;
            CoordinatorStore::move_group(&paths.state, &request.cluster)?;
            match fs::remove_file(crate::group::next_key_path(&paths.key)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            journal.cluster = Some(request.cluster.clone());
            journal.spec.expected_cluster = Some(request.cluster.clone());
        }
        let mut jf = JournalFile {
            path: paths.journal.clone(),
            proof: Some(Proof::read(&paths.journal)?),
        };
        let mut replacements = vec![(paths.key.clone(), request.key_file_text.as_bytes().to_vec())];
        if moving {
            replacements.push((paths.input.clone(), serde_json::to_vec(&journal.spec)?));
        }
        for (path, bytes) in replacements {
            let mut temp = tempfile::NamedTempFile::new_in(path.parent().context("file parent")?)?;
            temp.write_all(&bytes)?;
            temp.as_file().sync_all()?;
            temp.persist(&path).map_err(|e| e.error)?;
            File::open(path.parent().context("file parent")?)?.sync_all()?;
            let proof = Proof::read(&path)?;
            *journal
                .files
                .iter_mut()
                .find(|p| p.path == path)
                .context("receipt missing")? = proof;
        }
        jf.save(&journal)?;
        worker.observe_group_key(&crate::group::fingerprint(&crate::secrets::key::load(
            &paths.key,
        )?))?;
        worker
            .for_member(&member.caller_id)
            .replace_member_pin(member.caller_name.as_str(), crate::platform::wall_ms())?;
        if moving {
            control::write(&paths.worker, previous)?;
        }
        drop(_agent);
        drop(_owner);
        if moving
            && loaded == LoadedState::Match
            && launcher.loaded_state(&plan, &receipt) == LoadedState::Absent
        {
            launcher.launchctl(
                &plan
                    .start_arguments(receipt.path())
                    .map_err(anyhow::Error::msg)?,
            )?;
        }
        let running = launcher.loaded_state(&plan, &receipt) == LoadedState::Match;
        if running {
            launcher.verify_loaded(&plan, &receipt)?;
        }
        Ok(super::worker_service::Status {
            membership_only: true,
            coordinator_id: journal.coordinator,
            system_user: false,
            worker_id: spec.worker_id.clone(),
            cluster: request.cluster.clone(),
            binary_sha256: journal.binary_hash,
            loaded: Some(running),
            ready: running,
            draining: previous.drain,
            cancel_running: previous.cancel_running,
            live_attempts: Some(u32::try_from(worker.count_nonterminal()?).unwrap_or(u32::MAX)),
            bridge_binary: Some(spec.executable.to_string_lossy().into_owned()),
            traffic_scope: None,
            traffic_hold: None,
            switching_from: None,
            drained_by_upgrade: false,
            switch_running: false,
        })
    })();
    drop(_lease);
    match result {
        Ok(status) => {
            // Retain restart intent until the start and loaded verification succeed.
            membership_undo::finish(&paths)?;
            Ok(status)
        }
        Err(error) => {
            let rollback = membership_undo::recover(&paths, &mut launcher);
            let restore = control::write(&paths.worker, previous);
            let restart = (|| -> anyhow::Result<()> {
                if !moving || loaded != LoadedState::Match {
                    return Ok(());
                }
                files.verify(&receipt)?;
                match launcher.loaded_state(&plan, &receipt) {
                    LoadedState::Absent => launcher.launchctl(
                        &plan
                            .start_arguments(receipt.path())
                            .map_err(anyhow::Error::msg)?,
                    )?,
                    LoadedState::Match => launcher.verify_loaded(&plan, &receipt)?,
                    LoadedState::Unknown => {
                        anyhow::bail!("service identity unknown during membership restart")
                    }
                }
                Ok(())
            })();
            match rollback.and(restore.map_err(Into::into)).and(restart) {
                Ok(()) => Err(error),
                Err(rollback) => Err(anyhow::anyhow!(
                    "{error}; membership rollback failed: {rollback}"
                )),
            }
        }
    }
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
pub fn command(name: Option<&str>) -> Result<ResultReceipt, ApiError> {
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
        .block_on(initialize_named(
            context,
            name,
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

mod membership_undo;
mod takeover;

fn check_name(requested: Option<&WorkerId>, enrolled: &WorkerId) -> anyhow::Result<()> {
    ensure!(
        requested.is_none_or(|name| name == enrolled),
        "member is already named {enrolled}; init --name cannot rename a member"
    );
    Ok(())
}

fn hostname_name(hostname: &str) -> anyhow::Result<WorkerId> {
    let short = hostname.split('.').next().unwrap_or("");
    let reduced: String = short
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    WorkerId::parse(&reduced)
        .context("hostname is not a valid member name; run belt init --name <member-name>")
}
