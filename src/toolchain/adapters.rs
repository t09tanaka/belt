//! Private installation plans and durable activation contracts.
//! No process execution or admission grant lives here. Private preparation connects
//! the trusted helper to setup admission, singleflight and supervised child ownership.
use super::versions::{Constraint, FrozenToolchains, ToolId};
use crate::ids::Digest;
use node_semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
type Result<T> = std::result::Result<T, &'static str>;
const MAX_RECEIPT: u64 = 32 * 1024 * 1024;
const MAX_FILES: usize = 131072;
const MAX_TOTAL: u64 = 8 * 1024 * 1024 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdapterKind {
    Nodenv,
    Rustup,
    Mise,
    Fvm,
    Corepack,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    Installed(Box<InstallationReceipt>),
    Missing,
    Unknown(&'static str),
    ManualRequired(&'static str),
}
pub fn manual_requirement(tool: &str) -> Option<&'static str> {
    match tool {
        "xcode" => Some("install_xcode_and_accept_license_manually"),
        "docker-desktop" => Some("install_and_start_docker_desktop_manually"),
        "system-package" => Some("administrator_system_package_installation_required"),
        _ => None,
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIdentity {
    pub path: PathBuf,
    pub digest: Digest,
    pub size: u64,
    pub executable: bool,
}
/// One finite budget for both resolver passes and complete artifact validation.
/// The read-only verifier retains descriptor identities until its final sweep.
pub(crate) struct IdentityReadBudget {
    per_file: u64,
    remaining: std::cell::Cell<u64>,
    reads: std::cell::Cell<u64>,
    snapshot: std::cell::RefCell<probe::Snapshot>,
    limited: bool,
    started: u64,
    #[cfg(test)]
    total: u64,
}
impl IdentityReadBudget {
    pub(crate) fn probe() -> Self {
        Self::limited(16 * 1024 * 1024, 64 * 1024 * 1024)
    }
    fn limited(per_file: u64, total: u64) -> Self {
        Self {
            per_file,
            remaining: std::cell::Cell::new(total),
            reads: std::cell::Cell::new(0),
            snapshot: std::cell::RefCell::new(probe::Snapshot::default()),
            limited: true,
            started: crate::platform::monotonic_ms(),
            #[cfg(test)]
            total,
        }
    }
    fn admission() -> Self {
        let mut b = Self::limited(MAX_TOTAL, u64::MAX);
        b.limited = false;
        b
    }
    fn capture(&self, path: &Path) -> Result<FileIdentity> {
        if self.limited {
            probe::capture(path, self)
        } else {
            FileIdentity::capture(path)
        }
    }
    pub(crate) fn finish(&self) -> Result<()> {
        self.snapshot.borrow().finish()
    }
}
#[cfg(test)]
thread_local! { static PROBE_STATISTICS: std::cell::Cell<(u64,u64,usize,usize)> = const { std::cell::Cell::new((0,0,0,0)) }; }
#[cfg(test)]
impl Drop for IdentityReadBudget {
    fn drop(&mut self) {
        if self.limited {
            let snapshot = self.snapshot.borrow();
            PROBE_STATISTICS.with(|s| {
                s.set((
                    self.total - self.remaining.get(),
                    self.reads.get(),
                    snapshot.paths_len(),
                    snapshot.visits(),
                ))
            });
        }
    }
}
#[cfg(test)]
pub(crate) fn probe_statistics() -> (u64, u64, usize, usize) {
    PROBE_STATISTICS.with(std::cell::Cell::get)
}
mod probe;
pub(crate) use probe::{inspect_installation, prospective_activation, VerifiedInstallation};
impl FileIdentity {
    pub fn capture(path: &Path) -> Result<Self> {
        Self::capture_bounded(path, MAX_TOTAL)
    }
    fn capture_bounded(path: &Path, max_bytes: u64) -> Result<Self> {
        Self::capture_bounded_checked(path, max_bytes, || {})
    }
    fn capture_bounded_checked(
        path: &Path,
        max_bytes: u64,
        after_metadata: impl FnOnce(),
    ) -> Result<Self> {
        if !path.is_absolute() {
            return Err("absolute_identity_required");
        }
        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| "identity_open")?;
        let m = f.metadata().map_err(|_| "identity_metadata")?;
        if !m.is_file() || m.len() > max_bytes {
            return Err("identity_not_bounded_regular");
        }
        after_metadata();
        let mut h = Sha256::new();
        let mut buf = [0u8; 65536];
        let mut size = 0;
        loop {
            // One sentinel byte detects growth; never read an unbounded chunk
            // after a stale metadata size check.
            let left = max_bytes.saturating_sub(size).saturating_add(1);
            let length = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
            let n = f.read(&mut buf[..length]).map_err(|_| "identity_read")?;
            if n == 0 {
                break;
            }
            size += n as u64;
            if size > max_bytes {
                return Err("identity_too_large");
            }
            h.update(&buf[..n]);
        }
        let after = f.metadata().map_err(|_| "identity_metadata")?;
        if size != m.len()
            || m.len() != after.len()
            || m.mtime() != after.mtime()
            || m.mtime_nsec() != after.mtime_nsec()
        {
            return Err("identity_changed");
        }
        let digest =
            Digest::parse(&format!("sha256:{:x}", h.finalize())).map_err(|_| "identity_digest")?;
        Ok(Self {
            path: path.into(),
            digest,
            size,
            executable: m.mode() & 0o111 != 0,
        })
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if Self::capture(&self.path)? != *self {
            Err("tool_identity_drift")
        } else {
            Ok(())
        }
    }
}
/// Trusted operator configuration, never project/wire input. Canonical regular
/// executable paths and complete external runtime dependencies are required.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostTools {
    pub manager: FileIdentity,
    pub dependencies: Vec<FileIdentity>,
    pub search_path: Vec<PathBuf>,
    pub rust_host: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub argv: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
}
#[derive(Debug, Clone)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
}
/// Trusted host interface, NOT evidence that TTC admission occurred. Caller
/// clears env, bounds output, enforces deadlines/cancellation and owns drain.
pub trait AdmittedExecutor {
    fn execute(&mut self, command: &CommandSpec) -> Result<CommandOutput>;
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionEvidence {
    pub contract: Digest,
    pub tool: ToolId,
    pub concrete: Version,
    pub catalog: Digest,
    pub resolved_inputs: BTreeMap<String, Version>,
}
impl ResolutionEvidence {
    pub fn digest(&self) -> Digest {
        hash(self)
    }
}
pub fn resolve(
    frozen: &FrozenToolchains,
    evidence: &ResolutionEvidence,
) -> Result<FrozenToolchains> {
    frozen.validate()?;
    let mut unselected = frozen.clone();
    unselected.selections.clear();
    if evidence.contract != unselected.digest() || evidence.resolved_inputs.len() > 256 {
        return Err("resolution_contract_mismatch");
    }
    let req = frozen
        .requirements
        .iter()
        .find(|r| r.tool == evidence.tool)
        .ok_or("resolution_tool_missing")?;
    for c in &req.constraints {
        match c {
            Constraint::Resolve(s)
            | Constraint::Expression(s)
            | Constraint::VersionFileExpression(s)
                if evidence.resolved_inputs.get(s) != Some(&evidence.concrete) =>
            {
                return Err("resolution_input_unproven")
            }
            _ => (),
        }
    }
    frozen
        .with_selection(&evidence.tool, evidence.concrete.clone(), evidence.digest())
        .map_err(|_| "resolution_mismatch")
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstallPlan {
    kind: AdapterKind,
    frozen: FrozenToolchains,
    tool: ToolId,
    version: Version,
    root: PathBuf,
    payload: PathBuf,
    commands: Vec<CommandSpec>,
    probes: Vec<(String, CommandSpec)>,
    external: Vec<FileIdentity>,
    activation_env: BTreeMap<String, String>,
    activation_bin: PathBuf,
}
impl InstallPlan {
    pub fn key(&self) -> Digest {
        hash(&(
            self.kind,
            &self.tool,
            &self.version,
            &self.root,
            &self.payload,
            &self.commands,
            &self.probes,
            &self.external,
            &self.activation_env,
            &self.activation_bin,
        ))
    }
    pub fn commands(&self) -> &[CommandSpec] {
        &self.commands
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn probes(&self) -> &[(String, CommandSpec)] {
        &self.probes
    }
}
fn hash(value: &impl Serialize) -> Digest {
    Digest::of_bytes(&serde_json::to_vec(value).expect("serializable contract"))
}
fn text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or("non_utf8_private_path")
}
fn clean_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
        && !path.as_os_str().as_encoded_bytes().contains(&b':')
}
fn owned_dir(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|_| "private_directory_missing")?;
    if !m.is_dir() || m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o077 != 0 {
        return Err("unsafe_private_directory");
    }
    Ok(())
}
fn mkdir(path: &Path) -> Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(_) => return Err("private_directory_create"),
    }
    owned_dir(path)
}
/// Private root exists already, canonical absolute and owned0700. Exact artifact
/// keys support reuse across source contracts, activation binds each run later.
pub fn plan(
    kind: AdapterKind,
    frozen: &FrozenToolchains,
    tool: &ToolId,
    evidence: &ResolutionEvidence,
    state: &Path,
    host: &HostTools,
) -> Result<InstallPlan> {
    plan_with_limit(
        kind,
        frozen,
        tool,
        evidence,
        state,
        host,
        &IdentityReadBudget::admission(),
    )
}

pub(crate) fn plan_with_limit(
    kind: AdapterKind,
    frozen: &FrozenToolchains,
    tool: &ToolId,
    evidence: &ResolutionEvidence,
    state: &Path,
    host: &HostTools,
    budget: &IdentityReadBudget,
) -> Result<InstallPlan> {
    frozen.validate()?;
    if budget.limited {
        probe::private_root(state, budget)?;
    } else {
        owned_dir(state)?;
        if !clean_absolute(state)
            || fs::canonicalize(state).map_err(|_| "private_root_canonical")? != state
        {
            return Err("noncanonical_private_root");
        }
    }
    if host.dependencies.len() > 32
        || host.search_path.len() > 32
        || host.search_path.iter().any(|p| !clean_absolute(p))
    {
        return Err("invalid_host_tools");
    }
    if budget.capture(&host.manager.path)? != host.manager {
        return Err("tool_identity_drift");
    }
    if !host.manager.executable {
        return Err("manager_not_executable");
    }
    for dep in &host.dependencies {
        if budget.capture(&dep.path)? != *dep {
            return Err("tool_identity_drift");
        }
    }
    let selected = frozen
        .selections
        .iter()
        .find(|s| &s.tool == tool)
        .ok_or("concrete_selection_required")?;
    if selected.authority != evidence.digest()
        || evidence.tool != *tool
        || selected.concrete != evidence.concrete
    {
        return Err("resolution_authority_mismatch");
    }
    let mut prior = frozen.clone();
    prior.selections.retain(|s| &s.tool != tool);
    if resolve(&prior, evidence)? != *frozen {
        return Err("resolution_contract_mismatch");
    }
    let version = selected.concrete.to_string();
    if version.len() > 128
        || !version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-+".contains(&b))
    {
        return Err("unsafe_concrete_version");
    }
    let supported = match kind {
        AdapterKind::Nodenv => tool.as_str() == "node",
        AdapterKind::Rustup => tool.as_str() == "rust",
        AdapterKind::Mise => matches!(tool.as_str(), "node" | "python" | "go" | "ruby"),
        AdapterKind::Fvm => tool.as_str() == "flutter",
        AdapterKind::Corepack => matches!(tool.as_str(), "pnpm" | "yarn" | "npm"),
    };
    if !supported {
        return Err("manual_required_unsupported_adapter_tool");
    }
    if kind == AdapterKind::Rustup
        && (host.rust_host.is_empty()
            || host.rust_host.len() > 128
            || !host
                .rust_host
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-'))
    {
        return Err("explicit_rust_host_required");
    }
    if kind == AdapterKind::Corepack
        && !host
            .dependencies
            .iter()
            .any(|d| d.path.file_name().is_some_and(|s| s == "node") && d.executable)
    {
        return Err("trusted_node_identity_required");
    }
    let integrity: Vec<_> = frozen
        .requirements
        .iter()
        .filter(|r| &r.tool == tool)
        .flat_map(|r| &r.constraints)
        .filter_map(|c| {
            if let Constraint::Distribution { integrity, .. } = c {
                Some(integrity)
            } else {
                None
            }
        })
        .collect();
    // Restored typed contracts can have been constructed without the parser.
    // Reuse its integrity validator before any descriptor reaches a manager.
    for value in &integrity {
        let parsed = Constraint::parse(&format!("{}+{}", selected.concrete, value))
            .map_err(|_| "invalid_distribution_integrity")?;
        if !matches!(parsed,Constraint::Distribution{ref version,ref integrity} if version==&selected.concrete && integrity==*value)
        {
            return Err("invalid_distribution_integrity");
        }
    }
    let identity = hash(&(
        kind,
        &selected.concrete,
        integrity,
        tool,
        &host.manager,
        &host.dependencies,
        &host.search_path,
        &host.rust_host,
    ));
    let root = state.join(&identity.as_str()[7..]);
    let payload = root.join("payload");
    let mut env = BTreeMap::new();
    for (key, sub) in [
        ("HOME", "home"),
        ("XDG_CONFIG_HOME", "config"),
        ("XDG_CACHE_HOME", "cache"),
        ("XDG_DATA_HOME", "data"),
        ("TMPDIR", "tmp"),
    ] {
        env.insert(key.into(), text(&root.join(sub))?);
    }
    env.insert(
        "PATH".into(),
        std::env::join_paths(&host.search_path)
            .map_err(|_| "invalid_host_path")?
            .into_string()
            .map_err(|_| "non_utf8_host_path")?,
    );
    env.insert("CI".into(), "1".into());
    let mut install_argv: Vec<String>;
    let bin;
    let mut programs: Vec<(&str, PathBuf)> = vec![];
    match kind {
        AdapterKind::Nodenv => {
            env.insert("NODENV_ROOT".into(), text(&payload)?);
            env.insert("NODENV_VERSION".into(), version.clone());
            install_argv = vec![
                version.clone(),
                text(&payload.join("versions").join(&version))?,
            ];
            bin = payload.join("versions").join(&version).join("bin");
            programs.push(("node", bin.join("node")));
        }
        AdapterKind::Rustup => {
            env.insert("RUSTUP_HOME".into(), text(&payload)?);
            env.insert("CARGO_HOME".into(), text(&root.join("cargo"))?);
            env.insert("RUSTUP_AUTO_INSTALL".into(), "0".into());
            let exact = format!("{version}-{}", host.rust_host);
            env.insert("RUSTUP_TOOLCHAIN".into(), exact.clone());
            install_argv = vec![
                "toolchain".into(),
                "install".into(),
                exact.clone(),
                "--profile".into(),
                "minimal".into(),
                "--no-self-update".into(),
            ];
            bin = payload.join("toolchains").join(exact).join("bin");
            programs.extend([("rustc", bin.join("rustc")), ("cargo", bin.join("cargo"))]);
        }
        AdapterKind::Mise => {
            env.insert("MISE_DATA_DIR".into(), text(&root.join("mise-data"))?);
            env.insert("MISE_CONFIG_DIR".into(), text(&root.join("config"))?);
            env.insert("MISE_NO_CONFIG".into(), "1".into());
            env.insert("MISE_NO_HOOKS".into(), "1".into());
            env.insert("MISE_NO_ENV".into(), "1".into());
            env.insert("MISE_STATE_DIR".into(), text(&root.join("data"))?);
            env.insert("MISE_TMP_DIR".into(), text(&root.join("tmp"))?);
            env.insert("MISE_CACHE_DIR".into(), text(&root.join("cache"))?);
            env.insert("MISE_AUTO_INSTALL".into(), "false".into());
            install_argv = vec![
                "install-into".into(),
                format!("{}@{version}", tool.as_str()),
                text(&payload)?,
            ];
            bin = payload.join("bin");
            let program = match tool.as_str() {
                "python" => "python3",
                other => other,
            };
            programs.push((program, bin.join(program)));
        }
        AdapterKind::Fvm => {
            env.insert("FVM_CACHE_PATH".into(), text(&payload)?);
            env.insert("FLUTTER_SUPPRESS_ANALYTICS".into(), "true".into());
            env.insert("DART_SUPPRESS_ANALYTICS".into(), "true".into());
            install_argv = vec!["install".into(), version.clone(), "--skip-pub-get".into()];
            bin = payload.join("versions").join(&version).join("bin");
            programs.push(("flutter", bin.join("flutter")));
        }
        AdapterKind::Corepack => {
            env.insert("COREPACK_HOME".into(), text(&payload)?);
            env.insert("COREPACK_ENABLE_AUTO_PIN".into(), "0".into());
            env.insert("COREPACK_DEFAULT_TO_LATEST".into(), "0".into());
            env.insert("COREPACK_ENABLE_PROJECT_SPEC".into(), "0".into());
            env.insert("COREPACK_ENABLE_DOWNLOAD_PROMPT".into(), "0".into());
            env.insert("COREPACK_ENV_FILE".into(), "0".into());
            let req = frozen
                .requirements
                .iter()
                .find(|r| &r.tool == tool)
                .ok_or("missing_requirement")?;
            let hashes: BTreeSet<_> = req
                .constraints
                .iter()
                .filter_map(|c| {
                    if let Constraint::Distribution { integrity, .. } = c {
                        Some(integrity.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            if hashes.len() > 1 {
                return Err("conflicting_distribution_integrity");
            }
            let suffix = hashes.first().map(|h| format!("+{h}")).unwrap_or_default();
            install_argv = vec![
                "install".into(),
                "--global".into(),
                format!("{}@{version}{suffix}", tool.as_str()),
            ];
            bin = root.join("bin");
            programs.push((tool.as_str(), bin.join(tool.as_str())));
        }
    }
    if kind != AdapterKind::Corepack
        && frozen
            .requirements
            .iter()
            .find(|r| &r.tool == tool)
            .is_some_and(|r| {
                r.constraints
                    .iter()
                    .any(|c| matches!(c, Constraint::Distribution { .. }))
            })
    {
        return Err("distribution_integrity_not_supported");
    }
    let installer = if kind == AdapterKind::Nodenv {
        host.dependencies
            .iter()
            .find(|d| d.path.file_name().is_some_and(|n| n == "node-build") && d.executable)
            .ok_or("trusted_node_build_required")?
            .path
            .clone()
    } else {
        host.manager.path.clone()
    };
    let mut commands = vec![CommandSpec {
        program: installer,
        argv: std::mem::take(&mut install_argv),
        env: env.clone(),
        cwd: root.clone(),
    }];
    if kind == AdapterKind::Corepack {
        commands.push(CommandSpec {
            program: host.manager.path.clone(),
            argv: vec![
                "enable".into(),
                "--install-directory".into(),
                text(&bin)?,
                tool.as_str().into(),
            ],
            env: env.clone(),
            cwd: root.clone(),
        });
    }
    // Go 1.21+ otherwise switches/downloads toolchains from go.mod/go.work
    // or persisted go env settings, violating the captured concrete selection.
    if tool.as_str() == "go" {
        env.insert("GOTOOLCHAIN".into(), "local".into());
    }
    env.insert("COREPACK_ENABLE_NETWORK".into(), "0".into());
    env.insert("RUSTUP_AUTO_INSTALL".into(), "0".into());
    env.insert("MISE_AUTO_INSTALL".into(), "false".into());
    let activated_path = std::iter::once(bin.clone())
        .chain(host.search_path.clone())
        .collect::<Vec<_>>();
    env.insert(
        "PATH".into(),
        std::env::join_paths(activated_path)
            .map_err(|_| "invalid_activation_path")?
            .into_string()
            .map_err(|_| "invalid_activation_path")?,
    );
    let probes = programs
        .into_iter()
        .map(|(name, program)| {
            let argv = if name == "flutter" {
                vec!["--version".into(), "--machine".into()]
            } else if name == "go" {
                vec!["version".into()]
            } else {
                vec!["--version".into()]
            };
            (
                name.to_owned(),
                CommandSpec {
                    program,
                    argv,
                    env: env.clone(),
                    cwd: root.clone(),
                },
            )
        })
        .collect();
    let mut external = vec![host.manager.clone()];
    external.extend(host.dependencies.clone());
    Ok(InstallPlan {
        kind,
        frozen: frozen.clone(),
        tool: tool.clone(),
        version: selected.concrete.clone(),
        root,
        payload,
        commands,
        probes,
        external,
        activation_env: env,
        activation_bin: bin,
    })
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LinkIdentity {
    path: PathBuf,
    target: PathBuf,
    resolved: FileIdentity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TreeIdentity {
    files: Vec<FileIdentity>,
    links: Vec<LinkIdentity>,
    directories: Vec<PathBuf>,
}
fn tree(plan: &InstallPlan) -> Result<TreeIdentity> {
    let mut out = TreeIdentity {
        files: vec![],
        links: vec![],
        directories: vec![],
    };
    let mut pending = vec![plan.payload.clone()];
    if plan.kind == AdapterKind::Corepack {
        pending.push(plan.activation_bin.clone());
    }
    let mut total = 0u64;
    while let Some(path) = pending.pop() {
        if out.files.len() + out.links.len() + out.directories.len() >= MAX_FILES {
            return Err("installation_tree_too_large");
        }
        let m = fs::symlink_metadata(&path).map_err(|_| "installation_tree_missing")?;
        if m.uid() != rustix::process::geteuid().as_raw()
            || (!m.file_type().is_symlink() && m.mode() & 0o022 != 0)
        {
            return Err("unsafe_installation_owner_or_mode");
        }
        if m.file_type().is_symlink() {
            let target = fs::read_link(&path).map_err(|_| "installation_link_read")?;
            let resolved = fs::canonicalize(&path).map_err(|_| "installation_link_dangling")?;
            if !resolved.starts_with(&plan.root)
                && !(plan.kind == AdapterKind::Corepack
                    && plan.external.iter().any(|f| f.path == resolved))
            {
                return Err("installation_link_escape");
            }
            let identity = FileIdentity::capture(&resolved)?;
            total = total
                .checked_add(identity.size)
                .ok_or("installation_tree_too_large")?;
            out.links.push(LinkIdentity {
                path,
                target,
                resolved: identity,
            });
        } else if m.is_dir() {
            out.directories.push(path.clone());
            for entry in fs::read_dir(&path).map_err(|_| "installation_tree_read")? {
                pending.push(entry.map_err(|_| "installation_tree_read")?.path());
                if pending.len() > MAX_FILES {
                    return Err("installation_tree_too_large");
                }
            }
        } else if m.is_file() && m.nlink() == 1 {
            let identity = FileIdentity::capture(&path)?;
            total = total
                .checked_add(identity.size)
                .ok_or("installation_tree_too_large")?;
            out.files.push(identity);
        } else {
            return Err("installation_special_or_hardlink");
        }
        if total > MAX_TOTAL {
            return Err("installation_tree_too_large");
        }
    }
    out.files.sort_by(|a, b| a.path.cmp(&b.path));
    out.links.sort_by(|a, b| a.path.cmp(&b.path));
    out.directories.sort();
    Ok(out)
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationReceipt {
    schema: u32,
    plan: Digest,
    version: Version,
    root: PathBuf,
    tree: TreeIdentity,
    external: Vec<FileIdentity>,
    probe_outputs: BTreeMap<String, Digest>,
}
impl InstallationReceipt {
    pub fn digest(&self) -> Digest {
        hash(self)
    }
    pub fn version(&self) -> &Version {
        &self.version
    }
    pub fn validate(&self, plan: &InstallPlan) -> Result<()> {
        owned_dir(&plan.root)?;
        self.validate_contract(plan)?;
        for dep in &self.external {
            dep.validate()?;
        }
        if tree(plan)? != self.tree {
            return Err("installation_tree_drift");
        }
        for (_, probe) in &plan.probes {
            let actual = fs::canonicalize(&probe.program).map_err(|_| "installed_tool_missing")?;
            let identity = FileIdentity::capture(&actual)?;
            if !identity.executable {
                return Err("installed_tool_not_executable");
            }
            if !actual.starts_with(&plan.root) && !plan.external.contains(&identity) {
                return Err("installed_tool_escape");
            }
        }
        Ok(())
    }
    fn validate_contract(&self, plan: &InstallPlan) -> Result<()> {
        if self.schema != 1
            || self.plan != plan.key()
            || self.version != plan.version
            || self.root != plan.root
            || self.external != plan.external
            || self.probe_outputs.len() != plan.probes.len()
            || plan
                .probes
                .iter()
                .any(|(p, _)| !self.probe_outputs.contains_key(p))
        {
            return Err("installation_receipt_mismatch");
        }

        Ok(())
    }
}
fn read_receipt(plan: &InstallPlan) -> Result<Option<InstallationReceipt>> {
    let path = plan.root.join("receipt.json");
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("receipt_open"),
    };
    let m = file.metadata().map_err(|_| "receipt_metadata")?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != rustix::process::geteuid().as_raw()
        || m.mode() & 0o077 != 0
        || m.len() > MAX_RECEIPT
    {
        return Err("unsafe_receipt");
    }
    let mut bytes = vec![];
    (&mut file)
        .take(MAX_RECEIPT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "receipt_read")?;
    if bytes.len() as u64 != m.len() {
        return Err("receipt_changed");
    }
    let receipt: InstallationReceipt =
        serde_json::from_slice(&bytes).map_err(|_| "receipt_invalid")?;
    receipt.validate(plan)?;
    Ok(Some(receipt))
}
/// Strictly read-only. Does not create state or execute any manager/native probe.
pub fn detect(plan: &InstallPlan) -> Availability {
    match fs::symlink_metadata(&plan.root) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Availability::Missing,
        Err(_) => return Availability::Unknown("private_root_unreadable"),
        Ok(_) => (),
    }
    if let Err(e) = owned_dir(&plan.root) {
        return Availability::Unknown(e);
    }
    match read_receipt(plan) {
        Ok(Some(r)) => Availability::Installed(Box::new(r)),
        Ok(None) => Availability::Unknown("installation_incomplete_or_unreceipted"),
        Err(e) => Availability::Unknown(e),
    }
}
fn output_version(name: &str, bytes: &[u8]) -> Result<Version> {
    if bytes.len() > 8192 {
        return Err("probe_output_limit");
    }
    let s = std::str::from_utf8(bytes)
        .map_err(|_| "probe_output_utf8")?
        .trim();
    let value = if name == "flutter" {
        let json: serde_json::Value =
            serde_json::from_str(s).map_err(|_| "flutter_probe_invalid")?;
        return Version::parse(
            json.get("frameworkVersion")
                .and_then(|v| v.as_str())
                .ok_or("flutter_probe_invalid")?,
        )
        .map_err(|_| "probe_version_invalid");
    } else if matches!(name, "rustc" | "cargo" | "python3" | "ruby") {
        let mut words = s.split_whitespace();
        let prefix = words.next().ok_or("probe_version_invalid")?;
        let expected = if name == "python3" { "Python" } else { name };
        if prefix != expected {
            return Err("probe_prefix_invalid");
        }
        words.next().ok_or("probe_version_invalid")?
    } else if name == "go" {
        s.strip_prefix("go version go")
            .and_then(|s| s.split_whitespace().next())
            .ok_or("probe_prefix_invalid")?
    } else {
        s.strip_prefix('v').unwrap_or(s)
    };
    Version::parse(&normalize_probe_version(name, value)).map_err(|_| "probe_version_invalid")
}
/// Ruby before 3.2 prints `X.Y.ZpN` (patchlevel suffix) and Go before 1.21 prints
/// `X.Y` for `X.Y.0`; normalize only those two shapes to a strict three-part version.
fn normalize_probe_version(name: &str, value: &str) -> String {
    match name {
        "ruby" => match value.split_once('p') {
            Some((base, level))
                if !level.is_empty()
                    && level.bytes().all(|b| b.is_ascii_digit())
                    && base.split('.').count() == 3 =>
            {
                base.to_owned()
            }
            _ => value.to_owned(),
        },
        "go" if value.split('.').count() == 2
            && value
                .split('.')
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit())) =>
        {
            format!("{value}.0")
        }
        _ => value.to_owned(),
    }
}
/// Explicit install only. This nonblocking publication lock is not a scheduler
/// or setup singleflight. Partial roots require owned cleanup, never silent reuse.
pub fn install_admitted(
    plan: &InstallPlan,
    executor: &mut impl AdmittedExecutor,
) -> Result<InstallationReceipt> {
    for external in &plan.external {
        external.validate()?;
    }
    mkdir(&plan.root)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(plan.root.join("install.lock"))
        .map_err(|_| "install_lock_open")?;
    let m = lock.metadata().map_err(|_| "install_lock_metadata")?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != rustix::process::geteuid().as_raw()
        || m.mode() & 0o077 != 0
    {
        return Err("unsafe_install_lock");
    }
    lock.try_lock().map_err(|_| "installation_busy")?;
    if let Some(receipt) = read_receipt(plan)? {
        return Ok(receipt);
    }
    if fs::read_dir(&plan.root)
        .map_err(|_| "install_directory_read")?
        .any(|e| e.map_or(true, |e| e.file_name() != "install.lock"))
    {
        return Err("installation_partial_requires_cleanup");
    }
    for sub in [
        "home",
        "config",
        "cache",
        "data",
        "tmp",
        "cargo",
        "mise-data",
        "bin",
    ] {
        mkdir(&plan.root.join(sub))?;
    }
    for command in &plan.commands {
        let out = executor.execute(command)?;
        if !out.success {
            return Err("installation_command_failed");
        }
        if out.stdout.len() > 8192 {
            return Err("installer_output_limit");
        }
    }
    let mut probe_outputs = BTreeMap::new();
    for (name, command) in &plan.probes {
        let out = executor.execute(command)?;
        if !out.success || output_version(name, &out.stdout)? != plan.version {
            return Err("installed_version_mismatch");
        }
        probe_outputs.insert(name.clone(), Digest::of_bytes(&out.stdout));
    }
    let receipt = InstallationReceipt {
        schema: 1,
        plan: plan.key(),
        version: plan.version.clone(),
        root: plan.root.clone(),
        tree: tree(plan)?,
        external: plan.external.clone(),
        probe_outputs,
    };
    receipt.validate(plan)?;
    let bytes = serde_json::to_vec(&receipt).map_err(|_| "receipt_encode")?;
    if bytes.len() as u64 > MAX_RECEIPT {
        return Err("receipt_too_large");
    }
    let temporary = plan.root.join("receipt.pending");
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&temporary)
        .map_err(|_| "receipt_create")?;
    f.write_all(&bytes)
        .and_then(|_| f.sync_all())
        .map_err(|_| "receipt_write")?;
    fs::rename(temporary, plan.root.join("receipt.json")).map_err(|_| "receipt_publish")?;
    File::open(&plan.root)
        .and_then(|f| f.sync_all())
        .map_err(|_| "receipt_directory_sync")?;
    drop(lock);
    Ok(receipt)
}
/// Internal worker activation, never a TaskSpec PATH override.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationReceipt {
    pub base_profile: Digest,
    pub frozen: Digest,
    pub setup_policy: Digest,
    pub installation: Digest,
    pub environment: BTreeMap<String, String>,
    pub shell: FileIdentity,
    /// Source/contract binding, separate from actual environment identity.
    pub binding_digest: Digest,
    pub execution_fingerprint: Digest,
}
pub fn activate(
    plan: &InstallPlan,
    receipt: &InstallationReceipt,
    base_profile: Digest,
    base_env: &BTreeMap<String, String>,
    setup_policy: Digest,
    shell: &FileIdentity,
) -> Result<ActivationReceipt> {
    if plan.frozen.selections.len() != 1 || !plan.frozen.baseline.is_empty() {
        return Err("complete_activation_set_required");
    }
    activate_set(
        &plan.frozen,
        &[(plan, receipt)],
        base_profile,
        &BTreeMap::new(),
        base_env,
        setup_policy,
        shell,
    )
}
/// Complete activation preserves operator consumer HOME/XDG/TMP and isolates
/// manager roots only. No submitted command env can choose these overrides.
#[allow(clippy::too_many_arguments)]
pub fn activate_set(
    frozen: &FrozenToolchains,
    installed: &[(&InstallPlan, &InstallationReceipt)],
    base_profile: Digest,
    base_observations: &BTreeMap<ToolId, super::versions::VersionObservation>,
    base_env: &BTreeMap<String, String>,
    setup_policy: Digest,
    shell: &FileIdentity,
) -> Result<ActivationReceipt> {
    compose_activation(
        frozen,
        installed,
        base_profile,
        base_observations,
        base_env,
        setup_policy,
        shell,
        || shell.validate(),
        |plan, receipt| receipt.validate(plan),
    )
}
#[allow(clippy::too_many_arguments)]
fn compose_activation(
    frozen: &FrozenToolchains,
    installed: &[(&InstallPlan, &InstallationReceipt)],
    base_profile: Digest,
    base_observations: &BTreeMap<ToolId, super::versions::VersionObservation>,
    base_env: &BTreeMap<String, String>,
    setup_policy: Digest,
    shell: &FileIdentity,
    validate_shell: impl FnOnce() -> Result<()>,
    validate_receipt: impl Fn(&InstallPlan, &InstallationReceipt) -> Result<()>,
) -> Result<ActivationReceipt> {
    use super::versions::{Match, MatchPolicy, VersionObservation};
    if !frozen.is_resolved() || installed.len() != frozen.selections.len() || installed.len() > 256
    {
        return Err("complete_activation_set_required");
    }
    if !base_env
        .get("HOME")
        .is_some_and(|p| clean_absolute(Path::new(p)))
        || !base_env.contains_key("PATH")
    {
        return Err("invalid_base_environment");
    }
    for b in &frozen.baseline {
        if frozen.policy.compare(
            &b.version,
            base_observations
                .get(&b.tool)
                .unwrap_or(&VersionObservation::Unknown),
        ) != Match::Match
        {
            return Err("activation_baseline_mismatch");
        }
    }
    validate_shell()?;
    if !shell.executable {
        return Err("activation_shell_not_executable");
    }
    let mut environment = base_env.clone();
    let mut paths = vec![];
    let mut overrides = BTreeMap::new();
    let mut tools = BTreeSet::new();
    let mut receipts = vec![];
    let mut ordered = installed.to_vec();
    ordered.sort_by(|a, b| a.0.tool.cmp(&b.0.tool));
    for (plan, receipt) in ordered {
        if plan.frozen != *frozen || !tools.insert(plan.tool.clone()) {
            return Err("activation_contract_mismatch");
        }
        validate_receipt(plan, receipt)?;
        if MatchPolicy::Exact.compare(
            &VersionObservation::Known(plan.version.clone()),
            &VersionObservation::Known(receipt.version.clone()),
        ) != Match::Match
        {
            return Err("activation_version_mismatch");
        }
        paths.push(plan.activation_bin.clone());
        for (key, value) in &plan.activation_env {
            if matches!(
                key.as_str(),
                "PATH"
                    | "HOME"
                    | "XDG_CONFIG_HOME"
                    | "XDG_CACHE_HOME"
                    | "XDG_DATA_HOME"
                    | "TMPDIR"
                    | "CI"
            ) {
                continue;
            }
            if let Some(previous) = overrides.insert(key.clone(), value.clone()) {
                if previous != *value {
                    return Err("conflicting_activation_environment");
                }
            }
        }
        receipts.push((plan.tool.clone(), receipt.digest()));
    }
    if frozen.selections.iter().any(|s| !tools.contains(&s.tool)) {
        return Err("complete_activation_set_required");
    }
    if let Some(path) = base_env.get("PATH") {
        for p in std::env::split_paths(path) {
            if !clean_absolute(&p) {
                return Err("invalid_base_path");
            }
            paths.push(p);
        }
    }
    let mut unique = BTreeSet::new();
    paths.retain(|p| unique.insert(p.clone()));
    environment.extend(overrides);
    environment.insert(
        "PATH".into(),
        std::env::join_paths(paths)
            .map_err(|_| "invalid_activation_path")?
            .into_string()
            .map_err(|_| "invalid_activation_path")?,
    );
    receipts.sort_by(|a, b| a.0.cmp(&b.0));
    let installation = hash(&receipts);
    let frozen = frozen.digest();
    let binding_digest = hash(&(&base_profile, &frozen, &setup_policy, &installation));
    let execution_fingerprint = hash(&(
        &base_profile,
        &setup_policy,
        &installation,
        &environment,
        shell,
    ));
    Ok(ActivationReceipt {
        base_profile,
        frozen,
        setup_policy,
        installation,
        environment,
        shell: shell.clone(),
        binding_digest,
        execution_fingerprint,
    })
}
#[cfg(test)]
mod tests;
