//! Immutable worker-local closed adapter configuration. Never deserialize this from a task.
use super::adapters::{
    self, AdapterKind, Availability, FileIdentity, HostTools, ResolutionEvidence,
};
use super::preparation::PreparationRequest;
use super::profile::Profile;
use super::versions::{Constraint, FrozenToolchains, ToolId};
use crate::ids::Digest;
use node_semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

const MAX_CONFIG: u64 = 64 * 1024;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    schema_version: u32,
    active_profile: String,
    profile_definition: Digest,
    recipes: Vec<Recipe>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Recipe {
    tool: ToolId,
    kind: AdapterKind,
    manager: PathBuf,
    #[serde(default)]
    dependencies: Vec<PathBuf>,
    search_path: Vec<PathBuf>,
    #[serde(default)]
    rust_host: String,
    inventory: Vec<Version>,
    #[serde(default)]
    resolved_inputs: BTreeMap<String, Version>,
}
#[derive(Debug, Clone)]
struct Registered {
    recipe: Recipe,
    host: HostTools,
    catalog: Digest,
}
/// What one recipe may install and what is installed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstalledInventory {
    pub inventory: Vec<String>,
    pub installed: Vec<String>,
}
/// A negative result is deliberately distinct from an installable request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unavailable {
    Unconfigured,
    Unsupported,
    ManualRequired(&'static str),
    ResolveRequired,
    BaselineCheckRequired,
    NotInInventory,
    Invalid(&'static str),
}
impl Unavailable {
    /// No managed install can serve the selection, so the tool the worker's
    /// own profile already has may: at exactly the selected version nothing
    /// is left to install, by belt or by a person. Every other refusal stays
    /// a refusal.
    pub fn allows_native(&self) -> bool {
        matches!(
            self,
            Self::Unconfigured | Self::Unsupported | Self::NotInInventory | Self::ManualRequired(_)
        )
    }
}
/// Retain for the resident lifetime. Configuration changes require drain/restart.
/// Inventory means operator-authorized versions, not installed-version claims.
#[derive(Debug, Clone)]
pub struct Registry {
    root: PathBuf,
    profile_definition: Digest,
    base_profile: Digest,
    configuration: Option<Digest>,
    recipes: BTreeMap<ToolId, Registered>,
    /// Installers found with no configuration (spec 22.1): the user's own
    /// `mise` for node, python, go and ruby, and `rustup` for rust, on the
    /// profile PATH, each only for a tool no recipe names. They serve an
    /// exact version already selected, and only when no native tool does.
    installers: BTreeMap<ToolId, Registered>,
}
/// The tools the user's own mise installs for belt.
pub const MISE_TOOLS: &[&str] = &["node", "python", "go", "ruby"];
/// What a person runs to give a worker mise (spec 22.1).
pub const MISE_INSTALL_HINT: &str =
    "install mise: `brew install mise`, or `curl https://mise.run | sh` (https://mise.jdx.dev)";
/// The executable named `name` in the first directory of `dirs` that has
/// one, resolved through links. A target with another file name is not
/// taken: rustup and its proxies choose what they do by that name.
pub fn find_installer(dirs: &[PathBuf], name: &str) -> Option<PathBuf> {
    dirs.iter().find_map(|dir| {
        let candidate = dir.join(name);
        let meta = std::fs::metadata(&candidate).ok()?;
        if !meta.is_file() || meta.mode() & 0o111 == 0 {
            return None;
        }
        let real = std::fs::canonicalize(&candidate).ok()?;
        (real.file_name()? == name && clean_absolute(&real)).then_some(real)
    })
}
impl Registry {
    /// `actual_fingerprint` must come from the checked worker's existing profile
    /// measurement. It is not task input. Setups independently checks it at admission.
    /// `state/toolchains` must already exist (Setups::private_tool_root owns creation).
    pub fn load(
        state: &Path,
        profile: &Profile,
        actual_fingerprint: Digest,
    ) -> Result<Self, String> {
        let root = state.join("toolchains");
        private_root(&root)?;
        let mut registry = Self {
            root,
            profile_definition: profile.definition_digest().clone(),
            base_profile: actual_fingerprint,
            configuration: None,
            recipes: BTreeMap::new(),
            installers: BTreeMap::new(),
        };
        let Some(bytes) = read_configuration(&state.join("toolchains.toml"))? else {
            registry.discover_installers(profile);
            return Ok(registry);
        };
        let config: Configuration =
            toml::from_str(std::str::from_utf8(&bytes).map_err(|_| "toolchains_utf8")?)
                .map_err(|_| "invalid_toolchains_configuration")?;
        if config.schema_version != 1
            || config.recipes.len() > 32
            || config.active_profile != profile.name()
            || config.profile_definition != *profile.definition_digest()
        {
            return Err("toolchains_profile_or_schema_mismatch".into());
        }
        registry.configuration = Some(Digest::of_bytes(&bytes));
        for recipe in config.recipes {
            validate_recipe(&recipe)?;
            if registry.recipes.contains_key(&recipe.tool) {
                return Err("duplicate_toolchain_recipe".into());
            }
            let host = HostTools {
                manager: identity(&recipe.manager)?,
                dependencies: recipe
                    .dependencies
                    .iter()
                    .map(|p| identity(p))
                    .collect::<Result<_, _>>()?,
                search_path: recipe.search_path.clone(),
                rust_host: recipe.rust_host.clone(),
            };
            let required_dependency = match recipe.kind {
                AdapterKind::Nodenv => Some("node-build"),
                AdapterKind::Corepack => Some("node"),
                _ => None,
            };
            if !host.manager.executable
                || required_dependency.is_some_and(|name| {
                    !host
                        .dependencies
                        .iter()
                        .any(|d| d.executable && d.path.file_name().is_some_and(|n| n == name))
                })
            {
                return Err("toolchains_manager_or_dependency_not_executable".into());
            }
            let catalog =
                Digest::of_bytes(&serde_json::to_vec(&recipe).expect("recipe serializes"));
            registry.recipes.insert(
                recipe.tool.clone(),
                Registered {
                    recipe,
                    host,
                    catalog,
                },
            );
        }
        registry.discover_installers(profile);
        Ok(registry)
    }
    /// Read-only: finds `mise` and `rustup` on the profile PATH and captures
    /// their identity. Nothing runs. One that cannot be taken is no installer.
    fn discover_installers(&mut self, profile: &Profile) {
        let search_path: Vec<PathBuf> = profile
            .path_dirs()
            .iter()
            .filter(|p| canonical(p).is_ok() && p.is_dir())
            .take(32)
            .cloned()
            .collect();
        if search_path.is_empty() {
            return;
        }
        let mut wanted: Vec<(AdapterKind, &str, &str, String)> = MISE_TOOLS
            .iter()
            .map(|tool| (AdapterKind::Mise, *tool, "mise", String::new()))
            .collect();
        if let Some(host) = super::profile::rustup_host() {
            wanted.push((AdapterKind::Rustup, "rust", "rustup", host));
        }
        for (kind, tool, name, rust_host) in wanted {
            let tool = ToolId::parse(tool).expect("known tool");
            if self.recipes.contains_key(&tool) {
                continue;
            }
            let Some(manager) = find_installer(profile.path_dirs(), name)
                .and_then(|p| identity(&p).ok())
                .filter(|m| m.executable)
            else {
                continue;
            };
            let recipe = Recipe {
                tool: tool.clone(),
                kind,
                manager: manager.path.clone(),
                dependencies: Vec::new(),
                search_path: search_path.clone(),
                rust_host,
                inventory: Vec::new(),
                resolved_inputs: BTreeMap::new(),
            };
            let catalog = Digest::of_bytes(
                &serde_json::to_vec(&("belt_installer_v1", &recipe, &manager.digest))
                    .expect("recipe serializes"),
            );
            let host = HostTools {
                manager,
                dependencies: Vec::new(),
                search_path: search_path.clone(),
                rust_host: recipe.rust_host.clone(),
            };
            self.installers.insert(
                tool,
                Registered {
                    recipe,
                    host,
                    catalog,
                },
            );
        }
    }
    /// Whether this worker can install versions of `tool` it lacks: a recipe
    /// names it, or the user's own mise or rustup is on the profile PATH.
    pub fn can_install(&self, tool: &str) -> bool {
        ToolId::parse(tool)
            .is_ok_and(|t| self.recipes.contains_key(&t) || self.installers.contains_key(&t))
    }
    /// The installer found for `tool` with no configuration, if any.
    pub fn installer(&self, tool: &str) -> Option<&Path> {
        let tool = ToolId::parse(tool).ok()?;
        self.installers
            .get(&tool)
            .map(|e| e.host.manager.path.as_path())
    }
    /// The install of an exact version already selected (`evidence`), by the
    /// user's own mise or rustup, into belt's private root. Callers ask only
    /// after the registry and the native tool both refused, so a version the
    /// worker has is never installed again. Read-only.
    pub fn install_request(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: &ResolutionEvidence,
    ) -> Result<PreparationRequest, Unavailable> {
        self.request_from(true, frozen, tool, Some(evidence), None)
            .map(|v| v.0)
    }
    pub(crate) fn probe_install_plan_request(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: &ResolutionEvidence,
        budget: &adapters::IdentityReadBudget,
    ) -> Result<(PreparationRequest, adapters::InstallPlan), Unavailable> {
        self.probe_from(true, frozen, tool, evidence, budget)
    }
    pub fn configuration_digest(&self) -> Option<&Digest> {
        self.configuration.as_ref()
    }

    /// Read-only: no manager commands, discovery, config reread or state creation.
    /// Parent evidence retains its original authority; local resolution is used
    /// only when the tool has no frozen selection and no evidence is supplied.
    pub fn request(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: Option<&ResolutionEvidence>,
    ) -> Result<PreparationRequest, Unavailable> {
        self.request_with_limit(frozen, tool, evidence, None)
            .map(|v| v.0)
    }

    #[cfg(test)]
    pub(crate) fn probe_request(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: &ResolutionEvidence,
        budget: &adapters::IdentityReadBudget,
    ) -> Result<PreparationRequest, Unavailable> {
        self.probe_plan_request(frozen, tool, evidence, budget)
            .map(|v| v.0)
    }
    pub(crate) fn probe_plan_request(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: &ResolutionEvidence,
        budget: &adapters::IdentityReadBudget,
    ) -> Result<(PreparationRequest, adapters::InstallPlan), Unavailable> {
        self.probe_from(false, frozen, tool, evidence, budget)
    }
    fn probe_from(
        &self,
        installer: bool,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: &ResolutionEvidence,
        budget: &adapters::IdentityReadBudget,
    ) -> Result<(PreparationRequest, adapters::InstallPlan), Unavailable> {
        use crate::setup::preparation_probe::bounded_path;
        bounded_path(&self.root).map_err(|_| Unavailable::Invalid("probe_root"))?;
        let entries = if installer {
            &self.installers
        } else {
            &self.recipes
        };
        let entry = entries.get(tool).ok_or(Unavailable::Unsupported)?;
        if entry.host.dependencies.len() > 8 || entry.host.search_path.len() > 32 {
            return Err(Unavailable::Invalid("probe_budget"));
        }
        for path in std::iter::once(&entry.host.manager.path)
            .chain(entry.host.dependencies.iter().map(|d| &d.path))
            .chain(entry.host.search_path.iter())
        {
            bounded_path(path).map_err(|_| Unavailable::Invalid("probe_path"))?;
        }
        self.request_from(installer, frozen, tool, Some(evidence), Some(budget))
    }

    fn request_with_limit(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: Option<&ResolutionEvidence>,
        max_bytes: Option<&adapters::IdentityReadBudget>,
    ) -> Result<(PreparationRequest, adapters::InstallPlan), Unavailable> {
        self.request_from(false, frozen, tool, evidence, max_bytes)
    }
    fn request_from(
        &self,
        installer: bool,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        evidence: Option<&ResolutionEvidence>,
        max_bytes: Option<&adapters::IdentityReadBudget>,
    ) -> Result<(PreparationRequest, adapters::InstallPlan), Unavailable> {
        frozen.validate().map_err(Unavailable::Invalid)?;
        if frozen
            .baseline
            .iter()
            .any(|baseline| &baseline.tool == tool)
        {
            return Err(Unavailable::BaselineCheckRequired);
        }
        if let Some(reason) = adapters::manual_requirement(tool.as_str()) {
            return Err(Unavailable::ManualRequired(reason));
        }
        let entry = if installer {
            self.installers.get(tool).ok_or(Unavailable::Unsupported)?
        } else {
            self.recipes
                .get(tool)
                .ok_or(if self.configuration.is_none() {
                    Unavailable::Unconfigured
                } else {
                    Unavailable::Unsupported
                })?
        };
        let selected = frozen.selections.iter().find(|s| &s.tool == tool);
        let (resolved, evidence) = if let Some(evidence) = evidence {
            if evidence.tool != *tool || selected.is_none() {
                return Err(Unavailable::Invalid("selected_parent_evidence_required"));
            }
            let mut prior = frozen.clone();
            prior.selections.retain(|s| &s.tool != tool);
            if adapters::resolve(&prior, evidence).map_err(Unavailable::Invalid)? != *frozen {
                return Err(Unavailable::Invalid("parent_resolution_mismatch"));
            }
            // An installer has no inventory: it serves the exact version the
            // contract already selected, whatever selected it.
            if !installer && !entry.recipe.inventory.contains(&evidence.concrete) {
                return Err(Unavailable::NotInInventory);
            }
            (frozen.clone(), evidence.clone())
        } else {
            // An installer never chooses a version.
            if installer {
                return Err(Unavailable::ResolveRequired);
            }
            if selected.is_some() {
                return Err(Unavailable::ResolveRequired);
            }
            let requirement = frozen
                .requirements
                .iter()
                .find(|r| &r.tool == tool)
                .ok_or(Unavailable::Invalid("resolution_tool_missing"))?;
            let mut inputs = BTreeMap::new();
            for c in &requirement.constraints {
                if matches!(
                    c,
                    Constraint::Expression(_) | Constraint::VersionFileExpression(_)
                ) {
                    // Static operator mappings cannot attest a captured CI matrix leg.
                    return Err(Unavailable::ResolveRequired);
                }
                if let Constraint::Resolve(s) = c {
                    inputs.insert(
                        s.clone(),
                        entry
                            .recipe
                            .resolved_inputs
                            .get(s)
                            .ok_or(Unavailable::ResolveRequired)?
                            .clone(),
                    );
                }
            }
            let concrete = entry
                .recipe
                .inventory
                .iter()
                .filter(|version| {
                    inputs.values().all(|v| v == *version)
                        && frozen
                            .with_selection(tool, (*version).clone(), entry.catalog.clone())
                            .is_ok()
                })
                .max()
                .ok_or(Unavailable::NotInInventory)?
                .clone();
            let mut unselected = frozen.clone();
            unselected.selections.clear();
            let evidence = ResolutionEvidence {
                contract: unselected.digest(),
                tool: tool.clone(),
                concrete,
                catalog: entry.catalog.clone(),
                resolved_inputs: inputs,
            };
            let resolved = adapters::resolve(frozen, &evidence).map_err(Unavailable::Invalid)?;
            (resolved, evidence)
        };
        let plan = match max_bytes {
            Some(limit) => adapters::plan_with_limit(
                entry.recipe.kind,
                &resolved,
                tool,
                &evidence,
                &self.root,
                &entry.host,
                limit,
            ),
            None => adapters::plan(
                entry.recipe.kind,
                &resolved,
                tool,
                &evidence,
                &self.root,
                &entry.host,
            ),
        }
        .map_err(Unavailable::Invalid)?;
        Ok((
            PreparationRequest {
                kind: entry.recipe.kind,
                frozen: resolved,
                tool: tool.clone(),
                evidence,
                private_root: self.root.clone(),
                host: entry.host.clone(),
                expected_plan: plan.key(),
                profile_definition: self.profile_definition.clone(),
                base_profile: self.base_profile.clone(),
            },
            plan,
        ))
    }
    /// The install of the setup tool `tool` with the rust the attempt runs
    /// (spec 22.1): `rust_bin` is that toolchain's `bin`, which has the real
    /// `cargo` and `rustc` (a rustup proxy is refused: it would read the
    /// user's rustup home from the private one). `rust` is the selection's
    /// evidence. Read-only: identities are hashed, nothing is run.
    pub fn cargo_request(
        &self,
        frozen: &FrozenToolchains,
        tool: &ToolId,
        rust: &adapters::ResolutionEvidence,
        rust_bin: &Path,
    ) -> Result<PreparationRequest, Unavailable> {
        let program = |name: &str| -> Result<adapters::FileIdentity, Unavailable> {
            let path = std::fs::canonicalize(rust_bin.join(name))
                .map_err(|_| Unavailable::Invalid("cargo_unavailable"))?;
            if path.file_name().is_none_or(|n| n != name) {
                return Err(Unavailable::Invalid("cargo_proxy_unsupported"));
            }
            adapters::FileIdentity::capture(&path).map_err(Unavailable::Invalid)
        };
        let cargo = program("cargo")?;
        let rustc = program("rustc")?;
        let mut search_path = vec![cargo
            .path
            .parent()
            .ok_or(Unavailable::Invalid("cargo_unavailable"))?
            .to_path_buf()];
        // The linker cargo calls (`cc`).
        for dir in ["/usr/bin", "/bin"] {
            if !search_path.iter().any(|p| p == Path::new(dir)) {
                search_path.push(dir.into());
            }
        }
        let host = adapters::HostTools {
            manager: cargo,
            dependencies: vec![rustc],
            search_path,
            rust_host: String::new(),
        };
        let plan = adapters::plan(
            adapters::AdapterKind::Cargo,
            frozen,
            tool,
            rust,
            &self.root,
            &host,
        )
        .map_err(Unavailable::Invalid)?;
        Ok(PreparationRequest {
            kind: adapters::AdapterKind::Cargo,
            frozen: frozen.clone(),
            tool: tool.clone(),
            evidence: rust.clone(),
            private_root: self.root.clone(),
            host,
            expected_plan: plan.key(),
            profile_definition: self.profile_definition.clone(),
            base_profile: self.base_profile.clone(),
        })
    }
    /// The request for one operator-authorized exact version, as a warm-up
    /// asks for it: no project, no parent evidence. Its install plan (and so
    /// its singleflight key and private location) is the one an attempt that
    /// pins the same version gets. Read-only.
    pub fn inventory_request(
        &self,
        tool: &ToolId,
        version: &Version,
    ) -> Result<PreparationRequest, Unavailable> {
        use super::versions::{Declaration, MatchPolicy, Scope};
        let scope = Scope::project("");
        let constraint = Constraint::parse(&version.to_string())
            .map_err(|_| Unavailable::Invalid("inventory_version"))?;
        let frozen = FrozenToolchains::freeze(
            Digest::of_bytes(b"installed inventory"),
            self.profile_definition.clone(),
            scope.clone(),
            &[Declaration {
                tool: tool.clone(),
                constraint,
                scope,
                location: super::versions::Location {
                    path: "inventory".into(),
                    first_line: 1,
                    last_line: 1,
                },
            }],
            &[],
            &BTreeMap::new(),
            MatchPolicy::Exact,
        )
        .map_err(|_| Unavailable::Invalid("inventory_contract"))?;
        self.request(&frozen, tool, None)
    }
    /// For each configured recipe: the operator-authorized inventory and the
    /// subset whose private install is present and receipt-valid right now.
    /// Read-only (the same detect used at admission); a version that cannot be
    /// proven installed counts as not installed, never as installed.
    pub fn installed_inventory(&self) -> BTreeMap<String, InstalledInventory> {
        let mut out = BTreeMap::new();
        for (tool, entry) in &self.recipes {
            let mut installed = Vec::new();
            for version in &entry.recipe.inventory {
                let present = self
                    .inventory_request(tool, version)
                    .ok()
                    .and_then(|request| self.detect(&request).ok())
                    .is_some_and(|a| matches!(a, Availability::Installed(_)));
                if present {
                    installed.push(version.to_string());
                }
            }
            out.insert(
                tool.as_str().to_string(),
                InstalledInventory {
                    inventory: entry
                        .recipe
                        .inventory
                        .iter()
                        .map(|v| v.to_string())
                        .collect(),
                    installed,
                },
            );
        }
        out
    }
    /// Detect only the indexed exact artifact using its validated adapter receipt.
    pub fn detect(&self, request: &PreparationRequest) -> Result<Availability, Unavailable> {
        let expected = match self.request(&request.frozen, &request.tool, Some(&request.evidence)) {
            _ if request.kind == adapters::AdapterKind::Cargo => {
                let bin = request
                    .host
                    .search_path
                    .first()
                    .ok_or(Unavailable::Invalid("cargo_unavailable"))?;
                self.cargo_request(&request.frozen, &request.tool, &request.evidence, bin)?
            }
            Ok(expected) => expected,
            Err(refused) if refused.allows_native() => {
                self.install_request(&request.frozen, &request.tool, &request.evidence)?
            }
            Err(refused) => return Err(refused),
        };
        if expected.digest() != request.digest() {
            return Err(Unavailable::Invalid("registry_request_binding"));
        }
        let plan = expected
            .plan(&self.root)
            .map_err(|_| Unavailable::Invalid("registry_plan_invalid"))?;
        Ok(adapters::detect(&plan))
    }
}
fn read_configuration(path: &Path) -> Result<Option<Vec<u8>>, String> {
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("toolchains_open".into()),
    };
    let metadata = file.metadata().map_err(|_| "toolchains_metadata")?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
        || metadata.len() > MAX_CONFIG
    {
        return Err("unsafe_toolchains_configuration".into());
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_CONFIG + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "toolchains_read")?;
    if bytes.len() as u64 > MAX_CONFIG {
        return Err("toolchains_configuration_limit".into());
    }
    Ok(Some(bytes))
}
fn clean_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path.to_str().is_some()
        && !path.as_os_str().as_encoded_bytes().contains(&b':')
        && path
            .components()
            .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
}
fn canonical(path: &Path) -> Result<(), String> {
    if !clean_absolute(path)
        || std::fs::canonicalize(path).map_err(|_| "toolchains_host_path")? != path
    {
        return Err("noncanonical_toolchains_path".into());
    }
    Ok(())
}
fn private_root(path: &Path) -> Result<(), String> {
    canonical(path)?;
    let m = std::fs::symlink_metadata(path).map_err(|_| "toolchains_root")?;
    if !m.is_dir() || m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o077 != 0 {
        return Err("unsafe_toolchains_root".into());
    }
    Ok(())
}
fn identity(path: &Path) -> Result<FileIdentity, String> {
    canonical(path)?;
    FileIdentity::capture(path).map_err(str::to_string)
}
fn validate_recipe(r: &Recipe) -> Result<(), String> {
    let supported = match r.kind {
        AdapterKind::Nodenv => r.tool.as_str() == "node",
        AdapterKind::Rustup => r.tool.as_str() == "rust",
        AdapterKind::Mise => matches!(r.tool.as_str(), "node" | "python" | "go" | "ruby"),
        AdapterKind::Fvm => r.tool.as_str() == "flutter",
        AdapterKind::Corepack => matches!(r.tool.as_str(), "pnpm" | "yarn" | "npm"),
        AdapterKind::Cargo => false,
    };
    if !supported
        || r.inventory.is_empty()
        || r.inventory.len() > 256
        || r.dependencies.len() > 32
        || r.search_path.is_empty()
        || r.search_path.len() > 32
        || r.resolved_inputs.len() > 256
        || r.inventory
            .iter()
            .enumerate()
            .any(|(i, v)| r.inventory[..i].contains(v))
        || r.resolved_inputs.iter().any(|(k, v)| {
            k.is_empty()
                || k.len() > 512
                || k.contains(['\0', '\n', '\r'])
                || !r.inventory.contains(v)
        })
        || (r.kind == AdapterKind::Rustup
            && (r.rust_host.is_empty()
                || r.rust_host.len() > 128
                || !r
                    .rust_host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')))
    {
        return Err("invalid_toolchain_recipe".into());
    }
    for p in &r.search_path {
        canonical(p)?;
        if !p.is_dir() {
            return Err("toolchains_search_path_not_directory".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
