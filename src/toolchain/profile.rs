//! Private worker-local profiles. No submitted project can supply this file.
use super::capability::{
    valid_name, valid_version, ToolObservation, WorkerCapabilities, FEATURES, TOOLS,
};
use crate::ids::Digest;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_PROFILE_BYTES: u64 = 64 * 1024;
const MAX_PROBE_BYTES: u64 = 8192;
const PROBE_TIMEOUT: Duration = Duration::from_millis(300);
/// Startup only. One call may take `call`; a call that timed out is repeated
/// while the tool has attempts left and the shared `total` has time left, and
/// a repeat ends with that time. The first call of a tool never gets less than
/// the live 300ms, so the wait is bounded by `total` plus 300ms for each of
/// the fixed calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StartupProbes {
    call: Duration,
    total: Duration,
    attempts: u32,
}
const STARTUP_PROBES: StartupProbes = StartupProbes {
    call: Duration::from_millis(2_000),
    total: Duration::from_millis(5_000),
    attempts: 3,
};
/// The shared clock of one startup measurement.
struct StartupClock {
    limits: StartupProbes,
    started: Instant,
}
impl StartupClock {
    fn start(limits: StartupProbes) -> Self {
        Self {
            limits,
            started: Instant::now(),
        }
    }
    fn remaining(&self) -> Duration {
        self.limits.total.saturating_sub(self.started.elapsed())
    }
    /// A first call: never less than the live deadline.
    fn call_deadline(&self) -> Duration {
        self.repeat_deadline().max(PROBE_TIMEOUT)
    }
    /// A repeated call ends with the shared time.
    fn repeat_deadline(&self) -> Duration {
        self.remaining().min(self.limits.call)
    }
}
/// Why a fixed probe produced no output. Only a timeout may be repeated: a
/// failed or unparsable probe is already a determinate answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeMiss {
    TimedOut,
    Failed,
}
const PROBE_DIR_PREFIX: &str = "belt-tool-probe-";
const PREPARATION_SNAPSHOT_TTL_MS: u64 = 5_000;
// Tool discovery must not bootstrap a manager-selected tool behind admission.
const INSTALL_GUARDS: &[(&str, &str)] = &[
    ("COREPACK_ENABLE_NETWORK", "0"),
    ("COREPACK_ENABLE_AUTO_PIN", "0"),
    ("COREPACK_DEFAULT_TO_LATEST", "0"),
    ("COREPACK_ENABLE_DOWNLOAD_PROMPT", "0"),
    ("RUSTUP_AUTO_INSTALL", "0"),
    ("MISE_AUTO_INSTALL", "false"),
];
// `XDG_STATE_HOME` decides (with `HOME`) which task-traffic-controller ledger
// a nested CLI uses, `XDG_CONFIG_HOME` which configuration, and
// `TASK_TRAFFIC_CONTROLLER_NESTED` how it nests: a command keeps the worker's
// value of each, set or unset. They say where the scope is, not what the
// toolchain is: they are kept apart from `env`, outside the profile digest,
// the fingerprint and the activation of an execution binding.
use crate::config::traffic::is_step_env as scope_env;
fn protected_env(key: &str) -> bool {
    matches!(key, "PATH" | "SHELL" | "HOME" | "TMPDIR")
        || scope_env(key)
        || INSTALL_GUARDS.iter().any(|(name, _)| *name == key)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    schema_version: u32,
    active: String,
    profiles: BTreeMap<String, Definition>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Definition {
    path: Vec<PathBuf>,
    shell: PathBuf,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    allowed_env: BTreeSet<String>,
    #[serde(default)]
    required_tools: BTreeSet<String>,
    #[serde(default)]
    secret_refs: BTreeMap<String, String>,
    cache_namespace: String,
}
/// Retained for the resident lifetime. Changes require operator drain/restart.
#[derive(Clone)]
pub struct Profile {
    embedded_bin: Option<PathBuf>,
    explicit_allowlist: bool,
    name: String,
    definition: Definition,
    env: BTreeMap<String, String>,
    digest: Digest,
    /// The scope variables every command gets on top of `env`
    /// (`config::traffic::step_env`). Not hashed.
    scope_env: BTreeMap<String, String>,
    report_identity: Arc<Mutex<Option<ReportIdentity>>>,
    completed_report: Arc<Mutex<Option<CompletedReport>>>,
    report_sequence: Arc<AtomicU64>,
    /// The live deadline of one fixed call; only tests that are not about the
    /// deadline replace it.
    probe_timeout: Duration,
}

#[derive(Clone)]
struct CompletedReport {
    sequence: u64,
    boot: String,
    finished_ms: u64,
    files: Digest,
    report: WorkerCapabilities,
}

#[derive(Clone)]
struct ReportIdentity {
    files: Digest,
    context: Digest,
    fingerprint: Digest,
    tools: BTreeMap<String, ToolObservation>,
}

impl std::fmt::Debug for Profile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Profile")
            .field("name", &self.name)
            .field("digest", &self.digest)
            .field("env_keys", &self.env.keys().collect::<Vec<_>>())
            .finish()
    }
}
impl Profile {
    pub fn load(state: &Path, retained: &BTreeMap<String, String>) -> Result<Self, String> {
        let path = state.join("profiles.toml");
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => Some(file),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err("cannot open private worker profiles.toml".into()),
        };
        let explicit_allowlist = file.is_some();
        let (name, definition, env) = if let Some(mut file) = file {
            let metadata = file
                .metadata()
                .map_err(|_| "cannot inspect worker profile")?;
            // The open descriptor, not a second path lookup, is the authority.
            if !metadata.is_file()
                || metadata.uid() != rustix::process::geteuid().as_raw()
                || metadata.permissions().mode() & 0o077 != 0
                || metadata.len() > MAX_PROFILE_BYTES
            {
                return Err(
                    "worker profiles.toml must be an owned private bounded regular file".into(),
                );
            }
            let mut text = String::new();
            file.by_ref()
                .take(MAX_PROFILE_BYTES + 1)
                .read_to_string(&mut text)
                .map_err(|_| "cannot read worker profiles")?;
            if text.len() as u64 > MAX_PROFILE_BYTES {
                return Err("worker profiles.toml exceeds size limit".into());
            }
            let mut parsed: ProfileFile =
                toml::from_str(&text).map_err(|_| "invalid worker profiles.toml")?;
            if parsed.schema_version != 1
                || parsed.profiles.len() > 32
                || !valid_name(&parsed.active)
            {
                return Err("invalid worker profile schema or active name".into());
            }
            let definition = parsed
                .profiles
                .remove(&parsed.active)
                .ok_or("active worker profile is undefined")?;
            let mut env: BTreeMap<_, _> = retained
                .iter()
                .filter(|(k, _)| {
                    !scope_env(k)
                        && (definition.allowed_env.contains(*k)
                            || matches!(k.as_str(), "HOME" | "TMPDIR"))
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for (k, v) in &definition.env {
                if !definition.allowed_env.contains(k) || protected_env(k) {
                    return Err(
                        "profile env must use explicitly allowed nonreserved constants".into(),
                    );
                }
                env.insert(k.clone(), v.clone());
            }
            (parsed.active, definition, env)
        } else {
            let path = retained
                .get("PATH")
                .ok_or("retained worker environment requires PATH")?;
            let shell = PathBuf::from(
                retained
                    .get("SHELL")
                    .map(String::as_str)
                    .unwrap_or("/bin/bash"),
            );
            // Everything the worker kept, but the scope variables.
            let toolchain: BTreeMap<String, String> = retained
                .iter()
                .filter(|(k, _)| !scope_env(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let definition = Definition {
                path: std::env::split_paths(path).collect(),
                shell,
                env: BTreeMap::new(),
                allowed_env: toolchain.keys().cloned().collect(),
                required_tools: BTreeSet::new(),
                secret_refs: BTreeMap::new(),
                cache_namespace: "default".into(),
            };
            ("default".into(), definition, toolchain)
        };
        let scope = crate::config::traffic::step_env(retained);
        Self::new(name, definition, env, scope, explicit_allowlist)
    }
    fn new(
        name: String,
        definition: Definition,
        mut env: BTreeMap<String, String>,
        scope_env: BTreeMap<String, String>,
        explicit_allowlist: bool,
    ) -> Result<Self, String> {
        if !definition.secret_refs.is_empty() {
            return Err(
                "profile secret_refs are unsupported until a safe local provider is configured"
                    .into(),
            );
        }
        if definition.path.is_empty()
            || definition.path.len() > 64
            || definition.path.iter().any(|p| !p.is_absolute())
            || !definition.shell.is_absolute()
            || !executable(&definition.shell)
            || !valid_name(&definition.cache_namespace)
            || definition
                .required_tools
                .iter()
                .any(|t| !TOOLS.contains(&t.as_str()))
            || definition.allowed_env.len() > 256
            || env.len() > 256
            || env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > 65536
            || env.iter().chain(&scope_env).any(|(k, v)| {
                k.is_empty()
                    || k.len() > 128
                    || k.contains(['=', '\0'])
                    || v.len() > 16384
                    || v.contains('\0')
            })
        {
            return Err("invalid worker profile paths, shell, tool or environment".into());
        }
        // Shell is an explicit executable, never a login shell or sourced rc file.
        if !matches!(
            definition.shell.file_name().and_then(|v| v.to_str()),
            Some("bash" | "sh" | "zsh")
        ) {
            return Err("worker profile shell must be bash, sh or zsh".into());
        }
        let path = std::env::join_paths(&definition.path).map_err(|_| "invalid profile PATH")?;
        env.insert(
            "PATH".into(),
            path.into_string()
                .map_err(|_| "profile PATH must be UTF-8")?,
        );
        env.insert(
            "SHELL".into(),
            definition
                .shell
                .to_str()
                .ok_or("profile shell must be UTF-8")?
                .into(),
        );
        for (key, value) in INSTALL_GUARDS {
            env.insert((*key).into(), (*value).into());
        }
        if env.len() > 256 || env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() > 65536 {
            return Err("effective worker profile environment exceeds bounds".into());
        }
        let bytes = serde_json::to_vec(&(1u32, &name, &definition, &env, explicit_allowlist))
            .map_err(|_| "cannot hash worker profile")?;
        let digest = Digest::of_bytes(&bytes);
        let profile = Self {
            embedded_bin: None,
            explicit_allowlist,
            name,
            definition,
            env,
            scope_env,
            digest,
            report_identity: Arc::new(Mutex::new(None)),
            completed_report: Arc::new(Mutex::new(None)),
            report_sequence: Arc::new(AtomicU64::new(0)),
            probe_timeout: PROBE_TIMEOUT,
        };
        // Loading is part of startup: a cold required tool gets the startup
        // deadline and repeats, and is refused only once it stays undetermined.
        let required = profile.startup_probes(
            profile.definition.required_tools.iter().map(String::as_str),
            STARTUP_PROBES,
        );
        for (tool, observed) in &required {
            if !matches!(observed, ToolObservation::Available { .. }) {
                return Err(format!("required worker profile tool {tool} unavailable"));
            }
        }
        Ok(profile)
    }
    pub(crate) fn with_embedded_bin(mut self, bin: PathBuf) -> Result<Self, String> {
        let path = crate::workspace::embedded::prepend(&bin, &self.env["PATH"])?;
        self.env.insert("PATH".into(), path);
        self.embedded_bin = Some(bin);
        Ok(self)
    }
    /// The absolute PATH directories the profile executes with.
    pub fn path_dirs(&self) -> &[PathBuf] {
        &self.definition.path
    }
    pub fn required_tools(&self) -> &BTreeSet<String> {
        &self.definition.required_tools
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    /// The toolchain environment: what the digest, the fingerprint and an
    /// execution binding's activation are made from. A command also gets
    /// [`Profile::scope_environment`].
    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.env
    }
    /// The variables that tell a command's own task-traffic-controller which
    /// scope to use and how to nest: the worker's, on top of everything.
    pub fn scope_environment(&self) -> &BTreeMap<String, String> {
        &self.scope_env
    }
    /// What the worker's own helpers run with (tool probes, git, the
    /// workspace tools): the toolchain environment and the scope variables,
    /// as a command without an environment of its own gets them. A tool that
    /// reads `XDG_CONFIG_HOME` answers a probe as it will answer in a step.
    pub fn host_environment(&self) -> BTreeMap<String, String> {
        let mut env = self.env.clone();
        env.extend(self.scope_env.clone());
        env
    }
    pub fn execution_environment(
        &self,
        extra: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, String> {
        let own = |key: &str| self.scope_env.get(key).or_else(|| self.env.get(key));
        if extra.iter().any(|(key, value)| {
            (self.explicit_allowlist
                && !self.definition.allowed_env.contains(key)
                && !protected_env(key))
                || protected_env(key) && own(key) != Some(value)
        }) {
            return Err(
                "command env is outside the active profile allowlist or changes protected environment"
                    .into(),
            );
        }
        let mut env = self.host_environment();
        env.extend(extra.clone());
        Ok(env)
    }
    /// The environment of one command: [`Profile::execution_environment`],
    /// or with an execution binding its activation and `extra`. Either way
    /// the scope variables are the worker's present ones: the activation
    /// does not hold them, and a binding accepted before they changed (or
    /// before the worker carried them at all) stays valid.
    pub fn command_environment(
        &self,
        binding: Option<&super::contract::ExecutionBinding>,
        extra: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, String> {
        let unbound = self.execution_environment(extra)?;
        let Some(binding) = binding else {
            return Ok(unbound);
        };
        // Checked against the worker's values above; the activation has none.
        let own: BTreeMap<String, String> = extra
            .iter()
            .filter(|(k, _)| !scope_env(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let mut env = binding.environment(&own)?;
        if let Some(bin) = &self.embedded_bin {
            env.insert(
                "PATH".into(),
                crate::workspace::embedded::prepend(
                    bin,
                    env.get("PATH").ok_or("activation has no PATH")?,
                )?,
            );
        }
        crate::config::traffic::apply_step_env(&mut env, &self.scope_env);
        Ok(env)
    }
    pub fn shell(&self) -> &Path {
        &self.definition.shell
    }
    pub fn definition_digest(&self) -> &Digest {
        &self.digest
    }
    /// Fixed probes, measured in the same cleared environment as execution.
    /// Profile loading probes required tools; reports additionally probe the
    /// registered optional tools, bounded to 14 fixed calls ×
    /// 300ms, and no source command or script can be supplied over the wire.
    pub fn report(&self) -> WorkerCapabilities {
        self.measured_report(None)
    }
    /// Measure every tool to a determinate answer before publishing the
    /// resident baseline, so a cold native launcher is not recorded as unknown
    /// and then taken for drift when it later answers. One call may take 2s;
    /// only a call that timed out is repeated, after every tool had its first
    /// call, at most 3 times per tool within one shared 5s. The wait is at
    /// most 5s + 14 fixed calls x 300ms = 9.2s.
    /// A tool that is absent, fails, or still times out stays
    /// unavailable/unknown and does not prevent unconstrained shell work; if it
    /// answers after this baseline, that is drift.
    pub fn startup_report(&self) -> WorkerCapabilities {
        self.measured_report(Some(STARTUP_PROBES))
    }
    fn measured_report(&self, startup: Option<StartupProbes>) -> WorkerCapabilities {
        let files_before = self.executable_identity();
        let tools: BTreeMap<_, _> = match startup {
            Some(limits) => self.startup_probes(TOOLS.iter().copied(), limits),
            None => TOOLS
                .iter()
                .map(|t| (t.to_string(), self.probe(t)))
                .collect(),
        };
        let os = match std::env::consts::OS {
            "macos" => "macos",
            other => other,
        }
        .to_string();
        let arch = match std::env::consts::ARCH {
            "aarch64" => "aarch64",
            other => other,
        }
        .to_string();
        let shell_identity = fs::canonicalize(&self.definition.shell)
            .ok()
            .and_then(|path| {
                fs::metadata(&path)
                    .ok()
                    .map(|m| (path, m.dev(), m.ino(), m.len(), m.mtime(), m.mtime_nsec()))
            });
        let context = Digest::of_bytes(
            &serde_json::to_vec(&(
                &shell_identity,
                &self.digest,
                &os,
                &arch,
                rustix::process::geteuid().as_raw(),
            ))
            .expect("profile context serializes"),
        );
        let bytes = serde_json::to_vec(&(
            &shell_identity,
            &self.digest,
            &os,
            &arch,
            rustix::process::geteuid().as_raw(),
            &tools,
        ))
        .expect("profile report serializes");
        let files = self.executable_identity();
        let observed_fingerprint = Digest::of_bytes(
            &serde_json::to_vec(&(&bytes, &files_before, &files))
                .expect("profile identity serializes"),
        );
        // Probe failure is a current capability observation, not proof that the
        // execution profile changed. Keep the startup identity only while the
        // executable files and every positive observation still agree. Never
        // substitute a remembered success into the published tools map.
        let profile_fingerprint = match self.report_identity.lock() {
            Ok(mut retained) => {
                let baseline = retained.get_or_insert_with(|| ReportIdentity {
                    files: files.clone(),
                    context: context.clone(),
                    fingerprint: observed_fingerprint.clone(),
                    tools: tools.clone(),
                });
                let unchanged = files_before == files
                    && baseline.files == files
                    && baseline.context == context
                    && tools.iter().all(|(tool, observed)| {
                        baseline.tools.get(tool).is_some_and(|initial| {
                            observed == initial
                                || matches!(
                                    (initial, observed),
                                    (ToolObservation::Available { .. }, ToolObservation::Unknown)
                                )
                        })
                    });
                if unchanged {
                    baseline.fingerprint.clone()
                } else {
                    // Domain separation also invalidates drift if the raw
                    // observation happens to equal the original report again.
                    Digest::of_bytes(
                        &serde_json::to_vec(&("profile_drift", &observed_fingerprint))
                            .expect("profile drift serializes"),
                    )
                }
            }
            Err(_) => Digest::of_bytes(b"profile identity unavailable"),
        };
        let report = WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            schema_version: 1,
            os,
            arch,
            profile: self.name.clone(),
            profile_digest: self.digest.clone(),
            execution_uid: rustix::process::geteuid().as_raw(),
            protocol_versions: vec![1],
            profile_fingerprint,
            tools,
            features: FEATURES.iter().map(|v| v.to_string()).collect(),
        };
        // The replacement try_update name requires Rust 1.95; keep the 1.89 MSRV.
        #[allow(deprecated)]
        let sequence = self
            .report_sequence
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .ok()
            .and_then(|v| v.checked_add(1));
        let completed = crate::platform::boot_id()
            .filter(|_| files_before == files && sequence.is_some())
            .map(|boot| CompletedReport {
                sequence: sequence.expect("filtered sequence"),
                boot,
                finished_ms: crate::platform::monotonic_ms(),
                files,
                report: report.clone(),
            });
        if let Ok(mut slot) = self.completed_report.lock() {
            *slot = completed;
        }
        report
    }

    /// Consume only: never probes, renews the stamp, or changes admission reports.
    pub(crate) fn preparation_snapshot(
        &self,
        expected: &Digest,
    ) -> Option<(
        WorkerCapabilities,
        crate::setup::preparation_probe::ProfileSample,
    )> {
        let snapshot = self.completed_report.lock().ok()?.clone()?;
        let boot = crate::platform::boot_id()?;
        let now = crate::platform::monotonic_ms();
        let report = self.preparation_snapshot_at(&snapshot, expected, &boot, now)?;
        let finished = crate::platform::monotonic_ms();
        if finished < now
            || finished.checked_sub(snapshot.finished_ms)? > PREPARATION_SNAPSHOT_TTL_MS
            || crate::platform::boot_id().as_deref() != Some(snapshot.boot.as_str())
        {
            return None;
        }
        Some((
            report,
            crate::setup::preparation_probe::ProfileSample {
                native_boot: snapshot.boot,
                sequence: snapshot.sequence,
                completed_monotonic_ms: snapshot.finished_ms,
                ttl_ms: PREPARATION_SNAPSHOT_TTL_MS,
            },
        ))
    }
    fn preparation_snapshot_at(
        &self,
        snapshot: &CompletedReport,
        expected: &Digest,
        boot: &str,
        now: u64,
    ) -> Option<WorkerCapabilities> {
        if snapshot.boot != boot
            || now.checked_sub(snapshot.finished_ms)? > PREPARATION_SNAPSHOT_TTL_MS
            || &snapshot.report.profile_fingerprint != expected
        {
            return None;
        }
        // Fixed 15 tools x at most 64 search directories; paths are component-bounded
        // before metadata. No content/tree traversal or subprocess is performed.
        crate::setup::preparation_probe::bounded_path(self.shell()).ok()?;
        for dir in &self.definition.path {
            crate::setup::preparation_probe::bounded_path(dir).ok()?;
            for tool in TOOLS
                .iter()
                .copied()
                .chain(["xcrun"])
                .filter(|tool| crate::workspace::embedded::version(tool).is_none())
            {
                let binary = if tool == "xcode" { "xcodebuild" } else { tool };
                crate::setup::preparation_probe::bounded_path(&dir.join(binary)).ok()?;
            }
        }
        (snapshot.files == self.executable_identity()).then(|| snapshot.report.clone())
    }
    /// Safe classification of an already rejected report; no new probe or private value.
    pub(crate) fn identity_failure_message(&self, report: &WorkerCapabilities) -> &'static str {
        let Ok(retained) = self.report_identity.lock() else {
            return "worker profile identity unavailable";
        };
        let Some(initial) = retained.as_ref() else {
            return "worker profile identity unavailable";
        };
        if report.profile_fingerprint == initial.fingerprint {
            return "worker configured profile fingerprint mismatch";
        }
        if report.tools.iter().any(|(tool, observation)| {
            matches!(observation, ToolObservation::Available { .. })
                && !matches!(
                    initial.tools.get(tool),
                    Some(ToolObservation::Available { .. })
                )
        }) {
            return "worker profile gained a native observation";
        }
        if report.tools.iter().any(|(tool, observation)| {
            matches!(observation, ToolObservation::Available { .. })
                && initial.tools.get(tool) != Some(observation)
        }) {
            return "worker profile native observation changed";
        }
        "worker profile executable or context changed"
    }
    /// Resolve the same PATH candidates as the probes without executing them.
    /// ctime/mode catch replacement or permission changes even if mtime is kept.
    fn executable_identity(&self) -> Digest {
        type FileIdentity = (PathBuf, u64, u64, u64, i64, i64, i64, i64, u32);
        fn identity(path: &Path) -> Option<FileIdentity> {
            let real = fs::canonicalize(path).ok()?;
            let m = fs::metadata(&real).ok()?;
            Some((
                real,
                m.dev(),
                m.ino(),
                m.len(),
                m.mtime(),
                m.mtime_nsec(),
                m.ctime(),
                m.ctime_nsec(),
                m.mode(),
            ))
        }
        let tools: BTreeMap<_, _> = TOOLS
            .iter()
            .copied()
            .chain(["xcrun"])
            .filter(|tool| crate::workspace::embedded::version(tool).is_none())
            .map(|tool| {
                let binary = if tool == "xcode" { "xcodebuild" } else { tool };
                let file = self
                    .definition
                    .path
                    .iter()
                    .map(|p| p.join(binary))
                    .find(|p| executable(p))
                    .and_then(|p| identity(&p));
                (tool, file)
            })
            .collect();
        Digest::of_bytes(
            &serde_json::to_vec(&(identity(self.shell()), tools))
                .expect("executable identity serializes"),
        )
    }
    fn probe(&self, tool: &str) -> ToolObservation {
        self.probe_within(tool, &|| self.probe_timeout)
            .unwrap_or(ToolObservation::Unknown)
    }
    /// Every tool is called once, then only the ones that timed out are
    /// called again, pass by pass: a tool that never answers cannot use up
    /// the repeats of the others. Every other answer is final at once.
    fn startup_probes<'a>(
        &self,
        tools: impl Iterator<Item = &'a str>,
        limits: StartupProbes,
    ) -> BTreeMap<String, ToolObservation> {
        let clock = StartupClock::start(limits);
        let mut observed = BTreeMap::new();
        let mut timed_out = Vec::new();
        for tool in tools {
            match self.probe_within(tool, &|| clock.call_deadline()) {
                Some(answer) => {
                    observed.insert(tool.to_string(), answer);
                }
                None => timed_out.push(tool),
            }
        }
        for _ in 1..limits.attempts {
            let mut still = Vec::new();
            for tool in timed_out {
                let answer = match clock.remaining().is_zero() {
                    true => None,
                    false => self.probe_within(tool, &|| clock.repeat_deadline()),
                };
                match answer {
                    Some(answer) => {
                        observed.insert(tool.to_string(), answer);
                    }
                    None => still.push(tool),
                }
            }
            timed_out = still;
        }
        for tool in timed_out {
            observed.insert(tool.to_string(), ToolObservation::Unknown);
        }
        observed
    }
    /// `None`: a fixed call ran out of time, which says nothing about the tool.
    fn probe_within(&self, tool: &str, deadline: &dyn Fn() -> Duration) -> Option<ToolObservation> {
        Some(match self.probe_once(tool, deadline) {
            Ok(observed) => observed,
            Err(ProbeMiss::Failed) => ToolObservation::Unknown,
            Err(ProbeMiss::TimedOut) => return None,
        })
    }
    fn probe_once(
        &self,
        tool: &str,
        deadline: &dyn Fn() -> Duration,
    ) -> Result<ToolObservation, ProbeMiss> {
        if let Some(version) = crate::workspace::embedded::version(tool) {
            return Ok(ToolObservation::Available {
                version: version.into(),
                identity: Digest::of_bytes(format!("{tool}/{version}").as_bytes()),
            });
        }
        let binary = if tool == "xcode" { "xcodebuild" } else { tool };
        let Some(path) = self
            .definition
            .path
            .iter()
            .map(|p| p.join(binary))
            .find(|p| executable(p))
        else {
            return Ok(ToolObservation::Unavailable);
        };
        // The standard Flutter launcher bootstraps SDK/cache even for
        // --version. Its noninstalling snapshot adapter belongs to S20.
        // Flutter's dart launcher has the same behavior; native Dart can be
        // measured without invoking that bootstrap wrapper.
        if tool == "flutter" {
            return Ok(ToolObservation::Unknown);
        }
        if tool == "dart" {
            let mut magic = [0u8; 4];
            let native = std::fs::File::open(&path)
                .and_then(|mut f| f.read_exact(&mut magic))
                .is_ok()
                && matches!(
                    magic,
                    [0x7f, b'E', b'L', b'F']
                        | [0xcf, 0xfa, 0xed, 0xfe]
                        | [0xfe, 0xed, 0xfa, 0xcf]
                        | [0xca, 0xfe, 0xba, 0xbe]
                );
            if !native {
                return Ok(ToolObservation::Unknown);
            }
        }
        let args: &[&str] = match tool {
            "docker" => &["info", "--format", "{{.ServerVersion}}"],
            "xcode" => &["-version"],
            _ => &["--version"],
        };
        let version = bounded_probe(&path, args, &self.host_environment(), deadline())?;
        let Some(version) = measured_version(tool, &version) else {
            return Ok(ToolObservation::Unknown);
        };
        let sdk_identity = if tool == "xcode" {
            let Some(xcrun) = self
                .definition
                .path
                .iter()
                .map(|p| p.join("xcrun"))
                .find(|p| executable(p))
            else {
                return Ok(ToolObservation::Unknown);
            };
            let sdk = bounded_probe(
                &xcrun,
                &["--show-sdk-path"],
                &self.host_environment(),
                deadline(),
            )?;
            let sdk = PathBuf::from(sdk.trim());
            if !sdk.is_absolute() || !sdk.is_dir() {
                return Ok(ToolObservation::Unknown);
            }
            let Ok(real_sdk) = fs::canonicalize(&sdk) else {
                return Ok(ToolObservation::Unknown);
            };
            Some(Digest::of_bytes(real_sdk.as_os_str().as_encoded_bytes()))
        } else {
            None
        };
        let Ok(real) = fs::canonicalize(&path) else {
            return Ok(ToolObservation::Unknown);
        };
        let Ok(meta) = fs::metadata(&real) else {
            return Ok(ToolObservation::Unknown);
        };
        let bytes = serde_json::to_vec(&(
            &real,
            &sdk_identity,
            meta.dev(),
            meta.ino(),
            meta.len(),
            meta.mtime(),
            meta.mtime_nsec(),
            &version,
        ))
        .expect("tool identity serializes");
        Ok(ToolObservation::Available {
            version,
            identity: Digest::of_bytes(&bytes),
        })
    }
}
fn measured_version(tool: &str, output: &str) -> Option<String> {
    let text = output.trim();
    let text = match tool {
        "node" => text.strip_prefix('v')?,
        "git" => text.strip_prefix("git version ")?,
        "rustc" => text.strip_prefix("rustc ")?,
        "cargo" => text.strip_prefix("cargo ")?,
        "flutter" => text.strip_prefix("Flutter ")?,
        "dart" => text.strip_prefix("Dart SDK version: ")?,
        "xcode" => text.strip_prefix("Xcode ")?,
        "portool" => text.strip_prefix("portool ")?,
        "fast-worktree" => text.strip_prefix("fast-worktree ")?,
        _ => text,
    };
    let token = text.split_whitespace().next()?;
    let base = token.split(['-', '+']).next()?;
    if !valid_version(token)
        || !base.contains('.')
        || !base
            .split('.')
            .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    Some(if tool == "node" {
        format!("v{token}")
    } else {
        token.into()
    })
}
fn executable(path: &Path) -> bool {
    fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

/// Nonblocking pipe reads bound output and avoid waiting on descendant-held
/// pipes. WNOWAIT retains the leader pid until group cleanup and reap.
fn bounded_probe(
    path: &Path,
    args: &[&str],
    env: &BTreeMap<String, String>,
    deadline: Duration,
) -> Result<String, ProbeMiss> {
    static SWEPT: std::sync::Once = std::sync::Once::new();
    let parent = std::env::temp_dir();
    // A process that was killed, or that exited, in the middle of a probe ran
    // no destructor: its directory is removed here, once it is proven gone.
    SWEPT.call_once(|| {
        crate::platform::owned_temp::sweep(&parent, PROBE_DIR_PREFIX);
    });
    bounded_probe_in(&parent, path, args, env, deadline)
}
/// The probe runs in an empty directory under `parent`. It is removed on every
/// return below, after the probed process group is killed and its leader reaped.
fn bounded_probe_in(
    parent: &Path,
    path: &Path,
    args: &[&str],
    env: &BTreeMap<String, String>,
    deadline: Duration,
) -> Result<String, ProbeMiss> {
    let cwd = crate::platform::owned_temp::OwnedTemp::new_in(parent, PROBE_DIR_PREFIX)
        .map_err(|_| ProbeMiss::Failed)?;
    let mut child = Command::new(path)
        .args(args)
        .current_dir(cwd.path())
        .env_clear()
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
        .map_err(|_| ProbeMiss::Failed)?;
    let pid = child.id();
    // Every failure after spawn still passes through owned group cleanup/reap.
    let output = (|| {
        let mut out = child.stdout.take().ok_or(ProbeMiss::Failed)?;
        let mut err = child.stderr.take().ok_or(ProbeMiss::Failed)?;
        for fd in [out.as_raw_fd(), err.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return Err(ProbeMiss::Failed);
            }
        }
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let started = Instant::now();
        loop {
            if !read_available(&mut out, &mut stdout) || !read_available(&mut err, &mut stderr) {
                return Err(ProbeMiss::Failed);
            }
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            let observed = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid,
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if observed != 0 {
                return Err(ProbeMiss::Failed);
            }
            if started.elapsed() >= deadline {
                return Err(ProbeMiss::TimedOut);
            }
            if unsafe { info.si_pid() } == pid as i32 {
                if !read_available(&mut out, &mut stdout) || !read_available(&mut err, &mut stderr)
                {
                    return Err(ProbeMiss::Failed);
                }
                return Ok(if stdout.is_empty() { stderr } else { stdout });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    })();
    // Our unreaped Child owns pid, so this group number cannot be recycled.
    if let Ok(group) = i32::try_from(pid) {
        crate::platform::signal_process_group(group, libc::SIGKILL);
    }
    let status = child.wait().map_err(|_| ProbeMiss::Failed)?;
    // The timeout was decided before the kill above; its status is no answer.
    let output = output?;
    if !status.success() {
        return Err(ProbeMiss::Failed);
    }
    String::from_utf8(output).map_err(|_| ProbeMiss::Failed)
}
fn read_available(input: &mut impl Read, output: &mut Vec<u8>) -> bool {
    loop {
        let mut chunk = [0u8; 1024];
        let left = (MAX_PROBE_BYTES + 1).saturating_sub(output.len() as u64) as usize;
        if left == 0 {
            return false;
        }
        let length = chunk.len().min(left);
        match input.read(&mut chunk[..length]) {
            Ok(0) => return true,
            Ok(n) => {
                output.extend_from_slice(&chunk[..n]);
                if output.len() as u64 > MAX_PROBE_BYTES {
                    return false;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return true,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return false,
        }
    }
}

/// For a test that is not about the live deadline: its fakes are decided by
/// what they print, not by how fast a loaded host starts a new script. A fake
/// that must time out sleeps far longer than this.
#[cfg(test)]
fn patient(profile: Profile) -> Profile {
    Profile {
        probe_timeout: Duration::from_secs(2),
        ..profile
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    fn fake(dir: &Path, name: &str, body: &str, mode: u32) {
        let path = dir.join(name);
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }
    fn env(dir: &Path) -> BTreeMap<String, String> {
        [
            ("PATH".into(), dir.to_str().unwrap().into()),
            ("HOME".into(), dir.to_str().unwrap().into()),
        ]
        .into()
    }
    /// Every way a probe ends removes its directory: an answer, a failing
    /// tool, a tool that cannot be started, too much output, and a timeout
    /// whose descendant keeps the directory as its working directory.
    #[test]
    fn a_probe_leaves_nothing_under_its_parent_however_it_ends() {
        let tmp = crate::test_tmp::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        let parent = tmp.path().join("probes");
        fs::create_dir(&bin).unwrap();
        fs::create_dir(&parent).unwrap();
        // The tool sees an empty directory that this process is recorded to own.
        fake(&bin, "answers", "pwd -P; ls; cat ../owner", 0o755);
        fake(&bin, "fails", "echo no; exit 3", 0o755);
        fake(&bin, "floods", "yes | head -c 20000", 0o755);
        fake(&bin, "hangs", "(sleep 30 &) ; sleep 30", 0o755);
        fake(&bin, "unreadable", "echo 1", 0o111);
        let env: BTreeMap<String, String> =
            [("PATH".to_string(), "/usr/bin:/bin".to_string())].into();
        let run = |tool: &str, limit: u64| {
            let got = bounded_probe_in(
                &parent,
                &bin.join(tool),
                &[],
                &env,
                Duration::from_millis(limit),
            );
            assert!(
                fs::read_dir(&parent).unwrap().next().is_none(),
                "{tool} left a probe directory"
            );
            got
        };
        let answer = run("answers", 10_000).unwrap();
        let mut lines = answer.lines();
        let cwd = Path::new(lines.next().unwrap());
        assert_eq!(cwd.parent().unwrap().parent().unwrap(), parent);
        let name = cwd.parent().unwrap().file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with(PROBE_DIR_PREFIX), "{name}");
        assert_eq!(lines.nth(1).unwrap(), std::process::id().to_string());
        assert_eq!(run("fails", 10_000), Err(ProbeMiss::Failed));
        assert_eq!(run("floods", 10_000), Err(ProbeMiss::Failed));
        assert_eq!(run("unreadable", 10_000), Err(ProbeMiss::Failed));
        assert_eq!(run("missing", 10_000), Err(ProbeMiss::Failed));
        assert_eq!(run("hangs", 100), Err(ProbeMiss::TimedOut));
        // No parent to make the directory in: a failed probe, nothing made.
        assert_eq!(
            bounded_probe_in(
                &tmp.path().join("absent"),
                &bin.join("answers"),
                &[],
                &env,
                Duration::from_secs(10),
            ),
            Err(ProbeMiss::Failed)
        );
        assert!(!tmp.path().join("absent").exists());
    }
    fn write_profile(dir: &Path, active: &str, extra: &str) {
        fs::write(dir.join("profiles.toml"), format!("schema_version = 1\nactive = {active:?}\n[profiles.node22]\npath = [{:?}]\nshell = \"/bin/sh\"\ncache_namespace = \"node22\"\n{extra}", dir.to_str().unwrap())).unwrap();
        fs::set_permissions(dir.join("profiles.toml"), fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[test]
    fn actual_path_versions_and_private_profile_are_authoritative() {
        let a = crate::test_tmp::tempdir().unwrap();
        let b = crate::test_tmp::tempdir().unwrap();
        fake(a.path(), "node", "printf 'v22.0.0\\n'", 0o700);
        fake(b.path(), "node", "printf 'v20.0.0\\n'", 0o700);
        let p22 = Profile::load(a.path(), &env(a.path())).unwrap();
        let p20 = Profile::load(b.path(), &env(b.path())).unwrap();
        let a_report = p22.startup_report();
        let b_report = p20.startup_report();
        assert!(
            matches!(&a_report.tools["node"], ToolObservation::Available {version,..} if version == "v22.0.0")
        );
        assert!(
            matches!(&b_report.tools["node"], ToolObservation::Available {version,..} if version == "v20.0.0")
        );
        assert_ne!(a_report.profile_fingerprint, b_report.profile_fingerprint);
        assert_eq!(
            p22.execution_environment(&[("ORDINARY_VAR".into(), "kept".into())].into())
                .unwrap()["ORDINARY_VAR"],
            "kept"
        );
        assert!(p22
            .execution_environment(&[("PATH".into(), "/different".into())].into())
            .is_err());
        assert!(p22
            .execution_environment(&[("PATH".into(), a.path().to_str().unwrap().into())].into())
            .is_ok());

        write_profile(
            a.path(),
            "node22",
            "allowed_env = [\"CONSTANT\"]\n[profiles.node22.env]\nCONSTANT = \"value\"",
        );
        let p = Profile::load(a.path(), &env(b.path())).unwrap();
        assert_eq!(p.name(), "node22");
        assert_eq!(p.environment()["PATH"], a.path().to_str().unwrap());
        assert_eq!(p.environment()["SHELL"], "/bin/sh");
        assert_eq!(p.environment()["CONSTANT"], "value");
        assert!(p
            .execution_environment(&[("UNLISTED".into(), "value".into())].into())
            .is_err());
        assert!(p
            .execution_environment(&[("CONSTANT".into(), "allowed".into())].into())
            .is_ok());
        assert!(!format!("{p:?}").contains("value"));
        let public = serde_json::to_string(&p.report()).unwrap();
        assert!(!public.contains(a.path().to_str().unwrap()));
        assert!(!public.contains("CONSTANT"));
        assert!(!public.contains("value"));
        write_profile(a.path(), "absent", "");
        assert!(Profile::load(a.path(), &env(a.path())).is_err());
    }
    #[test]
    fn transient_unknown_preserves_identity_without_claiming_availability() {
        let root = crate::test_tmp::tempdir().unwrap();
        let marker = root.path().join("probe-mode");
        fake(root.path(), "node", &format!(
            "mode=; if [ -r {marker:?} ]; then IFS= read -r mode < {marker:?} || :; fi; case \"$mode\" in timeout) /bin/sleep 30;; changed) echo v20.0.0;; *) echo v22.0.0;; esac"
        ), 0o700);
        let profile = patient(Profile::load(root.path(), &env(root.path())).unwrap());
        // Establish the known baseline through the actual resident startup
        // path. The later deliberately unknown sample still uses report().
        let initial = profile.startup_report();
        assert!(matches!(
            initial.tools["node"],
            ToolObservation::Available { .. }
        ));
        fs::write(&marker, "timeout").unwrap();
        let unknown = profile.report();
        assert_eq!(unknown.tools["node"], ToolObservation::Unknown);
        assert_eq!(unknown.profile_fingerprint, initial.profile_fingerprint);
        let required =
            super::super::capability::ExecutionRequirements::parse(None, &["node".into()]).unwrap();
        assert!(!super::super::capability::missing_with_installable(
            &required,
            Some(&unknown),
            None
        )
        .is_empty());
        fs::write(&marker, "changed").unwrap();
        let changed = profile.report();
        assert_ne!(changed.profile_fingerprint, initial.profile_fingerprint);
        assert_eq!(
            profile.identity_failure_message(&changed),
            "worker profile native observation changed"
        );
        fs::remove_file(&marker).unwrap();
        assert_eq!(
            profile.report().profile_fingerprint,
            initial.profile_fingerprint
        );
        fake(root.path(), "node", "echo v22.0.0", 0o700);
        assert_ne!(
            profile.report().profile_fingerprint,
            initial.profile_fingerprint
        );
    }

    #[test]
    fn startup_cold_probe_recovers_before_publishing_baseline() {
        let root = crate::test_tmp::tempdir().unwrap();
        let marker = root.path().join("warmed");
        // The first owned probe times out after recording its cold launch;
        // subsequent invocations succeed without changing executable identity.
        fake(
            root.path(),
            "node",
            &format!(
            "if [ ! -f {marker:?} ]; then echo warm > {marker:?}; /bin/sleep 30; fi\necho v22.0.0"
        ),
            0o700,
        );
        let profile = patient(Profile::load(root.path(), &env(root.path())).unwrap());
        let initial = profile.startup_report();
        assert!(matches!(
            initial.tools["node"],
            ToolObservation::Available { .. }
        ));
        let live = profile.report();
        assert_eq!(live.tools["node"], initial.tools["node"]);
        assert_eq!(live.profile_fingerprint, initial.profile_fingerprint);
    }

    #[test]
    fn startup_unknown_stays_honest_and_late_success_invalidates_baseline() {
        let root = crate::test_tmp::tempdir().unwrap();
        let marker = root.path().join("ready");
        fake(
            root.path(),
            "docker",
            &format!("if [ -f {marker:?} ]; then echo 27.0.0; else exit 1; fi"),
            0o700,
        );
        let profile = patient(Profile::load(root.path(), &env(root.path())).unwrap());
        let initial = profile.startup_report();
        assert_eq!(initial.tools["docker"], ToolObservation::Unknown);
        assert_eq!(initial.tools["node"], ToolObservation::Unavailable);
        assert!(super::super::capability::missing_with_installable(
            &super::super::capability::ExecutionRequirements::default(),
            Some(&initial),
            None
        )
        .is_empty());
        fs::write(marker, "ready").unwrap();
        let live = profile.report();
        assert!(matches!(
            live.tools["docker"],
            ToolObservation::Available { .. }
        ));
        assert_ne!(live.profile_fingerprint, initial.profile_fingerprint);
    }

    #[test]
    fn startup_limits_are_pinned() {
        assert_eq!(PROBE_TIMEOUT, Duration::from_millis(300));
        assert_eq!(
            STARTUP_PROBES,
            StartupProbes {
                call: Duration::from_millis(2_000),
                total: Duration::from_millis(5_000),
                attempts: 3,
            }
        );
        let clock = StartupClock::start(STARTUP_PROBES);
        assert_eq!(clock.call_deadline(), Duration::from_millis(2_000));
        // Out of shared time, a call still gets the live deadline and no more.
        let spent = StartupClock::start(StartupProbes {
            call: Duration::from_millis(2_000),
            total: Duration::ZERO,
            attempts: 3,
        });
        assert!(spent.remaining().is_zero());
        assert_eq!(spent.call_deadline(), PROBE_TIMEOUT);
        assert!(spent.repeat_deadline().is_zero());
        assert_eq!(clock.repeat_deadline(), Duration::from_millis(2_000));
    }

    fn calls(file: &Path) -> usize {
        fs::read_to_string(file).map_or(0, |text| text.lines().count())
    }

    #[test]
    fn startup_repeats_only_timeouts_within_its_bounds_and_late_success_is_drift() {
        let root = crate::test_tmp::tempdir().unwrap();
        let node_calls = root.path().join("node-calls");
        let ready = root.path().join("ready");
        // node never answers in time until `ready`.
        fake(
            root.path(),
            "node",
            &format!(
                "echo call >> {node_calls:?}\nif [ ! -f {ready:?} ]; then /bin/sleep 30; fi\necho v22.0.0"
            ),
            0o700,
        );
        let profile = patient(Profile::load(root.path(), &env(root.path())).unwrap());
        let at = Instant::now();
        let initial = profile.measured_report(Some(StartupProbes {
            call: Duration::from_millis(400),
            total: Duration::from_secs(20),
            attempts: 3,
        }));
        // Three calls waited 400ms each (the time says at least three, the
        // count at most three), and nothing waits for the shared 20s.
        assert!(at.elapsed() >= Duration::from_millis(1_200));
        assert!(at.elapsed() < Duration::from_secs(10));
        assert!(calls(&node_calls) <= 3);
        assert_eq!(initial.tools["node"], ToolObservation::Unknown);

        // Shared time already spent: one call with the live deadline, no repeat.
        let other = crate::test_tmp::tempdir().unwrap();
        let other_calls = other.path().join("node-calls");
        fake(
            other.path(),
            "node",
            &format!("echo call >> {other_calls:?}\n/bin/sleep 30"),
            0o700,
        );
        let spent = Profile::load(other.path(), &env(other.path())).unwrap();
        let at = Instant::now();
        let report = spent.measured_report(Some(StartupProbes {
            call: Duration::from_secs(20),
            total: Duration::ZERO,
            attempts: 3,
        }));
        assert!(at.elapsed() >= PROBE_TIMEOUT);
        assert!(at.elapsed() < Duration::from_secs(10));
        assert!(calls(&other_calls) <= 1);
        assert_eq!(report.tools["node"], ToolObservation::Unknown);

        // Still unknown after every repeat: the baseline says so, and an
        // answer after it is drift, exactly as for a failed probe.
        fs::write(&ready, "ready").unwrap();
        let live = profile.report();
        assert!(matches!(
            live.tools["node"],
            ToolObservation::Available { .. }
        ));
        assert_ne!(live.profile_fingerprint, initial.profile_fingerprint);
        assert_eq!(
            profile.identity_failure_message(&live),
            "worker profile gained a native observation"
        );
    }

    #[test]
    fn a_tool_that_never_answers_does_not_use_up_the_repeats_of_the_others() {
        let root = crate::test_tmp::tempdir().unwrap();
        // node comes first and times out in every pass; git needs longer than
        // the live deadline, well inside one startup call.
        fake(root.path(), "node", "/bin/sleep 30", 0o700);
        fake(
            root.path(),
            "git",
            "/bin/sleep 0.6\necho 'git version 2.40.0'",
            0o700,
        );
        let profile = Profile::load(root.path(), &env(root.path())).unwrap();
        let report = profile.startup_report();
        assert_eq!(report.tools["node"], ToolObservation::Unknown);
        assert!(matches!(
            &report.tools["git"],
            ToolObservation::Available { .. }
        ));
    }

    #[test]
    fn startup_takes_a_failure_as_final_at_once() {
        let root = crate::test_tmp::tempdir().unwrap();
        let docker_calls = root.path().join("docker-calls");
        fake(
            root.path(),
            "docker",
            &format!("echo call >> {docker_calls:?}\nexit 1"),
            0o700,
        );
        let profile = Profile::load(root.path(), &env(root.path())).unwrap();
        // The call has all the time it could need, so it ends by failing.
        let report = profile.measured_report(Some(StartupProbes {
            call: Duration::from_secs(30),
            total: Duration::from_secs(60),
            attempts: 3,
        }));
        assert_eq!(report.tools["docker"], ToolObservation::Unknown);
        assert_eq!(calls(&docker_calls), 1);
    }

    #[test]
    fn a_cold_required_tool_is_measured_at_load_and_a_silent_one_is_refused() {
        let root = crate::test_tmp::tempdir().unwrap();
        let warmed = root.path().join("warmed");
        fake(
            root.path(),
            "node",
            &format!(
                "if [ ! -f {warmed:?} ]; then echo warm > {warmed:?}; /bin/sleep 30; fi\necho v22.0.0"
            ),
            0o700,
        );
        write_profile(root.path(), "node22", "required_tools = [\"node\"]\n");
        // The first call runs out of the startup deadline; the repeat answers.
        let profile = Profile::load(root.path(), &env(root.path())).unwrap();
        assert!(profile.required_tools().contains("node"));
        fake(root.path(), "node", "exit 1", 0o700);
        assert_eq!(
            Profile::load(root.path(), &env(root.path())).unwrap_err(),
            "required worker profile tool node unavailable"
        );
    }

    #[test]
    fn initially_unknown_probe_requires_new_identity_when_it_becomes_known() {
        let root = crate::test_tmp::tempdir().unwrap();
        let marker = root.path().join("probe-mode");
        fake(root.path(), "node", &format!(
            "case $(/bin/cat {marker:?} 2>/dev/null) in known) echo v22.0.0;; changed) echo v20.0.0;; *) exit 1;; esac"
        ), 0o700);
        let profile = patient(Profile::load(root.path(), &env(root.path())).unwrap());
        let initial = profile.report();
        assert_eq!(initial.tools["node"], ToolObservation::Unknown);
        fs::write(&marker, "known").unwrap();
        let recovered = profile.clone().report();
        assert!(matches!(
            recovered.tools["node"],
            ToolObservation::Available { .. }
        ));
        assert_ne!(recovered.profile_fingerprint, initial.profile_fingerprint);
        assert_eq!(
            profile.identity_failure_message(&recovered),
            "worker profile gained a native observation"
        );
        fs::write(&marker, "changed").unwrap();
        assert_ne!(
            profile.report().profile_fingerprint,
            initial.profile_fingerprint
        );
    }

    #[test]
    fn probe_failures_and_docker_info_are_not_available() {
        let root = crate::test_tmp::tempdir().unwrap();
        fake(root.path(), "node", "echo v22.0.0", 0o600);
        let p = Profile::load(root.path(), &env(root.path())).unwrap();
        // Deadlines are measured with the live one; answers that only have
        // to arrive are read through a clone that waits for a loaded host.
        let answers = patient(p.clone());
        assert_eq!(p.probe("node"), ToolObservation::Unavailable);
        for (key, value) in INSTALL_GUARDS {
            assert_eq!(p.environment().get(*key).map(String::as_str), Some(*value));
        }
        assert!(p
            .execution_environment(&[("RUSTUP_AUTO_INSTALL".into(), "1".into())].into())
            .is_err());
        let marker = root.path().join("implicit-install");
        for tool in ["flutter", "dart"] {
            fake(
                root.path(),
                tool,
                &format!("printf bootstrap > {:?}; echo 3.0.0", marker),
                0o700,
            );
            assert_eq!(p.probe(tool), ToolObservation::Unknown);
            assert!(!marker.exists());
        }
        fake(root.path(), "node", "echo malformed22", 0o700);
        assert_eq!(p.probe("node"), ToolObservation::Unknown);
        fake(root.path(), "node", "echo malformed", 0o700);
        assert_eq!(p.probe("node"), ToolObservation::Unknown);
        fake(root.path(), "node", "while :; do :; done", 0o700);
        let at = Instant::now();
        assert_eq!(p.probe("node"), ToolObservation::Unknown);
        assert!(at.elapsed() < Duration::from_secs(2));
        fake(
            root.path(),
            "node",
            "while :; do echo floodfloodfloodflood; done",
            0o700,
        );
        assert_eq!(p.probe("node"), ToolObservation::Unknown);
        fake(root.path(), "docker", "echo 28.0.0; exit 1", 0o700);
        assert_eq!(p.probe("docker"), ToolObservation::Unknown);
        fake(
            root.path(),
            "docker",
            "test \"$1\" = info || exit 1; echo 28.0.0",
            0o700,
        );
        assert!(matches!(
            answers.probe("docker"),
            ToolObservation::Available { .. }
        ));
        fake(root.path(), "xcodebuild", "echo 'Xcode 16.2'", 0o700);
        fake(root.path(), "xcrun", "exit 1", 0o700);
        assert_eq!(p.probe("xcode"), ToolObservation::Unknown);
        let sdk = root.path().join("sdk");
        fs::create_dir(&sdk).unwrap();
        fake(
            root.path(),
            "xcrun",
            &format!("printf '%s' {:?}", sdk),
            0o700,
        );
        assert!(matches!(
            answers.probe("xcode"),
            ToolObservation::Available { .. }
        ));
        fake(root.path(), "node", "echo v22.0.0 >&2", 0o700);
        assert!(matches!(
            answers.probe("node"),
            ToolObservation::Available { .. }
        ));
        fake(
            root.path(),
            "node",
            "(while :; do :; done) &\necho v22.0.0",
            0o700,
        );
        // The answer is taken when the leader exits, not when its busy
        // descendant would: well inside even the patient deadline.
        let at = Instant::now();
        assert!(matches!(
            answers.probe("node"),
            ToolObservation::Available { .. }
        ));
        assert!(at.elapsed() < Duration::from_secs(2));
    }
    /// A nested task-traffic-controller finds its ledger through `HOME` and
    /// `XDG_STATE_HOME`: a command gets the worker's values and cannot
    /// replace them, with or without an explicit profile.
    #[test]
    fn a_command_keeps_the_state_home_of_the_worker() {
        let set = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut with_state = env(dir.path());
        with_state.insert("XDG_STATE_HOME".into(), "/state".into());
        // The default profile: the worker has none, so a command has none.
        let step = |p: &Profile| p.execution_environment(&BTreeMap::new()).unwrap();
        let unset = Profile::load(dir.path(), &env(dir.path())).unwrap();
        assert!(!step(&unset).contains_key("XDG_STATE_HOME"));
        assert!(unset
            .execution_environment(&set(&[("XDG_STATE_HOME", "/other")]))
            .is_err());
        // The worker has one: passed on, and only as it is.
        let given = Profile::load(dir.path(), &with_state).unwrap();
        assert_eq!(step(&given)["XDG_STATE_HOME"], "/state");
        // It is the scope's, not the toolchain's; the worker's own helpers
        // run with both.
        assert!(!given.environment().contains_key("XDG_STATE_HOME"));
        assert_eq!(given.scope_environment()["XDG_STATE_HOME"], "/state");
        assert_eq!(given.host_environment(), step(&given));
        assert_eq!(given.host_environment()["XDG_STATE_HOME"], "/state");
        assert!(given
            .execution_environment(&set(&[("XDG_STATE_HOME", "/other")]))
            .is_err());
        assert_eq!(
            given
                .execution_environment(&set(&[("XDG_STATE_HOME", "/state")]))
                .unwrap()["XDG_STATE_HOME"],
            "/state"
        );
        // An explicit profile that does not list it still passes it on, and
        // one that lists it cannot give it a value of its own.
        write_profile(dir.path(), "node22", "allowed_env = [\"OTHER\"]");
        let explicit = Profile::load(dir.path(), &with_state).unwrap();
        assert_eq!(step(&explicit)["XDG_STATE_HOME"], "/state");
        assert!(explicit
            .execution_environment(&set(&[("XDG_STATE_HOME", "/other")]))
            .is_err());
        write_profile(
            dir.path(),
            "node22",
            "allowed_env = [\"XDG_STATE_HOME\"]\n[profiles.node22.env]\nXDG_STATE_HOME = \"/mine\"",
        );
        assert!(Profile::load(dir.path(), &with_state).is_err());
    }
    /// The configuration of a nested task-traffic-controller follows
    /// `XDG_CONFIG_HOME`: a command gets the worker's value, explicit profile
    /// or not, and cannot replace it.
    #[test]
    fn a_command_keeps_the_config_home_of_the_worker() {
        let set = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut with_config = env(dir.path());
        with_config.insert("XDG_CONFIG_HOME".into(), "/config".into());
        let step = |p: &Profile| p.execution_environment(&BTreeMap::new()).unwrap();
        let default = Profile::load(dir.path(), &with_config).unwrap();
        assert_eq!(step(&default)["XDG_CONFIG_HOME"], "/config");
        assert!(!default.environment().contains_key("XDG_CONFIG_HOME"));
        assert!(default
            .execution_environment(&set(&[("XDG_CONFIG_HOME", "/other")]))
            .is_err());
        write_profile(dir.path(), "node22", "allowed_env = [\"OTHER\"]");
        let explicit = Profile::load(dir.path(), &with_config).unwrap();
        assert_eq!(step(&explicit)["XDG_CONFIG_HOME"], "/config");
        assert_eq!(
            explicit
                .execution_environment(&set(&[("XDG_CONFIG_HOME", "/config")]))
                .unwrap()["XDG_CONFIG_HOME"],
            "/config"
        );
        assert!(explicit
            .execution_environment(&set(&[("XDG_CONFIG_HOME", "/other")]))
            .is_err());
        // Unset for the worker means unset for the command.
        let unset = Profile::load(dir.path(), &env(dir.path())).unwrap();
        assert!(!step(&unset).contains_key("XDG_CONFIG_HOME"));
        write_profile(
            dir.path(),
            "node22",
            "allowed_env = [\"XDG_CONFIG_HOME\"]\n[profiles.node22.env]\nXDG_CONFIG_HOME = \"/mine\"",
        );
        assert!(Profile::load(dir.path(), &with_config).is_err());
    }
    /// A nested `task-traffic-controller exec` runs under its step's entry
    /// (spec chapter 14): every command is told so, and cannot say otherwise.
    #[test]
    fn a_command_is_told_to_inherit_and_cannot_change_it() {
        let set = |k: &str, v: &str| BTreeMap::from([(k.to_string(), v.to_string())]);
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut worker = env(dir.path());
        // What the worker's own shell said does not reach a command.
        worker.insert("TASK_TRAFFIC_CONTROLLER_NESTED".into(), "admit".into());
        let check = |profile: &Profile| {
            let step = profile.execution_environment(&BTreeMap::new()).unwrap();
            assert_eq!(step["TASK_TRAFFIC_CONTROLLER_NESTED"], "inherit");
            assert!(!profile
                .environment()
                .contains_key("TASK_TRAFFIC_CONTROLLER_NESTED"));
            assert!(profile
                .execution_environment(&set("TASK_TRAFFIC_CONTROLLER_NESTED", "admit"))
                .is_err());
            assert!(profile
                .execution_environment(&set("TASK_TRAFFIC_CONTROLLER_NESTED", "inherit"))
                .is_ok());
        };
        check(&Profile::load(dir.path(), &worker).unwrap());
        write_profile(
            dir.path(),
            "node22",
            "allowed_env = [\"TASK_TRAFFIC_CONTROLLER_NESTED\"]",
        );
        check(&Profile::load(dir.path(), &worker).unwrap());
        write_profile(
            dir.path(),
            "node22",
            "allowed_env = [\"TASK_TRAFFIC_CONTROLLER_NESTED\"]\n[profiles.node22.env]\nTASK_TRAFFIC_CONTROLLER_NESTED = \"admit\"",
        );
        assert!(Profile::load(dir.path(), &worker).is_err());
    }
    /// The scope variables are no part of what a worker is: a worker that
    /// gains, loses or moves `XDG_STATE_HOME` / `XDG_CONFIG_HOME` keeps its
    /// digest and its fingerprint, so what it accepted before stays valid.
    #[test]
    fn the_scope_variables_do_not_change_the_identity_of_a_profile() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let plain = env(dir.path());
        let mut moved = plain.clone();
        moved.insert("XDG_STATE_HOME".into(), "/state".into());
        moved.insert("XDG_CONFIG_HOME".into(), "/config".into());
        moved.insert("TASK_TRAFFIC_CONTROLLER_NESTED".into(), "admit".into());
        let mut other = plain.clone();
        other.insert("ORDINARY".into(), "x".into());
        let identity = |retained: &BTreeMap<String, String>| {
            let profile = Profile::load(dir.path(), retained).unwrap();
            (
                profile.definition_digest().clone(),
                profile.startup_report().profile_fingerprint,
                profile.environment().clone(),
            )
        };
        // The default profile, then an explicit one that lists them, then
        // one that does not.
        for extra in [
            None,
            Some("allowed_env = [\"XDG_STATE_HOME\", \"XDG_CONFIG_HOME\", \"ORDINARY\"]"),
            Some("allowed_env = [\"ORDINARY\"]"),
        ] {
            match extra {
                Some(extra) => write_profile(dir.path(), "node22", extra),
                None => assert!(!dir.path().join("profiles.toml").exists()),
            }
            assert_eq!(identity(&plain), identity(&moved), "{extra:?}");
            // An ordinary variable still is part of it.
            assert_ne!(identity(&plain).0, identity(&other).0, "{extra:?}");
        }
    }
    /// With an execution binding the command starts from the activation,
    /// which holds no scope variable; they are the worker's present ones,
    /// whatever the worker carried when the binding was accepted.
    #[test]
    fn a_bound_command_gets_the_scope_of_the_worker_not_of_the_binding() {
        use crate::toolchain::adapters::{ActivationReceipt, FileIdentity};
        use crate::toolchain::contract::ExecutionBinding;
        let set = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let dir = crate::test_tmp::tempdir().unwrap();
        let mut worker = env(dir.path());
        worker.insert("XDG_STATE_HOME".into(), "/state".into());
        let profile = Profile::load(dir.path(), &worker).unwrap();
        let binding = |environment: BTreeMap<String, String>| ExecutionBinding {
            activation: ActivationReceipt {
                base_profile: Digest::of_bytes(b"fixture"),
                frozen: Digest::of_bytes(b"fixture"),
                setup_policy: Digest::of_bytes(b"fixture"),
                installation: Digest::of_bytes(b"fixture"),
                environment,
                shell: FileIdentity {
                    path: "/bin/sh".into(),
                    digest: Digest::of_bytes(b"fixture"),
                    size: 0,
                    executable: true,
                },
                binding_digest: Digest::of_bytes(b"fixture"),
                execution_fingerprint: Digest::of_bytes(b"fixture"),
            },
        };
        let bound = binding(set(&[("PATH", "/tools:/bin"), ("HOME", "/h")]));
        let env = profile
            .command_environment(Some(&bound), &set(&[("FLAG", "1")]))
            .unwrap();
        assert_eq!(
            env,
            set(&[
                ("PATH", "/tools:/bin"),
                ("HOME", "/h"),
                ("FLAG", "1"),
                ("XDG_STATE_HOME", "/state"),
                ("TASK_TRAFFIC_CONTROLLER_NESTED", "inherit"),
            ])
        );
        // A step that repeats the worker's own value is as before; one that
        // changes it is refused, as is one that changes the activation.
        assert_eq!(
            profile
                .command_environment(Some(&bound), &set(&[("XDG_STATE_HOME", "/state")]))
                .unwrap()["XDG_STATE_HOME"],
            "/state"
        );
        assert!(profile
            .command_environment(Some(&bound), &set(&[("XDG_STATE_HOME", "/other")]))
            .is_err());
        assert!(profile
            .command_environment(Some(&bound), &set(&[("XDG_CACHE_HOME", "/cache")]))
            .is_err());
        // An activation written by a worker that still carried them (or of
        // another scope) does not bring its values back.
        let stale = binding(set(&[
            ("PATH", "/tools:/bin"),
            ("XDG_STATE_HOME", "/old"),
            ("XDG_CONFIG_HOME", "/old-config"),
        ]));
        let env = profile
            .command_environment(Some(&stale), &BTreeMap::new())
            .unwrap();
        assert_eq!(env["XDG_STATE_HOME"], "/state");
        assert!(!env.contains_key("XDG_CONFIG_HOME"));
        // Without a binding it is the profile's own environment.
        assert_eq!(
            profile.command_environment(None, &set(&[("FLAG", "1")])),
            profile.execution_environment(&set(&[("FLAG", "1")]))
        );
    }
    #[test]
    fn private_input_rejects_symlinks_permissions_secrets_and_required_missing() {
        let root = crate::test_tmp::tempdir().unwrap();
        write_profile(root.path(), "node22", "required_tools = [\"node\"]");
        assert!(Profile::load(root.path(), &env(root.path())).is_err());
        write_profile(
            root.path(),
            "node22",
            "[profiles.node22.secret_refs]\nTOKEN = \"local:key\"",
        );
        assert!(Profile::load(root.path(), &env(root.path()))
            .unwrap_err()
            .contains("unsupported"));
        write_profile(root.path(), "node22", "");
        fs::set_permissions(
            root.path().join("profiles.toml"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(Profile::load(root.path(), &env(root.path())).is_err());
        fs::remove_file(root.path().join("profiles.toml")).unwrap();
        symlink(root.path().join("other"), root.path().join("profiles.toml")).unwrap();
        assert!(Profile::load(root.path(), &env(root.path())).is_err());
    }
}

#[cfg(test)]
mod preparation_snapshot_tests {
    use super::*;
    #[test]
    fn preparation_snapshot_sequence_survives_missing_snapshot_and_overflow() {
        let temp = crate::test_tmp::tempdir().unwrap();
        let profile = Profile::load(
            temp.path(),
            &BTreeMap::from([
                ("PATH".into(), temp.path().to_str().unwrap().into()),
                ("SHELL".into(), "/bin/sh".into()),
            ]),
        )
        .unwrap();
        profile.report();
        let first = profile
            .completed_report
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .sequence;
        *profile.completed_report.lock().unwrap() = None;
        profile.report();
        assert!(
            profile
                .completed_report
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .sequence
                > first
        );
        profile.report_sequence.store(u64::MAX, Ordering::SeqCst);
        profile.report();
        assert!(profile.completed_report.lock().unwrap().is_none());
        profile.report();
        assert!(profile.completed_report.lock().unwrap().is_none());
        assert_eq!(profile.report_sequence.load(Ordering::SeqCst), u64::MAX);
    }
    #[test]
    fn preparation_snapshot_consumption_is_process_free_and_never_renews_age() {
        let temp = crate::test_tmp::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let shell = root.join("sh");
        fs::copy("/bin/sh", &shell).unwrap();
        let marker = root.join("ran");
        let node = root.join("node");
        fs::write(
            &node,
            format!(
                "#!/bin/sh\necho ran >> '{}'\necho v22.0.0\n",
                marker.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&node, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = patient(
            Profile::load(
                &root,
                &BTreeMap::from([
                    ("PATH".into(), root.to_str().unwrap().into()),
                    ("SHELL".into(), shell.to_str().unwrap().into()),
                ]),
            )
            .unwrap(),
        );
        assert!(profile
            .preparation_snapshot(&Digest::of_bytes(b"none"))
            .is_none());
        let report = profile.report();
        let snapshot = profile.completed_report.lock().unwrap().clone().unwrap();
        let marker_before = fs::read(&marker).unwrap();
        for _ in 0..3 {
            assert!(profile
                .preparation_snapshot(&report.profile_fingerprint)
                .is_some());
        }
        assert_eq!(fs::read(&marker).unwrap(), marker_before);
        assert_eq!(
            profile
                .completed_report
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .finished_ms,
            snapshot.finished_ms
        );
        for (boot, now) in [
            ("different", snapshot.finished_ms),
            (
                snapshot.boot.as_str(),
                snapshot.finished_ms.saturating_sub(1),
            ),
            (snapshot.boot.as_str(), snapshot.finished_ms + 5001),
        ] {
            assert!(profile
                .preparation_snapshot_at(&snapshot, &report.profile_fingerprint, boot, now)
                .is_none());
        }
        assert!(profile
            .preparation_snapshot_at(
                &snapshot,
                &report.profile_fingerprint,
                &snapshot.boot,
                snapshot.finished_ms + 5000
            )
            .is_some());
        assert!(profile
            .preparation_snapshot(&Digest::of_bytes(b"wrong"))
            .is_none());
        fs::write(&node, b"changed identity").unwrap();
        assert!(profile
            .preparation_snapshot(&report.profile_fingerprint)
            .is_none());
    }
}
