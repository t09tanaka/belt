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
use std::time::{Duration, Instant};

const MAX_PROFILE_BYTES: u64 = 64 * 1024;
const MAX_PROBE_BYTES: u64 = 8192;
const PROBE_TIMEOUT: Duration = Duration::from_millis(300);
// Tool discovery must not bootstrap a manager-selected tool behind admission.
const INSTALL_GUARDS: &[(&str, &str)] = &[
    ("COREPACK_ENABLE_NETWORK", "0"),
    ("COREPACK_ENABLE_AUTO_PIN", "0"),
    ("COREPACK_DEFAULT_TO_LATEST", "0"),
    ("COREPACK_ENABLE_DOWNLOAD_PROMPT", "0"),
    ("RUSTUP_AUTO_INSTALL", "0"),
    ("MISE_AUTO_INSTALL", "false"),
];
fn protected_env(key: &str) -> bool {
    matches!(key, "PATH" | "SHELL" | "HOME" | "TMPDIR")
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
    explicit_allowlist: bool,
    name: String,
    definition: Definition,
    env: BTreeMap<String, String>,
    digest: Digest,
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
                    definition.allowed_env.contains(*k) || matches!(k.as_str(), "HOME" | "TMPDIR")
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
            let definition = Definition {
                path: std::env::split_paths(path).collect(),
                shell,
                env: BTreeMap::new(),
                allowed_env: retained.keys().cloned().collect(),
                required_tools: BTreeSet::new(),
                secret_refs: BTreeMap::new(),
                cache_namespace: "default".into(),
            };
            ("default".into(), definition, retained.clone())
        };
        Self::new(name, definition, env, explicit_allowlist)
    }
    fn new(
        name: String,
        definition: Definition,
        mut env: BTreeMap<String, String>,
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
            || env.iter().any(|(k, v)| {
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
            explicit_allowlist,
            name,
            definition,
            env,
            digest,
        };
        for tool in &profile.definition.required_tools {
            if !matches!(profile.probe(tool), ToolObservation::Available { .. }) {
                return Err(format!("required worker profile tool {tool} unavailable"));
            }
        }
        Ok(profile)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.env
    }
    pub fn execution_environment(
        &self,
        extra: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, String> {
        if extra.iter().any(|(key, value)| {
            (self.explicit_allowlist
                && !self.definition.allowed_env.contains(key)
                && !protected_env(key))
                || protected_env(key) && self.env.get(key) != Some(value)
        }) {
            return Err(
                "command env is outside the active profile allowlist or changes protected environment"
                    .into(),
            );
        }
        let mut env = self.env.clone();
        env.extend(extra.clone());
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
        let tools: BTreeMap<_, _> = TOOLS
            .iter()
            .map(|t| (t.to_string(), self.probe(t)))
            .collect();
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
        let bytes = serde_json::to_vec(&(
            &shell_identity,
            &self.digest,
            &os,
            &arch,
            rustix::process::geteuid().as_raw(),
            &tools,
        ))
        .expect("profile report serializes");
        WorkerCapabilities {
            disk_free_bytes: None,
            resource_budget: None,
            schema_version: 1,
            os,
            arch,
            profile: self.name.clone(),
            profile_digest: self.digest.clone(),
            execution_uid: rustix::process::geteuid().as_raw(),
            protocol_versions: vec![1],
            profile_fingerprint: Digest::of_bytes(&bytes),
            tools,
            features: FEATURES.iter().map(|v| v.to_string()).collect(),
        }
    }
    fn probe(&self, tool: &str) -> ToolObservation {
        let binary = if tool == "xcode" { "xcodebuild" } else { tool };
        let Some(path) = self
            .definition
            .path
            .iter()
            .map(|p| p.join(binary))
            .find(|p| executable(p))
        else {
            return ToolObservation::Unavailable;
        };
        // The standard Flutter launcher bootstraps SDK/cache even for
        // --version. Its noninstalling snapshot adapter belongs to S20.
        // Flutter's dart launcher has the same behavior; native Dart can be
        // measured without invoking that bootstrap wrapper.
        if tool == "flutter" {
            return ToolObservation::Unknown;
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
                return ToolObservation::Unknown;
            }
        }
        let args: &[&str] = match tool {
            "docker" => &["info", "--format", "{{.ServerVersion}}"],
            "xcode" => &["-version"],
            _ => &["--version"],
        };
        let Some(version) = bounded_probe(&path, args, &self.env) else {
            return ToolObservation::Unknown;
        };
        let Some(version) = measured_version(tool, &version) else {
            return ToolObservation::Unknown;
        };
        let sdk_identity = if tool == "xcode" {
            let Some(xcrun) = self
                .definition
                .path
                .iter()
                .map(|p| p.join("xcrun"))
                .find(|p| executable(p))
            else {
                return ToolObservation::Unknown;
            };
            let Some(sdk) = bounded_probe(&xcrun, &["--show-sdk-path"], &self.env) else {
                return ToolObservation::Unknown;
            };
            let sdk = PathBuf::from(sdk.trim());
            if !sdk.is_absolute() || !sdk.is_dir() {
                return ToolObservation::Unknown;
            }
            let Ok(real_sdk) = fs::canonicalize(&sdk) else {
                return ToolObservation::Unknown;
            };
            Some(Digest::of_bytes(real_sdk.as_os_str().as_encoded_bytes()))
        } else {
            None
        };
        let Ok(real) = fs::canonicalize(&path) else {
            return ToolObservation::Unknown;
        };
        let Ok(meta) = fs::metadata(&real) else {
            return ToolObservation::Unknown;
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
        ToolObservation::Available {
            version,
            identity: Digest::of_bytes(&bytes),
        }
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
fn bounded_probe(path: &Path, args: &[&str], env: &BTreeMap<String, String>) -> Option<String> {
    let cwd = tempfile::Builder::new()
        .prefix("rtr-tool-probe-")
        .tempdir()
        .ok()?;
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
        .ok()?;
    let pid = child.id();
    // Every failure after spawn still passes through owned group cleanup/reap.
    let output = (|| {
        let mut out = child.stdout.take()?;
        let mut err = child.stderr.take()?;
        for fd in [out.as_raw_fd(), err.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return None;
            }
        }
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let started = Instant::now();
        loop {
            if !read_available(&mut out, &mut stdout) || !read_available(&mut err, &mut stderr) {
                return None;
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
            if observed != 0 || started.elapsed() >= PROBE_TIMEOUT {
                return None;
            }
            if unsafe { info.si_pid() } == pid as i32 {
                if !read_available(&mut out, &mut stdout) || !read_available(&mut err, &mut stderr)
                {
                    return None;
                }
                return Some(if stdout.is_empty() { stderr } else { stdout });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    })();
    // Our unreaped Child owns pid, so this group number cannot be recycled.
    if let Ok(group) = i32::try_from(pid) {
        crate::platform::signal_process_group(group, libc::SIGKILL);
    }
    let status = child.wait().ok()?;
    if !status.success() {
        return None;
    }
    String::from_utf8(output?).ok()
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
    fn write_profile(dir: &Path, active: &str, extra: &str) {
        fs::write(dir.join("profiles.toml"), format!("schema_version = 1\nactive = {active:?}\n[profiles.node22]\npath = [{:?}]\nshell = \"/bin/sh\"\ncache_namespace = \"node22\"\n{extra}", dir.to_str().unwrap())).unwrap();
        fs::set_permissions(dir.join("profiles.toml"), fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[test]
    fn actual_path_versions_and_private_profile_are_authoritative() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        fake(a.path(), "node", "printf 'v22.0.0\\n'", 0o700);
        fake(b.path(), "node", "printf 'v20.0.0\\n'", 0o700);
        let p22 = Profile::load(a.path(), &env(a.path())).unwrap();
        let p20 = Profile::load(b.path(), &env(b.path())).unwrap();
        let a_report = p22.report();
        let b_report = p20.report();
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
    fn probe_failures_and_docker_info_are_not_available() {
        let root = tempfile::tempdir().unwrap();
        fake(root.path(), "node", "echo v22.0.0", 0o600);
        let p = Profile::load(root.path(), &env(root.path())).unwrap();
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
                &format!("touch {:?}; echo 3.0.0", marker),
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
            p.probe("docker"),
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
            p.probe("xcode"),
            ToolObservation::Available { .. }
        ));
        fake(root.path(), "node", "echo v22.0.0 >&2", 0o700);
        assert!(matches!(p.probe("node"), ToolObservation::Available { .. }));
        fake(
            root.path(),
            "node",
            "(while :; do :; done) &\necho v22.0.0",
            0o700,
        );
        let at = Instant::now();
        assert!(matches!(p.probe("node"), ToolObservation::Available { .. }));
        assert!(at.elapsed() < Duration::from_secs(2));
    }
    #[test]
    fn private_input_rejects_symlinks_permissions_secrets_and_required_missing() {
        let root = tempfile::tempdir().unwrap();
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
